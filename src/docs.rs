//! Documents from path-named query columns: the rows of all `selects` for a batch are merged by
//! `_key` into one object per key.
//!
//! * `name`, `price.final`, `array[1].value`: the value at that path (the last row wins);
//! * `ids[]`: every row of the key appends its value (NULLs are dropped, so an empty set is
//!   `[]`); `items[].sku` + `items[].qty` append one object per row;
//! * `_path` + `_value`: the path held in `_path` is set to `_value`, one row per field;
//! * `_op`: `delete` marks the key deleted; a key no select returns, while some select has an
//!   `_op` column, is deleted too;
//! * a name ending in `:json` holds JSON text that is parsed (MySQL `JSON` columns always are).

use std::collections::HashMap;

use anyhow::{bail, Result};
use indexmap::IndexMap;
use mysql_async::consts::ColumnType;
use mysql_async::{Column, Value as My};
use serde_json::{Map, Value};

use crate::path::{Path, Step};

#[derive(Debug, Clone, PartialEq)]
enum Role {
    Key,
    Op,
    DynPath,
    DynValue,
    Set(Path),
    /// Member of collect group `usize`, at this path inside the element (empty: the element).
    Member(usize, Path),
}

/// The JSON type a document field renders as. Targets that need the shape before they run,
/// such as the gRPC one, check it against their schema at load time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    /// Any integer column, signed or not: protobuf has no unsigned 64-bit type, so even
    /// `BIGINT UNSIGNED` goes out as a signed varint.
    Int,
    /// A `FLOAT`/`DOUBLE`/`DECIMAL`: a JSON number that may hold a fraction.
    Double,
    /// Text, and every column `to_json` renders as a string: dates and times become their
    /// text, an unknown type is read as UTF-8.
    Str,
    /// A `BINARY`/`VARBINARY`/`BLOB`: `to_json` renders it as lossy UTF-8 text, so it fills
    /// a string field and not protobuf's `bytes`, which wants base64.
    Blob,
    /// An array: the top-level key of a `path[]` collect column.
    Array,
    /// An object: the top-level key of a column with a path deeper than one step.
    Object,
    /// Whatever the column holds, read as JSON: a `JSON` column, one asked for as
    /// `col:json`, or a value with no column behind it. Its shape is only known at run time.
    Unknown,
}

impl ValueKind {
    /// The kind a value of a column with this MySQL type renders as, following the table in
    /// [`to_json`]. `binary` is the column's binary flag: `CHAR`/`VARCHAR` and `BINARY`/
    /// `VARBINARY` share a type and are told apart by it. `None` is a value with no column
    /// behind it.
    pub fn of_column(ty: Option<ColumnType>, binary: bool) -> Self {
        match ty {
            None => Self::Unknown,
            Some(
                ColumnType::MYSQL_TYPE_TINY
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_LONGLONG
                | ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_YEAR
                | ColumnType::MYSQL_TYPE_BIT,
            ) => Self::Int,
            Some(
                ColumnType::MYSQL_TYPE_FLOAT
                | ColumnType::MYSQL_TYPE_DOUBLE
                | ColumnType::MYSQL_TYPE_DECIMAL
                | ColumnType::MYSQL_TYPE_NEWDECIMAL,
            ) => Self::Double,
            // A JSON column is parsed, so it is whatever the document inside it is.
            Some(ColumnType::MYSQL_TYPE_JSON) => Self::Unknown,
            // A binary column arrives as bytes and is rendered as lossy UTF-8 text, which a
            // protobuf `bytes` field would read as base64. It has to be told apart from text.
            Some(
                ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB,
            ) => Self::Blob,
            Some(ColumnType::MYSQL_TYPE_STRING | ColumnType::MYSQL_TYPE_VAR_STRING) if binary => {
                Self::Blob
            }
            // Everything else, dates and times and `SET` and types not named here, becomes
            // text.
            Some(_) => Self::Str,
        }
    }
}

/// One column of a statement, with what its value does in the document.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// The column as the rule names it.
    pub column: String,
    /// The top-level document key its path fills.
    pub key: String,
    /// What lands there.
    pub kind: ValueKind,
}

impl Field {
    /// A column that fills its own document key. `ty` is its MySQL type and `binary` its
    /// binary flag; `None` is a value with no column behind it.
    pub fn new(column: impl Into<String>, ty: Option<ColumnType>, binary: bool) -> Self {
        let column = column.into();
        Field {
            kind: ValueKind::of_column(ty, binary),
            key: column.clone(),
            column,
        }
    }
}

/// The payload entry for a column whose path starts at `path`'s first step.
fn payload_field(column: &str, path: &Path, kind: ValueKind) -> Field {
    // A path always starts with a field name (`Plan::build` refuses one that does not), so the
    // fallback is only for a column that names its key itself.
    let key = match path.0.first() {
        Some(Step::Field(name)) => name.clone(),
        _ => column.to_string(),
    };
    Field {
        column: column.to_string(),
        key,
        kind,
    }
}

/// How one select's columns map into documents.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    roles: Vec<(Role, bool)>,
    /// Array path of each collect group.
    groups: Vec<Path>,
    /// The payload columns and what each lands as, in the order they were given.
    fields: Vec<Field>,
    pub has_op: bool,
}

impl Plan {
    /// Builds the plan for a select's column names; errors are registration errors.
    pub fn new(names: &[String]) -> Result<Plan> {
        Plan::build(
            &names
                .iter()
                .map(|n| Field::new(n, None, false))
                .collect::<Vec<_>>(),
        )
    }

    /// Builds the plan for a select's columns, keeping each one's MySQL type so a target can
    /// check its schema against what the rule actually sends.
    pub fn with_fields(fields: &[Field]) -> Result<Plan> {
        Plan::build(fields)
    }

    /// The payload columns and what each lands as, in order. Protocol columns (`_key`,
    /// `_op`, `_path`, `_value`) are not payload and are not listed.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    fn build(columns: &[Field]) -> Result<Plan> {
        let mut roles = Vec::with_capacity(columns.len());
        let mut groups: Vec<Path> = Vec::new();
        let mut payload: Vec<Field> = Vec::new();
        let (mut key, mut dp, mut dv, mut has_op) = (false, false, false, false);
        for column in columns {
            let name = column.column.as_str();
            let (base, json) = match name.strip_suffix(":json") {
                Some(b) => (b, true),
                None => (name, false),
            };
            let role = match base {
                "_key" => {
                    key = true;
                    Role::Key
                }
                "_op" => {
                    has_op = true;
                    Role::Op
                }
                "_path" => {
                    dp = true;
                    Role::DynPath
                }
                "_value" => {
                    dv = true;
                    Role::DynValue
                }
                _ => {
                    let p =
                        Path::parse(base).map_err(|e| anyhow::anyhow!("column `{name}`: {e}"))?;
                    if p.is_root() {
                        bail!("column `{name}` names no field");
                    }
                    match p.each_at() {
                        None => {
                            // A deeper path builds objects on the way down, so the key it
                            // fills holds an object whatever the column itself holds.
                            let kind = if p.0.len() == 1 {
                                if json {
                                    ValueKind::Unknown
                                } else {
                                    column.kind
                                }
                            } else {
                                ValueKind::Object
                            };
                            payload.push(payload_field(name, &p, kind));
                            Role::Set(p)
                        }
                        Some(0) => bail!("column `{name}`: `[]` must follow a field name"),
                        Some(i) => {
                            if p.suffix(i + 1).each_at().is_some() {
                                bail!("column `{name}`: only one `[]` per column");
                            }
                            let prefix = p.prefix(i);
                            let g = match groups.iter().position(|g| *g == prefix) {
                                Some(g) => g,
                                None => {
                                    groups.push(prefix.clone());
                                    groups.len() - 1
                                }
                            };
                            // The key holds the array, or the object that holds it.
                            let kind = if prefix.0.len() == 1 {
                                ValueKind::Array
                            } else {
                                ValueKind::Object
                            };
                            payload.push(payload_field(name, &prefix, kind));
                            Role::Member(g, p.suffix(i + 1))
                        }
                    }
                }
            };
            roles.push((role, json));
        }
        if !key {
            bail!("no `_key` column");
        }
        if dp != dv {
            bail!("`_path` and `_value` go together");
        }
        let plan = Plan {
            roles,
            groups,
            fields: payload,
            has_op,
        };
        // Within one select, written places must not overlap (members of one group excepted).
        let places = plan.places();
        for (i, a) in places.iter().enumerate() {
            for b in &places[i + 1..] {
                if a.0.overlaps(&b.0) && !(a.1.is_some() && a.1 == b.1 && !a.2.overlaps(&b.2)) {
                    bail!("columns `{}` and `{}` write the same place", a.3, b.3);
                }
            }
        }
        Ok(plan)
    }

    /// (top-level place, group, path inside the element, column name) for overlap checks.
    fn places(&self) -> Vec<(Path, Option<usize>, Path, String)> {
        self.roles
            .iter()
            .filter_map(|(r, _)| match r {
                Role::Set(p) => Some((p.clone(), None, Path::default(), p.to_string())),
                Role::Member(g, inner) => {
                    let mut full = self.groups[*g].clone();
                    full.0.push(Step::Each);
                    full.0.extend(inner.0.iter().cloned());
                    Some((
                        self.groups[*g].clone(),
                        Some(*g),
                        inner.clone(),
                        full.to_string(),
                    ))
                }
                _ => None,
            })
            .collect()
    }

    /// Fixed places this select writes (for the one-owner-per-path check across selects).
    pub fn owned(&self) -> Vec<(Path, String)> {
        let mut seen = Vec::new();
        for (p, g, _, name) in self.places() {
            if g.is_some() && seen.iter().any(|(q, _): &(Path, String)| *q == p) {
                continue;
            }
            seen.push((p, name));
        }
        seen
    }
}

/// Checks that no two selects write the same place.
pub fn check_owners(plans: &[(String, Plan)]) -> Result<()> {
    for (i, (na, a)) in plans.iter().enumerate() {
        for (nb, b) in &plans[i + 1..] {
            for (pa, ca) in a.owned() {
                for (pb, cb) in b.owned() {
                    if pa.overlaps(&pb) {
                        bail!("`{ca}` ({na}) and `{cb}` ({nb}) write the same place: a path has one owning select");
                    }
                }
            }
        }
    }
    Ok(())
}

/// One key's merged result.
#[derive(Debug, Clone, PartialEq)]
pub struct Doc {
    pub key: String,
    pub doc: Value,
    pub delete: bool,
}

impl Doc {
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("key".into(), Value::String(self.key.clone()));
        m.insert("doc".into(), self.doc.clone());
        m.insert(
            "op".into(),
            Value::String(if self.delete { "delete" } else { "upsert" }.into()),
        );
        Value::Object(m)
    }
}

struct Entry {
    doc: Value,
    delete: bool,
    seen: bool,
}

/// Collects rows of all selects for one batch of keys.
pub struct Merger {
    docs: IndexMap<String, Entry>,
    any_op: bool,
    pub warnings: Vec<String>,
}

impl Merger {
    pub fn new(keys: &[String]) -> Merger {
        Merger {
            docs: keys
                .iter()
                .map(|k| {
                    (
                        k.clone(),
                        Entry {
                            doc: Value::Object(Map::new()),
                            delete: false,
                            seen: false,
                        },
                    )
                })
                .collect(),
            any_op: false,
            warnings: Vec::new(),
        }
    }

    /// Adds the rows of one select (values already converted to JSON).
    pub fn add(&mut self, plan: &Plan, rows: Vec<Vec<Value>>) {
        self.any_op |= plan.has_op;
        let key_at = plan
            .roles
            .iter()
            .position(|(r, _)| *r == Role::Key)
            .expect("plan has _key");
        let mut members: HashMap<usize, Vec<(usize, &Path)>> = HashMap::new();
        for (i, (r, _)) in plan.roles.iter().enumerate() {
            if let Role::Member(g, inner) = r {
                members.entry(*g).or_default().push((i, inner));
            }
        }
        for mut row in rows {
            let key = match &row[key_at] {
                Value::Null => continue,
                v => crate::template::text(v),
            };
            let Some(e) = self.docs.get_mut(&key) else {
                continue;
            };
            e.seen = true;
            for (i, (role, json)) in plan.roles.iter().enumerate() {
                if *json {
                    row[i] = parse_json(std::mem::take(&mut row[i]));
                }
                match role {
                    Role::Set(p) => p.set(&mut e.doc, row[i].clone()),
                    Role::Op if crate::template::text(&row[i]).eq_ignore_ascii_case("delete") => {
                        e.delete = true;
                    }
                    _ => {}
                }
            }
            for (g, prefix) in plan.groups.iter().enumerate() {
                prefix.ensure_array(&mut e.doc);
                let ms = &members[&g];
                if ms.iter().all(|(i, _)| row[*i].is_null()) {
                    continue;
                }
                let elem = if ms.len() == 1 && ms[0].1.is_root() {
                    row[ms[0].0].clone()
                } else {
                    let mut el = Value::Object(Map::new());
                    for (i, inner) in ms {
                        inner.set(&mut el, row[*i].clone());
                    }
                    el
                };
                let mut each = prefix.clone();
                each.0.push(Step::Each);
                each.set(&mut e.doc, elem);
            }
            let dyn_path = plan.roles.iter().position(|(r, _)| *r == Role::DynPath);
            let dyn_value = plan.roles.iter().position(|(r, _)| *r == Role::DynValue);
            if let (Some(pi), Some(vi)) = (dyn_path, dyn_value) {
                let raw = crate::template::text(&row[pi]);
                if raw.is_empty() {
                    continue;
                }
                match Path::parse(&raw) {
                    Ok(p) if !p.is_root() => match p.each_at() {
                        Some(i) if i + 1 == p.0.len() => {
                            p.prefix(i).ensure_array(&mut e.doc);
                            if !row[vi].is_null() {
                                p.set(&mut e.doc, row[vi].clone());
                            }
                        }
                        Some(_) => self
                            .warnings
                            .push(format!("_path `{raw}`: `[]` only at the end")),
                        None => p.set(&mut e.doc, row[vi].clone()),
                    },
                    Ok(_) => self.warnings.push("empty _path".into()),
                    Err(err) => self.warnings.push(format!("_path `{raw}`: {err}")),
                }
            }
        }
    }

    pub fn finish(self) -> Vec<Doc> {
        let any_op = self.any_op;
        self.docs
            .into_iter()
            .map(|(key, e)| Doc {
                key,
                doc: e.doc,
                delete: e.delete || (any_op && !e.seen),
            })
            .collect()
    }
}

fn parse_json(v: Value) -> Value {
    match v {
        Value::String(s) => serde_json::from_str(&s).unwrap_or(Value::String(s)),
        other => other,
    }
}

fn pad2(n: impl Into<u32>) -> String {
    format!("{:02}", n.into())
}

fn frac(us: u32, decimals: u8) -> String {
    if us == 0 && decimals == 0 {
        return String::new();
    }
    let d = if decimals == 0 || decimals > 6 {
        6
    } else {
        decimals as usize
    };
    let s = format!("{us:06}");
    format!(".{}", &s[..d])
}

/// A query value as JSON, using the column's type.
pub fn to_json(v: My, col: Option<&Column>) -> Value {
    let ty = col.map(|c| c.column_type());
    let decimals = col.map(|c| c.decimals()).unwrap_or(0);
    match v {
        My::NULL => Value::Null,
        My::Int(i) => Value::from(i),
        My::UInt(u) => Value::from(u),
        My::Float(f) => {
            serde_json::Number::from_f64(f64::from(f)).map_or(Value::Null, Value::Number)
        }
        My::Double(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        My::Date(y, m, d, h, i, s, us) => {
            if ty == Some(ColumnType::MYSQL_TYPE_DATE) || ty == Some(ColumnType::MYSQL_TYPE_NEWDATE)
            {
                Value::String(format!("{y:04}-{}-{}", pad2(m), pad2(d)))
            } else {
                Value::String(format!(
                    "{y:04}-{}-{} {}:{}:{}{}",
                    pad2(m),
                    pad2(d),
                    pad2(h),
                    pad2(i),
                    pad2(s),
                    frac(us, decimals)
                ))
            }
        }
        My::Time(neg, d, h, i, s, us) => Value::String(format!(
            "{}{}:{}:{}{}",
            if neg { "-" } else { "" },
            pad2(d * 24 + u32::from(h)),
            pad2(i),
            pad2(s),
            frac(us, decimals)
        )),
        My::Bytes(b) => {
            let text = || String::from_utf8_lossy(&b).into_owned();
            match ty {
                Some(ColumnType::MYSQL_TYPE_NEWDECIMAL | ColumnType::MYSQL_TYPE_DECIMAL) => {
                    serde_json::from_str::<serde_json::Number>(&text())
                        .map_or_else(|_| Value::String(text()), Value::Number)
                }
                Some(
                    ColumnType::MYSQL_TYPE_TINY
                    | ColumnType::MYSQL_TYPE_SHORT
                    | ColumnType::MYSQL_TYPE_LONG
                    | ColumnType::MYSQL_TYPE_LONGLONG
                    | ColumnType::MYSQL_TYPE_INT24
                    | ColumnType::MYSQL_TYPE_YEAR
                    | ColumnType::MYSQL_TYPE_FLOAT
                    | ColumnType::MYSQL_TYPE_DOUBLE,
                ) => serde_json::from_str::<serde_json::Number>(&text())
                    .map_or_else(|_| Value::String(text()), Value::Number),
                Some(ColumnType::MYSQL_TYPE_JSON) => {
                    serde_json::from_slice(&b).unwrap_or_else(|_| Value::String(text()))
                }
                Some(ColumnType::MYSQL_TYPE_BIT) => Value::from(
                    b.iter()
                        .take(8)
                        .fold(0u64, |acc, x| (acc << 8) | u64::from(*x)),
                ),
                _ => Value::String(text()),
            }
        }
    }
}

/// A key as text (`_key` columns, snapshot keys): integers in decimal, bytes as UTF-8.
pub fn key_text(v: &My) -> Option<String> {
    match v {
        My::NULL => None,
        My::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        My::Int(i) => Some(i.to_string()),
        My::UInt(u) => Some(u.to_string()),
        other => Some(crate::template::text(&to_json(other.clone(), None))),
    }
}

#[cfg(test)]
#[path = "tests/docs.rs"]
mod tests;
