//! Binlog row images as named JSON values.
//!
//! Column names and types come from `information_schema`. With `binlog_row_metadata=FULL`
//! the table map carries the names itself, which stays exact while a DDL is in flight (events
//! read from before an `ALTER` keep their old layout); otherwise the layout is trusted when the
//! column counts agree.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use mysql_async::binlog::events::{OptionalMetadataField, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::*;
use mysql_async::{Conn, Value as My};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum ColKind {
    Plain,
    Unsigned(u32),
    Enum(Vec<String>),
    Set(Vec<String>),
    Bit,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    pub names: Vec<String>,
    kinds: Vec<ColKind>,
    by_name: HashMap<String, usize>,
}

impl Layout {
    pub fn new(names: Vec<String>, kinds: Vec<ColKind>) -> Layout {
        let by_name = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i))
            .collect();
        Layout {
            names,
            kinds,
            by_name,
        }
    }

    pub fn index(&self, name: &str) -> Option<usize> {
        self.by_name.get(name).copied().or_else(|| {
            // Column names are case-insensitive.
            self.names.iter().position(|n| n.eq_ignore_ascii_case(name))
        })
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// One column of a row image as JSON.
    pub fn value(&self, row: &BinlogRow, i: usize) -> Value {
        convert(row.as_ref(i), self.kinds.get(i).unwrap_or(&ColKind::Plain))
    }

    /// Whether the two images of a row differ in column `i`: the same answer as
    /// `self.value(b, i) != self.value(a, i)`, without building either value when the two can be
    /// shown to be equal.
    ///
    /// Only the equal case is skipped, and only where it is certain: `convert` is a function of the
    /// value and the column's kind, so identical input cannot give a different output. Anything the
    /// rule below does not cover — a `Jsonb`, two payloads that differ, one of the two missing — is
    /// converted and compared exactly as before. Inferring *difference* from the payloads would be a
    /// different matter (two `i64` can share an `Unsigned` mask, two byte strings can lose to the
    /// same `from_utf8_lossy` text), so it is not tried.
    pub fn changed(&self, b: &BinlogRow, a: &BinlogRow, i: usize) -> bool {
        let kind = self.kinds.get(i).unwrap_or(&ColKind::Plain);
        let (b, a) = (b.as_ref(i), a.as_ref(i));
        if same_value(b, a) {
            return false;
        }
        convert(b, kind) != convert(a, kind)
    }

    /// The whole image as an object.
    pub fn object(&self, row: &BinlogRow) -> Value {
        let mut m = Map::with_capacity(self.names.len());
        for (i, n) in self.names.iter().enumerate() {
            m.insert(n.clone(), self.value(row, i));
        }
        Value::Object(m)
    }
}

pub fn col_kind(data_type: &str, column_type: &str) -> ColKind {
    let dt = data_type.to_ascii_lowercase();
    let ct = column_type.to_ascii_lowercase();
    match dt.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
            if ct.contains("unsigned") =>
        {
            ColKind::Unsigned(match dt.as_str() {
                "tinyint" => 8,
                "smallint" => 16,
                "mediumint" => 24,
                "int" | "integer" => 32,
                _ => 64,
            })
        }
        "enum" => ColKind::Enum(parse_labels(column_type)),
        "set" => ColKind::Set(parse_labels(column_type)),
        "bit" => ColKind::Bit,
        _ => ColKind::Plain,
    }
}

/// `enum('a','b''c')` → its labels.
fn parse_labels(column_type: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = column_type.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut s = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    s.push('\'');
                }
                Some('\'') | None => break,
                Some('\\') => {
                    if let Some(n) = chars.next() {
                        s.push(n);
                    }
                }
                Some(c) => s.push(c),
            }
        }
        out.push(s);
    }
    out
}

fn convert(v: Option<&BinlogValue<'_>>, kind: &ColKind) -> Value {
    let v = match v {
        None => return Value::Null,
        Some(BinlogValue::Value(v)) => v,
        Some(BinlogValue::Jsonb(j)) => {
            return j.clone().try_into().unwrap_or(Value::Null);
        }
        // Partial JSON updates carry no full value.
        Some(BinlogValue::JsonDiff(_)) => return Value::Null,
    };
    match (kind, v) {
        (_, My::NULL) => Value::Null,
        (ColKind::Unsigned(bits), My::Int(i)) => {
            let mask = if *bits >= 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            };
            Value::from((*i as u64) & mask)
        }
        (ColKind::Enum(labels), My::Int(i)) => match usize::try_from(*i) {
            Ok(0) => Value::String(String::new()),
            Ok(i) if i <= labels.len() => Value::String(labels[i - 1].clone()),
            _ => Value::from(*i),
        },
        (ColKind::Set(labels), My::Bytes(b)) => {
            let mut mask = 0u64;
            for (i, byte) in b.iter().enumerate().take(8) {
                mask |= u64::from(*byte) << (8 * i);
            }
            let parts: Vec<&str> = labels
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1u64 << i) != 0)
                .map(|(_, l)| l.as_str())
                .collect();
            Value::String(parts.join(","))
        }
        (ColKind::Bit, My::Bytes(b)) => Value::from(
            b.iter()
                .take(8)
                .fold(0u64, |acc, x| (acc << 8) | u64::from(*x)),
        ),
        (_, other) => crate::docs::to_json_ref(other, None),
    }
}

/// Certain to render as the same JSON value. The missing column of an image is `None` and renders
/// as `Value::Null`, as do `NULL` and a partial JSON update, which are therefore equal to it.
fn same_value(a: Option<&BinlogValue<'_>>, b: Option<&BinlogValue<'_>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (None, Some(v)) | (Some(v), None) => is_null(v),
        (Some(a), Some(b)) => match (a, b) {
            // a partial JSON update carries no value: both sides are `Value::Null`
            (BinlogValue::JsonDiff(_), BinlogValue::JsonDiff(_)) => true,
            (BinlogValue::Value(a), BinlogValue::Value(b)) => same_my(a, b),
            // a `Jsonb` is as much work to compare as to convert, and it is not the hot column
            _ => false,
        },
    }
}

/// A value `convert` renders as `Value::Null`.
fn is_null(v: &BinlogValue<'_>) -> bool {
    matches!(v, BinlogValue::JsonDiff(_) | BinlogValue::Value(My::NULL))
}

/// Same variant, same payload. Floats compare by bits, which is finer than the text `Number` keeps,
/// so two values here cannot render differently; `Date` and `Time` are their fields.
fn same_my(a: &My, b: &My) -> bool {
    match (a, b) {
        (My::NULL, My::NULL) => true,
        (My::Int(a), My::Int(b)) => a == b,
        (My::UInt(a), My::UInt(b)) => a == b,
        (My::Float(a), My::Float(b)) => a.to_bits() == b.to_bits(),
        (My::Double(a), My::Double(b)) => a.to_bits() == b.to_bits(),
        (My::Date(y1, m1, d1, h1, i1, s1, us1), My::Date(y2, m2, d2, h2, i2, s2, us2)) => {
            (y1, m1, d1, h1, i1, s1, us1) == (y2, m2, d2, h2, i2, s2, us2)
        }
        (My::Time(n1, d1, h1, i1, s1, us1), My::Time(n2, d2, h2, i2, s2, us2)) => {
            (n1, d1, h1, i1, s1, us1) == (n2, d2, h2, i2, s2, us2)
        }
        (My::Bytes(a), My::Bytes(b)) => a == b,
        _ => false,
    }
}

/// Names from the table map's optional metadata (`binlog_row_metadata=FULL`).
pub fn table_map_names(tme: &TableMapEvent<'_>) -> Option<Vec<String>> {
    for f in tme.iter_optional_meta() {
        if let Ok(OptionalMetadataField::ColumnName(names)) = f {
            let v: Result<Vec<String>, _> = names
                .iter_names()
                .map(|n| n.map(|n| n.name().into_owned()))
                .collect();
            return v.ok();
        }
    }
    None
}

/// The layout an event's own column names describe: every column keeps the kind of the
/// `information_schema` column of the same name (a name the read does not know is plain).
/// Pure: the same `info` and the same names always derive the same layout.
pub fn layout_from_names(info: &Layout, names: &[String]) -> Arc<Layout> {
    let kinds = names
        .iter()
        .map(|n| {
            info.index(n)
                .and_then(|i| info.kinds.get(i).cloned())
                .unwrap_or(ColKind::Plain)
        })
        .collect();
    Arc::new(Layout::new(names.to_vec(), kinds))
}

/// The `information_schema` layouts, cached per table — and, keyed by those names, the
/// layouts derived from a table map's own names (`binlog_row_metadata=FULL`).
#[derive(Default)]
pub struct Catalog {
    cache: Cache,
    /// The FULL-metadata layouts, keyed by the names of the events that asked for them and
    /// stamped with the `information_schema` read they were derived from: an in-flight DDL
    /// makes two events of one table name different columns, and each keeps its own layout.
    /// A layout of this map is only ever handed out to an event carrying these very names,
    /// and only while the read it was derived from is still the current one (see
    /// [`Catalog::current`]) — the two maps never share an `Arc`.
    full: HashMap<(String, String), HashMap<Vec<String>, (Arc<Layout>, Arc<Layout>)>>,
}

/// The plain `(schema, table)` layout map. A thin `HashMap` wrapper whose every mutation —
/// every layout installed, every one dropped — raises the catalog's generation: an answer
/// cached at an older generation cannot outlive what it was built from. This is what lets
/// the column-count trust path in [`Catalog::layout`] keep its own
/// `self.cache.insert(key, info.clone())` line verbatim: a re-read installed is an `insert`
/// through here, and it raises.
#[derive(Default)]
struct Cache {
    map: HashMap<(String, String), Arc<Layout>>,
    generation: u64,
}

impl Cache {
    /// Raises the generation and installs the layout — the one door a layout goes in by.
    fn insert(&mut self, key: (String, String), layout: Arc<Layout>) -> Option<Arc<Layout>> {
        self.generation += 1;
        self.map.insert(key, layout)
    }

    fn get(&self, key: &(String, String)) -> Option<&Arc<Layout>> {
        self.map.get(key)
    }

    fn remove(&mut self, key: &(String, String)) -> Option<Arc<Layout>> {
        self.generation += 1;
        self.map.remove(key)
    }

    fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&(String, String), &mut Arc<Layout>) -> bool,
    {
        self.generation += 1;
        self.map.retain(f)
    }

    fn clear(&mut self) {
        self.generation += 1;
        self.map.clear()
    }

    #[cfg(test)]
    fn contains_key(&self, key: &(String, String)) -> bool {
        self.map.contains_key(key)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Why a row image cannot be named.
#[derive(Debug, thiserror::Error)]
#[error("table `{schema}`.`{table}` has {have} columns in the binlog but {want} in information_schema (a DDL changed it; set binlog_row_metadata=FULL to decode across DDL)")]
pub struct LayoutMismatch {
    pub schema: String,
    pub table: String,
    pub have: usize,
    pub want: usize,
}

impl Catalog {
    pub fn invalidate(&mut self, schema: &str, table: &str) {
        if table == "*" {
            self.cache.retain(|(s, _), _| s != schema);
            self.full.retain(|(s, _), _| s != schema);
        } else {
            let key = (schema.to_string(), table.to_string());
            self.cache.remove(&key);
            self.full.remove(&key);
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.full.clear();
    }

    /// What anything answering from this catalog must have been built at. The number is
    /// raised by every mutation of either map: `Cache::insert` — the only door a layout is
    /// installed by, which both the first load and the column-count re-read of
    /// [`Catalog::layout`] go through — `Cache::remove`/`retain`/`clear`, and the insert in
    /// `full_layout`; the `full` map is written nowhere else. So the counter cannot lag a
    /// change it should cover, and it is read by the engine's route probe as the cheap
    /// pre-check beside the identity check that really decides the question
    /// ([`Catalog::current`]).
    pub fn generation(&self) -> u64 {
        self.cache.generation
    }

    /// Records a freshly read `information_schema` layout.
    #[cfg(test)]
    pub(crate) fn put(&mut self, schema: &str, table: &str, layout: Arc<Layout>) {
        self.cache
            .insert((schema.to_string(), table.to_string()), layout);
    }

    /// The layout this catalog answers with, right now, for an event on `key` carrying
    /// `names` as its own column names (`None`: the event names no columns at all). It is
    /// [`Catalog::layout`]'s answer minus the database: the current `information_schema`
    /// read for a plain event, and for a FULL one a layout derived from exactly these names
    /// from exactly this read. A FULL entry derived from a since-replaced read answers
    /// `None` — `layout` would derive a fresh `Arc` for that event, so the entry in the map
    /// is not the current answer — and a plain request can never be answered from the
    /// derived map nor a FULL one from the plain read, which is the DDL-survival rule.
    pub fn current(
        &self,
        key: &(String, String),
        names: Option<&[String]>,
    ) -> Option<&Arc<Layout>> {
        let info = self.cache.get(key)?;
        match names {
            None => Some(info),
            Some(names) => self
                .full
                .get(key)?
                .get(names)
                .filter(|(from, _)| Arc::ptr_eq(from, info))
                .map(|(_, l)| l),
        }
    }

    /// The layout of these table-map names, cached per (table, names): a repeat of the same
    /// names from the same read costs one `Arc` clone and no allocation of its own; new ones
    /// (or ones from a since-read table) derive from `info` once, together with the read they
    /// were derived from. `pub(crate)` so the engine's route-cache tests can stock a catalog
    /// the way `layout` does; nothing outside this module calls it.
    pub(crate) fn full_layout(
        &mut self,
        key: &(String, String),
        names: &[String],
        info: &Arc<Layout>,
    ) -> Arc<Layout> {
        if let Some(l) = self
            .full
            .get(key)
            .and_then(|m| m.get(names))
            .filter(|(from, _)| Arc::ptr_eq(from, info))
            .map(|(_, l)| l.clone())
        {
            return l;
        }
        let l = layout_from_names(info, names);
        self.full
            .entry(key.clone())
            .or_default()
            .insert(names.to_vec(), (info.clone(), l.clone()));
        self.cache.generation += 1;
        l
    }

    async fn load(conn: &mut Conn, schema: &str, table: &str) -> Result<Arc<Layout>> {
        let rows: Vec<(String, String, String)> = conn
            .exec(
                "SELECT CAST(COLUMN_NAME AS CHAR), CAST(DATA_TYPE AS CHAR), CAST(COLUMN_TYPE AS CHAR) \
                 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (schema, table),
            )
            .await
            .with_context(|| format!("reading the columns of `{schema}`.`{table}`"))?;
        let (names, kinds) = rows
            .into_iter()
            .map(|(n, d, c)| (n, col_kind(&d, &c)))
            .unzip();
        Ok(Arc::new(Layout::new(names, kinds)))
    }

    /// The layout for rows described by `tme`.
    pub async fn layout(
        &mut self,
        conn: &mut Conn,
        schema: &str,
        table: &str,
        tme: Option<&TableMapEvent<'_>>,
    ) -> Result<Arc<Layout>> {
        let key = (schema.to_string(), table.to_string());
        let have = tme.map(|t| t.columns_count() as usize);
        let mut info = match self.cache.get(&key) {
            Some(l) => l.clone(),
            None => {
                let l = Self::load(conn, schema, table).await?;
                self.cache.insert(key.clone(), l.clone());
                l
            }
        };
        if let Some(names) = tme.and_then(table_map_names) {
            return Ok(self.full_layout(&key, &names, &info));
        }
        if let Some(have) = have {
            if have != info.len() {
                info = Self::load(conn, schema, table).await?;
                self.cache.insert(key, info.clone());
                if have != info.len() {
                    return Err(LayoutMismatch {
                        schema: schema.into(),
                        table: table.into(),
                        have,
                        want: info.len(),
                    }
                    .into());
                }
            }
        }
        Ok(info)
    }
}

#[cfg(test)]
#[path = "tests/rows_convert_tests.rs"]
mod convert_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_and_values() {
        assert_eq!(col_kind("int", "int(10) unsigned"), ColKind::Unsigned(32));
        assert_eq!(
            col_kind("enum", "enum('a','it''s')"),
            ColKind::Enum(vec!["a".into(), "it's".into()])
        );
        let u = BinlogValue::Value(My::Int(-1));
        assert_eq!(
            convert(Some(&u), &ColKind::Unsigned(32)),
            Value::from(4_294_967_295u64)
        );
        let e = BinlogValue::Value(My::Int(2));
        assert_eq!(
            convert(Some(&e), &ColKind::Enum(vec!["x".into(), "y".into()])),
            Value::from("y")
        );
        let s = BinlogValue::Value(My::Bytes(vec![0b101]));
        assert_eq!(
            convert(
                Some(&s),
                &ColKind::Set(vec!["a".into(), "b".into(), "c".into()])
            ),
            Value::from("a,c")
        );
        assert_eq!(convert(None, &ColKind::Plain), Value::Null);
        assert_eq!(
            convert(
                Some(&BinlogValue::Value(My::Bytes(b"sku-1".to_vec()))),
                &ColKind::Plain
            ),
            Value::from("sku-1")
        );
    }
}

#[cfg(test)]
#[path = "tests/rows_decode_tests.rs"]
mod decode_tests;
