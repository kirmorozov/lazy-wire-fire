//! JSON paths: column names that place a value in a document (`price.final`, `array[1].value`,
//! `category_ids[]`), and paths that read from a response (`hits.hits[]._id`,
//! `hits.hits[-1].sort`, `errors`).

use std::fmt;

use anyhow::{bail, Result};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Step {
    Field(String),
    /// Array element. Negative indexes count from the end (read paths only).
    Index(i64),
    /// `[]` (or `[*]`): in a column name, collect the values of all rows; in a read path, every
    /// element.
    Each,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Path(pub Vec<Step>);

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, s) in self.0.iter().enumerate() {
            match s {
                Step::Field(n) if i == 0 => write!(f, "{n}")?,
                Step::Field(n) => write!(f, ".{n}")?,
                Step::Index(n) => write!(f, "[{n}]")?,
                Step::Each => write!(f, "[]")?,
            }
        }
        Ok(())
    }
}

impl Path {
    /// Parses `a.b[2].c`, `ids[]`, `$.hits.hits[-1]`, `[0]` (a leading `$` or `$.` is the root;
    /// `$` or `.` alone is the root itself).
    pub fn parse(s: &str) -> Result<Path> {
        let mut rest = s.trim();
        if let Some(r) = rest.strip_prefix('$') {
            rest = r.strip_prefix('.').unwrap_or(r);
        } else if rest == "." {
            rest = "";
        }
        let mut steps = Vec::new();
        let b = rest.as_bytes();
        let mut i = 0;
        let mut expect_name = true;
        while i < b.len() {
            match b[i] {
                b'[' => {
                    let Some(close) = rest[i..].find(']') else {
                        bail!("path `{s}`: `[` without `]`");
                    };
                    let inner = rest[i + 1..i + close].trim();
                    if inner.is_empty() || inner == "*" {
                        steps.push(Step::Each);
                    } else {
                        match inner.parse::<i64>() {
                            Ok(n) => steps.push(Step::Index(n)),
                            Err(_) => bail!("path `{s}`: `[{inner}]` is not an index"),
                        }
                    }
                    i += close + 1;
                    expect_name = false;
                }
                b'.' => {
                    if expect_name {
                        bail!("path `{s}`: empty field name");
                    }
                    i += 1;
                    expect_name = true;
                    if i == b.len() {
                        bail!("path `{s}` ends with `.`");
                    }
                }
                _ => {
                    if !expect_name {
                        bail!("path `{s}`: `.` expected after `]`");
                    }
                    let end = rest[i..].find(['.', '[', ']']).map_or(b.len(), |n| i + n);
                    if rest.as_bytes().get(end) == Some(&b']') {
                        bail!("path `{s}`: `]` without `[`");
                    }
                    steps.push(Step::Field(rest[i..end].to_string()));
                    i = end;
                    expect_name = false;
                }
            }
        }
        Ok(Path(steps))
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether either path is a prefix of the other, i.e. they write into the same place. Any
    /// two array steps count as the same place.
    pub fn overlaps(&self, other: &Path) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(a, b)| match (a, b) {
                (Step::Field(x), Step::Field(y)) => x == y,
                (Step::Field(_), _) | (_, Step::Field(_)) => false,
                _ => true,
            })
    }

    /// Position of the `[]` step, if any.
    pub fn each_at(&self) -> Option<usize> {
        self.0.iter().position(|s| *s == Step::Each)
    }

    /// Reads every value the path designates (`[]` fans out; missing parts yield nothing).
    pub fn get<'a>(&self, v: &'a Value) -> Vec<&'a Value> {
        let mut cur = vec![v];
        for step in &self.0 {
            let mut next = Vec::new();
            for v in cur {
                match (step, v) {
                    (Step::Field(n), Value::Object(m)) => next.extend(m.get(n)),
                    (Step::Index(i), Value::Array(a)) => {
                        let idx = if *i < 0 { a.len() as i64 + i } else { *i };
                        if idx >= 0 {
                            next.extend(a.get(idx as usize));
                        }
                    }
                    (Step::Each, Value::Array(a)) => next.extend(a.iter()),
                    (Step::Each, Value::Object(m)) => next.extend(m.values()),
                    _ => {}
                }
            }
            cur = next;
        }
        cur
    }

    /// The single value at the path (first match), if any.
    pub fn get_one<'a>(&self, v: &'a Value) -> Option<&'a Value> {
        self.get(v).into_iter().next()
    }

    /// Writes `value` at the path inside `doc`, creating objects and arrays on the way
    /// (missing array elements become `null`). A value already in the way is replaced. A `[]`
    /// step appends a new element. Negative indexes are not allowed here.
    pub fn set(&self, doc: &mut Value, value: Value) {
        let mut cur = doc;
        let n = self.0.len();
        if n == 0 {
            *cur = value;
            return;
        }
        for (i, step) in self.0.iter().enumerate() {
            let last = i + 1 == n;
            match step {
                Step::Field(name) => {
                    if !cur.is_object() {
                        *cur = Value::Object(Map::new());
                    }
                    let m = cur.as_object_mut().expect("object");
                    if last {
                        m.insert(name.clone(), value);
                        return;
                    }
                    cur = m.entry(name.clone()).or_insert(Value::Null);
                }
                Step::Index(idx) => {
                    let idx = (*idx).max(0) as usize;
                    if !cur.is_array() {
                        *cur = Value::Array(Vec::new());
                    }
                    let a = cur.as_array_mut().expect("array");
                    if a.len() <= idx {
                        a.resize(idx + 1, Value::Null);
                    }
                    if last {
                        a[idx] = value;
                        return;
                    }
                    cur = &mut a[idx];
                }
                Step::Each => {
                    if !cur.is_array() {
                        *cur = Value::Array(Vec::new());
                    }
                    let a = cur.as_array_mut().expect("array");
                    if last {
                        a.push(value);
                        return;
                    }
                    a.push(Value::Null);
                    cur = a.last_mut().expect("pushed");
                }
            }
        }
    }

    /// Makes sure an array exists at this path (no `[]` steps), so an empty collection reads
    /// as `[]`.
    pub fn ensure_array(&self, doc: &mut Value) {
        let present = matches!(self.get_one(doc), Some(Value::Array(_)));
        if !present {
            self.set(doc, Value::Array(Vec::new()));
        }
    }

    pub fn prefix(&self, n: usize) -> Path {
        Path(self.0[..n].to_vec())
    }

    pub fn suffix(&self, n: usize) -> Path {
        Path(self.0[n..].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(s: &str) -> Path {
        Path::parse(s).unwrap()
    }

    #[test]
    fn parse_forms() {
        assert_eq!(p("name").0, vec![Step::Field("name".into())]);
        assert_eq!(
            p("array[1].value").0,
            vec![
                Step::Field("array".into()),
                Step::Index(1),
                Step::Field("value".into())
            ]
        );
        assert_eq!(
            p("category_ids[]").0,
            vec![Step::Field("category_ids".into()), Step::Each]
        );
        assert_eq!(p("$.hits.hits[-1].sort").to_string(), "hits.hits[-1].sort");
        assert_eq!(p("[0]").0, vec![Step::Index(0)]);
        assert!(p("$").is_root() && p(".").is_root());
        for bad in ["a..b", "a.", "a[x]", "a[1", "a]b", "a[0]b"] {
            assert!(Path::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn overlap() {
        assert!(p("price").overlaps(&p("price.final")));
        assert!(!p("price.final").overlaps(&p("price.base")));
        assert!(p("a[]").overlaps(&p("a[1].x")));
        assert!(!p("a").overlaps(&p("b")));
    }

    #[test]
    fn set_and_get() {
        let mut d = json!({});
        p("price.final").set(&mut d, json!(9.5));
        p("array[1].value").set(&mut d, json!("x"));
        p("ids[]").set(&mut d, json!(1));
        p("ids[]").set(&mut d, json!(2));
        assert_eq!(
            d,
            json!({"price": {"final": 9.5}, "array": [null, {"value": "x"}], "ids": [1, 2]})
        );
        let r =
            json!({"hits": {"hits": [{"_id": "1", "sort": ["1"]}, {"_id": "2", "sort": ["2"]}]}});
        let ids: Vec<_> = p("hits.hits[]._id").get(&r).into_iter().cloned().collect();
        assert_eq!(ids, vec![json!("1"), json!("2")]);
        assert_eq!(p("hits.hits[-1].sort").get_one(&r), Some(&json!(["2"])));
        assert!(p("hits.missing").get_one(&r).is_none());
        let mut e = json!({});
        p("tags").ensure_array(&mut e);
        assert_eq!(e, json!({"tags": []}));
    }
}
