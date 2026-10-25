//! A target's answer as one SQL row, and the SQL that reads it: a target's `expect` (a
//! condition) and a stray listing's `keys` / `next` (expressions). The source database
//! evaluates them, so they are plain MySQL / MariaDB SQL over the derived table `r`:
//!
//! | column | http | redis | mysql / pgsql |
//! |---|---|---|---|
//! | `status` | status code | NULL | NULL |
//! | `headers` | JSON object, names in lower case | NULL | NULL |
//! | `body` | the body when it is JSON, else NULL | the reply as JSON | listings: the rows as a JSON array |
//! | `text` | the body as text | a string or integer reply as text | NULL |
//! | `affected_rows` | NULL | NULL | rows the statement changed |
//!
//! Only the columns an expression names are sent. A condition that reads nothing but `r` (no
//! subquery, variable or function outside a deterministic set) is cached per distinct small
//! response, so repeated answers cost no query.

use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use indexmap::IndexMap;
use mysql_async::consts::ColumnType;
use mysql_async::prelude::*;
use mysql_async::{Column, Conn, Params, Pool, Value as My};
use serde_json::{Map, Value};

use crate::redis::Reply;
use crate::sql;

/// The columns of `r` an expression names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Uses {
    pub status: bool,
    pub headers: bool,
    pub body: bool,
    pub text: bool,
    pub affected_rows: bool,
}

impl Uses {
    fn of(e: &sql::Expression) -> Uses {
        let has = |c: &str| e.words.iter().any(|w| w.eq_ignore_ascii_case(c));
        Uses {
            status: has("status"),
            headers: has("headers"),
            body: has("body"),
            text: has("text"),
            affected_rows: has("affected_rows"),
        }
    }

    fn or(self, o: Uses) -> Uses {
        Uses {
            status: self.status || o.status,
            headers: self.headers || o.headers,
            body: self.body || o.body,
            text: self.text || o.text,
            affected_rows: self.affected_rows || o.affected_rows,
        }
    }
}

/// One response as the values of `r`; columns an expression does not name stay `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Row {
    pub status: Option<i64>,
    pub headers: Option<String>,
    pub body: Option<String>,
    pub text: Option<String>,
    pub affected_rows: Option<u64>,
}

impl Row {
    pub fn http(uses: Uses, status: u16, headers: &[(String, String)], body: &[u8]) -> Row {
        Row {
            status: uses.status.then_some(i64::from(status)),
            headers: uses.headers.then(|| {
                let mut m = Map::new();
                for (n, v) in headers {
                    let n = n.to_ascii_lowercase();
                    match m.get_mut(&n) {
                        Some(Value::String(s)) => {
                            s.push_str(", ");
                            s.push_str(v);
                        }
                        _ => {
                            m.insert(n, Value::String(v.clone()));
                        }
                    }
                }
                Value::Object(m).to_string()
            }),
            body: (uses.body && serde_json::from_slice::<Value>(body).is_ok())
                .then(|| String::from_utf8_lossy(body).into_owned()),
            text: uses
                .text
                .then(|| String::from_utf8_lossy(body).into_owned()),
            affected_rows: None,
        }
    }

    pub fn reply(uses: Uses, reply: &Reply) -> Row {
        Row {
            body: uses.body.then(|| reply.to_json().to_string()),
            text: if uses.text {
                match reply {
                    Reply::Simple(s) => Some(s.clone()),
                    Reply::Int(i) => Some(i.to_string()),
                    Reply::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                    _ => None,
                }
            } else {
                None
            },
            ..Row::default()
        }
    }

    pub fn affected(uses: Uses, n: u64) -> Row {
        Row {
            affected_rows: uses.affected_rows.then_some(n),
            ..Row::default()
        }
    }

    /// A JSON answer as `body` (the rows of a SQL listing).
    pub fn json(uses: Uses, v: &Value) -> Row {
        Row {
            body: uses.body.then(|| v.to_string()),
            ..Row::default()
        }
    }

    /// What a registration check runs the SQL with: an empty answer of every column.
    fn sample(uses: Uses) -> Row {
        Row {
            status: uses.status.then_some(200),
            headers: uses.headers.then(|| "{}".into()),
            body: uses.body.then(|| "{}".into()),
            text: uses.text.then(String::new),
            affected_rows: uses.affected_rows.then_some(0),
        }
    }

    fn size(&self) -> usize {
        [&self.headers, &self.body, &self.text]
            .iter()
            .map(|s| s.as_ref().map_or(0, String::len))
            .sum()
    }

    fn params(&self, uses: Uses) -> Params {
        let text = |s: &Option<String>| s.clone().map_or(My::NULL, |s| My::Bytes(s.into_bytes()));
        let mut p = Vec::new();
        if uses.status {
            p.push(self.status.map_or(My::NULL, My::Int));
        }
        if uses.headers {
            p.push(text(&self.headers));
        }
        if uses.body {
            p.push(text(&self.body));
        }
        if uses.text {
            p.push(text(&self.text));
        }
        if uses.affected_rows {
            p.push(self.affected_rows.map_or(My::NULL, My::UInt));
        }
        if p.is_empty() {
            Params::Empty
        } else {
            Params::Positional(p)
        }
    }
}

/// The explicit length matters: MySQL types a parameter in a derived table's select list as
/// at most 65535 bytes otherwise and truncates longer answers (a page of search hits).
const TEXT: &str = "CAST(? AS CHAR(16777215) CHARACTER SET utf8mb4)";

/// `(SELECT … ) AS r` with a `?` for each column `uses` names and NULL for the others.
fn derived(uses: Uses) -> String {
    let col = |used: bool, expr: &str, name: &str| {
        if used {
            format!("{expr} AS {name}")
        } else {
            format!("NULL AS {name}")
        }
    };
    format!(
        "(SELECT {}, {}, {}, {}, {}) AS r",
        col(uses.status, "CAST(? AS SIGNED)", "status"),
        col(uses.headers, TEXT, "headers"),
        col(uses.body, TEXT, "body"),
        col(uses.text, TEXT, "text"),
        col(uses.affected_rows, "CAST(? AS UNSIGNED)", "affected_rows"),
    )
}

/// Lexes one expression over `r`.
pub fn check(src: &str) -> Result<sql::Expression> {
    sql::expression(src).map_err(|e| {
        anyhow!("{e} (a SQL expression over the response row `r`: status, headers, body, text, affected_rows)")
    })
}

/// Functions whose result depends on their arguments alone (and words followed by `(` that
/// are operators). A condition calling anything else is not cached.
const DETERMINISTIC: &[&str] = &[
    "AND",
    "OR",
    "NOT",
    "XOR",
    "IN",
    "OF",
    "IS",
    "LIKE",
    "REGEXP",
    "RLIKE",
    "IF",
    "IFNULL",
    "COALESCE",
    "NULLIF",
    "ISNULL",
    "CAST",
    "CONVERT",
    "BINARY",
    "LOWER",
    "UPPER",
    "LCASE",
    "UCASE",
    "LENGTH",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "OCTET_LENGTH",
    "SUBSTRING",
    "SUBSTR",
    "SUBSTRING_INDEX",
    "LEFT",
    "RIGHT",
    "LOCATE",
    "INSTR",
    "POSITION",
    "CONCAT",
    "CONCAT_WS",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "REVERSE",
    "LPAD",
    "RPAD",
    "ABS",
    "FLOOR",
    "CEIL",
    "CEILING",
    "ROUND",
    "TRUNCATE",
    "MOD",
    "SIGN",
    "GREATEST",
    "LEAST",
    "FIELD",
    "FIND_IN_SET",
    "ELT",
    "HEX",
    "REGEXP_LIKE",
    "REGEXP_SUBSTR",
    "REGEXP_INSTR",
    "REGEXP_REPLACE",
];

fn deterministic(e: &sql::Expression) -> bool {
    !e.variables
        && !e.words.iter().any(|w| w.eq_ignore_ascii_case("select"))
        && e.calls.iter().all(|c| {
            let u = c.to_ascii_uppercase();
            u.starts_with("JSON_") || DETERMINISTIC.contains(&u.as_str())
        })
}

const CACHE_ENTRIES: usize = 256;
const CACHE_MAX_BYTES: usize = 1024;

/// A condition over `r`: met when the database returns a row for `WHERE (<condition>)`.
pub struct Condition {
    pub src: String,
    pub uses: Uses,
    sql: String,
    cache: Option<Mutex<IndexMap<Row, bool>>>,
}

impl Condition {
    pub fn new(src: &str) -> Result<Condition> {
        let e = check(src)?;
        let uses = Uses::of(&e);
        Ok(Condition {
            src: src.to_string(),
            uses,
            sql: format!("SELECT 1 FROM {} WHERE ({src}\n)", derived(uses)),
            cache: deterministic(&e).then(|| Mutex::new(IndexMap::new())),
        })
    }

    /// `Err`: the database could not evaluate it.
    pub async fn met(&self, pool: &Pool, row: &Row) -> Result<bool> {
        let cacheable = self.cache.is_some() && row.size() <= CACHE_MAX_BYTES;
        if cacheable {
            let cache = self.cache.as_ref().expect("checked").lock().expect("cache");
            if let Some(m) = cache.get(row) {
                return Ok(*m);
            }
        }
        let mut conn = pool.get_conn().await?;
        let got: Option<mysql_async::Row> = conn
            .exec_first(self.sql.as_str(), row.params(self.uses))
            .await?;
        let met = got.is_some();
        if cacheable {
            let mut cache = self.cache.as_ref().expect("checked").lock().expect("cache");
            if cache.len() >= CACHE_ENTRIES {
                cache.shift_remove_index(0);
            }
            cache.insert(row.clone(), met);
        }
        Ok(met)
    }

    /// Runs it once with an empty answer: syntax, unknown columns and (on MySQL) bad JSON paths
    /// show up at registration.
    pub async fn validate(&self, conn: &mut Conn) -> Result<()> {
        let _: Option<mysql_async::Row> = conn
            .exec_first(self.sql.as_str(), Row::sample(self.uses).params(self.uses))
            .await?;
        Ok(())
    }
}

/// Expressions over `r`, evaluated together: one value each.
pub struct Values {
    pub uses: Uses,
    sql: String,
}

impl Values {
    pub fn new(exprs: &[&str]) -> Result<Values> {
        let mut uses = Uses::default();
        let mut cols = Vec::new();
        for (i, src) in exprs.iter().enumerate() {
            uses = uses.or(Uses::of(&check(src)?));
            cols.push(format!("({src}\n) AS v{i}"));
        }
        Ok(Values {
            uses,
            sql: format!("SELECT {} FROM {}", cols.join(", "), derived(uses)),
        })
    }

    /// The values as JSON: JSON text (`JSON_EXTRACT` results) is parsed, other text is a string.
    pub async fn eval(&self, pool: &Pool, row: &Row) -> Result<Vec<Value>> {
        let mut conn = pool.get_conn().await?;
        let got: mysql_async::Row = conn
            .exec_first(self.sql.as_str(), row.params(self.uses))
            .await?
            .ok_or_else(|| anyhow!("no row"))?;
        let cols = got.columns();
        Ok(got
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(i, v)| value(v, cols.get(i)))
            .collect())
    }

    pub async fn validate(&self, conn: &mut Conn) -> Result<()> {
        let _: Option<mysql_async::Row> = conn
            .exec_first(self.sql.as_str(), Row::sample(self.uses).params(self.uses))
            .await?;
        Ok(())
    }
}

fn value(v: My, col: Option<&Column>) -> Value {
    let numeric = col.is_some_and(|c| {
        matches!(
            c.column_type(),
            ColumnType::MYSQL_TYPE_NEWDECIMAL
                | ColumnType::MYSQL_TYPE_DECIMAL
                | ColumnType::MYSQL_TYPE_TINY
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_LONGLONG
                | ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_FLOAT
                | ColumnType::MYSQL_TYPE_DOUBLE
        )
    });
    match v {
        My::Bytes(b) if !numeric => serde_json::from_slice(&b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into_owned())),
        other => crate::docs::to_json(other, col),
    }
}

/// A target's `expect`. Empty: http needs a 2xx status, redis any reply but an error, SQL
/// targets a statement that ran. A condition that does not name `status` keeps the 2xx rule.
pub struct Expect {
    cond: Option<Condition>,
}

impl Expect {
    pub fn new(src: Option<&str>) -> Result<Expect> {
        Ok(Expect {
            cond: src
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(Condition::new)
                .transpose()?,
        })
    }

    pub fn uses(&self) -> Uses {
        self.cond.as_ref().map(|c| c.uses).unwrap_or_default()
    }

    pub async fn validate(&self, conn: &mut Conn) -> Result<()> {
        match &self.cond {
            Some(c) => c.validate(conn).await,
            None => Ok(()),
        }
    }

    /// `Ok(Err(why))`: not met; `Err`: could not be evaluated (a transient failure).
    pub async fn http(
        &self,
        pool: &Pool,
        status: u16,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<std::result::Result<(), String>> {
        let ok_2xx = (200..300).contains(&status);
        let Some(c) = &self.cond else {
            return Ok(if ok_2xx {
                Ok(())
            } else {
                Err(format!("status {status}"))
            });
        };
        if !c.uses.status && !ok_2xx {
            return Ok(Err(format!("status {status}")));
        }
        let row = Row::http(c.uses, status, headers, body);
        Ok(self
            .verdict(pool, &row)
            .await?
            .map_err(|e| format!("status {status}, {e}")))
    }

    /// Redis replies and SQL results: `row` builds the answer for the columns named.
    pub async fn row(
        &self,
        pool: &Pool,
        row: impl FnOnce(Uses) -> Row,
    ) -> Result<std::result::Result<(), String>> {
        match &self.cond {
            None => Ok(Ok(())),
            Some(c) => self.verdict(pool, &row(c.uses)).await,
        }
    }

    async fn verdict(&self, pool: &Pool, row: &Row) -> Result<std::result::Result<(), String>> {
        let c = self.cond.as_ref().expect("a condition");
        let met = c
            .met(pool, row)
            .await
            .with_context(|| format!("evaluating `expect` ({})", short(&c.src)))?;
        Ok(if met {
            Ok(())
        } else {
            Err(format!("`expect` not met ({})", short(&c.src)))
        })
    }
}

fn short(s: &str) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    match s.char_indices().nth(120) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_named() {
        let c =
            Condition::new("status IN (200, 204) AND JSON_CONTAINS(r.body, 'false', '$.errors')")
                .unwrap();
        assert_eq!(
            c.uses,
            Uses {
                status: true,
                body: true,
                ..Uses::default()
            }
        );
        assert_eq!(
            c.sql,
            "SELECT 1 FROM (SELECT CAST(? AS SIGNED) AS status, NULL AS headers, \
             CAST(? AS CHAR(16777215) CHARACTER SET utf8mb4) AS body, NULL AS text, NULL AS affected_rows) AS r \
             WHERE (status IN (200, 204) AND JSON_CONTAINS(r.body, 'false', '$.errors')\n)"
        );
        // A name inside a string literal is not a column.
        assert_eq!(
            Condition::new("'body' = 'x'").unwrap().uses,
            Uses::default()
        );
        let v = Values::new(&["JSON_EXTRACT(body, '$[1]')", "JSON_EXTRACT(body, '$[0]')"]).unwrap();
        assert!(v.uses.body && !v.uses.text);
        assert!(v.sql.starts_with("SELECT (JSON_EXTRACT(body, '$[1]')\n) AS v0, (JSON_EXTRACT(body, '$[0]')\n) AS v1 FROM"));
    }

    #[test]
    fn caching_only_for_conditions_on_r() {
        let cached = |s: &str| Condition::new(s).unwrap().cache.is_some();
        assert!(cached(
            "JSON_UNQUOTE(JSON_EXTRACT(body, '$.result')) = 'created'"
        ));
        assert!(cached(
            "text = 'OK' OR (status BETWEEN 200 AND 299 AND LOWER(text) LIKE '%ok%')"
        ));
        assert!(!cached("status = 200 AND (SELECT COUNT(*) FROM flags) = 0"));
        assert!(!cached("status = 200 AND NOW() > '2026-01-01'"));
        assert!(!cached("status = @expected"));
        assert!(!cached("mydb.ok(status)"));
    }

    #[test]
    fn one_expression_only() {
        for (src, why) in [
            ("JSON_EXTRACT(body, '$.errors') == false", "`==`"),
            ("status = 200, status = 204", "AND / OR"),
            ("1; DELETE FROM t", "`;`"),
        ] {
            let e = format!("{:#}", Condition::new(src).err().unwrap());
            assert!(e.contains(why) && e.contains("response row"), "{src}: {e}");
        }
    }

    #[test]
    fn rows_from_answers() {
        let all = Uses {
            status: true,
            headers: true,
            body: true,
            text: true,
            affected_rows: true,
        };
        let h = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Set-Cookie".to_string(), "a=1".to_string()),
            ("set-cookie".to_string(), "b=2".to_string()),
        ];
        let r = Row::http(all, 201, &h, br#"{"ok":true}"#);
        assert_eq!(r.status, Some(201));
        assert_eq!(
            r.headers.as_deref(),
            Some(r#"{"content-type":"application/json","set-cookie":"a=1, b=2"}"#)
        );
        assert_eq!(r.body.as_deref(), Some(r#"{"ok":true}"#));
        let html = Row::http(all, 200, &[], b"<html>");
        assert_eq!((html.body, html.text.as_deref()), (None, Some("<html>")));
        // Only the named columns are filled.
        let only = Row::http(
            Uses {
                status: true,
                ..Uses::default()
            },
            200,
            &h,
            b"x",
        );
        assert_eq!(
            only,
            Row {
                status: Some(200),
                ..Row::default()
            }
        );

        let scan = Reply::Array(Some(vec![
            Reply::Bulk(Some(b"0".to_vec())),
            Reply::Array(Some(vec![Reply::Bulk(Some(b"k1".to_vec()))])),
        ]));
        let r = Row::reply(all, &scan);
        assert_eq!((r.body.as_deref(), r.text), (Some(r#"["0",["k1"]]"#), None));
        let r = Row::reply(all, &Reply::Simple("OK".into()));
        assert_eq!(
            (r.body.as_deref(), r.text.as_deref()),
            (Some(r#""OK""#), Some("OK"))
        );
        assert_eq!(Row::reply(all, &Reply::Int(3)).text.as_deref(), Some("3"));
        assert_eq!(Row::affected(all, 2).affected_rows, Some(2));
    }
}
