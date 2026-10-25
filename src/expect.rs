//! A target's `expect`: which responses count as success.
//!
//! `expect` is a list of parts separated by `,`, `;`, `and` or `&&`:
//!
//! * status codes (http only): `200`, a class `2xx`, or a range `200-299`; default `2xx`;
//! * predicates on the JSON response body (http) or on the reply (redis):
//!   `<path> == <value>` or `<path> != <value>`, e.g. `errors == false`. `<value>` is JSON
//!   (`true`, `null`, `0`, `"x"`) or a bare word (a string). A missing path reads as `null`.
//!
//! Redis replies always fail on an error reply; with no predicates any other reply succeeds.

use anyhow::{bail, Result};
use serde_json::Value;

use crate::path::Path;

#[derive(Debug, Clone, PartialEq)]
enum Status {
    Exact(u16),
    Range(u16, u16),
}

#[derive(Debug, Clone, PartialEq)]
struct Predicate {
    path: Path,
    equal: bool,
    value: Value,
    text: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Expect {
    status: Vec<Status>,
    predicates: Vec<Predicate>,
}

/// Splits at separators outside quotes.
fn parts(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    let flush = |cur: &mut String, out: &mut Vec<String>| {
        let t = cur.trim();
        if !t.is_empty() {
            out.push(t.to_string());
        }
        cur.clear();
    };
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            cur.push(c);
            if c == '\\' && i + 1 < chars.len() {
                cur.push(chars[i + 1]);
                i += 2;
                continue;
            }
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            '"' | '\'' => {
                quote = Some(c);
                cur.push(c);
            }
            ',' | ';' => flush(&mut cur, &mut out),
            '&' if chars.get(i + 1) == Some(&'&') => {
                flush(&mut cur, &mut out);
                i += 1;
            }
            _ if c.is_whitespace() => {
                // ` and ` as a word.
                let rest: String = chars[i + 1..].iter().take(4).collect();
                if rest.len() >= 4
                    && rest[..3].eq_ignore_ascii_case("and")
                    && rest[3..].starts_with(char::is_whitespace)
                {
                    flush(&mut cur, &mut out);
                    i += 4;
                    continue;
                }
                cur.push(c);
            }
            _ => cur.push(c),
        }
        i += 1;
    }
    flush(&mut cur, &mut out);
    out
}

fn status(p: &str) -> Option<Status> {
    let p = p.trim();
    let b = p.as_bytes();
    if b.len() == 3 && b[0].is_ascii_digit() && b[1..].eq_ignore_ascii_case(b"xx") {
        let c = u16::from(b[0] - b'0') * 100;
        return Some(Status::Range(c, c + 99));
    }
    if let Some((a, z)) = p.split_once('-') {
        let (a, z) = (a.trim().parse().ok()?, z.trim().parse().ok()?);
        return Some(Status::Range(a, z));
    }
    p.parse().ok().filter(|n| (100..=999).contains(n)).map(Status::Exact)
}

impl Expect {
    pub fn parse(s: Option<&str>, http: bool) -> Result<Expect> {
        let mut e = Expect::default();
        for p in parts(s.unwrap_or("")) {
            if let Some(st) = status(&p) {
                if !http {
                    bail!("expect `{p}`: status codes apply to http targets only");
                }
                e.status.push(st);
                continue;
            }
            let (lhs, equal, rhs) = if let Some((l, r)) = p.split_once("==") {
                (l, true, r)
            } else if let Some((l, r)) = p.split_once("!=") {
                (l, false, r)
            } else {
                bail!("expect `{p}`: neither a status code nor `<path> == <value>`");
            };
            let path = Path::parse(lhs.trim())
                .map_err(|err| anyhow::anyhow!("expect `{p}`: {err}"))?;
            let r = rhs.trim();
            let value = if let Some(inner) = r.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')) {
                Value::String(inner.replace("\\'", "'"))
            } else {
                serde_json::from_str(r).unwrap_or_else(|_| Value::String(r.to_string()))
            };
            e.predicates.push(Predicate { path, equal, value, text: p.clone() });
        }
        Ok(e)
    }

    /// Whether a response with `status` and `body` is a success; `Err` says why not.
    pub fn check_http(&self, status: u16, body: &[u8]) -> std::result::Result<(), String> {
        let ok = if self.status.is_empty() {
            (200..300).contains(&status)
        } else {
            self.status.iter().any(|s| match *s {
                Status::Exact(c) => c == status,
                Status::Range(a, z) => (a..=z).contains(&status),
            })
        };
        if !ok {
            return Err(format!("status {status}"));
        }
        if self.predicates.is_empty() {
            return Ok(());
        }
        let v: Value = serde_json::from_slice(body)
            .map_err(|_| format!("status {status}, body is not JSON (expect has predicates)"))?;
        self.check_value(&v).map_err(|e| format!("status {status}, {e}"))
    }

    /// Predicates against a JSON value (a redis reply, or a parsed body).
    pub fn check_value(&self, v: &Value) -> std::result::Result<(), String> {
        for p in &self.predicates {
            let got = p.path.get_one(v).unwrap_or(&Value::Null);
            if json_eq(got, &p.value) != p.equal {
                return Err(format!("`{}` failed (value: {})", p.text, short(got)));
            }
        }
        Ok(())
    }
}

fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => x == y,
        },
        // A bare word against a non-string compares the text, so `count == 3` and
        // `ok == yes` both behave.
        (Value::String(s), other) | (other, Value::String(s)) if !other.is_string() => {
            other.to_string() == *s
        }
        _ => a == b,
    }
}

fn short(v: &Value) -> String {
    let s = v.to_string();
    if s.len() > 200 {
        format!("{}…", &s[..s.char_indices().nth(200).map_or(s.len(), |(i, _)| i)])
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_2xx() {
        let e = Expect::parse(None, true).unwrap();
        assert!(e.check_http(204, b"").is_ok());
        assert!(e.check_http(404, b"").is_err());
    }

    #[test]
    fn codes_and_predicates() {
        let e = Expect::parse(Some("2xx and errors == false"), true).unwrap();
        assert!(e.check_http(200, br#"{"errors":false,"items":[]}"#).is_ok());
        let err = e.check_http(200, br#"{"errors":true}"#).unwrap_err();
        assert!(err.contains("errors == false"), "{err}");
        assert!(e.check_http(200, b"not json").is_err());
        assert!(e.check_http(500, br#"{"errors":false}"#).is_err());

        let e = Expect::parse(Some("200, 204"), true).unwrap();
        assert!(e.check_http(204, b"").is_ok() && e.check_http(201, b"").is_err());
        let e = Expect::parse(Some("200-201 && result != 'noop'; n == 2"), true).unwrap();
        assert!(e.check_http(201, br#"{"result":"created","n":2.0}"#).is_ok());
        assert!(e.check_http(201, br#"{"result":"noop","n":2}"#).is_err());
        assert!(Expect::parse(Some("errors"), true).is_err());
        assert!(Expect::parse(Some("2xx"), false).is_err());
    }

    #[test]
    fn redis_reply_predicates() {
        let e = Expect::parse(Some("$ == OK"), false).unwrap();
        assert!(e.check_value(&Value::String("OK".into())).is_ok());
        assert!(e.check_value(&Value::String("QUEUED".into())).is_err());
        assert!(Expect::parse(None, false).unwrap().check_value(&Value::Null).is_ok());
    }
}
