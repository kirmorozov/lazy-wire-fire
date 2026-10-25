//! Small MySQL-aware text helpers: named placeholders (`:keys`, `:old.col`, `:new.col`),
//! splitting a list of statements, and which tables a DDL statement replaces or reshapes.
//!
//! None of this parses SQL. A lexer skips string literals, quoted identifiers and comments, so a
//! `:name` or `;` inside them is left alone.

/// Byte ranges of the lexical elements a scan must skip, plus the significant tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// Unquoted word: keyword or identifier.
    Word(String),
    /// Backtick (or ANSI double-quote) identifier, unescaped.
    Quoted(String),
    /// `:name` / `:old.col` placeholder (name without the colon).
    Param(String),
    Dot,
    Comma,
    Semi,
    LParen,
    RParen,
    Other,
}

struct Lexed {
    tok: Tok,
    start: usize,
    end: usize,
}

/// Length of a quoted section starting at `b[i]` (the opening quote), backslash escapes and
/// doubled quotes included. Unterminated: runs to the end.
fn quoted_len(b: &[u8], i: usize) -> usize {
    let q = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == b'\\' && q != b'`' {
            j += 2;
            continue;
        }
        if b[j] == q {
            if b.get(j + 1) == Some(&q) {
                j += 2;
                continue;
            }
            return j + 1 - i;
        }
        j += 1;
    }
    b.len() - i
}

fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    if b.len() < 2 {
        return s.to_string();
    }
    let q = b[0] as char;
    let inner = &s[1..s.len() - if s.ends_with(q) { 1 } else { 0 }];
    inner.replace(&format!("{q}{q}"), &q.to_string())
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

fn lex(sql: &str) -> Vec<Lexed> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => {
                i += 1;
                continue;
            }
            b'\'' => {
                i += quoted_len(b, i);
                out.push(Lexed {
                    tok: Tok::Other,
                    start,
                    end: i,
                });
            }
            b'"' | b'`' => {
                i += quoted_len(b, i);
                out.push(Lexed {
                    tok: Tok::Quoted(unquote(&sql[start..i])),
                    start,
                    end: i,
                });
            }
            b'#' => {
                i = sql[i..].find('\n').map_or(b.len(), |n| i + n + 1);
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(|c| c.is_ascii_whitespace()) =>
            {
                i = sql[i..].find('\n').map_or(b.len(), |n| i + n + 1);
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let end = sql[i + 2..].find("*/").map_or(b.len(), |n| i + 2 + n + 2);
                // Executable comments (`/*! … */`, `/*!50001 … */`, `/*M! … */`) are SQL.
                let body = &sql[i + 2..end.saturating_sub(2).max(i + 2)];
                if let Some(rest) = body.strip_prefix('!').or_else(|| body.strip_prefix("M!")) {
                    let skip =
                        rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
                    let off = i + 2 + (body.len() - rest.len()) + skip;
                    for mut l in lex(&rest[skip..]) {
                        l.start += off;
                        l.end += off;
                        out.push(l);
                    }
                }
                i = end;
            }
            b':' if b
                .get(i + 1)
                .is_some_and(|&c| c.is_ascii_alphabetic() || c == b'_') =>
            {
                let mut j = i + 1;
                loop {
                    while j < b.len() && is_ident(b[j]) {
                        j += 1;
                    }
                    if b.get(j) == Some(&b'.') {
                        match b.get(j + 1) {
                            Some(&c) if is_ident(c) => {
                                j += 1;
                                continue;
                            }
                            Some(b'`') => {
                                j += 1 + quoted_len(b, j + 1);
                                break;
                            }
                            _ => {}
                        }
                    }
                    break;
                }
                let raw = &sql[i + 1..j];
                let name = match raw.split_once(".`") {
                    Some((head, tail)) => format!("{head}.{}", unquote(&format!("`{tail}"))),
                    None => raw.to_string(),
                };
                out.push(Lexed {
                    tok: Tok::Param(name),
                    start,
                    end: j,
                });
                i = j;
            }
            b'.' => {
                i += 1;
                out.push(Lexed {
                    tok: Tok::Dot,
                    start,
                    end: i,
                });
            }
            b',' => {
                i += 1;
                out.push(Lexed {
                    tok: Tok::Comma,
                    start,
                    end: i,
                });
            }
            b';' => {
                i += 1;
                out.push(Lexed {
                    tok: Tok::Semi,
                    start,
                    end: i,
                });
            }
            b'(' => {
                i += 1;
                out.push(Lexed {
                    tok: Tok::LParen,
                    start,
                    end: i,
                });
            }
            b')' => {
                i += 1;
                out.push(Lexed {
                    tok: Tok::RParen,
                    start,
                    end: i,
                });
            }
            c if is_ident(c) => {
                while i < b.len() && is_ident(b[i]) {
                    i += 1;
                }
                out.push(Lexed {
                    tok: Tok::Word(sql[start..i].to_string()),
                    start,
                    end: i,
                });
            }
            _ => {
                // One UTF-8 character.
                i += sql[i..].chars().next().map_or(1, char::len_utf8);
                out.push(Lexed {
                    tok: Tok::Other,
                    start,
                    end: i,
                });
            }
        }
    }
    out
}

/// A named placeholder in a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placeholder {
    /// Name without the colon: `keys`, `old.entity_id`, `new.sku`.
    pub name: String,
    pub start: usize,
    pub end: usize,
}

/// Named placeholders of `sql` in text order.
pub fn placeholders(sql: &str) -> Vec<Placeholder> {
    lex(sql)
        .into_iter()
        .filter_map(|l| match l.tok {
            Tok::Param(name) => Some(Placeholder {
                name,
                start: l.start,
                end: l.end,
            }),
            _ => None,
        })
        .collect()
}

/// Replaces every placeholder with `?` repeated `count(name)` times (comma separated), returning
/// the new statement and the placeholder names in parameter order (one entry per `?`).
pub fn expand(sql: &str, count: impl Fn(&str) -> usize) -> (String, Vec<String>) {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut names = Vec::new();
    let mut last = 0;
    for p in placeholders(sql) {
        out.push_str(&sql[last..p.start]);
        let n = count(&p.name).max(1);
        for k in 0..n {
            if k > 0 {
                out.push_str(", ");
            }
            out.push('?');
            names.push(p.name.clone());
        }
        last = p.end;
    }
    out.push_str(&sql[last..]);
    (out, names)
}

/// A SQL expression as far as lexing tells: the names in it and the functions it calls.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Expression {
    /// Words (keywords and identifiers, quoted ones unquoted) in text order.
    pub words: Vec<String>,
    /// Words directly followed by `(`: function calls (and `IN (`, `AND (`, …).
    pub calls: Vec<String>,
    /// Uses `@variables`.
    pub variables: bool,
}

/// Lexes `sql` as one expression to be embedded in a statement: not empty, parentheses
/// balanced, no `;`, no `,` outside parentheses, no `?` or `:name` placeholders (the caller
/// binds its own parameters around it).
pub fn expression(sql: &str) -> Result<Expression, String> {
    let toks = lex(sql);
    if toks.is_empty() {
        return Err("empty".into());
    }
    let mut e = Expression::default();
    let mut depth = 0usize;
    for (i, l) in toks.iter().enumerate() {
        let next = toks.get(i + 1);
        match &l.tok {
            Tok::Semi => return Err("`;`: one expression, not statements".into()),
            Tok::LParen => depth += 1,
            Tok::RParen => {
                depth = depth.checked_sub(1).ok_or("unbalanced `)`")?;
            }
            Tok::Comma if depth == 0 => {
                return Err(
                    "`,` outside parentheses: one expression (combine conditions with AND / OR)"
                        .into(),
                )
            }
            Tok::Param(p) => return Err(format!("placeholder `:{p}`")),
            Tok::Word(w) | Tok::Quoted(w) => {
                e.words.push(w.clone());
                if next.is_some_and(|n| n.tok == Tok::LParen) {
                    e.calls.push(w.clone());
                }
            }
            Tok::Other => match &sql[l.start..l.end] {
                "?" => return Err("placeholder `?`".into()),
                "@" => e.variables = true,
                "=" if next.is_some_and(|n| n.start == l.end && &sql[n.start..n.end] == "=") => {
                    return Err("`==`: SQL compares with `=`".into())
                }
                _ => {}
            },
            Tok::Dot | Tok::Comma => {}
        }
    }
    if depth != 0 {
        return Err("unbalanced `(`".into());
    }
    Ok(e)
}

/// Splits `text` into statements at `;` outside literals and comments. Empty statements are
/// dropped and each one is trimmed.
pub fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    for l in lex(text) {
        if l.tok == Tok::Semi {
            out.push(text[from..l.start].trim().to_string());
            from = l.end;
        }
    }
    out.push(text[from..].trim().to_string());
    out.retain(|s| !s.is_empty() && !lex(s).is_empty());
    out
}

/// What a DDL statement does to a table it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlEffect {
    /// Content replaced or removed without row events: `TRUNCATE`, `DROP`, `RENAME` (both
    /// sides), `CREATE OR REPLACE TABLE`, `EXCHANGE PARTITION`, `REPAIR`.
    Replace,
    /// Structure only (`ALTER TABLE t ADD …`, `CREATE INDEX … ON t`): column positions may move.
    Reshape,
}

/// Tables a non-row binlog statement affects, with how. `table` is `*` for a whole schema
/// (`DROP DATABASE`). Unqualified names belong to `default_schema`.
pub fn ddl_tables(query: &str, default_schema: &str) -> Vec<(String, String, DdlEffect)> {
    let toks: Vec<Tok> = lex(query).into_iter().map(|l| l.tok).collect();
    let mut p = Ddl {
        t: &toks,
        i: 0,
        schema: default_schema,
        out: Vec::new(),
    };
    p.statement();
    p.out
}

struct Ddl<'a> {
    t: &'a [Tok],
    i: usize,
    schema: &'a str,
    out: Vec<(String, String, DdlEffect)>,
}

impl Ddl<'_> {
    fn kw_at(&self, off: usize, kw: &str) -> bool {
        matches!(self.t.get(self.i + off), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
    }

    fn eat(&mut self, kw: &str) -> bool {
        let hit = self.kw_at(0, kw);
        if hit {
            self.i += 1;
        }
        hit
    }

    fn eat_if_exists(&mut self) {
        if self.kw_at(0, "IF") {
            if self.kw_at(1, "EXISTS") {
                self.i += 2;
            } else if self.kw_at(1, "NOT") && self.kw_at(2, "EXISTS") {
                self.i += 3;
            }
        }
    }

    fn ident(&mut self) -> Option<String> {
        let s = match self.t.get(self.i)? {
            Tok::Word(w) | Tok::Quoted(w) => w.clone(),
            _ => return None,
        };
        self.i += 1;
        Some(s)
    }

    fn name(&mut self) -> Option<(String, String)> {
        let first = self.ident()?;
        if self.t.get(self.i) == Some(&Tok::Dot) {
            self.i += 1;
            let second = self.ident()?;
            return Some((first, second));
        }
        Some((self.schema.to_string(), first))
    }

    fn push(&mut self, effect: DdlEffect) -> bool {
        match self.name() {
            Some((s, t)) => {
                self.out.push((s, t, effect));
                true
            }
            None => false,
        }
    }

    fn push_list(&mut self, effect: DdlEffect) {
        while self.push(effect) {
            if self.t.get(self.i) == Some(&Tok::Comma) {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    fn skip_modifiers(&mut self) {
        while self.eat("TEMPORARY")
            || self.eat("ONLINE")
            || self.eat("OFFLINE")
            || self.eat("IGNORE")
            || self.eat("UNIQUE")
            || self.eat("FULLTEXT")
            || self.eat("SPATIAL")
            || (self.kw_at(0, "OR") && self.kw_at(1, "REPLACE") && {
                self.i += 2;
                true
            })
        {}
    }

    fn statement(&mut self) {
        let Some(Tok::Word(first)) = self.t.get(self.i) else {
            return;
        };
        let first = first.to_ascii_uppercase();
        self.i += 1;
        match first.as_str() {
            "TRUNCATE" => {
                self.eat("TABLE");
                self.push(DdlEffect::Replace);
            }
            "REPAIR" => {
                let _ = self.eat("NO_WRITE_TO_BINLOG") || self.eat("LOCAL");
                if self.eat("TABLE") || self.eat("TABLES") {
                    self.push_list(DdlEffect::Replace);
                }
            }
            "RENAME" => {
                if self.eat("TABLE") || self.eat("TABLES") {
                    loop {
                        if !self.push(DdlEffect::Replace) {
                            break;
                        }
                        if !self.eat("TO") {
                            break;
                        }
                        if !self.push(DdlEffect::Replace) {
                            break;
                        }
                        if self.t.get(self.i) == Some(&Tok::Comma) {
                            self.i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            "DROP" => {
                self.skip_modifiers();
                if self.eat("TABLE") || self.eat("TABLES") {
                    self.eat_if_exists();
                    self.push_list(DdlEffect::Replace);
                } else if self.eat("DATABASE") || self.eat("SCHEMA") {
                    self.eat_if_exists();
                    if let Some(s) = self.ident() {
                        self.out.push((s, "*".into(), DdlEffect::Replace));
                    }
                } else if self.eat("INDEX") {
                    self.ident();
                    if self.eat("ON") {
                        self.push(DdlEffect::Reshape);
                    }
                }
            }
            "CREATE" => {
                // A new table holds no rows (`CREATE … SELECT` logs its rows as row events);
                // MariaDB's `CREATE OR REPLACE TABLE` drops the old one first.
                let replace = self.kw_at(0, "OR") && self.kw_at(1, "REPLACE");
                self.skip_modifiers();
                if self.eat("TABLE") {
                    self.eat_if_exists();
                    self.push(if replace {
                        DdlEffect::Replace
                    } else {
                        DdlEffect::Reshape
                    });
                } else if self.eat("INDEX") {
                    self.ident();
                    if self.eat("ON") {
                        self.push(DdlEffect::Reshape);
                    }
                } else if self.kw_at(0, "DATABASE") || self.kw_at(0, "SCHEMA") {
                    // CREATE OR REPLACE DATABASE (MariaDB) drops it first.
                }
            }
            "ALTER" => {
                self.skip_modifiers();
                if !self.eat("TABLE") {
                    return;
                }
                self.eat_if_exists();
                let Some((s, t)) = self.name() else { return };
                let mut effect = DdlEffect::Reshape;
                // Scan the rest for RENAME [TO|AS] x and EXCHANGE PARTITION p WITH TABLE x.
                while self.i < self.t.len() {
                    if self.eat("RENAME") {
                        if self.kw_at(0, "COLUMN") || self.kw_at(0, "INDEX") || self.kw_at(0, "KEY")
                        {
                            continue;
                        }
                        let _ = self.eat("TO") || self.eat("AS");
                        if self.push(DdlEffect::Replace) {
                            effect = DdlEffect::Replace;
                        }
                    } else if self.eat("EXCHANGE") {
                        while self.i < self.t.len() && !self.eat("TABLE") {
                            self.i += 1;
                        }
                        if self.push(DdlEffect::Replace) {
                            effect = DdlEffect::Replace;
                        }
                    } else if self.kw_at(0, "TRUNCATE") && self.kw_at(1, "PARTITION")
                        || self.kw_at(0, "DROP") && self.kw_at(1, "PARTITION")
                    {
                        effect = DdlEffect::Replace;
                        self.i += 2;
                    } else {
                        self.i += 1;
                    }
                }
                self.out.insert(0, (s, t, effect));
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_skip_literals_and_comments() {
        let sql = "SELECT ':keys', `a:keys`, \"x:y\" /* :no */ -- :no\n FROM t \
                   WHERE id IN (:keys) AND p = :old.parent_id AND q = :new.`odd col` # :no\n AND @a := 1";
        let names: Vec<_> = placeholders(sql).into_iter().map(|p| p.name).collect();
        assert_eq!(names, vec!["keys", "old.parent_id", "new.odd col"]);
    }

    #[test]
    fn expansion_counts() {
        let (s, names) = expand(
            "SELECT x AS _key FROM t WHERE id IN (:keys) OR p = :new.id",
            |n| {
                if n == "keys" {
                    3
                } else {
                    1
                }
            },
        );
        assert_eq!(s, "SELECT x AS _key FROM t WHERE id IN (?, ?, ?) OR p = ?");
        assert_eq!(names, vec!["keys", "keys", "keys", "new.id"]);
        // Escaped quotes do not end literals early.
        let (s, n) = expand(
            "SELECT 'it''s :keys', 'a\\':keys' WHERE k IN (:keys)",
            |_| 1,
        );
        assert_eq!(s, "SELECT 'it''s :keys', 'a\\':keys' WHERE k IN (?)");
        assert_eq!(n.len(), 1);
    }

    #[test]
    fn statements_split() {
        let parts =
            split_statements("SELECT 1 AS _key; SELECT ';' AS x -- ; c\n;\n\n ; /* only */ ;");
        assert_eq!(parts, vec!["SELECT 1 AS _key", "SELECT ';' AS x -- ; c"]);
    }

    #[test]
    fn ddl() {
        use DdlEffect::*;
        let d = |q: &str| ddl_tables(q, "app");
        assert_eq!(
            d("TRUNCATE TABLE t"),
            vec![("app".into(), "t".into(), Replace)]
        );
        assert_eq!(
            d("RENAME TABLE `t` TO old_t, s.new_t TO t"),
            vec![
                ("app".into(), "t".into(), Replace),
                ("app".into(), "old_t".into(), Replace),
                ("s".into(), "new_t".into(), Replace),
                ("app".into(), "t".into(), Replace)
            ]
        );
        assert_eq!(
            d("ALTER TABLE t ADD INDEX i (a)"),
            vec![("app".into(), "t".into(), Reshape)]
        );
        assert_eq!(
            d("ALTER TABLE t RENAME COLUMN a TO b"),
            vec![("app".into(), "t".into(), Reshape)]
        );
        assert_eq!(
            d("ALTER TABLE t RENAME TO u"),
            vec![
                ("app".into(), "t".into(), Replace),
                ("app".into(), "u".into(), Replace)
            ]
        );
        assert_eq!(
            d("DROP TABLE IF EXISTS a, `b`"),
            vec![
                ("app".into(), "a".into(), Replace),
                ("app".into(), "b".into(), Replace)
            ]
        );
        assert_eq!(
            d("CREATE TABLE IF NOT EXISTS x (a int)"),
            vec![("app".into(), "x".into(), Reshape)]
        );
        assert_eq!(
            d("CREATE OR REPLACE TABLE x (a int)"),
            vec![("app".into(), "x".into(), Replace)]
        );
        assert_eq!(
            d("CREATE UNIQUE INDEX i ON x (a)"),
            vec![("app".into(), "x".into(), Reshape)]
        );
        assert_eq!(
            d("DROP DATABASE shop"),
            vec![("shop".into(), "*".into(), Replace)]
        );
        assert_eq!(
            d("/*!40000 TRUNCATE t */"),
            vec![("app".into(), "t".into(), Replace)]
        );
        assert!(d("GRANT SELECT ON *.* TO u").is_empty());
        assert!(d("CREATE DATABASE x").is_empty());
    }

    #[test]
    fn expressions() {
        let e = expression(
            "status IN (200, 204) AND JSON_CONTAINS(body, 'false', '$.errors') -- ; ?\n/* , */",
        )
        .unwrap();
        assert_eq!(
            e.words,
            ["status", "IN", "200", "204", "AND", "JSON_CONTAINS", "body"].map(String::from)
        );
        assert_eq!(e.calls, ["IN", "JSON_CONTAINS"].map(String::from));
        assert!(!e.variables);
        assert!(expression("`text` = 'a;b' AND @x = 1").unwrap().variables);
        for bad in [
            "",
            " -- nothing",
            "1; DROP TABLE t",
            "status = ?",
            "status = :x",
            "errors == false",
            "200, 204",
            "1) OR (1",
            "(1",
        ] {
            assert!(expression(bad).is_err(), "{bad}");
        }
        assert!(expression("a != 1 AND b <= 2 AND c <=> NULL").is_ok());
    }
}
