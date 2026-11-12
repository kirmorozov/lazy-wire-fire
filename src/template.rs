//! Handlebars templates: actions (HTTP requests, Redis commands) and trigger keys.
//!
//! Output is never HTML-escaped; control characters in interpolated values become spaces (see
//! [`neutralize`]). Helpers beyond the built-ins (`if`, `unless`, `each`, `with`,
//! `lookup`, `eq`, `ne`, `gt`, `lt`, `and`, `or`, `not`, `len`):
//!
//! * `join list sep` (default `,`), `upper s`, `lower s`;
//! * `regex_escape s`, `url_encode s` (RFC 3986 unreserved kept), `json v` (compact JSON),
//!   `yaml v` (a YAML document, block style, keys in the value's order; the one helper whose
//!   output the escape function does not touch, see [`Yaml`]);
//! * `base64 s` (standard alphabet, padded; a Kinesis record's `Data`);
//! * `replace s regex replacement` (all matches, `$1` groups), `redis_quote s…` (its arguments,
//!   concatenated, as one double-quoted redis-cli argument).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::{anyhow, bail, Context as _, Result};
use handlebars::{
    Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext, RenderError,
    RenderErrorReason, ScopedJson,
};
use regex::Regex;
use serde_json::Value;

/// Text form of a value as templates print it: strings raw, `null` empty, others as JSON.
pub fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

fn param<'a>(h: &'a Helper<'_>, i: usize, name: &str) -> Result<&'a Value, RenderError> {
    h.param(i)
        .map(|p| p.value())
        .ok_or_else(|| RenderErrorReason::ParamNotFoundForIndex(leak(name), i).into())
}

/// Helper names are static strings in RenderErrorReason.
fn leak(name: &str) -> &'static str {
    match name {
        "join" => "join",
        "upper" => "upper",
        "lower" => "lower",
        "regex_escape" => "regex_escape",
        "url_encode" => "url_encode",
        "json" => "json",
        "add" => "add",
        "yaml" => "yaml",
        "replace" => "replace",
        _ => "redis_quote",
    }
}

type HelperFn = fn(&Helper<'_>) -> Result<Value, RenderError>;

struct Simple(HelperFn);

impl HelperDef for Simple {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        (self.0)(h).map(ScopedJson::Derived)
    }
}

fn h_join(h: &Helper<'_>) -> Result<Value, RenderError> {
    let list = param(h, 0, "join")?;
    let sep = h
        .param(1)
        .map(|p| text(p.value()))
        .unwrap_or_else(|| ",".into());
    let s = match list {
        Value::Array(a) => a.iter().map(text).collect::<Vec<_>>().join(&sep),
        other => text(other),
    };
    Ok(Value::String(s))
}

fn h_upper(h: &Helper<'_>) -> Result<Value, RenderError> {
    Ok(Value::String(text(param(h, 0, "upper")?).to_uppercase()))
}

fn h_lower(h: &Helper<'_>) -> Result<Value, RenderError> {
    Ok(Value::String(text(param(h, 0, "lower")?).to_lowercase()))
}

fn h_regex_escape(h: &Helper<'_>) -> Result<Value, RenderError> {
    Ok(Value::String(regex::escape(&text(param(
        h,
        0,
        "regex_escape",
    )?))))
}

fn h_url_encode(h: &Helper<'_>) -> Result<Value, RenderError> {
    Ok(Value::String(url_encode(&text(param(h, 0, "url_encode")?))))
}

/// `{{add a b}}`: the sum of two whole numbers, as a number. The query protocol's kinds number
/// the entries of a batch `…member.1`, `…member.2` — "`param.n` notation. Values of `n` are
/// integers starting from 1" (docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html) — while
/// `each` gives only `@index`, from 0, so the template of an `sns` target numbers its entries
/// `{{add @index 1}}`. A value that is not a whole number is an error rather than an empty
/// string: a batch whose entries cannot be numbered is not one AWS can answer per entry, so
/// rendering it would send a request no key can be booked against.
fn h_add(h: &Helper<'_>) -> Result<Value, RenderError> {
    fn whole(v: &Value, what: &str) -> Result<i64, RenderErrorReason> {
        match v {
            Value::Number(n) => n
                .as_i64()
                .ok_or_else(|| RenderErrorReason::Other(format!("{what} is not a whole number"))),
            // A column can hold the number as text.
            Value::String(s) => s.trim().parse::<i64>().map_err(|_| {
                RenderErrorReason::Other(format!("{what} is `{s}`, not a whole number"))
            }),
            other => Err(RenderErrorReason::Other(format!(
                "{what} is `{}`, not a whole number",
                text(other)
            ))),
        }
    }
    let a = whole(param(h, 0, "add")?, "the first number of `add`")?;
    let b = whole(param(h, 1, "add")?, "the second number of `add`")?;
    let sum = a.checked_add(b).ok_or_else(|| {
        RenderErrorReason::Other(format!("`add`: {a} + {b} is out of the range of a number"))
    })?;
    Ok(Value::Number(sum.into()))
}

fn h_json(h: &Helper<'_>) -> Result<Value, RenderError> {
    let v = h.param(0).map(|p| p.value().clone()).unwrap_or(Value::Null);
    Ok(Value::String(v.to_string()))
}

fn h_yaml(h: &Helper<'_>) -> Result<Value, RenderError> {
    let v = h.param(0).map(|p| p.value().clone()).unwrap_or(Value::Null);
    Ok(Value::String(yaml_doc(&v)))
}

/// `yaml v`: the value as a YAML document (see [`yaml_doc`]).
///
/// A document is multi-line, so this helper writes it straight to the output rather than
/// returning a value: Handlebars escapes only what the default [`HelperDef::call`] writes, so
/// overriding it is how a helper marks its output as already safe. `{{{yaml v}}}` renders
/// identically. Being exempt is sound only because [`yaml_doc`] keeps every control character
/// inside an escaped double-quoted scalar: the raw line breaks of the output are YAML's own
/// structure, never data, so no value starts a new key, list item, header or request line.
///
/// `call_inner` stays for a subexpression (`{{upper (yaml doc)}}`), where the caller's own
/// stash decides about escaping.
struct Yaml;

impl HelperDef for Yaml {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        h_yaml(h).map(ScopedJson::Derived)
    }

    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        rc: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        // What the default `call` does around its write, minus the escape: an empty output is
        // what drops a line, and this registry sets `prevent_indent`, so nothing indents.
        rc.set_content_produced(true);
        out.write(&text(&h_yaml(h)?))?;
        Ok(())
    }
}

fn h_base64(h: &Helper<'_>) -> Result<Value, RenderError> {
    use base64::Engine as _;
    let s = text(param(h, 0, "base64")?);
    Ok(Value::String(
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes()),
    ))
}

fn h_redis_quote(h: &Helper<'_>) -> Result<Value, RenderError> {
    // All parameters, concatenated, as one argument: `{{redis_quote target.vars.prefix key}}`.
    param(h, 0, "redis_quote")?;
    let joined: String = h.params().iter().map(|p| text(p.value())).collect();
    Ok(Value::String(redis_quote(&joined)))
}

/// Percent-encodes everything but RFC 3986 unreserved characters.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A YAML document body for `v`: block style, two-space indents, keys in `v`'s order, and no
/// `---` header (nothing that could read as one comes out unquoted).
///
/// A string comes out a plain scalar only when it cannot be read as anything else: a control
/// character (a line break included, and the ones YAML 1.1 reads as a line break), a number
/// (a sexagesimal like `1:20.5` included, which 1.1 resolves as a float), a boolean, a null, a
/// date/timestamp, the merge key, a YAML indicator (`: `, ` #`, a leading `-`, `?`, `#`, `&`,
/// `*`, `!`, `|`, `>`, `%`, `@`, backtick, `{`, `[`, `'`, `"`) or the start of a document-end
/// marker (`...`) sends it through double quotes with backslash escapes. So the only raw line
/// breaks the document holds are its own, no value can start a new key, list item or document,
/// and the receiver reads back exactly the document's values, with their types (a string `123`
/// or `true` stays a string; `null` stays a null). A key past 1.1's 1024-character simple-key
/// length goes on its own line in the `? key` / `: value` form ([`yaml_entry`]).
fn yaml_doc(v: &Value) -> String {
    let mut out = String::new();
    yaml_node(&mut out, v, 0);
    out
}

/// A value: a block (a mapping or sequence with entries) one line per entry at column `col`,
/// a scalar on its own. The cursor sits at the line's start when this is called.
fn yaml_node(out: &mut String, v: &Value, col: usize) {
    match v {
        Value::Object(m) if m.is_empty() => out.push_str("{}"),
        Value::Array(a) if a.is_empty() => out.push_str("[]"),
        Value::Object(m) => {
            for (i, (k, val)) in m.iter().enumerate() {
                if i > 0 {
                    out.push('\n');
                }
                out.push_str(&" ".repeat(col));
                yaml_entry(out, k, val, col);
            }
        }
        Value::Array(a) => {
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    out.push('\n');
                }
                out.push_str(&" ".repeat(col));
                yaml_item(out, item, col);
            }
        }
        _ => yaml_scalar(out, v),
    }
}

/// A `key: value` entry, the key at the cursor; a block value on the lines that follow, at
/// `col + 2`.
fn yaml_entry(out: &mut String, k: &str, v: &Value, col: usize) {
    // The key as it will stand on the line: plain, or double-quoted with escapes.
    let mut key = String::new();
    yaml_string(&mut key, k);
    // A key that shares its line with its value has to fit YAML 1.1's 1024-character limit
    // *as written*: the quotes and the `\n` escapes are what the reader scans, not the raw
    // key, so a key under the limit can still come out past it. Past it libyaml refuses the
    // `:` that follows ("mapping values are not allowed here"), in PyYAML and here alike, so
    // such a key takes the explicit form instead: `? <key>` and `: <value>`, one per line,
    // both at the entry's column.
    if key.chars().count() > YAML_MAX_PLAIN_KEY {
        out.push_str("? ");
        out.push_str(&key);
        out.push('\n');
        out.push_str(&" ".repeat(col));
    } else {
        out.push_str(&key);
    }
    match v {
        Value::Object(m) if !m.is_empty() => {
            out.push_str(":\n");
            yaml_node(out, v, col + 2);
        }
        Value::Array(a) if !a.is_empty() => {
            out.push_str(":\n");
            yaml_node(out, v, col + 2);
        }
        _ => {
            out.push_str(": ");
            yaml_scalar(out, v);
        }
    }
}

/// A `- value` item, the dash at the cursor; an item that is itself a block continues at
/// `col + 2`.
fn yaml_item(out: &mut String, v: &Value, col: usize) {
    match v {
        Value::Object(m) if !m.is_empty() => {
            // `… - a: 1`: the entry's keys ride the dash's line, at column `col + 2`.
            out.push_str("- ");
            let (k, val) = m.iter().next().unwrap();
            yaml_entry(out, k, val, col + 2);
            for (k, val) in m.iter().skip(1) {
                out.push('\n');
                out.push_str(&" ".repeat(col + 2));
                yaml_entry(out, k, val, col + 2);
            }
        }
        Value::Array(a) if !a.is_empty() => {
            // A sequence item: the dash stands alone, the items indent under it.
            out.push_str("-\n");
            yaml_node(out, v, col + 2);
        }
        _ => {
            out.push_str("- ");
            yaml_scalar(out, v);
        }
    }
}

/// A value on one line: null, booleans and numbers, strings quoted when they are not
/// plain-safe, empty collections in flow style.
fn yaml_scalar(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&yaml_number(n)),
        Value::String(s) => yaml_string(out, s),
        Value::Object(_) => out.push_str("{}"),
        Value::Array(_) => out.push_str("[]"),
    }
}

/// A JSON number the way a YAML reader resolves it as a number: an integer verbatim, a float
/// with the dot and the exponent sign that a 1.1 / 1.2 core reader wants. serde_json prints
/// `1e20`, and a reader that needs `1.0e+20` to see a float reads that back as a string.
fn yaml_number(n: &serde_json::Number) -> String {
    let s = n.to_string();
    if !n.is_f64() {
        return s;
    }
    let (mantissa, exp) = match s.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e)),
        None => (s.as_str(), None),
    };
    let mantissa = if mantissa.contains('.') {
        mantissa.to_string()
    } else {
        format!("{mantissa}.0")
    };
    match exp {
        None => mantissa,
        Some(e) => {
            let signless = !e.starts_with(['+', '-']);
            format!("{mantissa}e{}{e}", if signless { "+" } else { "" })
        }
    }
}

/// A string: plain when [`yaml_plain_safe`], otherwise double-quoted with escapes, so it
/// holds no control character raw.
fn yaml_string(out: &mut String, s: &str) {
    if yaml_plain_safe(s) {
        out.push_str(s);
        return;
    }
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // The control characters, and the ones YAML 1.1 reads as a line break or as a
            // byte-order mark: raw, they would break the line or the document.
            c if c.is_control() || YAML_BREAKS.contains(&c) => {
                out.push_str(&format!("\\u{:04X}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// What YAML 1.1 (PyYAML and libyaml both) reads as a line break inside a scalar, and the
/// byte-order mark, which 1.1 allows only at the start of a stream. `char::is_control` covers
/// C0 and C1, but none of these.
const YAML_BREAKS: [char; 3] = ['\u{2028}', '\u{2029}', '\u{feff}'];

/// How long a key that shares its line with its value may be, counting what the reader scans
/// — the quotes and escape sequences of the written key, not its raw characters. Past it a 1.1
/// reader refuses the `:` that follows, so [`yaml_entry`] writes such a key in the explicit
/// `? key` form instead. A value has no such limit.
const YAML_MAX_PLAIN_KEY: usize = 1024;

/// Plain-safe: what a YAML reader (1.1 or 1.2, any resolver's quirks included) could not
/// resolve into a number, a boolean, a null, a date, a marker or a different node.
fn yaml_plain_safe(s: &str) -> bool {
    let b = s.as_bytes();
    if s.is_empty()
        || s.chars()
            .any(|c| c.is_control() || YAML_BREAKS.contains(&c))
        || b[0] == b' '
        || b[b.len() - 1] == b' '
    {
        return false;
    }
    // An indicator at the start (or anywhere, for the flow ones) reads as a node of its own.
    const INDICATORS: &[u8] = b"-?:,[]{}#&*!|>'\"%@`=";
    if INDICATORS.contains(&b[0])
        || b.iter()
            .any(|c| matches!(c, b',' | b'[' | b']' | b'{' | b'}'))
    {
        return false;
    }
    if s.ends_with(':') || s.contains(": ") || s.contains(":\t") {
        return false;
    }
    if s.contains(" #") || s.contains("\t#") || s.starts_with("...") {
        return false;
    }
    let low = s.to_ascii_lowercase();
    if matches!(
        low.as_str(),
        "true"
            | "false"
            | "yes"
            | "no"
            | "on"
            | "off"
            | "y"
            | "n"
            | "null"
            | "~"
            | "<<"
            | ".inf"
            | "+.inf"
            | "-.inf"
            | ".nan"
            | "+.nan"
            | "-.nan"
    ) {
        return false;
    }
    !YAML_NUMBER.is_match(s) && !YAML_DATE.is_match(s)
}

/// What a YAML 1.1 reader resolves as a number: decimal with 1.1's underscores (including a
/// trailing one), a fraction after the dot (even a bare dot), an exponent, octal (`0777`,
/// `0o777`), hex / binary, and sexagesimal (`1:2:3`, and `1:20.5`, which 1.1's float resolver
/// reads as 80.5 — the fraction belongs to the sexagesimal form too).
static YAML_NUMBER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^[+-]?(0x[0-9A-Fa-f_]+|0b[0-9_]+|0o[0-7_]+|0[0-7_]+|[0-9][0-9_]*(\.[0-9_]*)?|\.[0-9][0-9_]*|[0-9_]+(:[0-9_]+)+(\.[0-9_]*)?)([eE][+-]?[0-9]+(_[0-9]+)*)?$",
    )
    .unwrap()
});

/// What a YAML 1.1 reader resolves as a date or timestamp (the time may follow `T`, `t` or a
/// space and may stop at the hour or the minute, the way PyYAML's resolver reads it).
static YAML_DATE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^[+-]?[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([Tt ][0-9]{1,2}(:[0-9]{2}){1,2}(\.[0-9]+)?( ?(Z|[+-][0-9]{1,2}(:?[0-9]{2})?))?)?$",
    )
    .unwrap()
});

/// A double-quoted redis-cli argument holding `s` exactly.
pub fn redis_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for b in s.bytes() {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out.push('"');
    out
}

/// `replace s regex replacement`, with compiled expressions cached.
#[derive(Default)]
struct Replace {
    cache: Mutex<HashMap<String, Arc<regex::Regex>>>,
}

impl HelperDef for Replace {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let s = text(param(h, 0, "replace")?);
        let pat = text(param(h, 1, "replace")?);
        let rep = h.param(2).map(|p| text(p.value())).unwrap_or_default();
        let re = {
            let mut c = self.cache.lock().expect("regex cache");
            match c.get(&pat) {
                Some(r) => r.clone(),
                None => {
                    let r = Arc::new(regex::Regex::new(&pat).map_err(|e| {
                        RenderErrorReason::Other(format!("replace: invalid regex `{pat}`: {e}"))
                    })?);
                    if c.len() > 256 {
                        c.clear();
                    }
                    c.insert(pat.clone(), r.clone());
                    r
                }
            }
        };
        Ok(ScopedJson::Derived(Value::String(
            re.replace_all(&s, rep.as_str()).into_owned(),
        )))
    }
}

/// Top-level names of an action template's context.
pub const ACTION_ROOTS: &[&str] = &[
    "keys", "docs", "rule", "target", "vars", "endpoint", "reset",
];

/// Top-level names of a trigger key template's context.
pub const KEY_ROOTS: &[&str] = &["old", "new", "row", "op", "table"];

/// Makes top-level names resolve to the top level inside `{{#each}}` / `{{#with}}` blocks, the
/// way Handlebars.js does in compat mode (which handlebars-rust lacks): `{{target.vars.prefix}}`
/// inside `{{#each keys}}` becomes `{{@root.target.vars.prefix}}`. A field of the current
/// element with one of these names stays reachable as `this.<name>`.
#[allow(clippy::manual_strip)] // offsets into `tag` are what the scan needs
pub fn resolve_roots(source: &str, roots: &[&str]) -> String {
    let mut out = String::with_capacity(source.len() + 32);
    // Open blocks: helper name and block parameters (`as |a b|`).
    let mut stack: Vec<(String, Vec<String>)> = Vec::new();
    let mut rest = source;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let tag = &rest[open..];
        // Comments and raw blocks (with their content) pass through untouched.
        let skip_to = if tag.starts_with("{{!--") {
            Some(tag[5..].find("--}}").map_or(tag.len(), |n| 5 + n + 4))
        } else if tag.starts_with("{{!") {
            Some(tag[3..].find("}}").map_or(tag.len(), |n| 3 + n + 2))
        } else if tag.starts_with("{{{{") {
            // `{{{{raw}}}} … {{{{/raw}}}}`
            Some(
                tag[4..]
                    .find("{{{{/")
                    .and_then(|n| tag[4 + n..].find("}}}}").map(|m| 4 + n + m + 4))
                    .unwrap_or(tag.len()),
            )
        } else {
            None
        };
        if let Some(end) = skip_to {
            out.push_str(&tag[..end]);
            rest = &tag[end..];
            continue;
        }
        let Some(close) = tag[2..].find("}}") else {
            out.push_str(tag);
            return out;
        };
        let mut close = 2 + close + 2;
        // Triple-stash: `{{{x}}}` closes with a third brace.
        if tag.starts_with("{{{") && tag.as_bytes().get(close) == Some(&b'}') {
            close += 1;
        }
        let inner = &tag[2..close - 2];
        let body = inner.trim_start_matches(['{', '~']).trim_start();
        let scoped = stack.iter().any(|(h, _)| h == "each" || h == "with");
        let params: Vec<String> = stack.iter().flat_map(|(_, p)| p.iter().cloned()).collect();
        let rewritten = if scoped {
            rewrite_tag(inner, roots, &params)
        } else {
            inner.to_string()
        };
        // `{{^}}` is `else` (handlebars-rust has no `{{^name}}` sections): no new block.
        if let Some(b) = body.strip_prefix('#') {
            let name = b
                .trim_start_matches(['*', '>'])
                .split(|c: char| c.is_whitespace() || c == '~' || c == '}')
                .next()
                .unwrap_or("");
            let block_params = b
                .split_once(" as |")
                .and_then(|(_, p)| p.split_once('|'))
                .map(|(p, _)| p.split_whitespace().map(str::to_string).collect())
                .unwrap_or_default();
            stack.push((name.to_string(), block_params));
        } else if body.starts_with('/') {
            stack.pop();
        }
        out.push_str("{{");
        out.push_str(&rewritten);
        out.push_str("}}");
        rest = &tag[close..];
    }
    out.push_str(rest);
    out
}

/// Partials are not supported: templates of a rule share one registry, and partials that
/// include each other recurse until the stack overflows.
pub fn check_no_partials(source: &str) -> Result<()> {
    let mut rest = source;
    while let Some(open) = rest.find("{{") {
        let body = rest[open + 2..].trim_start_matches(['{', '~']).trim_start();
        let body = body.strip_prefix('#').map_or(body, str::trim_start);
        if body.starts_with('>') || body.starts_with("*inline") {
            bail!("partials (`{{{{> …}}}}`, `{{{{#*inline}}}}`) are not supported");
        }
        rest = &rest[open + 2..];
    }
    Ok(())
}

/// Prefixes path expressions whose first segment is one of `roots` with `@root.`.
fn rewrite_tag(inner: &str, roots: &[&str], params: &[String]) -> String {
    let b = inner.as_bytes();
    let mut out = String::with_capacity(inner.len() + 8);
    let mut i = 0;
    let mut in_params = false; // inside `as |a b|`
                               // The name of a block (`{{#x}}`, `{{^x}}`, `{{/x}}`) stays as written: handlebars-rust
                               // takes no `@root.` path there, and an opening and its closing must keep matching.
    let block = inner.trim_start_matches(['{', '~']).trim_start();
    let mut skip_name = block.starts_with(['#', '^', '/']);
    while i < b.len() {
        let c = b[i];
        match c {
            b'"' | b'\'' => {
                let mut j = i + 1;
                while j < b.len() && b[j] != c {
                    j += if b[j] == b'\\' { 2 } else { 1 };
                }
                let end = (j + 1).min(b.len());
                out.push_str(&inner[i..end]);
                i = end;
            }
            b'|' => {
                in_params = !in_params;
                out.push('|');
                i += 1;
            }
            c if c.is_ascii_whitespace() || b"()=~{}#/^>&*".contains(&c) => {
                out.push(c as char);
                i += 1;
            }
            _ => {
                let mut j = i;
                while j < b.len() && !b[j].is_ascii_whitespace() && !b"()=~{}|\"'".contains(&b[j]) {
                    j += 1;
                }
                let tok = &inner[i..j];
                // No helper is named like a root, so only hash keys (`k=v`) and block
                // parameters (`as |x|`) are left alone.
                let is_hash_key = b.get(j) == Some(&b'=');
                let head = tok.split(['.', '/', '[']).next().unwrap_or("");
                let name = std::mem::take(&mut skip_name);
                // A block parameter with a root's name (`as |rule|`) is the parameter.
                if !name
                    && !in_params
                    && !is_hash_key
                    && roots.contains(&head)
                    && !params.iter().any(|p| p == head)
                {
                    out.push_str("@root.");
                }
                out.push_str(tok);
                i = j;
            }
        }
    }
    out
}

/// The escape function: no HTML escaping, but control characters in an interpolated value
/// (`{{x}}`) become spaces, so data never starts a new header, request, NDJSON line or
/// command line. `{{{x}}}` opts out; `json`, `redis_quote` and `url_encode` never emit
/// control characters, and `yaml` keeps them inside escaped double-quoted scalars — which is
/// what lets a YAML document, whose own line breaks are structure, be exempt from it
/// (see [`Yaml`]).
pub fn neutralize(s: &str) -> String {
    if s.chars().any(char::is_control) {
        s.chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect()
    } else {
        s.to_string()
    }
}

/// `sql_quote v` (a string literal, `NULL` for null) and `sql_ident name` in a SQL dialect.
struct Sql {
    dialect: crate::sqlwire::Dialect,
    ident: bool,
}

impl HelperDef for Sql {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let v = h.param(0).map(|p| p.value().clone()).unwrap_or(Value::Null);
        let out = if self.ident {
            crate::sqlwire::ident(self.dialect, &text(&v))
        } else {
            crate::sqlwire::quote(self.dialect, &v)
        };
        out.map(|s| ScopedJson::Derived(Value::String(s)))
            .map_err(|e| RenderErrorReason::Other(format!("{e:#}")).into())
    }
}

/// A set of named templates sharing the helpers.
pub struct Templates {
    hb: Handlebars<'static>,
}

impl std::fmt::Debug for Templates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Templates").finish_non_exhaustive()
    }
}

impl Default for Templates {
    fn default() -> Self {
        Self::new()
    }
}

impl Templates {
    pub fn new() -> Templates {
        Templates::for_dialect(crate::sqlwire::Dialect::MySql)
    }

    /// Templates whose `sql_quote` / `sql_ident` speak `dialect` (the rule's target's).
    pub fn for_dialect(dialect: crate::sqlwire::Dialect) -> Templates {
        let mut hb = Handlebars::new();
        hb.register_escape_fn(neutralize);
        hb.set_strict_mode(false);
        hb.set_prevent_indent(true);
        let simple: [(&str, HelperFn); 9] = [
            ("join", h_join),
            ("upper", h_upper),
            ("lower", h_lower),
            ("regex_escape", h_regex_escape),
            ("url_encode", h_url_encode),
            ("json", h_json),
            ("base64", h_base64),
            ("add", h_add),
            ("redis_quote", h_redis_quote),
        ];
        for (name, f) in simple {
            hb.register_helper(name, Box::new(Simple(f)));
        }
        hb.register_helper("yaml", Box::new(Yaml));
        hb.register_helper("replace", Box::<Replace>::default());
        hb.register_helper(
            "sql_quote",
            Box::new(Sql {
                dialect,
                ident: false,
            }),
        );
        hb.register_helper(
            "sql_ident",
            Box::new(Sql {
                dialect,
                ident: true,
            }),
        );
        Templates { hb }
    }

    /// Compiles `source` under `name` (top-level `roots` resolved inside blocks, see
    /// [`resolve_roots`]); the error names the template.
    pub fn add(&mut self, name: &str, source: &str, roots: &[&str]) -> Result<()> {
        check_no_partials(source).with_context(|| format!("template `{name}`"))?;
        self.hb
            .register_template_string(name, resolve_roots(source, roots))
            .map_err(|e| anyhow!("{e}"))
            .with_context(|| format!("template `{name}`"))
    }

    pub fn has(&self, name: &str) -> bool {
        self.hb.has_template(name)
    }

    pub fn render(&self, name: &str, ctx: &Value) -> Result<String> {
        self.hb
            .render(name, ctx)
            .map_err(|e| anyhow!("{e}"))
            .with_context(|| format!("rendering template `{name}`"))
    }
}

/// One line of a trigger's `key` template in its fast form: literal text and `{{old.c}}` /
/// `{{new.c}}` / `{{row.c}}` references only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seg {
    Lit(String),
    Col(String),
}

/// The fast form of every line of `source`, or `None` when any line needs Handlebars.
pub fn simple_key_template(source: &str) -> Option<Vec<Vec<Seg>>> {
    let mut lines = Vec::new();
    for line in source.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut segs = Vec::new();
        let mut rest = line;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                segs.push(Seg::Lit(rest[..open].to_string()));
            }
            let after = &rest[open + 2..];
            let close = after.find("}}")?;
            let inner = after[..close].trim();
            let (scope, col) = inner.split_once('.')?;
            if !matches!(scope, "old" | "new" | "row")
                || col.is_empty()
                || !col
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'$')
            {
                return None;
            }
            segs.push(Seg::Col(col.to_string()));
            rest = &after[close + 2..];
        }
        if rest.contains("}}") {
            return None;
        }
        if !rest.is_empty() {
            segs.push(Seg::Lit(rest.to_string()));
        }
        lines.push(segs);
    }
    Some(lines)
}

/// A key is one line of text: not empty, no control characters. Keys built from data that
/// break this are refused rather than passed on into requests and commands.
pub fn valid_key(k: &str) -> bool {
    !k.is_empty() && !k.bytes().any(|c| c < b' ' || c == 0x7f)
}

#[cfg(test)]
#[path = "tests/template.rs"]
mod tests;
