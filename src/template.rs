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
/// character (a line break included, and the ones YAML 1.1 reads as a line break), a number, a
/// boolean, a null, a date/timestamp, the merge key or a YAML indicator (`: `, ` #`, a leading
/// `-`, `?`, `#`, `&`, `*`, `!`, `|`, `>`, `%`, `@`, backtick, `{`, `[`, `'`, `"`, `…`) sends
/// it through double quotes with backslash escapes. So the only raw line breaks the document
/// holds are its own, no value can start a new key, list item or document, and the receiver
/// reads back exactly the document's values, with their types (a string `123` or `true` stays
/// a string; `null` stays a null).
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
    yaml_string(out, k);
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
    if s.contains(" #") || s.contains("\t#") || s == "..." {
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
/// `0o777`), hex / binary, and sexagesimal (`1:2:3`).
static YAML_NUMBER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^[+-]?(0x[0-9A-Fa-f_]+|0b[0-9_]+|0o[0-7_]+|0[0-7_]+|[0-9][0-9_]*(\.[0-9_]*)?|\.[0-9][0-9_]*|[0-9_]+(:[0-9_]+)+)([eE][+-]?[0-9]+(_[0-9]+)*)?$",
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
        let simple: [(&str, HelperFn); 8] = [
            ("join", h_join),
            ("upper", h_upper),
            ("lower", h_lower),
            ("regex_escape", h_regex_escape),
            ("url_encode", h_url_encode),
            ("json", h_json),
            ("base64", h_base64),
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

/// Keys from rendered key-template output: one per non-empty line, trimmed.
pub fn split_keys(rendered: &str) -> impl Iterator<Item = &str> {
    rendered.lines().map(str::trim).filter(|l| !l.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn render(src: &str, ctx: Value) -> String {
        let mut t = Templates::new();
        t.add("t", src, ACTION_ROOTS).unwrap();
        t.render("t", &ctx).unwrap()
    }

    #[test]
    fn varnish_purge() {
        let src = "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Cache-Tags-Pattern: {{#each keys}}((^|,){{this}}(,|$)){{#unless @last}}|{{/unless}}{{/each}}\n";
        let out = render(
            src,
            json!({"keys": ["item_1", "group_2"], "endpoint": {"host": "varnish"}}),
        );
        assert_eq!(
            out,
            "PURGE / HTTP/1.1\nHost: varnish\nX-Cache-Tags-Pattern: ((^|,)item_1(,|$))|((^|,)group_2(,|$))\n"
        );
    }

    #[test]
    fn redis_clear_by_tag() {
        let src = "EVAL \"for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end\" 0 {{#each keys}}{{target.vars.prefix}}{{upper (replace this \"[^A-Za-z0-9_]\" \"_\")}} {{/each}}";
        let out = render(
            src,
            json!({"keys": ["item_1", "a-b.c"], "target": {"vars": {"prefix": "69d_"}}}),
        );
        assert_eq!(
            out,
            "EVAL \"for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end\" 0 69d_ITEM_1 69d_A_B_C "
        );
    }

    #[test]
    fn search_bulk() {
        let src = "POST /{{vars.alias}}/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{{#each docs}}{{#if (eq op \"delete\")}}{\"delete\":{\"_id\":\"{{key}}\"}}\n{{else}}{\"update\":{\"_id\":\"{{key}}\"}}\n{\"doc\":{{json doc}},\"doc_as_upsert\":true}\n{{/if}}{{/each}}";
        let out = render(
            src,
            json!({"vars": {"alias": "items_1"}, "docs": [
                {"key": "1", "op": "upsert", "doc": {"sku": "a<b&\"c\"", "price": 1.5}},
                {"key": "2", "op": "delete", "doc": {}}
            ]}),
        );
        assert_eq!(
            out,
            "POST /items_1/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"sku\":\"a<b&\\\"c\\\"\",\"price\":1.5},\"doc_as_upsert\":true}\n{\"delete\":{\"_id\":\"2\"}}\n"
        );
    }

    #[test]
    fn s3_per_key() {
        let src = "{{#each docs}}{{#if (eq op \"delete\")}}DELETE /products/{{key}}.json HTTP/1.1\n\n{{else}}PUT /products/{{key}}.json HTTP/1.1\nContent-Type: application/json\n\n{{json doc}}{{/if}}{{/each}}";
        let put = render(
            src,
            json!({"docs": [{"key": "7", "op": "upsert", "doc": {"a": [1]}}]}),
        );
        assert_eq!(
            put,
            "PUT /products/7.json HTTP/1.1\nContent-Type: application/json\n\n{\"a\":[1]}"
        );
        let del = render(
            src,
            json!({"docs": [{"key": "7", "op": "delete", "doc": {}}]}),
        );
        assert_eq!(del, "DELETE /products/7.json HTTP/1.1\n\n");
    }

    #[test]
    fn helpers() {
        assert_eq!(
            render("{{join keys \"|\"}}", json!({"keys": ["a", 1, null]})),
            "a|1|"
        );
        assert_eq!(render("{{join keys}}", json!({"keys": ["a", "b"]})), "a,b");
        assert_eq!(render("{{lower x}}{{upper x}}", json!({"x": "aB"})), "abAB");
        assert_eq!(
            render("{{regex_escape x}}", json!({"x": "a.b*"})),
            "a\\.b\\*"
        );
        assert_eq!(
            render("{{url_encode x}}", json!({"x": "a b/ü"})),
            "a%20b%2F%C3%BC"
        );
        assert_eq!(
            render("{{json x}}", json!({"x": {"a": [1, "b"]}})),
            "{\"a\":[1,\"b\"]}"
        );
        assert_eq!(render("{{json missing}}", json!({})), "null");
        assert_eq!(
            render("{{replace x \"(\\\\d+)\" \"<$1>\"}}", json!({"x": "a1b22"})),
            "a<1>b<22>"
        );
        assert_eq!(
            render("{{redis_quote x}}", json!({"x": "a \"b\"\n\\"})),
            "\"a \\\"b\\\"\\n\\\\\""
        );
        assert_eq!(
            render("{{#if (eq a 1)}}y{{else}}n{{/if}}", json!({"a": 1})),
            "y"
        );
        assert_eq!(render("{{x}}", json!({"x": "<&>"})), "<&>");
    }

    #[test]
    fn base64_helper() {
        // Standard alphabet with padding; the text form of the value is encoded.
        assert_eq!(render("{{base64 x}}", json!({"x": "hello"})), "aGVsbG8=");
        assert_eq!(render("{{base64 x}}", json!({"x": "a?b>c"})), "YT9iPmM=");
        assert_eq!(render("{{base64 x}}", json!({"x": "ab"})), "YWI=");
        assert_eq!(render("{{base64 x}}", json!({"x": ""})), "");
        // Non-ASCII: the UTF-8 bytes are encoded.
        assert_eq!(render("{{base64 x}}", json!({"x": "ü"})), "w7w=");
        // A document, as a Kinesis record would carry it.
        assert_eq!(
            render("{{base64 (json doc)}}", json!({"doc": {"a": 1}})),
            "eyJhIjoxfQ=="
        );
        // Decodes back to the same text.
        use base64::Engine as _;
        let enc = render("{{base64 x}}", json!({"x": "{\"k\":\"v/+=\"}"}));
        let dec = base64::engine::general_purpose::STANDARD
            .decode(enc)
            .unwrap();
        assert_eq!(String::from_utf8(dec).unwrap(), "{\"k\":\"v/+=\"}");
    }

    #[test]
    fn yaml_helper() {
        let y = |v: Value| render("{{{yaml doc}}}", json!({"doc": v}));

        // The document's key order, scalars verbatim.
        assert_eq!(
            y(json!({"b": 1, "a": "two", "c": true, "d": null})),
            "b: 1\na: two\nc: true\nd: null"
        );
        assert_eq!(
            y(json!({"i": 7, "f": 1.5, "neg": -3})),
            "i: 7\nf: 1.5\nneg: -3"
        );
        // A key is quoted by the same rule as a value: unquoted, `n:` is a 1.1 boolean, so the
        // receiver would lose the string the document's key is.
        assert_eq!(
            y(json!({"y": 1, "n": 2, "null": 3})),
            "\"y\": 1\n\"n\": 2\n\"null\": 3"
        );
        // A float serde_json prints without a dot takes the dot and the exponent sign a YAML
        // reader needs to see a float: `1e20` alone reads back as a string.
        assert_eq!(
            y(json!({"a": 1e20, "b": 1.5e30, "c": 1e-7, "d": 1.0, "e": -0.5, "f": 3})),
            "a: 1.0e+20\nb: 1.5e+30\nc: 1.0e-7\nd: 1.0\ne: -0.5\nf: 3"
        );
        // Top-level values of every shape.
        assert_eq!(y(json!([1, 2])), "- 1\n- 2");
        assert_eq!(y(json!({})), "{}");
        assert_eq!(y(json!([])), "[]");
        assert_eq!(y(json!(null)), "null");
        assert_eq!(y(json!("hi")), "hi");
        assert_eq!(y(json!("")), "\"\"");
        // Nesting, two spaces a level.
        assert_eq!(
            y(json!({"outer": {"inner": 1}, "list": [1, "two", true]})),
            "outer:\n  inner: 1\nlist:\n  - 1\n  - two\n  - true"
        );
        // A sequence item that is a mapping: the first entry rides the dash's line.
        assert_eq!(
            y(json!({"rows": [{"id": 1, "name": "a"}, {"id": 2}]})),
            "rows:\n  - id: 1\n    name: a\n  - id: 2"
        );
        // A sequence item that is a sequence: the dash stands alone.
        assert_eq!(
            y(json!({"a": [[1, 2], [3]]})),
            "a:\n  -\n    - 1\n    - 2\n  -\n    - 3"
        );
        // A sequence item that is a mapping holding a block of its own: the entries ride the
        // dash and its block indents past them.
        assert_eq!(
            y(json!({"rows": [{"a": {"b": 1}, "c": [2]}]})),
            "rows:\n  - a:\n      b: 1\n    c:\n      - 2"
        );
        // Empty collections inside a document.
        assert_eq!(y(json!({"o": {}, "a": []})), "o: {}\na: []");
        // A string a YAML reader would resolve as something else stays a string:
        // numbers (whole, decimal, exponent, underscores, sexagesimal, hex),
        // booleans (1.1's included), null, .inf / .nan and dates.
        assert_eq!(
            y(json!({"a": "123", "b": "1.5", "c": "1.5e3", "d": "0x1F", "e": "1:2", "f": "true", "g": "yes", "h": "On", "i": "null", "j": "~", "k": ".inf", "l": "2026-11-07", "m": "2026-11-07 10:00:00"})),
            "a: \"123\"\nb: \"1.5\"\nc: \"1.5e3\"\nd: \"0x1F\"\ne: \"1:2\"\nf: \"true\"\ng: \"yes\"\nh: \"On\"\ni: \"null\"\nj: \"~\"\nk: \".inf\"\nl: \"2026-11-07\"\nm: \"2026-11-07 10:00:00\""
        );
        // Indicators and the comment / colon contexts are quoted; the rest stay plain.
        assert_eq!(
            y(
                json!({"a": "-lead", "b": "?q", "c": "x #y", "d": "a: b", "e": "...", "f": "it's", "g": "a b"})
            ),
            "a: \"-lead\"\nb: \"?q\"\nc: \"x #y\"\nd: \"a: b\"\ne: \"...\"\nf: it's\ng: a b"
        );
        // Control characters are escaped: a value holds no raw line break.
        assert_eq!(
            y(json!({"a": "l1\nl2", "b": "a\tb", "c": "x\u{0}y"})),
            "a: \"l1\\nl2\"\nb: \"a\\tb\"\nc: \"x\\u0000y\""
        );
        // The C1 controls and the characters YAML 1.1 reads as a line break or a byte-order
        // mark are escaped too: is_control does not cover the last three.
        assert_eq!(
            y(json!({"a": "x\u{85}y", "b": "x\u{9f}y", "c": "x\u{2028}y", "d": "x\u{2029}y", "e": "x\u{feff}y"})),
            "a: \"x\\u0085y\"\nb: \"x\\u009Fy\"\nc: \"x\\u2028y\"\nd: \"x\\u2029y\"\ne: \"x\\uFEFFy\""
        );
        // 1.1's one-letter booleans, the merge key, an underscored number and a timestamp that
        // stops at the minute are quoted as well.
        assert_eq!(
            y(
                json!({"a": "y", "b": "N", "c": "<<", "d": "1_000", "e": "2026-11-07 10:00", "f": "1e5"})
            ),
            "a: \"y\"\nb: \"N\"\nc: \"<<\"\nd: \"1_000\"\ne: \"2026-11-07 10:00\"\nf: \"1e5\""
        );
        // Unicode stays as is, quoted or not.
        assert_eq!(
            y(json!({"a": "日本", "b": "x\nü"})),
            "a: 日本\nb: \"x\\nü\""
        );
        // Keys take the same treatment.
        assert_eq!(
            y(json!({": x": 1, "123": 2, "": 3})),
            r#"": x": 1
"123": 2
"": 3"#
        );
        // Both stashes give the document: the helper writes it to the output itself, so the
        // escape function never sees it (and a value with a line break stays quoted, so the
        // exemption cannot smuggle one in).
        let ctx = json!({"doc": {"a": 1, "b": "x\ny"}});
        assert_eq!(render("{{yaml doc}}", ctx.clone()), "a: 1\nb: \"x\\ny\"");
        assert_eq!(render("{{{yaml doc}}}", ctx.clone()), "a: 1\nb: \"x\\ny\"");
        // Every other helper keeps its escaping: the exemption is this helper's alone. `json`
        // escapes a line break itself; `join` and a bare value still go through the escape.
        assert_eq!(
            render("{{json doc}}", json!({"doc": ["a\nb"]})),
            "[\"a\\nb\"]"
        );
        assert_eq!(render("{{join list}}", json!({"list": ["a\nb"]})), "a b");
        assert_eq!(render("{{this}}", json!("x\ny")), "x y");
    }

    /// What a YAML reader makes of the helper's output: exactly the document, with its types.
    /// The strings are the ones item by item kept out of plain scalars, plus the shapes the
    /// emitter writes around them.
    #[test]
    fn yaml_helper_round_trips_through_a_parser() {
        let cases = vec![
            json!(null),
            json!(true),
            json!(7),
            json!(-3),
            json!(1.5),
            // Floats serde_json prints without a dot, with and without an exponent: the
            // reader must still resolve them as numbers.
            json!(1e20),
            json!(1.5e30),
            json!(1e-7),
            json!(1.0),
            json!(-0.0),
            json!(1e308),
            json!("hi"),
            json!(""),
            json!([]),
            json!({}),
            json!({"a": [1, {"b": [true, null, "x"]}], "c": {"d": {}}, "e": [[1], []]}),
            // A list item that is a mapping with a block of its own: the entries ride the
            // dash's line and the block indents past them.
            json!({"rows": [{"a": {"b": 1}, "c": [2, {"d": 3}], "e": "x: y"}, {"f": null}]}),
            // The keys as well: unquoted, `n:`, `123:` or `<<:` are a boolean, a number and a
            // merge key to a reader, not the strings the document's keys are.
            Value::Object(
                [
                    ("y", 1),
                    ("n", 2),
                    ("123", 3),
                    ("true", 4),
                    ("null", 5),
                    ("~", 6),
                    ("2026-11-07", 7),
                    ("<<", 8),
                    ("a: b", 9),
                    ("- x", 10),
                    ("", 11),
                    (" lead", 12),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), json!(v)))
                .collect(),
            ),
            Value::Object(
                [
                    ("int", "123"),
                    ("dec", "1.5"),
                    ("exp", "1e5"),
                    ("hex", "0x1F"),
                    ("oct", "0777"),
                    ("oct2", "0o17"),
                    ("bin", "0b101"),
                    ("sex", "1:2:3"),
                    ("und", "1_000"),
                    ("dot", ".5"),
                    ("neg", "-3"),
                    ("bool1", "true"),
                    ("bool2", "yes"),
                    ("bool3", "y"),
                    ("bool4", "n"),
                    ("bool5", "On"),
                    ("nil", "null"),
                    ("tilde", "~"),
                    ("inf", ".inf"),
                    ("nan", "-.nan"),
                    ("date", "2026-11-07"),
                    ("ts", "2026-11-07 10:00"),
                    ("ts2", "2026-11-07T10:00:00.5-05:00"),
                    ("merge", "<<"),
                    ("dots", "..."),
                    ("dash", "- x"),
                    ("quest", "? q"),
                    ("cmt", "a #b"),
                    ("colon", "a: b"),
                    ("colon_end", "end:"),
                    ("amp", "&anchor"),
                    ("star", "*alias"),
                    ("tag", "!!str"),
                    ("pipe", "|"),
                    ("fold", ">"),
                    ("pct", "%YAML"),
                    ("at", "@a"),
                    ("tick", "`x"),
                    ("brace", "{a}"),
                    ("brack", "[a]"),
                    ("comma", "a,b"),
                    ("sq", "'x"),
                    ("dq", "\"x"),
                    ("lead", " lead"),
                    ("trail", "trail "),
                    ("tab", "a\tb"),
                    ("nl", "l1\nl2"),
                    ("cr", "a\rb"),
                    ("nul", "a\0b"),
                    ("del", "a\u{7f}b"),
                    ("c1", "a\u{85}b"),
                    ("c1b", "a\u{9f}b"),
                    ("ls", "a\u{2028}b"),
                    ("ps", "a\u{2029}b"),
                    ("bom", "a\u{feff}b"),
                    ("jp", "日本語"),
                    ("emoji", "🎉"),
                    ("acute", "e\u{301}"),
                    ("inject", "x\nInjected: yes\n\nGET / HTTP/1.1"),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.into())))
                .collect(),
            ),
        ];
        for v in cases {
            for src in ["{{yaml doc}}", "{{{yaml doc}}}"] {
                let doc = render(src, json!({"doc": v.clone()}));
                // The document's own line breaks are the only control characters in it.
                assert!(
                    !doc.chars().any(|c| c != '\n' && c.is_control()),
                    "a raw control character in {src}:\n{doc:?}"
                );
                assert_eq!(yaml_parse(&doc), v, "read back from {src}:\n{doc}");
            }
        }
    }

    /// The document as a YAML reader sees it, as JSON.
    fn yaml_parse(doc: &str) -> Value {
        let v: yaml_serde::Value = yaml_serde::from_str(doc)
            .unwrap_or_else(|e| panic!("the document is not YAML ({e}):\n{doc}"));
        serde_json::to_value(v).unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn roots_resolve_inside_blocks() {
        let r = |s: &str| resolve_roots(s, ACTION_ROOTS);
        assert_eq!(
            r("{{vars.a}}{{#each keys}}{{vars.a}}{{this}}{{/each}}"),
            "{{vars.a}}{{#each keys}}{{@root.vars.a}}{{this}}{{/each}}"
        );
        assert_eq!(
            r("{{#each docs}}{{key}}{{upper (replace target.vars.p \"x\" \"y\")}}{{#if (eq op \"vars\")}}{{/if}}{{/each}}"),
            "{{#each docs}}{{key}}{{upper (replace @root.target.vars.p \"x\" \"y\")}}{{#if (eq op \"vars\")}}{{/if}}{{/each}}"
        );
        assert_eq!(
            r("{{#each docs as |d|}}{{d.key}}{{this.vars}}{{@root.rule}}{{../rule}}{{/each}}"),
            "{{#each docs as |d|}}{{d.key}}{{this.vars}}{{@root.rule}}{{../rule}}{{/each}}"
        );
        assert_eq!(
            r("{{#each keys}}{{!-- {{vars}} --}}{{{vars.x}}}{{/each}}"),
            "{{#each keys}}{{!-- {{vars}} --}}{{{@root.vars.x}}}{{/each}}"
        );
        assert_eq!(
            r("{{#with target}}{{x k=vars}}{{/with}}{{vars}}"),
            "{{#with target}}{{x k=@root.vars}}{{/with}}{{vars}}"
        );
    }

    #[test]
    fn root_resolution_edge_cases() {
        let r = |s: &str| resolve_roots(s, ACTION_ROOTS);
        // An inverse section is a block of its own: closing it does not end the `each`.
        // `{{^}}` (else) keeps the `each` open.
        assert_eq!(
            r("{{#each keys}}{{this}}{{^}}{{rule}}{{/each}}{{rule}}"),
            "{{#each keys}}{{this}}{{^}}{{@root.rule}}{{/each}}{{rule}}"
        );
        assert_eq!(
            render(
                "{{#each keys}}{{this}}{{^}}none of {{rule}}{{/each}}",
                json!({"keys": [], "rule": "r"})
            ),
            "none of r"
        );
        // A block helper's parameters still resolve.
        assert_eq!(
            r("{{#each docs}}{{#each target.vars.list}}{{/each}}{{/each}}"),
            "{{#each docs}}{{#each @root.target.vars.list}}{{/each}}{{/each}}"
        );
        // Block parameters named like roots stay parameters.
        assert_eq!(
            r("{{#each docs as |rule|}}{{rule.key}}{{/each}}"),
            "{{#each docs as |rule|}}{{rule.key}}{{/each}}"
        );
        // Raw blocks are left alone, content included.
        assert_eq!(
            r("{{#each keys}}{{{{raw}}}}{{vars}}{{{{/raw}}}}{{/each}}"),
            "{{#each keys}}{{{{raw}}}}{{vars}}{{{{/raw}}}}{{/each}}"
        );
    }

    #[test]
    fn interpolated_control_characters_are_neutralized() {
        let ctx = json!({"t": "x\r\nX-Evil: 1", "doc": {"a": "l1\nl2"}});
        assert_eq!(
            render("X-Title: {{t}}\n", ctx.clone()),
            "X-Title: x  X-Evil: 1\n"
        );
        assert_eq!(render("{{json doc}}", ctx.clone()), "{\"a\":\"l1\\nl2\"}");
        assert_eq!(render("{{{t}}}", ctx), "x\r\nX-Evil: 1");
    }

    #[test]
    fn partials_are_refused() {
        for src in [
            "{{> action}}",
            "{{~> step0}}",
            "{{#> layout}}x{{/layout}}",
            "{{#*inline \"p\"}}x{{/inline}}",
            "{{ > x }}",
        ] {
            assert!(
                Templates::new().add("t", src, ACTION_ROOTS).is_err(),
                "{src}"
            );
        }
        Templates::new()
            .add("t", "{{#each keys}}{{this}}{{/each}} a > b", ACTION_ROOTS)
            .unwrap();
    }

    #[test]
    fn redis_quote_joins_its_arguments() {
        assert_eq!(
            render("{{redis_quote p k}}", json!({"p": "cust:", "k": "a \"b"})),
            "\"cust:a \\\"b\""
        );
    }

    #[test]
    fn decimals_stay_exact() {
        let v: Value = serde_json::from_str("{\"p\": 12.340000}").unwrap();
        assert_eq!(render("{{json p}} {{p}}", v), "12.340000 12.340000");
    }

    #[test]
    fn key_fast_path() {
        assert_eq!(
            simple_key_template("item_{{new.product_id}}\n  group_{{ old.category_id }}x\n"),
            Some(vec![
                vec![Seg::Lit("item_".into()), Seg::Col("product_id".into())],
                vec![
                    Seg::Lit("group_".into()),
                    Seg::Col("category_id".into()),
                    Seg::Lit("x".into())
                ],
            ])
        );
        assert_eq!(simple_key_template("{{upper new.a}}"), None);
        assert_eq!(simple_key_template("{{#if new.a}}x{{/if}}"), None);
        assert_eq!(simple_key_template("{{{new.a}}}"), None);
        assert_eq!(simple_key_template("{{vars.a}}"), None);
        assert_eq!(
            split_keys(" a \n\n b\n").collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }
}
