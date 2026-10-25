//! Handlebars templates: actions (HTTP requests, Redis commands) and trigger keys.
//!
//! Output is never HTML-escaped. Helpers beyond the built-ins (`if`, `unless`, `each`, `with`,
//! `lookup`, `eq`, `ne`, `gt`, `lt`, `and`, `or`, `not`, `len`):
//!
//! * `join list sep` (default `,`), `upper s`, `lower s`;
//! * `regex_escape s`, `url_encode s` (RFC 3986 unreserved kept), `json v` (compact JSON);
//! * `replace s regex replacement` (all matches, `$1` groups), `redis_quote s` (a double-quoted
//!   redis-cli argument).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context as _, Result};
use handlebars::{
    Context, Handlebars, Helper, HelperDef, RenderContext, RenderError, RenderErrorReason,
    ScopedJson,
};
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
    let sep = h.param(1).map(|p| text(p.value())).unwrap_or_else(|| ",".into());
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
    Ok(Value::String(regex::escape(&text(param(h, 0, "regex_escape")?))))
}

fn h_url_encode(h: &Helper<'_>) -> Result<Value, RenderError> {
    Ok(Value::String(url_encode(&text(param(h, 0, "url_encode")?))))
}

fn h_json(h: &Helper<'_>) -> Result<Value, RenderError> {
    let v = h.param(0).map(|p| p.value().clone()).unwrap_or(Value::Null);
    Ok(Value::String(v.to_string()))
}

fn h_redis_quote(h: &Helper<'_>) -> Result<Value, RenderError> {
    Ok(Value::String(redis_quote(&text(param(h, 0, "redis_quote")?))))
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
        Ok(ScopedJson::Derived(Value::String(re.replace_all(&s, rep.as_str()).into_owned())))
    }
}

/// Top-level names of an action template's context.
pub const ACTION_ROOTS: &[&str] = &["keys", "docs", "rule", "target", "vars", "endpoint", "reset"];

/// Top-level names of a trigger key template's context.
pub const KEY_ROOTS: &[&str] = &["old", "new", "row", "op", "table"];

/// Makes top-level names resolve to the top level inside `{{#each}}` / `{{#with}}` blocks, the
/// way Handlebars.js does in compat mode (which handlebars-rust lacks): `{{target.vars.prefix}}`
/// inside `{{#each keys}}` becomes `{{@root.target.vars.prefix}}`. A field of the current
/// element with one of these names stays reachable as `this.<name>`.
pub fn resolve_roots(source: &str, roots: &[&str]) -> String {
    let mut out = String::with_capacity(source.len() + 32);
    let mut stack: Vec<String> = Vec::new();
    let mut rest = source;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let tag = &rest[open..];
        // Comments and raw blocks pass through untouched.
        if tag.starts_with("{{!--") || tag.starts_with("{{!") || tag.starts_with("{{{{") {
            let end_pat = if tag.starts_with("{{!--") {
                "--}}"
            } else if tag.starts_with("{{{{") {
                "}}}}"
            } else {
                "}}"
            };
            let end = tag[3..].find(end_pat).map_or(tag.len(), |n| 3 + n + end_pat.len());
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
        let scoped = stack.iter().any(|h| h == "each" || h == "with");
        let rewritten = if scoped { rewrite_tag(inner, roots) } else { inner.to_string() };
        if let Some(b) = body.strip_prefix('#') {
            let name = b.trim_start_matches(['*', '>']).split(|c: char| c.is_whitespace() || c == '~' || c == '}').next().unwrap_or("");
            stack.push(name.to_string());
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

/// Prefixes path expressions whose first segment is one of `roots` with `@root.`.
fn rewrite_tag(inner: &str, roots: &[&str]) -> String {
    let b = inner.as_bytes();
    let mut out = String::with_capacity(inner.len() + 8);
    let mut i = 0;
    let mut in_params = false; // inside `as |a b|`
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
                while j < b.len()
                    && !b[j].is_ascii_whitespace()
                    && !b"()=~{}|\"'".contains(&b[j])
                {
                    j += 1;
                }
                let tok = &inner[i..j];
                // No helper is named like a root, so only hash keys (`k=v`) and block
                // parameters (`as |x|`) are left alone.
                let is_hash_key = b.get(j) == Some(&b'=');
                let head = tok.split(['.', '/', '[']).next().unwrap_or("");
                if !in_params && !is_hash_key && roots.contains(&head) {
                    out.push_str("@root.");
                }
                out.push_str(tok);
                i = j;
            }
        }
    }
    out
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
        let mut hb = Handlebars::new();
        hb.register_escape_fn(handlebars::no_escape);
        hb.set_strict_mode(false);
        hb.set_prevent_indent(true);
        let simple: [(&str, HelperFn); 7] = [
            ("join", h_join),
            ("upper", h_upper),
            ("lower", h_lower),
            ("regex_escape", h_regex_escape),
            ("url_encode", h_url_encode),
            ("json", h_json),
            ("redis_quote", h_redis_quote),
        ];
        for (name, f) in simple {
            hb.register_helper(name, Box::new(Simple(f)));
        }
        hb.register_helper("replace", Box::<Replace>::default());
        Templates { hb }
    }

    /// Compiles `source` under `name` (top-level `roots` resolved inside blocks, see
    /// [`resolve_roots`]); the error names the template.
    pub fn add(&mut self, name: &str, source: &str, roots: &[&str]) -> Result<()> {
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
                || !col.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'$')
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
        let src = "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Magento-Tags-Pattern: {{#each keys}}((^|,){{this}}(,|$)){{#unless @last}}|{{/unless}}{{/each}}\n";
        let out = render(src, json!({"keys": ["cat_p_1", "cat_c_2"], "endpoint": {"host": "varnish"}}));
        assert_eq!(
            out,
            "PURGE / HTTP/1.1\nHost: varnish\nX-Magento-Tags-Pattern: ((^|,)cat_p_1(,|$))|((^|,)cat_c_2(,|$))\n"
        );
    }

    #[test]
    fn redis_clear_by_tag() {
        let src = "EVAL \"for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end\" 0 {{#each keys}}{{target.vars.prefix}}{{upper (replace this \"[^A-Za-z0-9_]\" \"_\")}} {{/each}}";
        let out = render(src, json!({"keys": ["cat_p_1", "a-b.c"], "target": {"vars": {"prefix": "69d_"}}}));
        assert_eq!(
            out,
            "EVAL \"for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end\" 0 69d_CAT_P_1 69d_A_B_C "
        );
    }

    #[test]
    fn search_bulk() {
        let src = "POST /{{vars.alias}}/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{{#each docs}}{{#if (eq op \"delete\")}}{\"delete\":{\"_id\":\"{{key}}\"}}\n{{else}}{\"update\":{\"_id\":\"{{key}}\"}}\n{\"doc\":{{json doc}},\"doc_as_upsert\":true}\n{{/if}}{{/each}}";
        let out = render(
            src,
            json!({"vars": {"alias": "magento2_product_1"}, "docs": [
                {"key": "1", "op": "upsert", "doc": {"sku": "a<b&\"c\"", "price": 1.5}},
                {"key": "2", "op": "delete", "doc": {}}
            ]}),
        );
        assert_eq!(
            out,
            "POST /magento2_product_1/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"sku\":\"a<b&\\\"c\\\"\",\"price\":1.5},\"doc_as_upsert\":true}\n{\"delete\":{\"_id\":\"2\"}}\n"
        );
    }

    #[test]
    fn s3_per_key() {
        let src = "{{#each docs}}{{#if (eq op \"delete\")}}DELETE /products/{{key}}.json HTTP/1.1\n\n{{else}}PUT /products/{{key}}.json HTTP/1.1\nContent-Type: application/json\n\n{{json doc}}{{/if}}{{/each}}";
        let put = render(src, json!({"docs": [{"key": "7", "op": "upsert", "doc": {"a": [1]}}]}));
        assert_eq!(put, "PUT /products/7.json HTTP/1.1\nContent-Type: application/json\n\n{\"a\":[1]}");
        let del = render(src, json!({"docs": [{"key": "7", "op": "delete", "doc": {}}]}));
        assert_eq!(del, "DELETE /products/7.json HTTP/1.1\n\n");
    }

    #[test]
    fn helpers() {
        assert_eq!(render("{{join keys \"|\"}}", json!({"keys": ["a", 1, null]})), "a|1|");
        assert_eq!(render("{{join keys}}", json!({"keys": ["a", "b"]})), "a,b");
        assert_eq!(render("{{lower x}}{{upper x}}", json!({"x": "aB"})), "abAB");
        assert_eq!(render("{{regex_escape x}}", json!({"x": "a.b*"})), "a\\.b\\*");
        assert_eq!(render("{{url_encode x}}", json!({"x": "a b/ü"})), "a%20b%2F%C3%BC");
        assert_eq!(render("{{json x}}", json!({"x": {"a": [1, "b"]}})), "{\"a\":[1,\"b\"]}");
        assert_eq!(render("{{json missing}}", json!({})), "null");
        assert_eq!(render("{{replace x \"(\\\\d+)\" \"<$1>\"}}", json!({"x": "a1b22"})), "a<1>b<22>");
        assert_eq!(render("{{redis_quote x}}", json!({"x": "a \"b\"\n\\"})), "\"a \\\"b\\\"\\n\\\\\"");
        assert_eq!(render("{{#if (eq a 1)}}y{{else}}n{{/if}}", json!({"a": 1})), "y");
        assert_eq!(render("{{x}}", json!({"x": "<&>"})), "<&>");
    }

    #[test]
    fn roots_resolve_inside_blocks() {
        let r = |s: &str| resolve_roots(s, ACTION_ROOTS);
        assert_eq!(r("{{vars.a}}{{#each keys}}{{vars.a}}{{this}}{{/each}}"), "{{vars.a}}{{#each keys}}{{@root.vars.a}}{{this}}{{/each}}");
        assert_eq!(
            r("{{#each docs}}{{key}}{{upper (replace target.vars.p \"x\" \"y\")}}{{#if (eq op \"vars\")}}{{/if}}{{/each}}"),
            "{{#each docs}}{{key}}{{upper (replace @root.target.vars.p \"x\" \"y\")}}{{#if (eq op \"vars\")}}{{/if}}{{/each}}"
        );
        assert_eq!(r("{{#each docs as |d|}}{{d.key}}{{this.vars}}{{@root.rule}}{{../rule}}{{/each}}"), "{{#each docs as |d|}}{{d.key}}{{this.vars}}{{@root.rule}}{{../rule}}{{/each}}");
        assert_eq!(r("{{#each keys}}{{!-- {{vars}} --}}{{{vars.x}}}{{/each}}"), "{{#each keys}}{{!-- {{vars}} --}}{{{@root.vars.x}}}{{/each}}");
        assert_eq!(r("{{#with target}}{{x k=vars}}{{/with}}{{vars}}"), "{{#with target}}{{x k=@root.vars}}{{/with}}{{vars}}");
    }

    #[test]
    fn decimals_stay_exact() {
        let v: Value = serde_json::from_str("{\"p\": 12.340000}").unwrap();
        assert_eq!(render("{{json p}} {{p}}", v), "12.340000 12.340000");
    }

    #[test]
    fn key_fast_path() {
        assert_eq!(
            simple_key_template("cat_p_{{new.product_id}}\n  cat_c_{{ old.category_id }}x\n"),
            Some(vec![
                vec![Seg::Lit("cat_p_".into()), Seg::Col("product_id".into())],
                vec![Seg::Lit("cat_c_".into()), Seg::Col("category_id".into()), Seg::Lit("x".into())],
            ])
        );
        assert_eq!(simple_key_template("{{upper new.a}}"), None);
        assert_eq!(simple_key_template("{{#if new.a}}x{{/if}}"), None);
        assert_eq!(simple_key_template("{{{new.a}}}"), None);
        assert_eq!(simple_key_template("{{vars.a}}"), None);
        assert_eq!(split_keys(" a \n\n b\n").collect::<Vec<_>>(), vec!["a", "b"]);
    }
}
