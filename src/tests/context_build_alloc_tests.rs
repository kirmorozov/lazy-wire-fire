//! The render context (the hot-path audit's finding 10): `context` builds the map a template
//! renders against, and it builds it by **moving the flush's own values in** instead of
//! running them through `json!` — which serialises a `Value` into a new one, so every entry
//! was deep-copied on its way in as well as on the way out.
//!
//! Rendering is user-visible, so the proof that matters is EQUALITY: `context_ref` is the
//! `json!`-built context exactly as it was, and every case below asserts that the hand-built
//! context is the same `Value` and that the same template renders the same bytes against it.
//! `building_a_context_copies_the_group_once` is the cost: red on main at 53,058 allocations
//! for a 1,000-doc group, green here at about 24,000 — one copy of the group, not two.
//!
//! No server is needed: the docs are built here, and nothing in this module connects.

use super::*;
use crate::alloc_measure;
use crate::docs::Doc;
use crate::model::{RawRule, RawTarget, RuleDef, TargetDef};
use crate::rule::{Rule, Select, ACTION};
use crate::target::Target;
use crate::template::{Templates, ACTION_ROOTS};
use serde_json::{json, Map, Value};
use std::hint::black_box;
use std::sync::Arc;

/// `context` as it was before this change: `json!` over the same seven roots. The reference
/// the equality cases compare against — kept on purpose, so a future edit to `context` that
/// changes what a template sees fails here rather than in someone's webhook.
fn context_ref(
    rule: &Rule,
    target: &Target,
    keys: &[String],
    docs: &[Doc],
    endpoint: &Value,
    reset: &Value,
) -> Value {
    json!({
        "keys": keys,
        // The contract: `docs` is empty when the rule has no selects (the keys are the data).
        "docs": if rule.selects.is_empty() { Vec::new() } else { docs.iter().map(Doc::to_json).collect::<Vec<_>>() },
        "rule": rule.name(),
        "target": target.ctx,
        "vars": Value::Object(rule.def.vars.clone()),
        "endpoint": endpoint,
        "reset": reset,
    })
}

/// The roots of a context, in the order they are stored: `Value ==` ignores it (an ordered
/// map compares as a map), so a test that cares about the order asks for it as a sequence.
fn roots_of(v: &Value) -> Vec<&str> {
    v.as_object()
        .expect("a context is an object")
        .keys()
        .map(String::as_str)
        .collect()
}

/// A template that reads every root, as JSON and as a field: what the two contexts must
/// render identically, byte for byte. No `#` in the body, so one raw string holds it.
const EVERY_ROOT: &str = r#"POST /hook HTTP/1.1
Content-Type: application/json

{"rule":"{{rule}}","keys":{{json keys}},"docs":{{json docs}},"vars":{{json vars}},"target":{{json target}},"endpoint":{{json endpoint}},"reset":{{json reset}}}"#;

/// A rule of flesh: `n_selects` selects (the contract says the docs are the data when there
/// are any), `n_vars` rule variables, and an http target with its own vars and one endpoint
/// per `urls`. The pool is never connected.
fn rule_with(n_selects: usize, n_vars: usize, urls: &[String]) -> Arc<Rule> {
    let mut vars = Map::new();
    for i in 0..n_vars {
        vars.insert(format!("var_{i}"), json!(format!("value {i}")));
    }
    let def = RuleDef {
        vars,
        ..RuleDef::placeholder(&RawRule {
            name: "orders_sync".into(),
            enabled: true,
            ..Default::default()
        })
    };
    let mut templates = Templates::default();
    templates
        .add(ACTION, EVERY_ROOT, ACTION_ROOTS)
        .expect("the template compiles");
    let tdef = TargetDef::from_row(&RawTarget {
        name: "ef_ctx".into(),
        kind: Some("http".into()),
        endpoints: Some(Value::Array(urls.iter().map(|u| json!(u)).collect()).to_string()),
        vars: Some(r#"{"team":"ops","retries":3,"nested":{"on":true}}"#.into()),
        ..Default::default()
    })
    .expect("the target parses");
    let pool = mysql_async::Pool::new("mysql://root:root@127.0.0.1:1/x");
    let target = Target::new(tdef, pool, None).expect("the target builds");
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates,
        selects: (0..n_selects)
            .map(|i| Select {
                name: format!("s{i}"),
                sql: "SELECT id, sku, qty, note, meta FROM items WHERE id IN (:keys)".into(),
            })
            .collect(),
        triggers: Vec::new(),
        target: Some(target),
        error: None,
        trigger_error: None,
    })
}

/// One key's merged document, the shape a select row makes: five fields, a nested object, a
/// tombstone now and then.
fn docs(n: usize) -> Vec<Doc> {
    (0..n)
        .map(|i| Doc {
            key: format!("order-{i:05}-customer-acme"),
            doc: json!({
                "id": i as i64,
                "sku": format!("sku-{i:05}-with-a-little-text"),
                "qty": (i % 97) as i64,
                "note": format!("note for item {i}: a sentence of text to make the value real"),
                "meta": {"ok": true, "n": i},
            }),
            delete: i % 50 == 0,
        })
        .collect()
}

fn keys(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("order-{i:05}-customer-acme"))
        .collect()
}

/// The seven roots, the flush's own shape, built by hand and by `json!`: the same `Value`,
/// and the same bytes out of the same template — for a Simple-key rule, a rule with vars, a
/// rule with no selects, each of a target's endpoints, a reset context, an empty group, and
/// data that needs escaping on the way to the target.
#[test]
fn a_context_is_the_context_the_rule_always_rendered() {
    let urls: Vec<String> = (0..3)
        .map(|i| format!("http://127.0.0.1:9/hook{i}"))
        .collect();
    // The reset a run hands its steps: step, keys, and the page a `for_each` is on.
    let resets = [
        Value::Null,
        json!({"step": "purge", "keys": []}),
        json!({"step": "backfill", "keys": ["order-1", "order-2"], "next": "page-3", "page": 2}),
    ];
    // Data that only survives escaping intact: quotes, a backslash, a newline, `{{`, a
    // Handlebars comment, HTML, a non-ASCII name, an emoji, and a key with a space.
    let odd = vec![
        Doc {
            key: "order \"1\" \\ {{x}} <!--y--> ".into(),
            doc: json!({
                "sku": "a\"b\\c\nd{{z}}",
                "название": "Товар — 北京",
                "emoji": "🔥 tail",
                "tab\tsep": "  ",
                "nested": {"deep": [1, "two", null, true]},
            }),
            delete: true,
        },
        Doc {
            key: "order-ünïcödé-🔥".into(),
            doc: json!({"&#38;lt;": "<script>alert(1)</script>", "n": -0.0}),
            delete: false,
        },
    ];
    for (what, n_selects, n_vars, ds) in [
        ("a Simple-key rule", 1usize, 0usize, docs(1)),
        ("a rule with vars", 1, 6, docs(1)),
        ("a group of keys", 2, 3, docs(40)),
        ("a rule with no selects", 0, 2, Vec::new()),
        ("an empty group", 2, 2, Vec::new()),
        ("data that needs escaping", 1, 2, odd.clone()),
    ] {
        let rule = rule_with(n_selects, n_vars, &urls);
        let target = rule.target.as_ref().expect("the rule has a target");
        let ks: Vec<String> = if ds.is_empty() {
            keys(3)
        } else {
            ds.iter().map(|d| d.key.clone()).collect()
        };
        for (ep, endpoint) in target.endpoints.iter().enumerate() {
            for reset in &resets {
                let built = context(&rule, target, &ks, &ds, &endpoint.ctx, reset);
                let old = context_ref(&rule, target, &ks, &ds, &endpoint.ctx, reset);
                assert_eq!(
                    built, old,
                    "{what}, endpoint {ep}, reset {reset}: the hand-built context differs"
                );
                // `Value ==` would not catch the order: `serde_json` is built with
                // `preserve_order`, so an ordered map compares without regard to it. A
                // template that iterates the context, or a future `{{json this}}`, does see
                // it — so the sequence of the roots is asserted as a sequence, against the
                // reference and against what `json!` wrote.
                let roots = roots_of;
                assert_eq!(
                    roots(&built),
                    roots(&old),
                    "{what}, endpoint {ep}, reset {reset}: the roots are in another order"
                );
                assert_eq!(
                    roots(&built),
                    vec!["keys", "docs", "rule", "target", "vars", "endpoint", "reset"],
                    "{what}, endpoint {ep}, reset {reset}: the roots themselves changed"
                );
                assert_eq!(
                    rule.templates.render(ACTION, &built).expect("renders"),
                    rule.templates.render(ACTION, &old).expect("renders"),
                    "{what}, endpoint {ep}, reset {reset}: the bytes differ"
                );
            }
        }
    }
}

/// What one group of 1,000 docs costs to put into a context: one deep copy of the group,
/// not two. On main the same call costs 53,058 allocations — 23,001 for `Doc::to_json` and
/// 29,001 for `json!` serialising the `Vec<Value>` it just built into a second copy of
/// itself. Here the entries are moved in, so the group is copied once. A reintroduced
/// `json!({...})` walks the count back over this line.
#[test]
fn building_a_context_copies_the_group_once() {
    let n = 1000;
    let rule = rule_with(2, 6, &["http://127.0.0.1:9/hook".to_string()]);
    let target = rule.target.as_ref().expect("the rule has a target");
    let ep = &target.endpoints[0].ctx;
    let (ks, ds) = (keys(n), docs(n));
    // warm: the counted run is not the first, and the data is built outside the count
    let warm = context(&rule, target, &ks, &ds, ep, &Value::Null);
    assert!(!rule.templates.render(ACTION, &warm).unwrap().is_empty());
    let (_, n) =
        alloc_measure::count(|| black_box(context(&rule, target, &ks, &ds, ep, &Value::Null)));
    eprintln!("context of a 1,000-doc group: {n} allocations (main: 53,058)");
    assert!(
        n <= 27_000,
        "the context is copying its group again: {n} allocations against main's 53,058 and \
         the ~24,000 measured here; `json!` is serialising the entries on their way in"
    );
    // The reference still costs the second copy: this is what the 27,000 above is compared
    // against, in the same run, on the same data.
    let (_, old) =
        alloc_measure::count(|| black_box(context_ref(&rule, target, &ks, &ds, ep, &Value::Null)));
    eprintln!("the same context built by `json!`: {old} allocations");
    assert!(
        old > n + 20_000,
        "the reference is not costing the second copy any more ({old} against {n}): the \
         comparison this test makes has stopped meaning anything"
    );
}
