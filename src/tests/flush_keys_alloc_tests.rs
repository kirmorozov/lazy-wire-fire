//! The flush's own key list on its way out (the hot-path audit's finding 11, the keys
//! half): `fire_keys` takes the keys **by borrow** and groups them as slices, so a flush
//! that goes out well copies no key at all — and the keys that do come back, come back
//! whole. `a_flush_that_goes_well_copies_no_key` is the allocation bound (main pays 1,002
//! allocations for the chunking this removes); `a_flush_that_stops_returns_the_keys_that
//! wait` is the accounting the owned `Vec<Vec<String>>` used to provide.
//!
//! `fire_keys` is the production function, run against a mock target of flesh and TCP on
//! its own thread and runtime: what the client pays is counted on the calling thread alone
//! (`alloc_measure` counts one thread), and the target's own bytes are not tallied against
//! the flush. No server is needed: the docs come in through `docs_override`, the path a
//! flush with pinned docs takes, so the pool here is never connected.

use super::*;
use crate::alloc_measure;
use crate::docs::Doc;
use crate::model::{RawRule, RawTarget, ResetStep, RuleDef, TargetDef};
use crate::rule::{Rule, Select, ACTION};
use crate::source::Flavor;
use crate::target::Target;
use crate::template::{Templates, ACTION_ROOTS};
use serde_json::{json, Map, Value};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Keys as `buf.keys.render` hands them out: 28 bytes each, the shape the flush's own
/// allocation tests use.
fn keys(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("order-{i:05}-customer-acme"))
        .collect()
}

/// One key's merged document, shaped like a real select row: five fields, a nested
/// object, a tombstone now and then.
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

/// A rule of flesh: two selects (so the contract says the docs are the data and the
/// context carries them), six rule variables to clone per render, an http target on
/// `urls`, and the group size a flush of `batch_max_keys` would use. `error`: why the rule
/// cannot fire, for the test that needs one. The pool is never connected: nothing here
/// reads the database.
fn flush_rule(urls: &[String], batch_max_keys: Option<usize>, error: Option<String>) -> Arc<Rule> {
    let mut vars = Map::new();
    for i in 0..6 {
        vars.insert(format!("var_{i}"), json!(format!("value {i}")));
    }
    let def = RuleDef {
        vars,
        batch_max_keys,
        ..RuleDef::placeholder(&RawRule {
            name: "orders_sync".into(),
            enabled: true,
            ..Default::default()
        })
    };
    let mut templates = Templates::default();
    templates
        .add(
            ACTION,
            "POST /hook HTTP/1.1\nContent-Type: application/json\n\n{{json docs}}",
            ACTION_ROOTS,
        )
        .expect("the template compiles");
    let tdef = TargetDef::from_row(&RawTarget {
        name: "ef_keys".into(),
        kind: Some("http".into()),
        endpoints: Some(Value::Array(urls.iter().map(|u| json!(u)).collect()).to_string()),
        vars: Some("{}".into()),
        ..Default::default()
    })
    .expect("the target parses");
    let pool = mysql_async::Pool::new("mysql://root:root@127.0.0.1:1/x");
    let target = Target::new(tdef, pool, None).expect("the target builds");
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates,
        selects: (0..2)
            .map(|i| Select {
                name: format!("s{i}"),
                sql: "SELECT id, sku, qty, note, meta FROM items WHERE id IN (:keys)".into(),
            })
            .collect(),
        triggers: Vec::new(),
        target: Some(target),
        error,
        trigger_error: None,
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime: the counted call runs on the counting thread")
}

fn env() -> Arc<Env> {
    let pool = mysql_async::Pool::new("mysql://root:root@127.0.0.1:1/x");
    Arc::new(Env::new(
        pool.clone(),
        pool,
        Flavor::MySql,
        Duration::ZERO,
        1,
    ))
}

/// A target of flesh and TCP, on its own thread and runtime: every request gets 200 and
/// the connection is kept for the next one, except a request whose bytes contain `fail_on`
/// — the socket is dropped on that one, which is how a target that is unwell answers, and
/// why its keys come back as transient rather than refused.
fn mock_target(fail_on: Option<&[u8]>) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    l.set_nonblocking(true).expect("nonblocking for tokio");
    let addr = l.local_addr().expect("an address");
    let fail_on = fail_on.map(<[u8]>::to_vec);
    std::thread::spawn(move || {
        let rt = runtime();
        rt.block_on(async move {
            let l = tokio::net::TcpListener::from_std(l).expect("the listener moves");
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let fail_on = fail_on.clone();
                tokio::spawn(async move {
                    let mut piece = [0u8; 8192];
                    loop {
                        let mut buf: Vec<u8> = Vec::new();
                        // The request is whatever arrives before 300 ms of silence; the
                        // answer does not depend on its body, only on whether it is there.
                        loop {
                            match tokio::time::timeout(Duration::from_millis(300), s.read(&mut piece))
                                .await
                            {
                                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                                Ok(Ok(m)) => buf.extend_from_slice(&piece[..m]),
                            }
                        }
                        if buf.is_empty() {
                            break;
                        }
                        if fail_on.as_deref().is_some_and(|f| buf.windows(f.len()).any(|w| w == f)) {
                            drop(s);
                            return;
                        }
                        let _ = s
                            .write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
                            )
                            .await;
                        let _ = s.flush().await;
                    }
                });
            }
        });
    });
    addr
}

/// One 1,000-key group, sent and confirmed, counted: `fire_keys` gets the keys by borrow,
/// so the only copies of them left are the ones the flush's own data must own (the docs'
/// keys, and the context's `keys` array, which is what a template renders).
///
/// The bound is the chunking copy: on main the same call costs 109,180 allocations, of
/// which 1,002 are `keys.chunks(group).map(<[String]>::to_vec)` — every key deep-copied
/// once to build the groups it then takes back out. A reintroduced copy of the list walks
/// the count past this line.
#[test]
fn a_flush_that_goes_well_copies_no_key() {
    let addr = mock_target(None);
    let rule = flush_rule(&[format!("http://{addr}/hook")], None, None);
    let env = env();
    let rt = runtime();
    let ks = keys(1000);
    let fire = |docs: Vec<Doc>| {
        let rule = rule.clone();
        let env = env.clone();
        rt.block_on(fire_keys(&env, &rule, &ks, ACTION, Value::Null, Some(docs)))
    };
    // Warm: the connection up, the caches warm, so the counted run is not the first.
    let warm = fire(docs(1000));
    assert_eq!(warm.ok, 1, "the batch went out: {:?}", warm.error);
    assert_eq!(warm.keys_done, 1000);
    assert!(warm.failed_keys.is_empty(), "nothing to retry");
    // The counted runs' own docs are built before the counts: building the data is not the
    // flush's cost, and the keys are borrowed, so they need no copy at all. Counted twice
    // and the smaller of the two quoted — a flush's cost is what it costs when nothing else
    // is happening, and the first run may still pay for a connection the warm-up did not
    // leave reusable.
    let docs_a = docs(1000);
    let docs_b = docs(1000);
    let (fired_a, cost_a) = alloc_measure::count(|| black_box(fire(docs_a)));
    let (fired_b, cost_b) = alloc_measure::count(|| black_box(fire(docs_b)));
    let (fired, whole) = if cost_a <= cost_b {
        (fired_a, cost_a)
    } else {
        (fired_b, cost_b)
    };
    eprintln!(
        "fire_keys of 1,000 keys in one group, docs pinned: {whole} allocations \
         ({cost_a} and {cost_b}; main: 109,180, of which 1,002 the chunking copy)"
    );
    assert_eq!(
        fired.ok, 1,
        "the counted flush went out too: {:?}",
        fired.error
    );
    assert_eq!(fired.keys_done, 1000);
    assert!(
        whole <= 108_600,
        "the flush is copying its key list again: {whole} allocations against the 108,178 \
         measured here and main's 109,180 (1,002 of them the chunking copy)"
    );
}

/// The accounting the owned chunks used to provide, unchanged: a target that is unwell
/// stops the flush, its own group's keys come back from `fire_docs`, and the groups that
/// were never reached wait with them — in order, whole, none lost and none doubled.
#[test]
fn a_flush_that_stops_returns_the_keys_that_wait() {
    // The second group is the one the target drops: 250 keys in groups of 100.
    let addr = mock_target(Some(b"order-00100-"));
    let rule = flush_rule(&[format!("http://{addr}/hook")], Some(100), None);
    let env = env();
    let rt = runtime();
    let ks = keys(250);
    let ds = docs(250);
    let fired = rt.block_on(fire_keys(&env, &rule, &ks, ACTION, Value::Null, Some(ds)));
    assert_eq!(fired.ok, 1, "the first group went out");
    assert_eq!(fired.keys_done, 100, "and its keys are done");
    assert!(fired.target_error, "the target, not the data, is unwell");
    assert!(fired.rejected.is_empty(), "nothing was refused by a key");
    assert_eq!(
        fired.failed_keys,
        ks[100..],
        "the group that failed and the group never reached both wait, in order"
    );
}

/// The same rule with one `stray` reset step of its own action template: the rule
/// `fire_strays` fires, which deletes keys on one endpoint.
fn strays_rule(urls: &[String], batch_max_keys: Option<usize>) -> Arc<Rule> {
    let mut r = Arc::try_unwrap(flush_rule(urls, batch_max_keys, None))
        .ok()
        .expect("the only owner");
    r.def.reset_steps = vec![ResetStep {
        name: "purge".into(),
        kind: crate::model::StepKind::Stray {
            list_template: "list0".into(),
            keys: "$.keys".into(),
            next: None,
        },
        template: Some("DELETE /hook HTTP/1.1\n\n{{json keys}}".into()),
    }];
    r.templates
        .add(
            &crate::rule::step_template(0),
            "DELETE /hook HTTP/1.1\nContent-Type: application/json\n\n{{json keys}}",
            ACTION_ROOTS,
        )
        .expect("the step template compiles");
    Arc::new(r)
}

/// The stray step groups the same borrowed way: each group is one delete on one endpoint,
/// and a target that is unwell stops the step with the groups after it waiting.
#[test]
fn a_stray_step_that_stops_returns_the_keys_that_wait() {
    let addr = mock_target(Some(b"order-00100-"));
    let rule = strays_rule(&[format!("http://{addr}/hook")], Some(100));
    let env = env();
    let rt = runtime();
    let ks = keys(250);
    let fired = rt.block_on(fire_strays(&env, &rule, 0, 0, &ks));
    assert_eq!(fired.ok, 1, "the first group of deletes went out");
    assert_eq!(fired.keys_done, 100);
    assert!(fired.target_error, "the target, not the data, is unwell");
    assert_eq!(
        fired.failed_keys,
        ks[100..],
        "the group that failed and the group never reached both wait, in order"
    );
}

/// A rule that cannot fire at all still hands every key back: the borrow is not a way for
/// an early return to lose the list.
#[test]
fn a_rule_that_cannot_fire_returns_every_key_it_was_given() {
    let env = env();
    let rt = runtime();
    let ks = keys(3);
    // A rule with no target at all: the fields are moved out of the one built above,
    // which `Templates` cannot be cloned to make twice.
    let mut no_target = Arc::try_unwrap(flush_rule(
        &["http://127.0.0.1:9/hook".to_string()],
        None,
        None,
    ))
    .ok()
    .expect("the only owner");
    no_target.target = None;
    for broken in [
        Arc::new(no_target),
        // A rule the config refuses.
        flush_rule(
            &["http://127.0.0.1:9/hook".to_string()],
            None,
            Some("the target is gone".into()),
        ),
    ] {
        let fired = rt.block_on(fire_keys(
            &env,
            &broken,
            &ks,
            ACTION,
            Value::Null,
            Some(docs(3)),
        ));
        assert!(fired.error.is_some(), "the rule says why it did not fire");
        assert_eq!(
            fired.failed_keys, ks,
            "all three keys are the caller's again"
        );
    }
}
