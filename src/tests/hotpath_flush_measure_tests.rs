//! Measurement for the hot-path follow-up (worker lwf-worker-hotpath-ef, run_39cc7d210aab).
//!
//! Two questions, no production change:
//!
//! * the flush's render side (the audit's finding 10, PR-G's question): what one
//!   group of `docs` costs to put into a context and render, per endpoint, next to
//!   the 40,546 allocations a 1,000-key `load_docs` chunk costs (PR-F's number,
//!   `loaddocs_chunk_tests::a_load_docs_chunk_reports_its_allocation_cost`);
//! * PR-F's disclosed deviation (the keys half of finding 11): what the flush's two
//!   copies of its own key list cost — the armed panic fallback
//!   (`Fired::failed(keys.clone())` in `spawn_flush`) and the per-group chunking
//!   (`fire_keys`' `keys.chunks(group).map(<[String]>::to_vec)`).
//!
//! The counted calls are the production functions themselves — `context`,
//! `Templates::render`, `Target::send`, `fire_keys` — on the calling thread
//! (a current-thread runtime, so `alloc_measure::count` sees them). The mock
//! target answers from its own thread and runtime, so the server's own bytes are
//! not tallied against the flush. The printed counts are what
//! `tmp/out/lwf-worker-hotpath-ef/report.md` quotes; the assertions pin the shape
//! of the counts (a copy costs a String per key and nothing else) so a change to
//! the code or to the harness shows up as a failing test, not a changed story.

use super::*;
use crate::alloc_measure;
use crate::docs::Doc;
use crate::model::{RawRule, RawTarget, RuleDef, TargetDef};
use crate::rule::{Rule, Select, ACTION};
use crate::source::Flavor;
use crate::target::Target;
use crate::template::{Templates, ACTION_ROOTS};
use serde_json::{json, Map, Value};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// The chunk the flush takes: keys as `buf.keys.render` hands them out, of the
/// length the flush tests use (28 bytes).
fn chunk_keys(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("order-{i:05}-customer-acme"))
        .collect()
}

/// One key's merged document, shaped like a real select row: five fields, a
/// nested object, a tombstone now and then.
fn chunk_docs(n: usize) -> Vec<Doc> {
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

/// A rule of flesh: `n_selects` selects (so the contract says the docs are the
/// data and the context carries them), `n_vars` rule variables to clone per
/// render, and an http target on `urls`. The pool is never connected: this module
/// measures the render and the send, and the send goes to the mock below.
fn flush_rule(urls: &[String], n_selects: usize, n_vars: usize) -> Arc<Rule> {
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
        .add(
            ACTION,
            "POST /hook HTTP/1.1\nContent-Type: application/json\n\n{{json docs}}",
            ACTION_ROOTS,
        )
        .expect("the template compiles");
    let tdef = TargetDef::from_row(&RawTarget {
        name: "ef_measure".into(),
        kind: Some("http".into()),
        endpoints: Some(Value::Array(urls.iter().map(|u| json!(u)).collect()).to_string()),
        vars: Some("{}".into()),
        ..Default::default()
    })
    .expect("the target parses");
    let pool = mysql_async::Pool::new("mysql://root:root@127.0.0.1:34184");
    let target = Target::new(tdef, pool.clone(), None).expect("the target builds");
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime: the counted call runs on the counting thread")
}

/// What one group of `n` docs costs on its way into a render, and what the render
/// costs on top of it — the finding-10 numbers next to PR-F's `load_docs` number.
#[test]
fn rendering_a_chunk_costs_this() {
    let n = 1000;
    let ks = chunk_keys(n);
    let ds = chunk_docs(n);
    for endpoints in [1usize, 3] {
        let urls: Vec<String> = (0..endpoints)
            .map(|i| format!("http://127.0.0.1:9/hook{i}"))
            .collect();
        let rule = flush_rule(&urls, 2, 6);
        let target = rule.target.as_ref().expect("the rule has a target");
        let ep = &target.endpoints[0].ctx;
        // warm-up: one render of the same shape first (the same warm-up discipline
        // the load_docs test uses: caches warm, the counted run is not first).
        let warm = context(&rule, target, &ks, &ds, ep, &Value::Null);
        assert!(!rule.templates.render(ACTION, &warm).unwrap().is_empty());

        let (_, docs_json) = alloc_measure::count(|| {
            black_box(ds.iter().map(Doc::to_json).collect::<Vec<_>>()).len()
        });
        let (_, ctx_only) =
            alloc_measure::count(|| black_box(context(&rule, target, &ks, &ds, ep, &Value::Null)));
        let (_, render_shared) =
            alloc_measure::count(|| black_box(rule.templates.render(ACTION, &warm).unwrap()).len());
        let (len, ctx_and_render) = alloc_measure::count(|| {
            let c = context(&rule, target, &ks, &ds, ep, &Value::Null);
            black_box(rule.templates.render(ACTION, &c).unwrap()).len()
        });
        assert!(len > 1000, "the body is the docs: {len} bytes");
        eprintln!(
            "render of one {n}-doc group, {endpoints} endpoint(s): docs.to_json {docs_json} \
             allocs, context() {ctx_only} allocs, render(shared ctx) {render_shared} allocs, \
             context()+render {ctx_and_render} allocs"
        );
        // The shape of the counts: the context is at least the docs it serializes,
        // and building it fresh costs more than sharing one across the endpoints
        // (the whole of what PR-G would remove per extra endpoint).
        assert!(
            ctx_only >= docs_json,
            "the context costs less than the docs it builds: {ctx_only} < {docs_json}"
        );
        assert!(
            ctx_and_render > render_shared,
            "a fresh context costs nothing over a shared one"
        );
    }
}

/// Why `context()` costs more than the docs it builds: the `json!` macro sends
/// every expression through `to_value`, and serializing a `Value` deep-clones it
/// — so the array `Doc::to_json` just built is cloned again on its way into the
/// map. This is the half of finding 10 that costs nothing to fix (build the map
/// by hand), and this test is the measurement behind that claim.
#[test]
fn the_context_clones_what_it_was_given() {
    let n = 1000;
    let ds = chunk_docs(n);
    let (_, built) =
        alloc_measure::count(|| black_box(ds.iter().map(Doc::to_json).collect::<Vec<_>>()).len());
    let built2 = ds.iter().map(Doc::to_json).collect::<Vec<_>>();
    let (_, twice) = alloc_measure::count(|| {
        black_box(serde_json::to_value(&built2).unwrap());
    });
    eprintln!(
        "the docs array: built {built} allocs, `to_value` over a built array {twice} allocs \
         (json! does this inside context())"
    );
    assert!(
        twice > built / 2,
        "to_value over an owned array should deep-clone it: {twice} vs {built}"
    );
}

/// What the necessary half of `spawn_flush`'s count is: `buf.keys.render` builds one
/// `String` per key out of the buffer's ids, so the 417 the flush test prints splits
/// into the render (~n Strings), the armed fallback's clone (n + 1, measured by
/// `the_flush_copies_its_own_keys_twice`) and a handful of lane/Arc bookkeeping.
#[test]
fn rendering_the_flush_s_own_key_list_costs_this() {
    use crate::keyspace::{IdSets, KeySpaces};
    let n = 200;
    let mut ks = KeySpaces::new();
    let mut sets = IdSets::default();
    for i in 0..n {
        // The same shape the flush test's keys have: a text key (the tail is not a
        // number), so it lives in the text space as a tuple and renders as its text.
        let k = format!("order-{i:05}-customer-acme");
        let (s, key) = ks.locate(&k);
        sets.insert(s, key);
    }
    let warm = ks.render(sets.as_map());
    assert_eq!(warm.len(), n);
    let (_, render_cost) = alloc_measure::count(|| black_box(ks.render(sets.as_map())).len());
    eprintln!("buf.keys.render of {n} keys: {render_cost} allocs");
    assert!(
        render_cost >= n,
        "a rendered key is a String of its own: {render_cost} for {n}"
    );
}

/// The flush's two copies of its keys (finding 11's keys half, PR-F's disclosed
/// deviation): the armed fallback's `keys.clone()` and `fire_keys`' chunking.
/// Both expressions are verbatim from the code they cost.
#[test]
fn the_flush_copies_its_own_keys_twice() {
    for n in [200usize, 1000] {
        let ks = chunk_keys(n);
        // engine.rs `spawn_flush`: `Fired::failed(keys.clone())`, armed up front.
        let (_, clone_cost) = alloc_measure::count(|| black_box(ks.clone()));
        assert_eq!(
            clone_cost,
            n + 1,
            "the armed fallback's clone costs a String per key and one Vec: {clone_cost} for {n}"
        );
        // action.rs `fire_keys`: one owned Vec per group; with batch_max_keys unset a
        // 1,000-key flush is one group.
        let group = 10_000;
        let groups = ks.chunks(group).count();
        let (_, chunk_cost) = alloc_measure::count(|| {
            black_box(ks.chunks(group).map(<[String]>::to_vec).collect::<Vec<_>>())
        });
        assert_eq!(
            chunk_cost,
            n + groups + 1,
            "the chunking copies every key and builds one Vec per group: {chunk_cost} for {n}"
        );
        eprintln!(
            "flush of {n} keys: keys.clone() {clone_cost} allocs, chunking {chunk_cost} allocs"
        );
    }
}

/// A mock target of flesh and TCP, on its own thread and runtime: what the client
/// pays to send one rendered action is counted on the calling thread alone.
/// Answers 200 with a two-byte body and holds the connection for keep-alive.
fn mock_target() -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    l.set_nonblocking(true).expect("nonblocking for tokio");
    let addr = l.local_addr().expect("address");
    std::thread::spawn(move || {
        let rt = runtime();
        rt.block_on(async move {
            let l = tokio::net::TcpListener::from_std(l).expect("the listener moves");
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut piece = [0u8; 8192];
                    // Serve the connection until the client goes quiet: a keep-alive
                    // client sends its next request on the same connection, and every
                    // request gets an answer (a missed one would stall the client and
                    // the count). The request is whatever arrives before 300 ms of
                    // silence — the answer does not depend on its body.
                    loop {
                        let mut buf: Vec<u8> = Vec::new();
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

/// The send half of a chunk: one rendered action over the wire to a mock target,
/// counted alone (connect + request + reply, the client's own work) and as the
/// whole of `fire_keys` without `load_docs` (the `docs_override` path).
#[test]
fn sending_a_rendered_group_costs_this() {
    let addr = mock_target();
    let urls = vec![format!("http://{addr}/hook")];
    let rule = flush_rule(&urls, 2, 6);
    let target = rule.target.as_ref().expect("the rule has a target").clone();
    let ks = chunk_keys(1000);
    let ds = chunk_docs(1000);
    let rt = runtime();

    // The action as the flush renders it, then one send alone, warm.
    let ctx = context(
        &rule,
        &target,
        &ks,
        &ds,
        &target.endpoints[0].ctx,
        &Value::Null,
    );
    let rendered = rule.templates.render(ACTION, &ctx).expect("renders");
    let warm = rt
        .block_on(target.send(0, &rendered))
        .expect("the mock answers 200");
    assert!(matches!(warm, crate::target::Sent::Ok));
    let (_, send_cost) =
        alloc_measure::count(|| black_box(rt.block_on(target.send(0, &rendered)).unwrap()));

    // The whole of `fire_keys` without the selects: chunking, the docs handover,
    // the context, the render, the send, the bookkeeping. The second set of keys
    // and docs is built OUTSIDE the count (building them is not the flush's cost),
    // and one warm run comes first.
    let env = Arc::new(Env::new(
        mysql_async::Pool::new("mysql://root:root@127.0.0.1:34184"),
        mysql_async::Pool::new("mysql://root:root@127.0.0.1:34184"),
        Flavor::MySql,
        Duration::ZERO,
        1,
    ));
    let fire = |docs: Vec<Doc>, keys: Vec<String>| {
        let rule = rule.clone();
        let env = env.clone();
        rt.block_on(fire_keys(
            &env,
            &rule,
            keys,
            ACTION,
            Value::Null,
            Some(docs),
        ))
    };
    let f0 = fire(chunk_docs(1000), chunk_keys(1000));
    assert_eq!(f0.ok, 1, "the batch went out: {:#?}", f0.error);
    // The counted run's own keys and docs are built before the count starts.
    let count_docs = chunk_docs(1000);
    let count_keys = chunk_keys(1000);
    let (fired, whole) = alloc_measure::count(|| black_box(fire(count_docs, count_keys)));
    assert_eq!(
        fired.ok, 1,
        "the counted flush went out too: {:#?}",
        fired.error
    );
    eprintln!(
        "1,000-key flush chunk without load_docs: send alone {send_cost} allocs, \
         fire_keys(docs_override) {whole} allocs, keys_done {}",
        fired.keys_done
    );
}
