//! `load_docs` on the way out of a flush (the hot-path audit's finding 9): the chunk's rows
//! reach the merger without a copy of the rowset, the chunk's parameters are shared by its
//! selects, and a statement's columns are materialized once.
//!
//! The correctness half is pinned here rather than compared with itself: the docs a chunk must
//! produce are written out, including the two cases the handover could plausibly disturb — a key
//! no select answered, and a key the select answered as a delete.
//!
//! These run against a real server (`LAZYWF_TEST_DSN`), the way `load_docs` actually runs: it
//! prepares, selects and merges. Without the variable they skip. They own their current-thread
//! runtime so the counted call runs on the counting thread (`alloc_measure` counts one thread).

use super::*;
use crate::alloc_measure;
use crate::rule::{Rule, Select};
use serde_json::json;

/// Each test owns its own `lwf_*` schema (the tests of a file run in parallel, and another
/// suite may hold the lock's server at the same time), and drops it again at the end.
fn schema(tag: &str) -> String {
    format!("lwf_hotpath_f_{tag}")
}

fn dsn() -> Option<String> {
    std::env::var("LAZYWF_TEST_DSN")
        .ok()
        .filter(|d| !d.is_empty())
}

/// The same dsn-to-options conversion the engine itself uses, so the client here is the client
/// the flush uses.
fn opts(dsn: &str) -> mysql_async::Opts {
    crate::binlog::replica_opts(dsn).expect("parses the dsn")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime (the counted call must run on the counting thread)")
}

/// The suite's schema with its `items` table: `3` is deliberately missing (a key no select
/// answers) and `4` carries the `sku` the second select reports as a delete.
async fn server(dsn: &str, tag: &str, n_rows: usize) -> (Env, Rule) {
    let schema = schema(tag);
    let mut conn = mysql_async::Conn::new(opts(dsn))
        .await
        .expect("connects (the suite's server)");
    conn.exec_drop(format!("DROP DATABASE IF EXISTS `{schema}`"), ())
        .await
        .expect("drops the previous run");
    conn.exec_drop(format!("CREATE DATABASE `{schema}`"), ())
        .await
        .expect("creates the schema");
    conn.exec_drop(
        format!("CREATE TABLE `{schema}`.items (id INT PRIMARY KEY, sku VARCHAR(32), price INT)"),
        (),
    )
    .await
    .expect("creates the table");
    let mut values = Vec::new();
    for i in 1..=n_rows {
        if i == 3 {
            continue; // the key no select will answer
        }
        let sku = if i == 4 { "gone" } else { "stock" };
        values.push(format!("({i},'{sku}',{})", i * 10));
    }
    if !values.is_empty() {
        conn.exec_drop(
            format!(
                "INSERT INTO `{schema}`.items (id, sku, price) VALUES {}",
                values.join(",")
            ),
            (),
        )
        .await
        .expect("fills the table");
    }
    drop(conn);

    let pool = mysql_async::Pool::new(opts(dsn));
    let env = Env::new(
        pool.clone(),
        pool,
        crate::source::Flavor::MySql,
        std::time::Duration::ZERO,
        4,
    );
    let def = crate::model::RuleDef::placeholder(&crate::model::RawRule {
        name: "items_sync".into(),
        enabled: true,
        ..Default::default()
    });
    let rule = Rule {
        def,
        fingerprint: String::new(),
        templates: crate::template::Templates::default(),
        selects: vec![
            Select {
                name: "attrs".into(),
                sql: format!(
                    "SELECT id AS _key, sku, price FROM `{schema}`.items WHERE id IN (:keys)"
                ),
            },
            Select {
                name: "op".into(),
                sql: format!(
                    "SELECT id AS _key, CASE WHEN sku = 'gone' THEN 'delete' ELSE 'upsert' END \
                     AS _op FROM `{schema}`.items WHERE id IN (:keys)"
                ),
            },
        ],
        triggers: Vec::new(),
        target: None,
        error: None,
        trigger_error: None,
    };
    (env, rule)
}

/// No leftover schema: the e2e cleanup only knows the suite's own names.
async fn drop_schema(dsn: &str, tag: &str) {
    if let Ok(mut conn) = mysql_async::Conn::new(opts(dsn)).await {
        let _ = conn
            .exec_drop(format!("DROP DATABASE IF EXISTS `{}`", schema(tag)), ())
            .await;
    }
}

fn keys(ks: &[&str]) -> Vec<String> {
    ks.iter().map(|s| s.to_string()).collect()
}

fn rendered(ds: &[Doc]) -> Vec<String> {
    ds.iter()
        .map(|d| {
            format!(
                "{}|{}|{}",
                d.key,
                d.delete,
                serde_json::to_string(&d.doc).expect("renders")
            )
        })
        .collect()
}

/// The docs one chunk of `load_docs` must produce, written out: the answered keys carry both
/// selects' columns, the key no select answered stays empty, and the row whose `sku` is `gone`
/// comes back as a delete. A rule whose select carries `_op` also deletes a key no select
/// answered — the merger's verdict, which the flush's own re-shaping may not move.
#[test]
fn a_chunk_renders_the_pinned_docs() {
    let Some(dsn) = dsn() else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let rt = runtime();
    let (env, rule) = rt.block_on(server(&dsn, "pinned", 4));
    let ks = keys(&["1", "2", "3", "4"]);
    let docs = rt.block_on(load_docs(&env, &rule, &ks)).expect("loads");
    let got: Vec<(String, bool, serde_json::Value)> = docs
        .iter()
        .map(|d| (d.key.clone(), d.delete, d.doc.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".into(), false, json!({"sku": "stock", "price": 10})),
            ("2".into(), false, json!({"sku": "stock", "price": 20})),
            ("3".into(), true, json!({})),
            ("4".into(), true, json!({"sku": "gone", "price": 40})),
        ],
        "the chunk's docs, tombstones and unanswered key"
    );
    rt.block_on(drop_schema(&dsn, "pinned"));
}

/// The same chunk twice renders the same bytes: the chunk's parameters are shared by its
/// selects, and a select run re-reads them rather than mutating them.
#[test]
fn a_second_run_of_the_same_chunk_renders_the_same_docs() {
    let Some(dsn) = dsn() else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let rt = runtime();
    let (env, rule) = rt.block_on(server(&dsn, "twice", 4));
    let ks = keys(&["1", "2", "3", "4"]);
    let once = rt
        .block_on(load_docs(&env, &rule, &ks))
        .expect("first load");
    let twice = rt
        .block_on(load_docs(&env, &rule, &ks))
        .expect("second load");
    assert_eq!(rendered(&once), rendered(&twice), "same keys, same bytes");
    assert_eq!(rendered(&once).len(), 4);
    rt.block_on(drop_schema(&dsn, "twice"));
}

/// What one chunk of `load_docs` costs, measured through the real call (prepare, select, merge)
/// at the flush's own chunk size. The number is quoted in the PR body, taken on the commit the
/// branch started from and on this branch, with the same binary shape and the same input.
#[test]
fn a_load_docs_chunk_reports_its_allocation_cost() {
    let n = 1000; // SELECT_CHUNK: the flush's own chunk size
    let Some(dsn) = dsn() else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let rt = runtime();
    let (env, rule) = rt.block_on(server(&dsn, "cost", n));
    let ks: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
    // one run to warm the pool and the server's prepared statements, then count a chunk
    rt.block_on(load_docs(&env, &rule, &ks)).expect("warms up");
    let (docs, m) = alloc_measure::count(|| {
        rt.block_on(load_docs(&env, &rule, &ks))
            .expect("counts a chunk")
    });
    assert_eq!(docs.len(), n, "the chunk rendered every key");
    eprintln!("load_docs chunk of {n} keys x 2 selects (3 columns): {m} allocations");
    // The bound is the point of the test, not the print: the same chunk on the commit this
    // branch starts from (91ef9e7) costs 49,594 allocations and on this branch 40,546, so a
    // reintroduced copy of the rowset or of the per-select parameters walks the count back
    // past this line. (The counter's own credibility is pinned by
    // action_keyquery_tests::counting_the_allocator_counts_allocations_on_this_thread.)
    assert!(
        m <= 44_000,
        "the chunk's handover is copying again: {m} allocations against base 49,594 and \
         40,546 as measured here; the rowset or the per-select params/columns clones are back"
    );
    rt.block_on(drop_schema(&dsn, "cost"));
}
