//! The flush's own data on its way out (the hot-path audit's finding 11): what starting a
//! flush costs, and that a job which dies still brings its keys and its queries back.
//!
//! `spawn_flush`, `spawn_job` and `on_done` are the production functions here, unmodified. They
//! need a real `Engine`, and an `Engine` needs a connected `control`, so these run against a
//! server (`LAZYWF_TEST_DSN`) and skip without one — the same gate the integration suite uses.
//! The numbers they print are the numbers the PR body quotes, taken on the commit the branch
//! started from and on the branch.

use super::*;
use crate::alloc_measure;
use serde_json::json;

const RULE: &str = "orders_sync";
/// lazywf's own tables live here; the test only needs the engine's `control` to be a live
/// connection, so it is the suite's own schema, named `lwf_*` like every suite schema.
const SCHEMA: &str = "lwf_hotpath_f";

/// The keys one flush takes: long enough that a copy of them is worth counting, short enough
/// to stay one chunk (the flush's own chunking is not what is measured here).
fn flush_keys(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("order-{i:05}-customer-acme"))
        .collect()
}

/// `keys_sql` payloads of one flush: the parameters are what made the up-front copy expensive,
/// each query carrying a nested object and two strings.
fn flush_queries(n: usize) -> Vec<KeyQuery> {
    (0..n)
        .map(|i| KeyQuery {
            trigger: 7,
            sql: format!("SELECT k FROM orders WHERE customer = ? AND n = {i}").into(),
            params: vec![
                json!(i as i64),
                json!(format!("customer-{i:05}-with-a-fairly-long-parameter-value")),
                json!({"note": format!("notes for {i}: some text to make the value real"), "ok": true}),
            ],
        })
        .collect()
}

fn rule() -> Arc<Rule> {
    let def = crate::model::RuleDef::placeholder(&crate::model::RawRule {
        name: RULE.into(),
        enabled: true,
        ..Default::default()
    });
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates: crate::template::Templates::default(),
        selects: Vec::new(),
        triggers: Vec::new(),
        target: None,
        error: None,
        trigger_error: None,
    })
}

fn dsn() -> Option<String> {
    std::env::var("LAZYWF_TEST_DSN")
        .ok()
        .filter(|d| !d.is_empty())
}

/// An engine with one rule whose buffer holds `n_keys` keys and `n_queries` pending queries,
/// ready for `schedule` to flush: the state `spawn_flush` is called in. The receiver of the
/// done channel is handed back so a test can deliver an outcome to `on_done` itself.
async fn engine_with_flush_ready(
    n_keys: usize,
    n_queries: usize,
) -> Option<(Engine, mpsc::UnboundedReceiver<Done>)> {
    let dsn = dsn()?;
    let opts = crate::binlog::replica_opts(&dsn).expect("replica opts");
    let control = mysql_async::Conn::new(opts.clone())
        .await
        .expect("connects (the suite's server)");
    let pool = mysql_async::Pool::new(dsn.as_str());
    let env = Arc::new(Env::new(
        pool.clone(),
        pool,
        crate::source::Flavor::MySql,
        std::time::Duration::ZERO,
        1,
    ));
    let cfg = Config {
        dsn: dsn.clone(),
        binlog_dsn: dsn.clone(),
        query_dsn: dsn.clone(),
        query_account: false,
        schema: SCHEMA.into(),
        instance: "hotpath-f-test".into(),
        server_id: 7_101,
        server_id_fixed: true,
        health_addr: None,
        metrics_addr: None,
        heartbeat: std::time::Duration::from_secs(30),
        visibility_delay: std::time::Duration::ZERO,
        pool_size: 2,
        send_concurrency: 1,
        config_poll: std::time::Duration::from_secs(30),
        status_every: std::time::Duration::from_secs(5),
        max_backoff: std::time::Duration::from_secs(30),
        stop_grace: std::time::Duration::from_secs(5),
        allowed_hosts: None,
        health_token: None,
    };
    let (tx, rx) = mpsc::unbounded_channel();

    let mut st = RuleState {
        rule: rule(),
        incarnation: 1,
        buffer: Buffer::default(),
        lane: None,
        prep: false,
        skip_until: None,
        frozen_at: None,
        reset: None,
        reset_state: String::new(),
        last_error: None,
        backoff: Backoff::default(),
        pending_clear: false,
        reset_request_seen: false,
        counters: Counters::default(),
        applied_ts: 0,
        strikes: HashMap::new(),
        written: None,
        dirty: false,
        mrule: None,
        mpair: None,
    };
    let origin = Position {
        file: "mysql-bin.000001".into(),
        pos: 4,
    };
    for k in flush_keys(n_keys) {
        let (s, key) = st.buffer.locate(&k);
        st.buffer.mark(s, key, &origin);
    }
    for q in flush_queries(n_queries) {
        let id = q.id();
        st.buffer.add_query(id, q, &origin);
    }
    st.buffer.since_ts = 1;
    let now = Instant::now();
    st.buffer.first = Some(now);
    st.buffer.last = Some(now);

    let mut rules = BTreeMap::new();
    rules.insert(RULE.to_string(), st);
    let e = Engine {
        cfg,
        lock: "lwf-hotpath-f-test".into(),
        opts: opts.clone(),
        binlog_opts: opts,
        control,
        env,
        control_stalled: false,
        lock_unconfirmed: false,
        control_conn_id: None,
        next_control_try: Instant::now(),
        lock_denies: 0,
        flavor: crate::source::Flavor::MySql,
        identity: String::new(),
        lower_names: false,
        health: Arc::new(Health::new(None)),
        metrics: None,
        catalog: Catalog::default(),
        boundary: origin.clone(),
        settled: origin.clone(),
        in_txn: false,
        txn_foreign: false,
        last_gtid: None,
        txn_gtid: None,
        last_event_ts: 0,
        caught_up: true,
        table_ids: HashMap::new(),
        txn_marks: HashMap::new(),
        key_scratch: KeyScratch::default(),
        xa_current: None,
        xa_pending: HashMap::new(),
        rewinds: Vec::new(),
        rewound: None,
        shadows: HashMap::new(),
        stmt_dml_last: HashMap::new(),
        stmt_dml_ignored: 0,
        stmt_dml_capped: false,
        config_dirty: None,
        config_fp: 0,
        next_poll: now,
        targets: HashMap::new(),
        routes: HashMap::new(),
        route_cache: RouteCache::default(),
        rules,
        saved: HashMap::new(),
        incarnations: 0,
        runs: 0,
        target_backoff: HashMap::new(),
        mismatched: HashSet::new(),
        column_warnings: HashSet::new(),
        maria_skips_virtual: false,
        virtual_first: HashMap::new(),
        virtual_refusals: HashSet::new(),
        tx,
        last_persist: now,
        clear_retry: None,
        star_written: None,
        render_warnings: 0,
        invalid_keys: 0,
        filter_checked: HashSet::new(),
        binlog_filtered: BTreeSet::new(),
        stream_error: None,
        undecodable: UndecodableWatch::default(),
        // Same seed as `Engine::run`: a rule measured before the first connect ages from the
        // seed, not from the epoch (see the clock tests).
        clock: SourceClock {
            head_ts: crate::time::unix_now(),
            ..SourceClock::default()
        },
        last_clock: Instant::now(),
    };
    Some((e, rx))
}

/// What starting one flush costs, measured through the real `spawn_flush`. The job it spawns is
/// queued, not run, so this counts the engine's own work for the flush and nothing else.
#[tokio::test(flavor = "current_thread")]
async fn starting_a_flush_reports_its_allocation_cost() {
    let Some((mut e, _rx)) = engine_with_flush_ready(200, 20).await else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let ((), n) = alloc_measure::count(|| e.spawn_flush(RULE));
    eprintln!("spawn_flush of 200 keys and 20 keys_sql queries: {n} allocations");
    // The bound, not the print. The same flush costs 1,005 allocations on the commit PR-F
    // started from (91ef9e7: the query payloads and the keys were both copied up front) and
    // 417 on main after PR-F — measured three times, and accounted for part by part: 201 for
    // `buf.keys.render` (one String per key, necessary), 201 for the armed fallback's
    // `keys.clone()`, the rest the lane and the `Arc`s. Here it costs 216: the fallback is a
    // closure `spawn_job` calls only if the task died, so a flush that goes out well renders
    // its keys once. A reintroduced up-front copy — `queries.clone()` or `keys.clone()` —
    // walks the count back over this line.
    assert!(
        n <= 300,
        "the flush is copying its own data again: {n} allocations against main's 417 and the \
         216 measured here (201 to render the keys, the rest the lane and the Arcs); an \
         up-front keys or queries clone is back"
    );
    // it really flushed: the lane holds the generation and the buffer gave it up
    assert!(matches!(e.rules[RULE].lane, Some(Lane::Flush { .. })));
    assert_eq!(e.rules[RULE].buffer.core.pending_len(), 0);
    assert_eq!(e.rules[RULE].buffer.queries.len(), 0);
}

/// A job whose task panics still reports its keys: `spawn_job` is what turns a dead task into
/// the fallback outcome `spawn_flush` armed, and the keys must survive into it whole — an
/// `Arc` shared with a task that died must not leave the engine without its keys.
#[tokio::test(flavor = "current_thread")]
async fn a_panicked_job_still_reports_its_keys() {
    let Some((_e, _rx)) = engine_with_flush_ready(1, 0).await else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let keys = flush_keys(3);
    let (tx, mut rx) = mpsc::unbounded_channel();
    // The shape a real flush arms: the key list lives in one `Arc` the task holds too, and
    // the fallback is a closure `spawn_job` calls only because the task died.
    let armed = Arc::new(keys.clone());
    let failed = {
        let armed = Arc::clone(&armed);
        move || Outcome::Flush {
            fired: Fired::failed((*armed).clone()),
            keep_queries: true,
            resolved: Vec::new(),
        }
    };
    // the shared payloads a real flush would have handed to both sides
    let shared = Arc::new(flush_queries(3));
    let job = {
        let shared = Arc::clone(&shared);
        async move {
            if std::hint::black_box(false) {
                return Outcome::Flush {
                    fired: Fired::default(),
                    keep_queries: false,
                    resolved: Vec::new(),
                };
            }
            let _n = shared.len();
            panic!("the job died");
        }
    };
    spawn_job(tx, RULE.to_string(), 1, failed, job);
    let d = rx.recv().await.expect("the fallback reaches the engine");
    assert_eq!(d.rule, RULE);
    assert_eq!(d.incarnation, 1);
    let Outcome::Flush {
        fired,
        keep_queries,
        ..
    } = d.outcome
    else {
        panic!("a flush outcome was armed");
    };
    assert_eq!(
        fired.failed_keys, keys,
        "the keys of a job whose task panicked must come back whole"
    );
    assert!(
        fired.error.is_some(),
        "and the flush is known to have failed"
    );
    assert!(keep_queries, "and its queries are owed back");
    assert_eq!(
        Arc::strong_count(&shared),
        1,
        "the dead task released its handle"
    );
}

/// The engine's half of the same event: with the lane taken (a real `spawn_flush`) and the job
/// gone, `on_done` must put every key back in the buffer, put every query payload back under
/// its id, and keep the position the generation was taken at. This is the path the shared
/// payloads changed, so it is the one that must not lose anything.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_flush_goes_back_to_the_buffer_with_its_position() {
    let Some((mut e, _rx)) = engine_with_flush_ready(50, 6).await else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let keys = flush_keys(50);
    let origin = Position {
        file: "mysql-bin.000001".into(),
        pos: 4,
    };
    // what the buffer held before the flush
    let st = &e.rules[RULE];
    assert_eq!(st.buffer.core.pending_len(), 50 + 6);
    assert_eq!(st.buffer.queries.len(), 6);
    assert_eq!(
        st.buffer.core.pinned(),
        Some(&origin),
        "the floor is the flush's own"
    );

    e.spawn_flush(RULE);
    assert_eq!(
        e.rules[RULE].buffer.core.pending_len(),
        0,
        "the generation was taken"
    );
    assert_eq!(e.rules[RULE].buffer.queries.len(), 0, "with its payloads");

    // the outcome of the panicked job, delivered as `spawn_job` would deliver it. The flush's
    // own job is still queued: the runtime is not given a turn, so nothing else answers.
    e.on_done(Done {
        rule: RULE.to_string(),
        incarnation: 1,
        outcome: Outcome::Flush {
            fired: Fired::failed(keys.clone()),
            keep_queries: true,
            resolved: Vec::new(),
        },
    });

    let st = &e.rules[RULE];
    assert!(st.lane.is_none(), "the lane is released");
    assert_eq!(
        st.buffer.core.pending_len(),
        50 + 6,
        "every key and every query is pending again"
    );
    assert_eq!(
        st.buffer.queries.len(),
        6,
        "the payloads came back, so a retry does not recompute them"
    );
    for q in flush_queries(6) {
        assert!(
            st.buffer.queries.contains_key(&q.id()),
            "query {} came back under its own id",
            q.id()
        );
    }
    assert_eq!(
        st.buffer.core.pinned(),
        Some(&origin),
        "the position never ran ahead of keys that were not delivered"
    );
    assert!(
        st.buffer.first.is_some(),
        "the debounce clock is carried over"
    );
    assert_eq!(
        st.counters.actions_failed, 0,
        "a job that died reports no actions, only keys pending"
    );
}
