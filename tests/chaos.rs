//! Chaos seeds (PR 1 of the chaos suite, design §2/§5): the engine runs as a real child
//! process, in-process `FaultProxy`s separate it from the shared servers per role, and every
//! fault transition is gated on an observed event (`/status` fields, proxy counters, mock
//! deliveries), never on a wall-clock sleep. S1 `source_cut_mid_stream`, T1 `target_down`
//! (with a healthy sibling rule that must keep advancing) and P1 `sigkill_mid_flush` exercise
//! P1–P5 jointly. Assertions are on sets and existence; exact delivery counts are reported.
//!
//! `make test-chaos`, or through the shared runner: `BIN=<chaos binary> TESTS_DIR=<tests>
//! tmp/e2e-shared.sh` (schema names stay literal `chaos_*` so its cleanup finds them).

mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use common::fault::{drain, FaultLog, FaultProxy, Lazy, Sampler};
use common::*;
use mysql_async::prelude::*;

/// The design's `chaos_shop`: `shop()` tables plus the `items` table whose version column
/// serves P4's (key, version) check for the later churn scenarios.
async fn chaos_shop(db: &Db) {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20), total DECIMAL(10,2))").await;
    db.exec("CREATE TABLE order_tags (order_id INT, tag VARCHAR(20), PRIMARY KEY (order_id, tag))")
        .await;
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, order_id INT, name VARCHAR(50), v INT NOT NULL DEFAULT 1)")
        .await;
    tables(db).await;
}

/// `127.0.0.1:33084` out of a root DSN.
fn server_addr(root: &str) -> String {
    let (_, rest) = root.split_once("://").expect("dsn scheme");
    rest.rsplit_once('@').map_or(rest, |(_, h)| h).to_string()
}

/// The same schema reached through a proxy: credentials kept, host:port replaced.
fn via(root: &str, proxy: SocketAddr, schema: &str) -> String {
    let (scheme, rest) = root.split_once("://").expect("dsn scheme");
    let creds = rest.rsplit_once('@').map_or("", |(c, _)| c);
    format!("{scheme}://{creds}@{proxy}/{schema}?prefer_socket=false")
}

fn keyset(from: u32, to: u32) -> BTreeSet<String> {
    (from..=to).map(|i| i.to_string()).collect()
}

/// One bulk statement per phase, as the design loads them (one transaction = one group).
async fn bulk_orders(db: &Db, from: u32, to: u32) {
    let rows: Vec<String> = (from..=to)
        .map(|i| format!("({i}, 1, 'new', 1.00)"))
        .collect();
    db.exec(&format!("INSERT INTO orders VALUES {}", rows.join(",")))
        .await;
}

/// The binlog end as a checkpoint (a transaction boundary; e2e's `>= after` pattern).
async fn binlog_end(db: &Db) -> (String, u64) {
    let mut c = db.conn().await;
    let (major, minor, _) = c.server_version();
    let q = if major > 8 || (major == 8 && minor >= 2) && major < 10 {
        "SHOW BINARY LOG STATUS"
    } else {
        "SHOW MASTER STATUS"
    };
    let row: mysql_async::Row = match c.query_first(q).await {
        Ok(Some(r)) => r,
        _ => c.query_first("SHOW MASTER STATUS").await.unwrap().unwrap(),
    };
    quit(c).await;
    (
        row.get::<String, _>(0).unwrap(),
        row.get::<u64, _>(1).unwrap(),
    )
}

/// A rule keyed on `{{new.id}}`, debounce 100 ms, template `POST /hook` with the body
/// `{{join keys ","}}` so every key is visible in the mock's request log.
async fn join_rule(db: &Db, rule: &str, target: &str, table: &str) {
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, 'POST /hook HTTP/1.1\n\n{{join keys \",\"}}', 100)",
        (rule, target),
    )
    .await;
    db.trigger(rule, table, Some("{{new.id}}"), None).await;
}

fn assert_all_keys(mock: &Mock, from: u32, to: u32, what: &str) {
    let got = mock.keys_received();
    let missing: Vec<String> = (from..=to)
        .map(|i| i.to_string())
        .filter(|k| !got.contains(k))
        .collect();
    assert!(
        missing.is_empty(),
        "{what}: {} of {} keys were never delivered (e.g. {:?})",
        missing.len(),
        to - from + 1,
        missing.iter().take(8).collect::<Vec<_>>()
    );
}

/// P5 on the child log: an outage is transient — no key took a strike for it.
fn assert_no_undelivered(lazy: &Lazy) {
    let bad = lazy.log_lines_matching("lazywf::undelivered");
    assert!(
        bad.is_empty(),
        "P5: {} undelivered events in {}:\n{}",
        bad.len(),
        lazy.log_path.display(),
        bad.join("\n")
    );
}

async fn wait_pending(lazy: &Lazy, rule: &str, secs: u64) {
    wait_for(
        Duration::from_secs(secs),
        &format!("rule {rule} to show pending keys"),
        || async {
            let s = lazy.status().await;
            (s["rules"][rule]["pending_keys"] != 0).then_some(())
        },
    )
    .await;
}

async fn wait_row_past(db: &Db, rule: &str, cp: &(String, u64), secs: u64) {
    wait_for(
        Duration::from_secs(secs),
        &format!("rule {rule} status row past {}:{}", cp.0, cp.1),
        || async {
            db.status(rule)
                .await
                .filter(|(f, p, _, _)| (f.clone(), *p) >= *cp)
                .map(|_| ())
        },
    )
    .await;
}

/// The proxy DSN trio of a scenario (design §3): the roles are separated by the product's own
/// configuration, so a fault on one role never needs MySQL protocol parsing.
fn proxy_env<'a>(dsn_main: &'a str, dsn_bin: &'a str, dsn_q: &'a str) -> [(&'a str, &'a str); 3] {
    [
        ("LAZYWF_DSN", dsn_main),
        ("LAZYWF_BINLOG_DSN", dsn_bin),
        ("LAZYWF_QUERY_DSN", dsn_q),
    ]
}

// --------------------------------------------------------------------------- S1 source

/// S1 (design §2): after the phase-1 keys reach the mock, every dump connection is RST-cut
/// while commits keep landing (phase 2 is written during the outage, so its confirmation is
/// genuinely blocked — "positions stay < the phase-2 checkpoint" is a live assertion, not a
/// race). The heal is gated on the engine's own reconnect being observed at the proxy. The
/// stream reconnects on its own, all phase-2 keys arrive, nothing is lost or duplicated
/// outside the window, positions never regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_source_cut_mid_stream() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_s1").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("s1-hook");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "o", "hook", "orders").await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "s1_source_cut_mid_stream",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    lazy.ready(60).await;
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // Phase 1 flows cleanly first: its keys are at the mock before the fault window opens.
    bulk_orders(&db, 1, 500).await;
    let cp1 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("o", "phase 1", cp1.clone(), keyset(1, 500), &mock);
    drain(&sp, &lazy, &db, "o", cp1.clone(), 60).await;

    let conns = binp.conns();
    binp.cut_all();
    log.add("phase 2 written during the dump outage");
    bulk_orders(&db, 501, 1000).await;
    let cp2 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("o", "phase 2", cp2.clone(), keyset(501, 1000), &mock);
    // Gated on the observed event: the engine's reconnect attempt arrives at the proxy.
    binp.wait(
        |s| s.conns > conns,
        "the engine's reconnect attempt on the dump role",
    )
    .await;
    binp.heal();
    // 60 s drain + the ≤ 10 s backoff-capped reconnect budget (P3 bound, design §2).
    drain(&sp, &lazy, &db, "o", cp2.clone(), 75).await;
    sp.check();

    // P1: no committed key lost. P4's hard edge (no duplicate outside a fault window) is the
    // sampler's; exact counts are reported, not asserted.
    assert_all_keys(&mock, 1, 1000, "S1");
    eprintln!(
        "S1: {} deliveries for 1000 write-once keys, proxy conns={}",
        mock.delivery_count(),
        binp.conns()
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

// --------------------------------------------------------------------------- T1 target

/// T1 (design §2): the target endpoint points at a proxy in `refuse` mode (dead port) while
/// 1 000 orders land in 4 phases. The stuck rule's position stays below the first pending
/// checkpoint the whole outage, its `last_error` shows the outage, the healthy sibling rule
/// keeps advancing past the last checkpoint (rules and targets never wait for each other) —
/// and after the heal everything drains within the bound. With no restart happened, the clean
/// P4 window applies: any duplicate delivery outside the refuse window fails the suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t1_target_down() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_t1").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let down = Mock::start().await;
    down.set_fault_log(log.clone());
    down.tag("t1-down");
    let up = Mock::start().await;
    up.set_fault_log(log.clone());
    up.tag("t1-up");
    let tprog = FaultProxy::start(&down.addr.to_string(), "target-down", Some(log.clone())).await;
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target(
        "down",
        "http",
        &format!("http://{}/hook", tprog.addr()),
        None,
        "{}",
    )
    .await;
    db.target("free", "http", &up.url(), None, "{}").await;
    join_rule(&db, "stuck", "down", "orders").await;
    join_rule(&db, "free", "free", "orders").await;
    tprog.refuse(true);
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "t1_target_down",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    lazy.ready(60).await;
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_proxy(&tprog);
    sp.add_log(&lazy.log_path);

    let mut cps: Vec<(String, u64)> = Vec::new();
    for phase in 0..4u32 {
        let (from, to) = (1 + phase * 250, (phase + 1) * 250);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "stuck",
            &format!("phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &down,
        );
        sp.add_phase(
            "free",
            &format!("phase {} (sibling)", phase + 1),
            cp.clone(),
            keyset(from, to),
            &up,
        );
        cps.push(cp);
    }

    // The sibling rule on the healthy target keeps advancing through the whole outage.
    wait_row_past(&db, "free", &cps[3], 30).await;
    // The stuck rule is *visibly* stuck (an observed condition, then the heal).
    wait_pending(&lazy, "stuck", 30).await;
    wait_for(
        Duration::from_secs(30),
        "the outage on the stuck rule's last_error",
        || async {
            db.status("stuck")
                .await
                .and_then(|(_, _, _, e)| e)
                .map(|e| e.to_string())
        },
    )
    .await;
    let (f, p, _, _) = db.status("stuck").await.expect("stuck rule row");
    assert!(
        (f.clone(), p) < cps[0],
        "the stuck rule reached {f}:{p} while its first checkpoint {}:{} is unmet",
        cps[0].0,
        cps[0].1
    );
    sp.check();
    log.add(format!(
        "stuck observed pinned at {f}:{p}, healing the target"
    ));
    tprog.heal();
    // Drain bound: 60 s plus the fixed stall allowance (P3, design §2).
    drain(&sp, &lazy, &db, "stuck", cps[3].clone(), 75).await;
    sp.check();

    assert_all_keys(&down, 1, 1000, "T1 target");
    assert_all_keys(&up, 1, 1000, "T1 sibling");
    eprintln!(
        "T1: deliveries down={} sibling={} for 1000 write-once keys each",
        down.delivery_count(),
        up.delivery_count()
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

// --------------------------------------------------------------------------- P1 process

/// P1 (design §2): the mock answers nothing (accept-and-freeze) so the phase-2 flush can never
/// complete; when `/status` shows `firing == true` (the lane holding the flush — an event, not
/// a sleep) the child takes a SIGKILL. The second child logs `starting from the saved
/// positions` at exactly the persisted row and delivers every key: the keys of the unconfirmed
/// flush were re-read and re-sent (their earlier frozen answer never counted as a delivery),
/// the position stayed below the in-flight checkpoint all along, and no key was struck.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p1_sigkill_mid_flush() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_p1").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("p1-hook");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "o", "hook", "orders").await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy1 = Lazy::start(
        "p1_sigkill_mid_flush",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    lazy1.ready(60).await;
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy1.log_path);

    // Phase 1 completes cleanly and its confirmation is *persisted* before the fault: the
    // restarted child must resume from that row (the drain asserts row >= cp1).
    bulk_orders(&db, 1, 200).await;
    let cp1 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("o", "phase 1", cp1.clone(), keyset(1, 200), &mock);
    drain(&sp, &lazy1, &db, "o", cp1.clone(), 60).await;

    // The mid-flush anchor: every next request is accepted and never answered.
    mock.hold_after(0);
    bulk_orders(&db, 201, 600).await;
    let cp2 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("o", "phase 2", cp2.clone(), keyset(201, 600), &mock);
    // The kill point is an *observed* event, not a sleep: the mock has taken the phase-2
    // flush and frozen it (`hold_after(0)` answers nothing), which is exactly the mid-flush
    // state this seed is about. The row the child has persisted is still phase 1's.
    wait_for(
        Duration::from_secs(60),
        "the phase-2 flush to reach the target and be held",
        || async { log.has("hold_after fired").then_some(()) },
    )
    .await;
    let (pf, pp, _, _) = db.status("o").await.expect("status row");
    assert!(
        (pf.clone(), pp) >= cp1,
        "phase 1 was not confirmed before the kill ({pf}:{pp} < {}:{})",
        cp1.0,
        cp1.1
    );
    assert!(
        (pf.clone(), pp) < cp2,
        "the in-flight flush was confirmed although its expect was never met"
    );
    sp.check();
    log.add(format!(
        "killing child 1 at {pf}:{pp} (in-flight flush, expect unmet)"
    ));
    lazy1.kill();
    // Heal the target before the restart: the re-sent flush must be answerable. The kill and
    // the heal are fault events, so a legitimate duplicate inside this window is counted.
    mock.heal();
    let mut lazy2 = Lazy::start(
        "p1_sigkill_mid_flush",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    lazy2.ready(60).await;
    sp.add_log(&lazy2.log_path);
    drain(&sp, &lazy2, &db, "o", cp2.clone(), 75).await;
    sp.check();

    // P6: the restart resumes from the persisted row (monotone positions are the sampler's).
    let starts = lazy2.log_lines_matching("starting from the saved positions");
    assert!(
        !starts.is_empty(),
        "the restarted child logged no saved-position start in {:?}",
        lazy2.log_path
    );
    let line = starts.last().unwrap().clone();
    let v: serde_json::Value = serde_json::from_str(&line).expect("json log line");
    assert_eq!(
        v["fields"]["position"].as_str(),
        Some(format!("{pf}:{pp}").as_str()),
        "the restarted child did not start at the persisted row"
    );

    assert_all_keys(&mock, 1, 600, "P1");
    eprintln!(
        "P1: {} deliveries for 600 write-once keys (the killed flush's earlier answer was \
         frozen, never delivered)",
        mock.delivery_count()
    );
    assert_no_undelivered(&lazy1);
    assert_no_undelivered(&lazy2);
    lazy2.kill();
    sp.stop();
}
