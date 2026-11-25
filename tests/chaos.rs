//! Chaos seeds (design §2/§5): the engine runs as a real child process, in-process
//! `FaultProxy`s separate it from the shared servers per role, and every fault transition is
//! gated on an observed event (`/status` fields, proxy counters, mock deliveries), never on a
//! wall-clock sleep. S1 `source_cut_mid_stream`, T1 `target_down` (with a healthy sibling rule
//! that must keep advancing) and P1 `sigkill_mid_flush` exercise P1–P5 jointly; S2–S5 add the
//! rest of the source class (stall, flapping, a cut inside a 50 000-row group, and an XA whose
//! COMMIT lands across a cut and across a kill); T2–T6 add the target class. P2
//! `sigkill_during_reset` interrupts a snapshot reset mid-step and requires it to start over
//! after the restart, P2b `sigkill_during_requested_reset_resumes_the_saved_step` kills a
//! requested reset at a later step and requires the restarted child to resume there — the
//! finished steps answered no more, the interrupted one re-walked whole — P2c
//! `sigkill_during_unrequested_reset_resumes_the_saved_step` holds the same proof for a reset
//! that started on its own (the first start's walk, killed with the rule's position
//! persisted), P3
//! `graceful_sigterm` is the cheap shutdown control (exit 0 within the
//! stop grace, running actions finished, position saved for the next child), and C1
//! `target_down_while_source_flaps` is the combined outage that proves "outage ≠ data loss"
//! end to end. `h1` and `h2`
//! are harness self-tests, and they are the reason two of the sampler's invariants can be trusted:
//! each forges a row that *should* be refused — a position below a declared XA floor, and a position
//! at the checkpoint of a phase whose keys never arrived — and requires the refusal. An invariant
//! nobody has seen fail is not an invariant.
//!
//! `make test-chaos`, or through the shared runner: `BIN=<chaos binary> TESTS_DIR=<tests>
//! tmp/e2e-shared.sh` (schema names stay literal `chaos_*` so its cleanup finds them).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::fault::{
    drain, environment_aborts, sweep_ran, sweep_xa_once, xa_recover_lwf, FaultLog, FaultProxy,
    Lazy, Sampler, Stop, XaGuard, XaPin,
};
use common::*;
use mysql_async::prelude::*;

/// Brings the engine child to ready and ends the seed when the *environment* took it. `ready`
/// has already printed the signal, the host load and the OOM counters and recorded the abort in
/// `tmp/out/chaos/env-aborts.log`; every other way a child fails to start stays a panic inside
/// `ready`. A seed whose child is gone can assert nothing, and reporting the kernel's OOM kill as
/// a product failure is how a suite sends someone to bisect a commit that never moved.
macro_rules! ready_or_skip {
    ($lazy:expr, $secs:expr) => {{
        if !$lazy.ready($secs).await {
            eprintln!(
                "SKIPPED BY ENVIRONMENT at {}:{} — the harness's child was taken; see \
                 tmp/out/chaos/env-aborts.log",
                file!(),
                line!()
            );
            return;
        }
    }};
}

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
    assert_no_undelivered_at(&lazy.log_path);
}

/// The same, for a child that is already gone (killed in a chaos round): the log file outlives
/// it, and its undelivered lines are as much a failure as the live child's.
fn assert_no_undelivered_at(path: &std::path::Path) {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let bad: Vec<String> = text
        .lines()
        .filter(|l| l.contains("lazywf::undelivered"))
        .map(String::from)
        .collect();
    assert!(
        bad.is_empty(),
        "P5: {} undelivered events in {}:\n{}",
        bad.len(),
        path.display(),
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
    ready_or_skip!(lazy, 60);
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
    ready_or_skip!(lazy, 60);
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

// ---------------------------------------------------------------------- T2 slow target

/// `id:version` keys for a churn load: a row that comes back with a new version is a *new* key,
/// which is what keeps the sampler's P4 check hard for a load that repeats (design §2 T2).
fn version_keys(from: u32, to: u32, v: u32) -> BTreeSet<String> {
    (from..=to).map(|i| format!("{i}:{v}")).collect()
}

fn union_keys(sets: &[BTreeSet<String>]) -> BTreeSet<String> {
    sets.iter().flatten().cloned().collect()
}

/// Every key of `want` has to have reached `mock` at least once (P1's delivery side, for the
/// keyed loads whose keys are not a plain range).
fn assert_keys(mock: &Mock, want: &BTreeSet<String>, what: &str) {
    let got = mock.keys_received();
    let missing: Vec<&String> = want.iter().filter(|k| !got.contains(*k)).collect();
    assert!(
        missing.is_empty(),
        "{what}: {} of {} keys were never delivered (e.g. {:?})",
        missing.len(),
        want.len(),
        missing.iter().take(8).collect::<Vec<_>>()
    );
}

/// Whether `mock` has read in full a request whose `{{join keys ","}}` body carries every key
/// of `want`. A read request is what a `delay`-mode answer is made of (the mock answers every
/// request it reads), so this is the mock's own record of "this round reached the target" —
/// the condition T2 gates each round on. Unlike a firings counter, it cannot be satisfied by
/// an earlier round's retry, and it does not assume how many requests the engine's debounce
/// chose to make.
fn mock_seen_keys(mock: &Mock, want: &BTreeSet<String>) -> bool {
    mock.requests().iter().any(|r| {
        let got: BTreeSet<&str> = r
            .body
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        want.iter().all(|k| got.contains(k.as_str()))
    })
}

/// (deliveries, keys that arrived more than once) for a seed's own report. P4's rule is the
/// sampler's; the seed only has to say out loud what the run really delivered, because "no
/// duplicates" claimed without a number is exactly how a lost fault hides (mutation M1).
fn delivery_report(mock: &Mock) -> (usize, usize) {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for (_, keys) in mock.deliveries() {
        for k in keys {
            *seen.entry(k).or_insert(0) += 1;
        }
    }
    let repeated = seen.values().filter(|n| **n > 1).count();
    (seen.values().sum(), repeated)
}

/// One re-update of the whole churn range: one statement, so one transaction and one group.
async fn bump_items(db: &Db, from: u32, to: u32) {
    db.exec(&format!(
        "UPDATE items SET v = v + 1 WHERE id BETWEEN {from} AND {to}"
    ))
    .await;
}

/// The churn rule: keyed on `(id, v)`, body `{{join keys ","}}`, so the mock's request log shows
/// every key of every version the engine believed it had sent.
async fn version_rule(db: &Db, rule: &str, target: &str) {
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, 'POST /hook HTTP/1.1\n\n{{join keys \",\"}}', 100)",
        (rule, target),
    )
    .await;
    db.trigger(rule, "items", Some("{{new.id}}:{{new.v}}"), None)
        .await;
}

/// The rule's own `last_error`, once it says `needle`. Gating on the text (rather than on
/// "some error appeared") is what makes the fault's *shape* an assertion: an outage that shows
/// up as the wrong error is a different bug, and a mutant that breaks the error path would see
/// this wait time out with the text it did get.
async fn wait_rule_error(db: &Db, rule: &str, needle: &str, secs: u64) -> String {
    wait_for(
        Duration::from_secs(secs),
        &format!("rule {rule}'s last_error to name `{needle}`"),
        || async {
            db.status(rule)
                .await
                .and_then(|(_, _, _, e)| e)
                .filter(|e| e.contains(needle))
        },
    )
    .await
}

/// T2 (design §2): the target answers, but late. 300 `items` rows are re-updated six times —
/// 1 800 events, 300 distinct `(key, version)` keys per round — in two stages. Stage A answers
/// 400 ms after the request, inside the target's own 3 000 ms timeout: late, but in time, so
/// everything is still delivered. Stage B answers 4 000 ms after it, past that timeout: the mock
/// does write an answer, and no answer of that kind may be believed (`Mock::late_answers` counts
/// them and they are never deliveries, design §1 P4), so the rule's position has to stay below
/// the checkpoint of everything stage B left unconfirmed until the heal.
///
/// Asserted: the slow stage fires *at the mock* (an armed mode is not a fired one), stage A
/// delivers its keys, nothing is late inside the client's own timeout, stage B leaves the row
/// pinned below its first unconfirmed checkpoint while `/status` still shows pending keys and
/// `last_error` names the client timeout, and after `heal()` the backlog drains inside the bound
/// with every `(key, version)` key delivered and no key struck.
///
/// Each round waits for its own keys to be read at the mock before the next round is written:
/// the rule's 100 ms debounce runs from the LAST mark, so a server that answers the harness's
/// writes quickly lets several commits coalesce into one generation — the engine's designed
/// behavior, and one request carries only one firing. Counting firings after a bulk write of
/// all rounds made that legitimate coalescing look like "the fault never fired" (the original
/// flake, seen on MariaDB 10.11 only, where the harness's own write path is fastest).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t2_target_slow() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_t2").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("t2-slow");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("slow", "http", &mock.url(), None, "{}").await;
    version_rule(&db, "slow", "slow").await;
    let rows: Vec<String> = (1..=300).map(|i| format!("({i}, 1, 'x', 1)")).collect();
    db.exec(&format!("INSERT INTO items VALUES {}", rows.join(",")))
        .await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "t2_target_slow",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // ---- Stage A: 400 ms, inside the target's 3 000 ms timeout: late, but in time.
    mock.delay(400, 3000);
    let mut cps_a: Vec<(String, u64)> = Vec::new();
    let mut want_a: Vec<BTreeSet<String>> = Vec::new();
    for round in 1..=3u32 {
        bump_items(&db, 1, 300).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        let keys = version_keys(1, 300, round + 1);
        sp.add_phase(
            "slow",
            &format!("stage A round {round} (v={})", round + 1),
            cp.clone(),
            keys.clone(),
            &mock,
        );
        want_a.push(keys.clone());
        cps_a.push(cp);
        // The fault is a fact at the mock before the seed believes anything about the stage, and
        // before the next round is written: this round's keys have to have reached the mock, and
        // the record is the mock's own. Gating inside the loop is also what keeps the rounds
        // separate on the wire: the rule's debounce is measured from the LAST mark
        // (`due = (last + debounce).min(first + max_delay)` in src/engine.rs), so when the server
        // answers the harness's writes fast all three commits land inside one debounce window
        // and coalesce into a single generation — one request, one firing. That is the engine's
        // designed behavior (it is the very debounce T2 exists to exercise), not a fault: a bulk
        // write followed by a wait for three firings reads it as "the fault never fired", which
        // is this seed's MariaDB flake. A round written after the previous one was seen at the
        // mock always starts an empty buffer and so a generation of its own.
        wait_for(
            Duration::from_secs(60),
            &format!("T2 stage A round {round}: the mock reading a request with this round's keys"),
            || async { mock_seen_keys(&mock, &keys).then_some(()) },
        )
        .await;
    }
    drain(&sp, &lazy, &db, "slow", cps_a[2].clone(), 90).await;
    let fired_a = mock.delay_firings();
    let late_a = mock.late_answers();
    assert!(
        fired_a >= 3,
        "T2 stage A: the slow stage never fired at the mock (delay_firings={fired_a}): the mode \
         was armed and nothing answered late, so this half of the seed tested nothing"
    );
    assert_eq!(
        late_a, 0,
        "T2 stage A: 400 ms is inside the target's own 3 000 ms timeout, so nothing may be \
         counted late (late_answers={late_a}): the harness would be calling a delivered key lost"
    );
    assert_keys(&mock, &union_keys(&want_a), "T2 stage A");
    sp.check();

    // ---- Stage B: 4 000 ms, past the 3 000 ms timeout: the mock answers, and the engine has
    // already given up on the answer, so it is a fault event and not a delivery.
    mock.delay(4000, 3000);
    let mut cps_b: Vec<(String, u64)> = Vec::new();
    let mut want_b: Vec<BTreeSet<String>> = Vec::new();
    for round in 4..=6u32 {
        bump_items(&db, 1, 300).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        let keys = version_keys(1, 300, round + 1);
        sp.add_phase(
            "slow",
            &format!("stage B round {} (v={})", round - 3, round + 1),
            cp.clone(),
            keys.clone(),
            &mock,
        );
        want_b.push(keys.clone());
        cps_b.push(cp);
        // The same per-round gate as stage A: with `delay(4000)` armed, every request the mock
        // reads is answered past the client's 3 000 ms timeout, so a read request is this
        // round's fault event in the making. The gate keeps the three late answers counted
        // below owed to three rounds, not to one coalesced burst of them and its retries.
        wait_for(
            Duration::from_secs(60),
            &format!(
                "T2 stage B round {}: the mock reading a request with this round's keys",
                round - 3
            ),
            || async { mock_seen_keys(&mock, &keys).then_some(()) },
        )
        .await;
    }
    wait_for(
        Duration::from_secs(90),
        "T2 stage B: the mock answering past the client's own timeout",
        || async { (mock.late_answers() >= 3).then_some(()) },
    )
    .await;
    // The rule is visibly behind (an observed condition, then the heal).
    wait_pending(&lazy, "slow", 30).await;
    let (f, p, _, _) = db.status("slow").await.expect("slow rule row");
    assert!(
        (f.clone(), p) < cps_b[0],
        "the slow rule reached {f}:{p} while stage B's first checkpoint {}:{} is still \
         unconfirmed: an answer the client had already given up on was credited as a delivery",
        cps_b[0].0,
        cps_b[0].1
    );
    let err = wait_rule_error(&db, "slow", "no complete response within 3000 ms", 60).await;
    log.add(format!(
        "stage B observed pinned at {f}:{p} with {} late answers, healing the target",
        mock.late_answers()
    ));
    mock.heal();
    // Drain bound: the retry backoff is capped at 10 s, and every late stage costs the engine one
    // more 4 s wait before the heal takes effect.
    drain(&sp, &lazy, &db, "slow", cps_b[2].clone(), 120).await;
    sp.check();
    let mut all = want_a.clone();
    all.extend(want_b.iter().cloned());
    assert_keys(&mock, &union_keys(&all), "T2 target");
    let (delivered, repeated) = delivery_report(&mock);
    eprintln!(
        "T2: 1800 events / 1800 (key, version) keys, delay firings={} late answers={} \
         deliveries={} repeated keys={} (the late stage alone owes {} timeouts), \
         last_error={err}",
        mock.delay_firings(),
        mock.late_answers(),
        delivered,
        repeated,
        mock.late_answers()
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

// ------------------------------------------------------------------ T3 target 5xx burst

/// T3 (design §2): the mock answers the first 6 requests with `500 Internal Server Error` while
/// 1 000 orders land in 4 phases. A 5xx is a *transient* failure and not a refusal — an unmet
/// `expect` becomes `Fail::Transient` in `send_all`, so a bad status may never strike a key and
/// may never let the position pass a checkpoint whose keys were refused. The burst is counted at
/// the mock (`=500 fired` in the harness fault log) rather than trusted because it was armed, and
/// the same keys come back on the retry, so the duplicate rule is the sampler's (P4) and every
/// refusal it sees is a fault event in that log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t3_target_5xx_burst() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_t3").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("t3-burst");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("burst", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "burst", "burst", "orders").await;
    mock.respond_nth(6, 500);
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "t3_target_5xx_burst",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    let mut cps: Vec<(String, u64)> = Vec::new();
    for phase in 0..4u32 {
        let (from, to) = (1 + phase * 250, (phase + 1) * 250);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "burst",
            &format!("phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &mock,
        );
        cps.push(cp);
    }

    // The burst happened, exactly six times, at the mock — not "the mode was armed".
    wait_for(
        Duration::from_secs(60),
        "T3: the mock refusing six requests with 500",
        || async { (log.count("=500 fired") >= 6).then_some(()) },
    )
    .await;
    assert_eq!(
        log.count("=500 fired"),
        6,
        "T3: respond_nth(6, 500) refused {} requests; the mode is one-shot per request, so any \
         other number means the burst the seed reports is not the burst the mock performed",
        log.count("=500 fired")
    );
    // A 500 is transient and the rule says so, in the status row an operator would read.
    let err = wait_rule_error(&db, "burst", "status 500", 60).await;
    // P2 while the burst is unconfirmed: the sampler has watched every sample since the writes.
    sp.check();
    log.add(format!(
        "burst observed: 6 refusals, last_error={err}, waiting for the drain"
    ));
    drain(&sp, &lazy, &db, "burst", cps[3].clone(), 90).await;
    sp.check();
    // The burst ended where it was armed, read a second time at the end of the run. The single
    // read above is taken at one instant, and mutation M2 found that out: with `answered` never
    // advancing it passed (the gate hands the seed the moment six refusals exist, and the counter
    // still said six), and only the drain's own timeout killed the run. A second read at the end
    // is what makes "six, and then it stopped" a claim the seed can actually make.
    assert_eq!(
        log.count("=500 fired"),
        6,
        "T3: by the end of the run the mock had refused {} requests with 500, not the 6 that \
         `respond_nth(6, 500)` arms: the mode is one-shot per request and stops at six, so the \
         fault the seed reports is not the fault the mock performed",
        log.count("=500 fired")
    );
    assert_all_keys(&mock, 1, 1000, "T3 target");
    let (delivered, repeated) = delivery_report(&mock);
    eprintln!(
        "T3: 1000 write-once keys, deliveries={delivered} (repeated keys={repeated}), {} requests \
         at the mock of which 6 were refused with 500, last_error={err}",
        mock.requests().len()
    );
    // P5: a 5xx is not a key's fault, so nothing may have been given up on.
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

// ---------------------------------------------------------- T4 target cut mid answer

/// T4 (design §2): the mock answers the first 5 requests with a response head and *half* the
/// body, then resets the connection. `Content-Length` promised more than the client got, so the
/// engine's read fails in the middle of an answer it had already begun to read — the child log
/// says `POST http://127.0.0.1:PORT: Connection reset by peer`, with no `connecting to` in front
/// of it, which is exactly the difference between this seed and a dead port (a refused endpoint
/// reads `connecting to HOST:PORT: …`, src/http.rs, and the EOF twin of this error is
/// `connection closed inside the response body`, src/http.rs:436). A connection error is
/// transient: no strike, no position past an unconfirmed checkpoint, and every key arrives after
/// the heal. The cuts are counted where the reset is performed (`Mock::cut_firings`), and a half
/// answer is never a delivery (design §1 P4), so a phase cannot close on a lie.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t4_target_cut_mid_answer() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_t4").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("t4-cut");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("cut", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "cut", "cut", "orders").await;
    // Arming before the child starts: the mode is in place before the engine's first flush, so
    // the five cuts are five flushes and not a race with the first one.
    mock.cut_mid_answer(5);
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "t4_target_cut_mid_answer",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    let mut cps: Vec<(String, u64)> = Vec::new();
    for phase in 0..2u32 {
        let (from, to) = (1 + phase * 100, (phase + 1) * 100);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "cut",
            &format!("phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &mock,
        );
        cps.push(cp);
    }

    // The five half answers are a fact at the mock: counted at the reset, not at the arming.
    wait_for(
        Duration::from_secs(60),
        "T4: the mock cutting five answers in mid-body",
        || async { (mock.cut_firings() >= 5).then_some(()) },
    )
    .await;
    assert_eq!(
        mock.cut_firings(),
        5,
        "T4: cut_mid_answer(5) fired {} times; the mode stops at n, so any other number means \
         the fault the seed reports is not the fault the mock performed",
        mock.cut_firings()
    );
    // And the engine's side of it: the read failed *inside* an answer whose head had already
    // arrived. The bare reset is what the client reports (the kernel discards what linger-0 had
    // not sent); the two forms this error has in src/http.rs are this and
    // `connection closed inside the response body`, and neither is the `connecting to` form a
    // dead port produces — that distinction is what separates this seed from T1/T5/T6, so both
    // halves of it are asserted rather than one.
    let err = wait_rule_error(&db, "cut", "Connection reset by peer", 60).await;
    assert!(
        !err.contains("connecting to"),
        "T4: the rule reports `{err}`: a reset at the connect is an endpoint that is not there, \
         while this seed is about an answer that stops in the middle (the mock counted {} such \
         half answers, so the two stories are distinguishable and this one was not tested)",
        mock.cut_firings()
    );
    sp.check();
    log.add(format!(
        "5 half answers observed, last_error={err}, waiting for the drain"
    ));
    drain(&sp, &lazy, &db, "cut", cps[1].clone(), 90).await;
    sp.check();
    assert_all_keys(&mock, 1, 200, "T4 target");
    let (delivered, repeated) = delivery_report(&mock);
    eprintln!(
        "T4: 200 write-once keys, deliveries={delivered} (repeated keys={repeated} across the 5 \
         half answers), cut firings={}, last_error={err}",
        mock.cut_firings()
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

// ---------------------------------------------------------------- T5 target flapping

/// T5 (design §2): the target's port dies and lives again 8 times at 400 ms and then heals
/// permanently, while 1 000 orders land in 4 phases. The storm counts its own flips inside the
/// proxy (`FaultProxy::flips`) and every flip writes a fault-log event, so the seed gates on
/// flips it can observe rather than on a wall-clock guess, and a duplicate inside the window has
/// the event P4 demands. A refusal resets the connections the proxy was already relaying, so a
/// keep-alive pool cannot make the outage fictional, and the refusals are counted where the reset
/// is performed (`FaultProxy::refusals`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t5_target_flapping() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_t5").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("t5-flap");
    let tprog = FaultProxy::start(&mock.addr.to_string(), "target-flap", Some(log.clone())).await;
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target(
        "flap",
        "http",
        &format!("http://{}/hook", tprog.addr()),
        None,
        "{}",
    )
    .await;
    join_rule(&db, "flap", "flap", "orders").await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "t5_target_flapping",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_proxy(&tprog);
    sp.add_log(&lazy.log_path);

    tprog.flap_refuse(8, Duration::from_millis(400));
    let mut cps: Vec<(String, u64)> = Vec::new();
    for phase in 0..4u32 {
        let (from, to) = (1 + phase * 250, (phase + 1) * 250);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "flap",
            &format!("phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &mock,
        );
        cps.push(cp);
    }

    // The storm is the gate: 8 flips, half of them the port dead, all of them logged.
    wait_for(
        Duration::from_secs(60),
        "T5: the target port flipping 8 times",
        || async { (tprog.flips() >= 8).then_some(()) },
    )
    .await;
    assert_eq!(
        tprog.flips(),
        8,
        "T5: flap_refuse(8) flipped {} times: the storm the seed reports is not the storm the \
         proxy ran",
        tprog.flips()
    );
    assert_eq!(
        log.count("flap_refuse flip #"),
        8,
        "T5: {} of the 8 flips wrote a fault-log event; without them a duplicate in the window \
         would be an accident of timing rather than a legitimate redelivery (P4)",
        log.count("flap_refuse flip #")
    );
    // The engine's own pool was held across the flips, so at least one connection had to be
    // refused service: counted where the reset is performed, in the accept loop and in the relay
    // loop alike.
    assert!(
        tprog.refusals() >= 1,
        "T5: the port died 4 times for 400 ms each while the engine kept retrying, and not one \
         connection was refused (refusals=0): the outage never touched the wire"
    );
    // The engine's side of the storm, in the row an operator reads. The harness proxy accepts and
    // then resets, so a refused connect is `connecting to HOST:PORT: Connection reset by peer`
    // (src/http.rs) — the message has to name the port this seed took down.
    let err = wait_rule_error(&db, "flap", "Connection reset by peer", 60).await;
    assert!(
        err.contains(&tprog.addr().to_string()),
        "T5: the rule's last_error is `{err}`: it does not name the target's port ({}) whose \
         port the storm killed, so nothing here ties the outage the seed made to the outage the \
         rule reported",
        tprog.addr()
    );
    sp.check();
    log.add(format!(
        "storm over: {} flips, {} refused connections, last_error={err}",
        tprog.flips(),
        tprog.refusals()
    ));
    // `heal()` ends the storm for good (design §2: heals permanently after 8 flips) — a task
    // still flipping would inject a fault nobody asked for into the drain.
    tprog.heal();
    drain(&sp, &lazy, &db, "flap", cps[3].clone(), 120).await;
    sp.check();
    assert_all_keys(&mock, 1, 1000, "T5 target");
    let (delivered, repeated) = delivery_report(&mock);
    eprintln!(
        "T5: 1000 write-once keys, deliveries={delivered} (repeated keys={repeated} inside the \
         flap window), flips={}, refusals={}, last_error={err}",
        tprog.flips(),
        tprog.refusals()
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

// ------------------------------------------------- T6 one of two endpoints down

/// T6 (design §2): a target with two endpoints, where the second one's port is taken down after
/// the first phase has flowed cleanly to both of them. `send_all` sends to every endpoint at once
/// and the action is confirmed only when all of them answer (src/action.rs), so the healthy
/// endpoint *does* take the keys and the whole action still fails: a healthy sibling endpoint
/// cannot carry a rule whose other endpoint never answered. Asserted: the clean baseline both
/// endpoints answer phase 1 and the row persists its confirmation before the fault, with nothing
/// refused; the dead port then refuses and logs it (its own counter and its own log lines, not the
/// mode string), the rule's `last_error` names the dead endpoint and not the healthy one, the
/// position stays below the first checkpoint b could not confirm while `/status` still owes keys,
/// and after the heal both endpoints have every one of the 1000 keys and no key was struck.
///
/// Reported, not asserted: how many times the *healthy* endpoint was redelivered during the
/// outage. That is the product's designed shape (the fan-out above), and the harness cannot
/// witness it either: a retry that reuses a pooled connection is refused inside the client's own
/// connect/write path, so no accept reaches the proxy to be counted or logged, and registering
/// a's keys as a sampler phase would have P4 demand a fault event that no component ever
/// produced. The duplicate rule is therefore carried by b's phases, where it is sound — b is
/// silent all outage and answers each key exactly once after the heal — and the reason a's side
/// is reported rather than asserted is written where the phases are registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t6_target_one_of_two_endpoints_down() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_t6").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let a = Mock::start().await;
    a.set_fault_log(log.clone());
    a.tag("t6-endpoint-a");
    let b = Mock::start().await;
    b.set_fault_log(log.clone());
    b.tag("t6-endpoint-b");
    let bprog = FaultProxy::start(&b.addr.to_string(), "endpoint-b", Some(log.clone())).await;
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    // Both endpoints of one target: the engine fans out to both and needs both to confirm.
    db.target(
        "two",
        "http",
        &format!("http://{}/hook,http://{}/hook", a.addr, bprog.addr()),
        None,
        "{}",
    )
    .await;
    join_rule(&db, "two", "two", "orders").await;
    // The outage is armed *after* phase 1 has flowed to both endpoints and its confirmation is
    // read back from the status row. Two things depend on that order:
    //
    // * the harness's own — `refuse(true)` is only believed by an engine that is already holding a
    //   keep-alive connection to the port, because that is the case it has to survive: the refusal
    //   bumps the proxy's generation, which takes down the connections it was already relaying
    //   (`relay` compares its own generation against the proxy's). Arming it before the child
    //   exists means no such connection is ever born, the bump decides nothing, and the seed would
    //   pass with or without it. Here, b's generation goes up while a live relay is serving the
    //   engine's pool, and the mutant that drops the bump is the difference between the next
    //   actions failing and everything going green.
    // * and the sampler's — the phases registered below are watched from a moment when their keys
    //   are still owed, which is only meaningful if they were not already all delivered.
    //
    // Phase 1 is therefore *not* a fault phase and has no sampler phase of its own: its keys must
    // arrive cleanly (asserted below), and the first fault event happens after them.
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "t6_target_one_of_two_endpoints_down",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_proxy(&bprog);
    sp.add_log(&lazy.log_path);

    let mut cps: Vec<(String, u64)> = Vec::new();

    // ---- Phase 1, both endpoints alive. No sampler phase, no fault: this is the baseline that
    // makes the rest of the seed mean something. Both mocks have the whole set, and the status row
    // has *persisted* the confirmation, so the engine is proved capable of fanning out and
    // confirming before anything is taken away from it — and its HTTP pool is proved to be holding
    // a live relay to b, which is what makes `refuse`'s generation bump below a load-bearing
    // statement rather than an unused line.
    bulk_orders(&db, 1, 250).await;
    let cp1 = binlog_end(&db).await;
    db.tick().await;
    cps.push(cp1.clone());
    let want1 = keyset(1, 250);
    wait_for(
        Duration::from_secs(60),
        "T6 phase 1: both endpoints answering the first 250 keys",
        || async {
            (a.keys_received().len() >= 250 && b.keys_received().len() >= 250).then_some(())
        },
    )
    .await;
    assert_keys(&a, &want1, "T6 phase 1 at endpoint a");
    assert_keys(&b, &want1, "T6 phase 1 at endpoint b");
    wait_for(
        Duration::from_secs(60),
        "T6 phase 1: the confirmation persisted in the status row",
        || async {
            match db.status("two").await {
                Some((f, p, _, _)) if (f.as_str(), p) >= (cp1.0.as_str(), cp1.1) => Some(()),
                _ => None,
            }
        },
    )
    .await;
    // Nothing has been refused yet, so everything the next lines claim about the outage is the
    // outage's doing and not the state the seed started in.
    assert_eq!(
        bprog.refusals(),
        0,
        "T6: endpoint b's port had refused {} connections before the seed took it down, so the \
         baseline is not clean and the outage cannot be attributed",
        bprog.refusals()
    );

    // ---- Take the second endpoint down, with a live relay in hand.
    log.add(format!(
        "T6: phase 1 confirmed on both endpoints ({} keys at b), taking endpoint b's port down",
        b.keys_received().len()
    ));
    bprog.refuse(true);

    for phase in 1..4u32 {
        let (from, to) = (1 + phase * 250, (phase + 1) * 250);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        // The fault phases are registered against the *dead* endpoint, and that choice is the point.
        // P2 asks "while these keys have not reached that mock, the rule's position must stay
        // below this checkpoint" — with b refusing, that is the live assertion this seed needs for
        // the whole outage. P4's "a second delivery needs a fault event in between" then watches
        // b's deliveries too, where it is sound: b is silent during the outage and answers every
        // key once after the heal. It cannot watch a's, because `send_all` fans out and the
        // action only confirms when both endpoints answer, so a redelivery at the healthy a is
        // the expected shape — and the proxy cannot log an event for each of them: a retry that
        // reuses a pooled connection is refused inside the client's own connect/write path, and
        // no accept ever reaches the proxy to be counted or logged. A seed that registered a's
        // phases would therefore fail on a redelivery the harness has no way to explain, which is
        // a harness gap, not a product bug — so a's side is asserted by its own counters below
        // and reported, and the duplicate rule here is carried by b.
        sp.add_phase(
            "two",
            &format!("phase {} (endpoint b)", phase + 1),
            cp.clone(),
            keyset(from, to),
            &b,
        );
        cps.push(cp);
    }

    // One endpoint is dead, and the port itself says so — counted where the reset is performed,
    // and written to the fault log so that P4 has an event to point at.
    wait_for(
        Duration::from_secs(60),
        "T6: endpoint b's port refusing connections",
        || async { (bprog.refusals() >= 1).then_some(()) },
    )
    .await;
    assert!(
        log.count("proxy endpoint-b refused a connection") >= 1,
        "T6: the port refused {} connections and wrote {} fault-log events for them: a refusal \
         nobody logged is a refusal P4 cannot explain (the counters and the log are written at the \
         same place on purpose, and this is the assert that notices if one of them moves)",
        bprog.refusals(),
        log.count("proxy endpoint-b refused a connection")
    );
    // The outage is the rule's own reason for not moving, and it names the dead endpoint.
    let err = wait_rule_error(&db, "two", "Connection reset by peer", 60).await;
    assert!(
        err.contains(&bprog.addr().to_string()),
        "T6: the rule's last_error is `{err}`: it does not name the dead endpoint \
         ({}) the seed took down, so an operator reading the row could not find the outage",
        bprog.addr()
    );
    assert!(
        !err.contains(&a.addr.to_string()),
        "T6: the rule's last_error is `{err}`: the healthy endpoint is reported as failing \
         too, which is what the other half of the invariant forbids"
    );
    // The whole action fails, so nothing is confirmed: the position stays below the first
    // checkpoint the engine could not confirm on *both* endpoints (phase 1's own checkpoint is
    // already behind it, which is what the clean baseline above proved), and `/status` still owes
    // keys.
    wait_pending(&lazy, "two", 30).await;
    let (f, p, _, _) = db.status("two").await.expect("two rule row");
    assert!(
        (f.clone(), p) < cps[1],
        "the rule reached {f}:{p} while endpoint b was down and checkpoint {}:{} was unconfirmed: \
         a healthy sibling endpoint cannot carry a rule whose other endpoint never answered",
        cps[1].0,
        cps[1].1
    );
    sp.check();
    log.add(format!(
        "endpoint b down: {} refused connections, rule pinned at {f}:{p}, healing b",
        bprog.refusals()
    ));
    bprog.heal();
    drain(&sp, &lazy, &db, "two", cps[3].clone(), 120).await;
    sp.check();
    assert_all_keys(&a, 1, 1000, "T6 endpoint a");
    assert_all_keys(&b, 1, 1000, "T6 endpoint b");
    let (delivered_a, repeated_a) = delivery_report(&a);
    let (delivered_b, repeated_b) = delivery_report(&b);
    eprintln!(
        "T6: 1000 write-once keys, endpoint a deliveries={delivered_a} (repeated \
         {repeated_a}), endpoint b deliveries={delivered_b} (repeated {repeated_b}), b's port \
         refused {} connections while it was down, last_error={err}",
        bprog.refusals()
    );
    // Both endpoints answered every key: `send_all` has no notion of one endpoint covering
    // for the other, so a rule is confirmed only when both of them have the whole set.
    assert_eq!(
        a.keys_received().len(),
        1000,
        "T6: endpoint a received {} distinct keys, not the 1000 the rule sent",
        a.keys_received().len()
    );
    assert_eq!(
        b.keys_received().len(),
        1000,
        "T6: endpoint b received {} distinct keys, not the 1000 the rule sent",
        b.keys_received().len()
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
    ready_or_skip!(lazy1, 60);
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
    // The other half of what a restart reads. `choose_start` resumes at the MINIMUM of the
    // instance row and every rule row (src/engine.rs:1094), so the child's start is not the rule
    // row on its own: the first MariaDB run of this seed had the instance row still at
    // mysql-bin.000002:10222 while rule `o` had been confirmed at 29536, and the restarted child
    // legitimately started at the older of the two (evidence/proof-mar2.log). Read the pair, and
    // require it to have settled — the flush is held, so nothing may be confirmed after here, and
    // a pair taken from a moving engine would make the expectation below meaningless.
    let (sf, spos) = instance_row(&db)
        .await
        .expect("instance row before the kill");
    sleep_ms(300).await;
    let (pf2, pp2, _, _) = db.status("o").await.expect("status row, second read");
    let (sf2, spos2) = instance_row(&db).await.expect("instance row, second read");
    assert!(
        (sf.as_str(), spos) <= (sf2.as_str(), spos2) && (pf.as_str(), pp) <= (pf2.as_str(), pp2),
        "a saved row went backwards while the child was held mid-flush: instance {sf}:{spos} -> \
         {sf2}:{spos2}, rule {pf}:{pp} -> {pf2}:{pp2} — nothing was confirmed in between, so \
         neither row may move back"
    );
    let (pf, pp) = (pf2, pp2);
    let (sf, spos) = (sf2, spos2);
    let (ef, ep) = if (sf.as_str(), spos) <= (pf.as_str(), pp) {
        (sf.clone(), spos)
    } else {
        (pf.clone(), pp)
    };
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
    ready_or_skip!(lazy2, 60);
    sp.add_log(&lazy2.log_path);
    drain(&sp, &lazy2, &db, "o", cp2.clone(), 75).await;
    sp.check();

    // P6: the restart resumes from the saved rows, so positions never go backwards (monotonicity
    // itself is the sampler's claim; this is the one place the resume point is read directly).
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
        Some(format!("{ef}:{ep}").as_str()),
        "the restarted child did not start at the minimum of the two saved rows: the instance row \
         was {sf}:{spos} and the rule row {pf}:{pp}, so `choose_start` had to resume at \
         {ef}:{ep} (re-reading the range the newer row already covers is what that row's \
         `skip_until` skips)"
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

// ------------------------------------------------------------------- source class, PR 2

/// The stream's silence-detection floor, `max(3 × LAZYWF_HEARTBEAT_MS, 15 s)` in `src/engine.rs`
/// with the suite's `LAZYWF_HEARTBEAT_MS=1000`: **15 s**. Design §6 budgets it into the bounds
/// instead of hoping it away, so every deadline below that has to cover a dead stream carries it
/// explicitly (the drain bound of a stall seed is `60 s + SILENCE_FLOOR`), and no assertion here
/// ever reads "after X seconds" — the seeds wait for the engine's own observable responses.
const SILENCE_FLOOR: Duration = Duration::from_secs(15);

/// One bulk statement of `items` rows, as the design loads the big-transaction seed. The row
/// shape is fixed (`name` is padded to a stable width) so the byte calibration means something.
async fn bulk_items(db: &Db, from: u32, to: u32) {
    let rows: Vec<String> = (from..=to)
        .map(|i| format!("({i}, {}, 'item-{i:07}', 1)", i % 1000))
        .collect();
    db.exec(&format!("INSERT INTO items VALUES {}", rows.join(",")))
        .await;
}

/// The proxy's connection-accept instants, for a timing claim measured at the wire rather than
/// guessed from the clock.
async fn accept_gaps(p: &FaultProxy, from: u64) -> Vec<Duration> {
    let marks = p.accept_marks(from);
    marks.windows(2).map(|w| w[1] - w[0]).collect()
}

/// A suffix unique per run and process, for an XA xid that cannot collide with another tenant of
/// the shared server or with an earlier run of this seed.
fn rand_suffix() -> String {
    use std::hash::{BuildHasher, Hasher};
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.write_u64(u64::from(std::process::id()));
    h.write_u64(SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    format!("{:08x}", h.finish() & 0xffff_ffff)
}

/// The boundary a rule must not pass while the `skip`-th group after `from` that writes `table`
/// is unconfirmed: the position *before* that group, read back from the binlog. A
/// [`binlog_end`] snapshot taken around the write cannot serve as this bound — the suite runs two
/// threads against one server, so another session's commit can land between the snapshot and the
/// group, and a position that reaches it is perfectly legitimate. Walking by what *closes* a group
/// (`Xid`/`XA_prepare`/a `Query` that is not `BEGIN`, identical on both servers, the same
/// convention `XaPin` uses) and stopping at the `skip`-th `Table_map` of this schema's `table`
/// gives the group's own start, which is what a half-read group must never be confirmed past.
async fn rows_group_start(
    db: &Db,
    from: &(String, u64),
    table: &str,
    skip: usize,
) -> (String, u64) {
    // How the two servers print a Table_map's target differs in punctuation only (MySQL writes
    // ``Table_map: `db`.`t` mapped to table 1147``, MariaDB without the backquotes), so the needle
    // is compared with backquotes removed and both the parenthesised and the bare spelling accepted
    // — the same reading e2e's `table_info` uses, made spelling-agnostic so that a version
    // difference can never turn into "the write is not in the binlog" and accuse the engine.
    let paren = format!("({}.{})", db.schema, table).to_uppercase();
    let bare = format!("{}.{}", db.schema, table).to_uppercase();
    let mut c = db.conn().await;
    let rows: Vec<mysql_async::Row> = c
        .query(format!(
            "SHOW BINLOG EVENTS IN '{}' FROM {} LIMIT 40000",
            from.0.replace('\'', "''"),
            from.1
        ))
        .await
        .unwrap_or_else(|e| panic!("SHOW BINLOG EVENTS from {from:?} for the {table} group: {e}"));
    quit(c).await;
    let mut group = from.1;
    let mut seen = 0usize;
    // For the panic message: what the Table_map lines *do* say, so a needle that stopped matching
    // names the format instead of only accusing the write of being absent.
    let mut others: Vec<String> = Vec::new();
    for row in rows {
        let kind: String = row.get(2).expect("Event_type");
        let info: String = row.get(5).unwrap_or_default();
        let up = info.to_uppercase();
        let norm = info.replace('`', "").to_uppercase();
        if kind == "Table_map" {
            if norm.contains(&paren) || norm.contains(&bare) {
                if seen == skip {
                    return (from.0.clone(), group);
                }
                seen += 1;
            }
            if others.len() < 4 {
                others.push(clip(&info, 90));
            }
        }
        if kind == "Xid"
            || kind.eq_ignore_ascii_case("XA_prepare")
            || (kind == "Query" && !up.starts_with("BEGIN"))
        {
            group = row.get(4).expect("End_log_pos");
        }
    }
    panic!(
        "no group writing {}.{} ({} Table_map groups of it after {from:?}, looking for #{skip}) \
         in {}: the write the seed is waiting for is not in the binlog at all. The Table_map lines \
         there do say: {others:?}",
        db.schema, table, seen, from.0
    );
}

/// A string clipped to `n` chars, for a panic message that has to quote without flooding.
fn clip(s: &str, n: usize) -> String {
    let t: String = s.chars().take(n).collect();
    if s.chars().count() > n {
        format!("{t}…")
    } else {
        t
    }
}

/// The server's own clock, so a status row can be asked whether it was written after a moment
/// without comparing two machines (e2e's `server_now`, same reason).
async fn server_now(db: &Db) -> String {
    let mut c = db.conn().await;
    let row: mysql_async::Row = c
        .query_first("SELECT DATE_FORMAT(CURRENT_TIMESTAMP(3), '%Y-%m-%d %H:%i:%s.%f')")
        .await
        .unwrap()
        .expect("CURRENT_TIMESTAMP");
    quit(c).await;
    row.get::<String, _>(0).unwrap()
}

/// `(file, pos, reset_state, last_error)` of a rule's status row *written after* `written_after`
/// (the server's clock): a floor read that could be answered by a row left over from an earlier
/// persist would prove nothing.
async fn fresh_row(
    db: &Db,
    rule: &str,
    written_after: &str,
) -> Option<(String, u64, String, Option<String>)> {
    let mut c = db.conn().await;
    let r = c
        .exec_first(
            "SELECT binlog_file, binlog_pos, reset_state, last_error FROM lazy_wire_fire_status \
             WHERE rule = ? AND updated_at > ?",
            (rule, written_after),
        )
        .await
        .unwrap();
    quit(c).await;
    r
}

/// The keys of one flush body (`{{join keys ","}}`), as the mock saw them.
fn body_keys(mock: &Mock) -> BTreeSet<String> {
    mock.requests()
        .iter()
        .flat_map(|r| {
            r.body
                .split(',')
                .map(|k| k.trim().to_string())
                .filter(|k| !k.is_empty())
        })
        .collect()
}

/// The stream's own fault vocabulary: what the engine says when the dump connection died or
/// could not be opened, and the backoff it announced before the next attempt. Gating a fault
/// transition on these lines is what makes "the engine noticed" an observed event.
const STREAM_FAULT_PATTERNS: [&str; 5] = [
    "binlog connect failed",
    "the server closed the binlog stream",
    "binlog stream error",
    "no binlog event or heartbeat",
    "binlog dump at",
];

fn stream_fault_lines(lazy: &Lazy) -> Vec<String> {
    let mut out: Vec<String> = lazy
        .log_text()
        .lines()
        .filter(|l| STREAM_FAULT_PATTERNS.iter().any(|p| l.contains(p)))
        .map(String::from)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// How many such lines the child has logged, duplicates included: the deduplicated list above
/// is for reading a failure, this is for gating on "another one appeared after the fault I just
/// injected" (two identical lines are one line to a human and two reconnects to the engine).
fn stream_fault_count(lazy: &Lazy) -> usize {
    lazy.log_text()
        .lines()
        .filter(|l| STREAM_FAULT_PATTERNS.iter().any(|p| l.contains(p)))
        .count()
}

/// The `retrying in <dur>` of the engine's own reconnect message, in seconds (`None`: a line
/// that announces no wait). The child logs JSON, so the token ends at a quote or a brace, and
/// `Duration`'s Debug form is `500ms` / `1s` / `2.5s`.
fn announced_backoff_secs(line: &str) -> Option<f64> {
    let rest = line.split_once("retrying in ")?.1;
    let tok: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '.')
        .collect();
    if let Some(ms) = tok.strip_suffix("ms") {
        ms.parse::<f64>().ok().map(|v| v / 1000.0)
    } else if let Some(s) = tok.strip_suffix('s') {
        s.parse::<f64>().ok()
    } else {
        None
    }
}

/// Every reconnect wait the engine announced for a dump connection it could not open, in order:
/// one line per failed attempt, which is what a storm's spacing is counted from.
fn announced_backoffs(lazy: &Lazy) -> Vec<f64> {
    lazy.log_text()
        .lines()
        .filter(|l| l.contains("binlog connect failed"))
        .filter_map(announced_backoff_secs)
        .collect()
}

/// S2 (design §2): the dump port accepts and relays nothing, in either direction, so the
/// server's heartbeats die with the connection. 200 orders are committed while it is deaf.
///
/// Properties, each with the assertion that says it:
/// * the engine's OWN silence watchdog (`max(3×heartbeat, 15 s)`, `src/engine.rs`) fires while
///   the stall still holds — `wait_for` on the child's `no binlog event or heartbeat` line, whose
///   message must carry the 15 s floor;
/// * the heal is not what resumed it — the engine's next dump connection arrives at the proxy
///   *while `stat().mode` is still `stall`* (`FaultProxy::wait` on both);
/// * nothing is confirmed while stalled — the status row is exactly where it was when the stall
///   was armed and is below the checkpoint of the phase written during the outage (P2, asserted
///   by the sampler continuously and by the seed here);
/// * the outage is transient — zero `lazywf::undelivered` lines for it (P5);
/// * the backlog drains within the design's bound after the heal: `60 s + SILENCE_FLOOR`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_source_stall() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_s2").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("s2-hook");
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
    let mut probe_env: Vec<(&str, &str)> = proxy_env(&dsn_main, &dsn_bin, &dsn_q).to_vec();
    if std::env::var("LWF_PROBE").is_ok() {
        probe_env.push(("RUST_LOG", "debug"));
    }
    let mut lazy = Lazy::start("s2_source_stall", &db, &probe_env, Some(log.clone()));
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // Phase 1 flows cleanly first, so the stall is what stops phase 2 and not a slow start.
    bulk_orders(&db, 1, 200).await;
    let cp1 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("o", "phase 1", cp1.clone(), keyset(1, 200), &mock);
    drain(&sp, &lazy, &db, "o", cp1.clone(), 60).await;

    // The fault: the dump port holds connections open and relays nothing at all.
    let conns = binp.conns();
    let (pinned_f, pinned_p, _, _) = db.status("o").await.expect("row after phase 1");
    let server_before = binlog_end(&db).await;
    binp.stall(true);
    // The two references for "nothing moved while deaf", both taken AFTER arming: the engine's saved
    // position may still be settling phase 1 for a moment (bytes the proxy had already handed over
    // before the stall became real), so the position is reported rather than pinned, and what is
    // pinned is the proxy's own delivered counter — the only measurement that can say "the engine
    // was allowed to see nothing" without racing the engine's pipeline.
    let (ref_f, ref_p, _, _) = db
        .status("o")
        .await
        .expect("status row at the moment of arming");
    let delivered_at_arm = binp.delivered_s2c();
    log.add("phase 2 written while the dump role is stalled");

    // The outage's load: a phase whose commit the deaf stream cannot deliver.
    bulk_orders(&db, 201, 400).await;
    let cp2 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "o",
        "phase 2 (written during the stall)",
        cp2.clone(),
        keyset(201, 400),
        &mock,
    );
    let server_after = binlog_end(&db).await;

    // What the engine itself shows about the outage, sampled while it is on (the poll interval
    // is a poll interval, not a wait: every claim below keys on an event).
    let mut lag_max = 0u64;
    let mut stream_lag_max = 0u64;
    let t = Instant::now();
    while t.elapsed() < 3 * SILENCE_FLOOR / 4 {
        let s = lazy.status().await;
        lag_max = lag_max.max(s["rules"]["o"]["lag_ms"].as_u64().unwrap_or(0));
        stream_lag_max = stream_lag_max.max(s["stream_lag_ms"].as_u64().unwrap_or(0));
        sleep_ms(200).await;
    }

    // The outage was an outage, measured where the stall is actually implemented. The two halves
    // fail differently, so both are asserted:
    //
    // * the proxy handed the engine nothing between arming and here. This is the only one of the
    //   two that can be stated exactly. The engine's own saved position cannot be pinned to the
    //   byte: bytes the proxy had delivered a moment *before* arming are still in its
    //   read→flush→persist pipeline, and that pipeline does not freeze because the wire did — the
    //   first full-suite run of this assert failed with the position moving 82310 → 83720 while 0
    //   of 200 keys had leaked, which was the seed blaming the engine for its own race. The
    //   position is therefore reported (in the summary line), and the counter asserted here is the
    //   proxy's own, which counts only what the engine was allowed to see.
    // * not one key of the phase written *during* the stall has reached the target. This is the
    //   end-to-end version, and the one mutation M1 drives: revert the proxy's in-flight hold
    //   (`held_s2c` — the race MariaDB showed 0.7 s after arming, evidence/probe-s2-mariadb.log)
    //   and all 200 keys are at the mock while the stall is still armed, which used to leave this
    //   seed green while every claim below it was about a stream that had never been deaf.
    let phase2 = keyset(201, 400);
    let got2 = mock.keys_received();
    let leaked: Vec<&String> = phase2.iter().filter(|k| got2.contains(*k)).collect();
    let delivered_now = binp.delivered_s2c();
    assert_eq!(
        delivered_now,
        delivered_at_arm,
        "the proxy handed {} more server→client bytes to the engine while the dump port was \
         stalled ({} at arming, {delivered_now} now): the stream was never deaf, so the watchdog, \
         the reconnect and the drain claimed below are about a stream that was still being fed — \
         and {} of the 200 phase-2 keys are already at the target",
        delivered_now - delivered_at_arm,
        delivered_at_arm,
        leaked.len()
    );
    assert!(
        leaked.is_empty(),
        "{} of the 200 keys written during the stall reached the target before the heal (first \
         few {:?}): the proxy relayed bytes through a stall it was told to hold, so the stream \
         was never deaf and the reconnect and drain claims below are about nothing",
        leaked.len(),
        &leaked[..leaked.len().min(5)]
    );

    // The engine's OWN watchdog, not the heal: the floor is 15 s here, so this wait is the
    // floor plus the slack of a loaded host (`wait_for` scales it).
    let line = wait_for(
        Duration::from_secs(40),
        "the engine's own stream-silence watchdog to fire",
        || async {
            lazy.log_lines_matching("no binlog event or heartbeat")
                .into_iter()
                .next()
        },
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&line).expect("json log line");
    let msg = v["fields"]["message"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        msg.contains("15s"),
        "the silence watchdog did not wait the {SILENCE_FLOOR:?} floor \
         (max(3x heartbeat, 15 s) in src/engine.rs): {msg}"
    );
    // And it reconnects on its own initiative: a NEW dump connection shows up at the proxy while
    // the proxy is still stalling every byte of it. This is the assertion that the heal is not
    // what resumed the stream.
    binp.wait(
        |s| s.conns > conns && s.mode == "stall",
        "the engine's own reconnect attempt on the dump role, while the stall still holds",
    )
    .await;
    // The engine's own view of the dead stream while it waits for that connection.
    wait_for(
        Duration::from_secs(10),
        "/status to say the stream is not streaming",
        || async {
            let s = lazy.status().await;
            (!s["streaming"].as_bool().unwrap_or(true)).then_some(())
        },
    )
    .await;

    // Nothing was confirmed while the stream was deaf: the position is read twice, a status
    // period apart, and must not have moved (an engine that reads no events has nothing to
    // confirm), and it must not have gone backwards over the outage (P6).
    let (f1, p1, _, _) = db.status("o").await.expect("row during the stall");
    sleep_ms(600).await;
    let (f, p, _, _) = db
        .status("o")
        .await
        .expect("row during the stall, second read");
    assert_eq!(
        (f.clone(), p),
        (f1.clone(), p1),
        "P2: the rule position kept moving while the dump role was stalled: {f1}:{p1} and then \
         {f}:{p} — the stream could read nothing, so there was nothing to confirm"
    );
    assert!(
        (f.clone(), p) >= (pinned_f.clone(), pinned_p),
        "P6: the rule position went backwards over the outage: it is {f}:{p} and was \
         {pinned_f}:{pinned_p} when the stall was armed"
    );
    // The bound is the group's OWN start, read back from the binlog — not `cp2`, a snapshot of
    // where the *server* stood right after the write: this suite runs two threads on one server,
    // and another session's commit between the write and the snapshot makes such a snapshot
    // meaningless as a bound. A position at or below the group's start says the engine never
    // entered the group whose 200 keys the target has not seen.
    if std::env::var("LWF_PROBE").is_ok() {
        eprintln!(
            "PROBE row=({f}:{p}) pinned=({pinned_f}:{pinned_p}) cp1={cp1:?} cp2={cp2:?} \
             server_after={server_after:?} binp: mode={} conns={} bytes={} s2c={} cuts={}",
            binp.stat().mode,
            binp.conns(),
            binp.bytes(),
            binp.bytes_s2c(),
            binp.stat().byte_cuts
        );
        eprintln!("PROBE child log:\n{}", lazy.log_text());
    }
    let gp2 = rows_group_start(&db, &cp1, "orders", 0).await;
    assert!(
        (f.clone(), p) <= gp2,
        "P2: rule o reached {f}:{p} during the stall, past {}:{}, the start of the group that \
         carries keys 201..=400 (the server stood at {}:{} when the write was made): the dump \
         connection relayed nothing, so that group was never read, let alone delivered",
        gp2.0,
        gp2.1,
        cp2.0,
        cp2.1
    );
    // The outage left a backlog: the server is ahead of the deaf engine by (at least) the group it
    // never read. The first version of this line also required the backlog to have *grown* over the
    // outage, and that is not true in general: bytes the proxy handed over a moment *before*
    // arming are still in the engine's read→flush→persist pipeline and keep being confirmed after
    // the stall is armed, so the gap may shrink — MariaDB measured 22 055 -> 12 318 while the proxy
    // delivered nothing and 0 of 200 keys leaked (evidence/proof-mar3.log). What the outage does
    // guarantee is asserted above (the position never entered the unread group, no phase-2 key
    // reached the target, the proxy's delivered counter did not move) and here: at the end of the
    // outage the server is still ahead of the engine by the unread group's bytes.
    let behind_before = gap(&server_before, &(pinned_f.clone(), pinned_p));
    let behind_after = gap(&server_after, &(f.clone(), p));
    assert!(
        server_after > (f.clone(), p) && behind_after > 0,
        "the stall left no backlog: the server is at {}:{} while the engine is at {f}:{p} \
         ({behind_before} bytes behind at arming, {behind_after} now) — the phase-2 group begins \
         at {}:{}, so a stream that read nothing cannot have come past it",
        server_after.0,
        server_after.1,
        gp2.0,
        gp2.1
    );
    sp.check();
    log.add(format!(
        "stall proved (watchdog + engine's own reconnect) at {f}:{p}, healing the dump role"
    ));
    binp.heal();
    // P3's bound for a stall episode: the 60 s the engine gives one binlog connect plus the 15 s
    // silence floor (design §2/§6) — the healed attempt may have to wait out the connect that is
    // already in flight. `deadline` scales the allowance for the load of the box; the measured
    // time is reported next to the unscaled bound so the margin is visible in the log.
    let t0 = Instant::now();
    drain(
        &sp,
        &lazy,
        &db,
        "o",
        cp2.clone(),
        deadline(Duration::from_secs(75)).as_secs(),
    )
    .await;
    sp.check();
    let drained = t0.elapsed();

    // P1: every key of both phases is at the target, including the 200 the engine never saw
    // while the stall held. P4's hard edge (no duplicate without a fault event in between) is
    // the sampler's; the counts are reported.
    assert_all_keys(&mock, 1, 400, "S2");
    eprintln!(
        "S2: {} deliveries for 400 write-once keys; dump conns={} (>= one reconnect of the \
         engine's own); phase 2 drained in {drained:?} (the design's bound: 75 s = the 60 s a \
         binlog connect may take + the {SILENCE_FLOOR:?} silence floor); frozen during the stall \
         at {f}:{p} (armed at {pinned_f}:{pinned_p}, the unread group of keys 201..=400 starts at \
         {}:{}, the server stood at {}:{}); the proxy delivered {delivered_at_arm} server→client \
         bytes by arming and {delivered_now} by the end of the outage ({} of the 200 phase-2 keys \
         reached the target meanwhile), and the engine's own row was {ref_f}:{ref_p} at arming and \
         {f}:{p} at the end; the server stayed {behind_after} bytes ahead of the engine through the outage
          ({behind_before} at arming — the gap may shrink, see the assert above); sampled during
          the stall: max rule lag_ms={lag_max}, \
         max stream_lag_ms={stream_lag_max} (see report.md: on a stalled idle stream the engine's own \
         idle timer sets caught_up, so its lag reads 0 and the observable proof of the outage is \
         the watchdog line + streaming=false + the frozen position)",
        mock.delivery_count(),
        binp.conns(),
        gp2.0,
        gp2.1,
        server_after.0,
        server_after.1,
        leaked.len(),
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

/// The distance between two positions of the same binlog file (the shared runner's server has
/// only this suite writing, so a file change during a seed is rare; `None` says so).
fn gap(a: &(String, u64), b: &(String, u64)) -> u64 {
    if a.0 == b.0 && a.1 >= b.1 {
        a.1 - b.1
    } else {
        u64::MAX
    }
}

/// S3 (design §2): every dump connection the proxy accepts is reset, six times, and then the
/// port is let through — 900 orders written across the storm.
///
/// Properties: all 900 keys arrive (P1), positions never regress (P6, the sampler), no
/// `lazywf::undelivered` line for the storm (P5), no duplicate outside a fault window (P4, the
/// sampler), and the engine's own reconnect backoff keeps the storm from spinning — measured, not
/// claimed: the proxy's accept timestamps give the spacing of the engine's attempts, and the
/// child's own `retrying in <backoff>` lines give the schedule it promised to keep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_source_flapping() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_s3").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("s3-hook");
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
        "s3_source_flapping",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);
    // Wait for the stream to be up before the storm, so the first reset is a real cut of a live
    // connection and not an engine that had not connected yet.
    wait_for(
        Duration::from_secs(30),
        "the engine to be streaming before the storm starts",
        || async {
            lazy.status().await["streaming"]
                .as_bool()
                .unwrap_or(false)
                .then_some(())
        },
    )
    .await;

    let conns0 = binp.conns();
    binp.flap(6);
    log.add("the dump port is flapping: every accepted connection is reset, 6 times");

    // The design's load: 3 x 300 interleaved with the storm, gated on the proxy's own count of
    // reset connections (never on how long the storm has been going).
    let mut cps = Vec::new();
    for phase in 0..3u32 {
        let (from, to) = (1 + phase * 300, (phase + 1) * 300);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "o",
            &format!("phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &mock,
        );
        cps.push(cp);
        let seen = phase as u64 * 2 + 2;
        binp.wait(
            move |s| s.flap_cuts >= seen,
            &format!("{seen} dump connections reset by the storm"),
        )
        .await;
    }
    // The storm ends by itself, on the proxy's count: six resets and the port live again.
    binp.wait(
        |s| s.flap_cuts == 6 && s.mode == "live",
        "the storm to be over: 6 reset connections and the port live again",
    )
    .await;
    drain(&sp, &lazy, &db, "o", cps[2].clone(), 90).await;
    sp.check();

    assert_all_keys(&mock, 1, 900, "S3");
    assert_no_undelivered(&lazy);

    // What was measured, and what it has to satisfy. (1) the storm was real: the proxy counted
    // exactly the design's six resets, and the engine tried to connect that many more times. The
    // two counters are asked for separately on purpose: `flap_cuts` says how many connects the
    // storm *claimed*, `flap_resets` says how many sockets were actually given an RST. Mutation M2
    // (the reset call removed, the counting left alone) left every connection to be closed by the
    // normal drop — a plain EOF, not the reset the seed injects — the engine reconnected from that
    // anyway, and S3 was green. So the claim "every accepted dump connection was reset" is only
    // backed by the counter taken where the reset is performed. What the peer's side of that is
    // remains the asserts below about the engine's own reconnects, its announced backoff ladder and
    // the 900 keys.
    let st = binp.stat();
    assert_eq!(
        st.flap_cuts, 6,
        "the flap reset {} connections, not the six the seed injects",
        st.flap_cuts
    );
    assert_eq!(
        st.flap_resets, 6,
        "the flap counted {} cuts but only reset {} connections: a storm that decrements its own \
         counter without touching the sockets is a number, not a fault, and nothing below this \
         line would have noticed",
        st.flap_cuts, st.flap_resets
    );
    assert!(
        st.conns >= conns0 + 6,
        "the engine made only {} dump connection attempts across a storm that reset six of them",
        st.conns - conns0
    );
    // (2) it did not spin: the engine announced a growing backoff, and it kept the promises it
    // made in those lines.
    let faults = stream_fault_lines(&lazy);
    let announced = announced_backoffs(&lazy);
    assert!(
        announced.len() >= 5,
        "the engine announced only {announced:?} reconnect waits across a storm that reset six \
         connections; the child's stream lines are:\n{}",
        faults.join("\n")
    );
    assert!(
        announced.iter().cloned().fold(0f64, f64::max) >= 1.0,
        "the reconnect backoff never reached a second during the storm (announced {announced:?}): \
         the engine was re-dialing instead of backing off"
    );
    let gaps = accept_gaps(&binp, conns0).await;
    let spin = gaps
        .iter()
        .skip(1)
        .filter(|g| **g < Duration::from_millis(300))
        .count();
    assert_eq!(
        spin,
        0,
        "the storm spun: {} of {} reconnect gaps were under 300 ms (all: {:?})",
        spin,
        gaps.len(),
        gaps.iter()
            .map(|g| g.as_millis() as u64)
            .collect::<Vec<_>>()
    );
    eprintln!(
        "S3: {} deliveries for 900 write-once keys; dump conns={} (+{} over the pre-storm {}), \
         proxy flap_cuts={}, reconnect waits announced by the engine={:?}, gaps between dump \
         connections (ms)={:?}",
        mock.delivery_count(),
        st.conns,
        st.conns - conns0,
        conns0,
        st.flap_cuts,
        announced,
        gaps.iter()
            .map(|g| g.as_millis() as u64)
            .collect::<Vec<_>>()
    );
    lazy.kill();
    sp.stop();
}

/// S4 (design §2): one `INSERT` of 50 000 `items` rows is one binlog group far bigger than the
/// MTU, and the dump connection is RST at about half of it (`cut_after_bytes`, calibrated by
/// measuring a 1 000-row group through the same connection and halving the extrapolation).
///
/// Properties: during the cut the rule position never passes the pre-INSERT checkpoint (P2 — the
/// group is a boundary, so a position past it would be a position *inside* the group), no key of
/// the group reaches the target before the re-read, and after the heal all 50 000 keys arrive —
/// each exactly once, which is the assertion that the re-read of the partial group was deduped
/// into one flush. The child's peak RSS is measured against a stated cap: a chaos test that eats
/// the box is not a pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_source_cut_during_big_txn() {
    // The cap this seed is allowed to take: the child holds a 50 000-key dedup buffer plus the
    // rows of one group. Measured across the proof runs it sits far under this; the number is
    // reported with the run.
    const RSS_CAP_KIB: i64 = 512 * 1024;
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_s4").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("s4-hook");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "i", "hook", "items").await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "s4_source_cut_during_big_txn",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // Calibration, measured rather than assumed: a 1 000-row group of the same row shape goes
    // through the same dump connection, and the bytes it cost say what 50 000 rows will cost.
    // An idle window of its own is measured first so the server's own chatter is not counted as
    // the group's bytes.
    let idle_from = Instant::now();
    let b0 = binp.bytes_s2c();
    sleep_ms(2_000).await;
    let idle_rate = (binp.bytes_s2c() - b0) as f64 / idle_from.elapsed().as_secs_f64().max(0.5);
    let b1 = binp.bytes_s2c();
    let t1 = Instant::now();
    bulk_items(&db, 900_001, 901_000).await;
    db.tick().await;
    wait_for(
        Duration::from_secs(60),
        "the 1 000 calibration keys at the mock",
        || async {
            let got = mock.keys_received();
            (900_001..=901_000)
                .all(|i| got.contains(&i.to_string()))
                .then_some(())
        },
    )
    .await;
    let probe_bytes = (binp.bytes_s2c() - b1) as f64 - idle_rate * t1.elapsed().as_secs_f64();
    let per_row = probe_bytes / 1_000.0;
    let group_est = (per_row * 50_000.0) as u64;
    assert!(
        group_est > 100_000,
        "the calibration says a 50 000-row group is {group_est} bytes ({per_row:.1} per row): \
         the probe did not measure a real group, so the cut point would mean nothing"
    );
    let cut_at = group_est / 2;

    // The boundary the group begins at. The settle of the tool's own writes can reach it, so the
    // mid-cut claim below is "the position never passes it": a position past this line is a
    // position *inside* the group, and a transaction boundary is never inside a group (P2).
    db.tick().await;
    let cp0 = binlog_end(&db).await;
    let conns = binp.conns();
    binp.cut_after_bytes(cut_at);
    log.add(format!(
        "50 000-row items group written with the dump cut armed at {cut_at} bytes (group \
         estimated {group_est})"
    ));
    bulk_items(&db, 1, 50_000).await;
    // The group's own end is the phase checkpoint (PR 1's shape: the checkpoint of a phase is
    // the end of the group whose keys are pending — while they are unconfirmed the position may
    // not pass it, and the drain asserts it does).
    let cp_after = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "i",
        "the 50 000-row group",
        cp_after.clone(),
        (1..=50_000).map(|i| i.to_string()).collect(),
        &mock,
    );

    // The cut is gated on the proxy's own report that it fired, and on the group's keys still
    // being unseen: together those two say the reset landed *inside* the group.
    binp.wait(
        |s| s.byte_cuts >= 1,
        &format!("the byte-bounded cut to fire (armed at {cut_at} bytes)"),
    )
    .await;
    let got = mock.keys_received();
    let leaked: Vec<u32> = (1..=50_000u32)
        .filter(|i| got.contains(&i.to_string()))
        .collect();
    assert!(
        leaked.is_empty(),
        "{} keys of the group (e.g. {:?}) were delivered although the dump connection was reset \
         after {} of the group's ~{group_est} bytes: the cut did not land inside the group",
        leaked.len(),
        leaked.iter().take(5).collect::<Vec<_>>(),
        cut_at
    );
    // The bound is the group's own start, read back from the binlog: `cp0` is only where the
    // server stood when the seed armed the cut, and the suite's other thread commits on the same
    // server between the two.
    let grp = rows_group_start(&db, &cp0, "items", 0).await;
    let (f, p, _, _) = db.status("i").await.expect("status row during the cut");
    assert!(
        (f.clone(), p) <= grp,
        "P2: rule i reached {f}:{p} during the cut, past {}:{}, the boundary the 50 000-row \
         group begins at (the server stood at {}:{} when the cut was armed): a position past a \
         group's start is a position inside the group, and the group was not read, let alone \
         delivered",
        grp.0,
        grp.1,
        cp0.0,
        cp0.1
    );
    sp.check();

    // Heal: the partial group is re-read from the boundary, and all 50 000 keys must arrive.
    binp.wait(
        |s| s.conns > conns,
        "the engine's reconnect to re-read the partial group from its boundary",
    )
    .await;
    log.add("healing the dump role: the group is re-read from the boundary");
    let bytes_at_heal = binp.bytes_s2c();
    binp.heal();
    drain(&sp, &lazy, &db, "i", cp_after.clone(), 90).await;
    sp.check();
    // What the dump connection carried after the heal command. Reported, not asserted: the cut is
    // a one-shot, so the engine may already have re-read the group before this instant, and the
    // figure also contains the stream's own heartbeats.
    let bytes_after_heal = binp.bytes_s2c() - bytes_at_heal;

    assert_all_keys(&mock, 1, 50_000, "S4");
    // The "once" of the design's row: the re-read of the partial group was deduped into one
    // flush, so no key of the group may appear in two deliveries.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut dup = 0usize;
    let mut flushes = 0usize;
    for (_, keys) in mock.deliveries() {
        let mine: Vec<String> = keys
            .iter()
            .filter(|k| k.parse::<u32>().is_ok_and(|i| (1..=50_000).contains(&i)))
            .cloned()
            .collect();
        if mine.is_empty() {
            continue;
        }
        flushes += 1;
        for k in mine {
            if !seen.insert(k) {
                dup += 1;
            }
        }
    }
    assert_eq!(
        dup, 0,
        "{dup} keys of the 50 000-row group were delivered twice: the re-read after the cut was \
         not deduped into one flush ({flushes} deliveries carried them)"
    );
    assert_eq!(
        seen.len(),
        50_000,
        "only {} of the 50 000 keys were delivered",
        seen.len()
    );
    // The cut point is the design's own claim ("RST at about half of the group"), so it is
    // checked against bytes the proxy counted, not against the estimate that armed it: the reset
    // landed after at least a third of the probed group had flowed (the group was inside its own
    // flow when the connection died) and before three quarters of it (the group cannot have been
    // read and confirmed already). `group_bytes` — what the re-read cost on the wire — is
    // reported with the run.
    let st = binp.stat();
    assert!(
        st.bytes_at_cut >= group_est / 3 && st.bytes_at_cut <= group_est * 3 / 4,
        "the cut fired {} bytes past its arming point, which is not inside the 50 000-row group \
         the calibration measured at ~{group_est} bytes ({per_row:.1} B/row over a \
         {probe_bytes:.0}-byte 1 000-row probe): the design's row says the reset lands at about \
         half of the group",
        st.bytes_at_cut,
    );
    let rss = lazy.peak_rss_kib();
    assert!(
        (0..RSS_CAP_KIB).contains(&rss),
        "the child's peak RSS was {rss} KiB, over the {RSS_CAP_KIB} KiB this seed allows a \
         50 000-key load (a chaos test that eats the box is not a pass)"
    );
    eprintln!(
        "S4: 50 000 keys in {flushes} deliveries (batch split), no key twice; the cut fired at \
         {} bytes past its arming point (armed at {cut_at}, the calibration says a 50 000-row \
         group is ~{group_est} bytes: {per_row:.1} B/row over a {probe_bytes:.0}-byte 1 000-row \
         probe); the group begins at {grp:?} (the arming snapshot was {cp0:?}); dump bytes after \
         the heal: {bytes_after_heal}; child peak RSS {rss} KiB of the {RSS_CAP_KIB} KiB allowed; \
         dump conns={}",
        st.bytes_at_cut,
        binp.conns()
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}

/// S5 (design §2): an XA that prepares before a plain transaction and commits after it, with the
/// dump cut between PREPARE and COMMIT — and, in the second round, the child SIGKILLed in that
/// window as well. Mirrors e2e's `xa_committed_after_a_later_transaction_keeps_its_floor_across_a_
/// restart` under fault.
///
/// Properties, each with the assertion that says it:
/// * a prepared-not-committed branch is invisible: no request and no delivery of the XA's key
///   before `XA COMMIT` is even issued (`Item::XaPrepare` keeps the marks waiting);
/// * the floor a restart needs is in the instance row (`resume_pos` counts `xa_pending`), so the
///   restarted child provably resumes at or below the XA's start — the row the *rule* holds is
///   above it, which is why this reads `rule = '*'` and not the rule's row;
/// * the floor at the XA's start holds on the rule's row while its keys are unflushed, read from
///   a row written *after* the refusal, with both keys already refused in one body (a row from a
///   stream that had not read the COMMIT yet would prove nothing — the trap e2e documents);
/// * after the COMMIT plus the heal both keys arrive, and duplicates only inside the fault
///   windows, which the sampler windows on the fault log.
///
/// Shared-server hygiene (design §4): an xid unique per run, an `XaGuard` that rolls back on
/// every path including a panic, and the suite-start `XA RECOVER` sweep, whose running and whose
/// result are asserted here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s5_source_cut_around_xa() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_s5").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();

    // Design §4: the sweep runs inside the runner's heavy-suite lock, before anything can leave
    // a branch prepared, and the server must be left with no leftover lwf_* branch of its own.
    let left = sweep_xa_once(&root, &log).await;
    assert!(
        sweep_ran(),
        "the suite-start XA RECOVER sweep never ran: a prepared branch abandoned here would pin \
         the shared server's binlog purge for every tenant"
    );
    assert!(
        left.is_empty(),
        "the XA sweep left prepared lwf_* transactions on the shared server: {left:?}"
    );

    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("s5-hook");
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
        "s5_source_cut_around_xa",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // Two rounds, one per fault in the design's row: the cut between PREPARE and COMMIT, then
    // the same window with the child SIGKILLed in it. Distinct keys per round (the XA's key and
    // 20 plain orders) keep the claims separable, and every transition is gated on an observed
    // event — the refusal counted at the mock, the proxy's counters, the engine's own stream
    // line, the restarted child's own start line — never on a sleep.
    let xid_seq = rand_suffix();
    let mut dead_logs: Vec<std::path::PathBuf> = Vec::new();
    for (idx, (round, kill)) in [("cut", false), ("cut + SIGKILL", true)]
        .into_iter()
        .enumerate()
    {
        let xa_key = 31 + idx as u32 * 30;
        let (plain_from, plain_to) = (xa_key + 1, xa_key + 20);
        let xid = format!("lwf_s5_{xid_seq}_{idx}");
        // Design §4: the xid is under the guard before `XA START`, so even a panic between the
        // two leaves no branch prepared on a server the other tenants share.
        let guard = XaGuard::new(&root, &xid, Some(log.clone()));
        let refused0 = log.count("=500 fired");

        // Nothing may be confirmed while keys are pending: every flush is refused, so the saved
        // position is the pinned floor alone (as in e2e's floor test). All of them refused means
        // no strikes — `STRIKES` drops a key refused while its action's others went through —
        // so P5's `lazywf::undelivered` stays a signal here and not noise.
        mock.respond_nth(200, 500);
        let xa_at = binlog_end(&db).await;
        let mut c = db.conn().await;
        for q in [
            format!("XA START '{xid}'"),
            format!("INSERT INTO orders VALUES ({xa_key}, 1, 'x', 1)"),
            format!("XA END '{xid}'"),
            format!("XA PREPARE '{xid}'"),
        ] {
            c.query_drop(&q)
                .await
                .unwrap_or_else(|e| panic!("{q} ({xid}): {e}"));
        }
        // The branch is now visible to the only reader the harness's sweep has. Asserting it shows up
        // here is what keeps `sweep_xa_once`'s detection half from being a line that never saw a
        // branch: at suite start there is nothing to find, so an empty list proves nothing about the
        // reader. (The *rollback* half cannot be pinned by a seed: leaving a branch no in-process
        // guard owns also blocks that schema's teardown, so the mutant that neuters the rollback
        // hangs instead of failing — it is demonstrated by hand instead, in
        // `evidence/xa-blocks-drop-*.log`, and reported that way in report.md §5.)
        let seen = xa_recover_lwf(&root).await;
        assert!(
            seen.iter().any(|x| x.name == xid || x.gtrid == xid),
            "round {round}: '{xid}' is prepared on the server but the sweep's own reader does not \
             list it (it sees {seen:?}) — a sweep that cannot see a branch cannot roll it back \
             either, so its `left.is_empty()` below would be aiming at nothing",
        );
        // A filler transaction of its own, so the plain group does not begin right after the
        // XA's (what e2e does, and why: a statement matching no rows writes no group at all).
        // Its key is drawn from the round, because a second `INSERT` of the same primary key
        // would be an error and write no group either.
        db.exec(&format!(
            "INSERT INTO order_tags VALUES ({}, 'filler')",
            xa_key + 1_000
        ))
        .await;
        let plain_at = binlog_end(&db).await;
        assert!(
            xa_at < plain_at,
            "the filler must move the position between the XA ({xa_at:?}) and the plain \
             transaction ({plain_at:?}): with no gap between them the floor proves nothing"
        );
        bulk_orders(&db, plain_from, plain_to).await;
        let after_plain = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "o",
            &format!("round {round}: the 20 plain orders"),
            after_plain.clone(),
            keyset(plain_from, plain_to),
            &mock,
        );

        // Where the XA's group really starts, read back from the binlog: `xa_at` is only a lower
        // bound, because another commit of this run can land between the snapshot and `XA START`
        // (the same read-back e2e does for the same reason). `XaPin::read` is also the *only* way
        // to obtain a floor for the sampler, which is what scopes the P6 exemption to the XA case.
        let pin = XaPin::read(&db, &xa_at, &xid).await;
        let xa_start = pin.pos().clone();
        assert!(
            xa_at <= xa_start && xa_start <= after_plain,
            "the XA's group must begin between {xa_at:?} (taken before XA START) and \
             {after_plain:?} (taken after the plain transaction), got {xa_start:?}"
        );
        // The one thing P6 must not call a violation: when the engine reads this XA's COMMIT it
        // applies the marks it had held since PREPARE and pins them at the group's own start
        // (`apply(marks, &pos, &xa.start)` in src/engine.rs), so the saved position moves *back*
        // to exactly here. Declared as a floor, not waived: below it is still a violation (the
        // floor's own teeth are what h1 proves), the exemption ends when the key is delivered, and
        // every accepted regression is listed in `Sampler::backslides` and in this seed's summary.
        sp.allow_xa_pin(
            "o",
            &format!("round {round}"),
            &pin,
            [xa_key.to_string()].into_iter().collect(),
            &mock,
        );

        // The gate: the plain keys reached the target and were *refused* (a counted firing of
        // this round's `respond_nth`, not an earlier round's), so the engine genuinely holds
        // them unconfirmed. Nothing 2xx'd, or this is not a pending state.
        wait_for(
            Duration::from_secs(30),
            &format!("round {round}: the plain flush refused at the mock"),
            || async {
                let keys = body_keys(&mock);
                let got = mock.keys_received();
                (log.count("=500 fired") > refused0
                    && keys.contains(&plain_from.to_string())
                    && keys.contains(&plain_to.to_string())
                    && !got.contains(&plain_from.to_string()))
                .then_some(())
            },
        )
        .await;

        // Property 1 (design §2 S5): the prepared-not-committed branch is invisible. The marks
        // wait for the COMMIT (`Item::XaPrepare`, src/engine.rs), so neither a request nor a
        // delivery carries the XA's key before `XA COMMIT` has even been issued.
        assert!(
            !body_keys(&mock).contains(&xa_key.to_string()),
            "key {xa_key} of the prepared-not-committed XA '{xid}' reached the target before \
             XA COMMIT: its marks were applied at PREPARE"
        );
        assert!(
            !mock.keys_received().contains(&xa_key.to_string()),
            "key {xa_key} of the prepared XA '{xid}' was delivered before XA COMMIT"
        );

        // Property 2a — the floor a restart needs is already in the instance row: `resume_pos`
        // counts an open `xa_pending` (src/engine.rs), which is what lets a child killed in this
        // window resume at or below the XA's start instead of past its COMMIT.
        // Both rows are the engine's own persist output, and a first flush refused by the mock can
        // out-run the row that describes it: waiting for them is waiting for an observed event.
        let (pf, pp, _, _) = wait_for(
            Duration::from_secs(30),
            &format!("round {round}: the rule row to exist with the XA's keys pending"),
            || async { db.status("o").await },
        )
        .await;
        let (rf, rp) = wait_for(
            Duration::from_secs(30),
            &format!("round {round}: the instance row to exist with the XA's keys pending"),
            || async { instance_row(&db).await },
        )
        .await;
        assert!(
            (rf.clone(), rp) <= xa_start,
            "the instance row does not carry the XA's pin: it is at {rf}:{rp} while the XA's \
             group starts at {}:{}, so a restart would resume past its PREPARE and lose key \
             {xa_key} (the rule row was {pf}:{pp})",
            xa_start.0,
            xa_start.1
        );

        // The fault, in the window between PREPARE and COMMIT — gated on the engine *seeing* it
        // (a new line in its own stream-fault vocabulary), not on the proxy call returning.
        let faults0 = stream_fault_count(&lazy);
        binp.cut_all();
        log.add(format!(
            "round {round}: the dump role is cut between PREPARE and XA COMMIT"
        ));
        wait_for(
            Duration::from_secs(40),
            &format!("round {round}: the engine to see the dump connection die"),
            || async { (stream_fault_count(&lazy) > faults0).then_some(()) },
        )
        .await;
        if kill {
            log.add(format!(
                "round {round}: killing the child at rule {pf}:{pp} / instance {rf}:{rp} with \
                 the XA prepared"
            ));
            // SIGKILL of the child this harness spawned (the fault log names the pid); the
            // harness reaps it, so the process is provably gone when this returns.
            lazy.kill();
            dead_logs.push(lazy.log_path.clone());
        }
        // `XA COMMIT` with the source cut (and, in round 2, no child at all): the branch is
        // prepared in the server, so the commit lands and only the binlog can tell the engine.
        c.query_drop(format!("XA COMMIT '{xid}'"))
            .await
            .unwrap_or_else(|e| panic!("XA COMMIT {xid}: {e}"));
        guard.mark_finished();
        let commit_at = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "o",
            &format!("round {round}: the XA's key"),
            commit_at.clone(),
            [xa_key.to_string()].into_iter().collect(),
            &mock,
        );
        quit(c).await;

        // What the restarted child said it resumed from, for the summary: a killed child wrote its
        // rows on its own cadence, so the relation between that row and the start is reported.
        let mut resumed_report = String::from("no restart in this round");
        if kill {
            // Property 2b (P6): the restarted child resumes at or below the XA's start — the
            // floor survived the SIGKILL, which is what makes the key findable again at all.
            lazy = Lazy::start(
                "s5_source_cut_around_xa",
                &db,
                &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
                Some(log.clone()),
            );
            sp.add_log(&lazy.log_path);
            // Not `ready()` here: `/readyz` answers 200 only while the instance is active AND
            // streaming the binlog (src/health.rs:87), and the dump role is still cut — a child
            // that cannot open a dump connection is honestly not ready. What proves it is up is
            // its own start line, which is also exactly the evidence P6 needs; readiness is
            // required after the heal, when streaming is what it should be doing.
            let line = wait_for(
                Duration::from_secs(60),
                &format!(
                    "round {round}: the restarted child to report the positions it resumes from"
                ),
                || async {
                    lazy.log_lines_matching("starting from the saved positions")
                        .last()
                        .cloned()
                },
            )
            .await;
            let v: serde_json::Value = serde_json::from_str(&line).expect("json log line");
            let resumed = parse_pos(v["fields"]["position"].as_str().unwrap_or_default())
                .unwrap_or_else(|| panic!("unparsable start position in {line}"));
            // The property is the FLOOR, not the row: a child that comes back at or below the
            // branch's own start reads its PREPARE/COMMIT again, which is the only reason the key
            // can be found at all. The instance row is compared against nothing here — the killed
            // child wrote it on its own 200 ms cadence and may have moved it (and `resume_pos`
            // includes the XA pin, so the row a restart uses is legitimately the XA's start), so it
            // is reported, in the summary, as the position the child was last seen holding.
            assert!(
                (rf.clone(), rp) <= xa_start && resumed <= xa_start.clone(),
                "the restarted child resumed at {resumed:?}, past the XA's start {xa_start:?}: it \
                 would skip the branch's PREPARE/COMMIT and lose key {xa_key} (the instance row was \
                 {rf}:{rp} and the rule row {pf}:{pp} when they were last read)"
            );
            resumed_report = format!(
                "resumed at {resumed:?}, from the instance row {rf}:{rp} read before the kill"
            );
            log.add(format!(
                "round {round}: the restarted child resumed at {resumed:?}, at or below the \
                 XA's start"
            ));
        }

        // Heal the SOURCE only and keep the target refused: the engine can learn the COMMIT no
        // earlier than the heal, so the floor is asserted with BOTH keys in the buffer — the
        // trap e2e documents, where a row read before the XA's marks are applied holds only the
        // later key, whose origin is legitimately above the XA's start.
        let conns0 = binp.conns();
        log.add(format!(
            "round {round}: healing the dump role, so the XA's COMMIT is read across the cut"
        ));
        binp.heal();
        wait_for(
            Duration::from_secs(60),
            &format!("round {round}: the engine's reconnect after the heal"),
            || async { (binp.conns() > conns0).then_some(()) },
        )
        .await;
        // Healed, so streaming is what the child should be doing again: `/status` is what `drain`
        // polls, and a child that stays non-ready from here on (ready = active AND streaming,
        // src/health.rs:87) is a real failure rather than a fault the seed asked for.
        ready_or_skip!(lazy, 60);
        let refused1 = log.count("=500 fired");
        wait_for(
            Duration::from_secs(60),
            &format!("round {round}: the XA's key and the plain keys refused together at the mock"),
            || async {
                let keys = body_keys(&mock);
                (log.count("=500 fired") > refused1
                    && keys.contains(&xa_key.to_string())
                    && keys.contains(&plain_from.to_string())
                    && keys.contains(&plain_to.to_string()))
                .then_some(())
            },
        )
        .await;

        // Property 3 (P2): the floor at the XA's start holds while its keys are unflushed, read
        // from a row the engine wrote *after* that refusal (a stale row proves nothing).
        let since = server_now(&db).await;
        let (f, p, _, err) = wait_for(
            Duration::from_secs(45),
            &format!("round {round}: the refused flush in a status row written after it"),
            || async {
                fresh_row(&db, "o", &since)
                    .await
                    .filter(|(_, _, _, e)| e.as_deref().is_some_and(|e| e.contains("500")))
            },
        )
        .await;
        assert!(
            (f.clone(), p) <= xa_start,
            "the rule position climbed past the XA start while its keys are unflushed: saved \
             {f}:{p}, xa start {}:{} (last_error {err:?})",
            xa_start.0,
            xa_start.1
        );
        sp.check();

        // Heal the target: both keys must arrive — the XA's only through the COMMIT the engine
        // had to read across the cut (and, in round 2, across the kill and a restart).
        log.add(format!("round {round}: healing the target"));
        mock.heal();
        drain(&sp, &lazy, &db, "o", commit_at.clone(), 75).await;
        sp.check();
        assert_all_keys(&mock, plain_from, plain_to, &format!("S5 {round} plain"));
        assert!(
            mock.keys_received().contains(&xa_key.to_string()),
            "S5 {round}: key {xa_key} of the committed XA '{xid}' never reached the target \
             (floor {}:{}, the refused row was at {f}:{p}, kill={kill})",
            xa_start.0,
            xa_start.1
        );
        eprintln!(
            "S5 {round}: xid {xid} committed across a cut (kill={kill}); the XA's group starts at \
             {}:{}, the instance row held the floor at {rf}:{rp} and the refused rule row sat at \
             {f}:{p}; the restarted child {resumed_report}; keys {xa_key} + \
             {plain_from}..={plain_to} all at the mock, {} deliveries so far, {} refusals; P6 \
             regressions the sampler accepted as pins: {:?}",
            xa_start.0,
            xa_start.1,
            mock.delivery_count(),
            log.count("=500 fired"),
            sp.backslides(),
        );
    }
    // P5 across every child this seed ran, the killed one included: the outage was transient and
    // no key took a strike for it.
    assert_no_undelivered(&lazy);
    for path in &dead_logs {
        assert_no_undelivered_at(path);
    }
    lazy.kill();
    sp.stop();
}

// ---------------------------------------------------------------------- h1 harness self-test

/// Writes a position into a rule's own status row. Only a seed that tests the *harness* does this:
/// the engine has to be stopped first (h1 terminates its child), or it would simply write the row
/// back. Nothing else in the suite touches the status table by hand — every other seed's claims are
/// read from rows the engine wrote itself.
async fn forge_row(db: &Db, rule: &str, pos: &(String, u64)) {
    db.exec_params(
        "UPDATE lazy_wire_fire_status SET binlog_file = ?, binlog_pos = ? WHERE rule = ?",
        (pos.0.clone(), pos.1, rule),
    )
    .await;
}

/// The sampler is a test of the *tests*: an invariant nobody has seen fail is not an invariant. Its
/// one soft spot is P6's XA exemption — a rule that says "the position may fall back to this
/// floor" can quietly become "the position may go anywhere". `h1_sampler_pin_floor_still_bites`
/// breaks it on purpose, against a real floor:
///
/// 1. A branch is prepared (and never committed, so its key is never delivered and the exemption
///    stays open — a pin whose keys arrived excuses nothing). Its floor comes from [`XaPin::read`],
///    i.e. from the server's own binlog, not from anything this seed chooses.
/// 2. The child starts *after* that group, so the engine never reads the branch and its row is free
///    to climb: ten write-once orders drain, and the row is verified to sit strictly above the
///    floor — a forge that moves nothing would prove nothing.
/// 3. The child is terminated (a row nobody can rewrite), then forged to *exactly* the floor. The
///    sampler must accept that as the designed pin, so waiting is waiting for a line in
///    `Sampler::backslides` — an observed event — and the absence of a violation is asserted too,
///    because a floor that accepts everything passes this test for the wrong reason.
/// 4. The row is then forged *below* the floor and the sampler must call it a violation naming the
///    floor. That is the mutation this seed is: if the comparison in `sample` is relaxed to `>= 0`,
///    or the pin stops expiring, this seed fails.
///
/// `check()` is deliberately never called after step 4: the violation is the expected outcome, so
/// the seed reads it through `Sampler::violation` and quotes it in its summary instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h1_sampler_pin_floor_still_bites() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_h1").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("h1-hook");
    db.target("hook", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "h", "hook", "orders").await;

    // The uncommitted branch, prepared before the child exists: the engine starts from the current
    // position, so it never reads this branch and nothing holds its row at the floor. The branch is
    // guarded before `XA START` (design §4) and rolled back on every path out of here, panic included.
    let xa_key = 121u32;
    let xid = format!("lwf_chaos_h1_{}", rand_suffix());
    let guard = XaGuard::new(&root, &xid, Some(log.clone()));
    let before = binlog_end(&db).await;
    let mut c = db.conn().await;
    for q in [
        format!("XA START '{xid}'"),
        format!("INSERT INTO orders VALUES ({xa_key}, 1, 'x', 1)"),
        format!("XA END '{xid}'"),
        format!("XA PREPARE '{xid}'"),
    ] {
        c.query_drop(&q)
            .await
            .unwrap_or_else(|e| panic!("{q} ({xid}): {e}"));
    }
    quit(c).await;
    // A transaction of its own between the branch and the phase, so the floor is not merely the
    // boundary the phase's group begins at (the same reason e2e and S5 put a filler there).
    db.exec(&format!(
        "INSERT INTO order_tags VALUES ({}, 'filler')",
        xa_key + 1_000
    ))
    .await;
    let pin = XaPin::read(&db, &before, &xid).await;
    assert!(
        *pin.pos() >= before,
        "the floor {}:{} of '{xid}' must be at or after {before:?} (the snapshot taken before \
         XA START): a floor read from somewhere earlier in the log is not this branch's start",
        pin.pos().0,
        pin.pos().1
    );

    // No proxies: this seed injects no fault at all, it forges a row. That distinction is the point
    // — the fault class is proven by S*, and what is proven here is the checker that watches them.
    let mut lazy = Lazy::start(
        "h1_sampler_pin_floor_still_bites",
        &db,
        &[],
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_log(&lazy.log_path);

    bulk_orders(&db, 101, 110).await;
    let cp = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("h", "the 10 orders", cp.clone(), keyset(101, 110), &mock);
    drain(&sp, &lazy, &db, "h", cp.clone(), 60).await;
    assert_all_keys(&mock, 101, 110, "H1");
    sp.check();

    // The exemption must still be open when the forges come: the branch was never committed, so its
    // key must never have reached the target. If it had, the pin would be spent and the accepted
    // case below would fail for the wrong reason.
    assert!(
        !mock.keys_received().contains(&xa_key.to_string()),
        "key {xa_key} of the never-committed branch '{xid}' reached the target: the pin would be \
         spent, and an accepted regression would prove nothing"
    );

    // Stop the child first: a row the engine still owns cannot stay forged. If the environment
    // took the child instead (an OOM kill on a loaded box), `term` says so loudly — the signal,
    // the host load, the OOM counters — and this self-test returns without a verdict: that is not
    // a product failure, and `fault::environment_aborts()` plus `tmp/out/chaos/env-aborts.log`
    // keep it from being a silent pass either.
    if lazy.term().await.is_environment_kill() {
        eprintln!("H1: SKIPPED BY ENVIRONMENT — the harness's child was taken; see tmp/out/chaos/env-aborts.log");
        return;
    }
    let (saved_file, saved_pos, _, _) = db
        .status("h")
        .await
        .expect("the rule row a cleanly terminated child wrote");
    let saved = (saved_file, saved_pos);
    assert!(
        saved > *pin.pos(),
        "the row must sit above the floor for the forge to mean anything: saved {saved:?}, floor \
         {:?} of '{xid}' (the phase drained, so the engine read past the branch's start)",
        pin.pos()
    );

    // The declaration under test, and the only kind the sampler accepts: an `XaPin` the server's own
    // binlog confirms, through the one constructor there is.
    sp.allow_xa_pin(
        "h",
        "h1: the branch that never commits",
        &pin,
        [xa_key.to_string()].into_iter().collect(),
        &mock,
    );

    // The sampler must have seen the real row before it is asked to react to a forged one; without
    // this the first sample would simply be the forged value and no regression would ever occur.
    wait_for(
        Duration::from_secs(30),
        "the sampler to have completed a first sample",
        || async { (sp.samples() > 0).then_some(()) },
    )
    .await;

    // (a) Down TO the floor: the designed XA pin, so the event to wait for is a line in `backslides`
    // and a violation here would be the sampler over-ruling a real floor.
    let accepted0 = sp.backslides().len();
    log.add(format!(
        "h1: forging the rule row down to the floor {:?}",
        pin.pos()
    ));
    forge_row(&db, "h", pin.pos()).await;
    wait_for(
        Duration::from_secs(30),
        "the sampler to accept a position at the pin's floor",
        || async { (sp.backslides().len() > accepted0).then_some(()) },
    )
    .await;
    let accepted = sp
        .backslides()
        .last()
        .expect("the accepted regression the seed just waited for")
        .clone();
    assert!(
        sp.violation().is_none(),
        "the sampler called a position AT the pin's floor a violation: {v} (a floor that rejects \
         its own value is as useless as one that accepts anything below it)",
        v = sp.violation().expect("just checked")
    );
    assert!(
        accepted.contains("P6 accepted")
            && accepted.contains(&format!("{}:{}", pin.pos().0, pin.pos().1)),
        "the accepted regression must name the floor it fell to, got {accepted:?}"
    );

    // (b) BELOW the floor: the floor has to bite. Halving the offset keeps the forged value in the
    // floor's own file, so the comparison is about the position and nothing else.
    let below = (pin.pos().0.clone(), pin.pos().1 / 2);
    assert!(
        below < *pin.pos() && below.1 > 0,
        "the forged position {below:?} must be strictly below the floor {:?} (and above the log \
         header) to test the floor rather than the file name",
        pin.pos()
    );
    log.add(format!(
        "h1: forging the rule row below the floor to {below:?}"
    ));
    forge_row(&db, "h", &below).await;
    let v = wait_for(
        Duration::from_secs(30),
        "the sampler to call a position below the pin's floor a violation",
        || async { sp.violation() },
    )
    .await;
    assert!(
        v.contains("BELOW the floor")
            && v.contains(&format!("floor {}:{}", pin.pos().0, pin.pos().1)),
        "the violation must be the floor's own teeth — a position below the declared floor, named \
         with that floor — got {v}"
    );

    eprintln!(
        "H1: the sampler's XA floor bites both ways — rule h drained to {saved:?} with branch \
         '{xid}' unconfirmed, forged to the floor {:?} and the sampler accepted it ({} accepted \
         regression(s): {accepted}), then forged below it to {below:?} and the sampler refused: {}",
        pin.pos(),
        sp.backslides().len(),
        v.lines().next().unwrap_or_default(),
    );
    // P5: the child was terminated cleanly, so an undelivered line in its log would be a real
    // finding (and would have made `v` the P5 message, which the assert above names).
    assert_no_undelivered(&lazy);
    // `sp.check()` is NOT called: the violation just read is the expected outcome of the seed.
    drop(guard);
    sp.stop();
}

/// The other checker the whole file leans on, proven the same way as the floor: P2, "while a
/// phase's keys are not all at the target, its rule's position stays below that phase's
/// checkpoint". Every seed that *has* an open phase gets it for free from an honest engine, which
/// never crosses that line, so the branch in `sample` that reports it is a code path no green run
/// ever takes — mutation M3 (the check disabled) left the whole suite green, which is how I found
/// that nothing owned it. `h2_sampler_checkpoint_check_still_bites` takes it in both directions:
///
/// 1. Ten orders are written with the target holding every request it takes (accepted, never
///    answered), so the flush is frozen, the keys are never delivered and the phase stays open —
///    the exact state P2 speaks about. The engine's own row is verified to sit *below* the
///    checkpoint and the sampler, which has sampled several times by then, has said nothing. That
///    is the control half: without it, a sampler that refuses everything would pass step 2. A
///    small delivered phase comes first (see the note where it is written) so that the row this
///    step reads exists because the seed caused it, not because the engine happened to persist
///    something during its start.
/// 2. The child is terminated (so no one writes the row back) and the row is forged to *exactly*
///    the checkpoint — the boundary itself, not a byte past it, so a `>=` relaxed to `>` is caught
///    here too. The sampler must refuse, naming P2, the rule and the phase whose keys are missing.
///
/// Like h1, `check()` is never called after the forgery: the violation is the expected outcome, so
/// it is read through [`Sampler::violation`] and quoted in the summary. P5 does not apply to this
/// seed — its flush was never answered, which is a fault the seed itself injected, and the child
/// log is checked before that point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_sampler_checkpoint_check_still_bites() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_h2").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("h2-hook");
    db.target("hook", "http", &mock.url(), None, "{}").await;
    join_rule(&db, "h", "hook", "orders").await;
    // No proxies: like h1, this seed injects no fault at the wire. It injects a row.
    let mut lazy = Lazy::start(
        "h2_sampler_checkpoint_check_still_bites",
        &db,
        &[],
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_log(&lazy.log_path);

    // A delivered phase FIRST, for the same reason S2 has one: the row this seed reads in the
    // control (and forges in step 2) exists only once the engine has confirmed something, and a
    // seed may not read a row its own steps did not cause. The first version armed the hold as the
    // very first thing it did and then expected the row; on a run where the engine had persisted
    // nothing yet, `status("h")` answered None and the seed died on its own control, not on P2
    // (observed on the box, mariadb1011, 2026-11-20: 8 passed / 1 failed, h2 at "rule row while the
    // flush is held"). Draining a warm-up phase makes the row's existence a consequence, not luck.
    bulk_orders(&db, 1, 10).await;
    let cp0 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "h",
        "the 10 delivered orders",
        cp0.clone(),
        keyset(1, 10),
        &mock,
    );
    drain(&sp, &lazy, &db, "h", cp0.clone(), 60).await;

    // The hold now, then the write: nothing this phase produces can be answered, so its keys stay
    // undelivered and its phase stays open — the state P2 exists for.
    mock.hold_after(0);
    bulk_orders(&db, 201, 210).await;
    let cp = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "h",
        "the 10 held orders",
        cp.clone(),
        keyset(201, 210),
        &mock,
    );
    wait_for(
        Duration::from_secs(60),
        "the held flush to reach the target and be frozen",
        || async { log.has("hold_after fired").then_some(()) },
    )
    .await;
    wait_for(
        Duration::from_secs(30),
        "the sampler to have sampled the open phase a few times",
        || async { (sp.samples() >= 3).then_some(()) },
    )
    .await;

    // The control: an honest engine keeps the row below the checkpoint of a phase whose keys have
    // not arrived, and the sampler must have looked without saying anything. Read with a deadline
    // rather than `expect`: the warm-up above makes the row's existence a consequence, and a seed
    // should still say which condition it is waiting on if one ever is slow.
    let (before_f, before_p, _, _) = wait_for(
        Duration::from_secs(30),
        "the rule row the drained warm-up left behind",
        || async { db.status("h").await },
    )
    .await;
    assert!(
        (before_f.clone(), before_p) < cp,
        "the engine confirmed {}:{} at or past the checkpoint {}:{} of a phase whose keys were \
         never delivered: P2 would be reporting a real violation in a seed that expects none",
        before_f,
        before_p,
        cp.0,
        cp.1
    );
    assert!(
        sp.violation().is_none(),
        "the sampler called the honest state a violation before anything was forged: {}",
        sp.violation().expect("just checked")
    );
    assert!(
        sp.open_phases()
            .iter()
            .any(|(r, _, _, missing)| r == "h" && *missing == 10),
        "the phase must be open with all 10 keys missing for the forgery to test P2: {:?}",
        sp.open_phases()
    );

    // Forge to exactly the checkpoint, with the child gone so nobody writes the row back. The
    // same rule as in h1: an environment kill is announced and the self-test returns, it is
    // never reported as a product failure — and never dropped quietly either.
    if lazy.term().await.is_environment_kill() {
        eprintln!("H2: SKIPPED BY ENVIRONMENT — the harness's child was taken; see tmp/out/chaos/env-aborts.log");
        return;
    }
    log.add(format!(
        "h2: forging the rule row up to the checkpoint {cp:?} with its 10 keys still missing"
    ));
    forge_row(&db, "h", &cp).await;
    let v = wait_for(
        Duration::from_secs(30),
        "the sampler to call the checkpoint of an undelivered phase a violation",
        || async { sp.violation() },
    )
    .await;
    assert!(
        v.contains("P2:") && v.contains("rule h") && v.contains("is open"),
        "the refusal must be P2 naming the rule and the open phase whose keys are missing, got {v}"
    );

    eprintln!(
        "H2: {} samples with the honest row at {before_f}:{before_p} and the checkpoint at {}:{} \
         (10 keys held at the mock, phase open), then forged to the checkpoint and the sampler \
         refused: {}",
        sp.samples(),
        cp.0,
        cp.1,
        v.lines().next().unwrap_or_default(),
    );
    // P5 while the engine still owed nothing: the child was terminated after the hold, so a real
    // undelivered line at this point would be a finding. (`sp.check()` is NOT called: the violation
    // above is the outcome this seed asks for.)
    assert_no_undelivered(&lazy);
    mock.heal();
    sp.stop();
}

/// The status table's instance row (`rule = '*'`): where a restart resumes from, because
/// `resume_pos` takes the minimum over the rules *and* the open XA transactions (src/engine.rs).
/// The rule row does not carry an unapplied XA's pin, so a seed that wants to know what a killed
/// child would restart at has to ask this row.
async fn instance_row(db: &Db) -> Option<(String, u64)> {
    let mut c = db.conn().await;
    let r: Option<mysql_async::Row> = c
        .exec_first(
            "SELECT binlog_file, binlog_pos FROM lazy_wire_fire_status WHERE rule = '*'",
            (),
        )
        .await
        .unwrap();
    quit(c).await;
    r.map(|row| {
        (
            row.get::<String, _>(0).expect("binlog_file"),
            row.get::<u64, _>(1).expect("binlog_pos"),
        )
    })
}

/// A `file:pos` as the log lines write it.
fn parse_pos(s: &str) -> Option<(String, u64)> {
    let (f, p) = s.rsplit_once(':')?;
    Some((f.to_string(), p.parse().ok()?))
}

// -------------------------------------------------------------------- h3 harness self-test

/// Signals a pid from *outside* this process, through `/bin/sh`'s builtin so a box without procps
/// can still run it (the h1/h2 incident was an `ENOENT` on the `kill` *program*, so the seed that
/// tests what a foreign signal looks like must not depend on that program either). The harness
/// never learns about this signal, which is exactly the point: an OOM kill and this are the same
/// event to the harness, and what it makes of them is what `h3` is about.
fn signal_from_outside(pid: u32, sig: &str) {
    let cmd = format!("kill -{sig} {pid}");
    match std::process::Command::new("/bin/sh")
        .args(["-c", &cmd])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(st) if st.success() => {}
        Ok(st) => panic!(
            "h3: `{cmd}` exited {st:?}: the seed cannot inject the fault it is about to assert \
             about, so it has no verdict to report"
        ),
        Err(e) => panic!("h3: nothing here could signal a pid from outside the harness: {e}"),
    }
}

/// h3, the harness self-test the h1/h2 finding asked for (`exited -1 on SIGTERM` was the suite
/// reporting the *box* as a product regression). A chaos suite that reports an OOM kill as a
/// regression sends someone to bisect a commit that never moved, and a harness that avoids that by
/// calling every signal death an environment abort stops reporting crashes — so both halves are
/// asserted, against real children of the real binary where a real child is what carries the
/// meaning and against wait statuses built by hand where a real signal could not be delivered
/// reliably from a test process:
///
/// * a SIGKILL from outside the harness — the shape of an OOM kill, and of anything else this
///   process did not do — ends as [`Stop::EnvironmentKilled`], is named by [`environment_aborts`],
///   and is written to `tmp/out/chaos/env-aborts.log`, because a dropped self-test must not be
///   able to pass for the wrong reason either;
/// * the same SIGKILL *sent by the harness* (`kill_no_wait`, the kill half of `hard_stop`) ends as
///   [`Stop::HarnessSignalled`] and adds nothing to that list: the harness keeping its own teeth is
///   what makes the first bullet a classifier and not a mute;
/// * a wait status of signal 11, of exit code 9, and of exit 0 are judged panic, panic and
///   [`Stop::Exited(0)`] — the crash and the nonzero exit still fail the seed, the clean shutdown
///   still passes it, and so neither of the two bullets above is bought by a classifier that
///   simply fails (or excuses) everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h3_environment_kill_is_not_a_product_failure() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_h3").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    // Counted by seed name rather than by length: the suite runs two seeds at a time, and
    // another seed's environment kill is not this one's evidence.
    let aborts_for = |name: &str| {
        environment_aborts()
            .iter()
            .filter(|l| l.contains(name))
            .count()
    };

    // (a) A death this process did not cause: the environment's, and not a product failure.
    let mut foreign = Lazy::start("h3_environment_kill", &db, &[], Some(log.clone()));
    ready_or_skip!(foreign, 60);
    let pid = foreign.pid();
    log.add(format!(
        "h3: SIGKILLing {pid} from outside the harness (the shape of an OOM kill)"
    ));
    signal_from_outside(pid, "KILL");
    let stop = foreign.wait_stop("watched by the seed").await;
    assert!(
        matches!(stop, Stop::EnvironmentKilled { signal: 9 }),
        "h3: a SIGKILL the harness never sent was judged {stop:?}: this is the shape the finding \
         reported as `exited -1 on SIGTERM`, and a seed that fails on it sends someone to bisect \
         a commit that never moved"
    );
    assert_eq!(
        aborts_for("h3_environment_kill"),
        1,
        "h3: an environment kill was not recorded exactly once for this seed ({} were): a \
         self-test the environment took has to be visible in the run report, not only on the \
         console",
        aborts_for("h3_environment_kill")
    );
    let recorded = std::fs::read_to_string("tmp/out/chaos/env-aborts.log").unwrap_or_default();
    assert!(
        recorded.contains("h3_environment_kill") && recorded.contains("cgroup OOM counters"),
        "h3: tmp/out/chaos/env-aborts.log does not carry the abort with the numbers that explain \
         it (the signal, the host load, the OOM counters), so a reader after the run could not \
         tell an environment kill from a regression"
    );

    // (b) The same signal from the harness itself is the harness's own doing — never an abort.
    let mut ours = Lazy::start("h3_harness_kill", &db, &[], Some(log.clone()));
    ready_or_skip!(ours, 60);
    ours.kill_no_wait();
    let stop = ours.wait_stop("the harness's own SIGKILL").await;
    assert!(
        matches!(stop, Stop::HarnessSignalled { signal: 9 }),
        "h3: the harness's own SIGKILL was judged {stop:?}: a classifier that calls this an \
         environment abort too has stopped distinguishing the box from the product, and every \
         seed that kills its child would be skipped instead of checked"
    );
    assert_eq!(
        aborts_for("h3_harness_kill"),
        0,
        "h3: the harness's own kill was recorded as an environment abort: the two halves of the \
         classifier have collapsed into one"
    );

    // (c) A crash is the product's own doing, whatever else the box is doing. The last three
    // verdicts come from wait statuses the seed built itself (`ExitStatusExt::from_raw`) rather
    // than from a real signal: the first attempt at this seed injected a genuine `kill -SEGV` at a
    // genuine child and watched it keep streaming for the full 15 s of the watch
    // (tmp/out/lwf-worker-chaos-pr3/h3-run2.log), so a fault that does not land leaves the seed
    // with no verdict to make its point with — whatever the reason it did not land, which this
    // seed does not claim to know. The finding is about the classifier, and the classifier is a
    // function of the wait status, so the wait status is what is asserted, on (b)'s child and
    // beside the two deaths that really happened.
    use std::os::unix::process::ExitStatusExt;
    let said = |st: std::process::ExitStatus| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ours.judge_stop(
                st,
                Ok("synthetic: the seed built this wait status".to_string()),
                "a crash signal",
            )
        }))
    };
    let segv = said(std::process::ExitStatus::from_raw(11));
    let err = match segv {
        Err(e) => e,
        Ok(stop) => panic!(
            "h3: a child that died of SIGSEGV was judged {stop:?} instead of failing the seed: a \
             SIGSEGV is not something an OOM killer does, so it is the product's own doing even on \
             a box that is on fire"
        ),
    };
    assert!(
        err.downcast_ref::<String>()
            .is_some_and(|m| m.contains("SIGSEGV")),
        "h3: the crash death failed the seed for the wrong reason ({err:?}): the report has to \
         name the signal, because \"the child died\" alone is how this whole class was misread \
         before"
    );
    let nonzero = said(std::process::ExitStatus::from_raw(9 << 8));
    assert!(
        nonzero.is_err(),
        "h3: a child that exited 9 was judged without failing the seed: a nonzero exit is the \
         product's own verdict and no environment story excuses it"
    );
    // And the positive control: exit 0 after the stop is what a clean shutdown looks like. Without
    // this the two asserts above could be satisfied by a classifier that fails everything.
    let clean = ours.judge_stop(
        std::process::ExitStatus::from_raw(0),
        Ok("synthetic: the seed built this wait status".to_string()),
        "SIGTERM",
    );
    assert!(
        matches!(clean, Stop::Exited(0)),
        "h3: exit 0 on SIGTERM was judged {clean:?}: a classifier that cannot call the clean \
         stop clean is not classifying anything"
    );
    eprintln!(
        "h3: foreign SIGKILL -> EnvironmentKilled, harness SIGKILL -> HarnessSignalled, SIGSEGV \
         -> panic naming the signal ({} abort recorded for this seed)",
        aborts_for("h3_environment_kill")
    );
}

// --------------------------------------------------------------------------- P2 reset

/// The engine's own reset-progress line, as it is persisted in the status row's
/// `reset_state` column while a `for_each` step walks its batches (`src/engine.rs`):
/// `running: step <i>/<n> fp<digest> <name>: <done>/<total> keys: <reason>`. Returns
/// `(step, steps, name, done, total)`.
fn reset_progress(state: &str) -> Option<(u32, u32, String, u64, u64)> {
    let rest = state.strip_prefix("running: step ")?;
    let (step, tail) = rest.split_once('/')?;
    let (steps, tail) = tail.split_once(' ')?;
    // Between the counter and the step name sits the fingerprint digest token
    // (`fp<8 hex>`, see `resume_requested_reset`); step names in this suite never
    // look like it, so a line that carries none is a parse failure, not a silent
    // mis-parse of the name.
    let (_, tail) = tail.split_once(' ')?;
    let (name, tail) = tail.split_once(": ")?;
    let (done, rest) = tail.split_once('/')?;
    let (total, _) = rest.split_once(" keys:")?;
    Some((
        step.parse().ok()?,
        steps.parse().ok()?,
        name.to_owned(),
        done.parse().ok()?,
        total.parse().ok()?,
    ))
}

/// How many times the application was asked at `path` during the run so far. The
/// design §3 gate for a mid-step kill is "the step is not just claimed but observed
/// to be producing calls", and this is the counter taken where those calls land.
fn mock_path_hits(mock: &Mock, path: &str) -> usize {
    mock.requests().iter().filter(|r| r.path == path).count()
}

/// The rule row's `reset_requested` flag — the operator's lever, read back from the
/// table rather than trusted from the write.
async fn reset_flag(db: &Db, rule: &str) -> Option<i64> {
    let mut c = db.conn().await;
    let r: Option<i64> = c
        .exec_first(
            "SELECT reset_requested FROM lazy_wire_fire_rule WHERE name = ?",
            (rule,),
        )
        .await
        .unwrap();
    quit(c).await;
    r
}

/// P2 (design §2): an operator asks for a full re-send (`reset_requested = 1`) while the
/// engine is running, and the kernel does not cooperate: a SIGKILL lands while the
/// snapshot step is demonstrably mid-batch — the status row carries the reset in flight
/// and the step's own `/snap` calls are still arriving and advancing at the application.
/// (The engine marks the row dirty at `pending:` and updates the `running:` progress in
/// memory only — `drive_reset`, `src/engine.rs` — so the step's call counter, not the
/// column, is the mid-step observation; the row is what ties the calls to this flip.)
/// A reset is not a transaction.
///
/// Properties, each with the assertion that says it:
/// * the fault fires inside a live reset: the gate is the in-flight row plus the step's
///   own call counter at the mock, the child answers strictly fewer than all batches
///   before the kill, and the flag is read back as `1` immediately before it (nothing is
///   asserted about a reset nobody started);
/// * the restarted child starts the interrupted reset over — the step re-runs from the
///   beginning (`/snap` calls for the whole table again, `reset steps start` in its log)
///   and completes with `done:` in the status row;
/// * the engine clears the operator's flag itself after finishing (`reset_requested = 0`);
/// * the saved position survives the crash: the second child boots on
///   `starting from the saved positions`;
/// * the work written while the reset was in flight — 200 `orders` keys on a second,
///   disjoint rule — reaches the application exactly once after the restart, and the
///   re-sent snapshot adds no unexplained duplicate (P4 is the sampler's, and the kill
///   and the restart are its fault events).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p2_sigkill_during_reset() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_p2").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("p2-reset");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("hook", "http", &mock.url(), None, "{}").await;

    // The reset rule: an announcement step, then a snapshot of all 5 000 `items` keys in
    // batches of 250. The step's own template posts to `/snap`, so the step's calls are
    // countable apart from the rule's ordinary flushes (which go to `/hook`). At the
    // mock's armed delay every batch costs a third of a second, which makes the step
    // last seconds — a window the kill can be *seen* to land inside, not guessed at.
    let total = 5_000;
    let steps = serde_json::json!([
        {
            "name": "announce",
            "once": true,
            "template": "POST /reset-announce HTTP/1.1\n\n"
        },
        {
            "name": "snapshot",
            "for_each": "snapshot",
            "template": "POST /snap HTTP/1.1\n\n{{join keys \",\"}}"
        }
    ]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, snapshot_keys_sql, \
         batch_max_keys, reset_steps, debounce_ms) VALUES (?, ?, ?, ?, ?, ?, 100)",
        (
            "snap",
            "hook",
            "POST /hook HTTP/1.1\n\n{{join keys \",\"}}",
            "SELECT id AS _key FROM items",
            250u32,
            steps.to_string(),
        ),
    )
    .await;
    db.trigger("snap", "items", Some("{{new.id}}"), None).await;
    // The live rule owns the keys written during the reset window: its own table and
    // key range keep the `items` accounting clean and give the window its own traffic.
    join_rule(&db, "live", "hook", "orders").await;

    bulk_items(&db, 1, total).await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "p2_sigkill_during_reset",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // The precondition the design states: the engine must be genuinely live before the
    // fault — every seeded row delivered once, and no reset already in progress (an
    // old `running:` row would turn this seed into a resume test nobody asked for).
    let cp0 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase("snap", "seeded rows", cp0.clone(), keyset(1, total), &mock);
    drain(&sp, &lazy, &db, "snap", cp0.clone(), 180).await;
    let before = db
        .status("snap")
        .await
        .expect("snap row after the first drain");
    assert!(
        !before.2.starts_with("running") && !before.2.starts_with("pending"),
        "a reset was already in progress before the fault: reset_state = {:?}",
        before.2
    );

    // The operator's lever, pulled the normal way: an UPDATE of the rule row. The delay
    // is armed first so the snapshot step is slow enough to observe and interrupt.
    mock.delay(300, 3000);
    log.add("operator requests a reset: reset_requested = 1");
    db.exec("UPDATE lazy_wire_fire_rule SET reset_requested = 1 WHERE name = 'snap'")
        .await;
    let hits0 = mock_path_hits(&mock, "/snap");
    let (state_at_gate, hits_at_gate) = wait_for(
        Duration::from_secs(90),
        "the reset to be observed in flight (the row says pending/running) with the \
         snapshot step's own `/snap` calls at the mock advancing",
        || async {
            let r = db.status("snap").await.unwrap_or_default();
            let hits = mock_path_hits(&mock, "/snap");
            // Which line the row carries is the persist cadence's choice: `start_reset`
            // marks the row dirty at `pending:` and `drive_reset` rewrites the column
            // in memory without ever setting the dirty flag again (`src/engine.rs`), so
            // a poll sees `pending:` or a `running:` frozen wherever the step had got
            // to. The strongest mid-step observation is therefore the step's own calls
            // landing at the application, which is what `hits > hits0` counts; the row
            // standing on a live reset is what ties those calls to this flip.
            let s = &r.2;
            let inside = s.starts_with("pending:") || s.starts_with("running:");
            (inside && hits > hits0).then(|| (s.clone(), hits))
        },
    )
    .await;
    let progress = reset_progress(&state_at_gate);
    if let Some((_, _, _, _done, total_keys)) = &progress {
        assert_eq!(
            *total_keys, total as u64,
            "the snapshot step must walk every row of the table, not a part of it"
        );
    }
    assert_eq!(
        reset_flag(&db, "snap").await,
        Some(1),
        "the kill must happen while the operator's flag is still set — the engine \
         clears it only when the reset finishes"
    );
    let snap_hits_before = mock_path_hits(&mock, "/snap");
    assert!(
        snap_hits_before > hits0,
        "the snapshot step had produced no new call at the mock when the kill was about \
         to land ({hits0} calls before the flag, {snap_hits_before} after): the step was \
         never observed running, and a kill outside it would test nothing"
    );
    assert!(
        snap_hits_before < hits0 + total as usize / 250,
        "the first child had already answered the whole snapshot ({snap_hits_before} calls, \
         all {} batches) before the kill: the step was not in flight, and a kill after it \
         would test nothing",
        total as usize / 250
    );

    // Work written between the reset's start and the crash — the keys whose delivery
    // after the restart is the point of the seed.
    log.add(format!(
        "killing child 1 mid-snapshot (row `{state_at_gate}`, {snap_hits_before} step calls \
         answered at the mock), with orders 10001..=10100 written during the reset"
    ));
    bulk_orders(&db, 10_001, 10_100).await;
    let cp_live1 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "live",
        "keys written during the reset",
        cp_live1.clone(),
        keyset(10_001, 10_100),
        &mock,
    );
    lazy.kill();

    // The restart finds the same unfinished state in the rule row (the flag is still 1)
    // and in the status row (`running: step 2/2 ...`), and must run the reset to its
    // end — from the beginning — before anything is believed delivered.
    let mut lazy2 = Lazy::start(
        "p2_sigkill_during_reset",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy2, 60);
    sp.add_log(&lazy2.log_path);
    let starts: Vec<String> = lazy2.log_lines_matching("reset steps start");
    assert!(
        !starts.is_empty(),
        "the restarted child started no reset at all: the rule row still said \
         reset_requested = 1 and the status row still said `running:`, so it had to \
         ({:?})",
        db.status("snap").await.map(|r| r.2)
    );
    let saved = lazy2.log_lines_matching("starting from the saved positions");
    assert!(
        !saved.is_empty(),
        "the restarted child did not boot on the saved positions: {:?}",
        lazy2.log_path
    );
    // It does not resume the step where it stopped — the interrupted reset starts over,
    // so the step posts the whole table again: the first child answered at most the
    // chunks it got to (the gate saw `{done}`), and the second owes all 20 batches.
    wait_for(
        Duration::from_secs(120),
        "the crashed reset to finish on the second child (reset_state `done:`)",
        || async {
            let r = db.status("snap").await.unwrap_or_default();
            r.2.starts_with("done:").then_some(())
        },
    )
    .await;
    let snap_hits = mock_path_hits(&mock, "/snap");
    assert!(
        snap_hits >= snap_hits_before + total as usize / 250,
        "the snapshot step produced only {} `/snap` calls across the crash ({snap_hits_before} \
         before the kill): the restarted child must re-walk the whole table \
         ({total} keys in batches of 250), not half of it",
        snap_hits
    );
    let done_lines = lazy2.log_lines_matching("reset steps done");
    assert!(
        !done_lines.is_empty(),
        "the status row says done but the child never logged it: {:?}",
        lazy2.log_path
    );
    // The operator's request is answered, so the engine clears the flag itself.
    wait_for(
        Duration::from_secs(30),
        "the engine to clear the operator's reset_requested flag after finishing",
        || async { (reset_flag(&db, "snap").await == Some(0)).then_some(()) },
    )
    .await;

    // And the window's work arrives: the keys written during the reset (still unconfirmed
    // when the child died) and everything written after, each exactly once — the sampler
    // has policed P4 across the kill (its fault events are in the log), and P1-style
    // byte-exact resume is P1's own claim; here the boot line above is the guard that
    // the second child really started from the saved positions and not from the tail.
    bulk_orders(&db, 10_101, 10_200).await;
    let cp_live2 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "live",
        "keys written after the reset",
        cp_live2.clone(),
        keyset(10_101, 10_200),
        &mock,
    );
    drain(&sp, &lazy2, &db, "live", cp_live2.clone(), 120).await;
    sp.check();
    assert_all_keys(&mock, 10_001, 10_200, "P2 live (mid-reset window)");
    assert_all_keys(&mock, 1, total, "P2 snapshot rows");
    let (delivered, repeated) = delivery_report(&mock);
    let at = progress
        .map(|(_, _, _, d, t)| format!("{d}/{t} keys"))
        .unwrap_or_else(|| format!("{hits_at_gate} step calls answered (row `{state_at_gate}`)"));
    eprintln!(
        "P2: killed mid-snapshot at {at}; /snap calls={snap_hits} \
         ({snap_hits_before} before the kill, the rest is the second child's re-walk), \
         deliveries={delivered} with {repeated} keys repeated across the kill window, \
         200 live keys delivered through the crash"
    );
    assert_no_undelivered(&lazy);
    assert_no_undelivered(&lazy2);
    lazy2.kill();
    sp.stop();
}

// ------------------------------------------------------------------------ P2b reset resume

/// P2b (the resume half of P2): the same operator request and the same SIGKILL, but a rule
/// whose reset is four steps — an announcement and three `for_each` snapshot steps of 10
/// batches each over the 1 000 `items` keys (`batch_max_keys` = 100), each step posting to
/// its own path. The kill is gated on the PERSISTED row standing on `running: step 3/4
/// fp<digest> snapB: …` with that step's own calls advancing: a step advances only on
/// answered chunks
/// (`on_done`), and a persisted line never runs ahead of the walk, so at that gate the
/// announcement and `snapA` were answered to their last batch. The restarted child must
/// continue the run there — no further `/reset-announce` and no further `/snapA` call from
/// it (the two counters that a walk-from-step-1 recovery moves: this is the assertion that
/// reads red on main), the interrupted `snapB` re-walked whole because its key set died with
/// the process (at-least-once), `snapC` run, the row finishing `done:` with the flag cleared
/// by the engine, and every `items` key delivered at least once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p2b_sigkill_during_requested_reset_resumes_the_saved_step() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_p2b").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("p2b-reset");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("hook", "http", &mock.url(), None, "{}").await;

    // Four steps, three of them chunked: the two chunk counts the resumed child must not
    // repeat are the finished step's own (10 `/snapA` calls per walk), and the count it must
    // owe in full is the interrupted step's (10 more `/snapB` calls).
    let total = 1_000;
    let batch = 100u32;
    let chunks = (total / batch) as usize;
    let steps = serde_json::json!([
        {
            "name": "announce",
            "once": true,
            "template": "POST /reset-announce HTTP/1.1\n\n"
        },
        {
            "name": "snapA",
            "for_each": "snapshot",
            "template": "POST /snapA HTTP/1.1\n\n{{join keys \",\"}}"
        },
        {
            "name": "snapB",
            "for_each": "snapshot",
            "template": "POST /snapB HTTP/1.1\n\n{{join keys \",\"}}"
        },
        {
            "name": "snapC",
            "for_each": "snapshot",
            "template": "POST /snapC HTTP/1.1\n\n{{join keys \",\"}}"
        }
    ]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, snapshot_keys_sql, \
         batch_max_keys, reset_steps, debounce_ms) VALUES (?, ?, ?, ?, ?, ?, 100)",
        (
            "snapr",
            "hook",
            "POST /hook HTTP/1.1\n\n{{join keys \",\"}}",
            "SELECT id AS _key FROM items",
            batch,
            steps.to_string(),
        ),
    )
    .await;
    db.trigger("snapr", "items", Some("{{new.id}}"), None).await;

    bulk_items(&db, 1, total).await;
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "p2b_sigkill_during_requested_reset",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);

    // P2's precondition, taken the same way: the engine genuinely live, the first start's
    // reset drained to `done:` with every step answered, and no reset in progress — an old
    // `running:` row would turn the fault below into a resume nobody asked for.
    wait_for(
        Duration::from_secs(120),
        "the first start's reset to finish (row `done:`, every step answered)",
        || async {
            let r = db.status("snapr").await.unwrap_or_default();
            let walked = mock_path_hits(&mock, "/reset-announce") >= 1
                && mock_path_hits(&mock, "/snapA") >= chunks
                && mock_path_hits(&mock, "/snapB") >= chunks
                && mock_path_hits(&mock, "/snapC") >= chunks;
            (r.2.starts_with("done:") && walked).then_some(())
        },
    )
    .await;
    let before = db.status("snapr").await.expect("row after the first drain");
    assert!(
        !before.2.starts_with("running") && !before.2.starts_with("pending"),
        "a reset was already in progress before the fault: reset_state = {:?}",
        before.2
    );

    // The operator's lever, pulled the normal way, with the mock slow enough that the walk
    // is a window the kill can be seen to land inside.
    mock.delay(300, 3000);
    log.add("operator requests a reset: reset_requested = 1");
    db.exec("UPDATE lazy_wire_fire_rule SET reset_requested = 1 WHERE name = 'snapr'")
        .await;

    // The gate: the persisted row itself standing on step 3/4 — the third step's listing and
    // batches are the only thing that can put it there — with that step's calls at the mock
    // advancing. The row is written by the engine's own persist (every answered chunk marks
    // it dirty), so this is a condition, not a sleep.
    let snapb0 = mock_path_hits(&mock, "/snapB");
    let (state_at_gate, snapb_at_gate) = wait_for(
        Duration::from_secs(120),
        "the persisted row to stand on `running: step 3/4 fp… snapB` with the step's calls advancing",
        || async {
            let r = db.status("snapr").await.unwrap_or_default();
            let hits = mock_path_hits(&mock, "/snapB");
            let inside = r.2.starts_with("running: step 3/4 ") && r.2.contains(" snapB: ");
            (inside && hits > snapb0).then(|| (r.2.clone(), hits))
        },
    )
    .await;
    // The line a waiting chunk leaves in memory (the loop-top form, `drive_reset`) is what
    // the persist cadence usually catches; accept it, and require the counts only when the
    // batches form is what stood. Between the counter and the name sits the fingerprint
    // digest token, so the name is matched as a word, not right after the counter.
    assert!(
        state_at_gate.starts_with("running: step 3/4 ")
            && state_at_gate.contains(" snapB: ")
            && reset_progress(&state_at_gate).is_none_or(|(step, n, name, _done, tot)| step == 3
                && n == 4
                && name == "snapB"
                && tot == total as u64),
        "the gate row must be the step-3 progress line of a full walk: {state_at_gate:?}"
    );
    assert_eq!(
        reset_flag(&db, "snapr").await,
        Some(1),
        "the kill must happen while the operator's flag is still set — the engine clears \
         it only when the reset finishes"
    );
    // What the gate has proven about the steps before it, read as counters: the requested
    // run answered the announcement and all 10 `snapA` batches (its walk passed them — a
    // step advances only on answered chunks).
    let ann_frozen = mock_path_hits(&mock, "/reset-announce");
    let snapa_frozen = mock_path_hits(&mock, "/snapA");
    let snapc_gate = mock_path_hits(&mock, "/snapC");
    assert_eq!(
        snapa_frozen,
        2 * chunks,
        "the requested run had not answered the finished step's 10 batches at the gate \
         ({snapa_frozen} `/snapA` calls, counting the first start's walk): the gate row did \
         not stand on step 3 of a walk that had passed snapA"
    );
    assert_eq!(
        ann_frozen, 2,
        "the requested run had not answered the announcement at the gate: {ann_frozen}"
    );

    log.add(format!(
        "killing child 1 mid-snapB (row `{state_at_gate}`, step calls at the gate: \
         /snapA={snapa_frozen} /snapB={snapb_at_gate} /snapC={snapc_gate})"
    ));
    lazy.kill();

    // The restart finds the flag still 1 and the row still on the interrupted step: the run
    // that was serving that request continues at its step.
    let mut lazy2 = Lazy::start(
        "p2b_sigkill_during_requested_reset",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy2, 60);
    assert!(
        !lazy2.log_lines_matching("reset steps start").is_empty(),
        "the restarted child started no reset at all: the rule row still said \
         reset_requested = 1 and the status row still said `running:`, so it had to \
         ({:?})",
        db.status("snapr").await.map(|r| r.2)
    );
    assert!(
        !lazy2
            .log_lines_matching("starting from the saved positions")
            .is_empty(),
        "the restarted child did not boot on the saved positions: {:?}",
        lazy2.log_path
    );
    wait_for(
        Duration::from_secs(180),
        "the resumed reset to finish (status row `done:`)",
        || async {
            let r = db.status("snapr").await.unwrap_or_default();
            r.2.starts_with("done:").then_some(())
        },
    )
    .await;
    assert!(
        !lazy2.log_lines_matching("reset steps done").is_empty(),
        "the status row says done but the child never logged it: {:?}",
        lazy2.log_path
    );
    wait_for(
        Duration::from_secs(30),
        "the engine to clear the operator's reset_requested flag after finishing",
        || async { (reset_flag(&db, "snapr").await == Some(0)).then_some(()) },
    )
    .await;

    // THE proof, both read on counters the gate froze: the finished steps were not re-walked
    // (a walk-from-step-1 recovery moves both of these), and the interrupted step re-ran
    // whole. The chunk counts across the kill drop to the remaining steps, not the full one.
    let snapa_after = mock_path_hits(&mock, "/snapA");
    assert_eq!(
        snapa_after, snapa_frozen,
        "the restarted child re-sent the finished snapA step: {snapa_frozen} `/snapA` calls \
         stood answered at the kill, {snapa_after} after the reset finished — the row still \
         carried the request the saved run was serving, so its steps before the interrupted \
         one had been target-confirmed and owe no second send"
    );
    let ann_after = mock_path_hits(&mock, "/reset-announce");
    assert_eq!(
        ann_after, ann_frozen,
        "the restarted child re-sent the finished announcement step: {ann_frozen} calls at \
         the kill, {ann_after} after"
    );
    let snapb_after = mock_path_hits(&mock, "/snapB");
    assert!(
        snapb_after >= snapb_at_gate + chunks,
        "the interrupted step must be re-sent whole from its beginning ({chunks} batches): \
         {snapb_at_gate} `/snapB` calls at the gate, {snapb_after} at done"
    );
    let snapc_after = mock_path_hits(&mock, "/snapC");
    assert!(
        snapc_after >= snapc_gate + chunks,
        "the step after the interrupted one never ran: {snapc_gate} `/snapC` calls at the \
         gate, {snapc_after} at done"
    );
    // The line the resume itself prints (`add_rule`, engine.rs), read last so the chunk
    // counts above are what fails a walk-from-step-1 recovery.
    assert!(
        !lazy2
            .log_lines_matching("reset steps resume at step")
            .is_empty(),
        "the restarted child logged no resume of the saved run: {:?}",
        lazy2.log_path
    );
    assert_all_keys(&mock, 1, total, "P2b snapshot rows");
    eprintln!(
        "P2b: killed mid-snapB at `{state_at_gate}`; the restarted child resumed at step 3 \
         (finished steps added 0 calls: /reset-announce {ann_frozen}->{ann_after}, /snapA \
         {snapa_frozen}->{snapa_after}; interrupted + trailing steps answered {} /snapB and \
         {} /snapC calls across the kill)",
        snapb_after - snapb_at_gate,
        snapc_after - snapc_gate
    );
    assert_no_undelivered(&lazy);
    assert_no_undelivered(&lazy2);
    lazy2.kill();
}

// ------------------------------------------------------------------------ P2c unrequested reset resume

/// P2c (the resume half of the runs that started on their own): P2b's four steps, the same
/// paths and chunk counts — but no operator request anywhere. The reset is the first start's
/// own walk: the rule row stands in the table before the child ever starts, and the reason
/// `choose_start` writes is "first start". The kill lands mid the third step once the rule
/// and the instance rows are persisted — the restart must boot on their positions, which is
/// the one thing the resume leans on (the rows are the rule's confirmed point, and the
/// crashed child wrote them itself: nothing unconfirmed sits below them, `rule_pos` floors
/// the persisted position at the core's oldest unflushed mark). The restarted child must
/// continue the run there — no further `/reset-announce` and no further `/snapA` call from
/// it (the two counters a walk-from-step-1 recovery moves: these read red on main), the
/// interrupted `snapB` re-walked whole because its key set died with the process
/// (at-least-once), `snapC` run, the row finishing `done:` — the proof P2b holds for the
/// requested run, held now for one that started on its own. A boot that loses the rule's
/// continuity again refuses this resume (no candidate, no `skip_until`, fresh step-0 run);
/// that direction is P2's claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p2c_sigkill_during_unrequested_reset_resumes_the_saved_step() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_p2c").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("p2c-reset");
    let host = server_addr(&root);
    let mainp = FaultProxy::start(&host, "main", Some(log.clone())).await;
    let binp = FaultProxy::start(&host, "binlog", Some(log.clone())).await;
    let qp = FaultProxy::start(&host, "query", Some(log.clone())).await;
    db.target("hook", "http", &mock.url(), None, "{}").await;

    // P2b's four steps, three of them chunked: the two chunk counts the resumed child must
    // not repeat are the finished step's own (10 `/snapA` calls per walk), and the count it
    // must owe in full is the interrupted step's (10 more `/snapB` calls). The row carries
    // no request: this walk is the boot's own.
    let total = 1_000;
    let batch = 100u32;
    let chunks = (total / batch) as usize;
    let steps = serde_json::json!([
        {
            "name": "announce",
            "once": true,
            "template": "POST /reset-announce HTTP/1.1\n\n"
        },
        {
            "name": "snapA",
            "for_each": "snapshot",
            "template": "POST /snapA HTTP/1.1\n\n{{join keys \",\"}}"
        },
        {
            "name": "snapB",
            "for_each": "snapshot",
            "template": "POST /snapB HTTP/1.1\n\n{{join keys \",\"}}"
        },
        {
            "name": "snapC",
            "for_each": "snapshot",
            "template": "POST /snapC HTTP/1.1\n\n{{join keys \",\"}}"
        }
    ]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, snapshot_keys_sql, \
         batch_max_keys, reset_steps, debounce_ms) VALUES (?, ?, ?, ?, ?, ?, 100)",
        (
            "snapu",
            "hook",
            "POST /hook HTTP/1.1\n\n{{join keys \",\"}}",
            "SELECT id AS _key FROM items",
            batch,
            steps.to_string(),
        ),
    )
    .await;
    db.trigger("snapu", "items", Some("{{new.id}}"), None).await;

    // The rows are seeded before the child ever starts: they are older than any position
    // the stream could replay from, and the snapshot steps' own reads are their only
    // coverage — the exact shape that would lose keys if a restart skipped finished steps
    // it could not vouch for.
    bulk_items(&db, 1, total).await;
    // The mock is slow before the first walk begins: every answered chunk costs 600 ms, so
    // a ten-chunk step runs some six seconds — wider than the five-second persist cycle —
    // and the kill can be *seen* to land inside the step.
    mock.delay(600, 3000);

    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "p2c_sigkill_during_unrequested_reset",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);

    // The gate, read as P2b reads it: the persisted row standing on step 3/4 with the
    // third step's own calls advancing — plus the two positions the resume needs, both
    // status rows written. A "first start" boot claimed no position to begin with: only
    // what the crashed child persisted itself can keep the restart's replay at or below
    // every unconfirmed change, and these rows are that persisted word.
    let (state_at_gate, snapb_at_gate) = wait_for(
        Duration::from_secs(180),
        "the first start's reset to stand on `running: step 3/4 fp… snapB` in the persisted \
         row with the step's calls advancing and both status rows written",
        || async {
            let r = db.status("snapu").await.unwrap_or_default();
            let star = db.status("*").await.unwrap_or_default();
            let hits = mock_path_hits(&mock, "/snapB");
            let inside = r.2.starts_with("running: step 3/4 ") && r.2.contains(" snapB: ");
            let saved = !r.0.is_empty() && r.1 > 0 && !star.0.is_empty() && star.1 > 0;
            (inside && saved && hits > 0).then(|| (r.2.clone(), hits))
        },
    )
    .await;
    // The batches form is what the persist cadence usually catches; the loop-top form is
    // equally a step-3 line, and the counter only checks when keys are there to check.
    assert!(
        state_at_gate.starts_with("running: step 3/4 ")
            && state_at_gate.contains(" snapB: ")
            && reset_progress(&state_at_gate).is_none_or(|(step, n, name, _done, tot)| step == 3
                && n == 4
                && name == "snapB"
                && tot == total as u64),
        "the gate row must be the step-3 progress line of a full walk: {state_at_gate:?}"
    );
    assert_eq!(
        reset_flag(&db, "snapu").await,
        Some(0),
        "this seed never asks for a reset — the walk under the kill must be the boot's own, \
         not the operator's"
    );
    // What the gate has proven about the steps before it, read as counters: the walk
    // answered the announcement and all 10 `snapA` batches (a step advances only on
    // answered chunks, so its passing step 3 is the proof).
    let ann_frozen = mock_path_hits(&mock, "/reset-announce");
    let snapa_frozen = mock_path_hits(&mock, "/snapA");
    let snapc_gate = mock_path_hits(&mock, "/snapC");
    assert_eq!(
        snapa_frozen, chunks,
        "the first start's walk had not answered the finished step's 10 batches at the gate \
         ({snapa_frozen} `/snapA` calls): the gate row did not stand on step 3 of a walk that \
         had passed snapA"
    );
    assert_eq!(
        ann_frozen, 1,
        "the first start's walk had not answered the announcement at the gate: {ann_frozen}"
    );

    log.add(format!(
        "killing child 1 mid-snapB of the first start's own reset (row `{state_at_gate}`, \
         step calls at the gate: /snapA={snapa_frozen} /snapB={snapb_at_gate} \
         /snapC={snapc_gate})"
    ));
    lazy.kill();

    // The restart finds the row still on the interrupted step and no request in the rule
    // row: the boot takes the run up as its own carry-over and continues it at its step.
    let mut lazy2 = Lazy::start(
        "p2c_sigkill_during_unrequested_reset",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy2, 60);
    assert!(
        !lazy2.log_lines_matching("reset steps start").is_empty(),
        "the restarted child started no reset at all: the status row still said `running:`, \
         so it had to ({:?})",
        db.status("snapu").await.map(|r| r.2)
    );
    assert!(
        !lazy2
            .log_lines_matching("first start (resumed after restart)")
            .is_empty(),
        "the restarted child did not take the crashed run up as the boot's carry-over: {:?}",
        lazy2.log_path
    );
    assert!(
        !lazy2
            .log_lines_matching("starting from the saved positions")
            .is_empty(),
        "the restarted child did not boot on the saved positions — the resume reads a rule \
         that kept its position, and a boot that lost it again would have replayed from the \
         tail: {:?}",
        lazy2.log_path
    );
    wait_for(
        Duration::from_secs(180),
        "the resumed reset to finish (status row `done:`)",
        || async {
            let r = db.status("snapu").await.unwrap_or_default();
            r.2.starts_with("done:").then_some(())
        },
    )
    .await;
    assert!(
        !lazy2.log_lines_matching("reset steps done").is_empty(),
        "the status row says done but the child never logged it: {:?}",
        lazy2.log_path
    );

    // THE proof, both read on counters the gate froze: the finished steps were not
    // re-walked (a walk-from-step-1 recovery moves both of these), and the interrupted
    // step re-ran whole. The chunk counts across the kill drop to the remaining steps, not
    // the full one.
    let snapa_after = mock_path_hits(&mock, "/snapA");
    assert_eq!(
        snapa_after, snapa_frozen,
        "the restarted child re-sent the finished snapA step: {snapa_frozen} `/snapA` calls \
         stood answered at the kill, {snapa_after} after the reset finished — the rule kept \
         its position and the row carried this very def, so the steps before the interrupted \
         one had been target-confirmed and owe no second send"
    );
    let ann_after = mock_path_hits(&mock, "/reset-announce");
    assert_eq!(
        ann_after, ann_frozen,
        "the restarted child re-sent the finished announcement step: {ann_frozen} calls at \
         the kill, {ann_after} after"
    );
    let snapb_after = mock_path_hits(&mock, "/snapB");
    assert!(
        snapb_after >= snapb_at_gate + chunks,
        "the interrupted step must be re-sent whole from its beginning ({chunks} batches): \
         {snapb_at_gate} `/snapB` calls at the gate, {snapb_after} at done"
    );
    let snapc_after = mock_path_hits(&mock, "/snapC");
    assert!(
        snapc_after >= snapc_gate + chunks,
        "the step after the interrupted one never ran: {snapc_gate} `/snapC` calls at the \
         gate, {snapc_after} at done"
    );
    // The line the resume itself prints (`choose_start`, engine.rs), read last so the
    // chunk counts above are what fails a walk-from-step-1 recovery.
    assert!(
        !lazy2
            .log_lines_matching("reset steps resume at step")
            .is_empty(),
        "the restarted child logged no resume of the saved run: {:?}",
        lazy2.log_path
    );
    assert_all_keys(&mock, 1, total, "P2c snapshot rows");
    eprintln!(
        "P2c: killed mid-snapB of the first start's own reset at `{state_at_gate}`; the \
         restarted child resumed at step 3 (finished steps added 0 calls: /reset-announce \
         {ann_frozen}->{ann_after}, /snapA {snapa_frozen}->{snapa_after}; interrupted and \
         trailing steps answered {} /snapB and {} /snapC calls across the kill)",
        snapb_after - snapb_at_gate,
        snapc_after - snapc_gate
    );
    assert_no_undelivered(&lazy);
    assert_no_undelivered(&lazy2);
    lazy2.kill();
}

// ------------------------------------------------------------------------ P3 SIGTERM

/// P3 (design §2): the cheap shutdown control. The engine takes a SIGTERM while a batch
/// is demonstrably in flight at the target (the mock is answering 1 500 ms late and has
/// already taken the phase-3 request), and the stop-grace budget is `LAZYWF_STOP_GRACE_MS
/// = 3000` (`fault::Lazy::start`). This is the only place the suite asserts the *exit
/// code*: a chaos restart by SIGKILL never has one.
///
/// Properties, each with the assertion that says it:
/// * `Stop::Exited(0)` — not a signal death, not nonzero (P3's exit-code half);
/// * the stop took longer than the in-flight call's remaining wait (the engine really
///   waited for the running action) and well inside the grace plus slack;
/// * the child logged `shutdown requested` and `stopped` as JSON lines (the shutdown
///   path ran, in order, on its own);
/// * the in-flight batch was delivered exactly once and its keys survived into the next
///   child — which is asserted behaviourally: the restarted child boots on
///   `starting from the saved positions` at the saved row and replays nothing (every
///   key of the run, 1..=301, counted exactly once at the mock).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p3_graceful_sigterm() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_p3").await;
    chaos_shop(&db).await;
    let log = FaultLog::new();
    let mock = Mock::start().await;
    mock.set_fault_log(log.clone());
    mock.tag("p3-term");
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
        "p3_graceful_sigterm",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_log(&lazy.log_path);

    // Two phases drain normally first: the saved position the next child must find.
    for phase in 0..2u32 {
        let (from, to) = (1 + phase * 100, (phase + 1) * 100);
        bulk_orders(&db, from, to).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "o",
            &format!("phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &mock,
        );
        drain(&sp, &lazy, &db, "o", cp.clone(), 60).await;
    }

    // The in-flight window: every request is answered 1 500 ms after it arrives, inside
    // the target's own 3 000 ms timeout (a late-but-complete answer is still a delivery)
    // and inside the stop grace. SIGTERM may only be sent once the mock has *taken* the
    // phase-3 request and the rule is still unconfirmed — an event, never a sleep.
    mock.delay(1_500, 3_000);
    bulk_orders(&db, 201, 300).await;
    let cp3 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "o",
        "phase 3 (in flight at the signal)",
        cp3.clone(),
        keyset(201, 300),
        &mock,
    );
    wait_for(
        Duration::from_secs(60),
        "the phase-3 flush to be in flight at the mock (taken, unanswered, firing)",
        || async {
            let in_flight = mock
                .requests()
                .iter()
                .any(|r| r.path == "/hook" && r.body.split(',').any(|k| k.trim() == "201"));
            let row = db.status("o").await.map(|(f, p, _, _)| (f, p));
            let firing = lazy.status().await["rules"]["o"]["firing"].as_bool() == Some(true);
            (in_flight && firing && row.is_some_and(|r| r < cp3.clone())).then_some(())
        },
    )
    .await;
    log.add("SIGTERM sent while the phase-3 batch is in flight at the mock");
    let t_stop = Instant::now();
    let stop = lazy.term().await;
    let elapsed = t_stop.elapsed();
    if stop.is_environment_kill() {
        eprintln!("P3: SKIPPED BY ENVIRONMENT — the harness's child was taken; see tmp/out/chaos/env-aborts.log");
        return;
    }
    // The exit code, which only a clean shutdown has.
    assert!(
        matches!(stop, Stop::Exited(0)),
        "P3: SIGTERM with an in-flight batch ended {stop:?}, not a clean exit 0"
    );
    // The running action was waited for, not abandoned: the mock answers 1 500 ms after
    // the request arrives and the signal went out just after it did, so a stop that
    // finished in a flash had not waited for the lane. And the whole stop fits inside
    // the 3 000 ms grace with slack.
    assert!(
        elapsed >= Duration::from_millis(700),
        "P3: the child exited {elapsed:?} after SIGTERM although the in-flight call was \
         armed to answer 1 500 ms after arriving: the shutdown did not wait for the \
         running action before persisting"
    );
    assert!(
        elapsed <= Duration::from_millis(3_000 + 1_500),
        "P3: the stop took {elapsed:?}, beyond the 3 000 ms stop grace plus 1 500 ms of \
         harness slack — the engine ignored its deadline as much as a too-fast exit does"
    );
    // The child's own account of the shutdown, as JSON lines (`LAZYWF_LOG_FORMAT=json`).
    let requested = lazy.log_lines_matching("shutdown requested");
    assert_eq!(
        requested.len(),
        1,
        "P3: {} `shutdown requested` lines (the watcher logs exactly one): {:?}",
        requested.len(),
        lazy.log_path
    );
    let v: serde_json::Value = serde_json::from_str(&requested[0]).expect("json log line");
    assert_eq!(
        v["fields"]["message"].as_str(),
        Some("shutdown requested"),
        "the line matching `shutdown requested` is not the watcher's message: {v}"
    );
    let stopped = lazy.log_lines_matching("\"stopped\"");
    assert!(
        stopped.iter().any(|l| {
            serde_json::from_str::<serde_json::Value>(l)
                .ok()
                .filter(|v| v["fields"]["message"].as_str() == Some("stopped"))
                .is_some()
        }),
        "P3: no `stopped` message after the lane drained: {:?}",
        lazy.log_path
    );
    // The in-flight batch landed exactly once despite the signal — no lost and no
    // double-sent answer on the shutdown path.
    sp.check();
    assert_all_keys(&mock, 1, 300, "P3 (in-flight batch delivered)");

    // The position the next child resumes from: read after the clean exit, and the pair
    // `choose_start` takes its minimum over (P1's reading).
    db.tick().await;
    wait_row_past(&db, "o", &cp3.clone(), 30).await;
    let (pf, pp, _, _) = db.status("o").await.expect("rule row after the clean stop");
    let (sf, spos) = instance_row(&db)
        .await
        .expect("instance row after the clean stop");
    let (ef, ep) = if (sf.as_str(), spos) <= (pf.as_str(), pp) {
        (sf.clone(), spos)
    } else {
        (pf.clone(), pp)
    };

    // The second child: boots on the saved positions and re-delivers nothing — the
    // behavioural form of "the batch that confirmed before the exit was saved". One
    // more key proves the child is really streaming (an absence asserted against a
    // child that never woke would prove nothing), and then the exact-once count over
    // the whole run is the replay check.
    let mut lazy2 = Lazy::start(
        "p3_graceful_sigterm",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy2, 60);
    sp.add_log(&lazy2.log_path);
    let starts = lazy2.log_lines_matching("starting from the saved positions");
    assert!(
        !starts.is_empty(),
        "the restarted child did not boot on the saved positions: {:?}",
        lazy2.log_path
    );
    let line = starts.last().unwrap().clone();
    let v: serde_json::Value = serde_json::from_str(&line).expect("json log line");
    assert_eq!(
        v["fields"]["position"].as_str(),
        Some(format!("{ef}:{ep}").as_str()),
        "the restarted child did not resume at the minimum of the two saved rows \
         (instance {sf}:{spos}, rule {pf}:{pp})"
    );
    bulk_orders(&db, 301, 301).await;
    let cp4 = binlog_end(&db).await;
    db.tick().await;
    sp.add_phase(
        "o",
        "phase 4 (second child)",
        cp4.clone(),
        keyset(301, 301),
        &mock,
    );
    drain(&sp, &lazy2, &db, "o", cp4.clone(), 60).await;
    sp.check();
    assert_all_keys(&mock, 1, 301, "P3 (second child)");
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for (_, keys) in mock.deliveries() {
        for k in keys {
            *seen.entry(k).or_insert(0) += 1;
        }
    }
    let replayed: Vec<String> = (1..=301)
        .map(|i| i.to_string())
        .filter(|k| seen.get(k).copied().unwrap_or(0) != 1)
        .collect();
    assert!(
        replayed.is_empty(),
        "P3: {} keys were not delivered exactly once across the graceful stop and restart \
         (e.g. {:?}): a key whose flush confirmed before the SIGTERM must not be re-sent \
         by the next child, and one that was lost has no excuse either",
        replayed.len(),
        replayed.iter().take(5).collect::<Vec<_>>()
    );
    eprintln!(
        "P3: exit 0 in {elapsed:?} (stop grace 3 s, in-flight call 1.5 s), {} deliveries for \
         301 keys, each exactly once; the next child resumed at {ef}:{ep}",
        mock.delivery_count()
    );
    assert_no_undelivered(&lazy);
    assert_no_undelivered(&lazy2);
    lazy2.kill();
    sp.stop();
}

// -------------------------------------------------------------------------- C1 combined

/// The drain bound after the combined outage heals — measured, not guessed (design §3).
/// Three runs on MySQL 8.4 (`tmp/out/lwf-worker-chaos-pr4/evidence/run-c1_target_down_while_source_flaps.rep{1,2,3}.log`)
/// drained in 1.068 s, 1.082 s and 1.099 s; the bound is the measured figure with the
/// headroom a loaded box needs, and the seed prints the elapsed drain so a regression in
/// it stays visible. A heal that never lets the rules catch up is a hang this catches.
const C1_DRAIN_BOUND_SECS: u64 = 60;

/// C1 (design §2): the combined outage — the target is dead (`refuse`) for the whole
/// window while the binlog dump port flaps (6 resets, the S3 storm), under a load that
/// keeps both the key space and the versions moving: 6 × 250 write-once `orders`, 200
/// `customers` rows inserted and then re-updated twice (the `id:version` key template,
/// so every bump is a new key), and a sibling rule on healthy `items` traffic that must
/// keep advancing while everything else burns. "Outage ≠ data loss" end to end.
///
/// Properties:
/// * both faults are facts where they are performed — `flap_cuts`/`flap_resets` at the
///   dump proxy (and its mode back to `live`), `refusals()` at the target proxy, and the
///   stuck rules' `last_error` naming the refused port;
/// * the stuck rules' rows stay pinned below their first unconfirmed checkpoint all
///   window (the sampler's P2, checked here once per phase and at the end), while the
///   sibling rule's row walks past the last checkpoint *through* the window — the
///   combined claim that a dead target never waits for the stream and vice versa;
/// * after the heal everything drains within the bound: every orders key, every
///   `id:version` key, every sibling key, positions at or above their checkpoints, no
///   key given up on (P5), and any redelivery inside the window sits between the
///   storm's own fault events (P4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c1_target_down_while_source_flaps() {
    let root = need_db!();
    let db = Db::fresh(&root, "chaos_c1").await;
    chaos_shop(&db).await;
    // The customers table gains the version column the `id:version` key template rides on.
    db.exec("ALTER TABLE customers ADD COLUMN v INT NOT NULL DEFAULT 1")
        .await;
    let log = FaultLog::new();
    let down = Mock::start().await;
    down.set_fault_log(log.clone());
    down.tag("c1-down");
    let up = Mock::start().await;
    up.set_fault_log(log.clone());
    up.tag("c1-up");
    let tprog = FaultProxy::start(&down.addr.to_string(), "c1-target", Some(log.clone())).await;
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
    join_rule(&db, "o", "down", "orders").await;
    // The version-bump rule: one rule that takes both writes and key updates.
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 100)",
        ("v", "down", "POST /hook HTTP/1.1\n\n{{join keys \",\"}}"),
    )
    .await;
    db.trigger("v", "customers", Some("{{new.id}}:{{new.v}}"), None)
        .await;
    join_rule(&db, "sib", "free", "items").await;
    tprog.refuse(true);
    let (dsn_main, dsn_bin, dsn_q) = (
        via(&root, mainp.addr(), &db.schema),
        via(&root, binp.addr(), &db.schema),
        via(&root, qp.addr(), &db.schema),
    );
    let mut lazy = Lazy::start(
        "c1_target_down_while_source_flaps",
        &db,
        &proxy_env(&dsn_main, &dsn_bin, &dsn_q),
        Some(log.clone()),
    );
    ready_or_skip!(lazy, 60);
    let sp = Sampler::start(&db, log.clone());
    sp.add_proxy(&mainp);
    sp.add_proxy(&binp);
    sp.add_proxy(&qp);
    sp.add_proxy(&tprog);
    sp.add_log(&lazy.log_path);
    wait_for(
        Duration::from_secs(30),
        "the engine to be streaming before the storm starts",
        || async {
            lazy.status().await["streaming"]
                .as_bool()
                .unwrap_or(false)
                .then_some(())
        },
    )
    .await;

    // The storm first, so its cuts fall in the first half of the window and the writes
    // are interleaved with it (S3's pacing: every phase is gated on the proxy's own
    // count of reset connections, never on the clock).
    binp.flap(6);
    log.add("combined outage: dump port flapping (6 cuts) while the target port is dead");
    let mut cps: Vec<(String, u64)> = Vec::new();
    let mut cust_sets: Vec<BTreeSet<String>> = Vec::new();
    for phase in 0..6u32 {
        let (from, to) = (1 + phase * 250, (phase + 1) * 250);
        bulk_orders(&db, from, to).await;
        // Customers: inserted in phase 1, bumped twice after — each bump mints 200
        // fresh `id:version` keys on the stuck rule.
        match phase {
            0 => {
                let rows: Vec<String> = (1..=200).map(|i| format!("({i}, 'c{i}', 1)")).collect();
                db.exec(&format!(
                    "INSERT INTO customers (id, name, v) VALUES {}",
                    rows.join(",")
                ))
                .await;
                cust_sets.push(version_keys(1, 200, 1));
            }
            1 | 2 => {
                db.exec("UPDATE customers SET v = v + 1 WHERE id BETWEEN 1 AND 200")
                    .await;
                cust_sets.push(version_keys(1, 200, phase + 1));
            }
            _ => {}
        }
        // The sibling's healthy traffic, 100 fresh items keys per phase.
        let (ifrom, ito) = (6_001 + phase * 100, 6_100 + phase * 100);
        bulk_items(&db, ifrom, ito).await;
        let cp = binlog_end(&db).await;
        db.tick().await;
        sp.add_phase(
            "o",
            &format!("orders phase {}", phase + 1),
            cp.clone(),
            keyset(from, to),
            &down,
        );
        if let Some(keys) = cust_sets.get(phase as usize) {
            sp.add_phase(
                "v",
                &format!("customers v={}", phase + 1),
                cp.clone(),
                keys.clone(),
                &down,
            );
        }
        sp.add_phase(
            "sib",
            &format!("sibling phase {}", phase + 1),
            cp.clone(),
            keyset(ifrom, ito),
            &up,
        );
        cps.push(cp);
        // Pacing on the proxy's own counter (S3's pattern), capped at the six cuts the
        // storm makes: the later phases write after the storm, while the target is
        // still dead — the outage keeps its second half without the storm.
        let seen = ((phase as u64 + 1) * 2).min(6);
        binp.wait(
            move |s| s.flap_cuts >= seen,
            &format!("{seen} dump connections reset by the storm"),
        )
        .await;
    }
    binp.wait(
        |s| s.flap_cuts == 6 && s.mode == "live",
        "the storm to be over: 6 reset connections and the port live again",
    )
    .await;

    // Both faults observed, each counted where it is performed. The target count is
    // gated on the engine's own attempt first: `last_error` naming the refused port is
    // the event that says a connect happened, and only then is "the port refused it"
    // this proxy's claim to make.
    let st = binp.stat();
    assert_eq!(
        st.flap_cuts, 6,
        "the storm claimed {} cuts, not the design's 6",
        st.flap_cuts
    );
    assert_eq!(
        st.flap_resets, 6,
        "the storm counted {} cuts but reset only {} sockets: a number, not a fault",
        st.flap_cuts, st.flap_resets
    );
    wait_pending(&lazy, "o", 30).await;
    wait_pending(&lazy, "v", 30).await;
    let err = wait_rule_error(&db, "o", &tprog.addr().to_string(), 60).await;
    assert!(
        tprog.refusals() >= 1,
        "the target port was refused {} connections while the rule's own last_error says \
         the engine dialled it ({err}): a dead endpoint nobody refused is not an outage",
        tprog.refusals()
    );
    // The stuck rules stay pinned below their first unconfirmed checkpoint while the
    // storm is over and the target is still dead; the sibling walked through it.
    let (fo, po, _, _) = db.status("o").await.expect("stuck o row");
    assert!(
        (fo.clone(), po) < cps[0].clone(),
        "the stuck rule reached {fo}:{po} while its first checkpoint {}:{} is unconfirmed",
        cps[0].0,
        cps[0].1
    );
    let (fv, pv, _, _) = db.status("v").await.expect("stuck v row");
    assert!(
        (fv.clone(), pv) < cps[0].clone(),
        "the version rule reached {fv}:{pv} while its first checkpoint {}:{} is unconfirmed",
        cps[0].0,
        cps[0].1
    );
    sp.check();
    wait_row_past(&db, "sib", &cps[5], 90).await;
    sp.check();
    log.add(format!(
        "combined outage observed: {} refusals at the target, 6 dump cuts, o pinned at \
         {fo}:{po}, sibling past the last checkpoint — healing the target",
        tprog.refusals()
    ));
    let t_drain = Instant::now();
    tprog.heal();
    // The bound: the retry backoff is capped at 800 ms (LAZYWF_MAX_BACKOFF_MS), the
    // storm is over, so what remains is one backoff plus the queued flushes.
    drain(&sp, &lazy, &db, "o", cps[5].clone(), C1_DRAIN_BOUND_SECS).await;
    let drained_in = t_drain.elapsed();
    sp.check();
    wait_row_past(&db, "v", &cps[2], 90).await;
    assert_all_keys(&down, 1, 1_500, "C1 orders");
    assert_keys(&down, &union_keys(&cust_sets), "C1 customers");
    assert_all_keys(&up, 6_001, 6_600, "C1 sibling");
    let (delivered, repeated) = delivery_report(&down);
    let (sib_delivered, sib_repeated) = delivery_report(&up);
    eprintln!(
        "C1: {} orders + {} customer-version + 600 sibling write-once keys through a dead \
         target and a 6-cut dump storm; target deliveries={delivered} (repeated keys=\
         {repeated}, inside the outage's fault events), sibling deliveries={sib_delivered} \
         (repeated={sib_repeated}), refusals={}, dump cuts={}, drained in {:?} \
         (bound {C1_DRAIN_BOUND_SECS} s), last_error={err}",
        1_500,
        cust_sets.iter().map(|s| s.len()).sum::<usize>(),
        tprog.refusals(),
        st.flap_cuts,
        drained_in
    );
    assert_no_undelivered(&lazy);
    lazy.kill();
    sp.stop();
}
