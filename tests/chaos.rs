//! Chaos seeds (PRs 1 and 2 of the chaos suite, design §2/§5): the engine runs as a real child
//! process, in-process `FaultProxy`s separate it from the shared servers per role, and every
//! fault transition is gated on an observed event (`/status` fields, proxy counters, mock
//! deliveries), never on a wall-clock sleep. S1 `source_cut_mid_stream`, T1 `target_down`
//! (with a healthy sibling rule that must keep advancing) and P1 `sigkill_mid_flush` exercise
//! P1–P5 jointly; S2–S5 add the rest of the source class (stall, flapping, a cut inside a
//! 50 000-row group, and an XA whose COMMIT lands across a cut and across a kill). `h1` and `h2`
//! are harness self-tests, and they are the reason two of the sampler's invariants can be trusted:
//! each forges a row that *should* be refused — a position below a declared XA floor, and a position
//! at the checkpoint of a phase whose keys never arrived — and requires the refusal. An invariant
//! nobody has seen fail is not an invariant.
//!
//! `make test-chaos`, or through the shared runner: `BIN=<chaos binary> TESTS_DIR=<tests>
//! tmp/e2e-shared.sh` (schema names stay literal `chaos_*` so its cleanup finds them).

mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::fault::{
    drain, sweep_ran, sweep_xa_once, xa_recover_lwf, FaultLog, FaultProxy, Lazy, Sampler, XaGuard,
    XaPin,
};
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
    lazy2.ready(60).await;
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
    lazy.ready(60).await;
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
    lazy.ready(60).await;
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
    lazy.ready(60).await;
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
    lazy.ready(60).await;
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
        lazy.ready(60).await;
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
    lazy.ready(60).await;
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

    // Stop the child first: a row the engine still owns cannot stay forged.
    lazy.term().await;
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
    lazy.ready(60).await;
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

    // Forge to exactly the checkpoint, with the child gone so nobody writes the row back.
    lazy.term().await;
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
