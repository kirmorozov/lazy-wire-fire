//! The fire gate read at the last instant before a send (2026-11-27, #228 E-2).
//!
//! `flush_job` decides the dispatch at spawn: `lock_confirmed` is the answer from that moment.
//! A control loss that lands after the spawn and before the first send used to find the job
//! already past every gate — the fence rides a pool connection, `resolve_keys` rides the query
//! pool — and it fired while the instance lock was already suspected elsewhere. The job now
//! reads the shared [`FireGate`] again immediately before `action::fire_keys` and parks without
//! touching the wire.
//!
//! `lock_confirmed=true` with a parked gate is exactly the state the race produces: the
//! spawn-time snapshot says confirmed, the live cell says otherwise. It was unreachable before
//! this change, which is why no test held it. The target of flesh and TCP (the
//! `flush_keys_alloc_tests` pattern) makes the negative visible: a send that never happened is
//! an accepted connection that never arrived. The fence needs a live server, so these run
//! against `LAZYWF_TEST_DSN` and skip without one, like the flush allocation tests.

use super::*;
use crate::model::{RawTarget, TargetDef};
use crate::rule::{Rule, ACTION};
use crate::target::Target;
use crate::template::{Templates, ACTION_ROOTS};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn dsn() -> Option<String> {
    std::env::var("LAZYWF_TEST_DSN")
        .ok()
        .filter(|d| !d.is_empty())
}

/// A target of flesh and TCP that counts the connections it accepts: every request gets 200,
/// and the count is the witness — a flush that parked never let one through.
fn mock_target() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    l.set_nonblocking(true).expect("nonblocking for tokio");
    let addr = l.local_addr().expect("an address");
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&hits);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for the target");
        rt.block_on(async move {
            let l = tokio::net::TcpListener::from_std(l).expect("the listener moves");
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let hits = Arc::clone(&counted);
                tokio::spawn(async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    let mut piece = [0u8; 8192];
                    loop {
                        let mut buf: Vec<u8> = Vec::new();
                        loop {
                            match tokio::time::timeout(
                                Duration::from_millis(300),
                                s.read(&mut piece),
                            )
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
    (addr, hits)
}

/// A rule that fires for real: an http target on the mock, no selects (so `load_docs` answers
/// the docs itself and the database is asked nothing but the fence), and the action template
/// the send renders.
fn gate_rule(urls: &[String]) -> Arc<Rule> {
    let def = crate::model::RuleDef::placeholder(&crate::model::RawRule {
        name: "fire_gate_test".into(),
        enabled: true,
        ..Default::default()
    });
    let mut templates = Templates::default();
    templates
        .add(
            ACTION,
            "POST /hook HTTP/1.1\nContent-Type: application/json\n\n{{json docs}}",
            ACTION_ROOTS,
        )
        .expect("the template compiles");
    let tdef = TargetDef::from_row(&RawTarget {
        name: "fire_gate".into(),
        kind: Some("http".into()),
        endpoints: Some(Value::Array(urls.iter().map(|u| json!(u)).collect()).to_string()),
        vars: Some("{}".into()),
        ..Default::default()
    })
    .expect("the target parses");
    let target = Target::new(
        tdef,
        mysql_async::Pool::new("mysql://root:root@127.0.0.1:1/x"),
        None,
    )
    .expect("the target builds");
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates,
        selects: Vec::new(),
        triggers: Vec::new(),
        target: Some(target),
        error: None,
        trigger_error: None,
    })
}

fn keys() -> Arc<Vec<String>> {
    Arc::new(vec!["order-00001".to_string(), "order-00002".to_string()])
}

fn spec() -> FenceSpec {
    FenceSpec {
        gtid: None,
        pos: Position {
            file: "mysql-bin.000001".into(),
            pos: 4,
        },
    }
}

async fn flush(dsn: &str, rule: &Rule, lock_confirmed: bool, gate: Arc<FireGate>) -> Fired {
    let pool = mysql_async::Pool::new(dsn);
    let env = Env::new(
        pool.clone(),
        pool,
        crate::source::Flavor::MySql,
        Duration::ZERO,
        1,
    );
    let out = flush_job(&env, rule, spec(), keys(), &[], lock_confirmed, gate).await;
    let Outcome::Flush { fired, .. } = out else {
        panic!("a flush answers with a flush outcome");
    };
    fired
}

/// The window's non-event and the proof the harness really sends: spawn said confirmed, the
/// gate is still clear at the send, and the batch reaches the target.
#[tokio::test]
async fn a_clear_gate_lets_the_flush_send() {
    let Some(dsn) = dsn() else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    let (addr, hits) = mock_target();
    let rule = gate_rule(&[format!("http://{addr}/hook")]);
    let fired = flush(&dsn, &rule, true, Arc::new(FireGate::default())).await;
    assert!(
        fired.error.is_none(),
        "the batch went out: {:?}",
        fired.error
    );
    assert_eq!(fired.keys_done, 2, "both keys are done: {fired:?}");
    assert!(fired.failed_keys.is_empty(), "nothing to retry: {fired:?}");
    assert!(hits.load(Ordering::Relaxed) > 0, "the target was reached");
}

/// The race itself: the control loss lands after the spawn. The snapshot the job carries says
/// confirmed, the shared cell says otherwise — either flag, set at any point before the send,
/// parks the batch before its first request. The keys go back to the buffer whole for the
/// retry, nothing is counted against the target, and the mock stays untouched: the fence and
/// the resolution rode the database, the wire is where the gate must hold.
#[tokio::test]
async fn a_loss_after_the_spawn_parks_the_batch_before_the_first_send() {
    let Some(dsn) = dsn() else {
        eprintln!("LAZYWF_TEST_DSN not set: skipping");
        return;
    };
    for stalled in [true, false] {
        let (addr, hits) = mock_target();
        let rule = gate_rule(&[format!("http://{addr}/hook")]);
        let gate = Arc::new(FireGate::default());
        if stalled {
            gate.control_stalled.store(true, Ordering::Relaxed);
        } else {
            gate.lock_unconfirmed.store(true, Ordering::Relaxed);
        }
        let fired = flush(&dsn, &rule, true, gate).await;
        let why = fired.error.expect("the park says why in the record");
        assert!(why.contains("firing waits for the heal path"), "{why}");
        assert_eq!(
            fired.failed_keys,
            *keys(),
            "the parked keys go back to the buffer"
        );
        assert_eq!((fired.ok, fired.failed, fired.skipped), (0, 0, 0));
        assert_eq!(fired.keys_done, 0, "nothing was delivered");
        assert_eq!(
            hits.load(Ordering::Relaxed),
            0,
            "the gate held before the first request"
        );
    }
}
