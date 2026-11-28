//! The per-statement bound on the control connection: `ctl`/`ctl_my` end a control
//! operation that never answers as a [`ControlStalled`] transient error and mark the
//! connection to be replaced (the flag `housekeeping` short-circuits on and `heal_control`
//! acts on). A normal answer and an ordinary server error pass through untouched — the
//! stall flag stands only for the timeout, so a broken-connection heal never mistakes a
//! slow-but-live server (or a real SQL error) for a held connection. While that flag stands
//! a flush parks rather than fires ([`FlushParked`]), because firing needs a lock the heal
//! path has confirmed: that confirmation is a fact of its own (`lock_unconfirmed`), and a
//! refused take of the lock is a peer only when the holder the server names is not this node's
//! own session (see `denied_by_peer`).
//!
//! The end-to-end half of the proof is the chaos seed `e1_control_stall_keeps_the_stream`
//! (tests/chaos.rs): a held control connection stalls the loop without these bounds (red
//! on main) and does not with them (green on the branch), never advancing a position on a
//! timed-out write.

use super::*;

/// The bound a test watches: milliseconds, not the product's 10 s. Every call site passes
/// `CONTROL_STATEMENT_TIMEOUT` itself, so this only shortens what the helper waits.
const BOUND: Duration = Duration::from_millis(50);

#[tokio::test]
async fn ctl_passes_a_normal_answer_through() {
    let stalled = AtomicBool::new(false);
    assert_eq!(
        ctl(&stalled, "SELECT 1", BOUND, async { Ok(7i64) })
            .await
            .unwrap(),
        7
    );
    assert!(
        !stalled.load(Ordering::Relaxed),
        "an answered statement is not a stall episode"
    );
}

#[tokio::test]
async fn ctl_turns_a_silent_statement_into_a_transient_control_error() {
    let stalled = AtomicBool::new(false);
    // A statement that never answers (the half-open session, the parked metadata lock):
    // the bound ends the await with a named, typed error the existing arms (warn once +
    // `heal_control`) take, and the connection is marked to be replaced — the abandoned
    // statement may leave its answer unread on the socket, so nothing else may run on it.
    let e = ctl(
        &stalled,
        "SELECT UNIX_TIMESTAMP()",
        BOUND,
        std::future::pending::<Result<i64>>(),
    )
    .await
    .unwrap_err();
    assert!(e.downcast_ref::<ControlStalled>().is_some(), "{e:#}");
    assert!(e.to_string().contains("SELECT UNIX_TIMESTAMP()"), "{e}");
    assert!(
        !e.to_string().contains("instance lock"),
        "a stall is not a lost lock: housekeeping's persist arm reads that phrase as \
         lock-lost and exits — it must go to the heal path instead ({e})"
    );
    assert!(
        stalled.load(Ordering::Relaxed),
        "a timed-out statement must mark the connection to be replaced"
    );
}

#[tokio::test]
async fn ctl_a_stall_stands_until_cleared_and_logs_once() {
    // The flag is what the log-once gate reads (`ctl_note` warns only on the transition
    // into the episode) and what `housekeeping` short-circuits on: it must stand across
    // further timeouts and clear only when the heal path replaces the connection.
    let stalled = AtomicBool::new(false);
    for _ in 0..3 {
        let e = ctl(
            &stalled,
            "the lazy_wire_fire_status write",
            BOUND,
            std::future::pending::<Result<()>>(),
        )
        .await
        .unwrap_err();
        assert!(e.downcast_ref::<ControlStalled>().is_some());
        assert!(stalled.load(Ordering::Relaxed));
    }
    // `heal_control` clears it after a successful reconnect + re-take of the lock; from
    // here the tick runs statements again, and a fresh timeout would warn afresh (a new
    // episode is worth one line).
    stalled.store(false, Ordering::Relaxed);
    assert!(!stalled.load(Ordering::Relaxed));
}

#[tokio::test]
async fn ctl_my_passes_answers_and_server_errors_through_unmarked() {
    let stalled = AtomicBool::new(false);
    assert_eq!(
        ctl_my(&stalled, "COM_PING", BOUND, async { Ok(()) })
            .await
            .unwrap(),
        ()
    );
    // An ordinary driver error is exactly the old broken-connection path: the arms that
    // match it (`heal_control`'s ping, lock.rs's standby) keep working, and it is not a
    // stall episode — the flag is for the fault no error byte ever arrived for.
    let e = ctl_my::<(), _>(&stalled, "COM_PING", BOUND, async {
        Err(mysql_async::Error::Driver(
            mysql_async::DriverError::ConnectionClosed,
        ))
    })
    .await
    .unwrap_err();
    assert!(e.downcast_ref::<mysql_async::Error>().is_some(), "{e:#}");
    assert!(!stalled.load(Ordering::Relaxed));
}

#[tokio::test]
async fn ctl_my_marks_only_the_silence() {
    let stalled = AtomicBool::new(false);
    let e = ctl_my(
        &stalled,
        "SELECT GET_LOCK for the instance lock",
        BOUND,
        std::future::pending::<mysql_async::Result<Option<i64>>>(),
    )
    .await
    .unwrap_err();
    assert!(e.downcast_ref::<ControlStalled>().is_some(), "{e:#}");
    assert!(stalled.load(Ordering::Relaxed));
}

/// A flush of a stalled control plane parks in front of the fence instead of asking for one:
/// `action::fence` runs on a pool connection of the same account, so the fence is exactly the
/// thing that could answer on a session whose instance lock the heal path has not confirmed
/// — and firing on it would be firing without the lock. The pool here points at a port
/// nothing answers on, so this returning at once, with its keys back and nothing fired, is
/// only possible if the gate came first; a flush that reached the fence would have spent the
/// fence's own bounds there.
#[tokio::test]
async fn a_flush_parks_rather_than_fire_on_an_unconfirmed_lock() {
    let dead = || mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/lwf");
    let env = Env::new(
        dead(),
        dead(),
        crate::source::Flavor::MySql,
        Duration::ZERO,
        1,
    );
    let def = crate::model::RuleDef::placeholder(&crate::model::RawRule {
        name: "orders_sync".into(),
        enabled: true,
        ..Default::default()
    });
    let rule = Rule {
        def,
        fingerprint: String::new(),
        templates: crate::template::Templates::default(),
        selects: Vec::new(),
        triggers: Vec::new(),
        target: None,
        error: None,
        trigger_error: None,
    };
    let keys = Arc::new(vec!["1".to_string(), "2".to_string()]);
    let spec = FenceSpec {
        gtid: None,
        pos: Position {
            file: "mysql-bin.000001".into(),
            pos: 4,
        },
    };
    let out = flush_job(
        &env,
        &rule,
        spec,
        Arc::clone(&keys),
        &[],
        false,
        Arc::new(FireGate::default()),
    )
    .await;
    let Outcome::Flush {
        fired,
        keep_queries,
        ..
    } = out
    else {
        panic!("a flush answers with a flush outcome");
    };
    assert_eq!(
        fired.failed_keys,
        *keys.as_ref(),
        "the parked keys go back to the buffer: a park costs latency, never a delivery"
    );
    assert!(keep_queries, "the flush's queries survive the park");
    assert_eq!(
        (fired.ok, fired.failed, fired.skipped),
        (0, 0, 0),
        "nothing was asked of the target"
    );
    let why = fired.error.expect("the park says why in the record");
    assert!(why.contains("firing waits for the heal path"), "{why}");
    assert!(
        !why.contains("instance lock"),
        "a park is a flush holding still, not a lost lock: housekeeping's persist arm reads \
         that phrase as lock-lost and exits ({why})"
    );
    // The same error the housekeeping arms keep quiet about, so a held role costs one log
    // line per episode and not one per due tick.
    assert!(control_stalled_err(&anyhow::Error::from(FlushParked)));
}

/// The two facts that hold a flush back are separate and either one is enough: a control plane
/// no statement can answer for (so no lock probe can either), and an instance lock the heal path
/// has replaced the owner of without yet taking it back. The second is why the gate cannot read
/// the stall flag alone: after a refused take the connection answers perfectly, and `action::fence`
/// rides a pool connection — it would answer for a node that does not hold the lock.
#[test]
fn a_flush_fires_only_with_a_trusted_plane_and_a_confirmed_lock() {
    assert!(may_fire(false, false), "the normal case: a flush fires");
    assert!(
        !may_fire(true, false),
        "a stalled control plane cannot prove the lock, so nothing fires"
    );
    assert!(
        !may_fire(false, true),
        "the connection is healthy but the take is owed: firing here would fire without the lock"
    );
    assert!(!may_fire(true, true));
}

/// `GET_LOCK`'s refusal cannot tell a peer from this node's own session that the server has not
/// finished closing, and the two have opposite meanings: one is fatal, the other is the race the
/// heal path exists to wait out. The holder the server names tells them apart, and only a named
/// holder that is not our own session proves a takeover — every other answer is retried, parked,
/// and never fatal on an unproven claim.
#[test]
fn only_a_foreign_holder_proves_a_peer() {
    assert!(
        denied_by_peer(Some("41"), Some("22")),
        "another session holds it: the node whose place was taken must say so and stop"
    );
    assert!(
        !denied_by_peer(Some("22"), Some("22")),
        "that is this node's own closing session: no peer is proven, so it is waited out"
    );
    assert!(
        !denied_by_peer(None, Some("22")),
        "the lock read as free a moment later: the next take should get it"
    );
    assert!(
        !denied_by_peer(Some("41"), None),
        "with no recorded id of our own there is no proof either way, so no fatal"
    );
}

/// The one case the holder answer cannot settle: connection ids start again when the server
/// restarts, so a peer that took the lock afterwards can be holding it under **the very id this
/// node recorded** — the same answer as our own closing session, and no wiser. It is therefore
/// not fatal at once (a node whose server restarted under it would then be killed by its own
/// healed connection), and the denial budget is what keeps it finite: a peer in that position
/// can delay the same `LockDenied` fatal by the rounds of the budget, each already waiting
/// [`CONTROL_LOCK_WAIT`], and cannot escape it.
#[test]
fn a_reused_connection_id_delays_the_fatal_but_cannot_prevent_it() {
    // The holder reads as our own id: no proof, so the node does not die on this answer.
    assert!(
        !denied_by_peer(Some("22"), Some("22")),
        "an id the server may have handed out again is the same answer as our own closing session"
    );
    // ... and the budget is what ends it: short of the budget it is retried, at the budget the
    // refusal is final.
    assert!(
        !unproven_refusal_is_final(1),
        "one unproven refusal is waited out, not fatal"
    );
    assert!(
        !unproven_refusal_is_final(CONTROL_LOCK_DENIES - 1),
        "the last refusal before the budget is still waited out"
    );
    assert!(
        unproven_refusal_is_final(CONTROL_LOCK_DENIES),
        "at the budget the same fatal follows: the wait delays it, it cannot prevent it"
    );
    assert!(
        unproven_refusal_is_final(CONTROL_LOCK_DENIES + 1),
        "and it stays final past the budget"
    );
}
