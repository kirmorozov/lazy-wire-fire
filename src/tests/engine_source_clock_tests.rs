//! The source clock: the ported `SourceClock`, its two head proofs, and the settle gate.
//!
//! The clock and its tests are ported from lazyview's engine (the `clock_tests` beside
//! `SourceClock` at the locked commit); lazywf's deviations are marked as such: `lag_with` takes a
//! `settled` flag (a rule with keys still pending never reads zero) and the head proof from the
//! caller, the proofs are split-out free functions (the [`on_item_failure`] idiom — the unit test
//! drives the decision without a live engine), the heartbeat proof's fallback only stands while the
//! payload position cannot be matched, and [`read_proves_head`] proves the head for a source that
//! keeps writing and so never heartbeats.

use super::*;
use crate::alloc_measure;
use crate::model::RawRule;

fn pos(file: &str, p: u64) -> Position {
    Position {
        file: file.into(),
        pos: p,
    }
}

// ---- the clock (ported `clock_tests`) ----

/// Ruling 1, "idle: 0": a stream at its head with a rule that has applied everything reads 0
/// seconds behind the source — and stays 0 under a host/source clock skew, because the figure is
/// source-clock minus source-clock (the offset never enters the zero branch).
#[test]
fn idle_is_zero_even_under_clock_skew() {
    let caught = SourceClock {
        fed_ts: 2000,
        gap_ts: 2000,
        head_ts: 0,
        at_head: true,
        reading: true,
        pending_read: false,
        offset: 3600, // the source's clock runs an hour ahead of the host's
    };
    assert_eq!(
        caught.lag_with(2000, true, caught.at_head),
        0,
        "caught up, at head: 0"
    );
    assert_eq!(
        caught.lag_with(0, true, caught.at_head),
        0,
        "nothing applied yet, measured against the gap head: 0"
    );
}

/// Ruling 1, "backlog: the real age": a rule that has applied only up to `applied_ts` while the
/// source has advanced shows the age of its newest applied change on the source clock.
#[test]
fn backlog_is_the_real_age() {
    let now = crate::time::unix_now();
    let applied = now - 30;
    let c = SourceClock {
        fed_ts: now,
        gap_ts: applied,
        head_ts: 0,
        at_head: true,
        reading: true,
        pending_read: false,
        offset: 0,
    };
    let lag = c.lag_with(applied, true, c.at_head);
    assert!(
        (29..=32).contains(&lag),
        "backlog lag {lag} is not ~30s of real age"
    );
}

/// The same real age under a day of skew: the number is source clock minus source clock, so the
/// host's difference never enters it (an engine whose clock is off by a day still reads ~45).
#[test]
fn the_real_age_excludes_the_skew() {
    let local = crate::time::unix_now() as i64;
    let offset: i64 = 86_400; // the source runs a day ahead of us
    let head = (local + offset - 45) as u64; // the head transaction: 45 source-seconds old
    let c = SourceClock {
        fed_ts: head,
        gap_ts: head,
        head_ts: 0,
        at_head: false, // not proven at the head: no zero branch, the age is what reads
        reading: true,
        pending_read: false,
        offset,
    };
    let lag = c.lag_with(0, true, c.at_head);
    assert!((43..=48).contains(&lag), "lag {lag} carries host skew");
    assert!(lag < 1_000, "the day of skew leaked into the age: {lag}");
}

/// Ruling 1, "restarted over a gap: about the gap's length": a rule that has applied nothing
/// since the process started (`applied_ts` 0) is measured against the oldest change this
/// connection has read (`gap_ts`), so a backlog that opened while the engine was down reads as
/// the gap's length rather than the age of the epoch.
#[test]
fn restarted_over_a_gap_reports_the_gap() {
    let now = crate::time::unix_now();
    let gap_start = now - 40;
    let c = SourceClock {
        fed_ts: now,
        gap_ts: gap_start,
        head_ts: 0,
        at_head: true,
        reading: true,
        pending_read: true, // the queue still holds read-not-applied changes
        offset: 0,
    };
    let lag = c.lag_with(0, true, c.at_head);
    assert!(
        (38..=44).contains(&lag),
        "gap lag {lag} is not ~40s (the length of the gap)"
    );
}

/// Ruling 1, "paused reader: grows": a rule caught up on content reads 0 while the reader runs,
/// but the same rule shows a growing number the moment binlog reading stops (between rounds,
/// `reading` is false) — the source keeps writing changes nobody is reading.
#[test]
fn paused_reader_grows() {
    let now = crate::time::unix_now();
    let applied = now - 5;
    let caught = SourceClock {
        fed_ts: applied,
        gap_ts: applied,
        head_ts: 0,
        at_head: true,
        reading: true,
        pending_read: false,
        offset: 0,
    };
    assert_eq!(
        caught.lag_with(applied, true, caught.at_head),
        0,
        "caught up while reading: 0"
    );
    let held = SourceClock {
        reading: false,
        ..caught
    };
    let lag = held.lag_with(applied, true, held.at_head);
    assert!(
        (4..=8).contains(&lag),
        "held-reader lag {lag} is not ~5s and growing"
    );
}

/// Ruling 1's epoch guard: a connection proven behind (the heartbeat's position was not the
/// applied one) before it read a single routed change has no `gap_ts` to age against — it ages
/// from the last heartbeat, not from 1970.
#[test]
fn proven_behind_before_the_first_read_ages_from_the_heartbeat() {
    let now = crate::time::unix_now();
    let c = SourceClock {
        fed_ts: 0,
        gap_ts: 0,
        head_ts: now - 7,
        at_head: false,
        reading: true,
        pending_read: false,
        offset: 0,
    };
    let lag = c.lag_with(0, true, c.at_head);
    assert!(
        (5..=10).contains(&lag),
        "lag {lag} is not ~7s of age since the heartbeat (and never the age of the epoch)"
    );
    assert!(
        lag < 60,
        "lag {lag} reads the epoch, not the last heartbeat"
    );
}

/// The engine literal seeds `head_ts` at the local now for exactly this: `housekeeping` can reach
/// `persist` through `idle` before the first connect, and an unseeded clock would read the age of
/// the epoch for a rule that has applied nothing. The seed is load-bearing — the unseeded default
/// is asserted to read the epoch beside it.
#[test]
fn the_pre_connect_seed_is_load_bearing() {
    let seeded = SourceClock {
        head_ts: crate::time::unix_now(),
        ..SourceClock::default()
    };
    assert_eq!(
        seeded.lag_with(0, false, seeded.at_head),
        0,
        "age from the seeded head: ~now"
    );
    let unset = SourceClock::default();
    assert!(
        unset.lag_with(0, false, unset.at_head) > 1_000_000,
        "an unseeded clock reads the epoch ({}) — the seed must stay",
        unset.lag_with(0, false, unset.at_head)
    );
}

// ---- lazywf's deviations ----

/// lazywf's deviation from the reference's `lag(applied_ts)`: a single-transaction backlog leaves
/// `applied_ts` at 0, the fallback then equals `fed_ts`, and a rule that still holds keys — or
/// that stands with a flush in flight, or between connects — must not read "caught up".
#[test]
fn keys_pending_with_no_applied_stamp_never_reads_zero() {
    let now = crate::time::unix_now();
    let head = now - 300; // the one transaction: gap == fed == its header ts
    let c = SourceClock {
        fed_ts: head,
        gap_ts: head,
        head_ts: now,
        at_head: true,
        reading: true,
        pending_read: false,
        offset: 0,
    };
    // The settled rule at the proven head: the zero branch.
    assert_eq!(c.lag_with(head, true, c.at_head), 0);
    // Keys still pending, nothing ever applied: the fallback equals `fed_ts`, but the rule has
    // not settled — the real age of the head, not zero.
    let lag = c.lag_with(0, false, c.at_head);
    assert!(
        (295..=305).contains(&lag),
        "a rule with keys pending read {lag}, not the head's age"
    );
    // A half-read transaction in hand (`pending_read`, the engine's `in_txn` at publish): the
    // source is ahead of the rules by what stands unread.
    let inflight = SourceClock {
        pending_read: true,
        ..c
    };
    assert!(
        inflight.lag_with(head, true, inflight.at_head) >= 295,
        "a flush in flight read zero"
    );
    // Reconnecting: no head proof survives, and `reading` is off — zero would be a lie.
    let reconn = SourceClock {
        reading: false,
        ..c
    };
    assert!(
        reconn.lag_with(head, true, reconn.at_head) >= 295,
        "a disconnected reader read zero"
    );
}

/// The rows feed: the gap freezes at the first routed row of a run, `fed_ts` follows the last
/// one, an unknown ts (0) moves nothing, and any head proof is invalidated — a row we hold came
/// from below the position a heartbeat proved. Unwatched and own-pen tables never reach this
/// call (only the `Route::Watched` arm feeds the clock), so they cannot make a backlog read
/// caught up — the same ruling the reference reached for its own pen.
#[test]
fn note_fed_tracks_the_head_and_arms_the_head_proof() {
    let mut c = SourceClock::default();
    c.note_fed(0); // an unknown header ts moves nothing
    assert_eq!(c, SourceClock::default(), "ts 0 is unknown, not 1970");
    c.note_fed(1000);
    assert_eq!(
        (c.fed_ts, c.gap_ts, c.at_head),
        (1000, 1000, false),
        "the first row freezes the gap and de-proves the head"
    );
    c.at_head = true; // a heartbeat proved the head
    c.head_ts = 1200;
    c.note_fed(1200); // a later row: the gap stays, the head follows, the proof is stale
    assert_eq!((c.fed_ts, c.gap_ts, c.at_head), (1200, 1000, false));
}

// ---- the head proof (the heartbeat arm, split out) ----

#[test]
fn heartbeat_proves_head_only_in_the_clear() {
    let applied = pos("mysql-bin.000002", 400);
    // The payload names exactly where we applied, in the file the reader stands in: proof.
    assert!(heartbeat_proves_head(
        false,
        &applied,
        &pos("mysql-bin.000002", 400),
        400
    ));
    // The same payload while the reader stands in another file (a rotation): not comparable,
    // and the reader is not at the applied position either.
    assert!(!heartbeat_proves_head(
        false,
        &applied,
        &pos("mysql-bin.000003", 400),
        400
    ));
    // The server stood ahead and we have read into those rows: the reader stands between the
    // applied position and the head — not the head.
    assert!(!heartbeat_proves_head(
        false,
        &applied,
        &pos("mysql-bin.000002", 450),
        900
    ));
    // A payload ahead of us that we have not matched: while the reader still stands at the
    // applied position, everything about those rows is "not read yet", and that equality is
    // the proof the reference uses on servers whose payload cannot be matched at all (the
    // MySQL and MariaDB dumps carry a file name where the plain position was expected, which
    // reads as this: an unusable number). The momentary over-proof it allows — rows still in
    // transit between the two equal positions — is bounded and self-correcting: their feed
    // de-proves the head the instant they arrive.
    assert!(heartbeat_proves_head(false, &applied, &applied, 900));
    // No usable payload: our own position being the reader's is the proof; a reader ahead of
    // us is not (unconsumed events stand in the pipe).
    assert!(heartbeat_proves_head(false, &applied, &applied, 0));
    assert!(!heartbeat_proves_head(
        false,
        &applied,
        &pos("mysql-bin.000002", 900),
        0
    ));
    // A half-read transaction in hand: a row of ours stands between the reader and the head.
    assert!(!heartbeat_proves_head(true, &applied, &applied, 400));
}

// ---- the head proof (the reader keeping up: the second one, lazywf's) ----

/// A source that keeps writing never lets its dump go quiet, so it never heartbeats and
/// `at_head` cannot be proven: a rule that has read and applied everything would age forever,
/// which is the false alarm this field exists to rule out. The second proof closes that: the
/// newest change the reader handed us — of ANY table, watched or not — was logged no more than
/// one heartbeat period ago on the source's clock, so the source is writing and the reader is
/// keeping up. A row of a table no rule reads proves it just the same: it says nothing about what
/// this rule still owes (only [`SourceClock::note_fed`] moves that), only that nothing of ours can
/// stand unread beyond the transit of a change this fresh.
#[test]
fn a_fresh_row_proves_the_head_without_a_heartbeat() {
    let applied = crate::time::unix_now() - 10; // the last watched change this rule applied
    let c = SourceClock {
        fed_ts: applied,
        gap_ts: applied,
        head_ts: 0,
        at_head: false, // no heartbeat: the dump was never empty for a whole period
        reading: true,
        pending_read: false,
        offset: 0,
    };
    let period = Duration::from_secs(5); // the default `LAZYWF_HEARTBEAT_MS`
                                         // A row of another table, logged three seconds ago, has reached us.
    let keeping_up = read_proves_head(c.now() - 3, c.now(), period);
    assert!(keeping_up, "the reader is keeping up with a writing source");
    assert_eq!(
        c.lag_with(applied, true, keeping_up),
        0,
        "a caught-up rule on a source that keeps writing reads 0 with no heartbeat"
    );
    // A period later with nothing new read: the proof has expired, and the reading is the age of
    // what the rule last applied again. An unproven head never buys a zero.
    assert!(!read_proves_head(applied, c.now(), period));
    assert!(
        c.lag_with(applied, true, false) >= 9,
        "the expired proof must age, not read 0"
    );
}

/// What the second proof cannot do is fake a head. A reader stalled behind a backlog hands old
/// stamps — the source is ahead of a reader that is not keeping up, which is exactly the number
/// this field exists to show — and a transaction logged at its statement start (a long `BEGIN` …
/// `COMMIT`) reads old for the same reason.
#[test]
fn a_stale_read_denies_the_head() {
    let now = crate::time::unix_now();
    let period = Duration::from_secs(5);
    assert!(
        read_proves_head(now - 5, now, period),
        "one period is still keeping up"
    );
    assert!(
        !read_proves_head(now - 6, now, period),
        "beyond it, the reader trails"
    );
    assert!(
        !read_proves_head(0, now, period),
        "nothing read at all proves nothing"
    );
    // A stamp ahead of the clock we measured (an offset not re-asked since the source's clock
    // moved) cannot be older than the window; the age branch saturates to 0 for such a stamp too,
    // so this is the same skew tolerance the rest of the figure already carries.
    assert!(read_proves_head(now + 60, now, period));
    // The window is the configured period: a degenerate 0 (heartbeats off) still accepts a change
    // logged this second and nothing older.
    assert!(read_proves_head(now, now, Duration::from_secs(0)));
    assert!(!read_proves_head(now - 1, now, Duration::from_secs(0)));
}

/// The heartbeat proof stands on its own: with no row of any table read this connection
/// (`last_event_ts` 0, so the second proof is out of the question), a heartbeat that proved the
/// head still reads 0 — which is what an idle source must give, however long it stays idle.
#[test]
fn the_heartbeat_proof_stands_without_any_row() {
    let c = SourceClock {
        fed_ts: 0,
        gap_ts: 0,
        head_ts: 1_000,
        at_head: true,
        reading: true,
        pending_read: false,
        offset: 0,
    };
    assert!(
        !read_proves_head(0, c.now(), Duration::from_secs(5)),
        "with nothing read, the second proof cannot fire"
    );
    assert_eq!(
        c.lag_with(0, true, c.at_head),
        0,
        "the heartbeat proves the head alone"
    );
    assert!(
        c.lag_with(0, true, false) > 0,
        "without either proof the rule ages from the heartbeat instant"
    );
}

/// Neither proof and nothing read at all: `at_head` false, no heartbeat, no row of any table
/// (`last_event_ts` 0). The head is not proven and the rule ages from the instant the connection
/// was seeded with — never 0, never from the epoch.
#[test]
fn nothing_read_and_no_heartbeat_ages() {
    let seeded = crate::time::unix_now() - 30;
    let c = SourceClock {
        head_ts: seeded,
        at_head: false,
        reading: true,
        ..Default::default()
    };
    assert!(
        !read_proves_head(0, c.now(), Duration::from_secs(5)),
        "a source nobody has read from proves nothing"
    );
    let lag = c.lag_with(0, false, c.at_head);
    assert!(
        (28..=33).contains(&lag),
        "a rule that has read nothing ages from the seed, not the epoch: {lag}"
    );
}

// ---- the settle gate ----

fn plain_rule() -> Arc<Rule> {
    let def = RuleDef::placeholder(&RawRule {
        name: "orders_sync".into(),
        enabled: true,
        ..Default::default()
    });
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates: template::Templates::default(),
        selects: Vec::new(),
        triggers: Vec::new(),
        target: None,
        error: None,
        trigger_error: None,
    })
}

fn rule() -> RuleState {
    RuleState {
        rule: plain_rule(),
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
    }
}

/// The gate is every state that can still bring an older key out and deliver it later: the
/// rule's own backlog (buffer, lane), its own work in flight, its error and backoff holds, its
/// pending reset run, its freeze — and the shared target's backoff, which the caller reads from
/// `target_backoff` (the `schedule` pieces).
#[test]
fn rule_settled_table() {
    let now = Instant::now();
    let mut st = rule();
    assert!(rule_settled(&st, false, now), "a clear rule has settled");
    assert!(
        !rule_settled(&st, true, now),
        "the target is still taking rows"
    );

    let (s, key) = st.buffer.locate("42");
    st.buffer.mark(
        s,
        key,
        &Position {
            file: "mysql-bin.000002".into(),
            pos: 4,
        },
    );
    assert!(
        !rule_settled(&st, false, now),
        "a key still in the buffer is not applied"
    );
    st.buffer = Buffer::default();

    st.lane = Some(Lane::Reset);
    assert!(!rule_settled(&st, false, now), "a lane is work of its own");
    st.lane = None;

    st.prep = true;
    assert!(
        !rule_settled(&st, false, now),
        "a prep job reads the source now"
    );
    st.prep = false;

    st.reset = Some(ResetRun {
        id: 1,
        reason: "w".into(),
        requested: false,
        fingerprint: String::new(),
        step: 0,
        phase: Phase::Start,
        touched: None,
        backoff: Backoff::default(),
    });
    assert!(
        !rule_settled(&st, false, now),
        "a pending reset replays from the snapshot"
    );
    st.reset = None;

    st.frozen_at = Some(pos("mysql-bin.000002", 4));
    assert!(
        !rule_settled(&st, false, now),
        "a frozen rule settles nothing"
    );
    st.frozen_at = None;

    st.backoff.until = Some(now + Duration::from_secs(60));
    assert!(
        !rule_settled(&st, false, now),
        "the retry timer holds the keys"
    );
    assert!(
        rule_settled(&st, false, now + Duration::from_secs(61)),
        "once the timer passes, the rule has settled"
    );
    st.backoff = Backoff::default();

    Arc::get_mut(&mut st.rule).expect("sole owner").error = Some("sink 500".into());
    assert!(
        !rule_settled(&st, false, now),
        "a rule error holds its keys back"
    );
    Arc::get_mut(&mut st.rule).unwrap().error = None;
    Arc::get_mut(&mut st.rule).unwrap().trigger_error = Some("broken trigger".into());
    assert!(
        !rule_settled(&st, false, now),
        "a trigger error stops the marks themselves"
    );
}

// ---- the hot-path budget ----

/// The gate the flush and dispatch tests hold, on the clock's half of the hot path: per routed
/// rows event the feed is integer stores only, and the publish-time reading is too. 1000 rows
/// and 1000 polls, zero allocations.
#[test]
fn source_clock_costs_no_allocation() {
    let mut c = SourceClock::default();
    let ((), n) = alloc_measure::count(|| {
        for i in 0..1000u64 {
            c.note_fed(std::hint::black_box(1_700_000_000 + i));
        }
    });
    assert_eq!(n, 0, "feeding the clock allocated {n} times for 1000 rows");
    let ((), n) = alloc_measure::count(|| {
        for i in 0..1000u64 {
            std::hint::black_box(c.lag_with(std::hint::black_box(i), true, c.at_head));
        }
    });
    assert_eq!(
        n, 0,
        "reading the clock lag allocated {n} times for 1000 polls"
    );
    // The second head proof and the publish path that combines them: the same integer arithmetic,
    // the same zero allocations.
    let ((), n) = alloc_measure::count(|| {
        for i in 0..1000u64 {
            let head = c.at_head
                || read_proves_head(
                    std::hint::black_box(1_700_000_000 + i),
                    c.now(),
                    Duration::from_secs(5),
                );
            std::hint::black_box(c.lag_with(std::hint::black_box(i), true, head));
        }
    });
    assert_eq!(
        n, 0,
        "proving the head by the read and publishing the lag allocated {n} times for 1000 polls"
    );
}
