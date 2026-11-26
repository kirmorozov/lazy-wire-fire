//! The `undecodable` arm of the stream loop (backlog V-0 (c), designed in
//! `tmp/out/lwf-worker-resync-followups/report.md` §(c)): an event the reader cannot decode is not
//! a network fault. The copies classify it (`source::Undecodable`, `source::is_undecodable` — the
//! classification itself is the copy set's own test, `src/binlog/tests.rs` 1018-1130); what these
//! tests hold the *engine* to is what it then does: back off instead of reconnecting every 200 ms,
//! say the stuck position once, never skip the event, and exit at once (not after ten re-reads)
//! when the refused bytes are the rows of a watched table.

use super::*;
use crate::source::Undecodable;

/// A `MakeWriter` into a shared buffer, for reading back what a subscriber wrote (the same device
/// `tests/engine.rs` uses for its log lines).
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Captured {
        self.clone()
    }
}

/// The WARN and ERROR lines a closure logged, as parsed JSON.
fn logged<T>(f: impl FnOnce() -> T) -> (T, Vec<Value>) {
    let out = Captured::default();
    let sub = tracing_subscriber::fmt()
        .json()
        .with_writer(out.clone())
        .with_max_level(tracing::Level::WARN)
        .finish();
    let got = tracing::subscriber::with_default(sub, f);
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    (
        got,
        text.lines()
            .map(|l| serde_json::from_str(l).expect("a JSON line"))
            .collect(),
    )
}

fn pos(n: u64) -> Position {
    Position {
        file: "binlog.000001".into(),
        pos: n,
    }
}

/// A refused rows event, tagged the way `Tracker::rows` tags one: kind, table, the position the
/// reader keeps coming back to, and the decoder's own words (compare `src/binlog/tests.rs` 1022).
fn tagged_rows(at: &Position) -> anyhow::Error {
    Undecodable::tag(
        anyhow!("no TABLE_MAP for the id"),
        "rows(UPDATE_ROWS_V1)(31)".into(),
        Some("shop.orders".into()),
        at,
    )
}

/// A refused mapping (an event kind this build refuses to read), tagged the way `Tracker::map_event`
/// tags one — no table: the bytes name none (compare `src/binlog/tests.rs` 1070).
fn tagged_mapping(at: &Position) -> anyhow::Error {
    Undecodable::tag(
        anyhow!("compressed events need `binlog_commit_order_support`"),
        "QUERY(compressed)(165)".into(),
        None,
        at,
    )
}

/// The error class the copies leave to `is_transient`: a read cut short is the source going away,
/// and the stream keeps the fast reconnect schedule for it.
fn truncated() -> anyhow::Error {
    anyhow::Error::new(mysql_async::Error::Io(mysql_async::IoError::Io(
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"),
    )))
}

// ---- the mapping arm: the back-off ladder -----------------------------------------------

/// The round that ends on bytes this build cannot decode must not earn the 200 ms a stream that is
/// moving earns: the next connect re-reads the same position and fails on the same bytes, so a
/// round that delivered everything *before* the refused event still has to keep the growing backoff
/// (the shape the engine already models for a 1236 it cannot serve). This is the hot loop the
/// hazard is: on `main` such a round counts as recovered, resets the ladder and pauses 200 ms — a
/// fresh connection and dump request on the source five times a second, forever, with `/livez`
/// green and nothing on `/status`.
#[test]
fn an_undecodable_round_keeps_the_backoff_instead_of_the_fast_path() {
    // The healthy round keeps what it has: delivered, no kept position, so the fast path and a
    // ladder that starts over.
    let ok = round_schedule(true, false, false, Duration::from_secs(5));
    assert!(ok.recovered, "a stream that moved recovered");
    assert_eq!(ok.pause, Duration::from_millis(200), "the fast path");
    assert_eq!(
        ok.backoff,
        Duration::from_millis(500),
        "the ladder starts over"
    );

    // Undecodable bytes, and the events before them were delivered: still not recovered.
    let bad = round_schedule(true, false, true, Duration::from_millis(500));
    assert!(!bad.recovered, "the same bytes are still there");
    assert_eq!(
        bad.pause,
        Duration::from_millis(500),
        "the ladder, not 200 ms"
    );
    assert_eq!(bad.backoff, Duration::from_secs(1));

    // The ladder itself, round after stuck round: 500 ms to the 5 s cap, and never the fast path.
    let mut backoff = Duration::from_millis(500);
    let mut pauses = Vec::new();
    for _ in 0..6 {
        let s = round_schedule(true, false, true, backoff);
        assert!(!s.recovered);
        assert_ne!(s.pause, Duration::from_millis(200), "the 200 ms hot loop");
        pauses.push(s.pause);
        backoff = s.backoff;
    }
    assert_eq!(
        pauses,
        vec![
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(5),
            Duration::from_secs(5),
        ],
        "the capped ladder the kept-position 1236 already walks"
    );

    // A kept 1236 (the position the server cannot serve) is the same shape — the new class must
    // not have disturbed it.
    let kept = round_schedule(true, true, false, Duration::from_secs(5));
    assert!(!kept.recovered);
    assert_eq!(kept.pause, Duration::from_secs(5));

    // Neither: the source went away mid-round. Undecodable alone decides nothing about a round
    // that delivered nothing: it is not recovered either way.
    let quiet = round_schedule(false, false, false, Duration::from_secs(1));
    assert!(!quiet.recovered);
    assert_eq!(quiet.pause, Duration::from_secs(1));
}

/// What the ladder decides is only *when* to connect again — never *from where*: the round
/// re-reads from the stream's boundary every time, so the refused event is read again by the next
/// connect and the one after it, until a build that decodes it ships. `RoundSchedule` carries no
/// position: it cannot move the reader past the event.
#[test]
fn the_backoff_never_skips_the_event() {
    let s = round_schedule(true, false, true, Duration::from_secs(5));
    assert!(
        !s.recovered,
        "a recovered round would start the next read from a fresh resume"
    );
    // The type itself is the proof: a schedule that could skip would carry a position.
    let fields = format!("{s:?}");
    assert!(
        !fields.contains("binlog.000001") && !fields.contains("pos:"),
        "the schedule carries a position, so it could move the reader: {fields}"
    );
}

// ---- the mapping arm: one line per stuck position ------------------------------------------

/// One WARN line per stuck position, and one line only: a round stuck at `binlog.000001:40` re-reads
/// that position every 500 ms, 1 s, 2 s … and must not fill the log with the same sentence, while a
/// reader that moves to a *second* stuck position says so (a silent second standstill reads as a
/// source that recovered). Every event is counted whatever the lines do.
#[test]
fn one_line_per_stuck_position_and_one_again_for_the_next() {
    let mut w = UndecodableWatch::default();
    let a = pos(40);
    let b = pos(88);

    assert!(w.note(&a), "the first round at a position is worth a line");
    assert!(
        !w.note(&a),
        "and the rounds that follow it at the same position are not"
    );
    assert!(!w.note(&a));
    assert!(w.note(&b), "a second stuck position is said too");
    assert!(!w.note(&b));
    assert_eq!(w.seen, 5, "every event counted, line or no line");

    // A stream that delivered again forgets the position it had named: standing still there later
    // is news again (the `stmt_dml_last` key-reset idiom, not a growing set of keys).
    w.recovered();
    assert!(
        w.note(&a),
        "stuck at {a} again after a recovery: a new line"
    );
    assert_eq!(w.seen, 6);
    assert_eq!(w.named.as_ref(), Some(&a));
}

/// The line an operator of a stuck reader gets: the position the reader keeps coming back to, how
/// long until it comes back, that the event is neither skipped nor lost, and the copy's own words
/// for what the event was (kind, table when the bytes name one, position, and what the decoder
/// said). The `stream_error` it sets is what `/status` shows meanwhile.
#[test]
fn the_mapping_line_names_the_position_and_quotes_the_decoder() {
    let e = tagged_rows(&pos(31));
    let at = pos(4); // the boundary the next connect reads from
    let ((), lines) = logged(|| {
        warn_undecodable_stream_error(&e, &at, Duration::from_secs(2));
    });
    assert_eq!(lines.len(), 1, "one line: {lines:?}");
    let v = &lines[0];
    assert_eq!(v["level"], "WARN");
    assert_eq!(
        v["fields"]["at"], "binlog.000001:4",
        "where the reader returns"
    );
    assert_eq!(v["fields"]["retry_in"], "2s", "the wait that is growing");
    let m = v["fields"]["message"].as_str().unwrap();
    for want in [
        "rows(UPDATE_ROWS_V1)(31)",
        "shop.orders",
        "binlog.000001:31",
        "no TABLE_MAP",
    ] {
        assert!(m.contains(want), "the line does not name {want}: {m}");
    }
    assert!(
        m.contains("not skipped"),
        "the line must say the event is kept: {m}"
    );

    // A mapping failure names no table (the bytes name none) and still quotes the decoder.
    let e = tagged_mapping(&pos(512));
    let ((), lines) = logged(|| {
        warn_undecodable_stream_error(&e, &pos(512), Duration::from_millis(500));
    });
    let m = lines[0]["fields"]["message"].as_str().unwrap();
    assert!(
        m.contains("QUERY(compressed)(165) at binlog.000001:512"),
        "{m}"
    );
    assert!(m.contains("binlog_commit_order_support"), "{m}");
}

/// The one ERROR line of a refused rows event: the owner's ruling the copy carries
/// (`source/mod.rs`: a decode failure that exits the process stays fatal) makes this line the whole
/// record the operator gets before the process dies, so it names kind, table, position and the
/// decoder's own words — as fields an alert can filter on, and verbatim in the message.
#[test]
fn the_rows_line_is_the_whole_record() {
    let e = tagged_rows(&pos(31));
    let ((), lines) = logged(|| error_undecodable_rows(&e));
    assert_eq!(lines.len(), 1, "one line: {lines:?}");
    let v = &lines[0];
    assert_eq!(v["level"], "ERROR");
    assert_eq!(v["fields"]["kind"], "rows(UPDATE_ROWS_V1)(31)");
    assert_eq!(v["fields"]["table"], "shop.orders");
    assert_eq!(v["fields"]["at"], "binlog.000001:31");
    let m = v["fields"]["message"].as_str().unwrap();
    assert_eq!(
        m,
        "cannot decode rows(UPDATE_ROWS_V1)(31) at binlog.000001:31: table shop.orders, \
         no TABLE_MAP for the id",
        "the copy's own line, word for word"
    );

    // An undecodable failure the mapper raised names no table: the field says so rather than
    // staying away, and the message is still the decoder's.
    let e = tagged_mapping(&pos(512));
    let ((), lines) = logged(|| error_undecodable_rows(&e));
    assert_eq!(lines[0]["fields"]["kind"], "QUERY(compressed)(165)");
    assert_eq!(lines[0]["fields"]["at"], "binlog.000001:512");
    assert!(
        lines[0]["fields"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("binlog_commit_order_support")),
        "{:?}",
        lines[0]["fields"]["message"]
    );
}

// ---- the rows arm: fatal at once ----------------------------------------------------------

/// A refused rows event on a watched table exits the process at once. The ten generic re-reads that
/// used to precede the same exit bought nothing: every one of them re-read the same boundary and
/// failed on the same bytes, for ~2 s of log noise before the container restarted. Anything else
/// keeps the generic path — a source that went away, a control connection that flapped — and the
/// ten attempts that suit it.
#[test]
fn a_refused_rows_event_gives_up_and_a_network_fault_retries() {
    assert!(matches!(
        on_item_failure(&tagged_rows(&pos(31))),
        ItemFailure::GiveUp
    ));
    assert!(matches!(
        on_item_failure(&tagged_mapping(&pos(512))),
        ItemFailure::GiveUp
    ));
    // A dump packet that is not a readable event at all is the decoder's too (the copy's own rule).
    let packet = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "not a binlog event",
    ));
    assert!(matches!(on_item_failure(&packet), ItemFailure::GiveUp));

    // Transient stays transient: the class the copy deliberately leaves to `is_transient`.
    assert!(matches!(on_item_failure(&truncated()), ItemFailure::Retry));
    for kind in [
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::TimedOut,
    ] {
        let e = anyhow::Error::new(mysql_async::Error::Io(mysql_async::IoError::Io(
            std::io::Error::new(kind, "x"),
        )));
        assert!(
            matches!(on_item_failure(&e), ItemFailure::Retry),
            "{kind:?}"
        );
    }
    // The engine's own failures (a control statement that failed, a rule that refused) are not the
    // decoder's either.
    assert!(matches!(
        on_item_failure(&anyhow!("a control statement failed")),
        ItemFailure::Retry
    ));
    let gone = anyhow::Error::new(source::MissingBinlog {
        from: pos(4),
        message: "purged".into(),
    });
    assert!(matches!(on_item_failure(&gone), ItemFailure::Retry));
}

// ---- visibility ---------------------------------------------------------------------------

/// The counter and the current stream error are on the `/livez` status JSON beside the statement-
/// format DML counter: a reader stuck on undecodable bytes today shows a healthy stream at a
/// position that never moves (`/livez` beats on, `stream_error` is never set), which reads as a
/// network fault nobody can see.
#[test]
fn the_status_shows_the_counter_and_the_stuck_position() {
    let stuck = "cannot decode rows(UPDATE_ROWS_V1)(31) at binlog.000001:31: table shop.orders, \
                 no TABLE_MAP for the id";
    let v = status_json(
        "acme",
        &pos(4),
        0,
        Some(stuck),
        3,
        2,
        Map::new(),
        Vec::new(),
    );
    assert_eq!(
        v["undecodable"], 2,
        "the events this build could not decode"
    );
    assert_eq!(v["stream_error"], stuck, "what the reader is stuck on");
    assert_eq!(
        v["statement_events_ignored"], 3,
        "its neighbour is unchanged"
    );
    assert_eq!(v["instance"], "acme");
    assert_eq!(v["position"], "binlog.000001:4");

    // Nothing stuck: the keys are there and quiet, so a dashboard's query does not change shape.
    let quiet = status_json("acme", &pos(4), 0, None, 0, 0, Map::new(), Vec::new());
    assert!(quiet["undecodable"].is_number(), "0, not absent");
    assert!(quiet["stream_error"].is_null());
}

/// The counter is memory only (CLAUDE.md: counters appear on `/status`, never in the tool's
/// tables), and `UndecodableWatch` is the only place it lives.
#[test]
fn the_counter_lives_in_memory_only() {
    let mut w = UndecodableWatch::default();
    assert_eq!(w.seen, 0);
    w.count();
    w.note(&pos(4));
    w.recovered();
    assert_eq!(w.seen, 2, "a recovered stream does not forget what it saw");
    assert_eq!(w.named, None, "but it forgets which position it had named");
    // `Default` is all a boot needs: no load from the status table, no resume change.
    assert_eq!(UndecodableWatch::default().seen, 0);
}

/// The far end of the `stream_error` contract ("shown on `/status` until the stream recovers"): the
/// sequence a reader that gets stuck, is fixed, and gets stuck again goes through — the pieces the
/// stream loop runs in order ([`Engine::on_undecodable_stream_error`], [`round_schedule`],
/// [`Engine::on_stream_recovered`], [`UndecodableWatch::note`]). A stuck round climbs the ladder and
/// puts the decoder's words on `/status`; a round that delivers clears the error, drops the ladder
/// to its first step and lets the reader earn the fast path again; and a refused event at the *same*
/// position afterwards is a standstill again — said again, because a silent one would read as a
/// stream that had recovered.
#[test]
fn a_reader_that_gets_unstuck_is_said_to_be_stuck_again() {
    let at = pos(40); // where the reader keeps coming back to
    let e = tagged_rows(&pos(31));
    let mut w = UndecodableWatch::default();
    let mut backoff = STREAM_BACKOFF_FIRST;
    let mut stream_error: Option<String> = None;
    let mut lines = 0;

    // Three stuck rounds at one position: the ladder climbs, the line is said once, `/status` tells.
    for _ in 0..3 {
        // What Engine::on_undecodable_stream_error does with a refused event at this position.
        if w.note(&at) {
            lines += 1;
        }
        stream_error = Some(format!("{e:#} at {at}"));
        let s = round_schedule(true, false, true, backoff);
        assert!(!s.recovered, "the bytes are still there");
        assert_ne!(s.pause, STREAM_FAST_PATH, "no fast path while stuck");
        backoff = s.backoff;
    }
    assert_eq!(lines, 1, "one line for one stuck position");
    assert_eq!(w.seen, 3, "and every round counted");
    assert_eq!(
        backoff,
        Duration::from_secs(4),
        "500 ms, 1 s, 2 s: the ladder climbed"
    );
    let shown = status_json(
        "acme",
        &at,
        0,
        stream_error.as_deref(),
        0,
        w.seen,
        Map::new(),
        Vec::new(),
    );
    assert!(
        shown["stream_error"]
            .as_str()
            .is_some_and(|s| s.contains("no TABLE_MAP") && s.contains("binlog.000001:40")),
        "/status does not tell the operator what the reader is stuck on: {shown}"
    );
    assert_eq!(shown["undecodable"], 3);

    // A round that delivers — the fixed build shipped, or the source moved: recovered, the fast
    // path, the ladder at its first step. Engine::on_stream_recovered is these two lines.
    let s = round_schedule(true, false, false, backoff);
    assert!(s.recovered, "the stream is moving again");
    assert_eq!(s.pause, STREAM_FAST_PATH, "and earns the fast reconnect");
    assert_eq!(s.backoff, STREAM_BACKOFF_FIRST, "the ladder starts over");
    stream_error = None;
    w.recovered();
    let shown = status_json(
        "acme",
        &at,
        0,
        stream_error.as_deref(),
        0,
        w.seen,
        Map::new(),
        Vec::new(),
    );
    assert!(
        shown["stream_error"].is_null(),
        "the error is no longer current: {shown}"
    );
    assert_eq!(
        shown["undecodable"], 3,
        "the count is history, not state to forget"
    );

    // Refused at the same position afterwards: a standstill again, so it is said again, and the
    // ladder starts from its first step rather than continuing the old one.
    assert!(w.note(&at), "the position is named again after a recovery");
    let s = round_schedule(true, false, true, STREAM_BACKOFF_FIRST);
    assert!(!s.recovered);
    assert_eq!(s.pause, STREAM_BACKOFF_FIRST);
    assert_eq!(w.seen, 4);
}
