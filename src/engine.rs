//! The engine: one task reads the binlog, marks keys per rule in memory, fires due rules and
//! advances each rule's position once its actions met `expect`.
//!
//! * **Marks** of a transaction are collected while its rows arrive and applied at its commit
//!   (XA: at `XA COMMIT`), so nothing uncommitted is ever an input.
//! * **One lane per rule:** a flush or a reset batch at a time, so what is read last is sent
//!   last. Rules (and targets) do not wait for each other.
//! * **Flush:** when no key was marked for `debounce_ms`, or the oldest key waited
//!   `max_delay_ms`. The buffer is taken whole; keys marked meanwhile fire again afterwards.
//! * **Positions:** a rule's position is the start of its oldest unconfirmed mark (or where the
//!   stream is). The stream resumes from the lowest one; rules ahead skip what they confirmed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use indexmap::{IndexMap, IndexSet};
use mysql_async::binlog::events::Event;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::action::{self, Env, FenceSpec, Fired, KeyQuery};
use crate::analyze;
use crate::binlog::{self, BinlogReader};
use crate::config::Config;
use crate::dedup::{buffer, key};
use crate::fence;
use crate::filters::{self, Role};
use crate::health::Health;
use crate::keyspace::{op_text, text_set, IdSets, KeySpaces};
use crate::lock;
use crate::metrics::{FlushOutcome, Metrics, PairHandle, RuleHandle};
use crate::model::{self, On, RuleDef, StepKind, TargetDef, TriggerDef};
use crate::rows::{table_map_names, Catalog, Layout, LayoutMismatch};
use crate::rule::{self, KeyTemplate, Rule};
use crate::source::{self, Flavor, Item, Position, RowsKind};
use crate::status::{self, Saved, INSTANCE_ROW};
use crate::target::Target;
use crate::template;

const BINLOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// An XA transaction prepared longer ago stops holding the saved position back.
const XA_PIN_MAX: Duration = Duration::from_secs(3600);
/// Tables created `LIKE` another that are remembered at most (online schema changes in flight).
const MAX_SHADOWS: usize = 1000;
/// Tables the statement-format DML warning ([`Engine::note_statement_dml`]) rate limits at
/// most. Its key is a name the source states, so unlike lazyview's map (bounded by the tables
/// whose sessions twist `binlog_format`) this one needs a hard ceiling: a session can name a
/// new table in every statement, and the engine's memory must not follow it.
const MAX_STMT_DML_TABLES: usize = 1000;
/// Minimum time between the WARN lines of one table in [`Engine::note_statement_dml`]: a bulk
/// job in statement format is one event per statement, so one line per table per interval
/// carries the count of the rest.
const STMT_DML_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// Status rows are rewritten at least this often (`updated_at`).
const STATUS_HEARTBEAT: Duration = Duration::from_secs(30);
const RETRY_FIRST: Duration = Duration::from_millis(500);
/// The stream loop's reconnect ladder: the pause after a round that did not recover, doubled per
/// round up to the cap. A round that ends on a position the server cannot serve (a 1236 that keeps
/// it) or on bytes this build cannot decode walks it until the position moves, while a round that
/// delivered reconnects after [`STREAM_FAST_PATH`].
const STREAM_BACKOFF_FIRST: Duration = Duration::from_millis(500);
const STREAM_BACKOFF_MAX: Duration = Duration::from_secs(5);
const STREAM_FAST_PATH: Duration = Duration::from_millis(200);

#[derive(Debug, Default)]
struct Backoff {
    failures: u32,
    until: Option<Instant>,
}

impl Backoff {
    fn fail(&mut self, max: Duration) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let d = RETRY_FIRST
            .saturating_mul(1u32 << (self.failures - 1).min(16))
            .min(max);
        self.until = Some(Instant::now() + d);
        d
    }

    fn ok(&mut self) {
        *self = Backoff::default();
    }

    fn blocked(&self, now: Instant) -> Option<Instant> {
        self.until.filter(|u| *u > now)
    }
}

/// What the reader knows about the **source's** clock, ported from lazyview's `SourceClock`
/// (review O-01, ruling 1: "N seconds behind the source" has to hold during a backlog, not only
/// while the source is idle). Both timestamps are the source's own clock — a common-header
/// timestamp and the source's `UNIX_TIMESTAMP()` — so no host clock and no skew enters the
/// number, and an idle source gives exactly 0. A source that never goes idle gives it too, via the
/// second head proof ([`read_proves_head`]): what matters is that the reader is keeping up, not
/// that the server fell quiet.
///
/// Cost (owner rule): integer stores only, and only on the rows events of watched tables in
/// [`Engine::on_item`] (the header ts is a field the reader has already parsed) and on
/// heartbeats; no allocation; `offset` costs one statement per (re)connect and per
/// [`CLOCK_REFRESH`] tick, never per event; the head proofs read fields the engine already keeps.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SourceClock {
    /// Header ts of the newest change to a watched table this connection has seen
    /// (`BinlogReader::event_ts` at the routed [`Item::Rows`] in [`Engine::on_item`]); 0: nothing
    /// read yet. A rule that has applied this much is caught up with the source. The engine's
    /// own pen (the config and status tables) and the shape-only events are not a rule's
    /// business and do not move it.
    fed_ts: u64,
    /// Header ts of the *first* change to a watched table this connection has read: the oldest
    /// change the rules may still be missing. A rule that has applied nothing since the process
    /// started (`RuleState::applied_ts` 0) is measured against it, so an engine restarted over a
    /// gap reports the length of the gap rather than the age of the epoch. 0: nothing read.
    gap_ts: u64,
    /// The stream is at its head by the reference's proof: the last stamp was a heartbeat whose
    /// payload position was the position the engine has applied to ([`heartbeat_proves_head`]). A
    /// backlog can still sit in the read buffer while the server heartbeats, so the position, not
    /// the event type, is what proves the stream caught up with the source's binlog. The other
    /// proof, for a source that keeps writing, is [`read_proves_head`]; [`SourceClock::lag_with`]
    /// takes either.
    at_head: bool,
    /// When the server last said its dump was empty (a heartbeat arrived), on the source's clock —
    /// whether or not that heartbeat proved the head. A rule that has read nothing this
    /// connection ages from here, not from the epoch.
    head_ts: u64,
    /// The reader is connected and able to deliver: false while reconnecting or between rounds —
    /// a source that keeps writing while nothing is read is a growing lag, not a growing head.
    reading: bool,
    /// Rows the rules have not been handed yet: lazywf has no drain queue (rules are marked at
    /// COMMIT), so this is the engine's `in_txn` at publish time — a half-read transaction in
    /// hand is read, so the source is ahead of the rules, and not yet counted as fed.
    pending_read: bool,
    /// `source UNIX_TIMESTAMP() − local unix now`, asked at connect and at the housekeeping tick:
    /// [`SourceClock::now`] is the source's clock between two of those questions.
    offset: i64,
}

impl SourceClock {
    /// The source's clock, right now: the local clock shifted by the measured offset (so a source
    /// whose clock differs from the host's by minutes is still measured against itself).
    fn now(&self) -> u64 {
        (crate::time::unix_now() as i64 + self.offset).max(0) as u64
    }

    /// The newest change the rule has applied, on the source's clock: its own stamp; else the
    /// oldest change this connection has read (see [`SourceClock::gap_ts`]); else the last
    /// heartbeat — a rule that has read nothing this connection ages from when the server last
    /// said its dump was empty, never from the epoch.
    fn applied(&self, applied_ts: u64) -> u64 {
        if applied_ts != 0 {
            applied_ts
        } else if self.gap_ts != 0 {
            self.gap_ts
        } else {
            self.head_ts
        }
    }

    /// Seconds `applied_ts` is behind the source, saturating, with the head proven by the caller:
    ///
    /// * `0` while the rule is [`rule_settled`], the stream is at its head, the reader is reading,
    ///   nothing stands unread, and the rule has applied every change read — an idle source,
    ///   however long idle, is 0 seconds behind;
    /// * otherwise the age of the newest applied change on the source's clock, so a backlog after
    ///   an outage, a reconnecting stream and a rule whose sink keeps failing all show a number
    ///   that grows with the time the source has been ahead.
    ///
    /// Two deviations from the reference's `lag(applied_ts)`, both ours deliberately:
    ///
    /// * `settled` — a rule with keys still pending and no applied stamp (`applied_ts` 0) falls
    ///   back to `gap_ts`, which for a single-transaction backlog equals `fed_ts`; without the
    ///   flag such a rule would read "caught up" while its keys still wait to fire;
    /// * `head` is passed in rather than read from [`Self::at_head`] alone, because the heartbeat
    ///   is not the only proof available: a server sends a heartbeat only once its dump has been
    ///   empty for a whole period, so on a source that never goes quiet for one, `at_head` would
    ///   never be set and a rule that has read and applied everything would age forever — the
    ///   false alarm this field exists to rule out. The engine passes the heartbeat proof OR
    ///   [`read_proves_head`].
    fn lag_with(&self, applied_ts: u64, settled: bool, head: bool) -> u64 {
        let applied = self.applied(applied_ts);
        if settled && head && self.reading && !self.pending_read && applied >= self.fed_ts {
            0
        } else {
            self.now().saturating_sub(applied)
        }
    }

    /// A rows event of a watched table was read with header ts `ts` (0: unknown, ignored): the
    /// gap freezes at the first row of a run, the head follows the last one, and any head proof
    /// is stale (a row we hold came from below the position a heartbeat proved).
    fn note_fed(&mut self, ts: u64) {
        if ts != 0 {
            if self.gap_ts == 0 {
                self.gap_ts = ts;
            }
            self.fed_ts = ts;
            self.at_head = false;
        }
    }
}

/// The head proof of a heartbeat (the `Item::Heartbeat` arm of [`Engine::on_item`], split out so
/// the unit test can drive the decision without a live engine — the [`on_item_failure`] idiom).
///
/// `head` is the position the payload carried (the reference's semantics; see
/// [`BinlogReader::heartbeat_head`] — on servers whose payload is not a plain position, like the
/// MySQL 8.4 and MariaDB dumps whose first eight bytes are the file NAME, it is unusable, which
/// is what the second clause is for); `applied` is the position everything read is committed to,
/// `reader_pos` where the reader stands. Outside a transaction: the payload equality proves the
/// head when the payload is a plain position naming exactly where we applied, in the file the
/// reader stands in; the position equality does the same when the payload cannot be matched (the
/// reader standing at the applied position says everything this connection has read is committed
/// and nothing stands unread, so no rows sit between the applied position and the head the
/// heartbeat just proved). Follows the reference verbatim: a still-unread transit between the
/// two equal positions can prove the head a moment early, and it is bounded and self-correcting
/// — the arriving rows' feed ([`SourceClock::note_fed`]) de-proves it at once.
fn heartbeat_proves_head(
    in_txn: bool,
    applied: &Position,
    reader_pos: &Position,
    head: u64,
) -> bool {
    !in_txn
        && ((head != 0 && applied.pos == head && applied.file == reader_pos.file)
            || applied == reader_pos)
}

/// The second head proof, for a source that keeps writing: the newest change the reader handed us
/// — of ANY table, watched or not, [`Engine::last_event_ts`] — was logged no more than `within`
/// ago on the source's own clock. Such a source never lets its dump go quiet, so it never
/// heartbeats ([`heartbeat_proves_head`] cannot fire), and yet the reader is demonstrably keeping
/// up with it: the source is writing and every change it wrote has reached us. `within` is the
/// heartbeat period the dump itself is configured with (`cfg.heartbeat`, the value handed to
/// [`BinlogReader::connect`]): the interval within which an idle dump would have heartbeated
/// instead of sending this row.
///
/// What it cannot fake: a reader stalled behind a backlog hands old stamps, so the age is large
/// and the proof denies; a source that has stopped writing hands nothing at all (`last_read_ts` 0)
/// and the heartbeat path decides; a transaction logged at its statement start (a long `BEGIN` …
/// `COMMIT`) reads old and denies too. Every way this proof can be wrong denies the head rather
/// than inventing it, and the one over-proof it shares with the heartbeat — a change written just
/// after the newest one read, still in transit — is bounded by `within` and de-proved the moment
/// the change arrives ([`SourceClock::note_fed`]).
///
/// The third deviation from the reference, after `lag`'s `settled` flag and `pending_read`.
fn read_proves_head(last_read_ts: u64, src_now: u64, within: Duration) -> bool {
    last_read_ts != 0 && src_now.saturating_sub(last_read_ts) <= within.as_secs()
}

/// Whether a rule has settled: it holds nothing that could still be applied later — no buffered
/// keys, no lane (a flush or reset run of its own), no prep job, no pending reset run, no sink
/// or trigger error (the retry timer holds its keys), no freeze, its own backoff is open, and
/// its target is not on a backoff of its own. The same pieces [`Engine::schedule`] reads before
/// it fires a flush; split out so the unit test can drive the decision without a live engine.
fn rule_settled(st: &RuleState, target_blocked: bool, now: Instant) -> bool {
    st.buffer.is_empty()
        && st.lane.is_none()
        && !st.prep
        && st.reset.is_none()
        && st.rule.error.is_none()
        && st.rule.trigger_error.is_none()
        && st.frozen_at.is_none()
        && st.backoff.blocked(now).is_none()
        && !target_blocked
}

/// How often [`Engine::refresh_source_clock`] asks the source its time (lazyview's housekeeping
/// cadence). The throttle timestamp moves on every attempt, failures included, so a source that
/// will not answer costs one statement per tick and not one per loop turn.
const CLOCK_REFRESH: Duration = Duration::from_secs(30);

/// Keys one transaction marked for one rule.
#[derive(Debug, Default)]
struct Marks {
    ids: IdSets,
    queries: IndexMap<String, KeyQuery>,
    rows: u64,
}

/// Marked keys waiting for their flush: the dedup core's buffer of ids per key space (a key
/// that is an integer, or a prefix and one, lives as a number in the space of its prefix;
/// anything else as a tuple in the text space, see [`crate::keyspace`]), with the host's
/// clocks around it.
///
/// The core owns the position floor: every mark carries the start of the transaction that
/// marked it, the first mark of a pending cycle pins it, a taken generation keeps pinning
/// until its last chunk completed, and retried ids come back carrying it — a rule's position
/// never runs ahead of what is unflushed. `since_ts`, `first` and `last` are the debounce and
/// max_delay clocks (the core's counterpart of those is the plan's 6G, not vendored yet).
#[derive(Debug)]
struct Buffer {
    core: buffer::Buffer<Position, String>,
    keys: KeySpaces,
    /// `keys_sql` to run at flush time, deduplicated by parameters. The core holds a pending
    /// query as `Item::User` under its id (which pins the position floor exactly like a
    /// marked key does); this map holds the payload itself, 1:1 with the pending items.
    queries: IndexMap<String, KeyQuery>,
    /// Commit time (unix seconds) of the oldest transaction that marked something here.
    since_ts: u32,
    first: Option<Instant>,
    last: Option<Instant>,
}

impl Buffer {
    fn is_empty(&self) -> bool {
        self.core.pending_len() == 0
    }

    /// Keys waiting to fire: the pending items are the queries, and payload and items are
    /// 1:1.
    fn pending_keys(&self) -> usize {
        self.core.pending_len() - self.queries.len()
    }

    /// Remembers the oldest commit time (`ts` 0: unknown, kept out of the minimum).
    fn hold_ts(&mut self, ts: u32) {
        if self.since_ts == 0 || (ts != 0 && ts < self.since_ts) {
            self.since_ts = ts;
        }
    }

    fn due(&self, debounce: Duration, max_delay: Duration) -> Option<Instant> {
        if self.is_empty() {
            return None;
        }
        let (f, l) = (self.first?, self.last?);
        Some((l + debounce).min(f + max_delay))
    }

    /// Where key `k` lives, in this rule's spaces.
    fn locate(&mut self, k: &str) -> (key::SpaceId, key::Key) {
        self.keys.locate(k)
    }

    /// Marks one key; `origin`: the start of the transaction that marked it.
    fn mark(&mut self, s: key::SpaceId, k: key::Key, origin: &Position) {
        self.core.mark(s, k, origin);
    }

    /// A pending `keys_sql` query: the payload here, an item in the core (which holds the
    /// position floor like a marked key). The id is computed once at the insert site and
    /// passed in (it keys this map and names the item).
    fn add_query(&mut self, id: String, q: KeyQuery, origin: &Position) {
        self.queries.insert(id.clone(), q);
        self.core.item(key::Item::User(id), origin);
    }

    /// Puts back the queries of a failed flush (the items are re-added separately: `done`
    /// carries them when the generation is still there), under the ids they were taken with.
    fn put_back_queries(&mut self, queries: Vec<(String, KeyQuery)>) {
        for (id, q) in queries {
            self.queries.entry(id).or_insert(q);
        }
    }

    /// The payloads of the queries one taken generation carried, in item order, each with
    /// the id it was held under (the item's own id, moved out of `items`: never recomputed).
    fn take_query_payloads(&mut self, items: Vec<key::Item<String>>) -> Vec<(String, KeyQuery)> {
        let mut out = Vec::with_capacity(items.len());
        for i in items {
            if let key::Item::User(id) = i {
                if let Some(q) = self.queries.shift_remove(&id) {
                    out.push((id, q));
                }
            }
        }
        out
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            // `false`: the facts flag stays off (lazyview decision 3), which is what the
            // vendored buffer did before the flag existed — nothing per-id is tracked.
            core: buffer::Buffer::new(KeySpaces::SPACES, false),
            keys: KeySpaces::default(),
            queries: IndexMap::new(),
            since_ts: 0,
            first: None,
            last: None,
        }
    }
}

enum Lane {
    Flush {
        since_ts: u32,
        first: Option<Instant>,
        /// Keys of this flush that have strikes.
        struck: Vec<String>,
        /// The pending queries taken for this flush, each with its id (the dedup key it was
        /// inserted under, so a failed flush puts it back and completes the generation
        /// without recomputing ids). Shared with the job, which only reads them: a flush that
        /// goes through never copies a parameter, and the copy `put_back_queries` needs
        /// happens on the one path that takes the queries back.
        queries: Arc<Vec<(String, KeyQuery)>>,
    },
    Reset,
}

/// A key the target refused this many times (while others went through) is dropped.
const STRIKES: u32 = 5;

/// Event target of a dropped key: `LAZYWF_LOG_FORMAT=json` lines with this `target` are the
/// undelivered keys (one event per key, fields as in [`log_undelivered`]).
const UNDELIVERED: &str = "lazywf::undelivered";

/// Logs one key given up after [`STRIKES`] refusals. Fields: `rule`, `target` (the target's
/// name), `key`, `reason` (the last refusal), `strikes`. `op` is not recorded: a flush merges
/// the keys of all triggers, so the insert/update/delete of a key is not known here.
fn log_undelivered(rule: &str, target: &str, key: &str, reason: &str, strikes: u32) {
    warn!(
        target: UNDELIVERED,
        rule = %rule,
        target = %target,
        key = %key,
        reason = %reason,
        strikes,
        "undelivered key dropped"
    );
}

/// Splits refused keys into those to try again and those dropped (strikes in `strikes`). A
/// refusal counts against a key when other keys of the same action went through (or it was
/// singled out before): when everything is refused, the template or the target is at fault,
/// not a key.
fn judge_strikes(
    strikes: &mut HashMap<String, u32>,
    rejected: Vec<(String, String)>,
    others_ok: bool,
) -> (Vec<String>, Vec<(String, String)>) {
    let mut retry = Vec::new();
    let mut dropped = Vec::new();
    for (k, why) in rejected {
        if !others_ok && !strikes.contains_key(&k) {
            retry.push(k);
            continue;
        }
        let n = strikes.entry(k.clone()).or_insert(0);
        *n += 1;
        if *n >= STRIKES {
            strikes.remove(&k);
            dropped.push((k, why));
        } else {
            retry.push(k);
        }
    }
    (retry, dropped)
}

enum Phase {
    Start,
    /// A job of this step is running.
    Waiting,
    /// Snapshot keys still to send; `batch` is in flight (kept until confirmed).
    Batches {
        keys: buffer::IdSet,
        total: u64,
        done: u64,
        batch: Option<Vec<String>>,
    },
    Strays {
        per_ep: Vec<buffer::IdSet>,
        ep: usize,
        done: u64,
        batch: Option<Vec<String>>,
    },
}

struct ResetRun {
    id: u64,
    reason: String,
    requested: bool,
    fingerprint: String,
    step: usize,
    phase: Phase,
    /// Keys marked since the current stray step began (never deleted as strays).
    touched: Option<buffer::IdSet>,
    backoff: Backoff,
}

/// In-memory counters, shown on `/status` only.
#[derive(Debug, Default)]
struct Counters {
    events: u64,
    actions_ok: u64,
    actions_failed: u64,
    select_ms: u64,
}

struct RuleState {
    rule: Arc<Rule>,
    incarnation: u64,
    buffer: Buffer,
    lane: Option<Lane>,
    /// A read-only reset job (snapshot keys, stray listing) is running.
    prep: bool,
    /// Confirmed up to here in an earlier run: transactions ending at or before it are skipped.
    skip_until: Option<Position>,
    /// Triggers broken since this position: the rule cannot mark keys.
    frozen_at: Option<Position>,
    reset: Option<ResetRun>,
    reset_state: String,
    last_error: Option<(String, u64)>,
    backoff: Backoff,
    /// `reset_requested` must be cleared in the rule table.
    pending_clear: bool,
    /// A write of this rule's own row that still asked for a reset arrived while
    /// `pending_clear` stood: a request raised after the run that the clear serves finished,
    /// which that clear must not erase (see [`Engine::housekeeping`]). In memory only — the row
    /// and the status table hold no version to compare, and adding one is barred.
    reset_request_seen: bool,
    counters: Counters,
    /// Everything this rule has applied, on the **source's** clock (a header ts), stamped while
    /// the rule is [`rule_settled`] (see [`SourceClock::lag`]); 0: nothing since this process
    /// started — the clock then measures against the connection's `gap_ts`.
    applied_ts: u64,
    /// Refusals per key (see [`STRIKES`]).
    strikes: HashMap<String, u32>,
    written: Option<(Position, Instant)>,
    dirty: bool,
    /// The rule's resolved metric handles (`None` when metrics are off). Resolved once at rule
    /// load from the loaded config; the flush/drop sites only touch these atomics.
    mrule: Option<Arc<RuleHandle>>,
    mpair: Option<Arc<PairHandle>>,
}

impl RuleState {
    /// Splits refused keys into those to try again and those dropped. A refusal counts
    /// against a key when other keys of the same action went through (or it was singled out
    /// before): when everything is refused, the template or the target is at fault, not a key.
    fn judge(
        &mut self,
        rejected: Vec<(String, String)>,
        others_ok: bool,
    ) -> (Vec<String>, Vec<(String, String)>) {
        judge_strikes(&mut self.strikes, rejected, others_ok)
    }

    fn report_dropped(
        &mut self,
        rule: &str,
        target: &str,
        dropped: &[(String, String)],
        now_ms: u64,
    ) {
        let mpair = self.mpair.clone();
        for (k, why) in dropped {
            let k: String = k.chars().take(200).collect();
            log_undelivered(rule, target, &k, why, STRIKES);
            // The one place a key is given up after the fifth strike: count it here, next to the
            // `lazywf::undelivered` WARN. The label pair is the pre-resolved handle, no name is
            // named to the registry.
            if let Some(p) = &mpair {
                p.dropped(1);
            }
            self.last_error = Some((
                format!("key `{k}` refused {STRIKES} times, dropped: {why}"),
                now_ms,
            ));
        }
    }
}

enum Outcome {
    Flush {
        fired: Fired,
        keep_queries: bool,
        resolved: Vec<String>,
    },
    Reset {
        run: u64,
        fired: Fired,
    },
    Snapshot {
        run: u64,
        keys: Result<buffer::IdSet, String>,
    },
    Strays {
        run: u64,
        per_ep: Result<Vec<buffer::IdSet>, String>,
    },
}

struct Done {
    rule: String,
    incarnation: u64,
    outcome: Outcome,
}

/// Where the rows of a table map go in this transaction.
#[derive(Clone)]
enum Route {
    Ignore,
    /// A table of the tool's own config; `rules` says it is `lazy_wire_fire_rule` itself,
    /// whose row writes the engine reads as a possible new `reset_requested` (see
    /// [`Engine::housekeeping`]'s clear).
    Config {
        rules: bool,
    },
    Watched(Arc<Watched>),
}

struct Watched {
    layout: Arc<Layout>,
    table: String,
    triggers: Vec<(String, Arc<Rule>, usize)>,
    /// Some trigger of the table builds its keys with a Full template, so a row's image has to
    /// be built as an object (computed once here: a row of a table with Simple keys only never
    /// pays for it).
    any_full: bool,
    /// Some trigger of the table fires on this row kind (the union of the triggers' `on` flags,
    /// computed once here beside `any_full`, see [`Engine::watched_kinds`]): a rows event of a
    /// kind none of them watches marks nothing, so it is never even decoded (see
    /// [`Engine::on_rows`]).
    any_on: On,
}

impl Watched {
    /// Does any trigger of this table fire on `kind`? If none does, the rows events of that
    /// kind mark nothing (the `on` check in [`Engine::row_keys`] drops every row), so they are
    /// skipped undecoded (see [`Engine::on_rows`]).
    fn watches(&self, kind: RowsKind) -> bool {
        match kind {
            RowsKind::Insert => self.any_on.insert,
            RowsKind::Update => self.any_on.update,
            RowsKind::Delete => self.any_on.delete,
        }
    }
}

/// A built route and the catalog generation its layout came from (see [`RouteCache`]).
struct WatchedEntry {
    gen: u64,
    w: Arc<Watched>,
}

/// The per-table cache of built routes. A TableMap event for an already-routed table is
/// answered from here (see [`Engine::route`]) instead of rebuilding the trigger list, the
/// `format!`ed name and the flags from the rules on every event.
///
/// A hit requires three things: the table's entry exists; it was built at the catalog's
/// current generation (nothing has been invalidated since — the counter is bumped by every
/// mutation of either catalog map); and its `Layout` is still the one the catalog hands out
/// for exactly this event — the plain `information_schema` layout for an event that names no
/// columns, a layout derived from the event's own names for a full-metadata one. None of the
/// three allocates except the two small `String`s of the folded key when `lower_names` is on,
/// and even those are few beside the list and `format!`ed name this cache replaces. That
/// identity is what keeps the DDL-survival rule: a `Watched` built
/// from the `information_schema` layout can never answer a FULL-named request, no FULL-derived
/// layout ever answers a plain one (the catalog's two caches never share an `Arc`, and a FULL
/// entry's own names are part of its key), and a layout re-read under a DDL is a new `Arc`,
/// which fails the check even before the generation counter is consulted.
#[derive(Default)]
struct RouteCache {
    entries: HashMap<(String, String), WatchedEntry>,
}

impl RouteCache {
    /// Records the route `route` built for a table at the catalog's current generation.
    fn insert(&mut self, key: (String, String), w: Arc<Watched>, gen: u64) {
        self.entries.insert(key, WatchedEntry { gen, w });
    }

    /// Drops everything: the rules were rebuilt, or a reset started — the ways the trigger
    /// list a cached entry holds can go stale while the layout stays (see
    /// [`Engine::rebuild_routes`] and [`Engine::start_reset`]).
    fn clear(&mut self) {
        self.entries.clear();
    }

    /// Drops one table's entry, or a whole schema's (the `*` form `forget_columns` takes).
    fn forget(&mut self, key: &(String, String)) {
        if key.1 == "*" {
            self.entries.retain(|k, _| k.0 != key.0);
        } else {
            self.entries.remove(key);
        }
    }

    /// The route for `key`, if the cached one is still the answer for an event that carries
    /// `names` as its own column names (`None`: the event names none, so only the plain
    /// `information_schema` layout answers it). The hit check is
    /// `Arc::ptr_eq(cached.layout, the same Arc<Layout> the catalog currently returns for
    /// exactly this event's names)` — [`Catalog::current`] is that answer — and a
    /// generation that moved since the entry was built is a cheap pre-check that something
    /// was invalidated. Not just "the counter matches": the identity check is what decides.
    /// With folded names (`lower_names`) the entry is keyed by the folded pair, so the
    /// lookup folds first — as `norm` does on the path that builds the entry — while the
    /// catalog is keyed by the event's raw names, so `current` is asked with those.
    fn probe(
        &self,
        catalog: &Catalog,
        lower_names: bool,
        key: &(String, String),
        names: Option<&[String]>,
    ) -> Option<Route> {
        let folded;
        let ekey = if lower_names {
            folded = (key.0.to_lowercase(), key.1.to_lowercase());
            &folded
        } else {
            key
        };
        let entry = self.entries.get(ekey)?;
        if entry.gen != catalog.generation() {
            return None;
        }
        let current = catalog.current(key, names)?;
        if !Arc::ptr_eq(&entry.w.layout, current) {
            return None;
        }
        Some(Route::Watched(entry.w.clone()))
    }
}

/// The text a row's Simple keys are built in, reused from row to row: building one key is a
/// `clear` and some pushes, never an allocation.
#[derive(Default)]
struct KeyScratch {
    key: String,
}

impl KeyScratch {
    /// Empties the scratch (its capacity stays) and hands it out to build one key line in.
    fn begin(&mut self) -> &mut String {
        self.key.clear();
        &mut self.key
    }
}

/// What building one row's keys touches in the engine, borrowed as pieces: `marks` the rules
/// this transaction has marked so far, `rules` their buffers, `scratch` the string a key line is
/// built in, `render_warnings` the Full keys that did not render, `invalid` the keys holding
/// control characters and `invalid_keys` how many were ever reported (the invalid ones are
/// reported once the rows are done), and `lossy` the keys whose bytes no charset could be read
/// as — collected here and reported by [`Engine::on_rows`] once per trigger through
/// `lossy_refusals` (the decode-point guard, see [`Layout::key_is_lossy`]).
struct RowCtx<'a> {
    marks: &'a mut HashMap<String, Marks>,
    rules: &'a mut BTreeMap<String, RuleState>,
    scratch: &'a mut KeyScratch,
    render_warnings: &'a mut u64,
    invalid_keys: &'a mut u64,
    invalid: &'a mut Vec<(String, u64, String)>,
    lossy: &'a mut Vec<(String, u64, String)>,
    lossy_refusals: &'a mut HashSet<(String, u64)>,
}

struct XaTxn {
    start: Position,
    marks: HashMap<String, Marks>,
    prepared_at: Instant,
}

/// Where the stream was before a replay for re-enabled rules moved it back.
struct Rewound {
    boundary: Position,
    settled: Position,
    rules: Vec<String>,
}

pub struct Engine {
    cfg: Config,
    lock: String,
    opts: Opts,
    binlog_opts: Opts,
    control: Conn,
    env: Arc<Env>,
    flavor: Flavor,
    identity: String,
    lower_names: bool,
    health: Arc<Health>,
    /// The metrics registry when `LAZYWF_METRICS_ADDR` is set (`None` = off entirely).
    metrics: Option<Arc<Metrics>>,
    catalog: Catalog,

    /// After the last transaction read.
    boundary: Position,
    /// After the last transaction that touched anything but the status table.
    settled: Position,
    in_txn: bool,
    txn_foreign: bool,
    /// GTID of the last transaction read (`None` when it had none: the fence then waits
    /// another way).
    last_gtid: Option<String>,
    txn_gtid: Option<String>,
    last_event_ts: u32,
    caught_up: bool,
    table_ids: HashMap<u64, Route>,
    txn_marks: HashMap<String, Marks>,
    /// The string a row's Simple keys are built in (see [`KeyScratch`]).
    key_scratch: KeyScratch,
    xa_current: Option<(String, XaTxn)>,
    xa_pending: HashMap<String, XaTxn>,
    /// Rules enabled again at a position the stream has passed: replayed from there (see
    /// `begin_rewind`), not reset.
    rewinds: Vec<(String, Position)>,
    /// A replay whose first connect is pending, to undo if its position was purged meanwhile.
    rewound: Option<Rewound>,
    /// Tables created `LIKE` another while streaming (copy → original): the copies an online
    /// schema change fills and then swaps in.
    shadows: HashMap<(String, String), (String, String)>,
    /// Rate limiting of the statement-format DML warning ([`Engine::note_statement_dml`]):
    /// per (schema, table) the time of the last line — `None` while no line has been logged yet,
    /// which is how the first event logs at once without any arithmetic on the clock — and the
    /// events folded into the next line. Bounded by [`MAX_STMT_DML_TABLES`].
    stmt_dml_last: HashMap<(String, String), (Option<Instant>, u64)>,
    /// Statement-format DML events seen since the start (`lazywf_statement_events_ignored_total`,
    /// and `/status`): the map above stops taking tables once it is full, this number does not.
    stmt_dml_ignored: u64,
    /// The one WARN [`Engine::note_statement_dml`] says when [`MAX_STMT_DML_TABLES`] is reached:
    /// a table set that no longer fits says so, instead of looking like a quiet source.
    stmt_dml_capped: bool,

    config_dirty: Option<Instant>,
    config_fp: u64,
    next_poll: Instant,
    targets: HashMap<String, Arc<Target>>,
    routes: HashMap<(String, String), Arc<Vec<(String, usize)>>>,
    /// The routes built by [`Engine::route`], cached per table (see [`RouteCache`]).
    route_cache: RouteCache,
    rules: BTreeMap<String, RuleState>,
    saved: HashMap<String, Saved>,
    incarnations: u64,
    runs: u64,
    target_backoff: HashMap<String, Backoff>,
    /// Tables whose binlog layout does not match (reported once).
    mismatched: HashSet<(String, String)>,
    column_warnings: HashSet<(String, u64, String)>,
    /// The server is a MariaDB 11.4 or newer, whose UPDATE and DELETE row images leave generated
    /// columns — VIRTUAL or STORED — out (see `maria_skips_generated`).
    maria_skips_generated: bool,
    /// Per watched table, the index and name of its first generated column, of either kind
    /// (`None` when it has none). Read once per table like the layout, and dropped with it:
    /// `on_statement` and the mismatch path invalidate both, so a DDL that adds a generated
    /// column is seen again.
    generated_first: HashMap<(String, String), Option<(usize, String)>>,
    /// Triggers already reported as unreadable (see `refuse_generated_reads`), warned about once.
    generated_refusals: HashSet<(String, u64)>,
    /// Triggers already reported as keying on bytes no charset defines (see
    /// `refuse_binary_keys`), warned about once.
    binary_refusals: HashSet<(String, u64)>,
    /// Triggers already reported as holding a key whose bytes no charset could be read as, row
    /// by row (see `Layout::key_is_lossy`), warned about once. The rows themselves are dropped
    /// either way; this only holds the report back.
    lossy_refusals: HashSet<(String, u64)>,
    tx: mpsc::UnboundedSender<Done>,
    last_persist: Instant,
    clear_retry: Option<Instant>,
    star_written: Option<(Position, Instant)>,
    render_warnings: u64,
    invalid_keys: u64,
    /// Schemas already checked against the server's binlog filters (see `check_binlog_filters`).
    filter_checked: HashSet<String>,
    /// The checked schemas the filters drop (shown on `/status`).
    binlog_filtered: BTreeSet<String>,
    /// A binlog dump error whose message says nothing about the position (see `binlog_unusable`):
    /// the position is kept and the dump retried; shown on `/status` until the stream recovers.
    stream_error: Option<String>,
    /// What the stream loop remembers of the events this build cannot decode (see
    /// [`UndecodableWatch`]): the position its one line named, and how many such events there were.
    undecodable: UndecodableWatch,
    /// What the reader knows of the **source's** clock (see [`SourceClock`]): fed by the routed
    /// rows events and the heartbeats in [`Engine::on_item`], reset on every connect, and asked
    /// the source's time per [`CLOCK_REFRESH`] tick. The per-rule `source_lag_seconds` on
    /// `/status` and `lazywf_source_lag_seconds` are read from here at publish.
    clock: SourceClock,
    /// When the source's time was last asked ([`Engine::refresh_source_clock`]); moves on every
    /// attempt, failure included, so a flapping source cannot make this a per-turn statement.
    last_clock: Instant,
}

fn server_code(e: &anyhow::Error) -> Option<u16> {
    e.chain()
        .find_map(|c| match c.downcast_ref::<mysql_async::Error>() {
            Some(mysql_async::Error::Server(se)) => Some(se.code),
            _ => None,
        })
}

fn server_messages(e: &anyhow::Error) -> impl Iterator<Item = String> + '_ {
    e.chain()
        .filter_map(|c| match c.downcast_ref::<mysql_async::Error>() {
            Some(mysql_async::Error::Server(se)) => Some(se.message.clone()),
            _ => c
                .downcast_ref::<mysql_async::ServerError>()
                .map(|se| se.message.clone()),
        })
}

/// The 1236 wordings that say the requested position can never be served: the binlog file was
/// purged or is gone, the position is past the end of the file, or the GTIDs needed are gone.
/// From the servers' own sources — MySQL/Percona `sql/rpl_binlog_sender.cc` (8.0, 8.4, 9.x) and
/// MariaDB `sql/sql_repl.cc` (10.11–13.1); a purged file or GTIDs is already caught by
/// `source::is_missing_binlog`. Every other 1236 message — an event larger than
/// `max_allowed_packet`, a damaged event, the binlog closed, a GTID mode mismatch — reports a
/// server-side problem that says nothing about the position, so the position is kept and the
/// dump retried; an unknown message retries too, since the default must never re-send a target.
fn position_unavailable(e: &anyhow::Error) -> bool {
    const LOST: [&str; 7] = [
        "position > file size",    // MySQL: the position is past the end of the file
        "position < 4",            // MySQL: the position is before the first event
        "impossible position",     // MariaDB: the position is outside the file
        "could not find next log", // MySQL / MariaDB: the next file is gone
        "missing the gtid",        // MariaDB: a requested GTID is in no binlog
        "not in the master's binlog", // MariaDB: a requested GTID is above what the server has
        "has more gtids than",     // MySQL: the replica asks for GTIDs the source lost
    ];
    server_messages(e).any(|m| {
        let m = m.to_ascii_lowercase();
        LOST.iter().any(|p| m.contains(p))
    })
}

/// Whether the stream lost its continuity: the requested binlog is purged or gone, or a 1236
/// dump error whose own message says the requested position cannot be served. A 1236 with any
/// other message (or none recognised) is the server failing to read its own binlog — not a lost
/// position — and is retried in place by the callers below.
fn binlog_unusable(e: &anyhow::Error) -> bool {
    source::is_missing_binlog(e)
        || (server_code(e) == Some(1236)
            && !source::is_duplicate_server_id(e)
            && position_unavailable(e))
}

/// Whether this MariaDB leaves generated columns — VIRTUAL **or STORED** — out of its UPDATE and
/// DELETE row images.
///
/// It stopped writing them at 11.4: 10.6.28 and 10.11.19 still write them, as do MySQL 8.0.46,
/// 8.4.13, 9.7.4 and Percona 8.4.11. Measured on 11.4.13 and 11.8.9 (the compose servers,
/// `binlog_row_image=FULL`, a table with `code_hex CHAR(32) AS (HEX(code)) STORED` beside an
/// identical one with a real `CHAR(32)`): the INSERT event carries every column, the UPDATE and
/// DELETE events carry one value less than the table map names, while the table with the real
/// column carries all of them — MariaDB stopped writing generated columns into those images and
/// the two kinds are left out together. Such an image and the reader that pairs values with
/// columns by position make every column from the first generated one on read as its neighbour's
/// value — a key built from the wrong value, with nothing logged. A version that cannot be read
/// counts as unaffected: refusing on a parse failure would stop a working installation over a
/// version string.
// Temporary: remove this and `Engine::refuse_generated_reads` when the vendored reader learns to
// decode images whose values skip columns (lazyview issue #7); until then lazywf refuses the
// triggers that would read them, which is visible on `/status` instead of silently wrong.
fn maria_skips_generated(version: &str) -> bool {
    // A replication proxy reports the server it fronts as `5.5.5-<its version>`.
    let v = version.strip_prefix("5.5.5-").unwrap_or(version);
    let mut parts = v.split('.');
    let num = |p: Option<&str>| p.and_then(|p| p.parse::<u32>().ok());
    match (num(parts.next()), num(parts.next())) {
        (Some(major), Some(minor)) => (major, minor) >= (11, 4),
        _ => false,
    }
}

/// What a trigger reads of a row image the server left short.
#[derive(Debug, PartialEq, Eq)]
enum ShortRead {
    /// A column standing at or after the table's first generated column.
    Col(String),
    /// The whole row. A `KeyTemplate::Full` is any Handlebars template, so there is no telling
    /// from the rule whether it names the shifted columns: it may read every column, so it is
    /// refused for any table that has a generated one.
    Full,
}

/// The first thing `reads` takes from an image that skips its generated columns: `reads` holds the
/// columns the trigger names together with their index in the layout (columns the table does not
/// have are left out — `check_columns` reports those), `full` says whether the trigger renders
/// the whole row, and `from` is the index of the table's first generated column. A rule
/// that reads only columns before `from` reads values the image does carry, so it stays allowed.
fn unreadable_read(reads: &[(&str, usize)], full: bool, from: usize) -> Option<ShortRead> {
    if full {
        return Some(ShortRead::Full);
    }
    reads
        .iter()
        .find(|(_, i)| *i >= from)
        .map(|(c, _)| ShortRead::Col((*c).to_string()))
}

/// The path segments a Handlebars key template names inside its `{{ }}` tags: `{{new.code}}`,
/// `{{code}}` and `{{helper arg=this.code}}` all yield `code`, while literal text outside the
/// tags, comments and raw-block content — none of which resolve paths — cannot. A quoted
/// argument spelled like a column name does count, which errs towards refusing. Roots
/// (`new`, `row`, …) and literals (`true`, …) are filtered: a column cannot be named one of
/// those and resolve.
fn hb_refs(source: &str) -> Vec<&str> {
    // `get` everywhere instead of indexing: the offsets are proven by the `find`s and the
    // `starts_with` guards around them, and the panic-site gate reads the indexing, not
    // the argument.
    fn from(t: &str, n: usize) -> &str {
        t.get(n..).unwrap_or("")
    }
    let mut out = Vec::new();
    let mut rest = source;
    while let Some(open) = rest.find("{{") {
        let tag = from(rest, open);
        // What is never rendered passes unscanned, the way `resolve_roots` leaves it untouched.
        let skip_to = if tag.starts_with("{{!--") {
            tag.get(5..).and_then(|t| t.find("--}}")).map(|n| 5 + n + 4)
        } else if tag.starts_with("{{!") {
            tag.get(3..).and_then(|t| t.find("}}")).map(|n| 3 + n + 2)
        } else if tag.starts_with("{{{{") {
            // `{{{{raw}}}} … {{{{/raw}}}}`
            tag.get(4..)
                .and_then(|t| t.find("{{{{/"))
                .map(|n| 4 + n)
                .and_then(|close| {
                    tag.get(close..)
                        .and_then(|t| t.find("}}}}"))
                        .map(|m| close + m + 4)
                })
                .or(Some(tag.len()))
        } else {
            None
        };
        let (body, next): (&str, &str) = if let Some(end) = skip_to {
            ("", from(tag, end))
        } else {
            match tag.get(2..).and_then(|t| t.find("}}")) {
                // The whole tag: paths, helper names and arguments alike — the scan does not
                // parse, it finds names.
                Some(close) => {
                    let close = 2 + close + 2;
                    (tag.get(2..close - 2).unwrap_or(""), from(tag, close))
                }
                None => ("", ""),
            }
        };
        for seg in body
            .split(|c: char| !(c.is_alphanumeric() || matches!(c, '_' | '$' | '@' | '.' | '/')))
            .flat_map(|tok| tok.split(['.', '/']))
        {
            if !seg.is_empty()
                && !seg.starts_with('@')
                && !matches!(seg, "this" | "true" | "false" | "null")
                && !template::KEY_ROOTS.contains(&seg)
            {
                out.push(seg);
            }
        }
        rest = next;
    }
    out
}

/// The columns a trigger builds its key from, as layout indices: the `Seg::Col` names of a
/// `Simple` key (exact), the names a Handlebars key makes inside its tags (see [`hb_refs`]),
/// and the columns bound to its `keys_sql` — the bound values reach the user's statement as
/// decoded text, so bytes with no charset reach the `WHERE` as replacement characters too and
/// can answer with another row's `_key`. A trigger's `changed_columns` are not key reads and
/// do not count: they only compare two images of the same row.
fn key_column_reads(t: &crate::rule::Trigger, layout: &Layout) -> Vec<usize> {
    let mut out = Vec::new();
    match &t.key {
        Some(KeyTemplate::Simple(lines)) => {
            for seg in lines.iter().flatten() {
                if let template::Seg::Col(c) = seg {
                    out.extend(layout.index(c));
                }
            }
        }
        Some(KeyTemplate::Full(_)) => {
            // The source the compiled templates were registered from — what the user wrote.
            if let Some(src) = &t.def.key {
                for c in hb_refs(src) {
                    out.extend(layout.index(c));
                }
            }
        }
        None => {}
    }
    if let Some(ks) = &t.keys_sql {
        for c in &ks.columns {
            out.extend(layout.index(c));
        }
    }
    out
}

/// The first generated column of `rows`, which are the table's columns in
/// `information_schema` order as (name, EXTRA): its index in that order (the order
/// `rows::Catalog::load` builds a layout from) and its name. Either kind counts, because a
/// server that leaves generated columns out of its UPDATE and DELETE images leaves both out
/// (`maria_skips_generated`). Only the two generated wordings match: `DEFAULT_GENERATED`,
/// which MySQL writes for a column with an expression default, describes a default and not a
/// column the image skips.
fn generated_first_of(rows: &[(String, String)]) -> Option<(usize, String)> {
    rows.iter()
        .enumerate()
        .find(|(_, (_, extra))| {
            let e = extra.to_ascii_uppercase();
            e.contains("VIRTUAL GENERATED") || e.contains("STORED GENERATED")
        })
        .map(|(i, (name, _))| (i, name.clone()))
}

/// The columns of one table, in `information_schema` order, as (name, EXTRA).
async fn read_generated_first(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Option<(usize, String)>> {
    // The `CAST`s keep the columns out of the binary charset, as in `rows::Catalog::load`.
    let rows: Vec<(String, String)> = conn
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR), CAST(EXTRA AS CHAR) \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        .await
        .with_context(|| format!("reading the columns of `{schema}`.`{table}`"))?;
    Ok(generated_first_of(&rows))
}

async fn connect_wait(
    opts: &Opts,
    health: &Health,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Conn>> {
    let mut wait = Duration::from_millis(500);
    loop {
        match tokio::time::timeout(Duration::from_secs(30), Conn::new(opts.clone())).await {
            Ok(Ok(c)) => return Ok(Some(c)),
            Ok(Err(e)) => {
                let e = anyhow::Error::new(e);
                if matches!(server_code(&e), Some(1044 | 1045 | 1049)) {
                    return Err(e.context("connecting to the database"));
                }
                warn!("cannot connect to the database: {e:#}; retrying in {wait:?}");
            }
            Err(_) => warn!("connecting to the database timed out; retrying in {wait:?}"),
        }
        health.beat();
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
        }
        wait = (wait * 2).min(Duration::from_secs(5));
    }
}

/// Amazon RDS (`@@basedir` under `rdsdbbin`) or Aurora (`@@aurora_version` exists).
fn is_managed(basedir: Option<&str>, aurora: bool) -> bool {
    aurora || basedir.is_some_and(|b| b.contains("rdsdbbin"))
}

/// What to say at startup on Amazon RDS / Aurora, from the binlog retention (`Ok(None)`: not
/// set, `Err`: unreadable) and, on MySQL, `gtid_mode`. `true` marks a warning.
fn managed_notes(
    retention: &std::result::Result<Option<u64>, String>,
    gtid_mode: Option<&str>,
) -> Vec<(bool, String)> {
    const FIX: &str = "Run once as the master user: CALL mysql.rds_set_configuration('binlog retention hours', 24);";
    let mut out = Vec::new();
    let consequence =
        "after an outage longer than that lazywf cannot resume and runs every rule's reset steps";
    match retention {
        Ok(Some(h)) if *h >= 12 => {}
        Ok(Some(h)) => out.push((true, format!("Amazon RDS / Aurora binlog retention is {h} hours; {consequence}. {FIX}"))),
        Ok(None) => out.push((true, format!("Amazon RDS / Aurora binlog retention is not set (binlogs are purged within minutes); {consequence}. {FIX}"))),
        Err(e) => out.push((true, format!("Amazon RDS / Aurora binlog retention could not be read ({e}); make sure it is at least 12 hours (CALL mysql.rds_show_configuration). {FIX}"))),
    }
    if gtid_mode.is_some_and(|g| !g.eq_ignore_ascii_case("ON")) {
        out.push((false, "tip: gtid_mode=ON and enforce_gtid_consistency=ON in the DB parameter group (Aurora: the DB cluster parameter group) give lazywf an exact visibility fence; without GTIDs it waits LAZYWF_VISIBILITY_DELAY_MS before reading".into()));
    }
    out
}

/// Amazon RDS / Aurora purge binlogs within minutes by default, and without GTIDs the
/// visibility fence falls back to a delay: both are said once at startup.
async fn managed_service_checks(conn: &mut Conn, flavor: Flavor) {
    let basedir: Option<String> = conn.query_first("SELECT @@basedir").await.unwrap_or(None);
    let aurora: Option<String> = conn
        .query_first("SELECT @@aurora_version")
        .await
        .unwrap_or(None);
    if !is_managed(basedir.as_deref(), aurora.is_some()) {
        return;
    }
    let retention = conn
        .query_first::<Option<String>, _>(
            "SELECT value FROM mysql.rds_configuration WHERE name = 'binlog retention hours'",
        )
        .await
        .map(|v| v.flatten().and_then(|h| h.trim().parse::<u64>().ok()))
        .map_err(|e| e.to_string());
    let gtid: Option<String> = if flavor.is_mysql_family() {
        conn.query_first("SELECT @@GLOBAL.gtid_mode")
            .await
            .unwrap_or(None)
    } else {
        None
    };
    for (warning, note) in managed_notes(&retention, gtid.as_deref()) {
        if warning {
            warn!("{note}");
        } else {
            info!("{note}");
        }
    }
}

async fn server_identity(conn: &mut Conn, flavor: Flavor) -> Result<String> {
    let q = match flavor {
        Flavor::MySql | Flavor::Percona => "SELECT CAST(@@server_uuid AS CHAR)",
        Flavor::MariaDb => "SELECT CAST(CONCAT(@@server_id, ':', @@gtid_domain_id) AS CHAR)",
    };
    let v: Option<String> = conn
        .query_first(q)
        .await
        .context("reading the server identity")?;
    Ok(v.unwrap_or_default())
}

/// Runs until shutdown is requested (`Ok`) or a fatal error.
pub async fn run(
    cfg: Config,
    health: Arc<Health>,
    metrics: Option<Arc<Metrics>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // The reader names lazyview in its messages unless told otherwise (main does it earlier).
    binlog::set_product("lazywf", "LAZYWF_");
    fence::set_table(model::FENCE);
    analyze::set_reserved(model::RESERVED_TABLES, &[model::RESERVED_PREFIX]);
    let opts = binlog::replica_opts(&cfg.dsn)?;
    let binlog_opts = binlog::replica_opts(&cfg.binlog_dsn)?;
    let Some(mut control) = connect_wait(&opts, &health, &mut shutdown).await? else {
        return Ok(());
    };
    model::ensure(&mut control).await?;
    let lock_name = lock::name(&cfg.schema, &cfg.instance);
    if !lock::acquire(&mut control, &opts, &lock_name, &health, &mut shutdown).await? {
        return Ok(());
    }
    health.set_phase("starting");
    let info = binlog::server_info(&mut control).await?;
    managed_service_checks(&mut control, info.flavor).await;
    let identity = server_identity(&mut control, info.flavor).await?;
    let lower: Option<i64> = control
        .query_first("SELECT @@lower_case_table_names")
        .await
        .unwrap_or(None);
    let pool_opts = || {
        PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, cfg.pool_size).expect("0 <= max"))
    };
    let pool = Pool::new(OptsBuilder::from_opts(opts.clone()).pool_opts(pool_opts()));
    // The SQL that the configuration defines runs on the query account when one is set;
    // otherwise on the main pool (the same pool, so nothing changes).
    let query_account = cfg.query_account;
    let query = if query_account {
        let query_opts = binlog::replica_opts(&cfg.query_dsn)?;
        let query = Pool::new(OptsBuilder::from_opts(query_opts).pool_opts(pool_opts()));
        let mut probe = query
            .get_conn()
            .await
            .map_err(|e| anyhow!("LAZYWF_QUERY_* account cannot connect to the server: {e}"))?;
        probe
            .query_drop("SELECT 1")
            .await
            .context("LAZYWF_QUERY_* account: SELECT 1")?;
        drop(probe);
        query
    } else {
        pool.clone()
    };
    let env = Arc::new(Env::new(
        pool.clone(),
        query.clone(),
        info.flavor,
        cfg.visibility_delay,
        cfg.send_concurrency,
    ));
    let saved = status::load(&mut control, &cfg.instance).await?;
    let query_user = if cfg.query_account {
        query_opts_user(&cfg.query_dsn)
    } else {
        "none (the main account runs the queries)".to_string()
    };
    info!(server = %info.version, schema = %cfg.schema, instance = %cfg.instance, query_account = %query_user, "lazywf {} starting", env!("CARGO_PKG_VERSION"));
    match &cfg.allowed_hosts {
        Some(list) if list.is_deny_all() => warn!(
            "LAZYWF_ALLOWED_HOSTS is not set: target egress is denied and every target will \
             fail at load. List the hosts targets may reach, or set `LAZYWF_ALLOWED_HOSTS=*` \
             to allow any host (the pre-0.3 behaviour, not recommended)"
        ),
        Some(list) if list.is_allow_all() => info!(
            "LAZYWF_ALLOWED_HOSTS=*: target endpoints may use any host (the operator's \
             explicit opt-out; a target row can then reach any address)"
        ),
        Some(list) => info!(
            patterns = list.len(),
            "LAZYWF_ALLOWED_HOSTS is set: target endpoints must match it"
        ),
        None => info!("no allow-list: target endpoints may use any host (tests only)"),
    }
    // Whether the server's row images can come up short (see `maria_skips_generated`). Read once:
    // the server does not change underneath a running engine.
    let skips_generated = info.flavor == Flavor::MariaDb && maria_skips_generated(&info.version);
    if skips_generated {
        info!(
            server = %info.version,
            "this MariaDB leaves generated columns (VIRTUAL or STORED) out of its UPDATE and \
             DELETE row images: a trigger that reads a column from the first one on is refused"
        );
    }
    let (tx, rx) = mpsc::unbounded_channel();
    let mut e = Engine {
        lock: lock_name,
        opts,
        binlog_opts,
        control,
        env,
        flavor: info.flavor,
        identity,
        lower_names: lower.unwrap_or(0) != 0,
        health,
        metrics,
        catalog: Catalog::default(),
        boundary: Position {
            file: String::new(),
            pos: 4,
        },
        settled: Position {
            file: String::new(),
            pos: 4,
        },
        in_txn: false,
        txn_foreign: false,
        last_gtid: None,
        txn_gtid: None,
        last_event_ts: 0,
        caught_up: false,
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
        next_poll: Instant::now() + cfg.config_poll,
        targets: HashMap::new(),
        routes: HashMap::new(),
        route_cache: RouteCache::default(),
        rules: BTreeMap::new(),
        saved,
        incarnations: 0,
        runs: 0,
        target_backoff: HashMap::new(),
        mismatched: HashSet::new(),
        column_warnings: HashSet::new(),
        maria_skips_generated: skips_generated,
        generated_first: HashMap::new(),
        generated_refusals: HashSet::new(),
        binary_refusals: HashSet::new(),
        lossy_refusals: HashSet::new(),
        tx,
        last_persist: Instant::now(),
        clear_retry: None,
        star_written: None,
        render_warnings: 0,
        invalid_keys: 0,
        filter_checked: HashSet::new(),
        binlog_filtered: BTreeSet::new(),
        stream_error: None,
        undecodable: UndecodableWatch::default(),
        // `head_ts` at the local now: `housekeeping` can reach `persist` through `idle` before
        // the first connect, and a rule measured against an unset head must not read an epoch
        // age (the connect arms the clock for real, see the stream loop).
        clock: SourceClock {
            head_ts: crate::time::unix_now(),
            ..SourceClock::default()
        },
        last_clock: Instant::now(),
        cfg,
    };
    e.reload_config(true).await?;
    let start = e.choose_start().await?;
    let r = e.stream(start, rx, &mut shutdown).await;
    pool.disconnect().await.ok();
    if query_account {
        query.disconnect().await.ok();
    }
    r
}

/// The user name of a DSN, for logs (never the password).
fn query_opts_user(dsn: &str) -> String {
    binlog::replica_opts(dsn)
        .ok()
        .and_then(|o| o.user().map(str::to_string))
        .unwrap_or_default()
}

impl Engine {
    fn norm(&self, schema: &str, table: &str) -> (String, String) {
        if self.lower_names {
            (schema.to_lowercase(), table.to_lowercase())
        } else {
            (schema.to_string(), table.to_string())
        }
    }

    // ---- positions ----------------------------------------------------------------------

    fn rule_pos(&self, st: &RuleState) -> Position {
        let mut p = self.settled.clone();
        if let Some(s) = &st.skip_until {
            if *s > p {
                p = s.clone();
            }
        }
        let mut lower = |q: &Position| {
            if *q < p {
                p = q.clone();
            }
        };
        // The core's floor: the oldest unflushed mark (pending, or the generation a flush
        // took until its last chunk completed).
        if let Some(o) = st.buffer.core.pinned() {
            lower(o);
        }
        if let Some(f) = &st.frozen_at {
            lower(f);
        }
        p
    }

    /// Where a restart must resume: no rule, and no open XA transaction, is behind it.
    fn resume_pos(&self) -> Position {
        let mut p = self.settled.clone();
        for st in self.rules.values() {
            let q = self.rule_pos(st);
            if q < p {
                p = q;
            }
        }
        for xa in self.xa_pending.values() {
            if xa.prepared_at.elapsed() < XA_PIN_MAX && xa.start < p {
                p = xa.start.clone();
            }
        }
        p
    }

    fn stream_lag_ms(&self) -> u64 {
        if self.caught_up || self.last_event_ts == 0 {
            0
        } else {
            crate::time::unix_now().saturating_sub(u64::from(self.last_event_ts)) * 1000
        }
    }

    fn rule_lag_ms(&self, st: &RuleState) -> u64 {
        let ts = [
            (st.buffer.since_ts != 0).then_some(st.buffer.since_ts),
            match &st.lane {
                Some(Lane::Flush { since_ts, .. }) if *since_ts != 0 => Some(*since_ts),
                _ => None,
            },
        ]
        .into_iter()
        .flatten()
        .min();
        let pending = ts.map_or(0, |t| {
            crate::time::unix_now().saturating_sub(u64::from(t)) * 1000
        });
        pending.max(self.stream_lag_ms())
    }

    // ---- start ----------------------------------------------------------------------------

    /// The position to stream from, and which rules lost continuity.
    async fn choose_start(&mut self) -> Result<Position> {
        let (current, files) = self.binlog_files().await?;
        let available = |p: &Position| files.contains(&p.file) && *p <= current;
        let star = self.saved.get(INSTANCE_ROW).cloned();
        let lost_all = match &star {
            None => Some("first start".to_string()),
            Some(s) if s.pos.is_none() => Some("no saved position".to_string()),
            Some(s)
                if s.server_identity
                    .as_deref()
                    .is_some_and(|i| !i.is_empty() && i != self.identity) =>
            {
                Some(format!(
                    "the server changed (position saved on {}, this is {})",
                    s.server_identity.as_deref().unwrap_or(""),
                    self.identity
                ))
            }
            _ => None,
        };
        let mut candidates = Vec::new();
        let mut rule_pos: HashMap<String, Position> = HashMap::new();
        let mut lost: Vec<(String, String)> = Vec::new();
        let names: Vec<String> = self.rules.keys().cloned().collect();
        match &lost_all {
            Some(reason) => lost.extend(names.iter().map(|n| (n.clone(), reason.clone()))),
            None => {
                if let Some(p) = star.as_ref().and_then(|s| s.pos.clone()).filter(available) {
                    candidates.push(p);
                }
                for n in &names {
                    match self.saved.get(n).and_then(|s| s.pos.clone()) {
                        Some(p) if available(&p) => {
                            candidates.push(p.clone());
                            rule_pos.insert(n.clone(), p);
                        }
                        Some(p) => lost.push((
                            n.clone(),
                            format!("binlog position {p} is no longer available"),
                        )),
                        None => lost.push((n.clone(), "new rule".to_string())),
                    }
                }
            }
        }
        let start = candidates
            .into_iter()
            .min()
            .unwrap_or_else(|| current.clone());
        self.boundary = start.clone();
        self.settled = start.clone();
        for st in self.rules.values_mut() {
            if st.frozen_at.is_some() {
                st.frozen_at = Some(start.clone());
            }
        }
        for (n, p) in rule_pos {
            if p > start {
                if let Some(st) = self.rules.get_mut(&n) {
                    st.skip_until = Some(p);
                }
            }
        }
        let lost_now: HashSet<String> = lost.iter().map(|(n, _)| n.clone()).collect();
        for (n, reason) in lost {
            self.lose_continuity(&n, reason);
        }
        // Resets a previous run did not finish.
        for n in names {
            let Some(saved) = self.saved.get(&n) else {
                continue;
            };
            let Some(reason) = unfinished_reset(&saved.reset_state) else {
                continue;
            };
            if self.rules.get(&n).is_some_and(|s| s.reset.is_none()) {
                // A rule that lost continuity above already got its fresh step-0 run from
                // `lose_continuity`: its saved position is out of the game, so no boot may
                // skip its finished steps over. A rule that kept it may — the steps before
                // the interrupted one are target-confirmed, and everything the target has
                // not seen sits above the position this boot replays from at or below
                // (`rule_pos` is floored at the core's oldest unflushed mark), so the stream
                // here re-marks it. The requested run's own arms are `add_rule`'s (#200,
                // #203, #209); this is the boot's carry-over arm.
                let resume = self.rules.get(&n).and_then(|s| {
                    carried_reset_resume(
                        !lost_now.contains(&n),
                        &saved.reset_state,
                        &s.rule.def.reset_steps,
                        &s.rule.fingerprint,
                    )
                });
                self.start_reset(&n, format!("{reason} (resumed after restart)"), false);
                if let Some(step) = resume {
                    if let Some(run) = self.rules.get_mut(&n).and_then(|s| s.reset.as_mut()) {
                        run.step = step;
                        info!(
                            rule = %n,
                            "reset steps resume at step {}: the saved run was this def's and the rule kept its position",
                            step + 1
                        );
                    }
                }
            }
        }
        info!(position = %start, "starting from {}", if lost_all.is_some() { "the current binlog position" } else { "the saved positions" });
        Ok(start)
    }

    // ---- continuity and resets -----------------------------------------------------------

    fn lose_continuity(&mut self, name: &str, reason: String) {
        let Some(st) = self.rules.get_mut(name) else {
            return;
        };
        if st.rule.def.reset_steps.is_empty() {
            warn!(rule = %name, "continuity lost ({reason}); the rule has no reset_steps, changes in between are not covered");
            st.reset_state = format!("missing: {reason}");
            st.dirty = true;
        } else {
            self.start_reset(name, reason, false);
        }
    }

    fn start_reset(&mut self, name: &str, reason: String, requested: bool) {
        // A reset re-reads the table the rule watches: every route built beforehand — its
        // layout identity, its trigger list, its flags — is dropped with the rest of what it
        // assumed. Before the early returns: a reset of a rule that is gone is a signal too.
        self.route_cache.clear();
        self.runs += 1;
        let id = self.runs;
        let Some(st) = self.rules.get_mut(name) else {
            return;
        };
        // A new run consumes any request the clear was holding back: from here the reload's
        // own arms serve it, and a fresh one has to announce itself again.
        st.reset_request_seen = false;
        if st.rule.def.reset_steps.is_empty() {
            if requested && st.reset.is_none() && st.rule.error.is_none() {
                st.pending_clear = true;
            }
            return;
        }
        let requested = requested || st.reset.as_ref().is_some_and(|r| r.requested);
        info!(rule = %name, "reset steps start: {reason}");
        st.reset = Some(ResetRun {
            id,
            reason: reason.clone(),
            requested,
            fingerprint: st.rule.fingerprint.clone(),
            step: 0,
            phase: Phase::Start,
            touched: None,
            backoff: Backoff::default(),
        });
        st.reset_state = format!("pending: {reason}");
        // The run re-reads the table from a snapshot: what the rule applied before is no
        // longer its honest head, so the stamp starts over (the clock's own gap measures it
        // meanwhile, see [`SourceClock::applied`]).
        st.applied_ts = 0;
        st.dirty = true;
    }

    /// Positions in purged binlogs mean nothing any more: everything restarts at `p`.
    fn rebase(&mut self, p: &Position) {
        self.boundary = p.clone();
        self.settled = p.clone();
        self.xa_pending.clear();
        for st in self.rules.values_mut() {
            // The core cannot raise an origin: everything pending (and any generation in
            // flight) comes back re-marked at `p`. A flush job still out gets its refused
            // ids re-marked at `p` too once it reports (`on_done` sees no generation).
            if st.buffer.core.pinned().is_some() {
                let (sets, items) = st.buffer.core.drain();
                for (i, set) in sets.into_iter().enumerate() {
                    let s = key::SpaceId(i as u16);
                    for v in set.ints.to_sorted_vec() {
                        st.buffer.core.mark_int(s, v, p);
                    }
                    for t in set.keys {
                        st.buffer.core.mark_tuple(s, t, p);
                    }
                }
                for i in items {
                    st.buffer.core.item(i, p);
                }
            }
            st.skip_until = None;
            if st.frozen_at.is_some() {
                st.frozen_at = Some(p.clone());
            }
        }
    }

    /// A 1236 whose message says nothing about the requested position (see `binlog_unusable`):
    /// the server failed to read its own binlog. Nothing is re-sent: the position stays, the
    /// caller retries the dump with the capped backoff, and the server's message is shown on
    /// `/status` (`stream_error`) until the stream delivers events again.
    fn on_kept_binlog_error(&mut self, e: &anyhow::Error, at: &Position, retry_in: Duration) {
        self.stream_error = Some(format!("{e:#} at {at}"));
        warn!("binlog dump at {at} failed ({e:#}); the position is kept, retrying in {retry_in:?}");
    }

    /// An event this build cannot decode, seen where the reader maps the stream (review A-02, the
    /// `undecodable` arm): the bytes arrived and the decoder refused them, so every connect that
    /// starts at this position fails on them again until a build that reads them ships. The loop
    /// treats it as it treats a 1236 the server cannot serve — the position is kept, the event is
    /// never skipped, and the round does not count as recovered, so the backoff keeps growing
    /// instead of the 200 ms a stream that is moving earns — and `/status` carries the decoder's
    /// words (`stream_error`) until the stream delivers again. The position gets one WARN line, not
    /// one per round: see [`UndecodableWatch::note`].
    fn on_undecodable_stream_error(
        &mut self,
        e: &anyhow::Error,
        at: &Position,
        retry_in: Duration,
    ) {
        self.stream_error = Some(format!("{e:#} at {at}"));
        if self.undecodable.note(at) {
            warn_undecodable_stream_error(e, at, retry_in);
        }
        if let Some(m) = &self.metrics {
            m.undecodable_event();
        }
    }

    /// The rows of a watched table, refused by the decoder: the one ERROR line the owner's ruling
    /// of 2026-11-16 (carried by `source::Undecodable`) requires, `/status` told, the counter moved
    /// — and the caller exits at once, because the ruling keeps a decode failure fatal. The ten
    /// generic re-reads this error used to burn were re-reads of the same boundary, each failing on
    /// the same bytes: they reached the same exit two seconds later, with ten log lines.
    fn on_undecodable_rows(&mut self, e: &anyhow::Error) {
        self.stream_error = Some(format!("{e:#}"));
        self.undecodable.count();
        if let Some(m) = &self.metrics {
            m.undecodable_event();
        }
        error_undecodable_rows(e);
    }

    /// The stream delivered events again after a kept position (a 1236 the server cannot serve, or
    /// bytes this build cannot decode): the dump error is no longer current — `/status` stops
    /// showing it, which is the contract of [`Engine::on_kept_binlog_error`] — and the position the
    /// reader was stuck at is forgotten, so standing still there again is news.
    fn on_stream_recovered(&mut self) {
        self.stream_error = None;
        self.undecodable.recovered();
    }

    async fn on_missing_binlog(&mut self, e: &anyhow::Error) -> Result<()> {
        self.stream_error = None;
        if let Some(rw) = self.rewound.take() {
            // Purged before the replay began: only the rules enabled again lose continuity, the
            // others go on from where the stream was.
            let at = std::mem::replace(&mut self.boundary, rw.boundary);
            self.settled = rw.settled;
            warn!("binlog position {at} is no longer available ({e:#}); the rules enabled again run their reset steps instead of replaying");
            for n in rw.rules {
                self.lose_continuity(
                    &n,
                    format!("enabled again, and binlog position {at} is no longer available"),
                );
            }
            return Ok(());
        }
        let current = binlog::current_position(&mut self.control).await?;
        let old = self.boundary.clone();
        warn!("binlog position {old} is no longer available ({e:#}); continuing from {current} and running the reset steps");
        self.rebase(&current);
        let names: Vec<String> = self.rules.keys().cloned().collect();
        for n in names {
            self.lose_continuity(&n, format!("binlog position {old} is no longer available"));
        }
        Ok(())
    }

    /// Moves the stream back to the lowest position of the rules enabled again behind it, before
    /// the next connect. Every other rule skips what it already read; a rule whose position is
    /// no longer in the binlog runs its reset steps instead.
    async fn begin_rewind(&mut self) -> Result<()> {
        let (current, files) = self.binlog_files().await?;
        let mut replay: Vec<(String, Position)> = Vec::new();
        for (n, p) in std::mem::take(&mut self.rewinds) {
            if !self.rules.contains_key(&n) || p >= self.boundary {
                continue; // disabled again meanwhile, or nothing was missed
            }
            if files.contains(&p.file) && p <= current {
                replay.push((n, p));
            } else {
                self.lose_continuity(
                    &n,
                    format!("enabled again, and binlog position {p} is no longer available"),
                );
            }
        }
        let Some(to) = replay.iter().map(|(_, p)| p.clone()).min() else {
            return Ok(());
        };
        let from = self.boundary.clone();
        for (n, st) in self.rules.iter_mut() {
            let read = replay
                .iter()
                .find(|(r, _)| r == n)
                .map_or(&from, |(_, p)| p);
            if *read > to && st.skip_until.as_ref().is_none_or(|s| s < read) {
                st.skip_until = Some(read.clone());
            }
        }
        let rules: Vec<String> = replay.into_iter().map(|(n, _)| n).collect();
        info!(position = %to, ?rules, "rules enabled again: replaying the binlog from their positions");
        self.rewound = Some(Rewound {
            boundary: from,
            settled: std::mem::replace(&mut self.settled, to.clone()),
            rules,
        });
        self.boundary = to;
        Ok(())
    }

    /// The current binlog position and the binlog files the server still has.
    async fn binlog_files(&mut self) -> Result<(Position, HashSet<String>)> {
        let current = binlog::current_position(&mut self.control).await?;
        let logs: Vec<mysql_async::Row> = self
            .control
            .query("SHOW BINARY LOGS")
            .await
            .context("SHOW BINARY LOGS")?;
        let files = logs
            .iter()
            .filter_map(|r| r.get_opt::<String, _>(0).and_then(|v| v.ok()))
            .collect();
        Ok((current, files))
    }

    // ---- configuration ---------------------------------------------------------------------

    async fn reload_config(&mut self, initial: bool) -> Result<bool> {
        let raw = model::load(&mut self.control).await?;
        let fp = {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            format!("{raw:?}").hash(&mut h);
            h.finish()
        };
        if !initial && fp == self.config_fp {
            return Ok(false);
        }
        self.config_fp = fp;
        let mut targets: HashMap<String, std::result::Result<Arc<Target>, String>> = HashMap::new();
        for t in &raw.targets {
            let r = TargetDef::from_row(t)
                .map_err(|e| format!("{e:#}"))
                .and_then(|def| match self.targets.get(&def.name) {
                    Some(old) if old.def == def => Ok(old.clone()),
                    _ => Target::new(def, self.env.query.clone(), self.cfg.allowed_hosts.as_ref())
                        .map_err(|e| format!("{e:#}")),
                });
            if let Err(e) = &r {
                warn!(target = %t.name, "invalid target: {e}");
            }
            targets.insert(t.name.clone(), r);
        }
        self.targets = targets
            .iter()
            .filter_map(|(k, v)| v.as_ref().ok().map(|t| (k.clone(), t.clone())))
            .collect();
        let triggers = model::triggers_by_rule(&raw);
        let mut compiled: BTreeMap<String, Arc<Rule>> = BTreeMap::new();
        let mut disabled = HashSet::new();
        let mut sources = BTreeSet::new();
        for r in &raw.rules {
            if !r.enabled {
                disabled.insert(r.name.clone());
                continue;
            }
            let (def, parse_err) = match RuleDef::from_row(r) {
                Ok(d) => (d, None),
                Err(e) => (RuleDef::placeholder(r), Some(format!("{e:#}"))),
            };
            let mut defs = Vec::new();
            let mut terr = Vec::new();
            for t in triggers.get(&r.name).into_iter().flatten() {
                match TriggerDef::from_row(t, &self.cfg.schema, self.lower_names) {
                    Ok(d) => defs.push(d),
                    Err(e) => terr.push(format!("trigger {}: {e:#}", t.id)),
                }
            }
            sources.extend(defs.iter().map(|d| d.schema.clone()));
            // The configuration's SQL is prepared (and checked) on the query account.
            let mut rule = if self.cfg.query_account {
                let mut qc = self.env.query.get_conn().await?;
                rule::compile(&mut qc, def, defs, &targets).await
            } else {
                rule::compile(&mut self.control, def, defs, &targets).await
            };
            if let Some(e) = parse_err {
                rule.error = Some(match rule.error.take() {
                    Some(o) => format!("{e}; {o}"),
                    None => e,
                });
            }
            if !terr.is_empty() {
                let mut all = terr;
                all.extend(rule.trigger_error.take());
                rule.trigger_error = Some(all.join("; "));
            }
            compiled.insert(r.name.clone(), Arc::new(rule));
        }
        let existing: Vec<String> = self.rules.keys().cloned().collect();
        for name in existing {
            if !compiled.contains_key(&name) {
                let st = self.rules.remove(&name).expect("present");
                let pos = self.rule_pos(&st);
                if disabled.contains(&name) {
                    info!(rule = %name, "rule disabled");
                    // What comes back must find the reset that was going: the state running
                    // it is gone with the removal, and only a live rule writes the status
                    // table a restart resumes from — so the live state is copied here.
                    let rs = st.reset_state.clone();
                    let sv = self.saved.entry(name.clone()).or_default();
                    sv.pos = Some(pos);
                    sv.reset_state = rs;
                } else {
                    info!(rule = %name, "rule removed");
                    self.saved.remove(&name);
                    // A removed rule must leave no series behind: its handles are gone from
                    // `self.rules` above, so drop its rule entry and its (rule, *) pair entries too.
                    // A disabled rule keeps them: it can come back and reuses the same handles.
                    if let Some(reg) = &self.metrics {
                        reg.remove_rule(&name);
                    }
                    if let Err(e) =
                        status::delete(&mut self.control, &self.cfg.instance, &name).await
                    {
                        warn!(rule = %name, "removing its status row: {e:#}");
                    }
                }
            }
        }
        for (name, rule) in compiled {
            for (what, e) in [
                ("cannot fire", &rule.error),
                ("cannot mark keys", &rule.trigger_error),
            ] {
                if let Some(e) = e {
                    let changed = self.rules.get(&name).is_none_or(|s| {
                        let old = if what == "cannot fire" {
                            &s.rule.error
                        } else {
                            &s.rule.trigger_error
                        };
                        old != &rule.error && old != &rule.trigger_error
                    });
                    if changed {
                        warn!(rule = %name, "rule {what}: {e}");
                    }
                }
            }
            if self.rules.contains_key(&name) {
                self.update_rule(&name, rule);
            } else {
                self.add_rule(&name, rule, initial);
            }
        }
        self.check_binlog_filters(&sources).await;
        self.rebuild_routes();
        if !initial {
            debug!("configuration reloaded: {} rules", self.rules.len());
        }
        Ok(true)
    }

    /// Checks, once per schema, whether the server's binlog filters drop it: lazywf's own schema
    /// and each source schema of an enabled trigger (see [`filters`]). Advisory: a schema whose
    /// filters cannot be read is logged at debug and checked again on the next reload.
    async fn check_binlog_filters(&mut self, sources: &BTreeSet<String>) {
        let own: Option<String> = self
            .control
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .await
            .ok()
            .flatten()
            .flatten();
        let todo = filters::to_check(own.as_deref(), sources, &self.filter_checked);
        if todo.is_empty() {
            return;
        }
        let (own_todo, src_todo): (Vec<_>, Vec<_>) =
            todo.into_iter().partition(|(_, r)| *r == Role::Own);
        if !own_todo.is_empty() {
            match binlog::binlog_filters(&mut self.control).await {
                Ok(f) => self.record_filters(f, own_todo),
                Err(e) => debug!("reading the binlog filters of the status schema: {e:#}"),
            }
        }
        if !src_todo.is_empty() {
            let read = match tokio::time::timeout(
                Duration::from_secs(10),
                Conn::new(self.binlog_opts.clone()),
            )
            .await
            {
                Ok(Ok(mut c)) => {
                    let r = binlog::binlog_filters(&mut c).await;
                    c.disconnect().await.ok();
                    r
                }
                Ok(Err(e)) => Err(anyhow::Error::new(e)),
                Err(_) => Err(anyhow!("connecting timed out")),
            };
            match read {
                Ok(f) => self.record_filters(f, src_todo),
                Err(e) => debug!("reading the binlog filters of the source server: {e:#}"),
            }
        }
    }

    /// Records the checked schemas and warns about those the filters drop.
    fn record_filters(&mut self, (do_db, ignore_db): (String, String), todo: Vec<(String, Role)>) {
        for (schema, role) in todo {
            if let Some(filter) = filters::filter_of(&do_db, &ignore_db, &schema) {
                warn!(schema = %schema, "{}", filters::warning(role, &schema, filter));
                self.binlog_filtered.insert(schema.clone());
            }
            self.filter_checked.insert(schema);
        }
    }

    fn add_rule(&mut self, name: &str, rule: Arc<Rule>, initial: bool) {
        self.incarnations += 1;
        let frozen = rule.trigger_error.is_some();
        let error = rule.error.clone().or_else(|| rule.trigger_error.clone());
        // Resolve this rule's metric handles from the loaded config (bounded by config size).
        // Off (`None`), the rule keeps no handles and every flush/drop site is a skipped branch.
        let (mrule, mpair) = match &self.metrics {
            Some(reg) => (Some(reg.rule(name)), Some(reg.pair(name, &rule.def.target))),
            None => (None, None),
        };
        let st = RuleState {
            rule: rule.clone(),
            incarnation: self.incarnations,
            buffer: Buffer::default(),
            lane: None,
            prep: false,
            skip_until: None,
            frozen_at: frozen.then(|| self.settled.clone()),
            reset: None,
            // A finished or missing reset stays reported across restarts.
            reset_state: self
                .saved
                .get(name)
                .map(|s| s.reset_state.clone())
                .filter(|r| r.starts_with("done") || r.starts_with("missing"))
                .unwrap_or_default(),
            last_error: error.map(|e| (e, crate::time::unix_now_ms())),
            backoff: Backoff::default(),
            pending_clear: false,
            reset_request_seen: false,
            counters: Counters::default(),
            applied_ts: 0,
            strikes: HashMap::new(),
            written: None,
            dirty: true,
            mrule,
            mpair,
        };
        self.rules.insert(name.to_string(), st);
        if !initial {
            info!(rule = %name, "rule added");
            // A new rule's target has no history; one that ran before and comes back replays
            // what happened in between (reset only if the binlog no longer has it).
            match self.saved.get(name).and_then(|s| s.pos.clone()) {
                None => self.lose_continuity(name, "new rule".into()),
                Some(p) if p < self.settled => self.rewinds.push((name.to_string(), p)),
                Some(_) => {}
            }
            // A reset the removal interrupted — the rule was disabled while a step was in
            // flight — survives the re-add the way it survives a restart: `choose_start`
            // resumes exactly this saved state when the process comes back, and nothing about
            // an enable/disable says the operator asked for the reset to die. A row that also
            // carries `reset_requested` upgrades this run to the requested one below.
            if let Some(reason) = self
                .saved
                .get(name)
                .and_then(|s| unfinished_reset(&s.reset_state))
            {
                self.start_reset(
                    name,
                    format!("{reason} (resumed after the rule was added again)"),
                    false,
                );
            }
        }
        if rule.def.reset_requested {
            // A boot into a request that still stands: when the saved row is a reset that was
            // itself running on this request and its recorded step still belongs to these
            // steps, the new process continues that run at the step it died in — the steps
            // before it finished and their keys were confirmed by the target, and the
            // interrupted step re-runs from its own beginning (at-least-once). Every other
            // shape starts over from step 1, as a fresh request asks: the row carries no
            // version, so only what the saved line itself can vouch for serves as "this run
            // is the one that request started" (see [`resume_requested_reset`]). `initial`
            // only: a re-add or a reload has its own arms, and they keep restarting (#200,
            // #203).
            let resume = initial
                .then(|| {
                    self.saved.get(name).and_then(|s| {
                        resume_requested_reset(
                            &s.reset_state,
                            &rule.def.reset_steps,
                            &rule.fingerprint,
                        )
                    })
                })
                .flatten();
            self.start_reset(name, "reset_requested".into(), true);
            if let Some(step) = resume {
                if let Some(run) = self.rules.get_mut(name).and_then(|st| st.reset.as_mut()) {
                    run.step = step;
                    info!(
                        rule = %name,
                        "reset steps resume at step {}: the saved run was serving this request",
                        step + 1
                    );
                }
            }
        }
    }

    fn update_rule(&mut self, name: &str, rule: Arc<Rule>) {
        let pos = self.rule_pos(&self.rules[name]);
        let settled = self.settled.clone();
        // The target the rule pointed at before this reload: when a reload moves it to another
        // target, the old (rule, target) pair would otherwise keep exporting beside the new one.
        let old_target = self.rules.get(name).map(|s| s.rule.def.target.clone());
        // Re-resolve the (rule, target) handle: a reload may move the rule to another target. The
        // pair set stays bounded by the loaded config (only config names reach `pair`).
        let new_pair = self
            .metrics
            .as_ref()
            .map(|reg| reg.pair(name, &rule.def.target));
        if let (Some(reg), Some(old)) = (&self.metrics, old_target.as_deref()) {
            if old != rule.def.target {
                reg.remove_pair(name, old);
            }
        }
        let st = self.rules.get_mut(name).expect("present");
        let changed = st.rule.fingerprint != rule.fingerprint || st.rule.error != rule.error;
        st.rule = rule.clone();
        st.mpair = new_pair;
        if changed {
            st.backoff.ok();
            if let Some(e) = rule.error.clone().or_else(|| rule.trigger_error.clone()) {
                st.last_error = Some((e, crate::time::unix_now_ms()));
            }
            st.dirty = true;
        }
        let mut lost = None;
        match (rule.trigger_error.is_some(), st.frozen_at.is_some()) {
            (true, false) => st.frozen_at = Some(pos.min(settled)),
            (false, true) => {
                st.frozen_at = None;
                lost = Some("its triggers work again after an error".to_string());
            }
            _ => {}
        }
        let request = rule.def.reset_requested && !st.pending_clear;
        // The run in flight, seen before this request folds into it: a run that started on its own
        // (`requested` false) and is still going does not become the requested reset by having the
        // flag raised under it — its remainder runs the steps the rule was already running, and
        // the flag would then be cleared as if the reset that was asked for had run. Such a run
        // starts over from step 1, the way a changed rule restarts it below. A run that already
        // serves the request is left alone: the flag stays 1 until the steps succeed, so restarting
        // on every reload would be a reset that never finishes.
        let superseded = request
            && st
                .reset
                .as_ref()
                .is_some_and(|r| r.fingerprint == rule.fingerprint && !r.requested);
        let restart = st
            .reset
            .as_ref()
            .filter(|r| r.fingerprint != rule.fingerprint)
            .map(|r| (r.reason.clone(), r.requested || request));
        let running = st.reset.is_some();
        if let (Some(run), true) = (st.reset.as_mut(), request) {
            run.requested = true;
        }
        if let Some(reason) = lost {
            self.lose_continuity(name, reason);
        } else if let Some((reason, requested)) = restart {
            self.start_reset(
                name,
                format!("{reason}; restarted after the rule changed"),
                requested,
            );
        } else if request && (!running || superseded) {
            self.start_reset(name, "reset_requested".into(), true);
        }
    }

    fn rebuild_routes(&mut self) {
        let mut routes: HashMap<(String, String), Vec<(String, usize)>> = HashMap::new();
        for (name, st) in &self.rules {
            if st.rule.trigger_error.is_some() {
                continue;
            }
            for (i, t) in st.rule.triggers.iter().enumerate() {
                routes
                    .entry(self.norm(&t.def.schema, &t.def.table))
                    .or_default()
                    .push((name.clone(), i));
            }
        }
        // Shared `Arc`s: a route read costs one clone, not a `Vec` plus a name per entry.
        self.routes = routes.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
        // The cached routes hold trigger lists read from the rules these routes were just
        // rebuilt from: when the rules change, every cached route goes with them.
        self.route_cache.clear();
    }

    // ---- the stream ------------------------------------------------------------------------

    async fn stream(
        &mut self,
        start: Position,
        mut rx: mpsc::UnboundedReceiver<Done>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut backoff = STREAM_BACKOFF_FIRST;
        let mut pos = start;
        let mut item_failures = 0u32;
        let mut failed_at: Option<Position> = None;
        // The last reconnect ended on a 1236 that keeps the position (`binlog_unusable`): the
        // capped backoff must keep growing while that error repeats, even though each attempt
        // still delivers the events before the offending one.
        let mut kept_position = false;
        let silence = (self.cfg.heartbeat * 3).max(Duration::from_secs(15));
        self.health.set_phase("streaming");
        'reconnect: loop {
            self.health.beat();
            if !self.rewinds.is_empty() {
                self.begin_rewind().await?;
                pos = self.boundary.clone();
            }
            let connected = tokio::time::timeout(
                BINLOG_CONNECT_TIMEOUT,
                BinlogReader::connect(
                    self.binlog_opts.clone(),
                    self.cfg.server_id,
                    self.flavor,
                    &pos,
                    self.cfg.heartbeat,
                ),
            )
            .await
            .unwrap_or_else(|_| Err(anyhow!("timed out after {BINLOG_CONNECT_TIMEOUT:?}")));
            let mut reader = match connected {
                Ok(r) => {
                    // The server accepted the position, so a replay is under way.
                    self.rewound = None;
                    r
                }
                Err(e) => {
                    if binlog_unusable(&e) {
                        self.on_missing_binlog(&e).await?;
                        pos = self.boundary.clone();
                        continue 'reconnect;
                    }
                    if source::is_duplicate_server_id(&e) && self.reroll_server_id() {
                        continue 'reconnect;
                    }
                    if source::is_access_denied(&e) {
                        return Err(e);
                    }
                    if server_code(&e) == Some(1236) && !source::is_duplicate_server_id(&e) {
                        self.on_kept_binlog_error(&e, &pos, backoff);
                        kept_position = true;
                    } else if source::is_undecodable(&e) {
                        // Bytes this build cannot decode: every connect gets the same refusal, so
                        // the position is kept and the ladder keeps growing, as it does for a 1236
                        // the server cannot serve (see [`Engine::on_undecodable_stream_error`]).
                        self.on_undecodable_stream_error(&e, &pos, backoff);
                        kept_position = true;
                    } else {
                        warn!("binlog connect failed: {e:#}; retrying in {backoff:?}");
                    }
                    if self.idle(backoff, &mut rx, shutdown).await? {
                        break 'reconnect;
                    }
                    backoff = (backoff * 2).min(STREAM_BACKOFF_MAX);
                    continue 'reconnect;
                }
            };
            info!(position = %pos, "streaming the binlog");
            self.health.set_streaming(true);
            // Freshness (lazyview's review O-01, ruling 1): a connection knows nothing of the
            // source's clock until it asks, and has read nothing; the stamps are per connection,
            // so a reconnect that re-reads from the boundary starts afresh instead of keeping a
            // head this connection never saw. It starts believed at the head (the first heartbeat
            // proves it either way within a heartbeat period) and ages from the connect instant,
            // never from the epoch.
            let head_ts = self.clock.now();
            self.clock = SourceClock {
                offset: self.clock.offset,
                reading: true,
                at_head: true,
                head_ts,
                ..SourceClock::default()
            };
            self.refresh_source_clock().await;
            let mut last_event = Instant::now();
            let mut delivered = false;
            let mut hit_1236 = false;
            let mut hit_undecodable = false;
            loop {
                self.health.beat();
                let wake = self.next_wakeup();
                let mut quiet = false;
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => if *shutdown.borrow() { break 'reconnect; },
                    Some(done) = rx.recv() => self.on_done(done),
                    item = reader.next() => {
                        last_event = Instant::now();
                        match item {
                            Ok(Some(item)) => {
                                if !delivered {
                                    delivered = true;
                                    if !kept_position {
                                        backoff = STREAM_BACKOFF_FIRST;
                                    }
                                }
                                // The time a rows event was logged (statement start); 0 is unknown.
                                if let Item::Rows { event, .. } = &item {
                                    let ts = event.header().timestamp();
                                    if ts != 0 {
                                        self.last_event_ts = ts;
                                    }
                                }
                                self.caught_up = matches!(item, Item::Heartbeat);
                                if let Err(e) = self.on_item(&reader, item).await {
                                    match on_item_failure(&e) {
                                        ItemFailure::GiveUp => {
                                            // The rows of a watched table, refused by the decoder:
                                            // the ruling the copy carries keeps this fatal, and the
                                            // ten re-reads that used to precede the same exit each
                                            // failed on the same bytes.
                                            self.on_undecodable_rows(&e);
                                            return Err(e);
                                        }
                                        ItemFailure::Retry => {
                                            item_failures += 1;
                                            failed_at = Some(self.boundary.clone());
                                            if item_failures >= 10 {
                                                return Err(e.context("processing binlog events keeps failing"));
                                            }
                                            warn!("processing a binlog event: {e:#}; re-reading from {}", self.boundary);
                                            self.heal_control().await?;
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                warn!("the server closed the binlog stream; reconnecting");
                                break;
                            }
                            Err(e) => {
                                if binlog_unusable(&e) {
                                    self.on_missing_binlog(&e).await?;
                                } else if source::is_duplicate_server_id(&e) {
                                    self.reroll_server_id();
                                } else if source::is_undecodable(&e) {
                                    let at = self.boundary.clone();
                                    self.on_undecodable_stream_error(&e, &at, backoff);
                                    hit_undecodable = true;
                                    kept_position = true;
                                } else if server_code(&e) == Some(1236) {
                                    let at = self.boundary.clone();
                                    self.on_kept_binlog_error(&e, &at, backoff);
                                    hit_1236 = true;
                                    kept_position = true;
                                } else {
                                    warn!("binlog stream error: {e:#}; reconnecting");
                                }
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(wake.into()) => quiet = true,
                    _ = tokio::time::sleep_until((last_event + silence).into()) => {
                        warn!("no binlog event or heartbeat for {silence:?}; reconnecting");
                        break;
                    }
                }
                if quiet && !self.in_txn {
                    self.caught_up = true;
                }
                if failed_at.as_ref().is_some_and(|p| self.boundary > *p) {
                    item_failures = 0;
                    failed_at = None;
                }
                self.schedule();
                if !self.in_txn {
                    self.housekeeping().await?;
                    if !self.rewinds.is_empty() {
                        break; // reconnects from the positions of the rules enabled again
                    }
                }
            }
            self.health.set_streaming(false);
            // The reader stands between rounds: not reading, and no head proof survives a
            // reconnect — a source that keeps writing while nothing is read is a growing lag.
            self.clock.reading = false;
            self.clock.at_head = false;
            let _ = reader.close().await;
            // A partly read transaction is read again from its start.
            self.in_txn = false;
            self.table_ids.clear();
            self.txn_marks.clear();
            self.xa_current = None;
            pos = self.boundary.clone();
            // A connection that delivered events without a kept 1236 or a refused event: the
            // stream recovered, so the error is no longer current and the backoff starts over.
            // While a position the server cannot serve, or bytes this build cannot decode, repeats,
            // the capped backoff grows even though each attempt delivers the events before it.
            let sched = round_schedule(delivered, hit_1236, hit_undecodable, backoff);
            if sched.recovered {
                kept_position = false;
                self.on_stream_recovered();
            }
            backoff = sched.backoff;
            if self.idle(sched.pause, &mut rx, shutdown).await? {
                break 'reconnect;
            }
        }
        self.health.set_phase("stopping");
        self.health.set_streaming(false);
        // Let running actions finish, so their positions are saved.
        let deadline = Instant::now() + self.cfg.stop_grace;
        while self.rules.values().any(|s| s.lane.is_some()) {
            match tokio::time::timeout_at(deadline.into(), rx.recv()).await {
                Ok(Some(d)) => self.on_done(d),
                _ => break,
            }
        }
        if let Err(e) = self.persist(true).await {
            warn!("saving positions at shutdown: {e:#}");
        }
        lock::release(&mut self.control, &self.lock).await;
        info!("stopped");
        Ok(())
    }

    /// Waits `d` while still handling job results, flushes and housekeeping (the binlog is not
    /// connected meanwhile). `true`: shutdown requested.
    async fn idle(
        &mut self,
        d: Duration,
        rx: &mut mpsc::UnboundedReceiver<Done>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        let until = Instant::now() + d;
        loop {
            self.health.beat();
            let wake = self.next_wakeup().min(until);
            tokio::select! {
                biased;
                _ = shutdown.changed() => if *shutdown.borrow() { return Ok(true) },
                Some(done) = rx.recv() => self.on_done(done),
                _ = tokio::time::sleep_until(wake.into()) => {}
            }
            self.schedule();
            if let Err(e) = self.housekeeping().await {
                warn!("{e:#}");
            }
            if Instant::now() >= until {
                return Ok(false);
            }
        }
    }

    fn reroll_server_id(&mut self) -> bool {
        if self.cfg.server_id_fixed {
            warn!(
                server_id = self.cfg.server_id,
                "another replica connects with the same server id; set a unique LAZYWF_SERVER_ID"
            );
            return false;
        }
        let old = self.cfg.server_id;
        self.cfg.server_id = crate::config::random_server_id();
        info!(
            old,
            new = self.cfg.server_id,
            "server id collision with another replica; using a new random server id"
        );
        true
    }

    fn end_txn(&mut self, pos: Position) -> Position {
        self.in_txn = false;
        self.table_ids.clear();
        self.last_gtid = self.txn_gtid.take();
        if std::mem::take(&mut self.txn_foreign) {
            self.settled = pos.clone();
        }
        // A rule skips transactions ending at or before its `skip_until` (it read them already),
        // so the one ending exactly there is still skipped.
        for st in self.rules.values_mut() {
            if st.skip_until.as_ref().is_some_and(|s| *s < pos) {
                st.skip_until = None;
            }
        }
        std::mem::replace(&mut self.boundary, pos)
    }

    async fn on_item(&mut self, reader: &BinlogReader, item: Item) -> Result<()> {
        match item {
            Item::Gtid { gtid } => {
                self.txn_gtid = Some(gtid);
                self.in_txn = true;
            }
            Item::TableMap {
                table_id,
                schema,
                table,
                ..
            } => {
                self.in_txn = true;
                // The tool's own bookkeeping (status, commit fence) moves no position.
                let own = schema == self.cfg.schema
                    && matches!(table.as_str(), model::STATUS | model::FENCE);
                if !own {
                    self.txn_foreign = true;
                }
                let route = self.route(reader, table_id, schema, table).await?;
                self.table_ids.insert(table_id, route);
            }
            Item::Rows {
                table_id,
                kind,
                event,
            } => {
                self.in_txn = true;
                match self.table_ids.get(&table_id).cloned() {
                    Some(Route::Config { rules }) => {
                        self.config_dirty.get_or_insert_with(Instant::now);
                        if rules {
                            // A run that has just finished and waits to clear its flag consumed
                            // the request that stood when it started. A write that names that
                            // same rule and still asks for a reset is the operator asking again,
                            // and the clear must not erase what no run served: mark it, and
                            // `housekeeping` will not erase what it has not consumed. Only the
                            // rule the row names is marked — an unrelated rule's edit starts
                            // nothing here.
                            for n in Self::requested_rules(reader, table_id, kind, &event) {
                                if let Some(st) = self.rules.get_mut(&n) {
                                    if st.pending_clear {
                                        st.reset_request_seen = true;
                                    }
                                }
                            }
                        }
                    }
                    Some(Route::Watched(w)) => {
                        // Freshness (the ported clock, owner rule: integer stores only, on this
                        // arm alone): the rows about to be routed move the head the rules chase.
                        // Unwatched and own-pen tables never reach this arm, so they cannot put
                        // the head where the rules are not.
                        self.clock.note_fed(u64::from(reader.event_ts()));
                        let Engine {
                            txn_marks,
                            rules,
                            key_scratch,
                            render_warnings,
                            invalid_keys,
                            lossy_refusals,
                            ..
                        } = self;
                        let mut invalid = Vec::new();
                        let mut lossy = Vec::new();
                        let mut ctx = RowCtx {
                            marks: txn_marks,
                            rules,
                            scratch: key_scratch,
                            render_warnings,
                            invalid_keys,
                            invalid: &mut invalid,
                            lossy: &mut lossy,
                            lossy_refusals,
                        };
                        Engine::on_rows(&w, kind, || reader.rows(table_id, &event), &mut ctx)?;
                    }
                    _ => {}
                }
            }
            Item::XaStart { xid } => {
                self.in_txn = true;
                let marks = std::mem::take(&mut self.txn_marks);
                self.xa_current = Some((
                    xid,
                    XaTxn {
                        start: self.boundary.clone(),
                        marks,
                        prepared_at: Instant::now(),
                    },
                ));
            }
            Item::XaPrepare { xid, pos } => {
                // Prepared rows are not visible until XA COMMIT: their marks wait.
                let marks = std::mem::take(&mut self.txn_marks);
                let start = self.boundary.clone();
                self.end_txn(pos);
                let mut xa = match self.xa_current.take() {
                    Some((_, xa)) => xa,
                    None => XaTxn {
                        start,
                        marks: HashMap::new(),
                        prepared_at: Instant::now(),
                    },
                };
                for (r, m) in marks {
                    merge_marks(xa.marks.entry(r).or_default(), m);
                }
                xa.prepared_at = Instant::now();
                self.xa_pending.insert(xid, xa);
            }
            Item::XaEnd { xid, commit, pos } => {
                self.txn_foreign = true;
                let before = self.end_txn(pos.clone());
                match self.xa_pending.remove(&xid) {
                    Some(xa) if commit => self.apply(xa.marks, &pos, &xa.start),
                    Some(_) => {}
                    None if commit => {
                        warn!(%xid, "XA COMMIT of a transaction prepared before the start position: its changes are unknown");
                        // Rules that read this commit before (they skip it) lost nothing.
                        let names: Vec<String> = self
                            .rules
                            .iter()
                            .filter(|(_, st)| st.skip_until.as_ref().is_none_or(|s| pos > *s))
                            .map(|(n, _)| n.clone())
                            .collect();
                        for n in names {
                            self.lose_continuity(
                                &n,
                                format!(
                                    "XA COMMIT {xid} at {before} of a transaction prepared earlier"
                                ),
                            );
                        }
                    }
                    None => {}
                }
            }
            Item::Commit { pos } => {
                let marks = std::mem::take(&mut self.txn_marks);
                self.xa_current = None;
                let start = self.end_txn(pos.clone());
                self.apply(marks, &pos, &start);
            }
            Item::Statement { schema, query, pos } => {
                self.txn_foreign = true;
                self.end_txn(pos.clone());
                self.on_statement(&schema, &query, &pos);
            }
            // The vendored reader surfaces an INCIDENT event as its own item and logs nothing
            // itself; warn here, at the one place that saw it before, and change no state —
            // the reader used to fold INCIDENT into `Other` after this very warning.
            Item::Incident { pos } => {
                warn!(at = %pos, "INCIDENT event in the binlog: the source reports possibly lost changes");
            }
            Item::Heartbeat => {
                // The head proof (the ported clock): the payload's position is where the
                // server's dump stood; the stream is at its head only if the engine has applied
                // to exactly there, outside a transaction (the server drains its socket at once,
                // so a backlog may still sit in the read buffer while it heartbeats). Either way
                // the heartbeat is the last moment the server was known empty: a rule with
                // nothing read yet ages from here, not from the epoch.
                self.clock.head_ts = self.clock.now();
                if heartbeat_proves_head(
                    self.in_txn,
                    &self.boundary,
                    reader.position(),
                    reader.heartbeat_head(),
                ) {
                    self.clock.at_head = true;
                }
            }
            Item::Other => {}
        }
        Ok(())
    }

    /// The `Watched` route for `schema`.`table`, or `Ignored`.
    ///
    /// `schema` and `table` are the event's own `String`s, moved into the route key, so a
    /// repeat of a table the cache still answers allocates nothing of its own; the hit check
    /// is [`RouteCache::probe`]. The tables of the engine's own schema are answered by name
    /// alone, before either cache is touched.
    async fn route(
        &mut self,
        reader: &BinlogReader,
        table_id: u64,
        schema: String,
        table: String,
    ) -> Result<Route> {
        if schema == self.cfg.schema && model::is_own_table(table.as_str()) {
            return Ok(if model::is_config_table(&table) {
                Route::Config {
                    rules: table == model::RULE,
                }
            } else {
                Route::Ignore
            });
        }
        let tme = reader.table_map(table_id);
        // What the event says about its own columns: `None` on a server with
        // `binlog_row_metadata=MINIMAL`, the event's names on a FULL one. A cached route may
        // only answer an event whose own names match what it was built from (see `RouteCache`).
        let names = tme.and_then(table_map_names);
        let key = (schema, table);
        if let Some(route) =
            self.route_cache
                .probe(&self.catalog, self.lower_names, &key, names.as_deref())
        {
            return Ok(route);
        }
        let nkey = self.norm(&key.0, &key.1);
        let Some(list) = self.routes.get(&nkey).cloned() else {
            return Ok(Route::Ignore);
        };
        match self
            .catalog
            .layout(&mut self.control, &key.0, &key.1, tme)
            .await
        {
            Ok(layout) => {
                self.mismatched.remove(&nkey);
                let triggers: Vec<(String, Arc<Rule>, usize)> = list
                    .iter()
                    .filter_map(|(n, i)| self.rules.get(n).map(|s| (n.clone(), s.rule.clone(), *i)))
                    .collect();
                self.check_columns(&layout, &key.0, &key.1, &triggers);
                let triggers = self
                    .refuse_generated_reads(&nkey, &layout, &key.0, &key.1, triggers)
                    .await?;
                let triggers = self.refuse_binary_keys(&layout, &key.0, &key.1, triggers);
                let any_full = triggers.iter().any(|(_, rule, i)| {
                    matches!(rule.triggers[*i].key, Some(KeyTemplate::Full(_)))
                });
                let any_on = Self::watched_kinds(&triggers);
                let w = Arc::new(Watched {
                    layout,
                    table: format!("{}.{}", key.0, key.1),
                    triggers,
                    any_full,
                    any_on,
                });
                // Cached until the rules change (`rebuild_routes` clears this cache), a reset
                // starts (`start_reset` does too), or the layout behind it is dropped:
                // `forget_columns` takes the entry with the catalog entry and `generated_first`,
                // and every other catalog invalidation is caught by the generation the entry
                // was stamped with. The probe's identity check is what a stale entry must
                // still fail on its own.
                self.route_cache
                    .insert(nkey, w.clone(), self.catalog.generation());
                Ok(Route::Watched(w))
            }
            Err(e) if e.downcast_ref::<LayoutMismatch>().is_some() => {
                self.forget_columns(&key.0, &key.1);
                // Once per mismatch, not for every transaction read before the DDL.
                if self.mismatched.insert(nkey) {
                    warn!("{e:#}");
                    for (n, _) in list.iter() {
                        self.lose_continuity(n, format!("{e}"));
                    }
                }
                Ok(Route::Ignore)
            }
            Err(e) => Err(e),
        }
    }

    /// The index and name of the table's first generated column, of either kind, on a server that
    /// leaves those columns out of its row images; `Ok(None)` everywhere else and for a table
    /// without one. Read once per table, like the layout cache it lives and dies with.
    ///
    /// `key` is the table's identity as [`Engine::route`] knows it, so a cached answer costs no
    /// allocation; `schema` and `table` are the names to ask the server about.
    async fn first_generated_column(
        &mut self,
        key: &(String, String),
        schema: &str,
        table: &str,
    ) -> Result<Option<(usize, String)>> {
        if !self.maria_skips_generated {
            return Ok(None);
        }
        if let Some(found) = self.generated_first.get(key) {
            return Ok(found.clone());
        }
        // An image whose columns cannot be read is an image that must not be read: a server that
        // will not answer them is an error, as it is for the layout read above, and the caller
        // retries. A table that is really gone fails there too, not here.
        let found = read_generated_first(&mut self.control, schema, table).await?;
        self.generated_first.insert(key.clone(), found.clone());
        Ok(found)
    }

    /// Drops what is cached about a table's columns, so both the layout and the position of its
    /// first generated column are read again: a DDL changed the table, or its layout did not match
    /// the binlog. The built route goes with them — a DDL changes the columns a `Watched` decodes.
    fn forget_columns(&mut self, schema: &str, table: &str) {
        self.catalog.invalidate(schema, table);
        let key = self.norm(schema, table);
        if table == "*" {
            self.generated_first.retain(|(s, _), _| *s != key.0);
        } else {
            self.generated_first.remove(&key);
        }
        self.route_cache.forget(&key);
    }

    /// Refuses the triggers that would read what the row images do not carry, and returns those
    /// that may keep running.
    ///
    /// On MariaDB 11.4+ an UPDATE or DELETE image holds one value less than the table map names
    /// (its generated columns, VIRTUAL or STORED alike, are not written), and the reader pairs
    /// values with columns by position, so the first generated column and every column after it
    /// are read as their neighbour's value. Such a trigger marks keys that do not describe the
    /// row, so it marks nothing at all and carries the reason on `/status` the way an unknown
    /// column does. Every read of an image counts, not only the key: `changed_columns` (a
    /// comparison of the two images that would compare shifted values), the columns a `Simple`
    /// key names and the columns a `keys_sql` binds, and the whole row for a Handlebars key,
    /// which may name any column of it and is therefore refused for a table that has a generated
    /// column at all. The rule's own `selects` and its doc template read the selects' rows, not
    /// the image, so they are not reads of it.
    /// Temporary, see [`maria_skips_generated`].
    async fn refuse_generated_reads(
        &mut self,
        key: &(String, String),
        layout: &Layout,
        schema: &str,
        table: &str,
        triggers: Vec<(String, Arc<Rule>, usize)>,
    ) -> Result<Vec<(String, Arc<Rule>, usize)>> {
        let Some((from, gen_col)) = self.first_generated_column(key, schema, table).await? else {
            return Ok(triggers);
        };
        let mut kept = Vec::with_capacity(triggers.len());
        for (name, rule, ti) in triggers {
            let t = &rule.triggers[ti];
            // Every read of an image the trigger has: the same columns `check_columns` looks at
            // (its `changed_columns`, its key's columns and the columns its `keys_sql` binds),
            // and the whole row for a Full key. A Handlebars key is not scanned for names: it may
            // read any column, so a table with a generated column refuses it outright.
            let mut reads: Vec<(&str, usize)> = Vec::new();
            for c in &t.def.changed_columns {
                if let Some(i) = layout.index(c) {
                    reads.push((c.as_str(), i));
                }
            }
            if let Some(KeyTemplate::Simple(lines)) = &t.key {
                for seg in lines.iter().flatten() {
                    if let template::Seg::Col(c) = seg {
                        if let Some(i) = layout.index(c) {
                            reads.push((c, i));
                        }
                    }
                }
            }
            if let Some(ks) = &t.keys_sql {
                for c in &ks.columns {
                    if let Some(i) = layout.index(c) {
                        reads.push((c.as_str(), i));
                    }
                }
            }
            let full = matches!(t.key, Some(KeyTemplate::Full(_)));
            let Some(read) = unreadable_read(&reads, full, from) else {
                kept.push((name, rule, ti));
                continue;
            };
            // Refused whether or not it was reported: what it would mark is wrong either way.
            if self.generated_refusals.insert((name.clone(), t.def.id)) {
                let msg = match &read {
                    ShortRead::Col(c) => format!(
                        "trigger {}: `{schema}`.`{table}` reads `{c}`; this MariaDB leaves the \
                         generated column `{gen_col}` (VIRTUAL or STORED) out of its UPDATE and \
                         DELETE row images, and every column after it moves up a place, so \
                         lazywf cannot read `{c}` and refuses the trigger (move `{gen_col}` \
                         after every column this trigger reads, or read a plain column the \
                         application keeps)",
                        t.def.id
                    ),
                    ShortRead::Full => format!(
                        "trigger {}: `{schema}`.`{table}` uses a Handlebars key template, which \
                         can read any column of the row, including the generated column \
                         `{gen_col}` and the columns after it that this MariaDB's UPDATE and \
                         DELETE row images shift; lazywf refuses the trigger (move `{gen_col}` \
                         after every column the template reads, or use a plain {{{{new.col}}}} \
                         key over columns before it)",
                        t.def.id
                    ),
                };
                warn!(rule = %name, "{msg}");
                if let Some(st) = self.rules.get_mut(&name) {
                    st.last_error = Some((msg, crate::time::unix_now_ms()));
                    st.dirty = true;
                }
            }
        }
        Ok(kept)
    }

    /// Refuses the triggers that build a key out of bytes no charset can turn into exact
    /// text, and returns those that may keep running.
    ///
    /// The key text names the document at the target and in the buffer. A `binary`-charset
    /// column — BINARY, VARBINARY, the BLOB family, a text type with the BINARY attribute —
    /// and the geometry types come over the binlog as opaque bytes, and the lossy UTF-8
    /// decode renders every impossible byte as U+FFFD: two rows can then share a key, and
    /// the DELETE of one carries the other's key and removes its document (measured on both
    /// test servers: a BINARY(16) key deleted a live row's document). The catalog answers
    /// the charset and the data type that make this known when the route is built (see
    /// [`Layout::key_unsafe`]), so such a trigger never marks at all, the way an unreadable
    /// read does not (see [`Engine::refuse_generated_reads`]), and the rule carries the
    /// reason on `/status`.
    fn refuse_binary_keys(
        &mut self,
        layout: &Layout,
        schema: &str,
        table: &str,
        triggers: Vec<(String, Arc<Rule>, usize)>,
    ) -> Vec<(String, Arc<Rule>, usize)> {
        let mut kept = Vec::with_capacity(triggers.len());
        for (name, rule, ti) in triggers {
            // `get`, not indexing: a route's trigger index is trusted, and the panic-site
            // gate keeps the one allowlisted index to `refuse_generated_reads` alone.
            let Some(t) = rule.triggers.get(ti) else {
                kept.push((name, rule, ti));
                continue;
            };
            let bad = key_column_reads(t, layout)
                .into_iter()
                .find_map(|i| layout.key_unsafe(i).map(|why| (i, why)));
            let Some((i, why)) = bad else {
                kept.push((name, rule, ti));
                continue;
            };
            // Refused whether or not it was reported: the key it would mark is wrong either way.
            if self.binary_refusals.insert((name.clone(), t.def.id)) {
                let col = layout.column_name(i).unwrap_or("?");
                let ty = layout.column_type(i).unwrap_or("binary");
                // The workaround has to work on the server the rule runs on. A generated column
                // projects the bytes exactly, but a MariaDB that leaves generated columns out of
                // its UPDATE and DELETE images cannot key on one either (see
                // `refuse_generated_reads`), so there the advice is the plain column the
                // application keeps; both shapes carry `HEX`, which is the point of them.
                let advice = if self.maria_skips_generated {
                    format!(
                        "key on a text or number column, or on a plain column the application \
                         keeps, for example `{col}_hex CHAR(64)` written as HEX(`{col}`) and a \
                         key over that column (a generated column cannot carry a key on this \
                         server: its UPDATE and DELETE images leave generated columns out)"
                    )
                } else {
                    format!(
                        "key on a text or number column, or on a STORED generated column that \
                         projects it, for example `AS (HEX(`{col}`)) STORED` and a key over \
                         that column"
                    )
                };
                let msg = format!(
                    "rule `{name}` trigger {}: `{schema}`.`{table}` keys on column `{col}` \
                     of type `{ty}` — {why}, so its key would be replacement characters, not \
                     the row's value: the row marks nothing and its DELETE can remove another \
                     row's document; lazy-wire-fire refuses the trigger ({advice})",
                    t.def.id
                );
                warn!(rule = %name, "{msg}");
                if let Some(st) = self.rules.get_mut(&name) {
                    st.last_error = Some((msg, crate::time::unix_now_ms()));
                    st.dirty = true;
                }
            }
        }
        kept
    }

    /// Reports (once) trigger columns the table does not have: such a trigger marks nothing.
    fn check_columns(
        &mut self,
        layout: &Layout,
        schema: &str,
        table: &str,
        triggers: &[(String, Arc<Rule>, usize)],
    ) {
        for (name, rule, ti) in triggers {
            let t = &rule.triggers[*ti];
            let mut cols: Vec<&str> = t.def.changed_columns.iter().map(String::as_str).collect();
            if let Some(KeyTemplate::Simple(lines)) = &t.key {
                for seg in lines.iter().flatten() {
                    if let template::Seg::Col(c) = seg {
                        cols.push(c);
                    }
                }
            }
            if let Some(ks) = &t.keys_sql {
                cols.extend(ks.columns.iter().map(String::as_str));
            }
            for c in cols {
                if layout.index(c).is_some()
                    || !self
                        .column_warnings
                        .insert((name.clone(), t.def.id, c.to_string()))
                {
                    continue;
                }
                let msg = format!(
                    "trigger {}: `{schema}`.`{table}` has no column `{c}`",
                    t.def.id
                );
                warn!(rule = %name, "{msg}");
                if let Some(st) = self.rules.get_mut(name) {
                    st.last_error = Some((msg, crate::time::unix_now_ms()));
                    st.dirty = true;
                }
            }
        }
    }

    /// The row kinds any trigger of the table fires on: the union of their `on` flags,
    /// computed once at route time (see [`Watched::watches`]).
    fn watched_kinds(triggers: &[(String, Arc<Rule>, usize)]) -> On {
        let mut on = On::default();
        for (_, rule, i) in triggers {
            let t = &rule.triggers[*i].def.on;
            on.insert |= t.insert;
            on.update |= t.update;
            on.delete |= t.delete;
        }
        on
    }

    /// One `Rows` event of a watched table: decode its row images, mark every row's keys for
    /// every trigger that is `on` for this kind, and report the invalid keys. When no trigger
    /// of the table fires on this kind the event is passed over undecoded: every effect of the
    /// per-row path — the transaction's marks, the rows' count, the warnings, the invalid-key
    /// reports — sits behind the `on` check in `row_keys`, so nothing an unwatched kind does
    /// today is lost by skipping it (the hot-path audit's finding 4; `skip_kinds_tests` proves
    /// the skipped state identical to the decoded one, for a decodable and for an undecodable
    /// image alike). The skip costs no safety either: `reader.rows` decodes against the
    /// TABLE_MAP this transaction's `map_event` already stored and moves no position, its
    /// errors are row-image-vs-table-map mismatches, and the next watched-kind event of this
    /// table decodes against the same map, so a real layout mismatch still surfaces there;
    /// the event's framing (header, size, payload) was checked in `reader.next()` before this
    /// arm, and the server reports a corrupt event when it streams its own binlog. The client
    /// does not verify the event's CRC32 to begin with — `Event::read` (mysql_common) only
    /// stores the trailing checksum and `calc_checksum` runs on write alone — so the skip
    /// forgoes no checksum. The decode comes in as a closure, so a test can see whether it
    /// ran without a binlog reader.
    fn on_rows(
        w: &Watched,
        kind: RowsKind,
        decode: impl FnOnce() -> Result<Vec<(Option<BinlogRow>, Option<BinlogRow>)>>,
        ctx: &mut RowCtx<'_>,
    ) -> Result<()> {
        if !w.watches(kind) {
            return Ok(());
        }
        let rows = decode()?;
        // The pieces borrowed as one bag, re-borrowed per row, so a row's keys are built with
        // no borrow of the whole engine (see `row_keys`).
        for (before, after) in &rows {
            let mut row = RowCtx {
                marks: &mut *ctx.marks,
                rules: &mut *ctx.rules,
                scratch: &mut *ctx.scratch,
                render_warnings: &mut *ctx.render_warnings,
                invalid_keys: &mut *ctx.invalid_keys,
                invalid: &mut *ctx.invalid,
                lossy: &mut *ctx.lossy,
                lossy_refusals: &mut *ctx.lossy_refusals,
            };
            Self::row_keys(w, kind, before, after, &mut row);
        }
        // The keys whose bytes no charset could be read as: the row marked nothing under them
        // (see `row_keys`), and this is where that becomes visible — once per trigger, on the
        // rule, the way a key that cannot be rendered is.
        for (name, trigger, column) in std::mem::take(&mut *ctx.lossy) {
            if !ctx.lossy_refusals.insert((name.clone(), trigger)) {
                continue;
            }
            let ty = w
                .layout
                .index(&column)
                .and_then(|i| w.layout.column_type(i))
                .unwrap_or("?");
            let msg = format!(
                "trigger {trigger}: `{}`.`{column}` of type `{ty}` holds bytes its charset does \
                 not make text of, so its key came out through the decoder's replacement \
                 character: lazy-wire-fire marks no key it cannot read, and neither a lookup, a \
                 document nor a delete goes out for such a row (key on a column in a charset \
                 lazy-wire-fire decodes — utf8mb4 or latin1 — or on a plain column the \
                 application keeps, for example `{column}_hex CHAR(64)` written as \
                 HEX(`{column}`))",
                w.table
            );
            warn!(rule = %name, "{msg}");
            if let Some(st) = ctx.rules.get_mut(&name) {
                st.last_error = Some((msg, crate::time::unix_now_ms()));
                st.dirty = true;
            }
        }
        for (name, trigger, k) in std::mem::take(&mut *ctx.invalid) {
            *ctx.invalid_keys += 1;
            let shown: String = k.escape_debug().take(120).collect();
            if *ctx.invalid_keys <= 10 || (*ctx.invalid_keys).is_multiple_of(1000) {
                warn!(rule = %name, "trigger {trigger}: key \"{shown}\" holds control characters and is ignored");
            }
            if let Some(st) = ctx.rules.get_mut(&name) {
                st.last_error = Some((
                    format!("trigger {trigger}: key \"{shown}\" holds control characters and is ignored"),
                    crate::time::unix_now_ms(),
                ));
                st.dirty = true;
            }
        }
        Ok(())
    }

    /// The rules a write of `lazy_wire_fire_rule` names when their after-image still asks for a
    /// reset: the `name` of each inserted or updated row whose `reset_requested` is 1. A delete
    /// has no after-image, and a write that changes nothing makes no row event at all, so a
    /// leftover 1 nobody touched names nothing here — which is right: an ask the table itself
    /// cannot show is not an ask. The two columns sit at their [`model::RULE_COLS`] positions
    /// (the engine created this table), and only an image with exactly that many columns is read;
    /// a table map that names its own columns must agree with those positions.
    fn requested_rules(
        reader: &BinlogReader,
        table_id: u64,
        kind: RowsKind,
        event: &Event,
    ) -> Vec<String> {
        let mut out = Vec::new();
        if matches!(kind, RowsKind::Delete) {
            return out;
        }
        let Some(name_i) = model::RULE_COLS.iter().position(|c| *c == "name") else {
            return out;
        };
        let Some(flag_i) = model::RULE_COLS
            .iter()
            .position(|c| *c == "reset_requested")
        else {
            return out;
        };
        if let Some(names) = reader.table_map(table_id).and_then(table_map_names) {
            if names.get(name_i).map(String::as_str) != Some("name")
                || names.get(flag_i).map(String::as_str) != Some("reset_requested")
            {
                return out;
            }
        }
        let Ok(rows) = reader.rows(table_id, event) else {
            return out;
        };
        for (_, after) in rows {
            let Some(row) = after else { continue };
            if row.len() != model::RULE_COLS.len() {
                return out;
            }
            if !matches!(row.as_ref(flag_i), Some(BinlogValue::Value(My::Int(1)))) {
                continue;
            }
            if let Some(BinlogValue::Value(My::Bytes(b))) = row.as_ref(name_i) {
                out.push(String::from_utf8_lossy(b).into_owned());
            }
        }
        out
    }

    /// One row of a watched table: every trigger that is `on` for this kind marks the row's
    /// images — its keys located in the rule's buffer, its `keys_sql` query kept for the flush.
    /// The engine's pieces come in [`RowCtx`], borrowed rather than through `self`, so a test
    /// can feed a row without an engine; a row of a table whose triggers use Simple keys costs
    /// no allocation at all (`KeyScratch` holds the key text, the images are an array, and row
    /// objects exist only for a table with a Full-key trigger).
    fn row_keys(
        w: &Watched,
        kind: RowsKind,
        before: &Option<BinlogRow>,
        after: &Option<BinlogRow>,
        ctx: &mut RowCtx<'_>,
    ) {
        let op = match kind {
            RowsKind::Insert => key::Op::Insert,
            RowsKind::Update => key::Op::Update,
            RowsKind::Delete => key::Op::Delete,
        };
        // Both images at hand, the unused slot `None` (an insert or a delete has one): an
        // array, so a row costs no `Vec`.
        let images: [Option<&BinlogRow>; 2] = match kind {
            RowsKind::Insert => [after.as_ref(), None],
            RowsKind::Delete => [before.as_ref(), None],
            RowsKind::Update => [before.as_ref(), after.as_ref()],
        };
        // The images as objects, which only a Full key reads: allocated for a table that has a
        // Full-key trigger and never for one that has none (see `Watched::any_full`).
        let mut objects: Option<[Option<Value>; 2]> = w.any_full.then(|| [None, None]);
        let RowCtx {
            marks,
            rules,
            scratch,
            render_warnings,
            invalid,
            lossy,
            ..
        } = ctx;
        for (name, rule, ti) in &w.triggers {
            let t = &rule.triggers[*ti];
            let on = match kind {
                RowsKind::Insert => t.def.on.insert,
                RowsKind::Update => t.def.on.update,
                RowsKind::Delete => t.def.on.delete,
            };
            if !on {
                continue;
            }
            if kind == RowsKind::Update && !t.def.changed_columns.is_empty() {
                let (Some(b), Some(a)) = (images[0], images[1]) else {
                    continue;
                };
                let changed = t
                    .def
                    .changed_columns
                    .iter()
                    .any(|c| match w.layout.index(c) {
                        Some(i) => w.layout.changed(b, a, i),
                        None => true,
                    });
                if !changed {
                    continue;
                }
            }
            // The rule's name is cloned only for the transaction's first row that marks it.
            if marks.get_mut(name.as_str()).is_none() {
                marks.insert(name.clone(), Marks::default());
            }
            let marks = marks
                .get_mut(name.as_str())
                .expect("inserted just above if it was missing");
            marks.rows += 1;
            for ii in 0..2 {
                let Some(img) = images[ii] else {
                    continue;
                };
                match &t.key {
                    Some(KeyTemplate::Simple(lines)) => {
                        'line: for line in lines {
                            let k = scratch.begin();
                            for seg in line {
                                match seg {
                                    template::Seg::Lit(s) => k.push_str(s),
                                    template::Seg::Col(c) => {
                                        let Some(i) = w.layout.index(c) else {
                                            continue 'line;
                                        };
                                        let v = w.layout.value(img, i);
                                        if v.is_null() {
                                            continue 'line;
                                        }
                                        // The decode-point guard: bytes no charset can be read
                                        // as text make a key that does not describe this row —
                                        // it would mark nothing and its DELETE could carry
                                        // another row's key. Such a line marks nothing at all
                                        // (see [`Layout::key_is_lossy`]).
                                        if w.layout.key_is_lossy(img, i, &v) {
                                            lossy.push((name.clone(), t.def.id, c.clone()));
                                            continue 'line;
                                        }
                                        template::push_text(&v, k);
                                    }
                                }
                            }
                            let k = k.trim();
                            if template::valid_key(k) {
                                let (s, key) =
                                    rules.get_mut(name).expect("present").buffer.locate(k);
                                marks.ids.insert(s, key);
                            } else if !k.is_empty() {
                                invalid.push((name.clone(), t.def.id, k.to_string()));
                            }
                        }
                    }
                    Some(KeyTemplate::Full(lines)) => {
                        let objs = objects
                            .as_mut()
                            .expect("a Full-key trigger means the table has objects");
                        let obj = objs[ii].get_or_insert_with(|| w.layout.object(img)).clone();
                        // The row's context, built rather than serialised: `json!` of a
                        // `Value` deep-copies it, so this one object — owned once by the
                        // cache above, and read again by the pass that follows — was copied
                        // three more times to fill three roots that all hold it. Built by
                        // hand it is copied twice and moved into the third (the hot-path
                        // audit's finding 8, the row's own object).
                        let mut ctx = Map::new();
                        ctx.insert("old".into(), obj.clone());
                        ctx.insert("new".into(), obj.clone());
                        ctx.insert("row".into(), obj);
                        ctx.insert("op".into(), Value::String(op_text(op).into()));
                        ctx.insert("table".into(), Value::String(w.table.clone()));
                        let ctx = Value::Object(ctx);
                        for tpl in lines {
                            match rule.templates.render(tpl, &ctx) {
                                Ok(s) => {
                                    let k = s.trim();
                                    // A rendered key holding a replacement character may hold
                                    // the decoder's own: the columns the template names are
                                    // asked, but only now — a key without one is never looked
                                    // at (see [`Layout::key_is_lossy`]).
                                    if k.contains(char::REPLACEMENT_CHARACTER) {
                                        let bad = key_column_reads(t, &w.layout)
                                            .into_iter()
                                            .find(|&i| {
                                                w.layout.key_is_lossy(
                                                    img,
                                                    i,
                                                    &w.layout.value(img, i),
                                                )
                                            })
                                            .and_then(|i| w.layout.column_name(i))
                                            .map(str::to_string);
                                        if let Some(col) = bad {
                                            lossy.push((name.clone(), t.def.id, col));
                                            continue;
                                        }
                                    }
                                    if template::valid_key(k) {
                                        let (s2, key) =
                                            rules.get_mut(name).expect("present").buffer.locate(k);
                                        marks.ids.insert(s2, key);
                                    } else if !k.is_empty() {
                                        invalid.push((name.clone(), t.def.id, k.to_string()));
                                    }
                                }
                                Err(e) => {
                                    // Reported on the rule, so a key that cannot be rendered
                                    // does not pass unnoticed.
                                    **render_warnings += 1;
                                    if **render_warnings <= 10
                                        || (**render_warnings).is_multiple_of(1000)
                                    {
                                        warn!(rule = %name, "trigger {} key: {e:#}", t.def.id);
                                    }
                                    if let Some(st) = rules.get_mut(name) {
                                        st.last_error = Some((
                                            format!("trigger {} key: {e:#}", t.def.id),
                                            crate::time::unix_now_ms(),
                                        ));
                                        st.dirty = true;
                                    }
                                }
                            }
                        }
                    }
                    None => {}
                }
                if let Some(ks) = &t.keys_sql {
                    // The bound values are the row's decoded text: one of them made of
                    // replacement characters would answer the user's statement with another
                    // row's `_key`, so the query does not go out at all.
                    let mut params = Vec::with_capacity(ks.columns.len());
                    let mut bad = None;
                    for c in &ks.columns {
                        match w.layout.index(c) {
                            Some(i) => {
                                let v = w.layout.value(img, i);
                                if w.layout.key_is_lossy(img, i, &v) {
                                    bad = Some(c.clone());
                                    break;
                                }
                                params.push(v);
                            }
                            None => params.push(Value::Null),
                        }
                    }
                    if let Some(col) = bad {
                        lossy.push((name.clone(), t.def.id, col));
                        continue;
                    }
                    let q = KeyQuery {
                        trigger: t.def.id,
                        sql: ks.sql.clone(),
                        params,
                    };
                    marks.queries.insert(q.id(), q);
                }
            }
        }
    }
    /// Applies one committed transaction's marks (it ended at `end` and began after `since`).
    fn apply(&mut self, marks: HashMap<String, Marks>, end: &Position, since: &Position) {
        let now = Instant::now();
        let ts = self.last_event_ts;
        for (name, m) in marks {
            let Some(st) = self.rules.get_mut(&name) else {
                continue;
            };
            if st.skip_until.as_ref().is_some_and(|s| end <= s) {
                continue;
            }
            st.counters.events += m.rows;
            if m.ids.is_empty() && m.queries.is_empty() {
                continue;
            }
            // The newest release of this buffer: what the flush fences on.
            st.buffer.core.released(end);
            if let Some(t) = st.reset.as_mut().and_then(|r| r.touched.as_mut()) {
                t.extend(text_set(
                    st.buffer
                        .keys
                        .render(m.ids.as_map())
                        .iter()
                        .map(String::as_str),
                ));
            }
            for (s, set) in m.ids.into_pairs() {
                for v in set.ints.to_sorted_vec() {
                    st.buffer.mark(s, key::Key::Int(v), since);
                }
                for t in set.keys {
                    st.buffer.mark(s, key::Key::Tuple(t), since);
                }
            }
            for (id, q) in m.queries.into_iter() {
                st.buffer.add_query(id, q, since);
            }
            // The old `hold` kept the MINIMUM of the positions it was given. `mark` and
            // `item` pin the origin only when there is none, and marks do not all arrive in
            // binlog order: an XA's rows are applied at its COMMIT with the XA's START as
            // `since`, which is older than everything marked between PREPARE and COMMIT.
            // So lower the floor here (retried ids re-pin themselves in `done`).
            st.buffer.core.pin(since);
            st.buffer.hold_ts(ts);
            st.buffer.first.get_or_insert(now);
            st.buffer.last = Some(now);
        }
    }

    fn on_statement(&mut self, schema: &str, query: &str, pos: &Position) {
        if let Some(((cs, ct), (os, ot))) = crate::sql::table_like(query, schema) {
            if self.shadows.len() < MAX_SHADOWS {
                let copy = self.norm(&cs, &ct);
                let original = self.norm(&os, &ot);
                self.shadows.insert(copy, original);
            }
        }
        // An online schema change ends by swapping its copy in, in one RENAME (`t TO old,
        // copy TO t`). The copy was created LIKE `t` and filled through row events, so it holds
        // the same rows: the rules of `t` keep their continuity.
        let pairs: Vec<_> = crate::sql::renames(query, schema)
            .into_iter()
            .map(|((fs, ft), (ts, tt))| (self.norm(&fs, &ft), self.norm(&ts, &tt)))
            .collect();
        let mut kept: HashSet<(String, String)> = HashSet::new();
        for (k, (from, to)) in pairs.iter().enumerate() {
            let moved_away = pairs[..k].iter().any(|(f, _)| f == to);
            if moved_away && self.shadows.get(from) == Some(to) {
                info!(
                    "`{}`.`{}` was swapped for `{}`.`{}`, a copy created LIKE it (an online schema change): continuity kept",
                    to.0, to.1, from.0, from.1
                );
                kept.insert(to.clone());
            }
        }
        let mut reset: HashSet<String> = HashSet::new();
        for (s, t, effect) in crate::sql::ddl_tables(query, schema) {
            self.forget_columns(&s, &t);
            if s == self.cfg.schema && (model::is_config_table(&t) || t == "*") {
                self.config_dirty.get_or_insert_with(Instant::now);
            }
            if effect != crate::sql::DdlEffect::Replace {
                continue;
            }
            let (ns, nt) = self.norm(&s, &t);
            // Renamed or dropped: no longer a copy waiting to be swapped in.
            self.shadows.remove(&(ns.clone(), nt.clone()));
            if kept.contains(&(ns.clone(), nt.clone())) {
                continue;
            }
            // A table named twice (`t TO old, new TO t`) resets its rules once.
            let hit: Vec<String> = self
                .routes
                .iter()
                .filter(|((rs, rt), _)| *rs == ns && (nt == "*" || *rt == nt))
                .flat_map(|(_, v)| v.iter().map(|(n, _)| n.clone()))
                .filter(|n| reset.insert(n.clone()))
                .collect::<IndexSet<_>>()
                .into_iter()
                .collect();
            for n in hit {
                self.lose_continuity(
                    &n,
                    format!(
                        "`{s}`.`{t}` was replaced without row events ({})",
                        first_words(query)
                    ),
                );
            }
        }
        self.note_statement_dml(schema, query, pos);
    }

    /// DML that arrived as a statement-format QUERY event: with `binlog_format=ROW` a QUERY
    /// event carries DDL and administrative statements only, so an `INSERT` / `REPLACE` /
    /// `UPDATE` / `DELETE` here was logged in statement format by the executing session (that
    /// session's own doing, not the engine's). Its rows are not in the stream, so the rules that
    /// read the table missed this change and their targets are wrong until they are rebuilt: the
    /// WARN is what tells the operator. One line per (schema, table) per
    /// [`STMT_DML_LOG_INTERVAL`] with the count of the events folded into it, and never the
    /// statement text — in statement format it carries the row values, and lazywf logs nothing
    /// secret. Row events never reach the caller's arm, so they pay nothing for this check; the
    /// rare statement event pays a token scan and one map entry.
    fn note_statement_dml(&mut self, schema: &str, query: &str, pos: &Position) {
        let Some(d) = crate::sql::statement_dml(query, schema) else {
            return;
        };
        self.stmt_dml_ignored += 1;
        if let Some(m) = &self.metrics {
            m.statement_event_ignored();
        }
        let key = self.norm(&d.schema, &d.table);
        let now = Instant::now();
        let Self {
            stmt_dml_last,
            stmt_dml_capped,
            ..
        } = self;
        let Some(slot) = stmt_dml_slot(stmt_dml_last, key, stmt_dml_capped) else {
            return;
        };
        let Some(count) = stmt_dml_gate(slot, now) else {
            return; // folded into the next line for this table
        };
        warn_statement_dml(&d, pos, count);
    }

    // ---- scheduling ---------------------------------------------------------------------

    fn next_wakeup(&self) -> Instant {
        let now = Instant::now();
        let mut t = now + Duration::from_secs(3600);
        if !self.in_txn {
            t = t
                .min(self.last_persist + self.cfg.status_every)
                .min(self.next_poll);
            if let Some(d) = self.config_dirty {
                t = t.min(d);
            }
        }
        for st in self.rules.values() {
            let blocked = st.backoff.blocked(now).max(
                self.target_backoff
                    .get(&st.rule.def.target)
                    .and_then(|b| b.blocked(now)),
            );
            if st.lane.is_none() {
                if let Some(due) = st.buffer.due(st.rule.def.debounce, st.rule.def.max_delay) {
                    t = t.min(blocked.map_or(due, |b| b.max(due)));
                }
            }
            if let Some(r) = &st.reset {
                if let Some(b) = r.backoff.blocked(now) {
                    t = t.min(b);
                }
            }
        }
        t.max(now + Duration::from_millis(5))
    }

    fn schedule(&mut self) {
        let now = Instant::now();
        // Borrowed as pieces: the reset of one rule is driven with its own state, so iterating
        // the rules needs no clone of a name. A rule can fire a flush instead, which takes the
        // engine, so its name is collected there — the usual iteration has no such rule.
        let Engine {
            rules,
            target_backoff,
            env,
            tx,
            ..
        } = self;
        let mut due: Vec<String> = Vec::new();
        for (name, st) in rules.iter_mut() {
            let target_blocked = target_backoff
                .get(&st.rule.def.target)
                .and_then(|b| b.blocked(now))
                .is_some();
            let lane_free =
                st.lane.is_none() && st.backoff.blocked(now).is_none() && !target_blocked;
            let fire = lane_free
                && matches!(
                    st.buffer.due(st.rule.def.debounce, st.rule.def.max_delay),
                    Some(at) if at <= now
                );
            if fire {
                due.push(name.clone());
            } else {
                Self::drive_reset(st, env, tx, name, now, lane_free);
            }
        }
        // No rule driven above can have taken the lane of one of these: a reset touches its own
        // rule only, so each of these is still free and due, as checked just now.
        for name in due {
            self.spawn_flush(&name);
        }
    }

    fn spawn_flush(&mut self, name: &str) {
        let spec = FenceSpec {
            gtid: self.last_gtid.clone(),
            pos: self.boundary.clone(),
        };
        let (env, tx) = (self.env.clone(), self.tx.clone());
        let st = self.rules.get_mut(name).expect("present");
        let buf = &mut st.buffer;
        // The whole buffer in one generation, taken at once (unbounded, as always).
        // `schedule` calls this only when the buffer is due (so it is not empty) and the
        // lane is free (so no generation is in flight, see the report); a scheduling slip
        // must still not take the engine down: skipping one tick costs nothing.
        if !buf.core.begin() {
            warn!(rule = %name, "flush skipped: empty or a generation is already in flight");
            return;
        }
        // `begin` moved the non-empty pending sets into the generation, so this cannot be
        // `None` (that would mean `begin` took pending ids and the generation lost them).
        let taken = buf
            .core
            .take(usize::MAX, usize::MAX)
            .expect("begin moved the pending ids into the generation");
        let maps: BTreeMap<key::SpaceId, buffer::IdSet> = taken.ids.into_iter().collect();
        // The flush's own key list, in one copy: the job reads it, and the armed fallback
        // below reads it too (the audit's finding 11, the keys half — see `fire_keys`).
        let keys = Arc::new(buf.keys.render(&maps));
        // Shared with the job (which reads the payloads to resolve `keys_sql`) and with the
        // lane (which puts them back if the flush fails): one copy of every parameter set for
        // the whole flush instead of a deep clone of it here.
        let queries = Arc::new(buf.take_query_payloads(taken.items));
        // Only the keys with strikes are remembered (to clear them once confirmed).
        let struck = if st.strikes.is_empty() {
            Vec::new()
        } else {
            keys.iter()
                .filter(|k| st.strikes.contains_key(*k))
                .cloned()
                .collect()
        };
        st.lane = Some(Lane::Flush {
            since_ts: buf.since_ts,
            first: buf.first,
            struck,
            queries: Arc::clone(&queries),
        });
        buf.since_ts = 0;
        buf.first = None;
        buf.last = None;
        let (rule, incarnation, name) = (st.rule.clone(), st.incarnation, name.to_string());
        // The panic fallback, built only if the task dies: an armed `Outcome` here would
        // copy every key on every flush, including the ones that go out well. The job owns
        // the list, so the fallback holds another handle to the same one.
        let armed = Arc::clone(&keys);
        let failed = move || Outcome::Flush {
            fired: Fired::failed((*armed).clone()),
            keep_queries: true,
            resolved: Vec::new(),
        };
        let mrule = st.mrule.clone();
        spawn_job(tx, name, incarnation, failed, async move {
            let start = Instant::now();
            let out = flush_job(&env, &rule, spec, keys, &queries).await;
            // The flush duration is the job's own wall time (fence + resolve + fire); recorded
            // through the pre-resolved handle: one bucket bump, no name to the registry.
            if let Some(m) = &mrule {
                m.observe_flush(start.elapsed());
            }
            out
        });
    }

    /// Starts the next job of a rule's reset, if one can run now.
    /// Drives one rule's reset steps. Takes the rule's state rather than the engine, so
    /// `schedule` can drive every rule without cloning its name; `lane_free`: no flush of this
    /// rule is in flight (a step that sends takes the rule's lane, the one a flush uses too).
    fn drive_reset(
        st: &mut RuleState,
        env: &Arc<Env>,
        tx: &mpsc::UnboundedSender<Done>,
        name: &str,
        now: Instant,
        lane_free: bool,
    ) {
        let Some(run) = st.reset.as_mut() else { return };
        if st.prep || run.backoff.blocked(now).is_some() || st.rule.error.is_some() {
            return;
        }
        let (env, tx) = (Arc::clone(env), Clone::clone(tx));
        let rule = st.rule.clone();
        let (incarnation, rid) = (st.incarnation, run.id);
        let rname = name.to_string();
        let steps = rule.def.reset_steps.len();
        // The line a running step writes carries a digest of the rule it was walked under,
        // so only that same def lets a restart skip the finished steps (see
        // [`resume_requested_reset`]).
        let fp = format!("fp{}", fingerprint_digest(&rule.fingerprint));
        loop {
            if run.step >= steps {
                info!(rule = %name, "reset steps done ({})", run.reason);
                if run.requested {
                    st.pending_clear = true;
                }
                st.reset = None;
                st.reset_state = format!("done: {}", crate::time::iso(crate::time::unix_now()));
                st.dirty = true;
                return;
            }
            let i = run.step;
            let step = &rule.def.reset_steps[i];
            st.reset_state = format!(
                "running: step {}/{} {} {}: {}",
                i + 1,
                steps,
                fp,
                step.name,
                run.reason
            );
            let batch = match rule.def.fire_per {
                model::FirePer::Key => 100,
                model::FirePer::Batch => rule.def.batch_max_keys.unwrap_or(1000).min(10_000),
            };
            match &mut run.phase {
                Phase::Waiting => return,
                Phase::Start => match &step.kind {
                    StepKind::Once => {
                        if !lane_free {
                            return;
                        }
                        run.phase = Phase::Waiting;
                        st.lane = Some(Lane::Reset);
                        let (rule, tx, rname) = (rule.clone(), tx.clone(), rname.clone());
                        let failed = Outcome::Reset {
                            run: rid,
                            fired: Fired::failed(Vec::new()),
                        };
                        let failed = move || failed;
                        spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                            let reset = json!({"step": rule.def.reset_steps[i].name, "keys": []});
                            let fired =
                                action::fire_once(&rule, &rule::step_template(i), reset).await;
                            Outcome::Reset { run: rid, fired }
                        });
                        return;
                    }
                    StepKind::Snapshot => {
                        run.phase = Phase::Waiting;
                        st.prep = true;
                        let (rule, env, tx, rname) =
                            (rule.clone(), env.clone(), tx.clone(), rname.clone());
                        let failed = Outcome::Snapshot {
                            run: rid,
                            keys: Err("internal error".into()),
                        };
                        let failed = move || failed;
                        spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                            let keys = action::snapshot_keys(&env, &rule)
                                .await
                                .map_err(|e| format!("{e:#}"));
                            Outcome::Snapshot { run: rid, keys }
                        });
                        return;
                    }
                    StepKind::Stray { .. } => {
                        run.phase = Phase::Waiting;
                        run.touched = Some(buffer::IdSet::default());
                        st.prep = true;
                        let (rule, env, tx, rname) =
                            (rule.clone(), env.clone(), tx.clone(), rname.clone());
                        let failed = Outcome::Strays {
                            run: rid,
                            per_ep: Err("internal error".into()),
                        };
                        let failed = move || failed;
                        spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                            let per_ep = action::list_strays(&env, &rule, i)
                                .await
                                .map_err(|e| format!("{e:#}"));
                            Outcome::Strays { run: rid, per_ep }
                        });
                        return;
                    }
                },
                Phase::Batches {
                    keys,
                    total,
                    done,
                    batch: pending,
                } => {
                    if pending.is_none() {
                        let chunk = keys.take(batch);
                        if chunk.is_empty() {
                            run.step += 1;
                            run.phase = Phase::Start;
                            continue;
                        }
                        *pending = Some(idset_texts(&chunk));
                    }
                    if !lane_free {
                        return;
                    }
                    let chunk = pending.clone().expect("set above");
                    st.reset_state = format!(
                        "running: step {}/{} {} {}: {}/{} keys: {}",
                        i + 1,
                        steps,
                        fp,
                        step.name,
                        done,
                        total,
                        run.reason
                    );
                    st.lane = Some(Lane::Reset);
                    let (rule, env, tx, rname) =
                        (rule.clone(), env.clone(), tx.clone(), rname.clone());
                    let failed = Outcome::Reset {
                        run: rid,
                        fired: Fired::failed(Vec::new()),
                    };
                    let failed = move || failed;
                    spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                        let reset = json!({"step": rule.def.reset_steps[i].name, "keys": chunk});
                        let fired = action::fire_keys(
                            &env,
                            &rule,
                            &chunk,
                            &rule.step_action(i),
                            reset,
                            None,
                        )
                        .await;
                        Outcome::Reset { run: rid, fired }
                    });
                    return;
                }
                Phase::Strays {
                    per_ep,
                    ep,
                    done: _,
                    batch: pending,
                } => {
                    if pending.is_none() {
                        // Keys marked since the step began are not strays.
                        let touched = run.touched.as_ref();
                        let mut next = Vec::new();
                        while next.is_empty() && *ep < per_ep.len() {
                            let taken = per_ep[*ep].take(batch);
                            if taken.is_empty() {
                                *ep += 1;
                                continue;
                            }
                            next = taken
                                .to_keys()
                                .into_iter()
                                .filter(|k| touched.is_none_or(|t| !t.contains(k)))
                                .map(|k| key_text(&k))
                                .collect();
                        }
                        if next.is_empty() {
                            run.touched = None;
                            run.step += 1;
                            run.phase = Phase::Start;
                            continue;
                        }
                        *pending = Some(next);
                    }
                    if !lane_free {
                        return;
                    }
                    let chunk = pending.clone().expect("set above");
                    st.lane = Some(Lane::Reset);
                    let e = *ep;
                    let (rule, tx, rname) = (rule.clone(), tx.clone(), rname.clone());
                    let failed = Outcome::Reset {
                        run: rid,
                        fired: Fired::failed(Vec::new()),
                    };
                    let failed = move || failed;
                    spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                        let fired = action::fire_strays(&env, &rule, i, e, &chunk).await;
                        Outcome::Reset { run: rid, fired }
                    });
                    return;
                }
            }
        }
    }

    fn on_done(&mut self, d: Done) {
        let max = self.cfg.max_backoff;
        let now_ms = crate::time::unix_now_ms();
        let settle_now = Instant::now();
        // Borrowed as pieces before the rule's own state (the [`schedule`] idiom): the settle
        // stamp below reads the target's hold and the fed head while holding the rule.
        let Engine {
            rules,
            target_backoff,
            clock,
            settled,
            ..
        } = self;
        let Some(st) = rules.get_mut(&d.rule) else {
            return;
        };
        if st.incarnation != d.incarnation {
            return;
        }
        let target = st.rule.def.target.clone();
        let mut target_failed = None;
        let mut target_ok = false;
        match d.outcome {
            Outcome::Flush {
                fired,
                keep_queries,
                resolved,
            } => {
                let Some(Lane::Flush {
                    since_ts,
                    first,
                    struck,
                    queries,
                }) = st.lane.take()
                else {
                    return;
                };
                st.counters.actions_ok += fired.ok;
                st.counters.actions_failed += fired.failed;
                st.counters.select_ms = fired.select_time.as_millis() as u64;
                if let Some(t) = st.reset.as_mut().and_then(|r| r.touched.as_mut()) {
                    t.extend(text_set(resolved.iter().map(String::as_str)));
                }
                let keyless = fired.error.is_some()
                    && fired.failed_keys.is_empty()
                    && fired.rejected.is_empty();
                // A permanent refusal (the target refused keys on their own), read before the
                // strike logic consumes `rejected` and splits it into retry-or-drop.
                let had_refusals = !fired.rejected.is_empty();
                let (retry, dropped) = st.judge(fired.rejected, fired.keys_done > 0);
                st.report_dropped(&d.rule, &target, &dropped, now_ms);
                let mpair = st.mpair.clone();
                let mut again = IdSets::default();
                for k in fired.failed_keys.iter().chain(retry.iter()) {
                    let (s, key) = st.buffer.locate(k);
                    again.insert(s, key);
                }
                for k in &struck {
                    if !again.contains_text(&st.buffer.keys, k) {
                        st.strikes.remove(k);
                    }
                }
                // `expect_met`: nothing stays pending and the flush was not keyless → the position
                // advanced. The same condition picks the success arm of the match below.
                let expect_met = again.is_empty() && !keyless;
                match fired.error {
                    _ if expect_met => {
                        st.backoff.ok();
                        target_ok = !fired.target_error;
                        if st.buffer.core.in_flight() {
                            // Nothing goes back: this completes the flush's only chunk.
                            st.buffer.core.done(Vec::new(), Vec::new());
                        }
                        // Freshness: this flush went through clean. If the rule now holds nothing
                        // back (no buffer, lane, reset, error hold, freeze, target hold), its
                        // applied stamp moves to the head this connection was fed to. `persist`
                        // restamps every tick (that is what ages a rule that marks nothing); this
                        // stamp is what carries a rule that flushes the last keys of a backlog.
                        let target_blocked = target_backoff
                            .get(&st.rule.def.target)
                            .and_then(|b| b.blocked(settle_now))
                            .is_some();
                        if rule_settled(st, target_blocked, settle_now)
                            && clock.fed_ts > st.applied_ts
                        {
                            st.applied_ts = clock.fed_ts;
                        }
                    }
                    err => {
                        let err = err.unwrap_or_else(|| "some actions failed".into());
                        // Back into the buffer, carrying the position the generation was
                        // taken at (the core re-pins it; today's `hold` of the lane's since).
                        let items: Vec<key::Item<String>> = if keep_queries {
                            queries
                                .iter()
                                .map(|(id, _)| key::Item::User(id.clone()))
                                .collect()
                        } else {
                            Vec::new()
                        };
                        if st.buffer.core.in_flight() {
                            st.buffer.core.done(again.into_pairs(), items);
                        } else {
                            // A rebase drained the generation meanwhile: the refused keys
                            // go back at the position everything rebased to.
                            let origin = settled.clone();
                            for (s, set) in again.into_pairs() {
                                for v in set.ints.to_sorted_vec() {
                                    st.buffer.core.mark_int(s, v, &origin);
                                }
                                for t in set.keys {
                                    st.buffer.core.mark_tuple(s, t, &origin);
                                }
                            }
                            for i in items {
                                st.buffer.core.item(i, &origin);
                            }
                        }
                        if keep_queries {
                            // The job held the other handle and is finished with it, so the
                            // usual case reclaims the payloads where they are; only a handle
                            // that still has another owner copies, and that is this failure
                            // path alone.
                            let payloads =
                                Arc::try_unwrap(queries).unwrap_or_else(|a| (*a).clone());
                            st.buffer.put_back_queries(payloads);
                        }
                        st.buffer.hold_ts(since_ts);
                        let now = Instant::now();
                        st.buffer.first = [first, st.buffer.first]
                            .into_iter()
                            .flatten()
                            .min()
                            .or(Some(now));
                        st.buffer.last = st.buffer.last.or(Some(now));
                        let delay = st.backoff.fail(max);
                        if fired.target_error {
                            target_failed = Some(err.clone());
                        }
                        warn!(rule = %d.rule, "flush failed: {err}; retrying in {delay:?}");
                        st.last_error = Some((err, now_ms));
                    }
                }
                // One counter bump per flush completion, on the pre-resolved handle. A refusal
                // (permanent) dominates; otherwise expect-met is a success, else a retry.
                if let Some(p) = &mpair {
                    p.flush(FlushOutcome::classify(had_refusals, expect_met));
                }
                st.dirty = true;
            }
            Outcome::Reset { run: rid, fired } => {
                if matches!(st.lane, Some(Lane::Reset)) {
                    st.lane = None;
                }
                st.counters.actions_ok += fired.ok;
                st.counters.actions_failed += fired.failed;
                if !st.reset.as_ref().is_some_and(|r| r.id == rid) {
                    return;
                }
                let keyless = fired.error.is_some()
                    && fired.failed_keys.is_empty()
                    && fired.rejected.is_empty();
                let (retry, dropped) = st.judge(fired.rejected, fired.keys_done > 0);
                st.report_dropped(&d.rule, &target, &dropped, now_ms);
                let done = !keyless && fired.failed_keys.is_empty() && retry.is_empty();
                let run = st.reset.as_mut().expect("checked above");
                match fired.error {
                    _ if done => {
                        run.backoff.ok();
                        target_ok = true;
                        match &mut run.phase {
                            Phase::Waiting => {
                                run.step += 1;
                                run.phase = Phase::Start;
                            }
                            Phase::Batches { done, batch, .. }
                            | Phase::Strays { done, batch, .. } => {
                                *done += batch.take().map_or(0, |b| b.len() as u64);
                            }
                            Phase::Start => {}
                        }
                    }
                    err => {
                        let err = err.unwrap_or_else(|| "some actions failed".into());
                        if matches!(run.phase, Phase::Waiting) {
                            run.phase = Phase::Start;
                        }
                        let delay = run.backoff.fail(max);
                        if fired.target_error {
                            target_failed = Some(err.clone());
                        }
                        warn!(rule = %d.rule, "reset step {} failed: {err}; retrying in {delay:?}", run.step + 1);
                        st.last_error = Some((err, now_ms));
                    }
                }
                st.dirty = true;
            }
            Outcome::Snapshot { run: rid, keys } => {
                st.prep = false;
                let Some(run) = st.reset.as_mut().filter(|r| r.id == rid) else {
                    return;
                };
                match keys {
                    Ok(keys) => {
                        info!(rule = %d.rule, "reset step {}: {} keys in the snapshot", run.step + 1, keys.len());
                        let total = keys.len() as u64;
                        run.phase = Phase::Batches {
                            keys,
                            total,
                            done: 0,
                            batch: None,
                        };
                    }
                    Err(e) => {
                        run.phase = Phase::Start;
                        let delay = run.backoff.fail(max);
                        warn!(rule = %d.rule, "reading the snapshot keys failed: {e}; retrying in {delay:?}");
                        st.last_error = Some((e, now_ms));
                    }
                }
                st.dirty = true;
            }
            Outcome::Strays { run: rid, per_ep } => {
                st.prep = false;
                let Some(run) = st.reset.as_mut().filter(|r| r.id == rid) else {
                    return;
                };
                match per_ep {
                    Ok(per_ep) => {
                        info!(rule = %d.rule, "reset step {}: {} stray keys", run.step + 1, per_ep.iter().map(|s| s.len() as u64).sum::<u64>());
                        run.phase = Phase::Strays {
                            per_ep,
                            ep: 0,
                            done: 0,
                            batch: None,
                        };
                    }
                    Err(e) => {
                        run.phase = Phase::Start;
                        run.touched = None;
                        let delay = run.backoff.fail(max);
                        warn!(rule = %d.rule, "listing the target's keys failed: {e}; retrying in {delay:?}");
                        st.last_error = Some((e, now_ms));
                    }
                }
                st.dirty = true;
            }
        }
        if target_ok {
            if let Some(b) = target_backoff.get_mut(&target) {
                b.ok();
            }
        }
        if target_failed.is_some() {
            self.target_backoff.entry(target).or_default().fail(max);
        }
    }

    // ---- housekeeping ---------------------------------------------------------------------

    async fn housekeeping(&mut self) -> Result<()> {
        let now = Instant::now();
        let clears: Vec<String> = if self.clear_retry.is_some_and(|t| now < t) {
            Vec::new()
        } else {
            self.rules
                .iter()
                .filter(|(_, s)| s.pending_clear)
                .map(|(n, _)| n.clone())
                .collect()
        };
        for n in clears {
            // The finished run must not have its clear erase a request raised after it: a write
            // of this rule's row arrived while `pending_clear` stood (see `on_item`) and no run
            // ever served it. That request is consumed by running the reset again — the flag
            // stays 1 and the new run clears it when its own steps succeed.
            //
            // What this cannot see is a write whose row event has not arrived by the time this
            // UPDATE goes out: the row reads 1, no marker stands, and the clear erases an ask a
            // millisecond old. Telling that apart needs a per-row version the table does not
            // carry (neither the rule row nor the status table holds one), so an operator who
            // raises the flag while a reset is running raises it again once the row says `done`.
            if self.rules.get(&n).is_some_and(|s| s.reset_request_seen) {
                info!(rule = %n, "reset_requested was raised again while the finished reset waited to clear: running it again");
                if let Some(st) = self.rules.get_mut(&n) {
                    st.pending_clear = false;
                }
                self.start_reset(
                    &n,
                    "reset_requested raised again while the finished reset was being cleared"
                        .to_string(),
                    true,
                );
                continue;
            }
            match self
                .control
                .exec_drop(
                    "UPDATE `lazy_wire_fire_rule` SET `reset_requested` = 0 WHERE `name` = ?",
                    (&n,),
                )
                .await
            {
                Ok(()) => {
                    if let Some(st) = self.rules.get_mut(&n) {
                        st.pending_clear = false;
                    }
                }
                Err(e) => {
                    warn!(rule = %n, "clearing reset_requested: {e}; retrying in 5 s");
                    self.clear_retry = Some(now + Duration::from_secs(5));
                    self.heal_control().await?;
                }
            }
        }
        let dirty = self.config_dirty.is_some_and(|t| now >= t);
        if dirty || now >= self.next_poll {
            if dirty {
                // The change was read from the binlog: wait until it is visible.
                let spec = FenceSpec {
                    gtid: self.last_gtid.clone(),
                    pos: self.boundary.clone(),
                };
                if let Err(e) = action::fence(&self.env, &spec).await {
                    debug!("config fence: {e:#}");
                }
            }
            self.config_dirty = None;
            self.next_poll = now + self.cfg.config_poll;
            if let Err(e) = self.reload_config(false).await {
                warn!("reloading the configuration: {e:#}; retrying");
                self.config_dirty = Some(now + Duration::from_secs(1));
                self.heal_control().await?;
            }
            self.schedule();
        }
        // Freshness: re-measure the source's clock on this cadence (next to the persist tick,
        // never per event). The throttle moves on every attempt, failure included.
        if self.last_clock.elapsed() >= CLOCK_REFRESH {
            self.last_clock = Instant::now();
            self.refresh_source_clock().await;
        }
        if now >= self.last_persist + self.cfg.status_every {
            if let Err(e) = self.persist(false).await {
                if e.to_string().contains("instance lock") {
                    return Err(e);
                }
                warn!("writing lazy_wire_fire_status: {e:#}");
                self.heal_control().await?;
            }
        }
        Ok(())
    }

    /// Reconnects the control connection if it is broken, taking the instance lock again.
    async fn heal_control(&mut self) -> Result<()> {
        if self.control.ping().await.is_ok() {
            return Ok(());
        }
        let conn = match tokio::time::timeout(Duration::from_secs(10), Conn::new(self.opts.clone()))
            .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                warn!("control connection: {e}");
                return Ok(());
            }
            Err(_) => return Ok(()),
        };
        self.control = conn;
        lock::reacquire(&mut self.control, &self.lock).await?;
        info!("control connection re-established");
        Ok(())
    }

    /// Asks the source its own time (`SELECT UNIX_TIMESTAMP()`), once, and keeps the difference
    /// to the local clock: a rule's `source_lag_seconds` is then source clock minus source clock,
    /// so an engine whose host clock is off by minutes still reports 0 for a rule that is caught
    /// up, and a rule that is behind shows the real distance. One statement per (re)connect and
    /// per [`CLOCK_REFRESH`] tick — never per event, and no new write anywhere. A failed question
    /// keeps the last offset: the figures ride the previous measurement until the next tick.
    async fn refresh_source_clock(&mut self) {
        match self
            .control
            .query_first::<i64, _>("SELECT UNIX_TIMESTAMP()")
            .await
        {
            Ok(Some(src_now)) if src_now > 0 => {
                // Saturating, not raw: the offset is a measured difference, and a server that
                // answers with a nonsense clock must not be able to reach an arithmetic panic.
                self.clock.offset = src_now.saturating_sub(crate::time::unix_now() as i64);
            }
            Ok(_) => debug!("asking the source its time: no answer; keeping the last offset"),
            Err(e) => debug!("asking the source its time: {e:#}; keeping the last offset"),
        }
    }

    /// Writes the positions that moved (and every row now and then).
    async fn persist(&mut self, force: bool) -> Result<()> {
        self.last_persist = Instant::now();
        lock::verify(&mut self.control, &self.lock).await?;
        let mut rows = Vec::new();
        let mut summary = Map::new();
        let now = Instant::now();
        // Freshness (the settle stamp, mirroring the reference's re-publish on the housekeeping
        // tick): a rule with nothing left held back has applied everything it was handed, so its
        // stamp moves to the head this connection was fed to. Stamping here on every status tick,
        // not only when a flush completes, is what keeps a rule that marks nothing (a
        // `changed_columns` filter, a quiet watched table) honest instead of ageing an old stamp.
        // Not while a transaction is half read: its rows are read but no rule has been handed
        // them yet (rules are marked at COMMIT), and stamping then would claim them applied.
        {
            let Engine {
                rules,
                target_backoff,
                clock,
                in_txn,
                ..
            } = self;
            // Rows read but not handed to a rule (lazywf has no drain queue): the reading may not
            // claim caught up while a transaction is in hand. See [`SourceClock::pending_read`].
            clock.pending_read = *in_txn;
            for st in rules.values_mut() {
                let target_blocked = target_backoff
                    .get(&st.rule.def.target)
                    .and_then(|b| b.blocked(now))
                    .is_some();
                if rule_settled(st, target_blocked, now) && !*in_txn && clock.fed_ts > st.applied_ts
                {
                    st.applied_ts = clock.fed_ts;
                }
            }
        }
        // The head proof for this publish ([`SourceClock::lag_with`]): the reference's heartbeat,
        // or the reader keeping up with a source that keeps writing. `cfg.heartbeat` is the period
        // the dump is configured with (the value handed to `BinlogReader::connect`), the interval
        // within which an idle dump would have heartbeated; the floor keeps a degenerate 0 from
        // turning the proof into an equality against the second.
        let head = self.clock.at_head
            || read_proves_head(
                u64::from(self.last_event_ts),
                self.clock.now(),
                self.cfg.heartbeat.max(Duration::from_secs(1)),
            );
        for (name, st) in &self.rules {
            let pos = self.rule_pos(st);
            let lag = self.rule_lag_ms(st);
            // The honest per-rule lag on the source's clock (see [`SourceClock`]): the reader's
            // distance from the source's own timestamps, per rule. It is not `lag_ms`, which
            // stays the local-clock age of the rule's own pending marks.
            let target_blocked = self
                .target_backoff
                .get(&st.rule.def.target)
                .and_then(|b| b.blocked(now))
                .is_some();
            let source_lag_seconds =
                self.clock
                    .lag_with(st.applied_ts, rule_settled(st, target_blocked, now), head);
            // The pending gauge reads the same source the status table shows (`pending_keys`,
            // which reads the dedup buffer's `pending_len`); the value is pushed to the atomic.
            if let Some(m) = &st.mrule {
                m.set_pending(st.buffer.pending_keys() as u64);
                m.set_source_lag(source_lag_seconds);
            }
            let reset_state = if st.reset_state.is_empty() {
                "none".to_string()
            } else {
                st.reset_state.clone()
            };
            summary.insert(
                name.clone(),
                json!({
                    "position": pos.to_string(),
                    "lag_ms": lag,
                    "source_lag_seconds": source_lag_seconds,
                    "pending_keys": st.buffer.pending_keys() as u64,
                    "firing": st.lane.is_some(),
                    "events": st.counters.events,
                    "actions_ok": st.counters.actions_ok,
                    "actions_failed": st.counters.actions_failed,
                    "select_ms": st.counters.select_ms,
                    "reset": reset_state,
                    "error": st.rule.error.as_ref().or(st.rule.trigger_error.as_ref()),
                    "last_error": st.last_error.as_ref().map(|(e, at)| json!({"message": e, "at_ms": at})),
                }),
            );
            let moved = st
                .written
                .as_ref()
                .is_none_or(|(p, at)| *p != pos || at.elapsed() >= STATUS_HEARTBEAT);
            if force || st.dirty || moved {
                rows.push(status::Write {
                    rule: name.clone(),
                    pos,
                    lag_ms: Some(lag),
                    reset_state,
                    last_error: st.last_error.as_ref().map(|(e, _)| e.clone()),
                    last_error_at: st.last_error.as_ref().map(|(_, at)| *at),
                    server_identity: None,
                });
            }
        }
        let resume = self.resume_pos();
        if force
            || self
                .star_written
                .as_ref()
                .is_none_or(|(p, at)| *p != resume || at.elapsed() >= STATUS_HEARTBEAT)
        {
            rows.push(status::Write {
                rule: INSTANCE_ROW.into(),
                pos: resume.clone(),
                lag_ms: Some(self.stream_lag_ms()),
                reset_state: String::new(),
                last_error: None,
                last_error_at: None,
                server_identity: Some(self.identity.clone()),
            });
        }
        // The instance-wide binlog lag: the same `stream_lag_ms` the status table's instance row
        // writes (the reader's distance behind the server, seconds).
        if let Some(reg) = &self.metrics {
            reg.set_binlog_lag(self.stream_lag_ms() as f64 / 1000.0);
        }
        self.health.set_status(status_json(
            &self.cfg.instance,
            &resume,
            self.stream_lag_ms(),
            self.stream_error.as_deref(),
            self.stmt_dml_ignored,
            self.undecodable.seen,
            summary,
            self.binlog_filtered.iter().cloned().collect(),
        ));
        if rows.is_empty() {
            return Ok(());
        }
        status::write(&mut self.control, &self.cfg.instance, &rows).await?;
        for w in rows {
            if w.rule == INSTANCE_ROW {
                self.star_written = Some((w.pos.clone(), now));
                self.saved.entry(w.rule.clone()).or_default().pos = Some(w.pos);
            } else if let Some(st) = self.rules.get_mut(&w.rule) {
                st.dirty = false;
                st.written = Some((w.pos.clone(), now));
                self.saved.entry(w.rule).or_default().pos = Some(w.pos);
            }
        }
        Ok(())
    }
}

/// Runs a job on its own task; if it panics, a failure is still reported, so the rule's lane
/// is never left taken.
/// Runs one job of one rule and reports its outcome to the engine. `failed` is the outcome
/// to report when the task itself dies (it panicked, or was cancelled): a closure, so a
/// fallback that has to copy the job's data pays for it only on that path — the flush's
/// fallback holds a handle to the flush's own key list and clones it here, if at all.
fn spawn_job(
    tx: mpsc::UnboundedSender<Done>,
    rule: String,
    incarnation: u64,
    failed: impl FnOnce() -> Outcome + Send + 'static,
    job: impl std::future::Future<Output = Outcome> + Send + 'static,
) {
    tokio::spawn(async move {
        let outcome = match tokio::spawn(job).await {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(rule = %rule, "job failed unexpectedly: {e}");
                failed()
            }
        };
        let _ = tx.send(Done {
            rule,
            incarnation,
            outcome,
        });
    });
}

/// The reason of a reset a run never finished, from a `reset_state` that still says `pending…` or
/// `running …/…`: the one test the two arms that resume such a run share — `choose_start` after a
/// restart and `add_rule` after an enable/disable — so they cannot drift apart. A state saying
/// `done:`, `missing:` or nothing at all finished, and asks for nothing.
fn unfinished_reset(state: &str) -> Option<String> {
    if state.starts_with("pending") || state.starts_with("running") {
        Some(
            state
                .split_once(": ")
                .map_or("interrupted", |(_, r)| r)
                .to_string(),
        )
    } else {
        None
    }
}

/// The reasons a run that serves the operator's request writes — the two texts the requested
/// arms put into `reset_state`. An unrequested run writes other texts ("new rule", a lost
/// position, …), and both resume arms append "(resumed after restart)" / "(resumed after the
/// rule was added again)" to whatever they carry, so an exact match here never mistakes a run
/// that started on its own for one serving the request in the row.
const REQUESTED_RESET_REASONS: [&str; 2] = [
    "reset_requested",
    "reset_requested raised again while the finished reset was being cleared",
];

/// The token a `running:` line carries beside the step counter: a short digest of the rule
/// fingerprint — `Rule::fingerprint`, everything the actions are built from (the step bodies
/// and their listing SQL, the templates, and the target definition included), the same string
/// [`Engine::update_rule`] compares when a live rule change restarts a running reset. Only a
/// line whose token matches the loaded rule lets a restart skip its finished steps: a def
/// edited while the process was down ran those steps under a different contract, and the
/// target the new def names may never have seen them. `std`'s SipHash with fixed keys: stable
/// for the life of a build; an upgrade that moved the hash costs one re-walk — the safe
/// direction. A line written without the token (an older version) matches nothing and starts
/// over, as today.
fn fingerprint_digest(fingerprint: &str) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    fingerprint.hash(&mut h);
    format!("{:08x}", (h.finish() & 0xffff_ffff) as u32)
}

/// The step a saved `running:` line records, and the reason written after its step name:
/// `Some((i - 1, tail))` when `state` still says `running: step {i}/{n} fp{digest} {name}:
/// …`, the token digests the current fingerprint ([`fingerprint_digest`] — a def that moved
/// while the process was down restarts its own walk, the way [`Engine::update_rule`] restarts
/// a changed live run), and the recorded step count and step name still match `steps`.
/// Everything else is `None`: `pending:` (no step was reached yet), a line without the token
/// or with another def's, or a shape this parser does not recognise. The two resume arms read
/// the same line through this and apply their own reason test: [`resume_requested_reset`]
/// for the run serving the operator's request, [`resume_unrequested_reset`] for one that
/// started on its own.
///
/// Skipping the finished steps is safe to do at-least-once: a step advances only after every
/// chunk of it came back answered (`drive_reset` and `on_done`'s reset arms), so their keys
/// each had a send that the target confirmed. The interrupted step is re-run from its own
/// beginning: its key set lived in memory and died with the process, and the saved `done`
/// count lags reality by the persist cadence — a count into a re-listed set is a progress
/// note for `/status`, not a cursor a restart may skip keys by.
fn saved_reset_run<'a>(
    state: &'a str,
    steps: &[model::ResetStep],
    fingerprint: &str,
) -> Option<(usize, &'a str)> {
    let rest = state.strip_prefix("running: step ")?;
    let (idx, rest) = rest.split_once(' ')?;
    let (i, n) = idx.split_once('/')?;
    let i: usize = i.parse().ok()?;
    let n: usize = n.parse().ok()?;
    if i == 0 || i > n || n != steps.len() {
        return None;
    }
    // The digest token sits between the counter and the step name; a line that predates it
    // has the name there, never matches, and the run starts over.
    let (token, rest) = rest.split_once(' ')?;
    if token != format!("fp{}", fingerprint_digest(fingerprint)) {
        return None;
    }
    // `drive_reset` writes the step name after the token, then either the bare reason or
    // `{done}/{total} keys: {reason}`. A name that does not split this way simply never
    // matches and the run starts over.
    let (name, tail) = rest.split_once(": ")?;
    let want = steps.get(i - 1)?;
    if want.name != name {
        return None;
    }
    Some((i - 1, tail))
}

/// The step a restart continues a requested reset from: the line of the run that was serving
/// the operator's request ([`saved_reset_run`] for the shape and the def proofs), and only a
/// reason [`REQUESTED_RESET_REASONS`] carries — with the reasons' exact texts, so a carry-over
/// wording that appended itself to one is not the request's own; a run that restarted under
/// one starts over from step 1, exactly as a fresh request asks (#200, #203).
fn resume_requested_reset(
    state: &str,
    steps: &[model::ResetStep],
    fingerprint: &str,
) -> Option<usize> {
    let (step, tail) = saved_reset_run(state, steps, fingerprint)?;
    // Both accepted reasons end the line.
    REQUESTED_RESET_REASONS
        .into_iter()
        .find(|r| tail == *r || tail.ends_with(&format!(": {r}")))
        .map(|_| step)
}

/// The step a restart continues a run that started on its own from — the unrequested one
/// (`choose_start`'s and `add_rule`'s carry-over arms keep its reason's shape): the same line
/// read through [`saved_reset_run`], with the reason test inverted. The two
/// [`REQUESTED_RESET_REASONS`] are the request's own and belong to `add_rule`'s arm — a
/// leftover one under a flag that was cleared meanwhile keeps starting over, as #209 left it.
/// Everything else the engine writes for such a run resumes, the carry-over wordings of a run
/// that already took up a restart once included: each of its finished steps was target-
/// confirmed, and the interrupted one re-runs whole.
///
/// The one proof the line cannot carry is that this boot replays from at or below the rule's
/// confirmed point — `choose_start` holds that and enforces it: it resumes only a rule that
/// did NOT lose continuity on this boot. A rule that did takes no candidate from its saved
/// position and gets no `skip_until`, so the rows a skipped snapshot read would have covered
/// sit in nobody's replay window, and its walk starts over from step 1. With the position
/// kept, nothing is skipped over: the rule's persisted watermark never runs ahead of the
/// core's oldest unflushed mark (`rule_pos` takes it as a floor), so every change the target
/// has not yet seen sits above it and this boot's own replay re-marks it; a finished stray
/// step's keys were delete-confirmed, and the steps from the interrupted one list afresh when
/// their turn comes.
fn resume_unrequested_reset(
    state: &str,
    steps: &[model::ResetStep],
    fingerprint: &str,
) -> Option<usize> {
    let (step, tail) = saved_reset_run(state, steps, fingerprint)?;
    let requested = REQUESTED_RESET_REASONS
        .iter()
        .any(|r| tail == *r || tail.ends_with(&format!(": {r}")));
    (!requested).then_some(step)
}

/// The step `choose_start`'s carry-over arm continues a saved unrequested run from, given
/// what this boot knows about the rule beyond the line: `kept_position` says the rule is
/// not among this boot's `lost` — its saved position stood, so this boot replays from at
/// or below it and the skipped finished steps are covered. A rule that lost continuity on
/// this boot resumes NOTHING: `lose_continuity` has just given it a fresh step-0 run, and
/// with no usable position no boot bounds what a skipped step's read saw.
fn carried_reset_resume(
    kept_position: bool,
    state: &str,
    steps: &[model::ResetStep],
    fingerprint: &str,
) -> Option<usize> {
    kept_position.then(|| resume_unrequested_reset(state, steps, fingerprint))?
}

fn merge_marks(into: &mut Marks, m: Marks) {
    into.ids.extend(m.ids);
    into.queries.extend(m.queries);
    into.rows += m.rows;
}

/// The key as text: the core's form round-trips what `Key::from_text` made.
fn key_text(k: &key::Key) -> String {
    let mut s = String::new();
    k.to_text(&mut s);
    s
}

/// The keys of a flat set as text (a reset batch of snapshot or stray keys).
fn idset_texts(set: &buffer::IdSet) -> Vec<String> {
    set.to_keys().iter().map(key_text).collect()
}

fn first_words(q: &str) -> String {
    let w: Vec<&str> = q.split_whitespace().take(4).collect();
    w.join(" ")
}

/// What the stream loop remembers of the events this build cannot decode (review A-02, the
/// `undecodable` arm): the position its one line named, and how many such events there were since
/// the start. Memory only, like [`Engine::stream_error`]: a boot of a stream that works starts
/// clean, and the tool's tables hold no counters.
#[derive(Debug, Default, PartialEq, Eq)]
struct UndecodableWatch {
    /// The position the last line named. `None` until the first stuck round, and cleared when the
    /// stream delivers again, so any other position — a second stuck one, or the same one after the
    /// stream moved on — gets a line of its own instead of being silent.
    named: Option<Position>,
    /// Events this build could not decode: the `undecodable` field of `/status` and
    /// `lazywf_binlog_undecodable_total` on `/metrics`.
    seen: u64,
}

impl UndecodableWatch {
    /// Registers one refused event at `at` and answers whether a line is due: `true` the first time
    /// a position is named, `false` for the rounds that come back to it (the reader will return to
    /// the same bytes every round of the back-off ladder, and one sentence repeated every five
    /// seconds says nothing new), and `true` again for a position not named yet — a second standstill
    /// must not read as a stream that recovered. The key resets in [`UndecodableWatch::recovered`]
    /// rather than the watch remembering every position it has seen: the idiom of
    /// [`Engine::stmt_dml_last`]. Every event is counted, line or no line: an alert reads the number.
    ///
    /// Split out of [`Engine::on_undecodable_stream_error`] so the unit test can drive it without a
    /// live engine (the `stmt_dml_gate` idiom).
    fn note(&mut self, at: &Position) -> bool {
        self.seen += 1;
        if self.named.as_ref() == Some(at) {
            return false;
        }
        self.named = Some(at.clone());
        true
    }

    /// Registers one refused event whose line the caller logs as it pleases: the fatal rows decode,
    /// which says its one ERROR line and then exits, so there is nothing to rate limit.
    fn count(&mut self) {
        self.seen += 1;
    }

    /// The stream delivered events again: the position it had named is no longer where the reader
    /// stands, so standing still there later is news, and gets its line.
    fn recovered(&mut self) {
        self.named = None;
    }
}

/// What one finished round of the stream loop decided about the reconnect schedule (see
/// [`round_schedule`]).
#[derive(Debug)]
struct RoundSchedule {
    /// The connection delivered events and nothing kept the position: the dump error is no longer
    /// current, and the ladder starts over.
    recovered: bool,
    /// How long to wait before the next connect.
    pause: Duration,
    /// The ladder's next step, for the round after this one.
    backoff: Duration,
}

/// The schedule of the next connect, after a round that ended with the reader disconnected.
///
/// A round is recovered when it delivered and nothing kept the position. Both kinds of kept position
/// mean the next connect fails the same way — a 1236 the server cannot serve, or bytes this build
/// cannot decode (`source::is_undecodable`) — so such a round must not earn the [`STREAM_FAST_PATH`]
/// a stream that is moving earns, nor reset the ladder: that is the difference between waiting for
/// the source or for a deploy at a growing interval, and opening a new connection and dump request
/// on the server five times a second forever with `/livez` green. The schedule says only *when* to
/// connect again, never *from where*: the loop re-reads from the stream's boundary either way, so a
/// refused event is read again by every round until a build that decodes it ships.
///
/// Split out of [`Engine::stream`] so the unit test can drive the ladder without a live engine (the
/// `stmt_dml_gate` idiom).
fn round_schedule(
    delivered: bool,
    hit_1236: bool,
    hit_undecodable: bool,
    backoff: Duration,
) -> RoundSchedule {
    let recovered = delivered && !hit_1236 && !hit_undecodable;
    if recovered {
        return RoundSchedule {
            recovered,
            pause: STREAM_FAST_PATH,
            backoff: STREAM_BACKOFF_FIRST,
        };
    }
    RoundSchedule {
        recovered,
        pause: backoff,
        backoff: (backoff * 2).min(STREAM_BACKOFF_MAX),
    }
}

/// What the stream loop does with an `on_item` failure (see [`on_item_failure`]).
enum ItemFailure {
    /// The rows this build cannot decode: the ruling `source::Undecodable` carries keeps a decode
    /// failure fatal, so the process exits now.
    GiveUp,
    /// Anything else — a source that went away, a control connection that flapped: re-read from the
    /// boundary, on the generic budget of attempts the loop already keeps.
    Retry,
}

/// The class of a failed `on_item`. The classification is the copy's own (`is_undecodable` answers
/// "cannot decode", never "the source went away": a cut read stays `is_transient` and on the generic
/// path), and the consequence is the engine's: once the class is known, re-reading the same boundary
/// ten times reaches the same exit two seconds later, so the attempts are dropped rather than kept.
/// Split out so the unit test can drive the decision without a live engine.
fn on_item_failure(e: &anyhow::Error) -> ItemFailure {
    if source::is_undecodable(e) {
        ItemFailure::GiveUp
    } else {
        ItemFailure::Retry
    }
}

/// The ONE WARN line of a round that ended on bytes this build cannot decode: the position the
/// reader keeps coming back to, how long before it comes back, and that the event is neither
/// skipped nor lost. `{e:#}` is the copy's own line — kind, position, the table when the bytes name
/// one, and what the decoder said — so no words of ours repeat those (`source::Undecodable`'s
/// `Display` is written to be the record an operator gets).
fn warn_undecodable_stream_error(e: &anyhow::Error, at: &Position, retry_in: Duration) {
    warn!(
        at = %at,
        retry_in = ?retry_in,
        "binlog event this build cannot decode: {e:#}; the position is kept, the event is not \
         skipped, reading again in {retry_in:?}"
    );
}

/// The ONE ERROR line of the refused rows of a watched table: the process exits after it, so this
/// line is the whole record. It names kind, table and position as fields an alert can filter on,
/// and quotes the decoder verbatim — which is what `source::Undecodable`'s `Display` is for.
fn error_undecodable_rows(e: &anyhow::Error) {
    match e
        .chain()
        .find_map(|c| c.downcast_ref::<source::Undecodable>())
    {
        Some(u) => tracing::error!(
            kind = %u.kind,
            table = %u.table.as_deref().unwrap_or("unknown"),
            at = %u.position,
            "{u}"
        ),
        // `is_undecodable` also accepts an invalid-data packet the mapper never tagged.
        None => tracing::error!("binlog rows this build cannot decode: {e:#}"),
    }
}

/// The `/livez` status JSON: the instance's stream state and the per-rule summary. The three stream
/// keys are what an operator reads when the reader stands at one position and nothing moves — the
/// dump error it is kept through, the DML the stream could not carry, and the events this build
/// cannot decode. Split out of [`Engine::persist`] so the keys are testable without a connected
/// engine (the `stmt_dml_gate` idiom).
fn status_json(
    instance: &str,
    resume: &Position,
    stream_lag_ms: u64,
    stream_error: Option<&str>,
    statement_events_ignored: u64,
    undecodable: u64,
    rules: Map<String, Value>,
    binlog_filtered: Vec<String>,
) -> Value {
    json!({
        "instance": instance,
        "position": resume.to_string(),
        "stream_lag_ms": stream_lag_ms,
        "stream_error": stream_error,
        "statement_events_ignored": statement_events_ignored,
        "undecodable": undecodable,
        "rules": rules,
        "binlog_filtered_schemas": binlog_filtered,
    })
}

/// The rate-limit entry of `(schema, table)`: its slot, or `None` when the table is new and the
/// map already holds [`MAX_STMT_DML_TABLES`] of them — the key is a name the source states, so
/// the map cannot grow with it (lazyview's is bounded by the tables whose sessions twist
/// `binlog_format`; this one keeps a ceiling as well). The count and the metric keep taking every
/// event; only the per-table lines stop, and `capped` makes sure that is said out loud once.
/// Split out of the method so the unit test can drive it without a live engine.
fn stmt_dml_slot<'a>(
    map: &'a mut HashMap<(String, String), (Option<Instant>, u64)>,
    key: (String, String),
    capped: &mut bool,
) -> Option<&'a mut (Option<Instant>, u64)> {
    if !map.contains_key(&key) && map.len() >= MAX_STMT_DML_TABLES {
        if !*capped {
            *capped = true;
            warn!(
                tables = MAX_STMT_DML_TABLES,
                "too many tables logging DML in statement format: their WARN lines stop, \
                 lazywf_statement_events_ignored_total still counts them"
            );
        }
        return None;
    }
    Some(map.entry(key).or_insert((None, 0)))
}

/// The rate limit of [`Engine::note_statement_dml`] for one (schema, table) entry: `Some(count)`
/// when a line is due, `count` being the events it reports (this one plus those folded since the
/// last line, which the entry forgets as it reports them), and `None` when the event is folded
/// into the next line for that table. `None` for the time of the last line means "none logged
/// yet", so the first event always logs and no arithmetic on the clock is needed to say so:
/// `now - STMT_DML_LOG_INTERVAL` panics when the result would fall before the monotonic clock's
/// origin — on Linux the boot, so a host up for less than the interval would panic on its first
/// statement-format DML. Split out of the method so the unit test can drive it without a live
/// engine.
fn stmt_dml_gate(slot: &mut (Option<Instant>, u64), now: Instant) -> Option<u64> {
    if let Some(last) = slot.0 {
        if now.duration_since(last) < STMT_DML_LOG_INTERVAL {
            slot.1 += 1; // folded into the next line for this table
            return None;
        }
    }
    let count = slot.1 + 1;
    slot.0 = Some(now);
    slot.1 = 0;
    Some(count)
}

/// One WARN line of [`Engine::note_statement_dml`], split out so the unit test can read it back:
/// all it is given is what the operator needs — kind, schema, table, position and the folded
/// count — so no part of the statement, which in statement format carries the row values, can
/// reach the log.
fn warn_statement_dml(d: &crate::sql::StmtDml, at: &Position, count: u64) {
    warn!(
        kind = %d.kind,
        schema = %d.schema,
        table = %d.table,
        at = %at,
        count,
        "DML logged in statement format: its rows are not in the binlog, the rules on this \
         table missed this change"
    );
}

/// One flush: the fence, the `keys_sql` keys, then every key in groups. `keys` is the
/// flush's own list as `spawn_flush` built it, in an `Arc` it shares with its panic
/// fallback: the list is copied only when `keys_sql` resolves keys to add to it, because
/// that is the one thing that must extend it.
async fn flush_job(
    env: &Env,
    rule: &Rule,
    spec: FenceSpec,
    keys: Arc<Vec<String>>,
    queries: &[(String, KeyQuery)],
) -> Outcome {
    let fail = |e: anyhow::Error, keys: &[String]| Outcome::Flush {
        fired: Fired {
            error: Some(format!("{e:#}")),
            failed_keys: keys.to_vec(),
            ..Default::default()
        },
        keep_queries: true,
        resolved: Vec::new(),
    };
    if let Err(e) = action::fence(env, &spec).await {
        return fail(e, &keys);
    }
    let mut resolved = Vec::new();
    // What `keys_sql` found beyond the keys the rows already marked: kept apart from the
    // shared list so that list stays shared, and appended only below, where the two become
    // the list this flush fires.
    let mut found_keys = Vec::new();
    if !queries.is_empty() {
        match action::resolve_keys(env, queries.iter().map(|(_, q)| q)).await {
            Ok(found) => {
                let mut seen = text_set(keys.iter().map(String::as_str));
                for k in found {
                    if seen.insert(key::Key::from_text(&k)) {
                        found_keys.push(k.clone());
                        resolved.push(k);
                    }
                }
            }
            Err(e) => return fail(e, &keys),
        }
    }
    if keys.is_empty() && found_keys.is_empty() {
        return Outcome::Flush {
            fired: Fired::default(),
            keep_queries: false,
            resolved,
        };
    }
    let fired = if found_keys.is_empty() {
        action::fire_keys(env, rule, &keys, rule::ACTION, Value::Null, None).await
    } else {
        let mut all = (*keys).clone();
        all.extend(found_keys);
        action::fire_keys(env, rule, &all, rule::ACTION, Value::Null, None).await
    };
    Outcome::Flush {
        fired,
        keep_queries: false,
        resolved,
    }
}

#[cfg(test)]
#[path = "tests/engine.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/engine_batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
#[path = "tests/engine_stream_verdict_tests.rs"]
mod stream_verdict_tests;

#[cfg(test)]
#[path = "tests/engine_undecodable_tests.rs"]
mod undecodable_tests;

#[cfg(test)]
#[path = "tests/engine_hotpath_alloc_tests.rs"]
mod hotpath_alloc_tests;

#[cfg(test)]
#[path = "tests/engine_flush_alloc_tests.rs"]
mod flush_alloc_tests;

#[cfg(test)]
#[path = "tests/engine_binary_key_tests.rs"]
mod binary_key_tests;
#[cfg(test)]
#[path = "tests/engine_lossy_key_tests.rs"]
mod lossy_key_tests;
#[cfg(test)]
#[path = "tests/engine_skip_kinds_tests.rs"]
mod skip_kinds_tests;

#[cfg(test)]
#[path = "tests/engine_route_cache_tests.rs"]
mod route_cache_tests;

#[cfg(test)]
#[path = "tests/engine_source_clock_tests.rs"]
mod source_clock_tests;
