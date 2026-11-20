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
use mysql_async::binlog::row::BinlogRow;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
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
        /// without recomputing ids).
        queries: Vec<(String, KeyQuery)>,
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
    counters: Counters,
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
    Config,
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
/// reported once the rows are done).
struct RowCtx<'a> {
    marks: &'a mut HashMap<String, Marks>,
    rules: &'a mut BTreeMap<String, RuleState>,
    scratch: &'a mut KeyScratch,
    render_warnings: &'a mut u64,
    invalid_keys: &'a mut u64,
    invalid: &'a mut Vec<(String, u64, String)>,
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
    /// The server is a MariaDB 11.4 or newer, whose UPDATE and DELETE row images leave VIRTUAL
    /// generated columns out (see `maria_skips_virtual`).
    maria_skips_virtual: bool,
    /// Per watched table, the index and name of its first VIRTUAL generated column (`None` when
    /// it has none). Read once per table like the layout, and dropped with it: `on_statement` and
    /// the mismatch path invalidate both, so a DDL that adds a VIRTUAL column is seen again.
    virtual_first: HashMap<(String, String), Option<(usize, String)>>,
    /// Triggers already reported as unreadable (see `refuse_virtual_reads`), warned about once.
    virtual_refusals: HashSet<(String, u64)>,
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

/// Whether this MariaDB leaves VIRTUAL generated columns out of its UPDATE and DELETE row images.
///
/// It stopped doing that at 11.4: 10.6.28 and 10.11.19 still write them, as do MySQL 8.0.46,
/// 8.4.13, 9.7.4 and Percona 8.4.11. Such an image carries one value less than the table map
/// names, and the reader pairs values with columns by position, so every column from the first
/// VIRTUAL one on is read as its neighbour's value — a key built from the wrong value, with
/// nothing logged. A version that cannot be read counts as unaffected: refusing on a parse
/// failure would stop a working installation over a version string.
// Temporary: remove this and `Engine::refuse_virtual_reads` when the vendored reader learns to
// decode images whose values skip columns (lazyview issue #7); until then lazywf refuses the
// triggers that would read them, which is visible on `/status` instead of silently wrong.
fn maria_skips_virtual(version: &str) -> bool {
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
enum VirtualRead {
    /// A column standing at or after the table's first VIRTUAL generated column.
    Col(String),
    /// The whole row. A `KeyTemplate::Full` is any Handlebars template, so there is no telling
    /// from the rule whether it names the shifted columns: it may read every column, so it is
    /// refused for any table that has a VIRTUAL one.
    Full,
}

/// The first thing `reads` takes from an image that skips its VIRTUAL columns: `reads` holds the
/// columns the trigger names together with their index in the layout (columns the table does not
/// have are left out — `check_columns` reports those), `full` says whether the trigger renders
/// the whole row, and `from` is the index of the table's first VIRTUAL generated column. A rule
/// that reads only columns before `from` reads values the image does carry, so it stays allowed.
fn unreadable_read(reads: &[(&str, usize)], full: bool, from: usize) -> Option<VirtualRead> {
    if full {
        return Some(VirtualRead::Full);
    }
    reads
        .iter()
        .find(|(_, i)| *i >= from)
        .map(|(c, _)| VirtualRead::Col((*c).to_string()))
}

/// The first VIRTUAL generated column of `rows`, which are the table's columns in
/// `information_schema` order as (name, EXTRA): its index in that order (the order
/// `rows::Catalog::load` builds a layout from) and its name.
fn virtual_first_of(rows: &[(String, String)]) -> Option<(usize, String)> {
    rows.iter()
        .enumerate()
        .find(|(_, (_, extra))| extra.to_ascii_uppercase().contains("VIRTUAL GENERATED"))
        .map(|(i, (name, _))| (i, name.clone()))
}

/// The columns of one table, in `information_schema` order, as (name, EXTRA).
async fn read_virtual_first(
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
    Ok(virtual_first_of(&rows))
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
    // Whether the server's row images can come up short (see `maria_skips_virtual`). Read once:
    // the server does not change underneath a running engine.
    let skips_virtual = info.flavor == Flavor::MariaDb && maria_skips_virtual(&info.version);
    if skips_virtual {
        info!(
            server = %info.version,
            "this MariaDB leaves VIRTUAL generated columns out of its UPDATE and DELETE row \
             images: a trigger that reads a column from the first one on is refused"
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
        maria_skips_virtual: skips_virtual,
        virtual_first: HashMap::new(),
        virtual_refusals: HashSet::new(),
        tx,
        last_persist: Instant::now(),
        clear_retry: None,
        star_written: None,
        render_warnings: 0,
        invalid_keys: 0,
        filter_checked: HashSet::new(),
        binlog_filtered: BTreeSet::new(),
        stream_error: None,
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
        for (n, reason) in lost {
            self.lose_continuity(&n, reason);
        }
        // Resets a previous run did not finish.
        for n in names {
            let Some(saved) = self.saved.get(&n) else {
                continue;
            };
            let unfinished = saved.reset_state.starts_with("pending")
                || saved.reset_state.starts_with("running");
            if unfinished && self.rules.get(&n).is_some_and(|s| s.reset.is_none()) {
                let reason = saved
                    .reset_state
                    .split_once(": ")
                    .map_or("interrupted", |(_, r)| r)
                    .to_string();
                self.start_reset(&n, format!("{reason} (resumed after restart)"), false);
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
                    self.saved.entry(name.clone()).or_default().pos = Some(pos);
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
            counters: Counters::default(),
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
        }
        if rule.def.reset_requested {
            self.start_reset(name, "reset_requested".into(), true);
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
        let restart = st
            .reset
            .as_ref()
            .filter(|r| r.fingerprint != rule.fingerprint)
            .map(|r| (r.reason.clone(), r.requested));
        let request = rule.def.reset_requested && !st.pending_clear;
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
        } else if request && !running {
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
        let mut backoff = Duration::from_millis(500);
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
                    } else {
                        warn!("binlog connect failed: {e:#}; retrying in {backoff:?}");
                    }
                    if self.idle(backoff, &mut rx, shutdown).await? {
                        break 'reconnect;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    continue 'reconnect;
                }
            };
            info!(position = %pos, "streaming the binlog");
            self.health.set_streaming(true);
            let mut last_event = Instant::now();
            let mut delivered = false;
            let mut hit_1236 = false;
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
                                        backoff = Duration::from_millis(500);
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
                            Ok(None) => {
                                warn!("the server closed the binlog stream; reconnecting");
                                break;
                            }
                            Err(e) => {
                                if binlog_unusable(&e) {
                                    self.on_missing_binlog(&e).await?;
                                } else if source::is_duplicate_server_id(&e) {
                                    self.reroll_server_id();
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
            let _ = reader.close().await;
            // A partly read transaction is read again from its start.
            self.in_txn = false;
            self.table_ids.clear();
            self.txn_marks.clear();
            self.xa_current = None;
            pos = self.boundary.clone();
            // A connection that delivered events without a kept 1236: the stream recovered, so
            // the error is no longer current and the backoff starts over. While a 1236 that keeps
            // the position repeats, the capped backoff grows even though each attempt delivers
            // the events before the offending one.
            let recovered = delivered && !hit_1236;
            if recovered {
                kept_position = false;
                self.stream_error = None;
                backoff = Duration::from_millis(500);
            }
            let pause = if recovered {
                Duration::from_millis(200)
            } else {
                backoff
            };
            if !recovered {
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
            if self.idle(pause, &mut rx, shutdown).await? {
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
                    Some(Route::Config) => {
                        self.config_dirty.get_or_insert_with(Instant::now);
                    }
                    Some(Route::Watched(w)) => {
                        let Engine {
                            txn_marks,
                            rules,
                            key_scratch,
                            render_warnings,
                            invalid_keys,
                            ..
                        } = self;
                        let mut invalid = Vec::new();
                        let mut ctx = RowCtx {
                            marks: txn_marks,
                            rules,
                            scratch: key_scratch,
                            render_warnings,
                            invalid_keys,
                            invalid: &mut invalid,
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
            Item::Heartbeat | Item::Other => {}
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
                Route::Config
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
                    .refuse_virtual_reads(&nkey, &layout, &key.0, &key.1, triggers)
                    .await?;
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
                // `forget_columns` takes the entry with the catalog entry and `virtual_first`,
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

    /// The index and name of the table's first VIRTUAL generated column, on a server that leaves
    /// those columns out of its row images; `Ok(None)` everywhere else and for a table without
    /// one. Read once per table, like the layout cache it lives and dies with.
    ///
    /// `key` is the table's identity as [`Engine::route`] knows it, so a cached answer costs no
    /// allocation; `schema` and `table` are the names to ask the server about.
    async fn first_virtual_column(
        &mut self,
        key: &(String, String),
        schema: &str,
        table: &str,
    ) -> Result<Option<(usize, String)>> {
        if !self.maria_skips_virtual {
            return Ok(None);
        }
        if let Some(found) = self.virtual_first.get(key) {
            return Ok(found.clone());
        }
        // An image whose columns cannot be read is an image that must not be read: a server that
        // will not answer them is an error, as it is for the layout read above, and the caller
        // retries. A table that is really gone fails there too, not here.
        let found = read_virtual_first(&mut self.control, schema, table).await?;
        self.virtual_first.insert(key.clone(), found.clone());
        Ok(found)
    }

    /// Drops what is cached about a table's columns, so both the layout and the position of its
    /// first VIRTUAL column are read again: a DDL changed the table, or its layout did not match
    /// the binlog. The built route goes with them — a DDL changes the columns a `Watched` decodes.
    fn forget_columns(&mut self, schema: &str, table: &str) {
        self.catalog.invalidate(schema, table);
        let key = self.norm(schema, table);
        if table == "*" {
            self.virtual_first.retain(|(s, _), _| *s != key.0);
        } else {
            self.virtual_first.remove(&key);
        }
        self.route_cache.forget(&key);
    }

    /// Refuses the triggers that would read what the row images do not carry, and returns those
    /// that may keep running.
    ///
    /// On MariaDB 11.4+ an UPDATE or DELETE image holds one value less than the table map names
    /// (the VIRTUAL generated columns are not written), and the reader pairs values with columns
    /// by position, so the first VIRTUAL column and every column after it are read as their
    /// neighbour's value. Such a trigger marks keys that do not describe the row, so it marks
    /// nothing at all and carries the reason on `/status` the way an unknown column does.
    /// Temporary, see [`maria_skips_virtual`].
    async fn refuse_virtual_reads(
        &mut self,
        key: &(String, String),
        layout: &Layout,
        schema: &str,
        table: &str,
        triggers: Vec<(String, Arc<Rule>, usize)>,
    ) -> Result<Vec<(String, Arc<Rule>, usize)>> {
        let Some((from, virtual_col)) = self.first_virtual_column(key, schema, table).await? else {
            return Ok(triggers);
        };
        let mut kept = Vec::with_capacity(triggers.len());
        for (name, rule, ti) in triggers {
            let t = &rule.triggers[ti];
            // What the trigger reads of an image: the same columns `check_columns` looks at, and
            // the whole row for a Full key.
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
            if self.virtual_refusals.insert((name.clone(), t.def.id)) {
                let msg = match &read {
                    VirtualRead::Col(c) => format!(
                        "trigger {}: `{schema}`.`{table}` reads `{c}`; this MariaDB leaves the \
                         VIRTUAL generated column `{virtual_col}` out of its UPDATE and DELETE \
                         row images, and every column after it moves up a place, so lazywf \
                         cannot read `{c}` and refuses the trigger (make `{virtual_col}` STORED, \
                         or read only columns before it)",
                        t.def.id
                    ),
                    VirtualRead::Full => format!(
                        "trigger {}: `{schema}`.`{table}` uses a Handlebars key template, which \
                         can read any column of the row, including the VIRTUAL generated column \
                         `{virtual_col}` and the columns after it that this MariaDB's UPDATE \
                         and DELETE row images shift; lazywf refuses the trigger (make \
                         `{virtual_col}` STORED, or use a plain {{{{new.col}}}} key over columns \
                         before it)",
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
            };
            Self::row_keys(w, kind, before, after, &mut row);
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
                                        match w.layout.index(c).map(|i| w.layout.value(img, i)) {
                                            None | Some(Value::Null) => continue 'line,
                                            Some(v) => template::push_text(&v, k),
                                        }
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
                        let ctx = json!({"old": obj, "new": obj, "row": obj, "op": op_text(op), "table": w.table});
                        for tpl in lines {
                            match rule.templates.render(tpl, &ctx) {
                                Ok(s) => {
                                    let k = s.trim();
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
                    let params = ks
                        .columns
                        .iter()
                        .map(|c| {
                            w.layout
                                .index(c)
                                .map_or(Value::Null, |i| w.layout.value(img, i))
                        })
                        .collect();
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
        let keys: Vec<String> = buf.keys.render(&maps);
        let queries: Vec<(String, KeyQuery)> = buf.take_query_payloads(taken.items);
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
            queries: queries.clone(),
        });
        buf.since_ts = 0;
        buf.first = None;
        buf.last = None;
        let (rule, incarnation, name) = (st.rule.clone(), st.incarnation, name.to_string());
        let failed = Outcome::Flush {
            fired: Fired::failed(keys.clone()),
            keep_queries: true,
            resolved: Vec::new(),
        };
        let mrule = st.mrule.clone();
        spawn_job(tx, name, incarnation, failed, async move {
            let start = Instant::now();
            let out = flush_job(&env, &rule, spec, keys, queries).await;
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
                "running: step {}/{} {}: {}",
                i + 1,
                steps,
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
                        "running: step {}/{} {}: {}/{} keys: {}",
                        i + 1,
                        steps,
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
                    spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                        let reset = json!({"step": rule.def.reset_steps[i].name, "keys": chunk});
                        let fired = action::fire_keys(
                            &env,
                            &rule,
                            chunk,
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
                    spawn_job(tx.clone(), rname.clone(), incarnation, failed, async move {
                        let fired = action::fire_strays(&env, &rule, i, e, chunk).await;
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
        let Some(st) = self.rules.get_mut(&d.rule) else {
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
                            let origin = self.settled.clone();
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
                            st.buffer.put_back_queries(queries);
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
            if let Some(b) = self.target_backoff.get_mut(&target) {
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

    /// Writes the positions that moved (and every row now and then).
    async fn persist(&mut self, force: bool) -> Result<()> {
        self.last_persist = Instant::now();
        lock::verify(&mut self.control, &self.lock).await?;
        let mut rows = Vec::new();
        let mut summary = Map::new();
        let now = Instant::now();
        for (name, st) in &self.rules {
            let pos = self.rule_pos(st);
            let lag = self.rule_lag_ms(st);
            // The pending gauge reads the same source the status table shows (`pending_keys`,
            // which reads the dedup buffer's `pending_len`); the value is pushed to the atomic.
            if let Some(m) = &st.mrule {
                m.set_pending(st.buffer.pending_keys() as u64);
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
        self.health.set_status(json!({
            "instance": self.cfg.instance,
            "position": resume.to_string(),
            "stream_lag_ms": self.stream_lag_ms(),
            "stream_error": self.stream_error,
            "statement_events_ignored": self.stmt_dml_ignored,
            "rules": summary,
            "binlog_filtered_schemas": self.binlog_filtered.iter().collect::<Vec<_>>(),
        }));
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
fn spawn_job(
    tx: mpsc::UnboundedSender<Done>,
    rule: String,
    incarnation: u64,
    failed: Outcome,
    job: impl std::future::Future<Output = Outcome> + Send + 'static,
) {
    tokio::spawn(async move {
        let outcome = match tokio::spawn(job).await {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(rule = %rule, "job failed unexpectedly: {e}");
                failed
            }
        };
        let _ = tx.send(Done {
            rule,
            incarnation,
            outcome,
        });
    });
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

async fn flush_job(
    env: &Env,
    rule: &Rule,
    spec: FenceSpec,
    mut keys: Vec<String>,
    queries: Vec<(String, KeyQuery)>,
) -> Outcome {
    let fail = |e: anyhow::Error, keys: Vec<String>| Outcome::Flush {
        fired: Fired {
            error: Some(format!("{e:#}")),
            failed_keys: keys,
            ..Default::default()
        },
        keep_queries: true,
        resolved: Vec::new(),
    };
    if let Err(e) = action::fence(env, &spec).await {
        return fail(e, keys);
    }
    let mut resolved = Vec::new();
    if !queries.is_empty() {
        match action::resolve_keys(env, queries.iter().map(|(_, q)| q)).await {
            Ok(found) => {
                let mut seen = text_set(keys.iter().map(String::as_str));
                for k in found {
                    if seen.insert(key::Key::from_text(&k)) {
                        keys.push(k.clone());
                        resolved.push(k);
                    }
                }
            }
            Err(e) => return fail(e, keys),
        }
    }
    if keys.is_empty() {
        return Outcome::Flush {
            fired: Fired::default(),
            keep_queries: false,
            resolved,
        };
    }
    let fired = action::fire_keys(env, rule, keys, rule::ACTION, Value::Null, None).await;
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
#[path = "tests/engine_hotpath_alloc_tests.rs"]
mod hotpath_alloc_tests;

#[cfg(test)]
#[path = "tests/engine_skip_kinds_tests.rs"]
mod skip_kinds_tests;

#[cfg(test)]
#[path = "tests/engine_route_cache_tests.rs"]
mod route_cache_tests;
