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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use indexmap::{IndexMap, IndexSet};

use crate::keyset::KeySet;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::action::{self, Env, FenceSpec, Fired, KeyQuery};
use crate::binlog::{self, BinlogReader, Flavor, Item, Position, RowsKind};
use crate::config::Config;
use crate::health::Health;
use crate::lock;
use crate::model::{self, RuleDef, StepKind, TargetDef, TriggerDef};
use crate::rows::{Catalog, Layout, LayoutMismatch};
use crate::rule::{self, KeyTemplate, Rule};
use crate::status::{self, Saved, INSTANCE_ROW};
use crate::target::Target;
use crate::template;

const BINLOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// An XA transaction prepared longer ago stops holding the saved position back.
const XA_PIN_MAX: Duration = Duration::from_secs(3600);
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
    keys: KeySet,
    queries: IndexMap<String, KeyQuery>,
    rows: u64,
}

/// Marked keys waiting for their flush.
#[derive(Debug, Default)]
struct Buffer {
    keys: KeySet,
    /// `keys_sql` to run at flush time, deduplicated by parameters.
    queries: IndexMap<String, KeyQuery>,
    /// Stream position before the oldest transaction that marked something here.
    since: Option<Position>,
    /// Commit time (unix seconds) of that transaction.
    since_ts: u32,
    first: Option<Instant>,
    last: Option<Instant>,
}

impl Buffer {
    fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.queries.is_empty()
    }

    fn hold(&mut self, since: &Position, ts: u32) {
        if self.since.as_ref().is_none_or(|s| since < s) {
            self.since = Some(since.clone());
        }
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
}

enum Lane {
    Flush {
        since: Option<Position>,
        since_ts: u32,
        first: Option<Instant>,
        /// Keys of this flush that have strikes.
        struck: Vec<String>,
        queries: Vec<KeyQuery>,
    },
    Reset,
}

/// A key the target refused this many times (while others went through) is dropped.
const STRIKES: u32 = 5;

enum Phase {
    Start,
    /// A job of this step is running.
    Waiting,
    /// Snapshot keys still to send; `batch` is in flight (kept until confirmed).
    Batches {
        keys: KeySet,
        total: u64,
        done: u64,
        batch: Option<Vec<String>>,
    },
    Strays {
        per_ep: Vec<KeySet>,
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
    touched: Option<KeySet>,
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
        let mut retry = Vec::new();
        let mut dropped = Vec::new();
        for (k, why) in rejected {
            if !others_ok && !self.strikes.contains_key(&k) {
                retry.push(k);
                continue;
            }
            let n = self.strikes.entry(k.clone()).or_insert(0);
            *n += 1;
            if *n >= STRIKES {
                self.strikes.remove(&k);
                dropped.push((k, why));
            } else {
                retry.push(k);
            }
        }
        (retry, dropped)
    }

    fn report_dropped(&mut self, rule: &str, dropped: &[(String, String)], now_ms: u64) {
        for (k, why) in dropped {
            let k: String = k.chars().take(200).collect();
            warn!(rule = %rule, "key `{k}` was refused {STRIKES} times and is dropped: {why}");
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
        keys: Result<KeySet, String>,
    },
    Strays {
        run: u64,
        per_ep: Result<Vec<KeySet>, String>,
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
}

struct XaTxn {
    start: Position,
    marks: HashMap<String, Marks>,
    prepared_at: Instant,
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
    xa_current: Option<(String, XaTxn)>,
    xa_pending: HashMap<String, XaTxn>,

    config_dirty: Option<Instant>,
    config_fp: u64,
    next_poll: Instant,
    targets: HashMap<String, Arc<Target>>,
    routes: HashMap<(String, String), Vec<(String, usize)>>,
    rules: BTreeMap<String, RuleState>,
    saved: HashMap<String, Saved>,
    incarnations: u64,
    runs: u64,
    target_backoff: HashMap<String, Backoff>,
    /// Tables whose binlog layout does not match (reported once).
    mismatched: HashSet<(String, String)>,
    column_warnings: HashSet<(String, u64, String)>,
    tx: mpsc::UnboundedSender<Done>,
    last_persist: Instant,
    clear_retry: Option<Instant>,
    star_written: Option<(Position, Instant)>,
    render_warnings: u64,
    invalid_keys: u64,
}

fn server_code(e: &anyhow::Error) -> Option<u16> {
    e.chain()
        .find_map(|c| match c.downcast_ref::<mysql_async::Error>() {
            Some(mysql_async::Error::Server(se)) => Some(se.code),
            _ => None,
        })
}

fn binlog_unusable(e: &anyhow::Error) -> bool {
    binlog::is_missing_binlog(e)
        || (server_code(e) == Some(1236) && !binlog::is_duplicate_server_id(e))
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
    let gtid: Option<String> = if flavor == Flavor::MySql {
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
        Flavor::MySql => "SELECT CAST(@@server_uuid AS CHAR)",
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
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // The reader names lazyview in its messages unless told otherwise (main does it earlier).
    binlog::set_product("lazywf", "LAZYWF_");
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
    let pool = Pool::new(
        OptsBuilder::from_opts(opts.clone()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, cfg.pool_size).expect("0 <= max")),
        ),
    );
    let env = Arc::new(Env::new(
        pool.clone(),
        info.flavor,
        cfg.visibility_delay,
        cfg.send_concurrency,
    ));
    let saved = status::load(&mut control, &cfg.instance).await?;
    info!(server = %info.version, schema = %cfg.schema, instance = %cfg.instance, "lazywf {} starting", env!("CARGO_PKG_VERSION"));
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
        xa_current: None,
        xa_pending: HashMap::new(),
        config_dirty: None,
        config_fp: 0,
        next_poll: Instant::now() + cfg.config_poll,
        targets: HashMap::new(),
        routes: HashMap::new(),
        rules: BTreeMap::new(),
        saved,
        incarnations: 0,
        runs: 0,
        target_backoff: HashMap::new(),
        mismatched: HashSet::new(),
        column_warnings: HashSet::new(),
        tx,
        last_persist: Instant::now(),
        clear_retry: None,
        star_written: None,
        render_warnings: 0,
        invalid_keys: 0,
        cfg,
    };
    e.reload_config(true).await?;
    let start = e.choose_start().await?;
    let r = e.stream(start, rx, &mut shutdown).await;
    pool.disconnect().await.ok();
    r
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
        if let Some(s) = &st.buffer.since {
            lower(s);
        }
        if let Some(Lane::Flush { since: Some(s), .. }) = &st.lane {
            lower(s);
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
        let current = binlog::current_position(&mut self.control).await?;
        let logs: Vec<mysql_async::Row> = self
            .control
            .query("SHOW BINARY LOGS")
            .await
            .context("SHOW BINARY LOGS")?;
        let files: HashSet<String> = logs
            .iter()
            .filter_map(|r| r.get_opt::<String, _>(0).and_then(|v| v.ok()))
            .collect();
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
            if st.buffer.since.is_some() {
                st.buffer.since = Some(p.clone());
            }
            if let Some(Lane::Flush { since: Some(s), .. }) = &mut st.lane {
                *s = p.clone();
            }
            st.skip_until = None;
            if st.frozen_at.is_some() {
                st.frozen_at = Some(p.clone());
            }
        }
    }

    async fn on_missing_binlog(&mut self, e: &anyhow::Error) -> Result<()> {
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
                    _ => Target::new(def, self.env.pool.clone()).map_err(|e| format!("{e:#}")),
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
            let mut rule = rule::compile(&mut self.control, def, defs, &targets).await;
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
        self.rebuild_routes();
        if !initial {
            debug!("configuration reloaded: {} rules", self.rules.len());
        }
        Ok(true)
    }

    fn add_rule(&mut self, name: &str, rule: Arc<Rule>, initial: bool) {
        self.incarnations += 1;
        let frozen = rule.trigger_error.is_some();
        let error = rule.error.clone().or_else(|| rule.trigger_error.clone());
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
        };
        self.rules.insert(name.to_string(), st);
        if !initial {
            info!(rule = %name, "rule added");
            // A new rule's target has no history; one that ran before and comes back missed
            // what happened in between.
            match self.saved.get(name).and_then(|s| s.pos.clone()) {
                None => self.lose_continuity(name, "new rule".into()),
                Some(p) if p < self.settled => self.lose_continuity(
                    name,
                    format!("enabled again, nothing was tracked since {p}"),
                ),
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
        let st = self.rules.get_mut(name).expect("present");
        let changed = st.rule.fingerprint != rule.fingerprint || st.rule.error != rule.error;
        st.rule = rule.clone();
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
        self.routes = routes;
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
        let silence = (self.cfg.heartbeat * 3).max(Duration::from_secs(15));
        self.health.set_phase("streaming");
        'reconnect: loop {
            self.health.beat();
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
                Ok(r) => r,
                Err(e) => {
                    if binlog_unusable(&e) {
                        self.on_missing_binlog(&e).await?;
                        pos = self.boundary.clone();
                        continue 'reconnect;
                    }
                    if binlog::is_duplicate_server_id(&e) && self.reroll_server_id() {
                        continue 'reconnect;
                    }
                    if binlog::is_access_denied(&e) {
                        return Err(e);
                    }
                    warn!("binlog connect failed: {e:#}; retrying in {backoff:?}");
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
                                    backoff = Duration::from_millis(500);
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
                                } else if binlog::is_duplicate_server_id(&e) {
                                    self.reroll_server_id();
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
            let pause = if delivered {
                Duration::from_millis(200)
            } else {
                backoff
            };
            if !delivered {
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
        for st in self.rules.values_mut() {
            if st.skip_until.as_ref().is_some_and(|s| *s <= pos) {
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
                if !(schema == self.cfg.schema && table == model::STATUS) {
                    self.txn_foreign = true;
                }
                let route = self.route(reader, table_id, &schema, &table).await?;
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
                        let rows = reader.rows(table_id, &event)?;
                        self.feed(&w, kind, &rows);
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
                        let names: Vec<String> = self.rules.keys().cloned().collect();
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
                self.end_txn(pos);
                self.on_statement(&schema, &query);
            }
            Item::Heartbeat | Item::Other => {}
        }
        Ok(())
    }

    async fn route(
        &mut self,
        reader: &BinlogReader,
        table_id: u64,
        schema: &str,
        table: &str,
    ) -> Result<Route> {
        if schema == self.cfg.schema && model::is_own_table(table) {
            return Ok(if model::is_config_table(table) {
                Route::Config
            } else {
                Route::Ignore
            });
        }
        let key = self.norm(schema, table);
        let Some(list) = self.routes.get(&key).cloned() else {
            return Ok(Route::Ignore);
        };
        match self
            .catalog
            .layout(&mut self.control, schema, table, reader.table_map(table_id))
            .await
        {
            Ok(layout) => {
                self.mismatched.remove(&key);
                let triggers: Vec<(String, Arc<Rule>, usize)> = list
                    .iter()
                    .filter_map(|(n, i)| self.rules.get(n).map(|s| (n.clone(), s.rule.clone(), *i)))
                    .collect();
                self.check_columns(&layout, schema, table, &triggers);
                Ok(Route::Watched(Arc::new(Watched {
                    layout,
                    table: format!("{schema}.{table}"),
                    triggers,
                })))
            }
            Err(e) if e.downcast_ref::<LayoutMismatch>().is_some() => {
                self.catalog.invalidate(schema, table);
                // Once per mismatch, not for every transaction read before the DDL.
                if self.mismatched.insert(key) {
                    warn!("{e:#}");
                    for (n, _) in list {
                        self.lose_continuity(&n, format!("{e}"));
                    }
                }
                Ok(Route::Ignore)
            }
            Err(e) => Err(e),
        }
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

    /// Marks the keys of these rows' images for every trigger on the table.
    fn feed(
        &mut self,
        w: &Watched,
        kind: RowsKind,
        rows: &[(Option<BinlogRow>, Option<BinlogRow>)],
    ) {
        let op = match kind {
            RowsKind::Insert => "insert",
            RowsKind::Update => "update",
            RowsKind::Delete => "delete",
        };
        let mut invalid: Vec<(String, u64, String)> = Vec::new();
        for (before, after) in rows {
            let images: Vec<&BinlogRow> = match kind {
                RowsKind::Insert => after.iter().collect(),
                RowsKind::Delete => before.iter().collect(),
                RowsKind::Update => before.iter().chain(after.iter()).collect(),
            };
            let mut objects: Vec<Option<Value>> = vec![None; images.len()];
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
                    let (Some(b), Some(a)) = (before, after) else {
                        continue;
                    };
                    let changed = t
                        .def
                        .changed_columns
                        .iter()
                        .any(|c| match w.layout.index(c) {
                            Some(i) => w.layout.value(b, i) != w.layout.value(a, i),
                            None => true,
                        });
                    if !changed {
                        continue;
                    }
                }
                let marks = self.txn_marks.entry(name.clone()).or_default();
                marks.rows += 1;
                for (ii, img) in images.iter().enumerate() {
                    match &t.key {
                        Some(KeyTemplate::Simple(lines)) => {
                            'line: for line in lines {
                                let mut k = String::new();
                                for seg in line {
                                    match seg {
                                        template::Seg::Lit(s) => k.push_str(s),
                                        template::Seg::Col(c) => {
                                            match w.layout.index(c).map(|i| w.layout.value(img, i))
                                            {
                                                None | Some(Value::Null) => continue 'line,
                                                Some(v) => k.push_str(&template::text(&v)),
                                            }
                                        }
                                    }
                                }
                                let k = k.trim();
                                if template::valid_key(k) {
                                    marks.keys.insert(k);
                                } else if !k.is_empty() {
                                    invalid.push((name.clone(), t.def.id, k.to_string()));
                                }
                            }
                        }
                        Some(KeyTemplate::Full(lines)) => {
                            let obj = objects[ii]
                                .get_or_insert_with(|| w.layout.object(img))
                                .clone();
                            let ctx = json!({"old": obj, "new": obj, "row": obj, "op": op, "table": w.table});
                            for tpl in lines {
                                match rule.templates.render(tpl, &ctx) {
                                    Ok(s) => {
                                        let k = s.trim();
                                        if template::valid_key(k) {
                                            marks.keys.insert(k);
                                        } else if !k.is_empty() {
                                            invalid.push((name.clone(), t.def.id, k.to_string()));
                                        }
                                    }
                                    Err(e) => {
                                        // Reported on the rule, so a key that cannot be rendered
                                        // does not pass unnoticed.
                                        self.render_warnings += 1;
                                        if self.render_warnings <= 10
                                            || self.render_warnings.is_multiple_of(1000)
                                        {
                                            warn!(rule = %name, "trigger {} key: {e:#}", t.def.id);
                                        }
                                        if let Some(st) = self.rules.get_mut(name) {
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
        for (name, trigger, k) in invalid {
            self.invalid_keys += 1;
            let shown: String = k.escape_debug().take(120).collect();
            if self.invalid_keys <= 10 || self.invalid_keys.is_multiple_of(1000) {
                warn!(rule = %name, "trigger {trigger}: key \"{shown}\" holds control characters and is ignored");
            }
            if let Some(st) = self.rules.get_mut(&name) {
                st.last_error = Some((
                    format!("trigger {trigger}: key \"{shown}\" holds control characters and is ignored"),
                    crate::time::unix_now_ms(),
                ));
                st.dirty = true;
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
            if m.keys.is_empty() && m.queries.is_empty() {
                continue;
            }
            if let Some(t) = st.reset.as_mut().and_then(|r| r.touched.as_mut()) {
                t.extend(m.keys.clone());
            }
            st.buffer.keys.extend(m.keys);
            st.buffer.queries.extend(m.queries);
            st.buffer.hold(since, ts);
            st.buffer.first.get_or_insert(now);
            st.buffer.last = Some(now);
        }
    }

    fn on_statement(&mut self, schema: &str, query: &str) {
        for (s, t, effect) in crate::sql::ddl_tables(query, schema) {
            self.catalog.invalidate(&s, &t);
            if s == self.cfg.schema && (model::is_config_table(&t) || t == "*") {
                self.config_dirty.get_or_insert_with(Instant::now);
            }
            if effect != crate::sql::DdlEffect::Replace {
                continue;
            }
            let (ns, nt) = self.norm(&s, &t);
            let hit: Vec<String> = self
                .routes
                .iter()
                .filter(|((rs, rt), _)| *rs == ns && (nt == "*" || *rt == nt))
                .flat_map(|(_, v)| v.iter().map(|(n, _)| n.clone()))
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
        let names: Vec<String> = self.rules.keys().cloned().collect();
        for name in names {
            let st = &self.rules[&name];
            let target_blocked = self
                .target_backoff
                .get(&st.rule.def.target)
                .and_then(|b| b.blocked(now))
                .is_some();
            let lane_free =
                st.lane.is_none() && st.backoff.blocked(now).is_none() && !target_blocked;
            if lane_free {
                if let Some(due) = st.buffer.due(st.rule.def.debounce, st.rule.def.max_delay) {
                    if due <= now {
                        self.spawn_flush(&name);
                        continue;
                    }
                }
            }
            self.drive_reset(&name, now, lane_free);
        }
    }

    fn spawn_flush(&mut self, name: &str) {
        let spec = FenceSpec {
            gtid: self.last_gtid.clone(),
            pos: self.boundary.clone(),
        };
        let (env, tx) = (self.env.clone(), self.tx.clone());
        let st = self.rules.get_mut(name).expect("present");
        let buf = std::mem::take(&mut st.buffer);
        let keys: Vec<String> = buf.keys.into_vec();
        let queries: Vec<KeyQuery> = buf.queries.into_values().collect();
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
            since: buf.since,
            since_ts: buf.since_ts,
            first: buf.first,
            struck,
            queries: queries.clone(),
        });
        let (rule, incarnation, name) = (st.rule.clone(), st.incarnation, name.to_string());
        let failed = Outcome::Flush {
            fired: Fired::failed(keys.clone()),
            keep_queries: true,
            resolved: Vec::new(),
        };
        spawn_job(tx, name, incarnation, failed, async move {
            flush_job(&env, &rule, spec, keys, queries).await
        });
    }

    /// Starts the next job of a rule's reset, if one can run now.
    fn drive_reset(&mut self, name: &str, now: Instant, lane_free: bool) {
        let (env, tx) = (self.env.clone(), self.tx.clone());
        let st = self.rules.get_mut(name).expect("present");
        let Some(run) = st.reset.as_mut() else { return };
        if st.prep || run.backoff.blocked(now).is_some() || st.rule.error.is_some() {
            return;
        }
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
                        run.touched = Some(KeySet::new());
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
                        let next = keys.take(batch);
                        if next.is_empty() {
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
                                .into_iter()
                                .filter(|k| touched.is_none_or(|t| !t.contains(k)))
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
                    since,
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
                    for k in &resolved {
                        t.insert(k);
                    }
                }
                let keyless = fired.error.is_some()
                    && fired.failed_keys.is_empty()
                    && fired.rejected.is_empty();
                let (retry, dropped) = st.judge(fired.rejected, fired.keys_done > 0);
                st.report_dropped(&d.rule, &dropped, now_ms);
                let mut again: KeySet = fired.failed_keys.into_iter().collect();
                for k in &retry {
                    again.insert(k);
                }
                for k in &struck {
                    if !again.contains(k) {
                        st.strikes.remove(k);
                    }
                }
                match fired.error {
                    _ if again.is_empty() && !keyless => {
                        st.backoff.ok();
                        target_ok = !fired.target_error;
                    }
                    err => {
                        let err = err.unwrap_or_else(|| "some actions failed".into());
                        // Back into the buffer, ahead of what was marked meanwhile.
                        let mut keys = again;
                        keys.extend(std::mem::take(&mut st.buffer.keys));
                        st.buffer.keys = keys;
                        if keep_queries {
                            for q in queries {
                                st.buffer.queries.entry(q.id()).or_insert(q);
                            }
                        }
                        if let Some(s) = since {
                            st.buffer.hold(&s, since_ts);
                        }
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
                st.report_dropped(&d.rule, &dropped, now_ms);
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
                        let total = keys.len();
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
                        info!(rule = %d.rule, "reset step {}: {} stray keys", run.step + 1, per_ep.iter().map(KeySet::len).sum::<u64>());
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
                    "pending_keys": st.buffer.keys.len(),
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
        self.health.set_status(json!({
            "instance": self.cfg.instance,
            "position": resume.to_string(),
            "stream_lag_ms": self.stream_lag_ms(),
            "rules": summary,
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
    into.keys.extend(m.keys);
    into.queries.extend(m.queries);
    into.rows += m.rows;
}

fn first_words(q: &str) -> String {
    let w: Vec<&str> = q.split_whitespace().take(4).collect();
    w.join(" ")
}

async fn flush_job(
    env: &Env,
    rule: &Rule,
    spec: FenceSpec,
    mut keys: Vec<String>,
    queries: Vec<KeyQuery>,
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
        match action::resolve_keys(env, &queries).await {
            Ok(found) => {
                let mut seen: KeySet = keys.iter().map(String::as_str).collect();
                for k in found {
                    if seen.insert(&k) {
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
mod tests {
    use super::*;

    #[test]
    fn managed_service_notes() {
        assert!(is_managed(Some("/rdsdbbin/mysql-8.4.8.R1/"), false));
        assert!(is_managed(None, true));
        assert!(!is_managed(Some("/usr/"), false));
        let notes =
            |r: std::result::Result<Option<u64>, String>, g: Option<&str>| managed_notes(&r, g);
        assert!(notes(Ok(Some(24)), Some("ON")).is_empty());
        let short = notes(Ok(Some(2)), Some("ON"));
        assert!(short.len() == 1 && short[0].0 && short[0].1.contains("2 hours"));
        assert!(notes(Ok(None), None)[0].1.contains("not set"));
        assert!(notes(Err("denied".into()), None)[0]
            .1
            .contains("could not be read (denied)"));
        let tip = notes(Ok(Some(48)), Some("OFF"));
        assert!(tip.len() == 1 && !tip[0].0 && tip[0].1.contains("gtid_mode=ON"));
        // MariaDB: no gtid_mode to read, no tip.
        assert!(notes(Ok(Some(48)), None).is_empty());
    }

    #[test]
    fn debounce_and_max_delay() {
        let mut b = Buffer::default();
        assert!(b
            .due(Duration::from_secs(1), Duration::from_secs(5))
            .is_none());
        let t0 = Instant::now();
        b.keys.insert("a");
        b.first = Some(t0);
        b.last = Some(t0 + Duration::from_secs(3));
        assert_eq!(
            b.due(Duration::from_secs(1), Duration::from_secs(5)),
            Some(t0 + Duration::from_secs(4))
        );
        b.last = Some(t0 + Duration::from_millis(4500));
        assert_eq!(
            b.due(Duration::from_secs(1), Duration::from_secs(5)),
            Some(t0 + Duration::from_secs(5))
        );
        let p = |n| Position {
            file: "binlog.000001".into(),
            pos: n,
        };
        b.hold(&p(50), 0);
        b.hold(&p(10), 0);
        b.hold(&p(30), 0);
        assert_eq!(b.since, Some(p(10)));
    }

    #[test]
    fn backoff_grows_and_caps() {
        let mut b = Backoff::default();
        assert_eq!(b.fail(Duration::from_secs(60)), Duration::from_millis(500));
        assert_eq!(b.fail(Duration::from_secs(60)), Duration::from_secs(1));
        for _ in 0..20 {
            b.fail(Duration::from_secs(60));
        }
        assert_eq!(b.fail(Duration::from_secs(60)), Duration::from_secs(60));
        b.ok();
        assert!(b.blocked(Instant::now()).is_none());
    }
}
