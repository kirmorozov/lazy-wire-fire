//! The work behind a flush or a reset step: wait until the changes read are visible, find the
//! keys (`keys_sql`), read the documents (all `selects` concurrently), render and send.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use mysql_async::prelude::*;
use mysql_async::{Conn, Params, Pool, Row, Value as My};
use serde_json::{json, Value};
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

use crate::binlog::{Flavor, Position};
use crate::docs::{self, Doc, Merger, Plan};
use crate::fence;
use crate::keyset::KeySet;
use crate::model::{FirePer, StepKind};
use crate::rule::{self, Rule};
use crate::target::{Item, Sent, Target};

/// The commit fence's row id: one row, changed by every commit.
const FENCE_ID: &str = "lazywf";
/// Probes remembered at once; past this the memo starts over (pool reconnects make new ids).
const PROBE_MEMO_MAX: usize = 64;

pub struct Env {
    /// lazywf's own reads and writes (fence, status, config, the instance lock): the main account.
    pub pool: Pool,
    /// The SQL that the configuration defines (selects, `keys_sql`, snapshot keys, reset-step
    /// expressions and `expect` conditions): the query account; the same pool when unset.
    pub query: Pool,
    pub flavor: Flavor,
    pub visibility_delay: Duration,
    pub send_concurrency: usize,
    /// 0 unknown, 1 the server reports a consistent-snapshot binlog position, 2 it does not.
    snapshot_fence: AtomicU8,
    /// The commit-fence probe of each server connection, by connection id (see `commit_fence`).
    probes: StdMutex<HashMap<u32, Probe>>,
    /// The flush boundary the last commit fence was written for. Held across the write, so
    /// flushes at one boundary write once.
    fenced_at: TokioMutex<Option<Position>>,
    /// The WARN for each cause was logged (once per process).
    warned_unordered: AtomicBool,
    warned_probe: AtomicBool,
    warned_write: AtomicBool,
}

impl Env {
    pub fn new(
        pool: Pool,
        query: Pool,
        flavor: Flavor,
        visibility_delay: Duration,
        send_concurrency: usize,
    ) -> Env {
        Env {
            pool,
            query,
            flavor,
            visibility_delay,
            send_concurrency,
            snapshot_fence: AtomicU8::new(0),
            probes: StdMutex::new(HashMap::new()),
            fenced_at: TokioMutex::new(None),
            warned_unordered: AtomicBool::new(false),
            warned_probe: AtomicBool::new(false),
            warned_write: AtomicBool::new(false),
        }
    }
}

/// What a flush waits for before reading: the last transaction it covers.
#[derive(Debug, Clone)]
pub struct FenceSpec {
    pub gtid: Option<String>,
    pub pos: Position,
}

/// The commit-fence probe of one server connection (`fence::probe`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// `binlog_order_commits` is on: a commit fence is exact.
    Ordered,
    /// Commits are not in binlog order: no commit fence.
    Unordered,
    /// The probe failed (for example no CREATE privilege on the schema): no commit fence.
    Failed,
}

/// The fence for a flush whose exact paths (GTID, consistent snapshot) are unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Path {
    /// One commit-fence write on the source connection.
    Commit,
    /// Sleep `LAZYWF_VISIBILITY_DELAY_MS`.
    Delay,
}

/// The decision table of the commit fence: MySQL / Percona (`Flavor::MySql`) with the probe
/// `Ordered` writes the fence; MariaDB never does (its snapshot position is exact); every other
/// probe result sleeps the delay.
pub fn path(flavor: Flavor, probe: Probe) -> Path {
    match (flavor, probe) {
        (Flavor::MySql, Probe::Ordered) => Path::Commit,
        _ => Path::Delay,
    }
}

/// Whether a commit fence was already written for boundary `pos` (`last`: the boundary of the
/// last write). Commits are in binlog order, so one write covers every flush at that boundary.
fn already_fenced(last: &Option<Position>, pos: &Position) -> bool {
    last.as_ref() == Some(pos)
}

fn probe_cached(memo: &HashMap<u32, Probe>, conn: u32) -> Option<Probe> {
    memo.get(&conn).copied()
}

/// Remembers the probe of connection `conn`; a full memo starts over (see `PROBE_MEMO_MAX`).
fn probe_remember(memo: &mut HashMap<u32, Probe>, conn: u32, probe: Probe) {
    if memo.len() >= PROBE_MEMO_MAX && !memo.contains_key(&conn) {
        memo.clear();
    }
    memo.insert(conn, probe);
}

/// `true` for the first call with this flag (the WARN of a cause is logged once per process).
fn warn_once(flag: &AtomicBool) -> bool {
    !flag.swap(true, Ordering::Relaxed)
}

/// Waits until every transaction up to `spec` is visible to readers (binlog events are shipped
/// before the engine commit completes). MySQL / Percona with GTIDs:
/// `WAIT_FOR_EXECUTED_GTID_SET`; servers that commit in binlog order and report the binlog
/// position of a consistent snapshot (MariaDB, Percona): that position. MySQL / Percona with
/// neither (no GTID, no snapshot position): the commit fence (`commit_fence`), when commits are
/// in binlog order. Else `LAZYWF_VISIBILITY_DELAY_MS`.
pub async fn fence(env: &Env, spec: &FenceSpec) -> Result<()> {
    let mut conn = env.pool.get_conn().await.context("fence connection")?;
    if env.flavor == Flavor::MySql {
        if let Some(g) = &spec.gtid {
            let r: Result<Option<Option<i64>>, _> = conn
                .exec_first("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 5)", (g,))
                .await;
            match r {
                Ok(Some(Some(0))) => return Ok(()),
                Ok(Some(Some(_))) => bail!("transaction {g} not visible after 5 s"),
                Ok(_) => {}
                Err(e) => tracing::debug!("gtid fence unavailable: {e}"),
            }
        }
    }
    if env.snapshot_fence.load(Ordering::Relaxed) != 2 && !spec.pos.file.is_empty() {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut pause = Duration::from_millis(1);
        loop {
            conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await?;
            conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await?;
            let rows: Result<Vec<(String, String)>, _> = conn
                .query("SHOW SESSION STATUS LIKE 'binlog_snapshot_%'")
                .await;
            conn.query_drop("COMMIT").await?;
            let (mut file, mut pos) = (None, None);
            for (k, v) in rows.unwrap_or_default() {
                match k.to_ascii_lowercase().as_str() {
                    "binlog_snapshot_file" => file = Some(v),
                    "binlog_snapshot_position" => pos = v.trim().parse::<u64>().ok(),
                    _ => {}
                }
            }
            match (file, pos) {
                (Some(file), Some(pos)) if !file.is_empty() => {
                    env.snapshot_fence.store(1, Ordering::Relaxed);
                    if (Position { file, pos }) >= spec.pos {
                        return Ok(());
                    }
                    if Instant::now() >= deadline {
                        bail!("changes up to {} not visible after 5 s", spec.pos);
                    }
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_millis(50));
                }
                _ => {
                    env.snapshot_fence.store(2, Ordering::Relaxed);
                    break;
                }
            }
        }
    }
    // Neither exact path answered: the commit fence on MySQL / Percona, else the delay.
    if env.flavor == Flavor::MySql && commit_fence(env, &mut conn, &spec.pos).await {
        return Ok(());
    }
    if !env.visibility_delay.is_zero() {
        tokio::time::sleep(env.visibility_delay).await;
    }
    Ok(())
}

/// Commit fence for boundary `pos` on the source connection `conn`. The probe runs once per
/// connection (`fence::probe`, which also creates the fence table); the write is one autocommit
/// row change per boundary, so the binlog carries it and, with `binlog_order_commits`, it
/// returns only after every transaction up to the boundary is committed. `false`: the fence did
/// not apply (not ordered, probe or write failed), and the caller falls back to the delay. Each
/// cause logs one WARN per process.
async fn commit_fence(env: &Env, conn: &mut Conn, pos: &Position) -> bool {
    let id = conn.id();
    let known = probe_cached(&env.probes.lock().expect("probe memo"), id);
    let probe = match known {
        Some(p) => p,
        None => {
            let p = match fence::probe(conn).await {
                Ok(true) => Probe::Ordered,
                Ok(false) => Probe::Unordered,
                Err(e) => {
                    if warn_once(&env.warned_probe) {
                        warn!("commit fence probe failed: {e:#}; flushes wait LAZYWF_VISIBILITY_DELAY_MS instead");
                    }
                    Probe::Failed
                }
            };
            probe_remember(&mut env.probes.lock().expect("probe memo"), id, p);
            p
        }
    };
    if path(env.flavor, probe) != Path::Commit {
        if probe == Probe::Unordered && warn_once(&env.warned_unordered) {
            warn!("commit fence unavailable: binlog_order_commits is OFF; flushes wait LAZYWF_VISIBILITY_DELAY_MS instead (ON gives an exact fence)");
        }
        return false;
    }
    let mut last = env.fenced_at.lock().await;
    if already_fenced(&last, pos) {
        return true;
    }
    match fence::commit(conn, FENCE_ID).await {
        Ok(()) => {
            *last = Some(pos.clone());
            true
        }
        Err(e) => {
            if warn_once(&env.warned_write) {
                warn!("commit fence write failed: {e:#}; flushes wait LAZYWF_VISIBILITY_DELAY_MS instead");
            }
            false
        }
    }
}

/// A trigger's `keys_sql` with the values of one row image. It carries the statement it was
/// marked with, so a trigger edited or removed before the flush does not strand or drop it.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyQuery {
    pub trigger: u64,
    pub sql: Arc<str>,
    pub params: Vec<Value>,
}

impl KeyQuery {
    pub fn id(&self) -> String {
        format!(
            "{}:{}:{}",
            self.trigger,
            self.sql,
            Value::Array(self.params.clone())
        )
    }
}

pub fn to_param(v: &Value) -> My {
    match v {
        Value::Null => My::NULL,
        Value::Bool(b) => My::Int(i64::from(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                My::Int(i)
            } else if let Some(u) = n.as_u64() {
                My::UInt(u)
            } else {
                My::Bytes(n.to_string().into_bytes())
            }
        }
        Value::String(s) => My::Bytes(s.clone().into_bytes()),
        other => My::Bytes(other.to_string().into_bytes()),
    }
}

fn key_column(row: &Row) -> Option<usize> {
    row.columns_ref()
        .iter()
        .position(|c| c.name_str() == "_key")
}

/// The `_key` values of `rows` (keys with control characters are dropped, see
/// [`crate::template::valid_key`]).
fn keys_of(rows: Vec<Row>) -> Vec<String> {
    let Some(i) = rows.first().and_then(key_column) else {
        return Vec::new();
    };
    let mut bad = 0;
    let keys: Vec<String> = rows
        .into_iter()
        .filter_map(|mut r| r.take::<My, _>(i).and_then(|v| docs::key_text(&v)))
        .filter(|k| {
            let ok = crate::template::valid_key(k);
            bad += usize::from(!ok && !k.is_empty());
            ok
        })
        .collect();
    if bad > 0 {
        tracing::warn!("{bad} `_key` values hold control characters and are ignored");
    }
    keys
}

/// Runs the key queries of a flush; returns the keys they found.
pub async fn resolve_keys(env: &Env, queries: &[KeyQuery]) -> Result<Vec<String>> {
    // Futures collected first: closures inside stream combinators trip `Send` inference.
    let futs: Vec<_> = queries
        .iter()
        .map(|q| async move {
            let mut conn = env.query.get_conn().await?;
            let params: Vec<My> = q.params.iter().map(to_param).collect();
            let rows: Vec<Row> = conn
                .exec(&*q.sql, Params::Positional(params))
                .await
                .with_context(|| format!("trigger {} keys_sql", q.trigger))?;
            Ok::<_, anyhow::Error>(keys_of(rows))
        })
        .collect();
    let lists: Vec<Vec<String>> = stream::iter(futs).buffer_unordered(8).try_collect().await?;
    Ok(lists.into_iter().flatten().collect())
}

const SELECT_CHUNK: usize = 1000;

/// Parameter count for `n` keys: the next power of two (at most 1024), so a select is prepared
/// in at most 11 shapes; the extra slots repeat the last key, harmless inside `IN (…)`.
fn padded(n: usize) -> usize {
    n.max(1).next_power_of_two().min(1024).max(n)
}

/// Documents for `keys`: every select runs concurrently on its own connection.
pub async fn load_docs(env: &Env, rule: &Rule, keys: &[String]) -> Result<Vec<Doc>> {
    if rule.selects.is_empty() {
        return Ok(keys
            .iter()
            .map(|k| Doc {
                key: k.clone(),
                doc: json!({}),
                delete: false,
            })
            .collect());
    }
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(SELECT_CHUNK) {
        let n = padded(chunk.len());
        let params: Vec<My> = (0..n)
            .map(|i| My::Bytes(chunk[i.min(chunk.len() - 1)].clone().into_bytes()))
            .collect();
        let futs: Vec<_> = rule
            .selects
            .iter()
            .map(|s| {
                let params = params.clone();
                async move {
                    // Each `:keys` becomes `n` parameters.
                    let (sql, slots) = crate::sql::expand(&s.sql, |_| n);
                    let mut conn = env.query.get_conn().await?;
                    let stmt = conn
                        .prep(sql.as_str())
                        .await
                        .with_context(|| s.name.clone())?;
                    let names: Vec<String> = stmt
                        .columns()
                        .iter()
                        .map(|c| c.name_str().into_owned())
                        .collect();
                    let plan = Plan::new(&names).with_context(|| s.name.clone())?;
                    let cols = stmt.columns().to_vec();
                    let p = if slots.is_empty() {
                        Params::Empty
                    } else {
                        Params::Positional(
                            params.iter().cycle().take(slots.len()).cloned().collect(),
                        )
                    };
                    let rows: Vec<Row> =
                        conn.exec(&stmt, p).await.with_context(|| s.name.clone())?;
                    let rows = rows
                        .into_iter()
                        .map(|r| {
                            r.unwrap()
                                .into_iter()
                                .enumerate()
                                .map(|(i, v)| docs::to_json(v, cols.get(i)))
                                .collect()
                        })
                        .collect();
                    Ok::<_, anyhow::Error>((plan, rows))
                }
            })
            .collect();
        let results: Vec<(Plan, Vec<Vec<Value>>)> =
            futures_util::future::try_join_all(futs).await?;
        let mut m = Merger::new(chunk);
        for (plan, rows) in &results {
            m.add(plan, rows.clone());
        }
        for w in m.warnings.iter().take(3) {
            tracing::warn!(rule = %rule.name(), "{w}");
        }
        out.extend(m.finish());
    }
    Ok(out)
}

/// Context for one action.
pub fn context(
    rule: &Rule,
    target: &Target,
    keys: &[String],
    docs: &[Doc],
    endpoint: &Value,
    reset: &Value,
) -> Value {
    json!({
        "keys": keys,
        // The contract: `docs` is empty when the rule has no selects (the keys are the data).
        "docs": if rule.selects.is_empty() { Vec::new() } else { docs.iter().map(Doc::to_json).collect::<Vec<_>>() },
        "rule": rule.name(),
        "target": target.ctx,
        "vars": Value::Object(rule.def.vars.clone()),
        "endpoint": endpoint,
        "reset": reset,
    })
}

#[derive(Debug, Default)]
pub struct Fired {
    pub ok: u64,
    pub failed: u64,
    pub skipped: u64,
    pub keys_done: u64,
    /// Not confirmed for a transient reason (or not attempted): fire again.
    pub failed_keys: Vec<String>,
    /// Refused by the target on their own, with why.
    pub rejected: Vec<(String, String)>,
    pub error: Option<String>,
    /// The failure came from the target being unwell (transport, 5xx), not from this data.
    pub target_error: bool,
    pub select_time: Duration,
}

impl Fired {
    /// A job that ended without a result: all its keys stay pending.
    pub fn failed(keys: Vec<String>) -> Fired {
        Fired {
            error: Some("internal error (see the log)".into()),
            failed_keys: keys,
            ..Default::default()
        }
    }
}

/// Why an action did not go through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fail {
    /// The target or the way to it is unwell: everything waits.
    Transient,
    /// This action was refused (or could not be rendered): it may be its data.
    Rejected,
}

/// Sends one action (rendered per endpoint) to every endpoint, or to `only`.
async fn send_all(
    rule: &Rule,
    target: &Target,
    template: &str,
    keys: &[String],
    docs: &[Doc],
    reset: &Value,
    only: Option<usize>,
) -> Result<Sent, (anyhow::Error, Fail)> {
    let eps: Vec<usize> = match only {
        Some(e) => vec![e],
        None => (0..target.endpoints.len()).collect(),
    };
    let mut rendered = Vec::with_capacity(eps.len());
    for &i in &eps {
        let Some(ep) = target.endpoints.get(i) else {
            return Err((
                anyhow!("endpoint {i} of target `{}` is gone", target.def.name),
                Fail::Transient,
            ));
        };
        let ctx = context(rule, target, keys, docs, &ep.ctx, reset);
        let r = rule
            .templates
            .render(template, &ctx)
            .map_err(|e| (e, Fail::Rejected))?;
        rendered.push((i, r));
    }
    let results =
        futures_util::future::join_all(rendered.iter().map(|(i, r)| target.send(*i, r))).await;
    let mut sent = Sent::Skipped;
    let mut transient = None;
    let mut rejected = None;
    for r in results {
        match r {
            Ok(Sent::Ok) => sent = Sent::Ok,
            Ok(Sent::Skipped) => {}
            Err(e) if crate::target::is_rejected(&e) => rejected = rejected.or(Some(e)),
            Err(e) => transient = transient.or(Some(e)),
        }
    }
    // A refusal is judged only once every endpoint has answered: an endpoint that is unavailable
    // may still accept the action, so it makes the failure transient (no strike for the key).
    match (transient, rejected) {
        (Some(e), _) => Err((e, Fail::Transient)),
        (None, Some(e)) => Err((e, Fail::Rejected)),
        (None, None) => Ok(sent),
    }
}

/// Sends `keys` as one action; a rejected action is split until the keys it fails on stand
/// alone (they go to `f.rejected`). `Err`: a transient failure; the keys not confirmed are in
/// `f.failed_keys` and nothing more should be sent now.
async fn send_group(
    rule: &Rule,
    target: &Target,
    template: &str,
    keys: &[String],
    docs: &[Doc],
    reset: &Value,
    only: Option<usize>,
    f: &mut Fired,
) -> Result<(), ()> {
    if target.reports_items() {
        return send_items_group(rule, target, template, keys, docs, reset, only, f).await;
    }
    let mut todo = vec![(0, keys.len())];
    while let Some((a, b)) = todo.pop() {
        if a == b {
            continue;
        }
        match send_all(
            rule,
            target,
            template,
            &keys[a..b],
            &docs[a..b],
            reset,
            only,
        )
        .await
        {
            Ok(s) => {
                if s == Sent::Ok {
                    f.ok += 1;
                } else {
                    f.skipped += 1;
                }
                f.keys_done += (b - a) as u64;
            }
            Err((e, Fail::Transient)) => {
                f.failed += 1;
                f.error = Some(format!("{e:#}"));
                f.target_error = true;
                f.failed_keys.extend(keys[a..b].iter().cloned());
                for (a, b) in todo {
                    f.failed_keys.extend(keys[a..b].iter().cloned());
                }
                return Err(());
            }
            Err((e, Fail::Rejected)) => {
                f.failed += 1;
                if f.error.is_none() {
                    f.error = Some(format!("{e:#}"));
                }
                if b - a == 1 {
                    f.rejected.push((keys[a].clone(), format!("{e:#}")));
                } else {
                    let mid = a + (b - a) / 2;
                    todo.push((mid, b));
                    todo.push((a, mid));
                }
            }
        }
    }
    Ok(())
}

/// One batch request for `keys`, answered per item (see [`Target::send_items`]).
async fn send_batch_items(
    rule: &Rule,
    target: &Target,
    template: &str,
    keys: &[String],
    docs: &[Doc],
    reset: &Value,
    only: Option<usize>,
) -> Result<Vec<Item>, (anyhow::Error, Fail)> {
    let ep = only.unwrap_or(0);
    let Some(endpoint) = target.endpoints.get(ep) else {
        return Err((
            anyhow!("endpoint {ep} of target `{}` is gone", target.def.name),
            Fail::Transient,
        ));
    };
    let ctx = context(rule, target, keys, docs, &endpoint.ctx, reset);
    let rendered = rule
        .templates
        .render(template, &ctx)
        .map_err(|e| (e, Fail::Rejected))?;
    target.send_items(ep, &rendered, keys).await.map_err(|e| {
        let fail = if crate::target::is_rejected(&e) {
            Fail::Rejected
        } else {
            Fail::Transient
        };
        (e, fail)
    })
}

/// Books the answer of one batch request, item by item (`keys[i]` is item `i`). Confirmed items
/// are done, refused items go to `rejected`, items that may pass to `failed_keys`. Returns
/// whether some item may pass (the batches not yet sent then wait).
pub fn book_items(keys: &[String], items: Vec<Item>, f: &mut Fired) -> bool {
    let mut waiting = false;
    for (k, item) in keys.iter().zip(items) {
        match item {
            Item::Ok => f.keys_done += 1,
            Item::Rejected(why) => {
                f.failed += 1;
                if f.error.is_none() {
                    f.error = Some(why.clone());
                }
                f.rejected.push((k.clone(), why));
            }
            Item::Transient(why) => {
                f.failed += 1;
                f.error = Some(why);
                f.target_error = true;
                f.failed_keys.push(k.clone());
                waiting = true;
            }
        }
    }
    waiting
}

/// [`send_group`] for a target that answers per item. A confirmed item is done (`keys_done`);
/// a refused one goes to `rejected` (strikes, no bisect: the service said which one it is);
/// one that may pass again goes to `failed_keys`, and the batches not yet sent wait with it.
/// A whole-request refusal is bisected, as in `send_group`. Keys of a batch are never sent
/// again once their item was confirmed.
async fn send_items_group(
    rule: &Rule,
    target: &Target,
    template: &str,
    keys: &[String],
    docs: &[Doc],
    reset: &Value,
    only: Option<usize>,
    f: &mut Fired,
) -> Result<(), ()> {
    let mut todo = vec![(0, keys.len())];
    while let Some((a, b)) = todo.pop() {
        if a == b {
            continue;
        }
        match send_batch_items(
            rule,
            target,
            template,
            &keys[a..b],
            &docs[a..b],
            reset,
            only,
        )
        .await
        {
            Ok(items) => {
                f.ok += 1;
                if book_items(&keys[a..b], items, f) {
                    return Err(());
                }
            }
            Err((e, Fail::Transient)) => {
                f.failed += 1;
                f.error = Some(format!("{e:#}"));
                f.target_error = true;
                f.failed_keys.extend(keys[a..b].iter().cloned());
                for (a, b) in todo {
                    f.failed_keys.extend(keys[a..b].iter().cloned());
                }
                return Err(());
            }
            Err((e, Fail::Rejected)) => {
                f.failed += 1;
                if f.error.is_none() {
                    f.error = Some(format!("{e:#}"));
                }
                if b - a == 1 {
                    f.rejected.push((keys[a].clone(), format!("{e:#}")));
                } else {
                    let mid = a + (b - a) / 2;
                    todo.push((mid, b));
                    todo.push((a, mid));
                }
            }
        }
    }
    Ok(())
}

/// Splits a group so each rendered action stays within `batch_max_bytes` (the kind's own
/// limit when the rule sets none).
fn split_by_bytes(
    rule: &Rule,
    target: &Target,
    template: &str,
    keys: &[String],
    docs: &[Doc],
    reset: &Value,
) -> Result<Vec<(usize, usize)>> {
    let Some(max) = rule.def.batch_max_bytes.or(target.batch_bytes_limit()) else {
        return Ok(vec![(0, keys.len())]);
    };
    let ep = target
        .endpoints
        .first()
        .map(|e| e.ctx.clone())
        .unwrap_or(Value::Null);
    let mut out = Vec::new();
    let mut todo = vec![(0, keys.len())];
    while let Some((a, b)) = todo.pop() {
        let len = rule
            .templates
            .render(
                template,
                &context(rule, target, &keys[a..b], &docs[a..b], &ep, reset),
            )?
            .len();
        if len <= max || b - a <= 1 {
            out.push((a, b));
        } else {
            let mid = a + (b - a) / 2;
            todo.push((mid, b));
            todo.push((a, mid));
        }
    }
    Ok(out)
}

/// Sends the actions for `keys` with their documents (one per key, or in byte-limited
/// groups), to every endpoint or to `only`.
async fn fire_docs(
    env: &Env,
    rule: &Rule,
    target: &Target,
    keys: &[String],
    docs: &[Doc],
    template: &str,
    reset: &Value,
    only: Option<usize>,
    f: &mut Fired,
) -> Result<(), ()> {
    match rule.def.fire_per {
        FirePer::Key => {
            // Concurrently, each key on its own.
            let futs: Vec<_> = keys
                .iter()
                .zip(docs)
                .map(|(k, d)| async move {
                    let mut one = Fired::default();
                    let r = send_group(
                        rule,
                        target,
                        template,
                        std::slice::from_ref(k),
                        std::slice::from_ref(d),
                        reset,
                        only,
                        &mut one,
                    )
                    .await;
                    (one, r)
                })
                .collect();
            let results: Vec<(Fired, Result<(), ()>)> = stream::iter(futs)
                .buffer_unordered(env.send_concurrency)
                .collect()
                .await;
            let mut res = Ok(());
            for (one, r) in results {
                f.ok += one.ok;
                f.skipped += one.skipped;
                f.failed += one.failed;
                f.keys_done += one.keys_done;
                f.failed_keys.extend(one.failed_keys);
                f.rejected.extend(one.rejected);
                if one.target_error {
                    f.target_error = true;
                    f.error = one.error;
                } else if f.error.is_none() {
                    f.error = one.error;
                }
                res = res.and(r);
            }
            res
        }
        FirePer::Batch => {
            let parts = match split_by_bytes(rule, target, template, keys, docs, reset) {
                Ok(p) => p,
                Err(e) => {
                    f.error = Some(format!("{e:#}"));
                    f.failed_keys.extend(keys.iter().cloned());
                    return Err(());
                }
            };
            for (n, (a, b)) in parts.iter().enumerate() {
                if send_group(
                    rule,
                    target,
                    template,
                    &keys[*a..*b],
                    &docs[*a..*b],
                    reset,
                    only,
                    f,
                )
                .await
                .is_err()
                {
                    for (a, b) in &parts[n + 1..] {
                        f.failed_keys.extend(keys[*a..*b].iter().cloned());
                    }
                    return Err(());
                }
            }
            Ok(())
        }
    }
}

/// Reads the documents for `keys` and fires them, group by group. A transient failure stops
/// it; the keys not confirmed are in `failed_keys`, the refused ones in `rejected`.
pub async fn fire_keys(
    env: &Env,
    rule: &Rule,
    keys: Vec<String>,
    template: &str,
    reset: Value,
    docs_override: Option<Vec<Doc>>,
) -> Fired {
    let mut f = Fired::default();
    let Some(target) = rule.target.clone() else {
        f.error = Some(rule.error.clone().unwrap_or_else(|| "no target".into()));
        f.failed_keys = keys;
        return f;
    };
    if let Some(e) = &rule.error {
        f.error = Some(e.clone());
        f.failed_keys = keys;
        return f;
    }
    let group = rule.group_size().max(1);
    let mut chunks: Vec<Vec<String>> = keys.chunks(group).map(<[String]>::to_vec).collect();
    let mut overrides = docs_override.map(|d| {
        d.into_iter()
            .map(|d| (d.key.clone(), d))
            .collect::<HashMap<_, _>>()
    });
    let mut i = 0;
    while i < chunks.len() {
        let unit = std::mem::take(&mut chunks[i]);
        i += 1;
        let t0 = Instant::now();
        let docs = match &mut overrides {
            Some(o) => Ok(unit
                .iter()
                .map(|k| {
                    o.remove(k).unwrap_or(Doc {
                        key: k.clone(),
                        doc: json!({}),
                        delete: true,
                    })
                })
                .collect()),
            None => load_docs(env, rule, &unit).await,
        };
        f.select_time += t0.elapsed();
        let docs: Vec<Doc> = match docs {
            Ok(d) => d,
            Err(e) => {
                f.error = Some(format!("{e:#}"));
                f.failed_keys.extend(unit);
                break;
            }
        };
        if fire_docs(
            env, rule, &target, &unit, &docs, template, &reset, None, &mut f,
        )
        .await
        .is_err()
        {
            break;
        }
    }
    for c in chunks {
        f.failed_keys.extend(c);
    }
    f
}

/// A single action without keys (a `once` reset step).
pub async fn fire_once(rule: &Rule, template: &str, reset: Value) -> Fired {
    let mut f = Fired::default();
    let target = match (&rule.target, &rule.error) {
        (Some(t), None) => t.clone(),
        (_, e) => {
            f.error = Some(e.clone().unwrap_or_else(|| "no target".into()));
            return f;
        }
    };
    match send_all(rule, &target, template, &[], &[], &reset, None).await {
        Ok(Sent::Ok) => f.ok = 1,
        Ok(Sent::Skipped) => f.skipped = 1,
        Err((e, fail)) => {
            f.failed = 1;
            f.error = Some(format!("{e:#}"));
            f.target_error = fail == Fail::Transient;
        }
    }
    f
}

/// Every key, from `snapshot_keys_sql`.
pub async fn snapshot_keys(env: &Env, rule: &Rule) -> Result<KeySet> {
    let q = rule
        .def
        .snapshot_keys_sql
        .as_deref()
        .ok_or_else(|| anyhow!("the rule has no snapshot_keys_sql"))?;
    let mut conn = env.query.get_conn().await?;
    let rows: Vec<Row> = conn.exec(q, ()).await.context("snapshot_keys_sql")?;
    Ok(keys_of(rows).into_iter().collect())
}

const MAX_PAGES: usize = 1_000_000;

/// Stray candidates per endpoint for reset step `step`: the target's keys (paged with the
/// step's `list_template`) that the snapshot, read after the listing, does not have.
pub async fn list_strays(env: &Env, rule: &Rule, step: usize) -> Result<Vec<KeySet>> {
    let target = rule
        .target
        .clone()
        .ok_or_else(|| anyhow!(rule.error.clone().unwrap_or_else(|| "no target".into())))?;
    if let Some(e) = &rule.error {
        bail!("{e}");
    }
    let s = rule
        .step(step)
        .ok_or_else(|| anyhow!("no reset step {step}"))?;
    let StepKind::Stray { keys, next, .. } = &s.kind else {
        bail!("reset step {step} is not a stray step");
    };
    let mut exprs = vec![keys.as_str()];
    exprs.extend(next.as_deref());
    let read = crate::response::Values::new(&exprs)?;
    let mut listed: Vec<KeySet> = Vec::new();
    for (i, ep) in target.endpoints.iter().enumerate() {
        let mut listed_keys = KeySet::new();
        let mut cursor = Value::Null;
        for page in 0..MAX_PAGES {
            let reset = json!({"step": s.name, "keys": [], "next": cursor, "page": page});
            let ctx = context(rule, &target, &[], &[], &ep.ctx, &reset);
            let req = rule.templates.render(&rule::list_template(step), &ctx)?;
            let resp = target
                .fetch(i, &req, read.uses)
                .await
                .with_context(|| format!("listing page {page}"))?;
            let mut got = read
                .eval(&env.query, &resp)
                .await
                .with_context(|| format!("listing page {page}: `keys` / `next`"))?
                .into_iter();
            let page_keys: Vec<String> = match got.next().unwrap_or(Value::Null) {
                Value::Array(a) => a.iter().map(crate::template::text).collect(),
                Value::Null => Vec::new(),
                one => vec![crate::template::text(&one)],
            };
            let fresh = page_keys
                .iter()
                .filter(|k| crate::template::valid_key(k))
                .filter(|k| listed_keys.insert(k))
                .count();
            if next.is_none() {
                break;
            }
            let nv = got.next().unwrap_or(Value::Null);
            // HTTP: a page without new keys ends the listing (a scroll keeps its cursor, so an
            // unchanged cursor proves nothing). Redis SCAN pages may be empty mid-way: there the
            // cursor decides (`0` ends it).
            let done = nv.is_null()
                || nv == json!(0)
                || nv == json!("0")
                || if target.is_http() {
                    fresh == 0
                } else {
                    nv == cursor
                };
            if done {
                break;
            }
            cursor = nv;
        }
        listed.push(listed_keys);
    }
    // Read after the listing: a key created meanwhile is in the snapshot, not a stray.
    let snapshot = snapshot_keys(env, rule).await?;
    Ok(listed
        .iter()
        .map(|l| l.iter().filter(|k| !snapshot.contains(k)).collect())
        .collect())
}

/// Fires deletes for stray `keys` on endpoint `ep` only.
pub async fn fire_strays(
    env: &Env,
    rule: &Rule,
    step: usize,
    ep: usize,
    keys: Vec<String>,
) -> Fired {
    let mut f = Fired::default();
    let target = match (&rule.target, &rule.error) {
        (Some(t), None) => t.clone(),
        (_, e) => {
            f.error = Some(e.clone().unwrap_or_else(|| "no target".into()));
            f.failed_keys = keys;
            return f;
        }
    };
    if ep >= target.endpoints.len() {
        f.error = Some(format!(
            "endpoint {ep} of target `{}` is gone",
            target.def.name
        ));
        f.failed_keys = keys;
        return f;
    }
    let name = rule.step(step).map(|s| s.name.clone()).unwrap_or_default();
    let template = rule.step_action(step);
    let group = rule.group_size().max(1);
    let chunks: Vec<Vec<String>> = keys.chunks(group).map(<[String]>::to_vec).collect();
    for (n, chunk) in chunks.iter().enumerate() {
        let docs: Vec<Doc> = chunk
            .iter()
            .map(|k| Doc {
                key: k.clone(),
                doc: json!({}),
                delete: true,
            })
            .collect();
        let reset = json!({"step": name, "keys": chunk});
        if fire_docs(
            env,
            rule,
            &target,
            chunk,
            &docs,
            &template,
            &reset,
            Some(ep),
            &mut f,
        )
        .await
        .is_err()
        {
            for c in &chunks[n + 1..] {
                f.failed_keys.extend(c.iter().cloned());
            }
            break;
        }
    }
    f
}

pub type SharedRule = Arc<Rule>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn padding() {
        assert_eq!(padded(1), 1);
        assert_eq!(padded(3), 4);
        assert_eq!(padded(1000), 1024);
        assert_eq!(padded(1024), 1024);
    }
    fn at(file: &str, pos: u64) -> Position {
        Position {
            file: file.into(),
            pos,
        }
    }

    #[test]
    fn commit_fence_only_on_mysql_when_ordered() {
        // MySQL / Percona (one flavor) with binlog_order_commits ON: the commit fence.
        assert_eq!(path(Flavor::MySql, Probe::Ordered), Path::Commit);
        // Not ordered, or the probe failed: the delay.
        assert_eq!(path(Flavor::MySql, Probe::Unordered), Path::Delay);
        assert_eq!(path(Flavor::MySql, Probe::Failed), Path::Delay);
        // MariaDB keeps its snapshot position and its delay; it never writes the fence.
        assert_eq!(path(Flavor::MariaDb, Probe::Ordered), Path::Delay);
        assert_eq!(path(Flavor::MariaDb, Probe::Unordered), Path::Delay);
        assert_eq!(path(Flavor::MariaDb, Probe::Failed), Path::Delay);
    }

    #[test]
    fn one_write_per_boundary() {
        let a = at("binlog.000001", 100);
        let b = at("binlog.000001", 200);
        assert!(!already_fenced(&None, &a));
        assert!(already_fenced(&Some(a.clone()), &a));
        // A new boundary (a later flush) needs its own write.
        assert!(!already_fenced(&Some(a), &b));
        assert!(!already_fenced(&Some(b.clone()), &at("binlog.000002", 200)));
    }

    #[test]
    fn probe_is_remembered_per_connection() {
        let mut memo = HashMap::new();
        assert_eq!(probe_cached(&memo, 7), None);
        probe_remember(&mut memo, 7, Probe::Ordered);
        probe_remember(&mut memo, 8, Probe::Unordered);
        assert_eq!(probe_cached(&memo, 7), Some(Probe::Ordered));
        assert_eq!(probe_cached(&memo, 8), Some(Probe::Unordered));
        // A reconnect is a new connection id: it is probed again.
        assert_eq!(probe_cached(&memo, 9), None);
    }

    #[test]
    fn probe_memo_starts_over_when_full() {
        let mut memo = HashMap::new();
        for id in 0..PROBE_MEMO_MAX as u32 {
            probe_remember(&mut memo, id, Probe::Ordered);
        }
        assert_eq!(memo.len(), PROBE_MEMO_MAX);
        // Re-remembering a known id does not clear the memo.
        probe_remember(&mut memo, 0, Probe::Ordered);
        assert_eq!(memo.len(), PROBE_MEMO_MAX);
        probe_remember(&mut memo, 1000, Probe::Failed);
        assert_eq!(memo.len(), 1);
        assert_eq!(probe_cached(&memo, 1000), Some(Probe::Failed));
    }

    #[test]
    fn each_cause_warns_once() {
        let flag = AtomicBool::new(false);
        assert!(warn_once(&flag));
        assert!(!warn_once(&flag));
        assert!(!warn_once(&flag));
    }
}
