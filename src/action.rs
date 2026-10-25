//! The work behind a flush or a reset step: wait until the changes read are visible, find the
//! keys (`keys_sql`), read the documents (all `selects` concurrently), render and send.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use mysql_async::prelude::*;
use mysql_async::{Params, Pool, Row, Value as My};
use serde_json::{json, Value};

use crate::binlog::{Flavor, Position};
use crate::docs::{self, Doc, Merger, Plan};
use crate::keyset::KeySet;
use crate::model::{FirePer, StepKind};
use crate::rule::{self, Rule};
use crate::target::{Sent, Target};

pub struct Env {
    pub pool: Pool,
    pub flavor: Flavor,
    pub visibility_delay: Duration,
    pub send_concurrency: usize,
    /// 0 unknown, 1 the server reports a consistent-snapshot binlog position, 2 it does not.
    snapshot_fence: AtomicU8,
}

impl Env {
    pub fn new(
        pool: Pool,
        flavor: Flavor,
        visibility_delay: Duration,
        send_concurrency: usize,
    ) -> Env {
        Env {
            pool,
            flavor,
            visibility_delay,
            send_concurrency,
            snapshot_fence: AtomicU8::new(0),
        }
    }
}

/// What a flush waits for before reading: the last transaction it covers.
#[derive(Debug, Clone)]
pub struct FenceSpec {
    pub gtid: Option<String>,
    pub pos: Position,
}

/// Waits until every transaction up to `spec` is visible to readers (binlog events are shipped
/// before the engine commit completes). MySQL / Percona with GTIDs:
/// `WAIT_FOR_EXECUTED_GTID_SET`; servers that commit in binlog order and report the binlog
/// position of a consistent snapshot (MariaDB, Percona): that position; else
/// `LAZYWF_VISIBILITY_DELAY_MS`.
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
    if !env.visibility_delay.is_zero() {
        tokio::time::sleep(env.visibility_delay).await;
    }
    Ok(())
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
            let mut conn = env.pool.get_conn().await?;
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
                    let mut conn = env.pool.get_conn().await?;
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
    for r in results {
        match r {
            Ok(Sent::Ok) => sent = Sent::Ok,
            Ok(Sent::Skipped) => {}
            Err(e) if crate::target::is_rejected(&e) => return Err((e, Fail::Rejected)),
            Err(e) => transient = transient.or(Some(e)),
        }
    }
    match transient {
        Some(e) => Err((e, Fail::Transient)),
        None => Ok(sent),
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

/// Splits a group so each rendered action stays within `batch_max_bytes`.
fn split_by_bytes(
    rule: &Rule,
    target: &Target,
    template: &str,
    keys: &[String],
    docs: &[Doc],
    reset: &Value,
) -> Result<Vec<(usize, usize)>> {
    let Some(max) = rule.def.batch_max_bytes else {
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
    let mut conn = env.pool.get_conn().await?;
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
    let StepKind::Stray {
        keys_path,
        next_path,
        ..
    } = &s.kind
    else {
        bail!("reset step {step} is not a stray step");
    };
    let mut listed: Vec<KeySet> = Vec::new();
    for (i, ep) in target.endpoints.iter().enumerate() {
        let mut keys = KeySet::new();
        let mut next = Value::Null;
        for page in 0..MAX_PAGES {
            let reset = json!({"step": s.name, "keys": [], "next": next, "page": page});
            let ctx = context(rule, &target, &[], &[], &ep.ctx, &reset);
            let req = rule.templates.render(&rule::list_template(step), &ctx)?;
            let resp = target
                .fetch(i, &req)
                .await
                .with_context(|| format!("listing page {page}"))?;
            let page_keys: Vec<String> = keys_path
                .get(&resp)
                .into_iter()
                .map(crate::template::text)
                .filter(|k| crate::template::valid_key(k))
                .collect();
            let fresh = page_keys.iter().filter(|k| keys.insert(k)).count();
            let Some(np) = next_path else { break };
            let nv = np.get_one(&resp).cloned().unwrap_or(Value::Null);
            // HTTP: a page without new keys ends the listing (a scroll keeps its cursor, so an
            // unchanged cursor proves nothing). Redis SCAN pages may be empty mid-way: there the
            // cursor decides (`0` ends it).
            let done = nv.is_null()
                || nv == json!(0)
                || nv == json!("0")
                || if target.is_http() {
                    fresh == 0
                } else {
                    nv == next
                };
            if done {
                break;
            }
            next = nv;
        }
        listed.push(keys);
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
}
