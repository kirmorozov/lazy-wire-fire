//! Prometheus exposition endpoint: `GET /metrics` on its own address (`LAZYWF_METRICS_ADDR`).
//!
//! The endpoint says what a scrape may learn: the same token as `/status`
//! (`LAZYWF_HEALTH_TOKEN`, checked by [`crate::health::bearer_ok`]), so a scrape that reaches
//! outside the machine must carry it as `Authorization: Bearer <token>` and a miss is a 401 with
//! `WWW-Authenticate: Bearer`. Unset, the listener stays open — the loopback default it shipped
//! with.
//!
//! The production-readiness blocker is that "did it deliver" is only a WARN log, invisible to
//! monitoring. This is the small, bounded signal set that answers it: six metrics over the
//! flush path, the dedup buffer, the binlog lag and the statements the binlog carries as text.
//! Everything is `std::sync::atomic`; there is no metrics crate (house style: no new runtime
//! dependencies, the exposition is hand-rolled next to the hand-rolled health parser in
//! [`crate::health`]).
//!
//! Cardinality is the hazard, so the label sets are frozen at `{rule}`, `{target}` and
//! `{outcome}` — never a key, a refusal reason or an endpoint URL. Full keys belong to the
//! (future) DLQ, not to metrics. The report's leak audit lists exactly what this surface shows:
//! counts and the rule/target names from the loaded config, never row data and never a key.
//!
//! # Hot path
//!
//! The event and flush paths do only atomic operations on handles resolved once when a rule
//! loads ([`Metrics::rule`], [`Metrics::pair`], called from the config reload, never per event).
//! A handle is an `Arc` of atomics the rule holds; incrementing it is one `fetch_add`, no lookup
//! by `String`, no formatting, no lock. Only rendering `/metrics` (rare) allocates.
//!
//! # Cardinality bound
//!
//! The only way to create a labelled entry is [`Metrics::rule`] / [`Metrics::pair`], and the
//! engine calls them solely with the rule and target names of the loaded config
//! ([`crate::engine`]'s `add_rule` / `update_rule`). Those names come from the rule and target
//! tables, so the entry count is bounded by config size, which is bounded by rows. The flush and
//! drop sites never name a rule or target to the registry — they use the pre-resolved handle —
//! so an unknown name cannot create an entry. Removal keeps it exactly the loaded set and not its
//! history: [`Metrics::remove_rule`] (called when the engine removes a rule) and
//! [`Metrics::remove_pair`] (when a reload moves a rule to another target) drop the entries a
//! removed or moved rule would otherwise leave exporting until the process restarts.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The `version` label of `lazywf_build_info`: the crate version (README's version-mismatch habit).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The flush duration buckets, 1 ms .. 30 s: fixed, ten of them, the only set ever used. Bounded
/// by construction — a histogram never grows a bucket at runtime.
pub const BUCKETS: [f64; 10] = [0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 30.0];

/// The three outcomes of one flush, the `outcome` label of `lazywf_flush_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// The keys met `expect`: the rule's position advanced.
    Success,
    /// A transient failure: the keys are kept pending and fire again.
    Retry,
    /// The target refused keys on their own (a permanent refusal), counted before the strike
    /// logic turns the fifth one into a drop.
    Refused,
}

impl FlushOutcome {
    /// The offset in the per-pair counter array.
    fn index(self) -> usize {
        match self {
            FlushOutcome::Success => 0,
            FlushOutcome::Retry => 1,
            FlushOutcome::Refused => 2,
        }
    }

    fn label(self) -> &'static str {
        match self {
            FlushOutcome::Success => "success",
            FlushOutcome::Retry => "retry",
            FlushOutcome::Refused => "refused",
        }
    }

    /// The outcome of one flush completion, from two facts known at `on_done`:
    /// * `had_refusals` — the target refused keys on their own (`fired.rejected` non-empty), read
    ///   before the strike logic splits them into retry-or-drop;
    /// * `expect_met` — nothing is kept pending and the flush was not keyless (the position
    ///   advanced).
    ///
    /// A refusal dominates: the target said no to this data (a permanent refusal) even if the keys
    /// were then dropped on the fifth strike — that is the signal monitoring wants, distinct from a
    /// transient `retry`. Otherwise a completed flush is a `success`, anything else a `retry`.
    pub fn classify(had_refusals: bool, expect_met: bool) -> FlushOutcome {
        if had_refusals {
            FlushOutcome::Refused
        } else if expect_met {
            FlushOutcome::Success
        } else {
            FlushOutcome::Retry
        }
    }
}

/// One rule's `{rule}`-labelled metrics: the pending gauge and the flush-duration histogram.
/// Held by the rule as an `Arc`; the flush path only touches these atomics. The label value
/// (`rule`) lives on the registry key, so the handle carries only the counters.
#[derive(Debug)]
pub struct RuleHandle {
    /// `lazywf_pending_keys{rule}`: the dedup buffer's pending count, pushed at status time.
    pending_keys: AtomicU64,
    /// `lazywf_flush_duration_seconds{rule}`: per-bucket counts (the bucket whose `le` first
    /// covers the observation), summed cumulatively only at render.
    buckets: Vec<AtomicU64>,
    sum_bits: AtomicU64,
    count: AtomicU64,
}

impl RuleHandle {
    fn new() -> Self {
        RuleHandle {
            pending_keys: AtomicU64::new(0),
            buckets: (0..BUCKETS.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_bits: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Publishes the rule's pending key count (read from the dedup buffer at status time).
    pub fn set_pending(&self, n: u64) {
        self.pending_keys.store(n, Ordering::Relaxed);
    }

    /// Records one flush's wall time. Atomic only: one bucket bump, a sum CAS and a count bump.
    pub fn observe_flush(&self, d: Duration) {
        let secs = d.as_secs_f64();
        if let Some(i) = BUCKETS.iter().position(|&b| secs <= b) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let mut cur = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(cur) + secs).to_bits();
            match self.sum_bits.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }
}

/// One (rule, target) pair's metrics: the flush counters by outcome and the dropped-key counter.
/// Held by the rule as an `Arc` for its current target. The label values live on the registry
/// key (`(rule, target)`), so the handle carries only the counters.
#[derive(Debug)]
pub struct PairHandle {
    /// `lazywf_flush_total{rule,target,outcome}` indexed by [`FlushOutcome::index`].
    outcome: [AtomicU64; 3],
    /// `lazywf_dropped_keys_total{rule,target}`: bumped once per key at the strike-5 drop.
    dropped: AtomicU64,
}

impl PairHandle {
    fn new() -> Self {
        PairHandle {
            outcome: std::array::from_fn(|_| AtomicU64::new(0)),
            dropped: AtomicU64::new(0),
        }
    }

    /// Counts one flush completion under `outcome`.
    pub fn flush(&self, outcome: FlushOutcome) {
        self.outcome[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Counts `n` keys dropped after the fifth strike (the same place the `lazywf::undelivered`
    /// WARN fires).
    pub fn dropped(&self, n: u64) {
        self.dropped.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct Registry {
    rules: BTreeMap<String, Arc<RuleHandle>>,
    pairs: BTreeMap<(String, String), Arc<PairHandle>>,
}

/// The shared registry: bounded label sets plus the instance-wide lag gauge. `Arc`'d: the engine
/// writes atomics through handles it resolved at rule load, the listener renders them on scrape.
#[derive(Debug, Default)]
pub struct Metrics {
    inner: Mutex<Registry>,
    /// `lazywf_binlog_lag_seconds{}`: the stream lag (f64 seconds as bits), no labels.
    binlog_lag_bits: AtomicU64,
    /// `lazywf_statement_events_ignored_total`: DML that arrived as a statement-format QUERY
    /// event (a session switched its own `binlog_format` away from `ROW`), so its rows are not in
    /// the stream and the rules on its table missed the change until they are rebuilt. Counted
    /// per event; the WARN line is rate limited by the engine's `note_statement_dml` to one per
    /// table per interval. No labels: the table belongs to the log line, not to the surface.
    statement_events_ignored: AtomicU64,
    /// `lazywf_binlog_undecodable_total`: events the reader could not decode with this build
    /// (review A-02) — the stream is stuck at the position one of them sits at until a build that
    /// decodes it ships. Counted at both stream sites: the mapping failure the engine backs off
    /// from, and the refused rows of a watched table, which exits the process (so the number a
    /// restart starts back at 0 is itself the crash-loop's signal, next to the ERROR line).
    /// No labels: the position and the decoder's words belong to the log line.
    undecodable: AtomicU64,
}

impl Metrics {
    /// The registry, poisoned or not. Every mutation of it is one whole `BTreeMap` entry — an
    /// `Arc` handle inserted by `entry().or_insert_with()`, or a removal — so what a task that
    /// died holding this lock can leave behind is a handle that is present or absent, never a
    /// half-written one and never a value some later read turns into an outcome. A second panic
    /// here would be worse than stale series: `render` is the scrape, and the engine's flush
    /// accounting resolves its handles through here, where a panic ends the process. (The same
    /// choice `oauth`'s and `gcp`'s token caches make.)
    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The (or the existing) handle of `rule`. Called from the config reload only; the entry set
    /// is bounded by the loaded config.
    pub fn rule(&self, rule: &str) -> Arc<RuleHandle> {
        let mut r = self.registry();
        Arc::clone(
            r.rules
                .entry(rule.to_string())
                .or_insert_with(|| Arc::new(RuleHandle::new())),
        )
    }

    /// The (or the existing) handle of `(rule, target)`. Called from the config reload only.
    pub fn pair(&self, rule: &str, target: &str) -> Arc<PairHandle> {
        let mut r = self.registry();
        Arc::clone(
            r.pairs
                .entry((rule.to_string(), target.to_string()))
                .or_insert_with(|| Arc::new(PairHandle::new())),
        )
    }

    /// Drops `rule`'s `{rule}`-entry and every `(rule, *)` pair entry. Called from the config
    /// reload when the engine removes a rule, so its series stop being exported at once instead
    /// of lingering (and counting toward cardinality) until the process restarts. A flush already
    /// in flight keeps incrementing the `Arc` it resolved, but the registry no longer renders it.
    pub fn remove_rule(&self, rule: &str) {
        let mut r = self.registry();
        r.rules.remove(rule);
        r.pairs.retain(|(rl, _), _| rl != rule);
    }

    /// Drops one `(rule, target)` pair entry, leaving the rule entry and its other pairs. Called
    /// from the config reload when a reload moves a rule to another target: the old target's pair
    /// is no longer produced (the new one was resolved by [`Metrics::pair`]), so only the old key
    /// goes. Keeps the pair set exactly the loaded `{rule,target}` set, not its history.
    pub fn remove_pair(&self, rule: &str, target: &str) {
        let mut r = self.registry();
        r.pairs.remove(&(rule.to_string(), target.to_string()));
    }

    /// Publishes the instance binlog lag (seconds) — the same source as the status table's lag.
    pub fn set_binlog_lag(&self, secs: f64) {
        self.binlog_lag_bits
            .store(secs.to_bits(), Ordering::Relaxed);
    }

    /// Counts one DML that arrived as a statement-format QUERY event, whose rows are therefore
    /// not in the stream (`lazywf_statement_events_ignored_total`). One `fetch_add`, no lookup and
    /// no lock, on an event that is itself rare: the row path never reaches here.
    pub fn statement_event_ignored(&self) {
        self.statement_events_ignored
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one event the reader could not decode with this build
    /// (`lazywf_binlog_undecodable_total`). One `fetch_add`, no lookup and no lock, on an event the
    /// stream either backs off from or dies on — never the hot path.
    pub fn undecodable_event(&self) {
        self.undecodable.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders the exposition (`text/plain; version=0.0.4`). Allocates freely: only a scrape
    /// calls this. The order is deterministic (rules and pairs sorted, outcomes and buckets in
    /// fixed order) so a scrape snapshot is hand-checkable.
    pub fn render(&self) -> String {
        let r = self.registry();
        let mut out = String::new();
        build_info(&mut out);
        binlog_lag(
            &mut out,
            f64::from_bits(self.binlog_lag_bits.load(Ordering::Relaxed)),
        );
        statement_events_ignored(
            &mut out,
            self.statement_events_ignored.load(Ordering::Relaxed),
        );
        undecodable(&mut out, self.undecodable.load(Ordering::Relaxed));
        pending_keys(&mut out, &r.rules);
        flush_total(&mut out, &r.pairs);
        dropped_keys(&mut out, &r.pairs);
        flush_duration(&mut out, &r.rules);
        out
    }

    /// The number of distinct `(rule, target)` entries: bounded by the loaded config. Test hook.
    #[cfg(test)]
    fn entry_count(&self) -> (usize, usize) {
        let r = self.registry();
        (r.rules.len(), r.pairs.len())
    }
}

fn escape(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
}

fn label_str(out: &mut String, key: &str, value: &str) {
    out.push_str(key);
    out.push_str("=\"");
    escape(out, value);
    out.push('"');
}

/// A float in exposition form: no trailing `.0`, but `0` stays `0` (a Prometheus sample value).
fn fmt_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        // Enough precision to round-trip, trimmed of trailing zeros.
        let s = format!("{v:?}");
        s
    }
}

fn build_info(out: &mut String) {
    out.push_str("# HELP lazywf_build_info Build info of this lazywf instance (always 1).\n");
    out.push_str("# TYPE lazywf_build_info gauge\n");
    out.push_str("lazywf_build_info{");
    label_str(out, "version", VERSION);
    out.push_str("} 1\n");
}

fn binlog_lag(out: &mut String, secs: f64) {
    out.push_str("# HELP lazywf_binlog_lag_seconds How far behind the binlog the reader is, in seconds (the status table's stream lag).\n");
    out.push_str("# TYPE lazywf_binlog_lag_seconds gauge\n");
    out.push_str("lazywf_binlog_lag_seconds ");
    out.push_str(&fmt_f64(secs));
    out.push('\n');
}

/// The instance-wide count of the DML the binlog carried as text instead of row events. Not a
/// per-rule or per-table label: the table is on the WARN line, and the label sets are frozen.
fn statement_events_ignored(out: &mut String, n: u64) {
    out.push_str("# HELP lazywf_statement_events_ignored_total DML that arrived as a statement-format QUERY event (a session switched its own binlog_format away from ROW): its rows are not in the binlog, so the rules on its table missed those changes until they are rebuilt.\n");
    out.push_str("# TYPE lazywf_statement_events_ignored_total counter\n");
    out.push_str("lazywf_statement_events_ignored_total ");
    out.push_str(&n.to_string());
    out.push('\n');
}

/// The instance-wide count of the events this build could not decode. No label for the position or
/// the table: those are on the log line the reader writes once per stuck position, and the label
/// sets are frozen.
fn undecodable(out: &mut String, n: u64) {
    out.push_str("# HELP lazywf_binlog_undecodable_total Binlog events this build could not decode: the reader keeps coming back to the position one of them sits at until a build that decodes it ships.\n");
    out.push_str("# TYPE lazywf_binlog_undecodable_total counter\n");
    out.push_str("lazywf_binlog_undecodable_total ");
    out.push_str(&n.to_string());
    out.push('\n');
}

fn pending_keys(out: &mut String, rules: &BTreeMap<String, Arc<RuleHandle>>) {
    out.push_str(
        "# HELP lazywf_pending_keys Keys pending in a rule's dedup buffer, not yet delivered.\n",
    );
    out.push_str("# TYPE lazywf_pending_keys gauge\n");
    for (name, h) in rules {
        out.push_str("lazywf_pending_keys{");
        label_str(out, "rule", name);
        out.push_str("} ");
        out.push_str(&h.pending_keys.load(Ordering::Relaxed).to_string());
        out.push('\n');
    }
}

fn flush_total(out: &mut String, pairs: &BTreeMap<(String, String), Arc<PairHandle>>) {
    out.push_str("# HELP lazywf_flush_total Flushes by outcome: success (expect met), retry (transient, kept pending), refused (permanent refusal).\n");
    out.push_str("# TYPE lazywf_flush_total counter\n");
    for ((rule, target), h) in pairs {
        for outcome in [
            FlushOutcome::Success,
            FlushOutcome::Retry,
            FlushOutcome::Refused,
        ] {
            out.push_str("lazywf_flush_total{");
            label_str(out, "rule", rule);
            out.push(',');
            label_str(out, "target", target);
            out.push(',');
            label_str(out, "outcome", outcome.label());
            out.push_str("} ");
            out.push_str(
                &h.outcome[outcome.index()]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            out.push('\n');
        }
    }
}

fn dropped_keys(out: &mut String, pairs: &BTreeMap<(String, String), Arc<PairHandle>>) {
    out.push_str(
        "# HELP lazywf_dropped_keys_total Keys dropped after the fifth refusal of their target.\n",
    );
    out.push_str("# TYPE lazywf_dropped_keys_total counter\n");
    for ((rule, target), h) in pairs {
        out.push_str("lazywf_dropped_keys_total{");
        label_str(out, "rule", rule);
        out.push(',');
        label_str(out, "target", target);
        out.push_str("} ");
        out.push_str(&h.dropped.load(Ordering::Relaxed).to_string());
        out.push('\n');
    }
}

fn flush_duration(out: &mut String, rules: &BTreeMap<String, Arc<RuleHandle>>) {
    out.push_str("# HELP lazywf_flush_duration_seconds Wall time of a flush, in seconds.\n");
    out.push_str("# TYPE lazywf_flush_duration_seconds histogram\n");
    for (name, h) in rules {
        let mut cumulative = 0u64;
        for (i, &le) in BUCKETS.iter().enumerate() {
            cumulative += h.buckets[i].load(Ordering::Relaxed);
            out.push_str("lazywf_flush_duration_seconds{");
            label_str(out, "rule", name);
            out.push_str(",le=\"");
            out.push_str(&le.to_string());
            out.push_str("\"} ");
            out.push_str(&cumulative.to_string());
            out.push('\n');
        }
        let count = h.count.load(Ordering::Relaxed);
        out.push_str("lazywf_flush_duration_seconds{");
        label_str(out, "rule", name);
        out.push_str(",le=\"+Inf\"} ");
        out.push_str(&count.to_string());
        out.push('\n');
        out.push_str("lazywf_flush_duration_seconds_sum{");
        label_str(out, "rule", name);
        out.push_str("} ");
        out.push_str(&fmt_f64(f64::from_bits(h.sum_bits.load(Ordering::Relaxed))));
        out.push('\n');
        out.push_str("lazywf_flush_duration_seconds_count{");
        label_str(out, "rule", name);
        out.push_str("} ");
        out.push_str(&count.to_string());
        out.push('\n');
    }
}

/// The answer to one request on the metrics listener. `authorization` is the `Authorization`
/// header the request carried and `token` the gate's value (`None`: the listener is open, as it
/// was before the token existed). `/metrics` is the gated path; the index and the unknown paths
/// answer as they always did, and a refusal is the token gate's plain 401.
fn respond(
    m: &Metrics,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    token: Option<&str>,
) -> (u16, String, &'static str) {
    if !matches!(method, "GET" | "HEAD") {
        return (405, "method not allowed\n".into(), "text/plain");
    }
    match path {
        "/metrics" => {
            if let Some(want) = token {
                // Constant-time, and the same rule `/status` follows: one setting, one shape of
                // a valid credential, over both listeners.
                if !crate::health::bearer_ok(authorization, want) {
                    return (401, "unauthorized\n".into(), "text/plain");
                }
            }
            (200, m.render(), "text/plain; version=0.0.4")
        }
        "/" => (
            200,
            "lazywf metrics endpoint: /metrics\n".into(),
            "text/plain",
        ),
        _ => (404, "not found\n".into(), "text/plain"),
    }
}

async fn handle(mut s: TcpStream, m: Arc<Metrics>, token: Option<String>) {
    // The same bounded read as the health endpoint: a cap and a timeout, so a slow or hostile
    // client cannot grow the buffer or hold the task.
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let read = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let n = s.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.windows(2).any(|w| w == b"\n\n") {
                return Some(());
            }
            if buf.len() > 8192 {
                return None;
            }
        }
    })
    .await;
    if !matches!(read, Ok(Some(()))) {
        return;
    }
    let line = String::from_utf8_lossy(&buf);
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/").split('?').next().unwrap_or("/");
    let authorization = crate::health::header_value(&buf, crate::health::AUTHORIZATION_HEADER);
    let (status, body, ctype) =
        respond(&m, method, path, authorization.as_deref(), token.as_deref());
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Service Unavailable",
    };
    // A 401 here is only ever the token gate, and a bearer challenge is what such a refusal owes
    // the client — the same answer `/status` gives.
    let challenge = if status == 401 {
        "WWW-Authenticate: Bearer\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\n{challenge}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = s.write_all(head.as_bytes()).await;
    if method != "HEAD" {
        let _ = s.write_all(body.as_bytes()).await;
    }
    let _ = s.shutdown().await;
}

/// Serves `/metrics` until the process ends; a bind failure is logged and retried (like health).
/// `token` is `LAZYWF_HEALTH_TOKEN` — the same token that gates `/status`: set, it admits only a
/// request carrying `Authorization: Bearer <token>`; unset, the listener stays open.
pub async fn serve(addr: SocketAddr, m: Arc<Metrics>, token: Option<String>) {
    let listener = loop {
        match TcpListener::bind(addr).await {
            Ok(l) => break l,
            Err(e) => {
                tracing::warn!("metrics endpoint: cannot listen on {addr}: {e}; retrying in 5 s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    // The gate the log promises must be the gate the listener applies, so both read the token
    // through the same normalization.
    let token = crate::health::token_of(token);
    let needs = if token.is_some() {
        ", /metrics needs Authorization: Bearer"
    } else {
        ""
    };
    tracing::info!("metrics endpoint on http://{addr} (/metrics{needs})");
    serve_listener(listener, m, token).await;
}

/// The accept loop of [`serve`], over a listener someone else bound: the tests scrape a port of
/// the kernel's choosing instead of racing to grab a free one.
pub(crate) async fn serve_listener(listener: TcpListener, m: Arc<Metrics>, token: Option<String>) {
    // Stored by the health module's one rule (trimmed, blank is no token at all), so neither
    // listener can gate on a value nobody meant to set.
    let token = crate::health::token_of(token);
    loop {
        if let Ok((s, _)) = listener.accept().await {
            tokio::spawn(handle(s, m.clone(), token.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scrape snapshot of a fixed state, hand-checked line by line below.
    fn scenario() -> Arc<Metrics> {
        let m = Arc::new(Metrics::default());
        let r = m.rule("orders");
        let p = m.pair("orders", "search");
        // 12 successes, 3 retries, 2 refused; one key dropped on the fifth strike.
        for _ in 0..12 {
            p.flush(FlushOutcome::Success);
        }
        for _ in 0..3 {
            p.flush(FlushOutcome::Retry);
        }
        for _ in 0..2 {
            p.flush(FlushOutcome::Refused);
        }
        p.dropped(1);
        // 5 pending keys; three flushes timed at 0.5 s, 1 s and 2 s (exact in f64).
        r.set_pending(5);
        r.observe_flush(Duration::from_millis(500)); // <= 0.5 s bucket
        r.observe_flush(Duration::from_secs(1)); // <= 1 s bucket
        r.observe_flush(Duration::from_secs(2)); // <= 5 s bucket
        m
    }

    #[test]
    fn exposition_is_a_hand_checkable_snapshot() {
        let text = scenario().render();
        let want = [
            "# HELP lazywf_build_info Build info of this lazywf instance (always 1).",
            "# TYPE lazywf_build_info gauge",
            concat!(
                "lazywf_build_info{version=\"",
                env!("CARGO_PKG_VERSION"),
                "\"} 1"
            ),
            "# HELP lazywf_binlog_lag_seconds How far behind the binlog the reader is, in seconds (the status table's stream lag).",
            "# TYPE lazywf_binlog_lag_seconds gauge",
            "lazywf_binlog_lag_seconds 0",
            "# HELP lazywf_statement_events_ignored_total DML that arrived as a statement-format QUERY event (a session switched its own binlog_format away from ROW): its rows are not in the binlog, so the rules on its table missed those changes until they are rebuilt.",
            "# TYPE lazywf_statement_events_ignored_total counter",
            "lazywf_statement_events_ignored_total 0",
            "# HELP lazywf_binlog_undecodable_total Binlog events this build could not decode: the reader keeps coming back to the position one of them sits at until a build that decodes it ships.",
            "# TYPE lazywf_binlog_undecodable_total counter",
            "lazywf_binlog_undecodable_total 0",
            "# HELP lazywf_pending_keys Keys pending in a rule's dedup buffer, not yet delivered.",
            "# TYPE lazywf_pending_keys gauge",
            "lazywf_pending_keys{rule=\"orders\"} 5",
            "# HELP lazywf_flush_total Flushes by outcome: success (expect met), retry (transient, kept pending), refused (permanent refusal).",
            "# TYPE lazywf_flush_total counter",
            "lazywf_flush_total{rule=\"orders\",target=\"search\",outcome=\"success\"} 12",
            "lazywf_flush_total{rule=\"orders\",target=\"search\",outcome=\"retry\"} 3",
            "lazywf_flush_total{rule=\"orders\",target=\"search\",outcome=\"refused\"} 2",
            "# HELP lazywf_dropped_keys_total Keys dropped after the fifth refusal of their target.",
            "# TYPE lazywf_dropped_keys_total counter",
            "lazywf_dropped_keys_total{rule=\"orders\",target=\"search\"} 1",
            "# HELP lazywf_flush_duration_seconds Wall time of a flush, in seconds.",
            "# TYPE lazywf_flush_duration_seconds histogram",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.001\"} 0",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.005\"} 0",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.01\"} 0",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.05\"} 0",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.1\"} 0",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.25\"} 0",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.5\"} 1",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"1\"} 2",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"5\"} 3",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"30\"} 3",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"+Inf\"} 3",
            "lazywf_flush_duration_seconds_sum{rule=\"orders\"} 3.5",
            "lazywf_flush_duration_seconds_count{rule=\"orders\"} 3",
        ];
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            want,
            "scrape snapshot differs\n--- got ---\n{text}"
        );
        // Every sample carries only rule / target / outcome / le / version. No key, no reason.
        assert!(!text.contains("400"), "a refusal reason leaked");
        assert!(!text.contains("http"), "an endpoint URL leaked");
    }

    #[test]
    fn counters_move_on_the_flush_outcomes() {
        let m = Metrics::default();
        let p = m.pair("orders", "search");
        assert_eq!(outcome_of(&m, "orders", "search", FlushOutcome::Success), 0);
        p.flush(FlushOutcome::Success);
        p.flush(FlushOutcome::Success);
        p.flush(FlushOutcome::Retry);
        p.flush(FlushOutcome::Refused);
        p.dropped(2);
        assert_eq!(outcome_of(&m, "orders", "search", FlushOutcome::Success), 2);
        assert_eq!(outcome_of(&m, "orders", "search", FlushOutcome::Retry), 1);
        assert_eq!(outcome_of(&m, "orders", "search", FlushOutcome::Refused), 1);
        assert!(m
            .render()
            .contains("lazywf_dropped_keys_total{rule=\"orders\",target=\"search\"} 2"));
    }

    /// DML that arrives as a statement-format QUERY event is counted per event, whatever the
    /// WARN line does: the engine rate limits the lines (one per table per interval), the counter
    /// is what an alert reads. It is instance-wide, so it opens no labelled entry.
    #[test]
    fn statement_events_ignored_counts_every_statement_dml() {
        let m = Metrics::default();
        assert!(
            m.render()
                .contains("lazywf_statement_events_ignored_total 0"),
            "a counter a scrape has never seen still shows"
        );
        for _ in 0..5 {
            m.statement_event_ignored();
        }
        assert!(m
            .render()
            .contains("lazywf_statement_events_ignored_total 5"));
        assert_eq!(m.entry_count(), (0, 0), "no labels, so no registry entry");
    }

    /// An event the reader could not decode is counted per event, at both stream sites: the mapping
    /// failure the engine backs off from, and the refused rows event that exits the process. The
    /// line is rate limited (one per stuck position) and the process may not survive the second
    /// case, so the counter is what an alert reads — and a scrape of a stuck instance must show it.
    #[test]
    fn undecodable_counts_every_event_the_reader_refused() {
        let m = Metrics::default();
        assert!(
            m.render().contains("lazywf_binlog_undecodable_total 0"),
            "a counter a scrape has never seen still shows"
        );
        for _ in 0..4 {
            m.undecodable_event();
        }
        assert!(m.render().contains("lazywf_binlog_undecodable_total 4"));
        assert_eq!(m.entry_count(), (0, 0), "no labels, so no registry entry");
    }

    fn outcome_of(m: &Metrics, rule: &str, target: &str, o: FlushOutcome) -> u64 {
        let r = m.inner.lock().unwrap();
        r.pairs.get(&(rule.into(), target.into())).unwrap().outcome[o.index()]
            .load(Ordering::Relaxed)
    }

    /// A poisoned registry is data, not a second panic. Two owners take this lock and both of them
    /// can be the process's last act: a scrape renders it, and the engine's flush accounting times
    /// itself through handles resolved here (`set_pending`, `observe_flush`), so an `expect` here
    /// was a panic on the engine task. Every mutation of the registry is one whole `BTreeMap`
    /// entry — a handle inserted, or a removal — so what a task that died holding it leaves behind
    /// is an entry present or absent: here, the whole pre-panic snapshot, still rendered and still
    /// moving.
    #[test]
    fn a_poisoned_registry_renders_and_counts_on() {
        let m = scenario();
        crate::poison::poison(&m.inner);
        // The three sites that lock: resolve a rule, resolve a pair, time a flush.
        m.rule("orders").observe_flush(Duration::from_millis(1));
        m.pair("orders", "search").flush(FlushOutcome::Success);
        m.rule("customers").set_pending(2);
        let text = m.render();
        for want in [
            // what `scenario` built before the poison, unchanged: nothing was lost,
            "lazywf_flush_total{rule=\"orders\",target=\"search\",outcome=\"retry\"} 3",
            "lazywf_dropped_keys_total{rule=\"orders\",target=\"search\"} 1",
            "lazywf_pending_keys{rule=\"orders\"} 5",
            // and what moved on the poisoned lock: the handle was found, not rebuilt,
            "lazywf_flush_total{rule=\"orders\",target=\"search\",outcome=\"success\"} 13",
            "lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.001\"} 1",
            // a name the panicking task never reached is a new entry like any other,
            "lazywf_pending_keys{rule=\"customers\"} 2",
        ] {
            assert!(text.contains(want), "missing {want} in\n{text}");
        }
    }

    /// `remove_rule` is the only two-step update here (the rule entry, then its pairs), so it is
    /// the one site a torn moment could leave something behind — and what it leaves is a stale
    /// series until the next reload, never a wrong count. Under poison the removal is applied.
    #[test]
    fn a_poisoned_registry_still_forgets_a_removed_rule() {
        let m = scenario();
        crate::poison::poison(&m.inner);
        m.remove_rule("orders");
        let text = m.render();
        assert!(
            !text.contains("rule=\"orders\""),
            "the removed rule is still exporting:\n{text}"
        );
        assert!(
            text.contains("lazywf_build_info"),
            "the scrape itself still works:\n{text}"
        );
        // The entry set is still live: a pair named after the poison is created whole.
        m.pair("orders", "search").flush(FlushOutcome::Success);
        assert!(
            m.render().contains(
                "lazywf_flush_total{rule=\"orders\",target=\"search\",outcome=\"success\"} 1"
            ),
            "the pair could not be re-resolved"
        );
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_bounded() {
        let m = Metrics::default();
        let r = m.rule("orders");
        // Nothing over 30 s lands in a finite bucket, but every observation is counted.
        r.observe_flush(Duration::from_millis(1)); // exactly the first bucket
        r.observe_flush(Duration::from_secs(31)); // over the top: +Inf only
        let text = m.render();
        assert!(text.contains("lazywf_flush_duration_seconds{rule=\"orders\",le=\"0.001\"} 1"));
        assert!(text.contains("lazywf_flush_duration_seconds{rule=\"orders\",le=\"30\"} 1"));
        assert!(text.contains("lazywf_flush_duration_seconds{rule=\"orders\",le=\"+Inf\"} 2"));
        assert!(text.contains("lazywf_flush_duration_seconds_count{rule=\"orders\"} 2"));
        assert_eq!(r.buckets.len(), BUCKETS.len());
    }

    /// Unknown rule names cannot create entries: the hot-path mutators are handle methods, so
    /// the entry set grows only through `rule` / `pair`, which the engine calls with config names.
    #[test]
    fn cardinality_is_capped_at_the_loaded_config() {
        let m = Metrics::default();
        // "Loading" a config creates one rule and one pair per (rule, target) it holds.
        let a = m.rule("orders");
        let _ap = m.pair("orders", "search");
        let _b = m.rule("items");
        let _bp = m.pair("items", "index");
        assert_eq!(m.entry_count(), (2, 2));
        // Reloading the same config reuses the entries (same `Arc`, counters survive).
        let a2 = m.rule("orders");
        let ap2 = m.pair("orders", "search");
        assert!(
            Arc::ptr_eq(&a, &a2),
            "a reload does not mint a fresh rule entry"
        );
        assert!(Arc::ptr_eq(&m.pair("orders", "search"), &ap2));
        // Flushing and dropping through a resolved handle adds no entry — there is no name to
        // the registry, only atomics on the handle the rule already holds.
        ap2.flush(FlushOutcome::Success);
        ap2.dropped(1);
        m.rule("no-such-rule") // a genuine second rule loads fine...
            .observe_flush(Duration::from_millis(3));
        // ...but the hot path itself (handle mutators) never opened a "no-such-target" pair.
        assert_eq!(m.entry_count(), (3, 2));
    }

    /// The engine's removal path (`remove_rule`) must leave no series carrying the rule's name:
    /// neither its `{rule}` entry nor any of its `{rule,target}` pairs survives it.
    #[test]
    fn a_removed_rule_leaves_no_series_behind() {
        let m = Metrics::default();
        m.rule("orders").set_pending(3);
        m.pair("orders", "search").flush(FlushOutcome::Success);
        m.pair("orders", "archive").flush(FlushOutcome::Success);
        assert_eq!(m.entry_count(), (1, 2));
        assert!(
            m.render().contains("rule=\"orders\""),
            "present before removal"
        );
        m.remove_rule("orders");
        assert_eq!(
            m.entry_count(),
            (0, 0),
            "the rule entry and both its pairs go"
        );
        let t = m.render();
        assert!(!t.contains("orders"), "removed rule still renders:\n{t}");
        // Idempotent: a second removal (a reload seen twice) cannot panic or underflow.
        m.remove_rule("orders");
        assert_eq!(m.entry_count(), (0, 0));
    }

    /// A rename, as the reload does it: drop the old entry, resolve the new one. Only the new name
    /// exports afterwards.
    #[test]
    fn a_renamed_rule_exports_only_the_new_name() {
        let m = Metrics::default();
        m.rule("orders-old").set_pending(3);
        m.pair("orders-old", "search").flush(FlushOutcome::Success);
        // The reload that renames: the old rule is no longer in the compiled set, the new one is.
        m.remove_rule("orders-old");
        m.rule("orders-new").set_pending(3);
        m.pair("orders-new", "search").flush(FlushOutcome::Success);
        let t = m.render();
        assert!(t.contains("rule=\"orders-new\""), "new name missing:\n{t}");
        assert!(!t.contains("orders-old"), "old name still present:\n{t}");
        assert_eq!(m.entry_count(), (1, 1));
    }

    /// A move to another target (`remove_pair`): only the old `(rule, target)` pair leaves; the
    /// rule entry, its other pairs, and every other rule are untouched.
    #[test]
    fn moving_a_rule_drops_only_the_old_pair() {
        let m = Metrics::default();
        m.rule("orders").set_pending(1);
        m.rule("items").set_pending(1);
        m.pair("orders", "search").flush(FlushOutcome::Success);
        m.pair("orders", "archive").flush(FlushOutcome::Success);
        m.pair("items", "index").flush(FlushOutcome::Success);
        assert_eq!(m.entry_count(), (2, 3));
        m.remove_pair("orders", "search");
        assert_eq!(m.entry_count(), (2, 2), "only the old target's pair goes");
        let t = m.render();
        assert!(
            !t.contains("rule=\"orders\",target=\"search\""),
            "old pair still renders:\n{t}"
        );
        assert!(
            t.contains("rule=\"orders\",target=\"archive\""),
            "the rule's other pair must survive:\n{t}"
        );
        assert!(
            t.contains("rule=\"orders\""),
            "the rule entry must survive a move:\n{t}"
        );
        assert!(
            t.contains("rule=\"items\",target=\"index\""),
            "another rule untouched:\n{t}"
        );
        // Removing a pair the rule no longer holds is a no-op.
        m.remove_pair("orders", "search");
        assert_eq!(m.entry_count(), (2, 2));
    }

    #[test]
    fn classify_maps_a_flush_completion() {
        // A permanent refusal dominates, even over an expect-met flush (the keys were refused).
        assert_eq!(FlushOutcome::classify(true, true), FlushOutcome::Refused);
        assert_eq!(FlushOutcome::classify(true, false), FlushOutcome::Refused);
        // No refusals: expect met is a success, otherwise a transient retry.
        assert_eq!(FlushOutcome::classify(false, true), FlushOutcome::Success);
        assert_eq!(FlushOutcome::classify(false, false), FlushOutcome::Retry);
    }

    #[test]
    fn labels_are_escaped() {
        let m = Metrics::default();
        m.rule("we\"ird\\x").observe_flush(Duration::from_micros(1));
        let t = m.render();
        assert!(t.contains("rule=\"we\\\"ird\\\\x\""), "{t}");
    }

    /// The token, as the operator's variable would hold it after `Config` trimmed it.
    const TOKEN: &str = "s3cr3t-token";

    fn gated() -> (Arc<Metrics>, Option<String>) {
        let m = Arc::new(Metrics::default());
        m.rule("orders").set_pending(5);
        (m, Some(TOKEN.to_string()))
    }

    /// `LAZYWF_HEALTH_TOKEN` set: `/metrics` answers only to the bearer token, and a refusal says
    /// no more than `unauthorized` (the exposition, and the token, appear in neither). The index
    /// and the unknown paths are not gated: they name nothing a scraper needs a secret for.
    #[test]
    fn the_token_gates_the_exposition() {
        let (m, token) = gated();
        for given in [
            None,
            Some(""),
            Some("Bearer"),
            Some("Bearer s3cr3t-toke"),
            Some("Bearer s3cr3t-token!"),
            Some("Basic s3cr3t-token"),
            // what a reader of the old `/status` header would send: the value alone is not a
            // credential on either listener
            Some("s3cr3t-token"),
        ] {
            let (code, body, ctype) = respond(&m, "GET", "/metrics", given, token.as_deref());
            assert_eq!(code, 401, "given {given:?}");
            assert_eq!(body, "unauthorized\n", "given {given:?}");
            assert_eq!(ctype, "text/plain", "given {given:?}");
        }
        let (code, body, ctype) = respond(
            &m,
            "GET",
            "/metrics",
            Some("Bearer s3cr3t-token"),
            token.as_deref(),
        );
        assert_eq!(code, 200);
        assert_eq!(ctype, "text/plain; version=0.0.4");
        assert!(body.contains("lazywf_build_info"), "{body}");
        assert!(
            body.contains("lazywf_pending_keys{rule=\"orders\"} 5"),
            "{body}"
        );
        // A POST is not a scrape whether or not it carries the token.
        assert_eq!(
            respond(
                &m,
                "POST",
                "/metrics",
                Some("Bearer s3cr3t-token"),
                token.as_deref()
            )
            .0,
            405
        );
        // Ungated paths, and an unset token: as they were before the token existed.
        assert_eq!(respond(&m, "GET", "/", None, token.as_deref()).0, 200);
        assert_eq!(respond(&m, "GET", "/nope", None, token.as_deref()).0, 404);
        assert_eq!(respond(&m, "GET", "/metrics", None, None).0, 200);
    }

    /// The same gate with the credential padded the way a token file pads it: the trim happened on
    /// both sides, so a scrape of a file-sourced token still gets its exposition.
    #[test]
    fn a_padded_file_token_still_opens_the_exposition() {
        let (m, _) = gated();
        let token = Some(" s3cr3t-token\n".to_string());
        assert_eq!(
            respond(
                &m,
                "GET",
                "/metrics",
                Some("Bearer s3cr3t-token"),
                token.as_deref()
            )
            .0,
            200
        );
    }

    /// Serves `m` (gated by `token`) on a port of the kernel's choosing.
    async fn serving(m: Arc<Metrics>, token: Option<String>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_listener(listener, m, token));
        addr
    }

    async fn get(addr: SocketAddr, request: &str) -> String {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn code(answer: &str) -> u16 {
        answer.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    /// A Prometheus-style scrape over a real socket: with the bearer token it gets the exposition
    /// (`Accept`, `User-Agent` and the scrape timeout are what Prometheus 2.x itself sends),
    /// without it a 401 that carries `WWW-Authenticate: Bearer` and no metrics at all.
    #[tokio::test]
    async fn a_scrape_with_the_bearer_token_reads_the_exposition() {
        let (m, token) = gated();
        let addr = serving(m.clone(), token.clone()).await;
        let head = "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1:9090\r\nUser-Agent: Prometheus/2.53.1\r\n\
                    Accept: application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1\r\n\
                    Accept-Encoding: gzip\r\nX-Prometheus-Scrape-Timeout-Seconds: 10\r\n";
        let r = get(addr, &format!("{head}\r\n")).await;
        assert_eq!(code(&r), 401, "an anonymous scrape must be refused: {r}");
        assert!(r.contains("WWW-Authenticate: Bearer"), "{r}");
        assert!(!r.contains("lazywf_"), "a refusal exposes metrics: {r}");

        // The retired header, from a reader of the old `/status` docs: on this listener it is an
        // ordinary header, and so an ordinary refusal.
        let r = get(addr, &format!("{head}X-Lazywf-Token: {TOKEN}\r\n\r\n")).await;
        assert_eq!(
            code(&r),
            401,
            "the retired X-Lazywf-Token header opens nothing: {r}"
        );
        assert!(r.contains("WWW-Authenticate: Bearer"), "{r}");
        assert!(!r.contains("lazywf_"), "a refusal exposes metrics: {r}");

        let r = get(
            addr,
            &format!("{head}Authorization: Bearer {TOKEN}\r\n\r\n"),
        )
        .await;
        assert_eq!(code(&r), 200);
        assert!(r.contains("Content-Type: text/plain; version=0.0.4"), "{r}");
        assert!(r.contains("lazywf_build_info{version="), "{r}");
        assert!(r.contains("lazywf_pending_keys{rule=\"orders\"} 5"), "{r}");

        // HEAD of a gated path: the challenge, and no body.
        let r = get(addr, "HEAD /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(code(&r), 401, "{r}");
        assert!(
            r.ends_with("\r\n\r\n"),
            "a HEAD answer stops after the head: {r:?}"
        );
    }

    /// Without the setting the listener stays open, as it was before the token existed.
    #[tokio::test]
    async fn an_ungated_listener_scrapes_without_a_header() {
        let (m, _) = gated();
        let addr = serving(m.clone(), None).await;
        let r = get(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(code(&r), 200);
        assert!(r.contains("lazywf_build_info{version="), "{r}");
    }

    /// A token of nothing but spaces gates nothing, as `/status` does: the listener serves the
    /// exposition to an anonymous scrape rather than locking everyone out with a value nobody
    /// meant (`health::token_of`, the rule both listeners store the token through).
    #[tokio::test]
    async fn a_blank_token_gates_nothing() {
        let (m, _) = gated();
        let addr = serving(m.clone(), Some("   ".to_string())).await;
        let r = get(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(code(&r), 200, "a blank token leaves the listener open: {r}");
    }
}
