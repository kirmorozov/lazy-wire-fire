//! Prometheus exposition endpoint: `GET /metrics` on its own address (`LAZYWF_METRICS_ADDR`).
//!
//! The production-readiness blocker is that "did it deliver" is only a WARN log, invisible to
//! monitoring. This is the small, bounded signal set that answers it: five metrics over the
//! flush path, the dedup buffer and the binlog lag. Everything is `std::sync::atomic`; there is
//! no metrics crate (house style: no new runtime dependencies, the exposition is hand-rolled
//! next to the hand-rolled health parser in [`crate::health`]).
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
//! so an unknown name cannot create an entry.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
}

impl Metrics {
    /// The (or the existing) handle of `rule`. Called from the config reload only; the entry set
    /// is bounded by the loaded config.
    pub fn rule(&self, rule: &str) -> Arc<RuleHandle> {
        let mut r = self.inner.lock().expect("metrics");
        Arc::clone(
            r.rules
                .entry(rule.to_string())
                .or_insert_with(|| Arc::new(RuleHandle::new())),
        )
    }

    /// The (or the existing) handle of `(rule, target)`. Called from the config reload only.
    pub fn pair(&self, rule: &str, target: &str) -> Arc<PairHandle> {
        let mut r = self.inner.lock().expect("metrics");
        Arc::clone(
            r.pairs
                .entry((rule.to_string(), target.to_string()))
                .or_insert_with(|| Arc::new(PairHandle::new())),
        )
    }

    /// Publishes the instance binlog lag (seconds) — the same source as the status table's lag.
    pub fn set_binlog_lag(&self, secs: f64) {
        self.binlog_lag_bits
            .store(secs.to_bits(), Ordering::Relaxed);
    }

    /// Renders the exposition (`text/plain; version=0.0.4`). Allocates freely: only a scrape
    /// calls this. The order is deterministic (rules and pairs sorted, outcomes and buckets in
    /// fixed order) so a scrape snapshot is hand-checkable.
    pub fn render(&self) -> String {
        let r = self.inner.lock().expect("metrics");
        let mut out = String::new();
        build_info(&mut out);
        binlog_lag(
            &mut out,
            f64::from_bits(self.binlog_lag_bits.load(Ordering::Relaxed)),
        );
        pending_keys(&mut out, &r.rules);
        flush_total(&mut out, &r.pairs);
        dropped_keys(&mut out, &r.pairs);
        flush_duration(&mut out, &r.rules);
        out
    }

    /// The number of distinct `(rule, target)` entries: bounded by the loaded config. Test hook.
    #[cfg(test)]
    fn entry_count(&self) -> (usize, usize) {
        let r = self.inner.lock().expect("metrics");
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

async fn handle(mut s: TcpStream, m: Arc<Metrics>) {
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
    let (status, body, ctype) = if !matches!(method, "GET" | "HEAD") {
        (405, "method not allowed\n".into(), "text/plain")
    } else if path == "/metrics" {
        (200, m.render(), "text/plain; version=0.0.4")
    } else if path == "/" {
        (
            200,
            "lazywf metrics endpoint: /metrics\n".into(),
            "text/plain",
        )
    } else {
        (404, "not found\n".into(), "text/plain")
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Service Unavailable",
    };
    let head = format!("HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    let _ = s.write_all(head.as_bytes()).await;
    if method != "HEAD" {
        let _ = s.write_all(body.as_bytes()).await;
    }
    let _ = s.shutdown().await;
}

/// Serves `/metrics` until the process ends; a bind failure is logged and retried (like health).
pub async fn serve(addr: SocketAddr, m: Arc<Metrics>) {
    let listener = loop {
        match TcpListener::bind(addr).await {
            Ok(l) => break l,
            Err(e) => {
                tracing::warn!("metrics endpoint: cannot listen on {addr}: {e}; retrying in 5 s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    tracing::info!("metrics endpoint on http://{addr} (/metrics)");
    loop {
        if let Ok((s, _)) = listener.accept().await {
            tokio::spawn(handle(s, m.clone()));
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
            concat!("lazywf_build_info{version=\"", env!("CARGO_PKG_VERSION"), "\"} 1"),
            "# HELP lazywf_binlog_lag_seconds How far behind the binlog the reader is, in seconds (the status table's stream lag).",
            "# TYPE lazywf_binlog_lag_seconds gauge",
            "lazywf_binlog_lag_seconds 0",
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

    fn outcome_of(m: &Metrics, rule: &str, target: &str, o: FlushOutcome) -> u64 {
        let r = m.inner.lock().unwrap();
        r.pairs.get(&(rule.into(), target.into())).unwrap().outcome[o.index()]
            .load(Ordering::Relaxed)
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
}
