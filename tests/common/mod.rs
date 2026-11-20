//! Test harness: a schema per test, the engine running in-process, a scriptable HTTP target, and
//! the in-process gRPC server of [`grpc_server`].
#![allow(dead_code)]

pub mod fault;
pub mod grpc_server;

use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use lazy_wire_fire::config::Config;
use lazy_wire_fire::health::Health;
use lazy_wire_fire::metrics::Metrics;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Server root DSN (`LAZYWF_TEST_DSN`); `None` skips the integration tests.
pub fn root_dsn() -> Option<String> {
    std::env::var("LAZYWF_TEST_DSN")
        .ok()
        .filter(|v| !v.is_empty())
}

#[macro_export]
macro_rules! need_db {
    () => {
        match common::root_dsn() {
            Some(d) => d,
            None => {
                eprintln!("LAZYWF_TEST_DSN not set: skipping (make servers test-e2e)");
                return;
            }
        }
    };
}

pub struct Db {
    pub schema: String,
    pub dsn: String,
}

/// `Conn::disconnect` waits for the peer to close the socket. On a loaded box that never has to
/// come back, and a harness that hangs on it explains nothing (observed: the suite sitting on a
/// finished statement's disconnect). Every disconnect the harness does on its own connections is
/// bounded: after the grace period the socket is simply dropped, which for a test connection is
/// the same outcome.
pub async fn quit(c: Conn) {
    if tokio::time::timeout(Duration::from_secs(15), c.disconnect())
        .await
        .is_err()
    {
        eprintln!("harness> disconnect did not complete in 15 s; dropping the socket");
    }
}

impl Db {
    /// A fresh, empty schema `lwf_<name>`.
    pub async fn fresh(root: &str, name: &str) -> Db {
        let schema = format!("lwf_{name}");
        let mut c = match tokio::time::timeout(
            Duration::from_secs(20),
            Conn::new(Opts::from_url(&format!("{root}/?prefer_socket=false")).unwrap()),
        )
        .await
        {
            Ok(c) => c.unwrap(),
            Err(_) => panic!("{root}: the server did not answer a new connection in 20 s"),
        };
        c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{schema}`"))
            .await
            .unwrap();
        quit(c).await;
        Db {
            dsn: format!("{root}/{schema}?prefer_socket=false"),
            schema,
        }
    }

    pub async fn conn(&self) -> Conn {
        let opts = Opts::from_url(&self.dsn).unwrap();
        match tokio::time::timeout(Duration::from_secs(10), Conn::new(opts)).await {
            Ok(c) => c.unwrap(),
            Err(_) => panic!(
                "{}: no answer from the server within 10 s (new connection)",
                self.dsn
            ),
        }
    }

    pub async fn exec(&self, sql: &str) {
        let mut c = self.conn().await;
        c.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        quit(c).await;
    }

    pub async fn exec_params(&self, sql: &str, params: impl Into<mysql_async::Params> + Send) {
        let mut c = self.conn().await;
        c.exec_drop(sql, params)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        quit(c).await;
    }

    pub async fn query<T: FromRow + Send + 'static>(&self, sql: &str) -> Vec<T> {
        let mut c = self.conn().await;
        let r = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        quit(c).await;
        r
    }

    pub async fn target(
        &self,
        name: &str,
        kind: &str,
        endpoints: &str,
        expect: Option<&str>,
        vars: &str,
    ) {
        self.exec_params(
            "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars, timeout_ms) VALUES (?, ?, ?, ?, ?, 3000)",
            (name, kind, endpoints, expect, vars),
        )
        .await;
    }

    pub async fn trigger(
        &self,
        rule: &str,
        table: &str,
        key: Option<&str>,
        keys_sql: Option<&str>,
    ) {
        self.exec_params(
            "INSERT INTO lazy_wire_fire_trigger (rule, source_table, `key`, keys_sql) VALUES (?, ?, ?, ?)",
            (rule, table, key, keys_sql),
        )
        .await;
    }

    /// A transaction on a table of its own. Positions only pass transactions on tables other
    /// than the tool's status table, so after this one a rule with nothing pending can pass a
    /// binlog end that a status write slipped in front of.
    pub async fn tick(&self) {
        self.exec("CREATE TABLE IF NOT EXISTS tick (n INT)").await;
        self.exec("INSERT INTO tick VALUES (1)").await;
    }

    /// `(binlog_file, binlog_pos, reset_state, last_error)` of a status row.
    pub async fn status(&self, rule: &str) -> Option<(String, u64, String, Option<String>)> {
        let mut c = self.conn().await;
        let r = c
            .exec_first(
                "SELECT binlog_file, binlog_pos, reset_state, last_error FROM lazy_wire_fire_status WHERE rule = ?",
                (rule,),
            )
            .await
            .unwrap();
        quit(c).await;
        r
    }
}

/// Creates the tool's tables (the engine does it too, but rules go in before it starts).
pub async fn tables(db: &Db) {
    let mut c = db.conn().await;
    lazy_wire_fire::model::ensure(&mut c).await.unwrap();
    quit(c).await;
}

pub struct Instance {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    pub health: Arc<Health>,
    /// The engine's own answer, once the task has finished.
    done: Arc<Mutex<Option<anyhow::Result<()>>>>,
}

/// A replica server id for a test engine, in 4,000,000,000..=4,294,967,294: the range the
/// shared test servers leave to lazywf (the product's default, [2^30, 2^31), overlaps the
/// ids another tenant of those servers uses).
pub fn test_server_id() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    const LO: u64 = 4_000_000_000;
    const SPAN: u64 = 4_294_967_294 - LO + 1;
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.write_u64(SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    (LO + h.finish() % SPAN) as u32
}

pub fn config(db: &Db, extra: &[(&str, &str)]) -> Config {
    let mut vars: Vec<(String, String)> = vec![
        ("LAZYWF_DSN".into(), db.dsn.clone()),
        ("LAZYWF_HEALTH_ADDR".into(), "off".into()),
        ("LAZYWF_STATUS_MS".into(), "200".into()),
        ("LAZYWF_CONFIG_POLL_MS".into(), "1000".into()),
        ("LAZYWF_MAX_BACKOFF_MS".into(), "800".into()),
        ("LAZYWF_HEARTBEAT_MS".into(), "1000".into()),
        ("LAZYWF_STOP_GRACE_MS".into(), "3000".into()),
        // The product default denies every target host; a suite that wants egress allows the
        // hosts it uses here (an individual test may still override this, or leave the
        // variable unset to see the deny-all refusal).
        ("LAZYWF_ALLOWED_HOSTS".into(), "*".into()),
    ];
    for (k, v) in extra {
        vars.retain(|(a, _)| a != k);
        vars.push((k.to_string(), v.to_string()));
    }
    let mut cfg =
        Config::from_lookup(|k| vars.iter().find(|(a, _)| a == k).map(|(_, v)| v.clone())).unwrap();
    // Unless the test fixes LAZYWF_SERVER_ID, the id is random from the test range. It stays
    // "not fixed", so a collision still re-rolls the id as in production.
    if !extra.iter().any(|(k, _)| *k == "LAZYWF_SERVER_ID") {
        cfg.server_id = test_server_id();
    }
    cfg
}

/// Engine logs on the test output (`RUST_LOG`, default `info`), with the lines at `WARN` and above
/// kept for [`warnings`].
pub fn logs() {
    use tracing_subscriber::layer::{Layer, SubscriberExt};
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| "info".into());
        // Two layers over one registry: the formatter, for whoever reads the test output, and
        // [`Capture`], for the tests. Installing is the one step that must not fail quietly: a
        // subscriber that is not there means `warnings` stays empty, which from a test looks
        // exactly like a WARN that was never logged.
        let sub = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_test_writer()
                    .with_filter(filter),
            )
            .with(Capture);
        if let Err(e) = tracing::subscriber::set_global_default(sub) {
            eprintln!("the test subscriber was not installed: {e} (warnings() stays empty)");
        }
    });
}

/// How many lines [`warnings`] keeps: the oldest goes when a new one arrives, so a whole suite
/// costs a bounded buffer instead of every line the engines of every test ever logged. It has to
/// hold a full suite's worth: the tests run in parallel through one subscriber, and a test reads
/// its own line only after the others have logged around it.
const MAX_LOGGED: usize = 8192;

fn captured() -> Arc<Mutex<VecDeque<String>>> {
    static CAPTURED: OnceLock<Arc<Mutex<VecDeque<String>>>> = OnceLock::new();
    CAPTURED
        .get_or_init(|| Arc::new(Mutex::new(VecDeque::new())))
        .clone()
}

/// The `WARN`-and-above lines the engines of this test binary logged, oldest first, as plain text
/// (the level, the target, the message and `name=value` for every field — what a test matches).
/// The suite runs the engine in-process, so a test that expects a WARN reads its own line here —
/// and filters by what makes a line its own, which for every engine test is its own schema.
pub fn warnings() -> Vec<String> {
    captured()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .cloned()
        .collect()
}

/// The colour codes of one line, dropped: `ESC [ … m` sequences are markup, not text. The line
/// [`Capture`] assembles carries none of its own, but a value that came from the binlog (a table
/// name is source-stated text) is whatever the source said, so markup goes whatever its origin.
fn plain(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        // The sequence ends at `m`; anything else it holds (the parameter bytes) is markup too.
        for c in chars.by_ref() {
            if c == 'm' {
                break;
            }
        }
    }
    out
}

/// The harness's own `Layer`: it keeps the text of every `WARN` and `ERROR` event for
/// [`warnings`].
///
/// It reads the event rather than the bytes the formatter writes. Which writer a line reaches, and
/// where the formatter puts its line breaks and its colour, is the formatter's business — a test
/// must not fail because of either. So the level comes from the event's metadata (a `WARN` is kept
/// because it *is* one, not because three bytes appear in a rendered string) and the text is
/// assembled here, one event at a time.
#[derive(Default)]
struct Capture;

impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for Capture {
    fn on_event(&self, ev: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let meta = ev.metadata();
        // `Level` orders from `ERROR` down, so everything below `WARN` is not the test's business.
        if *meta.level() < tracing::Level::WARN {
            return;
        }
        let mut fields = Vec::new();
        let mut message = None;
        ev.record(&mut Collect {
            fields: &mut fields,
            message: &mut message,
        });
        let mut line = format!("{} {}", meta.level(), meta.target());
        if let Some(m) = message {
            line.push_str(": ");
            line.push_str(&m);
        }
        for (name, value) in fields {
            line.push(' ');
            line.push_str(&name);
            line.push('=');
            line.push_str(&value);
        }
        // The lock of a logged line must not be able to take the engine down with it.
        let keep = captured();
        let mut q = keep.lock().unwrap_or_else(|e| e.into_inner());
        if q.len() >= MAX_LOGGED {
            q.pop_front();
        }
        q.push_back(plain(&line));
    }
}

/// The fields of one event, as the plain format renders them: a string as it is (no quotes, so
/// `table=orders` reads as it is written), anything else through `Debug` — which is what a `%`-
/// displayed value prints. The message is a field too (`message`), and it goes to the front.
struct Collect<'a> {
    fields: &'a mut Vec<(String, String)>,
    message: &'a mut Option<String>,
}

impl Collect<'_> {
    fn put(&mut self, f: &tracing::field::Field, value: String) {
        if f.name() == "message" {
            *self.message = Some(value);
        } else {
            self.fields.push((f.name().to_string(), value));
        }
    }
}

impl tracing::field::Visit for Collect<'_> {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.put(f, v.to_string());
    }

    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.put(f, format!("{v:?}"));
    }
}

impl Instance {
    pub fn start(cfg: Config) -> Instance {
        Instance::with(cfg, None)
    }

    /// Like [`Instance::start`], with the registry the engine counts into: `Metrics::render` of
    /// that `Arc` is exactly the body `/metrics` serves (`respond` calls nothing else), so a test
    /// reads the scrape without a port to fight over.
    pub fn start_with_metrics(cfg: Config, metrics: Arc<Metrics>) -> Instance {
        Instance::with(cfg, Some(metrics))
    }

    fn with(cfg: Config, metrics: Option<Arc<Metrics>>) -> Instance {
        logs();
        let (tx, rx) = watch::channel(false);
        // The token comes from the config as it does in `main`, so a test that sets
        // LAZYWF_HEALTH_TOKEN gets a `/status` (and a `/metrics`) that ask for it.
        let health = Arc::new(Health::new(cfg.health_token.clone()));
        let h = health.clone();
        let done: Arc<Mutex<Option<anyhow::Result<()>>>> = Arc::new(Mutex::new(None));
        let d = done.clone();
        let task = tokio::spawn(async move {
            let r = lazy_wire_fire::engine::run(cfg, h, metrics, rx).await;
            *d.lock().unwrap() = Some(r);
        });
        Instance {
            stop: tx,
            task,
            health,
            done,
        }
    }

    /// Starts an instance and waits until it streams the binlog.
    pub async fn ready(cfg: Config) -> Instance {
        Instance::ready_inner(cfg, None).await
    }

    /// Like [`Instance::ready`], with the metrics registry the engine counts into (see
    /// [`Instance::start_with_metrics`]).
    pub async fn ready_with_metrics(cfg: Config, metrics: Arc<Metrics>) -> Instance {
        Instance::ready_inner(cfg, Some(metrics)).await
    }

    async fn ready_inner(cfg: Config, metrics: Option<Arc<Metrics>>) -> Instance {
        let i = Instance::with(cfg, metrics);
        let allowed = deadline(Duration::from_secs(20));
        let until = Instant::now() + allowed;
        while !i.health.is_ready() {
            if i.task.is_finished() {
                let r = i.done.lock().unwrap().take();
                panic!("the engine did not get ready: {r:?}");
            }
            if Instant::now() > until {
                panic!(
                    "the engine did not get ready in {allowed:?} (it is still running; an idle \
                     box allows {:?})",
                    Duration::from_secs(20)
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        i
    }

    /// Waits until no rule has keys pending or an action in flight (twice in a row, a status
    /// period apart): trailing fires of earlier changes are done.
    pub async fn quiet(&self) {
        let allowed = deadline(Duration::from_secs(20));
        let until = Instant::now() + allowed;
        let mut calm = 0;
        while calm < 2 {
            let s = self.health.status();
            let busy = s["rules"].as_object().is_none_or(|r| {
                r.values()
                    .any(|v| v["pending_keys"] != 0 || v["firing"] == true)
            });
            calm = if busy { 0 } else { calm + 1 };
            if Instant::now() > until {
                panic!("the engine did not quiet down in {allowed:?}: {s}");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    pub async fn stop(self) {
        let _ = self.stop.send(true);
        let grace = deadline(Duration::from_secs(20));
        match tokio::time::timeout(grace, self.task).await {
            Ok(Ok(())) => {}
            other => panic!("engine did not stop cleanly: {other:?} after {grace:?}"),
        }
        let engine = self.done.lock().unwrap().take();
        match engine {
            Some(Ok(())) => {}
            other => panic!("engine did not stop cleanly: the engine answered {other:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Req {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
    /// When the bytes of this request were read, stamped by the accept task before the handler
    /// and before the state lock. A test that measures how long the engine took to reach a
    /// target measures to this instant, not to the moment it happened to ask for the requests.
    pub arrived: Instant,
}

impl Req {
    pub fn header(&self, n: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(n))
            .map(|(_, v)| v.as_str())
    }

    /// The Standard Webhooks v1 signature a receiver computes over this request with `key`
    /// (the raw HMAC key, the base64 of a `whsec_` secret's remainder): `v1,` + the base64 of
    /// the HMAC-SHA256 of `<webhook-id>.<webhook-timestamp>.<body>`.
    pub fn standard_signature(&self, key: &[u8]) -> Option<String> {
        use sha2::Sha256;
        let id = self.header("webhook-id")?;
        let ts = self.header("webhook-timestamp")?;
        let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
        mac.update(id.as_bytes());
        mac.update(b".");
        mac.update(ts.as_bytes());
        mac.update(b".");
        mac.update(self.body.as_bytes());
        Some(format!(
            "v1,{}",
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        ))
    }

    /// Whether this request carries the standard signature of `key` (one of its space-separated
    /// `v1,` entries is what the receiver recomputes).
    pub fn standard_ok(&self, key: &[u8]) -> bool {
        let want = self.standard_signature(key);
        self.header("webhook-signature")
            .is_some_and(|h| h.split_whitespace().any(|e| Some(e) == want.as_deref()))
    }

    /// Whether this request's header `h` is `prefix` + the hex HMAC-SHA256 of its body with
    /// `key` (the GitHub-style default of the hmac signing).
    pub fn hmac_ok(&self, key: &[u8], h: &str, prefix: &str) -> bool {
        use sha2::Sha256;
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(self.body.as_bytes());
        let want = format!("{}{}", prefix, hex::encode(mac.finalize().into_bytes()));
        self.header(h) == Some(want.as_str())
    }

    /// NDJSON body lines as JSON.
    pub fn ndjson(&self) -> Vec<serde_json::Value> {
        self.body
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// The body parsed as YAML (the `yaml` helper's round-trip check).
    pub fn yaml(&self) -> serde_json::Value {
        let v: yaml_serde::Value = yaml_serde::from_str(&self.body)
            .unwrap_or_else(|e| panic!("the body is not YAML ({e}):\n{}", self.body));
        yaml_to_json(&v)
    }
}

/// A `yaml_serde::Value` as the JSON value it holds (numbers through their text form, so
/// `12.5` compares as `12.5`).
fn yaml_to_json(v: &yaml_serde::Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        yaml_serde::Value::Null => J::Null,
        yaml_serde::Value::Bool(b) => J::Bool(*b),
        yaml_serde::Value::Number(n) => {
            serde_json::from_str(&n.to_string()).unwrap_or_else(|e| panic!("number: {e}"))
        }
        yaml_serde::Value::String(s) => J::String(s.clone()),
        yaml_serde::Value::Sequence(a) => J::Array(a.iter().map(yaml_to_json).collect()),
        yaml_serde::Value::Mapping(m) => {
            let mut o = serde_json::Map::new();
            for (k, val) in m.iter() {
                o.insert(
                    match yaml_to_json(k) {
                        J::String(s) => s,
                        other => other.to_string(),
                    },
                    yaml_to_json(val),
                );
            }
            J::Object(o)
        }
        yaml_serde::Value::Tagged(_) => panic!("a tagged value in a test body"),
    }
}

type Handler = Box<dyn Fn(&Req) -> (u16, String) + Send>;
type HeaderFn = Box<dyn Fn(&Req) -> Vec<(String, String)> + Send>;

/// The chaos fault modes of the mock (design §1 P4, §3). Every firing registers itself in
/// the harness fault log, like the proxy commands: a held or cut answer is a fault event,
/// never a delivery.
enum MockFault {
    Live,
    /// Answer `n` requests, then accept and never answer (the mid-flush anchor of P1).
    HoldAfter {
        n: usize,
        answered: usize,
    },
    /// Answer the first `n` requests with `status` (design §3: the 5xx burst of T3; S5 keeps a
    /// flush refused with it). A refused answer is a fault event the mock itself registers, never
    /// a delivery (design §1 P4).
    RespondNth {
        n: usize,
        status: u16,
        answered: usize,
    },
}

struct MockState {
    reqs: Vec<Req>,
    handler: Handler,
    headers: HeaderFn,
    fault: MockFault,
    /// Full 2xx answers only (P4's delivery record): when the mock answered, and which keys
    /// (`{{join keys ","}}` bodies split on the documented separator) that answer carried.
    deliveries: Vec<(Instant, BTreeSet<String>)>,
    fault_log: Option<fault::FaultLog>,
    tag: String,
}

/// An HTTP server that records requests and answers with a replaceable handler.
#[derive(Clone)]
pub struct Mock {
    pub addr: SocketAddr,
    state: Arc<Mutex<MockState>>,
}

impl Mock {
    pub async fn start() -> Mock {
        Mock::start_on(0).await
    }

    /// On a fixed port (a backend other servers are configured with).
    pub async fn start_on(port: u16) -> Mock {
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockState {
            reqs: Vec::new(),
            handler: Box::new(|_| (200, "{}".into())),
            headers: Box::new(|_| Vec::new()),
            fault: MockFault::Live,
            deliveries: Vec::new(),
            fault_log: None,
            tag: "mock".into(),
        }));
        let st = state.clone();
        tokio::spawn(async move {
            // A transient accept error (EMFILE under an fd storm, a lost child mid-handshake)
            // must not retire the mock: a mock that stopped accepting looks exactly like a
            // target that stopped answering.
            let mut accept_errs = 0u64;
            loop {
                match listener.accept().await {
                    Ok((mut s, _)) => {
                        let st = st.clone();
                        tokio::spawn(async move {
                            let mut buf: Vec<u8> = Vec::new();
                            loop {
                                let head_end = loop {
                                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                        break i + 4;
                                    }
                                    let mut chunk = [0u8; 8192];
                                    match s.read(&mut chunk).await {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    }
                                };
                                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                                let mut lines = head.split("\r\n");
                                let first = lines.next().unwrap_or("");
                                let mut parts = first.split_whitespace();
                                let method = parts.next().unwrap_or("").to_string();
                                let path = parts.next().unwrap_or("").to_string();
                                let headers: Vec<(String, String)> = lines
                                    .filter_map(|l| {
                                        l.split_once(':').map(|(a, b)| {
                                            (a.trim().to_string(), b.trim().to_string())
                                        })
                                    })
                                    .collect();
                                let len: usize = headers
                                    .iter()
                                    .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                    .and_then(|(_, v)| v.parse().ok())
                                    .unwrap_or(0);
                                while buf.len() < head_end + len {
                                    let mut chunk = [0u8; 8192];
                                    match s.read(&mut chunk).await {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    }
                                }
                                let body = String::from_utf8_lossy(&buf[head_end..head_end + len])
                                    .to_string();
                                buf.drain(..head_end + len);
                                // Stamped here, before the handler and before the state lock: the instant
                                // the request is *here*, not the instant a test asks about it.
                                let arrived = Instant::now();
                                let req = Req {
                                    method,
                                    path,
                                    headers,
                                    body,
                                    arrived,
                                };
                                // The fault modes gate the answer. A request that arrives once
                                // hold_after is spent is accepted and frozen (never answered); when the
                                // mock heals, the held connection is reset — an answer that lands after
                                // the engine timed out is not a delivery, it is a lie (design §1 P4).
                                let mut held_logged = false;
                                loop {
                                    let hold = {
                                        let mut g = st.lock().unwrap();
                                        match &mut g.fault {
                                            MockFault::HoldAfter { n, answered }
                                                if *answered >= *n =>
                                            {
                                                true
                                            }
                                            MockFault::HoldAfter { answered, .. } => {
                                                *answered += 1;
                                                false
                                            }
                                            // respond_nth answers every request (some with 5xx):
                                            // nothing is ever held by it.
                                            MockFault::RespondNth { .. } => false,
                                            MockFault::Live => false,
                                        }
                                    };
                                    if !hold {
                                        break;
                                    }
                                    if !held_logged {
                                        held_logged = true;
                                        // ONE lock, one scope: a second `st.lock()` while this guard is
                                        // alive self-deadlocks the non-reentrant mutex, parks a runtime
                                        // worker holding it, and then every task that touches the mock
                                        // parks too (which is how the whole suite once froze silently).
                                        let mut g = st.lock().unwrap();
                                        if let Some(l) = &g.fault_log {
                                            l.add(format!(
                                                "mock {} hold_after fired: {} {} held",
                                                g.tag, req.method, req.path
                                            ));
                                        }
                                        g.reqs.push(req.clone());
                                    }
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                                if held_logged {
                                    // Reset the held connection on heal: the request was neither
                                    // answered nor delivered (#[allow]: linger-0 never blocks).
                                    #[allow(deprecated)]
                                    let _ = s.set_linger(Some(Duration::ZERO));
                                    return;
                                }
                                let keys: BTreeSet<String> = req
                                    .body
                                    .split(',')
                                    .map(|k| k.trim().to_string())
                                    .filter(|k| !k.is_empty())
                                    .collect();
                                let (status, body, extra) = {
                                    let mut g = st.lock().unwrap();
                                    let (mut status, body) = (g.handler)(&req);
                                    // The `respond_nth` burst: the answer is overridden here, in
                                    // the same single lock scope the held-request path warns
                                    // about, and the firing is registered in the harness fault log
                                    // (one lock each, never the mock's while the log's is held).
                                    let mut refused = None;
                                    // The tag is copied out first: `g.fault` is borrowed mutably
                                    // for the rest of this scope.
                                    let tag = g.tag.clone();
                                    if let MockFault::RespondNth {
                                        n,
                                        status: s,
                                        answered,
                                    } = &mut g.fault
                                    {
                                        if *answered < *n {
                                            *answered += 1;
                                            status = *s;
                                            refused = Some(format!(
                                                "mock {tag} respond_nth({n})={s} fired: {} {} \
                                                 refused ({}/{})",
                                                req.method, req.path, *answered, n
                                            ));
                                        }
                                    }
                                    let extra: String = (g.headers)(&req)
                                        .into_iter()
                                        .map(|(k, v)| format!("{k}: {v}\r\n"))
                                        .collect();
                                    g.reqs.push(req);
                                    if let (Some(l), Some(msg)) = (&g.fault_log, refused) {
                                        l.add(msg);
                                    }
                                    (status, body, extra)
                                };
                                let resp = format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                                if s.write_all(resp.as_bytes()).await.is_err() {
                                    return;
                                }
                                if (200..300).contains(&status) {
                                    // A delivery: the request the mock answered 2xx *in full*.
                                    st.lock().unwrap().deliveries.push((Instant::now(), keys));
                                }
                            }
                        });
                    }
                    Err(e) => {
                        accept_errs += 1;
                        if accept_errs <= 5 || accept_errs.is_multiple_of(100) {
                            eprintln!("mock> accept error #{accept_errs}: {e}");
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        });
        Mock { addr, state }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn respond(&self, f: impl Fn(&Req) -> (u16, String) + Send + 'static) {
        self.state.lock().unwrap().handler = Box::new(f);
    }

    /// Extra response headers.
    pub fn headers(&self, f: impl Fn(&Req) -> Vec<(String, String)> + Send + 'static) {
        self.state.lock().unwrap().headers = Box::new(f);
    }

    pub fn requests(&self) -> Vec<Req> {
        self.state.lock().unwrap().reqs.clone()
    }

    /// The earliest moment a request for `path` reached this mock (`None`: none has yet). The
    /// stamp is the accept task's, so a test that bounds how long the engine took to reach a
    /// target is bound to the arrival, not to the interval of its own polling.
    pub fn arrived(&self, path: &str) -> Option<Instant> {
        self.state
            .lock()
            .unwrap()
            .reqs
            .iter()
            .filter(|r| r.path == path)
            .map(|r| r.arrived)
            .min()
    }

    pub fn clear(&self) {
        let mut g = self.state.lock().unwrap();
        g.reqs.clear();
        g.deliveries.clear();
    }

    /// Register this mock's fault firings in a harness fault log (P4's windowing).
    pub fn set_fault_log(&self, log: fault::FaultLog) {
        self.state.lock().unwrap().fault_log = Some(log);
    }

    /// Name this mock in fault-log lines ("t1-down").
    pub fn tag(&self, tag: &str) {
        self.state.lock().unwrap().tag = tag.to_string();
    }

    /// Answer the first `n` still-unanswered requests with the current handler, then accept
    /// and freeze (the mid-flush anchor). Each frozen request registers a fault event.
    pub fn hold_after(&self, n: usize) {
        let mut g = self.state.lock().unwrap();
        g.fault = MockFault::HoldAfter { n, answered: 0 };
        if let Some(l) = &g.fault_log {
            l.add(format!("mock {} hold_after({n}) armed", g.tag));
        }
    }

    /// Answer the first `n` still-unanswered requests with `status`, the rest with the current
    /// handler (design §3's `respond_nth`). Every refusal it hands out registers a fault-log
    /// event, so a refused flush is a fault event, never a delivery (design §1 P4).
    pub fn respond_nth(&self, n: usize, status: u16) {
        let mut g = self.state.lock().unwrap();
        g.fault = MockFault::RespondNth {
            n,
            status,
            answered: 0,
        };
        if let Some(l) = &g.fault_log {
            l.add(format!("mock {} respond_nth({n}, {status}) armed", g.tag));
        }
    }

    /// Clear the fault mode. Held requests get an RST, never a late answer.
    pub fn heal(&self) {
        let mut g = self.state.lock().unwrap();
        g.fault = MockFault::Live;
        if let Some(l) = &g.fault_log {
            l.add(format!("mock {} heal() (held requests reset)", g.tag));
        }
    }

    /// The keys carried by requests the mock answered 2xx in full, `{{join keys ","}}` split
    /// on the separator. Fault answers (held, 5xx, cut) never contribute (design §1 P4).
    pub fn keys_received(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for (_, keys) in &self.state.lock().unwrap().deliveries {
            out.extend(keys.iter().cloned());
        }
        out
    }

    /// Every full-2xx answer as (instant, keys) — the P4 record.
    pub fn deliveries(&self) -> Vec<(Instant, BTreeSet<String>)> {
        self.state.lock().unwrap().deliveries.clone()
    }

    /// Exact delivery counts are *reported*, never asserted (dedupe/debounce are timing-
    /// sensitive by design); this is the reporting input.
    pub fn delivery_count(&self) -> usize {
        self.state.lock().unwrap().deliveries.len()
    }
}

/// The round trip of one of this harness's DB calls: a fresh connection, one query, the
/// disconnect. Measured three times in a thread of its own, the slowest one kept.
///
/// This is the load of the host as these tests feel it. What they wait for travels exactly this
/// path: the write goes over such a connection, the engine reads the binlog those commits land
/// in, and a server that answers slowly slows all of it down. That says more than the run-queue
/// depth of the box does. `None` when there is no server to ask.
pub fn db_rtt() -> Option<Duration> {
    static RTT: OnceLock<Option<Duration>> = OnceLock::new();
    *RTT.get_or_init(|| {
        let dsn = root_dsn()?;
        // A thread with a runtime of its own: this is called from inside the test's runtime.
        std::thread::spawn(move || {
            let probe = async move {
                let mut worst = Duration::ZERO;
                for _ in 0..3 {
                    let t = Instant::now();
                    let url = format!("{dsn}/?prefer_socket=false");
                    let mut c = mysql_async::Conn::new(Opts::from_url(&url)?).await?;
                    let _: Vec<i8> = c.query("SELECT 1").await?;
                    quit(c).await;
                    worst = worst.max(t.elapsed());
                }
                Ok::<_, mysql_async::Error>(worst)
            };
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?
                .block_on(probe)
                .ok()
        })
        .join()
        .unwrap_or_default()
    })
}

/// How much longer every deadline of this harness allows than an idle box needs.
///
/// What a test waits for travels a path the test does not control: the commit, the binlog, the
/// engine in this very process (competing with the test's own tasks for its worker threads), the
/// target's HTTP call, the mock's accept task. A host that is busy with other work delays all of
/// it, and a fixed deadline turns that into a flake that says nothing about the product. So the
/// deadlines are the ones of an idle box multiplied by a factor: `LAZYWF_TEST_WAIT_SCALE` when it
/// is set (the shared runner raises it while the box is loaded), otherwise the slower of the
/// server's round trip and the host's own run-queue depth per core. It never drops below 1, so on
/// a quiet machine and on CI these are exactly the old deadlines, and it never goes above 8, so a
/// condition that really never comes still fails — minutes later at the worst, with the allowance
/// it used in the message.
pub fn wait_scale() -> f64 {
    static SCALE: OnceLock<f64> = OnceLock::new();
    *SCALE.get_or_init(|| {
        let told = std::env::var("LAZYWF_TEST_WAIT_SCALE")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok());
        // A connect, a query and a disconnect of one of these servers: 1-3 ms when it is idle.
        let from_server = db_rtt().map_or(1.0, |r| r.as_secs_f64() / 0.005);
        let from_host = std::fs::read_to_string("/proc/loadavg")
            .ok()
            .and_then(|s| {
                s.split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<f64>().ok())
            })
            .zip(std::thread::available_parallelism().ok())
            .map(|(load, cores)| load / cores.get() as f64)
            .unwrap_or(1.0);
        let s = told
            .unwrap_or_else(|| from_server.max(from_host))
            .clamp(1.0, 8.0);
        eprintln!(
            "wait scale: {s} (server round trip {:?}, host load per core {from_host:.2})",
            db_rtt()
        );
        s
    })
}

/// The time to allow for a condition that is expected within `base` on an idle box.
pub fn deadline(base: Duration) -> Duration {
    Duration::from_secs_f64(base.as_secs_f64() * wait_scale())
}

/// Polls `f` until it returns `Some` or `timeout` passes (`timeout` is the allowance of an idle
/// box: [`deadline`] scales it for the load of the host).
pub async fn wait_for<T, F, Fut>(timeout: Duration, what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let allowed = deadline(timeout);
    let until = Instant::now() + allowed;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        if Instant::now() > until {
            panic!(
                "timed out waiting for {what} after {allowed:?} (an idle box allows {timeout:?})"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}
