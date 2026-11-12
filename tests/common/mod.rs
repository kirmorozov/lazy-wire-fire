//! Test harness: a schema per test, the engine running in-process, a scriptable HTTP target, and
//! the in-process gRPC server of [`grpc_server`].
#![allow(dead_code)]

pub mod grpc_server;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use lazy_wire_fire::config::Config;
use lazy_wire_fire::health::Health;
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

impl Db {
    /// A fresh, empty schema `lwf_<name>`.
    pub async fn fresh(root: &str, name: &str) -> Db {
        let schema = format!("lwf_{name}");
        let mut c = Conn::new(Opts::from_url(&format!("{root}/?prefer_socket=false")).unwrap())
            .await
            .unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{schema}`"))
            .await
            .unwrap();
        c.disconnect().await.ok();
        Db {
            dsn: format!("{root}/{schema}?prefer_socket=false"),
            schema,
        }
    }

    pub async fn conn(&self) -> Conn {
        Conn::new(Opts::from_url(&self.dsn).unwrap()).await.unwrap()
    }

    pub async fn exec(&self, sql: &str) {
        let mut c = self.conn().await;
        c.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        c.disconnect().await.ok();
    }

    pub async fn exec_params(&self, sql: &str, params: impl Into<mysql_async::Params> + Send) {
        let mut c = self.conn().await;
        c.exec_drop(sql, params)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        c.disconnect().await.ok();
    }

    pub async fn query<T: FromRow + Send + 'static>(&self, sql: &str) -> Vec<T> {
        let mut c = self.conn().await;
        let r = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        c.disconnect().await.ok();
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
        c.disconnect().await.ok();
        r
    }
}

/// Creates the tool's tables (the engine does it too, but rules go in before it starts).
pub async fn tables(db: &Db) {
    let mut c = db.conn().await;
    lazy_wire_fire::model::ensure(&mut c).await.unwrap();
    c.disconnect().await.ok();
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

/// Engine logs on the test output (`RUST_LOG`, default `info`).
pub fn logs() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| "info".into());
        // `try_init`: a test binary that installs its own subscriber first keeps it.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    });
}

impl Instance {
    pub fn start(cfg: Config) -> Instance {
        logs();
        let (tx, rx) = watch::channel(false);
        let health = Arc::new(Health::default());
        let h = health.clone();
        let done: Arc<Mutex<Option<anyhow::Result<()>>>> = Arc::new(Mutex::new(None));
        let d = done.clone();
        let task = tokio::spawn(async move {
            let r = lazy_wire_fire::engine::run(cfg, h, rx).await;
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
        let i = Instance::start(cfg);
        let deadline = Instant::now() + Duration::from_secs(20);
        while !i.health.is_ready() {
            if i.task.is_finished() {
                let r = i.done.lock().unwrap().take();
                panic!("the engine did not get ready: {r:?}");
            }
            if Instant::now() > deadline {
                panic!("the engine did not get ready in 20 s (it is still running)");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        i
    }

    /// Waits until no rule has keys pending or an action in flight (twice in a row, a status
    /// period apart): trailing fires of earlier changes are done.
    pub async fn quiet(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut calm = 0;
        while calm < 2 {
            let s = self.health.status();
            let busy = s["rules"].as_object().is_none_or(|r| {
                r.values()
                    .any(|v| v["pending_keys"] != 0 || v["firing"] == true)
            });
            calm = if busy { 0 } else { calm + 1 };
            if Instant::now() > deadline {
                panic!("the engine did not quiet down: {s}");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    pub async fn stop(self) {
        let _ = self.stop.send(true);
        match tokio::time::timeout(Duration::from_secs(20), self.task).await {
            Ok(Ok(())) => {}
            other => panic!("engine did not stop cleanly: {other:?}"),
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

struct MockState {
    reqs: Vec<Req>,
    handler: Handler,
    headers: HeaderFn,
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
        }));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
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
                                l.split_once(':')
                                    .map(|(a, b)| (a.trim().to_string(), b.trim().to_string()))
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
                        let body =
                            String::from_utf8_lossy(&buf[head_end..head_end + len]).to_string();
                        buf.drain(..head_end + len);
                        let req = Req {
                            method,
                            path,
                            headers,
                            body,
                        };
                        let (status, body, extra) = {
                            let mut g = st.lock().unwrap();
                            let (status, body) = (g.handler)(&req);
                            let extra: String = (g.headers)(&req)
                                .into_iter()
                                .map(|(k, v)| format!("{k}: {v}\r\n"))
                                .collect();
                            g.reqs.push(req);
                            (status, body, extra)
                        };
                        let resp = format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if s.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
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

    pub fn clear(&self) {
        self.state.lock().unwrap().reqs.clear();
    }
}

/// Polls `f` until it returns `Some` or `timeout` passes.
pub async fn wait_for<T, F, Fut>(timeout: Duration, what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}
