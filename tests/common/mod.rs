//! Test harness: a schema per test, the engine running in-process, a scriptable HTTP target.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    task: JoinHandle<anyhow::Result<()>>,
    pub health: Arc<Health>,
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
    Config::from_lookup(|k| vars.iter().find(|(a, _)| a == k).map(|(_, v)| v.clone())).unwrap()
}

/// Engine logs on the test output (`RUST_LOG`, default `info`).
pub fn logs() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| "info".into());
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .init();
    });
}

impl Instance {
    pub fn start(cfg: Config) -> Instance {
        logs();
        let (tx, rx) = watch::channel(false);
        let health = Arc::new(Health::default());
        let h = health.clone();
        let task = tokio::spawn(async move { lazy_wire_fire::engine::run(cfg, h, rx).await });
        Instance {
            stop: tx,
            task,
            health,
        }
    }

    /// Starts an instance and waits until it streams the binlog.
    pub async fn ready(cfg: Config) -> Instance {
        let i = Instance::start(cfg);
        let deadline = Instant::now() + Duration::from_secs(20);
        while !i.health.is_ready() {
            if i.task.is_finished() || Instant::now() > deadline {
                panic!("the engine did not get ready");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        i
    }

    pub async fn stop(self) {
        let _ = self.stop.send(true);
        match tokio::time::timeout(Duration::from_secs(20), self.task).await {
            Ok(Ok(Ok(()))) => {}
            other => panic!("engine did not stop cleanly: {other:?}"),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
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

    /// NDJSON body lines as JSON.
    pub fn ndjson(&self) -> Vec<serde_json::Value> {
        self.body
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

type Handler = Box<dyn Fn(&Req) -> (u16, String) + Send>;

struct MockState {
    reqs: Vec<Req>,
    handler: Handler,
}

/// An HTTP server that records requests and answers with a replaceable handler.
#[derive(Clone)]
pub struct Mock {
    pub addr: SocketAddr,
    state: Arc<Mutex<MockState>>,
}

impl Mock {
    pub async fn start() -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockState {
            reqs: Vec::new(),
            handler: Box::new(|_| (200, "{}".into())),
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
                        let (status, body) = {
                            let mut g = st.lock().unwrap();
                            let r = (g.handler)(&req);
                            g.reqs.push(req);
                            r
                        };
                        let resp = format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
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
