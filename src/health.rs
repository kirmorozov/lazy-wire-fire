//! Health endpoint: `/livez`, `/readyz` and `/status` (JSON), on a local address.
//!
//! * `/livez`: 200 while the main loop is turning (it beats at least every few seconds);
//! * `/readyz`: 200 while this instance is active and streaming the binlog;
//! * `/status`: what each rule is doing.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The loop must beat within this long for `/livez`.
pub const STALL: Duration = Duration::from_secs(60);

struct Inner {
    phase: &'static str,
    streaming: bool,
    beat: Instant,
    status: Value,
}

pub struct Health {
    inner: Mutex<Inner>,
}

impl Default for Health {
    fn default() -> Self {
        Health { inner: Mutex::new(Inner { phase: "starting", streaming: false, beat: Instant::now(), status: json!({}) }) }
    }
}

impl Health {
    fn with<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        f(&mut self.inner.lock().expect("health"))
    }

    pub fn beat(&self) {
        self.with(|i| i.beat = Instant::now());
    }

    pub fn set_phase(&self, phase: &'static str) {
        self.with(|i| {
            i.phase = phase;
            i.beat = Instant::now();
        });
    }

    pub fn set_streaming(&self, on: bool) {
        self.with(|i| i.streaming = on);
    }

    pub fn set_status(&self, status: Value) {
        self.with(|i| i.status = status);
    }

    fn respond(&self, path: &str) -> (u16, String, &'static str) {
        self.with(|i| {
            let live = i.beat.elapsed() < STALL;
            match path {
                "/livez" | "/healthz" => {
                    if live {
                        (200, "ok\n".into(), "text/plain")
                    } else {
                        (503, format!("main loop stalled for {}s\n", i.beat.elapsed().as_secs()), "text/plain")
                    }
                }
                "/readyz" => {
                    if live && i.phase == "streaming" && i.streaming {
                        (200, "ok\n".into(), "text/plain")
                    } else if !live {
                        (503, "stalled\n".into(), "text/plain")
                    } else {
                        (503, format!("{}{}\n", i.phase, if i.streaming { "" } else { ", binlog not connected" }), "text/plain")
                    }
                }
                "/status" => {
                    let mut s = i.status.clone();
                    if let Value::Object(m) = &mut s {
                        m.insert("phase".into(), json!(i.phase));
                        m.insert("streaming".into(), json!(i.streaming));
                    }
                    (200, format!("{s:#}\n"), "application/json")
                }
                "/" => (200, "lazywf health endpoint: /livez /readyz /status\n".into(), "text/plain"),
                _ => (404, "not found\n".into(), "text/plain"),
            }
        })
    }
}

async fn handle(mut s: TcpStream, h: Arc<Health>) {
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
    let (status, body, ctype) = if matches!(method, "GET" | "HEAD") { h.respond(path) } else { (405, "method not allowed\n".into(), "text/plain") };
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

/// Serves until the process ends; a bind failure is logged and retried.
pub async fn serve(addr: SocketAddr, h: Arc<Health>) {
    let listener = loop {
        match TcpListener::bind(addr).await {
            Ok(l) => break l,
            Err(e) => {
                tracing::warn!("health endpoint: cannot listen on {addr}: {e}; retrying in 5 s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    tracing::info!("health endpoint on http://{addr} (/livez, /readyz, /status)");
    loop {
        if let Ok((s, _)) = listener.accept().await {
            tokio::spawn(handle(s, h.clone()));
        }
    }
}

/// `lazywf healthcheck`: whether `path` answers 200.
pub async fn probe(addr: SocketAddr, path: &str) -> bool {
    let work = async {
        let mut s = TcpStream::connect(addr).await.ok()?;
        s.write_all(format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes()).await.ok()?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.ok()?;
        Some(buf.starts_with(b"HTTP/1.1 200"))
    };
    matches!(tokio::time::timeout(Duration::from_secs(3), work).await, Ok(Some(true)))
}
