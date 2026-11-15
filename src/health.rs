//! Health endpoint: `/livez`, `/readyz` and `/status` (JSON), on a local address.
//!
//! * `/livez`: 200 while the main loop is turning (it beats at least every few seconds);
//! * `/readyz`: 200 while this instance is active and streaming the binlog;
//! * `/status`: what each rule is doing. With `LAZYWF_STATUS_TOKEN` set it also requires the
//!   `X-Lazywf-Token` header to match (constant-time); without the header it answers 401.
//!   The two probes never ask for the token: liveness checks must not need secrets, and the
//!   container health check runs `lazywf healthcheck` without any environment of its own.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The header `/status` must carry when `LAZYWF_STATUS_TOKEN` is set.
pub const STATUS_TOKEN_HEADER: &str = "X-Lazywf-Token";

/// The loop must beat within this long for `/livez`.
pub const STALL: Duration = Duration::from_secs(60);

struct Inner {
    phase: &'static str,
    streaming: bool,
    beat: Instant,
    status: Value,
    /// `LAZYWF_STATUS_TOKEN`, trimmed and never blank; `None` when unset: `/status` stays
    /// open (the loopback default).
    status_token: Option<String>,
}

pub struct Health {
    inner: Mutex<Inner>,
}

impl Default for Health {
    fn default() -> Self {
        Health::new(None)
    }
}

impl Health {
    /// `/status` requires the token unless it is `None`.
    pub fn new(status_token: Option<String>) -> Health {
        Health {
            inner: Mutex::new(Inner {
                phase: "starting",
                streaming: false,
                beat: Instant::now(),
                status: json!({}),
                // Trimmed on store: a copied env token carries the trailing newline, while
                // `header_value` trims the header — the two must meet on the same bytes.
                status_token: status_token
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty()),
            }),
        }
    }

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

    /// What `/readyz` answers 200 for.
    pub fn is_ready(&self) -> bool {
        self.with(|i| i.phase == "streaming" && i.streaming && i.beat.elapsed() < STALL)
    }

    /// What `/status` shows (without `phase` and `streaming`).
    pub fn status(&self) -> Value {
        self.with(|i| i.status.clone())
    }

    pub fn set_status(&self, status: Value) {
        self.with(|i| i.status = status);
    }

    /// The answer to one request. `token` is the `X-Lazywf-Token` header the request carried:
    /// with `LAZYWF_STATUS_TOKEN` set, `/status` is served only when it matches in constant
    /// time; without the token (unset), `/status` answers as it always did. The probes ignore
    /// it.
    fn respond(&self, path: &str, token: Option<&str>) -> (u16, String, &'static str) {
        self.with(|i| {
            let live = i.beat.elapsed() < STALL;
            match path {
                "/livez" | "/healthz" => {
                    if live {
                        (200, "ok\n".into(), "text/plain")
                    } else {
                        (
                            503,
                            format!("main loop stalled for {}s\n", i.beat.elapsed().as_secs()),
                            "text/plain",
                        )
                    }
                }
                "/readyz" => {
                    if live && i.phase == "streaming" && i.streaming {
                        (200, "ok\n".into(), "text/plain")
                    } else if !live {
                        (503, "stalled\n".into(), "text/plain")
                    } else {
                        (
                            503,
                            format!(
                                "{}{}\n",
                                i.phase,
                                if i.streaming {
                                    ""
                                } else {
                                    ", binlog not connected"
                                }
                            ),
                            "text/plain",
                        )
                    }
                }
                "/status" => {
                    if let Some(want) = &i.status_token {
                        // Constant-time: the header is a secret presented over the network.
                        if !token.is_some_and(|t| crate::secrets::ct_eq(t, want)) {
                            return (401, "unauthorized\n".into(), "text/plain");
                        }
                    }
                    let mut s = i.status.clone();
                    if let Value::Object(m) = &mut s {
                        m.insert("phase".into(), json!(i.phase));
                        m.insert("streaming".into(), json!(i.streaming));
                    }
                    (200, format!("{s:#}\n"), "application/json")
                }
                "/" => (
                    200,
                    "lazywf health endpoint: /livez /readyz /status\n".into(),
                    "text/plain",
                ),
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
    let token = header_value(&buf, STATUS_TOKEN_HEADER);
    let (status, body, ctype) = if matches!(method, "GET" | "HEAD") {
        h.respond(path, token.as_deref())
    } else {
        (405, "method not allowed\n".into(), "text/plain")
    };
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
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

/// The value of one header of a request head (first occurrence, trimmed), or `None`. Split
/// on `\n` and trimmed of `\r`: the reader above accepts both line endings.
fn header_value(buf: &[u8], name: &str) -> Option<String> {
    let head = String::from_utf8_lossy(buf);
    head.split('\n').skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case(name)
            .then(|| v.trim().to_string())
    })
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
    let token = h.with(|i| i.status_token.is_some());
    let needs = if token {
        ", /status requires X-Lazywf-Token"
    } else {
        ""
    };
    tracing::info!("health endpoint on http://{addr} (/livez, /readyz, /status{needs})");
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
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.ok()?;
        Some(buf.starts_with(b"HTTP/1.1 200"))
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(3), work).await,
        Ok(Some(true))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(h: &Health, path: &str) -> (u16, String, &'static str) {
        h.respond(path, None)
    }

    fn answer_with(h: &Health, path: &str, token: Option<&str>) -> (u16, String, &'static str) {
        h.respond(path, token)
    }

    fn stall(h: &Health) {
        h.with(|i| {
            i.beat = Instant::now()
                .checked_sub(STALL + Duration::from_secs(60))
                .unwrap()
        });
    }

    #[test]
    fn livez_follows_the_heartbeat() {
        let h = Health::default();
        assert_eq!(answer(&h, "/livez"), (200, "ok\n".into(), "text/plain"));
        assert_eq!(answer(&h, "/healthz").0, 200);
        stall(&h);
        let (code, body, _) = answer(&h, "/livez");
        assert_eq!(code, 503);
        assert!(body.starts_with("main loop stalled for "), "{body}");
        assert!(!h.is_ready());
    }

    #[test]
    fn readyz_needs_streaming_phase_and_a_connected_binlog() {
        let h = Health::default();
        // Starting: not streaming yet, the body names both missing things.
        let (code, body, _) = answer(&h, "/readyz");
        assert_eq!(code, 503);
        assert_eq!(body, "starting, binlog not connected\n");

        h.set_phase("streaming");
        let (code, body, _) = answer(&h, "/readyz");
        assert_eq!(code, 503);
        assert_eq!(body, "streaming, binlog not connected\n");
        assert!(!h.is_ready());

        h.set_streaming(true);
        assert_eq!(answer(&h, "/readyz"), (200, "ok\n".into(), "text/plain"));
        assert!(h.is_ready());

        // A phase other than streaming stays not ready even with the binlog connected.
        h.set_phase("catching up");
        let (code, body, _) = answer(&h, "/readyz");
        assert_eq!(code, 503);
        assert_eq!(body, "catching up\n");
        assert!(!h.is_ready());
    }

    #[test]
    fn readyz_on_a_stalled_loop_says_stalled() {
        let h = Health::default();
        h.set_phase("streaming");
        h.set_streaming(true);
        stall(&h);
        assert_eq!(
            answer(&h, "/readyz"),
            (503, "stalled\n".into(), "text/plain")
        );
    }

    #[test]
    fn status_is_json_with_phase_and_streaming() {
        let h = Health::default();
        h.set_status(json!({"rules": {"orders": "ok"}}));
        h.set_phase("streaming");
        h.set_streaming(true);
        let (code, body, ctype) = answer(&h, "/status");
        assert_eq!(code, 200);
        assert_eq!(ctype, "application/json");
        assert!(body.ends_with('\n'));
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v,
            json!({"rules": {"orders": "ok"}, "phase": "streaming", "streaming": true})
        );
        // The accessor the rest of the program reads leaves out phase and streaming.
        assert_eq!(h.status(), json!({"rules": {"orders": "ok"}}));
    }

    #[test]
    fn index_and_unknown_paths() {
        let h = Health::default();
        let (code, body, _) = answer(&h, "/");
        assert_eq!(code, 200);
        assert!(body.contains("/livez") && body.contains("/readyz") && body.contains("/status"));
        assert_eq!(
            answer(&h, "/nope"),
            (404, "not found\n".into(), "text/plain")
        );
    }

    /// `LAZYWF_STATUS_TOKEN`: `/status` requires the header, the probes never do.
    #[test]
    fn status_token_gates_only_status() {
        let h = Health::new(Some("s3cret".into()));
        h.set_status(json!({"rules": {"orders": "ok"}}));
        h.set_phase("streaming");
        h.set_streaming(true);
        // No header, a too-short prefix, a same-length near-miss, an empty header: all 401
        // with the same body (the refusal gives nothing away).
        for given in [None, Some("s3cre"), Some("s3crex"), Some("")] {
            let (code, body, ctype) = answer_with(&h, "/status", given);
            assert_eq!(code, 401, "given {given:?}");
            assert_eq!(body, "unauthorized\n", "given {given:?}");
            assert_eq!(ctype, "text/plain", "given {given:?}");
        }
        let (code, body, _) = answer_with(&h, "/status", Some("s3cret"));
        assert_eq!(code, 200);
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["rules"], json!({"orders": "ok"}));
        assert_eq!(v["phase"], "streaming");
        // The probes and the index answer without any header.
        assert_eq!(answer_with(&h, "/livez", None).0, 200);
        assert_eq!(answer_with(&h, "/readyz", None).0, 200);
        assert_eq!(answer_with(&h, "/", None).0, 200);
        assert_eq!(answer_with(&h, "/nope", None).0, 404);
    }

    /// A token copied from an env file arrives padded (`s3cret\n`): the stored token is
    /// trimmed on store so it still meets the header `header_value` delivers (which trims
    /// its own value, as its test shows).
    #[test]
    fn a_padded_token_still_matches_a_clean_header() {
        let h = Health::new(Some(" s3cret\n".into()));
        h.set_status(json!({}));
        assert_eq!(answer_with(&h, "/status", Some("s3cret")).0, 200);
        assert_eq!(answer_with(&h, "/status", Some("s3cret2")).0, 401);
    }

    #[test]
    fn a_blank_token_is_no_token_and_an_extra_header_is_ignored() {
        let h = Health::new(Some("   ".into()));
        h.set_status(json!({}));
        assert_eq!(answer_with(&h, "/status", None).0, 200);
        // Without the setting the behaviour is unchanged, header or not.
        let h = Health::default();
        assert_eq!(answer_with(&h, "/status", Some("anything")).0, 200);
    }

    #[test]
    fn the_token_header_is_read_case_insensitively_from_both_line_endings() {
        let buf = b"GET /status HTTP/1.1\r\nHost: x\r\nx-lazywf-token: abc \r\n\r\n";
        assert_eq!(
            header_value(buf, STATUS_TOKEN_HEADER).as_deref(),
            Some("abc")
        );
        let buf = b"GET /status HTTP/1.1\nHost: x\nX-Lazywf-Token:abc\n\n";
        assert_eq!(
            header_value(buf, STATUS_TOKEN_HEADER).as_deref(),
            Some("abc")
        );
        let buf = b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(header_value(buf, STATUS_TOKEN_HEADER), None);
    }
}
