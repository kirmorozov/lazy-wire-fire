//! Health endpoint: `/livez`, `/readyz` and `/status` (JSON), on a local address.
//!
//! * `/livez`: 200 while the main loop is turning (it beats at least every few seconds);
//! * `/readyz`: 200 while this instance is active and streaming the binlog;
//! * `/status`: what each rule is doing. With `LAZYWF_HEALTH_TOKEN` set it also requires the
//!   `Authorization: Bearer <token>` header to match (constant-time); without it, 401 with
//!   `WWW-Authenticate: Bearer`. The same token gates `/metrics` on its own listener
//!   ([`crate::metrics`]), which is why the parser here is shared.
//!   The two probes never ask for the token: liveness checks must not need secrets, and the
//!   container health check runs `lazywf healthcheck` without any environment of its own.
//!
//! Both lazywf listeners read a request head with [`header_value`] and open a gated endpoint with
//! [`bearer_ok`], so the shape of a valid credential lives in exactly one place.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The header the token gate reads. HTTP names are case-insensitive, so a request carries it as
/// `Authorization`, `authorization` or `AUTHORIZATION` alike. The metrics listener gates with the
/// same header and the same token, so it reads it through this name too.
pub(crate) const AUTHORIZATION_HEADER: &str = "Authorization";

/// The loop must beat within this long for `/livez`.
pub const STALL: Duration = Duration::from_secs(60);

struct Inner {
    phase: &'static str,
    streaming: bool,
    beat: Instant,
    status: Value,
    /// `LAZYWF_HEALTH_TOKEN`, trimmed and never blank; `None` when unset: both gated endpoints
    /// stay open (the loopback default).
    health_token: Option<String>,
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
    /// `/status` (and `/metrics`, which is given the same token) requires it unless it is `None`.
    pub fn new(health_token: Option<String>) -> Health {
        Health {
            inner: Mutex::new(Inner {
                phase: "starting",
                streaming: false,
                beat: Instant::now(),
                status: json!({}),
                // Trimmed and blank-checked on store, the way the metrics listener stores it too:
                // a copied env token carries the trailing newline, while `bearer_ok` trims the
                // credential it reads — the two must meet on the same bytes.
                health_token: token_of(health_token),
            }),
        }
    }

    /// One lock for every field below, and it is never a second panic: each setter replaces one
    /// field whole, so a guard a task left behind by panicking still holds data. The worst a torn
    /// moment can say is `phase` already moved while `beat` has not caught up, and that direction
    /// only ever makes `/readyz` answer *not ready* — the conservative side. This is the lock the
    /// engine task writes (`beat`, `set_phase`, `set_status`), so a panic here would be the last
    /// thing the process did.
    fn with<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        f(&mut self.inner.lock().unwrap_or_else(|e| e.into_inner()))
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

    /// The answer to one request. `authorization` is the `Authorization` header the request
    /// carried: with `LAZYWF_HEALTH_TOKEN` set, `/status` is served only when it is
    /// `Bearer <token>` and matches in constant time; without the token (unset), `/status` answers
    /// as it always did. The probes ignore it.
    fn respond(&self, path: &str, authorization: Option<&str>) -> (u16, String, &'static str) {
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
                    if let Some(want) = &i.health_token {
                        // Constant-time: the header is a secret presented over the network.
                        if !bearer_ok(authorization, want) {
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
    let token = header_value(&buf, AUTHORIZATION_HEADER);
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
    // A 401 here is only ever the token gate, and a bearer challenge is what such a refusal owes
    // the client (the same answer lazyview's health server gives).
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

/// The value of one header of a request head (first occurrence, trimmed), or `None`. Split
/// on `\n` and trimmed of `\r`: the reader above accepts both line endings. Shared with
/// [`crate::metrics`], the other listener that reads a request head by hand.
pub(crate) fn header_value(buf: &[u8], name: &str) -> Option<String> {
    let head = String::from_utf8_lossy(buf);
    head.split('\n').skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case(name)
            .then(|| v.trim().to_string())
    })
}

/// The one gate both listeners ask about: `/status` here, `/metrics` in
/// [`crate::metrics::serve_listener`].
///
/// True when `header` is exactly `Bearer <token>` for `want`, compared in constant time: the
/// header is a secret presented over the network, so neither its length nor its first difference
/// may come back early. The scheme word is case-insensitive (HTTP allows it) and both sides are
/// trimmed — the token a file handed over keeps its newline until here, and a header can pad its
/// credential the same way, so the whitespace around a token is never part of the secret.
/// Anything else — no header, a bare `Bearer`, another scheme, a prefix of the token, the token
/// plus a word — is refused.
pub(crate) fn bearer_ok(header: Option<&str>, want: &str) -> bool {
    // An empty token authorizes nothing — not even an empty credential. `token_of` makes it
    // unreachable from either listener, and the gate does not rely on that.
    if want.trim().is_empty() {
        return false;
    }
    let Some((scheme, credential)) = header
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .and_then(|h| h.split_once(char::is_whitespace))
    else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer") && crate::secrets::ct_eq(credential.trim(), want.trim())
}

/// `LAZYWF_HEALTH_TOKEN` as both listeners store it: trimmed, and no token at all when it is
/// blank — an endpoint stays open rather than gating on a value nobody meant to set.
pub(crate) fn token_of(token: Option<String>) -> Option<String> {
    token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
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
    let token = h.with(|i| i.health_token.is_some());
    let needs = if token {
        ", /status needs Authorization: Bearer"
    } else {
        ""
    };
    tracing::info!("health endpoint on http://{addr} (/livez, /readyz, /status{needs})");
    serve_listener(listener, h).await;
}

/// The accept loop of [`serve`], over a listener someone else bound: the tests serve on a port
/// of the kernel's choosing instead of racing to grab a free one.
pub(crate) async fn serve_listener(listener: TcpListener, h: Arc<Health>) {
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

    fn answer_with(
        h: &Health,
        path: &str,
        authorization: Option<&str>,
    ) -> (u16, String, &'static str) {
        h.respond(path, authorization)
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

    /// `LAZYWF_HEALTH_TOKEN`: `/status` requires the bearer token, the probes never do.
    #[test]
    fn health_token_gates_only_status() {
        let h = Health::new(Some("s3cret".into()));
        h.set_status(json!({"rules": {"orders": "ok"}}));
        h.set_phase("streaming");
        h.set_streaming(true);
        // No header, an empty header, a bare scheme, another scheme, the bare token, a too-short
        // prefix, a same-length near-miss, the token with a word after it: all 401 with the same
        // body (the refusal gives nothing away).
        for given in [
            None,
            Some(""),
            Some("Bearer"),
            Some("Bearer "),
            Some("s3cret"),
            Some("Basic s3cret"),
            Some("Bearer s3cre"),
            Some("Bearer s3crex"),
            Some("Bearer s3cret extra"),
        ] {
            let (code, body, ctype) = answer_with(&h, "/status", given);
            assert_eq!(code, 401, "given {given:?}");
            assert_eq!(body, "unauthorized\n", "given {given:?}");
            assert_eq!(ctype, "text/plain", "given {given:?}");
        }
        let (code, body, _) = answer_with(&h, "/status", Some("Bearer s3cret"));
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

    /// The shape of a valid credential: the scheme word is HTTP-case-insensitive, the credential
    /// is trimmed (the newline a token file ends with is not part of the token), and nothing else
    /// opens — not a prefix of the token, not another scheme, not the token without a scheme.
    #[test]
    fn bearer_ok_accepts_only_a_bearer_credential() {
        let ok = |h: Option<&str>| bearer_ok(h, "s3cret");
        assert!(ok(Some("Bearer s3cret")));
        assert!(
            ok(Some("bearer s3cret")),
            "the scheme word is case-insensitive"
        );
        assert!(ok(Some("BEARER   s3cret  ")), "the credential is trimmed");
        assert!(!ok(None), "no header is no credential");
        assert!(!ok(Some("")), "an empty header");
        assert!(!ok(Some("s3cret")), "the scheme is required");
        assert!(!ok(Some("Basic s3cret")), "only Bearer");
        assert!(!ok(Some("Bearer")), "a scheme without a credential");
        assert!(!ok(Some("Bearer ")), "an empty credential");
        assert!(
            !ok(Some("Bearer s3cre")),
            "a prefix of the token is not the token"
        );
        assert!(!ok(Some("Bearer s3cret!")));
        assert!(!ok(Some("Bearer s3cret x")), "the credential is one word");
        assert!(!ok(Some("s3cret Bearer")), "the scheme comes first");
        // An empty token authorizes nothing, not even an empty credential: `token_of` never lets
        // one reach a listener, and the gate does not rely on that.
        assert!(!bearer_ok(Some("Bearer "), ""));
        assert!(!bearer_ok(Some("Bearer x"), ""));
    }

    /// A token copied from an env file arrives padded (` s3cret\n`), and a header can carry its
    /// credential padded too: the stored token is trimmed on store and `bearer_ok` trims the
    /// credential, so the two meet on the same bytes.
    #[test]
    fn a_padded_token_still_matches_a_clean_header() {
        let h = Health::new(Some(" s3cret\n".into()));
        h.set_status(json!({}));
        assert_eq!(answer_with(&h, "/status", Some("Bearer s3cret")).0, 200);
        assert_eq!(
            answer_with(&h, "/status", Some("Bearer   s3cret \r")).0,
            200,
            "the file's newline and the header's padding are both trim, not content"
        );
        assert_eq!(answer_with(&h, "/status", Some("Bearer s3cret2")).0, 401);
    }

    #[test]
    fn a_blank_token_is_no_token_and_an_extra_header_is_ignored() {
        let h = Health::new(Some("   ".into()));
        h.set_status(json!({}));
        assert_eq!(answer_with(&h, "/status", None).0, 200);
        // Without the setting the behaviour is unchanged, header or not.
        let h = Health::default();
        assert_eq!(answer_with(&h, "/status", Some("Bearer anything")).0, 200);
    }

    #[test]
    fn the_authorization_header_is_read_case_insensitively_from_both_line_endings() {
        let buf = b"GET /status HTTP/1.1\r\nHost: x\r\nauthorization: Bearer abc \r\n\r\n";
        assert_eq!(
            header_value(buf, AUTHORIZATION_HEADER).as_deref(),
            Some("Bearer abc")
        );
        let buf = b"GET /status HTTP/1.1\nHost: x\nAUTHORIZATION:Bearer abc\n\n";
        assert_eq!(
            header_value(buf, AUTHORIZATION_HEADER).as_deref(),
            Some("Bearer abc")
        );
        let buf = b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(header_value(buf, AUTHORIZATION_HEADER), None);
        // The retired `X-Lazywf-Token` is an ordinary header now: it answers nothing.
        let buf = b"GET /status HTTP/1.1\r\nHost: x\r\nX-Lazywf-Token: abc\r\n\r\n";
        assert_eq!(header_value(buf, AUTHORIZATION_HEADER), None);
    }

    /// Serves `h` on a port of the kernel's choosing: the two tests below speak HTTP to it the
    /// way a monitoring agent and a container health check do.
    async fn serving(h: Arc<Health>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_listener(listener, h));
        addr
    }

    /// One request over a real socket, answered as written (`"HTTP/1.1 401 ...\r\n..."`).
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

    /// The gate on the wire: every refusal is a 401 that carries `WWW-Authenticate: Bearer` and
    /// never repeats the token — including the one that still sends the retired
    /// `X-Lazywf-Token` header. The probes, the index and `lazywf healthcheck` need no header.
    #[tokio::test]
    async fn status_answers_only_to_the_bearer_token() {
        let h = Arc::new(Health::new(Some("s3cret".into())));
        h.set_status(json!({"rules": {"orders": "ok"}}));
        h.set_phase("streaming");
        h.set_streaming(true);
        let addr = serving(h.clone()).await;

        for head in [
            "GET /status HTTP/1.1\r\nHost: x\r\n\r\n",
            "GET /status HTTP/1.1\r\nHost: x\r\nX-Lazywf-Token: s3cret\r\n\r\n",
            "GET /status HTTP/1.1\r\nHost: x\r\nAuthorization: s3cret\r\n\r\n",
            "GET /status HTTP/1.1\r\nHost: x\r\nAuthorization: Basic s3cret\r\n\r\n",
            "GET /status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer s3cret2\r\n\r\n",
        ] {
            let r = get(addr, head).await;
            assert_eq!(code(&r), 401, "{head}");
            assert!(r.contains("WWW-Authenticate: Bearer"), "{head}: {r}");
            assert!(!r.contains("s3cret"), "the refusal repeats the token: {r}");
        }

        let r = get(
            addr,
            "GET /status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer s3cret\r\n\r\n",
        )
        .await;
        assert_eq!(code(&r), 200, "{r}");
        assert!(
            r.contains("application/json") && r.contains("\"orders\""),
            "{r}"
        );

        for path in ["/livez", "/readyz", "/"] {
            let r = get(addr, &format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")).await;
            assert_eq!(code(&r), 200, "{path} must stay open");
        }
        assert_eq!(
            code(&get(addr, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").await),
            404
        );
        assert!(
            probe(addr, "/livez").await,
            "lazywf healthcheck never needs the token"
        );
    }
}

#[cfg(test)]
#[path = "tests/health_serve_tests.rs"]
mod serve_tests;
