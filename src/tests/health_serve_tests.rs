//! The health endpoint on the wire: what `serve` and `handle` actually put on the socket (the
//! status line, the headers, the exact body), `HEAD` without a body, `405` for anything but
//! GET and HEAD, the bearer challenge a refused `/status` owes, and what `probe` (the container
//! health check) accepts as live. The answers themselves — which status for which path, what
//! `bearer_ok` accepts — are the module's own tests; these check the bytes around them.

use super::*;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The challenge `handle` adds to a `401`: the gate is a bearer one, so the refusal names it.
const CHALLENGE: &str = "WWW-Authenticate: Bearer\r\n";

/// The head `handle` writes for one answer, built from what the body is meant to be: the status
/// line, the content type, `extra` (the challenge, on a `401` only), the length of exactly this
/// body, and the close.
fn resp_head_with(status: u16, reason: &str, ctype: &str, extra: &str, body: &[u8]) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
}

/// The head of an answer that carries no challenge: every answer but the `401`.
fn resp_head(status: u16, reason: &str, ctype: &str, body: &[u8]) -> String {
    resp_head_with(status, reason, ctype, "", body)
}

/// Live, streaming, ready, with a status document.
fn ready(token: Option<&str>) -> Arc<Health> {
    let h = Health::new(token.map(|t| t.to_string()));
    h.set_status(json!({"rules": {"orders": "ok"}}));
    h.set_phase("streaming");
    h.set_streaming(true);
    Arc::new(h)
}

/// The status document `/status` serves, as the wire sends it: pretty JSON plus the two live
/// fields, and the newline that ends it.
const STATUS_BODY: &str = "{\n  \"rules\": {\n    \"orders\": \"ok\"\n  },\n  \
                           \"phase\": \"streaming\",\n  \"streaming\": true\n}\n";
const INDEX_BODY: &str = "lazywf health endpoint: /livez /readyz /status\n";
const OK: &[u8] = b"ok\n";
const NOT_FOUND: &[u8] = b"not found\n";
const UNAUTHORIZED: &[u8] = b"unauthorized\n";
const NOT_ALLOWED: &[u8] = b"method not allowed\n";

/// Serves one endpoint over [`serve_listener`] on a port of the kernel's choosing — the accept
/// loop is the same one `serve` runs once it has bound, and this way the test never races for a
/// number. Returns its address and its task.
async fn endpoint(h: Arc<Health>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let addr = listener.local_addr().expect("its address");
    (addr, tokio::spawn(serve_listener(listener, h)))
}

/// The same through the public [`serve`], which binds the address itself: the entry point the
/// process actually starts. The number comes from a listener dropped at once, so a competitor
/// that takes it in the gap leaves `serve` in its retry sleep — then the try is abandoned and
/// another number taken.
async fn served(h: Arc<Health>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    for _ in 0..5 {
        let spare = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
        let addr = spare.local_addr().expect("its address");
        drop(spare);
        let task = tokio::spawn(serve(addr, h.clone()));
        for _ in 0..200 {
            if TcpStream::connect(addr).await.is_ok() {
                return (addr, task);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        task.abort();
    }
    panic!("the health endpoint never listened on a port of its own")
}

/// Sends `request` as written and returns the whole answer.
async fn ask(addr: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(request).await.expect("write");
    // Half-close: the server's reader stops here and the answer follows.
    s.shutdown().await.expect("half-close");
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.expect("read to end");
    buf
}

/// Sends `request` and splits the answer into its head (with the blank line) and its body.
async fn request(addr: SocketAddr, request: &[u8]) -> (String, Vec<u8>) {
    let all = ask(addr, request).await;
    let text = String::from_utf8_lossy(&all).into_owned();
    let end = text.find("\r\n\r\n").expect("a header block") + 4;
    (text[..end].to_string(), all[end..].to_vec())
}

fn get(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").into_bytes()
}

/// A request of `method` with the credential `authorization` (sent verbatim, so a missing or
/// oddly-cased header is what it says it is).
fn with_authorization(method: &str, path: &str, authorization: Option<&str>) -> Vec<u8> {
    match authorization {
        Some(value) => format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\n{AUTHORIZATION_HEADER}: {value}\r\n\r\n"
        )
        .into_bytes(),
        None => get(path),
    }
}

/// The probes answer plain text, with the length the body really has, and no keep-alive.
#[tokio::test]
async fn the_probes_answer_with_exact_bytes() {
    let (addr, task) = endpoint(ready(None)).await;
    for path in ["/livez", "/healthz", "/readyz", "/livez?verbose=1"] {
        let (head, body) = request(addr, &get(path)).await;
        assert_eq!(
            (head.as_str(), body.as_slice()),
            (resp_head(200, "OK", "text/plain", OK).as_str(), OK,),
            "{path}"
        );
    }
    let (head, body) = request(addr, &get("/")).await;
    assert_eq!(
        (head.as_str(), body.as_slice()),
        (
            resp_head(200, "OK", "text/plain", INDEX_BODY.as_bytes()).as_str(),
            INDEX_BODY.as_bytes()
        )
    );
    task.abort();
}

/// `/status` is the pretty JSON document with the two live fields added, and the head says
/// JSON with the length of exactly that body.
#[tokio::test]
async fn status_is_pretty_json_with_the_live_fields() {
    let (addr, task) = endpoint(ready(None)).await;
    let (head, body) = request(addr, &get("/status")).await;
    assert_eq!(
        head,
        resp_head(200, "OK", "application/json", STATUS_BODY.as_bytes())
    );
    assert_eq!(String::from_utf8(body).expect("utf-8"), STATUS_BODY);
    task.abort();
}

/// A path that is none of the four: `404` with the plain refusal, nothing else to learn from.
#[tokio::test]
async fn an_unknown_path_is_not_found() {
    let (addr, task) = endpoint(ready(None)).await;
    for path in ["/nope", "/livez/", "/status2", "/livez/x"] {
        let (head, body) = request(addr, &get(path)).await;
        assert_eq!(
            (head.as_str(), body.as_slice()),
            (
                resp_head(404, "Not Found", "text/plain", NOT_FOUND).as_str(),
                NOT_FOUND,
            ),
            "{path}"
        );
    }
    task.abort();
}

/// `HEAD` gets the same head as `GET` — including the `Content-Length` of the body it does not
/// send — and no body at all, for an answer as well as for a refused path.
#[tokio::test]
async fn head_sends_the_head_and_no_body() {
    let (addr, task) = endpoint(ready(None)).await;
    for path in ["/livez", "/readyz", "/status", "/"] {
        let (head_head, head_body) = request(
            addr,
            format!("HEAD {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes(),
        )
        .await;
        let (get_head, get_body) = request(addr, &get(path)).await;
        assert!(head_body.is_empty(), "{path}: HEAD sent a body");
        assert_eq!(
            head_head, get_head,
            "{path}: HEAD and GET differ in the head"
        );
        assert!(
            head_head.contains(&format!("Content-Length: {}\r\n", get_body.len())),
            "{path}: {head_head}"
        );
    }
    // A `HEAD` for an unknown path is a 404 head with no body.
    let (head, body) = request(addr, b"HEAD /nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
    assert_eq!(head, resp_head(404, "Not Found", "text/plain", NOT_FOUND));
    assert!(body.is_empty());
    // A `HEAD` refused by the token gate keeps the challenge too, still with no body.
    let (gated, gated_task) = endpoint(ready(Some("s3cret"))).await;
    let (head, body) = request(
        gated,
        &with_authorization("HEAD", "/status", Some("Bearer nope")),
    )
    .await;
    assert_eq!(
        head,
        resp_head_with(401, "Unauthorized", "text/plain", CHALLENGE, UNAUTHORIZED)
    );
    assert!(body.is_empty(), "a HEAD sent a body");
    task.abort();
    gated_task.abort();
}

/// Only `GET` and `HEAD` are answered; everything else — and a request line with no method at
/// all — is the same `405`. The method is not case-folded, and the second word is the path
/// whatever it says. A `405` is not the token gate, so it carries no challenge.
#[tokio::test]
async fn anything_but_get_and_head_is_refused() {
    let (addr, task) = endpoint(ready(None)).await;
    let refused = (
        resp_head(405, "Method Not Allowed", "text/plain", NOT_ALLOWED),
        NOT_ALLOWED.to_vec(),
    );
    for c in [
        "POST /livez HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nhi",
        "PUT /status HTTP/1.1\r\nHost: x\r\n\r\n",
        "DELETE /readyz HTTP/1.1\r\nHost: x\r\n\r\n",
        "OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n",
        "get /livez HTTP/1.1\r\nHost: x\r\n\r\n",
        "\r\n\r\n",
    ] {
        assert_eq!(request(addr, c.as_bytes()).await, refused, "{c:?}");
    }
    // With no second word the path is `/`, the index page.
    let (head, body) = request(addr, b"GET\r\n\r\n").await;
    assert_eq!(
        (head.as_str(), body.as_slice()),
        (
            resp_head(200, "OK", "text/plain", INDEX_BODY.as_bytes()).as_str(),
            INDEX_BODY.as_bytes()
        )
    );
    // And the second word is taken as the path without being read.
    assert_eq!(
        request(addr, b"GET nonsense HTTP/1.1\r\n\r\n").await,
        (
            resp_head(404, "Not Found", "text/plain", NOT_FOUND),
            NOT_FOUND.to_vec()
        )
    );
    // A refused method asks for the token and is refused for the method anyway: the gate is
    // never reached, so no challenge comes back.
    let (gated, gated_task) = endpoint(ready(Some("s3cret"))).await;
    assert_eq!(
        request(gated, b"POST /status HTTP/1.1\r\nHost: x\r\n\r\n").await,
        refused,
        "a 405 is not the gate"
    );
    task.abort();
    gated_task.abort();
}

/// The `/status` gate as the network presents it: everything but `Bearer <token>` in the
/// `Authorization` header answers the one `401`, with the bearer challenge on it; the header is
/// read case-insensitively and trimmed; another header never opens it; the probes never ask.
#[tokio::test]
async fn the_status_gate_is_checked_on_the_wire() {
    let (addr, task) = endpoint(ready(Some("s3cret"))).await;
    let refused = (
        resp_head_with(401, "Unauthorized", "text/plain", CHALLENGE, UNAUTHORIZED),
        UNAUTHORIZED.to_vec(),
    );
    assert_eq!(request(addr, &get("/status")).await, refused);
    for given in [
        "s3cret",         // the scheme is required
        "Bearer",         // and so is the credential
        "Bearer ",        // an empty credential
        "Bearer s3cre",   // a prefix of the token
        "Bearer s3crex",  // a same-length near miss
        "Bearer s3cret!", // one byte more
        "Basic s3cret",   // another scheme
        "",               // an empty header
    ] {
        assert_eq!(
            request(
                addr,
                format!(
                    "GET /status HTTP/1.1\r\nHost: x\r\n{AUTHORIZATION_HEADER}: {given}\r\n\r\n"
                )
                .as_bytes(),
            )
            .await,
            refused,
            "{given:?}"
        );
    }
    // The header name is HTTP-case-insensitive, the scheme word too, and the padding around the
    // credential is not part of the secret.
    for (name, value) in [
        (AUTHORIZATION_HEADER, "Bearer s3cret"),
        ("authorization", "bearer s3cret"),
        ("AUTHORIZATION", "Bearer   s3cret  "),
    ] {
        let (head, body) = request(
            addr,
            format!("GET /status HTTP/1.1\r\nHost: x\r\n{name}: {value}\r\n\r\n").as_bytes(),
        )
        .await;
        assert_eq!(
            head,
            resp_head(200, "OK", "application/json", STATUS_BODY.as_bytes()),
            "{name}: {value:?}"
        );
        assert_eq!(String::from_utf8(body).expect("utf-8"), STATUS_BODY);
    }
    // A header of another name is not the credential, whatever it holds.
    assert_eq!(
        request(
            addr,
            b"GET /status HTTP/1.1\r\nHost: x\r\nX-Lazywf-Token: Bearer s3cret\r\n\r\n"
        )
        .await,
        refused,
        "only Authorization opens the gate"
    );
    // The probes and the index answer without it, and a credential on a probe changes nothing.
    for (path, want) in [
        ("/livez", "HTTP/1.1 200 OK\r\n"),
        ("/readyz", "HTTP/1.1 200 OK\r\n"),
        ("/", "HTTP/1.1 200 OK\r\n"),
        ("/nope", "HTTP/1.1 404 Not Found\r\n"),
    ] {
        let (head, _) = request(
            addr,
            format!("GET {path} HTTP/1.1\r\nHost: x\r\n{AUTHORIZATION_HEADER}: whatever\r\n\r\n")
                .as_bytes(),
        )
        .await;
        assert!(head.starts_with(want), "{path}: {head}");
    }
    task.abort();
}

/// A request head with bare `\n` endings is a request too (the reader and the header parser
/// both take either ending), and one that never ends or is far too big is dropped without an
/// answer.
#[tokio::test]
async fn a_request_head_of_bare_newlines_is_read() {
    let (addr, task) = endpoint(ready(None)).await;
    let (head, body) = request(addr, b"GET /livez HTTP/1.1\nHost: x\n\n").await;
    assert_eq!(
        (head.as_str(), body.as_slice()),
        (resp_head(200, "OK", "text/plain", OK).as_str(), OK)
    );
    // With the token set, the same bare-`\n` head carries its credential.
    let (gated, gated_task) = endpoint(ready(Some("s3cret"))).await;
    let (head, body) = request(
        gated,
        b"GET /status HTTP/1.1\nAuthorization: Bearer s3cret\n\n",
    )
    .await;
    assert_eq!(
        head,
        resp_head(200, "OK", "application/json", STATUS_BODY.as_bytes())
    );
    assert_eq!(body, STATUS_BODY.as_bytes());
    // Too big to be a request head: dropped without an answer.
    let mut big = b"GET /livez HTTP/1.1\r\nX-Pad: ".to_vec();
    big.extend(std::iter::repeat_n(b'x', 9000));
    assert!(
        ask(addr, &big).await.is_empty(),
        "no answer to an oversized head"
    );
    task.abort();
    gated_task.abort();
}

/// `probe` is what `lazywf healthcheck` asks: only a `200` on that path is alive. A stalled
/// loop, an unknown path, a token-gated `/status` and a port nobody listens on are all not.
/// Served through [`serve`] itself, the way the process runs it.
#[tokio::test]
async fn probe_accepts_only_a_live_200() {
    let h = ready(None);
    let (addr, task) = served(h.clone()).await;
    assert!(probe(addr, "/livez").await, "a live loop");
    assert!(probe(addr, "/readyz").await, "a streaming instance");
    assert!(probe(addr, "/").await, "the index answers 200 too");

    // Stalled: both probes answer 503, so the container is not alive.
    h.with(|i| {
        i.beat = Instant::now()
            .checked_sub(STALL + Duration::from_secs(1))
            .unwrap()
    });
    assert!(!probe(addr, "/livez").await, "a stalled loop is not live");
    assert!(!probe(addr, "/readyz").await, "a stalled loop is not ready");
    h.beat();
    assert!(probe(addr, "/livez").await, "and a beat brings it back");
    assert!(!probe(addr, "/nope").await, "404 is not alive");

    // A `/status` that answers 401 is not alive either; its probes still are.
    let (gated, gated_task) = endpoint(ready(Some("s3cret"))).await;
    assert!(
        !probe(gated, "/status").await,
        "a token-gated /status is not alive"
    );
    assert!(probe(gated, "/livez").await, "the probe needs no token");

    // Nothing listening: not alive, and no panic — the case the health check runs when the
    // process died with its socket.
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    assert!(!probe(dead, "/livez").await, "a closed port is not alive");

    task.abort();
    gated_task.abort();
}

/// A poisoned health mutex is data, not a second panic. Of all the `lock().expect` sites in the
/// hot path this is one of the two the engine task writes (`beat`, `set_phase`, `set_status`), so
/// a panic here was the process's last act rather than a bad answer. Every setter replaces one
/// field whole, so the guard a panicking task left holds a real document: `/status` serves what
/// the engine wrote, and the probe reads what it says.
#[tokio::test]
async fn a_poisoned_health_lock_still_serves_status_and_readyz() {
    let h = ready(Some("tok"));
    crate::poison::poison(&h.inner);
    // The engine's own writes work on the other side of the poison, and the fields still answer.
    h.beat();
    h.set_streaming(true);
    assert!(h.is_ready(), "a poisoned health is still ready");
    assert_eq!(
        h.status(),
        json!({"rules": {"orders": "ok"}}),
        "the document written before the panic is the one still held"
    );

    let (addr, task) = endpoint(Arc::clone(&h)).await;
    let (head, body) = request(
        addr,
        &with_authorization("GET", "/status", Some("Bearer tok")),
    )
    .await;
    task.abort();
    assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
    assert_eq!(
        String::from_utf8_lossy(&body),
        STATUS_BODY,
        "/status renders the pre-panic document, byte for byte"
    );
}
