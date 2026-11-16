//! The health endpoint on the wire: what `serve` and `handle` actually put on the socket (the
//! status line, the headers, the exact body), `HEAD` without a body, `405` for anything but
//! GET and HEAD, and what `probe` (the container health check) accepts as live.

use super::*;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The head `handle` writes for one answer, built from what the body is meant to be.
fn resp_head(status: u16, reason: &str, ctype: &str, body: &[u8]) -> String {
    format!("HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())
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

/// Serves one endpoint on a free loopback port and returns its address and its task; the port
/// comes from a listener that is dropped at once so the endpoint under test can take it, and
/// the connect loop waits for that bind.
async fn endpoint(h: Arc<Health>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let spare = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let addr = spare.local_addr().expect("its address");
    drop(spare);
    let task = tokio::spawn(serve(addr, h));
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return (addr, task);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the health endpoint never listened on {addr}");
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

/// The probes answer plain text, with the length the body really has, and no keep-alive.
#[tokio::test]
async fn the_probes_answer_with_exact_bytes() {
    let (addr, task) = endpoint(ready(None)).await;
    for path in ["/livez", "/healthz", "/readyz", "/livez?verbose=1"] {
        let (head, body) = request(addr, &get(path)).await;
        assert_eq!(
            (head.as_str(), body.as_slice()),
            (
                resp_head(200, "OK", "text/plain", b"ok\n").as_str(),
                b"ok\n".as_slice()
            ),
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
                resp_head(404, "Not Found", "text/plain", b"not found\n").as_str(),
                b"not found\n".as_slice()
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
    assert_eq!(
        head,
        resp_head(404, "Not Found", "text/plain", b"not found\n")
    );
    assert!(body.is_empty());
    task.abort();
}

/// Only `GET` and `HEAD` are answered; everything else — and a request line with no method at
/// all — is the same `405`. The method is not case-folded, and the second word is the path
/// whatever it says.
#[tokio::test]
async fn anything_but_get_and_head_is_refused() {
    let (addr, task) = endpoint(ready(None)).await;
    let refused = (
        resp_head(
            405,
            "Method Not Allowed",
            "text/plain",
            b"method not allowed\n",
        ),
        b"method not allowed\n".to_vec(),
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
            resp_head(404, "Not Found", "text/plain", b"not found\n"),
            b"not found\n".to_vec()
        )
    );
    task.abort();
}

/// The `/status` token as the network presents it: no header, a prefix, a same-length near
/// miss and an empty header all answer the one `401`; the header is read case-insensitively and
/// trimmed; the probes never ask for it.
#[tokio::test]
async fn the_status_token_is_checked_on_the_wire() {
    let (addr, task) = endpoint(ready(Some("s3cret"))).await;
    let refused = (
        resp_head(401, "Unauthorized", "text/plain", b"unauthorized\n"),
        b"unauthorized\n".to_vec(),
    );
    assert_eq!(request(addr, &get("/status")).await, refused);
    for token in ["s3cre", "s3crex", ""] {
        assert_eq!(
            request(
                addr,
                format!(
                    "GET /status HTTP/1.1\r\nHost: x\r\n{STATUS_TOKEN_HEADER}: {token}\r\n\r\n"
                )
                .as_bytes(),
            )
            .await,
            refused,
            "{token:?}"
        );
    }
    for (name, value) in [
        (STATUS_TOKEN_HEADER, "s3cret"),
        ("x-lazywf-token", "  s3cret  "),
        ("X-LAZYWF-TOKEN", "s3cret"),
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
    // A header of another name is not the token.
    assert_eq!(
        request(
            addr,
            b"GET /status HTTP/1.1\r\nHost: x\r\nAuthorization: s3cret\r\n\r\n"
        )
        .await,
        refused
    );
    // The probes and the index answer without it, and a token on a probe changes nothing.
    for (path, want) in [
        ("/livez", "HTTP/1.1 200 OK\r\n"),
        ("/readyz", "HTTP/1.1 200 OK\r\n"),
        ("/", "HTTP/1.1 200 OK\r\n"),
        ("/nope", "HTTP/1.1 404 Not Found\r\n"),
    ] {
        let (head, _) = request(
            addr,
            format!("GET {path} HTTP/1.1\r\nHost: x\r\n{STATUS_TOKEN_HEADER}: whatever\r\n\r\n")
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
        (
            resp_head(200, "OK", "text/plain", b"ok\n").as_str(),
            b"ok\n".as_slice()
        )
    );
    // With the token set, the same bare-`\n` head carries its header.
    let (gated, gated_task) = endpoint(ready(Some("s3cret"))).await;
    let (head, body) = request(gated, b"GET /status HTTP/1.1\nX-Lazywf-Token: s3cret\n\n").await;
    assert_eq!(
        head,
        resp_head(200, "OK", "application/json", STATUS_BODY.as_bytes())
    );
    assert_eq!(body, STATUS_BODY.as_bytes());
    // Too big to be a request head: dropped without an answer.
    let mut big = b"GET /livez HTTP/1.1\r\nX-Pad: ".to_vec();
    big.extend(std::iter::repeat(b'x').take(9000));
    assert!(
        ask(addr, &big).await.is_empty(),
        "no answer to an oversized head"
    );
    task.abort();
    gated_task.abort();
}

/// `probe` is what `lazywf healthcheck` asks: only a `200` on that path is alive. A stalled
/// loop, an unknown path, a token-gated `/status` and a port nobody listens on are all not.
#[tokio::test]
async fn probe_accepts_only_a_live_200() {
    let h = ready(None);
    let (addr, task) = endpoint(h.clone()).await;
    assert!(probe(addr, "/livez").await, "a live loop");
    assert!(probe(addr, "/readyz").await, "a streaming instance");

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
