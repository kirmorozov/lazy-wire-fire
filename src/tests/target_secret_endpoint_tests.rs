use super::*;
use crate::model::{RawTarget, TargetDef};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The secret part of a webhook URL. It must reach the wire and nowhere else.
const MARKER: &str = "SECMARK-4c7e";
const HOST: &str = "hooks.example.test";

fn raw_target(kind: &str, endpoints: &str, vars: &str) -> RawTarget {
    RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoints.into()),
        vars: Some(vars.into()),
        ..Default::default()
    }
}

fn load(kind: &str, endpoint: &str, vars: &str, allowed: &str) -> Result<Arc<Target>> {
    let def = TargetDef::from_row(&raw_target(kind, endpoint, vars))?;
    let list = HostList::parse(allowed)?;
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    Target::new(def, pool, list.as_ref())
}

/// The whole error of a load that should have failed.
fn load_err(kind: &str, endpoint: &str, vars: &str, allowed: &str) -> anyhow::Error {
    match load(kind, endpoint, vars, allowed) {
        Ok(_) => panic!("load should have failed"),
        Err(e) => e,
    }
}

/// Where a secret file of these tests lives.
fn secret_dir() -> std::path::PathBuf {
    std::env::current_dir()
        .unwrap()
        .join("tmp/out/secret-endpoints")
}

/// One http server on loopback: it keeps the head of every request it is sent and answers
/// `status` with `body` (and `Connection: close`, so no connection is pooled).
async fn serve(status: u16, body: &str) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let got: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (status, body, sink) = (status, body.to_string(), got.clone());
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let (body, sink) = (body.clone(), sink.clone());
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&chunk[..n]),
                    }
                }
                // Read the body the client is still writing: answering while it has more to
                // send makes the client's write fail, and the test would see a connection
                // error where it wanted the status code.
                if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&head[..end]).into_owned();
                    let want = text
                        .lines()
                        .find_map(|l| {
                            let (n, v) = l.split_once(':')?;
                            if n.trim().eq_ignore_ascii_case("content-length") {
                                v.trim().parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0usize);
                    let mut have = head.len() - (end + 4);
                    while have < want {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => have += n,
                        }
                    }
                }
                sink.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&head).into_owned());
                let reply = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(reply.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    (addr, got)
}

/// An address nothing listens on.
async fn closed_port() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

/// The whole message of an error: the chain, as the log and `last_error` show it.
fn chain(e: &anyhow::Error) -> String {
    format!("{e:#}")
}

#[tokio::test]
async fn a_reference_endpoint_loads_and_shows_only_its_origin() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let dir = secret_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("teams"),
        format!("https://{HOST}/hooks/b4b0f4/MARKER-ignored\n"),
    )
    .unwrap();
    std::fs::write(dir.join("wrong"), "not a url\n").unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("https://{HOST}/services/T0000/B0000/{MARKER}"),
    );

    for endpoint in [
        "env:LWF_UNIT_WEBHOOK_URL".to_string(),
        format!("file:{}/teams", dir.display()),
    ] {
        let t = load("http", &endpoint, "{}", "").unwrap();
        let ctx = &t.endpoints[0].ctx;
        // The origin, and nothing below it.
        assert_eq!(ctx["host"], serde_json::json!(HOST));
        assert_eq!(ctx["port"], serde_json::json!(443));
        assert_eq!(ctx["url"], serde_json::json!(format!("https://{HOST}/…")));
        assert!(
            !serde_json::to_string(ctx).unwrap().contains(MARKER),
            "the template context holds the secret: {ctx}"
        );
        assert!(
            !serde_json::to_string(&t.ctx).unwrap().contains(MARKER),
            "the target context holds the secret"
        );
    }

    // The reference is what a `{:?}` shows, never the value it names.
    let def = TargetDef::from_row(&raw_target("http", "env:LWF_UNIT_WEBHOOK_URL", "{}")).unwrap();
    let shown = format!("{def:?}");
    assert!(shown.contains("env:LWF_UNIT_WEBHOOK_URL"), "{shown}");
    assert!(!shown.contains(MARKER), "{shown}");

    // A value that is not a plain http URL, or carries userinfo, is a load error that names
    // the reference and not the secret.
    std::env::set_var("LWF_UNIT_WEBHOOK_BAD", format!("ftp://{HOST}/x/{MARKER}"));
    let e = load_err("http", "env:LWF_UNIT_WEBHOOK_BAD", "{}", "");
    let msg = chain(&e);
    assert!(msg.contains("endpoint `env:LWF_UNIT_WEBHOOK_BAD`"), "{msg}");
    assert!(msg.contains("full http(s) URL"), "{msg}");
    assert!(!msg.contains(MARKER), "{msg}");
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_PW",
        format!("https://u:{MARKER}@{HOST}/x"),
    );
    let e = load_err("http", "env:LWF_UNIT_WEBHOOK_PW", "{}", "");
    let msg = chain(&e);
    // One fixed message for anything `BaseUrl::parse` refuses: its own words would
    // quote the authority it was given, which can be a piece of the secret.
    assert!(msg.contains("not valid"), "{msg}");
    assert!(!msg.contains(MARKER), "{msg}");
    let e = load_err("http", &format!("file:{}/wrong", dir.display()), "{}", "");
    assert!(!chain(&e).contains("not a url"), "{e:#}");
    // A reference that names nothing at all is a load error too.
    let e = load_err("http", "env:LWF_UNIT_WEBHOOK_MISSING", "{}", "");
    assert!(chain(&e).contains("LWF_UNIT_WEBHOOK_MISSING"), "{e:#}");
}

#[tokio::test]
async fn the_secret_path_reaches_the_wire_and_no_message() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("https://{HOST}/services/T0000/B0000/{MARKER}"),
    );
    let t = load("http", "env:LWF_UNIT_WEBHOOK_URL", "{}", "").unwrap();
    let rendered = "POST / HTTP/1.1\nContent-Type: application/json\n\n{\"text\":\"out of stock\"}";

    // Every message one of these requests can produce.
    let mut messages: Vec<String> = Vec::new();
    // A connection that is refused (transient), and an answer that is not a 2xx (refused).
    let closed = closed_port().await;
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("http://{closed}/services/T0000/B0000/{MARKER}"),
    );
    let e = t.send(0, rendered).await.unwrap_err();
    assert!(!is_rejected(&e), "a refused connection is transient");
    messages.push(chain(&e));
    let (addr, got) = serve(500, "the service is unwell").await;
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("http://{addr}/services/T0000/B0000/{MARKER}"),
    );
    let e = t.send(0, rendered).await.unwrap_err();
    assert!(!is_rejected(&e), "a 500 says not now, so it is transient");
    messages.push(chain(&e));
    // A refusal that is about this request: the webhook was revoked, say, or the payload is
    // not allowed. It is refused for good, and the message still stops at the origin.
    let (denied, _) = serve(403, "the webhook is no longer active").await;
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("http://{denied}/services/T0000/B0000/{MARKER}"),
    );
    let e = t.send(0, rendered).await.unwrap_err();
    assert!(is_rejected(&e), "a 403 is a refusal");
    messages.push(chain(&e));
    // A listing whose answer is not a 2xx.
    let e = t
        .fetch(0, "GET / HTTP/1.1\n\n", Uses::default())
        .await
        .unwrap_err();
    messages.push(chain(&e));
    // The endpoint is gone, and a reference that names nothing.
    messages.push(chain(&t.send(9, rendered).await.unwrap_err()));
    std::env::set_var("LWF_UNIT_WEBHOOK_URL", "");
    messages.push(chain(&t.send(0, rendered).await.unwrap_err()));

    for m in &messages {
        assert!(!m.contains(MARKER), "the secret is in a message: {m}");
    }
    // The origin is in them, so an operator can still tell which endpoint failed.
    assert!(
        messages[1].contains(&format!("http://{addr}/…")),
        "{}",
        messages[1]
    );
    assert!(messages[1].contains("POST"), "{}", messages[1]);
    assert!(messages[1].contains("status 500"), "{}", messages[1]);
    // And the request the wire actually carried holds the path, once, exactly.
    let sent = got.lock().unwrap().remove(0);
    assert!(
        sent.starts_with(&format!("POST /services/T0000/B0000/{MARKER} HTTP/1.1\r\n")),
        "{sent}"
    );
}

#[tokio::test]
async fn the_endpoint_s_path_and_query_are_the_request_line() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let (addr, got) = serve(200, "ok").await;

    // The webhook: the request line says `/`, the URL says where that is.
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("http://{addr}/services/T0000/B0000/{MARKER}"),
    );
    let t = load("http", "env:LWF_UNIT_WEBHOOK_URL", "{}", "").unwrap();
    t.send(0, "POST / HTTP/1.1\n\nhello").await.unwrap();
    // A query in the endpoint's URL, and a path below it.
    std::env::set_var(
        "LWF_UNIT_WEBHOOK_URL",
        format!("http://{addr}/v1/spaces/AAA?key={MARKER}&token=BBB"),
    );
    t.send(0, "POST / HTTP/1.1\n\nhello").await.unwrap();
    t.send(0, "GET /media/1.png HTTP/1.1\n\n").await.unwrap();
    // A request line that asks for no path stays what it was.
    std::env::set_var("LWF_UNIT_WEBHOOK_URL", format!("http://{addr}"));
    t.send(0, "OPTIONS * HTTP/1.1\n\n").await.unwrap();

    let lines: Vec<String> = got
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.lines().next().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        lines,
        vec![
            format!("POST /services/T0000/B0000/{MARKER} HTTP/1.1"),
            format!("POST /v1/spaces/AAA?key={MARKER}&token=BBB HTTP/1.1"),
            format!("GET /v1/spaces/AAA/media/1.png?key={MARKER}&token=BBB HTTP/1.1"),
            "OPTIONS * HTTP/1.1".to_string(),
        ],
        "{lines:#?}"
    );
    // `Host` comes from the resolved URL, and the body went with the request.
    let first = got.lock().unwrap()[0].clone();
    assert!(first.contains(&format!("Host: {addr}\r\n")), "{first}");
    assert!(first.ends_with("\r\n\r\nhello"), "{first}");

    // A plain endpoint is still sent exactly as it was before: the base path in front of
    // the rendered target, and `POST /` on a base of `/base/` keeps its trailing slash.
    let (addr, got) = serve(200, "ok").await;
    let t = load("http", &format!("http://{addr}/base/"), "{}", "").unwrap();
    t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap();
    t.send(0, "POST /idx/_bulk HTTP/1.1\n\ny").await.unwrap();
    let lines: Vec<String> = got
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.lines().next().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        lines,
        vec![
            "POST /base/ HTTP/1.1".to_string(),
            "POST /base/idx/_bulk HTTP/1.1".to_string(),
        ],
        "{lines:#?}"
    );
}

#[tokio::test]
async fn the_allow_list_checks_the_resolved_host() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let dir = secret_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    let file = format!("file:{}/rotate", dir.display());

    // An endpoint whose host is not allowed never loads.
    std::fs::write(dir.join("rotate"), "https://hooks.example.test/a/b\n").unwrap();
    let e = load_err("http", &file, "{}", "hooks.other.test");
    let msg = chain(&e);
    assert!(msg.contains("LAZYWF_ALLOWED_HOSTS"), "{msg}");
    assert!(msg.contains("hooks.example.test"), "{msg}");
    assert!(load("http", &file, "{}", "hooks.example.test").is_ok());

    // A file rotated to another host is caught on the request, refused rather than waited
    // out, and named by its host and the setting — not by the URL.
    let t = load("http", &file, "{}", "hooks.example.test").unwrap();
    std::fs::write(
        dir.join("rotate"),
        format!("https://evil.example.test/a/{MARKER}\n"),
    )
    .unwrap();
    let e = t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap_err();
    assert!(
        is_rejected(&e),
        "a host the operator did not allow is not retried"
    );
    let msg = chain(&e);
    assert!(msg.contains("LAZYWF_ALLOWED_HOSTS"), "{msg}");
    assert!(msg.contains("evil.example.test"), "{msg}");
    assert!(!msg.contains(MARKER), "{msg}");
}

#[tokio::test]
async fn a_reference_endpoint_is_for_the_http_kinds_only() {
    for kind in ["http", "http.basic", "http.bearer", "http.header"] {
        let vars = match kind {
            "http.basic" => r#"{"username":"u","password":"p"}"#,
            "http.bearer" => r#"{"token":"t"}"#,
            "http.header" => r#"{"header":"X-Key","value":"v"}"#,
            _ => "{}",
        };
        assert!(
            TargetDef::from_row(&raw_target(kind, "env:LWF_UNIT_ANY", vars)).is_ok(),
            "{kind}"
        );
    }
    let err = |kind: &str, vars: &str| {
        TargetDef::from_row(&raw_target(kind, "env:LWF_UNIT_ANY", vars))
            .unwrap_err()
            .to_string()
    };
    for (kind, vars) in [
        ("http.s3", r#"{"bucket":"b","region":"eu-west-1"}"#),
        ("redis", "{}"),
        ("sqs", "{}"),
        ("kinesis", "{}"),
        ("lambda", "{}"),
        ("eventbridge", "{}"),
        ("sns", "{}"),
        ("amqp", r#"{"username":"u","password":"p"}"#),
        ("smtp", r#"{"username":"u","password":"p"}"#),
        ("pubsub", r#"{"topic":"t"}"#),
        ("kafka", "{}"),
        ("mysql", r#"{"username":"u","password":"p"}"#),
        ("pgsql", r#"{"username":"u","password":"p"}"#),
    ] {
        let e = err(kind, vars);
        assert!(
            e.contains("endpoint reference") && e.contains("env:LWF_UNIT_ANY"),
            "{kind}: {e}"
        );
    }
}
