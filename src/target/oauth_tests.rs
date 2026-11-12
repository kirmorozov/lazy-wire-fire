use super::*;
use crate::model::{RawTarget, TargetDef};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The token the token endpoint hands out (the hit's number appended) and the client
/// secret. Neither may reach an error message, a log line or `last_error`.
const TOKEN: &str = "LWF-TOKEN-6f1a";
const SECRET: &str = "LWF-SECRET-2b9c";
/// What the token endpoint answers: a token named for the hit, so a reused token and a
/// refetched one are distinguishable in what the API server recorded.
const TOKEN_JSON: &str =
    r#"{"access_token": "LWF-TOKEN-6f1a-{n}", "token_type": "Bearer", "expires_in": 3600}"#;
/// One action of the target, as a rule's template renders it.
const ACTION: &str = "POST /hook HTTP/1.1\nContent-Type: application/json\n\n{\"k\":\"1\"}";

/// One plain http server of these tests: it keeps the bytes of every request it is sent and
/// answers what `answer` holds (`{n}` in it becomes the hit's number). `delay` holds the
/// answer back, which is how a single-flight test makes two sends overlap.
struct Server {
    addr: SocketAddr,
    hits: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<String>>>,
    answer: Arc<Mutex<(u16, String)>>,
    delay: Arc<Mutex<Duration>>,
    /// When set, the JWT bearer assertion of the form is verified on the spot (the way
    /// a real endpoint must): a bad grant, signature or claim answers `invalid_grant`.
    jwt: Arc<Mutex<Option<crate::oauth::jwt_check::Expect>>>,
}

impl Server {
    async fn start(status: u16, answer: &str) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let answer = Arc::new(Mutex::new((status, answer.to_string())));
        let delay = Arc::new(Mutex::new(Duration::ZERO));
        let jwt = Arc::new(Mutex::new(None));
        let (h, r, a, d, j) = (
            hits.clone(),
            requests.clone(),
            answer.clone(),
            delay.clone(),
            jwt.clone(),
        );
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let (h, r, a, d, j) = (h.clone(), r.clone(), a.clone(), d.clone(), j.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head = loop {
                        if let Some(e) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break e;
                        }
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => break buf.len(),
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    // The body is read before answering: answering while the client still
                    // has bytes to send makes its write fail, and the test would see a
                    // connection error where it wanted the answer.
                    let want = String::from_utf8_lossy(&buf[..head])
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
                    let mut have = buf.len().saturating_sub(head + 4);
                    while have < want {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => have += n,
                        }
                    }
                    let n = h.fetch_add(1, Ordering::SeqCst);
                    r.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&buf).into_owned());
                    // The endpoint reads the assertion the way Salesforce must: a form
                    // whose grant, signature or claims are wrong is refused.
                    let refused = match &*j.lock().unwrap() {
                        Some(want) => {
                            let body =
                                String::from_utf8_lossy(buf.get(head + 4..).unwrap_or_default())
                                    .into_owned();
                            crate::oauth::jwt_check::check_form(&body, want).err()
                        }
                        None => None,
                    };
                    let delay = *d.lock().unwrap();
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let (status, body) = match refused {
                            Some(why) => (
                                400u16,
                                format!(
                                    "{{\"error\": \"invalid_grant\", \"error_description\": \"{why}\"}}"
                                ),
                            ),
                            None => {
                                let (status, body) = a.lock().unwrap().clone();
                                (status, body.replace("{n}", &n.to_string()))
                            }
                        };
                    let reply = format!(
                            "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                    let _ = s.write_all(reply.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        Server {
            addr,
            hits,
            requests,
            answer,
            delay,
            jwt,
        }
    }

    /// Demand a verified JWT bearer assertion from now on.
    fn verify(&self, want: crate::oauth::jwt_check::Expect) {
        *self.jwt.lock().unwrap() = Some(want);
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    /// What to answer from now on.
    fn answer(&self, status: u16, body: &str) {
        *self.answer.lock().unwrap() = (status, body.to_string());
    }

    fn delay(&self, d: Duration) {
        *self.delay.lock().unwrap() = d;
    }

    /// The bytes of the most recent request (its request line, headers and body).
    fn last(&self) -> String {
        self.requests
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

/// Where a secret file of these tests lives (the `file:` form of `client_secret_ref`).
fn secret_dir() -> std::path::PathBuf {
    std::env::current_dir().unwrap().join("tmp/out/oauth")
}

/// One valid endpoint per kind these refusals are checked on.
fn endpoint(kind: &str) -> &'static str {
    match kind {
        "pubsub" => "gc-pubsub://p/t",
        "kafka" => "kafka://127.0.0.1:9092/t",
        "redis" => "redis://127.0.0.1:6379",
        _ => "http://api.example.test",
    }
}

/// The `vars` of a `client_credentials` target whose token endpoint is `token`.
fn cc_vars(token: &str) -> String {
    format!(
        r#"{{"oauth": "client_credentials", "token_url": "{token}", "client_id": "cid",
                 "client_secret_ref": "env:LWF_UNIT_OAUTH_SECRET"}}"#
    )
}

/// A plain http target with one endpoint, loaded the way the engine loads it. The pool is
/// the source database, which only `expect` ever asks (an unreachable address, so a target
/// with an `expect` says so loudly — which is itself an assertion below).
fn target(api: &str, vars: &str, expect: Option<&str>) -> Arc<Target> {
    let def = TargetDef::from_row(&RawTarget {
        name: "t-oauth".into(),
        kind: Some("http".into()),
        endpoints: Some(api.into()),
        tls: None,
        timeout_ms: Some(5000),
        expect: expect.map(str::to_string),
        vars: Some(vars.into()),
    })
    .unwrap();
    Target::new(
        def,
        mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x"),
        None,
    )
    .unwrap()
}

/// The whole message of an error: the chain, as the log and `last_error` show it.
fn chain(e: &anyhow::Error) -> String {
    format!("{e:#}")
}

/// An address nothing listens on.
async fn closed_port() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

#[tokio::test]
async fn every_action_carries_the_token_the_endpoint_issued_and_it_is_reused() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let token = Server::start(200, TOKEN_JSON).await;
    let api = Server::start(200, "ok").await;
    let t = target(
        &api.url(),
        &cc_vars(&format!("{}/token", token.url())),
        None,
    );

    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    // the action: the rendered request plus the one header `vars.oauth` adds
    let sent = api.last();
    assert!(sent.starts_with("POST /hook HTTP/1.1\r\n"), "{sent}");
    assert!(
        sent.contains(&format!("Authorization: Bearer {TOKEN}-0\r\n")),
        "{sent}"
    );
    assert!(
        sent.contains("Content-Type: application/json\r\n"),
        "{sent}"
    );
    assert!(sent.ends_with("{\"k\":\"1\"}"), "{sent}");
    // the token request: the grant's form, the client authenticated with Basic
    let tr = token.last();
    assert!(tr.starts_with("POST /token HTTP/1.1\r\n"), "{tr}");
    assert!(tr.contains("grant_type=client_credentials"), "{tr}");
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("cid:{SECRET}"));
    assert!(
        tr.contains(&format!("Authorization: Basic {basic}\r\n")),
        "{tr}"
    );
    assert_eq!(token.hits(), 1);

    // the next action is served from the cache: same token, no second token request
    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    assert_eq!(token.hits(), 1);
    assert!(
        api.last().contains(&format!("Bearer {TOKEN}-0")),
        "{}",
        api.last()
    );
}

#[tokio::test]
async fn two_actions_at_once_cost_one_token_request() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let token = Server::start(200, TOKEN_JSON).await;
    token.delay(Duration::from_millis(200));
    let api = Server::start(200, "ok").await;
    let t = target(
        &api.url(),
        &cc_vars(&format!("{}/token", token.url())),
        None,
    );

    let (a, b) = tokio::join!(t.send(0, ACTION), t.send(0, ACTION));
    assert_eq!(a.unwrap(), Sent::Ok);
    assert_eq!(b.unwrap(), Sent::Ok);
    assert_eq!(api.hits(), 2, "both actions went out");
    assert_eq!(
        token.hits(),
        1,
        "the second sender waited for the first's token"
    );
    assert!(api.last().contains(&format!("Bearer {TOKEN}-0")));
}

#[tokio::test]
async fn a_failing_token_endpoint_keeps_every_key_and_names_its_cause() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let api = Server::start(200, "ok").await;
    let token = Server::start(200, TOKEN_JSON).await;

    // every class of failure is transient: the endpoint is unwell or its answer unusable,
    // which says nothing about the message, so no key is refused for it.
    let closed = closed_port().await;
    let cases: Vec<(String, &str)> = vec![
        (
            format!("http://{closed}/token"),
            "the connection is refused",
        ),
        (format!("{}/token", token.url()), "the endpoint answers 500"),
        (format!("{}/token", token.url()), "the answer is not JSON"),
        (format!("{}/token", token.url()), "the answer has no token"),
    ];
    let answers = [
        (200u16, TOKEN_JSON.to_string()),
        (500, "server unwell".to_string()),
        (200, "<html>a login page</html>".to_string()),
        (200, r#"{"nope": true}"#.to_string()),
    ];
    for (i, (url, why)) in cases.into_iter().enumerate() {
        token.answer(answers[i].0, &answers[i].1);
        let t = target(&api.url(), &cc_vars(&url), None);
        let e = t.send(0, ACTION).await.unwrap_err();
        let msg = chain(&e);
        assert!(!is_rejected(&e), "{why}: refused a key for {msg}");
        assert!(msg.contains("token endpoint"), "{why}: {msg}");
        // the secret and the token stay where they belong
        assert!(!msg.contains(SECRET), "{msg}");
        assert!(!msg.contains(TOKEN), "{msg}");
    }
    assert_eq!(api.hits(), 0, "no action left the process");
}

#[tokio::test]
async fn an_endpoint_401_keeps_the_key_drops_the_token_and_is_confirmed_next_time() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let token = Server::start(200, TOKEN_JSON).await;
    // A 401 that echoes the token it refused: the message the operator sees must not carry
    // it.
    let api = Server::start(
        401,
        &format!(r#"{{"error": "invalid_token", "token": "{TOKEN}-0"}}"#),
    )
    .await;
    let t = target(
        &api.url(),
        &cc_vars(&format!("{}/token", token.url())),
        None,
    );

    let e = t.send(0, ACTION).await.unwrap_err();
    let msg = chain(&e);
    assert!(!is_rejected(&e), "a key was refused for {msg}");
    assert!(msg.contains("oauth access token"), "{msg}");
    assert!(!msg.contains(TOKEN), "the token escaped: {msg}");
    assert_eq!(token.hits(), 1, "the token the endpoint refused is dropped");

    // with a new token the same action goes through: the key was never in question
    api.answer(200, "ok");
    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    assert_eq!(token.hits(), 2, "one refetch");
    assert!(
        api.last().contains(&format!("Bearer {TOKEN}-1")),
        "{}",
        api.last()
    );
    // and the new token is then reused, as any token is
    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    assert_eq!(token.hits(), 2);
}

#[tokio::test]
async fn expect_is_never_asked_about_an_oauth_401() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let token = Server::start(200, TOKEN_JSON).await;
    let api = Server::start(401, "the token is not valid").await;
    // `expect` asks the source database about every answer, and that database is an
    // unreachable address here. Getting the token's own message instead — and not the
    // database's — says the 401 was decided about the credential before `expect` was
    // consulted at all: whatever the condition says, a 401 cannot refuse a key.
    let t = target(
        &api.url(),
        &cc_vars(&format!("{}/token", token.url())),
        Some("status = 200"),
    );
    let e = t.send(0, ACTION).await.unwrap_err();
    let msg = chain(&e);
    assert!(!is_rejected(&e), "a key was refused for {msg}");
    assert!(msg.contains("oauth access token"), "{msg}");
    assert!(!msg.contains("status 401"), "expect decided: {msg}");
    assert!(
        !msg.contains("Connection refused"),
        "the database was asked: {msg}"
    );
}

#[tokio::test]
async fn a_listing_that_is_handed_a_401_is_transient_too() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let token = Server::start(200, TOKEN_JSON).await;
    let api = Server::start(401, "the token is not valid").await;
    let t = target(
        &api.url(),
        &cc_vars(&format!("{}/token", token.url())),
        None,
    );
    let e = t
        .fetch(0, "GET /keys HTTP/1.1\n\n", Uses::default())
        .await
        .unwrap_err();
    let msg = chain(&e);
    assert!(msg.contains("oauth access token"), "{msg}");
    assert!(!msg.contains("status 401"), "{msg}");
    // the token is dropped for the next attempt
    api.answer(200, "[]");
    t.fetch(0, "GET /keys HTTP/1.1\n\n", Uses::default())
        .await
        .unwrap();
    assert_eq!(token.hits(), 2);
}

#[tokio::test]
async fn a_request_that_renders_its_own_authorization_is_refused_at_the_bind() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_UNIT_OAUTH_SECRET", SECRET);
    let token = Server::start(200, TOKEN_JSON).await;
    let api = Server::start(200, "ok").await;
    let t = target(
        &api.url(),
        &cc_vars(&format!("{}/token", token.url())),
        None,
    );

    // the header name a template produced by rendering: the load check cannot see it, so
    // the bind refuses the request. It is a mistake of the rule, not a word about the
    // key, so the key stays.
    let e = t
        .send(
            0,
            "POST /hook HTTP/1.1\nAuthorization: Bearer hand-made\n\nx",
        )
        .await
        .unwrap_err();
    let msg = chain(&e);
    assert!(!is_rejected(&e), "a key was refused for {msg}");
    assert!(msg.contains("`vars.oauth=client_credentials`"), "{msg}");
    assert!(msg.contains("rule template"), "{msg}");
    assert_eq!(api.hits(), 0, "nothing was sent");
    assert_eq!(token.hits(), 0, "not even a token was fetched");

    // a header that is not `Authorization` is none of the grant's business
    assert_eq!(
        t.send(
            0,
            "POST /hook HTTP/1.1\nAuthorization-Kind: bearer\nX-Trace: 1\n\nx"
        )
        .await
        .unwrap(),
        Sent::Ok
    );
    assert!(api.last().contains("Authorization-Kind: bearer"));
    assert!(api.last().contains(&format!("Bearer {TOKEN}-0")));
}

#[test]
fn a_foreign_kind_refuses_the_oauth_settings_and_pubsub_keeps_its_own() {
    let err = |kind: &str, vars: &str| {
        format!(
            "{:#}",
            TargetDef::from_row(&RawTarget {
                name: "t".into(),
                kind: Some(kind.into()),
                endpoints: Some(endpoint(kind).into()),
                vars: Some(vars.into()),
                ..Default::default()
            })
            .unwrap_err()
        )
    };
    let e = err("redis", r#"{"oauth": "google"}"#);
    assert!(e.contains("`vars.oauth` is not used by kind redis"), "{e}");
    let e = err("kafka", r#"{"scope": "s"}"#);
    assert!(e.contains("`vars.scope` is not used by kind kafka"), "{e}");
    let e = err(
        "pubsub",
        r#"{"oauth": "client_credentials", "token_url": "https://a.test/t", "client_id": "c", "client_secret_ref": "env:S"}"#,
    );
    assert!(e.contains("`vars.oauth` is not used by kind pubsub"), "{e}");
    // the setting without the grant name the grant
    let e = err("http", r#"{"token_url": "https://a.test/token"}"#);
    assert!(e.contains("needs `vars.oauth`"), "{e}");
    // two claims in one request are refused, both named
    let e = err("http.bearer", r#"{"oauth": "google", "token": "env:T"}"#);
    assert!(e.contains("`vars.oauth`"), "{e}");
    assert!(e.contains("`vars.token`"), "{e}");

    // `credentials_ref` is pubsub's own setting: its use of it is not an oauth setting,
    // and this target must keep loading.
    TargetDef::from_row(&RawTarget {
        name: "t".into(),
        kind: Some("pubsub".into()),
        endpoints: Some(endpoint("pubsub").into()),
        vars: Some(r#"{"credentials_ref": "file:/keys/sa.json"}"#.into()),
        ..Default::default()
    })
    .unwrap();
}

#[tokio::test]
async fn the_secret_and_the_token_never_reach_the_target_definition_or_its_context() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let dir = secret_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("client"), format!("{SECRET}\n")).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    let token = Server::start(200, TOKEN_JSON).await;
    let api = Server::start(200, "ok").await;
    let vars = format!(
        r#"{{"oauth": "client_credentials", "token_url": "{}/token", "client_id": "cid",
                 "client_secret_ref": "file:{}/client"}}"#,
        token.url(),
        dir.display()
    );
    let def = TargetDef::from_row(&RawTarget {
        name: "t-oauth".into(),
        kind: Some("http".into()),
        endpoints: Some(api.url()),
        timeout_ms: Some(5000),
        vars: Some(vars),
        ..Default::default()
    })
    .unwrap();
    // the definition shows the names of its credential settings, never their bytes: the
    // reference and the value it names are both absent
    let shown = format!("{def:?}");
    assert!(shown.contains("client_secret_ref"), "{shown}");
    assert!(!shown.contains(SECRET), "{shown}");
    assert!(!shown.contains("file:"), "{shown}");
    let ctx = json!({"name": def.name, "vars": def.template_vars()});
    assert!(
        !serde_json::to_string(&ctx).unwrap().contains(SECRET),
        "{ctx}"
    );

    let t = Target::new(
        def,
        mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x"),
        None,
    )
    .unwrap();
    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    // the file reference was read: the endpoint saw those bytes as the client secret
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("cid:{SECRET}"));
    assert!(
        token
            .last()
            .contains(&format!("Authorization: Basic {basic}\r\n")),
        "{}",
        token.last()
    );
    std::fs::remove_dir_all(&dir).ok();
}

// ---- vars.oauth=jwt_bearer (RFC 7523 §2.1, Salesforce) --------------------------

/// The JWT claims of these tests: `iss` is Salesforce's consumer key, `sub` the user
/// the token acts for, `aud` the org's login host.
const JWT_ISS: &str = "issuer@company.test";
const JWT_SUB: &str = "integration@company.test";
const JWT_AUD: &str = "https://company.my.salesforce.com";

/// The `vars` of a jwt_bearer target whose token endpoint is `token` (the key behind
/// an `env:` reference; the unit tests of `src/oauth.rs` walk both reference forms).
fn jwt_vars(token: &str) -> String {
    format!(
        r#"{{"oauth": "jwt_bearer", "token_url": "{token}", "client_id": "{JWT_ISS}",
                 "subject": "{JWT_SUB}", "audience": "{JWT_AUD}",
                 "signing_key_ref": "env:LWF_UNIT_OAUTH_JWT_KEY"}}"#
    )
}

#[tokio::test]
async fn the_jwt_bearer_action_carries_the_token_and_the_endpoint_verifies_the_assertion() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let key = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/testdata/rsa-test-key-pkcs8.pem"
    ));
    std::env::set_var(
        "LWF_UNIT_OAUTH_JWT_KEY",
        String::from_utf8_lossy(key).into_owned(),
    );
    // the token endpoint verifies every assertion it is sent (grant, RS256 signature
    // with the public key, claims): a wrong one answers `invalid_grant` and this test
    // fails on the send.
    let token = Server::start(200, TOKEN_JSON).await;
    token.verify(crate::oauth::jwt_check::Expect {
        public_key: crate::oauth::jwt_check::test_public_key(key),
        iss: JWT_ISS.into(),
        sub: JWT_SUB.into(),
        aud: JWT_AUD.into(),
        scope: None,
    });
    let api = Server::start(200, "ok").await;
    let t = target(
        &api.url(),
        &jwt_vars(&format!("{}/token", token.url())),
        None,
    );

    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    let sent = api.last();
    assert!(
        sent.contains(&format!("Authorization: Bearer {TOKEN}-0\r\n")),
        "{sent}"
    );
    // the token request: the §2.1 form, and no client authentication header
    let tr = token.last();
    assert!(tr.starts_with("POST /token HTTP/1.1\r\n"), "{tr}");
    assert!(
        tr.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="),
        "{tr}"
    );
    assert!(
        !tr.contains("Authorization:"),
        "a client-auth header rides an assertion grant: {tr}"
    );
    assert_eq!(token.hits(), 1);
    // the next action is served from the cache: same token, no second token request
    assert_eq!(t.send(0, ACTION).await.unwrap(), Sent::Ok);
    assert_eq!(token.hits(), 1);
    assert!(
        api.last().contains(&format!("Bearer {TOKEN}-0")),
        "{}",
        api.last()
    );
}

#[tokio::test]
async fn every_jwt_token_endpoint_failure_keeps_the_key_and_names_the_cause() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let key = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/testdata/rsa-test-key-pkcs8.pem"
    ));
    std::env::set_var(
        "LWF_UNIT_OAUTH_JWT_KEY",
        String::from_utf8_lossy(key).into_owned(),
    );
    let api = Server::start(200, "ok").await;

    // the token endpoint is a closed port: a transport cause, and the key waits
    let closed = closed_port().await;
    let t = target(
        &api.url(),
        &jwt_vars(&format!("http://{closed}/token")),
        None,
    );
    let e = t.send(0, ACTION).await.unwrap_err();
    assert!(!is_rejected(&e), "the token failure refused the key: {e:#}");
    let msg = chain(&e);
    assert!(msg.contains("token endpoint POST"), "{msg}");
    assert!(msg.contains("refused"), "{msg}");
    assert_eq!(api.hits(), 0, "an action left the process without a token");

    // an endpoint that refuses the assertion: transient, its cause named
    let bad = Server::start(
        400,
        r#"{"error": "invalid_grant", "error_description": "assertion expired"}"#,
    )
    .await;
    let t = target(&api.url(), &jwt_vars(&format!("{}/token", bad.url())), None);
    let e = t.send(0, ACTION).await.unwrap_err();
    assert!(!is_rejected(&e), "the endpoint refused the key: {e:#}");
    let msg = chain(&e);
    assert!(msg.contains("status 400"), "{msg}");
    assert!(msg.contains("invalid_grant"), "{msg}");
    assert!(msg.contains("assertion expired"), "{msg}");
    assert_eq!(api.hits(), 0);

    // a key that cannot sign: transient too, naming the key's cause
    std::env::set_var("LWF_UNIT_OAUTH_JWT_KEY", "not a key at all");
    let t = target(&api.url(), &jwt_vars(&format!("{}/token", bad.url())), None);
    let e = t.send(0, ACTION).await.unwrap_err();
    assert!(!is_rejected(&e), "the key error refused the key: {e:#}");
    assert!(chain(&e).contains("not a PEM private key"), "{}", chain(&e));
    assert_eq!(api.hits(), 0);
}
