use super::*;
use crate::model::ResetStep;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A client secret with a character that percent-encodes: a scrub that only knows the
/// literal form would let the encoded one out.
const SECRET: &str = "sup3r/secret";

/// No form a secret was sent in may appear in an error text, a log line or `/status`.
fn assert_carries_no_secret(s: &str) {
    assert!(!s.contains(SECRET), "{s}");
    assert!(!s.contains(&form_encode(SECRET)), "{s}");
    // both the raw pair and the §2.3.1 form: the encoded pair is what the wire carries
    assert!(
        !s.contains(&base64::engine::general_purpose::STANDARD.encode(format!("cid:{SECRET}"))),
        "{s}"
    );
    assert!(
        !s.contains(
            &base64::engine::general_purpose::STANDARD
                .encode(format!("cid:{}", form_encode(SECRET)))
        ),
        "{s}"
    );
}

fn vars(v: &str) -> Map<String, Value> {
    serde_json::from_str(v).unwrap()
}

const CC: &str = r#"{"oauth": "client_credentials", "token_url": "https://auth.test/token",
        "client_id": "cid", "client_secret_ref": "env:LWF_OAUTH_TEST_SECRET"}"#;

#[test]
fn the_vars_parse_and_every_load_refusal_names_its_setting() {
    // accepted forms
    assert!(check(Kind::Http, &vars(CC)).is_ok());
    assert!(check(
        Kind::Http,
        &vars(r#"{"oauth": "google", "scope": "https://www.googleapis.com/auth/spreadsheets"}"#)
    )
    .is_ok());
    assert!(check(
        Kind::Http,
        &vars(r#"{"oauth": "google", "credentials_ref": "file:/keys/sa.json"}"#)
    )
    .is_ok());
    assert!(check(
        Kind::HttpHeader,
        &vars(
            r#"{"oauth": "client_credentials", "token_url": "https://a.test/t",
                     "client_id": "cid", "client_secret_ref": "file:/etc/secret",
                     "header": "X-Api-Key", "value": "env:V", "scope": "s"}"#
        )
    )
    .is_ok());
    // plain http on loopback is accepted (the test servers, a sidecar broker)
    assert!(check(
        Kind::Http,
        &vars(&CC.replace("https://auth.test/token", "http://127.0.0.1:9/token"))
    )
    .is_ok());
    assert!(check(
        Kind::Http,
        &vars(&CC.replace("https://auth.test/token", "http://localhost:9/token"))
    )
    .is_ok());

    let err = |kind: Kind, v: &str| format!("{:#}", check(kind, &vars(v)).unwrap_err());
    let unknown = err(
        Kind::Http,
        r#"{"oauth": "implicit", "token_url": "https://a.test/t"}"#,
    );
    assert!(unknown.contains("`vars.oauth` is `implicit`"), "{unknown}");
    assert!(
        unknown.contains("use google, client_credentials or jwt_bearer"),
        "{unknown}"
    );
    let missing = err(Kind::Http, r#"{"oauth": "client_credentials"}"#);
    assert!(missing.contains("`vars.token_url`"), "{missing}");
    let missing = err(
        Kind::Http,
        r#"{"oauth": "client_credentials", "token_url": "https://a.test/t"}"#,
    );
    assert!(missing.contains("`vars.client_id`"), "{missing}");
    let missing = err(
        Kind::Http,
        r#"{"oauth": "client_credentials", "token_url": "https://a.test/t", "client_id": "c"}"#,
    );
    assert!(missing.contains("`vars.client_secret_ref`"), "{missing}");
    // a secret inline instead of by reference
    let inline = err(
        Kind::Http,
        r#"{"oauth": "client_credentials", "token_url": "https://a.test/t",
                "client_id": "c", "client_secret_ref": "hunter2"}"#,
    );
    assert!(
        inline.contains("must be a reference (`env:NAME` or `file:/path`), never a literal"),
        "{inline}"
    );
    let inline = err(
        Kind::Http,
        r#"{"oauth": "google", "credentials_ref": "{...key...}"}"#,
    );
    assert!(
        inline.contains("`vars.credentials_ref` must be a reference"),
        "{inline}"
    );
    // the token URL: not https off loopback, userinfo refused
    let http = err(
        Kind::Http,
        &CC.replace("https://auth.test/token", "http://auth.test/token"),
    );
    assert!(http.contains("https is required"), "{http}");
    assert!(http.contains("only on loopback"), "{http}");
    let userinfo = err(
        Kind::Http,
        &CC.replace("https://auth.test/token", "https://u:pw@auth.test/token"),
    );
    assert!(userinfo.contains("vars.token_url"), "{userinfo}");
    // the ambiguities: two sources of Authorization, both named
    let basic = err(Kind::HttpBasic, CC);
    assert!(basic.contains("`vars.oauth`"), "{basic}");
    assert!(
        basic.contains("`vars.username` and `vars.password`"),
        "{basic}"
    );
    let bearer = err(Kind::HttpBearer, CC);
    assert!(bearer.contains("`vars.token`"), "{bearer}");
    let header = err(
        Kind::HttpHeader,
        r#"{"oauth": "client_credentials", "token_url": "https://a.test/t",
                "client_id": "c", "client_secret_ref": "env:S", "header": "Authorization",
                "value": "env:V"}"#,
    );
    assert!(header.contains("`vars.header`"), "{header}");
    // a grant's var the other grant does not use
    let stray = err(
        Kind::Http,
        r#"{"oauth": "google", "token_url": "https://a.test/t"}"#,
    );
    assert!(stray.contains("`vars.token_url` is not used"), "{stray}");
    let stray = err(
        Kind::Http,
        r#"{"oauth": "client_credentials", "token_url": "https://a.test/t",
                "client_id": "c", "client_secret_ref": "env:S", "credentials_ref": "env:K"}"#,
    );
    assert!(
        stray.contains("`vars.credentials_ref` is not used"),
        "{stray}"
    );
    // settings without the grant name the grant
    let dangling = err(Kind::Http, r#"{"token_url": "https://a.test/t"}"#);
    assert!(
        dangling.contains("needs `vars.oauth` (google, client_credentials or jwt_bearer)"),
        "{dangling}"
    );
    assert!(check(Kind::Http, &vars("{}")).is_ok()); // no oauth at all: untouched
}

#[test]
fn first_var_names_the_first_setting_a_foreign_kind_carries() {
    assert_eq!(
        first_var(&vars(r#"{"scope": "s"}"#), Kind::Kafka),
        Some("scope")
    );
    assert_eq!(
        first_var(&vars(r#"{"oauth": "google"}"#), Kind::Amqp),
        Some("oauth")
    );
    // `pubsub` owns its `credentials_ref`: naming it would refuse a working target.
    assert_eq!(
        first_var(
            &vars(r#"{"credentials_ref": "file:/k.json"}"#),
            Kind::PubSub
        ),
        None
    );
    assert_eq!(
        first_var(
            &vars(r#"{"credentials_ref": "file:/k.json"}"#),
            Kind::HttpS3
        ),
        Some("credentials_ref")
    );
}

#[test]
fn the_load_check_reads_the_header_block_only_and_the_token_url_shape() {
    // the request line's header block carries a literal `Authorization:`
    assert!(template_sets_authorization(
        "POST /x HTTP/1.1\nAuthorization: Bearer abc\n\nbody"
    ));
    assert!(template_sets_authorization(
        "POST /x HTTP/1.1\r\nauthorization:  x  \r\n\r\nbody"
    ));
    // a body naming Authorization is none of its business; a header name produced by
    // rendering is the bind-time catch, not the load check's
    assert!(!template_sets_authorization(
        "POST /x HTTP/1.1\n\n{\"Authorization\": 1}"
    ));
    assert!(!template_sets_authorization(
        "POST /x HTTP/1.1\nX-Tag: a\n\nAuthorization: x"
    ));
    assert!(!template_sets_authorization(
        "POST /x HTTP/1.1\n{{name}}: v\n\nbody"
    ));
    // token URL shapes
    assert!(parse_token_url("https://a.test/token").is_ok());
    assert!(parse_token_url("http://127.0.0.1:8080/token").is_ok());
    assert!(parse_token_url("http://[::1]:8080/token").is_ok());
    assert!(parse_token_url("http://a.test/token").is_err());
    assert!(parse_token_url("ftp://a.test/token").is_err());
    assert!(parse_token_url("https://u:pw@a.test/token").is_err());
    assert!(parse_token_url("").is_err());
    assert!(!is_loopback("10.0.0.1"));
    assert!(is_loopback("LOCALHOST"));
    assert!(is_loopback("::1"));
}

/// The form of a `usable` lifetime: the margin, halved for short lifetimes.
#[test]
fn a_token_is_replaced_before_its_lifetime_ends() {
    let usable = |life: Duration| {
        life.checked_sub(REFRESH_BEFORE.min(life / 2))
            .unwrap_or_default()
    };
    assert_eq!(usable(Duration::from_secs(3600)), Duration::from_secs(3540));
    assert_eq!(usable(Duration::from_secs(90)), Duration::from_secs(45));
    assert_eq!(usable(Duration::from_secs(1)), Duration::from_millis(500));
    assert_eq!(usable(Duration::ZERO), Duration::ZERO);
}

struct TokenServer {
    base: String,
    hits: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<String>>>,
    answer: Arc<Mutex<(u16, String)>>,
    delay: Arc<Mutex<Duration>>,
    /// When set, the JWT bearer assertion of the form is verified on the spot (the way
    /// a real endpoint must): a bad grant, signature or claim answers `invalid_grant`.
    jwt: Arc<Mutex<Option<jwt_check::Expect>>>,
}

impl TokenServer {
    /// The token endpoint of the tests: it records every request (its full bytes) and
    /// answers what `answer` holds. A new token value per hit, so a reused token is visible.
    async fn start() -> TokenServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let answer = Arc::new(Mutex::new((
            200u16,
            r#"{"access_token": "{n}", "token_type": "Bearer", "expires_in": 3600}"#.to_string(),
        )));
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
                let (mut s, _) = listener.accept().await.unwrap();
                let (h, r, a, d, j) = (h.clone(), r.clone(), a.clone(), d.clone(), j.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut byte = [0u8; 1];
                    while !buf.ends_with(b"\r\n\r\n") {
                        if s.read(&mut byte).await.unwrap() == 0 {
                            break;
                        }
                        buf.push(byte[0]);
                    }
                    let head = String::from_utf8_lossy(&buf).to_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            l.strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse().ok())
                        })
                        .unwrap_or(0);
                    let mut body = vec![0u8; len];
                    s.read_exact(&mut body).await.unwrap();
                    let n = h.fetch_add(1, Ordering::SeqCst);
                    r.lock().unwrap().push(format!(
                        "{}{}",
                        String::from_utf8_lossy(&buf),
                        String::from_utf8_lossy(&body)
                    ));
                    // The endpoint reads the assertion the way Salesforce must: a form
                    // whose grant, signature or claims are wrong is refused.
                    let refused = match &*j.lock().unwrap() {
                        Some(want) => {
                            jwt_check::check_form(&String::from_utf8_lossy(&body), want).err()
                        }
                        None => None,
                    };
                    let (delay, (status, answer)) = (*d.lock().unwrap(), a.lock().unwrap().clone());
                    let (status, answer) = match refused {
                            Some(why) => (
                                400u16,
                                format!(
                                    "{{\"error\": \"invalid_grant\", \"error_description\": \"{why}\"}}"
                                ),
                            ),
                            None => (status, answer),
                        };
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    // The token names the hit: two sends on one token are distinguishable.
                    let answer = answer.replace("{n}", &format!("tok-{n}"));
                    let resp = format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                            answer.len()
                        );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        TokenServer {
            base: format!("http://{addr}/token"),
            hits,
            requests,
            answer,
            delay,
            jwt,
        }
    }

    /// Demand a verified JWT bearer assertion from now on.
    fn verify(&self, want: jwt_check::Expect) {
        *self.jwt.lock().unwrap() = Some(want);
    }

    fn set_answer(&self, status: u16, body: &str) {
        *self.answer.lock().unwrap() = (status, body.to_string());
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    fn last(&self) -> String {
        self.requests
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

fn cc_provider(server: &TokenServer, scope: Option<&str>) -> Provider {
    let mut v = serde_json::json!({
        "oauth": "client_credentials",
        "token_url": server.base,
        "client_id": "cid",
        "client_secret_ref": "env:LWF_OAUTH_TEST_SECRET",
    });
    if let Some(s) = scope {
        v["scope"] = serde_json::Value::String(s.to_string());
    }
    let def = target_def(v);
    check(Kind::Http, &def.vars).unwrap(); // the load check accepts what is about to run
    Provider::new(&def, None).unwrap().unwrap()
}

/// A plain http target definition carrying `vars` (the shape every oauth target has).
fn target_def(vars: Value) -> TargetDef {
    TargetDef {
        name: "t-oauth".into(),
        kind: Kind::Http,
        endpoints: vec!["http://api.test".into()],
        tls: TlsSpec::parse(None).unwrap(),
        timeout: Duration::from_secs(5),
        expect: None,
        vars: vars.as_object().unwrap().clone(),
    }
}

#[tokio::test]
async fn the_token_request_is_the_form_and_basic_the_spec_wants() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "s3cr/t");
    let server = TokenServer::start().await;
    let p = cc_provider(&server, Some("https://graph.microsoft.com/.default"));
    let header = p.authorization().await.unwrap();
    assert_eq!(header, "Bearer tok-0");
    let want = base64::engine::general_purpose::STANDARD.encode("cid:s3cr%2Ft");
    let last = server.last();
    assert!(last.starts_with("POST /token HTTP/1.1\r\n"), "{last}");
    assert!(last.contains("Host: 127.0.0.1"), "{last}");
    assert!(
        last.contains(&format!("Authorization: Basic {want}\r\n")),
        "{last}"
    );
    assert!(
        last.contains("Content-Type: application/x-www-form-urlencoded\r\n"),
        "{last}"
    );
    assert!(last.contains("Accept: application/json\r\n"), "{last}");
    assert!(last.contains("User-Agent: lazywf/"), "{last}");
    // the body: the grant, and the scope percent-encoded as one form field
    assert!(
        last.ends_with(
            "grant_type=client_credentials&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default"
        ),
        "{last}"
    );
    assert_eq!(server.hits(), 1);
}

/// RFC 6749 §2.3.1: each credential is form-encoded BEFORE the colon joins them and
/// Base64 covers the pair. A secret full of the characters that encode (`:`, `+`, `%`,
/// a space) must reach the endpoint as `Basic base64("cid:s3%3A%2B+%25cr")`, never as the
/// literal bytes (a literal `:` would split the pair) and never double-encoded.
#[tokio::test]
async fn the_basic_credential_form_encodes_the_secret_first() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let nasty = "s3:+ %cr";
    std::env::set_var("LWF_OAUTH_TEST_SECRET", nasty);
    let server = TokenServer::start().await;
    let p = cc_provider(&server, None);
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    let want = base64::engine::general_purpose::STANDARD.encode("cid:s3%3A%2B+%25cr");
    let last = server.last();
    assert!(
        last.contains(&format!("Authorization: Basic {want}\r\n")),
        "{last}"
    );
    // the literal secret appears nowhere on the wire, its Base64 included
    assert!(!last.contains(nasty), "{last}");
    assert!(
        !last.contains(&base64::engine::general_purpose::STANDARD.encode(format!("cid:{nasty}"))),
        "the un-encoded pair went out: {last}"
    );
}

#[tokio::test]
async fn the_token_is_reused_and_due_again() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "s3cret");
    let server = TokenServer::start().await;
    let p = cc_provider(&server, None);
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0"); // the same token, no hit
    assert_eq!(server.hits(), 1);
    // due: the entry's `until` in the past, as `REFRESH_BEFORE` does an hour from now
    {
        let cc = match &p.grant {
            Grant::ClientCredentials(c) => c,
            _ => unreachable!(),
        };
        cc.lock().as_mut().unwrap().until = Instant::now() - Duration::from_millis(1);
    }
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-1");
    assert_eq!(server.hits(), 2);
    // `invalidate` (the 401 hook) drops it once: one refetch, then reuse again
    p.invalidate();
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-2");
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-2");
    assert_eq!(server.hits(), 3);
}

#[tokio::test]
async fn two_concurrent_sends_cost_one_token_request() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "s3cret");
    let server = TokenServer::start().await;
    *server.delay.lock().unwrap() = Duration::from_millis(200);
    let p = cc_provider(&server, None);
    let (a, b) = tokio::join!(p.authorization(), p.authorization());
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(server.hits(), 1, "single-flight: one fetch, both share it");
}

#[tokio::test]
async fn a_changed_secret_behind_the_reference_fetches_a_new_token() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "first");
    let server = TokenServer::start().await;
    let p = cc_provider(&server, None);
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "second");
    assert_eq!(
        p.authorization().await.unwrap(),
        "Bearer tok-1",
        "new bytes behind the reference drop the entry at once"
    );
    assert_eq!(server.hits(), 2);
    // back to the old bytes: still not the entry the second secret produced
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "first");
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-2");
}

#[tokio::test]
async fn the_client_id_and_secret_are_read_from_references_of_both_kinds() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let base = std::env::temp_dir().join(format!("lazywf-oauth-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    std::fs::write(base.join("secret"), "file-s3cret\n").unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", base.display().to_string());
    std::env::set_var("LWF_OAUTH_TEST_ID", "env-cid");
    let server = TokenServer::start().await;
    let vars = serde_json::json!({
        "oauth": "client_credentials",
        "token_url": server.base,
        "client_id": "env:LWF_OAUTH_TEST_ID",
        "client_secret_ref": format!("file:{}/secret", base.display()),
    });
    let p = Provider::new(&target_def(vars), None).unwrap().unwrap();
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    // the Basic credential carries what the two references held, trailing newline dropped
    let want = base64::engine::general_purpose::STANDARD.encode("env-cid:file-s3cret");
    assert!(
        server.last().contains(&format!("Basic {want}\r\n")),
        "{}",
        server.last()
    );
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn every_token_endpoint_failure_class_is_transient_with_its_cause() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_OAUTH_TEST_SECRET", SECRET);
    let provider = |url: String| {
        let vars = serde_json::json!({
            "oauth": "client_credentials", "token_url": url,
            "client_id": "cid", "client_secret_ref": "env:LWF_OAUTH_TEST_SECRET",
        });
        Provider::new(&target_def(vars), None).unwrap().unwrap()
    };

    // a closed port: the cause is the connection, and the address is safe to name
    let closed = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        format!("http://{a}/token")
    };
    let e = format!("{:#}", provider(closed).authorization().await.unwrap_err());
    assert!(e.contains("token endpoint POST"), "{e}");
    assert!(e.contains("refused"), "{e}");
    assert_carries_no_secret(&e);

    let server = TokenServer::start().await;
    let p = provider(server.base.clone());
    // every class names what went wrong: the status, or what the answer lacked
    let cases: Vec<(u16, &str, &[&str])> = vec![
        (
            400,
            r#"{"error": "invalid_client"}"#,
            &["status 400", "invalid_client"],
        ),
        (
            401,
            r#"{"error": "invalid_client", "error_description": "bad creds"}"#,
            &["status 401", "invalid_client", "bad creds"],
        ),
        (
            403,
            r#"{"error": "access_denied"}"#,
            &["status 403", "access_denied"],
        ),
        (500, "boom", &["status 500", "boom"]),
        (200, "<html>login</html>", &["not JSON", "login"]),
        (200, r#"{"token": "nope"}"#, &["no access_token"]),
        (
            200,
            r#"{"access_token": "", "expires_in": 1}"#,
            &["no access_token"],
        ),
        (
            200,
            r#"{"access_token": "t", "token_type": "mac"}"#,
            &["token_type `mac`"],
        ),
        (
            200,
            r#"{"access_token": "t", "expires_in": "soon"}"#,
            &["expires_in"],
        ),
        (
            200,
            r#"{"access_token": "t", "expires_in": -7}"#,
            &["expires_in"],
        ),
    ];
    for (status, body, wants) in cases {
        server.set_answer(status, body);
        let e = format!("{:#}", p.authorization().await.unwrap_err());
        for w in wants {
            assert!(e.contains(w), "`{body}` answered as `{e}`");
        }
        // transient means the message never wraps a refusal, and names nothing secret
        assert!(!e.contains("refused by the rule"), "{e}");
        assert_carries_no_secret(&e);
    }

    // an echoed `error_description` is scrubbed of the secret and clipped to one line
    let long = "x".repeat(300);
    server.set_answer(
            400,
            &format!(
                r#"{{"error": "invalid_request", "error_description": "got {SECRET} and {long}\nsecond line"}}"#
            ),
        );
    let e = format!("{:#}", p.authorization().await.unwrap_err());
    assert!(e.contains("invalid_request"), "{e}");
    assert!(!e.contains(&"x".repeat(ECHO_MAX + 1)), "not clipped: {e}");
    assert!(!e.contains("second line"), "{e}");
    assert_carries_no_secret(&e);

    // an endpoint that answers form-encoded, as some do: not JSON, and it carried a token
    server.set_answer(200, "access_token=S3cr3t.T0ken.Value&expires_in=3600");
    let e = format!("{:#}", p.authorization().await.unwrap_err());
    assert!(e.contains("not JSON"), "{e}");
    assert!(!e.contains("S3cr3t.T0ken.Value"), "{e}");

    // an endpoint that echoes the whole request cannot carry the secret out
    let echo = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                if s.read(&mut byte).await.unwrap() == 0 {
                    break;
                }
                buf.push(byte[0]);
            }
            let head = String::from_utf8_lossy(&buf).to_lowercase();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse().ok())
                })
                .unwrap_or(0);
            let mut body = vec![0u8; len];
            s.read_exact(&mut body).await.unwrap();
            let text = format!(
                "HTTP/1.1 400 X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}{}",
                buf.len() + body.len(),
                String::from_utf8_lossy(&buf),
                String::from_utf8_lossy(&body)
            );
            let _ = s.write_all(text.as_bytes()).await;
        });
        a
    };
    let e = format!(
        "{:#}",
        provider(format!("http://{echo}/token"))
            .authorization()
            .await
            .unwrap_err()
    );
    assert!(e.contains("status 400"), "{e}");
    assert_carries_no_secret(&e);
}
#[tokio::test]
async fn the_access_token_stays_out_of_messages_and_debug() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_OAUTH_TEST_SECRET", "topsecret");
    let server = TokenServer::start().await;
    server.set_answer(
        200,
        r#"{"access_token": "LWF-TOKEN-MARKER-9e2", "expires_in": 3600}"#,
    );
    let p = cc_provider(&server, None);
    let header = p.authorization().await.unwrap();
    assert_eq!(header, "Bearer LWF-TOKEN-MARKER-9e2");
    // the provider's Debug shows the grant and the URL, never the token or the secret
    let d = format!("{p:?}");
    assert!(!d.contains("LWF-TOKEN-MARKER-9e2"), "{d}");
    assert!(!d.contains("topsecret"), "{d}");
    assert!(d.contains("client_credentials"), "{d}");
    // an endpoint whose 400 body echoes the request: the scrub masks the cached token
    let scrubbed = p.scrub("Authorization: Bearer LWF-TOKEN-MARKER-9e2 seen");
    assert!(!scrubbed.contains("LWF-TOKEN-MARKER-9e2"), "{scrubbed}");
    // every failure message too: a token the endpoint answered never appears
    server.set_answer(
        500,
        r#"{"error_description": "internal LWF-TOKEN-MARKER-9e2 leak"}"#,
    );
    p.invalidate();
    let e = format!("{:#}", p.authorization().await.unwrap_err());
    assert!(!e.contains("LWF-TOKEN-MARKER-9e2"), "{e}");
}

#[test]
fn the_rule_template_refusals_name_the_template_and_the_grant() {
    let def = |template: &str| RuleDef {
        name: "r".into(),
        enabled: true,
        target: "t".into(),
        selects: vec![],
        snapshot_keys_sql: None,
        template: template.into(),
        fire_per: crate::model::FirePer::Key,
        batch_max_keys: None,
        batch_max_bytes: None,
        debounce: Duration::ZERO,
        max_delay: Duration::ZERO,
        reset_steps: vec![],
        reset_requested: false,
        vars: Map::new(),
    };
    let e = rule_errors(&def("POST /x HTTP/1.1\nAuthorization: Bearer abc\n\nbody"));
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("the rule template"), "{e:?}");
    assert!(e[0].contains("`vars.oauth`"), "{e:?}");
    assert!(rule_errors(&def("POST /x HTTP/1.1\n\nok")).is_empty());
    // a stray listing's template is checked as well
    let mut d = def("POST /x HTTP/1.1\n\nok");
    d.reset_steps.push(ResetStep {
        name: "sweep".into(),
        kind: StepKind::Stray {
            list_template: "GET /keys HTTP/1.1\nauthorization: x\n\n".into(),
            keys: "1".into(),
            next: None,
        },
        template: None,
    });
    let e = rule_errors(&d);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("reset step `sweep`"), "{e:?}");
}

// ---------------------------------------------------------------------------------
// vars.oauth=jwt_bearer (RFC 7523 §2.1, Salesforce)

/// The test-only RSA key of `src/tests/testdata`, both encodings (see its README).
fn key_pkcs8() -> Vec<u8> {
    jwt_check::fixture("rsa-test-key-pkcs8.pem")
}

fn key_pkcs1() -> Vec<u8> {
    jwt_check::fixture("rsa-test-key-pkcs1.pem")
}

const ISS: &str = "issuer@company.test";
const SUB: &str = "integration@company.test";
const AUD: &str = "https://company.my.salesforce.com";

/// The `vars` of a jwt_bearer target whose token endpoint is `token` and whose key sits
/// behind `key_ref`.
fn jwt_vars_json(token: &str, key_ref: &str, scope: Option<&str>) -> Value {
    let mut v = serde_json::json!({
        "oauth": "jwt_bearer",
        "token_url": token,
        "client_id": ISS,
        "subject": SUB,
        "audience": AUD,
        "signing_key_ref": key_ref,
    });
    if let Some(s) = scope {
        v["scope"] = Value::String(s.to_string());
    }
    v
}

fn jwt_provider(vars: Value) -> Provider {
    let def = target_def(vars);
    check(Kind::Http, &def.vars).unwrap(); // the load check accepts what is about to run
    Provider::new(&def, None).unwrap().unwrap()
}

/// What the test token endpoint verifies on every request it is sent.
fn expect_for(pem: &[u8], scope: Option<&str>) -> jwt_check::Expect {
    jwt_check::Expect {
        public_key: jwt_check::test_public_key(pem),
        iss: ISS.into(),
        sub: SUB.into(),
        aud: AUD.into(),
        scope: scope.map(str::to_string),
    }
}

#[test]
fn the_jwt_bearer_vars_parse_and_every_load_refusal_names_its_setting() {
    let base = || {
        jwt_vars_json("https://auth.test/token", "env:LWF_OAUTH_TEST_KEY", None)
            .as_object()
            .unwrap()
            .clone()
    };
    let err = |v: &Map<String, Value>| format!("{:#}", check(Kind::Http, v).unwrap_err());
    // accepted forms: plain, with the scope, plain http on loopback
    assert!(check(Kind::Http, &base()).is_ok());
    let with_scope = {
        let mut v = base();
        v.insert("scope".into(), Value::String("refresh_token api".into()));
        v
    };
    assert!(check(Kind::Http, &with_scope).is_ok());
    assert!(check(
        Kind::Http,
        &jwt_vars_json("http://127.0.0.1:9/token", "file:/k/key.pem", None)
            .as_object()
            .unwrap()
            .clone()
    )
    .is_ok());

    // every required setting is named when it is missing
    for k in [
        "token_url",
        "client_id",
        "subject",
        "audience",
        "signing_key_ref",
    ] {
        let mut v = base();
        v.remove(k);
        let e = err(&v);
        assert!(
            e.contains(&format!("`vars.{k}`")),
            "`{k}` missing, said `{e}`"
        );
    }
    // the key is a reference, never a literal
    let mut v = base();
    v.insert(
        "signing_key_ref".into(),
        Value::String("-----BEGIN PRIVATE KEY-----".into()),
    );
    let e = err(&v);
    assert!(
            e.contains("`vars.signing_key_ref` must be a reference (`env:NAME` or `file:/path`), never a literal"),
            "{e}"
        );
    // the token URL: the same checks as client_credentials
    let mut v = base();
    v.insert(
        "token_url".into(),
        Value::String("http://auth.test/token".into()),
    );
    let e = err(&v);
    assert!(e.contains("https is required"), "{e}");
    assert!(e.contains("only on loopback"), "{e}");
    let mut v = base();
    v.insert(
        "token_url".into(),
        Value::String("https://u:pw@auth.test/token".into()),
    );
    let e = err(&v);
    assert!(e.contains("vars.token_url"), "{e}");
    // an oauth setting of another grant is refused with this one, naming it
    for k in ["credentials_ref", "client_secret_ref"] {
        let mut v = base();
        v.insert(k.into(), Value::String("env:X".into()));
        let e = err(&v);
        assert!(
            e.contains(&format!(
                "`vars.{k}` is not used by `vars.oauth=jwt_bearer`"
            )),
            "{e}"
        );
    }
    // these settings with another grant are refused too
    for k in ["subject", "audience", "signing_key_ref"] {
        let mut v = Map::new();
        v.insert("oauth".into(), Value::String("google".into()));
        v.insert(k.into(), Value::String("x".into()));
        let e = err(&v);
        assert!(
            e.contains(&format!("`vars.{k}` is not used by `vars.oauth=google`")),
            "{e}"
        );
    }
    for k in ["subject", "audience", "signing_key_ref"] {
        let mut v = vars(CC);
        v.insert(k.into(), Value::String("x".into()));
        let e = err(&v);
        assert!(
            e.contains(&format!(
                "`vars.{k}` is not used by `vars.oauth=client_credentials`"
            )),
            "{e}"
        );
    }
    // a setting without the grant names the grant, with all three of its kinds
    let mut v = Map::new();
    v.insert("subject".into(), Value::String(SUB.into()));
    let e = err(&v);
    assert!(
        e.contains("needs `vars.oauth` (google, client_credentials or jwt_bearer)"),
        "{e}"
    );
    // a second source of Authorization is ambiguous with this grant as with the others
    let e = format!("{:#}", check(Kind::HttpBearer, &base()).unwrap_err());
    assert!(e.contains("`vars.token`"), "{e}");
}

#[test]
fn the_assertion_is_the_header_claims_and_signature_the_spec_wants() {
    let pubk = jwt_check::test_public_key(&key_pkcs8());
    let a = build_assertion(ISS, SUB, AUD, &key_pkcs8()).unwrap();
    jwt_check::check_assertion(&a, &pubk, ISS, SUB, AUD).unwrap();
    // three base64url parts without padding; the claims carry exactly what was set
    let parts: Vec<&str> = a.split('.').collect();
    assert_eq!(parts.len(), 3, "{a}");
    let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .unwrap();
    let claims: Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(claims["iss"], ISS);
    assert_eq!(claims["sub"], SUB);
    assert_eq!(claims["aud"], AUD);
    assert_eq!(
        claims["exp"].as_u64().unwrap() - claims["iat"].as_u64().unwrap(),
        180,
        "Salesforce accepts at most 3 minutes"
    );
    let header: Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[0])
            .unwrap(),
    )
    .unwrap();
    assert_eq!(header, serde_json::json!({"alg": "RS256", "typ": "JWT"}));
    // the same key in PKCS#1 form signs an assertion the same public key verifies
    let a1 = build_assertion(ISS, SUB, AUD, &key_pkcs1()).unwrap();
    jwt_check::check_assertion(&a1, &pubk, ISS, SUB, AUD).unwrap();
    // the verifier actually verifies: a tampered signature, a foreign key and a wrong
    // claim are each refused (an always-accepting check would make the tests a farce)
    let mut tampered = a.clone();
    // a character from the middle of the signature: the last one can carry
    // unused bits, so flipping it might not change the decoded bytes
    let at = a.rfind('.').unwrap() + 11;
    let first = a[at..].chars().next().unwrap();
    let flip = if first == 'A' { 'B' } else { 'A' };
    tampered.replace_range(at..at + 1, &flip.to_string());
    assert!(jwt_check::check_assertion(&tampered, &pubk, ISS, SUB, AUD).is_err());
    let other = build_assertion(
        ISS,
        SUB,
        AUD,
        &jwt_check::fixture("rsa-test-key-b-pkcs8.pem"),
    )
    .unwrap();
    assert!(jwt_check::check_assertion(&other, &pubk, ISS, SUB, AUD).is_err());
    assert!(jwt_check::check_assertion(&a, &pubk, "someone.else", SUB, AUD).is_err());
}

#[tokio::test]
async fn the_key_reaches_success_in_both_encodings_and_from_both_reference_forms() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    // PKCS#8 behind an `env:` reference: the endpoint verifies the assertion it is sent.
    std::env::set_var(
        "LWF_OAUTH_TEST_KEY",
        String::from_utf8_lossy(&key_pkcs8()).into_owned(),
    );
    let server = TokenServer::start().await;
    server.verify(expect_for(&key_pkcs8(), None));
    let p = jwt_provider(jwt_vars_json(&server.base, "env:LWF_OAUTH_TEST_KEY", None));
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    assert_eq!(server.hits(), 1);
    // the second call is the cache, not a second token request
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    assert_eq!(server.hits(), 1);
    // PKCS#1 behind a `file:` reference (through `LAZYWF_SECRET_DIRS`, as secrets.rs tests)
    let dir = std::env::temp_dir().join(format!("lazywf-oauth-jwt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("key.pem"), key_pkcs1()).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    let server2 = TokenServer::start().await;
    server2.verify(expect_for(&key_pkcs8(), Some("refresh_token api")));
    let p = jwt_provider(jwt_vars_json(
        &server2.base,
        &format!("file:{}/key.pem", dir.display()),
        Some("refresh_token api"),
    ));
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    // the token request: the §2.1 form, and no client authentication header
    let last = server2.last();
    assert!(last.starts_with("POST /token HTTP/1.1\r\n"), "{last}");
    assert!(
        last.contains(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="
        ),
        "{last}"
    );
    assert!(last.contains("&scope=refresh_token+api"), "{last}");
    assert!(
        !last.contains("Authorization:"),
        "a client-auth header rides an assertion grant: {last}"
    );
    assert!(
        last.contains("Content-Type: application/x-www-form-urlencoded\r\n"),
        "{last}"
    );
    assert!(last.contains("Accept: application/json\r\n"), "{last}");
    assert!(last.contains("User-Agent: lazywf/"), "{last}");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn a_key_that_is_not_rsa_or_not_a_key_is_transient_naming_the_cause() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let server = TokenServer::start().await;
    let provider = || jwt_provider(jwt_vars_json(&server.base, "env:LWF_OAUTH_TEST_KEY", None));

    // an EC key parses but cannot sign RS256: the cause names RSA
    std::env::set_var(
        "LWF_OAUTH_TEST_KEY",
        String::from_utf8_lossy(&jwt_check::fixture("ec-test-key-pkcs8.pem")).into_owned(),
    );
    let e = format!("{:#}", provider().authorization().await.unwrap_err());
    assert!(e.contains("not an RSA key"), "{e}");
    assert!(e.contains("oauth token"), "{e}");
    assert!(!e.contains("refused by the rule"), "{e}");
    assert!(
        !e.contains("PRIVATE KEY"),
        "the key bytes reached the message: {e}"
    );

    // PEM-shaped garbage: rustls parses the block and rejects the body, and
    // the cause is named without the key material
    std::env::set_var(
        "LWF_OAUTH_TEST_KEY",
        "-----BEGIN PRIVATE KEY-----\nZm9vYmFyLW5vdC1hLWtleQ\n-----END PRIVATE KEY-----",
    );
    let e = format!("{:#}", provider().authorization().await.unwrap_err());
    assert!(e.contains("cannot use"), "{e}");
    assert!(e.contains("parse private key"), "{e}");
    assert!(!e.contains("refused by the rule"), "{e}");

    // not PEM at all: the message says so
    std::env::set_var("LWF_OAUTH_TEST_KEY", "not a key, not even close");
    let e = format!("{:#}", provider().authorization().await.unwrap_err());
    assert!(e.contains("not a PEM private key"), "{e}");
    assert!(!e.contains("refused by the rule"), "{e}");

    // an unresolvable reference: transient, naming the reference that failed
    std::env::set_var("LWF_OAUTH_TEST_KEY", "unused");
    let p = jwt_provider(jwt_vars_json(
        &server.base,
        "env:LWF_OAUTH_TEST_KEY_DEFINITELY_MISSING",
        None,
    ));
    let e = format!("{:#}", p.authorization().await.unwrap_err());
    assert!(e.contains("vars.signing_key_ref"), "{e}");
    assert!(e.contains("is not set"), "{e}");
    assert!(!e.contains("refused by the rule"), "{e}");
}

#[tokio::test]
async fn new_bytes_behind_the_key_and_new_claims_drop_the_cached_token() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let dir = std::env::temp_dir().join(format!("lazywf-oauth-jwt-rot-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let key_file = dir.join("key.pem");
    std::fs::write(&key_file, key_pkcs8()).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    std::env::set_var("LWF_OAUTH_TEST_AUD", AUD);
    let server = TokenServer::start().await;
    server.verify(expect_for(&key_pkcs8(), None));
    let mut vars = jwt_vars_json(
        &server.base,
        &format!("file:{}/key.pem", dir.display()),
        None,
    )
    .as_object()
    .unwrap()
    .clone();
    vars.insert(
        "audience".into(),
        Value::String("env:LWF_OAUTH_TEST_AUD".into()),
    );
    let p = jwt_provider(Value::Object(vars));
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-0");
    assert_eq!(server.hits(), 1);
    // new bytes behind the same reference: the endpoint now demands the other key, and
    // the client indeed brings one the new public key verifies
    server.verify(expect_for(
        &jwt_check::fixture("rsa-test-key-b-pkcs8.pem"),
        None,
    ));
    std::fs::write(&key_file, jwt_check::fixture("rsa-test-key-b-pkcs8.pem")).unwrap();
    assert_eq!(
        p.authorization().await.unwrap(),
        "Bearer tok-1",
        "new bytes behind the reference drop the entry at once"
    );
    assert_eq!(server.hits(), 2);
    // a new audience drops it too: the hash covers iss/sub/aud
    std::env::set_var("LWF_OAUTH_TEST_AUD", "https://other.my.salesforce.com");
    server.verify(jwt_check::Expect {
        aud: "https://other.my.salesforce.com".into(),
        ..expect_for(&jwt_check::fixture("rsa-test-key-b-pkcs8.pem"), None)
    });
    assert_eq!(p.authorization().await.unwrap(), "Bearer tok-2");
    assert_eq!(server.hits(), 3);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn every_jwt_token_endpoint_failure_class_is_transient_with_its_cause() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var(
        "LWF_OAUTH_TEST_KEY",
        String::from_utf8_lossy(&key_pkcs8()).into_owned(),
    );
    let provider = |url: String| jwt_provider(jwt_vars_json(&url, "env:LWF_OAUTH_TEST_KEY", None));

    // a closed port: the cause is the connection
    let closed = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        format!("http://{a}/token")
    };
    let e = format!("{:#}", provider(closed).authorization().await.unwrap_err());
    assert!(e.contains("token endpoint POST"), "{e}");
    assert!(e.contains("refused"), "{e}");

    let server = TokenServer::start().await; // no verify hook: the answers are set below
    let p = provider(server.base.clone());
    let cases: Vec<(u16, &str, &[&str])> = vec![
        (
            400,
            r#"{"error": "invalid_grant", "error_description": "assertion expired"}"#,
            &["status 400", "invalid_grant", "assertion expired"],
        ),
        (
            401,
            r#"{"error": "invalid_client"}"#,
            &["status 401", "invalid_client"],
        ),
        (500, "boom", &["status 500", "boom"]),
        (200, "<html>login</html>", &["not JSON", "login"]),
        (200, r#"{"token": "nope"}"#, &["no access_token"]),
        (
            200,
            r#"{"access_token": "t", "token_type": "mac"}"#,
            &["token_type `mac`"],
        ),
    ];
    for (status, body, wants) in cases {
        server.set_answer(status, body);
        let e = format!("{:#}", p.authorization().await.unwrap_err());
        for w in wants {
            assert!(e.contains(w), "`{body}` answered as `{e}`");
        }
        assert!(!e.contains("refused by the rule"), "{e}");
        assert!(
            !e.contains("PRIVATE KEY"),
            "the key reached the message: {e}"
        );
    }

    // an endpoint that echoes the whole request cannot carry the assertion out
    let (echo, echo_reqs) = echo_endpoint().await;
    let p = provider(format!("http://{echo}/token"));
    let e = format!("{:#}", p.authorization().await.unwrap_err());
    assert!(e.contains("status 400"), "{e}");
    // what went out (and came back echoed) was one assertion: three base64url parts
    let body = echo_reqs
        .lock()
        .unwrap()
        .last()
        .cloned()
        .unwrap_or_default();
    let sent = body
        .rsplit_once("assertion=")
        .map(|(_, a)| a.split('&').next().unwrap_or_default().to_string())
        .unwrap_or_default();
    assert_eq!(
        sent.matches('.').count(),
        2,
        "not an assertion on the wire: `{sent}`"
    );
    assert!(!e.contains(&sent), "the assertion escaped into `{e}`");
}

/// An endpoint that answers 400 with the bytes of the request it was sent: the classic
/// way a credential escapes into an error text. Returns its address and its record.
async fn echo_endpoint() -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let seen = sink.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") {
                    if s.read(&mut byte).await.unwrap_or(0) == 0 {
                        break;
                    }
                    buf.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&buf).to_lowercase();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse().ok())
                    })
                    .unwrap_or(0);
                let mut body = vec![0u8; len];
                if len > 0 {
                    s.read_exact(&mut body).await.unwrap();
                }
                let all = format!(
                    "{}{}",
                    String::from_utf8_lossy(&buf),
                    String::from_utf8_lossy(&body)
                );
                seen.lock().unwrap().push(all.clone());
                let text = format!(
                    "HTTP/1.1 400 X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{all}",
                    all.len()
                );
                let _ = s.write_all(text.as_bytes()).await;
            });
        }
    });
    (a, seen)
}

#[tokio::test]
async fn the_jwt_provider_keeps_the_token_out_of_messages_and_debug() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var(
        "LWF_OAUTH_TEST_KEY",
        String::from_utf8_lossy(&key_pkcs8()).into_owned(),
    );
    let server = TokenServer::start().await;
    server.set_answer(200, r#"{"access_token": "LWF-JWT-TOKEN-MARKER-7a1"}"#); // no expires_in: Salesforce's form
    let p = jwt_provider(jwt_vars_json(&server.base, "env:LWF_OAUTH_TEST_KEY", None));
    // no `expires_in`: DEFAULT_LIFETIME applies (the short Salesforce session is covered
    // by the 401 drop, exactly as the design says)
    assert_eq!(
        p.authorization().await.unwrap(),
        "Bearer LWF-JWT-TOKEN-MARKER-7a1"
    );
    let d = format!("{p:?}");
    assert!(d.contains("jwt_bearer"), "{d}");
    assert!(!d.contains("LWF-JWT-TOKEN-MARKER-7a1"), "{d}");
    assert!(!d.contains("PRIVATE KEY"), "{d}");
    let scrubbed = p.scrub("Authorization: Bearer LWF-JWT-TOKEN-MARKER-7a1 seen");
    assert!(!scrubbed.contains("LWF-JWT-TOKEN-MARKER-7a1"), "{scrubbed}");
}
