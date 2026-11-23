//! What the Google token never leaks into, and what a broken credential says.
//! [`Auth::scrub`] masks the cached token and the one most recently handed out of any
//! text a service echoed back; a key that is not JSON, a key that is not a key, and an
//! ADC that cannot be built all fail with their own message before any request.

use super::*;
use std::sync::Arc;

#[test]
fn scrub_masks_the_cached_token_and_the_last_one_issued() {
    let auth = Auth::new(Source::Adc);
    // Nothing cached: the text passes as it came.
    assert_eq!(auth.scrub("plain text ya29.GUESS"), "plain text ya29.GUESS");
    *auth.lock() = Some(Cached {
        key_hash: [0u8; 32],
        token: "ya29.CACHED".into(),
        fetched: Instant::now(),
    });
    *auth.last() = Some("ya29.ISSUED".into());
    assert_eq!(
        auth.scrub("the service echoed: ya29.CACHED and ya29.ISSUED!"),
        "the service echoed: *** and ***!"
    );
    // `invalidate` drops the cache for the next request; the service can still echo the
    // token in the error of the very request that follows, so it stays masked.
    auth.invalidate();
    assert!(!auth.cached());
    assert_eq!(
        auth.scrub("echoed: ya29.CACHED and ya29.ISSUED"),
        "echoed: ya29.CACHED and ***"
    );
}

#[tokio::test]
async fn a_fresh_cache_answers_without_touching_the_service() {
    // The token taken from a cache that is not yet due: no key, no network.
    let auth = Auth::new(Source::Adc);
    *auth.lock() = Some(Cached {
        key_hash: [0u8; 32],
        token: "ya29.FRESH".into(),
        fetched: Instant::now(),
    });
    assert_eq!(auth.token().await.unwrap(), "ya29.FRESH");
}

#[tokio::test]
async fn a_key_that_is_not_json_is_refused_by_name() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_TEST_GCP_KEY_NOT_JSON", "this is not JSON at all");
    let auth = Auth::new(Source::Key("env:LWF_TEST_GCP_KEY_NOT_JSON".into()));
    let e = auth.token().await.unwrap_err();
    assert!(
        format!("{e:#}").contains("the Google service-account key is not JSON"),
        "{e:#}"
    );
}

#[tokio::test]
async fn a_json_that_is_not_a_service_account_key_says_so() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    // Valid JSON, no `private_key`/`client_email`: the builder itself refuses it,
    // before any token request. Once plainly, once with explicit scopes (the oauth
    // path, which wraps the specifier around the builder first).
    std::env::set_var(
        "LWF_TEST_GCP_KEY_NO_FIELDS",
        r#"{"type": "authorized_user"}"#,
    );
    let auth = Auth::new(Source::Key("env:LWF_TEST_GCP_KEY_NO_FIELDS".into()));
    let e = auth.token().await.unwrap_err();
    assert!(
        format!("{e:#}").contains("Google service-account credentials:"),
        "{e:#}"
    );
    let scoped = Auth::with_scopes(
        Source::Key("env:LWF_TEST_GCP_KEY_NO_FIELDS".into()),
        vec!["https://www.googleapis.com/auth/pubsub".into()],
    );
    let e = scoped.token().await.unwrap_err();
    assert!(
        format!("{e:#}").contains("Google service-account credentials:"),
        "{e:#}"
    );
}

#[tokio::test]
async fn an_adc_that_cannot_be_built_names_itself() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let was = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
    let dir = std::env::temp_dir().join(format!("lazywf-gcp-adc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let bogus = dir.join("bogus.json");
    std::fs::write(&bogus, "not a credential file").unwrap();
    std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", &bogus);
    let auth = Auth::new(Source::Adc);
    let got = tokio::time::timeout(Duration::from_secs(30), auth.token()).await;
    match was {
        Some(v) => std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", v),
        None => std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS"),
    }
    let e = got
        .expect("the builder fails without a network")
        .unwrap_err();
    assert!(
        format!("{e:#}").contains("Google application default credentials:"),
        "{e:#}"
    );
}

// ——— the fetch itself, against a token endpoint of the test's own ————————————————————————
//
// The credential is an `authorized_user` ADC file, the one shape of application default
// credentials that names its own token endpoint: the test points it at a server of loopback,
// so the whole path — the scopes asked for, the answer cached, the answer refused — runs
// without a network and without a real key.

/// A token endpoint of loopback: it answers every request with `status` and `body` and keeps
/// the body of each, so a test can read what was asked as well as what was answered.
async fn serve_token(
    status: u16,
    body: &'static str,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let got: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // The head, then the body the Content-Length promises: answering while the
                // client still has bytes to write makes its write fail instead of seeing the
                // answer.
                loop {
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let split = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                let head = String::from_utf8_lossy(&buf[..split]).into_owned();
                let want = head
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
                while buf.len() - split < want {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                sink.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[split..]).into_owned());
                let reason = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    _ => "Server Error",
                };
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                // Three unchecked results, and the reason each is unchecked: the `write_all` is
                // load-bearing — delete that one line and the suite goes red, because the test
                // that waits for a token fails at the await (`unit tests`: 989 passed; 1 failed).
                // `flush` is don't-care by reasoning rather than reflex: a `TcpStream` keeps no
                // buffer of its own, so `write_all` has already handed the bytes to the kernel
                // (deleting it left `unit tests` green at 990 passed; 0 failed). `shutdown` is
                // don't-care to the client too, because the answer carries `Content-Length` and
                // no reader waits for EOF; it is here so the accepted socket ends with this task
                // instead of whenever the runtime drops it (deleting it also left the suite green).
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.flush().await;
                let _ = s.shutdown().await;
            });
        }
    });
    (addr, got)
}

/// A scratch directory of this test run under the repository's `tmp/`, the same shape the other
/// unit tests use (`src/tests/aws_provider_tests.rs`): the ADC file a test wrote stays readable
/// next to the rest of the test output instead of disappearing into `/tmp`.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("unit")
        .join(format!("gcp-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// An ADC file of the one shape that names its own token endpoint, for `addr`. `tag` keeps the
/// file of one test from being the file of another, so both can be read after the run.
fn adc_file(tag: &str, addr: std::net::SocketAddr) -> std::path::PathBuf {
    let file = scratch(&format!("adc-{tag}")).join("adc.json");
    std::fs::write(
        &file,
        format!(
            r#"{{"type":"authorized_user","client_id":"lazywf.apps.googleusercontent.com",
                "client_secret":"lazywf-secret","refresh_token":"1//lazywf-refresh",
                "token_uri":"http://{addr}/token"}}"#
        ),
    )
    .unwrap();
    file
}

/// Points `GOOGLE_APPLICATION_CREDENTIALS` at `file` and puts the previous value back.
struct Adc {
    was: Option<String>,
}

impl Adc {
    fn point_at(file: &std::path::Path) -> Adc {
        let was = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
        std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", file);
        Adc { was }
    }
}

impl Drop for Adc {
    fn drop(&mut self) {
        match self.was.take() {
            Some(v) => std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", v),
            None => std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS"),
        }
    }
}

#[tokio::test]
async fn an_adc_of_its_own_asks_for_its_scopes_and_caches_the_answer() {
    // The scoped arm of the ADC branch: `pubsub` asks for nothing and an oauth target asks
    // for a scope of its own, both on the same builder. The scopes have to reach the token
    // endpoint, and what comes back has to be cached and masked like any other token.
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let (addr, got) = serve_token(
        200,
        r#"{"access_token":"ya29.ADC-TEST","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    let _adc = Adc::point_at(&adc_file("scopes-cached", addr));
    let scope = "https://www.googleapis.com/auth/pubsub";
    let auth = Auth::with_scopes(Source::Adc, vec![scope.into()]);

    assert_eq!(auth.token().await.unwrap(), "ya29.ADC-TEST");
    assert!(auth.cached(), "the answer is kept for the next request");
    let asked = got.lock().unwrap().join("\n---\n");
    assert!(
        asked.contains("\"grant_type\":\"refresh_token\""),
        "{asked}"
    );
    assert!(
        asked.contains("lazywf.apps.googleusercontent.com"),
        "the credential is the one the file names: {asked}"
    );
    assert!(asked.contains(scope), "the scope was asked for: {asked}");
    // A server that echoes the token cannot carry it into a log line.
    assert_eq!(auth.scrub("denied: ya29.ADC-TEST"), "denied: ***");

    // The next request reads the cache: no second token is fetched.
    assert_eq!(auth.token().await.unwrap(), "ya29.ADC-TEST");
    assert_eq!(got.lock().unwrap().len(), 1, "the cache answered");

    // A 401 drops the cache, and the request after it pays for a token again — while the
    // token it just lost keeps being masked, because the service may still echo it.
    auth.invalidate();
    assert!(!auth.cached());
    assert_eq!(auth.token().await.unwrap(), "ya29.ADC-TEST");
    assert_eq!(
        got.lock().unwrap().len(),
        2,
        "the invalidated cache asked again"
    );
}

#[tokio::test]
async fn a_token_endpoint_that_refuses_names_the_service_that_failed() {
    // The failure side of the same path: the endpoint answered and said no. The message has
    // to say whose token failed to arrive — and it must not read as a refusal of the keys,
    // which is what a credential that will not come never is.
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let (addr, _got) = serve_token(
        400,
        r#"{"error":"invalid_grant","error_description":"Bad Request"}"#,
    )
    .await;
    let _adc = Adc::point_at(&adc_file("refused", addr));
    let auth = Auth::with_scopes(
        Source::Adc,
        vec!["https://www.googleapis.com/auth/pubsub".into()],
    );
    let e = tokio::time::timeout(Duration::from_secs(30), auth.token())
        .await
        .expect("the refusal arrives without a network")
        .unwrap_err();
    let msg = format!("{e:#}");
    assert!(msg.contains("Google access token:"), "{msg}");
    assert!(
        !crate::target::is_rejected(&e),
        "a credential that will not come waits; it never drops a key: {msg}"
    );
    assert!(!auth.cached(), "nothing was cached from a refused fetch");
}
