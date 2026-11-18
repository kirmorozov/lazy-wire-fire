//! What the Google token never leaks into, and what a broken credential says.
//! [`Auth::scrub`] masks the cached token and the one most recently handed out of any
//! text a service echoed back; a key that is not JSON, a key that is not a key, and an
//! ADC that cannot be built all fail with their own message before any request.

use super::*;

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
