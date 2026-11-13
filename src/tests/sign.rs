use super::*;

fn vars(v: &str) -> Map<String, Value> {
    serde_json::from_str(v).unwrap()
}

/// The secret of the spec's published test vector (its reference libraries pin the same
/// one in their tests; it is not a real credential).
const STD_SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
const STD_KEY: &str = "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";

#[test]
fn the_spec_vector_signs_byte_for_byte() {
    let key = base64::engine::general_purpose::STANDARD
        .decode(STD_KEY)
        .unwrap();
    assert_eq!(
        standard_signature(
            &key,
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            "1614265330",
            br#"{"test": 2432232314}"#
        ),
        "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
    );
    // One byte of the body moves the whole signature (the spec's warning).
    assert_ne!(
        standard_signature(
            &key,
            "msg_p5jXN8AQM9LWM0D4loKWxJek",
            "1614265330",
            br#"{"test":2432232314}"#
        ),
        "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
    );
}

#[test]
fn the_github_style_vector() {
    // Cross-checked with openssl: the default form of the hmac scheme.
    assert_eq!(
        hmac_value(
            "sha256=",
            Encoding::Hex,
            b"0123456789012345678901234567890123456789012345678901234567890123",
            br#"{"foo":"bar"}"#
        ),
        "sha256=676a225e230b18d80bed1d8bc12f322f3d8b570fa2ffa470c254ff8c10ef7212"
    );
    // The base64 form of the same digest.
    let want = base64::engine::general_purpose::STANDARD.encode(
        hex::decode("676a225e230b18d80bed1d8bc12f322f3d8b570fa2ffa470c254ff8c10ef7212").unwrap(),
    );
    assert_eq!(
        hmac_value(
            "sha256=",
            Encoding::Base64,
            b"0123456789012345678901234567890123456789012345678901234567890123",
            br#"{"foo":"bar"}"#
        ),
        format!("sha256={want}")
    );
}

#[test]
fn load_checks_name_the_setting() {
    assert!(check(&vars(
        r#"{"sign": "standard", "sign_secret_ref": "env:SIGN_SECRET"}"#
    ))
    .is_ok());
    assert!(check(&vars(
        r#"{"sign": "hmac", "sign_secret_ref": "env:SIGN_SECRET"}"#
    ))
    .is_ok());
    assert!(check(&vars("{}")).is_ok()); // no signing at all
    let err = |v: &str| check(&vars(v)).unwrap_err().to_string();
    assert!(
        err(r#"{"sign": "ed25519", "sign_secret_ref": "env:A"}"#).contains("`vars.sign`")
            && err(r#"{"sign": "ed25519", "sign_secret_ref": "env:A"}"#)
                .contains("standard or hmac")
    );
    assert!(err(r#"{"sign": "standard"}"#).contains("`vars.sign_secret_ref`"));
    assert!(
        err(r#"{"sign": "standard", "sign_secret_ref": "a-literal"}"#)
            .contains("`vars.sign_secret_ref` must be a reference")
    );
    // a signing setting without the scheme
    assert!(err(r#"{"sign_secret_ref": "env:A"}"#).contains("`vars.sign`"));
    assert!(err(r#"{"sign_prefix": "x"}"#).contains("`vars.sign_prefix`"));
    assert!(err(r#"{"sign_header": "X-S"}"#).contains("`vars.sign_header`"));
    // the hmac header form
    assert!(
        err(r#"{"sign": "hmac", "sign_secret_ref": "env:A", "sign_encoding": "bits"}"#)
            .contains("`vars.sign_encoding`")
    );
    assert!(
        err(r#"{"sign": "hmac", "sign_secret_ref": "env:A", "sign_header": "a b"}"#)
            .contains("`vars.sign_header`")
    );
    // the standard scheme fixes its headers and value form
    for k in ["sign_header", "sign_encoding", "sign_prefix"] {
        assert!(
            err(&format!(
                r#"{{"sign": "standard", "sign_secret_ref": "env:A", "{k}": "x"}}"#
            ))
            .contains(&format!("`vars.{k}`")),
            "{k}"
        );
    }
    assert!(err(r#"{"sign": "standard", "sign_secret_ref": 1}"#).contains("`vars.sign_secret_ref`"));
}

fn spec_for(scheme: Scheme, secret_ref: &str) -> Spec {
    Spec {
        scheme,
        secret_ref: secret_ref.into(),
        header: "X-Signature-256".into(),
        encoding: Encoding::Hex,
        prefix: "sha256=".into(),
        seq: AtomicU64::new(0x0123_4567_89ab_cdef),
    }
}

fn hmac_spec(header: &str, encoding: Encoding, prefix: &str) -> Spec {
    Spec {
        scheme: Scheme::Hmac,
        secret_ref: "env:LWF_SIGN_TEST_HMAC".into(),
        header: header.into(),
        encoding,
        prefix: prefix.into(),
        seq: AtomicU64::new(0),
    }
}

#[test]
fn apply_standard_sets_the_spec_headers() {
    let _env = crate::secrets::ENV_LOCK.blocking_lock();
    std::env::set_var("LWF_SIGN_TEST_STD", STD_SECRET);
    let spec = spec_for(Scheme::Standard, "env:LWF_SIGN_TEST_STD");
    let key = base64::engine::general_purpose::STANDARD
        .decode(STD_KEY)
        .unwrap();
    let mut req = RawRequest {
        method: "POST".into(),
        target: "/hook".into(),
        headers: Vec::new(),
        body: br#"{"test": 2432232314}"#.to_vec(),
    };
    apply(&mut req, "t", &spec, 1_614_265_330).unwrap();
    let id = req.header("webhook-id").unwrap().to_string();
    assert_eq!(req.header("webhook-timestamp"), Some("1614265330"));
    assert_eq!(
        req.header("webhook-signature"),
        Some(standard_signature(&key, &id, "1614265330", &req.body).as_str())
    );
    // The id is unique per request and names the target.
    let mut again = req.clone();
    apply(&mut again, "t", &spec, 1_614_265_330).unwrap();
    let other = again.header("webhook-id").unwrap();
    assert_ne!(id, other);
    assert!(
        id.starts_with("t-") && other.starts_with("t-"),
        "{id} {other}"
    );
}

#[test]
fn apply_hmac_sets_the_configured_header() {
    let _env = crate::secrets::ENV_LOCK.blocking_lock();
    std::env::set_var("LWF_SIGN_TEST_HMAC", "0123456789");
    let spec = spec_for(Scheme::Hmac, "env:LWF_SIGN_TEST_HMAC");
    let body = br#"{"foo":"bar"}"#;
    let mut req = RawRequest {
        method: "POST".into(),
        target: "/hook".into(),
        headers: Vec::new(),
        body: body.to_vec(),
    };
    apply(&mut req, "t", &spec, 0).unwrap();
    assert_eq!(
        req.header("x-signature-256"),
        Some(hmac_value("sha256=", Encoding::Hex, b"0123456789", body).as_str())
    );
    // A configured header name, prefix and encoding are honoured.
    let spec = hmac_spec("X-Hook-Sig", Encoding::Base64, "sig=");
    let mut req = RawRequest {
        method: "POST".into(),
        target: "/hook".into(),
        headers: Vec::new(),
        body: body.to_vec(),
    };
    apply(&mut req, "t", &spec, 0).unwrap();
    assert!(req.header("x-signature-256").is_none());
    assert_eq!(
        req.header("x-hook-sig"),
        Some(hmac_value("sig=", Encoding::Base64, b"0123456789", body).as_str())
    );
}

#[test]
fn a_misformed_standard_secret_fails_naming_the_setting() {
    let _env = crate::secrets::ENV_LOCK.blocking_lock();
    for bad in ["no-prefix", "whsec_!!", "whsec_"] {
        std::env::set_var("LWF_SIGN_TEST_BAD", bad);
        let spec = spec_for(Scheme::Standard, "env:LWF_SIGN_TEST_BAD");
        let mut req = RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: b"x".to_vec(),
        };
        let err = apply(&mut req, "t", &spec, 0).unwrap_err().to_string();
        assert!(err.contains("whsec_"), "{bad}: {err}");
    }
    // A missing reference, as at send time: it names the setting too.
    std::env::remove_var("LWF_SIGN_TEST_BAD");
    let err = apply(
        &mut RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: b"x".to_vec(),
        },
        "t",
        &spec_for(Scheme::Hmac, "env:LWF_SIGN_TEST_MISSING"),
        0,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("LWF_SIGN_TEST_MISSING"), "{err}");
}
