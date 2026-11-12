use base64::Engine as _;
use serde_json::Value;

/// What the recorded token request must carry: the grant, a validly signed assertion
/// for these claims, and this `scope` (or none).
pub(crate) struct Expect {
    /// The public key as PKCS#1 `RSAPublicKey` DER (the form ring verifies against).
    pub public_key: Vec<u8>,
    pub iss: String,
    pub sub: String,
    pub aud: String,
    pub scope: Option<String>,
}

/// A fixture from `src/testdata` (the test-only keys).
pub(crate) fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/src/testdata/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

/// The public key belonging to a PEM private key (either encoding), as PKCS#1
/// `RSAPublicKey` DER: the form ring verifies RS256 against (rustls's `public_key()`
/// wraps these same bytes into an SPKI, which ring's verifier does not parse).
pub(crate) fn test_public_key(pem: &[u8]) -> Vec<u8> {
    use ring::signature::KeyPair as _;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::PrivateKeyDer;
    let der = PrivateKeyDer::from_pem_slice(pem).expect("the test key parses");
    let kp = match &der {
        PrivateKeyDer::Pkcs8(p) => ring::signature::RsaKeyPair::from_pkcs8(p.secret_pkcs8_der()),
        PrivateKeyDer::Pkcs1(p) => ring::signature::RsaKeyPair::from_der(p.secret_pkcs1_der()),
        _ => panic!("the RSA test keys are PKCS#8 or PKCS#1"),
    }
    .expect("the test key is an RSA key");
    kp.public_key().as_ref().to_vec()
}

/// The whole token body as the endpoint reads it: the grant, the assertion, the scope.
/// `Err(cause)` is what makes the stand-in answer `invalid_grant`.
pub(crate) fn check_form(body: &str, want: &Expect) -> Result<(), String> {
    let fields = body
        .split('&')
        .filter_map(|f| f.split_once('='))
        .map(|(k, v)| (k.to_string(), pct_decode(v)))
        .collect::<std::collections::BTreeMap<_, _>>();
    if fields.get("grant_type").map(String::as_str) != Some(super::JWT_BEARER_GRANT) {
        return Err(format!(
            "grant_type `{:?}` (want `{}`)",
            fields.get("grant_type"),
            super::JWT_BEARER_GRANT
        ));
    }
    let assertion = fields
        .get("assertion")
        .ok_or_else(|| "no assertion field".to_string())?
        .clone();
    check_assertion(
        &assertion,
        &want.public_key,
        &want.iss,
        &want.sub,
        &want.aud,
    )?;
    match (&want.scope, fields.get("scope")) {
        (None, None) => Ok(()),
        (Some(w), Some(got)) if w == got => Ok(()),
        (w, got) => Err(format!("scope {got:?} (want {:?})", w.as_deref())),
    }
}

/// One assertion, checked the way a receiver must: three base64url parts without
/// padding, the RS256 header, the claims, and the signature over `header.claims`.
pub(crate) fn check_assertion(
    assertion: &str,
    public_key: &[u8],
    iss: &str,
    sub: &str,
    aud: &str,
) -> Result<(), String> {
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let parts: Vec<&str> = assertion.split('.').collect();
    if parts.len() != 3 {
        return Err(format!(
            "{} parts (want header.claims.signature)",
            parts.len()
        ));
    }
    for p in &parts {
        if p.is_empty() || p.contains(['=', '+', '/']) {
            return Err(format!("not base64url without padding: `{p}`"));
        }
    }
    let header: Value = serde_json::from_slice(
        &engine
            .decode(parts[0])
            .map_err(|e| format!("header: {e}"))?,
    )
    .map_err(|e| format!("header: {e}"))?;
    if header.get("alg").and_then(Value::as_str) != Some("RS256")
        || header.get("typ").and_then(Value::as_str) != Some("JWT")
    {
        return Err(format!("header is {header}"));
    }
    let claims: Value = serde_json::from_slice(
        &engine
            .decode(parts[1])
            .map_err(|e| format!("claims: {e}"))?,
    )
    .map_err(|e| format!("claims: {e}"))?;
    for (k, want) in [("iss", iss), ("sub", sub), ("aud", aud)] {
        let got = claims.get(k).and_then(Value::as_str);
        if got != Some(want) {
            return Err(format!("claim `{k}` is {got:?} (want `{want}`)"));
        }
    }
    let iat = claims.get("iat").and_then(Value::as_u64);
    let exp = claims.get("exp").and_then(Value::as_u64);
    match (iat, exp) {
        (Some(iat), Some(exp)) if exp.saturating_sub(iat) == super::ASSERTION_LIFETIME => {}
        (iat, exp) => {
            return Err(format!(
                "iat {iat:?} exp {exp:?} (want exp − iat = {})",
                super::ASSERTION_LIFETIME
            ))
        }
    }
    let sig = engine
        .decode(parts[2])
        .map_err(|e| format!("signature: {e}"))?;
    let input = format!("{}.{}", parts[0], parts[1]);
    ring::signature::UnparsedPublicKey::new(
        &ring::signature::RSA_PKCS1_2048_8192_SHA256,
        public_key,
    )
    .verify(input.as_bytes(), &sig)
    .map_err(|e| format!("the RS256 signature does not verify: {e}"))
}

/// The percent-decoding a form endpoint does (`+` is a space).
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap_or(b'%'));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
