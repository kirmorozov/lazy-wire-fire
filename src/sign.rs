//! Webhook signing of the http kinds: `standard` (the Standard Webhooks specification) and
//! `hmac` (one header over the body, GitHub-style).
//!
//! The secret is a reference (`env:NAME` or `file:/path`), checked at load and resolved on
//! every use like the other credentials: it never reaches a template, a `{:?}` or a log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;

use crate::http::RawRequest;

type HmacSha256 = Hmac<Sha256>;

/// The signing scheme of an http target (`vars.sign`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// Standard Webhooks: `webhook-id`, `webhook-timestamp` and
    /// `webhook-signature: v1,<base64 HMAC-SHA256 of id.timestamp.body>` over the body.
    Standard,
    /// One header: `<prefix><hex|base64 HMAC-SHA256 of the body>` (GitHub-style).
    Hmac,
}

impl Scheme {
    pub fn parse(s: &str) -> Result<Scheme> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "standard" => Scheme::Standard,
            "hmac" => Scheme::Hmac,
            other => bail!("`vars.sign` is `{other}` (use standard or hmac)"),
        })
    }
}

/// The digest form of the `hmac` scheme's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Hex,
    Base64,
}

impl Encoding {
    fn parse(s: &str) -> Result<Encoding> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "hex" => Encoding::Hex,
            "base64" => Encoding::Base64,
            other => bail!("`vars.sign_encoding` is `{other}` (use hex or base64)"),
        })
    }
}

/// The signing of an http target, as loaded from `vars`: the secret stays a reference, it is
/// resolved on every use.
pub struct Spec {
    pub scheme: Scheme,
    /// `env:NAME` or `file:/path` (a reference, checked at load).
    pub secret_ref: String,
    /// The header of the `hmac` scheme (`X-Signature-256` by default).
    pub header: String,
    /// The digest form of the `hmac` scheme (hex by default).
    pub encoding: Encoding,
    /// The value prefix of the `hmac` scheme (`sha256=` by default).
    pub prefix: String,
    /// The id sequence of the `standard` scheme: unique per request, per target.
    seq: AtomicU64,
}

/// The first signing setting of `vars`, when it has one (for the load errors).
pub fn first_var(vars: &Map<String, Value>) -> Option<&'static str> {
    [
        "sign",
        "sign_secret_ref",
        "sign_header",
        "sign_encoding",
        "sign_prefix",
    ]
    .into_iter()
    .find(|k| vars.contains_key(*k))
}

/// The load check of an http target's signing vars: an unknown scheme, a missing secret or a
/// literal where a reference belongs is an error naming the setting.
pub fn check(vars: &Map<String, Value>) -> Result<()> {
    let Some(s) = vars.get("sign") else {
        // Without the scheme the other settings would go unused: name the first one found.
        if let Some(k) = [
            "sign_secret_ref",
            "sign_header",
            "sign_encoding",
            "sign_prefix",
        ]
        .into_iter()
        .find(|k| vars.contains_key(*k))
        {
            bail!("`vars.{k}` sets the signing, which needs `vars.sign` (standard or hmac)");
        }
        return Ok(());
    };
    let s = s
        .as_str()
        .ok_or_else(|| anyhow!("`vars.sign` must be a string"))?;
    let scheme = Scheme::parse(s)?;
    match vars.get("sign_secret_ref").and_then(Value::as_str) {
        None | Some("") => bail!(
            "`vars.sign` needs `vars.sign_secret_ref` (an `env:NAME` or `file:/path` reference)"
        ),
        Some(v) if !crate::secrets::is_reference(v) => bail!(
            "`vars.sign_secret_ref` must be a reference (`env:NAME` or `file:/path`), never a literal"
        ),
        Some(_) => {}
    }
    match scheme {
        Scheme::Standard => {
            // The standard scheme fixes its headers and its value form.
            for k in ["sign_header", "sign_encoding", "sign_prefix"] {
                if vars.contains_key(k) {
                    bail!(
                        "`vars.{k}` is not used by the standard signing (it sets the hmac scheme's header)"
                    );
                }
            }
        }
        Scheme::Hmac => {
            if let Some(h) = vars.get("sign_header").and_then(Value::as_str) {
                if h.is_empty() || h.bytes().any(|c| c <= b' ' || c >= 0x7f || c == b':') {
                    bail!("`vars.sign_header` is not a header name");
                }
            }
            if let Some(e) = vars.get("sign_encoding").and_then(Value::as_str) {
                Encoding::parse(e)?;
            }
            if let Some(p) = vars.get("sign_prefix") {
                if p.as_str().is_none() {
                    bail!("`vars.sign_prefix` must be a string");
                }
            }
        }
    }
    Ok(())
}

/// The signing of `vars`, when it is signed (the checked form of [`check`]).
pub fn spec(vars: &Map<String, Value>) -> Result<Option<Spec>> {
    let Some(s) = vars
        .get("sign")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    // `check` runs at load; this cannot fail on a loaded definition.
    let header = vars
        .get("sign_header")
        .and_then(Value::as_str)
        .unwrap_or("X-Signature-256")
        .to_string();
    let encoding = match vars.get("sign_encoding").and_then(Value::as_str) {
        Some(e) => Encoding::parse(e)?,
        None => Encoding::Hex,
    };
    let prefix = vars
        .get("sign_prefix")
        .and_then(Value::as_str)
        .unwrap_or("sha256=")
        .to_string();
    // Seed the id sequence from the clock and the process: it stays unique across requests
    // and across a restart, without a random source.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_nanos() as u64).wrapping_add(std::process::id() as u64))
        .unwrap_or(1);
    Ok(Some(Spec {
        scheme: Scheme::parse(s)?,
        secret_ref: vars
            .get("sign_secret_ref")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        header,
        encoding,
        prefix,
        seq: AtomicU64::new(seed),
    }))
}

/// The value of the `webhook-signature` header: `v1,` + the base64 HMAC-SHA256 of
/// `id.timestamp.body` with `key` (the spec's symmetric scheme).
pub fn standard_signature(key: &[u8], id: &str, ts: &str, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC takes keys of any size");
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(ts.as_bytes());
    mac.update(b".");
    mac.update(body);
    format!(
        "v1,{}",
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    )
}

/// The value of the `hmac` scheme's header: the prefix and the digest of the body, hex or
/// base64, keyed by the raw secret.
pub fn hmac_value(prefix: &str, encoding: Encoding, key: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC takes keys of any size");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    match encoding {
        Encoding::Hex => format!("{prefix}{}", hex::encode(digest)),
        Encoding::Base64 => {
            format!(
                "{prefix}{}",
                base64::engine::general_purpose::STANDARD.encode(digest)
            )
        }
    }
}

/// The key of a standard secret: the base64 after the `whsec_` prefix (the spec's form).
fn standard_key(secret: &str) -> Result<Vec<u8>> {
    let rest = secret
        .strip_prefix("whsec_")
        .ok_or_else(|| anyhow!("the standard signing secret must start with `whsec_`"))?;
    let key = base64::engine::general_purpose::STANDARD
        .decode(rest)
        .with_context(|| "the standard signing secret is not `whsec_` + base64")?;
    if key.is_empty() {
        bail!("the standard signing secret is `whsec_` + base64, but its key is empty");
    }
    Ok(key)
}

/// The next id of a target's standard signing.
fn next_id(target: &str, seq: &AtomicU64) -> String {
    let n = seq.fetch_add(1, Ordering::Relaxed);
    format!("{target}-{:016x}", n)
}

/// Signs `req` in place, over the exact body it will send. The secret reference is resolved
/// now (on every use, as for the other credentials); on failure the error names the setting.
pub fn apply(req: &mut RawRequest, target: &str, spec: &Spec, now: u64) -> Result<()> {
    let secret = crate::secrets::resolve_reference("vars.sign_secret_ref", &spec.secret_ref)?;
    match spec.scheme {
        Scheme::Standard => {
            let key = standard_key(&secret)?;
            let id = next_id(target, &spec.seq);
            let ts = now.to_string();
            let sig = standard_signature(&key, &id, &ts, &req.body);
            req.set_header("Webhook-Id", id);
            req.set_header("Webhook-Timestamp", ts);
            req.set_header("Webhook-Signature", sig);
        }
        Scheme::Hmac => {
            req.set_header(
                &spec.header,
                hmac_value(&spec.prefix, spec.encoding, secret.as_bytes(), &req.body),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
            hex::decode("676a225e230b18d80bed1d8bc12f322f3d8b570fa2ffa470c254ff8c10ef7212")
                .unwrap(),
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
        assert!(
            err(r#"{"sign": "standard", "sign_secret_ref": 1}"#).contains("`vars.sign_secret_ref`")
        );
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
}
