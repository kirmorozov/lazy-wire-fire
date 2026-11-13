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
#[path = "tests/sign.rs"]
mod tests;
