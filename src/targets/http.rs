//! The four plain http kinds: `http`, `http.basic`, `http.bearer` and `http.header` —
//! one family in one file, because they share everything but the credential.
//!
//! Not to be confused with [`crate::http`]: that is the http CLIENT (the transport this
//! family travels over, and the `http.s3` and aws kinds too); this module is the four
//! KINDS. Only what is theirs is decided here: the `vars` need-checks at load ([`check`])
//! and the credential headers of a rendered request ([`authorize`]). The request line, the
//! send and the `expect` verdict are the generic http path in [`crate::target`] — shared
//! transport of nine kinds. Webhook signing ([`crate::sign`]) and the OAuth 2.0 token
//! ([`crate::oauth`]) are options of these kinds too, but the guards that bind them to the
//! four live in `TargetDef::from_row` ([`crate::model`]) and both are applied in
//! `Target::prepare`, shared with the other kinds that use the http transport.

use anyhow::Result;
use base64::Engine as _;

use crate::http::RawRequest;
use crate::model::{Kind, TargetDef};
use crate::target::def_var;

/// Load checks for the plain http kinds (from `TargetDef::from_row`): each credential
/// kind names the `vars` its credential is made of. `need` is the caller's closure that
/// requires one `vars` entry; `Kind::Http` itself needs nothing — its URL can be the
/// secret (an endpoint reference, checked by the guard in `TargetDef::from_row`).
pub fn check(kind: Kind, need: &impl Fn(&str) -> Result<()>) -> Result<()> {
    match kind {
        Kind::HttpBasic => {
            need("username")?;
            need("password")?;
        }
        Kind::HttpBearer => need("token")?,
        Kind::HttpHeader => {
            need("header")?;
            need("value")?;
        }
        _ => {}
    }
    Ok(())
}

/// The credential headers of `def`'s kind on a rendered request (from `Target::prepare`,
/// after the shared prefix and Host lines, before the oauth token and the signature).
/// `get` is the caller's closure that resolves one `vars` entry: a reference is read
/// fresh on every request, as the other credentials are. The plain `Kind::Http` carries
/// no credential of its own here.
pub fn authorize(
    def: &TargetDef,
    req: &mut RawRequest,
    get: &impl Fn(&str) -> Result<String>,
) -> Result<()> {
    match def.kind {
        Kind::HttpBasic => {
            let token = base64::engine::general_purpose::STANDARD.encode(format!(
                "{}:{}",
                get("username")?,
                get("password")?
            ));
            req.set_header("Authorization", format!("Basic {token}"));
        }
        Kind::HttpBearer => req.set_header("Authorization", format!("Bearer {}", get("token")?)),
        Kind::HttpHeader => {
            let name = def_var(def, "header").unwrap_or_default().to_string();
            req.set_header(&name, get("value")?);
        }
        _ => {}
    }
    Ok(())
}
