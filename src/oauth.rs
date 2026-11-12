//! OAuth 2.0 access tokens for the plain http kinds: `vars.oauth` selects the token source,
//! and every action of the target carries `Authorization: Bearer <token>`.
//!
//! - `vars.oauth: google` — a Google service-account token through [`crate::gcp`] (the one
//!   Google path in the tree, shared with `pubsub`): `vars.credentials_ref` (a reference to
//!   the key JSON, read on every use) or Application Default Credentials, with the optional
//!   `vars.scope`.
//! - `vars.oauth: client_credentials` — RFC 6749 §4.4: `vars.token_url`, `vars.client_id`
//!   (an identifier: literal or reference) and `vars.client_secret_ref` (a reference only),
//!   with the optional `vars.scope`. The client authenticates with HTTP Basic (§2.3.1:
//!   servers MUST support it, and it keeps the secret out of the body where intermediaries
//!   log and echo more freely).
//!
//! The token is kept per target and reused until a margin before its lifetime ends (the
//! endpoint's `expires_in`, [`DEFAULT_LIFETIME`] when it says none); one fetch is in flight
//! at a time and the waiting senders share its token; new bytes behind the secret reference
//! drop it at once, and so does a 401 from the endpoint (see [`Provider::invalidate`]).
//!
//! Outcomes follow the owner rule: a token-endpoint failure of any kind — transport, 4xx
//! including `invalid_client`, 5xx, a body that is not JSON — says nothing about the
//! message, so it is **transient for every key**, never a refusal. The endpoint's own
//! answers keep the existing expect/refused rules; with `vars.oauth` set, an endpoint 401
//! is transient whatever `expect` says (it is about the credential, not the message).
//!
//! The access token, the client secret and the service-account key never appear in a log
//! line, in an error text, in `/status` or in a URL: the secret exists only as a reference
//! resolved on use, error echoes are scrubbed of it and clipped ([`clip_one_line`]), a body
//! that echoes the access token is scrubbed ([`Provider::scrub`]), and `RawRequest`'s `Debug`
//! already masks credential headers.

use std::net::IpAddr;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

use crate::allow::HostList;
use crate::gcp;
use crate::http::{BaseUrl, Client as HttpClient, RawRequest, Response};
use crate::model::{Kind, RuleDef, StepKind, TargetDef};
use crate::secrets;
use crate::tls::TlsSpec;

/// The lifetime taken when the token endpoint reports no `expires_in` (Google's and Entra's
/// usual hour).
pub const DEFAULT_LIFETIME: Duration = Duration::from_secs(3600);
/// A token is replaced this long before its lifetime ends (halved for shorter lifetimes, so
/// a token is never cached past the point where refreshing would be pointless).
const REFRESH_BEFORE: Duration = Duration::from_secs(60);
/// The longest lifetime trusted from an endpoint, whatever it claims: a token older than
/// this is fetched again anyway, and a wild `expires_in` cannot hold a target on a stale one.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 3600);
/// An echoed `error_description` or body: at most one line of this many characters.
const ECHO_MAX: usize = 200;

/// The `vars` of the oauth option, in the order a load error names the first one found.
const OAUTH_VARS: &[&str] = &[
    "oauth",
    "credentials_ref",
    "scope",
    "token_url",
    "client_id",
    "client_secret_ref",
];

fn var<'a>(vars: &'a Map<String, Value>, k: &str) -> Option<&'a str> {
    vars.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The first oauth setting of `vars`, for the load error on a kind that does not take them
/// (the `sign::first_var` pattern). `credentials_ref` is `pubsub`'s own setting: naming it
/// on that kind would refuse a working target.
pub fn first_var(vars: &Map<String, Value>, kind: Kind) -> Option<&'static str> {
    OAUTH_VARS
        .iter()
        .copied()
        .find(|k| vars.contains_key(*k) && (*k != "credentials_ref" || kind != Kind::PubSub))
}

/// The load check of an oauth target's `vars` (the plain http kinds; called from
/// `TargetDef::from_row`): an unknown grant, a missing setting, a literal where a reference
/// belongs, an unsafe `token_url`, or a second source of `Authorization` is an error naming
/// the setting.
pub fn check(kind: Kind, vars: &Map<String, Value>) -> Result<()> {
    let Some(grant) = var(vars, "oauth") else {
        // Without the grant the other settings would go unused: name the first one found.
        if let Some(k) = OAUTH_VARS
            .iter()
            .copied()
            .find(|k| *k != "oauth" && vars.contains_key(*k))
        {
            bail!("`vars.{k}` sets the oauth token, which needs `vars.oauth` (google or client_credentials)");
        }
        return Ok(());
    };
    // A second source of `Authorization` is ambiguous whatever the grant: refuse, naming
    // both. (A header the *template* renders is caught at load in [`rule_errors`] and at the
    // bind in `Target::prepare` — the target cannot see the template from here.)
    let ambiguous = match kind {
        Kind::HttpBasic => Some("`vars.username` and `vars.password` (the Basic header)"),
        Kind::HttpBearer => Some("`vars.token` (the Bearer header)"),
        Kind::HttpHeader
            if var(vars, "header").is_some_and(|h| h.eq_ignore_ascii_case("authorization")) =>
        {
            Some("`vars.header` (it names `Authorization`)")
        }
        _ => None,
    };
    if let Some(other) = ambiguous {
        bail!(
            "`vars.oauth` and {other}: the Authorization header would carry two claims; keep one"
        );
    }
    match grant {
        "google" => {
            for k in ["token_url", "client_id", "client_secret_ref"] {
                if vars.contains_key(k) {
                    bail!("`vars.{k}` is not used by `vars.oauth=google` (the token comes from the service-account key or ADC)");
                }
            }
            if let Some(r) = var(vars, "credentials_ref") {
                if !secrets::is_reference(r) {
                    bail!("`vars.credentials_ref` must be a reference (`env:NAME` or `file:/path`), never a literal");
                }
            }
            check_scope(vars)?;
        }
        "client_credentials" => {
            if vars.contains_key("credentials_ref") {
                bail!("`vars.credentials_ref` is not used by `vars.oauth=client_credentials` (the token comes from `vars.token_url`)");
            }
            let url = var(vars, "token_url")
                .ok_or_else(|| anyhow!("`vars.oauth=client_credentials` needs `vars.token_url`"))?;
            parse_token_url(url)?;
            var(vars, "client_id")
                .ok_or_else(|| anyhow!("`vars.oauth=client_credentials` needs `vars.client_id`"))?;
            let secret = var(vars, "client_secret_ref").ok_or_else(|| {
                anyhow!("`vars.oauth=client_credentials` needs `vars.client_secret_ref` (an `env:NAME` or `file:/path` reference)")
            })?;
            if !secrets::is_reference(secret) {
                bail!("`vars.client_secret_ref` must be a reference (`env:NAME` or `file:/path`), never a literal");
            }
            check_scope(vars)?;
        }
        other => bail!("`vars.oauth` is `{other}` (use google or client_credentials)"),
    }
    Ok(())
}

fn check_scope(vars: &Map<String, Value>) -> Result<()> {
    if let Some(s) = vars.get("scope") {
        if s.as_str().filter(|s| !s.is_empty()).is_none() {
            bail!("`vars.scope` must be a non-empty string");
        }
    }
    Ok(())
}

/// The token endpoint URL: https, or plain http on loopback only (the way the tests run an
/// in-process token server, and a sidecar broker stays possible); no userinfo; a parse of
/// [`BaseUrl`] with the name of the setting on it. The allow-list is checked where the
/// operator's list is at hand ([`Provider::new`]).
fn parse_token_url(s: &str) -> Result<BaseUrl> {
    let shown = secrets::redact_url(s);
    let base = BaseUrl::parse(s).with_context(|| format!("`vars.token_url` `{shown}`"))?;
    if !base.secure && !is_loopback(&base.host) {
        bail!("`vars.token_url` `{shown}`: https is required (plain http is accepted only on loopback)");
    }
    Ok(base)
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Load refusals of the rule against an oauth target (the `pubsub::rule_errors` pattern,
/// called from `rule::kind_errors` when the target has `vars.oauth`): a template whose
/// header block carries a literal `Authorization:` line is a second source of the header —
/// refused at load, naming the template. A header name only produced by rendering is caught
/// at the bind (`Target::prepare`).
pub fn rule_errors(def: &RuleDef) -> Vec<String> {
    let mut out = Vec::new();
    let mut check_one = |what: String, text: &str| {
        if template_sets_authorization(text) {
            out.push(format!(
                "{what}: the template renders an `Authorization` header while `vars.oauth` sets one too; keep one"
            ));
        }
    };
    check_one("the rule template".to_string(), &def.template);
    for s in &def.reset_steps {
        match &s.kind {
            StepKind::Once | StepKind::Snapshot => {
                if let Some(t) = &s.template {
                    check_one(format!("reset step `{}`", s.name), t);
                }
            }
            StepKind::Stray { list_template, .. } => {
                check_one(format!("reset step `{}`", s.name), list_template);
            }
        }
    }
    out
}

/// Whether the header block of a rendered-request template (between the request line and the
/// blank line, as [`RawRequest::parse`] reads it) carries a line whose name is literally
/// `authorization`. Anything below the blank line is the body and stays unread: a JSON body
/// naming an `Authorization` member is none of our business.
fn template_sets_authorization(text: &str) -> bool {
    let trimmed = text.trim_start_matches(['\r', '\n', ' ', '\t']);
    trimmed
        .lines()
        .skip(1) // past the request line
        // the header block ends at the first blank line; everything below it is body
        .take_while(|l| !l.trim_end_matches('\r').trim().is_empty())
        .any(|l| {
            l.trim_end_matches('\r')
                .split_once(':')
                .is_some_and(|(n, _)| n.trim().eq_ignore_ascii_case("authorization"))
        })
}

/// The token source of a target, when `vars.oauth` names one (the checked form of [`check`],
/// built by `Target::new`).
pub struct Provider {
    name: String,
    grant: Grant,
}

enum Grant {
    /// The service-account/ADC token of [`crate::gcp`], its own cache and expiry policy
    /// (Google does not report `expires_in` to this crate, so `gcp` assumes its hour).
    Google(gcp::Auth),
    ClientCredentials(Box<ClientCredentials>),
}

struct ClientCredentials {
    /// The full token URL, for the error messages and the log line: the URL is a setting the
    /// template may see anyway, not a secret.
    shown: String,
    /// The request line's target: the URL's path, `/` when it has none.
    path: String,
    client: HttpClient,
    /// `vars.client_id`: an identifier, literal or reference (like `http.basic`'s username).
    client_id: String,
    /// `vars.client_secret_ref`: a reference, resolved on every use and never kept.
    client_secret_ref: String,
    scope: Option<String>,
    timeout: Duration,
    cache: Mutex<Option<Cached>>,
    /// The token most recently handed out, kept after [`Provider::invalidate`] for
    /// scrubbing alone: the endpoint can echo it back in the error of the next request.
    issued: Mutex<Option<String>>,
    flight: AsyncMutex<()>,
}

struct Cached {
    token: String,
    /// Over the resolved `client_id` and secret: new bytes behind either reference drop it.
    hash: [u8; 32],
    until: Instant,
}

/// `vars.oauth` as written, for a message that names the setting.
pub fn grant_name(vars: &Map<String, Value>) -> &'static str {
    match var(vars, "oauth") {
        Some("google") => "google",
        _ => "client_credentials",
    }
}

impl Provider {
    pub fn new(def: &TargetDef, allowed: Option<&HostList>) -> Result<Option<Provider>> {
        let Some(grant) = var(&def.vars, "oauth") else {
            return Ok(None); // `check` refused any leftover oauth setting already
        };
        let provider = match grant {
            "google" => {
                let scopes = var(&def.vars, "scope")
                    .map(|s| s.split_ascii_whitespace().map(str::to_string).collect())
                    .unwrap_or_default();
                let source = match var(&def.vars, "credentials_ref") {
                    Some(r) => gcp::Source::Key(r.to_string()),
                    None => gcp::Source::Adc,
                };
                Provider {
                    name: def.name.clone(),
                    grant: Grant::Google(gcp::Auth::with_scopes(source, scopes)),
                }
            }
            _ => {
                let base = parse_token_url(var(&def.vars, "token_url").unwrap_or_default())?;
                // The token host is an outbound host like any endpoint: the operator's list
                // must allow it. (The Google token host is fixed by the tool, as `pubsub`'s
                // service host is, and is not on the list's hands.)
                if let Some(list) = allowed {
                    crate::target::check_host(list, &base.host, base.port)
                        .with_context(|| "vars.token_url")?;
                }
                // The token flow verifies TLS on the built-in roots, never on the target's
                // `vars.tls`: a `server_name` pinned for the API host must not be reused to
                // verify the token host. (A private CA for a token host is not supported yet.)
                let client = HttpClient::new(base.clone(), &TlsSpec::parse(None)?)?;
                let path = if base.base.is_empty() {
                    "/".to_string()
                } else {
                    base.base.clone()
                };
                Provider {
                    name: def.name.clone(),
                    grant: Grant::ClientCredentials(Box::new(ClientCredentials {
                        shown: base.url(),
                        path,
                        client,
                        client_id: var(&def.vars, "client_id").unwrap_or_default().to_string(),
                        client_secret_ref: var(&def.vars, "client_secret_ref")
                            .unwrap_or_default()
                            .to_string(),
                        scope: var(&def.vars, "scope").map(str::to_string),
                        timeout: def.timeout,
                        cache: Mutex::new(None),
                        issued: Mutex::new(None),
                        flight: AsyncMutex::new(()),
                    })),
                }
            }
        };
        Ok(Some(provider))
    }

    /// The value for the `Authorization` header of one request.
    pub async fn authorization(&self) -> Result<String> {
        match &self.grant {
            Grant::Google(a) => a.token().await.map(|t| format!("Bearer {t}")).map_err(|e| {
                warn!(target = %self.name, "oauth token: {e:#}");
                anyhow!("oauth token: {e:#}")
            }),
            Grant::ClientCredentials(c) => c.authorization().await,
        }
    }

    /// Drops the cached token, so the next request fetches a new one (the endpoint answered
    /// 401: the credential is what it refused, whatever this message was).
    pub fn invalidate(&self) {
        match &self.grant {
            Grant::Google(a) => a.invalidate(),
            Grant::ClientCredentials(c) => *c.lock() = None,
        }
    }

    /// Masks the cached token in a text the endpoint's answer produced: a server that echoes
    /// the request cannot carry the token into a log line or `last_error`.
    pub fn scrub(&self, s: &str) -> String {
        let masked = match &self.grant {
            Grant::Google(a) => a.scrub(s),
            Grant::ClientCredentials(c) => {
                let mut s = match &*c.lock() {
                    Some(e) => s.replace(&e.token, "***"),
                    None => s.to_string(),
                };
                if let Some(t) = &*c.last() {
                    s = s.replace(t.as_str(), "***");
                }
                s
            }
        };
        mask_token(&masked)
    }
}

impl std::fmt::Debug for Provider {
    /// What a `Debug` of a target may show: the grant, where the token comes from, and
    /// whether one is cached. Never the token, the client secret or the key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (grant, at, cached) = match &self.grant {
            Grant::Google(a) => ("google", "googleapis.com", a.cached()),
            Grant::ClientCredentials(c) => {
                ("client_credentials", c.shown.as_str(), c.lock().is_some())
            }
        };
        f.debug_struct("Provider")
            .field("target", &self.name)
            .field("grant", &grant)
            .field("token endpoint", &at)
            .field("cached", &cached)
            .finish()
    }
}

impl ClientCredentials {
    async fn authorization(&self) -> Result<String> {
        let (id, secret) = self.resolve()?;
        let hash = digest(&id, &secret);
        if let Some(token) = self.fresh(&hash) {
            return Ok(token);
        }
        let _flight = self.flight.lock().await;
        // Read again: a rotation that landed while we waited must not be hidden behind the
        // entry the other sender fetched for the old bytes.
        let (id, secret) = self.resolve()?;
        let hash = digest(&id, &secret);
        if let Some(token) = self.fresh(&hash) {
            return Ok(token);
        }
        let (token, lifetime) = self.fetch(&id, &secret).await.map_err(|e| {
            warn!(target = %self.shown, "oauth token: {e:#}");
            anyhow!("oauth token: {e:#}")
        })?;
        let usable = lifetime
            .checked_sub(REFRESH_BEFORE.min(lifetime / 2))
            .unwrap_or_default();
        *self.last() = Some(token.clone());
        if !usable.is_zero() {
            *self.lock() = Some(Cached {
                token: token.clone(),
                hash,
                until: Instant::now() + usable,
            });
        }
        Ok(format!("Bearer {token}"))
    }

    /// The references resolved now (as the other credentials of the tree are).
    fn resolve(&self) -> Result<(String, String)> {
        let id = secrets::resolve(&self.client_id).with_context(|| "vars.client_id")?;
        let secret = secrets::resolve_reference("vars.client_secret_ref", &self.client_secret_ref)?;
        Ok((id, secret))
    }

    /// The cached header value, when it belongs to these credential bytes and is not due.
    fn fresh(&self, hash: &[u8; 32]) -> Option<String> {
        let guard = self.lock();
        let c = guard.as_ref()?;
        (c.hash == *hash && Instant::now() < c.until).then(|| format!("Bearer {}", c.token))
    }

    /// One token request: the RFC 6749 §4.1.3 form over the existing http client, the client
    /// authenticated with HTTP Basic. Every failure is an `Err` for the caller to make
    /// transient, naming its cause; nothing here refuses a key.
    async fn fetch(&self, id: &str, secret: &str) -> Result<(String, Duration)> {
        let mut body = "grant_type=client_credentials".to_string();
        if let Some(scope) = &self.scope {
            body.push_str(&format!("&scope={}", form_encode(scope)));
        }
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("{id}:{secret}"));
        let mut req = RawRequest {
            method: "POST".into(),
            target: self.path.clone(),
            headers: Vec::new(),
            body: body.into_bytes(),
        };
        req.set_header("Host", self.client.base.host_header());
        req.set_header(
            "User-Agent",
            format!("lazywf/{}", env!("CARGO_PKG_VERSION")),
        );
        req.set_header("Accept", "application/json".to_string());
        req.set_header(
            "Content-Type",
            "application/x-www-form-urlencoded".to_string(),
        );
        req.set_header("Authorization", format!("Basic {basic}"));
        let resp = self
            .client
            .send(&req, self.timeout)
            .await
            .with_context(|| format!("token endpoint POST {}", self.shown))?;
        let parsed: Option<Value> = serde_json::from_slice(&resp.body).ok();
        // What a server may echo of the request (the secret in its body form, its percent
        // encoding, and the Basic header), and of an access token it handed out in an answer
        // that then went wrong, never leaves this function into a message.
        let form = form_encode(secret);
        let answered = parsed
            .as_ref()
            .and_then(|v| v.get("access_token"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let previously = self.last().clone();
        let echo = |s: &str| {
            let mut s = s
                .replace(secret, "***")
                .replace(&form, "***")
                .replace(&basic, "***");
            if let Some(t) = answered.as_deref() {
                s = s.replace(t, "***");
            }
            if let Some(t) = previously.as_deref() {
                s = s.replace(t, "***");
            }
            mask_token(&s)
        };
        if !(200..300).contains(&resp.status) {
            let mut msg = format!("token endpoint at {}: status {}", self.shown, resp.status);
            if let Some(v) = &parsed {
                if let Some(e) = v.get("error").and_then(Value::as_str) {
                    msg.push_str(&format!(", error `{e}`"));
                }
                if let Some(d) = v.get("error_description").and_then(Value::as_str) {
                    msg.push_str(&format!(" ({})", clip_one_line(&echo(d))));
                }
            }
            if parsed.is_none() {
                msg.push_str(&format!(": {}", clip_one_line(&echo(&body_snippet(&resp)))));
            }
            bail!("{msg}");
        }
        let v = parsed.ok_or_else(|| {
            anyhow!(
                "token endpoint at {}: the answer is not JSON: {}",
                self.shown,
                clip_one_line(&echo(&body_snippet(&resp)))
            )
        })?;
        if let Some(tt) = v
            .get("token_type")
            .and_then(Value::as_str)
            .filter(|t| !t.eq_ignore_ascii_case("bearer"))
        {
            bail!(
                "token endpoint at {}: token_type `{tt}` (lazywf sends Bearer tokens only)",
                self.shown
            );
        }
        let token = v
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "token endpoint at {}: the answer holds no access_token",
                    self.shown
                )
            })?;
        let lifetime = match v.get("expires_in") {
            None | Some(Value::Null) => DEFAULT_LIFETIME,
            Some(Value::Number(n)) => parse_expires(&n.to_string())?,
            Some(Value::String(s)) => parse_expires(s.trim())?,
            Some(_) => bail!(
                "token endpoint at {}: `expires_in` is not a number of seconds",
                self.shown
            ),
        };
        Ok((token.to_string(), lifetime))
    }

    fn lock(&self) -> MutexGuard<'_, Option<Cached>> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The token most recently handed out, kept for scrubbing (see the field's note).
    fn last(&self) -> MutexGuard<'_, Option<String>> {
        self.issued.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn parse_expires(s: &str) -> Result<Duration, anyhow::Error> {
    let secs: u64 = s
        .parse()
        .with_context(|| "token endpoint: `expires_in` is not a number of seconds")?;
    Ok(Duration::from_secs(secs).min(MAX_LIFETIME))
}

/// The body for an error message: as short as the http path makes it, and one line.
fn body_snippet(resp: &Response) -> String {
    resp.body_snippet()
}

/// One line, at most [`ECHO_MAX`] characters: an echoed text may inform, not flood.
fn clip_one_line(s: &str) -> String {
    let one: String = s
        .lines()
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    match one.char_indices().nth(ECHO_MAX) {
        Some((i, _)) => format!("{}…", &one[..i]),
        None => one,
    }
}

/// Masks the value of any `access_token` member of an echoed text: an answer that is not the
/// JSON we expect (a form body, a page that repeats what it was given) can still carry the
/// token it was asked to hand out, and no token goes into a message or a log line.
fn mask_token(text: &str) -> String {
    const NAME: &str = "access_token";
    const VALUE: &[char] = &['-', '.', '_', '~', '+', '/', '%', ':'];
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        match rest.to_ascii_lowercase().find(NAME) {
            Some(i) => {
                out.push_str(&rest[..i]);
                out.push_str(&rest[i..i + NAME.len()]);
                rest = &rest[i + NAME.len()..];
                let lead = rest.len()
                    - rest
                        .trim_start_matches([' ', '\t', ':', '=', '"', '\''])
                        .len();
                out.push_str(&rest[..lead]);
                rest = &rest[lead..];
                let end = rest
                    .find(|c: char| !(c.is_alphanumeric() || VALUE.contains(&c)))
                    .unwrap_or(rest.len());
                if end > 0 {
                    out.push_str("***");
                }
                rest = &rest[end..];
            }
            None => {
                out.push_str(rest);
                break;
            }
        }
    }
    out
}

/// The `application/x-www-form-urlencoded` form: only the unreserved set stands literal.
fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn digest(client_id: &str, secret: &str) -> [u8; 32] {
    Sha256::digest(format!("{client_id}\n{secret}").as_bytes()).into()
}

// ---------------------------------------------------------------------------------------------
// tests

#[cfg(test)]
mod tests {
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
        assert!(
            !s.contains(&base64::engine::general_purpose::STANDARD.encode(format!("cid:{SECRET}"))),
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
            &vars(
                r#"{"oauth": "google", "scope": "https://www.googleapis.com/auth/spreadsheets"}"#
            )
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
            unknown.contains("use google or client_credentials"),
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
            dangling.contains("needs `vars.oauth` (google or client_credentials)"),
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
                r#"{"access_token": "{n}", "token_type": "Bearer", "expires_in": 3600}"#
                    .to_string(),
            )));
            let delay = Arc::new(Mutex::new(Duration::ZERO));
            let (h, r, a, d) = (
                hits.clone(),
                requests.clone(),
                answer.clone(),
                delay.clone(),
            );
            tokio::spawn(async move {
                loop {
                    let (mut s, _) = listener.accept().await.unwrap();
                    let (h, r, a, d) = (h.clone(), r.clone(), a.clone(), d.clone());
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
                        let (delay, (status, answer)) =
                            (*d.lock().unwrap(), a.lock().unwrap().clone());
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
            }
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
        let want = base64::engine::general_purpose::STANDARD.encode("cid:s3cr/t");
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
}
