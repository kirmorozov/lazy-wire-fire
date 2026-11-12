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
//! - `vars.oauth: jwt_bearer` — RFC 7523 §2.1 (Salesforce): `vars.token_url`, `vars.client_id`
//!   (the assertion's `iss`: Salesforce's consumer key), `vars.subject` (`sub`), `vars.audience`
//!   (`aud`, required: the login host for Salesforce) and `vars.signing_key_ref` (a reference
//!   only: a PEM RSA private key, PKCS#8 or PKCS#1), with the optional `vars.scope`. The
//!   assertion is signed RS256 and lives 180 s (Salesforce's own cap); the token request
//!   carries no client authentication (§2.1 needs none, Salesforce takes none).
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
use crate::target::var;
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
    "subject",
    "audience",
    "signing_key_ref",
];

/// The lifetime of a JWT bearer assertion: Salesforce accepts `exp` at most 3 minutes past
/// `iat` (RFC 7523 leaves the lifetime to the client, so the shortest server limit in the
/// field rules).
const ASSERTION_LIFETIME: u64 = 180;
/// The `grant_type` of the JWT bearer grant (RFC 7523 §2.1).
const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

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
            bail!("`vars.{k}` sets the oauth token, which needs `vars.oauth` (google, client_credentials or jwt_bearer)");
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
            for k in [
                "token_url",
                "client_id",
                "client_secret_ref",
                "subject",
                "audience",
                "signing_key_ref",
            ] {
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
            for k in ["subject", "audience", "signing_key_ref"] {
                if vars.contains_key(k) {
                    bail!("`vars.{k}` is not used by `vars.oauth=client_credentials` (the signed assertion belongs to `vars.oauth=jwt_bearer`)");
                }
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
        "jwt_bearer" => {
            for k in ["credentials_ref", "client_secret_ref"] {
                if vars.contains_key(k) {
                    bail!("`vars.{k}` is not used by `vars.oauth=jwt_bearer` (the token request authenticates with the signed assertion alone)");
                }
            }
            let url = var(vars, "token_url")
                .ok_or_else(|| anyhow!("`vars.oauth=jwt_bearer` needs `vars.token_url`"))?;
            parse_token_url(url)?;
            var(vars, "client_id").ok_or_else(|| {
                anyhow!("`vars.oauth=jwt_bearer` needs `vars.client_id` (the assertion's `iss`: the consumer key)")
            })?;
            var(vars, "subject").ok_or_else(|| {
                anyhow!("`vars.oauth=jwt_bearer` needs `vars.subject` (the assertion's `sub`: the user the token acts for)")
            })?;
            var(vars, "audience").ok_or_else(|| {
                anyhow!("`vars.oauth=jwt_bearer` needs `vars.audience` (the assertion's `aud`: the login host for Salesforce)")
            })?;
            let key = var(vars, "signing_key_ref").ok_or_else(|| {
                anyhow!("`vars.oauth=jwt_bearer` needs `vars.signing_key_ref` (an `env:NAME` or `file:/path` reference to the PEM private key)")
            })?;
            if !secrets::is_reference(key) {
                bail!("`vars.signing_key_ref` must be a reference (`env:NAME` or `file:/path`), never a literal");
            }
            check_scope(vars)?;
        }
        other => bail!("`vars.oauth` is `{other}` (use google, client_credentials or jwt_bearer)"),
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
    JwtBearer(Box<JwtBearer>),
}

struct ClientCredentials {
    /// The token endpoint's origin (scheme, host, port) — never its path: the full URL stays
    /// out of error messages, the warn line and the `Debug`, the way a secret endpoint is
    /// named. It is still a config the template could see, not a secret itself.
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

/// `vars.oauth=jwt_bearer` (RFC 7523 §2.1): the token endpoint takes a signed assertion in
/// place of client credentials, and the cache of a `client_credentials` target wraps it —
/// same single-flight, same secret-hash drop, same 401 drop, same outcome rule.
struct JwtBearer {
    /// The token endpoint's origin (scheme, host, port) — never its path (see the same
    /// field of [`ClientCredentials`]).
    shown: String,
    /// The request line's target: the URL's path, `/` when it has none.
    path: String,
    client: HttpClient,
    /// `vars.client_id`: the assertion's `iss` (Salesforce's consumer key). An identifier:
    /// literal or reference, like `http.basic`'s username.
    issuer: String,
    /// `vars.subject`: the assertion's `sub`, the user the token acts for.
    subject: String,
    /// `vars.audience`: the assertion's `aud` (the login host for Salesforce).
    audience: String,
    /// `vars.signing_key_ref`: a reference to the PEM private key (PKCS#8 or PKCS#1 RSA),
    /// resolved on every use and never kept.
    signing_key_ref: String,
    scope: Option<String>,
    timeout: Duration,
    cache: Mutex<Option<Cached>>,
    /// The token most recently handed out, kept after [`Provider::invalidate`] for
    /// scrubbing alone: the endpoint can echo it back in the error of the next request.
    issued: Mutex<Option<String>>,
    flight: AsyncMutex<()>,
}

/// `vars.oauth` as written, for a message that names the setting.
pub fn grant_name(vars: &Map<String, Value>) -> &'static str {
    match var(vars, "oauth") {
        Some("google") => "google",
        Some("jwt_bearer") => "jwt_bearer",
        _ => "client_credentials",
    }
}

/// The token endpoint of a target that names one: the checked URL, its origin for
/// messages, the request line's target, and the client that speaks to it. Shared by the
/// two grants that take a `vars.token_url`.
fn token_endpoint(
    def: &TargetDef,
    allowed: Option<&HostList>,
) -> Result<(String, String, HttpClient)> {
    let base = parse_token_url(var(&def.vars, "token_url").unwrap_or_default())?;
    // The token host is an outbound host like any endpoint: the operator's list
    // must allow it. (The Google token host is fixed by the tool, as `pubsub`'s
    // service host is, and is not on the list's hands.)
    if let Some(list) = allowed {
        crate::target::check_host(list, &base.host, base.port).with_context(|| "vars.token_url")?;
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
    Ok((base.origin(), path, client))
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
            "jwt_bearer" => {
                let (shown, path, client) = token_endpoint(def, allowed)?;
                Provider {
                    name: def.name.clone(),
                    grant: Grant::JwtBearer(Box::new(JwtBearer {
                        shown,
                        path,
                        client,
                        issuer: var(&def.vars, "client_id").unwrap_or_default().to_string(),
                        subject: var(&def.vars, "subject").unwrap_or_default().to_string(),
                        audience: var(&def.vars, "audience").unwrap_or_default().to_string(),
                        signing_key_ref: var(&def.vars, "signing_key_ref")
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
            _ => {
                let (shown, path, client) = token_endpoint(def, allowed)?;
                Provider {
                    name: def.name.clone(),
                    grant: Grant::ClientCredentials(Box::new(ClientCredentials {
                        shown,
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
            Grant::JwtBearer(j) => j.authorization().await,
        }
    }

    /// Drops the cached token, so the next request fetches a new one (the endpoint answered
    /// 401: the credential is what it refused, whatever this message was).
    pub fn invalidate(&self) {
        match &self.grant {
            Grant::Google(a) => a.invalidate(),
            Grant::ClientCredentials(c) => *c.lock() = None,
            Grant::JwtBearer(j) => *j.lock() = None,
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
            Grant::JwtBearer(j) => {
                let mut s = match &*j.lock() {
                    Some(e) => s.replace(&e.token, "***"),
                    None => s.to_string(),
                };
                if let Some(t) = &*j.last() {
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
            Grant::JwtBearer(j) => ("jwt_bearer", j.shown.as_str(), j.lock().is_some()),
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
        // RFC 6749 §2.3.1: the two are each form-encoded before the colon and the whole.
        let basic = base64::engine::general_purpose::STANDARD.encode(format!(
            "{}:{}",
            form_encode(id),
            form_encode(secret)
        ));
        let resp = post_token(
            &self.client,
            &self.path,
            &self.shown,
            self.timeout,
            body,
            Some(format!("Basic {basic}")),
        )
        .await?;
        // What a server may echo of the request (the secret in its body form, its percent
        // encoding, and the Basic header), and of an access token it handed out in an answer
        // that then went wrong, never leaves this function into a message.
        let scrub = vec![secret.to_string(), form_encode(secret), basic];
        let previously = self.last().clone();
        parse_token_answer(&resp, &self.shown, &scrub, previously.as_deref())
    }

    fn lock(&self) -> MutexGuard<'_, Option<Cached>> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The token most recently handed out, kept for scrubbing (see the field's note).
    fn last(&self) -> MutexGuard<'_, Option<String>> {
        self.issued.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl JwtBearer {
    async fn authorization(&self) -> Result<String> {
        let (iss, sub, aud, key) = self.resolve()?;
        let hash = digest_assertion(&iss, &sub, &aud, key.as_bytes());
        if let Some(token) = self.fresh(&hash) {
            return Ok(token);
        }
        let _flight = self.flight.lock().await;
        // Read again: a key rotation that landed while we waited must not be hidden behind
        // the entry the other sender fetched for the old bytes.
        let (iss, sub, aud, key) = self.resolve()?;
        let hash = digest_assertion(&iss, &sub, &aud, key.as_bytes());
        if let Some(token) = self.fresh(&hash) {
            return Ok(token);
        }
        let (token, lifetime) = self.fetch(&iss, &sub, &aud, &key).await.map_err(|e| {
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
    fn resolve(&self) -> Result<(String, String, String, String)> {
        let iss = secrets::resolve(&self.issuer).with_context(|| "vars.client_id")?;
        let sub = secrets::resolve(&self.subject).with_context(|| "vars.subject")?;
        let aud = secrets::resolve(&self.audience).with_context(|| "vars.audience")?;
        let key = secrets::resolve_reference("vars.signing_key_ref", &self.signing_key_ref)
            .with_context(|| "vars.signing_key_ref")?;
        Ok((iss, sub, aud, key))
    }

    /// The cached header value, when it belongs to these credential bytes and is not due.
    fn fresh(&self, hash: &[u8; 32]) -> Option<String> {
        let guard = self.lock();
        let c = guard.as_ref()?;
        (c.hash == *hash && Instant::now() < c.until).then(|| format!("Bearer {}", c.token))
    }

    /// One token request: the RFC 7523 §2.1 form (the signed assertion in place of client
    /// credentials, no client authentication header). Every failure is an `Err` for the
    /// caller to make transient, naming its cause; nothing here refuses a key.
    async fn fetch(
        &self,
        iss: &str,
        sub: &str,
        aud: &str,
        key_pem: &str,
    ) -> Result<(String, Duration)> {
        let assertion = build_assertion(iss, sub, aud, key_pem.as_bytes())?;
        let mut body = format!(
            "grant_type={}&assertion={}",
            form_encode(JWT_BEARER_GRANT),
            form_encode(&assertion)
        );
        if let Some(scope) = &self.scope {
            body.push_str(&format!("&scope={}", form_encode(scope)));
        }
        // RFC 7523 §2.1 needs no client authentication (and Salesforce takes none): the
        // assertion identifies the client. No `Authorization` header goes out.
        let resp = post_token(
            &self.client,
            &self.path,
            &self.shown,
            self.timeout,
            body,
            None,
        )
        .await?;
        // The assertion is the credential for its lifetime, and the key is the secret behind
        // it: neither in any form they were sent, nor an answer that went wrong, may leave
        // this function into a message.
        let scrub = vec![assertion, key_pem.to_string()];
        let previously = self.last().clone();
        parse_token_answer(&resp, &self.shown, &scrub, previously.as_deref())
    }

    fn lock(&self) -> MutexGuard<'_, Option<Cached>> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The token most recently handed out, kept for scrubbing (see the field's note).
    fn last(&self) -> MutexGuard<'_, Option<String>> {
        self.issued.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The POST every token request is: the form body to the endpoint's path over the existing
/// http client, carrying no credential beyond `auth` (the Basic header of
/// `client_credentials`; `jwt_bearer` sends none). A transport failure is an `Err` naming
/// the endpoint by origin.
async fn post_token(
    client: &HttpClient,
    path: &str,
    shown: &str,
    timeout: Duration,
    body: String,
    auth: Option<String>,
) -> Result<Response> {
    let mut req = RawRequest {
        method: "POST".into(),
        target: path.to_string(),
        headers: Vec::new(),
        body: body.into_bytes(),
    };
    req.set_header("Host", client.base.host_header());
    req.set_header(
        "User-Agent",
        format!("lazywf/{}", env!("CARGO_PKG_VERSION")),
    );
    req.set_header("Accept", "application/json".to_string());
    req.set_header(
        "Content-Type",
        "application/x-www-form-urlencoded".to_string(),
    );
    if let Some(a) = auth {
        req.set_header("Authorization", a);
    }
    client
        .send(&req, timeout)
        .await
        .with_context(|| format!("token endpoint POST {shown}"))
}

/// The answer to a token request, shared by both grants: the status, the JSON, the
/// `access_token` with its `token_type` and `expires_in`. Every failure names its cause and
/// is an `Err` for the caller to make transient. `scrub` holds every form of a credential
/// the endpoint may echo back (the secret, its percent encoding, the Basic header, the
/// assertion, the key): none of them, nor a token this endpoint handed out before, may
/// survive into the message.
fn parse_token_answer(
    resp: &Response,
    shown: &str,
    scrub: &[String],
    previously: Option<&str>,
) -> Result<(String, Duration)> {
    let parsed: Option<Value> = serde_json::from_slice(&resp.body).ok();
    let answered = parsed
        .as_ref()
        .and_then(|v| v.get("access_token"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let echo = |s: &str| {
        let mut s = s.to_string();
        for secret in scrub {
            if !secret.is_empty() {
                s = s.replace(secret, "***");
            }
        }
        if let Some(t) = answered.as_deref() {
            s = s.replace(t, "***");
        }
        if let Some(t) = previously {
            s = s.replace(t, "***");
        }
        mask_token(&s)
    };
    if !(200..300).contains(&resp.status) {
        let mut msg = format!("token endpoint at {shown}: status {}", resp.status);
        if let Some(v) = &parsed {
            if let Some(e) = v.get("error").and_then(Value::as_str) {
                msg.push_str(&format!(", error `{e}`"));
            }
            if let Some(d) = v.get("error_description").and_then(Value::as_str) {
                msg.push_str(&format!(" ({})", clip_one_line(&echo(d))));
            }
        }
        if parsed.is_none() {
            msg.push_str(&format!(": {}", clip_one_line(&echo(&body_snippet(resp)))));
        }
        bail!("{msg}");
    }
    let v = parsed.ok_or_else(|| {
        anyhow!(
            "token endpoint at {shown}: the answer is not JSON: {}",
            clip_one_line(&echo(&body_snippet(resp)))
        )
    })?;
    if let Some(tt) = v
        .get("token_type")
        .and_then(Value::as_str)
        .filter(|t| !t.eq_ignore_ascii_case("bearer"))
    {
        bail!("token endpoint at {shown}: token_type `{tt}` (lazywf sends Bearer tokens only)");
    }
    let token = v
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| anyhow!("token endpoint at {shown}: the answer holds no access_token"))?;
    let lifetime = match v.get("expires_in") {
        None | Some(Value::Null) => DEFAULT_LIFETIME,
        Some(Value::Number(n)) => parse_expires(&n.to_string())?,
        Some(Value::String(s)) => parse_expires(s.trim())?,
        Some(_) => bail!("token endpoint at {shown}: `expires_in` is not a number of seconds"),
    };
    Ok((token.to_string(), lifetime))
}

/// The RFC 7523 §2.1 assertion: header `{"alg":"RS256","typ":"JWT"}` and the claims `iss`,
/// `sub`, `aud`, `iat` (now) and `exp` (now + [`ASSERTION_LIFETIME`]: Salesforce's own cap),
/// base64url without padding, signed RS256. The key is the resolved PEM bytes of
/// `vars.signing_key_ref` — PKCS#8 and PKCS#1 both read ([`PrivateKeyDer::from_pem_slice`]
/// of rustls's pki-types). A key that does not parse, or is not RSA, is an error naming the
/// cause: the reference is resolved on use, as the other secrets are, so the caller turns
/// it transient.
fn build_assertion(iss: &str, sub: &str, aud: &str, key_pem: &[u8]) -> Result<String> {
    use rustls::crypto::ring::sign::any_supported_type;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::SignatureScheme;
    let der = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|e| anyhow!("`vars.signing_key_ref` is not a PEM private key: {e}"))?;
    let key = any_supported_type(&der)
        .map_err(|e| anyhow!("`vars.signing_key_ref` holds a key this tool cannot use: {e}"))?;
    let signer = key
        .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
        .ok_or_else(|| {
            anyhow!("`vars.signing_key_ref` is not an RSA key (the JWT bearer assertion is signed RS256)")
        })?;
    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
    let now = crate::time::unix_now();
    let claims = serde_json::json!({
        "iss": iss,
        "sub": sub,
        "aud": aud,
        "iat": now,
        "exp": now + ASSERTION_LIFETIME,
    });
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&claims).with_context(|| "the assertion claims")?);
    let signing_input = format!("{header}.{claims}");
    let sig = signer
        .sign(signing_input.as_bytes())
        .map_err(|e| anyhow!("signing the assertion RS256: {e}"))?;
    Ok(format!(
        "{signing_input}.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&sig[..])
    ))
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

/// The cache identity of the JWT bearer grant: the resolved key bytes and the claims that
/// name who the assertion speaks for. New bytes behind the reference, or a new `iss`/`sub`/
/// `aud`, drop the entry at once.
fn digest_assertion(iss: &str, sub: &str, aud: &str, key: &[u8]) -> [u8; 32] {
    let mut d = Sha256::new();
    d.update(format!("{iss}\n{sub}\n{aud}").as_bytes());
    d.update(key);
    d.finalize().into()
}

// ---------------------------------------------------------------------------------------------
// tests

#[cfg(test)]
mod tests;

/// The tests' verifier of the RFC 7523 §2.1 form: what a token endpoint must do with an
/// assertion — check the signature RS256 with the public key and read the claims — done
/// here with `ring` (a dev-dependency: the same one the lock carries through rustls) so
/// the tests pin the shape of the credential, not only its presence.
#[cfg(test)]
pub(crate) mod jwt_check;
