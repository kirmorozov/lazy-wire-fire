//! The `nats` kind: one JetStream publish per message of an action, to the subject of the
//! endpoint. Client: async-nats (pure Rust, ring TLS; `jetstream` and `nkeys` features — see
//! Cargo.toml).
//!
//! # Endpoint and credentials
//!
//! `nats://<host>[:port][,<host>[:port]…]/<subject>`, or `tls://` for TLS (default port 4222).
//! One endpoint per target: one server set of a cluster and the subject it writes to (a batch is
//! answered per item, which the engine sends to a single endpoint, as for kafka, sqs and kinesis).
//! No credentials in the URL. Auth: `vars.token`, or `vars.username` with `vars.password`, or
//! `vars.nkey` — the seed of an account user, with `vars.jwt` when the server wants the user's
//! account token too. Each of those is a secret and takes a reference (`env:NAME`, `file:/path`),
//! never a literal; without any of them the connection is anonymous, which is what a development
//! server offers.
//!
//! Streams belong to the operator: the target never creates, purges or deletes one. A subject no
//! stream binds is refused rather than dropped — a JetStream publish to such a subject is answered
//! with no responders and stores nothing, and an engine that believed otherwise would stop
//! retrying a row nobody ever has.
//!
//! # The rendered action
//!
//! One action is one publish of a batch of messages; the client sends them one after another, each
//! awaited before the next starts, so the stream's sequence order is the request's row order:
//!
//! ```text
//! POST / HTTP/1.1
//! Content-Type: application/json
//!
//! {"messages": [{"value": …, "headers": {"op": "upsert"}}, {"value": …}]}
//! ```
//!
//! Message i takes the document key `keys[i]`, its rendered `value` (one JSON value, serialized
//! compactly) and optional `headers` (string to string; `Nats-Msg-Id` is the client's to set). A
//! message carries only `value` and `headers` — the subject comes from the endpoint, the key from
//! the row. With no keys (a `once` reset step) nothing is rendered to send.
//!
//! `vars.dedup` asks for JetStream's de-duplication window: `content` sends the `Nats-Msg-Id` of a
//! message as the hash of its key and its document, so a redelivery of that same message is
//! acknowledged as a duplicate instead of storing a second copy. `none` (the default) sends no id:
//! a document that legitimately returns to an earlier value inside the server's window would then
//! be dropped, which is a worse trade than the duplicate a retry may store.
//!
//! # Outcomes
//!
//! The PubAck of each message maps to that message's verdict:
//!
//! - a PubAck with a sequence: sent (`duplicate` says the server already had it under that id).
//!   `expect` then sees, per message, `body` `{"stream": …, "seq": …, "duplicate": …}` (no
//!   `expect`: met).
//! - timeouts, a lost connection, no ack at all, and the codes that describe the cluster or the
//!   stream rather than the message (no leader, no quorum, insufficient resources, the stream
//!   gone): transient, and the rows after a row that did not complete share that verdict — a
//!   retry cannot drop a key.
//! - a subject no stream binds, JetStream off on the server, a message over the stream's or the
//!   server's size limit, an authorization violation: refused for that message, and the rest of
//!   the batch still gets its own verdict.
//! - anything the client cannot judge: transient, as the safe default.
//!
//! # Concurrency
//!
//! One publish at a time per endpoint: the endpoint's lock is held over all of an action's
//! publishes, as for `amqp` and `kafka` — one target is one stream's ordering, and two actions
//! racing to append would settle in arbitrary order. Different targets and rules run in parallel.
//!
//! # Reconnects
//!
//! The library's own reconnect loop is off (`max_reconnects(0)`): it would sit between an outage
//! and the engine's strike, holding the endpoint lock while it waited. A failed action drops the
//! connection and its cached stream; the next action connects again and asks again.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use mysql_async::Pool;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;

use crate::model::TargetDef;
use crate::response::{Expect, Row};
use crate::secrets;
use crate::target::{rejected, Item, Sent};
use crate::tls::{Mode, TlsSpec};

/// The port a NATS server listens on when the endpoint names none.
pub const DEFAULT_PORT: u16 = 4222;

/// The `vars` that hold credentials: each is a secret and must be a reference.
pub const SECRET_VARS: [&str; 4] = ["token", "password", "nkey", "jwt"];

/// An endpoint: `nats(s)://<host>[:port][,<host>[:port]…]/<subject>`, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub secure: bool,
    /// Every server of the cluster, in the order the endpoint lists them: each one is connected
    /// to, and each one goes through the allow-list.
    pub servers: Vec<(String, u16)>,
    pub subject: String,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("tls://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("nats://") {
            (false, r)
        } else {
            bail!("endpoint `{shown}`: use nats://host[:port][,host[:port]…]/<subject> or tls://…");
        };
        if rest.contains('@') {
            bail!("endpoint `{shown}`: credentials belong in vars.token, vars.username and vars.password, or vars.nkey, not in the URL");
        }
        if rest.contains(['?', '#']) {
            bail!("endpoint `{shown}`: no query string or fragment (TLS is the target's `tls`)");
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        let servers = authority
            .split(',')
            .filter(|b| !b.is_empty())
            .map(|b| {
                if b.contains('[') {
                    bail!("endpoint `{shown}`: IPv6 addresses are not supported");
                }
                let (host, port) = match b.rsplit_once(':') {
                    Some((h, p)) => (
                        h,
                        p.parse::<u16>()
                            .with_context(|| format!("endpoint `{shown}`: port `{p}`"))?,
                    ),
                    None => (b, DEFAULT_PORT),
                };
                if host.is_empty() || host.bytes().all(|c| matches!(c, b'-' | b'.')) {
                    bail!("endpoint `{shown}`: a server has no host");
                }
                Ok((host.to_string(), port))
            })
            .collect::<Result<Vec<_>>>()?;
        if servers.is_empty() {
            bail!("endpoint `{shown}`: no server");
        }
        Ok(Addr {
            secure,
            servers,
            subject: subject_name(path)?,
        })
    }

    /// The server list async-nats takes: `host:port` per server.
    pub fn bootstrap(&self) -> Vec<String> {
        self.servers
            .iter()
            .map(|(h, p)| format!("{h}:{p}"))
            .collect()
    }
}

/// A NATS subject: dot-separated tokens of letters, digits, `-` and `_`, at least one token.
/// Wildcards (`*`, `>`) are refused: the engine has to know where a row went, and a wildcard would
/// make a message's stream depend on which streams the operator happens to have.
pub fn subject_name(s: &str) -> Result<String> {
    let ok = !s.is_empty()
        && s.len() <= 1024
        && s.split('.').all(|t| {
            !t.is_empty()
                && t.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        });
    if !ok {
        bail!(
            "the subject `{s}` is dot-separated tokens of letters, digits, `-` and `_` (no \
             wildcard, no empty token)"
        );
    }
    Ok(s.to_string())
}

/// The async-nats TLS settings for an endpoint (`None`: plain TCP). `tls://` is verified by
/// default; `tls.server_name` is refused (each server's own name is checked).
pub fn tls_config(spec: &TlsSpec, secure: bool) -> Result<Option<Arc<rustls::ClientConfig>>> {
    if spec.server_name.is_some() {
        bail!("`tls.server_name` is not supported by kind nats: each server's own name is checked");
    }
    match spec.effective(secure) {
        Mode::Disabled => {
            if secure {
                bail!("a tls:// endpoint needs TLS, but `tls` says disabled");
            }
            Ok(None)
        }
        Mode::Required => Ok(Some(crate::tls::client_config(spec, Mode::Required, &[])?)),
        Mode::VerifyCa => Ok(Some(crate::tls::client_config(spec, Mode::VerifyCa, &[])?)),
        Mode::VerifyFull => Ok(Some(crate::tls::client_config(
            spec,
            Mode::VerifyFull,
            &[],
        )?)),
        Mode::Auto => bail!("tls mode `auto` was not resolved"),
    }
}

fn var<'a>(vars: &'a Map<String, Value>, k: &str) -> Option<&'a str> {
    vars.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The credentials of the target, by mechanism, as `vars` gives them: reference strings, resolved
/// on every connect (so a rotated secret file is picked up without a restart, as for kafka).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// No credentials: an open server.
    None,
    Token(String),
    User {
        user: String,
        password: String,
    },
    /// The seed of an account user, with its account token when the server asks for that too.
    Nkey {
        seed: String,
        jwt: Option<String>,
    },
}

/// The credentials `vars` select: one mechanism, no half pairs, and a reference for every secret.
pub fn auth_of(vars: &Map<String, Value>) -> Result<Auth> {
    let token = var(vars, "token");
    let username = var(vars, "username");
    let password = var(vars, "password");
    let nkey = var(vars, "nkey");
    let jwt = var(vars, "jwt");
    if jwt.is_some() && nkey.is_none() {
        bail!(
            "`vars.jwt` needs `vars.nkey`: an account token is only usable by the user whose \
              signing key holds it"
        );
    }
    if username.is_some() != password.is_some() {
        bail!("`vars.username` and `vars.password` go together");
    }
    let mechanisms = usize::from(token.is_some())
        + usize::from(username.is_some() || password.is_some())
        + usize::from(nkey.is_some());
    ensure!(
        mechanisms < 2,
        "choose one NATS credential: `vars.token`, `vars.username` with `vars.password`, or \
         `vars.nkey` (with `vars.jwt`)"
    );
    for k in SECRET_VARS {
        if let Some(v) = var(vars, k) {
            ensure!(
                secrets::is_reference(v),
                "`vars.{k}` must be a reference (`env:NAME` or `file:/path`), never a literal"
            );
        }
    }
    Ok(match (token, username, password, nkey, jwt) {
        (Some(token), None, None, None, _) => Auth::Token(token.to_string()),
        (None, Some(user), Some(password), None, _) => Auth::User {
            user: user.to_string(),
            password: password.to_string(),
        },
        (None, None, None, Some(seed), jwt) => Auth::Nkey {
            seed: seed.to_string(),
            jwt: jwt.map(str::to_string),
        },
        _ => Auth::None,
    })
}

/// `vars.dedup`: whether a message carries a de-duplication id (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dedup {
    None,
    Content,
}

pub fn dedup_of(vars: &Map<String, Value>) -> Result<Dedup> {
    match var(vars, "dedup") {
        None | Some("none") => Ok(Dedup::None),
        Some("content") => Ok(Dedup::Content),
        Some(other) => bail!("`vars.dedup` `{other}` (use none or content)"),
    }
}

/// Load checks for a target of kind nats (from `TargetDef::from_row`): its endpoint, its TLS mode,
/// its credentials and its de-duplication setting.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    // One endpoint: one server set and one subject. A batch is answered per item, which the engine
    // sends to a single endpoint — as for kafka, sqs, kinesis and lambda.
    if endpoints.len() != 1 {
        bail!(
            "kind nats takes exactly one endpoint: \
             nats://<host>[:port][,<host>[:port]…]/<subject>"
        );
    }
    let tls = TlsSpec::parse(tls).context("`tls`")?;
    for e in endpoints {
        let a = Addr::parse(e)?;
        tls_config(&tls, a.secure)?;
    }
    auth_of(vars)?;
    dedup_of(vars)?;
    Ok(())
}

/// A `nats://` or `tls://` endpoint belongs to kind nats only.
pub fn refuse_scheme(kind_is_nats: bool, endpoints: &[String]) -> Result<()> {
    if !kind_is_nats
        && endpoints
            .iter()
            .any(|e| e.trim().starts_with("nats://") || e.trim().starts_with("tls://"))
    {
        bail!("a nats:// or tls:// endpoint needs kind nats");
    }
    Ok(())
}

/// The de-duplication id of a message, `Nats-Msg-Id`: the hash of what the message *is* — its key
/// and its document. JetStream keeps the id per stream in a window the operator sets, so a
/// redelivery of a message the server already stored is answered as a duplicate rather than stored
/// again. Content, not a delivery counter: the same row of the same key through the same target is
/// the same message, and a document that is not the same gets a different id (`Value` keeps object
/// keys in insertion order, so two documents that differ only in order are two documents, the way
/// two HTTP bodies that differ in order differ).
fn message_id(key: &str, document: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update([0xff]);
    hasher.update(document.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

/// One message of a rendered action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Msg {
    pub value: Value,
    pub headers: BTreeMap<String, String>,
    /// The value as the server takes it (serialized once, when the body was parsed).
    bytes: Vec<u8>,
}

/// A rendered action: the `{"messages": […]}` of a publish (message i is answered by key i).
pub struct Action {
    pub messages: Vec<Msg>,
}

impl Action {
    /// Parses a rendered body. `Ok(None)`: an empty `messages` list (nothing to send). The
    /// messages' count must match `n_keys` (a `once` step has none).
    pub fn parse(body: &[u8], n_keys: usize) -> Result<Option<Action>> {
        let v: Value = serde_json::from_slice(body).context("the publish body is not JSON")?;
        let messages = v
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow!("the publish body is `{{\"messages\": […]}}`"))?;
        let messages = messages
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let o = m
                    .as_object()
                    .ok_or_else(|| anyhow!("message {i} is not an object"))?;
                let value = o
                    .get("value")
                    .cloned()
                    .ok_or_else(|| anyhow!("message {i} has no `value`"))?;
                if value.is_null() {
                    bail!("message {i}: the `value` is null (render an empty object for a delete)");
                }
                let headers = match o.get("headers") {
                    None | Some(Value::Null) => BTreeMap::new(),
                    Some(Value::Object(h)) => h
                        .iter()
                        .map(|(k, v)| {
                            let v = v.as_str().ok_or_else(|| {
                                anyhow!("message {i}: the header `{k}` is not a string")
                            })?;
                            header_name(k).with_context(|| format!("message {i}"))?;
                            ensure!(
                                !v.bytes().any(|c| c < b' ' || c == 0x7f),
                                "message {i}: the header `{k}` holds a control character"
                            );
                            Ok((k.clone(), v.to_string()))
                        })
                        .collect::<Result<BTreeMap<String, String>>>()?,
                    Some(_) => bail!("message {i}: `headers` is not an object of strings"),
                };
                for f in o.keys() {
                    if f != "value" && f != "headers" {
                        bail!("message {i} has the unknown field `{f}` (a message carries `value` and `headers` only)");
                    }
                }
                Ok(Msg {
                    bytes: serde_json::to_vec(&value)
                        .map_err(|e| anyhow!("message {i}: the `value` cannot be written ({e})"))?,
                    value,
                    headers,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if messages.len() != n_keys {
            bail!(
                "the publish lists {} messages for {} keys (message i takes key i)",
                messages.len(),
                n_keys
            );
        }
        if messages.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Action { messages }))
        }
    }
}

/// A header name the server will take: letters, digits, `-` and `_`. async-nats drops a name it
/// cannot parse instead of failing, so a template that renders a bad name would silently lose the
/// header; it is refused here instead.
fn header_name(s: &str) -> Result<()> {
    ensure!(
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')),
        "the header name `{s}` is 1 to 128 letters, digits, `-` or `_`"
    );
    Ok(())
}

/// The engine's reading of one message's PubAck.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// Stored, with the stream and its sequence in it. `duplicate`: the server already had a
    /// message with this id, so nothing new was stored (it is on the stream either way).
    Ok {
        stream: String,
        seq: u64,
        duplicate: bool,
    },
    /// Refused for this message (rejected: strikes).
    Refused(String),
    /// Not known to have gone through: retry.
    Transient(String),
}

/// Whether a JetStream error code describes the server's answer about *this message* (it would
/// answer the same way on every attempt, so the row is refused) rather than the state of the
/// stream or the cluster (it may answer differently a moment later, so the row waits).
///
/// Anything this does not name is transient: only a code whose meaning is about the message may
/// end a row's retries, and a code the client does not know says nothing about the message.
fn code_refuses(code: u64) -> bool {
    use async_nats::jetstream::ErrorCode as C;
    [
        // The message, or its headers, are larger than the stream takes.
        C::STREAM_MESSAGE_EXCEEDS_MAXIMUM,
        C::STREAM_HEADER_EXCEEDS_MAXIMUM,
        // Nothing would store it: no such stream for this account, no account.
        C::STREAM_NOT_FOUND,
        C::NO_ACCOUNT,
        // JetStream is not on here at all, or not for this account.
        C::JETSTREAM_NOT_ENABLED,
        C::JETSTREAM_NOT_ENABLED_FOR_ACCOUNT,
        // The server's expected-last check failed, or the message is not for the stream it
        // named: the same answer on every attempt.
        C::STREAM_WRONG_LAST_MESSAGE_ID,
        C::STREAM_WRONG_LAST_SEQUENCE,
        C::STREAM_MISMATCH,
    ]
    .iter()
    .any(|c| c.0 == code)
}

/// Whether the text of a server error refuses this message rather than describing the server.
/// Permission failures reach us as text (the server's 503 `Authorization Violation` carries no
/// code the client exposes), and they are the same verdict every time.
fn text_refuses(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    t.contains("authorization violation")
        || t.contains("not authorized")
        || t.contains("permission denied")
}

/// What the target's endpoint and subject are called in messages (credentials never reach them:
/// the URL holds none, and the resolved secrets are redacted where an error quotes them).
fn describe(url: &str, subject: &str) -> String {
    format!("{url} (subject `{subject}`)")
}

/// The JetStream context of one connection.
struct Inner {
    client: async_nats::Client,
    js: async_nats::jetstream::Context,
    /// The stream that owns the subject, from the last lookup. `None`: not asked yet, or the last
    /// failure forgot it — an operator who creates or deletes a stream meanwhile must not have to
    /// wait for a restart.
    stream: Option<String>,
    /// The largest message the server takes, from its INFO.
    max_payload: usize,
}

/// One endpoint of a target.
pub struct Client {
    /// The endpoint as written (no credentials): the name in errors.
    pub url: String,
    addr: Addr,
    auth: Auth,
    dedup: Dedup,
    tls: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
    /// The connection (and its cached stream). Holding the lock keeps one action's publishes in
    /// flight at a time (see the module docs).
    inner: Mutex<Option<Inner>>,
}

impl Client {
    /// The endpoint's context (`endpoint` in templates) and its client.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint)?;
        let tls = tls_config(&def.tls, addr.secure)?;
        let auth = auth_of(&def.vars)?;
        let dedup = dedup_of(&def.vars)?;
        let (host, port) = addr.servers[0].clone();
        let ctx = serde_json::json!({
            "host": host,
            "port": port,
            "subject": addr.subject,
            "servers": addr.servers.iter().map(|(h, p)| serde_json::json!({"host": h, "port": p})).collect::<Vec<_>>(),
            "url": endpoint.trim(),
        });
        Ok((
            ctx,
            Client {
                url: endpoint.trim().to_string(),
                addr,
                auth,
                dedup,
                tls,
                timeout: def.timeout,
                inner: Mutex::new(None),
            },
        ))
    }

    /// Connects and builds the JetStream context on the connection. Credentials are resolved here,
    /// so a rotated secret file is picked up on the next connection; the values are redacted out of
    /// any error that quotes them and dropped when this returns.
    async fn open(&self) -> Result<Inner> {
        let mut opts = async_nats::ConnectOptions::new()
            .connection_timeout(self.timeout)
            .request_timeout(Some(self.timeout))
            .max_reconnects(0usize);
        // The secret values of this connection, to scrub out of an error message: async-nats quotes
        // what it was asked to do, and a token or a seed must not reach the log that way.
        let mut given = Vec::new();
        match &self.auth {
            Auth::None => {}
            Auth::Token(token) => {
                let token =
                    secrets::resolve_reference("vars.token", token).context("vars.token")?;
                given.push(token.clone());
                opts = opts.token(token);
            }
            Auth::User { user, password } => {
                let user = secrets::resolve(user).context("vars.username")?;
                let password = secrets::resolve_reference("vars.password", password)
                    .context("vars.password")?;
                given.push(password.clone());
                opts = opts.user_and_password(user, password);
            }
            Auth::Nkey { seed, jwt } => {
                let seed = secrets::resolve_reference("vars.nkey", seed).context("vars.nkey")?;
                given.push(seed.clone());
                match jwt {
                    Some(jwt) => {
                        let jwt =
                            secrets::resolve_reference("vars.jwt", jwt).context("vars.jwt")?;
                        given.push(jwt.clone());
                        // The server sends a nonce; the user's key signs it. Nothing else in the
                        // handshake needs the seed, so it is read per connection.
                        let signer = seed.clone();
                        opts = opts.jwt(jwt, move |nonce: Vec<u8>| {
                            let signer = signer.clone();
                            async move {
                                let pair = nkeys::KeyPair::from_seed(&signer).map_err(|e| {
                                    async_nats::AuthError::new(format!(
                                        "vars.nkey is not a user seed: {e}"
                                    ))
                                })?;
                                pair.sign(&nonce).map_err(|e| {
                                    async_nats::AuthError::new(format!(
                                        "cannot sign the server's nonce: {e}"
                                    ))
                                })
                            }
                        });
                    }
                    None => opts = opts.nkey(seed),
                }
            }
        }
        if let Some(config) = &self.tls {
            opts = opts.tls_client_config((**config).clone());
        }
        let client = tokio::time::timeout(self.timeout, opts.connect(self.addr.bootstrap()))
            .await
            .map_err(|_| {
                anyhow!(
                    "connecting to {} timed out after {:?}",
                    self.url,
                    self.timeout
                )
            })?
            .map_err(|e| connect_error(&self.url, &given, e))?;
        let info = client.server_info();
        // A server without JetStream will not answer any publish, ever: that is the target's
        // configuration, not an outage, and the engine should strike rather than retry.
        ensure!(
            info.jetstream,
            "{}: the server does not have JetStream enabled ({} at {}:{}, version {}): start \
             the server with `-js`, or point the target at one that has it",
            describe(&self.url, &self.addr.subject),
            info.server_name,
            info.host,
            info.port,
            info.version
        );
        // Bounded so a server that accepts the connection and then stops answering cannot hold the
        // endpoint lock past the action's time limit.
        let js = async_nats::jetstream::ContextBuilder::new()
            .timeout(self.timeout)
            .build(client.clone());
        Ok(Inner {
            client,
            js,
            stream: None,
            max_payload: info.max_payload,
        })
    }

    /// The stream that owns the subject, asked once per connection (see `Inner::stream`).
    /// `Ok(None)`: no stream binds it, which no retry can fix. `Err`: the lookup itself failed.
    async fn stream_of(&self, inner: &mut Inner) -> Result<Option<String>> {
        if let Some(name) = &inner.stream {
            return Ok(Some(name.clone()));
        }
        match inner.js.stream_by_subject(self.addr.subject.clone()).await {
            Ok(name) => {
                inner.stream = Some(name.clone());
                Ok(Some(name))
            }
            Err(e) => match e.kind() {
                async_nats::jetstream::context::GetStreamByNameErrorKind::NotFound => Ok(None),
                async_nats::jetstream::context::GetStreamByNameErrorKind::JetStream(err) => {
                    let code = err.error_code().0;
                    if code == async_nats::jetstream::ErrorCode::JETSTREAM_NOT_ENABLED.0
                        || code
                            == async_nats::jetstream::ErrorCode::JETSTREAM_NOT_ENABLED_FOR_ACCOUNT.0
                    {
                        return Err(rejected(format!(
                            "{}: the server does not have JetStream enabled for this account: {err}",
                            describe(&self.url, &self.addr.subject)
                        )));
                    }
                    Err(anyhow!(
                        "{}: cannot look up the stream of the subject: {err}",
                        describe(&self.url, &self.addr.subject)
                    ))
                }
                async_nats::jetstream::context::GetStreamByNameErrorKind::InvalidSubject => {
                    Err(rejected(format!(
                        "{}: the subject is not valid for the server",
                        describe(&self.url, &self.addr.subject)
                    )))
                }
                // The API request itself failed: nothing is known about the subject.
                _ => Err(anyhow!(
                    "{}: cannot look up the stream of the subject: {e}",
                    describe(&self.url, &self.addr.subject)
                )),
            },
        }
    }

    /// Publishes one message and waits for its PubAck. `stream` is the stream the lookup above
    /// said owns the subject.
    async fn publish(&self, inner: &mut Inner, stream: &str, key: &str, msg: &Msg) -> Outcome {
        let mut message = async_nats::jetstream::message::PublishMessage::build()
            .payload(msg.bytes.clone().into());
        for (name, value) in &msg.headers {
            message = message.header(name.as_str(), value.as_str());
        }
        if self.dedup == Dedup::Content {
            message = message.message_id(message_id(key, &msg.value));
        }
        // The name from the lookup above: the server refuses the message rather than filing it in
        // a stream that appeared since, which would put a row where nothing reads it.
        let message = message.expected_stream(stream);
        let at = describe(&self.url, &self.addr.subject);
        let max = inner.max_payload;
        // The whole exchange — the publish and its ack — under the target's time limit: past it
        // nothing is known to be in, which is transient, not a verdict on the message.
        let step = async {
            let ack = inner
                .js
                .send_publish(self.addr.subject.clone(), message)
                .await
                .map_err(|e| publish_error(&at, max, &e))?;
            ack.await.map_err(|e| publish_error(&at, max, &e))
        };
        match tokio::time::timeout(self.timeout, step).await {
            Err(_) => Outcome::Transient(format!("no PubAck from {at} within {:?}", self.timeout)),
            Ok(Err(outcome)) => outcome,
            Ok(Ok(ack)) => {
                tracing::debug!(
                    target: "lazywf::nats",
                    endpoint = %self.url,
                    subject = %self.addr.subject,
                    stream = %ack.stream,
                    seq = ack.sequence,
                    duplicate = ack.duplicate,
                    "lazywf.nats.published"
                );
                Outcome::Ok {
                    stream: ack.stream,
                    seq: ack.sequence,
                    duplicate: ack.duplicate,
                }
            }
        }
    }

    /// Publishes the action's messages in order, each awaited before the next starts, so the
    /// stream's sequence order is the request's row order. `Err`: the action could not be
    /// attempted at all (the connection, or the stream lookup), as for a kafka produce whose
    /// metadata read failed; the messages then have no verdict of their own.
    async fn publish_all(&self, keys: &[String], action: &Action) -> Result<Vec<Outcome>> {
        let mut slot = self.inner.lock().await;
        let r = self.publish_on(&mut slot, keys, action).await;
        // Anything that leaves the connection untrusted — a whole-call failure, or a message
        // whose fate is unknown — drops it: the next action connects afresh and asks about the
        // stream again, as `amqp` drops its session and `kafka` its cached partitions.
        if r.is_err()
            || r.as_ref()
                .is_ok_and(|out| out.iter().any(|o| matches!(o, Outcome::Transient(_))))
        {
            if let Some(inner) = slot.take() {
                drop_client(inner.client);
            }
        }
        r
    }

    async fn publish_on(
        &self,
        slot: &mut Option<Inner>,
        keys: &[String],
        action: &Action,
    ) -> Result<Vec<Outcome>> {
        // The library's reconnect loop is off, so a lost connection stays lost: start afresh.
        let lost = slot.as_ref().is_some_and(|i| {
            i.client.connection_state() != async_nats::connection::State::Connected
        });
        if lost {
            if let Some(inner) = slot.take() {
                drop_client(inner.client);
            }
        }
        if slot.is_none() {
            *slot = Some(self.open().await?);
        }
        let inner = slot.as_mut().expect("opened above");
        // Asked once per connection: this is what tells "the target is up and nothing will ever
        // store this subject" from "the target is down", which no publish answer can tell us.
        let stream = match self.stream_of(inner).await? {
            Some(name) => name,
            // A subject no stream binds: no retry can fix it, and a publish to it stores nothing,
            // so every row is refused here rather than reported as sent.
            None => {
                return Err(rejected(format!(
                    "{}: no JetStream stream binds the subject (create the stream, or fix the \
                     subject in the endpoint)",
                    describe(&self.url, &self.addr.subject)
                )));
            }
        };
        let mut out = Vec::with_capacity(action.messages.len());
        for (i, msg) in action.messages.iter().enumerate() {
            let key = keys.get(i).map(String::as_str).unwrap_or_default();
            let outcome = self.publish(inner, &stream, key, msg).await;
            // A message whose fate is unknown says nothing about the ones after it: they take the
            // same verdict rather than each getting its own guess.
            let stop = matches!(outcome, Outcome::Transient(_));
            out.push(outcome);
            if stop {
                out.resize_with(action.messages.len(), || {
                    Outcome::Transient(
                        "not sent: an earlier message of this action did not complete".to_string(),
                    )
                });
                break;
            }
        }
        Ok(out)
    }
}

/// The values that must not appear in a message, masked.
fn redact(text: &str, given: &[String]) -> String {
    let mut out = text.to_string();
    for value in given {
        if !value.is_empty() {
            out = out.replace(value.as_str(), "***");
        }
    }
    out
}

/// A connect failure: refused when the server says the credentials are wrong (retrying them would
/// only fill the log), transient for everything else, with no credential in the text.
fn connect_error(url: &str, given: &[String], error: async_nats::ConnectError) -> anyhow::Error {
    use async_nats::ConnectErrorKind as K;
    let text = redact(&error.to_string(), given);
    match error.kind() {
        K::Authentication | K::AuthorizationViolation => rejected(format!(
            "{url}: the server rejected the credentials ({text}): check vars.token, \
             vars.username and vars.password, or vars.nkey and vars.jwt"
        )),
        _ => anyhow!("connecting to {url} failed: {text}"),
    }
}

/// A publish failure, mapped to what it says about the message: only a verdict about *this*
/// message refuses it; the cluster's and the stream's troubles wait, and so does anything the
/// client cannot judge.
fn publish_error(
    at: &str,
    max_payload: usize,
    error: &async_nats::jetstream::context::PublishError,
) -> Outcome {
    use async_nats::jetstream::context::PublishErrorKind as K;
    match error.kind() {
        // Nobody is answering for the subject. The lookup above found a stream, so this says the
        // cluster is not answering for it right now: wait, and let the next action's lookup be the
        // one that says the stream is gone for good — which refuses.
        K::StreamNotFound => Outcome::Transient(format!(
            "{at}: no stream is answering for the subject right now"
        )),
        // The server will not take a message this big, and will not take it on any attempt.
        K::MaxPayloadExceeded => Outcome::Refused(format!(
            "{at}: the message is over the {max_payload} bytes the server takes (its max_payload)"
        )),
        K::MaxAckPending => Outcome::Transient(format!(
            "{at}: the server has too many publishes unacknowledged"
        )),
        K::TimedOut => {
            Outcome::Transient(format!("{at}: JetStream did not acknowledge the publish"))
        }
        K::BrokenPipe => Outcome::Transient(format!("{at}: the connection to the server closed")),
        // Only set with `expected_last_*`, which this client does not use; a refusal if it happens.
        K::WrongLastMessageId | K::WrongLastSequence => Outcome::Refused(format!(
            "{at}: the server expects a different last message: {error}"
        )),
        K::Other => {
            // The server's own answer, if it sent one: a code about this message refuses it, the
            // permission texts refuse it, and everything else — the cluster codes, an unknown code,
            // a description with no code — says nothing about the message.
            let server = std::error::Error::source(error)
                .and_then(|s| s.downcast_ref::<async_nats::jetstream::Error>());
            match server {
                Some(err) if code_refuses(err.error_code().0) || text_refuses(&err.to_string()) => {
                    Outcome::Refused(format!("{at}: the server refused the message: {err}"))
                }
                _ => Outcome::Transient(format!("{at}: the publish failed: {error}")),
            }
        }
    }
}

/// Drop the connection and stop the library's task for it; the next action connects afresh.
fn drop_client(client: async_nats::Client) {
    // `drain` asks the connection task to finish and exit; with reconnects off, dropping the last
    // handle would end it too, but not before its buffers and pings time out.
    tokio::spawn(async move {
        let _ = client.drain().await;
    });
}

/// Sends one rendered `once` action (no keys) and checks `expect` on every message.
pub async fn send(client: &Client, rendered: &str, expect: &Expect, pool: &Pool) -> Result<Sent> {
    let Some(outcomes) = items_of(client, rendered, &[]).await? else {
        return Ok(Sent::Skipped);
    };
    for (i, outcome) in outcomes.iter().enumerate() {
        match outcome {
            Outcome::Ok {
                stream,
                seq,
                duplicate,
            } => expect
                .row(pool, |u| Row::nats(u, stream, *seq, *duplicate))
                .await
                .with_context(|| format!("message {i} of {}", client.url))?
                .map_err(|e| rejected(format!("{} message {i}: {e}", client.url)))?,
            Outcome::Refused(why) => {
                return Err(rejected(format!(
                    "{} message {i}: the server refused it: {why}",
                    client.url
                )));
            }
            Outcome::Transient(why) => bail!("{} message {i}: {why}", client.url),
        }
    }
    Ok(Sent::Ok)
}

/// Sends one rendered action (message i takes key i) and checks `expect` on every sent message.
/// `Ok(vec!)`: nothing rendered. `Err`: the whole call failed (the messages have no answer).
pub async fn send_items(
    client: &Client,
    rendered: &str,
    keys: &[String],
    expect: &Expect,
    pool: &Pool,
) -> Result<Vec<Item>> {
    let outcomes = match items_of(client, rendered, keys).await? {
        Some(outcomes) => outcomes,
        None => return Ok(vec![Item::Ok; keys.len()]),
    };
    let mut out = Vec::with_capacity(outcomes.len());
    for (i, outcome) in outcomes.iter().enumerate() {
        let item = match outcome {
            Outcome::Ok {
                stream,
                seq,
                duplicate,
            } => match expect
                .row(pool, |u| Row::nats(u, stream, *seq, *duplicate))
                .await
                .with_context(|| format!("message {i} of {}", client.url))?
            {
                Ok(()) => Item::Ok,
                Err(why) => Item::Rejected(format!("message {i} of {}: {why}", client.url)),
            },
            Outcome::Refused(why) => Item::Rejected(format!(
                "message {i} of {}: the server refused it: {why}",
                client.url
            )),
            Outcome::Transient(why) => {
                Item::Transient(format!("message {i} of {}: {why}", client.url))
            }
        };
        out.push(item);
    }
    Ok(out)
}

/// The outcomes of one rendered action (message i takes key i). `Ok(None)`: nothing rendered.
/// `Err`: the whole call failed (a connection or a JetStream error, before any message answer).
async fn items_of(
    client: &Client,
    rendered: &str,
    keys: &[String],
) -> Result<Option<Vec<Outcome>>> {
    let req = match crate::http::RawRequest::parse(rendered) {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(None),
        Err(e) => return Err(rejected(format!("the rendered request: {e:#}"))),
    };
    if req.method != "POST" || req.target != "/" {
        return Err(rejected(format!(
            "nats targets only publish: {} {} is not POST /",
            req.method, req.target
        )));
    }
    let Some(action) = Action::parse(&req.body, keys.len())
        .map_err(|e| rejected(format!("the publish body: {e:#}")))?
    else {
        return Ok(None);
    };
    client.publish_all(keys, &action).await.map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{RawTarget, TargetDef};
    use serde_json::json;

    fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
        RawTarget {
            name: "events".into(),
            kind: Some("nats".into()),
            endpoints: Some(endpoints.into()),
            tls: tls.map(Into::into),
            timeout_ms: None,
            expect: None,
            vars: Some(vars.into()),
        }
    }

    fn def(endpoints: &str, tls: Option<&str>, vars: &str) -> TargetDef {
        TargetDef::from_row(&raw(endpoints, tls, vars)).unwrap()
    }

    fn spec(s: &str) -> TlsSpec {
        TlsSpec::parse(Some(s)).unwrap()
    }

    const CREDS: &str = r#"{"token": "env:LWF_TEST_NATS_TOKEN"}"#;

    #[test]
    fn endpoints() {
        let a = Addr::parse("nats://server:4222/orders.created").unwrap();
        let one: &[(String, u16)] = &[("server".to_string(), 4222)];
        assert_eq!(
            (a.secure, &a.servers[..], a.subject.as_str()),
            (false, one, "orders.created")
        );
        let a = Addr::parse("tls://n1.internal:4222,n2.internal/orders.v2_x").unwrap();
        let servers: Vec<(String, u16)> = vec![
            ("n1.internal".to_string(), 4222),
            ("n2.internal".to_string(), DEFAULT_PORT),
        ];
        assert_eq!(
            (a.secure, a.servers, a.subject.clone()),
            (true, servers, "orders.v2_x".to_string())
        );
        assert_eq!(
            Addr::parse("nats://n1,n2/t").unwrap().bootstrap(),
            vec!["n1:4222", "n2:4222"]
        );
        for bad in [
            "http://h/s",
            "nats://u:p@h/s",
            "nats://h/s?js=yes",
            "nats://h/s#frag",
            "nats://h",
            "nats://h/",
            "nats://:4222/s",
            "nats://h:x/s",
            "nats://[::1]/s",
            "nats://h,-/s",
            "nats://h/a.*.c",
            "nats://h/a.>.c",
            "nats://h/a..c",
            "nats://h/a b",
            "nats://h/a/",
        ] {
            assert!(Addr::parse(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn subjects() {
        assert_eq!(subject_name("orders").unwrap(), "orders");
        assert_eq!(subject_name("a.b-c_1.d").unwrap(), "a.b-c_1.d");
        for bad in [
            "", ".", "a.", ".a", "a..b", "a.*", "a.>", "a b", "a\nb", "a/b",
        ] {
            assert!(subject_name(bad).is_err(), "accepted {bad:?}");
        }
        assert!(subject_name(&"a".repeat(1025)).is_err());
    }

    #[test]
    fn tls_modes_map() {
        assert!(tls_config(&spec(""), false).unwrap().is_none());
        assert!(tls_config(&spec(""), true).unwrap().is_some());
        assert!(tls_config(&spec("required"), true).unwrap().is_some());
        assert!(tls_config(&spec("verify-ca"), true).unwrap().is_some());
        let e = tls_config(
            &spec(r#"{"mode": "verify-full", "server_name": "s"}"#),
            true,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("server_name"), "{e}");
        let e = tls_config(&spec("disabled"), true).unwrap_err().to_string();
        assert!(e.contains("disabled"), "{e}");
        assert!(tls_config(&spec(r#"{"ca": "/nonexistent/lwf-ca.pem"}"#), true).is_err());
    }

    #[test]
    fn credentials_are_one_mechanism_and_reference_only() {
        // No credentials at all: a development server that asks for nothing.
        assert_eq!(auth_of(&Map::new()).unwrap(), Auth::None);
        let mut v = Map::new();
        v.insert("token".into(), json!("env:T"));
        assert_eq!(auth_of(&v).unwrap(), Auth::Token("env:T".into()));
        // An empty reference is no credential, not a broken one.
        v.insert("token".into(), json!(""));
        assert_eq!(auth_of(&v).unwrap(), Auth::None);

        let mut v = Map::new();
        v.insert("username".into(), json!("app"));
        v.insert("password".into(), json!("env:P"));
        assert_eq!(
            auth_of(&v).unwrap(),
            Auth::User {
                user: "app".into(),
                password: "env:P".into()
            }
        );
        let mut v = Map::new();
        v.insert("nkey".into(), json!("file:/run/secrets/nkey"));
        v.insert("jwt".into(), json!("file:/run/secrets/jwt"));
        assert_eq!(
            auth_of(&v).unwrap(),
            Auth::Nkey {
                seed: "file:/run/secrets/nkey".into(),
                jwt: Some("file:/run/secrets/jwt".into())
            }
        );
        // Half a pair, either way.
        let mut v = Map::new();
        v.insert("jwt".into(), json!("file:/run/secrets/jwt"));
        assert!(auth_of(&v).is_err());
        let mut v = Map::new();
        v.insert("username".into(), json!("app"));
        assert!(auth_of(&v).is_err());
        // Two mechanisms is ambiguous.
        let mut v = Map::new();
        v.insert("token".into(), json!("env:T"));
        v.insert("nkey".into(), json!("env:K"));
        assert!(auth_of(&v).is_err());
        // A secret literal is a load error, whatever the mechanism.
        for (k, bad) in [
            ("token", "literal-token"),
            ("password", "literal-password"),
            ("nkey", "SUAPLAINSEED"),
            ("jwt", "eyJhbGciOiJIUzI1NiJ9.x"),
        ] {
            let mut v = Map::new();
            v.insert(k.into(), json!(bad));
            if k == "password" {
                v.insert("username".into(), json!("app"));
            }
            if k == "jwt" {
                v.insert("nkey".into(), json!("env:K"));
            }
            let e = auth_of(&v).unwrap_err().to_string();
            assert!(
                e.contains(&format!("vars.{k}")) && e.contains("reference"),
                "{e}"
            );
        }
        assert_eq!(SECRET_VARS, ["token", "password", "nkey", "jwt"]);
    }

    #[test]
    fn dedup_forms() {
        assert_eq!(dedup_of(&Map::new()).unwrap(), Dedup::None);
        let mut v = Map::new();
        v.insert("dedup".into(), json!("none"));
        assert_eq!(dedup_of(&v).unwrap(), Dedup::None);
        v.insert("dedup".into(), json!("content"));
        assert_eq!(dedup_of(&v).unwrap(), Dedup::Content);
        v.insert("dedup".into(), json!("key"));
        assert!(dedup_of(&v).is_err());
    }

    #[test]
    fn load_checks() {
        // One endpoint, a subject, references only.
        assert!(check(&["nats://127.0.0.1:4222/orders".into()], &Map::new(), None).is_ok());
        assert!(check(
            &[
                "nats://127.0.0.1:4222/orders".into(),
                "nats://127.0.0.2:4222/orders".into()
            ],
            &Map::new(),
            None
        )
        .is_err());
        assert!(check(&["nats://127.0.0.1:4222".into()], &Map::new(), None).is_err());
        assert!(check(
            &["nats://127.0.0.1:4222/orders".into()],
            &Map::new(),
            Some("off")
        )
        .is_ok());
        let mut vars = Map::new();
        vars.insert("token".into(), json!("literal"));
        assert!(check(&["nats://127.0.0.1:4222/orders".into()], &vars, None).is_err());
        // The whole path through the target row.
        let d = def("nats://127.0.0.1:4222/orders", None, CREDS);
        assert_eq!(d.kind.name(), "nats");
        assert!(!d.kind.is_http());
        assert!(d.kind.reports_items());
        assert!(d.kind.append_only());
        assert_eq!(d.kind.name(), "nats");
        assert!(TargetDef::from_row(&raw("nats://h/x", None, r#"{"token": "literal"}"#)).is_err());
        assert!(TargetDef::from_row(&raw("nats://h/x", Some("nonsense"), CREDS)).is_err());
        // A nats:// endpoint on another kind is refused, before its own parser can misread it.
        assert!(refuse_scheme(false, &["nats://h/s".into()]).is_err());
        assert!(refuse_scheme(false, &["tls://h/s".into()]).is_err());
        assert!(refuse_scheme(true, &["tls://h/s".into()]).is_ok());
        assert!(refuse_scheme(false, &["http://h/s".into()]).is_ok());
    }

    #[test]
    fn rendered_form() {
        let a = Action::parse(
            br#"{"messages": [{"value": {"qty": 3}, "headers": {"op": "upsert"}}, {"value": []}]}"#,
            2,
        )
        .unwrap()
        .unwrap();
        assert_eq!(a.messages.len(), 2);
        assert_eq!(a.messages[0].value, json!({"qty": 3}));
        assert_eq!(a.messages[0].bytes, br#"{"qty":3}"#);
        assert_eq!(a.messages[0].headers["op"], "upsert");
        assert!(a.messages[1].headers.is_empty());
        // Nothing rendered is not an error.
        assert!(Action::parse(br#"{"messages": []}"#, 0).unwrap().is_none());
        for bad in [
            br#"{"messages": [{"headers": {}}]}"#.as_slice(),
            br#"{"messages": [{"value": null}]}"#.as_slice(),
            br#"{"messages": [{"value": {}, "key": "x"}]}"#.as_slice(),
            br#"{"messages": [{"value": {}, "headers": "x"}]}"#.as_slice(),
            br#"{"messages": [{"value": {}, "headers": {"a": 1}}]}"#.as_slice(),
            br#"{"messages": [{"value": {}, "headers": {"a b": "c"}}]}"#.as_slice(),
            br#"{"messages": [{"value": {}, "headers": {"a": "x\ny"}}]}"#.as_slice(),
            br#"{"messages": []}"#.as_slice(),
            br#"{"records": []}"#.as_slice(),
            br#"[]"#.as_slice(),
        ] {
            let n = usize::from(!bad.starts_with(br#"b""#) || bad.len() > 16);
            assert!(
                Action::parse(bad, n).is_err() || Action::parse(bad, 0).is_err(),
                "accepted {}",
                String::from_utf8_lossy(bad)
            );
        }
        // The count must match the keys.
        assert!(Action::parse(br#"{"messages": [{"value": 1}, {"value": 2}]}"#, 1).is_err());
    }

    #[test]
    fn message_ids_follow_the_message() {
        let document = json!({"order": 1, "note": "same"});
        let a = message_id("key-1", &document);
        assert_eq!(a, message_id("key-1", &document));
        // A different key, or a different document, is a different message.
        assert_ne!(a, message_id("key-2", &document));
        assert_ne!(
            a,
            message_id("key-1", &json!({"order": 1, "note": "other"}))
        );
        // Document order is content, the way two HTTP bodies that differ in order differ.
        assert_ne!(a, message_id("key-1", &json!({"note": "same", "order": 1})));
        // A key that is not a document must not collide with one that is.
        assert_ne!(
            message_id("a", &json!({"b": 1})),
            message_id("a\u{ff}b", &json!({}))
        );
        assert!(
            a.len() == 64 && a.chars().all(|c| c.is_ascii_hexdigit()),
            "the id is a hex sha256: {a}"
        );
    }

    #[test]
    fn cluster_codes_wait_and_message_codes_refuse() {
        use async_nats::jetstream::ErrorCode as C;
        // A stream mid-election, a cluster without quorum, resources that may free up: an outage,
        // and the row has to be tried again.
        for code in [
            C::CLUSTER_NOT_AVAILABLE.0,
            C::CLUSTER_NOT_LEADER.0,
            C::CLUSTER_NOT_ACTIVE.0,
            C::CLUSTER_NO_PEERS.0,
            C::CLUSTER_INCOMPLETE.0,
            C::INSUFFICIENT_RESOURCES.0,
            C::ACCOUNT_RESOURCES_EXCEEDED.0,
            C::STREAM_ASSIGNMENT.0,
            // An unknown code says nothing about the message.
            0,
            99_999,
        ] {
            assert!(!code_refuses(code), "{code} must be retried");
        }
        // The same answer every time: retrying is waiting for nothing.
        for code in [
            C::STREAM_MESSAGE_EXCEEDS_MAXIMUM.0,
            C::STREAM_NOT_FOUND.0,
            C::NO_ACCOUNT.0,
            C::JETSTREAM_NOT_ENABLED.0,
            C::JETSTREAM_NOT_ENABLED_FOR_ACCOUNT.0,
            C::STREAM_WRONG_LAST_MESSAGE_ID.0,
        ] {
            assert!(code_refuses(code), "{code} must be refused");
        }
        assert!(text_refuses(
            "503 Authorization Violation (code 503, error code 0)"
        ));
        assert!(text_refuses("publish Permission Denied"));
        assert!(!text_refuses("no responders"));
    }

    #[test]
    fn a_size_refusal_and_an_outage_are_told_apart() {
        use async_nats::jetstream::context::PublishErrorKind as K;
        // The server will not take a message this big, on this attempt or any other: refused.
        assert!(matches!(
            publish_error("u", 1024, &publish_error_of(K::MaxPayloadExceeded)),
            Outcome::Refused(t) if t.contains("1024")
        ));
        assert!(matches!(
            publish_error("u", 1024, &publish_error_of(K::WrongLastMessageId)),
            Outcome::Refused(_)
        ));
        // Nothing was learned about the message: transient, so the key waits instead of
        // accumulating strikes for an outage.
        for kind in [
            K::TimedOut,
            K::BrokenPipe,
            K::StreamNotFound,
            K::MaxAckPending,
            K::Other,
        ] {
            assert!(
                matches!(
                    publish_error("u", 1024, &publish_error_of(kind)),
                    Outcome::Transient(_)
                ),
                "{kind:?} must be retried"
            );
        }
    }

    fn publish_error_of(
        kind: async_nats::jetstream::context::PublishErrorKind,
    ) -> async_nats::jetstream::context::PublishError {
        async_nats::jetstream::context::PublishError::new(kind)
    }

    #[test]
    fn nothing_a_message_carries_reaches_a_log() {
        // The endpoint and the subject are operator data; the credentials are not, and they are
        // never in the endpoint in the first place. Every mechanism, one at a time: a template
        // must see none of them.
        for vars in [
            r#"{"token": "env:LWF_TEST_NATS_TOKEN"}"#,
            r#"{"username": "app", "password": "env:LWF_TEST_NATS_PW"}"#,
            r#"{"nkey": "file:/run/secrets/nkey", "jwt": "file:/run/secrets/jwt"}"#,
        ] {
            let d = def("nats://127.0.0.1:4222/orders", None, vars);
            let text = d.template_vars().to_string();
            for k in SECRET_VARS {
                assert!(!text.contains(k), "{k} reached templates: {text}");
            }
            assert!(
                !text.contains("env:") && !text.contains("file:"),
                "a reference reached templates: {text}"
            );
        }
        let text = redact(
            "connecting failed with token super-secret-token and seed SUAPLAINSEEDHERE",
            &[
                "super-secret-token".to_string(),
                "SUAPLAINSEEDHERE".to_string(),
            ],
        );
        assert!(!text.contains("super-secret-token"), "{text}");
        assert!(!text.contains("SUAPLAINSEEDHERE"), "{text}");
        assert!(text.contains("connecting failed"), "{text}");
    }

    #[tokio::test]
    async fn a_dead_server_is_transient_not_a_refusal() {
        // Nothing listens on port 1: the connect fails with an IO error. The action could not be
        // attempted at all, which is the whole-call transient path — and above all not a refusal,
        // which is how a key gets dropped for nothing.
        let d = def("nats://127.0.0.1:1/orders.created", None, "{}");
        let (ctx, client) = Client::new(&d, "nats://127.0.0.1:1/orders.created").unwrap();
        assert_eq!(ctx["subject"], "orders.created");
        assert_eq!(ctx["port"], 1);
        let keys = vec!["k1".to_string(), "k2".to_string()];
        let e = client
            .publish_all(
                &keys,
                &Action {
                    messages: vec![msg(1), msg(2)],
                },
            )
            .await
            .unwrap_err();
        assert!(!crate::target::is_rejected(&e), "an outage refused: {e:#}");
        // The endpoint names itself in the error; a credential does not (there are none here, and
        // the resolved ones are scrubbed where an error quotes them).
        assert!(format!("{e:#}").contains("orders.created"), "{e:#}");
        // The connection slot is empty again: the next action connects afresh.
        assert!(client.inner.try_lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_request_that_is_not_a_publish_is_refused_before_connecting() {
        let d = def("nats://127.0.0.1:1/orders", None, "{}");
        let (_, client) = Client::new(&d, "nats://127.0.0.1:1/orders").unwrap();
        let keys = vec!["k".to_string()];
        for rendered in [
            "GET / HTTP/1.1\n\n",
            "POST /extra HTTP/1.1\n\n{\"messages\": [{\"value\": {}}]}",
            "POST / HTTP/1.1\n\nnot json",
        ] {
            let e = items_of(&client, rendered, &keys).await.unwrap_err();
            assert!(crate::target::is_rejected(&e), "{rendered}: {e:#}");
        }
        // Nothing rendered at all: no request, no verdict.
        assert!(items_of(&client, "", &keys).await.unwrap().is_none());
    }

    fn msg(n: u64) -> Msg {
        let value = json!({"n": n});
        Msg {
            bytes: serde_json::to_vec(&value).unwrap(),
            value,
            headers: BTreeMap::new(),
        }
    }
}
