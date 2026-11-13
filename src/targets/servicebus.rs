//! The `servicebus` kind: one batch of messages per action to an Azure Service Bus queue or
//! topic, over AMQP 1.0. Client: fe2o3-amqp (pure Rust; the 0.14 line, whose `rustls` feature
//! is ring only — see Cargo.toml).
//!
//! # Endpoint and credentials
//!
//! `sb://<namespace>.servicebus.windows.net/<queue-or-topic>`: AMQP 1.0 over TLS on port
//! 5671 (the resource-URI form of the Microsoft SAS doc:
//! <https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-sas>). One
//! endpoint per target: a batch is answered per item, which the engine sends to a single
//! endpoint, as for kafka, sqs and sns. A plain host with an explicit port
//! (`sb://127.0.0.1:34610/queues/orders`) is the local-broker override for tests: its path is
//! the AMQP 1.0 address the broker is given verbatim (leading `/` included), since an address
//! means whatever the broker says it means. No credentials in the URL.
//!
//! # Auth: SAS through CBS
//!
//! `vars.key_name` (the authorization rule name) and `vars.key` (the key, a reference, never
//! a literal — the secret of the kind) carry a Shared Access Signature, put to the `$cbs`
//! node of the connection and refreshed before it expires (claims-based security,
//! <https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide>).
//! The token is built exactly as the Microsoft SAS doc prescribes:
//! `SharedAccessSignature sr=<enc>&sig=<enc>&se=<epoch-seconds>&skn=<name>` with
//! `sig = urlencode(base64(hmac-sha256(urlencode(audience) + "\n" + se, key)))` over the
//! *UTF-8 bytes of the Base64 key as written* (the doc: "They must not be decoded before
//! they're used"). Microsoft Entra ID is out of scope for this kind: SAS only.
//!
//! Service Bus accepts CBS only on a SASL ANONYMOUS handshake over TLS (the protocol guide
//! above), so the connection opens ANONYMOUS and the token authenticates the links. Without
//! keys (the local override only) the connection is anonymous and CBS is skipped;
//! `vars.username` + `vars.password` (references, as kafka takes them) select SASL PLAIN
//! instead — for a broker that speaks AMQP 1.0 but not CBS (the RabbitMQ of the e2e).
//!
//! # The rendered action
//!
//! One action is one batch of messages sent over one sender link, a disposition awaited for
//! every transfer, as
//!
//! ```text
//! POST / HTTP/1.1
//! Content-Type: application/json
//!
//! {"messages": [{"value": …, "session": "…", "properties": {…}}, …]}
//! ```
//!
//! Message i takes the document key `keys[i]`: its `value` (one JSON value, serialized
//! compactly into an AMQP-Value string), its optional `session` (the group id — required for
//! a session-enabled entity, `vars.session` states it, and refused for that one message when
//! missing) and optional `properties` (string to string: the application properties). A
//! message carries `value`, `session` and `properties` only. With no keys (a `once` step) the
//! messages have no key. `vars.dedup` (off by default) sets each message's MessageId to
//! `hex(sha256(key + \0 + value))` for the entity's duplicate detection
//! (<https://learn.microsoft.com/en-us/azure/service-bus-messaging/how-duplicate-detection-works-filtering>):
//! opt-in, because a document that legitimately returns to an earlier value within the
//! detection window would then be dropped as a duplicate.
//!
//! The batch is bounded by the entity's maximum message size — 256 KiB, the default of the
//! Microsoft quotas
//! (<https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quotas>),
//! raised by `vars.max_message_size` to the entity's — and by the link's own limits (the
//! engine halves a batch over the limit, as for kafka).
//!
//! # Outcomes
//!
//! The disposition awaited per message maps the safe way (only a verdict about *this
//! message* refuses it; everything else is transient, per the strikes rule of `engine`):
//!
//! - Accepted: sent. `expect` then sees, per message, `body`
//!   `{"entity": …, "message_id": …}` (no `expect`: met).
//! - Rejected with a condition that is about this message — `amqp:link:message-size-exceeded`,
//!   `amqp:decode-error`, `amqp:invalid-field` (the OASIS condition set:
//!   <https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html>):
//!   refused, its own strike.
//! - Released, Modified, a server-busy or throttling condition, a timeout without a
//!   disposition, a lost link, an unknown condition and a missing ack: transient. The message
//!   is not known to have reached the entity; only a retry can say.
//! - A missing entity or an authorization failure (the attach refused with `amqp:not-found`
//!   or `amqp:unauthorized-access`) is a whole-target refusal found once per attempt, not a
//!   per-message strike: the target does not create entities. So are a CBS reply that refuses
//!   the token (status 400, 401, 403 or 404) and a SASL PLAIN failure.
//!
//! # Concurrency and reconnects
//!
//! One connection and one sender link per endpoint, held across actions behind the endpoint's
//! lock (one action at a time per endpoint, as for kafka and amqp; different targets and
//! rules run in parallel). Any failure that leaves the state unknown drops the connection:
//! the next action reconnects, re-puts the token and re-attaches. The CBS links ride their
//! own session on the same connection, as the protocol guide shows.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use hmac::{Hmac, KeyInit, Mac};
use mysql_async::Pool;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::{DetachError, Receiver, SendError, Sender, SenderAttachError};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::definitions::{Error as PeerError, ErrorCondition};
use fe2o3_amqp::types::messaging::{
    AmqpValue, ApplicationProperties, Body, Data, Header, Message, MessageId,
    Outcome as Disposition, Properties,
};
use fe2o3_amqp::types::primitives::{LazyValue, SimpleValue, Symbol, Timestamp};
use fe2o3_amqp::Connection;

use crate::model::{FirePer, RuleDef, TargetDef};
use crate::response::{Expect, Row};
use crate::target::{rejected, var, Item, Sent};
use crate::tls::{Mode, TlsSpec};

/// The entity's default maximum message size: 256 KiB, the smallest option of the Microsoft
/// quotas; `vars.max_message_size` raises it to the entity's own limit.
pub const MAX_MESSAGE_BYTES: usize = 262_144;

/// The largest message the biggest tier of the quotas takes (Premium: 100 MiB).
const MAX_MESSAGE_BYTES_CAP: u64 = 104_857_600;

/// The default port of AMQP 1.0 over TLS (the protocol guide).
const DEFAULT_PORT: u16 = 5671;

/// The SAS token's lifetime and the refresh margin: the client replaces the token well before
/// it expires (the SAS doc's first best practice: renew well before expiration), and the
/// CBS spec terminates the links of an expired token.
const TOKEN_TTL_SECS: u64 = 3600;
const TOKEN_REFRESH_SECS: u64 = 900;

/// The suffix of a public-cloud namespace host.
const AZURE_SUFFIX: &str = ".servicebus.windows.net";

/// The CBS node of the connection (the protocol guide).
const CBS_NODE: &str = "$cbs";

/// The node name the CBS reply comes back on (the protocol guide's `cbs-client-reply-to`).
const CBS_REPLY_TO: &str = "lwf-cbs-reply-to";

/// The token type of a Service Bus SAS token (the protocol guide's table).
const SAS_TOKEN_TYPE: &str = "servicebus.windows.net:sastoken";

/// The Rejected conditions that are positively about one message (OASIS AMQP 1.0 §3.2.7 and
/// the disposition section of the messaging work draft). Every other Rejected condition says
/// nothing about this message and goes transient.
const MESSAGE_CONDITIONS: &[&str] = &[
    "amqp:link:message-size-exceeded",
    "amqp:decode-error",
    "amqp:invalid-field",
];

/// The attach conditions that refuse the whole target: the entity does not exist, or the
/// token carries no right to send (a sender link needs the Send right — the protocol guide's
/// rights table).
const ENTITY_CONDITIONS: &[(&str, &str)] = &[
    (
        "amqp:not-found",
        "the queue or topic does not exist (the target does not create it)",
    ),
    (
        "amqp:unauthorized-access",
        "the token cannot send to the entity (check vars.key_name, vars.key and the rule's Send right)",
    ),
];

// ---------------------------------------------------------------------------------------------
// Endpoint

/// An endpoint: `sb://<host>[:port]/<entity>`, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    /// Whether the host is a public Azure namespace (`*.servicebus.windows.net`): then TLS
    /// and the SAS keys are required. Any other host is the local-broker override.
    pub namespace: bool,
    pub host: String,
    pub port: u16,
    pub entity: String,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let rest = s.strip_prefix("sb://").ok_or_else(|| {
            anyhow!(
                "endpoint `{shown}`: use sb://<namespace>.servicebus.windows.net/<queue-or-topic>"
            )
        })?;
        if rest.contains('@') {
            bail!("endpoint `{shown}`: credentials belong in vars.key_name / vars.key, not in the URL");
        }
        if rest.contains(['?', '#']) {
            bail!("endpoint `{shown}`: no query string or fragment (TLS is the target's `tls`)");
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        if authority.contains('[') {
            bail!("endpoint `{shown}`: IPv6 addresses are not supported");
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h,
                p.parse::<u16>()
                    .with_context(|| format!("endpoint `{shown}`: port `{p}`"))?,
            ),
            None => (authority, DEFAULT_PORT),
        };
        if host.is_empty()
            || !host
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.'))
            || host.bytes().all(|c| matches!(c, b'-' | b'.'))
        {
            bail!("endpoint `{shown}`: the host is not a name");
        }
        let namespace = host.ends_with(AZURE_SUFFIX);
        let entity = entity_path(&shown, path, namespace)?;
        Ok(Addr {
            namespace,
            host: host.to_string(),
            port,
            entity,
        })
    }

    /// The SAS token's audience: the full resource URI of the entity (the SAS doc: "the full
    /// URI of the resource", in the `sb://` form it shows).
    pub fn audience(&self) -> String {
        format!("sb://{}/{}", self.host, self.entity.trim_start_matches('/'))
    }
}

/// The address of the entity, as the sender link's target takes it. For a public namespace it is
/// the Service Bus entity path: 1 to 260 letters, digits, `-`, `.` or `_`, not starting with `$`
/// (the system nodes: `$cbs` and friends) and no `/subscriptions/` segment (a sender writes to
/// the topic; the namespace fans the copy out).
fn entity_path(shown: &str, path: &str, namespace: bool) -> Result<String> {
    if !namespace {
        return local_address(shown, path);
    }
    let p = path.trim_matches('/');
    if p.is_empty() {
        bail!("endpoint `{shown}`: no queue or topic in the path");
    }
    if p.len() > 260 {
        bail!("endpoint `{shown}`: the entity path is 1 to 260 characters");
    }
    if p.starts_with('$') {
        bail!("endpoint `{shown}`: an entity path does not start with `$`");
    }
    if !p
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_'))
    {
        bail!("endpoint `{shown}`: an entity path holds letters, digits, `-`, `.` and `_` (a subscription is not sent to: send to the topic)");
    }
    if p.to_ascii_lowercase().contains("/subscriptions/") {
        bail!("endpoint `{shown}`: send to the topic, not to a subscription");
    }
    Ok(p.to_string())
}

/// The address of the local-broker override: the URL's path taken verbatim, `/`-prefixed, because
/// how an AMQP 1.0 broker reads an address is its own business (OASIS §3.4: the meaning of a
/// terminus's address is not defined by the spec) — RabbitMQ's current form is `/queues/<name>`
/// (its AMQP 1.0 guide). Letters, digits, `-`, `.`, `_`, `/` and `%` (percent-encoded segment
/// characters) only, and the Service Bus refusals ($-prefixed, a subscription) stay in force so a
/// typo cannot reach a system node.
fn local_address(shown: &str, path: &str) -> Result<String> {
    if path.is_empty() {
        bail!("endpoint `{shown}`: no queue or topic in the path");
    }
    if !path
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_' | b'/' | b'%'))
    {
        bail!("endpoint `{shown}`: a local broker's address holds letters, digits, `-`, `.`, `_`, `/` and `%`");
    }
    if path.starts_with('$') || path.starts_with("/$") {
        bail!("endpoint `{shown}`: an address does not start with `$`");
    }
    if path.to_ascii_lowercase().contains("/subscriptions/") {
        bail!("endpoint `{shown}`: send to the topic, not to a subscription");
    }
    let p = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if p.len() > 260 {
        bail!("endpoint `{shown}`: the address is 1 to 260 characters");
    }
    Ok(p)
}

/// The TLS settings for an endpoint. `tls: disabled` is allowed only for the local-broker
/// override: a namespace is TLS, always (the protocol guide: a TLS connection must exist
/// before the handshake).
pub fn tls_config(spec: &TlsSpec, namespace: bool) -> Result<Option<Arc<rustls::ClientConfig>>> {
    if spec.server_name.is_some() {
        bail!("`tls.server_name` is not supported by kind servicebus: the endpoint's own host is checked");
    }
    match spec.effective(true) {
        Mode::Disabled => {
            if namespace {
                bail!("a servicebus.windows.net endpoint needs TLS, but `tls` says disabled");
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

// ---------------------------------------------------------------------------------------------
// Auth

/// What the connection does for auth (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// SAS through CBS: the key name and the key (a reference) — the Azure way.
    Cbs { key_name: String, key_ref: String },
    /// SASL PLAIN with the account (the local override: a broker without CBS).
    Plain {
        username: String,
        password_ref: String,
    },
    /// Nothing (the local override: a broker that takes anonymous connections).
    Anonymous,
}

fn flag(vars: &Map<String, Value>, k: &str) -> Result<bool> {
    match vars.get(k) {
        None => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(v) => bail!("`vars.{k}` is {v} (a boolean)"),
    }
}

/// The target's auth from `vars` (see the module docs). Keys are required for a namespace;
/// username/password serve the local override; both at once is a load error (Service Bus
/// allows CBS only on SASL ANONYMOUS).
pub fn auth_of(vars: &Map<String, Value>, namespace: bool) -> Result<Auth> {
    let key_name = var(vars, "key_name");
    let key = var(vars, "key");
    let user = var(vars, "username");
    let pass = var(vars, "password");
    if key_name.is_some() || key.is_some() {
        let key_name = key_name.ok_or_else(|| {
            anyhow!("`vars.key` needs `vars.key_name` (the authorization rule name)")
        })?;
        let key = key.ok_or_else(|| anyhow!("`vars.key_name` needs `vars.key` (the key)"))?;
        if user.is_some() || pass.is_some() {
            bail!("kind servicebus takes SASL credentials or SAS keys, not both (Service Bus allows CBS only on SASL ANONYMOUS)");
        }
        if !crate::secrets::is_reference(key) {
            bail!("`vars.key` must be a reference (`env:NAME` or `file:/path`), never a literal");
        }
        return Ok(Auth::Cbs {
            key_name: key_name.to_string(),
            key_ref: key.to_string(),
        });
    }
    match (user, pass) {
        (None, None) => {
            if namespace {
                bail!(
                    "a servicebus.windows.net endpoint needs `vars.key_name` and `vars.key` \
                     (the SAS rule; Microsoft Entra ID is not supported by this kind)"
                );
            }
            Ok(Auth::Anonymous)
        }
        (Some(u), Some(p)) => {
            if !crate::secrets::is_reference(p) {
                bail!("`vars.password` must be a reference (`env:NAME` or `file:/path`), never a literal");
            }
            Ok(Auth::Plain {
                username: u.to_string(),
                password_ref: p.to_string(),
            })
        }
        _ => bail!("`vars.username` and `vars.password` come together"),
    }
}

/// The entity's maximum message size: 256 KiB, raised by `vars.max_message_size` to the
/// entity's limit (the quotas page: 256 KiB / 1 MiB by tier, 100 MiB on Premium).
pub fn max_message_bytes(vars: &Map<String, Value>) -> Result<usize> {
    match vars.get("max_message_size") {
        None => Ok(MAX_MESSAGE_BYTES),
        Some(v) => {
            let n = v
                .as_u64()
                .ok_or_else(|| anyhow!("`vars.max_message_size` is not a number"))?;
            if n == 0 || n > MAX_MESSAGE_BYTES_CAP {
                bail!("`vars.max_message_size` is {n} (1 to {MAX_MESSAGE_BYTES_CAP} bytes)");
            }
            Ok(n as usize)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// SAS tokens

/// The RFC 3986 percent-encoding behind the SAS doc's `urlencode` of the audience and of the
/// signature (letters, digits and `-._~` pass; everything else is `%XX`).
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Whether the token put to CBS still stands at `now` (epoch seconds): until the refresh
/// margin before its expiry.
fn token_stands(exp: u64, now: u64) -> bool {
    now < exp.saturating_sub(TOKEN_REFRESH_SECS)
}

/// Builds a SAS token for `audience` (see the module docs for the formula and the doc).
/// `key` is the Base64 key **as written** (the SAS doc: it must not be decoded before use).
pub fn sas_token(audience: &str, key_name: &str, key: &str, now: u64, ttl: u64) -> Result<String> {
    let sr = percent_encode(audience);
    let se = now + ttl;
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())
        .map_err(|e| anyhow!("vars.key is not a usable signing key ({e})"))?;
    mac.update(format!("{sr}\n{se}").as_bytes());
    let sig = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    );
    Ok(format!(
        "SharedAccessSignature sr={sr}&sig={}&se={se}&skn={}",
        percent_encode(&sig),
        percent_encode(key_name)
    ))
}

// ---------------------------------------------------------------------------------------------
// The rendered action

/// One message of a rendered action.
#[derive(Debug, Clone, PartialEq)]
pub struct Msg {
    pub value: Value,
    /// The value as the wire takes it (serialized once, when the body was parsed).
    bytes: Vec<u8>,
    pub session: Option<String>,
    pub props: BTreeMap<String, String>,
    /// The MessageId for duplicate detection (`vars.dedup`), or None.
    pub message_id: Option<String>,
    /// Set at parse when the message is refused without a send (no `session` for a
    /// session-enabled entity, or over the entity's message size): the message itself is the
    /// verdict, so the engine strikes it alone.
    pub refused: Option<String>,
}

/// A rendered action: the `{"messages": […]}` of a batch (message i is answered by key i).
pub struct Action {
    pub msgs: Vec<Msg>,
}

impl Action {
    /// Parses a rendered body. `Ok(None)`: an empty `messages` list (nothing to send). The
    /// messages' count must match `n_keys` (a `once` step has none). `session_required` is
    /// `vars.session` (a message without its `session` is then refused on its own), and
    /// `max_bytes` the entity's message size (a message whose value alone is larger is
    /// refused on its own too: the broker would refuse it with
    /// `amqp:link:message-size-exceeded` — the encoded frame is only bigger).
    pub fn parse(
        body: &[u8],
        n_keys: usize,
        session_required: bool,
        max_bytes: usize,
    ) -> Result<Option<Action>> {
        let v: Value = serde_json::from_slice(body).context("the send body is not JSON")?;
        let messages = v
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow!("the send body is `{{\"messages\": […]}}`"))?;
        let msgs = messages
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
                let session = match o.get("session") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                    Some(_) => bail!("message {i}: `session` is a non-empty string"),
                };
                let props = match o.get("properties") {
                    None | Some(Value::Null) => BTreeMap::new(),
                    Some(Value::Object(p)) => p
                        .iter()
                        .map(|(k, v)| {
                            let v = v.as_str().ok_or_else(|| {
                                anyhow!("message {i}: the property `{k}` is not a string")
                            })?;
                            Ok((k.clone(), v.to_string()))
                        })
                        .collect::<Result<BTreeMap<String, String>>>()?,
                    Some(_) => bail!("message {i}: `properties` is not an object of strings"),
                };
                for f in o.keys() {
                    if !matches!(f.as_str(), "value" | "session" | "properties") {
                        bail!("message {i} has the unknown field `{f}` (a message carries `value`, `session` and `properties`)");
                    }
                }
                let bytes = serde_json::to_vec(&value)
                    .map_err(|e| anyhow!("message {i}: the `value` cannot be written ({e})"))?;
                let mut refused = None;
                if session_required && session.is_none() {
                    refused = Some("the entity takes sessions but the message renders no `session`".into());
                } else if bytes.len() > max_bytes {
                    refused = Some(format!(
                        "the value is {} bytes, over the entity's {max_bytes}-byte maximum",
                        bytes.len()
                    ));
                }
                Ok(Msg {
                    value,
                    bytes,
                    session,
                    props,
                    message_id: None,
                    refused,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if msgs.len() != n_keys {
            bail!(
                "the send lists {} messages for {} keys (message i takes key i)",
                msgs.len(),
                n_keys
            );
        }
        if msgs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Action { msgs }))
        }
    }

    /// Sets the MessageId of each message to `hex(sha256(key + \0 + value))` (`vars.dedup`).
    fn dedup(&mut self, keys: &[String]) {
        for (i, m) in self.msgs.iter_mut().enumerate() {
            let mut h = Sha256::new();
            h.update(keys.get(i).map(String::as_str).unwrap_or(""));
            h.update([0]);
            h.update(&m.bytes);
            m.message_id = Some(hex_lower(&h.finalize()));
        }
    }

    /// The AMQP message of index `i` as the sender takes it.
    fn wire(&self, i: usize) -> Message<Data> {
        let m = &self.msgs[i];
        let mut p = Properties::new();
        if let Some(id) = &m.message_id {
            p.message_id = Some(MessageId::String(id.clone()));
        }
        if let Some(s) = &m.session {
            p.group_id = Some(s.clone());
        }
        p.content_type = Some(Symbol::from("application/json"));
        let mut builder = Message::builder()
            // `durable` (OASIS §3.2.3: the message is to be held durably): Service Bus keeps
            // every message whatever the flag says, but a broker that takes it seriously (the
            // RabbitMQ of the e2e) only survives its own restart with it set.
            .header(Header::builder().durable(true).build())
            .properties(p);
        if !m.props.is_empty() {
            let mut ap = ApplicationProperties::default();
            for (k, v) in &m.props {
                ap.0.insert(k.clone(), SimpleValue::String(v.clone()));
            }
            builder = builder.application_properties(ap);
        }
        // The document as an AMQP Data section: `content_type` says it is JSON, and a consumer
        // of the entity reads it as the body (an AmqpValue section of the same bytes is legal
        // per OASIS §3.2.7, but a consumer of the other protocol on the same broker does not
        // see it as the body — the e2e reads the queue over AMQP 0-9-1 and proved it).
        builder.data(m.bytes.clone()).build()
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------------------------
// Outcomes

/// The engine's reading of one disposition, shared by its message.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// Sent: Accepted. `expect` then sees the row of the message.
    Ok,
    /// Refused for this message (rejected: a strike).
    Refused(String),
    /// Not known to have gone through: retry.
    Transient(String),
}

/// How the sender link went away: after either case the fate of the unanswered messages
/// is unknown, so they are transient and the connection is dropped. (Deliberately not a
/// `SendError` of the client's: a timeout fabricates no protocol state.)
enum Lost {
    /// The peer detached or closed the link, possibly naming a condition.
    Link {
        condition: Option<String>,
        why: String,
    },
    /// Nothing came back within the target's time limit.
    Slow,
}

/// Whether a Rejected condition is a verdict on THIS message (refused) — the three of
/// `MESSAGE_CONDITIONS` — or says nothing about it (transient, with the condition kept in the
/// message). Only a positive verdict on the message may refuse it (the strikes rule).
fn is_message_condition(c: &str) -> bool {
    MESSAGE_CONDITIONS.contains(&c)
}

/// Whether an attach condition refuses the whole target (a missing entity or a token without
/// the Send right): refused once, not per message.
fn entity_refusal(c: &str) -> Option<&'static str> {
    ENTITY_CONDITIONS
        .iter()
        .find(|(k, _)| *k == c)
        .map(|(_, why)| *why)
}

/// The wire text of an error condition (`amqp:not-found`, `com.microsoft:server-busy`, …).
/// The 0.14 types crate displays an `ErrorCondition` as its Rust variant name, so the
/// symbol is taken through its `From<&_> for Symbol` conversions instead.
fn condition_text(c: &ErrorCondition) -> String {
    let s = match c {
        ErrorCondition::AmqpError(e) => Symbol::from(e),
        ErrorCondition::ConnectionError(e) => Symbol::from(e),
        ErrorCondition::SessionError(e) => Symbol::from(e),
        ErrorCondition::LinkError(e) => Symbol::from(e),
        ErrorCondition::Custom(s) => s.clone(),
    };
    s.as_str().to_string()
}

/// The condition of a peer error, as text.
fn peer_condition(e: &PeerError) -> String {
    condition_text(&e.condition)
}

/// The condition an attach failure carries, if the peer named one.
fn attach_condition(err: &SenderAttachError) -> Option<String> {
    match err {
        SenderAttachError::RemoteClosedWithError(e) => Some(peer_condition(e)),
        _ => None,
    }
}

/// The condition a link loss carries, if the peer named one.
fn detach_condition(err: &DetachError) -> Option<String> {
    match err {
        DetachError::RemoteDetachedWithError(e) | DetachError::RemoteClosedWithError(e) => {
            Some(peer_condition(e))
        }
        _ => None,
    }
}

/// One CBS reply: the `status-code` / `status-description` application properties of the
/// response (the protocol guide's table). `Ok(())` for 200/201; `Err(true)`: the token itself
/// is refused (400, 401, 403, 404: malformed token, bad key or key name, no right, unknown
/// audience — a retry cannot help); `Err(false)`: nothing is known (5xx, a malformed reply,
/// and 408/429 — a timeout or throttling says nothing about the token).
fn cbs_verdict(status: Option<i64>, description: Option<&str>) -> Result<(), (bool, String)> {
    let desc = description.unwrap_or("no description");
    match status {
        Some(200) | Some(201) => Ok(()),
        Some(s @ (400 | 401 | 403 | 404)) => {
            Err((true, format!("CBS refused the token (status {s}): {desc}")))
        }
        Some(s) => Err((
            false,
            format!("CBS did not accept the token (status {s}): {desc}"),
        )),
        None => Err((false, "the CBS reply carries no status-code".into())),
    }
}

/// The `status-code` / `status-description` of a CBS reply message, as far as it reads
/// (missing or malformed properties are "nothing known").
fn cbs_reply_parts(ap: Option<&ApplicationProperties>) -> (Option<i64>, Option<String>) {
    let Some(map) = ap.map(|a| &a.0) else {
        return (None, None);
    };
    let status = map.get("status-code").and_then(|v| match v {
        SimpleValue::Int(i) => Some(*i as i64),
        SimpleValue::Long(l) => Some(*l),
        SimpleValue::Ulong(u) => i64::try_from(*u).ok(),
        SimpleValue::String(s) => s.parse().ok(),
        _ => None,
    });
    let desc = map.get("status-description").and_then(|v| match v {
        SimpleValue::String(s) => Some(s.clone()),
        _ => None,
    });
    (status, desc)
}

/// The outcome of one awaited disposition.
fn disposition_outcome(o: &Disposition) -> Outcome {
    match o {
        Disposition::Accepted(_) => Outcome::Ok,
        Disposition::Rejected(r) => {
            let (cond, desc) = match &r.error {
                Some(e) => (Some(condition_text(&e.condition)), e.description.clone()),
                None => (None, None),
            };
            let desc = desc.unwrap_or_else(|| "no description".into());
            match cond {
                Some(c) if is_message_condition(&c) => {
                    Outcome::Refused(format!("the entity refused the message ({c}): {desc}"))
                }
                // Server-busy, throttling, an unknown condition, a Rejected without an
                // error: no verdict on this message.
                c => Outcome::Transient(format!(
                    "the entity did not take the message (rejected{}): {desc}",
                    c.map(|s| format!(", {s}")).unwrap_or_default()
                )),
            }
        }
        Disposition::Released(_) => Outcome::Transient(
            "the entity released the message (refused to take it, kept it re-deliverable)".into(),
        ),
        Disposition::Modified(m) => Outcome::Transient(format!(
            "the entity modified the message{}",
            m.delivery_failed
                .map(|f| format!(" (delivery_failed: {f})"))
                .unwrap_or_default()
        )),
    }
}

/// Fills the unanswered slots with the whole-action verdict a lost link leaves behind.
fn fill_unknown(out: &mut [Option<Outcome>], why: String) {
    for o in out {
        if o.is_none() {
            *o = Some(Outcome::Transient(why.clone()));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The client

/// The CBS pair of the connection (its own session, as the protocol guide shows). The links
/// stay open so a token can be re-put without touching the payload traffic. The session is
/// owned, never called: dropping it would end the session the two links live on.
struct Cbs {
    _session: SessionHandle<()>,
    tx: Sender,
    rx: Receiver,
}

/// The connection of one endpoint: the entity session and its sender, the CBS links when the
/// auth is SAS, and the epoch second the put token lives until. The session and the
/// connection are owned, never called: dropping them would end the link traffic.
struct Inner {
    _connection: ConnectionHandle<()>,
    _session: SessionHandle<()>,
    sender: Sender,
    cbs: Option<Cbs>,
    token_exp: u64,
}

/// One endpoint of a target.
pub struct Client {
    /// The endpoint as written (no credentials): the name in errors.
    pub url: String,
    addr: Addr,
    auth: Auth,
    tls: Option<Arc<rustls::ClientConfig>>,
    max_bytes: usize,
    session_enabled: bool,
    dedup: bool,
    timeout: Duration,
    /// The connection. Holding the lock also keeps one action of the endpoint in flight at a
    /// time (see the module docs).
    inner: Mutex<Option<Inner>>,
}

impl Client {
    /// The endpoint's context (`ctx` in templates) and its client.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint)?;
        let tls = tls_config(&def.tls, addr.namespace)?;
        let auth = auth_of(&def.vars, addr.namespace)?;
        let max_bytes = max_message_bytes(&def.vars)?;
        let ctx = serde_json::json!({
            "host": addr.host,
            "port": addr.port,
            "entity": addr.entity,
            "url": endpoint.trim(),
        });
        Ok((
            ctx,
            Client {
                url: endpoint.trim().to_string(),
                addr,
                auth,
                tls,
                max_bytes,
                session_enabled: flag(&def.vars, "session")?,
                dedup: flag(&def.vars, "dedup")?,
                timeout: def.timeout,
                inner: Mutex::new(None),
            },
        ))
    }

    /// Opens the connection: TCP/TLS, the SASL handshake the auth selects, the CBS
    /// put-token when SAS, the entity session and its sender. A whole-target refusal
    /// (`Err` of [`rejected`]) comes from a missing entity, an unauthorized attach, a token
    /// CBS refuses, or a SASL failure; anything else is transient.
    async fn open(&self) -> Result<Inner> {
        let scheme = if self.tls.is_some() { "amqps" } else { "amqp" };
        let url = format!("{scheme}://{}:{}", self.addr.host, self.addr.port);
        let mut builder =
            Connection::builder().container_id(format!("lazywf {}", std::process::id()));
        match &self.auth {
            // Service Bus allows CBS only on ANONYMOUS (the protocol guide).
            Auth::Cbs { .. } | Auth::Anonymous => {
                builder = builder.sasl_profile(SaslProfile::Anonymous)
            }
            Auth::Plain {
                username,
                password_ref,
            } => {
                let username = crate::secrets::resolve(username).context("vars.username")?;
                let password = crate::secrets::resolve(password_ref).context("vars.password")?;
                builder = builder.sasl_profile(SaslProfile::Plain { username, password });
            }
        }
        // The rustls connector moves the builder to another typestate, so the two cases
        // are opened in their own branch.
        let opened = match &self.tls {
            Some(cfg) => {
                let builder = builder.tls_connector(tokio_rustls::TlsConnector::from(cfg.clone()));
                tokio::time::timeout(self.timeout, builder.open(url.as_str())).await
            }
            None => tokio::time::timeout(self.timeout, builder.open(url.as_str())).await,
        };
        let connection = opened
            .map_err(|_| {
                anyhow!(
                    "connecting to {} timed out after {:?}",
                    self.url,
                    self.timeout
                )
            })?
            .map_err(|e| {
                let text = e.to_string();
                // A SASL refusal is a credential problem the retry cannot fix.
                if text.to_ascii_lowercase().contains("sasl") {
                    rejected(format!("{}: {text}", self.url))
                } else {
                    anyhow!("connecting to {} failed: {e}", self.url)
                }
            })?;
        // The session, the CBS node and the entity link are the peer's to answer, so the
        // whole of it gets the same budget as the socket: an attach nobody replies to is a
        // busy or stuck broker, and a worker stuck inside one fire is a worker that stops
        // draining every other target.
        tokio::time::timeout(self.timeout, self.open_rest(connection))
            .await
            .map_err(|_| {
                anyhow!(
                    "opening the session and sender link of {} timed out after {:?} \
                     (the broker did not answer begin or attach)",
                    self.url,
                    self.timeout
                )
            })?
    }

    /// The CBS negotiation and the entity attach of a fresh connection (split off `open` so
    /// the unit tests can drive it with an established local connection).
    async fn open_rest(&self, mut connection: ConnectionHandle<()>) -> Result<Inner> {
        let mut cbs = None;
        let token_exp = match &self.auth {
            Auth::Cbs { key_name, key_ref } => {
                let key = crate::secrets::resolve(key_ref)
                    .with_context(|| format!("vars.key for {}", self.url))?;
                let audience = self.addr.audience();
                let now = crate::time::unix_now_ms() / 1000;
                let token = sas_token(&audience, key_name, &key, now, TOKEN_TTL_SECS)?;
                let mut pair = Cbs::open(&mut connection).await?;
                put_cbs_token(&mut pair.tx, &mut pair.rx, &token, &audience, self.timeout)
                    .await
                    .map_err(|(fatal, why)| {
                        if fatal {
                            rejected(format!("{}: {why}", self.url))
                        } else {
                            anyhow!("{}: {why}", self.url)
                        }
                    })?;
                cbs = Some(pair);
                now + TOKEN_TTL_SECS
            }
            _ => 0,
        };
        let mut session = fe2o3_amqp::Session::begin(&mut connection)
            .await
            .map_err(|e| anyhow!("beginning the session of {} failed: {e}", self.url))?;
        let sender = match Sender::attach(&mut session, "lwf-tx", self.addr.entity.clone()).await {
            Ok(s) => s,
            Err(e) => {
                let why = attach_condition(&e)
                    .as_deref()
                    .and_then(entity_refusal)
                    .map(|w| rejected(format!("{}: {w}", self.url)));
                return match why {
                    Some(rejected) => Err(rejected),
                    None => Err(anyhow!("attaching the sender of {} failed: {e}", self.url)),
                };
            }
        };
        Ok(Inner {
            _connection: connection,
            _session: session,
            sender,
            cbs,
            token_exp,
        })
    }

    /// Sends the action's messages (message i takes key i), pipelining the transfers and
    /// awaiting a disposition for every one of them under the target's time limit. The
    /// connection is dropped when anything left the state unknown (as amqp drops its
    /// session): the next action reconnects and re-puts the token.
    async fn send_batch(&self, keys: &[String], action: &mut Action) -> Result<Vec<Outcome>> {
        let mut slot = self.inner.lock().await;
        if slot.is_none() {
            *slot = Some(self.open().await?);
        }
        let inner = slot.as_mut().expect("opened above");
        // The per-action limit starts once the connection and the token stand: the
        // dispositions below are what it times.
        let deadline = tokio::time::Instant::now() + self.timeout;
        // Refresh the token well before its expiry (the CBS spec drops the links of an
        // expired token).
        let now = crate::time::unix_now_ms() / 1000;
        if let (Some(cbs), Auth::Cbs { key_name, key_ref }) = (&mut inner.cbs, &self.auth) {
            if !token_stands(inner.token_exp, now) {
                let key = crate::secrets::resolve(key_ref)
                    .with_context(|| format!("vars.key for {}", self.url))?;
                let audience = self.addr.audience();
                let token = sas_token(&audience, key_name, &key, now, TOKEN_TTL_SECS)?;
                let (tx, rx) = (&mut cbs.tx, &mut cbs.rx);
                match tokio::time::timeout(
                    self.timeout,
                    put_cbs_token(tx, rx, &token, &audience, self.timeout),
                )
                .await
                {
                    Err(_) => {
                        *slot = None;
                        return Err(anyhow!("{}: the CBS token refresh timed out", self.url));
                    }
                    Ok(Err((fatal, why))) => {
                        *slot = None;
                        return Err(if fatal {
                            rejected(format!("{}: {why}", self.url))
                        } else {
                            anyhow!("{}: {why}", self.url)
                        });
                    }
                    Ok(Ok(())) => inner.token_exp = now + TOKEN_TTL_SECS,
                }
            }
        }
        if self.dedup {
            action.dedup(keys);
        }
        let n = action.msgs.len();
        let mut out: Vec<Option<Outcome>> = vec![None; n];
        // Phase 1: hand every message to the link; phase 2: await each disposition, in
        // order. A pipelined transfer is a detached future (`send_batchable`), so the
        // link keeps pumping while the previous disposition is awaited.
        type Pending = Pin<Box<dyn Future<Output = Result<Disposition, SendError>> + Send>>;
        let mut futs: Vec<Option<Pending>> = Vec::with_capacity(n);
        let mut lost: Option<Lost> = None;
        for (i, m) in action.msgs.iter().enumerate() {
            if m.refused.is_some() || lost.is_some() {
                futs.push(None);
                continue;
            }
            let msg = action.wire(i);
            match tokio::time::timeout_at(deadline, inner.sender.send_batchable(msg)).await {
                Err(_) => {
                    lost = Some(Lost::Slow);
                    futs.push(None);
                    break;
                }
                Ok(Err(SendError::MessageEncodeError)) => {
                    // The message never left and never would: a deterministic verdict
                    // on this message alone, the link is untouched.
                    out[i] = Some(Outcome::Refused("the message could not be encoded".into()));
                    futs.push(None);
                }
                Ok(Err(e)) => {
                    let condition = match &e {
                        SendError::Detached(d) => detach_condition(d),
                        _ => None,
                    };
                    lost = Some(Lost::Link {
                        condition,
                        why: e.to_string(),
                    });
                    futs.push(None);
                    break;
                }
                Ok(Ok(f)) => futs.push(Some(Box::pin(f))),
            }
        }
        // Pre-refused messages (session missing, over size): their own verdict, no send.
        for (i, m) in action.msgs.iter().enumerate() {
            if let Some(why) = &m.refused {
                out[i] = Some(Outcome::Refused(why.clone()));
            }
        }
        for (i, f) in futs.into_iter().enumerate() {
            if out[i].is_some() {
                continue;
            }
            let Some(f) = f else {
                continue; // answered below with the link verdict
            };
            if lost.is_some() {
                continue; // the link is gone: every remaining message has no answer
            }
            match tokio::time::timeout_at(deadline, f).await {
                Err(_) => {
                    // No disposition within the target's limit: nothing is known.
                    lost = Some(Lost::Slow);
                    break;
                }
                Ok(Err(e)) => match &e {
                    // The link itself went away: this message and every later one is
                    // unknown until the reconnect says otherwise.
                    SendError::Detached(_) | SendError::LinkStateError(_) => {
                        let condition = match &e {
                            SendError::Detached(d) => detach_condition(d),
                            _ => None,
                        };
                        lost = Some(Lost::Link {
                            condition,
                            why: e.to_string(),
                        });
                        break;
                    }
                    // The link stands, this delivery did not settle: not known in.
                    _ => out[i] = Some(Outcome::Transient(format!("the send failed: {e}"))),
                },
                Ok(Ok(o)) => out[i] = Some(disposition_outcome(&o)),
            }
        }
        // The whole-action verdict a lost link leaves behind.
        match lost {
            None => {}
            Some(Lost::Link { condition, why }) => {
                // The connection goes: the state is unknown, as amqp drops its session.
                *slot = None;
                if let Some(why) = condition.as_deref().and_then(entity_refusal) {
                    // The link said the entity is gone or the token is not good: a
                    // whole-target refusal, found once. (Messages already Answered keep
                    // their answer: the engine only strikes what is not known to be in.)
                    let answered = out.iter().any(|o| matches!(o, Some(Outcome::Ok)));
                    if !answered {
                        return Err(rejected(format!("{}: {why}", self.url)));
                    }
                    fill_unknown(&mut out, format!("the link was lost ({why})"));
                } else {
                    fill_unknown(&mut out, format!("the link was lost: {why}"));
                }
            }
            Some(Lost::Slow) => {
                *slot = None;
                fill_unknown(
                    &mut out,
                    format!("no disposition within the target's {:?}", self.timeout),
                );
            }
        }
        Ok(out
            .into_iter()
            .map(|o| o.expect("every message was answered"))
            .collect())
    }

    /// Produces the outcomes of one rendered action (message i takes key i). `Ok(None)`:
    /// nothing rendered. `Err`: the whole call failed (connect, token or attach), as for
    /// kafka.
    async fn items_of(&self, rendered: &str, keys: &[String]) -> Result<Option<Vec<Outcome>>> {
        let req = match crate::http::RawRequest::parse(rendered) {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(None),
            Err(e) => return Err(rejected(format!("the rendered request: {e:#}"))),
        };
        if req.method != "POST" || req.target != "/" {
            return Err(rejected(format!(
                "servicebus targets only send: {} {} is not POST /",
                req.method, req.target
            )));
        }
        let Some(mut action) =
            Action::parse(&req.body, keys.len(), self.session_enabled, self.max_bytes)
                .map_err(|e| rejected(format!("the send body: {e:#}")))?
        else {
            return Ok(None);
        };
        self.send_batch(keys, &mut action).await.map(Some)
    }
}

impl Cbs {
    /// The `$cbs` request/response pair of a fresh connection: a sender to `$cbs` and a
    /// receiver whose target is the reply node (the protocol guide's example).
    async fn open(connection: &mut ConnectionHandle<()>) -> Result<Cbs> {
        let mut session = fe2o3_amqp::Session::begin(connection)
            .await
            .map_err(|e| anyhow!("beginning the CBS session failed: {e}"))?;
        let tx = Sender::attach(&mut session, "lwf-cbs-tx", CBS_NODE)
            .await
            .map_err(|e| anyhow!("attaching to {CBS_NODE} failed: {e}"))?;
        let rx = Receiver::builder()
            .name("lwf-cbs-rx")
            .source(CBS_NODE)
            .target(CBS_REPLY_TO)
            .attach(&mut session)
            .await
            .map_err(|e| anyhow!("attaching the {CBS_NODE} reply link failed: {e}"))?;
        Ok(Cbs {
            _session: session,
            tx,
            rx,
        })
    }
}

/// The `put-token` request of CBS as the sender takes it (the protocol guide's `PutCbsToken`):
/// `operation`, `type`, `name` and `expiration` as application properties, the token itself as
/// the AmqpValue body, and a `reply-to` the reply is expected on. Split off [`put_cbs_token`]
/// so its shape is checked without a broker.
fn cbs_put_token_msg(
    token: &str,
    audience: &str,
    now_ms: u64,
    timeout: Duration,
) -> Message<AmqpValue<String>> {
    let mut p = Properties::new();
    p.message_id = Some(MessageId::String(format!("lwf-cbs-{}", std::process::id())));
    p.reply_to = Some(CBS_REPLY_TO.into());
    let mut ap = ApplicationProperties::default();
    ap.0.insert("operation".into(), SimpleValue::String("put-token".into()));
    ap.0.insert("type".into(), SimpleValue::String(SAS_TOKEN_TYPE.into()));
    ap.0.insert("name".into(), SimpleValue::String(audience.into()));
    ap.0.insert(
        "expiration".into(),
        SimpleValue::Timestamp(Timestamp::from_milliseconds(
            (now_ms + timeout.as_millis() as u64 + TOKEN_REFRESH_SECS * 1000) as i64,
        )),
    );
    Message::builder()
        .properties(p)
        .application_properties(ap)
        .value(token.to_string())
        .build()
}

/// Puts one SAS token to `$cbs` and awaits the status of the reply (the protocol guide's
/// `PutCbsToken`). `Err((true, ..))`: the token is refused outright (status 400, 401, 403
/// or 404, or a SASL-class failure) — a whole-target refusal; `Err((false, ..))`: nothing
/// is known — transient.
async fn put_cbs_token(
    tx: &mut Sender,
    rx: &mut Receiver,
    token: &str,
    audience: &str,
    timeout: Duration,
) -> Result<(), (bool, String)> {
    let msg = cbs_put_token_msg(token, audience, crate::time::unix_now_ms(), timeout);
    let sent = tx
        .send(msg)
        .await
        .map_err(|e| (false, format!("sending the CBS token failed: {e}")))?;
    if !matches!(sent, Disposition::Accepted(_)) {
        return Err((
            false,
            format!("the CBS node did not accept the token request ({sent:?})"),
        ));
    }
    let delivery = tokio::time::timeout(timeout, rx.recv::<Body<LazyValue>>())
        .await
        .map_err(|_| {
            (
                false,
                "no CBS reply within the target's time limit".to_string(),
            )
        })?
        .map_err(|e| (false, format!("reading the CBS reply failed: {e}")))?;
    let parts = cbs_reply_parts(delivery.message().application_properties.as_ref());
    let _ = rx.accept(&delivery).await;
    cbs_verdict(parts.0, parts.1.as_deref())
}

/// Sends one rendered `once` action (no keys) and checks `expect` on every message.
pub async fn send(client: &Client, rendered: &str, expect: &Expect, pool: &Pool) -> Result<Sent> {
    let Some(outcomes) = client.items_of(rendered, &[]).await? else {
        return Ok(Sent::Skipped);
    };
    for (i, o) in outcomes.iter().enumerate() {
        match o {
            Outcome::Ok => expect
                .row(pool, |u| Row::servicebus(u, &client.addr.entity, None))
                .await
                .with_context(|| format!("message {i} of {}", client.url))?
                .map_err(|e| rejected(format!("{} message {i}: {e}", client.url)))?,
            Outcome::Refused(why) => {
                return Err(rejected(format!(
                    "{} message {i}: the entity refused it: {why}",
                    client.url
                )))
            }
            Outcome::Transient(why) => bail!("{} message {i}: {why}", client.url),
        }
    }
    Ok(Sent::Ok)
}

/// Sends one rendered action (message i takes key i) and checks `expect` on every sent
/// message. `Ok(vec!)`: nothing rendered. `Err`: the whole call failed (the messages have no
/// answer).
pub async fn send_items(
    client: &Client,
    rendered: &str,
    keys: &[String],
    expect: &Expect,
    pool: &Pool,
) -> Result<Vec<Item>> {
    let Some(outcomes) = client.items_of(rendered, keys).await? else {
        return Ok(vec![Item::Ok; keys.len()]);
    };
    let mut out = Vec::with_capacity(outcomes.len());
    for (i, o) in outcomes.iter().enumerate() {
        match o {
            Outcome::Ok => {
                let item = match expect
                    .row(pool, |u| {
                        Row::servicebus(u, &client.addr.entity, keys.get(i).map(String::as_str))
                    })
                    .await
                    .with_context(|| format!("message {i} of {}", client.url))?
                {
                    Ok(()) => Item::Ok,
                    Err(why) => Item::Rejected(format!("message {i} of {}: {why}", client.url)),
                };
                out.push(item);
            }
            Outcome::Refused(why) => out.push(Item::Rejected(format!(
                "message {i} of {}: the entity refused it: {why}",
                client.url
            ))),
            Outcome::Transient(why) => out.push(Item::Transient(format!(
                "message {i} of {}: {why}",
                client.url
            ))),
        }
    }
    Ok(out)
}

/// Rule-load checks for a servicebus rule: a batch over the entity's limit (256 KiB, raised
/// by the target's `vars.max_message_size`) is refused at load.
pub fn rule_errors(def: &RuleDef, limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    if def.fire_per == FirePer::Batch && def.batch_max_bytes.is_some_and(|n| n > limit) {
        out.push(format!(
            "`batch_max_bytes` {} is over {limit}, the most a servicebus entity takes (raise it with the target's `vars.max_message_size`)",
            def.batch_max_bytes.unwrap()
        ));
    }
    out
}

/// Load checks for a target of kind servicebus (from `TargetDef::from_row`).
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    // One endpoint: one namespace and one entity. A batch is answered per item, which the
    // engine sends to a single endpoint — as for kafka, sqs and sns.
    if endpoints.len() != 1 {
        bail!(
            "kind servicebus takes exactly one endpoint: \
             sb://<namespace>.servicebus.windows.net/<queue-or-topic>"
        );
    }
    let tls = TlsSpec::parse(tls).context("`tls`")?;
    for e in endpoints {
        let a = Addr::parse(e)?;
        tls_config(&tls, a.namespace)?;
        auth_of(vars, a.namespace)?;
    }
    max_message_bytes(vars)?;
    flag(vars, "session")?;
    flag(vars, "dedup")?;
    Ok(())
}

/// A `sb://` endpoint belongs to kind servicebus only.
pub fn refuse_scheme(kind_is_servicebus: bool, endpoints: &[String]) -> Result<()> {
    if !kind_is_servicebus && endpoints.iter().any(|e| e.trim().starts_with("sb://")) {
        bail!("a sb:// endpoint needs kind servicebus");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/targets_servicebus.rs"]
mod tests;
