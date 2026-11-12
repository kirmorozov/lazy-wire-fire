//! The `rabbitmq-stream` kind: one publish batch per action to a stream (or to the partitions
//! of a super stream) over RabbitMQ's stream protocol, port 5552 (5551 over TLS). Client:
//! rabbitmq-stream-client (the official one, pure Rust), used through its raw `Client`: the
//! high-level `Producer` cannot carry this module's outcome contract (a hardcoded 1 s confirm
//! timeout, `todo!()` on publishing ids it does not expect, a batch task that swallows publish
//! errors), and `Environment` re-resolves partition leaders from broker-reported hosts, which
//! would send connections to hosts `LAZYWF_ALLOWED_HOSTS` never approved. The raw `Client` is
//! exactly the kafka "kind owns its connection" shape: the confirms of a publish arrive on the
//! connection's handler and this module waits for them itself.
//!
//! # Endpoint and credentials
//!
//! `rabbitmq-stream://<host>[:port]/[<vhost>/]<stream>`, or `rabbitmq-stream+tls://` for TLS
//! (default ports 5552 / 5551 — RabbitMQ's own names and ports, as in its docs and its Java
//! and Go clients). One endpoint per target: one node and the stream its batches go to (a
//! batch is answered per item, which the engine sends to a single endpoint, as for kafka).
//! No credentials in the URL: `vars.username` (a reference or a literal, as amqp takes it) and
//! `vars.password` (a reference, never a literal) are the SASL PLAIN account the protocol
//! always asks for.
//!
//! The stream must exist (the target never creates one); so must every partition of a super
//! stream. A super stream is used by name: the first batch asks the broker for its partitions
//! (and caches them), each record's `routing_key` is placed on one of them by murmur3 (the
//! hash the client library's own super-stream producer uses, seed 104729, modulo the broker's
//! partition count), and one publisher is declared per partition stream — all of it on the
//! connection to the endpoint's host. A partition whose leader is another node
//! answers `StreamNotAvailable`, which is transient here: front the cluster with a stream load
//! balancer, or name the node that hosts the partitions.
//!
//! # The rendered action
//!
//! One action is one publish batch; item i is answered by key i, as
//!
//! ```text
//! POST / HTTP/1.1
//! Content-Type: application/json
//!
//! {"records": [{"value": …, "routing_key": "order-1"}, {"value": …, "routing_key": null}]}
//! ```
//!
//! A record carries its rendered `value` (one JSON value, serialized compactly) and its
//! `routing_key` (a string, or null). The routing key decides the partition of a super stream
//! (murmur3 of it, modulo the partitions the broker lists); on a plain stream it is carried but
//! unused. A super stream record without one is refused alone (nothing to route by). The
//! document key `keys[i]`
//! travels as the application property `lazywf_key` (the counterpart of amqp's message id);
//! with no keys (a `once` step) the records carry none.
//!
//! # Outcomes
//!
//! - a publish confirm for the record: sent. `expect` then sees, per record, `body`
//!   `{"stream": …, "publishing_id": …}` and status 200 (no `expect`: met).
//! - a publish error whose code speaks about this one message (`FrameTooLarge`): refused
//!   alone, its neighbours go through.
//! - `AccessRefused`, the broker saying this account may not publish to this stream: a
//!   refusal of the **whole call**, like the declare and the connect ones — never a strike on
//!   the keys. A super stream's partitions are separate streams with separate rights, so a
//!   missing right would otherwise refuse the keys of one partition while the others go
//!   through, and `judge_strikes` would drop them after five: a permissions mistake would
//!   quietly lose data. Refusing the call instead, the rule keeps its position and
//!   `last_error` says what to fix.
//! - everything that does not positively refuse this message is transient — a lost connection,
//!   no confirm within the target's time limit, and every code that says "not now" or nothing
//!   (`StreamNotAvailable`, `InternalError`, a publisher gone stale, an unknown code). The
//!   connection drops and the next action reconnects; a retry cannot drop a key.
//! - a stream the broker does not know at its declare, and a SASL or vhost failure at the
//!   connect: a refusal of the whole target at link level, once, as kafka's unknown topic at
//!   the metadata check — never a per-message strike.
//!
//! # Concurrency
//!
//! One publish batch at a time per endpoint: the endpoint's lock is held over all of an
//! action's publishes. Different targets and rules run in parallel.
//!
//! # TLS
//!
//! `verify-full` with `tls.ca` naming exactly one PEM file: the crate trusts that file and
//! nothing else (no Mozilla roots, no OS store), so a `+tls://` target without `tls.ca` is a
//! load error — name the CA. `required` and `verify-ca` (no host name check) and
//! `tls.server_name` are load errors that name them, as for amqp; the crate checks the
//! endpoint host's name itself. Never the crate's `Untrusted` mode: it verifies nothing.
//! lazywf installs the ring rustls provider as the process default (src/main.rs), so this
//! client's handshakes run on ring too; aws-lc-rs, which the crate's tokio-rustls pulls into
//! the tree, stays compiled and unused.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use mysql_async::Pool;
use rabbitmq_stream_client::error::ClientError;
use rabbitmq_stream_client::types::{Message, Response, ResponseCode, ResponseKind};
use rabbitmq_stream_client::{Client as StreamClient, ClientOptions, TlsConfiguration};
use serde_json::{Map, Value};
use tokio::sync::{oneshot, Mutex};

use crate::model::{FirePer, RuleDef, TargetDef};
use crate::response::{Expect, Row};
use crate::target::{rejected, var, Item, Sent};
use crate::tls::{Mode, TlsSpec};

/// The frame limit both sides tune to by default: 1 MiB. The crate sends a publish batch as
/// one frame and does not split it, so this is the largest batch the target takes.
pub const MESSAGE_MAX_BYTES: usize = 1_048_576;

/// The default ports (plain / TLS).
const DEFAULT_PORT: u16 = 5552;
const DEFAULT_SECURE_PORT: u16 = 5551;

/// The seed of the super stream hash routing: the one `rabbitmq-stream-client`'s own
/// super-stream producer hashes with (`superstream.rs`), so the two place a key alike.
const SUPER_STREAM_SEED: u32 = 104729;

/// An endpoint: `rabbitmq-stream(+tls)://<host>[:port]/[<vhost>/]<stream>`, no credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub secure: bool,
    pub host: String,
    pub port: u16,
    pub vhost: String,
    pub stream: String,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("rabbitmq-stream+tls://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("rabbitmq-stream://") {
            (false, r)
        } else {
            bail!(
                "endpoint `{shown}`: use rabbitmq-stream://host[:port]/[vhost/]stream \
                 or rabbitmq-stream+tls://…"
            );
        };
        if rest.contains('@') {
            bail!(
                "endpoint `{shown}`: credentials belong in vars.username and vars.password, \
                 not in the URL"
            );
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
            None => (
                authority,
                if secure {
                    DEFAULT_SECURE_PORT
                } else {
                    DEFAULT_PORT
                },
            ),
        };
        if host.is_empty() || host.bytes().all(|c| matches!(c, b'-' | b'.')) {
            bail!("endpoint `{shown}`: no host");
        }
        // `<stream>` (the default vhost) or `<vhost>/<stream>`; `%2F` names the default vhost.
        let segs: Vec<String> = path.split('/').map(percent_decode).collect();
        if segs.iter().any(|s| s.is_empty()) {
            bail!(
                "endpoint `{shown}`: the path is `<stream>` or `<vhost>/<stream>` \
                 (`%2F` names the default vhost)"
            );
        }
        let (vhost, stream) = match segs.as_slice() {
            [stream] => ("/".to_string(), stream.clone()),
            [vhost, stream] => (vhost.clone(), stream.clone()),
            _ => bail!(
                "endpoint `{shown}`: the path is `<stream>` or `<vhost>/<stream>` \
                 (`%2F` names the default vhost)"
            ),
        };
        Ok(Addr {
            secure,
            host: host.to_string(),
            port,
            vhost: if vhost.is_empty() { "/".into() } else { vhost },
            stream: stream_name(&stream)?,
        })
    }
}

/// The minimal percent-decoding an endpoint needs (`%2F` for the default vhost).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(n) = u8::from_str_radix(&String::from_utf8_lossy(&bytes[i + 1..i + 3]), 16) {
                out.push(n);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A stream (or super stream) name, as the broker names queues: 1 to 255 letters, digits,
/// `-`, `.`, `_` or `:`.
pub fn stream_name(s: &str) -> Result<String> {
    let ok = (1..=255).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_' | b':'));
    if !ok {
        bail!("the stream `{s}` is 1 to 255 letters, digits, `-`, `.`, `_` or `:`");
    }
    Ok(s.to_string())
}

/// The crate's TLS settings for an endpoint (`None`: plain TCP). Only `verify-full` with one
/// named CA file exists here: the crate trusts that file alone (no Mozilla roots), checks the
/// endpoint host itself, and offers no mode without a name check.
pub fn tls_config(spec: &TlsSpec, secure: bool) -> Result<Option<TlsConfiguration>> {
    if spec.server_name.is_some() {
        bail!(
            "`tls.server_name` is not supported by kind rabbitmq-stream: \
             the host name of the endpoint is checked"
        );
    }
    match spec.effective(secure) {
        Mode::Disabled => {
            if secure {
                bail!("a rabbitmq-stream+tls endpoint needs TLS, but `tls` says disabled");
            }
            Ok(None)
        }
        Mode::Required => bail!(
            "tls mode `required` is not supported by kind rabbitmq-stream: the certificate is \
             always verified; use verify-full with `tls.ca`"
        ),
        Mode::VerifyCa => bail!(
            "tls mode `verify-ca` is not supported by kind rabbitmq-stream: use verify-full \
             (the host name is checked too)"
        ),
        Mode::VerifyFull => match spec.ca.as_slice() {
            [ca] => Ok(Some(TlsConfiguration::Trusted {
                root_certificates_path: PathBuf::from(ca),
                client_certificates: None,
            })),
            [] => bail!(
                "kind rabbitmq-stream over TLS verifies against `tls.ca` alone (no system \
                 roots): name the CA file in `tls.ca`"
            ),
            many => bail!(
                "`tls.ca` lists {} files; kind rabbitmq-stream takes exactly one \
                 (the client trusts only that file)",
                many.len()
            ),
        },
        Mode::Auto => bail!("tls mode `auto` was not resolved"),
    }
}

/// The account of the SASL PLAIN handshake the protocol always makes: `vars.username` (a
/// reference or a literal) and `vars.password` (a reference, never a literal; it is a secret).
fn creds(vars: &Map<String, Value>) -> Result<(String, String)> {
    let username = var(vars, "username")
        .ok_or_else(|| anyhow!("kind rabbitmq-stream needs `vars.username`"))?
        .to_string();
    let password = var(vars, "password")
        .ok_or_else(|| anyhow!("kind rabbitmq-stream needs `vars.password`"))?;
    if !crate::secrets::is_reference(password) {
        bail!("`vars.password` must be a reference (`env:NAME` or `file:/path`), never a literal");
    }
    Ok((username, password.to_string()))
}

/// Load checks for a target of kind rabbitmq-stream (from `TargetDef::from_row`): its
/// endpoint, its TLS mode and its account.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    // One endpoint: one node and one stream (or super stream). A batch is answered per item,
    // which the engine sends to a single endpoint — as for kafka, which takes one too.
    if endpoints.len() != 1 {
        bail!(
            "kind rabbitmq-stream takes exactly one endpoint: \
             rabbitmq-stream://<host>[:port]/[<vhost>/]<stream>"
        );
    }
    let tls = TlsSpec::parse(tls).context("`tls`")?;
    for e in endpoints {
        let a = Addr::parse(e)?;
        tls_config(&tls, a.secure)?;
    }
    creds(vars)?;
    Ok(())
}

/// A `rabbitmq-stream://` or `rabbitmq-stream+tls://` endpoint belongs to kind
/// rabbitmq-stream only.
pub fn refuse_scheme(kind_is_stream: bool, endpoints: &[String]) -> Result<()> {
    if !kind_is_stream
        && endpoints.iter().any(|e| {
            let e = e.trim();
            e.starts_with("rabbitmq-stream://") || e.starts_with("rabbitmq-stream+tls://")
        })
    {
        bail!("a rabbitmq-stream:// or rabbitmq-stream+tls:// endpoint needs kind rabbitmq-stream");
    }
    Ok(())
}

/// The engine's reading of one record's publish answer.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// Sent: the record's stream (the partition stream of a super stream) and publishing id.
    Ok(String, u64),
    /// Refused for this record alone (`FrameTooLarge` is the only code that says so): rejected,
    /// so the engine strikes this key while its neighbours of the batch are done. A code about
    /// the account's right to the stream is not a verdict on the record at all and refuses the
    /// whole call instead — see `PublishErr::Denied`.
    Refused(String),
    /// Not known to have gone through: retry.
    Transient(String),
}

/// What one publish-error code says about the batch. Only a code that speaks positively about
/// this one record refuses it alone (too large to fit a frame). `AccessRefused` denies the
/// **whole call**: a super stream's partitions are separate streams with separate rights, so
/// treating it as this record's verdict would strike the keys of the partition the account may
/// not publish to while its neighbours go through, and five strikes would drop them — a
/// permissions mistake losing keys quietly. Everything else — the leader having moved
/// (`StreamNotAvailable`), the broker's own `InternalError`, a publisher or stream that went
/// stale (the next attempt reconnects and re-declares, and a stream that is truly gone refuses
/// at its declare then), and any code this client cannot judge — says nothing and is transient:
/// a retry cannot drop a key.
enum PublishErr {
    /// This record alone, by what the code says about it.
    Refused(String),
    /// The account may not publish to this stream: a verdict on the target, not on the record.
    Denied(String),
    /// Nothing is known about the record: retry.
    Transient(String),
}

fn code_outcome(code: ResponseCode) -> PublishErr {
    match code {
        ResponseCode::FrameTooLarge => {
            PublishErr::Refused("the record does not fit a frame (FrameTooLarge)".into())
        }
        ResponseCode::AccessRefused => PublishErr::Denied(
            "the account is not allowed to publish to this stream (AccessRefused)".into(),
        ),
        c => PublishErr::Transient(format!("the broker answered {c:?} for this publish")),
    }
}

/// The error a denied publish raises: a `Rejected` one, so the engine answers it as the whole
/// call's refusal (the shape `declare_of` and `connect_error` take) and strikes no key of the
/// batch. Out of `publish_on` so that the shape has a test of its own.
fn denied(stream: &str, why: &str) -> anyhow::Error {
    rejected(format!(
        "{stream}: {why}: the batch is refused by this, not by the records, so no key is struck"
    ))
}

/// What a waiting record learned from the connection.
enum Confirm {
    Confirmed,
    /// The broker's publish error with a code that refuses this record alone.
    Refused(String),
    /// The broker's publish error saying the account may not publish to this stream: it
    /// refuses the whole call, not this record (see the module docs).
    Denied(String),
    /// The broker's publish error with a "not now" code (retry, and reconnect).
    Failed(String),
    /// The connection is gone, or no answer came in time: nothing is known.
    Lost,
}

/// The confirms and publish errors still awaited, by publishing id. The connection's handler
/// fills it; every id is registered before its publish is sent, so no confirm can arrive for
/// an unregistered id. A `std` mutex, held only for map work and never across an await.
type Waiters = StdMutex<HashMap<u64, oneshot::Sender<Confirm>>>;

fn resolve(waiters: &Waiters, id: u64, c: Confirm) {
    if let Some(tx) = waiters.lock().unwrap().remove(&id) {
        let _ = tx.send(c);
    }
}

fn lose_all(waiters: &Waiters) {
    let mut w = waiters.lock().unwrap();
    for (_, tx) in w.drain() {
        let _ = tx.send(Confirm::Lost);
    }
}

/// The connection of one endpoint, with what was declared over it.
struct Inner {
    client: StreamClient,
    /// The confirms awaited on this connection (see `Waiters`).
    waiters: Arc<Waiters>,
    /// The publishing ids this connection hands out. They only have to tell the awaited
    /// confirms apart: dedup by publishing id is a property of *named* publishers, and this
    /// target names none, so the broker keeps no sequence for it.
    next_id: Arc<AtomicU64>,
    /// The declared publishers, by the stream they publish to (a partition stream of a super
    /// stream, or the endpoint's stream).
    publishers: HashMap<String, u8>,
    next_publisher: u8,
    /// The super stream's partitions as the broker's `partitions` answered (cached like
    /// kafka's topic partitions). `None`: not asked yet; `Some(empty)`: the endpoint names a
    /// plain stream.
    partitions: Option<Vec<String>>,
    /// Routing key to partition stream, as the hash of the key over the broker's partition list
    /// placed it (cached).
    routes: HashMap<String, String>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Ask the broker to close the connection (the crate's own close): its reader task
        // lives as long as the connection does, and a target reloaded must not leave the old
        // one open. Best effort.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let client = self.client.clone();
            handle.spawn(async move {
                let _ = client.close().await;
            });
        }
    }
}

/// One endpoint of a target.
pub struct Client {
    /// The endpoint as written (no credentials): the name in errors.
    pub url: String,
    addr: Addr,
    user: String,
    pass_ref: String,
    tls: Option<TlsConfiguration>,
    timeout: Duration,
    /// The connection (and its publishers, partitions and routes). Holding the lock also
    /// keeps one publish batch of the action in flight at a time (see the module docs).
    inner: Mutex<Option<Inner>>,
}

impl Client {
    /// The endpoint's context (`endpoint` in templates) and its client.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint)?;
        let tls = tls_config(&def.tls, addr.secure)?;
        let (user, pass_ref) = creds(&def.vars)?;
        let ctx = serde_json::json!({
            "host": addr.host,
            "port": addr.port,
            "vhost": addr.vhost,
            "stream": addr.stream,
            "url": endpoint.trim(),
        });
        Ok((
            ctx,
            Client {
                url: endpoint.trim().to_string(),
                addr,
                user,
                pass_ref,
                tls,
                timeout: def.timeout,
                inner: Mutex::new(None),
            },
        ))
    }

    /// Connects (peer properties, SASL PLAIN, tune, open); the secret references are resolved
    /// here, so a rotated secret file is picked up on the next connection.
    async fn open(&self) -> Result<Inner> {
        let user = crate::secrets::resolve(&self.user).context("vars.username")?;
        let pass = crate::secrets::resolve(&self.pass_ref).context("vars.password")?;
        let mut opts = ClientOptions::builder()
            .host(&self.addr.host)
            .port(self.addr.port)
            .user(&user)
            .password(&pass)
            .v_host(&self.addr.vhost)
            .client_provided_name("lazywf".into());
        if let Some(tls) = &self.tls {
            opts = opts.tls(tls.clone());
        }
        let client = tokio::time::timeout(self.timeout, StreamClient::connect(opts.build()))
            .await
            .map_err(|_| {
                anyhow!(
                    "connecting to {} timed out after {:?}",
                    self.url,
                    self.timeout
                )
            })?
            .map_err(|e| connect_error(&self.url, e))?;

        // The confirms of a publish arrive here, on the connection's handler. The crate's
        // blanket `MessageHandler` impl asks for a `Clone` closure that owns everything it
        // touches; it runs on its own task per message.
        let waiters: Arc<Waiters> = Arc::new(StdMutex::new(HashMap::new()));
        let w = waiters.clone();
        client
            .set_handler(move |item: Option<Result<Response, ClientError>>| {
                let w = w.clone();
                async move {
                    match item {
                        Some(Ok(resp)) => match resp.kind_ref() {
                            ResponseKind::PublishConfirm(c) => {
                                for id in &c.publishing_ids {
                                    resolve(&w, *id, Confirm::Confirmed);
                                }
                            }
                            ResponseKind::PublishError(e) => {
                                for pe in &e.publishing_errors {
                                    let c = match code_outcome(pe.error_code.clone()) {
                                        PublishErr::Refused(why) => Confirm::Refused(why),
                                        PublishErr::Denied(why) => Confirm::Denied(why),
                                        PublishErr::Transient(why) => Confirm::Failed(why),
                                    };
                                    resolve(&w, pe.publishing_id, c);
                                }
                            }
                            _ => {}
                        },
                        // A protocol error on the connection, or the connection ending:
                        // nothing awaited on it can be answered anymore.
                        _ => lose_all(&w),
                    }
                    Ok(())
                }
            })
            .await;

        Ok(Inner {
            client,
            waiters,
            next_id: Arc::new(AtomicU64::new(1)),
            publishers: HashMap::new(),
            next_publisher: 1,
            partitions: None,
            routes: HashMap::new(),
        })
    }

    /// Publishes the action's records (record i takes key i), grouped by the stream each
    /// record's routing key routes to: one publish per stream, their confirms awaited
    /// together. `Err`: the batch could not be attempted (the connection, a declare or a
    /// route that failed) — the records have no answer of their own.
    async fn publish(&self, keys: &[String], action: &Action) -> Result<Vec<Outcome>> {
        let mut slot = self.inner.lock().await;
        let r = self.publish_on(&mut slot, keys, action).await;
        // Any failure drops the connection, as kafka drops its client: the next action
        // reconnects and re-declares (a removed stream may be created meanwhile). So does any
        // transient record answer: the confirm did not come as it should have.
        match &r {
            Err(_) => *slot = None,
            Ok(out) if out.iter().any(|o| matches!(o, Outcome::Transient(_))) => *slot = None,
            _ => {}
        }
        r
    }

    async fn publish_on(
        &self,
        slot: &mut Option<Inner>,
        keys: &[String],
        action: &Action,
    ) -> Result<Vec<Outcome>> {
        if slot.as_ref().is_none_or(|i| i.client.is_closed()) {
            *slot = Some(self.open().await?);
        }
        let inner = slot.as_mut().expect("opened above");

        // Plain stream or super stream (the broker's `partitions` says; cached until the
        // connection drops).
        let partitions = self.partitions_of(inner).await?;

        // Group record i by the stream its routing key routes to.
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut out: Vec<Option<Outcome>> = vec![None; action.records.len()];
        for (i, rec) in action.records.iter().enumerate() {
            let stream = match &partitions {
                None => self.addr.stream.clone(),
                Some(parts) => {
                    // A super stream record routes by its `routing_key`; with none, the
                    // broker cannot place it and no retry changes that (the template renders
                    // the routing key): refused alone.
                    let Some(key) = rec.routing_key.as_deref().filter(|k| !k.is_empty()) else {
                        out[i] = Some(Outcome::Refused(
                            "a super stream record needs a `routing_key` to route by".into(),
                        ));
                        continue;
                    };
                    self.route_of(inner, key, parts)?
                }
            };
            groups.entry(stream).or_default().push(i);
        }

        // Declare the publishers the groups need and build their messages, registering every
        // publishing id's waiter before any frame of the batch goes out.
        let mut batches: Vec<Batch> = Vec::with_capacity(groups.len());
        for (stream, idxs) in groups {
            let pub_id = self.declare_of(inner, &stream).await?;
            let mut msgs = Vec::with_capacity(idxs.len());
            let mut waits = Vec::with_capacity(idxs.len());
            {
                let mut w = inner.waiters.lock().unwrap();
                for &i in &idxs {
                    let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = oneshot::channel();
                    w.insert(id, tx);
                    waits.push((i, id, rx));
                    let mut b = Message::builder()
                        .body(action.records[i].bytes.clone())
                        .publishing_id(id);
                    if let Some(k) = keys.get(i).filter(|k| !k.is_empty()) {
                        b = b
                            .application_properties()
                            .insert("lazywf_key", k.as_str())
                            .message_builder();
                    }
                    msgs.push(b.build());
                }
            }
            batches.push(Batch {
                stream,
                pub_id,
                msgs,
                waits,
            });
        }
        if batches.is_empty() {
            return Ok(out.into_iter().map(Option::unwrap).collect());
        }

        // The frames of the action go out together; a publish the connection itself refuses
        // says the broker took nothing of it: the whole action retries.
        for b in &batches {
            let n = b.msgs.len();
            inner
                .client
                .publish(b.pub_id, b.msgs.clone(), 1)
                .await
                .map_err(|e| {
                    anyhow!(
                        "publishing {} records to {stream}: {e}",
                        n,
                        stream = b.stream
                    )
                })?;
        }

        // Await every confirm within one time limit over the action: past it nothing that is
        // still without an answer is known to be in.
        let deadline = tokio::time::Instant::now() + self.timeout;
        for b in &mut batches {
            for (i, id, rx) in &mut b.waits {
                let c = match tokio::time::timeout_at(deadline, rx).await {
                    Ok(Ok(c)) => c,
                    // The waiter was dropped with the connection, or the wait ran out.
                    Ok(Err(_)) | Err(_) => Confirm::Lost,
                };
                out[*i] = Some(match c {
                    Confirm::Confirmed => Outcome::Ok(b.stream.clone(), *id),
                    Confirm::Refused(why) => Outcome::Refused(format!("{}: {why}", b.stream)),
                    Confirm::Failed(why) => Outcome::Transient(format!("{}: {why}", b.stream)),
                    Confirm::Lost => Outcome::Transient(format!(
                        "{}: no publish confirm within {:?} (the connection may be gone)",
                        b.stream, self.timeout
                    )),
                    // The account may not publish to this stream at all: a verdict on the
                    // target, not on this record. Nothing of this call can be answered on its
                    // own merits anymore, so the whole call refuses — as the declare and the
                    // connect refusals do — and no key of the batch is struck by it. `publish`
                    // drops the connection, so a right granted meanwhile is re-declared by the
                    // next attempt.
                    Confirm::Denied(why) => return Err(denied(&b.stream, &why)),
                });
            }
        }
        Ok(out.into_iter().map(Option::unwrap).collect())
    }

    /// The endpoint's super stream partitions (`None`: a plain stream), asked of the broker
    /// once per connection. A `partitions` answer that is not ok names a plain stream — the
    /// stream's existence itself is the declare's verdict (`declare_of`).
    async fn partitions_of(&self, inner: &mut Inner) -> Result<Option<Vec<String>>> {
        if let Some(p) = &inner.partitions {
            return Ok(if p.is_empty() { None } else { Some(p.clone()) });
        }
        let p = match inner.client.partitions(self.addr.stream.clone()).await {
            Ok(r) if r.is_ok() && !r.streams.is_empty() => r.streams.clone(),
            Ok(_) => Vec::new(),
            // The broker does not know it as a super stream: a plain stream (its existence
            // is decided at the declare); any other failure is the broker's, not an answer.
            Err(ClientError::RequestError(ResponseCode::StreamDoesNotExist)) => Vec::new(),
            Err(e) => bail!("asking the partitions of {}: {e}", self.addr.stream),
        };
        inner.partitions = Some(p.clone());
        Ok(if p.is_empty() { None } else { Some(p) })
    }

    /// The partition stream of a routing key (cached): **murmur3 over the broker's own partition
    /// list** — the rule `rabbitmq-stream-client`'s own super-stream producer uses
    /// (`superstream.rs`, seed 104729), so a key sent by this kind and the same key sent by that
    /// crate's `SuperStreamProducer` land on the same partition stream.
    ///
    /// The broker's `super_stream_route` command is deliberately not used: its answer is an
    /// ongoing publishing id plus publisher items (host, port, reference), and this client
    /// decodes it as a plain string list (`rabbitmq-stream-protocol` 0.11
    /// `commands/superstream_route.rs`), so it reads back empty for every key — which is also
    /// why the crate's own `RoutingKeyRoutingStrategy` publishes nowhere. The partition *names*
    /// still come from the broker (`super_stream_partitions`); only the modulo of the hash is
    /// done locally, as every official client does with the list it got.
    fn route_of(&self, inner: &mut Inner, key: &str, partitions: &[String]) -> Result<String> {
        if let Some(s) = inner.routes.get(key) {
            return Ok(s.clone());
        }
        let s = partition_of(partitions, key)
            .and_then(|i| partitions.get(i).cloned())
            .ok_or_else(|| anyhow!("`{key}` routes nowhere in {}", self.addr.stream))?;
        inner.routes.insert(key.to_string(), s.clone());
        Ok(s)
    }

    /// The publisher of a stream, declared once per connection. A stream the broker does not
    /// list refuses at link level (the target never creates one, so a publish to it would be
    /// refused again) — as kafka's unknown topic; anything else the declare cannot answer is
    /// transient.
    async fn declare_of(&self, inner: &mut Inner, stream: &str) -> Result<u8> {
        if let Some(id) = inner.publishers.get(stream) {
            return Ok(*id);
        }
        let id = inner.next_publisher;
        inner.next_publisher = inner.next_publisher.wrapping_add(1);
        let r = inner
            .client
            .declare_publisher(id, None, stream)
            .await
            .map_err(|e| anyhow!("declaring a publisher of {stream}: {e}"))?;
        if !r.is_ok() {
            let code = r.code().clone();
            if code == ResponseCode::StreamDoesNotExist {
                return Err(rejected(format!(
                    "{}: the stream `{stream}` is unknown to the broker \
                     (it must exist: the target does not create it)",
                    self.url
                )));
            }
            bail!("declaring a publisher of {stream}: the broker answered {code:?}");
        }
        inner.publishers.insert(stream.to_string(), id);
        Ok(id)
    }
}

/// The index in the broker's partition list a routing key belongs to: murmur3 with the seed the
/// client library hashes super stream keys with, modulo the count — the rule of that library's
/// own super-stream producer, so a key of this kind and the same key of that producer share a
/// partition. Out of `route_of` so that the rule has a test of its own. `None`: nothing to place
/// it in (the broker named no partitions).
fn partition_of(partitions: &[String], key: &str) -> Option<usize> {
    if partitions.is_empty() {
        return None;
    }
    let h = murmur3::murmur3_32(&mut std::io::Cursor::new(key), SUPER_STREAM_SEED).ok()?;
    Some((h % partitions.len() as u32) as usize)
}

/// One group of the action's records going to one stream: the publisher it uses, the messages
/// (kept until their frames are out) and each record's id with its awaited confirm.
struct Batch {
    stream: String,
    pub_id: u8,
    msgs: Vec<Message>,
    waits: Vec<(usize, u64, oneshot::Receiver<Confirm>)>,
}

/// A connect failure's verdict: a SASL or vhost failure is a refusal (the account is wrong
/// and reconnecting will not change that), everything else — TCP, TLS, a timeout, a broker
/// that is not answering — is transient.
fn connect_error(url: &str, e: ClientError) -> anyhow::Error {
    if matches!(
        e,
        ClientError::RequestError(
            ResponseCode::AuthenticationFailure
                | ResponseCode::AuthenticationFailureLoopback
                | ResponseCode::VirtualHostAccessFailure
                | ResponseCode::SaslMechanismNotSupported
                | ResponseCode::SaslError
        )
    ) {
        rejected(format!(
            "{url}: the broker refused the account (check vars.username and vars.password): {e}"
        ))
    } else {
        anyhow!("{url}: connecting failed: {e}")
    }
}

/// One record of a rendered action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rec {
    pub value: Value,
    pub routing_key: Option<String>,
    /// The value as the stream takes it (serialized once, when the body was parsed).
    bytes: Vec<u8>,
}

/// A rendered action: the `{"records": […]}` of a publish (record i is answered by key i).
pub struct Action {
    pub records: Vec<Rec>,
}

impl Action {
    /// Parses a rendered body. `Ok(None)`: an empty `records` list (nothing to send). The
    /// records' count must match `n_keys` (a `once` step has none). A batch over the frame
    /// limit is refused here, whole: the crate sends it as one frame the broker would drop,
    /// and no retry would take it (a template that renders 1 MiB of documents is the bug).
    pub fn parse(body: &[u8], n_keys: usize) -> Result<Option<Action>> {
        let v: Value = serde_json::from_slice(body).context("the publish body is not JSON")?;
        let records = v
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow!("the publish body is `{{\"records\": […]}}`"))?;
        let mut total = 0usize;
        let records = records
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let o = r
                    .as_object()
                    .ok_or_else(|| anyhow!("record {i} is not an object"))?;
                let value = o
                    .get("value")
                    .cloned()
                    .ok_or_else(|| anyhow!("record {i} has no `value`"))?;
                if value.is_null() {
                    bail!("record {i}: the `value` is null (render an empty object for a delete)");
                }
                let routing_key = match o.get("routing_key") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(k)) => Some(k.clone()),
                    Some(_) => bail!("record {i}: `routing_key` is not a string or null"),
                };
                for f in o.keys() {
                    if f != "value" && f != "routing_key" {
                        bail!(
                            "record {i} has the unknown field `{f}` \
                             (a record carries `value` and `routing_key` only)"
                        );
                    }
                }
                let bytes = serde_json::to_vec(&value)
                    .map_err(|e| anyhow!("record {i}: the `value` cannot be written ({e})"))?;
                total += bytes.len() + 64;
                Ok(Rec {
                    bytes,
                    value,
                    routing_key,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if records.len() != n_keys {
            bail!(
                "the publish lists {} records for {} keys (record i takes key i)",
                records.len(),
                n_keys
            );
        }
        if records.is_empty() {
            return Ok(None);
        }
        if total > MESSAGE_MAX_BYTES {
            return Err(anyhow!(
                "the publish of {} records is {total} bytes, over the {MESSAGE_MAX_BYTES} a \
                 stream frame takes (lower `batch_max_bytes`, or render smaller documents)",
                records.len()
            ));
        }
        Ok(Some(Action { records }))
    }
}

/// Sends one rendered `once` action (no keys) and checks `expect` on every record.
pub async fn send(client: &Client, rendered: &str, expect: &Expect, pool: &Pool) -> Result<Sent> {
    let Some(items) = items_of(client, rendered, &[]).await? else {
        return Ok(Sent::Skipped);
    };
    for (i, o) in items.iter().enumerate() {
        match o {
            Outcome::Ok(stream, id) => expect
                .row(pool, |u| Row::rabbitmq_stream(u, stream, *id))
                .await
                .with_context(|| format!("record {i} of {}", client.url))?
                .map_err(|e| rejected(format!("{} record {i}: {e}", client.url)))?,
            Outcome::Refused(why) => {
                return Err(rejected(format!(
                    "{} record {i}: the broker refused it: {why}",
                    client.url
                )))
            }
            Outcome::Transient(why) => bail!("{} record {i}: {why}", client.url),
        }
    }
    Ok(Sent::Ok)
}

/// Sends one rendered action (record i takes key i) and checks `expect` on every sent record.
/// `Ok(vec!)`: nothing rendered. `Err`: the whole call failed (the items have no answer).
pub async fn send_items(
    client: &Client,
    rendered: &str,
    keys: &[String],
    expect: &Expect,
    pool: &Pool,
) -> Result<Vec<Item>> {
    let Some(items) = items_of(client, rendered, keys).await? else {
        return Ok(vec![Item::Ok; keys.len()]);
    };
    let mut out = Vec::with_capacity(items.len());
    for (i, o) in items.iter().enumerate() {
        match o {
            Outcome::Ok(stream, id) => {
                let item = match expect
                    .row(pool, |u| Row::rabbitmq_stream(u, stream, *id))
                    .await
                    .with_context(|| format!("record {i} of {}", client.url))?
                {
                    Ok(()) => Item::Ok,
                    Err(why) => Item::Rejected(format!("record {i} of {}: {why}", client.url)),
                };
                out.push(item);
            }
            Outcome::Refused(why) => out.push(Item::Rejected(format!(
                "record {i} of {}: the broker refused it: {why}",
                client.url
            ))),
            Outcome::Transient(why) => out.push(Item::Transient(format!(
                "record {i} of {}: {why}",
                client.url
            ))),
        }
    }
    Ok(out)
}

/// The outcomes of one rendered action (record i takes key i). `Ok(None)`: nothing rendered.
/// `Err`: the whole call failed (a connection, stream or frame-size error, before any record
/// answer).
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
            "rabbitmq-stream targets only publish: {} {} is not POST /",
            req.method, req.target
        )));
    }
    let Some(action) = Action::parse(&req.body, keys.len())
        .map_err(|e| rejected(format!("the publish body: {e:#}")))?
    else {
        return Ok(None);
    };
    client.publish(keys, &action).await.map(Some)
}

/// Rule-load checks for a rabbitmq-stream rule: a batch over the target's limit (one stream
/// frame, 1 MiB) is refused at load.
pub fn rule_errors(def: &RuleDef, limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    if def.fire_per == FirePer::Batch && def.batch_max_bytes.is_some_and(|n| n > limit) {
        out.push(format!(
            "`batch_max_bytes` {} is over {limit}, the most one publish takes \
             (the stream frame limit)",
            def.batch_max_bytes.unwrap()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RawTarget;
    use serde_json::json;

    fn spec(s: &str) -> TlsSpec {
        TlsSpec::parse(Some(s)).unwrap()
    }

    fn vars(s: &str) -> Map<String, Value> {
        serde_json::from_str(s).unwrap()
    }

    fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
        RawTarget {
            name: "events".into(),
            kind: Some("rabbitmq-stream".into()),
            endpoints: Some(endpoints.into()),
            tls: tls.map(Into::into),
            timeout_ms: None,
            expect: None,
            vars: Some(vars.into()),
        }
    }

    const CREDS: &str = r#"{"username": "lwf", "password": "env:LWF_TEST_RMQSTREAM_PW"}"#;

    #[test]
    fn endpoints() {
        let a = Addr::parse("rabbitmq-stream://broker:5552/orders").unwrap();
        assert_eq!(
            (
                a.secure,
                a.host.as_str(),
                a.port,
                a.vhost.as_str(),
                a.stream.as_str()
            ),
            (false, "broker", 5552, "/", "orders")
        );
        let a = Addr::parse("rabbitmq-stream+tls://mq.internal/orders.v2").unwrap();
        assert_eq!(
            (a.secure, a.host.as_str(), a.port, a.stream.as_str()),
            (true, "mq.internal", 5551, "orders.v2")
        );
        // The vhost is the first path segment; `%2F` names the default one.
        let a = Addr::parse("rabbitmq-stream://127.0.0.1:34572/app/orders:eu").unwrap();
        assert_eq!((a.vhost.as_str(), a.stream.as_str()), ("app", "orders:eu"));
        let a = Addr::parse("rabbitmq-stream://127.0.0.1/%2F/orders").unwrap();
        assert_eq!((a.vhost.as_str(), a.stream.as_str()), ("/", "orders"));
    }

    #[test]
    fn endpoints_that_do_not_parse() {
        // The scheme of another kind, credentials in the URL, a missing stream, a bad name.
        for bad in [
            "rabbitmq://h/orders",
            "amqp://h/orders",
            "rabbitmq-stream://user:pw@h/orders",
            "rabbitmq-stream://h",
            "rabbitmq-stream://h//orders",
            "rabbitmq-stream://h/a/b/c",
            "rabbitmq-stream://h/ord ers",
            "rabbitmq-stream://h/orders?x=1",
            "rabbitmq-stream://:5552/orders",
            "rabbitmq-stream://h/[::1]",
        ] {
            assert!(Addr::parse(bad).is_err(), "accepted `{bad}`");
        }
        // An empty stream name and one with a forbidden character.
        assert!(stream_name("").is_err());
        assert!(stream_name("orders/1").is_err());
        assert!(stream_name("orders:eu-2_x").is_ok());
    }

    #[test]
    fn tls_modes() {
        // Only verify-full with exactly one named CA exists (the crate trusts that file
        // alone); everything that would skip the name check or the certificate is refused
        // by name, as amqp does.
        assert!(tls_config(&spec("disabled"), false).unwrap().is_none());
        for bad in [
            "disabled".to_string(),
            "required".to_string(),
            "verify-ca".to_string(),
            r#"{"mode": "verify-full"}"#.to_string(),
            r#"{"mode": "verify-full", "ca": ["/a.pem", "/b.pem"]}"#.to_string(),
            r#"{"mode": "verify-full", "server_name": "mq"}"#.to_string(),
        ] {
            let e = format!("{:#}", tls_config(&spec(&bad), true).err().unwrap());
            let wants = match bad.as_str() {
                "disabled" => "disabled",
                "required" => "required",
                "verify-ca" => "verify-ca",
                _ if bad.contains("server_name") => "server_name",
                _ if bad.contains('[') => "exactly one",
                _ => "tls.ca",
            };
            assert!(e.contains(wants), "{bad} => {e}");
        }
        let t = tls_config(&spec(r#"{"mode": "verify-full", "ca": "/ca.pem"}"#), true)
            .unwrap()
            .unwrap();
        assert!(matches!(
            t,
            TlsConfiguration::Trusted { ref root_certificates_path, client_certificates: None }
                if root_certificates_path == &PathBuf::from("/ca.pem")
        ));
    }

    #[test]
    fn credentials() {
        // A literal password is a load error; the username may be a literal.
        let e = format!(
            "{:#}",
            creds(&vars(r#"{"username": "lwf", "password": "hunter2"}"#))
                .err()
                .unwrap()
        );
        assert!(e.contains("reference"), "{e}");
        assert!(creds(&vars(CREDS)).is_ok());
        for missing in [
            r#"{"password": "env:X"}"#,
            r#"{"username": "u"}"#,
            r#"{"username": "", "password": "env:X"}"#,
        ] {
            assert!(creds(&vars(missing)).is_err(), "accepted {missing}");
        }
    }

    #[test]
    fn target_checks() {
        // One endpoint, its form, the TLS mode and the account: all decided at load.
        check(&["rabbitmq-stream://h/orders".into()], &vars(CREDS), None).unwrap();
        let e = format!(
            "{:#}",
            check(
                &[
                    "rabbitmq-stream://h/a".into(),
                    "rabbitmq-stream://h/b".into()
                ],
                &vars(CREDS),
                None
            )
            .err()
            .unwrap()
        );
        assert!(e.contains("exactly one endpoint"), "{e}");
        let e = format!(
            "{:#}",
            check(&["kafka://h/t".into()], &vars(CREDS), None)
                .err()
                .unwrap()
        );
        assert!(e.contains("rabbitmq-stream://"), "{e}");
        // The scheme belongs to this kind alone.
        refuse_scheme(false, &["rabbitmq-stream+tls://h/s".into()]).unwrap_err();
        refuse_scheme(true, &["rabbitmq-stream+tls://h/s".into()]).unwrap();
        refuse_scheme(false, &["amqp://h/q".into()]).unwrap();
        // The end-to-end load check of a row (from_row reaches these through check).
        crate::model::TargetDef::from_row(&raw("rabbitmq-stream://h/orders", None, CREDS)).unwrap();
        crate::model::TargetDef::from_row(&raw(
            "rabbitmq-stream://h/orders",
            None,
            r#"{"username": "u", "password": "literal"}"#,
        ))
        .unwrap_err();
    }

    #[test]
    fn the_rendered_form() {
        let body = |s: Value| serde_json::to_vec(&s).unwrap();
        // The shape of the module docs: value plus an optional routing key.
        let a = Action::parse(
            &body(json!({"records": [
                {"value": {"a": 1}, "routing_key": "k1"},
                {"value": 2, "routing_key": null},
                {"value": [1, 2]}
            ]})),
            3,
        )
        .unwrap()
        .unwrap();
        assert_eq!(a.records[0].value, json!({"a": 1}));
        assert_eq!(a.records[0].routing_key.as_deref(), Some("k1"));
        assert_eq!(a.records[0].bytes, br#"{"a":1}"#);
        assert_eq!(a.records[1].routing_key, None);
        assert_eq!(a.records[2].bytes, b"[1,2]");
        // Nothing rendered: the empty batch.
        assert!(Action::parse(&body(json!({"records": []})), 0)
            .unwrap()
            .is_none());
        // What is refused, and why.
        for (bad, n_keys) in [
            (json!({"records": [{"value": null}]}), 1), // a null value
            (json!({"records": [{"value": 1, "key": "x"}]}), 1), // an unknown record field
            (json!({"records": [{"routing_key": "k"}]}), 1), // no value
            (json!({"records": [{"value": 1, "routing_key": 7}]}), 1), // a numeric routing key
            (json!({"records": [{"value": 1}, {"value": 2}]}), 1), // records for one key
            (json!({"records": [{"value": 1}]}), 2),    // fewer records than keys
            (json!("not json at all"), 0),              // not a body
        ] {
            assert!(
                Action::parse(&body(bad.clone()), n_keys).is_err(),
                "accepted {bad}"
            );
        }
        // A batch over the frame limit refuses the whole action (deterministic: the broker
        // would never take this frame).
        let big = vec![Value::String("x".repeat(64 * 1024)); 20]; // 1.25 MB of values
        let e = format!(
            "{:#}",
            Action::parse(
                &body(
                    json!({"records": big.iter().map(|v| json!({"value": v})).collect::<Vec<_>>()})
                ),
                20
            )
            .err()
            .unwrap()
        );
        assert!(e.contains("frame"), "{e}");
    }

    /// The super stream partition rule: the same key always lands on the same partition, a key
    /// never lands outside the partitions the broker named, the partitions are all reached (so
    /// the hash is the hash, not a constant), and no partitions is no answer at all.
    #[test]
    fn super_stream_partition_of_a_routing_key() {
        let parts: Vec<String> = (0..3).map(|i| format!("orders-{i}")).collect();
        let mut hit = [false; 3];
        for n in 0..200 {
            let key = format!("order-{n}");
            let i = partition_of(&parts, &key).expect("a partition");
            assert_eq!(
                Some(i),
                partition_of(&parts, &key),
                "`{key}` must land in the same partition twice"
            );
            hit[i] = true;
        }
        assert!(hit.iter().all(|h| *h), "every partition is used: {hit:?}");
        assert!(
            partition_of(&parts, "").is_some(),
            "an empty routing key still has a partition"
        );
        assert_eq!(partition_of(&[], "any"), None, "no partitions, no answer");
    }

    #[test]
    fn publish_error_codes() {
        // The one code that speaks about this record alone refuses it; its neighbours of the
        // batch go through.
        assert!(matches!(
            code_outcome(ResponseCode::FrameTooLarge),
            PublishErr::Refused(_)
        ));
        // The code that says this account may not publish here denies the whole call instead:
        // a super stream's partitions have rights of their own, and a per-record verdict would
        // strike the keys of the forbidden partition until they were dropped.
        assert!(matches!(
            code_outcome(ResponseCode::AccessRefused),
            PublishErr::Denied(_)
        ));
        // …and everything else, including the codes that say "not now" or nothing, retries.
        for c in [
            ResponseCode::StreamNotAvailable,
            ResponseCode::InternalError,
            ResponseCode::PublisherDoesNotExist,
            ResponseCode::StreamDoesNotExist,
            ResponseCode::PreconditionFailed,
            ResponseCode::Ok, // a code that means nothing on a publish error
        ] {
            assert!(
                matches!(code_outcome(c.clone()), PublishErr::Transient(_)),
                "{c:?}"
            );
        }
        // A denial reaches the engine as the whole call's refusal (`Rejected`, the shape of the
        // declare and the connect ones), which is what keeps the keys from being struck.
        let e = denied("lwf.orders", "the account may not publish here");
        assert!(crate::target::is_rejected(&e), "{e:#} is not a refusal");
        assert!(
            format!("{e:#}").contains("lwf.orders") && format!("{e:#}").contains("no key"),
            "{e:#} does not say what it refuses"
        );
    }

    #[test]
    fn connect_failures_are_classified() {
        for c in [
            ResponseCode::AuthenticationFailure,
            ResponseCode::VirtualHostAccessFailure,
            ResponseCode::SaslError,
        ] {
            let e = connect_error(
                "rabbitmq-stream://h/s",
                ClientError::RequestError(c.clone()),
            );
            assert!(crate::target::is_rejected(&e), "{c:?} must refuse");
        }
        let e = connect_error("rabbitmq-stream://h/s", ClientError::AlreadyClosed);
        assert!(
            !crate::target::is_rejected(&e),
            "a closed connection must retry"
        );
    }

    #[test]
    fn a_batch_over_the_frame_limit_refuses_at_load() {
        // rule_errors: the engine would otherwise cut the batch to a size the frame refuses.
        let mut def = RuleDef::placeholder(&crate::model::RawRule {
            name: "r".into(),
            enabled: true,
            ..Default::default()
        });
        def.fire_per = FirePer::Batch;
        def.batch_max_bytes = Some(4_000_000);
        let errs = rule_errors(&def, MESSAGE_MAX_BYTES);
        assert!(
            errs.iter().any(|e| e.contains("batch_max_bytes")),
            "{errs:?}"
        );
        def.batch_max_bytes = Some(65_536);
        assert!(rule_errors(&def, MESSAGE_MAX_BYTES).is_empty());
        def.fire_per = FirePer::Key;
        def.batch_max_bytes = Some(4_000_000);
        assert!(
            rule_errors(&def, MESSAGE_MAX_BYTES).is_empty(),
            "a key fires no batch"
        );
    }
}
