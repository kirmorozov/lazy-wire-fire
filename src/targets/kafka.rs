//! The `kafka` kind: one produce per action to a broker of the Kafka protocol (Apache Kafka,
//! Redpanda, Amazon MSK and Azure Event Hubs all speak it). Client: rskafka (pure Rust, no
//! librdkafka; ring TLS; SASL PLAIN and SCRAM-SHA-256/512; gzip, lz4 and zstd compression —
//! see Cargo.toml).
//!
//! # Endpoint and credentials
//!
//! `kafka://<broker>[:port][,<broker>[:port]…]/<topic>`, or `kafkas://` for TLS (default ports
//! 9092 / 9093). One endpoint per target: one broker set and the topic it writes to (a batch is
//! answered per item, which the engine sends to a single endpoint, as for sqs, kinesis and
//! lambda). No credentials in the URL. Auth: when `vars.sasl` is one of `plain`,
//! `scram-sha-256` or `scram-sha-512`, `vars.username` (a reference or a literal, as amqp
//! takes it) and `vars.password` (a reference, never a literal) select the SASL mechanism and
//! the account; without `vars.sasl` the connection is anonymous.
//!
//! # The rendered action
//!
//! One action is one produce of a batch of records; the client groups the records by
//! partition, as
//!
//! ```text
//! POST / HTTP/1.1
//! Content-Type: application/json
//!
//! {"records": [{"value": …, "headers": {"op": "upsert"}}, {"value": …}]}
//! ```
//!
//! Record i takes the document key `keys[i]` as its record key (a key always lands on one
//! partition, in order: `partition_of` picks it the way the brokers' default partitioner does —
//! the murmur2 of the key, made positive with `& 0x7fffffff`, mod the partition count), its
//! rendered `value` (one JSON value, serialized compactly) and optional
//! `headers` (string to string). A record carries only `value` and `headers` — the key comes
//! from the row. With no keys (a `once` reset step) the records have no key and land on
//! partition 0.
//!
//! The batch is compressed per target (`vars.compression`: `none`, `gzip`, `lz4` or `zstd` —
//! zstd by default), and the largest one the target takes is the broker's `message.max.bytes`,
//! 1 MiB by default, raised by `vars.message_max_bytes`.
//!
//! # Outcomes
//!
//! The produce answer per partition maps to that partition's records:
//!
//! - a base offset: sent. `expect` then sees, per record, `body`
//!   `{"topic": …, "partition": …, "offset": …}` (no `expect`: met).
//! - retriable broker errors (leader not available, not leader for partition, request timed
//!   out, throttling, the network) and a whole-call connection failure: transient, together
//!   with a produce's `UnknownTopicOrPartition` (stale routing: the failed step clears the
//!   cached partitions, so the next attempt re-reads them) and any code the client cannot
//!   judge (`UnknownServerError`, an unrecognised code) — a retry cannot drop a key. The
//!   connection and its cached metadata are dropped; the next action reconnects.
//! - record too large, message too large, an unknown topic at the metadata check, an invalid
//!   record, an authorization or a SASL failure, an unsupported compression: refused. The
//!   broker refuses a whole record set over one record of it and names none, so a batch refused
//!   over a size or a format code is split in two and both halves produced again: the record the
//!   broker will not have is refused alone and its neighbours go through (see
//!   `Client::produce_group`).
//!
//! # Concurrency
//!
//! One produce at a time per endpoint: the endpoint's lock is held over all of an action's
//! partition produces, as for `amqp` (one batch of records per partition, in one produce each,
//! keeps the order of the action). Different targets and rules run in parallel.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::Pool;
use rskafka::chrono::TimeZone as _;
use rskafka::chrono::{DateTime, Utc};
use rskafka::client::error::Error as ClientError;
use rskafka::client::error::ProtocolError;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Credentials, SaslConfig};
use rskafka::record::Record;
use rskafka::{BackoffConfig, ConnectionError};
use rustls::ClientConfig;
use serde_json::{Map, Value};
use tokio::sync::Mutex;

use crate::model::{FirePer, RuleDef, TargetDef};
use crate::response::{Expect, Row};
use crate::target::{rejected, var, Item, Sent};
use crate::tls::{Mode, TlsSpec};

/// The broker's default `message.max.bytes`: 1 MiB.
pub const MESSAGE_MAX_BYTES: usize = 1_048_576;

/// The extra produces one action may spend on splitting a batch the broker refuses over a
/// single record (see `Client::produce_group`). Finding one record in a batch of a thousand
/// costs about twenty, so this only bites when nearly every record of an action is refused —
/// where the whole batch is refused instead of one produce per record.
const MAX_SPLITS: usize = 64;

/// The default ports (plain / TLS).
const DEFAULT_PORT: u16 = 9092;
const DEFAULT_SECURE_PORT: u16 = 9093;

/// An endpoint: `kafka(s)://<broker>[:port][,<broker>[:port]…]/<topic>`, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub secure: bool,
    pub brokers: Vec<(String, u16)>,
    pub topic: String,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("kafkas://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("kafka://") {
            (false, r)
        } else {
            bail!("endpoint `{shown}`: use kafka://broker[:port][,broker[:port]…]/topic or kafkas://…");
        };
        if rest.contains('@') {
            bail!("endpoint `{shown}`: credentials belong in vars.sasl / vars.username / vars.password, not in the URL");
        }
        if rest.contains(['?', '#']) {
            bail!("endpoint `{shown}`: no query string or fragment (TLS is the target's `tls`)");
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        let brokers = authority
            .split(',')
            .filter(|b| !b.is_empty())
            .map(|b| {
                if b.contains('[') {
                    bail!("endpoint `{shown}`: IPv6 addresses are not supported");
                }
                let default = if secure {
                    DEFAULT_SECURE_PORT
                } else {
                    DEFAULT_PORT
                };
                let (host, port) = match b.rsplit_once(':') {
                    Some((h, p)) => (
                        h,
                        p.parse::<u16>()
                            .with_context(|| format!("endpoint `{shown}`: port `{p}`"))?,
                    ),
                    None => (b, default),
                };
                if host.is_empty() || host.bytes().all(|c| matches!(c, b'-' | b'.')) {
                    bail!("endpoint `{shown}`: a broker has no host");
                }
                Ok((host.to_string(), port))
            })
            .collect::<Result<Vec<_>>>()?;
        if brokers.is_empty() {
            bail!("endpoint `{shown}`: no broker");
        }
        Ok(Addr {
            secure,
            brokers,
            topic: topic_name(path)?,
        })
    }

    /// The bootstrap list rskafka takes: `host:port` per broker.
    pub fn bootstrap(&self) -> Vec<String> {
        self.brokers
            .iter()
            .map(|(h, p)| format!("{h}:{p}"))
            .collect()
    }
}

/// A Kafka topic name: 1 to 249 letters, digits, `-` or `.` (not empty, not `-` or `..`).
pub fn topic_name(s: &str) -> Result<String> {
    let ok = (1..=249).contains(&s.len())
        && s != "-"
        && s != ".."
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.'));
    if !ok {
        bail!("the topic `{s}` is 1 to 249 letters, digits, `-` or `.`, and not `-` or `..`");
    }
    Ok(s.to_string())
}

/// The rskafka TLS settings for an endpoint (`None`: plain TCP). `kafkas://` is verified by
/// default; `tls.server_name` is refused (every broker's own name is checked).
pub fn tls_config(spec: &TlsSpec, secure: bool) -> Result<Option<Arc<ClientConfig>>> {
    if spec.server_name.is_some() {
        bail!(
            "`tls.server_name` is not supported by kind kafka: each broker's own name is checked"
        );
    }
    match spec.effective(secure) {
        Mode::Disabled => {
            if secure {
                bail!("a kafkas endpoint needs TLS, but `tls` says disabled");
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

/// `vars.sasl`: the SASL mechanism of the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sasl {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl Sasl {
    pub fn parse(s: &str) -> Result<Sasl> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "plain" => Sasl::Plain,
            "scram-sha-256" | "scram_sha_256" => Sasl::ScramSha256,
            "scram-sha-512" | "scram_sha_512" => Sasl::ScramSha512,
            other => bail!(
                "`vars.sasl` `{other}` (use plain, scram-sha-256 or scram-sha-512; leave it unset for no SASL)"
            ),
        })
    }
}

/// `vars.compression`: the compression of the produce batch (zstd by default).
pub fn compression(vars: &Map<String, Value>) -> Result<Compression> {
    match vars.get("compression").and_then(Value::as_str) {
        None => Ok(Compression::Zstd),
        Some(s) => Ok(match s.trim().to_ascii_lowercase().as_str() {
            "none" => Compression::NoCompression,
            "gzip" => Compression::Gzip,
            "lz4" => Compression::Lz4,
            "zstd" => Compression::Zstd,
            other => bail!("`vars.compression` `{other}` (use none, gzip, lz4 or zstd)"),
        }),
    }
}

/// The target's largest record batch: the broker's `message.max.bytes` (1 MiB), raised by
/// `vars.message_max_bytes` when the broker takes more.
pub fn message_max_bytes(vars: &Map<String, Value>) -> Result<usize> {
    match vars.get("message_max_bytes") {
        None => Ok(MESSAGE_MAX_BYTES),
        Some(v) => {
            let n = v
                .as_u64()
                .ok_or_else(|| anyhow!("`vars.message_max_bytes` is not a number"))?;
            if n == 0 || n > u32::MAX as u64 {
                bail!("`vars.message_max_bytes` is {n} (1 to 4294967295 bytes)");
            }
            Ok(n as usize)
        }
    }
}

/// The target's SASL settings, when `vars.sasl` selects one. The password is a reference, never
/// a literal (it is a secret); the username is a reference or a literal, as amqp takes it.
fn sasl_of(vars: &Map<String, Value>) -> Result<Option<(Sasl, String, String)>> {
    match var(vars, "sasl") {
        None => {
            if var(vars, "username").is_some() || var(vars, "password").is_some() {
                bail!("`vars.username` / `vars.password` need `vars.sasl` (plain, scram-sha-256 or scram-sha-512)");
            }
            Ok(None)
        }
        Some(m) => {
            let mech = Sasl::parse(m)?;
            let username = var(vars, "username")
                .ok_or_else(|| anyhow!("kind kafka with `vars.sasl` needs `vars.username`"))?
                .to_string();
            let password = var(vars, "password")
                .ok_or_else(|| anyhow!("kind kafka with `vars.sasl` needs `vars.password`"))?;
            if !crate::secrets::is_reference(password) {
                bail!("`vars.password` must be a reference (`env:NAME` or `file:/path`), never a literal");
            }
            Ok(Some((mech, username, password.to_string())))
        }
    }
}

/// Load checks for a target of kind kafka (from `TargetDef::from_row`): its endpoint, its TLS
/// mode, the SASL settings, the compression and the batch limit.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    // One endpoint: one broker set and one topic. A batch is answered per item, which the engine
    // sends to a single endpoint — as for sqs, kinesis and lambda, which take one endpoint too.
    if endpoints.len() != 1 {
        bail!(
            "kind kafka takes exactly one endpoint: \
             kafka://<broker>[:port][,<broker>[:port]…]/<topic-name>"
        );
    }
    let tls = TlsSpec::parse(tls).context("`tls`")?;
    for e in endpoints {
        let a = Addr::parse(e)?;
        tls_config(&tls, a.secure)?;
    }
    sasl_of(vars)?;
    compression(vars)?;
    message_max_bytes(vars)?;
    Ok(())
}

/// A `kafka://` or `kafkas://` endpoint belongs to kind kafka only.
pub fn refuse_scheme(kind_is_kafka: bool, endpoints: &[String]) -> Result<()> {
    if !kind_is_kafka
        && endpoints
            .iter()
            .any(|e| e.trim().starts_with("kafka://") || e.trim().starts_with("kafkas://"))
    {
        bail!("a kafka:// or kafkas:// endpoint needs kind kafka");
    }
    Ok(())
}

/// The murmur2 hash of the brokers' default partitioner: Kafka's `HashUtils.murmur2` (seed
/// `0x9747b28c`, the length mixed into it, `m = 0x5bd1e995`, `r = 24`). Not murmur3: the
/// brokers' Java clients, `rpk` and every other default partitioner use this one, so a key
/// hashed here lands where a key produced by any other client with the same name lands.
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let len = data.len() as u32;
    let mut h: u32 = 0x9747_b28c ^ len;
    for chunk in data.as_chunks::<4>().0 {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    // The last one to three bytes (Kafka's `switch` falls through to the multiply).
    let tail = &data[(data.len() & !3)..];
    if let Some(b) = tail.get(2) {
        h ^= u32::from(*b) << 16;
    }
    if let Some(b) = tail.get(1) {
        h ^= u32::from(*b) << 8;
    }
    if let Some(b) = tail.first() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// The partition of a record key: the murmur2 of it made positive the brokers' way —
/// `toPositive`, `& 0x7fffffff`, which is not the absolute value — mod the partition count
/// (the brokers' default partitioner). `partitions` is the topic's, sorted and not empty.
pub fn partition_of(key: &str, partitions: &[i32]) -> i32 {
    let h = (murmur2(key.as_bytes()) as u32) & 0x7fff_ffff;
    partitions[h as usize % partitions.len()]
}

/// One record of a rendered action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rec {
    pub value: Value,
    pub headers: BTreeMap<String, String>,
    /// The value as the broker takes it (serialized once, when the body was parsed).
    bytes: Vec<u8>,
}

/// A rendered action: the `{"records": […]}` of a produce (record i is answered by key i).
pub struct Action {
    pub records: Vec<Rec>,
}

impl Action {
    /// Parses a rendered body. `Ok(None)`: an empty `records` list (nothing to send). The
    /// records' count must match `n_keys` (a `once` step has none).
    pub fn parse(body: &[u8], n_keys: usize) -> Result<Option<Action>> {
        let v: Value = serde_json::from_slice(body).context("the produce body is not JSON")?;
        let records = v
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow!("the produce body is `{{\"records\": […]}}`"))?;
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
                let headers = match o.get("headers") {
                    None | Some(Value::Null) => BTreeMap::new(),
                    Some(Value::Object(h)) => h
                        .iter()
                        .map(|(k, v)| {
                            let v = v.as_str().ok_or_else(|| {
                                anyhow!("record {i}: the header `{k}` is not a string")
                            })?;
                            Ok((k.clone(), v.to_string()))
                        })
                        .collect::<Result<BTreeMap<String, String>>>()?,
                    Some(_) => bail!("record {i}: `headers` is not an object of strings"),
                };
                for f in o.keys() {
                    if f != "value" && f != "headers" {
                        bail!("record {i} has the unknown field `{f}` (a record carries `value` and `headers` only)");
                    }
                }
                Ok(Rec {
                    bytes: serde_json::to_vec(&value)
                        .map_err(|e| anyhow!("record {i}: the `value` cannot be written ({e})"))?,
                    value,
                    headers,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if records.len() != n_keys {
            bail!(
                "the produce lists {} records for {} keys (record i takes key i)",
                records.len(),
                n_keys
            );
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Action { records }))
        }
    }
}

/// The engine's reading of a produce answer of one partition, shared by all of its records.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// Sent: the record's offset.
    Ok(i64),
    /// Refused for these records (rejected: strikes).
    Refused(String),
    /// Not known to have gone through: retry.
    Transient(String),
}

/// Whether the broker's code for a failed partition says "not now" (transient) or "never for
/// this data" (refused). Transient: the leadership churn, timeouts, the network, throttling
/// and storage hiccups (including the replicas that are not in place yet, which the broker may
/// well take a moment later); `UnknownTopicOrPartition` on a produce, which Kafka classes as
/// retriable — stale routing (a partition moved, or a broker that does not host it yet),
/// while a topic that truly does not exist was already refused at the metadata check
/// (`Partitions::Missing`); and anything the client cannot judge: the broker's own
/// `UnknownServerError` and a code this client does not know say nothing about the records,
/// and the safe default is the one that cannot drop a key. Every other code refuses the
/// partition's records (record too large, invalid record, an authorization or a SASL failure,
/// an unsupported compression, …).
fn code_outcome(code: ProtocolError) -> Outcome {
    match code {
        ProtocolError::LeaderNotAvailable
        | ProtocolError::NotLeaderOrFollower
        | ProtocolError::RequestTimedOut
        | ProtocolError::BrokerNotAvailable
        | ProtocolError::ReplicaNotAvailable
        | ProtocolError::NotEnoughReplicas
        | ProtocolError::NotEnoughReplicasAfterAppend
        | ProtocolError::OperationNotAttempted
        | ProtocolError::NetworkException
        | ProtocolError::CorruptMessage
        | ProtocolError::KafkaStorageError
        | ProtocolError::LogDirNotFound
        | ProtocolError::StaleControllerEpoch
        | ProtocolError::StaleBrokerEpoch
        | ProtocolError::FencedLeaderEpoch
        | ProtocolError::UnknownLeaderEpoch
        | ProtocolError::PreferredLeaderNotAvailable
        | ProtocolError::EligibleLeadersNotAvailable
        | ProtocolError::OffsetNotAvailable
        | ProtocolError::ThrottlingQuotaExceeded
        // Stale routing, not a verdict on the records: the failed step clears the cached
        // partitions either way, so the next attempt re-reads them.
        | ProtocolError::UnknownTopicOrPartition
        // Not known to be a refusal: retry, with the code in the `last_error`.
        | ProtocolError::UnknownServerError
        | ProtocolError::Unknown(_) => Outcome::Transient(code.to_string()),
        _ => Outcome::Refused(code.to_string()),
    }
}

/// Whether a produce failure says *one record of this batch* is the problem rather than this
/// batch's topic or account: the size and format codes are the broker refusing a record set it
/// cannot take, and only splitting the set shows which record it will not have. Every other
/// refusal (an authorization or a SASL failure, an unsupported compression, an invalid
/// topic, …) is a property of the topic or the target, and all of the batch's records share it.
fn is_per_record_refusal(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::ServerError {
            protocol_error: ProtocolError::RecordListTooLarge
                | ProtocolError::MessageTooLarge
                | ProtocolError::InvalidRecord,
            ..
        }
    )
}

/// The broker does not list the topic at all (checked at the metadata read, before any
/// produce): the target never creates one, so a produce to it would be refused again — the
/// engine strikes the keys. This is where an unknown topic refuses; a produce answer's
/// `UnknownTopicOrPartition` is only stale routing and retries (`code_outcome`).
fn missing_refusal(url: &str, topic: &str) -> anyhow::Error {
    rejected(format!(
        "{url}: the topic `{topic}` is unknown to the broker (it must exist: the target does not create it)"
    ))
}

/// The outcome of a rskafka failure of one partition step (the leader read or the produce),
/// for every record of the partition it failed on. A produce answer's code maps through
/// `code_outcome`; a broken connection, a request that could not be answered and the retriable
/// errors that outlasted the backoff deadline are transient (not known to have gone through),
/// and a SASL failure is a refusal (the credentials are wrong).
fn client_error_outcome(e: &ClientError) -> Outcome {
    match e {
        ClientError::ServerError { protocol_error, .. } => code_outcome(*protocol_error),
        // The backoff deadline ran out on a retriable error (the connection or the leadership
        // kept failing): the broker is unwell, not the data.
        ClientError::RetryFailed(_) => {
            Outcome::Transient("the retriable broker errors outlasted the deadline".into())
        }
        ClientError::Connection(c) => match c {
            ConnectionError::SaslFailed(_) => Outcome::Refused(
                "SASL authentication failed: check vars.sasl, vars.username and vars.password"
                    .into(),
            ),
            _ => Outcome::Transient(format!("connecting to the broker failed: {c:#}")),
        },
        // A malformed answer, a broken stream or the time limit: not known to have gone through.
        _ => Outcome::Transient("the broker's answer is not known to be complete".into()),
    }
}

/// The metadata of the topic the records would go to.
enum Partitions {
    /// The topic's partitions, sorted (0..n).
    Got(Vec<i32>),
    /// The broker does not list this topic (the target never creates one, so a produce to it
    /// would be refused again: the engine strikes the keys).
    Missing,
}

/// The rskafka client of one endpoint, with its cached topic partitions.
struct Inner {
    client: rskafka::client::Client,
    partitions: Option<Vec<i32>>,
}

impl Inner {
    /// The topic's partitions, sorted (cached; any failed produce step clears them, so a topic
    /// created or given more partitions meanwhile is seen again). `Err`: the metadata request
    /// itself failed.
    async fn partitions(&mut self, topic: &str) -> Result<Partitions> {
        if let Some(p) = &self.partitions {
            return Ok(Partitions::Got(p.clone()));
        }
        let topics = self.client.list_topics().await.context("the topic list")?;
        let Some(t) = topics.iter().find(|t| t.name == topic) else {
            return Ok(Partitions::Missing);
        };
        // The partition ids, sorted: the record's partition is chosen by position, so the order
        // the broker lists them in must not decide where a key goes.
        let mut ps: Vec<i32> = t.partitions.iter().copied().collect();
        ps.sort_unstable();
        if ps.is_empty() {
            return Ok(Partitions::Missing);
        }
        self.partitions = Some(ps.clone());
        Ok(Partitions::Got(ps))
    }

    fn forget_partitions(&mut self) {
        self.partitions = None;
    }
}

/// One endpoint of a target.
pub struct Client {
    /// The endpoint as written (no credentials): the name in errors.
    pub url: String,
    addr: Addr,
    sasl: Option<(Sasl, String, String)>,
    tls: Option<Arc<ClientConfig>>,
    compression: Compression,
    timeout: Duration,
    /// The connection (and its cached topic partitions). Holding the lock also keeps one
    /// produce of the action in flight at a time (see the module docs).
    inner: Mutex<Option<Inner>>,
}

impl Client {
    /// The endpoint's context (`endpoint` in templates) and its client.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint)?;
        let tls = tls_config(&def.tls, addr.secure)?;
        let sasl = sasl_of(&def.vars)?;
        let compression = compression(&def.vars)?;
        let (host, port) = &addr.brokers[0];
        let ctx = serde_json::json!({
            "host": host,
            "port": port,
            "topic": addr.topic,
            "brokers": addr.brokers.iter().map(|(h, p)| serde_json::json!({"host": h, "port": p})).collect::<Vec<_>>(),
            "url": endpoint.trim(),
        });
        Ok((
            ctx,
            Client {
                url: endpoint.trim().to_string(),
                addr,
                sasl,
                tls,
                compression,
                timeout: def.timeout,
                inner: Mutex::new(None),
            },
        ))
    }

    /// Connects (and reads the cluster metadata); the SASL references are resolved now, so a
    /// rotated secret file is picked up on the next connection.
    async fn open(&self) -> Result<Inner> {
        let mut builder = rskafka::client::ClientBuilder::new(self.addr.bootstrap())
            .client_id("lazywf")
            .backoff_config(BackoffConfig {
                max_backoff: Duration::from_secs(2),
                deadline: Some(self.timeout),
                ..Default::default()
            });
        if let Some(tls) = &self.tls {
            builder = builder.tls_config(tls.clone());
        }
        if let Some((mech, user, pass)) = &self.sasl {
            let user = crate::secrets::resolve(user).context("vars.username")?;
            let pass = crate::secrets::resolve(pass).context("vars.password")?;
            let creds = Credentials::new(user, pass);
            let sasl = match mech {
                Sasl::Plain => SaslConfig::Plain(creds),
                Sasl::ScramSha256 => SaslConfig::ScramSha256(creds),
                Sasl::ScramSha512 => SaslConfig::ScramSha512(creds),
            };
            builder = builder.sasl_config(sasl);
        }
        let client = tokio::time::timeout(self.timeout, builder.build())
            .await
            .map_err(|_| {
                anyhow!(
                    "connecting to {} timed out after {:?}",
                    self.url,
                    self.timeout
                )
            })?
            .with_context(|| format!("connecting to {}", self.url))?;
        Ok(Inner {
            client,
            partitions: None,
        })
    }

    /// Produces the action's records (record i takes key i), grouped by partition: one produce
    /// per partition, split further when the broker refuses one over a single record (see
    /// `produce_group`). For every record: its partition and the answer of its produce.
    /// `Err`: the produce could not be attempted (the connection or the topic).
    async fn produce(&self, keys: &[String], action: &Action) -> Result<Vec<(i32, Outcome)>> {
        let mut slot = self.inner.lock().await;
        let r = self.produce_on(&mut slot, keys, action).await;
        // Any failure drops the connection, as amqp drops its session: the next action
        // reconnects and re-reads the metadata (a refused topic may be created meanwhile).
        if r.is_err() {
            *slot = None;
        }
        r
    }

    async fn produce_on(
        &self,
        slot: &mut Option<Inner>,
        keys: &[String],
        action: &Action,
    ) -> Result<Vec<(i32, Outcome)>> {
        if slot.is_none() {
            *slot = Some(self.open().await?);
        }
        let inner = slot.as_mut().expect("opened above");
        let partitions = match inner.partitions(&self.addr.topic).await? {
            Partitions::Got(p) => p,
            Partitions::Missing => return Err(missing_refusal(&self.url, &self.addr.topic)),
        };
        // Group record i by the partition of key i. A record without a key (a `once` step) has
        // no hash to go by and takes the first partition.
        let mut groups: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for i in 0..action.records.len() {
            let p = match keys.get(i) {
                Some(k) if !k.is_empty() => partition_of(k, &partitions),
                _ => partitions[0],
            };
            groups.entry(p).or_default().push(i);
        }
        // The records' create time: now (rskafka's chrono has no clock feature of its own).
        let job = Job {
            keys,
            action,
            now: rskafka::chrono::Utc
                .timestamp_millis_opt(crate::time::unix_now_ms() as i64)
                .single()
                .expect("millis are always a valid Utc time"),
        };
        let mut out: Vec<Option<(i32, Outcome)>> = vec![None; action.records.len()];
        // The extra produces the whole action may spend on splitting a refused batch.
        let mut splits = MAX_SPLITS;
        for (pid, idxs) in groups {
            let topic = self.addr.topic.clone();
            // One step per partition: the leader read, then the produce (or the produces of a
            // split). The time limit covers the whole step: past it nothing is known to be in.
            let res = tokio::time::timeout(self.timeout, async {
                let pc = inner
                    .client
                    .partition_client(&topic, pid, UnknownTopicHandling::Error)
                    .await
                    .map_err(StepErr::Client)?;
                self.produce_group(&pc, pid, &job, &idxs, &mut out, &mut splits)
                    .await;
                Ok(())
            })
            .await
            .map_err(|_| StepErr::Timeout)
            .and_then(|r| r);
            if let Err(e) = res {
                let o = match e {
                    StepErr::Timeout => {
                        Outcome::Transient(format!("no produce answer within {:?}", self.timeout))
                    }
                    StepErr::Client(e) => client_error_outcome(&e),
                };
                // A failed partition step drops the cached partition list: the topic may have
                // been created or given more partitions meanwhile, and the next action should
                // ask the broker again (as amqp drops its session).
                inner.forget_partitions();
                // The records a split had already answered keep their answer.
                for &i in &idxs {
                    if out[i].is_none() {
                        out[i] = Some((pid, o.clone()));
                    }
                }
            }
        }
        Ok(out
            .into_iter()
            .map(|o| o.expect("every record was answered"))
            .collect())
    }

    /// Produces one group of the action's records on its partition and answers every one of them
    /// in `out` — the group always leaves with a verdict per record, unless the caller's time
    /// limit cut it short (which the caller answers itself).
    ///
    /// A produce the broker refuses over *one record of the batch* (`is_per_record_refusal`) says
    /// nothing about the others: the broker took none of them, and its answer names no record. So
    /// the group is split in two and both halves produced: the record the broker will not have is
    /// refused by itself while its neighbours go through. The engine's own bisection cannot do
    /// this here — it re-sends a whole action, and the records of the other partitions have gone
    /// through by now. `splits` bounds the extra produces, so a batch refused whole (an
    /// authorization failure, say) costs one produce and not one per record.
    async fn produce_group(
        &self,
        pc: &PartitionClient,
        pid: i32,
        job: &Job<'_>,
        idxs: &[usize],
        out: &mut [Option<(i32, Outcome)>],
        splits: &mut usize,
    ) {
        let recs: Vec<Record> = idxs
            .iter()
            .map(|&i| Record {
                key: job
                    .keys
                    .get(i)
                    .filter(|k| !k.is_empty())
                    .map(|k| k.as_bytes().to_vec()),
                value: Some(job.action.records[i].bytes.clone()),
                headers: job.action.records[i]
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                    .collect(),
                timestamp: job.now,
            })
            .collect();
        match pc.produce(recs, self.compression).await {
            Ok(offsets) => {
                // The broker's answer covers the batch in order: base offset, plus the place in
                // it. (An answer that comes up short leaves its records "not known to be in".)
                for (j, &i) in idxs.iter().enumerate() {
                    out[i] = Some((
                        pid,
                        offsets.get(j).copied().map(Outcome::Ok).unwrap_or_else(|| {
                            Outcome::Transient(
                                "the produce answer has no offset for this record".into(),
                            )
                        }),
                    ));
                }
            }
            Err(e) => {
                if idxs.len() > 1 && *splits > 0 && is_per_record_refusal(&e) {
                    *splits -= 1;
                    let mid = idxs.len() / 2;
                    for half in [&idxs[..mid], &idxs[mid..]] {
                        // A recursive `async fn` needs indirection at the recursive call.
                        Box::pin(self.produce_group(pc, pid, job, half, out, splits)).await;
                    }
                }
                // Whatever the group, or a half of it, is still without a verdict goes with
                // this failure.
                let o = client_error_outcome(&e);
                for &i in idxs {
                    if out[i].is_none() {
                        out[i] = Some((pid, o.clone()));
                    }
                }
            }
        }
    }
}

/// The action a partition's groups are produced from: its keys, its records and the records'
/// create time.
struct Job<'a> {
    keys: &'a [String],
    action: &'a Action,
    now: DateTime<Utc>,
}

/// What a partition step (the leader read or the produce) failed with.
enum StepErr {
    /// A rskafka error (classified by `client_error_outcome`).
    Client(ClientError),
    /// The whole step ran past the target's time limit (no answer at all).
    Timeout,
}

/// Sends one rendered `once` action (no keys) and checks `expect` on every record.
pub async fn send(client: &Client, rendered: &str, expect: &Expect, pool: &Pool) -> Result<Sent> {
    let Some(items) = items_of(client, rendered, &[]).await? else {
        return Ok(Sent::Skipped);
    };
    for (i, (pid, o)) in items.iter().enumerate() {
        match o {
            Outcome::Ok(offset) => expect
                .row(pool, |u| Row::kafka(u, &client.addr.topic, *pid, *offset))
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
    let items = items_of(client, rendered, keys).await?;
    let Some(items) = items else {
        return Ok(vec![Item::Ok; keys.len()]);
    };
    let mut out = Vec::with_capacity(items.len());
    for (i, (pid, o)) in items.iter().enumerate() {
        match o {
            Outcome::Ok(offset) => {
                let item = match expect
                    .row(pool, |u| Row::kafka(u, &client.addr.topic, *pid, *offset))
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
/// `Err`: the whole call failed (a connection or a topic error, before any partition answer).
async fn items_of(
    client: &Client,
    rendered: &str,
    keys: &[String],
) -> Result<Option<Vec<(i32, Outcome)>>> {
    let req = match crate::http::RawRequest::parse(rendered) {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(None),
        Err(e) => return Err(rejected(format!("the rendered request: {e:#}"))),
    };
    if req.method != "POST" || req.target != "/" {
        return Err(rejected(format!(
            "kafka targets only produce: {} {} is not POST /",
            req.method, req.target
        )));
    }
    let Some(action) = Action::parse(&req.body, keys.len())
        .map_err(|e| rejected(format!("the produce body: {e:#}")))?
    else {
        return Ok(None);
    };
    client.produce(keys, &action).await.map(Some)
}

/// Rule-load checks for a kafka rule: a batch over the target's limit (the broker's
/// `message.max.bytes`, raised by `vars.message_max_bytes`) is refused at load.
pub fn rule_errors(def: &RuleDef, limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    if def.fire_per == FirePer::Batch && def.batch_max_bytes.is_some_and(|n| n > limit) {
        out.push(format!(
            "`batch_max_bytes` {} is over {limit}, the most a produce takes (the broker's message limit, raised by the target's `vars.message_max_bytes`)",
            def.batch_max_bytes.unwrap()
        ));
    }
    out
}

#[cfg(test)]
#[path = "../tests/targets_kafka.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/targets_kafka_action_body_tests.rs"]
mod action_body_tests;
