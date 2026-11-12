//! A target at runtime: its endpoints, how a rendered action reaches each of them, and whether
//! the answer meets `expect` (a SQL condition the source database evaluates).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use mysql_async::Pool;
use serde_json::{json, Map, Value};

use crate::allow::HostList;
use crate::aws;
use crate::http::{BaseUrl, Client as HttpClient, RawRequest, Response, SecretUrl};
use crate::model::{Kind, TargetDef};
use crate::redis::{self, Client as RedisClient, RedisAddr, Reply};
use crate::response::{Expect, Row, Uses};
use crate::s3::S3Spec;
use crate::secrets;
use crate::tls::TlsSpec;

const USER_AGENT: &str = concat!("lazywf/", env!("CARGO_PKG_VERSION"));

pub struct Endpoint {
    /// `endpoint` in the template context.
    pub ctx: Value,
    transport: Transport,
}

enum Transport {
    Http {
        client: Arc<HttpClient>,
        /// Prepended to the rendered path (base URL path, or `/<bucket>` for path-style S3).
        prefix: String,
        host_header: String,
    },
    /// An http endpoint whose URL is a secret: `env:NAME` or `file:/path` (see
    /// [`crate::secrets`]), whose value is a full http(s) URL. It is resolved on every request,
    /// like the other credentials, so a rotated file applies without a reload.
    HttpSecret {
        /// The reference itself. The value it names is never kept: it is resolved, used for one
        /// request and dropped.
        reference: String,
        /// One keep-alive client per origin the reference has named (`scheme://host:port`). The
        /// key is the origin, never the URL: a pool entry cannot carry the secret.
        clients: Mutex<HashMap<String, Arc<HttpClient>>>,
    },
    Redis(RedisClient),
    /// One RabbitMQ endpoint (see `amqp.rs`).
    Amqp(Box<crate::amqp::Client>),
    /// One SMTP endpoint (see `smtp.rs`).
    Smtp(Box<crate::smtp::Client>),
    MySql(crate::sqlwire::MySqlClient),
    Pg(crate::sqlwire::PgClient),
    /// Google Cloud Pub/Sub (`src/pubsub.rs`): one publish per action.
    PubSub(Box<crate::pubsub::Client>),
    /// One Kafka endpoint (`src/kafka.rs`): one produce of a batch of records per action.
    Kafka(Box<crate::kafka::Client>),
    /// One RabbitMQ Stream endpoint (`src/rabbitmq_stream.rs`): one publish batch of records
    /// per action, on the stream protocol.
    RabbitMqStream(Box<crate::rabbitmq_stream::Client>),
    /// One gRPC endpoint (`src/grpc/`): the rule's documents as protobuf messages on one
    /// long-lived bidirectional stream, acknowledged item by item.
    Grpc(Box<crate::grpc::Client>),
    /// One Azure Service Bus endpoint (`src/servicebus.rs`): one batch of messages per
    /// action to a queue or topic, a disposition per message.
    ServiceBus(Box<crate::servicebus::Client>),
}

pub struct Target {
    pub def: TargetDef,
    /// `target` in the template context: `name` and `vars` without credentials.
    pub ctx: Value,
    pub endpoints: Vec<Endpoint>,
    pub expect: Expect,
    /// SigV4 signing for the AWS kinds (`http.s3`, `sqs`, `kinesis`, `lambda`, `eventbridge`,
    /// `sns`).
    aws: Option<AwsSign>,
    /// Webhook signing for the http kinds (`src/sign.rs`); the secret is resolved on every use.
    sign: Option<crate::sign::Spec>,
    /// The `aws-lambda://` endpoint of a `lambda` target (its function, qualifier, invocation).
    lambda: Option<aws::LambdaUrl>,
    /// The effective batch limit of a `kafka` target, when `vars.message_max_bytes` raises the
    /// kind's default (the broker's `message.max.bytes`).
    kafka: Option<usize>,
    /// The effective batch limit of a `grpc` target, when `vars.max_message_bytes` raises the
    /// kind's default (the server's own maximum message size).
    grpc: Option<usize>,
    /// The effective batch limit of a `servicebus` target, when `vars.max_message_size`
    /// raises the kind's default (the entity's maximum message size).
    servicebus: Option<usize>,
    /// The `aws-sns://` endpoint of an `sns` target and its batch byte limit.
    sns: Option<Sns>,
    /// `LAZYWF_ALLOWED_HOSTS`, kept for the endpoints that are resolved per request: a rotated
    /// secret file must not be able to move a target to a host the operator did not allow.
    allowed: Option<HostList>,
    /// The source database, which evaluates `expect`.
    pool: Pool,
}

/// How an AWS kind signs its requests.
struct AwsSign {
    service: &'static str,
    region: String,
    provider: aws::Provider,
}

/// What an `sns` target knows about its topic beyond the endpoint: whether it is FIFO (every
/// entry then needs a `MessageGroupId`) and its `MaximumMessageSize`, which is the batch's byte
/// limit (`vars.maximum_message_size` states it; see [`crate::sns::maximum_message_size`]).
struct Sns {
    url: aws::SnsUrl,
    max_bytes: usize,
}

/// The outcome of one item of a batch request (see [`Target::send_items`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Ok,
    /// Refused on its own (`expect` not met, or the service rejected the entry).
    Rejected(String),
    /// Not sent for a reason that may pass (the service was unwell).
    Transient(String),
}

/// What one `Records[i]` of a `PutRecords` answer says, before `expect` (see
/// [`kinesis_verdict`]). Kinesis answers by position: the i-th record of the answer is the i-th
/// record of the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordVerdict {
    /// No `ErrorCode`: the record was stored. `expect` decides, with the record as `body`.
    Stored(Vec<u8>),
    /// `ProvisionedThroughputExceededException`, `InternalFailure` or `ServiceUnavailable`: try
    /// again later.
    Transient(String),
    /// Any other `ErrorCode`: refused for this record.
    Rejected(String),
    /// The answer has no entry for this record: it is not known to be stored, so it waits.
    Missing,
}

/// The per-record mapping of a Kinesis `PutRecords` answer. The transient codes are
/// `ProvisionedThroughputExceededException`, `InternalFailure` and `ServiceUnavailable` (the
/// service is unwell: keep retrying, as for a whole-request 503); every other code is a refusal
/// of that record.
pub fn kinesis_verdict(entry: Option<&Value>, i: usize) -> RecordVerdict {
    let Some(e) = entry else {
        return RecordVerdict::Missing;
    };
    match e
        .get("ErrorCode")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
    {
        None => RecordVerdict::Stored(serde_json::to_vec(e).unwrap_or_default()),
        Some(code) => {
            let msg = format!(
                "record {i}: {code}: {}",
                e.get("ErrorMessage").and_then(Value::as_str).unwrap_or("")
            );
            match code {
                "ProvisionedThroughputExceededException"
                | "InternalFailure"
                | "ServiceUnavailable" => RecordVerdict::Transient(msg),
                _ => RecordVerdict::Rejected(msg),
            }
        }
    }
}

/// What one `Entries[i]` of a `PutEvents` answer says, before `expect` (see
/// [`eventbridge_verdict`]). EventBridge answers by position: the i-th entry of the answer is
/// the i-th entry of the request.
pub fn eventbridge_verdict(entry: Option<&Value>, i: usize) -> RecordVerdict {
    let Some(e) = entry else {
        return RecordVerdict::Missing;
    };
    // An `EventId` says the event was stored (PutEvents, PutEventsResponseEntry.EventId).
    if e.get("EventId")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty())
    {
        return RecordVerdict::Stored(serde_json::to_vec(e).unwrap_or_default());
    }
    let text = |k: &str| e.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    match e
        .get("ErrorCode")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
    {
        // The AWS "not now" codes (aws::transient_code: throttling, the service is unwell) keep
        // the key waiting, as for a whole-request 503; any other code is the service's clear
        // refusal of this one entry (PutEvents, PutEventsResultEntry.ErrorCode).
        Some(code) if crate::aws::transient_code(code) => {
            RecordVerdict::Transient(format!("entry {i}: {code}: {}", text("ErrorMessage")))
        }
        Some(code) => {
            RecordVerdict::Rejected(format!("entry {i}: {code}: {}", text("ErrorMessage")))
        }
        // Neither an `EventId` nor an `ErrorCode`: nothing is known about this entry.
        None => RecordVerdict::Missing,
    }
}

/// The rendered body of a `PutEvents` action: `{"Entries": [...]}` with exactly one entry per
/// key (`Entries[i]` is answered by position), each holding the members AWS requires on every
/// entry as non-empty strings (PutEvents, PutEventsRequestEntry: `Source`, `DetailType`,
/// `Detail`). Checked before the request goes out: a template that renders a broken batch
/// fails once with one clear error, not as `n` per-entry refusals on every retry.
fn eventbridge_entries_ok(body: &[u8], n: usize) -> Result<(), String> {
    let request: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return Err(format!("the body is not JSON: {e}")),
    };
    let Some(entries) = request.get("Entries").and_then(Value::as_array) else {
        return Err("the body has no `Entries` array".to_string());
    };
    if entries.len() != n {
        return Err(format!(
            "`Entries` holds {} entries for {n} keys",
            entries.len()
        ));
    }
    for (i, e) in entries.iter().enumerate() {
        for field in ["Source", "DetailType", "Detail"] {
            if e.get(field)
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err(format!(
                    "entry {i}: `{field}` is not a non-empty string (PutEvents requires it on every entry)"
                ));
            }
        }
    }
    Ok(())
}

/// The items of a Lambda answer: the keys the function listed in `batchItemFailures` retry
/// (transient); every other key is met. An identifier that is not a key of the batch is a wrong
/// answer, so it is an error and nothing is trusted.
pub fn lambda_items(failed: &[String], keys: &[String]) -> Result<Vec<Item>> {
    let mut out = vec![Item::Ok; keys.len()];
    for id in failed {
        let i = keys.iter().position(|k| k == id).ok_or_else(|| {
            anyhow!("batchItemFailures lists `{id}`, which is not a key of the batch")
        })?;
        out[i] = Item::Transient(format!("key `{id}`: listed in batchItemFailures"));
    }
    Ok(out)
}

/// The AWS credentials of a target's `vars`, when it gives any.
fn aws_refs(def: &TargetDef) -> Option<aws::Refs> {
    match (
        def_var(def, "access_key_ref"),
        def_var(def, "secret_key_ref"),
    ) {
        (Some(a), Some(s)) => Some(aws::Refs {
            access_key: a.to_string(),
            secret_key: s.to_string(),
            session_token: def_var(def, "session_token_ref").map(str::to_string),
        }),
        _ => None,
    }
}

/// The target refused an action; sending it again would not help.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Rejected(pub String);

pub(crate) fn rejected(msg: String) -> anyhow::Error {
    anyhow::Error::new(Rejected(msg))
}

pub fn is_rejected(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<Rejected>().is_some())
}

/// What sending one rendered action did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent {
    /// The template rendered nothing to send.
    Skipped,
    Ok,
}

/// The string a target's `vars` holds under `k`: empty is unset. The kind modules read their
/// own `vars` through this one.
pub(crate) fn var<'a>(vars: &'a Map<String, Value>, k: &str) -> Option<&'a str> {
    vars.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn def_var<'a>(def: &'a TargetDef, k: &str) -> Option<&'a str> {
    var(&def.vars, k)
}

/// One http request with its endpoint resolved: the client that owns the connection, the
/// request the template rendered, and the two ways a message may name it.
///
/// A secret endpoint's URL is not part of this. Its path and query join the request line only as
/// the bytes are written ([`Client::send_to`]), and both message forms name the endpoint by its
/// origin alone ([`BaseUrl::origin`]), so a log line at any level, `/status` and `last_error`
/// see `POST https://hooks.example.test/…` and nothing more. Every place that names an http
/// request goes through these two fields: that is the one redacting display.
struct HttpRequest {
    client: Arc<HttpClient>,
    req: RawRequest,
    /// The endpoint's own path and query, for an endpoint whose URL is a secret.
    secret: Option<SecretUrl>,
    /// The endpoint as a message may name it: its base URL, or its origin and an ellipsis
    /// (`https://hooks.example.test/…`) when its URL is a secret.
    url: String,
    /// `<METHOD> <url>`: what a connection error names.
    at: String,
    /// `<METHOD> <url><target>`: what a status or an unmet `expect` names. For a secret endpoint
    /// this is `at` — the target is the secret.
    at_target: String,
}

impl HttpRequest {
    async fn send(&self, timeout: Duration) -> Result<Response> {
        match &self.secret {
            // The secret path and query exist for these bytes and for nowhere else.
            Some(url) => {
                self.client
                    .send_to(&self.req, &url.target(&self.req.target), timeout)
                    .await
            }
            None => self.client.send(&self.req, timeout).await,
        }
    }
}

/// Every endpoint's host (the `ctx` host: the name or IP literal lazywf connects to) and port
/// must match the allow-list. The first one that does not names the host and the setting. A
/// `kafka` endpoint's `ctx` lists every broker of its set (the `brokers` array) alongside the
/// first one. A secret endpoint's `ctx` holds the host of the URL its reference held when the
/// target was loaded; every request checks the resolved host again (see [`Target::http_bind`]).
fn check_allowed(list: &HostList, endpoints: &[Endpoint]) -> Result<()> {
    for ep in endpoints {
        let mut hosts = vec![(
            ep.ctx["host"].as_str().unwrap_or_default().to_string(),
            ep.ctx["port"]
                .as_u64()
                .and_then(|p| u16::try_from(p).ok())
                .unwrap_or(0),
        )];
        if let Some(bs) = ep.ctx["brokers"].as_array() {
            for b in bs {
                hosts.push((
                    b["host"].as_str().unwrap_or_default().to_string(),
                    b["port"]
                        .as_u64()
                        .and_then(|p| u16::try_from(p).ok())
                        .unwrap_or(0),
                ));
            }
        }
        for (host, port) in hosts {
            check_host(list, &host, port)?;
        }
    }
    Ok(())
}

/// One host against the allow-list, with the message every refusal of it uses (at load and on
/// a request): the host and the setting, nothing else — a caller that resolves a secret URL
/// must be able to say this without naming the URL.
fn check_host(list: &HostList, host: &str, port: u16) -> Result<()> {
    if !list.allows(host, port) {
        bail!("endpoint host `{host}` (port {port}) is not in LAZYWF_ALLOWED_HOSTS");
    }
    Ok(())
}

/// The URL behind a secret endpoint's reference, resolved and checked. `reference` is what an
/// error names: the value it holds is the secret and never appears in a message.
fn secret_url(reference: &str) -> Result<SecretUrl> {
    let value = secrets::resolve(reference).with_context(|| format!("endpoint `{reference}`"))?;
    SecretUrl::parse(&value).with_context(|| format!("endpoint `{reference}`"))
}

/// The keep-alive client of a secret endpoint's origin: one per `scheme://host:port`, never one
/// per URL, so nothing cached here can carry the secret part of it. A reference that keeps
/// naming new origins is bounded by replacing the pool rather than growing without end.
fn secret_client(
    clients: &Mutex<HashMap<String, Arc<HttpClient>>>,
    url: &SecretUrl,
    tls: &TlsSpec,
) -> Result<Arc<HttpClient>> {
    let key = format!(
        "{}://{}",
        if url.base.secure { "https" } else { "http" },
        url.base.host_header()
    );
    let mut pool = clients.lock().expect("the endpoint's client pool");
    if let Some(client) = pool.get(&key) {
        return Ok(client.clone());
    }
    if pool.len() >= 16 {
        pool.clear();
    }
    let client = Arc::new(HttpClient::new(url.base.clone(), tls)?);
    pool.insert(key, client.clone());
    Ok(client)
}

impl Target {
    /// Loads a target. With an allow-list (`LAZYWF_ALLOWED_HOSTS`), every endpoint's effective
    /// host and port must match it, or the target is refused.
    pub fn new(def: TargetDef, pool: Pool, allowed: Option<&HostList>) -> Result<Arc<Target>> {
        let expect = Expect::new(def.expect.as_deref()).context("`expect`")?;
        let mut endpoints = Vec::new();
        let mut aws_rt = None;
        let mut lambda_url = None;
        let mut sns = None;
        match def.kind {
            Kind::MySql | Kind::PgSql => {
                use crate::sqlwire::{Credentials, MySqlClient, PgClient, SqlAddr};
                let d = def.kind.dialect();
                for e in &def.endpoints {
                    let addr = SqlAddr::parse(e, d)?;
                    let ctx = addr.ctx(d);
                    let creds = Credentials {
                        user: def_var(&def, "username").map(str::to_string),
                        password: def_var(&def, "password").map(str::to_string),
                    };
                    let transport = if def.kind == Kind::MySql {
                        Transport::MySql(MySqlClient::new(addr, &def.tls, creds))
                    } else {
                        Transport::Pg(PgClient::new(addr, &def.tls, creds))
                    };
                    endpoints.push(Endpoint { ctx, transport });
                }
            }
            Kind::Amqp => {
                for e in &def.endpoints {
                    let (ctx, client) = crate::amqp::Client::new(&def, e)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Amqp(Box::new(client)),
                    });
                }
            }
            Kind::Smtp => {
                for e in &def.endpoints {
                    let (ctx, client) = crate::smtp::Client::new(&def, e)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Smtp(Box::new(client)),
                    });
                }
            }
            Kind::Redis => {
                for e in &def.endpoints {
                    let addr = RedisAddr::parse(e)?;
                    let ctx = json!({"host": addr.host, "port": addr.port, "db": addr.db, "url": addr.url()});
                    let client = RedisClient::new(
                        addr,
                        &def.tls,
                        def_var(&def, "username").map(str::to_string),
                        def_var(&def, "password").map(str::to_string),
                    )?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Redis(client),
                    });
                }
            }
            Kind::HttpS3 => {
                let path_style = match def.vars.get("path_style") {
                    Some(Value::Bool(b)) => *b,
                    Some(Value::String(s)) => {
                        matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
                    }
                    Some(Value::Number(n)) => n.as_u64() == Some(1),
                    _ => false,
                };
                let spec = S3Spec {
                    bucket: def_var(&def, "bucket").unwrap_or_default().to_string(),
                    region: def_var(&def, "region").unwrap_or_default().to_string(),
                    path_style,
                };
                let mut services: Vec<BaseUrl> = def
                    .endpoints
                    .iter()
                    .map(|e| BaseUrl::parse(e))
                    .collect::<Result<_>>()?;
                if services.is_empty() {
                    services.push(match def_var(&def, "endpoint") {
                        Some(e) => BaseUrl::parse(e).context("`vars.endpoint`")?,
                        None => spec.aws_endpoint(),
                    });
                }
                for svc in services {
                    let (base, prefix) = spec.address(&svc);
                    let ctx = json!({"host": base.host, "port": base.port, "url": format!("{}{prefix}", base.url())});
                    let host_header = base.host_header();
                    let client = Arc::new(HttpClient::new(base, &def.tls)?);
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Http {
                            client,
                            prefix,
                            host_header,
                        },
                    });
                }
                let provider = aws::Provider::new(aws_refs(&def), &spec.region);
                aws_rt = Some(AwsSign {
                    service: "s3",
                    region: spec.region.clone(),
                    provider,
                });
            }
            Kind::PubSub => {
                // from_row: exactly one `gc-pubsub://` endpoint (the publish path is built from it).
                let client = crate::pubsub::Client::new(&def)?;
                endpoints.push(Endpoint {
                    ctx: client.ctx(),
                    transport: Transport::PubSub(Box::new(client)),
                });
            }
            Kind::Kinesis => {
                // from_row: exactly one `aws-kinesis://[<region>/]<stream>` endpoint. The request
                // goes to the service root; the stream is named in the body (`endpoint.stream`).
                let url =
                    aws::KinesisUrl::parse(def.endpoints.first().ok_or_else(|| {
                        anyhow!("kind kinesis needs its aws-kinesis:// endpoint")
                    })?)?;
                let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
                let mut base = aws::address("kinesis", url.endpoint.as_deref(), &region)?;
                let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                                 "region": region, "stream": url.stream});
                base.base = String::new();
                let host_header = base.host_header();
                let client = Arc::new(HttpClient::new(base, &def.tls)?);
                endpoints.push(Endpoint {
                    ctx,
                    transport: Transport::Http {
                        client,
                        prefix: String::new(),
                        host_header,
                    },
                });
                let provider = aws::Provider::new(aws_refs(&def), &region);
                aws_rt = Some(AwsSign {
                    service: "kinesis",
                    region,
                    provider,
                });
            }
            Kind::Sqs => {
                // from_row: exactly one `aws-sqs://[<region>/]<account>/<queue>` endpoint. The
                // request goes to the service root (`/`); the queue URL is `endpoint.url`.
                let url = aws::SqsUrl::parse(
                    def.endpoints
                        .first()
                        .ok_or_else(|| anyhow!("kind sqs needs its aws-sqs:// endpoint"))?,
                )?;
                let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
                let mut base = aws::address("sqs", url.endpoint.as_deref(), &region)?;
                let queue_url = format!("{}/{}/{}", base.url(), url.account, url.queue);
                let ctx = json!({"host": base.host, "port": base.port, "url": queue_url,
                                 "region": region, "queue": url.queue});
                base.base = String::new();
                let host_header = base.host_header();
                let client = Arc::new(HttpClient::new(base, &def.tls)?);
                endpoints.push(Endpoint {
                    ctx,
                    transport: Transport::Http {
                        client,
                        prefix: String::new(),
                        host_header,
                    },
                });
                let provider = aws::Provider::new(aws_refs(&def), &region);
                aws_rt = Some(AwsSign {
                    service: "sqs",
                    region,
                    provider,
                });
            }
            Kind::Lambda => {
                // from_row: exactly one `aws-lambda://[<region>/]<function>[:<qualifier>]` endpoint.
                // The request goes to the service root; its path names the function.
                let url = aws::LambdaUrl::parse(
                    def.endpoints
                        .first()
                        .ok_or_else(|| anyhow!("kind lambda needs its aws-lambda:// endpoint"))?,
                )?;
                let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
                let mut base = aws::address("lambda", url.endpoint.as_deref(), &region)?;
                let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                                 "region": region, "function": url.function,
                                 "qualifier": url.qualifier,
                                 "invocation": if url.invocation == aws::Invocation::Event { "event" } else { "sync" }});
                base.base = String::new();
                let host_header = base.host_header();
                let client = Arc::new(HttpClient::new(base, &def.tls)?);
                endpoints.push(Endpoint {
                    ctx,
                    transport: Transport::Http {
                        client,
                        prefix: String::new(),
                        host_header,
                    },
                });
                let provider = aws::Provider::new(aws_refs(&def), &region);
                aws_rt = Some(AwsSign {
                    service: "lambda",
                    region,
                    provider,
                });
                lambda_url = Some(url);
            }
            Kind::EventBridge => {
                // from_row: exactly one `aws-eventbridge://[<region>/]<event-bus>` endpoint. The
                // request goes to the service root; the bus is named in the entries
                // (`endpoint.bus`).
                let url = aws::EventBridgeUrl::parse(def.endpoints.first().ok_or_else(|| {
                    anyhow!("kind eventbridge needs its aws-eventbridge:// endpoint")
                })?)?;
                let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
                let mut base = aws::address("events", url.endpoint.as_deref(), &region)?;
                let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                                 "region": region, "bus": url.bus});
                base.base = String::new();
                let host_header = base.host_header();
                let client = Arc::new(HttpClient::new(base, &def.tls)?);
                endpoints.push(Endpoint {
                    ctx,
                    transport: Transport::Http {
                        client,
                        prefix: String::new(),
                        host_header,
                    },
                });
                let provider = aws::Provider::new(aws_refs(&def), &region);
                aws_rt = Some(AwsSign {
                    service: "events",
                    region,
                    provider,
                });
            }
            Kind::Sns => {
                // from_row: exactly one `aws-sns://[<region>/]<account-id>/<topic-name>` endpoint.
                // The request goes to the service root (`/`); the topic is named by its ARN in
                // the body, as `sqs` names its queue by `endpoint.url`.
                let url = aws::SnsUrl::parse(
                    def.endpoints
                        .first()
                        .ok_or_else(|| anyhow!("kind sns needs its aws-sns:// endpoint"))?,
                )?;
                let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
                let mut base = aws::address("sns", url.endpoint.as_deref(), &region)?;
                let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                                 "region": region, "topic": url.topic,
                                 "topic_arn": url.arn(&region), "fifo": url.fifo()});
                base.base = String::new();
                let host_header = base.host_header();
                let client = Arc::new(HttpClient::new(base, &def.tls)?);
                endpoints.push(Endpoint {
                    ctx,
                    transport: Transport::Http {
                        client,
                        prefix: String::new(),
                        host_header,
                    },
                });
                let provider = aws::Provider::new(aws_refs(&def), &region);
                aws_rt = Some(AwsSign {
                    service: "sns",
                    region,
                    provider,
                });
                // The topic's MaximumMessageSize is the batch's byte limit; a var that does not
                // state it is refused here as at load (`TargetDef::from_row` checks it too).
                sns = Some(Sns {
                    url,
                    max_bytes: crate::sns::maximum_message_size(&def.vars)?,
                });
            }
            Kind::Kafka => {
                // from_row: exactly one endpoint, `kafka(s)://<broker>[:port][,…]/<topic>` —
                // one broker set and the topic the records of an action go to.
                for e in &def.endpoints {
                    let (ctx, client) = crate::kafka::Client::new(&def, e)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Kafka(Box::new(client)),
                    });
                }
            }
            Kind::RabbitMqStream => {
                // from_row: exactly one endpoint, `rabbitmq-stream(+tls)://<host>[:port]/
                // [vhost/]stream` — one node and the stream (or super stream) the records of
                // an action go to.
                for e in &def.endpoints {
                    let (ctx, client) = crate::rabbitmq_stream::Client::new(&def, e)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::RabbitMqStream(Box::new(client)),
                    });
                }
            }
            Kind::Grpc => {
                // from_row: exactly one endpoint, `grpc(s)://<host>[:port]/<Service>/<Method>`.
                // The descriptor set, the item type and the method's shape are all read here,
                // so a schema that does not fit is a target that never started and says so
                // (see `grpc::Client::new`); the stream itself is opened per flush.
                for e in &def.endpoints {
                    let (ctx, client) = crate::grpc::Client::new(&def, e)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Grpc(Box::new(client)),
                    });
                }
            }
            Kind::ServiceBus => {
                // from_row: exactly one endpoint, `sb://<namespace>.servicebus.windows.net/
                // <queue-or-topic>` (or a plain host with a port for a local broker).
                for e in &def.endpoints {
                    let (ctx, client) = crate::servicebus::Client::new(&def, e)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::ServiceBus(Box::new(client)),
                    });
                }
            }
            _ => {
                // The plain http kinds. An endpoint is an address, or a reference whose value is
                // the full URL — for an incoming webhook the URL *is* the credential, so it
                // belongs behind `env:` / `file:` like any other secret.
                for e in &def.endpoints {
                    if !secrets::is_reference(e) {
                        let base = BaseUrl::parse(e)?;
                        let ctx = json!({"host": base.host, "port": base.port, "url": base.url()});
                        let host_header = base.host_header();
                        let prefix = base.base.clone();
                        let client = Arc::new(HttpClient::new(base, &def.tls)?);
                        endpoints.push(Endpoint {
                            ctx,
                            transport: Transport::Http {
                                client,
                                prefix,
                                host_header,
                            },
                        });
                        continue;
                    }
                    // Resolved once here to check the form and the allow-list; every request
                    // resolves it again, so a rotated file applies without a reload.
                    let url = secret_url(e)?;
                    let ctx = json!({
                        "host": url.base.host,
                        "port": url.base.port,
                        // The origin alone: a template cannot reach past it (see `BaseUrl::origin`).
                        "url": url.base.origin(),
                    });
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::HttpSecret {
                            reference: e.clone(),
                            clients: Mutex::new(HashMap::new()),
                        },
                    });
                }
            }
        }
        if let Some(list) = allowed {
            check_allowed(list, &endpoints)?;
        }
        let sign = crate::sign::spec(&def.vars)?;
        let ctx = json!({"name": def.name, "vars": def.template_vars()});
        let kafka = (def.kind == Kind::Kafka)
            .then(|| crate::kafka::message_max_bytes(&def.vars))
            .and_then(|r| r.ok());
        let grpc = (def.kind == Kind::Grpc)
            .then(|| crate::grpc::options(&def.vars).map(|o| o.max_message_bytes))
            .and_then(|r| r.ok());
        let servicebus = (def.kind == Kind::ServiceBus)
            .then(|| crate::servicebus::max_message_bytes(&def.vars))
            .and_then(|r| r.ok());
        Ok(Arc::new(Target {
            def,
            ctx,
            endpoints,
            expect,
            aws: aws_rt,
            sign,
            lambda: lambda_url,
            kafka,
            grpc,
            servicebus,
            sns,
            allowed: allowed.cloned(),
            pool,
        }))
    }

    /// Whether this kind's answer to a batch says per item what happened (see `send_items`).
    pub fn reports_items(&self) -> bool {
        self.def.kind.reports_items()
    }

    /// The largest rendered action this target takes, when it has one: the kind's own limit, the
    /// payload limit of the Lambda invocation (6 MB sync, 1 MB event), the `kafka` target's
    /// broker limit raised by `vars.message_max_bytes`, or the `sns` target's topic
    /// `MaximumMessageSize` stated by `vars.maximum_message_size`.
    pub fn batch_bytes_limit(&self) -> Option<usize> {
        if let Some(n) = self.kafka {
            return Some(n);
        }
        if let Some(n) = self.grpc {
            return Some(n);
        }
        if let Some(n) = self.servicebus {
            return Some(n);
        }
        if let Some(s) = &self.sns {
            return Some(s.max_bytes);
        }
        match &self.lambda {
            Some(u) => Some(u.payload_limit()),
            None => self.def.kind.batch_bytes_limit(),
        }
    }

    /// One Invoke of a `lambda` target with the rendered payload (the template's body), signed
    /// through the AWS path of `prepare`. `Ok(None)`: nothing rendered. `Ok(Some(failed))`: the
    /// answer is met, and `failed` lists the identifiers the function left out (`batchItemFailures`;
    /// always empty for an event). `Err`: refused ([`Rejected`]) or transient, as for `send`: a
    /// non-2xx status (5xx, 408, 429 and AWS throttling codes are transient), an
    /// `X-Amz-Function-Error` (refused), or a refused `expect` (sync only; an event is met on 202).
    async fn lambda_invoke(&self, ep: usize, rendered: &str) -> Result<Option<Vec<String>>> {
        let (Some(endpoint), Some(url)) = (self.endpoints.get(ep), self.lambda.as_ref()) else {
            bail!("endpoint {ep} of target `{}` is gone", self.def.name);
        };
        if !matches!(endpoint.transport, Transport::Http { .. }) {
            bail!("target `{}` is not a lambda target", self.def.name);
        }
        if rendered.trim().is_empty() {
            return Ok(None);
        }
        if rendered.len() > url.payload_limit() {
            return Err(rejected(format!(
                "the payload is {} bytes, over the {} bytes an Invoke takes",
                rendered.len(),
                url.payload_limit()
            )));
        }
        let invocation = match url.invocation {
            aws::Invocation::Sync => "RequestResponse",
            aws::Invocation::Event => "Event",
        };
        let req = RawRequest {
            method: "POST".into(),
            target: url.path(),
            headers: vec![
                ("Content-Type".into(), "application/json".into()),
                ("X-Amz-Invocation-Type".into(), invocation.into()),
            ],
            body: rendered.as_bytes().to_vec(),
        };
        let h = self.http_bind(endpoint, req).await?;
        let at = h.at_target.clone();
        let resp = h.send(self.def.timeout).await.with_context(|| at.clone())?;
        if !(200..300).contains(&resp.status) {
            let msg = format!("{at}: status {}: {}", resp.status, resp.body_snippet());
            let transient = resp.status >= 500
                || matches!(resp.status, 408 | 429)
                || self.aws_throttled(&resp.headers, &resp.body);
            return Err(if transient {
                anyhow!(msg)
            } else {
                rejected(msg)
            });
        }
        if let Some(err) = aws::function_error(&resp.headers) {
            return Err(rejected(format!(
                "{at}: the function raised an error ({err}): {}",
                resp.body_snippet()
            )));
        }
        match url.invocation {
            aws::Invocation::Event => {
                if resp.status != 202 {
                    bail!("{at}: an event invocation answers 202, not {}", resp.status);
                }
                Ok(Some(Vec::new()))
            }
            aws::Invocation::Sync => {
                let verdict = self
                    .expect
                    .http(&self.pool, resp.status, &resp.headers, &resp.body)
                    .await
                    .with_context(|| at.clone())?;
                if let Err(e) = verdict {
                    return Err(rejected(format!("{at}: {e}: {}", resp.body_snippet())));
                }
                Ok(Some(
                    aws::lambda_failed_items(&resp.body).with_context(|| at)?,
                ))
            }
        }
    }

    /// A non-2xx answer of an AWS kind whose error code says "not now" (throttling,
    /// unavailable), whatever the status (AWS JSON errors come as HTTP 400).
    fn aws_throttled(&self, headers: &[(String, String)], body: &[u8]) -> bool {
        self.aws.is_some()
            && aws::error_code(headers, body).is_some_and(|c| aws::transient_code(&c))
    }

    pub fn is_http(&self) -> bool {
        self.def.kind.is_http()
    }

    /// Sends one rendered action to endpoint `ep` and checks `expect`. An error wrapping
    /// [`Rejected`] means the target refused this action (it would refuse it again); any
    /// other error is transient (the target or the way to it is unwell).
    pub async fn send(&self, ep: usize, rendered: &str) -> Result<Sent> {
        let Some(endpoint) = self.endpoints.get(ep) else {
            bail!("endpoint {ep} of target `{}` is gone", self.def.name);
        };
        if self.def.kind == Kind::Lambda {
            // A lambda answers per item (see `send_items`); one action on its own cannot name
            // the items it lists, so any listed item is a transient failure here.
            return match self.lambda_invoke(ep, rendered).await? {
                None => Ok(Sent::Skipped),
                Some(failed) if failed.is_empty() => Ok(Sent::Ok),
                Some(failed) => Err(anyhow!(
                    "batchItemFailures lists {failed:?}: the action is not confirmed"
                )),
            };
        }
        match &endpoint.transport {
            Transport::Http { .. } | Transport::HttpSecret { .. } => {
                let req = match RawRequest::parse(rendered) {
                    Ok(Some(r)) => r,
                    Ok(None) => return Ok(Sent::Skipped),
                    Err(e) => return Err(rejected(format!("the rendered request: {e:#}"))),
                };
                let h = self.http_bind(endpoint, req).await?;
                let resp = h
                    .send(self.def.timeout)
                    .await
                    .with_context(|| h.at.clone())?;
                let verdict = self
                    .expect
                    .http(&self.pool, resp.status, &resp.headers, &resp.body)
                    .await
                    .with_context(|| h.at_target.clone())?;
                if let Err(e) = verdict {
                    let msg = format!("{}: {e}: {}", h.at_target, resp.body_snippet());
                    // 5xx, 408 and 429 say "not now", and so do AWS throttling codes; other
                    // refusals are about this request.
                    let transient = resp.status >= 500
                        || matches!(resp.status, 408 | 429)
                        || self.aws_throttled(&resp.headers, &resp.body);
                    return Err(if transient {
                        anyhow!(msg)
                    } else {
                        rejected(msg)
                    });
                }
                Ok(Sent::Ok)
            }
            Transport::Amqp(client) => {
                crate::amqp::send(client, rendered, &self.expect, &self.pool).await
            }
            Transport::Smtp(client) => {
                crate::smtp::send(client, rendered, &self.expect, &self.pool).await
            }
            Transport::Redis(client) => {
                let args = redis::tokenize(rendered)
                    .map_err(|e| rejected(format!("the rendered command: {e:#}")))?;
                if args.is_empty() {
                    return Ok(Sent::Skipped);
                }
                let name = String::from_utf8_lossy(&args[0]).to_uppercase();
                let reply = client
                    .command(&args, self.def.timeout)
                    .await
                    .with_context(|| format!("{name} on {}", client.addr.url()))?;
                if let Some(e) = reply.error() {
                    let msg = format!("{name} on {}: {e}", client.addr.url());
                    return Err(if redis::transient_error(e) {
                        anyhow!(msg)
                    } else {
                        rejected(msg)
                    });
                }
                self.expect
                    .row(&self.pool, |u| Row::reply(u, &reply))
                    .await
                    .with_context(|| format!("{name} on {}", client.addr.url()))?
                    .map_err(|e| rejected(format!("{name} on {}: {e}", client.addr.url())))?;
                Ok(Sent::Ok)
            }
            Transport::PubSub(client) => {
                crate::pubsub::send(client, rendered, self.def.timeout, &self.expect, &self.pool)
                    .await
            }
            Transport::Kafka(client) => {
                // A `once` reset step: no keys, the records take partition 0 (see `kafka.rs`).
                crate::kafka::send(client, rendered, &self.expect, &self.pool).await
            }
            Transport::RabbitMqStream(client) => {
                // A `once` reset step: no keys, no `lazywf_key` property (see
                // `rabbitmq_stream.rs`).
                crate::rabbitmq_stream::send(client, rendered, &self.expect, &self.pool).await
            }
            Transport::Grpc(client) => {
                crate::grpc::send(client, rendered, &self.expect, &self.pool).await
            }
            Transport::ServiceBus(client) => {
                // A `once` reset step: no keys, the messages carry no key (see `servicebus.rs`).
                crate::servicebus::send(client, rendered, &self.expect, &self.pool).await
            }
            Transport::MySql(_) | Transport::Pg(_) => {
                let sql = match crate::sqlwire::single_statement(rendered) {
                    Ok(Some(s)) => s,
                    Ok(None) => return Ok(Sent::Skipped),
                    Err(e) => return Err(rejected(format!("{e:#}"))),
                };
                let (affected, at) = match &endpoint.transport {
                    Transport::MySql(c) => (
                        c.exec(sql, self.def.timeout).await?,
                        format!("{}/{}", c.addr.host, c.addr.db),
                    ),
                    Transport::Pg(c) => (
                        c.exec(&sql, self.def.timeout).await?,
                        format!("{}/{}", c.addr.host, c.addr.db),
                    ),
                    _ => unreachable!(),
                };
                self.expect
                    .row(&self.pool, |u| Row::affected(u, affected))
                    .await
                    .with_context(|| at.clone())?
                    .map_err(|e| rejected(format!("{at}: {e}")))?;
                Ok(Sent::Ok)
            }
        }
    }

    /// Sends a listing request (stray reset steps) and returns the response as the row `r`
    /// with the columns in `uses`: http must answer 2xx (and JSON when `body` is read), redis
    /// any reply but an error, SQL targets give their rows as `body`.
    pub async fn fetch(&self, ep: usize, rendered: &str, uses: Uses) -> Result<Row> {
        let Some(endpoint) = self.endpoints.get(ep) else {
            bail!("endpoint {ep} of target `{}` is gone", self.def.name);
        };
        match &endpoint.transport {
            Transport::Http { .. } | Transport::HttpSecret { .. } => {
                let req = RawRequest::parse(rendered)?
                    .ok_or_else(|| anyhow!("the list template rendered nothing"))?;
                let h = self.http_bind(endpoint, req).await?;
                let resp = h.send(self.def.timeout).await?;
                if !(200..300).contains(&resp.status) {
                    bail!(
                        "{}: status {}: {}",
                        h.at_target,
                        resp.status,
                        resp.body_snippet()
                    );
                }
                if uses.body && serde_json::from_slice::<Value>(&resp.body).is_err() {
                    bail!("the listing response is not JSON: {}", resp.body_snippet());
                }
                Ok(Row::http(uses, resp.status, &resp.headers, &resp.body))
            }
            Transport::Amqp(_) => bail!("kind amqp appends: it cannot list keys"),
            Transport::Smtp(_) => bail!("kind smtp appends: it cannot list keys"),
            Transport::Redis(client) => {
                let args = redis::tokenize(rendered)?;
                if args.is_empty() {
                    bail!("the list template rendered nothing");
                }
                let reply: Reply = client.command(&args, self.def.timeout).await?;
                if let Some(e) = reply.error() {
                    bail!("{e}");
                }
                Ok(Row::reply(uses, &reply))
            }
            Transport::PubSub(_) => {
                bail!("pubsub targets only publish: a listing is refused")
            }
            Transport::Kafka(_) => bail!("kind kafka appends: it cannot list keys"),
            Transport::RabbitMqStream(_) => {
                bail!("kind rabbitmq-stream appends: it cannot list keys")
            }
            Transport::Grpc(_) => bail!("kind grpc pushes: it cannot list keys"),
            Transport::ServiceBus(_) => bail!("kind servicebus appends: it cannot list keys"),
            Transport::MySql(c) => {
                let sql = crate::sqlwire::single_statement(rendered)?
                    .ok_or_else(|| anyhow!("the list template rendered nothing"))?;
                Ok(Row::json(uses, &c.rows(sql, self.def.timeout).await?))
            }
            Transport::Pg(c) => {
                let sql = crate::sqlwire::single_statement(rendered)?
                    .ok_or_else(|| anyhow!("the list template rendered nothing"))?;
                Ok(Row::json(uses, &c.rows(&sql, self.def.timeout).await?))
            }
        }
    }

    /// Sends one batch request whose answer says per item (`SendMessageBatch`-style: `Successful`
    /// and `Failed` entries with the `Id` of the item, the index of its key; a Lambda's
    /// `batchItemFailures` naming the key). `keys[i]` is item `i`. Returns one [`Item`] per item,
    /// in order. `Err`: the whole request was refused (rejected) or failed (transient), as for
    /// [`Target::send`]; the items then have no outcome of their own.
    pub async fn send_items(
        &self,
        ep: usize,
        rendered: &str,
        keys: &[String],
    ) -> Result<Vec<Item>> {
        let n = keys.len();
        if self.def.kind == Kind::Lambda {
            return match self.lambda_invoke(ep, rendered).await? {
                None => Ok(vec![Item::Ok; n]),
                Some(failed) => lambda_items(&failed, keys),
            };
        }
        if self.def.kind == Kind::Kafka {
            let Some(endpoint) = self.endpoints.get(ep) else {
                bail!("endpoint {ep} of target `{}` is gone", self.def.name);
            };
            let Transport::Kafka(client) = &endpoint.transport else {
                bail!("target `{}` is not a kafka target", self.def.name);
            };
            return crate::kafka::send_items(client, rendered, keys, &self.expect, &self.pool)
                .await;
        }
        if self.def.kind == Kind::RabbitMqStream {
            let Some(endpoint) = self.endpoints.get(ep) else {
                bail!("endpoint {ep} of target `{}` is gone", self.def.name);
            };
            let Transport::RabbitMqStream(client) = &endpoint.transport else {
                bail!("target `{}` is not a rabbitmq-stream target", self.def.name);
            };
            return crate::rabbitmq_stream::send_items(
                client,
                rendered,
                keys,
                &self.expect,
                &self.pool,
            )
            .await;
        }
        if self.def.kind == Kind::Grpc {
            let Some(endpoint) = self.endpoints.get(ep) else {
                bail!("endpoint {ep} of target `{}` is gone", self.def.name);
            };
            let Transport::Grpc(client) = &endpoint.transport else {
                bail!("target `{}` is not a grpc target", self.def.name);
            };
            return crate::grpc::send_items(client, rendered, keys, &self.expect, &self.pool).await;
        }
        if self.def.kind == Kind::ServiceBus {
            let Some(endpoint) = self.endpoints.get(ep) else {
                bail!("endpoint {ep} of target `{}` is gone", self.def.name);
            };
            let Transport::ServiceBus(client) = &endpoint.transport else {
                bail!("target `{}` is not a servicebus target", self.def.name);
            };
            return crate::servicebus::send_items(client, rendered, keys, &self.expect, &self.pool)
                .await;
        }
        let Some(endpoint) = self.endpoints.get(ep) else {
            bail!("endpoint {ep} of target `{}` is gone", self.def.name);
        };
        if !matches!(endpoint.transport, Transport::Http { .. }) {
            bail!("target `{}` does not answer per item", self.def.name);
        }
        let req = match RawRequest::parse(rendered) {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(vec![Item::Ok; n]),
            Err(e) => return Err(rejected(format!("the rendered request: {e:#}"))),
        };
        if self.def.kind == Kind::EventBridge {
            // The batch is answered by position: check the rendered entries before the request
            // goes out (see [`eventbridge_entries_ok`]).
            if let Err(why) = eventbridge_entries_ok(&req.body, n) {
                return Err(rejected(format!("the rendered request: {why}")));
            }
        }
        if self.def.kind == Kind::Sns {
            // The batch is answered per entry, by its `Id`: check the rendered form body before
            // the request goes out (see [`crate::sns::entries_ok`]).
            let fifo = self.sns.as_ref().is_some_and(|s| s.url.fifo());
            if let Err(why) = crate::sns::entries_ok(&req.body, n, fifo) {
                return Err(rejected(format!("the rendered request: {why}")));
            }
        }
        let h = self.http_bind(endpoint, req).await?;
        let resp = h
            .send(self.def.timeout)
            .await
            .with_context(|| h.at.clone())?;
        if !(200..300).contains(&resp.status) {
            let msg = format!(
                "{}: status {}: {}",
                h.at_target,
                resp.status,
                resp.body_snippet()
            );
            let transient = resp.status >= 500
                || matches!(resp.status, 408 | 429)
                || self.aws_throttled(&resp.headers, &resp.body);
            return Err(if transient {
                anyhow!(msg)
            } else {
                rejected(msg)
            });
        }
        let answer: Value = if self.def.kind == Kind::Sns {
            // `PublishBatch` is the query protocol, whose answer is XML: read into the same
            // `Successful`/`Failed` shape the mapping below reads (see
            // [`crate::sns::batch_answer`]).
            crate::sns::batch_answer(&resp.body).with_context(|| {
                format!(
                    "{}: the answer is not a PublishBatchResult: {}",
                    h.at,
                    resp.body_snippet()
                )
            })?
        } else {
            serde_json::from_slice(&resp.body).with_context(|| {
                format!("{}: the answer is not JSON: {}", h.at, resp.body_snippet())
            })?
        };
        if self.def.kind == Kind::Kinesis {
            // `Records[i]` answers item i. `expect` per record, as for the entries below.
            let records = answer
                .get("Records")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let item = match kinesis_verdict(records.get(i), i) {
                    RecordVerdict::Stored(bytes) => match self
                        .expect
                        .http(&self.pool, resp.status, &resp.headers, &bytes)
                        .await
                        .with_context(|| format!("record {i} of {}", h.url))?
                    {
                        Ok(()) => Item::Ok,
                        Err(why) => Item::Rejected(format!("record {i}: {why}")),
                    },
                    RecordVerdict::Transient(m) => Item::Transient(m),
                    RecordVerdict::Rejected(m) => Item::Rejected(m),
                    RecordVerdict::Missing => Item::Transient(format!("record {i}: no answer")),
                };
                out.push(item);
            }
            return Ok(out);
        }
        if self.def.kind == Kind::EventBridge {
            // `Entries[i]` answers item i. `expect` per entry, as for the records above.
            let entries = answer
                .get("Entries")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let item = match eventbridge_verdict(entries.get(i), i) {
                    RecordVerdict::Stored(bytes) => match self
                        .expect
                        .http(&self.pool, resp.status, &resp.headers, &bytes)
                        .await
                        .with_context(|| format!("entry {i} of {}", h.url))?
                    {
                        Ok(()) => Item::Ok,
                        Err(why) => Item::Rejected(format!("entry {i}: {why}")),
                    },
                    RecordVerdict::Transient(m) => Item::Transient(m),
                    RecordVerdict::Rejected(m) => Item::Rejected(m),
                    RecordVerdict::Missing => Item::Transient(format!("entry {i}: no answer")),
                };
                out.push(item);
            }
            return Ok(out);
        }
        let entries = |field: &str| -> Vec<Value> {
            answer
                .get(field)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let index = |e: &Value| -> Result<usize> {
            let id = e.get("Id").and_then(|v| match v {
                Value::String(s) => s.parse::<usize>().ok(),
                Value::Number(n) => n.as_u64().map(|n| n as usize),
                _ => None,
            });
            match id {
                Some(i) if i < n => Ok(i),
                _ => bail!("the answer names item {:?}, not one of {n}", e.get("Id")),
            }
        };
        let mut out: Vec<Option<Item>> = vec![None; n];
        for e in entries("Successful") {
            let i = index(&e)?;
            // `expect` per item: `status` is the batch's, `body` the entry.
            let bytes = serde_json::to_vec(&e)?;
            let item = match self
                .expect
                .http(&self.pool, resp.status, &resp.headers, &bytes)
                .await
                .with_context(|| format!("item {i} of {}", h.url))?
            {
                Ok(()) => Item::Ok,
                Err(why) => Item::Rejected(format!("item {i}: {why}")),
            };
            out[i] = Some(item);
        }
        for e in entries("Failed") {
            let i = index(&e)?;
            let text = |k: &str| e.get(k).and_then(Value::as_str).unwrap_or("").to_string();
            let why = format!("item {i}: {}: {}", text("Code"), text("Message"));
            // Only a `SenderFault: true` entry is refused. Without a clear answer, the item
            // is retried (nothing is lost by waiting).
            out[i] = Some(match e.get("SenderFault").and_then(Value::as_bool) {
                Some(true) => Item::Rejected(why),
                _ => Item::Transient(why),
            });
        }
        Ok(out
            .into_iter()
            .enumerate()
            .map(|(i, it)| it.unwrap_or_else(|| Item::Transient(format!("item {i}: no answer"))))
            .collect())
    }

    /// Resolves `endpoint` and prepares `req` for it: the URL, the `Host` header, the kind's
    /// credential, the webhook signature. An endpoint whose URL is a secret is resolved here, on
    /// every request (so a rotated file applies), its host is checked against the allow-list
    /// again, and the answer names it by its origin only: the path and the query join the request
    /// line as [`HttpRequest::send`] writes the bytes, and reach no message.
    async fn http_bind(&self, endpoint: &Endpoint, req: RawRequest) -> Result<HttpRequest> {
        match &endpoint.transport {
            Transport::Http {
                client,
                prefix,
                host_header,
            } => {
                let req = self.prepare(req, prefix, host_header).await?;
                let url = client.base.url();
                Ok(HttpRequest {
                    at: format!("{} {url}", req.method),
                    at_target: format!("{} {url}{}", req.method, req.target),
                    client: client.clone(),
                    req,
                    secret: None,
                    url,
                })
            }
            Transport::HttpSecret { reference, clients } => {
                let url = secret_url(reference)?;
                if let Some(list) = &self.allowed {
                    // Refused, not waited out: a host the operator did not allow will not start
                    // allowing this target, whatever the file says later.
                    check_host(list, &url.base.host, url.base.port)
                        .map_err(|e| rejected(e.to_string()))?;
                }
                let host_header = url.base.host_header();
                // No prefix: the endpoint's path belongs to the request line, not in front of it.
                let req = self.prepare(req, "", &host_header).await?;
                let client = secret_client(clients, &url, &self.def.tls)?;
                let url_shown = url.base.origin();
                let at = format!("{} {url_shown}", req.method);
                Ok(HttpRequest {
                    // The target is the secret: a message stops at the origin.
                    at_target: at.clone(),
                    client,
                    req,
                    secret: Some(url),
                    url: url_shown,
                    at,
                })
            }
            _ => bail!("target `{}` is not an http target", self.def.name),
        }
    }

    async fn prepare(
        &self,
        mut req: RawRequest,
        prefix: &str,
        host_header: &str,
    ) -> Result<RawRequest> {
        if req.header("user-agent").is_none() {
            req.set_header("User-Agent", USER_AGENT.to_string());
        }
        if let Some(a) = &self.aws {
            let c = a.provider.get().await?;
            aws::sign(
                &mut req,
                host_header,
                prefix,
                &a.region,
                a.service,
                &c,
                crate::time::unix_now(),
            )?;
            return Ok(req);
        }
        if !prefix.is_empty() && req.target.starts_with('/') {
            req.target = format!("{prefix}{}", req.target);
        }
        if req.header("host").is_none() {
            req.set_header("Host", host_header.to_string());
        }
        let get = |k: &str| -> Result<String> {
            secrets::resolve(def_var(&self.def, k).ok_or_else(|| anyhow!("vars.{k} is missing"))?)
                .with_context(|| format!("vars.{k}"))
        };
        match self.def.kind {
            Kind::HttpBasic => {
                let token = base64::engine::general_purpose::STANDARD.encode(format!(
                    "{}:{}",
                    get("username")?,
                    get("password")?
                ));
                req.set_header("Authorization", format!("Basic {token}"));
            }
            Kind::HttpBearer => {
                req.set_header("Authorization", format!("Bearer {}", get("token")?))
            }
            Kind::HttpHeader => {
                let name = def_var(&self.def, "header").unwrap_or_default().to_string();
                req.set_header(&name, get("value")?);
            }
            _ => {}
        }
        // The signature goes over the exact body the request will send.
        if let Some(s) = &self.sign {
            crate::sign::apply(&mut req, &self.def.name, s, crate::time::unix_now())?;
        }
        Ok(req)
    }
}

#[cfg(test)]
mod lambda_items_tests {
    use super::*;

    fn keys(ks: &[&str]) -> Vec<String> {
        ks.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn nothing_listed_meets_every_key() {
        assert_eq!(
            lambda_items(&[], &keys(&["a", "b"])).unwrap(),
            vec![Item::Ok, Item::Ok]
        );
    }

    #[test]
    fn only_the_listed_keys_retry() {
        let items = lambda_items(&["b".into(), "d".into()], &keys(&["a", "b", "c", "d"])).unwrap();
        assert_eq!(items[0], Item::Ok);
        assert!(matches!(&items[1], Item::Transient(m) if m.contains("`b`")));
        assert_eq!(items[2], Item::Ok);
        assert!(matches!(&items[3], Item::Transient(m) if m.contains("`d`")));
    }

    #[test]
    fn an_identifier_that_is_not_a_key_is_a_wrong_answer() {
        let e = lambda_items(&["zz".into()], &keys(&["a"])).unwrap_err();
        assert!(e.to_string().contains("`zz`"), "{e}");
    }
}

#[cfg(test)]
mod kinesis_verdict_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_record_without_error_code_is_stored_and_checked_by_expect() {
        let e = json!({"SequenceNumber": "49590", "ShardId": "shardId-000000000000"});
        match kinesis_verdict(Some(&e), 0) {
            RecordVerdict::Stored(bytes) => {
                assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), e)
            }
            other => panic!("{other:?}"),
        }
        // an empty or null code is no code
        let e = json!({"ErrorCode": null, "SequenceNumber": "1"});
        assert!(matches!(
            kinesis_verdict(Some(&e), 1),
            RecordVerdict::Stored(_)
        ));
        let e = json!({"ErrorCode": "", "SequenceNumber": "1"});
        assert!(matches!(
            kinesis_verdict(Some(&e), 1),
            RecordVerdict::Stored(_)
        ));
    }

    #[test]
    fn throttling_internal_failure_and_unavailable_are_transient() {
        let e = json!({"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "Rate exceeded for shard"});
        assert_eq!(
            kinesis_verdict(Some(&e), 3),
            RecordVerdict::Transient(
                "record 3: ProvisionedThroughputExceededException: Rate exceeded for shard".into()
            )
        );
        let e = json!({"ErrorCode": "InternalFailure", "ErrorMessage": "x"});
        assert!(matches!(
            kinesis_verdict(Some(&e), 0),
            RecordVerdict::Transient(_)
        ));
        // the owner's "keep retrying": a record the service could not take now is retried
        let e = json!({"ErrorCode": "ServiceUnavailable", "ErrorMessage": "try later"});
        assert_eq!(
            kinesis_verdict(Some(&e), 1),
            RecordVerdict::Transient("record 1: ServiceUnavailable: try later".into())
        );
    }

    #[test]
    fn any_other_code_is_a_refusal_of_that_record() {
        let e = json!({"ErrorCode": "ValidationException", "ErrorMessage": "too big"});
        assert_eq!(
            kinesis_verdict(Some(&e), 2),
            RecordVerdict::Rejected("record 2: ValidationException: too big".into())
        );
        // a service code that is not "not now" refuses the record
        let e = json!({"ErrorCode": "AccessDeniedException"});
        assert!(matches!(
            kinesis_verdict(Some(&e), 0),
            RecordVerdict::Rejected(_)
        ));
    }

    #[test]
    fn a_missing_entry_waits() {
        assert_eq!(kinesis_verdict(None, 5), RecordVerdict::Missing);
    }

    #[test]
    fn the_data_of_a_record_is_base64_of_the_document() {
        // What the `base64` template helper produces for a document, decoded back.
        let doc = br#"{"id":1}"#;
        let data = base64::engine::general_purpose::STANDARD.encode(doc);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(data)
            .unwrap();
        assert_eq!(decoded, doc);
    }
}

#[cfg(test)]
mod eventbridge_verdict_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_entry_with_an_event_id_is_stored_and_checked_by_expect() {
        let e = json!({"EventId": "919a0147-83c9-4f53-a66c-c6a0aabbccdd"});
        match eventbridge_verdict(Some(&e), 0) {
            RecordVerdict::Stored(bytes) => {
                assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), e)
            }
            other => panic!("{other:?}"),
        }
        // an empty or null id is no id
        let e = json!({"EventId": "", "ErrorCode": "ValidationException"});
        assert!(matches!(
            eventbridge_verdict(Some(&e), 1),
            RecordVerdict::Rejected(_)
        ));
        let e = json!({"EventId": null});
        assert_eq!(eventbridge_verdict(Some(&e), 2), RecordVerdict::Missing);
    }

    #[test]
    fn throttling_and_unwell_service_codes_are_transient() {
        for code in [
            "ThrottlingException",
            "InternalFailure",
            "ServiceUnavailable",
        ] {
            let e = json!({"ErrorCode": code, "ErrorMessage": "try later"});
            assert_eq!(
                eventbridge_verdict(Some(&e), 3),
                RecordVerdict::Transient(format!("entry 3: {code}: try later"))
            );
        }
    }

    #[test]
    fn any_other_code_is_a_refusal_of_that_entry() {
        // what moto and AWS answer for a bad entry (PutEvents result codes)
        for code in [
            "InvalidArgument",
            "MalformedDetail",
            "AccessDeniedException",
        ] {
            let e = json!({"ErrorCode": code, "ErrorMessage": "the reason"});
            assert_eq!(
                eventbridge_verdict(Some(&e), 1),
                RecordVerdict::Rejected(format!("entry 1: {code}: the reason"))
            );
        }
    }

    #[test]
    fn a_missing_or_answerless_entry_waits() {
        assert_eq!(eventbridge_verdict(None, 5), RecordVerdict::Missing);
        assert_eq!(
            eventbridge_verdict(Some(&json!({})), 0),
            RecordVerdict::Missing
        );
    }

    fn entries(n: usize) -> String {
        let one = r#"{"Source":"lazywf","DetailType":"t","Detail":"{}"}"#;
        format!("{{\"Entries\": [{}]}}", vec![one; n].join(", "))
    }

    #[test]
    fn a_batch_of_complete_entries_passes() {
        assert!(eventbridge_entries_ok(entries(3).as_bytes(), 3).is_ok());
    }

    #[test]
    fn the_body_must_be_entries_json() {
        assert!(eventbridge_entries_ok(b"not json", 1).is_err());
        assert!(eventbridge_entries_ok(br#"{"Records":[]}"#, 0).is_err());
        assert!(eventbridge_entries_ok(br#"{"Entries":{}}"#, 0).is_err());
    }

    #[test]
    fn the_entry_count_must_match_the_keys() {
        // one entry short or long: the positional answer could not be trusted
        assert!(eventbridge_entries_ok(entries(2).as_bytes(), 3).is_err());
        assert!(eventbridge_entries_ok(entries(4).as_bytes(), 3).is_err());
        assert!(eventbridge_entries_ok(br#"{"Entries": []}"#, 0).is_ok());
    }

    #[test]
    fn every_entry_needs_the_three_required_members() {
        let cases = [
            r#"{"Entries":[{"DetailType":"t","Detail":"{}"}]}"#, // no Source
            r#"{"Entries":[{"Source":"s","Detail":"{}"}]}"#,     // no DetailType
            r#"{"Entries":[{"Source":"s","DetailType":"t"}]}"#,  // no Detail
            r#"{"Entries":[{"Source":"","DetailType":"t","Detail":"{}"}]}"#, // empty
            r#"{"Entries":[{"Source":"s","DetailType":"t","Detail":null}]}"#, // not a string
            r#"{"Entries":[{"Source":"s","DetailType":"t","Detail":{"k":1}}]}"#, // the JSON, not its text
            r#"{"Entries":["a"]}"#, // not even an object
        ];
        for body in cases {
            assert!(
                eventbridge_entries_ok(body.as_bytes(), 1).is_err(),
                "accepted `{body}`"
            );
        }
    }
}

#[cfg(test)]
mod allow_tests {
    use super::*;
    use crate::model::{RawTarget, TargetDef};

    /// Loads a target of `kind` with one endpoint, under the allow-list `allowed`.
    fn load(kind: &str, endpoint: &str, vars: &str, allowed: &str) -> Result<Arc<Target>> {
        let def = TargetDef::from_row(&RawTarget {
            name: "t".into(),
            kind: Some(kind.into()),
            endpoints: Some(endpoint.into()),
            vars: Some(vars.into()),
            ..Default::default()
        })?;
        let list = HostList::parse(allowed)?;
        let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
        Target::new(def, pool, list.as_ref())
    }

    /// Each kind's effective host (the one lazywf connects to) is the one the list is matched
    /// against: the listed host loads, another host is refused and the error names it.
    #[test]
    fn each_kind_checks_its_effective_host() {
        let cases = [
            (
                "http",
                "https://api.example.test:8443/hook",
                "{}",
                "api.example.test",
            ),
            (
                "http.s3",
                "",
                r#"{"bucket":"orders","region":"eu-west-1"}"#,
                "orders.s3.eu-west-1.amazonaws.com",
            ),
            (
                "http.s3",
                "",
                r#"{"bucket":"orders","region":"eu-west-1","path_style":true}"#,
                "s3.eu-west-1.amazonaws.com",
            ),
            (
                "sqs",
                "aws-sqs://eu-west-1/123456789012/orders",
                "{}",
                "sqs.eu-west-1.amazonaws.com",
            ),
            (
                "sqs",
                "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561",
                "{}",
                "127.0.0.1",
            ),
            (
                "kinesis",
                "aws-kinesis://eu-west-1/orders",
                "{}",
                "kinesis.eu-west-1.amazonaws.com",
            ),
            (
                "lambda",
                "aws-lambda://eu-west-1/orders",
                "{}",
                "lambda.eu-west-1.amazonaws.com",
            ),
            (
                "eventbridge",
                "aws-eventbridge://eu-west-1/orders",
                "{}",
                "events.eu-west-1.amazonaws.com",
            ),
            (
                "eventbridge",
                "aws-eventbridge://eu-west-1/orders?endpoint=http://127.0.0.1:34569",
                "{}",
                "127.0.0.1",
            ),
            (
                "sns",
                "aws-sns://eu-west-1/123456789012/orders",
                "{}",
                "sns.eu-west-1.amazonaws.com",
            ),
            (
                "sns",
                "aws-sns://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34569",
                "{}",
                "127.0.0.1",
            ),
            (
                "pubsub",
                "gc-pubsub://lwf-test/orders?endpoint=http://127.0.0.1:34563",
                "{}",
                "127.0.0.1",
            ),
            (
                "pubsub",
                "gc-pubsub://lwf-test/orders",
                "{}",
                "pubsub.googleapis.com",
            ),
            (
                "redis",
                "redis://cache.example.test:6380/0",
                "{}",
                "cache.example.test",
            ),
            (
                "mysql",
                "mysql://db.example.test:3306/app",
                r#"{"username":"u","password":"p"}"#,
                "db.example.test",
            ),
            (
                "pgsql",
                "postgres://pg.example.test:5432/app",
                r#"{"username":"u","password":"p"}"#,
                "pg.example.test",
            ),
            (
                "amqp",
                "amqp://mq.example.test:5672/",
                r#"{"username":"u","password":"p"}"#,
                "mq.example.test",
            ),
            (
                "smtp",
                "smtp://mail.example.test:587",
                "{}",
                "mail.example.test",
            ),
            (
                "smtp",
                "smtps://mail.example.test:465",
                "{}",
                "mail.example.test",
            ),
            (
                "kafka",
                "kafka://mq.example.test:9094/orders",
                "{}",
                "mq.example.test",
            ),
        ];
        for (kind, endpoint, vars, host) in cases {
            let listed = load(kind, endpoint, vars, host);
            assert!(
                listed.is_ok(),
                "{kind} {endpoint}: {:?}",
                listed.err().map(|e| format!("{e:#}"))
            );
            let refused = load(kind, endpoint, vars, "unrelated.example.test").err();
            let msg = format!(
                "{:#}",
                refused.unwrap_or_else(|| panic!("{kind} {endpoint}: not refused"))
            );
            assert!(
                msg.contains(host) && msg.contains("LAZYWF_ALLOWED_HOSTS"),
                "{kind}: {msg}"
            );
        }
        // Unset: no check, whatever the host.
        assert!(load("http", "https://api.example.test/hook", "{}", "").is_ok());
    }

    /// `grpc` too: the host and port of the endpoint are the ones lazywf connects to, so they
    /// are the ones the list is matched against. The descriptor set is the shipped contract,
    /// read as a `file:` reference from a directory `LAZYWF_SECRET_DIRS` names.
    #[test]
    fn the_grpc_host_and_port_are_matched() {
        let _env = crate::secrets::ENV_LOCK.blocking_lock();
        let was = std::env::var("LAZYWF_SECRET_DIRS").ok();
        let dir = std::env::temp_dir().join(format!("lazywf-allow-grpc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let desc = dir.join("lazywf.desc");
        std::fs::write(&desc, crate::grpc::contract::DESCRIPTOR).unwrap();
        std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
        let vars = format!(
            r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item"}}"#,
            desc.display()
        );
        let ep = "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push";
        // Every load reads the descriptor set through the var, so they all happen while it is
        // set; what they said is checked once it is back as it was.
        let listed = load("grpc", ep, &vars, "ingest.example.test")
            .err()
            .map(|e| format!("{e:#}"));
        let other_port = load("grpc", ep, &vars, "ingest.example.test:9001")
            .err()
            .map(|e| format!("{e:#}"));
        let other = load("grpc", ep, &vars, "unrelated.example.test")
            .err()
            .map(|e| format!("{e:#}"));
        match was {
            Some(v) => std::env::set_var("LAZYWF_SECRET_DIRS", v),
            None => std::env::remove_var("LAZYWF_SECRET_DIRS"),
        }
        assert!(listed.is_none(), "the listed host was refused: {listed:?}");
        let msg = other_port.expect("the endpoint's port was not refused");
        assert!(
            msg.contains("ingest.example.test") && msg.contains("LAZYWF_ALLOWED_HOSTS"),
            "{msg}"
        );
        let msg = other.expect("another host was not refused");
        assert!(
            msg.contains("ingest.example.test") && msg.contains("LAZYWF_ALLOWED_HOSTS"),
            "{msg}"
        );
    }

    /// Every broker of a `kafka` endpoint is matched, not only the first: one off the list is
    /// refused, and the error names it. (The multi-broker URL holds one endpoint, so the
    /// endpoints column is the JSON-array form: a bare comma would split it.)
    #[test]
    fn every_kafka_broker_is_matched() {
        let both = r#"["kafka://mq.example.test:9094,mq2.example.test:9094/orders"]"#;
        let ok = load(
            "kafka",
            both,
            "{}",
            "mq.example.test:9094, mq2.example.test:9094",
        );
        assert!(ok.is_ok(), "{:?}", ok.err().map(|e| format!("{e:#}")));
        let refused = load("kafka", both, "{}", "mq.example.test:9094").err();
        let msg = format!(
            "{:#}",
            refused.unwrap_or_else(|| panic!("the second broker was not refused"))
        );
        assert!(
            msg.contains("mq2.example.test") && msg.contains("LAZYWF_ALLOWED_HOSTS"),
            "{msg}"
        );
    }

    #[test]
    fn the_port_of_an_endpoint_is_matched_too() {
        assert!(load(
            "http",
            "https://api.example.test:8443/hook",
            "{}",
            "api.example.test:443"
        )
        .is_err());
        assert!(load(
            "http",
            "https://api.example.test:8443/hook",
            "{}",
            "api.example.test:8443"
        )
        .is_ok());
        assert!(load(
            "redis",
            "redis://cache.example.test:6380/0",
            "{}",
            "*.example.test:6379"
        )
        .is_err());
    }

    /// Wildcards and CIDR ranges are matched on the effective host of each kind as well.
    #[test]
    fn wildcards_and_ranges_match_the_derived_hosts() {
        assert!(load(
            "http.s3",
            "",
            r#"{"bucket":"orders","region":"eu-west-1"}"#,
            "*.s3.eu-west-1.amazonaws.com"
        )
        .is_ok());
        assert!(load(
            "http.s3",
            "",
            r#"{"bucket":"orders","region":"eu-west-1"}"#,
            "s3.eu-west-1.amazonaws.com"
        )
        .is_err());
        assert!(load(
            "pubsub",
            "gc-pubsub://lwf-test/orders",
            "{}",
            "*.googleapis.com"
        )
        .is_ok());
        assert!(load(
            "sqs",
            "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://10.4.0.7:9324",
            "{}",
            "10.4.0.0/16"
        )
        .is_ok());
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use crate::model::{RawTarget, TargetDef};

    /// A target of `kind` with one endpoint, without a database and without a server:
    /// what a transport decides before it opens a connection is what is checked here.
    fn target(kind: &str, endpoint: &str, vars: &str) -> Arc<Target> {
        let def = TargetDef::from_row(&RawTarget {
            name: "t".into(),
            kind: Some(kind.into()),
            endpoints: Some(endpoint.into()),
            vars: Some(vars.into()),
            ..Default::default()
        })
        .unwrap();
        let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
        Target::new(def, pool, None).unwrap()
    }

    fn http() -> Arc<Target> {
        target("http", "https://api.example.test:8443/hook", "{}")
    }
    fn sqs() -> Arc<Target> {
        target(
            "sqs",
            "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561",
            "{}",
        )
    }
    fn lambda(event: bool) -> Arc<Target> {
        let url = if event {
            "aws-lambda://eu-west-1/orders?invocation=event"
        } else {
            "aws-lambda://eu-west-1/orders"
        };
        target("lambda", url, "{}")
    }
    fn kafka() -> Arc<Target> {
        target("kafka", "kafka://mq.example.test:9094/orders", "{}")
    }
    fn redis() -> Arc<Target> {
        target("redis", "redis://cache.example.test:6380/0", "{}")
    }
    fn smtp() -> Arc<Target> {
        target("smtp", "smtp://mail.example.test:587", "{}")
    }
    fn amqp() -> Arc<Target> {
        target(
            "amqp",
            "amqp://mq.example.test:5672/",
            r#"{"username":"u","password":"p"}"#,
        )
    }
    fn pubsub() -> Arc<Target> {
        target(
            "pubsub",
            "gc-pubsub://lwf-test/orders?endpoint=http://127.0.0.1:34563",
            "{}",
        )
    }
    fn mysql() -> Arc<Target> {
        target(
            "mysql",
            "mysql://db.example.test:3306/app",
            r#"{"username":"u","password":"p"}"#,
        )
    }

    /// A template that renders nothing is neither a delivery nor a failure: the keys
    /// stay where they are. Every transport has to answer the same way, or a rule whose
    /// `{{#if}}` excludes a row would strike its keys to death.
    #[tokio::test]
    async fn nothing_rendered_sends_nothing() {
        for blank in ["", "   ", "\n\t "] {
            for t in [
                http(),
                sqs(),
                lambda(false),
                kafka(),
                redis(),
                smtp(),
                amqp(),
                pubsub(),
                mysql(),
            ] {
                let kind = t.def.kind.name();
                let sent = t
                    .send(0, blank)
                    .await
                    .unwrap_or_else(|e| panic!("{kind} on {blank:?}: {e:#}"));
                assert_eq!(sent, Sent::Skipped, "{kind} on {blank:?}");
            }
        }
    }

    /// A rendering the kind cannot take is refused (it would be refused again) rather
    /// than retried: a key whose template is broken must not hold the lane forever.
    #[tokio::test]
    async fn a_rendering_the_kind_cannot_take_is_refused() {
        let cases = [
            (http(), "not a request line at all\n\nx"),
            (kafka(), "POST / HTTP/1.1\n\nnot json"),
            (kafka(), "GET / HTTP/1.1\n\n{\"records\":[]}"),
            (redis(), "GET 'unbalanced"),
            (smtp(), "not a message"),
            (amqp(), "PUBLISH no-leading-slash\nMessage-Id: k\n\n{}"),
            (pubsub(), "POST /wrong/path HTTP/1.1\n\n{}"),
            (mysql(), "SELECT 1; SELECT 2"),
        ];
        for (t, rendered) in cases {
            let kind = t.def.kind.name();
            let e = t
                .send(0, rendered)
                .await
                .err()
                .unwrap_or_else(|| panic!("{kind} accepted {rendered:?}"));
            assert!(
                is_rejected(&e),
                "{kind} on {rendered:?}: {e:#} is not a refusal"
            );
        }
    }

    /// A `pubsub`, `amqp`, `smtp` or `kafka` target only appends: it cannot answer "which
    /// keys do you hold", so a stray listing against it is refused at once instead of
    /// passing for an empty target and clearing keys that were never delivered.
    #[tokio::test]
    async fn an_append_only_kind_refuses_to_list() {
        let body = Uses {
            body: true,
            ..Uses::default()
        };
        for (t, why) in [
            (amqp(), "cannot list keys"),
            (smtp(), "cannot list keys"),
            (kafka(), "cannot list keys"),
            (pubsub(), "only publish"),
        ] {
            let kind = t.def.kind.name();
            let e = format!("{:#}", t.fetch(0, "GET /", body).await.err().unwrap());
            assert!(e.contains(why), "{kind}: {e}");
        }
        // A SQL or redis listing that renders nothing is refused too: an empty
        // listing would be read as "the target holds nothing".
        let e = format!("{:#}", redis().fetch(0, "  ", body).await.err().unwrap());
        assert!(e.contains("rendered nothing"), "{e}");
        let e = format!("{:#}", mysql().fetch(0, "  ", body).await.err().unwrap());
        assert!(e.contains("rendered nothing"), "{e}");
    }

    /// An endpoint index that does not exist is an error of its own: firing at
    /// "endpoint 9" of a one-endpoint target must not be read as "nothing to send",
    /// which is what a confirmed flush looks like.
    #[tokio::test]
    async fn an_endpoint_that_is_gone_is_not_a_skip() {
        let keys = vec![String::from("1")];
        for t in [
            http(),
            sqs(),
            lambda(false),
            kafka(),
            redis(),
            pubsub(),
            mysql(),
        ] {
            let kind = t.def.kind.name();
            let e = format!(
                "{:#}",
                t.send(9, "POST / HTTP/1.1\n\n{}").await.err().unwrap()
            );
            assert!(
                e.contains("endpoint 9") && e.contains("is gone"),
                "{kind}: {e}"
            );
            let e = format!(
                "{:#}",
                t.send_items(9, "POST / HTTP/1.1\n\n{}", &keys)
                    .await
                    .err()
                    .unwrap()
            );
            assert!(
                e.contains("endpoint 9") && e.contains("is gone"),
                "{kind}: {e}"
            );
            let e = format!(
                "{:#}",
                t.fetch(9, "GET /", Uses::default()).await.err().unwrap()
            );
            assert!(
                e.contains("endpoint 9") && e.contains("is gone"),
                "{kind}: {e}"
            );
        }
    }

    /// The largest action this target takes: the kind's own limit, the broker's message
    /// limit raised by `vars.message_max_bytes`, or the Lambda payload limit of the
    /// invocation. Cutting at the wrong size means the service refuses the whole batch,
    /// every key of it at once.
    #[test]
    fn the_size_a_batch_is_cut_to() {
        assert_eq!(http().batch_bytes_limit(), None);
        assert_eq!(sqs().batch_bytes_limit(), Some(262_144));
        assert_eq!(
            kafka().batch_bytes_limit(),
            Some(crate::kafka::MESSAGE_MAX_BYTES)
        );
        assert_eq!(
            target(
                "kafka",
                "kafka://mq.example.test:9094/orders",
                r#"{"message_max_bytes": 8388608}"#
            )
            .batch_bytes_limit(),
            Some(8_388_608),
            "a broker with a raised message.max.bytes takes bigger batches"
        );
        // A Lambda: an event invocation takes a sixth of what a synchronous one does.
        let sync = lambda(false);
        let event = lambda(true);
        assert_eq!(
            sync.batch_bytes_limit(),
            Some(crate::aws::LAMBDA_SYNC_PAYLOAD)
        );
        assert_eq!(
            event.batch_bytes_limit(),
            Some(crate::aws::LAMBDA_EVENT_PAYLOAD)
        );
        assert!(sync.batch_bytes_limit() > event.batch_bytes_limit());
    }

    /// A throttled AWS answer comes as HTTP 400 with a code that says "not now". Read
    /// as a refusal, every key of the batch would take a strike and be dropped while
    /// the account is merely over its rate.
    #[test]
    fn an_aws_throttle_is_not_a_refusal() {
        let with_code = |code: &str| {
            (
                vec![("x-amzn-errortype".to_string(), code.to_string())],
                br#"{"message":"Rate exceeded"}"#.to_vec(),
            )
        };
        let (headers, body) = with_code("ThrottlingException");
        assert!(sqs().aws_throttled(&headers, &body));
        // The code may come in the body instead, and with the service's prefix.
        assert!(sqs().aws_throttled(
            &[],
            br#"{"__type":"com.amazon.coral.service#ServiceUnavailable"}"#
        ));
        // The same answer from a kind that is not AWS has no code to trust.
        let (headers, body) = with_code("ThrottlingException");
        assert!(
            !http().aws_throttled(&headers, &body),
            "an http target's 400 is about this request"
        );
        // A code that does not say "not now" stays a refusal of this request.
        let (headers, body) = with_code("InvalidMessageDigest");
        assert!(!sqs().aws_throttled(&headers, &body));
        // No code at all: not a throttle.
        assert!(!sqs().aws_throttled(&[], b"not json"));
    }

    /// A Lambda payload over the limit of its invocation is refused before it goes out:
    /// the service answers the same way every time, so retrying would only spend the
    /// key's strikes.
    #[tokio::test]
    async fn a_payload_an_invoke_cannot_take_is_refused_before_it_goes_out() {
        let event = lambda(true);
        let over = "x".repeat(crate::aws::LAMBDA_EVENT_PAYLOAD + 1);
        let e = event
            .send(0, &over)
            .await
            .err()
            .unwrap_or_else(|| panic!("an oversized payload was accepted"));
        assert!(is_rejected(&e), "{e:#} is not a refusal");
        let msg = format!("{e:#}");
        assert!(
            msg.contains("over the") && msg.contains("bytes an Invoke takes"),
            "{msg}"
        );
        // A batch of items is refused the same way: the payload is the whole request.
        let e = event
            .send_items(0, &over, &[String::from("1"), String::from("2")])
            .await
            .err()
            .unwrap();
        assert!(is_rejected(&e), "{e:#} is not a refusal");
    }
}

#[cfg(test)]
mod secret_endpoint_tests {
    use super::*;
    use crate::model::{RawTarget, TargetDef};
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The secret part of a webhook URL. It must reach the wire and nowhere else.
    const MARKER: &str = "SECMARK-4c7e";
    const HOST: &str = "hooks.example.test";

    fn raw_target(kind: &str, endpoints: &str, vars: &str) -> RawTarget {
        RawTarget {
            name: "t".into(),
            kind: Some(kind.into()),
            endpoints: Some(endpoints.into()),
            vars: Some(vars.into()),
            ..Default::default()
        }
    }

    fn load(kind: &str, endpoint: &str, vars: &str, allowed: &str) -> Result<Arc<Target>> {
        let def = TargetDef::from_row(&raw_target(kind, endpoint, vars))?;
        let list = HostList::parse(allowed)?;
        let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
        Target::new(def, pool, list.as_ref())
    }

    /// The whole error of a load that should have failed.
    fn load_err(kind: &str, endpoint: &str, vars: &str, allowed: &str) -> anyhow::Error {
        match load(kind, endpoint, vars, allowed) {
            Ok(_) => panic!("load should have failed"),
            Err(e) => e,
        }
    }

    /// Where a secret file of these tests lives.
    fn secret_dir() -> std::path::PathBuf {
        std::env::current_dir()
            .unwrap()
            .join("tmp/out/secret-endpoints")
    }

    /// One http server on loopback: it keeps the head of every request it is sent and answers
    /// `status` with `body` (and `Connection: close`, so no connection is pooled).
    async fn serve(status: u16, body: &str) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let got: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (status, body, sink) = (status, body.to_string(), got.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let (body, sink) = (body.clone(), sink.clone());
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => head.extend_from_slice(&chunk[..n]),
                        }
                    }
                    // Read the body the client is still writing: answering while it has more to
                    // send makes the client's write fail, and the test would see a connection
                    // error where it wanted the status code.
                    if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                        let text = String::from_utf8_lossy(&head[..end]).into_owned();
                        let want = text
                            .lines()
                            .find_map(|l| {
                                let (n, v) = l.split_once(':')?;
                                if n.trim().eq_ignore_ascii_case("content-length") {
                                    v.trim().parse().ok()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0usize);
                        let mut have = head.len() - (end + 4);
                        while have < want {
                            match s.read(&mut chunk).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => have += n,
                            }
                        }
                    }
                    sink.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&head).into_owned());
                    let reply = format!(
                        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(reply.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        (addr, got)
    }

    /// An address nothing listens on.
    async fn closed_port() -> SocketAddr {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    }

    /// The whole message of an error: the chain, as the log and `last_error` show it.
    fn chain(e: &anyhow::Error) -> String {
        format!("{e:#}")
    }

    #[tokio::test]
    async fn a_reference_endpoint_loads_and_shows_only_its_origin() {
        let _env = crate::secrets::ENV_LOCK.lock().await;
        let dir = secret_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("teams"),
            format!("https://{HOST}/hooks/b4b0f4/MARKER-ignored\n"),
        )
        .unwrap();
        std::fs::write(dir.join("wrong"), "not a url\n").unwrap();
        std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("https://{HOST}/services/T0000/B0000/{MARKER}"),
        );

        for endpoint in [
            "env:LWF_UNIT_WEBHOOK_URL".to_string(),
            format!("file:{}/teams", dir.display()),
        ] {
            let t = load("http", &endpoint, "{}", "").unwrap();
            let ctx = &t.endpoints[0].ctx;
            // The origin, and nothing below it.
            assert_eq!(ctx["host"], serde_json::json!(HOST));
            assert_eq!(ctx["port"], serde_json::json!(443));
            assert_eq!(ctx["url"], serde_json::json!(format!("https://{HOST}/…")));
            assert!(
                !serde_json::to_string(ctx).unwrap().contains(MARKER),
                "the template context holds the secret: {ctx}"
            );
            assert!(
                !serde_json::to_string(&t.ctx).unwrap().contains(MARKER),
                "the target context holds the secret"
            );
        }

        // The reference is what a `{:?}` shows, never the value it names.
        let def =
            TargetDef::from_row(&raw_target("http", "env:LWF_UNIT_WEBHOOK_URL", "{}")).unwrap();
        let shown = format!("{def:?}");
        assert!(shown.contains("env:LWF_UNIT_WEBHOOK_URL"), "{shown}");
        assert!(!shown.contains(MARKER), "{shown}");

        // A value that is not a plain http URL, or carries userinfo, is a load error that names
        // the reference and not the secret.
        std::env::set_var("LWF_UNIT_WEBHOOK_BAD", format!("ftp://{HOST}/x/{MARKER}"));
        let e = load_err("http", "env:LWF_UNIT_WEBHOOK_BAD", "{}", "");
        let msg = chain(&e);
        assert!(msg.contains("endpoint `env:LWF_UNIT_WEBHOOK_BAD`"), "{msg}");
        assert!(msg.contains("full http(s) URL"), "{msg}");
        assert!(!msg.contains(MARKER), "{msg}");
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_PW",
            format!("https://u:{MARKER}@{HOST}/x"),
        );
        let e = load_err("http", "env:LWF_UNIT_WEBHOOK_PW", "{}", "");
        let msg = chain(&e);
        // One fixed message for anything `BaseUrl::parse` refuses: its own words would
        // quote the authority it was given, which can be a piece of the secret.
        assert!(msg.contains("not valid"), "{msg}");
        assert!(!msg.contains(MARKER), "{msg}");
        let e = load_err("http", &format!("file:{}/wrong", dir.display()), "{}", "");
        assert!(!chain(&e).contains("not a url"), "{e:#}");
        // A reference that names nothing at all is a load error too.
        let e = load_err("http", "env:LWF_UNIT_WEBHOOK_MISSING", "{}", "");
        assert!(chain(&e).contains("LWF_UNIT_WEBHOOK_MISSING"), "{e:#}");
    }

    #[tokio::test]
    async fn the_secret_path_reaches_the_wire_and_no_message() {
        let _env = crate::secrets::ENV_LOCK.lock().await;
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("https://{HOST}/services/T0000/B0000/{MARKER}"),
        );
        let t = load("http", "env:LWF_UNIT_WEBHOOK_URL", "{}", "").unwrap();
        let rendered =
            "POST / HTTP/1.1\nContent-Type: application/json\n\n{\"text\":\"out of stock\"}";

        // Every message one of these requests can produce.
        let mut messages: Vec<String> = Vec::new();
        // A connection that is refused (transient), and an answer that is not a 2xx (refused).
        let closed = closed_port().await;
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("http://{closed}/services/T0000/B0000/{MARKER}"),
        );
        let e = t.send(0, rendered).await.unwrap_err();
        assert!(!is_rejected(&e), "a refused connection is transient");
        messages.push(chain(&e));
        let (addr, got) = serve(500, "the service is unwell").await;
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("http://{addr}/services/T0000/B0000/{MARKER}"),
        );
        let e = t.send(0, rendered).await.unwrap_err();
        assert!(!is_rejected(&e), "a 500 says not now, so it is transient");
        messages.push(chain(&e));
        // A refusal that is about this request: the webhook was revoked, say, or the payload is
        // not allowed. It is refused for good, and the message still stops at the origin.
        let (denied, _) = serve(403, "the webhook is no longer active").await;
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("http://{denied}/services/T0000/B0000/{MARKER}"),
        );
        let e = t.send(0, rendered).await.unwrap_err();
        assert!(is_rejected(&e), "a 403 is a refusal");
        messages.push(chain(&e));
        // A listing whose answer is not a 2xx.
        let e = t
            .fetch(0, "GET / HTTP/1.1\n\n", Uses::default())
            .await
            .unwrap_err();
        messages.push(chain(&e));
        // The endpoint is gone, and a reference that names nothing.
        messages.push(chain(&t.send(9, rendered).await.unwrap_err()));
        std::env::set_var("LWF_UNIT_WEBHOOK_URL", "");
        messages.push(chain(&t.send(0, rendered).await.unwrap_err()));

        for m in &messages {
            assert!(!m.contains(MARKER), "the secret is in a message: {m}");
        }
        // The origin is in them, so an operator can still tell which endpoint failed.
        assert!(
            messages[1].contains(&format!("http://{addr}/…")),
            "{}",
            messages[1]
        );
        assert!(messages[1].contains("POST"), "{}", messages[1]);
        assert!(messages[1].contains("status 500"), "{}", messages[1]);
        // And the request the wire actually carried holds the path, once, exactly.
        let sent = got.lock().unwrap().remove(0);
        assert!(
            sent.starts_with(&format!("POST /services/T0000/B0000/{MARKER} HTTP/1.1\r\n")),
            "{sent}"
        );
    }

    #[tokio::test]
    async fn the_endpoint_s_path_and_query_are_the_request_line() {
        let _env = crate::secrets::ENV_LOCK.lock().await;
        let (addr, got) = serve(200, "ok").await;

        // The webhook: the request line says `/`, the URL says where that is.
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("http://{addr}/services/T0000/B0000/{MARKER}"),
        );
        let t = load("http", "env:LWF_UNIT_WEBHOOK_URL", "{}", "").unwrap();
        t.send(0, "POST / HTTP/1.1\n\nhello").await.unwrap();
        // A query in the endpoint's URL, and a path below it.
        std::env::set_var(
            "LWF_UNIT_WEBHOOK_URL",
            format!("http://{addr}/v1/spaces/AAA?key={MARKER}&token=BBB"),
        );
        t.send(0, "POST / HTTP/1.1\n\nhello").await.unwrap();
        t.send(0, "GET /media/1.png HTTP/1.1\n\n").await.unwrap();
        // A request line that asks for no path stays what it was.
        std::env::set_var("LWF_UNIT_WEBHOOK_URL", format!("http://{addr}"));
        t.send(0, "OPTIONS * HTTP/1.1\n\n").await.unwrap();

        let lines: Vec<String> = got
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.lines().next().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            lines,
            vec![
                format!("POST /services/T0000/B0000/{MARKER} HTTP/1.1"),
                format!("POST /v1/spaces/AAA?key={MARKER}&token=BBB HTTP/1.1"),
                format!("GET /v1/spaces/AAA/media/1.png?key={MARKER}&token=BBB HTTP/1.1"),
                "OPTIONS * HTTP/1.1".to_string(),
            ],
            "{lines:#?}"
        );
        // `Host` comes from the resolved URL, and the body went with the request.
        let first = got.lock().unwrap()[0].clone();
        assert!(first.contains(&format!("Host: {addr}\r\n")), "{first}");
        assert!(first.ends_with("\r\n\r\nhello"), "{first}");

        // A plain endpoint is still sent exactly as it was before: the base path in front of
        // the rendered target, and `POST /` on a base of `/base/` keeps its trailing slash.
        let (addr, got) = serve(200, "ok").await;
        let t = load("http", &format!("http://{addr}/base/"), "{}", "").unwrap();
        t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap();
        t.send(0, "POST /idx/_bulk HTTP/1.1\n\ny").await.unwrap();
        let lines: Vec<String> = got
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.lines().next().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            lines,
            vec![
                "POST /base/ HTTP/1.1".to_string(),
                "POST /base/idx/_bulk HTTP/1.1".to_string(),
            ],
            "{lines:#?}"
        );
    }

    #[tokio::test]
    async fn the_allow_list_checks_the_resolved_host() {
        let _env = crate::secrets::ENV_LOCK.lock().await;
        let dir = secret_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
        let file = format!("file:{}/rotate", dir.display());

        // An endpoint whose host is not allowed never loads.
        std::fs::write(dir.join("rotate"), "https://hooks.example.test/a/b\n").unwrap();
        let e = load_err("http", &file, "{}", "hooks.other.test");
        let msg = chain(&e);
        assert!(msg.contains("LAZYWF_ALLOWED_HOSTS"), "{msg}");
        assert!(msg.contains("hooks.example.test"), "{msg}");
        assert!(load("http", &file, "{}", "hooks.example.test").is_ok());

        // A file rotated to another host is caught on the request, refused rather than waited
        // out, and named by its host and the setting — not by the URL.
        let t = load("http", &file, "{}", "hooks.example.test").unwrap();
        std::fs::write(
            dir.join("rotate"),
            format!("https://evil.example.test/a/{MARKER}\n"),
        )
        .unwrap();
        let e = t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap_err();
        assert!(
            is_rejected(&e),
            "a host the operator did not allow is not retried"
        );
        let msg = chain(&e);
        assert!(msg.contains("LAZYWF_ALLOWED_HOSTS"), "{msg}");
        assert!(msg.contains("evil.example.test"), "{msg}");
        assert!(!msg.contains(MARKER), "{msg}");
    }

    #[tokio::test]
    async fn a_reference_endpoint_is_for_the_http_kinds_only() {
        for kind in ["http", "http.basic", "http.bearer", "http.header"] {
            let vars = match kind {
                "http.basic" => r#"{"username":"u","password":"p"}"#,
                "http.bearer" => r#"{"token":"t"}"#,
                "http.header" => r#"{"header":"X-Key","value":"v"}"#,
                _ => "{}",
            };
            assert!(
                TargetDef::from_row(&raw_target(kind, "env:LWF_UNIT_ANY", vars)).is_ok(),
                "{kind}"
            );
        }
        let err = |kind: &str, vars: &str| {
            TargetDef::from_row(&raw_target(kind, "env:LWF_UNIT_ANY", vars))
                .unwrap_err()
                .to_string()
        };
        for (kind, vars) in [
            ("http.s3", r#"{"bucket":"b","region":"eu-west-1"}"#),
            ("redis", "{}"),
            ("sqs", "{}"),
            ("kinesis", "{}"),
            ("lambda", "{}"),
            ("eventbridge", "{}"),
            ("sns", "{}"),
            ("amqp", r#"{"username":"u","password":"p"}"#),
            ("smtp", r#"{"username":"u","password":"p"}"#),
            ("pubsub", r#"{"topic":"t"}"#),
            ("kafka", "{}"),
            ("mysql", r#"{"username":"u","password":"p"}"#),
            ("pgsql", r#"{"username":"u","password":"p"}"#),
        ] {
            let e = err(kind, vars);
            assert!(
                e.contains("endpoint reference") && e.contains("env:LWF_UNIT_ANY"),
                "{kind}: {e}"
            );
        }
    }
}
