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
    /// The OAuth 2.0 access token of a plain http target that asks for one (`vars.oauth`,
    /// `src/oauth.rs`): every action of the target carries it as `Authorization: Bearer`.
    oauth: Option<crate::oauth::Provider>,
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
/// The operator's allow-list applied to one outbound host (also the token host of an oauth
/// target, see [`crate::oauth`]).
pub(crate) fn check_host(list: &HostList, host: &str, port: u16) -> Result<()> {
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
        // The token source of `vars.oauth`: `TargetDef::from_row` has refused the setting on
        // any other kind, and refused an unusable shape on these four.
        let oauth = crate::oauth::Provider::new(&def, allowed)?;
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
            oauth,
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
                // A 401 while `vars.oauth` is set speaks about the access token, not about this
                // message: whatever `expect` says, no key is refused for it. The cached token is
                // dropped, so the next attempt presents a new one.
                if let Some(o) = &self.oauth {
                    if resp.status == 401 {
                        o.invalidate();
                        return Err(anyhow!(
                            "{}: 401 with the oauth access token: it is dropped and a new one \
                             will be fetched; the endpoint said: {}",
                            h.at_target,
                            self.scrub(&resp.body_snippet())
                        ));
                    }
                }
                let verdict = self
                    .expect
                    .http(&self.pool, resp.status, &resp.headers, &resp.body)
                    .await
                    .with_context(|| h.at_target.clone())?;
                if let Err(e) = verdict {
                    let msg = self.scrub(&format!("{}: {e}: {}", h.at_target, resp.body_snippet()));
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
                // As in `send`: a 401 is the credential being refused, never the listing.
                if let Some(o) = &self.oauth {
                    if resp.status == 401 {
                        o.invalidate();
                        bail!(
                            "{}: 401 with the oauth access token: it is dropped and a new one \
                             will be fetched; the endpoint said: {}",
                            h.at_target,
                            self.scrub(&resp.body_snippet())
                        );
                    }
                }
                if !(200..300).contains(&resp.status) {
                    bail!(
                        "{}: status {}: {}",
                        h.at_target,
                        resp.status,
                        self.scrub(&resp.body_snippet())
                    );
                }
                if uses.body && serde_json::from_slice::<Value>(&resp.body).is_err() {
                    bail!(
                        "the listing response is not JSON: {}",
                        self.scrub(&resp.body_snippet())
                    );
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
    /// The text of an http error message, with the oauth access token masked out of it: an
    /// endpoint that echoes the request (or its own answer) cannot carry the token into a log
    /// line or `last_error`. Without `vars.oauth` there is no token and nothing to mask.
    fn scrub(&self, s: &str) -> String {
        match &self.oauth {
            Some(o) => o.scrub(s),
            None => s.to_string(),
        }
    }

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
        // The oauth token, after the kind's own credential so that a target can carry only one
        // claim (`TargetDef::from_row` refuses the two together). A request line that rendered
        // its own `Authorization:` is a second claim: refused, naming the setting and where the
        // other claim came from. The literal form is caught when the rule loads
        // (`crate::oauth::rule_errors`); this catches a header name rendering produced.
        if let Some(o) = &self.oauth {
            if req.header("authorization").is_some() {
                // A configuration mistake of the rule, not a word about the key: transient.
                // The rendered value is never quoted — it may be a resolved secret.
                return Err(anyhow!(
                    "`vars.oauth={}` sets the `Authorization` header, and the rule template \
                     rendered one too: keep one",
                    crate::oauth::grant_name(&self.def.vars)
                ));
            }
            req.set_header("Authorization", o.authorization().await?);
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
mod eventbridge_verdict_tests;

#[cfg(test)]
mod allow_tests;

#[cfg(test)]
mod dispatch_tests;

#[cfg(test)]
mod secret_endpoint_tests;

// ---------------------------------------------------------------------------------------------
// oauth: the plain http kinds carry `Authorization: Bearer <token>` (see `src/oauth.rs`).

#[cfg(test)]
mod oauth_tests;
