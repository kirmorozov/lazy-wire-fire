//! The tool's tables and the definitions read from them.
//!
//! * `lazy_wire_fire_target`: where actions go;
//! * `lazy_wire_fire_rule`: what one kind of entity fires;
//! * `lazy_wire_fire_trigger`: which row changes mark which keys of a rule;
//! * `lazy_wire_fire_status`: written by the tool (see `status.rs`).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row};
use serde_json::{Map, Value};

use crate::aws;
use crate::sql;
use crate::tls::TlsSpec;

pub const TARGET: &str = "lazy_wire_fire_target";
pub const RULE: &str = "lazy_wire_fire_rule";
pub const TRIGGER: &str = "lazy_wire_fire_trigger";
pub const STATUS: &str = "lazy_wire_fire_status";
/// The commit fence's table (see `fence.rs`), created by `fence::probe` on the first flush.
pub const FENCE: &str = "lazy_wire_fire_fence";

/// The tables the vendored view planner refuses as view sources (`analyze::set_reserved` at
/// startup): the exact names above and their shared prefix.
pub const RESERVED_TABLES: &[&str] = &[TARGET, RULE, TRIGGER, STATUS, FENCE];
pub const RESERVED_PREFIX: &str = "lazy_wire_fire_";

pub const DDL: [&str; 4] = [
    "CREATE TABLE IF NOT EXISTS `lazy_wire_fire_target` (
  `name` VARCHAR(191) NOT NULL,
  `kind` VARCHAR(32) NOT NULL DEFAULT 'http',
  `endpoints` TEXT NULL,
  `tls` TEXT NULL,
  `timeout_ms` INT UNSIGNED NOT NULL DEFAULT 10000,
  `expect` VARCHAR(1024) NULL,
  `vars` TEXT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
    "CREATE TABLE IF NOT EXISTS `lazy_wire_fire_rule` (
  `name` VARCHAR(191) NOT NULL,
  `enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `target` VARCHAR(191) NOT NULL,
  `selects` MEDIUMTEXT NULL,
  `snapshot_keys_sql` MEDIUMTEXT NULL,
  `template` MEDIUMTEXT NOT NULL,
  `fire_per` VARCHAR(8) NOT NULL DEFAULT 'batch',
  `batch_max_keys` INT UNSIGNED NULL,
  `batch_max_bytes` INT UNSIGNED NULL,
  `debounce_ms` INT UNSIGNED NOT NULL DEFAULT 1000,
  `max_delay_ms` INT UNSIGNED NOT NULL DEFAULT 5000,
  `reset_steps` MEDIUMTEXT NULL,
  `reset_requested` TINYINT(1) NOT NULL DEFAULT 0,
  `vars` TEXT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
    "CREATE TABLE IF NOT EXISTS `lazy_wire_fire_trigger` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `rule` VARCHAR(191) NOT NULL,
  `enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `source_table` VARCHAR(255) NOT NULL,
  `on` VARCHAR(64) NOT NULL DEFAULT 'insert,update,delete',
  `changed_columns` TEXT NULL,
  `key` TEXT NULL,
  `keys_sql` MEDIUMTEXT NULL,
  PRIMARY KEY (`id`),
  KEY `rule` (`rule`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
    "CREATE TABLE IF NOT EXISTS `lazy_wire_fire_status` (
  `instance` VARCHAR(64) NOT NULL,
  `rule` VARCHAR(191) NOT NULL,
  `binlog_file` VARCHAR(255) NOT NULL DEFAULT '',
  `binlog_pos` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `lag_ms` BIGINT UNSIGNED NULL,
  `reset_state` VARCHAR(255) NOT NULL DEFAULT '',
  `last_error` TEXT NULL,
  `last_error_at` DATETIME(3) NULL,
  `server_identity` VARCHAR(255) NULL,
  `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`instance`, `rule`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
];

/// Creates the tables that are missing. Without the CREATE privilege, tables created by
/// someone else are fine.
pub async fn ensure(conn: &mut Conn) -> Result<()> {
    for ddl in DDL {
        let name = ddl.split('`').nth(1).unwrap_or_default();
        if let Err(e) = conn.query_drop(ddl).await {
            let exists: Option<String> = conn
                .exec_first("SHOW TABLES LIKE ?", (name,))
                .await
                .unwrap_or(None);
            if exists.is_none() {
                return Err(anyhow::Error::new(e).context(format!("creating {name}")));
            }
        }
    }
    Ok(())
}

pub fn is_config_table(table: &str) -> bool {
    matches!(table, TARGET | RULE | TRIGGER)
}

pub fn is_own_table(table: &str) -> bool {
    is_config_table(table) || table == STATUS || table == FENCE
}

// ---------------------------------------------------------------------------------------------
// Targets

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Http,
    HttpBasic,
    HttpBearer,
    HttpHeader,
    HttpS3,
    /// Amazon SQS `SendMessageBatch` (one request per action, an answer per entry).
    Sqs,
    /// Amazon Kinesis `PutRecords` (one request per action, an answer per record).
    Kinesis,
    /// AWS Lambda `Invoke` (one invocation per action; `batchItemFailures` per item).
    Lambda,
    /// Amazon EventBridge `PutEvents` (one request per action, an answer per entry).
    EventBridge,
    /// Amazon SNS `PublishBatch` (one request per action, an answer per entry; see `sns.rs`).
    Sns,
    /// RabbitMQ: one publish with a confirm per action (see `amqp.rs`).
    Amqp,
    /// SMTP: one mail per action (see `smtp.rs`).
    Smtp,
    /// Google Cloud Pub/Sub `topics.publish` (one request per action, a whole-action answer).
    PubSub,
    /// Kafka: one produce of a batch of records per action, an answer per partition (see
    /// `kafka.rs`).
    Kafka,
    /// RabbitMQ Streams: one publish batch per action to a stream or a super stream over the
    /// stream protocol, an answer per record (see `rabbitmq_stream.rs`).
    RabbitMqStream,
    /// gRPC: the rule's documents as protobuf messages, pushed onto one long-lived
    /// bidirectional stream per endpoint and acknowledged item by item (see `grpc/mod.rs`).
    Grpc,
    /// Azure Service Bus: one batch of messages per action to a queue or topic, a
    /// disposition per message (see `servicebus.rs`).
    ServiceBus,
    Redis,
    MySql,
    PgSql,
}

impl Kind {
    pub fn parse(s: &str) -> Result<Kind> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "http" | "https" | "" => Kind::Http,
            "http.basic" => Kind::HttpBasic,
            "http.bearer" => Kind::HttpBearer,
            "http.header" => Kind::HttpHeader,
            "http.s3" => Kind::HttpS3,
            "sqs" => Kind::Sqs,
            "kinesis" => Kind::Kinesis,
            "lambda" => Kind::Lambda,
            "eventbridge" => Kind::EventBridge,
            "sns" => Kind::Sns,
            "amqp" => Kind::Amqp,
            "smtp" => Kind::Smtp,
            "pubsub" => Kind::PubSub,
            "kafka" => Kind::Kafka,
            "rabbitmq-stream" => Kind::RabbitMqStream,
            "grpc" => Kind::Grpc,
            "servicebus" => Kind::ServiceBus,
            "redis" => Kind::Redis,
            "mysql" | "mariadb" => Kind::MySql,
            "pgsql" | "postgres" | "postgresql" => Kind::PgSql,
            other => bail!(
                "kind `{other}` (use http, http.basic, http.bearer, http.header, http.s3, sqs, kinesis, lambda, eventbridge, sns, amqp, smtp, pubsub, kafka, rabbitmq-stream, grpc, servicebus, redis, mysql or pgsql)"
            ),
        })
    }

    pub fn is_http(self) -> bool {
        !matches!(
            self,
            Kind::Amqp
                | Kind::Smtp
                | Kind::Redis
                | Kind::MySql
                | Kind::PgSql
                | Kind::Kafka
                | Kind::RabbitMqStream
                | Kind::Grpc
                | Kind::ServiceBus
        )
    }

    /// The name of the kind, as written in `kind`.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Http => "http",
            Kind::HttpBasic => "http.basic",
            Kind::HttpBearer => "http.bearer",
            Kind::HttpHeader => "http.header",
            Kind::HttpS3 => "http.s3",
            Kind::Sqs => "sqs",
            Kind::Kinesis => "kinesis",
            Kind::Lambda => "lambda",
            Kind::EventBridge => "eventbridge",
            Kind::Sns => "sns",
            Kind::Amqp => "amqp",
            Kind::Smtp => "smtp",
            Kind::PubSub => "pubsub",
            Kind::Kafka => "kafka",
            Kind::RabbitMqStream => "rabbitmq-stream",
            Kind::Grpc => "grpc",
            Kind::ServiceBus => "servicebus",
            Kind::Redis => "redis",
            Kind::MySql => "mysql",
            Kind::PgSql => "pgsql",
        }
    }

    /// Kinds whose answer says, per item of a batch, which items went through (see
    /// `Target::send_items`). The others answer per action.
    pub fn reports_items(self) -> bool {
        matches!(
            self,
            Kind::Sqs
                | Kind::Kinesis
                | Kind::Lambda
                | Kind::Kafka
                | Kind::RabbitMqStream
                | Kind::Grpc
                | Kind::EventBridge
                | Kind::Sns
                | Kind::ServiceBus
        )
    }

    /// Kinds that can only append: a sent message cannot be recalled or deleted by key, so
    /// stray cleanup and purge-like reset actions are refused at rule load (decision D9 / D7).
    pub fn append_only(self) -> bool {
        matches!(
            self,
            Kind::Sqs
                | Kind::Kinesis
                | Kind::EventBridge
                | Kind::Sns
                | Kind::Amqp
                | Kind::Smtp
                | Kind::PubSub
                | Kind::Kafka
                | Kind::RabbitMqStream
                | Kind::Grpc
                | Kind::ServiceBus
        )
    }

    /// The largest request body the service takes, when the kind has a fixed one.
    pub fn batch_bytes_limit(self) -> Option<usize> {
        match self {
            Kind::Sqs => Some(262_144),
            // PutRecords: 5 MiB per request, partition keys included (the rendered request
            // holds the base64 `Data`, so the count includes the 4/3 inflation).
            Kind::Kinesis => Some(5_242_880),
            // topics.publish: 10 MB per request (base64 `data` is in the rendered text).
            Kind::PubSub => Some(10_000_000),
            // PutEvents: 1 MB is the limit for the sum of the entries; the rendered request
            // carries the JSON around them, so it is the text that is measured (aws::EVENTBRIDGE_MAX_BYTES).
            Kind::EventBridge => Some(crate::aws::EVENTBRIDGE_MAX_BYTES),
            // PublishBatch: 256 KiB is the default for one message and for the sum of the
            // batch; `vars.maximum_message_size` raises it to the topic's
            // `MaximumMessageSize` (see `Target::batch_bytes_limit`).
            Kind::Sns => Some(crate::aws::SNS_MAX_BYTES),
            // The broker's `message.max.bytes`, by default; `vars.message_max_bytes` raises it
            // (see `Target::batch_bytes_limit`).
            Kind::Kafka => Some(crate::targets::kafka::MESSAGE_MAX_BYTES),
            // One stream frame, 1 MiB (the crate sends a batch as a single frame; see
            // `rabbitmq_stream.rs`).
            Kind::RabbitMqStream => Some(crate::targets::rabbitmq_stream::MESSAGE_MAX_BYTES),
            // The server's own maximum message size, 4 MiB in the usual implementations;
            // `vars.max_message_bytes` raises it (see `Target::batch_bytes_limit`).
            Kind::Grpc => Some(crate::targets::grpc::MAX_MESSAGE_BYTES),
            // The entity's maximum message size, 256 KiB by default; `vars.max_message_size`
            // raises it to the entity's (see `servicebus.rs`).
            Kind::ServiceBus => Some(crate::targets::servicebus::MAX_MESSAGE_BYTES),
            _ => None,
        }
    }

    /// The SQL dialect templates quote for (`sql_quote`, `sql_ident`): PostgreSQL for `pgsql`,
    /// MySQL otherwise (also right for SQL sent over HTTP to MySQL-like engines).
    pub fn dialect(self) -> crate::sqlwire::Dialect {
        if self == Kind::PgSql {
            crate::sqlwire::Dialect::Postgres
        } else {
            crate::sqlwire::Dialect::MySql
        }
    }

    /// `vars` entries that hold credentials and never reach templates.
    pub fn secret_vars(self) -> &'static [&'static str] {
        match self {
            Kind::Http => &[
                "sign_secret_ref",
                "client_secret_ref",
                "credentials_ref",
                "signing_key_ref",
            ],
            Kind::HttpBasic => &[
                "password",
                "sign_secret_ref",
                "client_secret_ref",
                "credentials_ref",
                "signing_key_ref",
            ],
            Kind::HttpBearer => &[
                "token",
                "sign_secret_ref",
                "client_secret_ref",
                "credentials_ref",
                "signing_key_ref",
            ],
            Kind::HttpHeader => &[
                "value",
                "sign_secret_ref",
                "client_secret_ref",
                "credentials_ref",
                "signing_key_ref",
            ],
            Kind::HttpS3
            | Kind::Sqs
            | Kind::Kinesis
            | Kind::Lambda
            | Kind::EventBridge
            | Kind::Sns => &["access_key_ref", "secret_key_ref", "session_token_ref"],
            Kind::PubSub => &["credentials_ref"],
            Kind::Amqp
            | Kind::Smtp
            | Kind::Redis
            | Kind::MySql
            | Kind::PgSql
            | Kind::Kafka
            | Kind::RabbitMqStream => &["password"],
            // `token` is a reference, not a value, but a path in a template is one leak too far.
            Kind::Grpc => &["token", "descriptor_set"],
            Kind::ServiceBus => &["key", "password"],
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct TargetDef {
    pub name: String,
    pub kind: Kind,
    pub endpoints: Vec<String>,
    pub tls: TlsSpec,
    pub timeout: Duration,
    /// SQL condition over the response row (see [`crate::response`]); `None`: the default.
    pub expect: Option<String>,
    pub vars: Map<String, Value>,
}

/// Endpoints are shown without userinfo, and `vars` by key only: a literal credential
/// (a header value, a password) must not reach a `{:?}` of the definition.
impl std::fmt::Debug for TargetDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let endpoints: Vec<String> = self
            .endpoints
            .iter()
            .map(|e| crate::secrets::redact_url(e))
            .collect();
        let vars: Vec<&String> = self.vars.keys().collect();
        f.debug_struct("TargetDef")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("endpoints", &endpoints)
            .field("tls", &self.tls)
            .field("timeout", &self.timeout)
            .field("expect", &self.expect)
            .field("vars (keys)", &vars)
            .finish()
    }
}

/// A JSON object column (empty when NULL or blank).
fn json_object(field: &str, s: Option<&str>) -> Result<Map<String, Value>> {
    match s.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(Map::new()),
        Some(s) => {
            match serde_json::from_str(s).with_context(|| format!("`{field}` is not valid JSON"))? {
                Value::Object(m) => Ok(m),
                _ => bail!("`{field}` must be a JSON object"),
            }
        }
    }
}

/// A list column: a JSON array of strings, or items separated by commas, spaces or newlines.
pub fn list(s: Option<&str>) -> Result<Vec<String>> {
    let s = s.unwrap_or("").trim();
    if s.starts_with('[') {
        let v: Vec<Value> = serde_json::from_str(s).context("not a valid JSON array")?;
        return v
            .into_iter()
            .map(|x| match x {
                Value::String(s) => Ok(s.trim().to_string()),
                other => Ok(other.to_string()),
            })
            .filter(|x| x.as_ref().map_or(true, |s| !s.is_empty()))
            .collect();
    }
    Ok(s.split([',', ' ', '\n', '\r', '\t'])
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(str::to_string)
        .collect())
}

fn var_str<'a>(vars: &'a Map<String, Value>, k: &str) -> Option<&'a str> {
    vars.get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Keys that a kind no longer takes in `vars` (its region and stream come from the endpoint).
pub(crate) fn refuse_vars(
    vars: &Map<String, Value>,
    keys: &[&str],
    kind: Kind,
    hint: &str,
) -> Result<()> {
    for k in keys {
        if vars.contains_key(*k) {
            bail!("`vars.{k}` is not used by kind {}: {hint}", kind.name());
        }
    }
    Ok(())
}

impl TargetDef {
    pub fn from_row(r: &RawTarget) -> Result<TargetDef> {
        let kind = Kind::parse(r.kind.as_deref().unwrap_or("http"))?;
        let endpoints = list(r.endpoints.as_deref()).context("`endpoints`")?;
        let vars = json_object("vars", r.vars.as_deref())?;
        // An endpoint that is a reference (`env:NAME` / `file:/path`) is for the plain http kinds:
        // there the URL itself can be the credential — the incoming webhook of Slack, Microsoft
        // Teams Workflows, Google Chat or Discord. Every other kind takes its address in plain
        // text and keeps its secrets in `vars`, so a reference there would only hide the address.
        if let Some(e) = endpoints.iter().find(|e| crate::secrets::is_reference(e)) {
            if !matches!(
                kind,
                Kind::Http | Kind::HttpBasic | Kind::HttpBearer | Kind::HttpHeader
            ) {
                bail!(
                    "`{e}`: an endpoint reference (`env:NAME` or `file:/path`) is for kind http, \
                     http.basic, http.bearer and http.header; kind {} takes its address in plain \
                     text and its credentials in `vars`",
                    kind.name()
                );
            }
        }
        let need = |k: &str| -> Result<()> {
            if var_str(&vars, k).is_none() {
                bail!("kind {:?} needs `vars.{k}`", kind);
            }
            Ok(())
        };
        match kind {
            Kind::Http | Kind::HttpBasic | Kind::HttpBearer | Kind::HttpHeader => {
                crate::targets::http::check(kind, &need)?
            }
            Kind::HttpS3 => {
                need("bucket")?;
                need("region")?;
                aws::check_refs(&vars)?;
            }
            Kind::Kinesis => {
                // `aws-kinesis://[<region>/]<stream>`: the stream and the region are in the URL.
                match endpoints.as_slice() {
                    [url] => {
                        aws::KinesisUrl::parse(url)?;
                    }
                    _ => bail!(
                        "kind kinesis takes exactly one endpoint: aws-kinesis://[<region>/]<stream-name>"
                    ),
                }
                refuse_vars(&vars, &["region", "stream_name", "stream_arn"], kind, "the region and the stream go in the endpoint: aws-kinesis://[<region>/]<stream-name>")?;
                aws::check_refs(&vars)?;
            }
            Kind::Sqs => crate::targets::sqs::check(&endpoints, &vars, kind)?,
            Kind::Lambda => {
                // `aws-lambda://[<region>/]<function>[:<qualifier>][?invocation=…]`: the function,
                // the region and the invocation are in the URL.
                match endpoints.as_slice() {
                    [url] => {
                        aws::LambdaUrl::parse(url)?;
                    }
                    _ => bail!(
                        "kind lambda takes exactly one endpoint: aws-lambda://[<region>/]<function-name>[:<qualifier>]"
                    ),
                }
                refuse_vars(
                    &vars,
                    &["region"],
                    kind,
                    "the region goes in the endpoint: aws-lambda://<region>/<function-name>",
                )?;
                aws::check_refs(&vars)?;
            }
            Kind::EventBridge => {
                // `aws-eventbridge://[<region>/]<event-bus>` (name or ARN): the bus and the
                // region are in the URL.
                match endpoints.as_slice() {
                    [url] => {
                        aws::EventBridgeUrl::parse(url)?;
                    }
                    _ => bail!(
                        "kind eventbridge takes exactly one endpoint: aws-eventbridge://[<region>/]<event-bus-name-or-arn>"
                    ),
                }
                refuse_vars(&vars, &["region", "event_bus_name", "event_bus_arn"], kind, "the region and the event bus go in the endpoint: aws-eventbridge://[<region>/]<event-bus-name-or-arn>")?;
                aws::check_refs(&vars)?;
            }
            Kind::Sns => {
                // `aws-sns://[<region>/]<account-id>/<topic-name>`: account, topic and region are
                // in the URL; the ARN is built from them (as SqsUrl builds the queue URL).
                match endpoints.as_slice() {
                    [url] => {
                        aws::SnsUrl::parse(url)?;
                    }
                    _ => bail!(
                        "kind sns takes exactly one endpoint: aws-sns://[<region>/]<account-id>/<topic-name>"
                    ),
                }
                refuse_vars(&vars, &["region", "account_id", "topic_name", "topic_arn"], kind, "the region, the account and the topic go in the endpoint: aws-sns://[<region>/]<account-id>/<topic-name>")?;
                crate::sns::check_vars(&vars)?;
                aws::check_refs(&vars)?;
            }
            Kind::Amqp => crate::targets::amqp::check(&endpoints, &vars, r.tls.as_deref())?,
            Kind::Smtp => crate::targets::smtp::check(&endpoints, &vars, r.tls.as_deref())?,
            Kind::PubSub => crate::targets::pubsub::check(&vars, &endpoints)?,
            Kind::Kafka => crate::targets::kafka::check(&endpoints, &vars, r.tls.as_deref())?,
            Kind::RabbitMqStream => {
                crate::targets::rabbitmq_stream::check(&endpoints, &vars, r.tls.as_deref())?
            }
            Kind::Grpc => crate::targets::grpc::check(&endpoints, &vars, r.tls.as_deref())?,
            Kind::ServiceBus => {
                crate::targets::servicebus::check(&endpoints, &vars, r.tls.as_deref())?
            }
            Kind::Redis | Kind::MySql | Kind::PgSql => {}
        }
        // Webhook signing is an option of the plain http kinds; on the others the settings
        // would go unused (http.s3 signs SigV4, the queue kinds their own way).
        if matches!(
            kind,
            Kind::Http | Kind::HttpBasic | Kind::HttpBearer | Kind::HttpHeader
        ) {
            crate::sign::check(&vars)?;
        } else if let Some(k) = crate::sign::first_var(&vars) {
            bail!(
                "`vars.{k}` is not used by kind {} (signing is an option of http, http.basic, http.bearer and http.header)",
                kind.name()
            );
        }
        // An OAuth 2.0 access token is an option of the same plain http kinds; on the others
        // the settings would go unused (each queue kind authenticates in its own way).
        if matches!(
            kind,
            Kind::Http | Kind::HttpBasic | Kind::HttpBearer | Kind::HttpHeader
        ) {
            crate::oauth::check(kind, &vars)?;
        } else if let Some(k) = crate::oauth::first_var(&vars, kind) {
            bail!(
                "`vars.{k}` is not used by kind {} (vars.oauth is an option of http, http.basic, http.bearer and http.header)",
                kind.name()
            );
        }
        if !matches!(
            kind,
            Kind::Sqs | Kind::Kinesis | Kind::Lambda | Kind::EventBridge | Kind::Sns
        ) {
            if let Some(e) = endpoints
                .iter()
                .find(|e| crate::aws::scheme_of(e).is_some_and(|s| s.starts_with("aws-")))
            {
                bail!(
                    "`{e}` is an endpoint of kind sqs, kinesis, lambda, eventbridge or sns, not of kind {}",
                    kind.name()
                );
            }
        }
        crate::targets::pubsub::refuse_scheme(kind == Kind::PubSub, &endpoints)?;
        crate::targets::kafka::refuse_scheme(kind == Kind::Kafka, &endpoints)?;
        crate::targets::rabbitmq_stream::refuse_scheme(kind == Kind::RabbitMqStream, &endpoints)?;
        crate::targets::grpc::refuse_scheme(kind == Kind::Grpc, &endpoints)?;
        crate::targets::servicebus::refuse_scheme(kind == Kind::ServiceBus, &endpoints)?;
        if endpoints.is_empty() && !matches!(kind, Kind::HttpS3) {
            bail!("`endpoints` is empty");
        }
        let timeout = Duration::from_millis(match r.timeout_ms {
            Some(0) | None => 10_000,
            Some(n) => n,
        });
        Ok(TargetDef {
            name: r.name.clone(),
            kind,
            endpoints,
            tls: TlsSpec::parse(r.tls.as_deref()).context("`tls`")?,
            timeout,
            expect: match r.expect.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                Some(s) => {
                    crate::response::check(s).context("`expect`")?;
                    Some(s.to_string())
                }
                None => None,
            },
            vars,
        })
    }

    /// `vars` without credentials, as templates see them (`target.vars`).
    pub fn template_vars(&self) -> Value {
        let mut v = self.vars.clone();
        for k in self.kind.secret_vars() {
            v.remove(*k);
        }
        Value::Object(v)
    }
}

// ---------------------------------------------------------------------------------------------
// Rules

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirePer {
    Batch,
    Key,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectDef {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StepKind {
    Once,
    Snapshot,
    Stray {
        list_template: String,
        /// SQL expressions over the listing response (see [`crate::response`]).
        keys: String,
        next: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResetStep {
    pub name: String,
    pub kind: StepKind,
    /// The action template; `None` for `for_each` steps means the rule's template.
    pub template: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleDef {
    pub name: String,
    pub enabled: bool,
    pub target: String,
    pub selects: Vec<SelectDef>,
    pub snapshot_keys_sql: Option<String>,
    pub template: String,
    pub fire_per: FirePer,
    pub batch_max_keys: Option<usize>,
    pub batch_max_bytes: Option<usize>,
    pub debounce: Duration,
    pub max_delay: Duration,
    pub reset_steps: Vec<ResetStep>,
    pub reset_requested: bool,
    pub vars: Map<String, Value>,
}

/// `selects`: a JSON array (strings, or objects `{"name", "sql"}`), or SQL statements separated
/// by `;`.
pub fn parse_selects(s: Option<&str>) -> Result<Vec<SelectDef>> {
    let s = s.unwrap_or("").trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let named = |i: usize, name: Option<String>, sql: String| SelectDef {
        name: name.unwrap_or_else(|| format!("select {}", i + 1)),
        sql: sql.trim().trim_end_matches(';').trim().to_string(),
    };
    if s.starts_with('[') {
        let v: Vec<Value> =
            serde_json::from_str(s).context("`selects` is not a valid JSON array")?;
        return v
            .into_iter()
            .enumerate()
            .map(|(i, x)| match x {
                Value::String(sql) => Ok(named(i, None, sql)),
                Value::Object(o) => {
                    let sql = o
                        .get("sql")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("`selects[{i}]` has no `sql`"))?;
                    Ok(named(
                        i,
                        o.get("name").and_then(Value::as_str).map(str::to_string),
                        sql.to_string(),
                    ))
                }
                _ => bail!("`selects[{i}]` must be a string or an object with `sql`"),
            })
            .collect();
    }
    Ok(sql::split_statements(s)
        .into_iter()
        .enumerate()
        .map(|(i, q)| named(i, None, q))
        .collect())
}

pub fn parse_reset_steps(s: Option<&str>) -> Result<Vec<ResetStep>> {
    let s = s.unwrap_or("").trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let v: Vec<Value> =
        serde_json::from_str(s).context("`reset_steps` is not a valid JSON array")?;
    v.into_iter()
        .enumerate()
        .map(|(i, x)| {
            let o = x.as_object().ok_or_else(|| anyhow!("`reset_steps[{i}]` must be an object"))?;
            let get = |k: &str| o.get(k).and_then(Value::as_str).map(str::to_string);
            let kind = match (get("kind"), get("for_each"), o.get("once")) {
                (Some(k), _, _) => k,
                (None, Some(f), _) => f,
                (None, None, Some(v)) if v != &Value::Bool(false) && !v.is_null() => "once".into(),
                _ => bail!("`reset_steps[{i}]`: say `\"once\": true` or `\"for_each\": \"snapshot\"|\"stray\"`"),
            };
            let template = get("template").filter(|t| !t.trim().is_empty());
            let kind = match kind.to_ascii_lowercase().as_str() {
                "once" => {
                    if template.is_none() {
                        bail!("`reset_steps[{i}]` (once) needs a `template`");
                    }
                    StepKind::Once
                }
                "snapshot" => StepKind::Snapshot,
                "stray" => {
                    let expr = |name: &str| -> Result<Option<String>> {
                        match get(name).filter(|e| !e.trim().is_empty()) {
                            Some(e) => {
                                crate::response::check(&e)
                                    .with_context(|| format!("`reset_steps[{i}].{name}`"))?;
                                Ok(Some(e))
                            }
                            None => Ok(None),
                        }
                    };
                    StepKind::Stray {
                        list_template: get("list_template")
                            .ok_or_else(|| anyhow!("`reset_steps[{i}]` (stray) needs `list_template`"))?,
                        keys: expr("keys")?
                            .ok_or_else(|| anyhow!("`reset_steps[{i}]` (stray) needs `keys`"))?,
                        next: expr("next")?,
                    }
                }
                other => bail!("`reset_steps[{i}]`: unknown kind `{other}`"),
            };
            Ok(ResetStep { name: get("name").unwrap_or_else(|| format!("step {}", i + 1)), kind, template })
        })
        .collect()
}

impl RuleDef {
    pub fn from_row(r: &RawRule) -> Result<RuleDef> {
        let fire_per = match r
            .fire_per
            .as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("batch") => FirePer::Batch,
            Some("key") => FirePer::Key,
            Some(other) => bail!("`fire_per` `{other}` (use batch or key)"),
        };
        if r.template.as_deref().is_none_or(|t| t.trim().is_empty()) {
            bail!("`template` is empty");
        }
        let positive = |v: Option<u64>| v.filter(|n| *n > 0).map(|n| n as usize);
        let rule = RuleDef {
            name: r.name.clone(),
            enabled: r.enabled,
            target: r.target.clone().unwrap_or_default(),
            selects: parse_selects(r.selects.as_deref())?,
            snapshot_keys_sql: r.snapshot_keys_sql.clone().filter(|s| !s.trim().is_empty()),
            template: r.template.clone().unwrap_or_default(),
            fire_per,
            batch_max_keys: positive(r.batch_max_keys),
            batch_max_bytes: positive(r.batch_max_bytes),
            debounce: Duration::from_millis(r.debounce_ms.unwrap_or(1000)),
            max_delay: Duration::from_millis(r.max_delay_ms.unwrap_or(5000).max(1)),
            reset_steps: parse_reset_steps(r.reset_steps.as_deref())?,
            reset_requested: r.reset_requested,
            vars: json_object("vars", r.vars.as_deref())?,
        };
        if rule.target.is_empty() {
            bail!("`target` is empty");
        }
        if rule.reset_steps.iter().any(|s| s.kind != StepKind::Once)
            && rule.snapshot_keys_sql.is_none()
        {
            bail!("`for_each` reset steps need `snapshot_keys_sql`");
        }
        Ok(rule)
    }

    /// Stand-in for a row that does not parse: it fires nothing, but its triggers still mark
    /// keys, so none are lost while the row is being fixed.
    pub fn placeholder(r: &RawRule) -> RuleDef {
        RuleDef {
            name: r.name.clone(),
            enabled: r.enabled,
            target: r.target.clone().unwrap_or_default(),
            selects: Vec::new(),
            snapshot_keys_sql: None,
            template: String::new(),
            fire_per: FirePer::Batch,
            batch_max_keys: None,
            batch_max_bytes: None,
            debounce: Duration::from_millis(r.debounce_ms.unwrap_or(1000)),
            max_delay: Duration::from_millis(r.max_delay_ms.unwrap_or(5000).max(1)),
            reset_steps: Vec::new(),
            reset_requested: false,
            vars: Map::new(),
        }
    }

    /// Everything but the switches: two rules with the same fingerprint fire the same actions.
    pub fn fingerprint(&self) -> String {
        format!(
            "{}|{:?}|{:?}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{}",
            self.target,
            self.selects,
            self.snapshot_keys_sql,
            self.template,
            self.fire_per,
            self.batch_max_keys,
            self.batch_max_bytes,
            self.reset_steps,
            (self.debounce, self.max_delay),
            Value::Object(self.vars.clone())
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Triggers

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct On {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
}

impl On {
    pub fn parse(s: Option<&str>) -> Result<On> {
        let items = list(s)?;
        if items.is_empty() {
            return Ok(On {
                insert: true,
                update: true,
                delete: true,
            });
        }
        let mut on = On::default();
        for i in items {
            match i.to_ascii_lowercase().as_str() {
                "insert" => on.insert = true,
                "update" => on.update = true,
                "delete" => on.delete = true,
                "*" | "all" => {
                    on = On {
                        insert: true,
                        update: true,
                        delete: true,
                    }
                }
                other => bail!("`on`: unknown event `{other}` (use insert, update, delete)"),
            }
        }
        Ok(on)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggerDef {
    pub id: u64,
    pub rule: String,
    pub schema: String,
    pub table: String,
    pub on: On,
    pub changed_columns: Vec<String>,
    pub key: Option<String>,
    pub keys_sql: Option<String>,
}

impl TriggerDef {
    pub fn from_row(
        r: &RawTrigger,
        default_schema: &str,
        lower_case_names: bool,
    ) -> Result<TriggerDef> {
        let st = r.source_table.trim();
        let unq = |s: &str| s.trim().trim_matches('`').to_string();
        let (schema, table) = match st.split_once('.') {
            Some((s, t)) => (unq(s), unq(t)),
            None => (default_schema.to_string(), unq(st)),
        };
        if table.is_empty() {
            bail!("`source_table` is empty");
        }
        let (schema, table) = if lower_case_names {
            (schema.to_lowercase(), table.to_lowercase())
        } else {
            (schema, table)
        };
        let key = r.key.clone().filter(|k| !k.trim().is_empty());
        let keys_sql = r.keys_sql.clone().filter(|k| !k.trim().is_empty());
        if key.is_none() && keys_sql.is_none() {
            bail!("trigger {} needs `key` or `keys_sql`", r.id);
        }
        if let Some(q) = &keys_sql {
            for p in sql::placeholders(q) {
                let ok = p.name.split_once('.').is_some_and(|(img, col)| {
                    matches!(img, "old" | "new" | "row") && !col.is_empty()
                });
                if !ok {
                    bail!(
                        "`keys_sql`: `:{}` is not `:old.<column>` or `:new.<column>`",
                        p.name
                    );
                }
            }
        }
        Ok(TriggerDef {
            id: r.id,
            rule: r.rule.clone(),
            schema,
            table,
            on: On::parse(r.on.as_deref())?,
            changed_columns: list(r.changed_columns.as_deref())?,
            key,
            keys_sql,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Raw rows

#[derive(Debug, Clone, Default)]
pub struct RawTarget {
    pub name: String,
    pub kind: Option<String>,
    pub endpoints: Option<String>,
    pub tls: Option<String>,
    pub timeout_ms: Option<u64>,
    pub expect: Option<String>,
    pub vars: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RawRule {
    pub name: String,
    pub enabled: bool,
    pub target: Option<String>,
    pub selects: Option<String>,
    pub snapshot_keys_sql: Option<String>,
    pub template: Option<String>,
    pub fire_per: Option<String>,
    pub batch_max_keys: Option<u64>,
    pub batch_max_bytes: Option<u64>,
    pub debounce_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
    pub reset_steps: Option<String>,
    pub reset_requested: bool,
    pub vars: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RawTrigger {
    pub id: u64,
    pub rule: String,
    pub enabled: bool,
    pub source_table: String,
    pub on: Option<String>,
    pub changed_columns: Option<String>,
    pub key: Option<String>,
    pub keys_sql: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RawConfig {
    pub targets: Vec<RawTarget>,
    pub rules: Vec<RawRule>,
    pub triggers: Vec<RawTrigger>,
}

fn s(r: &Row, i: usize) -> Option<String> {
    r.get_opt::<Option<String>, _>(i)
        .and_then(|v| v.ok())
        .flatten()
}

fn n(r: &Row, i: usize) -> Option<u64> {
    r.get_opt::<Option<u64>, _>(i)
        .and_then(|v| v.ok())
        .flatten()
}

pub async fn load(conn: &mut Conn) -> Result<RawConfig> {
    let targets: Vec<Row> = conn
        .query(
            "SELECT CAST(`name` AS CHAR), CAST(`kind` AS CHAR), CAST(`endpoints` AS CHAR), CAST(`tls` AS CHAR), \
             `timeout_ms`, CAST(`expect` AS CHAR), CAST(`vars` AS CHAR) FROM `lazy_wire_fire_target` ORDER BY `name`",
        )
        .await
        .context("reading lazy_wire_fire_target")?;
    let rules: Vec<Row> = conn
        .query(
            "SELECT CAST(`name` AS CHAR), `enabled` <> 0, CAST(`target` AS CHAR), CAST(`selects` AS CHAR), \
             CAST(`snapshot_keys_sql` AS CHAR), CAST(`template` AS CHAR), CAST(`fire_per` AS CHAR), `batch_max_keys`, \
             `batch_max_bytes`, `debounce_ms`, `max_delay_ms`, CAST(`reset_steps` AS CHAR), `reset_requested` <> 0, \
             CAST(`vars` AS CHAR) FROM `lazy_wire_fire_rule` ORDER BY `name`",
        )
        .await
        .context("reading lazy_wire_fire_rule")?;
    let triggers: Vec<Row> = conn
        .query(
            "SELECT `id`, CAST(`rule` AS CHAR), `enabled` <> 0, CAST(`source_table` AS CHAR), CAST(`on` AS CHAR), \
             CAST(`changed_columns` AS CHAR), CAST(`key` AS CHAR), CAST(`keys_sql` AS CHAR) \
             FROM `lazy_wire_fire_trigger` ORDER BY `id`",
        )
        .await
        .context("reading lazy_wire_fire_trigger")?;
    Ok(RawConfig {
        targets: targets
            .iter()
            .map(|r| RawTarget {
                name: s(r, 0).unwrap_or_default(),
                kind: s(r, 1),
                endpoints: s(r, 2),
                tls: s(r, 3),
                timeout_ms: n(r, 4),
                expect: s(r, 5),
                vars: s(r, 6),
            })
            .collect(),
        rules: rules
            .iter()
            .map(|r| RawRule {
                name: s(r, 0).unwrap_or_default(),
                enabled: n(r, 1) == Some(1),
                target: s(r, 2),
                selects: s(r, 3),
                snapshot_keys_sql: s(r, 4),
                template: s(r, 5),
                fire_per: s(r, 6),
                batch_max_keys: n(r, 7),
                batch_max_bytes: n(r, 8),
                debounce_ms: n(r, 9),
                max_delay_ms: n(r, 10),
                reset_steps: s(r, 11),
                reset_requested: n(r, 12) == Some(1),
                vars: s(r, 13),
            })
            .collect(),
        triggers: triggers
            .iter()
            .map(|r| RawTrigger {
                id: n(r, 0).unwrap_or_default(),
                rule: s(r, 1).unwrap_or_default(),
                enabled: n(r, 2) == Some(1),
                source_table: s(r, 3).unwrap_or_default(),
                on: s(r, 4),
                changed_columns: s(r, 5),
                key: s(r, 6),
                keys_sql: s(r, 7),
            })
            .collect(),
    })
}

/// Triggers grouped by rule name.
pub fn triggers_by_rule(raw: &RawConfig) -> BTreeMap<String, Vec<RawTrigger>> {
    let mut m: BTreeMap<String, Vec<RawTrigger>> = BTreeMap::new();
    for t in raw.triggers.iter().filter(|t| t.enabled) {
        m.entry(t.rule.clone()).or_default().push(t.clone());
    }
    m
}

#[cfg(test)]
#[path = "tests/model.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/model_aws_endpoint_tests.rs"]
mod aws_endpoint_tests;

#[cfg(test)]
mod target_debug_tests {
    use super::*;

    #[test]
    fn debug_shows_var_keys_and_endpoints_without_userinfo() {
        let mut vars = Map::new();
        vars.insert("password".into(), Value::String("VAR-LITERAL-7".into()));
        let def = TargetDef {
            name: "t".into(),
            kind: Kind::HttpBasic,
            endpoints: vec!["http://u:URL-PW-8@h:1/base".into()],
            tls: crate::tls::TlsSpec::parse(None).unwrap(),
            timeout: Duration::from_secs(1),
            expect: None,
            vars,
        };
        let shown = format!("{def:?}");
        assert!(!shown.contains("VAR-LITERAL-7"), "{shown}");
        assert!(!shown.contains("URL-PW-8"), "{shown}");
        assert!(shown.contains("password"), "{shown}");
    }
}

#[cfg(test)]
#[path = "tests/model_load_refusals_tests.rs"]
mod load_refusals_tests;
