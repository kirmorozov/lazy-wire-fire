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
    /// RabbitMQ: one publish with a confirm per action (see `amqp.rs`).
    Amqp,
    /// Google Cloud Pub/Sub `topics.publish` (one request per action, a whole-action answer).
    PubSub,
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
            "amqp" => Kind::Amqp,
            "pubsub" => Kind::PubSub,
            "redis" => Kind::Redis,
            "mysql" | "mariadb" => Kind::MySql,
            "pgsql" | "postgres" | "postgresql" => Kind::PgSql,
            other => bail!(
                "kind `{other}` (use http, http.basic, http.bearer, http.header, http.s3, sqs, kinesis, lambda, amqp, pubsub, redis, mysql or pgsql)"
            ),
        })
    }

    pub fn is_http(self) -> bool {
        !matches!(self, Kind::Amqp | Kind::Redis | Kind::MySql | Kind::PgSql)
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
            Kind::Amqp => "amqp",
            Kind::PubSub => "pubsub",
            Kind::Redis => "redis",
            Kind::MySql => "mysql",
            Kind::PgSql => "pgsql",
        }
    }

    /// Kinds whose answer says, per item of a batch, which items went through (see
    /// `Target::send_items`). The others answer per action.
    pub fn reports_items(self) -> bool {
        matches!(self, Kind::Sqs | Kind::Kinesis | Kind::Lambda)
    }

    /// Kinds that can only append: a sent message cannot be recalled or deleted by key, so
    /// stray cleanup and purge-like reset actions are refused at rule load (decision D9 / D7).
    pub fn append_only(self) -> bool {
        matches!(self, Kind::Sqs | Kind::Kinesis | Kind::Amqp | Kind::PubSub)
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
            Kind::Http => &[],
            Kind::HttpBasic => &["password"],
            Kind::HttpBearer => &["token"],
            Kind::HttpHeader => &["value"],
            Kind::HttpS3 | Kind::Sqs | Kind::Kinesis | Kind::Lambda => {
                &["access_key_ref", "secret_key_ref", "session_token_ref"]
            }
            Kind::PubSub => &["credentials_ref"],
            Kind::Amqp | Kind::Redis | Kind::MySql | Kind::PgSql => &["password"],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
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
fn refuse_vars(vars: &Map<String, Value>, keys: &[&str], kind: Kind, hint: &str) -> Result<()> {
    for k in keys {
        if vars.contains_key(*k) {
            bail!("`vars.{k}` is not used by kind {}: {hint}", kind.name());
        }
    }
    Ok(())
}

/// AWS credentials in a target's `vars`: references only, and an access key with its secret.
fn aws_refs(vars: &Map<String, Value>) -> Result<()> {
    for k in ["access_key_ref", "secret_key_ref", "session_token_ref"] {
        if let Some(v) = var_str(vars, k) {
            if !crate::secrets::is_reference(v) {
                bail!(
                    "`vars.{k}` must be a reference (`env:NAME` or `file:/path`), never a literal"
                );
            }
        }
    }
    if var_str(vars, "access_key_ref").is_some() != var_str(vars, "secret_key_ref").is_some() {
        bail!("`vars.access_key_ref` and `vars.secret_key_ref` go together");
    }
    Ok(())
}

impl TargetDef {
    pub fn from_row(r: &RawTarget) -> Result<TargetDef> {
        let kind = Kind::parse(r.kind.as_deref().unwrap_or("http"))?;
        let endpoints = list(r.endpoints.as_deref()).context("`endpoints`")?;
        let vars = json_object("vars", r.vars.as_deref())?;
        let need = |k: &str| -> Result<()> {
            if var_str(&vars, k).is_none() {
                bail!("kind {:?} needs `vars.{k}`", kind);
            }
            Ok(())
        };
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
            Kind::HttpS3 => {
                need("bucket")?;
                need("region")?;
                aws_refs(&vars)?;
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
                aws_refs(&vars)?;
            }
            Kind::Sqs => {
                // `aws-sqs://[<region>/]<account>/<queue>`: the queue and the region are in the URL.
                match endpoints.as_slice() {
                    [url] => {
                        aws::SqsUrl::parse(url)?;
                    }
                    _ => bail!(
                        "kind sqs takes exactly one endpoint: aws-sqs://[<region>/]<account-id>/<queue-name>"
                    ),
                }
                refuse_vars(
                    &vars,
                    &["region"],
                    kind,
                    "the region goes in the endpoint: aws-sqs://<region>/<account-id>/<queue-name>",
                )?;
                aws_refs(&vars)?;
            }
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
                aws_refs(&vars)?;
            }
            Kind::Amqp => crate::amqp::check(&endpoints, &vars, r.tls.as_deref())?,
            Kind::PubSub => crate::pubsub::check(&vars, &endpoints)?,
            Kind::Http | Kind::Redis | Kind::MySql | Kind::PgSql => {}
        }
        if !matches!(kind, Kind::Sqs | Kind::Kinesis | Kind::Lambda) {
            if let Some(e) = endpoints
                .iter()
                .find(|e| crate::aws::scheme_of(e).is_some_and(|s| s.starts_with("aws-")))
            {
                bail!(
                    "`{e}` is an endpoint of kind sqs, kinesis or lambda, not of kind {}",
                    kind.name()
                );
            }
        }
        crate::pubsub::refuse_scheme(kind == Kind::PubSub, &endpoints)?;
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
mod tests {
    use super::*;

    #[test]
    fn selects_forms() {
        let s = parse_selects(Some("SELECT 1 AS _key; SELECT 'a;b' AS _key;")).unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s[1].sql, "SELECT 'a;b' AS _key");
        let s = parse_selects(Some(
            r#"["SELECT 1 AS _key", {"name": "prices", "sql": "SELECT 2 AS _key;"}]"#,
        ))
        .unwrap();
        assert_eq!(
            (s[0].name.as_str(), s[1].name.as_str(), s[1].sql.as_str()),
            ("select 1", "prices", "SELECT 2 AS _key")
        );
        assert!(parse_selects(None).unwrap().is_empty());
    }

    #[test]
    fn steps() {
        let s = parse_reset_steps(Some(
            r#"[{"once": true, "template": "FLUSHDB"},
                {"for_each": "snapshot"},
                {"for_each": "stray", "list_template": "GET /", "keys": "JSON_EXTRACT(body, '$.hits.hits[*]._id')",
                 "next": "JSON_EXTRACT(body, '$.hits.hits[last].sort')"}]"#,
        ))
        .unwrap();
        assert_eq!(s[0].kind, StepKind::Once);
        assert_eq!(s[1].kind, StepKind::Snapshot);
        assert!(matches!(
            &s[2].kind,
            StepKind::Stray { keys, next: Some(_), .. } if keys.starts_with("JSON_EXTRACT")
        ));
        assert!(parse_reset_steps(Some(r#"[{"once": true}]"#)).is_err());
        assert!(parse_reset_steps(Some(r#"[{"for_each": "stray"}]"#)).is_err());
        assert!(parse_reset_steps(Some(
            r#"[{"for_each": "stray", "list_template": "GET /", "keys": "JSON_EXTRACT(body, '$.ids'); DROP TABLE t"}]"#
        ))
        .is_err());
        assert!(parse_reset_steps(Some(r#"[{"template": "x"}]"#)).is_err());
    }

    #[test]
    fn targets() {
        let t = TargetDef::from_row(&RawTarget {
            name: "s3".into(),
            kind: Some("http.s3".into()),
            vars: Some(r#"{"bucket":"b","region":"r","access_key_ref":"env:A","secret_key_ref":"env:S","prefix":"p"}"#.into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            t.template_vars(),
            serde_json::json!({"bucket":"b","region":"r","prefix":"p"})
        );
        let lit = RawTarget {
            name: "s3".into(),
            kind: Some("http.s3".into()),
            vars: Some(
                r#"{"bucket":"b","region":"r","access_key_ref":"AKIA","secret_key_ref":"env:S"}"#
                    .into(),
            ),
            ..Default::default()
        };
        assert!(TargetDef::from_row(&lit).is_err());
        let basic = RawTarget {
            name: "es".into(),
            kind: Some("http.basic".into()),
            endpoints: Some("http://a:9200, http://b:9200".into()),
            vars: Some(r#"{"username":"u"}"#.into()),
            ..Default::default()
        };
        assert!(TargetDef::from_row(&basic).is_err());
        let redis = TargetDef::from_row(&RawTarget {
            name: "r".into(),
            kind: Some("redis".into()),
            endpoints: Some(r#"["cache:6379/0","cache:6379/1"]"#.into()),
            vars: Some(r#"{"password":"env:REDIS_PW","prefix":"69d_"}"#.into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(redis.endpoints.len(), 2);
        assert_eq!(redis.template_vars(), serde_json::json!({"prefix":"69d_"}));
    }

    #[test]
    fn triggers() {
        let t = TriggerDef::from_row(
            &RawTrigger {
                id: 1,
                rule: "r".into(),
                enabled: true,
                source_table: "`Shop`.`T`".into(),
                on: Some("insert, delete".into()),
                key: Some("item_{{new.id}}".into()),
                ..Default::default()
            },
            "app",
            true,
        )
        .unwrap();
        assert_eq!((t.schema.as_str(), t.table.as_str()), ("shop", "t"));
        assert_eq!(
            t.on,
            On {
                insert: true,
                update: false,
                delete: true
            }
        );
        let bad = RawTrigger {
            id: 2,
            source_table: "t".into(),
            keys_sql: Some("SELECT :x AS _key".into()),
            ..Default::default()
        };
        assert!(TriggerDef::from_row(&bad, "app", false).is_err());
    }
}

#[cfg(test)]
mod aws_endpoint_tests {
    use super::*;

    fn row(kind: &str, endpoints: Option<&str>, vars: &str) -> RawTarget {
        RawTarget {
            name: "t".into(),
            kind: Some(kind.into()),
            endpoints: endpoints.map(str::to_string),
            vars: Some(vars.into()),
            ..Default::default()
        }
    }

    const QUEUE: &str = "aws-sqs://eu-west-1/123456789012/orders";
    const STREAM: &str = "aws-kinesis://eu-west-1/orders";
    const CREDS: &str = r#""access_key_ref":"env:AK","secret_key_ref":"env:SK""#;

    #[test]
    fn sqs_accepts_the_url_forms_with_references() {
        let t = TargetDef::from_row(&row("sqs", Some(QUEUE), &format!("{{{CREDS}}}"))).unwrap();
        assert_eq!(t.kind, Kind::Sqs);
        assert_eq!(t.endpoints, vec![QUEUE.to_string()]);
        // Credentials never reach templates; the region is in the URL.
        assert_eq!(t.template_vars(), serde_json::json!({}));
        assert!(TargetDef::from_row(&row(
            "sqs",
            Some("aws-sqs://123456789012/orders.fifo"),
            "{}"
        ))
        .is_ok());
        assert!(TargetDef::from_row(&row(
            "sqs",
            Some("aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561"),
            "{}"
        ))
        .is_ok());
    }

    #[test]
    fn sqs_refuses_bad_endpoints() {
        let ok = "{}";
        // exactly one endpoint
        assert!(TargetDef::from_row(&row("sqs", None, ok)).is_err());
        assert!(TargetDef::from_row(&row("sqs", Some(&format!("{QUEUE},{QUEUE}")), ok)).is_err());
        // the scheme: aws-sqs://, never a plain URL or another aws- scheme
        assert!(TargetDef::from_row(&row(
            "sqs",
            Some("https://sqs.eu-west-1.amazonaws.com/123456789012/orders"),
            ok
        ))
        .is_err());
        assert!(TargetDef::from_row(&row("sqs", Some(STREAM), ok)).is_err());
        // the region is not in vars any more
        assert!(
            TargetDef::from_row(&row("sqs", Some(QUEUE), r#"{"region":"eu-west-1"}"#)).is_err()
        );
        // userinfo is never in the URL
        assert!(TargetDef::from_row(&row(
            "sqs",
            Some("aws-sqs://eu-west-1/u:p@123456789012/orders"),
            ok
        ))
        .is_err());
        // credentials are references, never literals, and come in pairs
        assert!(TargetDef::from_row(&row(
            "sqs",
            Some(QUEUE),
            r#"{"access_key_ref":"AKIA","secret_key_ref":"env:SK"}"#
        ))
        .is_err());
        assert!(
            TargetDef::from_row(&row("sqs", Some(QUEUE), r#"{"access_key_ref":"env:AK"}"#))
                .is_err()
        );
    }

    #[test]
    fn kinesis_accepts_the_url_forms_with_references() {
        let t =
            TargetDef::from_row(&row("kinesis", Some(STREAM), &format!("{{{CREDS}}}"))).unwrap();
        assert_eq!(t.kind, Kind::Kinesis);
        assert_eq!(t.endpoints, vec![STREAM.to_string()]);
        assert_eq!(t.template_vars(), serde_json::json!({}));
        assert!(TargetDef::from_row(&row("kinesis", Some("aws-kinesis://orders"), "{}")).is_ok());
        assert!(TargetDef::from_row(&row(
            "kinesis",
            Some("aws-kinesis://orders?endpoint=http://127.0.0.1:34562"),
            "{}"
        ))
        .is_ok());
    }

    #[test]
    fn kinesis_refuses_bad_endpoints() {
        let ok = "{}";
        assert!(TargetDef::from_row(&row("kinesis", None, ok)).is_err());
        assert!(
            TargetDef::from_row(&row("kinesis", Some(&format!("{STREAM},{STREAM}")), ok)).is_err()
        );
        // the scheme, and no plain service URL
        assert!(TargetDef::from_row(&row(
            "kinesis",
            Some("https://kinesis.eu-west-1.amazonaws.com"),
            ok
        ))
        .is_err());
        assert!(TargetDef::from_row(&row("kinesis", Some(QUEUE), ok)).is_err());
        // the stream and region are not in vars any more
        for v in [
            r#"{"stream_name":"orders"}"#,
            r#"{"stream_arn":"arn:aws:kinesis:eu-west-1:123456789012:stream/orders"}"#,
            r#"{"region":"eu-west-1"}"#,
        ] {
            assert!(
                TargetDef::from_row(&row("kinesis", Some(STREAM), v)).is_err(),
                "{v}"
            );
        }
        // userinfo and literal credentials are refused
        assert!(
            TargetDef::from_row(&row("kinesis", Some("aws-kinesis://u:p@orders"), ok)).is_err()
        );
        assert!(TargetDef::from_row(&row(
            "kinesis",
            Some(STREAM),
            r#"{"access_key_ref":"AKIA","secret_key_ref":"env:SK"}"#
        ))
        .is_err());
    }

    #[test]
    fn the_scheme_must_match_the_kind() {
        // an aws- scheme on a kind that does not take it
        assert!(TargetDef::from_row(&row("http", Some(QUEUE), "{}")).is_err());
        assert!(TargetDef::from_row(&row(
            "http.s3",
            Some(STREAM),
            r#"{"bucket":"b","region":"r"}"#
        ))
        .is_err());
        // a plain URL for http is still fine
        assert!(TargetDef::from_row(&row("http", Some("https://example.com"), "{}")).is_ok());
    }

    #[test]
    fn kind_properties() {
        assert_eq!(Kind::parse("kinesis").unwrap(), Kind::Kinesis);
        assert!(Kind::Kinesis.reports_items());
        assert!(Kind::Kinesis.append_only());
        assert_eq!(Kind::Kinesis.batch_bytes_limit(), Some(5_242_880));
        assert!(Kind::Kinesis.is_http());
    }

    #[test]
    fn lambda_properties_and_endpoints() {
        assert_eq!(Kind::parse("lambda").unwrap(), Kind::Lambda);
        assert_eq!(Kind::Lambda.name(), "lambda");
        assert!(Kind::Lambda.reports_items());
        // Not append-only: the stray refusal is a rule check (`rule::kind_errors`), not this.
        assert!(!Kind::Lambda.append_only());
        assert_eq!(Kind::Lambda.batch_bytes_limit(), None);
        let ok =
            |url: &str, vars: &str| TargetDef::from_row(&row("lambda", Some(url), vars)).is_ok();
        assert!(ok("aws-lambda://orders", r#"{}"#));
        assert!(ok(
            "aws-lambda://eu-west-1/orders:live?invocation=event",
            r#"{}"#
        ));
        assert!(ok(
            "aws-lambda://orders",
            r#"{"access_key_ref":"env:AK","secret_key_ref":"env:SK"}"#
        ));
        // one endpoint, the aws-lambda:// form, no region var, references only
        assert!(!ok("https://example.com/orders", "{}"));
        assert!(!ok("aws-sqs://123456789012/orders", "{}"));
        assert!(!ok("aws-lambda://orders", r#"{"region":"eu-west-1"}"#));
        assert!(!ok(
            "aws-lambda://orders",
            r#"{"access_key_ref":"AKIAEXAMPLE","secret_key_ref":"env:SK"}"#
        ));
        assert!(!ok("aws-lambda://orders?invocation=batch", "{}"));
        // the other kinds refuse an aws-lambda:// endpoint
        assert!(TargetDef::from_row(&row("http", Some("aws-lambda://orders"), "{}")).is_err());
    }
}
