//! A rule compiled for running: templates, select plans, triggers and its target.
//!
//! Registration errors are kept on the rule rather than dropping it: a rule whose triggers
//! compile keeps marking keys (and holds its position) while its selects, templates or target
//! are broken, so nothing is lost once they are fixed. A rule whose triggers do not compile
//! cannot mark keys; it is frozen and resets once fixed.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::Conn;

use crate::docs::{self, Plan};
use crate::model::{FirePer, ResetStep, RuleDef, StepKind, TriggerDef};
use crate::sql;
use crate::target::Target;
use crate::template::{self, Seg, Templates, ACTION_ROOTS, KEY_ROOTS};

pub const ACTION: &str = "action";

pub fn step_template(i: usize) -> String {
    format!("step{i}")
}

pub fn list_template(i: usize) -> String {
    format!("list{i}")
}

#[derive(Debug, Clone)]
pub enum KeyTemplate {
    /// Literal text and column references per line.
    Simple(Vec<Vec<Seg>>),
    /// One Handlebars template per line of the source (registered under these names), so a
    /// value holding a newline cannot add keys.
    Full(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct KeysSql {
    /// The statement with `?` placeholders.
    pub sql: Arc<str>,
    /// Column of the image bound to each `?`.
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Trigger {
    pub def: TriggerDef,
    pub key: Option<KeyTemplate>,
    pub keys_sql: Option<KeysSql>,
}

#[derive(Debug, Clone)]
pub struct Select {
    pub name: String,
    /// Statement with `:keys` still in it (expanded per batch size).
    pub sql: String,
}

pub struct Rule {
    pub def: RuleDef,
    pub fingerprint: String,
    pub templates: Templates,
    pub selects: Vec<Select>,
    pub triggers: Vec<Trigger>,
    pub target: Option<Arc<Target>>,
    /// Why the rule cannot fire (selects, templates, target).
    pub error: Option<String>,
    /// Why some trigger cannot mark keys.
    pub trigger_error: Option<String>,
}

impl Rule {
    pub fn name(&self) -> &str {
        &self.def.name
    }

    /// Keys per action group (and per reset batch).
    pub fn group_size(&self) -> usize {
        match self.def.fire_per {
            FirePer::Key => 100,
            FirePer::Batch => self.def.batch_max_keys.unwrap_or(10_000),
        }
    }

    pub fn step(&self, i: usize) -> Option<&ResetStep> {
        self.def.reset_steps.get(i)
    }

    /// Template name an action of reset step `i` renders.
    pub fn step_action(&self, i: usize) -> String {
        match self
            .def
            .reset_steps
            .get(i)
            .and_then(|s| s.template.as_ref())
        {
            Some(_) => step_template(i),
            None => ACTION.to_string(),
        }
    }
}

/// Column names and types a statement returns, from a prepare (nothing runs).
async fn columns(conn: &mut Conn, sql: &str) -> Result<Vec<docs::Field>> {
    let stmt = conn.prep(sql).await?;
    let out = stmt
        .columns()
        .iter()
        .map(|c| {
            docs::Field::new(
                c.name_str(),
                Some(c.column_type()),
                c.flags()
                    .contains(mysql_async::consts::ColumnFlags::BINARY_FLAG),
            )
        })
        .collect();
    conn.close(stmt).await.ok();
    Ok(out)
}

/// `:keys` (the only placeholder selects take) expanded to `n` parameters.
pub fn expand_keys(sql: &str, n: usize) -> String {
    sql::expand(sql, |_| n).0
}

/// The selects, and every payload column they return (for [`kind_errors`]).
async fn compile_selects(
    conn: &mut Conn,
    def: &RuleDef,
) -> Result<(Vec<Select>, Vec<docs::Field>)> {
    let mut plans = Vec::new();
    let mut out = Vec::new();
    let mut all_columns = Vec::new();
    for s in &def.selects {
        for p in sql::placeholders(&s.sql) {
            if p.name != "keys" {
                bail!("{}: `:{}` (selects take only `:keys`)", s.name, p.name);
            }
        }
        let cols = columns(conn, &expand_keys(&s.sql, 1))
            .await
            .with_context(|| format!("{} does not prepare", s.name))?;
        let plan = Plan::with_fields(&cols).with_context(|| s.name.clone())?;
        // The plan's payload columns, not the raw ones: a target that checks its schema against
        // them wants the document key each column fills (`lines[].sku` fills `lines`, and as an
        // array), and the protocol columns are not part of any document.
        all_columns.extend_from_slice(plan.fields());
        plans.push((s.name.clone(), plan));
        out.push(Select {
            name: s.name.clone(),
            sql: s.sql.clone(),
        });
    }
    docs::check_owners(&plans)?;
    Ok((out, all_columns))
}

/// Refusals that depend on the target's kind, checked when the rule loads (`columns`: every
/// column the rule's selects return).
///
/// - Append-only kinds (`sqs`): no `for_each: stray` step (a sent message cannot be recalled),
///   and no `once` step that purges the queue (`PurgeQueue`; the only purge-like action named
///   in the templates we can recognize, see the report).
/// - `sqs`: a batch holds at most 10 keys (`batch_max_keys`, required) and 256 KiB.
/// - A FIFO queue (the endpoint names a `.fifo` queue) needs a `version` column: it is the
///   `MessageDeduplicationId` (decision D5).
/// - `eventbridge`: a batch holds at most 10 keys (`batch_max_keys`, required: 10 entries per
///   PutEvents) and 1 MB.
/// - `sns`: a batch holds at most 10 keys (`batch_max_keys`, required: 10 entries per
///   PublishBatch) and 256 KiB — or the topic's `MaximumMessageSize`, up to 1 MiB, as the
///   target's `vars.maximum_message_size` states it. A FIFO topic (its name ends in `.fifo`)
///   needs a `version` column: it is the `MessageDeduplicationId`, as for a FIFO queue.
/// - `kafka`: a batch holds at most the target's limit (the broker's `message.max.bytes`,
///   raised by its `vars.message_max_bytes`).
/// - `grpc`: the rule's payload columns are the item message's fields, so they are checked
///   against the target's descriptor set, and a batch is held to the target's
///   `vars.max_message_bytes` (see [`crate::targets::grpc::rule_errors`]).
/// - a plain http kind with `vars.oauth`: the token sets the `Authorization` header, so a
///   template whose own header block carries a literal one is refused (see
///   [`crate::oauth::rule_errors`]).
pub fn kind_errors(
    def: &RuleDef,
    target: &crate::model::TargetDef,
    fields: &[docs::Field],
) -> Vec<String> {
    use crate::model::Kind;
    let kind = target.kind;
    let endpoints = &target.endpoints;
    let mut out = Vec::new();
    if kind.append_only() || kind == Kind::Lambda {
        // A Lambda function cannot list what it was invoked with, so a stray listing has nothing
        // to find (it is not append-only by nature, but the listing does not exist).
        for s in &def.reset_steps {
            if matches!(s.kind, StepKind::Stray { .. }) {
                let why = if kind == Kind::Lambda {
                    "a stray cleanup needs a target that can list keys; kind lambda cannot"
                        .to_string()
                } else {
                    format!(
                        "a stray cleanup needs a target that can delete keys; kind {} only appends",
                        kind.name()
                    )
                };
                out.push(format!("reset step `{}`: {why}", s.name));
            }
        }
    }
    if kind == Kind::Lambda {
        // The payload limit is set by the invocation (6 MB sync, 1 MB event), so a larger
        // `batch_max_bytes` is refused at load.
        if let Some(u) = endpoints
            .first()
            .and_then(|e| crate::aws::LambdaUrl::parse(e).ok())
        {
            if def.batch_max_bytes.is_some_and(|n| n > u.payload_limit()) {
                out.push(format!(
                    "`batch_max_bytes` is over {} bytes, the most a {} Invoke payload takes",
                    u.payload_limit(),
                    match u.invocation {
                        crate::aws::Invocation::Sync => "synchronous",
                        crate::aws::Invocation::Event => "asynchronous (event)",
                    }
                ));
            }
        }
    }
    if kind == Kind::Amqp {
        out.extend(crate::targets::amqp::rule_errors(def.fire_per));
    }
    if kind == Kind::Smtp {
        out.extend(crate::targets::smtp::rule_errors(def.fire_per));
    }
    if matches!(
        kind,
        Kind::Sqs | Kind::Kinesis | Kind::EventBridge | Kind::Sns
    ) {
        // Purge-like `once` actions: a queue purge (`PurgeQueue`), a stream delete
        // (`DeleteStream`, which drops every record), an event bus delete (`DeleteEventBus`,
        // which takes the bus and every rule on it) or a topic delete (`DeleteTopic`, which takes
        // the topic and every subscription on it). Recognized by text, as the only names of
        // such actions the templates can show us.
        let (word, action) = match kind {
            Kind::Sqs => ("purgequeue", "PurgeQueue"),
            Kind::EventBridge => ("deleteeventbus", "DeleteEventBus"),
            Kind::Sns => ("deletetopic", "DeleteTopic"),
            _ => ("deletestream", "DeleteStream"),
        };
        for s in &def.reset_steps {
            let purge = s.kind == StepKind::Once
                && s.template.as_deref().is_some_and(|t| {
                    let t = t.to_ascii_lowercase();
                    t.contains(word)
                });
            if purge {
                out.push(format!(
                    "reset step `{}`: purging is refused (kind {}: {} would drop the records)",
                    s.name,
                    kind.name(),
                    action
                ));
            }
        }
    }
    if kind == Kind::PubSub {
        out.extend(crate::targets::pubsub::rule_errors(def));
    }
    if matches!(
        kind,
        Kind::Http | Kind::HttpBasic | Kind::HttpBearer | Kind::HttpHeader
    ) && target.vars.contains_key("oauth")
    {
        // `vars.oauth` sets the `Authorization` header: a template whose own header block
        // carries a literal one is a second source of it (a name produced only by rendering
        // is caught at the bind, in `Target::prepare`).
        out.extend(crate::oauth::rule_errors(def));
    }
    if kind == Kind::Kafka {
        // The batch's byte limit: the target's own (the broker's `message.max.bytes`, raised
        // by its `vars.message_max_bytes` — checked at load, so it cannot fail here).
        let limit = crate::targets::kafka::message_max_bytes(&target.vars)
            .unwrap_or(crate::targets::kafka::MESSAGE_MAX_BYTES);
        out.extend(crate::targets::kafka::rule_errors(def, limit));
    }
    if kind == Kind::ServiceBus {
        // The batch's byte limit: the entity's maximum message size (256 KiB by default,
        // raised by the target's `vars.max_message_size` — checked at load, so the read
        // cannot fail here).
        let limit = crate::targets::servicebus::max_message_bytes(&target.vars)
            .unwrap_or(crate::targets::servicebus::MAX_MESSAGE_BYTES);
        out.extend(crate::targets::servicebus::rule_errors(def, limit));
    }
    if kind == Kind::RabbitMqStream {
        // One publish is one stream frame: 1 MiB, with no var to raise it (the crate sends
        // the batch as a single frame).
        out.extend(crate::targets::rabbitmq_stream::rule_errors(
            def,
            crate::targets::rabbitmq_stream::MESSAGE_MAX_BYTES,
        ));
    }
    if kind == Kind::Kinesis && def.fire_per == FirePer::Batch {
        match def.batch_max_keys {
            None => out.push(
                "kind kinesis needs `batch_max_keys` (1 to 500: records per PutRecords)".into(),
            ),
            Some(n) if n > 500 => out.push(format!(
                "`batch_max_keys` {n} is over 500, the most records a PutRecords takes"
            )),
            Some(_) => {}
        }
        if def.batch_max_bytes.is_some_and(|n| n > 5_242_880) {
            out.push(
                "`batch_max_bytes` is over 5242880 (5 MiB), the most a PutRecords takes".into(),
            );
        }
    }
    if kind == Kind::Sqs {
        if def.fire_per == FirePer::Batch {
            match def.batch_max_keys {
                None => out.push(
                    "kind sqs needs `batch_max_keys` (1 to 10: entries per SendMessageBatch)"
                        .into(),
                ),
                Some(n) if n > 10 => out.push(format!(
                    "`batch_max_keys` {n} is over 10, the most entries a SendMessageBatch takes"
                )),
                Some(_) => {}
            }
        }
        if def.batch_max_bytes.is_some_and(|n| n > 262_144) {
            out.push("`batch_max_bytes` is over 262144, the most a SendMessageBatch takes".into());
        }
        let fifo = endpoints
            .iter()
            .any(|e| crate::aws::SqsUrl::parse(e).is_ok_and(|u| u.fifo()));
        if fifo && !fields.iter().any(|f| f.column == "version") {
            out.push(
                "a FIFO queue (a queue name ending in `.fifo`) needs a `version` column in a select: it is the MessageDeduplicationId"
                    .into(),
            );
        }
    }
    if kind == Kind::EventBridge {
        // One action is one PutEvents (aws::EVENTBRIDGE_MAX_ENTRIES / _MAX_BYTES); the engine
        // splits by bytes, never by count, so the count is a load-time requirement as for sqs.
        if def.fire_per == FirePer::Batch {
            match def.batch_max_keys {
                None => out.push(
                    "kind eventbridge needs `batch_max_keys` (1 to 10: entries per PutEvents)"
                        .into(),
                ),
                Some(n) if n > crate::aws::EVENTBRIDGE_MAX_ENTRIES => out.push(format!(
                    "`batch_max_keys` {n} is over {}, the most entries a PutEvents takes",
                    crate::aws::EVENTBRIDGE_MAX_ENTRIES
                )),
                Some(_) => {}
            }
        }
        if def
            .batch_max_bytes
            .is_some_and(|n| n > crate::aws::EVENTBRIDGE_MAX_BYTES)
        {
            out.push(
                "`batch_max_bytes` is over 1048576 (1 MB), the most a PutEvents request takes"
                    .into(),
            );
        }
    }
    if kind == Kind::Sns {
        // One action is one PublishBatch: the same number bounds one message and the whole batch,
        // the topic's MaximumMessageSize (256 KiB by default, at most 1 MiB), which
        // `vars.maximum_message_size` states for the target (checked at load, so it cannot fail
        // here). The engine splits by bytes, never by count, so the count is a load-time
        // requirement as for sqs and eventbridge.
        let limit =
            crate::sns::maximum_message_size(&target.vars).unwrap_or(crate::aws::SNS_MAX_BYTES);
        if def.fire_per == FirePer::Batch {
            match def.batch_max_keys {
                None => out.push(
                    "kind sns needs `batch_max_keys` (1 to 10: entries per PublishBatch)".into(),
                ),
                Some(n) if n > crate::aws::SNS_MAX_ENTRIES => out.push(format!(
                    "`batch_max_keys` {n} is over {}, the most entries a PublishBatch takes",
                    crate::aws::SNS_MAX_ENTRIES
                )),
                Some(_) => {}
            }
        }
        if def.batch_max_bytes.is_some_and(|n| n > limit) {
            out.push(format!(
                "`batch_max_bytes` is over {limit} bytes, the largest message and the largest batch the topic takes (its MaximumMessageSize, 262144 by default and at most 1048576; `vars.maximum_message_size` states it)"
            ));
        }
        let fifo = endpoints
            .iter()
            .any(|e| crate::aws::SnsUrl::parse(e).is_ok_and(|u| u.fifo()));
        if fifo && !fields.iter().any(|f| f.column == "version") {
            out.push(
                "a FIFO topic (a topic name ending in `.fifo`) needs a `version` column in a select: it is the MessageDeduplicationId"
                    .into(),
            );
        }
    }
    if kind == Kind::Grpc {
        // The rule's columns are the item message's fields: check them against the target's
        // descriptor set now, so a schema that does not fit is a rule that never started
        // rather than a flush whose documents are all refused (see [`crate::targets::grpc::rule_errors`]).
        out.extend(crate::targets::grpc::rule_errors(def, target, fields));
    }
    out
}

async fn compile_trigger(
    conn: &mut Conn,
    templates: &mut Templates,
    t: &TriggerDef,
) -> Result<Trigger> {
    let key = match &t.key {
        None => None,
        Some(src) => Some(match template::simple_key_template(src) {
            Some(lines) => KeyTemplate::Simple(lines),
            None => {
                let mut names = Vec::new();
                for (n, line) in src
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .enumerate()
                {
                    let name = format!("key{}_{n}", t.id);
                    templates.add(&name, line, KEY_ROOTS).with_context(|| {
                        format!(
                            "trigger {} `key`, line {} (each line is a template of its own)",
                            t.id,
                            n + 1
                        )
                    })?;
                    names.push(name);
                }
                KeyTemplate::Full(names)
            }
        }),
    };
    let keys_sql = match &t.keys_sql {
        None => None,
        Some(q) => {
            let (sql, names) = sql::expand(q, |_| 1);
            let columns = names
                .iter()
                .map(|n| n.split_once('.').map_or(n.clone(), |(_, c)| c.to_string()))
                .collect();
            let cols = self::columns(conn, &sql)
                .await
                .with_context(|| format!("trigger {} `keys_sql` does not prepare", t.id))?;
            if !cols.iter().any(|c| c.column == "_key") {
                bail!("trigger {} `keys_sql` returns no `_key` column", t.id);
            }
            Some(KeysSql {
                sql: sql.into(),
                columns,
            })
        }
    };
    Ok(Trigger {
        def: t.clone(),
        key,
        keys_sql,
    })
}

/// Compiles `def` with its triggers; problems end up in `error` / `trigger_error`.
pub async fn compile(
    conn: &mut Conn,
    def: RuleDef,
    triggers: Vec<TriggerDef>,
    targets: &HashMap<String, std::result::Result<Arc<Target>, String>>,
) -> Rule {
    let dialect = match targets.get(&def.target) {
        Some(Ok(t)) => t.def.kind.dialect(),
        _ => crate::sqlwire::Dialect::MySql,
    };
    let mut templates = Templates::for_dialect(dialect);
    let mut errors: Vec<String> = Vec::new();
    let mut add = |templates: &mut Templates, name: &str, src: &str| {
        if let Err(e) = templates.add(name, src, ACTION_ROOTS) {
            errors.push(format!("{e:#}"));
        }
    };
    add(&mut templates, ACTION, &def.template);
    for (i, s) in def.reset_steps.iter().enumerate() {
        if let Some(t) = &s.template {
            add(&mut templates, &step_template(i), t);
        }
        if let StepKind::Stray {
            list_template: l, ..
        } = &s.kind
        {
            add(&mut templates, &list_template(i), l);
        }
    }
    let (selects, select_columns) = match compile_selects(conn, &def).await {
        Ok(s) => s,
        Err(e) => {
            errors.push(format!("selects: {e:#}"));
            (Vec::new(), Vec::new())
        }
    };
    if let Some(q) = &def.snapshot_keys_sql {
        match columns(conn, q).await {
            Ok(c) if c.iter().any(|c| c.column == "_key") => {}
            Ok(_) => errors.push("`snapshot_keys_sql` returns no `_key` column".into()),
            Err(e) => errors.push(format!("`snapshot_keys_sql` does not prepare: {e:#}")),
        }
    }
    let target = match targets.get(&def.target) {
        Some(Ok(t)) => Some(t.clone()),
        Some(Err(e)) => {
            errors.push(format!("target `{}`: {e}", def.target));
            None
        }
        None => {
            errors.push(format!("target `{}` does not exist", def.target));
            None
        }
    };
    if let Some(t) = &target {
        if t.endpoints.is_empty() {
            errors.push(format!("target `{}` has no endpoints", def.target));
        }
        errors.extend(kind_errors(&def, &t.def, &select_columns));
        if let Err(e) = t.expect.validate(conn).await {
            errors.push(format!("target `{}` `expect`: {e:#}", def.target));
        }
    }
    for (i, s) in def.reset_steps.iter().enumerate() {
        if let StepKind::Stray { keys, next, .. } = &s.kind {
            let mut exprs = vec![keys.as_str()];
            exprs.extend(next.as_deref());
            let checked = match crate::response::Values::new(&exprs) {
                Ok(v) => v.validate(conn).await,
                Err(e) => Err(e),
            };
            if let Err(e) = checked {
                errors.push(format!("`reset_steps[{i}]` `keys` / `next`: {e:#}"));
            }
        }
    }
    let mut compiled = Vec::new();
    let mut trigger_errors = Vec::new();
    for t in &triggers {
        match compile_trigger(conn, &mut templates, t).await {
            Ok(c) => compiled.push(c),
            Err(e) => trigger_errors.push(format!("{e:#}")),
        }
    }
    if triggers.is_empty() {
        tracing::debug!(rule = %def.name, "rule has no triggers");
    }
    Rule {
        // A changed target (endpoints, kind) restarts a running reset like a changed rule.
        fingerprint: format!(
            "{}|{:?}",
            def.fingerprint(),
            target.as_ref().map(|t| &t.def)
        ),
        def,
        templates,
        selects,
        triggers: compiled,
        target,
        error: (!errors.is_empty()).then(|| errors.join("; ")),
        trigger_error: (!trigger_errors.is_empty()).then(|| trigger_errors.join("; ")),
    }
}

/// A rule-load target of `kind` with one endpoint and `vars` (the tests' kind targets load).
#[cfg(test)]
fn test_target(kind: &str, endpoint: &str, vars: &str) -> crate::model::TargetDef {
    crate::model::TargetDef::from_row(&crate::model::RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoint.into()),
        vars: Some(vars.into()),
        ..Default::default()
    })
    .unwrap()
}

#[cfg(test)]
#[path = "tests/rule_kind_tests.rs"]
mod kind_tests;

#[cfg(test)]
mod pubsub_kind_tests {
    use super::*;
    use crate::model::{FirePer, RawRule, ResetStep};

    const TOPIC: &str = "gc-pubsub://lwf-test/orders";

    fn def() -> RuleDef {
        let mut d = RuleDef::placeholder(&RawRule {
            name: "r".into(),
            enabled: true,
            ..Default::default()
        });
        d.fire_per = FirePer::Batch;
        d.batch_max_keys = Some(1000);
        d
    }

    fn step(kind: StepKind, template: &str) -> ResetStep {
        ResetStep {
            name: "s".into(),
            kind,
            template: Some(template.to_string()),
        }
    }

    #[test]
    fn pubsub_refuses_stray_steps_purges_and_oversized_batches() {
        let d = def();
        assert!(kind_errors(&d, &test_target("pubsub", TOPIC, "{}"), &[]).is_empty());
        // Stray cleanup: the kind only appends (the append-only check).
        let mut s = def();
        s.reset_steps.push(step(
            StepKind::Stray {
                list_template: "GET /".into(),
                keys: "JSON_EXTRACT(body, '$.ids')".into(),
                next: None,
            },
            "POST /",
        ));
        assert_eq!(
            kind_errors(&s, &test_target("pubsub", TOPIC, "{}"), &[]).len(),
            1
        );
        // A seek, a delete or a subscription-level request in a once/snapshot step is refused.
        for bad in [
            "POST /v1/projects/p/topics/t:publish\n\n:seek",
            "DELETE /v1/projects/p/topics/t",
            "POST /v1/projects/p/subscriptions/s:seek",
            "POST /v1/projects/p/topics/t:detach",
            "POST /v1/projects/p/topics/other:publish HTTP/1.1 purge",
        ] {
            let mut r = def();
            r.reset_steps.push(step(StepKind::Once, bad));
            assert!(
                !kind_errors(&r, &test_target("pubsub", TOPIC, "{}"), &[]).is_empty(),
                "accepted {bad}"
            );
        }
        let mut ok = def();
        ok.reset_steps.push(step(
            StepKind::Once,
            "POST /v1/projects/p/topics/t:publish HTTP/1.1",
        ));
        assert!(kind_errors(&ok, &test_target("pubsub", TOPIC, "{}"), &[]).is_empty());
        // Batch limits: 1000 messages and 10 MB per publish.
        let mut none = def();
        none.batch_max_keys = None;
        assert_eq!(
            kind_errors(&none, &test_target("pubsub", TOPIC, "{}"), &[]).len(),
            1
        );
        let mut over = def();
        over.batch_max_keys = Some(1001);
        assert_eq!(
            kind_errors(&over, &test_target("pubsub", TOPIC, "{}"), &[]).len(),
            1
        );
        let mut bytes = def();
        bytes.batch_max_bytes = Some(10_000_001);
        assert_eq!(
            kind_errors(&bytes, &test_target("pubsub", TOPIC, "{}"), &[]).len(),
            1
        );
    }
}
