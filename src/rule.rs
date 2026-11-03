//! A rule compiled for running: templates, select plans, triggers and its target.
//!
//! Registration errors are kept on the rule rather than dropping it: a rule whose triggers
//! compile keeps marking keys (and holds its position) while its selects, templates or target
//! are broken, so nothing is lost once they are fixed. A rule whose triggers do not compile
//! cannot mark keys; it is frozen and resets once fixed.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
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

/// Column names a statement returns, from a prepare (nothing runs).
async fn columns(conn: &mut Conn, sql: &str) -> Result<Vec<String>> {
    let stmt = conn.prep(sql).await?;
    let names = stmt
        .columns()
        .iter()
        .map(|c| c.name_str().into_owned())
        .collect();
    conn.close(stmt).await.ok();
    Ok(names)
}

/// `:keys` (the only placeholder selects take) expanded to `n` parameters.
pub fn expand_keys(sql: &str, n: usize) -> String {
    sql::expand(sql, |_| n).0
}

/// The selects, and every column they return (for [`kind_errors`]).
async fn compile_selects(conn: &mut Conn, def: &RuleDef) -> Result<(Vec<Select>, Vec<String>)> {
    let mut plans = Vec::new();
    let mut out = Vec::new();
    let mut all_columns = Vec::new();
    for s in &def.selects {
        for p in sql::placeholders(&s.sql) {
            if p.name != "keys" {
                bail!("{}: `:{}` (selects take only `:keys`)", s.name, p.name);
            }
        }
        let names = columns(conn, &expand_keys(&s.sql, 1))
            .await
            .with_context(|| format!("{} does not prepare", s.name))?;
        let plan = Plan::new(&names).with_context(|| s.name.clone())?;
        plans.push((s.name.clone(), plan));
        all_columns.extend(names);
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
pub fn kind_errors(
    def: &RuleDef,
    kind: crate::model::Kind,
    endpoints: &[String],
    columns: &[String],
) -> Vec<String> {
    use crate::model::Kind;
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
        out.extend(crate::amqp::rule_errors(def.fire_per));
    }
    if matches!(kind, Kind::Sqs | Kind::Kinesis) {
        // Purge-like `once` actions: a queue purge (`PurgeQueue`) or a stream delete
        // (`DeleteStream`, which drops every record). Recognized by text, as the only names of
        // such actions the templates can show us.
        let purge_words: &[&str] = if kind == Kind::Sqs {
            &["purgequeue"]
        } else {
            &["deletestream"]
        };
        for s in &def.reset_steps {
            let purge = s.kind == StepKind::Once
                && s.template.as_deref().is_some_and(|t| {
                    let t = t.to_ascii_lowercase();
                    purge_words.iter().any(|w| t.contains(w))
                });
            if purge {
                out.push(format!(
                    "reset step `{}`: purging is refused (kind {}: {} would drop the records)",
                    s.name,
                    kind.name(),
                    if kind == Kind::Sqs {
                        "PurgeQueue"
                    } else {
                        "DeleteStream"
                    }
                ));
            }
        }
    }
    if kind == Kind::PubSub {
        out.extend(crate::pubsub::rule_errors(def));
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
        if fifo && !columns.iter().any(|c| c == "version") {
            out.push(
                "a FIFO queue (a queue name ending in `.fifo`) needs a `version` column in a select: it is the MessageDeduplicationId"
                    .into(),
            );
        }
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
            if !cols.iter().any(|c| c == "_key") {
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
            Ok(c) if c.iter().any(|c| c == "_key") => {}
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
        errors.extend(kind_errors(
            &def,
            t.def.kind,
            &t.def.endpoints,
            &select_columns,
        ));
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

/// A trigger definition error for a row that did not even parse.
pub fn trigger_parse_error(id: u64, e: &anyhow::Error) -> anyhow::Error {
    anyhow!("trigger {id}: {e:#}")
}

#[cfg(test)]
mod kind_tests {
    use super::*;
    use crate::model::{Kind, RawRule, ResetStep};

    const QUEUE: &str = "aws-sqs://eu-west-1/123456789012/orders";
    const FIFO: &str = "aws-sqs://eu-west-1/123456789012/orders.fifo";

    fn def() -> RuleDef {
        let mut d = RuleDef::placeholder(&RawRule {
            name: "r".into(),
            enabled: true,
            ..Default::default()
        });
        d.fire_per = FirePer::Batch;
        d.batch_max_keys = Some(10);
        d
    }

    fn cols(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn stray() -> StepKind {
        StepKind::Stray {
            list_template: "GET /".into(),
            keys: "JSON_EXTRACT(body, '$.ids')".into(),
            next: None,
        }
    }

    fn step(kind: StepKind, template: Option<&str>) -> ResetStep {
        ResetStep {
            name: "s".into(),
            kind,
            template: template.map(str::to_string),
        }
    }

    #[test]
    fn lambda_refuses_strays_and_oversized_payloads() {
        const FN: &str = "aws-lambda://eu-west-1/orders";
        const EVENT: &str = "aws-lambda://eu-west-1/orders?invocation=event";
        // a stray listing does not exist for a function
        let mut d = def();
        d.reset_steps = vec![step(stray(), None)];
        let e = kind_errors(&d, Kind::Lambda, &[FN.into()], &[]);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("can list keys; kind lambda cannot"), "{e:?}");
        // the sync payload limit is 6 MB; the event limit is 1 MB
        let mut d = def();
        d.batch_max_bytes = Some(6_291_456);
        assert!(kind_errors(&d, Kind::Lambda, &[FN.into()], &[]).is_empty());
        d.batch_max_bytes = Some(6_291_457);
        assert_eq!(kind_errors(&d, Kind::Lambda, &[FN.into()], &[]).len(), 1);
        let mut d = def();
        d.batch_max_bytes = Some(1_048_577);
        assert_eq!(kind_errors(&d, Kind::Lambda, &[EVENT.into()], &[]).len(), 1);
        d.batch_max_bytes = Some(1_048_576);
        assert!(kind_errors(&d, Kind::Lambda, &[EVENT.into()], &[]).is_empty());
        // no batch_max_keys is needed: a batch is bounded by bytes
        let mut d = def();
        d.batch_max_keys = None;
        assert!(kind_errors(&d, Kind::Lambda, &[FN.into()], &[]).is_empty());
    }

    #[test]
    fn stray_steps_are_refused_for_append_only_kinds_only() {
        let mut d = def();
        d.reset_steps = vec![step(stray(), None)];
        let e = kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &cols(&["version"]));
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("only appends"));
        assert!(kind_errors(&d, Kind::Http, &["https://h".into()], &[]).is_empty());
    }

    #[test]
    fn amqp_refuses_stray_steps_and_batches() {
        let mut d = def();
        d.fire_per = FirePer::Key;
        d.reset_steps = vec![step(stray(), None)];
        let e = kind_errors(&d, Kind::Amqp, &["amqp://broker".into()], &cols(&["_key"]));
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("only appends"));
        d.reset_steps = Vec::new();
        d.fire_per = FirePer::Batch;
        let e = kind_errors(&d, Kind::Amqp, &["amqp://broker".into()], &[]);
        assert_eq!(e.len(), 1, "{e:?}");
        d.fire_per = FirePer::Key;
        assert!(kind_errors(&d, Kind::Amqp, &["amqp://broker".into()], &[]).is_empty());
    }

    #[test]
    fn purge_once_steps_are_refused_for_sqs() {
        let mut d = def();
        d.reset_steps = vec![step(
            StepKind::Once,
            Some("X-Amz-Target: AmazonSQS.PurgeQueue\n\n{}"),
        )];
        assert_eq!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).len(), 1);
        d.reset_steps = vec![step(
            StepKind::Once,
            Some("POST / HTTP/1.1\n\n{\"announce\":true}"),
        )];
        assert!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).is_empty());
    }

    #[test]
    fn sqs_batches_hold_at_most_ten_keys_and_256_kib() {
        let mut d = def();
        d.batch_max_keys = None;
        assert_eq!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).len(), 1);
        d.batch_max_keys = Some(11);
        assert_eq!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).len(), 1);
        d.batch_max_keys = Some(10);
        assert!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).is_empty());
        d.batch_max_bytes = Some(262_145);
        assert_eq!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).len(), 1);
        // One key per action has no batch to size.
        d.fire_per = FirePer::Key;
        d.batch_max_keys = None;
        d.batch_max_bytes = None;
        assert!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).is_empty());
    }

    #[test]
    fn fifo_queues_need_a_version_column() {
        let d = def();
        assert_eq!(
            kind_errors(&d, Kind::Sqs, &[FIFO.into()], &cols(&["_key"])).len(),
            1
        );
        assert!(kind_errors(&d, Kind::Sqs, &[FIFO.into()], &cols(&["_key", "version"])).is_empty());
        // A standard queue does not need one.
        assert!(kind_errors(&d, Kind::Sqs, &[QUEUE.into()], &[]).is_empty());
    }

    const STREAM: &str = "aws-kinesis://eu-west-1/orders";

    #[test]
    fn kinesis_batches_hold_at_most_500_records_and_5_mib() {
        let mut d = def();
        d.batch_max_keys = None;
        assert_eq!(
            kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).len(),
            1
        );
        d.batch_max_keys = Some(501);
        assert_eq!(
            kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).len(),
            1
        );
        d.batch_max_keys = Some(500);
        assert!(kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).is_empty());
        d.batch_max_bytes = Some(5_242_881);
        assert_eq!(
            kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).len(),
            1
        );
        d.batch_max_bytes = Some(5_242_880);
        assert!(kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).is_empty());
        // One key per action has no batch to size.
        d.fire_per = FirePer::Key;
        d.batch_max_keys = None;
        d.batch_max_bytes = None;
        assert!(kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).is_empty());
    }

    #[test]
    fn kinesis_refuses_stray_and_stream_delete_resets() {
        let mut d = def();
        d.reset_steps = vec![step(stray(), None)];
        let e = kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]);
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("only appends"));

        d.reset_steps = vec![step(
            StepKind::Once,
            Some("POST / HTTP/1.1\nX-Amz-Target: Kinesis_20131202.DeleteStream\n\n{}"),
        )];
        assert_eq!(
            kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).len(),
            1
        );
        // An announce record is a legitimate `once` action.
        d.reset_steps = vec![step(
            StepKind::Once,
            Some("POST / HTTP/1.1\nX-Amz-Target: Kinesis_20131202.PutRecords\n\n{}"),
        )];
        assert!(kind_errors(&d, Kind::Kinesis, &[STREAM.into()], &[]).is_empty());
    }
}

#[cfg(test)]
mod pubsub_kind_tests {
    use super::*;
    use crate::model::{Kind, RawRule, ResetStep};

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
        assert!(kind_errors(&d, Kind::PubSub, &[TOPIC.into()], &[]).is_empty());
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
        assert_eq!(kind_errors(&s, Kind::PubSub, &[TOPIC.into()], &[]).len(), 1);
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
                !kind_errors(&r, Kind::PubSub, &[TOPIC.into()], &[]).is_empty(),
                "accepted {bad}"
            );
        }
        let mut ok = def();
        ok.reset_steps.push(step(
            StepKind::Once,
            "POST /v1/projects/p/topics/t:publish HTTP/1.1",
        ));
        assert!(kind_errors(&ok, Kind::PubSub, &[TOPIC.into()], &[]).is_empty());
        // Batch limits: 1000 messages and 10 MB per publish.
        let mut none = def();
        none.batch_max_keys = None;
        assert_eq!(
            kind_errors(&none, Kind::PubSub, &[TOPIC.into()], &[]).len(),
            1
        );
        let mut over = def();
        over.batch_max_keys = Some(1001);
        assert_eq!(
            kind_errors(&over, Kind::PubSub, &[TOPIC.into()], &[]).len(),
            1
        );
        let mut bytes = def();
        bytes.batch_max_bytes = Some(10_000_001);
        assert_eq!(
            kind_errors(&bytes, Kind::PubSub, &[TOPIC.into()], &[]).len(),
            1
        );
    }
}
