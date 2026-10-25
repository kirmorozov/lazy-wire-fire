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
    /// A Handlebars template registered under this name.
    Full(String),
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

async fn compile_selects(conn: &mut Conn, def: &RuleDef) -> Result<Vec<Select>> {
    let mut plans = Vec::new();
    let mut out = Vec::new();
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
        out.push(Select {
            name: s.name.clone(),
            sql: s.sql.clone(),
        });
    }
    docs::check_owners(&plans)?;
    Ok(out)
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
                let name = format!("key{}", t.id);
                templates
                    .add(&name, src, KEY_ROOTS)
                    .with_context(|| format!("trigger {} `key`", t.id))?;
                KeyTemplate::Full(name)
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
    let mut templates = Templates::new();
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
    let selects = match compile_selects(conn, &def).await {
        Ok(s) => s,
        Err(e) => {
            errors.push(format!("selects: {e:#}"));
            Vec::new()
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
