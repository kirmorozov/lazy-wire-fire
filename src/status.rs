//! `lazy_wire_fire_status`: per rule (and `*` for the instance), the binlog position up to which
//! every action met `expect`. Only what a restart needs is kept here; key buffers live in memory.

use std::collections::HashMap;

use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row};

use crate::binlog::Position;

/// The instance's own row: where the stream resumes, and the server it belongs to.
pub const INSTANCE_ROW: &str = "*";

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Saved {
    pub pos: Option<Position>,
    pub reset_state: String,
    pub server_identity: Option<String>,
}

pub async fn load(conn: &mut Conn, instance: &str) -> Result<HashMap<String, Saved>> {
    let rows: Vec<Row> = conn
        .exec(
            "SELECT CAST(`rule` AS CHAR), CAST(`binlog_file` AS CHAR), `binlog_pos`, CAST(`reset_state` AS CHAR), \
             CAST(`server_identity` AS CHAR) FROM `lazy_wire_fire_status` WHERE `instance` = ?",
            (instance,),
        )
        .await
        .context("reading lazy_wire_fire_status")?;
    let mut out = HashMap::new();
    for r in rows {
        let s = |i: usize| {
            r.get_opt::<Option<String>, _>(i)
                .and_then(|v| v.ok())
                .flatten()
        };
        let file = s(1).unwrap_or_default();
        let pos = r
            .get_opt::<Option<u64>, _>(2)
            .and_then(|v| v.ok())
            .flatten()
            .unwrap_or(0);
        out.insert(
            s(0).unwrap_or_default(),
            Saved {
                pos: (!file.is_empty()).then_some(Position { file, pos }),
                reset_state: s(3).unwrap_or_default(),
                server_identity: s(4),
            },
        );
    }
    Ok(out)
}

/// One row as written.
#[derive(Debug, Clone, PartialEq)]
pub struct Write {
    pub rule: String,
    pub pos: Position,
    pub lag_ms: Option<u64>,
    pub reset_state: String,
    pub last_error: Option<String>,
    /// Unix milliseconds.
    pub last_error_at: Option<u64>,
    pub server_identity: Option<String>,
}

fn clip(s: &str, n: usize) -> String {
    match s.char_indices().nth(n) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

/// Writes `rows` in one transaction.
pub async fn write(conn: &mut Conn, instance: &str, rows: &[Write]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut tx = conn.start_transaction(Default::default()).await?;
    for w in rows {
        let vals: Vec<mysql_async::Value> = vec![
            w.pos.file.clone().into(),
            w.pos.pos.into(),
            w.lag_ms.into(),
            clip(&w.reset_state, 250).into(),
            w.last_error.as_deref().map(|e| clip(e, 4000)).into(),
            w.last_error_at.into(),
            w.server_identity.clone().into(),
        ];
        let mut params: Vec<mysql_async::Value> = vec![instance.into(), w.rule.clone().into()];
        params.extend(vals.iter().cloned());
        params.extend(vals);
        tx.exec_drop(
            "INSERT INTO `lazy_wire_fire_status` (`instance`, `rule`, `binlog_file`, `binlog_pos`, `lag_ms`, `reset_state`, \
             `last_error`, `last_error_at`, `server_identity`, `updated_at`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(? / 1000), ?, NOW(3)) \
             ON DUPLICATE KEY UPDATE `binlog_file` = ?, `binlog_pos` = ?, `lag_ms` = ?, `reset_state` = ?, `last_error` = ?, \
             `last_error_at` = FROM_UNIXTIME(? / 1000), `server_identity` = ?, `updated_at` = NOW(3)",
            params,
        )
        .await
        .with_context(|| format!("writing the status of `{}`", w.rule))?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn delete(conn: &mut Conn, instance: &str, rule: &str) -> Result<()> {
    conn.exec_drop(
        "DELETE FROM `lazy_wire_fire_status` WHERE `instance` = ? AND `rule` = ?",
        (instance, rule),
    )
    .await?;
    Ok(())
}
