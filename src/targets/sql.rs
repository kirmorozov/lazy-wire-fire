//! The `mysql` and `pgsql` kinds: the template renders one SQL statement per action, run
//! on the target database over its own protocol. The two kinds share everything but the
//! `Dialect` and the client variant: the clients and the dialect live in
//! [`crate::sqlwire`] (wire plumbing, not a kind), and this module owns the kinds' load
//! and dispatch bodies.
//!
//! Not to be confused with [`crate::sql`]: that is SQL handling (placeholders,
//! expressions, DDL), used far beyond targets — it is not this kind.
//!
//! An endpoint is parsed as [`SqlAddr`]; `vars.username` and `vars.password` (references
//! or literals) are the credentials. The answer is judged by `expect` over the row of the
//! statement's affected count, or — for a listing — over the JSON of its rows.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use mysql_async::Pool;
use serde_json::Value;

use crate::model::{Kind, TargetDef};
use crate::response::{Expect, Row, Uses};
use crate::sqlwire::{Credentials, MySqlClient, PgClient, SqlAddr};
use crate::target::{def_var, rejected, Sent, Transport};

/// The endpoint's context (`endpoint` in templates) and its transport (the client the
/// kind's dialect picks).
pub(crate) fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Transport)> {
    let d = def.kind.dialect();
    let addr = SqlAddr::parse(endpoint, d)?;
    let ctx = addr.ctx(d);
    let creds = Credentials {
        user: def_var(def, "username").map(str::to_string),
        password: def_var(def, "password").map(str::to_string),
    };
    let transport = if def.kind == Kind::MySql {
        Transport::MySql(MySqlClient::new(addr, &def.tls, creds))
    } else {
        Transport::Pg(PgClient::new(addr, &def.tls, creds))
    };
    Ok((ctx, transport))
}

/// Sends one rendered statement to a `mysql`/`pgsql` endpoint (see the module docs).
pub(crate) async fn send(
    transport: &Transport,
    rendered: &str,
    timeout: Duration,
    expect: &Expect,
    pool: &Pool,
) -> Result<Sent> {
    let sql = match crate::sqlwire::single_statement(rendered) {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(Sent::Skipped),
        Err(e) => return Err(rejected(format!("{e:#}"))),
    };
    let (affected, at) = match transport {
        Transport::MySql(c) => (
            c.exec(sql, timeout).await?,
            format!("{}/{}", c.addr.host, c.addr.db),
        ),
        Transport::Pg(c) => (
            c.exec(&sql, timeout).await?,
            format!("{}/{}", c.addr.host, c.addr.db),
        ),
        _ => unreachable!(),
    };
    expect
        .row(pool, |u| Row::affected(u, affected))
        .await
        .with_context(|| at.clone())?
        .map_err(|e| rejected(format!("{at}: {e}")))?;
    Ok(Sent::Ok)
}

/// Runs one rendered listing statement on a `mysql`/`pgsql` endpoint and answers its
/// rows as the row (see the module docs).
pub(crate) async fn fetch(
    transport: &Transport,
    rendered: &str,
    timeout: Duration,
    uses: Uses,
) -> Result<Row> {
    match transport {
        Transport::MySql(c) => {
            let sql = crate::sqlwire::single_statement(rendered)?
                .ok_or_else(|| anyhow!("the list template rendered nothing"))?;
            Ok(Row::json(uses, &c.rows(sql, timeout).await?))
        }
        Transport::Pg(c) => {
            let sql = crate::sqlwire::single_statement(rendered)?
                .ok_or_else(|| anyhow!("the list template rendered nothing"))?;
            Ok(Row::json(uses, &c.rows(&sql, timeout).await?))
        }
        _ => unreachable!(),
    }
}
