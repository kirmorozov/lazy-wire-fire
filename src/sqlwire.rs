//! `mysql` and `pgsql` targets: the template renders one SQL statement, run on the target
//! database over its own protocol.
//!
//! * One statement per action: a top-level `;` followed by more is refused (PostgreSQL runs it
//!   through the extended protocol, which refuses batches as well).
//! * The session is pinned on every connect, so quoting is sound: MySQL `SET NAMES utf8mb4` with
//!   backslash escapes on (`NO_BACKSLASH_ESCAPES` removed, verified); PostgreSQL UTF-8 with
//!   `standard_conforming_strings` on (verified).
//! * Values go in through `sql_quote` (a string literal of the target's dialect) and
//!   `sql_ident` (a quoted identifier).

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use mysql_async::prelude::*;
use rustls::pki_types::ServerName;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;

use crate::http::split_host_port;
use crate::secrets;
use crate::tls::{self, Mode, TlsSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    MySql,
    Postgres,
}

/// `sql_quote`: the value as a string literal (`NULL` for null). Numbers and booleans are
/// quoted too (`'12.5'`, `'1'`), which both servers convert for numeric columns; objects and
/// arrays become their JSON text. A NUL character is refused.
pub fn quote(d: Dialect, v: &Value) -> Result<String> {
    let s = match v {
        Value::Null => return Ok("NULL".into()),
        Value::Bool(b) => (if *b { "1" } else { "0" }).to_string(),
        Value::String(s) => s.clone(),
        other => crate::template::text(other),
    };
    if s.contains('\0') {
        bail!("sql_quote: the value holds a NUL character");
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            // MySQL sessions keep backslash escapes on (pinned at connect).
            '\\' if d == Dialect::MySql => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('\'');
    Ok(out)
}

/// `sql_ident`: a quoted identifier (MySQL backticks, PostgreSQL double quotes).
pub fn ident(d: Dialect, name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        bail!("sql_ident: empty name or NUL character");
    }
    let q = if d == Dialect::MySql { '`' } else { '"' };
    Ok(format!("{q}{}{q}", name.replace(q, &format!("{q}{q}"))))
}

/// The rendered statement, trimmed (`None` when blank). A statement followed by another one
/// is refused; one trailing `;` is fine.
pub fn single_statement(rendered: &str) -> Result<Option<String>> {
    let parts = crate::sql::split_statements(rendered);
    match parts.len() {
        0 => Ok(None),
        1 => Ok(Some(parts.into_iter().next().expect("one"))),
        n => bail!("the rendered SQL holds {n} statements; an action is one statement"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlAddr {
    pub host: String,
    pub port: u16,
    pub db: String,
}

impl SqlAddr {
    /// `host:port/db`, or with a scheme (`mysql://`, `postgres://`, `postgresql://`, `pgsql://`).
    pub fn parse(s: &str, d: Dialect) -> Result<SqlAddr> {
        let s = s.trim();
        let rest = match s.split_once("://") {
            Some((scheme, r)) => {
                let ok = match d {
                    Dialect::MySql => matches!(scheme, "mysql" | "mariadb"),
                    Dialect::Postgres => matches!(scheme, "postgres" | "postgresql" | "pgsql"),
                };
                if !ok {
                    bail!("endpoint `{s}`: unexpected scheme `{scheme}`");
                }
                r
            }
            None => s,
        };
        let (authority, db) = rest.split_once('/').unwrap_or((rest, ""));
        if authority.contains('@') {
            bail!("endpoint `{s}`: credentials belong in the target's vars, not in the address");
        }
        let default = if d == Dialect::MySql { 3306 } else { 5432 };
        let (host, port) =
            split_host_port(authority, default).with_context(|| format!("endpoint `{s}`"))?;
        let db = db.trim_matches('/').to_string();
        if db.is_empty() {
            bail!("endpoint `{s}` names no database (`host:port/db`)");
        }
        Ok(SqlAddr { host, port, db })
    }

    pub fn ctx(&self, d: Dialect) -> Value {
        let scheme = if d == Dialect::MySql {
            "mysql"
        } else {
            "postgres"
        };
        json!({"host": self.host, "port": self.port, "db": self.db, "url": format!("{scheme}://{}:{}/{}", self.host, self.port, self.db)})
    }
}

/// Server errors that mean "not now" (deadlocks, lock waits, timeouts, lost connections,
/// overload) rather than "not this statement".
fn mysql_transient(e: &mysql_async::Error) -> bool {
    match e {
        mysql_async::Error::Server(se) => {
            matches!(
                se.code,
                1040 | 1053 | 1205 | 1213 | 1290 | 1317 | 1927 | 1969 | 2006 | 2013 | 3024 | 4031
            )
        }
        mysql_async::Error::Io(_) | mysql_async::Error::Driver(_) => true,
        _ => false,
    }
}

fn pg_transient(e: &tokio_postgres::Error) -> bool {
    if e.is_closed() {
        return true;
    }
    match e.code() {
        Some(code) => {
            let c = code.code();
            ["40", "08", "53", "57", "58"]
                .iter()
                .any(|p| c.starts_with(p))
                || c == "55P03"
        }
        None => true,
    }
}

/// A server refusal as [`crate::target::Rejected`], anything else as transient.
fn classify(transient: bool, msg: String) -> anyhow::Error {
    if transient {
        anyhow!(msg)
    } else {
        anyhow::Error::new(crate::target::Rejected(msg))
    }
}

pub struct Credentials {
    pub user: Option<String>,
    pub password: Option<String>,
}

impl Credentials {
    fn resolve(&self) -> Result<(Option<String>, Option<String>)> {
        Ok((
            self.user
                .as_deref()
                .map(secrets::resolve)
                .transpose()
                .context("vars.username")?,
            self.password
                .as_deref()
                .map(secrets::resolve)
                .transpose()
                .context("vars.password")?,
        ))
    }
}

// ---- MySQL ----------------------------------------------------------------------------

pub struct MySqlClient {
    pub addr: SqlAddr,
    tls: TlsSpec,
    creds: Credentials,
    conn: Mutex<Option<mysql_async::Conn>>,
}

impl MySqlClient {
    pub fn new(addr: SqlAddr, tls: &TlsSpec, creds: Credentials) -> MySqlClient {
        MySqlClient {
            addr,
            tls: tls.clone(),
            creds,
            conn: Mutex::new(None),
        }
    }

    async fn open(&self, timeout: Duration) -> Result<mysql_async::Conn> {
        let (user, pass) = self.creds.resolve()?;
        let mut b = mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.addr.host.clone())
            .tcp_port(self.addr.port)
            .db_name(Some(self.addr.db.clone()))
            .user(user)
            .pass(pass)
            .prefer_socket(false);
        let mode = self.tls.effective(false);
        if mode != Mode::Disabled {
            let mut ssl = mysql_async::SslOpts::default().with_root_certs(
                self.tls
                    .ca
                    .iter()
                    .map(|c| std::path::PathBuf::from(c).into())
                    .collect(),
            );
            match mode {
                Mode::Required => ssl = ssl.with_danger_accept_invalid_certs(true),
                Mode::VerifyCa => ssl = ssl.with_danger_skip_domain_validation(true),
                _ => {}
            }
            b = b.ssl_opts(ssl);
        }
        let mut conn = tokio::time::timeout(timeout, mysql_async::Conn::new(b))
            .await
            .map_err(|_| {
                anyhow!(
                    "connecting to {}:{} timed out",
                    self.addr.host,
                    self.addr.port
                )
            })??;
        conn.query_drop("SET NAMES utf8mb4").await?;
        conn.query_drop(
            "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '')",
        )
        .await?;
        let mode: Option<String> = conn.query_first("SELECT @@SESSION.sql_mode").await?;
        if mode
            .unwrap_or_default()
            .to_ascii_uppercase()
            .contains("NO_BACKSLASH_ESCAPES")
        {
            bail!("the session keeps NO_BACKSLASH_ESCAPES (init_connect?): sql_quote would not be safe");
        }
        Ok(conn)
    }

    async fn with_conn<T>(
        &self,
        timeout: Duration,
        f: impl for<'c> FnOnce(
            &'c mut mysql_async::Conn,
        ) -> futures_util::future::BoxFuture<
            'c,
            std::result::Result<T, mysql_async::Error>,
        >,
    ) -> Result<T> {
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            *guard = Some(self.open(timeout).await?);
        }
        let conn = guard.as_mut().expect("open");
        match tokio::time::timeout(timeout, f(conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                let transient = mysql_transient(&e);
                if transient {
                    *guard = None;
                }
                Err(classify(
                    transient,
                    format!("mysql {}/{}: {e}", self.addr.host, self.addr.db),
                ))
            }
            Err(_) => {
                *guard = None;
                Err(anyhow!("no answer within {} ms", timeout.as_millis()))
            }
        }
    }

    /// Runs the statement; returns the affected rows.
    pub async fn exec(&self, sql: String, timeout: Duration) -> Result<u64> {
        self.with_conn(timeout, |c| {
            Box::pin(async move {
                c.query_drop(sql).await?;
                Ok(c.affected_rows())
            })
        })
        .await
    }

    /// Runs a query; returns its rows as objects.
    pub async fn rows(&self, sql: String, timeout: Duration) -> Result<Value> {
        let rows: Vec<mysql_async::Row> = self
            .with_conn(timeout, |c| Box::pin(async move { c.query(sql).await }))
            .await?;
        Ok(Value::Array(
            rows.into_iter()
                .map(|r| {
                    let cols = r.columns();
                    let mut m = Map::new();
                    for (i, v) in r.unwrap().into_iter().enumerate() {
                        m.insert(
                            cols[i].name_str().into_owned(),
                            crate::docs::to_json(v, cols.get(i)),
                        );
                    }
                    Value::Object(m)
                })
                .collect(),
        ))
    }
}

// ---- PostgreSQL ---------------------------------------------------------------------------

#[derive(Clone)]
struct PgTls {
    cfg: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
}

struct PgTlsConnect {
    cfg: Arc<rustls::ClientConfig>,
    name: ServerName<'static>,
}

pub struct PgTlsStream(tokio_rustls::client::TlsStream<tokio_postgres::Socket>);

impl tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> for PgTls {
    type Stream = PgTlsStream;
    type TlsConnect = PgTlsConnect;
    type Error = io::Error;

    fn make_tls_connect(&mut self, domain: &str) -> io::Result<PgTlsConnect> {
        let name = self
            .server_name
            .clone()
            .unwrap_or_else(|| domain.to_string());
        let name = ServerName::try_from(name)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(PgTlsConnect {
            cfg: self.cfg.clone(),
            name,
        })
    }
}

impl tokio_postgres::tls::TlsConnect<tokio_postgres::Socket> for PgTlsConnect {
    type Stream = PgTlsStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn std::future::Future<Output = io::Result<PgTlsStream>> + Send>>;

    fn connect(self, s: tokio_postgres::Socket) -> Self::Future {
        Box::pin(async move {
            let t = tokio_rustls::TlsConnector::from(self.cfg)
                .connect(self.name, s)
                .await?;
            Ok(PgTlsStream(t))
        })
    }
}

impl tokio_postgres::tls::TlsStream for PgTlsStream {
    fn channel_binding(&self) -> tokio_postgres::tls::ChannelBinding {
        tokio_postgres::tls::ChannelBinding::none()
    }
}

impl AsyncRead for PgTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for PgTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

pub struct PgClient {
    pub addr: SqlAddr,
    tls: TlsSpec,
    creds: Credentials,
    client: Mutex<Option<tokio_postgres::Client>>,
}

impl PgClient {
    pub fn new(addr: SqlAddr, tls: &TlsSpec, creds: Credentials) -> PgClient {
        PgClient {
            addr,
            tls: tls.clone(),
            creds,
            client: Mutex::new(None),
        }
    }

    async fn open(&self, timeout: Duration) -> Result<tokio_postgres::Client> {
        let (user, pass) = self.creds.resolve()?;
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&self.addr.host)
            .port(self.addr.port)
            .dbname(&self.addr.db)
            .application_name("lazywf")
            .connect_timeout(timeout);
        if let Some(u) = user {
            cfg.user(u);
        }
        if let Some(p) = pass {
            cfg.password(p);
        }
        let mode = self.tls.effective(false);
        let client = if mode == Mode::Disabled {
            let (client, conn) = cfg.connect(tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::debug!("postgres connection: {e}");
                }
            });
            client
        } else {
            cfg.ssl_mode(tokio_postgres::config::SslMode::Require);
            let tls = PgTls {
                cfg: tls::client_config(&self.tls, mode, &[])?,
                server_name: self.tls.server_name.clone(),
            };
            let (client, conn) = cfg.connect(tls).await?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::debug!("postgres connection: {e}");
                }
            });
            client
        };
        client.batch_execute("SET client_encoding = 'UTF8'").await?;
        let scs = client
            .query_one("SHOW standard_conforming_strings", &[])
            .await?;
        if scs.get::<_, String>(0) != "on" {
            bail!("standard_conforming_strings is off: sql_quote would not be safe");
        }
        client
            .batch_execute(&format!("SET statement_timeout = {}", timeout.as_millis()))
            .await?;
        Ok(client)
    }

    /// Runs the statement (extended protocol: one statement only); returns the affected rows.
    pub async fn exec(&self, sql: &str, timeout: Duration) -> Result<u64> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(self.open(timeout).await?);
        }
        let client = guard.as_ref().expect("open");
        match tokio::time::timeout(timeout, client.execute(sql, &[])).await {
            Ok(Ok(n)) => Ok(n),
            Ok(Err(e)) => {
                let transient = pg_transient(&e);
                if transient {
                    *guard = None;
                }
                let detail = e.as_db_error().map_or_else(
                    || e.to_string(),
                    |d| format!("{}: {}", d.code().code(), d.message()),
                );
                Err(classify(
                    transient,
                    format!("postgres {}/{}: {detail}", self.addr.host, self.addr.db),
                ))
            }
            Err(_) => {
                *guard = None;
                Err(anyhow!("no answer within {} ms", timeout.as_millis()))
            }
        }
    }

    /// Runs a query (extended protocol: one statement only); returns its rows as objects.
    pub async fn rows(&self, sql: &str, timeout: Duration) -> Result<Value> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(self.open(timeout).await?);
        }
        let client = guard.as_ref().expect("open");
        let rows = match tokio::time::timeout(timeout, client.query(sql, &[])).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                if pg_transient(&e) {
                    *guard = None;
                }
                bail!("postgres {}/{}: {e}", self.addr.host, self.addr.db);
            }
            Err(_) => {
                *guard = None;
                bail!("no answer within {} ms", timeout.as_millis());
            }
        };
        Ok(Value::Array(
            rows.iter()
                .map(|r| {
                    let mut obj = Map::new();
                    for (i, c) in r.columns().iter().enumerate() {
                        obj.insert(c.name().to_string(), pg_value(r, i));
                    }
                    Value::Object(obj)
                })
                .collect(),
        ))
    }
}

/// A column of a PostgreSQL row as JSON (text, integer, float and boolean types; cast others
/// to `text` in the query).
fn pg_value(r: &tokio_postgres::Row, i: usize) -> Value {
    if let Ok(v) = r.try_get::<_, Option<String>>(i) {
        return v.map_or(Value::Null, Value::String);
    }
    if let Ok(v) = r.try_get::<_, Option<i64>>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = r.try_get::<_, Option<i32>>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = r.try_get::<_, Option<i16>>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = r.try_get::<_, Option<f64>>(i) {
        return v
            .and_then(serde_json::Number::from_f64)
            .map_or(Value::Null, Value::Number);
    }
    if let Ok(v) = r.try_get::<_, Option<bool>>(i) {
        return v.map_or(Value::Null, Value::Bool);
    }
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        let q = |d, v: Value| quote(d, &v).unwrap();
        assert_eq!(
            q(Dialect::MySql, json!("it's a \\ test")),
            "'it''s a \\\\ test'"
        );
        assert_eq!(
            q(Dialect::Postgres, json!("it's a \\ test")),
            "'it''s a \\ test'"
        );
        assert_eq!(q(Dialect::Postgres, json!(12.5)), "'12.5'");
        assert_eq!(q(Dialect::MySql, json!(true)), "'1'");
        assert_eq!(q(Dialect::MySql, Value::Null), "NULL");
        assert_eq!(q(Dialect::Postgres, json!({"a": [1]})), "'{\"a\":[1]}'");
        assert!(quote(Dialect::MySql, &json!("a\u{0}b")).is_err());
        assert_eq!(ident(Dialect::MySql, "we`ird").unwrap(), "`we``ird`");
        assert_eq!(
            ident(Dialect::Postgres, "we\"ird").unwrap(),
            "\"we\"\"ird\""
        );
    }

    #[test]
    fn statements() {
        assert_eq!(
            single_statement("  INSERT INTO t VALUES ('a;b');  ")
                .unwrap()
                .as_deref(),
            Some("INSERT INTO t VALUES ('a;b')")
        );
        assert!(single_statement("DELETE FROM t; DROP TABLE t").is_err());
        assert_eq!(single_statement(" \n ").unwrap(), None);
    }

    #[test]
    fn addresses() {
        assert_eq!(
            SqlAddr::parse("db:3307/sink", Dialect::MySql).unwrap(),
            SqlAddr {
                host: "db".into(),
                port: 3307,
                db: "sink".into()
            }
        );
        assert_eq!(
            SqlAddr::parse("postgres://pg/app", Dialect::Postgres)
                .unwrap()
                .port,
            5432
        );
        assert!(SqlAddr::parse("db:3306", Dialect::MySql).is_err());
        assert!(SqlAddr::parse("postgres://u:p@pg/app", Dialect::Postgres).is_err());
        assert!(SqlAddr::parse("mysql://db/app", Dialect::Postgres).is_err());
    }
}
