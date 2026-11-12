//! The `smtp` kind: one mail per action to an SMTP server. Client: lettre
//! (`default-features = false`, rustls-ring only; see Cargo.toml).
//!
//! # Endpoint and credentials
//!
//! `smtp://host[:port]` (STARTTLS, port 587 by default) or `smtps://host[:port]` (implicit
//! TLS, 465). No credentials in the URL: `vars.username` and `vars.password` (a reference or a
//! literal, as `redis` and `amqp` take them) authenticate with PLAIN, then LOGIN, whichever
//! the server offers first; without a username the session is anonymous.
//!
//! # The rendered action
//!
//! One message per action (`fire_per = key`; `batch` is refused at load):
//!
//! ```text
//! From: sender@example.com
//! To: a@example.com, b@example.com
//! Cc: c@example.com
//! Subject: one line
//! Reply-To: other@example.com
//! Content-Type: text/plain; charset=utf-8
//!
//! <body>
//! ```
//!
//! The envelope sender is `From`; the envelope recipients are `To` plus `Cc` (`vars.recipients`
//! many at most, 50 by default; a Bcc has no envelope, so the header is refused). Addresses are
//! one line each, `user@domain`, comma-separated (never folded). A header line with a control
//! character is refused, as http does with a request line; the headers are `From`, `To`, `Cc`,
//! `Subject`, `Reply-To` and `Content-Type` only, each once, and `Content-Type` must be
//! `text/plain` or `text/html` (an optional `charset` parameter must be `utf-8`).
//!
//! # Outcomes
//!
//! - a 2xx after the message: sent. `expect` then sees a row with the reply code as `status`
//!   and the reply as `text` (no `expect`: met).
//! - a 4xx (421 and 451 among them: throttling, greylisting): transient, with backoff.
//! - a 5xx: refused (rejected: bisect, strikes).
//! - a connection, a TLS or a STARTTLS failure: transient.
//!
//! # Concurrency
//!
//! One connection per endpoint, held by the endpoint's lock: a send holds it until the reply
//! is read. The connection is kept while the server accepts; every other answer (a 4xx/5xx,
//! a broken or a timed-out session) drops it, and the next send opens a new one. The cost is
//! one connect and one EHLO per refused message, per endpoint; accepted messages reuse the
//! connection.
//!
//! # TLS
//!
//! `tls` maps onto lettre's rustls parameters: `verify-full` (the default for both schemes)
//! checks the chain against the Mozilla roots and the `tls.ca` files, and the host name;
//! `verify-ca` checks the chain only; `required` encrypts without checking the certificate;
//! `disabled` skips TLS — on `smtp://` only (no STARTTLS): `smtps://` with `disabled` is a
//! load error.

use std::error::Error as _;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use lettre::address::{Address, Envelope};
use lettre::transport::smtp::authentication::{Credentials, DEFAULT_MECHANISMS};
use lettre::transport::smtp::client::AsyncSmtpConnection;
use lettre::transport::smtp::client::{Certificate, TlsParameters, TlsParametersBuilder};
use lettre::transport::smtp::extension::ClientId;
use lettre::transport::smtp::response::Response;
use lettre::transport::smtp::Error as SmtpError;
use mysql_async::Pool;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex;

use crate::model::{FirePer, TargetDef};
use crate::response::{Expect, Row, Uses};
use crate::secrets;
use crate::target::{rejected, var, Sent};
use crate::tls::{Mode, TlsSpec};

/// The most recipients a message takes when `vars.recipients` is unset.
pub const MAX_RECIPIENTS: usize = 50;

/// An endpoint: `smtp(s)://host[:port]`, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    /// `smtps://`: implicit TLS from the connection.
    pub implicit: bool,
    pub host: String,
    pub port: u16,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (implicit, rest) = if let Some(r) = s.strip_prefix("smtps://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("smtp://") {
            (false, r)
        } else {
            bail!("endpoint `{shown}`: use smtp://host:port or smtps://host:port");
        };
        if rest.contains('@') {
            bail!("endpoint `{shown}`: credentials belong in vars.username and vars.password, not in the URL");
        }
        if rest.contains(['/', '?', '#']) {
            bail!(
                "endpoint `{shown}`: no path, query string or fragment (TLS is the target's `tls`)"
            );
        }
        if rest.starts_with('[') {
            bail!("endpoint `{shown}`: IPv6 addresses are not supported");
        }
        let default_port = if implicit { 465 } else { 587 };
        let (host, port) = match rest.rsplit_once(':') {
            Some((h, p)) => (
                h,
                p.parse::<u16>()
                    .with_context(|| format!("endpoint `{shown}`: port `{p}`"))?,
            ),
            None => (rest, default_port),
        };
        if host.is_empty() {
            bail!("endpoint `{shown}`: no host");
        }
        Ok(Addr {
            implicit,
            host: host.to_string(),
            port,
        })
    }
}

/// The lettre TLS parameters for the target's `tls` and the endpoint host:
/// `None` when the mode is `disabled` (a plain connection, STARTTLS off).
pub fn tls_parameters(spec: &TlsSpec, host: &str) -> Result<Option<TlsParameters>> {
    if spec.server_name.is_some() {
        bail!(
            "`tls.server_name` is not supported by kind smtp: the endpoint's host name is checked"
        );
    }
    match spec.effective(true) {
        Mode::Disabled => Ok(None),
        Mode::Auto | Mode::VerifyFull | Mode::VerifyCa | Mode::Required => {
            let mut b = TlsParametersBuilder::new(host.to_string());
            for path in &spec.ca {
                let pem =
                    std::fs::read(path).with_context(|| format!("reading CA file `{path}`"))?;
                let cert = Certificate::from_pem(&pem)
                    .with_context(|| format!("parsing CA file `{path}`"))?;
                b = b.add_root_certificate(cert);
            }
            b = match spec.effective(true) {
                Mode::VerifyCa => b.dangerous_accept_invalid_hostnames(true),
                Mode::Required => b
                    .dangerous_accept_invalid_certs(true)
                    .dangerous_accept_invalid_hostnames(true),
                _ => b,
            };
            Ok(Some(b.build().context("`tls`")?))
        }
    }
}

/// `vars.recipients`: the most recipients one message takes (1 or more).
fn recipients_limit(vars: &Map<String, Value>) -> Result<usize> {
    let n: u64 = match vars.get("recipients") {
        None | Some(Value::Null) => return Ok(MAX_RECIPIENTS),
        Some(Value::String(s)) => s
            .trim()
            .parse::<u64>()
            .with_context(|| format!("`vars.recipients` is `{s}`, must be a number"))?,
        Some(Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| anyhow!("`vars.recipients` must be a whole number"))?,
        Some(other) => bail!("`vars.recipients` must be a number, not {other}"),
    };
    ensure!(n >= 1, "`vars.recipients` must be 1 or more");
    Ok(n as usize)
}

/// Load checks for a target of kind smtp (`TargetDef::from_row`): each endpoint, the TLS mode
/// for each endpoint's scheme, and the credentials pair.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    let spec = TlsSpec::parse(tls).context("`tls`")?;
    recipients_limit(vars)?;
    match (
        var(vars, "username").is_some(),
        var(vars, "password").is_some(),
    ) {
        (true, false) => bail!("kind smtp needs both `vars.username` and `vars.password`"),
        (false, true) => bail!("kind smtp needs both `vars.username` and `vars.password`"),
        _ => {}
    }
    for e in endpoints {
        let a = Addr::parse(e)?;
        if a.implicit && tls_parameters(&spec, &a.host)?.is_none() {
            bail!("an smtps endpoint needs TLS, but `tls` says disabled");
        }
    }
    Ok(())
}

/// Load checks for a rule whose target is smtp: one message per action (`fire_per = key`).
pub fn rule_errors(fire_per: FirePer) -> Vec<String> {
    if fire_per == FirePer::Batch {
        vec!["kind smtp sends one message per action: set `fire_per = key`".into()]
    } else {
        Vec::new()
    }
}

/// The rendered message of one action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The whole message as rendered (the headers, the blank line and the body), the bytes that
    /// go on the wire (CRLF-normalized): nothing of it is rebuilt.
    pub raw: String,
    /// The envelope sender (`From`).
    pub from: String,
    /// The envelope recipients (`To`).
    pub to: Vec<String>,
    /// The envelope recipients (`Cc`).
    pub cc: Vec<String>,
    pub subject: Option<String>,
    pub reply_to: Option<String>,
    /// `text/plain` or `text/html`, with an optional `charset` parameter (must be `utf-8`).
    pub content_type: Option<String>,
    /// The body, after the blank line (trailing line endings dropped).
    pub body: String,
}

impl Message {
    /// Parses a rendered action (the form in the module docs). `Ok(None)`: nothing rendered.
    pub fn parse(text: &str, limit: usize) -> Result<Option<Message>> {
        let text = text.trim_start_matches(['\r', '\n', ' ', '\t']);
        if text.trim().is_empty() {
            return Ok(None);
        }
        let mut rest = text;
        let mut headers: Vec<&str> = Vec::new();
        let mut from = None;
        let mut to: Vec<String> = Vec::new();
        let mut cc: Vec<String> = Vec::new();
        let mut subject = None;
        let mut reply_to = None;
        let mut ctype = None;
        loop {
            let (l, tail) = line(rest);
            rest = tail;
            if l.trim().is_empty() {
                break;
            }
            check_line(l)?;
            if l.starts_with([' ', '\t']) {
                bail!("a folded header line: headers are one line each");
            }
            let (name, value) = l
                .split_once(':')
                .ok_or_else(|| anyhow!("header `{l}` has no `:`"))?;
            let name = name.trim();
            let value = value.trim();
            match name.to_ascii_lowercase().as_str() {
                "from" => {
                    ensure_once("From", from.is_none())?;
                    from = Some(one_address(name, value)?);
                }
                "to" => {
                    ensure_once("To", to.is_empty())?;
                    to = address_list(name, value)?;
                }
                "cc" => {
                    ensure_once("Cc", cc.is_empty())?;
                    cc = address_list(name, value)?;
                }
                "subject" => {
                    ensure_once("Subject", subject.is_none())?;
                    subject = Some(value.to_string());
                }
                "reply-to" => {
                    ensure_once("Reply-To", reply_to.is_none())?;
                    reply_to = Some(one_address(name, value)?);
                }
                "content-type" => {
                    ensure_once("Content-Type", ctype.is_none())?;
                    ctype = Some(check_content_type(value)?);
                }
                other => bail!(
                    "unknown header `{other}` (the headers are From, To, Cc, Subject, Reply-To and Content-Type)"
                ),
            }
            headers.push(l);
        }
        let body = rest.trim_end_matches(['\r', '\n']);
        ensure!(!body.trim().is_empty(), "the message has no body");
        let from = from.ok_or_else(|| anyhow!("the message needs a `From` header"))?;
        ensure!(!to.is_empty(), "the message needs a `To` header");
        let recipients = to.len() + cc.len();
        ensure!(
            recipients <= limit,
            "the message has {recipients} recipients (To + Cc), over `vars.recipients` ({limit})"
        );
        let mut raw = headers.join("\n");
        raw.push_str("\n\n");
        raw.push_str(body);
        Ok(Some(Message {
            raw,
            from,
            to,
            cc,
            subject,
            reply_to,
            content_type: ctype,
            body: body.to_string(),
        }))
    }

    fn describe(&self) -> String {
        format!(
            "from `{}` to {} recipient(s)",
            self.from,
            self.to.len() + self.cc.len()
        )
    }
}

/// `text/plain` or `text/html`, with an optional `charset` parameter that must be `utf-8`.
/// The header is kept as written.
fn check_content_type(v: &str) -> Result<String> {
    let mut parts = v.split(';');
    let media = parts.next().unwrap();
    for param in parts {
        let (k, val) = param
            .trim()
            .split_once('=')
            .ok_or_else(|| anyhow!("the Content-Type parameter `{param}` has no value"))?;
        match k.trim().to_ascii_lowercase().as_str() {
            "charset" => {
                ensure!(
                    val.trim().eq_ignore_ascii_case("utf-8"),
                    "the Content-Type charset is `{}`, must be utf-8",
                    val.trim()
                );
            }
            other => bail!("unknown Content-Type parameter `{other}`"),
        }
    }
    match media.trim().to_ascii_lowercase().as_str() {
        "text/plain" | "text/html" => Ok(v.to_string()),
        other => bail!("the Content-Type `{other}` must be text/plain or text/html"),
    }
}

fn ensure_once(what: &str, first: bool) -> Result<()> {
    ensure!(first, "header `{what}` is more than once");
    Ok(())
}

/// One address: `user@domain`, one line (lettre's `Address` validation).
fn one_address(name: &str, v: &str) -> Result<String> {
    let a = v
        .parse::<Address>()
        .map_err(|e| anyhow!("the {name} address `{v}`: {e}"))?;
    Ok(a.to_string())
}

/// A comma-separated list of addresses.
fn address_list(name: &str, v: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for a in v.split(',') {
        let a = a.trim();
        ensure!(!a.is_empty(), "the {name} list has an empty address");
        out.push(one_address(name, a)?);
    }
    Ok(out)
}

/// One line without its line ending, and the rest.
fn line(s: &str) -> (&str, &str) {
    let (l, tail) = match s.find('\n') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, ""),
    };
    (l.strip_suffix('\r').unwrap_or(l), tail)
}

fn check_line(l: &str) -> Result<()> {
    ensure!(
        !l.bytes().any(|c| (c < b' ' && c != b'\t') || c == 0x7f),
        "a control character in a header line of the message"
    );
    Ok(())
}

/// What the server said about one message, by reply class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// Accepted (2xx after the message).
    Accepted { code: i64, text: String },
    /// A transient negative completion (4xx: throttling, greylisting, 421).
    Transient { code: i64, text: String },
    /// A permanent negative completion (5xx).
    Refused { code: i64, text: String },
}

impl Reply {
    /// The engine's reading of the reply.
    pub fn outcome(&self) -> Result<(), Outcome> {
        match self {
            Reply::Accepted { .. } => Ok(()),
            Reply::Transient { code, text } => {
                Err(Outcome::Transient(format!("reply {code}: {text}")))
            }
            Reply::Refused { code, text } => Err(Outcome::Refused(format!("reply {code}: {text}"))),
        }
    }

    /// The row `expect` reads (`status`: the reply code, `text`: the reply; see the module
    /// docs).
    pub fn row(&self, uses: Uses) -> Row {
        let (code, text) = match self {
            Reply::Accepted { code, text }
            | Reply::Transient { code, text }
            | Reply::Refused { code, text } => (*code, text),
        };
        Row {
            status: uses.status.then_some(code),
            text: uses.text.then(|| text.to_string()),
            ..Row::default()
        }
    }
}

/// The engine's reading of a [`Reply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Refused for this message (rejected: bisect, strikes).
    Refused(String),
    /// Not known to have gone through: retry.
    Transient(String),
}

/// The reply of one `send` (a 2xx, a 4xx or a 5xx), read from lettre's result.
fn reply_of(r: Result<Response, SmtpError>) -> Result<Reply, SmtpError> {
    match r {
        Ok(resp) => Ok(Reply::Accepted {
            code: code_i64(&resp.code()),
            text: resp.message().collect::<Vec<_>>().join(" "),
        }),
        Err(e) => {
            let text = e
                .source()
                .map(|s| s.to_string())
                .unwrap_or_else(|| e.to_string());
            Ok(match e.status() {
                Some(c) if e.is_transient() => Reply::Transient {
                    code: code_i64(&c),
                    text,
                },
                Some(c) => Reply::Refused {
                    code: code_i64(&c),
                    text,
                },
                None => return Err(e),
            })
        }
    }
}

fn code_i64(c: &lettre::transport::smtp::response::Code) -> i64 {
    i64::from(c.severity as u8) * 100 + i64::from(c.category as u8) * 10 + i64::from(c.detail as u8)
}

/// One endpoint of a target.
pub struct Client {
    /// The endpoint as written (no credentials): the name in errors.
    pub url: String,
    addr: Addr,
    /// `vars.username` / `vars.password`: references or literals, resolved on each connect.
    user: Option<String>,
    password: Option<String>,
    /// The TLS parameters (both for the implicit connection and for STARTTLS); `None`:
    /// plain TCP on `smtp://` only.
    tls: Option<TlsParameters>,
    /// The most recipients one message takes (`vars.recipients`).
    pub recipients: usize,
    timeout: Duration,
    hello: ClientId,
    /// The connection. Holding the lock also keeps one send in flight; a negative reply or a
    /// broken session is dropped, so a kept connection always accepted its last message.
    session: Mutex<Option<AsyncSmtpConnection>>,
}

impl Client {
    /// The endpoint's context (`endpoint` in templates) and its client.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint)?;
        let tls = tls_parameters(&def.tls, &addr.host)?;
        if addr.implicit && tls.is_none() {
            bail!("an smtps endpoint needs TLS, but `tls` says disabled");
        }
        let (user, password) = match (var(&def.vars, "username"), var(&def.vars, "password")) {
            (Some(u), Some(p)) => (Some(u.to_string()), Some(p.to_string())),
            (None, None) => (None, None),
            _ => bail!("kind smtp needs both `vars.username` and `vars.password`"),
        };
        let recipients = recipients_limit(&def.vars)?;
        let ctx = json!({"host": addr.host, "port": addr.port, "url": endpoint.trim()});
        Ok((
            ctx,
            Client {
                url: endpoint.trim().to_string(),
                addr,
                user,
                password,
                tls,
                recipients,
                timeout: def.timeout,
                hello: ClientId::default(),
                session: Mutex::new(None),
            },
        ))
    }

    /// Delivers `msg` and reads the reply. `Err`: a connection, a TLS, a STARTTLS or a
    /// protocol failure (the session is broken): transient.
    pub async fn deliver(&self, msg: &Message) -> Result<Reply> {
        let mut slot = self.session.lock().await;
        let r = match tokio::time::timeout(self.timeout, self.deliver_on(&mut slot, msg)).await {
            Ok(r) => r,
            Err(_) => Err(anyhow!("no reply within {:?}", self.timeout)),
        };
        // A kept connection accepted its last message; a 4xx/5xx aborts the connection
        // (lettre marks it broken), as does any failure.
        if !matches!(r, Ok(Reply::Accepted { .. })) {
            *slot = None;
        }
        match r {
            Err(e) => Err(e.context("the SMTP session")),
            Ok(reply) => Ok(reply),
        }
    }

    async fn deliver_on(
        &self,
        slot: &mut Option<AsyncSmtpConnection>,
        msg: &Message,
    ) -> Result<Reply> {
        if slot
            .as_ref()
            .is_some_and(|c| c.has_broken() || c.is_encrypted() != self.wants_encryption())
        {
            *slot = None;
        }
        if slot.is_none() {
            *slot = Some(self.open().await?);
        }
        let conn = slot.as_mut().expect("a session just opened");
        let from = msg.from.parse::<Address>()?;
        let mut to = msg
            .to
            .iter()
            .map(|a| a.parse::<Address>())
            .collect::<Result<Vec<_>, _>>()?;
        to.extend(
            msg.cc
                .iter()
                .map(|a| a.parse::<Address>())
                .collect::<Result<Vec<_>, _>>()?,
        );
        let envelope = Envelope::new(Some(from), to).with_context(|| "the envelope")?;
        let bytes = crlf(&msg.raw);
        Ok(reply_of(conn.send(&envelope, &bytes).await)?)
    }

    fn wants_encryption(&self) -> bool {
        self.tls.is_some()
    }

    async fn open(&self) -> Result<AsyncSmtpConnection> {
        let implicit = self.tls.clone().filter(|_| self.addr.implicit);
        let starttls = self.tls.clone().filter(|_| !self.addr.implicit);
        let conn = AsyncSmtpConnection::connect_tokio1(
            (self.addr.host.as_str(), self.addr.port),
            Some(self.timeout),
            &self.hello,
            implicit,
            None,
        )
        .await
        .with_context(|| format!("connecting to {}", self.url))?;
        let conn = match starttls {
            Some(t) => {
                let mut c = conn;
                c.starttls(t, &self.hello)
                    .await
                    .with_context(|| format!("STARTTLS with {}", self.url))?;
                c
            }
            None => conn,
        };
        let conn = match (&self.user, &self.password) {
            (Some(u), Some(p)) => {
                let mut c = conn;
                let user = secrets::resolve(u).context("vars.username")?;
                let password = secrets::resolve(p).context("vars.password")?;
                c.auth(DEFAULT_MECHANISMS, &Credentials::new(user, password))
                    .await
                    .with_context(|| format!("AUTH with {}", self.url))?;
                c
            }
            _ => conn,
        };
        Ok(conn)
    }
}

/// CRLF line endings, the wire form of an SMTP message.
fn crlf(s: &str) -> Vec<u8> {
    s.replace("\r\n", "\n").replace('\n', "\r\n").into_bytes()
}

/// Sends one rendered action to `client` and checks `expect` on an acceptance.
pub async fn send(client: &Client, rendered: &str, expect: &Expect, pool: &Pool) -> Result<Sent> {
    let msg = match Message::parse(rendered, client.recipients) {
        Ok(Some(m)) => m,
        Ok(None) => return Ok(Sent::Skipped),
        Err(e) => return Err(rejected(format!("the rendered message: {e:#}"))),
    };
    let reply = client
        .deliver(&msg)
        .await
        .with_context(|| format!("{} {}", client.url, msg.describe()))?;
    if let Err(why) = reply.outcome() {
        let ctx = format!("{} {}", client.url, msg.describe());
        return match why {
            Outcome::Refused(why) => Err(rejected(format!("{ctx}: {why}"))),
            Outcome::Transient(why) => bail!("{ctx}: {why}"),
        };
    }
    expect
        .row(pool, |u| reply.row(u))
        .await
        .with_context(|| format!("{} {}", client.url, msg.describe()))?
        .map_err(|e| rejected(format!("{} {}: {e}", client.url, msg.describe())))?;
    Ok(Sent::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RawTarget;

    fn spec(s: &str) -> TlsSpec {
        TlsSpec::parse(Some(s)).unwrap()
    }

    fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
        RawTarget {
            name: "mail".into(),
            kind: Some("smtp".into()),
            endpoints: Some(endpoints.into()),
            tls: tls.map(Into::into),
            timeout_ms: None,
            expect: None,
            vars: Some(vars.into()),
        }
    }

    #[test]
    fn rendered_form() {
        let m = Message::parse(
            "From: sender@example.com\nTo: a@example.com, b@example.com\nCc: c@example.com\n\
             Subject: Order 1\nReply-To: other@example.com\nContent-Type: text/plain; charset=utf-8\n\n\
             {\"key\":\"1\",\"doc\":{\"sku\":\"A-1\"}}\n",
            50,
        )
        .unwrap()
        .unwrap();
        assert_eq!(m.from, "sender@example.com");
        assert_eq!(m.to, vec!["a@example.com", "b@example.com"]);
        assert_eq!(m.cc, vec!["c@example.com"]);
        assert_eq!(m.subject.as_deref(), Some("Order 1"));
        assert_eq!(m.reply_to.as_deref(), Some("other@example.com"));
        assert_eq!(m.content_type.as_deref(), Some("text/plain; charset=utf-8"));
        assert_eq!(m.body, r#"{"key":"1","doc":{"sku":"A-1"}}"#);

        // CRLF, and the minimal form.
        let m = Message::parse("From: a@x.com\r\nTo: b@x.com\r\n\r\n{}\r\n", 50)
            .unwrap()
            .unwrap();
        assert_eq!((m.subject, m.cc, m.content_type), (None, Vec::new(), None));
        assert_eq!(m.body, "{}");

        // Header names are case-insensitive, in any order.
        let m = Message::parse("to: b@x.com\nFROM: a@x.com\n\nbody", 50)
            .unwrap()
            .unwrap();
        assert_eq!(m.from, "a@x.com");
        assert_eq!(m.to, vec!["b@x.com"]);

        assert!(Message::parse(" \n\t", 50).unwrap().is_none());
    }

    #[test]
    fn malformed_messages_are_refused() {
        for bad in [
            // unknown / repeated / missing headers
            "From: a@x.com\nTo: b@x.com\nX-Evil: 1\n\nbody",
            "From: a@x.com\nFrom: c@x.com\nTo: b@x.com\n\nbody",
            "To: b@x.com\n\nbody",
            "From: a@x.com\n\nbody",
            "From: a@x.com\nTo: b@x.com\n",
            "From: a@x.com\nTo: b@x.com",
            // addresses
            "From: a\nTo: b@x.com\n\nbody",
            "From: @x.com\nTo: b@x.com\n\nbody",
            "From: a@x.com\nTo: , b@x.com\n\nbody",
            "From: a@x.com\nTo: b@x.com,\n\nbody",
            "From: a@b@c.com\nTo: b@x.com\n\nbody",
            // control characters and folding: a header is one line
            "From: a@x.com\nTo: b@x.com\nX\n\nbody",
            "From: a@x.com\n To: b@x.com\n\nbody",
            "From: a@x.com\nTo: b@x.com\n\0\n\nbody",
            // content types
            "From: a@x.com\nTo: b@x.com\nContent-Type: application/json\n\nbody",
            "From: a@x.com\nTo: b@x.com\nContent-Type: text/plain; charset=ascii\n\nbody",
            "From: a@x.com\nTo: b@x.com\nContent-Type: text/plain; name=x\n\nbody",
            "From: a@x.com\nTo: b@x.com\nContent-Type: text/plain;\n\nbody",
        ] {
            assert!(Message::parse(bad, 50).is_err(), "accepted {bad:?}");
        }
        // text/html is a content type of its own.
        assert!(Message::parse(
            "From: a@x.com\nTo: b@x.com\nContent-Type: text/html\n\n<body>",
            50
        )
        .is_ok());
        // a control character in the body is fine: only the header lines are checked.
        assert!(Message::parse("From: a@x.com\nTo: b@x.com\n\nbody\u{1}", 50).is_ok());
    }

    #[test]
    fn the_recipient_limit() {
        let many = |n: usize| -> String {
            let list = (0..n)
                .map(|i| format!("m{i}@x.com"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("From: a@x.com\nTo: {list}\n\nbody")
        };
        assert!(Message::parse(&many(50), 50).is_ok());
        assert!(Message::parse(&many(51), 50).is_err());
        // To + Cc count together.
        let m = "From: a@x.com\nTo: b@x.com\nCc: c@x.com\n\nbody";
        assert!(Message::parse(m, 1).is_err());
        assert!(Message::parse(&many(10), 10).is_ok());
        let e = Message::parse(&many(51), 50).unwrap_err().to_string();
        assert!(e.contains("51") && e.contains("vars.recipients"), "{e}");
    }

    #[test]
    fn the_wire_form() {
        // The message goes on the wire as rendered (CRLF-normalized): nothing is rebuilt.
        let src = "From: a@x.com\nTo: b@x.com, c@x.com\nSubject: s\n\
                   Content-Type: text/plain; charset=utf-8\n\nbody\n";
        let m = Message::parse(src, 50).unwrap().unwrap();
        assert_eq!(m.raw, "From: a@x.com\nTo: b@x.com, c@x.com\nSubject: s\nContent-Type: text/plain; charset=utf-8\n\nbody");
        assert_eq!(crlf(&m.raw), b"From: a@x.com\r\nTo: b@x.com, c@x.com\r\nSubject: s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nbody");
        assert_eq!(crlf("a\nb\r\nc\r\n"), b"a\r\nb\r\nc\r\n");
        assert_eq!(crlf("\n"), b"\r\n");
    }

    #[test]
    fn reply_classes() {
        use lettre::transport::smtp::response::{Category, Code, Detail, Severity};
        let c = |s, c, d| Code {
            severity: s,
            category: c,
            detail: d,
        };
        let p = Severity::PositiveCompletion;
        let t = Severity::TransientNegativeCompletion;
        let pe = Severity::PermanentNegativeCompletion;
        let cat = Category::MailSystem;
        assert_eq!(code_i64(&c(p, cat, Detail::Zero)), 250);
        assert_eq!(code_i64(&c(t, cat, Detail::One)), 451);
        assert_eq!(code_i64(&c(pe, cat, Detail::One)), 551);
        let a = Reply::Accepted {
            code: 250,
            text: "2.0.0 Ok: queued".into(),
        };
        assert!(a.outcome().is_ok());
        let tr = Reply::Transient {
            code: 451,
            text: "try later".into(),
        };
        assert!(matches!(tr.outcome(), Err(Outcome::Transient(_))));
        let rf = Reply::Refused {
            code: 550,
            text: "no such user".into(),
        };
        match rf.outcome() {
            Err(Outcome::Refused(w)) => assert!(w.contains("550") && w.contains("no such user")),
            _ => panic!("{rf:?}"),
        }
        // The row `expect` reads: the code as `status`, the reply as `text`.
        let all = Uses {
            status: true,
            headers: false,
            body: false,
            text: true,
            affected_rows: false,
        };
        let r = a.row(all);
        assert_eq!(
            (r.status, r.text.as_deref()),
            (Some(250), Some("2.0.0 Ok: queued"))
        );
        let r = rf.row(all);
        assert_eq!(
            (r.status, r.text.as_deref()),
            (Some(550), Some("no such user"))
        );
        let r = a.row(Uses::default());
        assert_eq!((r.status, r.text), (None, None));
    }

    #[test]
    fn endpoints() {
        let a = Addr::parse("smtp://mail").unwrap();
        assert_eq!((a.implicit, a.host.as_str(), a.port), (false, "mail", 587));
        let a = Addr::parse("smtps://mail.example:465").unwrap();
        assert_eq!((a.implicit, a.port), (true, 465));
        assert_eq!(Addr::parse("smtps://b").unwrap().port, 465);
        assert_eq!(Addr::parse("smtp://h:25").unwrap().port, 25);
        for bad in [
            "http://h",
            "smtp://u:p@h",
            "smtp://h/p",
            "smtp://h?x=1",
            "smtp://h#f",
            "smtp://h:x",
            "smtp://[::1]",
            "smtp://",
            "smtps://:465",
        ] {
            assert!(Addr::parse(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn tls_modes_map_or_refuse() {
        // auto: verified, both schemes; the CA files are parsed.
        assert!(tls_parameters(&spec(""), "h").unwrap().is_some());
        assert!(tls_parameters(&spec("verify-full"), "h").unwrap().is_some());
        assert!(tls_parameters(&spec("verify-ca"), "h").unwrap().is_some());
        assert!(tls_parameters(&spec("required"), "h").unwrap().is_some());
        assert!(tls_parameters(&spec("off"), "h").unwrap().is_none());
        let e = match tls_parameters(&spec(r#"{"mode": "verify-full", "server_name": "b"}"#), "h") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("accepted tls.server_name"),
        };
        assert!(e.contains("server_name"), "{e}");
        assert!(tls_parameters(&spec(r#"{"ca": "/nonexistent/lwf-ca.pem"}"#), "h").is_err());
    }

    #[test]
    fn the_recipients_limit_var() {
        fn v(s: &str) -> Map<String, Value> {
            serde_json::from_str(s).unwrap()
        }
        assert_eq!(recipients_limit(&v("{}")).unwrap(), MAX_RECIPIENTS);
        assert_eq!(recipients_limit(&v(r#"{"recipients": 10}"#)).unwrap(), 10);
        assert_eq!(recipients_limit(&v(r#"{"recipients": "7"}"#)).unwrap(), 7);
        assert!(recipients_limit(&v(r#"{"recipients": 0}"#)).is_err());
        assert!(recipients_limit(&v(r#"{"recipients": "x"}"#)).is_err());
    }

    #[test]
    fn from_row_checks() {
        let ok = TargetDef::from_row(&raw("smtp://mail.example", None, "{}")).unwrap();
        assert_eq!(ok.kind, crate::model::Kind::Smtp);
        // Anonymous, or both: the pair check.
        assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"username": "u"}"#)).is_err());
        assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"password": "p"}"#)).is_err());
        assert!(TargetDef::from_row(&raw(
            "smtp://m",
            None,
            r#"{"username": "u", "password": "env:X"}"#
        ))
        .is_ok());
        // vars.recipients.
        assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"recipients": "10"}"#)).is_ok());
        assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"recipients": "0"}"#)).is_err());
        assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"recipients": "x"}"#)).is_err());
        // userinfo is never in the URL.
        assert!(TargetDef::from_row(&raw("smtp://u:p@mail", None, "{}")).is_err());
        // smtps with disabled tls is a load error; smtp with it is plain.
        assert!(TargetDef::from_row(&raw("smtps://m", Some("off"), "{}")).is_err());
        assert!(TargetDef::from_row(&raw("smtp://m", Some("off"), "{}")).is_ok());
        let e = TargetDef::from_row(&raw("smtps://m", Some("off"), "{}"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("disabled"), "{e}");
        assert!(crate::model::Kind::Smtp.append_only());
    }

    #[test]
    fn batch_is_refused() {
        assert_eq!(rule_errors(FirePer::Batch).len(), 1);
        assert!(rule_errors(FirePer::Key).is_empty());
    }
}

#[cfg(test)]
mod reply_row_tests {
    use super::*;

    fn vars(s: &str) -> Map<String, Value> {
        serde_json::from_str(s).unwrap()
    }

    /// A 4xx is the server saying "not now": the row it is checked against has to
    /// carry its code and text like a 2xx or a 5xx does, or an `expect` that reads
    /// `status` sees an empty row and the key is decided from nothing.
    #[test]
    fn a_transient_reply_is_a_row_of_its_own() {
        let all = Uses {
            status: true,
            text: true,
            ..Uses::default()
        };
        let tr = Reply::Transient {
            code: 451,
            text: "4.7.1 greylisted, try again later".into(),
        };
        let r = tr.row(all);
        assert_eq!(
            (r.status, r.text.as_deref()),
            (Some(451), Some("4.7.1 greylisted, try again later"))
        );
        // The columns are only filled when named, and nothing else of `r` exists.
        let r = tr.row(Uses {
            status: true,
            ..Uses::default()
        });
        assert_eq!(
            (r.status, r.text, r.body, r.affected_rows),
            (Some(451), None, None, None)
        );
        // …and the engine reads it as a retry, not as a refusal of this key.
        assert!(matches!(tr.outcome(), Err(Outcome::Transient(_))));
    }

    /// A `vars.recipients` of the wrong shape is refused at load: read as "no limit",
    /// a message with a thousand recipients would be refused by the server for every
    /// key of the batch.
    #[test]
    fn a_recipients_limit_that_is_not_a_number_is_refused() {
        for bad in [
            r#"{"recipients": true}"#,
            r#"{"recipients": []}"#,
            r#"{"recipients": {"n": 3}}"#,
        ] {
            let e = format!("{:#}", recipients_limit(&vars(bad)).err().unwrap());
            assert!(
                e.contains("`vars.recipients`") && e.contains("number"),
                "{bad}: {e}"
            );
        }
        // A JSON number that is not a whole number is refused by name too.
        let e = format!(
            "{:#}",
            recipients_limit(&vars(r#"{"recipients": 2.5}"#))
                .err()
                .unwrap()
        );
        assert!(e.contains("whole number"), "{e}");
        assert!(recipients_limit(&vars(r#"{"recipients": null}"#)).is_ok());
    }
}
