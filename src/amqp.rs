//! The `amqp` kind: one message per action to a RabbitMQ (AMQP 0-9-1) broker, with publisher
//! confirms. Client: lapin (`default-features = false`, ring only; see Cargo.toml).
//!
//! # Endpoint and credentials
//!
//! `amqp://host[:port][/vhost]`, or `amqps://` for TLS (default ports 5672 / 5671). The vhost is
//! the path, percent-decoded: empty or `/` is the default vhost. No credentials in the URL:
//! `vars.username` and `vars.password` (references or literals, as `redis` takes them).
//!
//! # The rendered action
//!
//! One message per action (`fire_per = key`; `batch` is refused at load):
//!
//! ```text
//! PUBLISH /<exchange>/<routing-key>
//! Message-Id: <key>
//!
//! <body: one JSON value, the envelope the template renders>
//! ```
//!
//! The exchange is the path segment before the second `/` (empty for the default exchange:
//! `PUBLISH //queue`); the routing key is everything after it. Names are taken as written (no
//! escaping). The message is persistent, `content-type: application/json`, `mandatory`, and its
//! message id is the key. Lines end in LF or CRLF; the only header is `Message-Id`. Exchange,
//! routing key and message id are AMQP short strings: at most 255 bytes each.
//!
//! # Outcomes
//!
//! - ack: sent. `expect` then sees the row `status` 200, `text` `ack` (no `expect`: met).
//! - ack with basic.return (reply 312 NO_ROUTE, no queue bound): refused (rejected). Its row is
//!   `status` 312, `text` `return`, `body` `{"reply_code", "reply_text"}`.
//! - nack, a failed publish, a lost connection, a missing confirm or a timeout: transient. The
//!   message may or may not have reached the broker, so it is sent again (at-least-once).
//!
//! # Concurrency
//!
//! One session (connection and channel) per endpoint, and one publish in flight on it: a
//! publish holds the endpoint's lock until its confirm arrives. Concurrent sends to one endpoint
//! queue behind each other, and different endpoints run in parallel. lapin can hand a confirm
//! to the wrong publish when several are in flight on one channel (lapin #487). The cost is one
//! round trip per message, per endpoint.
//!
//! # Reconnects
//!
//! `enable_auto_recover` is not used (lapin #481: a mandatory publish can be acknowledged
//! before its binding is restored). A failed publish drops the session, and the next send opens
//! a new one; the retry is the engine's transient path.
//!
//! # TLS
//!
//! `tls` maps onto lapin's rustls setup: `verify-full` (the default for `amqps://`) checks the
//! chain against the Mozilla roots and the `tls.ca` files, and the host name. Modes lapin cannot
//! express are load errors that name them: `required` and `verify-ca` (the name is checked),
//! `tls.server_name`, and `disabled` on `amqps://`.

use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::tcp::OwnedTLSConfig;
use lapin::uri::{AMQPAuthority, AMQPQueryString, AMQPScheme, AMQPUri, AMQPUserInfo};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties};
use mysql_async::Pool;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex;

use crate::model::{FirePer, TargetDef};
use crate::response::{Expect, Row, Uses};
use crate::secrets;
use crate::target::{rejected, var, Sent};
use crate::tls::{Mode, TlsSpec};

/// AMQP short strings hold at most 255 octets.
const SHORT_STR: usize = 255;

/// An endpoint: `amqp(s)://host[:port][/vhost]`, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub secure: bool,
    pub host: String,
    pub port: u16,
    pub vhost: String,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("amqps://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("amqp://") {
            (false, r)
        } else {
            bail!("endpoint `{shown}`: use amqp://host:port/vhost or amqps://host:port/vhost");
        };
        if rest.contains('@') {
            bail!("endpoint `{shown}`: credentials belong in vars.username and vars.password, not in the URL");
        }
        if rest.contains(['?', '#']) {
            bail!("endpoint `{shown}`: no query string or fragment (TLS is the target's `tls`)");
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        if authority.starts_with('[') {
            bail!("endpoint `{shown}`: IPv6 addresses are not supported");
        }
        let default_port = if secure { 5671 } else { 5672 };
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h,
                p.parse::<u16>()
                    .with_context(|| format!("endpoint `{shown}`: port `{p}`"))?,
            ),
            None => (authority, default_port),
        };
        if host.is_empty() {
            bail!("endpoint `{shown}`: no host");
        }
        let vhost = pct_decode(path.trim_end_matches('/')).context("endpoint vhost")?;
        Ok(Addr {
            secure,
            host: host.to_string(),
            port,
            vhost: if vhost.is_empty() {
                "/".to_string()
            } else {
                vhost
            },
        })
    }
}

fn pct_decode(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = b
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| anyhow!("bad percent-escape in `{s}`"))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).context("not UTF-8")
}

/// The lapin TLS settings for an endpoint (`None`: plain TCP). See the module docs for the modes
/// that are refused.
pub fn tls_config(spec: &TlsSpec, secure: bool) -> Result<Option<OwnedTLSConfig>> {
    if spec.server_name.is_some() {
        bail!("`tls.server_name` is not supported by kind amqp: the broker's host name is checked");
    }
    match spec.effective(secure) {
        Mode::Disabled => {
            if secure {
                bail!("an amqps endpoint needs TLS, but `tls` says disabled");
            }
            Ok(None)
        }
        Mode::VerifyFull => {
            // The same loader as the other kinds validates the CA files first.
            crate::tls::client_config(spec, Mode::VerifyFull, &[]).context("`tls`")?;
            let mut chain = String::new();
            for path in &spec.ca {
                let pem = std::fs::read_to_string(path)
                    .with_context(|| format!("reading CA file `{path}`"))?;
                chain.push_str(&pem);
                chain.push('\n');
            }
            Ok(Some(OwnedTLSConfig {
                identity: None,
                cert_chain: (!chain.is_empty()).then_some(chain),
            }))
        }
        Mode::Required => bail!(
            "tls mode `required` is not supported by kind amqp: use verify-full (lapin always checks the chain and the host name)"
        ),
        Mode::VerifyCa => bail!(
            "tls mode `verify-ca` is not supported by kind amqp: use verify-full (the host name is checked too)"
        ),
        Mode::Auto => bail!("tls mode `auto` was not resolved"),
    }
}

/// Load checks for a target of kind amqp (`TargetDef::from_row`): the credentials, each
/// endpoint, and the TLS mode for each endpoint's scheme.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    for k in ["username", "password"] {
        ensure!(var(vars, k).is_some(), "kind amqp needs `vars.{k}`");
    }
    let tls = TlsSpec::parse(tls).context("`tls`")?;
    for e in endpoints {
        let a = Addr::parse(e)?;
        tls_config(&tls, a.secure)?;
    }
    Ok(())
}

/// Load checks for a rule whose target is amqp: one message per action (`fire_per = key`).
pub fn rule_errors(fire_per: FirePer) -> Vec<String> {
    if fire_per == FirePer::Batch {
        vec!["kind amqp sends one message per action: set `fire_per = key`".into()]
    } else {
        Vec::new()
    }
}

/// The rendered message of one action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub exchange: String,
    pub routing_key: String,
    pub message_id: String,
    pub body: Vec<u8>,
}

impl Message {
    /// Parses a rendered action (the form in the module docs). `Ok(None)`: nothing rendered.
    pub fn parse(text: &str) -> Result<Option<Message>> {
        let text = text.trim_start_matches(['\r', '\n', ' ', '\t']);
        if text.trim().is_empty() {
            return Ok(None);
        }
        let (first, mut rest) = line(text);
        check_line(first)?;
        let path = first
            .strip_prefix("PUBLISH ")
            .ok_or_else(|| anyhow!("the first line must be `PUBLISH /<exchange>/<routing-key>`"))?
            .trim();
        let path = path
            .strip_prefix('/')
            .ok_or_else(|| anyhow!("the path `{path}` must start with `/`"))?;
        let (exchange, routing_key) = path.split_once('/').ok_or_else(|| {
            anyhow!(
                "the path must be `/<exchange>/<routing-key>` (`//queue` is the default exchange)"
            )
        })?;
        let mut message_id = None;
        loop {
            ensure!(
                !rest.is_empty(),
                "the message has no blank line before its body"
            );
            let (l, tail) = line(rest);
            rest = tail;
            if l.trim().is_empty() {
                break;
            }
            check_line(l)?;
            let (name, value) = l
                .split_once(':')
                .ok_or_else(|| anyhow!("header `{l}` has no `:`"))?;
            ensure!(
                name.trim().eq_ignore_ascii_case("message-id"),
                "unknown header `{}` (the only header is Message-Id)",
                name.trim()
            );
            message_id = Some(value.trim().to_string());
        }
        let body = rest.trim_end_matches(['\r', '\n']);
        ensure!(!body.trim().is_empty(), "the message has no body");
        serde_json::from_str::<Value>(body).context("the body is not JSON")?;
        let message_id = message_id
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("the message needs a `Message-Id` header: the key"))?;
        for (what, s) in [
            ("exchange", exchange),
            ("routing key", routing_key),
            ("Message-Id", message_id.as_str()),
        ] {
            ensure!(
                s.len() <= SHORT_STR,
                "the {what} is over {SHORT_STR} bytes (an AMQP short string)"
            );
        }
        Ok(Some(Message {
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            message_id,
            body: body.as_bytes().to_vec(),
        }))
    }

    fn describe(&self) -> String {
        format!(
            "exchange `{}` key `{}` (message {})",
            self.exchange, self.routing_key, self.message_id
        )
    }
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
        "a control character in a line of the message"
    );
    Ok(())
}

/// What the broker said about one publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirm {
    /// Acknowledged: the broker took the message.
    Ack,
    /// Acknowledged but unroutable (basic.return).
    Return { reply_code: u16, reply_text: String },
    /// Negatively acknowledged: the broker could not take it now.
    Nack,
}

/// The engine's reading of a [`Confirm`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Sent,
    /// Refused for this message (rejected: bisect, strikes).
    Refused(String),
    /// Not known to have gone through: retry.
    Transient(String),
}

impl Confirm {
    pub fn outcome(&self) -> Outcome {
        match self {
            Confirm::Ack => Outcome::Sent,
            Confirm::Return {
                reply_code,
                reply_text,
            } => Outcome::Refused(format!(
                "basic.return {reply_code} {reply_text}: no queue is bound to the routing key"
            )),
            Confirm::Nack => Outcome::Transient("the broker nacked the message".into()),
        }
    }

    /// The row `expect` reads (`status`, `text`, `body`; see the module docs).
    pub fn row(&self, uses: Uses) -> Row {
        let (status, text, body) = match self {
            Confirm::Ack => (Some(200), "ack", None),
            Confirm::Return {
                reply_code,
                reply_text,
            } => (
                Some(i64::from(*reply_code)),
                "return",
                Some(json!({"reply_code": reply_code, "reply_text": reply_text}).to_string()),
            ),
            Confirm::Nack => (None, "nack", None),
        };
        Row {
            status: uses.status.then_some(status).flatten(),
            text: uses.text.then(|| text.to_string()),
            body: uses.body.then_some(body).flatten(),
            ..Row::default()
        }
    }
}

fn confirm(c: Confirmation) -> Result<Confirm> {
    Ok(match c {
        Confirmation::Ack(None) => Confirm::Ack,
        Confirmation::Ack(Some(r)) => Confirm::Return {
            reply_code: r.reply_code,
            reply_text: r.reply_text.to_string(),
        },
        Confirmation::Nack(_) => Confirm::Nack,
        Confirmation::NotRequested => bail!("the broker sent no publisher confirm"),
    })
}

/// A connection and its channel (publisher confirms on).
struct Session {
    conn: Connection,
    chan: Channel,
}

/// One endpoint of a target.
pub struct Client {
    /// The endpoint as written (no credentials): the name in errors.
    pub url: String,
    addr: Addr,
    /// `vars.username` and `vars.password`: references or literals, resolved on each connect.
    user: String,
    password: String,
    tls: Option<OwnedTLSConfig>,
    timeout: Duration,
    /// The session. Holding the lock also keeps one publish in flight (see the module docs).
    session: Mutex<Option<Session>>,
}

impl Client {
    /// The endpoint's context (`endpoint` in templates) and its client.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint)?;
        let tls = tls_config(&def.tls, addr.secure)?;
        let need = |k: &str| -> Result<String> {
            var(&def.vars, k)
                .map(str::to_string)
                .ok_or_else(|| anyhow!("kind amqp needs `vars.{k}`"))
        };
        let ctx = json!({"host": addr.host, "port": addr.port, "vhost": addr.vhost, "url": endpoint.trim()});
        Ok((
            ctx,
            Client {
                url: endpoint.trim().to_string(),
                user: need("username")?,
                password: need("password")?,
                addr,
                tls,
                timeout: def.timeout,
                session: Mutex::new(None),
            },
        ))
    }

    /// Publishes `msg` and waits for its confirm. `Err`: the outcome is unknown (the publish,
    /// the confirm, the connection or the time limit failed): transient.
    pub async fn publish(&self, msg: &Message) -> Result<Confirm> {
        let mut slot = self.session.lock().await;
        let r = match tokio::time::timeout(self.timeout, self.publish_on(&mut slot, msg)).await {
            Ok(r) => r,
            Err(_) => Err(anyhow!("no confirm within {:?}", self.timeout)),
        };
        if r.is_err() {
            *slot = None;
        }
        r
    }

    async fn publish_on(&self, slot: &mut Option<Session>, msg: &Message) -> Result<Confirm> {
        if slot
            .as_ref()
            .is_some_and(|s| !(s.conn.status().connected() && s.chan.status().connected()))
        {
            *slot = None;
        }
        if slot.is_none() {
            *slot = Some(self.open().await?);
        }
        let Some(s) = slot.as_ref() else {
            bail!("no session");
        };
        let props = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_message_id(msg.message_id.as_str().into());
        let opts = BasicPublishOptions {
            mandatory: true,
            ..Default::default()
        };
        let pending = s
            .chan
            .basic_publish(
                msg.exchange.as_str().into(),
                msg.routing_key.as_str().into(),
                opts,
                &msg.body,
                props,
            )
            .await
            .context("basic.publish")?;
        let c = pending.await.context("publisher confirm")?;
        confirm(c)
    }

    async fn open(&self) -> Result<Session> {
        let user = secrets::resolve(&self.user).context("vars.username")?;
        let password = secrets::resolve(&self.password).context("vars.password")?;
        let uri = AMQPUri {
            scheme: if self.addr.secure {
                AMQPScheme::AMQPS
            } else {
                AMQPScheme::AMQP
            },
            authority: AMQPAuthority {
                userinfo: AMQPUserInfo {
                    username: user,
                    password,
                },
                host: self.addr.host.clone(),
                port: self.addr.port,
            },
            vhost: self.addr.vhost.clone(),
            query: AMQPQueryString::default(),
        };
        let conn = Connection::connect_uri_with_config(
            uri,
            ConnectionProperties::default(),
            self.tls.clone().unwrap_or_default(),
            lapin::runtime::default_runtime()?,
        )
        .await
        .with_context(|| format!("connecting to {}", self.url))?;
        let chan = conn.create_channel().await.context("opening a channel")?;
        chan.confirm_select(ConfirmSelectOptions::default())
            .await
            .context("turning on publisher confirms")?;
        Ok(Session { conn, chan })
    }
}

/// Sends one rendered action to `client` and checks `expect` on an ack.
pub async fn send(client: &Client, rendered: &str, expect: &Expect, pool: &Pool) -> Result<Sent> {
    let msg = match Message::parse(rendered) {
        Ok(Some(m)) => m,
        Ok(None) => return Ok(Sent::Skipped),
        Err(e) => return Err(rejected(format!("the rendered message: {e:#}"))),
    };
    let c = client
        .publish(&msg)
        .await
        .with_context(|| format!("{} {}", client.url, msg.describe()))?;
    match c.outcome() {
        Outcome::Sent => {}
        Outcome::Refused(why) => {
            return Err(rejected(format!(
                "{} {}: {why}",
                client.url,
                msg.describe()
            )))
        }
        Outcome::Transient(why) => bail!("{} {}: {why}", client.url, msg.describe()),
    }
    expect
        .row(pool, |u| c.row(u))
        .await
        .with_context(|| format!("{} {}", client.url, msg.describe()))?
        .map_err(|e| rejected(format!("{} {}: {e}", client.url, msg.describe())))?;
    Ok(Sent::Ok)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod confirm_wire_tests {
    use super::*;
    use lapin::message::{BasicReturnMessage, Delivery};

    fn returned(code: u16, text: &str) -> BasicReturnMessage {
        BasicReturnMessage {
            delivery: Delivery::mock(
                1,
                "orders".into(),
                "orders/new".into(),
                false,
                b"{}".to_vec(),
            ),
            reply_code: code,
            reply_text: text.into(),
        }
    }

    /// What the broker's publisher confirm says about one publish. A `basic.return` is
    /// this routing key having no queue behind it: the same message would be returned
    /// again, so the key must take a strike rather than retry for good. A `nack` is the
    /// broker unable to store it now, which is no verdict on the message at all.
    #[test]
    fn the_broker_answer_becomes_the_engines_verdict() {
        assert_eq!(confirm(Confirmation::Ack(None)).unwrap(), Confirm::Ack);

        let c = confirm(Confirmation::Ack(Some(returned(312, "NO_ROUTE")))).unwrap();
        assert_eq!(
            c,
            Confirm::Return {
                reply_code: 312,
                reply_text: "NO_ROUTE".into()
            }
        );
        match c.outcome() {
            Outcome::Refused(why) => assert!(
                why.contains("312") && why.contains("NO_ROUTE") && why.contains("routing key"),
                "{why}"
            ),
            other => panic!("an unroutable message is refused, not retried: {other:?}"),
        }

        // A nack, with or without the message the broker also returned.
        assert_eq!(confirm(Confirmation::Nack(None)).unwrap(), Confirm::Nack);
        assert_eq!(
            confirm(Confirmation::Nack(Some(returned(541, "INTERNAL_ERROR")))).unwrap(),
            Confirm::Nack
        );
        assert!(matches!(
            confirm(Confirmation::Nack(None)).unwrap().outcome(),
            Outcome::Transient(_)
        ));

        // Publisher confirms were never turned on: there is no answer, and "no
        // answer" must never read as "the broker took it".
        let e = format!("{:#}", confirm(Confirmation::NotRequested).err().unwrap());
        assert!(e.contains("no publisher confirm"), "{e}");
    }
}
