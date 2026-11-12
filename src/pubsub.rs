//! The `pubsub` kind: Google Cloud Pub/Sub topics. One action is one `topics.publish` call.
//!
//! - Endpoint `gc-pubsub://<project>/<topic>`; `?endpoint=http(s)://host:port` replaces the
//!   service address (the emulator). A URL carrying userinfo is a load error.
//! - Credentials: `vars.credentials_ref` (a service-account key, read on each use), else
//!   Application Default Credentials ([`crate::gcp`]). With `?endpoint=` and no reference, no
//!   credentials are sent (the emulator takes anonymous requests).
//! - The request is rendered by the template: base64 `data`, `attributes` `key` and `op`, and
//!   `orderingKey` = the key only when `vars.ordering` is true.
//! - Outcomes are whole-action. 2xx with a `messageIds` list as long as the messages is met
//!   (`expect` then decides); a shorter list fails closed (transient). 400 `INVALID_ARGUMENT`
//!   is the only refusal. Every other non-2xx answer, and any transport error, is transient.
//! - Append-only: a stray cleanup, a purge-like or a non-`:publish` request is refused.
//! - Ordering: `vars.ordering` puts the key in `orderingKey`. A failed publish is classified
//!   like any other (P5), and the retry publishes again with the same key. There is no resume
//!   step: `resumePublish` is a client-library method, not a REST method (lazywf publishes over
//!   REST). Lane order (one lane per rule) keeps the newest state last across retries.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use mysql_async::Pool;
use serde_json::{json, Map, Value};

use crate::gcp::{Auth, Source};
use crate::http::{BaseUrl, Client as HttpClient, RawRequest};
use crate::model::{FirePer, RuleDef, StepKind, TargetDef};
use crate::response::Expect;
use crate::target::{rejected, Sent};

const SCHEME: &str = "gc-pubsub://";
const DEFAULT_SERVICE: &str = "https://pubsub.googleapis.com";
/// `topics.publish` takes at most 1000 messages and 10 MB per request.
pub const MAX_MESSAGES: usize = 1000;
pub const MAX_REQUEST_BYTES: usize = 10_000_000;

/// The parts of a `gc-pubsub://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub project: String,
    pub topic: String,
    /// `?endpoint=`: the service address (emulators, private endpoints).
    pub endpoint: Option<String>,
}

impl Addr {
    pub fn parse(url: &str) -> Result<Addr> {
        let rest = url
            .strip_prefix(SCHEME)
            .ok_or_else(|| anyhow!("a pubsub endpoint starts with {SCHEME}"))?;
        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (rest, None),
        };
        if path.contains('@') {
            bail!("a pubsub endpoint carries no userinfo: name the key in `vars.credentials_ref`");
        }
        let parts: Vec<&str> = path.split('/').collect();
        let [project, topic] = parts.as_slice() else {
            bail!("the pubsub endpoint is {SCHEME}<project>/<topic>");
        };
        name_ok("project", project)?;
        name_ok("topic", topic)?;
        let mut endpoint = None;
        if let Some(q) = query {
            for kv in q.split('&') {
                let (k, v) = kv
                    .split_once('=')
                    .ok_or_else(|| anyhow!("the pubsub endpoint query is `endpoint=<url>`"))?;
                match k {
                    "endpoint" => {
                        if !(v.starts_with("http://") || v.starts_with("https://")) {
                            bail!("`?endpoint=` must be an http:// or https:// address");
                        }
                        if v.contains('@') {
                            bail!("`?endpoint=` carries no userinfo");
                        }
                        endpoint = Some(v.trim_end_matches('/').to_string());
                    }
                    other => bail!("unknown pubsub endpoint query key `{other}`"),
                }
            }
        }
        Ok(Addr {
            project: project.to_string(),
            topic: topic.to_string(),
            endpoint,
        })
    }

    /// `/v1/projects/{p}/topics/{t}:publish`: the only request this target sends.
    pub fn publish_path(&self) -> String {
        format!(
            "/v1/projects/{}/topics/{}:publish",
            self.project, self.topic
        )
    }
}

fn name_ok(what: &str, s: &str) -> Result<()> {
    let ok = !s.is_empty()
        && s.len() <= 255
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'~' | b'+' | b'-'));
    if !ok {
        bail!("the pubsub {what} name `{s}` has characters a URL path must not carry");
    }
    Ok(())
}

/// Load-time checks of a `pubsub` target (called from `TargetDef::from_row`).
pub fn check(vars: &Map<String, Value>, endpoints: &[String]) -> Result<()> {
    let [url] = endpoints else {
        bail!("kind pubsub takes exactly one endpoint: gc-pubsub://<project>/<topic>");
    };
    Addr::parse(url)?;
    if let Some(r) = vars.get("credentials_ref").and_then(Value::as_str) {
        if !crate::secrets::is_reference(r) {
            bail!("`vars.credentials_ref` must be a reference (`env:NAME` or `file:/path`), never a literal");
        }
    }
    if let Some(v) = vars.get("ordering") {
        if !v.is_boolean() {
            bail!("`vars.ordering` must be true or false");
        }
    }
    Ok(())
}

/// A `gc-pubsub://` endpoint is only for the kind pubsub.
pub fn refuse_scheme(kind_is_pubsub: bool, endpoints: &[String]) -> Result<()> {
    if !kind_is_pubsub && endpoints.iter().any(|e| e.starts_with(SCHEME)) {
        bail!("a {SCHEME} endpoint needs kind pubsub");
    }
    Ok(())
}

/// Rule-load refusals for a `pubsub` rule (called from `rule::kind_errors`).
pub fn rule_errors(def: &RuleDef) -> Vec<String> {
    let mut out = Vec::new();
    if def.fire_per == FirePer::Batch {
        match def.batch_max_keys {
            None => out.push(format!(
                "kind pubsub needs `batch_max_keys` (1 to {MAX_MESSAGES}: messages per publish)"
            )),
            Some(n) if n > MAX_MESSAGES => out.push(format!(
                "`batch_max_keys` {n} is over {MAX_MESSAGES}, the most messages a publish takes"
            )),
            Some(_) => {}
        }
        if def.batch_max_bytes.is_some_and(|n| n > MAX_REQUEST_BYTES) {
            out.push(format!(
                "`batch_max_bytes` is over {MAX_REQUEST_BYTES} (10 MB), the most a publish takes"
            ));
        }
    }
    for s in &def.reset_steps {
        match &s.kind {
            StepKind::Stray { .. } => {} // refused for every append-only kind (kind_errors)
            StepKind::Once | StepKind::Snapshot => {
                let text = s.template.as_deref().unwrap_or("").to_ascii_lowercase();
                let publish_only = !text.contains("topics/") || text.contains(":publish");
                let bad = [":seek", ":detach", "delete", "purge", "subscriptions/"]
                    .iter()
                    .any(|w| text.contains(w));
                if bad || !publish_only {
                    out.push(format!(
                        "reset step `{}`: pubsub targets only publish (a seek, detach, delete or purge is refused)",
                        s.name
                    ));
                }
            }
        }
    }
    out
}

/// What the rendered publish body holds, once checked.
#[derive(Debug, PartialEq, Eq)]
pub struct Publish {
    pub messages: usize,
    /// The `orderingKey`s of the messages (empty when ordering is off).
    pub ordering_keys: Vec<String>,
}

/// Checks a rendered `topics.publish` body: at most 1000 messages and 10 MB, each with base64
/// `data` and `attributes` `key` and `op`. `Err` is a refusal of the rendering.
pub fn check_body(body: &[u8]) -> Result<Publish, String> {
    if body.len() > MAX_REQUEST_BYTES {
        return Err(format!(
            "the publish request is {} bytes, over {MAX_REQUEST_BYTES}",
            body.len()
        ));
    }
    let v: Value =
        serde_json::from_slice(body).map_err(|e| format!("the publish body is not JSON: {e}"))?;
    let msgs = v
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "the publish body has no `messages` array".to_string())?;
    if msgs.len() > MAX_MESSAGES {
        return Err(format!("{} messages, over {MAX_MESSAGES}", msgs.len()));
    }
    let mut ordering_keys = Vec::new();
    for (i, m) in msgs.iter().enumerate() {
        let data = m.get("data").and_then(Value::as_str);
        let decoded = data.map(|d| base64::engine::general_purpose::STANDARD.decode(d));
        if !matches!(decoded, Some(Ok(_))) {
            return Err(format!("message {i}: `data` is not base64"));
        }
        let attrs = m.get("attributes").and_then(Value::as_object);
        for k in ["key", "op"] {
            if attrs
                .and_then(|a| a.get(k))
                .and_then(Value::as_str)
                .is_none()
            {
                return Err(format!("message {i}: attribute `{k}` is missing"));
            }
        }
        match m.get("orderingKey") {
            None => {}
            Some(Value::String(k)) => ordering_keys.push(k.clone()),
            Some(_) => return Err(format!("message {i}: `orderingKey` is not a string")),
        }
    }
    Ok(Publish {
        messages: msgs.len(),
        ordering_keys,
    })
}

/// What a non-2xx publish answer means for the action (`refused` = the service refused this
/// request for good; otherwise it is transient). Google's error envelope carries `error.status`.
pub fn refused(status: u16, body: &[u8]) -> bool {
    if status != 400 {
        return false;
    }
    let api = error_status(body);
    api.is_empty() || api == "INVALID_ARGUMENT"
}

fn error_status(body: &[u8]) -> String {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| {
            v.pointer("/error/status")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default()
}

/// True when a 2xx publish answer names fewer messages than were sent (fail closed).
pub fn short_answer(body: &[u8], sent: usize) -> bool {
    let ids = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("messageIds").and_then(Value::as_array).map(Vec::len))
        .unwrap_or(0);
    ids < sent
}

/// One `pubsub` endpoint: the service address and the token.
pub struct Client {
    pub addr: Addr,
    http: HttpClient,
    host_header: String,
    auth: Option<Auth>,
}

impl Client {
    pub fn new(def: &TargetDef) -> Result<Client> {
        let url = def
            .endpoints
            .first()
            .ok_or_else(|| anyhow!("kind pubsub needs gc-pubsub://<project>/<topic>"))?;
        let addr = Addr::parse(url)?;
        let base = BaseUrl::parse(addr.endpoint.as_deref().unwrap_or(DEFAULT_SERVICE))
            .context("the pubsub service address")?;
        let host_header = base.host_header();
        let auth = match (
            def.vars.get("credentials_ref").and_then(Value::as_str),
            &addr.endpoint,
        ) {
            (Some(r), _) => Some(Auth::new(Source::Key(r.to_string()))),
            (None, None) => Some(Auth::new(Source::Adc)),
            (None, Some(_)) => None,
        };
        let http = HttpClient::new(base, &def.tls)?;
        Ok(Client {
            addr,
            http,
            host_header,
            auth,
        })
    }

    /// The endpoint's template context: `endpoint.project` / `endpoint.topic`, plus the address.
    pub fn ctx(&self) -> Value {
        json!({
            "host": self.http.base.host,
            "port": self.http.base.port,
            "url": self.http.base.url(),
            "project": self.addr.project,
            "topic": self.addr.topic,
        })
    }

    /// Headers every request carries: User-Agent, Host, and the bearer token when there is one.
    async fn prepare(&self, req: &mut RawRequest) -> Result<()> {
        if req.header("user-agent").is_none() {
            req.set_header(
                "User-Agent",
                format!("lazywf/{}", env!("CARGO_PKG_VERSION")),
            );
        }
        if req.header("host").is_none() {
            req.set_header("Host", self.host_header.clone());
        }
        if let Some(a) = &self.auth {
            let token = a.token().await?;
            req.set_header("Authorization", format!("Bearer {token}"));
        }
        Ok(())
    }
}

/// Sends one rendered publish action to a `pubsub` endpoint (see the module docs).
pub async fn send(
    c: &Client,
    rendered: &str,
    timeout: Duration,
    expect: &Expect,
    pool: &Pool,
) -> Result<Sent> {
    let req = match RawRequest::parse(rendered) {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(Sent::Skipped),
        Err(e) => return Err(rejected(format!("the rendered request: {e:#}"))),
    };
    let path = c.addr.publish_path();
    if req.method != "POST" || req.target != path {
        return Err(rejected(format!(
            "pubsub targets only publish: {} {} is not POST {path}",
            req.method, req.target
        )));
    }
    let publish =
        check_body(&req.body).map_err(|e| rejected(format!("the publish request: {e}")))?;
    if publish.messages == 0 {
        return Ok(Sent::Skipped);
    }
    publish_once(c, &req.body, &publish, timeout, expect, pool).await
}

async fn publish_once(
    c: &Client,
    body: &[u8],
    publish: &Publish,
    timeout: Duration,
    expect: &Expect,
    pool: &Pool,
) -> Result<Sent> {
    let mut req = RawRequest {
        method: "POST".into(),
        target: c.addr.publish_path(),
        headers: Vec::new(),
        body: body.to_vec(),
    };
    req.set_header("Content-Type", "application/json".into());
    c.prepare(&mut req).await?;
    let resp = c
        .http
        .send(&req, timeout)
        .await
        .with_context(|| format!("POST {}{}", c.http.base.url(), req.target))?;
    if resp.status == 401 {
        if let Some(a) = &c.auth {
            a.invalidate();
        }
    }
    if (200..300).contains(&resp.status) {
        // Fail closed: a short answer is not known to be stored.
        if short_answer(&resp.body, publish.messages) {
            bail!(
                "publish answered fewer message ids than the {} messages sent: {}",
                publish.messages,
                resp.body_snippet()
            );
        }
        return match expect
            .http(pool, resp.status, &resp.headers, &resp.body)
            .await?
        {
            Ok(()) => Ok(Sent::Ok),
            Err(why) => Err(rejected(format!("publish: {why}"))),
        };
    }
    let msg = format!(
        "POST {}{}: status {}: {}",
        c.http.base.url(),
        req.target,
        resp.status,
        resp.body_snippet()
    );
    Err(if refused(resp.status, &resp.body) {
        rejected(msg)
    } else {
        anyhow!(msg)
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ordering_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A stand-in service: answers the requests in order with the scripted (status, body) and
    /// records each request line. Every answer closes its connection.
    async fn stand_in(script: Vec<(u16, &'static str)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            let mut script = script.into_iter();
            while let Ok((mut s, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = s.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..p]).to_string();
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let l = l.to_ascii_lowercase();
                            l.strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if buf.len() >= p + 4 + len {
                        break;
                    }
                }
                let line = String::from_utf8_lossy(&buf)
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string();
                log.lock().unwrap().push(line);
                let (status, body) = script.next().unwrap_or((500, "{}"));
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.shutdown().await;
            }
        });
        (addr, seen)
    }

    #[tokio::test]
    async fn an_ordered_retry_is_the_same_publish_with_no_resume_call() {
        let (addr, seen) = stand_in(vec![
            (500, r#"{"error":{"code":500,"status":"INTERNAL"}}"#),
            (200, r#"{"messageIds":["1"]}"#), // the retry, with the same orderingKey
        ])
        .await;
        let url = format!("http://{addr}");
        let c = Client {
            addr: Addr {
                project: "p".into(),
                topic: "t".into(),
                endpoint: Some(url.clone()),
            },
            http: HttpClient::new(
                BaseUrl::parse(&url).unwrap(),
                &crate::tls::TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
            host_header: addr.clone(),
            auth: None,
        };
        let rendered = "POST /v1/projects/p/topics/t:publish HTTP/1.1\nContent-Type: application/json\n\n{\"messages\":[{\"data\":\"e30=\",\"attributes\":{\"key\":\"1\",\"op\":\"upsert\"},\"orderingKey\":\"1\"}]}";
        let expect = Expect::new(None).unwrap();
        let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
        let first = send(&c, rendered, Duration::from_secs(5), &expect, &pool).await;
        let err = first.expect_err("the 500 is transient");
        assert!(!crate::target::is_rejected(&err));
        let second = send(&c, rendered, Duration::from_secs(5), &expect, &pool).await;
        assert_eq!(second.unwrap(), Sent::Ok);
        let line = |m: &str| format!("POST /v1/projects/p/topics/t:{m} HTTP/1.1");
        assert_eq!(
            seen.lock().unwrap().clone(),
            vec![line("publish"), line("publish")],
            "a retry must not call resumePublish"
        );
    }
}

#[cfg(test)]
mod body_and_rule_limits_tests;
