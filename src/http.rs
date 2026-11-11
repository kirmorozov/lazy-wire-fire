//! HTTP/1.1: the raw request a template renders, and a small keep-alive client.
//!
//! A rendered request is a request line (`METHOD /path?query [HTTP/1.1]`), header lines, a
//! blank line and an optional body. Lines may end in `\n` or `\r\n`; text before the request
//! line that is only whitespace is ignored. The client sends it as HTTP/1.1, adding `Host`
//! (when missing), `Content-Length` (from the body) and the target's authentication.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::tls::{Connector, Stream, TlsSpec};

#[derive(Clone, PartialEq, Eq)]
pub struct RawRequest {
    pub method: String,
    /// Path and query, as written (`/a/b?x=1`).
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Header values whose names say credential (`Authorization`, `X-Amz-Security-Token`,
/// `X-Api-Key`, ...) are masked, and the body shows its length only: a request is data a
/// target receives, and a `{:?}` of it must not carry a credential.
impl std::fmt::Debug for RawRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(n, v)| {
                let l = n.to_ascii_lowercase();
                let secret = ["auth", "token", "secret", "key", "password", "cookie"]
                    .iter()
                    .any(|w| l.contains(w));
                (
                    n.as_str(),
                    if secret { "** redacted **" } else { v.as_str() },
                )
            })
            .collect();
        f.debug_struct("RawRequest")
            .field("method", &self.method)
            .field("target", &self.target)
            .field("headers", &headers)
            .field("body", &format_args!("{} bytes", self.body.len()))
            .finish()
    }
}

impl RawRequest {
    /// Parses rendered template output; `Ok(None)` when it is blank (nothing to send).
    pub fn parse(text: &str) -> Result<Option<RawRequest>> {
        let text = text.trim_start_matches(['\r', '\n', ' ', '\t']);
        if text.trim().is_empty() {
            return Ok(None);
        }
        let mut rest = text;
        let mut next_line = || -> Option<&str> {
            if rest.is_empty() {
                return None;
            }
            let (line, tail) = match rest.find('\n') {
                Some(i) => (&rest[..i], &rest[i + 1..]),
                None => (rest, ""),
            };
            rest = tail;
            Some(line.strip_suffix('\r').unwrap_or(line))
        };
        let line = next_line().unwrap_or_default();
        // A control character (a stray CR, a NUL…) in the request line or a header is never
        // meant: it is how data smuggles extra lines in.
        let control = |s: &str| s.bytes().any(|c| (c < b' ' && c != b'\t') || c == 0x7f);
        if control(line) {
            bail!("the request line holds a control character");
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let target = parts
            .next()
            .ok_or_else(|| anyhow!("request line `{line}` has no path"))?
            .to_string();
        if let Some(v) = parts.next() {
            if !v.starts_with("HTTP/") {
                bail!("request line `{line}`: `{v}` is not an HTTP version");
            }
        }
        if parts.next().is_some() {
            bail!("request line `{line}` has extra words (spaces in the path must be encoded)");
        }
        if !method
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            bail!("request line `{line}`: `{method}` is not a method");
        }
        if !(target.starts_with('/') || target == "*") {
            bail!("request line `{line}`: the path must start with `/`");
        }
        let mut headers = Vec::new();
        loop {
            match next_line() {
                None => break,
                Some("") => break,
                Some(h) => {
                    let (name, value) = h
                        .split_once(':')
                        .ok_or_else(|| anyhow!("header line `{h}` has no `:`"))?;
                    let name = name.trim();
                    if name.is_empty() || name.bytes().any(|c| c <= b' ' || c >= 0x7f) {
                        bail!("header line `{h}`: invalid header name");
                    }
                    if control(value) {
                        bail!("header `{name}` holds a control character");
                    }
                    headers.push((name.to_string(), value.trim().to_string()));
                }
            }
        }
        Ok(Some(RawRequest {
            method,
            target,
            headers,
            body: rest.as_bytes().to_vec(),
        }))
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn set_header(&mut self, name: &str, value: String) {
        self.headers.retain(|(n, _)| !n.eq_ignore_ascii_case(name));
        self.headers.push((name.to_string(), value));
    }

    pub fn remove_header(&mut self, name: &str) {
        self.headers.retain(|(n, _)| !n.eq_ignore_ascii_case(name));
    }

    /// Wire bytes. `Content-Length` and `Transfer-Encoding` from the template are replaced by
    /// the body's length.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_bytes_with(&self.target)
    }

    /// The same with the request line's target written as `target` (the way a secret endpoint's
    /// own path and query join it, see [`SecretUrl::target`]).
    pub fn to_bytes_with(&self, target: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(256 + self.body.len());
        out.extend_from_slice(format!("{} {target} HTTP/1.1\r\n", self.method).as_bytes());
        for (n, v) in &self.headers {
            if n.eq_ignore_ascii_case("content-length")
                || n.eq_ignore_ascii_case("transfer-encoding")
            {
                continue;
            }
            out.extend_from_slice(format!("{n}: {v}\r\n").as_bytes());
        }
        let bodyless = matches!(
            self.method.as_str(),
            "GET" | "HEAD" | "DELETE" | "OPTIONS" | "PURGE" | "BAN"
        );
        if !self.body.is_empty() || !bodyless {
            out.extend_from_slice(format!("Content-Length: {}\r\n", self.body.len()).as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&self.body);
        out
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The body as text, shortened for messages.
    pub fn body_snippet(&self) -> String {
        let s = String::from_utf8_lossy(&self.body);
        let s = s.trim();
        match s.char_indices().nth(300) {
            Some((i, _)) => format!("{}…", &s[..i]),
            None => s.to_string(),
        }
    }
}

/// A base URL: `scheme://host[:port][/base/path]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseUrl {
    pub secure: bool,
    pub host: String,
    pub port: u16,
    /// Path prefix without a trailing `/` (empty for none).
    pub base: String,
}

impl BaseUrl {
    pub fn parse(s: &str) -> Result<BaseUrl> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("http://") {
            (false, r)
        } else if s.contains("://") {
            bail!("endpoint `{shown}`: only http:// and https:// are supported");
        } else {
            (false, s)
        };
        let (authority, base) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
            None => (rest, ""),
        };
        if authority.contains('@') {
            bail!("endpoint `{shown}`: credentials belong in the target's vars, not in the URL");
        }
        let (host, port) = split_host_port(authority, if secure { 443 } else { 80 })
            .with_context(|| format!("endpoint `{shown}`"))?;
        Ok(BaseUrl {
            secure,
            host,
            port,
            base: base.to_string(),
        })
    }

    /// `Host` header value (port omitted when it is the scheme's default).
    pub fn host_header(&self) -> String {
        let default = if self.secure { 443 } else { 80 };
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        if self.port == default {
            host
        } else {
            format!("{host}:{}", self.port)
        }
    }

    pub fn url(&self) -> String {
        format!(
            "{}://{}{}",
            if self.secure { "https" } else { "http" },
            self.host_header(),
            self.base
        )
    }

    /// How an endpoint whose URL is a secret is shown: the scheme, the host, the port, and an
    /// ellipsis for everything below the origin. Every message that names such an endpoint —
    /// a log line at any `RUST_LOG` level, `/status`, `last_error`, a refused `expect` — goes
    /// through this one display, so the secret part of the URL has no way out.
    pub fn origin(&self) -> String {
        format!(
            "{}://{}/…",
            if self.secure { "https" } else { "http" },
            self.host_header()
        )
    }
}

/// The URL of a secret http endpoint: a full http(s) URL that lives behind an `env:` / `file:`
/// reference (see [`crate::secrets`]) because its path or query *is* the credential — the
/// incoming webhook of Slack, Microsoft Teams Workflows, Google Chat or Discord.
///
/// Everything below the origin is a secret: the value is resolved on every request, is never
/// handed to a template, never reaches a `{:?}` and is never named in a message. Only the origin
/// is shown, through [`BaseUrl::origin`]; the path and the query are joined to the request line
/// as the bytes are written ([`Client::send_to`]).
#[derive(Clone, PartialEq, Eq)]
pub struct SecretUrl {
    /// The origin: scheme, host and port. Nothing below the origin is kept.
    pub base: BaseUrl,
    /// The path, without a trailing `/` (empty when the URL has none).
    pub path: String,
    /// The query, without its `?` (empty when the URL has none).
    pub query: String,
}

/// The origin and an ellipsis, the way [`BaseUrl::origin`] shows it: the derived `Debug`
/// would print the path and the query, and those *are* the secret, so a future
/// `debug!("{url:?}")` could carry it into a log line.
impl std::fmt::Debug for SecretUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretUrl({})", self.base.origin())
    }
}

impl SecretUrl {
    /// Parses the value a reference holds. It must be a full http(s) URL, without userinfo (a
    /// credential belongs in the target's `vars`, and here it would be a second secret) and
    /// without a fragment. A message says what is wrong with the shape and never quotes the
    /// URL: the whole of it below the origin is the secret. A broken authority gets one fixed
    /// message, because `BaseUrl::parse` would quote what it was given, and the cut at the
    /// first `/` can leave a piece of the secret in its place: for `https://user:pa/ss@host/x`
    /// the authority becomes `user:pa` and its answer is `invalid port `pa``.
    pub fn parse(s: &str) -> Result<SecretUrl> {
        let s = s.trim();
        let (scheme, rest) = match s.strip_prefix("https://") {
            Some(r) => ("https", r),
            None => match s.strip_prefix("http://") {
                Some(r) => ("http", r),
                None => bail!(
                    "a secret endpoint is a full http(s) URL: `http://host/…` or `https://host/…`"
                ),
            },
        };
        let cut = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(cut);
        // Only the origin goes to `BaseUrl::parse` — and not even its message: it quotes the
        // authority it was given, which can be a piece of the secret. One fixed message says
        // what is wrong and names nothing.
        let base = BaseUrl::parse(&format!("{scheme}://{authority}"))
            .map_err(|_| anyhow!("the host or port of a secret endpoint is not valid"))?;
        if tail.contains('#') {
            bail!("a secret endpoint has no fragment: nothing after `#` is ever sent");
        }
        if tail.bytes().any(|c| c <= b' ' || c == 0x7f) {
            bail!("a secret endpoint has a space or a control character in its path or query");
        }
        let (path, query) = match tail.split_once('?') {
            Some((p, q)) => (p, q),
            None => (tail, ""),
        };
        Ok(SecretUrl {
            base,
            path: path.trim_end_matches('/').to_string(),
            query: query.to_string(),
        })
    }

    /// The request line's target for a template that wrote `request_target`: what the endpoint's
    /// own path and query make of it, so a webhook URL is called exactly as given.
    ///
    /// * `POST /` asks for the endpoint itself: the result is the endpoint's path and query, with
    ///   no stray trailing slash (`https://host/services/T0/B0/SECRET` and nothing else);
    /// * any other target goes below the endpoint's path: `/media/1.png` on
    ///   `https://host/hooks/a` is requested as `/hooks/a/media/1.png`;
    /// * the endpoint's query comes last, joined to a query the template wrote with `&`;
    /// * `*` stays `*` (a request line that asks for no path at all).
    pub fn target(&self, request_target: &str) -> String {
        if request_target == "*" {
            return "*".to_string();
        }
        let joined = if self.path.is_empty() {
            request_target.to_string()
        } else if request_target == "/" {
            self.path.clone()
        } else {
            format!("{}{}", self.path, request_target)
        };
        if self.query.is_empty() {
            return joined;
        }
        match joined.split_once('?') {
            Some((head, q)) if !q.is_empty() => format!("{head}?{q}&{}", self.query),
            Some((head, _)) => format!("{head}?{}", self.query),
            None => format!("{joined}?{}", self.query),
        }
    }
}

/// `host`, `host:port`, `[v6]`, `[v6]:port`.
pub fn split_host_port(authority: &str, default_port: u16) -> Result<(String, u16)> {
    if let Some(r) = authority.strip_prefix('[') {
        let (h, tail) = r.split_once(']').ok_or_else(|| anyhow!("unclosed `[`"))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p.parse().map_err(|_| anyhow!("invalid port `{p}`"))?,
            None if tail.is_empty() => default_port,
            None => bail!("unexpected `{tail}` after the address"),
        };
        return Ok((h.to_string(), port));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => {
            let port = p.parse().map_err(|_| anyhow!("invalid port `{p}`"))?;
            if h.is_empty() {
                bail!("no host");
            }
            Ok((h.to_string(), port))
        }
        _ if authority.is_empty() => bail!("no host"),
        _ => Ok((authority.to_string(), default_port)),
    }
}

const MAX_HEAD: usize = 64 * 1024;
const MAX_BODY: usize = 256 * 1024 * 1024;
const POOL_IDLE: Duration = Duration::from_secs(20);
const POOL_MAX: usize = 16;

struct Conn {
    stream: Stream,
    buf: Vec<u8>,
}

impl Conn {
    async fn fill(&mut self) -> Result<usize> {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.stream.read(&mut chunk).await?;
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(n)
    }

    async fn read_exact_buf(&mut self, n: usize) -> Result<Vec<u8>> {
        while self.buf.len() < n {
            if self.fill().await? == 0 {
                bail!("connection closed inside the response body");
            }
        }
        let rest = self.buf.split_off(n);
        Ok(std::mem::replace(&mut self.buf, rest))
    }

    async fn read_line(&mut self) -> Result<String> {
        loop {
            if let Some(i) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = self.read_exact_buf(i + 2).await?;
                return Ok(String::from_utf8_lossy(&line[..i]).into_owned());
            }
            if self.buf.len() > MAX_HEAD {
                bail!("response line too long");
            }
            if self.fill().await? == 0 {
                bail!("connection closed inside a chunked body");
            }
        }
    }
}

/// One HTTP endpoint with a pool of idle keep-alive connections.
pub struct Client {
    pub base: BaseUrl,
    connector: Connector,
    idle: Mutex<Vec<(Conn, Instant)>>,
}

impl Client {
    pub fn new(base: BaseUrl, tls: &TlsSpec) -> Result<Client> {
        let connector = Connector::new(&base.host, base.port, tls, base.secure, &[b"http/1.1"])?;
        Ok(Client {
            base,
            connector,
            idle: Mutex::new(Vec::new()),
        })
    }

    fn pooled(&self) -> Option<Conn> {
        let mut idle = self.idle.lock().expect("pool");
        while let Some((c, at)) = idle.pop() {
            if at.elapsed() < POOL_IDLE {
                return Some(c);
            }
        }
        None
    }

    fn put_back(&self, c: Conn) {
        let mut idle = self.idle.lock().expect("pool");
        if idle.len() < POOL_MAX {
            idle.push((c, Instant::now()));
        }
    }

    /// Sends `req` (path already including the base prefix) and reads the response, all
    /// within `timeout`. A pooled connection that turns out to be closed is retried once on a
    /// new one.
    pub async fn send(&self, req: &RawRequest, timeout: Duration) -> Result<Response> {
        self.send_to(req, &req.target, timeout).await
    }

    /// Sends `req` with the request line's target written as `target` instead of the one the
    /// template rendered: how a secret endpoint's own path and query reach the wire (see
    /// [`SecretUrl::target`]). `target` is the secret, so it is used for these bytes and
    /// dropped: nothing here keeps it, and no error names it.
    pub async fn send_to(
        &self,
        req: &RawRequest,
        target: &str,
        timeout: Duration,
    ) -> Result<Response> {
        let bytes = req.to_bytes_with(target);
        let head_only = req.method.eq_ignore_ascii_case("HEAD");
        let work = async {
            if let Some(conn) = self.pooled() {
                match self.exchange(conn, &bytes, head_only).await {
                    Ok(r) => return Ok(r),
                    Err((e, false)) => return Err(e),
                    Err((_, true)) => {} // stale keep-alive connection: retry fresh
                }
            }
            let conn = Conn {
                stream: self.connector.connect(timeout).await?,
                buf: Vec::new(),
            };
            self.exchange(conn, &bytes, head_only)
                .await
                .map_err(|(e, _)| e)
        };
        tokio::time::timeout(timeout, work)
            .await
            .map_err(|_| anyhow!("no complete response within {} ms", timeout.as_millis()))?
    }

    /// `Err((e, retry))`: `retry` when nothing of a response arrived (the request can go again
    /// on a new connection).
    async fn exchange(
        &self,
        mut c: Conn,
        bytes: &[u8],
        head_only: bool,
    ) -> std::result::Result<Response, (anyhow::Error, bool)> {
        if let Err(e) = async {
            c.stream.write_all(bytes).await?;
            c.stream.flush().await
        }
        .await
        {
            return Err((anyhow::Error::new(e).context("sending the request"), true));
        }
        let mut got_any = false;
        loop {
            // Head.
            let head_end = loop {
                if let Some(i) = c.buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
                if c.buf.len() > MAX_HEAD {
                    return Err((anyhow!("response head larger than {MAX_HEAD} bytes"), false));
                }
                match c.fill().await {
                    Ok(0) => {
                        return Err((
                            anyhow!("connection closed before a complete response head"),
                            !got_any && c.buf.is_empty(),
                        ))
                    }
                    Ok(_) => got_any = true,
                    Err(e) => {
                        return Err((
                            e.context("reading the response"),
                            !got_any && c.buf.is_empty(),
                        ))
                    }
                }
            };
            let mut hdrs = [httparse::EMPTY_HEADER; 128];
            let mut parsed = httparse::Response::new(&mut hdrs);
            match parsed.parse(&c.buf[..head_end]) {
                Ok(httparse::Status::Complete(_)) => {}
                Ok(httparse::Status::Partial) => {
                    return Err((anyhow!("incomplete response head"), false))
                }
                Err(e) => return Err((anyhow!("malformed response head: {e}"), false)),
            }
            let status = parsed.code.unwrap_or(0);
            let http10 = parsed.version == Some(0);
            let headers: Vec<(String, String)> = parsed
                .headers
                .iter()
                .map(|h| {
                    (
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).trim().to_string(),
                    )
                })
                .collect();
            c.buf.drain(..head_end);
            if (100..200).contains(&status) {
                continue; // 100 Continue and friends: the real response follows.
            }
            let get = |n: &str| {
                headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(n))
                    .map(|(_, v)| v.as_str())
            };
            let conn_hdr = get("connection").unwrap_or("").to_ascii_lowercase();
            let mut reusable = if http10 {
                conn_hdr.contains("keep-alive")
            } else {
                !conn_hdr.contains("close")
            };
            let chunked = get("transfer-encoding")
                .is_some_and(|t| t.to_ascii_lowercase().contains("chunked"));
            let body = if head_only || status == 204 || status == 304 {
                Vec::new()
            } else if chunked {
                read_chunked(&mut c).await.map_err(|e| (e, false))?
            } else if let Some(len) = get("content-length") {
                let n: usize = len
                    .parse()
                    .map_err(|_| (anyhow!("invalid Content-Length `{len}`"), false))?;
                if n > MAX_BODY {
                    return Err((anyhow!("response body of {n} bytes is too large"), false));
                }
                c.read_exact_buf(n).await.map_err(|e| (e, false))?
            } else {
                reusable = false;
                loop {
                    if c.buf.len() > MAX_BODY {
                        return Err((anyhow!("response body too large"), false));
                    }
                    match c.fill().await {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(e) => return Err((e, false)),
                    }
                }
                std::mem::take(&mut c.buf)
            };
            if reusable {
                self.put_back(c);
            }
            return Ok(Response {
                status,
                headers,
                body,
            });
        }
    }
}

async fn read_chunked(c: &mut Conn) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line = c.read_line().await?;
        let size_txt = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_txt, 16)
            .map_err(|_| anyhow!("invalid chunk size `{size_txt}`"))?;
        if size == 0 {
            // Trailers until an empty line.
            while !c.read_line().await?.is_empty() {}
            return Ok(body);
        }
        if body.len().checked_add(size).is_none_or(|n| n > MAX_BODY) {
            bail!("chunked response body too large");
        }
        let chunk = c.read_exact_buf(size + 2).await?;
        body.extend_from_slice(&chunk[..size]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connection whose peer sends `bytes` and then closes, over loopback.
    async fn conn_with(bytes: &[u8]) -> Conn {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bytes = bytes.to_vec();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            s.write_all(&bytes).await.unwrap();
            let _ = s.shutdown().await;
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        Conn {
            stream: Stream::Plain(stream),
            buf: Vec::new(),
        }
    }

    #[tokio::test]
    async fn chunked_body_joins_its_chunks() {
        let mut c = conn_with(b"4\r\nWiki\r\n5\r\npedia\r\n1\r\n!\r\n0\r\n\r\n").await;
        assert_eq!(read_chunked(&mut c).await.unwrap(), b"Wikipedia!");
    }

    #[tokio::test]
    async fn chunked_body_skips_chunk_extensions() {
        let mut c =
            conn_with(b"4;name=val\r\nWiki\r\n5 ; x=\"y\"\r\npedia\r\n0;final\r\n\r\n").await;
        assert_eq!(read_chunked(&mut c).await.unwrap(), b"Wikipedia");
    }

    #[tokio::test]
    async fn chunked_body_reads_and_drops_trailers() {
        let mut c = conn_with(b"4\r\nWiki\r\n0\r\nX-Checksum: abc\r\nX-Other: 1\r\n\r\n").await;
        assert_eq!(read_chunked(&mut c).await.unwrap(), b"Wiki");
    }

    #[tokio::test]
    async fn chunked_empty_body() {
        let mut c = conn_with(b"0\r\n\r\n").await;
        assert_eq!(read_chunked(&mut c).await.unwrap(), b"");
    }

    #[tokio::test]
    async fn chunked_malformed_size_is_an_error() {
        for bytes in [&b"zz\r\nabc\r\n0\r\n\r\n"[..], b"-1\r\n", b"\r\n"] {
            let mut c = conn_with(bytes).await;
            let err = read_chunked(&mut c).await.unwrap_err().to_string();
            assert!(err.starts_with("invalid chunk size `"), "{err}");
        }
    }

    #[tokio::test]
    async fn chunked_size_over_the_body_limit_is_refused_before_reading_it() {
        // 0x10000001 bytes is one more than MAX_BODY (256 MiB); the size alone is enough.
        let mut c = conn_with(b"10000001\r\n").await;
        let err = read_chunked(&mut c).await.unwrap_err().to_string();
        assert_eq!(err, "chunked response body too large");
    }

    #[tokio::test]
    async fn chunked_body_cut_short_is_an_error() {
        let mut c = conn_with(b"5\r\nab").await;
        let err = read_chunked(&mut c).await.unwrap_err().to_string();
        assert_eq!(err, "connection closed inside the response body");
    }

    #[test]
    fn parse_requests() {
        let r = RawRequest::parse(
            "\n\nPOST /idx/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{\"a\":1}\n",
        )
        .unwrap()
        .unwrap();
        assert_eq!(r.method, "POST");
        assert_eq!(r.target, "/idx/_bulk");
        assert_eq!(r.header("content-type"), Some("application/x-ndjson"));
        assert_eq!(r.body, b"{\"a\":1}\n");

        let r = RawRequest::parse("PURGE / HTTP/1.1\r\nHost: v\r\nX-Cache-Tags-Pattern: .*\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(r.headers.len(), 2);
        assert!(r.body.is_empty());
        let wire = String::from_utf8(r.to_bytes()).unwrap();
        assert_eq!(
            wire,
            "PURGE / HTTP/1.1\r\nHost: v\r\nX-Cache-Tags-Pattern: .*\r\n\r\n"
        );

        let r = RawRequest::parse("DELETE /p/1.json HTTP/1.1\n\n")
            .unwrap()
            .unwrap();
        assert!(r.headers.is_empty() && r.body.is_empty());
        let mut put = RawRequest::parse("PUT /x\nContent-Length: 99\n\nabc")
            .unwrap()
            .unwrap();
        put.set_header("Host", "h".into());
        assert_eq!(
            String::from_utf8(put.to_bytes()).unwrap(),
            "PUT /x HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc"
        );

        assert!(RawRequest::parse("  \n ").unwrap().is_none());
        assert!(RawRequest::parse("GET").is_err());
        assert!(RawRequest::parse("GET /a b HTTP/1.1").is_err());
        assert!(RawRequest::parse("GET x HTTP/1.1").is_err());
        assert!(RawRequest::parse("GET / HTTP/1.1\nbroken header\n").is_err());
        assert!(RawRequest::parse("GET / HTTP/1.1\nX-Tag: a\rX-Evil: 1\n").is_err());
        assert!(RawRequest::parse("GET /a\u{0}b HTTP/1.1\n").is_err());
        assert!(RawRequest::parse("GET / HTTP/1.1\nX-Tab: a\tb\n").is_ok());
    }

    #[test]
    fn base_urls() {
        let b = BaseUrl::parse("https://es.example.com:9243/prefix/").unwrap();
        assert_eq!(
            (b.secure, b.host.as_str(), b.port, b.base.as_str()),
            (true, "es.example.com", 9243, "/prefix")
        );
        assert_eq!(b.host_header(), "es.example.com:9243");
        let b = BaseUrl::parse("varnish:6081").unwrap();
        assert_eq!((b.secure, b.port), (false, 6081));
        assert_eq!(
            BaseUrl::parse("http://[::1]").unwrap().host_header(),
            "[::1]"
        );
        assert_eq!(BaseUrl::parse("https://h").unwrap().url(), "https://h");
        assert!(BaseUrl::parse("ftp://h").is_err());
        assert!(BaseUrl::parse("http://u:p@h").is_err());
    }
}

#[cfg(test)]
mod secret_url_tests {
    use super::*;

    const MARKER: &str = "SECMARK-6f1a";

    /// The whole URL of a webhook, with a path that is the secret.
    const WEBHOOK: &str = "https://hooks.example.test/services/T0000/B0000/SECMARK-6f1a";

    fn parse(s: &str) -> SecretUrl {
        SecretUrl::parse(s).unwrap()
    }

    #[test]
    fn a_full_https_url_keeps_its_origin_and_holds_its_path_and_query() {
        let u = parse(WEBHOOK);
        assert_eq!(
            (u.base.secure, u.base.host.as_str(), u.base.port),
            (true, "hooks.example.test", 443)
        );
        assert_eq!(u.path, "/services/T0000/B0000/SECMARK-6f1a");
        assert_eq!(u.query, "");
        // What the rest of the program is allowed to show.
        assert_eq!(u.base.origin(), "https://hooks.example.test/…");

        let u = parse("http://127.0.0.1:34599/hooks/a/?team=42&key=SECMARK-6f1a");
        assert_eq!((u.base.host.as_str(), u.base.port), ("127.0.0.1", 34599));
        assert_eq!(u.path, "/hooks/a");
        assert_eq!(u.query, "team=42&key=SECMARK-6f1a");
        assert_eq!(u.base.origin(), "http://127.0.0.1:34599/…");

        // An origin with nothing below it, and a bracketed IPv6.
        let u = parse("https://[::1]");
        assert_eq!((u.path.as_str(), u.query.as_str()), ("", ""));
        assert_eq!(u.base.origin(), "https://[::1]/…");
        // Surrounding whitespace a secret file may hold.
        assert_eq!(parse("  https://h/a  ").path, "/a");
    }

    #[test]
    fn a_url_that_is_not_a_plain_http_one_is_refused_without_naming_it() {
        // The whole chain: a load error is shown that way too, in the log and in `last_error`.
        let err = |s: &str| format!("{:#}", SecretUrl::parse(s).unwrap_err());
        // Not a full http(s) URL.
        assert!(err("hooks.example.test/services/T/B/x").contains("full http(s) URL"));
        assert!(err("ftp://h/SECMARK-6f1a").contains("full http(s) URL"));
        assert!(err("").contains("full http(s) URL"));
        // A credential in the URL is a load error, and the message names neither it nor
        // the host: every `BaseUrl::parse` error becomes one fixed message.
        let e = err("https://u:SECMARK-6f1a@h/x");
        assert!(e.contains("not valid"), "{e}");
        assert!(!e.contains(MARKER), "{e}");
        // A fragment is never sent, so it cannot be part of the endpoint.
        assert!(err("https://h/x#SECMARK-6f1a").contains("fragment"));
        // A space or a control character is how a second header smuggles itself in.
        for bad in ["https://h/a b", "https://h/a\u{1}b", "https://h/a\tb"] {
            assert!(err(bad).contains("control character"), "{bad}");
        }
        // A broken address: one fixed message, whatever `BaseUrl::parse` would have said.
        for bad in ["https://", "https://h:notaport/x"] {
            assert!(err(bad).contains("not valid"), "{bad}");
        }

        // Nothing that is refused carries the secret out in the message. The userinfo with a
        // `/` in it is the sharp case: the cut at the first `/` leaves `user:SECMARK-6f1a` as
        // the authority, and `BaseUrl::parse` would answer `invalid port `SECMARK-6f1a``.
        for bad in [
            "ftp://h/SECMARK-6f1a",
            "https://u:SECMARK-6f1a@h/x",
            "https://user:SECMARK-6f1a/ss@h/x",
            "https://h/x#SECMARK-6f1a",
            "https://h/a SECMARK-6f1a",
        ] {
            let e = err(bad);
            assert!(!e.contains(MARKER), "`{bad}` is shown as `{e}`");
        }
    }

    #[test]
    fn a_debug_shows_the_origin_and_an_ellipsis_and_nothing_else() {
        // Path and query both hold the marker; neither may reach the `{:?}`.
        let u = parse("https://hooks.example.test/services/T0000/SECMARK-6f1a?key=SECMARK-6f1a");
        let d = format!("{u:?}");
        assert_eq!(d, "SecretUrl(https://hooks.example.test/…)");
        assert!(!d.contains(MARKER), "{d}");
    }

    #[test]
    fn the_endpoint_s_path_and_query_join_the_request_line() {
        let u = parse(WEBHOOK);
        // `POST /` calls the webhook exactly as given: no trailing slash, nothing appended.
        assert_eq!(u.target("/"), "/services/T0000/B0000/SECMARK-6f1a");
        // Any other target goes below the endpoint's path.
        assert_eq!(
            u.target("/media/1.png"),
            "/services/T0000/B0000/SECMARK-6f1a/media/1.png"
        );
        // `*` asks for no path.
        assert_eq!(u.target("*"), "*");

        // The query comes last, whatever the template's target carried.
        let u =
            parse("https://chat.example.test/v1/spaces/AAA/messages?key=SECMARK-6f1a&token=BBB");
        assert_eq!(
            u.target("/"),
            "/v1/spaces/AAA/messages?key=SECMARK-6f1a&token=BBB"
        );
        assert_eq!(
            u.target("/x?a=1"),
            "/v1/spaces/AAA/messages/x?a=1&key=SECMARK-6f1a&token=BBB"
        );
        assert_eq!(
            u.target("/x?"),
            "/v1/spaces/AAA/messages/x?key=SECMARK-6f1a&token=BBB"
        );
        // An origin with no path leaves the request line as the template wrote it.
        let u = parse("http://localhost:9/");
        assert_eq!(u.target("/"), "/");
        assert_eq!(u.target("/a?b=1"), "/a?b=1");
        // A trailing slash on the endpoint never reaches the wire twice.
        let u = parse("https://h/hooks/a///");
        assert_eq!(u.target("/"), "/hooks/a");
    }

    #[test]
    fn the_joined_target_is_the_only_place_the_secret_path_appears() {
        // `send_to` writes it into the request line; `to_bytes` on its own keeps what the
        // template rendered, which holds no secret.
        let u = parse(WEBHOOK);
        let req = RawRequest::parse("POST / HTTP/1.1\n\nok").unwrap().unwrap();
        assert_eq!(req.target, "/");
        let wire = String::from_utf8(req.to_bytes_with(&u.target(&req.target))).unwrap();
        assert!(
            wire.starts_with("POST /services/T0000/B0000/SECMARK-6f1a HTTP/1.1\r\n"),
            "{wire}"
        );
        assert!(String::from_utf8(req.to_bytes())
            .unwrap()
            .starts_with("POST / HTTP/1.1\r\n"));
        assert!(!format!("{req:?}").contains(MARKER));
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn debug_masks_credential_headers_and_the_body() {
        let r = RawRequest {
            method: "POST".into(),
            target: "/x".into(),
            headers: vec![
                ("Authorization".into(), "Basic HDR-AUTH-1".into()),
                ("X-Amz-Security-Token".into(), "HDR-TOKEN-2".into()),
                ("X-Api-Key".into(), "HDR-KEY-3".into()),
                ("Host".into(), "visible.example".into()),
            ],
            body: b"BODY-SECRET-4".to_vec(),
        };
        let shown = format!("{r:?}");
        for hidden in ["HDR-AUTH-1", "HDR-TOKEN-2", "HDR-KEY-3", "BODY-SECRET-4"] {
            assert!(!shown.contains(hidden), "{shown}");
        }
        assert!(shown.contains("visible.example"), "{shown}");
        assert!(shown.contains("13 bytes"), "{shown}");
    }
}

/// What a rendered template is allowed to put on the wire: a request line that smuggles a
/// second header, or a body length that lies, must be refused before it is sent.
#[cfg(test)]
mod request_tests {
    use super::*;

    fn err(text: &str) -> String {
        RawRequest::parse(text).unwrap_err().to_string()
    }

    #[test]
    fn a_blank_render_is_nothing_to_send() {
        assert!(RawRequest::parse("").unwrap().is_none());
        assert!(RawRequest::parse("\r\n \t\n").unwrap().is_none());
    }

    #[test]
    fn the_request_line_is_checked_word_by_word() {
        assert!(RawRequest::parse("GET /a HTTP/1.1\r\n\r\n")
            .unwrap()
            .is_some());
        assert!(RawRequest::parse("OPTIONS * HTTP/1.1\r\n\r\n")
            .unwrap()
            .is_some());
        assert!(err("GET\r\n").contains("has no path"));
        assert!(err("GET /a 1.1\r\n").contains("`1.1` is not an HTTP version"));
        assert!(err("GET /a HTTP/1.1 x\r\n").contains("extra words"));
        assert!(err("PO!ST /a HTTP/1.1\r\n").contains("`PO!ST` is not a method"));
        assert!(err("GET a/b HTTP/1.1\r\n").contains("the path must start with `/`"));
        // A stray control character is how a template smuggles extra lines in.
        assert!(err("GET /a\u{1} HTTP/1.1\r\n").contains("holds a control character"));
    }

    #[test]
    fn headers_need_a_name_and_a_clean_value() {
        assert!(err("GET /a HTTP/1.1\r\nNoColon\r\n\r\n").contains("has no `:`"));
        assert!(err("GET /a HTTP/1.1\r\n: 1\r\n\r\n").contains("invalid header name"));
        assert!(err("GET /a HTTP/1.1\r\nX Y: 1\r\n\r\n").contains("invalid header name"));
        assert!(err("GET /a HTTP/1.1\r\nX: a\u{0}b\r\n\r\n").contains("control character"));

        let r = RawRequest::parse(
            "POST /a HTTP/1.1\r\nContent-Type: application/json\r\nX-Trace:  t1 \r\n\r\nhi",
        )
        .unwrap()
        .expect("a request");
        assert_eq!(r.header("content-TYPE"), Some("application/json"));
        assert_eq!(r.header("X-TRACE"), Some("t1"), "a value is trimmed");
        assert_eq!(r.header("Authorization"), None);
        assert_eq!(r.body, b"hi");
    }

    #[test]
    fn the_only_content_length_on_the_wire_is_the_body_s() {
        let r = RawRequest {
            method: "GET".into(),
            target: "/orders/7".into(),
            headers: vec![
                ("Content-Length".into(), "999".into()),
                ("transfer-encoding".into(), "chunked".into()),
                ("Accept".into(), "application/json".into()),
            ],
            body: b"hello".to_vec(),
        };
        let wire = String::from_utf8(r.to_bytes()).unwrap();
        assert!(wire.starts_with("GET /orders/7 HTTP/1.1\r\n"), "{wire}");
        assert!(wire.contains("Accept: application/json\r\n"), "{wire}");
        assert!(
            wire.ends_with("\r\nContent-Length: 5\r\n\r\nhello"),
            "{wire}"
        );
        assert!(!wire.contains("999") && !wire.contains("chunked"), "{wire}");

        let mut r = RawRequest {
            method: "HEAD".into(),
            target: "/".into(),
            headers: vec![],
            body: vec![],
        };
        let wire = String::from_utf8(r.to_bytes()).unwrap();
        assert_eq!(
            wire, "HEAD / HTTP/1.1\r\n\r\n",
            "a bodyless call states no length"
        );
        r.method = "POST".into();
        assert!(String::from_utf8(r.to_bytes())
            .unwrap()
            .ends_with("Content-Length: 0\r\n\r\n"));
        r.body = b"ok".to_vec();
        assert!(String::from_utf8(r.to_bytes())
            .unwrap()
            .ends_with("Content-Length: 2\r\n\r\nok"));
    }

    #[test]
    fn an_answer_is_read_case_insensitively_and_shown_short() {
        let resp = Response {
            status: 500,
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: b"  Bad Gateway \n".to_vec(),
        };
        assert_eq!(resp.header("CONTENT-type"), Some("text/plain"));
        assert_eq!(resp.header("Date"), None);
        assert_eq!(resp.body_snippet(), "Bad Gateway", "padded for the log");

        let long = Response {
            status: 502,
            headers: vec![],
            body: vec![b'x'; 400],
        };
        let shown = long.body_snippet();
        assert_eq!(shown.chars().count(), 301, "300 characters and an ellipsis");
        assert!(shown.ends_with('…'));

        // Clipping counts characters, so a multi-byte body is never cut in half.
        let long = Response {
            status: 200,
            headers: vec![],
            body: "é".repeat(350).into_bytes(),
        };
        assert_eq!(long.body_snippet().chars().count(), 301);

        let binary = Response {
            status: 200,
            headers: vec![],
            body: vec![0xff, b's', b'o', b's'],
        };
        assert_eq!(
            binary.body_snippet(),
            "\u{fffd}sos",
            "undecodable bytes are still shown"
        );
    }

    #[test]
    fn an_address_is_split_the_way_it_is_written() {
        for (authority, default, want_host, want_port) in [
            ("example.com", 80u16, "example.com", 80u16),
            ("example.com:8443", 80, "example.com", 8443),
            ("[::1]", 6379, "::1", 6379),
            ("[::1]:6380", 6379, "::1", 6380),
        ] {
            assert_eq!(
                split_host_port(authority, default).unwrap(),
                (want_host.to_string(), want_port),
                "{authority}"
            );
        }
        for bad in ["[::1", "[::1]x", "[::1]:abc", ":80", "", "h:abc"] {
            assert!(
                split_host_port(bad, 80).is_err(),
                "`{bad}` is not an address"
            );
        }
        // A bracketless IPv6 is left whole rather than having its last group read as a port.
        assert_eq!(split_host_port("::1", 80).unwrap(), ("::1".to_string(), 80));
    }
}
