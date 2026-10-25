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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRequest {
    pub method: String,
    /// Path and query, as written (`/a/b?x=1`).
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
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
        let mut out = Vec::with_capacity(256 + self.body.len());
        out.extend_from_slice(format!("{} {} HTTP/1.1\r\n", self.method, self.target).as_bytes());
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
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("http://") {
            (false, r)
        } else if s.contains("://") {
            bail!("endpoint `{s}`: only http:// and https:// are supported");
        } else {
            (false, s)
        };
        let (authority, base) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
            None => (rest, ""),
        };
        if authority.contains('@') {
            bail!("endpoint `{s}`: credentials belong in the target's vars, not in the URL");
        }
        let (host, port) = split_host_port(authority, if secure { 443 } else { 80 })
            .with_context(|| format!("endpoint `{s}`"))?;
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
        let bytes = req.to_bytes();
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
        if body.len() + size > MAX_BODY {
            bail!("chunked response body too large");
        }
        let chunk = c.read_exact_buf(size + 2).await?;
        body.extend_from_slice(&chunk[..size]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let r = RawRequest::parse("PURGE / HTTP/1.1\r\nHost: v\r\nX-Magento-Tags-Pattern: .*\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(r.headers.len(), 2);
        assert!(r.body.is_empty());
        let wire = String::from_utf8(r.to_bytes()).unwrap();
        assert_eq!(
            wire,
            "PURGE / HTTP/1.1\r\nHost: v\r\nX-Magento-Tags-Pattern: .*\r\n\r\n"
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
