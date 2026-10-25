//! Redis targets: endpoints (`host:port/db`), commands tokenized like redis-cli arguments, and
//! a minimal RESP2 client (`AUTH` and `SELECT` on connect).

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::http::split_host_port;
use crate::tls::{Connector, Stream, TlsSpec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisAddr {
    pub secure: bool,
    pub host: String,
    pub port: u16,
    pub db: u32,
}

impl RedisAddr {
    /// `host:port/db`, `host:port`, `redis://host:port/db`, `rediss://…` (TLS).
    pub fn parse(s: &str) -> Result<RedisAddr> {
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("rediss://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("redis://") {
            (false, r)
        } else if s.contains("://") {
            bail!("redis endpoint `{s}`: only redis:// and rediss:// are supported");
        } else {
            (false, s)
        };
        let (authority, db) = match rest.split_once('/') {
            Some((a, d)) if d.trim_matches('/').is_empty() => (a, 0),
            Some((a, d)) => (
                a,
                d.trim_matches('/')
                    .parse()
                    .map_err(|_| anyhow!("redis endpoint `{s}`: invalid database `{d}`"))?,
            ),
            None => (rest, 0),
        };
        if authority.contains('@') {
            bail!(
                "redis endpoint `{s}`: credentials belong in the target's vars, not in the address"
            );
        }
        let (host, port) =
            split_host_port(authority, 6379).with_context(|| format!("redis endpoint `{s}`"))?;
        Ok(RedisAddr {
            secure,
            host,
            port,
            db,
        })
    }

    pub fn url(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!(
            "{}://{host}:{}/{}",
            if self.secure { "rediss" } else { "redis" },
            self.port,
            self.db
        )
    }
}

/// Splits a command line the way redis-cli does (`sdssplitargs`): whitespace separates
/// arguments; `"…"` understands `\n \r \t \b \a \\ \" \xHH`; `'…'` only `\'`. A closing quote
/// must be followed by whitespace or the end.
pub fn tokenize(line: &str) -> Result<Vec<Vec<u8>>> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            return Ok(out);
        }
        let mut cur = Vec::new();
        let mut in_dq = false;
        let mut in_sq = false;
        loop {
            if in_dq {
                match b.get(i) {
                    None => bail!("unbalanced double quote in redis command"),
                    Some(b'\\')
                        if b.get(i + 1) == Some(&b'x')
                            && b.get(i + 2).is_some_and(u8::is_ascii_hexdigit)
                            && b.get(i + 3).is_some_and(u8::is_ascii_hexdigit) =>
                    {
                        cur.push(u8::from_str_radix(&line[i + 2..i + 4], 16).expect("hex"));
                        i += 4;
                        continue;
                    }
                    Some(b'\\') if i + 1 < b.len() => {
                        cur.push(match b[i + 1] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 8,
                            b'a' => 7,
                            c => c,
                        });
                        i += 2;
                        continue;
                    }
                    Some(b'"') => {
                        if b.get(i + 1).is_some_and(|c| !c.is_ascii_whitespace()) {
                            bail!("closing quote must be followed by a space in redis command");
                        }
                        i += 1;
                        break;
                    }
                    Some(&c) => cur.push(c),
                }
            } else if in_sq {
                match b.get(i) {
                    None => bail!("unbalanced single quote in redis command"),
                    Some(b'\\') if b.get(i + 1) == Some(&b'\'') => {
                        cur.push(b'\'');
                        i += 2;
                        continue;
                    }
                    Some(b'\'') => {
                        if b.get(i + 1).is_some_and(|c| !c.is_ascii_whitespace()) {
                            bail!("closing quote must be followed by a space in redis command");
                        }
                        i += 1;
                        break;
                    }
                    Some(&c) => cur.push(c),
                }
            } else {
                match b.get(i) {
                    None => break,
                    Some(c) if c.is_ascii_whitespace() => break,
                    Some(b'"') => in_dq = true,
                    Some(b'\'') => in_sq = true,
                    Some(&c) => cur.push(c),
                }
            }
            i += 1;
        }
        out.push(cur);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    pub fn to_json(&self) -> Value {
        match self {
            Reply::Simple(s) | Reply::Error(s) => Value::String(s.clone()),
            Reply::Int(i) => Value::from(*i),
            Reply::Bulk(None) | Reply::Array(None) => Value::Null,
            Reply::Bulk(Some(b)) => Value::String(String::from_utf8_lossy(b).into_owned()),
            Reply::Array(Some(a)) => Value::Array(a.iter().map(Reply::to_json).collect()),
        }
    }

    /// The first error in the reply (nested ones included, as `EXEC` returns them).
    pub fn error(&self) -> Option<&str> {
        match self {
            Reply::Error(e) => Some(e),
            Reply::Array(Some(a)) => a.iter().find_map(Reply::error),
            _ => None,
        }
    }
}

pub fn encode(args: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

struct Conn {
    stream: Stream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    async fn fill(&mut self) -> Result<()> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let mut chunk = [0u8; 16 * 1024];
        let n = self.stream.read(&mut chunk).await?;
        if n == 0 {
            bail!("redis closed the connection");
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }

    async fn line(&mut self) -> Result<String> {
        loop {
            if let Some(i) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + i]).into_owned();
                self.pos += i + 2;
                return Ok(s);
            }
            self.fill().await?;
        }
    }

    async fn bytes(&mut self, n: usize) -> Result<Vec<u8>> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill().await?;
        }
        let v = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        Ok(v)
    }

    async fn reply(&mut self) -> Result<Reply> {
        // Iterative over nesting: a stack of (array, remaining).
        let mut stack: Vec<(Vec<Reply>, usize)> = Vec::new();
        loop {
            let line = self.line().await?;
            let (t, rest) = line.split_at(line.len().min(1));
            let mut item = match t {
                "+" => Reply::Simple(rest.to_string()),
                "-" => Reply::Error(rest.to_string()),
                ":" => Reply::Int(
                    rest.parse()
                        .map_err(|_| anyhow!("bad integer reply `{rest}`"))?,
                ),
                "$" => {
                    let n: i64 = rest
                        .parse()
                        .map_err(|_| anyhow!("bad bulk length `{rest}`"))?;
                    if n < 0 {
                        Reply::Bulk(None)
                    } else {
                        Reply::Bulk(Some(self.bytes(n as usize).await?))
                    }
                }
                "*" => {
                    let n: i64 = rest
                        .parse()
                        .map_err(|_| anyhow!("bad array length `{rest}`"))?;
                    if n < 0 {
                        Reply::Array(None)
                    } else if n == 0 {
                        Reply::Array(Some(Vec::new()))
                    } else {
                        stack.push((Vec::with_capacity(n as usize), n as usize));
                        continue;
                    }
                }
                other => bail!("unexpected redis reply type `{other}`"),
            };
            loop {
                let Some((arr, left)) = stack.last_mut() else {
                    return Ok(item);
                };
                arr.push(item);
                *left -= 1;
                if *left > 0 {
                    break;
                }
                let (arr, _) = stack.pop().expect("non-empty");
                item = Reply::Array(Some(arr));
            }
        }
    }

    async fn call(&mut self, args: &[Vec<u8>]) -> Result<Reply> {
        self.stream.write_all(&encode(args)).await?;
        self.stream.flush().await?;
        self.reply().await
    }
}

/// One Redis endpoint; the connection is opened on first use and after any error.
pub struct Client {
    pub addr: RedisAddr,
    connector: Connector,
    username: Option<String>,
    password: Option<String>,
    conn: Mutex<Option<Conn>>,
}

impl Client {
    pub fn new(
        addr: RedisAddr,
        tls: &TlsSpec,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Client> {
        let connector = Connector::new(&addr.host, addr.port, tls, addr.secure, &[])?;
        Ok(Client {
            addr,
            connector,
            username,
            password,
            conn: Mutex::new(None),
        })
    }

    async fn open(&self, timeout: Duration) -> Result<Conn> {
        let mut c = Conn {
            stream: self.connector.connect(timeout).await?,
            buf: Vec::new(),
            pos: 0,
        };
        // Credentials are references or literals, resolved at every connect (secrets rotate).
        if let Some(pw) = &self.password {
            let pw = crate::secrets::resolve(pw).context("redis password")?;
            let mut args = vec![b"AUTH".to_vec()];
            if let Some(u) = &self.username {
                args.push(
                    crate::secrets::resolve(u)
                        .context("redis username")?
                        .into_bytes(),
                );
            }
            args.push(pw.into_bytes());
            if let Some(e) = c.call(&args).await?.error() {
                bail!("AUTH failed: {e}");
            }
        }
        if self.addr.db != 0 {
            let r = c
                .call(&[b"SELECT".to_vec(), self.addr.db.to_string().into_bytes()])
                .await?;
            if let Some(e) = r.error() {
                bail!("SELECT {} failed: {e}", self.addr.db);
            }
        }
        Ok(c)
    }

    /// Runs one command. An error reply is returned as `Ok(Reply::Error)`; transport problems
    /// are `Err` (and drop the connection).
    pub async fn command(&self, args: &[Vec<u8>], timeout: Duration) -> Result<Reply> {
        let mut guard = self.conn.lock().await;
        let work = async {
            if guard.is_none() {
                *guard = Some(self.open(timeout).await?);
            }
            guard.as_mut().expect("open").call(args).await
        };
        match tokio::time::timeout(timeout, work).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => {
                *guard = None;
                Err(e)
            }
            Err(_) => {
                *guard = None;
                Err(anyhow!("no reply within {} ms", timeout.as_millis()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Vec<String> {
        tokenize(s)
            .unwrap()
            .into_iter()
            .map(|b| String::from_utf8(b).unwrap())
            .collect()
    }

    #[test]
    fn tokenizing() {
        assert_eq!(t("DEL k1  k2\n"), vec!["DEL", "k1", "k2"]);
        assert_eq!(
            t(r#"EVAL "for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end" 0 a b"#),
            vec![
                "EVAL",
                "for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end",
                "0",
                "a",
                "b"
            ]
        );
        assert_eq!(
            t(r#"SET "a\"b\n\x41" 'it\'s'"#),
            vec!["SET", "a\"b\nA", "it's"]
        );
        assert_eq!(t("SET a\"b c\""), vec!["SET", "ab c"]);
        assert!(tokenize("SET a\"b\"c").is_err());
        assert_eq!(t(r#"SET k """#), vec!["SET", "k", ""]);
        assert!(t("   ").is_empty());
        assert!(tokenize("SET \"a").is_err());
        assert!(tokenize("SET \"a\"b").is_err());
        let quoted = crate::template::redis_quote("x \"y\"\n\\\u{1}");
        assert_eq!(t(&format!("SET {quoted}")), vec!["SET", "x \"y\"\n\\\u{1}"]);
    }

    #[test]
    fn addresses() {
        assert_eq!(
            RedisAddr::parse("cache:6379/1").unwrap(),
            RedisAddr {
                secure: false,
                host: "cache".into(),
                port: 6379,
                db: 1
            }
        );
        assert_eq!(
            RedisAddr::parse("rediss://r.example.com/2").unwrap().url(),
            "rediss://r.example.com:6379/2"
        );
        assert_eq!(RedisAddr::parse("10.0.0.1").unwrap().db, 0);
        assert!(RedisAddr::parse("redis://:pw@h:6379").is_err());
        assert!(RedisAddr::parse("h:6379/x").is_err());
    }

    #[test]
    fn encoding() {
        assert_eq!(
            encode(&[b"DEL".to_vec(), b"k".to_vec()]),
            b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"
        );
        let r = Reply::Array(Some(vec![
            Reply::Bulk(Some(b"17".to_vec())),
            Reply::Array(Some(vec![Reply::Bulk(Some(b"k1".to_vec()))])),
        ]));
        assert_eq!(r.to_json(), serde_json::json!(["17", ["k1"]]));
        assert_eq!(
            Reply::Array(Some(vec![Reply::Int(1), Reply::Error("ERR x".into())])).error(),
            Some("ERR x")
        );
    }
}
