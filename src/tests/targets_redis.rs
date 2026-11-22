use super::*;

#[test]
fn transient_error_replies() {
    for e in [
        "LOADING Redis is loading the dataset in memory",
        "BUSY Redis is busy running a script",
        "TRYAGAIN Multiple keys request during rehashing",
        "CLUSTERDOWN The cluster is down",
        "MASTERDOWN Link with MASTER is down",
        "READONLY You can't write against a read only replica.",
        "NOAUTH Authentication required.",
        "OOM command not allowed when used memory > 'maxmemory'.",
        "MISCONF Redis is configured to save RDB snapshots",
        "NOREPLICAS Not enough good replicas to write.",
        "MOVED 3999 127.0.0.1:6381",
        "ASK 3999 127.0.0.1:6381",
    ] {
        assert!(transient_error(e), "{e} should be transient");
    }
    for e in [
        "WRONGTYPE Operation against a key holding the wrong kind of value",
        "ERR syntax error",
        "NOSCRIPT No matching script",
        "loading is not an error code in lower case",
        "",
    ] {
        assert!(!transient_error(e), "{e} should be a refusal");
    }
}

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
fn a_scheme_that_is_not_redis_names_the_two_it_takes() {
    // A memcached or http URL slipped into a redis endpoint fails at the parse, with the
    // two schemes the kind takes — the message is the only thing telling the operator what
    // to write, so it has to name them.
    for s in ["memcached://cache:11211", "http://cache:6379", "tc://h/0"] {
        let e = RedisAddr::parse(s).unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("only redis:// and rediss:// are supported"),
            "{s}: {msg}"
        );
        assert!(
            msg.contains(s),
            "{s}: the refusal must show the endpoint: {msg}"
        );
    }
    // No scheme at all is the plain `host:port/db` form, not a refusal.
    assert!(!RedisAddr::parse("cache:6379/1").unwrap().secure);
}

#[test]
fn the_url_of_an_ipv6_address_brackets_the_host() {
    // The address keeps its colons, so the URL has to bracket it or it names a port
    // instead of a host — and the URL is what a log line and the `endpoint` context show.
    let a = RedisAddr::parse("redis://[::1]:6379/0").unwrap();
    assert_eq!(a.host, "::1");
    assert_eq!(a.url(), "redis://[::1]:6379/0");
    let a = RedisAddr::parse("rediss://[fd00::5]").unwrap();
    assert_eq!(a.url(), "rediss://[fd00::5]:6379/0");
    // A host without colons is written as it is.
    assert_eq!(
        RedisAddr::parse("redis://cache/3").unwrap().url(),
        "redis://cache:6379/3"
    );
}

#[test]
fn a_client_that_cannot_be_built_fails_the_endpoint() {
    // TLS settings are read when the client is built: a CA file that is not there must
    // fail the endpoint at load, naming what is missing, rather than leave a target that
    // fails on every key it is asked to send.
    let def = |tls: &str| TargetDef {
        name: "t".into(),
        kind: Kind::Redis,
        endpoints: vec!["rediss://cache:6379/0".into()],
        tls: TlsSpec::parse(Some(tls)).unwrap(),
        timeout: Duration::from_secs(1),
        expect: None,
        vars: Default::default(),
    };
    let e = new(
        &def(r#"{"ca":"/nonexistent/lazywf-ca.pem"}"#),
        "rediss://cache:6379/0",
    )
    .unwrap_err();
    let msg = format!("{e:#}");
    assert!(
        msg.contains("nonexistent/lazywf-ca.pem"),
        "the refusal must name the file: {msg}"
    );
    // The same call with no CA to read builds, so the failure is the missing file and not
    // the code path around it.
    let (ctx, _) = new(&def("verify-full"), "rediss://cache:6379/2").unwrap();
    assert_eq!(ctx["url"], "rediss://cache:6379/2");
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

// ——— the verdict an answer earns, against a server of flesh and TCP ——————————
//
// `send` and `fetch` decide from the reply: an error reply whose code is on the
// transient list waits, any other error reply is refused by this key, silence is
// transient, and an empty render for a listing bails before a byte goes out.

/// Reads what the client sends (one command) and answers with `reply`, then closes.
async fn serve_reply(reply: &'static str) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        let mut chunk = [0u8; 1024];
        loop {
            // One command arrives; the read window is so the server does not answer
            // while the client is still writing the command bytes.
            match tokio::time::timeout(std::time::Duration::from_millis(30), s.read(&mut chunk))
                .await
            {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(n)) => {
                    if !chunk[..n].is_empty() {
                        break;
                    }
                }
            }
        }
        let _ = s.write_all(reply.as_bytes()).await;
        let _ = s.flush().await;
        let _ = s.shutdown().await;
    });
    addr
}

/// Accepts and reads, but never answers: the client's own timeout is the verdict.
async fn serve_silence() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        let mut chunk = [0u8; 1024];
        loop {
            match s.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    });
    addr
}

/// A db-0 client without credentials: no AUTH and no SELECT on connect, so the
/// server sees exactly one command.
fn client_on(addr: std::net::SocketAddr) -> Client {
    Client::new(
        RedisAddr::parse(&format!("redis://{addr}/0")).unwrap(),
        &TlsSpec::parse(None).unwrap(),
        None,
        None,
    )
    .unwrap()
}

fn pool() -> mysql_async::Pool {
    mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x")
}

#[tokio::test]
async fn an_error_reply_waits_or_is_refused_by_its_code() {
    let cases = [
        (
            "-LOADING Redis is loading the dataset in memory\r\n",
            "LOADING",
            false,
        ),
        (
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            "WRONGTYPE",
            true,
        ),
        ("-CLUSTERDOWN The cluster is down\r\n", "CLUSTERDOWN", false),
    ];
    for (reply, code, refused) in cases {
        let addr = serve_reply(reply).await;
        let client = client_on(addr);
        let expect = Expect::new(None).unwrap();
        let e = send(
            &client,
            "GET k",
            std::time::Duration::from_secs(2),
            &expect,
            &pool(),
        )
        .await
        .unwrap_err();
        let msg = format!("{e:#}");
        assert_eq!(crate::target::is_rejected(&e), refused, "`{code}`: {msg}");
        assert!(msg.contains(code), "{msg}");
        assert!(msg.contains("GET on"), "{msg}");
    }
}

#[tokio::test]
async fn no_reply_within_the_timeout_waits() {
    let addr = serve_silence().await;
    let client = client_on(addr);
    let expect = Expect::new(None).unwrap();
    let e = send(
        &client,
        "GET k",
        std::time::Duration::from_millis(150),
        &expect,
        &pool(),
    )
    .await
    .unwrap_err();
    let msg = format!("{e:#}");
    assert!(
        !crate::target::is_rejected(&e),
        "silence says not now: {msg}"
    );
    assert!(msg.contains("no reply within"), "{msg}");
}

#[tokio::test]
async fn an_ok_reply_sends_and_a_fetch_reads_the_reply() {
    let addr = serve_reply("+OK\r\n").await;
    let client = client_on(addr);
    let expect = Expect::new(None).unwrap();
    assert_eq!(
        send(
            &client,
            "SET k v",
            std::time::Duration::from_secs(2),
            &expect,
            &pool()
        )
        .await
        .unwrap(),
        Sent::Ok
    );
    // A listing reads its reply as the row; a render of nothing bails before a byte.
    let addr = serve_reply("$2\r\n17\r\n").await;
    let client = client_on(addr);
    let uses = Uses {
        body: true,
        ..Default::default()
    };
    let row = fetch(&client, "GET k", std::time::Duration::from_secs(2), uses)
        .await
        .unwrap();
    let _ = row;
    let client = client_on(addr);
    let e = fetch(&client, "   ", std::time::Duration::from_secs(2), uses)
        .await
        .unwrap_err();
    assert!(
        format!("{e:#}").contains("the list template rendered nothing"),
        "{e:#}"
    );
}

#[tokio::test]
async fn a_fetch_denied_by_the_server_fails_the_step() {
    let addr =
        serve_reply("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").await;
    let client = client_on(addr);
    let uses = Uses {
        body: true,
        ..Default::default()
    };
    let e = fetch(&client, "GET k", std::time::Duration::from_secs(2), uses)
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("WRONGTYPE"), "{e:#}");
}

/// Answers the commands it is sent one by one from `replies` (so a connect handshake
/// of `AUTH` and `SELECT` can be served before the real command).
async fn serve_replies(replies: &'static [&'static str]) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        for reply in replies {
            let mut chunk = [0u8; 1024];
            loop {
                match tokio::time::timeout(std::time::Duration::from_millis(30), s.read(&mut chunk))
                    .await
                {
                    Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                    Ok(Ok(n)) => {
                        if !chunk[..n].is_empty() {
                            break;
                        }
                    }
                }
            }
            if s.write_all(reply.as_bytes()).await.is_err() {
                return;
            }
            let _ = s.flush().await;
        }
        let _ = s.shutdown().await;
    });
    addr
}

#[tokio::test]
async fn the_connect_handshake_sends_auth_and_select_in_order() {
    // Credentials and a database: `AUTH user pw`, then `SELECT 2`, then the command.
    let addr = serve_replies(&["+OK\r\n", "+OK\r\n", "$2\r\n17\r\n"]).await;
    let client = Client::new(
        RedisAddr::parse(&format!("redis://{addr}/2")).unwrap(),
        &TlsSpec::parse(None).unwrap(),
        Some("keeper".into()),
        Some("s3cret".into()),
    )
    .unwrap();
    let expect = Expect::new(None).unwrap();
    assert_eq!(
        send(
            &client,
            "GET k",
            std::time::Duration::from_secs(2),
            &expect,
            &pool()
        )
        .await
        .unwrap(),
        Sent::Ok
    );
}

#[tokio::test]
async fn a_handshake_the_server_refuses_names_the_step_that_died() {
    // `AUTH` refused: the connect fails, naming AUTH (a credential being rotated is
    // waited out, not a key being refused: transient).
    let addr = serve_replies(&["-WRONGPASS invalid username-password pair\r\n"]).await;
    let client = Client::new(
        RedisAddr::parse(&format!("redis://{addr}/0")).unwrap(),
        &TlsSpec::parse(None).unwrap(),
        None,
        Some("s3cret".into()),
    )
    .unwrap();
    let expect = Expect::new(None).unwrap();
    let e = send(
        &client,
        "GET k",
        std::time::Duration::from_secs(2),
        &expect,
        &pool(),
    )
    .await
    .unwrap_err();
    let msg = format!("{e:#}");
    assert!(msg.contains("AUTH failed: WRONGPASS"), "{msg}");
    assert!(!crate::target::is_rejected(&e), "{msg}");
    // `SELECT` refused (db 9 of a five-database server): naming SELECT.
    let addr = serve_replies(&["-ERR DB index is out of range\r\n"]).await;
    let client = Client::new(
        RedisAddr::parse(&format!("redis://{addr}/9")).unwrap(),
        &TlsSpec::parse(None).unwrap(),
        None,
        None,
    )
    .unwrap();
    let e = send(
        &client,
        "GET k",
        std::time::Duration::from_secs(2),
        &expect,
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(format!("{e:#}").contains("SELECT 9 failed"), "{e:#}");
}

#[tokio::test]
async fn array_replies_are_read_well_and_malformed_ones_wait() {
    // Shape: the command of the client is one line; the server answers it and closes.
    for (reply, want_ok, want) in [
        ("*0\r\n", true, ""),                             // empty array
        ("*-1\r\n", true, ""),                            // nil array
        ("$-1\r\n", true, ""),                            // nil bulk
        ("*2\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n", true, ""), // nested array
        ("*abc\r\n", false, "bad array length"),
        ("$x\r\n", false, "bad bulk length"),
        ("*10000001\r\n", false, "too large"),
    ] {
        let addr = serve_reply(reply).await;
        let client = client_on(addr);
        let expect = Expect::new(None).unwrap();
        let got = send(
            &client,
            "GET k",
            std::time::Duration::from_secs(2),
            &expect,
            &pool(),
        )
        .await;
        match (want_ok, got) {
            (true, Ok(sent)) => assert_eq!(sent, Sent::Ok, "reply {reply:?}"),
            (true, Err(e)) => panic!("reply {reply:?} read as an error: {e:#}"),
            (false, Ok(sent)) => panic!("reply {reply:?} read as {sent:?}"),
            (false, Err(e)) => {
                let msg = format!("{e:#}");
                assert!(msg.contains(want), "reply {reply:?}: {msg}");
                assert!(!crate::target::is_rejected(&e), "reply {reply:?}: {msg}");
            }
        }
    }
}

#[tokio::test]
async fn a_connection_hung_up_mid_reply_waits() {
    // The server takes the command and closes without a byte of answer: the reply
    // cannot be read, which says nothing about the key — transient.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        let mut chunk = [0u8; 1024];
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(30), s.read(&mut chunk)).await;
        drop(s);
    });
    let client = client_on(addr);
    let expect = Expect::new(None).unwrap();
    let e = send(
        &client,
        "GET k",
        std::time::Duration::from_secs(2),
        &expect,
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(
        !crate::target::is_rejected(&e),
        "a hung-up connection is not a refusal: {e:#}"
    );
    assert!(format!("{e:#}").contains("GET on"), "{e:#}");
}

/// Answers the one command it is sent with `reply` — bytes, so a reply longer than a
/// literal can hold (a line over the line limit) can be served.
async fn serve_bytes(reply: Vec<u8>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        let mut chunk = [0u8; 1024];
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(30), s.read(&mut chunk)).await;
        let _ = s.write_all(&reply).await;
        let _ = s.flush().await;
        let _ = s.shutdown().await;
    });
    addr
}

#[tokio::test]
async fn a_reply_beyond_the_limits_of_a_reply_waits() {
    // The three shapes a server (or a hostile one) can answer with that no key caused: a
    // line past the line limit, a bulk whose declared length is past the bulk limit, and a
    // type byte that is not a reply type at all. All say `not now` — a reply the client
    // cannot read says nothing about the key, so none of them may drop it.
    for (reply, want) in [
        // A simple string with no CRLF within the line limit: the client may not buffer an
        // answer of a server that never stops talking.
        (
            format!("+{}\r\n", "x".repeat(128 * 1024)),
            "reply line longer than",
        ),
        // A bulk that declares more than the bulk limit: refused on the length, before any
        // of its bytes are read.
        ("$268435457\r\n".to_string(), "bulk longer than"),
        // A type byte that is not one of `+ - : $ *`.
        ("?oops\r\n".to_string(), "unknown redis reply type"),
    ] {
        let addr = serve_bytes(reply.as_bytes().to_vec()).await;
        let client = client_on(addr);
        let expect = Expect::new(None).unwrap();
        let e = send(
            &client,
            "GET k",
            std::time::Duration::from_secs(5),
            &expect,
            &pool(),
        )
        .await
        .unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains(want), "`{want}`: {msg}");
        assert!(
            !crate::target::is_rejected(&e),
            "`{want}` is the server's doing, not the key's: {msg}"
        );
    }
}
