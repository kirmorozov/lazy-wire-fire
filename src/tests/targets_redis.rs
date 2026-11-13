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
