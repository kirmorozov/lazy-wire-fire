//! The TLS default of 0.3.0: a source host that is not loopback is connected to with TLS
//! unless the operator says otherwise, and saying otherwise is loud.

use super::tests::cfg;
use super::*;

const REQ: &str = "require_ssl=true&verify_ca=false&verify_identity=false";

/// A `get` of the kind `from_lookup` takes, from pairs.
fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let m: std::collections::HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |k| m.get(k).cloned()
}

fn dsn_of(host: &str) -> String {
    format!("mysql://app:pw@{host}:3306/shop")
}

/// The host of each connection decides its own default: the machine's own server needs no
/// certificate, a server somewhere else is reached over an encrypted channel.
#[test]
fn a_local_source_stays_plaintext_and_a_remote_one_requires_tls() {
    for host in [
        "127.0.0.1",
        "127.0.0.5", // the whole of 127/8 is the machine itself
        "localhost",
        "LOCALHOST",
        "localhost.",
        "[::1]",
    ] {
        let c = cfg(&[("LAZYWF_DSN", &dsn_of(host))]).unwrap();
        assert_eq!(c.dsn, dsn_of(host), "{host} must stay plaintext");
        assert!(!c.dsn.contains("require_ssl"), "{host}");
    }
    for host in [
        "db.example.com",
        "db",
        "10.0.0.7",
        "192.168.1.9",
        "203.0.113.5",
        "172.16.17.2",
        "mysql",
    ] {
        let c = cfg(&[("LAZYWF_DSN", &dsn_of(host))]).unwrap();
        assert_eq!(c.dsn, format!("{}?{REQ}", dsn_of(host)), "{host}");
    }
    // The assembled form is decided the same way (it is the same host string).
    let c = cfg(&[
        ("LAZYWF_HOST", "db.example.com"),
        ("LAZYWF_DATABASE", "shop"),
    ])
    .unwrap();
    assert!(c.dsn.ends_with(&format!("?{REQ}")), "{}", c.dsn);
    let c = cfg(&[("LAZYWF_HOST", "127.0.0.1"), ("LAZYWF_DATABASE", "shop")]).unwrap();
    assert!(!c.dsn.contains("require_ssl"), "{}", c.dsn);
    // The assembled form brackets IPv6, and a loopback address stays plaintext either way.
    let c = cfg(&[("LAZYWF_HOST", "::1"), ("LAZYWF_DATABASE", "shop")]).unwrap();
    assert_eq!(c.dsn, "mysql://root@[::1]:3306/shop");
}

/// A local socket never leaves the machine, whichever way the DSN spells it.
#[test]
fn a_local_socket_is_a_local_source() {
    let dsn = "mysql://app:pw@127.0.0.1:3306/shop?socket=/run/mysqld/mysqld.sock";
    assert!(is_loopback_dsn(dsn));
    let c = cfg(&[("LAZYWF_DSN", dsn)]).unwrap();
    assert!(!c.dsn.contains("require_ssl"), "{}", c.dsn);

    let dsn = "mysql://app:pw@db.example.com/shop?socket=/run/mysqld/mysqld.sock";
    assert!(is_loopback_dsn(dsn), "the socket wins over the name");
    let c = cfg(&[("LAZYWF_DSN", dsn)]).unwrap();
    assert!(!c.dsn.contains("require_ssl"), "{}", c.dsn);

    // A DSN with no host at all is a socket connection too.
    assert!(is_loopback_dsn("mysql://app:pw@/shop"));
    assert_eq!(dsn_host("mysql://app:pw@/shop"), "");
}

/// An explicit level is what it says, on any host: `disabled` on a remote one is the loud
/// opt-out, `required` on loopback is the operator asking for TLS anyway.
#[test]
fn an_explicit_level_wins_over_the_host_default() {
    for off in ["disabled", "OFF", "false", "  off  "] {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@db.example.com:3306/shop"),
            ("LAZYWF_TLS", off),
        ])
        .unwrap();
        assert_eq!(c.dsn, "mysql://app:pw@db.example.com:3306/shop", "{off}");
    }
    for (level, params) in [
        ("required", REQ),
        (
            "verify-ca",
            "require_ssl=true&verify_ca=true&verify_identity=false",
        ),
        (
            "verify-full",
            "require_ssl=true&verify_ca=true&verify_identity=true",
        ),
    ] {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop"),
            ("LAZYWF_TLS", level),
        ])
        .unwrap();
        assert_eq!(
            c.dsn,
            format!("mysql://app:pw@127.0.0.1:3306/shop?{params}"),
            "{level}"
        );
    }
}

/// A blank value is the variable being unset, not `disabled`: that is the difference between
/// an operator who configured nothing and an operator who turned TLS off.
#[test]
fn a_blank_level_is_an_unset_one() {
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@db.example.com:3306/shop"),
        ("LAZYWF_TLS", ""),
    ])
    .unwrap();
    assert!(c.dsn.ends_with(&format!("?{REQ}")), "{}", c.dsn);
    // Whitespace is not a level either, but it is not nothing: it is refused.
    assert!(cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@db.example.com:3306/shop"),
        ("LAZYWF_TLS", "   "),
    ])
    .is_err());
}

/// Each connection is decided by its own host: a local source with a replica or a reader
/// somewhere else encrypts that one and not this one.
#[test]
fn every_connection_is_decided_by_its_own_host() {
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop"),
        (
            "LAZYWF_BINLOG_DSN",
            "mysql://repl:rp@db.example.com:3306/shop",
        ),
    ])
    .unwrap();
    assert_eq!(c.dsn, "mysql://app:pw@127.0.0.1:3306/shop");
    assert_eq!(
        c.binlog_dsn,
        format!("mysql://repl:rp@db.example.com:3306/shop?{REQ}")
    );

    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@db.example.com:3306/shop"),
        ("LAZYWF_QUERY_DSN", "mysql://ro:rp@127.0.0.1:3306/shop"),
    ])
    .unwrap();
    assert!(c.dsn.ends_with(&format!("?{REQ}")), "{}", c.dsn);
    assert_eq!(c.query_dsn, "mysql://ro:rp@127.0.0.1:3306/shop");

    // An account taken from the main DSN carries the main DSN's parameters.
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@db.example.com:3306/shop"),
        ("LAZYWF_BINLOG_USER", "repl"),
    ])
    .unwrap();
    assert_eq!(
        c.binlog_dsn,
        format!("mysql://repl@db.example.com:3306/shop?{REQ}")
    );
}

/// `tls_for` in itself: the parameters it chooses and, in the second slot, whether the
/// process owes the operator one loud WARN about a plaintext remote connection.
#[test]
fn the_plaintext_remote_choice_is_the_one_worth_warning_about() {
    let remote = "mysql://a:b@db.example.com:3306/shop";
    let local = "mysql://a:b@127.0.0.1:3306/shop";

    let (tls, warn) = tls_for(&lookup(&[]), "LAZYWF", remote).unwrap();
    assert_eq!(tls.as_deref(), Some("required"));
    assert!(!warn, "the default needs no warning");
    let (tls, warn) = tls_for(&lookup(&[]), "LAZYWF", local).unwrap();
    assert_eq!(tls, None);
    assert!(!warn);

    let (tls, warn) = tls_for(&lookup(&[("LAZYWF_TLS", "disabled")]), "LAZYWF", remote).unwrap();
    assert_eq!(tls.as_deref(), Some("disabled"));
    assert!(warn, "plaintext to a remote host is what the WARN is for");

    let (_, warn) = tls_for(&lookup(&[("LAZYWF_TLS", "disabled")]), "LAZYWF", local).unwrap();
    assert!(!warn, "loopback plaintext is the ordinary case");

    let (tls, warn) = tls_for(&lookup(&[("LAZYWF_TLS", "required")]), "LAZYWF", remote).unwrap();
    assert_eq!(tls.as_deref(), Some("required"));
    assert!(!warn);

    let ca = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let (tls, warn) =
        tls_for(&lookup(&[("LAZYWF_TLS_CA", ca.as_str())]), "LAZYWF", remote).unwrap();
    assert_eq!(tls.as_deref(), Some("verify-full"));
    assert!(!warn);
}

/// The host of a DSN as lazywf reads it, which is the host the default and the allow-list
/// are both about: no DNS, nothing resolved, and userinfo that contains an `@` stays userinfo.
#[test]
fn dsn_host_is_the_host_as_written() {
    for (dsn, host) in [
        ("mysql://app:pw@db.example.com:3306/shop", "db.example.com"),
        ("mysql://db.example.com:3306/shop", "db.example.com"),
        ("mysql://db.example.com/shop", "db.example.com"),
        ("mysql://app@10.0.0.7/shop", "10.0.0.7"),
        ("mysql://app:p%40w@db:3306/shop?pool_max=20", "db"),
        ("mysql://u@x:pw@db:3306/shop", "db"),
        ("mysql://app@127.0.0.1/shop", "127.0.0.1"),
        ("mysql://app@[::1]:3307/shop", "::1"),
        ("mariadb://app@db.example.com:3306/shop", "db.example.com"),
        ("mysql://app:pw@/shop", ""),
    ] {
        assert_eq!(dsn_host(dsn), host, "{dsn}");
    }
    assert!(is_loopback_dsn("mysql://app@[::1]:3307/shop"));
    assert!(!is_loopback_dsn("mysql://app@[fd00::1]:3307/shop"));
    assert!(!is_loopback_dsn("mysql://app@localhost.example.com/shop"));
}

/// `LAZYWF_HEALTH_TOKEN` gates `/status` and `/metrics` when it is set; blank is the same as
/// unset. The value is kept for the two listeners and never shown.
#[test]
fn the_health_token_is_kept_but_never_shown() {
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop"),
        ("LAZYWF_HEALTH_TOKEN", "s3cr3t-token"),
    ])
    .unwrap();
    assert_eq!(c.health_token.as_deref(), Some("s3cr3t-token"));
    let shown = format!("{c:?}");
    assert!(!shown.contains("s3cr3t-token"), "{shown}");
    assert!(shown.contains("health_token: Some(\"set\")"), "{shown}");
    for blank in ["", "   "] {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop"),
            ("LAZYWF_HEALTH_TOKEN", blank),
        ])
        .unwrap();
        assert_eq!(c.health_token, None, "{blank:?}");
    }
    // The value is trimmed on its way in: a token copied from a file into a shell keeps its
    // newline, and that newline is not part of the secret.
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop"),
        ("LAZYWF_HEALTH_TOKEN", "  s3cr3t-token\n"),
    ])
    .unwrap();
    assert_eq!(c.health_token.as_deref(), Some("s3cr3t-token"));
}
