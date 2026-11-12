use super::tests::cfg;
use super::*;

const DSN: (&str, &str) = ("LAZYWF_DSN", "mysql://app:secret@db:3306/shop");

#[test]
fn tls_levels_are_translated_or_refused() {
    for (level, params) in [
        (
            "required",
            "require_ssl=true&verify_ca=false&verify_identity=false",
        ),
        (
            "VERIFY-CA",
            "require_ssl=true&verify_ca=true&verify_identity=false",
        ),
        (
            "verify_identity",
            "require_ssl=true&verify_ca=true&verify_identity=true",
        ),
    ] {
        let c = cfg(&[DSN, ("LAZYWF_TLS", level)]).unwrap();
        assert_eq!(
            c.dsn,
            format!("mysql://app:secret@db:3306/shop?{params}"),
            "{level}"
        );
    }
    // A level that is off, and a value that is merely blank, both mean "no TLS".
    for off in ["disabled", "OFF", "false", ""] {
        let c = cfg(&[DSN, ("LAZYWF_TLS", off)]).unwrap();
        assert_eq!(c.dsn, "mysql://app:secret@db:3306/shop", "{off}");
    }
    // Parameters join whatever the DSN already carried.
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app@db:3306/shop?pool_max=20"),
        ("LAZYWF_TLS", "verify-full"),
    ])
    .unwrap();
    assert_eq!(
        c.dsn,
        "mysql://app@db:3306/shop?pool_max=20&require_ssl=true&verify_ca=true&verify_identity=true"
    );
    // A level nobody meant: refused rather than quietly left unencrypted.
    let err = cfg(&[DSN, ("LAZYWF_TLS", "passthrough")])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("LAZYWF_TLS must be disabled|required|verify-ca|verify-full"),
        "{err}"
    );
    assert!(
        cfg(&[DSN, ("LAZYWF_TLS", "   ")]).is_err(),
        "a blank level is not `off`"
    );
}

#[test]
fn a_ca_file_implies_verify_full_and_is_encoded() {
    // The file is looked for while the environment is still being read: a typo is
    // refused there rather than at the first handshake.
    let ca = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let encoded = format!("tls_ca={}", ca.replace('/', "%2F"));
    let c = cfg(&[DSN, ("LAZYWF_TLS_CA", ca.as_str())]).unwrap();
    assert!(
        c.dsn.starts_with(
            "mysql://app:secret@db:3306/shop?require_ssl=true&verify_ca=true\
                 &verify_identity=true&"
        ),
        "{}",
        c.dsn
    );
    assert!(c.dsn.ends_with(&encoded), "{}", c.dsn);
    // With TLS turned off explicitly the path travels along, but nothing is required.
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://app@db:3306/shop"),
        ("LAZYWF_TLS", "disabled"),
        ("LAZYWF_TLS_CA", ca.as_str()),
    ])
    .unwrap();
    assert!(!c.dsn.contains("require_ssl"), "{}", c.dsn);
    assert!(c.dsn.ends_with(&format!("?{encoded}")), "{}", c.dsn);

    let err = cfg(&[DSN, ("LAZYWF_TLS_CA", "/no/such/ca.pem")])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("TLS CA file `/no/such/ca.pem` not found"),
        "{err}"
    );
}

#[test]
fn second_accounts_replace_only_the_credentials() {
    // `@` inside the database name must not be read as the end of the credentials.
    let c = cfg(&[
        ("LAZYWF_DSN", "mysql://h:3306/sh@op"),
        ("LAZYWF_BINLOG_USER", "repl"),
        ("LAZYWF_BINLOG_PASSWORD", "p@ss"),
        ("LAZYWF_QUERY_USER", "reader"),
    ])
    .unwrap();
    assert_eq!(c.schema, "sh@op");
    assert_eq!(c.binlog_dsn, "mysql://repl:p%40ss@h:3306/sh@op");
    assert_eq!(c.query_dsn, "mysql://reader@h:3306/sh@op");
    assert!(c.query_account, "a separate query account is reported");
    // Without one, everything shares the main DSN.
    let c = cfg(&[DSN]).unwrap();
    assert_eq!(
        (c.binlog_dsn.as_str(), c.query_dsn.as_str()),
        (
            "mysql://app:secret@db:3306/shop",
            "mysql://app:secret@db:3306/shop"
        )
    );
    assert!(!c.query_account);
}

#[test]
fn an_assembled_dsn_quotes_its_parts() {
    // No password: the user alone, and an IPv6 host gets its brackets back.
    let c = cfg(&[
        ("LAZYWF_HOST", "::1"),
        ("LAZYWF_PORT", "3307"),
        ("LAZYWF_DATABASE", "shop"),
    ])
    .unwrap();
    assert_eq!(c.dsn, "mysql://root@[::1]:3307/shop");
    assert_eq!(c.schema, "shop");

    // The schema is asked for before the port is looked at: both are refused, in that order.
    let err = cfg(&[("LAZYWF_HOST", "db"), ("LAZYWF_PORT", "not-a-port")])
        .unwrap_err()
        .to_string();
    assert!(err.contains("LAZYWF_DATABASE is required"), "{err}");
    let err = cfg(&[
        ("LAZYWF_HOST", "db"),
        ("LAZYWF_DATABASE", "shop"),
        ("LAZYWF_PORT", "not-a-port"),
    ])
    .unwrap_err()
    .to_string();
    assert!(err.contains("LAZYWF_PORT must be a port number"), "{err}");
}

#[test]
fn numbers_are_clamped_to_what_the_engine_can_use() {
    let c = cfg(&[
        DSN,
        ("LAZYWF_POOL_SIZE", "1"),
        ("LAZYWF_SEND_CONCURRENCY", "0"),
        ("LAZYWF_CONFIG_POLL_MS", "10"),
        ("LAZYWF_STATUS_MS", "1"),
        ("LAZYWF_MAX_BACKOFF_MS", "1"),
        ("LAZYWF_VISIBILITY_DELAY_MS", "  "),
    ])
    .unwrap();
    assert_eq!(
        c.pool_size, 2,
        "a pool smaller than 2 cannot serve and replicate"
    );
    assert_eq!(
        c.send_concurrency, 1,
        "nothing may be sent with no worker at all"
    );
    assert_eq!(c.config_poll, Duration::from_millis(200));
    assert_eq!(c.status_every, Duration::from_millis(100));
    assert_eq!(c.max_backoff, Duration::from_millis(100));
    assert_eq!(
        c.visibility_delay,
        Duration::from_millis(20),
        "blank is the default"
    );

    for (k, v) in [
        ("LAZYWF_POOL_SIZE", "many"),
        ("LAZYWF_STATUS_MS", "-1"),
        ("LAZYWF_HEARTBEAT_MS", "5s"),
        ("LAZYWF_SERVER_ID", "nope"),
        ("LAZYWF_SERVER_ID", "0"),
    ] {
        let err = cfg(&[DSN, (k, v)]).unwrap_err().to_string();
        assert!(err.contains(k), "{k}={v} blames the wrong setting: {err}");
    }
}

#[test]
fn the_health_address_and_instance_are_checked_at_load() {
    let c = cfg(&[DSN, ("LAZYWF_HEALTH_ADDR", "0.0.0.0:9999")]).unwrap();
    assert_eq!(c.health_addr, Some(([0, 0, 0, 0], 9999).into()));
    let c = cfg(&[DSN, ("LAZYWF_HEALTH_ADDR", "[::1]:9000")]).unwrap();
    assert_eq!(
        c.health_addr,
        Some("[::1]:9000".parse().expect("an IPv6 health address parses"))
    );
    assert_eq!(
        cfg(&[DSN, ("LAZYWF_HEALTH_ADDR", "")]).unwrap().health_addr,
        None,
        "blank turns the health server off"
    );
    let err = cfg(&[DSN, ("LAZYWF_HEALTH_ADDR", "db:8081")])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("LAZYWF_HEALTH_ADDR `db:8081` is not host:port"),
        "{err}"
    );

    assert_eq!(
        cfg(&[DSN, ("LAZYWF_INSTANCE", "   ")]).unwrap().instance,
        "default"
    );
    let edge = "i".repeat(65);
    assert_eq!(
        cfg(&[DSN, ("LAZYWF_INSTANCE", &edge[..64])])
            .unwrap()
            .instance
            .len(),
        64
    );
    for bad in ["*", edge.as_str()] {
        assert!(cfg(&[DSN, ("LAZYWF_INSTANCE", bad)]).is_err(), "`{bad}`");
    }
}
