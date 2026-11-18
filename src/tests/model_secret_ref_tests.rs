//! The credential fields of `vars` (password, token, key) are references only: the target
//! rows are SQL, so a literal sitting in one of them is a credential readable by anyone with
//! a SELECT grant, and one is refused while the row is read.

use super::*;

fn raw(kind: &str, endpoints: &str, vars: &str) -> RawTarget {
    RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoints.into()),
        vars: Some(vars.into()),
        ..Default::default()
    }
}

/// `base` with the field `field` set to the string `value`.
fn with(base: &str, field: &str, value: &str) -> String {
    let mut v: Map<String, Value> = serde_json::from_str(base).unwrap();
    v.insert(field.into(), Value::String(value.into()));
    serde_json::to_string(&v).unwrap()
}

/// (kind, an endpoint of it, the `vars` a target of that kind loads with) — the kinds whose
/// credential fields 0.3.0 added to the guard. The base carries the field as a reference, so
/// the target loads; the test replaces it with a literal.
const CASES: &[(&str, &str, &str)] = &[
    (
        "http.basic",
        "https://api.example.test:8443/hook",
        r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
    ),
    (
        "http.bearer",
        "https://api.example.test:8443/hook",
        r#"{"token":"env:LWF_TEST_PW"}"#,
    ),
    (
        "http.header",
        "https://api.example.test:8443/hook",
        r#"{"header":"X-Api-Key","value":"env:LWF_TEST_PW"}"#,
    ),
    (
        "redis",
        "redis://cache.example.test:6380/0",
        r#"{"password":"env:LWF_TEST_PW"}"#,
    ),
    (
        "mysql",
        "mysql://db.example.test:3306/app",
        r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
    ),
    (
        "pgsql",
        "postgres://pg.example.test:5432/app",
        r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
    ),
    (
        "amqp",
        "amqp://mq.example.test:5672/",
        r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
    ),
    (
        "smtp",
        "smtp://mail.example.test:587",
        r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
    ),
];

/// Every field of the table is checked, per kind: a literal is refused, a reference loads.
#[test]
fn a_literal_credential_is_refused_field_by_field() {
    for (kind, endpoint, base) in CASES {
        let k = Kind::parse(kind).unwrap();
        assert_eq!(k.secret_ref_fields().len(), 1, "{kind}");
        let field = k.secret_ref_fields()[0];
        assert!(
            TargetDef::from_row(&raw(kind, endpoint, base)).is_ok(),
            "{kind}: {base}"
        );
        for literal in ["hunter2", "s3cret with spaces", "env", "file", "Env:X", " "] {
            let e = format!(
                "{:#}",
                TargetDef::from_row(&raw(kind, endpoint, &with(base, field, literal)))
                    .err()
                    .unwrap_or_else(|| panic!("{kind}: `{literal}` in `{field}` loaded"))
            );
            assert!(
                e.contains(&format!("`vars.{field}`")),
                "{kind} {literal}: {e}"
            );
            assert!(e.contains("must be a reference"), "{kind} {literal}: {e}");
            assert!(e.contains("env:NAME"), "{kind} {literal}: {e}");
            // The refusal names the field, never the secret it refuses.
            if !matches!(literal, "env" | "file" | " ") {
                assert!(!e.contains(literal), "{kind} {literal}: {e}");
            }
        }
        // The documented flows load: an environment variable and a file.
        for reference in ["env:LWF_TEST_PW", "file:/etc/lwf/secrets/pw"] {
            assert!(
                TargetDef::from_row(&raw(kind, endpoint, &with(base, field, reference))).is_ok(),
                "{kind}: {reference}"
            );
        }
    }
}

/// The audit, as a table: which field of which kind is a credential. A new kind with a
/// password has to appear here, or this test says so.
#[test]
fn the_table_names_exactly_the_audited_fields() {
    for (kind, fields) in [
        (Kind::Http, &[][..]),
        (Kind::HttpBasic, &["password"][..]),
        (Kind::HttpBearer, &["token"][..]),
        (Kind::HttpHeader, &["value"][..]),
        (
            Kind::HttpS3,
            &["access_key_ref", "secret_key_ref", "session_token_ref"][..],
        ),
        (
            Kind::Sqs,
            &["access_key_ref", "secret_key_ref", "session_token_ref"][..],
        ),
        (
            Kind::Kinesis,
            &["access_key_ref", "secret_key_ref", "session_token_ref"][..],
        ),
        (
            Kind::Lambda,
            &["access_key_ref", "secret_key_ref", "session_token_ref"][..],
        ),
        (
            Kind::EventBridge,
            &["access_key_ref", "secret_key_ref", "session_token_ref"][..],
        ),
        (
            Kind::Sns,
            &["access_key_ref", "secret_key_ref", "session_token_ref"][..],
        ),
        (Kind::PubSub, &["credentials_ref"][..]),
        (Kind::Amqp, &["password"][..]),
        (Kind::Smtp, &["password"][..]),
        (Kind::Redis, &["password"][..]),
        (Kind::MySql, &["password"][..]),
        (Kind::PgSql, &["password"][..]),
        (Kind::Kafka, &["password"][..]),
        (Kind::RabbitMqStream, &["password"][..]),
        (Kind::Grpc, &["token"][..]),
        (Kind::ServiceBus, &["key", "password"][..]),
    ] {
        assert_eq!(kind.secret_ref_fields(), fields, "{}", kind.name());
    }
}

/// The kinds that already refused a literal credential keep refusing it, with their own
/// wording and ahead of the table's (the kind knows best what its credential means).
#[test]
fn the_kinds_that_checked_their_own_credentials_still_do() {
    for (kind, endpoint, field, base) in [
        (
            "kafka",
            "kafka://mq.example.test:9094/orders",
            "password",
            r#"{"username":"u","sasl":"plain"}"#,
        ),
        (
            "rabbitmq-stream",
            "rabbitmq-stream://mq.example.test:5552/orders",
            "password",
            r#"{"username":"lwf"}"#,
        ),
        (
            "servicebus",
            "sb://contoso.servicebus.windows.net/orders",
            "key",
            r#"{"key_name":"lwf-listen"}"#,
        ),
        (
            "grpc",
            "grpc://ingest.example.test:9000/catalog.v1.Ingest/Push",
            "token",
            r#"{"descriptor_set":"file:/etc/lwf/catalog.pb","item_type":"catalog.v1.Item"}"#,
        ),
        (
            "sqs",
            "aws-sqs://eu-west-1/123456789012/orders",
            "secret_key_ref",
            r#"{"access_key_ref":"env:LWF_AK"}"#,
        ),
        (
            "pubsub",
            "gc-pubsub://lwf-test/orders",
            "credentials_ref",
            "{}",
        ),
    ] {
        let e = format!(
            "{:#}",
            TargetDef::from_row(&raw(kind, endpoint, &with(base, field, "hunter2")))
                .err()
                .unwrap_or_else(|| panic!("{kind}: a literal `{field}` loaded"))
        );
        assert!(e.contains("must be a reference"), "{kind} {field}: {e}");
        assert!(e.contains(field), "{kind} {field}: {e}");
    }
}

/// A username, a header name, a SAS key name: these identify the credential, they are not
/// it, so they stay literals (a reference works there too, but nothing demands one).
#[test]
fn what_identifies_a_credential_is_not_one() {
    for (kind, endpoint, vars) in [
        (
            "http.basic",
            "https://api.example.test:8443/hook",
            r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
        ),
        (
            "http.header",
            "https://api.example.test:8443/hook",
            r#"{"header":"X-Api-Key","value":"env:LWF_TEST_PW"}"#,
        ),
        (
            "amqp",
            "amqp://mq.example.test:5672/",
            r#"{"username":"guest","password":"env:LWF_TEST_PW"}"#,
        ),
        (
            "smtp",
            "smtp://mail.example.test:587",
            r#"{"username":"lwf","password":"env:LWF_TEST_PW"}"#,
        ),
        (
            "pgsql",
            "postgres://pg.example.test:5432/app",
            r#"{"username":"lwf","password":"env:LWF_TEST_PW"}"#,
        ),
    ] {
        assert!(
            TargetDef::from_row(&raw(kind, endpoint, vars)).is_ok(),
            "{kind}: {vars}"
        );
    }
    // The plain http kind has no credential of its own. When it signs, the secret is a
    // reference by `crate::sign`'s own rule, which this table does not repeat.
    assert!(Kind::Http.secret_ref_fields().is_empty());
    assert!(TargetDef::from_row(&raw("http", "https://api.example.test:8443/hook", "{}")).is_ok());
    let e = format!(
        "{:#}",
        TargetDef::from_row(&raw(
            "http",
            "https://api.example.test:8443/hook",
            r#"{"sign":"hmac","sign_secret_ref":"shared-key"}"#
        ))
        .err()
        .unwrap()
    );
    assert!(
        e.contains("sign_secret_ref") && e.contains("must be a reference"),
        "{e}"
    );
}

/// An empty credential carries no secret, so — unlike every other literal — it is not refused:
/// basic auth with an empty password (and an empty bearer token or header value) loads. A
/// whitespace value is a real (whitespace) secret and is still refused as a literal, with the
/// same message as any other literal.
///
/// PROVISIONAL (the owner's ruling of 2026-11-18): this pins the behaviour as it is today, it does
/// not argue for it. Allowing the empty literal may be rejected later; if it is, the three `is_ok`
/// asserts become refusal asserts with the whitespace message and `need` in `TargetDef::from_row`
/// goes back to `var(&vars, k).is_some()`.
#[test]
fn an_empty_credential_literal_is_accepted() {
    for (kind, endpoint, base, field) in [
        (
            "http.basic",
            "https://api.example.test:8443/hook",
            r#"{"username":"u","password":"env:LWF_TEST_PW"}"#,
            "password",
        ),
        (
            "http.bearer",
            "https://api.example.test:8443/hook",
            r#"{"token":"env:LWF_TEST_PW"}"#,
            "token",
        ),
        (
            "http.header",
            "https://api.example.test:8443/hook",
            r#"{"header":"X-Api-Key","value":"env:LWF_TEST_PW"}"#,
            "value",
        ),
    ] {
        assert!(
            TargetDef::from_row(&raw(kind, endpoint, &with(base, field, ""))).is_ok(),
            "{kind}: an empty `{field}` was refused"
        );
        // Whitespace is a secret value, not an empty one: still refused as a literal.
        let e = format!(
            "{:#}",
            TargetDef::from_row(&raw(kind, endpoint, &with(base, field, " ")))
                .err()
                .unwrap_or_else(|| panic!("{kind}: a whitespace `{field}` was accepted"))
        );
        assert!(
            e.contains(&format!("`vars.{field}`"))
                && e.contains("must be a reference")
                && e.contains("env:NAME"),
            "{kind} whitespace {field}: {e}"
        );
    }
    // How far that acceptance reaches: the kinds whose load check asks through `need` —
    // `http.basic`, `http.bearer`, `http.header`. A queue kind with the same field name, whose own
    // check requires the credential as text, still refuses an empty one; the refusal is
    // `crate::targets::amqp::check`'s own wording and not `need`'s, which is the proof that the
    // change never reached it.
    let e = format!(
        "{:#}",
        TargetDef::from_row(&raw(
            "amqp",
            "amqp://mq.example.test:5672/",
            r#"{"username":"guest","password":""}"#
        ))
        .err()
        .unwrap_or_else(|| panic!("amqp: an empty `password` was accepted"))
    );
    assert!(
        e.contains("kind amqp needs `vars.password`"),
        "amqp empty password: {e}"
    );
}
