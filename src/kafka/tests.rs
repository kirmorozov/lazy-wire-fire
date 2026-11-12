use super::*;
use crate::model::RawTarget;
use serde_json::json;

fn spec(s: &str) -> TlsSpec {
    TlsSpec::parse(Some(s)).unwrap()
}

fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
    RawTarget {
        name: "events".into(),
        kind: Some("kafka".into()),
        endpoints: Some(endpoints.into()),
        tls: tls.map(Into::into),
        timeout_ms: None,
        expect: None,
        vars: Some(vars.into()),
    }
}

const CREDS: &str = r#"{"sasl": "scram-sha-256", "username": "env:LWF_TEST_KAFKA_USER", "password": "env:LWF_TEST_KAFKA_PW"}"#;

#[test]
fn endpoints() {
    let a = Addr::parse("kafka://broker:9092/orders").unwrap();
    let one: &[(String, u16)] = &[("broker".to_string(), 9092)];
    assert_eq!(
        (a.secure, &a.brokers[..], a.topic.as_str()),
        (false, one, "orders")
    );
    let a = Addr::parse("kafkas://b1.internal:8843,b2.internal/orders.v2").unwrap();
    let brokers: Vec<(String, u16)> = vec![
        ("b1.internal".to_string(), 8843),
        ("b2.internal".to_string(), 9093),
    ];
    assert_eq!(
        (a.secure, a.brokers, a.topic.clone()),
        (true, brokers, "orders.v2".to_string())
    );
    assert_eq!(
        Addr::parse("kafka://b1,b2/t").unwrap().bootstrap(),
        vec!["b1:9092", "b2:9092"]
    );
    for bad in [
        "http://h/t",
        "kafka://u:p@h/t",
        "kafka://h/t?tls=yes",
        "kafka://h/t#frag",
        "kafka://h",
        "kafka:///t",
        "kafka://:9092/t",
        "kafka://h:x/t",
        "kafka://[::1]/t",
        "kafka://h,-/t",
        "kafka://h/-",
        "kafka://h/..",
        "kafkas://h/a b",
    ] {
        assert!(Addr::parse(bad).is_err(), "accepted {bad}");
    }
}

#[test]
fn topic_names() {
    assert!(topic_name("a").is_ok());
    assert!(topic_name("orders.v2-x").is_ok());
    let long = "a".repeat(250);
    assert!(topic_name(&long).is_err());
    for bad in ["", "-", "..", "a b", "a/b"] {
        assert!(topic_name(bad).is_err(), "accepted {bad:?}");
    }
}

#[test]
fn tls_modes_map() {
    assert!(tls_config(&spec(""), false).unwrap().is_none());
    assert!(tls_config(&spec(""), true).unwrap().is_some());
    assert!(tls_config(&spec("required"), true).unwrap().is_some());
    assert!(tls_config(&spec("verify-ca"), true).unwrap().is_some());
    let e = tls_config(
        &spec(r#"{"mode": "verify-full", "server_name": "b"}"#),
        true,
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("server_name"), "{e}");
    let e = tls_config(&spec("off"), true).unwrap_err().to_string();
    assert!(e.contains("disabled"), "{e}");
    assert!(tls_config(&spec(r#"{"ca": "/nonexistent/lwf-ca.pem"}"#), true).is_err());
}

#[test]
fn sasl_forms() {
    assert_eq!(Sasl::parse("plain").unwrap(), Sasl::Plain);
    assert_eq!(Sasl::parse("Scram-SHA-256").unwrap(), Sasl::ScramSha256);
    assert_eq!(Sasl::parse("scram-sha-512").unwrap(), Sasl::ScramSha512);
    assert!(Sasl::parse("scram-sha-128").is_err());
    // No sasl: no credentials at all.
    assert!(sasl_of(&Map::new()).unwrap().is_none());
    // A credential without the mechanism is a load error…
    let mut v = Map::new();
    v.insert("username".into(), json!("u"));
    assert!(sasl_of(&v).is_err());
    // …and with it, both names, references only.
    v.insert("sasl".into(), json!("plain"));
    assert!(sasl_of(&v).is_err());
    let mut v = Map::new();
    v.insert("sasl".into(), json!("plain"));
    v.insert("username".into(), json!("lwf-user"));
    v.insert("password".into(), json!("env:P"));
    assert!(sasl_of(&v).unwrap().is_some());
    v.insert("password".into(), json!("literal"));
    assert!(sasl_of(&v).is_err());
}

#[test]
fn compression_forms() {
    assert_eq!(compression(&Map::new()).unwrap(), Compression::Zstd);
    for (s, c) in [
        ("none", Compression::NoCompression),
        ("gzip", Compression::Gzip),
        ("lz4", Compression::Lz4),
        ("zstd", Compression::Zstd),
    ] {
        let mut v = Map::new();
        v.insert("compression".into(), json!(s));
        assert_eq!(compression(&v).unwrap(), c, "{s}");
    }
    let mut v = Map::new();
    v.insert("compression".into(), json!("snappy"));
    assert!(compression(&v).is_err());
}

#[test]
fn the_batch_limit_is_the_broker_default_raised_by_vars() {
    assert_eq!(message_max_bytes(&Map::new()).unwrap(), MESSAGE_MAX_BYTES);
    let mut v = Map::new();
    v.insert("message_max_bytes".into(), json!(2_097_152));
    assert_eq!(message_max_bytes(&v).unwrap(), 2_097_152);
    for bad in [json!(0), json!(-5), json!("2MB"), json!(5_000_000_000i64)] {
        let mut v = Map::new();
        v.insert("message_max_bytes".into(), bad);
        assert!(message_max_bytes(&v).is_err());
    }
}

#[test]
fn rendered_form() {
    let a = Action::parse(
        br#"{"records": [{"value": {"qty": 3}, "headers": {"op": "upsert"}}, {"value": []}]}"#,
        2,
    )
    .unwrap()
    .unwrap();
    assert_eq!(a.records.len(), 2);
    assert_eq!(a.records[0].value, json!({"qty": 3}));
    assert_eq!(
        a.records[0].headers,
        BTreeMap::from([("op".to_string(), "upsert".to_string())])
    );
    assert_eq!(a.records[1].value, json!([]));
    assert!(a.records[1].headers.is_empty());
    assert!(Action::parse(b"{}", 1).is_err(), "no records for one key");
    assert!(Action::parse(b"not json", 1).is_err());
    assert!(Action::parse(b"null", 0).is_err());
    // The count must match the keys.
    let one = br#"{"records": [{"value": 1}]}"#;
    assert!(Action::parse(one, 0).is_err(), "one record, no keys");
    assert!(Action::parse(one, 2).is_err(), "one record, two keys");
    assert!(Action::parse(b"[]", 0).is_err());
    // Records carry value and headers only.
    let bad = br#"{"records": [{"value": 1, "key": "x"}]}"#;
    assert!(Action::parse(bad, 1).is_err());
    let bad = br#"{"records": [{"value": 1, "headers": {"op": 3}}]}"#;
    assert!(Action::parse(bad, 1).is_err());
    let bad = br#"{"records": [{"headers": {"op": "upsert"}}]}"#;
    assert!(Action::parse(bad, 1).is_err());
    let bad = br#"{"records": [{"value": null}]}"#;
    assert!(Action::parse(bad, 1).is_err());
    // Nothing rendered: an empty records list (a bare body is malformed, not empty).
    assert!(Action::parse(b"{}", 0).is_err());
    assert!(Action::parse(b"{\"records\": []}", 0).unwrap().is_none());
}

fn server_error(code: ProtocolError) -> ClientError {
    ClientError::ServerError {
        protocol_error: code,
        error_message: None,
        request: rskafka::client::error::RequestContext::Partition("orders".to_string(), 0),
        response: None,
        is_virtual: false,
    }
}

#[test]
fn outcomes_map_the_broker_answers() {
    for c in [
        ProtocolError::LeaderNotAvailable,
        ProtocolError::NotLeaderOrFollower,
        ProtocolError::RequestTimedOut,
        ProtocolError::BrokerNotAvailable,
        ProtocolError::ReplicaNotAvailable,
        ProtocolError::NotEnoughReplicas,
        ProtocolError::NotEnoughReplicasAfterAppend,
        ProtocolError::OperationNotAttempted,
        ProtocolError::NetworkException,
        ProtocolError::KafkaStorageError,
        ProtocolError::LogDirNotFound,
        ProtocolError::ThrottlingQuotaExceeded,
        ProtocolError::PreferredLeaderNotAvailable,
        // Stale routing on a produce (the topic exists — a topic that does not is refused
        // at the metadata check, see `a_moved_partition_retries_but_a_missing_topic_refuses`)
        // and the codes the client cannot judge: none of them can drop a key.
        ProtocolError::UnknownTopicOrPartition,
        ProtocolError::UnknownServerError,
        ProtocolError::Unknown(4999),
    ] {
        assert!(
            matches!(code_outcome(c), Outcome::Transient(_)),
            "{c:?} must be transient"
        );
    }
    for c in [
        ProtocolError::RecordListTooLarge,
        ProtocolError::MessageTooLarge,
        ProtocolError::InvalidRecord,
        ProtocolError::TopicAuthorizationFailed,
        ProtocolError::SaslAuthenticationFailed,
        ProtocolError::SecurityDisabled,
        ProtocolError::UnsupportedCompressionType,
        ProtocolError::InvalidTopicException,
    ] {
        assert!(
            matches!(code_outcome(c), Outcome::Refused(_)),
            "{c:?} must be refused"
        );
    }
    // A connection failure of the whole call is transient…
    let conn = ClientError::Connection(ConnectionError::Metadata(
        rskafka::client::error::RequestError::IO(std::io::Error::other("the stream broke")),
    ));
    assert!(matches!(client_error_outcome(&conn), Outcome::Transient(_)));
    // …and a SASL failure of it is a refusal.
    let sasl = ClientError::Connection(ConnectionError::SaslFailed(
        rskafka::SaslError::UnsupportedSaslMechanism,
    ));
    match client_error_outcome(&sasl) {
        Outcome::Refused(w) => assert!(w.contains("SASL"), "{w}"),
        o => panic!("{o:?}"),
    }
    // The retriable-error deadline is transient (the broker, not the data, is unwell).
    assert!(matches!(
        client_error_outcome(&ClientError::RetryFailed(
            rskafka::BackoffError::DeadlineExceded {
                deadline: Duration::from_secs(10),
                source: Box::new(std::io::Error::other("last")),
            }
        )),
        Outcome::Transient(_)
    ));
    // A refused code stays a refusal through the whole-call path…
    match client_error_outcome(&server_error(ProtocolError::RecordListTooLarge)) {
        Outcome::Refused(w) => assert!(w.contains("RecordListTooLarge"), "{w}"),
        o => panic!("{o:?}"),
    }
    // …and an unknown code retries, with the code in the outcome's text: the safe
    // default is the one that cannot drop a key.
    match client_error_outcome(&server_error(ProtocolError::UnknownServerError)) {
        Outcome::Transient(w) => assert!(w.contains("UnknownServerError"), "{w}"),
        o => panic!("{o:?}"),
    }
    match client_error_outcome(&server_error(ProtocolError::Unknown(4999))) {
        Outcome::Transient(w) => assert!(w.contains("4999"), "{w}"),
        o => panic!("{o:?}"),
    }
}

/// The two faces of an unknown topic: a *produce* answering `UnknownTopicOrPartition` is
/// stale routing (Kafka classes it retriable) and retries, while a topic the metadata does
/// not list at all stays a refusal — the difference between a retry and dropped keys.
#[test]
fn a_moved_partition_retries_but_a_missing_topic_refuses() {
    assert!(matches!(
        client_error_outcome(&server_error(ProtocolError::UnknownTopicOrPartition)),
        Outcome::Transient(_)
    ));
    let e = missing_refusal("kafka://b:9092/orders", "orders");
    assert!(crate::target::is_rejected(&e), "{e:#}");
    assert!(e.to_string().contains("`orders` is unknown"), "{e:#}");
}

/// The codes the client splits a refused batch over (the broker refused the record set and
/// named no record) and the ones it does not (a property of the topic or the account, which
/// every record of the batch shares).
#[test]
fn only_a_per_record_refusal_splits_the_batch() {
    for c in [
        ProtocolError::RecordListTooLarge,
        ProtocolError::MessageTooLarge,
        ProtocolError::InvalidRecord,
    ] {
        assert!(is_per_record_refusal(&server_error(c)), "{c:?}");
    }
    for c in [
        ProtocolError::TopicAuthorizationFailed,
        ProtocolError::SaslAuthenticationFailed,
        ProtocolError::UnsupportedCompressionType,
        ProtocolError::InvalidTopicException,
        ProtocolError::InvalidRequiredAcks,
        // Transient codes are not refusals at all, so they never split a batch either.
        ProtocolError::UnknownTopicOrPartition,
        ProtocolError::LeaderNotAvailable,
        ProtocolError::NotEnoughReplicas,
    ] {
        assert!(!is_per_record_refusal(&server_error(c)), "{c:?}");
    }
    // A connection failure is not a refusal either.
    assert!(!is_per_record_refusal(&ClientError::Connection(
        ConnectionError::SaslFailed(rskafka::SaslError::UnsupportedSaslMechanism),
    )));
}

#[test]
fn the_key_hash_is_the_brokers_partitioner() {
    // The vectors are the partitions the test broker's own default partitioner gives these
    // keys: produced with `rpk topic produce` (no `--partition`, so the client's default
    // partitioner chooses) and read back with `rpk topic consume -f '%p %k'`, on a topic of
    // 3 and of 8 partitions. They pin the hash, and with it the promise that a key produced
    // by lazywf lands where any other client's default partitioner puts it.
    assert_eq!(murmur2(b""), 275_646_681);
    assert_eq!(murmur2(b"a"), -1_563_381_124);
    assert_eq!(murmur2(b"b"), -1_853_091_852);
    assert_eq!(murmur2(b"k"), -1_567_496_736);
    assert_eq!(murmur2(b"orders-42"), 479_277_457);
    assert_eq!(murmur2(b"1"), -1_993_445_489);
    assert_eq!(murmur2(b"hello world"), 1_221_641_059);
    assert_eq!(murmur2(b"long-key-abcdefghijklmnop"), 483_618_708);

    // The partition of each key, as the broker chose it (its own partitioning, not ours).
    let p8: Vec<i32> = (0..8).collect();
    for (key, want) in [
        ("a", 4),
        ("b", 4),
        ("k", 0),
        ("orders-42", 1),
        ("1", 7),
        ("2", 0),
        ("3", 7),
        ("5", 6),
        ("key", 1),
        ("hello world", 3),
        ("xxxxxxx", 7),
        ("user-1000", 7),
        ("long-key-abcdefghijklmnop", 4),
    ] {
        assert_eq!(partition_of(key, &p8), want, "{key} (8 partitions)");
    }
    // An odd count separates `toPositive` from the absolute value: these four keys have a
    // negative hash, where the two differ (abs would put "1" and "3" on 2 and "b" and "k"
    // on 0, the broker says otherwise).
    let p3: Vec<i32> = (0..3).collect();
    for (key, want) in [
        ("1", 0),
        ("3", 2),
        ("b", 2),
        ("k", 2),
        ("a", 1),
        ("key", 1),
        ("5", 0),
    ] {
        assert_eq!(partition_of(key, &p3), want, "{key} (3 partitions)");
    }
    // One key, one partition, stable across calls.
    let big = "k".repeat(1000);
    assert_eq!(partition_of(&big, &p8), partition_of(&big, &p8));
}

#[test]
fn from_row_checks() {
    let ok = TargetDef::from_row(&raw("kafka://broker:9092/orders", None, CREDS)).unwrap();
    assert_eq!(ok.kind, crate::model::Kind::Kafka);
    assert!(ok.template_vars().get("password").is_none());
    assert!(ok.template_vars().get("username").is_some());
    assert!(ok.template_vars().get("sasl").is_some());
    // Anonymous: no sasl vars at all.
    assert!(TargetDef::from_row(&raw("kafka://broker/orders", None, "{}")).is_ok());
    // No sasl with credentials is a load error, and so is a literal password.
    assert!(TargetDef::from_row(&raw(
        "kafka://broker/orders",
        None,
        r#"{"username": "env:U", "password": "env:P"}"#
    ))
    .is_err());
    assert!(TargetDef::from_row(&raw(
        "kafka://broker/orders",
        None,
        r#"{"sasl": "plain", "username": "env:U", "password": "literal"}"#
    ))
    .is_err());
    // The URL forms refuse userinfo and query strings.
    assert!(TargetDef::from_row(&raw("kafka://u:p@broker/orders", None, "{}")).is_err());
    assert!(TargetDef::from_row(&raw("kafka://broker/orders?x=1", None, "{}")).is_err());
    // TLS: disabled on kafkas is refused.
    assert!(TargetDef::from_row(&raw("kafkas://broker/orders", Some("off"), "{}")).is_err());
    // Exactly one endpoint: one broker set and its topic (a batch answers per item, which
    // goes to a single endpoint, as for sqs, kinesis and lambda).
    assert!(TargetDef::from_row(&raw(
        r#"["kafka://b1/orders", "kafka://b2/orders"]"#,
        None,
        "{}"
    ))
    .is_err());
    assert!(TargetDef::from_row(&raw("[]", None, "{}")).is_err());
    // A kafka endpoint on another kind is refused.
    assert!(TargetDef::from_row(&RawTarget {
        name: "t".into(),
        kind: Some("http".into()),
        endpoints: Some("kafka://broker/orders".into()),
        ..Default::default()
    })
    .is_err());
    assert!(crate::model::Kind::Kafka.append_only());
    assert!(crate::model::Kind::Kafka.reports_items());
    assert!(!crate::model::Kind::Kafka.is_http());
    assert_eq!(
        crate::model::Kind::Kafka.batch_bytes_limit(),
        Some(MESSAGE_MAX_BYTES)
    );
}

#[test]
fn rule_load_checks() {
    let def = |fire_per: FirePer, bytes: Option<usize>| {
        let mut d = RuleDef::placeholder(&crate::model::RawRule {
            name: "r".into(),
            enabled: true,
            ..Default::default()
        });
        d.fire_per = fire_per;
        d.batch_max_bytes = bytes;
        d
    };
    // Both fire_per values are fine for kafka…
    assert!(rule_errors(&def(FirePer::Key, None), MESSAGE_MAX_BYTES).is_empty());
    assert!(rule_errors(&def(FirePer::Batch, Some(1024)), MESSAGE_MAX_BYTES).is_empty());
    // …but a batch over the limit is refused at load.
    let e = rule_errors(
        &def(FirePer::Batch, Some(MESSAGE_MAX_BYTES + 1)),
        MESSAGE_MAX_BYTES,
    );
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("batch_max_bytes"), "{e:?}");
    // A raised limit raises the rule's allowance with it.
    assert!(rule_errors(&def(FirePer::Batch, Some(2_000_000)), 2_097_152).is_empty());
}
