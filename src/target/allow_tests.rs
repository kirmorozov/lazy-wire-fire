use super::*;
use crate::model::{RawTarget, TargetDef};

/// Loads a target of `kind` with one endpoint, under the allow-list `allowed`.
fn load(kind: &str, endpoint: &str, vars: &str, allowed: &str) -> Result<Arc<Target>> {
    let def = TargetDef::from_row(&RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoint.into()),
        vars: Some(vars.into()),
        ..Default::default()
    })?;
    let list = HostList::parse(allowed)?;
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    Target::new(def, pool, list.as_ref())
}

/// Each kind's effective host (the one lazywf connects to) is the one the list is matched
/// against: the listed host loads, another host is refused and the error names it.
#[test]
fn each_kind_checks_its_effective_host() {
    let cases = [
        (
            "http",
            "https://api.example.test:8443/hook",
            "{}",
            "api.example.test",
        ),
        (
            "http.s3",
            "",
            r#"{"bucket":"orders","region":"eu-west-1"}"#,
            "orders.s3.eu-west-1.amazonaws.com",
        ),
        (
            "http.s3",
            "",
            r#"{"bucket":"orders","region":"eu-west-1","path_style":true}"#,
            "s3.eu-west-1.amazonaws.com",
        ),
        (
            "sqs",
            "aws-sqs://eu-west-1/123456789012/orders",
            "{}",
            "sqs.eu-west-1.amazonaws.com",
        ),
        (
            "sqs",
            "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561",
            "{}",
            "127.0.0.1",
        ),
        (
            "kinesis",
            "aws-kinesis://eu-west-1/orders",
            "{}",
            "kinesis.eu-west-1.amazonaws.com",
        ),
        (
            "lambda",
            "aws-lambda://eu-west-1/orders",
            "{}",
            "lambda.eu-west-1.amazonaws.com",
        ),
        (
            "eventbridge",
            "aws-eventbridge://eu-west-1/orders",
            "{}",
            "events.eu-west-1.amazonaws.com",
        ),
        (
            "eventbridge",
            "aws-eventbridge://eu-west-1/orders?endpoint=http://127.0.0.1:34569",
            "{}",
            "127.0.0.1",
        ),
        (
            "sns",
            "aws-sns://eu-west-1/123456789012/orders",
            "{}",
            "sns.eu-west-1.amazonaws.com",
        ),
        (
            "sns",
            "aws-sns://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34569",
            "{}",
            "127.0.0.1",
        ),
        (
            "pubsub",
            "gc-pubsub://lwf-test/orders?endpoint=http://127.0.0.1:34563",
            "{}",
            "127.0.0.1",
        ),
        (
            "pubsub",
            "gc-pubsub://lwf-test/orders",
            "{}",
            "pubsub.googleapis.com",
        ),
        (
            "redis",
            "redis://cache.example.test:6380/0",
            "{}",
            "cache.example.test",
        ),
        (
            "mysql",
            "mysql://db.example.test:3306/app",
            r#"{"username":"u","password":"p"}"#,
            "db.example.test",
        ),
        (
            "pgsql",
            "postgres://pg.example.test:5432/app",
            r#"{"username":"u","password":"p"}"#,
            "pg.example.test",
        ),
        (
            "amqp",
            "amqp://mq.example.test:5672/",
            r#"{"username":"u","password":"p"}"#,
            "mq.example.test",
        ),
        (
            "smtp",
            "smtp://mail.example.test:587",
            "{}",
            "mail.example.test",
        ),
        (
            "smtp",
            "smtps://mail.example.test:465",
            "{}",
            "mail.example.test",
        ),
        (
            "kafka",
            "kafka://mq.example.test:9094/orders",
            "{}",
            "mq.example.test",
        ),
    ];
    for (kind, endpoint, vars, host) in cases {
        let listed = load(kind, endpoint, vars, host);
        assert!(
            listed.is_ok(),
            "{kind} {endpoint}: {:?}",
            listed.err().map(|e| format!("{e:#}"))
        );
        let refused = load(kind, endpoint, vars, "unrelated.example.test").err();
        let msg = format!(
            "{:#}",
            refused.unwrap_or_else(|| panic!("{kind} {endpoint}: not refused"))
        );
        assert!(
            msg.contains(host) && msg.contains("LAZYWF_ALLOWED_HOSTS"),
            "{kind}: {msg}"
        );
    }
    // Unset: no check, whatever the host.
    assert!(load("http", "https://api.example.test/hook", "{}", "").is_ok());
}

/// `grpc` too: the host and port of the endpoint are the ones lazywf connects to, so they
/// are the ones the list is matched against. The descriptor set is the shipped contract,
/// read as a `file:` reference from a directory `LAZYWF_SECRET_DIRS` names.
#[test]
fn the_grpc_host_and_port_are_matched() {
    let _env = crate::secrets::ENV_LOCK.blocking_lock();
    let was = std::env::var("LAZYWF_SECRET_DIRS").ok();
    let dir = std::env::temp_dir().join(format!("lazywf-allow-grpc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let desc = dir.join("lazywf.desc");
    std::fs::write(&desc, crate::grpc::contract::DESCRIPTOR).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
    let vars = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item"}}"#,
        desc.display()
    );
    let ep = "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push";
    // Every load reads the descriptor set through the var, so they all happen while it is
    // set; what they said is checked once it is back as it was.
    let listed = load("grpc", ep, &vars, "ingest.example.test")
        .err()
        .map(|e| format!("{e:#}"));
    let other_port = load("grpc", ep, &vars, "ingest.example.test:9001")
        .err()
        .map(|e| format!("{e:#}"));
    let other = load("grpc", ep, &vars, "unrelated.example.test")
        .err()
        .map(|e| format!("{e:#}"));
    match was {
        Some(v) => std::env::set_var("LAZYWF_SECRET_DIRS", v),
        None => std::env::remove_var("LAZYWF_SECRET_DIRS"),
    }
    assert!(listed.is_none(), "the listed host was refused: {listed:?}");
    let msg = other_port.expect("the endpoint's port was not refused");
    assert!(
        msg.contains("ingest.example.test") && msg.contains("LAZYWF_ALLOWED_HOSTS"),
        "{msg}"
    );
    let msg = other.expect("another host was not refused");
    assert!(
        msg.contains("ingest.example.test") && msg.contains("LAZYWF_ALLOWED_HOSTS"),
        "{msg}"
    );
}

/// Every broker of a `kafka` endpoint is matched, not only the first: one off the list is
/// refused, and the error names it. (The multi-broker URL holds one endpoint, so the
/// endpoints column is the JSON-array form: a bare comma would split it.)
#[test]
fn every_kafka_broker_is_matched() {
    let both = r#"["kafka://mq.example.test:9094,mq2.example.test:9094/orders"]"#;
    let ok = load(
        "kafka",
        both,
        "{}",
        "mq.example.test:9094, mq2.example.test:9094",
    );
    assert!(ok.is_ok(), "{:?}", ok.err().map(|e| format!("{e:#}")));
    let refused = load("kafka", both, "{}", "mq.example.test:9094").err();
    let msg = format!(
        "{:#}",
        refused.unwrap_or_else(|| panic!("the second broker was not refused"))
    );
    assert!(
        msg.contains("mq2.example.test") && msg.contains("LAZYWF_ALLOWED_HOSTS"),
        "{msg}"
    );
}

#[test]
fn the_port_of_an_endpoint_is_matched_too() {
    assert!(load(
        "http",
        "https://api.example.test:8443/hook",
        "{}",
        "api.example.test:443"
    )
    .is_err());
    assert!(load(
        "http",
        "https://api.example.test:8443/hook",
        "{}",
        "api.example.test:8443"
    )
    .is_ok());
    assert!(load(
        "redis",
        "redis://cache.example.test:6380/0",
        "{}",
        "*.example.test:6379"
    )
    .is_err());
}

/// Wildcards and CIDR ranges are matched on the effective host of each kind as well.
#[test]
fn wildcards_and_ranges_match_the_derived_hosts() {
    assert!(load(
        "http.s3",
        "",
        r#"{"bucket":"orders","region":"eu-west-1"}"#,
        "*.s3.eu-west-1.amazonaws.com"
    )
    .is_ok());
    assert!(load(
        "http.s3",
        "",
        r#"{"bucket":"orders","region":"eu-west-1"}"#,
        "s3.eu-west-1.amazonaws.com"
    )
    .is_err());
    assert!(load(
        "pubsub",
        "gc-pubsub://lwf-test/orders",
        "{}",
        "*.googleapis.com"
    )
    .is_ok());
    assert!(load(
        "sqs",
        "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://10.4.0.7:9324",
        "{}",
        "10.4.0.0/16"
    )
    .is_ok());
}
