use super::*;

fn row(kind: &str, endpoints: Option<&str>, vars: &str) -> RawTarget {
    RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: endpoints.map(str::to_string),
        vars: Some(vars.into()),
        ..Default::default()
    }
}

const QUEUE: &str = "aws-sqs://eu-west-1/123456789012/orders";
const STREAM: &str = "aws-kinesis://eu-west-1/orders";
const CREDS: &str = r#""access_key_ref":"env:AK","secret_key_ref":"env:SK""#;

#[test]
fn sqs_accepts_the_url_forms_with_references() {
    let t = TargetDef::from_row(&row("sqs", Some(QUEUE), &format!("{{{CREDS}}}"))).unwrap();
    assert_eq!(t.kind, Kind::Sqs);
    assert_eq!(t.endpoints, vec![QUEUE.to_string()]);
    // Credentials never reach templates; the region is in the URL.
    assert_eq!(t.template_vars(), serde_json::json!({}));
    assert!(TargetDef::from_row(&row(
        "sqs",
        Some("aws-sqs://123456789012/orders.fifo"),
        "{}"
    ))
    .is_ok());
    assert!(TargetDef::from_row(&row(
        "sqs",
        Some("aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561"),
        "{}"
    ))
    .is_ok());
}

#[test]
fn sqs_refuses_bad_endpoints() {
    let ok = "{}";
    // exactly one endpoint
    assert!(TargetDef::from_row(&row("sqs", None, ok)).is_err());
    assert!(TargetDef::from_row(&row("sqs", Some(&format!("{QUEUE},{QUEUE}")), ok)).is_err());
    // the scheme: aws-sqs://, never a plain URL or another aws- scheme
    assert!(TargetDef::from_row(&row(
        "sqs",
        Some("https://sqs.eu-west-1.amazonaws.com/123456789012/orders"),
        ok
    ))
    .is_err());
    assert!(TargetDef::from_row(&row("sqs", Some(STREAM), ok)).is_err());
    // the region is not in vars any more
    assert!(TargetDef::from_row(&row("sqs", Some(QUEUE), r#"{"region":"eu-west-1"}"#)).is_err());
    // userinfo is never in the URL
    assert!(TargetDef::from_row(&row(
        "sqs",
        Some("aws-sqs://eu-west-1/u:p@123456789012/orders"),
        ok
    ))
    .is_err());
    // credentials are references, never literals, and come in pairs
    assert!(TargetDef::from_row(&row(
        "sqs",
        Some(QUEUE),
        r#"{"access_key_ref":"AKIA","secret_key_ref":"env:SK"}"#
    ))
    .is_err());
    assert!(
        TargetDef::from_row(&row("sqs", Some(QUEUE), r#"{"access_key_ref":"env:AK"}"#)).is_err()
    );
}

#[test]
fn kinesis_accepts_the_url_forms_with_references() {
    let t = TargetDef::from_row(&row("kinesis", Some(STREAM), &format!("{{{CREDS}}}"))).unwrap();
    assert_eq!(t.kind, Kind::Kinesis);
    assert_eq!(t.endpoints, vec![STREAM.to_string()]);
    assert_eq!(t.template_vars(), serde_json::json!({}));
    assert!(TargetDef::from_row(&row("kinesis", Some("aws-kinesis://orders"), "{}")).is_ok());
    assert!(TargetDef::from_row(&row(
        "kinesis",
        Some("aws-kinesis://orders?endpoint=http://127.0.0.1:34562"),
        "{}"
    ))
    .is_ok());
}

#[test]
fn kinesis_refuses_bad_endpoints() {
    let ok = "{}";
    assert!(TargetDef::from_row(&row("kinesis", None, ok)).is_err());
    assert!(TargetDef::from_row(&row("kinesis", Some(&format!("{STREAM},{STREAM}")), ok)).is_err());
    // the scheme, and no plain service URL
    assert!(TargetDef::from_row(&row(
        "kinesis",
        Some("https://kinesis.eu-west-1.amazonaws.com"),
        ok
    ))
    .is_err());
    assert!(TargetDef::from_row(&row("kinesis", Some(QUEUE), ok)).is_err());
    // the stream and region are not in vars any more
    for v in [
        r#"{"stream_name":"orders"}"#,
        r#"{"stream_arn":"arn:aws:kinesis:eu-west-1:123456789012:stream/orders"}"#,
        r#"{"region":"eu-west-1"}"#,
    ] {
        assert!(
            TargetDef::from_row(&row("kinesis", Some(STREAM), v)).is_err(),
            "{v}"
        );
    }
    // userinfo and literal credentials are refused
    assert!(TargetDef::from_row(&row("kinesis", Some("aws-kinesis://u:p@orders"), ok)).is_err());
    assert!(TargetDef::from_row(&row(
        "kinesis",
        Some(STREAM),
        r#"{"access_key_ref":"AKIA","secret_key_ref":"env:SK"}"#
    ))
    .is_err());
}

#[test]
fn the_scheme_must_match_the_kind() {
    // an aws- scheme on a kind that does not take it
    assert!(TargetDef::from_row(&row("http", Some(QUEUE), "{}")).is_err());
    assert!(TargetDef::from_row(&row(
        "http.s3",
        Some(STREAM),
        r#"{"bucket":"b","region":"r"}"#
    ))
    .is_err());
    // a plain URL for http is still fine
    assert!(TargetDef::from_row(&row("http", Some("https://example.com"), "{}")).is_ok());
}

const BUS: &str = "aws-eventbridge://eu-west-1/orders";

#[test]
fn eventbridge_accepts_the_url_forms_with_references() {
    let t = TargetDef::from_row(&row("eventbridge", Some(BUS), &format!("{{{CREDS}}}"))).unwrap();
    assert_eq!(t.kind, Kind::EventBridge);
    assert_eq!(t.endpoints, vec![BUS.to_string()]);
    assert_eq!(t.template_vars(), serde_json::json!({}));
    let ok = |url: &str| TargetDef::from_row(&row("eventbridge", Some(url), "{}")).is_ok();
    assert!(ok("aws-eventbridge://orders"));
    assert!(ok(
        "aws-eventbridge://orders?endpoint=http://127.0.0.1:34569"
    ));
    assert!(ok(
        "aws-eventbridge://arn:aws:events:eu-west-1:123456789012:event-bus/orders"
    ));
}

#[test]
fn eventbridge_refuses_bad_endpoints() {
    let ok = "{}";
    assert!(TargetDef::from_row(&row("eventbridge", None, ok)).is_err());
    assert!(TargetDef::from_row(&row("eventbridge", Some(&format!("{BUS},{BUS}")), ok)).is_err());
    // the scheme, and no plain service URL, no other aws- scheme
    assert!(TargetDef::from_row(&row(
        "eventbridge",
        Some("https://events.eu-west-1.amazonaws.com"),
        ok
    ))
    .is_err());
    assert!(TargetDef::from_row(&row("eventbridge", Some(QUEUE), ok)).is_err());
    // the bus and the region are not in vars
    for v in [
        r#"{"region":"eu-west-1"}"#,
        r#"{"event_bus_name":"orders"}"#,
        r#"{"event_bus_arn":"arn:aws:events:eu-west-1:123456789012:event-bus/orders"}"#,
    ] {
        assert!(
            TargetDef::from_row(&row("eventbridge", Some(BUS), v)).is_err(),
            "{v}"
        );
    }
    // userinfo and literal credentials are refused
    assert!(TargetDef::from_row(&row(
        "eventbridge",
        Some("aws-eventbridge://u:p@orders"),
        ok
    ))
    .is_err());
    assert!(TargetDef::from_row(&row(
        "eventbridge",
        Some(BUS),
        r#"{"access_key_ref":"AKIA","secret_key_ref":"env:SK"}"#
    ))
    .is_err());
    // the other kinds refuse an aws-eventbridge:// endpoint
    assert!(TargetDef::from_row(&row("http", Some("aws-eventbridge://orders"), "{}")).is_err());
}

const TOPIC: &str = "aws-sns://eu-west-1/123456789012/orders";

#[test]
fn sns_accepts_the_url_forms_with_references() {
    let t = TargetDef::from_row(&row("sns", Some(TOPIC), &format!("{{{CREDS}}}"))).unwrap();
    assert_eq!(t.kind, Kind::Sns);
    assert_eq!(t.endpoints, vec![TOPIC.to_string()]);
    assert_eq!(t.template_vars(), serde_json::json!({}));
    let ok = |url: &str| TargetDef::from_row(&row("sns", Some(url), "{}")).is_ok();
    assert!(ok("aws-sns://123456789012/orders"));
    assert!(ok(
        "aws-sns://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34569"
    ));
    // a FIFO topic by its name, and the name's `_` and `-`
    assert!(ok("aws-sns://eu-west-1/123456789012/orders.fifo"));
    assert!(ok("aws-sns://eu-west-1/123456789012/orders_events-1"));
    // the topic's MaximumMessageSize, stated for the batch's byte limit: an ordinary var, so
    // a template can read it too
    let t = TargetDef::from_row(&row(
        "sns",
        Some(TOPIC),
        r#"{"maximum_message_size":1048576}"#,
    ))
    .unwrap();
    assert_eq!(
        t.template_vars(),
        serde_json::json!({"maximum_message_size": 1048576})
    );
}

#[test]
fn sns_refuses_bad_endpoints_and_vars() {
    let ok = "{}";
    assert!(TargetDef::from_row(&row("sns", None, ok)).is_err());
    assert!(TargetDef::from_row(&row("sns", Some(&format!("{TOPIC},{TOPIC}")), ok)).is_err());
    // the scheme, and no plain service URL, no other aws- scheme
    assert!(
        TargetDef::from_row(&row("sns", Some("https://sns.eu-west-1.amazonaws.com"), ok)).is_err()
    );
    assert!(TargetDef::from_row(&row("sns", Some(QUEUE), ok)).is_err());
    // the account, the topic and the region are in the endpoint, not in vars
    for v in [
        r#"{"region":"eu-west-1"}"#,
        r#"{"account_id":"123456789012"}"#,
        r#"{"topic_name":"orders"}"#,
        r#"{"topic_arn":"arn:aws:sns:eu-west-1:123456789012:orders"}"#,
    ] {
        assert!(
            TargetDef::from_row(&row("sns", Some(TOPIC), v)).is_err(),
            "{v}"
        );
    }
    // the endpoint: the account is twelve digits, the topic name is a topic name
    for url in [
        "aws-sns://eu-west-1/12345678901/orders",
        "aws-sns://eu-west-1/123456789012x/orders",
        "aws-sns://eu-west-1/123456789012/",
        "aws-sns://eu-west-1/123456789012/order.s",
        "aws-sns://eu-west-1/123456789012/orders.fif",
        "aws-sns://eu-west-1/123456789012/ord ers",
        "aws-sns://eu-west-1/123456789012",
    ] {
        assert!(
            TargetDef::from_row(&row("sns", Some(url), ok)).is_err(),
            "{url}"
        );
    }
    // MaximumMessageSize: what a topic can hold, no more
    for v in [
        r#"{"maximum_message_size":0}"#,
        r#"{"maximum_message_size":1048577}"#,
        r#"{"maximum_message_size":"262144"}"#,
        r#"{"maximum_message_size":null}"#,
    ] {
        assert!(
            TargetDef::from_row(&row("sns", Some(TOPIC), v)).is_err(),
            "{v}"
        );
    }
    // userinfo and literal credentials are refused
    assert!(
        TargetDef::from_row(&row("sns", Some("aws-sns://u:p@123456789012/orders"), ok)).is_err()
    );
    assert!(TargetDef::from_row(&row(
        "sns",
        Some(TOPIC),
        r#"{"access_key_ref":"AKIA","secret_key_ref":"env:SK"}"#
    ))
    .is_err());
    // the other kinds refuse an aws-sns:// endpoint
    assert!(TargetDef::from_row(&row("http", Some(TOPIC), "{}")).is_err());
}

#[test]
fn kind_properties() {
    assert_eq!(Kind::parse("kinesis").unwrap(), Kind::Kinesis);
    assert!(Kind::Kinesis.reports_items());
    assert!(Kind::Kinesis.append_only());
    assert_eq!(Kind::Kinesis.batch_bytes_limit(), Some(5_242_880));
    assert!(Kind::Kinesis.is_http());
    assert_eq!(Kind::parse("eventbridge").unwrap(), Kind::EventBridge);
    assert_eq!(Kind::EventBridge.name(), "eventbridge");
    assert!(Kind::EventBridge.reports_items());
    assert!(Kind::EventBridge.append_only());
    assert_eq!(Kind::EventBridge.batch_bytes_limit(), Some(1_048_576));
    assert!(Kind::EventBridge.is_http());
    assert_eq!(Kind::parse("sns").unwrap(), Kind::Sns);
    assert_eq!(Kind::Sns.name(), "sns");
    assert!(Kind::Sns.reports_items());
    assert!(Kind::Sns.append_only());
    assert_eq!(Kind::Sns.batch_bytes_limit(), Some(262_144));
    assert!(Kind::Sns.is_http());
}

#[test]
fn lambda_properties_and_endpoints() {
    assert_eq!(Kind::parse("lambda").unwrap(), Kind::Lambda);
    assert_eq!(Kind::Lambda.name(), "lambda");
    assert!(Kind::Lambda.reports_items());
    // Not append-only: the stray refusal is a rule check (`rule::kind_errors`), not this.
    assert!(!Kind::Lambda.append_only());
    assert_eq!(Kind::Lambda.batch_bytes_limit(), None);
    let ok = |url: &str, vars: &str| TargetDef::from_row(&row("lambda", Some(url), vars)).is_ok();
    assert!(ok("aws-lambda://orders", r#"{}"#));
    assert!(ok(
        "aws-lambda://eu-west-1/orders:live?invocation=event",
        r#"{}"#
    ));
    assert!(ok(
        "aws-lambda://orders",
        r#"{"access_key_ref":"env:AK","secret_key_ref":"env:SK"}"#
    ));
    // one endpoint, the aws-lambda:// form, no region var, references only
    assert!(!ok("https://example.com/orders", "{}"));
    assert!(!ok("aws-sqs://123456789012/orders", "{}"));
    assert!(!ok("aws-lambda://orders", r#"{"region":"eu-west-1"}"#));
    assert!(!ok(
        "aws-lambda://orders",
        r#"{"access_key_ref":"AKIAEXAMPLE","secret_key_ref":"env:SK"}"#
    ));
    assert!(!ok("aws-lambda://orders?invocation=batch", "{}"));
    // the other kinds refuse an aws-lambda:// endpoint
    assert!(TargetDef::from_row(&row("http", Some("aws-lambda://orders"), "{}")).is_err());
}
