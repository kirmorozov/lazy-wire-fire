use super::*;
use crate::targets::eventbridge::{EventBridgeUrl, EVENTBRIDGE_MAX_BYTES, EVENTBRIDGE_MAX_ENTRIES};
use crate::targets::kinesis::KinesisUrl;
use crate::targets::sqs::SqsUrl;

fn sqs(url: &str) -> Result<SqsUrl> {
    SqsUrl::parse(url)
}

fn kin(url: &str) -> Result<KinesisUrl> {
    KinesisUrl::parse(url)
}

#[test]
fn sqs_url_forms() {
    let u = sqs("aws-sqs://123456789012/orders").unwrap();
    assert_eq!(u.region, None);
    assert_eq!(
        (u.account.as_str(), u.queue.as_str()),
        ("123456789012", "orders")
    );
    assert_eq!(u.endpoint, None);
    assert!(!u.fifo());
    let u = sqs("aws-sqs://eu-west-1/123456789012/orders.fifo").unwrap();
    assert_eq!(u.region.as_deref(), Some("eu-west-1"));
    assert!(u.fifo());
    // the emulator's override, raw and percent-encoded
    let u = sqs("aws-sqs://us-east-1/000000000000/q?endpoint=http://127.0.0.1:34561").unwrap();
    assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34561"));
    let u =
        sqs("aws-sqs://us-east-1/000000000000/q?endpoint=http%3A%2F%2F127.0.0.1%3A34561").unwrap();
    assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34561"));
    // a VPC endpoint is https, with a trailing slash allowed
    assert!(sqs("aws-sqs://eu-west-1/123456789012/q?endpoint=https://vpce.example:443/").is_ok());
}

#[test]
fn sqs_refuses_bad_urls() {
    for url in [
        // the scheme: aws-sqs:// only
        "https://sqs.eu-west-1.amazonaws.com/123456789012/orders",
        "aws-kinesis://eu-west-1/orders",
        "sqs://123456789012/orders",
        // the path: two or three segments, no empty ones
        "aws-sqs://orders",
        "aws-sqs://eu-west-1/123456789012/a/b",
        "aws-sqs://123456789012/orders/",
        "aws-sqs://",
        // the account and the region
        "aws-sqs://12345678901/orders",
        "aws-sqs://eu-west-1/12345678901a/orders",
        "aws-sqs://EU-WEST-1/123456789012/orders",
        "aws-sqs://arn:aws:sqs:eu-west-1/123456789012/orders",
        // the queue name
        "aws-sqs://123456789012/.fifo",
        "aws-sqs://123456789012/a.b",
        "aws-sqs://123456789012/x.fifo.fifo",
        // userinfo, in the path or in the override
        "aws-sqs://u:p@123456789012/orders",
        "aws-sqs://123456789012/orders?endpoint=http://u:p@127.0.0.1:34561",
        // the query: `endpoint` only, once, http(s), no path
        "aws-sqs://123456789012/orders?region=eu-west-1",
        "aws-sqs://123456789012/orders?endpoint=ftp://127.0.0.1:21",
        "aws-sqs://123456789012/orders?endpoint=http://127.0.0.1:34561/path",
        "aws-sqs://123456789012/orders?endpoint=http://a:1&endpoint=http://b:2",
    ] {
        assert!(sqs(url).is_err(), "{url} should be refused");
    }
}

#[test]
fn kinesis_url_forms() {
    let u = kin("aws-kinesis://orders").unwrap();
    assert_eq!((u.region, u.stream.as_str()), (None, "orders"));
    let u = kin("aws-kinesis://eu-west-1/orders.v2_a-b").unwrap();
    assert_eq!(u.region.as_deref(), Some("eu-west-1"));
    assert_eq!(u.stream, "orders.v2_a-b");
    let u = kin("aws-kinesis://orders?endpoint=http://127.0.0.1:34562").unwrap();
    assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34562"));
}

#[test]
fn kinesis_refuses_bad_urls() {
    for url in [
        "https://kinesis.eu-west-1.amazonaws.com",
        "aws-sqs://123456789012/orders",
        "aws-kinesis://",
        "aws-kinesis://eu-west-1/a/b",
        "aws-kinesis://orders/",
        "aws-kinesis://eu-west-1/bad name",
        "aws-kinesis://u:p@orders",
        "aws-kinesis://orders?endpoint=https://k.example/x",
        "aws-kinesis://orders?stream=x",
        // an ARN does not fit the URL form (its slash and colons split the path)
        "aws-kinesis://arn:aws:kinesis:eu-west-1:123456789012:stream/orders",
    ] {
        assert!(kin(url).is_err(), "{url} should be refused");
    }
}

#[test]
fn scheme_errors_name_the_scheme() {
    let e = sqs("https://sqs.eu-west-1.amazonaws.com/123456789012/q").unwrap_err();
    assert!(
        format!("{e:#}").contains("not a plain http(s) URL"),
        "{e:#}"
    );
    let e = kin("aws-sqs://123456789012/q").unwrap_err();
    assert!(format!("{e:#}").contains("not `aws-sqs://`"), "{e:#}");
    let e = eb("https://example.com/bus").unwrap_err();
    assert!(format!("{e:#}").contains("aws-eventbridge://"), "{e:#}");
}

fn eb(url: &str) -> Result<EventBridgeUrl> {
    EventBridgeUrl::parse(url)
}

#[test]
fn eventbridge_url_forms() {
    let u = eb("aws-eventbridge://orders").unwrap();
    assert_eq!((u.region, u.bus.as_str()), (None, "orders"));
    assert_eq!(u.endpoint, None);
    let u = eb("aws-eventbridge://eu-west-1/orders.v2_a-b").unwrap();
    assert_eq!(u.region.as_deref(), Some("eu-west-1"));
    assert_eq!(u.bus, "orders.v2_a-b");
    let u = eb("aws-eventbridge://orders?endpoint=http://127.0.0.1:34569").unwrap();
    assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34569"));
    // the ARN: its region is the region, the bus is the resource name
    let u = eb("aws-eventbridge://arn:aws:events:eu-west-1:123456789012:event-bus/orders").unwrap();
    assert_eq!(u.region.as_deref(), Some("eu-west-1"));
    assert_eq!(u.bus, "orders");
    let u = eb("aws-eventbridge://arn:aws-cn:events:cn-north-1:123456789012:event-bus/default")
        .unwrap();
    assert_eq!(u.region.as_deref(), Some("cn-north-1"));
    assert_eq!(u.bus, "default");
}

#[test]
fn eventbridge_refuses_bad_urls() {
    for url in [
        "https://events.eu-west-1.amazonaws.com",
        "aws-sqs://123456789012/orders",
        "eventbridge://orders",
        "aws-eventbridge://",
        "aws-eventbridge://orders/",
        "aws-eventbridge://eu-west-1/bad name",
        "aws-eventbridge://eu-west-1/a/b",
        "aws-eventbridge://u:p@orders",
        "aws-eventbridge://orders?endpoint=ftp://127.0.0.1:21",
        "aws-eventbridge://orders?region=eu-west-1",
        "aws-eventbridge://eu-west-1/orders?endpoint=http://127.0.0.1:34569/x",
        // not region/bus (the first segment is no region, the second no ARN)
        "aws-eventbridge://Orders/orders",
        // the ARN: seven parts, events, event-bus, twelve-digit account, valid region and name
        "aws-eventbridge://arn:aws:events:eu-west-1:123456789012:event-bus",
        "aws-eventbridge://arn:aws:events:eu-west-1:123456789012:rule/orders",
        "aws-eventbridge://arn:aws:sqs:eu-west-1:123456789012:event-bus/orders",
        "aws-eventbridge://arn:aws:events:eu-west-1:12345678901:event-bus/orders",
        "aws-eventbridge://arn:aws:events:eu-west-1:123456789012:event-bus/bad*name",
        // the ARN carries its region, so a region segment before it is a mistake
        "aws-eventbridge://eu-west-1/arn:aws:events:eu-west-1:123456789012:event-bus/orders",
    ] {
        assert!(eb(url).is_err(), "{url} should be refused");
    }
    // 256 characters are the longest name
    assert!(eb(&format!("aws-eventbridge://{}", "b".repeat(256))).is_ok());
    assert!(eb(&format!("aws-eventbridge://{}", "b".repeat(257))).is_err());
}

#[test]
fn eventbridge_limits_are_the_documented_ones() {
    assert_eq!(EVENTBRIDGE_MAX_ENTRIES, 10);
    assert_eq!(EVENTBRIDGE_MAX_BYTES, 1_048_576);
}

#[test]
fn region_comes_from_the_url_then_the_environment_then_the_profile() {
    let env = |vars: &'static [(&'static str, &'static str)]| {
        move |n: &str| {
            vars.iter()
                .find(|(k, _)| *k == n)
                .map(|(_, v)| v.to_string())
        }
    };
    let config = || Some("ap-south-1".to_string());
    // the URL wins
    assert_eq!(
        region_from(
            Some("eu-west-1"),
            env(&[("AWS_REGION", "us-east-1")]),
            config
        )
        .as_deref(),
        Some("eu-west-1")
    );
    // AWS_REGION, then AWS_DEFAULT_REGION
    assert_eq!(
        region_from(
            None,
            env(&[("AWS_REGION", "us-east-1"), ("AWS_DEFAULT_REGION", "x-y-1")]),
            config
        )
        .as_deref(),
        Some("us-east-1")
    );
    assert_eq!(
        region_from(None, env(&[("AWS_DEFAULT_REGION", "us-west-2")]), config).as_deref(),
        Some("us-west-2")
    );
    // the shared config profile, only when the environment has none
    assert_eq!(
        region_from(None, env(&[]), config).as_deref(),
        Some("ap-south-1")
    );
    // none of them: no region
    assert_eq!(region_from(None, env(&[]), || None), None);
}

#[test]
fn config_region_reads_the_named_profile() {
    let text = "# shared config\n[default]\nregion = us-east-2\n\n[profile work]\noutput = json\nregion=eu-central-1\n";
    assert_eq!(config_region(text, "default").as_deref(), Some("us-east-2"));
    assert_eq!(config_region(text, "work").as_deref(), Some("eu-central-1"));
    assert_eq!(config_region(text, "missing"), None);
    // a `region` under another profile is not this one's
    assert_eq!(
        config_region(
            "[profile a]\nregion = x-y-1\n[default]\noutput=json\n",
            "default"
        ),
        None
    );
}
