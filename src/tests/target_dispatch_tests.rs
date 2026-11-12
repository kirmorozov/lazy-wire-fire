use super::*;
use crate::model::{RawTarget, TargetDef};

/// A target of `kind` with one endpoint, without a database and without a server:
/// what a transport decides before it opens a connection is what is checked here.
fn target(kind: &str, endpoint: &str, vars: &str) -> Arc<Target> {
    let def = TargetDef::from_row(&RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoint.into()),
        vars: Some(vars.into()),
        ..Default::default()
    })
    .unwrap();
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    Target::new(def, pool, None).unwrap()
}

fn http() -> Arc<Target> {
    target("http", "https://api.example.test:8443/hook", "{}")
}
fn sqs() -> Arc<Target> {
    target(
        "sqs",
        "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561",
        "{}",
    )
}
fn lambda(event: bool) -> Arc<Target> {
    let url = if event {
        "aws-lambda://eu-west-1/orders?invocation=event"
    } else {
        "aws-lambda://eu-west-1/orders"
    };
    target("lambda", url, "{}")
}
fn kafka() -> Arc<Target> {
    target("kafka", "kafka://mq.example.test:9094/orders", "{}")
}
fn redis() -> Arc<Target> {
    target("redis", "redis://cache.example.test:6380/0", "{}")
}
fn smtp() -> Arc<Target> {
    target("smtp", "smtp://mail.example.test:587", "{}")
}
fn amqp() -> Arc<Target> {
    target(
        "amqp",
        "amqp://mq.example.test:5672/",
        r#"{"username":"u","password":"p"}"#,
    )
}
fn pubsub() -> Arc<Target> {
    target(
        "pubsub",
        "gc-pubsub://lwf-test/orders?endpoint=http://127.0.0.1:34563",
        "{}",
    )
}
fn mysql() -> Arc<Target> {
    target(
        "mysql",
        "mysql://db.example.test:3306/app",
        r#"{"username":"u","password":"p"}"#,
    )
}

/// A template that renders nothing is neither a delivery nor a failure: the keys
/// stay where they are. Every transport has to answer the same way, or a rule whose
/// `{{#if}}` excludes a row would strike its keys to death.
#[tokio::test]
async fn nothing_rendered_sends_nothing() {
    for blank in ["", "   ", "\n\t "] {
        for t in [
            http(),
            sqs(),
            lambda(false),
            kafka(),
            redis(),
            smtp(),
            amqp(),
            pubsub(),
            mysql(),
        ] {
            let kind = t.def.kind.name();
            let sent = t
                .send(0, blank)
                .await
                .unwrap_or_else(|e| panic!("{kind} on {blank:?}: {e:#}"));
            assert_eq!(sent, Sent::Skipped, "{kind} on {blank:?}");
        }
    }
}

/// A rendering the kind cannot take is refused (it would be refused again) rather
/// than retried: a key whose template is broken must not hold the lane forever.
#[tokio::test]
async fn a_rendering_the_kind_cannot_take_is_refused() {
    let cases = [
        (http(), "not a request line at all\n\nx"),
        (kafka(), "POST / HTTP/1.1\n\nnot json"),
        (kafka(), "GET / HTTP/1.1\n\n{\"records\":[]}"),
        (redis(), "GET 'unbalanced"),
        (smtp(), "not a message"),
        (amqp(), "PUBLISH no-leading-slash\nMessage-Id: k\n\n{}"),
        (pubsub(), "POST /wrong/path HTTP/1.1\n\n{}"),
        (mysql(), "SELECT 1; SELECT 2"),
    ];
    for (t, rendered) in cases {
        let kind = t.def.kind.name();
        let e = t
            .send(0, rendered)
            .await
            .err()
            .unwrap_or_else(|| panic!("{kind} accepted {rendered:?}"));
        assert!(
            is_rejected(&e),
            "{kind} on {rendered:?}: {e:#} is not a refusal"
        );
    }
}

/// A `pubsub`, `amqp`, `smtp` or `kafka` target only appends: it cannot answer "which
/// keys do you hold", so a stray listing against it is refused at once instead of
/// passing for an empty target and clearing keys that were never delivered.
#[tokio::test]
async fn an_append_only_kind_refuses_to_list() {
    let body = Uses {
        body: true,
        ..Uses::default()
    };
    for (t, why) in [
        (amqp(), "cannot list keys"),
        (smtp(), "cannot list keys"),
        (kafka(), "cannot list keys"),
        (pubsub(), "only publish"),
    ] {
        let kind = t.def.kind.name();
        let e = format!("{:#}", t.fetch(0, "GET /", body).await.err().unwrap());
        assert!(e.contains(why), "{kind}: {e}");
    }
    // A SQL or redis listing that renders nothing is refused too: an empty
    // listing would be read as "the target holds nothing".
    let e = format!("{:#}", redis().fetch(0, "  ", body).await.err().unwrap());
    assert!(e.contains("rendered nothing"), "{e}");
    let e = format!("{:#}", mysql().fetch(0, "  ", body).await.err().unwrap());
    assert!(e.contains("rendered nothing"), "{e}");
}

/// An endpoint index that does not exist is an error of its own: firing at
/// "endpoint 9" of a one-endpoint target must not be read as "nothing to send",
/// which is what a confirmed flush looks like.
#[tokio::test]
async fn an_endpoint_that_is_gone_is_not_a_skip() {
    let keys = vec![String::from("1")];
    for t in [
        http(),
        sqs(),
        lambda(false),
        kafka(),
        redis(),
        pubsub(),
        mysql(),
    ] {
        let kind = t.def.kind.name();
        let e = format!(
            "{:#}",
            t.send(9, "POST / HTTP/1.1\n\n{}").await.err().unwrap()
        );
        assert!(
            e.contains("endpoint 9") && e.contains("is gone"),
            "{kind}: {e}"
        );
        let e = format!(
            "{:#}",
            t.send_items(9, "POST / HTTP/1.1\n\n{}", &keys)
                .await
                .err()
                .unwrap()
        );
        assert!(
            e.contains("endpoint 9") && e.contains("is gone"),
            "{kind}: {e}"
        );
        let e = format!(
            "{:#}",
            t.fetch(9, "GET /", Uses::default()).await.err().unwrap()
        );
        assert!(
            e.contains("endpoint 9") && e.contains("is gone"),
            "{kind}: {e}"
        );
    }
}

/// The largest action this target takes: the kind's own limit, the broker's message
/// limit raised by `vars.message_max_bytes`, or the Lambda payload limit of the
/// invocation. Cutting at the wrong size means the service refuses the whole batch,
/// every key of it at once.
#[test]
fn the_size_a_batch_is_cut_to() {
    assert_eq!(http().batch_bytes_limit(), None);
    assert_eq!(sqs().batch_bytes_limit(), Some(262_144));
    assert_eq!(
        kafka().batch_bytes_limit(),
        Some(crate::kafka::MESSAGE_MAX_BYTES)
    );
    assert_eq!(
        target(
            "kafka",
            "kafka://mq.example.test:9094/orders",
            r#"{"message_max_bytes": 8388608}"#
        )
        .batch_bytes_limit(),
        Some(8_388_608),
        "a broker with a raised message.max.bytes takes bigger batches"
    );
    // A Lambda: an event invocation takes a sixth of what a synchronous one does.
    let sync = lambda(false);
    let event = lambda(true);
    assert_eq!(
        sync.batch_bytes_limit(),
        Some(crate::aws::LAMBDA_SYNC_PAYLOAD)
    );
    assert_eq!(
        event.batch_bytes_limit(),
        Some(crate::aws::LAMBDA_EVENT_PAYLOAD)
    );
    assert!(sync.batch_bytes_limit() > event.batch_bytes_limit());
}

/// A throttled AWS answer comes as HTTP 400 with a code that says "not now". Read
/// as a refusal, every key of the batch would take a strike and be dropped while
/// the account is merely over its rate.
#[test]
fn an_aws_throttle_is_not_a_refusal() {
    let with_code = |code: &str| {
        (
            vec![("x-amzn-errortype".to_string(), code.to_string())],
            br#"{"message":"Rate exceeded"}"#.to_vec(),
        )
    };
    let (headers, body) = with_code("ThrottlingException");
    assert!(sqs().aws_throttled(&headers, &body));
    // The code may come in the body instead, and with the service's prefix.
    assert!(sqs().aws_throttled(
        &[],
        br#"{"__type":"com.amazon.coral.service#ServiceUnavailable"}"#
    ));
    // The same answer from a kind that is not AWS has no code to trust.
    let (headers, body) = with_code("ThrottlingException");
    assert!(
        !http().aws_throttled(&headers, &body),
        "an http target's 400 is about this request"
    );
    // A code that does not say "not now" stays a refusal of this request.
    let (headers, body) = with_code("InvalidMessageDigest");
    assert!(!sqs().aws_throttled(&headers, &body));
    // No code at all: not a throttle.
    assert!(!sqs().aws_throttled(&[], b"not json"));
}

/// A Lambda payload over the limit of its invocation is refused before it goes out:
/// the service answers the same way every time, so retrying would only spend the
/// key's strikes.
#[tokio::test]
async fn a_payload_an_invoke_cannot_take_is_refused_before_it_goes_out() {
    let event = lambda(true);
    let over = "x".repeat(crate::aws::LAMBDA_EVENT_PAYLOAD + 1);
    let e = event
        .send(0, &over)
        .await
        .err()
        .unwrap_or_else(|| panic!("an oversized payload was accepted"));
    assert!(is_rejected(&e), "{e:#} is not a refusal");
    let msg = format!("{e:#}");
    assert!(
        msg.contains("over the") && msg.contains("bytes an Invoke takes"),
        "{msg}"
    );
    // A batch of items is refused the same way: the payload is the whole request.
    let e = event
        .send_items(0, &over, &[String::from("1"), String::from("2")])
        .await
        .err()
        .unwrap();
    assert!(is_rejected(&e), "{e:#} is not a refusal");
}
