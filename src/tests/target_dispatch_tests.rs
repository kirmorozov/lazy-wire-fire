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
/// `vars` with a credential: the secret fields hold a reference, never the secret.
fn cred_vars() -> &'static str {
    std::env::set_var("LWF_TEST_TARGET_PW", "p");
    r#"{"username":"u","password":"env:LWF_TEST_TARGET_PW"}"#
}

fn amqp() -> Arc<Target> {
    target("amqp", "amqp://mq.example.test:5672/", cred_vars())
}
fn pubsub() -> Arc<Target> {
    target(
        "pubsub",
        "gc-pubsub://lwf-test/orders?endpoint=http://127.0.0.1:34563",
        "{}",
    )
}
fn mysql() -> Arc<Target> {
    target("mysql", "mysql://db.example.test:3306/app", cred_vars())
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
        Some(crate::targets::kafka::MESSAGE_MAX_BYTES)
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
        Some(crate::targets::lambda::LAMBDA_SYNC_PAYLOAD)
    );
    assert_eq!(
        event.batch_bytes_limit(),
        Some(crate::targets::lambda::LAMBDA_EVENT_PAYLOAD)
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
    let over = "x".repeat(crate::targets::lambda::LAMBDA_EVENT_PAYLOAD + 1);
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

/// The batch front of a kind with a module of its own asks that module — but only of a
/// transport that is what the kind speaks. The guard is there for a target that ever is not
/// (a `Target` is built once, from its row, so today it cannot happen): its answer must be a
/// clear failure of the target, never a refusal of the keys, which the engine would drop.
#[tokio::test]
async fn the_batch_front_of_a_transport_the_kind_does_not_speak_names_the_kind() {
    // The whole test moves `LAZYWF_SECRET_DIRS`, which the grpc `Target::new` reads: it is
    // process-wide, so `ENV_LOCK` is held as long as it is set, as in every other test
    // that sets it — without it the var moves under a concurrent grpc load and its
    // descriptor set reads as outside the secret directories.
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let keys = vec![String::from("1"), String::from("2")];
    let was = std::env::var("LAZYWF_SECRET_DIRS").ok();
    let dir = std::env::temp_dir().join(format!("lazywf-dispatch-grpc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let desc = dir.join("lazywf.desc");
    std::fs::write(&desc, crate::targets::grpc::contract::DESCRIPTOR).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
    let grpc_vars = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item"}}"#,
        desc.display()
    );
    let cases = [
        (
            "kafka",
            "kafka://mq.example.test:9094/orders",
            "{}",
            "is not a kafka target",
        ),
        (
            "rabbitmq-stream",
            "rabbitmq-stream://127.0.0.1:34612/orders",
            cred_vars(),
            "is not a rabbitmq-stream target",
        ),
        (
            "servicebus",
            "sb://127.0.0.1:34610/orders",
            "{}",
            "is not a servicebus target",
        ),
        (
            "grpc",
            "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push",
            grpc_vars.as_str(),
            "is not a grpc target",
        ),
    ];
    for (kind, endpoint, vars, want) in cases {
        let mut t = target(kind, endpoint, vars);
        // The transport of a target the engine loads always matches its kind, so the only way
        // to the guard is to put another kind's transport under it. A redis client asks
        // nothing of the network when it is built, which is what makes it usable here.
        let redis = crate::targets::redis::Client::new(
            crate::targets::redis::RedisAddr::parse("redis://cache.example.test:6379/0").unwrap(),
            &crate::tls::TlsSpec::parse(None).unwrap(),
            None,
            None,
        )
        .unwrap();
        Arc::get_mut(&mut t)
            .expect("the test holds the only reference")
            .endpoints[0]
            .transport = Transport::Redis(redis);
        let e = t
            .send_items(0, "POST / HTTP/1.1\n\n{}", &keys)
            .await
            .unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains(want), "{kind}: {msg}");
        assert!(
            !is_rejected(&e),
            "{kind}: a transport that cannot answer per item says nothing about the keys, \
             which must wait and not be dropped: {msg}"
        );
    }
    match was {
        Some(v) => std::env::set_var("LAZYWF_SECRET_DIRS", v),
        None => std::env::remove_var("LAZYWF_SECRET_DIRS"),
    }
}

/// The three kinds that raise their own batch limit by `vars` — a gRPC server's maximum
/// message size, a Service Bus entity's, and a topic's `MaximumMessageSize` — say so through
/// the one front the engine cuts a batch at. A limit read as absent means a batch the service
/// refuses whole, so every key of it fails at once.
#[tokio::test]
async fn the_size_a_batch_is_cut_to_follows_the_service_that_set_it() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let was = std::env::var("LAZYWF_SECRET_DIRS").ok();
    let dir = scratch("limits");
    let desc = dir.join("lazywf.desc");
    std::fs::write(&desc, crate::targets::grpc::contract::DESCRIPTOR).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
    // The default of each kind: no `vars` raising it, no limit of its own to report.
    let grpc_default = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item"}}"#,
        desc.display()
    );
    let grpc = target(
        "grpc",
        "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push",
        &grpc_default,
    );
    assert_eq!(
        grpc.batch_bytes_limit(),
        Some(crate::targets::grpc::MAX_MESSAGE_BYTES),
        "the kind's own maximum"
    );
    let grpc_big = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item","max_message_bytes":8388608}}"#,
        desc.display()
    );
    assert_eq!(
        target(
            "grpc",
            "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push",
            &grpc_big
        )
        .batch_bytes_limit(),
        Some(8_388_608),
        "a server with a raised maximum takes bigger batches"
    );
    assert_eq!(
        target("servicebus", "sb://127.0.0.1:34610/orders", "{}").batch_bytes_limit(),
        Some(crate::targets::servicebus::MAX_MESSAGE_BYTES),
        "the kind's own maximum"
    );
    assert_eq!(
        target(
            "servicebus",
            "sb://127.0.0.1:34610/orders",
            r#"{"max_message_size": 1048576}"#
        )
        .batch_bytes_limit(),
        Some(1_048_576),
        "an entity with a raised maximum message size takes bigger batches"
    );
    assert_eq!(
        target(
            "sns",
            "aws-sns://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561",
            r#"{"maximum_message_size": 1048576}"#
        )
        .batch_bytes_limit(),
        Some(1_048_576),
        "a topic whose MaximumMessageSize was raised takes a batch of that size"
    );
    match was {
        Some(v) => std::env::set_var("LAZYWF_SECRET_DIRS", v),
        None => std::env::remove_var("LAZYWF_SECRET_DIRS"),
    }
}

/// A scratch directory of this test run under the repository's `tmp/`, the shape the other unit
/// tests use (`src/tests/aws_provider_tests.rs`): what a test wrote stays readable next to the
/// rest of the test output instead of disappearing into `/tmp`.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("unit")
        .join(format!("target-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The http bind belongs to the http kinds. A kind whose transport is another one's must be
/// refused by name rather than have a request built it can never put on the wire: this is the
/// arm between the http front of `http_bind` and `send`'s own dispatch by kind.
#[tokio::test]
async fn a_kind_that_speaks_another_transport_is_refused_the_http_bind_by_name() {
    for (kind, endpoint) in [
        ("redis", "redis://cache.example.test:6380/0"),
        ("kafka", "kafka://mq.example.test:9094/orders"),
    ] {
        let t = target(kind, endpoint, "{}");
        let req = RawRequest::parse("POST /h HTTP/1.1\n\n{}")
            .unwrap()
            .unwrap();
        let e = match t.http_bind(&t.endpoints[0], req).await {
            Ok(_) => panic!("a {kind} target has no http transport to bind a request to"),
            Err(e) => format!("{e:#}"),
        };
        assert!(e.contains("is not an http target"), "{kind}: {e}");
        assert!(e.contains("t"), "{kind}: the refusal names the target: {e}");
    }
}

/// Credentials that resolved to nothing are refused by the signer rather than sent: a request
/// signed with an empty key is not an anonymous request, it is a request AWS will refuse in a
/// way that reads like a target bug, and the tool would have paid a round trip for it.
#[tokio::test]
async fn credentials_that_are_empty_are_refused_before_the_signed_send() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_TEST_AWS_EMPTY_AK", "");
    std::env::set_var("LWF_TEST_AWS_EMPTY_SK", "");
    let t = target(
        "sqs",
        "aws-sqs://eu-west-1/123456789012/orders?endpoint=http://127.0.0.1:34561",
        r#"{"access_key_ref":"env:LWF_TEST_AWS_EMPTY_AK","secret_key_ref":"env:LWF_TEST_AWS_EMPTY_SK"}"#,
    );
    let req = RawRequest::parse("POST / HTTP/1.1\n\n{}").unwrap().unwrap();
    let e = match t.prepare(req, "", "sqs.eu-west-1.amazonaws.com").await {
        Ok(_) => panic!("empty credentials must not be signed and sent"),
        Err(e) => format!("{e:#}"),
    };
    assert!(e.contains("empty AWS credentials"), "{e}");
}

/// A credential var that is absent, not merely empty: the load path cannot produce this (the
/// row check requires the vars each kind names), so it is written the way the engine would
/// meet it — a def that got past that check. The answer is the var by name, never an empty
/// claim on the wire, and never a panic on a missing map entry.
#[tokio::test]
async fn a_credential_var_that_is_absent_is_named_and_not_left_empty() {
    let def = TargetDef {
        name: "t".into(),
        kind: Kind::HttpBearer,
        endpoints: vec!["https://api.example.test/hook".into()],
        tls: crate::tls::TlsSpec::parse(None).unwrap(),
        timeout: std::time::Duration::from_secs(1),
        expect: None,
        vars: serde_json::Map::new(),
    };
    let t = Target::new(
        def,
        mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x"),
        None,
    )
    .unwrap();
    let req = RawRequest::parse("POST /h HTTP/1.1\n\n{}")
        .unwrap()
        .unwrap();
    let e = match t.prepare(req, "", "api.example.test").await {
        Ok(_) => panic!("a bearer target with no `token` var must not be prepared"),
        Err(e) => format!("{e:#}"),
    };
    assert!(e.contains("vars.token is missing"), "{e}");
}
