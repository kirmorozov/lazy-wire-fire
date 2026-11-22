//! Which verdict an answer earns: refused ([`Rejected`], the key is dropped) or transient
//! (the key waits and retries). Every classification arm of `Target::send`,
//! `Target::send_items` and `Target::fetch` that the unit suites did not reach: the status
//! rules (5xx, 408, 429), the AWS throttling codes that override a status, answers that
//! are not JSON, empty renders, the fetch guards, a Lambda's `batchItemFailures`, and a
//! `PublishBatch` answer read per entry. Each test asserts the exact verdict and the
//! message, never merely that nothing panicked.

use super::*;
use crate::model::{RawTarget, TargetDef};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A target of `kind` with one endpoint, without a database: what a transport decides
/// about an answer is what is checked here.
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

/// The two credential references every AWS-kind target of these tests carries: real
/// signing against a mock that does not check the signature, and no default-chain hunt.
fn aws_vars() -> &'static str {
    r#"{"access_key_ref":"env:LWF_TEST_AWS_AK","secret_key_ref":"env:LWF_TEST_AWS_SK"}"#
}

fn set_aws_env() {
    std::env::set_var("LWF_TEST_AWS_AK", "AKIAIOSFODNN7EXAMPLE");
    std::env::set_var(
        "LWF_TEST_AWS_SK",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    );
}

fn sqs(addr: SocketAddr) -> Arc<Target> {
    target(
        "sqs",
        &format!("aws-sqs://eu-west-1/123456789012/orders?endpoint=http://{addr}"),
        aws_vars(),
    )
}

fn sns(addr: SocketAddr) -> Arc<Target> {
    target(
        "sns",
        &format!("aws-sns://eu-west-1/123456789012/orders?endpoint=http://{addr}"),
        aws_vars(),
    )
}

fn lambda(addr: SocketAddr, event: bool) -> Arc<Target> {
    let url = if event { "invocation=event&" } else { "" };
    target(
        "lambda",
        &format!("aws-lambda://eu-west-1/orders?{url}endpoint=http://{addr}"),
        aws_vars(),
    )
}

fn http(addr: SocketAddr) -> Arc<Target> {
    target(
        "http",
        &format!("http://{addr}/hook"),
        r#"{"method":"POST"}"#,
    )
}

fn kinesis(addr: SocketAddr) -> Arc<Target> {
    target(
        "kinesis",
        &format!("aws-kinesis://eu-west-1/orders?endpoint=http://{addr}"),
        aws_vars(),
    )
}

fn eventbridge(addr: SocketAddr) -> Arc<Target> {
    target(
        "eventbridge",
        &format!("aws-eventbridge://eu-west-1/orders?endpoint=http://{addr}"),
        aws_vars(),
    )
}

/// One http server on loopback: it reads the whole request (head and body) and answers
/// `status` with `body` and the extra headers, with `Connection: close` so no connection
/// is pooled and the client re-connects for every request.
async fn serve_with(
    status: u16,
    headers: &[(&str, &str)],
    body: &str,
) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let got: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (status, headers, body) = (
        status,
        headers
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect::<Vec<(String, String)>>(),
        body.to_string(),
    );
    let sink = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let (headers, body, sink) = (headers.clone(), body.clone(), sink.clone());
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&chunk[..n]),
                    }
                }
                // Read the body the client is still writing: answering while it has more
                // to send makes the client's write fail, and the test would see a
                // connection error where it wanted the status code.
                if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&head[..end]).into_owned();
                    let want = text
                        .lines()
                        .find_map(|l| {
                            let (n, v) = l.split_once(':')?;
                            if n.trim().eq_ignore_ascii_case("content-length") {
                                v.trim().parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0usize);
                    let mut have = head.len() - (end + 4);
                    while have < want {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => have += n,
                        }
                    }
                }
                sink.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&head).into_owned());
                let mut reply = format!("HTTP/1.1 {status} X\r\n");
                for (n, v) in &headers {
                    reply.push_str(&format!("{n}: {v}\r\n"));
                }
                reply.push_str(&format!(
                    "Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ));
                let _ = s.write_all(reply.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    (addr, got)
}

async fn serve(status: u16, body: &str) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    serve_with(status, &[], body).await
}

/// The whole message of an error: the chain, as the log and `last_error` show it.
fn chain(e: &anyhow::Error) -> String {
    format!("{e:#}")
}

fn keys(ks: &[&str]) -> Vec<String> {
    ks.iter().map(|k| k.to_string()).collect()
}

/// The form body of a `PublishBatch` with one entry per pair, numbered from 1, as the
/// `sns` pre-check demands before it lets the request out.
fn publish_body(ids: &[&str]) -> String {
    let mut out = String::from(
        "Action=PublishBatch&Version=2010-03-31&TopicArn=arn%3Aaws%3Asns%3Aeu-west-1%3A123456789012%3Aorders",
    );
    for (n, id) in ids.iter().enumerate() {
        out.push_str(&format!(
            "&PublishBatchRequestEntries.member.{}.Id={}&PublishBatchRequestEntries.member.{}.Message=m{n}",
            n + 1,
            id,
            n + 1
        ));
    }
    out
}

// ——— the status rule of a batch answer (`Target::send_items`) ————————————————

#[tokio::test]
async fn a_batch_status_of_5xx_408_or_429_waits_and_other_denyings_refuse() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    for (status, transient) in [
        (500, true),
        (503, true),
        (408, true),
        (429, true),
        (400, false),
        (403, false),
    ] {
        let (addr, _) = serve(status, "the service speaks its piece").await;
        let t = sqs(addr);
        let e = t
            .send_items(0, "POST / HTTP/1.1\n\nAction=x", &keys(&["1"]))
            .await
            .unwrap_err();
        assert_eq!(
            is_rejected(&e),
            !transient,
            "status {status}: {}",
            chain(&e)
        );
        assert!(chain(&e).contains(&format!("status {status}")), "{e:#}");
    }
}

#[tokio::test]
async fn an_aws_error_code_says_not_now_whatever_the_status() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // A throttling code arrives with HTTP 400 on the AWS protocols: the code wins.
    let cases: Vec<(Vec<(&str, &str)>, &str, bool)> = vec![
        (vec![("x-amzn-errortype", "ThrottlingException")], "", true),
        (
            vec![("x-amzn-errortype", "com.amazonaws.sqs#QueueDoesNotExist")],
            "",
            false,
        ),
        (
            vec![],
            r#"{"__type":"ProvisionedThroughputExceededException","message":"Rate exceeded"}"#,
            true,
        ),
        (
            vec![],
            r#"{"__type":"AccessDenied","message":"nope"}"#,
            false,
        ),
        (
            vec![],
            "<ErrorResponse><Error><Code>RequestThrottled</Code><Message>slow down</Message></Error></ErrorResponse>",
            true,
        ),
        (
            vec![],
            "<ErrorResponse><Error><Code>AccessDenied</Code></Error></ErrorResponse>",
            false,
        ),
    ];
    for (headers, body, throttled) in cases {
        let (addr, _) = serve_with(400, &headers, body).await;
        let t = sqs(addr);
        let e = t
            .send_items(0, "POST / HTTP/1.1\n\nAction=x", &keys(&["1"]))
            .await
            .unwrap_err();
        assert_eq!(
            is_rejected(&e),
            !throttled,
            "{headers:?} {body}: {}",
            chain(&e)
        );
    }
}

#[tokio::test]
async fn only_aws_kinds_may_believe_a_throttling_code() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    // The same 400 with a throttling code: a plain http target has no AWS error protocol,
    // so the code is just a header and the denial stands as a refusal.
    let (addr, _) = serve_with(400, &[("x-amzn-errortype", "ThrottlingException")], "no").await;
    let t = http(addr);
    let e = t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap_err();
    assert!(
        is_rejected(&e),
        "a plain http target is never throttled: {}",
        chain(&e)
    );
    // And a 500 from the same plain target still says not now.
    let (addr, _) = serve(500, "later").await;
    let t = http(addr);
    let e = t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap_err();
    assert!(!is_rejected(&e), "a 500 is transient: {}", chain(&e));
}

#[tokio::test]
async fn a_credential_that_cannot_be_resolved_waits() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    // A bearer token whose `env:` reference names nothing: the secret cannot be read,
    // which says nothing about the key — the send waits, it is not a refusal.
    std::env::remove_var("LWF_TEST_BEARER_UNSET");
    let (addr, got) = serve(200, "ok").await;
    let t = target(
        "http.bearer",
        &format!("http://{addr}/hook"),
        r#"{"token":"env:LWF_TEST_BEARER_UNSET"}"#,
    );
    let e = t.send(0, "POST / HTTP/1.1\n\nx").await.unwrap_err();
    assert!(
        !is_rejected(&e),
        "an unreadable credential is transient: {}",
        chain(&e)
    );
    assert!(chain(&e).contains("vars.token"), "{e:#}");
    assert!(
        got.lock().unwrap().is_empty(),
        "the request went out without a token"
    );
}

#[tokio::test]
async fn an_answer_that_is_not_json_waits() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // A batch: the JSON the mapping needs cannot be read, so nothing is trusted.
    let (addr, _) = serve(200, "<html>a proxy answered, not SQS</html>").await;
    let t = sqs(addr);
    let e = t
        .send_items(0, "POST / HTTP/1.1\n\nAction=x", &keys(&["1"]))
        .await
        .unwrap_err();
    assert!(!is_rejected(&e), "a broken answer waits: {}", chain(&e));
    assert!(chain(&e).contains("the answer is not JSON"), "{e:#}");
}

// ——— the `PublishBatch` answer, read per entry ——————————————————————————————

#[tokio::test]
async fn a_publish_batch_answer_maps_every_entry_to_its_verdict() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // entry 0 stored, entry 1 refused for the sender's own sake, entry 2 failed for the
    // service's (wait), entry 3 not named at all (wait).
    let xml = r#"<?xml version="1.0"?>
<PublishBatchResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><PublishBatchResult><Successful><member><Id>0</Id><MessageId>m-0</MessageId></member></Successful><Failed><member><Id>1</Id><Code>InvalidParameter</Code><Message>Message too long</Message><SenderFault>true</SenderFault></member><member><Id>2</Id><Code>InternalFailure</Code><Message>try later</Message><SenderFault>false</SenderFault></member></Failed></PublishBatchResult></PublishBatchResponse>"#;
    let (addr, got) = serve(200, xml).await;
    let t = sns(addr);
    let items = t
        .send_items(
            0,
            &format!("POST / HTTP/1.1\n\n{}", publish_body(&["0", "1", "2", "3"])),
            &keys(&["0", "1", "2", "3"]),
        )
        .await
        .unwrap();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0], Item::Ok);
    match &items[1] {
        Item::Rejected(why) => {
            assert!(
                why.contains("item 1: InvalidParameter: Message too long"),
                "{why}"
            )
        }
        other => panic!("entry 1 should be refused, not {other:?}"),
    }
    match &items[2] {
        Item::Transient(why) => {
            assert!(why.contains("item 2: InternalFailure: try later"), "{why}")
        }
        other => panic!("entry 2 should wait, not {other:?}"),
    }
    match &items[3] {
        Item::Transient(why) => assert!(why.contains("item 3: no answer"), "{why}"),
        other => panic!("entry 3 should wait, not {other:?}"),
    }
    assert_eq!(got.lock().unwrap().len(), 1, "one request went out");
}

#[tokio::test]
async fn an_answer_that_is_not_a_publish_batch_result_waits() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    let (addr, _) = serve(200, "I am not XML at all").await;
    let t = sns(addr);
    let e = t
        .send_items(
            0,
            &format!("POST / HTTP/1.1\n\n{}", publish_body(&["0"])),
            &keys(&["0"]),
        )
        .await
        .unwrap_err();
    assert!(
        !is_rejected(&e),
        "an unreadable PublishBatch answer waits: {}",
        chain(&e)
    );
    assert!(
        chain(&e).contains("the answer is not a PublishBatchResult"),
        "{e:#}"
    );
}

#[tokio::test]
async fn entries_the_render_left_out_are_refused_before_the_request() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // Two keys, one entry: the answer is given by `Id`, so a missing entry is a broken
    // template — refused, and nothing goes out.
    let (addr, got) = serve(200, "must never be asked").await;
    let t = sns(addr);
    let e = t
        .send_items(
            0,
            &format!("POST / HTTP/1.1\n\n{}", publish_body(&["0"])),
            &keys(&["0", "1"]),
        )
        .await
        .unwrap_err();
    assert!(
        is_rejected(&e),
        "a broken render is a refusal: {}",
        chain(&e)
    );
    assert!(chain(&e).contains("the rendered request"), "{e:#}");
    assert!(
        got.lock().unwrap().is_empty(),
        "the request went out anyway"
    );
}

// ——— Kinesis `PutRecords` and EventBridge `PutEvents`, answered by position ————

/// The `PutEvents` request body `eventbridge_entries_ok` demands before the batch goes out.
fn put_events_body(n: usize) -> String {
    let entries: Vec<String> = (0..n)
        .map(|i| format!(r#"{{"Source":"s{i}","DetailType":"t","Detail":"{{}}"}}"#,))
        .collect();
    format!(r#"{{"Entries":[{}]}}"#, entries.join(","))
}

#[tokio::test]
async fn a_put_records_answer_marks_each_record_by_position() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // record 0 stored, record 1 a transient code, record 2 a clear refusal, record 3 absent.
    let (addr, _) = serve(
        200,
        r#"{"Records":[
            {"SequenceNumber":"1","Data":"e30="},
            {"ErrorCode":"ProvisionedThroughputExceededException","ErrorMessage":"Rate exceeded"},
            {"ErrorCode":"ValidationException","ErrorMessage":"too big"}
        ]}"#,
    )
    .await;
    let t = kinesis(addr);
    let items = t
        .send_items(0, "POST / HTTP/1.1\n\n{}", &keys(&["0", "1", "2", "3"]))
        .await
        .unwrap();
    assert_eq!(items[0], Item::Ok);
    match &items[1] {
        Item::Transient(why) => assert!(
            why.contains("record 1: ProvisionedThroughputExceededException: Rate exceeded"),
            "{why}"
        ),
        other => panic!("record 1 should wait, not {other:?}"),
    }
    match &items[2] {
        Item::Rejected(why) => {
            assert!(
                why.contains("record 2: ValidationException: too big"),
                "{why}"
            )
        }
        other => panic!("record 2 should be refused, not {other:?}"),
    }
    match &items[3] {
        Item::Transient(why) => assert!(why.contains("record 3: no answer"), "{why}"),
        other => panic!("record 3 should wait, not {other:?}"),
    }
}

#[tokio::test]
async fn a_put_events_answer_marks_each_entry_by_position() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    let (addr, _) = serve(
        200,
        r#"{"Entries":[
            {"EventId":"e-0"},
            {"ErrorCode":"InternalFailure","ErrorMessage":"unwell"},
            {"ErrorCode":"MalformedDetail","ErrorMessage":"not JSON"}
        ]}"#,
    )
    .await;
    let t = eventbridge(addr);
    let items = t
        .send_items(
            0,
            &format!("POST / HTTP/1.1\n\n{}", put_events_body(4)),
            &keys(&["0", "1", "2", "3"]),
        )
        .await
        .unwrap();
    assert_eq!(items[0], Item::Ok);
    match &items[1] {
        Item::Transient(why) => {
            assert!(why.contains("entry 1: InternalFailure: unwell"), "{why}")
        }
        other => panic!("entry 1 should wait, not {other:?}"),
    }
    match &items[2] {
        Item::Rejected(why) => {
            assert!(why.contains("entry 2: MalformedDetail: not JSON"), "{why}")
        }
        other => panic!("entry 2 should be refused, not {other:?}"),
    }
    match &items[3] {
        Item::Transient(why) => assert!(why.contains("entry 3: no answer"), "{why}"),
        other => panic!("entry 3 should wait, not {other:?}"),
    }
}

#[tokio::test]
async fn a_put_events_render_missing_an_entry_is_refused_before_the_request() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // Three keys, a body of two entries: the answer is by position, so a mismatch is
    // the template's fault — refused, nothing goes out.
    let (addr, got) = serve(200, r#"{"Entries":[]}"#).await;
    let t = eventbridge(addr);
    let e = t
        .send_items(
            0,
            &format!("POST / HTTP/1.1\n\n{}", put_events_body(2)),
            &keys(&["0", "1", "2"]),
        )
        .await
        .unwrap_err();
    assert!(
        is_rejected(&e),
        "a short render is a refusal: {}",
        chain(&e)
    );
    assert!(chain(&e).contains("the rendered request"), "{e:#}");
    assert!(
        got.lock().unwrap().is_empty(),
        "the request went out anyway"
    );
}

// ——— renders of nothing ————————————————————————————————————————————————————

#[tokio::test]
async fn a_render_of_nothing_sends_nothing_and_is_met() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    let (addr, got) = serve(200, "{}").await;
    // A batch of nothing: every item is met, no request.
    let t = sqs(addr);
    assert_eq!(
        t.send_items(0, "   ", &keys(&["1", "2"])).await.unwrap(),
        vec![Item::Ok, Item::Ok]
    );
    // A single action of nothing, plain and lambda.
    let t = http(addr);
    assert_eq!(t.send(0, "  \n ").await.unwrap(), Sent::Skipped);
    let t = lambda(addr, false);
    assert_eq!(t.send(0, "").await.unwrap(), Sent::Skipped);
    assert_eq!(
        t.send_items(0, " ", &keys(&["1"])).await.unwrap(),
        vec![Item::Ok]
    );
    assert!(
        got.lock().unwrap().is_empty(),
        "an empty render reached the wire"
    );
}

// ——— the fetch guards ———————————————————————————————————————————————————————

#[tokio::test]
async fn kinds_that_only_append_or_push_refuse_to_list() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let was = std::env::var("LAZYWF_SECRET_DIRS").ok();
    let dir = std::env::temp_dir().join(format!("lazywf-outcome-grpc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let desc = dir.join("lazywf.desc");
    std::fs::write(&desc, crate::targets::grpc::contract::DESCRIPTOR).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
    std::env::set_var("LWF_TEST_OUTCOME_PW", "p");
    let creds = r#"{"username":"u","password":"env:LWF_TEST_OUTCOME_PW"}"#;
    let grpc_vars = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item"}}"#,
        desc.display()
    );
    let cases = [
        (
            "grpc",
            "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push",
            grpc_vars.as_str(),
            "kind grpc pushes: it cannot list keys",
        ),
        (
            "servicebus",
            "sb://127.0.0.1:34610/orders",
            "{}",
            "kind servicebus appends: it cannot list keys",
        ),
        (
            "kafka",
            "kafka://mq.example.test:9094/orders",
            "{}",
            "kind kafka appends: it cannot list keys",
        ),
        (
            "amqp",
            "amqp://mq.example.test:5672/",
            creds,
            "kind amqp appends: it cannot list keys",
        ),
        (
            "smtp",
            "smtp://mail.example.test:587",
            creds,
            "kind smtp appends: it cannot list keys",
        ),
        (
            "rabbitmq-stream",
            "rabbitmq-stream://127.0.0.1:34612/orders",
            creds,
            "kind rabbitmq-stream appends: it cannot list keys",
        ),
        (
            "pubsub",
            "gc-pubsub://lwf-test/orders",
            "{}",
            "pubsub targets only publish: a listing is refused",
        ),
    ];
    let mut messages = Vec::new();
    for (kind, endpoint, vars, want) in cases {
        let t = target(kind, endpoint, vars);
        let uses = crate::response::Uses {
            body: true,
            ..Default::default()
        };
        let e = t.fetch(0, "GET / HTTP/1.1\n\n", uses).await.unwrap_err();
        let msg = chain(&e);
        assert!(msg.contains(want), "{kind}: {msg}");
        messages.push(msg);
    }
    match was {
        Some(v) => std::env::set_var("LAZYWF_SECRET_DIRS", v),
        None => std::env::remove_var("LAZYWF_SECRET_DIRS"),
    }
}

#[tokio::test]
async fn a_listing_that_denies_or_answers_garbage_fails_the_step() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    // A denied listing names the status; it is a failed step, not a refused key.
    let (addr, _) = serve(403, "no listings here").await;
    let t = http(addr);
    let uses = crate::response::Uses {
        body: true,
        ..Default::default()
    };
    let e = t
        .fetch(0, "GET /keys HTTP/1.1\n\n", uses)
        .await
        .unwrap_err();
    assert!(chain(&e).contains("status 403"), "{e:#}");
    // A listing whose JSON the step reads cannot be read: the step fails, no key is judged.
    let (addr, _) = serve(200, "keys keys keys").await;
    let t = http(addr);
    let e = t
        .fetch(0, "GET /keys HTTP/1.1\n\n", uses)
        .await
        .unwrap_err();
    assert!(
        chain(&e).contains("the listing response is not JSON"),
        "{e:#}"
    );
}

// ——— the Lambda verdicts —————————————————————————————————————————————————————

#[tokio::test]
async fn a_lambda_naming_failures_on_its_own_leaves_the_action_unconfirmed() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // One action of its own cannot name items: a listed identifier is transient.
    let (addr, _) = serve(200, r#"{"batchItemFailures":[{"itemIdentifier":"7"}]}"#).await;
    let t = lambda(addr, false);
    let e = t.send(0, r#"{"id":7}"#).await.unwrap_err();
    assert!(
        !is_rejected(&e),
        "the function asks for a retry, so the key waits: {}",
        chain(&e)
    );
    assert!(chain(&e).contains("batchItemFailures lists"), "{e:#}");
    assert!(chain(&e).contains("the action is not confirmed"), "{e:#}");
    // A clean answer confirms it.
    let (addr, _) = serve(200, r#"{"ok":true}"#).await;
    let t = lambda(addr, false);
    assert_eq!(t.send(0, r#"{"id":7}"#).await.unwrap(), Sent::Ok);
}

#[tokio::test]
async fn a_function_error_is_refused_and_an_event_answered_wrong_waits() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // `X-Amz-Function-Error` on a 200: the function raised, and it would raise again.
    let (addr, _) = serve_with(
        200,
        &[("X-Amz-Function-Error", "Unhandled")],
        r#"{"errorMessage":"boom"}"#,
    )
    .await;
    let t = lambda(addr, false);
    let e = t.send(0, r#"{"id":7}"#).await.unwrap_err();
    assert!(is_rejected(&e), "the function raised: {}", chain(&e));
    assert!(
        chain(&e).contains("the function raised an error (Unhandled)"),
        "{e:#}"
    );
    // An event invocation that is answered 200: Lambda itself says 202; anything else is
    // something the endpoint should not be doing (a proxy in front, say) — wait.
    let (addr, _) = serve(200, "").await;
    let t = lambda(addr, true);
    let e = t.send(0, r#"{"id":7}"#).await.unwrap_err();
    assert!(
        !is_rejected(&e),
        "an event answered 200 is a surprise, not a refusal: {}",
        chain(&e)
    );
    assert!(
        chain(&e).contains("an event invocation answers 202, not 200"),
        "{e:#}"
    );
    // The 202 it should answer confirms the action.
    let (addr, _) = serve(202, "").await;
    let t = lambda(addr, true);
    assert_eq!(t.send(0, r#"{"id":7}"#).await.unwrap(), Sent::Ok);
}

#[tokio::test]
async fn a_lambda_batch_answer_marks_the_failed_keys_transient() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // A partial-batch answer: exactly the listed key waits, the other is met.
    let (addr, _) = serve(200, r#"{"batchItemFailures":[{"itemIdentifier":"7"}]}"#).await;
    let t = lambda(addr, false);
    let items = t
        .send_items(0, r#"{"ids":[7,8]}"#, &keys(&["7", "8"]))
        .await
        .unwrap();
    match &items[0] {
        Item::Transient(why) => assert!(
            why.contains("key `7`: listed in batchItemFailures"),
            "{why}"
        ),
        other => panic!("key 7 should wait, not {other:?}"),
    }
    assert_eq!(items[1], Item::Ok);
    // An identifier that is no key of the batch: the whole answer is untrusted.
    let (addr, _) = serve(200, r#"{"batchItemFailures":[{"itemIdentifier":"9"}]}"#).await;
    let t = lambda(addr, false);
    let e = t
        .send_items(0, r#"{"ids":[7]}"#, &keys(&["7"]))
        .await
        .unwrap_err();
    assert!(
        !is_rejected(&e),
        "a wrong answer is not a refusal: {}",
        chain(&e)
    );
    assert!(chain(&e).contains("not a key of the batch"), "{e:#}");
}

#[tokio::test]
async fn a_lambda_status_and_throttling_code_classify_as_the_batch_does() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    for (status, headers, body, transient) in [
        (500u16, Vec::<(&str, &str)>::new(), "", true),
        (429, vec![], "too fast", true),
        (400, vec![], r#"{"message":"bad"}"#, false),
        (
            400,
            vec![("x-amzn-errortype", "ThrottlingException")],
            "",
            true,
        ),
    ] {
        let (addr, _) = serve_with(status, &headers, body).await;
        let t = lambda(addr, false);
        let e = t
            .send_items(0, r#"{"ids":[1]}"#, &keys(&["1"]))
            .await
            .unwrap_err();
        assert_eq!(
            is_rejected(&e),
            !transient,
            "status {status} {headers:?}: {}",
            chain(&e)
        );
    }
}

#[tokio::test]
async fn a_payload_over_the_invoke_limit_is_refused_before_the_request() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    let (addr, got) = serve(200, "").await;
    // An event invocation takes 1 MB: one byte over is refused without a request.
    let t = lambda(addr, true);
    let payload = "x".repeat(1024 * 1024 + 1);
    let e = t.send(0, &payload).await.unwrap_err();
    assert!(
        is_rejected(&e),
        "an oversized payload never travels: {}",
        chain(&e)
    );
    assert!(
        chain(&e).contains("over the 1048576 bytes an Invoke takes"),
        "{e:#}"
    );
    assert!(
        got.lock().unwrap().is_empty(),
        "the payload went out anyway"
    );
}

/// The engine asks the target which front to use at all: kinds whose answer names the
/// items per entry (batch verdicts) go through `send_items`, the rest through `send`.
#[test]
fn only_the_kinds_that_answer_per_item_are_offered_the_batch_front() {
    for (kind, endpoint, vars) in [
        ("sqs", "aws-sqs://eu-west-1/123456789012/orders", "{}"),
        ("sns", "aws-sns://eu-west-1/123456789012/orders", "{}"),
        ("lambda", "aws-lambda://eu-west-1/orders", "{}"),
        ("kinesis", "aws-kinesis://eu-west-1/orders", "{}"),
        ("eventbridge", "aws-eventbridge://eu-west-1/orders", "{}"),
        ("kafka", "kafka://mq.example.test:9094/orders", "{}"),
    ] {
        assert!(
            target(kind, endpoint, vars).reports_items(),
            "{kind} answers per item"
        );
    }
    for (kind, endpoint, vars) in [
        ("redis", "redis://cache.example.test:6380/0", "{}"),
        ("http", "http://api.example.test/hook", "{}"),
    ] {
        assert!(
            !target(kind, endpoint, vars).reports_items(),
            "{kind} answers for the whole action only"
        );
    }
}

// ——— the batch front of a transport that answers nothing per item, and the answer
// that numbers its entries wrong ————————————————————————————————————————————————————

/// The batch front belongs to the kinds whose answer names every entry. Asked for it on a
/// transport that cannot answer per item, the answer is a clear failure of the target — never
/// a refusal of the keys, which the engine would drop. The engine only ever asks after
/// [`Target::reports_items`] said yes, so this guard speaks about a wrong call, not about a
/// key: which is exactly why it must not be a refusal either.
#[tokio::test]
async fn a_transport_that_never_answers_per_item_fails_the_target_and_not_the_keys() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    let cases = [
        ("redis", "redis://cache.example.test:6380/0", "{}"),
        (
            "smtp",
            "smtp://mail.example.test:587",
            r#"{"username":"u","password":"p"}"#,
        ),
        (
            "amqp",
            "amqp://mq.example.test:5672/",
            r#"{"username":"u","password":"p"}"#,
        ),
        (
            "mysql",
            "mysql://db.example.test:3306/app",
            r#"{"username":"u","password":"p"}"#,
        ),
    ];
    for (kind, endpoint, vars) in cases {
        let t = target(kind, endpoint, vars);
        let e = t
            .send_items(0, "POST / HTTP/1.1\n\n{}", &keys(&["1", "2"]))
            .await
            .unwrap_err();
        assert!(
            !is_rejected(&e),
            "{kind}: the keys did nothing to deserve being dropped: {}",
            chain(&e)
        );
        assert!(
            chain(&e).contains("does not answer per item"),
            "{kind}: {:#}",
            e
        );
    }
}

/// A rendered request the parser cannot read is the render's fault, and a render is a
/// property of the key: unlike a transport that cannot answer per item, this one is a refusal,
/// so a broken template costs the key its place in the lane instead of holding it forever.
#[tokio::test]
async fn a_render_the_batch_front_cannot_parse_is_a_refusal() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    for rendered in ["nonsense", "GET / HTTP/1.1\nHeaderWithNoColon\n\n{}"] {
        let (addr, got) = serve(200, "{}").await;
        let t = sqs(addr);
        let e = t
            .send_items(0, rendered, &keys(&["1", "2"]))
            .await
            .unwrap_err();
        assert!(
            is_rejected(&e),
            "a render the parser refuses is the key's own fault: {:#}",
            e
        );
        assert!(
            chain(&e).contains("the rendered request"),
            "{rendered}: {:#}",
            e
        );
        assert!(got.lock().unwrap().is_empty(), "nothing reached the wire");
    }
}

/// The `Id` of an entry is how a batch answer says which key it means. An answer that writes
/// it as a number, or writes one that is not in the batch, cannot be mapped onto keys at all:
/// the honest answer is that nothing was answered, so no key is dropped on it.
#[tokio::test]
async fn an_aws_batch_answer_that_numbers_its_entries_wrong_answers_none_of_them() {
    let _env = crate::secrets::ENV_LOCK.lock().await;
    set_aws_env();
    // A numeric `Id` is the index it means: the batch is answered.
    let (addr, _) = serve(
        200,
        r#"{"Successful":[{"Id":1,"MessageId":"m"}],"Failed":[]}"#,
    )
    .await;
    let items = sqs(addr)
        .send_items(0, "POST / HTTP/1.1\n\nAction=x", &keys(&["1", "2"]))
        .await
        .unwrap();
    assert_eq!(items[1], Item::Ok, "the numeric Id named the second key");
    assert!(
        matches!(&items[0], Item::Transient(why) if why.contains("no answer")),
        "the key the answer never named waits: {items:?}"
    );
    // An `Id` the batch does not contain: the whole answer is refused, so no key of it is
    // believed — and no key of it is dropped either.
    for answer in [
        r#"{"Successful":[],"Failed":[{"Id":"7","Code":"X","Message":"y","SenderFault":true}]}"#,
        r#"{"Successful":[{"Id":true,"MessageId":"m"}],"Failed":[]}"#,
        r#"{"Successful":[{"Id":"0","MessageId":"m"}],"Failed":[{"Id":"x","Code":"X","Message":"y"}]}"#,
    ] {
        let (addr, _) = serve(200, answer).await;
        let e = sqs(addr)
            .send_items(0, "POST / HTTP/1.1\n\nAction=x", &keys(&["1", "2"]))
            .await
            .unwrap_err();
        let msg = chain(&e);
        assert!(msg.contains("not one of 2"), "{answer}: {msg}");
        assert!(
            !is_rejected(&e),
            "{answer}: an answer that cannot be read says nothing about the keys: {msg}"
        );
    }
}
