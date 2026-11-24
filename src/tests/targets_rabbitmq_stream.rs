use super::*;
use crate::model::RawTarget;
use serde_json::json;

fn spec(s: &str) -> TlsSpec {
    TlsSpec::parse(Some(s)).unwrap()
}

fn vars(s: &str) -> Map<String, Value> {
    serde_json::from_str(s).unwrap()
}

fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
    RawTarget {
        name: "events".into(),
        kind: Some("rabbitmq-stream".into()),
        endpoints: Some(endpoints.into()),
        tls: tls.map(Into::into),
        timeout_ms: None,
        expect: None,
        vars: Some(vars.into()),
    }
}

const CREDS: &str = r#"{"username": "lwf", "password": "env:LWF_TEST_RMQSTREAM_PW"}"#;

#[test]
fn endpoints() {
    let a = Addr::parse("rabbitmq-stream://broker:5552/orders").unwrap();
    assert_eq!(
        (
            a.secure,
            a.host.as_str(),
            a.port,
            a.vhost.as_str(),
            a.stream.as_str()
        ),
        (false, "broker", 5552, "/", "orders")
    );
    let a = Addr::parse("rabbitmq-stream+tls://mq.internal/orders.v2").unwrap();
    assert_eq!(
        (a.secure, a.host.as_str(), a.port, a.stream.as_str()),
        (true, "mq.internal", 5551, "orders.v2")
    );
    // The vhost is the first path segment; `%2F` names the default one.
    let a = Addr::parse("rabbitmq-stream://127.0.0.1:34572/app/orders:eu").unwrap();
    assert_eq!((a.vhost.as_str(), a.stream.as_str()), ("app", "orders:eu"));
    let a = Addr::parse("rabbitmq-stream://127.0.0.1/%2F/orders").unwrap();
    assert_eq!((a.vhost.as_str(), a.stream.as_str()), ("/", "orders"));
}

#[test]
fn endpoints_that_do_not_parse() {
    // The scheme of another kind, credentials in the URL, a missing stream, a bad name.
    for bad in [
        "rabbitmq://h/orders",
        "amqp://h/orders",
        "rabbitmq-stream://user:pw@h/orders",
        "rabbitmq-stream://h",
        "rabbitmq-stream://h//orders",
        "rabbitmq-stream://h/a/b/c",
        "rabbitmq-stream://h/ord ers",
        "rabbitmq-stream://h/orders?x=1",
        "rabbitmq-stream://:5552/orders",
        "rabbitmq-stream://h/[::1]",
    ] {
        assert!(Addr::parse(bad).is_err(), "accepted `{bad}`");
    }
    // An empty stream name and one with a forbidden character.
    assert!(stream_name("").is_err());
    assert!(stream_name("orders/1").is_err());
    assert!(stream_name("orders:eu-2_x").is_ok());
}

#[test]
fn tls_modes() {
    // Only verify-full with exactly one named CA exists (the crate trusts that file
    // alone); everything that would skip the name check or the certificate is refused
    // by name, as amqp does.
    assert!(tls_config(&spec("disabled"), false).unwrap().is_none());
    for bad in [
        "disabled".to_string(),
        "required".to_string(),
        "verify-ca".to_string(),
        r#"{"mode": "verify-full"}"#.to_string(),
        r#"{"mode": "verify-full", "ca": ["/a.pem", "/b.pem"]}"#.to_string(),
        r#"{"mode": "verify-full", "server_name": "mq"}"#.to_string(),
    ] {
        let e = format!("{:#}", tls_config(&spec(&bad), true).err().unwrap());
        let wants = match bad.as_str() {
            "disabled" => "disabled",
            "required" => "required",
            "verify-ca" => "verify-ca",
            _ if bad.contains("server_name") => "server_name",
            _ if bad.contains('[') => "exactly one",
            _ => "tls.ca",
        };
        assert!(e.contains(wants), "{bad} => {e}");
    }
    let t = tls_config(&spec(r#"{"mode": "verify-full", "ca": "/ca.pem"}"#), true)
        .unwrap()
        .unwrap();
    assert!(matches!(
        t,
        TlsConfiguration::Trusted { ref root_certificates_path, client_certificates: None }
            if root_certificates_path == &PathBuf::from("/ca.pem")
    ));
}

#[test]
fn credentials() {
    // A literal password is a load error; the username may be a literal.
    let e = format!(
        "{:#}",
        creds(&vars(r#"{"username": "lwf", "password": "hunter2"}"#))
            .err()
            .unwrap()
    );
    assert!(e.contains("reference"), "{e}");
    assert!(creds(&vars(CREDS)).is_ok());
    for missing in [
        r#"{"password": "env:X"}"#,
        r#"{"username": "u"}"#,
        r#"{"username": "", "password": "env:X"}"#,
    ] {
        assert!(creds(&vars(missing)).is_err(), "accepted {missing}");
    }
}

#[test]
fn target_checks() {
    // One endpoint, its form, the TLS mode and the account: all decided at load.
    check(&["rabbitmq-stream://h/orders".into()], &vars(CREDS), None).unwrap();
    let e = format!(
        "{:#}",
        check(
            &[
                "rabbitmq-stream://h/a".into(),
                "rabbitmq-stream://h/b".into()
            ],
            &vars(CREDS),
            None
        )
        .err()
        .unwrap()
    );
    assert!(e.contains("exactly one endpoint"), "{e}");
    let e = format!(
        "{:#}",
        check(&["kafka://h/t".into()], &vars(CREDS), None)
            .err()
            .unwrap()
    );
    assert!(e.contains("rabbitmq-stream://"), "{e}");
    // The scheme belongs to this kind alone.
    refuse_scheme(false, &["rabbitmq-stream+tls://h/s".into()]).unwrap_err();
    refuse_scheme(true, &["rabbitmq-stream+tls://h/s".into()]).unwrap();
    refuse_scheme(false, &["amqp://h/q".into()]).unwrap();
    // The end-to-end load check of a row (from_row reaches these through check).
    crate::model::TargetDef::from_row(&raw("rabbitmq-stream://h/orders", None, CREDS)).unwrap();
    crate::model::TargetDef::from_row(&raw(
        "rabbitmq-stream://h/orders",
        None,
        r#"{"username": "u", "password": "literal"}"#,
    ))
    .unwrap_err();
}

#[test]
fn the_rendered_form() {
    let body = |s: Value| serde_json::to_vec(&s).unwrap();
    // The shape of the module docs: value plus an optional routing key.
    let a = Action::parse(
        &body(json!({"records": [
            {"value": {"a": 1}, "routing_key": "k1"},
            {"value": 2, "routing_key": null},
            {"value": [1, 2]}
        ]})),
        3,
    )
    .unwrap()
    .unwrap();
    assert_eq!(a.records[0].value, json!({"a": 1}));
    assert_eq!(a.records[0].routing_key.as_deref(), Some("k1"));
    assert_eq!(a.records[0].bytes, br#"{"a":1}"#);
    assert_eq!(a.records[1].routing_key, None);
    assert_eq!(a.records[2].bytes, b"[1,2]");
    // Nothing rendered: the empty batch.
    assert!(Action::parse(&body(json!({"records": []})), 0)
        .unwrap()
        .is_none());
    // What is refused, and why.
    for (bad, n_keys) in [
        (json!({"records": [{"value": null}]}), 1), // a null value
        (json!({"records": [{"value": 1, "key": "x"}]}), 1), // an unknown record field
        (json!({"records": [{"routing_key": "k"}]}), 1), // no value
        (json!({"records": [{"value": 1, "routing_key": 7}]}), 1), // a numeric routing key
        (json!({"records": [{"value": 1}, {"value": 2}]}), 1), // records for one key
        (json!({"records": [{"value": 1}]}), 2),    // fewer records than keys
        (json!("not json at all"), 0),              // not a body
    ] {
        assert!(
            Action::parse(&body(bad.clone()), n_keys).is_err(),
            "accepted {bad}"
        );
    }
    // A batch over the frame limit refuses the whole action (deterministic: the broker
    // would never take this frame).
    let big = vec![Value::String("x".repeat(64 * 1024)); 20]; // 1.25 MB of values
    let e = format!(
        "{:#}",
        Action::parse(
            &body(json!({"records": big.iter().map(|v| json!({"value": v})).collect::<Vec<_>>()})),
            20
        )
        .err()
        .unwrap()
    );
    assert!(e.contains("frame"), "{e}");
}

/// The super stream partition rule: the same key always lands on the same partition, a key
/// never lands outside the partitions the broker named, the partitions are all reached (so
/// the hash is the hash, not a constant), and no partitions is no answer at all.
#[test]
fn super_stream_partition_of_a_routing_key() {
    let parts: Vec<String> = (0..3).map(|i| format!("orders-{i}")).collect();
    let mut hit = [false; 3];
    for n in 0..200 {
        let key = format!("order-{n}");
        let i = partition_of(&parts, &key).expect("a partition");
        assert_eq!(
            Some(i),
            partition_of(&parts, &key),
            "`{key}` must land in the same partition twice"
        );
        hit[i] = true;
    }
    assert!(hit.iter().all(|h| *h), "every partition is used: {hit:?}");
    assert!(
        partition_of(&parts, "").is_some(),
        "an empty routing key still has a partition"
    );
    assert_eq!(partition_of(&[], "any"), None, "no partitions, no answer");
}

#[test]
fn publish_error_codes() {
    // The one code that speaks about this record alone refuses it; its neighbours of the
    // batch go through.
    assert!(matches!(
        code_outcome(ResponseCode::FrameTooLarge),
        PublishErr::Refused(_)
    ));
    // The code that says this account may not publish here denies the whole call instead:
    // a super stream's partitions have rights of their own, and a per-record verdict would
    // strike the keys of the forbidden partition until they were dropped.
    assert!(matches!(
        code_outcome(ResponseCode::AccessRefused),
        PublishErr::Denied(_)
    ));
    // …and everything else, including the codes that say "not now" or nothing, retries.
    for c in [
        ResponseCode::StreamNotAvailable,
        ResponseCode::InternalError,
        ResponseCode::PublisherDoesNotExist,
        ResponseCode::StreamDoesNotExist,
        ResponseCode::PreconditionFailed,
        ResponseCode::Ok, // a code that means nothing on a publish error
    ] {
        assert!(
            matches!(code_outcome(c.clone()), PublishErr::Transient(_)),
            "{c:?}"
        );
    }
    // A denial reaches the engine as the whole call's refusal (`Rejected`, the shape of the
    // declare and the connect ones), which is what keeps the keys from being struck.
    let e = denied("lwf.orders", "the account may not publish here");
    assert!(crate::target::is_rejected(&e), "{e:#} is not a refusal");
    assert!(
        format!("{e:#}").contains("lwf.orders") && format!("{e:#}").contains("no key"),
        "{e:#} does not say what it refuses"
    );
}

#[test]
fn connect_failures_are_classified() {
    for c in [
        ResponseCode::AuthenticationFailure,
        ResponseCode::VirtualHostAccessFailure,
        ResponseCode::SaslError,
    ] {
        let e = connect_error(
            "rabbitmq-stream://h/s",
            ClientError::RequestError(c.clone()),
        );
        assert!(crate::target::is_rejected(&e), "{c:?} must refuse");
    }
    let e = connect_error("rabbitmq-stream://h/s", ClientError::AlreadyClosed);
    assert!(
        !crate::target::is_rejected(&e),
        "a closed connection must retry"
    );
}

#[test]
fn a_batch_over_the_frame_limit_refuses_at_load() {
    // rule_errors: the engine would otherwise cut the batch to a size the frame refuses.
    let mut def = RuleDef::placeholder(&crate::model::RawRule {
        name: "r".into(),
        enabled: true,
        ..Default::default()
    });
    def.fire_per = FirePer::Batch;
    def.batch_max_bytes = Some(4_000_000);
    let errs = rule_errors(&def, MESSAGE_MAX_BYTES);
    assert!(
        errs.iter().any(|e| e.contains("batch_max_bytes")),
        "{errs:?}"
    );
    def.batch_max_bytes = Some(65_536);
    assert!(rule_errors(&def, MESSAGE_MAX_BYTES).is_empty());
    def.fire_per = FirePer::Key;
    def.batch_max_bytes = Some(4_000_000);
    assert!(
        rule_errors(&def, MESSAGE_MAX_BYTES).is_empty(),
        "a key fires no batch"
    );
}

/// A poisoned confirm map is neither a second panic nor a stuck publisher. `resolve` still delivers
/// what the broker said, `lose_all` still answers everything still awaited as `Lost`, and an id that
/// never made it into the map before the panic is answered by the publish deadline — which is
/// `Confirm::Lost`, a retry, never a refusal.
#[tokio::test]
async fn confirm_waiters_settle_their_items_on_a_poisoned_lock() {
    let w: Waiters = StdMutex::new(HashMap::new());
    let (tx, before) = oneshot::channel();
    lock_waiters(&w).insert(1, tx);
    crate::poison::poison(&w);
    // Registered after the poison, exactly as the publish path registers a batch: one `insert` per
    // publishing id, the sender whole in each.
    let (tx2, after) = oneshot::channel();
    lock_waiters(&w).insert(2, tx2);
    resolve(&w, 1, Confirm::Confirmed);
    assert!(
        matches!(before.await.unwrap(), Confirm::Confirmed),
        "the confirm the broker gave still reaches the record it belongs to"
    );
    // An id the map never held is answered by nobody here — and that is the safe answer, because
    // the await turns it into `Lost` at the deadline.
    resolve(&w, 99, Confirm::Confirmed);
    lose_all(&w);
    assert!(
        matches!(after.await.unwrap(), Confirm::Lost),
        "everything still awaited is answered as lost, which the caller turns into a retry"
    );
    assert!(lock_waiters(&w).is_empty(), "and the map is empty again");
}

// ——— what the rendered action decides before the broker is asked ——————————————————
//
// `items_of` reads the rendered request and the publish body of a batch (`send_items`) or of a
// `once` step (`send`) before it opens a connection. These refusals are about the render, so the
// dead endpoint below answers them: a test that reached the broker would fail with a connection
// error instead of the message it asserts.

/// A client of `endpoint`, built and never connected.
fn client(endpoint: &str) -> Client {
    let def = TargetDef::from_row(&raw(endpoint, None, CREDS)).unwrap();
    Client::new(&def, endpoint).unwrap().1
}

fn keys(ks: &[&str]) -> Vec<String> {
    ks.iter().map(|k| k.to_string()).collect()
}

fn no_expect() -> Expect {
    Expect::new(None).unwrap()
}

/// A pool no test here uses: without an `expect` condition, `Expect::row` answers without
/// asking the database, so an unroutable DSN is long enough.
fn pool() -> Pool {
    mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x")
}

fn chain(e: &anyhow::Error) -> String {
    format!("{e:#}")
}

/// One publish body of `n` records, each with a routing key of its own.
fn body(n: usize) -> String {
    let recs: Vec<Value> = (0..n)
        .map(|i| json!({"value": i, "routing_key": format!("k{i}")}))
        .collect();
    json!({"records": recs}).to_string()
}

/// Only `POST /` is a publish; any other request line refuses the whole action.
#[tokio::test]
async fn only_a_post_to_the_root_is_a_publish() {
    let c = client("rabbitmq-stream://127.0.0.1:1/orders");
    for (rendered, why) in [
        (
            format!("GET / HTTP/1.1\n\n{}", body(1)),
            "GET / is not POST /",
        ),
        (
            format!("POST /in HTTP/1.1\n\n{}", body(1)),
            "POST /in is not POST /",
        ),
    ] {
        for batch in [true, false] {
            let e = if batch {
                send_items(&c, &rendered, &keys(&["1"]), &no_expect(), &pool())
                    .await
                    .unwrap_err()
            } else {
                send(&c, &rendered, &no_expect(), &pool())
                    .await
                    .unwrap_err()
            };
            assert!(
                crate::target::is_rejected(&e),
                "a request line that is not a publish refuses: {}",
                chain(&e)
            );
            let m = chain(&e);
            assert!(m.contains("rabbitmq-stream targets only publish"), "{m}");
            assert!(m.contains(why), "{m}");
        }
    }
}

/// A render that is not a request, and a body that is not a publish body: each names the part
/// that failed, and neither opens a connection.
#[tokio::test]
async fn a_render_that_is_not_a_publish_names_the_part_that_failed() {
    let c = client("rabbitmq-stream://127.0.0.1:1/orders");
    let e = send_items(
        &c,
        "POST /a HTTP/1.1 extra\n\n{}",
        &keys(&["1"]),
        &no_expect(),
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(crate::target::is_rejected(&e), "{}", chain(&e));
    let m = chain(&e);
    assert!(m.contains("the rendered request"), "{m}");
    assert!(m.contains("extra words"), "{m}");

    let e = send_items(
        &c,
        "POST / HTTP/1.1\n\nnot json",
        &keys(&["1"]),
        &no_expect(),
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(crate::target::is_rejected(&e), "{}", chain(&e));
    let m = chain(&e);
    assert!(m.contains("the publish body"), "{m}");

    // The `once` step reads its render the same way.
    let e = send(&c, "POST / HTTP/1.1\n\nnot json", &no_expect(), &pool())
        .await
        .unwrap_err();
    assert!(crate::target::is_rejected(&e), "{}", chain(&e));
    assert!(chain(&e).contains("the publish body"), "{}", chain(&e));
}

/// Nothing rendered publishes nothing: every item of the batch is ok (there was no answer to
/// have, and no key may be struck for it) and the `once` step is skipped, not refused.
#[tokio::test]
async fn a_render_of_nothing_publishes_nothing() {
    let c = client("rabbitmq-stream://127.0.0.1:1/orders");
    let items = send_items(&c, "  \n ", &keys(&["1", "2"]), &no_expect(), &pool())
        .await
        .unwrap();
    assert_eq!(
        items,
        vec![Item::Ok, Item::Ok],
        "a blank render is not a publish"
    );
    assert_eq!(
        send(&c, "", &no_expect(), &pool()).await.unwrap(),
        Sent::Skipped,
        "a blank `once` render is skipped"
    );
    assert_eq!(
        send(
            &c,
            "POST / HTTP/1.1\n\n{\"records\": []}",
            &no_expect(),
            &pool()
        )
        .await
        .unwrap(),
        Sent::Skipped
    );
    let items = send_items(
        &c,
        "POST / HTTP/1.1\n\n{\"records\": []}",
        &[],
        &no_expect(),
        &pool(),
    )
    .await
    .unwrap();
    assert!(items.is_empty(), "no keys, no items: {items:?}");
}

/// A `once` step has no keys, and record i takes key i: a `once` render that lists records
/// names records nothing can be keyed to, so it refuses the action rather than publish them
/// unkeyed — the same rule `Action::parse` states for a batch.
#[tokio::test]
async fn a_once_step_has_no_keys_so_it_cannot_list_records() {
    let c = client("rabbitmq-stream://127.0.0.1:1/orders");
    let e = send(
        &c,
        &format!("POST / HTTP/1.1\n\n{}", body(1)),
        &no_expect(),
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(
        crate::target::is_rejected(&e),
        "a `once` render that lists records refuses: {}",
        chain(&e)
    );
    let m = chain(&e);
    assert!(m.contains("the publish body"), "{m}");
    assert!(m.contains("1 records for 0 keys"), "{m}");
    let e = send_items(
        &c,
        &format!("POST / HTTP/1.1\n\n{}", body(1)),
        &keys(&["1", "2"]),
        &no_expect(),
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(crate::target::is_rejected(&e), "{}", chain(&e));
    assert!(chain(&e).contains("1 records for 2 keys"), "{}", chain(&e));
}

/// A broker that cannot be reached says nothing about the records: the call fails as a wait
/// (never a refusal, which would strike every key of the batch), naming the endpoint.
#[tokio::test]
async fn a_broker_that_never_answers_waits_and_says_why() {
    // The connect resolves the password reference, so the variable has to exist; the lock is
    // the one every unit test that writes the environment takes.
    let _env = crate::secrets::ENV_LOCK.lock().await;
    std::env::set_var("LWF_TEST_RMQSTREAM_PW", "s3cret");
    let c = client("rabbitmq-stream://127.0.0.1:1/orders");
    let e = send_items(
        &c,
        &format!("POST / HTTP/1.1\n\n{}", body(2)),
        &keys(&["1", "2"]),
        &no_expect(),
        &pool(),
    )
    .await
    .unwrap_err();
    assert!(
        !crate::target::is_rejected(&e),
        "an unreachable broker is not a verdict on the records: {}",
        chain(&e)
    );
    let m = chain(&e);
    assert!(m.contains("127.0.0.1:1"), "the endpoint is named: {m}");
    assert!(m.contains("connecting failed"), "{m}");
}
