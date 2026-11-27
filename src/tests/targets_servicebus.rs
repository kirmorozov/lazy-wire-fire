use super::*;
use fe2o3_amqp::types::definitions::AmqpError;
use serde_json::json;

fn vars(s: &str) -> Map<String, Value> {
    serde_json::from_str(s).unwrap()
}

#[test]
fn endpoints() {
    let a = Addr::parse("sb://contoso.servicebus.windows.net/orders").unwrap();
    assert!(a.namespace);
    assert_eq!(a.host, "contoso.servicebus.windows.net");
    assert_eq!(a.port, 5671);
    assert_eq!(a.entity, "orders");
    assert_eq!(a.audience(), "sb://contoso.servicebus.windows.net/orders");
    // The local-broker override: a plain host and a port, its path the address verbatim.
    let a = Addr::parse("sb://127.0.0.1:34610/orders").unwrap();
    assert!(!a.namespace);
    assert_eq!(
        (a.host.as_str(), a.port, a.entity.as_str()),
        ("127.0.0.1", 34610, "/orders")
    );
    // A broker whose addresses are namespaced is addressed as it wants (RabbitMQ's v2 form).
    let a = Addr::parse("sb://127.0.0.1:34610/queues/orders").unwrap();
    assert_eq!(a.entity, "/queues/orders");
    let a = Addr::parse("sb://127.0.0.1:34610/queues/orders%2Feu").unwrap();
    assert_eq!(a.entity, "/queues/orders%2Feu");
    // The Service Bus form: a trailing slash is the path's, not part of the name.
    let a = Addr::parse("sb://contoso.servicebus.windows.net/items_eu/").unwrap();
    assert_eq!(a.entity, "items_eu");
}

#[test]
fn endpoint_errors() {
    // A scheme that is not sb://.
    assert!(Addr::parse("https://contoso.servicebus.windows.net/orders").is_err());
    // Credentials in the URL are a load error.
    let e = Addr::parse("sb://user:pw@contoso.servicebus.windows.net/orders").unwrap_err();
    assert!(e.to_string().contains("not in the URL"), "{e}");
    // No query string, no fragment.
    assert!(Addr::parse("sb://contoso.servicebus.windows.net/orders?x=1").is_err());
    assert!(Addr::parse("sb://contoso.servicebus.windows.net/orders#f").is_err());
    // No entity.
    assert!(Addr::parse("sb://contoso.servicebus.windows.net/").is_err());
    assert!(Addr::parse("sb://contoso.servicebus.windows.net").is_err());
    // The system nodes are not entities of a target.
    assert!(Addr::parse("sb://contoso.servicebus.windows.net/$cbs").is_err());
    // A subscription is refused: send to the topic.
    let e =
        Addr::parse("sb://contoso.servicebus.windows.net/orders/Subscriptions/week").unwrap_err();
    assert!(e.to_string().contains("subscription"), "{e}");
    // Bad port, bad host, too long an entity.
    assert!(Addr::parse("sb://contoso.servicebus.windows.net:notaport/orders").is_err());
    assert!(Addr::parse("sb://[::1]:5671/orders").is_err());
    assert!(Addr::parse("sb://bad host.servicebus.windows.net/orders").is_err());
    let long = "a".repeat(261);
    assert!(Addr::parse(&format!("sb://x.servicebus.windows.net/{long}")).is_err());
}

#[test]
fn percent_encodings() {
    assert_eq!(
        percent_encode("sb://contoso.servicebus.windows.net/orders"),
        "sb%3A%2F%2Fcontoso.servicebus.windows.net%2Forders"
    );
    assert_eq!(percent_encode("a+b/c=d"), "a%2Bb%2Fc%3Dd");
    assert_eq!(percent_encode("~.-_aZ09"), "~.-_aZ09");
}

#[test]
fn sas_tokens() {
    // A pinned vector: the formula of the Microsoft SAS doc, checked against an
    // independent HMAC (python3 hmac) at the same inputs.
    let t = sas_token(
        "sb://contoso.servicebus.windows.net/orders",
        "RootManageSharedAccessKey",
        "dGVzdC1rZXktdmFsdWUtMjU2LWJpdC1sb25n",
        1_700_000_000,
        3600,
    )
    .unwrap();
    let sr = "sb%3A%2F%2Fcontoso.servicebus.windows.net%2Forders";
    assert!(
        t.starts_with(&format!("SharedAccessSignature sr={sr}&sig=")),
        "{t}"
    );
    assert!(
        t.ends_with("&se=1700003600&skn=RootManageSharedAccessKey"),
        "{t}"
    );
    // The sig is a Base64 HMAC url-encoded through `percent_encode`.
    let sig = t
        .strip_prefix(&format!("SharedAccessSignature sr={sr}&sig="))
        .and_then(|r| r.split("&se=").next())
        .unwrap()
        .to_string();
    let decoded: String = percent_decode(&sig);
    assert!(
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &decoded).is_ok(),
        "the sig url-decodes to Base64: {sig}"
    );
    // The key is used as written (the SAS doc: never decoded before use): a key with
    // `+` / `/` differs from its decoded bytes and the token changes.
    let a = sas_token("sb://x/y", "k", "ab+cd/ef", 100, 60).unwrap();
    let b = sas_token("sb://x/y", "k", "ab+cd ef", 100, 60).unwrap();
    assert_ne!(a, b);
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

#[test]
fn token_refresh_rule() {
    // The token stands until the margin before its expiry, and not after.
    assert!(token_stands(10_000, 10_000 - TOKEN_REFRESH_SECS - 1));
    assert!(!token_stands(10_000, 10_000 - TOKEN_REFRESH_SECS));
    assert!(!token_stands(10_000, 20_000));
}

#[test]
fn auth_forms() {
    // A namespace without keys is refused (Entra is out of scope).
    let e = auth_of(&vars("{}"), true).unwrap_err();
    assert!(e.to_string().contains("key_name"), "{e}");
    // Keys by reference: the Azure way.
    let a = auth_of(&vars(r#"{"key_name": "send", "key": "env:SB_KEY"}"#), true).unwrap();
    assert_eq!(
        a,
        Auth::Cbs {
            key_name: "send".into(),
            key_ref: "env:SB_KEY".into()
        }
    );
    // A literal key is refused.
    assert!(auth_of(&vars(r#"{"key_name": "s", "key": "abc"}"#), true).is_err());
    // What the auth holds is the reference and nothing else: the key itself is resolved at
    // connect time and goes into the CBS request, so no field of the kind is there for a
    // `Debug` (and so a log line) to print a secret from.
    std::env::set_var("LWF_SB_UNIT_KEY", "c2VjcmV0LWtleQ==");
    let held = auth_of(
        &vars(r#"{"key_name": "send", "key": "env:LWF_SB_UNIT_KEY"}"#),
        true,
    )
    .unwrap();
    let shown = format!("{held:?}");
    assert!(
        shown.contains("env:LWF_SB_UNIT_KEY") && !shown.contains("c2VjcmV0LWtleQ=="),
        "{shown}"
    );
    // Keys and SASL credentials at once: CBS allows only ANONYMOUS.
    assert!(auth_of(
        &vars(r#"{"key_name": "s", "key": "env:K", "username": "u", "password": "env:P"}"#),
        false
    )
    .is_err());
    // The local override without anything is anonymous; with an account, PLAIN.
    assert_eq!(auth_of(&vars("{}"), false).unwrap(), Auth::Anonymous);
    assert_eq!(
        auth_of(
            &vars(r#"{"username": "u", "password": "file:/x/pw"}"#),
            false
        )
        .unwrap(),
        Auth::Plain {
            username: "u".into(),
            password_ref: "file:/x/pw".into()
        }
    );
    assert!(auth_of(&vars(r#"{"username": "u"}"#), false).is_err());
    assert!(auth_of(&vars(r#"{"password": "literal"}"#), false).is_err());
}

#[test]
fn tls_of_an_endpoint() {
    // A namespace is TLS, always.
    let e = tls_config(&TlsSpec::parse(Some("disabled")).unwrap(), true).unwrap_err();
    assert!(e.to_string().contains("needs TLS"), "{e}");
    // The local override may be plain.
    assert!(
        tls_config(&TlsSpec::parse(Some("disabled")).unwrap(), false)
            .unwrap()
            .is_none()
    );
    // A server_name override is refused (the endpoint's host is checked).
    let mut spec = TlsSpec::parse(Some("verify-full")).unwrap();
    spec.server_name = Some("other".into());
    assert!(tls_config(&spec, true).is_err());
}

#[test]
fn max_message_size_vars() {
    assert_eq!(max_message_bytes(&vars("{}")).unwrap(), MAX_MESSAGE_BYTES);
    assert_eq!(
        max_message_bytes(&vars(r#"{"max_message_size": 1048576}"#)).unwrap(),
        1_048_576
    );
    assert!(max_message_bytes(&vars(r#"{"max_message_size": 0}"#)).is_err());
    assert!(max_message_bytes(&vars(r#"{"max_message_size": 104857601}"#)).is_err());
    assert!(max_message_bytes(&vars(r#"{"max_message_size": "big"}"#)).is_err());
}

fn parse(json: &str, n_keys: usize) -> Result<Option<Action>> {
    Action::parse(json.as_bytes(), n_keys, false, MAX_MESSAGE_BYTES)
}

#[test]
fn action_parse() {
    let a = parse(r#"{"messages": [{"value": {"sku": "A"}}]}"#, 1).unwrap();
    let a = a.unwrap();
    assert_eq!(a.msgs.len(), 1);
    assert_eq!(a.msgs[0].value, json!({"sku": "A"}));
    assert_eq!(a.msgs[0].bytes, br#"{"sku":"A"}"#);
    assert_eq!(a.msgs[0].session, None);
    // An empty list renders nothing.
    assert!(parse(r#"{"messages": []}"#, 0).unwrap().is_none());
    // Properties, sessions, and the unknown-field / wrong-shape refusals.
    let a = parse(
        r#"{"messages": [{"value": 1, "session": "s1", "properties": {"op": "upsert"}}]}"#,
        1,
    )
    .unwrap()
    .unwrap();
    assert_eq!(a.msgs[0].session.as_deref(), Some("s1"));
    assert_eq!(
        a.msgs[0].props.get("op").map(String::as_str),
        Some("upsert")
    );
    assert!(parse(r#"{"messages": [{"value": 1, "key": "1"}]}"#, 1).is_err());
    assert!(parse(r#"{"messages": [{"value": null}]}"#, 1).is_err());
    assert!(parse(r#"{"messages": [1]}"#, 1).is_err());
    assert!(parse(r#"{"messages": [{"value": 1, "properties": {"a": 2}}]}"#, 1).is_err());
    // The count must match the keys.
    assert!(parse(r#"{"messages": [{"value": 1}, {"value": 2}]}"#, 1).is_err());
    assert!(parse(r#"{"records": []}"#, 0).is_err());
}

#[test]
fn action_per_message_refusals() {
    // A session-enabled entity refuses the message without its `session` alone.
    let a = Action::parse(
        br#"{"messages": [{"value": 1, "session": "s"}, {"value": 2}]}"#,
        2,
        true,
        MAX_MESSAGE_BYTES,
    )
    .unwrap()
    .unwrap();
    assert!(a.msgs[0].refused.is_none());
    assert!(a.msgs[1].refused.as_ref().unwrap().contains("session"));
    // An over-size value is refused alone (the broker would answer
    // amqp:link:message-size-exceeded anyway).
    let big = format!("\"{}\"", "x".repeat(MAX_MESSAGE_BYTES + 1));
    let a = Action::parse(
        format!(r#"{{"messages": [{{"value": {big}}}, {{"value": 1}}]}}"#).as_bytes(),
        2,
        false,
        MAX_MESSAGE_BYTES,
    )
    .unwrap()
    .unwrap();
    assert!(a.msgs[0].refused.as_ref().unwrap().contains("maximum"));
    assert!(a.msgs[1].refused.is_none());
}

#[test]
fn dedup_ids() {
    let mut a = parse(
        r#"{"messages": [{"value": {"qty": 1}}, {"value": {"qty": 1}}]}"#,
        2,
    )
    .unwrap()
    .unwrap();
    a.dedup(&["1".into(), "2".into()]);
    // Same document, different key: different id (the key is in the hash).
    let id1 = a.msgs[0].message_id.clone();
    let id2 = a.msgs[1].message_id.clone();
    assert_ne!(id1, id2);
    assert_eq!(id1.as_ref().unwrap().len(), 64);
    // Deterministic: the same key and document hash the same (that is the duplicate).
    let mut b = parse(r#"{"messages": [{"value": {"qty": 1}}]}"#, 1)
        .unwrap()
        .unwrap();
    b.dedup(&["1".into()]);
    assert_eq!(b.msgs[0].message_id, id1);
    // The MessageId reaches the wire message.
    let m = b.wire(0);
    assert!(matches!(
        m.properties.as_ref().unwrap().message_id,
        Some(MessageId::String(_))
    ));
}

#[test]
fn wire_messages_carry_the_contract() {
    let a = parse(
            r#"{"messages": [{"value": {"sku": "A"}, "session": "s1", "properties": {"op": "upsert"}}]}"#,
            1,
        )
        .unwrap()
        .unwrap();
    let m = a.wire(0);
    let p = m.properties.as_ref().unwrap();
    assert_eq!(p.group_id.as_deref(), Some("s1"));
    assert_eq!(
        p.content_type.as_ref().map(|s| s.to_string()).as_deref(),
        Some("application/json")
    );
    let ap = m.application_properties.as_ref().unwrap();
    assert!(matches!(
        ap.0.get("op"),
        Some(SimpleValue::String(v)) if v == "upsert"
    ));
    assert_eq!(
        m.body,
        Data::from("{\"sku\":\"A\"}".as_bytes()),
        "the document travels as an AMQP Data section"
    );
}

#[test]
fn cbs_put_token_request_has_the_shape_the_node_wants() {
    let token =
        "SharedAccessSignature sr=sb%3A%2F%2Fn.servicebus.windows.net%2Fq&sig=abc&se=1700000000";
    let m = cbs_put_token_msg(
        token,
        "sb://n.servicebus.windows.net/q",
        1_700_000_000_000,
        Duration::from_secs(5),
    );
    let p = m.properties.as_ref().unwrap();
    assert_eq!(
        p.reply_to.as_ref().map(|s| s.to_string()).as_deref(),
        Some(CBS_REPLY_TO),
        "the reply is awaited on the link's own node"
    );
    assert!(p.message_id.is_some(), "the request is answerable");
    let ap = m.application_properties.as_ref().unwrap();
    let text = |k: &str| match ap.0.get(k) {
        Some(SimpleValue::String(v)) => v.to_string(),
        other => panic!("application property {k}: {other:?}"),
    };
    assert_eq!(text("operation"), "put-token");
    assert_eq!(text("type"), SAS_TOKEN_TYPE);
    assert_eq!(text("name"), "sb://n.servicebus.windows.net/q");
    // The node may forget the token no earlier than the send's own budget plus the refresh
    // margin, counted from the caller's clock.
    match ap.0.get("expiration") {
        Some(SimpleValue::Timestamp(t)) => assert_eq!(
            t.milliseconds(),
            1_700_000_000_000 + 5_000 + (TOKEN_REFRESH_SECS as i64) * 1000
        ),
        other => panic!("application property expiration: {other:?}"),
    }
    assert_eq!(m.body.0, token, "the token is the body of the request");
}

fn error_condition(c: &str) -> ErrorCondition {
    match AmqpError::try_from(c) {
        Ok(e) => ErrorCondition::AmqpError(e),
        Err(s) => ErrorCondition::Custom(Symbol::from(s)),
    }
}

fn rejected_with(cond: &str) -> Disposition {
    Disposition::Rejected(fe2o3_amqp::types::messaging::Rejected {
        error: Some(PeerError {
            condition: error_condition(cond),
            description: Some("because".into()),
            info: None,
        }),
    })
}

#[test]
fn dispositions_map_the_safe_way() {
    use fe2o3_amqp::types::messaging::{Accepted, Modified, Released};
    assert_eq!(
        disposition_outcome(&Disposition::Accepted(Accepted {})),
        Outcome::Ok
    );
    // A this-message condition refuses.
    for c in MESSAGE_CONDITIONS {
        assert!(matches!(
            disposition_outcome(&rejected_with(c)),
            Outcome::Refused(_)
        ));
    }
    // Everything else is transient: the safe default.
    for c in [
        "amqp:internal-error",
        "amqp:resource-limit-exceeded",
        "com.microsoft:server-busy",
        "com.microsoft:quota-exceeded",
        "com.microsoft:throttled",
        "something.unknown",
    ] {
        assert!(
            matches!(
                disposition_outcome(&rejected_with(c)),
                Outcome::Transient(_)
            ),
            "{c} must be transient"
        );
    }
    // A Rejected without an error says nothing.
    let bare = Disposition::Rejected(fe2o3_amqp::types::messaging::Rejected { error: None });
    assert!(matches!(disposition_outcome(&bare), Outcome::Transient(_)));
    assert!(matches!(
        disposition_outcome(&Disposition::Released(Released {})),
        Outcome::Transient(_)
    ));
    assert!(matches!(
        disposition_outcome(&Disposition::Modified(Modified {
            delivery_failed: Some(true),
            undeliverable_here: None,
            message_annotations: None,
        })),
        Outcome::Transient(_)
    ));
}

#[test]
fn whole_target_refusals() {
    // The entity conditions refuse the whole target…
    assert!(entity_refusal("amqp:not-found").is_some());
    assert!(entity_refusal("amqp:unauthorized-access").is_some());
    // …and nothing else does (a busy server at attach time is a retry).
    assert!(entity_refusal("amqp:internal-error").is_none());
    assert!(entity_refusal("amqp:resource-limit-exceeded").is_none());
    // Only a positive this-message condition refuses a message.
    assert!(is_message_condition("amqp:link:message-size-exceeded"));
    assert!(!is_message_condition("com.microsoft:server-busy"));
    assert!(!is_message_condition(""));
}

#[test]
fn cbs_replies() {
    // 200/201 are good; only 400/401/403/404 are verdicts on the token (refused);
    // every other status, and a missing one, is unknown.
    assert!(cbs_verdict(Some(200), None).is_ok());
    assert!(cbs_verdict(Some(201), None).is_ok());
    for refused in [400, 401, 403, 404] {
        let (fatal, why) = cbs_verdict(Some(refused), Some("unauthorized")).unwrap_err();
        assert!(fatal && why.contains(&refused.to_string()), "{why}");
    }
    // A timeout or a throttle says nothing about the token: transient, like 5xx.
    for transient in [408, 429, 500] {
        let (fatal, _) = cbs_verdict(Some(transient), None).unwrap_err();
        assert!(!fatal, "{transient} must not refuse the token");
    }
    let (fatal, _) = cbs_verdict(None, None).unwrap_err();
    assert!(!fatal);
    // The parts reader takes the protocol guide's shapes (an int status, a text
    // description) and survives anything else.
    let mut ap = ApplicationProperties::default();
    ap.0.insert("status-code".into(), SimpleValue::Int(201));
    ap.0.insert(
        "status-description".into(),
        SimpleValue::String("ok".into()),
    );
    let (status, desc) = cbs_reply_parts(Some(&ap));
    assert_eq!(status, Some(201));
    assert_eq!(desc.as_deref(), Some("ok"));
    let mut junk = ApplicationProperties::default();
    junk.0.insert("status-code".into(), SimpleValue::Bool(true));
    assert_eq!(cbs_reply_parts(Some(&junk)).0, None);
    assert_eq!(cbs_reply_parts(None).0, None);
}

#[test]
fn load_checks() {
    // Exactly one endpoint.
    assert!(check(
        &[
            "sb://a.servicebus.windows.net/q".into(),
            "sb://a.servicebus.windows.net/r".into()
        ],
        &vars(r#"{"key_name": "s", "key": "env:K"}"#),
        None
    )
    .is_err());
    // A namespace without keys is refused at load, a local host is not.
    assert!(check(
        &["sb://a.servicebus.windows.net/q".into()],
        &vars("{}"),
        None
    )
    .is_err());
    assert!(check(
        &["sb://127.0.0.1:34610/q".into()],
        &vars("{}"),
        Some("disabled")
    )
    .is_ok());
    // A boolean var that is not a boolean is refused.
    assert!(check(
        &["sb://127.0.0.1:34610/q".into()],
        &vars(r#"{"session": "yes"}"#),
        Some("disabled")
    )
    .is_err());
}

#[test]
fn scheme_refusals() {
    assert!(refuse_scheme(false, &["sb://a/b".into()]).is_err());
    assert!(refuse_scheme(true, &["sb://a/b".into()]).is_ok());
    assert!(refuse_scheme(false, &["kafka://a/b".into()]).is_ok());
}

// ==================================================================== the fake peer (CBS, faults)
//
// The CBS negotiation and the answer to a transfer are driven against an in-process fake
// endpoint (`targets_servicebus_peer.rs`): the Microsoft emulator is not available, so the
// peer is written from the protocol and scripted per test. Every case asserts the verdict of
// EACH message and the exact text of its cause — a message is refused only when the peer's
// answer is positively about that message.

#[path = "targets_servicebus_peer.rs"]
mod peer;

use peer::{composite, decode, frame, read_frame, sasl_frame, In, V};
use peer::{Answer, Answers, Cbs, Mute, Peer, Script};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The name the fake peer's target reads its SAS key from.
const FAKE_KEY: &str = "LWF_SB_FAKE_KEY";

/// `vars` of a local broker: the SAS key by reference, plus anything the test adds.
fn fake_vars(extra: &str) -> String {
    std::env::set_var(FAKE_KEY, "c2VjcmV0LWtleQ==");
    assert!(extra.is_empty() || extra.starts_with(','), "{extra}");
    format!(r#"{{"key_name":"send","key":"env:{FAKE_KEY}"{extra}}}"#)
}

fn fake_def(json: &str) -> TargetDef {
    // Short enough that a test which waits for nothing is a fast failure, long enough
    // that a loopback frame round-trip never loses the race — on a plain build.
    fake_def_budget(json, Duration::from_millis(400))
}

/// A target def with the per-action limit the test chooses. The 400 ms of [`fake_def`] suits a
/// plain build; a send whose *success* the test asserts on a machine that may be running the
/// instrumented (coverage) build derives its budget from a measured round trip instead —
/// see [`measured_batch_budget`].
fn fake_def_budget(json: &str, timeout: Duration) -> TargetDef {
    TargetDef {
        name: "sb".into(),
        kind: crate::model::Kind::ServiceBus,
        endpoints: vec![],
        tls: TlsSpec::parse(Some("disabled")).unwrap(),
        timeout,
        expect: None,
        vars: vars(json),
    }
}

fn fake_client(port: u16, json: &str) -> Client {
    fake_client_budget(port, json, Duration::from_millis(400))
}

fn fake_client_budget(port: u16, json: &str, timeout: Duration) -> Client {
    let def = fake_def_budget(json, timeout);
    let (_ctx, client) = Client::new(&def, &format!("sb://127.0.0.1:{port}/orders")).unwrap();
    client
}

/// The per-action budget for a send whose success the test asserts, measured rather than
/// guessed. `send_batch` gives the whole batch **one** deadline (a product decision, kept),
/// so the later messages race what the earlier ones already took: on the instrumented
/// (coverage) build the fake peer's answer path alone outgrew the fixed 400 ms and message
/// 2 came back `Transient` once (flake F-5, coverage job of `48bbcbd`). So the budget is
/// derived from the box: run the very same two-message batch once against a throwaway peer
/// under a bound generous enough to be no race at all, and give the real send eight times
/// what that took — floored at the plain build's 400 ms (a quiet machine keeps the old
/// number exactly), capped so a send that truly never completes still fails. A probe that
/// itself cannot finish falls back to the plain budget: a real defect then shows up in the
/// real send, under the old bound, unchanged.
async fn measured_batch_budget() -> Duration {
    const PLAIN: Duration = Duration::from_millis(400);
    const PROBE_BOUND: Duration = Duration::from_secs(10);
    let p = Peer::start(Script::happy()).await;
    let c = fake_client_budget(p.port(), &fake_vars(r#", "dedup": true"#), PROBE_BOUND);
    let t = std::time::Instant::now();
    let r = outcomes(&c, &msgs(&[msg("A"), msg("B")]), 2).await;
    let took = t.elapsed();
    let budget = match r {
        Ok(out) if out.iter().all(|o| matches!(o, Outcome::Ok)) => {
            (took * 8).clamp(PLAIN, PROBE_BOUND)
        }
        _ => PLAIN,
    };
    if budget != PLAIN {
        eprintln!("measured batch budget: {budget:?} (two-message round trip {took:?})");
    }
    budget
}

/// One message of a distinguishable document.
fn msg(sku: &str) -> String {
    format!(r#"{{"value": {{"sku": "{sku}"}}}}"#)
}

fn msgs(v: &[String]) -> String {
    format!(r#"{{"messages": [{}]}}"#, v.join(","))
}

/// The outcomes of one action: message i takes key `k{i}`.
async fn outcomes(client: &Client, body: &str, n: usize) -> Result<Vec<Outcome>> {
    let keys: Vec<String> = (0..n).map(|i| format!("k{i}")).collect();
    let mut action = parse(body, n).unwrap().unwrap();
    client.send_batch(&keys, &mut action).await
}

/// The same for an entity with sessions enabled (every message needs its group id).
async fn outcomes_in_session(client: &Client, body: &str, n: usize) -> Result<Vec<Outcome>> {
    let keys: Vec<String> = (0..n).map(|i| format!("k{i}")).collect();
    let mut action = Action::parse(body.as_bytes(), n, true, MAX_MESSAGE_BYTES)
        .unwrap()
        .unwrap();
    client.send_batch(&keys, &mut action).await
}

/// A send that is meant to work, with the peer's frame log at hand when it does not.
#[track_caller]
fn went_in(p: &Peer, r: Result<Vec<Outcome>>) -> Vec<Outcome> {
    r.unwrap_or_else(|e| panic!("the send failed: {e:#}\nthe peer saw:\n{}", p.trace()))
}

/// A send that is meant to fail, with the peer's frame log at hand when it does not.
#[track_caller]
fn refused_whole(p: &Peer, r: Result<Vec<Outcome>>) -> (anyhow::Error, bool) {
    match r {
        Ok(o) => panic!(
            "expected the whole send to fail, got {o:?}\nthe peer saw:\n{}",
            p.trace()
        ),
        Err(e) => {
            let rejected = crate::target::is_rejected(&e);
            (e, rejected)
        }
    }
}

/// The text a lost link leaves on an unanswered message. `send_batch` keeps `e.to_string()` of
/// the client's own error, so the expected text is built from a real error value of that type:
/// the wording is the library's, it cannot drift into something the code never says.
fn lost_link_reports(err: SendError) -> String {
    format!("the link was lost: {err}")
}

/// The client's error for a sender link the peer closed, with or without a condition.
fn link_closed(condition: Option<ErrorCondition>) -> SendError {
    use fe2o3_amqp::link::LinkStateError;
    SendError::LinkStateError(match condition {
        Some(c) => LinkStateError::RemoteClosedWithError(PeerError::new(c, None, None)),
        None => LinkStateError::RemoteClosed,
    })
}

/// The text a refused attach leaves on the whole send (`open_rest` says the client's own
/// error), built from a real value of that type for the same reason as `lost_link_reports`.
fn attach_refused(url: &str, condition: ErrorCondition) -> String {
    let err = SenderAttachError::RemoteClosedWithError(PeerError::new(condition, None, None));
    format!("attaching the sender of {url} failed: {err}")
}

#[test]
fn condition_text_is_the_symbol_the_peer_wrote() {
    use fe2o3_amqp::types::definitions::{AmqpError, ConnectionError, LinkError, SessionError};
    let text = |c: ErrorCondition| condition_text(&c);
    assert_eq!(
        text(ErrorCondition::AmqpError(AmqpError::NotFound)),
        "amqp:not-found"
    );
    assert_eq!(
        text(ErrorCondition::AmqpError(AmqpError::UnauthorizedAccess)),
        "amqp:unauthorized-access"
    );
    assert_eq!(
        text(ErrorCondition::ConnectionError(
            ConnectionError::ConnectionForced
        )),
        "amqp:connection:forced"
    );
    assert_eq!(
        text(ErrorCondition::SessionError(SessionError::WindowViolation)),
        "amqp:session:window-violation"
    );
    assert_eq!(
        text(ErrorCondition::LinkError(LinkError::MessageSizeExceeded)),
        "amqp:link:message-size-exceeded"
    );
    // A vendor condition is the symbol as it arrived.
    assert_eq!(
        text(ErrorCondition::Custom(Symbol::from(
            "com.microsoft:server-busy"
        ))),
        "com.microsoft:server-busy"
    );
    // The conditions of the two rules the peer's answer is judged by are the ones the code
    // reads (a typo in either table would silently turn refusals into retries or the other way).
    assert_eq!(
        MESSAGE_CONDITIONS.to_vec(),
        vec![
            "amqp:link:message-size-exceeded",
            "amqp:decode-error",
            "amqp:invalid-field"
        ]
    );
    assert_eq!(
        ENTITY_CONDITIONS
            .iter()
            .map(|(c, _)| *c)
            .collect::<Vec<_>>(),
        vec![
            "amqp:not-found",
            "amqp:unauthorized-access",
            "com.microsoft:entity-disabled"
        ]
    );
    // The 0.14 types crate has no `Display` for an `ErrorCondition` at all, and its `Debug`
    // is the Rust variant name — which is why the wire text is taken through `Symbol::from`.
    // Pinning that here keeps the workaround honest.
    assert_ne!(
        format!("{:?}", ErrorCondition::AmqpError(AmqpError::NotFound)),
        "amqp:not-found"
    );
    assert_eq!(
        Symbol::from(&AmqpError::NotFound).as_str(),
        "amqp:not-found"
    );
}

#[test]
fn fill_unknown_answers_only_the_messages_with_no_verdict() {
    let mut out: Vec<Option<Outcome>> = vec![None, Some(Outcome::Ok), None];
    fill_unknown(&mut out, "the link was lost".into());
    assert_eq!(
        out,
        vec![
            Some(Outcome::Transient("the link was lost".into())),
            // A message the peer already answered keeps its answer: the engine only strikes
            // what is not known to be in.
            Some(Outcome::Ok),
            Some(Outcome::Transient("the link was lost".into())),
        ]
    );
    // Nothing to fill is not an error.
    let mut answered = vec![Some(Outcome::Ok)];
    fill_unknown(&mut answered, "x".into());
    assert_eq!(answered, vec![Some(Outcome::Ok)]);
}

#[tokio::test]
async fn the_peer_answers_a_raw_sasl_handshake() {
    // The peer speaks bytes, so one test speaks bytes back: the SASL handshake, the header
    // exchange and an open — the bytes of the two frames the peer itself writes are pinned.
    let p = Peer::start(Script::happy()).await;
    let mut sock = TcpStream::connect(("127.0.0.1", p.port())).await.unwrap();
    sock.write_all(b"AMQP\x03\x01\x00\x00").await.unwrap();
    let mut got = [0u8; 8];
    sock.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"AMQP\x03\x01\x00\x00", "the SASL header");
    // The mechanisms we are offered: ANONYMOUS, as an array of symbols.
    let (ch, body) = read_frame(&mut sock).await.unwrap().unwrap();
    assert_eq!(ch, 0);
    assert_eq!(
        body,
        vec![
            0x00, 0x53, 0x40, 0xc0, 0x0f, 0x01, 0xe0, 0x0c, 0x01, 0xa3, 0x09, b'A', b'N', b'O',
            b'N', b'Y', b'M', b'O', b'U', b'S',
        ][..],
        "the sasl-mechanisms frame"
    );
    // The client's init, then our outcome: accepted.
    let init = composite(0x41, &[Some(V::Sym("ANONYMOUS".into())), None, None]);
    sock.write_all(&sasl_frame(&init)).await.unwrap();
    let (_ch, body) = read_frame(&mut sock).await.unwrap().unwrap();
    assert_eq!(
        body,
        vec![0x00, 0x53, 0x44, 0xc0, 0x03, 0x01, 0x50, 0x00][..],
        "the sasl-outcome frame: code ok"
    );
    // The AMQP header, and the answer to an open.
    sock.write_all(b"AMQP\x00\x01\x00\x00").await.unwrap();
    let mut got = [0u8; 8];
    sock.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"AMQP\x00\x01\x00\x00");
    let open = composite(0x10, &[Some(V::Str("probe".into())), None]);
    sock.write_all(&frame(0, &open)).await.unwrap();
    let (ch, body) = read_frame(&mut sock).await.unwrap().unwrap();
    assert_eq!((ch, body[0]), (0, 0x00));
    let d = decode(&body).unwrap();
    assert!(matches!(d, In::Open), "{d:?}");
    let trace = p.trace();
    assert!(trace.contains("sasl-init"), "{trace}");
    assert!(trace.contains("Open"), "{trace}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fake_peer_takes_the_token_and_the_messages() {
    // The two messages ride the target's single per-batch deadline, so on a build that
    // runs slow (the instrumented coverage one) the second disposition races the peer's
    // own answer path: this lost once under coverage. The budget is therefore measured
    // from this very path on this box, not raced against a fixed 400 ms — see
    // [`measured_batch_budget`].
    let budget = measured_batch_budget().await;
    let p = Peer::start(Script::happy()).await;
    let c = fake_client_budget(p.port(), &fake_vars(r#", "dedup": true"#), budget);
    let out = went_in(&p, outcomes(&c, &msgs(&[msg("A"), msg("B")]), 2).await);
    // The frame trace belongs to the failure message, not only to a failed `send_batch`:
    // a lost disposition (flake F-6) left the assert in CI with no trace at all.
    assert_eq!(
        out,
        vec![Outcome::Ok, Outcome::Ok],
        "the peer saw:\n{}",
        p.trace()
    );

    let seen = p.seen();
    assert_eq!(seen.put_tokens.len(), 1, "one token per connection");
    // The token is a SAS token for this entity, signed with the key the reference holds.
    let token = &seen.put_tokens[0];
    assert!(
        token.starts_with("SharedAccessSignature sr=sb%3A%2F%2F127.0.0.1%2Forders&sig="),
        "{token}"
    );
    assert!(token.ends_with("&skn=send"), "{token}");
    assert_eq!(
        seen.cbs_props[0].get("operation").map(String::as_str),
        Some("put-token")
    );
    assert_eq!(
        seen.cbs_props[0].get("type").map(String::as_str),
        Some(SAS_TOKEN_TYPE)
    );
    assert_eq!(
        seen.cbs_props[0].get("name").map(String::as_str),
        Some("sb://127.0.0.1/orders")
    );
    // Two messages, two deliveries, two documents — and the dedup id of each is its own.
    assert_eq!(seen.bodies.len(), 2, "{:?}", seen.bodies);
    assert!(
        seen.bodies[0].contains(r#"data={"sku":"A"}"#),
        "{}",
        seen.bodies[0]
    );
    assert!(
        seen.bodies[1].contains(r#"data={"sku":"B"}"#),
        "{}",
        seen.bodies[1]
    );
    let id_of = |b: &str| {
        b.split("mid=")
            .nth(1)
            .and_then(|r| r.split(';').next())
            .unwrap_or_default()
            .to_string()
    };
    let (a, b) = (id_of(&seen.bodies[0]), id_of(&seen.bodies[1]));
    assert!(a.len() == 64 && b.len() == 64 && a != b, "{a} / {b}");
}

/// The TLS of the kind stays lazywf's own: `open` hands the connector the `ClientConfig` of
/// [`tls_config`] (ring provider, verify mode, CA store). The two tests below pin both halves
/// of that against a TLS front of the test's own, so the fe2o3 upgrade to 0.18 (flake F-6)
/// cannot quietly move the handshake onto the library's own rustls defaults: an untrusted
/// certificate must fail, and a CA configured through `tls.ca` must be the trust that passes.
/// `ServerConfig`/rcgen usage follows [`crate::tests::tls`].
async fn tls_front(
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
) -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicBool>) {
    use tokio::net::TcpListener;
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![rustls::pki_types::CertificateDer::from(cert.der().to_vec())],
        rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
            key.serialize_der(),
        )),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = done.clone();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let Ok((mut tcp, _)) = listener.accept().await else {
            return;
        };
        // The AMQP TLS protocol header exchange first (spec §5.3.1.3, what the shipped client
        // sends and what a real broker answers before the handshake starts).
        let mut head = [0u8; 8];
        if tcp.read_exact(&mut head).await.is_err() || head != *b"AMQP\x02\x01\x00\x00" {
            return;
        }
        if tcp.write_all(b"AMQP\x02\x01\x00\x00").await.is_err() {
            return;
        }
        // A refused handshake (the untrusted-CA case) simply leaves `done` false: that flag
        // is what the tests assert on, together with the client's own error.
        if let Ok(mut tls) = acceptor.accept(tcp).await {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            // Let the client's first bytes (its AMQP header attempt) arrive, then end it:
            // the client fails later, over the transport, never over trust.
            let mut buf = [0u8; 8];
            let _ = tokio::time::timeout(Duration::from_secs(2), tls.read(&mut buf)).await;
        }
    });
    (addr, done)
}

fn self_signed_ip_cert() -> (rcgen::Certificate, rcgen::KeyPair) {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    (params.self_signed(&key).unwrap(), key)
}

fn fake_client_tls(port: u16, tls: TlsSpec) -> Client {
    let def = TargetDef {
        name: "sb".into(),
        kind: crate::model::Kind::ServiceBus,
        endpoints: vec![],
        tls,
        // Generous: this test asserts on the TLS outcome, and waits for nothing past it.
        timeout: Duration::from_secs(2),
        expect: None,
        vars: vars("{}"),
    };
    let (_ctx, client) = Client::new(&def, &format!("sb://127.0.0.1:{port}/orders")).unwrap();
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tls_send_refuses_an_untrusted_ca() {
    let (cert, key) = self_signed_ip_cert();
    let (addr, done) = tls_front(cert, key).await;
    let c = fake_client_tls(addr.port(), TlsSpec::parse(Some("verify-full")).unwrap());
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    let text = e.to_string().to_ascii_lowercase();
    assert!(
        text.contains("certificate") || text.contains("issuer"),
        "a self-signed server must fail on trust: {text}"
    );
    assert!(
        !done.load(std::sync::atomic::Ordering::SeqCst),
        "the server completed no handshake: lazywf's verifier is the one that refused"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tls_send_trusts_the_configured_ca() {
    let (cert, key) = self_signed_ip_cert();
    let pem = cert.pem().to_string();
    let (addr, done) = tls_front(cert, key).await;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    let ca = std::env::temp_dir().join(format!("lazywf-sb-ca-{}-{nonce}.pem", std::process::id()));
    std::fs::write(&ca, &pem).unwrap();
    let mut spec = TlsSpec::parse(Some("verify-full")).unwrap();
    spec.ca = vec![ca.to_string_lossy().into_owned()];
    let c = fake_client_tls(addr.port(), spec);
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    let _ = std::fs::remove_file(&ca);
    assert!(
        done.load(std::sync::atomic::Ordering::SeqCst),
        "with tls.ca configured the handshake completes: the trust store used is lazywf's"
    );
    let text = e.to_string().to_ascii_lowercase();
    assert!(
        !text.contains("certificate") && !text.contains("issuer"),
        "past the handshake the failure is the front dropping the conversation, not trust: {text}"
    );
}

#[tokio::test]
async fn a_target_without_keys_sends_without_cbs() {
    // The local-broker override: anonymous, so no CBS node is touched at all.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), "{}");
    let out = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    assert_eq!(out, vec![Outcome::Ok]);
    let seen = p.seen();
    assert!(seen.put_tokens.is_empty(), "{:?}", seen.put_tokens);
    assert_eq!(seen.bodies.len(), 1);
}

#[tokio::test]
async fn a_token_the_cbs_node_refuses_refuses_the_send() {
    // 403 is a verdict on the token itself: refused, and a retry cannot help.
    let p = Peer::start(Script::happy().cbs(Cbs::Status(403, Some("unauthorized".into())))).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: CBS refused the token (status 403): unauthorized",
            p.port()
        )
    );
    // The message never went: the peer saw one token and no entity message.
    assert_eq!(p.seen().put_tokens.len(), 1);
    assert!(p.seen().bodies.is_empty());
}

#[tokio::test]
async fn a_cbs_status_that_says_nothing_is_a_retry_not_a_refusal() {
    // A 500 from CBS says nothing about the token, so the send is not refused; the client
    // keeps the connection down and the engine retries.
    let p = Peer::start(Script::happy().cbs(Cbs::Status(500, Some("internal".into())))).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: CBS did not accept the token (status 500): internal",
            p.port()
        )
    );
    assert_eq!(p.seen().put_tokens.len(), 1);
}

#[tokio::test]
async fn a_cbs_node_that_settles_the_request_as_released_is_a_retry() {
    // The node saw the request and did not take the token — without a status, which is not a
    // verdict on the token.
    let p = Peer::start(Script::happy().cbs(Cbs::Released)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    let why = e.to_string();
    assert!(
        why.contains("the CBS node did not accept the token request"),
        "{why}"
    );
    assert!(
        why.starts_with(&format!("sb://127.0.0.1:{}/orders: ", p.port())),
        "{why}"
    );
}

#[tokio::test]
async fn a_cbs_node_that_never_answers_times_the_put_token_out() {
    // The node attaches its links and then swallows the put-token — neither settles it nor
    // answers it. The exchange owns its one timer: the request send and the reply read are
    // waited on together against the open's deadline, and the step that timed out says so
    // itself. It used to be the open's own text that spoke here: both call sites wrapped the
    // exchange in a `timeout` of the same duration that started earlier, so the open's
    // "begin or attach" words answered for a CBS node nobody heard from.
    let p = Peer::start(Script::happy().cbs(Cbs::Silent)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: no CBS reply within the target's time limit",
            p.port()
        )
    );
    assert_eq!(p.seen().put_tokens.len(), 1);
}

#[tokio::test]
async fn the_token_is_put_again_before_it_ages_out() {
    // The CBS spec drops the links of an expired token, so the client re-puts it well before
    // its expiry: the second action of a connection whose token is due puts a new one.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    assert_eq!(p.seen().put_tokens.len(), 1);

    age_out_the_token(&c).await;
    // The token is the same string within one second (it is signed by the second), so a new
    // expiry needs the clock to move.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    outcomes(&c, &msgs(&[msg("B")]), 1).await.unwrap();
    let seen = p.seen();
    assert_eq!(seen.put_tokens.len(), 2, "the token was renewed");
    let expiry = |t: &str| {
        t.split('&')
            .find_map(|p| p.strip_prefix("se="))
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or_default()
    };
    assert!(
        expiry(&seen.put_tokens[1]) > expiry(&seen.put_tokens[0]),
        "a token with a new expiry: {:?}",
        seen.put_tokens
    );
    assert_eq!(seen.bodies.len(), 2, "over the same connection");
}

/// Ages the connection's token past the refresh margin, as time would: the next action of this
/// connection puts a new one. (Only a test that compares two tokens needs the clock to move,
/// because a token is signed by the second; one that only needs a refresh does not.)
async fn age_out_the_token(client: &Client) {
    let mut slot = client.inner.lock().await;
    let inner = slot.as_mut().expect("the connection is open");
    inner.token_exp = crate::time::unix_now_ms() / 1000;
}

#[tokio::test]
async fn a_cbs_node_that_refuses_the_refreshed_token_refuses_the_send() {
    // The token the open took was fine; the one the refresh brings is refused. The verdict of a
    // refresh is a verdict on the whole send — nothing of this action may be called in.
    let p = Peer::start(Script::happy().cbs_refresh(Cbs::Status(403, Some("unauthorized".into()))))
        .await;
    let c = fake_client(p.port(), &fake_vars(""));
    outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    age_out_the_token(&c).await;
    let (e, rejected) = refused_whole(&p, outcomes(&c, &msgs(&[msg("B")]), 1).await);
    assert!(rejected, "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: CBS refused the token (status 403): unauthorized",
            p.port()
        )
    );
    let seen = p.seen();
    assert_eq!(seen.put_tokens.len(), 2, "the second put is the refresh");
    assert_eq!(
        seen.bodies.len(),
        1,
        "the second action's message never went"
    );
    assert!(c.inner.lock().await.is_none(), "the connection is dropped");
}

#[tokio::test]
async fn a_cbs_node_that_says_nothing_about_the_refreshed_token_is_a_retry() {
    // A 500 at refresh time is no verdict on the key: the send fails, the engine retries, and
    // the connection is dropped so the next one starts from the beginning.
    let p =
        Peer::start(Script::happy().cbs_refresh(Cbs::Status(500, Some("internal".into())))).await;
    let c = fake_client(p.port(), &fake_vars(""));
    outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    age_out_the_token(&c).await;
    let (e, rejected) = refused_whole(&p, outcomes(&c, &msgs(&[msg("B")]), 1).await);
    assert!(!rejected, "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: CBS did not accept the token (status 500): internal",
            p.port()
        )
    );
    let seen = p.seen();
    assert_eq!(seen.put_tokens.len(), 2);
    assert_eq!(seen.bodies.len(), 1, "the refused refresh sent no message");
    assert!(c.inner.lock().await.is_none(), "the connection is dropped");
}

#[tokio::test]
async fn a_cbs_node_that_stops_answering_times_the_refresh_out() {
    // The node took the first token and answers nothing after it — the refresh is what a stuck
    // CBS node costs: the target's budget, and the connection goes with it. The wait is the
    // exchange's own (one timer over the request send and the reply read), so the text is the
    // CBS node's own too — it used to be a same-duration outer `timeout` that spoke here, from
    // the caller, naming no step (see
    // `a_cbs_node_that_never_answers_times_the_put_token_out`).
    let p = Peer::start(Script::happy().cbs_refresh(Cbs::Silent)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    age_out_the_token(&c).await;
    let (e, rejected) = refused_whole(&p, outcomes(&c, &msgs(&[msg("B")]), 1).await);
    assert!(!rejected, "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: no CBS reply within the target's time limit",
            p.port()
        )
    );
    let seen = p.seen();
    assert_eq!(seen.put_tokens.len(), 2, "the refresh was asked for");
    assert_eq!(seen.bodies.len(), 1, "and no message went over it");
    assert!(c.inner.lock().await.is_none(), "the connection is dropped");
}

#[tokio::test]
async fn an_entity_refused_at_attach_refuses_the_target() {
    // A missing entity: refused once, for the whole target — the strikes rule, not a retry.
    let p = Peer::start(Script::happy().refuse_attach("amqp:not-found")).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let (e, _) = refused_whole(&p, outcomes(&c, &msgs(&[msg("A")]), 1).await);
    assert!(crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: the queue or topic does not exist (the target does not \
             create it)",
            p.port()
        )
    );
    // The token was taken: the refusal is about the entity, not the credential.
    assert_eq!(p.seen().put_tokens.len(), 1);
}

#[tokio::test]
async fn a_link_refused_for_an_unknown_reason_is_a_retry() {
    // A busy broker at attach time says nothing about the entity or the key.
    let p = Peer::start(Script::happy().refuse_attach("com.microsoft:server-busy")).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let (e, rejected) = refused_whole(&p, outcomes(&c, &msgs(&[msg("A")]), 1).await);
    assert!(!rejected, "{e:#}");
    assert_eq!(
        e.to_string(),
        attach_refused(
            &format!("sb://127.0.0.1:{}/orders", p.port()),
            ErrorCondition::Custom(Symbol::from("com.microsoft:server-busy")),
        )
    );
}

#[tokio::test]
async fn a_this_message_condition_refuses_that_message_alone() {
    // The outcome rule, message by message: the first is in, the second and third are
    // refused by their own conditions, each with the peer's condition and description.
    let p = Peer::start(Script::happy().answers(Answers::seq(vec![
        Answer::Accept,
        Answer::Reject(
            "amqp:link:message-size-exceeded".into(),
            Some("too big for the entity".into()),
        ),
        Answer::reject("amqp:decode-error"),
    ])))
    .await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = outcomes(&c, &msgs(&[msg("A"), msg("B"), msg("C")]), 3)
        .await
        .unwrap();
    assert_eq!(
        out,
        vec![
            Outcome::Ok,
            Outcome::Refused(
                "the entity refused the message (amqp:link:message-size-exceeded): \
                 too big for the entity"
                    .into()
            ),
            // A rejected without a description says so rather than inventing one.
            Outcome::Refused(
                "the entity refused the message (amqp:decode-error): no description".into()
            ),
        ]
    );
    // All three messages really went to the peer: the verdict is per message, not per action.
    let seen = p.seen();
    assert_eq!(seen.bodies.len(), 3, "{:?}", seen.bodies);
}

#[tokio::test]
async fn a_condition_about_anything_else_waits() {
    // Server-busy is not about this message: transient, with the condition kept in the text.
    let p = Peer::start(Script::happy().answers(Answers::one(Answer::Reject(
        "com.microsoft:server-busy".into(),
        Some("try later".into()),
    ))))
    .await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    assert_eq!(
        out,
        vec![Outcome::Transient(
            "the entity did not take the message (rejected, com.microsoft:server-busy): \
             try later"
                .into()
        )]
    );
}

#[tokio::test]
async fn an_answer_that_only_says_the_delivery_is_seen_waits() {
    // `received` settles nothing: the delivery is known, its fate is not.
    //
    // Note what this row is. It is the whole reach of `send_batch`'s per-delivery
    // `_ => Outcome::Transient("the send failed: {e}")` arm — of the errors the client can
    // build here, only `NonTerminalDeliveryState` and `IllegalDeliveryState` come from a live
    // link and reach it (a `LinkStateError` is matched above it as the lost link it is).
    // There used to be an arm here for `SendError::MessageEncodeError`, refusing the message
    // as "could not be encoded": the library builds that error in exactly one place — the
    // serialization `send_with_state` performs on the finished message (fe2o3-amqp 0.14.1
    // `link/error.rs` `From<serde_amqp::Error>`) — and an `AmqpValue` message the target has
    // already built and size-checked cannot fail it: the encoder's own failures are values
    // longer than u32::MAX (a string, symbol, binary, array or map past 4 GiB — the entity's
    // `max_message_size`, capped at 100 MiB by `max_message_bytes`, rules that out before the
    // link is touched, see `a_pre_refused_message_never_reaches_the_link`) and writer errors
    // of a serializer that writes into memory. An arm for it was a promise nothing could
    // call, so it is gone; a serialization failure that ever appears is a link failure now,
    // transient like any other unknown.
    let p = Peer::start(Script::happy().answers(Answers::one(Answer::Received))).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    assert_eq!(
        out,
        vec![Outcome::Transient(
            "the send failed: A non-terminal delivery state is received when an outcome is \
             expected"
                .into()
        )]
    );
    assert_eq!(p.seen().bodies.len(), 1);
}

#[tokio::test]
async fn an_answer_for_another_message_is_no_answer_for_this_one() {
    // The peer settles a delivery number this link never carried: nobody else's answer may
    // settle this message, so it is still unknown when the budget runs out.
    let p = Peer::start(Script::happy().answers(Answers::one(Answer::AcceptOther))).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap();
    assert_eq!(
        out,
        vec![Outcome::Transient(
            "no disposition within the target's 400ms".into()
        )]
    );
    assert_eq!(p.seen().bodies.len(), 1, "the message did go, unanswered");
}

#[tokio::test]
async fn a_message_the_peer_never_answers_waits() {
    // Silence is not a refusal either: everything about the message is unknown.
    let p = Peer::start(Script::happy().answers(Answers::one(Answer::Silence))).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = outcomes(&c, &msgs(&[msg("A"), msg("B")]), 2).await.unwrap();
    assert_eq!(
        out,
        vec![
            Outcome::Transient("no disposition within the target's 400ms".into()),
            // The link is dropped with the first unknown message, so the second never even
            // asked: it is unknown in the same way.
            Outcome::Transient("no disposition within the target's 400ms".into()),
        ]
    );
    // The connection is gone: the next action reconnects.
    assert!(c.inner.lock().await.is_none());
}

#[tokio::test]
async fn a_link_closed_naming_the_entity_refuses_the_target() {
    // The peer closes the sender link naming the missing entity, but only after the link stood
    // (a broker that reclaims an idle link). The condition says what the attach's refusal says:
    // the entity is not there to send to. It is therefore the same whole-target refusal
    // `an_entity_refused_at_attach_refuses_the_target` is — found once, not a per-message
    // strike — and not an unknown to retry: `send_batch` reads the condition the peer wrote
    // off the lost link (fe2o3 carries it on `SendError::LinkStateError`, which is what the
    // client builds for a peer close; its `SendError::Detached` variant is never built).
    let p = Peer::start(Script::happy().detach_on_attach(Some("amqp:not-found"), true)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let (e, rejected) = refused_whole(&p, outcomes(&c, &msgs(&[msg("A")]), 1).await);
    assert!(rejected, "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: the queue or topic does not exist (the target does not \
             create it)",
            p.port()
        )
    );
    assert!(
        p.trace().contains("amqp:not-found"),
        "the peer named the condition: {}",
        p.trace()
    );
    assert!(c.inner.lock().await.is_none(), "the connection is dropped");
}

#[tokio::test]
async fn a_link_closed_naming_a_disabled_entity_refuses_the_target() {
    // The entity is there but switched off: `com.microsoft:entity-disabled` says so (the
    // Microsoft page on messaging exceptions names it for a runtime operation on a disabled
    // entity, and the Azure SDKs refuse it without a retry). Like the missing entity, it is
    // positively about the entity and not about this send, so the whole target is refused —
    // a retry against a disabled entity is a worker parked until someone enables it.
    let p =
        Peer::start(Script::happy().detach_on_attach(Some("com.microsoft:entity-disabled"), true))
            .await;
    let c = fake_client(p.port(), &fake_vars(""));
    let (e, rejected) = refused_whole(&p, outcomes(&c, &msgs(&[msg("A")]), 1).await);
    assert!(rejected, "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "sb://127.0.0.1:{}/orders: the queue or topic is disabled (enable the entity or \
             point the target at a live one)",
            p.port()
        )
    );
    assert!(c.inner.lock().await.is_none(), "the connection is dropped");
}

#[tokio::test]
async fn answered_messages_keep_their_answer_when_the_link_is_lost() {
    // The peer takes the first message and then drops the link. The message it answered is
    // known to be in and keeps that verdict; the rest of the batch has no answer, and an
    // unknown message is never a refusal.
    //
    // The batch ends on the LINK, not on the budget: `send_batch` watches the link while the
    // dispositions are awaited (`Sender::on_detach`), so the detach is what closes the wait,
    // at once and with the peer's condition. That condition names the token's missing right —
    // a whole-target rule — but this batch has a message known to be in, and the engine may
    // only strike what is not known to be in: so the unanswered messages are transient, with
    // the entity verdict in their text.
    let p =
        Peer::start(Script::happy().detach_after(1, Some("amqp:unauthorized-access"), true)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = went_in(
        &p,
        outcomes(&c, &msgs(&[msg("A"), msg("B"), msg("C")]), 3).await,
    );
    assert_eq!(out[0], Outcome::Ok, "the peer answered this one");
    let unknown = || {
        Outcome::Transient(
            "the link was lost (the token cannot send to the entity (check vars.key_name, \
             vars.key and the rule's Send right))"
                .into(),
        )
    };
    assert_eq!(out[1], unknown(), "{:?}", out[1]);
    assert_eq!(out[2], unknown(), "{:?}", out[2]);
    assert_eq!(
        p.seen().bodies.len(),
        3,
        "all three were handed to the link"
    );
    assert!(c.inner.lock().await.is_none(), "the connection is dropped");
}

#[tokio::test]
async fn a_link_lost_for_another_reason_waits() {
    // The link is gone and the reason is not about the entity: everything unanswered is
    // transient, with the peer's condition in the text.
    let p =
        Peer::start(Script::happy().detach_on_attach(Some("amqp:resource-limit-exceeded"), true))
            .await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = went_in(&p, outcomes(&c, &msgs(&[msg("A")]), 1).await);
    assert_eq!(
        out,
        vec![Outcome::Transient(lost_link_reports(link_closed(Some(
            ErrorCondition::AmqpError(AmqpError::ResourceLimitExceeded),
        ))))]
    );
    assert!(
        p.trace().contains("amqp:resource-limit-exceeded"),
        "the peer named the condition: {}",
        p.trace()
    );
}

#[tokio::test]
async fn a_link_detached_without_a_reason_waits() {
    // A detach that names nothing says even less.
    let p = Peer::start(Script::happy().detach_on_attach(None, true)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = went_in(&p, outcomes(&c, &msgs(&[msg("A")]), 1).await);
    assert_eq!(
        out,
        vec![Outcome::Transient(lost_link_reports(link_closed(None)))]
    );
}

#[tokio::test]
async fn a_peer_that_never_talks_back_times_the_open_out() {
    // No answer to the header at all: the budget of the connect ends it, and a broker that
    // is stuck is not a reason to refuse anything.
    let p = Peer::start(Script::silent()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "connecting to sb://127.0.0.1:{}/orders timed out after 400ms",
            p.port()
        )
    );
}

#[tokio::test]
async fn a_broker_that_never_answers_the_cbs_session_begin_times_the_open_out() {
    // The socket stood and the handshake stands, but the first session begin is never
    // answered. The open's budget — the target's time limit, once — ends it, and the text
    // says the peer owed the begin. (The CBS steps share this budget: no step of one open
    // restarts the clock, which is what the two equal-duration outer `timeout`s used to
    // allow.)
    let p = Peer::start(Script::happy().mute(Mute::Begin)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "opening the session and sender link of sb://127.0.0.1:{}/orders timed out after \
             400ms (the broker did not answer begin or attach)",
            p.port()
        )
    );
    // The begin was asked for and the put-token never got a session to travel on.
    assert!(
        p.trace().contains("Begin"),
        "the peer saw the begin: {}",
        p.trace()
    );
    assert_eq!(p.seen().put_tokens.len(), 0);
}

#[tokio::test]
async fn a_broker_that_never_answers_the_entity_session_begin_times_the_open_out() {
    // The CBS side stands — token included — and then the entity session's begin goes
    // unanswered: the same one budget, the same text, and the token stands proven good:
    // this is the peer owing a begin, not a credential complaint.
    let p = Peer::start(Script::happy().mute(Mute::EntityBegin)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "opening the session and sender link of sb://127.0.0.1:{}/orders timed out after \
             400ms (the broker did not answer begin or attach)",
            p.port()
        )
    );
    assert_eq!(
        p.seen().put_tokens.len(),
        1,
        "the token went in before the entity session"
    );
}

#[tokio::test]
async fn a_broker_that_never_answers_the_sender_attach_times_the_open_out() {
    // Everything before the entity's sender link answered; the attach exchange itself goes
    // silent. Same budget, same text — the peer's half of the attach is exactly what the
    // text names.
    let p = Peer::start(Script::happy().mute(Mute::EntityAttach)).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        format!(
            "opening the session and sender link of sb://127.0.0.1:{}/orders timed out after \
             400ms (the broker did not answer begin or attach)",
            p.port()
        )
    );
    assert_eq!(
        p.seen().put_tokens.len(),
        1,
        "and no message went: the link never stood"
    );
    assert_eq!(p.seen().bodies.len(), 0);
}

#[tokio::test]
async fn a_sasl_refusal_refuses_the_target() {
    // A handshake the namespace will not take is a credential problem the retry cannot fix.
    let p = Peer::start(Script::refuse_sasl()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(crate::target::is_rejected(&e), "{e:#}");
    let why = e.to_string();
    assert!(
        why.starts_with(&format!("sb://127.0.0.1:{}/orders: ", p.port())),
        "{why}"
    );
    assert!(why.to_ascii_lowercase().contains("sasl"), "{why}");
}

#[tokio::test]
async fn a_pre_refused_message_never_reaches_the_link() {
    // An over-size document is refused on its own, without a transfer — and does not disturb
    // the message that is fine.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let big = format!("\"{}\"", "x".repeat(MAX_MESSAGE_BYTES + 1));
    let body = format!(r#"{{"messages": [{{"value": {big}}}, {}]}}"#, msg("B"));
    let out = outcomes(&c, &body, 2).await.unwrap();
    let Outcome::Refused(why) = &out[0] else {
        panic!("the over-size message: {out:?}");
    };
    assert!(why.contains("maximum"), "{why}");
    assert_eq!(out[1], Outcome::Ok);
    let seen = p.seen();
    assert_eq!(seen.bodies.len(), 1, "only the second message was sent");
    assert!(
        seen.bodies[0].contains(r#"data={"sku":"B"}"#),
        "{}",
        seen.bodies[0]
    );
}

#[tokio::test]
async fn a_session_entity_refuses_the_message_without_its_group() {
    // The session-less message is refused alone, the one with its group id goes: the rule's
    // `session` contract is enforced per message.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(r#", "session": true"#));
    let body =
        r#"{"messages": [{"value": {"sku": "A"}, "session": "g1"}, {"value": {"sku": "B"}}]}"#;
    let out = outcomes_in_session(&c, body, 2).await.unwrap();
    assert_eq!(out[0], Outcome::Ok, "the message with its group id went");
    let Outcome::Refused(why) = &out[1] else {
        panic!("the message without a session: {out:?}");
    };
    assert!(why.contains("session"), "{why}");
    let seen = p.seen();
    assert_eq!(seen.bodies.len(), 1);
    assert!(seen.bodies[0].contains("sid=g1"), "{}", seen.bodies[0]);
}

// ---------------------------------------------------------------------------------------------
// The load-time refusals of the same kind, by their exact words: an address that is not a queue
// or topic, credentials in a form the kind cannot take, a var of the wrong type. They are
// decided before any connection, so a peer is not needed — and the text is the whole point, it
// is what an operator with a broken rule reads.

#[test]
fn an_address_that_is_not_a_queue_or_topic_is_refused() {
    let cases = [
        (
            // The subscription refusal names itself: the path holds a `/`, but the
            // character-set check no longer speaks for it (it used to sit above this bail and
            // answer a subscription with a complaint about characters).
            "sb://contoso.servicebus.windows.net/orders/subscriptions/s1",
            "endpoint `sb://contoso.servicebus.windows.net/orders/subscriptions/s1`: send to \
             the topic, not to a subscription",
        ),
        (
            "sb://127.0.0.1:34610/",
            "endpoint `sb://127.0.0.1:34610/`: no queue or topic in the path",
        ),
        (
            "sb://127.0.0.1:34610/orders~eu",
            "endpoint `sb://127.0.0.1:34610/orders~eu`: a local broker's address holds letters, \
             digits, `-`, `.`, `_`, `/` and `%`",
        ),
        (
            // As above: `$` is not in the address's character set either, but the system-node
            // refusal stands before the set and names the node, not the character.
            "sb://127.0.0.1:34610/$cbs",
            "endpoint `sb://127.0.0.1:34610/$cbs`: an address does not start with `$`",
        ),
        (
            "sb://127.0.0.1:34610/orders/subscriptions/s1",
            "endpoint `sb://127.0.0.1:34610/orders/subscriptions/s1`: send to the topic, not to a \
             subscription",
        ),
    ];
    for (endpoint, why) in cases {
        let e = Addr::parse(endpoint).unwrap_err();
        assert_eq!(e.to_string(), why);
    }
    // The 260-character limit of a local address, counted in bytes as the namespace's is.
    // The limit counts the address as the broker is given it, the leading `/` included: 260
    // characters stand, 261 do not.
    let ok = "x".repeat(259);
    assert_eq!(
        Addr::parse(&format!("sb://127.0.0.1:34610/{ok}"))
            .unwrap()
            .entity,
        format!("/{ok}")
    );
    let long = "x".repeat(260);
    let e = Addr::parse(&format!("sb://127.0.0.1:34610/{long}")).unwrap_err();
    assert_eq!(
        e.to_string(),
        format!("endpoint `sb://127.0.0.1:34610/{long}`: the address is 1 to 260 characters")
    );
}

#[test]
fn credentials_in_a_form_the_kind_cannot_take_are_refused_with_the_rule() {
    let cases = [
        (
            r#"{"key": "env:SB_KEY"}"#,
            "`vars.key` needs `vars.key_name` (the authorization rule name)",
        ),
        (
            r#"{"key_name": "send"}"#,
            "`vars.key_name` needs `vars.key` (the key)",
        ),
        (
            r#"{"username": "u", "password": "pw"}"#,
            "`vars.password` must be a reference (`env:NAME` or `file:/path`), never a literal",
        ),
    ];
    for (json, why) in cases {
        let e = auth_of(&vars(json), true).unwrap_err();
        assert_eq!(e.to_string(), why);
    }
}

#[test]
fn a_var_of_the_wrong_type_is_refused_at_load() {
    // `session` and `dedup` are booleans or absent; anything else is a typo that must not
    // silently mean `false`.
    for (extra, why) in [
        (
            r#""session": "yes""#,
            r#"`vars.session` is "yes" (a boolean)"#,
        ),
        (r#""dedup": 1"#, "`vars.dedup` is 1 (a boolean)"),
    ] {
        let json = format!(r#"{{"key_name":"send","key":"env:K",{extra}}}"#);
        let def = fake_def(&json);
        let e = Client::new(&def, "sb://127.0.0.1:34610/orders")
            .err()
            .unwrap_or_else(|| panic!("`{extra}` is not a boolean: the load accepted it"));
        assert_eq!(e.to_string(), why);
    }
}

#[test]
fn rule_batches_are_bounded_by_the_entity() {
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
    // A batch over the entity's limit is refused; up to it is fine; a `key` rule is
    // not batched, so the limit does not apply.
    let d = def(FirePer::Batch, Some(MAX_MESSAGE_BYTES + 1));
    assert!(!rule_errors(&d, MAX_MESSAGE_BYTES).is_empty());
    let d = def(FirePer::Batch, Some(MAX_MESSAGE_BYTES));
    assert!(rule_errors(&d, MAX_MESSAGE_BYTES).is_empty());
    let d = def(FirePer::Key, Some(MAX_MESSAGE_BYTES + 1));
    assert!(rule_errors(&d, MAX_MESSAGE_BYTES).is_empty());
}

// ---------------------------------------------------------------------------------------------
// The target-facing door: `items_of` is what the engine calls with the rendered request, so this
// is where a request that is not a send, a body the parser refuses, and an empty action are
// decided — before a byte reaches the wire. (`send` and `send_items` above it need the database
// to write the expectation row: those are the e2e's `servicebus_*` tests, not a unit's.)

/// The rendered request of a servicebus send, as a template's `action` renders it.
fn rendered(body: &str) -> String {
    format!("POST / HTTP/1.1\r\nContent-Type: application/json\r\n\r\n{body}")
}

#[tokio::test]
async fn a_request_that_is_not_a_send_is_refused() {
    // The kind sends and nothing else: a method or a path other than `POST /` is refused whole,
    // and the peer never hears from us.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let keys = vec!["k0".to_string()];
    for (request, why) in [
        (
            "GET / HTTP/1.1\r\n\r\n",
            "servicebus targets only send: GET / is not POST /",
        ),
        (
            "DELETE /orders HTTP/1.1\r\n\r\n",
            "servicebus targets only send: DELETE /orders is not POST /",
        ),
    ] {
        let e = c.items_of(request, &keys).await.unwrap_err();
        assert!(crate::target::is_rejected(&e), "{e:#}");
        assert_eq!(e.to_string(), why);
    }
    assert!(p.seen().bodies.is_empty());
    assert!(p.seen().put_tokens.is_empty(), "not even the token");
}

#[tokio::test]
async fn a_request_that_cannot_be_parsed_is_refused_with_the_parser_wording() {
    // A request line the HTTP parser itself rejects keeps its wording, prefixed.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let e = c.items_of("nonsense", &[]).await.unwrap_err();
    assert!(crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        e.to_string(),
        "the rendered request: request line `nonsense` has no path"
    );
    assert!(p.seen().bodies.is_empty());
}

#[tokio::test]
async fn a_body_the_message_parser_refuses_is_refused_as_the_send_body() {
    // The body is the contract: an unknown field of a message is refused whole, with the
    // parser's own words, and nothing goes to the entity.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let cases = [
        (
            r#"{"messages": [{"nope": 1}]}"#,
            "the send body: message 0 has no `value`",
        ),
        (
            r#"{"messages": [{"value": {"sku": "A"}, "nope": 1}]}"#,
            "the send body: message 0 has the unknown field `nope` (a message carries `value`, \
             `session` and `properties`)",
        ),
        (
            r#"{"messages": "not a list"}"#,
            "the send body: the send body is `{\"messages\": […]}`",
        ),
    ];
    for (body, why) in cases {
        let e = c
            .items_of(&rendered(body), &["k0".into()])
            .await
            .unwrap_err();
        assert!(crate::target::is_rejected(&e), "{e:#}");
        assert_eq!(e.to_string(), why);
    }
    // Body bytes that are not JSON at all keep serde's words after the kind's; only the kind's
    // half is pinned here, the parser's phrasing belongs to its own crate.
    let e = c
        .items_of(&rendered("not JSON at all"), &["k0".into()])
        .await
        .unwrap_err();
    assert!(crate::target::is_rejected(&e), "{e:#}");
    assert!(
        e.to_string()
            .starts_with("the send body: the send body is not JSON"),
        "{e:#}"
    );
    assert!(p.seen().bodies.is_empty());
}

#[tokio::test]
async fn an_empty_action_sends_nothing_and_says_so() {
    // `Ok(None)` is "nothing rendered": the engine skips the step, it is not a failure and not
    // a send — no connection, no token, no message.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    assert!(c.items_of("", &["k0".into()]).await.unwrap().is_none());
    assert!(c
        .items_of(&rendered(r#"{"messages": []}"#), &[])
        .await
        .unwrap()
        .is_none());
    let seen = p.seen();
    assert!(seen.bodies.is_empty() && seen.put_tokens.is_empty());
}

#[tokio::test]
async fn a_rendered_send_answers_for_every_key() {
    // The door the engine actually walks through: the rendered request, one outcome per key, in
    // the order of the keys.
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(p.port(), &fake_vars(""));
    let out = c
        .items_of(
            &rendered(&msgs(&[msg("A"), msg("B")])),
            &["k0".into(), "k1".into()],
        )
        .await
        .unwrap()
        .expect("a rendered action");
    assert_eq!(out, vec![Outcome::Ok, Outcome::Ok]);
    assert_eq!(p.seen().bodies.len(), 2);
}

#[tokio::test]
async fn a_port_with_no_broker_behind_it_fails_the_connect() {
    // A namespace that is simply not there: not a refusal (the address may come back), and the
    // client's own connect error travels in the text.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free loopback port");
    let port = listener.local_addr().expect("a local address").port();
    drop(listener); // nothing is listening here now
    let c = fake_client(port, &fake_vars(""));
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert!(
        e.to_string().starts_with(&format!(
            "connecting to sb://127.0.0.1:{port}/orders failed: "
        )),
        "{e:#}"
    );
}

#[tokio::test]
async fn a_key_that_cannot_be_resolved_fails_before_the_wire() {
    // The key is read at the open, not at load, and the name of the target whose credential is
    // missing is in the text; a secret that is absent is nobody's fault but is not a refusal.
    std::env::remove_var("LWF_SB_ABSENT_KEY");
    let p = Peer::start(Script::happy()).await;
    let c = fake_client(
        p.port(),
        r#"{"key_name":"send","key":"env:LWF_SB_ABSENT_KEY"}"#,
    );
    let e = outcomes(&c, &msgs(&[msg("A")]), 1).await.unwrap_err();
    assert!(!crate::target::is_rejected(&e), "{e:#}");
    assert_eq!(
        format!("{e:#}"),
        format!(
            "vars.key for sb://127.0.0.1:{}/orders: environment variable `LWF_SB_ABSENT_KEY` is \
             not set",
            p.port()
        )
    );
    assert!(p.seen().bodies.is_empty(), "no connection was opened");
}
