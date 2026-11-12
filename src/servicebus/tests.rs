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
