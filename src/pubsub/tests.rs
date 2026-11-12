use super::*;

fn check_url(url: &str) -> Result<Addr> {
    Addr::parse(url)
}

#[test]
fn endpoint_parses_project_topic_and_the_emulator_address() {
    let a = check_url("gc-pubsub://lwf-test/orders").unwrap();
    assert_eq!(
        (a.project.as_str(), a.topic.as_str()),
        ("lwf-test", "orders")
    );
    assert_eq!(a.endpoint, None);
    assert_eq!(
        a.publish_path(),
        "/v1/projects/lwf-test/topics/orders:publish"
    );
    let e = check_url("gc-pubsub://p/t?endpoint=http://127.0.0.1:34563/").unwrap();
    assert_eq!(e.endpoint.as_deref(), Some("http://127.0.0.1:34563"));
}

#[test]
fn endpoint_refusals() {
    for bad in [
        "gc-pubsub://p",                       // no topic
        "gc-pubsub://p/t/extra",               // extra segment
        "gc-pubsub:///t",                      // empty project
        "gc-pubsub://u:pw@p/t",                // userinfo
        "gc-pubsub://p/t?endpoint=http://u@h", // userinfo in the address
        "gc-pubsub://p/t?unknown=1",           // unknown query key
        "gc-pubsub://p/t?endpoint=ftp://h",    // not http(s)
        "gc-pubsub://p/t?endpoint",            // no value
        "gc-pubsub://p/a%20b",                 // characters a path must not carry
        "redis://p/t",                         // another scheme
    ] {
        assert!(Addr::parse(bad).is_err(), "accepted {bad}");
    }
}

#[test]
fn credentials_and_the_scheme_are_checked_at_load() {
    let ok = vec!["gc-pubsub://p/t".to_string()];
    let mut vars = Map::new();
    assert!(check(&vars, &ok).is_ok());
    vars.insert("credentials_ref".into(), json!("env:GOOGLE_KEY"));
    vars.insert("ordering".into(), json!(true));
    assert!(check(&vars, &ok).is_ok());
    vars.insert(
        "credentials_ref".into(),
        json!("{\"private_key\":\"literal\"}"),
    );
    assert!(check(&vars, &ok).is_err(), "a literal key was accepted");
    vars.insert("credentials_ref".into(), json!("env:GOOGLE_KEY"));
    vars.insert("ordering".into(), json!("yes"));
    assert!(check(&vars, &ok).is_err());
    assert!(check(&Map::new(), &[]).is_err());
    assert!(check(&Map::new(), &["http://h".into()]).is_err());
    assert!(refuse_scheme(false, &ok).is_err());
    assert!(refuse_scheme(true, &ok).is_ok());
}

#[test]
fn publish_body_checks() {
    let good = json!({"messages": [
        {"data": "eyJpZCI6MX0=", "attributes": {"key": "1", "op": "upsert"}, "orderingKey": "1"},
        {"data": "e30=", "attributes": {"key": "2", "op": "delete"}}
    ]})
    .to_string();
    let p = check_body(good.as_bytes()).unwrap();
    assert_eq!(p.messages, 2);
    assert_eq!(p.ordering_keys, vec!["1".to_string()]);
    let no_key =
        json!({"messages": [{"data": "e30=", "attributes": {"op": "upsert"}}]}).to_string();
    assert!(check_body(no_key.as_bytes()).is_err());
    let not_b64 = json!({"messages": [{"data": "!!", "attributes": {"key": "1", "op": "upsert"}}]})
        .to_string();
    assert!(check_body(not_b64.as_bytes()).is_err());
    let too_many: Vec<Value> = (0..=MAX_MESSAGES)
        .map(|i| json!({"data": "e30=", "attributes": {"key": i.to_string(), "op": "upsert"}}))
        .collect();
    assert!(check_body(json!({ "messages": too_many }).to_string().as_bytes()).is_err());
    let big = "A".repeat(MAX_REQUEST_BYTES);
    assert!(check_body(big.as_bytes()).is_err());
    assert!(check_body(b"not json").is_err());
}

#[test]
fn outcome_mapping() {
    // 400 INVALID_ARGUMENT is the only refusal.
    let inv = br#"{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"too big"}}"#;
    assert!(refused(400, inv));
    let pre = br#"{"error":{"code":400,"status":"FAILED_PRECONDITION"}}"#;
    assert!(!refused(400, pre));
    assert!(!refused(400, br#"{"error":{"status":"OUT_OF_RANGE"}}"#));
    assert!(refused(400, b""), "a 400 without an envelope is a refusal");
    for s in [429, 500, 502, 503, 504, 401, 403, 404, 408] {
        assert!(!refused(s, inv), "status {s} must be transient");
    }
    // A short answer fails closed.
    assert!(short_answer(br#"{"messageIds":["1"]}"#, 2));
    assert!(!short_answer(br#"{"messageIds":["1","2"]}"#, 2));
    assert!(short_answer(b"{}", 1));
}

/// The rendered request of the e2e template: the path, the body, and the checks pass.
#[test]
fn rendered_request_passes_the_checks() {
    let tpl = "POST /v1/projects/{{endpoint.project}}/topics/{{endpoint.topic}}:publish HTTP/1.1\nContent-Type: application/json\n\n{\"messages\": [{{#each docs}}{\"data\": {{json (base64 (json doc))}}, \"attributes\": {\"key\": {{json key}}, \"op\": {{json op}}}{{#if @root.target.vars.ordering}}, \"orderingKey\": {{json key}}{{/if}}}{{#unless @last}}, {{/unless}}{{/each}}]}";
    let ctx = json!({
        "endpoint": {"project": "p", "topic": "t"},
        "target": {"vars": {"ordering": true}},
        "docs": [{"key": "1", "op": "upsert", "doc": {"id": 1}}, {"key": "2", "op": "delete", "doc": {"id": 2}}]
    });
    let mut t = crate::template::Templates::new();
    t.add("t", tpl, crate::template::ACTION_ROOTS).unwrap();
    let text = t.render("t", &ctx).unwrap();
    let req = RawRequest::parse(&text).unwrap().unwrap();
    assert_eq!(req.target, "/v1/projects/p/topics/t:publish");
    let p = check_body(&req.body).unwrap();
    assert_eq!(p.messages, 2);
    assert_eq!(p.ordering_keys, vec!["1", "2"]);
}
