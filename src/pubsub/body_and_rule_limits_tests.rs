use super::*;
use crate::model::{FirePer, RawRule, RuleDef};

fn message(key: &str, extra: Value) -> Value {
    let mut m = json!({"data": "e30=", "attributes": {"key": key, "op": "upsert"}});
    if let Value::Object(o) = &mut m {
        if let Value::Object(add) = extra {
            for (k, v) in add {
                o.insert(k, v);
            }
        }
    }
    m
}

/// The 10 MB of one publish is a limit of the service, not of lazywf: a body that
/// goes over it is refused by Google the same way every time, so the whole batch's
/// keys would take a strike together. The check has to fire on the byte count
/// before anything is parsed.
#[test]
fn a_publish_over_the_request_limit_is_refused_by_size() {
    let just_fits = "A".repeat(MAX_REQUEST_BYTES);
    // Exactly the limit is not over it (the message would then be a JSON parse
    // error, not a size refusal).
    assert!(check_body(just_fits.as_bytes())
        .err()
        .is_some_and(|e| !e.contains("bytes, over")));
    let over = "A".repeat(MAX_REQUEST_BYTES + 1);
    let e = check_body(over.as_bytes()).err().unwrap();
    assert!(
        e.contains(&format!("{}", MAX_REQUEST_BYTES + 1))
            && e.contains(&format!("{MAX_REQUEST_BYTES}")),
        "{e}"
    );
}

/// An `orderingKey` that is not a string is refused: with ordered delivery the key
/// is what puts two writes of one row in order, and a number or an object would be
/// refused by the service or, worse, silently order nothing.
#[test]
fn an_ordering_key_must_be_a_string() {
    for bad in [
        json!(7),
        json!(true),
        json!(null),
        json!({"k": 1}),
        json!([]),
    ] {
        let body = json!({"messages": [message("1", json!({"orderingKey": bad}))]}).to_string();
        let e = check_body(body.as_bytes())
            .err()
            .unwrap_or_else(|| panic!("an orderingKey of {bad} was accepted"));
        assert!(e.contains("`orderingKey` is not a string"), "{bad}: {e}");
    }
    // A string is kept, in the order of the messages: it is what the ordered
    // publisher groups by.
    let body = json!({"messages": [
        message("1", json!({"orderingKey": "1"})),
        message("2", json!({"orderingKey": "2"})),
        message("3", json!({"orderingKey": "1"}))
    ]})
    .to_string();
    let p = check_body(body.as_bytes()).unwrap();
    assert_eq!(
        p.ordering_keys,
        vec!["1".to_string(), "2".to_string(), "1".to_string()]
    );
}

fn rule(fire_per: FirePer, keys: Option<u64>, bytes: Option<u64>, steps: Option<&str>) -> RuleDef {
    RuleDef::from_row(&RawRule {
        name: "orders".into(),
        enabled: true,
        target: Some("events".into()),
        template: Some("POST /x".into()),
        fire_per: Some(
            match fire_per {
                FirePer::Batch => "batch",
                FirePer::Key => "key",
            }
            .to_string(),
        ),
        batch_max_keys: keys,
        batch_max_bytes: bytes,
        reset_steps: steps.map(str::to_string),
        ..Default::default()
    })
    .unwrap()
}

/// A batch publish takes at most 1000 messages, and the worker only knows that
/// from the rule: a rule that leaves `batch_max_keys` unset would let the batch
/// grow past it, the service would refuse the whole publish, and every key of it
/// would be struck.
#[test]
fn a_batch_rule_must_stay_inside_one_publish() {
    let errs = rule_errors(&rule(FirePer::Batch, None, None, None));
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(errs[0].contains("batch_max_keys"), "{errs:?}");

    let errs = rule_errors(&rule(
        FirePer::Batch,
        Some(MAX_MESSAGES as u64 + 1),
        None,
        None,
    ));
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(errs[0].contains("is over"), "{errs:?}");

    // Inside both limits: nothing to say.
    assert!(rule_errors(&rule(
        FirePer::Batch,
        Some(MAX_MESSAGES as u64),
        Some(MAX_REQUEST_BYTES as u64),
        None
    ))
    .is_empty());
    // The byte limit of the rule may not exceed what a publish takes.
    let errs = rule_errors(&rule(
        FirePer::Batch,
        Some(10),
        Some(MAX_REQUEST_BYTES as u64 + 1),
        None,
    ));
    assert!(
        errs.iter().any(|e| e.contains("batch_max_bytes")),
        "{errs:?}"
    );
    // A `key` rule fires one record per action: the batch columns do not apply.
    assert!(rule_errors(&rule(FirePer::Key, None, None, None)).is_empty());
}

/// A reset step of a pubsub target may only publish: a seek, a detach, a delete or
/// a purge would be refused by the service after the reset already reported the
/// keys gone, which is how a key gets lost.
#[test]
fn a_reset_step_of_a_pubsub_target_may_only_publish() {
    for bad in [
        r#"[{"once": true, "template": "POST /v1/projects/p/subscriptions/s:seek"}]"#,
        r#"[{"once": true, "template": "POST /v1/projects/p/subscriptions/s:detach"}]"#,
        r#"[{"once": true, "template": "DELETE /v1/projects/p/topics/t"}]"#,
        r#"[{"once": true, "template": "POST /v1/projects/p/subscriptions/s:purge"}]"#,
        r#"[{"once": true, "template": "POST /v1/projects/p/subscriptions/s"}]"#,
        // A publish to something that is not this target's topic.
        r#"[{"once": true, "template": "POST /v1/projects/p/topics/other"}]"#,
    ] {
        let errs = rule_errors(&rule(FirePer::Key, None, None, Some(bad)));
        assert!(
            errs.iter().any(|e| e.contains("only publish")),
            "{bad} was accepted: {errs:?}"
        );
    }
    assert!(rule_errors(&rule(
        FirePer::Key,
        None,
        None,
        Some(r#"[{"once": true, "template": "POST /v1/projects/p/topics/t:publish"}]"#)
    ))
    .is_empty());
}
