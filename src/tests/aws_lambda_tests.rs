use super::*;
use crate::targets::lambda::{function_error, lambda_failed_items, Invocation, LambdaUrl};

fn lam(url: &str) -> Result<LambdaUrl> {
    LambdaUrl::parse(url)
}

#[test]
fn lambda_url_forms() {
    let u = lam("aws-lambda://orders").unwrap();
    assert_eq!(u.region, None);
    assert_eq!(u.function, "orders");
    assert_eq!(u.qualifier, None);
    assert_eq!(u.invocation, Invocation::Sync);
    assert_eq!(u.endpoint, None);
    assert_eq!(u.path(), "/2015-03-31/functions/orders/invocations");

    let u = lam("aws-lambda://eu-west-1/orders:live?invocation=event").unwrap();
    assert_eq!(u.region.as_deref(), Some("eu-west-1"));
    assert_eq!(u.qualifier.as_deref(), Some("live"));
    assert_eq!(u.invocation, Invocation::Event);
    assert_eq!(
        u.path(),
        "/2015-03-31/functions/orders/invocations?Qualifier=live"
    );

    let u = lam("aws-lambda://orders:$LATEST").unwrap();
    assert_eq!(u.qualifier.as_deref(), Some("$LATEST"));
    assert_eq!(
        u.path(),
        "/2015-03-31/functions/orders/invocations?Qualifier=$LATEST"
    );

    let u =
        lam("aws-lambda://us-east-1/orders?endpoint=http://127.0.0.1:9&invocation=sync").unwrap();
    assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:9"));
    assert_eq!(u.invocation, Invocation::Sync);
    let u =
        lam("aws-lambda://us-east-1/orders?invocation=sync&endpoint=http://127.0.0.1:9").unwrap();
    assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:9"));
}

#[test]
fn lambda_refuses_bad_urls() {
    for bad in [
        "https://example.com/orders",
        "aws-sqs://123456789012/orders",
        "aws-kinesis://orders",
        "aws-lambda://user@orders",
        "aws-lambda://",
        "aws-lambda://a/b/c",
        "aws-lambda://Eu/orders",
        "aws-lambda://orders:",
        "aws-lambda://orders:bad*qualifier",
        "aws-lambda://bad name",
        "aws-lambda://arn:aws:lambda:us-east-1:123456789012:function:orders",
        "aws-lambda://orders?invocation=batch",
        "aws-lambda://orders?invocation=sync&invocation=event",
        "aws-lambda://orders?region=eu-west-1",
        "aws-lambda://orders?endpoint=http://127.0.0.1:9/path",
        "aws-lambda://orders?endpoint=http://user@127.0.0.1:9",
    ] {
        assert!(lam(bad).is_err(), "accepted `{bad}`");
    }
    let e = lam("aws-sqs://123456789012/orders")
        .unwrap_err()
        .to_string();
    assert!(e.contains("aws-lambda://"), "{e}");
    let e = lam("https://example.com/orders").unwrap_err().to_string();
    assert!(e.contains("aws-lambda://"), "{e}");
}

#[test]
fn lambda_payload_limits_are_the_documented_ones() {
    assert_eq!(
        lam("aws-lambda://orders").unwrap().payload_limit(),
        6_291_456
    );
    assert_eq!(
        lam("aws-lambda://orders?invocation=event")
            .unwrap()
            .payload_limit(),
        1_048_576
    );
}

#[test]
fn function_error_header_is_read_case_insensitively() {
    let h = |n: &str, v: &str| vec![(n.to_string(), v.to_string())];
    assert_eq!(
        function_error(&h("X-Amz-Function-Error", "Unhandled")).as_deref(),
        Some("Unhandled")
    );
    assert_eq!(
        function_error(&h("x-amz-function-error", "Handled")).as_deref(),
        Some("Handled")
    );
    assert_eq!(function_error(&h("X-Amz-Log-Result", "x")), None);
}

#[test]
fn batch_item_failures_list_the_failed_keys() {
    assert_eq!(lambda_failed_items(b"{}").unwrap(), Vec::<String>::new());
    assert_eq!(lambda_failed_items(b"[1,2]").unwrap(), Vec::<String>::new());
    assert_eq!(
        lambda_failed_items(b"not json").unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        lambda_failed_items(br#"{"batchItemFailures":[]}"#).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        lambda_failed_items(
            br#"{"batchItemFailures":[{"itemIdentifier":"2"},{"itemIdentifier":"7"}]}"#
        )
        .unwrap(),
        vec!["2".to_string(), "7".to_string()]
    );
    assert!(lambda_failed_items(br#"{"batchItemFailures":"2"}"#).is_err());
    assert!(lambda_failed_items(br#"{"batchItemFailures":[{"id":"2"}]}"#).is_err());
}

use serde_json::{json, Map};

use crate::model::{Kind, RawTarget, RuleDef, TargetDef};
use crate::target::Target;
use crate::targets::lambda::{build, check, rule_errors};

fn raw(kind: &str, endpoints: &[&str], vars: &str) -> RawTarget {
    RawTarget {
        name: "t".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoints.join("\n")),
        vars: Some(vars.into()),
        ..Default::default()
    }
}

/// The load check of the kind: the function, the region and the invocation are all in the
/// one endpoint, so two endpoints is not a target with a choice — it is a target that cannot
/// say which function it fires. `from_row` refuses it first; the check is what says why.
#[test]
fn the_lambda_kind_takes_exactly_one_endpoint() {
    for endpoints in [
        vec![],
        vec![
            "aws-lambda://eu-west-1/a".into(),
            "aws-lambda://eu-west-1/b".into(),
        ],
    ] {
        let e = format!(
            "{:#}",
            check(&endpoints, &Map::new(), Kind::Lambda).unwrap_err()
        );
        assert!(
            e.contains("kind lambda takes exactly one endpoint"),
            "{endpoints:?}: {e}"
        );
        assert!(e.contains("aws-lambda://[<region>/]"), "{endpoints:?}: {e}");
    }
    // One endpoint, and the kind has nothing to say.
    assert!(check(
        &["aws-lambda://eu-west-1/orders:live".into()],
        &Map::new(),
        Kind::Lambda
    )
    .is_ok());
    // The region belongs in the endpoint, and a credential is a reference or nothing.
    let mut region = Map::new();
    region.insert("region".into(), json!("eu-west-1"));
    let e = format!(
        "{:#}",
        check(&["aws-lambda://orders".into()], &region, Kind::Lambda).unwrap_err()
    );
    assert!(e.contains("the region goes in the endpoint"), "{e}");
    let mut literal = Map::new();
    literal.insert("access_key_ref".into(), json!("AKIDLITERAL"));
    literal.insert("secret_key_ref".into(), json!("env:LWF_TEST_LAMBDA_SK"));
    let e = format!(
        "{:#}",
        check(&["aws-lambda://orders".into()], &literal, Kind::Lambda).unwrap_err()
    );
    assert!(e.contains("must be a reference"), "{e}");
}

/// The rule check of the kind reads the payload limit from the endpoint, so a rule whose
/// endpoint the kind cannot parse gets no limit error from it: the endpoint is refused by the
/// load check, and a second message about its size would send the author looking for a
/// `batch_max_bytes` that is not the problem.
#[test]
fn the_rule_check_of_lambda_says_nothing_it_cannot_read() {
    let rule = |bytes: usize| RuleDef {
        name: "orders".into(),
        enabled: true,
        target: "orders".into(),
        selects: Vec::new(),
        snapshot_keys_sql: None,
        template: "{{json docs}}".into(),
        fire_per: crate::model::FirePer::Batch,
        batch_max_keys: None,
        batch_max_bytes: Some(bytes),
        debounce: std::time::Duration::ZERO,
        max_delay: std::time::Duration::from_secs(1),
        reset_steps: Vec::new(),
        reset_requested: false,
        vars: serde_json::Map::new(),
    };
    // An endpoint the kind cannot parse: no payload limit is known, so none is refused.
    assert!(rule_errors(&rule(9_000_000), &["nonsense".into()]).is_empty());
    // The same rule against a readable endpoint is refused, and says which invocation set
    // the limit — the two invocations have different limits and different words.
    let e = rule_errors(&rule(9_000_000), &["aws-lambda://eu-west-1/orders".into()]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("synchronous"), "{e:?}");
    let e = rule_errors(
        &rule(1_048_577),
        &["aws-lambda://eu-west-1/orders?invocation=event".into()],
    );
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("asynchronous (event)"), "{e:?}");
}

/// A row with no endpoint at all reaches the kind's own build, which must name what it needs
/// rather than index into nothing. `from_row` refuses that shape for `lambda` first ("kind
/// lambda takes exactly one endpoint"), so the definition is loaded the legal way and then
/// emptied by hand: what is under test is the guard behind the arity check, the one that has to
/// speak instead of panicking if `endpoints` is ever empty by the time `build` is reached.
#[test]
fn a_lambda_target_without_its_endpoint_names_it() {
    let mut def =
        TargetDef::from_row(&raw("lambda", &["aws-lambda://eu-west-1/orders"], "{}")).unwrap();
    def.endpoints.clear();
    // `build`'s answer is a bundle the kind does not let be printed, so the refusal is taken
    // by hand: the Ok side has to fail the test, not just be ignored.
    let e = match build(&def) {
        Ok(_) => panic!("a lambda target with no endpoint must not build"),
        Err(e) => format!("{e:#}"),
    };
    assert!(
        e.contains("kind lambda needs its aws-lambda:// endpoint"),
        "{e}"
    );
}

/// A flush can hold a loaded target whose endpoint row disappeared: the index it fires at is
/// out of range. That is a fact about the target, not about the keys, so both fronts of the
/// Lambda answer it as a failure to wait out and never as a refusal.
#[tokio::test]
async fn a_lambda_firing_at_an_endpoint_that_is_gone_waits() {
    let def = TargetDef::from_row(&raw(
        "lambda",
        &["aws-lambda://eu-west-1/orders?endpoint=http://127.0.0.1:9"],
        "{}",
    ))
    .unwrap();
    let t = Target::new(
        def,
        mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x"),
        None,
    )
    .unwrap();
    let e = t.send(9, "POST / HTTP/1.1\n\n{}").await.unwrap_err();
    let msg = format!("{e:#}");
    assert!(msg.contains("endpoint 9 of target `t` is gone"), "{msg}");
    assert!(!crate::target::is_rejected(&e), "{msg}");
    let e = t
        .send_items(9, "POST / HTTP/1.1\n\n{}", &["1".into(), "2".into()])
        .await
        .unwrap_err();
    let msg = format!("{e:#}");
    assert!(msg.contains("endpoint 9 of target `t` is gone"), "{msg}");
    assert!(
        !crate::target::is_rejected(&e),
        "a target row that went away mid-run may not drop its keys: {msg}"
    );
}
