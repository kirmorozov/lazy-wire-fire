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
