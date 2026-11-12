use super::*;
use crate::model::{FirePer, RawRule, ResetStep, TargetDef};

const QUEUE: &str = "aws-sqs://eu-west-1/123456789012/orders";
const FIFO: &str = "aws-sqs://eu-west-1/123456789012/orders.fifo";

fn def() -> RuleDef {
    let mut d = RuleDef::placeholder(&RawRule {
        name: "r".into(),
        enabled: true,
        ..Default::default()
    });
    d.fire_per = FirePer::Batch;
    d.batch_max_keys = Some(10);
    d
}

fn cols(v: &[&str]) -> Vec<docs::Field> {
    v.iter()
        .map(|s| docs::Field::new(*s, None, false))
        .collect()
}

fn stray() -> StepKind {
    StepKind::Stray {
        list_template: "GET /".into(),
        keys: "JSON_EXTRACT(body, '$.ids')".into(),
        next: None,
    }
}

fn step(kind: StepKind, template: Option<&str>) -> ResetStep {
    ResetStep {
        name: "s".into(),
        kind,
        template: template.map(str::to_string),
    }
}

/// The target of a rule of kind `kind` on `endpoint` (its load checks pass; the tests
/// exercise the rule side).
fn target(kind: &str, endpoint: &str, vars: &str) -> TargetDef {
    test_target(kind, endpoint, vars)
}

#[test]
fn lambda_refuses_strays_and_oversized_payloads() {
    const FN: &str = "aws-lambda://eu-west-1/orders";
    const EVENT: &str = "aws-lambda://eu-west-1/orders?invocation=event";
    // a stray listing does not exist for a function
    let mut d = def();
    d.reset_steps = vec![step(stray(), None)];
    let e = kind_errors(&d, &target("lambda", FN, "{}"), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("can list keys; kind lambda cannot"), "{e:?}");
    // the sync payload limit is 6 MB; the event limit is 1 MB
    let mut d = def();
    d.batch_max_bytes = Some(6_291_456);
    assert!(kind_errors(&d, &target("lambda", FN, "{}"), &[]).is_empty());
    d.batch_max_bytes = Some(6_291_457);
    assert_eq!(kind_errors(&d, &target("lambda", FN, "{}"), &[]).len(), 1);
    let mut d = def();
    d.batch_max_bytes = Some(1_048_577);
    assert_eq!(
        kind_errors(&d, &target("lambda", EVENT, "{}"), &[]).len(),
        1
    );
    d.batch_max_bytes = Some(1_048_576);
    assert!(kind_errors(&d, &target("lambda", EVENT, "{}"), &[]).is_empty());
    // no batch_max_keys is needed: a batch is bounded by bytes
    let mut d = def();
    d.batch_max_keys = None;
    assert!(kind_errors(&d, &target("lambda", FN, "{}"), &[]).is_empty());
}

#[test]
fn stray_steps_are_refused_for_append_only_kinds_only() {
    let mut d = def();
    d.reset_steps = vec![step(stray(), None)];
    let e = kind_errors(&d, &target("sqs", QUEUE, "{}"), &cols(&["version"]));
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("only appends"));
    assert!(kind_errors(&d, &target("http", "https://h", "{}"), &[]).is_empty());
    // kafka appends too (its stray step is refused by the same check).
    let e = kind_errors(
        &d,
        &target("kafka", "kafka://broker:9092/orders", "{}"),
        &[],
    );
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("only appends"));
}

#[test]
fn amqp_refuses_stray_steps_and_batches() {
    let t = "amqp://broker";
    let v = r#"{"username": "u", "password": "env:P"}"#;
    let mut d = def();
    d.fire_per = FirePer::Key;
    d.reset_steps = vec![step(stray(), None)];
    let e = kind_errors(&d, &target("amqp", t, v), &cols(&["_key"]));
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("only appends"));
    d.reset_steps = Vec::new();
    d.fire_per = FirePer::Batch;
    let e = kind_errors(&d, &target("amqp", t, v), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    d.fire_per = FirePer::Key;
    assert!(kind_errors(&d, &target("amqp", t, v), &[]).is_empty());
}

#[test]
fn purge_once_steps_are_refused_for_sqs() {
    let mut d = def();
    d.reset_steps = vec![step(
        StepKind::Once,
        Some("X-Amz-Target: AmazonSQS.PurgeQueue\n\n{}"),
    )];
    assert_eq!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).len(), 1);
    d.reset_steps = vec![step(
        StepKind::Once,
        Some("POST / HTTP/1.1\n\n{\"announce\":true}"),
    )];
    assert!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).is_empty());
}

#[test]
fn sqs_batches_hold_at_most_ten_keys_and_256_kib() {
    let mut d = def();
    d.batch_max_keys = None;
    assert_eq!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).len(), 1);
    d.batch_max_keys = Some(11);
    assert_eq!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).len(), 1);
    d.batch_max_keys = Some(10);
    assert!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).is_empty());
    d.batch_max_bytes = Some(262_145);
    assert_eq!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).len(), 1);
    // One key per action has no batch to size.
    d.fire_per = FirePer::Key;
    d.batch_max_keys = None;
    d.batch_max_bytes = None;
    assert!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).is_empty());
}

#[test]
fn fifo_queues_need_a_version_column() {
    let d = def();
    assert_eq!(
        kind_errors(&d, &target("sqs", FIFO, "{}"), &cols(&["_key"])).len(),
        1
    );
    assert!(kind_errors(&d, &target("sqs", FIFO, "{}"), &cols(&["_key", "version"])).is_empty());
    // A standard queue does not need one.
    assert!(kind_errors(&d, &target("sqs", QUEUE, "{}"), &[]).is_empty());
}

const STREAM: &str = "aws-kinesis://eu-west-1/orders";

#[test]
fn kinesis_batches_hold_at_most_500_records_and_5_mib() {
    let mut d = def();
    d.batch_max_keys = None;
    assert_eq!(
        kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).len(),
        1
    );
    d.batch_max_keys = Some(501);
    assert_eq!(
        kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).len(),
        1
    );
    d.batch_max_keys = Some(500);
    assert!(kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).is_empty());
    d.batch_max_bytes = Some(5_242_881);
    assert_eq!(
        kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).len(),
        1
    );
    d.batch_max_bytes = Some(5_242_880);
    assert!(kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).is_empty());
    // One key per action has no batch to size.
    d.fire_per = FirePer::Key;
    d.batch_max_keys = None;
    d.batch_max_bytes = None;
    assert!(kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).is_empty());
}

#[test]
fn kinesis_refuses_stray_and_stream_delete_resets() {
    let mut d = def();
    d.reset_steps = vec![step(stray(), None)];
    let e = kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("only appends"));

    d.reset_steps = vec![step(
        StepKind::Once,
        Some("POST / HTTP/1.1\nX-Amz-Target: Kinesis_20131202.DeleteStream\n\n{}"),
    )];
    assert_eq!(
        kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).len(),
        1
    );
    // An announce record is a legitimate `once` action.
    d.reset_steps = vec![step(
        StepKind::Once,
        Some("POST / HTTP/1.1\nX-Amz-Target: Kinesis_20131202.PutRecords\n\n{}"),
    )];
    assert!(kind_errors(&d, &target("kinesis", STREAM, "{}"), &[]).is_empty());
}

const KAFKA: &str = "kafka://broker:9092/orders";

#[test]
fn kafka_batches_are_bounded_by_the_broker_limit() {
    let mut d = def();
    // Both fire_per values are fine; a batch is bounded by bytes only…
    d.fire_per = FirePer::Key;
    d.batch_max_keys = None;
    assert!(kind_errors(&d, &target("kafka", KAFKA, "{}"), &[]).is_empty());
    d.fire_per = FirePer::Batch;
    d.batch_max_bytes = Some(1_048_576);
    assert!(kind_errors(&d, &target("kafka", KAFKA, "{}"), &[]).is_empty());
    // …and over the broker's `message.max.bytes` is refused at load.
    d.batch_max_bytes = Some(1_048_577);
    let e = kind_errors(&d, &target("kafka", KAFKA, "{}"), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("batch_max_bytes"), "{e:?}");
    // The target's `vars.message_max_bytes` raises the allowance with it.
    d.batch_max_bytes = Some(2_000_000);
    let raised = r#"{"message_max_bytes": 2097152}"#;
    assert!(kind_errors(&d, &target("kafka", KAFKA, raised), &[]).is_empty());
}

const BUS: &str = "aws-eventbridge://eu-west-1/orders";

#[test]
fn eventbridge_batches_hold_at_most_ten_entries_and_1_mib() {
    let eb = || target("eventbridge", BUS, "{}");
    let mut d = def();
    d.batch_max_keys = None;
    assert_eq!(kind_errors(&d, &eb(), &[]).len(), 1);
    d.batch_max_keys = Some(11);
    let e = kind_errors(&d, &eb(), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("a PutEvents takes"), "{e:?}");
    d.batch_max_keys = Some(10);
    assert!(kind_errors(&d, &eb(), &[]).is_empty());
    d.batch_max_bytes = Some(1_048_577);
    assert_eq!(kind_errors(&d, &eb(), &[]).len(), 1);
    d.batch_max_bytes = Some(1_048_576);
    assert!(kind_errors(&d, &eb(), &[]).is_empty());
    // One key per action has no batch to size.
    d.fire_per = FirePer::Key;
    d.batch_max_keys = None;
    d.batch_max_bytes = None;
    assert!(kind_errors(&d, &eb(), &[]).is_empty());
}

#[test]
fn eventbridge_refuses_stray_and_bus_delete_resets() {
    let eb = || target("eventbridge", BUS, "{}");
    let mut d = def();
    d.reset_steps = vec![step(stray(), None)];
    let e = kind_errors(&d, &eb(), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("only appends"));

    d.reset_steps = vec![step(
        StepKind::Once,
        Some("POST / HTTP/1.1\nX-Amz-Target: AWSEvents.DeleteEventBus\n\n{}"),
    )];
    let e = kind_errors(&d, &eb(), &[]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("DeleteEventBus"), "{e:?}");
    // PutEvents is the kind's everyday action, not a purge.
    d.reset_steps = vec![step(
        StepKind::Once,
        Some("POST / HTTP/1.1\nX-Amz-Target: AWSEvents.PutEvents\n\n{}"),
    )];
    assert!(kind_errors(&d, &eb(), &[]).is_empty());
}
