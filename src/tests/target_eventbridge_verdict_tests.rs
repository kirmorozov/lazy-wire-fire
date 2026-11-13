use super::*;
use crate::targets::eventbridge::{eventbridge_entries_ok, eventbridge_verdict};
use serde_json::json;

#[test]
fn an_entry_with_an_event_id_is_stored_and_checked_by_expect() {
    let e = json!({"EventId": "919a0147-83c9-4f53-a66c-c6a0aabbccdd"});
    match eventbridge_verdict(Some(&e), 0) {
        RecordVerdict::Stored(bytes) => {
            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), e)
        }
        other => panic!("{other:?}"),
    }
    // an empty or null id is no id
    let e = json!({"EventId": "", "ErrorCode": "ValidationException"});
    assert!(matches!(
        eventbridge_verdict(Some(&e), 1),
        RecordVerdict::Rejected(_)
    ));
    let e = json!({"EventId": null});
    assert_eq!(eventbridge_verdict(Some(&e), 2), RecordVerdict::Missing);
}

#[test]
fn throttling_and_unwell_service_codes_are_transient() {
    for code in [
        "ThrottlingException",
        "InternalFailure",
        "ServiceUnavailable",
    ] {
        let e = json!({"ErrorCode": code, "ErrorMessage": "try later"});
        assert_eq!(
            eventbridge_verdict(Some(&e), 3),
            RecordVerdict::Transient(format!("entry 3: {code}: try later"))
        );
    }
}

#[test]
fn any_other_code_is_a_refusal_of_that_entry() {
    // what moto and AWS answer for a bad entry (PutEvents result codes)
    for code in [
        "InvalidArgument",
        "MalformedDetail",
        "AccessDeniedException",
    ] {
        let e = json!({"ErrorCode": code, "ErrorMessage": "the reason"});
        assert_eq!(
            eventbridge_verdict(Some(&e), 1),
            RecordVerdict::Rejected(format!("entry 1: {code}: the reason"))
        );
    }
}

#[test]
fn a_missing_or_answerless_entry_waits() {
    assert_eq!(eventbridge_verdict(None, 5), RecordVerdict::Missing);
    assert_eq!(
        eventbridge_verdict(Some(&json!({})), 0),
        RecordVerdict::Missing
    );
}

fn entries(n: usize) -> String {
    let one = r#"{"Source":"lazywf","DetailType":"t","Detail":"{}"}"#;
    format!("{{\"Entries\": [{}]}}", vec![one; n].join(", "))
}

#[test]
fn a_batch_of_complete_entries_passes() {
    assert!(eventbridge_entries_ok(entries(3).as_bytes(), 3).is_ok());
}

#[test]
fn the_body_must_be_entries_json() {
    assert!(eventbridge_entries_ok(b"not json", 1).is_err());
    assert!(eventbridge_entries_ok(br#"{"Records":[]}"#, 0).is_err());
    assert!(eventbridge_entries_ok(br#"{"Entries":{}}"#, 0).is_err());
}

#[test]
fn the_entry_count_must_match_the_keys() {
    // one entry short or long: the positional answer could not be trusted
    assert!(eventbridge_entries_ok(entries(2).as_bytes(), 3).is_err());
    assert!(eventbridge_entries_ok(entries(4).as_bytes(), 3).is_err());
    assert!(eventbridge_entries_ok(br#"{"Entries": []}"#, 0).is_ok());
}

#[test]
fn every_entry_needs_the_three_required_members() {
    let cases = [
        r#"{"Entries":[{"DetailType":"t","Detail":"{}"}]}"#, // no Source
        r#"{"Entries":[{"Source":"s","Detail":"{}"}]}"#,     // no DetailType
        r#"{"Entries":[{"Source":"s","DetailType":"t"}]}"#,  // no Detail
        r#"{"Entries":[{"Source":"","DetailType":"t","Detail":"{}"}]}"#, // empty
        r#"{"Entries":[{"Source":"s","DetailType":"t","Detail":null}]}"#, // not a string
        r#"{"Entries":[{"Source":"s","DetailType":"t","Detail":{"k":1}}]}"#, // the JSON, not its text
        r#"{"Entries":["a"]}"#,                                              // not even an object
    ];
    for body in cases {
        assert!(
            eventbridge_entries_ok(body.as_bytes(), 1).is_err(),
            "accepted `{body}`"
        );
    }
}
