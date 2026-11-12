use super::*;
use serde_json::json;

fn parsed(body: &str, n_keys: usize) -> Result<Option<Action>> {
    Action::parse(body.as_bytes(), n_keys)
}

/// The rendered body is written by a template, so every way it can be wrong has to
/// be refused with the record's number in the message: a record that is quietly
/// dropped is a key that is never delivered and never retried.
#[test]
fn a_record_is_its_value_and_string_headers() {
    let a = parsed(
        r#"{"records":[{"value":{"id":1,"note":"x"},"headers":{"trace":"t-1","kind":"order"}}]}"#,
        1,
    )
    .unwrap()
    .unwrap();
    assert_eq!(a.records.len(), 1);
    assert_eq!(a.records[0].value, json!({"id": 1, "note": "x"}));
    assert_eq!(a.records[0].bytes, br#"{"id":1,"note":"x"}"#);
    assert_eq!(a.records[0].headers.len(), 2);
    assert_eq!(
        a.records[0].headers.get("trace").map(String::as_str),
        Some("t-1")
    );

    // Headers are optional; a null is the same as none.
    for body in [
        r#"{"records":[{"value":1}]}"#,
        r#"{"records":[{"value":1,"headers":null}]}"#,
    ] {
        let a = parsed(body, 1).unwrap().unwrap();
        assert!(a.records[0].headers.is_empty(), "{body}");
    }

    let refused = |body: &str, n_keys: usize, why: &str| {
        let e = format!(
            "{:#}",
            parsed(body, n_keys)
                .err()
                .unwrap_or_else(|| panic!("not refused: {body}"))
        );
        assert!(e.contains(why), "{body}: {e}");
    };
    // A header that is not a string, and `headers` that is not an object: the
    // broker's header wire format is text, and sending a number as a header
    // would have the whole record refused by the broker.
    refused(
        r#"{"records":[{"value":1,"headers":{"k":5}}]}"#,
        1,
        "the header `k` is not a string",
    );
    refused(
        r#"{"records":[{"value":1,"headers":["k"]}]}"#,
        1,
        "`headers` is not an object of strings",
    );
    // A field the broker does not take (`key` comes to mind: the partition key is
    // the rule's key, never the template's to choose).
    refused(
        r#"{"records":[{"value":1,"key":"7"}]}"#,
        1,
        "unknown field `key`",
    );
    // A null value is a Kafka tombstone, which lazywf does not send: a delete has
    // to be a record the consumer can read.
    refused(r#"{"records":[{"value":null}]}"#, 1, "the `value` is null");
    refused(r#"{"records":[{}]}"#, 1, "has no `value`");
    refused(r#"{"records":["x"]}"#, 1, "record 0 is not an object");
    refused(
        r#"{"records":[{"value":1},{"value":2}]}"#,
        1,
        "2 records for 1 keys",
    );
    refused(r#"{"records":[{"value":1}]}"#, 2, "1 records for 2 keys");
    refused("not json", 1, "not JSON");
    refused(r#"{"rows":[]}"#, 0, "records");
    // An empty list is nothing to send, and says so instead of producing.
    assert!(parsed(r#"{"records":[]}"#, 0).unwrap().is_none());
}

/// Anything the client cannot judge must retry: the safe default is the one that
/// cannot drop a key. A half-read answer is exactly that case.
#[test]
fn an_answer_the_client_cannot_read_is_not_a_refusal() {
    for e in [
        ClientError::InvalidResponse("the batch ends mid-record".into()),
        ClientError::Request(rskafka::client::error::RequestError::IO(
            std::io::Error::other("the socket closed"),
        )),
    ] {
        match client_error_outcome(&e) {
            Outcome::Transient(why) => {
                assert!(why.contains("not known to be complete"), "{e:?}: {why}")
            }
            other => panic!("{e:?} must retry, got {other:?}"),
        }
    }
}
