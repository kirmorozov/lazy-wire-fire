use super::*;
use serde_json::json;

/// The form body of a `PublishBatch` with one entry per pair, numbered from 1 (`Id` is the
/// item's index, the member number one more), as a template renders it.
fn body<S: AsRef<str>>(entries: &[(S, S)]) -> String {
    let mut out = String::from(
            "Action=PublishBatch&Version=2010-03-31&TopicArn=arn%3Aaws%3Asns%3Aeu-west-1%3A123456789012%3Aorders",
        );
    for (n, (id, message)) in entries.iter().enumerate() {
        out.push_str(&format!(
                "&PublishBatchRequestEntries.member.{}.Id={}&PublishBatchRequestEntries.member.{}.Message={}",
                n + 1,
                id.as_ref(),
                n + 1,
                message.as_ref()
            ));
    }
    out
}

/// moto's answer to that batch: item 0 stored, item 1 refused for a too-long message.
const ANSWER: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<PublishBatchResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><PublishBatchResult><Successful><member><Id>0</Id><MessageId>cf659c87-ab85-46b4-b5d7-a82fbe6b2a88</MessageId></member></Successful><Failed><member><Id>1</Id><Code>InvalidParameter</Code><Message>An error occurred (InvalidParameter) when calling the Publish operation: Invalid parameter: Message too long</Message><SenderFault>true</SenderFault></member></Failed></PublishBatchResult><ResponseMetadata><RequestId>dqgjHcxEllHQEUFOdDzbY82eG5Jkz3jm6wTZjxz4LUVDsMuagBfl</RequestId></ResponseMetadata></PublishBatchResponse>"#;

#[test]
fn a_rendered_batch_of_two_entries_is_ok() {
    assert!(entries_ok(
        body(&[("0", "hello"), ("1", "%7B%22a%22%3A1%7D")]).as_bytes(),
        2,
        false
    )
    .is_ok());
}

#[test]
fn entries_are_numbered_from_one() {
    // The mistake a template makes when it forgets `{{add @index 1}}`.
    let mut from_zero = String::from("Action=PublishBatch");
    from_zero.push_str(
        "&PublishBatchRequestEntries.member.0.Id=0&PublishBatchRequestEntries.member.0.Message=m",
    );
    let e = entries_ok(from_zero.as_bytes(), 1, false).unwrap_err();
    assert!(e.contains("numbered [0]"), "{e}");
    assert!(e.contains("from 1"), "{e}");
}

#[test]
fn every_key_needs_its_own_entry() {
    // Two keys, one entry.
    let e = entries_ok(body(&[("0", "hello")]).as_bytes(), 2, false).unwrap_err();
    assert!(e.contains("numbered [1]"), "{e}");
    // The same entry twice (`member.1` twice is not two entries).
    let twice = body(&[("0", "a")]) + "&PublishBatchRequestEntries.member.1.Id=1";
    assert!(entries_ok(twice.as_bytes(), 2, false).is_err());
    // And not one entry too many: that is what the batch limits are for.
    let many: Vec<(String, String)> = (0..=SNS_MAX_ENTRIES)
        .map(|i| (i.to_string(), "m".to_string()))
        .collect();
    assert_eq!(
        entries_ok(body(&many).as_bytes(), SNS_MAX_ENTRIES + 1, false).unwrap_err(),
        "11 entries is over 10, the most a PublishBatch takes"
    );
}

#[test]
fn an_entry_needs_its_id_and_message() {
    let e = entries_ok(body(&[("0", "hello"), ("1", "")]).as_bytes(), 2, false).unwrap_err();
    assert!(e.contains("entry 1 (member 2)"), "{e}");
    assert!(e.contains("`Message` is not a non-empty value"), "{e}");
    let e = entries_ok(body(&[("", "m"), ("1", "m")]).as_bytes(), 2, false).unwrap_err();
    assert!(e.contains("entry 0 (member 1)"), "{e}");
    assert!(e.contains("`Id`"), "{e}");
    // A field of no entry, and an entry number that is not a number.
    assert!(entries_ok(b"Action=PublishBatch&TopicArn=x", 1, false)
        .unwrap_err()
        .contains("numbered []"));
}

#[test]
fn a_fifo_entry_needs_a_message_group_id() {
    let ok = entries_ok(body(&[("0", "m")]).as_bytes(), 1, false);
    assert!(ok.is_ok());
    let e = entries_ok(body(&[("0", "m")]).as_bytes(), 1, true).unwrap_err();
    assert!(
        e.contains("`MessageGroupId` is not a non-empty value"),
        "{e}"
    );
    let with_group =
        body(&[("0", "m")]) + "&PublishBatchRequestEntries.member.1.MessageGroupId=tenant%2F7";
    assert!(entries_ok(with_group.as_bytes(), 1, true).is_ok());
}

#[test]
fn the_answer_says_per_entry_what_happened() {
    let answer = batch_answer(ANSWER.as_bytes()).unwrap();
    assert_eq!(
        answer,
        json!({
            "Successful": [{"Id": "0", "MessageId": "cf659c87-ab85-46b4-b5d7-a82fbe6b2a88"}],
            "Failed": [{
                "Id": "1",
                "Code": "InvalidParameter",
                "Message": "An error occurred (InvalidParameter) when calling the Publish operation: Invalid parameter: Message too long",
                "SenderFault": true,
            }]
        })
    );
}

#[test]
fn an_empty_failed_section_is_no_failures() {
    // What moto writes when nothing failed: `<Failed/>`.
    let xml = "<PublishBatchResponse><PublishBatchResult><Successful><member><Id>0</Id>\
                   <MessageId>m</MessageId></member></Successful><Failed/></PublishBatchResult>\
                   </PublishBatchResponse>";
    assert_eq!(
        batch_answer(xml.as_bytes()).unwrap(),
        json!({"Successful": [{"Id": "0", "MessageId": "m"}], "Failed": []})
    );
}

#[test]
fn a_sender_fault_that_says_nothing_keeps_the_item_waiting() {
    let answer = |fault: &str| {
        batch_answer(
            format!(
                "<PublishBatchResponse><PublishBatchResult><Successful/>\
                     <Failed><member><Id>0</Id><Code>InternalError</Code><Message>x</Message>\
                     <SenderFault>{fault}</SenderFault></member></Failed></PublishBatchResult>\
                     </PublishBatchResponse>"
            )
            .as_bytes(),
        )
        .unwrap()["Failed"][0]
            .get("SenderFault")
            .cloned()
    };
    assert_eq!(answer("true"), Some(json!(true)));
    assert_eq!(answer("True"), Some(json!(true)));
    assert_eq!(answer("false"), Some(json!(false)));
    // Neither: text, which `send_items` cannot read as a bool and so retries.
    assert_eq!(answer("maybe"), Some(json!("maybe")));
    assert_eq!(answer(""), Some(json!("")));
}

#[test]
fn the_answer_of_a_refused_request_names_the_code() {
    // moto's answer to a batch of eleven entries.
    let xml = "<ErrorResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\"><Error>\
                   <Type>Sender</Type><Code>TooManyEntriesInBatchRequest</Code>\
                   <Message>The batch request contains more entries than permissible.</Message>\
                   </Error><RequestId>x</RequestId></ErrorResponse>";
    let e = format!("{:#}", batch_answer(xml.as_bytes()).unwrap_err());
    assert!(e.contains("TooManyEntriesInBatchRequest"), "{e}");
    assert!(e.contains("more entries than permissible"), "{e}");
}

#[test]
fn an_answer_that_is_no_answer_is_an_error() {
    // Not the document at all: nothing can be trusted, so say so (the keys stay waiting).
    let e = format!(
        "{:#}",
        batch_answer(b"<html>Bad Gateway</html>").unwrap_err()
    );
    assert!(e.contains("not a PublishBatchResult"), "{e}");
}

#[test]
fn entities_are_read_back() {
    let xml = "<PublishBatchResponse><PublishBatchResult><Successful/>\
                   <Failed><member><Id>0</Id><Code>InvalidParameter</Code>\
                   <Message>a &amp; b &lt;x&gt; &#65;&#x42;</Message>\
                   <SenderFault>true</SenderFault></member></Failed></PublishBatchResult>\
                   </PublishBatchResponse>";
    let failed = batch_answer(xml.as_bytes()).unwrap()["Failed"][0].clone();
    assert_eq!(failed["Message"], json!("a & b <x> AB"));
    // A `&` that begins no reference stays.
    let xml = xml.replace("a &amp; b", "a & b");
    let failed = batch_answer(xml.as_bytes()).unwrap()["Failed"][0].clone();
    assert_eq!(failed["Message"], json!("a & b <x> AB"));
}

#[test]
fn the_largest_message_is_the_topics_and_can_be_raised() {
    let vars = |v: &str| -> Map<String, Value> {
        serde_json::from_str(&format!("{{\"maximum_message_size\": {v}}}")).unwrap()
    };
    assert_eq!(maximum_message_size(&Map::new()).unwrap(), SNS_MAX_BYTES);
    assert_eq!(maximum_message_size(&vars("262144")).unwrap(), 262_144);
    assert_eq!(maximum_message_size(&vars("1048576")).unwrap(), 1_048_576);
    // Tighter is the author's own business; beyond what AWS takes is refused.
    assert_eq!(maximum_message_size(&vars("1024")).unwrap(), 1024);
    assert!(maximum_message_size(&vars("0")).is_err());
    assert!(maximum_message_size(&vars("1048577")).is_err());
    assert!(maximum_message_size(&vars("null")).is_err());
    assert!(maximum_message_size(&vars("\"1048576\"")).is_err());
    assert!(format!("{:#}", maximum_message_size(&vars("0")).unwrap_err()).contains("0"));
    assert!(check_vars(&Map::new()).is_ok());
    assert!(check_vars(&vars("2000000")).is_err());
}
