//! Amazon SNS `PublishBatch`: the wire details of the `sns` target kind.
//!
//! One action is one `PublishBatch`: a POST to the service root in the query protocol's form
//! encoding (`Action=PublishBatch&Version=2010-03-31&TopicArn=…`, the entries as
//! `PublishBatchRequestEntries.member.<n>.<Field>` with `n` from 1), signed with SigV4 for
//! service `sns` (see [`crate::aws`]; the topic and its region come from the
//! `aws-sns://[<region>/]<account-id>/<topic-name>` endpoint, whose ARN the template puts in the
//! body as `endpoint.topic_arn`). The answer says per entry what happened — `Successful` holds
//! the `Id` and `MessageId` of what was stored, `Failed` the `Id`, `Code`, `Message` and
//! `SenderFault` of what was not — so [`batch_answer`] reads that XML into the JSON shape
//! [`crate::target::Target::send_items`] maps onto items, and [`entries_ok`] checks the rendered
//! entries before the request goes out.
//!
//! The query protocol rather than the JSON one the `sqs` kind uses: AWS answers
//! `X-Amz-Target: AmazonSNS.PublishBatch` as well, but moto — the emulator the tests run against —
//! answers it with a 500 and answers the query protocol in full. Source: the PublishBatch
//! operation (docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html) and the query protocol
//! (docs.aws.amazon.com/general/latest/gr/query-protocols.html).

use anyhow::{anyhow, bail, Result};
use serde_json::{Map, Value};

use crate::aws;

/// Checks what a `sns` target's `vars` must hold (called by `TargetDef::from_row`).
pub fn check_vars(vars: &Map<String, Value>) -> Result<()> {
    maximum_message_size(vars).map(|_| ())
}

/// The largest message a `sns` target may put in a batch: the topic's `MaximumMessageSize`,
/// which is 256 KiB by default and at most 1 MiB. `vars.maximum_message_size` states it for the
/// target, as kafka's `vars.message_max_bytes` states the broker's limit: the same number bounds
/// one message and the whole batch, so it is the kind's batch byte limit (see
/// [`crate::target::Target::batch_bytes_limit`]). A smaller value is allowed: it only tightens
/// the load-time check. A larger one is refused — AWS would refuse the message anyway, and a
/// message it refuses after it is rendered would cost the key a refusal. Source: PublishBatch
/// ("By default, the maximum allowed individual message size and the maximum total payload size
/// … are both 256 KiB (262,144 bytes)"; docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html)
/// and `MaximumMessageSize` (API_SetTopicAttributes.html).
pub fn maximum_message_size(vars: &Map<String, Value>) -> Result<usize> {
    match vars.get("maximum_message_size") {
        None => Ok(aws::SNS_MAX_BYTES),
        Some(v) => {
            let n = v
                .as_u64()
                .ok_or_else(|| anyhow!("`vars.maximum_message_size` is not a number"))?;
            if n == 0 || n as usize > aws::SNS_MAX_MESSAGE_SIZE {
                bail!(
                    "`vars.maximum_message_size` is {n} (1 to {} bytes: the topic's MaximumMessageSize, which is 256 KiB by default and at most 1 MiB)",
                    aws::SNS_MAX_MESSAGE_SIZE
                );
            }
            Ok(n as usize)
        }
    }
}

/// The rendered body of a `PublishBatch` action: the form encoding's
/// `PublishBatchRequestEntries.member.<n>` groups, one per key (`n` from 1, so a template
/// numbers them `{{add @index 1}}`), each holding what AWS requires of every entry: `Id` and
/// `Message` as non-empty values (PublishBatchRequestEntry: both Required;
/// docs.aws.amazon.com/sns/latest/api/API_PublishBatchRequestEntry.html), plus
/// `MessageGroupId`, which a FIFO topic requires on every message (the same page). Checked
/// before the request goes out: a template that renders a broken batch fails once, with one
/// clear error, instead of `n` per-entry refusals on every retry.
pub fn entries_ok(body: &[u8], n: usize, fifo: bool) -> Result<(), String> {
    if n == 0 {
        return Err("no keys: a batch sends no PublishBatch".to_string());
    }
    if n > aws::SNS_MAX_ENTRIES {
        return Err(format!(
            "{n} entries is over {}, the most a PublishBatch takes",
            aws::SNS_MAX_ENTRIES
        ));
    }
    let mut groups: std::collections::BTreeMap<usize, std::collections::BTreeMap<String, String>> =
        std::collections::BTreeMap::new();
    for (key, value) in form_pairs(body) {
        // The request's own parameters (`Action`, `Version`, `TopicArn`, …) name no entry.
        let Some(rest) = key.strip_prefix("PublishBatchRequestEntries.member.") else {
            continue;
        };
        let Some((ordinal, field)) = rest.split_once('.') else {
            return Err(format!("`{key}` names no field of an entry"));
        };
        let Ok(ordinal) = ordinal.parse::<usize>() else {
            return Err(format!(
                "`{key}`: an entry is numbered `PublishBatchRequestEntries.member.<n>` with `n` from 1"
            ));
        };
        groups
            .entry(ordinal)
            .or_default()
            .insert(field.to_string(), value);
    }
    // The numbers run 1 to the number of entries, each used once: the API's own numbering, so an
    // answer's `Id` can be the index of its key.
    let got: Vec<usize> = groups.keys().copied().collect();
    let want: Vec<usize> = (1..=n).collect();
    if got != want {
        return Err(format!(
            "the entries are numbered {got:?}, not {want:?} (PublishBatch numbers its entries from 1)"
        ));
    }
    for i in 0..n {
        let group = &groups[&(i + 1)];
        let need = |field: &str, why: &str| -> Result<(), String> {
            if group.get(field).is_none_or(String::is_empty) {
                return Err(format!(
                    "entry {i} (member {}): `{field}` is not a non-empty value ({why})",
                    i + 1
                ));
            }
            Ok(())
        };
        need("Id", "PublishBatch names the entry by it")?;
        need("Message", "PublishBatch requires a message")?;
        if fifo {
            need(
                "MessageGroupId",
                "a FIFO topic requires it on every message",
            )?;
        }
    }
    Ok(())
}

/// The answer of a `PublishBatch` request, in the JSON shape `send_items` reads per item:
/// `{"Successful": [{"Id": …, "MessageId": …}], "Failed": [{"Id": …, "Code": …, "Message": …,
/// "SenderFault": true}]}` (PublishBatchResult, PublishBatchResultEntry and
/// BatchResultErrorEntry; docs.aws.amazon.com/sns/latest/api/API_PublishBatchResult.html).
/// `SenderFault` becomes a JSON bool; text that is neither true nor false is passed through as
/// text, which the item mapping reads as "nothing was said clearly" and retries the key.
pub fn batch_answer(body: &[u8]) -> Result<Value> {
    let xml = String::from_utf8_lossy(body);
    if let Some((code, message)) = query_error(&xml) {
        // The error document of the query protocol. It comes with a 4xx, whose code `aws::
        // error_code` reads for the same reason: here it only makes the message say what AWS said.
        bail!("SNS refused the request: {code}: {message}");
    }
    let result = element(&xml, "PublishBatchResult")
        .ok_or_else(|| anyhow!("the answer is not a PublishBatchResult: {xml}"))?;
    let read = |field: &str| -> Vec<Value> {
        element(&result, field)
            .map(|text| children(&text))
            .unwrap_or_default()
            .into_iter()
            .filter(|(name, _)| name == "member")
            .map(|(_, text)| {
                let mut item = Map::new();
                for (name, value) in children(&text) {
                    let value = if name == "SenderFault" {
                        match bool_text(&value) {
                            Some(b) => Value::Bool(b),
                            None => Value::String(value),
                        }
                    } else {
                        Value::String(value)
                    };
                    item.insert(name, value);
                }
                Value::Object(item)
            })
            .collect()
    };
    let mut out = Map::new();
    out.insert("Successful".to_string(), Value::Array(read("Successful")));
    out.insert("Failed".to_string(), Value::Array(read("Failed")));
    Ok(Value::Object(out))
}

/// The pairs of an `application/x-www-form-urlencoded` body, each decoded (`%XX` and `+`).
fn form_pairs(body: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(body)
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (crate::aws::percent_decode(k), crate::aws::percent_decode(v))
        })
        .collect()
}

/// The code and message of the query protocol's error document
/// (`<ErrorResponse><Error><Code>NotFound</Code><Message>…</Message>…</Error>…</ErrorResponse>`).
/// Source: AWS General Reference, errors for the query protocol
/// (docs.aws.amazon.com/general/latest/gr/query-error-responses.html).
fn query_error(xml: &str) -> Option<(String, String)> {
    let error = element(xml, "ErrorResponse").and_then(|root| element(&root, "Error"))?;
    let fields = children(&error);
    let get = |name: &str| {
        fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let code = get("Code");
    (!code.is_empty()).then(|| (code, get("Message")))
}

/// Whether an opening tag's text (between its name and its `>`) closes the element itself: a
/// trailing `/` that is not inside a quoted attribute value — `<Failed/>` and `<Failed />` do,
/// `<PublishBatchResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">` does not. Source:
/// XML 1.0 §3.1 empty-element tags (www.w3.org/TR/xml/#sec-starttags).
fn self_closed(tag_text: &str) -> bool {
    let mut quoted = false;
    let mut last: Option<char> = None;
    for c in tag_text.chars() {
        if c == '"' {
            quoted = !quoted;
            last = Some(c);
        } else if !quoted && !c.is_whitespace() {
            last = Some(c);
        }
    }
    !quoted && last == Some('/')
}

/// The text of the element `name` among those `xml` holds directly, descending through the
/// document's root element when it is not among them: what the caller wants of a `PublishBatch`
/// answer sits one level in (`<PublishBatchResponse><PublishBatchResult>…`). Each step goes
/// strictly deeper, so it ends at the leaves.
fn element(xml: &str, name: &str) -> Option<String> {
    let held = children(xml);
    if let Some((_, text)) = held.iter().find(|(n, _)| n == name) {
        return Some(text.clone());
    }
    held.into_iter()
        .next()
        .and_then(|(_, root)| element(&root, name))
}

/// The elements `xml` holds directly, as `(name, text of what it holds)`, in document order. The
/// answer's elements do not nest their own name and their values hold no markup (AWS escapes
/// `<` and `&`), so a scan of the tags finds exactly the elements of the document.
fn children(xml: &str) -> Vec<(String, String)> {
    let b = xml.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'<' {
            i += 1;
            continue;
        }
        // The name: letters and digits after the `<` (`<?xml …?>`, `</…>` and `<!…>` hold none).
        let mut j = i + 1;
        while j < b.len() && b[j].is_ascii_alphanumeric() {
            j += 1;
        }
        if j == i + 1 {
            i += 1;
            continue;
        }
        let name = xml[i + 1..j].to_string();
        let Some(rel) = xml[j..].find('>') else {
            break;
        };
        let gt = j + rel;
        if self_closed(&xml[j..gt]) {
            i = gt + 1; // `<Failed/>`: an element that holds nothing
            continue;
        }
        let close = match xml[gt + 1..].find(&format!("</{name}>")) {
            Some(c) => gt + 1 + c,
            None => break,
        };
        let text = unescape(&xml[gt + 1..close]);
        i = close + name.len() + 3;
        out.push((name, text));
    }
    out
}

/// The XML entities of an element's text: the five the predefined ones give, and the numeric
/// forms (`&#65;`, `&#x41;`). Source: XML 1.0 §4.6 character references
/// (www.w3.org/TR/xml/#sec-predefined-ent).
fn unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let Some(end) = after.find(';') else {
            out.push('&');
            rest = after;
            continue;
        };
        let name = &after[..end];
        let held = match name {
            "amp" => Some("&"),
            "lt" => Some("<"),
            "gt" => Some(">"),
            "quot" => Some("\""),
            "apos" => Some("'"),
            _ => None,
        };
        if let Some(c) = held {
            out.push_str(c);
            rest = &after[end + 1..];
            continue;
        }
        let digits = name.strip_prefix('#').unwrap_or(name);
        let code = if let Some(hex) = digits.strip_prefix(['x', 'X']) {
            u32::from_str_radix(hex, 16).ok()
        } else if digits.is_empty() {
            None
        } else {
            digits.parse::<u32>().ok()
        };
        match code.and_then(char::from_u32) {
            Some(c) => {
                out.push(c);
                rest = &after[end + 1..];
            }
            // Not a reference: the `&` was meant as itself.
            None => {
                out.push('&');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// A `SenderFault` as AWS writes it: `true` or `false`, in either case (the query protocol's
/// booleans are the strings `true` and `false`; API_BatchResultErrorEntry.html). `None`: text
/// that says neither, which the caller leaves the item mapping to treat as no clear answer.
fn bool_text(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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
        let many: Vec<(String, String)> = (0..=aws::SNS_MAX_ENTRIES)
            .map(|i| (i.to_string(), "m".to_string()))
            .collect();
        assert_eq!(
            entries_ok(body(&many).as_bytes(), aws::SNS_MAX_ENTRIES + 1, false).unwrap_err(),
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
        assert_eq!(
            maximum_message_size(&Map::new()).unwrap(),
            aws::SNS_MAX_BYTES
        );
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
}
