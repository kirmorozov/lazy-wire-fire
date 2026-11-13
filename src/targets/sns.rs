//! `sns`: an AWS SNS topic — `aws-sns://[<region>/]<account-id>/<topic-name>`, with the usual
//! optional `?endpoint=` (emulators), one `PublishBatch` per action over http, signed with SigV4
//! for service `sns`. Nothing in the endpoint URL is a path: the topic is named by its ARN, as
//! [`crate::targets::sqs::SqsUrl`] names the queue URL, and the host is
//! `sns.<region>.amazonaws.com`.
//!
//! What the kind decides is here: the endpoint and `vars` checks at load ([`check`]), the rule
//! checks ([`rule_errors`]), the endpoint and the signer of a loaded target ([`build`]), what
//! the target knows of its topic ([`Sns`]) with the pre-check of a rendered batch
//! ([`pre_check`]), and the URL its endpoint is written in ([`SnsUrl`] with its constants). The
//! purge-like `once` step is the one rule check that is not: it is refused for the four AWS
//! kinds that can purge (a queue, a stream, an event bus, a topic) by the one segment in
//! [`crate::rule::kind_errors`] that names them all, as the guards naming several kinds stay in
//! [`crate::model`] — `sqs`, `kinesis` and `eventbridge` share it and keep it there.
//!
//! The rest of the send path is not here either, and that is deliberate: a rendered action goes
//! out through the shared http-batch front of [`crate::target::Target::send_items`], whose XML
//! branch reads the answer with [`batch_answer`] and falls through to [`crate::aws::batch_items`]
//! — the AWS `Successful`/`Failed`/`Id`/`SenderFault` protocol it shares with `sqs`. The plain
//! `send` of one action is the generic http path of [`crate::target`] too, as it is for the
//! other kinds that speak http.
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

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Map, Value};

use crate::aws;
use crate::aws::{split_aws_url, valid_region};
use crate::docs;
use crate::http::Client as HttpClient;
use crate::model::{refuse_vars, FirePer, Kind, RuleDef, TargetDef};
use crate::target::{aws_refs, rejected, Transport};

/// Load checks for `sns` (from `TargetDef::from_row`): the account, the topic and the region
/// are in the URL, so `vars` takes none of them, and the credentials are references. `kind` is
/// the caller's `Kind`, which `refuse_vars` names in its message.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, kind: Kind) -> Result<()> {
    // `aws-sns://[<region>/]<account-id>/<topic-name>`: account, topic and region are
    // in the URL; the ARN is built from them (as SqsUrl builds the queue URL).
    match endpoints {
        [url] => {
            SnsUrl::parse(url)?;
        }
        _ => bail!(
            "kind sns takes exactly one endpoint: aws-sns://[<region>/]<account-id>/<topic-name>"
        ),
    }
    refuse_vars(vars, &["region", "account_id", "topic_name", "topic_arn"], kind, "the region, the account and the topic go in the endpoint: aws-sns://[<region>/]<account-id>/<topic-name>")?;
    check_vars(vars)?;
    aws::check_refs(vars)?;
    Ok(())
}

/// The `sns` checks of a rule (from `kind_errors`): one action is one `PublishBatch` (10
/// entries; the same byte limit bounds one message and the batch, the topic's
/// `MaximumMessageSize` raised by `vars.maximum_message_size`), and a FIFO topic needs a
/// `version` column to deduplicate by. The purge-like `once` step of these kinds is checked in
/// [`crate::rule::kind_errors`], shared by the four AWS kinds that can purge (the tuple naming
/// `DeleteTopic` lives there, as the shared guards that name several kinds live in
/// [`crate::model`]).
pub fn rule_errors(
    def: &RuleDef,
    fields: &[docs::Field],
    endpoints: &[String],
    vars: &Map<String, Value>,
) -> Vec<String> {
    let mut out = Vec::new();
    // One action is one PublishBatch: the same number bounds one message and the whole batch,
    // the topic's MaximumMessageSize (256 KiB by default, at most 1 MiB), which
    // `vars.maximum_message_size` states for the target (checked at load, so it cannot fail
    // here). The engine splits by bytes, never by count, so the count is a load-time
    // requirement as for sqs and eventbridge.
    let limit = maximum_message_size(vars).unwrap_or(SNS_MAX_BYTES);
    if def.fire_per == FirePer::Batch {
        match def.batch_max_keys {
            None => out
                .push("kind sns needs `batch_max_keys` (1 to 10: entries per PublishBatch)".into()),
            Some(n) if n > SNS_MAX_ENTRIES => out.push(format!(
                "`batch_max_keys` {n} is over {}, the most entries a PublishBatch takes",
                SNS_MAX_ENTRIES
            )),
            Some(_) => {}
        }
    }
    if def.batch_max_bytes.is_some_and(|n| n > limit) {
        out.push(format!(
            "`batch_max_bytes` is over {limit} bytes, the largest message and the largest batch the topic takes (its MaximumMessageSize, 262144 by default and at most 1048576; `vars.maximum_message_size` states it)"
        ));
    }
    let fifo = endpoints
        .iter()
        .any(|e| SnsUrl::parse(e).is_ok_and(|u| u.fifo()));
    if fifo && !fields.iter().any(|f| f.column == "version") {
        out.push(
            "a FIFO topic (a topic name ending in `.fifo`) needs a `version` column in a select: it is the MessageDeduplicationId"
                .into(),
        );
    }
    out
}

/// The endpoint of a loaded `sns` target: its template context, its http transport, how it
/// signs and what the target knows of its topic (from `Target::new`).
pub(crate) fn build(def: &TargetDef) -> Result<(Value, Transport, aws::Sign, Sns)> {
    // from_row: exactly one `aws-sns://[<region>/]<account-id>/<topic-name>` endpoint.
    // The request goes to the service root (`/`); the topic is named by its ARN in
    // the body, as `sqs` names its queue by `endpoint.url`.
    let url = SnsUrl::parse(
        def.endpoints
            .first()
            .ok_or_else(|| anyhow!("kind sns needs its aws-sns:// endpoint"))?,
    )?;
    let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
    let mut base = aws::address("sns", url.endpoint.as_deref(), &region)?;
    let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                     "region": region, "topic": url.topic,
                     "topic_arn": url.arn(&region), "fifo": url.fifo()});
    base.base = String::new();
    let host_header = base.host_header();
    let client = Arc::new(HttpClient::new(base, &def.tls)?);
    let transport = Transport::Http {
        client,
        prefix: String::new(),
        host_header,
    };
    let provider = aws::Provider::new(aws_refs(def), &region);
    let sign = aws::Sign {
        service: "sns",
        region,
        provider,
    };
    // The topic's MaximumMessageSize is the batch's byte limit; a var that does not
    // state it is refused here as at load (`TargetDef::from_row` checks it too).
    let sns = Sns {
        url,
        max_bytes: maximum_message_size(&def.vars)?,
    };
    Ok((ctx, transport, sign, sns))
}

/// What an `sns` target knows about its topic beyond the endpoint: whether it is FIFO (every
/// entry then needs a `MessageGroupId`) and its `MaximumMessageSize`, which is the batch's byte
/// limit (`vars.maximum_message_size` states it; see [`maximum_message_size`]).
pub(crate) struct Sns {
    pub(crate) url: SnsUrl,
    pub(crate) max_bytes: usize,
}

/// The pre-check of a rendered `PublishBatch` (from `Target::send_items`). An error is a
/// refusal of the whole action: the batch is not sent, the keys stay waiting.
pub(crate) fn pre_check(sns: &Option<Sns>, body: &[u8], n: usize) -> Result<()> {
    // The batch is answered per entry, by its `Id`: check the rendered form body before
    // the request goes out (see [`entries_ok`]).
    let fifo = sns.as_ref().is_some_and(|s| s.url.fifo());
    if let Err(why) = entries_ok(body, n, fifo) {
        return Err(rejected(format!("the rendered request: {why}")));
    }
    Ok(())
}

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
        None => Ok(SNS_MAX_BYTES),
        Some(v) => {
            let n = v
                .as_u64()
                .ok_or_else(|| anyhow!("`vars.maximum_message_size` is not a number"))?;
            if n == 0 || n as usize > SNS_MAX_MESSAGE_SIZE {
                bail!(
                    "`vars.maximum_message_size` is {n} (1 to {} bytes: the topic's MaximumMessageSize, which is 256 KiB by default and at most 1 MiB)",
                    SNS_MAX_MESSAGE_SIZE
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
    if n > SNS_MAX_ENTRIES {
        return Err(format!(
            "{n} entries is over {}, the most a PublishBatch takes",
            SNS_MAX_ENTRIES
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

// ---- `aws-sns://` endpoints ----

/// The most entries a PublishBatch request takes: 10 ("Publishes up to 10 messages to the
/// specified topic in a single batch"; more is `TooManyEntriesInBatchRequest`). Source:
/// docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html
pub const SNS_MAX_ENTRIES: usize = 10;

/// The default largest individual message and the default largest total payload of one
/// PublishBatch (the sum of the lengths of the batched messages): 256 KiB. Source: the same
/// page: "By default, the maximum allowed individual message size and the maximum total payload
/// size … are both 256 KiB (262,144 bytes)".
pub const SNS_MAX_BYTES: usize = 262_144;

/// The most a topic's `MaximumMessageSize` raises those two: 1 MiB (the same page). A topic
/// attribute, so `vars.maximum_message_size` states it for the target and raises the load-time
/// check with it, as kafka's `vars.message_max_bytes` does.
pub const SNS_MAX_MESSAGE_SIZE: usize = 1_048_576;

/// An `aws-sns://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnsUrl {
    pub region: Option<String>,
    pub account: String,
    pub topic: String,
    /// `?endpoint=`, as written (a validated http(s) URL).
    pub endpoint: Option<String>,
}

impl SnsUrl {
    /// `aws-sns://<account-id>/<topic-name>` or
    /// `aws-sns://<region>/<account-id>/<topic-name>`.
    pub fn parse(url: &str) -> Result<SnsUrl> {
        let shown = crate::secrets::redact_url(url);
        let (segs, endpoint) = split_aws_url(url, "sns")?;
        let (region, account, topic) = match segs.as_slice() {
            [a, t] => (None, a, t),
            [r, a, t] => (Some(r), a, t),
            _ => bail!(
                "`{shown}`: kind sns takes `aws-sns://<account-id>/<topic-name>` or `aws-sns://<region>/<account-id>/<topic-name>`"
            ),
        };
        if let Some(r) = region {
            if !valid_region(r) {
                bail!("`{shown}`: `{r}` is not an AWS region name, e.g. eu-west-1");
            }
        }
        if account.len() != 12 || !account.bytes().all(|c| c.is_ascii_digit()) {
            bail!("`{shown}`: the account id is twelve digits");
        }
        // A topic name is 1 to 256 letters, digits, `_` or `-`; a FIFO topic's name ends in
        // `.fifo` (CreateTopic, Name; docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html).
        let stem = topic.strip_suffix(".fifo").unwrap_or(topic);
        let name_ok = !stem.is_empty()
            && topic.len() <= 256
            && stem
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-');
        if !name_ok {
            bail!("`{shown}`: the topic name is 1 to 256 letters, digits, `-` or `_`, optionally ending in `.fifo`");
        }
        Ok(SnsUrl {
            region: region.cloned(),
            account: account.clone(),
            topic: topic.clone(),
            endpoint,
        })
    }

    /// A FIFO topic: its name ends in `.fifo`.
    pub fn fifo(&self) -> bool {
        self.topic.ends_with(".fifo")
    }

    /// The topic's ARN, as `PublishBatch`'s `TopicArn` takes it:
    /// `arn:aws:sns:<region>:<account-id>:<topic-name>` (CreateTopic returns exactly this form,
    /// with no `topic/` resource prefix before the name).
    pub fn arn(&self, region: &str) -> String {
        format!("arn:aws:sns:{region}:{}:{}", self.account, self.topic)
    }
}

#[cfg(test)]
#[path = "../tests/targets_sns.rs"]
mod tests;
