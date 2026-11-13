//! `eventbridge`: an AWS EventBridge event bus —
//! `aws-eventbridge://[<region>/]<event-bus-name-or-arn>`, with the usual optional `?endpoint=`
//! (emulators, VPC endpoints), one `PutEvents` per action over http, signed with SigV4
//! ([`crate::aws`]). The request goes to the service root and the bus is named in the entries
//! (`endpoint.bus`); the region is in the URL, or else from the default chain (`region_from`).
//! Credentials never go in the URL.
//!
//! What the kind decides is here: the endpoint and `vars` checks at load ([`check`]), the rule
//! checks ([`rule_errors`]), the endpoint and the signer of a loaded target ([`build`]), how one
//! `PutEvents` answer becomes the items of a batch ([`entries_items`], [`eventbridge_verdict`]),
//! the check of the rendered entries before the request goes out ([`eventbridge_entries_ok`]) and
//! the URL its endpoint is written in ([`EventBridgeUrl`], with the two limits of one
//! `PutEvents`). The purge-like `once` step is the one rule check that is not: it is refused for
//! the four AWS kinds that can purge (a queue, a stream, an event bus, a topic) by the one
//! segment in [`crate::rule::kind_errors`] that names them all, as the guards naming several
//! kinds stay in [`crate::model`] — `sqs`, `kinesis` and `sns` share it and keep it there.
//!
//! The rest of the send path is not here either, and that is deliberate. A rendered action is
//! one `PutEvents` request: it goes out through the shared http-batch front of
//! [`crate::target::Target::send_items`] (the request line, the status verdict, the JSON answer
//! read), and only the answer's per-entry mapping is [`entries_items`]. The plain `send` of one
//! action is the generic http path of [`crate::target`] too, as it is for the other kinds that
//! speak http. `PutEvents` is a JSON 1.1 call to the service root: `X-Amz-Target:
//! AWSEvents.PutEvents`, `Content-Type: application/x-amz-json-1.1`, body `{"Entries": [...]}`;
//! SigV4 signs with service `events`, the host is `events.<region>.amazonaws.com`. It answers by
//! position, so unlike `sqs` and `sns` the kind has no share of the answer protocol to leave in
//! [`crate::aws`].
//! Source: PutEvents API (docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html)
//! and the service endpoints reference (`events.<region>.amazonaws.com`).

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::Pool;
use serde_json::{json, Map, Value};

use crate::aws;
use crate::aws::{split_aws_url, valid_region};
use crate::http::Client as HttpClient;
use crate::model::{refuse_vars, FirePer, Kind, RuleDef, TargetDef};
use crate::response::Expect;
use crate::target::{aws_refs, Item, RecordVerdict, Transport};

/// Load checks for `eventbridge` (from `TargetDef::from_row`): the bus (name or ARN) and the
/// region are in the URL, so `vars` takes none of them, and the credentials are references.
/// `kind` is the caller's `Kind`, which `refuse_vars` names in its message.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, kind: Kind) -> Result<()> {
    // `aws-eventbridge://[<region>/]<event-bus>` (name or ARN): the bus and the
    // region are in the URL.
    match endpoints {
        [url] => {
            EventBridgeUrl::parse(url)?;
        }
        _ => bail!(
            "kind eventbridge takes exactly one endpoint: aws-eventbridge://[<region>/]<event-bus-name-or-arn>"
        ),
    }
    refuse_vars(
        vars,
        &["region", "event_bus_name", "event_bus_arn"],
        kind,
        "the region and the event bus go in the endpoint: aws-eventbridge://[<region>/]<event-bus-name-or-arn>",
    )?;
    aws::check_refs(vars)?;
    Ok(())
}

/// The `eventbridge` checks of a rule (from `kind_errors`): a batch is one `PutEvents`
/// ([`EVENTBRIDGE_MAX_ENTRIES`] entries, [`EVENTBRIDGE_MAX_BYTES`]). The purge-like `once` step of
/// these kinds is checked in [`crate::rule::kind_errors`], shared by the four AWS kinds that can
/// purge (the tuple naming `DeleteEventBus` lives there, as the shared guards that name several
/// kinds live in [`crate::model`]).
pub fn rule_errors(def: &RuleDef) -> Vec<String> {
    let mut out = Vec::new();
    // One action is one PutEvents (`EVENTBRIDGE_MAX_ENTRIES` / `_MAX_BYTES`); the engine
    // splits by bytes, never by count, so the count is a load-time requirement as for sqs.
    if def.fire_per == FirePer::Batch {
        match def.batch_max_keys {
            None => out.push(
                "kind eventbridge needs `batch_max_keys` (1 to 10: entries per PutEvents)".into(),
            ),
            Some(n) if n > EVENTBRIDGE_MAX_ENTRIES => out.push(format!(
                "`batch_max_keys` {n} is over {}, the most entries a PutEvents takes",
                EVENTBRIDGE_MAX_ENTRIES
            )),
            Some(_) => {}
        }
    }
    if def
        .batch_max_bytes
        .is_some_and(|n| n > EVENTBRIDGE_MAX_BYTES)
    {
        out.push(
            "`batch_max_bytes` is over 1048576 (1 MB), the most a PutEvents request takes".into(),
        );
    }
    out
}

/// The endpoint of a loaded `eventbridge` target: its template context, its http transport and
/// how it signs (from `Target::new`).
pub(crate) fn build(def: &TargetDef) -> Result<(Value, Transport, aws::Sign)> {
    // from_row: exactly one `aws-eventbridge://[<region>/]<event-bus>` endpoint. The
    // request goes to the service root; the bus is named in the entries
    // (`endpoint.bus`).
    let url = EventBridgeUrl::parse(
        def.endpoints
            .first()
            .ok_or_else(|| anyhow!("kind eventbridge needs its aws-eventbridge:// endpoint"))?,
    )?;
    let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
    let mut base = aws::address("events", url.endpoint.as_deref(), &region)?;
    let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                     "region": region, "bus": url.bus});
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
        service: "events",
        region,
        provider,
    };
    Ok((ctx, transport, sign))
}

/// What one `Entries[i]` of a `PutEvents` answer says, before `expect` (see
/// [`eventbridge_verdict`]). EventBridge answers by position: the i-th entry of the answer is
/// the i-th entry of the request.
pub fn eventbridge_verdict(entry: Option<&Value>, i: usize) -> RecordVerdict {
    let Some(e) = entry else {
        return RecordVerdict::Missing;
    };
    // An `EventId` says the event was stored (PutEvents, PutEventsResponseEntry.EventId).
    if e.get("EventId")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty())
    {
        return RecordVerdict::Stored(serde_json::to_vec(e).unwrap_or_default());
    }
    let text = |k: &str| e.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    match e
        .get("ErrorCode")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
    {
        // The AWS "not now" codes (aws::transient_code: throttling, the service is unwell) keep
        // the key waiting, as for a whole-request 503; any other code is the service's clear
        // refusal of this one entry (PutEvents, PutEventsResultEntry.ErrorCode).
        Some(code) if crate::aws::transient_code(code) => {
            RecordVerdict::Transient(format!("entry {i}: {code}: {}", text("ErrorMessage")))
        }
        Some(code) => {
            RecordVerdict::Rejected(format!("entry {i}: {code}: {}", text("ErrorMessage")))
        }
        // Neither an `EventId` nor an `ErrorCode`: nothing is known about this entry.
        None => RecordVerdict::Missing,
    }
}

/// The rendered body of a `PutEvents` action: `{"Entries": [...]}` with exactly one entry per
/// key (`Entries[i]` is answered by position), each holding the members AWS requires on every
/// entry as non-empty strings (PutEvents, PutEventsRequestEntry: `Source`, `DetailType`,
/// `Detail`). Checked before the request goes out: a template that renders a broken batch
/// fails once with one clear error, not as `n` per-entry refusals on every retry.
pub(crate) fn eventbridge_entries_ok(body: &[u8], n: usize) -> Result<(), String> {
    let request: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return Err(format!("the body is not JSON: {e}")),
    };
    let Some(entries) = request.get("Entries").and_then(Value::as_array) else {
        return Err("the body has no `Entries` array".to_string());
    };
    if entries.len() != n {
        return Err(format!(
            "`Entries` holds {} entries for {n} keys",
            entries.len()
        ));
    }
    for (i, e) in entries.iter().enumerate() {
        for field in ["Source", "DetailType", "Detail"] {
            if e.get(field)
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err(format!(
                    "entry {i}: `{field}` is not a non-empty string (PutEvents requires it on every entry)"
                ));
            }
        }
    }
    Ok(())
}

/// The items of a `PutEvents` answer: `Entries[i]` answers item i, and `expect` decides each
/// stored entry with the entry as `body`, as the records of
/// [`crate::targets::kinesis::records_items`] do for their batch.
pub async fn entries_items(
    answer: &Value,
    n: usize,
    expect: &Expect,
    pool: &Pool,
    status: u16,
    headers: &[(String, String)],
    url: &str,
) -> Result<Vec<Item>> {
    // `Entries[i]` answers item i. `expect` per entry, as for the records above.
    let entries = answer
        .get("Entries")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let item = match eventbridge_verdict(entries.get(i), i) {
            RecordVerdict::Stored(bytes) => match expect
                .http(pool, status, headers, &bytes)
                .await
                .with_context(|| {
                format!("entry {i} of {}", url)
            })? {
                Ok(()) => Item::Ok,
                Err(why) => Item::Rejected(format!("entry {i}: {why}")),
            },
            RecordVerdict::Transient(m) => Item::Transient(m),
            RecordVerdict::Rejected(m) => Item::Rejected(m),
            RecordVerdict::Missing => Item::Transient(format!("entry {i}: no answer")),
        };
        out.push(item);
    }
    Ok(out)
}

/// The most entries a PutEvents request takes: 10 (`Entries` array, "The maximum number of
/// records per transaction is 10"). Source:
/// docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html
pub const EVENTBRIDGE_MAX_ENTRIES: usize = 10;

/// The largest sum of all the entries of one PutEvents request: 1 MB (1,048,576 bytes). The
/// rendered request also carries the JSON around the entries, so the limit is applied to the
/// rendered body (conservative, as for Kinesis). Source: "Event size" of the PutEvents
/// user guide (docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevents.html#eb-putevent-size).
pub const EVENTBRIDGE_MAX_BYTES: usize = 1_048_576;

/// An `aws-eventbridge://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBridgeUrl {
    pub region: Option<String>,
    /// The event bus, as `Entries[].EventBusName` takes it: the name, for an ARN endpoint the
    /// ARN's resource name.
    pub bus: String,
    /// `?endpoint=`, as written (a validated http(s) URL).
    pub endpoint: Option<String>,
}

/// An event bus name: 1 to 256 letters, digits, `-`, `_` or `.` (CreateEventBus, `Name`;
/// docs.aws.amazon.com/eventbridge/latest/APIReference/API_CreateEventBus.html).
fn check_bus_name(shown: &str, name: &str) -> Result<()> {
    let ok = (1..=256).contains(&name.len())
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'));
    if !ok {
        bail!("`{shown}`: an event bus name is 1 to 256 letters, digits, `-`, `_` or `.`");
    }
    Ok(())
}

/// The region and the bus name of an event bus ARN:
/// `arn:<partition>:events:<region>:<account-id>:event-bus/<name>` (CreateEventBus, Arn;
/// docs.aws.amazon.com/eventbridge/latest/APIReference/API_CreateEventBus.html).
fn bus_of_eventbus_arn(shown: &str, arn: &str) -> Result<(String, String)> {
    let bad = || {
        anyhow!("`{shown}`: an event bus ARN is `arn:aws:events:<region>:<account-id>:event-bus/<event-bus-name>`")
    };
    // The resource part (`event-bus/<name>`) is everything after the last colon: it is the
    // only part a `/` can appear in (a bus name holds no `/`, and `split_aws_url` has just
    // split the URL on `/`).
    let (head, resource) = arn.rsplit_once(':').ok_or_else(bad)?;
    let (res_type, name) = resource.split_once('/').ok_or_else(bad)?;
    let parts: Vec<&str> = head.split(':').collect();
    let [_, partition, service, region, account] = parts.as_slice() else {
        return Err(bad());
    };
    if !matches!(*partition, "aws" | "aws-cn" | "aws-us-gov")
        || *service != "events"
        || res_type != "event-bus"
    {
        return Err(bad());
    }
    if !valid_region(region) {
        bail!("`{shown}`: `{region}` is not an AWS region name, e.g. eu-west-1");
    }
    if account.len() != 12 || !account.bytes().all(|c| c.is_ascii_digit()) {
        bail!("`{shown}`: the account id is twelve digits");
    }
    check_bus_name(shown, name)?;
    Ok((region.to_string(), name.to_string()))
}

impl EventBridgeUrl {
    /// `aws-eventbridge://<bus>` or `aws-eventbridge://<region>/<bus>`, the bus a name or its
    /// ARN (an ARN names its own region).
    pub fn parse(url: &str) -> Result<EventBridgeUrl> {
        let shown = crate::secrets::redact_url(url);
        let (segs, endpoint) = split_aws_url(url, "eventbridge")?;
        // The ARN's resource part (`event-bus/<name>`) contains a `/`, which `split_aws_url`
        // has split on; it is rejoined here. A leading region segment therefore needs two
        // ordinary segments, neither of which starts an ARN.
        let (region, text) = match segs.as_slice() {
            [r, name] if !r.starts_with("arn:") && !name.starts_with("arn:") => {
                if !valid_region(r) {
                    bail!("`{shown}`: `{r}` is not an AWS region name, e.g. eu-west-1");
                }
                (Some(r.clone()), name.clone())
            }
            _ => (None, segs.join("/")),
        };
        if text.starts_with("arn:") {
            let (arn_region, bus) = bus_of_eventbus_arn(&shown, &text)?;
            Ok(EventBridgeUrl {
                region: Some(arn_region),
                bus,
                endpoint,
            })
        } else {
            if text.contains("/arn:") {
                bail!("`{shown}`: an event bus ARN names its own region: write it alone, `aws-eventbridge://<arn>`, or use the bus name with a region");
            }
            check_bus_name(&shown, &text)?;
            Ok(EventBridgeUrl {
                region,
                bus: text,
                endpoint,
            })
        }
    }
}
