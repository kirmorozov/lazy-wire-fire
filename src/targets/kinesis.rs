//! `kinesis`: an AWS Kinesis stream — `aws-kinesis://[<region>/]<stream-name>`, with an
//! optional `?endpoint=http(s)://host:port` (emulators, VPC endpoints), one `PutRecords` per
//! action over http, signed with SigV4 ([`crate::aws`]). The request goes to the service root
//! and the stream is named in the body (`endpoint.stream`); the region is in the URL, or else
//! from the default chain (`region_from`). Credentials never go in the URL.
//!
//! What the kind decides is here: the endpoint and `vars` checks at load ([`check`]), the rule
//! checks ([`rule_errors`]), the endpoint and the signer of a loaded target ([`build`]), how one
//! `PutRecords` answer becomes the items of a batch ([`records_items`], [`kinesis_verdict`]) and
//! the URL its endpoint is written in ([`KinesisUrl`]). The purge-like `once` step is the one
//! rule check that is not: it is refused for the four AWS kinds that can purge (a queue, a
//! stream, an event bus, a topic) by the one segment in [`crate::rule::kind_errors`] that names
//! them all, as the guards naming several kinds stay in [`crate::model`] — `sqs`, `eventbridge`
//! and `sns` share it and keep it there.
//!
//! The rest of the send path is not here either, and that is deliberate. A rendered action is
//! one `PutRecords` request: it goes out through the shared http-batch front of
//! [`crate::target::Target::send_items`] (the request line, the status verdict, the JSON answer
//! read), and only the answer's per-record mapping is [`records_items`]. The plain `send` of one
//! action is the generic http path of [`crate::target`] too, as it is for the other kinds that
//! speak http. `PutRecords` answers by position, so unlike `sqs` and `sns` the kind has no share
//! of the answer protocol to leave in [`crate::aws`].

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

/// Load checks for `kinesis` (from `TargetDef::from_row`): the stream and the region are in the
/// URL, so `vars` takes none of them, and the credentials are references. `kind` is the caller's
/// `Kind`, which `refuse_vars` names in its message.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, kind: Kind) -> Result<()> {
    // `aws-kinesis://[<region>/]<stream>`: the stream and the region are in the URL.
    match endpoints {
        [url] => {
            KinesisUrl::parse(url)?;
        }
        _ => {
            bail!("kind kinesis takes exactly one endpoint: aws-kinesis://[<region>/]<stream-name>")
        }
    }
    refuse_vars(
        vars,
        &["region", "stream_name", "stream_arn"],
        kind,
        "the region and the stream go in the endpoint: aws-kinesis://[<region>/]<stream-name>",
    )?;
    aws::check_refs(vars)?;
    Ok(())
}

/// The `kinesis` checks of a rule (from `kind_errors`): a batch is one `PutRecords` (500 records,
/// 5 MiB). The purge-like `once` step of these kinds is checked in [`crate::rule::kind_errors`],
/// shared by the four AWS kinds that can purge (the tuple naming `DeleteStream` lives there, as
/// the shared guards that name several kinds live in [`crate::model`]).
pub fn rule_errors(def: &RuleDef) -> Vec<String> {
    let mut out = Vec::new();
    if def.fire_per == FirePer::Batch {
        match def.batch_max_keys {
            None => out.push(
                "kind kinesis needs `batch_max_keys` (1 to 500: records per PutRecords)".into(),
            ),
            Some(n) if n > 500 => out.push(format!(
                "`batch_max_keys` {n} is over 500, the most records a PutRecords takes"
            )),
            Some(_) => {}
        }
        if def.batch_max_bytes.is_some_and(|n| n > 5_242_880) {
            out.push(
                "`batch_max_bytes` is over 5242880 (5 MiB), the most a PutRecords takes".into(),
            );
        }
    }
    out
}

/// The endpoint of a loaded `kinesis` target: its template context, its http transport and how it
/// signs (from `Target::new`).
pub(crate) fn build(def: &TargetDef) -> Result<(Value, Transport, aws::Sign)> {
    // from_row: exactly one `aws-kinesis://[<region>/]<stream>` endpoint. The request
    // goes to the service root; the stream is named in the body (`endpoint.stream`).
    let url = KinesisUrl::parse(
        def.endpoints
            .first()
            .ok_or_else(|| anyhow!("kind kinesis needs its aws-kinesis:// endpoint"))?,
    )?;
    let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
    let mut base = aws::address("kinesis", url.endpoint.as_deref(), &region)?;
    let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                     "region": region, "stream": url.stream});
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
        service: "kinesis",
        region,
        provider,
    };
    Ok((ctx, transport, sign))
}

/// The per-record mapping of a Kinesis `PutRecords` answer. The transient codes are
/// `ProvisionedThroughputExceededException`, `InternalFailure` and `ServiceUnavailable` (the
/// service is unwell: keep retrying, as for a whole-request 503); every other code is a refusal
/// of that record.
pub fn kinesis_verdict(entry: Option<&Value>, i: usize) -> RecordVerdict {
    let Some(e) = entry else {
        return RecordVerdict::Missing;
    };
    match e
        .get("ErrorCode")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
    {
        None => RecordVerdict::Stored(serde_json::to_vec(e).unwrap_or_default()),
        Some(code) => {
            let msg = format!(
                "record {i}: {code}: {}",
                e.get("ErrorMessage").and_then(Value::as_str).unwrap_or("")
            );
            match code {
                "ProvisionedThroughputExceededException"
                | "InternalFailure"
                | "ServiceUnavailable" => RecordVerdict::Transient(msg),
                _ => RecordVerdict::Rejected(msg),
            }
        }
    }
}

/// The items of a `PutRecords` answer: `Records[i]` answers item i, and `expect` decides each
/// stored record with the record as `body`, as the entries of
/// [`crate::targets::eventbridge::entries_items`] do for their batch.
pub async fn records_items(
    answer: &Value,
    n: usize,
    expect: &Expect,
    pool: &Pool,
    status: u16,
    headers: &[(String, String)],
    url: &str,
) -> Result<Vec<Item>> {
    // `Records[i]` answers item i. `expect` per record, as for the entries of
    // [`crate::targets::eventbridge::entries_items`].
    let records = answer
        .get("Records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let item = match kinesis_verdict(records.get(i), i) {
            RecordVerdict::Stored(bytes) => match expect
                .http(pool, status, headers, &bytes)
                .await
                .with_context(|| {
                format!("record {i} of {}", url)
            })? {
                Ok(()) => Item::Ok,
                Err(why) => Item::Rejected(format!("record {i}: {why}")),
            },
            RecordVerdict::Transient(m) => Item::Transient(m),
            RecordVerdict::Rejected(m) => Item::Rejected(m),
            RecordVerdict::Missing => Item::Transient(format!("record {i}: no answer")),
        };
        out.push(item);
    }
    Ok(out)
}

/// An `aws-kinesis://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KinesisUrl {
    pub region: Option<String>,
    pub stream: String,
    /// `?endpoint=`, as written (a validated http(s) URL).
    pub endpoint: Option<String>,
}

impl KinesisUrl {
    /// `aws-kinesis://<stream-name>` or `aws-kinesis://<region>/<stream-name>`.
    pub fn parse(url: &str) -> Result<KinesisUrl> {
        let shown = crate::secrets::redact_url(url);
        let (segs, endpoint) = split_aws_url(url, "kinesis")?;
        let (region, stream) = match segs.as_slice() {
            [s] => (None, s),
            [r, s] => (Some(r), s),
            _ => bail!(
                "`{shown}`: kind kinesis takes `aws-kinesis://<stream-name>` or `aws-kinesis://<region>/<stream-name>`"
            ),
        };
        if let Some(r) = region {
            if !valid_region(r) {
                bail!("`{shown}`: `{r}` is not an AWS region name, e.g. eu-west-1");
            }
        }
        let ok = (1..=128).contains(&stream.len())
            && stream
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'));
        if !ok {
            bail!("`{shown}`: a stream name is 1 to 128 letters, digits, `-`, `_` or `.`");
        }
        Ok(KinesisUrl {
            region: region.cloned(),
            stream: stream.clone(),
            endpoint,
        })
    }
}
