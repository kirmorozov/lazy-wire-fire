//! `sqs`: an AWS SQS queue — `aws-sqs://[<region>/]<account-id>/<queue-name>`, one
//! `SendMessageBatch` per action over http, signed with SigV4 ([`crate::aws`]).
//!
//! What the kind decides is here: the endpoint and `vars` checks at load ([`check`]), the rule
//! checks ([`rule_errors`]), the endpoint and the signer of a loaded target ([`build`]), and the
//! URL its endpoint is written in ([`SqsUrl`]). The purge-like `once` step is the one rule check
//! that is not: it is refused for the four AWS kinds that can purge (a queue, a stream, an event
//! bus, a topic) by the one segment in [`crate::rule::kind_errors`] that names them all, as the
//! guards naming several kinds stay in [`crate::model`] — `kinesis`, `eventbridge` and `sns`
//! share it and keep it there.
//!
//! The send path is not here either, and that is deliberate. A rendered action is one
//! `SendMessageBatch` request: it goes out through the shared http-batch front of
//! [`crate::target::Target::send_items`] (the request line, the status verdict, the JSON answer
//! read), whose answer tail is [`crate::aws::batch_items`] — the AWS
//! `Successful`/`Failed`/`Id`/`SenderFault` protocol. That tail belongs to `sqs` and to `sns` and
//! to no one else: `sns` reaches it from its own branch of
//! the same front, whose `PublishBatch` answer is XML and is read into the very same shape
//! ([`crate::sns::batch_answer`]). So it is AWS wire plumbing next to the signer, not a copy of
//! it here. The plain `send` of one action is the generic http path of [`crate::target`] too, as
//! it is for the other kinds that speak http.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Map, Value};

use crate::aws;
use crate::aws::{split_aws_url, valid_region};
use crate::docs;
use crate::http::Client as HttpClient;
use crate::model::{refuse_vars, FirePer, Kind, RuleDef, TargetDef};
use crate::target::{aws_refs, Transport};

/// Load checks for `sqs` (from `TargetDef::from_row`): the queue and the region are in the URL,
/// so `vars` takes none of them, and the credentials are references. `kind` is the caller's
/// `Kind`, which `refuse_vars` names in its message.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, kind: Kind) -> Result<()> {
    // `aws-sqs://[<region>/]<account>/<queue>`: the queue and the region are in the URL.
    match endpoints {
        [url] => {
            SqsUrl::parse(url)?;
        }
        _ => bail!(
            "kind sqs takes exactly one endpoint: aws-sqs://[<region>/]<account-id>/<queue-name>"
        ),
    }
    refuse_vars(
        vars,
        &["region"],
        kind,
        "the region goes in the endpoint: aws-sqs://<region>/<account-id>/<queue-name>",
    )?;
    aws::check_refs(vars)?;
    Ok(())
}

/// The `sqs` checks of a rule (from `kind_errors`): a batch is one `SendMessageBatch` (10 entries,
/// 256 KiB), and a FIFO queue needs a `version` column to deduplicate by. The purge-like `once`
/// step of these kinds is checked in [`crate::rule::kind_errors`], shared by the four AWS kinds
/// that can purge (the tuple naming `PurgeQueue` lives there, as the shared guards that name
/// several kinds live in [`crate::model`]).
pub fn rule_errors(def: &RuleDef, fields: &[docs::Field], endpoints: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    if def.fire_per == FirePer::Batch {
        match def.batch_max_keys {
            None => out.push(
                "kind sqs needs `batch_max_keys` (1 to 10: entries per SendMessageBatch)".into(),
            ),
            Some(n) if n > 10 => out.push(format!(
                "`batch_max_keys` {n} is over 10, the most entries a SendMessageBatch takes"
            )),
            Some(_) => {}
        }
    }
    if def.batch_max_bytes.is_some_and(|n| n > 262_144) {
        out.push("`batch_max_bytes` is over 262144, the most a SendMessageBatch takes".into());
    }
    let fifo = endpoints
        .iter()
        .any(|e| SqsUrl::parse(e).is_ok_and(|u| u.fifo()));
    if fifo && !fields.iter().any(|f| f.column == "version") {
        out.push(
            "a FIFO queue (a queue name ending in `.fifo`) needs a `version` column in a select: it is the MessageDeduplicationId"
                .into(),
        );
    }
    out
}

/// The endpoint of a loaded `sqs` target: its template context, its http transport and how it
/// signs (from `Target::new`).
pub(crate) fn build(def: &TargetDef) -> Result<(Value, Transport, aws::Sign)> {
    // from_row: exactly one `aws-sqs://[<region>/]<account>/<queue>` endpoint. The
    // request goes to the service root (`/`); the queue URL is `endpoint.url`.
    let url = SqsUrl::parse(
        def.endpoints
            .first()
            .ok_or_else(|| anyhow!("kind sqs needs its aws-sqs:// endpoint"))?,
    )?;
    let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
    let mut base = aws::address("sqs", url.endpoint.as_deref(), &region)?;
    let queue_url = format!("{}/{}/{}", base.url(), url.account, url.queue);
    let ctx = json!({"host": base.host, "port": base.port, "url": queue_url,
                     "region": region, "queue": url.queue});
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
        service: "sqs",
        region,
        provider,
    };
    Ok((ctx, transport, sign))
}

/// An `aws-sqs://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqsUrl {
    pub region: Option<String>,
    pub account: String,
    pub queue: String,
    /// `?endpoint=`, as written (a validated http(s) URL).
    pub endpoint: Option<String>,
}

impl SqsUrl {
    /// `aws-sqs://<account-id>/<queue-name>` or `aws-sqs://<region>/<account-id>/<queue-name>`.
    pub fn parse(url: &str) -> Result<SqsUrl> {
        let shown = crate::secrets::redact_url(url);
        let (segs, endpoint) = split_aws_url(url, "sqs")?;
        let (region, account, queue) = match segs.as_slice() {
            [a, q] => (None, a, q),
            [r, a, q] => (Some(r), a, q),
            _ => bail!(
                "`{shown}`: kind sqs takes `aws-sqs://<account-id>/<queue-name>` or `aws-sqs://<region>/<account-id>/<queue-name>`"
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
        let stem = queue.strip_suffix(".fifo").unwrap_or(queue);
        let name_ok = !stem.is_empty()
            && queue.len() <= 80
            && stem
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-');
        if !name_ok {
            bail!("`{shown}`: the queue name is 1 to 80 letters, digits, `-` or `_`, optionally ending in `.fifo`");
        }
        Ok(SqsUrl {
            region: region.cloned(),
            account: account.clone(),
            queue: queue.clone(),
            endpoint,
        })
    }

    /// A FIFO queue: its name ends in `.fifo`.
    pub fn fifo(&self) -> bool {
        self.queue.ends_with(".fifo")
    }
}
