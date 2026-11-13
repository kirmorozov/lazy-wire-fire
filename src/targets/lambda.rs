//! `lambda`: an AWS Lambda function — `aws-lambda://[<region>/]<function>[:<qualifier>]`,
//! with `?invocation=sync` (the default, RequestResponse) or `?invocation=event` (Event), and
//! the usual optional `?endpoint=` (emulators), one `Invoke` per action over http, signed with
//! SigV4 ([`crate::aws`]). The request goes to the service root; its path names the function.
//! The region and credential rules are those of `aws-sqs://`: the region is in the URL or from
//! the default chain (`region_from`), and credentials never go in the URL.
//!
//! What the kind decides is here: the endpoint and `vars` checks at load ([`check`]), the rule
//! checks ([`rule_errors`]), the endpoint and the signer of a loaded target ([`build`]), one
//! `Invoke` with the rendered payload ([`lambda_invoke`]), how its `batchItemFailures` answer
//! becomes the items of a batch ([`lambda_items`]), and the URL its endpoint is written in
//! ([`LambdaUrl`] with [`Invocation`]), the `X-Amz-Function-Error` read ([`function_error`]) and
//! the failure list read ([`lambda_failed_items`]). The stray-step refusal of these rules is
//! the one rule check that is not: the segment in [`crate::rule::kind_errors`] that also covers
//! the append-only kinds names lambda with them, as the guards naming several kinds stay in
//! [`crate::model`].
//!
//! The dispatch is not here either, and that is deliberate: [`crate::target::Target::send`] and
//! [`crate::target::Target::send_items`] call [`lambda_invoke`] and map its answer to [`Item`]s
//! with [`lambda_items`]. A rendered action is one signed `Invoke` answered by
//! `batchItemFailures`, so the kind shares no part of the AWS `Successful`/`Failed` answer
//! protocol that [`crate::aws`] keeps for `sqs` and `sns`.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::Pool;
use serde_json::{json, Map, Value};

use crate::aws;
use crate::aws::{split_aws_url, valid_region};
use crate::http::{Client as HttpClient, RawRequest};
use crate::model::{refuse_vars, Kind, RuleDef, TargetDef};
use crate::target::{aws_refs, rejected, Item, Target, Transport};

/// Load checks for `lambda` (from `TargetDef::from_row`): the function, the region and the
/// invocation are in the URL, so `vars` takes none of them, and the credentials are references.
/// `kind` is the caller's `Kind`, which `refuse_vars` names in its message.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, kind: Kind) -> Result<()> {
    // `aws-lambda://[<region>/]<function>[:<qualifier>][?invocation=…]`: the function,
    // the region and the invocation are in the URL.
    match endpoints {
        [url] => {
            LambdaUrl::parse(url)?;
        }
        _ => bail!(
            "kind lambda takes exactly one endpoint: aws-lambda://[<region>/]<function-name>[:<qualifier>]"
        ),
    }
    refuse_vars(
        vars,
        &["region"],
        kind,
        "the region goes in the endpoint: aws-lambda://<region>/<function-name>",
    )?;
    aws::check_refs(vars)?;
    Ok(())
}

/// The `lambda` checks of a rule (from `kind_errors`): the payload limit is set by the
/// invocation (6 MB sync, 1 MB event), so a larger `batch_max_bytes` is refused at load. The
/// stray-step refusal of these rules is checked in [`crate::rule::kind_errors`], shared with
/// the append-only kinds (the segment naming them lives there, as the shared guards that name
/// several kinds live in [`crate::model`]).
pub fn rule_errors(def: &RuleDef, endpoints: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    // The payload limit is set by the invocation (6 MB sync, 1 MB event), so a larger
    // `batch_max_bytes` is refused at load.
    if let Some(u) = endpoints.first().and_then(|e| LambdaUrl::parse(e).ok()) {
        if def.batch_max_bytes.is_some_and(|n| n > u.payload_limit()) {
            out.push(format!(
                "`batch_max_bytes` is over {} bytes, the most a {} Invoke payload takes",
                u.payload_limit(),
                match u.invocation {
                    Invocation::Sync => "synchronous",
                    Invocation::Event => "asynchronous (event)",
                }
            ));
        }
    }
    out
}

/// The endpoint of a loaded `lambda` target: its template context, its http transport, how it
/// signs and the invocation endpoint the target keeps (from `Target::new`).
pub(crate) fn build(def: &TargetDef) -> Result<(Value, Transport, aws::Sign, LambdaUrl)> {
    // from_row: exactly one `aws-lambda://[<region>/]<function>[:<qualifier>]` endpoint.
    // The request goes to the service root; its path names the function.
    let url = LambdaUrl::parse(
        def.endpoints
            .first()
            .ok_or_else(|| anyhow!("kind lambda needs its aws-lambda:// endpoint"))?,
    )?;
    let region = aws::resolve_region(url.region.as_deref()).context("`endpoints`")?;
    let mut base = aws::address("lambda", url.endpoint.as_deref(), &region)?;
    let ctx = json!({"host": base.host, "port": base.port, "url": base.url(),
                     "region": region, "function": url.function,
                     "qualifier": url.qualifier,
                     "invocation": if url.invocation == Invocation::Event { "event" } else { "sync" }});
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
        service: "lambda",
        region,
        provider,
    };
    Ok((ctx, transport, sign, url))
}

/// The items of a Lambda answer: the keys the function listed in `batchItemFailures` retry
/// (transient); every other key is met. An identifier that is not a key of the batch is a wrong
/// answer, so it is an error and nothing is trusted.
pub fn lambda_items(failed: &[String], keys: &[String]) -> Result<Vec<Item>> {
    let mut out = vec![Item::Ok; keys.len()];
    for id in failed {
        let i = keys.iter().position(|k| k == id).ok_or_else(|| {
            anyhow!("batchItemFailures lists `{id}`, which is not a key of the batch")
        })?;
        out[i] = Item::Transient(format!("key `{id}`: listed in batchItemFailures"));
    }
    Ok(out)
}

/// One Invoke of a `lambda` target with the rendered payload (the template's body), signed
/// through the AWS path of `prepare`. `Ok(None)`: nothing rendered. `Ok(Some(failed))`: the
/// answer is met, and `failed` lists the identifiers the function left out (`batchItemFailures`;
/// always empty for an event). `Err`: refused ([`crate::target::Rejected`]) or transient, as for
/// `send`: a non-2xx status (5xx, 408, 429 and AWS throttling codes are transient), an
/// `X-Amz-Function-Error` (refused), or a refused `expect` (sync only; an event is met on 202).
pub(crate) async fn lambda_invoke(
    target: &Target,
    pool: &Pool,
    ep: usize,
    url: Option<&LambdaUrl>,
    rendered: &str,
) -> Result<Option<Vec<String>>> {
    let (Some(endpoint), Some(url)) = (target.endpoints.get(ep), url) else {
        bail!("endpoint {ep} of target `{}` is gone", target.def.name);
    };
    if !matches!(endpoint.transport, Transport::Http { .. }) {
        bail!("target `{}` is not a lambda target", target.def.name);
    }
    if rendered.trim().is_empty() {
        return Ok(None);
    }
    if rendered.len() > url.payload_limit() {
        return Err(rejected(format!(
            "the payload is {} bytes, over the {} bytes an Invoke takes",
            rendered.len(),
            url.payload_limit()
        )));
    }
    let invocation = match url.invocation {
        Invocation::Sync => "RequestResponse",
        Invocation::Event => "Event",
    };
    let req = RawRequest {
        method: "POST".into(),
        target: url.path(),
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("X-Amz-Invocation-Type".into(), invocation.into()),
        ],
        body: rendered.as_bytes().to_vec(),
    };
    let h = target.http_bind(endpoint, req).await?;
    let at = h.at_target.clone();
    let resp = h
        .send(target.def.timeout)
        .await
        .with_context(|| at.clone())?;
    if !(200..300).contains(&resp.status) {
        let msg = format!("{at}: status {}: {}", resp.status, resp.body_snippet());
        let transient = resp.status >= 500
            || matches!(resp.status, 408 | 429)
            || target.aws_throttled(&resp.headers, &resp.body);
        return Err(if transient {
            anyhow!(msg)
        } else {
            rejected(msg)
        });
    }
    if let Some(err) = function_error(&resp.headers) {
        return Err(rejected(format!(
            "{at}: the function raised an error ({err}): {}",
            resp.body_snippet()
        )));
    }
    match url.invocation {
        Invocation::Event => {
            if resp.status != 202 {
                bail!("{at}: an event invocation answers 202, not {}", resp.status);
            }
            Ok(Some(Vec::new()))
        }
        Invocation::Sync => {
            let verdict = target
                .expect
                .http(pool, resp.status, &resp.headers, &resp.body)
                .await
                .with_context(|| at.clone())?;
            if let Err(e) = verdict {
                return Err(rejected(format!("{at}: {e}: {}", resp.body_snippet())));
            }
            Ok(Some(lambda_failed_items(&resp.body).with_context(|| at)?))
        }
    }
}

/// How a function is invoked: the caller waits for its result, or the call is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invocation {
    /// `RequestResponse`: the answer is the function's result.
    Sync,
    /// `Event`: queued, answered with 202 before the function runs.
    Event,
}

/// The largest Invoke payload, in bytes, for a synchronous call: 6 MB. Lambda's MB is 1,024 KB.
/// Source: AWS Lambda quotas, "Invocation payload" (docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)
/// and the Invoke API, Request Body (docs.aws.amazon.com/lambda/latest/api/API_Invoke.html).
pub const LAMBDA_SYNC_PAYLOAD: usize = 6 * 1024 * 1024;
/// The largest Invoke payload for an asynchronous (`Event`) call: 1 MB. Same sources.
pub const LAMBDA_EVENT_PAYLOAD: usize = 1024 * 1024;

/// An `aws-lambda://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LambdaUrl {
    pub region: Option<String>,
    pub function: String,
    /// A version or alias (`live`), or `$LATEST`, `$LATEST.PUBLISHED`.
    pub qualifier: Option<String>,
    pub invocation: Invocation,
    /// `?endpoint=`, as written (a validated http(s) URL).
    pub endpoint: Option<String>,
}

impl LambdaUrl {
    /// `aws-lambda://<function>[:<qualifier>]` or `aws-lambda://<region>/<function>[:<qualifier>]`,
    /// with `?invocation=sync|event` and `?endpoint=`.
    pub fn parse(url: &str) -> Result<LambdaUrl> {
        let shown = crate::secrets::redact_url(url);
        // `invocation` is this kind's own parameter; `split_aws_url` takes `endpoint` only, so the
        // rest of the query is passed on to it.
        let (base, query) = match url.split_once('?') {
            Some((b, q)) => (b, q),
            None => (url, ""),
        };
        let mut invocation = None;
        let mut rest = Vec::new();
        for part in query.split('&').filter(|p| !p.is_empty()) {
            match part.strip_prefix("invocation=") {
                Some(v) => {
                    if invocation.is_some() {
                        bail!("`{shown}`: `invocation` is given twice");
                    }
                    invocation = Some(match v {
                        "sync" => Invocation::Sync,
                        "event" => Invocation::Event,
                        other => {
                            bail!("`{shown}`: `invocation` is `sync` or `event`, not `{other}`")
                        }
                    });
                }
                None => rest.push(part),
            }
        }
        let shared = if rest.is_empty() {
            base.to_string()
        } else {
            format!("{base}?{}", rest.join("&"))
        };
        let (segs, endpoint) = split_aws_url(&shared, "lambda")?;
        let (region, last) = match segs.as_slice() {
            [f] => (None, f.as_str()),
            [r, f] => (Some(r.as_str()), f.as_str()),
            _ => bail!(
                "`{shown}`: kind lambda takes `aws-lambda://<function-name>[:<qualifier>]` or `aws-lambda://<region>/<function-name>[:<qualifier>]`"
            ),
        };
        if let Some(r) = region {
            if !valid_region(r) {
                bail!("`{shown}`: `{r}` is not an AWS region name, e.g. eu-west-1");
            }
        }
        let (function, qualifier) = match last.split_once(':') {
            Some((f, q)) => (f, Some(q)),
            None => (last, None),
        };
        // A function name: 1 to 64 letters, digits, `-`, `_` or `.` (Invoke, FunctionName).
        let name_ok = (1..=64).contains(&function.len())
            && function
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'));
        if !name_ok {
            bail!("`{shown}`: a function name is 1 to 64 letters, digits, `-`, `_` or `.`, optionally followed by `:<qualifier>`");
        }
        // A qualifier: `$LATEST`, `$LATEST.PUBLISHED`, a version number or an alias (Invoke, Qualifier).
        if let Some(q) = qualifier {
            let q_ok = q == "$LATEST"
                || q == "$LATEST.PUBLISHED"
                || ((1..=128).contains(&q.len())
                    && q.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')));
            if !q_ok {
                bail!("`{shown}`: the qualifier is `$LATEST`, a version or an alias (letters, digits, `-`, `_`)");
            }
        }
        Ok(LambdaUrl {
            region: region.map(str::to_string),
            function: function.to_string(),
            qualifier: qualifier.map(str::to_string),
            invocation: invocation.unwrap_or(Invocation::Sync),
            endpoint,
        })
    }

    /// The largest payload this invocation takes (`LAMBDA_SYNC_PAYLOAD` or `LAMBDA_EVENT_PAYLOAD`).
    pub fn payload_limit(&self) -> usize {
        match self.invocation {
            Invocation::Sync => LAMBDA_SYNC_PAYLOAD,
            Invocation::Event => LAMBDA_EVENT_PAYLOAD,
        }
    }

    /// The Invoke request path: `/2015-03-31/functions/<function>/invocations`, with
    /// `?Qualifier=<qualifier>` when the URL gives one.
    pub fn path(&self) -> String {
        let mut p = format!("/2015-03-31/functions/{}/invocations", self.function);
        if let Some(q) = &self.qualifier {
            p.push_str("?Qualifier=");
            p.push_str(q);
        }
        p
    }
}

/// The `X-Amz-Function-Error` header of an Invoke answer (`Handled` or `Unhandled`), when the
/// function raised an error. Header names are matched case-insensitively.
pub fn function_error(headers: &[(String, String)]) -> Option<String> {
    headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("x-amz-function-error"))
        .map(|(_, v)| v.clone())
}

/// The item identifiers a function lists in `batchItemFailures`, the common partial-batch
/// answer: `{"batchItemFailures": [{"itemIdentifier": "<key>"}]}`. A result without the field
/// lists none (the whole batch is met); a malformed list is an error.
pub fn lambda_failed_items(body: &[u8]) -> Result<Vec<String>> {
    let Ok(Value::Object(obj)) = serde_json::from_slice::<Value>(body) else {
        return Ok(Vec::new());
    };
    let Some(list) = obj.get("batchItemFailures") else {
        return Ok(Vec::new());
    };
    let entries = list
        .as_array()
        .ok_or_else(|| anyhow!("`batchItemFailures` is not a list"))?;
    entries
        .iter()
        .map(|e| {
            e.get("itemIdentifier")
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    anyhow!("a `batchItemFailures` entry has no `itemIdentifier` string")
                })
        })
        .collect()
}
