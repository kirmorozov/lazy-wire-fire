//! AWS for the `http.s3`, `sqs` and `kinesis` targets: credentials and SigV4 signing.
//!
//! Credentials come from a target's references (`access_key_ref`, `secret_key_ref`,
//! `session_token_ref`), read on every request so that rotated mounted secrets are picked up.
//! A target without them uses aws-config's default credentials chain: the environment, the
//! shared credentials and config files, web identity (EKS IRSA), container credentials (ECS, EKS
//! Pod Identity) and EC2 instance metadata (IMDSv2, unless `AWS_EC2_METADATA_DISABLED=true`).
//! The chain's own HTTP calls use a rustls-ring client, so the build needs neither aws-lc, cmake
//! nor OpenSSL. Credentials that expire are fetched again five minutes before they do.
//!
//! Signing is aws-sigv4. lazywf canonicalises the target itself (`canonical_target`), and the
//! signer signs that wire form as it is sent, so the signed path is the path on the wire.

use std::time::{Duration, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::provider_config::ProviderConfig;
use aws_config::Region;
use aws_credential_types::provider::{error::CredentialsError, future, ProvideCredentials};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign as sigv4_sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4::SigningParams;
use aws_smithy_http_client::{tls, Builder as HttpClientBuilder};
use aws_smithy_runtime_api::client::identity::Identity;
use serde_json::Value;
use tokio::sync::{Mutex, OnceCell};

use crate::http::{BaseUrl, RawRequest};
use crate::secrets;

/// The target's references (`access_key_ref`, `secret_key_ref`, `session_token_ref`).
#[derive(Clone, PartialEq, Eq)]
pub struct Refs {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Refs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Refs")
            .field("access_key", &self.access_key)
            .field("secret_key", &"** redacted **")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "** redacted **"),
            )
            .finish()
    }
}

impl Refs {
    /// The credentials the references name, read now.
    fn resolve(&self) -> Result<Credentials> {
        let session_token = self
            .session_token
            .as_deref()
            .map(|t| secrets::resolve_reference("session_token_ref", t))
            .transpose()?;
        Ok(Credentials::new(
            secrets::resolve_reference("access_key_ref", &self.access_key)?,
            secrets::resolve_reference("secret_key_ref", &self.secret_key)?,
            session_token,
            None,
            "lazywf-references",
        ))
    }
}

/// The references as a credentials provider. Every call reads them again: a mounted secret
/// file that is rewritten is picked up at the next request.
impl ProvideCredentials for Refs {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::ready(self.resolve().map_err(CredentialsError::provider_error))
    }
}

/// Temporary credentials are fetched again this many seconds before they expire.
const REFRESH_BEFORE: u64 = 300;

/// The credentials of one target's requests: its references, or the default chain.
pub struct Provider {
    refs: Option<Refs>,
    region: String,
    /// aws-config's default chain, built on first use (the loader is asynchronous).
    chain: OnceCell<DefaultCredentialsChain>,
    /// The chain's last answer, while it is still good.
    cache: Mutex<Option<Credentials>>,
}

impl Provider {
    pub fn new(refs: Option<Refs>, region: &str) -> Provider {
        Provider {
            refs,
            region: region.to_string(),
            chain: OnceCell::new(),
            cache: Mutex::new(None),
        }
    }

    pub async fn get(&self) -> Result<Credentials> {
        if let Some(refs) = &self.refs {
            return refs.provide_credentials().await.map_err(credentials_error);
        }
        let mut cache = self.cache.lock().await;
        let now = crate::time::unix_now();
        if let Some(c) = cache.as_ref() {
            let fresh = c.expiry().is_none_or(|e| {
                e.duration_since(UNIX_EPOCH)
                    .is_ok_and(|d| d.as_secs() > now + REFRESH_BEFORE)
            });
            if fresh {
                return Ok(c.clone());
            }
        }
        let chain = self.chain.get_or_init(|| default_chain(&self.region)).await;
        let c = chain
            .provide_credentials()
            .await
            .map_err(missing_credentials)?;
        *cache = Some(c.clone());
        Ok(c)
    }
}

/// aws-config's default chain, over the rustls-ring client. Its own calls (web identity's STS)
/// take the region of the environment (`AWS_REGION`, `AWS_DEFAULT_REGION`), else the target's;
/// with neither, the profile's.
async fn default_chain(region: &str) -> DefaultCredentialsChain {
    let http = HttpClientBuilder::new()
        .tls_provider(tls::Provider::rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https();
    let builder = DefaultCredentialsChain::builder()
        .configure(ProviderConfig::empty().with_http_client(http));
    let region = env("AWS_REGION")
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .or_else(|| (!region.is_empty()).then(|| region.to_string()));
    match region {
        Some(r) => builder.region(Region::new(r)).build().await,
        None => builder.build().await,
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// An error and its causes, joined by `: `.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        msg.push_str(": ");
        msg.push_str(&c.to_string());
        cause = c.source();
    }
    msg
}

/// A failed credentials lookup, with the causes it carries.
fn credentials_error(e: CredentialsError) -> anyhow::Error {
    anyhow!("AWS credentials: {}", error_chain(&e))
}

/// The default chain found no credentials. The message names the sources it tried and the
/// references that take their place, and carries the chain's own error. Other failures (a
/// source that was tried and broke) keep their text.
fn missing_credentials(e: CredentialsError) -> anyhow::Error {
    if matches!(e, CredentialsError::CredentialsNotLoaded(_)) {
        anyhow!(
            "no AWS credentials: set access_key_ref / secret_key_ref in the target's vars, or provide them \
             through the environment (tried: environment, shared credentials and config files, web identity, \
             container credentials, EC2 instance metadata unless AWS_EC2_METADATA_DISABLED=true): {}",
            error_chain(&e)
        )
    } else {
        credentials_error(e)
    }
}

/// SigV4 URI encoding: unreserved characters stay, `/` stays unless `encode_slash`.
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'.' | b'_' | b'~')
            || (b == b'/' && !encode_slash)
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A percent-decoded (`%XX`, and `+` as a space) piece of a URL or a form body. `sns` reads its
/// rendered batch this way (`crate::sns::form_pairs`).
pub(crate) fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Path and query in canonical (and wire) form: each part decoded and re-encoded, the query
/// sorted.
fn canonical_target(target: &str) -> (String, String) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = path
        .split('/')
        .map(|seg| uri_encode(&percent_decode_path(seg), true))
        .collect::<Vec<_>>()
        .join("/");
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (
                uri_encode(&percent_decode(k), true),
                uri_encode(&percent_decode(v), true),
            )
        })
        .collect();
    pairs.sort();
    let query = pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    (if path.is_empty() { "/".into() } else { path }, query)
}

/// Like `percent_decode`, but `+` is a literal plus in a path.
fn percent_decode_path(s: &str) -> String {
    percent_decode(&s.replace('+', "%2B"))
}

/// Headers that are signed when present, besides `host` and every `x-amz-*`. This is the set
/// lazywf has always signed, so a request's signature does not depend on the signer.
const SIGNED: &[&str] = &["content-type", "content-md5", "range", "date"];

/// Signs `req` for `service` in `region`: rewrites the target in canonical form (prefixed by
/// `prefix`), and sets `Host`, `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token`
/// (with a session token) and `Authorization`.
pub fn sign(
    req: &mut RawRequest,
    host_header: &str,
    prefix: &str,
    region: &str,
    service: &str,
    creds: &Credentials,
    unix_now: u64,
) -> Result<()> {
    if creds.access_key_id().is_empty() || creds.secret_access_key().is_empty() {
        bail!("empty AWS credentials");
    }
    let (path, query) = canonical_target(&format!("{prefix}{}", req.target));
    req.target = if query.is_empty() {
        path
    } else {
        format!("{path}?{query}")
    };
    req.set_header("Host", host_header.to_string());
    // Computed by the signer below; whatever the request carries is discarded.
    for name in [
        "authorization",
        "x-amz-date",
        "x-amz-content-sha256",
        "x-amz-security-token",
    ] {
        req.remove_header(name);
    }
    let signed: Vec<(String, String)> = req
        .headers
        .iter()
        .filter_map(|(n, v)| {
            let n = n.to_ascii_lowercase();
            (n == "host" || n.starts_with("x-amz-") || SIGNED.contains(&n.as_str()))
                .then(|| (n, v.split_whitespace().collect::<Vec<_>>().join(" ")))
        })
        .collect();

    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    // The target is in wire form already (see `canonical_target`): sign it as it is sent, with
    // no second encoding, and no path normalisation (an S3 key may hold `.` or `..` segments).
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    let identity: Identity = creds.clone().into();
    let params: aws_sigv4::http_request::SigningParams<'_> = SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(service)
        .time(UNIX_EPOCH + Duration::from_secs(unix_now))
        .settings(settings)
        .build()
        .map_err(|e| anyhow!("SigV4 parameters: {e}"))?
        .into();

    let method = req.method.to_ascii_uppercase();
    let added: Vec<(String, String)> = {
        let request = SignableRequest::new(
            method.as_str(),
            req.target.as_str(),
            signed.iter().map(|(n, v)| (n.as_str(), v.as_str())),
            SignableBody::Bytes(req.body.as_slice()),
        )
        .map_err(|e| anyhow!("SigV4 request: {e}"))?;
        let (instructions, _signature) = sigv4_sign(request, &params)
            .map_err(|e| anyhow!("SigV4: {e}"))?
            .into_parts();
        instructions
            .headers()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    };
    for (name, value) in added {
        req.set_header(&name, value);
    }
    Ok(())
}

/// The regional endpoint of an AWS service: `https://<service>.<region>.amazonaws.com`.
pub fn endpoint(service: &str, region: &str) -> BaseUrl {
    BaseUrl {
        secure: true,
        host: format!("{service}.{region}.amazonaws.com"),
        port: 443,
        base: String::new(),
    }
}

// ---- `aws-sqs://` and `aws-kinesis://` endpoints ----
//
// `aws-sqs://[<region>/]<account-id>/<queue-name>` and `aws-kinesis://[<region>/]<stream-name>`,
// each with an optional `?endpoint=http(s)://host:port` (emulators, VPC endpoints). The region is
// in the URL, or else from the default chain (`region_from`). Credentials never go in the URL.

/// The scheme of a URL (`aws-sqs` for `aws-sqs://…`), when it has one.
pub fn scheme_of(url: &str) -> Option<&str> {
    let (scheme, _) = url.split_once("://")?;
    let valid = !scheme.is_empty()
        && scheme
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'.');
    valid.then_some(scheme)
}

/// `aws-sqs://…` for kind sqs and `aws-kinesis://…` for kind kinesis (`service` is both the
/// kind name and the scheme suffix). Any other URL is refused, with the reason.
fn scheme_error(url: &str, service: &str) -> anyhow::Error {
    let shown = crate::secrets::redact_url(url);
    let want = format!("aws-{service}://");
    if url.starts_with("https://") || url.starts_with("http://") {
        anyhow!("kind {service} takes `{want}` endpoints, not a plain http(s) URL")
    } else if let Some(other) = scheme_of(url).filter(|s| s.starts_with("aws-")) {
        anyhow!("kind {service} takes `{want}` endpoints, not `{other}://`")
    } else {
        anyhow!("kind {service} takes `{want}` endpoints, not `{shown}`")
    }
}

/// The path segments of an `aws-<service>://` URL, and its `?endpoint=` override (validated).
fn split_aws_url(url: &str, service: &str) -> Result<(Vec<String>, Option<String>)> {
    let shown = crate::secrets::redact_url(url);
    let rest = url
        .strip_prefix(&format!("aws-{service}://"))
        .ok_or_else(|| scheme_error(url, service))?;
    let (path, query) = match rest.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (rest, None),
    };
    if path.contains('@') {
        bail!("`{shown}`: credentials belong in the target's vars (secret references), never in the URL");
    }
    if path.is_empty() || path.split('/').any(str::is_empty) {
        bail!("`{shown}`: the path has an empty segment");
    }
    let mut endpoint = None;
    for part in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let Some(value) = part.strip_prefix("endpoint=") else {
            let name = part.split('=').next().unwrap_or(part);
            bail!("`{shown}`: unknown query parameter `{name}` (the only one is `endpoint`)");
        };
        if endpoint.is_some() {
            bail!("`{shown}`: `endpoint` is given twice");
        }
        let value = percent_decode(value);
        let base = BaseUrl::parse(&value)
            .with_context(|| format!("`{shown}`: `endpoint` must be http(s)://host:port"))?;
        if !base.base.is_empty() {
            bail!("`{shown}`: `endpoint` must be http(s)://host:port, without a path");
        }
        endpoint = Some(value);
    }
    Ok((path.split('/').map(str::to_string).collect(), endpoint))
}

fn valid_region(r: &str) -> bool {
    // `eu-west-1`, `us-gov-west-1`: lowercase letters and digits, hyphens, ending in a digit.
    r.contains('-')
        && r.starts_with(|c: char| c.is_ascii_lowercase())
        && r.ends_with(|c: char| c.is_ascii_digit())
        && r.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

/// The regional address of a service, or the `?endpoint=` override when the URL gives one.
pub fn address(service: &str, endpoint_override: Option<&str>, region: &str) -> Result<BaseUrl> {
    match endpoint_override {
        Some(e) => BaseUrl::parse(e),
        None => Ok(endpoint(service, region)),
    }
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

// ---- `aws-eventbridge://` endpoints ----
//
// `aws-eventbridge://[<region>/]<event-bus-name-or-arn>`, with the usual optional `?endpoint=`
// (emulators, VPC endpoints). PutEvents is a JSON 1.1 call to the service root: `X-Amz-Target:
// AWSEvents.PutEvents`, `Content-Type: application/x-amz-json-1.1`, body `{"Entries": [...]}`;
// SigV4 signs with service `events`, the host is `events.<region>.amazonaws.com`.
// Source: PutEvents API (docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html)
// and the service endpoints reference (`events.<region>.amazonaws.com`).

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

// ---- `aws-sns://` endpoints ----
//
// `aws-sns://[<region>/]<account-id>/<topic-name>`, with the usual optional `?endpoint=`
// (emulators). `PublishBatch` is a query-protocol call to the service root
// (`Action=PublishBatch&Version=2010-03-31`, the `param.n` notation with `n` from 1), so nothing
// in the URL is a path: the topic is named by its ARN, as `SqsUrl` names the queue URL. SigV4
// signs with service `sns`, the host is `sns.<region>.amazonaws.com`.
// Source: PublishBatch API (docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html).

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

// ---- `aws-lambda://` endpoints ----
//
// `aws-lambda://[<region>/]<function-name>[:<qualifier>]`, with `?invocation=sync` (the default,
// RequestResponse) or `?invocation=event` (Event), and the same optional `?endpoint=`. The region
// and credential rules are those of `aws-sqs://`.

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

#[cfg(test)]
#[path = "tests/aws_lambda_tests.rs"]
mod lambda_tests;

/// The region of an AWS endpoint: the URL's, else `AWS_REGION`, else `AWS_DEFAULT_REGION`, else
/// `region` in the shared config file's profile (`config`, read only when the others are unset).
pub fn region_from(
    from_url: Option<&str>,
    env_var: impl Fn(&str) -> Option<String>,
    config: impl FnOnce() -> Option<String>,
) -> Option<String> {
    from_url
        .map(str::to_string)
        .or_else(|| env_var("AWS_REGION"))
        .or_else(|| env_var("AWS_DEFAULT_REGION"))
        .or_else(config)
}

/// The region of an AWS endpoint, resolved through `region_from` with the process environment.
pub fn resolve_region(from_url: Option<&str>) -> Result<String> {
    region_from(from_url, env, || {
        let profile = env("AWS_PROFILE").unwrap_or_else(|| "default".into());
        config_region(&config_file_text()?, &profile)
    })
    .ok_or_else(|| {
        anyhow!(
            "no AWS region: give it in the endpoint (`aws-sqs://<region>/…` or `aws-kinesis://<region>/…`), \
             or set AWS_REGION or AWS_DEFAULT_REGION (or `region` in the profile of the shared config file)"
        )
    })
}

fn config_file_text() -> Option<String> {
    let path = match env("AWS_CONFIG_FILE") {
        Some(p) => p,
        None => format!("{}/.aws/config", env("HOME")?),
    };
    std::fs::read_to_string(path).ok()
}

/// `region` of `profile` in the text of a shared config file (`[default]`, `[profile <name>]`).
pub fn config_region(text: &str, profile: &str) -> Option<String> {
    let wanted = if profile == "default" {
        "default".to_string()
    } else {
        format!("profile {profile}")
    };
    let mut in_profile = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with(';') || line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            in_profile = name.trim() == wanted;
            continue;
        }
        if let (true, Some((k, v))) = (in_profile, line.split_once('=')) {
            if k.trim().eq_ignore_ascii_case("region") && !v.trim().is_empty() {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

/// The error code of an AWS answer: the `x-amzn-ErrorType` header, else the `__type` field of a
/// JSON body, else the `<Code>` of the query protocol's error document (which carries no
/// `x-amzn-ErrorType` header at all: SNS and SQS answer a refusal as
/// `<ErrorResponse><Error><Code>Throttling</Code>…`). Without the last one a throttled batch
/// request would read as a refusal of its keys rather than as "not now". The namespace
/// (`com.amazonaws.sqs#`) and any `:<uri>` suffix are dropped.
pub fn error_code(headers: &[(String, String)], body: &[u8]) -> Option<String> {
    let raw = headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("x-amzn-errortype"))
        .map(|(_, v)| v.clone())
        .or_else(|| {
            serde_json::from_slice::<Value>(body)
                .ok()?
                .get("__type")?
                .as_str()
                .map(str::to_string)
        })
        .or_else(|| query_error_code(body))?;
    let code = raw.split(':').next()?.rsplit('#').next()?.trim();
    (!code.is_empty()).then(|| code.to_string())
}

/// The `<Code>` of the query protocol's error document, whose text the code is a word of
/// (`NotFound`, `Throttling`): the first `<Code>` inside the `<Error>` element is it. Only a
/// document that opens an `<Error>` element counts, so the `Code` of a `BatchResultErrorEntry`
/// in an answer that succeeded is not read as one. Source: AWS General Reference, errors of the
/// query protocol (docs.aws.amazon.com/general/latest/gr/query-error-responses.html).
fn query_error_code(body: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(body);
    let start = text.find("<Error")?;
    let text = &text[start..];
    let open = text.find("<Code>")? + "<Code>".len();
    let close = text[open..].find("</Code>")?;
    let code = text[open..open + close].trim();
    (!code.is_empty()).then(|| code.to_string())
}

/// AWS error codes that say "not now" (throttling, or the service is unavailable), whatever the
/// HTTP status they come with. Sources (AWS API reference, Common Errors): `ThrottlingException`
/// and `ServiceUnavailable` (all AWS services); `InternalFailure` (all services, HTTP 500);
/// `ProvisionedThroughputExceededException` (Kinesis PutRecords, HTTP 400 in its error list);
/// `RequestThrottled` (SQS SendMessageBatch, HTTP 400: "The request was denied due to request
/// throttling. Exceeds the permitted request rate for the queue or for the recipient of the
/// request."). Source: docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
/// `InternalError` ("Indicates an internal service error") and `KMSThrottling` ("The request was
/// denied due to request throttling") are the two of SNS `PublishBatch` that answer with a code
/// of their own rather than one of the common ones, and its `KMSThrottling` comes as HTTP 400, so
/// without them a throttled batch would read as a refusal of every key in it. Source:
/// docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html (Errors).
pub fn transient_code(code: &str) -> bool {
    matches!(
        code,
        "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "RequestThrottled"
            | "ServiceUnavailable"
            | "InternalFailure"
            | "InternalError"
            | "KMSThrottling"
    )
}

#[cfg(test)]
#[path = "tests/aws_provider_tests.rs"]
mod provider_tests;

#[cfg(test)]
mod sigv4_tests {
    use super::*;

    fn creds() -> Credentials {
        Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "test",
        )
    }

    // Examples from the AWS S3 docs ("Signature Calculations for the Authorization Header:
    // Transferring Payload in a Single Chunk").
    #[test]
    fn aws_get_object_example() {
        let mut r = RawRequest::parse("GET /test.txt HTTP/1.1\nRange: bytes=0-9\n\n")
            .unwrap()
            .unwrap();
        sign(
            &mut r,
            "examplebucket.s3.amazonaws.com",
            "",
            "us-east-1",
            "s3",
            &creds(),
            1_369_353_600,
        )
        .unwrap();
        assert_eq!(
            r.header("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn aws_put_object_example() {
        let mut r = RawRequest::parse(
            "PUT /test$file.text HTTP/1.1\nDate: Fri, 24 May 2013 00:00:00 GMT\nx-amz-storage-class: REDUCED_REDUNDANCY\n\nWelcome to Amazon S3.",
        )
        .unwrap()
        .unwrap();
        sign(
            &mut r,
            "examplebucket.s3.amazonaws.com",
            "",
            "us-east-1",
            "s3",
            &creds(),
            1_369_353_600,
        )
        .unwrap();
        assert_eq!(r.target, "/test%24file.text");
        assert!(r.header("authorization").unwrap().ends_with(
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, \
             Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        ));
    }

    #[test]
    fn aws_list_objects_example() {
        let mut r = RawRequest::parse("GET /?max-keys=2&prefix=J HTTP/1.1\n\n")
            .unwrap()
            .unwrap();
        sign(
            &mut r,
            "examplebucket.s3.amazonaws.com",
            "",
            "us-east-1",
            "s3",
            &creds(),
            1_369_353_600,
        )
        .unwrap();
        assert!(r.header("authorization").unwrap().ends_with(
            "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        ));
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    fn h(name: &str, value: &str) -> Vec<(String, String)> {
        vec![(name.to_string(), value.to_string())]
    }

    #[test]
    fn codes_from_body_and_header() {
        assert_eq!(
            error_code(&[], br#"{"__type":"com.amazonaws.sqs#RequestThrottled"}"#).as_deref(),
            Some("RequestThrottled")
        );
        assert_eq!(
            error_code(
                &h("X-Amzn-ErrorType", "ProvisionedThroughputExceededException:http://internal.amazon.com/coral/com.amazon.coral.service/"),
                b"{}"
            )
            .as_deref(),
            Some("ProvisionedThroughputExceededException")
        );
        assert_eq!(
            error_code(&[], br#"{"__type":"ThrottlingException","message":"x"}"#).as_deref(),
            Some("ThrottlingException")
        );
        assert_eq!(error_code(&[], b"<Error/>"), None);
        assert_eq!(error_code(&[], br#"{"message":"no type"}"#), None);
        // The query protocol's error document: no header, the code in the XML.
        assert_eq!(
            error_code(
                &[],
                br#"<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><Error><Type>Sender</Type><Code>ThrottlingException</Code><Message>Rate exceeded</Message></Error><RequestId>x</RequestId></ErrorResponse>"#
            )
            .as_deref(),
            Some("ThrottlingException")
        );
        // The `Code` of a `Failed` entry of an answer that did answer is not an error code: the
        // document opens no `<Error>` element.
        assert_eq!(
            error_code(
                &[],
                br#"<PublishBatchResponse><PublishBatchResult><Failed><member><Id>1</Id><Code>InvalidParameter</Code><SenderFault>true</SenderFault></member></Failed></PublishBatchResult></PublishBatchResponse>"#
            ),
            None
        );
    }

    #[test]
    fn throttling_codes_are_transient() {
        for c in [
            "ThrottlingException",
            "ProvisionedThroughputExceededException",
            "RequestThrottled",
            "ServiceUnavailable",
            "InternalFailure",
            // SNS PublishBatch's own two (aws::transient_code).
            "InternalError",
            "KMSThrottling",
        ] {
            assert!(transient_code(c), "{c}");
        }
        for c in [
            "AccessDeniedException",
            "ValidationError",
            "InvalidParameterValue",
            "SlowDown",
            // What the `sns` kind refuses: a clear no about this batch.
            "InvalidParameter",
            "NotFound",
            "TooManyEntriesInBatchRequest",
            "BatchRequestTooLong",
            "KMSAccessDenied",
        ] {
            assert!(!transient_code(c), "{c}");
        }
    }

    #[test]
    fn regional_endpoint() {
        let e = endpoint("sqs", "eu-west-1");
        assert_eq!(e.url(), "https://sqs.eu-west-1.amazonaws.com");
    }

    #[test]
    fn canonical_target_moved() {
        assert_eq!(
            canonical_target("/a b/c%2Fd+e?z=1&a=x%20y&b"),
            ("/a%20b/c%2Fd%2Be".into(), "a=x%20y&b=&z=1".into())
        );
    }
}

#[cfg(test)]
#[path = "tests/aws_url_tests.rs"]
mod url_tests;

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn debug_masks_the_secret_key_and_the_session_token() {
        let refs = Refs {
            access_key: "env:AK-NAME".into(),
            secret_key: "SK-LITERAL-5".into(),
            session_token: Some("ST-LITERAL-6".into()),
        };
        let shown = format!("{refs:?}");
        assert!(!shown.contains("SK-LITERAL-5"), "{shown}");
        assert!(!shown.contains("ST-LITERAL-6"), "{shown}");
        assert!(shown.contains("env:AK-NAME"), "{shown}");
    }
}
