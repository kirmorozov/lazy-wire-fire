//! AWS credentials for `http.s3` targets: the target's secret references, or the default
//! credential chain of the environment.
//!
//! The chain, in order (static profiles only, no SSO or role chaining from config files):
//!
//! 1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`;
//! 2. the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, default
//!    `~/.aws/credentials`), profile `AWS_PROFILE` (default `default`);
//! 3. web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, e.g. EKS IRSA) through
//!    STS `AssumeRoleWithWebIdentity`;
//! 4. container credentials (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or `_FULL_URI`, with
//!    `AWS_CONTAINER_AUTHORIZATION_TOKEN[_FILE]`; ECS, EKS Pod Identity);
//! 5. EC2 instance metadata (IMDSv2), unless `AWS_EC2_METADATA_DISABLED=true`.
//!
//! Temporary credentials are cached until five minutes before they expire.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::http::{BaseUrl, Client, RawRequest};
use crate::secrets;
use crate::tls::TlsSpec;

#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    /// Unix seconds.
    pub expires: Option<u64>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("expires", &self.expires)
            .finish_non_exhaustive()
    }
}

/// The target's references (`access_key_ref`, `secret_key_ref`, `session_token_ref`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refs {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

pub struct Provider {
    refs: Option<Refs>,
    region: String,
    cache: Mutex<Option<Credentials>>,
}

const REFRESH_BEFORE: u64 = 300;

impl Provider {
    pub fn new(refs: Option<Refs>, region: &str) -> Provider {
        Provider {
            refs,
            region: region.to_string(),
            cache: Mutex::new(None),
        }
    }

    pub async fn get(&self) -> Result<Credentials> {
        if let Some(r) = &self.refs {
            // Read every time: mounted secret files rotate.
            return Ok(Credentials {
                access_key: secrets::resolve_reference("access_key_ref", &r.access_key)?,
                secret_key: secrets::resolve_reference("secret_key_ref", &r.secret_key)?,
                session_token: r
                    .session_token
                    .as_deref()
                    .map(|t| secrets::resolve_reference("session_token_ref", t))
                    .transpose()?,
                expires: None,
            });
        }
        let mut cache = self.cache.lock().await;
        let now = crate::time::unix_now();
        if let Some(c) = cache.as_ref() {
            if c.expires.is_none_or(|e| e > now + REFRESH_BEFORE) {
                return Ok(c.clone());
            }
        }
        let c = chain(&self.region).await?;
        *cache = Some(c.clone());
        Ok(c)
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

async fn chain(region: &str) -> Result<Credentials> {
    let mut tried = Vec::new();
    if let (Some(a), Some(s)) = (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
        return Ok(Credentials {
            access_key: a,
            secret_key: s,
            session_token: env("AWS_SESSION_TOKEN"),
            expires: None,
        });
    }
    tried.push("environment");
    match profile_file() {
        Ok(Some(c)) => return Ok(c),
        Ok(None) => tried.push("shared credentials file"),
        Err(e) => return Err(e),
    }
    if let (Some(token_file), Some(role)) =
        (env("AWS_WEB_IDENTITY_TOKEN_FILE"), env("AWS_ROLE_ARN"))
    {
        return web_identity(&token_file, &role, region)
            .await
            .context("web identity (STS AssumeRoleWithWebIdentity)");
    }
    tried.push("web identity");
    if env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").is_some()
        || env("AWS_CONTAINER_CREDENTIALS_FULL_URI").is_some()
    {
        return container().await.context("container credentials endpoint");
    }
    tried.push("container");
    if !env("AWS_EC2_METADATA_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        match imds().await {
            Ok(c) => return Ok(c),
            Err(e) => tracing::debug!("EC2 instance metadata: {e:#}"),
        }
        tried.push("EC2 instance metadata");
    }
    bail!(
        "no AWS credentials: set access_key_ref / secret_key_ref in the target's vars, or provide them \
         through the environment (tried: {})",
        tried.join(", ")
    )
}

fn profile_file() -> Result<Option<Credentials>> {
    let path = match env("AWS_SHARED_CREDENTIALS_FILE") {
        Some(p) => p,
        None => match env("HOME") {
            Some(h) => format!("{h}/.aws/credentials"),
            None => return Ok(None),
        },
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    let profile = env("AWS_PROFILE").unwrap_or_else(|| "default".into());
    let mut in_profile = false;
    let (mut a, mut s, mut t) = (None, None, None);
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with(';') || line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            in_profile = name.trim() == profile;
            continue;
        }
        if !in_profile {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let v = Some(v.trim().to_string());
            match k.trim().to_ascii_lowercase().as_str() {
                "aws_access_key_id" => a = v,
                "aws_secret_access_key" => s = v,
                "aws_session_token" => t = v,
                _ => {}
            }
        }
    }
    Ok(match (a, s) {
        (Some(access_key), Some(secret_key)) => Some(Credentials {
            access_key,
            secret_key,
            session_token: t,
            expires: None,
        }),
        _ => None,
    })
}

async fn get(
    url: &str,
    headers: &[(&str, String)],
    method: &str,
    timeout: Duration,
) -> Result<(u16, Vec<u8>)> {
    let (base, path) = match url
        .find("://")
        .and_then(|i| url[i + 3..].find('/').map(|j| i + 3 + j))
    {
        Some(i) => (&url[..i], &url[i..]),
        None => (url, "/"),
    };
    let base = BaseUrl::parse(base)?;
    let client = Client::new(base.clone(), &TlsSpec::parse(None)?)?;
    let mut req = RawRequest {
        method: method.into(),
        target: path.to_string(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    req.set_header("Host", base.host_header());
    for (n, v) in headers {
        req.set_header(n, v.clone());
    }
    let r = client.send(&req, timeout).await?;
    Ok((r.status, r.body))
}

fn from_json(body: &[u8], what: &str) -> Result<Credentials> {
    let v: Value =
        serde_json::from_slice(body).with_context(|| format!("{what}: response is not JSON"))?;
    let s = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    Ok(Credentials {
        access_key: s("AccessKeyId").ok_or_else(|| anyhow!("{what}: no AccessKeyId"))?,
        secret_key: s("SecretAccessKey").ok_or_else(|| anyhow!("{what}: no SecretAccessKey"))?,
        session_token: s("Token"),
        expires: s("Expiration").and_then(|e| crate::time::parse_iso8601(&e)),
    })
}

fn xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{tag}>"))? + start;
    Some(xml[start..end].trim())
}

async fn web_identity(token_file: &str, role: &str, region: &str) -> Result<Credentials> {
    let token =
        std::fs::read_to_string(token_file).with_context(|| format!("reading `{token_file}`"))?;
    let region = env("AWS_REGION")
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| region.to_string());
    let session = env("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| "lazywf".into());
    let enc = |s: &str| crate::template::url_encode(s);
    let url = format!(
        "https://sts.{region}.amazonaws.com/?Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}&RoleSessionName={}&WebIdentityToken={}",
        enc(role),
        enc(&session),
        enc(token.trim())
    );
    let (status, body) = get(
        &url,
        &[("Accept", "application/xml".into())],
        "GET",
        Duration::from_secs(10),
    )
    .await?;
    let xml = String::from_utf8_lossy(&body);
    if status != 200 {
        bail!(
            "STS answered {status}: {}",
            xml_tag(&xml, "Message").unwrap_or(&xml)
        );
    }
    Ok(Credentials {
        access_key: xml_tag(&xml, "AccessKeyId")
            .ok_or_else(|| anyhow!("STS: no AccessKeyId"))?
            .to_string(),
        secret_key: xml_tag(&xml, "SecretAccessKey")
            .ok_or_else(|| anyhow!("STS: no SecretAccessKey"))?
            .to_string(),
        session_token: xml_tag(&xml, "SessionToken").map(str::to_string),
        expires: xml_tag(&xml, "Expiration").and_then(crate::time::parse_iso8601),
    })
}

async fn container() -> Result<Credentials> {
    let url = match env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
        Some(rel) => format!("http://169.254.170.2{rel}"),
        None => env("AWS_CONTAINER_CREDENTIALS_FULL_URI").expect("checked by the caller"),
    };
    let token = match env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
        Some(f) => Some(
            std::fs::read_to_string(&f)
                .with_context(|| format!("reading `{f}`"))?
                .trim()
                .to_string(),
        ),
        None => env("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
    };
    let headers: Vec<(&str, String)> = token.into_iter().map(|t| ("Authorization", t)).collect();
    let (status, body) = get(&url, &headers, "GET", Duration::from_secs(5)).await?;
    if status != 200 {
        bail!("{url} answered {status}");
    }
    from_json(&body, "container credentials")
}

async fn imds() -> Result<Credentials> {
    const BASE: &str = "http://169.254.169.254";
    let t = Duration::from_secs(1);
    let (status, token) = get(
        &format!("{BASE}/latest/api/token"),
        &[("X-aws-ec2-metadata-token-ttl-seconds", "21600".into())],
        "PUT",
        t,
    )
    .await?;
    if status != 200 {
        bail!("IMDSv2 token request answered {status}");
    }
    let tok = vec![(
        "X-aws-ec2-metadata-token",
        String::from_utf8_lossy(&token).trim().to_string(),
    )];
    let (status, role) = get(
        &format!("{BASE}/latest/meta-data/iam/security-credentials/"),
        &tok,
        "GET",
        t,
    )
    .await?;
    if status != 200 {
        bail!("no instance role ({status})");
    }
    let role = String::from_utf8_lossy(&role)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    let (status, body) = get(
        &format!("{BASE}/latest/meta-data/iam/security-credentials/{role}"),
        &tok,
        "GET",
        t,
    )
    .await?;
    if status != 200 {
        bail!("instance role credentials answered {status}");
    }
    from_json(&body, "instance metadata")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_and_xml() {
        let c = from_json(
            br#"{"AccessKeyId":"A","SecretAccessKey":"S","Token":"T","Expiration":"2030-01-01T00:00:00Z"}"#,
            "t",
        )
        .unwrap();
        assert_eq!(
            (c.access_key.as_str(), c.session_token.as_deref()),
            ("A", Some("T"))
        );
        assert_eq!(
            c.expires,
            crate::time::parse_iso8601("2030-01-01T00:00:00Z")
        );
        let xml = "<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId> AK </AccessKeyId></Credentials></AssumeRoleWithWebIdentityResponse>";
        assert_eq!(xml_tag(xml, "AccessKeyId"), Some("AK"));
        assert_eq!(xml_tag(xml, "Nope"), None);
    }
}

// ---- SigV4 (generic over the AWS service; moved from s3.rs) ----

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

fn percent_decode(s: &str) -> String {
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

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    inner.update(msg);
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// Headers that are signed when present, besides `host` and every `x-amz-*`.
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
    if creds.access_key.is_empty() || creds.secret_key.is_empty() {
        bail!("empty AWS credentials");
    }
    let (date, stamp) = crate::time::amz_dates(unix_now);
    let (path, query) = canonical_target(&format!("{prefix}{}", req.target));
    req.target = if query.is_empty() {
        path.clone()
    } else {
        format!("{path}?{query}")
    };
    let payload = hex::encode(Sha256::digest(&req.body));
    req.set_header("Host", host_header.to_string());
    req.set_header("x-amz-date", stamp.clone());
    req.set_header("x-amz-content-sha256", payload.clone());
    match &creds.session_token {
        Some(t) => req.set_header("x-amz-security-token", t.clone()),
        None => req.remove_header("x-amz-security-token"),
    }
    req.remove_header("authorization");
    let mut signed: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(n, v)| {
            (
                n.to_ascii_lowercase(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .filter(|(n, _)| n == "host" || n.starts_with("x-amz-") || SIGNED.contains(&n.as_str()))
        .collect();
    signed.sort();
    let canonical_headers: String = signed.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let signed_names = signed
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{signed_names}\n{payload}",
        req.method.to_ascii_uppercase()
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", creds.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, to_sign.as_bytes()));
    req.set_header(
        "Authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, Signature={signature}",
            creds.access_key
        ),
    );
    Ok(())
}

#[cfg(test)]
mod sigv4_tests {
    use super::*;

    fn creds() -> Credentials {
        Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
            expires: None,
        }
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

    #[test]
    fn hmac_rfc4231_case2() {
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
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
    let want = format!("aws-{service}://");
    if url.starts_with("https://") || url.starts_with("http://") {
        anyhow!("kind {service} takes `{want}` endpoints, not a plain http(s) URL")
    } else if let Some(other) = scheme_of(url).filter(|s| s.starts_with("aws-")) {
        anyhow!("kind {service} takes `{want}` endpoints, not `{other}://`")
    } else {
        anyhow!("kind {service} takes `{want}` endpoints, not `{url}`")
    }
}

/// The path segments of an `aws-<service>://` URL, and its `?endpoint=` override (validated).
fn split_aws_url(url: &str, service: &str) -> Result<(Vec<String>, Option<String>)> {
    let rest = url
        .strip_prefix(&format!("aws-{service}://"))
        .ok_or_else(|| scheme_error(url, service))?;
    let (path, query) = match rest.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (rest, None),
    };
    if path.contains('@') {
        bail!("`{url}`: credentials belong in the target's vars (secret references), never in the URL");
    }
    if path.is_empty() || path.split('/').any(str::is_empty) {
        bail!("`{url}`: the path has an empty segment");
    }
    let mut endpoint = None;
    for part in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let Some(value) = part.strip_prefix("endpoint=") else {
            let name = part.split('=').next().unwrap_or(part);
            bail!("`{url}`: unknown query parameter `{name}` (the only one is `endpoint`)");
        };
        if endpoint.is_some() {
            bail!("`{url}`: `endpoint` is given twice");
        }
        let value = percent_decode(value);
        let base = BaseUrl::parse(&value)
            .with_context(|| format!("`{url}`: `endpoint` must be http(s)://host:port"))?;
        if !base.base.is_empty() {
            bail!("`{url}`: `endpoint` must be http(s)://host:port, without a path");
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
        let (segs, endpoint) = split_aws_url(url, "sqs")?;
        let (region, account, queue) = match segs.as_slice() {
            [a, q] => (None, a, q),
            [r, a, q] => (Some(r), a, q),
            _ => bail!(
                "`{url}`: kind sqs takes `aws-sqs://<account-id>/<queue-name>` or `aws-sqs://<region>/<account-id>/<queue-name>`"
            ),
        };
        if let Some(r) = region {
            if !valid_region(r) {
                bail!("`{url}`: `{r}` is not an AWS region name, e.g. eu-west-1");
            }
        }
        if account.len() != 12 || !account.bytes().all(|c| c.is_ascii_digit()) {
            bail!("`{url}`: the account id is twelve digits");
        }
        let stem = queue.strip_suffix(".fifo").unwrap_or(queue);
        let name_ok = !stem.is_empty()
            && queue.len() <= 80
            && stem
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-');
        if !name_ok {
            bail!("`{url}`: the queue name is 1 to 80 letters, digits, `-` or `_`, optionally ending in `.fifo`");
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
        let (segs, endpoint) = split_aws_url(url, "kinesis")?;
        let (region, stream) = match segs.as_slice() {
            [s] => (None, s),
            [r, s] => (Some(r), s),
            _ => bail!(
                "`{url}`: kind kinesis takes `aws-kinesis://<stream-name>` or `aws-kinesis://<region>/<stream-name>`"
            ),
        };
        if let Some(r) = region {
            if !valid_region(r) {
                bail!("`{url}`: `{r}` is not an AWS region name, e.g. eu-west-1");
            }
        }
        let ok = (1..=128).contains(&stream.len())
            && stream
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'));
        if !ok {
            bail!("`{url}`: a stream name is 1 to 128 letters, digits, `-`, `_` or `.`");
        }
        Ok(KinesisUrl {
            region: region.cloned(),
            stream: stream.clone(),
            endpoint,
        })
    }
}

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

/// The error code of an AWS JSON-protocol answer: the `x-amzn-ErrorType` header, else the
/// `__type` field of a JSON body. The namespace (`com.amazonaws.sqs#`) and any `:<uri>` suffix
/// are dropped.
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
        })?;
    let code = raw.split(':').next()?.rsplit('#').next()?.trim();
    (!code.is_empty()).then(|| code.to_string())
}

/// AWS error codes that say "not now" (throttling, or the service is unavailable), whatever the
/// HTTP status they come with. Sources (AWS API reference, Common Errors): `ThrottlingException`
/// and `ServiceUnavailable` (all AWS services); `InternalFailure` (all services, HTTP 500);
/// `ProvisionedThroughputExceededException` (Kinesis PutRecords, HTTP 400 in its error list);
/// `RequestThrottled` (SQS SendMessageBatch, HTTP 400: "The request was denied due to request
/// throttling. Exceeds the permitted request rate for the queue or for the recipient of the
/// request."). Source: docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
pub fn transient_code(code: &str) -> bool {
    matches!(
        code,
        "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "RequestThrottled"
            | "ServiceUnavailable"
            | "InternalFailure"
    )
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
    }

    #[test]
    fn throttling_codes_are_transient() {
        for c in [
            "ThrottlingException",
            "ProvisionedThroughputExceededException",
            "RequestThrottled",
            "ServiceUnavailable",
            "InternalFailure",
        ] {
            assert!(transient_code(c), "{c}");
        }
        for c in [
            "AccessDeniedException",
            "ValidationError",
            "InvalidParameterValue",
            "SlowDown",
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
mod url_tests {
    use super::*;

    fn sqs(url: &str) -> Result<SqsUrl> {
        SqsUrl::parse(url)
    }

    fn kin(url: &str) -> Result<KinesisUrl> {
        KinesisUrl::parse(url)
    }

    #[test]
    fn sqs_url_forms() {
        let u = sqs("aws-sqs://123456789012/orders").unwrap();
        assert_eq!(u.region, None);
        assert_eq!(
            (u.account.as_str(), u.queue.as_str()),
            ("123456789012", "orders")
        );
        assert_eq!(u.endpoint, None);
        assert!(!u.fifo());
        let u = sqs("aws-sqs://eu-west-1/123456789012/orders.fifo").unwrap();
        assert_eq!(u.region.as_deref(), Some("eu-west-1"));
        assert!(u.fifo());
        // the emulator's override, raw and percent-encoded
        let u = sqs("aws-sqs://us-east-1/000000000000/q?endpoint=http://127.0.0.1:34561").unwrap();
        assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34561"));
        let u = sqs("aws-sqs://us-east-1/000000000000/q?endpoint=http%3A%2F%2F127.0.0.1%3A34561")
            .unwrap();
        assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34561"));
        // a VPC endpoint is https, with a trailing slash allowed
        assert!(
            sqs("aws-sqs://eu-west-1/123456789012/q?endpoint=https://vpce.example:443/").is_ok()
        );
    }

    #[test]
    fn sqs_refuses_bad_urls() {
        for url in [
            // the scheme: aws-sqs:// only
            "https://sqs.eu-west-1.amazonaws.com/123456789012/orders",
            "aws-kinesis://eu-west-1/orders",
            "sqs://123456789012/orders",
            // the path: two or three segments, no empty ones
            "aws-sqs://orders",
            "aws-sqs://eu-west-1/123456789012/a/b",
            "aws-sqs://123456789012/orders/",
            "aws-sqs://",
            // the account and the region
            "aws-sqs://12345678901/orders",
            "aws-sqs://eu-west-1/12345678901a/orders",
            "aws-sqs://EU-WEST-1/123456789012/orders",
            "aws-sqs://arn:aws:sqs:eu-west-1/123456789012/orders",
            // the queue name
            "aws-sqs://123456789012/.fifo",
            "aws-sqs://123456789012/a.b",
            "aws-sqs://123456789012/x.fifo.fifo",
            // userinfo, in the path or in the override
            "aws-sqs://u:p@123456789012/orders",
            "aws-sqs://123456789012/orders?endpoint=http://u:p@127.0.0.1:34561",
            // the query: `endpoint` only, once, http(s), no path
            "aws-sqs://123456789012/orders?region=eu-west-1",
            "aws-sqs://123456789012/orders?endpoint=ftp://127.0.0.1:21",
            "aws-sqs://123456789012/orders?endpoint=http://127.0.0.1:34561/path",
            "aws-sqs://123456789012/orders?endpoint=http://a:1&endpoint=http://b:2",
        ] {
            assert!(sqs(url).is_err(), "{url} should be refused");
        }
    }

    #[test]
    fn kinesis_url_forms() {
        let u = kin("aws-kinesis://orders").unwrap();
        assert_eq!((u.region, u.stream.as_str()), (None, "orders"));
        let u = kin("aws-kinesis://eu-west-1/orders.v2_a-b").unwrap();
        assert_eq!(u.region.as_deref(), Some("eu-west-1"));
        assert_eq!(u.stream, "orders.v2_a-b");
        let u = kin("aws-kinesis://orders?endpoint=http://127.0.0.1:34562").unwrap();
        assert_eq!(u.endpoint.as_deref(), Some("http://127.0.0.1:34562"));
    }

    #[test]
    fn kinesis_refuses_bad_urls() {
        for url in [
            "https://kinesis.eu-west-1.amazonaws.com",
            "aws-sqs://123456789012/orders",
            "aws-kinesis://",
            "aws-kinesis://eu-west-1/a/b",
            "aws-kinesis://orders/",
            "aws-kinesis://eu-west-1/bad name",
            "aws-kinesis://u:p@orders",
            "aws-kinesis://orders?endpoint=https://k.example/x",
            "aws-kinesis://orders?stream=x",
            // an ARN does not fit the URL form (its slash and colons split the path)
            "aws-kinesis://arn:aws:kinesis:eu-west-1:123456789012:stream/orders",
        ] {
            assert!(kin(url).is_err(), "{url} should be refused");
        }
    }

    #[test]
    fn scheme_errors_name_the_scheme() {
        let e = sqs("https://sqs.eu-west-1.amazonaws.com/123456789012/q").unwrap_err();
        assert!(
            format!("{e:#}").contains("not a plain http(s) URL"),
            "{e:#}"
        );
        let e = kin("aws-sqs://123456789012/q").unwrap_err();
        assert!(format!("{e:#}").contains("not `aws-sqs://`"), "{e:#}");
    }

    #[test]
    fn region_comes_from_the_url_then_the_environment_then_the_profile() {
        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |n: &str| {
                vars.iter()
                    .find(|(k, _)| *k == n)
                    .map(|(_, v)| v.to_string())
            }
        };
        let config = || Some("ap-south-1".to_string());
        // the URL wins
        assert_eq!(
            region_from(
                Some("eu-west-1"),
                env(&[("AWS_REGION", "us-east-1")]),
                config
            )
            .as_deref(),
            Some("eu-west-1")
        );
        // AWS_REGION, then AWS_DEFAULT_REGION
        assert_eq!(
            region_from(
                None,
                env(&[("AWS_REGION", "us-east-1"), ("AWS_DEFAULT_REGION", "x-y-1")]),
                config
            )
            .as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            region_from(None, env(&[("AWS_DEFAULT_REGION", "us-west-2")]), config).as_deref(),
            Some("us-west-2")
        );
        // the shared config profile, only when the environment has none
        assert_eq!(
            region_from(None, env(&[]), config).as_deref(),
            Some("ap-south-1")
        );
        // none of them: no region
        assert_eq!(region_from(None, env(&[]), || None), None);
    }

    #[test]
    fn config_region_reads_the_named_profile() {
        let text = "# shared config\n[default]\nregion = us-east-2\n\n[profile work]\noutput = json\nregion=eu-central-1\n";
        assert_eq!(config_region(text, "default").as_deref(), Some("us-east-2"));
        assert_eq!(config_region(text, "work").as_deref(), Some("eu-central-1"));
        assert_eq!(config_region(text, "missing"), None);
        // a `region` under another profile is not this one's
        assert_eq!(
            config_region(
                "[profile a]\nregion = x-y-1\n[default]\noutput=json\n",
                "default"
            ),
            None
        );
    }
}
