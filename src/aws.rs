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
