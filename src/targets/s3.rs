//! `http.s3` targets: addressing (virtual-hosted or path style) and AWS Signature Version 4.
//!
//! What the kind decides is here: the `vars` checks at load ([`check`]), the endpoints and the
//! signer of a loaded target ([`build`]), and the bucket addressing they build on ([`S3Spec`]).
//! The rest of the send path is not here either, and that is deliberate: `http.s3` is an
//! http-transport kind, so the request line, the send and the `expect` verdict are the generic
//! http path of [`crate::target`] (shared with the plain kinds of [`crate::targets::http`],
//! which they are not — that module is the four credential kinds, this one is the object
//! store), and the request is signed on the shared aws-sign path of
//! [`crate::target::Target::prepare`], the way `sqs`, `kinesis`, `lambda`, `eventbridge` and
//! `sns` sign theirs ([`crate::aws`]).

use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::{json, Map, Value};

use crate::aws;
use crate::http::{BaseUrl, Client as HttpClient};
use crate::model::TargetDef;
use crate::target::{aws_refs, def_var, Transport};

/// Load checks for `http.s3` (from `TargetDef::from_row`): the bucket and the region are
/// required `vars` (an empty `endpoints` is allowed here — the service falls back to
/// `vars.endpoint` or the regional AWS one, the guard at the end of `from_row` waives it for
/// this kind). `need` is the caller's closure that requires one `vars` entry, as in
/// [`crate::targets::http::check`].
pub fn check(vars: &Map<String, Value>, need: &impl Fn(&str) -> Result<()>) -> Result<()> {
    need("bucket")?;
    need("region")?;
    aws::check_refs(vars)?;
    Ok(())
}

/// The endpoints of a loaded `http.s3` target and how it signs (from `Target::new`): one
/// endpoint per service URL — every `endpoints` entry, else `vars.endpoint`, else the regional
/// AWS one — each with the bucket addressed into it, and the SigV4 signer of the `s3` service.
pub(crate) fn build(def: &TargetDef) -> Result<(Vec<(Value, Transport)>, aws::Sign)> {
    let path_style = match def.vars.get("path_style") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => {
            matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
        }
        Some(Value::Number(n)) => n.as_u64() == Some(1),
        _ => false,
    };
    let spec = S3Spec {
        bucket: def_var(def, "bucket").unwrap_or_default().to_string(),
        region: def_var(def, "region").unwrap_or_default().to_string(),
        path_style,
    };
    let mut services: Vec<BaseUrl> = def
        .endpoints
        .iter()
        .map(|e| BaseUrl::parse(e))
        .collect::<Result<_>>()?;
    if services.is_empty() {
        services.push(match def_var(def, "endpoint") {
            Some(e) => BaseUrl::parse(e).context("`vars.endpoint`")?,
            None => spec.aws_endpoint(),
        });
    }
    let mut out = Vec::new();
    for svc in services {
        let (base, prefix) = spec.address(&svc);
        let ctx =
            json!({"host": base.host, "port": base.port, "url": format!("{}{prefix}", base.url())});
        let host_header = base.host_header();
        let client = Arc::new(HttpClient::new(base, &def.tls)?);
        out.push((
            ctx,
            Transport::Http {
                client,
                prefix,
                host_header,
            },
        ));
    }
    let provider = aws::Provider::new(aws_refs(def), &spec.region);
    let sign = aws::Sign {
        service: "s3",
        region: spec.region.clone(),
        provider,
    };
    Ok((out, sign))
}

/// Bucket addressing from a target's `vars`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Spec {
    pub bucket: String,
    pub region: String,
    pub path_style: bool,
}

impl S3Spec {
    /// The base URL to connect to and the path prefix for requests, given the service
    /// endpoint (`vars.endpoint`, an entry of `endpoints`, or AWS's regional one).
    pub fn address(&self, service: &BaseUrl) -> (BaseUrl, String) {
        if self.path_style {
            let prefix = format!("{}/{}", service.base, aws::uri_encode(&self.bucket, true));
            (
                BaseUrl {
                    base: String::new(),
                    ..service.clone()
                },
                prefix,
            )
        } else {
            let host = format!("{}.{}", self.bucket, service.host);
            (
                BaseUrl {
                    host,
                    base: String::new(),
                    ..service.clone()
                },
                service.base.clone(),
            )
        }
    }

    pub fn aws_endpoint(&self) -> BaseUrl {
        aws::endpoint("s3", &self.region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addressing() {
        let s = S3Spec {
            bucket: "b".into(),
            region: "eu-west-1".into(),
            path_style: false,
        };
        let (base, prefix) = s.address(&s.aws_endpoint());
        assert_eq!(
            (base.host.as_str(), prefix.as_str()),
            ("b.s3.eu-west-1.amazonaws.com", "")
        );
        let p = S3Spec {
            path_style: true,
            ..s
        };
        let (base, prefix) = p.address(&BaseUrl::parse("http://minio:9000").unwrap());
        assert_eq!(
            (base.host_header(), prefix.as_str()),
            ("minio:9000".to_string(), "/b")
        );
    }
}
