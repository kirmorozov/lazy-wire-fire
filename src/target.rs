//! A target at runtime: its endpoints, how a rendered action reaches each of them, and whether
//! the answer meets `expect`.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use serde_json::{json, Value};

use crate::aws;
use crate::http::{BaseUrl, Client as HttpClient, RawRequest};
use crate::model::{Kind, TargetDef};
use crate::redis::{self, Client as RedisClient, RedisAddr, Reply};
use crate::s3::{self, S3Spec};
use crate::secrets;

const USER_AGENT: &str = concat!("lazywf/", env!("CARGO_PKG_VERSION"));

pub struct Endpoint {
    /// `endpoint` in the template context.
    pub ctx: Value,
    transport: Transport,
}

enum Transport {
    Http {
        client: HttpClient,
        /// Prepended to the rendered path (base URL path, or `/<bucket>` for path-style S3).
        prefix: String,
        host_header: String,
    },
    Redis(RedisClient),
}

pub struct Target {
    pub def: TargetDef,
    /// `target` in the template context: `name` and `vars` without credentials.
    pub ctx: Value,
    pub endpoints: Vec<Endpoint>,
    s3: Option<(S3Spec, aws::Provider)>,
}

/// What sending one rendered action did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent {
    /// The template rendered nothing to send.
    Skipped,
    Ok,
}

fn var<'a>(def: &'a TargetDef, k: &str) -> Option<&'a str> {
    def.vars
        .get(k)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

impl Target {
    pub fn new(def: TargetDef) -> Result<Arc<Target>> {
        let mut endpoints = Vec::new();
        let mut s3_rt = None;
        match def.kind {
            Kind::Redis => {
                for e in &def.endpoints {
                    let addr = RedisAddr::parse(e)?;
                    let ctx = json!({"host": addr.host, "port": addr.port, "db": addr.db, "url": addr.url()});
                    let client = RedisClient::new(
                        addr,
                        &def.tls,
                        var(&def, "username").map(str::to_string),
                        var(&def, "password").map(str::to_string),
                    )?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Redis(client),
                    });
                }
            }
            Kind::HttpS3 => {
                let path_style = match def.vars.get("path_style") {
                    Some(Value::Bool(b)) => *b,
                    Some(Value::String(s)) => {
                        matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
                    }
                    Some(Value::Number(n)) => n.as_u64() == Some(1),
                    _ => false,
                };
                let spec = S3Spec {
                    bucket: var(&def, "bucket").unwrap_or_default().to_string(),
                    region: var(&def, "region").unwrap_or_default().to_string(),
                    path_style,
                };
                let mut services: Vec<BaseUrl> = def
                    .endpoints
                    .iter()
                    .map(|e| BaseUrl::parse(e))
                    .collect::<Result<_>>()?;
                if services.is_empty() {
                    services.push(match var(&def, "endpoint") {
                        Some(e) => BaseUrl::parse(e).context("`vars.endpoint`")?,
                        None => spec.aws_endpoint(),
                    });
                }
                for svc in services {
                    let (base, prefix) = spec.address(&svc);
                    let ctx = json!({"host": base.host, "port": base.port, "url": format!("{}{prefix}", base.url())});
                    let host_header = base.host_header();
                    let client = HttpClient::new(base, &def.tls)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Http {
                            client,
                            prefix,
                            host_header,
                        },
                    });
                }
                let refs = match (var(&def, "access_key_ref"), var(&def, "secret_key_ref")) {
                    (Some(a), Some(s)) => Some(aws::Refs {
                        access_key: a.to_string(),
                        secret_key: s.to_string(),
                        session_token: var(&def, "session_token_ref").map(str::to_string),
                    }),
                    _ => None,
                };
                let provider = aws::Provider::new(refs, &spec.region);
                s3_rt = Some((spec, provider));
            }
            _ => {
                for e in &def.endpoints {
                    let base = BaseUrl::parse(e)?;
                    let ctx = json!({"host": base.host, "port": base.port, "url": base.url()});
                    let host_header = base.host_header();
                    let prefix = base.base.clone();
                    let client = HttpClient::new(base, &def.tls)?;
                    endpoints.push(Endpoint {
                        ctx,
                        transport: Transport::Http {
                            client,
                            prefix,
                            host_header,
                        },
                    });
                }
            }
        }
        let ctx = json!({"name": def.name, "vars": def.template_vars()});
        Ok(Arc::new(Target {
            def,
            ctx,
            endpoints,
            s3: s3_rt,
        }))
    }

    pub fn is_http(&self) -> bool {
        self.def.kind.is_http()
    }

    /// Sends one rendered action to endpoint `ep` and checks `expect`. `Err` holds why it
    /// failed (transport, status, body predicate or Redis error).
    pub async fn send(&self, ep: usize, rendered: &str) -> Result<Sent> {
        match &self.endpoints[ep].transport {
            Transport::Http {
                client,
                prefix,
                host_header,
            } => {
                let Some(req) = RawRequest::parse(rendered).context("the rendered request")? else {
                    return Ok(Sent::Skipped);
                };
                let req = self.prepare(req, prefix, host_header).await?;
                let resp = client
                    .send(&req, self.def.timeout)
                    .await
                    .with_context(|| format!("{} {}", req.method, client.base.url()))?;
                self.def
                    .expect
                    .check_http(resp.status, &resp.body)
                    .map_err(|e| {
                        anyhow!(
                            "{} {}{}: {e}: {}",
                            req.method,
                            client.base.url(),
                            req.target,
                            resp.body_snippet()
                        )
                    })?;
                Ok(Sent::Ok)
            }
            Transport::Redis(client) => {
                let args = redis::tokenize(rendered).context("the rendered command")?;
                if args.is_empty() {
                    return Ok(Sent::Skipped);
                }
                let name = String::from_utf8_lossy(&args[0]).to_uppercase();
                let reply = client
                    .command(&args, self.def.timeout)
                    .await
                    .with_context(|| format!("{name} on {}", client.addr.url()))?;
                if let Some(e) = reply.error() {
                    bail!("{name} on {}: {e}", client.addr.url());
                }
                self.def
                    .expect
                    .check_value(&reply.to_json())
                    .map_err(|e| anyhow!("{name} on {}: {e}", client.addr.url()))?;
                Ok(Sent::Ok)
            }
        }
    }

    /// Sends a listing request (stray reset steps) and returns the response as JSON: the parsed
    /// body (http, which must answer 2xx) or the reply (redis).
    pub async fn fetch(&self, ep: usize, rendered: &str) -> Result<Value> {
        match &self.endpoints[ep].transport {
            Transport::Http {
                client,
                prefix,
                host_header,
            } => {
                let req = RawRequest::parse(rendered)?
                    .ok_or_else(|| anyhow!("the list template rendered nothing"))?;
                let req = self.prepare(req, prefix, host_header).await?;
                let resp = client.send(&req, self.def.timeout).await?;
                if !(200..300).contains(&resp.status) {
                    bail!(
                        "{} {}{}: status {}: {}",
                        req.method,
                        client.base.url(),
                        req.target,
                        resp.status,
                        resp.body_snippet()
                    );
                }
                serde_json::from_slice(&resp.body).map_err(|_| {
                    anyhow!("the listing response is not JSON: {}", resp.body_snippet())
                })
            }
            Transport::Redis(client) => {
                let args = redis::tokenize(rendered)?;
                if args.is_empty() {
                    bail!("the list template rendered nothing");
                }
                let reply: Reply = client.command(&args, self.def.timeout).await?;
                if let Some(e) = reply.error() {
                    bail!("{e}");
                }
                Ok(reply.to_json())
            }
        }
    }

    async fn prepare(
        &self,
        mut req: RawRequest,
        prefix: &str,
        host_header: &str,
    ) -> Result<RawRequest> {
        if req.header("user-agent").is_none() {
            req.set_header("User-Agent", USER_AGENT.to_string());
        }
        if let Some((spec, creds)) = &self.s3 {
            let c = creds.get().await?;
            s3::sign(
                &mut req,
                host_header,
                prefix,
                &spec.region,
                "s3",
                &c,
                crate::time::unix_now(),
            )?;
            return Ok(req);
        }
        if !prefix.is_empty() && req.target.starts_with('/') {
            req.target = format!("{prefix}{}", req.target);
        }
        if req.header("host").is_none() {
            req.set_header("Host", host_header.to_string());
        }
        let get = |k: &str| -> Result<String> {
            secrets::resolve(var(&self.def, k).ok_or_else(|| anyhow!("vars.{k} is missing"))?)
                .with_context(|| format!("vars.{k}"))
        };
        match self.def.kind {
            Kind::HttpBasic => {
                let token = base64::engine::general_purpose::STANDARD.encode(format!(
                    "{}:{}",
                    get("username")?,
                    get("password")?
                ));
                req.set_header("Authorization", format!("Basic {token}"));
            }
            Kind::HttpBearer => {
                req.set_header("Authorization", format!("Bearer {}", get("token")?))
            }
            Kind::HttpHeader => {
                let name = var(&self.def, "header").unwrap_or_default().to_string();
                req.set_header(&name, get("value")?);
            }
            _ => {}
        }
        Ok(req)
    }
}
