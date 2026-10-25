//! Environment configuration (`LAZYWF_*`).

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    /// Connection for queries, the tool's tables and the instance lock.
    pub dsn: String,
    /// Connection for the binlog stream (replication account); defaults to `dsn`.
    pub binlog_dsn: String,
    /// Schema holding the tool's tables (the DSN's database).
    pub schema: String,
    pub instance: String,
    pub server_id: u32,
    pub server_id_fixed: bool,
    pub health_addr: Option<SocketAddr>,
    pub heartbeat: Duration,
    pub visibility_delay: Duration,
    pub pool_size: usize,
    pub send_concurrency: usize,
    pub config_poll: Duration,
    pub status_every: Duration,
    pub max_backoff: Duration,
    pub stop_grace: Duration,
}

impl Config {
    pub fn from_env() -> Result<Config> {
        Config::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Config> {
        let dsn = connection(&get, "LAZYWF")?.ok_or_else(|| {
            anyhow!(
                "no database configured: set LAZYWF_HOST, LAZYWF_USER, LAZYWF_PASSWORD, LAZYWF_DATABASE \
                 (or LAZYWF_DSN=mysql://user:pass@host:3306/db)"
            )
        })?;
        let binlog_dsn = match get("LAZYWF_BINLOG_DSN").filter(|v| !v.is_empty()) {
            Some(d) => finish_dsn(&d, tls_mode(&get, "LAZYWF")?.as_deref(), "LAZYWF")?,
            None => match get("LAZYWF_BINLOG_USER").filter(|v| !v.is_empty()) {
                Some(user) => {
                    with_credentials(&dsn, &user, get("LAZYWF_BINLOG_PASSWORD").as_deref())?
                }
                None => dsn.clone(),
            },
        };
        let schema = schema_of(&dsn)?;
        let num = |k: &str, d: u64| -> Result<u64> {
            match get(k).filter(|v| !v.trim().is_empty()) {
                Some(v) => v
                    .trim()
                    .parse()
                    .with_context(|| format!("{k} must be a number")),
                None => Ok(d),
            }
        };
        let (server_id, server_id_fixed) = match get("LAZYWF_SERVER_ID").filter(|v| !v.is_empty()) {
            Some(v) => {
                let id: u32 = v
                    .trim()
                    .parse()
                    .context("LAZYWF_SERVER_ID must be a number")?;
                if id == 0 {
                    bail!("LAZYWF_SERVER_ID must not be 0");
                }
                (id, true)
            }
            None => (random_server_id(), false),
        };
        let health_addr = match get("LAZYWF_HEALTH_ADDR").map(|v| v.trim().to_string()) {
            Some(v) if v.eq_ignore_ascii_case("off") || v.is_empty() => None,
            Some(v) => Some(
                v.parse()
                    .with_context(|| format!("LAZYWF_HEALTH_ADDR `{v}` is not host:port"))?,
            ),
            None => Some(([127, 0, 0, 1], 8081).into()),
        };
        let instance = get("LAZYWF_INSTANCE")
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "default".into());
        if instance.len() > 64 || instance == "*" {
            bail!("LAZYWF_INSTANCE must be at most 64 characters and not `*`");
        }
        let ms = |k: &str, d: u64| num(k, d).map(Duration::from_millis);
        Ok(Config {
            dsn,
            binlog_dsn,
            schema,
            instance,
            server_id,
            server_id_fixed,
            health_addr,
            heartbeat: ms("LAZYWF_HEARTBEAT_MS", 5_000)?,
            visibility_delay: ms("LAZYWF_VISIBILITY_DELAY_MS", 20)?,
            pool_size: num("LAZYWF_POOL_SIZE", 16)?.max(2) as usize,
            send_concurrency: num("LAZYWF_SEND_CONCURRENCY", 16)?.max(1) as usize,
            config_poll: ms("LAZYWF_CONFIG_POLL_MS", 30_000)?.max(Duration::from_millis(200)),
            status_every: ms("LAZYWF_STATUS_MS", 5_000)?.max(Duration::from_millis(100)),
            max_backoff: ms("LAZYWF_MAX_BACKOFF_MS", 60_000)?.max(Duration::from_millis(100)),
            stop_grace: ms("LAZYWF_STOP_GRACE_MS", 10_000)?,
        })
    }
}

fn tls_mode(get: &impl Fn(&str) -> Option<String>, prefix: &str) -> Result<Option<String>> {
    let ca = get(&format!("{prefix}_TLS_CA")).filter(|v| !v.is_empty());
    Ok(get(&format!("{prefix}_TLS"))
        .filter(|v| !v.is_empty())
        .or_else(|| ca.map(|_| "verify-full".to_string())))
}

/// `<PREFIX>_DSN`, or one assembled from `<PREFIX>_HOST`, `_PORT`, `_USER`, `_PASSWORD`,
/// `_DATABASE` (values verbatim, no URL encoding needed), with `<PREFIX>_TLS` / `_TLS_CA`.
fn connection(get: &impl Fn(&str) -> Option<String>, prefix: &str) -> Result<Option<String>> {
    let part = |k: &str| get(&format!("{prefix}_{k}")).filter(|v| !v.is_empty());
    let ca = part("TLS_CA");
    let tls = tls_mode(get, prefix)?;
    let dsn = match part("DSN") {
        Some(dsn) => Some(dsn),
        None => assemble(&part, prefix)?,
    };
    dsn.map(|d| {
        let mut d = finish_dsn(&d, tls.as_deref(), prefix)?;
        if let Some(ca) = &ca {
            d.push(if d.contains('?') { '&' } else { '?' });
            d.push_str("tls_ca=");
            d.push_str(&encode(ca));
        }
        Ok(d)
    })
    .transpose()
}

/// Accepts `mariadb://` as an alias and appends TLS parameters for `<PREFIX>_TLS`.
fn finish_dsn(dsn: &str, tls: Option<&str>, prefix: &str) -> Result<String> {
    let mut d = match dsn.strip_prefix("mariadb://") {
        Some(rest) => format!("mysql://{rest}"),
        None => dsn.to_string(),
    };
    let params = match tls.map(|t| t.trim().to_ascii_lowercase()) {
        None => None,
        Some(t) => match t.as_str() {
            "disabled" | "off" | "false" => None,
            "required" | "require" | "on" | "true" => {
                Some("require_ssl=true&verify_ca=false&verify_identity=false")
            }
            "verify-ca" | "verify_ca" => {
                Some("require_ssl=true&verify_ca=true&verify_identity=false")
            }
            "verify-full" | "verify_identity" | "verify-identity" => {
                Some("require_ssl=true&verify_ca=true&verify_identity=true")
            }
            other => {
                bail!("{prefix}_TLS must be disabled|required|verify-ca|verify-full, got {other}")
            }
        },
    };
    if let Some(p) = params {
        d.push(if d.contains('?') { '&' } else { '?' });
        d.push_str(p);
    }
    Ok(d)
}

fn assemble(part: &impl Fn(&str) -> Option<String>, prefix: &str) -> Result<Option<String>> {
    let Some(host) = part("HOST") else {
        return Ok(None);
    };
    let db = part("DATABASE")
        .ok_or_else(|| anyhow!("{prefix}_DATABASE is required (the schema lazywf works in)"))?;
    let user = part("USER").unwrap_or_else(|| "root".into());
    let port: u16 = match part("PORT") {
        Some(p) => p
            .trim()
            .parse()
            .with_context(|| format!("{prefix}_PORT must be a port number"))?,
        None => 3306,
    };
    let auth = match part("PASSWORD") {
        Some(pw) => format!("{}:{}", encode(&user), encode(&pw)),
        None => encode(&user),
    };
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host
    };
    Ok(Some(format!(
        "mysql://{auth}@{host}:{port}/{}",
        encode(&db)
    )))
}

/// The same DSN with other credentials.
fn with_credentials(dsn: &str, user: &str, password: Option<&str>) -> Result<String> {
    let (scheme, rest) = dsn
        .split_once("://")
        .ok_or_else(|| anyhow!("DSN without `://`"))?;
    let host_part = match rest.rfind('@') {
        Some(i) if rest[..i].find('/').is_none() => &rest[i + 1..],
        _ => rest,
    };
    let auth = match password {
        Some(pw) => format!("{}:{}", encode(user), encode(pw)),
        None => encode(user),
    };
    Ok(format!("{scheme}://{auth}@{host_part}"))
}

fn schema_of(dsn: &str) -> Result<String> {
    let opts = crate::binlog::replica_opts(dsn)?;
    opts.db_name()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!("the DSN names no database: the schema of lazywf's tables is required")
        })
}

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Random replica server id in [2^30, 2^31), away from small hand-assigned ids.
pub fn random_server_id() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.write_u32(std::process::id());
    (1 << 30) | (h.finish() as u32 & ((1 << 30) - 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| m.get(k).cloned())
    }

    #[test]
    fn connection_forms() {
        let c = cfg(&[
            ("LAZYWF_HOST", "db"),
            ("LAZYWF_USER", "app"),
            ("LAZYWF_PASSWORD", "p@ss:w/rd"),
            ("LAZYWF_DATABASE", "shop"),
        ])
        .unwrap();
        assert_eq!(c.dsn, "mysql://app:p%40ss%3Aw%2Frd@db:3306/shop");
        assert_eq!(c.binlog_dsn, c.dsn);
        assert_eq!(c.schema, "shop");
        assert_eq!(c.instance, "default");
        assert_eq!(c.health_addr, Some(([127, 0, 0, 1], 8081).into()));
        let c = cfg(&[
            ("LAZYWF_DSN", "mariadb://a:b@h:3307/s"),
            ("LAZYWF_BINLOG_USER", "repl"),
            ("LAZYWF_BINLOG_PASSWORD", "x y"),
            ("LAZYWF_TLS", "required"),
            ("LAZYWF_HEALTH_ADDR", "off"),
            ("LAZYWF_SERVER_ID", "77"),
        ])
        .unwrap();
        assert_eq!(
            c.dsn,
            "mysql://a:b@h:3307/s?require_ssl=true&verify_ca=false&verify_identity=false"
        );
        assert_eq!(
            c.binlog_dsn,
            "mysql://repl:x%20y@h:3307/s?require_ssl=true&verify_ca=false&verify_identity=false"
        );
        assert_eq!(
            (c.server_id, c.server_id_fixed, c.health_addr),
            (77, true, None)
        );
        assert!(cfg(&[]).is_err());
        assert!(cfg(&[("LAZYWF_DSN", "mysql://h/")]).is_err());
        assert!(cfg(&[("LAZYWF_DSN", "mysql://h/s"), ("LAZYWF_SERVER_ID", "0")]).is_err());
    }
}
