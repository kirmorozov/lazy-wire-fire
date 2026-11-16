//! Environment configuration (`LAZYWF_*`).

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};

use crate::allow::HostList;

#[derive(Clone)]
pub struct Config {
    /// Connection for queries, the tool's tables and the instance lock.
    pub dsn: String,
    /// Connection for the binlog stream (replication account); defaults to `dsn`.
    pub binlog_dsn: String,
    /// Connection for the SQL that the configuration defines (rule selects, trigger
    /// `keys_sql`, reset-step queries, `expect` and `keys` / `next` expressions): a SELECT-only
    /// account is recommended. Equal to `dsn` when `query_account` is false.
    pub query_dsn: String,
    /// `LAZYWF_QUERY_DSN` or `LAZYWF_QUERY_USER` is set: the query statements use `query_dsn`.
    pub query_account: bool,
    /// Schema holding the tool's tables (the DSN's database).
    pub schema: String,
    pub instance: String,
    pub server_id: u32,
    pub server_id_fixed: bool,
    pub health_addr: Option<SocketAddr>,
    /// `LAZYWF_METRICS_ADDR`: when set, serve `GET /metrics` on this address. Opt-in: unset (the
    /// default) keeps the whole surface off — no listener, no counters maintained.
    pub metrics_addr: Option<SocketAddr>,
    pub heartbeat: Duration,
    pub visibility_delay: Duration,
    pub pool_size: usize,
    pub send_concurrency: usize,
    pub config_poll: Duration,
    pub status_every: Duration,
    pub max_backoff: Duration,
    pub stop_grace: Duration,
    /// `LAZYWF_ALLOWED_HOSTS`: the endpoint hosts a target may connect to. `from_lookup`
    /// always sets it: unset becomes the deny-all list (0.3.0), `*` the allow-all one.
    /// `None` only exists for tests that carry no operator setting.
    pub allowed_hosts: Option<HostList>,
    /// `LAZYWF_STATUS_TOKEN`: when set, `/status` on the health listener requires the
    /// `X-Lazywf-Token` header (constant-time compare). `/livez` and `/readyz` never need it.
    pub status_token: Option<String>,
}

/// The DSNs are shown without their passwords (`mysql://***@host/...`): a `{:?}` of the
/// configuration must not reach the log.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("dsn", &crate::secrets::redact_url(&self.dsn))
            .field("binlog_dsn", &crate::secrets::redact_url(&self.binlog_dsn))
            .field("query_dsn", &crate::secrets::redact_url(&self.query_dsn))
            .field("query_account", &self.query_account)
            .field("schema", &self.schema)
            .field("instance", &self.instance)
            .field("server_id", &self.server_id)
            .field("server_id_fixed", &self.server_id_fixed)
            .field("health_addr", &self.health_addr)
            .field("metrics_addr", &self.metrics_addr)
            .field("heartbeat", &self.heartbeat)
            .field("visibility_delay", &self.visibility_delay)
            .field("pool_size", &self.pool_size)
            .field("send_concurrency", &self.send_concurrency)
            .field("config_poll", &self.config_poll)
            .field("status_every", &self.status_every)
            .field("max_backoff", &self.max_backoff)
            .field("stop_grace", &self.stop_grace)
            .field(
                "allowed_hosts",
                &self.allowed_hosts.as_ref().map(HostList::len),
            )
            // The token itself never appears: only whether it is set.
            .field("status_token", &self.status_token.as_ref().map(|_| "set"))
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Config> {
        Config::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Config> {
        // The connections assemble without TLS parameters first: the level for each one
        // depends on its own host (loopback stays plaintext, a remote host is required).
        let (dsn, mut plain_remote) = match connection(&get, "LAZYWF")? {
            (Some(d), warn) => (d, warn),
            (None, _) => {
                return Err(anyhow!(
                    "no database configured: set LAZYWF_HOST, LAZYWF_USER, LAZYWF_PASSWORD, LAZYWF_DATABASE \
                     (or LAZYWF_DSN=mysql://user:pass@host:3306/db)"
                ))
            }
        };
        let binlog_dsn = match get("LAZYWF_BINLOG_DSN").filter(|v| !v.is_empty()) {
            Some(d) => {
                let (tls, warn) = tls_for(&get, "LAZYWF", &d)?;
                plain_remote |= warn;
                finish_dsn(&d, tls.as_deref(), "LAZYWF")?
            }
            None => match get("LAZYWF_BINLOG_USER").filter(|v| !v.is_empty()) {
                Some(user) => {
                    with_credentials(&dsn, &user, get("LAZYWF_BINLOG_PASSWORD").as_deref())?
                }
                None => dsn.clone(),
            },
        };
        let (query_dsn, query_account) = match get("LAZYWF_QUERY_DSN").filter(|v| !v.is_empty()) {
            Some(d) => {
                let (tls, warn) = tls_for(&get, "LAZYWF", &d)?;
                plain_remote |= warn;
                (finish_dsn(&d, tls.as_deref(), "LAZYWF")?, true)
            }
            None => match get("LAZYWF_QUERY_USER").filter(|v| !v.is_empty()) {
                Some(user) => (
                    with_credentials(&dsn, &user, get("LAZYWF_QUERY_PASSWORD").as_deref())?,
                    true,
                ),
                None => (dsn.clone(), false),
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
        // Opt-in, unlike the health address: unset stays off entirely (no listener, no counters).
        let metrics_addr = match get("LAZYWF_METRICS_ADDR").map(|v| v.trim().to_string()) {
            Some(v) if v.eq_ignore_ascii_case("off") || v.is_empty() => None,
            Some(v) => Some(
                v.parse()
                    .with_context(|| format!("LAZYWF_METRICS_ADDR `{v}` is not host:port"))?,
            ),
            None => None,
        };
        let instance = get("LAZYWF_INSTANCE")
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "default".into());
        if instance.len() > 64 || instance == "*" {
            bail!("LAZYWF_INSTANCE must be at most 64 characters and not `*`");
        }
        let ms = |k: &str, d: u64| num(k, d).map(Duration::from_millis);
        let cfg = Config {
            dsn,
            binlog_dsn,
            query_dsn,
            query_account,
            schema,
            instance,
            server_id,
            server_id_fixed,
            health_addr,
            metrics_addr,
            heartbeat: ms("LAZYWF_HEARTBEAT_MS", 5_000)?,
            visibility_delay: ms("LAZYWF_VISIBILITY_DELAY_MS", 20)?,
            pool_size: num("LAZYWF_POOL_SIZE", 16)?.max(2) as usize,
            send_concurrency: num("LAZYWF_SEND_CONCURRENCY", 16)?.max(1) as usize,
            config_poll: ms("LAZYWF_CONFIG_POLL_MS", 30_000)?.max(Duration::from_millis(200)),
            status_every: ms("LAZYWF_STATUS_MS", 5_000)?.max(Duration::from_millis(100)),
            max_backoff: ms("LAZYWF_MAX_BACKOFF_MS", 60_000)?.max(Duration::from_millis(100)),
            stop_grace: ms("LAZYWF_STOP_GRACE_MS", 10_000)?,
            // Unset denies all target egress (0.3.0): the configuration rows are SQL, and an
            // INSERT grant must not be able to turn a target into a channel to any address.
            // The operator opts out with `LAZYWF_ALLOWED_HOSTS=*` (the behaviour unset had
            // before) or lists the hosts.
            allowed_hosts: Some(
                HostList::parse(&get("LAZYWF_ALLOWED_HOSTS").unwrap_or_default())
                    .context("LAZYWF_ALLOWED_HOSTS")?
                    .unwrap_or_else(HostList::deny_all),
            ),
            status_token: get("LAZYWF_STATUS_TOKEN").filter(|v| !v.trim().is_empty()),
        };
        if plain_remote {
            // One loud line, at startup, for the explicit opt-out: it must not scroll by
            // unnoticed, because the binlog behind it carries every watched row.
            tracing::warn!(
                "LAZYWF_TLS says disabled while the source host is not loopback: the \
                 connection to the database is UNENCRYPTED. The binlog stream carries every \
                 watched row, so anyone on the network path can read them. This is the \
                 explicit opt-out from the TLS that lazywf 0.3.0 requires by default: remove \
                 LAZYWF_TLS to get `required`, or set it to verify-ca / verify-full to check \
                 the server's certificate too."
            );
        }
        Ok(cfg)
    }
}

/// The TLS level for one DSN, decided per connection (0.3.0): the explicit `<PREFIX>_TLS`
/// value when set (a blank value is unset), the `<PREFIX>_TLS_CA` setting implying
/// `verify-full` as before, and otherwise: `required` for a host that is not loopback, no
/// TLS parameters for a loopback one (the machine's own server needs no certificate).
/// The bool says an explicit `disabled` was chosen against a non-loopback host: the
/// connection is plaintext by choice, and the process logs one loud WARN for it.
fn tls_for(
    get: &impl Fn(&str) -> Option<String>,
    prefix: &str,
    dsn: &str,
) -> Result<(Option<String>, bool)> {
    match get(&format!("{prefix}_TLS")).filter(|v| !v.is_empty()) {
        Some(v) => {
            let off = matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "disabled" | "off" | "false"
            );
            // The level's own validity is checked by finish_dsn, whose message names the
            // setting; here only the plaintext opt-out is noticed.
            Ok((Some(v), off && !is_loopback_dsn(dsn)))
        }
        None => {
            if get(&format!("{prefix}_TLS_CA"))
                .filter(|v| !v.is_empty())
                .is_some()
            {
                Ok((Some("verify-full".to_string()), false))
            } else if is_loopback_dsn(dsn) {
                Ok((None, false))
            } else {
                Ok((Some("required".to_string()), false))
            }
        }
    }
}

/// The host of a DSN as lazywf connects to it: between `://` (and any userinfo) and the
/// path, without the port or the IPv6 brackets; empty when the DSN names no host (a
/// socket connection). No DNS: the name as written.
fn dsn_host(dsn: &str) -> String {
    let rest = dsn.split_once("://").map(|(_, r)| r).unwrap_or(dsn);
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let hostport = authority.rsplit('@').next().unwrap_or(authority).trim();
    if let Some(inner) = hostport.strip_prefix('[') {
        return inner.split(']').next().unwrap_or_default().to_string();
    }
    match hostport.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => h.to_string(),
        _ => hostport.to_string(),
    }
}

/// Whether the DSN's host cannot leave the machine: a loopback address, `localhost` (any
/// case, trailing dot allowed), or a local socket (no host, or a `socket=` parameter).
/// Everything else is remote: since 0.3.0 the connection to it requires TLS unless
/// `LAZYWF_TLS` says otherwise.
fn is_loopback_dsn(dsn: &str) -> bool {
    if dsn.split_once('?').is_some_and(|(_, q)| {
        q.split('&')
            .any(|p| p == "socket" || p.starts_with("socket="))
    }) {
        return true;
    }
    let host = dsn_host(dsn);
    let h = host.trim().trim_end_matches('.').to_ascii_lowercase();
    h.is_empty() || h == "localhost" || h.parse::<std::net::IpAddr>().is_ok_and(|a| a.is_loopback())
}

/// `<PREFIX>_DSN`, or one assembled from `<PREFIX>_HOST`, `_PORT`, `_USER`, `_PASSWORD`,
/// `_DATABASE` (values verbatim, no URL encoding needed), with `<PREFIX>_TLS` / `_TLS_CA`.
/// Returns the DSN and whether an explicit `disabled` left a non-loopback host plaintext.
fn connection(
    get: &impl Fn(&str) -> Option<String>,
    prefix: &str,
) -> Result<(Option<String>, bool)> {
    let part = |k: &str| get(&format!("{prefix}_{k}")).filter(|v| !v.is_empty());
    let ca = part("TLS_CA");
    let dsn = match part("DSN") {
        Some(dsn) => Some(dsn),
        None => assemble(&part, prefix)?,
    };
    let Some(dsn) = dsn else {
        return Ok((None, false));
    };
    let (tls, plain_remote) = tls_for(get, prefix, &dsn)?;
    let mut d = finish_dsn(&dsn, tls.as_deref(), prefix)?;
    if let Some(ca) = &ca {
        d.push(if d.contains('?') { '&' } else { '?' });
        d.push_str("tls_ca=");
        d.push_str(&encode(ca));
    }
    Ok((Some(d), plain_remote))
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

    pub(super) fn cfg(pairs: &[(&str, &str)]) -> Result<Config> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| m.get(k).cloned())
    }

    #[test]
    fn connection_forms() {
        // The loopback host keeps the assembly under test free of the TLS default (the
        // TLS table lives in `config_tls_default_tests`).
        let c = cfg(&[
            ("LAZYWF_HOST", "127.0.0.1"),
            ("LAZYWF_USER", "app"),
            ("LAZYWF_PASSWORD", "p@ss:w/rd"),
            ("LAZYWF_DATABASE", "shop"),
        ])
        .unwrap();
        assert_eq!(c.dsn, "mysql://app:p%40ss%3Aw%2Frd@127.0.0.1:3306/shop");
        assert_eq!(c.binlog_dsn, c.dsn);
        assert_eq!(c.query_dsn, c.dsn);
        assert!(!c.query_account);
        assert_eq!(c.schema, "shop");
        assert_eq!(c.instance, "default");
        assert_eq!(c.health_addr, Some(([127, 0, 0, 1], 8081).into()));
        assert_eq!(c.metrics_addr, None, "metrics are opt-in: unset stays off");
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
        assert_eq!(c.query_dsn, c.dsn);
        assert!(!c.query_account);
        assert!(cfg(&[]).is_err());
        assert!(cfg(&[("LAZYWF_DSN", "mysql://h/")]).is_err());
        assert!(cfg(&[("LAZYWF_DSN", "mysql://h/s"), ("LAZYWF_SERVER_ID", "0")]).is_err());
    }

    /// `LAZYWF_METRICS_ADDR` is opt-in: unset or `off`/empty is off entirely; a bad host:port is a
    /// refused start (the listener would otherwise bind the wrong place).
    #[test]
    fn metrics_addr_is_opt_in() {
        let on = cfg(&[
            ("LAZYWF_DSN", "mysql://h/s"),
            ("LAZYWF_METRICS_ADDR", "127.0.0.1:8082"),
        ])
        .unwrap();
        assert_eq!(on.metrics_addr, Some(([127, 0, 0, 1], 8082).into()));
        for off in ["off", "OFF", ""] {
            let c = cfg(&[("LAZYWF_DSN", "mysql://h/s"), ("LAZYWF_METRICS_ADDR", off)]).unwrap();
            assert_eq!(c.metrics_addr, None, "`{off}` must be off");
        }
        assert!(cfg(&[
            ("LAZYWF_DSN", "mysql://h/s"),
            ("LAZYWF_METRICS_ADDR", "nope")
        ])
        .is_err());
    }
}

#[cfg(test)]
mod allow_tests {
    use super::*;

    fn with_allowed(v: Option<&'static str>) -> Config {
        Config::from_lookup(|k| match k {
            "LAZYWF_DSN" => Some("mysql://u:p@db.test:3306/app".into()),
            "LAZYWF_ALLOWED_HOSTS" => v.map(Into::into),
            _ => None,
        })
        .unwrap()
    }

    /// Unset denies every host; `*` allows every host; a list is matched as written.
    #[test]
    fn allowed_hosts_denies_until_it_is_set() {
        for unset in [None, Some(""), Some("  ")] {
            let cfg = with_allowed(unset);
            let l = cfg.allowed_hosts.expect("always a list");
            assert!(l.is_deny_all(), "{unset:?}");
            assert!(!l.allows("api.test", 443));
            assert!(!l.allows("127.0.0.1", 6379));
        }
        let cfg = with_allowed(Some("*"));
        assert!(cfg.allowed_hosts.as_ref().unwrap().is_allow_all());
        assert!(format!("{cfg:?}").contains("allowed_hosts: Some(1)"));
        let cfg = with_allowed(Some("api.test, *.cdn.test, 10.0.0.0/8:443"));
        assert_eq!(cfg.allowed_hosts.as_ref().map(|l| l.len()), Some(3));
        assert!(format!("{cfg:?}").contains("allowed_hosts: Some(3)"));
        let err = format!(
            "{:#}",
            Config::from_lookup(|k| match k {
                "LAZYWF_DSN" => Some("mysql://u:p@db.test:3306/app".into()),
                "LAZYWF_ALLOWED_HOSTS" => Some("bad host name".into()),
                _ => None,
            })
            .unwrap_err()
        );
        assert!(err.contains("LAZYWF_ALLOWED_HOSTS"), "{err}");
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn debug_shows_the_dsn_without_its_password() {
        let cfg = Config::from_lookup(|k| match k {
            "LAZYWF_DSN" => Some("mysql://user:DBPW-debug-1@db.example:3306/app".into()),
            _ => None,
        })
        .unwrap();
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("DBPW-debug-1"), "{shown}");
        assert!(shown.contains("mysql://***@db.example:3306/app"), "{shown}");
    }
}

#[cfg(test)]
mod query_tests {
    use super::tests::cfg;

    // The hosts are loopback: these tests are about which account a DSN carries, and a loopback
    // source keeps the DSN free of the TLS parameters (`tls_default_tests` covers the TLS side).
    #[test]
    fn query_account_user_and_password_on_the_main_dsn() {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop"),
            ("LAZYWF_QUERY_USER", "lwf_query"),
            ("LAZYWF_QUERY_PASSWORD", "q p"),
        ])
        .unwrap();
        assert!(c.query_account);
        assert_eq!(c.query_dsn, "mysql://lwf_query:q%20p@127.0.0.1:3306/shop");
        assert_eq!(c.dsn, "mysql://app:pw@127.0.0.1:3306/shop");
        assert_eq!(c.binlog_dsn, c.dsn);
        assert_eq!(c.schema, "shop");
    }

    #[test]
    fn query_account_user_without_password_keeps_the_host() {
        let c = cfg(&[
            ("LAZYWF_HOST", "127.0.0.2"),
            ("LAZYWF_PORT", "3307"),
            ("LAZYWF_USER", "app"),
            ("LAZYWF_PASSWORD", "pw"),
            ("LAZYWF_DATABASE", "shop"),
            ("LAZYWF_QUERY_USER", "lwf_query"),
        ])
        .unwrap();
        assert_eq!(c.query_dsn, "mysql://lwf_query@127.0.0.2:3307/shop");
    }

    #[test]
    fn query_dsn_takes_precedence_over_query_user() {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@db:3306/shop"),
            ("LAZYWF_QUERY_DSN", "mariadb://ro:rp@other:3308/shop"),
            ("LAZYWF_QUERY_USER", "ignored"),
            ("LAZYWF_QUERY_PASSWORD", "ignored"),
            ("LAZYWF_TLS", "required"),
        ])
        .unwrap();
        assert!(c.query_account);
        assert_eq!(
            c.query_dsn,
            "mysql://ro:rp@other:3308/shop?require_ssl=true&verify_ca=false&verify_identity=false"
        );
    }

    #[test]
    fn empty_query_settings_mean_no_query_account() {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@db:3306/shop"),
            ("LAZYWF_QUERY_DSN", ""),
            ("LAZYWF_QUERY_USER", ""),
        ])
        .unwrap();
        assert!(!c.query_account);
        assert_eq!(c.query_dsn, c.dsn);
    }

    #[test]
    fn debug_shows_the_query_dsn_without_its_password() {
        let c = cfg(&[
            ("LAZYWF_DSN", "mysql://app:pw@db:3306/shop"),
            ("LAZYWF_QUERY_USER", "lwf_query"),
            ("LAZYWF_QUERY_PASSWORD", "QPW-debug-7"),
        ])
        .unwrap();
        let shown = format!("{c:?}");
        assert!(!shown.contains("QPW-debug-7"), "{shown}");
        assert!(shown.contains("mysql://***@db:3306/shop"), "{shown}");
        assert!(shown.contains("query_account: true"), "{shown}");
    }
}

/// A wrong secret or address is refused when the process reads its environment, never
/// discovered halfway through a flush.
#[cfg(test)]
#[path = "tests/config_secret_refusals_tests.rs"]
mod secret_refusals_tests;

/// The TLS that 0.3.0 requires by default, connection by connection.
#[cfg(test)]
#[path = "tests/config_tls_default_tests.rs"]
mod tls_default_tests;
