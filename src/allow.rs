//! `LAZYWF_ALLOWED_HOSTS`: the allow-list of endpoint hosts. Every target endpoint's effective
//! host (after `?endpoint=` overrides and the derived AWS / Google service hosts) must match a
//! pattern. Matching is on the name lazywf connects to; nothing is resolved in DNS.
//!
//! Since 0.3.0 the setting is opt-out, not opt-in: unset means no host is allowed at all
//! ([`HostList::deny_all`] — the engine refuses every target at load), and the operator opts out
//! explicitly with `LAZYWF_ALLOWED_HOSTS=*` ([`HostList::allow_all`], the behaviour an unset
//! setting had before). The configuration rows are SQL: an INSERT grant must not be able to turn
//! a target into a channel to any address.

use std::net::IpAddr;

use anyhow::{anyhow, bail, Result};

/// The patterns of an allow-list, or its two special forms: refuse everything (the unset
/// default) and allow everything (`*`, the pre-0.3 unset behaviour).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostList {
    patterns: Vec<Pattern>,
    /// The setting was absent: every host is refused with a message that names it (the
    /// engine builds such a list when `LAZYWF_ALLOWED_HOSTS` is unset).
    deny_all: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Pattern {
    host: Host,
    /// `:port` on the pattern: the endpoint's port must be this one.
    port: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Host {
    /// An exact host name, lower case.
    Name(String),
    /// `*.suffix`: any name below `suffix` (`suffix` itself is not matched), lower case.
    Suffix(String),
    Ip(IpAddr),
    /// An IP range: the address and its prefix length.
    Cidr(IpAddr, u8),
    /// `*`: any host (the setting's whole value, the explicit opt-out of 0.3.0's refusal).
    Any,
}

impl HostList {
    /// Parses the setting. `None` when it is blank (the caller decides what unset means;
    /// the engine denies all egress, see [`HostList::deny_all`]). The single pattern `*`
    /// allows every host; a `*` among other patterns makes the list allow everything.
    pub fn parse(setting: &str) -> Result<Option<HostList>> {
        let mut patterns = setting
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(Pattern::parse)
            .collect::<Result<Vec<_>>>()?;
        if patterns.iter().any(|p| p.host == Host::Any) {
            patterns = vec![Pattern {
                host: Host::Any,
                port: None,
            }];
        }
        Ok((!patterns.is_empty()).then_some(HostList {
            patterns,
            deny_all: false,
        }))
    }

    /// No setting: every host is refused, and the refusal names the setting (0.3.0's
    /// default; the operator restores the old behaviour with `LAZYWF_ALLOWED_HOSTS=*`).
    pub fn deny_all() -> HostList {
        HostList {
            patterns: Vec::new(),
            deny_all: true,
        }
    }

    /// The `*` setting: every host is allowed (what an unset setting meant before 0.3.0).
    pub fn allow_all() -> HostList {
        HostList {
            patterns: vec![Pattern {
                host: Host::Any,
                port: None,
            }],
            deny_all: false,
        }
    }

    /// Whether this is the unset default (every host refused).
    pub fn is_deny_all(&self) -> bool {
        self.deny_all
    }

    /// Whether this is the `*` opt-out (every host allowed).
    pub fn is_allow_all(&self) -> bool {
        self.patterns.iter().any(|p| p.host == Host::Any)
    }

    /// The number of patterns (the only thing the log shows).
    pub fn len(&self) -> usize {
        self.patterns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Whether an endpoint at `host` (a name or an IP literal, brackets optional) and `port`
    /// is allowed.
    pub fn allows(&self, host: &str, port: u16) -> bool {
        let host = normalize(host);
        let ip = host.parse::<IpAddr>().ok().map(|a| a.to_canonical());
        self.patterns.iter().any(|p| p.matches(&host, ip, port))
    }
}

impl Pattern {
    /// `name`, `*.name`, `ip`, `ip/len`, `*` (any host), each optionally with `:port`; an
    /// IPv6 address with a port is written in brackets: `[2001:db8::1]:443`,
    /// `[2001:db8::/32]:443`.
    fn parse(p: &str) -> Result<Pattern> {
        let (spec, port) = if let Some(rest) = p.strip_prefix('[') {
            let (inner, tail) = rest
                .split_once(']')
                .ok_or_else(|| anyhow!("allow-list pattern `{p}`: unclosed `[`"))?;
            let port = match tail {
                "" => None,
                t => match t.strip_prefix(':') {
                    Some(n) => Some(port_number(n, p)?),
                    None => bail!("allow-list pattern `{p}`: unexpected `{tail}` after `]`"),
                },
            };
            (inner, port)
        } else if Host::parse(p, p).is_ok() {
            (p, None)
        } else {
            match p.rsplit_once(':') {
                Some((h, n)) => (h, Some(port_number(n, p)?)),
                None => (p, None),
            }
        };
        if spec == "*" {
            if port.is_some() {
                bail!("allow-list pattern `{p}`: `*` is the whole list, it takes no port");
            }
            return Ok(Pattern {
                host: Host::Any,
                port: None,
            });
        }
        Ok(Pattern {
            host: Host::parse(spec, p)?,
            port,
        })
    }

    fn matches(&self, host: &str, ip: Option<IpAddr>, port: u16) -> bool {
        if self.port.is_some_and(|p| p != port) {
            return false;
        }
        match &self.host {
            Host::Any => true,
            Host::Name(n) => host == n,
            Host::Suffix(s) => host.len() > s.len() + 1 && host.ends_with(&format!(".{s}")),
            Host::Ip(a) => ip == Some(a.to_canonical()),
            Host::Cidr(net, len) => ip.is_some_and(|ip| in_cidr(ip, *net, *len)),
        }
    }
}

impl Host {
    fn parse(spec: &str, whole: &str) -> Result<Host> {
        if let Some((addr, len)) = spec.split_once('/') {
            let net: IpAddr = addr.parse().map_err(|_| {
                anyhow!("allow-list pattern `{whole}`: `{addr}` is not an IP address")
            })?;
            let max = if net.is_ipv4() { 32 } else { 128 };
            let len: u8 = len.parse().ok().filter(|l| *l <= max).ok_or_else(|| {
                anyhow!("allow-list pattern `{whole}`: prefix length must be 0 to {max}")
            })?;
            return Ok(Host::Cidr(net.to_canonical(), len));
        }
        if let Ok(ip) = spec.parse::<IpAddr>() {
            return Ok(Host::Ip(ip.to_canonical()));
        }
        let name = normalize(spec);
        if let Some(suffix) = name.strip_prefix("*.") {
            if suffix.is_empty()
                || suffix.contains('*')
                || suffix
                    .chars()
                    .any(|c| c.is_whitespace() || c == '/' || c == ':')
            {
                bail!("allow-list pattern `{whole}`: `*.` takes a domain name");
            }
            return Ok(Host::Suffix(suffix.to_string()));
        }
        if name.is_empty()
            || name.contains('*')
            || name
                .chars()
                .any(|c| c.is_whitespace() || c == '/' || c == ':')
        {
            bail!(
                "allow-list pattern `{whole}`: not a host name, IP address, range or `*.` suffix"
            );
        }
        Ok(Host::Name(name))
    }
}

fn port_number(n: &str, whole: &str) -> Result<u16> {
    n.parse::<u16>()
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| anyhow!("allow-list pattern `{whole}`: `{n}` is not a port"))
}

/// Lower case, no brackets, no trailing dot.
fn normalize(host: &str) -> String {
    let h = host.trim();
    let h = h
        .strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .unwrap_or(h);
    h.trim_end_matches('.').to_ascii_lowercase()
}

fn in_cidr(ip: IpAddr, net: IpAddr, len: u8) -> bool {
    match (ip, net) {
        (IpAddr::V4(a), IpAddr::V4(n)) => {
            let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
            u32::from(a) & mask == u32::from(n) & mask
        }
        (IpAddr::V6(a), IpAddr::V6(n)) => {
            let mask = if len == 0 {
                0
            } else {
                u128::MAX << (128 - len)
            };
            u128::from(a) & mask == u128::from(n) & mask
        }
        _ => false,
    }
}

#[cfg(test)]
#[path = "tests/allow.rs"]
mod tests;
