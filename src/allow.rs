//! `LAZYWF_ALLOWED_HOSTS`: the opt-in allow-list of endpoint hosts. Every target endpoint's
//! effective host (after `?endpoint=` overrides and the derived AWS / Google service hosts) must
//! match a pattern. Matching is on the name lazywf connects to; nothing is resolved in DNS.

use std::net::IpAddr;

use anyhow::{anyhow, bail, Result};

/// The patterns of a set allow-list (never empty: an empty setting is no list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostList {
    patterns: Vec<Pattern>,
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
}

impl HostList {
    /// Parses the setting. `None` when it is blank (no restriction).
    pub fn parse(setting: &str) -> Result<Option<HostList>> {
        let patterns = setting
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(Pattern::parse)
            .collect::<Result<Vec<_>>>()?;
        Ok((!patterns.is_empty()).then_some(HostList { patterns }))
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
    /// `name`, `*.name`, `ip`, `ip/len`, each optionally with `:port`; an IPv6 address with a
    /// port is written in brackets: `[2001:db8::1]:443`, `[2001:db8::/32]:443`.
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
mod tests {
    use super::*;

    fn list(s: &str) -> HostList {
        HostList::parse(s).unwrap().unwrap()
    }

    #[test]
    fn blank_setting_is_no_list() {
        assert!(HostList::parse("").unwrap().is_none());
        assert!(HostList::parse("  , ,").unwrap().is_none());
        assert_eq!(list("a.test, b.test ,").len(), 2);
    }

    #[test]
    fn exact_names_ignore_case_and_a_trailing_dot() {
        let l = list("Api.Example.com");
        assert!(l.allows("api.example.com", 443));
        assert!(l.allows("API.EXAMPLE.COM", 443));
        assert!(l.allows("api.example.com.", 443));
        assert!(!l.allows("x.api.example.com", 443));
        assert!(!l.allows("example.com", 443));
    }

    #[test]
    fn wildcard_matches_subdomains_not_the_apex() {
        let l = list("*.example.com");
        assert!(l.allows("a.example.com", 80));
        assert!(l.allows("A.B.Example.COM", 80));
        assert!(!l.allows("example.com", 80));
        assert!(!l.allows("badexample.com", 80));
        assert!(!l.allows("a.example.org", 80));
    }

    #[test]
    fn wildcard_needs_a_name_after_the_star() {
        assert!(HostList::parse("*.").is_err());
        assert!(HostList::parse("*.example.com:443:1").is_err());
        assert!(HostList::parse("*").is_err());
        assert!(HostList::parse("a.*.com").is_err());
    }

    #[test]
    fn ports_restrict_the_pattern() {
        let l = list("api.example.com:8443, *.example.org:443");
        assert!(l.allows("api.example.com", 8443));
        assert!(!l.allows("api.example.com", 443));
        assert!(l.allows("q.example.org", 443));
        assert!(!l.allows("q.example.org", 80));
        let no_port = list("api.example.com");
        assert!(no_port.allows("api.example.com", 1));
    }

    #[test]
    fn ipv4_literals_and_ranges() {
        let l = list("10.1.2.3, 192.168.0.0/16");
        assert!(l.allows("10.1.2.3", 80));
        assert!(!l.allows("10.1.2.4", 80));
        assert!(l.allows("192.168.200.9", 80));
        assert!(!l.allows("192.169.0.1", 80));
        // A name never matches a range, and an IP never matches a name pattern.
        assert!(!l.allows("localhost", 80));
        assert!(!list("localhost").allows("127.0.0.1", 80));
        assert!(list("0.0.0.0/0").allows("8.8.8.8", 53));
        assert!(!list("0.0.0.0/0").allows("::1", 53));
    }

    #[test]
    fn ipv6_literals_ranges_and_ports() {
        let l = list("2001:db8::1, [fd00::/8]:9000, [2001:db8:1::/48]");
        assert!(l.allows("2001:db8::1", 1));
        assert!(l.allows("[2001:DB8::1]", 1));
        assert!(!l.allows("2001:db8::2", 1));
        assert!(l.allows("fd12:3456::9", 9000));
        assert!(!l.allows("fd12:3456::9", 9001));
        assert!(l.allows("2001:db8:1:ffff::5", 443));
        assert!(!l.allows("2001:db8:2::5", 443));
        // An IPv4-mapped address is the IPv4 host it names.
        assert!(list("10.0.0.0/8").allows("::ffff:10.9.8.7", 1));
        assert!(list("10.0.0.0/8:443").allows("10.1.1.1", 443));
        assert!(!list("10.0.0.0/8:443").allows("10.1.1.1", 80));
    }

    #[test]
    fn ipv6_patterns_with_bracketed_ports() {
        let l = list("[2001:db8::1]:443");
        assert!(l.allows("2001:db8::1", 443));
        assert!(!l.allows("2001:db8::1", 80));
        assert!(HostList::parse("[2001:db8::1]x").is_err());
        assert!(HostList::parse("[2001:db8::1").is_err());
        assert!(HostList::parse("host.test:0").is_err());
        assert!(HostList::parse("host.test:http").is_err());
    }

    #[test]
    fn bad_ranges_are_refused() {
        assert!(HostList::parse("10.0.0.0/33").is_err());
        assert!(HostList::parse("fd00::/129").is_err());
        assert!(HostList::parse("example.com/8").is_err());
    }
}
