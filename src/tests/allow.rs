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
