use super::*;

fn err(text: &str) -> String {
    RawRequest::parse(text).unwrap_err().to_string()
}

#[test]
fn a_blank_render_is_nothing_to_send() {
    assert!(RawRequest::parse("").unwrap().is_none());
    assert!(RawRequest::parse("\r\n \t\n").unwrap().is_none());
}

#[test]
fn the_request_line_is_checked_word_by_word() {
    assert!(RawRequest::parse("GET /a HTTP/1.1\r\n\r\n")
        .unwrap()
        .is_some());
    assert!(RawRequest::parse("OPTIONS * HTTP/1.1\r\n\r\n")
        .unwrap()
        .is_some());
    assert!(err("GET\r\n").contains("has no path"));
    assert!(err("GET /a 1.1\r\n").contains("`1.1` is not an HTTP version"));
    assert!(err("GET /a HTTP/1.1 x\r\n").contains("extra words"));
    assert!(err("PO!ST /a HTTP/1.1\r\n").contains("`PO!ST` is not a method"));
    assert!(err("GET a/b HTTP/1.1\r\n").contains("the path must start with `/`"));
    // A stray control character is how a template smuggles extra lines in.
    assert!(err("GET /a\u{1} HTTP/1.1\r\n").contains("holds a control character"));
}

#[test]
fn headers_need_a_name_and_a_clean_value() {
    assert!(err("GET /a HTTP/1.1\r\nNoColon\r\n\r\n").contains("has no `:`"));
    assert!(err("GET /a HTTP/1.1\r\n: 1\r\n\r\n").contains("invalid header name"));
    assert!(err("GET /a HTTP/1.1\r\nX Y: 1\r\n\r\n").contains("invalid header name"));
    assert!(err("GET /a HTTP/1.1\r\nX: a\u{0}b\r\n\r\n").contains("control character"));

    let r = RawRequest::parse(
        "POST /a HTTP/1.1\r\nContent-Type: application/json\r\nX-Trace:  t1 \r\n\r\nhi",
    )
    .unwrap()
    .expect("a request");
    assert_eq!(r.header("content-TYPE"), Some("application/json"));
    assert_eq!(r.header("X-TRACE"), Some("t1"), "a value is trimmed");
    assert_eq!(r.header("Authorization"), None);
    assert_eq!(r.body, b"hi");
}

#[test]
fn the_only_content_length_on_the_wire_is_the_body_s() {
    let r = RawRequest {
        method: "GET".into(),
        target: "/orders/7".into(),
        headers: vec![
            ("Content-Length".into(), "999".into()),
            ("transfer-encoding".into(), "chunked".into()),
            ("Accept".into(), "application/json".into()),
        ],
        body: b"hello".to_vec(),
    };
    let wire = String::from_utf8(r.to_bytes()).unwrap();
    assert!(wire.starts_with("GET /orders/7 HTTP/1.1\r\n"), "{wire}");
    assert!(wire.contains("Accept: application/json\r\n"), "{wire}");
    assert!(
        wire.ends_with("\r\nContent-Length: 5\r\n\r\nhello"),
        "{wire}"
    );
    assert!(!wire.contains("999") && !wire.contains("chunked"), "{wire}");

    let mut r = RawRequest {
        method: "HEAD".into(),
        target: "/".into(),
        headers: vec![],
        body: vec![],
    };
    let wire = String::from_utf8(r.to_bytes()).unwrap();
    assert_eq!(
        wire, "HEAD / HTTP/1.1\r\n\r\n",
        "a bodyless call states no length"
    );
    r.method = "POST".into();
    assert!(String::from_utf8(r.to_bytes())
        .unwrap()
        .ends_with("Content-Length: 0\r\n\r\n"));
    r.body = b"ok".to_vec();
    assert!(String::from_utf8(r.to_bytes())
        .unwrap()
        .ends_with("Content-Length: 2\r\n\r\nok"));
}

#[test]
fn an_answer_is_read_case_insensitively_and_shown_short() {
    let resp = Response {
        status: 500,
        headers: vec![("Content-Type".into(), "text/plain".into())],
        body: b"  Bad Gateway \n".to_vec(),
    };
    assert_eq!(resp.header("CONTENT-type"), Some("text/plain"));
    assert_eq!(resp.header("Date"), None);
    assert_eq!(resp.body_snippet(), "Bad Gateway", "padded for the log");

    let long = Response {
        status: 502,
        headers: vec![],
        body: vec![b'x'; 400],
    };
    let shown = long.body_snippet();
    assert_eq!(shown.chars().count(), 301, "300 characters and an ellipsis");
    assert!(shown.ends_with('…'));

    // Clipping counts characters, so a multi-byte body is never cut in half.
    let long = Response {
        status: 200,
        headers: vec![],
        body: "é".repeat(350).into_bytes(),
    };
    assert_eq!(long.body_snippet().chars().count(), 301);

    let binary = Response {
        status: 200,
        headers: vec![],
        body: vec![0xff, b's', b'o', b's'],
    };
    assert_eq!(
        binary.body_snippet(),
        "\u{fffd}sos",
        "undecodable bytes are still shown"
    );
}

#[test]
fn an_address_is_split_the_way_it_is_written() {
    for (authority, default, want_host, want_port) in [
        ("example.com", 80u16, "example.com", 80u16),
        ("example.com:8443", 80, "example.com", 8443),
        ("[::1]", 6379, "::1", 6379),
        ("[::1]:6380", 6379, "::1", 6380),
    ] {
        assert_eq!(
            split_host_port(authority, default).unwrap(),
            (want_host.to_string(), want_port),
            "{authority}"
        );
    }
    for bad in ["[::1", "[::1]x", "[::1]:abc", ":80", "", "h:abc"] {
        assert!(
            split_host_port(bad, 80).is_err(),
            "`{bad}` is not an address"
        );
    }
    // A bracketless IPv6 is left whole rather than having its last group read as a port.
    assert_eq!(split_host_port("::1", 80).unwrap(), ("::1".to_string(), 80));
}
