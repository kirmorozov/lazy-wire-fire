use super::*;

const MARKER: &str = "SECMARK-6f1a";

/// The whole URL of a webhook, with a path that is the secret.
const WEBHOOK: &str = "https://hooks.example.test/services/T0000/B0000/SECMARK-6f1a";

fn parse(s: &str) -> SecretUrl {
    SecretUrl::parse(s).unwrap()
}

#[test]
fn a_full_https_url_keeps_its_origin_and_holds_its_path_and_query() {
    let u = parse(WEBHOOK);
    assert_eq!(
        (u.base.secure, u.base.host.as_str(), u.base.port),
        (true, "hooks.example.test", 443)
    );
    assert_eq!(u.path, "/services/T0000/B0000/SECMARK-6f1a");
    assert_eq!(u.query, "");
    // What the rest of the program is allowed to show.
    assert_eq!(u.base.origin(), "https://hooks.example.test/…");

    let u = parse("http://127.0.0.1:34599/hooks/a/?team=42&key=SECMARK-6f1a");
    assert_eq!((u.base.host.as_str(), u.base.port), ("127.0.0.1", 34599));
    assert_eq!(u.path, "/hooks/a");
    assert_eq!(u.query, "team=42&key=SECMARK-6f1a");
    assert_eq!(u.base.origin(), "http://127.0.0.1:34599/…");

    // An origin with nothing below it, and a bracketed IPv6.
    let u = parse("https://[::1]");
    assert_eq!((u.path.as_str(), u.query.as_str()), ("", ""));
    assert_eq!(u.base.origin(), "https://[::1]/…");
    // Surrounding whitespace a secret file may hold.
    assert_eq!(parse("  https://h/a  ").path, "/a");
}

#[test]
fn a_url_that_is_not_a_plain_http_one_is_refused_without_naming_it() {
    // The whole chain: a load error is shown that way too, in the log and in `last_error`.
    let err = |s: &str| format!("{:#}", SecretUrl::parse(s).unwrap_err());
    // Not a full http(s) URL.
    assert!(err("hooks.example.test/services/T/B/x").contains("full http(s) URL"));
    assert!(err("ftp://h/SECMARK-6f1a").contains("full http(s) URL"));
    assert!(err("").contains("full http(s) URL"));
    // A credential in the URL is a load error, and the message names neither it nor
    // the host: every `BaseUrl::parse` error becomes one fixed message.
    let e = err("https://u:SECMARK-6f1a@h/x");
    assert!(e.contains("not valid"), "{e}");
    assert!(!e.contains(MARKER), "{e}");
    // A fragment is never sent, so it cannot be part of the endpoint.
    assert!(err("https://h/x#SECMARK-6f1a").contains("fragment"));
    // A space or a control character is how a second header smuggles itself in.
    for bad in ["https://h/a b", "https://h/a\u{1}b", "https://h/a\tb"] {
        assert!(err(bad).contains("control character"), "{bad}");
    }
    // A broken address: one fixed message, whatever `BaseUrl::parse` would have said.
    for bad in ["https://", "https://h:notaport/x"] {
        assert!(err(bad).contains("not valid"), "{bad}");
    }

    // Nothing that is refused carries the secret out in the message. The userinfo with a
    // `/` in it is the sharp case: the cut at the first `/` leaves `user:SECMARK-6f1a` as
    // the authority, and `BaseUrl::parse` would answer `invalid port `SECMARK-6f1a``.
    for bad in [
        "ftp://h/SECMARK-6f1a",
        "https://u:SECMARK-6f1a@h/x",
        "https://user:SECMARK-6f1a/ss@h/x",
        "https://h/x#SECMARK-6f1a",
        "https://h/a SECMARK-6f1a",
    ] {
        let e = err(bad);
        assert!(!e.contains(MARKER), "`{bad}` is shown as `{e}`");
    }
}

#[test]
fn a_debug_shows_the_origin_and_an_ellipsis_and_nothing_else() {
    // Path and query both hold the marker; neither may reach the `{:?}`.
    let u = parse("https://hooks.example.test/services/T0000/SECMARK-6f1a?key=SECMARK-6f1a");
    let d = format!("{u:?}");
    assert_eq!(d, "SecretUrl(https://hooks.example.test/…)");
    assert!(!d.contains(MARKER), "{d}");
}

#[test]
fn the_endpoint_s_path_and_query_join_the_request_line() {
    let u = parse(WEBHOOK);
    // `POST /` calls the webhook exactly as given: no trailing slash, nothing appended.
    assert_eq!(u.target("/"), "/services/T0000/B0000/SECMARK-6f1a");
    // Any other target goes below the endpoint's path.
    assert_eq!(
        u.target("/media/1.png"),
        "/services/T0000/B0000/SECMARK-6f1a/media/1.png"
    );
    // `*` asks for no path.
    assert_eq!(u.target("*"), "*");

    // The query comes last, whatever the template's target carried.
    let u = parse("https://chat.example.test/v1/spaces/AAA/messages?key=SECMARK-6f1a&token=BBB");
    assert_eq!(
        u.target("/"),
        "/v1/spaces/AAA/messages?key=SECMARK-6f1a&token=BBB"
    );
    assert_eq!(
        u.target("/x?a=1"),
        "/v1/spaces/AAA/messages/x?a=1&key=SECMARK-6f1a&token=BBB"
    );
    assert_eq!(
        u.target("/x?"),
        "/v1/spaces/AAA/messages/x?key=SECMARK-6f1a&token=BBB"
    );
    // An origin with no path leaves the request line as the template wrote it.
    let u = parse("http://localhost:9/");
    assert_eq!(u.target("/"), "/");
    assert_eq!(u.target("/a?b=1"), "/a?b=1");
    // A trailing slash on the endpoint never reaches the wire twice.
    let u = parse("https://h/hooks/a///");
    assert_eq!(u.target("/"), "/hooks/a");
}

#[test]
fn the_joined_target_is_the_only_place_the_secret_path_appears() {
    // `send_to` writes it into the request line; `to_bytes` on its own keeps what the
    // template rendered, which holds no secret.
    let u = parse(WEBHOOK);
    let req = RawRequest::parse("POST / HTTP/1.1\n\nok").unwrap().unwrap();
    assert_eq!(req.target, "/");
    let wire = String::from_utf8(req.to_bytes_with(&u.target(&req.target))).unwrap();
    assert!(
        wire.starts_with("POST /services/T0000/B0000/SECMARK-6f1a HTTP/1.1\r\n"),
        "{wire}"
    );
    assert!(String::from_utf8(req.to_bytes())
        .unwrap()
        .starts_with("POST / HTTP/1.1\r\n"));
    assert!(!format!("{req:?}").contains(MARKER));
}
