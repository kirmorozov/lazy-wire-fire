use super::*;
use crate::model::RawTarget;

fn spec(s: &str) -> TlsSpec {
    TlsSpec::parse(Some(s)).unwrap()
}

fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
    RawTarget {
        name: "mail".into(),
        kind: Some("smtp".into()),
        endpoints: Some(endpoints.into()),
        tls: tls.map(Into::into),
        timeout_ms: None,
        expect: None,
        vars: Some(vars.into()),
    }
}

#[test]
fn rendered_form() {
    let m = Message::parse(
            "From: sender@example.com\nTo: a@example.com, b@example.com\nCc: c@example.com\n\
             Subject: Order 1\nReply-To: other@example.com\nContent-Type: text/plain; charset=utf-8\n\n\
             {\"key\":\"1\",\"doc\":{\"sku\":\"A-1\"}}\n",
            50,
        )
        .unwrap()
        .unwrap();
    assert_eq!(m.from, "sender@example.com");
    assert_eq!(m.to, vec!["a@example.com", "b@example.com"]);
    assert_eq!(m.cc, vec!["c@example.com"]);
    assert_eq!(m.subject.as_deref(), Some("Order 1"));
    assert_eq!(m.reply_to.as_deref(), Some("other@example.com"));
    assert_eq!(m.content_type.as_deref(), Some("text/plain; charset=utf-8"));
    assert_eq!(m.body, r#"{"key":"1","doc":{"sku":"A-1"}}"#);

    // CRLF, and the minimal form.
    let m = Message::parse("From: a@x.com\r\nTo: b@x.com\r\n\r\n{}\r\n", 50)
        .unwrap()
        .unwrap();
    assert_eq!((m.subject, m.cc, m.content_type), (None, Vec::new(), None));
    assert_eq!(m.body, "{}");

    // Header names are case-insensitive, in any order.
    let m = Message::parse("to: b@x.com\nFROM: a@x.com\n\nbody", 50)
        .unwrap()
        .unwrap();
    assert_eq!(m.from, "a@x.com");
    assert_eq!(m.to, vec!["b@x.com"]);

    assert!(Message::parse(" \n\t", 50).unwrap().is_none());
}

#[test]
fn malformed_messages_are_refused() {
    for bad in [
        // unknown / repeated / missing headers
        "From: a@x.com\nTo: b@x.com\nX-Evil: 1\n\nbody",
        "From: a@x.com\nFrom: c@x.com\nTo: b@x.com\n\nbody",
        "To: b@x.com\n\nbody",
        "From: a@x.com\n\nbody",
        "From: a@x.com\nTo: b@x.com\n",
        "From: a@x.com\nTo: b@x.com",
        // addresses
        "From: a\nTo: b@x.com\n\nbody",
        "From: @x.com\nTo: b@x.com\n\nbody",
        "From: a@x.com\nTo: , b@x.com\n\nbody",
        "From: a@x.com\nTo: b@x.com,\n\nbody",
        "From: a@b@c.com\nTo: b@x.com\n\nbody",
        // control characters and folding: a header is one line
        "From: a@x.com\nTo: b@x.com\nX\n\nbody",
        "From: a@x.com\n To: b@x.com\n\nbody",
        "From: a@x.com\nTo: b@x.com\n\0\n\nbody",
        // content types
        "From: a@x.com\nTo: b@x.com\nContent-Type: application/json\n\nbody",
        "From: a@x.com\nTo: b@x.com\nContent-Type: text/plain; charset=ascii\n\nbody",
        "From: a@x.com\nTo: b@x.com\nContent-Type: text/plain; name=x\n\nbody",
        "From: a@x.com\nTo: b@x.com\nContent-Type: text/plain;\n\nbody",
    ] {
        assert!(Message::parse(bad, 50).is_err(), "accepted {bad:?}");
    }
    // text/html is a content type of its own.
    assert!(Message::parse(
        "From: a@x.com\nTo: b@x.com\nContent-Type: text/html\n\n<body>",
        50
    )
    .is_ok());
    // a control character in the body is fine: only the header lines are checked.
    assert!(Message::parse("From: a@x.com\nTo: b@x.com\n\nbody\u{1}", 50).is_ok());
}

#[test]
fn the_recipient_limit() {
    let many = |n: usize| -> String {
        let list = (0..n)
            .map(|i| format!("m{i}@x.com"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("From: a@x.com\nTo: {list}\n\nbody")
    };
    assert!(Message::parse(&many(50), 50).is_ok());
    assert!(Message::parse(&many(51), 50).is_err());
    // To + Cc count together.
    let m = "From: a@x.com\nTo: b@x.com\nCc: c@x.com\n\nbody";
    assert!(Message::parse(m, 1).is_err());
    assert!(Message::parse(&many(10), 10).is_ok());
    let e = Message::parse(&many(51), 50).unwrap_err().to_string();
    assert!(e.contains("51") && e.contains("vars.recipients"), "{e}");
}

#[test]
fn the_wire_form() {
    // The message goes on the wire as rendered (CRLF-normalized): nothing is rebuilt.
    let src = "From: a@x.com\nTo: b@x.com, c@x.com\nSubject: s\n\
                   Content-Type: text/plain; charset=utf-8\n\nbody\n";
    let m = Message::parse(src, 50).unwrap().unwrap();
    assert_eq!(m.raw, "From: a@x.com\nTo: b@x.com, c@x.com\nSubject: s\nContent-Type: text/plain; charset=utf-8\n\nbody");
    assert_eq!(crlf(&m.raw), b"From: a@x.com\r\nTo: b@x.com, c@x.com\r\nSubject: s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nbody");
    assert_eq!(crlf("a\nb\r\nc\r\n"), b"a\r\nb\r\nc\r\n");
    assert_eq!(crlf("\n"), b"\r\n");
}

#[test]
fn reply_classes() {
    use lettre::transport::smtp::response::{Category, Code, Detail, Severity};
    let c = |s, c, d| Code {
        severity: s,
        category: c,
        detail: d,
    };
    let p = Severity::PositiveCompletion;
    let t = Severity::TransientNegativeCompletion;
    let pe = Severity::PermanentNegativeCompletion;
    let cat = Category::MailSystem;
    assert_eq!(code_i64(&c(p, cat, Detail::Zero)), 250);
    assert_eq!(code_i64(&c(t, cat, Detail::One)), 451);
    assert_eq!(code_i64(&c(pe, cat, Detail::One)), 551);
    let a = Reply::Accepted {
        code: 250,
        text: "2.0.0 Ok: queued".into(),
    };
    assert!(a.outcome().is_ok());
    let tr = Reply::Transient {
        code: 451,
        text: "try later".into(),
    };
    assert!(matches!(tr.outcome(), Err(Outcome::Transient(_))));
    let rf = Reply::Refused {
        code: 550,
        text: "no such user".into(),
    };
    match rf.outcome() {
        Err(Outcome::Refused(w)) => assert!(w.contains("550") && w.contains("no such user")),
        _ => panic!("{rf:?}"),
    }
    // The row `expect` reads: the code as `status`, the reply as `text`.
    let all = Uses {
        status: true,
        headers: false,
        body: false,
        text: true,
        affected_rows: false,
    };
    let r = a.row(all);
    assert_eq!(
        (r.status, r.text.as_deref()),
        (Some(250), Some("2.0.0 Ok: queued"))
    );
    let r = rf.row(all);
    assert_eq!(
        (r.status, r.text.as_deref()),
        (Some(550), Some("no such user"))
    );
    let r = a.row(Uses::default());
    assert_eq!((r.status, r.text), (None, None));
}

#[test]
fn endpoints() {
    let a = Addr::parse("smtp://mail").unwrap();
    assert_eq!((a.implicit, a.host.as_str(), a.port), (false, "mail", 587));
    let a = Addr::parse("smtps://mail.example:465").unwrap();
    assert_eq!((a.implicit, a.port), (true, 465));
    assert_eq!(Addr::parse("smtps://b").unwrap().port, 465);
    assert_eq!(Addr::parse("smtp://h:25").unwrap().port, 25);
    for bad in [
        "http://h",
        "smtp://u:p@h",
        "smtp://h/p",
        "smtp://h?x=1",
        "smtp://h#f",
        "smtp://h:x",
        "smtp://[::1]",
        "smtp://",
        "smtps://:465",
    ] {
        assert!(Addr::parse(bad).is_err(), "accepted {bad}");
    }
}

#[test]
fn tls_modes_map_or_refuse() {
    // auto: verified, both schemes; the CA files are parsed.
    assert!(tls_parameters(&spec(""), "h").unwrap().is_some());
    assert!(tls_parameters(&spec("verify-full"), "h").unwrap().is_some());
    assert!(tls_parameters(&spec("verify-ca"), "h").unwrap().is_some());
    assert!(tls_parameters(&spec("required"), "h").unwrap().is_some());
    assert!(tls_parameters(&spec("off"), "h").unwrap().is_none());
    let e = match tls_parameters(&spec(r#"{"mode": "verify-full", "server_name": "b"}"#), "h") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("accepted tls.server_name"),
    };
    assert!(e.contains("server_name"), "{e}");
    assert!(tls_parameters(&spec(r#"{"ca": "/nonexistent/lwf-ca.pem"}"#), "h").is_err());
}

#[test]
fn the_recipients_limit_var() {
    fn v(s: &str) -> Map<String, Value> {
        serde_json::from_str(s).unwrap()
    }
    assert_eq!(recipients_limit(&v("{}")).unwrap(), MAX_RECIPIENTS);
    assert_eq!(recipients_limit(&v(r#"{"recipients": 10}"#)).unwrap(), 10);
    assert_eq!(recipients_limit(&v(r#"{"recipients": "7"}"#)).unwrap(), 7);
    assert!(recipients_limit(&v(r#"{"recipients": 0}"#)).is_err());
    assert!(recipients_limit(&v(r#"{"recipients": "x"}"#)).is_err());
}

#[test]
fn from_row_checks() {
    let ok = TargetDef::from_row(&raw("smtp://mail.example", None, "{}")).unwrap();
    assert_eq!(ok.kind, crate::model::Kind::Smtp);
    // Anonymous, or both: the pair check.
    assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"username": "u"}"#)).is_err());
    assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"password": "p"}"#)).is_err());
    assert!(TargetDef::from_row(&raw(
        "smtp://m",
        None,
        r#"{"username": "u", "password": "env:X"}"#
    ))
    .is_ok());
    // vars.recipients.
    assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"recipients": "10"}"#)).is_ok());
    assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"recipients": "0"}"#)).is_err());
    assert!(TargetDef::from_row(&raw("smtp://m", None, r#"{"recipients": "x"}"#)).is_err());
    // userinfo is never in the URL.
    assert!(TargetDef::from_row(&raw("smtp://u:p@mail", None, "{}")).is_err());
    // smtps with disabled tls is a load error; smtp with it is plain.
    assert!(TargetDef::from_row(&raw("smtps://m", Some("off"), "{}")).is_err());
    assert!(TargetDef::from_row(&raw("smtp://m", Some("off"), "{}")).is_ok());
    let e = TargetDef::from_row(&raw("smtps://m", Some("off"), "{}"))
        .unwrap_err()
        .to_string();
    assert!(e.contains("disabled"), "{e}");
    assert!(crate::model::Kind::Smtp.append_only());
}

#[test]
fn batch_is_refused() {
    assert_eq!(rule_errors(FirePer::Batch).len(), 1);
    assert!(rule_errors(FirePer::Key).is_empty());
}
