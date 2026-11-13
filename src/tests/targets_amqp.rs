use super::*;
use crate::model::RawTarget;

fn spec(s: &str) -> TlsSpec {
    TlsSpec::parse(Some(s)).unwrap()
}

fn raw(endpoints: &str, tls: Option<&str>, vars: &str) -> RawTarget {
    RawTarget {
        name: "events".into(),
        kind: Some("amqp".into()),
        endpoints: Some(endpoints.into()),
        tls: tls.map(Into::into),
        timeout_ms: None,
        expect: None,
        vars: Some(vars.into()),
    }
}

const CREDS: &str = r#"{"username": "lwf", "password": "env:LWF_TEST_AMQP_PW"}"#;

#[test]
fn rendered_form() {
    let m = Message::parse(
            "PUBLISH /lwf.orders/order.changed\nMessage-Id: 42\n\n{\"key\":\"42\",\"op\":\"upsert\",\"doc\":{\"qty\":3}}\n",
        )
        .unwrap()
        .unwrap();
    assert_eq!(m.exchange, "lwf.orders");
    assert_eq!(m.routing_key, "order.changed");
    assert_eq!(m.message_id, "42");
    assert_eq!(m.body, br#"{"key":"42","op":"upsert","doc":{"qty":3}}"#);

    // CRLF, and the default exchange.
    let m = Message::parse("PUBLISH //queue\r\nMessage-Id: k\r\n\r\n{}\r\n")
        .unwrap()
        .unwrap();
    assert_eq!((m.exchange.as_str(), m.routing_key.as_str()), ("", "queue"));
    assert_eq!(m.body, b"{}");

    // A routing key may hold slashes: the exchange is the segment before the first one.
    let m = Message::parse("PUBLISH /x/a/b\nMessage-Id: k\n\n[]")
        .unwrap()
        .unwrap();
    assert_eq!(m.routing_key, "a/b");

    assert!(Message::parse(" \n\t").unwrap().is_none());
}

#[test]
fn malformed_messages_are_refused() {
    for bad in [
        "POST /x/y\nMessage-Id: k\n\n{}",
        "PUBLISH x/y\nMessage-Id: k\n\n{}",
        "PUBLISH /xy\nMessage-Id: k\n\n{}",
        "PUBLISH /x/y\n\n{}",
        "PUBLISH /x/y\nMessage-Id: k\n\nnot json",
        "PUBLISH /x/y\nMessage-Id: k\nX-Other: 1\n\n{}",
        "PUBLISH /x/y\nMessage-Id: k\n",
        "PUBLISH /x/y\nMessage-Id: k",
        "PUBLISH /x/y\nMessage-Id: k\nno colon here\n\n{}",
        "PUBLISH /x/y\nMessage-Id: k\n\n",
    ] {
        assert!(Message::parse(bad).is_err(), "accepted {bad:?}");
    }
    // A key with a line break cannot add a header.
    assert!(Message::parse("PUBLISH /x/y\nMessage-Id: a\nX-Evil: 1\n\n{}").is_err());
    // AMQP short strings.
    let long = "k".repeat(256);
    assert!(Message::parse(&format!("PUBLISH /x/y\nMessage-Id: {long}\n\n{{}}")).is_err());
    assert!(Message::parse(&format!("PUBLISH /{long}/y\nMessage-Id: k\n\n{{}}")).is_err());
}

#[test]
fn outcomes_and_rows() {
    assert_eq!(Confirm::Ack.outcome(), Outcome::Sent);
    let ret = Confirm::Return {
        reply_code: 312,
        reply_text: "NO_ROUTE".into(),
    };
    match ret.outcome() {
        Outcome::Refused(w) => assert!(w.contains("312") && w.contains("NO_ROUTE"), "{w}"),
        o => panic!("{o:?}"),
    }
    assert!(matches!(Confirm::Nack.outcome(), Outcome::Transient(_)));

    let all = Uses {
        status: true,
        headers: false,
        body: true,
        text: true,
        affected_rows: false,
    };
    let r = Confirm::Ack.row(all);
    assert_eq!(
        (r.status, r.text.as_deref(), r.body),
        (Some(200), Some("ack"), None)
    );
    let r = ret.row(all);
    assert_eq!(r.status, Some(312));
    assert_eq!(r.text.as_deref(), Some("return"));
    assert!(r.body.unwrap().contains("NO_ROUTE"));
    let r = Confirm::Nack.row(all);
    assert_eq!((r.status, r.text.as_deref()), (None, Some("nack")));
    // Only the columns `expect` names are filled.
    let r = Confirm::Ack.row(Uses::default());
    assert_eq!((r.status, r.text, r.body), (None, None, None));
}

#[test]
fn endpoints() {
    let a = Addr::parse("amqp://broker").unwrap();
    assert_eq!(
        (a.secure, a.host.as_str(), a.port, a.vhost.as_str()),
        (false, "broker", 5672, "/")
    );
    let a = Addr::parse("amqps://broker.internal:5671/orders%2Fv2").unwrap();
    assert_eq!(
        (a.secure, a.port, a.vhost.as_str()),
        (true, 5671, "orders/v2")
    );
    assert_eq!(Addr::parse("amqps://b").unwrap().port, 5671);
    assert_eq!(Addr::parse("amqp://h:5672/").unwrap().vhost, "/");
    for bad in [
        "http://h",
        "amqp://u:p@h",
        "amqp://h/x?heartbeat=1",
        "amqp://h:x",
        "amqp://[::1]",
        "amqp:///x",
        "amqp://h/%zz",
    ] {
        assert!(Addr::parse(bad).is_err(), "accepted {bad}");
    }
}

#[test]
fn tls_modes_map_or_refuse() {
    assert!(tls_config(&spec(""), false).unwrap().is_none());
    assert!(tls_config(&spec(""), true).unwrap().is_some());
    assert!(tls_config(&spec("on"), false).unwrap().is_some());
    let cfg = tls_config(&spec(r#"{"mode": "verify-full", "ca": []}"#), true)
        .unwrap()
        .unwrap();
    assert_eq!(cfg.cert_chain, None);

    let e = tls_config(&spec("required"), false)
        .unwrap_err()
        .to_string();
    assert!(e.contains("`required`"), "{e}");
    let e = tls_config(&spec("verify-ca"), true)
        .unwrap_err()
        .to_string();
    assert!(e.contains("`verify-ca`"), "{e}");
    let e = tls_config(
        &spec(r#"{"mode": "verify-full", "server_name": "b"}"#),
        true,
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("server_name"), "{e}");
    let e = tls_config(&spec("off"), true).unwrap_err().to_string();
    assert!(e.contains("disabled"), "{e}");
    assert!(tls_config(&spec(r#"{"ca": "/nonexistent/lwf-ca.pem"}"#), true).is_err());
}

#[test]
fn from_row_checks() {
    let ok = TargetDef::from_row(&raw("amqp://broker:5672/", None, CREDS)).unwrap();
    assert_eq!(ok.kind, crate::model::Kind::Amqp);
    assert!(ok.template_vars().get("password").is_none());
    assert!(ok.template_vars().get("username").is_some());
    assert!(TargetDef::from_row(&raw("amqp://broker", None, r#"{"username": "a"}"#)).is_err());
    assert!(TargetDef::from_row(&raw("amqp://u:p@broker", None, CREDS)).is_err());
    let e = TargetDef::from_row(&raw("amqp://broker", Some("required"), CREDS))
        .unwrap_err()
        .to_string();
    assert!(e.contains("`required`"), "{e:#}");
    assert!(TargetDef::from_row(&raw("amqps://broker", Some("off"), CREDS)).is_err());
    assert!(crate::model::Kind::Amqp.append_only());
}

#[test]
fn batch_is_refused() {
    assert_eq!(rule_errors(FirePer::Batch).len(), 1);
    assert!(rule_errors(FirePer::Key).is_empty());
}
