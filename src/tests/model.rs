use super::*;

#[test]
fn selects_forms() {
    let s = parse_selects(Some("SELECT 1 AS _key; SELECT 'a;b' AS _key;")).unwrap();
    assert_eq!(s.len(), 2);
    assert_eq!(s[1].sql, "SELECT 'a;b' AS _key");
    let s = parse_selects(Some(
        r#"["SELECT 1 AS _key", {"name": "prices", "sql": "SELECT 2 AS _key;"}]"#,
    ))
    .unwrap();
    assert_eq!(
        (s[0].name.as_str(), s[1].name.as_str(), s[1].sql.as_str()),
        ("select 1", "prices", "SELECT 2 AS _key")
    );
    assert!(parse_selects(None).unwrap().is_empty());
}

#[test]
fn steps() {
    let s = parse_reset_steps(Some(
            r#"[{"once": true, "template": "FLUSHDB"},
                {"for_each": "snapshot"},
                {"for_each": "stray", "list_template": "GET /", "keys": "JSON_EXTRACT(body, '$.hits.hits[*]._id')",
                 "next": "JSON_EXTRACT(body, '$.hits.hits[last].sort')"}]"#,
        ))
        .unwrap();
    assert_eq!(s[0].kind, StepKind::Once);
    assert_eq!(s[1].kind, StepKind::Snapshot);
    assert!(matches!(
        &s[2].kind,
        StepKind::Stray { keys, next: Some(_), .. } if keys.starts_with("JSON_EXTRACT")
    ));
    assert!(parse_reset_steps(Some(r#"[{"once": true}]"#)).is_err());
    assert!(parse_reset_steps(Some(r#"[{"for_each": "stray"}]"#)).is_err());
    assert!(parse_reset_steps(Some(
            r#"[{"for_each": "stray", "list_template": "GET /", "keys": "JSON_EXTRACT(body, '$.ids'); DROP TABLE t"}]"#
        ))
        .is_err());
    assert!(parse_reset_steps(Some(r#"[{"template": "x"}]"#)).is_err());
}

#[test]
fn targets() {
    let t = TargetDef::from_row(&RawTarget {
            name: "s3".into(),
            kind: Some("http.s3".into()),
            vars: Some(r#"{"bucket":"b","region":"r","access_key_ref":"env:A","secret_key_ref":"env:S","prefix":"p"}"#.into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        t.template_vars(),
        serde_json::json!({"bucket":"b","region":"r","prefix":"p"})
    );
    let lit = RawTarget {
        name: "s3".into(),
        kind: Some("http.s3".into()),
        vars: Some(
            r#"{"bucket":"b","region":"r","access_key_ref":"AKIA","secret_key_ref":"env:S"}"#
                .into(),
        ),
        ..Default::default()
    };
    assert!(TargetDef::from_row(&lit).is_err());
    let basic = RawTarget {
        name: "es".into(),
        kind: Some("http.basic".into()),
        endpoints: Some("http://a:9200, http://b:9200".into()),
        vars: Some(r#"{"username":"u"}"#.into()),
        ..Default::default()
    };
    assert!(TargetDef::from_row(&basic).is_err());
    let redis = TargetDef::from_row(&RawTarget {
        name: "r".into(),
        kind: Some("redis".into()),
        endpoints: Some(r#"["cache:6379/0","cache:6379/1"]"#.into()),
        vars: Some(r#"{"password":"env:REDIS_PW","prefix":"69d_"}"#.into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(redis.endpoints.len(), 2);
    assert_eq!(redis.template_vars(), serde_json::json!({"prefix":"69d_"}));
}

#[test]
fn signing_vars_load_and_hide() {
    let row = |kind: &str, endpoint: &str, vars: &str| RawTarget {
        name: "hook".into(),
        kind: Some(kind.into()),
        endpoints: Some(endpoint.into()),
        vars: Some(vars.into()),
        ..Default::default()
    };
    let http = |vars: &str| row("http", "http://a.example", vars);
    let t = TargetDef::from_row(&http(
        r#"{"sign": "standard", "sign_secret_ref": "env:HOOK_SECRET", "note": "visible"}"#,
    ))
    .unwrap();
    // The secret reference never reaches templates; the scheme and the rest do.
    assert_eq!(
        t.template_vars(),
        serde_json::json!({"sign": "standard", "note": "visible"})
    );
    let e = |vars: &str| TargetDef::from_row(&http(vars)).unwrap_err().to_string();
    // The load checks name the setting.
    assert!(e(r#"{"sign": "ed25519", "sign_secret_ref": "env:A"}"#).contains("`vars.sign`"));
    assert!(e(r#"{"sign": "standard"}"#).contains("`vars.sign_secret_ref`"));
    assert!(
        e(r#"{"sign": "standard", "sign_secret_ref": "a-literal"}"#).contains("never a literal")
    );
    // The settings are refused by the kinds that sign their own way or not at all.
    let sqs_err = TargetDef::from_row(&row(
        "sqs",
        "aws-sqs://eu-west-1/123456789012/orders",
        r#"{"sign": "hmac", "sign_secret_ref": "env:A"}"#,
    ))
    .unwrap_err()
    .to_string();
    assert!(sqs_err.contains("`vars.sign`"), "{sqs_err}");
    // A plain http target without the settings stays fine.
    assert!(TargetDef::from_row(&http("{}")).is_ok());
}

#[test]
fn triggers() {
    let t = TriggerDef::from_row(
        &RawTrigger {
            id: 1,
            rule: "r".into(),
            enabled: true,
            source_table: "`Shop`.`T`".into(),
            on: Some("insert, delete".into()),
            key: Some("item_{{new.id}}".into()),
            ..Default::default()
        },
        "app",
        true,
    )
    .unwrap();
    assert_eq!((t.schema.as_str(), t.table.as_str()), ("shop", "t"));
    assert_eq!(
        t.on,
        On {
            insert: true,
            update: false,
            delete: true
        }
    );
    let bad = RawTrigger {
        id: 2,
        source_table: "t".into(),
        keys_sql: Some("SELECT :x AS _key".into()),
        ..Default::default()
    };
    assert!(TriggerDef::from_row(&bad, "app", false).is_err());
}
