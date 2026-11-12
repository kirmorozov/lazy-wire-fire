use super::*;

/// A row that says something impossible has to be refused while it is read: a
/// rule that loads but cannot fire loses keys silently, and the operator sees
/// only a worker that never delivers.
#[test]
fn every_kind_is_named_the_way_it_is_written() {
    let kinds = [
        Kind::Http,
        Kind::HttpBasic,
        Kind::HttpBearer,
        Kind::HttpHeader,
        Kind::HttpS3,
        Kind::Sqs,
        Kind::Kinesis,
        Kind::Lambda,
        Kind::EventBridge,
        Kind::Amqp,
        Kind::Smtp,
        Kind::PubSub,
        Kind::Kafka,
        Kind::RabbitMqStream,
        Kind::Redis,
        Kind::MySql,
        Kind::PgSql,
    ];
    for k in kinds {
        // `name()` is what `/status` and the error messages print, and what an
        // operator writes back into the table: it has to parse to the same kind.
        assert_eq!(Kind::parse(k.name()).unwrap(), k, "{}", k.name());
    }
    // Aliases an operator reaches for.
    for (text, kind) in [
        ("", Kind::Http),
        ("HTTP", Kind::Http),
        (" mariadb ", Kind::MySql),
        ("PostgreSQL", Kind::PgSql),
        ("postgres", Kind::PgSql),
    ] {
        assert_eq!(Kind::parse(text).unwrap(), kind, "{text:?}");
    }
    // A kind that does not exist is refused, and the message lists what does.
    let e = format!("{:#}", Kind::parse("rabbitmq").err().unwrap());
    assert!(e.contains("`rabbitmq`") && e.contains("amqp"), "{e}");

    // The size a batch is cut to, per kind: too large and the service refuses
    // the whole request, so every key of it would be refused at once.
    assert_eq!(Kind::Sqs.batch_bytes_limit(), Some(262_144));
    assert_eq!(Kind::Kinesis.batch_bytes_limit(), Some(5_242_880));
    assert_eq!(Kind::PubSub.batch_bytes_limit(), Some(10_000_000));
    assert_eq!(
        Kind::EventBridge.batch_bytes_limit(),
        Some(crate::aws::EVENTBRIDGE_MAX_BYTES)
    );
    assert_eq!(
        Kind::Kafka.batch_bytes_limit(),
        Some(crate::kafka::MESSAGE_MAX_BYTES)
    );
    assert_eq!(
        Kind::RabbitMqStream.batch_bytes_limit(),
        Some(crate::rabbitmq_stream::MESSAGE_MAX_BYTES)
    );
    for k in [Kind::Http, Kind::Redis, Kind::MySql, Kind::Smtp] {
        assert_eq!(k.batch_bytes_limit(), None, "{}", k.name());
    }
    // Only these kinds answer per item; the others answer for the whole action.
    for k in [
        Kind::Sqs,
        Kind::Kinesis,
        Kind::Lambda,
        Kind::Kafka,
        Kind::RabbitMqStream,
        Kind::EventBridge,
    ] {
        assert!(k.reports_items(), "{}", k.name());
    }
    for k in [Kind::Http, Kind::Redis, Kind::MySql, Kind::PubSub] {
        assert!(!k.reports_items(), "{}", k.name());
    }
}

#[test]
fn a_list_column_takes_json_or_plain_text() {
    // Items of a JSON array that are not strings (a number in a list of columns)
    // are still items: refusing the row would silently stop a trigger.
    assert_eq!(
        list(Some(r#"["a", 1, true, "", null]"#)).unwrap(),
        vec!["a", "1", "true", "null"]
    );
    // The same column written without JSON.
    assert_eq!(
        list(Some("  a, b\nc\td \r\n e")).unwrap(),
        vec!["a", "b", "c", "d", "e"]
    );
    assert!(list(None).unwrap().is_empty());
    assert!(list(Some("   ")).unwrap().is_empty());
    // A `[` that does not open an array is refused rather than read as items.
    assert!(list(Some("[oops")).is_err());
}

#[test]
fn a_json_column_must_be_an_object() {
    assert_eq!(json_object("vars", None).unwrap(), Map::new());
    assert_eq!(json_object("vars", Some("  ")).unwrap(), Map::new());
    assert_eq!(
        json_object("vars", Some(r#"{"a":1}"#)).unwrap(),
        serde_json::from_str::<Map<String, Value>>(r#"{"a":1}"#).unwrap()
    );
    // An array or a scalar where an object was expected: the message names the
    // column, because `vars` and `reset_steps` look alike in a config table.
    for (text, why) in [
        ("[1,2]", "must be a JSON object"),
        ("{oops", "not valid JSON"),
    ] {
        let e = format!("{:#}", json_object("vars", Some(text)).err().unwrap());
        assert!(e.contains("`vars`") && e.contains(why), "{text}: {e}");
    }
}

fn rule() -> RawRule {
    RawRule {
        name: "orders".into(),
        enabled: true,
        target: Some("search".into()),
        template: Some("POST / {{row.id}}".into()),
        ..Default::default()
    }
}

#[test]
fn a_rule_that_cannot_fire_is_refused_at_load() {
    // `fire_per` decides whether one key is one action or many keys are one; a
    // typo would fire nothing (or one action per key) without a word about it.
    let e = format!(
        "{:#}",
        RuleDef::from_row(&RawRule {
            fire_per: Some("each".into()),
            ..rule()
        })
        .err()
        .unwrap()
    );
    assert!(e.contains("`fire_per`") && e.contains("each"), "{e}");
    assert_eq!(
        RuleDef::from_row(&RawRule {
            fire_per: Some(" KEY ".into()),
            ..rule()
        })
        .unwrap()
        .fire_per,
        FirePer::Key
    );
    assert_eq!(RuleDef::from_row(&rule()).unwrap().fire_per, FirePer::Batch);

    for template in [Some("".into()), Some("   ".into())] {
        let e = format!(
            "{:#}",
            RuleDef::from_row(&RawRule {
                template: template.clone(),
                ..rule()
            })
            .err()
            .unwrap()
        );
        assert!(e.contains("`template` is empty"), "{e}");
    }
    // A blank `target` is refused; the name is only looked up later, so a name
    // that exists nowhere is a configuration error the rule reports, not a load
    // refusal.
    for target in [None, Some("".into())] {
        let e = format!(
            "{:#}",
            RuleDef::from_row(&RawRule {
                target: target.clone(),
                ..rule()
            })
            .err()
            .unwrap()
        );
        assert!(e.contains("`target` is empty"), "{e}");
    }

    // A `for_each` step walks the keys of `snapshot_keys_sql`; without it the
    // step would list nothing and the reset would silently finish.
    let steps = r#"[{"for_each": "snapshot"}]"#;
    let e = format!(
        "{:#}",
        RuleDef::from_row(&RawRule {
            reset_steps: Some(steps.into()),
            ..rule()
        })
        .err()
        .unwrap()
    );
    assert!(
        e.contains("for_each") && e.contains("snapshot_keys_sql"),
        "{e}"
    );
    let ok = RuleDef::from_row(&RawRule {
        reset_steps: Some(steps.into()),
        snapshot_keys_sql: Some("SELECT id FROM orders".into()),
        ..rule()
    })
    .unwrap();
    assert_eq!(ok.reset_steps[0].kind, StepKind::Snapshot);
    // `once` needs nothing of the kind.
    assert!(RuleDef::from_row(&RawRule {
        reset_steps: Some(r#"[{"once": true, "template": "FLUSHDB"}]"#.into()),
        ..rule()
    })
    .is_ok());

    // Zero is not a limit: it would cut every batch to nothing and nothing
    // would ever be sent.
    let r = RuleDef::from_row(&RawRule {
        batch_max_keys: Some(0),
        batch_max_bytes: Some(0),
        max_delay_ms: Some(0),
        ..rule()
    })
    .unwrap();
    assert_eq!((r.batch_max_keys, r.batch_max_bytes), (None, None));
    // …while a zero `max_delay` becomes 1 ms, so a flush is never deferred forever.
    assert_eq!(r.max_delay, Duration::from_millis(1));
    let r = RuleDef::from_row(&rule()).unwrap();
    assert_eq!(
        (r.debounce, r.max_delay),
        (Duration::from_millis(1000), Duration::from_millis(5000))
    );
    // A broken `vars` column is refused by name.
    let e = format!(
        "{:#}",
        RuleDef::from_row(&RawRule {
            vars: Some("nope".into()),
            ..rule()
        })
        .err()
        .unwrap()
    );
    assert!(e.contains("`vars`"), "{e}");
}

#[test]
fn a_reset_step_says_once_or_for_each() {
    // `kind` wins over `for_each`: a row written by a tool that sets both is read
    // the way it says, not by whichever column happens to come first.
    let s = parse_reset_steps(Some(
        r#"[{"kind": "once", "for_each": "stray", "template": "x"}]"#,
    ))
    .unwrap();
    assert_eq!(s[0].kind, StepKind::Once);
    assert_eq!(s[0].name, "step 1");
    // A `once` written as a number or a string is still "once"; only a false or
    // null `once` means "not a once step".
    for truthy in ["1", "\"yes\""] {
        let json = format!(r#"[{{"once": {truthy}, "template": "x"}}]"#);
        let s = parse_reset_steps(Some(&json)).unwrap();
        assert_eq!(s[0].kind, StepKind::Once, "{truthy}");
    }
    for falsy in ["false", "null"] {
        let json = format!(r#"[{{"once": {falsy}, "template": "x"}}]"#);
        assert!(
            parse_reset_steps(Some(&json)).is_err(),
            "{falsy} must not read as a step"
        );
    }
    // A step of a kind that does not exist: refused, not run as something else.
    let e = format!(
        "{:#}",
        parse_reset_steps(Some(r#"[{"kind": "purge", "template": "x"}]"#))
            .err()
            .unwrap()
    );
    assert!(e.contains("unknown kind `purge`"), "{e}");
    // A stray step without `next` has no cursor: one page, then it is done.
    let s = parse_reset_steps(Some(
            r#"[{"for_each": "stray", "list_template": "GET /", "keys": "JSON_EXTRACT(body, '$.ids')"}]"#,
        ))
        .unwrap();
    assert!(
        matches!(&s[0].kind, StepKind::Stray { next: None, .. }),
        "{:?}",
        s[0].kind
    );
}

#[test]
fn a_select_that_is_not_sql_is_refused() {
    let e = format!("{:#}", parse_selects(Some(r#"[42]"#)).err().unwrap());
    assert!(
        e.contains("selects[0]") && e.contains("must be a string"),
        "{e}"
    );
    let e = format!(
        "{:#}",
        parse_selects(Some(r#"[{"name": "a"}]"#)).err().unwrap()
    );
    assert!(e.contains("selects[0]") && e.contains("`sql`"), "{e}");
}

#[test]
fn a_trigger_needs_a_table_and_a_way_to_name_the_key() {
    let t = |id: u64, table: &str, key: Option<&str>, keys_sql: Option<&str>| RawTrigger {
        id,
        rule: "orders".into(),
        enabled: true,
        source_table: table.into(),
        key: key.map(str::to_string),
        keys_sql: keys_sql.map(str::to_string),
        ..Default::default()
    };
    // Quoted and schema-qualified, or bare with the connection's schema.
    let d = TriggerDef::from_row(
        &t(1, "`shop`.`orders`", Some("{{new.id}}"), None),
        "app",
        false,
    )
    .unwrap();
    assert_eq!((d.schema.as_str(), d.table.as_str()), ("shop", "orders"));
    let d = TriggerDef::from_row(&t(1, "Orders", Some("{{new.id}}"), None), "app", false).unwrap();
    assert_eq!((d.schema.as_str(), d.table.as_str()), ("app", "Orders"));
    // On a case-insensitive server the names are lowered, so the binlog rows
    // (which carry the server's own spelling) find the trigger.
    let d =
        TriggerDef::from_row(&t(1, "Shop.Orders", Some("{{new.id}}"), None), "app", true).unwrap();
    assert_eq!((d.schema.as_str(), d.table.as_str()), ("shop", "orders"));

    let e = format!(
        "{:#}",
        TriggerDef::from_row(&t(7, "   ", Some("{{new.id}}"), None), "app", false)
            .err()
            .unwrap()
    );
    assert!(e.contains("`source_table` is empty"), "{e}");
    // A trigger with no key template and no keys query would mark nothing: the
    // row changes and no key is ever fired.
    let e = format!(
        "{:#}",
        TriggerDef::from_row(&t(7, "orders", None, None), "app", false)
            .err()
            .unwrap()
    );
    assert!(
        e.contains("trigger 7") && e.contains("`key` or `keys_sql`"),
        "{e}"
    );
    // Blank counts as absent.
    assert!(TriggerDef::from_row(&t(7, "orders", Some("  "), Some(" ")), "app", false).is_err());
    // Placeholders of `keys_sql` may only read the row of this event.
    let e = format!(
        "{:#}",
        TriggerDef::from_row(
            &t(9, "orders", None, Some("SELECT :other.id")),
            "app",
            false
        )
        .err()
        .unwrap()
    );
    assert!(e.contains(":other.id"), "{e}");
    assert!(TriggerDef::from_row(
        &t(9, "orders", None, Some("SELECT :new.id, :old.id")),
        "app",
        false
    )
    .is_ok());
}

#[test]
fn a_trigger_reads_the_events_it_is_told_to() {
    let row = |on: Option<&str>| RawTrigger {
        id: 1,
        rule: "orders".into(),
        enabled: true,
        source_table: "orders".into(),
        on: on.map(str::to_string),
        key: Some("{{new.id}}".into()),
        ..Default::default()
    };
    let on = |s: Option<&str>| TriggerDef::from_row(&row(s), "app", false).unwrap().on;
    let every = On {
        insert: true,
        update: true,
        delete: true,
    };
    // No `on`: every event of the table fires, which is what a trigger without
    // an opinion means.
    assert_eq!(on(None), every);
    assert_eq!(on(Some("")), every);
    assert_eq!(
        on(Some("insert")),
        On {
            insert: true,
            ..Default::default()
        }
    );
    assert_eq!(
        on(Some(" DELETE , update ")),
        On {
            update: true,
            delete: true,
            ..Default::default()
        }
    );
    for all in ["*", "all", "INSERT,update,delete"] {
        assert_eq!(on(Some(all)), every, "{all}");
    }
    // An event that does not exist is refused: reading it as "nothing" would
    // stop the feed of this table without a word.
    let e = format!(
        "{:#}",
        TriggerDef::from_row(&row(Some("replace")), "app", false)
            .err()
            .unwrap()
    );
    assert!(e.contains("unknown event `replace`"), "{e}");
}
