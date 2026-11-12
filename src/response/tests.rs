use super::*;

#[test]
fn columns_named() {
    let c = Condition::new("status IN (200, 204) AND JSON_CONTAINS(r.body, 'false', '$.errors')")
        .unwrap();
    assert_eq!(
        c.uses,
        Uses {
            status: true,
            body: true,
            ..Uses::default()
        }
    );
    assert_eq!(
            c.sql,
            "SELECT 1 FROM (SELECT CAST(? AS SIGNED) AS status, NULL AS headers, \
             CAST(? AS CHAR(16777215) CHARACTER SET utf8mb4) AS body, NULL AS text, NULL AS affected_rows) AS r \
             WHERE (status IN (200, 204) AND JSON_CONTAINS(r.body, 'false', '$.errors')\n)"
        );
    // A name inside a string literal is not a column.
    assert_eq!(
        Condition::new("'body' = 'x'").unwrap().uses,
        Uses::default()
    );
    let v = Values::new(&["JSON_EXTRACT(body, '$[1]')", "JSON_EXTRACT(body, '$[0]')"]).unwrap();
    assert!(v.uses.body && !v.uses.text);
    assert!(v.sql.starts_with(
        "SELECT (JSON_EXTRACT(body, '$[1]')\n) AS v0, (JSON_EXTRACT(body, '$[0]')\n) AS v1 FROM"
    ));
}

#[test]
fn caching_only_for_conditions_on_r() {
    let cached = |s: &str| Condition::new(s).unwrap().cache.is_some();
    assert!(cached(
        "JSON_UNQUOTE(JSON_EXTRACT(body, '$.result')) = 'created'"
    ));
    assert!(cached(
        "text = 'OK' OR (status BETWEEN 200 AND 299 AND LOWER(text) LIKE '%ok%')"
    ));
    assert!(!cached("status = 200 AND (SELECT COUNT(*) FROM flags) = 0"));
    assert!(!cached("status = 200 AND NOW() > '2026-01-01'"));
    assert!(!cached("status = @expected"));
    assert!(!cached("mydb.ok(status)"));
}

#[test]
fn one_expression_only() {
    for (src, why) in [
        ("JSON_EXTRACT(body, '$.errors') == false", "`==`"),
        ("status = 200, status = 204", "AND / OR"),
        ("1; DELETE FROM t", "`;`"),
    ] {
        let e = format!("{:#}", Condition::new(src).err().unwrap());
        assert!(e.contains(why) && e.contains("response row"), "{src}: {e}");
    }
}

#[test]
fn rows_from_answers() {
    let all = Uses {
        status: true,
        headers: true,
        body: true,
        text: true,
        affected_rows: true,
    };
    let h = vec![
        ("Content-Type".to_string(), "application/json".to_string()),
        ("Set-Cookie".to_string(), "a=1".to_string()),
        ("set-cookie".to_string(), "b=2".to_string()),
    ];
    let r = Row::http(all, 201, &h, br#"{"ok":true}"#);
    assert_eq!(r.status, Some(201));
    assert_eq!(
        r.headers.as_deref(),
        Some(r#"{"content-type":"application/json","set-cookie":"a=1, b=2"}"#)
    );
    assert_eq!(r.body.as_deref(), Some(r#"{"ok":true}"#));
    let html = Row::http(all, 200, &[], b"<html>");
    assert_eq!((html.body, html.text.as_deref()), (None, Some("<html>")));
    // Only the named columns are filled.
    let only = Row::http(
        Uses {
            status: true,
            ..Uses::default()
        },
        200,
        &h,
        b"x",
    );
    assert_eq!(
        only,
        Row {
            status: Some(200),
            ..Row::default()
        }
    );

    let scan = Reply::Array(Some(vec![
        Reply::Bulk(Some(b"0".to_vec())),
        Reply::Array(Some(vec![Reply::Bulk(Some(b"k1".to_vec()))])),
    ]));
    let r = Row::reply(all, &scan);
    assert_eq!((r.body.as_deref(), r.text), (Some(r#"["0",["k1"]]"#), None));
    let r = Row::reply(all, &Reply::Simple("OK".into()));
    assert_eq!(
        (r.body.as_deref(), r.text.as_deref()),
        (Some(r#""OK""#), Some("OK"))
    );
    assert_eq!(Row::reply(all, &Reply::Int(3)).text.as_deref(), Some("3"));
    assert_eq!(Row::affected(all, 2).affected_rows, Some(2));
}

#[test]
fn a_bulk_reply_is_text_only_when_the_condition_reads_text() {
    let all = Uses {
        body: true,
        text: true,
        ..Uses::default()
    };
    // A bulk reply is the value a `GET` answers with: the text is the value
    // itself, so a condition on `text` must see it and one on `body` the JSON.
    let r = Row::reply(all, &Reply::Bulk(Some(b"order-7".to_vec())));
    assert_eq!(
        (r.body.as_deref(), r.text.as_deref()),
        (Some(r#""order-7""#), Some("order-7"))
    );
    // Only the named columns travel: a reply the condition cannot name is not sent.
    let only_body = Row::reply(
        Uses {
            body: true,
            ..Uses::default()
        },
        &Reply::Bulk(Some(b"x".to_vec())),
    );
    assert_eq!(
        (only_body.body.as_deref(), only_body.text),
        (Some(r#""x""#), None)
    );
    // A nil reply (a key that is gone) has no text: a condition on `text` gets NULL.
    let r = Row::reply(all, &Reply::Bulk(None));
    assert_eq!((r.body.as_deref(), r.text), (Some("null"), None));
    // An error reply is text for `body`, never for `text`: the caller checks
    // `Reply::error` before the condition runs.
    let r = Row::reply(all, &Reply::Error("WRONGTYPE x".into()));
    assert_eq!(
        (r.body.as_deref(), r.text),
        (Some(r#""WRONGTYPE x""#), None)
    );
}

#[test]
fn a_produce_and_a_json_answer_become_the_body_column() {
    let all = Uses {
        body: true,
        status: true,
        ..Uses::default()
    };
    // The offset of the record is what a `expect` checks the produce against;
    // without it a key whose record is not in the partition would be lost.
    let r = Row::kafka(all, "orders", 3, 42);
    assert_eq!(
        r.body.as_deref(),
        Some(r#"{"topic":"orders","partition":3,"offset":42}"#)
    );
    assert_eq!(r.status, None);
    // Nothing named: nothing is built, and the exchange answers with no columns.
    assert_eq!(Row::kafka(Uses::default(), "orders", 3, 42), Row::default());

    let rows = serde_json::json!([{"_key": "1"}, {"_key": "2"}]);
    assert_eq!(
        Row::json(all, &rows).body.as_deref(),
        Some(rows.to_string().as_str())
    );
    assert_eq!(Row::json(Uses::default(), &rows), Row::default());
}

#[test]
fn an_expectation_that_names_nothing_needs_no_parameters() {
    // The derived table gets one `?` per named column; with none named the
    // query must be sent with no parameters at all, or the server rejects it.
    let empty = Row::default();
    assert_eq!(empty.params(Uses::default()), Params::Empty);
    let all = Uses {
        status: true,
        headers: true,
        body: true,
        text: true,
        affected_rows: true,
    };
    let p = empty.params(all);
    match p {
        Params::Positional(v) => assert_eq!(v.len(), 5),
        other => panic!("every named column needs a parameter: {other:?}"),
    }
}

#[tokio::test]
async fn an_expectation_of_nothing_needs_no_columns_and_takes_any_2xx() {
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    for blank in [None, Some(""), Some("   ")] {
        let ex = Expect::new(blank).unwrap();
        assert_eq!(ex.uses(), Uses::default());
        // No condition: the status rule is all there is, and it is decided
        // without a query.
        assert_eq!(ex.http(&pool, 204, &[], b"").await.unwrap(), Ok(()));
        let why = ex.http(&pool, 503, &[], b"").await.unwrap().unwrap_err();
        assert_eq!(why, "status 503");
        assert_eq!(ex.row(&pool, |_| Row::default()).await.unwrap(), Ok(()));
    }
}

#[tokio::test]
async fn a_condition_that_does_not_read_status_keeps_the_2xx_rule() {
    // A target that answers 500 with a body that happens to match must not
    // count as delivered: the status rule stays unless the condition names
    // `status` and so takes responsibility for it.
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    let ex = Expect::new(Some("JSON_EXTRACT(body, '$.ok') = true")).unwrap();
    assert!(ex.uses().body && !ex.uses().status);
    let why = ex
        .http(&pool, 500, &[], br#"{"ok":true}"#)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(why, "status 500");
}

#[test]
fn a_long_condition_is_clipped_in_the_message() {
    let long = format!("status = 200 AND {}", "a".repeat(300));
    let s = short(&long);
    assert_eq!(s.chars().count(), 121); // 120 chars and the ellipsis
    assert!(s.ends_with('…'));
    // Whitespace of a multi-line condition is collapsed, and a short one is
    // quoted as it is.
    assert_eq!(
        short("status = 200\n  AND body IS NULL"),
        "status = 200 AND body IS NULL"
    );
}
