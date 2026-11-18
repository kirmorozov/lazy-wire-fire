use super::*;
use serde_json::json;

fn names(s: &[&str]) -> Vec<String> {
    s.iter().map(|x| x.to_string()).collect()
}

fn keys(s: &[&str]) -> Vec<String> {
    names(s)
}

#[test]
fn plan_errors() {
    assert!(Plan::new(&names(&["sku"])).is_err());
    assert!(Plan::new(&names(&["_key", "_path"])).is_err());
    assert!(Plan::new(&names(&["_key", "a", "a.b"])).is_err());
    assert!(Plan::new(&names(&["_key", "a[]", "a[].x"])).is_err());
    assert!(Plan::new(&names(&["_key", "a[].b[]"])).is_err());
    assert!(Plan::new(&names(&["_key", "[]"])).is_err());
    Plan::new(&names(&[
        "_key",
        "items[].sku",
        "items[].qty",
        "price.final",
        "price.base",
    ]))
    .unwrap();
}

#[test]
fn owners() {
    let a = Plan::new(&names(&["_key", "price.final"])).unwrap();
    let b = Plan::new(&names(&["_key", "price"])).unwrap();
    let c = Plan::new(&names(&["_key", "category_ids[]", "_path", "_value"])).unwrap();
    assert!(check_owners(&[("a".into(), a.clone()), ("b".into(), b)]).is_err());
    check_owners(&[("a".into(), a), ("c".into(), c)]).unwrap();
}

#[test]
fn merge() {
    let ks = keys(&["1", "2", "3"]);
    let mut m = Merger::new(&ks);
    let attrs = Plan::new(&names(&[
        "_key",
        "sku",
        "price.final",
        "array[1].value",
        "meta:json",
    ]))
    .unwrap();
    m.add(
        &attrs,
        vec![
            vec![
                json!(1),
                json!("A"),
                json!(9.5),
                json!("x"),
                json!("{\"a\":1}"),
            ],
            vec![json!(2), json!("B"), Value::Null, Value::Null, Value::Null],
            vec![
                json!(99),
                json!("ignored"),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ],
    );
    let cats = Plan::new(&names(&[
        "_key",
        "category_ids[]",
        "items[].sku",
        "items[].qty",
    ]))
    .unwrap();
    m.add(
        &cats,
        vec![
            vec![json!("1"), json!(10), json!("c1"), json!(2)],
            vec![json!("1"), json!(11), Value::Null, Value::Null],
            vec![json!("2"), Value::Null, Value::Null, Value::Null],
        ],
    );
    let dynp = Plan::new(&names(&["_key", "_path", "_value"])).unwrap();
    m.add(
        &dynp,
        vec![
            vec![json!(1), json!("position_category_10"), json!(3)],
            vec![json!(1), json!("tags[]"), json!("t")],
        ],
    );
    let docs = m.finish();
    assert_eq!(
        docs[0].doc,
        json!({"sku": "A", "price": {"final": 9.5}, "array": [null, {"value": "x"}], "meta": {"a": 1},
                   "category_ids": [10, 11], "items": [{"sku": "c1", "qty": 2}], "position_category_10": 3, "tags": ["t"]})
    );
    assert_eq!(
        docs[1].doc,
        json!({"sku": "B", "price": {"final": null}, "array": [null, {"value": null}], "meta": null, "category_ids": [], "items": []})
    );
    assert_eq!(docs[2].doc, json!({}));
    assert!(docs.iter().all(|d| !d.delete));
}

#[test]
fn ops() {
    let ks = keys(&["1", "2", "3"]);
    let mut m = Merger::new(&ks);
    let presence = Plan::new(&names(&["_key", "_op"])).unwrap();
    m.add(
        &presence,
        vec![vec![json!(1), Value::Null], vec![json!(2), json!("DELETE")]],
    );
    let d = m.finish();
    assert_eq!(
        d.iter().map(|d| d.delete).collect::<Vec<_>>(),
        vec![false, true, true]
    );
    assert_eq!(
        d[1].to_json(),
        json!({"key": "2", "doc": {}, "op": "delete"})
    );
}

#[test]
fn values() {
    assert_eq!(to_json(My::Int(-3), None), json!(-3));
    assert_eq!(to_json(My::Bytes(b"x".to_vec()), None), json!("x"));
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 0), None),
        json!("2024-01-02 03:04:05")
    );
    assert_eq!(
        to_json(My::Time(true, 1, 2, 3, 4, 500_000), None),
        json!("-26:03:04.500000")
    );
    assert_eq!(key_text(&My::UInt(7)).as_deref(), Some("7"));
    assert_eq!(key_text(&My::NULL), None);
}

fn col(ty: ColumnType, decimals: u8) -> Option<Column> {
    Some(Column::new(ty).with_decimals(decimals))
}

/// The shape a target is told to expect for each column type: `Int` for every integer and
/// `BIT`, `Double` for the floating and decimal ones, `Blob` for the binary ones (a protobuf
/// `bytes` field would read lossy text as base64, so it has to be told apart from text),
/// `Unknown` for `JSON` and for a value with no column behind it.
#[test]
fn value_kinds_by_column_type() {
    use ValueKind::*;
    for ty in [
        ColumnType::MYSQL_TYPE_TINY,
        ColumnType::MYSQL_TYPE_SHORT,
        ColumnType::MYSQL_TYPE_LONG,
        ColumnType::MYSQL_TYPE_LONGLONG,
        ColumnType::MYSQL_TYPE_INT24,
        ColumnType::MYSQL_TYPE_YEAR,
        ColumnType::MYSQL_TYPE_BIT,
    ] {
        assert_eq!(ValueKind::of_column(Some(ty), false), Int, "{ty:?}");
        // The binary flag never makes an integer a blob.
        assert_eq!(ValueKind::of_column(Some(ty), true), Int, "{ty:?} binary");
    }
    for ty in [
        ColumnType::MYSQL_TYPE_FLOAT,
        ColumnType::MYSQL_TYPE_DOUBLE,
        ColumnType::MYSQL_TYPE_DECIMAL,
        ColumnType::MYSQL_TYPE_NEWDECIMAL,
    ] {
        assert_eq!(ValueKind::of_column(Some(ty), false), Double, "{ty:?}");
    }
    // A JSON column is parsed, so its shape is whatever the document inside it is.
    assert_eq!(
        ValueKind::of_column(Some(ColumnType::MYSQL_TYPE_JSON), false),
        Unknown
    );
    // The four blob types, whichever way the binary flag goes.
    for ty in [
        ColumnType::MYSQL_TYPE_TINY_BLOB,
        ColumnType::MYSQL_TYPE_BLOB,
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
        ColumnType::MYSQL_TYPE_LONG_BLOB,
    ] {
        assert_eq!(ValueKind::of_column(Some(ty), false), Blob, "{ty:?}");
        assert_eq!(ValueKind::of_column(Some(ty), true), Blob, "{ty:?} binary");
    }
    // `CHAR`/`VARCHAR` share a type with `BINARY`/`VARBINARY`: only the flag tells them apart.
    for ty in [
        ColumnType::MYSQL_TYPE_STRING,
        ColumnType::MYSQL_TYPE_VAR_STRING,
    ] {
        assert_eq!(ValueKind::of_column(Some(ty), false), Str, "{ty:?}");
        assert_eq!(ValueKind::of_column(Some(ty), true), Blob, "{ty:?} binary");
    }
    // Everything else, dates and times and `SET` and types not named here, is text.
    for ty in [
        ColumnType::MYSQL_TYPE_DATE,
        ColumnType::MYSQL_TYPE_NEWDATE,
        ColumnType::MYSQL_TYPE_TIME,
        ColumnType::MYSQL_TYPE_DATETIME,
        ColumnType::MYSQL_TYPE_TIMESTAMP,
        ColumnType::MYSQL_TYPE_VARCHAR,
        ColumnType::MYSQL_TYPE_ENUM,
        ColumnType::MYSQL_TYPE_SET,
        ColumnType::MYSQL_TYPE_GEOMETRY,
    ] {
        assert_eq!(ValueKind::of_column(Some(ty), false), Str, "{ty:?}");
    }
    assert_eq!(ValueKind::of_column(None, false), Unknown);
    assert_eq!(ValueKind::of_column(None, true), Unknown);

    // `Field::new` takes the kind from the same decision and keys the document by the column.
    let f = Field::new("photo", Some(ColumnType::MYSQL_TYPE_BLOB), false);
    assert_eq!(
        f,
        Field {
            column: "photo".into(),
            key: "photo".into(),
            kind: Blob,
        }
    );
    assert_eq!(Field::new("sku", None, false).kind, Unknown);
}

/// The number, text and date arms of the query conversion, each with the column type that
/// decides it. `DECIMAL` keeps the digits the server sent.
#[test]
fn to_json_follows_the_column_type() {
    assert_eq!(to_json(My::NULL, None), Value::Null);
    assert_eq!(to_json(My::Int(i64::MIN), None), json!(i64::MIN));
    assert_eq!(to_json(My::UInt(u64::MAX), None), json!(u64::MAX));
    assert_eq!(to_json(My::Float(1.5), None), json!(1.5));
    assert_eq!(to_json(My::Double(-0.25), None), json!(-0.25));
    // A number that is not a JSON number at all becomes null, not the string "NaN".
    assert_eq!(to_json(My::Float(f32::NAN), None), Value::Null);
    assert_eq!(to_json(My::Float(f32::INFINITY), None), Value::Null);
    assert_eq!(to_json(My::Double(f64::NEG_INFINITY), None), Value::Null);

    // `DECIMAL`: the text the server sent, as a number when it is one, and with its own
    // trailing zero — a decimal must not round on its way to the target.
    let dec = col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 2);
    assert_eq!(
        to_json(My::Bytes(b"12.50".to_vec()), dec.as_ref()).to_string(),
        "12.50"
    );
    assert_eq!(
        to_json(My::Bytes(b"-0.005".to_vec()), dec.as_ref()).to_string(),
        "-0.005"
    );
    assert_eq!(
        to_json(My::Bytes(b"n/a".to_vec()), dec.as_ref()),
        json!("n/a")
    );
    // The integer and float families, when the value arrives as text (the text protocol).
    for ty in [
        ColumnType::MYSQL_TYPE_TINY,
        ColumnType::MYSQL_TYPE_SHORT,
        ColumnType::MYSQL_TYPE_LONG,
        ColumnType::MYSQL_TYPE_LONGLONG,
        ColumnType::MYSQL_TYPE_INT24,
        ColumnType::MYSQL_TYPE_YEAR,
        ColumnType::MYSQL_TYPE_FLOAT,
        ColumnType::MYSQL_TYPE_DOUBLE,
    ] {
        let c = col(ty, 0);
        assert_eq!(
            to_json(My::Bytes(b"42".to_vec()), c.as_ref()),
            json!(42),
            "{ty:?}"
        );
        assert_eq!(
            to_json(My::Bytes(b"-1.25".to_vec()), c.as_ref()),
            json!(-1.25),
            "{ty:?}"
        );
        // Text that is not a number stays text.
        assert_eq!(
            to_json(My::Bytes(b"42abc".to_vec()), c.as_ref()),
            json!("42abc"),
            "{ty:?}"
        );
    }
    // `JSON` columns are parsed; text that is not JSON is kept as text.
    let js = col(ColumnType::MYSQL_TYPE_JSON, 0);
    assert_eq!(
        to_json(My::Bytes(br#"{"a": [1, 2]}"#.to_vec()), js.as_ref()),
        json!({"a": [1, 2]})
    );
    assert_eq!(
        to_json(My::Bytes(b"{oops".to_vec()), js.as_ref()),
        json!("{oops")
    );
    // `BIT` is the bytes of the value, big-endian, eight at most.
    let bit = col(ColumnType::MYSQL_TYPE_BIT, 0);
    assert_eq!(
        to_json(My::Bytes(vec![0x01, 0x02]), bit.as_ref()),
        json!(258u64)
    );
    assert_eq!(to_json(My::Bytes(vec![]), bit.as_ref()), json!(0u64));
    assert_eq!(
        to_json(My::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]), bit.as_ref()),
        json!(0x0102_0304_0506_0708u64)
    );
    // A binary column with bytes that are not UTF-8 is lossy text, and a column of no
    // particular type is text too.
    let blob = col(ColumnType::MYSQL_TYPE_BLOB, 0);
    assert_eq!(
        to_json(My::Bytes(vec![0xff, b'A']), blob.as_ref()),
        json!("\u{FFFD}A")
    );
    assert_eq!(
        to_json(My::Bytes(vec![0xff, b'A']), None),
        json!("\u{FFFD}A")
    );

    // Dates: a `DATE` column has no time, a `DATETIME` one carries the fraction the column
    // declares (all six digits when the column says nothing, none when there is nothing).
    let d = col(ColumnType::MYSQL_TYPE_DATE, 0);
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 123_456), d.as_ref()),
        json!("2024-01-02")
    );
    let nd = col(ColumnType::MYSQL_TYPE_NEWDATE, 0);
    assert_eq!(
        to_json(My::Date(2024, 12, 1, 0, 0, 0, 0), nd.as_ref()),
        json!("2024-12-01")
    );
    let dt0 = col(ColumnType::MYSQL_TYPE_DATETIME, 0);
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 0), dt0.as_ref()),
        json!("2024-01-02 03:04:05")
    );
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 123_456), dt0.as_ref()),
        json!("2024-01-02 03:04:05.123456")
    );
    let dt3 = col(ColumnType::MYSQL_TYPE_DATETIME, 3);
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 123_456), dt3.as_ref()),
        json!("2024-01-02 03:04:05.123")
    );
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 700_000), dt3.as_ref()),
        json!("2024-01-02 03:04:05.700")
    );
    // More decimals than a microsecond can hold reads as six digits, not a panic.
    let dt9 = col(ColumnType::MYSQL_TYPE_DATETIME, 9);
    assert_eq!(
        to_json(My::Date(2024, 1, 2, 3, 4, 5, 123_456), dt9.as_ref()),
        json!("2024-01-02 03:04:05.123456")
    );
    // `TIME`: the days are folded into the hours, the sign and the fraction as declared.
    let t1 = col(ColumnType::MYSQL_TYPE_TIME, 1);
    assert_eq!(
        to_json(My::Time(false, 2, 3, 4, 5, 500_000), t1.as_ref()),
        json!("51:04:05.5")
    );
    let t0 = col(ColumnType::MYSQL_TYPE_TIME, 0);
    assert_eq!(
        to_json(My::Time(true, 0, 0, 0, 0, 0), t0.as_ref()),
        json!("-00:00:00")
    );

    // A key is text: integers in decimal, bytes as UTF-8, anything else as the document form.
    assert_eq!(key_text(&My::Int(-3)).as_deref(), Some("-3"));
    assert_eq!(
        key_text(&My::Bytes(b"sku-1".to_vec())).as_deref(),
        Some("sku-1")
    );
    assert_eq!(
        key_text(&My::Bytes(vec![0xff, b'A'])).as_deref(),
        Some("\u{FFFD}A")
    );
    assert_eq!(
        key_text(&My::Date(2024, 1, 2, 3, 4, 5, 0)).as_deref(),
        Some("2024-01-02 03:04:05")
    );
    assert_eq!(key_text(&My::Float(1.5)).as_deref(), Some("1.5"));
}

/// The `_path` columns a rule cannot mean: a path with `[]` in the middle, a blank one, an
/// unparsable one. Each is a warning the operator sees, and the row still contributes nothing.
#[test]
fn dynamic_paths_warn_and_move_on() {
    let ks = keys(&["1", "2", "3", "4", "5"]);
    let mut m = Merger::new(&ks);
    let dynp = Plan::new(&names(&["_key", "_path", "_value"])).unwrap();
    m.add(
        &dynp,
        vec![
            vec![json!("1"), json!("tags[]"), json!("t")],
            vec![json!("2"), json!("a[].b"), json!(1)],
            vec![json!("3"), json!(" "), json!(1)],
            vec![json!("4"), json!("a["), json!(1)],
            vec![json!("1"), json!("x.y[]"), Value::Null],
            vec![json!("5"), Value::Null, json!(1)],
        ],
    );
    assert_eq!(
        m.warnings,
        vec![
            "_path `a[].b`: `[]` only at the end",
            "empty _path",
            "_path `a[`: path `a[`: `[` without `]`",
        ]
    );
    let docs = m.finish();
    let by_key: HashMap<&str, Value> = docs
        .iter()
        .map(|d| (d.key.as_str(), d.doc.clone()))
        .collect();
    assert_eq!(by_key["1"], json!({"tags": ["t"], "x": {"y": []}}));
    assert_eq!(by_key["2"], json!({}), "a refused path sets nothing");
    assert_eq!(by_key["3"], json!({}));
    assert_eq!(by_key["4"], json!({}));
    assert_eq!(by_key["5"], json!({}));
}
