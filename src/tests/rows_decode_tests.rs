//! Decoding a binlog row image into named JSON (`Layout`, `convert`, `col_kind`) and the
//! catalog cache that holds one layout per table — and, keyed by their own names, the layouts
//! derived from full-metadata table maps. Fixtures are `BinlogValue`s, the way the
//! reader hands them over; every assertion is the exact value (or the exact message), never
//! "it did not panic".

use super::*;
use mysql_async::binlog::jsonb::{self, JsonbString, OpaqueValue};
use mysql_async::consts::ColumnType;
use serde_json::json;

/// A row image with no column descriptions: `Layout` names the columns, not the row.
fn row(vals: Vec<Option<BinlogValue<'static>>>) -> BinlogRow {
    BinlogRow::new(vals, Arc::new([]))
}

fn val(v: My) -> Option<BinlogValue<'static>> {
    Some(BinlogValue::Value(v))
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|n| n.to_string()).collect()
}

/// Every `ColKind` in one row: the object a Full-template rule gets for `{{row}}`.
#[test]
fn object_names_every_column_with_its_kind() {
    let layout = Layout::new(
        names(&["id", "n", "state", "tags", "flags", "doc", "note"]),
        vec![
            ColKind::Plain,
            ColKind::Unsigned(8),
            ColKind::Enum(vec!["new".into(), "it's".into()]),
            ColKind::Set(vec!["a".into(), "b".into(), "c".into()]),
            ColKind::Bit,
            ColKind::Plain,
            ColKind::Plain,
        ],
    );
    let r = row(vec![
        val(My::Bytes(b"order-1".to_vec())),
        val(My::Int(-1)),
        val(My::Int(2)),
        val(My::Bytes(vec![0b101])),
        val(My::Bytes(vec![0x01, 0x02])),
        Some(BinlogValue::Jsonb(jsonb::Value::String(JsonbString::new(
            br#"{"a":1}"#.to_vec(),
        )))),
        None,
    ]);
    assert_eq!(
        layout.object(&r),
        json!({
            "id": "order-1",
            "n": 255u64,
            "state": "it's",
            "tags": "a,c",
            "flags": 258u64,
            "doc": "{\"a\":1}",
            "note": null,
        })
    );
    // The same columns read one at a time give the same values.
    assert_eq!(layout.value(&r, 1), json!(255u64));
    assert_eq!(layout.value(&r, 4), json!(258u64));
}

/// A `BIT(n)` column arrives as big-endian bytes; the number is what they spell.
#[test]
fn bit_columns_are_big_endian_and_bounded() {
    let bit = |b: Vec<u8>| convert(val(My::Bytes(b)).as_ref(), &ColKind::Bit);
    assert_eq!(bit(vec![0x00]), json!(0u64));
    assert_eq!(bit(vec![0xff]), json!(255u64));
    assert_eq!(bit(vec![0x01, 0x00]), json!(256u64));
    assert_eq!(
        bit(vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        json!(u64::MAX)
    );
    // `BIT(64)` is the widest column, and the fold reads eight bytes at most: anything past
    // them is not part of the value.
    assert_eq!(
        bit(vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]),
        json!(0x0102_0304_0506_0708u64)
    );
    assert_eq!(bit(vec![]), json!(0u64));
    // A `BIT` column whose value is SQL NULL is null, not zero.
    assert_eq!(convert(None, &ColKind::Bit), Value::Null);
}

/// `JSON` columns come over as JSONB: scalars decode, and a value that cannot be a JSON value
/// (`Opaque`, invalid UTF-8) is null rather than a broken document.
#[test]
fn jsonb_columns_decode_or_are_null() {
    let j = |v: jsonb::Value<'static>| convert(Some(&BinlogValue::Jsonb(v)), &ColKind::Plain);
    assert_eq!(j(jsonb::Value::Null), json!(null));
    assert_eq!(j(jsonb::Value::Bool(true)), json!(true));
    assert_eq!(j(jsonb::Value::I64(-7)), json!(-7));
    assert_eq!(j(jsonb::Value::U64(u64::MAX)), json!(u64::MAX));
    assert_eq!(j(jsonb::Value::I32(-3)), json!(-3));
    assert_eq!(j(jsonb::Value::U16(9)), json!(9));
    assert_eq!(j(jsonb::Value::F64(1.5)), json!(1.5));
    assert_eq!(
        j(jsonb::Value::String(JsonbString::new(
            b"two words".to_vec()
        ))),
        json!("two words")
    );
    // `DECIMAL` (and any other opaque JSONB value) has no JSON representation: null.
    assert_eq!(
        j(jsonb::Value::Opaque(OpaqueValue::new(
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
            b"12.50".to_vec(),
        ))),
        Value::Null
    );
    // Nor has a string that is not UTF-8.
    assert_eq!(
        j(jsonb::Value::String(JsonbString::new(vec![0xff, 0xfe]))),
        Value::Null
    );
    // A partial JSON update carries no full value: null, so the rule re-reads the row.
    assert_eq!(
        convert(Some(&BinlogValue::JsonDiff(Vec::new())), &ColKind::Plain,),
        Value::Null
    );
}

/// The unsigned masks (one per width), the enum labels, and the set mask.
#[test]
fn unsigned_enums_and_sets() {
    let c = |v: My, k: ColKind| convert(val(v).as_ref(), &k);
    for (bits, want) in [
        (8u32, 255u64),
        (16, 65_535),
        (24, 16_777_215),
        (32, 4_294_967_295),
        (64, u64::MAX),
    ] {
        assert_eq!(
            c(My::Int(-1), ColKind::Unsigned(bits)),
            json!(want),
            "{bits} bits"
        );
    }
    // A positive value is its own unsigned reading.
    assert_eq!(c(My::Int(7), ColKind::Unsigned(8)), json!(7u64));
    // The signed path is untouched by the kind.
    assert_eq!(c(My::Int(-1), ColKind::Plain), json!(-1));

    let labels = vec!["new".to_string(), "it''s".to_string(), "gone".to_string()];
    assert_eq!(c(My::Int(0), ColKind::Enum(labels.clone())), json!(""));
    assert_eq!(c(My::Int(1), ColKind::Enum(labels.clone())), json!("new"));
    assert_eq!(c(My::Int(3), ColKind::Enum(labels.clone())), json!("gone"));
    // Past the last label there is no name: the raw index is the honest answer.
    assert_eq!(c(My::Int(4), ColKind::Enum(labels.clone())), json!(4));
    // A negative index is not a label either.
    assert_eq!(c(My::Int(-1), ColKind::Enum(labels)), json!(-1));

    let set = ColKind::Set(vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
    ]);
    assert_eq!(c(My::Bytes(vec![0b0000]), set.clone()), json!(""));
    assert_eq!(c(My::Bytes(vec![0b0101]), set.clone()), json!("a,c"));
    // `SET` has at most 64 members: bytes past the eighth are not part of the mask.
    assert_eq!(
        c(
            My::Bytes(vec![0b0010, 0, 0, 0, 0, 0, 0, 0, 0b11]),
            set.clone()
        ),
        json!("b")
    );
    // A `SET` value that is not bytes (it never should be) falls through to the plain path.
    assert_eq!(c(My::Int(3), set), json!(3));
}

/// Values no kind claims go through the same conversion a query result uses.
#[test]
fn plain_values_use_the_query_conversion() {
    let c = |v: My| convert(val(v).as_ref(), &ColKind::Plain);
    assert_eq!(c(My::Int(7)), json!(7));
    assert_eq!(c(My::UInt(8)), json!(8));
    assert_eq!(c(My::NULL), Value::Null);
    assert_eq!(c(My::Bytes(b"sku-1".to_vec())), json!("sku-1"));
    assert_eq!(
        c(My::Date(2024, 1, 2, 3, 4, 5, 0)),
        json!("2024-01-02 03:04:05")
    );
    // A `NULL` under a kind that would have decoded it is still null.
    assert_eq!(convert(val(My::NULL).as_ref(), &ColKind::Bit), Value::Null);
    assert_eq!(
        convert(val(My::NULL).as_ref(), &ColKind::Enum(vec!["a".into()])),
        Value::Null
    );
}

/// A column the layout has no kind for (a shorter `kinds`) is plain, and an index the row does
/// not have is null rather than a panic.
#[test]
fn a_missing_kind_is_plain_and_a_missing_value_is_null() {
    let layout = Layout::new(names(&["a", "b", "c"]), vec![ColKind::Bit]);
    let r = row(vec![
        val(My::Bytes(vec![0x02])),
        val(My::Bytes(b"hi".to_vec())),
        None,
    ]);
    assert_eq!(layout.value(&r, 0), json!(2u64));
    assert_eq!(
        layout.value(&r, 1),
        json!("hi"),
        "no kind at index 1 → Plain"
    );
    assert_eq!(layout.value(&r, 2), Value::Null);
    assert_eq!(layout.value(&r, 9), Value::Null, "no column at index 9");
    assert_eq!(layout.object(&r), json!({"a": 2u64, "b": "hi", "c": null}));
    // An empty layout renders an empty object and reports itself as empty.
    let empty = Layout::new(vec![], vec![]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.object(&row(vec![])), json!({}));
}

/// Column names are case-insensitive, and an unknown name is `None` rather than a guess.
#[test]
fn index_is_case_insensitive() {
    let layout = Layout::new(
        names(&["Id", "customer_id"]),
        vec![ColKind::Plain, ColKind::Plain],
    );
    assert_eq!(layout.index("Id"), Some(0));
    assert_eq!(layout.index("id"), Some(0));
    assert_eq!(layout.index("ID"), Some(0));
    assert_eq!(layout.index("CUSTOMER_ID"), Some(1));
    assert_eq!(layout.index("nope"), None);
    assert_eq!(layout.index(""), None);
    assert_eq!(layout.len(), 2);
    assert!(!layout.is_empty());
}

/// `col_kind` reads `information_schema`'s `DATA_TYPE` and `COLUMN_TYPE`, both cases.
#[test]
fn col_kind_reads_the_column_descriptions() {
    for (dt, ct, want) in [
        ("tinyint", "tinyint(3) unsigned", ColKind::Unsigned(8)),
        ("smallint", "smallint(6) unsigned", ColKind::Unsigned(16)),
        ("mediumint", "mediumint(9) unsigned", ColKind::Unsigned(24)),
        ("int", "int(11) unsigned", ColKind::Unsigned(32)),
        ("integer", "int(10) unsigned", ColKind::Unsigned(32)),
        ("bigint", "bigint(20) unsigned", ColKind::Unsigned(64)),
        ("bit", "bit(8)", ColKind::Bit),
        ("varchar", "varchar(20)", ColKind::Plain),
        ("int", "int(11)", ColKind::Plain),
        ("bigint", "bigint(20) zerofill", ColKind::Plain),
        ("text", "text", ColKind::Plain),
    ] {
        assert_eq!(col_kind(dt, ct), want, "{dt} / {ct}");
    }
    // The server's own spelling is upper case in some views.
    assert_eq!(col_kind("BIT", "BIT(4)"), ColKind::Bit);
    assert_eq!(col_kind("INT", "INT(10) UNSIGNED"), ColKind::Unsigned(32));
    assert_eq!(
        col_kind("enum", "enum('a','b')"),
        ColKind::Enum(vec!["a".into(), "b".into()])
    );
    assert_eq!(
        col_kind("set", "set('a','b','c')"),
        ColKind::Set(vec!["a".into(), "b".into(), "c".into()])
    );
    // A type name that merely mentions `unsigned` is not an unsigned integer.
    assert_eq!(col_kind("varchar", "varchar(8) unsigned"), ColKind::Plain);
}

/// The labels of `enum('a','it''s','b\qc')`: a doubled quote is one quote, a backslash
/// escapes what follows it, and a comma inside a label does not split it.
#[test]
fn labels_survive_quotes_escapes_and_commas() {
    assert_eq!(
        parse_labels("enum('a','it''s','b\\qc','x,y','')"),
        vec!["a", "it's", "bqc", "x,y", ""]
    );
    assert_eq!(parse_labels("set('a')"), vec!["a"]);
    assert_eq!(parse_labels("int(11)"), Vec::<String>::new());
    // An unterminated label ends at the end of the string, not at the opening quote.
    assert_eq!(parse_labels("enum('ab"), vec!["ab"]);
}

/// What the operator sees when the binlog and `information_schema` disagree: the message names
/// both counts and the fix.
#[test]
fn layout_mismatch_names_the_tables_and_both_counts() {
    let e = LayoutMismatch {
        schema: "shop".into(),
        table: "orders".into(),
        have: 5,
        want: 4,
    };
    assert_eq!(
        e.to_string(),
        "table `shop`.`orders` has 5 columns in the binlog but 4 in information_schema \
         (a DDL changed it; set binlog_row_metadata=FULL to decode across DDL)"
    );
    // It is an error, so `?` on `Catalog::layout` carries it to the engine, which invalidates
    // the layout and resets the rules of that table.
    let any: anyhow::Error = e.into();
    assert!(any.to_string().starts_with("table `shop`.`orders` has 5"));
}

/// The catalog cache: one table out, a whole schema out (the `*` form a `DROP TABLE`/rename
/// path uses), and everything out on a reconnect.
#[test]
fn the_catalog_cache_invalidates_one_table_or_a_schema() {
    let layout =
        |cols: &[&str]| Arc::new(Layout::new(names(cols), vec![ColKind::Plain; cols.len()]));
    let key = |s: &str, t: &str| (s.to_string(), t.to_string());
    let mut cat = Catalog::default();
    cat.cache.insert(key("shop", "orders"), layout(&["id"]));
    cat.cache
        .insert(key("shop", "items"), layout(&["id", "sku"]));
    cat.cache.insert(key("other", "orders"), layout(&["id"]));
    assert_eq!(cat.cache.len(), 3);

    cat.invalidate("shop", "items");
    assert!(cat.cache.contains_key(&key("shop", "orders")));
    assert!(!cat.cache.contains_key(&key("shop", "items")));
    assert!(cat.cache.contains_key(&key("other", "orders")));

    cat.invalidate("shop", "*");
    assert!(
        !cat.cache.contains_key(&key("shop", "orders")),
        "the whole schema went"
    );
    assert!(
        cat.cache.contains_key(&key("other", "orders")),
        "other schemas stay"
    );

    // Invalidating what is not there is a no-op, and `clear` empties the rest.
    cat.invalidate("shop", "nothing_here");
    cat.invalidate("nowhere", "*");
    assert_eq!(cat.cache.len(), 1);
    cat.clear();
    assert!(cat.cache.is_empty());
}

/// The `information_schema` read of `shop`.`orders`: every kind a derived layout has to carry.
fn info_layout() -> Arc<Layout> {
    Arc::new(Layout::new(
        names(&["id", "sku", "state", "note"]),
        vec![
            ColKind::Unsigned(32),
            ColKind::Plain,
            ColKind::Enum(vec!["a".into(), "b".into()]),
            ColKind::Plain,
        ],
    ))
}

fn tkey(s: &str, t: &str) -> (String, String) {
    (s.to_string(), t.to_string())
}

/// The FULL half of the catalog caches per names: a repeat of the same names from the same
/// read is the one `Arc` (that is the whole point — finding 1), a different list is its own,
/// and no derived layout is ever the `information_schema` one or shares an `Arc` with it.
#[test]
fn full_layouts_are_cached_per_names_and_never_share_the_plain_arc() {
    let info = info_layout();
    let mut cat = Catalog::default();
    cat.put("shop", "orders", info.clone());

    // The event's own names, in the case the event wrote them: `Layout::index` is
    // case-insensitive, so the kinds still follow the same-named column.
    let event = names(&["id", "SKU", "STATE", "note"]);
    let ckey = tkey("shop", "orders");
    let l1 = cat.full_layout(&ckey, &event, &info);
    let l2 = cat.full_layout(&ckey, &event, &info);
    assert!(
        Arc::ptr_eq(&l1, &l2),
        "the same names from the same read answer with the same `Arc`"
    );
    // The finding itself, counted: a repeat of the event derives nothing. What it replaced —
    // deriving the layout from the names on every event, which is exactly what the cache
    // used to do — allocates, and now only ever runs once per (table, names, read).
    let (again, cached) = crate::alloc_measure::count(|| cat.full_layout(&ckey, &event, &info));
    assert!(Arc::ptr_eq(&again, &l1));
    assert_eq!(
        cached, 0,
        "the repeat of a FULL event costs one `Arc` clone"
    );
    let (_, fresh) = crate::alloc_measure::count(|| layout_from_names(&info, &event));
    assert!(fresh > 0, "what that clone replaced");
    assert!(
        !Arc::ptr_eq(&l1, &info),
        "never the `information_schema` one"
    );
    assert_eq!(l1.names, event, "the names are the event's, not the read's");
    assert_eq!(l1.kinds[0], ColKind::Unsigned(32));
    assert_eq!(l1.kinds[2], ColKind::Enum(vec!["a".into(), "b".into()]));
    assert_eq!(
        l1.kinds[3],
        ColKind::Plain,
        "a column the read does not name would be plain; this one it does"
    );

    // A second, differently-ordered list is its own layout — the two never share an `Arc`,
    // which is what the mutation check (key without the names) must break.
    let other = names(&["note", "state", "sku", "id"]);
    let l3 = cat.full_layout(&tkey("shop", "orders"), &other, &info);
    assert!(
        !Arc::ptr_eq(&l1, &l3),
        "two lists of one table, two layouts"
    );
    assert_eq!(l3.names, other);
    assert_eq!(cat.full[&tkey("shop", "orders")].len(), 2);
}

/// The two sides of the catalog never answer for each other: a plain request gets the
/// `information_schema` layout (never a derived one), a FULL request gets a layout derived
/// from exactly its names (never the plain one, never one derived from other names).
#[test]
fn plain_and_full_requests_never_cross() {
    let info = info_layout();
    let mut cat = Catalog::default();
    cat.put("shop", "orders", info.clone());
    let event = names(&["id", "sku", "state", "note"]);
    let derived = cat.full_layout(&tkey("shop", "orders"), &event, &info);
    let key = tkey("shop", "orders");

    assert!(Arc::ptr_eq(cat.current(&key, None).unwrap(), &info));
    assert!(Arc::ptr_eq(
        cat.current(&key, Some(&event)).unwrap(),
        &derived
    ));

    // A FULL request for names nobody ever asked about: `layout` would derive a fresh one,
    // so nothing in the cache is the current answer — and above all not the plain layout.
    let unknown = names(&["id"]);
    assert!(
        cat.current(&key, Some(&unknown)).is_none(),
        "a FULL request is never answered from the plain read"
    );
    // An unknown table answers `None` on both sides.
    assert!(cat.current(&tkey("shop", "ghosts"), None).is_none());
    assert!(cat.current(&tkey("shop", "ghosts"), Some(&event)).is_none());
}

/// A derived layout is only the answer while the read it was derived from is the current
/// one: any re-read of `information_schema` — the column-count trust path installing its
/// fresh `Arc`, an invalidation, a clear — takes the derived answer with it, and the same
/// `Arc` back makes it the answer again.
#[test]
fn derived_layouts_die_with_the_read_they_came_from() {
    let info = info_layout();
    let mut cat = Catalog::default();
    cat.put("shop", "orders", info.clone());
    let event = names(&["id", "sku", "state", "note"]);
    let derived = cat.full_layout(&tkey("shop", "orders"), &event, &info);
    let key = tkey("shop", "orders");
    assert!(Arc::ptr_eq(
        cat.current(&key, Some(&event)).unwrap(),
        &derived
    ));

    // The trust path of `layout` after a count mismatch: a new `Arc` under the same key.
    let reread = info_layout();
    assert!(!Arc::ptr_eq(&reread, &info));
    cat.put("shop", "orders", reread.clone());
    assert!(
        cat.current(&key, Some(&event)).is_none(),
        "derived from a replaced read: not the current answer"
    );
    assert!(Arc::ptr_eq(cat.current(&key, None).unwrap(), &reread));

    // Put the very same `Arc` back and the derived layout is the answer again — which is
    // exactly what `layout` does with it (the cached read, the cached names: the one `Arc`).
    cat.put("shop", "orders", info.clone());
    assert!(Arc::ptr_eq(
        cat.current(&key, Some(&event)).unwrap(),
        &derived
    ));

    // Invalidations take the derived map with the plain one — per table, per schema.
    cat.put("other", "orders", info.clone());
    cat.full_layout(&tkey("other", "orders"), &event, &info);
    cat.invalidate("shop", "orders");
    assert!(cat.current(&key, None).is_none());
    assert!(cat.current(&key, Some(&event)).is_none());
    assert!(
        cat.current(&tkey("other", "orders"), Some(&event))
            .is_some(),
        "the other schema keeps both halves"
    );
    cat.invalidate("other", "*");
    assert!(cat
        .current(&tkey("other", "orders"), Some(&event))
        .is_none());
    cat.clear();
    assert!(cat.cache.is_empty() && cat.full.is_empty());
}

/// The route probe's pre-check: the generation moves with everything a cached answer was
/// built from — every install, every drop, both maps — and stays put when a request was
/// merely answered from what was already there.
#[test]
fn the_generation_moves_with_everything_a_cached_answer_was_built_from() {
    let mut cat = Catalog::default();
    let g0 = cat.generation();

    let info = info_layout();
    cat.put("shop", "orders", info.clone());
    let g1 = cat.generation();
    assert!(g1 > g0, "a layout installed");

    let event = names(&["id", "sku", "state", "note"]);
    cat.full_layout(&tkey("shop", "orders"), &event, &info);
    assert!(cat.generation() > g1, "a derived layout installed");
    let g2 = cat.generation();

    // Answers, not changes: the cached repeat and every `current` read leave it alone.
    cat.full_layout(&tkey("shop", "orders"), &event, &info);
    cat.current(&tkey("shop", "orders"), None);
    cat.current(&tkey("shop", "orders"), Some(&event));
    assert_eq!(cat.generation(), g2, "nothing was invalidated");

    // Every way a cached answer can go stale raises it again — including the trust path's
    // re-read, which is a `put` too.
    let reread = info_layout();
    cat.put("shop", "orders", reread.clone());
    assert!(cat.generation() > g2, "the trust path's re-read");
    let g3 = cat.generation();
    cat.invalidate("shop", "orders");
    assert!(cat.generation() > g3, "one table out");
    let g4 = cat.generation();
    cat.put("shop", "items", info.clone());
    cat.invalidate("shop", "*");
    assert!(cat.generation() > g4, "a whole schema out");
    let g5 = cat.generation();
    cat.clear();
    assert!(cat.generation() > g5, "everything out");
}
