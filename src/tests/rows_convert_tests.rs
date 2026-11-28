//! The borrowed conversion and the borrowed comparison (the hot-path audit's finding 2) have to
//! answer exactly what the owned ones answered. `serde_json` is built here with
//! `arbitrary_precision` and `preserve_order` (Cargo.toml), which means a `Number` keeps the
//! digits it was given and compares by them: `1.10` ≠ `1.1`, `1` ≠ `1.0`, `-0.0` ≠ `0.0`, and an
//! object keeps the order its keys were inserted in. Whether that is right is not this file's
//! question — its only job is to pin that `changed_columns` and the key bytes keep answering as
//! they did, column kind by column kind and edge value by edge value.

use super::*;
use crate::alloc_measure;
use crate::docs::{to_json, to_json_ref};
use crate::template::text;
use mysql_async::binlog::jsonb::{JsonbString, Value as Jsonb};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::ColumnType;
use mysql_async::{Column, Value as My};
use serde_json::Value;
use std::sync::Arc;

/// `convert` as it was before this PR (the head it branched from), kept here verbatim as the
/// oracle: `Layout::value`'s last arm went through `docs::to_json(v.clone(), None)`, so a `Bytes`
/// column paid for its `Vec` and for the text it loses to. Every test below runs both and compares.
fn owned_value(v: Option<&BinlogValue<'_>>, kind: &ColKind) -> Value {
    let v = match v {
        None => return Value::Null,
        Some(BinlogValue::Value(v)) => v,
        Some(BinlogValue::Jsonb(j)) => {
            return j.clone().try_into().unwrap_or(Value::Null);
        }
        // Partial JSON updates carry no full value.
        Some(BinlogValue::JsonDiff(_)) => return Value::Null,
    };
    match (kind, v) {
        (_, My::NULL) => Value::Null,
        (ColKind::Unsigned(bits), My::Int(i)) => {
            let mask = if *bits >= 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            };
            Value::from((*i as u64) & mask)
        }
        (ColKind::Enum(labels), My::Int(i)) => match usize::try_from(*i) {
            Ok(0) => Value::String(String::new()),
            Ok(i) if i <= labels.len() => Value::String(labels[i - 1].clone()),
            _ => Value::from(*i),
        },
        (ColKind::Set(labels), My::Bytes(b)) => {
            let mut mask = 0u64;
            for (i, byte) in b.iter().enumerate().take(8) {
                mask |= u64::from(*byte) << (8 * i);
            }
            let parts: Vec<&str> = labels
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1u64 << i) != 0)
                .map(|(_, l)| l.as_str())
                .collect();
            Value::String(parts.join(","))
        }
        (ColKind::Bit, My::Bytes(b)) => Value::from(
            b.iter()
                .take(8)
                .fold(0u64, |acc, x| (acc << 8) | u64::from(*x)),
        ),
        (_, other) => to_json(other.clone(), None),
    }
}

/// How `changed_columns` decided before: both images converted, then the two `Value`s compared.
fn owned_changed(layout: &Layout, b: &BinlogRow, a: &BinlogRow, i: usize) -> bool {
    let kind = layout.kinds.get(i).unwrap_or(&ColKind::Plain);
    owned_value(b.as_ref(i), kind) != owned_value(a.as_ref(i), kind)
}

/// Every kind a column can have, including the label lists and the masks.
///
/// The list is not typed out: it is what `covered` answers for one seed of each variant, and
/// `covered` matches `ColKind` with no wildcard — a variant added there does not compile until
/// this file gives it values to test, so the equivalence test cannot go on printing green over a
/// kind it stopped covering. `the_corpus_still_covers_every_kind` is the runtime half: a seed
/// dropped from the list fails by name instead of thinning the corpus quietly.
fn kinds() -> Vec<ColKind> {
    let seeds = [
        ColKind::Plain,
        ColKind::Unsigned(8),
        ColKind::Enum(vec![String::new()]),
        ColKind::Set(vec![String::new()]),
        ColKind::Bit,
    ];
    seeds.iter().flat_map(covered).collect()
}

/// The kinds one seed of a variant contributes. Exhaustive by construction: no wildcard, so a new
/// `ColKind` variant is a compile error here until the corpus covers it.
fn covered(kind: &ColKind) -> Vec<ColKind> {
    match kind {
        ColKind::Plain => vec![ColKind::Plain],
        ColKind::Unsigned(_) => vec![
            ColKind::Unsigned(8),
            ColKind::Unsigned(16),
            ColKind::Unsigned(24),
            ColKind::Unsigned(32),
            ColKind::Unsigned(64),
        ],
        ColKind::Enum(_) => vec![
            ColKind::Enum(vec![
                "a".into(),
                "it's".into(),
                "1".into(),
                String::new(),
                "1.10".into(),
            ]),
            // an enum with one label: the `Ok(0)` and the `i <= len` boundaries alone
            ColKind::Enum(vec!["only".into()]),
        ],
        ColKind::Set(_) => vec![
            ColKind::Set(vec!["x".into(), "y".into(), "z".into()]),
            ColKind::Set(vec!["one".into()]),
        ],
        ColKind::Bit => vec![ColKind::Bit],
    }
}

/// Which variant a kind is, for the coverage test's message. Exhaustive, like `covered`.
fn kind_name(kind: &ColKind) -> &'static str {
    match kind {
        ColKind::Plain => "Plain",
        ColKind::Unsigned(_) => "Unsigned",
        ColKind::Enum(_) => "Enum",
        ColKind::Set(_) => "Set",
        ColKind::Bit => "Bit",
    }
}

/// One-column layout of one kind.
fn layout_of(kind: ColKind) -> Layout {
    Layout::new(vec!["c".into()], vec![kind])
}

fn row_of(v: Option<BinlogValue<'static>>) -> BinlogRow {
    BinlogRow::new(vec![v], Arc::new([]))
}

/// A row with no columns at all: `as_ref` answers `None`, as it does for a column `Row::take`
/// has already taken.
fn empty_row() -> BinlogRow {
    BinlogRow::new(Vec::new(), Arc::new([]))
}

/// The corpus: every `My` variant with the values that have ever distinguished one rendering
/// from another — the decimal digits `arbitrary_precision` keeps, the two zeros, the values
/// `Number::from_f64` refuses, the masks' boundaries, the bytes that are not UTF-8, and both
/// JSON shapes.
fn corpus() -> Vec<(&'static str, Option<BinlogValue<'static>>)> {
    let mut out: Vec<(&'static str, Option<BinlogValue<'static>>)> = Vec::new();
    let mut put = |label, v| out.push((label, v));
    let i = |n: i64| Some(BinlogValue::Value(My::Int(n)));
    let u = |n: u64| Some(BinlogValue::Value(My::UInt(n)));
    let f = |n: f32| Some(BinlogValue::Value(My::Float(n)));
    let d = |n: f64| Some(BinlogValue::Value(My::Double(n)));
    let b = |s: &[u8]| Some(BinlogValue::Value(My::Bytes(s.to_vec())));

    put("missing", None);
    put("null", Some(BinlogValue::Value(My::NULL)));
    // integers: zero, the signs, the mask boundaries, the type boundaries
    for (label, n) in [
        ("int 0", 0i64),
        ("int 1", 1),
        ("int -1", -1),
        ("int 127", 127),
        ("int 128", 128),
        ("int 255", 255),
        ("int 256", 256),
        ("int -256", -256),
        ("int 65535", 65_535),
        ("int 65536", 65_536),
        ("int 2^24-1", 16_777_215),
        ("int 2^24", 16_777_216),
        ("int 2^32-1", 4_294_967_295),
        ("int 2^32", 4_294_967_296),
        ("int i64::MIN", i64::MIN),
        ("int i64::MAX", i64::MAX),
    ] {
        put(label, i(n));
    }
    for (label, n) in [
        ("uint 0", 0u64),
        ("uint 1", 1),
        ("uint 2^32-1", 4_294_967_295),
        ("uint 2^63", 9_223_372_036_854_775_808),
        ("uint u64::MAX", u64::MAX),
    ] {
        put(label, u(n));
    }
    // floats: both zeros, the values a Number refuses, one f32 that is exact in f64 and one that
    // is not, and the shortest-repr boundaries `arbitrary_precision` renders from
    for (label, n) in [
        ("float 0", 0.0f32),
        ("float -0", -0.0f32),
        ("float 1", 1.0),
        ("float 1.5", 1.5),
        ("float 0.1", 0.1),
        ("float 1.10", 1.10),
        ("float nan", f32::NAN),
        ("float inf", f32::INFINITY),
        ("float min positive", f32::MIN_POSITIVE),
        ("float max", f32::MAX),
    ] {
        put(label, f(n));
    }
    for (label, n) in [
        ("double 0", 0.0f64),
        ("double -0", -0.0),
        ("double 1", 1.0),
        ("double 1.5", 1.5),
        ("double 1.10", 1.10),
        ("double 1.1", 1.1),
        ("double 0.1", 0.1),
        ("double 1e300", 1e300),
        ("double -1e-300", -1e-300),
        ("double nan", f64::NAN),
        ("double inf", f64::INFINITY),
        ("double min positive", f64::MIN_POSITIVE),
        ("double 12.34", 12.34),
        ("double -0.0000001", -0.0000001),
    ] {
        put(label, d(n));
    }
    // bytes: the empty string, text that is a number (`arbitrary_precision` keeps its digits),
    // text that is JSON, text that is neither, and bytes that are not text at all
    let mut binary = Vec::with_capacity(256);
    binary.extend(0u8..=255);
    let long = "x".repeat(1000);
    let texts: &[(&str, &[u8])] = &[
        ("bytes empty", b""),
        ("bytes sku", b"sku-1"),
        ("bytes 0", b"0"),
        ("bytes 00", b"00"),
        ("bytes 1.10", b"1.10"),
        ("bytes 1.1", b"1.1"),
        ("bytes 0.000", b"0.000"),
        ("bytes -0.0", b"-0.0"),
        ("bytes -0", b"-0"),
        ("bytes +7", b"+7"),
        ("bytes .5", b".5"),
        ("bytes 1.", b"1."),
        ("bytes 1e309", b"1e309"),
        ("bytes 1_000", b"1_000"),
        ("bytes padded number", b"  12  "),
        ("bytes abc", b"abc"),
        ("bytes null text", b"null"),
        ("bytes true text", b"true"),
        ("bytes json object", br#"{"b": [1, 2], "a": 1}"#),
        ("bytes json array", b"[1,2]"),
        ("bytes broken json", b"{"),
        ("bytes invalid utf8", &[0xff, 0xfe]),
        ("bytes one bad byte", &[0xff]),
        ("bytes NUL in the middle", b"a\0b"),
        ("bytes NUL", b"\0"),
        ("bytes latin1", "ünïcødé".as_bytes()),
        ("bytes 9 bytes", &[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ("bytes 8 ff", &[0xff; 8]),
        ("bytes binary", binary.as_slice()),
        ("bytes long text", long.as_bytes()),
    ];
    for (label, s) in texts {
        put(label, b(s));
    }
    for (label, v) in [
        ("date plain", My::Date(2024, 1, 2, 3, 4, 5, 0)),
        (
            "date microseconds",
            My::Date(2024, 12, 31, 23, 59, 59, 999_999),
        ),
        ("date zero", My::Date(0, 0, 0, 0, 0, 0, 0)),
        ("date max", My::Date(9999, 1, 1, 0, 0, 0, 1)),
        ("time zero", My::Time(false, 0, 0, 0, 0, 0)),
        ("time negative", My::Time(true, 1, 2, 3, 4, 500_000)),
        ("time over a day", My::Time(false, 40, 1, 2, 3, 999_999)),
        ("time negative microseconds", My::Time(true, 0, 0, 0, 0, 1)),
    ] {
        put(label, Some(BinlogValue::Value(v)));
    }
    // the two JSON shapes of a binlog row: a full JSONB value, and a partial update (which
    // carries no value at all, so it renders as `Value::Null` whatever it holds)
    for (label, j) in [
        ("jsonb null", Jsonb::Null),
        ("jsonb true", Jsonb::Bool(true)),
        ("jsonb false", Jsonb::Bool(false)),
        ("jsonb i64", Jsonb::I64(-3)),
        ("jsonb u64", Jsonb::U64(7)),
        ("jsonb f64 1.10", Jsonb::F64(1.10)),
        ("jsonb f64 -0", Jsonb::F64(-0.0)),
        (
            "jsonb text 1.10",
            Jsonb::String(JsonbString::new(b"1.10".to_vec())),
        ),
        (
            "jsonb text invalid utf8",
            Jsonb::String(JsonbString::new(vec![0xff, 0xfe])),
        ),
    ] {
        put(label, Some(BinlogValue::Jsonb(j)));
    }
    put("json diff", Some(BinlogValue::JsonDiff(Vec::new())));
    out
}

/// The two ways of rendering one value, as the engine sees them: through `Layout`, and the raw
/// pair `to_json` / `to_json_ref`.
#[test]
fn the_corpus_still_covers_every_kind() {
    // The compile-time half is `covered`'s match without a wildcard. This is the runtime half: a
    // seed dropped from `kinds`, or an arm that answers nothing, names itself here instead of
    // thinning the equivalence corpus in silence.
    let have: Vec<&'static str> = kinds().iter().map(kind_name).collect();
    let mut missing = Vec::new();
    for probe in [
        ColKind::Plain,
        ColKind::Unsigned(8),
        ColKind::Enum(vec!["a".into()]),
        ColKind::Set(vec!["a".into()]),
        ColKind::Bit,
    ] {
        let name = kind_name(&probe);
        if !have.contains(&name) || covered(&probe).is_empty() {
            missing.push(name);
        }
    }
    assert!(
        missing.is_empty(),
        "the corpus covers no kind of: {missing:?}"
    );
}

#[test]
fn the_borrowed_conversion_matches_the_owned_one() {
    for kind in kinds() {
        let layout = layout_of(kind.clone());
        for (label, v) in corpus() {
            let row = row_of(v.clone());
            let mine = layout.value(&row, 0);
            let theirs = owned_value(row.as_ref(0), &kind);
            let what = format!("{label} as {kind:?}");
            assert_eq!(mine, theirs, "converted value differs: {what}");
            assert_eq!(text(&mine), text(&theirs), "text differs: {what}");
            assert_eq!(
                serde_json::to_string(&mine).ok(),
                serde_json::to_string(&theirs).ok(),
                "json bytes differ: {what}"
            );
        }
    }
    // a column the row does not have: `None`, which is `Value::Null` in both
    let layout = layout_of(ColKind::Unsigned(32));
    assert_eq!(layout.value(&empty_row(), 0), Value::Null);
    assert_eq!(layout.value(&row_of(None), 9), Value::Null);
    assert_eq!(owned_value(None, &ColKind::Bit), Value::Null);
}

/// The same for the query-value conversion with a column, which the hot path passes `None` to but
/// `load_docs`, `response` and `sqlwire` do not: the type-dependent arms (the numeric text that
/// becomes a number, JSON text, `BIT`, the `DATE` shape of a `Date`) must not notice the borrow.
#[test]
fn the_borrowed_query_conversion_matches_for_every_column_type() {
    let col = |t: ColumnType, decimals: u8| Column::new(t).with_decimals(decimals);
    let cols: Vec<(&str, Option<Column>)> = vec![
        ("no column", None),
        (
            "newdecimal",
            Some(col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 4)),
        ),
        ("decimal", Some(col(ColumnType::MYSQL_TYPE_DECIMAL, 2))),
        ("tiny", Some(col(ColumnType::MYSQL_TYPE_TINY, 0))),
        ("short", Some(col(ColumnType::MYSQL_TYPE_SHORT, 0))),
        ("long", Some(col(ColumnType::MYSQL_TYPE_LONG, 0))),
        ("longlong", Some(col(ColumnType::MYSQL_TYPE_LONGLONG, 0))),
        ("int24", Some(col(ColumnType::MYSQL_TYPE_INT24, 0))),
        ("year", Some(col(ColumnType::MYSQL_TYPE_YEAR, 0))),
        ("float", Some(col(ColumnType::MYSQL_TYPE_FLOAT, 2))),
        ("double", Some(col(ColumnType::MYSQL_TYPE_DOUBLE, 6))),
        ("json", Some(col(ColumnType::MYSQL_TYPE_JSON, 0))),
        ("bit", Some(col(ColumnType::MYSQL_TYPE_BIT, 0))),
        ("date", Some(col(ColumnType::MYSQL_TYPE_DATE, 0))),
        ("newdate", Some(col(ColumnType::MYSQL_TYPE_NEWDATE, 0))),
        ("datetime", Some(col(ColumnType::MYSQL_TYPE_DATETIME, 6))),
        ("timestamp", Some(col(ColumnType::MYSQL_TYPE_TIMESTAMP, 0))),
        ("time", Some(col(ColumnType::MYSQL_TYPE_TIME, 6))),
        ("varchar", Some(col(ColumnType::MYSQL_TYPE_VAR_STRING, 0))),
        ("blob", Some(col(ColumnType::MYSQL_TYPE_BLOB, 0))),
        ("enum", Some(col(ColumnType::MYSQL_TYPE_ENUM, 0))),
    ];
    for (column, col) in cols {
        for (label, v) in corpus() {
            let Some(BinlogValue::Value(v)) = v else {
                continue; // the two JSON shapes never reach the query-value path
            };
            let mine = to_json_ref(&v, col.as_ref());
            let theirs = to_json(v.clone(), col.as_ref());
            let what = format!("{label} as {column}");
            assert_eq!(mine, theirs, "converted value differs: {what}");
            assert_eq!(text(&mine), text(&theirs), "text differs: {what}");
            assert_eq!(
                serde_json::to_string(&mine).ok(),
                serde_json::to_string(&theirs).ok(),
                "json bytes differ: {what}"
            );
        }
    }
}

/// The verdict `changed_columns` reaches has to be the verdict comparing the owned values gave —
/// for every kind, and for every ordered pair of the corpus, so that both the equal and the
/// unequal answers come from the shortcut's rule and from the comparison itself.
#[test]
fn the_borrowed_verdict_matches_the_owned_comparison() {
    let corpus = corpus();
    for kind in kinds() {
        let layout = layout_of(kind.clone());
        for (lb, b) in &corpus {
            let rb = row_of(b.clone());
            for (la, a) in &corpus {
                let ra = row_of(a.clone());
                let want = owned_changed(&layout, &rb, &ra, 0);
                assert_eq!(
                    layout.changed(&rb, &ra, 0),
                    want,
                    "{lb} vs {la} as {kind:?}"
                );
            }
        }
    }
    // and a column one of the images does not have compares as the `None` it is
    let full = row_of(Some(BinlogValue::Value(My::Bytes(b"sku-1".to_vec()))));
    for kind in kinds() {
        let layout = layout_of(kind);
        assert!(!layout.changed(&full, &empty_row(), 1)); // both are `None`: equal
        assert!(layout.changed(&full, &empty_row(), 0)); // bytes vs `None`: changed
        assert!(!layout.changed(&empty_row(), &empty_row(), 0));
    }
}

// ── the binlog bytes of a text column, decoded by the column's own charset (backlog B-1) ──

/// A one-column latin1 layout.
fn latin1_layout() -> Layout {
    Layout::with_charsets(
        vec!["c".into()],
        vec![ColKind::Plain],
        vec![Charset::Latin1],
        Vec::new(),
    )
}

/// A latin1 key column's bytes are cp1252 (MySQL's latin1), not broken UTF-8: 0xC9 is `É`,
/// the bytes ≥ 0x80 keep their one character each, and none of it loses to U+FFFD — which
/// is what let the binlog side of a latin1 key mark a key the target's select could never
/// match, and delete the document of a live row.
#[test]
fn latin1_bytes_render_as_their_text() {
    let layout = latin1_layout();
    let cases: &[(&[u8], &str)] = &[
        (&[0x41], "A"),
        (&[0xC9], "\u{c9}"),   // É, the measured key of the e2e case
        (&[0xE9], "\u{e9}"),   // é
        (&[0x80], "\u{20ac}"), // €  (cp1252)
        (&[0x99], "\u{2122}"), // ™  (cp1252)
        (&[0x9F], "\u{178}"),  // Ÿ  (cp1252)
        (&[0x81], "\u{81}"),   // MySQL's latin1 keeps the five unused cp1252 slots as C1
        (&[0x8d], "\u{8d}"),
        (&[0x8f], "\u{8f}"),
        (&[0x90], "\u{90}"),
        (&[0x9d], "\u{9d}"),
        (&[0xA0], "\u{a0}"),
        (&[0xFF], "\u{ff}"), // ÿ
        (b"caf\xe9", "caf\u{e9}"),
        (&[], ""),
    ];
    for (bytes, want) in cases {
        let row = row_of(Some(BinlogValue::Value(My::Bytes(bytes.to_vec()))));
        assert_eq!(
            layout.value(&row, 0),
            Value::String((*want).into()),
            "bytes {bytes:02x?}"
        );
    }
}

/// What the charset does not decode stays exactly as it was: the default (and unknown)
/// charset keeps the lossy path, so utf8mb4 keys and BINARY keys are byte-for-byte the
/// behaviour the corpus pins above.
#[test]
fn other_charsets_keep_the_lossy_path() {
    // `Layout::new` knows no charset: today's answers, pinned by the corpus, unchanged.
    let layout = layout_of(ColKind::Plain);
    let row = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xff, 0xfe]))));
    assert_eq!(
        layout.value(&row, 0),
        Value::String("\u{fffd}\u{fffd}".into())
    );
    // valid UTF-8 is untouched by the decoding: the default layout renders it as the text it is
    let e = "É😀".as_bytes().to_vec();
    let row = row_of(Some(BinlogValue::Value(My::Bytes(e.clone()))));
    assert_eq!(
        layout_of(ColKind::Plain).value(&row, 0),
        Value::String("É😀".into())
    );
    // a latin1 column holds latin1 bytes: six bytes, six characters — decoding is what the
    // column's charset says, not what happens to parse (and cp1252 is not ISO-8859-1: the
    // 0x80..0x9F bytes are the Windows specials, 89 is ‰ and 9F is Ÿ)
    assert_eq!(
        latin1_layout().value(&row, 0),
        Value::String("\u{c3}\u{2030}\u{f0}\u{178}\u{2dc}\u{20ac}".into())
    );
}

/// The `information_schema` names that decide the decoding.
#[test]
fn col_charset_reads_the_information_schema_name() {
    assert_eq!(col_charset(Some("latin1")), Charset::Latin1);
    // the `binary` charset gets its own variant: same rendering as `Utf8`, but the key
    // refusal sees it (see `Layout::key_unsafe`)
    assert_eq!(col_charset(Some("binary")), Charset::Binary);
    // the UTF-8 family and `ascii`: bytes the column's own definition makes valid UTF-8,
    // so the lossy path renders them exactly
    for name in ["utf8mb4", "utf8mb3", "utf8", "ascii"] {
        assert_eq!(col_charset(Some(name)), Charset::Utf8, "{name} is exact");
    }
    // everything else is a name lazywf has no table for: the same rendering as `Utf8`, but
    // under a variant that says nothing about those bytes is text — the one the decode-point
    // guard watches (see `Layout::key_is_lossy`)
    for name in ["gbk", "latin2", "ujis", "big5", "ucs2", "utf16le", "koi8r"] {
        assert_eq!(
            col_charset(Some(name)),
            Charset::Unknown,
            "{name} keeps today's path, under a name that says nothing about its bytes"
        );
    }
    // NULL is the columns that have no charset at all: numbers and dates, whose bytes the
    // guard never reads, and the geometry family, whose bytes it does
    for name in [None, Some("")] {
        assert_eq!(col_charset(name), Charset::Unknown, "no charset named");
    }
}

/// `binlog_row_metadata=FULL`: a layout derived from the event's own names keeps the kinds,
/// the charsets and the data types of the `information_schema` read it was derived from.
#[test]
fn layout_from_names_keeps_the_charsets() {
    let info = Layout::with_charsets(
        vec!["id".into(), "code".into(), "blob".into()],
        vec![ColKind::Unsigned(32), ColKind::Plain, ColKind::Plain],
        vec![Charset::Utf8, Charset::Latin1, Charset::Binary],
        vec!["int".into(), "varchar".into(), "blob".into()],
    );
    // the event's own order, a column the read does not know, and the latin1 column by name
    let names = vec![
        "code".to_string(),
        "id".to_string(),
        "extra".to_string(),
        "blob".to_string(),
    ];
    let l = layout_from_names(&info, &names);
    let bytes = |b: Vec<u8>| Some(BinlogValue::Value(My::Bytes(b)));
    let row = BinlogRow::new(vec![bytes(vec![0xC9]); 4], Arc::new([]));
    assert_eq!(
        l.value(&row, 0),
        Value::String("\u{c9}".into()),
        "the latin1 name found its charset"
    );
    // the plain and the unknown columns keep the lossy path (the value is read again below)
    let row = BinlogRow::new(vec![bytes(vec![0xff]); 4], Arc::new([]));
    assert_eq!(l.value(&row, 1), Value::String("\u{fffd}".into()));
    assert_eq!(l.value(&row, 2), Value::String("\u{fffd}".into()));
    assert_eq!(l.value(&row, 3), Value::String("\u{fffd}".into()));
    // the types travel with them — the key refusal reads them from the derived layout too
    assert_eq!(l.column_type(0), Some("varchar"));
    assert_eq!(l.column_type(1), Some("int"));
    // a key may stand on the decoded latin1 and the number, and on the unknown name (no
    // refusal on a guess); the blob is unsafe wherever it stands
    assert_eq!(l.key_unsafe(0), None);
    assert_eq!(l.key_unsafe(1), None);
    assert_eq!(l.column_type(2), Some(""));
    assert_eq!(l.key_unsafe(2), None);
    assert!(l.key_unsafe(3).is_some(), "the blob is unsafe by name");
}

/// Two latin1 payloads that differ stay different (cp1252 is one-to-one), so the changed
/// shortcut still decides them by the conversion it must make — and identical bytes still
/// cost nothing.
#[test]
fn latin1_changed_keeps_the_shortcut_and_the_verdict() {
    let layout = latin1_layout();
    let a = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xC9]))));
    let b = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xE9]))));
    assert!(
        layout.changed(&a, &b, 0),
        "\u{c9} and \u{e9} are different text"
    );
    // where the old lossy path saw one replacement char in both and called it unchanged
    let f = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xff]))));
    let e = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xfe]))));
    assert!(
        layout.changed(&f, &e, 0),
        "\u{ff} and \u{fe} are different characters, whatever lossy made of them"
    );
    let ((), n) = alloc_measure::count(|| {
        for _ in 0..20 {
            std::hint::black_box(layout.changed(&a, &a, 0));
        }
    });
    assert_eq!(n, 0, "identical latin1 bytes were converted anyway");
}

/// One `Value::String` of these bytes: the cost the borrowed conversion pays for a text column.
fn bytes_cost() -> usize {
    let (_, n) = alloc_measure::count(|| {
        std::hint::black_box(String::from_utf8_lossy(b"sku-1").into_owned())
    });
    n
}

/// What the two ways cost, and that the shortcut is not decoration: comparing two images that hold
/// the same bytes converts neither, so it allocates nothing at all.
#[test]
fn comparing_two_images_costs_nothing_when_their_bytes_are_the_same() {
    assert_eq!(bytes_cost(), 1, "a text column should cost one allocation");
    let layout = layout_of(ColKind::Plain);
    let sku = row_of(Some(BinlogValue::Value(My::Bytes(b"sku-1".to_vec()))));
    let other = row_of(Some(BinlogValue::Value(My::Bytes(b"sku-2".to_vec()))));
    // the same `Vec` twice: the images of a row come from one event, so this is the common case
    let ((), same) = alloc_measure::count(|| {
        for _ in 0..20 {
            std::hint::black_box(layout.changed(&sku, &sku, 0));
        }
    });
    assert_eq!(same, 0, "identical Bytes columns were converted anyway");
    // and the same bytes in two separate allocations: still no conversion
    let twin = row_of(Some(BinlogValue::Value(My::Bytes(b"sku-1".to_vec()))));
    let ((), twin_n) = alloc_measure::count(|| {
        for _ in 0..20 {
            std::hint::black_box(layout.changed(&sku, &twin, 0));
        }
    });
    assert_eq!(twin_n, 0, "equal Bytes columns were converted anyway");

    // two different columns do convert: one text each, where the owned comparison paid for the
    // text and for the `Vec` clone it came out of
    let ((), mine) = alloc_measure::count(|| {
        for _ in 0..20 {
            std::hint::black_box(layout.changed(&sku, &other, 0));
        }
    });
    let ((), theirs) = alloc_measure::count(|| {
        for _ in 0..20 {
            std::hint::black_box(owned_changed(&layout, &sku, &other, 0));
        }
    });
    assert_eq!(
        mine,
        2 * 20 * bytes_cost(),
        "a Bytes comparison should convert both sides once"
    );
    assert_eq!(
        theirs,
        2 * 20 * (bytes_cost() + 1),
        "the owned comparison no longer pays for the clone it was the finding"
    );

    // the same for the other shapes that skip: `None`, `NULL`, and a partial JSON update
    for (label, v) in [
        ("null", Some(BinlogValue::Value(My::NULL))),
        ("json diff", Some(BinlogValue::JsonDiff(Vec::new()))),
        ("missing", None),
    ] {
        let r = row_of(v);
        let ((), n) = alloc_measure::count(|| {
            for _ in 0..20 {
                std::hint::black_box(layout.changed(&r, &r, 0));
            }
        });
        assert_eq!(n, 0, "two {label} columns were converted");
    }
    // a `Jsonb` is not one of them: it converts and compares, which is the old answer
    let j = row_of(Some(BinlogValue::Jsonb(Jsonb::String(JsonbString::new(
        b"abc".to_vec(),
    )))));
    let ((), n) = alloc_measure::count(|| {
        for _ in 0..20 {
            std::hint::black_box(layout.changed(&j, &j, 0));
        }
    });
    assert!(
        n > 0,
        "a Jsonb comparison should still go through the conversion"
    );
}

/// Where the shortcut has to keep its hands off: payloads that differ but convert to the same
/// value, and payloads that are equal in the database but differ as `My`. Both must land on the
/// comparison, because the shortcut cannot prove anything about them.
#[test]
fn the_shortcut_only_skips_what_it_can_prove() {
    let layout = layout_of(ColKind::Unsigned(8));
    // two `i64` whose low byte is the same: the mask makes them one value, so the verdict is the
    // comparison's, not the payloads'
    let a = row_of(Some(BinlogValue::Value(My::Int(1))));
    let b = row_of(Some(BinlogValue::Value(My::Int(257))));
    assert!(!layout.changed(&a, &b, 0), "the mask is not applied");
    assert!(!owned_changed(&layout, &a, &b, 0));
    // two byte strings that lose to the same text: invalid UTF-8 collapses to one `U+FFFD` run
    let layout = layout_of(ColKind::Plain);
    let a = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xff]))));
    let b = row_of(Some(BinlogValue::Value(My::Bytes(vec![0xfe]))));
    assert!(!same_value(a.as_ref(0), b.as_ref(0)));
    assert!(
        !layout.changed(&a, &b, 0),
        "both bytes lose to one replacement char: unchanged"
    );
    // the same bytes are equal to a missing column only when they render as `Null`
    let null = row_of(Some(BinlogValue::Value(My::NULL)));
    assert!(!layout.changed(&null, &empty_row(), 0));
    let empty = row_of(Some(BinlogValue::Value(My::Bytes(Vec::new()))));
    assert!(
        layout.changed(&empty, &empty_row(), 0),
        "an empty string is not NULL, and the check says so"
    );
    // floats compare by bits: two NaNs are not the same bits, and so go to the comparison
    let n1 = row_of(Some(BinlogValue::Value(My::Double(f64::NAN))));
    let n2 = row_of(Some(BinlogValue::Value(My::Double(f64::NAN))));
    assert!(
        !same_my(
            &My::Double(f64::NAN),
            &My::Double(f64::from_bits(0x7ff8_0000_0000_0001))
        ),
        "a different NaN payload is not the same bits"
    );
    assert!(
        !layout.changed(&n1, &n2, 0),
        "both NaN render as `Null`, which is what today says"
    );
    // and the two zeros, which differ as bits and as digits
    let z = row_of(Some(BinlogValue::Value(My::Double(0.0))));
    let nz = row_of(Some(BinlogValue::Value(My::Double(-0.0))));
    assert!(layout.changed(&z, &nz, 0), "+0.0 vs -0.0 stays changed");
    assert_eq!(
        layout.changed(&z, &nz, 0),
        owned_changed(&layout, &z, &nz, 0)
    );
    // a `Jsonb` never takes the shortcut, equal or not
    let j = row_of(Some(BinlogValue::Jsonb(Jsonb::U64(1))));
    assert!(!same_value(j.as_ref(0), j.as_ref(0)));
    assert!(!layout.changed(&j, &j, 0));
    // a `Value` and a `Jsonb` of the same thing compare as their two conversions do
    let one = row_of(Some(BinlogValue::Value(My::Bytes(b"1".to_vec()))));
    assert_eq!(
        layout.changed(&j, &one, 0),
        owned_changed(&layout, &j, &one, 0)
    );
}

// ── the columns a key may stand on: bytes no charset defines are refused at rule load
// (backlog B-1, refusal [A]) ──

/// A one-column layout as `Catalog::load` builds it: name, charset, `DATA_TYPE`.
fn typed(name: &str, charset: Charset, data_type: &str) -> Layout {
    Layout::with_charsets(
        vec![name.into()],
        vec![col_kind(data_type, data_type)],
        vec![charset],
        vec![data_type.into()],
    )
}

/// The decode-point guard (`Layout::key_is_lossy`): which bytes the decoder invented a
/// character for, and which the row really holds.
fn guard_row(b: &[u8]) -> (Layout, BinlogRow) {
    let l = typed("c", Charset::Unknown, "varchar");
    let row = row_of(Some(BinlogValue::Value(My::Bytes(b.to_vec()))));
    (l, row)
}

/// Bytes no charset reads as text: the replacement character in the key is the decoder's own,
/// so the guard says so — this is the case that used to mark a key of U+FFFDs and let its
/// DELETE remove another row's document.
#[test]
fn impossible_bytes_of_an_unknown_charset_are_the_decoders_text() {
    let (l, row) = guard_row(&[0xFF, 0xFE, 0x41]);
    assert!(
        l.value(&row, 0)
            .as_str()
            .is_some_and(|s| s.contains(char::REPLACEMENT_CHARACTER)),
        "the lossy path renders what it always rendered"
    );
    assert!(l.key_is_lossy(&row, 0, &l.value(&row, 0)));
}

/// A value that really holds U+FFFD is the row's own: its bytes are valid UTF-8, and the key
/// that names them is exact. The guard tells the two apart by the bytes, not by the text.
#[test]
fn a_legal_replacement_character_stays_the_rows_key() {
    let (l, row) = guard_row("a\u{FFFD}b".as_bytes());
    assert_eq!(l.value(&row, 0), Value::String("a\u{FFFD}b".into()));
    assert!(
        !l.key_is_lossy(&row, 0, &l.value(&row, 0)),
        "the row holds the character, the decoder did not make it"
    );
}

/// What keeps the guard off the hot path: a column whose charset says what its bytes are
/// never has them looked at. `utf8mb4` promises valid UTF-8, `latin1` is decoded exactly and
/// cp1252 has no replacement character to give; the other kinds are not bytes of a text
/// charset at all, and a column the row does not have cannot be anyone's key.
#[test]
fn a_charset_that_names_its_bytes_is_never_read_by_the_guard() {
    let bytes = vec![0xFFu8, 0xFE];
    let row = row_of(Some(BinlogValue::Value(My::Bytes(bytes.clone()))));
    for charset in [Charset::Utf8, Charset::Latin1] {
        let l = typed("c", charset, "varchar");
        assert!(
            !l.key_is_lossy(&row, 0, &l.value(&row, 0)),
            "{charset:?}: a charset lazywf decodes cannot owe the guard a look"
        );
    }
    // the `binary` family is refused as a key at load, and the guard holds the line too
    let l = typed("c", Charset::Binary, "binary");
    assert!(l.key_is_lossy(&row, 0, &l.value(&row, 0)));
    // a number under an unknown charset: no bytes, so nothing to read
    let n = row_of(Some(BinlogValue::Value(My::Int(7))));
    let l = typed("n", Charset::Unknown, "int");
    assert!(!l.key_is_lossy(&n, 0, &l.value(&n, 0)));
    // a label list whose label holds U+FFFD is the label, not broken bytes
    let labels = Layout::with_charsets(
        vec!["c".into()],
        vec![ColKind::Set(vec!["\u{FFFD}".into()])],
        vec![Charset::Unknown],
        vec!["set".into()],
    );
    let set = row_of(Some(BinlogValue::Value(My::Bytes(vec![1]))));
    assert_eq!(labels.value(&set, 0), Value::String("\u{FFFD}".into()));
    assert!(
        !labels.key_is_lossy(&set, 0, &labels.value(&set, 0)),
        "a mask never renders bytes as text, so it cannot hold the decoder's character"
    );
    // a column the row does not have
    assert!(!l.key_is_lossy(&empty_row(), 0, &Value::String("x\u{FFFD}".into())));
}

/// The layout a table map's own names derive (`layout_from_names`) leaves a column the
/// `information_schema` read does not know in [`Charset::Unknown`]: nothing said what its
/// bytes are, which is exactly what the guard asks about.
#[test]
fn a_column_no_read_names_is_unknown_to_the_guard() {
    let info = typed("known", Charset::Utf8, "varchar");
    let l = layout_from_names(&info, &["known".to_string(), "surprise".to_string()]);
    let row = BinlogRow::new(
        vec![
            Some(BinlogValue::Value(My::Bytes(b"ok".to_vec()))),
            Some(BinlogValue::Value(My::Bytes(vec![0xC3, 0x28, 0xA0]))),
        ],
        Arc::new([]),
    );
    assert!(l.key_is_lossy(&row, 1, &l.value(&row, 1)));
    assert!(!l.key_is_lossy(&row, 0, &l.value(&row, 0)));
}

/// The `binary`-charset family — BINARY, VARBINARY and the BLOBs on both servers, and the
/// text types with the BINARY attribute — cannot carry a key: no charset defines their bytes,
/// so the key text is whatever `from_utf8_lossy` leaves. The type alone refuses too, for a
/// server that reports the charset as `NULL`.
#[test]
fn binary_charset_columns_cannot_carry_a_key() {
    for t in [
        "binary",
        "varbinary",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
    ] {
        let l = typed("c", Charset::Binary, t);
        assert!(l.key_unsafe(0).is_some(), "{t} must be refused");
    }
    // `VARCHAR(16) BINARY`: a varchar in the binary charset is bytes too
    assert!(typed("c", Charset::Binary, "varchar")
        .key_unsafe(0)
        .is_some());
    for t in [
        "binary",
        "varbinary",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
    ] {
        let l = typed("c", Charset::Utf8, t);
        assert!(l.key_unsafe(0).is_some(), "{t} is refused by type as well");
    }
}

/// The geometry family: opaque WKB under a `NULL` charset, spelled as each server spells it
/// (MySQL's `geomcollection` and `gis`, MariaDB's `geometrycollection`), plus MySQL 9's
/// `VECTOR`. None of them can carry a key.
#[test]
fn geometry_columns_cannot_carry_a_key() {
    for t in [
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geomcollection",
        "geometrycollection",
        "gis",
        "vector",
    ] {
        let l = typed("c", Charset::Utf8, t);
        assert!(l.key_unsafe(0).is_some(), "{t} must be refused");
    }
}

/// What the decoder answers exactly stays accepted — text in any charset the decode renders
/// (or leaves lossless), numbers, and the types whose wire form is not bytes at all. The e2e
/// pins a few of these end to end; this is the full list the refusal walks past.
#[test]
fn text_and_number_columns_carry_a_key() {
    for (charset, t) in [
        (Charset::Utf8, "varchar"),
        (Charset::Latin1, "varchar"),
        (Charset::Utf8, "text"),
        (Charset::Utf8, "char"),
        (Charset::Utf8, "int"),
        (Charset::Utf8, "bigint"),
        (Charset::Utf8, "decimal"),
        (Charset::Utf8, "date"),
        (Charset::Utf8, "json"),
        (Charset::Utf8, "enum"),
        (Charset::Utf8, "set"),
        (Charset::Utf8, "bit"),
    ] {
        assert_eq!(
            typed("c", charset, t).key_unsafe(0),
            None,
            "{t} must stay accepted"
        );
    }
    // no type read (a layout built without the catalog): no refusal on a guess
    assert_eq!(
        Layout::new(vec!["c".into()], vec![ColKind::Plain]).key_unsafe(0),
        None
    );
    // and were a server ever to report the `binary` charset on one of them, the charset
    // rule answers first: bytes are bytes whatever the type says
    assert!(typed("c", Charset::Binary, "json").key_unsafe(0).is_some());
}
