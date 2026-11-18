use super::*;

fn locate_all(ks: &mut KeySpaces, keys: &[&str]) -> IdSets {
    let mut sets = IdSets::default();
    for k in keys {
        let (s, key) = ks.locate(k);
        assert!(sets.insert(s, key), "{k} inserted twice");
    }
    sets
}

#[test]
fn shapes_round_trip() {
    let keys = [
        "42",
        "0",
        "item_42",
        "item_7",
        "group_item_7",
        "007",
        "sku-ABC",
        "a b",
        "18446744073709551615",
        "18446744073709551616",
        "x0",
        "-5",
        "+5",
        "9223372036854775808",
        "item_-5",
    ];
    let mut ks = KeySpaces::new();
    let sets = locate_all(&mut ks, &keys);
    assert_eq!(sets.len(), keys.len());
    for k in keys {
        assert!(sets.contains_text(&ks, k) || ks.peek(k).is_none(), "{k}");
        let (s, key) = ks.locate(k);
        assert!(sets.contains(s, &key), "{k}");
    }
    let mut got = ks.render(&sets.sets);
    got.sort();
    let mut want: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    want.sort();
    assert_eq!(got, want, "every key reads back exactly");
    // Leading zeros and numbers too large for a u64 are kept as text.
    assert_eq!(split("007"), None);
    assert_eq!(split("18446744073709551616"), None);
    assert_eq!(split("item_42"), Some(("item_", 42)));
}

/// Where `split` does not take a key (no trailing digits, leading zeros, or more than
/// 20 of them), the core's rule agrees: `from_text` keeps it as a bytes tuple and it
/// round-trips exactly.
#[test]
fn the_core_text_rule_matches_split() {
    for k in [
        "007",
        "sku-ABC",
        "a b",
        "",
        "item_07",
        "18446744073709551616",
        "00000000000000000000005",
    ] {
        assert!(split(k).is_none(), "{k}: a text key here");
        let key = text_key(k);
        assert!(
            matches!(&key, Key::Tuple(t) if matches!(&**t, [Datum::Bytes(_)])),
            "{k}"
        );
        let mut out = String::new();
        key.to_text(&mut out);
        assert_eq!(out, k, "the text form round-trips for {k:?}");
    }
    // A key split does take round-trips through the prefix and the number too (the
    // number is a u64, so it reaches past the i64 the core's own rule would take).
    let mut ks = KeySpaces::new();
    for k in [
        "42",
        "18446744073709551615",
        "9223372036854775808",
        "item_42",
        "-5",
        "+5",
        "-0",
        "A-1",
        "x0",
    ] {
        let (s, key) = ks.locate(k);
        assert!(matches!(key, Key::Int(_)), "{k} is an integer key");
        let mut set = IdSet::default();
        set.insert(key.clone());
        let mut sets = BTreeMap::new();
        sets.insert(s, set);
        assert_eq!(ks.render(&sets), vec![k.to_string()], "{k} renders back");
    }
}

#[test]
fn iteration_order_prefixes_then_numbers_then_text() {
    let mut ks = KeySpaces::new();
    // Filled in scrambled order: prefixes still render in order, numbers ascending,
    // the text keys last (their internal order is the core set's).
    let sets = locate_all(
        &mut ks,
        &["z", "p_1", "3", "item_9", "abc_2", "1", "y", "2"],
    );
    let mut got = ks.render(&sets.sets);
    let text = got.split_off(6);
    assert_eq!(got, vec!["1", "2", "3", "abc_2", "item_9", "p_1"]);
    assert_eq!(text.len(), 2);
    assert!(text.contains(&"y".to_string()) && text.contains(&"z".to_string()));
}

#[test]
fn dedup_extend_and_bounded_take() {
    let mut ks = KeySpaces::new();
    let mut a = IdSets::default();
    let (s, k) = ks.locate("p_1");
    assert!(a.insert(s, k));
    let (s, k) = ks.locate("p_1");
    assert!(!a.insert(s, k), "the second insert is not new");
    for t in ["3", "1", "z"] {
        let (s, k) = ks.locate(t);
        a.insert(s, k);
    }
    let mut b = IdSets::default();
    for t in ["2", "p_1", "y"] {
        let (s, k) = ks.locate(t);
        b.insert(s, k);
    }
    a.extend(b);
    assert_eq!(a.len(), 6);
    assert_eq!(a.take_texts(2, &ks), vec!["1", "2"]);
    // The batch crosses into the text space, whose internal order the core set is free
    // to pick (a seeded hash): the integers are deterministic, the text key is `y` or `z`.
    let batch = a.take_texts(3, &ks);
    assert_eq!(&batch[..2], ["3", "p_1"]);
    assert!(batch[2] == "z" || batch[2] == "y");
    let rest = a.take_texts(9, &ks);
    assert_eq!(rest.len(), 1);
    assert!((rest[0] == "z" || rest[0] == "y") && rest[0] != batch[2]);
    assert!(a.is_empty());
    let mut c = IdSets::default();
    for i in 1..=10u64 {
        let (s, k) = ks.locate(&i.to_string());
        c.insert(s, k);
    }
    assert!(c.contains_text(&ks, "5"));
    assert!(!c.contains_text(&ks, "50"));
}

#[test]
fn a_million_dense_ids_drain_in_order() {
    let mut ks = KeySpaces::new();
    let mut sets = IdSets::default();
    for i in 1..=1_000_000u64 {
        let (s, k) = ks.locate(&i.to_string());
        assert!(sets.insert(s, k));
    }
    assert_eq!(sets.len(), 1_000_000);
    assert_eq!(sets.take_texts(3, &ks), vec!["1", "2", "3"]);
    let (s, k) = ks.locate("0");
    sets.insert(s, k);
    assert_eq!(sets.take_texts(2, &ks), vec!["0", "4"]);
    assert_eq!(sets.len(), 999_996);
}

#[test]
fn prefixes_beyond_the_slots_live_in_the_text_space() {
    let mut ks = KeySpaces::new();
    let mut sets = IdSets::default();
    for i in 0..KeySpaces::SLOTS as u64 + 5 {
        let (s, k) = ks.locate(&format!("p{i}_{}", i + 1));
        if i >= KeySpaces::SLOTS as u64 {
            assert_eq!(s, ks.text(), "the overflow keys are tuples");
            assert!(matches!(k, Key::Tuple(_)));
        }
        assert!(sets.insert(s, k));
    }
    let mut got = ks.render(&sets.sets);
    got.sort();
    let mut want: Vec<String> = (0..KeySpaces::SLOTS as u64 + 5)
        .map(|i| format!("p{i}_{}", i + 1))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

/// The real bytes behind a million marked keys (the hash-set backend): run with
/// `cargo test --lib keyspace -- --ignored --exact keyspace::tests::keys_cost --nocapture`
/// (the ignored filter keeps it alone, so the counting allocator sees only it).
#[test]
#[ignore]
fn keys_cost() {
    // `plain` and `prefixed` are canonical integers (integer sets); `text` ends in a
    // letter, so `split` refuses it and it lives in the text space as a tuple.
    for shape in ["plain", "prefixed", "text"] {
        let mut ks = KeySpaces::new();
        let mut marks = IdSets::default();
        let mut buffer = IdSets::default();
        let text = |i: u64| match shape {
            "plain" => i.to_string(),
            "prefixed" => format!("item_{i}"),
            _ => format!("k{i}x"),
        };
        let n = 1_000_000u64;
        measure::ON.store(true, std::sync::atomic::Ordering::Relaxed);
        let before = measure::live();
        for i in 1..=n {
            let t = text(i);
            let (s, k) = ks.locate(&t);
            marks.insert(s, k.clone());
            buffer.insert(s, k);
        }
        let used = measure::live() - before;
        measure::ON.store(false, std::sync::atomic::Ordering::Relaxed);
        let per_key = used as f64 / (2 * n) as f64;
        eprintln!("{shape}: {per_key:.1} bytes per key held twice (marks + buffer)");
        // The integer sets are roaring bitmaps in this run (consecutive ids are nearly
        // free); the text keys pay for the tuple and its bytes on top of the set node.
        let ceiling = if shape == "text" { 260.0 } else { 20.0 };
        assert!(
            per_key < ceiling,
            "{shape}: {per_key} bytes per key is a regression"
        );
        assert_eq!(buffer.len(), n as usize);
    }
}

/// The `{{op}}` text a template sees for a row change.
#[test]
fn op_text_names_the_row_change() {
    assert_eq!(op_text(Op::Insert), "insert");
    assert_eq!(op_text(Op::Update), "update");
    assert_eq!(op_text(Op::Delete), "delete");
}

/// `as_map` is the raw view `render` and `take_texts` read: one set per space, in space order
/// (the text space first, then the prefix slots as they were assigned), empties kept — dropping
/// them is `into_pairs`, the shape the core's buffer takes back.
#[test]
fn as_map_holds_one_set_per_space_in_space_order() {
    let mut ks = KeySpaces::new();
    let mut sets = IdSets::default();
    for k in ["sku-x", "item_1", "item_2", "42"] {
        let (s, key) = ks.locate(k);
        sets.insert(s, key);
    }
    // Text space 0 first, then the prefix slots in the order they were assigned: `item_`
    // took slot 1 and the bare-number prefix `""` took slot 2.
    assert_eq!(
        sets.as_map().keys().map(|s| s.0).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(sets.as_map().values().map(|s| s.len()).sum::<usize>(), 4);
    assert_eq!(sets.as_map()[&ks.text()].len(), 1, "sku-x is the text key");
    assert_eq!(sets.as_map()[&SpaceId(1)].len(), 2, "the `item_` slot");

    // An empty set stays in `as_map` and is dropped by `into_pairs`.
    sets.sets.entry(SpaceId(7)).or_default();
    assert!(sets.as_map().contains_key(&SpaceId(7)));
    assert!(sets.as_map()[&SpaceId(7)].is_empty());
    assert_eq!(
        sets.clone()
            .into_pairs()
            .iter()
            .map(|(_, s)| s.len())
            .sum::<usize>(),
        4,
        "the empty space is not handed to the core"
    );
    assert_eq!(sets.clone().into_pairs().len(), 3);
}

/// A key the core holds as a multi-part tuple (only an embedding's own keys are such): it
/// renders as the core names it — the parts' `Debug` forms joined by `,`, which is a log form,
/// never data — where a one-part `Bytes` tuple renders as its text.
#[test]
fn a_multi_part_tuple_renders_as_the_core_names_it() {
    let mut ks = KeySpaces::new();
    let (slot, _) = ks.locate("p_1");
    let mut sets = IdSets::default();
    assert!(sets.insert(
        slot,
        Key::Tuple(Box::new([Datum::Bytes(b"item".to_vec()), Datum::Int(7)]))
    ));
    assert_eq!(
        ks.render(&sets.sets),
        vec!["Bytes([105, 116, 101, 109]),Int(7)".to_string()]
    );
    // The same key through the bounded take of a reset batch.
    let mut sets = sets;
    assert_eq!(
        sets.take_texts(1, &ks),
        vec!["Bytes([105, 116, 101, 109]),Int(7)".to_string()]
    );
    assert!(sets.is_empty());
}

/// A set of text keys as the core holds them (`Key::from_text`): what a listing or a snapshot
/// page is, and the seen-keys of one flush. Every key written is found again, a duplicate is
/// one key, and the two kinds of id — the integer `1`, the tuple `007` — live side by side.
#[test]
fn a_text_set_holds_the_keys_as_the_core_holds_them() {
    let set = text_set(["1", "item_1", "sku-x", "007", ""]);
    assert_eq!(set.len(), 5);
    for k in ["1", "item_1", "sku-x", "007", ""] {
        assert!(set.contains(&Key::from_text(k)), "{k}");
    }
    assert!(!set.contains(&Key::from_text("2")), "a key never written");
    assert_eq!(text_set(["1", "1"]).len(), 1, "a duplicate is one key");
    assert!(text_set(std::iter::empty::<&str>()).is_empty());
    // The canonical integer is the integer id, held as `intset::from_i64` maps it; `007` is
    // not canonical, so it is a tuple of its bytes.
    let set = text_set(["1", "007"]);
    assert!(
        set.ints.contains(crate::intset::from_i64(1)),
        "`1` is an integer id"
    );
    let k7 = Key::from_text("007");
    let Key::Tuple(t) = &k7 else {
        panic!("`007` is not canonical, so it is a tuple id, not {k7:?}");
    };
    assert!(set.keys.contains(t), "`007` is a tuple id");
    // Rendering them back gives the text that went in.
    let ks = KeySpaces::new();
    let mut sets = IdSets::default();
    sets.sets.insert(ks.text(), text_set(["sku-x", "item_1"]));
    let mut got = ks.render(&sets.sets);
    got.sort();
    assert_eq!(got, vec!["item_1".to_string(), "sku-x".to_string()]);
}

// The counting allocator lives in the dedicated `crate::alloc_measure` module; the tests
// above use it as `measure`.
use crate::alloc_measure as measure;
