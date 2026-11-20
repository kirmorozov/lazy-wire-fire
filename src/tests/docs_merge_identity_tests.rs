//! The merger's verdict and its output are the contract. `load_docs` used to hand it a deep
//! copy of every select's rowset (`m.add(plan, rows.clone())`) and now hands it the rowset
//! itself; the same keys in must still produce the same `Doc`s out, byte for byte — same field
//! values, same tombstones, same keys answered and keys not answered.
//!
//! These compare the two handovers over one pinned corpus, so a change that altered which doc a
//! key gets shows up here rather than in a diff of allocation counts.

use super::*;
use crate::alloc_measure;
use serde_json::json;

fn names(s: &[&str]) -> Vec<String> {
    s.iter().map(|x| x.to_string()).collect()
}

fn keys(s: &[&str]) -> Vec<String> {
    names(s)
}

/// The pinned corpus: an attribute select over four keys. `1` and `2` are answered, `3` comes
/// back as NULL (a key the select has no row for is skipped, not blanked), and `4` is never
/// answered at all. The `meta` column is a JSON text column, so it goes through `parse_json`.
fn attrs_plan() -> Plan {
    Plan::new(&names(&["_key", "sku", "price.final", "meta:json"])).expect("plan")
}

fn attrs_rows() -> Vec<Vec<Value>> {
    vec![
        vec![json!("1"), json!("A-1"), json!(9.5), json!("{\"a\":1}")],
        vec![json!("2"), json!("B-2"), Value::Null, json!("not json")],
        vec![json!("3"), Value::Null, Value::Null, Value::Null],
    ]
}

/// A second select for the same keys: a member group, and the `_op` that makes one key a
/// tombstone.
fn ops_plan() -> Plan {
    Plan::new(&names(&["_key", "tags[].tag", "_op"])).expect("plan")
}

fn ops_rows() -> Vec<Vec<Value>> {
    vec![
        vec![json!("1"), json!("new"), json!("upsert")],
        vec![json!("2"), json!("old"), json!("DELETE")],
    ]
}

/// The docs of a chunk merged through both handovers: `rows.clone()` from a rowset the caller
/// keeps (what `load_docs` did) and the rowset itself, moved (what it does now). The rows are
/// copied for the moved side OUTSIDE anything being measured, purely to have a second owner to
/// hand over: what the two paths differ in is whether the merger's argument is a copy.
fn both_handovers(keys: &[String], selects: &[(&Plan, Vec<Vec<Value>>)]) -> (Vec<Doc>, Vec<Doc>) {
    let mut cloned = Merger::new(keys);
    let mut moved = Merger::new(keys);
    for (plan, rows) in selects {
        cloned.add(plan, rows.clone());
        let owned = rows.clone();
        moved.add(plan, owned);
    }
    (cloned.finish(), moved.finish())
}

/// Renders docs the way a target sees them, so the assertion is about bytes and not about
/// `Value`'s `PartialEq` treating two number spellings as one.
fn rendered(docs: &[Doc]) -> Vec<String> {
    docs.iter()
        .map(|d| {
            format!(
                "{}|{}|{}",
                d.key,
                d.delete,
                serde_json::to_string(&d.doc).expect("renders")
            )
        })
        .collect()
}

#[test]
fn the_two_handovers_render_the_same_docs() {
    let ks = keys(&["1", "2", "3", "4"]);
    let (cloned, moved) = both_handovers(&ks, &[(&attrs_plan(), attrs_rows())]);
    assert_eq!(rendered(&cloned), rendered(&moved), "one select");
    // Pinned, so the two sides cannot drift together: the keys the select answered carry their
    // values (a NULL column is set to null, which is what the merger has always done), and the
    // key it never answered keeps an empty doc.
    assert_eq!(
        rendered(&cloned),
        vec![
            r#"1|false|{"sku":"A-1","price":{"final":9.5},"meta":{"a":1}}"#,
            r#"2|false|{"sku":"B-2","price":{"final":null},"meta":"not json"}"#,
            r#"3|false|{"sku":null,"price":{"final":null},"meta":null}"#,
            r#"4|false|{}"#,
        ]
    );
    // and the same for a chunk several selects answer, which is what a rule really has
    let (cloned, moved) = both_handovers(
        &ks,
        &[(&attrs_plan(), attrs_rows()), (&ops_plan(), ops_rows())],
    );
    assert_eq!(rendered(&cloned), rendered(&moved), "two selects");
    // the JSON column was parsed, not passed through as text, in both
    assert_eq!(cloned[0].doc["meta"], json!({"a": 1}));
    assert_eq!(moved[0].doc["meta"], json!({"a": 1}));
    // and a text that is not JSON stayed text
    assert_eq!(cloned[1].doc["meta"], json!("not json"));
    assert_eq!(moved[1].doc["meta"], json!("not json"));
}

#[test]
fn the_tombstone_and_the_unanswered_key_survive_both_handovers() {
    let ks = keys(&["1", "2", "3", "4"]);
    let (cloned, moved) = both_handovers(
        &ks,
        &[(&attrs_plan(), attrs_rows()), (&ops_plan(), ops_rows())],
    );
    // `2` is the delete, `4` no select answered: both verdicts are the merger's, not the
    // handover's, and must not move when the copy does. A select that carries `_op` makes a key
    // no select answered a delete as well (`any_op && !seen`) — that verdict is the reason the
    // handover may not change what the merger saw.
    assert_eq!(
        cloned.iter().map(|d| d.delete).collect::<Vec<_>>(),
        vec![false, true, false, true]
    );
    assert_eq!(
        moved.iter().map(|d| d.delete).collect::<Vec<_>>(),
        vec![false, true, false, true]
    );
    // the member group of the second select reached the docs it belongs to: `tags[].tag` is a
    // member with an inner path, so each element is an object, not a bare string
    assert_eq!(cloned[0].doc["tags"], json!([{"tag": "new"}]));
    assert_eq!(moved[0].doc["tags"], json!([{"tag": "new"}]));
    assert_eq!(cloned[1].doc["tags"], json!([{"tag": "old"}]));
    assert_eq!(moved[1].doc["tags"], json!([{"tag": "old"}]));
    // the key no select answered keeps an empty doc and is still in the chunk, in order
    assert_eq!(cloned[3].doc, json!({}));
    assert_eq!(moved[3].doc, json!({}));
    assert_eq!(
        cloned.iter().map(|d| d.key.as_str()).collect::<Vec<_>>(),
        moved.iter().map(|d| d.key.as_str()).collect::<Vec<_>>()
    );
    // the same keys out in the same order, which is what the caller chunks and sends
    assert_eq!(rendered(&cloned), rendered(&moved));
}

/// A select that answers no key at all (the query returned nothing for this chunk): the keys
/// keep the docs they had from the other selects, and none of them becomes a blank one.
#[test]
fn a_select_that_answers_nothing_changes_nothing() {
    let ks = keys(&["1", "2"]);
    let (cloned, moved) = both_handovers(
        &ks,
        &[(&attrs_plan(), attrs_rows()), (&ops_plan(), Vec::new())],
    );
    assert_eq!(rendered(&cloned), rendered(&moved));
    assert_eq!(cloned[0].doc["sku"], json!("A-1"));
    assert!(!cloned[0].delete);
}

/// The same chunk merged twice (a rule whose selects both carry `_key` for one key) still
/// agrees with itself whichever way the rows arrived.
#[test]
fn a_key_two_selects_answer_agrees_across_both_handovers() {
    let ks = keys(&["1"]);
    let (cloned, moved) = both_handovers(
        &ks,
        &[
            (&attrs_plan(), attrs_rows()),
            (&attrs_plan(), attrs_rows()),
            (&ops_plan(), ops_rows()),
        ],
    );
    assert_eq!(rendered(&cloned), rendered(&moved));
}

/// Finding 9, counted: handing the merger its rows used to cost a deep copy of the whole
/// rowset — every `Value`, every string in it — before the merger cloned out of it what it
/// keeps. Now the caller gives the rowset up. Same rows in, same docs out, one copy less.
#[test]
fn feeding_the_merger_no_longer_copies_the_rowset() {
    let ks = keys(&["1", "2", "3", "4"]);
    let plan = attrs_plan();
    // Both paths start from the same rows, built outside the counted region so their input is
    // identical and its cost is charged to neither.
    let for_cloned = attrs_rows();
    let for_moved = attrs_rows();
    let (a, n_cloned) = alloc_measure::count(|| {
        let mut m = Merger::new(&ks);
        m.add(&plan, for_cloned.clone());
        m.finish()
    });
    let (b, n_moved) = alloc_measure::count(|| {
        let mut m = Merger::new(&ks);
        m.add(&plan, for_moved);
        m.finish()
    });
    assert_eq!(rendered(&a), rendered(&b), "the same docs either way");
    eprintln!(
        "one chunk of {} keys through the merger: cloned in {n_cloned} allocations, \
         moved in {n_moved}",
        ks.len()
    );
    // the rowset is three rows of four values, two of them strings: the copy is worth several
    // allocations, and none of them is the merger's work
    assert!(
        n_cloned >= n_moved + 4,
        "the rowset was still copied: {n_cloned} against {n_moved}"
    );
}
