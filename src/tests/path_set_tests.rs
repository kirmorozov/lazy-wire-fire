//! `Path::set` on the arms an object-only document never reaches: an index that creates, pads
//! and replaces, a `[]` step that appends and descends, the root path that replaces the whole
//! document, and `ensure_array` where the value in the way is not an array.

use super::*;
use serde_json::json;

fn p(s: &str) -> Path {
    Path::parse(s).unwrap()
}

/// The empty path is the document itself: whatever was there, including a non-object, is
/// replaced — not merged into.
#[test]
fn a_root_path_replaces_the_whole_document() {
    let mut d = json!({"keep": 1});
    p("").set(&mut d, json!({"a": 1}));
    assert_eq!(d, json!({"a": 1}));

    let mut d = json!([1, 2, 3]);
    p("").set(&mut d, json!({"a": 1}));
    assert_eq!(d, json!({"a": 1}));

    let mut d = json!("text");
    p("").set(&mut d, Value::Null);
    assert_eq!(d, Value::Null);

    // The same path built by hand from no steps.
    let mut d = json!({"a": 1});
    Path(vec![]).set(&mut d, json!([1, 2]));
    assert_eq!(d, json!([1, 2]));
}

/// An index creates the array, pads the hole with `null`, replaces what is in the way (a
/// scalar, an object, a string), and replaces the element it is given.
#[test]
fn an_index_creates_pads_and_replaces() {
    let mut d = json!({});
    p("a[2]").set(&mut d, json!("x"));
    assert_eq!(d, json!({"a": [null, null, "x"]}));
    // Inside the array now: an element is replaced, the others stay.
    p("a[0]").set(&mut d, json!(1));
    assert_eq!(d, json!({"a": [1, null, "x"]}));
    // Growing keeps what is there and leaves the new hole null.
    p("a[4]").set(&mut d, json!(true));
    assert_eq!(d, json!({"a": [1, null, "x", null, true]}));

    // Whatever is in the way of the array is replaced by it.
    for (src, want) in [
        (json!({"a": 5}), json!({"a": [null, 9]})),
        (json!({"a": "text"}), json!({"a": [null, 9]})),
        (json!({"a": {"b": 1}}), json!({"a": [null, 9]})),
        (json!({"a": null}), json!({"a": [null, 9]})),
        (json!({"a": [1, 2, 3]}), json!({"a": [1, 9, 3]})),
    ] {
        let mut d = src;
        p("a[1]").set(&mut d, json!(9));
        assert_eq!(d, want);
    }

    // A top-level index with nothing above it.
    let mut d = json!({});
    p("[0]").set(&mut d, json!("first"));
    assert_eq!(d, json!(["first"]));
    // A scalar where the array has to be, two levels down.
    let mut d = json!({"a": {"b": "text"}});
    p("a.b[0]").set(&mut d, json!(7));
    assert_eq!(d, json!({"a": {"b": [7]}}));
}

/// An index that is not the last step descends through the hole it just made, so one column can
/// fill one field of the element.
#[test]
fn an_index_in_the_middle_descends_through_the_hole() {
    let mut d = json!({});
    p("a[1].b[0].c").set(&mut d, json!(7));
    assert_eq!(d, json!({"a": [null, {"b": [{"c": 7}]}]}));
    // Another field of the same element: the array and the element are already there.
    p("a[1].b[0].d").set(&mut d, json!(8));
    assert_eq!(d, json!({"a": [null, {"b": [{"c": 7, "d": 8}]}]}));
    // A further element, and the one between them stays null.
    p("a[3].b[0].c").set(&mut d, json!(1));
    assert_eq!(
        d,
        json!({"a": [null, {"b": [{"c": 7, "d": 8}]}, null, {"b": [{"c": 1}]}]})
    );
    // An element that holds an element.
    let mut d = json!({});
    p("a[0][1]").set(&mut d, json!("deep"));
    assert_eq!(d, json!({"a": [[null, "deep"]]}));
}

/// A `[]` step appends: an element per call, never merged with the previous one — that is what
/// lets one select row contribute one element of a collection.
#[test]
fn an_each_step_appends_and_descends() {
    let mut d = json!({});
    p("ids[]").set(&mut d, json!(1));
    p("ids[]").set(&mut d, json!(2));
    assert_eq!(d, json!({"ids": [1, 2]}));

    // `[]` in the middle: a fresh element, then the rest of the path inside it. Each call
    // opens its own element, which is how `items[].sku` + `items[].qty` from separate rows
    // become separate elements.
    let mut d = json!({});
    p("items[].sku").set(&mut d, json!("c1"));
    p("items[].sku").set(&mut d, json!("c2"));
    p("items[].qty").set(&mut d, json!(3));
    assert_eq!(
        d,
        json!({"items": [{"sku": "c1"}, {"sku": "c2"}, {"qty": 3}]})
    );
    // A scalar in the way of the collection is replaced by it.
    let mut d = json!({"a": 5});
    p("a[].b").set(&mut d, json!(1));
    assert_eq!(d, json!({"a": [{"b": 1}]}));

    // `[]` then an index: the new element becomes the array the index writes into.
    let mut d = json!({});
    p("a[][0]").set(&mut d, json!(7));
    assert_eq!(d, json!({"a": [[7]]}));
    // Two `[]` steps: a new element holding a new element.
    let mut d = json!({});
    p("a[][]").set(&mut d, json!(1));
    p("a[][]").set(&mut d, json!(2));
    assert_eq!(d, json!({"a": [[1], [2]]}));
    // `[*]` is the same step as `[]`.
    let mut d = json!({});
    p("a[*]").set(&mut d, json!(1));
    assert_eq!(d, json!({"a": [1]}));
}

/// `ensure_array` makes the place an array when it is not one — and leaves a real array alone,
/// contents and all, including through an index.
#[test]
fn ensure_array_leaves_an_array_and_replaces_anything_else() {
    // Missing: the empty array appears.
    let mut d = json!({});
    p("a").ensure_array(&mut d);
    assert_eq!(d, json!({"a": []}));
    // Present, at the top and nested: untouched.
    let mut d = json!({"a": [1], "b": {"c": [2]}});
    p("a").ensure_array(&mut d);
    p("b.c").ensure_array(&mut d);
    assert_eq!(d, json!({"a": [1], "b": {"c": [2]}}));
    // Through an index: an element that is already an array stays.
    let mut d = json!({"a": [[1], 2]});
    p("a[0]").ensure_array(&mut d);
    assert_eq!(d, json!({"a": [[1], 2]}), "the element at 0 is an array");
    // And one that is not becomes the empty array, the other elements staying.
    p("a[1]").ensure_array(&mut d);
    assert_eq!(d, json!({"a": [[1], []]}));

    // Whatever else is in the way is replaced.
    for src in [
        json!({"a": 5}),
        json!({"a": "text"}),
        json!({"a": {"b": 1}}),
        json!({"a": null}),
    ] {
        let mut d = src.clone();
        p("a").ensure_array(&mut d);
        assert_eq!(d, json!({"a": []}), "{src} was in the way");
    }
    // A document that is not an object at all becomes one.
    let mut d = json!(5);
    p("a").ensure_array(&mut d);
    assert_eq!(d, json!({"a": []}));
    // An index below a missing array: the array is created padded, the element at the index
    // becomes the empty array.
    let mut d = json!({});
    p("a[1]").ensure_array(&mut d);
    assert_eq!(d, json!({"a": [null, []]}));
    // A `[]` step in the middle: the whole path is ensured, so the step appends an element
    // (a fresh object) and the key after it becomes the empty array inside that element.
    let mut d = json!({});
    p("a[].b").ensure_array(&mut d);
    assert_eq!(d, json!({"a": [{"b": []}]}));
}

/// The two paths that write into the same place: any two array steps count as the same place,
/// whatever their indexes, because the elements a `[]` step appends cannot be predicted.
#[test]
fn paths_that_write_into_the_same_place() {
    assert!(p("a[]").overlaps(&p("a[1].x")));
    assert!(p("a[2]").overlaps(&p("a[]")));
    assert!(p("a[1]").overlaps(&p("a[2]")));
    assert!(p("a[].b").overlaps(&p("a[].b")));
    assert!(!p("a[].b").overlaps(&p("a[].c")));
    assert!(!p("a.b").overlaps(&p("a[0]")));
    assert!(!p("a").overlaps(&p("b")));
    // An empty path is a prefix of everything.
    assert!(p("").overlaps(&p("a.b[0]")));
    // And the pieces of a path are what `docs` walks: prefix then suffix.
    let full = p("items[2].sku");
    assert_eq!(full.prefix(2), p("items[2]"));
    assert_eq!(full.suffix(2), p("sku"));
    assert_eq!(full.each_at(), None);
    assert_eq!(p("ids[]").each_at(), Some(1));
    assert!(p("").is_root());
    assert!(!p("a").is_root());
    assert_eq!(p("a.b[2].c[]").to_string(), "a.b[2].c[]");
    assert_eq!(p("[0]").to_string(), "[0]");
}
