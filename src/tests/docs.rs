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
