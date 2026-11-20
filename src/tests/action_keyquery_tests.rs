//! `KeyQuery::id` (hot-path audit finding 5): byte-identical to the old deep-clone
//! format, and no longer deep-clones the parameters.

use super::KeyQuery;
use crate::alloc_measure;
use serde_json::{json, Value};

/// The implementation that shipped before this fix, kept verbatim as the reference for
/// byte-equality and for the before/after allocation count: it deep-clones every param
/// into a `Value::Array` and formats it (`Value`'s `Display` is `serde_json`'s serializer).
fn old_id(q: &KeyQuery) -> String {
    format!("{}:{}:{}", q.trigger, q.sql, Value::Array(q.params.clone()))
}

fn q(trigger: u64, sql: &str, params: Vec<Value>) -> KeyQuery {
    KeyQuery {
        trigger,
        sql: sql.into(),
        params,
    }
}

#[test]
fn id_is_byte_identical_to_the_old_format_for_every_params_shape() {
    let cases: Vec<Vec<Value>> = vec![
        vec![],                                      // empty params
        vec![Value::Null],                           // Null
        vec![Value::Bool(true), Value::Bool(false)], // Bool
        vec![
            Value::from(0i64),
            Value::from(i64::MIN),
            Value::from(i64::MAX),
        ], // Int
        vec![Value::from(1u64), Value::from(u64::MAX)], // UInt
        vec![
            Value::from(1.5),
            Value::from(-0.125),
            Value::from(1.0),
            Value::from(1e300),
        ], // Float
        vec![
            Value::String(String::new()),
            Value::String("a\"b\\c\u{1}\u{7f}dé💯\n\t".into()),
        ], // String: empty, escapes, control chars, unicode
        vec![json!({"nested": {"deep": [1, "two", null, true]}, "": 0.0})], // nested Object
        vec![json!([[[{"x": []}]], 3.25, [], {}])],  // nested Array
        vec![
            Value::Null,
            json!({"k": [1.0, {"m": "n"}]}),
            Value::from(7u64),
            Value::String("mixed".into()),
            Value::Bool(true),
        ], // every shape in one call
    ];
    for (i, params) in cases.iter().enumerate() {
        let query = q(
            i as u64,
            "SELECT k FROM orders WHERE a = ? AND b = ?",
            params.clone(),
        );
        assert_eq!(query.id(), old_id(&query), "params shape {i}");
    }
    // The id's other two fields stay faithful too: ':' and quotes in the sql, max trigger.
    let query = q(
        u64::MAX,
        "SELECT ':' AS \"?\" FROM t WHERE x = ?",
        vec![json!({"a": 1})],
    );
    assert_eq!(query.id(), old_id(&query), "sql holding the separators");
}

#[test]
fn id_no_longer_deep_clones_the_params() {
    let query = q(
        7,
        "SELECT k FROM orders WHERE a = ? AND b = ? AND c = ?",
        vec![
            Value::from(7712i64),
            Value::String("order-7712-customer-acme".into()),
            Value::String("notes with \"quotes\", \\ backslash and some padding padding".into()),
            json!({"a": [1, 2, {"b": "a deeply nested string value"}], "c": true}),
            Value::Null,
        ],
    );
    // Warm both paths up, then count one computation of each on this thread only.
    let _ = old_id(&query);
    let _ = query.id();
    let (a, n_old) = alloc_measure::count(|| old_id(&query));
    let (b, n_new) = alloc_measure::count(|| query.id());
    assert_eq!(a, b, "the same bytes");
    eprintln!("allocations for one id: old {n_old}, new {n_new}");
    // The difference is the deep clone of the params (the Vec, both Strings, the
    // nested map, array and String): the new path only serializes by reference.
    assert!(
        n_old >= n_new + 4,
        "the old id cost {n_old} allocations, the new one {n_new}: no deep clone left",
    );
}

#[test]
fn counting_the_allocator_counts_allocations_on_this_thread() {
    let (value, n) = alloc_measure::count(Vec::<u8>::new);
    assert!(value.is_empty());
    assert_eq!(n, 0, "an allocation-free call counts zero");
    let (_, n) = alloc_measure::count(|| Vec::<u8>::with_capacity(64));
    assert_eq!(n, 1, "one Vec::with_capacity counts one");
}

/// The commit-fence memo answers on a poisoned lock. A probe is decided before the lock and stored
/// by one `insert`, and an over-full memo is dropped whole, so what a task that died holding it can
/// leave is one connection re-probed (`fence::probe` is idempotent) — never a second panic on the
/// job task, which is where the fence runs.
#[test]
fn the_probe_memo_answers_on_a_poisoned_lock() {
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    let env = super::Env::new(
        pool.clone(),
        pool,
        crate::source::Flavor::MySql,
        std::time::Duration::ZERO,
        1,
    );
    crate::poison::poison(&env.probes);
    assert_eq!(
        super::probe_cached(&super::probe_memo(&env), 7),
        None,
        "a poisoned memo still reads as empty"
    );
    super::probe_remember(&mut super::probe_memo(&env), 7, super::Probe::Ordered);
    super::probe_remember(&mut super::probe_memo(&env), 8, super::Probe::Unordered);
    assert_eq!(
        super::probe_cached(&super::probe_memo(&env), 7),
        Some(super::Probe::Ordered),
        "and still remembers what it was told"
    );
    assert_eq!(
        super::probe_cached(&super::probe_memo(&env), 8),
        Some(super::Probe::Unordered),
        "the second entry did not cost the first"
    );
}
