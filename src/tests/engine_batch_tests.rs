use super::*;
use crate::action::{book_items, Fired};
use crate::target::Item;

fn keys(ks: &[&str]) -> Vec<String> {
    ks.iter().map(|s| s.to_string()).collect()
}

fn one(k: &str) -> Vec<(String, String)> {
    vec![(k.to_string(), "refused".to_string())]
}

/// A mixed batch: confirmed items are done, only the refused one comes back, and the keys
/// that were confirmed are not in what the next flush sends.
#[test]
fn mixed_batch_retries_only_the_refused_key() {
    let ks = keys(&["a", "b", "c", "d"]);
    let mut f = Fired::default();
    let waiting = book_items(
        &ks,
        vec![
            Item::Ok,
            Item::Rejected("bad entry".into()),
            Item::Ok,
            Item::Ok,
        ],
        &mut f,
    );
    assert!(!waiting);
    assert_eq!(f.keys_done, 3);
    assert_eq!(f.rejected, vec![("b".to_string(), "bad entry".to_string())]);
    assert!(f.failed_keys.is_empty());
    let mut strikes = HashMap::new();
    let (retry, dropped) = judge_strikes(&mut strikes, f.rejected.clone(), f.keys_done > 0);
    assert!(dropped.is_empty());
    let again: Vec<String> = f.failed_keys.iter().cloned().chain(retry).collect();
    assert_eq!(again, keys(&["b"]));
    assert_eq!(strikes.get("b"), Some(&1));
}

/// A transient item stops the batches not yet sent (the caller does that) and its key
/// waits with the refused ones; the confirmed key is not in the retry set.
#[test]
fn transient_item_waits_with_the_refused_ones() {
    let ks = keys(&["a", "b", "c"]);
    let mut f = Fired::default();
    let waiting = book_items(
        &ks,
        vec![
            Item::Transient("throttled".into()),
            Item::Ok,
            Item::Rejected("bad".into()),
        ],
        &mut f,
    );
    assert!(waiting);
    assert!(f.target_error);
    assert_eq!(f.keys_done, 1);
    assert_eq!(f.failed_keys, keys(&["a"]));
    assert_eq!(f.rejected, vec![("c".to_string(), "bad".to_string())]);
    let mut strikes = HashMap::new();
    let (retry, _) = judge_strikes(&mut strikes, f.rejected.clone(), f.keys_done > 0);
    let again: Vec<String> = f.failed_keys.iter().cloned().chain(retry).collect();
    assert_eq!(again, keys(&["a", "c"]));
}

#[test]
fn all_refused_is_not_a_key_fault() {
    // Nothing went through: the template or the target is at fault, so no strike.
    let mut strikes = HashMap::new();
    let (retry, dropped) = judge_strikes(&mut strikes, one("x"), false);
    assert_eq!(retry, keys(&["x"]));
    assert!(dropped.is_empty());
    assert!(strikes.is_empty());
}

/// A key refused while others went through is dropped on the fifth strike.
#[test]
fn fifth_strike_drops_the_key() {
    let mut strikes = HashMap::new();
    for n in 1..STRIKES {
        let (retry, dropped) = judge_strikes(&mut strikes, one("bad"), true);
        assert_eq!(retry, keys(&["bad"]), "strike {n}");
        assert!(dropped.is_empty(), "strike {n}");
    }
    let (retry, dropped) = judge_strikes(&mut strikes, one("bad"), true);
    assert!(retry.is_empty());
    assert_eq!(dropped.len(), 1);
    assert!(!strikes.contains_key("bad"));
}

/// Once a key has a strike, a single-key refusal counts (it is not "everything refused").
#[test]
fn struck_key_counts_even_alone() {
    let mut strikes = HashMap::new();
    judge_strikes(&mut strikes, one("k"), true);
    judge_strikes(&mut strikes, one("k"), false);
    assert_eq!(strikes.get("k"), Some(&2));
}
