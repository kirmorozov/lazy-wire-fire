//! Sets of keys, compact for the usual shapes: a key that is an integer (`42`) or a prefix and
//! an integer (`item_42`) lives in an [`IntSet`] of its prefix (`src/intset.rs`, shared with
//! lazyview); any other key in an insertion-ordered set.
//!
//! Costs of the default backend (a hash set per prefix), release build, measured on the earlier
//! hash-set version of this type: about 15-19 bytes per number, a million consecutive ids about
//! 19 MB, 10M about 189 MB. A drain adds the sorted cache of `IntSet` (8 bytes per key) until
//! it is empty. With `--features roaring` the integer sets are roaring bitmaps instead: a
//! million consecutive ids under 300 KB and 10M dense ids about 1.3 MB, but sparse ids cost
//! about 212 bytes per key and drain about 240 times slower at 1M.
//!
//! Only canonical integers go into the integer sets (no leading zeros, up to `u64::MAX`), so a
//! key reads back exactly as written. Iteration: prefixes in order, numbers ascending, then the
//! other keys in insertion order.

use std::collections::BTreeMap;

use foldhash::fast::RandomState;
use indexmap::IndexSet;

use crate::intset::IntSet;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeySet {
    nums: BTreeMap<String, IntSet>,
    other: IndexSet<String, RandomState>,
}

/// `(prefix, number)` when `k` ends in a canonical integer.
fn split(k: &str) -> Option<(&str, u64)> {
    let digits = k.bytes().rev().take_while(u8::is_ascii_digit).count();
    if digits == 0 || digits > 20 {
        return None;
    }
    let (prefix, num) = k.split_at(k.len() - digits);
    if num.len() > 1 && num.starts_with('0') {
        return None;
    }
    num.parse().ok().map(|n| (prefix, n))
}

impl KeySet {
    pub fn new() -> KeySet {
        KeySet::default()
    }

    /// Adds `k`; whether it was new.
    pub fn insert(&mut self, k: &str) -> bool {
        match split(k) {
            Some((p, n)) => match self.nums.get_mut(p) {
                Some(b) => b.insert(n),
                None => {
                    let mut b = IntSet::new();
                    b.insert(n);
                    self.nums.insert(p.to_string(), b);
                    true
                }
            },
            None => self.other.insert(k.to_string()),
        }
    }

    pub fn contains(&self, k: &str) -> bool {
        match split(k) {
            Some((p, n)) => self.nums.get(p).is_some_and(|b| b.contains(n)),
            None => self.other.contains(k),
        }
    }

    pub fn remove(&mut self, k: &str) -> bool {
        match split(k) {
            Some((p, n)) => {
                let Some(b) = self.nums.get_mut(p) else {
                    return false;
                };
                let hit = b.remove(n);
                if b.is_empty() {
                    self.nums.remove(p);
                }
                hit
            }
            None => self.other.shift_remove(k),
        }
    }

    pub fn len(&self) -> u64 {
        self.nums.values().map(IntSet::len).sum::<u64>() + self.other.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.nums.is_empty() && self.other.is_empty()
    }

    /// Adds every key of `other`.
    pub fn extend(&mut self, other: KeySet) {
        for (p, b) in other.nums {
            match self.nums.get_mut(&p) {
                Some(mine) => mine.union_with(b),
                None => {
                    self.nums.insert(p, b);
                }
            }
        }
        self.other.extend(other.other);
    }

    pub fn iter(&self) -> impl Iterator<Item = String> + '_ {
        self.nums
            .iter()
            .flat_map(|(p, b)| {
                b.to_sorted_vec()
                    .into_iter()
                    .map(move |n| format!("{p}{n}"))
            })
            .chain(self.other.iter().cloned())
    }

    /// Removes and returns up to `n` keys, in iteration order.
    pub fn take(&mut self, n: usize) -> Vec<String> {
        let mut out = Vec::with_capacity(n.min(self.len() as usize));
        let mut emptied = Vec::new();
        for (p, b) in self.nums.iter_mut() {
            if out.len() >= n {
                break;
            }
            for v in b.pop_smallest(n - out.len()) {
                out.push(format!("{p}{v}"));
            }
            if b.is_empty() {
                emptied.push(p.clone());
            }
        }
        for p in emptied {
            self.nums.remove(&p);
        }
        if out.len() < n {
            let m = (n - out.len()).min(self.other.len());
            out.extend(self.other.drain(..m));
        }
        out
    }

    pub fn into_vec(mut self) -> Vec<String> {
        let n = self.len() as usize;
        self.take(n)
    }
}

impl FromIterator<String> for KeySet {
    fn from_iter<I: IntoIterator<Item = String>>(it: I) -> KeySet {
        let mut s = KeySet::new();
        for k in it {
            s.insert(&k);
        }
        s
    }
}

impl<'a> FromIterator<&'a str> for KeySet {
    fn from_iter<I: IntoIterator<Item = &'a str>>(it: I) -> KeySet {
        let mut s = KeySet::new();
        for k in it {
            s.insert(k);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        ];
        let s: KeySet = keys.iter().copied().collect();
        assert_eq!(s.len(), keys.len() as u64);
        for k in keys {
            assert!(s.contains(k), "{k}");
        }
        assert!(!s.contains("43") && !s.contains("item_8") && !s.contains("7"));
        let mut got: Vec<String> = s.iter().collect();
        got.sort();
        let mut want: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        want.sort();
        assert_eq!(got, want, "every key reads back exactly");
        // Leading zeros and too large numbers are kept as text.
        assert_eq!(split("007"), None);
        assert_eq!(split("18446744073709551616"), None);
        assert_eq!(split("item_42"), Some(("item_", 42)));
    }

    #[test]
    fn dedup_union_take() {
        let mut a = KeySet::new();
        assert!(a.insert("p_1"));
        assert!(!a.insert("p_1"));
        a.insert("3");
        a.insert("1");
        a.insert("z");
        let mut b = KeySet::new();
        b.insert("2");
        b.insert("p_1");
        b.insert("y");
        a.extend(b);
        assert_eq!(a.len(), 6);
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec!["1", "2", "3", "p_1", "z", "y"]
        );
        assert_eq!(a.take(2), vec!["1", "2"]);
        assert_eq!(a.take(3), vec!["3", "p_1", "z"]);
        assert_eq!(a.into_vec(), vec!["y"]);
        let mut c: KeySet = (1..=10).map(|i| i.to_string()).collect();
        assert!(c.remove("5") && !c.remove("5"));
        assert_eq!(c.len(), 9);
    }

    #[test]
    fn a_million_dense_ids_drain_in_order() {
        let mut s: KeySet = (1..=1_000_000u64).map(|i| i.to_string()).collect();
        assert_eq!(s.len(), 1_000_000);
        assert!(s.contains("1") && s.contains("1000000") && !s.contains("1000001"));
        assert_eq!(s.take(3), vec!["1", "2", "3"]);
        assert!(s.insert("0"));
        assert_eq!(s.take(2), vec!["0", "4"]);
        assert_eq!(s.len(), 999_996);
    }
}
