//! Sets of keys, compact for the usual shapes: a key that is an integer (`42`) or a prefix and
//! an integer (`cat_p_42`) lives in a 64-bit roaring bitmap of its prefix, like lazyview's unit
//! sets; any other key in an ordered set. A million consecutive ids take under 300 KB, not
//! 60 MB of strings.
//!
//! Only canonical integers go into bitmaps (no leading zeros, up to `u64::MAX`), so a key reads
//! back exactly as written. Iteration: prefixes in order, numbers ascending, then the other keys
//! in insertion order.

use std::collections::BTreeMap;

use indexmap::IndexSet;
use roaring::RoaringTreemap;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeySet {
    nums: BTreeMap<String, RoaringTreemap>,
    other: IndexSet<String>,
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
                    let mut b = RoaringTreemap::new();
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
        self.nums.values().map(RoaringTreemap::len).sum::<u64>() + self.other.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.nums.is_empty() && self.other.is_empty()
    }

    /// Adds every key of `other`.
    pub fn extend(&mut self, other: KeySet) {
        for (p, b) in other.nums {
            match self.nums.get_mut(&p) {
                Some(mine) => *mine |= b,
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
            .flat_map(|(p, b)| b.iter().map(move |n| format!("{p}{n}")))
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
            let want = n - out.len();
            let mut last = None;
            for v in b.iter().take(want) {
                out.push(format!("{p}{v}"));
                last = Some(v);
            }
            if let Some(last) = last {
                b.remove_range(..=last);
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
            "cat_p_42",
            "cat_p_7",
            "cat_c_p_7",
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
        assert!(!s.contains("43") && !s.contains("cat_p_8") && !s.contains("7"));
        let mut got: Vec<String> = s.iter().collect();
        got.sort();
        let mut want: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        want.sort();
        assert_eq!(got, want, "every key reads back exactly");
        // Leading zeros and too large numbers are kept as text.
        assert_eq!(split("007"), None);
        assert_eq!(split("18446744073709551616"), None);
        assert_eq!(split("cat_p_42"), Some(("cat_p_", 42)));
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
    fn compact() {
        let s: KeySet = (1..=1_000_000u64).map(|i| i.to_string()).collect();
        assert_eq!(s.len(), 1_000_000);
        assert!(
            s.nums[""].serialized_size() < 300_000,
            "{}",
            s.nums[""].serialized_size()
        );
    }
}
