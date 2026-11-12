//! The engine's text keys on the dedup core's ids: a key that ends in a canonical integer
//! (`42`, `item_42`) becomes the integer `42` in the space of its prefix, anything else is a
//! one-part bytes tuple in the text space. The container per space is the core's
//! [`IdSet`]/[`Key`]; this module only maps text to spaces and back (iteration: prefixes in
//! order, numbers ascending, then the text keys), and holds the host's per-owner sets
//! ([`IdSets`]: the marks of a transaction, the keys a reset batch fires).
//!
//! Costs follow the core: an integer mark is one hash insert into the space's
//! [`crate::intset::IntSet`] (shared with lazyview, roaring behind the `roaring` feature) and
//! allocates nothing; a text key is a tuple (one `Vec` for its bytes, one box for its parts).
//! A rule has [`KeySpaces::SLOTS`] prefix slots; keys of any further prefix live in the text
//! space as tuples: correct, just not compact.

use std::collections::BTreeMap;

use crate::datum::Datum;
use crate::dedup::buffer::IdSet;
use crate::dedup::key::{Key, Op, SpaceId};

/// `(prefix, number)` when `k` ends in a canonical integer (no leading zeros, up to
/// `u64::MAX`), so a key reads back exactly as written.
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

/// A key with no integer tail: `Key::from_text` keeps it as a one-part bytes tuple and
/// `to_text` gives the text back exactly, so the core's rule and [`split`] agree: a key
/// `split` does not take is a tuple, and round-trips (`from_text(k).to_text() == k`).
fn text_key(k: &str) -> Key {
    Key::from_text(k)
}

/// The `{{op}}` text of a row change, from the core's op.
pub fn op_text(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
    }
}

/// The key spaces of one rule: space 0 is the text space, spaces `1..=` [`SLOTS`] are the
/// prefix slots, assigned in order as prefixes appear. The rule's core buffer is created for
/// [`KeySpaces::SPACES`] spaces once; this map owns the prefix of each slot.
#[derive(Debug, Clone)]
pub struct KeySpaces {
    slots: BTreeMap<String, SpaceId>,
    /// The next free slot; space 0 is the text space, so slots start at 1.
    next: u16,
}

impl Default for KeySpaces {
    fn default() -> Self {
        Self {
            slots: BTreeMap::new(),
            next: 1,
        }
    }
}

impl KeySpaces {
    /// Prefix slots per rule; a key of any further prefix goes to the text space as a tuple.
    pub const SLOTS: u16 = 32;
    /// The spaces of one rule's core buffer: the text space plus the prefix slots.
    pub const SPACES: usize = Self::SLOTS as usize + 1;

    pub fn new() -> KeySpaces {
        KeySpaces::default()
    }

    /// The space of the text keys (those [`split`] does not take).
    pub fn text(&self) -> SpaceId {
        SpaceId(0)
    }

    /// Where key `k` lives: its slot as an integer, or the text space as a tuple.
    pub fn locate(&mut self, k: &str) -> (SpaceId, Key) {
        if let Some((prefix, n)) = split(k) {
            if let Some(&s) = self.slots.get(prefix) {
                return (s, Key::Int(n));
            }
            if self.next <= Self::SLOTS {
                let s = SpaceId(self.next);
                self.next += 1;
                self.slots.insert(prefix.to_string(), s);
                return (s, Key::Int(n));
            }
        }
        (self.text(), text_key(k))
    }

    /// Where key `k` lives, assigning nothing: a key marked before is found, an unknown
    /// prefix with an integer tail reports `None` (its slot does not exist yet).
    pub fn peek(&self, k: &str) -> Option<(SpaceId, Key)> {
        let (prefix, n) = split(k)?;
        self.slots.get(prefix).map(|s| (*s, Key::Int(n)))
    }

    /// The ids of these spaces back to text: prefixes in order, numbers ascending, then the
    /// text keys.
    pub fn render(&self, sets: &BTreeMap<SpaceId, IdSet>) -> Vec<String> {
        let mut out = Vec::new();
        for (prefix, s) in &self.slots {
            if let Some(set) = sets.get(s) {
                render_into(prefix, set, &mut out);
            }
        }
        if let Some(set) = sets.get(&self.text()) {
            render_into("", set, &mut out);
        }
        out
    }

    /// The prefix of a slot (`""` for the text space and for unknown spaces, whose keys are
    /// tuples rendered as their text).
    fn prefix(&self, s: SpaceId) -> &str {
        if s == self.text() {
            return "";
        }
        self.slots
            .iter()
            .find(|(_, v)| **v == s)
            .map_or("", |(p, _)| p.as_str())
    }
}

fn render_into(prefix: &str, set: &IdSet, out: &mut Vec<String>) {
    out.reserve(set.len());
    for n in set.ints.to_sorted_vec() {
        out.push(format!("{prefix}{n}"));
    }
    for t in set.keys.iter() {
        // Text keys enter through `text_key`, so the tuple is one bytes part; anything
        // else (only the core can produce it) renders as the core renders it.
        match &**t {
            [Datum::Bytes(b)] => out.push(String::from_utf8_lossy(b).into_owned()),
            _ => {
                let mut s = String::new();
                Key::Tuple(t.clone()).to_text(&mut s);
                out.push(s);
            }
        }
    }
}

/// The keys of one owner, one [`IdSet`] per space that holds any: the marks of a transaction,
/// the keys a flush sends back for a retry. Text enters through [`KeySpaces::locate`] and
/// leaves through [`KeySpaces::render`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IdSets {
    sets: BTreeMap<SpaceId, IdSet>,
}

impl IdSets {
    pub fn insert(&mut self, s: SpaceId, k: Key) -> bool {
        self.sets.entry(s).or_default().insert(k)
    }

    /// The sets, by space (for rendering through a [`KeySpaces`]).
    pub fn as_map(&self) -> &BTreeMap<SpaceId, IdSet> {
        &self.sets
    }

    pub fn contains(&self, s: SpaceId, k: &Key) -> bool {
        self.sets.get(&s).is_some_and(|set| set.contains(k))
    }

    /// Whether `k` is one of the keys, located by `ks` (`peek`: the keys of a set were
    /// marked by a rule, so their slots exist).
    pub fn contains_text(&self, ks: &KeySpaces, k: &str) -> bool {
        ks.peek(k).is_some_and(|(s, key)| self.contains(s, &key)) || {
            let t = self.sets.get(&ks.text());
            t.is_some_and(|set| set.contains(&text_key(k)))
        }
    }

    /// Adds every key of `other`.
    pub fn extend(&mut self, other: IdSets) {
        for (s, set) in other.sets {
            self.sets.entry(s).or_default().extend(set);
        }
    }

    pub fn len(&self) -> usize {
        self.sets.values().map(IdSet::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.sets.values().all(IdSet::is_empty)
    }

    /// The sets, in space order, empty ones dropped: what [`crate::dedup::buffer::Buffer::done`]
    /// takes back.
    pub fn into_pairs(self) -> Vec<(SpaceId, IdSet)> {
        self.sets
            .into_iter()
            .filter(|(_, s)| !s.is_empty())
            .collect()
    }

    /// Removes and renders up to `n` keys, prefixes in order, numbers ascending, the text
    /// keys last (a reset batch).
    pub fn take_texts(&mut self, n: usize, ks: &KeySpaces) -> Vec<String> {
        let mut out = Vec::new();
        for s in ks.slots.values() {
            if out.len() >= n {
                break;
            }
            if let Some(set) = self.sets.get_mut(s) {
                let taken = set.take(n - out.len());
                render_into(ks.prefix(*s), &taken, &mut out);
            }
        }
        if out.len() < n {
            if let Some(set) = self.sets.get_mut(&ks.text()) {
                let taken = set.take(n - out.len());
                render_into("", &taken, &mut out);
            }
        }
        self.sets.retain(|_, s| !s.is_empty());
        out
    }
}

/// A set of text keys as the core holds them (`Key::from_text`): the keys of one listing or
/// snapshot page, the seen-keys of one flush.
pub fn text_set<'a>(keys: impl IntoIterator<Item = &'a str>) -> IdSet {
    let mut set = IdSet::default();
    for k in keys {
        set.insert(text_key(k));
    }
    set
}

#[cfg(test)]
mod tests {
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

    /// A counting allocator: while `ON`, every live heap byte is tallied (a `#[test]`-only
    /// global allocator; the shipping binary is unaffected).
    mod measure {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

        static LIVE: AtomicIsize = AtomicIsize::new(0);
        pub static ON: AtomicBool = AtomicBool::new(false);

        pub fn live() -> isize {
            LIVE.load(Ordering::Relaxed)
        }

        struct Counting;

        // SAFETY: delegates every allocation to `System` and only tallies sizes.
        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                let p = System.alloc(layout);
                if ON.load(Ordering::Relaxed) && !p.is_null() {
                    LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
                }
                p
            }

            unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
                if ON.load(Ordering::Relaxed) {
                    LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
                }
                System.dealloc(p, layout);
            }
        }

        #[global_allocator]
        static COUNTING: Counting = Counting;
    }
}
