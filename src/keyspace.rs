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
mod tests;
