//! The rows events no trigger fires on: the skip (the hot-path audit's finding 4). When no
//! trigger of a table fires on insert, update or delete, `on_rows` passes the event's kind
//! over without decoding it — the `on` check in `row_keys` used to drop every row of such an
//! event after paying for the decode. These tests compare the skipped event's state against
//! the state the old path (decode anyway, then drop on the `on` check) produced, and use the
//! counting allocator to show the skip drops the decode: the images come from a closure the
//! way `reader.rows` hands them over, so an unwatched kind must not run it and a watched one
//! must. An undecodable image of an unwatched kind is skipped without error; the watched
//! kind's decode error still propagates, exactly as the `?` in the rows arm did.

use super::*;
use crate::alloc_measure;
use crate::model::{On, RawRule};
use crate::rows::ColKind;
use crate::rule::Trigger;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value as My;
use std::cell::Cell;

/// `shop`.`orders`: `id`, `sku`, `qty`.
fn layout() -> Arc<Layout> {
    Arc::new(Layout::new(
        vec!["id".into(), "sku".into(), "qty".into()],
        vec![ColKind::Plain, ColKind::Plain, ColKind::Plain],
    ))
}

fn row(id: i64, sku: &str, qty: i64) -> Option<BinlogRow> {
    let v = |x: My| Some(BinlogValue::Value(x));
    Some(BinlogRow::new(
        vec![
            v(My::Int(id)),
            v(My::Bytes(sku.as_bytes().to_vec())),
            v(My::Int(qty)),
        ],
        Arc::new([]),
    ))
}

/// One rule, one trigger on `shop`.`orders` firing on the `on` kinds, key `key`, no
/// `keys_sql`.
fn rule(name: &str, key: KeyTemplate, on: On) -> Arc<Rule> {
    let trigger = Trigger {
        def: TriggerDef {
            id: 7,
            rule: name.into(),
            schema: "shop".into(),
            table: "orders".into(),
            on,
            changed_columns: Vec::new(),
            key: None,
            keys_sql: None,
        },
        key: Some(key),
        keys_sql: None,
    };
    let def = RuleDef::placeholder(&RawRule {
        name: name.into(),
        enabled: true,
        ..Default::default()
    });
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates: template::Templates::default(),
        selects: Vec::new(),
        triggers: vec![trigger],
        target: None,
        error: None,
        trigger_error: None,
    })
}

fn state(rule: &Arc<Rule>) -> RuleState {
    RuleState {
        rule: rule.clone(),
        incarnation: 1,
        buffer: Buffer::default(),
        lane: None,
        prep: false,
        skip_until: None,
        frozen_at: None,
        reset: None,
        reset_state: String::new(),
        last_error: None,
        backoff: Backoff::default(),
        pending_clear: false,
        reset_request_seen: false,
        counters: Counters::default(),
        strikes: HashMap::new(),
        written: None,
        dirty: false,
        // metrics off: the skip is what is measured here, not its counters
        mrule: None,
        mpair: None,
    }
}

const RULE: &str = "orders_sync";
const OTHER: &str = "orders_archive";

/// `o-{{id}}`: an integer tail, so the key lives in a slot.
fn simple_id(prefix: &str) -> KeyTemplate {
    KeyTemplate::Simple(vec![vec![
        template::Seg::Lit(prefix.into()),
        template::Seg::Col("id".into()),
    ]])
}

const INSERT_ONLY: On = On {
    insert: true,
    update: false,
    delete: false,
};
const UPDATE_ONLY: On = On {
    insert: false,
    update: true,
    delete: false,
};
const DELETE_ONLY: On = On {
    insert: false,
    update: false,
    delete: true,
};

/// One table's worth of the state `on_rows` borrows, with the event's images handed over by
/// a closure the way `reader.rows` hands them over (so a test sees whether the decode ran).
struct Skip {
    w: Watched,
    marks: HashMap<String, Marks>,
    rules: BTreeMap<String, RuleState>,
    scratch: KeyScratch,
    render_warnings: u64,
    invalid_keys: u64,
    invalid: Vec<(String, u64, String)>,
}

impl Skip {
    /// A table whose triggers are `rules`: each a (rule name, key, kinds it fires on).
    /// `any_on` comes from `Engine::watched_kinds`, the function `route` uses.
    fn new(rules: &[(&str, KeyTemplate, On)]) -> Skip {
        let mut triggers = Vec::new();
        let mut states = BTreeMap::new();
        for (name, key, on) in rules {
            let rule = rule(name, key.clone(), *on);
            states.insert(name.to_string(), state(&rule));
            triggers.push((name.to_string(), rule, 0));
        }
        let any_on = Engine::watched_kinds(&triggers);
        Skip {
            w: Watched {
                layout: layout(),
                table: "shop.orders".into(),
                triggers,
                any_full: false,
                any_on,
            },
            marks: HashMap::new(),
            rules: states,
            scratch: KeyScratch::default(),
            render_warnings: 0,
            invalid_keys: 0,
            invalid: Vec::new(),
        }
    }

    /// One rows event, exactly as the rows arm of `on_item` hands it.
    fn on_rows(
        &mut self,
        kind: RowsKind,
        decode: impl FnOnce() -> Result<Vec<(Option<BinlogRow>, Option<BinlogRow>)>>,
    ) -> Result<()> {
        let Skip {
            w,
            marks,
            rules,
            scratch,
            render_warnings,
            invalid_keys,
            invalid,
        } = self;
        let mut ctx = RowCtx {
            marks,
            rules,
            scratch,
            render_warnings,
            invalid_keys,
            invalid,
        };
        Engine::on_rows(w, kind, decode, &mut ctx)
    }

    /// The old path for an event `on_rows` skips: decode anyway, then let `row_keys`'s `on`
    /// check drop the rows.
    fn feed_decoded(&mut self, kind: RowsKind, rows: &[(Option<BinlogRow>, Option<BinlogRow>)]) {
        for (before, after) in rows {
            let Skip {
                w,
                marks,
                rules,
                scratch,
                render_warnings,
                invalid_keys,
                invalid,
            } = self;
            let mut ctx = RowCtx {
                marks,
                rules,
                scratch,
                render_warnings,
                invalid_keys,
                invalid,
            };
            Engine::row_keys(w, kind, before, after, &mut ctx);
        }
    }

    /// Everything a rows event can change: the transaction's marks (rule, rows counted, keys
    /// as a flush would render them), the counters /status shows, the warnings and the
    /// invalid-key reports. Two tables whose states here are equal behaved the same.
    fn state_of(
        &self,
    ) -> (
        Vec<(String, u64, String)>,
        Vec<(String, u64)>,
        u64,
        u64,
        Vec<(String, u64, String)>,
        Vec<(String, Option<String>)>,
    ) {
        let mut marks: Vec<(String, u64, String)> = self
            .marks
            .iter()
            .map(|(n, m)| {
                (
                    n.clone(),
                    m.rows,
                    self.rules[n].buffer.keys.render(m.ids.as_map()).join(","),
                )
            })
            .collect();
        marks.sort();
        let counters: Vec<(String, u64)> = self
            .rules
            .iter()
            .map(|(n, st)| (n.clone(), st.counters.events))
            .collect();
        let errors: Vec<(String, Option<String>)> = self
            .rules
            .iter()
            .map(|(n, st)| (n.clone(), st.last_error.as_ref().map(|(m, _)| m.clone())))
            .collect();
        (
            marks,
            counters,
            self.render_warnings,
            self.invalid_keys,
            self.invalid.clone(),
            errors,
        )
    }

    /// How many rows of this transaction marked `name` (`None`: it was never marked).
    fn rows(&self, name: &str) -> Option<u64> {
        self.marks.get(name).map(|m| m.rows)
    }
}

#[test]
fn an_unwatched_kind_leaves_the_same_state_whether_decoded_or_not() {
    // A rule firing on inserts only, fed UPDATE and DELETE events: the skip must leave the
    // state identical to the old path of decoding the rows and letting the `on` check drop
    // them — no marks entry, no counted row, no warning, no report.
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    let upd = vec![(before.clone(), after.clone())];
    let del = vec![(before.clone(), None)];

    let mut skipped = Skip::new(&[(RULE, simple_id("o-"), INSERT_ONLY)]);
    let (seen_u, seen_d) = (Cell::new(0usize), Cell::new(0usize));
    // each closure owns the images it hands over (built outside `count`: what the counting
    // allocator measures is the decode the engine would run, not the harness preparing it)
    let dec_u = {
        let u = upd.clone();
        let seen = &seen_u;
        move || {
            seen.set(seen.get() + 1);
            Ok(u)
        }
    };
    let ((), n_upd) = alloc_measure::count(|| {
        skipped
            .on_rows(RowsKind::Update, dec_u)
            .expect("a skipped event cannot fail: nothing is decoded")
    });
    let dec_d = {
        let d = del.clone();
        let seen = &seen_d;
        move || {
            seen.set(seen.get() + 1);
            Ok(d)
        }
    };
    let ((), n_del) = alloc_measure::count(|| {
        skipped
            .on_rows(RowsKind::Delete, dec_d)
            .expect("a skipped event cannot fail: nothing is decoded")
    });
    assert_eq!(
        (seen_u.get(), seen_d.get()),
        (0, 0),
        "an unwatched kind still decoded its event"
    );
    eprintln!("unwatched rows event (update + delete): {n_upd} + {n_del} allocations");
    assert_eq!(
        n_upd + n_del,
        0,
        "skipping allocated {} times: the decode did not disappear",
        n_upd + n_del
    );

    let mut decoded = Skip::new(&[(RULE, simple_id("o-"), INSERT_ONLY)]);
    decoded.feed_decoded(RowsKind::Update, &upd);
    decoded.feed_decoded(RowsKind::Delete, &del);
    assert_eq!(skipped.state_of(), decoded.state_of());
    // and both did nothing at all: the rule was never even entered into the transaction
    assert!(skipped.marks.is_empty() && skipped.invalid.is_empty());
    assert_eq!(skipped.rows(RULE), None);
    assert_eq!(skipped.rules[RULE].counters.events, 0);
    assert_eq!(skipped.rules[RULE].last_error, None);
}

#[test]
fn a_rule_watching_updates_still_decodes_and_marks() {
    // The other half: the same kinds are decoded and marked as before when a trigger fires
    // on them, and a kind this rule does not watch is skipped even on the same table.
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    let upd = vec![(before.clone(), after.clone())];
    let mut s = Skip::new(&[(RULE, simple_id("o-"), UPDATE_ONLY)]);
    let seen = Cell::new(0usize);
    let dec = {
        let u = upd.clone();
        let seen = &seen;
        move || {
            seen.set(seen.get() + 1);
            Ok(u)
        }
    };
    let ((), n) = alloc_measure::count(|| {
        s.on_rows(RowsKind::Update, dec)
            .expect("the watched kind decodes")
    });
    assert_eq!(seen.get(), 1, "a watched kind stopped decoding its event");
    eprintln!("watched rows event (update): {n} allocations (decode + row)");
    assert!(
        n > 0,
        "the watched event cost nothing: the decode was skipped"
    );
    // both images of the row reached the key (the same id, deduped to one key) and one row
    assert_eq!(s.rows(RULE), Some(1));
    let keys = s.rules[RULE]
        .buffer
        .keys
        .render(s.marks[RULE].ids.as_map())
        .join(",");
    assert_eq!(keys, "o-7");
    assert!(s.invalid.is_empty() && s.render_warnings == 0 && s.invalid_keys == 0);

    // the same rule, a kind it does not watch: skipped, the marks stay as they were
    let ins = vec![(None, row(9, "sku-9", 1))];
    let seen2 = Cell::new(0usize);
    let dec_ins = {
        let i = ins.clone();
        let seen = &seen2;
        move || {
            seen.set(seen.get() + 1);
            Ok(i)
        }
    };
    let ((), n) = alloc_measure::count(|| {
        s.on_rows(RowsKind::Insert, dec_ins)
            .expect("a skipped event cannot fail")
    });
    assert_eq!(
        seen2.get(),
        0,
        "an insert reached an update-only rule's decode"
    );
    assert_eq!(n, 0);
    assert_eq!(s.rows(RULE), Some(1), "the skipped event marked something");
}

#[test]
fn an_undecodable_image_errors_only_for_a_watched_kind() {
    // `reader.rows` fails when a row image does not decode against the table map. For an
    // unwatched kind the event is skipped before that can happen: no error, no mark (the
    // rows could not have marked anything — the `on` check drops them). For a watched kind
    // the error propagates as the `?` in the rows arm always made it.
    let mut s = Skip::new(&[(RULE, simple_id("o-"), INSERT_ONLY)]);
    let bad = || {
        Err(anyhow!(
            "decoding rows of shop.orders (table id 42): failed to fill whole buffer"
        ))
    };
    let r = s.on_rows(RowsKind::Update, bad);
    assert!(
        r.is_ok(),
        "an undecodable image of an unwatched kind errored: {r:?}"
    );
    assert!(s.marks.is_empty() && s.invalid.is_empty());
    assert_eq!((s.render_warnings, s.invalid_keys), (0, 0));
    assert_eq!(s.rules[RULE].last_error, None);

    let mut s = Skip::new(&[(RULE, simple_id("o-"), INSERT_ONLY)]);
    let bad2 = || {
        Err(anyhow!(
            "decoding rows of shop.orders (table id 42): failed to fill whole buffer"
        ))
    };
    let r = s.on_rows(RowsKind::Insert, bad2);
    assert!(
        r.is_err(),
        "the decode error of a watched kind was swallowed"
    );
    assert!(s.marks.is_empty(), "a failed decode left marks behind");
}

#[test]
fn watched_kinds_is_the_union_over_all_of_a_tables_triggers() {
    // One rule fires on inserts, another on deletes: updates fire none, so an UPDATE event
    // is skipped for the whole table, while INSERT and DELETE still reach their own rule.
    let mut s = Skip::new(&[
        (RULE, simple_id("o-"), INSERT_ONLY),
        (OTHER, simple_id("a-"), DELETE_ONLY),
    ]);
    assert!(s.w.watches(RowsKind::Insert) && s.w.watches(RowsKind::Delete));
    assert!(
        !s.w.watches(RowsKind::Update),
        "no trigger fires on updates and yet the kind is watched"
    );

    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    let upd = vec![(before.clone(), after.clone())];
    let seen = Cell::new(0usize);
    let dec_upd = {
        let u = upd.clone();
        let seen = &seen;
        move || {
            seen.set(seen.get() + 1);
            Ok(u)
        }
    };
    let ((), n) = alloc_measure::count(|| {
        s.on_rows(RowsKind::Update, dec_upd)
            .expect("a skipped event cannot fail")
    });
    assert_eq!(
        (seen.get(), n),
        (0, 0),
        "the union watched a kind nobody fires on"
    );
    assert!(s.marks.is_empty());

    let ins = vec![(None, row(8, "sku-8", 1))];
    s.on_rows(RowsKind::Insert, || Ok(ins.clone()))
        .expect("inserts are watched");
    assert_eq!(s.rows(RULE), Some(1));
    assert_eq!(s.rows(OTHER), None);
    let del = vec![(row(8, "sku-8", 1), None)];
    s.on_rows(RowsKind::Delete, || Ok(del.clone()))
        .expect("deletes are watched");
    assert_eq!(s.rows(OTHER), Some(1));
    assert_eq!(
        s.rows(RULE),
        Some(1),
        "the delete marked the insert rule too"
    );

    // a table no trigger is left on (its rules were removed between route and read) watches
    // nothing at all: every kind is skipped
    assert_eq!(Engine::watched_kinds(&[]), On::default());
}
