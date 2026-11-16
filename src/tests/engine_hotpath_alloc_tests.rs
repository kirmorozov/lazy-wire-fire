//! The rows' hot path: what one row costs. The per-row machinery — the row's images, the
//! transaction's marks, the string a key is built in — allocates nothing once a transaction has
//! paid for its first row (the hot-path audit's findings 3, 6 and 7). What a row still pays is
//! one column value per watched column, which is finding 2 (`Layout::value`, another PR).

use super::*;
use crate::alloc_count;
use crate::model::{On, RawRule};
use crate::rows::ColKind;
use crate::rule::Trigger;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value as My;

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

/// One rule, one trigger on `shop`.`orders` (every kind, no `changed_columns` unless asked
/// for): keys `key`, and no `keys_sql`. `full` registers the template a Full key renders
/// (`{{row.sku}}` under the name `k0`).
fn rule(key: KeyTemplate, changed: &[&str], full: bool) -> Arc<Rule> {
    let mut templates = template::Templates::default();
    if full {
        templates
            .add("k0", "{{row.sku}}", &["row", "old", "new"])
            .expect("compiles");
    }
    let trigger = Trigger {
        def: TriggerDef {
            id: 7,
            rule: "orders_sync".into(),
            schema: "shop".into(),
            table: "orders".into(),
            on: On {
                insert: true,
                update: true,
                delete: true,
            },
            changed_columns: changed.iter().map(|c| c.to_string()).collect(),
            key: None,
            keys_sql: None,
        },
        key: Some(key),
        keys_sql: None,
    };
    let def = RuleDef::placeholder(&RawRule {
        name: "orders_sync".into(),
        enabled: true,
        ..Default::default()
    });
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates,
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
        counters: Counters::default(),
        strikes: HashMap::new(),
        written: None,
        dirty: false,
    }
}

const RULE: &str = "orders_sync";

/// One rule's worth of the state `feed` borrows to build a row's keys.
struct Hot {
    w: Watched,
    marks: HashMap<String, Marks>,
    rules: BTreeMap<String, RuleState>,
    scratch: KeyScratch,
    render_warnings: u64,
    invalid: Vec<(String, u64, String)>,
}

impl Hot {
    /// A table with one trigger whose key is `key` (`any_full`: whether it is a Full one).
    fn new(key: KeyTemplate, changed: &[&str], any_full: bool) -> Hot {
        let rule = rule(key, changed, any_full);
        let mut rules = BTreeMap::new();
        rules.insert(RULE.to_string(), state(&rule));
        Hot {
            w: Watched {
                layout: layout(),
                table: "shop.orders".into(),
                triggers: vec![(RULE.to_string(), rule, 0)],
                any_full,
            },
            marks: HashMap::new(),
            rules,
            scratch: KeyScratch::default(),
            render_warnings: 0,
            invalid: Vec::new(),
        }
    }

    /// One row, exactly as `feed` hands it to `row_keys`.
    fn feed(&mut self, kind: RowsKind, before: &Option<BinlogRow>, after: &Option<BinlogRow>) {
        let Hot {
            w,
            marks,
            rules,
            scratch,
            render_warnings,
            invalid,
        } = self;
        let mut ctx = RowCtx {
            marks,
            rules,
            scratch,
            render_warnings,
            invalid,
        };
        Engine::row_keys(w, kind, before, after, &mut ctx);
    }

    /// The keys the rule has marked, as a flush would render them (empty when it marked none).
    fn keys(&self) -> String {
        match self.marks.get(RULE) {
            Some(m) => self.rules[RULE]
                .buffer
                .keys
                .render(m.ids.as_map())
                .join(","),
            None => String::new(),
        }
    }

    /// How many rows of this transaction marked the rule (`None`: the rule was never marked).
    fn rows(&self) -> Option<u64> {
        self.marks.get(RULE).map(|m| m.rows)
    }
}

/// The Simple key of the audit: `o-{{new.id}}`, an integer tail, so the key lives in a slot.
fn simple_id() -> KeyTemplate {
    KeyTemplate::Simple(vec![vec![
        template::Seg::Lit("o-".into()),
        template::Seg::Col("id".into()),
    ]])
}

/// One allocation: `serde_json` is built with `arbitrary_precision`, so the digits of a number
/// are a boxed string. Everything a row costs beyond this is the machinery below, which is what
/// these tests measure (and what the audit's findings 3, 6 and 7 are about).
fn value_cost() -> u64 {
    let (_, n) = alloc_count::counted(|| std::hint::black_box(Value::from(7i64)));
    n
}

#[test]
fn a_repeated_row_costs_its_column_values_and_nothing_else() {
    // `o-{{id}}` reads one column per image: an update row has two images, so two column
    // values. Nothing else about a row may allocate.
    let one = value_cost();
    assert_eq!(one, 1, "a column value should cost one allocation");
    let mut hot = Hot::new(simple_id(), &[], false);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(hot.keys(), "o-7");
    assert_eq!(hot.rows(), Some(1));

    let ((), n) = alloc_count::counted(|| {
        for _ in 0..20 {
            hot.feed(RowsKind::Update, &before, &after);
        }
    });
    assert_eq!(
        n,
        2 * 20 * one,
        "a repeated Simple-key row allocated {n} times, not the {one} per image"
    );
    // and it marked what it always marked, no more and no less
    assert_eq!(hot.rows(), Some(21));
    assert_eq!(hot.keys(), "o-7");
    assert!(hot.invalid.is_empty() && hot.render_warnings == 0);
}

#[test]
fn the_row_machinery_itself_allocates_nothing() {
    // A key with no column at all: what is left is the row's images, the transaction's marks
    // and the scratch string — the machinery findings 3, 6 and 7 cover. It must be free.
    let mut hot = Hot::new(
        KeyTemplate::Simple(vec![vec![template::Seg::Lit("7".into())]]),
        &[],
        false,
    );
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(hot.keys(), "7");
    let ((), n) = alloc_count::counted(|| {
        for _ in 0..20 {
            hot.feed(RowsKind::Update, &before, &after);
        }
    });
    assert_eq!(n, 0, "the per-row machinery allocated {n} times");
    // an insert and a delete have one image each: the unused slot costs nothing either
    let only = row(9, "sku-9", 1);
    hot.feed(RowsKind::Insert, &None, &only);
    hot.feed(RowsKind::Delete, &only, &None);
    let ((), n) = alloc_count::counted(|| {
        for _ in 0..10 {
            hot.feed(RowsKind::Insert, &None, &only);
            hot.feed(RowsKind::Delete, &only, &None);
        }
    });
    assert_eq!(n, 0, "a one-image row allocated {n} times");
    // one update, twenty like it, an insert, a delete, and twenty of each
    assert_eq!(hot.rows(), Some(43));
}

#[test]
fn a_rule_name_is_cloned_once_per_transaction_not_per_row() {
    // finding 6: the first row of a transaction takes the name, the rest only look it up.
    let mut hot = Hot::new(simple_id(), &[], false);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    let ((), first) = alloc_count::counted(|| hot.feed(RowsKind::Update, &before, &after));
    assert!(
        first > 2 * value_cost(),
        "the transaction's first row pays for the rule's name and the key's slot: {first}"
    );
    let ((), later) = alloc_count::counted(|| hot.feed(RowsKind::Update, &before, &after));
    assert_eq!(later, 2 * value_cost(), "a later row cloned the name again");
    assert_eq!(hot.rows(), Some(2));
}

#[test]
fn changed_columns_still_decide_what_a_row_marks() {
    // `qty` is what the trigger watches, and `qty` does not change: the row marks nothing and
    // the rule does not even enter the transaction's marks.
    let mut quiet = Hot::new(simple_id(), &["qty"], false);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 3));
    quiet.feed(RowsKind::Update, &before, &after);
    assert_eq!(quiet.rows(), None);
    assert_eq!(quiet.keys(), "");

    // the same trigger, and `qty` does change: the two images of the check and the two of the
    // key are all it costs
    let mut loud = Hot::new(simple_id(), &["qty"], false);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 4));
    loud.feed(RowsKind::Update, &before, &after);
    assert_eq!(loud.rows(), Some(1));
    assert_eq!(loud.keys(), "o-7");
    let ((), n) = alloc_count::counted(|| loud.feed(RowsKind::Update, &before, &after));
    assert_eq!(n, 4 * value_cost(), "a row that marks again cost {n}");
}

#[test]
fn a_full_key_row_still_gets_its_object() {
    // the object a Full key renders is built only for a table that has a Full-key trigger
    // (`Watched::any_full`); both images of the row still reach the template.
    let mut hot = Hot::new(KeyTemplate::Full(vec!["k0".into()]), &[], true);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(hot.keys(), "sku-1,sku-2");
    assert_eq!(hot.render_warnings, 0);
    assert_eq!(hot.rows(), Some(1));
}
