//! The rows' hot path: what one row costs. The per-row machinery — the row's images, the
//! transaction's marks, the string a key is built in — allocates nothing once a transaction has
//! paid for its first row (the hot-path audit's findings 3, 6 and 7). What a row still pays is
//! one column value per watched column, which is finding 2 (`Layout::value`, another PR).

use super::*;
use crate::alloc_measure;
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
        // every root a Full key can read, in one key: the context's shape, visible in what
        // the trigger marks (`a_full_key_row_renders_the_context_it_always_rendered`)
        templates
            .add(
                "k1",
                "{{op}}|{{table}}|{{row.sku}}|{{old.qty}}|{{new.qty}}",
                &["row", "old", "new", "op", "table"],
            )
            .expect("compiles");
        // the whole context as JSON: the order of its roots is part of what a template can
        // see (`a_full_key_row_costs_three_copies_of_its_object` pins it)
        templates
            .add("k2", "{{json this}}", &["row", "old", "new", "op", "table"])
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
        reset_request_seen: false,
        counters: Counters::default(),
        strikes: HashMap::new(),
        written: None,
        dirty: false,
        // metrics off: a row's keys are what is measured here, not its counters
        mrule: None,
        mpair: None,
    }
}

const RULE: &str = "orders_sync";

/// One rule's worth of the state `on_rows` borrows to build a row's keys.
struct Hot {
    w: Watched,
    marks: HashMap<String, Marks>,
    rules: BTreeMap<String, RuleState>,
    scratch: KeyScratch,
    render_warnings: u64,
    invalid_keys: u64,
    invalid: Vec<(String, u64, String)>,
}

impl Hot {
    /// A table with one trigger whose key is `key` (`any_full`: whether it is a Full one).
    fn new(key: KeyTemplate, changed: &[&str], any_full: bool) -> Hot {
        Self::at(layout(), key, changed, any_full)
    }

    /// The same over a given layout: the Full-key tests use a wide table, where the row's
    /// object is big enough for its copies to show up above the render.
    fn at(layout: Arc<Layout>, key: KeyTemplate, changed: &[&str], any_full: bool) -> Hot {
        let rule = rule(key, changed, any_full);
        let mut rules = BTreeMap::new();
        rules.insert(RULE.to_string(), state(&rule));
        let triggers = vec![(RULE.to_string(), rule, 0)];
        let any_on = Engine::watched_kinds(&triggers);
        Hot {
            w: Watched {
                layout,
                table: "shop.orders".into(),
                triggers,
                any_full,
                any_on,
            },
            marks: HashMap::new(),
            rules,
            scratch: KeyScratch::default(),
            render_warnings: 0,
            invalid_keys: 0,
            invalid: Vec::new(),
        }
    }

    /// One row, exactly as `on_rows` hands it to `row_keys`.
    fn feed(&mut self, kind: RowsKind, before: &Option<BinlogRow>, after: &Option<BinlogRow>) {
        let Hot {
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
fn value_cost() -> usize {
    let (_, n) = alloc_measure::count(|| std::hint::black_box(Value::from(7i64)));
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

    let ((), n) = alloc_measure::count(|| {
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
    let ((), n) = alloc_measure::count(|| {
        for _ in 0..20 {
            hot.feed(RowsKind::Update, &before, &after);
        }
    });
    assert_eq!(n, 0, "the per-row machinery allocated {n} times");
    // an insert and a delete have one image each: the unused slot costs nothing either
    let only = row(9, "sku-9", 1);
    hot.feed(RowsKind::Insert, &None, &only);
    hot.feed(RowsKind::Delete, &only, &None);
    let ((), n) = alloc_measure::count(|| {
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
    let ((), first) = alloc_measure::count(|| hot.feed(RowsKind::Update, &before, &after));
    assert!(
        first > 2 * value_cost(),
        "the transaction's first row pays for the rule's name and the key's slot: {first}"
    );
    let ((), later) = alloc_measure::count(|| hot.feed(RowsKind::Update, &before, &after));
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
    let ((), n) = alloc_measure::count(|| loud.feed(RowsKind::Update, &before, &after));
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

/// The same table, wide: twelve watched columns, so one copy of the row's object is a dozen
/// keys and a dozen values and the copies a Full key pays for it show up above the render.
fn wide_layout() -> Arc<Layout> {
    Arc::new(Layout::new(
        (0..12)
            .map(|i| format!("c{i}"))
            .chain(["sku".into(), "qty".into()])
            .collect(),
        vec![ColKind::Plain; 14],
    ))
}

/// A row of `wide_layout`: text columns with text in them, so a copy costs what it costs.
fn wide_row(sku: &str, qty: i64) -> Option<BinlogRow> {
    let v = |x: My| Some(BinlogValue::Value(x));
    Some(BinlogRow::new(
        (0..12)
            .map(|i| {
                v(My::Bytes(
                    format!("column-{i}-value-of-a-decent-length").into_bytes(),
                ))
            })
            .chain([v(My::Bytes(sku.as_bytes().to_vec())), v(My::Int(qty))])
            .collect(),
        Arc::new([]),
    ))
}

/// The five roots a Full key reads come out of one row object: `old`, `new` and `row` are
/// the same value. Building the context by hand instead of letting `json!` serialise that
/// object three times must change nothing a user can see — the same template, the same key,
/// including a value that needs escaping and a non-ASCII one. Main renders the same string:
/// this assertion is the equality, not the cost (`the row's object is copied three times`).
#[test]
fn a_full_key_row_renders_the_context_it_always_rendered() {
    let mut hot = Hot::new(KeyTemplate::Full(vec!["k0".into(), "k1".into()]), &[], true);
    let (before, after) = (row(7, "sku-\"1\"", 3), row(7, "北京-2", 5));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(hot.render_warnings, 0);
    assert_eq!(hot.rows(), Some(1));
    // The buffer hands the marked keys out in its own order; what is compared is the set.
    let marked = hot.keys();
    let got: std::collections::BTreeSet<&str> = marked.split(',').collect();
    let want: std::collections::BTreeSet<&str> = [
        "sku-\"1\"",
        "北京-2",
        "update|shop.orders|sku-\"1\"|3|3",
        "update|shop.orders|北京-2|5|5",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        got, want,
        "a Full key renders the same context it always rendered"
    );
}

/// The order of the row context's roots is part of what a template can see, and `Value ==`
/// would not catch it: `serde_json` is built with `preserve_order`, so an ordered map
/// compares without regard to order. It is pinned where it is real — a Full key whose
/// template is `{{json this}}` marks the rendered context as its key. The order is the one
/// `json!` wrote, the values are the row's.
#[test]
fn a_full_key_rows_context_keeps_the_order_of_its_roots() {
    let mut hot = Hot::new(KeyTemplate::Full(vec!["k2".into()]), &[], true);
    // one image: an insert, so the context renders once and the rule marks one key
    let after = row(7, "sku-1", 3);
    hot.feed(RowsKind::Insert, &None, &after);
    assert_eq!(hot.render_warnings, 0);
    let rendered = hot.keys();
    let ctx: Value = serde_json::from_str(&rendered).expect("the marked key is the context");
    let roots: Vec<&str> = ctx
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        roots,
        vec!["old", "new", "row", "op", "table"],
        "the row context's roots are not in the order `json!` wrote them ({rendered})"
    );
    assert_eq!(ctx["op"], json!("insert"), "the op the row carries");
    assert_eq!(ctx["table"], json!("shop.orders"), "the table it came from");
    assert_eq!(ctx["row"]["sku"], json!("sku-1"), "the row itself");
    assert_eq!(ctx["old"], ctx["row"], "and the three roots that hold it");
    assert_eq!(ctx["new"], ctx["row"]);
}

/// The cost half of the same finding: the row's object is built once per image and read by
/// three roots. `json!` deep-copied it into a new value for each of them — four copies
/// counting the build; built by hand, two roots clone it and the third takes it, three.
/// The bound is the four-copy shape's number less the copy this removes: run against main's
/// `json!` line it fails, which is what makes it a bound and not a print.
#[test]
fn a_full_key_row_costs_three_copies_of_its_object() {
    let mut hot = Hot::at(
        wide_layout(),
        KeyTemplate::Full(vec!["k0".into()]),
        &[],
        true,
    );
    let (before, after) = (wide_row("sku-1", 3), wide_row("sku-2", 5));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(hot.render_warnings, 0);
    assert_eq!(hot.keys(), "sku-1,sku-2");
    let ((), n) = alloc_measure::count(|| {
        for _ in 0..10 {
            hot.feed(RowsKind::Update, &before, &after);
        }
    });
    let per_row = n / 10;
    eprintln!("a Full-key row of fourteen columns, both images: {per_row} allocations");
    assert!(
        per_row <= 500,
        "the row's object is being copied again: {per_row} allocations per Full-key row \
         against the number measured here, with main's four copies per image"
    );
}

/// What one text column costs as a value, the way the row path takes it: the borrowed conversion
/// pays for the `String` it builds, where the owned one (`to_json(v.clone(), None)`, what
/// `Layout::value` did before the borrow) paid for that and for the `Vec<u8>` it came out of.
fn text_value_cost() -> usize {
    let v = My::Bytes(b"sku-1".to_vec());
    let (_, n) = alloc_measure::count(|| std::hint::black_box(crate::docs::to_json_ref(&v, None)));
    n
}

/// The same value through the owned conversion, for the number the tests above compare against.
fn owned_text_value_cost() -> usize {
    let v = My::Bytes(b"sku-1".to_vec());
    let (_, n) =
        alloc_measure::count(|| std::hint::black_box(crate::docs::to_json(v.clone(), None)));
    n
}

#[test]
fn a_text_column_value_is_built_from_its_bytes() {
    // finding 2, the conversion half: a watched `VARCHAR` used to clone its buffer to convert it.
    let (borrowed, owned) = (text_value_cost(), owned_text_value_cost());
    assert_eq!(
        value_cost(),
        borrowed,
        "a borrowed text value is its String"
    );
    assert_eq!(
        owned,
        borrowed + 1,
        "the owned conversion no longer pays for the `Vec` clone too"
    );
}

#[test]
fn an_unchanged_watched_text_column_costs_the_row_nothing() {
    // finding 2, the comparison half: `sku` is what the trigger watches and both images hold the
    // same bytes, so the row is refused without converting either of them.
    let mut hot = Hot::new(simple_id(), &["sku"], false);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-1", 4));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(
        hot.rows(),
        None,
        "an unchanged watched column must not mark"
    );
    assert_eq!(hot.keys(), "");
    let ((), n) = alloc_measure::count(|| {
        for _ in 0..20 {
            hot.feed(RowsKind::Update, &before, &after);
        }
    });
    assert_eq!(
        n, 0,
        "a row whose watched text column is the same allocated {n}"
    );
    assert_eq!(hot.rows(), None);
}

#[test]
fn a_changed_watched_text_column_costs_one_value_per_image() {
    // the same trigger, and `sku` does change: two values for the check (one per image) plus the
    // two the key always costs.
    let mut hot = Hot::new(simple_id(), &["sku"], false);
    let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 3));
    hot.feed(RowsKind::Update, &before, &after);
    assert_eq!(hot.rows(), Some(1));
    assert_eq!(hot.keys(), "o-7");
    let ((), n) = alloc_measure::count(|| hot.feed(RowsKind::Update, &before, &after));
    assert_eq!(
        n,
        2 * text_value_cost() + 2 * value_cost(),
        "a row with a changed text column cost {n}, not one value per image and per key"
    );
    assert_eq!(hot.rows(), Some(2));
    assert_eq!(hot.keys(), "o-7");
}
