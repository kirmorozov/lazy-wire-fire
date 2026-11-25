//! Measurement for the hot-path follow-up (worker lwf-worker-hotpath-ef): the Full-key
//! row, the half of the audit's finding 10 that lives on the ROW path. `row_keys` builds
//! the image's object once per row (`objs[ii]`), then pays a `.clone()` of it and a
//! `json!({"old","new","row"})` — three more deep copies through `to_value` — per
//! (trigger, image), per row. The harness mirrors `engine_hotpath_alloc_tests`' `Hot`
//! exactly (same production `Engine::row_keys`); the printed counts feed
//! `tmp/out/lwf-worker-hotpath-ef/report.md`.

use super::*;
use crate::alloc_measure;
use crate::model::{On, RawRule};
use crate::rows::ColKind;
use crate::rule::Trigger;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value as My;

const RULE: &str = "orders_sync";

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

/// One Full-key trigger on `shop`.`orders`, rendering `lines` key templates. The
/// context is built once per (trigger, image) and shared by its lines, so `lines`
/// shows what the per-image copies are worth beyond the first render.
fn full_rule(lines: usize) -> Arc<Rule> {
    let mut templates = template::Templates::default();
    for i in 0..lines {
        templates
            .add(&format!("k{i}"), "{{row.sku}}", &["row", "old", "new"])
            .expect("compiles");
    }
    let trigger = Trigger {
        def: TriggerDef {
            id: 7,
            rule: RULE.into(),
            schema: "shop".into(),
            table: "orders".into(),
            on: On {
                insert: true,
                update: true,
                delete: true,
            },
            changed_columns: Vec::new(),
            key: None,
            keys_sql: None,
        },
        key: Some(KeyTemplate::Full(
            (0..lines).map(|i| format!("k{i}")).collect(),
        )),
        keys_sql: None,
    };
    let def = crate::model::RuleDef::placeholder(&RawRule {
        name: RULE.into(),
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
        mrule: None,
        mpair: None,
    }
}

/// One rule's worth of the state `on_rows` borrows to build a row's keys (the harness
/// of `engine_hotpath_alloc_tests`, for a Full-key table).
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
    fn new(lines: usize) -> Hot {
        let rule = full_rule(lines);
        let mut rules = BTreeMap::new();
        rules.insert(RULE.to_string(), state(&rule));
        let triggers = vec![(RULE.to_string(), rule, 0)];
        let any_on = Engine::watched_kinds(&triggers);
        Hot {
            w: Watched {
                layout: layout(),
                table: "shop.orders".into(),
                triggers,
                any_full: true,
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
}

/// What a repeated Full-key update row costs (two images, one trigger): the object
/// build, the explicit clone, and the three `json!` clones per image. A Simple-key
/// row of the same table costs 2 (`a_repeated_row_costs_its_column_values_and_nothing_else`).
#[test]
fn a_repeated_full_key_row_costs_this() {
    for lines in [1usize, 3] {
        let mut hot = Hot::new(lines);
        let (before, after) = (row(7, "sku-1", 3), row(7, "sku-2", 5));
        hot.feed(RowsKind::Update, &before, &after);
        let (rows, n) = alloc_measure::count(|| {
            for _ in 0..20 {
                hot.feed(RowsKind::Update, &before, &after);
            }
            hot.marks.get(RULE).map_or(0, |m| m.rows)
        });
        assert_eq!(rows, 21, "every row marked");
        assert!(hot.invalid.is_empty() && hot.render_warnings == 0);
        eprintln!(
            "repeated Full-key update row, {lines} key line(s) on one trigger: {} allocs/row \
             (the same row with a Simple key costs 2)",
            n / 20
        );
        assert!(
            n / 20 >= 8,
            "a Full-key row should cost several copies per image: {}",
            n / 20
        );
    }
}
