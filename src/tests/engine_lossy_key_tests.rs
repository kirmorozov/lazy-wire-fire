//! The decode-point guard (backlog B-1): a key whose bytes no charset can be read as text
//! must never become a lookup, a document or a delete.
//!
//! [`Layout::key_unsafe`] refuses a column whose *catalog* answer says its bytes are not text
//! (the `binary` family, geometry, `vector`) when the route is built. The catalog's answer is
//! not always there — a privilege gap, a table it does not list, a race with DDL, a charset
//! lazywf has no table for — and there the only proof is the row's own bytes: text holding a
//! replacement character whose bytes are not valid UTF-8 is text the decoder made up. Such a
//! key marks nothing and reports itself once, the way a key that cannot be rendered does.
//!
//! The e2e case `an_unreadable_key_never_reaches_the_target` holds the whole line, from the
//! server's latin2 bytes to a mock that sees no request at all; these pin which rows the guard
//! drops and which it leaves alone, per key form, without a server.

use super::*;
use crate::model::{On, RawRule, TriggerDef};
use crate::rows::{col_kind, Charset};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value as My;

const RULE: &str = "codes_sync";

/// `shop`.`codes`: `id` INT (no charset), `code` VARCHAR in `code_charset`, `note` VARCHAR in
/// utf8mb4.
fn layout(code_charset: Charset) -> Arc<Layout> {
    Arc::new(Layout::with_charsets(
        vec!["id".into(), "code".into(), "note".into()],
        vec![
            col_kind("int", "int"),
            col_kind("varchar", "varchar"),
            col_kind("varchar", "varchar"),
        ],
        vec![Charset::Unknown, code_charset, Charset::Utf8],
        vec!["int".into(), "varchar".into(), "varchar".into()],
    ))
}

fn bytes(v: &[u8]) -> Option<BinlogRow> {
    Some(BinlogRow::new(
        vec![
            Some(BinlogValue::Value(My::Int(7))),
            Some(BinlogValue::Value(My::Bytes(v.to_vec()))),
            Some(BinlogValue::Value(My::Bytes(b"plain".to_vec()))),
        ],
        Arc::new([]),
    ))
}

/// A trigger as `compile_trigger` leaves it: the compiled key beside the raw source the
/// Handlebars scan reads, and the bound columns of a `keys_sql`.
fn trigger(
    key: Option<KeyTemplate>,
    raw: Option<&str>,
    ks: Option<(&str, &[&str])>,
) -> crate::rule::Trigger {
    crate::rule::Trigger {
        def: TriggerDef {
            id: 7,
            rule: RULE.into(),
            schema: "shop".into(),
            table: "codes".into(),
            on: On {
                insert: true,
                update: true,
                delete: true,
            },
            changed_columns: Vec::new(),
            key: raw.map(str::to_string),
            keys_sql: ks.map(|(q, _)| q.to_string()),
        },
        key,
        keys_sql: ks.map(|(_, cols)| crate::rule::KeysSql {
            sql: "SELECT ?".into(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
        }),
    }
}

fn simple(segs: Vec<(&str, &str)>) -> KeyTemplate {
    KeyTemplate::Simple(vec![segs
        .into_iter()
        .map(|(lit, col)| {
            if col.is_empty() {
                template::Seg::Lit(lit.into())
            } else {
                template::Seg::Col(col.into())
            }
        })
        .collect()])
}

fn rule(key: Option<KeyTemplate>, raw: Option<&str>, ks: Option<(&str, &[&str])>) -> Arc<Rule> {
    let mut templates = template::Templates::default();
    if matches!(key, Some(KeyTemplate::Full(_))) {
        // the two templates the Full tests key on: one names `code`, one names only `id`
        templates
            .add("by_code", "{{row.code}}", &["row", "old", "new"])
            .expect("compiles");
        templates
            .add("by_id", "i_{{new.id}}", &["row", "old", "new"])
            .expect("compiles");
    }
    let def = RuleDef::placeholder(&RawRule {
        name: RULE.into(),
        enabled: true,
        ..Default::default()
    });
    Arc::new(Rule {
        def,
        fingerprint: String::new(),
        templates,
        selects: Vec::new(),
        triggers: vec![trigger(key, raw, ks)],
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
        applied_ts: 0,
        strikes: HashMap::new(),
        written: None,
        dirty: false,
        mrule: None,
        mpair: None,
    }
}

/// One rule's worth of what `on_rows` borrows to build a row's keys — driven through
/// `on_rows` itself, so the report the guard leaves behind (`last_error`, the warn-once set)
/// is the one the engine writes.
struct Guard {
    w: Watched,
    marks: HashMap<String, Marks>,
    rules: BTreeMap<String, RuleState>,
    scratch: KeyScratch,
    render_warnings: u64,
    invalid_keys: u64,
    invalid: Vec<(String, u64, String)>,
    lossy: Vec<(String, u64, String)>,
    lossy_refusals: HashSet<(String, u64)>,
}

impl Guard {
    fn at(
        layout: Arc<Layout>,
        key: Option<KeyTemplate>,
        raw: Option<&str>,
        ks: Option<(&str, &[&str])>,
    ) -> Guard {
        let any_full = matches!(key, Some(KeyTemplate::Full(_)));
        let rule = rule(key, raw, ks);
        let mut rules = BTreeMap::new();
        rules.insert(RULE.to_string(), state(&rule));
        let triggers = vec![(RULE.to_string(), rule, 0)];
        let any_on = Engine::watched_kinds(&triggers);
        Guard {
            w: Watched {
                layout,
                table: "shop.codes".into(),
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
            lossy: Vec::new(),
            lossy_refusals: HashSet::new(),
        }
    }

    fn rows(&mut self, kind: RowsKind, rows: Vec<(Option<BinlogRow>, Option<BinlogRow>)>) {
        let Self {
            w,
            marks,
            rules,
            scratch,
            render_warnings,
            invalid_keys,
            invalid,
            lossy,
            lossy_refusals,
        } = self;
        Engine::on_rows(
            w,
            kind,
            || Ok(rows),
            &mut RowCtx {
                marks,
                rules,
                scratch,
                render_warnings,
                invalid_keys,
                invalid,
                lossy,
                lossy_refusals,
            },
        )
        .expect("the rows decode");
    }

    /// The keys the rule marked, as a flush would render them.
    fn keys(&self) -> Vec<String> {
        match self.marks.get(RULE) {
            Some(m) => self.rules[RULE].buffer.keys.render(m.ids.as_map()).to_vec(),
            None => Vec::new(),
        }
    }

    /// The `keys_sql` queries a flush would run.
    fn queries(&self) -> usize {
        self.marks.get(RULE).map_or(0, |m| m.queries.len())
    }

    /// What the rule reports on `/status`, and whether it is still waiting to be written.
    fn report(&self) -> (Option<String>, bool) {
        let st = &self.rules[RULE];
        (st.last_error.as_ref().map(|(m, _)| m.clone()), st.dirty)
    }
}

/// The case the guard exists for: a `code` column in a charset lazywf has no table for, holding
/// bytes that are not UTF-8. Neither the row's key nor its delete goes out, the rule says why
/// once, and the report is the one `/status` shows.
#[test]
fn unreadable_key_bytes_mark_nothing_and_report_once() {
    for kind in [RowsKind::Insert, RowsKind::Update, RowsKind::Delete] {
        let mut g = Guard::at(
            layout(Charset::Unknown),
            Some(simple(vec![("k-", ""), ("", "code")])),
            Some("k-{{new.code}}"),
            None,
        );
        let row = bytes(&[0xFF, 0xFE, 0x41]);
        let images = match kind {
            RowsKind::Insert => vec![(None, row)],
            RowsKind::Update => vec![(row.clone(), row.clone())],
            RowsKind::Delete => vec![(row.clone(), None)],
        };
        // a hundred rows of them: the report is held back after the first
        let mut all = Vec::new();
        for _ in 0..100 {
            all.extend(images.clone());
        }
        g.rows(kind, all);
        assert!(
            g.keys().is_empty(),
            "{kind:?}: a key of the decoder's own characters marked {:?}",
            g.keys()
        );
        let (err, dirty) = g.report();
        let err = err.expect("{kind:?}: the rule says why it marked nothing");
        assert!(
            err.contains("`code`") && err.contains("varchar"),
            "the report does not name the column and its type: {err}"
        );
        assert!(
            err.contains("replacement character") && err.contains("lazy-wire-fire marks no key"),
            "the report does not say what was refused: {err}"
        );
        assert!(
            err.contains("lookup") && err.contains("document") && err.contains("delete"),
            "the report does not name what does not go out: {err}"
        );
        assert!(dirty, "the report is never written to `/status`");
        assert_eq!(
            g.lossy_refusals.len(),
            1,
            "the trigger was reported more than once"
        );
    }
}

/// A key line is one key: when one of its columns cannot be read, the whole line is dropped
/// rather than marked under a key with half of it missing.
#[test]
fn one_unreadable_column_drops_the_whole_key_line() {
    let mut g = Guard::at(
        layout(Charset::Unknown),
        Some(simple(vec![("k-", ""), ("", "id"), ("-", "code")])),
        Some("k-{{new.id}}-{{new.code}}"),
        None,
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xFF, 0x41]))]);
    assert!(g.keys().is_empty(), "half a key was marked: {:?}", g.keys());
}

/// What the guard leaves alone, on the same column and the same bytes: text the row really
/// holds. Valid UTF-8, a legal U+FFFD the value itself carries, and a charset whose decoder
/// cannot produce the character at all.
#[test]
fn a_key_the_row_can_read_marks_as_it_always_did() {
    // valid UTF-8 in a charset nobody named
    let mut g = Guard::at(
        layout(Charset::Unknown),
        Some(simple(vec![("k-", ""), ("", "code")])),
        Some("k-{{new.code}}"),
        None,
    );
    g.rows(RowsKind::Insert, vec![(None, bytes("café".as_bytes()))]);
    assert_eq!(g.keys(), vec!["k-café".to_string()]);
    assert_eq!(g.report(), (None, false), "a readable key reported");

    // the replacement character as the row's own value: its bytes read fine
    let mut g = Guard::at(
        layout(Charset::Unknown),
        Some(simple(vec![("k-", ""), ("", "code")])),
        Some("k-{{new.code}}"),
        None,
    );
    g.rows(
        RowsKind::Insert,
        vec![(None, bytes("a\u{FFFD}b".as_bytes()))],
    );
    assert_eq!(g.keys(), vec!["k-a\u{FFFD}b".to_string()]);
    assert_eq!(
        g.report(),
        (None, false),
        "the row's own character reported"
    );

    // latin1: the same impossible bytes are cp1252 text, exactly decoded
    let mut g = Guard::at(
        layout(Charset::Latin1),
        Some(simple(vec![("k-", ""), ("", "code")])),
        Some("k-{{new.code}}"),
        None,
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xC9, 0x80]))]);
    assert_eq!(g.keys(), vec!["k-É€".to_string()]);
    assert_eq!(g.report(), (None, false), "a latin1 key reported");

    // utf8mb4: the guard does not look at bytes a charset has already named
    let mut g = Guard::at(
        layout(Charset::Utf8),
        Some(simple(vec![("k-", ""), ("", "code")])),
        Some("k-{{new.code}}"),
        None,
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xFF, 0xFE]))]);
    assert_eq!(
        g.keys().len(),
        1,
        "a utf8mb4 column paid for the guard's read of its bytes"
    );
    assert_eq!(g.report(), (None, false));
}

/// A `keys_sql` bound value is the row's decoded text in the user's statement: one made of
/// replacement characters would answer with another row's `_key`, so the query does not go out.
#[test]
fn an_unreadable_bound_value_sends_no_query() {
    let mut g = Guard::at(
        layout(Charset::Unknown),
        None,
        None,
        Some(("SELECT _key FROM t WHERE code IN (?)", &["code"][..])),
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xFF, 0xFE]))]);
    assert_eq!(g.queries(), 0, "the query went out with lossy bytes");
    let (err, dirty) = g.report();
    let err = err.expect("the rule says why no query went out");
    assert!(
        err.contains("`code`") && err.contains("replacement character"),
        "the report names {err}"
    );
    assert!(dirty);

    // the same trigger binding a column the row can read: the query goes out as it always did
    let mut g = Guard::at(
        layout(Charset::Unknown),
        None,
        None,
        Some(("SELECT _key FROM t WHERE id IN (?)", &["id"][..])),
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xFF, 0xFE]))]);
    assert_eq!(g.queries(), 1, "the query on `id` did not go out");
    assert_eq!(g.report(), (None, false));
}

/// A Handlebars key renders the whole row, so the guard asks the columns the template names —
/// the same scan the load-time refusal uses. The ones it does not name are none of its business.
#[test]
fn a_full_key_names_only_the_columns_it_reads() {
    // the template reads `code`
    let mut g = Guard::at(
        layout(Charset::Unknown),
        Some(KeyTemplate::Full(vec!["by_code".into()])),
        Some("{{row.code}}"),
        None,
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xFF, 0xFE]))]);
    assert!(
        g.keys().is_empty(),
        "the Full key of unreadable bytes marked {:?}",
        g.keys()
    );
    let (err, dirty) = g.report();
    let err = err.expect("the Full rule says why it marked nothing");
    assert!(err.contains("`code`") && dirty, "the report is {err:?}");

    // the template reads `id` only: the unreadable column stands beside it, untouched
    let mut g = Guard::at(
        layout(Charset::Unknown),
        Some(KeyTemplate::Full(vec!["by_id".into()])),
        Some("i_{{new.id}}"),
        None,
    );
    g.rows(RowsKind::Insert, vec![(None, bytes(&[0xFF, 0xFE]))]);
    assert_eq!(g.keys(), vec!["i_7".to_string()]);
    assert_eq!(
        g.report(),
        (None, false),
        "a Full key reported a column it never read"
    );
}
