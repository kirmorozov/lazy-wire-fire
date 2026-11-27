//! The two pure halves of the [A] refusal (backlog B-1): which columns a trigger builds its
//! key from (`key_column_reads`) and what a Handlebars key names inside its tags (`hb_refs`).
//! The refusal's effect — the warning, the rule's `last_error`, the trigger out of the route
//! so no delete reaches the target — is pinned by the e2e cases
//! (`binary_key_is_refused_at_load`, `geometry_key_is_refused_at_load`,
//! `a_stored_hex_projection_carries_the_key`); these pin the scans the refusal hangs on,
//! per key form, so a template that never names the binary column never meets it.

use super::*;
use crate::model::{On, TriggerDef};
use crate::rows::{Charset, ColKind};

/// `s`.`t`: `id` INT, `code` BINARY, `note` VARCHAR, `pic` POINT.
fn layout() -> Layout {
    Layout::with_charsets(
        vec!["id".into(), "code".into(), "note".into(), "pic".into()],
        vec![ColKind::Plain; 4],
        vec![Charset::Utf8, Charset::Binary, Charset::Utf8, Charset::Utf8],
        vec![
            "int".into(),
            "binary".into(),
            "varchar".into(),
            "point".into(),
        ],
    )
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
            rule: "r".into(),
            schema: "s".into(),
            table: "t".into(),
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

/// A `Simple` key reads exactly its `Seg::Col` columns — and a table holding bytes is
/// untouched as long as the key stands on safe ones.
#[test]
fn simple_key_reads_its_columns() {
    let l = layout();
    let t = trigger(
        Some(simple(vec![("o-", ""), ("", "code")])),
        Some("o-{{new.code}}"),
        None,
    );
    assert_eq!(key_column_reads(&t, &l), vec![1]);
    assert!(l.key_unsafe(1).is_some());

    let t = trigger(
        Some(simple(vec![("o-", ""), ("", "id")])),
        Some("o-{{new.id}}"),
        None,
    );
    assert_eq!(key_column_reads(&t, &l), vec![0]);
    assert_eq!(l.key_unsafe(0), None);
}

/// A Handlebars key reads what its tags name — through blocks, helpers and `this` paths.
#[test]
fn handlebars_key_reads_the_columns_its_tags_name() {
    let l = layout();
    let raw = "{{#if (eq new.flag 1)}}a{{else}}{{upper this.code}}-{{note}}{{/if}}";
    let t = trigger(
        Some(KeyTemplate::Full(vec!["key7_0".into()])),
        Some(raw),
        None,
    );
    // `flag` is no column of this table, so the read list holds the two that are
    assert_eq!(key_column_reads(&t, &l), vec![1, 2]);
    // the roots are not reads; helper names are (the scan finds names, it does not parse:
    // a column named `upper` and a helper `upper` are refused together, erring towards safe)
    assert!(!hb_refs(raw).contains(&"new"));
    assert!(hb_refs(raw).contains(&"upper"));
    assert!(
        hb_refs(raw).contains(&"flag"),
        "the scan is not layout-bound"
    );
}

/// Literal text, comments and raw-block content name nothing: a table with a binary column
/// keeps a Handlebars key that only spells its name in text.
#[test]
fn handlebars_text_outside_the_tags_is_not_a_read() {
    let l = layout();
    let raw = "code-{{id}} {{!-- keyed on code --}} {{{{raw}}}}code{{{{/raw}}}} {{! code }}";
    let t = trigger(
        Some(KeyTemplate::Full(vec!["key7_0".into()])),
        Some(raw),
        None,
    );
    assert_eq!(key_column_reads(&t, &l), vec![0]);
    assert_eq!(l.key_unsafe(0), None, "the key stands on `id` alone");
}

/// The columns bound to a `keys_sql` are key reads too: the bound value reaches the
/// statement as decoded text, so bytes with no charset answer with replacement characters.
#[test]
fn keys_sql_reads_its_bound_columns() {
    let l = layout();
    let t = trigger(
        None,
        None,
        Some(("SELECT HEX(code) AS _key FROM t WHERE id IN (?)", &["id"])),
    );
    assert_eq!(key_column_reads(&t, &l), vec![0]);
    assert_eq!(l.key_unsafe(0), None);

    let t = trigger(
        None,
        None,
        Some(("SELECT code AS _key WHERE (?)", &["code"])),
    );
    assert_eq!(key_column_reads(&t, &l), vec![1]);
    assert!(l.key_unsafe(1).is_some());

    // a key and a `keys_sql` on one trigger: both reads count
    let t = trigger(
        Some(simple(vec![("o-", ""), ("", "id")])),
        Some("o-{{new.id}}"),
        Some(("SELECT ? AS _key", &["pic"])),
    );
    let reads = key_column_reads(&t, &l);
    assert!(reads.contains(&0) && reads.contains(&3));
    assert!(l.key_unsafe(3).is_some(), "the POINT is the refusal here");
}

/// A trigger keyed on nothing but `changed_columns` pays no key reads: the comparison of
/// two images of the same row cannot name another row's document.
#[test]
fn no_key_no_reads() {
    let l = layout();
    let mut t = trigger(None, None, None);
    t.def.changed_columns = vec!["code".into(), "pic".into()];
    assert!(key_column_reads(&t, &l).is_empty());
}
