//! The startup registration of lazywf's own tables with the vendored view planner: the planner
//! refuses them as view sources (`analyze::set_reserved`).
//!
//! A test binary of its own, not the lib tests: `set_reserved` takes effect on the first call in
//! a process, and the planner's own unit tests (in the lib test binary) already take that call
//! with their names.

use std::collections::HashMap;
use std::sync::Arc;

use lazy_wire_fire::analyze::{parse_definition, plan_block, set_reserved};
use lazy_wire_fire::catalog::{Column, TableInfo, TableRef};
use lazy_wire_fire::model::{RESERVED_PREFIX, RESERVED_TABLES};

fn catalog(tables: &[&str]) -> HashMap<TableRef, Arc<TableInfo>> {
    tables
        .iter()
        .map(|name| {
            let t = TableInfo {
                schema: "db".into(),
                name: (*name).into(),
                columns: vec![Column::simple("id", "int")],
                pk: vec![0],
                unique_keys: vec![],
                cascades: vec![],
                is_view: false,
                view_bases: vec![],
            };
            (("db".into(), (*name).into()), Arc::new(t))
        })
        .collect()
}

/// The error of planning `SELECT id FROM <table>` as a view, against `cat`.
fn source_error(table: &str, cat: &HashMap<TableRef, Arc<TableInfo>>) -> String {
    plan_block(
        &parse_definition(&format!("CREATE VIEW v AS SELECT id FROM {table}")).unwrap(),
        "db",
        cat,
    )
    .unwrap_err()
    .to_string()
}

#[test]
fn startup_reserves_the_own_tables() {
    assert!(
        set_reserved(RESERVED_TABLES, &[RESERVED_PREFIX]),
        "the first call in this process is the startup registration"
    );
    let cat = catalog(&["orders"]);

    // Every own table is refused as a view source, exact name and case variant.
    for t in RESERVED_TABLES {
        for table in [t, t.to_uppercase().as_str()] {
            let e = source_error(table, &cat);
            assert!(e.contains("reserved table"), "{table}: {e}");
        }
    }

    // The shared prefix also refuses own tables the exact list does not enumerate.
    let e = source_error("lazy_wire_fire_something", &cat);
    assert!(e.contains("reserved table"), "{e}");

    // A data table still plans.
    plan_block(
        &parse_definition("CREATE VIEW v AS SELECT o.id FROM orders o").unwrap(),
        "db",
        &cat,
    )
    .expect("a data table is not reserved");
}
