//! The server's binlog filters against the schemas lazywf depends on.
//!
//! With `binlog-do-db` / `binlog-ignore-db` set, row changes to a dropped schema are not binlogged
//! at all: a source schema's triggers never fire, and a filtered own schema makes the status and
//! commit-fence writes invisible to the binlog. The check is advisory; the engine logs it and
//! keeps going.

use std::collections::BTreeSet;

use crate::binlog::schema_filtered;

/// Why lazywf checks a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The schema of lazywf's own tables (the DSN's database).
    Own,
    /// A schema named by an enabled trigger's `source_table`.
    Source,
}

/// Which server filter drops a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Filter {
    /// `Binlog_Do_DB` is set and does not list the schema.
    DoDb,
    /// `Binlog_Ignore_DB` lists the schema.
    IgnoreDb,
}

/// The schemas to check now, own first, each once: the own schema (when known), then each source
/// schema that is neither the own one nor already in `checked`. The result is ordered and unique.
pub fn to_check(
    own: Option<&str>,
    sources: &BTreeSet<String>,
    checked: &std::collections::HashSet<String>,
) -> Vec<(String, Role)> {
    let mut out = Vec::new();
    if let Some(own) = own {
        if !checked.contains(own) {
            out.push((own.to_string(), Role::Own));
        }
    }
    for s in sources {
        if Some(s.as_str()) == own || checked.contains(s) {
            continue;
        }
        out.push((s.clone(), Role::Source));
    }
    out
}

/// The filter that drops `schema`, if any. `Binlog_Do_DB` is tested first: when both drop it, the
/// missing do-list is the reason reported.
pub fn filter_of(do_db: &str, ignore_db: &str, schema: &str) -> Option<Filter> {
    if schema_filtered(do_db, "", schema) {
        Some(Filter::DoDb)
    } else if schema_filtered("", ignore_db, schema) {
        Some(Filter::IgnoreDb)
    } else {
        None
    }
}

/// The WARN text for a dropped schema.
pub fn warning(role: Role, schema: &str, filter: Filter) -> String {
    let why = match filter {
        Filter::DoDb => "Binlog_Do_DB does not list it",
        Filter::IgnoreDb => "Binlog_Ignore_DB lists it",
    };
    match role {
        Role::Source => format!(
            "the server's binlog filter drops source schema `{schema}` ({why}): \
             changes in it are not binlogged, so its triggers never fire; \
             remove the filter or include the schema"
        ),
        Role::Own => format!(
            "the server's binlog filter drops lazywf's own schema `{schema}` ({why}): \
             the status and commit-fence writes are not binlogged, so the fence cannot prove \
             visibility; remove the filter or include the schema"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn checks_own_then_each_source_once() {
        let sources = set(&["orders", "customers", "lwf"]);
        let got = to_check(Some("lwf"), &sources, &HashSet::new());
        assert_eq!(
            got,
            vec![
                ("lwf".to_string(), Role::Own),
                ("customers".to_string(), Role::Source),
                ("orders".to_string(), Role::Source),
            ]
        );
    }

    #[test]
    fn skips_schemas_already_checked() {
        let sources = set(&["orders", "customers"]);
        let checked: HashSet<String> = ["lwf".to_string(), "orders".to_string()].into();
        let got = to_check(Some("lwf"), &sources, &checked);
        assert_eq!(got, vec![("customers".to_string(), Role::Source)]);
    }

    #[test]
    fn no_own_schema_checks_sources_only() {
        let got = to_check(None, &set(&["orders"]), &HashSet::new());
        assert_eq!(got, vec![("orders".to_string(), Role::Source)]);
    }

    #[test]
    fn empty_sources_and_no_own_check_nothing() {
        assert!(to_check(None, &BTreeSet::new(), &HashSet::new()).is_empty());
    }

    #[test]
    fn filters_unset_check_nothing() {
        assert_eq!(filter_of("", "", "orders"), None);
        assert_eq!(filter_of(" , ", " , ", "orders"), None);
    }

    #[test]
    fn do_db_not_listing_the_schema_is_reported_as_do_db() {
        assert_eq!(filter_of("app", "", "orders"), Some(Filter::DoDb));
        assert_eq!(filter_of("app,other", "", "app"), None);
    }

    #[test]
    fn ignore_db_listing_the_schema_is_reported_as_ignore_db() {
        assert_eq!(filter_of("", "orders", "orders"), Some(Filter::IgnoreDb));
        assert_eq!(filter_of("", "other", "orders"), None);
    }

    #[test]
    fn both_filters_dropping_reports_do_db() {
        assert_eq!(filter_of("app", "orders", "orders"), Some(Filter::DoDb));
    }

    #[test]
    fn names_match_case_sensitively() {
        assert_eq!(filter_of("APP", "", "app"), Some(Filter::DoDb));
    }

    #[test]
    fn source_warning_names_schema_filter_and_consequence() {
        let w = warning(Role::Source, "orders", Filter::IgnoreDb);
        assert!(w.contains("`orders`"));
        assert!(w.contains("Binlog_Ignore_DB lists it"));
        assert!(w.contains("its triggers never fire"));
        let w = warning(Role::Source, "orders", Filter::DoDb);
        assert!(w.contains("Binlog_Do_DB does not list it"));
    }

    #[test]
    fn own_warning_names_schema_filter_and_consequence() {
        let w = warning(Role::Own, "lwf", Filter::DoDb);
        assert!(w.contains("own schema `lwf`"));
        assert!(w.contains("Binlog_Do_DB does not list it"));
        assert!(w.contains("cannot prove visibility"));
        assert!(!w.contains("triggers never fire"));
    }
}
