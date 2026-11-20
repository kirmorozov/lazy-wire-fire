//! The route cache (the hot-path audit's finding 8): a TableMap event for an already-routed
//! table is answered from the cache instead of rebuilding the trigger list, the `format!`ed
//! name and the flags. What a hit must still pass: the catalog's generation as the cheap
//! pre-check, and the layout's identity — `Arc::ptr_eq` against what the catalog hands out
//! for exactly this event's names — as the check that really decides. Every expectation here
//! is an `assert`, so the mutation checks (a key without the event's names, a probe without
//! the identity check) fail here and not with a panic somewhere else.

use super::*;
use crate::alloc_measure;
use crate::rows::ColKind;

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|n| n.to_string()).collect()
}

fn cols(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("c{i}")).collect()
}

fn layout(cols: &[&str]) -> Arc<Layout> {
    Arc::new(Layout::new(names(cols), vec![ColKind::Plain; cols.len()]))
}

fn watched(layout: Arc<Layout>) -> Arc<Watched> {
    Arc::new(Watched {
        layout,
        table: "shop.orders".into(),
        triggers: Vec::new(),
        any_full: false,
        any_on: On {
            insert: true,
            update: true,
            delete: true,
        },
    })
}

fn key() -> (String, String) {
    ("shop".into(), "orders".into())
}

/// A catalog answering `shop`.`orders` with `info`, the way `layout` leaves it after the
/// first read of the table.
fn catalog_with(info: &Arc<Layout>) -> Catalog {
    let mut cat = Catalog::default();
    cat.put("shop", "orders", info.clone());
    cat
}

/// The whole point of the cache: a repeat of a routed table is one hash lookup, and the hit
/// path allocates nothing of its own on a table whose names are not folded.
#[test]
fn a_routed_table_is_answered_from_the_cache_and_allocates_nothing() {
    let info = layout(&["id", "sku"]);
    let cat = catalog_with(&info);
    let w = watched(info.clone());
    let mut rc = RouteCache::default();
    rc.insert(key(), w.clone(), cat.generation());

    let k = key();
    let (hit, allocs) = alloc_measure::count(|| rc.probe(&cat, false, &k, None));
    assert!(
        matches!(&hit, Some(Route::Watched(r)) if Arc::ptr_eq(r, &w)),
        "the cached route, the very `Arc`"
    );
    assert_eq!(allocs, 0, "no list, no `format!`ed name, no flags rebuilt");

    // An empty cache is a miss, not a wrong answer.
    let empty = RouteCache::default();
    assert!(empty.probe(&cat, false, &key(), None).is_none());
    // And an entry for another table never answers this one.
    let mut rc = RouteCache::default();
    rc.insert(
        ("shop".into(), "items".into()),
        watched(info.clone()),
        cat.generation(),
    );
    assert!(rc.probe(&cat, false, &key(), None).is_none());
}

/// The two halves of the catalog never answer for each other through the route cache either:
/// a `Watched` built from the `information_schema` layout cannot answer a FULL event, and a
/// FULL-derived one cannot answer a plain event.
#[test]
fn a_plain_route_never_answers_a_full_event_and_a_full_route_never_a_plain_one() {
    let info = layout(&["id", "sku", "qty", "note"]);
    let mut cat = catalog_with(&info);
    let event = names(&["id", "sku", "qty", "note"]);
    let derived = cat.full_layout(&key(), &event, &info);
    let gen = cat.generation();

    let mut rc = RouteCache::default();
    rc.insert(key(), watched(derived.clone()), gen);
    assert!(
        matches!(
            rc.probe(&cat, false, &key(), Some(&event)),
            Some(Route::Watched(_))
        ),
        "the event that carries exactly these names is answered"
    );
    assert!(
        rc.probe(&cat, false, &key(), None).is_none(),
        "a plain event's columns are the `information_schema` ones, not this route's"
    );

    let mut rc = RouteCache::default();
    rc.insert(key(), watched(info.clone()), gen);
    assert!(matches!(
        rc.probe(&cat, false, &key(), None),
        Some(Route::Watched(_))
    ));
    assert!(
        rc.probe(&cat, false, &key(), Some(&event)).is_none(),
        "an `information_schema` route cannot answer an event with its own names — that is \
         the DDL-survival rule"
    );

    // Names nobody derived a layout for: a miss, never the plain route as a fallback.
    let unknown = names(&["id"]);
    assert!(rc.probe(&cat, false, &key(), Some(&unknown)).is_none());
}

/// Two name lists of one table are two routes that never share an `Arc` — and the number of
/// allocations a hit costs does not care whether the event names four columns or forty.
#[test]
fn two_lists_never_share_a_route_and_a_wide_event_costs_no_more_than_a_narrow_one() {
    let n4 = cols(4);
    let n40 = cols(40);
    let info = Arc::new(Layout::new(n40.clone(), vec![ColKind::Plain; 40]));
    let mut cat = Catalog::default();
    cat.put("shop", "orders", info.clone());
    let l4 = cat.full_layout(&key(), &n4, &info);
    let l40 = cat.full_layout(&key(), &n40, &info);
    assert!(
        !Arc::ptr_eq(&l4, &l40),
        "if the names left the key these two would be one `Arc`"
    );
    let gen = cat.generation();

    let mut rc = RouteCache::default();
    rc.insert(key(), watched(l4.clone()), gen);
    let k = key();
    let (hit, allocs) = alloc_measure::count(|| rc.probe(&cat, false, &k, Some(&n4)));
    assert!(matches!(&hit, Some(Route::Watched(r)) if Arc::ptr_eq(&r.layout, &l4)));
    assert_eq!(allocs, 0);
    assert!(
        rc.probe(&cat, false, &key(), Some(&n40)).is_none(),
        "the forty-column event is not answered by the four-column route"
    );

    rc.insert(key(), watched(l40.clone()), gen);
    let (hit, allocs_wide) = alloc_measure::count(|| rc.probe(&cat, false, &k, Some(&n40)));
    assert!(matches!(&hit, Some(Route::Watched(r)) if Arc::ptr_eq(&r.layout, &l40)));
    assert_eq!(
        allocs_wide, allocs,
        "a hit is the same few comparisons whatever the width of the event"
    );
    assert!(
        rc.probe(&cat, false, &key(), Some(&n4)).is_none(),
        "and the narrow event does not get the wide route"
    );
}

/// The counter being right is not enough: an entry stamped with the current generation but
/// holding a layout the catalog no longer hands out must still miss.
#[test]
fn a_matching_generation_alone_is_not_a_hit() {
    let stale = layout(&["id", "sku"]);
    let fresh = layout(&["id", "sku"]);
    let mut cat = Catalog::default();
    cat.put("shop", "orders", stale.clone());
    cat.put("shop", "orders", fresh.clone());

    let mut rc = RouteCache::default();
    rc.insert(key(), watched(stale.clone()), cat.generation());
    assert!(
        rc.probe(&cat, false, &key(), None).is_none(),
        "the generation matches; the identity check is what refuses"
    );
    rc.insert(key(), watched(fresh.clone()), cat.generation());
    assert!(rc.probe(&cat, false, &key(), None).is_some());
}

/// Everything that changes what a cached route holds drops it: the generation pre-check for
/// anything the catalog did, and the cache's own doors for what the engine itself changes.
#[test]
fn every_invalidator_drops_the_entries_it_must() {
    let info = layout(&["id"]);
    let mut cat = Catalog::default();
    cat.put("shop", "orders", info.clone());
    cat.put("shop", "items", info.clone());
    cat.put("other", "orders", info.clone());
    let mut rc = RouteCache::default();
    for t in ["orders", "items"] {
        rc.insert(
            ("shop".into(), t.into()),
            watched(info.clone()),
            cat.generation(),
        );
    }
    rc.insert(
        ("other".into(), "orders".into()),
        watched(info.clone()),
        cat.generation(),
    );
    for k in [
        key(),
        ("shop".into(), "items".into()),
        ("other".into(), "orders".into()),
    ] {
        assert!(rc.probe(&cat, false, &k, None).is_some(), "{k:?} routed");
    }

    // A catalog change anywhere — a re-read of one table, an invalidation of another — is a
    // miss for every entry, including the tables the change never touched: the counter, not
    // the caller's memory of what it invalidated, is the pre-check.
    cat.put("elsewhere", "things", info.clone());
    for k in [
        key(),
        ("shop".into(), "items".into()),
        ("other".into(), "orders".into()),
    ] {
        assert!(rc.probe(&cat, false, &k, None).is_none(), "{k:?} stale");
    }

    // The cache's own doors, restamped at the current generation.
    let gen = cat.generation();
    let mut rc = RouteCache::default();
    rc.insert(key(), watched(info.clone()), gen);
    rc.insert(("shop".into(), "items".into()), watched(info.clone()), gen);
    rc.insert(
        ("other".into(), "orders".into()),
        watched(info.clone()),
        gen,
    );

    // `forget_columns` takes the table with the catalog entry.
    rc.forget(&key());
    assert!(rc.probe(&cat, false, &key(), None).is_none());
    assert!(rc
        .probe(&cat, false, &("shop".into(), "items".into()), None)
        .is_some());

    // The `*` form `forget_columns` takes for a `DROP`/rename.
    rc.forget(&("shop".into(), "*".into()));
    assert!(rc
        .probe(&cat, false, &("shop".into(), "items".into()), None)
        .is_none());
    assert!(rc
        .probe(&cat, false, &("other".into(), "orders".into()), None)
        .is_some());

    // And everything out: `rebuild_routes` and `start_reset` go through this door.
    rc.clear();
    assert!(rc
        .probe(&cat, false, &("other".into(), "orders".into()), None)
        .is_none());
}

/// With `lower_names` the entry is stored folded (`norm`) and the lookup folds first; the
/// catalog keeps the event's raw names and is asked with them.
#[test]
fn a_folded_entry_answers_the_event_whose_case_it_folded_away() {
    let info = layout(&["id"]);
    let cat = {
        let mut cat = Catalog::default();
        cat.put("Shop", "Orders", info.clone());
        cat
    };
    let raw = ("Shop".to_string(), "Orders".to_string());
    let mut rc = RouteCache::default();
    rc.insert(
        ("shop".into(), "orders".into()),
        watched(info.clone()),
        cat.generation(),
    );
    assert!(
        rc.probe(&cat, true, &raw, None).is_some(),
        "the folded lookup finds the folded entry, the catalog is asked raw"
    );
    let (hit, allocs_folded) = alloc_measure::count(|| rc.probe(&cat, true, &raw, None));
    assert!(hit.is_some());
    assert_eq!(
        allocs_folded, 2,
        "with the folding on the hit pays exactly the two `String`s of the folded key — \
         nothing else"
    );
    assert!(
        rc.probe(&cat, false, &raw, None).is_none(),
        "unfolded, the entry is simply not there — and only the same flag gates both the \
         storing and this lookup, so that combination is not what the engine ever does"
    );
}
