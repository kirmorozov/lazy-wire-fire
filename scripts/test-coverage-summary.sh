#!/usr/bin/env bash
# Fixture test for the coverage summary (make lint runs it next to the other self-checks).
#
# Like scripts/test-check-panic-sites.sh, it builds a temporary tree — the SHIPPED scripts plus
# fixture sources plus a hand-built COV_JSON — and runs the shipped script against it, asserting
# the exact numbers rather than "the number went down":
#
#   an inline `#[cfg(test)] mod … { … }`, a gated fn/const/static/impl, `#[cfg(all(test, …))]`,
#     an attribute line between them, and the one-line form are all subtracted from that file's
#     own line/region/function counts — asserted as the exact excluded line set (--show-gated)
#     and as the exact own-code row;
#   the range boundary comes from brace matching over the MASKED source, so the fixtures put a
#     `"{"`, a `"}"`, a `'{'`, an escaped `\"` and a `// }` comment INSIDE the gated bodies: a
#     line-based scanner, or one that did not mask, would end those modules early;
#   a nested `#[cfg(test)]` inside a gated module is subtracted once, not twice;
#   lines and regions follow different per-item rules on purpose — lines follow every
#     count-carrying segment (llvm's own per-line rule, the one its lcov `DA:` records use),
#     regions only the segments that OPEN one, so a continuation segment on a gated line never
#     removes a second region;
#   the NEGATIVE cases: a file with no gated code, and a `#[cfg(test)] mod tests;` DECLARATION,
#     both report exactly the numbers llvm-cov gave, and `src/tests/` stays out of own code;
#   the vendored bucket is not ours to change: a vendored file keeps llvm-cov's numbers, tests
#     included;
#   FAIL CLOSED: an unreadable source, a missing scripts/panic-sites.py, a gated item whose
#     braces never close, a subtraction that would go negative and a missing lock all exit non-zero.
#
# --no-gated is the pre-fix behaviour, so the fixture proves, in permanent CI, that these numbers
# depend on the fix: it asserts BOTH rows, and a script that stopped subtracting goes red.
set -uo pipefail
dir=$(cd "$(dirname "$0")" && pwd)

pass=0
fail=0
OUT_FILE=
ok()  { pass=$((pass + 1)); }
bad() { fail=$((fail + 1)); echo "test-coverage-summary: FAIL: $1" >&2
        [[ -n $OUT_FILE ]] && sed 's/^/    /' "$OUT_FILE" >&2; }

# expect <case> <want-exit> <cmd…>: run cmd, capture combined output, check the exit code.
expect() {
  local case=$1 want=$2; shift 2
  OUT_FILE=$tmp/out
  local got=0
  "$@" >"$OUT_FILE" 2>&1 || got=$?
  if [[ $got != "$want" ]]; then
    bad "$case: exit $got, wanted $want"
    return 1
  fi
  ok
}
want_grep() { # want_grep <case> <pattern>: the last expect's output must contain it
  if grep -q -- "$2" "$OUT_FILE"; then ok; else bad "$1: output lacks /$2/"; fi
}
want_no_grep() { # want_no_grep <case> <pattern>: the last expect's output must NOT contain it
  if grep -q -- "$2" "$OUT_FILE"; then bad "$1: output contains /$2/, wanted none"; else ok; fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

root=
new_root() { # a fresh fixture tree: the shipped scripts, fixture sources, a vendored lock
  root=$tmp/root
  rm -rf "$root"; mkdir -p "$root/scripts" "$root/src/tests" "$root/vendor" "$root/cov"
  cp "$dir/coverage-summary.py" "$dir/panic-sites.py" "$root/scripts/"
  printf 'commit 0000000\nglob src/vendored/**\nfile src/vendored_copy.rs\n' > "$root/vendor/lazyview.lock"

  # the fixture that decides the boundary: a gated module and a gated fn whose bodies hold a
  # brace in a string, an escaped quote, a char literal and a `//` comment that looks like the
  # end of the block. The assertions below name these line numbers, so keep them in place.
  cat > "$root/src/widgets.rs" <<'EOF'
//! Orders and their items.

pub fn total(items: &[u32]) -> u32 {
    let mut sum = 0;
    for q in items {
        sum += *q;
    }
    sum
}

pub fn label(id: u64, note: &str) -> String {
    let open = "{";
    let close = "}";
    // a comment that looks like the end of a block: } { /*
    format!("order-{id}{open}{note}{close}")
}

#[cfg(test)]
mod tests {
    #[test]
    fn totals() {
        let open = "{";
        let close = "}";
        // }  this comment looks like a closing brace
        assert_eq!(super::total(&[1, 2]), 3);
        assert_ne!(open, close);
    }

    #[test]
    fn labels() {
        let s = "\"}";
        let c = '{';
        assert!(super::label(7, s).contains(&c.to_string()));
    }
}

#[cfg(test)]
fn helper() -> u32 {
    // a gated item whose comment holds a brace {
    let s = "}";
    total(&[2]) + s.len() as u32
}

pub fn last(items: &[u32]) -> Option<&u32> {
    items.last()
}
EOF

  # no gated code at all: the negative case, its numbers must not move
  cat > "$root/src/plain.rs" <<'EOF'
pub fn fee(qty: u32) -> u32 {
    qty * 2
}

pub fn due(qty: u32) -> u32 {
    qty + 1
}
EOF

  # the child-file case: a bare declaration whose body lives in src/tests/ — nothing to subtract
  cat > "$root/src/declonly.rs" <<'EOF'
pub fn ship(items: &[u32]) -> u32 {
    items.len() as u32
}

#[cfg(test)]
mod tests;
EOF

  # a #[cfg(test)] module inside a #[cfg(test)] module: subtracted once, not twice
  cat > "$root/src/nested.rs" <<'EOF'
pub fn fee(items: &[u32]) -> u32 {
    items.len() as u32
}

#[cfg(test)]
mod outer {
    #[cfg(test)]
    mod inner {
        fn b() -> u32 {
            let c = '{';
            1
        }
    }

    fn a() -> u32 {
        let s = "}";
        1
    }
}

pub fn due() -> u32 {
    0
}
EOF

  # every single-item form, an attribute line between them, #[cfg(all(test, …))] and the one-line mod
  cat > "$root/src/gated_items.rs" <<'EOF'
pub fn cart_total(items: &[u32]) -> u32 {
    items.iter().sum()
}

#[cfg(test)]
const FIXTURE: &[u32] = &[1, 2, 3];

#[cfg(test)]
static COUNT: u32 = 3;

#[cfg(test)]
impl Cart {
    fn sample() -> Self {
        let note = "a } in a string";
        Self {}
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn quiet() -> u32 {
    // } a brace in a comment
    0
}

#[cfg(all(test, feature = "audit"))]
fn audit(items: &[u32]) -> u32 {
    items.len() as u32
}

#[cfg(test)] mod one { fn t() {} }

pub fn empty() -> bool {
    true
}

pub fn is_empty(items: &[u32]) -> bool {
    items.is_empty()
}
EOF

  # a vendored file with an inline test module: the lock lists it, tests included
  cat > "$root/src/vendored_copy.rs" <<'EOF'
pub fn copied(items: &[u32]) -> u32 {
    items.len() as u32
}

#[cfg(test)]
mod tests {
    #[test]
    fn copied_works() {
        assert_eq!(super::copied(&[1]), 1);
    }
}
EOF

  # a region whose ENTRY opens on a production line and whose CONTINUATION segment carries a
  # count on a gated line: lines follow both (that is llvm's own per-line rule), regions follow
  # only the entry (the conservative rule — see region_delta)
  cat > "$root/src/cont.rs" <<'EOF'
pub fn fee(items: &[u32]) -> u32 {
    items.len() as u32
}

#[cfg(test)]
mod tests {
    fn helper() -> u32 {
        1
    }
}

pub fn due() -> u32 {
    0
}
EOF

  # own unit-test FILE: excluded as a whole file, before the fix and after it
  cat > "$root/src/tests/unit.rs" <<'EOF'
#[test]
fn a_whole_test_file() {
    assert_eq!(1 + 1, 2);
}
EOF

  # The hand-built llvm-cov export. A segment is [line, col, count, hasCount, isRegionEntry,
  # isGapRegion], and a segment that carries a count covers its line up to the next segment —
  # the per-line rule llvm-cov writes its own lcov `DA:` records with. Each region here opens on
  # its first statement line and closes on its last, so no reading of a boundary is ambiguous.
  #
  # widgets.rs   prod 3-9, 11-16 (covered) and 44-46 (never run); gated 22-26, 31-33, 38-42
  # plain.rs     prod 1-3 (covered), 5-7 (never run); no gated code
  # declonly.rs  prod 1-3 (covered); gated: only the `mod tests;` declaration at 5-6
  # nested.rs    prod 1-3 (covered), 21-23 (never run); gated 9-12 (covered) and 15-18 (covered)
  # gated_items  prod 1-3 (covered), 33-35 (covered), 37-39 (never run); gated 13-16, 21-24,
  #              27-29, 31 (all never run)
  cat > "$root/cov/main.json" <<EOF
{"data":[{"files":[
 {"filename":"$root/src/widgets.rs",
  "segments":[[3,1,5,1,1,0],[9,6,0,0,0,0],[11,1,4,1,1,0],[16,2,0,0,0,0],
              [22,1,2,1,1,0],[26,9,0,0,0,0],[31,1,0,1,1,0],[33,9,0,0,0,0],
              [38,1,0,1,1,0],[42,2,0,0,0,0],[44,1,0,1,1,0],[46,2,0,0,0,0]],
  "summary":{"lines":{"count":29,"covered":18},"regions":{"count":6,"covered":3},
             "functions":{"count":7,"covered":4}}},
 {"filename":"$root/src/plain.rs",
  "segments":[[1,1,2,1,1,0],[3,2,0,0,0,0],[5,1,0,1,1,0],[7,2,0,0,0,0]],
  "summary":{"lines":{"count":6,"covered":3},"regions":{"count":2,"covered":1},
             "functions":{"count":2,"covered":1}}},
 {"filename":"$root/src/declonly.rs",
  "segments":[[1,1,4,1,1,0],[3,2,0,0,0,0]],
  "summary":{"lines":{"count":3,"covered":3},"regions":{"count":1,"covered":1},
             "functions":{"count":1,"covered":1}}},
 {"filename":"$root/src/nested.rs",
  "segments":[[1,1,3,1,1,0],[3,2,0,0,0,0],[9,1,2,1,1,0],[12,10,0,0,0,0],
              [15,1,1,1,1,0],[18,6,0,0,0,0],[21,1,0,1,1,0],[23,2,0,0,0,0]],
  "summary":{"lines":{"count":14,"covered":11},"regions":{"count":4,"covered":3},
             "functions":{"count":4,"covered":3}}},
 {"filename":"$root/src/gated_items.rs",
  "segments":[[1,1,2,1,1,0],[3,2,0,0,0,0],[13,1,0,1,1,0],[16,6,0,0,0,0],
              [21,1,0,1,1,0],[24,6,0,0,0,0],[27,1,0,1,1,0],[29,2,0,0,0,0],
              [31,1,0,1,1,0],[31,24,0,0,0,0],[33,1,1,1,1,0],[35,2,0,0,0,0],
              [37,1,0,1,1,0],[39,2,0,0,0,0]],
  "summary":{"lines":{"count":21,"covered":6},"regions":{"count":7,"covered":2},
             "functions":{"count":7,"covered":2}}},
 {"filename":"$root/src/tests/unit.rs",
  "segments":[[2,1,1,1,1,0],[4,2,0,0,0,0]],
  "summary":{"lines":{"count":40,"covered":40},"regions":{"count":30,"covered":30},
             "functions":{"count":20,"covered":20}}},
 {"filename":"$root/src/vendored_copy.rs",
  "segments":[[1,1,9,1,1,0],[3,2,0,0,0,0]],
  "summary":{"lines":{"count":20,"covered":15},"regions":{"count":5,"covered":4},
             "functions":{"count":4,"covered":3}}}
],
 "functions":[
  {"name":"widgets::total","count":5,"filenames":["$root/src/widgets.rs"],"regions":[[3,1,9,6,5,1,1,0]]},
  {"name":"widgets::label","count":4,"filenames":["$root/src/widgets.rs"],"regions":[[11,1,16,2,4,1,1,0]]},
  {"name":"widgets::merge","count":3,"filenames":["$root/src/widgets.rs"],"regions":[[44,1,46,2,3,1,1,0]]},
  {"name":"widgets::merge","count":0,"filenames":["$root/src/widgets.rs"],"regions":[[44,1,46,2,0,1,1,0]]},
  {"name":"widgets::last","count":0,"filenames":["$root/src/widgets.rs"],"regions":[[45,1,46,2,0,1,1,0]]},
  {"name":"widgets::tests::totals","count":2,"filenames":["$root/src/widgets.rs"],"regions":[[22,1,26,9,2,1,1,0]]},
  {"name":"widgets::tests::labels","count":0,"filenames":["$root/src/widgets.rs"],"regions":[[31,1,33,9,0,1,1,0]]},
  {"name":"widgets::helper","count":0,"filenames":["$root/src/widgets.rs"],"regions":[[38,1,42,2,0,1,1,0]]},
  {"name":"plain::fee","count":2,"filenames":["$root/src/plain.rs"],"regions":[[1,1,3,2,2,1,1,0]]},
  {"name":"plain::due","count":0,"filenames":["$root/src/plain.rs"],"regions":[[5,1,7,2,0,1,1,0]]},
  {"name":"declonly::ship","count":4,"filenames":["$root/src/declonly.rs"],"regions":[[1,1,3,2,4,1,1,0]]},
  {"name":"nested::fee","count":3,"filenames":["$root/src/nested.rs"],"regions":[[1,1,3,2,3,1,1,0]]},
  {"name":"nested::outer::inner::b","count":2,"filenames":["$root/src/nested.rs"],"regions":[[9,1,12,10,2,1,1,0]]},
  {"name":"nested::outer::a","count":1,"filenames":["$root/src/nested.rs"],"regions":[[15,1,18,6,1,1,1,0]]},
  {"name":"nested::due","count":0,"filenames":["$root/src/nested.rs"],"regions":[[21,1,23,2,0,1,1,0]]},
  {"name":"gated::cart_total","count":2,"filenames":["$root/src/gated_items.rs"],"regions":[[1,1,3,2,2,1,1,0]]},
  {"name":"gated::sample","count":0,"filenames":["$root/src/gated_items.rs"],"regions":[[13,1,16,6,0,1,1,0]]},
  {"name":"gated::quiet","count":0,"filenames":["$root/src/gated_items.rs"],"regions":[[21,1,24,6,0,1,1,0]]},
  {"name":"gated::audit","count":0,"filenames":["$root/src/gated_items.rs"],"regions":[[27,1,29,2,0,1,1,0]]},
  {"name":"gated::one::t","count":0,"filenames":["$root/src/gated_items.rs"],"regions":[[31,1,31,24,0,1,1,0]]},
  {"name":"gated::empty","count":1,"filenames":["$root/src/gated_items.rs"],"regions":[[33,1,35,2,1,1,1,0]]},
  {"name":"gated::is_empty","count":0,"filenames":["$root/src/gated_items.rs"],"regions":[[37,1,39,2,0,1,1,0]]},
  {"name":"vendored_copy::copied","count":9,"filenames":["$root/src/vendored_copy.rs"],"regions":[[1,1,3,2,9,1,1,0]]},
  {"name":"vendored_copy::tests::copied_works","count":3,"filenames":["$root/src/vendored_copy.rs"],"regions":[[6,1,8,6,3,1,1,0]]}
]}]}
EOF

  # A COV_JSON of its own for the continuation case, so its numbers are asserted on their own:
  # region 7-10 OPENS on the gated line 7 (isRegionEntry) and its continuation segment at line 8
  # carries a count without opening a region. Lines follow both (llvm's own per-line rule, the one
  # its lcov `DA:` records use); regions follow the entry only, so exactly one region comes off
  # even though two count-carrying segments sit on gated lines.
  cat > "$root/cov/cont.json" <<EOF
{"data":[{"files":[
 {"filename":"$root/src/cont.rs",
  "segments":[[1,1,2,1,1,0],[3,2,0,0,0,0],[7,1,1,1,1,0],[8,6,3,1,0,0],[10,1,0,0,0,0],
              [12,1,0,1,1,0],[14,2,0,0,0,0]],
  "summary":{"lines":{"count":10,"covered":7},"regions":{"count":3,"covered":2},
             "functions":{"count":3,"covered":2}}}
],
 "functions":[
  {"name":"cont::fee","count":2,"filenames":["$root/src/cont.rs"],"regions":[[1,1,3,2,2,1,1,0]]},
  {"name":"cont::tests::helper","count":1,"filenames":["$root/src/cont.rs"],"regions":[[7,1,9,6,1,1,1,0]]},
  {"name":"cont::due","count":0,"filenames":["$root/src/cont.rs"],"regions":[[12,1,14,2,0,1,1,0]]}
]}]}
EOF

  # single-file exports, for the cases that must show one file's numbers on their own
  single() { # single <name> <src-file>
    python3 - "$root" "$1" "$2" <<'PY'
import json, sys
root, name, src = sys.argv[1], sys.argv[2], sys.argv[3]
main = json.load(open(f"{root}/cov/main.json"))["data"][0]
keep = [f for f in main["files"] if f["filename"] == f"{root}/{src}"]
fns = [f for f in main["functions"] if f["filenames"] == [f"{root}/{src}"]]
json.dump({"data": [{"files": keep, "functions": fns}]}, open(f"{root}/cov/{name}.json", "w"))
PY
  }
  single plain-only src/plain.rs
  single declonly-only src/declonly.rs
  single nested-only src/nested.rs
  single gated-items-only src/gated_items.rs
}

cov() { # cov <json> [flag…]: the shipped script, from the tree root, as `make coverage` runs it
  (cd "$root" && python3 scripts/coverage-summary.py "cov/$1.json" vendor/lazyview.lock "${@:2}")
}
same_fixed_and_raw() { # the table must not move at all, fix on or off
  local case=$1 json=$2
  expect "$case (fix on)" 0 cov "$json"
  cp "$OUT_FILE" "$tmp/on"
  expect "$case (fix off)" 0 cov "$json" --no-gated
  cp "$OUT_FILE" "$tmp/off"
  if cmp -s "$tmp/on" "$tmp/off"; then
    ok
  else
    bad "$case: the table moved although the file has nothing to subtract"
    OUT_FILE=$tmp/on
    sed 's/^/    on:  /' "$tmp/on" >&2
    sed 's/^/    off: /' "$tmp/off" >&2
  fi
}

# 1. the fixed table: the own row is production lines only — 40 counted / 28 covered, not the
#    73/41 llvm-cov reports (every inline test body), and not 80/68 either, because the own test
#    FILE src/tests/unit.rs (40/40) is still excluded whole.
new_root
expect "table" 0 cov main
want_grep "own row" "| lazywf's own code | 70.00% (28/40) | 63.64% (7/11) | 66.67% (8/12) |"

# 2. the same tree with the fix off: every inline test body counted as production. This is the
#    assertion that keeps case 1 from passing vacuously.
expect "no-gated row" 0 cov main --no-gated
want_grep "no-gated own row" "| lazywf's own code | 56.16% (41/73) | 50.00% (10/20) | 52.38% (11/21) |"

# 3. the exact excluded line set of every fixture file.
expect "gated ranges" 0 cov main --show-gated
want_grep "widgets ranges" "gated .*src/widgets.rs 18-35,37-42$"
want_grep "nested ranges" "gated .*src/nested.rs 5-19$"
want_grep "gated_items ranges" "gated .*src/gated_items.rs 5-6,8-9,11-17,19-24,26-29,31$"
want_grep "declonly ranges" "gated .*src/declonly.rs 5-6$"
want_no_grep "plain has no gated code" "src/plain.rs"

# 4. vendored is not ours to change: the lock's file keeps llvm-cov's numbers, inline tests and
#    all — its module sits at lines 5-10, exactly what would come off if it were own code.
want_grep "vendored row" '| `src/vendored_copy.rs` | 75.00% (15/20) | 80.00% (4/5) | 75.00% (3/4) |'
want_grep "total row" "| total | 71.67% (43/60) | 68.75% (11/16) | 68.75% (11/16) |"

# 5. the negative cases: no gated code at all, and a bare `mod tests;` declaration — with the
#    fix on and off the tables are byte-identical, and the numbers are llvm-cov's own.
same_fixed_and_raw "plain, no gated code" plain-only
same_fixed_and_raw "mod tests; declaration" declonly-only
expect "declonly row" 0 cov declonly-only
want_grep "declonly production numbers" "| lazywf's own code | 100.00% (3/3) | 100.00% (1/1) | 100.00% (1/1) |"

# 6. a nested #[cfg(test)] inside a gated module is taken out once: 6 lines left, not 14-8-8.
expect "nested row" 0 cov nested-only
want_grep "nested subtracted once" "| lazywf's own code | 50.00% (3/6) | 50.00% (1/2) | 50.00% (1/2) |"
expect "nested raw row" 0 cov nested-only --no-gated
want_grep "nested raw" "| lazywf's own code | 78.57% (11/14) | 75.00% (3/4) | 75.00% (3/4) |"

# 7. every single-item form at once: const, static, impl, an attribute line between them,
#    #[cfg(all(test, …))] and the one-line mod come off; the three production functions do not.
expect "gated_items row" 0 cov gated-items-only
want_grep "gated_items subtracted" "| lazywf's own code | 66.67% (6/9) | 66.67% (2/3) | 66.67% (2/3) |"

# 7b. the two per-item rules are different on purpose: a gated line holding a count-carrying
#     CONTINUATION segment takes its lines off but not a second region.
expect "continuation row" 0 cov cont
want_grep "continuation subtracted" "| lazywf's own code | 50.00% (3/6) | 50.00% (1/2) | 50.00% (1/2) |"
expect "continuation raw row" 0 cov cont --no-gated
want_grep "continuation raw" "| lazywf's own code | 70.00% (7/10) | 66.67% (2/3) | 66.67% (2/3) |"

# 8. fail closed: a report that could not measure the gated lines must not print a number.
new_root
rm "$root/src/widgets.rs"
expect "unreadable source fails" 1 cov main
want_grep "unreadable source fails" "no source for"
new_root
rm "$root/scripts/panic-sites.py"
expect "missing scanner fails" 1 cov main
want_grep "missing scanner fails" "panic-sites.py is missing"
new_root
rm "$root/vendor/lazyview.lock"
expect "missing lock fails" 1 cov main
want_grep "missing lock fails" "is missing"
new_root
printf '\n#[cfg(test)]\nmod unclosed {\n    fn a() {\n' > "$root/src/broken.rs"
python3 - "$root" <<'PY'
import json, sys
root = sys.argv[1]
main = json.load(open(f"{root}/cov/main.json"))
main["data"][0]["files"].append({
    "filename": f"{root}/src/broken.rs", "segments": [[3, 1, 1, 1, 1, 0], [4, 2, 0, 0, 0, 0]],
    "summary": {"lines": {"count": 4, "covered": 1}, "regions": {"count": 1, "covered": 1},
                "functions": {"count": 1, "covered": 1}}})
json.dump(main, open(f"{root}/cov/broken.json", "w"))
PY
expect "unterminated gated item fails" 1 cov broken
want_grep "unterminated gated item fails" "never closes"
new_root
python3 - "$root" <<'PY'
import json, sys
root = sys.argv[1]
main = json.load(open(f"{root}/cov/main.json"))
for f in main["data"][0]["files"]:  # a summary smaller than the gated lines it holds
    if f["filename"].endswith("src/widgets.rs"):
        f["summary"]["lines"] = {"count": 3, "covered": 3}
json.dump(main, open(f"{root}/cov/negative.json", "w"))
PY
expect "negative subtraction fails" 1 cov negative
want_grep "negative subtraction fails" "would go negative"
new_root
expect "unknown flag fails" 2 cov main --nonsense-flag

echo "test-coverage-summary: $pass passed, $fail failed"
(( fail == 0 )) || exit 1
