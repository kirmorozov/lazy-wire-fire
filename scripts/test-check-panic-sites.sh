#!/usr/bin/env bash
# Fixture test for the panic-site gate (make lint runs it next to the check itself).
#
# It builds a temporary tree with fixture sources and runs the SHIPPED scripts
# (scripts/check-panic-sites.sh and scripts/panic-sites.py are copied into it) against it,
# proving the gate's contract — above all that it FAILS when it must, because a gate that
# cannot go vacuous is the whole point:
#
#   a new `unwrap` fails the check        an unchanged tree passes
#   a removed site passes with a warning  moved code (a site pushed to another line by an
#                                         unrelated edit above it) passes — entries are keyed
#                                         by content, never by line number
#   missing/empty/malformed allowlist, missing lock, a crashed scanner all FAIL (fail closed)
#   vendored files, src/tests/, *_tests.rs, #[cfg(test)] modules, comments and string
#   literals are not own code and never count as sites
set -uo pipefail
dir=$(cd "$(dirname "$0")" && pwd)

pass=0
fail=0
OUT_FILE=
ok()  { pass=$((pass + 1)); }
bad() { fail=$((fail + 1)); echo "test-check-panic-sites: FAIL: $1" >&2
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
  grep -q "$2" "$OUT_FILE" || bad "$1: output lacks /$2/"
}
want_nowarn() {
  grep -qi "warn" "$OUT_FILE" && bad "$1: output has a warning, wanted none"
  return 0
}
want_no_grep() { # want_no_grep <case> <pattern>: the last expect's output must NOT contain it
  grep -q "$2" "$OUT_FILE" && bad "$1: output contains /$2/, wanted none"
  return 0
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

new_root() { # a fresh fixture tree: the shipped scripts, fixture sources, a vendored lock
  root=$tmp/root
  rm -rf "$root"; mkdir -p "$root/scripts" "$root/src/tests" "$root/src/vendored" "$root/vendor"
  cp "$dir/check-panic-sites.sh" "$dir/panic-sites.py" "$root/scripts/"
  printf 'commit 0000000\nglob src/vendored/**\nglob src/vendored.rs\n' > "$root/vendor/lazyview.lock"
  cat > "$root/src/good.rs" <<'EOF'
// the `unwrap()` in this comment is not a site
fn sum(orders: &[u64]) -> u64 {
    let note = "the unwrap() inside a string is not a site";
    let _ = note;
    let a = orders[0];
    let b = orders.get(1).unwrap();
    let d = orders.len() as u8;
    a + b + d as u64
}

fn twice(orders: &[u64]) -> u64 {
    let a = orders[0];
    let b = orders.get(1).unwrap();
    a + b
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_test_unwrap_is_not_a_site() {
        let _ = Some(1).unwrap();
        assert!(true);
        panic!("nor is this");
    }
}
EOF
  printf 'fn v() -> u64 {\n    let _ = Some(1).unwrap();\n    [1][0]\n}\n' > "$root/src/vendored/keep.rs"
  printf 'fn v() -> u64 {\n    Some(1).unwrap()\n}\n' > "$root/src/vendored.rs"
  printf 'fn t() { let _ = Some(1).unwrap(); }\n' > "$root/src/tests/inside.rs"
  printf 'fn t() { let _ = Some(1).unwrap(); }\n' > "$root/src/fixture_tests.rs"
  export PANIC_SITES_ROOT=$root
  export PANIC_SITES_ALLOW=$root/scripts/panic-sites.allow
}
check() { "$root/scripts/check-panic-sites.sh" "$@"; }

# 1–2. the seed pass: --prune writes the allowlist, the unchanged tree then passes clean.
new_root
expect "seed --prune" 0 check --prune
want_grep "seed --prune" "wrote .* entries"
expect "unchanged passes" 0 check
want_grep "unchanged passes" "ok — .* sites"
want_nowarn "unchanged passes"

# 3. (the mutation test) a brand-new unwrap in a new file FAILS the check, printing the
#    file:line, the text and the exact allowlist line to add.
printf 'fn s() -> u64 {\n    let items = vec![7_u64];\n    *items.first().unwrap()\n}\n' > "$root/src/scratch.rs"
expect "new unwrap fails" 1 check
want_grep "new unwrap fails" "NEW site not allowlisted"
want_grep "new unwrap fails" "src/scratch.rs:3"
want_grep "new unwrap fails" "items.first"

# 4. allowlisting that one site (the line the failure printed) makes it pass again.
printf 'src/scratch.rs\t*items.first().unwrap()\n' >> "$root/scripts/panic-sites.allow"
expect "new unwrap allowlisted passes" 0 check

# 5. a site moved to another line by an unrelated edit ABOVE it still passes: entries are
#    keyed by content, never by line numbers.
sed -i 's|fn s() -> u64 {|fn s() -> u64 {\n    // three unrelated lines above the site,\n    // added by someone who never touched it:\n    // the gate must not care|' "$root/src/scratch.rs"
expect "moved site passes" 0 check
want_nowarn "moved site passes"

# 6. a removed site: exit 0 with a warning naming --prune; --prune really drops the entry.
cat > "$root/src/good.rs" <<'EOF'
fn sum(orders: &[u64]) -> u64 {
    let a = orders[0];
    a
}

fn twice(orders: &[u64]) -> u64 {
    let a = orders[0];
    let b = orders.get(1).unwrap();
    a + b
}
EOF
expect "removed site warns and passes" 0 check
want_grep "removed site warns and passes" "WARNING"
want_grep "removed site warns and passes" "prune"
expect "prune after removal" 0 check --prune
expect "green after prune" 0 check
want_nowarn "green after prune"

# 7. one more occurrence of an already-allowlisted line: the recorded count catches it.
printf '\nfn third(orders: &[u64]) -> u64 {\n    let b = orders.get(1).unwrap();\n    b\n}\n' >> "$root/src/good.rs"
expect "extra occurrence fails" 1 check
want_grep "extra occurrence fails" "NEW occurrence"

# 8. fail closed: every way of not scanning must fail, never pass — missing allowlist, empty
#    allowlist, malformed entries, missing lock, a crashed scanner.
new_root
check --prune >/dev/null 2>&1
rm "$PANIC_SITES_ALLOW"
expect "missing allowlist fails" 1 check
want_grep "missing allowlist fails" "missing"
: > "$PANIC_SITES_ALLOW"
expect "empty allowlist fails" 1 check
printf '# only a header\n' > "$PANIC_SITES_ALLOW"
expect "header-only allowlist fails" 1 check
printf 'src/good.rs\tthis entry has no tab\n' >> "$PANIC_SITES_ALLOW"
expect "malformed entry fails" 1 check
printf 'src/good.rs\tlet a = orders[0];\tnotanumber\n' >> "$PANIC_SITES_ALLOW"
expect "bad count fails" 1 check
printf 'src/good.rs\tlet a  = orders[0];\n' >> "$PANIC_SITES_ALLOW" # not whitespace-normalized
expect "unnormalized entry fails" 1 check
rm "$root/vendor/lazyview.lock"
expect "missing lock fails" 1 check
new_root
printf 'raise RuntimeError("the scanner crashed")\n' >> "$root/scripts/panic-sites.py"
expect "crashed scanner fails" 1 check

# 9. non-vacuous the other way too: vendored files, src/tests/, *_tests.rs, #[cfg(test)]
#    modules, comments and strings are not own sites — the seed tree holds one of each; the
#    seeded allowlist covers the real sites and none of the excluded ones.
new_root
OUT_FILE=$tmp/out
check --prune >"$OUT_FILE" 2>&1
want_grep "excluded seeded out" "wrote 4 entries"
expect "excluded code passes" 0 check
want_grep "excluded code passes" "ok —"
OUT_FILE=$PANIC_SITES_ALLOW
want_no_grep "excluded code passes" "vendored"
want_no_grep "excluded code passes" "tests/inside"
want_no_grep "excluded code passes" "fixture_tests"
want_no_grep "excluded code passes" "Some(1).unwrap"
want_no_grep "excluded code passes" "assert!(true)"
want_no_grep "excluded code passes" "panic!"

echo "test-check-panic-sites: $pass passed, $fail failed"
(( fail == 0 )) || exit 1
