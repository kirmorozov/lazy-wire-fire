#!/usr/bin/env bash
# make lint: make a new panic site a CI failure instead of a surprise.
#
# Scans lazywf's own non-test src/ for panic-capable sites (`unwrap`/`expect`/`panic!`/
# `unreachable!`/indexing/slicing/… — the rules live in scripts/panic-sites.py) and compares
# them with scripts/panic-sites.allow, which lists every site that exists today and was
# reviewed. A site that appears without an allowlist entry FAILS: the pull request that adds
# it must justify it and add the entry --prune prints. An entry whose site was REMOVED only
# warns, so removing panic sites never touches this file:
#
#   scripts/check-panic-sites.sh            the check (make lint)
#   scripts/check-panic-sites.sh --prune    rewrite the allowlist from the current scan
#
# FAIL CLOSED: a gate that could not run must look like a gate that found a violation. A
# missing python3, a missing or malformed allowlist, a missing vendor/lazyview.lock, or a scan
# that found no files or no sites all exit non-zero — never "could not scan" as "nothing to
# report". scripts/test-check-panic-sites.sh (also in make lint) is the fixture test that
# proves the gate fails on a new site and cannot go vacuous.
#
# PANIC_SITES_ROOT / PANIC_SITES_ALLOW override the tree and the allowlist (fixture test).
set -euo pipefail
dir=$(cd "$(dirname "$0")" && pwd)
root=${PANIC_SITES_ROOT:-$(cd "$dir/.." && pwd)}
allow=${PANIC_SITES_ALLOW:-$dir/panic-sites.allow}
if ! command -v python3 >/dev/null 2>&1; then
  echo "check-panic-sites: python3 not found — the gate cannot run, and a gate that cannot" >&2
  echo "run must fail, not pass. Install python3." >&2
  exit 1
fi
exec python3 "$dir/panic-sites.py" --root "$root" --allow "$allow" "$@"
