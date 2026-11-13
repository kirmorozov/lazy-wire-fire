#!/usr/bin/env python3
# The coverage summary of `make coverage` (the CI step summary reads it): lines, regions and
# functions for lazywf's own code and, separately, the vendored files listed in
# vendor/lazyview.lock, from the llvm-cov JSON export.
#
#   scripts/coverage-summary.py COV_JSON LOCK
import json
import os
import sys


def cell(s):
    covered, count = s["covered"], s["count"]
    if count == 0:
        return "0.00% (0/0)"
    return f"{100.0 * covered / count:.2f}% ({covered}/{count})"


def aggregate(items):
    out = {}
    for kind in ("lines", "regions", "functions"):
        out[kind] = {
            "covered": sum(i["summary"][kind]["covered"] for i in items),
            "count": sum(i["summary"][kind]["count"] for i in items),
        }
    return out


def is_own_test(filename):
    """lazywf's own unit-test files (all of them under src/tests/, some reached through a
    `#[path = "../tests/…"]` module, which llvm-cov records un-normalized): test modules never
    count as production lines (lazyview's convention 5), so they are excluded from the own-code
    numbers. The vendored section keeps whatever the lock lists, tests included."""
    p = os.path.normpath(filename)
    return p.startswith("src/tests/") or "/src/tests/" in p


def main():
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} COV_JSON LOCK")
    data = json.load(open(sys.argv[1]))["data"][0]
    vendored = []
    for line in open(sys.argv[2]):
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "file":
            vendored.append(parts[1])
    own, ven = [], []
    for f in data["files"]:
        hit = next((v for v in vendored if f["filename"].endswith("/" + v)), None)
        if hit:
            ven.append(f)
        elif not is_own_test(f["filename"]):
            own.append(f)
    print("| scope | lines | regions | functions |")
    print("|---|---:|---:|---:|")
    for label, items in (("lazywf's own code", own),
                         (f"vendored lazyview files ({len(vendored)})", ven),
                         ("total", own + ven)):
        a = aggregate(items)
        print(f"| {label} | " + " | ".join(cell(a[k]) for k in ("lines", "regions", "functions")) + " |")
    print()
    print("| vendored file | lines | regions | functions |")
    print("|---|---:|---:|---:|")
    for v in vendored:
        f = next((x for x in ven if x["filename"].endswith("/" + v)), None)
        if f is None:
            print(f"| `{v}` | not in the coverage data | | |")
            continue
        print(f"| `{v}` | " + " | ".join(cell(f["summary"][k]) for k in ("lines", "regions", "functions")) + " |")


if __name__ == "__main__":
    main()
