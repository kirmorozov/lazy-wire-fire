#!/usr/bin/env python3
# The coverage summary of `make coverage` (the CI step summary reads it): lines, regions and
# functions for lazywf's own code and, separately, the vendored files listed in
# vendor/lazyview.lock, from the llvm-cov JSON export.
#
#   scripts/coverage-summary.py COV_JSON LOCK
#
# Two things are kept out of the own-code numbers, because a test is not production code
# (lazyview's convention 5):
#
#   * own unit-test FILES — everything under src/tests/, some of it reached through a
#     `#[path = "../tests/…"]` module, which llvm-cov records un-normalized;
#   * INLINE `#[cfg(test)]` code inside a production file: a module body (`#[cfg(test)]
#     mod tests { … }`) or a single gated item (`#[cfg(test)] fn helper() { … }`, `const`,
#     `static`, `impl`). llvm-cov records those lines under `src/http.rs`, `src/lock.rs` and so
#     on, so without this they count as production lines. The lines of a gated range come off
#     that one file's counts. A `#[cfg(test)] mod tests;` DECLARATION subtracts nothing: its
#     body is the first case, in src/tests/.
#
# The vendored section keeps whatever the lock lists, tests included — that copy is lazyview's
# and its numbers are meant to read the same as lazyview's own report.
#
# WHERE A GATED ITEM ENDS is decided by brace matching over the MASKED source: comment and
# string/char-literal contents are blanked first by scripts/panic-sites.py's `mask`, the very
# scanner the panic-site gate uses (exact masking, never a line-based heuristic), so a `"{"` in a
# string, a `}` inside a `//` comment or a `*/` inside a string cannot move the boundary. The
# `#[cfg(test)]` attribute pattern is the gate's too (`ATTR_RE`), so the two tools cannot disagree
# about what a gated item is; only its end anchor is relaxed, to also catch the one-line form
# `#[cfg(test)] mod t { … }` that rustfmt never writes.
#
# FAIL CLOSED, like the gate: a missing scripts/panic-sites.py, a source file the JSON names but
# the tree does not have, a gated item whose braces never close, or a subtraction larger than the
# summary it comes off all exit non-zero — a report that could not measure must not print a number
# it cannot support. scripts/test-coverage-summary.sh is the fixture test for that (make lint).
#
#   --no-gated     count inline #[cfg(test)] code as production lines (the pre-fix behaviour: the
#                  fixture test uses it to prove it can go red, and it is what to compare against)
#   --show-gated   print the subtracted line ranges per own file to stderr (fixture test, auditing)
import argparse
import importlib.util
import json
import os
import re
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


def die(msg):
    print("coverage-summary: ERROR: %s" % msg, file=sys.stderr)
    sys.exit(1)


def load_scanner():
    """The masking and the `#[cfg(test)]` attribute pattern, taken from scripts/panic-sites.py
    rather than reimplemented: two scanners that disagree about where a `mod` ends is how this
    class of tool rots. The module name has a hyphen, so it is loaded by path."""
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "panic-sites.py")
    if not os.path.isfile(path):
        die("%s is missing: the #[cfg(test)] ranges would have to be found by a second, weaker "
            "scanner, and the two would drift — refusing to report" % path)
    spec = importlib.util.spec_from_file_location("lwf_panic_sites", path)
    module = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True  # importing the gate's scanner must not drop a __pycache__ in scripts/
    spec.loader.exec_module(module)
    return module


SCANNER = load_scanner()


def oneline_form(pattern, name):
    """The same attribute pattern without its end anchor, so the one-line `#[cfg(test)] mod t {…}`
    form that rustfmt never writes is caught too. A pattern that no longer ends in `\\s*$` means
    scripts/panic-sites.py changed shape: say so instead of matching something else."""
    if not pattern.endswith(r"\s*$"):
        die("%s no longer ends in `\\s*$` — read scripts/panic-sites.py before changing this "
            "script" % name)
    return re.compile(pattern[: -len(r"\s*$")] + r"\s+\S")


# The gate's own pattern: `#[cfg(test)]` on its own line.
ATTR_RE = SCANNER.ATTR_RE
# …and the case its `[^)]*` cannot reach: `#[cfg(all(test, …))]` / `#[cfg(any(test, …))]` close
# twice before the `]`, which `[^)]*` will not cross — checked: ATTR_RE matches none of them. That
# is a safe gap for the gate, which then scans such an item as production code and errs towards
# FLAGGING a new panic site; it is not safe here, where the same miss counts a test body as a
# production line. Same words, same whole-line rule, parens counted rather than excluded — and
# `#[cfg(not(test))]` deliberately stays out, because that code is the production build's.
ALL_TEST_ATTR_RE = re.compile(
    r"^#\[cfg\((?:all|any)\((?:[^()]|\([^()]*\))*\btest\b(?:[^()]|\([^()]*\))*\)\)\]\s*$")
ONELINE_ATTR_RE = oneline_form(ATTR_RE.pattern, "ATTR_RE")
ONELINE_ALL_TEST_ATTR_RE = oneline_form(ALL_TEST_ATTR_RE.pattern, "ALL_TEST_ATTR_RE")


def is_cfg_test_attr(line, one_line=False):
    """A masked line that is a whole `#[cfg(test)]`-style attribute (one_line: or that attribute
    followed by its item, the one-line form)."""
    if one_line:
        return bool(ONELINE_ATTR_RE.match(line) or ONELINE_ALL_TEST_ATTR_RE.match(line))
    return bool(ATTR_RE.match(line) or ALL_TEST_ATTR_RE.match(line))


def matching_brace(masked, pos, path, attr_line):
    """The offset of the `}` that closes the `{` at `pos`, or a hard error: in masked Rust a brace
    that never closes means the masking missed a literal, and guessing from there would silently
    leave a whole test body counted as production code."""
    depth = 0
    i = pos
    while i < len(masked):
        c = masked[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    die("%s: the #[cfg(test)] item at line %d never closes: brace matching over the masked source "
        "ran to the end of the file — that is the masking missing a literal, not a quirk of code"
        % (path, attr_line))


def gated_lines(src, path):
    """The 1-based lines of every `#[cfg(test)]`-gated item of a source: each inline module body,
    each single gated item, and the attribute and doc lines that belong to them. The walk resumes
    AFTER a gated item, so a nested `#[cfg(test)]` inside one is never subtracted twice. `mask`
    has already blanked comments and string/char-literal contents, so the brace count below sees
    only real braces."""
    masked = SCANNER.mask(src)
    lines = masked.split("\n")
    starts = []
    off = 0
    for line in lines:
        starts.append(off)
        off += len(line) + 1
    gated = set()
    i = 0
    n = len(lines)
    while i < n:
        stripped = lines[i].strip()
        if is_cfg_test_attr(stripped):
            first = i
            j = i + 1
            # attribute lines and blank lines between the attribute and its item (the gate's walk)
            while j < n and (lines[j].strip() == "" or lines[j].strip().startswith("#[")):
                j += 1
        elif is_cfg_test_attr(stripped, one_line=True):
            first = i
            j = i
        else:
            i += 1
            continue
        if j >= n:
            break
        # the item ends at its `;` (a brace-less item: `mod tests;`, a gated `use`) or at the `}`
        # that closes its first `{`
        k = j
        last = None
        while k < n:
            opens = lines[k].find("{")
            semi = lines[k].find(";")
            if opens >= 0 and (semi < 0 or opens < semi):
                closed = matching_brace(masked, starts[k] + opens, path, first + 1)
                last = masked.count("\n", 0, closed)
                break
            if semi >= 0:
                last = k
                break
            k += 1
        if last is None:
            die("%s: the #[cfg(test)] item at line %d has neither a body nor a `;` before the end "
                "of the file: refusing to guess where it ends" % (path, first + 1))
        gated.update(range(first + 1, last + 2))  # 1-based, the closing line included
        i = last + 1
    return gated


def line_delta(segments, gated):
    """(counted, covered) lines llvm-cov attributes to the gated lines. The per-line rule is the
    one llvm-cov writes its own lcov `DA:` records with: a segment carrying a count covers its
    line up to the next segment; gap regions and count-less continuation segments carry no line.
    Checked against a real run of this project: it reproduces the `DA:` records of the lcov the
    same llvm-cov run exports, for every file."""
    counted = set()
    covered = set()
    for i in range(len(segments) - 1):
        line, _col, count, has_count, _entry, is_gap = segments[i]
        if not has_count or is_gap:
            continue
        for ln in range(line, segments[i + 1][0] + 1):
            if ln in gated:
                counted.add(ln)
                if count > 0:
                    covered.add(ln)
    return len(counted), len(covered)


def region_delta(segments, gated):
    """(count, covered) of the regions that OPEN on a gated line: one region per count-carrying,
    non-gap segment that is a region ENTRY. llvm-cov's per-file region summary cannot be rebuilt
    from the export (a real run's export shows 64339 entry segments where its own summaries add up
    to 65961 regions, and 7600 of the export's count-carrying segments are continuations inside an
    already-open region), so this is the closest attribution the JSON allows — and it is the
    conservative one: it only ever takes off regions the export itself shows opening on a gated
    line, so the residual leaves a test region counted rather than removing a production one."""
    count = covered = 0
    for segment in segments:
        line, _col, value, has_count, is_entry, is_gap = segment
        if not has_count or not is_entry or is_gap or line not in gated:
            continue
        count += 1
        if value > 0:
            covered += 1
    return count, covered


def function_delta(functions, filename, gated):
    """(count, covered) of the functions DEFINED on a gated line. llvm-cov lists every
    instantiation under data.functions, each with the file it was instantiated in, its count and
    the line its body starts on; the summary counts one function however often it was
    instantiated, so instantiations are deduped by name here."""
    seen = {}
    for fn in functions:
        name = fn.get("name")
        regions = fn.get("regions") or []
        if not name or not regions or filename not in (fn.get("filenames") or []):
            continue
        for region in regions:
            if region[0] in gated:
                seen[name] = seen.get(name, False) or fn.get("count", 0) > 0
                break
    return len(seen), sum(1 for covered in seen.values() if covered)


def source_text(filename, root):
    """The source of a file the JSON names: llvm-cov records the absolute path of the tree it
    compiled, which is the tree the report is for; a JSON copied from another checkout is read
    against the working tree instead. A file that cannot be read is a hard error — its gated
    lines would otherwise stay silently counted as production code."""
    for cand in (filename, *[os.path.join(root, filename[at + 1:])
                             for marker in ("/src/", "/tests/")
                             for at in (filename.find(marker),) if at >= 0]):
        if os.path.isfile(cand):
            with open(cand, encoding="utf-8") as fh:
                return fh.read()
    die("no source for %s: that file's #[cfg(test)] lines cannot be found, so they cannot be "
        "subtracted — run this from the tree the JSON was reported from" % filename)


def without_gated(f, functions, root):
    """A file entry with its inline `#[cfg(test)]` code taken out of its own-code summary, plus
    the line set that was taken out."""
    gated = gated_lines(source_text(f["filename"], root), f["filename"])
    summary = f["summary"]
    new = dict(summary)
    for kind, (cut, cut_covered) in (
            ("lines", line_delta(f.get("segments") or [], gated)),
            ("regions", region_delta(f.get("segments") or [], gated)),
            ("functions", function_delta(functions, f["filename"], gated))):
        s = summary[kind]
        if cut > s["count"] or cut_covered > s["covered"] or cut_covered > cut:
            die("%s: taking %d/%d %s off a summary of %d/%d would go negative — the scanner and "
                "llvm-cov disagree about this file, refusing to report"
                % (f["filename"], cut, cut_covered, kind, s["count"], s["covered"]))
        count = s["count"] - cut
        covered = s["covered"] - cut_covered
        new[kind] = {"count": count, "covered": covered,
                     "percent": 100.0 * covered / count if count else 0.0}
    out = dict(f)
    out["summary"] = new
    return out, gated


def ranges(lines):
    """A line set as `12-17,23`, for --show-gated."""
    out = []
    for line in sorted(lines):
        if out and line == out[-1][1] + 1:
            out[-1][1] = line
        else:
            out.append([line, line])
    return ",".join(str(a) if a == b else f"{a}-{b}" for a, b in out)


def main():
    ap = argparse.ArgumentParser(
        prog="coverage-summary.py",
        description="The own-code and vendored coverage table of `make coverage`.")
    ap.add_argument("cov_json", metavar="COV_JSON")
    ap.add_argument("lock", metavar="LOCK")
    ap.add_argument("--no-gated", action="store_true",
                    help="count inline #[cfg(test)] code as production lines (the pre-fix "
                         "behaviour: the fixture test uses it to prove it can go red)")
    ap.add_argument("--show-gated", action="store_true",
                    help="print the subtracted line ranges per own file to stderr")
    args = ap.parse_args()

    data = json.load(open(args.cov_json))["data"][0]
    if not os.path.isfile(args.lock):
        die("%s is missing: the vendored copy list would go unmasked, refusing to report"
            % args.lock)
    vendored = []
    for line in open(args.lock):
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "file":
            vendored.append(parts[1])
    root = os.getcwd()
    functions = data.get("functions") or []
    own, ven, shown = [], [], []
    for f in data["files"]:
        hit = next((v for v in vendored if f["filename"].endswith("/" + v)), None)
        if hit:
            ven.append(f)  # the lock's list, as llvm-cov reported it, inline tests included
        elif not is_own_test(f["filename"]):
            if args.no_gated:
                own.append(f)
            else:
                adjusted, gated = without_gated(f, functions, root)
                own.append(adjusted)
                if gated:
                    shown.append((f["filename"], gated))
    if args.show_gated:
        for filename, gated in shown:
            print("gated %s %s" % (filename, ranges(gated)), file=sys.stderr)
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
