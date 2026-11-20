#!/usr/bin/env python3
"""The panic-site gate behind scripts/check-panic-sites.sh: scan own non-test src/ for
panic-capable sites and compare them with scripts/panic-sites.allow.

A site is a piece of code that can panic at run time: `unwrap`/`expect`/`panic!`/`unreachable!`/
`todo!`/`unimplemented!`/`assert!`/`debug_assert!`/`transmute`/`expect_err`/`unwrap_err`, an
`as` cast between integer types, or an indexing/slicing `[..]`. The rules and the masking are
the panic audit's scanner: exact comment and string/char-literal masking (never a line-based
heuristic, so a word inside a comment or a log string is not a site), then blanking of every
`#[cfg(test)]` item, then the site rules. Vendored files (the `glob` lines of
vendor/lazyview.lock) and test paths (src/tests/, tests.rs, *_tests.rs) are not own code and
are not scanned.

Entries in the allowlist are keyed by CONTENT, never by line number, so moving code around or
editing unrelated lines can never fail the check:

    <path>\t<whitespace-collapsed source text of the site's line>[\t<count>]

`count` is written only when a file holds the same text on more than one line. The gate:

  * a site that is not allowlisted (or occurs more often than allowlisted) FAILS (exit 1),
    printing file:line, the text and the exact entry to add — a new panic site must be
    justified in the pull request that adds it;
  * an allowlist entry whose site no longer exists only WARNS (exit 0), so a change that
    REMOVES panic sites never has to edit this file and never conflicts with it;
    `scripts/check-panic-sites.sh --prune` drops the dead entries.

FAIL CLOSED: this tool is the thing `make lint` trusts, so every way of not scanning is an
error, never a pass. A missing or unparseable lock, a missing/empty/malformed allowlist, a
scan of zero files or zero sites, or a crashed scanner all exit non-zero. A gate that could
not run must look exactly like a gate that found a violation.
"""
import argparse
import fnmatch
import os
import re
import sys
from collections import Counter

NUMERIC = {
    "i8", "i16", "i32", "i64", "i128", "isize",
    "u8", "u16", "u32", "u64", "u128", "usize",
    "f32", "f64",
}


def die(msg):
    print("panic-sites: ERROR: %s" % msg, file=sys.stderr)
    sys.exit(1)


def vendored_globs(lock):
    """The `glob <pattern>` lines of vendor/lazyview.lock — the vendored copy list. The lock is
    written by scripts/sync-lazyview.sh; a missing lock is a broken tree, not an empty list."""
    if not os.path.isfile(lock):
        die("%s is missing: the vendored copy list would go unmasked, refusing to scan" % lock)
    globs = []
    with open(lock, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("glob "):
                globs.append(line[5:].strip())
    return globs


def is_vendored(path, globs):
    for g in globs:
        if fnmatch.fnmatch(path, g):
            return True
    return False


def is_test_path(path):
    base = os.path.basename(path)
    return (
        base == "tests.rs"
        or base.endswith("_tests.rs")
        or "/tests/" in "/" + path
        or path.startswith("tests/")
    )


def mask(src):
    """Blank out comments and string/char literal contents; keep length & newlines exactly."""
    out = []
    i = 0
    n = len(src)
    while i < n:
        c = src[i]
        if c == "/" and src.startswith("//", i):
            while i < n and src[i] != "\n":
                out.append(" ")
                i += 1
            continue
        if c == "/" and src.startswith("/*", i):
            depth = 0
            while i < n:
                if src.startswith("/*", i):
                    depth += 1
                    out.append("  ")
                    i += 2
                    continue
                if src.startswith("*/", i):
                    depth -= 1
                    out.append("  ")
                    i += 2
                    if depth == 0:
                        break
                    continue
                out.append("\n" if src[i] == "\n" else " ")
                i += 1
            continue
        m = re.match(r'(?:b?r|rb?)(#*)"', src[i:])
        if m:
            hashes = m.group(1)
            close = '"' + hashes
            out.append(" " * m.end())
            j = i + m.end()
            while j < n:
                if src.startswith(close, j):
                    out.append('"' + hashes)
                    j += 1 + len(hashes)
                    break
                out.append("\n" if src[j] == "\n" else " ")
                j += 1
            i = j
            continue
        if c == '"' or (c == "b" and src[i + 1: i + 2] == '"'):
            j = i + (2 if c == "b" else 1)
            out.append(" " * (j - i))
            while j < n:
                ch = src[j]
                if ch == "\\":
                    if src[j + 1: j + 2] == "\n":  # line continuation keeps the newline
                        out.append(" \n")
                    else:
                        out.append("  ")
                    j += 2
                    continue
                if ch == '"':
                    out.append('"')
                    j += 1
                    break
                out.append("\n" if ch == "\n" else " ")
                j += 1
            i = j
            continue
        if c == "'":
            m = re.match(r"'(?:\\.|[^\\'])'", src[i:])
            if m:
                out.append(" " * (m.end() - 1))
                out.append("'")
                i += m.end()
                continue
            out.append(c)
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


ATTR_RE = re.compile(r"^#\[cfg\((?:test|all\([^)]*\btest\b)[^)]*\)\]\s*$")


def strip_cfg_test(masked_lines):
    """Blank out the text of `#[cfg(test)]` items (incl. nested modules)."""
    res = list(masked_lines)
    i = 0
    while i < len(res):
        s = res[i].strip()
        if ATTR_RE.match(s):
            j = i
            # blank the attribute line(s) and blank lines between attributes
            while j < len(res) and (res[j].strip() == "" or res[j].strip().startswith("#[")):
                res[j] = " " * len(res[j])
                j += 1
            if j >= len(res):
                i = j
                continue
            # find the item's opening brace; a `;` first means a brace-less item
            k = j
            while k < len(res):
                if "{" in res[k]:
                    break
                if ";" in res[k]:
                    res[k] = " " * len(res[k])
                    k += 1
                    break
                res[k] = " " * len(res[k])
                k += 1
            else:
                i = k
                continue
            if k < len(res) and "{" in res[k]:
                depth = 0
                started = False
                m = k
                while m < len(res):
                    line = res[m]
                    for ch in line:
                        if ch == "{":
                            depth += 1
                            started = True
                        elif ch == "}":
                            depth -= 1
                    res[m] = " " * len(line)
                    if started and depth <= 0:
                        break
                    m += 1
                i = m + 1
                continue
            i = k
            continue
        i += 1
    return res


PATTERNS = [
    re.compile(r"\.unwrap\s*\("),
    re.compile(r"\.expect\s*\("),
    re.compile(r"\bpanic!\s*\("),
    re.compile(r"\bunreachable!\s*\("),
    re.compile(r"\btodo!\s*\("),
    re.compile(r"\bunimplemented!\s*\("),
    re.compile(r"\bassert(!|_eq!|_ne!|_matches!)\s*\("),
    re.compile(r"\bdebug_assert(!|_eq!|_ne!)?\s*\("),
    re.compile(r"\btransmute\s*[:(<]|\bfrom_raw_parts\b|\bfrom_u8_unchecked\b"),
    re.compile(r"\.expect_err\s*\(|\.unwrap_err\s*\("),
]

CAST_RE = re.compile(r"\bas\s+(?:[A-Za-z_][A-Za-z0-9_]*::)*([a-z0-9_]+)\b")

# a keyword right before the bracket means a pattern/type/array-literal, not an index
KEYWORDS_BEFORE_BRACKET = {
    "in", "let", "return", "move", "impl", "static", "const", "as", "where",
    "else", "match", "if", "for", "while", "type", "use", "pub", "mut", "ref",
    "then", "yield", "await", "box", "typeof",
}


def line_is_site(code, masked, idx):
    """True when the masked source line at idx can panic: a pattern, an integer `as`, or an
    index/slice. Attribute and doc lines are skipped; comments and string contents are already
    blanked, so a word inside them is never a site."""
    stripped = code.strip()
    if stripped.startswith("#[") or stripped.startswith("//!") or stripped.startswith("///"):
        return False
    # Every pattern counts. The audit's scanner skipped a line's `expect` rows when the same
    # line also held `expect_err`/`expect_some`/`expect_ok`; this gate keeps counting it —
    # erring towards flagging, never towards silence.
    for rx in PATTERNS:
        if rx.search(code):
            return True
    for m in CAST_RE.finditer(code):
        if m.group(1) in NUMERIC:
            return True
    for m in re.finditer(r"\[", code):
        p = m.start()
        q = p - 1
        while q >= 0 and code[q] == " ":
            q -= 1
        prev = code[q] if q >= 0 else None
        prev2 = code[q - 1] if q >= 1 else None
        if prev is None:
            # look back on previous code lines
            k = idx - 1
            while k >= 0 and not masked[k].strip():
                k -= 1
            if k >= 0:
                t = masked[k].rstrip()
                prev = t[-1] if t else None
                prev2 = t[-2] if len(t) > 1 else None
        if prev is None:
            continue
        if prev == "#":
            continue
        if prev == ">" and prev2 == "=":
            continue  # `=> [..]` array literal in match arm
        if prev == "!":
            continue  # macro like vec![..]
        if not (prev.isalnum() or prev in ")]_"):
            continue
        wq = q
        while wq >= 0 and (code[wq].isalnum() or code[wq] == "_"):
            wq -= 1
        word = code[wq + 1:q + 1]
        if word in KEYWORDS_BEFORE_BRACKET:
            continue
        # find matching ]
        depth = 0
        r = p
        close = -1
        while r < len(code):
            if code[r] == "[":
                depth += 1
            elif code[r] == "]":
                depth -= 1
                if depth == 0:
                    close = r
                    break
            r += 1
        if close == -1:
            # multi-line index; take to end of line
            close = len(code) - 1
        inner = code[p + 1:close].strip()
        if inner == "":
            continue
        # type-ish noise: `[u8; 4]`, `[(); N]`, `[0; N]`. The audit's scanner also skipped a
        # bare identifier (`cols[i]`) here, which hid every `v[i]` site from it — clippy's
        # indexing lint caught those, and `make lint` does not enable that lint. This gate is
        # the only thing between a new `customers[i]` and silence, so a `[i]` with no `;`
        # counts, and the keyword look-ahead above is what keeps `let [a, b]` and friends out.
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\s*;.*", inner):
            continue
        if re.fullmatch(r"\d+\s*;\s*[A-Za-z0-9_]+", inner):
            continue
        return True
    return False


def normalize(text):
    """The allowlist key form: whitespace collapsed to single spaces, trimmed."""
    return " ".join(text.split())


def scan(root):
    """Every own non-test site: a list of (path, line, normalized_text), one per site LINE
    (a line holding several sites is one site — the line is what the allowlist keys)."""
    src = os.path.join(root, "src")
    if not os.path.isdir(src):
        die("no src/ under %s: refusing to pass a tree it cannot scan" % root)
    globs = vendored_globs(os.path.join(root, "vendor", "lazyview.lock"))
    files = []
    for dirpath, dirnames, filenames in os.walk(src):
        dirnames[:] = [d for d in dirnames if d != "tests"]
        for f in sorted(filenames):
            if not f.endswith(".rs"):
                continue
            rel = os.path.relpath(os.path.join(dirpath, f), root)
            if is_vendored(rel, globs) or is_test_path(rel):
                continue
            files.append(rel)
    files.sort()
    if not files:
        die("the scan found no own .rs files under %s: refusing to pass a broken scan" % src)
    sites = []
    for rel in files:
        with open(os.path.join(root, rel), encoding="utf-8") as fh:
            raw = fh.read()
        lines_raw = raw.split("\n")
        masked = strip_cfg_test(mask(raw).split("\n"))
        for idx, code in enumerate(masked):
            if not line_is_site(code, masked, idx):
                continue
            lineno = idx + 1
            text = normalize(lines_raw[idx] if idx < len(lines_raw) else "")
            if text == "":
                # a site line whose raw text is empty cannot exist (blanked lines do not match
                # any rule); zero-length keys would match each other — refuse instead.
                die("%s:%d: site line normalizes to empty text" % (rel, lineno))
            sites.append((rel, lineno, text))
    if not sites:
        die("the scan found no sites in %s: a tree this size with zero panic sites means the "
            "scanner is broken, not clean — refusing to pass" % src)
    return sites, len(files)


HEADER = """\
# panic-sites.allow — every panic-capable site in lazywf's own non-test src/, as of the last
# review. Written by `scripts/check-panic-sites.sh --prune`; add a line by hand only when a
# pull request justifies a new site in its description.
#
# One entry per line, tab-separated, matched by CONTENT (never by line number, so moving code
# or editing unrelated lines never fails the check):
#
#     <path>\t<whitespace-collapsed source text of the site's line>[\t<count>]
#
# `count` is written only when the same text occurs several times in the file. A site that is
# not here fails `make lint`; an entry whose site no longer exists only warns, and
# `scripts/check-panic-sites.sh --prune` drops it.
"""


def parse_allow(path):
    """The allowlist as {(path, text): count}, refusing to trust a file it cannot read exactly:
    a missing file, no entries, a malformed or un-normalized line, or a duplicate entry are
    all errors (fail closed — an entry that parses to nothing must never be skipped)."""
    if not os.path.isfile(path):
        die("%s is missing: every site would pass the gate, refusing to pass" % path)
    entries = {}
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.rstrip("\n")
            if line == "" or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) not in (2, 3):
                die("%s:%d: expected <path>\\t<text>[\\t<count>], got %d tab-separated field(s)"
                    " — fix it or run scripts/check-panic-sites.sh --prune" % (path, n, len(fields)))
            f, text = fields[0], fields[1]
            count = 1
            if len(fields) == 3:
                if not fields[2].isdigit() or int(fields[2]) < 1:
                    die("%s:%d: count %r is not a positive integer" % (path, n, fields[2]))
                count = int(fields[2])
            if not f.startswith("src/") or not f.endswith(".rs"):
                die("%s:%d: path %r is not an own src file" % (path, n, f))
            if normalize(text) != text or text == "":
                die("%s:%d: site text is empty or not whitespace-normalized — run "
                    "scripts/check-panic-sites.sh --prune" % (path, n))
            if (f, text) in entries:
                die("%s:%d: duplicate entry for (%s, %r)" % (path, n, f, text))
            entries[(f, text)] = count
    if not entries:
        die("%s holds no entries: the gate would pass every new site, refusing to pass" % path)
    return entries


def entry_line(f, text, count):
    return "%s\t%s" % (f, text) if count <= 1 else "%s\t%s\t%d" % (f, text, count)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(prog="panic-sites.py", description=__doc__.split("\n")[0],
                                 add_help=True)
    ap.add_argument("--root", default=os.path.dirname(here),
                    help="repository root (default: the parent of scripts/)")
    ap.add_argument("--allow", default=os.path.join(here, "panic-sites.allow"),
                    help="allowlist file (default: scripts/panic-sites.allow)")
    ap.add_argument("--prune", action="store_true",
                    help="rewrite the allowlist from the current scan: drop entries whose site "
                         "is gone, fix counts, add sites (every added line must be justified in "
                         "the pull request's description)")
    args = ap.parse_args()

    sites, nfiles = scan(args.root)
    groups = Counter((f, t) for f, _, t in sites)
    lines = {}
    for f, ln, t in sites:
        lines.setdefault((f, t), []).append(ln)

    if args.prune:
        with open(args.allow, "w", encoding="utf-8") as fh:
            fh.write(HEADER)
            for (f, t), n in sorted(groups.items()):
                fh.write(entry_line(f, t, n) + "\n")
        print("panic-sites: wrote %d entries (%d sites in %d files) to %s"
              % (len(groups), len(sites), nfiles, args.allow))
        return 0

    entries = parse_allow(args.allow)
    failures = []
    warnings = []
    for (f, t), n in sorted(groups.items()):
        allowlisted = entries.get((f, t))
        if allowlisted is None:
            failures.append("NEW site not allowlisted: %s:%d\n    %s\n  allowlist line to add:\n    %s"
                            % (f, lines[(f, t)][0], t, entry_line(f, t, n)))
        elif n > allowlisted:
            where = ", ".join("%s:%d" % (f, ln) for ln in lines[(f, t)])
            failures.append(
                "NEW occurrence of an allowlisted site (%d seen, %d allowlisted): %s\n"
                "    %s\n  allowlist line after the change would be:\n    %s"
                % (n, allowlisted, where, t, entry_line(f, t, n)))
    for (f, t), m in sorted(entries.items()):
        n = groups.get((f, t), 0)
        if n == 0:
            warnings.append("entry matches no site any more (%s): %s" % (f, t))
        elif n < m:
            warnings.append("entry counts %d, only %d seen (%s): %s" % (m, n, f, t))

    if warnings:
        print("panic-sites: WARNING: %d stale allowlist entr%s:"
              % (len(warnings), "y" if len(warnings) == 1 else "ies"), file=sys.stderr)
        for w in warnings:
            print("  " + w, file=sys.stderr)
        print("  prune: scripts/check-panic-sites.sh --prune", file=sys.stderr)
    if failures:
        print("panic-sites: FAIL: %d new site(s) — every new panic site must be justified in "
              "the pull request that adds it, and its line added to %s:"
              % (len(failures), args.allow), file=sys.stderr)
        for fl in failures:
            print("  " + fl.replace("\n", "\n  "), file=sys.stderr)
        return 1
    print("panic-sites: ok — %d sites in %d files, %d allowlist entries"
          % (len(sites), nfiles, len(entries)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
