#!/usr/bin/env bash
# Files copied byte for byte from lazyview (https://github.com/kirmorozov/lazyview), listed in
# vendor/lazyview.lock with the lazyview commit they come from and their sha256. Never edit one of
# them here: change lazyview, then sync.
#
#   scripts/sync-lazyview.sh COMMIT [PATH...]   copy every listed file (and each new PATH) from that
#                                               lazyview commit, rewrite the lock
#   scripts/sync-lazyview.sh --check            the files match the lock (make lint)
#   scripts/sync-lazyview.sh --check --upstream  … and the lock matches lazyview at its commit (CI)
#   scripts/sync-lazyview.sh --drift            advisory, exits 0 always: each locked file lazyview's
#                                               main has changed, and copy-set files main added
#
# A path is the same in both repositories. LAZYVIEW_RAW overrides where files are fetched from
# (default: raw.githubusercontent.com), LAZYVIEW_REPO the git repository (default:
# github.com/kirmorozov/lazyview).
set -euo pipefail
cd "$(dirname "$0")/.."

lock=vendor/lazyview.lock
raw=${LAZYVIEW_RAW:-https://raw.githubusercontent.com/kirmorozov/lazyview}
repo=${LAZYVIEW_REPO:-https://github.com/kirmorozov/lazyview.git}
mkdir -p tmp && tmp=$(mktemp -d tmp/sync-lazyview.XXXXXX)
trap 'rm -rf "$tmp"' EXIT

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }
fetch() { curl -fsSL --retry 3 "$raw/$1/$2"; }
locked_commit() { awk '$1 == "commit" { print $2 }' "$lock"; }
locked_files() { awk '$1 == "file" { print $2, $3 }' "$lock"; }

check() {
  local upstream=$1 commit bad=0 path want
  [[ -f $lock ]] || { echo "$lock: missing" >&2; exit 1; }
  commit=$(locked_commit)
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || { echo "$lock: no full commit id" >&2; exit 1; }
  while read -r path want; do
    if [[ ! -f $path ]]; then
      echo "$path: missing (listed in $lock)" >&2; bad=1; continue
    fi
    if [[ $(sha "$path") != "$want" ]]; then
      echo "$path: differs from lazyview $commit (edited here? fix it in lazyview, then sync)" >&2
      bad=1
    fi
    if [[ $upstream == 1 ]]; then
      fetch "$commit" "$path" > "$tmp/f" || { echo "$path: cannot fetch lazyview $commit" >&2; bad=1; continue; }
      if [[ $(sha "$tmp/f") != "$want" ]]; then
        echo "$path: $lock does not match lazyview $commit" >&2; bad=1
      fi
    fi
  done < <(locked_files)
  [[ $bad == 0 ]] || exit 1
  echo "lazyview copies match $lock (commit ${commit:0:12}$([[ $upstream == 1 ]] && echo ', checked upstream'))"
}

sync() {
  local commit=$1 path; shift
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || { echo "COMMIT must be a full 40-character commit id" >&2; exit 1; }
  local paths=()
  [[ -f $lock ]] && while read -r path _; do paths+=("$path"); done < <(locked_files)
  paths+=("$@")
  [[ ${#paths[@]} -gt 0 ]] || { echo "nothing to sync: name a PATH" >&2; exit 1; }
  {
    echo "# Files copied byte for byte from lazyview (https://github.com/kirmorozov/lazyview, MIT,"
    echo "# Copyright (c) 2026 Kirill Morozov). Written by scripts/sync-lazyview.sh: never edit the files"
    echo "# it lists, change lazyview and sync instead."
    echo "commit $commit"
  } > "$tmp/lock"
  for path in $(printf '%s\n' "${paths[@]}" | sort -u); do
    mkdir -p "$tmp/files/$(dirname "$path")"
    fetch "$commit" "$path" > "$tmp/files/$path" || { echo "$path: cannot fetch lazyview $commit" >&2; exit 1; }
    echo "file $path $(sha "$tmp/files/$path")" >> "$tmp/lock"
  done
  # Everything fetched: only now touch the working tree.
  for path in $(printf '%s\n' "${paths[@]}" | sort -u); do
    mkdir -p "$(dirname "$path")"
    cp "$tmp/files/$path" "$path"
  done
  mkdir -p "$(dirname "$lock")"
  cp "$tmp/lock" "$lock"
  echo "synced ${#paths[@]} file(s) from lazyview ${commit:0:12}"
}

# The copy-set files of a lazyview commit: its guard doc lists them ("…Together they form the COPY
# SET"), the wildcards expanded against the commit's tree. Empty when the list is not discoverable
# (the doc moved, or the network failed): the drift check then reports the differing files only.
copy_set() {
  local main=$1 doc pats pat dir tree
  doc=$(fetch "$main" .claude/agents/generic-guard.md 2>/dev/null) || return 0
  pats=$(printf '%s' "$doc" | tr '\n' ' ' | sed 's/  */ /g' \
    | sed -n 's/.*Another tool copies these files VERBATIM: \(.*\) Together they form the COPY SET\..*/\1/p' \
    | grep -oE 'src/[A-Za-z0-9_/*.-]+')
  [[ -n $pats ]] || return 0
  git clone --quiet --depth 1 --filter=blob:none --no-checkout "$repo" "$tmp/lv" 2>/dev/null || return 0
  tree=$(git -C "$tmp/lv" ls-tree -r --name-only "$main" 2>/dev/null) || return 0
  while read -r pat; do
    case $pat in
      *'/**') dir=${pat%'/**'}; printf '%s\n' "$tree" | { grep -E "^${dir}/" || true; } ;;
      *'*')   dir=${pat%'/*'};  printf '%s\n' "$tree" | { grep -E "^${dir}/[^/]+$" || true; } ;;
      *)     printf '%s\n' "$tree" | { grep -Fx "$pat" || true; } ;;
    esac
  done < <(printf '%s\n' "$pats" | sort -u) | sort -u
}

# Advisory: what has lazyview's main changed since the lock? Each locked file that differs from
# main (or that main no longer has), and each copy-set file main added. Always exits 0: it is a
# reminder to re-sync, not a gate.
drift() {
  local commit main path want c differ=0 added=0 gone=0
  [[ -f $lock ]] || { echo "$lock: missing" >&2; return 0; }
  commit=$(locked_commit)
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || { echo "$lock: no full commit id" >&2; return 0; }
  main=$(git ls-remote "$repo" refs/heads/main 2>/dev/null | awk '{ print $1 }' || true)
  [[ $main =~ ^[0-9a-f]{40}$ ]] || { echo "cannot resolve lazyview's main ($repo): network?" >&2; return 0; }
  echo "lazyview main: ${main:0:12} (the lock holds ${commit:0:12})"
  if [[ $main == "$commit" ]]; then
    echo "no drift: lazyview's main is the locked commit"
    return 0
  fi
  while read -r path want; do
    if ! fetch "$main" "$path" > "$tmp/f" 2>/dev/null; then
      echo "$path: not in lazyview main anymore (locked ${commit:0:12})"
      gone=$((gone + 1))
      continue
    fi
    if [[ $(sha "$tmp/f") != "$want" ]]; then
      echo "$path: differs from lazyview main ${main:0:12} (locked ${commit:0:12})"
      differ=$((differ + 1))
    fi
  done < <(locked_files)
  while read -r c; do
    [[ -n $c ]] || continue
    if ! awk -v p="$c" '$1 == "file" && $2 == p { found = 1 } END { exit !found }' "$lock"; then
      echo "$c: new in lazyview's copy set (main ${main:0:12})"
      added=$((added + 1))
    fi
  done < <(copy_set "$main")
  if [[ $differ == 0 && $added == 0 && $gone == 0 ]]; then
    echo "no drift: every locked file matches lazyview main"
  fi
  return 0
}

case ${1:-} in
  --check)
    case ${2:-} in
      "") check 0 ;;
      --upstream) check 1 ;;
      *) echo "usage: $0 --check [--upstream]" >&2; exit 2 ;;
    esac ;;
  --drift) drift ;;
  ""|-h|--help) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
  *) sync "$@" ;;
esac
