#!/usr/bin/env bash
# Files copied byte for byte from lazyview (https://github.com/kirmorozov/lazyview), listed in
# vendor/lazyview.lock with the lazyview commit they come from and their sha256. Never edit one of
# them here: change lazyview, then sync.
#
#   scripts/sync-lazyview.sh COMMIT [PATH...]   copy every listed file (and each new PATH) from that
#                                               lazyview commit, rewrite the lock
#   scripts/sync-lazyview.sh --check            the files match the lock (make lint)
#   scripts/sync-lazyview.sh --check --upstream  … and the lock matches lazyview at its commit (CI)
#
# A path is the same in both repositories. LAZYVIEW_RAW overrides where files are fetched from
# (default: raw.githubusercontent.com).
set -euo pipefail
cd "$(dirname "$0")/.."

lock=vendor/lazyview.lock
raw=${LAZYVIEW_RAW:-https://raw.githubusercontent.com/kirmorozov/lazyview}
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

case ${1:-} in
  --check)
    case ${2:-} in
      "") check 0 ;;
      --upstream) check 1 ;;
      *) echo "usage: $0 --check [--upstream]" >&2; exit 2 ;;
    esac ;;
  ""|-h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
  *) sync "$@" ;;
esac
