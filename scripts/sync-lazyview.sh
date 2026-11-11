#!/usr/bin/env bash
# Files copied byte for byte from lazyview (https://github.com/kirmorozov/lazyview), listed in
# vendor/lazyview.lock with the lazyview commit they come from and their sha256. Never edit one of
# them here: change lazyview, then sync.
#
# The lock is three sections, in this order: a header, a `commit <sha>` line, `glob <pattern>` lines
# (the copy list), and `file <path> <sha256>` lines (the expansion of the globs at that commit —
# written by this script, never by hand). A glob is a directory glob `dir/**` (everything under dir,
# at any depth) or a single path `src/x.rs`. lazyview splits its files by concern (src/<x>/*.rs); a
# glob tracks a split with no change here, a fixed file list would lag every one.
#
#   scripts/sync-lazyview.sh COMMIT [PATH...]   expand the lock's globs (plus each PATH, added as a
#                                               single-path glob) against lazyview's tree at COMMIT,
#                                               copy every match, drop copies the commit no longer
#                                               has, rewrite the lock
#   scripts/sync-lazyview.sh --check            the files match the lock (make lint)
#   scripts/sync-lazyview.sh --check --upstream  … the globs expand to exactly the locked files and
#                                               every hash matches lazyview at its commit (CI)
#   scripts/sync-lazyview.sh --drift            advisory, exits 0 always: each locked file lazyview's
#                                               main has changed or dropped, and each file main added
#                                               under a glob
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
locked_paths() { awk '$1 == "file" { print $2 }' "$lock"; }
locked_globs() { awk '$1 == "glob" { print $2 }' "$lock"; }

# The tree of a lazyview commit — its full file list, from the git object store, never a guess. The
# clone is blobless (ls-tree needs trees, not contents) and cached in $tmp for the run.
tree_of() {
  local ref=$1
  [[ -d $tmp/lv/.git ]] || git clone --quiet --filter=blob:none --no-checkout "$repo" "$tmp/lv" 2>/dev/null || return 1
  git -C "$tmp/lv" rev-parse --verify --quiet "$ref^{tree}" >/dev/null 2>&1 \
    || git -C "$tmp/lv" fetch --quiet origin "$ref" 2>/dev/null || return 1
  git -C "$tmp/lv" ls-tree -r --name-only "$ref" 2>/dev/null || return 1
}

# Expand glob patterns (one per line on stdin) against a tree (arg 1). `dir/**` matches everything
# under dir/ at any depth; `dir/*` matches files directly under dir/; anything else is a path taken
# literally. Sorted, de-duplicated.
expand() {
  local tree=$1 pat dir
  while read -r pat; do
    [[ -n $pat ]] || continue
    case $pat in
      *'/**') dir=${pat%'/**'}; printf '%s\n' "$tree" | { grep -E "^${dir}/" || true; } ;;
      *'*')   dir=${pat%'/*'};  printf '%s\n' "$tree" | { grep -E "^${dir}/[^/]+$" || true; } ;;
      *)      printf '%s\n' "$tree" | { grep -Fx "$pat" || true; } ;;
    esac
  done | sort -u
}

check() {
  local upstream=$1 commit bad=0 path want tree
  [[ -f $lock ]] || { echo "$lock: missing" >&2; exit 1; }
  commit=$(locked_commit)
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || { echo "$lock: no full commit id" >&2; exit 1; }
  while read -r path want; do
    [[ -n $path ]] || continue
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
  # Upstream: the `file` set must be exactly the lock's globs expanded at the commit.
  if [[ $upstream == 1 ]]; then
    local globs want have
    globs=$(locked_globs)
    [[ -n $globs ]] || { echo "$lock: no 'glob' lines to check the copy list against" >&2; bad=1; }
    tree=$(tree_of "$commit") || { echo "$lock: cannot read lazyview's tree at $commit ($repo)" >&2; bad=1; }
    if [[ -n $tree && -n $globs ]]; then
      want=$(printf '%s\n' "$globs" | expand "$tree")
      have=$(locked_paths)
      while read -r path; do
        [[ -n $path ]] || continue
        if ! printf '%s\n' "$want" | grep -Fxq "$path"; then
          echo "$path: in $lock but no glob covers it (drop it, or add a 'glob' line)" >&2; bad=1
        fi
      done <<< "$have"
      while read -r path; do
        [[ -n $path ]] || continue
        if ! printf '%s\n' "$have" | grep -Fxq "$path"; then
          echo "$path: lazyview $commit has it under a glob but $lock omits it (re-sync)" >&2; bad=1
        fi
      done <<< "$want"
    fi
  fi
  [[ $bad == 0 ]] || exit 1
  echo "lazyview copies match $lock (commit ${commit:0:12}$([[ $upstream == 1 ]] && echo ', checked upstream'))"
}

sync() {
  local commit=$1 path; shift
  [[ $commit =~ ^[0-9a-f]{40}$ ]] || { echo "COMMIT must be a full 40-character commit id" >&2; exit 1; }
  local globs=()
  [[ -f $lock ]] && while read -r path; do globs+=("$path"); done < <(locked_globs)
  globs+=("$@")
  [[ ${#globs[@]} -gt 0 ]] || { echo "nothing to sync: $lock has no 'glob' lines and no PATH was given" >&2; exit 1; }
  local tree
  tree=$(tree_of "$commit") || { echo "cannot read lazyview's tree at $commit ($repo)" >&2; exit 1; }
  [[ -n $tree ]] || { echo "empty tree for lazyview $commit" >&2; exit 1; }
  local wanted
  wanted=$(printf '%s\n' "${globs[@]}" | expand "$tree")
  [[ -n $wanted ]] || { echo "the globs match no file in lazyview $commit" >&2; exit 1; }
  # The copies we have now, so ones the commit no longer has can be removed (a moved file lingers).
  local old=()
  [[ -f $lock ]] && while read -r path; do old+=("$path"); done < <(locked_paths)
  # Everything is fetched before the working tree is touched.
  {
    echo "# Files copied byte for byte from lazyview (https://github.com/kirmorozov/lazyview, MIT,"
    echo "# Copyright (c) 2026 Kirill Morozov). Written by scripts/sync-lazyview.sh: never edit the files"
    echo "# it lists, change lazyview and sync instead."
    echo "commit $commit"
    printf '%s\n' "${globs[@]}" | sort -u | sed 's/^/glob /'
    while read -r path; do
      [[ -n $path ]] || continue
      mkdir -p "$tmp/files/$(dirname "$path")"
      fetch "$commit" "$path" > "$tmp/files/$path" || { echo "$path: cannot fetch lazyview $commit" >&2; exit 1; }
      echo "file $path $(sha "$tmp/files/$path")"
    done <<< "$wanted"
  } > "$tmp/lock"
  while read -r path; do
    [[ -n $path ]] || continue
    mkdir -p "$(dirname "$path")"
    cp "$tmp/files/$path" "$path"
  done <<< "$wanted"
  if [[ ${#old[@]} -gt 0 ]]; then
    for path in "${old[@]}"; do
      if printf '%s\n' "$wanted" | grep -Fxq "$path"; then continue; fi
      [[ -f $path ]] && { rm -f "$path"; echo "removed $path (no longer under a glob in lazyview ${commit:0:12})"; }
    done
  fi
  # Tidy: drop directories a move emptied, under the `**` copy dirs only.
  local dir
  for path in "${globs[@]}"; do
    [[ $path == *'/**' ]] || continue
    dir=${path%'/**'}
    [[ -d $dir ]] && find "$dir" -mindepth 1 -type d -empty -delete 2>/dev/null || true
  done
  mkdir -p "$(dirname "$lock")"
  cp "$tmp/lock" "$lock"
  echo "synced $(printf '%s\n' "$wanted" | grep -c .) file(s) from lazyview ${commit:0:12}"
}

# Advisory: what has lazyview's main changed since the lock? Each locked file that differs from main
# (or that main no longer has), and each file main added under one of the lock's globs — so a split
# into a subdirectory shows up before it is copied. Always exits 0: a reminder to re-sync, not a gate.
drift() {
  local commit main path want differ=0 added=0 gone=0 tree
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
    [[ -n $path ]] || continue
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
  tree=$(tree_of "$main" 2>/dev/null) || tree=""
  if [[ -n $tree ]]; then
    while read -r path; do
      [[ -n $path ]] || continue
      if awk -v p="$path" '$1 == "file" && $2 == p { found = 1 } END { exit !found }' "$lock"; then continue; fi
      echo "$path: new under a glob in lazyview's copy set (main ${main:0:12})"
      added=$((added + 1))
    done < <(printf '%s\n' "$(locked_globs)" | expand "$tree")
  fi
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
  ""|-h|--help) sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
  *) sync "$@" ;;
esac
