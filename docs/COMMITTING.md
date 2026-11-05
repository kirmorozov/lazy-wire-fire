# Committing

House rules for commits, branches and pull requests on this repository.

## Commit messages

- One line: an area prefix, a colon, and a short summary in the repo's voice
  (plain statement or imperative). Examples from the log:
  `vendor: re-sync lazyview's planner to 808e743`,
  `engine: a re-enabled rule replays what it missed instead of resetting`,
  `test servers: pin the MariaDB 13.1.1 RC by digest`.
- The area is the part of the tool the change touches: `engine`, `binlog`,
  `vendor`, `ci`, `docs`, `test servers`.
- No co-author lines and no "generated with" lines.

## Branches and pull requests

- One branch and one pull request per change, cut from the tip of `main`.
- The pull request is merged only when CI (`.github/workflows/ci.yml`) is
  green: lint, unit tests, the integration default set (MySQL 8.4 and
  MariaDB 10.11 with the targets), the full server matrix, search and the
  image jobs. The MariaDB pre-release matrix entries are advisory
  (`continue-on-error`) and never block.
- Merge by squashing, so `main` stays a linear history of one commit per
  change, and delete the branch afterwards.

## Hand-offs from workers

- The author of a change works on its own branch, cut from the tip of
  `main`: one commit per change, with a message per this file. The author
  pushes the branch, opens a draft pull request, runs its CI until green,
  and only then marks it ready to merge (`gh pr ready`). The keeper never
  writes a commit for the author's change.
- The git keeper squash-merges the pull requests that are ready, green and
  mergeable, in a merge pass over the open pull requests. Squash merging
  does not need the branch to be up to date, so nobody rebases for the
  merge.
- A pull request that conflicts with `main` goes back to its author:
  "needs refresh." The author rebases onto the new `main`, force-pushes its
  own branch, and gets its CI green again. The keeper never rebases another
  author's branch and never resolves a conflict.

## Checks before a commit

- `make lint` — `cargo fmt --check`, `clippy -D warnings`, and the lazyview
  lock check (below).
- `make test` — the unit tests.
- `make test-e2e` — when the engine changed: start the test servers first
  (`make servers`), then the integration suite against MySQL 8.4;
  `make test-e2e-all` covers the default pair, MySQL 8.4 and MariaDB 10.11.
- Keep `Cargo.lock` in step with `Cargo.toml`: CI runs `cargo fetch --locked`
  and fails on drift.

## Vendored lazyview files

The files listed in `vendor/lazyview.lock` are byte-for-byte copies of
[lazyview](https://github.com/kirmorozov/lazyview) — `src/binlog.rs` (the
binlog reader) and the view planner. Never edit the copies in this repo:

- a fix lands in lazyview first;
- then the copies are synced with `scripts/sync-lazyview.sh <commit>`, which
  updates the lock;
- `make lint` verifies the copies against the lock, and on CI (with `CI=true`)
  additionally against lazyview itself at the locked commit.

## Dependabot

Dependabot opens weekly bumps (`.github/dependabot.yml`): workflow actions
(grouped, `ci` prefix), cargo minor/patch updates (`deps`), the Dockerfile
base images pinned by digest (`docker`) — bumping the rust image also means
bumping `RUST_TOOLCHAIN` in `ci.yml` — and the test servers' patch builds
(`test`).

- Squash-merge a dependabot pull request when its CI is green, with the
  subject Bump <what> from <old> to <new> and no area prefix, as in the log.
- Hold a bump that lazyview shares (for example the `roaring` crate) until
  lazyview itself moves to that version: the vendored copies are built
  against lazyview's pinned dependencies, so this repo follows lazyview.

## Releases

- A release is a `vX.Y.Z` tag on `main`. The binary's `--version` must report
  the tag's number (CI checks it), so bump the version in `Cargo.toml` before
  tagging.
- On the tag, `ci.yml` runs and then `publish-image.yml` assembles the
  multi-arch image, attaches provenance and an SBOM, pushes
  `ghcr.io/kirmorozov/lazy-wire-fire` and verifies the result.
- The GitHub release notes say what changed, the image digest, and which
  server lines were tested.

## Documentation

README.md and the rest of ./docs are edited by the documentation keeper and
committed by the git keeper.
