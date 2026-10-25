# lazy_wire_fire (lazywf): working rules

Generic change-triggered actions from the MySQL binlog. Keep the tool generic: nothing
application-specific in `src/`, examples and tests use plain tables (orders, customers, items).
The repository is public: it names no project that uses the tool and no local paths.

## Design rules
- Keys and buffers live in memory. The database holds only what a restart needs:
  `lazy_wire_fire_status` = confirmed binlog position, lag, reset state, last error. No counters
  or key ledgers in tables (counters may appear on `/status` only).
- A rule's position advances only after its actions met `expect`. One lane per rule (a flush or a
  reset batch at a time); rules and targets never wait for each other.
- `src/binlog.rs` is lazyview's reader (https://github.com/kirmorozov/lazyview); never modify lazyview,
  copy fixes from it instead of diverging.

## Files and the host
- Scratch files and outputs: `tmp/` and `tmp/out/` in this repo (git-ignored), not `/tmp`.
- The host is shared (other agents, many database containers, little free memory): run cargo with
  `CARGO_BUILD_JOBS=4` (the Makefile does) and one cargo command at a time.
- Test servers: `make servers` (docker compose `lazywf-test`: MySQL 8.4 :34184, MySQL 8.0 :34180,
  MariaDB 10.11 :34111, Redis :34379, versitygw S3 :34070, all on 127.0.0.1). Do not use other
  projects' containers.

## Checks
- `make test` (unit), `make lint` (fmt + clippy -D warnings), `make test-e2e` / `make test-e2e-all`.
- Commit messages: no co-author or "generated with" lines.
