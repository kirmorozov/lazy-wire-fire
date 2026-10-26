# lazy_wire_fire (lazywf)

[![ci](https://github.com/kirmorozov/lazy-wire-fire/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/kirmorozov/lazy-wire-fire/actions/workflows/ci.yml)
[![image](https://img.shields.io/badge/image-ghcr.io%2Fkirmorozov%2Flazy--wire--fire-2496ED?logo=docker&logoColor=white)](https://github.com/kirmorozov/lazy-wire-fire/pkgs/container/lazy-wire-fire)
[![license: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Change-triggered actions from the MySQL binlog. Row changes in the tables you register mark
**keys**. For the changed keys a rule runs its queries concurrently, merges the rows into one JSON
document per key, renders one Handlebars template and fires the result at a target: an HTTP
endpoint (any method, a whole raw request), Redis (one command), or MySQL / PostgreSQL (one SQL
statement).

Search index upserts, cache purges, webhooks and object-store exports are all just rules. The tool
knows nothing about any application: everything it does is rows in four tables. It reads the
binlog as a replica, so the source tables are never locked or polled.

```
binlog ─► triggers mark keys ─► per rule: debounce, dedupe ─► selects (concurrent) ─► docs
                                                                      │
                     lazy_wire_fire_status ◄── confirmed ◄── expect ◄─┴─► template ─► HTTP / Redis / SQL
```

## Quickstart

lazywf creates its tables on start (`lazy_wire_fire_target`, `_rule`, `_trigger`, `_status`) in
the schema of its DSN. The account needs `SELECT` on what the rules read, `SELECT, INSERT,
UPDATE, DELETE, CREATE` on its own schema, and `REPLICATION SLAVE, REPLICATION CLIENT` (MariaDB:
`BINLOG MONITOR` too), and no admin rights. The binlog must be `ROW` with
`binlog_row_image=FULL`. Details, Amazon RDS and Aurora included:
[server setup](docs/server-setup.md).

```sh
LAZYWF_DSN=mysql://lazywf:secret@db:3306/app lazywf
```

Keep a search index of orders in sync. A target, a rule, and the triggers that mark its keys:

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect)
VALUES ('search', 'http', 'http://opensearch:9200', 'JSON_CONTAINS(body, ''false'', ''$.errors'')');

INSERT INTO lazy_wire_fire_rule (name, target, selects, template) VALUES ('orders_index', 'search',
 'SELECT o.id AS _key, o.status AS status, o.total AS `amount.total`, c.name AS `customer.name`,
         IF(o.status = ''cancelled'', ''delete'', NULL) AS _op
    FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.id IN (:keys);
  SELECT o.id AS _key, t.tag AS `tags[]`
    FROM orders o LEFT JOIN order_tags t ON t.order_id = o.id WHERE o.id IN (:keys)',
 'POST /orders/_bulk HTTP/1.1
Content-Type: application/x-ndjson

{{#each docs}}{{#if (eq op "delete")}}{"delete":{"_id":{{json key}}}}
{{else}}{"update":{"_id":{{json key}}}}
{"doc":{{json doc}},"doc_as_upsert":true}
{{/if}}{{/each}}');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `key`, keys_sql) VALUES
 ('orders_index', 'orders',     '{{new.id}}',       NULL),
 ('orders_index', 'order_tags', '{{new.order_id}}', NULL),
 ('orders_index', 'customers',  NULL, 'SELECT id AS _key FROM orders WHERE customer_id = :new.id');
```

Thirty updates of order 42 within a second become one `_bulk` line for `42`, rendered from the
state after the last of them. Renaming a customer re-sends that customer's orders. A cancelled or
deleted order becomes a `delete`. Changes take effect without a restart.

Values from rows go into templates through the helper of the place they land in (`json`,
`url_encode`, `regex_escape`, `redis_quote`), and the tool's tables are admin-only: see
[security](docs/reference.md#security). More examples: a cache purge, a Redis command, an S3
export, reset steps, in [docs/reference.md](docs/reference.md#examples).

## How it behaves

| guarantee | how |
|---|---|
| after the commit | marks of a transaction apply at its commit (XA: at `XA COMMIT`, never on rollback); before reading, a flush waits until the transaction is visible (`WAIT_FOR_EXECUTED_GTID_SET`, the consistent-snapshot position on MariaDB / Percona, else `LAZYWF_VISIBILITY_DELAY_MS`) |
| dedupe and debounce | keys are collected per rule in memory and fired once per key when nothing was marked for `debounce_ms`. Keys that are an integer or a prefix and an integer (`42`, `cat_p_42`) live in 64-bit roaring bitmaps, as in lazyview: a million consecutive ids take under 300 KB |
| bounded delay | no key waits more than `max_delay_ms` after its first change, even while it keeps changing |
| latest state wins | selects run at flush time. A key marked again while its action is in flight fires again afterwards. Each rule has one lane (one flush or reset batch at a time), so the last read is the last sent |
| at least once | a rule's position in `lazy_wire_fire_status` only passes changes whose actions met `expect` on every endpoint. After a restart the stream resumes from the lowest position; rules ahead skip what they confirmed |
| failure | retry with backoff (0.5 s doubling, up to `LAZYWF_MAX_BACKOFF_MS`), the keys stay pending. Other rules and targets keep going |
| lost continuity | first start, a purged binlog, another server, a new rule (a rule enabled again replays what it missed while the binlog still has it), a watched table replaced without row events (`TRUNCATE`, `RENAME` onto it, `DROP`; an online schema change's swap keeps continuity): the rule's `reset_steps` run once in the background while streaming continues. Rules without them are reported in `reset_state` |

The status table holds only what a restart needs: per rule (and `*` for the instance) the
confirmed binlog position, lag, reset state and the last error, written every few seconds when it
moved. Keys themselves are never stored.

## Configuration

Environment variables:

| variable | default | meaning |
|---|---|---|
| `LAZYWF_DSN` | — | `mysql://user:pass@host:3306/schema` (`mariadb://` accepted); the schema holds the tool's tables |
| `LAZYWF_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_DATABASE` | —, `3306`, `root`, —, — | instead of the DSN (values used verbatim) |
| `LAZYWF_BINLOG_USER`, `LAZYWF_BINLOG_PASSWORD` (or `LAZYWF_BINLOG_DSN`) | the DSN's | replication account for the binlog stream |
| `LAZYWF_TLS`, `LAZYWF_TLS_CA` | `disabled`, — | `required`, `verify-ca`, `verify-full`; extra CA file |
| `LAZYWF_INSTANCE` | `default` | instance name: status rows and the instance lock (one active copy per schema and name; a second one stands by) |
| `LAZYWF_SERVER_ID` | random | replica server id |
| `LAZYWF_HEALTH_ADDR` | `127.0.0.1:8081` | `/livez`, `/readyz`, `/status`; `off` disables |
| `LAZYWF_HEARTBEAT_MS` | `5000` | binlog heartbeat (a silent connection is dropped after 3×) |
| `LAZYWF_VISIBILITY_DELAY_MS` | `20` | visibility fence when neither GTIDs nor a snapshot position are available |
| `LAZYWF_POOL_SIZE` | `16` | query connections (selects run concurrently) |
| `LAZYWF_SEND_CONCURRENCY` | `16` | concurrent actions of a `fire_per: key` flush |
| `LAZYWF_STATUS_MS` | `5000` | status rows are written at most this often |
| `LAZYWF_CONFIG_POLL_MS` | `30000` | rules are re-read this often besides on their own binlog events |
| `LAZYWF_MAX_BACKOFF_MS` | `60000` | longest retry wait |
| `LAZYWF_STOP_GRACE_MS` | `10000` | on SIGTERM, how long running actions may finish |
| `LAZYWF_SECRET_DIRS` | `/run/secrets:/var/run/secrets` | where `file:` secret references may point |
| `RUST_LOG`, `LAZYWF_LOG_FORMAT` | `info`, `text` | log level, `json` |

Rules, triggers and targets: [docs/reference.md](docs/reference.md).

## Operations

* SIGTERM stops gracefully: running actions finish (up to `LAZYWF_STOP_GRACE_MS`), positions are
  saved, exit 0. A non-zero exit means a real failure (bad configuration, lost instance lock,
  missing privileges).
* `/livez`: the main loop turns. `/readyz`: this instance is active and streaming. `/status`:
  per rule the position, lag, pending keys, reset state and errors (JSON). `lazywf healthcheck`
  probes `/livez` for container health checks.
* `SELECT * FROM lazy_wire_fire_status`: positions, lag, `reset_state`, `last_error`.
* `UPDATE lazy_wire_fire_rule SET reset_requested = 1 WHERE name = …` re-runs a rule's reset
  steps; the tool sets it back to 0 when they succeeded.

Under a supervisor: environment for the DSN and secrets, a health address on 127.0.0.1, an
instance name, SIGTERM to stop.

## Image

```sh
docker pull ghcr.io/kirmorozov/lazy-wire-fire:edge
docker run -d --name lazywf -e LAZYWF_DSN=mysql://lazywf:secret@db:3306/app ghcr.io/kirmorozov/lazy-wire-fire:edge
```

| | |
|---|---|
| platforms | `linux/amd64`, `linux/arm64`: one static musl binary `/lazywf`, cross-compiled (cargo-zigbuild) |
| base | `gcr.io/distroless/static-debian12:nonroot`, pinned by digest: uid 65532, no shell, no package manager |
| health | `HEALTHCHECK` runs `lazywf healthcheck` (`/livez` on `LAZYWF_HEALTH_ADDR`, inside the container by default) |
| tags | `edge`: the latest `main`; `sha-<commit>`: every build, the newest 20 kept; releases: `X.Y.Z`, `X.Y`, `X` (not for `0.x`), `latest` |
| attestations | BuildKit provenance (`mode=max`) and SBOM (Debian packages and crates), a signed GitHub build provenance |

```sh
gh attestation verify oci://ghcr.io/kirmorozov/lazy-wire-fire:edge --owner kirmorozov
docker buildx imagetools inspect ghcr.io/kirmorozov/lazy-wire-fire:edge --format '{{json .SBOM}}'
```

Releases: bump `version` in `Cargo.toml`, merge, push a `vX.Y.Z` tag. CI publishes it once every
blocking job passes, the search engines included, and fails it when `lazywf --version` does not
match the tag. Pre-release tags (`v1.2.0-rc.1`) publish only `1.2.0-rc.1`.

## Supported servers

Every vendor-supported LTS line at its latest build, each product's current GA, the lines Amazon
RDS / Aurora still run after their community end of life, and the newest MariaDB release candidate
and preview. The whole integration suite runs on each, one CI runner per server; every tier but
pre-release blocks `edge` and releases. Builds are pinned exactly in
[docker-compose.test.yml](docker-compose.test.yml) (the preview by digest).

| product | line | tested build | tier | vendor support ends (as of 2026-10-25) |
|---|---|---|---|---|
| MySQL | 8.0 | 8.0.46 (final) | AWS-only (RDS / Aurora 8.0), GTID off, blocking | community: April 2026; RDS Extended Support: 2029-07-31; Aurora MySQL 3: standard 2028-04-30, Extended Support 2029-07-31 |
| MySQL | 8.4 LTS | 8.4.13 | LTS, blocking | Premier 2029-04-30, Extended 2032-04-30 (third-party dates, from Oracle's 5 + 3 year rule) |
| MySQL | 9.7 LTS | 9.7.4 | LTS, blocking | Premier 2031-04-30, Extended 2034-04-30 (third-party dates, from Oracle's 5 + 3 year rule) |
| MySQL | 26.10 Innovation | 26.10.0 | GA, blocking | with the next Innovation release (no fixed date) |
| Percona Server | 8.4 LTS | 8.4.11-11 | LTS, blocking | end of life 2032-04-30 (its table also lists 2029-04-30 under "extended support") |
| Percona Server | 9.7 LTS | 9.7.2-2 | LTS, blocking | end of life 2034-04-21 (its table also lists 2031-04-21 under "extended support") |
| MariaDB | 10.6 | 10.6.28 (final) | AWS-only (RDS), blocking until 2026-12-31 | community: 2026-07-06; RDS standard support: 2026-12-31 |
| MariaDB | 10.11 LTS | 10.11.19 | LTS, blocking | 2028-02-16 |
| MariaDB | 11.4 LTS | 11.4.13 | LTS, blocking | 2029-05-29 |
| MariaDB | 11.8 LTS | 11.8.9 | LTS, blocking | 2028-06-04 |
| MariaDB | 12.3 LTS | 12.3.3 | LTS, blocking | 2029-06-12 |
| MariaDB | 13.0 rolling | 13.0.2 | GA, blocking | Q4 2026 |
| MariaDB | 13.1 | 13.1.1 RC | pre-release, advisory | pre-release; GA date unknown (policy page: end of life Q1 2027) |
| MariaDB | 13.2 | 13.2 Preview | pre-release, advisory | pre-release; unknown |

Also supported: Amazon RDS for MySQL and MariaDB, and Aurora MySQL, on the lines above; setup in [docs/server-setup.md](docs/server-setup.md#amazon-rds-and-aurora).
AWS offers only some of them: RDS for MySQL 8.4 and 8.0, RDS for MariaDB 10.6 to 12.3, Aurora
MySQL 3 (8.0) and 8.4. Lifecycle pages: [MySQL](https://dev.mysql.com/doc/en/mysql-releases.html),
[Percona](https://www.percona.com/services/policies/percona-software-support-lifecycle),
[MariaDB](https://mariadb.org/about/),
[RDS for MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MySQL.Concepts.VersionMgmt.html),
[RDS for MariaDB](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MariaDB.Concepts.VersionMgmt.html),
[Aurora MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraMySQLReleaseNotes/AuroraMySQL.release-calendars.html).

MySQL and Percona publish no pre-release in a registry (MySQL early access ships as downloads
only), so they have no advisory job. An advisory job that fails shows as a warning in the run and
its summary, and blocks nothing. MySQL and Percona run with GTIDs on, except MySQL 8.0: off, as in
RDS for MySQL's default parameter group, so it also tests the `LAZYWF_VISIBILITY_DELAY_MS` fence.
Dropped as end of life: Percona Server 8.0 (no RDS edition). MariaDB 10.6 leaves the matrix after
2026-12-31.

Targets tested: OpenSearch 2.19, Elasticsearch 7.17 and 8.17 (security on), Varnish 7.6, Redis 7.4,
Valkey 8.1, an S3 API (versitygw 1.10), MySQL and PostgreSQL 17 as SQL targets.

## Development

```sh
make test                      # unit tests
make servers                   # MySQL 8.4, MariaDB 10.11, Redis, Valkey, PostgreSQL, S3 (docker)
make test-e2e                  # integration tests on MySQL 8.4; test-e2e-all: MySQL 8.4 and MariaDB 10.11
make test-matrix               # every other blocking line of "Supported servers", one at a time
make test-prerelease           # the advisory MariaDB 13.1 RC and 13.2 preview
make test-e2e-search           # OpenSearch, Elasticsearch 7 and 8, one at a time
docker buildx build -t lazywf:dev --load .   # static musl binary on distroless
make docker-smoke docker-e2e   # the loaded lazywf:dev: no shell, uid 65532; a row fires through Redis
```

CI ([ci.yml](.github/workflows/ci.yml)) runs on every push and pull request, weekly and on demand:
lint, unit tests, the integration suite on the default servers (Varnish included), on every other
server (one runner each, pre-releases advisory) and with the search engines, and the image for both
platforms. Pushes to `main` publish `edge` once every blocking job passes; `v*` tags a release, which
also needs the search engines. Dependabot bumps actions, crates, base images and the test servers'
patch builds weekly.

The binlog reader (`src/binlog.rs`) is an unmodified copy of
[lazyview](https://github.com/kirmorozov/lazyview)'s, pinned in `vendor/lazyview.lock` and updated
with `scripts/sync-lazyview.sh`.

License: MIT.
