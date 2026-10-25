# lazy_wire_fire (lazywf)

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
                     lazy_wire_fire_status ◄── confirmed ◄── expect ◄─┴─► template ─► HTTP / Redis
```

## Quickstart

lazywf creates its tables on start (`lazy_wire_fire_target`, `_rule`, `_trigger`, `_status`) in
the schema of its DSN. The account needs `SELECT` on what the rules read, the usual DML on its
own tables, and `REPLICATION SLAVE, REPLICATION CLIENT` (MariaDB: `BINLOG MONITOR` too). The
binlog must be `ROW` with `binlog_row_image=FULL`.

```sh
LAZYWF_DSN=mysql://lazywf:secret@db:3306/app lazywf
```

Keep a search index of orders in sync. A target, a rule, and the triggers that mark its keys:

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect)
VALUES ('search', 'http', 'http://opensearch:9200', '2xx and errors == false');

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
| dedupe and debounce | keys are collected per rule in memory and fired once per key when nothing was marked for `debounce_ms`. Keys that are an integer or a prefix and an integer (`42`, `cat_p_42`) live in 64-bit roaring bitmaps, as in lazyview: a million product ids take a few MB |
| bounded delay | no key waits more than `max_delay_ms` after its first change, even while it keeps changing |
| latest state wins | selects run at flush time. A key marked again while its action is in flight fires again afterwards. Each rule has one lane (one flush or reset batch at a time), so the last read is the last sent |
| at least once | a rule's position in `lazy_wire_fire_status` only passes changes whose actions met `expect` on every endpoint. After a restart the stream resumes from the lowest position; rules ahead skip what they confirmed |
| failure | retry with backoff (0.5 s doubling, up to `LAZYWF_MAX_BACKOFF_MS`), the keys stay pending. Other rules and targets keep going |
| lost continuity | first start, a purged binlog, another server, a re-enabled rule, a watched table replaced without row events (`TRUNCATE`, `RENAME` onto it, `DROP`): the rule's `reset_steps` run once in the background while streaming continues. Rules without them are reported in `reset_state` |

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

## Supported servers

Tested on every line below with the whole integration suite (`make test-matrix`, `make test-e2e-all`):

| product | lines |
|---|---|
| MySQL | 8.0, 8.4 LTS, 9.7 LTS, 26.10 (latest GA) |
| Percona Server | 8.4 LTS, 9.7 LTS (latest) |
| MariaDB | 10.11 LTS, 12.3 LTS, 13.0 (latest GA) |

Targets tested: OpenSearch 2.19, Elasticsearch 7.17 and 8.17 (security on), Varnish 7.6, Redis 7,
Valkey 8, an S3 API (versitygw), MySQL and PostgreSQL 17 as SQL targets.

## Development

```sh
make test                      # unit tests
make servers                   # MySQL 8.4 / 8.0, MariaDB 10.11, Redis, Valkey, PostgreSQL, S3 (docker)
make test-e2e                  # integration tests on MySQL 8.4; test-e2e-all: 8.4, 8.0, MariaDB 10.11
make test-matrix               # latest LTS and latest GA of MySQL, Percona, MariaDB, one at a time
make test-e2e-search           # OpenSearch, Elasticsearch 7 and 8, one at a time
docker buildx build -t lazywf --load .   # static musl binary on distroless
```

The binlog reader (`src/binlog.rs`) comes from [lazyview](https://github.com/kirmorozov/lazyview).

License: MIT.
