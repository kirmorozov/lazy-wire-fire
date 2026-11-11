# lazy_wire_fire (lazywf)

[![ci](https://github.com/kirmorozov/lazy-wire-fire/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/kirmorozov/lazy-wire-fire/actions/workflows/ci.yml)
[![image](https://img.shields.io/badge/image-ghcr.io%2Fkirmorozov%2Flazy--wire--fire-2496ED?logo=docker&logoColor=white)](https://github.com/kirmorozov/lazy-wire-fire/pkgs/container/lazy-wire-fire)
[![license: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Change-triggered actions from the MySQL binlog. Row changes in the tables you register mark
**keys**. For the changed keys a rule runs its queries concurrently, merges the rows into one JSON
document per key, renders one Handlebars template and fires the result at a target: an HTTP
endpoint (any method, a whole raw request), an Amazon SQS queue (one `SendMessageBatch` per
action), an Amazon Kinesis stream (one `PutRecords` per action), an AWS Lambda function
(one `Invoke` per action), an Amazon EventBridge event bus (one `PutEvents` per action), an
Amazon SNS topic (one `PublishBatch` per action), a Google Cloud Pub/Sub topic (one `publish`
per action), an AMQP broker (one publish per action),
an SMTP server (one mail per action), a Kafka-protocol broker (one produce of a batch of records
per action), a RabbitMQ stream (one publish batch of records per action), an Azure Service Bus
queue or topic (one batch of messages per action), Redis (one
command), or MySQL / PostgreSQL (one SQL statement).

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

On macOS or Windows, run that inside Docker or WSL 2: [Platforms](#platforms).

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

If the source tables live in another schema, qualify their names in the selects and `keys_sql`
(`FROM app.orders o`): unqualified names resolve in the DSN's schema. And lazywf does not create
the destination: create the index and its mapping before the first action. A search engine that
auto-creates indexes would otherwise create it with a dynamic mapping; when a target refuses
actions instead (auto-creation off), the rule's position holds, the keys are retried after
backoff, and the error is in `last_error`.

Values from rows go into templates through the helper of the place they land in (`json`,
`yaml`, `url_encode`, `regex_escape`, `redis_quote`), and the tool's tables are admin-only: see
[security](docs/reference.md#security). More examples: a cache purge, a Redis command, an S3
export, an SQS queue, a Kinesis stream, a Lambda function, an AMQP broker, a Pub/Sub topic,
reset steps, in [docs/reference.md](docs/reference.md#examples).

Vendor-by-vendor http recipes (Meilisearch, Typesense, ClickHouse, a CDN purge, a signed webhook,
a chat hook), with the endpoint, credential and `expect` each one wants, in
[docs/recipes.md](docs/recipes.md).

Business-app http recipes (Microsoft Teams, Google Chat, Segment, Klaviyo, HubSpot, Airtable,
Notion), with the extra header each service wants and what its 2xx is worth, in
[docs/recipes-apps.md](docs/recipes-apps.md).

## Platforms

lazywf is built for Linux only: the image is `linux/amd64` and `linux/arm64`, with one static
binary `/lazywf` inside, and there is no native macOS or Windows build.

| host | run |
|---|---|
| Linux | the image, or the static binary |
| macOS | the image with Docker (Docker Desktop or any Docker-compatible runtime); Apple silicon uses the `linux/arm64` image |
| Windows | inside WSL 2 (the image with Docker inside WSL, or the static binary), or the image with Docker Desktop |

If the database runs on the same Mac or Windows machine, outside Docker, its host in the DSN is
`host.docker.internal`, not `127.0.0.1`: Docker Desktop resolves that name to the host machine.
Example: `LAZYWF_DSN=mysql://lazywf:secret@host.docker.internal:3306/app`.

## How it behaves

| guarantee | how |
|---|---|
| after the commit | marks of a transaction apply at its commit (XA: at `XA COMMIT`, never on rollback); before reading, a flush waits until the transaction is visible (`WAIT_FOR_EXECUTED_GTID_SET`, the consistent-snapshot position on MariaDB / Percona, the commit fence on MySQL / Percona with neither, else `LAZYWF_VISIBILITY_DELAY_MS`; [server setup](docs/server-setup.md#binlog-settings)) |
| dedupe and debounce | keys are collected per rule in memory and fired once per key when nothing was marked for `debounce_ms`. Keys that are an integer or a prefix and an integer (`42`, `item_42`) keep their prefix's integers in a hash set by default (about 19 bytes per key at a million dense ids); with `--features roaring` they are 64-bit roaring bitmaps (a million consecutive ids under 300 KB, but sparse ids cost more) — [Development](#development) |
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
| `LAZYWF_QUERY_USER`, `LAZYWF_QUERY_PASSWORD` (or `LAZYWF_QUERY_DSN`) | the DSN's | account that runs the SQL the configuration defines; unset: the main account runs it. A SELECT-only account on the source tables the rules read is recommended ([server setup](docs/server-setup.md#query-account)) |
| `LAZYWF_TLS`, `LAZYWF_TLS_CA` | `disabled`, — | `required`, `verify-ca`, `verify-full`; extra CA file |
| `LAZYWF_INSTANCE` | `default` | instance name: status rows and the instance lock (one active copy per schema and name; a second one stands by) |
| `LAZYWF_SERVER_ID` | random | replica server id |
| `LAZYWF_HEALTH_ADDR` | `127.0.0.1:8081` | `/livez`, `/readyz`, `/status`; `off` disables |
| `LAZYWF_HEARTBEAT_MS` | `5000` | binlog heartbeat (a silent connection is dropped after 3×) |
| `LAZYWF_VISIBILITY_DELAY_MS` | `20` | the last-resort visibility fence, when GTIDs, a consistent-snapshot position and the commit fence are all unavailable ([server setup](docs/server-setup.md#binlog-settings)) |
| `LAZYWF_POOL_SIZE` | `16` | query connections (selects run concurrently) |
| `LAZYWF_SEND_CONCURRENCY` | `16` | concurrent actions of a `fire_per: key` flush |
| `LAZYWF_STATUS_MS` | `5000` | status rows are written at most this often |
| `LAZYWF_CONFIG_POLL_MS` | `30000` | rules are re-read this often besides on their own binlog events |
| `LAZYWF_MAX_BACKOFF_MS` | `60000` | longest retry wait |
| `LAZYWF_STOP_GRACE_MS` | `10000` | on SIGTERM, how long running actions may finish |
| `LAZYWF_SECRET_DIRS` | `/run/secrets:/var/run/secrets` | where `file:` secret references may point |
| `LAZYWF_ALLOWED_HOSTS` | — | opt-in: the endpoint hosts a target may connect to (exact names, `*.` suffixes, IP literals or CIDR ranges, each optionally with a port); unset: no restriction ([security](docs/reference.md#security)) |
| `RUST_LOG`, `LAZYWF_LOG_FORMAT` | `info`, `text` | log level, `json` |

Rules, triggers and targets: [docs/reference.md](docs/reference.md).

## Undelivered keys

A key is dropped only when the target refuses it for good: it is refused five times while other
keys of the same actions go through (the strikes live in memory only; when a whole flush is
refused, the template or the target is at fault, and nothing is dropped). Transient failures
(connection errors, timeouts, 5xx, 408, 429, AWS throttling codes, a Redis error reply, an SMTP
4xx reply, a retriable Kafka broker error, a RabbitMQ stream publish code that says nothing,
transient SQL errors) never drop a key: it stays pending and the rule retries it with backoff.

A dropped key is named in the rule's `last_error` (`/status`, `lazy_wire_fire_status`). With
`LAZYWF_LOG_FORMAT=json` it is also one WARN event per key, on the event target
`lazywf::undelivered`:

```json
{"timestamp":"2026-11-02T10:15:04.123456Z","level":"WARN","fields":{"message":"undelivered key dropped","rule":"orders_sync","target":"search","key":"42","reason":"400 Bad Request: document is not valid","strikes":5},"target":"lazywf::undelivered"}
```

Fields: `rule` the rule name; `target` the target's name in the configuration; `key` the key
(truncated to 200 characters); `reason` the last refusal; `strikes` the number of refusals at
the drop (5). The `op` (insert, update or delete) is not logged: a flush merges the keys of all
triggers. To filter for dropped keys in a log pipeline, match the top-level `target`
`lazywf::undelivered`. The event is WARN, so a `RUST_LOG` of `error` hides it.

## Operations

* SIGTERM stops gracefully: running actions finish (up to `LAZYWF_STOP_GRACE_MS`), positions are
  saved, exit 0. A non-zero exit means a real failure (bad configuration, lost instance lock,
  missing privileges).
* `/livez`: the main loop turns. `/readyz`: this instance is active and streaming. `/status`:
  per rule the position, lag, pending keys, reset state, counters and errors (JSON,
  [the fields](docs/reference.md#status)). `lazywf healthcheck` probes `/livez` for container
  health checks.
* `SELECT * FROM lazy_wire_fire_status`: positions, lag, `reset_state`, `last_error`.
* `UPDATE lazy_wire_fire_rule SET reset_requested = 1 WHERE name = …` re-runs a rule's reset
  steps; the tool sets it back to 0 when they succeeded.
* A new rule's first reset runs its reset steps in the background; a `for_each: snapshot` step
  reads every key of `snapshot_keys_sql`, so on a large source the destination matches only as
  that goes. Watch it in the `reset` field of `/status` (`pending: …`, `running: step i/n …`,
  `done: …`); the log says `reset step N: K keys in the snapshot`.

Under a supervisor: environment for the DSN and secrets, a health address on 127.0.0.1, an
instance name, SIGTERM to stop.

Inside a container, the default health address `127.0.0.1:8081` is the container's own loopback.
To query `/status` from the host: run with host networking (port 8081 free), or set
`LAZYWF_HEALTH_ADDR=0.0.0.0:8081` and publish `-p 127.0.0.1:8081:8081`.
`docker exec <name> /lazywf healthcheck` needs neither.

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
RDS for MySQL's default parameter group, so it also tests the commit fence.
On Oracle MySQL without GTIDs the visibility fence is the commit fence — one write to lazywf's
own table, exact when commits are in binlog order, with a `LAZYWF_VISIBILITY_DELAY_MS` fallback
([server setup](docs/server-setup.md#binlog-settings)).
Dropped as end of life: Percona Server 8.0 (no RDS edition). MariaDB 10.6 leaves the matrix after
2026-12-31.

## Supported targets

Where actions go — one row per target kind. The full endpoint forms, limits, resets and delivery
contracts are in [docs/reference.md](docs/reference.md#targets); the images are pinned in
[docker-compose.test.yml](docker-compose.test.yml).

| target | `kind` | endpoint | one action is | per-key answer | stray cleanup | credentials | tested against |
|---|---|---|---|---|---|---|---|
| HTTP endpoint: webhooks, Elasticsearch / OpenSearch, Varnish | `http` (also `https`, or empty), and the auth variants `http.basic`, `http.bearer`, `http.header` ([targets](docs/reference.md#targets)) | `http(s)://host:port[/prefix]`, or, for these four kinds, one `env:` / `file:` reference whose value is the whole URL ([secret http endpoints](docs/reference.md#secret-http-endpoints)) | one HTTP request, optionally signed ([webhook signing](docs/reference.md#webhook-signing)) | for the action | allowed | the auth variants' `vars`: a literal, or an `env:` / `file:` reference; the signing secret `vars.sign_secret_ref`: a reference only | OpenSearch 2.19, Elasticsearch 7.17 / 8.17 (security on), Varnish 7.6 — CI |
| S3 API | `http.s3` ([targets](docs/reference.md#targets)) | the service URL, optional (derived from `vars` when empty) | one signed S3 request | for the action | allowed | `access_key_ref` / `secret_key_ref` (references only; optional `session_token_ref`), else the [AWS default chain](docs/reference.md#aws-credentials) | versitygw 1.10 — CI |
| Redis / Valkey, Streams included | `redis` ([targets](docs/reference.md#targets)) | `host:port/db` (or `redis://` / `rediss://`) | one Redis command | for the action | allowed | `vars.password`, optional `vars.username` for ACLs: a literal or a reference | Redis 7.4, Valkey 8.1 — CI |
| MySQL / MariaDB | `mysql` (also `mariadb`) ([SQL targets](docs/reference.md#sql-targets)) | `host:port/database` | one SQL statement | for the action | allowed | `vars.username`, `vars.password`: a literal or a reference | MySQL 8.4 — CI |
| PostgreSQL | `pgsql` (also `postgres`, `postgresql`) ([SQL targets](docs/reference.md#sql-targets)) | `host:port/database` | one SQL statement | for the action | allowed | `vars.username`, `vars.password`: a literal or a reference | PostgreSQL 17 — CI |
| Amazon SQS | `sqs` ([SQS targets](docs/reference.md#sqs-targets)) | `aws-sqs://[<region>/]<account-id>/<queue-name>` | one `SendMessageBatch` | per entry, per key | refused at load (append-only; a purging `once` step too) | as `http.s3`; never in the URL | ElasticMQ 1.6 — CI (release gate) |
| Amazon Kinesis Data Streams | `kinesis` ([Kinesis targets](docs/reference.md#kinesis-targets)) | `aws-kinesis://[<region>/]<stream-name>` | one `PutRecords` | per record, per key | refused at load (append-only; a stream-deleting `once` step too) | as `http.s3`; never in the URL | kinesis-mock 0.2 — CI (release gate) |
| AWS Lambda | `lambda` ([Lambda targets](docs/reference.md#lambda-targets)) | `aws-lambda://[<region>/]<function-name>[:<qualifier>]` (`?invocation=sync\|event`) | one `Invoke` | per item: the result's `batchItemFailures` | refused at load (a function cannot list its invocations) | as `http.s3`; never in the URL | a stand-in Invoke service — CI |
| Amazon EventBridge | `eventbridge` ([EventBridge targets](docs/reference.md#eventbridge-targets)) | `aws-eventbridge://[<region>/]<event-bus-name-or-arn>` | one `PutEvents` | per entry, per key | refused at load (append-only; a bus-deleting `once` step too) | as `http.s3`; never in the URL | moto 5.2.5 — CI (release gate) |
| Amazon SNS | `sns` ([SNS targets](docs/reference.md#sns-targets)) | `aws-sns://[<region>/]<account-id>/<topic-name>` | one `PublishBatch` | per entry, per key | refused at load (append-only; a topic-deleting `once` step too) | as `http.s3`; never in the URL | moto 5.2.5 — CI (release gate) |
| AMQP 0-9-1 broker | `amqp` ([AMQP targets](docs/reference.md#amqp-targets)) | `amqp(s)://host[:port][/vhost]` | one confirmed publish | for the action (one message per action: `fire_per = key`) | refused at load (append-only) | `vars.username`, `vars.password`: a literal or a reference; never in the URL | RabbitMQ 4.3 — CI (release gate) |
| SMTP server | `smtp` ([SMTP targets](docs/reference.md#smtp-targets)) | `smtp(s)://host[:port]` (587 / 465 by default) | one mail | for the action (one message per action: `fire_per = key`) | refused at load (append-only) | `vars.username`, `vars.password`: a literal or a reference; never in the URL | Mailpit 1.33 — CI (release gate) |
| Google Cloud Pub/Sub | `pubsub` ([Pub/Sub targets](docs/reference.md#pubsub-targets)) | `gc-pubsub://<project>/<topic>` | one `topics.publish` | for the action | refused at load (append-only; publish-only reset steps) | `vars.credentials_ref` (reference only), else Application Default Credentials; never in the URL | the Pub/Sub emulator — CI (release gate) |
| Kafka-protocol broker (Apache Kafka, Redpanda, MSK, Event Hubs) | `kafka` ([Kafka targets](docs/reference.md#kafka-targets)) | `kafka(s)://<broker>[:port][,<broker>[:port]…]/<topic>` | one produce of a batch of records | per record, per key | refused at load (append-only; a listing is refused too) | `vars.sasl` (`plain`, `scram-sha-256`, `scram-sha-512`) with `vars.username` (a literal or a reference) and `vars.password` (a reference only); never in the URL | Redpanda 26.1.20 — CI (release gate) |
| RabbitMQ stream (native stream protocol, super streams included) | `rabbitmq-stream` ([RabbitMQ stream targets](docs/reference.md#rabbitmq-stream-targets)) | `rabbitmq-stream://<host>[:port]/[<vhost>/]<stream>`, or `rabbitmq-stream+tls://…` for TLS (5552 / 5551) | one publish batch of records | per record, per key | refused at load (append-only; a listing is refused too) | `vars.username` (a literal or a reference) and `vars.password` (a reference only); never in the URL | RabbitMQ 4.3 with the stream plugin — CI (release gate) |
| Azure Service Bus (queues, topics) | `servicebus` ([Service Bus targets](docs/reference.md#service-bus-targets)) | `sb://<namespace>.servicebus.windows.net/<queue-or-topic>` (a local broker: `sb://<host>:<port>/<address>`) | one batch of messages over AMQP 1.0, TLS on 5671, a disposition awaited per message | per message, per key | refused at load (append-only; a listing is refused too) | `vars.key_name` with `vars.key` (a reference only) for a SAS token over `$cbs`, or `vars.username` with `vars.password` (a reference only) for SASL PLAIN on a broker without CBS; no Entra ID; never in the URL | RabbitMQ 4.3 answering AMQP 1.0 on its 5672 (SASL PLAIN; no local broker has a `$cbs` node) — CI (release gate) |

The append-only kinds and `lambda` refuse a stray cleanup at load, and `sqs` / `kinesis` /
`eventbridge` / `sns` / `pubsub` refuse purge-like or non-publishing reset steps; no other kind
refuses one.

## Development

```sh
make test                      # unit tests
make servers                   # MySQL 8.4, MariaDB 10.11, Redis, Valkey, PostgreSQL, S3 (docker)
make test-e2e                  # integration tests on MySQL 8.4; test-e2e-all: MySQL 8.4 and MariaDB 10.11
make test-matrix               # every other blocking line of "Supported servers", one at a time
make test-prerelease           # the advisory MariaDB 13.1 RC and 13.2 preview
make test-e2e-search           # OpenSearch, Elasticsearch 7 and 8, one at a time
make test-e2e-queues           # SQS, Kinesis, AMQP, Service Bus, RabbitMQ Streams, Pub/Sub, SMTP, Kafka, EventBridge and SNS against their emulators (the sqs, kinesis, amqp, pubsub, smtp, kafka, eventbridge and sns compose profiles; the servicebus flows ride the amqp profile's RabbitMQ, whose listener answers AMQP 1.0 too, and the rabbitmq-stream flows its stream plugin on port 5552)
make coverage                  # unit tests + e2e under cargo-llvm-cov 0.9.1 into tmp/coverage (informational, not a gate)
docker buildx build -t lazywf:dev --load .   # static musl binary on distroless
make docker-smoke docker-e2e   # the loaded lazywf:dev: no shell, uid 65532; a row fires through Redis
```

The integer key sets have two backends: a hash set by default, and 64-bit roaring bitmaps
behind the `roaring` Cargo feature (`cargo build --release --features roaring`). `make test`
and `make lint` cover both; `make test-roaring` the roaring one alone. The roaring backend
pays on very large dense resets and snapshots (a million consecutive ids take under 300 KB,
against about 19 MB in the hash set) but costs more for sparse ids (about 212 bytes per
key); the default is what the integration suite and the published image use.

On macOS the test servers (`make servers`) need Docker. On Windows, develop inside WSL 2, with
Docker for the test servers.

CI ([ci.yml](.github/workflows/ci.yml)) runs on every push and pull request, weekly and on demand:
lint, unit tests, the integration suite on the default servers (Varnish included), on every other
server (one runner each, pre-releases advisory), with the search engines, with the queue
emulators (SQS, Kinesis, AMQP, Service Bus, RabbitMQ Streams, Pub/Sub, SMTP, Kafka (Redpanda),
EventBridge (moto) and SNS (moto)), and the image for both platforms. Two jobs run on narrower
triggers: the `coverage (informational)` job (pull requests and pushes to `main`; the table in the
job summary, the lcov and HTML in the `coverage` artifact for 14 days) and the weekly
`lazyview drift (advisory)` job (`scripts/sync-lazyview.sh --drift`, a warning per drifted file,
never red). Pushes to `main` publish `edge` once every blocking job passes; `v*` tags a release,
which also needs the search engines and the queue emulators. Dependabot bumps actions, crates,
base images and the test servers' patch builds weekly.

Every file `vendor/lazyview.lock` lists is an unmodified copy of
[lazyview](https://github.com/kirmorozov/lazyview)'s, pinned in that lock and updated with
`scripts/sync-lazyview.sh`. Four of its groups are used by the engine: the binlog reader
(`src/binlog.rs`), the commit fence (`src/fence.rs`, used by `src/engine.rs`, `src/action.rs` and
`src/main.rs`), the dedup core (`src/dedup/`: its `Buffer` is what the engine's per-rule flush
buffer wraps, and its ids back the rule keys, which `src/keyspace.rs` maps from text onto the
core's id spaces) and the integer key set (`src/intset.rs`, the set the core's integer spaces
keep their numbers in); the core's tuple ids live on the copied tuple datum (`src/datum.rs`).
One is not used by the engine: the view planner (`src/analyze.rs`, `src/analyzer/`,
`src/access.rs`, `src/catalog.rs`), which startup only asks to refuse lazywf's own tables as view
sources (`analyze::set_reserved`, `src/engine.rs` and `src/main.rs`). The weekly `lazyview drift
(advisory)` job above warns when lazyview's `main` moves a locked file or adds one the copy set
should gain.

License: MIT.
