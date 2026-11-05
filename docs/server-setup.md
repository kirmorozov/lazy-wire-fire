# Server setup

What the source server (the one whose binlog lazywf reads) needs: binlog settings, an account,
and, on Amazon RDS and Aurora, a few parameter-group and retention settings. The tested server
lines are in the [README](../README.md#supported-servers).

## Binlog settings

| setting | value | checked |
|---|---|---|
| `log_bin` | on | at start: refused otherwise |
| `binlog_format` | `ROW` | at start: refused otherwise |
| `binlog_row_image` | `FULL` | at start: refused otherwise |
| `binlog_row_value_options` (MySQL) | empty (no `PARTIAL_JSON`) | at start: refused otherwise |
| `log_bin_compress` (MariaDB) | `OFF` | at start: refused otherwise |
| `binlog_storage_engine` (MariaDB 12.3+) | unset | at start: refused otherwise |
| `binlog_row_metadata` | `FULL` recommended | column names then come from the table map, so an `ALTER` of a watched table never resets its rules; otherwise they come from `information_schema`, and a change in its column count does |
| `gtid_mode`, `enforce_gtid_consistency` (MySQL, Percona) | `ON` recommended | the visibility fence, below |
| `binlog_order_commits` (MySQL, Percona) | `ON` (the server default) | by the commit fence's probe: with it `OFF`, flushes fall back to `LAZYWF_VISIBILITY_DELAY_MS` (a WARN) |
| binlog expiry (`binlog_expire_logs_seconds`) | longer than the longest outage to ride out | a restart after a longer one finds its position purged and runs every rule's reset steps |

Before reading, a flush waits until the transaction that marked its keys is visible to other
sessions. It takes the first of these that the server can answer:

- with GTIDs (MySQL / Percona): `WAIT_FOR_EXECUTED_GTID_SET`, exact;
- on MariaDB and Percona where it reports one: the consistent-snapshot binlog position, exact;
- on MySQL / Percona with neither: the [commit fence](#commit-fence), exact when commits are in
  binlog order;
- otherwise: `LAZYWF_VISIBILITY_DELAY_MS`.

### Commit fence

On a server without GTIDs and without a consistent-snapshot position (Oracle MySQL without
GTIDs), a flush writes one row of lazywf's own table `lazy_wire_fire_fence` (in the DSN's schema;
the probe creates it when it is missing) on the source connection. With `binlog_order_commits=ON`
— the server default; on Aurora, the enhanced binlog counts whatever the variable says — the
server commits transactions in binlog order, so the write returns only after every transaction
up to the flush's boundary is visible. One write per flush boundary: flushes at the same boundary
share the write, events never write it, and the counter grows with flushes, not with changes.
The table's own transactions are lazywf's bookkeeping: they never move a rule's position, and
routing never treats the table as a source. Nothing else needs to be configured.

The fence needs `INSERT` and `UPDATE` on `lazy_wire_fire_fence`, and `CREATE` on the schema only
while the table is missing (the probe creates it then; once it exists — for example created by an
administrator — an account without `CREATE` works). The account in [the next section](#account)
has all three.

When `binlog_order_commits` is `OFF`, or when the probe or the write fails, a flush waits
`LAZYWF_VISIBILITY_DELAY_MS` instead; each cause is logged once per process, at WARN:

- commits not in binlog order: `commit fence unavailable: binlog_order_commits is OFF; flushes wait LAZYWF_VISIBILITY_DELAY_MS instead (ON gives an exact fence)`
- the probe failed (for example no `CREATE` on the schema while the table is missing): `commit fence probe failed: …; flushes wait LAZYWF_VISIBILITY_DELAY_MS instead`
- the write failed: `commit fence write failed: …; flushes wait LAZYWF_VISIBILITY_DELAY_MS instead`

MariaDB and the GTID path do not change.

lazywf connects as a replica with its own `server_id` (`LAZYWF_SERVER_ID`, random by default).

lazywf creates no triggers and needs no trigger privilege; other applications on the same server
may. With binary logging on, a `CREATE TRIGGER` by an account without `SUPER` is refused (MySQL
error 1419) unless `log_bin_trust_function_creators=1` — that is the database administrator's
decision, and lazywf does not need it.

### Binlog filters

`binlog-do-db` and `binlog-ignore-db` must not drop lazywf's own schema (the DSN's schema,
where the status and commit-fence writes go) or the schema of an enabled trigger: with row
logging a change to a dropped schema is not binlogged at all.

lazywf checks the filters at start and when the trigger set changes, once per schema (a
schema whose filters cannot be read is checked again on the next reload). The check is
advisory: it never refuses to start. A dropped schema is logged at WARN:

- a source schema: `the server's binlog filter drops source schema \`<schema>\` (…): changes in it are not binlogged, so its triggers never fire; remove the filter or include the schema`
- the own schema: `the server's binlog filter drops lazywf's own schema \`<schema>\` (…): the status and commit-fence writes are not binlogged, so the fence cannot prove visibility; remove the filter or include the schema`

The (…) is `Binlog_Do_DB does not list it` (the schema is not in `binlog-do-db`) or
`Binlog_Ignore_DB lists it` (it is in `binlog-ignore-db`); when both drop the schema the
first is reported. The fix: remove the filter, or include the schema in `binlog-do-db`. The
checked schemas the filters drop are on `/status` as `binlog_filtered_schemas`
([reference](reference.md#status)).

## Account

Run once as an admin. `app` is the schema of the DSN, which holds lazywf's tables. Add a
`SELECT` grant for every other schema the rules read.

```sql
CREATE USER 'lazywf'@'%' IDENTIFIED BY 'change-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lazywf'@'%';
-- MariaDB: GRANT BINLOG MONITOR ON *.* TO 'lazywf'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON app.* TO 'lazywf'@'%';
```

Nothing else, and no admin right (`SUPER`, `SYSTEM_VARIABLES_ADMIN`, …): the integration suite
runs an account with exactly these grants on every tested server. The replication grants can
live on a separate account (`LAZYWF_BINLOG_USER`, `LAZYWF_BINLOG_PASSWORD`).

To keep the account's write rights off the source, put the tool's tables in a schema of their
own: the DSN points at that schema (`app_state` here), and the source gets `SELECT` only.

```sql
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON app_state.* TO 'lazywf'@'%';
GRANT SELECT ON app.* TO 'lazywf'@'%';
```

The rules' queries then name the source's tables qualified (`FROM app.orders o`), since
unqualified names resolve in the DSN's schema ([reference](reference.md#rules)).

### Query account

The SQL the configuration defines (rule selects, trigger `keys_sql`, reset-step queries, and the
`expect` / `keys` / `next` expressions) can run on a separate account instead of the one above:

- `LAZYWF_QUERY_DSN`: a full DSN for that account (the main account's TLS settings apply); it
  takes precedence over the pair below; or
- `LAZYWF_QUERY_USER` and `LAZYWF_QUERY_PASSWORD`: the main DSN's host, port and schema with the
  query credentials.

With neither set, the main account runs everything: the behaviour above, unchanged.

Recommend a SELECT-only account on the source tables the rules read — it can do no more than the
rules read:

```sql
CREATE USER 'lwf_query'@'%' IDENTIFIED BY 'change-me';
GRANT SELECT ON app.orders TO 'lwf_query'@'%';
GRANT SELECT ON app.customers TO 'lwf_query'@'%';
```

(one `GRANT` per source table; `SELECT` on the whole source schema is the looser form).
The source's `SELECT` grants then move to the query account: give the main account only the
rights on its own schema and the replication grants. What stays on the main account: the tool's
tables, the status writes, the commit fence and the instance lock. Target connections keep their
own credentials.

lazywf does not check the first keyword of the configuration's SQL: the `_key` column keeps
plain DML out of the queries, but a `CALL` or a side-effecting function that returns `_key` rows
passes the load checks. The account's grants are the real boundary — `SELECT` only, no write
privilege, and no `EXECUTE` on stored routines that write.

At start the account is probed with `SELECT 1`: a failure stops the start, and the error names
`LAZYWF_QUERY_*` (never the password). The start line logs `query_account=<user>` (the user name
only), or `none (the main account runs the queries)` when unset.

## Amazon RDS and Aurora

lazywf runs on Amazon RDS for MySQL and MariaDB and on Aurora MySQL, on the lines AWS offers that
are also tested here: RDS for MySQL 8.0 (Extended Support) and 8.4; Aurora MySQL 3 (8.0) and 8.4;
RDS for MariaDB 10.6 (until RDS ends it on 2026-12-31), 10.11, 11.4, 11.8 and 12.3. CI cannot run
on AWS itself. It runs the same server lines and RDS's privilege model (the account above, with no
admin rights), and the startup checks below are unit-tested.

* **Binary logging.** RDS for MySQL and MariaDB turn it on when automated backups are enabled
  (backup retention of at least one day). Aurora MySQL turns it on when `binlog_format` is set in
  the DB cluster parameter group.
* **Parameter group** (Aurora: the DB cluster parameter group):
  * `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_value_options` empty, optionally
    `binlog_row_metadata=FULL`;
  * on MySQL, `gtid_mode=ON` and `enforce_gtid_consistency=ON` for the exact visibility fence.
    lazywf logs a tip at startup when GTIDs are off.

  Reboot the instance (Aurora: the writer) when a static parameter changed.
* **Binlog retention.** RDS purges binlogs within minutes by default, so a restart after any
  outage would find its position gone and run every rule's reset steps. lazywf warns at startup
  when the retention is under 12 hours (or cannot be read). Run once as the master user:

  ```sql
  CALL mysql.rds_set_configuration('binlog retention hours', 24);
  ```

* **Account.** The master user can create the account above; nothing needs `rds_superuser_role`.
* **Endpoints.** Connect to the instance endpoint or the Aurora cluster (writer) endpoint
  directly. Two things are not supported:
  * RDS Proxy: lazywf reads the binlog as a replica and holds a named lock (`GET_LOCK`, one active copy per
    schema and instance name) for its whole session, and a pooling proxy passes neither through;
  * a reader endpoint: status writes and the lock need the writer.
* **Failover.** lazywf compares the server identity (MySQL `@@server_uuid`, MariaDB
  `@@server_id` and `@@gtid_domain_id`) with the one it saved. If a failover changes it, that
  counts as another server: continuity is lost, and rules with reset steps re-sync.
* **TLS.** RDS and Aurora certificates come from Amazon's own CA: set `LAZYWF_TLS=verify-full`
  and `LAZYWF_TLS_CA` to the RDS certificate bundle (`global-bundle.pem` from AWS's trust store).
  Aurora MySQL 8.4 enforces TLS by default.
