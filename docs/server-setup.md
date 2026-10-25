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
| `binlog_row_metadata` | `FULL` optional | column names then come from the table map, else from `information_schema` |
| `gtid_mode`, `enforce_gtid_consistency` (MySQL, Percona) | `ON` recommended | the visibility fence, below |
| binlog expiry (`binlog_expire_logs_seconds`) | longer than the longest outage to ride out | a restart after a longer one finds its position purged and runs every rule's reset steps |

Before reading, a flush waits until the transaction that marked its keys is visible to other
sessions:

- with GTIDs: `WAIT_FOR_EXECUTED_GTID_SET`, exact;
- on MariaDB and Percona: the consistent-snapshot binlog position, exact;
- otherwise: `LAZYWF_VISIBILITY_DELAY_MS`.

lazywf connects as a replica with its own `server_id` (`LAZYWF_SERVER_ID`, random by default).

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
  * RDS Proxy: lazywf reads the binlog as a replica and holds a named lock (`GET_LOCK`, one active
    copy per instance name) for its whole session, and a pooling proxy passes neither through;
  * a reader endpoint: status writes and the lock need the writer.
* **Failover.** lazywf compares the server identity (MySQL `@@server_uuid`, MariaDB
  `@@server_id` and `@@gtid_domain_id`) with the one it saved. If a failover changes it, that
  counts as another server: continuity is lost, and rules with reset steps re-sync.
* **TLS.** RDS and Aurora certificates come from Amazon's own CA: set `LAZYWF_TLS=verify-full`
  and `LAZYWF_TLS_CA` to the RDS certificate bundle (`global-bundle.pem` from AWS's trust store).
  Aurora MySQL 8.4 enforces TLS by default.
