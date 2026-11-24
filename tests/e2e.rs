//! End to end: a MySQL-family server (`LAZYWF_TEST_DSN`), the engine in-process, and HTTP,
//! Redis and S3 targets. `make servers test-e2e`.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use mysql_async::prelude::*;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

const BULK: &str = "POST /orders/_bulk HTTP/1.1
Content-Type: application/x-ndjson

{{#each docs}}{{#if (eq op \"delete\")}}{\"delete\":{\"_id\":\"{{key}}\"}}
{{else}}{\"update\":{\"_id\":\"{{key}}\"}}
{\"doc\":{{json doc}},\"doc_as_upsert\":true}
{{/if}}{{/each}}";

async fn shop(db: &Db) {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20), total DECIMAL(10,2))").await;
    db.exec("CREATE TABLE order_tags (order_id INT, tag VARCHAR(20), PRIMARY KEY (order_id, tag))")
        .await;
    tables(db).await;
}

/// The last document each key received in `_bulk` bodies (`None`: deleted).
fn last_docs(reqs: &[Req]) -> std::collections::BTreeMap<String, Option<Value>> {
    let mut out = std::collections::BTreeMap::new();
    for r in reqs {
        let lines = r.ndjson();
        let mut i = 0;
        while i < lines.len() {
            if let Some(d) = lines[i].get("delete") {
                out.insert(d["_id"].as_str().unwrap().to_string(), None);
                i += 1;
            } else {
                let id = lines[i]["update"]["_id"].as_str().unwrap().to_string();
                out.insert(id, Some(lines[i + 1]["doc"].clone()));
                i += 2;
            }
        }
    }
    out
}

async fn order_rule(db: &Db, mock: &Mock) {
    db.target(
        "search",
        "http",
        &mock.url(),
        Some("JSON_CONTAINS(body, 'false', '$.errors')"),
        "{}",
    )
    .await;
    let selects = json!([
        "SELECT o.id AS _key, o.status AS status, o.total AS `amount.total`, c.name AS `customer.name`, \
         IF(o.status = 'cancelled', 'delete', NULL) AS _op \
         FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.id IN (:keys)",
        "SELECT o.id AS _key, t.tag AS `tags[]` FROM orders o LEFT JOIN order_tags t ON t.order_id = o.id \
         WHERE o.id IN (:keys) ORDER BY t.tag"
    ]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, template, debounce_ms, max_delay_ms) VALUES ('orders', 'search', ?, ?, 500, 2000)",
        (selects.to_string(), BULK),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    db.trigger("orders", "order_tags", Some("{{new.order_id}}"), None)
        .await;
    db.trigger(
        "orders",
        "customers",
        None,
        Some("SELECT id AS _key FROM orders WHERE customer_id = :new.id"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn documents_follow_the_rows() {
    let root = need_db!();
    let db = Db::fresh(&root, "docs").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, r#"{"errors":false,"items":[]}"#.into()));
    order_rule(&db, &mock).await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    db.exec("INSERT INTO orders VALUES (10, 1, 'new', 12.50), (11, 2, 'new', 3.00)")
        .await;
    db.exec("INSERT INTO order_tags VALUES (10, 'gift'), (10, 'express')")
        .await;
    let docs = wait_for(
        Duration::from_secs(15),
        "documents for 10 and 11",
        || async {
            let d = last_docs(&mock.requests());
            let ok = d
                .get("10")
                .cloned()
                .flatten()
                .is_some_and(|v| v["tags"] == json!(["express", "gift"]))
                && d.get("11").cloned().flatten().is_some();
            ok.then_some(d)
        },
    )
    .await;
    assert_eq!(
        serde_json::to_string(&docs["10"]).unwrap(),
        r#"{"status":"new","amount":{"total":12.50},"customer":{"name":"Ann"},"tags":["express","gift"]}"#
    );
    assert_eq!(docs["11"].as_ref().unwrap()["tags"], json!([]));
    let r = &mock.requests()[0];
    assert_eq!(
        (r.method.as_str(), r.path.as_str()),
        ("POST", "/orders/_bulk")
    );
    assert_eq!(r.header("content-type"), Some("application/x-ndjson"));
    assert_eq!(r.header("host"), Some(mock.addr.to_string().as_str()));

    // A change in a related table reaches the orders through keys_sql (after the trailing
    // fires of the inserts are done).
    inst.quiet().await;
    mock.clear();
    db.exec("UPDATE customers SET name = 'Ann B' WHERE id = 1")
        .await;
    wait_for(
        Duration::from_secs(10),
        "order 10 with the new customer name",
        || async {
            let d = last_docs(&mock.requests());
            d.get("10")
                .cloned()
                .flatten()
                .filter(|v| v["customer"]["name"] == "Ann B")
        },
    )
    .await;
    assert!(
        !last_docs(&mock.requests()).contains_key("11"),
        "order 11 belongs to another customer"
    );

    // A burst of changes to one key fires it once (or twice with a trailing fire). On a
    // loaded host the 30 commits can take longer than the debounce, which adds fires.
    inst.quiet().await;
    mock.clear();
    // On one connection: a burst, not 30 logins (those alone can outlast the debounce).
    let mut c = db.conn().await;
    let started = std::time::Instant::now();
    for i in 0..30 {
        c.query_drop(format!("UPDATE orders SET total = {i} WHERE id = 11"))
            .await
            .unwrap();
    }
    let burst = started.elapsed();
    c.disconnect().await.ok();
    let d = wait_for(
        Duration::from_secs(10),
        "the burst to settle on 29",
        || async {
            let d = last_docs(&mock.requests());
            d.get("11")
                .cloned()
                .flatten()
                .filter(|v| serde_json::to_string(&v["amount"]["total"]).unwrap() == "29.00")
        },
    )
    .await;
    assert_eq!(
        d["amount"]["total"].to_string(),
        "29.00",
        "DECIMAL stays exact"
    );
    sleep_ms(500).await;
    let fires = mock
        .requests()
        .iter()
        .filter(|r| r.body.contains("\"_id\":\"11\""))
        .count();
    let allowed = 3 + burst.as_millis() as usize / 500;
    assert!(
        fires <= allowed,
        "30 updates in {burst:?} fired {fires} times"
    );

    // `_op`: cancelled orders and orders gone are deleted.
    inst.quiet().await;
    mock.clear();
    db.exec("UPDATE orders SET status = 'cancelled' WHERE id = 11")
        .await;
    db.exec("DELETE FROM orders WHERE id = 10").await;
    wait_for(Duration::from_secs(10), "deletes", || async {
        let d = last_docs(&mock.requests());
        (d.get("10") == Some(&None) && d.get("11") == Some(&None)).then_some(())
    })
    .await;

    // The status row catches up with the stream.
    wait_for(Duration::from_secs(10), "status position", || async {
        let (f, p, reset, err) = db.status("orders").await?;
        let (sf, sp, _, _) = db.status("*").await?;
        (!f.is_empty() && (f.clone(), p) >= (sf, sp) && err.is_none()).then_some(reset)
    })
    .await;
    assert!(db
        .status("orders")
        .await
        .unwrap()
        .2
        .starts_with("missing: first start"));
    inst.stop().await;
}

/// Where the server's binlog has reached *at this moment*. Other sessions (the engine's status
/// writes, the tests running in parallel on this server) append to the same binlog, so this is a
/// bound, never the start or end of someone else's transaction: use it as a lower bound after the
/// writes you care about (the `>= after` pattern), or read the real position back afterwards, as
/// [`xa_group_start`] does.
async fn binlog_end(db: &Db) -> (String, u64) {
    let mut c = db.conn().await;
    let (major, minor, _) = c.server_version();
    let q = if major > 8 || (major == 8 && minor >= 2) && major < 10 {
        "SHOW BINARY LOG STATUS"
    } else {
        "SHOW MASTER STATUS"
    };
    let row: mysql_async::Row = match c.query_first(q).await {
        Ok(Some(r)) => r,
        _ => c.query_first("SHOW MASTER STATUS").await.unwrap().unwrap(),
    };
    c.disconnect().await.ok();
    (
        row.get::<String, _>(0).unwrap(),
        row.get::<u64, _>(1).unwrap(),
    )
}

/// Where the prepared XA transaction `xid` begins in the binlog: the position a rule holding one
/// of its unflushed keys must stay at or below, and where a restart has to resume to see the
/// PREPARE at all. A [`binlog_end`] snapshot taken before `XA START` is only a lower bound for
/// it: another session — the engine's own status write, or one of the tests running in parallel
/// on this server — can commit between the two, and then the XA's group starts that much later
/// and the engine holds the floor at *that* position. `None`: no `XA PREPARE`/`XA START` of `xid`
/// in the events after `from`, or that group is not in the window at all.
async fn xa_group_start(db: &Db, from: &(String, u64), xid: &str) -> Option<(String, u64)> {
    let hex: String = xid.bytes().map(|b| format!("{b:02x}")).collect();
    let needle_hex = format!("X'{hex}'").to_uppercase();
    let needle_prepare = format!("XA PREPARE '{xid}'").to_uppercase();
    let needle_start = format!("XA START '{xid}'").to_uppercase();
    let mut c = db.conn().await;
    let rows: Vec<mysql_async::Row> = c
        .query(format!(
            "SHOW BINLOG EVENTS IN '{}' FROM {} LIMIT 10000",
            from.0.replace('\'', "''"),
            from.1
        ))
        .await
        .unwrap_or_else(|e| panic!("SHOW BINLOG EVENTS from {from:?}: {e}"));
    c.disconnect().await.ok();
    // What the engine pins is the transaction *boundary* it was holding when `XA START` arrived:
    // the end of the last event that closed a transaction before it (`end_txn` in the engine, fed by
    // `Xid`, `COMMIT`/`ROLLBACK`, a bare statement, `XA END`/`XA PREPARE`), which is exactly where
    // the XA's group begins. So walk the same way instead of looking for a group opener: which
    // event opens a group differs per server (`Gtid`, `Anonymous_Gtid` with GTIDs off, `Query
    // BEGIN`), while what closes one does not.
    let mut group = from.1;
    for row in rows {
        let kind: String = row.get(2).expect("Event_type");
        let info: String = row.get(5).unwrap_or_default();
        let up = info.to_uppercase();
        let ours =
            up.contains(&needle_hex) || up.contains(&needle_prepare) || up.contains(&needle_start);
        if ours {
            return Some((from.0.clone(), group));
        }
        if kind == "Xid"
            || kind.eq_ignore_ascii_case("XA_prepare")
            || (kind == "Query" && !up.starts_with("BEGIN"))
        {
            group = row.get(4).expect("End_log_pos");
        }
    }
    None
}

/// The server's own clock as a `TIMESTAMP(3)` literal, so a status row can be asked whether the
/// engine wrote it after a given moment without comparing two machines' clocks.
async fn server_now(db: &Db) -> String {
    let mut c = db.conn().await;
    let row: mysql_async::Row = c
        .query_first("SELECT DATE_FORMAT(CURRENT_TIMESTAMP(3), '%Y-%m-%d %H:%i:%s.%f')")
        .await
        .unwrap()
        .expect("CURRENT_TIMESTAMP");
    c.disconnect().await.ok();
    row.get::<String, _>(0).unwrap()
}

/// `(file, pos, reset_state, last_error)` of a rule's status row, once the engine has *written* it
/// after `written_after` (the server's clock from [`server_now`]). `status::write` stamps
/// `updated_at = NOW(3)` on every write and `persist` rewrites a row whose position has moved, so
/// this cannot hand back a row left over from an earlier persist.
async fn status_row(
    db: &Db,
    rule: &str,
    written_after: &str,
) -> Option<(String, u64, String, Option<String>)> {
    let mut c = db.conn().await;
    let r = c
        .exec_first(
            "SELECT binlog_file, binlog_pos, reset_state, last_error FROM lazy_wire_fire_status \
             WHERE rule = ? AND updated_at > ?",
            (rule, written_after),
        )
        .await
        .unwrap();
    c.disconnect().await.ok();
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failures_retry_and_hold_the_position() {
    let root = need_db!();
    let db = Db::fresh(&root, "retry").await;
    shop(&db).await;
    let down = Mock::start().await;
    down.respond(|_| (503, "unavailable".into()));
    let up = Mock::start().await;
    db.target("down", "http", &down.url(), None, "{}").await;
    db.target("up", "http", &up.url(), None, "{}").await;
    for (rule, target) in [("a", "down"), ("b", "up")] {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, 'POST /hook HTTP/1.1\n\n{{join keys \",\"}}', 100)",
            (rule, target),
        )
        .await;
        db.trigger(rule, "orders", Some("{{new.id}}"), None).await;
    }
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    // The insert ended before this point (the binlog is shared with other tests).
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(
        Duration::from_secs(10),
        "retries on the failing target",
        || async { (down.requests().len() >= 3).then_some(()) },
    )
    .await;
    wait_for(Duration::from_secs(10), "the working rule", || async {
        up.requests().iter().any(|r| r.body == "1").then_some(())
    })
    .await;
    wait_for(
        Duration::from_secs(10),
        "rule b past the insert",
        || async {
            let (bf, bp, _, _) = db.status("b").await?;
            ((bf, bp) >= after).then_some(())
        },
    )
    .await;
    sleep_ms(1000).await;
    let (af, ap, _, err) = db.status("a").await.unwrap();
    assert!((af, ap) < after, "rule a moved past its failing action");
    assert!(err.as_deref().is_some_and(|e| e.contains("503")), "{err:?}");
    let before = after;
    // Pending keys survive a restart: they are read again from the saved position.
    inst.stop().await;
    down.respond(|_| (200, "{}".into()));
    down.clear();
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(
        Duration::from_secs(10),
        "the key after the restart",
        || async { down.requests().iter().any(|r| r.body == "1").then_some(()) },
    )
    .await;
    wait_for(Duration::from_secs(10), "rule a to catch up", || async {
        let (af, ap, _, _) = db.status("a").await?;
        ((af, ap) > before).then_some(())
    })
    .await;
    inst.stop().await;
}

/// An account with only the documented grants (no SUPER or other admin rights: what Amazon
/// RDS lets the master user create) runs the tool end to end: it creates its tables, takes its
/// lock, streams the binlog, reads, fires and saves its position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn least_privilege_account() {
    let root = need_db!();
    let db = Db::fresh(&root, "grants").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20), total DECIMAL(10,2))")
        .await;
    let mariadb = {
        let mut c = db.conn().await;
        let v: String = c.query_first("SELECT VERSION()").await.unwrap().unwrap();
        c.disconnect().await.ok();
        v.contains("MariaDB")
    };
    let user = "'lwf_least'@'%'";
    db.exec(&format!("DROP USER IF EXISTS {user}")).await;
    db.exec(&format!("CREATE USER {user} IDENTIFIED BY 'least-pw'"))
        .await;
    db.exec(&format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT{} ON *.* TO {user}",
        if mariadb { ", BINLOG MONITOR" } else { "" }
    ))
    .await;
    db.exec(&format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON `{}`.* TO {user}",
        db.schema
    ))
    .await;
    let (scheme, rest) = root.split_once("://").unwrap();
    let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    let least = Db {
        schema: db.schema.clone(),
        dsn: format!(
            "{scheme}://lwf_least:least-pw@{host}/{}?prefer_socket=false",
            db.schema
        ),
    };
    // The tool's tables do not exist yet: the account creates them.
    let inst = Instance::ready(config(&least, &[])).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, template, debounce_ms) VALUES ('o', 'hook', \
         'SELECT id AS _key, status FROM orders WHERE id IN (:keys)', \
         'POST /o HTTP/1.1\n\n{{#each docs}}{{key}}={{doc.status}};{{/each}}', 100)",
    )
    .await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    // The rule is picked up from its own row events.
    sleep_ms(1500).await;
    db.exec("INSERT INTO orders VALUES (7, 1, 'paid', 3)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(15), "the action", || async {
        mock.requests()
            .iter()
            .any(|r| r.body == "7=paid;")
            .then_some(())
    })
    .await;
    wait_for(Duration::from_secs(10), "the saved position", || async {
        let (f, p, _, _) = db.status("o").await?;
        ((f, p) >= after).then_some(())
    })
    .await;
    inst.stop().await;
    db.exec(&format!("DROP USER {user}")).await;
}

/// The SQL that the configuration defines runs on the query account (`LAZYWF_QUERY_*`), a
/// SELECT-only account with grants on the two source tables alone: a rule's selects and a
/// trigger's `keys_sql` work under it; a select that reads past its grants is refused by the
/// server, and the rule's status names it; lazywf's own tables stay on the main account.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_account_runs_the_configured_sql() {
    let root = need_db!();
    let db = Db::fresh(&root, "queryacct").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    db.exec("CREATE TABLE secret (id INT PRIMARY KEY)").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20))")
        .await;
    let user = "'lwf_query'@'%'";
    db.exec(&format!("DROP USER IF EXISTS {user}")).await;
    db.exec(&format!("CREATE USER {user} IDENTIFIED BY 'query-pw'"))
        .await;
    for table in ["orders", "customers"] {
        db.exec(&format!(
            "GRANT SELECT ON `{}`.`{table}` TO {user}",
            db.schema
        ))
        .await;
    }
    let inst = Instance::ready(config(
        &db,
        &[
            ("LAZYWF_QUERY_USER", "lwf_query"),
            ("LAZYWF_QUERY_PASSWORD", "query-pw"),
        ],
    ))
    .await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let template = "POST /o HTTP/1.1\n\n{{#each docs}}{{key}}={{doc.status}};{{/each}}";
    let good = json!([{"name": "by_id", "sql": "SELECT id AS _key, status FROM orders WHERE id IN (:keys)"}]);
    let refused = json!([{"name": "by_id_locked", "sql": "SELECT id AS _key, status FROM orders WHERE id IN (:keys) AND id IN (SELECT id FROM secret)"}]);
    for (name, selects) in [("good", good), ("locked", refused)] {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, selects, template, debounce_ms) VALUES (?, 'hook', ?, ?, 100)",
            (name, selects.to_string(), template),
        )
        .await;
    }
    db.trigger("good", "orders", Some("{{new.id}}"), None).await;
    db.trigger(
        "good",
        "customers",
        None,
        Some("SELECT id AS _key FROM orders WHERE customer_id = :new.id"),
    )
    .await;
    db.trigger("locked", "orders", Some("{{new.id}}"), None)
        .await;
    // The rules are picked up from their own row events.
    sleep_ms(1500).await;
    db.exec("INSERT INTO orders VALUES (7, 1, 'paid')").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(15), "the select action", || async {
        mock.requests()
            .iter()
            .any(|r| r.body == "7=paid;")
            .then_some(())
    })
    .await;
    // The customer row's keys_sql finds the order: the same action again.
    mock.clear();
    db.exec("INSERT INTO customers VALUES (1, 'ann')").await;
    db.tick().await;
    wait_for(Duration::from_secs(15), "the keys_sql action", || async {
        mock.requests()
            .iter()
            .any(|r| r.body == "7=paid;")
            .then_some(())
    })
    .await;
    // The select reads a table the account has no grant on: the server refuses it, and the
    // status names the select. (A locking read, FOR UPDATE, is refused by MySQL only: MariaDB
    // needs just SELECT for it.)
    wait_for(
        Duration::from_secs(20),
        "the refusal in the status",
        || async {
            let (_, _, _, err) = db.status("locked").await?;
            err.filter(|e| e.contains("by_id_locked") && e.contains("denied"))
                .map(|_| ())
        },
    )
    .await;
    wait_for(Duration::from_secs(10), "the saved position", || async {
        let (f, p, _, _) = db.status("good").await?;
        ((f, p) >= after).then_some(())
    })
    .await;
    inst.stop().await;
    db.exec(&format!("DROP USER {user}")).await;
}

/// A query account that cannot connect stops the start, and the error names the setting, never
/// the password.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_account_that_cannot_connect_stops_the_start() {
    let root = need_db!();
    let db = Db::fresh(&root, "queryrefuse").await;
    let cfg = config(
        &db,
        &[
            ("LAZYWF_QUERY_USER", "lwf_nobody"),
            ("LAZYWF_QUERY_PASSWORD", "NOPE-query-7"),
        ],
    );
    let (_tx, rx) = watch::channel(false);
    let health = Arc::new(lazy_wire_fire::health::Health::default());
    let err = lazy_wire_fire::engine::run(cfg, health, None, rx)
        .await
        .expect_err("the start must fail");
    let shown = format!("{err:#}");
    assert!(shown.contains("LAZYWF_QUERY_"), "{shown}");
    assert!(!shown.contains("NOPE-query-7"), "{shown}");
}

/// `expect` is SQL over the response row: a condition on the body and a header holds the key
/// (and the rule's position) until the target's answer meets it; a condition naming `status`
/// replaces the 2xx default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expect_is_sql_over_the_response() {
    let root = need_db!();
    let db = Db::fresh(&root, "expect").await;
    shop(&db).await;
    let store = Mock::start().await;
    store.respond(|_| (200, r#"{"state":"busy"}"#.into()));
    store.headers(|_| vec![("X-Ack".into(), "yes".into())]);
    let gone = Mock::start().await;
    gone.respond(|_| (404, r#"{"found":false}"#.into()));
    db.target(
        "store",
        "http",
        &store.url(),
        Some(
            "JSON_UNQUOTE(JSON_EXTRACT(body, '$.state')) = 'stored' \
             AND JSON_UNQUOTE(JSON_EXTRACT(headers, '$.\"x-ack\"')) = 'yes'",
        ),
        "{}",
    )
    .await;
    db.target(
        "gone",
        "http",
        &gone.url(),
        Some("status IN (200, 404)"),
        "{}",
    )
    .await;
    for (rule, target) in [("a", "store"), ("b", "gone")] {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, 'POST /hook HTTP/1.1\n\n{{join keys \",\"}}', 100)",
            (rule, target),
        )
        .await;
        db.trigger(rule, "orders", Some("{{new.id}}"), None).await;
    }
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(10), "the 404 accepted", || async {
        let (f, p, _, _) = db.status("b").await?;
        ((f, p) >= after).then_some(())
    })
    .await;
    assert_eq!(gone.requests().len(), 1, "an accepted 404 is not retried");
    wait_for(
        Duration::from_secs(10),
        "retries while the body says busy",
        || async { (store.requests().len() >= 2).then_some(()) },
    )
    .await;
    sleep_ms(1000).await;
    let (af, ap, _, err) = db.status("a").await.unwrap();
    assert!((af, ap) < after, "rule a moved past an unmet expect");
    assert!(
        err.as_deref()
            .is_some_and(|e| e.contains("`expect` not met")),
        "{err:?}"
    );
    // Larger than 64 KB: the whole answer reaches the condition.
    let pad = "x".repeat(100_000);
    store.respond(move |_| (200, format!(r#"{{"pad":"{pad}","state":"stored"}}"#)));
    wait_for(
        Duration::from_secs(30),
        "rule a past the insert",
        || async {
            let (f, p, _, _) = db.status("a").await?;
            ((f, p) >= after).then_some(())
        },
    )
    .await;
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_steps_run_in_order() {
    let root = need_db!();
    let db = Db::fresh(&root, "reset").await;
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, title VARCHAR(50))")
        .await;
    db.exec("INSERT INTO items VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        .await;
    tables(&db).await;
    let mock = Mock::start().await;
    mock.respond(|r| {
        if r.path.starts_with("/items/_ids") {
            if r.path.contains("after=p1") {
                (200, r#"{"ids":["3","98"],"next":null}"#.into())
            } else {
                (200, r#"{"ids":["1","2","99"],"next":"p1"}"#.into())
            }
        } else {
            (200, "{}".into())
        }
    });
    db.target("idx", "http", &mock.url(), None, "{}").await;
    let steps = json!([
        {"once": true, "template": "DELETE /items/_cache HTTP/1.1\n\n"},
        {"for_each": "snapshot"},
        {"for_each": "stray", "list_template": "GET /items/_ids?after={{reset.next}} HTTP/1.1\n\n",
         "keys": "JSON_EXTRACT(body, '$.ids')", "next": "JSON_EXTRACT(body, '$.next')",
         "template": "POST /items/_strays HTTP/1.1\n\n{{json reset.keys}}"}
    ]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, snapshot_keys_sql, template, reset_steps, debounce_ms) \
         VALUES ('items', 'idx', 'SELECT id AS _key, title FROM items WHERE id IN (:keys)', 'SELECT id AS _key FROM items', \
         'POST /items/_bulk HTTP/1.1\n\n{{#each docs}}{{key}}={{doc.title}};{{/each}}', ?, 100)",
        (steps.to_string(),),
    )
    .await;
    db.trigger("items", "items", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    let order = wait_for(Duration::from_secs(15), "the three steps", || async {
        let reqs = mock.requests();
        let once = reqs
            .iter()
            .position(|r| r.method == "DELETE" && r.path == "/items/_cache")?;
        let bulk = reqs
            .iter()
            .position(|r| r.path == "/items/_bulk" && r.body.contains("5=e"))?;
        let stray = reqs.iter().position(|r| r.path == "/items/_strays")?;
        Some((once, bulk, stray, reqs[stray].body.clone()))
    })
    .await;
    assert!(order.0 < order.1 && order.1 < order.2, "{order:?}");
    let strays: Vec<String> = serde_json::from_str(&order.3).unwrap();
    assert_eq!(strays, vec!["98".to_string(), "99".to_string()]);
    wait_for(Duration::from_secs(5), "reset done", || async {
        db.status("items")
            .await
            .filter(|s| s.2.starts_with("done"))
            .map(|_| ())
    })
    .await;

    // On request, the steps run again and the flag is cleared.
    mock.clear();
    db.exec("UPDATE lazy_wire_fire_rule SET reset_requested = 1 WHERE name = 'items'")
        .await;
    wait_for(Duration::from_secs(15), "the requested reset", || async {
        let cleared: Vec<u8> = db
            .query("SELECT reset_requested FROM lazy_wire_fire_rule WHERE name = 'items'")
            .await;
        let reqs = mock.requests();
        (cleared == vec![0] && reqs.iter().any(|r| r.path == "/items/_strays")).then_some(())
    })
    .await;
    inst.stop().await;
}

/// The Redis rules against one server: a document per key (SET / DEL) and the
/// clear-by-tag script in the Cm_Cache_Backend_Redis layout.
async fn redis_rules(
    root: &str,
    name: &str,
    endpoint: &str,
    username: Option<&str>,
    password: &str,
) {
    use lazy_wire_fire::targets::redis::{Client, RedisAddr, Reply};
    use lazy_wire_fire::tls::TlsSpec;
    let db = Db::fresh(root, name).await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)")
        .await;
    tables(&db).await;
    let pw_var = format!("LWF_TEST_{}_PW", name.to_uppercase());
    std::env::set_var(&pw_var, password);
    let redis = Client::new(
        RedisAddr::parse(endpoint).unwrap(),
        &TlsSpec::parse(None).unwrap(),
        username.map(str::to_string),
        Some(password.into()),
    )
    .unwrap();
    let t = Duration::from_secs(3);
    let cmd = |a: &[&str]| a.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
    redis.command(&cmd(&["FLUSHDB"]), t).await.unwrap();
    // A tagged cache entry in the Cm_Cache_Backend_Redis layout.
    redis
        .command(&cmd(&["SADD", "zc:ti:app_TAG_1", "e1"]), t)
        .await
        .unwrap();
    redis
        .command(&cmd(&["HSET", "zc:k:e1", "d", "x"]), t)
        .await
        .unwrap();

    let mut vars = json!({"password": format!("env:{pw_var}"), "prefix": "app_"});
    if let Some(u) = username {
        vars["username"] = json!(u);
    }
    // SET answers OK, DEL a count, EVAL nil: `text` holds the first two, NULL the last.
    db.target(
        "cache",
        "redis",
        endpoint,
        Some("text IS NULL OR text = 'OK' OR text REGEXP '^[0-9]+$'"),
        &vars.to_string(),
    )
    .await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('docs', 'cache', 'key', \
         'SELECT id AS _key, name, IF(active, NULL, ''delete'') AS _op FROM customers WHERE id IN (:keys)', \
         '{{#each docs}}{{#if (eq op \"delete\")}}DEL {{redis_quote target.vars.prefix key}}{{else}}SET {{redis_quote target.vars.prefix key}} {{redis_quote (json doc)}}{{/if}}{{/each}}', 100)",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('tags', 'cache', ?, 100)",
        ("EVAL \"for _,t in ipairs(ARGV) do local ids=redis.call('SMEMBERS','zc:ti:'..t) for _,i in ipairs(ids) do redis.call('DEL','zc:k:'..i) end redis.call('DEL','zc:ti:'..t) redis.call('SREM','zc:tags',t) end\" 0 {{#each keys}}{{target.vars.prefix}}{{upper (replace this \"[^A-Za-z0-9_]\" \"_\")}} {{/each}}",),
    )
    .await;
    db.trigger("docs", "customers", Some("{{new.id}}"), None)
        .await;
    db.trigger("tags", "customers", Some("tag-{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO customers VALUES (1, 'Ann \"A\"', 1), (2, 'Bob', 1)")
        .await;
    let v = wait_for(Duration::from_secs(10), "SET app_1", || async {
        match redis.command(&cmd(&["GET", "app_1"]), t).await.ok()? {
            Reply::Bulk(Some(b)) => Some(String::from_utf8(b).unwrap()),
            _ => None,
        }
    })
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(&v).unwrap(),
        json!({"name": "Ann \"A\""})
    );
    wait_for(
        Duration::from_secs(10),
        "the tagged entry cleared",
        || async {
            let r = redis
                .command(&cmd(&["EXISTS", "zc:k:e1", "zc:ti:app_TAG_1"]), t)
                .await
                .ok()?;
            (r == Reply::Int(0)).then_some(())
        },
    )
    .await;
    db.exec("UPDATE customers SET active = 0 WHERE id = 2")
        .await;
    wait_for(Duration::from_secs(10), "DEL app_2", || async {
        let r = redis.command(&cmd(&["EXISTS", "app_2"]), t).await.ok()?;
        (r == Reply::Int(0)
            && redis.command(&cmd(&["EXISTS", "app_1"]), t).await.ok()? == Reply::Int(1))
        .then_some(())
    })
    .await;
    inst.stop().await;
}

/// Redis 7, password `AUTH`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_target() {
    let root = need_db!();
    redis_rules(&root, "redis", "127.0.0.1:34379/5", None, "redispw").await;
}

/// Valkey 8 with an ACL user (`AUTH user password`); its default user is off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valkey_target() {
    let root = need_db!();
    redis_rules(
        &root,
        "valkey",
        "127.0.0.1:34380/2",
        Some("lazywf"),
        "valkeypw",
    )
    .await;
}

/// A TCP stand-in in front of a Redis: `cut` aborts the live connections and refuses new ones
/// until `heal`, so the engine sees its connection drop, as it does when Redis restarts.
struct Flaky {
    port: u16,
    down: std::sync::Arc<std::sync::atomic::AtomicBool>,
    refused: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    live: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Flaky {
    async fn start(upstream: &'static str) -> Flaky {
        use std::sync::atomic::Ordering::SeqCst;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let f = Flaky {
            port: listener.local_addr().unwrap().port(),
            down: Default::default(),
            refused: Default::default(),
            live: Default::default(),
        };
        let (down, refused, live) = (f.down.clone(), f.refused.clone(), f.live.clone());
        tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                if down.load(SeqCst) {
                    refused.fetch_add(1, SeqCst);
                    continue; // the socket closes here: the engine reads end of stream
                }
                let task = tokio::spawn(async move {
                    if let Ok(mut server) = tokio::net::TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
                live.lock().unwrap().push(task.abort_handle());
            }
        });
        f
    }

    fn cut(&self) {
        self.down.store(true, std::sync::atomic::Ordering::SeqCst);
        for h in self.live.lock().unwrap().drain(..) {
            h.abort();
        }
    }

    fn heal(&self) {
        self.down.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn refused(&self) -> usize {
        self.refused.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// One stream entry: its id and fields.
type Entry = (String, std::collections::BTreeMap<String, String>);

fn bulk(r: &lazy_wire_fire::targets::redis::Reply) -> String {
    match r {
        lazy_wire_fire::targets::redis::Reply::Bulk(Some(b)) => {
            String::from_utf8(b.clone()).unwrap()
        }
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// `XRANGE` reply as entries, in stream order.
fn stream_entries(reply: lazy_wire_fire::targets::redis::Reply) -> Vec<Entry> {
    use lazy_wire_fire::targets::redis::Reply;
    let Reply::Array(Some(list)) = reply else {
        panic!("XRANGE reply: {reply:?}")
    };
    list.iter()
        .map(|e| {
            let Reply::Array(Some(parts)) = e else {
                panic!("stream entry: {e:?}")
            };
            let Reply::Array(Some(fields)) = &parts[1] else {
                panic!("stream fields: {:?}", parts[1])
            };
            let map = fields
                .chunks(2)
                .map(|kv| (bulk(&kv[0]), bulk(&kv[1])))
                .collect();
            (bulk(&parts[0]), map)
        })
        .collect()
}

/// The entries of a stream, once there are at least `n`.
async fn entries_at_least(redis: &lazy_wire_fire::targets::redis::Client, n: usize) -> Vec<Entry> {
    let t = Duration::from_secs(3);
    wait_for(
        Duration::from_secs(30),
        &format!("{n} stream entries"),
        || async {
            let r = redis
                .command(
                    &[
                        b"XRANGE".to_vec(),
                        b"orders:changes".to_vec(),
                        b"-".to_vec(),
                        b"+".to_vec(),
                    ],
                    t,
                )
                .await
                .ok()?;
            let e = stream_entries(r);
            (e.len() >= n).then_some(e)
        },
    )
    .await
}

/// A stream entry as (key, op, document).
fn change(f: &std::collections::BTreeMap<String, String>) -> (String, String, Value) {
    (
        f["key"].clone(),
        f["op"].clone(),
        serde_json::from_str(&f["doc"]).unwrap(),
    )
}

/// The redis kind as a stream producer: one `XADD` per changed key (its key, op and JSON
/// document) to `orders:changes` on db 6. A dropped connection is retried, and the entry arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_stream_target() {
    let root = need_db!();
    use lazy_wire_fire::targets::redis::{Client, RedisAddr};
    use lazy_wire_fire::tls::TlsSpec;
    let db = Db::fresh(&root, "redis_stream").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let proxy = Flaky::start("127.0.0.1:34379").await;
    let pw_var = "LWF_TEST_REDIS_STREAM_PW";
    std::env::set_var(pw_var, "redispw");
    let direct = Client::new(
        RedisAddr::parse("127.0.0.1:34379/6").unwrap(),
        &TlsSpec::parse(None).unwrap(),
        None,
        Some("redispw".into()),
    )
    .unwrap();
    let t = Duration::from_secs(3);
    direct.command(&[b"FLUSHDB".to_vec()], t).await.unwrap();

    let vars = json!({"password": format!("env:{pw_var}")});
    db.target(
        "stream",
        "redis",
        &format!("127.0.0.1:{}/6", proxy.port),
        None,
        &vars.to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('changes', 'stream', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            "{{#each docs}}XADD orders:changes * key {{redis_quote key}} op {{op}} doc {{redis_quote (json doc)}}{{/each}}",
        ),
    )
    .await;
    db.trigger("changes", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    // Two inserts in one transaction: one entry per key, in no fixed order.
    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    let first = entries_at_least(&direct, 2).await;
    let mut got: Vec<_> = first.iter().map(|(_, f)| change(f)).collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![
            ("1".into(), "upsert".into(), json!({"sku": "A-1", "qty": 3})),
            ("2".into(), "upsert".into(), json!({"sku": "B-2", "qty": 1})),
        ]
    );

    // Each later change, waited for, is one more entry.
    db.exec("UPDATE orders SET qty = 4 WHERE id = 1").await;
    let s = entries_at_least(&direct, 3).await;
    assert_eq!(s.len(), 3);
    assert_eq!(
        change(&s[2].1),
        ("1".into(), "upsert".into(), json!({"sku": "A-1", "qty": 4}))
    );

    db.exec("DELETE FROM orders WHERE id = 2").await;
    let s = entries_at_least(&direct, 4).await;
    assert_eq!(s.len(), 4);
    assert_eq!(
        (change(&s[3].1).0, change(&s[3].1).1),
        ("2".into(), "delete".into())
    );

    // Redis drops every connection and refuses new ones: the change is retried, and its
    // entry arrives once Redis answers again.
    proxy.cut();
    db.exec("INSERT INTO orders VALUES (3, 'C-3', 7)").await;
    wait_for(Duration::from_secs(30), "a refused connection", || async {
        (proxy.refused() >= 1).then_some(())
    })
    .await;
    assert_eq!(
        entries_at_least(&direct, 0).await.len(),
        4,
        "nothing lands while Redis is cut off"
    );
    proxy.heal();
    let s = entries_at_least(&direct, 5).await;
    assert_eq!(
        s.len(),
        5,
        "the retried change arrives once, nothing is lost"
    );
    assert_eq!(
        change(&s[4].1),
        ("3".into(), "upsert".into(), json!({"sku": "C-3", "qty": 7}))
    );
    inst.stop().await;
}

/// Tag-based purging through a real Varnish (`LAZYWF_TEST_VARNISH`, compose profile
/// `varnish`; its backend is this test on 127.0.0.1:34682): pages tagged with a changed key are
/// banned, other pages stay cached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn varnish_purges_by_tag() {
    let root = need_db!();
    let Ok(varnish) = std::env::var("LAZYWF_TEST_VARNISH") else {
        return eprintln!("LAZYWF_TEST_VARNISH not set: skipping");
    };
    use lazy_wire_fire::http::{BaseUrl, Client, RawRequest};
    use lazy_wire_fire::tls::TlsSpec;
    let backend = Mock::start_on(34682).await;
    backend.respond(|r| (200, format!("page {}", r.path)));
    backend.headers(|r| {
        let n = r.path.trim_start_matches("/p/").to_string();
        vec![
            ("X-Cache-Tags".into(), format!("item_{n},group_9")),
            ("Cache-Control".into(), "public, max-age=3600".into()),
        ]
    });
    let base = BaseUrl::parse(&varnish).unwrap();
    let client = Client::new(base.clone(), &TlsSpec::parse(Some("disabled")).unwrap()).unwrap();
    let get = |path: &str| {
        let mut r = RawRequest::parse(&format!("GET {path} HTTP/1.1\n\n"))
            .unwrap()
            .unwrap();
        r.set_header("Host", base.host_header());
        r
    };
    let hits = |b: &Mock, path: &str| b.requests().iter().filter(|r| r.path == path).count();

    let db = Db::fresh(&root, "varnish").await;
    db.exec("CREATE TABLE stock (product_id INT PRIMARY KEY, qty INT)")
        .await;
    db.exec("INSERT INTO stock VALUES (1, 5), (2, 5)").await;
    tables(&db).await;
    db.target("varnish", "http", &varnish, None, "{}").await;
    let steps = json!([{"once": true, "template": "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Cache-Tags-Pattern: .*\n"}]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, batch_max_bytes, debounce_ms) VALUES ('purge', 'varnish', ?, ?, 7000, 100)",
        (
            "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Cache-Tags-Pattern: {{#each keys}}((^|,){{regex_escape this}}(,|$)){{#unless @last}}|{{/unless}}{{/each}}\n",
            steps.to_string(),
        ),
    )
    .await;
    db.trigger("purge", "stock", Some("item_{{new.product_id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    // The first start purges everything once.
    wait_for(Duration::from_secs(10), "the first-start purge", || async {
        db.status("purge")
            .await
            .filter(|s| s.2.starts_with("done"))
            .map(|_| ())
    })
    .await;
    for path in ["/p/1", "/p/2", "/p/1", "/p/2"] {
        let r = client
            .send(&get(path), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(r.status, 200);
    }
    assert_eq!(
        (hits(&backend, "/p/1"), hits(&backend, "/p/2")),
        (1, 1),
        "pages are cached"
    );

    db.exec("UPDATE stock SET qty = 0 WHERE product_id = 1")
        .await;
    wait_for(Duration::from_secs(10), "page 1 purged", || async {
        client
            .send(&get("/p/1"), Duration::from_secs(5))
            .await
            .ok()?;
        (hits(&backend, "/p/1") >= 2).then_some(())
    })
    .await;
    client
        .send(&get("/p/2"), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(hits(&backend, "/p/2"), 1, "page 2 is still cached");
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_target() {
    use aws_credential_types::Credentials;
    use lazy_wire_fire::http::{BaseUrl, Client, RawRequest};
    use lazy_wire_fire::tls::TlsSpec;
    let root = need_db!();
    let db = Db::fresh(&root, "s3").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_TEST_S3_AK", "lazywfaccess");
    std::env::set_var("LWF_TEST_S3_SK", "lazywfsecret");
    let creds = Credentials::new("lazywfaccess", "lazywfsecret", None, None, "e2e");
    let client = Client::new(
        BaseUrl::parse("http://127.0.0.1:34070").unwrap(),
        &TlsSpec::parse(None).unwrap(),
    )
    .unwrap();
    let bucket = format!("lwf{}", std::process::id());
    let s3 = |method: &str, path: String| {
        let mut r = RawRequest::parse(&format!("{method} {path} HTTP/1.1\n\n"))
            .unwrap()
            .unwrap();
        lazy_wire_fire::aws::sign(
            &mut r,
            "127.0.0.1:34070",
            "",
            "us-east-1",
            "s3",
            &creds,
            lazy_wire_fire::time::unix_now(),
        )
        .unwrap();
        r
    };
    let made = client
        .send(&s3("PUT", format!("/{bucket}")), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        made.status == 200,
        "create bucket: {} {}",
        made.status,
        made.body_snippet()
    );

    db.target(
        "store",
        "http.s3",
        "",
        Some("status IN (200, 204)"),
        &json!({"bucket": bucket, "region": "us-east-1", "endpoint": "http://127.0.0.1:34070", "path_style": true,
                "access_key_ref": "env:LWF_TEST_S3_AK", "secret_key_ref": "env:LWF_TEST_S3_SK"})
        .to_string(),
    )
    .await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('export', 'store', 'key', \
         'SELECT c.id AS _key, c.name FROM customers c WHERE c.id IN (:keys)', \
         '{{#each docs}}{{#if (eq op \"delete\")}}DELETE /customers/{{key}}.json HTTP/1.1\n\n{{else}}PUT /customers/{{key}}.json HTTP/1.1\nContent-Type: application/json\n\n{{json doc}}{{/if}}{{/each}}', 100)",
    )
    .await;
    db.exec("UPDATE lazy_wire_fire_rule SET selects = CONCAT(selects, '; SELECT id AS _key, IF(COUNT(*) = 0, ''delete'', NULL) AS _op FROM customers WHERE id IN (:keys) GROUP BY id') WHERE name = 'export'").await;
    db.trigger("export", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (7, 'Ann'), (8, 'Bob')")
        .await;
    for (id, name) in [(7, "Ann"), (8, "Bob")] {
        let body = wait_for(Duration::from_secs(10), "the object", || async {
            let r = client
                .send(
                    &s3("GET", format!("/{bucket}/customers/{id}.json")),
                    Duration::from_secs(5),
                )
                .await
                .ok()?;
            (r.status == 200).then(|| String::from_utf8(r.body).unwrap())
        })
        .await;
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            json!({"name": name})
        );
    }
    db.exec("DELETE FROM customers WHERE id = 7").await;
    wait_for(Duration::from_secs(10), "the object deleted", || async {
        let r = client
            .send(
                &s3("GET", format!("/{bucket}/customers/7.json")),
                Duration::from_secs(5),
            )
            .await
            .ok()?;
        (r.status == 404).then_some(())
    })
    .await;
    inst.stop().await;
}

/// An http target whose template sends the document as a YAML body (`{{yaml doc}}`, with the
/// `Content-Type` set by the template): the receiver parses the body back to the document.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn yaml_body_over_http() {
    let root = need_db!();
    let db = Db::fresh(&root, "yaml").await;
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, sku VARCHAR(20), qty INT, price DECIMAL(10,1), active INT, note VARCHAR(200), gone DATE)")
        .await;
    tables(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("yamlhook", "http", &mock.url(), None, "{}").await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('y', 'yamlhook', 'key', \
         'SELECT id AS _key, sku, qty, price, active, note, gone FROM items WHERE id IN (:keys)', \
         '{{#each docs}}POST /items/{{key}} HTTP/1.1\nContent-Type: application/yaml\n\n{{yaml doc}}{{/each}}', 100)",
    )
    .await;
    db.trigger("y", "items", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    // A string a YAML reader would resolve as a date, one with a colon, and one whose line
    // breaks, `---` and `key: value` would start a new key, a new document or a new header of
    // the request if the helper let them through raw.
    db.exec("INSERT INTO items VALUES (1, 'sku: 1', 3, 12.5, 1, 'line1\\n---\\ninjected: yes\\nGET /evil HTTP/1.1', '2026-11-07')")
        .await;
    wait_for(Duration::from_secs(15), "the yaml body", || async {
        (!mock.requests().is_empty()).then_some(())
    })
    .await;
    let r = &mock.requests()[0];
    assert_eq!((r.method.as_str(), r.path.as_str()), ("POST", "/items/1"));
    assert_eq!(r.header("content-type"), Some("application/yaml"));
    assert_eq!(r.header("injected"), None);
    assert_eq!(
        r.yaml(),
        json!({
            "sku": "sku: 1",
            "qty": 3,
            "price": 12.5,
            "active": 1,
            "note": "line1\n---\ninjected: yes\nGET /evil HTTP/1.1",
            "gone": "2026-11-07"
        })
    );
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn xa_fires_only_after_commit() {
    let root = need_db!();
    {
        // A failed earlier run may have left its XA transactions prepared (they outlive the
        // connection) and holding locks.
        let mut c = mysql_async::Conn::new(
            mysql_async::Opts::from_url(&format!("{root}/?prefer_socket=false")).unwrap(),
        )
        .await
        .unwrap();
        for x in ["lwf1", "lwf2"] {
            let _ = c.query_drop(format!("XA ROLLBACK '{x}'")).await;
        }
        c.disconnect().await.ok();
    }
    let db = Db::fresh(&root, "xa").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('o', 'hook', 'POST /k HTTP/1.1\n\n{{join keys \",\"}}', 50)").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    let mut c = db.conn().await;
    for q in [
        "XA START 'lwf1'",
        "INSERT INTO orders VALUES (21, 1, 'x', 1)",
        "XA END 'lwf1'",
        "XA PREPARE 'lwf1'",
    ] {
        c.query_drop(q).await.unwrap();
    }
    // MariaDB keeps a prepared XA transaction attached to its connection: one each.
    let mut c2 = db.conn().await;
    for q in [
        "XA START 'lwf2'",
        "INSERT INTO orders VALUES (22, 1, 'x', 1)",
        "XA END 'lwf2'",
        "XA PREPARE 'lwf2'",
    ] {
        c2.query_drop(q).await.unwrap();
    }
    sleep_ms(1500).await;
    assert!(
        mock.requests().is_empty(),
        "prepared XA work fired: {:?}",
        mock.requests()
    );
    c2.query_drop("XA ROLLBACK 'lwf2'").await.unwrap();
    c.query_drop("XA COMMIT 'lwf1'").await.unwrap();
    c.query_drop("INSERT INTO orders VALUES (23, 1, 'x', 1)")
        .await
        .unwrap();
    wait_for(Duration::from_secs(10), "the committed keys", || async {
        let all: Vec<String> = mock
            .requests()
            .iter()
            .flat_map(|r| r.body.split(',').map(str::to_string).collect::<Vec<_>>())
            .collect();
        (all.contains(&"21".to_string()) && all.contains(&"23".to_string())).then_some(all)
    })
    .await;
    sleep_ms(300).await;
    assert!(
        !mock
            .requests()
            .iter()
            .any(|r| r.body.split(',').any(|k| k == "22")),
        "rolled back XA work fired"
    );
    c.disconnect().await.ok();
    c2.disconnect().await.ok();
    inst.stop().await;
}

/// The XA floor across a restart: an XA that starts before a plain transaction but commits
/// after it must keep the rule's saved position at its start while its keys are unflushed —
/// or the restart resumes past its COMMIT and the keys are lost (the core's `pin` is what
/// keeps the floor the minimum, `apply` in `src/engine.rs`). Two things are read back rather
/// than assumed: the XA's start, from the binlog ([`xa_group_start`]) because the snapshot
/// before `XA START` is only a lower bound once other sessions write to this server; and the
/// saved position, from a status row written after both keys were refused ([`status_row`])
/// because until the engine has applied the XA's rows the buffer holds only the later key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn xa_committed_after_a_later_transaction_keeps_its_floor_across_a_restart() {
    let root = need_db!();
    {
        // A failed earlier run may have left 'lwf3' prepared on the shared server.
        let mut c = mysql_async::Conn::new(
            mysql_async::Opts::from_url(&format!("{root}/?prefer_socket=false")).unwrap(),
        )
        .await
        .unwrap();
        let _ = c.query_drop("XA ROLLBACK 'lwf3'").await;
        c.disconnect().await.ok();
    }
    let db = Db::fresh(&root, "xafloor").await;
    shop(&db).await;
    let mock = Mock::start().await;
    // Nothing may be confirmed while the keys are pending: every flush is refused, so the
    // saved position is the pinned floor alone (like `failures_retry_and_hold_the_position`).
    mock.respond(|_| (500, "unavailable".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('o', 'hook', 'POST /k HTTP/1.1\n\n{{join keys \",\"}}', 100)").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    sleep_ms(300).await;
    // The XA opens first, so its start is the position the floor must stay at. This snapshot is
    // a lower bound for it only; the real one comes from the binlog after the COMMIT below.
    let xa_at = binlog_end(&db).await;
    let mut c = db.conn().await;
    for q in [
        "XA START 'lwf3'",
        "INSERT INTO orders VALUES (31, 1, 'x', 1)",
        "XA END 'lwf3'",
        "XA PREPARE 'lwf3'",
    ] {
        c.query_drop(q).await.unwrap();
    }
    // A filler transaction of its own, so the plain transaction's group does not begin right after
    // the XA's. (`shop` fills no tables, so the UPDATE this test used to run here matched no rows
    // and wrote nothing to the binlog at all. `order_tags` has no rule, so it marks no keys.)
    db.exec("INSERT INTO order_tags VALUES (31, 'filler')")
        .await;
    let plain_at = binlog_end(&db).await;
    assert!(xa_at < plain_at, "the filler must move the position");
    db.exec("INSERT INTO orders VALUES (32, 1, 'n', 1)").await;
    c.query_drop("XA COMMIT 'lwf3'").await.unwrap();
    // The flush is refused and both keys stay pending, so the rule's saved position must hold at
    // the XA's start — not climb to the plain transaction's. Both keys have to have reached the
    // target first: the XA's rows arrive with its COMMIT, and until the engine has applied them
    // the buffer holds only the plain key, whose origin is legitimately above the XA's start.
    wait_for(
        Duration::from_secs(15),
        "both keys refused at the target",
        || async {
            let keys: Vec<String> = mock
                .requests()
                .iter()
                .flat_map(|r| r.body.split(',').map(|k| k.trim().to_string()))
                .collect();
            (keys.iter().any(|k| k == "31") && keys.iter().any(|k| k == "32")).then_some(())
        },
    )
    .await;
    // Then the status row the engine wrote *after* that moment. The XA's re-pin moves the position
    // and `persist` rewrites a row whose position has moved, so this is the floor with the XA's key
    // in the buffer — not the row the first refused flush left behind (the old test slept 600 ms
    // and read whatever was there, which is why it hid the order of these two events).
    let after = server_now(&db).await;
    let (f, p, ..) = wait_for(
        Duration::from_secs(30),
        "the refused flush in the status row, rewritten after the XA landed",
        || async {
            status_row(&db, "o", &after)
                .await
                .filter(|(_, _, _, e)| e.as_deref().is_some_and(|e| e.contains("500")))
        },
    )
    .await;
    // The XA's own start, read back from the binlog: `xa_at` is only a lower bound, because
    // another session can commit between the snapshot and `XA START` (the engine's status
    // write, or a test running in parallel on this server), and then the XA's group starts
    // that much later — and holding the floor there is right, not a climb past it.
    let xa_start = xa_group_start(&db, &xa_at, "lwf3")
        .await
        .unwrap_or_else(|| panic!("no XA PREPARE for 'lwf3' in the binlog after {xa_at:?}"));
    assert!(
        xa_at <= xa_start && xa_start < plain_at,
        "the XA's group must begin between {xa_at:?} (taken before XA START) and {plain_at:?} \
         (taken before the plain transaction), got {xa_start:?}"
    );
    assert!(
        (f.clone(), p) <= xa_start,
        "the rule position climbed past the XA start while its keys are unflushed: \
         saved ({f},{p}), xa start {xa_start:?}"
    );
    // Restart with the target still down, then let it through: a resume point above the
    // XA start would skip its PREPARE/COMMIT and lose key 31 forever.
    inst.stop().await;
    mock.clear(); // only deliveries after the restart may be seen
    let inst = Instance::ready(config(&db, &[])).await;
    mock.respond(|_| (200, r#"{"errors":false}"#.into()));
    wait_for(
        Duration::from_secs(15),
        "both keys after the restart",
        || async {
            let all: Vec<String> = mock
                .requests()
                .iter()
                .flat_map(|r| r.body.split(',').map(str::to_string).collect::<Vec<_>>())
                .collect();
            (all.contains(&"31".to_string()) && all.contains(&"32".to_string())).then_some(all)
        },
    )
    .await;
    c.disconnect().await.ok();
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddl_resets_and_rules_change_live() {
    let root = need_db!();
    let db = Db::fresh(&root, "live").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps = json!([{"once": true, "template": "PURGE /all HTTP/1.1\n\n"}]).to_string();
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) VALUES ('o', 'hook', \
         'POST /k HTTP/1.1\n\n{{join keys \",\"}}', ?, 50)",
        (steps,),
    )
    .await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    // First start: the reset ran once.
    wait_for(Duration::from_secs(10), "the first-start reset", || async {
        mock.requests()
            .iter()
            .any(|r| r.method == "PURGE")
            .then_some(())
    })
    .await;
    mock.clear();
    db.exec("TRUNCATE TABLE orders").await;
    wait_for(
        Duration::from_secs(10),
        "the reset after TRUNCATE",
        || async {
            mock.requests()
                .iter()
                .any(|r| r.method == "PURGE")
                .then_some(())
        },
    )
    .await;

    // A rule added while running fires, a changed template is used right away.
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('c', 'hook', 'POST /customers HTTP/1.1\n\n{{join keys}}', 50)").await;
    db.trigger("c", "customers", Some("cust-{{new.id}}"), None)
        .await;
    db.exec("UPDATE lazy_wire_fire_rule SET template = 'POST /k2 HTTP/1.1\n\n{{join keys \",\"}}' WHERE name = 'o'").await;
    sleep_ms(500).await;
    mock.clear();
    db.exec("INSERT INTO customers VALUES (5, 'Eve')").await;
    db.exec("INSERT INTO orders VALUES (31, 5, 'new', 1)").await;
    wait_for(Duration::from_secs(10), "both rules", || async {
        let r = mock.requests();
        (r.iter()
            .any(|r| r.path == "/customers" && r.body == "cust-5")
            && r.iter().any(|r| r.path == "/k2" && r.body == "31"))
        .then_some(())
    })
    .await;
    inst.stop().await;
}

/// The keys `path` received, over all its requests (comma-separated bodies).
fn fired(reqs: &[Req], path: &str) -> Vec<String> {
    reqs.iter()
        .filter(|r| r.path == path)
        .flat_map(|r| r.body.split(',').map(String::from).collect::<Vec<_>>())
        .collect()
}

/// The route cache (the hot-path audit's finding 8) must not turn a live rule edit into the
/// old world: the rebuild that carries the edit drops the built routes, so the next event of
/// the table is answered by the rule as it is now — its template, then its key — and not by
/// what the first event was routed with. A cache that survived the edit would keep every
/// following row on `/o1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_rule_edit_is_answered_by_the_new_route_not_the_cached_one() {
    let root = need_db!();
    let db = Db::fresh(&root, "rtedit").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('o', 'hook', 'POST /o1 HTTP/1.1\n\n{{join keys \",\"}}', 50)").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    // The route is built here, once; the events that follow are answered from the cache.
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(10), "the first route", || async {
        (fired(&mock.requests(), "/o1") == ["1"]).then_some(())
    })
    .await;

    // Edit the template while the engine is running.
    db.exec("UPDATE lazy_wire_fire_rule SET template = 'POST /o2 HTTP/1.1\n\n{{join keys \",\"}}' WHERE name = 'o'").await;
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    wait_for(Duration::from_secs(10), "the edited template", || async {
        (fired(&mock.requests(), "/o2") == ["2"]).then_some(())
    })
    .await;
    // And the route that edit built is the one kept: a following event of the same table is
    // answered by it too, not re-read back onto the old template.
    db.exec("UPDATE orders SET total = 3 WHERE id = 2").await;
    wait_for(
        Duration::from_secs(10),
        "the rebuilt route kept",
        || async {
            (fired(&mock.requests(), "/o2")
                .iter()
                .filter(|k| *k == "2")
                .count()
                >= 2)
                .then_some(())
        },
    )
    .await;
    let r = mock.requests();
    assert_eq!(
        fired(&r, "/o1"),
        ["1"],
        "the edit reached some events and not others: {r:?}"
    );

    // Edit the key the same way: the trigger row changes, so what the route marks changes.
    db.exec("UPDATE lazy_wire_fire_trigger SET `key` = 'x_{{new.id}}' WHERE rule = 'o' AND source_table = 'orders'")
        .await;
    db.exec("INSERT INTO orders VALUES (3, 1, 'new', 3)").await;
    wait_for(Duration::from_secs(10), "the edited key", || async {
        (fired(&mock.requests(), "/o2").contains(&"x_3".to_string())).then_some(())
    })
    .await;
    inst.stop().await;
}

/// An `ALTER` that adds a column to a watched table has to reach both caches before the first
/// row of the new shape: the layout (so the new column can be named at all) and the route
/// beside it (so the columns a `Watched` decodes are not the ones of the table before it).
/// The second rule keys on the column the `ALTER` adds — a route built while the table had
/// four columns can never produce its key. And the pinned backstop message ("... in the
/// binlog but ...") stays silent: the rows never arrived at a layout that lagged them, which
/// is what dropping the route together with the layout is for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alter_mid_run_rebuilds_the_route_and_the_rule_keeps_decoding() {
    let root = need_db!();
    let db = Db::fresh(&root, "altd").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('o', 'hook', 'POST /o HTTP/1.1\n\n{{join keys \",\"}}', 50)").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('nt', 'hook', 'POST /nt HTTP/1.1\n\n{{join keys \",\"}}', 50)").await;
    db.trigger("nt", "orders", Some("n_{{new.note}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(10),
        "the rule on the plain table",
        || async { (fired(&mock.requests(), "/o") == ["1"]).then_some(()) },
    )
    .await;

    // The `ALTER` mid-run, then a row of the new shape on both rules.
    db.exec("ALTER TABLE orders ADD COLUMN note VARCHAR(20)")
        .await;
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2, 'hi')")
        .await;
    wait_for(
        Duration::from_secs(10),
        "the new shape on the plain rule",
        || async { (fired(&mock.requests(), "/o").contains(&"2".to_string())).then_some(()) },
    )
    .await;
    wait_for(
        Duration::from_secs(10),
        "the column the ALTER added",
        || async { (fired(&mock.requests(), "/nt").contains(&"n_hi".to_string())).then_some(()) },
    )
    .await;

    // The layout behind the rows moved once, with the `ALTER`, and the rows never outran it:
    // the backstop that names such a lag has nothing to say about this schema.
    let stale: Vec<String> = warnings()
        .into_iter()
        .filter(|l| l.contains(&db.schema) && l.contains("in the binlog but"))
        .collect();
    assert!(
        stale.is_empty(),
        "the layout lagged the binlog across the ALTER: {stale:?}"
    );
    // And an added column is not a dropped or replaced table: no continuity was lost, so no
    // reset reason appeared on either rule. The rows are the engine's to write when its loop
    // tick comes (status::write rides the persist deadline, and a busy shared server can push
    // that tick out), so poll for both before unwrapping either — the pattern of the
    // virtual-column test beside this one.
    let resets = wait_for(
        Duration::from_secs(15),
        "the status rows of both rules",
        || async {
            let mut got = Vec::new();
            for name in ["o", "nt"] {
                let (_, _, reset, _) = db.status(name).await?;
                got.push((name, reset));
            }
            Some(got)
        },
    )
    .await;
    for (name, reset) in resets {
        assert!(
            reset.starts_with("missing: first start"),
            "the {name} rule lost continuity across the ALTER: {reset}"
        );
    }
    inst.stop().await;
}

/// Editing a rule rebuilds the routes of its tables, and a rebuild re-runs every check the
/// first build ran — including the refusal of a trigger that reads past a `VIRTUAL` column on
/// a server that leaves those columns out of its images. A cached route that skipped the
/// checks would hand the refused trigger back with the edit. Only the servers that run short
/// have a refusal to protect: the others log the skip and pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rule_edit_does_not_resurrect_a_refused_virtual_read() {
    /// The keys every request carried in its `X-Keys` header.
    fn keys(m: &Mock) -> std::collections::BTreeSet<String> {
        m.requests()
            .iter()
            .filter_map(|r| r.header("x-keys"))
            .flat_map(|k| k.split('|').map(str::to_string))
            .collect()
    }

    let root = need_db!();
    let db = Db::fresh(&root, "vnores").await;
    db.exec(
        "CREATE TABLE items (id INT PRIMARY KEY, price INT, price_x2 INT AS (price * 2) VIRTUAL, \
         name VARCHAR(50), stock INT)",
    )
    .await;
    tables(&db).await;

    // Which world this server is in, from its own wire (as the virtual-column test does): an
    // UPDATE image that holds fewer than the five named columns is one that omits the VIRTUAL.
    let facts = wire_facts(&db, "items", 2, || async {
        db.exec("INSERT INTO items (id, price, name, stock) VALUES (1, 5, 'probe', 3)")
            .await;
        db.exec("UPDATE items SET stock = 9 WHERE id = 1").await;
    })
    .await;
    for line in &facts {
        println!("vnores {line}");
    }
    let short = facts.iter().any(|l| {
        l.starts_with("Update") && l.contains("value(s)") && !l.contains("row image 5 value(s)")
    });
    if !short {
        println!(
            "vnores: this server's images hold the VIRTUAL column, so its trigger is never \
             refused and there is nothing to resurrect; skipping"
        );
        return;
    }

    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES \
         ('v', 'hook', 'PURGE /keys HTTP/1.1\nHost: {{endpoint.host}}\nX-Keys: \
          {{#each keys}}{{this}}{{#unless @last}}|{{/unless}}{{/each}}\n', 100)",
    )
    .await;
    db.trigger("v", "items", Some("{{new.name}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO items (id, price, name, stock) VALUES (2, 7, 'gadget', 4)")
        .await;
    db.exec("UPDATE items SET stock = 5 WHERE id = 2").await;
    inst.quiet().await;
    assert!(
        !keys(&mock).contains("gadget"),
        "the refused trigger marked before any edit: {:?}",
        keys(&mock)
    );
    wait_for(
        Duration::from_secs(15),
        "the refusal on the status",
        || async {
            let s = db.status("v").await?;
            (s.3.as_deref()
                .is_some_and(|e| e.contains("price_x2") && e.contains("name")))
            .then_some(())
        },
    )
    .await;

    // The edit: the rule's routes are dropped and built again — and the trigger that reads
    // past the VIRTUAL column is refused again, not remembered as it was never refused.
    db.exec(
        "UPDATE lazy_wire_fire_rule SET template = 'PURGE /keys2 HTTP/1.1\nHost: {{endpoint.host}}\nX-Keys: \
          {{#each keys}}{{this}}{{#unless @last}}|{{/unless}}{{/each}}\n' WHERE name = 'v'",
    )
    .await;
    db.exec("UPDATE items SET stock = 6 WHERE id = 2").await;
    db.exec("INSERT INTO items (id, price, name, stock) VALUES (3, 2, 'bolt', 1)")
        .await;
    inst.quiet().await;
    let got = keys(&mock);
    assert!(
        !got.contains("gadget") && !got.contains("bolt"),
        "the rebuild resurrected the refused trigger: {got:?}"
    );
    let s = db.status("v").await.expect("the status row");
    let err =
        s.3.expect("the rebuilt route still says why the trigger marks nothing");
    assert!(
        err.contains("price_x2"),
        "the refusal after the edit says {err}"
    );
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rule_enabled_again_replays_what_it_missed() {
    let root = need_db!();
    let db = Db::fresh(&root, "reenable").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps = json!([{"once": true, "template": "PURGE /all HTTP/1.1\n\n"}]).to_string();
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) VALUES ('o', 'hook', \
         'POST /o HTTP/1.1\n\n{{join keys \",\"}}', ?, 50)",
        (steps,),
    )
    .await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('c', 'hook', 'POST /c HTTP/1.1\n\n{{join keys \",\"}}', 50)").await;
    db.trigger("c", "customers", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(Duration::from_secs(10), "the first-start reset", || async {
        mock.requests()
            .iter()
            .any(|r| r.method == "PURGE")
            .then_some(())
    })
    .await;
    db.exec("INSERT INTO customers VALUES (1, 'a')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 1)").await;
    wait_for(Duration::from_secs(10), "both rules", || async {
        let r = mock.requests();
        (fired(&r, "/o") == ["1"] && fired(&r, "/c") == ["1"]).then_some(())
    })
    .await;
    inst.quiet().await;

    // Disabled: its changes stay in the binlog, the other rule goes on.
    db.exec("UPDATE lazy_wire_fire_rule SET enabled = 0 WHERE name = 'o'")
        .await;
    wait_for(Duration::from_secs(10), "the rule disabled", || async {
        inst.health.status()["rules"]
            .get("o")
            .is_none()
            .then_some(())
    })
    .await;
    mock.clear();
    db.exec("INSERT INTO orders VALUES (2, 1, 'n', 1)").await;
    db.exec("UPDATE orders SET total = 2 WHERE id = 2").await;
    db.exec("INSERT INTO orders VALUES (3, 1, 'n', 1)").await;
    db.exec("INSERT INTO customers VALUES (2, 'b')").await;
    wait_for(Duration::from_secs(10), "the other rule", || async {
        (fired(&mock.requests(), "/c") == ["2"]).then_some(())
    })
    .await;
    assert!(
        fired(&mock.requests(), "/o").is_empty(),
        "fired while disabled"
    );

    // Enabled again: it replays what it missed instead of resetting, and the other rule does not
    // fire again for what it already read.
    db.exec("UPDATE lazy_wire_fire_rule SET enabled = 1 WHERE name = 'o'")
        .await;
    wait_for(Duration::from_secs(15), "the replayed keys", || async {
        let o = fired(&mock.requests(), "/o");
        (o.contains(&"2".to_string()) && o.contains(&"3".to_string())).then_some(())
    })
    .await;
    inst.quiet().await;
    let r = mock.requests();
    assert!(!r.iter().any(|r| r.method == "PURGE"), "a reset ran: {r:?}");
    assert!(
        !fired(&r, "/o").contains(&"1".to_string()),
        "order 1 was confirmed before the rule was disabled: {r:?}"
    );
    assert_eq!(fired(&r, "/c"), ["2"], "the other rule fired again");
    let (_, _, reset_state, _) = db.status("o").await.unwrap();
    assert!(
        !reset_state.starts_with("pending") && !reset_state.starts_with("running"),
        "{reset_state}"
    );
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_schema_change_keeps_continuity() {
    let root = need_db!();
    let db = Db::fresh(&root, "osc").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps = json!([{"once": true, "template": "PURGE /all HTTP/1.1\n\n"}]).to_string();
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) VALUES ('o', 'hook', \
         'POST /o HTTP/1.1\n\n{{join keys \",\"}}', ?, 50)",
        (steps,),
    )
    .await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 1), (2, 1, 'n', 1)")
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(Duration::from_secs(10), "the first-start reset", || async {
        mock.requests()
            .iter()
            .any(|r| r.method == "PURGE")
            .then_some(())
    })
    .await;
    inst.quiet().await;
    mock.clear();

    // As gh-ost does it: a copy created LIKE the table, altered, filled, kept current, swapped in.
    db.exec("CREATE TABLE _orders_gho LIKE orders").await;
    db.exec("ALTER TABLE _orders_gho ADD COLUMN note VARCHAR(20) NULL")
        .await;
    db.exec("INSERT INTO _orders_gho (id, customer_id, status, total) SELECT id, customer_id, status, total FROM orders").await;
    db.exec("UPDATE orders SET total = 5 WHERE id = 1").await;
    db.exec("UPDATE _orders_gho SET total = 5 WHERE id = 1")
        .await;
    db.exec("RENAME TABLE orders TO _orders_del, _orders_gho TO orders")
        .await;
    db.exec("INSERT INTO orders (id, customer_id, status, total, note) VALUES (3, 1, 'n', 1, 'x')")
        .await;
    // The two changes are waited for one after the other, each with its own allowance: they
    // shared one deadline, so a slow first one left the second almost nothing, and the message
    // said only that the pair did not arrive.
    wait_for(
        Duration::from_secs(10),
        "the change during the copy (the update of order 1 on the old table)",
        || async {
            fired(&mock.requests(), "/o")
                .contains(&"1".to_string())
                .then_some(())
        },
    )
    .await;
    wait_for(
        Duration::from_secs(10),
        "the insert after the swap (order 3 in the table that was swapped in)",
        || async {
            fired(&mock.requests(), "/o")
                .contains(&"3".to_string())
                .then_some(())
        },
    )
    .await;
    inst.quiet().await;
    let r = mock.requests();
    assert!(!r.iter().any(|r| r.method == "PURGE"), "a reset ran: {r:?}");
    assert!(
        !fired(&r, "/o").contains(&"2".to_string()),
        "an unchanged row fired: {r:?}"
    );

    // A table swapped in that is not such a copy may hold other rows: the rule resets.
    mock.clear();
    db.exec("CREATE TABLE orders_rebuilt (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20), total DECIMAL(10,2), note VARCHAR(20))").await;
    db.exec("RENAME TABLE orders TO orders_before, orders_rebuilt TO orders")
        .await;
    wait_for(
        Duration::from_secs(10),
        "the reset after a swap",
        || async {
            mock.requests()
                .iter()
                .any(|r| r.method == "PURGE")
                .then_some(())
        },
    )
    .await;
    inst.stop().await;
}

/// What the binlog really carries for `table` and what lazywf's layout makes of it: the table
/// map's column count and optional metadata names (only with `binlog_row_metadata=FULL`), the
/// number of values in each row image, and the named object the engine's own [`Catalog`] would
/// name them with. `writes` runs between reading the start position and connecting the reader, so
/// the events it produces are the ones read back — no timing involved. Every line is one
/// `println!` of the test output, so the servers of the matrix can be compared from the job log.
async fn wire_facts<F, Fut>(db: &Db, table: &str, events: usize, writes: F) -> Vec<String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    use lazy_wire_fire::rows::Catalog;
    use lazy_wire_fire::source::Item;

    let mut out = Vec::new();
    let mut control = db.conn().await;
    let info = lazy_wire_fire::binlog::server_info(&mut control)
        .await
        .unwrap();
    let meta: Option<String> = control
        .query_first("SELECT @@GLOBAL.binlog_row_metadata")
        .await
        .unwrap();
    let from = lazy_wire_fire::binlog::current_position(&mut control)
        .await
        .unwrap();
    let listed: Vec<(String,)> = control
        .exec(
            "SELECT CAST(COLUMN_NAME AS CHAR) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (db.schema.clone(), table.to_string()),
        )
        .await
        .unwrap();
    control.disconnect().await.ok();
    let listed = listed
        .into_iter()
        .map(|(n,)| n)
        .collect::<Vec<_>>()
        .join(",");
    out.push(format!(
        "{:?} {} binlog_row_metadata={} information_schema=[{}]",
        info.flavor,
        info.version,
        meta.as_deref().unwrap_or("?"),
        listed,
    ));

    // The start position is the one read above, so the writes happen after it and their events
    // are streamed back from the file: no timing involved.
    let mut reader = lazy_wire_fire::binlog::BinlogReader::connect(
        lazy_wire_fire::binlog::replica_opts(&db.dsn).unwrap(),
        test_server_id(),
        info.flavor,
        &from,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    writes().await;
    let mut catalog = Catalog::default();
    let until = std::time::Instant::now() + deadline(Duration::from_secs(30));
    let mut seen = 0;
    while seen < events && std::time::Instant::now() < until {
        let item = match tokio::time::timeout(Duration::from_secs(10), reader.next()).await {
            Ok(Ok(Some(i))) => i,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("reading the binlog of `{table}`: {e:#}"),
            Err(_) => break, // an idle stretch: the events we want were committed already
        };
        let (kind, table_id, event) = match item {
            Item::Rows {
                table_id,
                kind,
                event,
            } => (kind, table_id, event),
            _ => continue,
        };
        let Some(tme) = reader.table_map(table_id) else {
            continue;
        };
        if tme.database_name().as_ref() != db.schema || tme.table_name().as_ref() != table {
            continue; // another session's table on this shared server
        }
        seen += 1;
        let rows = reader.rows(table_id, &event).unwrap();
        let (before, after) = rows[0].clone();
        let image = after.as_ref().or(before.as_ref());
        let names = lazy_wire_fire::rows::table_map_names(tme);
        let mut control = db.conn().await;
        let layout = catalog
            .layout(&mut control, &db.schema, table, Some(tme))
            .await;
        control.disconnect().await.ok();
        out.push(format!(
            "{kind:?}: table map {} columns, metadata names [{}], row image {} value(s), \
             layout {}",
            tme.columns_count(),
            names.unwrap_or_default().join(","),
            image.map(|r| r.len()).unwrap_or(0),
            match &layout {
                Ok(l) => format!("[{}]", l.names.join(",")),
                Err(e) => format!("ERROR {e:#}"),
            },
        ));
        if let Ok(l) = &layout {
            for (what, row) in [("before", &before), ("after", &after)] {
                if let Some(r) = row {
                    out.push(format!("{kind:?} {what}: {}", l.object(r)));
                }
            }
        }
    }
    reader.close().await.unwrap();
    out
}

/// A `VIRTUAL` generated column is stored nowhere, and MariaDB 11.4+ leaves it out of the UPDATE
/// and DELETE row images while the table map and `information_schema` still list it (the vendored
/// reader learning to decode those images is lazyview #7). lazywf names an image's values by
/// position, so on such a server the VIRTUAL column and every column after it would be read as
/// their neighbour's value — a key built from the wrong value, with nothing logged. So:
///
/// * on a server whose images carry the column (MariaDB before 11.4, MySQL, Percona — which of the
///   two worlds a run is in comes from the printed facts, not from the version), the rule reads
///   the value it asked for and marks its keys;
/// * on a server that leaves it out, the trigger that would read it is refused, with the reason on
///   `/status`, and marks nothing at all;
/// * a rule that reads only columns standing BEFORE the first VIRTUAL column is allowed on every
///   server, because those values are in the image everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn virtual_generated_column_between_real_columns() {
    let root = need_db!();
    let db = Db::fresh(&root, "vcol").await;
    db.exec(
        "CREATE TABLE items (id INT PRIMARY KEY, price INT, price_x2 INT AS (price * 2) VIRTUAL, \
         name VARCHAR(50), stock INT)",
    )
    .await;
    // The same shape with the VIRTUAL column last: everything a rule reads stands before it.
    db.exec(
        "CREATE TABLE kept (id INT PRIMARY KEY, name VARCHAR(50), stock INT, \
         note VARCHAR(60) AS (CONCAT(name, '/', stock)) VIRTUAL)",
    )
    .await;
    // And the same shape read through a Handlebars key, which takes the whole row at once and so
    // cannot be checked column by column.
    db.exec(
        "CREATE TABLE wide (id INT PRIMARY KEY, name VARCHAR(50), stock INT, \
         note VARCHAR(60) AS (CONCAT(name, '/', stock)) VIRTUAL)",
    )
    .await;
    tables(&db).await;

    /// The keys of every action the mock has seen, from the header the template renders.
    fn keys(m: &Mock) -> std::collections::BTreeSet<String> {
        m.requests()
            .iter()
            .filter_map(|r| r.header("x-keys"))
            .flat_map(|k| k.split('|').map(str::to_string))
            .collect()
    }
    async fn keys_until(
        m: &Mock,
        want: &[&str],
        what: &'static str,
    ) -> std::collections::BTreeSet<String> {
        let wanted: std::collections::BTreeSet<String> =
            want.iter().map(|k| k.to_string()).collect();
        wait_for(Duration::from_secs(15), what, || async {
            let got = keys(m);
            wanted.iter().all(|k| got.contains(k)).then_some(got)
        })
        .await
    }

    // The wire first: what the server writes and what the engine's catalog would name it with.
    // Row 1 belongs to the probe; the rules below work on rows 2 and 3.
    let facts = wire_facts(&db, "items", 3, || async {
        db.exec("INSERT INTO items (id, price, name, stock) VALUES (1, 5, 'thing', 3)")
            .await;
        db.exec("UPDATE items SET stock = 9 WHERE id = 1").await;
        db.exec("DELETE FROM items WHERE id = 1").await;
    })
    .await;
    for line in &facts {
        println!("vcol {line}");
    }
    let kept_facts = wire_facts(&db, "kept", 3, || async {
        db.exec("INSERT INTO kept (id, name, stock) VALUES (1, 'probe', 1)")
            .await;
        db.exec("UPDATE kept SET stock = 2 WHERE id = 1").await;
        db.exec("DELETE FROM kept WHERE id = 1").await;
    })
    .await;
    for line in &kept_facts {
        println!("vcol {line}");
    }

    let images: Vec<&String> = facts.iter().filter(|l| l.contains("value(s)")).collect();
    assert_eq!(
        images.len(),
        3,
        "the wire facts, one line per row event:\n{}",
        facts.join("\n")
    );
    // Whatever an image carries, the table map names all five columns and the layout the engine
    // reads is the table `information_schema` describes: that is why the count check below cannot
    // see a short image, and why the reads have to be refused by name instead.
    for line in &images {
        for want in [
            "table map 5 columns",
            "layout [id,price,price_x2,name,stock]",
        ] {
            assert!(
                line.contains(want),
                "no `{want}` in {line}\nthe wire facts:\n{}",
                facts.join("\n")
            );
        }
    }
    // An INSERT image carries the VIRTUAL column on every server; only UPDATE and DELETE run short.
    assert!(
        images
            .iter()
            .any(|l| l.starts_with("Insert") && l.contains("row image 5 value(s)")),
        "the insert image does not carry the VIRTUAL column:\n{}",
        facts.join("\n")
    );
    // Which of the two worlds this server is in, from its own wire rather than its version string.
    let short = images
        .iter()
        .any(|l| l.starts_with("Update") && !l.contains("row image 5 value(s)"));
    if short {
        // The two symptoms the refusal answers: a value the image ran out of, and a real column's
        // value sitting on the VIRTUAL column's name.
        for want in [r#""price_x2":"thing""#, r#""stock":null"#] {
            assert!(
                facts
                    .iter()
                    .any(|l| l.contains("Update after") && l.contains(want)),
                "the update's after-image does not show {want}\nthe wire facts:\n{}",
                facts.join("\n")
            );
        }
    } else {
        for line in &images {
            assert!(
                line.contains("row image 5 value(s)"),
                "this server writes the VIRTUAL column, so no image is short: {line}\n{}",
                facts.join("\n")
            );
        }
        // And the names the catalog gives the values are their own: the VIRTUAL column holds what the
        // server computed, the real columns around it hold what was written.
        for want in [
            r#""id":1"#,
            r#""price":5"#,
            r#""price_x2":10"#,
            r#""name":"thing""#,
            r#""stock":9"#,
        ] {
            assert!(
                facts
                    .iter()
                    .any(|l| l.contains("Update after") && l.contains(want)),
                "the update's after-image does not name {want}\nthe wire facts:\n{}",
                facts.join("\n")
            );
        }
    }
    // The table whose reads all stand before the VIRTUAL column: four columns named, and the image
    // holds the three the rule reads on every server.
    for line in kept_facts
        .iter()
        .filter(|l| l.contains("value(s)") && l.contains("table map"))
    {
        assert!(
            line.contains("table map 4 columns") && line.contains("layout [id,name,stock,note]"),
            "the kept table's facts: {line}\n{}",
            kept_facts.join("\n")
        );
    }

    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES \
         ('items', 'hook', 'PURGE /keys HTTP/1.1\nHost: {{endpoint.host}}\nX-Keys: \
          {{#each keys}}{{this}}{{#unless @last}}|{{/unless}}{{/each}}\n', 200)",
    )
    .await;
    // A key from a column after the virtual one, and a `changed_columns` trigger on another
    // column after it.
    db.trigger("items", "items", Some("{{new.name}}"), None)
        .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) \
         VALUES ('items', 'items', 'update', 'stock', 's_{{new.id}}')",
        (),
    )
    .await;
    // The rule that reads only columns before the VIRTUAL column of its table.
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES \
         ('kept', 'hook', 'PURGE /keys HTTP/1.1\nHost: {{endpoint.host}}\nX-Keys: \
          {{#each keys}}{{this}}{{#unless @last}}|{{/unless}}{{/each}}\n', 200)",
    )
    .await;
    db.trigger("kept", "kept", Some("k_{{new.name}}"), None)
        .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) \
         VALUES ('kept', 'kept', 'update', 'stock', 'n_{{new.id}}')",
        (),
    )
    .await;
    // A Handlebars key: a template of its own per line, rendered from the whole row.
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES \
         ('tpl', 'hook', 'PURGE /keys HTTP/1.1\nHost: {{endpoint.host}}\nX-Keys: \
          {{#each keys}}{{this}}{{#unless @last}}|{{/unless}}{{/each}}\n', 200)",
    )
    .await;
    db.trigger(
        "tpl",
        "wide",
        Some("{{#if new.name}}t_{{new.id}}{{else}}t_0{{/if}}"),
        None,
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;

    // The status row, so a later failure's log already carries what the rule itself says.
    println!("vcol status at start: {:?}", db.status("items").await);

    // Rows 2 and 3 of both tables (the probes' row 1 is deleted).
    mock.clear();
    db.exec(
        "INSERT INTO items (id, price, name, stock) VALUES (2, 7, 'gadget', 4), (3, 1, 'bolt', 8)",
    )
    .await;
    db.exec("INSERT INTO kept (id, name, stock) VALUES (2, 'alpha', 5), (3, 'beta', 6)")
        .await;
    db.exec("INSERT INTO wide (id, name, stock) VALUES (2, 'gamma', 7), (3, 'delta', 9)")
        .await;

    // The rule that reads nothing past the VIRTUAL column marks its keys on every server: those
    // values are in the image wherever the run happens.
    let got = keys_until(&mock, &["k_alpha", "k_beta"], "the kept rule's names").await;
    assert!(
        !got.contains("5") && !got.contains("6"),
        "the stock values reached the keys: {got:?}"
    );
    if short {
        // The other rule reads `name` and `stock`, both behind the VIRTUAL column: refused, and
        // nothing of it reaches the target — neither the right key nor the wrong one.
        assert!(
            !got.contains("gadget")
                && !got.contains("bolt")
                && !got.iter().any(|k| k.starts_with("s_")),
            "a refused trigger marked {got:?}"
        );
    } else {
        let got = keys_until(&mock, &["gadget", "bolt"], "the names of both rows").await;
        assert!(
            !got.contains("4") && !got.contains("8"),
            "the stock values reached the keys: {got:?}"
        );
    }

    // `stock` changes: the `changed_columns` trigger marks its key, on the columns before the
    // VIRTUAL one; the rule behind it stays refused or silent.
    inst.quiet().await;
    mock.clear();
    db.exec("UPDATE items SET stock = 12 WHERE id = 2").await;
    db.exec("UPDATE kept SET stock = 12 WHERE id = 2").await;
    db.exec("UPDATE wide SET stock = 12 WHERE id = 2").await;
    let got = keys_until(&mock, &["n_2"], "the stock change of the kept row").await;
    if short {
        assert!(
            !got.iter().any(|k| k.starts_with("s_")),
            "a refused trigger marked {got:?}"
        );
    } else {
        keys_until(&mock, &["s_2"], "the stock change of row 2").await;
    }
    // The Handlebars key reads the whole row, so on a server that runs short there is no telling
    // which of its values the template wanted: the rule is refused for the VIRTUAL column, and
    // marks nothing. Elsewhere it renders like any other key.
    if short {
        inst.quiet().await;
        assert!(
            !keys(&mock).iter().any(|k| k.starts_with("t_")),
            "the refused Handlebars rule marked {:?}",
            keys(&mock)
        );
    } else {
        keys_until(&mock, &["t_2"], "the Handlebars key of the changed row").await;
    }

    // `stock` unchanged: that trigger marks nothing (the other one does, which is how we know the
    // update was read at all).
    inst.quiet().await;
    mock.clear();
    db.exec("UPDATE items SET price = 8 WHERE id = 3").await;
    db.exec("UPDATE kept SET name = 'beta2' WHERE id = 3").await;
    if short {
        inst.quiet().await;
        let got = keys(&mock);
        assert!(
            !got.iter().any(|k| k.starts_with("s_") || k == "bolt"),
            "a refused trigger marked {got:?}"
        );
    } else {
        keys_until(&mock, &["bolt"], "the price change of row 3").await;
        assert!(
            !keys(&mock).iter().any(|k| k.starts_with("s_")),
            "stock did not change, yet the trigger fired: {:?}",
            keys(&mock)
        );
    }

    // The rule keeps its continuity either way; what differs is whether it reports a refusal.
    let status = wait_for(Duration::from_secs(15), "the status position", || async {
        let s = db.status("items").await?;
        (!s.0.is_empty() && s.1 > 0).then_some(s)
    })
    .await;
    let (_, _, reset, err) = status;
    assert!(
        reset.starts_with("missing: first start"),
        "the rule lost continuity: {reset}"
    );
    if short {
        let err = err.expect("the refused trigger reports why it marks nothing");
        println!("vcol last_error: {err}");
        for want in [
            "VIRTUAL generated column `price_x2`",
            "UPDATE and DELETE row images",
            "refuses the trigger",
        ] {
            assert!(err.contains(want), "the refusal says {err:?}, not `{want}`");
        }
        assert!(
            err.contains("reads `name`") || err.contains("reads `stock`"),
            "the refusal does not name the column it cannot read: {err}"
        );
    } else {
        assert!(err.is_none(), "the rule reports {err:?} (reset {reset:?})");
    }
    // The rule that reads the columns before the VIRTUAL one never carries a refusal, on any
    // server.
    let kept = db.status("kept").await.expect("the kept rule's status row");
    assert!(
        kept.3.is_none(),
        "the rule that reads before the VIRTUAL column reports {:?}",
        kept.3
    );
    // The Handlebars rule: refused where the images run short, silent and marking where they do.
    let tpl = wait_for(
        Duration::from_secs(15),
        "the Handlebars rule's position",
        || async {
            let s = db.status("tpl").await?;
            (!s.0.is_empty() && s.1 > 0).then_some(s)
        },
    )
    .await;
    if short {
        let err = tpl
            .3
            .expect("the Handlebars rule says why it marks nothing");
        println!("vcol handlebars last_error: {err}");
        for want in [
            "Handlebars key template",
            "VIRTUAL generated column `note`",
            "STORED",
        ] {
            assert!(err.contains(want), "the refusal says {err:?}, not `{want}`");
        }
    } else {
        assert!(tpl.3.is_none(), "the Handlebars rule reports {:?}", tpl.3);
    }
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_split_and_go_to_every_endpoint() {
    let root = need_db!();
    let db = Db::fresh(&root, "split").await;
    shop(&db).await;
    let (m1, m2) = (Mock::start().await, Mock::start().await);
    db.target(
        "purge",
        "http",
        &format!("{}, {}", m1.url(), m2.url()),
        None,
        "{}",
    )
    .await;
    db.exec(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, batch_max_bytes, debounce_ms) VALUES ('p', 'purge', \
         'PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Tags: {{#each keys}}{{this}}{{#unless @last}}|{{/unless}}{{/each}}\n', 160, 100)",
    )
    .await;
    db.trigger("p", "orders", Some("a_{{new.id}}\nb_{{new.id}}"), None)
        .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES ('p', 'customers', 'update', 'name', 'c_{{new.id}}')",
        (),
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'x')").await; // not an update: no key
    let values: Vec<String> = (1..=30).map(|i| format!("({i}, 1, 'n', 1)")).collect();
    db.exec(&format!("INSERT INTO orders VALUES {}", values.join(",")))
        .await;
    let tags = |m: &Mock| -> std::collections::BTreeSet<String> {
        m.requests()
            .iter()
            .filter_map(|r| r.header("x-tags").map(str::to_string))
            .flat_map(|t| t.split('|').map(str::to_string).collect::<Vec<_>>())
            .collect()
    };
    wait_for(
        Duration::from_secs(10),
        "60 tags on both endpoints",
        || async { (tags(&m1).len() == 60 && tags(&m2).len() == 60).then_some(()) },
    )
    .await;
    assert!(m1.requests().len() > 1, "split into several actions");
    for r in m1.requests() {
        let size = format!(
            "PURGE / HTTP/1.1\nHost: {}\nX-Tags: {}\n",
            r.header("host").unwrap(),
            r.header("x-tags").unwrap()
        )
        .len();
        assert!(size <= 160, "{size} bytes");
    }
    assert!(!tags(&m1).contains("c_1"));
    db.exec("UPDATE customers SET name = 'x' WHERE id = 1")
        .await; // no change of `name`: no key
    db.exec("UPDATE customers SET name = 'y' WHERE id = 1")
        .await;
    wait_for(Duration::from_secs(10), "the customer tag", || async {
        tags(&m1).contains("c_1").then_some(())
    })
    .await;
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_delay_while_keys_keep_changing() {
    let root = need_db!();
    let db = Db::fresh(&root, "delay").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    // What keeps the batch waiting instead of firing at the first quiet moment: the key changes
    // faster than the debounce window expires, so the window keeps restarting and the batch goes
    // out at the cap. The three numbers have to keep that relation at any load, so they are
    // derived from one measured quantity.
    //
    // The cadence of a DB call of this harness, measured with the engine already up and polling,
    // which is the load the loop below will run under: four writes to a table no batch is
    // watching, and the widest of those gaps. The debounce is four measured cadences (never under
    // the 1 s the test always used), the interval between key changes half the debounce, the cap a
    // debounce and a half. On an idle host, where a write costs a couple of hundred milliseconds,
    // that is the 1 s debounce and the 1.5 s cap the test always had and a change every 500 ms; on
    // a host where one write costs a second, all three stretch together instead of walking into
    // each other. The bound the test measures against stays `max_delay_ms` plus one scaled second.
    let mut cadence = Duration::ZERO;
    for i in 0..4u32 {
        let since = std::time::Instant::now();
        db.exec(&format!(
            "INSERT INTO customers (id, name) VALUES ({}, 'c')",
            100 + i
        ))
        .await;
        cadence = cadence.max(since.elapsed());
    }
    let debounce_ms = (4 * cadence.as_millis() as u64).max(1000);
    let every_ms = (debounce_ms / 2).max(1);
    let max_delay_ms = debounce_ms + debounce_ms / 2;
    db.exec(&format!(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms, max_delay_ms) \
         VALUES ('o', 'hook', 'POST /k HTTP/1.1\n\n{{{{join keys}}}}', {debounce_ms}, {max_delay_ms})"
    ))
    .await;
    // The delay is measured between two clock points of this test: the instant before the write
    // that queues the first key, and the instant the mock received the fire. Not the instant the
    // loop below happened to look at the mock: that one is a poll late at best, and a whole
    // round trip of the UPDATE late when the host is busy.
    let t0 = std::time::Instant::now();
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 0)").await;
    let bound = Duration::from_millis(max_delay_ms) + deadline(Duration::from_secs(1));
    let mut first;
    let mut gap = Duration::ZERO;
    let mut prev = t0;
    // The instants at which this test changed the key: the insert that queued it, then every
    // update below. The premise check further down names the one the batch was built on.
    let mut changes: Vec<std::time::Instant> = vec![std::time::Instant::now()];
    let mut i = 0;
    loop {
        db.exec(&format!("UPDATE orders SET total = {i} WHERE id = 1"))
            .await;
        let now = std::time::Instant::now();
        gap = gap.max(now.duration_since(prev));
        prev = now;
        changes.push(now);
        i += 1;
        first = mock.arrived("/k");
        // The key keeps changing until the fire has been seen (and five times at least, so the
        // window is a moving one): when it arrives, the key was still moving, which is what the
        // test is named for. Past the bound it stops, and the `no fire` expect below is the
        // failure of a rule that never released the batch.
        if i >= 5 && (first.is_some() || t0.elapsed() > bound) {
            break;
        }
        sleep_ms(every_ms).await;
    }
    let at = first.expect("no fire while the key kept changing");
    let delay = at
        .checked_duration_since(t0)
        .expect("the fire arrived before the write that queued it");
    // The change of the key that the batch was built on: the last one this test made at or before
    // the instant the mock received the fire.
    let containing = changes
        .iter()
        .rfind(|when| **when <= at)
        .copied()
        .expect("the fire arrived before any change this test made");
    let since_change = at.duration_since(containing);
    eprintln!(
        "first fire {delay:?} after the write (bound {bound:?} = max_delay {max_delay_ms} ms + \
         the host's own slack), key changed at most every {gap:?}, debounce {debounce_ms} ms, the \
         batch's own change was {since_change:?} old"
    );
    assert!(
        delay <= bound,
        "first fire after {delay:?}, the bound is {bound:?}"
    );
    // The premise of the name, checked rather than assumed: the fire is tied to a change this test
    // made, and that change was still inside the debounce window when the fire arrived. If it were
    // older than the window, the batch would have sat idle and this run would be measuring some
    // other rule's timing, not a key that kept changing. The allowance is one change interval:
    // these are the client's own instants, and the engine sees a change only when it polls it, so
    // its view of the window trails this record by up to the interval between changes (`gap`, the
    // widest the loop actually saw). That interval is derived from the same measured cadence as the
    // debounce for the same reason: on a host where a write costs a second, a fixed interval would
    // let the window expire mid-change and the test would be measuring something else again.
    assert!(
        since_change < Duration::from_millis(debounce_ms) + gap,
        "the change the batch was built on was {since_change:?} old when the fire arrived, longer \
         than the debounce of {debounce_ms} ms plus the widest gap between changes ({gap:?}): the \
         key had stopped moving, so this run measures a batch that sat idle rather than one whose \
         key kept changing (cadence was {cadence:?})"
    );
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_copy_stands_by_and_takes_over() {
    let root = need_db!();
    let db = Db::fresh(&root, "standby").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('o', 'hook', 'POST /k HTTP/1.1\n\n{{join keys \",\"}}', 50)").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let a = Instance::ready(config(&db, &[])).await;
    let b = Instance::start(config(&db, &[]));
    sleep_ms(1500).await;
    assert!(!b.health.is_ready(), "two active copies");
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 1)").await;
    wait_for(Duration::from_secs(10), "the active copy", || async {
        mock.requests().iter().any(|r| r.body == "1").then_some(())
    })
    .await;
    a.stop().await;
    wait_for(
        Duration::from_secs(15),
        "the standby to take over",
        || async { b.health.is_ready().then_some(()) },
    )
    .await;
    db.exec("INSERT INTO orders VALUES (2, 1, 'n', 1)").await;
    wait_for(Duration::from_secs(10), "the new active copy", || async {
        mock.requests()
            .iter()
            .any(|r| r.body.split(',').any(|k| k == "2"))
            .then_some(())
    })
    .await;
    b.stop().await;
}

/// `cargo test --test e2e throughput -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn throughput() {
    let root = need_db!();
    let db = Db::fresh(&root, "bulk").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, r#"{"errors":false}"#.into()));
    order_rule(&db, &mock).await;
    db.exec("UPDATE lazy_wire_fire_rule SET batch_max_keys = 1000 WHERE name = 'orders'")
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    let n = 50_000;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    let t0 = std::time::Instant::now();
    db.exec(&format!(
        "INSERT INTO orders WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 999) \
         SELECT a.i * 1000 + b.i + 1, 1, 'new', b.i FROM s a, s b WHERE a.i < {}",
        n / 1000
    ))
    .await;
    let written = t0.elapsed();
    wait_for(Duration::from_secs(120), "every order", || async {
        (last_docs(&mock.requests()).len() >= n).then_some(())
    })
    .await;
    let total = t0.elapsed();
    eprintln!(
        "{n} rows in one transaction: written in {written:?}, all fired after {total:?} ({} requests)",
        mock.requests().len()
    );
    inst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn targets_add_their_authentication() {
    let root = need_db!();
    let db = Db::fresh(&root, "auth").await;
    shop(&db).await;
    let mock = Mock::start().await;
    std::env::set_var("LWF_TEST_TOKEN", "t0ken");
    std::env::set_var("LWF_TEST_HEADER_KEY", "k3y");
    let dir = std::env::current_dir().unwrap().join("tmp/out/secrets");
    std::fs::create_dir_all(&dir).unwrap();
    let secret = dir.join("basic-pw");
    std::fs::write(&secret, "s3cret\n").unwrap();
    // `file:` references must lie in a secret directory.
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    let file_ref = format!("file:{}", secret.display());
    let url = format!("{}/base", mock.url());
    db.target(
        "basic",
        "http.basic",
        &url,
        None,
        &json!({"username": "u", "password": file_ref}).to_string(),
    )
    .await;
    db.target(
        "bearer",
        "http.bearer",
        &url,
        None,
        r#"{"token": "env:LWF_TEST_TOKEN"}"#,
    )
    .await;
    db.target(
        "header",
        "http.header",
        &url,
        None,
        // The secret fields of the http kinds are references only.
        r#"{"header": "X-Api-Key", "value": "env:LWF_TEST_HEADER_KEY", "note": "visible"}"#,
    )
    .await;
    for t in ["basic", "bearer", "header"] {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, \
             'POST /{{rule}} HTTP/1.1\n\n{{json target.vars}}', 50)",
            (t, t),
        )
        .await;
        db.trigger(t, "orders", Some("{{new.id}}"), None).await;
    }
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 1)").await;
    let reqs = wait_for(Duration::from_secs(10), "three requests", || async {
        let r = mock.requests();
        (r.len() >= 3).then_some(r)
    })
    .await;
    let get = |p: &str| {
        reqs.iter()
            .find(|r| r.path == p)
            .unwrap_or_else(|| panic!("no {p}: {reqs:?}"))
    };
    // base64("u:s3cret")
    assert_eq!(
        get("/base/basic").header("authorization"),
        Some("Basic dTpzM2NyZXQ=")
    );
    assert_eq!(
        get("/base/bearer").header("authorization"),
        Some("Bearer t0ken")
    );
    assert_eq!(get("/base/header").header("x-api-key"), Some("k3y"));
    // Credentials never reach templates.
    assert_eq!(get("/base/basic").body, r#"{"username":"u"}"#);
    assert_eq!(get("/base/bearer").body, "{}");
    assert_eq!(
        get("/base/header").body,
        r#"{"header":"X-Api-Key","note":"visible"}"#
    );
    inst.stop().await;
}

/// Signing of the http kinds: the Standard Webhooks scheme and the GitHub-style header. The
/// Mock is the receiver: it verifies the signature of each request and refuses the wrong
/// secret with a 401, so the mis-signed rule is held back as any other refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webhooks_signed_and_verified_by_the_receiver() {
    let root = need_db!();
    let db = Db::fresh(&root, "sign").await;
    shop(&db).await;
    // The spec's published test secret (its reference libraries pin the same one in their
    // tests): the Mock verifies against its key. The wrong one decodes to another key.
    use base64::Engine as _;
    const STD_SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    const WRONG_SECRET: &str = "whsec_9m8ZK5oW1xQeVbTcDrYfGhJkLnMoPqRsUvWxYz012345";
    let key = base64::engine::general_purpose::STANDARD
        .decode("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
        .unwrap();
    const HMAC_KEY: &str = "0123456789012345678901234567890123456789012345678901234567890123";
    std::env::set_var("LWF_E2E_SIGN_STD", STD_SECRET);
    std::env::set_var("LWF_E2E_SIGN_HMAC", HMAC_KEY);
    std::env::set_var("LWF_E2E_SIGN_WRONG", WRONG_SECRET);
    let mock = Mock::start().await;
    mock.respond(move |req| {
        let key = base64::engine::general_purpose::STANDARD
            .decode("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")
            .unwrap();
        let ok = match req.path.as_str() {
            "/std" | "/wrong" => req.standard_ok(&key),
            "/hmac" => req.hmac_ok(HMAC_KEY.as_bytes(), "x-signature-256", "sha256="),
            _ => false,
        };
        if ok {
            (200, "{}".into())
        } else {
            (401, "invalid signature".into())
        }
    });
    db.target(
        "sign-std",
        "http",
        &mock.url(),
        None,
        r#"{"sign": "standard", "sign_secret_ref": "env:LWF_E2E_SIGN_STD"}"#,
    )
    .await;
    db.target(
        "sign-hmac",
        "http",
        &mock.url(),
        None,
        r#"{"sign": "hmac", "sign_secret_ref": "env:LWF_E2E_SIGN_HMAC"}"#,
    )
    .await;
    db.target(
        "sign-wrong",
        "http",
        &mock.url(),
        None,
        r#"{"sign": "standard", "sign_secret_ref": "env:LWF_E2E_SIGN_WRONG"}"#,
    )
    .await;
    for (rule, target, path) in [
        ("sign-std", "sign-std", "/std"),
        ("sign-hmac", "sign-hmac", "/hmac"),
        ("sign-wrong", "sign-wrong", "/wrong"),
    ] {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 50)",
            (rule, target, &format!("POST {path} HTTP/1.1\n\n{{{{join keys \",\"}}}}")),
        )
        .await;
        db.trigger(rule, "orders", Some("{{new.id}}"), None).await;
    }
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(10), "the signed requests", || async {
        let r = mock.requests();
        (r.iter()
            .any(|r| r.path == "/std" && r.header("webhook-signature").is_some())
            && r.iter()
                .any(|r| r.path == "/hmac" && r.header("x-signature-256").is_some()))
        .then_some(())
    })
    .await;
    // The Mock accepted them: it answers 200 only when the signature verifies.
    let reqs = mock.requests();
    let std = reqs.iter().find(|r| r.path == "/std").unwrap();
    assert_eq!(
        std.header("webhook-signature"),
        std.standard_signature(&key).as_deref()
    );
    assert!(std
        .header("webhook-timestamp")
        .unwrap()
        .parse::<u64>()
        .is_ok());
    // The second insert: the ids stay unique per request.
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    wait_for(
        Duration::from_secs(10),
        "the second standard request",
        || async {
            let got = mock
                .requests()
                .iter()
                .filter(|r| r.path == "/std")
                .map(|r| r.header("webhook-id").unwrap().to_string())
                .collect::<Vec<_>>();
            (got.len() >= 2 && got[0] != got[1]).then_some(())
        },
    )
    .await;
    wait_for(
        Duration::from_secs(10),
        "the two signed rules past the inserts",
        || async {
            // A status row appears only once the rule has something to record: a missing row
            // waits for the next poll, as for the other rules' positions.
            let (b1, p1, _, e1) = db.status("sign-std").await?;
            let (b2, p2, _, e2) = db.status("sign-hmac").await?;
            (((b1, p1) > after && e1.is_none()) && ((b2, p2) > after && e2.is_none())).then_some(())
        },
    )
    .await;
    // The wrong secret: the Mock refused it, and lazywf's rule sees the refusal.
    wait_for(
        Duration::from_secs(10),
        "the mis-signed request",
        || async {
            mock.requests()
                .iter()
                .any(|r| r.path == "/wrong")
                .then_some(())
        },
    )
    .await;
    sleep_ms(1000).await;
    let (bf, bp, _, err) = db.status("sign-wrong").await.unwrap();
    assert!(
        (bf, bp) < after,
        "the mis-signed rule moved past its failing action"
    );
    assert!(err.as_deref().is_some_and(|e| e.contains("401")), "{err:?}");
    inst.stop().await;
}

/// One search engine of the OpenSearch / Elasticsearch family.
struct Search {
    name: &'static str,
    url: String,
    /// The target's `tls` column.
    tls: Option<String>,
    kind: &'static str,
    vars: Value,
    /// `Authorization` for the test's own calls.
    auth: Option<String>,
    /// How strays are paged: `search_after` sorted by `_id`, or a scroll (Elasticsearch 8
    /// refuses to sort on `_id` by default).
    scroll: bool,
}

async fn search_count(
    client: &lazy_wire_fire::http::Client,
    req: impl Fn(&str, &str, &str) -> lazy_wire_fire::http::RawRequest,
    index: &str,
    q: &str,
) -> Option<u64> {
    let t = Duration::from_secs(10);
    client
        .send(&req("POST", &format!("/{index}/_refresh"), ""), t)
        .await
        .ok()?;
    let r = client
        .send(&req("POST", &format!("/{index}/_search?size=0"), q), t)
        .await
        .ok()?;
    let v: Value = serde_json::from_slice(&r.body).ok()?;
    v["hits"]["total"]["value"].as_u64()
}

/// The re-sync in place against a real engine: upserts and deletes through `_bulk`, strays
/// paged through the index.
async fn search_resync(root: &str, e: Search) {
    use lazy_wire_fire::http::{BaseUrl, Client, RawRequest};
    use lazy_wire_fire::tls::TlsSpec;
    let base = BaseUrl::parse(&e.url).unwrap();
    // The test's own calls skip certificate checks; the target is verified as configured.
    let tls = TlsSpec::parse(Some(if base.secure { "required" } else { "disabled" })).unwrap();
    let client = Client::new(base.clone(), &tls).unwrap();
    let host = base.host_header();
    let auth = e.auth.clone();
    let call = move |method: &str, path: &str, body: &str| {
        let mut r = RawRequest::parse(&format!(
            "{method} {path} HTTP/1.1\nContent-Type: application/json\n\n{body}"
        ))
        .unwrap()
        .unwrap();
        r.set_header("Host", host.clone());
        if let Some(a) = &auth {
            r.set_header("Authorization", a.clone());
        }
        r
    };
    // A passing health check does not mean the cluster takes index calls yet (CI run
    // 41748509714): wait for yellow first, bounded by the engine's own 60 s.
    let health = client
        .send(
            &call(
                "GET",
                "/_cluster/health?wait_for_status=yellow&timeout=60s",
                "",
            ),
            Duration::from_secs(70),
        )
        .await
        .unwrap();
    assert_eq!(health.status, 200, "{}: {}", e.name, health.body_snippet());
    let index = format!("lwf_items_{}_{}", e.name, std::process::id());
    let t = Duration::from_secs(10);
    // The index admin calls and the setup documents get more than the per-call 10 s.
    let admin = Duration::from_secs(60);
    client
        .send(&call("DELETE", &format!("/{index}"), ""), admin)
        .await
        .unwrap();
    // number_of_replicas 0: the single-node cluster can never host the replica, so it would sit
    // unassigned for the whole test (the cluster's yellow status is already waited for above).
    let r = client
        .send(
            &call(
                "PUT",
                &format!("/{index}"),
                r#"{"settings":{"number_of_replicas":0},"mappings":{"properties":{"title":{"type":"keyword"}}}}"#,
            ),
            admin,
        )
        .await
        .unwrap();
    assert_eq!(r.status, 200, "{}: {}", e.name, r.body_snippet());
    // Strays and a stale document, as an index left behind would have them.
    for (id, title) in [("5000", "stray"), ("5001", "stray"), ("1", "stale")] {
        client
            .send(
                &call(
                    "PUT",
                    &format!("/{index}/_doc/{id}?refresh=true"),
                    &format!(r#"{{"title":"{title}"}}"#),
                ),
                admin,
            )
            .await
            .unwrap();
    }

    let db = Db::fresh(root, &format!("search_{}", e.name)).await;
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, title VARCHAR(50))")
        .await;
    // ids 1..999 and 1001..1999: two listing pages of 1000.
    db.exec(
        "INSERT INTO items WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 999) \
         SELECT a.i * 1000 + b.i, CONCAT('item ', a.i * 1000 + b.i) FROM s a JOIN s b WHERE a.i < 2 AND b.i <= 999 \
         UNION ALL SELECT b.i, CONCAT('item ', b.i) FROM s b",
    )
    .await;
    let n: Vec<u64> = db.query("SELECT COUNT(*) FROM items").await;
    tables(&db).await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, expect, vars) VALUES ('os', ?, ?, ?, 'JSON_CONTAINS(body, ''false'', ''$.errors'')', ?)",
        (e.kind, &e.url, e.tls.clone(), e.vars.to_string()),
    )
    .await;
    let list = if e.scroll {
        "{{#if reset.next}}POST /_search/scroll HTTP/1.1\nContent-Type: application/json\n\n{\"scroll\":\"1m\",\"scroll_id\":{{json reset.next}}}{{else}}POST /{{vars.index}}/_search?scroll=1m HTTP/1.1\nContent-Type: application/json\n\n{\"size\":1000,\"_source\":false,\"sort\":[\"_doc\"]}{{/if}}"
    } else {
        "POST /{{vars.index}}/_search HTTP/1.1\nContent-Type: application/json\n\n{\"size\":1000,\"_source\":false,\"sort\":[{\"_id\":\"asc\"}]{{#if reset.next}},\"search_after\":{{json reset.next}}{{/if}}}"
    };
    let next = if e.scroll {
        "JSON_EXTRACT(body, '$._scroll_id')"
    } else {
        "JSON_EXTRACT(body, '$.hits.hits[last].sort')"
    };
    let steps = json!([
        {"for_each": "snapshot"},
        {"for_each": "stray", "list_template": list, "keys": "JSON_EXTRACT(body, '$.hits.hits[*]._id')", "next": next}
    ]);
    let template = "POST /{{vars.index}}/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{{#each docs}}{{#if (eq op \"delete\")}}{\"delete\":{\"_id\":{{json key}}}}\n{{else}}{\"update\":{\"_id\":{{json key}}}}\n{\"doc\":{{json doc}},\"doc_as_upsert\":true}\n{{/if}}{{/each}}";
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, snapshot_keys_sql, template, reset_steps, vars, debounce_ms) \
         VALUES ('items', 'os', 'SELECT id AS _key, title, NULL AS _op FROM items WHERE id IN (:keys)', 'SELECT id AS _key FROM items', ?, ?, ?, 100)",
        (template, steps.to_string(), json!({"index": index}).to_string()),
    )
    .await;
    db.trigger("items", "items", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(Duration::from_secs(60), "the re-synced index", || async {
        let total = search_count(&client, &call, &index, r#"{"track_total_hits":true}"#).await?;
        let strays = search_count(
            &client,
            &call,
            &index,
            r#"{"query":{"term":{"title":"stray"}}}"#,
        )
        .await?;
        let stale = search_count(
            &client,
            &call,
            &index,
            r#"{"query":{"term":{"title":"stale"}}}"#,
        )
        .await?;
        (total == n[0] && strays == 0 && stale == 0).then_some(())
    })
    .await;
    wait_for(Duration::from_secs(10), "reset done", || async {
        db.status("items")
            .await
            .filter(|s| s.2.starts_with("done"))
            .map(|_| ())
    })
    .await;

    db.exec("UPDATE items SET title = 'renamed' WHERE id = 5")
        .await;
    db.exec("DELETE FROM items WHERE id = 6").await;
    wait_for(Duration::from_secs(10), "incremental changes", || async {
        let renamed = search_count(
            &client,
            &call,
            &index,
            r#"{"query":{"term":{"title":"renamed"}}}"#,
        )
        .await?;
        let total = search_count(&client, &call, &index, r#"{"track_total_hits":true}"#).await?;
        (renamed == 1 && total == n[0] - 1).then_some(())
    })
    .await;
    inst.stop().await;
    client
        .send(&call("DELETE", &format!("/{index}"), ""), t)
        .await
        .ok();
}

/// `LAZYWF_TEST_OPENSEARCH=http://127.0.0.1:34200` (compose profile `search`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_opensearch() {
    let root = need_db!();
    let Ok(url) = std::env::var("LAZYWF_TEST_OPENSEARCH") else {
        return eprintln!("LAZYWF_TEST_OPENSEARCH not set: skipping");
    };
    search_resync(
        &root,
        Search {
            name: "os",
            url,
            tls: None,
            kind: "http",
            vars: json!({}),
            auth: None,
            scroll: false,
        },
    )
    .await;
}

/// `LAZYWF_TEST_ES7=http://127.0.0.1:34207`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_elasticsearch7() {
    let root = need_db!();
    let Ok(url) = std::env::var("LAZYWF_TEST_ES7") else {
        return eprintln!("LAZYWF_TEST_ES7 not set: skipping");
    };
    search_resync(
        &root,
        Search {
            name: "es7",
            url,
            tls: None,
            kind: "http",
            vars: json!({}),
            auth: None,
            scroll: false,
        },
    )
    .await;
}

/// `LAZYWF_TEST_ES8=https://127.0.0.1:34202`, `LAZYWF_TEST_ES8_CA=<http_ca.crt>`: security on,
/// basic auth from an `env:` reference, the cluster's private CA (`verify-ca`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_elasticsearch8() {
    let root = need_db!();
    let (Ok(url), Ok(ca)) = (
        std::env::var("LAZYWF_TEST_ES8"),
        std::env::var("LAZYWF_TEST_ES8_CA"),
    ) else {
        return eprintln!("LAZYWF_TEST_ES8 / LAZYWF_TEST_ES8_CA not set: skipping");
    };
    std::env::set_var("LWF_TEST_ES_PW", "elasticpw");
    search_resync(
        &root,
        Search {
            name: "es8",
            url,
            tls: Some(json!({"mode": "verify-ca", "ca": ca}).to_string()),
            kind: "http.basic",
            vars: json!({"username": "elastic", "password": "env:LWF_TEST_ES_PW"}),
            auth: Some("Basic ZWxhc3RpYzplbGFzdGljcHc=".into()),
            scroll: true,
        },
    )
    .await;
}

/// Row values written by an attacker: quotes and newlines in string keys, CR-LF in values that
/// land in a header, a document the target refuses. Nothing breaks out, the refused key is
/// isolated and dropped after STRIKES refusals, the rest goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hostile_values_stay_contained() {
    let root = need_db!();
    let db = Db::fresh(&root, "hostile").await;
    db.exec("CREATE TABLE notes (id VARCHAR(64) PRIMARY KEY, title VARCHAR(200))")
        .await;
    tables(&db).await;
    let mock = Mock::start().await;
    mock.respond(|r| {
        if r.body.contains("poison") {
            (400, r#"{"error":"bad document"}"#.into())
        } else {
            (200, "{}".into())
        }
    });
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let template = "POST /notes/_bulk HTTP/1.1\nX-Titles: {{#each docs}}{{doc.title}};{{/each}}\nContent-Type: application/x-ndjson\n\n{{#each docs}}{\"update\":{\"_id\":{{json key}}}}\n{\"doc\":{{json doc}}}\n{{/each}}";
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, template, debounce_ms) \
         VALUES ('n', 'hook', 'SELECT id AS _key, title FROM notes WHERE id IN (:keys)', ?, 100)",
        (template,),
    )
    .await;
    db.trigger("n", "notes", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec_params(
        "INSERT INTO notes VALUES (?, 'quote'), (?, 'newline in the key'), ('ok1', ?), ('poison', 'refused'), ('ok2', 'fine')",
        ("a\"b\"}},{\"delete\":{\"_id\":\"ok2\"}}", "line\nbreak", "x\r\nX-Evil: 1"),
    )
    .await;
    let start = binlog_end(&db).await;
    let ids = wait_for(Duration::from_secs(15), "the harmless keys", || async {
        let mut ids = std::collections::BTreeSet::new();
        for r in mock
            .requests()
            .iter()
            .filter(|r| !r.body.contains("poison"))
        {
            for line in r.ndjson() {
                if let Some(id) = line["update"]["_id"].as_str() {
                    ids.insert(id.to_string());
                }
                assert!(
                    line.get("delete").is_none(),
                    "a key broke out of its JSON string: {line}"
                );
            }
        }
        (ids.len() >= 3).then_some(ids)
    })
    .await;
    let expected: std::collections::BTreeSet<String> =
        ["a\"b\"}},{\"delete\":{\"_id\":\"ok2\"}}", "ok1", "ok2"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    assert_eq!(ids, expected, "keys reached the target intact and alone");
    for r in mock.requests() {
        assert!(
            r.header("x-evil").is_none(),
            "a value added a header: {r:?}"
        );
    }
    // The refused key is dropped after its strikes, the position moves on.
    let err = wait_for(
        Duration::from_secs(30),
        "the poison key dropped",
        || async {
            let (f, p, _, err) = db.status("n").await?;
            let err = err?;
            (err.contains("`poison` refused") && (f, p) >= start).then_some(err)
        },
    )
    .await;
    assert!(err.contains("bad document"), "{err}");
    let poison_tries = mock
        .requests()
        .iter()
        .filter(|r| r.body.contains("poison"))
        .count();
    assert!(
        (5..=7).contains(&poison_tries),
        "poison sent {poison_tries} times"
    );
    assert!(
        !mock.requests().iter().any(|r| r.body.contains("line")),
        "a key with a newline was sent"
    );
    inst.stop().await;
}

/// Names an attacker could enter: quotes, backslashes, a statement after a `;`.
const HOSTILE_NAMES: [&str; 3] = [
    "O'Brien",
    "x'); DROP TABLE copy; --",
    "back\\slash \\' quote",
];

/// `mysql` and `pgsql` targets: the template renders one statement per action; `sql_quote`
/// keeps every value a value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_targets() {
    let root = need_db!();
    let db = Db::fresh(&root, "sqlsrc").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100))")
        .await;
    tables(&db).await;
    // MySQL sink: another schema of the same server.
    let sink = Db::fresh(&root, "sqlsink").await;
    sink.exec("CREATE TABLE copy (id INT PRIMARY KEY, name VARCHAR(100), doc JSON)")
        .await;
    let my_host = root
        .trim_start_matches("mysql://")
        .split('@')
        .nth(1)
        .unwrap()
        .to_string();
    db.target(
        "mysink",
        "mysql",
        &format!("{my_host}/lwf_sqlsink"),
        Some("affected_rows != 0"),
        r#"{"username": "root", "password": "env:LWF_TEST_MY_PW"}"#,
    )
    .await;
    std::env::set_var("LWF_TEST_MY_PW", "root");
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, template, debounce_ms) VALUES ('my', 'mysink', \
         'SELECT id AS _key, name, NULL AS _op FROM customers WHERE id IN (:keys)', ?, 100)",
        ("{{#each docs}}{{#if (eq op \"delete\")}}DELETE FROM copy WHERE id = {{sql_quote key}}{{else}}INSERT INTO {{sql_ident \"copy\"}} (id, name, doc) VALUES ({{sql_quote key}}, {{sql_quote doc.name}}, {{sql_quote (json doc)}}) ON DUPLICATE KEY UPDATE name = VALUES(name), doc = VALUES(doc){{/if}}{{/each}}",),
    )
    .await;
    db.exec("UPDATE lazy_wire_fire_rule SET fire_per = 'key' WHERE name = 'my'")
        .await;
    db.trigger("my", "customers", Some("{{new.id}}"), None)
        .await;
    // PostgreSQL sink, if one is up (`postgres` service of the compose file).
    let pg = tokio_postgres::connect(
        "host=127.0.0.1 port=34432 user=postgres password=pgpw dbname=sink",
        tokio_postgres::NoTls,
    )
    .await
    .ok();
    let pg = match pg {
        Some((client, conn)) => {
            tokio::spawn(conn);
            client.batch_execute("DROP TABLE IF EXISTS copy; CREATE TABLE copy (id text PRIMARY KEY, name text, doc jsonb)").await.unwrap();
            std::env::set_var("LWF_TEST_PG_PW", "pgpw");
            db.target(
                "pgsink",
                "pgsql",
                "127.0.0.1:34432/sink",
                None,
                r#"{"username": "postgres", "password": "env:LWF_TEST_PG_PW"}"#,
            )
            .await;
            db.exec_params(
                "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('pg', 'pgsink', 'key', \
                 'SELECT id AS _key, name, NULL AS _op FROM customers WHERE id IN (:keys)', ?, 100)",
                ("{{#each docs}}{{#if (eq op \"delete\")}}DELETE FROM copy WHERE id = {{sql_quote key}}{{else}}INSERT INTO {{sql_ident \"copy\"}} (id, name, doc) VALUES ({{sql_quote key}}, {{sql_quote doc.name}}, {{sql_quote (json doc)}}) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, doc = EXCLUDED.doc{{/if}}{{/each}}",),
            )
            .await;
            db.trigger("pg", "customers", Some("{{new.id}}"), None)
                .await;
            Some(client)
        }
        None => {
            eprintln!("no PostgreSQL on 127.0.0.1:34432: testing the mysql target only");
            None
        }
    };
    let inst = Instance::ready(config(&db, &[])).await;
    for (i, n) in HOSTILE_NAMES.iter().enumerate() {
        db.exec_params("INSERT INTO customers VALUES (?, ?)", (i as i64 + 1, *n))
            .await;
    }
    let want: Vec<(i64, String)> = HOSTILE_NAMES
        .iter()
        .enumerate()
        .map(|(i, n)| (i as i64 + 1, n.to_string()))
        .collect();
    wait_for(Duration::from_secs(15), "the MySQL copy", || async {
        let got: Vec<(i64, String)> = sink.query("SELECT id, name FROM copy ORDER BY id").await;
        (got == want).then_some(())
    })
    .await;
    let doc: Vec<String> = sink
        .query("SELECT CAST(doc AS CHAR) FROM copy WHERE id = 3")
        .await;
    assert_eq!(
        serde_json::from_str::<Value>(&doc[0]).unwrap(),
        json!({"name": HOSTILE_NAMES[2]})
    );
    if let Some(pg) = &pg {
        wait_for(Duration::from_secs(15), "the PostgreSQL copy", || async {
            let rows = pg
                .query("SELECT id, name, doc::text FROM copy ORDER BY id", &[])
                .await
                .ok()?;
            let got: Vec<(i64, String)> = rows
                .iter()
                .map(|r| {
                    (
                        r.get::<_, String>(0).parse().unwrap(),
                        r.get::<_, String>(1),
                    )
                })
                .collect();
            let doc: Value = serde_json::from_str(&rows.get(2)?.get::<_, String>(2)).ok()?;
            (got == want && doc == json!({"name": HOSTILE_NAMES[2]})).then_some(())
        })
        .await;
    }
    db.exec("DELETE FROM customers WHERE id = 2").await;
    wait_for(Duration::from_secs(10), "the deletes", || async {
        let my: Vec<i64> = sink.query("SELECT id FROM copy ORDER BY id").await;
        let pg_ok = match &pg {
            Some(pg) => {
                pg.query("SELECT id FROM copy ORDER BY id", &[])
                    .await
                    .ok()?
                    .len()
                    == 2
            }
            None => true,
        };
        (my == vec![1, 3] && pg_ok).then_some(())
    })
    .await;
    inst.stop().await;
}

/// Values that look like SQL to a careless splice: an executable comment, a `;`, a backslash.
const ONE_STMT_NAMES: [&str; 3] = ["keep /*!; x */ \\ end", "it's \\' ; /*!", "plain"];

/// A MySQL SQL target runs one statement per action, and the server enforces it as well. The
/// lexer refuses the probe's shape (an executable comment, which MySQL runs as a second statement)
/// and two statements: those keys insert nothing. Sent straight to the server, the same texts are
/// refused too (prepared statements), and `sql_quote`d values come out as they went in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_target_one_statement() {
    let root = need_db!();
    let db = Db::fresh(&root, "sqlone").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100))")
        .await;
    tables(&db).await;
    let sink = Db::fresh(&root, "sqlone_sink").await;
    sink.exec("CREATE TABLE probe (id INT PRIMARY KEY)").await;
    sink.exec("CREATE TABLE copy (id INT PRIMARY KEY, name VARCHAR(100))")
        .await;
    sink.exec("CREATE TABLE fl (f FLOAT)").await;
    sink.exec("INSERT INTO fl VALUES (1.1)").await;
    let opts = mysql_async::Opts::from_url(&root).unwrap();
    let user = opts.user().unwrap_or("root").to_string();
    let pass = opts.pass().unwrap_or("").to_string();
    std::env::set_var("LWF_TEST_ONE_PW", &pass);
    let my_host = format!(
        "{}:{}/{}",
        opts.ip_or_hostname(),
        opts.tcp_port(),
        sink.schema
    );
    db.target(
        "mysink",
        "mysql",
        &my_host,
        None,
        &json!({"username": user, "password": "env:LWF_TEST_ONE_PW"}).to_string(),
    )
    .await;
    // Rule, template: one per case. Each rule fires per key, on every customer.
    let rules = [
        // The probe's shape: the lexer reads one statement; MySQL would run the INSERT too.
        (
            "probe",
            "/*!SELECT '*/' */; INSERT INTO probe VALUES (21)",
        ),
        // Two plain statements: refused by the count.
        (
            "twice",
            "INSERT INTO probe VALUES (22); INSERT INTO probe VALUES (23)",
        ),
        // Copies the hostile names with sql_quote, one statement per key.
        (
            "copy",
            "{{#each docs}}INSERT INTO copy (id, name) VALUES ({{sql_quote key}}, {{sql_quote doc.name}}){{/each}}",
        ),
    ];
    for (name, template) in rules {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES (?, 'mysink', 'key', \
             'SELECT id AS _key, name, NULL AS _op FROM customers WHERE id IN (:keys)', ?, 100)",
            (name, template),
        )
        .await;
        db.trigger(name, "customers", Some("{{new.id}}"), None)
            .await;
    }
    let inst = Instance::ready(config(&db, &[])).await;
    for (i, n) in ONE_STMT_NAMES.iter().enumerate() {
        db.exec_params("INSERT INTO customers VALUES (?, ?)", (i as i64 + 1, *n))
            .await;
    }
    let want: Vec<(i64, String)> = ONE_STMT_NAMES
        .iter()
        .enumerate()
        .map(|(i, n)| (i as i64 + 1, n.to_string()))
        .collect();
    wait_for(
        Duration::from_secs(30),
        "the copy of the hostile names",
        || async {
            let got: Vec<(i64, String)> = sink.query("SELECT id, name FROM copy ORDER BY id").await;
            (got == want).then_some(())
        },
    )
    .await;
    let err = wait_for(Duration::from_secs(30), "the probe refused", || async {
        let (_, _, _, err) = db.status("probe").await?;
        err.filter(|e| e.contains("executable comments are not allowed"))
    })
    .await;
    assert!(err.contains("executable comments"), "{err}");
    let err = wait_for(
        Duration::from_secs(30),
        "the two statements refused",
        || async {
            let (_, _, _, err) = db.status("twice").await?;
            err.filter(|e| e.contains("holds 2 statements"))
        },
    )
    .await;
    assert!(err.contains("holds 2 statements"), "{err}");
    let probe: Vec<i64> = sink.query("SELECT id FROM probe ORDER BY id").await;
    assert!(probe.is_empty(), "refused actions inserted {probe:?}");
    inst.stop().await;

    // The server, without the lexer: the same texts are refused by the server itself.
    let addr = lazy_wire_fire::sqlwire::SqlAddr {
        host: opts.ip_or_hostname().to_string(),
        port: opts.tcp_port(),
        db: sink.schema.clone(),
    };
    let client = lazy_wire_fire::sqlwire::MySqlClient::new(
        addr,
        &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
        lazy_wire_fire::sqlwire::Credentials {
            user: Some(user),
            password: Some(pass),
        },
    );
    let t = Duration::from_secs(10);
    for sql in [
        "/*!SELECT '*/' */; INSERT INTO probe VALUES (41)",
        "INSERT INTO probe VALUES (42); INSERT INTO probe VALUES (43)",
    ] {
        client
            .exec(sql.to_string(), t)
            .await
            .expect_err("the server runs one statement per prepare");
    }
    client
        .rows("SELECT 1 AS a; SELECT 2 AS b".to_string(), t)
        .await
        .expect_err("the server runs one statement per prepare");
    let probe: Vec<i64> = sink.query("SELECT id FROM probe ORDER BY id").await;
    assert!(
        probe.is_empty(),
        "the server let a second statement run: {probe:?}"
    );
    // A single statement still runs, and the rows come back as before (FLOAT as its text).
    assert_eq!(
        client
            .exec("INSERT INTO probe VALUES (44)".to_string(), t)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        client
            .rows("SELECT f FROM fl".to_string(), t)
            .await
            .unwrap(),
        json!([{"f": 1.1}])
    );
    // PostgreSQL, if up: the extended protocol runs one statement per call as well.
    match tokio_postgres::connect(
        "host=127.0.0.1 port=34432 user=postgres password=pgpw dbname=sink",
        tokio_postgres::NoTls,
    )
    .await
    {
        Ok((pg, conn)) => {
            tokio::spawn(conn);
            pg.batch_execute("DROP TABLE IF EXISTS one_probe; CREATE TABLE one_probe (id int)")
                .await
                .unwrap();
            pg.execute(
                "INSERT INTO one_probe VALUES (1); INSERT INTO one_probe VALUES (2)",
                &[],
            )
            .await
            .expect_err("PostgreSQL runs one statement per call");
            let n: i64 = pg
                .query_one("SELECT count(*) FROM one_probe", &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(n, 0, "PostgreSQL ran the second statement");
        }
        Err(_) => eprintln!("no PostgreSQL on 127.0.0.1:34432: skipping its check"),
    }
}

// ---------------------------------------------------------------------------------------------
// The Postgres line of `sqlwire`: how the classes the *server* answers with arrive, and what they
// cost the pooled connection. Proved against the compose PostgreSQL (`postgres` service,
// 127.0.0.1:34432) in our own `covpg_*` tables. The classes a running server will not produce on
// request — 53… insufficient resources, 58… system error, the 08 connection family, 40001
// serialization failure, a mid-query TCP cut — are proved over an in-process peer that answers
// with the same `ErrorResponse`s, in `src/tests/sqlwire.rs`. Nothing here stops, reconfigures or
// loads a server: no global setting is touched, only our own sessions and tables.

/// Is the server still serving that backend? The one witness to whether the pooled connection was
/// kept or dropped that cannot be fooled by a client-side lie.
async fn backend_alive(admin: &tokio_postgres::Client, pid: i32) -> bool {
    admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 AND datname = 'sink'",
            &[&pid],
        )
        .await
        .unwrap()
        .get::<_, i64>(0)
        > 0
}

/// Which backend the client's pooled connection is talking to, as the server itself answers it.
async fn pg_pid(c: &lazy_wire_fire::sqlwire::PgClient) -> i32 {
    let rows = c
        .rows("SELECT pg_backend_pid() AS pid", Duration::from_secs(10))
        .await
        .unwrap();
    rows[0]["pid"].as_i64().unwrap() as i32
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_target_reports_the_class_it_reached() {
    const PG: &str = "host=127.0.0.1 port=34432 user=postgres password=pgpw dbname=sink";
    let (admin, conn) = match tokio_postgres::connect(PG, tokio_postgres::NoTls).await {
        Ok(x) => x,
        Err(_) => {
            eprintln!("no PostgreSQL on 127.0.0.1:34432: skipping");
            return;
        }
    };
    tokio::spawn(conn);
    // The witnesses read from the admin session, which the closures below borrow: a
    // `tokio_postgres::Client` is not `Clone`, so they share one reference rather than a copy.
    let admin = &admin;
    admin
        .batch_execute(
            "DROP TABLE IF EXISTS covpg_lock, covpg_dup, covpg_wal, covpg_typed;
             CREATE TABLE covpg_lock (id bigint PRIMARY KEY);
             INSERT INTO covpg_lock VALUES (7), (11), (12), (13);
             CREATE TABLE covpg_dup (id text PRIMARY KEY);
             INSERT INTO covpg_dup VALUES ('taken');
             CREATE TABLE covpg_wal (id bigserial PRIMARY KEY, blob text);
             CREATE TABLE covpg_typed (t text, i2 int2, i4 int4, i8 int8, f8 float8, b bool,
                                       n numeric, nan float8, z timestamptz);
             INSERT INTO covpg_typed
                 VALUES ('plain é text', -7, 400, 9007199254740993, -2.25, true, 12.34, 'NaN',
                         '2026-11-23T00:00:00Z');
             INSERT INTO covpg_typed (t) VALUES (NULL);",
        )
        .await
        .unwrap();

    // A pooled client of the product's own: one per claim that needs a session of its own.
    fn pool() -> lazy_wire_fire::sqlwire::PgClient {
        lazy_wire_fire::sqlwire::PgClient::new(
            lazy_wire_fire::sqlwire::SqlAddr {
                host: "127.0.0.1".into(),
                port: 34432,
                db: "sink".into(),
            },
            &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
            lazy_wire_fire::sqlwire::Credentials {
                user: Some("postgres".into()),
                password: Some("pgpw".into()),
            },
        )
    }
    let client = pool();
    let t = Duration::from_secs(10);
    // Another session, for the locks and the cancels — never the one under test.
    async fn other() -> tokio_postgres::Client {
        let (c, conn) = tokio_postgres::connect(PG, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        c
    }

    // A statement the server refuses *as a statement*: the SQLSTATE and the server's own words in
    // the text, a refusal the engine counts as a strike — and the pooled connection survives it,
    // so the next statement runs on the very same backend.
    let p = pg_pid(&client).await;
    let e = client
        .exec("INSERT INTO covpg_dup VALUES ('taken')", t)
        .await
        .expect_err("the key is taken");
    assert_eq!(
        format!("{e:#}"),
        r#"postgres 127.0.0.1/sink: 23505: duplicate key value violates unique constraint "covpg_dup_pkey""#
    );
    assert!(
        lazy_wire_fire::target::is_rejected(&e),
        "a duplicate key is the statement's fault: {e:#}"
    );
    assert!(
        backend_alive(admin, p).await,
        "a refusal costs nothing but the answer"
    );
    assert_eq!(
        pg_pid(&client).await,
        p,
        "and the next statement reuses the connection"
    );

    // A lock that will not come, asked for with `NOWAIT`: 55P03 is not the statement's fault (the
    // key stays buffered rather than struck) and the session that waited on it is dropped, so the
    // retry does not walk into a backend the server may still be busy with.
    let locker = other().await;
    locker
        .batch_execute("BEGIN; UPDATE covpg_lock SET id = id WHERE id = 7;")
        .await
        .unwrap();
    let p = pg_pid(&client).await;
    let e = client
        .exec(
            "SELECT id FROM covpg_lock WHERE id = 7 FOR UPDATE NOWAIT",
            t,
        )
        .await
        .expect_err("row 7 is locked");
    assert_eq!(
        format!("{e:#}"),
        r#"postgres 127.0.0.1/sink: 55P03: could not obtain lock on row in relation "covpg_lock""#
    );
    assert!(
        !lazy_wire_fire::target::is_rejected(&e),
        "a lock is not the statement's fault: {e:#}"
    );
    wait_for(
        Duration::from_secs(10),
        "the backend a transient cost",
        || async { (!backend_alive(admin, p).await).then_some(()) },
    )
    .await;
    locker.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        client
            .exec(
                "SELECT id FROM covpg_lock WHERE id = 7 FOR UPDATE NOWAIT",
                t
            )
            .await
            .unwrap(),
        1,
        "the same statement runs once the lock is free"
    );
    assert_ne!(pg_pid(&client).await, p, "and on another backend");

    // A real deadlock, with our own statement the one the server rolls back. The other session
    // holds a row; our session holds a session advisory lock and waits for that row; the other
    // session then asks for the advisory lock we hold. The wait is a cycle, and the server ends
    // one of the two transactions to break it: the session that *finds* the cycle — the one that
    // blocks last — is not the one it rolls back, so the other side asks only after we are queued,
    // and our statement is the victim. Both sides are ordinary sessions of our own: nothing global
    // is set, nothing is stopped or reconfigured.
    let mut attempts = Vec::new();
    let mut ours_died = false;
    for _ in 0..3 {
        // A session of the product's own for this attempt: the blocked statement holds the pool's
        // one connection, and the attempt's assertions are about that pool.
        let c = pool();
        // The budget is also what this session's `statement_timeout` is pinned to, so it has to be
        // well above the second the server takes to notice the cycle.
        let w = Duration::from_secs(20);
        c.exec("SELECT pg_advisory_unlock_all()", w)
            .await
            .expect("a first statement on the session");
        let p = pg_pid(&c).await;
        let rival = other().await;
        rival
            .batch_execute("BEGIN; UPDATE covpg_lock SET id = id WHERE id = 13;")
            .await
            .expect("the other side takes the row");
        c.exec("BEGIN", w).await.expect("a transaction block");
        c.exec("SELECT pg_advisory_lock(5002)", w)
            .await
            .expect("the advisory lock is free");
        // We block on the row. The `Client` goes with the statement, and comes back with it.
        let ours = tokio::spawn(async move {
            let r = c
                .exec("SELECT id FROM covpg_lock WHERE id = 13 FOR UPDATE", w)
                .await
                .map(|_| "ran".to_string())
                .map_err(|e| (format!("{e:#}"), lazy_wire_fire::target::is_rejected(&e)));
            (c, r)
        });
        wait_for(
            Duration::from_secs(15),
            "we wait for the other side's row",
            || async {
                (admin
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE pid = $1 \
                           AND state = 'active' AND wait_event_type = 'Lock' \
                           AND wait_event = 'transactionid' \
                           AND query LIKE '%covpg_lock WHERE id = 13 FOR UPDATE%'",
                        &[&p],
                    )
                    .await
                    .unwrap()
                    .get::<_, i64>(0)
                    == 1)
                    .then_some(())
            },
        )
        .await;
        // Now the other side asks for our advisory lock: it is the one that finds the cycle. It
        // stays connected (its row lock held) until the answer has been read.
        let (let_go, held_until) = tokio::sync::oneshot::channel::<()>();
        let asked = tokio::spawn(async move {
            let r = rival
                .batch_execute("SELECT pg_advisory_lock(5002);")
                .await
                .map(|_| "ran".to_string())
                .map_err(|e| {
                    e.as_db_error()
                        .map_or_else(|| e.to_string(), |d| d.code().code().to_string())
                });
            let _ = held_until.await;
            let _ = rival
                .batch_execute("ROLLBACK; SELECT pg_advisory_unlock_all();")
                .await;
            r
        });
        let (c, ours) = ours.await.expect("our statement came back");
        let _ = let_go.send(());
        let theirs = match tokio::time::timeout(Duration::from_secs(20), asked).await {
            Ok(Ok(Ok(s))) => s,
            Ok(Ok(Err(s))) => s,
            Ok(Err(_)) => "join".into(),
            Err(_) => "the other side stayed blocked".into(),
        };
        let text = ours.as_ref().map_or_else(|(t, _)| t.clone(), |s| s.clone());
        match text.as_str() {
            "postgres 127.0.0.1/sink: 40P01: deadlock detected" => {
                let (_, rejected) =
                    ours.expect_err("the deadlock our statement was rolled back for");
                // The server's decision, not the statement's: no strike.
                assert!(!rejected, "a deadlock is not the statement's fault: {text}");
                // And the session it rolled back is not pooled any more: the same statement runs
                // again, on another backend.
                wait_for(
                    Duration::from_secs(10),
                    "the backend the deadlock cost",
                    || async { (!backend_alive(admin, p).await).then_some(()) },
                )
                .await;
                assert_eq!(c.exec("SELECT 13", w).await.unwrap(), 1);
                assert_ne!(pg_pid(&c).await, p, "the retry is another backend");
                ours_died = true;
                break;
            }
            _ => {
                // The server ended the other transaction this time: our statement ran, and the
                // session still sits in its block holding the advisory lock, so let it go and try
                // again.
                c.exec("ROLLBACK", w).await.expect("end the block");
                c.exec("SELECT pg_advisory_unlock_all()", w)
                    .await
                    .expect("release the advisory lock");
                attempts.push(format!("ours {text:?}, theirs {theirs:?}"));
            }
        }
    }
    assert!(
        ours_died,
        "the server never made our statement the deadlock victim: {attempts:?}"
    );

    // A statement the server cancels for us (57014): transient by class — the wait was the
    // server's, not the statement's — and the session is dropped all the same.
    let p = pg_pid(&client).await;
    let watcher = {
        // A spawn needs ownership, and a `Client` cannot be cloned: the watcher watches from a
        // session of its own.
        let spy = other().await;
        tokio::spawn(async move {
            for _ in 0..400 {
                let n: i64 = spy
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity \
                         WHERE pid = $1 AND state = 'active' AND query LIKE 'SELECT pg_sleep%'",
                        &[&p],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if n == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            spy.query_one("SELECT pg_cancel_backend($1)", &[&p])
                .await
                .unwrap()
                .get::<_, bool>(0)
        })
    };
    let e = client
        .exec("SELECT pg_sleep(5)", t)
        .await
        .expect_err("the sleep was cancelled");
    assert!(watcher.await.unwrap(), "the cancel was accepted");
    assert_eq!(
        format!("{e:#}"),
        "postgres 127.0.0.1/sink: 57014: canceling statement due to user request"
    );
    assert!(
        !lazy_wire_fire::target::is_rejected(&e),
        "a cancel is not the statement's fault: {e:#}"
    );
    wait_for(
        Duration::from_secs(10),
        "the backend the cancel cost",
        || async { (!backend_alive(admin, p).await).then_some(()) },
    )
    .await;

    // The caller's own budget, not the server's: the statement is blocked elsewhere and the
    // session's `statement_timeout` (pinned to the first call's budget, 10 s) is far above it, so
    // the only clock that can end the wait is the one the action was given.
    let blocker = other().await;
    blocker
        .batch_execute("BEGIN; UPDATE covpg_lock SET id = id WHERE id = 13;")
        .await
        .unwrap();
    let p = pg_pid(&client).await;
    let e = client
        .exec(
            "SELECT id FROM covpg_lock WHERE id = 13 FOR UPDATE",
            Duration::from_millis(400),
        )
        .await
        .expect_err("the row stays locked");
    assert_eq!(format!("{e:#}"), "no answer within 400 ms");
    assert!(
        !lazy_wire_fire::target::is_rejected(&e),
        "a timeout is not a refusal: {e:#}"
    );
    blocker.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(client.exec("SELECT 13", t).await.unwrap(), 1);
    assert_ne!(
        pg_pid(&client).await,
        p,
        "the session still busy with the abandoned statement is gone"
    );

    // The listing path, which is what a stray step reads. The class still decides whether the
    // pooled connection survives, but the text is the client's own words: `rows` formats the
    // `tokio_postgres::Error`, whose `Display` for a server error is the flat "db error", so the
    // SQLSTATE the statement path names is lost here — and nothing on this path is a refusal.
    let p = pg_pid(&client).await;
    let e = client
        .rows("SELECT * FROM covpg_missing", t)
        .await
        .expect_err("there is no such table");
    assert_eq!(format!("{e:#}"), "postgres 127.0.0.1/sink: db error");
    assert!(
        !lazy_wire_fire::target::is_rejected(&e),
        "a listing's refusal never strikes: {e:#}"
    );
    assert_eq!(pg_pid(&client).await, p, "and it keeps the connection");
    let locker = other().await;
    locker
        .batch_execute("BEGIN; UPDATE covpg_lock SET id = id WHERE id = 11;")
        .await
        .unwrap();
    let p = pg_pid(&client).await;
    let e = client
        .rows(
            "SELECT id FROM covpg_lock WHERE id = 11 FOR UPDATE NOWAIT",
            t,
        )
        .await
        .expect_err("row 11 is locked");
    assert_eq!(format!("{e:#}"), "postgres 127.0.0.1/sink: db error");
    wait_for(
        Duration::from_secs(10),
        "the backend a transient listing cost",
        || async { (!backend_alive(admin, p).await).then_some(()) },
    )
    .await;
    locker.batch_execute("COMMIT").await.unwrap();

    // `pg_value`, with the server's own binary encodings: every mapping it makes, an int8 past
    // what an `f64` route would keep, and the shapes that come out `null` — a type no branch
    // accepts (numeric, timestamptz), a float8 the JSON has no number for (NaN), and a NULL.
    assert_eq!(
        client
            .rows("SELECT * FROM covpg_typed ORDER BY i8 NULLS LAST", t)
            .await
            .unwrap(),
        json!([
            {
                "t": "plain é text", "i2": -7, "i4": 400, "i8": 9_007_199_254_740_993i64,
                "f8": -2.25, "b": true, "n": null, "nan": null, "z": null,
            },
            {
                "t": null, "i2": null, "i4": null, "i8": null, "f8": null, "b": null,
                "n": null, "nan": null, "z": null,
            }
        ])
    );
}

/// The other half of the pg line: `targets::sql`'s listing, the half an engine action reaches — a
/// stray step's `list_template` is a SELECT and its `keys` read the rows. The rows come through
/// `pg_value`, so the keys are only the right ones if an int8 an `f64` would round arrives exactly:
/// the two strays here, 2^53 and 2^53+1, are one key to a float route and two to this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_target_stray_listing_reads_typed_rows() {
    const PG: &str = "host=127.0.0.1 port=34432 user=postgres password=pgpw dbname=sink";
    let (admin, conn) = match tokio_postgres::connect(PG, tokio_postgres::NoTls).await {
        Ok(x) => x,
        Err(_) => {
            eprintln!("no PostgreSQL on 127.0.0.1:34432: skipping");
            return;
        }
    };
    tokio::spawn(conn);
    admin
        .batch_execute(
            "DROP TABLE IF EXISTS covpg_strays, covpg_sent;
             CREATE TABLE covpg_strays (what text PRIMARY KEY, i8 bigint);
             INSERT INTO covpg_strays VALUES
                 ('k1', 1), ('k2', 2), ('k3', 3),
                 ('k9007199254740992', 9007199254740992),
                 ('k9007199254740993', 9007199254740993);
             CREATE TABLE covpg_sent (what text PRIMARY KEY, i8 text);",
        )
        .await
        .unwrap();

    let root = need_db!();
    let db = Db::fresh(&root, "covpgsrc").await;
    db.exec("CREATE TABLE covpg_items (id BIGINT PRIMARY KEY, name VARCHAR(50))")
        .await;
    db.exec("INSERT INTO covpg_items VALUES (1,'a'),(2,'b'),(3,'c')")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_TEST_PG_PW", "pgpw");
    db.target(
        "pglist",
        "pgsql",
        "127.0.0.1:34432/sink",
        None,
        r#"{"username": "postgres", "password": "env:LWF_TEST_PG_PW"}"#,
    )
    .await;
    let steps = json!([
        {"for_each": "snapshot"},
        {"for_each": "stray",
         "list_template": "SELECT what, i8 FROM covpg_strays",
         "keys": "JSON_EXTRACT(body, '$[*].i8')",
         "template": "DELETE FROM covpg_strays WHERE what \
           IN ({{#each docs}}{{#unless @first}}, {{/unless}}concat('k', {{sql_quote key}}){{/each}})"}
    ]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, snapshot_keys_sql, template, reset_steps, debounce_ms) \
         VALUES ('pgl', 'pglist', 'key', \
         'SELECT id AS _key, name, NULL AS _op FROM covpg_items WHERE id IN (:keys)', \
         'SELECT id AS _key FROM covpg_items', \
         '{{#each docs}}INSERT INTO covpg_sent (what, i8) \
          VALUES (concat(''k'', {{sql_quote key}}), {{sql_quote key}}) \
          ON CONFLICT (what) DO UPDATE SET i8 = EXCLUDED.i8{{/each}}', ?, 100)",
        (steps.to_string(),),
    )
    .await;
    db.trigger("pgl", "covpg_items", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    async fn listed(admin: &tokio_postgres::Client, what: &str) -> Vec<String> {
        admin
            .query(&format!("SELECT what FROM {what} ORDER BY what"), &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect()
    }
    wait_for(Duration::from_secs(60), "the strays deleted", || async {
        (listed(&admin, "covpg_strays").await == ["k1", "k2", "k3"]).then_some(())
    })
    .await;
    // The three documents of the source table went to the same target on the way: a listing the
    // engine reads, and statements it writes, over one connection.
    db.exec("UPDATE covpg_items SET name = 'z' WHERE id IN (1, 2, 3)")
        .await;
    wait_for(Duration::from_secs(60), "the documents sent", || async {
        (listed(&admin, "covpg_sent").await == ["k1", "k2", "k3"]).then_some(())
    })
    .await;
    wait_for(Duration::from_secs(15), "reset done", || async {
        db.status("pgl")
            .await
            .filter(|s| s.2.starts_with("done"))
            .map(|_| ())
    })
    .await;
    // Both big int8s were listed as keys of their own, and so both were deleted: a float route
    // would have folded them into one key and left the other row behind.
    assert_eq!(listed(&admin, "covpg_strays").await, ["k1", "k2", "k3"]);
    let n: i64 = admin
        .query_one(
            "SELECT count(*) FROM covpg_sent WHERE i8 IN ('1','2','3')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 3, "the documents went to the pg target too");
    assert!(
        db.status("pgl").await.unwrap().3.is_none(),
        "the rule said nothing wrong: {:?}",
        db.status("pgl").await
    );
    let struck: Vec<String> = warnings()
        .into_iter()
        .filter(|l| l.starts_with("WARN") && l.contains("pgl"))
        .collect();
    assert!(
        struck.is_empty(),
        "the pg listing or its deletes were refused: {struck:?}"
    );
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// `sqs` targets, against ElasticMQ (`LAZYWF_TEST_SQS`, e.g. http://127.0.0.1:34561; the
// `sqs` compose profile). The tests read the queues back through the same SQS JSON API.

const SQS_STANDARD: &str = r#"POST / HTTP/1.1
X-Amz-Target: AmazonSQS.SendMessageBatch
Content-Type: application/x-amz-json-1.0

{"QueueUrl": {{json endpoint.url}}, "Entries": [{{#each docs}}{"Id": "{{@index}}", "MessageBody": {{json (json doc)}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

const SQS_FIFO: &str = r#"POST / HTTP/1.1
X-Amz-Target: AmazonSQS.SendMessageBatch
Content-Type: application/x-amz-json-1.0

{"QueueUrl": {{json endpoint.url}}, "Entries": [{{#each docs}}{"Id": "{{@index}}", "MessageBody": {{json (json doc)}}, "MessageGroupId": {{json key}}{{#if doc.version}}, "MessageDeduplicationId": {{json doc.version}}{{/if}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

fn sqs_root() -> Option<String> {
    std::env::var("LAZYWF_TEST_SQS")
        .ok()
        .filter(|v| !v.is_empty())
}

macro_rules! need_sqs {
    () => {
        match sqs_root() {
            Some(u) => u,
            None => {
                eprintln!("LAZYWF_TEST_SQS not set: skipping (docker compose --profile sqs up -d elasticmq)");
                return;
            }
        }
    };
}

/// The SQS JSON API of the store (ElasticMQ).
struct Sqs {
    root: String,
    client: lazy_wire_fire::http::Client,
}

impl Sqs {
    fn new(root: &str) -> Sqs {
        Sqs {
            root: root.to_string(),
            client: lazy_wire_fire::http::Client::new(
                lazy_wire_fire::http::BaseUrl::parse(root).unwrap(),
                &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
        }
    }

    async fn call(&self, action: &str, body: Value) -> Value {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header("Content-Type", "application/x-amz-json-1.0".into());
        req.set_header("X-Amz-Target", format!("AmazonSQS.{action}"));
        let resp = self
            .client
            .send(&req, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            resp.status == 200,
            "{action}: {} {}",
            resp.status,
            resp.body_snippet()
        );
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// The `aws-sqs://` endpoint of a queue this store holds: the region in the URL, the
    /// store as `?endpoint=` (ElasticMQ answers as the account 000000000000).
    fn target(&self, queue_url: &str) -> String {
        let path = queue_url
            .strip_prefix(&self.root)
            .expect("a queue of this store");
        format!("aws-sqs://us-east-1{path}?endpoint={}", self.root)
    }

    /// Creates a queue; its queue URL.
    async fn create(&self, name: &str, fifo: bool) -> String {
        let attributes = if fifo {
            json!({"FifoQueue": "true", "ContentBasedDeduplication": "false"})
        } else {
            json!({})
        };
        self.call(
            "CreateQueue",
            json!({"QueueName": name, "Attributes": attributes}),
        )
        .await;
        format!("{}/000000000000/{name}", self.root)
    }

    async fn count(&self, url: &str) -> u64 {
        let v = self
            .call(
                "GetQueueAttributes",
                json!({"QueueUrl": url, "AttributeNames": ["ApproximateNumberOfMessages"]}),
            )
            .await;
        v["Attributes"]["ApproximateNumberOfMessages"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Every message of the queue, each received once (received messages stay hidden).
    async fn drain(&self, url: &str) -> Vec<Value> {
        let mut out = Vec::new();
        loop {
            let v = self
                .call(
                    "ReceiveMessage",
                    json!({"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0,
                           "VisibilityTimeout": 600, "AttributeNames": ["All"]}),
                )
                .await;
            let got = v
                .get("Messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if got.is_empty() {
                return out;
            }
            out.extend(got);
        }
    }
}

fn sqs_creds() -> String {
    std::env::set_var("LWF_TEST_SQS_AK", "lazywf");
    std::env::set_var("LWF_TEST_SQS_SK", "lazywfsecret");
    json!({"access_key_ref": "env:LWF_TEST_SQS_AK",
           "secret_key_ref": "env:LWF_TEST_SQS_SK"})
    .to_string()
}

/// A customers table with the rule `queue` that sends each changed row to `url`.
async fn sqs_rule(db: &Db, url: &str, template: &str, selects: &str) {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL)")
        .await;
    tables(db).await;
    db.target(
        "queue",
        "sqs",
        url,
        Some("JSON_EXTRACT(body, '$.MessageId') IS NOT NULL"),
        &sqs_creds(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('queue', 'queue', 'batch', 10, ?, ?, 100)",
        (selects, template),
    )
    .await;
    db.trigger("queue", "customers", Some("{{new.id}}"), None)
        .await;
}

/// Every changed key arrives once, as a message on a standard queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_standard_queue_every_key_once() {
    let root = need_db!();
    let sqs = Sqs::new(&need_sqs!());
    let db = Db::fresh(&root, "sqs_std").await;
    let url = sqs
        .create(&format!("lwf_std_{}", std::process::id()), false)
        .await;
    sqs_rule(
        &db,
        &sqs.target(&url),
        SQS_STANDARD,
        "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec(
        "INSERT INTO customers (id, name) VALUES (1,'Ann'),(2,'Bob'),(3,'Cy'),(4,'Di'),(5,'Ed')",
    )
    .await;
    wait_for(Duration::from_secs(20), "five messages", || async {
        (sqs.count(&url).await == 5).then_some(())
    })
    .await;
    sleep_ms(2000).await;
    assert_eq!(sqs.count(&url).await, 5, "a key was sent twice");
    let mut ids: Vec<i64> = sqs
        .drain(&url)
        .await
        .iter()
        .map(|m| {
            let body: Value = serde_json::from_str(m["Body"].as_str().unwrap()).unwrap();
            body["id"].as_i64().unwrap()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    inst.stop().await;
}

/// FIFO: the group id is the key, the dedupe id is the rule's `version` column (key and
/// revision), so a changed row is a new message and an unchanged one is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_fifo_group_is_the_key_and_dedupe_is_the_version() {
    let root = need_db!();
    let sqs = Sqs::new(&need_sqs!());
    let db = Db::fresh(&root, "sqs_fifo").await;
    let url = sqs
        .create(&format!("lwf_fifo_{}.fifo", std::process::id()), true)
        .await;
    sqs_rule(
        &db,
        &sqs.target(&url),
        SQS_FIFO,
        "SELECT c.id AS _key, c.id, c.name, CONCAT(c.id, '#', c.rev) AS version FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1)")
        .await;
    wait_for(Duration::from_secs(20), "three messages", || async {
        (sqs.count(&url).await == 3).then_some(())
    })
    .await;
    db.exec("UPDATE customers SET name = 'Bobby', rev = 2 WHERE id = 2")
        .await;
    wait_for(Duration::from_secs(20), "the changed row", || async {
        (sqs.count(&url).await == 4).then_some(())
    })
    .await;
    let mut seen: Vec<(String, String, String)> = sqs
        .drain(&url)
        .await
        .iter()
        .map(|m| {
            let body: Value = serde_json::from_str(m["Body"].as_str().unwrap()).unwrap();
            let attr = |k: &str| m["Attributes"][k].as_str().unwrap_or("").to_string();
            (
                body["id"].to_string(),
                attr("MessageGroupId"),
                attr("MessageDeduplicationId"),
            )
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("1".to_string(), "1".to_string(), "1#1".to_string()),
            ("2".to_string(), "2".to_string(), "2#1".to_string()),
            ("2".to_string(), "2".to_string(), "2#2".to_string()),
            ("3".to_string(), "3".to_string(), "3#1".to_string()),
        ]
    );
    inst.stop().await;
}

/// A batch with one bad entry: the refused key is retried and then dropped (a strike per
/// refusal), the others are confirmed once and not sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_batch_with_one_bad_entry_retries_only_that_key() {
    let root = need_db!();
    let sqs = Sqs::new(&need_sqs!());
    let db = Db::fresh(&root, "sqs_bad").await;
    let url = sqs
        .create(&format!("lwf_bad_{}.fifo", std::process::id()), true)
        .await;
    sqs_rule(
        &db,
        &sqs.target(&url),
        SQS_FIFO,
        "SELECT c.id AS _key, c.id, c.name, CONCAT(c.id, '#', c.rev) AS version FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    // Row 4 has no revision: its version (the dedupe id) is NULL, so the queue refuses that
    // entry alone (SenderFault true).
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1),(4,'Di',NULL)")
        .await;
    wait_for(
        Duration::from_secs(20),
        "the three good messages",
        || async { (sqs.count(&url).await == 3).then_some(()) },
    )
    .await;
    wait_for(
        Duration::from_secs(60),
        "key 4 dropped after its strikes",
        || async {
            db.status("queue")
                .await
                .and_then(|s| s.3)
                .filter(|e| e.contains("key `4`"))
                .map(|_| ())
        },
    )
    .await;
    sleep_ms(1000).await;
    assert_eq!(sqs.count(&url).await, 3, "a confirmed key was sent again");
    let mut ids: Vec<i64> = sqs
        .drain(&url)
        .await
        .iter()
        .map(|m| {
            let body: Value = serde_json::from_str(m["Body"].as_str().unwrap()).unwrap();
            body["id"].as_i64().unwrap()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// `lambda` targets, against an in-process Invoke stand-in (`Mock` in tests/common). The stand-in
// answers the Invoke path with the status, `X-Amz-Function-Error` and body each test scripts, and
// records every request: its path, its SigV4 `Authorization` and its payload. No emulator: the
// behaviours under test (per-item retries, function errors, throttles, event mode) are answered
// by the Invoke API itself, and a function image is not needed. The payload is the template's
// body; the key's document is in `orders[i].doc`.

const LAMBDA_PAYLOAD: &str = r#"{"orders": [{{#each docs}}{"key": {{json key}}, "doc": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;
const LAMBDA_SELECTS: &str =
    "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)";

fn lambda_creds() -> String {
    std::env::set_var("LWF_TEST_LAMBDA_AK", "lazywf");
    std::env::set_var("LWF_TEST_LAMBDA_SK", "lazywfsecret");
    json!({"access_key_ref": "env:LWF_TEST_LAMBDA_AK",
           "secret_key_ref": "env:LWF_TEST_LAMBDA_SK"})
    .to_string()
}

/// The invocations the stand-in received (its path is the function's Invoke path).
fn invokes(mock: &Mock) -> Vec<Req> {
    mock.requests()
        .into_iter()
        .filter(|r| {
            r.path
                .starts_with("/2015-03-31/functions/orders/invocations")
        })
        .collect()
}

/// The names of the customers an invocation carried.
fn names_in(r: &Req) -> Vec<String> {
    let v: Value = serde_json::from_str(&r.body).unwrap();
    v["orders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["doc"]["name"].as_str().unwrap().to_string())
        .collect()
}

/// A customers table with the rule `fn` (fire per `fire_per`) that invokes `endpoint`.
async fn lambda_rule(db: &Db, endpoint: &str, fire_per: &str) {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL)")
        .await;
    tables(db).await;
    db.target("fn", "lambda", endpoint, None, &lambda_creds())
        .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('fn', 'fn', ?, ?, ?, 100)",
        (fire_per, LAMBDA_SELECTS, LAMBDA_PAYLOAD),
    )
    .await;
    db.trigger("fn", "customers", Some("{{new.id}}"), None)
        .await;
}

/// Every key is invoked once with its document, signed for Lambda, on the function's qualifier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lambda_invokes_every_key_with_its_document() {
    let root = need_db!();
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    let db = Db::fresh(&root, "lambda_every").await;
    let endpoint = format!("aws-lambda://us-east-1/orders:live?endpoint={}", mock.url());
    lambda_rule(&db, &endpoint, "key").await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1)")
        .await;
    wait_for(Duration::from_secs(30), "three invocations", || async {
        (invokes(&mock).len() >= 3).then_some(())
    })
    .await;
    sleep_ms(1000).await;
    let calls = invokes(&mock);
    assert_eq!(calls.len(), 3, "a key was invoked again");
    let mut names: Vec<String> = calls.iter().flat_map(names_in).collect();
    names.sort();
    assert_eq!(names, ["Ann", "Bob", "Cy"]);
    let r = &calls[0];
    assert_eq!(r.method, "POST");
    assert_eq!(
        r.path,
        "/2015-03-31/functions/orders/invocations?Qualifier=live"
    );
    assert_eq!(r.header("X-Amz-Invocation-Type"), Some("RequestResponse"));
    let auth = r.header("Authorization").expect("a signed request");
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 Credential=lazywf/"),
        "{auth}"
    );
    assert!(auth.contains("/us-east-1/lambda/aws4_request"), "{auth}");
    inst.stop().await;
}

/// A function error (`X-Amz-Function-Error`) on one key's invocation refuses that key: it is
/// retried, then dropped after its strikes; the other keys are confirmed once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lambda_function_error_is_a_refusal() {
    let root = need_db!();
    let mock = Mock::start().await;
    mock.respond(|r| {
        if r.body.contains("Bob") {
            (200, r#"{"errorMessage":"boom","errorType":"Error"}"#.into())
        } else {
            (200, "{}".into())
        }
    });
    mock.headers(|r| {
        if r.body.contains("Bob") {
            vec![("X-Amz-Function-Error".into(), "Unhandled".into())]
        } else {
            Vec::new()
        }
    });
    let db = Db::fresh(&root, "lambda_fail").await;
    let endpoint = format!("aws-lambda://us-east-1/orders?endpoint={}", mock.url());
    lambda_rule(&db, &endpoint, "key").await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1)")
        .await;
    wait_for(
        Duration::from_secs(60),
        "key 2 dropped after its strikes",
        || async {
            db.status("fn")
                .await
                .and_then(|s| s.3)
                .filter(|e| e.contains("key `2`"))
                .map(|_| ())
        },
    )
    .await;
    sleep_ms(1000).await;
    let calls = invokes(&mock);
    let count = |n: &str| calls.iter().filter(|r| names_in(r) == [n]).count();
    assert_eq!(count("Ann"), 1, "a confirmed key was invoked again");
    assert_eq!(count("Cy"), 1, "a confirmed key was invoked again");
    assert!(count("Bob") >= 2, "a function error was not retried");
    inst.stop().await;
}

/// A throttle (429 TooManyRequestsException) is transient: the keys are retried until accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lambda_throttle_is_retried() {
    let root = need_db!();
    let mock = Mock::start().await;
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let accepted = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let (attempts, accepted) = (attempts.clone(), accepted.clone());
        mock.respond(move |r| {
            if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
                return (429, r#"{"Type":"User","message":"Rate Exceeded."}"#.into());
            }
            accepted.lock().unwrap().extend(names_in(r));
            (200, "{}".into())
        });
    }
    let db = Db::fresh(&root, "lambda_throttle").await;
    let endpoint = format!("aws-lambda://us-east-1/orders?endpoint={}", mock.url());
    lambda_rule(&db, &endpoint, "key").await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1)")
        .await;
    wait_for(Duration::from_secs(60), "both keys accepted", || async {
        (accepted.lock().unwrap().len() == 2).then_some(())
    })
    .await;
    sleep_ms(1000).await;
    let mut names = accepted.lock().unwrap().clone();
    names.sort();
    assert_eq!(names, ["Ann", "Bob"], "a key was lost or sent twice");
    assert!(
        attempts.load(std::sync::atomic::Ordering::SeqCst) >= 4,
        "the throttled keys were not retried"
    );
    inst.stop().await;
}

/// One batch invocation: the function lists one key in `batchItemFailures`. Only that key
/// retries; the keys the answer does not list are confirmed once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lambda_partial_batch_retries_only_the_listed_key() {
    let root = need_db!();
    let mock = Mock::start().await;
    let failed_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let failed_once = failed_once.clone();
        mock.respond(move |r| {
            if r.body.contains("Bob")
                && !failed_once.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return (
                    200,
                    r#"{"batchItemFailures":[{"itemIdentifier":"2"}]}"#.into(),
                );
            }
            (200, "{}".into())
        });
    }
    let db = Db::fresh(&root, "lambda_partial").await;
    let endpoint = format!("aws-lambda://us-east-1/orders?endpoint={}", mock.url());
    lambda_rule(&db, &endpoint, "batch").await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1)")
        .await;
    wait_for(
        Duration::from_secs(60),
        "Bob retried and accepted",
        || async {
            (invokes(&mock)
                .iter()
                .filter(|r| names_in(r).contains(&"Bob".to_string()))
                .count()
                >= 2)
                .then_some(())
        },
    )
    .await;
    sleep_ms(1000).await;
    let calls = invokes(&mock);
    let with = |n: &str| {
        calls
            .iter()
            .filter(|r| names_in(r).contains(&n.to_string()))
            .count()
    };
    assert_eq!(with("Ann"), 1, "a confirmed key was sent again");
    assert_eq!(with("Cy"), 1, "a confirmed key was sent again");
    assert_eq!(with("Bob"), 2, "only the listed key retries");
    let last = calls
        .iter()
        .rfind(|r| names_in(r).contains(&"Bob".to_string()))
        .unwrap();
    assert_eq!(names_in(last), ["Bob"], "the retry carried confirmed keys");
    inst.stop().await;
}

/// Event mode: one queued invocation per key, answered 202; nothing is sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lambda_event_mode_is_fire_and_forget() {
    let root = need_db!();
    let mock = Mock::start().await;
    mock.respond(|_| (202, String::new()));
    let db = Db::fresh(&root, "lambda_event").await;
    let endpoint = format!(
        "aws-lambda://us-east-1/orders?invocation=event&endpoint={}",
        mock.url()
    );
    lambda_rule(&db, &endpoint, "key").await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1)")
        .await;
    wait_for(Duration::from_secs(30), "two events", || async {
        (invokes(&mock).len() >= 2).then_some(())
    })
    .await;
    sleep_ms(1000).await;
    let calls = invokes(&mock);
    assert_eq!(calls.len(), 2, "an event was sent again");
    for r in &calls {
        assert_eq!(r.header("X-Amz-Invocation-Type"), Some("Event"));
        assert!(!r.path.contains("Qualifier"), "{}", r.path);
    }
    let mut names: Vec<String> = calls.iter().flat_map(names_in).collect();
    names.sort();
    assert_eq!(names, ["Ann", "Bob"]);
    assert!(
        db.status("fn").await.and_then(|s| s.3).is_none(),
        "an accepted event left an error"
    );
    inst.stop().await;
}

// `kinesis` targets, against kinesis-mock (`LAZYWF_TEST_KINESIS`, e.g. http://127.0.0.1:34562;
// the `kinesis` compose profile). The tests read the streams back through the same JSON API.
// A record's `Data` is the document (base64, `base64` in the template); a delete's is
// `{"op":"delete"}`. The key is the `PartitionKey`.

const KINESIS_PUT: &str = r#"POST / HTTP/1.1
X-Amz-Target: Kinesis_20131202.PutRecords
Content-Type: application/x-amz-json-1.1

{"StreamName": {{json endpoint.stream}}, "Records": [{{#each docs}}{"Data": "{{#if (eq op "delete")}}{{base64 '{"op":"delete"}'}}{{else}}{{base64 (json doc)}}{{/if}}", "PartitionKey": {{json key}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

fn kinesis_root() -> Option<String> {
    std::env::var("LAZYWF_TEST_KINESIS")
        .ok()
        .filter(|v| !v.is_empty())
}

macro_rules! need_kinesis {
    () => {
        match kinesis_root() {
            Some(u) => u,
            None => {
                eprintln!("LAZYWF_TEST_KINESIS not set: skipping (docker compose --profile kinesis up -d kinesis-mock)");
                return;
            }
        }
    };
}

/// The Kinesis JSON API of the store (kinesis-mock).
struct Kinesis {
    root: String,
    client: lazy_wire_fire::http::Client,
}

impl Kinesis {
    fn new(root: &str) -> Kinesis {
        Kinesis {
            root: root.to_string(),
            client: lazy_wire_fire::http::Client::new(
                lazy_wire_fire::http::BaseUrl::parse(root).unwrap(),
                &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
        }
    }

    async fn call(&self, action: &str, body: Value) -> Value {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header("Content-Type", "application/x-amz-json-1.1".into());
        req.set_header("X-Amz-Target", format!("Kinesis_20131202.{action}"));
        // kinesis-mock wants a SigV4-shaped Authorization and a date; it does not check the
        // signature. The test reads the streams back, it does not sign them for AWS.
        req.set_header("X-Amz-Date", "20260101T000000Z".into());
        req.set_header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=lazywf/20260101/us-east-1/kinesis/aws4_request, SignedHeaders=host;x-amz-date, Signature=00".into(),
        );
        let resp = self
            .client
            .send(&req, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            resp.status == 200,
            "{action}: {} {}",
            resp.status,
            resp.body_snippet()
        );
        if resp.body.is_empty() {
            // CreateStream answers 200 with no body.
            return Value::Null;
        }
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// Creates the stream and waits until it is ACTIVE: kinesis-mock answers
    /// ResourceInUseException ("the stream is not currently ACTIVE") to puts and reads while
    /// a new stream is still CREATING.
    async fn create_stream(&self, name: &str) {
        self.call("CreateStream", json!({"StreamName": name, "ShardCount": 1}))
            .await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let got = self
                .call("DescribeStreamSummary", json!({"StreamName": name}))
                .await;
            let status = got["StreamDescriptionSummary"]["StreamStatus"]
                .as_str()
                .unwrap_or("(no status)")
                .to_string();
            if status == "ACTIVE" {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "stream {name} is still {status} after 30 s"
            );
            sleep_ms(200).await;
        }
    }

    /// Every record of the stream, shard by shard, in the order they were put: the partition
    /// key and the decoded `Data`.
    async fn records(&self, stream: &str) -> Vec<(String, Vec<u8>)> {
        use base64::Engine as _;
        let shards = self.call("ListShards", json!({"StreamName": stream})).await;
        let mut out = Vec::new();
        for shard in shards["Shards"].as_array().cloned().unwrap_or_default() {
            let shard_id = shard["ShardId"].as_str().unwrap().to_string();
            let it = self
                .call(
                    "GetShardIterator",
                    json!({"StreamName": stream, "ShardId": shard_id,
                           "ShardIteratorType": "TRIM_HORIZON"}),
                )
                .await;
            let mut iterator = it["ShardIterator"].as_str().unwrap().to_string();
            loop {
                let got = self
                    .call(
                        "GetRecords",
                        json!({"ShardIterator": iterator, "Limit": 1000}),
                    )
                    .await;
                let recs = got["Records"].as_array().cloned().unwrap_or_default();
                if recs.is_empty() {
                    break;
                }
                for r in recs {
                    let key = r["PartitionKey"].as_str().unwrap().to_string();
                    let data = base64::engine::general_purpose::STANDARD
                        .decode(r["Data"].as_str().unwrap())
                        .unwrap();
                    out.push((key, data));
                }
                iterator = got["NextShardIterator"].as_str().unwrap().to_string();
            }
        }
        out
    }
}

fn kinesis_creds() -> String {
    std::env::set_var("LWF_TEST_KINESIS_AK", "lazywf");
    std::env::set_var("LWF_TEST_KINESIS_SK", "lazywfsecret");
    json!({"access_key_ref": "env:LWF_TEST_KINESIS_AK",
           "secret_key_ref": "env:LWF_TEST_KINESIS_SK"})
    .to_string()
}

/// A customers table, and the rule `stream` that puts each changed row to `stream` (one
/// PutRecords per flush, `PartitionKey` = the row's key). The target's endpoint names the stream
/// and the region, and sends to the store at `endpoint`.
async fn kinesis_rule(db: &Db, endpoint: &str, stream: &str, selects: &str) {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT NOT NULL DEFAULT 1)")
        .await;
    tables(db).await;
    db.target(
        "stream",
        "kinesis",
        &format!("aws-kinesis://us-east-1/{stream}?endpoint={endpoint}"),
        Some("JSON_EXTRACT(body, '$.SequenceNumber') IS NOT NULL"),
        &kinesis_creds(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('stream', 'stream', 'batch', 500, ?, ?, 100)",
        (selects, KINESIS_PUT),
    )
    .await;
    db.trigger("stream", "customers", Some("{{new.id}}"), None)
        .await;
}

/// Every changed key arrives once, as a record: `PartitionKey` = the key, `Data` = the document.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kinesis_every_changed_key_arrives_as_a_record() {
    let root = need_db!();
    let endpoint = need_kinesis!();
    let kin = Kinesis::new(&endpoint);
    let stream = format!("lwf_kin_every_{}", std::process::id());
    kin.create_stream(&stream).await;
    let db = Db::fresh(&root, "kin_every").await;
    kinesis_rule(
        &db,
        &endpoint,
        &stream,
        "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec(
        "INSERT INTO customers (id, name) VALUES (1,'Ann'),(2,'Bob'),(3,'Cy'),(4,'Di'),(5,'Ed')",
    )
    .await;
    wait_for(Duration::from_secs(20), "five records", || async {
        (kin.records(&stream).await.len() == 5).then_some(())
    })
    .await;
    sleep_ms(2000).await;
    let mut recs = kin.records(&stream).await;
    assert_eq!(recs.len(), 5, "a key was sent twice");
    recs.sort_by(|a, b| a.0.cmp(&b.0));
    let names = ["Ann", "Bob", "Cy", "Di", "Ed"];
    for (i, (key, data)) in recs.iter().enumerate() {
        assert_eq!(
            key,
            &(i + 1).to_string(),
            "the partition key is the row key"
        );
        let doc: Value = serde_json::from_slice(data).unwrap();
        assert_eq!(doc, json!({"id": i + 1, "name": names[i]}), "the document");
    }
    inst.stop().await;
}

/// A deleted row arrives as `op` delete (`{"op":"delete"}`), under its key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kinesis_delete_arrives_as_op_delete() {
    let root = need_db!();
    let endpoint = need_kinesis!();
    let kin = Kinesis::new(&endpoint);
    let stream = format!("lwf_kin_delete_{}", std::process::id());
    kin.create_stream(&stream).await;
    let db = Db::fresh(&root, "kin_delete").await;
    kinesis_rule(
        &db,
        &endpoint,
        &stream,
        "SELECT c.id AS _key, c.id, c.name, IF(c.active, NULL, 'delete') AS _op FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name) VALUES (1,'Ann'),(2,'Bob')")
        .await;
    wait_for(Duration::from_secs(20), "two records", || async {
        (kin.records(&stream).await.len() == 2).then_some(())
    })
    .await;
    db.exec("UPDATE customers SET active = 0 WHERE id = 2")
        .await;
    wait_for(Duration::from_secs(20), "the delete record", || async {
        (kin.records(&stream).await.len() == 3).then_some(())
    })
    .await;
    sleep_ms(2000).await;
    let recs = kin.records(&stream).await;
    assert_eq!(recs.len(), 3, "a key was sent twice");
    let deletes: Vec<&(String, Vec<u8>)> = recs
        .iter()
        .filter(|(_, d)| d.as_slice() == br#"{"op":"delete"}"#)
        .collect();
    assert_eq!(deletes.len(), 1, "one delete record");
    assert_eq!(deletes[0].0, "2", "the delete carries the row's key");
    // The live row is still an upsert with its document.
    let upserts: Vec<&(String, Vec<u8>)> = recs
        .iter()
        .filter(|(_, d)| d.as_slice() != br#"{"op":"delete"}"#)
        .collect();
    assert_eq!(upserts.len(), 2);
    assert!(upserts.iter().all(|(k, _)| k == "1" || k == "2"));
    inst.stop().await;
}

/// Two endpoints of one target disagree about a key: `refuser` refuses key 2 (400) while
/// `flaky` is unavailable for it (503). Key 1 shares its batch. A transient failure on any
/// endpoint means the action may pass later, so key 2 must not collect strikes (and be dropped)
/// while `flaky` is still unavailable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_on_one_endpoint_is_not_a_refusal() {
    let root = need_db!();
    let db = Db::fresh(&root, "mixed").await;
    shop(&db).await;
    let refuser = Mock::start().await;
    refuser.respond(|r| {
        if r.body.contains('2') {
            (400, "refused".into())
        } else {
            (200, "{}".into())
        }
    });
    let flaky = Mock::start().await;
    flaky.respond(|r| {
        if r.body.contains('2') {
            (503, "unavailable".into())
        } else {
            (200, "{}".into())
        }
    });
    db.target(
        "both",
        "http",
        &format!("{}, {}", refuser.url(), flaky.url()),
        None,
        "{}",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('m', 'both', 'POST /m HTTP/1.1\n\n{{join keys \",\"}}', 100)",
        (),
    )
    .await;
    db.trigger("m", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 1), (2, 1, 'n', 1)")
        .await;
    // Ten attempts at key 2 (a key that gets five strikes is dropped after the sixth).
    let dropped = wait_for(Duration::from_secs(30), "key 2 attempts", || async {
        let (_, _, _, err) = db.status("m").await?;
        if err.as_deref().is_some_and(|e| e.contains("dropped")) {
            return Some(true);
        }
        (flaky
            .requests()
            .iter()
            .filter(|r| r.body.contains('2'))
            .count()
            >= 10)
            .then_some(false)
    })
    .await;
    let (_, _, _, err) = db.status("m").await.unwrap();
    assert!(
        !dropped,
        "key 2 was dropped while `flaky` was unavailable: {err:?}"
    );
    inst.stop().await;
}

/// An endpoint whose first request is held until `open` is called and then answered 503 (the
/// flush that sent it fails); every other request is answered 200 at once. `arrived` and
/// `confirmed` record the keys of the requests received and of those answered 200.
struct Gate {
    url: String,
    open_tx: watch::Sender<bool>,
    arrived: Arc<Mutex<Vec<String>>>,
    confirmed: Arc<Mutex<Vec<String>>>,
}

impl Gate {
    async fn start() -> Gate {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (open_tx, open_rx) = watch::channel(false);
        let arrived = Arc::new(Mutex::new(Vec::new()));
        let confirmed = Arc::new(Mutex::new(Vec::new()));
        let (arrived2, confirmed2) = (arrived.clone(), confirmed.clone());
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let hold = std::mem::replace(&mut first, false);
                let (mut open, arrived, confirmed) =
                    (open_rx.clone(), arrived2.clone(), confirmed2.clone());
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        let mut chunk = [0u8; 8192];
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + len {
                        let mut chunk = [0u8; 8192];
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body = String::from_utf8_lossy(&buf[head_end..head_end + len]).to_string();
                    arrived.lock().unwrap().push(body.clone());
                    let status = if hold {
                        while !*open.borrow() {
                            if open.changed().await.is_err() {
                                return;
                            }
                        }
                        503
                    } else {
                        200
                    };
                    if status == 200 {
                        confirmed.lock().unwrap().push(body);
                    }
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        Gate {
            url,
            open_tx,
            arrived,
            confirmed,
        }
    }

    fn open(&self) {
        self.open_tx.send(true).unwrap();
    }

    /// The keys of the requests with the given record (comma-separated bodies), as a set.
    fn keys(record: &Mutex<Vec<String>>) -> std::collections::BTreeSet<String> {
        record
            .lock()
            .unwrap()
            .iter()
            .flat_map(|b| b.split(',').map(String::from).collect::<Vec<_>>())
            .collect()
    }
}

/// A rule removed while its flush waits on the target, and added back (with its reset steps)
/// before the target answers: every key of the table still reaches the target. The old flush
/// fails once released, and its result is discarded; the re-added rule covers the keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_and_added_back_mid_flush_loses_no_key() {
    let root = need_db!();
    let db = Db::fresh(&root, "readd").await;
    shop(&db).await;
    let gate = Gate::start().await;
    db.target("slow", "http", &gate.url, None, "{}").await;
    let steps = json!([{"for_each": "snapshot"}]).to_string();
    let add = "INSERT INTO lazy_wire_fire_rule (name, target, template, snapshot_keys_sql, reset_steps, debounce_ms) \
               VALUES ('o', 'slow', 'POST /o HTTP/1.1\n\n{{join keys \",\"}}', 'SELECT id AS _key FROM orders', ?, 50)";
    db.exec_params(add, (steps.clone(),)).await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 1), (2, 1, 'n', 1)")
        .await;
    wait_for(Duration::from_secs(10), "the flush in flight", || async {
        Gate::keys(&gate.arrived).contains("1").then_some(())
    })
    .await;
    db.exec("DELETE FROM lazy_wire_fire_rule WHERE name = 'o'")
        .await;
    wait_for(Duration::from_secs(10), "the rule removed", || async {
        inst.health.status()["rules"]
            .get("o")
            .is_none()
            .then_some(())
    })
    .await;
    db.exec_params(add, (steps,)).await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    db.exec("INSERT INTO orders VALUES (3, 1, 'n', 1)").await;
    wait_for(Duration::from_secs(10), "the rule back", || async {
        inst.health.status()["rules"]
            .get("o")
            .is_some()
            .then_some(())
    })
    .await;
    wait_for(Duration::from_secs(20), "every key confirmed", || async {
        let k = Gate::keys(&gate.confirmed);
        (["1", "2", "3"].iter().all(|x| k.contains(*x))).then_some(())
    })
    .await;
    gate.open();
    // The released flush answers 503 and its result is discarded: give it the moment it needs
    // to be processed before reading what the rule ended up reporting.
    sleep_ms(300).await;
    // Removing the rule deleted its status row, and only the engine's next persist writes it
    // again, so a one-shot read can land on the missing row and panic on a busy host. Poll for
    // the row's arrival, then assert what it says: `wait_for` panics on the timeout, so a row
    // that never comes back is still a red test.
    let (_, _, _, err) = wait_for(
        Duration::from_secs(20),
        "the re-added rule's status row",
        || async { db.status("o").await },
    )
    .await;
    assert!(!err.as_deref().unwrap_or("").contains("dropped"), "{err:?}");
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// The amqp kind against RabbitMQ (`LAZYWF_TEST_AMQP=127.0.0.1:34560`, compose profile `amqp`).

/// The broker's `host:port`, when the suite has one.
fn amqp_broker() -> Option<String> {
    std::env::var("LAZYWF_TEST_AMQP").ok()
}

/// One message as the test consumer reads it.
#[derive(Debug, Clone)]
struct Got {
    routing_key: String,
    message_id: String,
    content_type: String,
    persistent: bool,
    body: Value,
}

/// The test's own connection to the broker: an exchange, a durable queue bound to `order.changed`,
/// and everything read from the queue so far.
struct Amqp {
    _conn: lapin::Connection,
    chan: lapin::Channel,
    queue: String,
    got: std::sync::Mutex<Vec<Got>>,
}

impl Amqp {
    async fn setup(broker: &str, exchange: &str, queue: &str) -> Amqp {
        use lapin::{options::*, types::FieldTable, ConnectionProperties, ExchangeKind};
        let url = format!("amqp://lwftest:lwftest@{broker}/%2f");
        let conn = lapin::Connection::connect(&url, ConnectionProperties::default())
            .await
            .unwrap();
        let chan = conn.create_channel().await.unwrap();
        // A fresh queue: a rerun does not see the messages of an earlier one.
        chan.queue_delete(queue.into(), QueueDeleteOptions::default())
            .await
            .ok();
        chan.exchange_declare(
            exchange.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        chan.queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        chan.queue_bind(
            queue.into(),
            exchange.into(),
            "order.changed".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
        Amqp {
            _conn: conn,
            chan,
            queue: queue.into(),
            got: Default::default(),
        }
    }

    /// Reads every message waiting on the queue into `got`.
    async fn pull(&self) {
        use lapin::options::BasicGetOptions;
        loop {
            let Some(m) = self
                .chan
                .basic_get(self.queue.as_str().into(), BasicGetOptions { no_ack: true })
                .await
                .unwrap()
            else {
                return;
            };
            let d = m.delivery;
            let p = &d.properties;
            self.got.lock().unwrap().push(Got {
                routing_key: d.routing_key.to_string(),
                message_id: p
                    .message_id()
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                content_type: p
                    .content_type()
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                persistent: *p.delivery_mode() == Some(2),
                body: serde_json::from_slice(&d.data).unwrap_or(Value::Null),
            });
        }
    }

    /// Waits until at least `n` messages have been read, and returns all of them so far.
    async fn at_least(&self, n: usize) -> Vec<Got> {
        wait_for(
            Duration::from_secs(30),
            &format!("{n} AMQP messages"),
            || async {
                self.pull().await;
                let all = self.got.lock().unwrap().clone();
                (all.len() >= n).then_some(all)
            },
        )
        .await
    }
}

/// The envelope template: one message per key, to `exchange` / `order.changed`, with the key as
/// the message id and the JSON envelope `{key, op, doc}` as the body.
fn amqp_envelope(exchange: &str) -> String {
    format!(
        "{{{{#each docs}}}}PUBLISH /{exchange}/order.changed\nMessage-Id: {{{{key}}}}\n\n\
         {{\"key\":{{{{json key}}}},\"op\":{{{{json op}}}},\"doc\":{{{{json doc}}}}}}{{{{/each}}}}"
    )
}

/// `vars` of the `amqp` kind: the password of the test broker as a reference, since a
/// secret-bearing field never holds the secret itself.
fn amqp_vars() -> String {
    std::env::set_var("LWF_TEST_AMQP_PW", "lwftest");
    json!({"username": "lwftest", "password": "env:LWF_TEST_AMQP_PW"}).to_string()
}

/// The `amqp` kind as a message producer: one persistent JSON message per changed key on a bound
/// queue; a delete arrives as op delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn amqp_target() {
    let root = need_db!();
    let Some(broker) = amqp_broker() else {
        return eprintln!("LAZYWF_TEST_AMQP not set: skipping");
    };
    let db = Db::fresh(&root, "amqp_target").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let (ex, q) = ("lwf.amqp_target", "lwf.amqp_target.q");
    let admin = Amqp::setup(&broker, ex, q).await;

    let vars = amqp_vars();
    db.target(
        "bus",
        "amqp",
        &format!("amqp://{broker}/"),
        None,
        &vars.to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            amqp_envelope(ex),
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    let got = admin.at_least(2).await;
    assert_eq!(got.len(), 2, "one message per key: {got:?}");
    let by_id = |id: &str| got.iter().find(|g| g.message_id == id).unwrap().clone();
    for (id, sku, qty) in [("1", "A-1", 3), ("2", "B-2", 1)] {
        let g = by_id(id);
        assert_eq!(g.routing_key, "order.changed");
        assert_eq!(g.content_type, "application/json");
        assert!(g.persistent, "{g:?}");
        assert_eq!(
            g.body,
            json!({"key": id, "op": "upsert", "doc": {"sku": sku, "qty": qty}})
        );
    }

    db.exec("UPDATE orders SET qty = 4 WHERE id = 1").await;
    let got = admin.at_least(3).await;
    assert_eq!(got.len(), 3);
    assert_eq!(
        got[2].body,
        json!({"key": "1", "op": "upsert", "doc": {"sku": "A-1", "qty": 4}})
    );

    db.exec("DELETE FROM orders WHERE id = 2").await;
    let got = admin.at_least(4).await;
    assert_eq!(got.len(), 4);
    assert_eq!(got[3].message_id, "2");
    assert_eq!(got[3].body["key"], "2");
    assert_eq!(got[3].body["op"], "delete", "{:?}", got[3]);
    inst.stop().await;
}

/// A key whose message has no bound queue comes back as basic.return (312). It is refused,
/// struck and dropped after its strikes, because the other keys went through; the position
/// moves on, and `last_error` says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn amqp_refusal() {
    let root = need_db!();
    let Some(broker) = amqp_broker() else {
        return eprintln!("LAZYWF_TEST_AMQP not set: skipping");
    };
    let db = Db::fresh(&root, "amqp_refusal").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let (ex, q) = ("lwf.amqp_refusal", "lwf.amqp_refusal.q");
    let admin = Amqp::setup(&broker, ex, q).await;

    // Orders with the sku NOROUTE go to a routing key nothing is bound to.
    let template = amqp_envelope(ex).replace(
        "/order.changed",
        "/{{#if (eq doc.sku \"NOROUTE\")}}no.such.key{{else}}order.changed{{/if}}",
    );
    let vars = amqp_vars();
    db.target(
        "bus",
        "amqp",
        &format!("amqp://{broker}/"),
        None,
        &vars.to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            template,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    let start = binlog_end(&db).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'NOROUTE', 1)")
        .await;
    let err = wait_for(
        Duration::from_secs(60),
        "the unroutable key dropped",
        || async {
            let (f, p, _, err) = db.status("orders").await?;
            let err = err?;
            (err.contains("`2` refused") && (f, p) >= start).then_some(err)
        },
    )
    .await;
    assert!(err.contains("312") && err.contains("NO_ROUTE"), "{err}");

    let got = admin.at_least(1).await;
    assert_eq!(got.len(), 1, "only the routable key arrives: {got:?}");
    assert_eq!(got[0].message_id, "1");
    assert_eq!(got[0].body["doc"]["sku"], "A-1");
    inst.stop().await;
}

/// The broker drops every connection and refuses new ones (a restart, seen from lazywf). The
/// keys changed meanwhile are retried, and each arrives; a message the broker took before the cut
/// may arrive again (at-least-once), but none is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn amqp_restart() {
    let root = need_db!();
    let Some(broker) = amqp_broker() else {
        return eprintln!("LAZYWF_TEST_AMQP not set: skipping");
    };
    let db = Db::fresh(&root, "amqp_restart").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let (ex, q) = ("lwf.amqp_restart", "lwf.amqp_restart.q");
    let admin = Amqp::setup(&broker, ex, q).await;

    let upstream: &'static str = Box::leak(broker.clone().into_boxed_str());
    let proxy = Flaky::start(upstream).await;
    let vars = amqp_vars();
    db.target(
        "bus",
        "amqp",
        &format!("amqp://127.0.0.1:{}/", proxy.port),
        None,
        &vars.to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            amqp_envelope(ex),
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    assert_eq!(admin.at_least(2).await.len(), 2);

    proxy.cut();
    db.exec("INSERT INTO orders VALUES (3, 'C-3', 7)").await;
    wait_for(Duration::from_secs(30), "a refused connection", || async {
        (proxy.refused() >= 1).then_some(())
    })
    .await;
    admin.pull().await;
    assert!(
        !admin
            .got
            .lock()
            .unwrap()
            .iter()
            .any(|g| g.message_id == "3"),
        "key 3 arrived while the broker was cut off"
    );
    proxy.heal();

    let got = admin.at_least(3).await;
    let mut keys: Vec<&str> = got.iter().map(|g| g.message_id.as_str()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys, vec!["1", "2", "3"], "every key arrives: {got:?}");
    let g3 = got.iter().find(|g| g.message_id == "3").unwrap();
    assert_eq!(
        g3.body,
        json!({"key": "3", "op": "upsert", "doc": {"sku": "C-3", "qty": 7}})
    );
    inst.stop().await;
}

/// The `amqp` kind into a **stream queue** (`x-queue-type: stream`): the same publisher
/// confirms and the same envelope over AMQP 0-9-1, onto RabbitMQ's append-only log. A stream
/// is read with a consumer, not `basic.get`: it starts at the first offset and acks every
/// delivery (the acks are the credit that advances the consumer's offset), so the test reads
/// the log back without removing anything from it.
struct StreamQueue {
    _conn: lapin::Connection,
    drain: tokio::task::JoinHandle<()>,
    got: Arc<std::sync::Mutex<Vec<Got>>>,
}

impl StreamQueue {
    /// A fresh stream queue `queue` bound to `exchange` on `order.changed`, with a reader
    /// from its first offset. The queue is deleted first: a rerun sees no earlier messages.
    async fn setup(broker: &str, exchange: &str, queue: &str) -> StreamQueue {
        use futures_util::StreamExt;
        use lapin::options::*;
        use lapin::types::{AMQPValue, FieldTable};
        use lapin::{ConnectionProperties, ExchangeKind};
        let url = format!("amqp://lwftest:lwftest@{broker}/%2f");
        let conn = lapin::Connection::connect(&url, ConnectionProperties::default())
            .await
            .unwrap();
        let chan = conn.create_channel().await.unwrap();
        chan.queue_delete(queue.into(), QueueDeleteOptions::default())
            .await
            .ok();
        chan.exchange_declare(
            exchange.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        let mut args = FieldTable::default();
        args.insert(
            "x-queue-type".into(),
            AMQPValue::LongString("stream".into()),
        );
        chan.queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await
        .unwrap();
        chan.queue_bind(
            queue.into(),
            exchange.into(),
            "order.changed".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
        // A stream demands a prefetch and acknowledgements.
        chan.basic_qos(200, BasicQosOptions::default())
            .await
            .unwrap();
        let mut copts = FieldTable::default();
        copts.insert(
            "x-stream-offset".into(),
            AMQPValue::LongString("first".into()),
        );
        let mut consumer = chan
            .basic_consume(
                queue.into(),
                "lwf-reader".into(),
                BasicConsumeOptions::default(),
                copts,
            )
            .await
            .unwrap();
        let got = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = got.clone();
        let drain = tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                let Ok(d) = delivery else { break };
                let p = &d.properties;
                sink.lock().unwrap().push(Got {
                    routing_key: d.routing_key.to_string(),
                    message_id: p
                        .message_id()
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    content_type: p
                        .content_type()
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    persistent: *p.delivery_mode() == Some(2),
                    body: serde_json::from_slice(&d.data).unwrap_or(Value::Null),
                });
                let _ = d.ack(BasicAckOptions::default()).await;
            }
        });
        StreamQueue {
            _conn: conn,
            drain,
            got,
        }
    }

    /// Waits until at least `n` messages have been read from the log, and returns them all.
    async fn at_least(&self, n: usize) -> Vec<Got> {
        wait_for(
            Duration::from_secs(30),
            &format!("{n} stream messages"),
            || async {
                let all = self.got.lock().unwrap().clone();
                (all.len() >= n).then_some(all)
            },
        )
        .await
    }
}

impl Drop for StreamQueue {
    fn drop(&mut self) {
        self.drain.abort();
    }
}

/// The `amqp` kind publishing into a stream queue: every key arrives once with its document
/// (insert, update, delete), the confirm path unchanged from a classic queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn amqp_stream_queue_target() {
    let root = need_db!();
    let Some(broker) = amqp_broker() else {
        return eprintln!("LAZYWF_TEST_AMQP not set: skipping");
    };
    let db = Db::fresh(&root, "amqp_stream_queue_target").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let (ex, q) = (
        "lwf.amqp_stream_queue_target",
        "lwf.amqp_stream_queue_target.q",
    );
    let admin = StreamQueue::setup(&broker, ex, q).await;

    let vars = amqp_vars();
    db.target(
        "bus",
        "amqp",
        &format!("amqp://{broker}/"),
        None,
        &vars.to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            amqp_envelope(ex),
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    let got = admin.at_least(2).await;
    assert_eq!(got.len(), 2, "one message per key, no duplicates: {got:?}");
    let by_id = |id: &str| got.iter().find(|g| g.message_id == id).unwrap().clone();
    for (id, sku, qty) in [("1", "A-1", 3), ("2", "B-2", 1)] {
        let g = by_id(id);
        assert_eq!(g.routing_key, "order.changed");
        assert_eq!(g.content_type, "application/json");
        assert_eq!(
            g.body,
            json!({"key": id, "op": "upsert", "doc": {"sku": sku, "qty": qty}})
        );
    }

    db.exec("UPDATE orders SET qty = 4 WHERE id = 1").await;
    let got = admin.at_least(3).await;
    assert_eq!(got.len(), 3);
    assert_eq!(
        got[2].body,
        json!({"key": "1", "op": "upsert", "doc": {"sku": "A-1", "qty": 4}})
    );

    db.exec("DELETE FROM orders WHERE id = 2").await;
    let got = admin.at_least(4).await;
    assert_eq!(got.len(), 4);
    assert_eq!(got[3].message_id, "2");
    assert_eq!(got[3].body["op"], "delete", "{:?}", got[3]);
    inst.stop().await;
}

/// The broker is cut off and comes back, and lazywf itself restarts over it: the keys changed
/// meanwhile are re-sent and the stream holds every one of them (read back from its first
/// offset), none lost. A re-send may store a message twice (at-least-once); keys are checked
/// deduplicated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn amqp_stream_queue_restart() {
    let root = need_db!();
    let Some(broker) = amqp_broker() else {
        return eprintln!("LAZYWF_TEST_AMQP not set: skipping");
    };
    let db = Db::fresh(&root, "amqp_stream_queue_restart").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let (ex, q) = (
        "lwf.amqp_stream_queue_restart",
        "lwf.amqp_stream_queue_restart.q",
    );
    let admin = StreamQueue::setup(&broker, ex, q).await;

    let upstream: &'static str = Box::leak(broker.clone().into_boxed_str());
    let proxy = Flaky::start(upstream).await;
    let vars = amqp_vars();
    let cfg = config(&db, &[]);
    db.target(
        "bus",
        "amqp",
        &format!("amqp://127.0.0.1:{}/", proxy.port),
        None,
        &vars.to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            amqp_envelope(ex),
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(cfg.clone()).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    assert_eq!(admin.at_least(2).await.len(), 2);

    // The broker goes away; a key changes meanwhile and cannot be confirmed. The instance is
    // then stopped: its position did not pass key 3, so the new instance re-sends it.
    proxy.cut();
    db.exec("INSERT INTO orders VALUES (3, 'C-3', 7)").await;
    wait_for(Duration::from_secs(30), "a refused connection", || async {
        (proxy.refused() >= 1).then_some(())
    })
    .await;
    assert!(
        !admin
            .got
            .lock()
            .unwrap()
            .iter()
            .any(|g| g.message_id == "3"),
        "key 3 arrived while the broker was cut off"
    );
    inst.stop().await;
    proxy.heal();

    let inst = Instance::ready(cfg).await;
    let got = admin.at_least(3).await;
    let mut keys: Vec<&str> = got.iter().map(|g| g.message_id.as_str()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys, vec!["1", "2", "3"], "every key arrives: {got:?}");
    let g3 = got.iter().find(|g| g.message_id == "3").unwrap();
    assert_eq!(
        g3.body,
        json!({"key": "3", "op": "upsert", "doc": {"sku": "C-3", "qty": 7}})
    );
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// The smtp kind against Mailpit (`LAZYWF_TEST_SMTP=127.0.0.1:34565`, compose profile `smtp`;
// Mailpit's HTTP API is on the next port, 34566). The server is configured to refuse every
// recipient but `lwf-smtp-*@lwf-smtp.test` (a 550), which the refusal test uses.

/// The server's `host:port`, when the suite has one.
fn smtp_endpoint() -> Option<(String, u16)> {
    let s = std::env::var("LAZYWF_TEST_SMTP").ok()?;
    let (host, port) = s.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

/// The messages Mailpit holds, read through its HTTP API.
struct Mailpit {
    host: String,
    port: u16,
    client: lazy_wire_fire::http::Client,
}

impl Mailpit {
    /// The SMTP endpoint and the API (Mailpit's HTTP port is the SMTP port plus one).
    fn new(host: &str, smtp_port: u16) -> Mailpit {
        let port = smtp_port + 1;
        let api = lazy_wire_fire::http::BaseUrl::parse(&format!("http://{host}:{port}")).unwrap();
        Mailpit {
            host: host.to_string(),
            port,
            client: lazy_wire_fire::http::Client::new(
                api,
                &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
        }
    }

    fn endpoint(&self) -> String {
        format!("smtp://{}:{}", self.host, self.port - 1)
    }

    async fn call(&self, method: &str, path: &str) -> Value {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: method.into(),
            target: path.into(),
            headers: Vec::new(),
            body: Vec::new(),
        };
        req.set_header("Host", format!("{}:{}", self.host, self.port));
        let resp = self
            .client
            .send(&req, Duration::from_secs(10))
            .await
            .unwrap_or_else(|e| panic!("{method} {path}: {e:#}"));
        assert!(
            (200..300).contains(&resp.status),
            "{method} {path}: {} {}",
            resp.status,
            resp.body_snippet()
        );
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// The summaries of the mails held now (newest first).
    async fn messages(&self) -> Vec<Value> {
        self.call("GET", "/api/v1/messages?limit=100")
            .await
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    /// One full message (the headers and the body as Mailpit parsed them).
    async fn get(&self, id: &str) -> Value {
        self.call("GET", &format!("/api/v1/message/{id}")).await
    }
}

/// Whether a mail summary is one of the test's own (its addresses carry the test's suffix).
fn own(suffix: &str, m: &Value) -> bool {
    m.get("To")
        .and_then(Value::as_array)
        .and_then(|t| t.first())
        .and_then(|t| t.get("Address"))
        .and_then(Value::as_str)
        .is_some_and(|a| a.contains(suffix))
}

/// The envelope template: one mail per key (the key's address as the recipient, a Cc of its
/// own), with the JSON envelope `{key, op, doc}` as the body. The suffix marks the test's mails
/// (the tests share the Mailpit).
fn smtp_envelope(suffix: &str) -> String {
    r#"{{#each docs}}From: lazywf@lwf-smtp.test
To: lwf-smtp-{{key}}"#
        .to_string()
        + suffix
        + r#"@lwf-smtp.test
Cc: lwf-smtp-cc-{{key}}"#
        + suffix
        + r#"@lwf-smtp.test
Subject: Order {{key}}
Content-Type: text/plain; charset=utf-8

{"key":{{json key}},"op":{{json op}},"doc":{{json doc}}}{{/each}}"#
}

/// The `smtp` kind as a mail producer: one mail per changed key; the Mailpit API shows the
/// sender, the recipients (To and Cc), the subject and the body exactly; a delete sends its mail
/// too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smtp_target() {
    let root = need_db!();
    let Some((host, port)) = smtp_endpoint() else {
        return eprintln!("LAZYWF_TEST_SMTP not set: skipping");
    };
    let mp = Mailpit::new(&host, port);
    let suffix = format!("-tg{}", std::process::id());
    let db = Db::fresh(&root, "smtp_target").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    // Mailpit runs plain on the test port: no STARTTLS (the target's `tls` says so).
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, vars) VALUES ('mail', 'smtp', ?, 'off', ?)",
        (mp.endpoint(), "{}"),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'mail', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            smtp_envelope(&suffix),
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    let got = wait_for(Duration::from_secs(30), "two mails", || async {
        let m = mp.messages().await;
        let mine: Vec<Value> = m.into_iter().filter(|m| own(&suffix, m)).collect();
        (mine.len() >= 2).then_some(mine)
    })
    .await;
    assert_eq!(got.len(), 2, "one mail per key: {got:?}");
    for (id, sku, qty) in [("1", "A-1", 3), ("2", "B-2", 1)] {
        let m = got
            .iter()
            .find(|m| m["To"][0]["Address"] == format!("lwf-smtp-{id}{suffix}@lwf-smtp.test"))
            .unwrap_or_else(|| panic!("the mail to lwf-smtp-{id}{suffix}@lwf-smtp.test: {got:?}"))
            .clone();
        assert_eq!(m["From"]["Address"], "lazywf@lwf-smtp.test", "{m:?}");
        assert_eq!(m["Subject"], format!("Order {id}"), "{m:?}");
        assert_eq!(
            m["Cc"][0]["Address"],
            format!("lwf-smtp-cc-{id}{suffix}@lwf-smtp.test"),
            "{m:?}"
        );
        let full = mp.get(m["ID"].as_str().unwrap()).await;
        // Mailpit stores the body with its final line ending: the rendered body plus a CRLF.
        assert_eq!(
            full["Text"].as_str().unwrap(),
            &format!(
                "{{\"key\":\"{id}\",\"op\":\"upsert\",\"doc\":{{\"sku\":\"{sku}\",\"qty\":{qty}}}}}\r\n"
            ),
            "{full:?}"
        );
    }

    db.exec("DELETE FROM orders WHERE id = 2").await;
    wait_for(Duration::from_secs(30), "the delete mail", || async {
        let m = mp
            .messages()
            .await
            .into_iter()
            .filter(|m| own(&suffix, m))
            .find(|m| m["To"][0]["Address"] == format!("lwf-smtp-2{suffix}@lwf-smtp.test"))?;
        let full = mp.get(m["ID"].as_str().unwrap()).await;
        (full_body(&full).as_deref() == Some(r#"{"key":"2","op":"delete","doc":{}}"#)).then_some(())
    })
    .await;
    inst.stop().await;
}

/// The body of a full Mailpit message (its stored final CRLF dropped).
fn full_body(m: &Value) -> Option<String> {
    m.get("Text")
        .and_then(Value::as_str)
        .map(|t| t.trim_end_matches("\r\n").to_string())
}

/// A recipient Mailpit is configured to refuse comes back as a 550. It is refused, struck and
/// dropped after its strikes, because the other key went through; the refused mail never arrives
/// and `last_error` says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smtp_refusal() {
    let root = need_db!();
    let Some((host, port)) = smtp_endpoint() else {
        return eprintln!("LAZYWF_TEST_SMTP not set: skipping");
    };
    let mp = Mailpit::new(&host, port);
    let suffix = format!("-rf{}", std::process::id());
    let db = Db::fresh(&root, "smtp_refusal").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    // Orders with the sku NOROUTE are addressed to a recipient the server refuses (a 550).
    let to = "lwf-smtp-{{key}}".to_string() + &suffix + "@lwf-smtp.test";
    let replaced = r#"{{#if (eq doc.sku "NOROUTE")}}refused@lwf-smtp.test{{else}}"#.to_string()
        + &to
        + "{{/if}}";
    let template = smtp_envelope(&suffix).replace(&to, &replaced);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, vars) VALUES ('mail', 'smtp', ?, 'off', ?)",
        (mp.endpoint(), "{}"),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders', 'mail', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            template,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    let start = binlog_end(&db).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'NOROUTE', 1)")
        .await;
    let err = wait_for(
        Duration::from_secs(60),
        "the refused key dropped",
        || async {
            let (f, p, _, err) = db.status("orders").await?;
            let err = err?;
            (err.contains("`2` refused") && (f, p) >= start).then_some(err)
        },
    )
    .await;
    assert!(
        err.contains("550") && err.contains("mailbox unavailable"),
        "{err}"
    );

    let got = mp
        .messages()
        .await
        .into_iter()
        .filter(|m| own(&suffix, m))
        .collect::<Vec<_>>();
    assert_eq!(got.len(), 1, "only the good key's mail arrives: {got:?}");
    assert_eq!(
        got[0]["To"][0]["Address"],
        format!("lwf-smtp-1{suffix}@lwf-smtp.test")
    );
    assert_eq!(
        full_body(&mp.get(got[0]["ID"].as_str().unwrap()).await).as_deref(),
        Some(r#"{"key":"1","op":"upsert","doc":{"sku":"A-1","qty":3}}"#)
    );
    inst.stop().await;
}
// ---------------------------------------------------------------------------------------------
// The kafka kind against a Redpanda dev container (`LAZYWF_TEST_KAFKA=127.0.0.1:34564`, compose
// profile `kafka`). The tests read the topics back through their own rskafka client: a record's
// key is the row key, its value the document, and its `op` header names the operation.

/// The rendered action: one record per key (record i takes key i), each with its document and
/// the `op` header.
const KAFKA_PUT: &str = r#"POST / HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{"value": {{json doc}}, "headers": {"op": {{json op}}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

/// The target's `expect`: the produce answered with an offset.
const KAFKA_EXPECT: &str = "JSON_EXTRACT(body, '$.offset') IS NOT NULL";

/// The broker's `host:port`, when the suite has one.
fn kafka_broker() -> Option<String> {
    std::env::var("LAZYWF_TEST_KAFKA").ok()
}

macro_rules! need_kafka {
    () => {
        match kafka_broker() {
            Some(b) => b,
            None => {
                eprintln!(
                    "LAZYWF_TEST_KAFKA not set: skipping (make servers with the kafka profile)"
                );
                return;
            }
        }
    };
}

/// One test at a time on the shared broker (the restart test stops it).
static KAFKA_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The shared Redpanda of the suite (the compose project `lazywf-test`).
const KAFKA_CONTAINER: &str = "lazywf-test-kafka-1";

/// One docker call on the host of the suite.
fn docker(args: &[&str]) {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("docker {args:?}: {e}"));
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// One shell command inside the broker's container, bounded by `timeout` (an `rpk` that waits
/// for records that never come must not hang the suite).
fn rpk_run(cmd: &str, secs: u64) -> std::io::Result<std::process::Output> {
    std::process::Command::new("docker")
        .args([
            "exec",
            KAFKA_CONTAINER,
            "sh",
            "-c",
            &format!("timeout {secs} {cmd}"),
        ])
        .output()
}

/// As `rpk_run`, panicking when the command fails. Its stdout.
fn rpk(cmd: &str) -> String {
    let out = rpk_run(cmd, 60).unwrap_or_else(|e| panic!("docker exec `sh -c {cmd}`: {e}"));
    assert!(
        out.status.success(),
        "rpk `{cmd}`: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// As `rpk`, but `None` instead of a panic, and bounded by `secs` inside the container: for asking
/// a broker that is still starting, where a slow or refusing `rpk` only means "not yet".
fn rpk_try(cmd: &str, secs: u64) -> Option<String> {
    let out = rpk_run(cmd, secs).ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).to_string())
}

/// The container's health as the compose health check sees it (`"starting"`, `"healthy"`,
/// `"unhealthy"`; empty while the container is down).
fn container_health() -> String {
    std::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.State.Health.Status}}",
            KAFKA_CONTAINER,
        ])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

/// Whether the text of `rpk cluster health` reports the cluster up with every partition having a
/// leader: `Healthy:  true`, and the counts `Leaderless partitions (0):  []` and
/// `Under-replicated partitions (0):  []`.
fn cluster_is_healthy(health: &str) -> bool {
    let count = |name: &str| -> Option<usize> {
        let (_, rest) = health
            .lines()
            .find(|l| l.starts_with(name))?
            .split_once('(')?;
        rest.split(')').next()?.parse().ok()
    };
    health.lines().any(|l| {
        l.starts_with("Healthy:") && l.split_once(':').is_some_and(|(_, v)| v.trim() == "true")
    }) && count("Leaderless partitions") == Some(0)
        && count("Under-replicated partitions") == Some(0)
}

/// The partition the broker's own default partitioner gives each of `keys`: the records are
/// produced with no partition chosen, so the client's partitioner decides, and read back. The
/// test compares this with `partition_of`, which is what makes "a key lands on the partition the
/// brokers' partitioner picks" a checked fact and not a claim about the hash.
///
/// `keys` are read as lines of `<key>\t<value>` and must be plain (no `%`, no `'`, no tab).
fn broker_partitions(broker: &str, topic: &str, keys: &[&str]) -> Vec<(String, i32)> {
    let input: String = keys.iter().map(|k| format!("{k}\\tv\\n")).collect();
    // `rpk produce` prints what it wrote; its own reading of the topic says the same, and a
    // short print (a lost record) is caught by the compare below.
    let produced = rpk(&format!(
        "printf '{input}' | rpk topic produce {topic} --format '%k\\t%v\\n' -X brokers={broker}"
    ));
    assert_eq!(
        produced
            .lines()
            .filter(|l| l.starts_with("Produced to partition"))
            .count(),
        keys.len(),
        "rpk did not produce every key"
    );
    let n = keys.len();
    rpk(&format!(
        "rpk topic consume {topic} -f '%p %k\\n' -o start -n {n} -X brokers={broker}"
    ))
    .lines()
    .filter_map(|line| {
        let (partition, key) = line.split_once(' ')?;
        Some((key.to_string(), partition.parse().ok()?))
    })
    .collect()
}

/// One record of a topic as the test reads it.
#[derive(Debug)]
struct Recd {
    partition: i32,
    offset: i64,
    key: Option<String>,
    value: Value,
    headers: std::collections::BTreeMap<String, String>,
}

/// The test's own rskafka client: it makes the topics and reads the records back.
///
/// A client is built for every call rather than held across them: rskafka caches the cluster
/// metadata for the life of a client (its `MetadataCache` has no expiry) and answers the leader
/// read of `partition_client` from that cache, retrying a `LeaderNotAvailable` out of it without
/// asking the broker again. A client built while the broker has no leader for a partition keeps
/// answering `LeaderNotAvailable` after the broker has healed, which is how the `queues` job lost
/// 120 s over a broker whose leaders were back within a second.
struct Kafka {
    broker: String,
    /// How long one rskafka request (the connect, the leader read, a fetch) waits out a transient
    /// broker state inside its client before the call is retried with a fresh client.
    retry: Duration,
}

impl Kafka {
    /// Connects, retrying until the broker's metadata answers (a fresh or a just-restarted
    /// broker takes a few seconds).
    async fn new(broker: &str) -> Kafka {
        Kafka::with_retry(broker, 5).await
    }

    /// As `new`, but each rskafka request waits out a transient broker state for `retry_secs`: a
    /// just-restarted broker keeps answering `LeaderNotAvailable` while it re-elects the leaders.
    /// Keep it short where the broker changes under the reader (a restart): the patience that
    /// matters is the loop in `partition_client`, and every retry of that loop asks the broker
    /// again.
    async fn with_retry(broker: &str, retry_secs: u64) -> Kafka {
        let k = Kafka {
            broker: broker.to_string(),
            retry: Duration::from_secs(retry_secs),
        };
        k.client().await; // as `new` always did: the broker answers, or this panics.
        k
    }

    /// A client for one call, connected (retrying while the broker is down). Its metadata is read
    /// now, which is the point of building one per call: see the struct.
    async fn client(&self) -> rskafka::client::Client {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            match rskafka::client::ClientBuilder::new(vec![self.broker.clone()])
                .client_id("lazywf-e2e")
                .backoff_config(rskafka::BackoffConfig {
                    deadline: Some(self.retry),
                    ..Default::default()
                })
                .build()
                .await
            {
                Ok(client) => return client,
                Err(e) if std::time::Instant::now() < deadline => {
                    eprintln!(
                        "the kafka broker at {} is not up yet ({e:#}): retrying",
                        self.broker
                    );
                    sleep_ms(500).await;
                }
                Err(e) => panic!(
                    "the kafka broker at {} did not answer in 60 s: {e:#}",
                    self.broker
                ),
            }
        }
    }

    /// Creates the topic of `n` partitions and waits until the metadata lists it.
    async fn topic(&self, name: &str, n: i32) {
        self.client()
            .await
            .controller_client()
            .unwrap()
            .create_topic(name, n, 1, 10_000)
            .await
            .unwrap_or_else(|e| panic!("creating the topic `{name}`: {e:#}"));
        self.partitions(name).await;
    }

    /// Deletes the topic (best effort: a rerun makes a fresh one, the broker is shared).
    async fn drop_topic(&self, name: &str) {
        self.client()
            .await
            .controller_client()
            .unwrap()
            .delete_topic(name, 10_000)
            .await
            .ok();
    }

    /// The topic's partitions, sorted (0..n).
    async fn partitions(&self, name: &str) -> Vec<i32> {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let topics = self
                .client()
                .await
                .list_topics()
                .await
                .unwrap_or_else(|e| panic!("the topic list: {e:#}"));
            if let Some(t) = topics.iter().find(|t| t.name == name) {
                let ps: Vec<i32> = t.partitions.iter().copied().collect();
                if !ps.is_empty() {
                    return ps;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the topic `{name}` did not appear in the metadata in 30 s"
            );
            sleep_ms(200).await;
        }
    }

    /// The partition client, waiting a leader election out: a just-restarted broker answers
    /// `LeaderNotAvailable` until it has re-elected every leader, which on a busy runner
    /// outlasts one client's own retry deadline (the queues job failed there once).
    async fn partition_client(
        &self,
        name: &str,
        pid: i32,
    ) -> rskafka::client::partition::PartitionClient {
        use rskafka::client::partition::UnknownTopicHandling;
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            // A fresh client per attempt: a held one answers the leader read from the metadata
            // it was built with, and no retry inside rskafka asks the broker again.
            match self
                .client()
                .await
                .partition_client(name, pid, UnknownTopicHandling::Retry)
                .await
            {
                Ok(pc) => return pc,
                Err(e) if std::time::Instant::now() < deadline => {
                    eprintln!("the leader of `{name}/{pid}` is not ready yet ({e:#}): retrying");
                    sleep_ms(250).await;
                }
                Err(e) => panic!("the leader of `{name}/{pid}`: {e:#}"),
            }
        }
    }

    /// Every record of the topic, partition by partition, in offset order.
    async fn records(&self, name: &str) -> Vec<Recd> {
        use rskafka::client::partition::OffsetAt;
        let pids = self.partitions(name).await;
        let mut out = Vec::new();
        for pid in pids {
            let pc = self.partition_client(name, pid).await;
            let mut offset = pc
                .get_offset(OffsetAt::Earliest)
                .await
                .unwrap_or_else(|e| panic!("the earliest offset of `{name}/{pid}`: {e:#}"));
            loop {
                let (recs, hw) = pc
                    .fetch_records(offset, 0..1_048_576, 500)
                    .await
                    .unwrap_or_else(|e| panic!("the records of `{name}/{pid}`: {e:#}"));
                if recs.is_empty() {
                    if hw <= offset {
                        break;
                    }
                    offset = hw;
                    continue;
                }
                for r in &recs {
                    out.push(Recd {
                        partition: pid,
                        offset: r.offset,
                        key: r.record.key.clone().map(|k| String::from_utf8(k).unwrap()),
                        value: serde_json::from_slice(r.record.value.clone().unwrap().as_slice())
                            .unwrap(),
                        headers: r
                            .record
                            .headers
                            .iter()
                            .map(|(k, v)| (k.clone(), String::from_utf8(v.clone()).unwrap()))
                            .collect(),
                    });
                }
                offset = recs.last().unwrap().offset + 1;
            }
        }
        out
    }
}

/// The shared broker's container, kept up when a test ends between its stop and its start.
struct BrokerRestart {
    down: bool,
}

impl BrokerRestart {
    fn new() -> Self {
        Self { down: false }
    }
    /// Stops the container (graceful, 5 s).
    fn stop(&mut self) {
        docker(&["stop", "-t", "5", KAFKA_CONTAINER]);
        self.down = true;
    }
    /// Starts it again: `docker start` returns when the process is launched, so the reader waits
    /// for `healthy` before it asks the broker anything.
    fn start(&mut self) {
        docker(&["start", KAFKA_CONTAINER]);
        self.down = false;
    }
    /// Waits for the started broker to report healthy on both counts: the container's own health
    /// check (compose asks `/v1/status/ready` every 2 s) and the broker's cluster health, which
    /// counts the leaderless partitions. Reading a partition while it has no leader is what the
    /// reader used to wait out for minutes.
    async fn healthy(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let health = rpk_try("rpk cluster health", 10);
            if container_health() == "healthy" && health.as_deref().is_some_and(cluster_is_healthy)
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the broker did not report healthy in 120 s: {}",
                health
                    .as_deref()
                    .unwrap_or("`rpk cluster health` did not run")
            );
            sleep_ms(250).await;
        }
    }
}

impl Drop for BrokerRestart {
    fn drop(&mut self) {
        if self.down {
            let _ = std::process::Command::new("docker")
                .args(["start", KAFKA_CONTAINER])
                .status();
        }
    }
}

/// Every changed key arrives once, on the partition of its key, with its document and the `op`
/// header; an update appends a record, a delete appends one with an empty document.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_every_key_arrives_once() {
    let root = need_db!();
    let broker = need_kafka!();
    let _lock = KAFKA_LOCK.lock().await;
    let k = Kafka::new(&broker).await;
    let topic = format!("lwf-kafka-every-{}", std::process::id());
    k.topic(&topic, 4).await;
    let db = Db::fresh(&root, "kafka_every").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    db.target(
        "events",
        "kafka",
        &format!("kafka://{broker}/{topic}"),
        Some(KAFKA_EXPECT),
        "{}",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'events', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            KAFKA_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec(
        "INSERT INTO orders VALUES (1,'A-1',3),(2,'B-2',1),(3,'C-3',5),(4,'D-4',2),(5,'E-5',9)",
    )
    .await;
    wait_for(Duration::from_secs(30), "the five records", || async {
        (k.records(&topic).await.len() == 5).then_some(())
    })
    .await;
    inst.quiet().await;
    let pids = k.partitions(&topic).await;
    let recs = k.records(&topic).await;
    assert_eq!(recs.len(), 5, "one record per key: {recs:?}");
    for (id, sku, qty) in [
        (1, "A-1", 3),
        (2, "B-2", 1),
        (3, "C-3", 5),
        (4, "D-4", 2),
        (5, "E-5", 9),
    ] {
        let key = id.to_string();
        let r = recs
            .iter()
            .find(|r| r.key.as_deref() == Some(key.as_str()))
            .unwrap();
        assert_eq!(
            r.partition,
            lazy_wire_fire::targets::kafka::partition_of(&key, &pids),
            "{key}: the partition is the murmur2 of the key mod the count"
        );
        assert_eq!(
            r.value,
            json!({"sku": sku, "qty": qty}),
            "{key}: the document"
        );
        assert_eq!(
            r.headers.get("op"),
            Some(&"upsert".to_string()),
            "{key}: the op header"
        );
    }

    // The partitions above are the ones lazywf chose. Here is the broker agreeing with them:
    // the same keys, produced by `rpk` with no partition chosen, land on the partitions
    // `partition_of` picks — so a key lazywf writes is where any other client's default
    // partitioner would put it.
    let ptopic = format!("lwf-kafka-parts-{}", std::process::id());
    k.topic(&ptopic, 4).await;
    let keys = ["1", "2", "3", "4", "5"];
    let pids = k.partitions(&ptopic).await;
    let mut want: Vec<(String, i32)> = keys
        .iter()
        .map(|k| {
            (
                k.to_string(),
                lazy_wire_fire::targets::kafka::partition_of(k, &pids),
            )
        })
        .collect();
    want.sort();
    // `rpk` reads the partitions at once, so the pairs come back in no particular order.
    let mut got = broker_partitions(&broker, &ptopic, &keys);
    got.sort();
    assert_eq!(
        got, want,
        "the broker's own partitioner chooses the same partitions"
    );
    k.drop_topic(&ptopic).await;

    db.exec("UPDATE orders SET qty = 4 WHERE id = 1").await;
    db.exec("UPDATE orders SET sku = 'C-3x', qty = 6 WHERE id = 3")
        .await;
    wait_for(Duration::from_secs(30), "the seven records", || async {
        (k.records(&topic).await.len() == 7).then_some(())
    })
    .await;
    inst.quiet().await;
    let recs = k.records(&topic).await;
    let by_key = |key: &str| -> Vec<&Recd> {
        recs.iter()
            .filter(|r| r.key.as_deref() == Some(key))
            .collect()
    };
    assert_eq!(by_key("1").len(), 2, "the update appends: {recs:?}");
    assert_eq!(
        by_key("1").last().unwrap().value,
        json!({"sku": "A-1", "qty": 4})
    );
    assert_eq!(by_key("3").len(), 2);
    assert_eq!(
        by_key("3").last().unwrap().value,
        json!({"sku": "C-3x", "qty": 6})
    );
    for key in ["2", "4", "5"] {
        assert_eq!(by_key(key).len(), 1, "{key} was not changed");
    }

    db.exec("DELETE FROM orders WHERE id = 2").await;
    wait_for(Duration::from_secs(30), "the eight records", || async {
        (k.records(&topic).await.len() == 8).then_some(())
    })
    .await;
    inst.quiet().await;
    let recs = k.records(&topic).await;
    let key2: Vec<&Recd> = recs
        .iter()
        .filter(|r| r.key.as_deref() == Some("2"))
        .collect();
    assert_eq!(key2.len(), 2, "the delete appends a record: {key2:?}");
    assert_eq!(key2[0].headers.get("op"), Some(&"upsert".to_string()));
    assert_eq!(key2[1].value, json!({}), "the delete's document is empty");
    assert_eq!(key2[1].headers.get("op"), Some(&"delete".to_string()));
    inst.stop().await;
    k.drop_topic(&topic).await;
}

/// A key fired one at a time keeps its order on its partition: one record per fire, in the
/// order of the fires, with the document of each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_ordering() {
    let root = need_db!();
    let broker = need_kafka!();
    let _lock = KAFKA_LOCK.lock().await;
    let k = Kafka::new(&broker).await;
    let topic = format!("lwf-kafka-order-{}", std::process::id());
    k.topic(&topic, 1).await;
    let db = Db::fresh(&root, "kafka_order").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    db.target(
        "events",
        "kafka",
        &format!("kafka://{broker}/{topic}"),
        Some(KAFKA_EXPECT),
        "{}",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'events', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty FROM orders WHERE id IN (:keys)",
            KAFKA_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3)").await;
    wait_for(Duration::from_secs(30), "the first record", || async {
        (!k.records(&topic).await.is_empty()).then_some(())
    })
    .await;
    inst.quiet().await;

    for (i, qty) in [4, 5].into_iter().enumerate() {
        db.exec(&format!("UPDATE orders SET qty = {qty} WHERE id = 1"))
            .await;
        let want = i + 2;
        wait_for(
            Duration::from_secs(30),
            &format!("the record {want}"),
            || async { (k.records(&topic).await.len() == want).then_some(()) },
        )
        .await;
        inst.quiet().await;
    }

    let recs = k.records(&topic).await;
    assert_eq!(recs.len(), 3, "one record per fire: {recs:?}");
    let mut offsets = Vec::new();
    for (i, r) in recs.iter().enumerate() {
        let qty = [3, 4, 5][i];
        assert_eq!(r.partition, 0, "{i}");
        assert_eq!(r.key, Some("1".to_string()), "{i}");
        assert_eq!(
            r.value,
            json!({"sku": "A-1", "qty": qty}),
            "{i}: the document of its fire"
        );
        offsets.push(r.offset);
    }
    assert!(
        offsets.windows(2).all(|w| w[0] < w[1]),
        "the offsets grow: {offsets:?}"
    );
    inst.stop().await;
    k.drop_topic(&topic).await;
}

/// A record the broker takes no (the topic's `max.message.bytes`) is refused alone: the batch it
/// travelled in is split until the record stands by itself, so the other keys of the flush
/// arrive, only the refused one is struck, the position moves on and `last_error` says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_refusal() {
    let root = need_db!();
    let broker = need_kafka!();
    let _lock = KAFKA_LOCK.lock().await;
    let k = Kafka::new(&broker).await;
    let topic = format!("lwf-kafka-refusal-{}", std::process::id());
    k.topic(&topic, 1).await;
    // The topic takes 50000 bytes per record set (the target itself assumes the broker's
    // default 1 MiB, which is why the batch is produced at all): a record of 190000 is refused.
    docker(&[
        "exec",
        KAFKA_CONTAINER,
        "rpk",
        "topic",
        "alter-config",
        &topic,
        "--set",
        "max.message.bytes=50000",
        "--no-confirm",
        "-X",
        &format!("brokers={broker}"),
    ]);
    let db = Db::fresh(&root, "kafka_refusal").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, payload MEDIUMTEXT)")
        .await;
    tables(&db).await;
    db.target(
        "events",
        "kafka",
        &format!("kafka://{broker}/{topic}"),
        Some(KAFKA_EXPECT),
        &json!({"compression": "none"}).to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'events', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, payload FROM orders WHERE id IN (:keys)",
            KAFKA_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    let start = binlog_end(&db).await;

    // One flush, one batch: the two small records travel with the too-large one.
    db.exec_params(
        "INSERT INTO orders VALUES (1, 'small'), (2, ?), (3, 'also small')",
        ("A".repeat(190_000),),
    )
    .await;
    let err = wait_for(
        Duration::from_secs(60),
        "the too-large record refused and the position moved on",
        || async {
            let (f, p, _, err) = db.status("orders").await?;
            let err = err?;
            (err.contains("`2` refused") && (f, p) >= start).then_some(err)
        },
    )
    .await;
    assert!(
        err.contains("MessageTooLarge") || err.contains("RecordListTooLarge"),
        "{err}"
    );

    // The batch was split until record 2 stood alone: its neighbours arrived, it did not.
    let recs = k.records(&topic).await;
    let keys: Vec<&str> = recs.iter().filter_map(|r| r.key.as_deref()).collect();
    assert_eq!(
        keys,
        ["1", "3"],
        "only the two small records arrive: {recs:?}"
    );
    assert_eq!(recs[0].value, json!({"payload": "small"}));
    assert_eq!(recs[1].value, json!({"payload": "also small"}));
    inst.stop().await;
    k.drop_topic(&topic).await;
}

/// The broker goes down (a restart, seen from lazywf): a key fired while it is down is not
/// lost; it arrives after the heal, with the document of its fire (at-least-once).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_restart() {
    let root = need_db!();
    let broker = need_kafka!();
    let _lock = KAFKA_LOCK.lock().await;
    let mut restart = BrokerRestart::new();
    let k = Kafka::new(&broker).await;
    let topic = format!("lwf-kafka-restart-{}", std::process::id());
    k.topic(&topic, 1).await;
    let db = Db::fresh(&root, "kafka_restart").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    db.target(
        "events",
        "kafka",
        &format!("kafka://{broker}/{topic}"),
        Some(KAFKA_EXPECT),
        "{}",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'events', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty FROM orders WHERE id IN (:keys)",
            KAFKA_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3)").await;
    wait_for(Duration::from_secs(30), "the first record", || async {
        (!k.records(&topic).await.is_empty()).then_some(())
    })
    .await;
    inst.quiet().await;

    restart.stop();
    db.exec("INSERT INTO orders VALUES (2, 'B-2', 1)").await;
    // The engine holds key 2: its produce cannot reach the broker.
    wait_for(
        Duration::from_secs(30),
        "key 2 pending while the broker is down",
        || async {
            let s = inst.health.status();
            s["rules"]["orders"]["pending_keys"]
                .as_u64()
                .is_some_and(|n| n >= 1)
                .then_some(())
        },
    )
    .await;

    restart.start();
    // `docker start` only launches the process: wait for the broker's health check and for its
    // leaders before anyone reads, so the reader below meets a healed broker and not a
    // half-started one whose "no leader" answer it would then repeat from its own metadata.
    restart.healthy().await;
    // The reader after the heal: a client of its own per call, with short rskafka retries, so
    // every retry of `partition_client` asks the broker again.
    let k = Kafka::with_retry(&broker, 2).await;
    wait_for(Duration::from_secs(90), "key 2 after the heal", || async {
        k.records(&topic)
            .await
            .iter()
            .any(|r| r.key.as_deref() == Some("2"))
            .then_some(())
    })
    .await;
    inst.quiet().await;
    let recs = k.records(&topic).await;
    let by_key = |key: &str| -> Vec<&Recd> {
        recs.iter()
            .filter(|r| r.key.as_deref() == Some(key))
            .collect()
    };
    assert_eq!(
        by_key("1").len(),
        1,
        "key 1 fired before the cut, once: {recs:?}"
    );
    assert!(
        !by_key("2").is_empty(),
        "key 2 arrived after the heal: {recs:?}"
    );
    assert_eq!(
        by_key("2").last().unwrap().value,
        json!({"sku": "B-2", "qty": 1})
    );
    for r in &by_key("2") {
        assert_eq!(r.partition, 0);
    }
    inst.stop().await;
    k.drop_topic(&topic).await;
}

// ---------------------------------------------------------------------------------------------
// The servicebus kind against RabbitMQ 4 speaking AMQP 1.0 (`LAZYWF_TEST_SERVICEBUS=
// 127.0.0.1:34610`, the compose `amqp` profile's RabbitMQ: its 5672 answers both 0-9-1 and 1.0
// and picks by itself, and 34610 is that port published for this kind). RabbitMQ has no `$cbs`
// node, so the SAS path cannot run here: the target connects with SASL PLAIN (`vars.username`,
// `vars.password`), which is what a broker that is not Service Bus takes. The queue is declared
// and read over AMQP 0-9-1 with lapin, as the `amqp` tests do: what arrives is the target's own
// message seen from the other protocol of the same broker.

/// The rendered action: one message per key (message i takes key i), each with its document.
const SB_PUT: &str = r#"POST / HTTP/1.1
Content-Type: application/json

{"messages": [{{#each docs}}{"value": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

/// The target's `expect`: the send answered with the entity the message went to.
const SB_EXPECT: &str = "JSON_EXTRACT(body, '$.entity') IS NOT NULL";

/// The broker's `host:port`, when the suite has one.
fn servicebus_broker() -> Option<String> {
    std::env::var("LAZYWF_TEST_SERVICEBUS").ok()
}

macro_rules! need_servicebus {
    () => {
        match servicebus_broker() {
            Some(b) => b,
            None => {
                eprintln!(
                    "LAZYWF_TEST_SERVICEBUS not set: skipping (make servers with the amqp profile)"
                );
                return;
            }
        }
    };
}

/// One test at a time on the shared broker (the restart test stops it).
static SB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The shared RabbitMQ of the suite (the compose project `lazywf-test`).
const SB_CONTAINER: &str = "lazywf-test-rabbitmq-1";

/// The password of the broker's test user, by reference as the kind demands it.
const SB_PW_VAR: &str = "LWF_TEST_SB_PW";

/// The container's health as the compose health check sees it (`rabbitmq-diagnostics -q ping`).
fn sb_container_health() -> String {
    std::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.State.Health.Status}}",
            SB_CONTAINER,
        ])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

/// The shared RabbitMQ container, kept up when a test ends between its stop and its start.
struct SbRestart {
    down: bool,
}

impl SbRestart {
    fn new() -> Self {
        Self { down: false }
    }
    /// Stops the container (graceful: what the broker holds is meant to survive).
    fn stop(&mut self) {
        docker(&["stop", "-t", "10", SB_CONTAINER]);
        self.down = true;
    }
    fn start(&mut self) {
        docker(&["start", SB_CONTAINER]);
        self.down = false;
    }
    /// Waits for the started broker to answer its own health check.
    async fn healthy(&self) {
        wait_for(
            Duration::from_secs(120),
            "the broker healthy again",
            || async { (sb_container_health() == "healthy").then_some(()) },
        )
        .await;
    }
}

impl Drop for SbRestart {
    fn drop(&mut self) {
        if self.down {
            let _ = std::process::Command::new("docker")
                .args(["start", SB_CONTAINER])
                .status();
        }
    }
}

/// One message of a queue as the test reads it over AMQP 0-9-1.
#[derive(Debug, Clone)]
struct SbMsg {
    message_id: String,
    content_type: String,
    durable: bool,
    body: Value,
}

/// The test's view of the broker: declares its queue and reads it back. One connection per call,
/// so a broker that went away and came back is met with a fresh one.
struct Sbus {
    url: String,
}

impl Sbus {
    fn new(broker: &str) -> Sbus {
        Sbus {
            url: format!("amqp://lwftest:lwftest@{broker}/%2f"),
        }
    }

    async fn channel(&self) -> Option<lapin::Channel> {
        let conn = lapin::Connection::connect(&self.url, lapin::ConnectionProperties::default())
            .await
            .ok()?;
        conn.create_channel().await.ok()
    }

    /// (Re)creates the durable queue, empty: a rerun does not see an earlier run's messages.
    async fn declare(&self, queue: &str) {
        use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
        let conn = lapin::Connection::connect(&self.url, lapin::ConnectionProperties::default())
            .await
            .unwrap();
        let chan = conn.create_channel().await.unwrap();
        chan.queue_delete(queue.into(), QueueDeleteOptions::default())
            .await
            .ok();
        chan.queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
    }

    /// How many messages the queue holds now (a passive declare); `None` while the broker is
    /// down or the queue is gone.
    async fn depth(&self, queue: &str) -> Option<u32> {
        use lapin::options::QueueDeclareOptions;
        let chan = self.channel().await?;
        chan.queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .ok()
        .map(|q| q.message_count())
    }

    /// Every message waiting on the queue now, taken (acked) from it.
    async fn take(&self, queue: &str) -> Vec<SbMsg> {
        use lapin::options::BasicGetOptions;
        let Some(chan) = self.channel().await else {
            return Vec::new();
        };
        let mut out = Vec::new();
        loop {
            let Ok(Some(m)) = chan
                .basic_get(queue.into(), BasicGetOptions { no_ack: true })
                .await
            else {
                return out;
            };
            let d = m.delivery;
            let p = &d.properties;
            out.push(SbMsg {
                message_id: p
                    .message_id()
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                content_type: p
                    .content_type()
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                durable: *p.delivery_mode() == Some(2),
                body: serde_json::from_slice(&d.data).unwrap_or(Value::Null),
            });
        }
    }
}

/// Reads the queue into `seen` (a `take` acks, so what was read must be kept) and returns
/// everything seen so far.
async fn sb_read(sb: &Sbus, queue: &str, seen: &std::sync::Mutex<Vec<SbMsg>>) -> Vec<SbMsg> {
    let mut new = sb.take(queue).await;
    let mut all = seen.lock().unwrap();
    if !new.is_empty() {
        all.append(&mut new);
    }
    all.clone()
}

/// Waits until `n` messages have arrived on the queue and returns them all.
async fn sb_arrived(sb: &Sbus, queue: &str, n: usize) -> Vec<SbMsg> {
    let seen = std::sync::Mutex::new(Vec::new());
    wait_for(
        Duration::from_secs(60),
        &format!("{n} servicebus messages on {queue}"),
        || async {
            let all = sb_read(sb, queue, &seen).await;
            (all.len() >= n).then_some(all)
        },
    )
    .await
}

/// The target row: one endpoint, plain TLS (a local broker may be plain), SASL PLAIN as the
/// kind's own vars demand them (`vars.password` by reference only).
async fn sb_target(db: &Db, broker: &str, queue: &str, extra_vars: Value) {
    std::env::set_var(SB_PW_VAR, "lwftest");
    let mut vars = json!({"username": "lwftest", "password": format!("env:{SB_PW_VAR}")});
    if let Some(extra) = extra_vars.as_object() {
        vars.as_object_mut().unwrap().extend(extra.clone());
    }
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, expect, vars, timeout_ms) \
         VALUES ('sb', 'servicebus', ?, 'off', ?, ?, 8000)",
        (
            format!("sb://{broker}/queues/{queue}"),
            SB_EXPECT,
            vars.to_string(),
        ),
    )
    .await;
}

/// The rule that turns every changed order into one message of the action.
async fn sb_rule(db: &Db, fire_per: &str) {
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'sb', ?, ?, ?, 100)",
        (
            fire_per,
            "SELECT id AS _key, sku, qty FROM orders WHERE id IN (:keys)",
            SB_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
}

/// Every changed key arrives once, as its own message, with its document, the JSON content type
/// and a durable header; `vars.dedup` puts the key-and-content id on the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn servicebus_every_key_arrives_once() {
    let root = need_db!();
    let broker = need_servicebus!();
    let _lock = SB_LOCK.lock().await;
    let sb = Sbus::new(&broker);
    let queue = format!("lwf-sb-every-{}", std::process::id());
    sb.declare(&queue).await;
    let db = Db::fresh(&root, "sb_every").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    sb_target(&db, &broker, &queue, json!({"dedup": true})).await;
    sb_rule(&db, "batch").await;
    let inst = Instance::ready(config(&db, &[])).await;
    let start = binlog_end(&db).await;

    db.exec(
        "INSERT INTO orders VALUES (1,'A-1',3),(2,'B-2',1),(3,'C-3',5),(4,'D-4',2),(5,'E-5',9)",
    )
    .await;
    let got = sb_arrived(&sb, &queue, 5).await;
    assert_eq!(got.len(), 5, "nothing arrives twice: {got:?}");
    let docs = [
        ("1", "A-1", 3),
        ("2", "B-2", 1),
        ("3", "C-3", 5),
        ("4", "D-4", 2),
        ("5", "E-5", 9),
    ];
    let of = |key: &str| -> Vec<SbMsg> {
        got.iter()
            .filter(|g| {
                g.body.get("sku").and_then(Value::as_str)
                    == Some(docs.iter().find(|d| d.0 == key).unwrap().1)
            })
            .cloned()
            .collect()
    };
    for (key, sku, qty) in docs {
        let m = &of(key);
        assert_eq!(m.len(), 1, "key {key} arrives once: {got:?}");
        assert_eq!(m[0].body, json!({"sku": sku, "qty": qty}), "key {key}");
        assert_eq!(m[0].content_type, "application/json", "key {key}");
        assert!(m[0].durable, "key {key}: the message is durable");
    }
    // `vars.dedup`: the id is the hex sha256 of key + NUL + value, so it is 64 hex characters
    // and two keys holding the same document still differ (the id is not only the content).
    for m in &got {
        // First of all: the document has to be the body a consumer of the entity reads. The
        // rest of the test is worthless if it is not, and a `Data` section that arrived as
        // something else would otherwise only show up as a key "not arriving".
        assert!(
            m.body.get("sku").and_then(Value::as_str).is_some(),
            "the document is the body of the message: {m:?}"
        );
        assert_eq!(m.message_id.len(), 64, "dedup id: {m:?}");
        assert!(
            m.message_id
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "dedup id is hex: {m:?}"
        );
    }
    assert_eq!(
        got.iter()
            .map(|m| m.message_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        5,
        "the ids are distinct per key: {got:?}"
    );
    inst.quiet().await;
    // The status row appears when the engine next saves its position, which is its own cadence
    // — so it is waited for, not assumed. And nothing failed on the way.
    let (f, p, _, err) = wait_for(
        Duration::from_secs(30),
        "the status row of the rule",
        || async { db.status("orders").await },
    )
    .await;
    let shown = format!("{f}:{p} {}", err.as_deref().unwrap_or("no error"));
    assert!(
        (f, p) > start,
        "the position has moved past the insert: {shown}"
    );
    assert!(err.is_none(), "nothing failed: {shown}");
    inst.stop().await;
}

/// One message the entity will not take is refused alone: its own strike, while its neighbours
/// of the same action go through. Here the refusal is the target's own size check — a value over
/// the entity's `vars.max_message_size` never goes on the wire, because the broker would only
/// refuse it with a this-message condition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn servicebus_refusal() {
    let root = need_db!();
    let broker = need_servicebus!();
    let _lock = SB_LOCK.lock().await;
    let sb = Sbus::new(&broker);
    let queue = format!("lwf-sb-refusal-{}", std::process::id());
    sb.declare(&queue).await;
    let db = Db::fresh(&root, "sb_refusal").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), payload TEXT)")
        .await;
    tables(&db).await;
    // The entity takes 200 bytes a message: keys 1 and 3 fit, key 2 does not.
    sb_target(&db, &broker, &queue, json!({"max_message_size": 200})).await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'sb', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, payload FROM orders WHERE id IN (:keys)",
            SB_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    let start = binlog_end(&db).await;

    db.exec_params(
        "INSERT INTO orders VALUES (1, 'A-1', 'small'), (2, 'B-2', ?), (3, 'C-3', 'also small')",
        ("B".repeat(400),),
    )
    .await;
    let err = wait_for(
        Duration::from_secs(60),
        "the over-size message refused and the position moved on",
        || async {
            let (f, p, _, err) = db.status("orders").await?;
            let err = err?;
            (err.contains("`2` refused") && (f, p) >= start).then_some(err)
        },
    )
    .await;
    assert!(err.contains("maximum"), "{err}");

    let got = sb_arrived(&sb, &queue, 2).await;
    let skus: Vec<Option<&str>> = got
        .iter()
        .map(|g| g.body.get("sku").and_then(Value::as_str))
        .collect();
    assert_eq!(
        skus,
        [Some("A-1"), Some("C-3")],
        "the two small messages arrive: {got:?}"
    );
    inst.stop().await;
}

/// The broker goes down and comes back: the messages already in the entity are still there
/// (durable queue, durable messages), and a key fired while the broker was down is not lost — it
/// arrives after the heal, once, with the document of its fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn servicebus_restart() {
    let root = need_db!();
    let broker = need_servicebus!();
    let _lock = SB_LOCK.lock().await;
    let mut restart = SbRestart::new();
    let sb = Sbus::new(&broker);
    let queue = format!("lwf-sb-restart-{}", std::process::id());
    sb.declare(&queue).await;
    let db = Db::fresh(&root, "sb_restart").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    sb_target(&db, &broker, &queue, json!({})).await;
    sb_rule(&db, "batch").await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1,'A-1',3),(2,'B-2',1),(3,'C-3',5)")
        .await;
    wait_for(
        Duration::from_secs(60),
        "the first three in the entity",
        || async { (sb.depth(&queue).await == Some(3)).then_some(()) },
    )
    .await;
    inst.quiet().await;

    restart.stop();
    db.exec("INSERT INTO orders VALUES (4,'D-4',2),(5,'E-5',9)")
        .await;
    // The engine holds keys 4 and 5: their send cannot reach the broker.
    wait_for(
        Duration::from_secs(30),
        "keys 4 and 5 pending while the broker is down",
        || async {
            let s = inst.health.status();
            s["rules"]["orders"]["pending_keys"]
                .as_u64()
                .is_some_and(|n| n >= 1)
                .then_some(())
        },
    )
    .await;

    restart.start();
    restart.healthy().await;
    let got = sb_arrived(&sb, &queue, 5).await;
    inst.quiet().await;
    for m in &got {
        assert!(
            m.body.get("sku").and_then(Value::as_str).is_some(),
            "the document is the body of the message: {m:?}"
        );
    }
    let by_sku = |sku: &str| -> usize {
        got.iter()
            .filter(|g| g.body.get("sku").and_then(Value::as_str) == Some(sku))
            .count()
    };
    for sku in ["A-1", "B-2", "C-3"] {
        assert_eq!(by_sku(sku), 1, "{sku} survived the restart once: {got:?}");
    }
    for sku in ["D-4", "E-5"] {
        assert_eq!(by_sku(sku), 1, "{sku} arrived after the heal: {got:?}");
    }
    assert_eq!(
        got.iter()
            .filter(|g| g.body.get("qty").and_then(Value::as_i64) == Some(1))
            .count(),
        1,
        "key 2's document as it was when it fired: {got:?}"
    );
    inst.stop().await;
}

/// Commits the commit fence has written for lazywf (0 before the first one; the table is made by
/// the first flush).
async fn fence_count(db: &Db) -> u64 {
    let mut c = db.conn().await;
    let r: Result<Option<u64>, _> = c
        .exec_first(
            "SELECT n FROM lazy_wire_fire_fence WHERE id = ?",
            ("lazywf",),
        )
        .await;
    c.disconnect().await.ok();
    r.ok().flatten().unwrap_or(0)
}

/// Binlog events of `file` from `from`: (position, event type, info).
async fn binlog_events(db: &Db, file: &str, from: u64) -> Vec<(u64, String, String)> {
    let mut c = db.conn().await;
    let rows: Vec<(String, u64, String, u32, u64, String)> = c
        .query(format!("SHOW BINLOG EVENTS IN '{file}' FROM {from}"))
        .await
        .unwrap();
    c.disconnect().await.ok();
    rows.into_iter().map(|r| (r.1, r.2, r.5)).collect()
}

/// The Table_map info of `table` in `schema` (`table_id: 12 (schema.table)`).
fn table_info(schema: &str, table: &str) -> String {
    format!("({schema}.{table})")
}

/// A binlog event of a transaction the engine reads as foreign for the instance in `schema`: a
/// table but its own status and fence tables (the same table names in another schema are another
/// instance's, and foreign), or a statement other than the `BEGIN` of an autocommit transaction.
fn foreign(event: &(u64, String, String), schema: &str) -> bool {
    let (_, kind, info) = event;
    match kind.as_str() {
        "Table_map" => {
            !(info.contains(&table_info(schema, "lazy_wire_fire_status"))
                || info.contains(&table_info(schema, "lazy_wire_fire_fence")))
        }
        "Query" => !info.starts_with("BEGIN"),
        _ => false,
    }
}

/// MySQL without GTIDs (the commit fence): one write per flush boundary, never per row, and
/// the fence's own binlog transactions move no rule position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_fence_writes_per_flush_and_moves_no_position() {
    let root = need_db!();
    let db = Db::fresh(&root, "fence").await;
    // The commit fence is for MySQL without GTIDs; GTID servers and MariaDB use their own paths.
    let mut c = db.conn().await;
    let mode: Option<String> = c
        .query_first("SELECT @@GLOBAL.gtid_mode")
        .await
        .unwrap_or(None);
    c.disconnect().await.ok();
    if mode.as_deref() != Some("OFF") {
        eprintln!(
            "gtid_mode is not OFF (MySQL without GTIDs only): skipping the commit fence test"
        );
        return;
    }
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, v INT)")
        .await;
    db.exec("CREATE TABLE other (n INT)").await;
    tables(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('i', 'hook', 'POST /i HTTP/1.1\n\n{{join keys \",\"}}', 50)")
        .await;
    db.trigger("i", "items", Some("{{new.id}}"), None).await;
    let start = binlog_end(&db).await;
    let inst = Instance::ready(config(&db, &[])).await;

    // One change at a time: each flush is a boundary of its own, so each writes the fence once.
    let n0 = fence_count(&db).await;
    for i in 1..=5 {
        db.exec(&format!("INSERT INTO items VALUES ({i}, 0)")).await;
        wait_for(Duration::from_secs(15), "a flush", || async {
            (mock.requests().len() >= i).then_some(())
        })
        .await;
        inst.quiet().await;
    }
    let n1 = fence_count(&db).await;
    assert_eq!(n1 - n0, 5, "one fence write per flush");

    // The rule's position may pass this instance's fence write only over a foreign transaction. The
    // suite runs other instances on this server at the same time (in other schemas, with the same
    // table names): their transactions are foreign to this one, and the engine moves past them.
    // So the check is on the binlog, for this schema's tables only: a position past one of our
    // fence writes with nothing foreign in between would mean the fence moved it.
    let (file, p1, _, _) = db.status("i").await.expect("status row");
    assert_eq!(file, start.0, "the test window stays in one binlog file");
    let events = binlog_events(&db, &start.0, start.1).await;
    let fence_table = table_info(&db.schema, "lazy_wire_fire_fence");
    let fences: Vec<u64> = events
        .iter()
        .filter(|e| e.1 == "Table_map" && e.2.contains(&fence_table))
        .map(|e| e.0)
        .collect();
    assert!(
        fences.len() >= 5,
        "the fence writes are in the binlog: {fences:?}"
    );
    if let Some(last) = fences.iter().copied().filter(|&f| f < p1).max() {
        if !events
            .iter()
            .any(|e| e.0 > last && e.0 < p1 && foreign(e, &db.schema))
        {
            // Diagnostic (kept for the next failure): every event from the fence write to the
            // position, and the fence writes the binlog holds (all schemas).
            eprintln!("fence writes in the binlog (table_map positions): {fences:?}");
            eprintln!("events from {last} to {p1} (position, type, info):");
            for e in events.iter().filter(|e| e.0 >= last && e.0 <= p1) {
                eprintln!("  {} {} {}", e.0, e.1, e.2);
            }
            panic!(
                "the position {p1} passed the fence write at {last} with no foreign transaction between"
            );
        }
    }

    // A foreign change does move it past the fence writes.
    db.exec("INSERT INTO other VALUES (1)").await;
    let end = binlog_end(&db).await;
    wait_for(
        Duration::from_secs(15),
        "the position to pass a foreign change",
        || async {
            let (f, p, _, _) = db.status("i").await?;
            (f == end.0 && p >= end.1).then_some(())
        },
    )
    .await;

    // A burst in one transaction is one flush and one write, not one per row.
    let before = fence_count(&db).await;
    let rows: Vec<String> = (100..150).map(|i| format!("({i}, 0)")).collect();
    db.exec(&format!("INSERT INTO items VALUES {}", rows.join(",")))
        .await;
    wait_for(Duration::from_secs(15), "the burst", || async {
        let all: String = mock.requests().iter().map(|r| r.body.clone()).collect();
        (100..150)
            .all(|k| all.contains(&k.to_string()))
            .then_some(())
    })
    .await;
    inst.quiet().await;
    let burst = fence_count(&db).await - before;
    assert!(
        (1..50).contains(&burst),
        "{burst} fence writes for a burst of 50 rows"
    );
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// The rabbitmq-stream kind against RabbitMQ's stream protocol (`LAZYWF_TEST_RABBITMQ_STREAM`,
// e.g. 127.0.0.1:34572; the `amqp` compose profile, which enables the stream plugin on the
// broker). Streams are managed through the official client's environment and read back with an
// AMQP 0-9-1 consumer: a stream is an append-only log, not `basic.get`-able, so the reader
// starts at its first offset and its acks are the credit that advances it.

/// The rendered action: one record per key, its value the `{key, op, doc}` document (the
/// shape the amqp tests assert). Plain streams ignore `routing_key`; the super stream tests
/// render it.
const RMQS_PUT: &str = r#"POST / HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{"value": {"key": {{json key}}, "op": {{json op}}, "doc": {{json doc}}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

/// The target's `expect`: a publish the broker confirmed (see `Row::rabbitmq_stream`).
const RMQS_EXPECT: &str = "JSON_EXTRACT(body, '$.publishing_id') IS NOT NULL";

/// The broker's stream endpoint `host:port`, when the suite has one.
fn rabbitmq_stream_broker() -> Option<String> {
    std::env::var("LAZYWF_TEST_RABBITMQ_STREAM")
        .ok()
        .filter(|v| !v.is_empty())
}

/// The stream account of the compose broker (same user as the amqp tests).
fn rabbitmq_stream_vars() -> String {
    std::env::set_var("LWF_TEST_RMQS_PW", "lwftest");
    json!({"username": "lwftest", "password": "env:LWF_TEST_RMQS_PW"}).to_string()
}

/// The (host, port) of the stream endpoint (the default port when the value has none).
fn rabbitmq_stream_addr(broker: &str) -> (String, u16) {
    match broker.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().expect("stream port")),
        None => (broker.to_string(), 5552),
    }
}

/// The test's own administration connection (the official client's environment).
async fn stream_env(broker: &str) -> rabbitmq_stream_client::Environment {
    let (host, port) = rabbitmq_stream_addr(broker);
    rabbitmq_stream_client::Environment::builder()
        .host(&host)
        .port(port)
        .username("lwftest")
        .password("lwftest")
        .build()
        .await
        .unwrap()
}

/// A fresh stream: deleted when one survives from an earlier run, then created.
async fn make_stream(broker: &str, stream: &str) {
    let env = stream_env(broker).await;
    env.delete_stream(stream).await.ok();
    env.stream_creator().create(stream).await.unwrap();
}

/// A fresh super stream of `n` partitions (`{name}-0` … `{name}-{n-1}`), leftover partitions
/// deleted with it.
async fn make_super_stream(broker: &str, name: &str, n: usize) {
    let env = stream_env(broker).await;
    env.delete_super_stream(name).await.ok();
    for i in 0..n {
        env.delete_stream(&format!("{name}-{i}")).await.ok();
    }
    env.stream_creator()
        .create_super_stream(name, n, None)
        .await
        .unwrap();
}

/// The partition stream a routing key goes to: the broker's own partition list
/// (`super_stream_partitions`) with murmur3 (seed 104729) modulo its length — the rule of the
/// client library's own super-stream producer, and the rule the kind uses. The broker's
/// `super_stream_route` command is not asked: this client decodes its answer (publisher items)
/// as a string list, so it reads back empty for every key.
async fn expected_partition(broker: &str, key: &str, super_stream: &str) -> String {
    use rabbitmq_stream_client::{Client, ClientOptions};
    let (host, port) = rabbitmq_stream_addr(broker);
    let opts = ClientOptions::builder()
        .host(&host)
        .port(port)
        .user("lwftest")
        .password("lwftest")
        .build();
    let client = Client::connect(opts).await.unwrap();
    let r = client.partitions(super_stream.to_string()).await.unwrap();
    client.close().await.ok();
    assert!(
        r.is_ok() && !r.streams.is_empty(),
        "the broker lists no partitions of {super_stream}"
    );
    let h = murmur3::murmur3_32(&mut std::io::Cursor::new(key), 104729).unwrap();
    r.streams[(h % r.streams.len() as u32) as usize].clone()
}

/// One record of a stream as the test consumer reads it: the key carried as the application
/// property `lazywf_key`, and the value.
#[derive(Debug, Clone, PartialEq)]
struct SRec {
    key: Option<String>,
    value: Value,
}

/// A reader over one stream: a consumer from its first offset with a prefetch and an ack per
/// delivery, collecting everything without removing anything from the log.
struct StreamRead {
    _conn: lapin::Connection,
    drain: tokio::task::JoinHandle<()>,
    got: Arc<std::sync::Mutex<Vec<SRec>>>,
}

impl StreamRead {
    /// One attempt at the AMQP 0-9-1 consumer of the stream, from the fresh connection to the
    /// subscribed consumer. A stream is read this way because the point is what a consumer of the
    /// stream sees: the `lazywf_key` of each record and the document, as appended by the kind.
    async fn attach(
        url: &str,
        stream: &str,
    ) -> anyhow::Result<(lapin::Connection, lapin::Consumer)> {
        use lapin::options::*;
        use lapin::types::AMQPValue;
        use lapin::ConnectionProperties;
        let conn = lapin::Connection::connect(url, ConnectionProperties::default()).await?;
        let chan = conn.create_channel().await?;
        chan.basic_qos(200, BasicQosOptions::default()).await?;
        let mut copts = lapin::types::FieldTable::default();
        copts.insert(
            "x-stream-offset".into(),
            AMQPValue::LongString("first".into()),
        );
        let consumer = chan
            .basic_consume(
                stream.into(),
                "lwf-reader".into(),
                BasicConsumeOptions::default(),
                copts,
            )
            .await?;
        Ok((conn, consumer))
    }

    /// Attaches a consumer of the whole stream — over the broker's **AMQP 0-9-1** endpoint, the way
    /// a stream is read: a consumer with `x-stream-offset: first` (the stream port itself speaks
    /// another protocol and would abort this handshake). The RabbitMQ of the shared compose file
    /// belongs to the host too and another agent can restart it while a suite runs, so attaching is
    /// retried for a while: what the test asserts is what the reader sees, not that the broker
    /// never restarts. A broker that stays gone is still a failure.
    async fn setup(stream: &str) -> StreamRead {
        use futures_util::StreamExt;
        let broker = amqp_broker()
            .filter(|v| !v.is_empty())
            .expect("LAZYWF_TEST_AMQP: a stream is read back by an AMQP 0-9-1 consumer, this port");
        let url = format!("amqp://lwftest:lwftest@{broker}/%2f");
        let (conn, mut consumer) = {
            let mut last = "never attempted".to_string();
            let mut out = None;
            for _ in 0..30 {
                match StreamRead::attach(&url, stream).await {
                    Ok(v) => {
                        out = Some(v);
                        break;
                    }
                    Err(e) => {
                        last = e.to_string();
                        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                    }
                }
            }
            out.unwrap_or_else(|| panic!("the reader could not attach to {broker}: {last}"))
        };
        let got = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = got.clone();
        let drain = tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                let Ok(d) = delivery else { break };
                let key = d
                    .properties
                    .headers()
                    .as_ref()
                    .and_then(|h| {
                        h.into_iter().find(|(k, _)| k.as_str() == "lazywf_key").map(
                            |(_, v)| match v {
                                lapin::types::AMQPValue::LongString(s) => s.to_string(),
                                _ => String::new(),
                            },
                        )
                    })
                    .filter(|k| !k.is_empty());
                sink.lock().unwrap().push(SRec {
                    key,
                    value: serde_json::from_slice(&d.data).unwrap_or(Value::Null),
                });
                let _ = d.ack(lapin::options::BasicAckOptions::default()).await;
            }
        });
        StreamRead {
            _conn: conn,
            drain,
            got,
        }
    }

    /// Waits until at least `n` records have been read from the log, and returns them all.
    async fn at_least(&self, n: usize) -> Vec<SRec> {
        wait_for(
            Duration::from_secs(30),
            &format!("{n} stream records"),
            || async {
                let all = self.got.lock().unwrap().clone();
                (all.len() >= n).then_some(all)
            },
        )
        .await
    }
}

impl Drop for StreamRead {
    fn drop(&mut self) {
        self.drain.abort();
    }
}

/// The `rabbitmq-stream` kind into a plain stream: every key arrives with its document
/// (insert, update, delete), the key also carried as the `lazywf_key` application property,
/// the target's `expect` met on every confirm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_stream_target() {
    let root = need_db!();
    let Some(broker) = rabbitmq_stream_broker() else {
        return eprintln!("LAZYWF_TEST_RABBITMQ_STREAM not set: skipping");
    };
    let db = Db::fresh(&root, "rabbitmq_stream_target").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let stream = "lwf.rabbitmq_stream_target";
    make_stream(&broker, stream).await;
    let read = StreamRead::setup(stream).await;

    db.target(
        "bus",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://{broker}/{stream}"),
        Some(RMQS_EXPECT),
        &rabbitmq_stream_vars(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            RMQS_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    let got = read.at_least(2).await;
    assert_eq!(got.len(), 2, "one record per key, no duplicates: {got:?}");
    for (key, sku, qty) in [("1", "A-1", 3), ("2", "B-2", 1)] {
        let r = got.iter().find(|r| r.key.as_deref() == Some(key)).unwrap();
        assert_eq!(
            r.value,
            json!({"key": key, "op": "upsert", "doc": {"sku": sku, "qty": qty}})
        );
    }

    db.exec("UPDATE orders SET qty = 4 WHERE id = 1").await;
    let got = read.at_least(3).await;
    assert_eq!(
        got[2].value,
        json!({"key": "1", "op": "upsert", "doc": {"sku": "A-1", "qty": 4}})
    );

    db.exec("DELETE FROM orders WHERE id = 2").await;
    let got = read.at_least(4).await;
    assert_eq!(got[3].key.as_deref(), Some("2"));
    assert_eq!(got[3].value["op"], "delete", "{:?}", got[3]);
    // The `expect` held: an unmet expectation would have refused every record and the rule
    // would be striking keys.
    inst.quiet().await;
    inst.stop().await;
}

/// A stream the broker does not know: every action is refused at the declare (the target
/// never creates streams), the keys are struck until they are dropped, `last_error` names the
/// stream — and nothing is delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_stream_unknown_stream() {
    let root = need_db!();
    let Some(broker) = rabbitmq_stream_broker() else {
        return eprintln!("LAZYWF_TEST_RABBITMQ_STREAM not set: skipping");
    };
    let db = Db::fresh(&root, "rabbitmq_stream_unknown_stream").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let stream = "lwf.rabbitmq_stream_no_such_stream";
    let env = stream_env(&broker).await;
    env.delete_stream(stream).await.ok();
    std::mem::forget(env);

    db.target(
        "bus",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://{broker}/{stream}"),
        None,
        &rabbitmq_stream_vars(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            RMQS_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3)").await;
    // The refusal is a link-level one (the declare), so it is the rule's last error and the key
    // is **not** struck: nothing of the batch arrived, so the answer says nothing about this key
    // on its own — the engine keeps the key pending and retries, as it does for kafka's unknown
    // topic, until the operator creates the stream or fixes the target.
    let err = wait_for(
        Duration::from_secs(90),
        "the unknown stream refused at link level",
        || async {
            let (_, _, _, last) = db.status("orders").await?;
            let err = last?;
            (err.contains(stream) && err.contains("unknown to the broker")).then_some(err)
        },
    )
    .await;
    assert!(err.contains("the target does not create it"), "{err}");
    inst.stop().await;
}

/// The connection is cut and the instance restarts over the broker: the keys changed meanwhile
/// are re-sent and the stream holds every one of them (read back from its first offset), none
/// lost. A re-send may store a record twice (at-least-once); keys are checked deduplicated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_stream_restart() {
    let root = need_db!();
    let Some(broker) = rabbitmq_stream_broker() else {
        return eprintln!("LAZYWF_TEST_RABBITMQ_STREAM not set: skipping");
    };
    let db = Db::fresh(&root, "rabbitmq_stream_restart").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let stream = "lwf.rabbitmq_stream_restart";
    make_stream(&broker, stream).await;
    let read = StreamRead::setup(stream).await;

    let upstream: &'static str = Box::leak(broker.clone().into_boxed_str());
    let proxy = Flaky::start(upstream).await;
    let cfg = config(&db, &[]);
    db.target(
        "bus",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://127.0.0.1:{}/{stream}", proxy.port),
        None,
        &rabbitmq_stream_vars(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            RMQS_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(cfg.clone()).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    assert_eq!(read.at_least(2).await.len(), 2);

    // The broker goes away; a key changes meanwhile and cannot be confirmed. The instance is
    // then stopped: its position did not pass key 3, so the new instance re-sends it.
    proxy.cut();
    db.exec("INSERT INTO orders VALUES (3, 'C-3', 7)").await;
    wait_for(Duration::from_secs(30), "a refused connection", || async {
        (proxy.refused() >= 1).then_some(())
    })
    .await;
    assert!(
        !read
            .got
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.key.as_deref() == Some("3")),
        "key 3 arrived while the broker was cut off"
    );
    inst.stop().await;
    proxy.heal();

    let inst = Instance::ready(cfg).await;
    let got = read.at_least(3).await;
    let mut keys: Vec<String> = got.iter().filter_map(|r| r.key.clone()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys, ["1", "2", "3"], "every key arrives: {got:?}");
    let r3 = got.iter().find(|r| r.key.as_deref() == Some("3")).unwrap();
    assert_eq!(
        r3.value,
        json!({"key": "3", "op": "upsert", "doc": {"sku": "C-3", "qty": 7}})
    );
    inst.stop().await;
}

/// A super stream routed by key: records with a `routing_key` land on the partition the broker's
/// partition list and the hash rule give (checked per key against the partition they were read
/// from), every key exactly once across the three partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_stream_super_stream() {
    let root = need_db!();
    let Some(broker) = rabbitmq_stream_broker() else {
        return eprintln!("LAZYWF_TEST_RABBITMQ_STREAM not set: skipping");
    };
    let db = Db::fresh(&root, "rabbitmq_stream_super_stream").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let super_name = "lwf.rabbitmq_stream_super_stream";
    make_super_stream(&broker, super_name, 3).await;
    let parts = [
        "lwf.rabbitmq_stream_super_stream-0",
        "lwf.rabbitmq_stream_super_stream-1",
        "lwf.rabbitmq_stream_super_stream-2",
    ];
    let mut reads = Vec::new();
    for p in parts {
        reads.push(StreamRead::setup(p).await);
    }

    let template = r#"POST / HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{"value": {"key": {{json key}}, "op": {{json op}}, "doc": {{json doc}}}, "routing_key": {{json key}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;
    db.target(
        "bus",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://{broker}/{super_name}"),
        Some(RMQS_EXPECT),
        &rabbitmq_stream_vars(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            template,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1), (3, 'C-3', 7), (4, 'D-4', 2)")
        .await;
    // (key, partition) of every record read so far, taken fresh on each look.
    let seen: Vec<(String, String)> =
        wait_for(Duration::from_secs(60), "the four records", || async {
            let mut got: Vec<(String, String)> = Vec::new();
            for (i, r) in reads.iter().enumerate() {
                for rec in r.got.lock().unwrap().iter() {
                    if let Some(k) = &rec.key {
                        got.push((k.clone(), parts[i].to_string()));
                    }
                }
            }
            (got.len() >= 4).then_some(got)
        })
        .await;
    let mut uniq: Vec<&String> = seen.iter().map(|(k, _)| k).collect();
    uniq.sort();
    let before = uniq.len();
    uniq.dedup();
    assert_eq!(before, uniq.len(), "a key arrived twice: {seen:?}");
    assert_eq!(uniq, ["1", "2", "3", "4"], "every key once: {seen:?}");
    // The partition of each key is the one the broker's partition list and the hash rule give.
    for key in ["1", "2", "3", "4"] {
        let want = expected_partition(&broker, key, super_name).await;
        let got = seen
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, p)| p.as_str())
            .unwrap();
        assert_eq!(got, want.as_str(), "key {key} on the wrong partition");
    }
    inst.stop().await;
}

/// One record the target refuses alone (a super stream record with no routing key: nothing to
/// route by): its neighbours of the same batch go through, only it is struck, and the position
/// moves on — `last_error` says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rabbitmq_stream_refused_alone() {
    let root = need_db!();
    let Some(broker) = rabbitmq_stream_broker() else {
        return eprintln!("LAZYWF_TEST_RABBITMQ_STREAM not set: skipping");
    };
    let db = Db::fresh(&root, "rabbitmq_stream_refused_alone").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    let super_name = "lwf.rabbitmq_stream_refused_alone";
    make_super_stream(&broker, super_name, 3).await;
    let parts: Vec<String> = (0..3).map(|i| format!("{super_name}-{i}")).collect();
    let mut reads = Vec::new();
    for p in &parts {
        reads.push(StreamRead::setup(p).await);
    }

    // Key 2 renders a null routing_key: the broker cannot place it, and no retry changes that.
    let template = r#"POST / HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{"value": {"key": {{json key}}, "op": {{json op}}, "doc": {{json doc}}}, "routing_key": {{#if (eq key "2")}}null{{else}}{{json key}}{{/if}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;
    db.target(
        "bus",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://{broker}/{super_name}"),
        None,
        &rabbitmq_stream_vars(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            template,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    let start = binlog_end(&db).await;

    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1), (3, 'C-3', 7)")
        .await;
    // Keys 1 and 3 arrive; key 2 never does: its neighbours of the batch went through, so the
    // answer is about that record alone, it is struck five times and dropped, and the position
    // moves on (`last_error` is the engine's drop line, engine.rs `judge_strikes`).
    let err = wait_for(
        Duration::from_secs(60),
        "key 2 refused alone and the position moved on",
        || async {
            let (f, p, _, last) = db.status("orders").await?;
            let err = last?;
            (err.contains("`2` refused") && (f, p) >= start).then_some(err)
        },
    )
    .await;
    assert!(err.contains("routing_key"), "{err}");
    let mut keys: Vec<String> = Vec::new();
    for r in &reads {
        keys.extend(r.got.lock().unwrap().iter().filter_map(|x| x.key.clone()));
    }
    keys.sort();
    keys.dedup();
    assert_eq!(keys, ["1", "3"], "the neighbours went through: {keys:?}");
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// `pubsub` targets, against Google's Pub/Sub emulator (`LAZYWF_TEST_PUBSUB`, e.g.
// http://127.0.0.1:34563; the `pubsub` compose profile). The tests read the messages back
// through a subscription on the emulator (its REST API, project `lwf-test`).

const PUBSUB_PUBLISH: &str = r#"POST /v1/projects/{{endpoint.project}}/topics/{{endpoint.topic}}:publish HTTP/1.1
Content-Type: application/json

{"messages": [{{#each docs}}{"data": {{json (base64 (json doc))}}, "attributes": {"key": {{json key}}, "op": {{json op}}}{{#if @root.target.vars.ordering}}, "orderingKey": {{json key}}{{/if}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

fn pubsub_root() -> Option<String> {
    std::env::var("LAZYWF_TEST_PUBSUB")
        .ok()
        .filter(|v| !v.is_empty())
}

macro_rules! need_pubsub {
    () => {
        match pubsub_root() {
            Some(u) => u,
            None => {
                eprintln!("LAZYWF_TEST_PUBSUB not set: skipping (docker compose --profile pubsub up -d --wait pubsub-emulator)");
                return;
            }
        }
    };
}

/// The Pub/Sub REST API of the emulator.
struct Pubsub {
    host: String,
    client: lazy_wire_fire::http::Client,
}

impl Pubsub {
    fn new(root: &str) -> Pubsub {
        Pubsub {
            host: root.trim_start_matches("http://").to_string(),
            client: lazy_wire_fire::http::Client::new(
                lazy_wire_fire::http::BaseUrl::parse(root).unwrap(),
                &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
        }
    }

    async fn call(&self, method: &str, path: &str, body: Value) -> Value {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: method.into(),
            target: path.into(),
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        };
        req.set_header("Host", self.host.clone());
        req.set_header("Content-Type", "application/json".into());
        let resp = self
            .client
            .send(&req, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(
            (200..300).contains(&resp.status),
            "{method} {path}: {} {}",
            resp.status,
            resp.body_snippet()
        );
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    /// Creates a topic; the endpoint the target uses is `gc-pubsub://lwf-test/<topic>`.
    async fn topic(&self, name: &str) {
        self.call(
            "PUT",
            &format!("/v1/projects/lwf-test/topics/{name}"),
            json!({}),
        )
        .await;
    }

    async fn subscription(&self, topic: &str, name: &str, ordering: bool) {
        // The emulator keeps subscriptions across runs: start each test from an empty one.
        let path = format!("/v1/projects/lwf-test/subscriptions/{name}");
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "DELETE".into(),
            target: path.clone(),
            headers: Vec::new(),
            body: Vec::new(),
        };
        req.set_header("Host", self.host.clone());
        self.client
            .send(&req, Duration::from_secs(10))
            .await
            .unwrap();
        self.call(
            "PUT",
            &path,
            json!({"topic": format!("projects/lwf-test/topics/{topic}"), "enableMessageOrdering": ordering}),
        )
        .await;
    }

    /// The messages the subscription holds now; they are acknowledged, so each comes once.
    async fn pull(&self, sub: &str) -> Vec<Value> {
        let v = self
            .call(
                "POST",
                &format!("/v1/projects/lwf-test/subscriptions/{sub}:pull"),
                // The emulator holds an empty pull for ~10 s unless told to return at once.
                json!({"maxMessages": 100, "returnImmediately": true}),
            )
            .await;
        let got = v
            .get("receivedMessages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let ids: Vec<Value> = got.iter().filter_map(|m| m.get("ackId").cloned()).collect();
        if !ids.is_empty() {
            self.call(
                "POST",
                &format!("/v1/projects/lwf-test/subscriptions/{sub}:acknowledge"),
                json!({"ackIds": ids}),
            )
            .await;
        }
        got.into_iter()
            .filter_map(|m| m.get("message").cloned())
            .collect()
    }
}

/// Pulls until `n` messages have arrived (or the time is up).
async fn pubsub_pull_until(ps: &Pubsub, sub: &str, n: usize, secs: u64) -> Vec<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    let mut got = Vec::new();
    loop {
        got.extend(ps.pull(sub).await);
        if got.len() >= n {
            return got;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out with {} of {n} messages",
            got.len()
        );
        sleep_ms(100).await;
    }
}

/// A message's document, key and op (`data` is base64 of the document's JSON).
fn pubsub_decoded(m: &Value) -> (Value, String, String) {
    use base64::Engine as _;
    let data = base64::engine::general_purpose::STANDARD
        .decode(m["data"].as_str().unwrap())
        .unwrap();
    let doc: Value = serde_json::from_slice(&data).unwrap();
    let key = m["attributes"]["key"].as_str().unwrap().to_string();
    let op = m["attributes"]["op"].as_str().unwrap().to_string();
    (doc, key, op)
}

/// A customers table, a topic with a subscription, and the rule `queue` that publishes each
/// changed row to the topic (`ordering`: the key is the ordering key).
async fn pubsub_rule(
    db: &Db,
    ps: &Pubsub,
    topic: &str,
    sub: &str,
    ordering: bool,
    selects: &str,
) -> String {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL)")
        .await;
    tables(db).await;
    ps.topic(topic).await;
    ps.subscription(topic, sub, ordering).await;
    let vars = if ordering {
        r#"{"ordering": true}"#
    } else {
        "{}"
    };
    db.target(
        "queue",
        "pubsub",
        &format!(
            "gc-pubsub://lwf-test/{topic}?endpoint={}",
            pubsub_root().unwrap()
        ),
        Some("JSON_LENGTH(JSON_EXTRACT(body, '$.messageIds')) > 0"),
        vars,
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('queue', 'queue', 'batch', 1000, ?, ?, 100)",
        (selects, PUBSUB_PUBLISH),
    )
    .await;
    db.trigger("queue", "customers", Some("{{new.id}}"), None)
        .await;
    pubsub_root().unwrap()
}

const PUBSUB_SELECT: &str =
    "SELECT c.id AS _key, c.id, c.name, c.rev, NULL AS _op FROM customers c WHERE c.id IN (:keys)";

/// Every changed key arrives once, with its document as `data` and `key` / `op` attributes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_every_key_arrives_with_data_and_attributes() {
    let root = need_db!();
    let ps = Pubsub::new(&need_pubsub!());
    let db = Db::fresh(&root, "pubsub_all").await;
    let topic = format!("lwf_all_{}", std::process::id());
    pubsub_rule(&db, &ps, &topic, "all-s", false, PUBSUB_SELECT).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1)")
        .await;
    let got = pubsub_pull_until(&ps, "all-s", 3, 30).await;
    sleep_ms(1000).await;
    let more = ps.pull("all-s").await;
    assert!(more.is_empty(), "a key was sent twice: {more:?}");
    let mut keys: Vec<String> = got
        .iter()
        .map(|m| {
            let (doc, key, op) = pubsub_decoded(m);
            assert_eq!(
                doc["id"].as_i64().unwrap().to_string(),
                key,
                "key attribute"
            );
            assert_eq!(op, "upsert");
            assert_eq!(
                doc["name"].as_str().unwrap(),
                match key.as_str() {
                    "1" => "Ann",
                    "2" => "Bob",
                    _ => "Cy",
                }
            );
            key
        })
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["1", "2", "3"]);
    inst.stop().await;
}

/// An update is a new message with the new document; a delete arrives with `op` delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_update_and_delete_arrive_as_ops() {
    let root = need_db!();
    let ps = Pubsub::new(&need_pubsub!());
    let db = Db::fresh(&root, "pubsub_upd").await;
    let topic = format!("lwf_upd_{}", std::process::id());
    pubsub_rule(&db, &ps, &topic, "upd-s", false, PUBSUB_SELECT).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1)")
        .await;
    let first = pubsub_pull_until(&ps, "upd-s", 1, 30).await;
    assert_eq!(pubsub_decoded(&first[0]).2, "upsert");
    db.exec("UPDATE customers SET name = 'Anna', rev = 2 WHERE id = 1")
        .await;
    let second = pubsub_pull_until(&ps, "upd-s", 1, 30).await;
    let (doc, key, op) = pubsub_decoded(&second[0]);
    assert_eq!(
        (key.as_str(), op.as_str(), doc["name"].as_str()),
        ("1", "upsert", Some("Anna"))
    );
    db.exec("DELETE FROM customers WHERE id = 1").await;
    let third = pubsub_pull_until(&ps, "upd-s", 1, 30).await;
    let (_, key, op) = pubsub_decoded(&third[0]);
    assert_eq!(
        (key.as_str(), op.as_str()),
        ("1", "delete"),
        "third pull: {third:?}"
    );
    inst.stop().await;
}

/// With `ordering` on, the messages of one key arrive in the order they were sent, and the
/// newest state is the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_ordering_keeps_the_order_of_a_key() {
    let root = need_db!();
    let ps = Pubsub::new(&need_pubsub!());
    let db = Db::fresh(&root, "pubsub_ord").await;
    let topic = format!("lwf_ord_{}", std::process::id());
    pubsub_rule(&db, &ps, &topic, "ord-s", true, PUBSUB_SELECT).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'v1',1)")
        .await;
    pubsub_pull_until(&ps, "ord-s", 1, 30).await;
    db.exec("UPDATE customers SET name = 'v2', rev = 2 WHERE id = 1")
        .await;
    pubsub_pull_until(&ps, "ord-s", 1, 30).await;
    db.exec("UPDATE customers SET name = 'v3', rev = 3 WHERE id = 1")
        .await;
    let last = pubsub_pull_until(&ps, "ord-s", 1, 30).await;
    let (doc, _, _) = pubsub_decoded(last.last().unwrap());
    assert_eq!(doc["rev"].as_i64(), Some(3), "the newest state is not last");
    inst.stop().await;
}

/// An outage of the endpoint is retried: keys changed during it are not lost, and arrive
/// after the heal. Nothing is delivered while the stand-in refuses connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_retry_after_an_outage_loses_nothing() {
    let root = need_db!();
    let direct = need_pubsub!();
    let ps = Pubsub::new(&direct);
    let upstream: &'static str = Box::leak(
        direct
            .trim_start_matches("http://")
            .to_string()
            .into_boxed_str(),
    );
    let flaky = Flaky::start(upstream).await;
    let db = Db::fresh(&root, "pubsub_retry").await;
    let topic = format!("lwf_retry_{}", std::process::id());
    ps.topic(&topic).await;
    ps.subscription(&topic, "retry-s", false).await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL)")
        .await;
    tables(&db).await;
    db.target(
        "queue",
        "pubsub",
        &format!(
            "gc-pubsub://lwf-test/{topic}?endpoint=http://127.0.0.1:{}",
            flaky.port
        ),
        Some("JSON_LENGTH(JSON_EXTRACT(body, '$.messageIds')) > 0"),
        "{}",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('queue', 'queue', 'batch', 1000, ?, ?, 100)",
        (PUBSUB_SELECT, PUBSUB_PUBLISH),
    )
    .await;
    db.trigger("queue", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    flaky.cut();
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1)")
        .await;
    wait_for(Duration::from_secs(20), "refused connections", || async {
        (flaky.refused() > 0).then_some(())
    })
    .await;
    assert!(
        ps.pull("retry-s").await.is_empty(),
        "delivered during the outage"
    );
    flaky.heal();
    let got = pubsub_pull_until(&ps, "retry-s", 2, 60).await;
    let mut keys: Vec<String> = got.iter().map(|m| pubsub_decoded(m).1).collect();
    keys.sort();
    assert_eq!(keys, vec!["1", "2"]);
    inst.stop().await;
}

/// A publish the service refuses (here: a document over the 10 MB request limit) is bisected:
/// the other keys arrive, and the refused key alone takes the strikes and is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_oversized_key_is_refused_alone() {
    let root = need_db!();
    let ps = Pubsub::new(&need_pubsub!());
    let db = Db::fresh(&root, "pubsub_big").await;
    let topic = format!("lwf_big_{}", std::process::id());
    let selects = "SELECT c.id AS _key, c.id, CASE WHEN c.id = 99 THEN REPEAT('x', 10500000) ELSE c.name END AS name, c.rev, NULL AS _op FROM customers c WHERE c.id IN (:keys)";
    pubsub_rule(&db, &ps, &topic, "big-s", false, selects).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(99,'Big',1)")
        .await;
    let got = pubsub_pull_until(&ps, "big-s", 2, 30).await;
    let mut keys: Vec<String> = got.iter().map(|m| pubsub_decoded(m).1).collect();
    keys.sort();
    assert_eq!(keys, vec!["1", "2"]);
    wait_for(
        Duration::from_secs(90),
        "key 99 dropped after its strikes",
        || async {
            db.status("queue")
                .await
                .and_then(|s| s.3)
                .filter(|e| e.contains("key `99`"))
                .map(|_| ())
        },
    )
    .await;
    assert!(
        ps.pull("big-s").await.is_empty(),
        "the refused key was sent"
    );
    inst.stop().await;
}

/// Reads one HTTP request (head and body by Content-Length) from a connection.
async fn pubsub_read_request(s: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let len = head
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|v| v.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while buf.len() < head_end + 4 + len {
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Some((head, buf[head_end + 4..head_end + 4 + len].to_vec()))
}

/// A TCP stand-in in front of the emulator. The `nth` publish request is answered 500 and never
/// forwarded; every other request is forwarded on its own connection (closed after the answer).
struct PubsubInject {
    port: u16,
}

impl PubsubInject {
    async fn start(upstream: &'static str, nth: usize) -> PubsubInject {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let publishes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let publishes = publishes.clone();
                tokio::spawn(PubsubInject::serve(client, upstream, nth, publishes));
            }
        });
        PubsubInject { port }
    }

    async fn serve(
        mut client: tokio::net::TcpStream,
        upstream: &'static str,
        nth: usize,
        publishes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use tokio::io::AsyncWriteExt;
        let Some((head, body)) = pubsub_read_request(&mut client).await else {
            return;
        };
        let first = head.lines().next().unwrap_or("").to_string();
        if first.contains(":publish") {
            let n = publishes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if n == nth {
                let text = r#"{"error":{"code":500,"status":"INTERNAL","message":"injected"}}"#;
                let resp = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                );
                let _ = client.write_all(resp.as_bytes()).await;
                let _ = client.shutdown().await;
                return;
            }
        }
        let Ok(mut up) = tokio::net::TcpStream::connect(upstream).await else {
            return;
        };
        let rest = head.split_once("\r\n").map(|(_, r)| r).unwrap_or("");
        let mut fwd = format!("{first}\r\nConnection: close\r\n{rest}\r\n\r\n").into_bytes();
        fwd.extend_from_slice(&body);
        if up.write_all(&fwd).await.is_err() {
            return;
        }
        // The answer by its Content-Length (the emulator keeps the connection open otherwise).
        let Some((rhead, rbody)) = pubsub_read_request(&mut up).await else {
            return;
        };
        let mut answer = format!("{rhead}\r\n\r\n").into_bytes();
        answer.extend_from_slice(&rbody);
        let _ = client.write_all(&answer).await;
        let _ = client.shutdown().await;
    }
}

/// With ordering on, a transient failure of one ordered publish is retried, and the messages of
/// the key still arrive in publish order (the newest state last). No resume call is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_ordering_survives_a_transient_failure_in_order() {
    let root = need_db!();
    let direct = need_pubsub!();
    let ps = Pubsub::new(&direct);
    let upstream: &'static str = Box::leak(
        direct
            .trim_start_matches("http://")
            .to_string()
            .into_boxed_str(),
    );
    // The second publish request of the rule fails once (the second update of the key).
    let proxy = PubsubInject::start(upstream, 2).await;
    let db = Db::fresh(&root, "pubsub_ordfail").await;
    let topic = format!("lwf_ordfail_{}", std::process::id());
    ps.topic(&topic).await;
    ps.subscription(&topic, "ordfail-s", true).await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL)")
        .await;
    tables(&db).await;
    db.target(
        "queue",
        "pubsub",
        &format!(
            "gc-pubsub://lwf-test/{topic}?endpoint=http://127.0.0.1:{}",
            proxy.port
        ),
        Some("JSON_LENGTH(JSON_EXTRACT(body, '$.messageIds')) > 0"),
        r#"{"ordering": true}"#,
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('queue', 'queue', 'batch', 1000, ?, ?, 100)",
        (PUBSUB_SELECT, PUBSUB_PUBLISH),
    )
    .await;
    db.trigger("queue", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    // One change at a time: each is delivered before the next is made, so the second publish
    // (v2) is the one the proxy refuses, and v3 waits behind its retry.
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'v1',1)")
        .await;
    let mut got = pubsub_pull_until(&ps, "ordfail-s", 1, 60).await;
    db.exec("UPDATE customers SET name = 'v2', rev = 2 WHERE id = 1")
        .await;
    got.extend(pubsub_pull_until(&ps, "ordfail-s", 1, 60).await);
    db.exec("UPDATE customers SET name = 'v3', rev = 3 WHERE id = 1")
        .await;
    got.extend(pubsub_pull_until(&ps, "ordfail-s", 1, 60).await);
    let revs: Vec<i64> = got
        .iter()
        .map(|m| {
            let (doc, key, _) = pubsub_decoded(m);
            assert_eq!(key, "1");
            doc["rev"].as_i64().unwrap()
        })
        .collect();
    assert_eq!(
        revs,
        vec![1, 2, 3],
        "the messages of the key are out of order"
    );
    assert!(
        ps.pull("ordfail-s").await.is_empty(),
        "a key was sent twice"
    );
    inst.stop().await;
}

/// `LAZYWF_ALLOWED_HOSTS`: a target whose endpoint host is not listed is refused at load, and
/// its rule's `last_error` names the host and the setting; a listed host delivers. The names
/// are matched as written (`localhost` is not `127.0.0.1`, and nothing is resolved).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allowed_hosts_refuse_unlisted_endpoints() {
    let root = need_db!();
    let db = Db::fresh(&root, "allowlist").await;
    shop(&db).await;
    let mock = Mock::start().await;
    let listed = mock.url();
    let unlisted = listed.replace("127.0.0.1", "localhost");
    db.target("listed", "http", &listed, None, "{}").await;
    db.target("unlisted", "http", &unlisted, None, "{}").await;
    for (rule, target) in [("ok", "listed"), ("no", "unlisted")] {
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, 'POST /hook HTTP/1.1\n\n{{join keys \",\"}}', 100)",
            (rule, target),
        )
        .await;
        db.trigger(rule, "orders", Some("{{new.id}}"), None).await;
    }
    let inst = Instance::ready(config(&db, &[("LAZYWF_ALLOWED_HOSTS", "127.0.0.1")])).await;
    db.exec("INSERT INTO orders VALUES (7, 1, 'new', 1)").await;
    db.tick().await;
    wait_for(
        Duration::from_secs(10),
        "the listed target delivers",
        || async { mock.requests().iter().any(|r| r.body == "7").then_some(()) },
    )
    .await;
    wait_for(
        Duration::from_secs(10),
        "the unlisted rule's error",
        || async {
            let (_, _, _, err) = db.status("no").await?;
            err.filter(|e| e.contains("localhost")).map(|_| ())
        },
    )
    .await;
    let (_, _, _, err) = db.status("no").await.unwrap();
    assert!(
        err.as_deref()
            .is_some_and(|e| e.contains("LAZYWF_ALLOWED_HOSTS")),
        "{err:?}"
    );
    // Only the listed target was called, and only with the listed rule's key.
    assert!(mock.requests().iter().all(|r| r.body == "7"));
    inst.stop().await;
}

/// The default of `LAZYWF_ALLOWED_HOSTS` denies every target: with the variable unset (the empty
/// value is the unset one) the target is refused at load, its rule's `last_error` names the
/// variable and the `*` opt-out, and the backend is never called. `*` brings it back to life.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allowed_hosts_unset_denies_every_target() {
    let root = need_db!();
    let db = Db::fresh(&root, "allowdeny").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, 'POST /hook HTTP/1.1\n\n{{join keys \",\"}}', 100)",
        ("hook", "hook"),
    )
    .await;
    db.trigger("hook", "orders", Some("{{new.id}}"), None).await;

    let inst = Instance::ready(config(&db, &[("LAZYWF_ALLOWED_HOSTS", "")])).await;
    db.exec("INSERT INTO orders VALUES (8, 1, 'new', 1)").await;
    db.tick().await;
    wait_for(
        Duration::from_secs(10),
        "the denied rule's error",
        || async {
            let (_, _, _, err) = db.status("hook").await?;
            err.filter(|e| e.contains("LAZYWF_ALLOWED_HOSTS is not set"))
                .map(|_| ())
        },
    )
    .await;
    let (_, _, _, err) = db.status("hook").await.unwrap();
    let err = err.unwrap();
    assert!(err.contains("127.0.0.1"), "{err}");
    assert!(err.contains('*'), "{err}");
    // The refused target's backend never heard from the engine.
    assert!(mock.requests().is_empty(), "{:?}", mock.requests());
    inst.stop().await;

    // `*` is the opt out: the same rule delivers now. The denied key of the first phase was
    // never struck (nothing was sent, so nothing was judged), so it comes along in the batch.
    mock.clear();
    let inst = Instance::ready(config(&db, &[("LAZYWF_ALLOWED_HOSTS", "*")])).await;
    db.exec("INSERT INTO orders VALUES (9, 1, 'new', 1)").await;
    db.tick().await;
    wait_for(
        Duration::from_secs(10),
        "the allowed target delivers",
        || async {
            mock.requests()
                .iter()
                .any(|r| r.body.split(',').any(|k| k == "9"))
                .then_some(())
        },
    )
    .await;
    inst.stop().await;
}

/// An http endpoint whose URL is a secret (`env:NAME` / `file:/path`): the incoming webhook of a
/// business app, where everything below the host *is* the credential. The URL is resolved on every
/// request, so a rotated file applies without a reload; the template context, the log and
/// `last_error` see the origin only; the request line calls the webhook exactly as the file gives
/// it, with no trailing slash added; and `LAZYWF_ALLOWED_HOSTS` is checked against the host the
/// reference holds at that moment, not the one the target was loaded with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secret_endpoint_calls_the_webhook_exactly_as_given() {
    let root = need_db!();
    let db = Db::fresh(&root, "secretep").await;
    shop(&db).await;
    let mock = Mock::start().await;
    // The same secret directory the other reference tests use (it is one process-wide setting).
    let dir = std::env::current_dir().unwrap().join("tmp/out/secrets");
    std::fs::create_dir_all(&dir).unwrap();
    let hook = dir.join("incoming-webhook");
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    // Everything after the host is the credential.
    std::fs::write(
        &hook,
        format!(
            "http://{}/services/T0000/B0000/WEbhook5ecret?token=Qz9x\n",
            mock.addr
        ),
    )
    .unwrap();

    db.target(
        "hook",
        "http",
        &format!("file:{}", hook.display()),
        None,
        "{}",
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 100)",
        (
            "hook",
            "hook",
            r#"POST / HTTP/1.1
Content-Type: application/json

{"keys":{{join keys ","}},"url":{{json endpoint.url}}}"#,
        ),
    )
    .await;
    db.trigger("hook", "orders", Some("{{new.id}}"), None).await;

    let inst = Instance::ready(config(&db, &[("LAZYWF_ALLOWED_HOSTS", "127.0.0.1")])).await;
    db.exec("INSERT INTO orders VALUES (7, 1, 'new', 1)").await;
    db.tick().await;
    let got = wait_for(Duration::from_secs(30), "the webhook is called", || async {
        mock.requests()
            .into_iter()
            .find(|r| r.path == "/services/T0000/B0000/WEbhook5ecret?token=Qz9x")
    })
    .await;
    // Called exactly as the file gives it: the endpoint's own path and query, nothing appended,
    // and the `Host` of the resolved URL.
    assert_eq!(got.method, "POST");
    assert_eq!(got.header("host"), Some(mock.addr.to_string().as_str()));
    // What the template sees of the endpoint: the origin, and nothing below it.
    assert_eq!(
        got.body,
        format!("{{\"keys\":7,\"url\":\"http://{}/…\"}}", mock.addr)
    );

    // Rotated to a host the operator did not allow: refused before anything is sent, and the
    // rule's `last_error` names the host and the setting — never the path it stands for.
    mock.clear();
    std::fs::write(
        &hook,
        format!(
            "http://localhost:{}/services/T0000/B0000/ROTATED-path-9f2?token=Qz9x\n",
            mock.addr.port()
        ),
    )
    .unwrap();
    db.exec("INSERT INTO orders VALUES (8, 1, 'paid', 2)").await;
    db.tick().await;
    wait_for(
        Duration::from_secs(30),
        "the unlisted host is refused",
        || async {
            let (_, _, _, err) = db.status("hook").await?;
            err.filter(|e| e.contains("localhost") && e.contains("LAZYWF_ALLOWED_HOSTS"))
                .map(|_| ())
        },
    )
    .await;
    assert!(
        mock.requests().iter().all(|r| !r.path.contains("ROTATED")),
        "an unlisted host was called: {:?}",
        mock.requests()
            .iter()
            .map(|r| r.path.clone())
            .collect::<Vec<_>>()
    );

    // Rotated back, to another path: the next request uses it without a reload, still exactly as
    // the file gives it.
    mock.clear();
    std::fs::write(&hook, format!("http://{}/hooks/rotated-4c1\n", mock.addr)).unwrap();
    db.exec("INSERT INTO orders VALUES (9, 1, 'sent', 3)").await;
    db.tick().await;
    let got = wait_for(
        Duration::from_secs(30),
        "the rotated secret delivers",
        || async {
            mock.requests()
                .into_iter()
                .find(|r| r.path == "/hooks/rotated-4c1")
        },
    )
    .await;
    assert_eq!(got.path, "/hooks/rotated-4c1");
    // The document the template built names the origin, and nothing below it. (Which keys it
    // carries is the engine's business: a key the flush lost waits in the buffer and rides out
    // with the next one.)
    assert!(
        got.body
            .contains(&format!("\"url\":\"http://{}/…\"", mock.addr)),
        "{}",
        got.body
    );
    assert!(!got.body.contains("rotated-4c1"), "{}", got.body);
    inst.stop().await;
}

// `eventbridge` targets, against moto (`LAZYWF_TEST_EVENTBRIDGE`, e.g. http://127.0.0.1:34569;
// the `eventbridge` compose profile). The bus is read back the way AWS delivers: a rule on the
// bus with an SQS queue as its target, the queue drained through the SQS API. An entry's
// `DetailType` is the key, its `Detail` the document; the queue message is the event envelope.

const EB_PUT_EVENTS: &str = r#"POST / HTTP/1.1
X-Amz-Target: AWSEvents.PutEvents
Content-Type: application/x-amz-json-1.1

{"Entries": [{{#each docs}}{"Source": "lazywf", "DetailType": {{json key}}, "Detail": {{json (json doc)}}, "EventBusName": {{json endpoint.bus}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

/// The same, for the `rev` column: the entry of a row whose `rev` is NULL carries `not-json`
/// as its `Detail`, which EventBridge refuses entry by entry (`MalformedDetail`).
const EB_PUT_EVENTS_BAD_DETAIL: &str = r#"POST / HTTP/1.1
X-Amz-Target: AWSEvents.PutEvents
Content-Type: application/x-amz-json-1.1

{"Entries": [{{#each docs}}{"Source": "lazywf", "DetailType": {{json key}}, "Detail": {{#if doc.rev}}{{json (json doc)}}{{else}}{{json "not-json"}}{{/if}}, "EventBusName": {{json endpoint.bus}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

fn eventbridge_root() -> Option<String> {
    std::env::var("LAZYWF_TEST_EVENTBRIDGE")
        .ok()
        .filter(|v| !v.is_empty())
}

macro_rules! need_eventbridge {
    () => {
        match eventbridge_root() {
            Some(u) => u,
            None => {
                eprintln!("LAZYWF_TEST_EVENTBRIDGE not set: skipping (docker compose --profile eventbridge up -d moto)");
                return;
            }
        }
    };
}

/// moto: the EventBridge JSON API (the very API the target calls) and the SQS JSON API the
/// read-back queues are created and drained with. moto answers as the account 123456789012,
/// does not check signatures, and dispatches by the service of the credential scope.
struct Moto {
    root: String,
    client: lazy_wire_fire::http::Client,
}

impl Moto {
    /// Waits for the server: compose has no healthcheck for it (the other emulators' precedent).
    async fn new(root: &str) -> Moto {
        let moto = Moto {
            root: root.to_string(),
            client: lazy_wire_fire::http::Client::new(
                lazy_wire_fire::http::BaseUrl::parse(root).unwrap(),
                &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
        };
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "GET".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: Vec::new(),
        };
        req.set_header("Host", root.trim_start_matches("http://").to_string());
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if matches!(moto.client.send(&req, Duration::from_secs(2)).await, Ok(r) if r.status < 500)
            {
                return moto;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "moto at {root} did not come up within 30 s"
            );
            sleep_ms(200).await;
        }
    }

    async fn call(&self, target: &str, service: &str, body: Value) -> Value {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header("Content-Type", "application/x-amz-json-1.1".into());
        req.set_header("X-Amz-Target", target.into());
        req.set_header(
            "Authorization",
            format!("AWS4-HMAC-SHA256 Credential=lazywf/20260101/us-east-1/{service}/aws4_request, SignedHeaders=content-type;host;x-amz-target, Signature=00"),
        );
        let resp = self
            .client
            .send(&req, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            resp.status == 200,
            "{target}: {} {}",
            resp.status,
            resp.body_snippet()
        );
        if resp.body.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&resp.body).unwrap()
    }

    async fn eb(&self, action: &str, body: Value) -> Value {
        self.call(&format!("AWSEvents.{action}"), "events", body)
            .await
    }

    async fn sqs(&self, action: &str, body: Value) -> Value {
        self.call(&format!("AmazonSQS.{action}"), "sqs", body).await
    }

    /// Creates the bus and a queue, and the rule on the bus that delivers every event of it
    /// to the queue (the SQS target, as AWS would); the queue URL.
    async fn pipe(&self, bus: &str, queue: &str) -> String {
        self.eb("CreateEventBus", json!({"Name": bus})).await;
        self.sqs("CreateQueue", json!({"QueueName": queue})).await;
        let url = format!("{}/123456789012/{queue}", self.root);
        self.eb(
            "PutRule",
            json!({"Name": format!("{bus}-rule"), "EventBusName": bus, "EventPattern": "{}"}),
        )
        .await;
        self.eb(
            "PutTargets",
            json!({"EventBusName": bus, "Rule": format!("{bus}-rule"),
                   "Targets": [{"Id": "sqs",
                                "Arn": format!("arn:aws:sqs:us-east-1:123456789012:{queue}")}]}),
        )
        .await;
        url
    }

    async fn count(&self, url: &str) -> u64 {
        let v = self
            .sqs(
                "GetQueueAttributes",
                json!({"QueueUrl": url, "AttributeNames": ["ApproximateNumberOfMessages"]}),
            )
            .await;
        v["Attributes"]["ApproximateNumberOfMessages"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// The messages of the queue as the service holds them, each received once (received
    /// messages stay hidden): `Body` as written and the attributes (`MessageGroupId` and
    /// `MessageDeduplicationId` of a FIFO subscription among them).
    async fn receive(&self, url: &str) -> Vec<Value> {
        let mut out = Vec::new();
        loop {
            let v = self
                .sqs(
                    "ReceiveMessage",
                    json!({"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0,
                           "VisibilityTimeout": 600, "AttributeNames": ["All"]}),
                )
                .await;
            let got = v
                .get("Messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if got.is_empty() {
                return out;
            }
            out.extend(got);
        }
    }

    /// The event envelopes of the queue, each message received once (received messages stay
    /// hidden): what the rule with its SQS target delivered.
    async fn drain(&self, url: &str) -> Vec<Value> {
        self.receive(url)
            .await
            .iter()
            .map(|m| serde_json::from_str(m["Body"].as_str().unwrap()).unwrap())
            .collect()
    }
}

fn eventbridge_creds() -> String {
    std::env::set_var("LWF_TEST_EVENTBRIDGE_AK", "lazywf");
    std::env::set_var("LWF_TEST_EVENTBRIDGE_SK", "lazywfsecret");
    json!({"access_key_ref": "env:LWF_TEST_EVENTBRIDGE_AK",
           "secret_key_ref": "env:LWF_TEST_EVENTBRIDGE_SK"})
    .to_string()
}

/// A customers table (with a `rev` column) and the rule `bus` that puts each changed row to
/// `bus` (one PutEvents per flush, `batch_max_keys` at the documented maximum of 10).
async fn eventbridge_rule(db: &Db, root: &str, bus: &str, template: &str, selects: &str) {
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL)")
        .await;
    tables(db).await;
    db.target(
        "bus",
        "eventbridge",
        &format!("aws-eventbridge://us-east-1/{bus}?endpoint={root}"),
        Some("JSON_EXTRACT(body, '$.EventId') IS NOT NULL"),
        &eventbridge_creds(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('bus', 'bus', 'batch', 10, ?, ?, 100)",
        (selects, template),
    )
    .await;
    db.trigger("bus", "customers", Some("{{new.id}}"), None)
        .await;
}

/// Every changed key arrives once, as an event on the bus: `DetailType` is the key, `Detail`
/// the document, read back through the rule with its SQS target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventbridge_every_changed_key_arrives_as_an_event() {
    let root = need_db!();
    let endpoint = need_eventbridge!();
    let moto = Moto::new(&endpoint).await;
    let bus = format!("lwf-eb-every-{}", std::process::id());
    let db = Db::fresh(&root, "eb_every").await;
    let url = moto.pipe(&bus, &bus.replace('-', "_")).await;
    eventbridge_rule(
        &db,
        &endpoint,
        &bus,
        EB_PUT_EVENTS,
        "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec(
        "INSERT INTO customers (id, name) VALUES (1,'Ann'),(2,'Bob'),(3,'Cy'),(4,'Di'),(5,'Ed')",
    )
    .await;
    wait_for(Duration::from_secs(20), "five events", || async {
        (moto.count(&url).await == 5).then_some(())
    })
    .await;
    sleep_ms(2000).await;
    assert_eq!(moto.count(&url).await, 5, "a key was sent twice");
    let mut events = moto.drain(&url).await;
    assert_eq!(events.len(), 5);
    events.sort_by_key(|e| e["detail-type"].as_str().unwrap_or("").to_string());
    let names = ["Ann", "Bob", "Cy", "Di", "Ed"];
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e["source"], "lazywf", "the source of the event");
        assert_eq!(
            e["detail-type"],
            Value::from((i + 1).to_string()),
            "the detail-type is the row key"
        );
        assert_eq!(
            e["detail"],
            json!({"id": i + 1, "name": names[i]}),
            "the detail is the document"
        );
    }
    assert!(
        db.status("bus").await.and_then(|s| s.3).is_none(),
        "accepted entries left an error"
    );
    inst.stop().await;
}

/// One entry of a batch carries a `Detail` that is not JSON: the service refuses that entry
/// (`MalformedDetail`) and the other entries of the same request are delivered; only the
/// refused key drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventbridge_refuses_a_bad_entry_and_delivers_the_good_ones() {
    let root = need_db!();
    let endpoint = need_eventbridge!();
    let moto = Moto::new(&endpoint).await;
    let bus = format!("lwf-eb-bad-{}", std::process::id());
    let db = Db::fresh(&root, "eb_bad").await;
    let url = moto.pipe(&bus, &bus.replace('-', "_")).await;
    eventbridge_rule(
        &db,
        &endpoint,
        &bus,
        EB_PUT_EVENTS_BAD_DETAIL,
        "SELECT c.id AS _key, c.id, c.name, c.rev FROM customers c WHERE c.id IN (:keys)",
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    // Key 4 is the one whose `Detail` renders as `not-json` (its `rev` is NULL).
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1),(3,'Cy',1),(4,'Di',NULL)")
        .await;
    wait_for(Duration::from_secs(30), "three events", || async {
        (moto.count(&url).await == 3).then_some(())
    })
    .await;
    wait_for(Duration::from_secs(30), "the refused entry", || async {
        db.status("bus")
            .await
            .and_then(|s| s.3)
            .filter(|e| e.contains("MalformedDetail"))
            .map(|_| ())
    })
    .await;
    sleep_ms(2000).await;
    assert_eq!(moto.count(&url).await, 3, "the refused key was sent");
    let mut keys: Vec<String> = moto
        .drain(&url)
        .await
        .iter()
        .map(|e| e["detail-type"].as_str().unwrap_or("").to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, ["1", "2", "3"], "the good entries went through");
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// `sns` targets, against moto (`LAZYWF_TEST_SNS`, e.g. http://127.0.0.1:34569; the `sns` compose
// profile). One action is one `PublishBatch` in the query protocol: a form body whose entries are
// `PublishBatchRequestEntries.member.<n>.<Field>` with `n` from 1 (`{{add @index 1}}`), and an XML
// answer that says per entry what happened. The topic is read back the way AWS delivers: a
// subscription of an SQS queue on the topic, the queue drained through the SQS API, which gives
// the notification envelope with the published message in its `Message` field.

const SNS_PUBLISH_BATCH: &str = r#"POST / HTTP/1.1
Content-Type: application/x-www-form-urlencoded

Action=PublishBatch&Version=2010-03-31&TopicArn={{url_encode endpoint.topic_arn}}{{#each docs}}&PublishBatchRequestEntries.member.{{add @index 1}}.Id={{@index}}&PublishBatchRequestEntries.member.{{add @index 1}}.Message={{url_encode (json doc)}}{{/each}}"#;

/// The same, for a FIFO topic: every entry carries the key as its `MessageGroupId` and the row's
/// `version` as its `MessageDeduplicationId` (both required of a FIFO topic's messages).
const SNS_PUBLISH_FIFO: &str = r#"POST / HTTP/1.1
Content-Type: application/x-www-form-urlencoded

Action=PublishBatch&Version=2010-03-31&TopicArn={{url_encode endpoint.topic_arn}}{{#each docs}}&PublishBatchRequestEntries.member.{{add @index 1}}.Id={{@index}}&PublishBatchRequestEntries.member.{{add @index 1}}.Message={{url_encode (json doc)}}&PublishBatchRequestEntries.member.{{add @index 1}}.MessageGroupId={{url_encode key}}&PublishBatchRequestEntries.member.{{add @index 1}}.MessageDeduplicationId={{url_encode doc.version}}{{/each}}"#;

/// The same without the `MessageGroupId`, which a FIFO topic requires: the target is to refuse it
/// before the request goes out (`sns::entries_ok`).
const SNS_PUBLISH_NO_GROUP: &str = r#"POST / HTTP/1.1
Content-Type: application/x-www-form-urlencoded

Action=PublishBatch&Version=2010-03-31&TopicArn={{url_encode endpoint.topic_arn}}{{#each docs}}&PublishBatchRequestEntries.member.{{add @index 1}}.Id={{@index}}&PublishBatchRequestEntries.member.{{add @index 1}}.Message={{url_encode (json doc)}}&PublishBatchRequestEntries.member.{{add @index 1}}.MessageDeduplicationId={{url_encode doc.version}}{{/each}}"#;

fn sns_root() -> Option<String> {
    std::env::var("LAZYWF_TEST_SNS")
        .ok()
        .filter(|v| !v.is_empty())
}

macro_rules! need_sns {
    () => {
        match sns_root() {
            Some(u) => u,
            None => {
                eprintln!(
                    "LAZYWF_TEST_SNS not set: skipping (docker compose --profile sns up -d moto)"
                );
                return;
            }
        }
    };
}

impl Moto {
    /// One call of the query protocol (`Action=…&Version=…`, the `param.n` notation), which is
    /// how the SNS API documents itself; the status and the answer's text. The values of these
    /// calls hold no `&` or `=`, so they go in as they are.
    async fn query(&self, service: &str, params: &[(String, String)]) -> (u16, String) {
        let mut pairs: Vec<String> = Vec::new();
        for (k, v) in params {
            pairs.push(format!("{k}={v}"));
        }
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: pairs.join("&").into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header(
            "Content-Type",
            "application/x-www-form-urlencoded".to_string(),
        );
        req.set_header(
            "Authorization",
            format!("AWS4-HMAC-SHA256 Credential=lazywf/20260101/us-east-1/{service}/aws4_request, SignedHeaders=content-type;host, Signature=00"),
        );
        let resp = self
            .client
            .send(&req, Duration::from_secs(10))
            .await
            .unwrap();
        (
            resp.status,
            String::from_utf8_lossy(&resp.body).into_owned(),
        )
    }

    async fn sns(&self, action: &str, params: &[(&str, &str)]) -> String {
        let mut pairs: Vec<(String, String)> = vec![
            ("Action".to_string(), action.to_string()),
            ("Version".to_string(), "2010-03-31".to_string()),
        ];
        pairs.extend(params.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let (status, xml) = self.query("sns", &pairs).await;
        assert_eq!(status, 200, "SNS {action}: {xml}");
        xml
    }

    /// The text of the first `<tag>…</tag>` of an answer.
    fn tag(xml: &str, name: &str) -> String {
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        let from = xml
            .find(&open)
            .unwrap_or_else(|| panic!("no {open} in {xml}"))
            + open.len();
        let end = xml[from..]
            .find(&close)
            .unwrap_or_else(|| panic!("no {close} in {xml}"));
        xml[from..from + end].to_string()
    }

    /// Creates the topic (a FIFO one when `name` ends in `.fifo`) and returns its ARN.
    async fn create_topic(&self, name: &str) -> String {
        let mut params: Vec<(&str, &str)> = vec![("Name", name)];
        if name.ends_with(".fifo") {
            params.push(("Attributes.entry.1.key", "FifoTopic"));
            params.push(("Attributes.entry.1.value", "true"));
        }
        let xml = self.sns("CreateTopic", &params).await;
        Self::tag(&xml, "TopicArn")
    }

    /// Creates the queue (FIFO when `fifo`) and returns its URL, as the read-back of a topic.
    async fn create_queue(&self, name: &str, fifo: bool) -> String {
        let body = if fifo {
            json!({"QueueName": name, "Attributes": {"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
        } else {
            json!({"QueueName": name})
        };
        self.sqs("CreateQueue", body).await;
        format!("{}/123456789012/{name}", self.root)
    }

    /// Subscribes the queue (by its ARN) to the topic: what AWS then delivers to the queue.
    async fn subscribe(&self, topic_arn: &str, queue_arn: &str) {
        self.sns(
            "Subscribe",
            &[
                ("TopicArn", topic_arn),
                ("Protocol", "sqs"),
                ("Endpoint", queue_arn),
            ],
        )
        .await;
    }

    /// The topic with one queue subscribed to it: the ARN (what the target publishes to) and the
    /// queue URL (what the test drains).
    async fn pipe_topic(&self, topic: &str, queue: &str, fifo: bool) -> (String, String) {
        let arn = self.create_topic(topic).await;
        let url = self.create_queue(queue, fifo).await;
        self.subscribe(&arn, &format!("arn:aws:sqs:us-east-1:123456789012:{queue}"))
            .await;
        (arn, url)
    }
}

fn sns_vars(extra: Option<(&str, Value)>) -> String {
    std::env::set_var("LWF_TEST_SNS_AK", "lazywf");
    std::env::set_var("LWF_TEST_SNS_SK", "lazywfsecret");
    let mut vars = json!({"access_key_ref": "env:LWF_TEST_SNS_AK",
                          "secret_key_ref": "env:LWF_TEST_SNS_SK"});
    if let Some((k, v)) = extra {
        vars.as_object_mut().unwrap().insert(k.into(), v);
    }
    vars.to_string()
}

/// A customers table (with `rev` and a `payload`) and the rule `topic` that publishes each
/// changed row to `topic` (one PublishBatch per flush, `batch_max_keys` at the documented
/// maximum of 10). `extra` adds target vars (a raised `maximum_message_size`).
async fn sns_rule(
    db: &Db,
    root: &str,
    topic: &str,
    template: &str,
    selects: &str,
    extra: Option<(&str, Value)>,
) {
    db.exec(
        "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), rev INT NULL, payload MEDIUMTEXT NULL)",
    )
    .await;
    tables(db).await;
    db.target(
        "topic",
        "sns",
        &format!("aws-sns://us-east-1/123456789012/{topic}?endpoint={root}"),
        Some("JSON_EXTRACT(body, '$.MessageId') IS NOT NULL"),
        &sns_vars(extra),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('topic', 'topic', 'batch', 10, ?, ?, 100)",
        (selects, template),
    )
    .await;
    db.trigger("topic", "customers", Some("{{new.id}}"), None)
        .await;
}

/// The documents of the messages the queue holds, in key order: what a subscription of the topic
/// delivered, each message received once. A message is the document, as the template published it.
async fn published(moto: &Moto, url: &str) -> Vec<Value> {
    let mut docs: Vec<Value> = moto
        .drain(url)
        .await
        .iter()
        .map(|e| {
            let message = e["Message"].as_str().unwrap_or_default();
            serde_json::from_str(message)
                .unwrap_or_else(|e| panic!("the message is not the document: {message}: {e}"))
        })
        .collect();
    docs.sort_by_key(|d| d["id"].as_i64().unwrap_or(0));
    docs
}

/// The documents of the FIFO messages the queue holds, with what the subscription carried of
/// each: `(document, MessageGroupId, MessageDeduplicationId)`, in key order.
async fn published_fifo(moto: &Moto, url: &str) -> Vec<(Value, String, String)> {
    let mut docs: Vec<(Value, String, String)> = moto
        .receive(url)
        .await
        .iter()
        .map(|e| {
            let envelope = e["Body"].as_str().unwrap_or_default();
            let envelope: Value = serde_json::from_str(envelope)
                .unwrap_or_else(|err| panic!("the body is not a notification: {envelope}: {err}"));
            let message = envelope["Message"].as_str().unwrap_or_default();
            (
                serde_json::from_str(message).unwrap_or_else(|err| {
                    panic!("the message is not the document: {message}: {err}")
                }),
                e["Attributes"]["MessageGroupId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                e["Attributes"]["MessageDeduplicationId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
        })
        .collect();
    docs.sort_by_key(|(d, _, _)| d["id"].as_i64().unwrap_or(0));
    docs
}

/// Every changed key arrives once, as a message of the topic: the document is the message, read
/// back through the SQS subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_every_changed_key_arrives_as_a_message() {
    let root = need_db!();
    let endpoint = need_sns!();
    let moto = Moto::new(&endpoint).await;
    let topic = format!("lwf-sns-every-{}", std::process::id());
    let queue = topic.replace('-', "_");
    let db = Db::fresh(&root, "sns_every").await;
    let (_arn, url) = moto.pipe_topic(&topic, &queue, false).await;
    sns_rule(
        &db,
        &endpoint,
        &topic,
        SNS_PUBLISH_BATCH,
        "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
        None,
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec(
        "INSERT INTO customers (id, name) VALUES (1,'Ann'),(2,'Bob'),(3,'Cy'),(4,'Di'),(5,'Ed')",
    )
    .await;
    wait_for(Duration::from_secs(20), "five messages", || async {
        (moto.count(&url).await == 5).then_some(())
    })
    .await;
    sleep_ms(2000).await;
    assert_eq!(moto.count(&url).await, 5, "a key was sent twice");
    let names = ["Ann", "Bob", "Cy", "Di", "Ed"];
    let docs = published(&moto, &url).await;
    assert_eq!(docs.len(), 5);
    for (i, d) in docs.iter().enumerate() {
        assert_eq!(
            *d,
            json!({"id": i as i64 + 1, "name": names[i]}),
            "the message is the document of its key"
        );
    }
    assert!(
        db.status("topic").await.and_then(|s| s.3).is_none(),
        "accepted entries left an error"
    );
    inst.stop().await;
}

/// One message of a batch is larger than the topic takes (its document holds a 300 KB `payload`):
/// the service refuses that entry (`InvalidParameter`: Message too long) while the other entries
/// of the same request are delivered, and only the refused key drops. The target states the
/// topic's `MaximumMessageSize` as 1 MiB, which is what lets the large message be sent at all; the
/// topic's own attribute is the default 256 KiB, so it is the service's answer that ends that key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_batch_with_one_message_too_long_refuses_only_that_key() {
    let root = need_db!();
    let endpoint = need_sns!();
    let moto = Moto::new(&endpoint).await;
    let topic = format!("lwf-sns-long-{}", std::process::id());
    let queue = topic.replace('-', "_");
    let db = Db::fresh(&root, "sns_long").await;
    let (_arn, url) = moto.pipe_topic(&topic, &queue, false).await;
    sns_rule(
        &db,
        &endpoint,
        &topic,
        SNS_PUBLISH_BATCH,
        "SELECT c.id AS _key, c.id, c.name, c.payload FROM customers c WHERE c.id IN (:keys)",
        Some(("maximum_message_size", json!(1_048_576))),
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    // Key 4 is the one whose message is over the topic's 256 KiB.
    db.exec(
        "INSERT INTO customers (id, name, payload) VALUES \
         (1,'Ann','a'),(2,'Bob','b'),(3,'Cy','c'),(4,'Di',REPEAT('x',300000))",
    )
    .await;
    wait_for(Duration::from_secs(30), "three messages", || async {
        (moto.count(&url).await == 3).then_some(())
    })
    .await;
    wait_for(Duration::from_secs(30), "the refused entry", || async {
        db.status("topic")
            .await
            .and_then(|s| s.3)
            .filter(|e| e.contains("Message too long"))
            .map(|_| ())
    })
    .await;
    sleep_ms(2000).await;
    assert_eq!(moto.count(&url).await, 3, "the refused key was sent");
    let ids: Vec<i64> = published(&moto, &url)
        .await
        .iter()
        .map(|d| d["id"].as_i64().unwrap_or(0))
        .collect();
    assert_eq!(ids, [1, 2, 3], "the good entries went through");
    inst.stop().await;
}

/// A FIFO topic: every entry carries its `MessageGroupId` (the key) and its
/// `MessageDeduplicationId` (the row's `version`), and the batch goes out and arrives once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_fifo_topic_delivers_each_key_once() {
    let root = need_db!();
    let endpoint = need_sns!();
    let moto = Moto::new(&endpoint).await;
    let topic = format!("lwf-sns-fifo-{}.fifo", std::process::id());
    let queue = topic.replace('-', "_");
    let db = Db::fresh(&root, "sns_fifo").await;
    let (_arn, url) = moto.pipe_topic(&topic, &queue, true).await;
    sns_rule(
        &db,
        &endpoint,
        &topic,
        SNS_PUBLISH_FIFO,
        "SELECT c.id AS _key, c.id, c.name, c.rev AS version FROM customers c WHERE c.id IN (:keys)",
        None,
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    // Every row has its own version: the emulator's FIFO queue deduplicates by
    // `MessageDeduplicationId` alone, where AWS scopes the dedup to the message group (the key),
    // so four rows at version 1 would arrive as one message here. Distinct versions are what a
    // stream of updates holds anyway, and they let the test tell the group from the dedup id.
    db.exec(
        "INSERT INTO customers (id, name, rev) VALUES (1,'Ann',7),(2,'Bob',8),(3,'Cy',9),(4,'Di',10)",
    )
    .await;
    wait_for(Duration::from_secs(20), "four messages", || async {
        (moto.count(&url).await == 4).then_some(())
    })
    .await;
    sleep_ms(2000).await;
    assert_eq!(moto.count(&url).await, 4, "a key was sent twice");
    let names = ["Ann", "Bob", "Cy", "Di"];
    let got = published_fifo(&moto, &url).await;
    for (i, (doc, group, dedup)) in got.iter().enumerate() {
        let key = i as i64 + 1;
        assert_eq!(
            *doc,
            json!({"id": key, "name": names[i], "version": 6 + key}),
            "the message is the document of its key"
        );
        // What the two fields of the entry are for: the key keeps the messages of one row in
        // order, the row's version is what a redelivery of the same change is deduplicated by.
        assert_eq!(
            group,
            &key.to_string(),
            "the group of a FIFO entry is its key"
        );
        assert_eq!(
            dedup,
            &(6 + key).to_string(),
            "the dedup id of a FIFO entry is the row's version"
        );
    }
    assert!(
        db.status("topic").await.and_then(|s| s.3).is_none(),
        "accepted entries left an error"
    );
    inst.stop().await;
}

/// A FIFO entry without its `MessageGroupId` is refused where it is rendered, before the request
/// goes out: the status names the field, and nothing reaches the topic (a request that went out
/// would answer `InvalidParameter` instead).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_fifo_entry_without_a_group_id_is_refused_before_it_is_sent() {
    let root = need_db!();
    let endpoint = need_sns!();
    let moto = Moto::new(&endpoint).await;
    let topic = format!("lwf-sns-nogroup-{}.fifo", std::process::id());
    let queue = topic.replace('-', "_");
    let db = Db::fresh(&root, "sns_nogroup").await;
    let (_arn, url) = moto.pipe_topic(&topic, &queue, true).await;
    sns_rule(
        &db,
        &endpoint,
        &topic,
        SNS_PUBLISH_NO_GROUP,
        "SELECT c.id AS _key, c.id, c.name, c.rev AS version FROM customers c WHERE c.id IN (:keys)",
        None,
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers (id, name, rev) VALUES (1,'Ann',1),(2,'Bob',1)")
        .await;
    wait_for(Duration::from_secs(30), "the refusal", || async {
        db.status("topic")
            .await
            .and_then(|s| s.3)
            .filter(|e| e.contains("MessageGroupId"))
            .map(|_| ())
    })
    .await;
    let error = db.status("topic").await.and_then(|s| s.3).unwrap();
    assert!(
        error.contains("non-empty value"),
        "the refusal says what is missing: {error}"
    );
    assert_eq!(moto.count(&url).await, 0, "a refused batch was sent");
    inst.stop().await;
}

// ---------------------------------------------------------------------------------------------
// The pgoutput SOURCE (issue #57): a PostgreSQL publisher with wal_level=logical — the compose
// `pgsrc` service (`LAZYWF_TEST_PG=1`, `make test-e2e-pgsrc`). lazyview integrates PostgreSQL as
// a replication source and lazywf copies the reader when it lands; these tests prepare the
// matrix. They drive the publisher through SQL only (the slot functions of the server itself),
// never the replication protocol: the walsender transport (START_REPLICATION, CopyData 'w'/'k'
// framing, keepalives, standby status updates) belongs to the replication client crate that
// arrives with the engine PR, and no such client lives in src/ (the reader code is copy-set-only,
// CLAUDE.md). src/pgoutput.rs decodes the payloads these peek rows carry.

/// The publisher's DSN when the suite has one (`LAZYWF_TEST_PG=1`); `LAZYWF_TEST_PG_DSN` points
/// it somewhere other than the compose `pgsrc` service.
fn pg_src_dsn() -> Option<String> {
    match std::env::var("LAZYWF_TEST_PG")
        .ok()
        .filter(|v| !v.is_empty())
    {
        Some(_) => Some(std::env::var("LAZYWF_TEST_PG_DSN").unwrap_or_else(|_| {
            "host=127.0.0.1 port=34532 user=lwfsrc password=lwfsrcpw dbname=lwfsrc".to_string()
        })),
        None => {
            eprintln!("LAZYWF_TEST_PG not set: skipping (make test-e2e-pgsrc)");
            None
        }
    }
}

macro_rules! need_pg_src {
    () => {
        match pg_src_dsn() {
            Some(d) => d,
            None => return,
        }
    };
}

/// The test table, its publication and the peeked slot: names of these tests only, created and
/// dropped by them (a leftover from an interrupted run is dropped at the start).
const PG_SRC_TABLE: &str = "pgsrc_items";
const PG_SRC_PUB: &str = "lwf_pgsrc_pub";
const PG_SRC_SLOT: &str = "lwf_pgsrc_peek";

/// The publisher as the replication role (docker/pgsrc/init.sql).
async fn pg_src_client(dsn: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("the pgsrc publisher: compose profile `pgsrc` (make test-e2e-pgsrc)");
    tokio::spawn(conn);
    client
}

/// The pgoutput payloads the slot holds, without consuming them: one row per message, `data`
/// the payload bytes verbatim (the OUT columns are `slot_name, lsn` — the aliases pin the
/// names, which differ between minor lines). The variadic options are what the replication
/// client will pass in START_REPLICATION: protocol version 1, the one publication.
async fn pg_peek(pg: &tokio_postgres::Client) -> Vec<tokio_postgres::Row> {
    let slot = PG_SRC_SLOT.to_string();
    let sql = format!(
        "SELECT lsn::text, xid::text, data \
           FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, \
                'proto_version', '1', 'publication_names', '{PG_SRC_PUB}') AS t(lsn, xid, data)"
    );
    pg.query(&sql, &[&slot]).await.unwrap()
}

/// The message kinds of the peeked payloads decoded by the vendored reader: every payload
/// must parse, and a parse error here is a decoder bug found against a real PostgreSQL
/// publisher — report it (issue #57), never loosen this.
fn pg_decode(rows: &[tokio_postgres::Row]) -> Vec<lazy_wire_fire::pgoutput::Message<'_>> {
    rows.iter()
        .map(|r| {
            let data = r.get::<_, &[u8]>(2);
            lazy_wire_fire::pgoutput::parse(data).unwrap_or_else(|e| {
                panic!("the decoder refused a real publisher's payload ({e}): {data:?}")
            })
        })
        .collect()
}

/// The cells of a tuple as text (an INSERT of plain columns sends only `'t'` values).
fn pg_tuple(t: &lazy_wire_fire::pgoutput::Tuple) -> Vec<String> {
    t.columns()
        .map(
            |c| match c.expect("the tuple cells of a parsed message decode") {
                lazy_wire_fire::pgoutput::Column::Text(v) => {
                    String::from_utf8_lossy(v).into_owned()
                }
                other => panic!("a non-text cell in an INSERT: {other:?}"),
            },
        )
        .collect()
}

/// The publisher is what the pgoutput plugin needs: `wal_level=logical`, a role that may hold
/// slots, and the walsender/slot budgets the reader will use.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_publisher_is_a_logical_decoder() {
    let dsn = need_pg_src!();
    let pg = pg_src_client(&dsn).await;
    let wal_level: String = pg
        .query_one("SELECT current_setting('wal_level')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        wal_level, "logical",
        "the pgoutput plugin needs wal_level=logical"
    );
    let row = pg
        .query_one(
            "SELECT (SELECT rolreplication FROM pg_roles WHERE rolname = current_user), \
                    current_setting('max_replication_slots')::int, \
                    current_setting('max_wal_senders')::int",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.get::<_, bool>(0),
        "the test role connects without REPLICATION: docker/pgsrc/init.sql"
    );
    assert!(
        row.get::<_, i32>(1) >= 16,
        "max_replication_slots = {}",
        row.get::<_, i32>(1)
    );
    assert!(
        row.get::<_, i32>(2) >= 16,
        "max_wal_senders = {}",
        row.get::<_, i32>(2)
    );
}

/// The SQL half of the reader's future feed: a publication, a pgoutput slot, a commit — and the
/// peek returns it as the BEGIN, RELATION, INSERT, COMMIT payloads `src/pgoutput.rs` decodes
/// (the data column of `pg_logical_slot_peek_binary_changes` is the plugin payload verbatim,
/// the bytes XLogData carries). The drop is clean: no leftover slot pins WAL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_slot_peek_yields_begin_insert_commit() {
    let dsn = need_pg_src!();
    let pg = pg_src_client(&dsn).await;
    // From nothing: drop what an interrupted run may have left (the slot last; it holds WAL,
    // not locks, so the table and publication go first and freely).
    pg.batch_execute(&format!(
        "DROP PUBLICATION IF EXISTS {PG_SRC_PUB};
         DROP TABLE IF EXISTS {PG_SRC_TABLE};
         SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
          WHERE slot_name = '{PG_SRC_SLOT}'"
    ))
    .await
    .unwrap();
    // Table, then publication, then slot: the slot's consistent point is after both, so every
    // change below it is one the publisher may decode.
    pg.batch_execute(&format!(
        "CREATE TABLE {PG_SRC_TABLE} (id INT PRIMARY KEY, note TEXT);
         CREATE PUBLICATION {PG_SRC_PUB} FOR TABLE {PG_SRC_TABLE}"
    ))
    .await
    .unwrap();
    let slot = PG_SRC_SLOT.to_string();
    let row = pg
        .query_one(
            "SELECT slot_name::text, lsn::text \
               FROM pg_create_logical_replication_slot($1, 'pgoutput', false)",
            &[&slot],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), PG_SRC_SLOT);
    assert!(
        row.get::<_, String>(1).contains('/'),
        "the consistent point is no LSN"
    );
    // One transaction, two rows: the INSERT two-row statement is a single commit record.
    pg.batch_execute(&format!(
        "INSERT INTO {PG_SRC_TABLE} VALUES (1, 'gift'), (2, 'express')"
    ))
    .await
    .unwrap();

    use lazy_wire_fire::pgoutput::{replica_identity, Message};

    // Not the kind bytes: the payloads through the vendored decoder, so the assertions are on
    // what the reader will actually consume. An Err from `parse` is a decoder bug found against
    // a real PostgreSQL 17 publisher (issue #57) — report it, never loosen this.
    let rows = pg_peek(&pg).await;
    let msgs = pg_decode(&rows);
    // The shape of one committed two-row INSERT: the transaction opens with BEGIN and closes
    // with COMMIT, the RELATION describes the table before the INSERTs name it, and the INSERTs
    // come one per row, in the commit's order. (A fresh publisher sends nothing else: no origin,
    // no Type.)
    let [Message::Begin {
        final_lsn,
        xid: begin_xid,
        ..
    }, Message::Relation(rel), Message::Insert {
        rel_id: r1,
        new: t1,
    }, Message::Insert {
        rel_id: r2,
        new: t2,
    }, Message::Commit { commit_lsn, .. }] = &msgs[..]
    else {
        panic!("the peek of the committed two-row INSERT decoded to: {msgs:?}");
    };

    // The RELATION describes the table we created, replica identity DEFAULT (the primary key,
    // which is what an INSERT needs no old tuple for), columns in declaration order with only
    // the key flagged.
    assert_eq!(rel.namespace, "public", "the relation's namespace");
    assert_eq!(rel.name, PG_SRC_TABLE, "the relation's name");
    assert_eq!(
        rel.replica_identity,
        replica_identity::DEFAULT,
        "the relation's replica identity"
    );
    let cols: Vec<_> = rel
        .columns()
        .map(|c| c.expect("the column descriptors of a parsed RELATION decode"))
        .collect();
    let names: Vec<&str> = cols.iter().map(|c| c.name).collect();
    assert_eq!(names, ["id", "note"], "the relation's columns, in order");
    assert!(cols[0].key, "the primary key column is not flagged");
    assert!(!cols[1].key, "the plain column is flagged as a key");

    // The two rows, decoded: the values we inserted, cell by cell, and they name this relation.
    assert_eq!(pg_tuple(t1), ["1", "gift"], "the first INSERT's new tuple");
    assert_eq!(
        pg_tuple(t2),
        ["2", "express"],
        "the second INSERT's new tuple"
    );
    assert_eq!(r1, &rel.rel_id, "the first INSERT names another relation");
    assert_eq!(r2, &rel.rel_id, "the second INSERT names another relation");

    // BEGIN and COMMIT describe the same commit record.
    assert_eq!(
        final_lsn, commit_lsn,
        "the BEGIN's final LSN is not the COMMIT's LSN"
    );

    // One transaction: one xid on every SQL row, not the zero xid of a non-transactional
    // change, and the BEGIN carries that same id.
    let xid = rows[0].get::<_, String>(1);
    assert_ne!(xid, "0");
    assert!(
        rows.iter().all(|r| r.get::<_, String>(1) == xid),
        "the rows do not belong to one transaction"
    );
    assert_eq!(
        begin_xid.to_string(),
        xid,
        "the decoded BEGIN's xid is not the one the publisher reported"
    );

    // Peeking does not consume: the same messages, decoded, again unchanged.
    assert_eq!(
        pg_decode(&pg_peek(&pg).await),
        msgs,
        "peeking advanced the slot"
    );

    // Drop the slot cleanly and confirm it is gone: a leftover slot pins WAL forever.
    pg.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .unwrap();
    let left: i64 = pg
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0, "the slot survived pg_drop_replication_slot");
    pg.batch_execute(&format!(
        "DROP PUBLICATION {PG_SRC_PUB}; DROP TABLE {PG_SRC_TABLE}"
    ))
    .await
    .unwrap();
}

/// The piece that is missing: real XLogData bytes from a walsender, end to end — START_REPLICATION
/// over a replication connection, the CopyData `'w'`/`'k'` framing, keepalives and standby status
/// updates, feeding `src/pgoutput.rs` with the bytes of a live stream (the peeked payloads above
/// prove the content; this proves the transport carries them). It belongs to the engine PR that
/// follows: the reader and the replication client (lazyview's pgwire-replication client) arrive
/// there, issue #57 — `make test-e2e-pgsrc` runs it with `--ignored` once they do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the replication client: it activates with the engine PR that lands the reader (issue #57)"]
async fn pg_decode_real_xlogdata_bytes_end_to_end() {
    let _dsn = need_pg_src!();
    unimplemented!("the walsender transport (START_REPLICATION, CopyData framing) is not here yet");
}

/// What `/metrics` says about DML the binlog carried as text: the sample value of
/// `lazywf_statement_events_ignored_total` in the exposition a scrape would be handed (the
/// endpoint's handler renders nothing else).
fn statement_events_ignored(m: &lazy_wire_fire::metrics::Metrics) -> u64 {
    m.render()
        .lines()
        .find_map(|l| l.strip_prefix("lazywf_statement_events_ignored_total "))
        .and_then(|v| v.trim().parse().ok())
        .expect("the counter is on the exposition")
}

/// A session that switches its own `binlog_format` to STATEMENT logs its DML as QUERY events, so
/// the rows are not in the stream: the rules reading the table miss the change and their targets
/// stay wrong until they are rebuilt. The engine must not drop such an event without a trace — one
/// `lazywf_statement_events_ignored_total` count per event, one WARN per table per interval with
/// the kind, the schema, the table and the binlog position, and never the statement text, which in
/// statement format carries the row values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statement_format_dml_is_counted_and_warned() {
    let root = need_db!();
    let db = Db::fresh(&root, "stmt_dml").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, r#"{"errors":false,"items":[]}"#.into()));
    order_rule(&db, &mock).await;
    let metrics = Arc::new(lazy_wire_fire::metrics::Metrics::default());
    let inst = Instance::ready_with_metrics(config(&db, &[]), metrics.clone()).await;

    // The baseline: a row-format change does reach the target, so a later silence about the
    // statement-format one means the rows were missing and not the rule asleep.
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (10, 1, 'new', 12.50)")
        .await;
    wait_for(
        Duration::from_secs(20),
        "orders 10 delivered from its row events",
        || async {
            last_docs(&mock.requests())
                .get("10")
                .cloned()
                .flatten()
                .filter(|d| d["status"] == "new")
        },
    )
    .await;
    inst.quiet().await;
    let before = statement_events_ignored(&metrics);

    // The twisted session: one connection, its own session only — a session variable, never the
    // server's. A server without the variable (MySQL 9 removed `binlog_format`) or without the
    // privilege for it says so, and the test skips with the reason rather than failing. A macro
    // and not a closure: the connection and the instance are each moved into the stop path, which
    // every one of the four reasons below takes exactly once.
    let mut t = db.conn().await;
    macro_rules! skip {
        ($why:expr) => {{
            eprintln!("statement-format DML: skipping, {}", $why);
            t.disconnect().await.ok();
            inst.stop().await;
            return;
        }};
    }
    let log_bin: i64 = t
        .query_first("SELECT @@log_bin")
        .await
        .unwrap_or(None)
        .unwrap_or(0);
    if log_bin == 0 {
        skip!("the server logs no binlog, so no QUERY event would arrive");
    }
    if let Err(e) = t
        .query_drop("SET SESSION binlog_format = 'STATEMENT'")
        .await
    {
        skip!(format!("the server refuses it for a session: {e}"));
    }
    let fmt: String = t
        .query_first("SELECT @@session.binlog_format")
        .await
        .unwrap_or(None)
        .unwrap_or_default();
    if !fmt.eq_ignore_ascii_case("STATEMENT") {
        skip!(format!("the session still reads binlog_format={fmt}"));
    }

    // Three statements from that one session: two on the table the rule reads (they owe one line
    // between them, the interval is far longer than this test) and one on another watched table
    // (its own line: the rate limit is per table). A marker in a value proves nothing of the
    // statement ever reaches a log or a target.
    mock.clear();
    for q in [
        "UPDATE orders SET status = 'twstmark' WHERE id = 10",
        "UPDATE orders SET total = 13.50 WHERE id = 10",
        "INSERT INTO customers VALUES (2, 'Bobmark')",
    ] {
        t.query_drop(q)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e} (binlog_format=STATEMENT)"));
    }
    t.disconnect().await.ok();
    inst.quiet().await;

    // Every one of the three events is counted, whatever the rate-limited lines do, on `/metrics`
    // and on `/status` alike.
    assert_eq!(
        statement_events_ignored(&metrics) - before,
        3,
        "one count per statement-format DML event"
    );
    assert_eq!(
        inst.health.status()["statement_events_ignored"],
        3,
        "the same count on /status"
    );
    // And the rows are gone: the rule never saw them, so the target still holds `new` at 12.50.
    sleep_ms(500).await;
    assert!(
        !mock
            .requests()
            .iter()
            .any(|r| r.body.contains("twstmark") || r.body.contains("Bobmark")),
        "a statement-format row reached the target: {:?}",
        mock.requests().iter().map(|r| &r.body).collect::<Vec<_>>()
    );

    // Both tables reported, each with the kind it was, and nothing of the statement in any line.
    // The wait is on those two lines having arrived — a loaded box is slow, not absent. What the
    // capture holds beyond them belongs to the engines of the tests running beside this one, which
    // is why the assertion below is on the shape of the lines and not on their number. The size of
    // the capture goes into the wording of the wait: a suite that times out here has to say
    // whether it saw no lines at all or lines it could not match.
    let captured = warnings().len();
    let warned = wait_for(
        Duration::from_secs(10),
        &format!("one WARN line per table ({captured} lines were captured at the start)"),
        || async {
            let w: Vec<String> = warnings()
                .into_iter()
                .filter(|l| l.contains(&db.schema) && l.contains("statement format"))
                .collect();
            (w.iter().any(|l| l.contains("table=orders"))
                && w.iter().any(|l| l.contains("table=customers")))
            .then_some(w)
        },
    )
    .await;
    // What the interval owes — one line per table per 60 s — is the unit tests' job
    // (`the_statement_dml_lines_are_rate_limited_per_table` and its neighbours), because a live
    // suite cannot pin it down: the tests run several engines over one process and one server, and
    // the WARN is for every table the stream carries and not only the watched ones, so every engine
    // alive at that moment reports the same statement from its own rate limit — and one that
    // started between my two statements on `orders` owes the second of them, at its own position.
    // What is provable across engines is the shape of the lines: the two tables, each named with
    // the kind it was, and nothing of the statement in any of them.
    // The lifetime is named because it has to be: with two reference parameters no elision rule
    // can decide which one a borrowed return came from, and rustc refuses rather than guessing
    // (E0106, which CI found in twelve seconds where my box found nothing at all). `line` is the
    // one it comes from — the fields are cut out of the captured line, while `name` only picks
    // which field and is gone by the time the slice is returned.
    fn field<'a>(line: &'a str, name: &str) -> &'a str {
        line.split(&format!("{name}="))
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .unwrap_or("")
    }
    let kinds: std::collections::BTreeSet<(&str, &str)> = warned
        .iter()
        .map(|l| (field(l, "kind"), field(l, "table")))
        .collect();
    assert_eq!(
        kinds,
        std::collections::BTreeSet::from([("UPDATE", "orders"), ("INSERT", "customers")]),
        "one line per table, each naming its kind and its table, and nothing else:\n{}",
        warned.join("\n")
    );
    for line in &warned {
        assert!(line.contains("WARN"), "not a WARN: {line}");
        assert!(
            line.contains(&format!("schema={}", db.schema)),
            "the schema missing: {line}"
        );
        let at = field(line, "at");
        assert!(at.contains(':'), "the binlog position missing: {line}");
        let count = field(line, "count");
        assert!(
            count.parse::<u64>().is_ok_and(|c| c >= 1),
            "the folded count missing or not a count: {line}"
        );
        assert!(
            !line.contains("twstmark") && !line.contains("Bobmark"),
            "a row value reached the log: {line}"
        );
        let upper = line.to_ascii_uppercase();
        assert!(
            !upper.contains("UPDATE ORDERS") && !upper.contains("INSERT INTO CUSTOMERS"),
            "the statement text reached the log: {line}"
        );
    }
    inst.stop().await;
}

// ---- control-plane resilience (coverage PR-1) -------------------------------------------------
//
// The failure paths of the control plane: `flush_job`'s fence and `keys_sql` failure arms, the
// control-connection heal (`Engine::heal_control`, `lock::reacquire`, the reconnect arm of the
// standby loop in `lock::acquire`), and the config-reload paths (`reload_config`'s error joins,
// the frozen rule, and the two retry timers: the 1 s reload retry and the 5 s `reset_requested`
// clear retry). Each test breaks something on purpose and asserts the failure happened FOR the
// stated reason (the error text, the captured WARN, what the mock did and did not receive) and
// that the recovery is real (the position moves, the keys arrive), never merely "no error".

/// The instance lock of a test engine: every test's `config()` runs the default instance.
fn instance_lock(db: &Db) -> String {
    format!("lazywf:{}:default", db.schema)
}

/// The connection id that holds `lock` (`None`: nobody does).
async fn lock_holder(db: &Db, lock: &str) -> Option<u64> {
    let mut c = db.conn().await;
    let r: Option<Option<u64>> = c
        .exec_first("SELECT IS_USED_LOCK(?)", (lock,))
        .await
        .unwrap();
    quit(c).await;
    r.flatten()
}

/// Whether the server still has a connection with this processlist id.
async fn conn_alive(db: &Db, id: u64) -> bool {
    let mut c = db.conn().await;
    let r: Option<i64> = c
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.processlist WHERE ID = ?",
            (id,),
        )
        .await
        .unwrap();
    quit(c).await;
    r.unwrap_or(0) > 0
}

/// The id holding `lock`, when that connection is still in the processlist: a session the server
/// has killed but not finished tearing down must not pass for a live holder.
async fn lock_holder_live(db: &Db, lock: &str) -> Option<u64> {
    let mut c = db.conn().await;
    let r: Option<Option<u64>> = c
        .exec_first(
            "SELECT h FROM (SELECT IS_USED_LOCK(?) AS h) t \
             WHERE t.h IN (SELECT ID FROM information_schema.processlist)",
            (lock,),
        )
        .await
        .unwrap();
    quit(c).await;
    r.flatten()
}

/// Kills one connection of this test's own engine by its processlist id.
async fn kill_conn(db: &Db, id: u64) {
    let mut c = db.conn().await;
    c.query_drop(format!("KILL {id}"))
        .await
        .unwrap_or_else(|e| panic!("KILL {id}: {e}"));
    quit(c).await;
}

/// The processlist rows (id, command) of the connections of one account.
async fn user_conns(db: &Db, user: &str) -> Vec<(u64, String)> {
    let mut c = db.conn().await;
    let r: Vec<(u64, Option<String>)> = c
        .query(format!(
            "SELECT ID, COMMAND FROM information_schema.processlist WHERE USER = '{user}'"
        ))
        .await
        .unwrap();
    quit(c).await;
    r.into_iter()
        .map(|(id, cmd)| (id, cmd.unwrap_or_default()))
        .collect()
}

/// Creates an account with the documented least privileges and returns a DSN to it.
async fn engine_user(db: &Db, user: &str, pw: &str, root: &str) -> String {
    let mariadb = {
        let mut c = db.conn().await;
        let v: String = c.query_first("SELECT VERSION()").await.unwrap().unwrap();
        quit(c).await;
        v.contains("MariaDB")
    };
    db.exec(&format!("DROP USER IF EXISTS '{user}'@'%'")).await;
    db.exec(&format!("CREATE USER '{user}'@'%' IDENTIFIED BY '{pw}'"))
        .await;
    db.exec(&format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT{} ON *.* TO '{user}'@'%'",
        if mariadb { ", BINLOG MONITOR" } else { "" }
    ))
    .await;
    db.exec(&format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON `{}`.* TO '{user}'@'%'",
        db.schema
    ))
    .await;
    let (scheme, rest) = root.split_once("://").unwrap();
    let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    format!(
        "{scheme}://{user}:{pw}@{host}/{}?prefer_socket=false",
        db.schema
    )
}

/// The captured WARN-and-above lines that contain every needle AND the schema of the `Db` the test
/// created. Where the line names its schema this is the filter the harness contract asks for
/// (`tests/common/mod.rs:276-279`), and it cannot drift from the schema the test really made: a
/// hand-typed schema in a needle that expects NO lines is the bad failure mode — it passes forever.
///
/// It is therefore only used where the line provably carries the schema. Of the lines this section
/// reads, the server's 1146 does (both servers): `Table 'lwf_ksql.items' doesn't exist` inside the
/// `keys_sql` WARN, `Table 'lwf_clr5s.lazy_wire_fire_rule' doesn't exist` inside the
/// `clearing reset_requested` one. These do NOT, so their tests attribute by `rule=` (or by the
/// account they created) instead: the 1045 behind `fence connection` names a user, the 1142 of the
/// reload WARN names `for table 'lazy_wire_fire_rule'`, and a rule that cannot compile names only
/// its trigger.
fn warn_lines_in(db: &Db, needles: &[&str]) -> Vec<String> {
    warn_lines(&[needles, &[&db.schema]].concat())
}

/// The captured WARN-and-above lines that contain every needle. The raw form, for the texts that
/// carry nothing of the test's own — in the arms this section reads there are exactly three, and
/// they are these (`src`, as of `8415d23`):
///
/// ```text
/// warn!("writing lazy_wire_fire_status: {e:#}")                    src/engine.rs:3241
/// warn!("reloading the configuration: {e:#}; retrying")            src/engine.rs:3230
/// warn!("standby: control connection lost ({e}); reconnecting")   src/lock.rs:44
/// ```
///
/// No needle can attribute those, so a test that reads one does it in two moves: `warn_baselines` /
/// `warn_grew` make the line carry a reason (a wording of [`DEAD_CONNECTION`], or an error text of
/// the test's own), and the claim itself is settled where the test does own the evidence — the
/// session gone from `information_schema.processlist`, the lock's holder being a live connection of
/// another id, the standby's own account having a new connection, the status row not having moved,
/// the key the mock ANSWERED. Every other WARN read here carries `rule = %name`, and a test's rule
/// names are its own: no other test in this file inserts a rule of that name.
fn warn_lines(needles: &[&str]) -> Vec<String> {
    warnings()
        .into_iter()
        .filter(|l| needles.iter().all(|n| l.contains(n)))
        .collect()
}

/// How a killed connection answers a statement on these servers. A test that proves an arm noticed
/// a dead connection ANDs one of these wordings into the WARN it waits for: "a WARN of this shape
/// appeared" is then "this statement died on a dead connection", not "the engine logged something".
const DEAD_CONNECTION: &[&str] = &[
    "connection closed",
    "Connection refused",
    "Broken pipe",
    "Connection reset by peer",
    "Lost connection",
    "gone away",
];

/// The captured-WARN counts for one needle set plus each wording of `reasons`, in that order. The
/// ring is process-wide (every engine in this binary logs into it, tests and their retries
/// included), so a test never matches history: it takes this baseline before it breaks something
/// and asks `warn_grew` whether one of the lines appeared after.
fn warn_baselines(needles: &[&str], reasons: &[&str]) -> Vec<usize> {
    reasons
        .iter()
        .map(|r| warn_lines(&[needles, &[*r]].concat()).len())
        .collect()
}

/// The first wording of `reasons` that has more matching lines than in `base` — the WARN that
/// appeared since `warn_baselines` — as the line itself, so a test asserts on what the engine
/// actually said rather than on the fact that something was logged.
fn warn_grew(needles: &[&str], reasons: &[&str], base: &[usize]) -> Option<String> {
    reasons.iter().zip(base).find_map(|(r, b)| {
        let matched = warn_lines(&[needles, &[*r]].concat());
        (matched.len() > *b)
            .then(|| matched.last().cloned())
            .flatten()
    })
}

/// Records the path of every request the mock ANSWERS, in order, and answers 2xx. The record is
/// the answer, not the arrival: a request the mock held and then reset is in `requests()` but
/// was never answered, so it is not here — which is the difference between "the engine sent the
/// step" and "the step was delivered", and what lets a test tell one run of a reset from another.
fn answer_log(mock: &Mock) -> Arc<Mutex<Vec<String>>> {
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let l = log.clone();
    mock.respond(move |r| {
        l.lock().unwrap().push(r.path.clone());
        (200, "{}".into())
    });
    log
}

/// How many times `path` was answered by this mock (see [`answer_log`]).
fn answered(log: &Arc<Mutex<Vec<String>>>, path: &str) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_str() == path)
        .count()
}

/// What the mock saw, for an assertion message: every request that arrived (a held one included,
/// which is how a test tells "sent" from "delivered") and every path that was answered.
fn traffic(mock: &Mock, log: &Arc<Mutex<Vec<String>>>) -> String {
    let reqs: Vec<String> = mock
        .requests()
        .iter()
        .map(|r| format!("{} {}", r.method, r.path))
        .collect();
    format!(
        "arrived {reqs:?} answered {:?}",
        log.lock().unwrap().clone()
    )
}

/// The engine started directly (as `Instance::with` does), with the parts a test drives by
/// hand: the shutdown sender, the health, and the engine's own answer once the task is done.
struct Direct {
    stop: watch::Sender<bool>,
    health: Arc<lazy_wire_fire::health::Health>,
    done: Arc<Mutex<Option<anyhow::Result<()>>>>,
    task: tokio::task::JoinHandle<()>,
}

fn start_direct(cfg: lazy_wire_fire::config::Config) -> Direct {
    common::logs();
    let (tx, rx) = watch::channel(false);
    let health = Arc::new(lazy_wire_fire::health::Health::default());
    let h = health.clone();
    let done: Arc<Mutex<Option<anyhow::Result<()>>>> = Arc::new(Mutex::new(None));
    let d = done.clone();
    let task = tokio::spawn(async move {
        let r = lazy_wire_fire::engine::run(cfg, h, None, rx).await;
        *d.lock().unwrap() = Some(r);
    });
    Direct {
        stop: tx,
        health,
        done,
        task,
    }
}

impl Direct {
    async fn ready(&self) {
        let until = std::time::Instant::now() + Duration::from_secs(30);
        while !self.health.is_ready() {
            if self.died() {
                panic!(
                    "the engine died before it got ready: {:?}",
                    self.done.lock().unwrap()
                );
            }
            assert!(
                std::time::Instant::now() < until,
                "the engine did not get ready in 30 s"
            );
            sleep_ms(20).await;
        }
    }

    fn died(&self) -> bool {
        self.done.lock().unwrap().is_some()
    }

    /// The engine's own answer once it has finished (it must have finished cleanly).
    async fn stop_clean(self) {
        let _ = self.stop.send(true);
        let grace = Duration::from_secs(20);
        match tokio::time::timeout(grace, self.task).await {
            Ok(Ok(())) => {}
            other => panic!("engine did not stop cleanly: {other:?} after {grace:?}"),
        }
        let engine = self.done.lock().unwrap().take();
        match engine {
            Some(Ok(())) => {}
            other => panic!("engine did not stop cleanly: it answered {other:?}"),
        }
    }
}

/// The fence of a flush cannot connect: the flush fails at `action::fence` (the fail closure of
/// `flush_job`), every key of the flush is kept (`keep_queries`), the position does NOT advance,
/// and after the account works again the same keys flush and the position moves. The fence runs
/// on lazywf's own pool (`Env::pool`), so rotating the engine account's password and killing its
/// pool connections (only this engine's own connections) is what reaches that arm: a fence
/// whose connection cannot be made is the error the flush reports, while the control connection
/// (which the pool refusal does not touch) keeps the engine alive and writing status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_fails_when_the_fence_connection_cannot_connect() {
    let root = need_db!();
    let db = Db::fresh(&root, "fenceconn").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, r#"{"errors":false,"items":[]}"#.into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('frul', 'hook', 'POST /fk HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("frul", "orders", Some("{{new.id}}"), None).await;
    let user = "lwf_fnc";
    let dsn = engine_user(&db, user, "fnc-pw-one", &root).await;
    let edb = Db {
        schema: db.schema.clone(),
        dsn: dsn.clone(),
    };
    let inst = Instance::ready(config(&edb, &[])).await;

    // Control: the account works, the first key flushes and the position moves.
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(15), "the baseline key", || async {
        (fired(&mock.requests(), "/fk").contains(&"1".to_string())).then_some(())
    })
    .await;
    inst.quiet().await;
    let holder = lock_holder(&db, &instance_lock(&db))
        .await
        .expect("the engine holds its instance lock");

    // Break: the password rotates and the pool connections of THIS engine are swept away (never
    // the control connection the lock names, never the binlog reader): any pool connection made
    // after this is refused, so `fence` fails before the first key of the flush reaches the
    // target. The sweep repeats for a moment because the pool keeps no minimum and an idle
    // connection of the old password is the one thing that would let a flush through.
    db.exec(&format!(
        "ALTER USER '{user}'@'%' IDENTIFIED BY 'fnc-pw-two'"
    ))
    .await;
    let w0 = warn_lines(&["frul", "fence connection"]).len();
    for _ in 0..10 {
        let ids: Vec<u64> = user_conns(&db, user)
            .await
            .into_iter()
            .filter(|(id, cmd)| *id != holder && !cmd.starts_with("Binlog Dump"))
            .map(|(id, _)| id)
            .collect();
        for id in ids {
            kill_conn(&db, id).await;
        }
        sleep_ms(200).await;
    }

    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    // The flush fails FOR the fence: the fail closure's error names the fence connection, and
    // it keeps failing (the keys retry, they are not dropped), and the engine survives it.
    wait_for(
        Duration::from_secs(20),
        "the fence failures repeat",
        || async { (warn_lines(&["frul", "fence connection"]).len() >= w0 + 3).then_some(()) },
    )
    .await;
    sleep_ms(500).await;
    let keys = fired(&mock.requests(), "/fk");
    assert!(
        !keys.contains(&"2".to_string()),
        "the key was delivered: {keys:?}"
    );
    let (af, ap, _, err) = db.status("frul").await.unwrap();
    assert!((af, ap) < after, "the position passed the flush's insert");
    let shown = err.unwrap_or_default();
    assert!(
        shown.contains("fence connection"),
        "the last error does not name the fence: {shown:?}"
    );
    assert!(inst.health.is_ready(), "the engine died on the fence");

    // Restore: the same keys flush (the queries were kept, the keys were never lost) and the
    // position moves past the insert the failure held it below.
    db.exec(&format!(
        "ALTER USER '{user}'@'%' IDENTIFIED BY 'fnc-pw-one'"
    ))
    .await;
    wait_for(
        Duration::from_secs(20),
        "the held key after the restore",
        || async {
            let (af, ap, _, _) = db.status("frul").await?;
            (fired(&mock.requests(), "/fk").contains(&"2".to_string()) && (af, ap) >= after)
                .then_some(())
        },
    )
    .await;
    inst.stop().await;
    db.exec(&format!("DROP USER IF EXISTS '{user}'@'%'")).await;
}

/// A `keys_sql` that starts erroring mid-run fails the whole flush (`action::resolve_keys`'
/// error arm in `flush_job`): the flush's keys reach neither the target nor the position; when
/// the SQL works again the keys resolve and flush — including the key only `keys_sql` can find,
/// which proves the query payloads were kept (`keep_queries`). A `keys_sql` that resolves to
/// NOTHING is the other arm: a no-op flush that still confirms (the position passes it, the
/// next key still fires — not a silent skip that would strand the generation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_sql_failure_holds_and_empty_resolution_confirms() {
    let root = need_db!();
    let db = Db::fresh(&root, "ksql").await;
    shop(&db).await;
    db.exec("CREATE TABLE items (oid INT, cid INT, PRIMARY KEY (oid, cid))")
        .await;
    db.exec("CREATE TABLE mapping (cid INT, k VARCHAR(20), PRIMARY KEY (cid))")
        .await;
    db.exec("INSERT INTO items VALUES (1, 100), (2, 200)").await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('krul', 'hook', 'POST /kk HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("krul", "orders", Some("{{new.id}}"), None).await;
    db.trigger(
        "krul",
        "orders",
        None,
        Some("SELECT cid AS _key FROM items WHERE oid = :new.id"),
    )
    .await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('erul', 'hook', 'POST /ee HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger(
        "erul",
        "customers",
        None,
        Some("SELECT k AS _key FROM mapping WHERE cid = :new.id"),
    )
    .await;
    let inst = Instance::ready(config(&db, &[])).await;

    // Control: the marked key AND the key only `keys_sql` finds reach the target.
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(15), "the resolved key", || async {
        let k = fired(&mock.requests(), "/kk");
        (k.contains(&"1".to_string()) && k.contains(&"100".to_string())).then_some(())
    })
    .await;
    inst.quiet().await;
    mock.clear();

    // Break: `keys_sql` errors (its table is gone) while flushes of this rule are pending. The
    // server's 1146 names the table with this schema in front of it (`Table 'lwf_ksql.items'
    // doesn't exist`), so the schema is a needle the line really carries.
    db.exec("RENAME TABLE items TO items_gone").await;
    let w0 = warn_lines_in(&db, &["krul", "keys_sql"]).len();
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(
        Duration::from_secs(20),
        "the keys_sql failures repeat",
        || async { (warn_lines_in(&db, &["krul", "keys_sql"]).len() >= w0 + 2).then_some(()) },
    )
    .await;
    let keys = fired(&mock.requests(), "/kk");
    assert!(
        !keys.contains(&"2".to_string()) && !keys.contains(&"200".to_string()),
        "a key of the failed flush was delivered: {keys:?}"
    );
    let (af, ap, _, err) = db.status("krul").await.unwrap();
    assert!((af, ap) < after, "the position passed the failing flush");
    let shown = err.unwrap_or_default();
    assert!(
        shown.contains("keys_sql"),
        "the last error does not name keys_sql: {shown:?}"
    );

    // Restore: the flush succeeds — and the key that only `keys_sql` can name (200) arrives,
    // which is only possible when the failed flush kept its query payloads.
    db.exec("RENAME TABLE items_gone TO items").await;
    wait_for(
        Duration::from_secs(20),
        "both keys after the restore",
        || async {
            let (af, ap, _, _) = db.status("krul").await?;
            let k = fired(&mock.requests(), "/kk");
            (k.contains(&"2".to_string()) && k.contains(&"200".to_string()) && (af, ap) >= after)
                .then_some(())
        },
    )
    .await;

    // Empty resolution: the `keys_sql` finds nothing. The flush is a no-op (the target sees
    // nothing) yet it CONFIRMS: the position passes it and the next key still fires — an
    // unconfirmed generation would strand both (and log "flush skipped").
    mock.clear();
    inst.quiet().await;
    db.exec("INSERT INTO customers VALUES (7, 'Gil')").await;
    let after_e = binlog_end(&db).await;
    db.tick().await;
    wait_for(
        Duration::from_secs(20),
        "the empty flush confirmed",
        || async {
            let (af, ap, _, _) = db.status("erul").await?;
            ((af, ap) >= after_e).then_some(())
        },
    )
    .await;
    sleep_ms(400).await;
    assert!(
        fired(&mock.requests(), "/ee").is_empty(),
        "the empty flush delivered something: {:?}",
        mock.requests()
    );
    assert!(
        !warn_lines(&["erul", "flush skipped"])
            .into_iter()
            .any(|l| l.contains("generation is already in flight")),
        "a later flush found the generation still in flight (a silent skip)"
    );
    // The same rule delivers once its `keys_sql` resolves: the buffer and its generation are alive.
    db.exec("INSERT INTO mapping VALUES (7, 'k7')").await;
    db.exec("UPDATE customers SET name = 'Gil B' WHERE id = 7")
        .await;
    wait_for(
        Duration::from_secs(15),
        "the resolved key of erul",
        || async { (fired(&mock.requests(), "/ee").contains(&"k7".to_string())).then_some(()) },
    )
    .await;
    inst.stop().await;
}

/// The engine's own control connection is killed — the connection holding the instance lock, and
/// only this engine's own connection. The persist fails (the WARN says which arm noticed and names
/// the killed connection), the lock goes with the killed session (asserted in the database:
/// `IS_USED_LOCK` is NULL while the engine is not looking), `heal_control` reconnects and
/// `lock::reacquire` takes the lock again (asserted in the database: the holder's id changes from
/// the killed one), the engine stays alive, and a key written after the kill reaches the target:
/// the reconnected connection is usable, not merely open.
///
/// The status write is the only thing that touches `control` here, and it is slowed to 5 s: the
/// kill then lands while the connection is idle, so the server has finished releasing the killed
/// session's lock before the engine notices, and the assertions below have a window to look at the
/// lock in between. A kill that lands mid-statement leaves the engine racing a session that is
/// still dying, which is a different and not deterministic story.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_control_connection_is_healed_and_flushing_continues() {
    let root = need_db!();
    let db = Db::fresh(&root, "healctl").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('hrul', 'hook', 'POST /hk HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("hrul", "orders", Some("{{new.id}}"), None).await;
    let lock = instance_lock(&db);
    // The config poll never fires here, so the WARN that proves which arm noticed is the status
    // write's, and the 5 s cadence is the window the kill lands in.
    let inst = Instance::ready(config(
        &db,
        &[
            ("LAZYWF_CONFIG_POLL_MS", "120000"),
            ("LAZYWF_STATUS_MS", "5000"),
        ],
    ))
    .await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(15), "the baseline key", || async {
        (fired(&mock.requests(), "/hk").contains(&"1".to_string())).then_some(())
    })
    .await;
    inst.quiet().await;
    let before = lock_holder(&db, &lock)
        .await
        .expect("the engine holds its lock");
    // The WARN ring is shared with every other test in this binary and this line carries no rule,
    // schema or account of its own, so the assertion is a delta over the baseline of each wording
    // taken here, and the rest of the attribution is done in the database below (the session that
    // is gone, the lock's new holder, the key that gets delivered).
    // Where the server is in its binlog now: the row the engine persists must still be BEHIND this
    // when the failure WARN appears (see the assertion after it).
    let boundary = binlog_end(&db).await;
    let base = warn_baselines(&["writing lazy_wire_fire_status"], DEAD_CONNECTION);

    kill_conn(&db, before).await;
    // The killed session really is gone from the server, so the lock cannot have stayed with it.
    wait_for(
        Duration::from_secs(10),
        "the killed session gone",
        || async { (!conn_alive(&db, before).await).then_some(()) },
    )
    .await;
    // The engine notices on a write it has to make anyway, and the line says which arm it noticed
    // on and what the killed connection answered.
    let noticed = wait_for(
        Duration::from_secs(40),
        "the persist noticed the kill, naming the dead connection",
        || async { warn_grew(&["writing lazy_wire_fire_status"], DEAD_CONNECTION, &base) },
    )
    .await;
    // And the write of that run really did not land: the row the engine reports is still the one
    // from before the kill, because `persist` asks for the lock before it writes anything, so the
    // dead connection loses the whole write and not one statement of it. A WARN borrowed from
    // another test could not keep this row where it is.
    let (row_file, row_pos) = db
        .status("hrul")
        .await
        .map(|(f, p, _, _)| (f, p))
        .unwrap_or_default();
    assert!(
        (row_file.as_str(), row_pos) < (boundary.0.as_str(), boundary.1),
        "the status row already shows {row_file}:{row_pos}, at or past the position the kill \
         happened at ({boundary:?}): the write of that run did not fail, so this WARN is not its \
         failure: {noticed}"
    );
    // A live connection other than the killed one holds the lock now: `GET_LOCK` could only grant
    // it because the killed session took its lock to the grave, and `lock::reacquire` is what asked
    // for it — in the database, not just in a log line.
    wait_for(
        Duration::from_secs(40),
        "the lock on a new live connection",
        || async {
            lock_holder_live(&db, &lock)
                .await
                .and_then(|id| (id != before).then_some(()))
        },
    )
    .await;
    // And the engine is still alive on that new connection: what the arm is accused of is giving
    // up, and `lost the instance lock` (src/lock.rs:77) is a `bail!` returned to `run()`, never a
    // WARN, so "no such WARN" would prove nothing. The proof is that it keeps beating here and
    // shuts down clean at the end.
    assert!(
        inst.health.is_ready(),
        "the engine gave up instead of healing its control connection (it stopped beating after: \
         {noticed})"
    );

    // The proof the reconnect was real: a key written after the kill reaches the target, and
    // the position moves past it.
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(30), "a key after the kill", || async {
        let (af, ap, _, _) = db.status("hrul").await?;
        (fired(&mock.requests(), "/hk").contains(&"2".to_string()) && (af, ap) >= after)
            .then_some(())
    })
    .await;
    // The engine finished cleanly: the kill healed instead of killing it.
    inst.stop().await;
}

/// The instance lock was taken while the control connection was down: `heal_control`'s
/// `lock::reacquire` refuses and the engine dies with exactly that error (the fatal is proven
/// by the message, the reload-failure WARN proves what made it notice, and the timing is
/// deterministic: both timers stand at 120 s, so only the dirty config row moves the engine).
/// The control: the very same kill and grab, but the lock is released before the engine
/// notices — the heal succeeds, the config lands, delivery goes on, and the engine lives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instance_lock_taken_during_control_loss_is_fatal() {
    let root = need_db!();
    let db = Db::fresh(&root, "lockfatal").await;
    shop(&db).await;
    let mock = Mock::start().await;
    let ans = answer_log(&mock);
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('lrul', 'hook', 'POST /lk HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("lrul", "orders", Some("{{new.id}}"), None).await;
    let lock = instance_lock(&db);
    let slow = [
        ("LAZYWF_STATUS_MS", "120000"),
        ("LAZYWF_CONFIG_POLL_MS", "120000"),
    ];
    let eng = start_direct(config(&db, &slow));
    eng.ready().await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(15), "the baseline key", || async {
        (fired(&mock.requests(), "/lk").contains(&"1".to_string())).then_some(())
    })
    .await;
    let before = lock_holder(&db, &lock)
        .await
        .expect("the engine holds its lock");
    // Which arm noticed the dead control connection is a corroborating signal only: this WARN
    // carries no schema or rule of its own, so it gets the reason of the failure and a baseline per
    // wording, while the claim that carries weight — the engine died ON THIS LOCK — is the error it
    // returned, which names this schema's lock by name.
    let base = warn_baselines(
        &["reloading the configuration", "retrying"],
        DEAD_CONNECTION,
    );

    kill_conn(&db, before).await;
    wait_for(
        Duration::from_secs(10),
        "the lock released with the connection",
        || async { (lock_holder(&db, &lock).await.is_none()).then_some(()) },
    )
    .await;
    // Take the engine's own instance lock on a session of ours (kept open across the race).
    let mut admin = db.conn().await;
    let got: Option<Option<i64>> = admin
        .exec_first("SELECT GET_LOCK(?, 5)", (&lock,))
        .await
        .unwrap();
    assert_eq!(
        got.flatten(),
        Some(1),
        "the second connection could not take the instance lock: the premise is broken"
    );

    // The only control use left is the reload the dirty config row forces: it fails (the
    // control connection is dead), `heal_control` reconnects, `reacquire` refuses — fatal.
    db.exec("UPDATE lazy_wire_fire_rule SET debounce_ms = 60 WHERE name = 'lrul'")
        .await;
    wait_for(
        Duration::from_secs(20),
        "the engine to die on the lock",
        || async { eng.died().then_some(()) },
    )
    .await;
    let err = eng
        .done
        .lock()
        .unwrap()
        .take()
        .expect("the engine answered");
    let shown = format!("{:#}", err.unwrap_err());
    let noticed = warn_grew(
        &["reloading the configuration", "retrying"],
        DEAD_CONNECTION,
        &base,
    )
    .unwrap_or_else(|| {
        panic!(
            "nothing shows the reload was the arm that noticed the dead control connection, and \
             died on it: {shown}"
        )
    });
    assert!(
        shown.contains("taken by another instance") && shown.contains(&lock),
        "the fatal does not name the lock taken by another instance (the arm that noticed was: \
         {noticed}): {shown}"
    );
    admin
        .exec_drop("DO RELEASE_LOCK(?)", (&lock,))
        .await
        .unwrap();
    quit(admin).await;
    drop(eng.stop);

    // The control: the same kill and the same grab in a second schema — only the lock goes back
    // BEFORE the engine looks for it. Then `reacquire` takes it again on the new connection, the
    // reload lands, delivery goes on, and the engine lives (which is what makes the fatal above
    // the fault of the held lock and not of the kill).
    mock.clear();
    let db2 = {
        // A second schema: the dead engine's status rows and lock name must not blur this one.
        let d = Db::fresh(&root, "lockctl2").await;
        shop(&d).await;
        d.target("hook", "http", &mock.url(), None, "{}").await;
        d.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('lrul2', 'hook', 'POST /lk HTTP/1.1\n\n{{join keys \",\"}}', 100)")
            .await;
        d.trigger("lrul2", "orders", Some("{{new.id}}"), None).await;
        d
    };
    let lock2 = instance_lock(&db2);
    let eng2 = start_direct(config(&db2, &slow));
    eng2.ready().await;
    db2.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db2.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(15),
        "the baseline key of the control",
        || async { (fired(&mock.requests(), "/lk").contains(&"1".to_string())).then_some(()) },
    )
    .await;
    let before2 = lock_holder(&db2, &lock2)
        .await
        .expect("the engine holds its lock");
    let cbase = warn_baselines(
        &["reloading the configuration", "retrying"],
        DEAD_CONNECTION,
    );

    kill_conn(&db2, before2).await;
    wait_for(
        Duration::from_secs(10),
        "the lock released with the connection",
        || async { (lock_holder(&db2, &lock2).await.is_none()).then_some(()) },
    )
    .await;
    // The same grab — and the same release, before the engine has any reason to look: both
    // timers stand at 120 s, so the config row below is the only control use left.
    let mut admin = db2.conn().await;
    let got: Option<Option<i64>> = admin
        .exec_first("SELECT GET_LOCK(?, 5)", (&lock2,))
        .await
        .unwrap();
    assert_eq!(
        got.flatten(),
        Some(1),
        "the second connection could not take the instance lock: the premise is broken"
    );
    admin
        .exec_drop("DO RELEASE_LOCK(?)", (&lock2,))
        .await
        .unwrap();
    quit(admin).await;

    // The reload the dirty row forces: it fails on the dead control connection (the WARN names
    // the arm), `heal_control` reconnects and takes the lock again — and the engine goes on
    // reading its configuration: a rule that did not exist when the connection died delivers.
    db2.exec("UPDATE lazy_wire_fire_rule SET debounce_ms = 60 WHERE name = 'lrul2'")
        .await;
    let reloaded = wait_for(
        Duration::from_secs(20),
        "the reload after the heal",
        || async {
            warn_grew(
                &["reloading the configuration", "retrying"],
                DEAD_CONNECTION,
                &cbase,
            )
        },
    )
    .await;
    assert!(
        !eng2.died(),
        "the engine died although the lock was free again (it had noticed the dead connection \
         here: {reloaded})"
    );
    let holder2 = lock_holder_live(&db2, &lock2)
        .await
        .expect("the healed engine holds its instance lock again on a live connection");
    assert_ne!(
        holder2, before2,
        "the lock is still on the killed connection"
    );
    // A rule that did not exist when the control connection died, read through the healed one.
    // One transaction, so no reload can ever see the rule without its trigger (with both timers
    // at 120 s there would be no second look), and with `reset_steps`, whose step the engine runs
    // the moment it registers the rule: an answer on `/lk2warm` can only have come from THIS
    // engine having read THIS rule, which no status row (its timer stands at 120 s too) could
    // prove. The key goes in only after that: a new rule starts at the current binlog position,
    // so a key that passed before the rule was read is not its to deliver.
    let warm = json!([{"once": true, "template": "DELETE /lk2warm HTTP/1.1\n\n"}]).to_string();
    let mut add = db2.conn().await;
    add.query_drop("START TRANSACTION").await.unwrap();
    add.exec_drop(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) \
         VALUES ('lrul2b', 'hook', 'POST /lk2 HTTP/1.1\n\n{{join keys \",\"}}', ?, 50)",
        (warm,),
    )
    .await
    .unwrap();
    add.query_drop("INSERT INTO lazy_wire_fire_trigger (rule, source_table, `key`) VALUES ('lrul2b', 'orders', '{{new.id}}')")
        .await
        .unwrap();
    add.query_drop("COMMIT").await.unwrap();
    quit(add).await;
    wait_for(
        Duration::from_secs(20),
        "the engine to read the rule added after the heal",
        || async { (answered(&ans, "/lk2warm") >= 1).then_some(()) },
    )
    .await;
    db2.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    wait_for(
        Duration::from_secs(20),
        "the rule added after the heal to deliver",
        || async { (fired(&mock.requests(), "/lk2").contains(&"2".to_string())).then_some(()) },
    )
    .await;
    // One warm step, one flush of the key: the rule was read once and delivered what it marks.
    sleep_ms(500).await;
    assert_eq!(
        answered(&ans, "/lk2warm"),
        1,
        "the new rule's reset did not run exactly once ({})",
        traffic(&mock, &ans)
    );
    assert_eq!(
        fired(&mock.requests(), "/lk2")
            .iter()
            .filter(|k| k.as_str() == "2")
            .count(),
        1,
        "the healed engine did not deliver the new rule's key exactly once: {:?}",
        mock.requests()
    );
    eng2.stop_clean().await;
}

/// The standby's only connection is the one that polls `GET_LOCK` (`lock::acquire` runs before
/// any pool exists), so killing it is the reconnect arm of the standby loop in `lock::acquire`.
/// The WARN names the arm; a NEW connection of the standby's own account proves the reconnect
/// happened in the server and not only in a log line; the healed standby does NOT take the lock
/// from the live copy; and it is that healed connection that takes over and delivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_reconnects_its_control_connection() {
    let root = need_db!();
    let db = Db::fresh(&root, "stbyctl").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('srul', 'hook', 'POST /sk HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("srul", "orders", Some("{{new.id}}"), None).await;
    // Two accounts: whose connection is whose is then a fact from the server, not a guess.
    let dsn_a = engine_user(&db, "lwf_stba", "stby-pw-a", &root).await;
    let dsn_b = engine_user(&db, "lwf_stbb", "stby-pw-b", &root).await;
    let with = |dsn: &str| Db {
        schema: db.schema.clone(),
        dsn: dsn.to_string(),
    };
    let inst = Instance::ready(config(&with(&dsn_a), &[])).await;
    let stby = Instance::start(config(&with(&dsn_b), &[]));
    sleep_ms(1500).await;
    assert!(
        !stby.health.is_ready(),
        "the standby became active beside the active copy"
    );

    // What the killed connection answers with. This WARN carries no rule, schema or account of its
    // own, so each wording gets a baseline here and the attribution of the reconnect is done in the
    // server below, on the standby's own account (`lwf_stbb` exists only in this test).
    let base = warn_baselines(&["standby: control connection lost"], DEAD_CONNECTION);
    let killed: u64 = wait_for(
        Duration::from_secs(30),
        "an idle connection of the standby to kill",
        || async {
            let idle = user_conns(&db, "lwf_stbb")
                .await
                .into_iter()
                .find(|(_, cmd)| cmd == "Sleep");
            match idle {
                Some((id, _)) => {
                    kill_conn(&db, id).await;
                    Some(id)
                }
                None => None,
            }
        },
    )
    .await;
    // The arm that noticed, by its own words, on a connection the server says is gone.
    let noticed = wait_for(
        Duration::from_secs(30),
        "the standby to notice its dead connection",
        || async {
            warn_grew(
                &["standby: control connection lost"],
                DEAD_CONNECTION,
                &base,
            )
        },
    )
    .await;
    // The reconnect happened in the server: the account has a connection that is not the killed
    // one (the standby has no pool, so this is its control connection).
    wait_for(
        Duration::from_secs(30),
        "the standby to reconnect",
        || async {
            let cs = user_conns(&db, "lwf_stbb").await;
            (!cs.is_empty() && !cs.iter().any(|(i, _)| *i == killed)).then_some(())
        },
    )
    .await;
    // Still only standing by: the new connection did not take the lock from the live copy.
    sleep_ms(1500).await;
    assert!(
        !stby.health.is_ready(),
        "the healed standby took the lock beside the active copy (the failure it healed was: \
         {noticed})"
    );
    assert!(inst.health.is_ready(), "the active copy died");

    // The healed connection works: the active copy fires, then stops, and the standby — on the
    // connection it made after the kill — takes the lock and fires too.
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(20), "the active copy's key", || async {
        (fired(&mock.requests(), "/sk").contains(&"1".to_string())).then_some(())
    })
    .await;
    inst.stop().await;
    wait_for(
        Duration::from_secs(30),
        "the standby to take over",
        || async { stby.health.is_ready().then_some(()) },
    )
    .await;
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(
        Duration::from_secs(20),
        "a key delivered by the healed standby",
        || async {
            let (af, ap, _, _) = db.status("srul").await?;
            (fired(&mock.requests(), "/sk").contains(&"2".to_string()) && (af, ap) >= after)
                .then_some(())
        },
    )
    .await;
    stby.stop().await;
    // The two accounts of this test are its own: no leftovers on a shared server.
    for u in ["lwf_stba", "lwf_stbb"] {
        db.exec(&format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    }
}

/// A rule that breaks while running is frozen where it stands: both error paths of
/// `reload_config` join their causes (the two trigger errors into `trigger_error`, the row parse
/// error into `error`), the WARNs name which is which, the status row shows the rule error, and
/// `rebuild_routes` marks nothing for the broken rule — its saved position stays pinned below
/// what it missed while the healthy rule of the same engine keeps flowing. Healed, it is the
/// unfreeze arm that decides how the gap is covered: `lose_continuity`, which for a rule with
/// `reset_steps` means the reset runs again (and a key that arrived while broken is not
/// silently delivered by the buffer).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broken_rule_freezes_where_it_stands_and_heals_by_its_reset() {
    let root = need_db!();
    let db = Db::fresh(&root, "freeze1").await;
    shop(&db).await;
    let mock = Mock::start().await;
    let ans = answer_log(&mock);
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps = json!([{"once": true, "template": "PURGE /zpurge HTTP/1.1\n\n"}]).to_string();
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) VALUES ('zrul', 'hook', \
         'POST /zk HTTP/1.1\n\n{{join keys \",\"}}', ?, 100)",
        (steps.clone(),),
    )
    .await;
    db.trigger("zrul", "orders", Some("{{new.id}}"), None).await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('z2', 'hook', 'POST /z2 HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("z2", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(Duration::from_secs(15), "the first-start reset", || async {
        (answered(&ans, "/zpurge") == 1).then_some(())
    })
    .await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(15),
        "both rules before the break",
        || async {
            let k = fired(&mock.requests(), "/zk");
            let c = fired(&mock.requests(), "/z2");
            (k.contains(&"1".to_string()) && c.contains(&"1".to_string())).then_some(())
        },
    )
    .await;
    inst.quiet().await;

    // Break it three ways at once, in one transaction: the engine reads one configuration in
    // which all three causes are present together, so each reaches the log with its own words.
    let mut bad = db.conn().await;
    bad.query_drop("START TRANSACTION").await.unwrap();
    bad.query_drop("INSERT INTO lazy_wire_fire_trigger (rule, source_table, keys_sql) VALUES ('zrul','order_tags','SELECT tag FROM order_tags WHERE order_id = :bogus')")
        .await
        .unwrap();
    bad.query_drop("INSERT INTO lazy_wire_fire_trigger (rule, source_table, keys_sql) VALUES ('zrul','order_tags','SELECT tag FROM order_tags WHERE order_id = :new.order_id')")
        .await
        .unwrap();
    bad.exec_drop(
        "UPDATE lazy_wire_fire_rule SET reset_steps = ? WHERE name = 'zrul'",
        ("{not json",),
    )
    .await
    .unwrap();
    bad.query_drop("COMMIT").await.unwrap();
    quit(bad).await;
    // The trigger join: both causes in ONE message ("; " in `reload_config`).
    wait_for(
        Duration::from_secs(20),
        "both trigger causes joined",
        || async {
            (!warn_lines(&["zrul", "cannot mark keys", ":bogus", "no `_key` column"]).is_empty())
                .then_some(())
        },
    )
    .await;
    // The row parse error, joined into the rule's own error, is what the status row shows.
    wait_for(
        Duration::from_secs(20),
        "the rule error in the status row",
        || async {
            let (_, _, _, err) = db.status("zrul").await?;
            err.is_some_and(|e| e.contains("`reset_steps` is not a valid JSON array"))
                .then_some(())
        },
    )
    .await;
    assert!(
        !warn_lines(&["zrul", "cannot fire", "reset_steps"]).is_empty(),
        "the rule error was never warned as what it is: {:?}",
        warn_lines(&["zrul", "cannot fire"])
    );

    // Nothing is marked for the frozen rule: its keys reach neither the target nor its position.
    db.exec("INSERT INTO orders VALUES (5, 1, 'new', 5), (6, 1, 'new', 6)")
        .await;
    let missed = binlog_end(&db).await;
    sleep_ms(2000).await;
    let keys = fired(&mock.requests(), "/zk");
    assert!(
        !keys.contains(&"5".to_string()) && !keys.contains(&"6".to_string()),
        "a frozen rule delivered what it was not supposed to mark: {keys:?}"
    );
    let (zf, zp, _, _) = db.status("zrul").await.unwrap();
    assert!(
        (&zf, &zp) < (&missed.0, &missed.1),
        "the frozen rule's position passed what it missed: {zf}:{zp} vs {missed:?}"
    );
    // The healthy rule of the same engine is unaffected: only the broken rule is pinned.
    db.tick().await;
    wait_for(
        Duration::from_secs(20),
        "the healthy rule to keep moving",
        || async {
            let (f, p, _, _) = db.status("z2").await?;
            ((f, p) >= missed).then_some(())
        },
    )
    .await;

    // Heal: the triggers go and the row parses again — one transaction, so the unfreeze happens
    // with the rule's `reset_steps` already readable. Nothing may answer while that unfreeze is
    // supposed to be in flight: its step 1 is what holds the reason in the status row, because
    // the moment the last step is answered the row is overwritten with `done: <time>`.
    mock.hold_after(0);
    let mut heal = db.conn().await;
    heal.query_drop("START TRANSACTION").await.unwrap();
    heal.query_drop(
        "DELETE FROM lazy_wire_fire_trigger WHERE rule = 'zrul' AND keys_sql IS NOT NULL",
    )
    .await
    .unwrap();
    heal.exec_drop(
        "UPDATE lazy_wire_fire_rule SET reset_steps = ? WHERE name = 'zrul'",
        (steps.clone(),),
    )
    .await
    .unwrap();
    heal.query_drop("COMMIT").await.unwrap();
    quit(heal).await;
    wait_for(
        Duration::from_secs(20),
        "the unfrozen rule to say why it restarted",
        || async {
            let (_, _, rs, _) = db.status("zrul").await?;
            (rs.contains("running: step 1/1")
                && rs.contains("its triggers work again after an error"))
            .then_some(())
        },
    )
    .await;
    // The reason is a promise, not the proof: the reset is what covers the keys the frozen rule
    // never marked, so let the held step be answered and require that this reset ran. Two
    // answers ever: the one at first start and this one.
    mock.heal();
    wait_for(
        Duration::from_secs(20),
        "the reset that covers the gap",
        || async { (answered(&ans, "/zpurge") >= 2).then_some(()) },
    )
    .await;
    wait_for(Duration::from_secs(20), "the reset to finish", || async {
        let (_, _, rs, _) = db.status("zrul").await?;
        rs.starts_with("done:").then_some(())
    })
    .await;
    sleep_ms(500).await;
    assert_eq!(
        answered(&ans, "/zpurge"),
        2,
        "the reset ran other than once at first start and once on the unfreeze ({})",
        traffic(&mock, &ans)
    );
    // It marks again: a key after the heal is delivered, and the ones it missed while frozen
    // are still not in the buffer (the reset, not a replay of marks, is what covers them).
    db.exec("INSERT INTO orders VALUES (7, 1, 'new', 7)").await;
    let back = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(20), "a key after the heal", || async {
        let (f, p, _, _) = db.status("zrul").await?;
        (fired(&mock.requests(), "/zk").contains(&"7".to_string()) && (f, p) >= back).then_some(())
    })
    .await;
    let keys = fired(&mock.requests(), "/zk");
    assert_eq!(
        keys.iter().filter(|k| k.as_str() == "1").count(),
        1,
        "the key delivered before the break fired again: {keys:?}"
    );
    assert!(
        !keys.contains(&"5".to_string()) && !keys.contains(&"6".to_string()),
        "the frozen rule replayed marks it never made: {keys:?}"
    );
    inst.stop().await;
}

/// A rule changed while its reset is in flight: `update_rule` notices the fingerprint of the
/// running reset differs and starts it over, with the reason chain of both in the status row —
/// and started over means step 1 runs a second time, not the old run's remainder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rule_change_restarts_the_reset_in_flight() {
    let root = need_db!();
    let db = Db::fresh(&root, "midreset").await;
    shop(&db).await;
    let mock = Mock::start().await;
    let ans = answer_log(&mock);
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps = json!([{"once": true, "template": "DELETE /rcache HTTP/1.1\n\n"}]).to_string();
    // The restarted run runs the NEW rule's steps from step 1, so a second path tells the two
    // runs apart for good: an answer on `/rcache2` can only have come from the restarted one.
    let steps2 = json!([{"once": true, "template": "DELETE /rcache2 HTTP/1.1\n\n"}]).to_string();
    // `reset_requested = 1` in the INSERT would not survive the start: both the flag and the
    // first start ask for a reset at load and the first start overwrites the other. So the
    // requested reset below is raised on a live engine, once the first-start one is over.
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, reset_requested, debounce_ms) \
         VALUES ('rrul', 'hook', 'POST /mr HTTP/1.1\n\n{{join keys \",\"}}', ?, 0, 100)",
        (steps.clone(),),
    )
    .await;
    db.trigger("rrul", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(
        Duration::from_secs(20),
        "the first-start reset to finish",
        || async {
            let (_, _, rs, _) = db.status("rrul").await?;
            (rs.starts_with("done:") && answered(&ans, "/rcache") == 1).then_some(())
        },
    )
    .await;

    // Now raise the flag with nothing able to answer: the requested reset reaches its step 1 and
    // stands there unanswered, which is the only place a restart can be seen to restart.
    mock.hold_after(0);
    db.exec("UPDATE lazy_wire_fire_rule SET reset_requested = 1 WHERE name = 'rrul'")
        .await;
    wait_for(
        Duration::from_secs(20),
        "the requested reset to reach step 1",
        || async {
            let (_, _, rs, _) = db.status("rrul").await?;
            (rs.contains("running: step 1/1") && rs.contains(": reset_requested")).then_some(())
        },
    )
    .await;
    // THE premise of what follows, proven rather than assumed: the row says step 1 is running and
    // the mock has answered this rule's step exactly once in its whole life — the first start's.
    // So this run is in flight with its step unanswered: the change below finds it mid-step.
    assert_eq!(
        answered(&ans, "/rcache"),
        1,
        "the requested reset's step 1 had already been answered: it was not in flight ({})",
        traffic(&mock, &ans)
    );

    // Change the rule (its fingerprint) with that step still unanswered.
    db.exec_params(
        "UPDATE lazy_wire_fire_rule SET template = 'POST /mr2 HTTP/1.1\n\n{{join keys \",\"}}', \
         reset_steps = ? WHERE name = 'rrul'",
        (steps2,),
    )
    .await;
    wait_for(
        Duration::from_secs(20),
        "the reset to say it restarted",
        || async {
            let (_, _, rs, _) = db.status("rrul").await?;
            (rs.contains("running: step 1/1")
                && rs.contains("reset_requested; restarted after the rule changed"))
            .then_some(())
        },
    )
    .await;
    // Now the two runs can be told apart by what they were ANSWERED, which nothing in the status
    // row could: the restarted run runs the NEW rule's step, so `/rcache2` is its signature, and
    // the abandoned run's own step — held, then reset by `heal()` — goes out again as the HTTP
    // layer's fresh retry (a step is at-least-once, which is why it must be idempotent).
    mock.heal();
    wait_for(
        Duration::from_secs(20),
        "the restarted reset to finish",
        || async {
            let (_, _, rs, _) = db.status("rrul").await?;
            (rs.starts_with("done:") && answered(&ans, "/rcache2") >= 1).then_some(())
        },
    )
    .await;
    sleep_ms(500).await;
    assert_eq!(
        answered(&ans, "/rcache2"),
        1,
        "the restarted run did not run the new step exactly once from the beginning ({})",
        traffic(&mock, &ans)
    );
    assert_eq!(
        answered(&ans, "/rcache"),
        2,
        "the old step was not run once at first start and once by the run that was restarted ({})",
        traffic(&mock, &ans)
    );
    // The change itself landed: keys now go to the new template's path.
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (3, 1, 'new', 3)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(20), "the new template", || async {
        let (af, ap, _, _) = db.status("rrul").await?;
        (fired(&mock.requests(), "/mr2").contains(&"3".to_string()) && (af, ap) >= after)
            .then_some(())
    })
    .await;
    assert!(
        fired(&mock.requests(), "/mr").is_empty(),
        "the old template fired after the change: {:?}",
        mock.requests()
    );
    // The finished reset clears the flag it was asked with (`reset_requested`).
    wait_for(
        Duration::from_secs(20),
        "reset_requested cleared",
        || async {
            let mut c = db.conn().await;
            let r: Option<i8> = c
                .exec_first(
                    "SELECT reset_requested FROM lazy_wire_fire_rule WHERE name = 'rrul'",
                    (),
                )
                .await
                .unwrap();
            quit(c).await;
            (r == Some(0)).then_some(())
        },
    )
    .await;
    inst.stop().await;
}

/// A configuration the engine cannot read: `reload_config` fails, the WARN says so, and the
/// retry is the 1 s one and not the 120 s poll — proven by what the engine does with a rule the
/// server only lets it read again later. Revoking SELECT on the rule table alone (table-granular
/// grants, which is the only kind a table `REVOKE` can take away) is what makes the load fail;
/// while it fails, the rules already loaded keep firing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_config_reload_retries_in_one_second() {
    let root = need_db!();
    let db = Db::fresh(&root, "rlretry").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('arl', 'hook', 'POST /ar HTTP/1.1\n\n{{join keys \",\"}}', 100)")
        .await;
    db.trigger("arl", "orders", Some("{{new.id}}"), None).await;

    // The engine account, granted table by table: a database-level grant cannot be carved out
    // by a table REVOKE, so this is what makes revoking one config table bite.
    let user = "lwf_rld";
    let mariadb = {
        let mut c = db.conn().await;
        let v: String = c.query_first("SELECT VERSION()").await.unwrap().unwrap();
        quit(c).await;
        v.contains("MariaDB")
    };
    db.exec(&format!("DROP USER IF EXISTS '{user}'@'%'")).await;
    db.exec(&format!(
        "CREATE USER '{user}'@'%' IDENTIFIED BY 'rld-pw-1'"
    ))
    .await;
    db.exec(&format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT{} ON *.* TO '{user}'@'%'",
        if mariadb { ", BINLOG MONITOR" } else { "" }
    ))
    .await;
    db.exec(&format!(
        "GRANT CREATE ON `{}`.* TO '{user}'@'%'",
        db.schema
    ))
    .await;
    // The fence table is created lazily by the engine, and a table-level GRANT needs the table
    // to exist, so it is made here first with the very DDL `fence::probe` would use.
    db.exec("CREATE TABLE IF NOT EXISTS `lazy_wire_fire_fence` (`id` VARCHAR(64) NOT NULL PRIMARY KEY, `n` BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        .await;
    for t in [
        "lazy_wire_fire_target",
        "lazy_wire_fire_rule",
        "lazy_wire_fire_trigger",
        "lazy_wire_fire_status",
        "lazy_wire_fire_fence",
        "customers",
        "orders",
        "order_tags",
    ] {
        db.exec(&format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON `{}`.`{t}` TO '{user}'@'%'",
            db.schema
        ))
        .await;
    }
    let (scheme, rest) = root.split_once("://").unwrap();
    let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    let edb = Db {
        schema: db.schema.clone(),
        dsn: format!(
            "{scheme}://{user}:rld-pw-1@{host}/{}?prefer_socket=false",
            db.schema
        ),
    };
    // The config poll stands at 120 s: only the failure retry can read a config in this test.
    let inst = Instance::ready(config(
        &edb,
        &[
            ("LAZYWF_CONFIG_POLL_MS", "120000"),
            ("LAZYWF_STATUS_MS", "200"),
        ],
    ))
    .await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(Duration::from_secs(20), "the baseline key", || async {
        (fired(&mock.requests(), "/ar").contains(&"1".to_string())).then_some(())
    })
    .await;
    inst.quiet().await;

    // Break: the rule table can no longer be read.
    db.exec(&format!(
        "REVOKE SELECT ON `{}`.`lazy_wire_fire_rule` FROM '{user}'@'%'",
        db.schema
    ))
    .await;
    // The engine's own account names itself in the refusal, which is what makes these lines this
    // test's: no other test creates `lwf_rld`, and no other reason but the refusal is accepted.
    let w0 = warn_lines(&["reloading the configuration", "retrying", user, "denied"]).len();
    let t0 = std::time::Instant::now();
    // A rule added now stays invisible: the load fails before any rule is read.
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('blr', 'hook', 'POST /br HTTP/1.1\n\n{{join keys \",\"}}', 50)")
        .await;
    db.trigger("blr", "orders", Some("{{new.id}}"), None).await;
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    // The rules it already has keep firing (the failed load changes nothing), and it retries at
    // its own 1 s pace, not at the 120 s poll: several attempts within seconds.
    wait_for(Duration::from_secs(30), "the reload retries", || async {
        (warn_lines(&["reloading the configuration", "retrying", user, "denied"]).len() >= w0 + 3)
            .then_some(())
    })
    .await;
    let retrying = t0.elapsed();
    assert!(
        retrying < Duration::from_secs(30),
        "three reload attempts took {retrying:?} with the poll at 120 s: this is not the 1 s retry"
    );
    assert!(
        fired(&mock.requests(), "/ar").contains(&"2".to_string()),
        "the rules already loaded stopped firing while the configuration could not be read"
    );
    assert!(
        !fired(&mock.requests(), "/br").contains(&"2".to_string()),
        "the rule the engine could not read fired anyway: {:?}",
        mock.requests()
    );
    assert!(
        inst.health.is_ready(),
        "the engine died on the failed reload"
    );

    // Restore: the next 1 s retry reads it, and the rule that appeared while the table was
    // closed to the engine now fires — which the 120 s poll could not have done in this time.
    db.exec(&format!(
        "GRANT SELECT ON `{}`.`lazy_wire_fire_rule` TO '{user}'@'%'",
        db.schema
    ))
    .await;
    let granted = std::time::Instant::now();
    wait_for(
        Duration::from_secs(30),
        "the closed rule to be read",
        || async { db.status("blr").await.map(|_| ()) },
    )
    .await;
    let seen = granted.elapsed();
    assert!(
        seen < Duration::from_secs(30),
        "the rule appeared only after {seen:?} with the poll at 120 s: this is not the 1 s retry"
    );
    db.exec("INSERT INTO orders VALUES (3, 1, 'new', 3)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(
        Duration::from_secs(20),
        "the rule read after the restore",
        || async {
            let (af, ap, _, _) = db.status("blr").await?;
            (fired(&mock.requests(), "/br").contains(&"3".to_string()) && (af, ap) >= after)
                .then_some(())
        },
    )
    .await;
    assert!(
        !fired(&mock.requests(), "/br").contains(&"2".to_string()),
        "the key that passed while the rule was unreadable was replayed by a rewind: {:?}",
        mock.requests()
    );
    inst.stop().await;
    db.exec(&format!("DROP USER IF EXISTS '{user}'@'%'")).await;
}

/// The `reset_requested = 0` of a finished reset, refused by the server: the WARN names the
/// statement and the 5 s wait, the flag stays 1 through the wait (the retry is 5 s, not the 1 s
/// the reload uses), and it clears on the next attempt once the table is back — with the reset
/// itself already done and the engine otherwise healthy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_reset_requested_clear_waits_five_seconds() {
    let root = need_db!();
    let db = Db::fresh(&root, "clr5s").await;
    shop(&db).await;
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps = json!([{"once": true, "template": "PURGE /ccache HTTP/1.1\n\n"}]).to_string();
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, reset_requested, debounce_ms) \
         VALUES ('crul', 'hook', 'POST /cr HTTP/1.1\n\n{{join keys \",\"}}', ?, 1, 100)",
        (steps,),
    )
    .await;
    db.trigger("crul", "orders", Some("{{new.id}}"), None).await;
    // The reset's step is unanswered: the reset cannot finish, so the clear cannot be attempted
    // before the table is taken away.
    mock.hold_after(0);
    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(Duration::from_secs(20), "the held reset step", || async {
        (mock.requests().iter().any(|r| r.path == "/ccache")).then_some(())
    })
    .await;
    db.exec("RENAME TABLE lazy_wire_fire_rule TO lazy_wire_fire_rule_off")
        .await;
    // The server refuses the clear with 1146, and that text names the table with this schema in
    // front of it (`Table 'lwf_clr5s.lazy_wire_fire_rule' doesn't exist`): the schema is a needle
    // the line really carries, so a matching line can only be this test's.
    let w0 = warn_lines_in(&db, &["crul", "clearing reset_requested"]).len();
    mock.heal();

    // The reset finishes and the clear of the flag fails FOR the missing table.
    wait_for(
        Duration::from_secs(20),
        "the clear failure named",
        || async {
            (!warn_lines_in(
                &db,
                &["crul", "clearing reset_requested", "retrying in 5 s"],
            )
            .is_empty())
            .then_some(())
        },
    )
    .await;
    let warned = warn_lines_in(&db, &["crul", "clearing reset_requested"]);
    assert!(
        warned.iter().any(|l| l.contains("lazy_wire_fire_rule")),
        "the clear failure does not name the statement's table: {warned:?}"
    );
    // Through the wait it is not retried (the flag is still 1, still only one WARN), and the
    // reset that asked for it is finished.
    sleep_ms(2000).await;
    let now = warn_lines_in(&db, &["crul", "clearing reset_requested"]);
    assert_eq!(
        now.len(),
        w0 + 1,
        "the clear retried inside the 5 s wait: {now:?}"
    );
    let mut c = db.conn().await;
    let held: Option<i8> = c
        .exec_first(
            "SELECT reset_requested FROM lazy_wire_fire_rule_off WHERE name = 'crul'",
            (),
        )
        .await
        .unwrap();
    quit(c).await;
    assert_eq!(
        held,
        Some(1),
        "the flag was cleared while the table was gone"
    );
    let (_, _, rs, _) = db.status("crul").await.unwrap();
    assert!(
        rs.starts_with("done:") || rs.contains("running"),
        "the reset that asked for the clear is not where it should be: {rs}"
    );

    // The table is back: the attempt that the 5 s timer holds does the clear the server refused.
    db.exec("RENAME TABLE lazy_wire_fire_rule_off TO lazy_wire_fire_rule")
        .await;
    let t0 = std::time::Instant::now();
    wait_for(
        Duration::from_secs(30),
        "the flag cleared by the retry",
        || async {
            let mut c = db.conn().await;
            let r: Option<i8> = c
                .exec_first(
                    "SELECT reset_requested FROM lazy_wire_fire_rule WHERE name = 'crul'",
                    (),
                )
                .await
                .unwrap();
            quit(c).await;
            (r == Some(0)).then_some(())
        },
    )
    .await;
    assert!(
        t0.elapsed() >= Duration::from_secs(1),
        "the clear did not wait for its timer: {:?}",
        t0.elapsed()
    );
    assert!(inst.health.is_ready(), "the engine died on the clear");
    // The rule itself still works after all of that.
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;
    db.exec("INSERT INTO orders VALUES (4, 1, 'new', 4)").await;
    let after = binlog_end(&db).await;
    db.tick().await;
    wait_for(Duration::from_secs(20), "a key after the clear", || async {
        let (af, ap, _, _) = db.status("crul").await?;
        (fired(&mock.requests(), "/cr").contains(&"4".to_string()) && (af, ap) >= after)
            .then_some(())
    })
    .await;
    inst.stop().await;
}
// --------------------------------------------------------------------------- binlog continuity at restart
//
// The regions here are the ones the engine runs when the binlog it stored a position in is not
// the binlog it is being asked to read: `choose_start` (src/engine.rs:1139), `lose_continuity`
// and `start_reset` (1227, 1240), and the resume of unfinished resets (1206-1220). What they all
// read is `lazy_wire_fire_status` — the positions, the server identity and the per-rule reset
// state the engine wrote on its last shutdown — so a test that stops the engine and edits its own
// rows reaches the same arms a purged binlog or a promoted server would, without deleting a file
// from the server every worker of this repo shares. That is a deliberate limit and it is stated
// where it belongs: these tests prove what the engine *does* with a position it cannot honour.
// They do not prove the server ever reports one gone — that is the source layer's
// `is_missing_binlog` (`src/source/mod.rs:714`), reachable only by deleting a binlog file, which
// on a shared server is barred; the chaos design proposed separately (PR-6) gives it a container of
// its own instead.

/// The `host:port` a root DSN points at, as the chaos harness reads it (`tests/chaos.rs:43`).
fn server_host(root: &str) -> String {
    let (_, rest) = root.split_once("://").expect("dsn scheme");
    rest.rsplit_once('@').map_or(rest, |(_, h)| h).to_string()
}

/// The same schema reached through a proxy: credentials kept, host:port replaced
/// (`tests/chaos.rs:49`).
fn via(root: &str, proxy: std::net::SocketAddr, schema: &str) -> String {
    let (scheme, rest) = root.split_once("://").expect("dsn scheme");
    let creds = rest.rsplit_once('@').map_or("", |(c, _)| c);
    format!("{scheme}://{creds}@{proxy}/{schema}?prefer_socket=false")
}

/// One column of the instance (`*`) status row, as the engine wrote it.
async fn star_column(db: &Db, column: &str) -> String {
    assert!(
        column
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "not a column name: {column}"
    );
    let mut c = db.conn().await;
    let v: Option<String> = c
        .query_first(format!(
            "SELECT {column} FROM lazy_wire_fire_status WHERE rule = '*'"
        ))
        .await
        .unwrap();
    quit(c).await;
    v.unwrap_or_default()
}

/// The server's own identity, the way the engine reads it (`SELECT CAST(@@server_uuid AS CHAR)`,
/// or the id pair on MariaDB): a test that forges a saved identity has to know what a real one is
/// before it can tell the two apart in a WARN.
async fn real_identity(db: &Db) -> String {
    let mut c = db.conn().await;
    let mariadb: String = c
        .query_first("SELECT VERSION()")
        .await
        .unwrap()
        .unwrap_or_default();
    let q = if mariadb.contains("MariaDB") {
        "SELECT CAST(CONCAT(@@server_id, ':', @@gtid_domain_id) AS CHAR)"
    } else {
        "SELECT CAST(@@server_uuid AS CHAR)"
    };
    let v: Option<String> = c.query_first(q).await.unwrap();
    quit(c).await;
    v.unwrap_or_default()
}

/// One row of `lazy_wire_fire_status`, in the order the query below reads it. Named rather than
/// spelled out there because a six-element tuple is what `clippy::type_complexity` is there to
/// catch, the way `tests/common/fault.rs` names its `SampledPhase`.
type StatusRow = (String, String, u64, String, Option<String>, Option<String>);

/// Every status row as the engine wrote it: where it believes it is, what each rule's reset state
/// is, what it last failed on, and the identity it saved. A wait that gives up prints this beside
/// the mock's traffic — an engine parked at a position the server has moved past and an engine that
/// is reading the stream but holding a rule's keys look the same from outside, and which of them it
/// is decides whether a red is a test bug or a product bug.
async fn status_dump(db: &Db) -> String {
    let mut c = db.conn().await;
    let rows: Vec<StatusRow> = c
        .query(
            "SELECT `rule`, `binlog_file`, `binlog_pos`, `reset_state`, `last_error`, \
             `server_identity` FROM `lazy_wire_fire_status` ORDER BY `rule`",
        )
        .await
        .unwrap_or_default();
    quit(c).await;
    rows.iter()
        .map(|(rule, file, pos, state, err, id)| {
            format!(
                "{rule}@{file}:{pos} reset={state} error={error} identity={identity}",
                error = err.as_deref().unwrap_or("-"),
                identity = id.as_deref().unwrap_or("-"),
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

/// A rule with `reset_steps` and one without, both watching `shop`'s tables and both sending to
/// the mock: the two ways a loss of continuity is handled. The step's path is its own, so an
/// answered `/reset-<rule>` is the step and nothing else (see [`answer_log`]).
async fn pair_of_rules(db: &Db, mock: &Mock, with_steps: &str, without_steps: &str) {
    db.target("hook", "http", &mock.url(), None, "{}").await;
    let steps =
        json!([{"once": true, "template": format!("PURGE /reset-{with_steps} HTTP/1.1\n\n")}])
            .to_string();
    db.exec_params(
        &format!(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) \
             VALUES ('{with_steps}', 'hook', 'POST /{with_steps} HTTP/1.1\n\n{{{{join keys \",\"}}}}', ?, 50)"
        ),
        (steps,),
    )
    .await;
    db.trigger(with_steps, "orders", Some("{{new.id}}"), None)
        .await;
    db.exec(&format!(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) \
         VALUES ('{without_steps}', 'hook', 'POST /{without_steps} HTTP/1.1\n\n{{{{join keys \",\"}}}}', 50)"
    ))
    .await;
    db.trigger(without_steps, "customers", Some("{{new.id}}"), None)
        .await;
}

/// The engine wrote a position into a file the server no longer lists. At start the rule cannot
/// resume from it, so the rule with `reset_steps` re-reads its table from scratch and the rule
/// without them says out loud that what happened in between is not covered
/// (`choose_start`'s `lost` list, `lose_continuity`'s two arms).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_position_the_binlog_no_longer_has_resets_the_rule() {
    let root = need_db!();
    let db = Db::fresh(&root, "gonepos").await;
    shop(&db).await;
    let mock = Mock::start().await;
    pair_of_rules(&db, &mock, "goneo", "gonec").await;
    let log = answer_log(&mock);

    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(20),
        "both rules before the restart",
        || async {
            (answered(&log, "/reset-goneo") >= 1
                && fired(&mock.requests(), "/goneo") == ["1"]
                && fired(&mock.requests(), "/gonec") == ["1"])
            .then_some(())
        },
    )
    .await;
    inst.quiet().await;
    let reset_base = answered(&log, "/reset-goneo");
    inst.stop().await;

    // A file name this server cannot have. The instance's own row keeps a position the server
    // does have, so it is these two rules that lost theirs and not the run as a whole.
    let gone = "lwf-gone-binlog.000001";
    db.exec(&format!(
        "UPDATE lazy_wire_fire_status SET binlog_file = '{gone}', binlog_pos = 4 \
         WHERE rule IN ('goneo','gonec')"
    ))
    .await;
    let base = warn_baselines(&["rule=gonec"], &["no longer available"]);
    // The row read below has to be one the *new* engine wrote: the row the old run left behind
    // said `none` (the rule had never lost anything), and reading that would prove nothing.
    // `persist` rewrites every row at start and then whenever its position moves, else every
    // `STATUS_HEARTBEAT`, so polling for a newer row settles.
    let after = server_now(&db).await;

    let inst = Instance::ready(config(&db, &[])).await;
    // The rule that can cover the gap does: its step is answered again, after the restart.
    wait_for(
        Duration::from_secs(25),
        "the reset of goneo after the restart",
        || async { (answered(&log, "/reset-goneo") > reset_base).then_some(()) },
    )
    .await;
    // The rule that cannot says why, and names the position it lost.
    let lost = wait_for(
        Duration::from_secs(25),
        "the continuity WARN of gonec",
        || async { warn_grew(&["rule=gonec"], &["no longer available"], &base) },
    )
    .await;
    assert!(
        lost.contains(gone) && lost.contains("no reset_steps"),
        "the WARN names neither the lost position nor what is uncovered: {lost}"
    );
    let recorded = wait_for(
        Duration::from_secs(60),
        "gonec's status row to record the loss",
        || async {
            status_row(&db, "gonec", &after)
                .await
                .map(|(_, _, s, _)| s)
                .filter(|s| s.starts_with("missing:"))
        },
    )
    .await;
    assert!(
        recorded.contains("no longer available"),
        "the rule without reset_steps did not record which position is gone: {recorded}"
    );

    // The stream itself went on from the instance's position, not from the forged one: a key
    // written now reaches both rules.
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 2)").await;
    db.exec("INSERT INTO customers VALUES (2, 'bob')").await;
    wait_for(
        Duration::from_secs(20),
        "both rules after the restart",
        || async {
            let r = mock.requests();
            (fired(&r, "/goneo").contains(&"2".to_string())
                && fired(&r, "/gonec").contains(&"2".to_string()))
            .then_some(())
        },
    )
    .await;
    inst.stop().await;
}

/// The saved server identity is not this server's: continuity is gone for every rule, the engine
/// starts from the current position rather than the saved one, and what the two disagreed about
/// is covered by the reset steps — or explicitly not covered, for the rule that has none.
///
/// The identity column is the engine's own writing, and the test reads it back before forging it,
/// so the comparison is not between two values nothing ever set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_server_identity_that_is_not_this_server_starts_from_now() {
    let root = need_db!();
    let db = Db::fresh(&root, "idchange").await;
    shop(&db).await;
    let mock = Mock::start().await;
    pair_of_rules(&db, &mock, "ideto", "idetc").await;
    let log = answer_log(&mock);

    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'ann')").await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(20),
        "both rules before the restart",
        || async {
            (answered(&log, "/reset-ideto") >= 1
                && fired(&mock.requests(), "/ideto") == ["1"]
                && fired(&mock.requests(), "/idetc") == ["1"])
            .then_some(())
        },
    )
    .await;
    inst.quiet().await;
    let reset_base = answered(&log, "/reset-ideto");
    let real = real_identity(&db).await;
    let saved = wait_for(
        Duration::from_secs(20),
        "the instance row to carry this server's identity",
        || async {
            let s = star_column(&db, "server_identity").await;
            (!s.is_empty()).then_some(s)
        },
    )
    .await;
    assert_eq!(
        saved, real,
        "the engine saved an identity that is not the server's"
    );
    inst.stop().await;

    // Rows written while the engine is down, one per table so that both rules have something the
    // binlog holds from before the engine started: they sit behind the saved position, and the
    // point of this test is that the engine does not read them.
    db.exec("INSERT INTO orders VALUES (9, 1, 'new', 9)").await;
    db.exec("INSERT INTO customers VALUES (9, 'dan')").await;
    let other = "00000000-0000-0000-0000-00000000dead";
    db.exec(&format!(
        "UPDATE lazy_wire_fire_status SET server_identity = '{other}' WHERE rule = '*'"
    ))
    .await;
    let base = warn_baselines(&["rule=idetc"], &["the server changed"]);
    // Same reason as in the test above: the row that must say `missing:` is one the new engine
    // wrote, not the row the first run left behind.
    let after = server_now(&db).await;

    let inst = Instance::ready(config(&db, &[])).await;
    let lost = wait_for(
        Duration::from_secs(25),
        "the continuity WARN of idetc",
        || async { warn_grew(&["rule=idetc"], &["the server changed"], &base) },
    )
    .await;
    assert!(
        lost.contains(other) && lost.contains(&real),
        "the WARN does not name both identities (saved {other}, this server {real}): {lost}"
    );
    wait_for(
        Duration::from_secs(25),
        "the reset of ideto after the restart",
        || async { (answered(&log, "/reset-ideto") > reset_base).then_some(()) },
    )
    .await;
    let recorded = wait_for(
        Duration::from_secs(60),
        "idetc's status row to record the loss",
        || async {
            status_row(&db, "idetc", &after)
                .await
                .map(|(_, _, s, _)| s)
                .filter(|s| s.starts_with("missing: the server changed"))
        },
    )
    .await;
    assert!(
        recorded.contains(other) && recorded.contains(&real),
        "the row does not name both identities (saved {other}, this server {real}): {recorded}"
    );

    // From the current position means from now: what the engine missed while it was down is not
    // replayed, and what is written now is delivered. Asked of each rule separately, because the
    // two are handled by different arms — `ideto` re-read its table, `idetc` recorded `missing:` —
    // and one wait over both says only that something went quiet. Written out rather than through
    // `wait_for` so the timeout can ask the server and the engine where they each are: which of the
    // two rules stopped delivering, and whether the stream moved, is the difference between a test
    // bug and a product bug, and it cannot be recovered afterwards.
    // One row in each of the two tables, because that is where the two rules live: `pair_of_rules`
    // triggers `ideto` on `orders` and `idetc` on `customers`, so asking both for key 10 after a
    // write to one of them would be asking `idetc` for a row the binlog never held.
    db.exec("INSERT INTO orders VALUES (10, 1, 'new', 10)")
        .await;
    db.exec("INSERT INTO customers VALUES (10, 'kim')").await;
    for path in ["/ideto", "/idetc"] {
        let until = std::time::Instant::now() + deadline(Duration::from_secs(20));
        loop {
            if fired(&mock.requests(), path).contains(&"10".to_string()) {
                break;
            }
            if std::time::Instant::now() > until {
                panic!(
                    "no key 10 on {path} after 20s: the mock saw {seen}; the engine is \
                     ready={ready} and reports {report}; {rows}",
                    seen = traffic(&mock, &log),
                    ready = inst.health.is_ready(),
                    report = inst.health.status(),
                    rows = status_dump(&db).await,
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let r = mock.requests();
    for path in ["/ideto", "/idetc"] {
        assert!(
            !fired(&r, path).contains(&"9".to_string()),
            "{path} replayed what the binlog held from before the engine started: {r:?}"
        );
    }
    inst.stop().await;
}

/// A reset a previous run never finished is finished at start, and only for the rule whose status
/// row says so: a restart is no reason to re-read every table.
///
/// `choose_start` decides this from `reset_state` alone, so the row is the whole trigger. The mock
/// holds every request, which catches the resumed run in flight — its step sent, never answered —
/// and the rule whose reset had been recorded as done is the control: it must stay done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_a_restart_interrupted_finishes_itself() {
    let root = need_db!();
    let db = Db::fresh(&root, "intreset").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    for name in ["resumo", "resumkeep"] {
        let steps =
            json!([{"once": true, "template": format!("PURGE /reset-{name} HTTP/1.1\n\n")}])
                .to_string();
        db.exec_params(
            &format!(
                "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, debounce_ms) \
                 VALUES ('{name}', 'hook', 'POST /{name} HTTP/1.1\n\n{{{{join keys \",\"}}}}', ?, 50)"
            ),
            (steps,),
        )
        .await;
        db.trigger(name, "orders", Some("{{new.id}}"), None).await;
    }
    let log = answer_log(&mock);

    let inst = Instance::ready(config(&db, &[])).await;
    wait_for(
        Duration::from_secs(25),
        "both resets of the first start",
        || async {
            (answered(&log, "/reset-resumo") >= 1 && answered(&log, "/reset-resumkeep") >= 1)
                .then_some(())
        },
    )
    .await;
    inst.quiet().await;
    let base = answered(&log, "/reset-resumo");
    inst.stop().await;

    // One run interrupted, one finished: nothing else about the two rules differs.
    db.exec("UPDATE lazy_wire_fire_status SET reset_state = 'pending: the step that never came back' WHERE rule = 'resumo'")
        .await;
    let mut c = db.conn().await;
    let finished: Vec<String> = c
        .query("SELECT reset_state FROM lazy_wire_fire_status WHERE rule = 'resumkeep'")
        .await
        .unwrap();
    quit(c).await;
    assert!(
        finished[0].starts_with("done:"),
        "the control rule's reset is not recorded as finished: {}",
        finished[0]
    );

    mock.clear();
    mock.hold_after(0);
    // Every row read below is one this run wrote: `after` is taken before the engine comes up, so
    // `status_row` will not hand back the rows the first run left behind (the resumed rule's row
    // already said `pending:` before the restart, which is exactly the row that must not be
    // mistaken for the engine noticing).
    let after = server_now(&db).await;
    let inst = Instance::start(config(&db, &[]));
    // The unfinished one is picked up; the finished one is left alone.
    let running = wait_for(
        Duration::from_secs(60),
        "the resumed reset in flight",
        || async {
            status_row(&db, "resumo", &after)
                .await
                .map(|(_, _, s, _)| s)
                .filter(|s| s.starts_with("pending:") || s.starts_with("running:"))
        },
    )
    .await;
    // The control, read while the mock still holds every request: a rule that had been reset
    // wrongly would say `pending:` or `running:` here, because its step could not be answered.
    let kept = wait_for(
        Duration::from_secs(60),
        "resumkeep's own row, rewritten by this run",
        || async {
            status_row(&db, "resumkeep", &after)
                .await
                .map(|(_, _, s, _)| s)
        },
    )
    .await;
    assert!(
        kept.starts_with("done:"),
        "the rule whose reset had finished was reset too: {kept}"
    );
    assert!(
        mock.requests().iter().any(|r| r.path == "/reset-resumo"),
        "the resumed reset never sent its step: {}",
        traffic(&mock, &log)
    );

    // Let the step through: the run completes and the row stops claiming a reset is going on.
    mock.heal();
    wait_for(
        Duration::from_secs(60),
        "the resumed reset of resumo to finish (its step answered, its row saying done)",
        || async {
            status_row(&db, "resumo", &after)
                .await
                .map(|(_, _, s, _)| s)
                .filter(|s| s.starts_with("done:") && answered(&log, "/reset-resumo") > base)
                .map(|_| ())
        },
    )
    .await;
    assert!(
        running.starts_with("pending:") || running.starts_with("running:"),
        "{running}"
    );

    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(20),
        "keys after the resumed reset",
        || async { (fired(&mock.requests(), "/resumo") == ["1"]).then_some(()) },
    )
    .await;
    inst.quiet().await;
    inst.stop().await;
}

/// The stream itself dies on the wire three different ways and the engine comes back from each
/// without losing its position: a dump connection reset under it, a port that refuses the
/// reconnect outright, and a connection that stays open but says nothing at all.
///
/// Only the binlog connection goes through the proxy (`binlog_dsn`), so the engine's queries,
/// status writes and instance lock keep running while the stream is broken: what is under test is
/// the reconnect loop, not the server. The three WARNs here carry nothing of the test's own, so
/// each is read as a growth over a baseline (`warn_baselines` / `warn_grew`) and the claim itself
/// rests on what the test owns — the proxy's own counters, and the key the mock answered after
/// every fault was healed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_binlog_stream_recovers_from_wire_faults() {
    let root = need_db!();
    let db = Db::fresh(&root, "streamfault").await;
    shop(&db).await;
    let mock = Mock::start().await;
    db.target("hook", "http", &mock.url(), None, "{}").await;
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES ('faulto', 'hook', 'POST /faulto HTTP/1.1\n\n{{join keys \",\"}}', 50)")
        .await;
    db.trigger("faulto", "orders", Some("{{new.id}}"), None)
        .await;

    let binp = common::fault::FaultProxy::start(&server_host(&root), "binlog", None).await;
    let mut cfg = config(&db, &[]);
    cfg.binlog_dsn = via(&root, binp.addr(), &db.schema);
    let inst = Instance::ready(cfg).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(20),
        "the stream through the proxy",
        || async { (fired(&mock.requests(), "/faulto") == ["1"]).then_some(()) },
    )
    .await;
    assert!(
        binp.conns() >= 1,
        "the engine never connected through the proxy"
    );

    // 1. The dump connection is reset while the stream is up. The reader's error says nothing
    //    about the position, so the engine reconnects from where it stood. `conns` counts every
    //    connection this proxy accepted, refused ones included, and only this test's engine dials
    //    this port, so a number that grows is the engine retrying — not another test's WARN.
    let conns = binp.conns();
    let base = warn_baselines(&["binlog stream error"], DEAD_CONNECTION);
    binp.cut_all();
    let line = wait_for(Duration::from_secs(30), "the stream-error WARN", || async {
        warn_grew(&["binlog stream error"], DEAD_CONNECTION, &base)
    })
    .await;
    assert!(line.contains("reconnecting"), "{line}");
    wait_for(
        Duration::from_secs(30),
        "the engine to retry the cut port",
        || async { (binp.conns() > conns).then_some(binp.conns()) },
    )
    .await;
    binp.heal();
    db.exec("INSERT INTO orders VALUES (2, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(30),
        "the key after the reset",
        || async { (fired(&mock.requests(), "/faulto").contains(&"2".to_string())).then_some(()) },
    )
    .await;

    // 2. The port refuses: the reconnect itself fails and the position is kept, so the engine
    //    retries with the capped backoff until the port answers again.
    let conns = binp.conns();
    let base = warn_baselines(&["binlog connect failed"], DEAD_CONNECTION);
    // Reset whatever connection the engine raised in the recovery above and keep refusing new
    // ones. The engine sits in a pause after the previous fault, so arming once can miss it.
    for _ in 0..15 {
        binp.cut_all();
        binp.refuse(true);
        if warn_grew(&["binlog connect failed"], DEAD_CONNECTION, &base).is_some() {
            break;
        }
        sleep_ms(200).await;
    }
    let line = wait_for(
        Duration::from_secs(40),
        "the connect-failed WARN",
        || async { warn_grew(&["binlog connect failed"], DEAD_CONNECTION, &base) },
    )
    .await;
    assert!(line.contains("retrying"), "{line}");
    assert!(
        binp.conns() > conns,
        "the engine never retried the refused port ({} connections at the proxy since \
         arming: {conns})",
        binp.conns() - conns
    );
    binp.heal();
    db.exec("INSERT INTO orders VALUES (3, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(30),
        "the key after the refused port",
        || async { (fired(&mock.requests(), "/faulto").contains(&"3".to_string())).then_some(()) },
    )
    .await;

    // 3. The connection stays open and goes silent: past the silence window the engine stops
    //    trusting it and reconnects (the window is max(3 x heartbeat, 15 s), so this phase is
    //    the slow one and sits last; a stalled proxy still counts the connect it freezes).
    let conns = binp.conns();
    let base = warn_baselines(&["no binlog event or heartbeat"], &["reconnecting"]);
    binp.stall(true);
    let line = wait_for(
        Duration::from_secs(90),
        "the silence WARN (the window is max(3 x heartbeat, 15 s))",
        || async { warn_grew(&["no binlog event or heartbeat"], &["reconnecting"], &base) },
    )
    .await;
    assert!(line.contains("no binlog event or heartbeat"), "{line}");
    wait_for(
        Duration::from_secs(60),
        "the engine to drop the silent connection and dial again",
        || async { (binp.conns() > conns).then_some(binp.conns()) },
    )
    .await;
    binp.heal();
    db.exec("INSERT INTO orders VALUES (4, 1, 'new', 1)").await;
    wait_for(
        Duration::from_secs(40),
        "the key after the silence",
        || async { (fired(&mock.requests(), "/faulto").contains(&"4".to_string())).then_some(()) },
    )
    .await;

    inst.quiet().await;
    inst.stop().await;
}

// Two more tests of this section were written, run, and removed on what the servers answered —
// not on doubt about the product, and not on the assertion being inconvenient. They asked a
// replica question of the *server*: that a second dump thread registering a `server_id` an
// incumbent already holds is turned away, MySQL with 1236 ("A slave with the same
// server_uuid/server_id as this slave has connected to the master") and MariaDB with 4052
// (ER_SLAVE_SAME_ID). On that answer the rest followed: the engine, refused on an id it had not
// been given, rolls a new one (`reroll_server_id`, src/engine.rs:1943), and refused on the id its
// operator gave it, keeps that id, says so ("another replica connects with the same server id; set
// a unique LAZYWF_SERVER_ID", src/engine.rs:1947), and retries.
//
// Neither server here answers that way. Both let the newcomer register, and both dump threads then
// read the log: the engine streamed on the duplicated id for the whole window of each test, and
// nothing from that chain appears anywhere in either log — no 1236, no 4052, no "same server_uuid",
// not the WARN the refusal path writes — although every line the engines of both schemas wrote is
// in them. The positions are the ones each engine's own start banner reported, read from the log of
// the job that ran against the server that printed them (run 45045903180: the
// `integration (MySQL 8.4.13 (LTS), MariaDB 10.11.19 (LTS) + targets)` job for MySQL, the
// `integration (MariaDB 10.6.28 (AWS-only))` job for MariaDB):
//
//   MySQL 8.4.13     rival holds the engine's id:   streamed at binlog.000003:260320
//                    rival holds a fixed id:        streamed at binlog.000003:30817
//   MariaDB 10.6.28  rival holds the engine's id:   streamed at mysql-bin.000002:369998
//                    rival holds a fixed id:        streamed at mysql-bin.000002:21940
//
// (MariaDB 10.11.19 is not in that list, and the reason is worth keeping: `make test-e2e-all` stops
// at the first red, and at the head that still carried these two tests only one binary run happened
// in that job — one `test result:` line, and `binlog.000003` names and no `mysql-bin.` ones — so the
// pair was answered by MySQL 8.4.13 and never reached 10.11.19. The four tests that do ship run
// against 10.11.19 (the same job is green there, `96 passed; 0 failed; 2 ignored` twice over), but
// none of them asks the server the duplicate-id question, so nothing here claims to know how
// 10.11.19 answers it.)
//
// So what is untested, named and counted rather than left as a passing test that proves less than it
// reads as: the roll on a refused unfixed id (src/engine.rs:1943), and the refusal of a fixed id
// that keeps retrying (src/source/mod.rs:750 and src/engine.rs:1947). Both are servers that do not
// collide, and no test can make a server collide; for them to be reachable the server would have to
// answer `get_binlog_stream` (src/binlog/stream.rs:258) with a duplicate-id error, which is exactly
// what it does not do. A probe that would settle what the servers do answer is designed but not
// run: open the product's own `binlog::BinlogReader` twice with one `server_id` from the active
// log's start with a heartbeat, take the source's own `@@server_id` from the server rather than
// assume it, and print `SHOW REPLICAS`/`SHOW SLAVE HOSTS` before and after each attempt so the
// server itself says which ids registered and whether the incumbent survived. It is not in the tree
// because nothing in CI runs an `#[ignore]`d test, so it could only ever be run by hand, and a
// witness nobody can re-run is not evidence.
//
// The two arms that need a purged binlog (`on_missing_binlog`, src/engine.rs:1312, and `rebase`,
// src/engine.rs:1273) are out of a test's reach for the same kind of reason, and the reason is the
// point: no test can cause it. A test cannot delete from the server's log, and these servers expire
// it on their own schedule, so nothing here can put a stored coordinate behind the end of the log.
// That is a statement about what a test on a shared server may do, not about the code — nothing here
// says those arms are unreachable in production, and they are listed as a gap rather than tested.
