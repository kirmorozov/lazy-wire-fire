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
    let err = lazy_wire_fire::engine::run(cfg, health, rx)
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
    use lazy_wire_fire::redis::{Client, RedisAddr, Reply};
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

fn bulk(r: &lazy_wire_fire::redis::Reply) -> String {
    match r {
        lazy_wire_fire::redis::Reply::Bulk(Some(b)) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// `XRANGE` reply as entries, in stream order.
fn stream_entries(reply: lazy_wire_fire::redis::Reply) -> Vec<Entry> {
    use lazy_wire_fire::redis::Reply;
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
async fn entries_at_least(redis: &lazy_wire_fire::redis::Client, n: usize) -> Vec<Entry> {
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
    use lazy_wire_fire::redis::{Client, RedisAddr};
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
/// keeps the floor the minimum, `apply` in `src/engine.rs`).
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
    // The XA opens first: its start is the position the floor must stay at.
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
    // A filler event, then the plain transaction, then the COMMIT lands after both: the
    // XA's rows and its (older) start reach the engine at the COMMIT.
    db.exec("UPDATE customers SET name = 'filler' WHERE id = 1")
        .await;
    let plain_at = binlog_end(&db).await;
    assert!(xa_at < plain_at, "the filler must move the position");
    db.exec("INSERT INTO orders VALUES (32, 1, 'n', 1)").await;
    c.query_drop("XA COMMIT 'lwf3'").await.unwrap();
    // The flush is refused, both keys stay pending, and the rule's saved position must
    // hold at the XA start — not climb to the plain transaction's.
    wait_for(Duration::from_secs(10), "the refused flush", || async {
        (!mock.requests().is_empty()).then_some(())
    })
    .await;
    sleep_ms(600).await;
    let (f, p, _, err) = db.status("o").await.unwrap();
    assert!(
        err.as_deref().is_some_and(|e| e.contains("500")),
        "the flush was not refused: {err:?}"
    );
    assert!(
        (f.clone(), p) <= xa_at,
        "the rule position climbed past the XA start while its keys are unflushed: \
         saved ({f},{p}), xa start {xa_at:?}"
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
    wait_for(
        Duration::from_secs(10),
        "the change during the copy and the insert after the swap",
        || async {
            let o = fired(&mock.requests(), "/o");
            (o.contains(&"1".to_string()) && o.contains(&"3".to_string())).then_some(())
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
    db.exec("INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms, max_delay_ms) VALUES ('o', 'hook', 'POST /k HTTP/1.1\n\n{{join keys}}', 1000, 1500)").await;
    db.trigger("o", "orders", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 1, 'n', 0)").await;
    let t0 = std::time::Instant::now();
    let mut first = None;
    for i in 0..20 {
        db.exec(&format!("UPDATE orders SET total = {i} WHERE id = 1"))
            .await;
        if first.is_none() && !mock.requests().is_empty() {
            first = Some(t0.elapsed());
        }
        sleep_ms(200).await;
    }
    let first = first.expect("no fire while the key kept changing");
    assert!(
        first < Duration::from_millis(2500),
        "first fire after {first:?}"
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
        r#"{"header": "X-Api-Key", "value": "k3y", "note": "visible"}"#,
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
    sleep_ms(300).await;
    let (_, _, _, err) = db.status("o").await.unwrap();
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

    let vars = json!({"username": "lwftest", "password": "lwftest"});
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
    let vars = json!({"username": "lwftest", "password": "lwftest"});
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
    let vars = json!({"username": "lwftest", "password": "lwftest"});
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
            lazy_wire_fire::kafka::partition_of(&key, &pids),
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
        .map(|k| (k.to_string(), lazy_wire_fire::kafka::partition_of(k, &pids)))
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
