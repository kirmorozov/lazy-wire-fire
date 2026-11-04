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
    let r = client
        .send(
            &call(
                "PUT",
                &format!("/{index}"),
                r#"{"mappings":{"properties":{"title":{"type":"keyword"}}}}"#,
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

/// A binlog event of a transaction the engine reads as foreign: any table but its own status
/// and fence tables, or a statement other than the `BEGIN` of an autocommit transaction.
fn foreign(event: &(u64, String, String)) -> bool {
    let (_, kind, info) = event;
    match kind.as_str() {
        "Table_map" => {
            !(info.contains("lazy_wire_fire_status") || info.contains("lazy_wire_fire_fence"))
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

    // The rule's position may pass a fence write only over a foreign transaction. The suite runs
    // other tests on this server at the same time, and their transactions are foreign too, so
    // the check is on the binlog: a position past a fence write with nothing foreign in between
    // would mean the fence moved it.
    let (file, p1, _, _) = db.status("i").await.expect("status row");
    assert_eq!(file, start.0, "the test window stays in one binlog file");
    let events = binlog_events(&db, &start.0, start.1).await;
    let fences: Vec<u64> = events
        .iter()
        .filter(|e| e.1 == "Table_map" && e.2.contains("lazy_wire_fire_fence"))
        .map(|e| e.0)
        .collect();
    assert!(
        fences.len() >= 5,
        "the fence writes are in the binlog: {fences:?}"
    );
    if let Some(last) = fences.iter().copied().filter(|&f| f < p1).max() {
        assert!(
            events.iter().any(|e| e.0 > last && e.0 < p1 && foreign(e)),
            "the position {p1} passed the fence write at {last} with no foreign transaction between"
        );
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
