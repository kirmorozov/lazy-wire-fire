//! End to end: a MySQL-family server (`LAZYWF_TEST_DSN`), the engine in-process, and HTTP,
//! Redis and S3 targets. `make servers test-e2e`.

mod common;

use std::time::Duration;

use common::*;
use mysql_async::prelude::*;
use serde_json::{json, Value};

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
        Some("2xx and errors == false"),
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
        "INSERT INTO lazy_wire_fire_rule (name, target, selects, template, debounce_ms, max_delay_ms) VALUES ('orders', 'search', ?, ?, 200, 2000)",
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

    // A change in a related table reaches the orders through keys_sql.
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

    // A burst of changes to one key fires it once (or twice with a trailing fire).
    mock.clear();
    for i in 0..30 {
        db.exec(&format!("UPDATE orders SET total = {i} WHERE id = 11"))
            .await;
    }
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
    assert!(fires <= 3, "30 updates fired {fires} times");

    // `_op`: cancelled orders and orders gone are deleted.
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
         "keys_path": "ids[]", "next_path": "next",
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
    assert_eq!(strays, vec!["99".to_string(), "98".to_string()]);
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
    db.target("cache", "redis", endpoint, None, &vars.to_string())
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

/// Magento-style tag purging through a real Varnish (`LAZYWF_TEST_VARNISH`, compose profile
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
            ("X-Magento-Tags".into(), format!("cat_p_{n},cat_c_9")),
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
    let steps = json!([{"once": true, "template": "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Magento-Tags-Pattern: .*\n"}]);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, reset_steps, batch_max_bytes, debounce_ms) VALUES ('purge', 'varnish', ?, ?, 7000, 100)",
        (
            "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Magento-Tags-Pattern: {{#each keys}}((^|,){{regex_escape this}}(,|$)){{#unless @last}}|{{/unless}}{{/each}}\n",
            steps.to_string(),
        ),
    )
    .await;
    db.trigger("purge", "stock", Some("cat_p_{{new.product_id}}"), None)
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
    use lazy_wire_fire::aws::Credentials;
    use lazy_wire_fire::http::{BaseUrl, Client, RawRequest};
    use lazy_wire_fire::tls::TlsSpec;
    let root = need_db!();
    let db = Db::fresh(&root, "s3").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_TEST_S3_AK", "lazywfaccess");
    std::env::set_var("LWF_TEST_S3_SK", "lazywfsecret");
    let creds = Credentials {
        access_key: "lazywfaccess".into(),
        secret_key: "lazywfsecret".into(),
        session_token: None,
        expires: None,
    };
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
        lazy_wire_fire::s3::sign(
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
        Some("200, 204"),
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
    let index = format!("lwf_items_{}_{}", e.name, std::process::id());
    let t = Duration::from_secs(10);
    client
        .send(&call("DELETE", &format!("/{index}"), ""), t)
        .await
        .unwrap();
    let r = client
        .send(
            &call(
                "PUT",
                &format!("/{index}"),
                r#"{"mappings":{"properties":{"title":{"type":"keyword"}}}}"#,
            ),
            t,
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
                t,
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
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, expect, vars) VALUES ('os', ?, ?, ?, '2xx and errors == false', ?)",
        (e.kind, &e.url, e.tls.clone(), e.vars.to_string()),
    )
    .await;
    let list = if e.scroll {
        "{{#if reset.next}}POST /_search/scroll HTTP/1.1\nContent-Type: application/json\n\n{\"scroll\":\"1m\",\"scroll_id\":{{json reset.next}}}{{else}}POST /{{vars.index}}/_search?scroll=1m HTTP/1.1\nContent-Type: application/json\n\n{\"size\":1000,\"_source\":false,\"sort\":[\"_doc\"]}{{/if}}"
    } else {
        "POST /{{vars.index}}/_search HTTP/1.1\nContent-Type: application/json\n\n{\"size\":1000,\"_source\":false,\"sort\":[{\"_id\":\"asc\"}]{{#if reset.next}},\"search_after\":{{json reset.next}}{{/if}}}"
    };
    let next = if e.scroll {
        "_scroll_id"
    } else {
        "hits.hits[-1].sort"
    };
    let steps = json!([
        {"for_each": "snapshot"},
        {"for_each": "stray", "list_template": list, "keys_path": "hits.hits[]._id", "next_path": next}
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
