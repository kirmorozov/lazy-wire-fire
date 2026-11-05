//! No secret in the logs at any level. This binary owns the global subscriber: the production
//! filter (`lazy_wire_fire::logging::filter`) at `trace`, with `RUST_LOG` set to try to re-enable
//! the capped dependency targets, writing to a buffer. Representative flows run with fake
//! secrets (target credentials, an S3 session token, a SQL and a Redis password, a credential
//! inside an endpoint), and the captured output must contain none of them, nor the DSN password
//! (when it is long enough to tell apart from ordinary text). `make servers test-e2e`.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use common::*;
use lazy_wire_fire::http::{BaseUrl, Client, RawRequest};
use lazy_wire_fire::tls::TlsSpec;
use serde_json::json;

/// Fake secrets: each is unique, so finding one in the output is a leak, not a coincidence.
const BASIC_PW: &str = "LWFSECRET-basic-pw-3c1";
const BEARER_TOKEN: &str = "LWFSECRET-bearer-7e8d";
const HEADER_VALUE: &str = "LWFSECRET-hdr-2a9b";
const S3_SESSION: &str = "LWFSECRET-s3-session-4f6";
const SQL_PW: &str = "LWFSECRET-sql-pw-8a0";
const URL_PW: &str = "LWFSECRET-url-pw-6b7";
/// The S3 test server's secret key (`docker-compose.test.yml`, the `s3` service).
const S3_SECRET: &str = "lazywfsecret";
/// The Redis test server's password (`requirepass`); changing it is a global setting of a
/// shared server, so it stays as the e2e suite has it.
const REDIS_PW: &str = "redispw";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

static CAPTURE: std::sync::OnceLock<Capture> = std::sync::OnceLock::new();
static INSTALL: Once = Once::new();

/// The process's log output so far.
fn captured() -> String {
    let c = CAPTURE.get().expect("subscriber installed");
    String::from_utf8_lossy(&c.0.lock().unwrap()).to_string()
}

/// The global subscriber: the production filter at `trace` (with the caps), into the buffer.
fn install() -> &'static Capture {
    INSTALL.call_once(|| {
        let cap = Capture::default();
        CAPTURE.set(cap.clone()).ok();
        let filter = lazy_wire_fire::logging::filter(Some(
            "trace,aws_sigv4=trace,aws_config=trace,aws_smithy_runtime=trace,aws_smithy_http_client=trace",
        ));
        let sub = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(cap)
            .finish();
        tracing::subscriber::set_global_default(sub).expect("the test owns the subscriber");
    });
    CAPTURE.get().unwrap()
}

/// Every form a secret may take in the output: as is, and base64 (basic auth).
fn forms(secret: &str) -> Vec<String> {
    use base64::Engine;
    vec![
        secret.to_string(),
        base64::engine::general_purpose::STANDARD.encode(secret),
    ]
}

/// Development aid: the captured output, under tmp/out (git-ignored).
fn dump(out: &str) {
    let p = std::env::current_dir()
        .unwrap()
        .join("tmp/out/lwf-worker-secrets-logs/captured.log");
    std::fs::create_dir_all(p.parent().unwrap()).ok();
    std::fs::write(p, out).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_at_any_level() {
    let root = need_db!();
    install();

    let dsn_pw = root
        .split("://")
        .nth(1)
        .and_then(|a| a.split('@').next())
        .and_then(|ua| ua.split_once(':'))
        .map(|(_, p)| p.to_string())
        .unwrap_or_default();

    let db = Db::fresh(&root, "logsecrets").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;

    // Credentials: a file reference (basic), an env reference (bearer, S3, Redis, SQL).
    let dir = std::env::current_dir()
        .unwrap()
        .join("tmp/out/secrets-logs");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("basic-pw"), format!("{BASIC_PW}\n")).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    std::env::set_var("LWF_LOGSECRETS_TOKEN", BEARER_TOKEN);
    std::env::set_var("LWF_LOGSECRETS_S3_AK", "lazywfaccess");
    std::env::set_var("LWF_LOGSECRETS_S3_SK", S3_SECRET);
    std::env::set_var("LWF_LOGSECRETS_REDIS_PW", REDIS_PW);
    std::env::set_var("LWF_LOGSECRETS_SQL_PW", SQL_PW);

    // An S3 bucket on the test server, created as the e2e suite does.
    let bucket = format!("lwfls{}", std::process::id());
    {
        use aws_credential_types::Credentials;
        let creds = Credentials::new("lazywfaccess", S3_SECRET, None, None, "logs");
        let client = Client::new(
            BaseUrl::parse("http://127.0.0.1:34070").unwrap(),
            &TlsSpec::parse(None).unwrap(),
        )
        .unwrap();
        let mut r = RawRequest::parse(&format!("PUT /{bucket} HTTP/1.1\n\n"))
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
        let made = client.send(&r, Duration::from_secs(5)).await.unwrap();
        assert!(made.status == 200, "create bucket: {}", made.status);
        // Temporary credentials: the session token is signed (the test server refuses it, which
        // is fine: the signing path, and what it logs, is what this checks).
        let temp = Credentials::new(
            "lazywfaccess",
            S3_SECRET,
            Some(S3_SESSION.to_string()),
            None,
            "logs",
        );
        let mut r = RawRequest::parse("HEAD /lwf-session-probe HTTP/1.1\n\n")
            .unwrap()
            .unwrap();
        lazy_wire_fire::aws::sign(
            &mut r,
            "127.0.0.1:34070",
            "",
            "us-east-1",
            "s3",
            &temp,
            lazy_wire_fire::time::unix_now(),
        )
        .unwrap();
        client.send(&r, Duration::from_secs(5)).await.ok();
    }

    // The SQL sink: a schema of this server, and an account that holds SQL_PW.
    let sink = Db::fresh(&root, "logsecrets_sink").await;
    sink.exec("CREATE TABLE copy (id INT PRIMARY KEY, name VARCHAR(100))")
        .await;
    sink.exec("DROP USER IF EXISTS 'lwf_logsecrets'@'%'").await;
    sink.exec(&format!(
        "CREATE USER 'lwf_logsecrets'@'%' IDENTIFIED BY '{SQL_PW}'"
    ))
    .await;
    sink.exec("GRANT ALL ON `lwf_logsecrets_sink`.* TO 'lwf_logsecrets'@'%'")
        .await;
    let my_host = root
        .trim_start_matches("mysql://")
        .split('@')
        .nth(1)
        .unwrap()
        .to_string();

    let mock = Mock::start().await;
    let url = format!("{}/base", mock.url());
    db.target(
        "basic",
        "http.basic",
        &url,
        None,
        &json!({"username": "u", "password": format!("file:{}", dir.join("basic-pw").display())})
            .to_string(),
    )
    .await;
    db.target(
        "bearer",
        "http.bearer",
        &url,
        None,
        r#"{"token": "env:LWF_LOGSECRETS_TOKEN"}"#,
    )
    .await;
    db.target(
        "header",
        "http.header",
        &url,
        None,
        &json!({"header": "X-Api-Key", "value": HEADER_VALUE}).to_string(),
    )
    .await;
    db.target(
        "store",
        "http.s3",
        "",
        Some("status IN (200, 204)"),
        &json!({"bucket": bucket, "region": "us-east-1", "endpoint": "http://127.0.0.1:34070",
                "path_style": true, "access_key_ref": "env:LWF_LOGSECRETS_S3_AK",
                "secret_key_ref": "env:LWF_LOGSECRETS_S3_SK"})
        .to_string(),
    )
    .await;
    db.target(
        "cache",
        "redis",
        "127.0.0.1:34379/5",
        Some("text IS NULL OR text = 'OK' OR text REGEXP '^[0-9]+$'"),
        &json!({"password": "env:LWF_LOGSECRETS_REDIS_PW", "prefix": "lwfls_"}).to_string(),
    )
    .await;
    db.target(
        "sqlsink",
        "mysql",
        &format!("{my_host}/lwf_logsecrets_sink"),
        Some("affected_rows != 0"),
        r#"{"username": "lwf_logsecrets", "password": "env:LWF_LOGSECRETS_SQL_PW"}"#,
    )
    .await;
    // A credential in an endpoint: refused as an invalid target, and the refusal is logged.
    db.target(
        "urlcred",
        "http.bearer",
        &format!("http://u:{URL_PW}@127.0.0.1:9/"),
        None,
        r#"{"token": "env:LWF_LOGSECRETS_TOKEN"}"#,
    )
    .await;

    let rules: &[(&str, &str, &str, &str)] = &[
        ("basic", "basic", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        ("bearer", "bearer", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        ("header", "header", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        (
            "export",
            "store",
            "SELECT c.id AS _key, c.name FROM customers c WHERE c.id IN (:keys)",
            "{{#each docs}}PUT /customers/{{key}}.json HTTP/1.1\nContent-Type: application/json\n\n{{json doc}}{{/each}}",
        ),
        (
            "cache",
            "cache",
            "SELECT id AS _key, name, NULL AS _op FROM customers WHERE id IN (:keys)",
            "{{#each docs}}SET {{redis_quote target.vars.prefix key}} {{redis_quote (json doc)}}{{/each}}",
        ),
        (
            "sql",
            "sqlsink",
            "SELECT id AS _key, name, NULL AS _op FROM customers WHERE id IN (:keys)",
            "{{#each docs}}INSERT INTO {{sql_ident \"copy\"}} (id, name) VALUES ({{sql_quote key}}, {{sql_quote doc.name}}) ON DUPLICATE KEY UPDATE name = VALUES(name){{/each}}",
        ),
    ];
    for (name, target, selects, template) in rules {
        if selects.is_empty() {
            db.exec_params(
                "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 50)",
                (*name, *target, *template),
            )
            .await;
        } else {
            db.exec_params(
                "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES (?, ?, 'key', ?, ?, 100)",
                (*name, *target, *selects, *template),
            )
            .await;
        }
        db.trigger(name, "customers", Some("{{new.id}}"), None)
            .await;
    }

    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    loop {
        let http_ok = mock.requests().len() >= 3;
        let sink_ok = sink.query::<(i64,)>("SELECT id FROM copy").await.len() >= 2;
        if (http_ok && sink_ok) || std::time::Instant::now() > deadline {
            if !(http_ok && sink_ok) {
                dump(&captured());
                panic!(
                    "flows did not complete: http {} sink {}",
                    mock.requests().len(),
                    sink_ok
                );
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    inst.quiet().await;
    inst.stop().await;
    sink.exec("DROP USER IF EXISTS 'lwf_logsecrets'@'%'").await;

    let out = captured();
    // The capture is real: the engine logged its start and the refused target.
    assert!(out.contains("starting"), "no engine log in the capture");
    assert!(
        out.contains("invalid target"),
        "the URL-credential target was not refused"
    );

    let mut secrets: Vec<(&str, String)> = vec![
        ("basic password", BASIC_PW.into()),
        ("bearer token", BEARER_TOKEN.into()),
        ("header value", HEADER_VALUE.into()),
        ("S3 session token", S3_SESSION.into()),
        ("S3 secret key", S3_SECRET.into()),
        ("SQL password", SQL_PW.into()),
        ("Redis password", REDIS_PW.into()),
        ("URL password", URL_PW.into()),
    ];
    for form in forms(&format!("u:{BASIC_PW}")) {
        secrets.push(("basic credentials (u:password)", form));
    }
    if dsn_pw.len() >= 4 {
        secrets.push(("DSN password", dsn_pw.clone()));
    } else {
        // Too short to search for: its userinfo form (`user:password@`) is searched instead.
        let user = root
            .trim_start_matches("mysql://")
            .split(':')
            .next()
            .unwrap_or("");
        secrets.push(("DSN userinfo", format!("{user}:{dsn_pw}@")));
    }
    let mut leaks = Vec::new();
    for (what, s) in &secrets {
        for form in forms(s) {
            if out.contains(&form) {
                let at = out.find(&form).unwrap();
                let line = out[..at].matches('\n').count() + 1;
                leaks.push(format!("{what} ({} bytes) at log line {line}", form.len()));
            }
        }
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}
