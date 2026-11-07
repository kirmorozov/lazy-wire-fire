//! No secret in the logs at any level. This binary owns the global subscriber: the production
//! filter (`lazy_wire_fire::logging::filter`) at `trace`, with `RUST_LOG` set to try to re-enable
//! the capped dependency targets, writing to a buffer. Representative flows run with fake
//! secrets (target credentials, an S3 session token, a SQL and a Redis password, a credential
//! inside an endpoint, a separate binlog account, and, for the queue targets: AWS access /
//! secret / session references, an AMQP account and a service-account key by reference), and
//! the captured output must contain none of them, nor the DSN password (when it is long enough
//! to tell apart from ordinary text). `make servers test-e2e`; the sqs, kinesis, amqp and
//! pubsub flows are `make test-e2e-queues` (their emulators; skipped when absent).

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use common::*;
use lazy_wire_fire::http::{BaseUrl, Client, RawRequest};
use lazy_wire_fire::tls::TlsSpec;
use serde_json::{json, Value};

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
/// Fake AWS credentials of the sqs, kinesis and lambda flows (the targets' `env:` references).
/// The access key id is an identifier, not a secret: it is not searched for.
const AWS_AK: &str = "lwfqueueak";
const AWS_SK: &str = "LWFSECRET-queue-sk-3f7d";
const AWS_SESSION: &str = "LWFSECRET-queue-session-8a2b";
/// The AMQP account the flow creates on the test broker (dropped at its end).
const AMQP_USER: &str = "lwf_ls_amqp";
const AMQP_PW: &str = "LWFSECRET-amqp-pw-5e9c";
/// The SMTP account of the smtp flow: Mailpit's `any` AUTH accepts it (its `Username` in the
/// API says the flow really authenticated).
const SMTP_USER: &str = "lwf_ls_smtp";
const SMTP_PW: &str = "LWFSECRET-smtp-pw-7c2e";
/// The password of the separate binlog account (`lwf_ls_binlog`), created for the flow.
const BINLOG_PW: &str = "LWFSECRET-binlog-pw-1d4f";
/// The fake Google service-account key behind the pubsub flow's `credentials_ref`. The private
/// key is a test-only RSA key; `SA_KEY_FRAG` is its prefix, what a leak would have to carry.
const SA_KEY: &str = r#"{"type":"service_account","project_id":"lwf-test","private_key_id":"lwflogsakey0000000","client_email":"logsecrets@lwf-test.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDPPV+OBRe/GnMo\nF1WQp05LO5uKzh2Gim1icaodqhFWjvQmAHYtA/3g51UrooQY37H7Y0dSynVZ5BoR\n+ZyB9BjumHByNmvwBKgcvkQbz/JX/hR4wcHTz9PPyOIyr1WIQxU9eTK/+Bzur4Vq\n7AEEdCIruYeJMqXoUxL26jdZMdvKMFVYEwvN2cI43JsSz+fWDC2zy13au6rwgfi7\nqxJZeOAczfNlZXa6WFUGGkzUgSq7X9UNKmZhG/Dl3DiNh4YjiG9+ES/XlC8thE9w\nPTM8ZzjgXAq26V1tuZnT86SYv5PVKy37yIMEW+IHQFpTLn1OJUxyjQxdwFHEJzOH\nsEtbjUyjAgMBAAECggEAHtx9X9lbTW6jant7FwULMEHqqlOCaxgvLONPWYaLeW/l\nZMlZz99Rzc21Tb+wW0Hor857fxU+P4or5bRdva57jgBucluvXDldTZZ57whbBwaD\nxu3XrvK6kK1SHgklPBr9TT/AsnzA1jyWKjOF/UNMGWXSDLjZowzH/LQpVOOLXwLp\n8zEQ2P1Qcy7cx4AcH7Z0/r82M25eEFRZoL01pcd94VXj/dlEWSygAdY5O4TaDKzm\nDV+W+Pcm/8qksD/mhK6AD8BOHikqngXM0HxTGwoOQsVGycZqIqkG0BuoCGyYlj36\nENlYlyPsE4FQaabfrXTXdqcPAf0NRL7DyHofh/VmBQKBgQDvh+R+va7QDSNUWesa\nmg8Z9kxNiLcpaY9o8lW9bNbFS/o0SMAZ9DwO+WdoyHQXiaPW2qIwYesABU+ScJOX\nae3mBoAZV6F+VQWMAeN1T1AV0Gas3/+I1IhfapXVDEc8+zoAqvtv+kJhxQ1Ietqd\nXRNh3vfUUMwLZ+EXMrjx8vuZNQKBgQDdfRu6t2yunQGwANxq7P4EFpIl55WJVrlu\nlscSWcoM1tcLBIhzaaJfxRvWsHAHcTAp1qDpS9+xGSo7Mu6WZ1EmWOFRgcoyOXJS\nNhGQuIhaxKK2K6jzZ5KIP4QT7jCSRLnp5M+bdA8OMhVg59ylm9UTcb6YCooV6tnN\ngHj4AtxhdwKBgQDEOzYMRuVB+KFq50deQbwBbu/PzSMQwDQGrBFl4BaLB9Vwo/EM\n0jOpqNT42M+SwOYCjJa2wGE/YRpxQ90F/HgzLDwhhtL8pjanHpzzbAiV9mdHVTMJ\nK+SVFsKtCSQLZ117Upgugn2Dj2V0bYFPzgKk+X9a5t5isbWLxbgSLxfDyQKBgQCy\nJTZwb6fSqiX5Ujf3KLcGwsC6DBKSbFapREzJzb5dpU9BZVv6uDQJo5hIjZEN61Q5\n0o+KFaTN5Ne61bRg9Kiol0RjMmlXDC2PQj3OkR3bI91beLJjVCLfw3aQMTyP6Qc/\nwPHarvaNDGt1jghCi5xOD2alE5UNG+AjEfo6QNLQ8QKBgQCPN4iYKAiN2qcw0dGz\nnhr5P/9l+LZwteHa/NFp8qieT3nPXdoONK+dmUMS6RrokR6xmviJmR0T1eDB1gRw\n40OxmoCmMkdvVLTFMoGrNcu/TT597U/5669/FTxVI1Jdy2tD2L+mL6ZJRFuikbYC\nkseTs63uBwj90peHtRtMWZAs/Q==\n-----END PRIVATE KEY-----\n"}"#;
const SA_KEY_FRAG: &str = "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDPPV";

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
            "trace,aws_sigv4=trace,aws_config=trace,aws_credential_types=trace,\
             aws_smithy_runtime=trace,aws_smithy_runtime_api=trace,aws_smithy_http_client=trace,\
             lapin=trace,amq_protocol_tcp=trace,lettre=trace",
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

/// The `secrets` found in `out` (any form, as is or base64): what, the form's length, the log line.
fn leaks<S: AsRef<str>>(out: &str, secrets: &[(&str, S)]) -> Vec<String> {
    let mut leaks = Vec::new();
    for (what, s) in secrets {
        for form in forms(s.as_ref()) {
            if out.contains(&form) {
                let at = out.find(&form).unwrap();
                let line = out[..at].matches('\n').count() + 1;
                leaks.push(format!("{what} ({} bytes) at log line {line}", form.len()));
            }
        }
    }
    leaks
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

    // A separate binlog account (LAZYWF_BINLOG_USER / LAZYWF_BINLOG_PASSWORD): the engine
    // streams the binlog with it, and its password is one of the secrets checked below.
    let mariadb = db
        .query::<String>("SELECT VERSION()")
        .await
        .pop()
        .is_some_and(|v| v.contains("MariaDB"));
    db.exec("DROP USER IF EXISTS 'lwf_ls_binlog'@'%'").await;
    db.exec(&format!(
        "CREATE USER 'lwf_ls_binlog'@'%' IDENTIFIED BY '{BINLOG_PW}'"
    ))
    .await;
    db.exec(&format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT{} ON *.* TO 'lwf_ls_binlog'@'%'",
        if mariadb { ", BINLOG MONITOR" } else { "" }
    ))
    .await;
    // The stream connection opens with the schema as its default database.
    db.exec(&format!(
        "GRANT SELECT ON `{}`.* TO 'lwf_ls_binlog'@'%'",
        db.schema
    ))
    .await;

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

    let inst = Instance::ready(config(
        &db,
        &[
            ("LAZYWF_BINLOG_USER", "lwf_ls_binlog"),
            ("LAZYWF_BINLOG_PASSWORD", BINLOG_PW),
        ],
    ))
    .await;
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
    db.exec("DROP USER IF EXISTS 'lwf_ls_binlog'@'%'").await;

    let out = captured();
    // The capture is real: the engine logged its start and the refused target.
    assert!(out.contains("starting"), "no engine log in the capture");
    assert!(
        out.contains("invalid target"),
        "the URL-credential target was not refused"
    );

    let mut secrets: Vec<(&str, String)> = vec![
        ("basic password", BASIC_PW.into()),
        ("basic credentials (u:password)", format!("u:{BASIC_PW}")),
        ("bearer token", BEARER_TOKEN.into()),
        ("header value", HEADER_VALUE.into()),
        ("S3 session token", S3_SESSION.into()),
        ("S3 secret key", S3_SECRET.into()),
        ("SQL password", SQL_PW.into()),
        ("Redis password", REDIS_PW.into()),
        ("URL password", URL_PW.into()),
        ("binlog password", BINLOG_PW.into()),
    ];
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
    let leaks = leaks(&out, &secrets);
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// `lambda` target: the in-process Invoke stand-in of the e2e suite. Every invocation is
/// signed SigV4 with the fake reference credentials; the stand-in receives the requests, the
/// logs must not carry the credentials. Always runs: no emulator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_lambda() {
    let root = need_db!();
    install();
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    let db = Db::fresh(&root, "loglm").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGLM_AK", AWS_AK);
    std::env::set_var("LWF_LOGLM_SK", AWS_SK);
    std::env::set_var("LWF_LOGLM_ST", AWS_SESSION);
    db.target(
        "fn",
        "lambda",
        &format!("aws-lambda://us-east-1/orders?endpoint={}", mock.url()),
        None,
        &json!({"access_key_ref": "env:LWF_LOGLM_AK",
                "secret_key_ref": "env:LWF_LOGLM_SK",
                "session_token_ref": "env:LWF_LOGLM_ST"})
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('fn', 'fn', 'key', ?, ?, 100)",
        (
            "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
            r#"{"orders": [{{#each docs}}{"key": {{json key}}, "doc": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#,
        ),
    )
    .await;
    db.trigger("fn", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    wait_for(
        Duration::from_secs(30),
        "two signed invocations",
        || async {
            let got = mock
                .requests()
                .iter()
                .filter(|r| {
                    r.path == "/2015-03-31/functions/orders/invocations"
                        && r.header("authorization")
                            .is_some_and(|a| a.contains(&format!("Credential={AWS_AK}/")))
                        && r.header("x-amz-security-token").is_some()
                })
                .count();
            (got >= 2).then_some(())
        },
    )
    .await;
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("queue secret key", AWS_SK),
            ("queue session token", AWS_SESSION),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

// ---- The queue kinds against their emulators (`make test-e2e-queues`), as the e2e suite does.

/// The SQS JSON API of the store (ElasticMQ): queue creation and the message count.
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

    async fn call(&self, action: &str, body: serde_json::Value) -> serde_json::Value {
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

    /// Creates a standard queue; its queue URL (ElasticMQ answers as the account 000000000000).
    async fn create(&self, name: &str) -> String {
        self.call("CreateQueue", json!({"QueueName": name})).await;
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
}

/// The `sqs` target against ElasticMQ (`LAZYWF_TEST_SQS`): every request is SigV4-signed with
/// the fake reference credentials, and the queue reads them back through the same JSON API.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_sqs() {
    let root = need_db!();
    let Ok(sqs_root) = std::env::var("LAZYWF_TEST_SQS") else {
        return eprintln!("LAZYWF_TEST_SQS not set: skipping the sqs flow (make test-e2e-queues)");
    };
    install();
    let sqs = Sqs::new(&sqs_root);
    let db = Db::fresh(&root, "logsq").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGSQ_AK", AWS_AK);
    std::env::set_var("LWF_LOGSQ_SK", AWS_SK);
    std::env::set_var("LWF_LOGSQ_ST", AWS_SESSION);
    let queue = format!("lwf_logsqs_{}", std::process::id());
    let url = sqs.create(&queue).await;
    db.target(
        "queue",
        "sqs",
        &format!("aws-sqs://us-east-1/000000000000/{queue}?endpoint={sqs_root}"),
        None,
        &json!({"access_key_ref": "env:LWF_LOGSQ_AK",
                "secret_key_ref": "env:LWF_LOGSQ_SK",
                "session_token_ref": "env:LWF_LOGSQ_ST"})
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('queue', 'queue', 'batch', 10, ?, ?, 100)",
        (
            "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
            r#"POST / HTTP/1.1
X-Amz-Target: AmazonSQS.SendMessageBatch
Content-Type: application/x-amz-json-1.0

{"QueueUrl": {{json endpoint.url}}, "Entries": [{{#each docs}}{"Id": "{{@index}}", "MessageBody": {{json (json doc)}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#,
        ),
    )
    .await;
    db.trigger("queue", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    wait_for(Duration::from_secs(30), "two messages", || async {
        (sqs.count(&url).await >= 2).then_some(())
    })
    .await;
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("queue secret key", AWS_SK),
            ("queue session token", AWS_SESSION),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// The Kinesis JSON API of the store (kinesis-mock): stream creation, the record count. A
/// SigV4-shaped `Authorization` is needed on the API calls (kinesis-mock does not check the
/// signature).
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

    async fn call(&self, action: &str, body: serde_json::Value) -> serde_json::Value {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header("Content-Type", "application/x-amz-json-1.1".into());
        req.set_header("X-Amz-Target", format!("Kinesis_20131202.{action}"));
        req.set_header("X-Amz-Date", "20260101T000000Z".into());
        req.set_header(
            "Authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={AWS_AK}/20260101/us-east-1/kinesis/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=00"
            ),
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
            return serde_json::Value::Null;
        }
        serde_json::from_slice(&resp.body).unwrap()
    }

    /// Creates the stream and waits until it is ACTIVE: kinesis-mock refuses puts and reads
    /// while a new stream is still CREATING.
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

    /// The records of the stream, shard by shard.
    async fn count(&self, stream: &str) -> usize {
        let shards = self.call("ListShards", json!({"StreamName": stream})).await;
        let mut total = 0;
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
                total += recs.len();
                iterator = got["NextShardIterator"].as_str().unwrap().to_string();
            }
        }
        total
    }
}

/// The `kinesis` target against kinesis-mock (`LAZYWF_TEST_KINESIS`): every put is signed with
/// the fake reference credentials; the stream reads them back through the same JSON API.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_kinesis() {
    let root = need_db!();
    let Ok(kin_root) = std::env::var("LAZYWF_TEST_KINESIS") else {
        return eprintln!(
            "LAZYWF_TEST_KINESIS not set: skipping the kinesis flow (make test-e2e-queues)"
        );
    };
    install();
    let kin = Kinesis::new(&kin_root);
    let stream = format!("lwf_logkin_{}", std::process::id());
    kin.create_stream(&stream).await;
    let db = Db::fresh(&root, "logkn").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGKN_AK", AWS_AK);
    std::env::set_var("LWF_LOGKN_SK", AWS_SK);
    std::env::set_var("LWF_LOGKN_ST", AWS_SESSION);
    db.target(
        "stream",
        "kinesis",
        &format!("aws-kinesis://us-east-1/{stream}?endpoint={kin_root}"),
        None,
        &json!({"access_key_ref": "env:LWF_LOGKN_AK",
                "secret_key_ref": "env:LWF_LOGKN_SK",
                "session_token_ref": "env:LWF_LOGKN_ST"})
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('stream', 'stream', 'batch', 100, ?, ?, 100)",
        (
            "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
            r#"POST / HTTP/1.1
X-Amz-Target: Kinesis_20131202.PutRecords
Content-Type: application/x-amz-json-1.1

{"StreamName": {{json endpoint.stream}}, "Records": [{{#each docs}}{"Data": "{{#if (eq op "delete")}}{{base64 '{"op":"delete"}'}}{{else}}{{base64 (json doc)}}{{/if}}", "PartitionKey": {{json key}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#,
        ),
    )
    .await;
    db.trigger("stream", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    wait_for(Duration::from_secs(30), "two records", || async {
        (kin.count(&stream).await >= 2).then_some(())
    })
    .await;
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("queue secret key", AWS_SK),
            ("queue session token", AWS_SESSION),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// RabbitMQ administration of the test broker (the compose `rabbitmq` container): the flow's
/// account, created for the test and dropped at its end.
fn rabbitmqctl(args: &[&str]) {
    let out = std::process::Command::new("docker")
        .args(["exec", "lazywf-test-rabbitmq-1", "rabbitmqctl"])
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("the RabbitMQ container (docker rabbitmqctl {args:?}): {e}"));
    assert!(
        out.status.success(),
        "rabbitmqctl {}: {}",
        args[0],
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The test's own consumer on the broker (the suite's `lwftest` account, as the e2e amqp tests
/// use it): a bound queue, and the message ids read from it so far.
struct Amqp {
    _conn: lapin::Connection,
    chan: lapin::Channel,
    queue: String,
    got: std::sync::Mutex<Vec<String>>,
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

    /// Waits until at least `n` messages have been read; their ids so far.
    async fn ids(&self, n: usize) -> Vec<String> {
        use lapin::options::BasicGetOptions;
        wait_for(
            Duration::from_secs(30),
            &format!("{n} AMQP messages"),
            || async {
                loop {
                    let Some(m) = self
                        .chan
                        .basic_get(self.queue.as_str().into(), BasicGetOptions { no_ack: true })
                        .await
                        .unwrap()
                    else {
                        break;
                    };
                    self.got.lock().unwrap().push(
                        m.delivery
                            .properties
                            .message_id()
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    );
                }
                let all = self.got.lock().unwrap().clone();
                (all.len() >= n).then_some(all)
            },
        )
        .await
    }
}

/// The `amqp` target against RabbitMQ (`LAZYWF_TEST_AMQP`): one message per key under the
/// flow's own account, whose password is a fake secret; the consumer reads the queue back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_amqp() {
    let root = need_db!();
    let Some(broker) = std::env::var("LAZYWF_TEST_AMQP").ok() else {
        return eprintln!(
            "LAZYWF_TEST_AMQP not set: skipping the amqp flow (make test-e2e-queues)"
        );
    };
    install();
    let db = Db::fresh(&root, "logamqp").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    // The flow's account: a rerun after a failed one drops the old user first (its absence is
    // not an error).
    let _ = std::process::Command::new("docker")
        .args([
            "exec",
            "lazywf-test-rabbitmq-1",
            "rabbitmqctl",
            "delete_user",
            AMQP_USER,
        ])
        .output();
    rabbitmqctl(&["add_user", AMQP_USER, AMQP_PW]);
    rabbitmqctl(&["set_permissions", "-p", "/", AMQP_USER, ".*", ".*", ".*"]);
    let (ex, q) = ("lwf.logsecrets.amqp", "lwf.logsecrets.amqp.q");
    let consumer = Amqp::setup(&broker, ex, q).await;
    std::env::set_var("LWF_LOGAMQP_USER", AMQP_USER);
    std::env::set_var("LWF_LOGAMQP_PW", AMQP_PW);
    db.target(
        "bus",
        "amqp",
        &format!("amqp://{broker}/"),
        None,
        &json!({"username": "env:LWF_LOGAMQP_USER", "password": "env:LWF_LOGAMQP_PW"}).to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            format!(
                "{{{{#each docs}}}}PUBLISH /{ex}/order.changed\nMessage-Id: {{{{key}}}}\n\n\
                 {{\"key\":{{{{json key}}}},\"op\":{{{{json op}}}},\"doc\":{{{{json doc}}}}}}{{{{/each}}}}"
            ),
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    let mut ids = consumer.ids(2).await;
    ids.sort();
    assert_eq!(ids, vec!["1", "2"], "one message per key: {ids:?}");
    inst.quiet().await;
    inst.stop().await;
    rabbitmqctl(&["delete_user", AMQP_USER]);
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[("AMQP username", AMQP_USER), ("AMQP password", AMQP_PW)],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// The Pub/Sub emulator's REST API (project `lwf-test`): topic and subscription, the pull.
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

    async fn call(&self, method: &str, path: &str, body: serde_json::Value) -> serde_json::Value {
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
        serde_json::from_slice(&resp.body).unwrap_or(serde_json::Value::Null)
    }

    /// Creates a topic; the target's endpoint is `gc-pubsub://lwf-test/<topic>`.
    async fn topic(&self, name: &str) {
        self.call(
            "PUT",
            &format!("/v1/projects/lwf-test/topics/{name}"),
            json!({}),
        )
        .await;
    }

    async fn subscription(&self, topic: &str, name: &str) {
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
            json!({"topic": format!("projects/lwf-test/topics/{topic}")}),
        )
        .await;
    }

    /// The messages the subscription holds now; they are acknowledged, so each comes once.
    async fn pull(&self, sub: &str) -> Vec<serde_json::Value> {
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
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let ids: Vec<serde_json::Value> =
            got.iter().filter_map(|m| m.get("ackId").cloned()).collect();
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

/// The `pubsub` target against Google's emulator (`LAZYWF_TEST_PUBSUB`): the service-account
/// key is by reference (`vars.credentials_ref`); the key's private part is a fake secret. The
/// token is minted from the key locally (google-cloud-auth signs a JWT, no network call), so
/// the flow runs offline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_pubsub() {
    let root = need_db!();
    let Ok(ps_root) = std::env::var("LAZYWF_TEST_PUBSUB") else {
        return eprintln!(
            "LAZYWF_TEST_PUBSUB not set: skipping the pubsub flow (make test-e2e-queues)"
        );
    };
    install();
    let ps = Pubsub::new(&ps_root);
    let db = Db::fresh(&root, "logps").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    let topic = format!("lwf_logps_{}", std::process::id());
    ps.topic(&topic).await;
    ps.subscription(&topic, "logps-s").await;
    std::env::set_var("LWF_LOGPS_SA_KEY", SA_KEY);
    db.target(
        "queue",
        "pubsub",
        &format!("gc-pubsub://lwf-test/{topic}?endpoint={ps_root}"),
        Some("JSON_LENGTH(JSON_EXTRACT(body, '$.messageIds')) > 0"),
        &json!({"credentials_ref": "env:LWF_LOGPS_SA_KEY"}).to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('queue', 'queue', 'batch', 1000, ?, ?, 100)",
        (
            "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
            r#"POST /v1/projects/{{endpoint.project}}/topics/{{endpoint.topic}}:publish HTTP/1.1
Content-Type: application/json

{"messages": [{{#each docs}}{"data": {{json (base64 (json doc))}}, "attributes": {"key": {{json key}}, "op": {{json op}}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#,
        ),
    )
    .await;
    db.trigger("queue", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while got.len() < 2 {
        got.extend(ps.pull("logps-s").await);
        if std::time::Instant::now() > deadline {
            dump(&captured());
            panic!("timed out waiting for two messages: {got:?}");
        }
        sleep_ms(200).await;
    }
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(&out, &[("service-account key", SA_KEY_FRAG)]);
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// Mailpit's HTTP API (its port is the SMTP port plus one): the mailbox, emptied for the flow.
async fn mailpit(host: &str, port: u16, method: &str, path: &str) -> Value {
    let client = lazy_wire_fire::http::Client::new(
        lazy_wire_fire::http::BaseUrl::parse(&format!("http://{host}:{port}")).unwrap(),
        &lazy_wire_fire::tls::TlsSpec::parse(None).unwrap(),
    )
    .unwrap();
    let mut req = lazy_wire_fire::http::RawRequest {
        method: method.into(),
        target: path.into(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    req.set_header("Host", format!("{host}:{port}"));
    let resp = client
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

/// The `smtp` target against Mailpit (`LAZYWF_TEST_SMTP`): the target's credentials are by
/// reference (the password a fake secret), the AUTH goes over the wire, and the mails read back
/// through Mailpit's API carry the authenticated username (the flow really ran).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_smtp() {
    let root = need_db!();
    let Ok(s) = std::env::var("LAZYWF_TEST_SMTP") else {
        return eprintln!(
            "LAZYWF_TEST_SMTP not set: skipping the smtp flow (make test-e2e-queues)"
        );
    };
    let (host, port) = match s.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>().ok() {
            Some(p) => (h, p),
            None => panic!("LAZYWF_TEST_SMTP is `{s}`, not host:port"),
        },
        None => panic!("LAZYWF_TEST_SMTP is `{s}`, not host:port"),
    };
    install();
    let api = port + 1;
    let db = Db::fresh(&root, "logsmtp").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGSMTP_USER", SMTP_USER);
    std::env::set_var("LWF_LOGSMTP_PW", SMTP_PW);
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, vars) VALUES ('mail', 'smtp', ?, 'off', ?)",
        (
            format!("smtp://{s}"),
            json!({"username": "env:LWF_LOGSMTP_USER", "password": "env:LWF_LOGSMTP_PW"})
                .to_string(),
        ),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'mail', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)",
            "{{#each docs}}From: lazywf@lwf-smtp.test\nTo: lwf-smtp-{{key}}@lwf-smtp.test\n\
             Subject: Order {{key}}\nContent-Type: text/plain; charset=utf-8\n\n\
             {\"key\":{{json key}},\"op\":{{json op}},\"doc\":{{json doc}}}{{/each}}",
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    wait_for(
        Duration::from_secs(30),
        "two authenticated mails",
        || async {
            let v = mailpit(host, api, "GET", "/api/v1/messages?limit=100").await;
            let m = v
                .get("messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let n = m
                .iter()
                .filter(|m| m.get("Username").and_then(Value::as_str) == Some(SMTP_USER))
                .count();
            (n >= 2).then_some(())
        },
    )
    .await;
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    // PLAIN puts both credentials in one base64 blob: that exact wire form is what an
    // un-capped debug log of the AUTH command would carry.
    use base64::Engine as _;
    let plain =
        base64::engine::general_purpose::STANDARD.encode(format!("\u{0}{SMTP_USER}\u{0}{SMTP_PW}"));
    let leaks = leaks(
        &out,
        &[
            ("SMTP username", SMTP_USER),
            ("SMTP password", SMTP_PW),
            ("SMTP PLAIN auth response", &plain),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}
