//! No secret in the logs at any level. This binary owns the global subscriber: the production
//! filter (`lazy_wire_fire::logging::filter`) at `trace`, with `RUST_LOG` set to try to re-enable
//! the capped dependency targets, writing to a buffer. Representative flows run with fake
//! secrets (target credentials, an S3 session token, a SQL and a Redis password, a credential
//! inside an endpoint, a separate binlog account, the webhook signing secrets, and, for the
//! queue targets: AWS access / secret / session references, an AMQP account, a service-account
//! key by reference and a SCRAM account of the Kafka broker, and for a `grpc` target the bearer
//! token of `vars.token` by reference, and for a `vars.oauth` flow the OAuth 2.0 client secret
//! behind `vars.client_secret_ref` and the access token that target's endpoint hands out (both
//! echoed back by the servers the flow talks to), and the captured output must contain none of
//! them, nor the DSN password (when it is long enough to tell apart from ordinary text).
//! `make servers test-e2e`; the sqs, kinesis, amqp, pubsub, smtp and kafka flows are
//! `make test-e2e-queues` (their emulators; skipped when absent).

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

/// The servicebus flow's own account and SAS rule key (both by reference only; the key is a
/// Base64 string as Service Bus issues one).
const SB_USER: &str = "lwf_ls_sb";
const SB_PW: &str = "LWFSECRET-sb-pw-7b13";
const SB_KEY: &str = "c2VjcmV0LXNlcnZpY2VidXMta2V5LTExLTAx";

/// The `rabbitmq-stream` flow's own account on the broker's stream endpoint (by reference
/// only), and the wrong password of the send that must fail: the broker's refusal is where a
/// credential could surface.
const RMQS_USER: &str = "lwf_ls_rmq_stream";
const RMQS_PW: &str = "LWFSECRET-rmq-stream-pw-4d7f";
const RMQS_BAD_PW: &str = "LWFSECRET-rmq-stream-bad-pw-9a12";

/// The rendered action of the `rabbitmq-stream` flow: one record per key with its document.
const RMQS_PUT: &str = r#"POST / HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{"value": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;

/// The rendered action of the servicebus flow: one message per key with its document (the
/// template renders the document whole, so it serves either table).
const SB_PUT: &str = r#"POST / HTTP/1.1
Content-Type: application/json

{"messages": [{{#each docs}}{"value": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#;
/// The SMTP account of the smtp flow: Mailpit's `any` AUTH accepts it (its `Username` in the
/// API says the flow really authenticated).
const SMTP_USER: &str = "lwf_ls_smtp";
const SMTP_PW: &str = "LWFSECRET-smtp-pw-7c2e";
/// The password of the separate binlog account (`lwf_ls_binlog`), created for the flow.
const BINLOG_PW: &str = "LWFSECRET-binlog-pw-1d4f";
/// The signing secrets of the signed-webhook flow: `SIGN_STD_KEY` is the raw key of the
/// standard scheme (its secret is `whsec_` + its base64), `SIGN_HMAC_KEY` the secret of the
/// hmac scheme. Both are fake, so finding either in the output is a leak, not a coincidence.
const SIGN_STD_KEY: &str = "LWFSECRET-whsec-key-5f3b";
const SIGN_HMAC_KEY: &str = "LWFSECRET-hmac-key-1c8d";
/// The fake Google service-account key behind the pubsub flow's `credentials_ref`. The private
/// key is a test-only RSA key; `SA_KEY_FRAG` is its prefix, what a leak would have to carry.
const SA_KEY: &str = r#"{"type":"service_account","project_id":"lwf-test","private_key_id":"lwflogsakey0000000","client_email":"logsecrets@lwf-test.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDPPV+OBRe/GnMo\nF1WQp05LO5uKzh2Gim1icaodqhFWjvQmAHYtA/3g51UrooQY37H7Y0dSynVZ5BoR\n+ZyB9BjumHByNmvwBKgcvkQbz/JX/hR4wcHTz9PPyOIyr1WIQxU9eTK/+Bzur4Vq\n7AEEdCIruYeJMqXoUxL26jdZMdvKMFVYEwvN2cI43JsSz+fWDC2zy13au6rwgfi7\nqxJZeOAczfNlZXa6WFUGGkzUgSq7X9UNKmZhG/Dl3DiNh4YjiG9+ES/XlC8thE9w\nPTM8ZzjgXAq26V1tuZnT86SYv5PVKy37yIMEW+IHQFpTLn1OJUxyjQxdwFHEJzOH\nsEtbjUyjAgMBAAECggEAHtx9X9lbTW6jant7FwULMEHqqlOCaxgvLONPWYaLeW/l\nZMlZz99Rzc21Tb+wW0Hor857fxU+P4or5bRdva57jgBucluvXDldTZZ57whbBwaD\nxu3XrvK6kK1SHgklPBr9TT/AsnzA1jyWKjOF/UNMGWXSDLjZowzH/LQpVOOLXwLp\n8zEQ2P1Qcy7cx4AcH7Z0/r82M25eEFRZoL01pcd94VXj/dlEWSygAdY5O4TaDKzm\nDV+W+Pcm/8qksD/mhK6AD8BOHikqngXM0HxTGwoOQsVGycZqIqkG0BuoCGyYlj36\nENlYlyPsE4FQaabfrXTXdqcPAf0NRL7DyHofh/VmBQKBgQDvh+R+va7QDSNUWesa\nmg8Z9kxNiLcpaY9o8lW9bNbFS/o0SMAZ9DwO+WdoyHQXiaPW2qIwYesABU+ScJOX\nae3mBoAZV6F+VQWMAeN1T1AV0Gas3/+I1IhfapXVDEc8+zoAqvtv+kJhxQ1Ietqd\nXRNh3vfUUMwLZ+EXMrjx8vuZNQKBgQDdfRu6t2yunQGwANxq7P4EFpIl55WJVrlu\nlscSWcoM1tcLBIhzaaJfxRvWsHAHcTAp1qDpS9+xGSo7Mu6WZ1EmWOFRgcoyOXJS\nNhGQuIhaxKK2K6jzZ5KIP4QT7jCSRLnp5M+bdA8OMhVg59ylm9UTcb6YCooV6tnN\ngHj4AtxhdwKBgQDEOzYMRuVB+KFq50deQbwBbu/PzSMQwDQGrBFl4BaLB9Vwo/EM\n0jOpqNT42M+SwOYCjJa2wGE/YRpxQ90F/HgzLDwhhtL8pjanHpzzbAiV9mdHVTMJ\nK+SVFsKtCSQLZ117Upgugn2Dj2V0bYFPzgKk+X9a5t5isbWLxbgSLxfDyQKBgQCy\nJTZwb6fSqiX5Ujf3KLcGwsC6DBKSbFapREzJzb5dpU9BZVv6uDQJo5hIjZEN61Q5\n0o+KFaTN5Ne61bRg9Kiol0RjMmlXDC2PQj3OkR3bI91beLJjVCLfw3aQMTyP6Qc/\nwPHarvaNDGt1jghCi5xOD2alE5UNG+AjEfo6QNLQ8QKBgQCPN4iYKAiN2qcw0dGz\nnhr5P/9l+LZwteHa/NFp8qieT3nPXdoONK+dmUMS6RrokR6xmviJmR0T1eDB1gRw\n40OxmoCmMkdvVLTFMoGrNcu/TT597U/5669/FTxVI1Jdy2tD2L+mL6ZJRFuikbYC\nkseTs63uBwj90peHtRtMWZAs/Q==\n-----END PRIVATE KEY-----\n"}"#;
const SA_KEY_FRAG: &str = "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDPPV";
/// The SCRAM account the kafka flow creates on the test broker (dropped at its end).
const KAFKA_USER: &str = "lwf_ls_kafka";
const KAFKA_PW: &str = "LWFSECRET-kafka-pw-9d3e";
/// The bearer token of the `grpc` flow (`vars.token`, an `env:` reference): its value is the
/// `authorization` metadata of every call, so a leak would be a credential, not a path.
const GRPC_TOKEN: &str = "LWFSECRET-grpc-token-4b1a";
/// The path and the query of the webhook URL an endpoint reference holds. For these targets the
/// URL below the host *is* the credential (the incoming webhook of Slack, Microsoft Teams
/// Workflows, Google Chat or Discord), so neither may appear in a log, a `/status` or
/// `last_error`.
const WEBHOOK_PATH: &str = "LWFSECRET-webhook-path-9f2c";
const WEBHOOK_QUERY: &str = "LWFSECRET-webhook-query-4b71";

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
             lapin=trace,amq_protocol_tcp=trace,lettre=trace,rskafka=trace,rsasl=trace",
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
    // The signing secrets: by reference only (the standard one is `whsec_` + base64 of its key).
    use base64::Engine as _;
    let sign_std_secret = format!(
        "whsec_{}",
        base64::engine::general_purpose::STANDARD.encode(SIGN_STD_KEY.as_bytes())
    );
    std::env::set_var("LWF_LOGSECRETS_SIGN_STD", &sign_std_secret);
    std::env::set_var("LWF_LOGSECRETS_SIGN_HMAC", SIGN_HMAC_KEY);
    db.target(
        "signstd",
        "http",
        &url,
        None,
        r#"{"sign": "standard", "sign_secret_ref": "env:LWF_LOGSECRETS_SIGN_STD"}"#,
    )
    .await;
    db.target(
        "signhmac",
        "http",
        &url,
        None,
        r#"{"sign": "hmac", "sign_secret_ref": "env:LWF_LOGSECRETS_SIGN_HMAC"}"#,
    )
    .await;

    let rules: &[(&str, &str, &str, &str)] = &[
        ("basic", "basic", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        ("bearer", "bearer", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        ("header", "header", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        ("signstd", "signstd", "", "POST /{{rule}} HTTP/1.1\n\nok"),
        ("signhmac", "signhmac", "", "POST /{{rule}} HTTP/1.1\n\nok"),
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
        let http_ok = mock.requests().len() >= 5;
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
        ("standard signing secret", sign_std_secret),
        ("standard signing key", SIGN_STD_KEY.into()),
        ("hmac signing secret", SIGN_HMAC_KEY.into()),
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

/// The test's own consumer of a queue the servicebus target writes over AMQP 1.0: the queue is
/// declared over 0-9-1 (the same listener answers both) and read back the same way.
struct Sbus {
    _conn: lapin::Connection,
    chan: lapin::Channel,
    queue: String,
    got: std::sync::Mutex<Vec<String>>,
}

impl Sbus {
    async fn setup(broker: &str, queue: &str) -> Sbus {
        use lapin::{options::*, types::FieldTable, ConnectionProperties};
        let url = format!("amqp://lwftest:lwftest@{broker}/%2f");
        let conn = lapin::Connection::connect(&url, ConnectionProperties::default())
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
            FieldTable::default(),
        )
        .await
        .unwrap();
        Sbus {
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
            Duration::from_secs(60),
            &format!("{n} AMQP 1.0 messages"),
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

/// A reader over a stream, from its first offset: a stream is an append-only log, so reading
/// it costs nothing and removes nothing. Collects the `lazywf_key` of every record.
struct StreamReader {
    _conn: lapin::Connection,
    drain: tokio::task::JoinHandle<()>,
    got: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StreamReader {
    /// Attaches the test's consumer of the stream over the broker's **AMQP 0-9-1** endpoint
    /// (`LAZYWF_TEST_AMQP`, the port the `amqp` flow uses): reading a stream is an AMQP consumer
    /// with `x-stream-offset`, and the stream port itself speaks another protocol and would abort
    /// this handshake. Attaching is retried: the RabbitMQ of the shared compose file belongs to the
    /// host too and another agent can restart it while the suite runs.
    async fn setup(stream: &str) -> StreamReader {
        use futures_util::StreamExt;
        let broker = std::env::var("LAZYWF_TEST_AMQP")
            .ok()
            .filter(|v| !v.is_empty())
            .expect("LAZYWF_TEST_AMQP: a stream is read back by an AMQP 0-9-1 consumer, this port");
        let url = format!("amqp://lwftest:lwftest@{broker}/%2f");
        let (conn, mut consumer) = {
            let mut last = "never attempted".to_string();
            let mut out = None;
            for _ in 0..30 {
                match StreamReader::attach(&url, stream).await {
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
            use lapin::options::BasicAckOptions;
            use lapin::types::AMQPValue;
            while let Some(delivery) = consumer.next().await {
                let Ok(d) = delivery else { break };
                // The key as the kind carries it: the `lazywf_key` application property (the
                // counterpart of `Message-Id` in the amqp flow above).
                let key = d
                    .properties
                    .headers()
                    .as_ref()
                    .and_then(|h| {
                        h.into_iter().find(|(k, _)| k.as_str() == "lazywf_key").map(
                            |(_, v)| match v {
                                AMQPValue::LongString(s) => s.to_string(),
                                _ => String::new(),
                            },
                        )
                    })
                    .filter(|k| !k.is_empty());
                if let Some(key) = key {
                    sink.lock().unwrap().push(key);
                }
                let _ = d.ack(BasicAckOptions::default()).await;
            }
        });
        StreamReader {
            _conn: conn,
            drain,
            got,
        }
    }

    /// One attempt: the connection, the channel with its prefetch, and the consumer of the whole
    /// stream from its first record.
    async fn attach(
        url: &str,
        stream: &str,
    ) -> anyhow::Result<(lapin::Connection, lapin::Consumer)> {
        use lapin::options::*;
        use lapin::types::AMQPValue;
        use lapin::ConnectionProperties;
        let conn = lapin::Connection::connect(url, ConnectionProperties::default()).await?;
        let chan = conn.create_channel().await?;
        // A stream demands a prefetch and acknowledgements (the acks are the credit that
        // advances this consumer's own offset, nothing is removed from the log).
        chan.basic_qos(200, BasicQosOptions::default()).await?;
        let mut copts = lapin::types::FieldTable::default();
        copts.insert(
            "x-stream-offset".into(),
            AMQPValue::LongString("first".into()),
        );
        let consumer = chan
            .basic_consume(
                stream.into(),
                "lwf-logsecrets".into(),
                BasicConsumeOptions::default(),
                copts,
            )
            .await?;
        Ok((conn, consumer))
    }

    /// Waits until at least `n` records have been read, and returns the ids collected so far.
    async fn at_least(&self, n: usize) -> Vec<String> {
        wait_for(
            Duration::from_secs(60),
            &format!("{n} stream records"),
            || async {
                let all = self.got.lock().unwrap().clone();
                (all.len() >= n).then_some(all)
            },
        )
        .await
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        self.drain.abort();
    }
}

/// The `rabbitmq-stream` kind against the broker's stream endpoint
/// (`LAZYWF_TEST_RABBITMQ_STREAM`): the flow's own account, its password a fake secret taken by
/// `env:` reference only. Both the send that goes through and the send that cannot (a wrong
/// password, so the broker refuses the login: the path a credential could ride out on) must
/// leave nothing in the logs at any level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_rabbitmq_stream() {
    let root = need_db!();
    let Some(broker) = std::env::var("LAZYWF_TEST_RABBITMQ_STREAM")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        return eprintln!(
            "LAZYWF_TEST_RABBITMQ_STREAM not set: skipping the stream flow (make test-e2e-queues)"
        );
    };
    install();
    let db = Db::fresh(&root, "logrmqs").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(20))")
        .await;
    tables(&db).await;
    // The flow's own account (dropped at its end; its absence is not an error) and the flow's
    // own stream, created over the suite's `lwftest` account with the official client.
    let _ = std::process::Command::new("docker")
        .args([
            "exec",
            "lazywf-test-rabbitmq-1",
            "rabbitmqctl",
            "delete_user",
            RMQS_USER,
        ])
        .output();
    rabbitmqctl(&["add_user", RMQS_USER, RMQS_PW]);
    rabbitmqctl(&["set_permissions", "-p", "/", RMQS_USER, ".*", ".*", ".*"]);
    let (host, port) = match broker.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().expect("stream port")),
        None => (broker.clone(), 5552),
    };
    let stream = format!("lwf.logsecrets.rmqstream.{}", std::process::id());
    let env = rabbitmq_stream_client::Environment::builder()
        .host(&host)
        .port(port)
        .username("lwftest")
        .password("lwftest")
        .build()
        .await
        .unwrap();
    env.delete_stream(&stream).await.ok();
    env.stream_creator().create(&stream).await.unwrap();
    let reader = StreamReader::setup(&stream).await;
    std::env::set_var("LWF_LOGRMQS_USER", RMQS_USER);
    std::env::set_var("LWF_LOGRMQS_PW", RMQS_PW);
    std::env::set_var("LWF_LOGRMQS_BAD", RMQS_BAD_PW);
    // The send that goes through: the account by reference, `expect` on the broker's confirm.
    db.target(
        "bus",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://{broker}/{stream}"),
        Some("JSON_EXTRACT(body, '$.publishing_id') IS NOT NULL"),
        &json!({"username": "env:LWF_LOGRMQS_USER", "password": "env:LWF_LOGRMQS_PW"}).to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'bus', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty FROM orders WHERE id IN (:keys)",
            RMQS_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    // And the send that cannot: the same stream, an account whose password is wrong.
    db.target(
        "bad",
        "rabbitmq-stream",
        &format!("rabbitmq-stream://{broker}/{stream}"),
        None,
        &json!({"username": "env:LWF_LOGRMQS_USER", "password": "env:LWF_LOGRMQS_BAD"}).to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('items', 'bad', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, name FROM items WHERE id IN (:keys)",
            RMQS_PUT,
        ),
    )
    .await;
    db.trigger("items", "items", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    db.exec("INSERT INTO items VALUES (7, 'widget')").await;
    let mut ids = reader.at_least(2).await;
    ids.sort();
    assert_eq!(ids, vec!["1", "2"], "one record per key: {ids:?}");
    // Not `inst.quiet()`: the wrong password is refused while connecting, so nothing of that
    // batch is confirmed, and a refusal that says nothing about one key on its own is retried
    // rather than struck (`engine.rs judge_strikes`) — `pending_keys` of that rule stays 1 while
    // the instance runs, as it does for the servicebus flow below. What the test needs is that
    // the failing path really ran, so it waits for the attempt itself: the flush failure in the
    // log (of rule `items`, the failing target's rule) or the same refusal as the rule's
    // `last_error`. Then the instance stops and the capture is read.
    wait_for(
        Duration::from_secs(90),
        "the wrong-password attempt",
        || async {
            let url = format!("rabbitmq-stream://{broker}/{stream}");
            let out = captured();
            let attempted =
                out.contains("flush failed") && out.contains(&url) && out.contains("rule=items");
            let reported = db
                .status("items")
                .await
                .and_then(|s| s.3)
                .is_some_and(|e| e.contains(&url));
            (attempted || reported).then_some(())
        },
    )
    .await;
    inst.stop().await;
    rabbitmqctl(&["delete_user", RMQS_USER]);
    env.delete_stream(&stream).await.ok();
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("stream username", RMQS_USER),
            ("stream password", RMQS_PW),
            ("stream wrong password", RMQS_BAD_PW),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// The servicebus kind over AMQP 1.0 (the same RabbitMQ answers 1.0 on its 5672, published here
/// as `LAZYWF_TEST_SERVICEBUS=127.0.0.1:34610`): the SASL PLAIN password and the SAS key never
/// appear in a log line — nor does the token built from the key — on a send that goes through
/// (PLAIN) and on one that fails (this broker has no `$cbs` node, so CBS cannot answer).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn servicebus_secrets_never_appear() {
    let root = need_db!();
    let Some(broker) = std::env::var("LAZYWF_TEST_SERVICEBUS").ok() else {
        return eprintln!("LAZYWF_TEST_SERVICEBUS not set: skipping");
    };
    let db = Db::fresh(&root, "sb_secrets").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(20))")
        .await;
    tables(&db).await;
    // The flow's own account and its own SAS rule key; both by reference only.
    let _ = std::process::Command::new("docker")
        .args([
            "exec",
            "lazywf-test-rabbitmq-1",
            "rabbitmqctl",
            "delete_user",
            SB_USER,
        ])
        .output();
    rabbitmqctl(&["add_user", SB_USER, SB_PW]);
    rabbitmqctl(&["set_permissions", "-p", "/", SB_USER, ".*", ".*", ".*"]);
    std::env::set_var("LWF_LOGSB_USER", SB_USER);
    std::env::set_var("LWF_LOGSB_PW", SB_PW);
    std::env::set_var("LWF_LOGSB_KEY", SB_KEY);
    install();
    let q = format!("lwf.logsecrets.servicebus.{}", std::process::id());
    let consumer = Sbus::setup(&broker, &q).await;

    // The broker's AMQP 1.0 listener takes SASL PLAIN, so this send goes through (`dedup` puts
    // a recognizable id on the wire, which is how the test sees its own messages).
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, vars, timeout_ms) \
         VALUES ('sb', 'servicebus', ?, 'off', ?, 8000)",
        (
            format!("sb://{broker}/queues/{q}"),
            json!({"username": "env:LWF_LOGSB_USER", "password": "env:LWF_LOGSB_PW",
                   "dedup": true})
            .to_string(),
        ),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'sb', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty FROM orders WHERE id IN (:keys)",
            SB_PUT,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    // And a send that cannot: a SAS target puts its token to `$cbs`, which this broker has no
    // node for. The failure is what must stay clean of the key.
    db.exec_params(
        "INSERT INTO lazy_wire_fire_target (name, kind, endpoints, tls, vars, timeout_ms) \
         VALUES ('sbsas', 'servicebus', ?, 'off', ?, 3000)",
        (
            format!("sb://{broker}/queues/{q}"),
            json!({"key_name": "RootManageSharedAccessKey", "key": "env:LWF_LOGSB_KEY"})
                .to_string(),
        ),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('items', 'sbsas', 'batch', ?, ?, 100)",
        (
            "SELECT id AS _key, name FROM items WHERE id IN (:keys)",
            SB_PUT,
        ),
    )
    .await;
    db.trigger("items", "items", Some("{{new.id}}"), None).await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;
    db.exec("INSERT INTO items VALUES (7, 'widget')").await;

    let mut ids = consumer.ids(2).await;
    ids.sort();
    assert_eq!(ids.len(), 2, "one message per key: {ids:?}");
    for id in &ids {
        assert_eq!(id.len(), 64, "the dedup id of {id}");
    }
    assert_ne!(ids[0], ids[1], "the two keys differ: {ids:?}");
    // The SAS target's own failing attempt is the line the key could leak into. This broker has
    // no `$cbs` node and does not complete a SASL ANONYMOUS handshake either (Service Bus allows
    // CBS on ANONYMOUS only), so the attempt ends in the target's time limit and the engine
    // retries it as the transient failure it is — the log line and the rule's `last_error` are
    // the evidence that it was made. (CBS against a real namespace is therefore unproven here;
    // the report says so, and `cbs_put_token_request_has_the_shape_the_node_wants` covers the
    // request's shape.)
    wait_for(
        Duration::from_secs(90),
        "the SAS target's failing attempt",
        || async {
            let url = format!("sb://{broker}/queues/{q}");
            let out = captured();
            let attempted =
                out.contains("flush failed") && out.contains(&url) && out.contains("rule=items");
            let reported = db
                .status("items")
                .await
                .and_then(|s| s.3)
                .is_some_and(|e| e.contains(&url));
            (attempted || reported).then_some(())
        },
    )
    .await;
    inst.stop().await;
    rabbitmqctl(&["delete_user", SB_USER]);

    // The token the kind builds for that key and audience: as secret as the key itself.
    let now = lazy_wire_fire::time::unix_now_ms() / 1000;
    let token = lazy_wire_fire::servicebus::sas_token(
        &format!("sb://{broker}/queues/{q}"),
        "RootManageSharedAccessKey",
        SB_KEY,
        now,
        3600,
    )
    .unwrap();
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("SASL user", SB_USER),
            ("SASL password", SB_PW),
            ("SAS key", SB_KEY),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
    // A token is only secret while its inputs are: the one of this minute must not be there
    // either (the expiry moves, so its own `se` is not part of the compare).
    let sig = token
        .split('&')
        .find(|p| p.starts_with("sig="))
        .unwrap_or_default()
        .to_string();
    assert!(!sig.is_empty(), "the token carries a signature: {token}");
    assert!(!out.contains(&sig), "the SAS signature is in the logs");
    assert!(
        !out.contains(SB_KEY),
        "the key is in the log of the CBS attempt"
    );
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

/// The Redpanda admin API of the test broker (`LAZYWF_TEST_KAFKA_ADMIN`): the flow's account
/// and the cluster settings it turns on for the flow, restored at its end.
struct KafkaAdmin {
    host: String,
    client: Client,
}

impl KafkaAdmin {
    fn new(root: &str) -> KafkaAdmin {
        KafkaAdmin {
            host: root.trim_start_matches("http://").to_string(),
            client: Client::new(
                BaseUrl::parse(root).unwrap(),
                &TlsSpec::parse(None).unwrap(),
            )
            .unwrap(),
        }
    }

    /// One call; the answer's JSON (an empty answer is `Value::Null`). `body` null: no body
    /// (the GETs).
    async fn call(&self, method: &str, path: &str, body: Value) -> Value {
        let mut req = RawRequest {
            method: method.into(),
            target: path.into(),
            headers: Vec::new(),
            body: if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            },
        };
        req.set_header("Host", self.host.clone());
        if !body.is_null() {
            req.set_header("Content-Type", "application/json".into());
        }
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
        if resp.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&resp.body).unwrap()
        }
    }

    /// The flow's SCRAM account.
    async fn create_user(&self, user: &str, password: &str) {
        self.call(
            "POST",
            "/v1/security/users",
            json!({"username": user, "password": password, "algorithm": "SCRAM-SHA-256"}),
        )
        .await;
    }

    /// Deletes the account (404: it is already gone, a rerun after a failed one).
    async fn delete_user(&self, user: &str) {
        let mut req = RawRequest {
            method: "DELETE".into(),
            target: format!("/v1/security/users/{user}"),
            headers: Vec::new(),
            body: Vec::new(),
        };
        req.set_header("Host", self.host.clone());
        let resp = self
            .client
            .send(&req, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(
            resp.status == 200 || resp.status == 404,
            "delete the user {user}: {}",
            resp.status
        );
    }

    /// The current value of one cluster key (the admin's native JSON).
    async fn cluster_value(&self, key: &str) -> Value {
        self.call("GET", &format!("/v1/cluster_config?key={key}"), Value::Null)
            .await[key]
            .clone()
    }

    /// Sets one cluster key to `value` (the admin's string form of it).
    async fn cluster_set(&self, key: &str, value: &str) {
        self.cluster_set_json(key, json!(value)).await;
    }

    /// Sets one cluster key to a native JSON `value` (array-typed keys as `superusers` need
    /// the array itself, not its string form).
    async fn cluster_set_json(&self, key: &str, value: Value) {
        self.call(
            "PUT",
            "/v1/cluster_config",
            json!({"remove": [], "upsert": {key: value}}),
        )
        .await;
    }

    /// Drops one cluster key (back to the broker's default; restores what a GET left as null).
    async fn cluster_drop(&self, key: &str) {
        self.call(
            "PUT",
            "/v1/cluster_config",
            json!({"remove": [key], "upsert": {}}),
        )
        .await;
    }

    /// Restores one key from the `cluster_value` read at the flow's start.
    async fn cluster_restore(&self, key: &str, was: &Value) {
        match was {
            Value::Null => self.cluster_drop(key).await,
            v => self.cluster_set_json(key, v.clone()).await,
        }
    }
}

/// Puts the test broker back the way the kafka flow found it when that flow ends, *including*
/// when it ends by panicking. This matters beyond the flow: `enable_sasl` left on makes every
/// unauthenticated Kafka call fail (`rpk`, and the other kafka flows' topic creation), so one
/// dead flow would break the ones after it on the shared broker. On the normal path the flow
/// restores and drops the topic itself and marks this done, so `Drop` does nothing.
struct BrokerRestore {
    /// The admin API's address, for the settings and the account.
    root: String,
    /// The bootstrap address, for dropping the flow's own topic.
    broker: String,
    topic: String,
    user: &'static str,
    sasl_was: Value,
    supers_was: Value,
    done: bool,
}

impl BrokerRestore {
    /// The flow's own restore, awaited at its end; afterwards `Drop` has nothing left to do.
    async fn restore(&mut self) {
        let admin = KafkaAdmin::new(&self.root);
        admin.cluster_restore("enable_sasl", &self.sasl_was).await;
        admin.cluster_restore("superusers", &self.supers_was).await;
        admin.delete_user(self.user).await;
        self.delete_topic().await;
        self.done = true;
    }

    async fn delete_topic(&self) {
        match rskafka::client::ClientBuilder::new(vec![self.broker.clone()])
            .client_id("lazywf-logs")
            .backoff_config(rskafka::BackoffConfig {
                deadline: Some(Duration::from_secs(5)),
                ..Default::default()
            })
            .build()
            .await
        {
            Ok(c) => {
                if let Ok(cc) = c.controller_client() {
                    cc.delete_topic(&self.topic, 5_000).await.ok();
                }
            }
            Err(e) => {
                eprintln!(
                    "the kafka broker at {}: no client to drop the topic: {e:#}",
                    self.broker
                )
            }
        }
    }
}

impl Drop for BrokerRestore {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        eprintln!(
            "the kafka log flow ended without restoring the broker, doing it now \
             (`enable_sasl` left on would break every later kafka flow)"
        );
        let (root, broker, topic, user) = (
            self.root.clone(),
            self.broker.clone(),
            self.topic.clone(),
            self.user,
        );
        let (sasl, supers) = (self.sasl_was.clone(), self.supers_was.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        // On a thread of its own with a runtime of its own: `Drop` cannot await, and the test's
        // runtime is the one being unwound. A panic here while unwinding would abort the binary,
        // so it is contained; a restore that does not finish in time is left to the next run.
        std::thread::spawn(move || {
            let done = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                rt.block_on(async move {
                    let admin = KafkaAdmin::new(&root);
                    admin.cluster_restore("enable_sasl", &sasl).await;
                    admin.cluster_restore("superusers", &supers).await;
                    admin.delete_user(user).await;
                    let cleanup = BrokerRestore {
                        root,
                        broker,
                        topic,
                        user,
                        sasl_was: sasl,
                        supers_was: supers,
                        done: true,
                    };
                    cleanup.delete_topic().await;
                });
            }));
            let _ = tx.send(done.is_ok());
        });
        let _ = rx.recv_timeout(std::time::Duration::from_secs(10));
    }
}

/// The `kafka` target against the test broker (`LAZYWF_TEST_KAFKA`, admin API
/// `LAZYWF_TEST_KAFKA_ADMIN`): the account is the flow's own (SCRAM-SHA-256; its password is
/// a fake secret, a reference, never a literal), and the records are read back through the
/// broker as that account.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_kafka() {
    let root = need_db!();
    let Some(broker) = std::env::var("LAZYWF_TEST_KAFKA")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        return eprintln!(
            "LAZYWF_TEST_KAFKA not set: skipping the kafka flow (make test-e2e-queues)"
        );
    };
    let Some(admin_root) = std::env::var("LAZYWF_TEST_KAFKA_ADMIN")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        return eprintln!("LAZYWF_TEST_KAFKA_ADMIN not set: skipping the kafka flow");
    };
    install();
    let admin = KafkaAdmin::new(&admin_root);

    // The broker's settings as the flow finds them (put back by the flow, or by the guard if the
    // flow panics). The flow's own account is taken out of the recorded `superusers`: a rerun
    // after a dead run must not put the dead run's account back.
    let mut sasl_was = admin.cluster_value("enable_sasl").await;
    let mut supers_was = admin.cluster_value("superusers").await;
    if let Some(list) = supers_was.as_array_mut() {
        list.retain(|u| u.as_str() != Some(KAFKA_USER));
    }
    if sasl_was.as_bool() == Some(true) {
        // A previous run died with it on. On this shared broker `off` is the only state the
        // other kafka flows can use (their `rpk` and topic calls are anonymous), so it is what
        // gets restored either way -- and it goes off now, before the anonymous calls below.
        eprintln!("`enable_sasl` was already on from a dead run: healing it");
        admin.cluster_set("enable_sasl", "false").await;
        sasl_was = Value::Bool(false);
    }
    let topic = format!("lwf-logkafka-{}", std::process::id());
    let mut broker_state = BrokerRestore {
        root: admin_root.clone(),
        broker: broker.clone(),
        topic: topic.clone(),
        user: KAFKA_USER,
        sasl_was: sasl_was.clone(),
        supers_was: supers_was.clone(),
        done: false,
    };

    // The topic, made by the test's own anonymous client (the broker has no SASL yet).
    {
        let anon = rskafka::client::ClientBuilder::new(vec![broker.clone()])
            .client_id("lazywf-logs")
            .backoff_config(rskafka::BackoffConfig {
                deadline: Some(Duration::from_secs(10)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap_or_else(|e| panic!("the kafka broker at {broker}: {e:#}"));
        anon.controller_client()
            .unwrap()
            .create_topic(&topic, 1, 1, 10_000)
            .await
            .unwrap_or_else(|e| panic!("creating the topic `{topic}`: {e:#}"));
    }

    // The flow's account, and the settings that use it: a rerun after a failed one drops the
    // old user first (its absence is not an error).
    admin.delete_user(KAFKA_USER).await;
    admin.create_user(KAFKA_USER, KAFKA_PW).await;
    admin
        .cluster_set_json("superusers", json!([KAFKA_USER]))
        .await;
    admin.cluster_set("enable_sasl", "true").await;

    let db = Db::fresh(&root, "logkafka").await;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, sku VARCHAR(20), qty INT)")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGKAFKA_USER", KAFKA_USER);
    std::env::set_var("LWF_LOGKAFKA_PW", KAFKA_PW);
    db.target(
        "events",
        "kafka",
        &format!("kafka://{broker}/{topic}"),
        Some("JSON_EXTRACT(body, '$.offset') IS NOT NULL"),
        &json!({"sasl": "scram-sha-256",
                "username": "env:LWF_LOGKAFKA_USER",
                "password": "env:LWF_LOGKAFKA_PW"})
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) \
         VALUES ('orders', 'events', 'key', ?, ?, 100)",
        (
            "SELECT id AS _key, sku, qty FROM orders WHERE id IN (:keys)",
            r#"POST / HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{"value": {{json doc}}, "headers": {"op": {{json op}}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#,
        ),
    )
    .await;
    db.trigger("orders", "orders", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO orders VALUES (1, 'A-1', 3), (2, 'B-2', 1)")
        .await;

    // The records, read back as the flow's account (a SCRAM handshake on the test's own
    // client).
    let reader = rskafka::client::ClientBuilder::new(vec![broker.clone()])
        .client_id("lazywf-logs")
        .backoff_config(rskafka::BackoffConfig {
            deadline: Some(Duration::from_secs(10)),
            ..Default::default()
        })
        .sasl_config(rskafka::client::SaslConfig::ScramSha256(
            rskafka::client::Credentials::new(KAFKA_USER.to_string(), KAFKA_PW.to_string()),
        ))
        .build()
        .await
        .unwrap_or_else(|e| panic!("the kafka broker at {broker}: {e:#}"));
    use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
    let pc = reader
        .partition_client(&topic, 0, UnknownTopicHandling::Retry)
        .await
        .unwrap_or_else(|e| panic!("the leader of `{topic}/0`: {e:#}"));
    let mut offset = pc
        .get_offset(OffsetAt::Earliest)
        .await
        .unwrap_or_else(|e| panic!("the earliest offset of `{topic}/0`: {e:#}"));
    let mut keys = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (recs, hw) = pc
            .fetch_records(offset, 0..1_048_576, 500)
            .await
            .unwrap_or_else(|e| panic!("the records of `{topic}/0`: {e:#}"));
        for r in &recs {
            keys.push(String::from_utf8(r.record.key.clone().unwrap()).unwrap());
        }
        offset = if recs.is_empty() {
            hw
        } else {
            recs.last().unwrap().offset + 1
        };
        if keys.len() >= 2 || std::time::Instant::now() > deadline {
            break;
        }
        sleep_ms(100).await;
    }
    keys.sort();
    assert_eq!(keys, vec!["1", "2"], "one record per key: {keys:?}");
    inst.quiet().await;
    inst.stop().await;

    // The broker's back the way the flow found it: its old settings, no user, no topic. The
    // guard notes it, so a later panic in this flow (the leak assertions) cannot leave the
    // broker mid-change.
    broker_state.restore().await;

    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[("kafka username", KAFKA_USER), ("kafka password", KAFKA_PW)],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// moto: the EventBridge JSON API and the SQS JSON API of the read-back queue. moto dispatches
/// by the service of the credential scope and does not check the signature.
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

    async fn call(&self, target: &str, service: &str, body: serde_json::Value) {
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
            format!(
                "AWS4-HMAC-SHA256 Credential=lazywf/20260101/us-east-1/{service}/aws4_request, \
                 SignedHeaders=content-type;host;x-amz-target, Signature=00"
            ),
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
    }

    /// Creates the bus and a queue, and the rule on the bus that delivers every event of it
    /// to the queue (an SQS target, as AWS would); the queue URL. moto answers as the
    /// account 123456789012.
    async fn pipe(&self, bus: &str, queue: &str) -> String {
        self.call("AWSEvents.CreateEventBus", "events", json!({"Name": bus}))
            .await;
        self.call("AmazonSQS.CreateQueue", "sqs", json!({"QueueName": queue}))
            .await;
        self.call(
            "AWSEvents.PutRule",
            "events",
            json!({"Name": format!("{bus}-rule"), "EventBusName": bus, "EventPattern": "{}"}),
        )
        .await;
        self.call(
            "AWSEvents.PutTargets",
            "events",
            json!({"EventBusName": bus, "Rule": format!("{bus}-rule"),
                   "Targets": [{"Id": "sqs",
                                "Arn": format!("arn:aws:sqs:us-east-1:123456789012:{queue}")}]}),
        )
        .await;
        format!("{}/123456789012/{queue}", self.root)
    }

    /// One call of the query protocol, which is how the SNS API documents itself (`Action=…`,
    /// `Version=2010-03-31`); the status and the answer's text.
    async fn query(&self, service: &str, action: &str, params: &[(&str, &str)]) -> (u16, String) {
        let body = std::iter::once(("Action", action))
            .chain(std::iter::once(("Version", "2010-03-31")))
            .chain(params.iter().copied())
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: body.into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header(
            "Content-Type",
            "application/x-www-form-urlencoded".to_string(),
        );
        req.set_header(
            "Authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential=lazywf/20260101/us-east-1/{service}/aws4_request, \
                 SignedHeaders=content-type;host, Signature=00"
            ),
        );
        let resp = self
            .client
            .send(&req, Duration::from_secs(5))
            .await
            .unwrap();
        (
            resp.status,
            String::from_utf8_lossy(&resp.body).into_owned(),
        )
    }

    /// Creates the topic and subscribes a queue to it (what AWS then delivers to the queue); the
    /// queue URL. moto answers as the account 123456789012.
    async fn pipe_topic(&self, topic: &str, queue: &str) -> String {
        let (status, xml) = self.query("sns", "CreateTopic", &[("Name", topic)]).await;
        assert_eq!(status, 200, "CreateTopic: {xml}");
        let open = "<TopicArn>";
        let from = xml.find(open).unwrap() + open.len();
        let arn = xml[from..xml[from..].find("</TopicArn>").unwrap() + from].to_string();
        self.call("AmazonSQS.CreateQueue", "sqs", json!({"QueueName": queue}))
            .await;
        let (status, xml) = self
            .query(
                "sns",
                "Subscribe",
                &[
                    ("TopicArn", &arn),
                    ("Protocol", "sqs"),
                    (
                        "Endpoint",
                        &format!("arn:aws:sqs:us-east-1:123456789012:{queue}"),
                    ),
                ],
            )
            .await;
        assert_eq!(status, 200, "Subscribe: {xml}");
        format!("{}/123456789012/{queue}", self.root)
    }

    async fn count(&self, url: &str) -> u64 {
        let mut req = lazy_wire_fire::http::RawRequest {
            method: "POST".into(),
            target: "/".into(),
            headers: Vec::new(),
            body: json!({"QueueUrl": url, "AttributeNames": ["ApproximateNumberOfMessages"]})
                .to_string()
                .into_bytes(),
        };
        req.set_header("Host", self.root.trim_start_matches("http://").to_string());
        req.set_header("Content-Type", "application/x-amz-json-1.0".into());
        req.set_header("X-Amz-Target", "AmazonSQS.GetQueueAttributes".into());
        req.set_header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=lazywf/20260101/us-east-1/sqs/aws4_request, \
             SignedHeaders=content-type;host;x-amz-target, Signature=00"
                .into(),
        );
        let resp = self
            .client
            .send(&req, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(resp.status == 200, "GetQueueAttributes: {}", resp.status);
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        v["Attributes"]["ApproximateNumberOfMessages"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }
}

/// The `eventbridge` target against moto (`LAZYWF_TEST_EVENTBRIDGE`): every put is signed with
/// the fake reference credentials, and the bus is read back through a rule with an SQS target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_eventbridge() {
    let root = need_db!();
    let Ok(moto_root) = std::env::var("LAZYWF_TEST_EVENTBRIDGE") else {
        return eprintln!(
            "LAZYWF_TEST_EVENTBRIDGE not set: skipping the eventbridge flow (make test-e2e-queues)"
        );
    };
    install();
    let moto = Moto::new(&moto_root).await;
    let bus = format!("lwf_logseb_{}", std::process::id());
    let url = moto.pipe(&bus, &bus).await;
    let db = Db::fresh(&root, "logeb").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGB_AK", AWS_AK);
    std::env::set_var("LWF_LOGB_SK", AWS_SK);
    std::env::set_var("LWF_LOGB_ST", AWS_SESSION);
    db.target(
        "bus",
        "eventbridge",
        &format!("aws-eventbridge://us-east-1/{bus}?endpoint={moto_root}"),
        None,
        &json!({"access_key_ref": "env:LWF_LOGB_AK",
                "secret_key_ref": "env:LWF_LOGB_SK",
                "session_token_ref": "env:LWF_LOGB_ST"})
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('bus', 'bus', 'batch', 10, ?, ?, 100)",
        (
            "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
            r#"POST / HTTP/1.1
X-Amz-Target: AWSEvents.PutEvents
Content-Type: application/x-amz-json-1.1

{"Entries": [{{#each docs}}{"Source": "lazywf", "DetailType": {{json key}}, "Detail": {{json (json doc)}}, "EventBusName": {{json endpoint.bus}}}{{#unless @last}}, {{/unless}}{{/each}}]}"#,
        ),
    )
    .await;
    db.trigger("bus", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    wait_for(Duration::from_secs(30), "two events", || async {
        (moto.count(&url).await >= 2).then_some(())
    })
    .await;
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("bus secret key", AWS_SK),
            ("bus session token", AWS_SESSION),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// The `sns` target against moto (`LAZYWF_TEST_SNS`): every batch is signed with the fake
/// reference credentials, and the topic is read back through a subscription of an SQS queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_sns() {
    let root = need_db!();
    let Ok(moto_root) = std::env::var("LAZYWF_TEST_SNS") else {
        return eprintln!("LAZYWF_TEST_SNS not set: skipping the sns flow (make test-e2e-queues)");
    };
    install();
    let moto = Moto::new(&moto_root).await;
    let topic = format!("lwf_logssns_{}", std::process::id());
    let url = moto.pipe_topic(&topic, &topic).await;
    let db = Db::fresh(&root, "logsns").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;
    std::env::set_var("LWF_LOGSNS_AK", AWS_AK);
    std::env::set_var("LWF_LOGSNS_SK", AWS_SK);
    std::env::set_var("LWF_LOGSNS_ST", AWS_SESSION);
    db.target(
        "topic",
        "sns",
        &format!("aws-sns://us-east-1/123456789012/{topic}?endpoint={moto_root}"),
        None,
        &json!({"access_key_ref": "env:LWF_LOGSNS_AK",
                "secret_key_ref": "env:LWF_LOGSNS_SK",
                "session_token_ref": "env:LWF_LOGSNS_ST"})
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms) \
         VALUES ('topic', 'topic', 'batch', 10, ?, ?, 100)",
        (
            "SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)",
            r#"POST / HTTP/1.1
Content-Type: application/x-www-form-urlencoded

Action=PublishBatch&Version=2010-03-31&TopicArn={{url_encode endpoint.topic_arn}}{{#each docs}}&PublishBatchRequestEntries.member.{{add @index 1}}.Id={{@index}}&PublishBatchRequestEntries.member.{{add @index 1}}.Message={{url_encode (json doc)}}{{/each}}"#,
        ),
    )
    .await;
    db.trigger("topic", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    wait_for(Duration::from_secs(30), "two messages", || async {
        (moto.count(&url).await >= 2).then_some(())
    })
    .await;
    inst.quiet().await;
    inst.stop().await;
    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    let leaks = leaks(
        &out,
        &[
            ("topic secret key", AWS_SK),
            ("topic session token", AWS_SESSION),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// The credential of a `grpc` target: `vars.token` is a reference whose value becomes the
/// `authorization: Bearer …` metadata of every call. The flow drives the kind over every path
/// where a credential could be repeated — the target's load, an endpoint context and an action's
/// template variables, the refusals an owner can write, a host that never answers — and then
/// flushes two keys for real at the in-process gRPC server of `common::grpc_server`, which wants
/// exactly this token and so proves the call carried it. Neither the value nor the descriptor
/// set's contents may turn up in the capture: not in lazywf's own lines, and not in what tonic and
/// h2 log around a call that succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_grpc() {
    install();
    // The same directory the other flows use: the var is process-wide.
    let dir = std::env::current_dir()
        .unwrap()
        .join("tmp/out/secrets-logs");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("grpc-token"), format!("{GRPC_TOKEN}\n")).unwrap();
    let desc = dir.join("logsecrets.desc");
    std::fs::write(&desc, lazy_wire_fire::grpc::contract::DESCRIPTOR).unwrap();
    // The schema a flushing target builds its documents onto: an app's own message, and a service
    // of the contract's shape to call. This binary compiles it here, once.
    let schema = common::grpc_server::write_schema(&dir);
    // Not a descriptor set, and its bytes are the token: a refusal that quoted the file would
    // be a leak of the credential.
    let not_desc = dir.join("not-a-desc");
    std::fs::write(&not_desc, format!("not protobuf: {GRPC_TOKEN}")).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    std::env::set_var("LWF_LOGGRPC_TOKEN", GRPC_TOKEN);

    let ep = "grpcs://ingest.example.test:9000/lazywf.v1.Sink/Push";
    let good = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item","token":"env:LWF_LOGGRPC_TOKEN"}}"#,
        desc.display()
    );
    let row = |vars: String, endpoint: &str| lazy_wire_fire::model::RawTarget {
        name: "sink".into(),
        kind: Some("grpc".into()),
        endpoints: Some(endpoint.into()),
        vars: Some(vars),
        ..Default::default()
    };

    let def = lazy_wire_fire::model::TargetDef::from_row(&row(good.clone(), ep)).expect("loads");
    // What a template can render, and what a Debug of the endpoint holds: the token is in
    // neither, so nothing that logs them can print it.
    tracing::debug!(vars = %def.template_vars(), "grpc target loaded");
    let (ctx, client) = lazy_wire_fire::grpc::Client::new(&def, &def.endpoints[0]).expect("starts");
    tracing::trace!(?ctx, ?client.opts, "the endpoint of a grpc target");

    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    let target =
        lazy_wire_fire::target::Target::new(def, pool.clone(), None).expect("the target starts");
    // A `once` step whose body is not the `docs` array is refused, in a message that names the
    // endpoint and nothing else.
    let e = target.send(0, "{}").await.unwrap_err();
    tracing::debug!(error = %e, "a once step of a grpc target");
    assert!(format!("{e:#}").contains("lazywf.v1.Sink/Push"), "{e:#}");

    // A descriptor set that is not one is refused when the target starts (the row alone only
    // says what the vars have to look like), and the refusal does not quote the file — whose
    // bytes here are the token.
    let bad = lazy_wire_fire::model::TargetDef::from_row(&row(
        format!(
            r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item","token":"env:LWF_LOGGRPC_TOKEN"}}"#,
            not_desc.display()
        ),
        ep,
    ))
    .and_then(|def| {
        lazy_wire_fire::target::Target::new(
            def,
            mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x"),
            None,
        )
    })
    .err()
    .unwrap_or_else(|| panic!("accepted a file that is not a descriptor set"));
    tracing::debug!(error = %bad, "a descriptor set that is not one");

    // The refusals an owner can write with a credential: each names the var or the endpoint,
    // none repeats what it was given.
    for (what, vars, endpoint) in [
        (
            "a literal token",
            format!(
                r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item","token":"{GRPC_TOKEN}"}}"#,
                desc.display()
            ),
            ep.to_string(),
        ),
        (
            "credentials in the URL",
            good.clone(),
            format!("grpcs://{GRPC_TOKEN}@ingest.example.test:9000/lazywf.v1.Sink/Push"),
        ),
        (
            "a token that is a bare path instead of a reference",
            format!(
                r#"{{"descriptor_set":"file:{}","item_type":"lazywf.v1.Item","token":"{}"}}"#,
                desc.display(),
                dir.join("grpc-token").display()
            ),
            ep.to_string(),
        ),
    ] {
        let e = lazy_wire_fire::model::TargetDef::from_row(&row(vars, &endpoint))
            .err()
            .unwrap_or_else(|| panic!("accepted {what}"));
        tracing::debug!(error = %e, "a grpc target refused");
    }

    use lazy_wire_fire::target::Item;
    // What a flush of two keys is: the `docs` array a rule's template renders for them, and the
    // keys the engine hands the kind beside it.
    let keys: Vec<String> = vec!["1".into(), "2".into()];
    let body = common::grpc_server::rendered(&["1", "2"]);
    let flush_vars = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"{}","token":"env:LWF_LOGGRPC_TOKEN"}}"#,
        schema,
        common::grpc_server::ITEM_TYPE,
    );
    let flushing = |endpoint: String| {
        let def = lazy_wire_fire::model::TargetDef::from_row(&row(flush_vars.clone(), &endpoint))
            .expect("loads");
        lazy_wire_fire::target::Target::new(def, pool.clone(), None).expect("the target starts")
    };

    // A host nothing answers: every key waits, and what the log has to say about the failed call
    // is lazywf's own words about the endpoint, never the credential it was given. The method is
    // the app schema's own, since this target's descriptor set is the one written above.
    let dead_ep = format!(
        "grpcs://ingest.example.test:9000/{}",
        common::grpc_server::METHOD
    );
    let dead = flushing(dead_ep);
    let items = dead
        .send_items(0, &body, &keys)
        .await
        .expect("the answer is per item");
    assert!(
        items.iter().all(|i| matches!(i, Item::Transient(_))),
        "{items:?}"
    );
    assert!(
        format!("{items:?}").contains("ingest.example.test:9000/catalog.v1.Ingest/Push"),
        "the failed call is not named by its endpoint: {items:?}"
    );

    // And the stream itself, at a server of lazywf's own that wants exactly this token: the call
    // the capture is taken around. The token is the `authorization` metadata of the call, so
    // everything the kind and its transport have to say about a call that *succeeds* is in the
    // buffer by now.
    let server = common::grpc_server::Server::start(common::grpc_server::Policy {
        token: Some(GRPC_TOKEN.to_string()),
        ..Default::default()
    })
    .await;
    let live = flushing(server.endpoint());
    let items = live
        .send_items(0, &body, &keys)
        .await
        .expect("the call itself went through");
    assert!(items.iter().all(|i| *i == Item::Ok), "{items:?}");
    // Both halves of this are what makes the check below mean something: the items really
    // reached the server, and what it authenticated the call with really was the token.
    assert_eq!(server.seen().len(), 2, "the flush did not reach the server");
    assert_eq!(
        server.header("authorization").as_deref(),
        Some(&format!("Bearer {GRPC_TOKEN}")[..]),
        "the token did not go as the call's bearer metadata"
    );
    server.stop();

    let out = captured();
    let leaks = leaks(&out, &[("grpc bearer token", GRPC_TOKEN)]);
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

/// An `http` target whose endpoint is itself a reference (`file:/path`, `env:NAME`): the incoming
/// webhook of a business app, where everything below the host is the credential. The flow
/// delivers to a stand-in at the exact secret path, while a third target whose reference names a
/// port nothing listens on keeps failing — which is what fills the log at `trace`. The captured
/// output, `/status` and every `last_error` are searched for the path and the query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_webhook_endpoint() {
    let root = need_db!();
    install();
    let mock = Mock::start().await;
    mock.respond(|_| (200, "{}".into()));
    let db = Db::fresh(&root, "logweb").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;

    let dir = std::env::current_dir()
        .unwrap()
        .join("tmp/out/secrets-logs");
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    let hook = dir.join("webhook");
    std::fs::write(
        &hook,
        format!(
            "http://{}/services/T0000/B0000/{WEBHOOK_PATH}?token={WEBHOOK_QUERY}\n",
            mock.addr
        ),
    )
    .unwrap();
    std::env::set_var(
        "LWF_LOGWEB_TEAMS",
        format!("http://{}/hooks/b4b0/{WEBHOOK_PATH}", mock.addr),
    );
    // A reference that names a port nothing listens on: its failed connection is what the log has
    // to show, at every level.
    let dead = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let broken = dir.join("webhook-dead");
    std::fs::write(
        &broken,
        format!("http://{dead}/services/T0000/B0000/{WEBHOOK_PATH}?token={WEBHOOK_QUERY}"),
    )
    .unwrap();

    for (name, endpoint) in [
        ("hook", format!("file:{}", hook.display())),
        ("teams", "env:LWF_LOGWEB_TEAMS".to_string()),
        ("dead", format!("file:{}", broken.display())),
    ] {
        db.target(name, "http", &endpoint, None, "{}").await;
        db.exec_params(
            "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 100)",
            (
                name,
                name,
                r#"POST / HTTP/1.1
Content-Type: application/json

{"text":"out of stock","at":{{json endpoint.url}}}"#,
            ),
        )
        .await;
        db.trigger(name, "customers", Some("{{new.id}}"), None)
            .await;
    }

    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')")
        .await;
    // The two reachable targets are called, each at its own secret path, with the origin (and
    // nothing below it) in the document the template built.
    wait_for(
        Duration::from_secs(30),
        "both webhooks are called",
        || async {
            let got = mock.requests();
            (got.iter().any(|r| {
                r.path == format!("/services/T0000/B0000/{WEBHOOK_PATH}?token={WEBHOOK_QUERY}")
            }) && got
                .iter()
                .any(|r| r.path == format!("/hooks/b4b0/{WEBHOOK_PATH}"))
                && got.iter().all(|r| r.body.contains("\"at\":\"http://")))
            .then_some(())
        },
    )
    .await;
    // The third is still failing: the engine has tried it, and said why.
    wait_for(
        Duration::from_secs(30),
        "the unreachable target has complained",
        || async {
            db.status("dead")
                .await
                .and_then(|(_, _, _, e)| e)
                .map(|_| ())
        },
    )
    .await;
    let status = serde_json::to_string(&inst.health.status()).unwrap();
    let mut errors: Vec<String> = Vec::new();
    for name in ["hook", "teams", "dead"] {
        if let Some((_, _, _, Some(e))) = db.status(name).await {
            errors.push(e);
        }
    }
    inst.stop().await;

    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    // The failing endpoint is still identifiable: its origin, with everything below it an
    // ellipsis.
    assert!(
        out.contains(&format!("POST http://{dead}/…")),
        "the unreachable endpoint is not named by its origin in the log"
    );
    // Everything that could carry the secret: the log, `/status` and every `last_error`.
    let everything = format!("{out}\n{status}\n{errors:#?}");
    let leaks = leaks(
        &everything,
        &[
            ("webhook path", WEBHOOK_PATH),
            ("webhook query", WEBHOOK_QUERY),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(
        leaks.is_empty(),
        "the webhook URL is in the output: {leaks:#?}"
    );
}

// ---- OAuth 2.0 access tokens of the plain http kinds (`vars.oauth`).

/// The oauth flow's own credentials: the client secret behind `vars.client_secret_ref` (a
/// `file:` reference) and the access token the token endpoint hands out. The endpoint and the
/// API below both echo them back — an endpoint that repeats what it was sent is exactly where a
/// credential would surface, in the error it produces and in what the engine logs of it.
const OAUTH_SECRET: &str = "LWFSECRET-oauth-client-secret-8d41";
const OAUTH_TOKEN: &str = "LWFSECRET-oauth-access-token-2e9";

/// What the stand-in of the oauth flow answers next: a token endpoint that echoes the request
/// it was sent, an API that refuses the credential (401), and the acceptance that follows.
#[derive(Clone, Copy, PartialEq)]
enum OauthPhase {
    TokenEchoes,
    Unauthorized,
    Accepted,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_oauth() {
    use base64::Engine as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let root = need_db!();
    install();
    let db = Db::fresh(&root, "logoauth").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;

    // The client secret, behind a `file:` reference in the directory the flows of this binary
    // share. `client_id` is an identifier and goes in literally.
    let dir = std::env::current_dir()
        .unwrap()
        .join("tmp/out/secrets-logs");
    std::fs::create_dir_all(&dir).unwrap();
    let secret_file = dir.join("oauth-client-secret");
    std::fs::write(&secret_file, format!("{OAUTH_SECRET}\n")).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());

    let mock = Mock::start().await;
    let phase = Arc::new(Mutex::new(OauthPhase::TokenEchoes));
    let tokens = Arc::new(AtomicUsize::new(0));
    {
        let (phase, tokens) = (phase.clone(), tokens.clone());
        mock.respond(move |r| match r.path.as_str() {
            "/token" => {
                let n = tokens.fetch_add(1, Ordering::SeqCst) + 1;
                if *phase.lock().unwrap() == OauthPhase::TokenEchoes {
                    // the echo of the request it was sent: the Basic credential of that request
                    // is the client secret in Base64, so a message quoting the echo carries it
                    (
                        400,
                        format!(
                            r#"{{"error": "invalid_client", "got": "{}"}}"#,
                            r.header("authorization").unwrap_or_default()
                        ),
                    )
                } else {
                    (
                        200,
                        format!(
                            r#"{{"access_token": "{OAUTH_TOKEN}-{n}", "token_type": "Bearer", "expires_in": 3600}}"#
                        ),
                    )
                }
            }
            "/hook" => match *phase.lock().unwrap() {
                OauthPhase::TokenEchoes => (200, "ok".into()),
                OauthPhase::Unauthorized => (
                    401,
                    format!(
                        r#"{{"error": "invalid_token", "authorization": "{}"}}"#,
                        r.header("authorization").unwrap_or_default()
                    ),
                ),
                OauthPhase::Accepted => (200, "ok".into()),
            },
            _ => (404, "{}".into()),
        });
    }

    db.target(
        "wh",
        "http",
        &mock.url(),
        None,
        &json!({
            "oauth": "client_credentials",
            "token_url": format!("{}/token", mock.url()),
            "client_id": "lwf-client",
            "client_secret_ref": format!("file:{}", secret_file.display()),
        })
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 50)",
        ("wh", "wh", "POST /hook HTTP/1.1\n\nok"),
    )
    .await;
    db.trigger("wh", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;

    let hits = |path: &str| mock.requests().iter().filter(|r| r.path == path).count();

    // 1. the token endpoint echoes the request it was sent (its Basic credential is the client
    // secret in Base64). The send fails; the key waits, and what the failure logs must not
    // carry the secret in any of its forms.
    wait_for(Duration::from_secs(30), "a token request", || async {
        (hits("/token") >= 1).then_some(())
    })
    .await;
    let err = wait_for(
        Duration::from_secs(30),
        "the token failure on the rule",
        || async {
            db.status("wh")
                .await
                .and_then(|(_, _, _, e)| e)
                .filter(|e| e.contains("token endpoint"))
        },
    )
    .await;
    assert!(!err.contains(OAUTH_SECRET), "the secret escaped: {err}");
    *phase.lock().unwrap() = OauthPhase::Unauthorized;

    // 2. the API refuses the credential: a 401 speaks about the token, so the key is kept and
    // the token is dropped; the message the endpoint produced echoes the token back, and that
    // message is what `last_error` and the log show.
    wait_for(
        Duration::from_secs(30),
        "the action to reach the API",
        || async { (hits("/hook") >= 1).then_some(()) },
    )
    .await;
    let err = wait_for(Duration::from_secs(30), "the 401 on the rule", || async {
        db.status("wh")
            .await
            .and_then(|(_, _, _, e)| e)
            .filter(|e| e.contains("oauth access token"))
    })
    .await;
    assert!(
        !err.contains(OAUTH_TOKEN),
        "the access token reached last_error: {err}"
    );
    *phase.lock().unwrap() = OauthPhase::Accepted;

    // 3. with a fresh token the same action goes through: the quiet engine has nothing left to
    // retry, which is the proof the 401 never refused the key.
    inst.quiet().await;
    inst.stop().await;

    let reqs = mock.requests();
    let basic = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("lwf-client:{OAUTH_SECRET}"))
    );
    let token_reqs: Vec<&Req> = reqs.iter().filter(|r| r.path == "/token").collect();
    assert!(
        token_reqs
            .iter()
            .all(|r| r.header("authorization") == Some(basic.as_str())),
        "the client did not authenticate with Basic as RFC 6749 §2.3.1 says"
    );
    assert!(
        token_reqs
            .iter()
            .any(|r| r.body.contains("grant_type=client_credentials")),
        "the grant was not in the form body"
    );
    let hooks: Vec<&Req> = reqs.iter().filter(|r| r.path == "/hook").collect();
    let bearers: Vec<&str> = hooks
        .iter()
        .map(|r| r.header("authorization").unwrap_or_default())
        .collect();
    assert!(
        bearers.iter().all(|b| b.starts_with("Bearer ")),
        "an action without the bearer token: {bearers:?}"
    );
    assert!(
        bearers.windows(2).any(|w| w[0] != w[1]),
        "the 401 did not drop the cached token: {bearers:?}"
    );

    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    assert!(
        out.contains("oauth token"),
        "the token endpoint's failure was never logged: it is what the flow ran on"
    );
    let leaks = leaks(
        &out,
        &[
            ("oauth client secret", OAUTH_SECRET),
            ("oauth access token", OAUTH_TOKEN),
        ],
    );
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}

// ---- The JWT bearer grant of `vars.oauth` (RFC 7523 §2.1, src/oauth.rs).

/// The access token the token endpoint hands out (the hit's number appended). The private
/// key comes from the test fixture under `src/testdata` (a throwaway key, see its README);
/// a distinctive run of its base64 body is the marker below. None of these may reach a log
/// line at `trace`, an error text or `last_error` — the token endpoint of this flow echoes
/// the whole request it was sent (its assertion included), and the API echoes the
/// `Authorization` header of the request it refuses.
const JWT_ACCESS_TOKEN: &str = "LWFSECRET-jwt-access-token";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_secret_in_the_logs_oauth_jwt() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let root = need_db!();
    install();
    let db = Db::fresh(&root, "logoauthjwt").await;
    db.exec("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    tables(&db).await;

    // The signing key behind a `file:` reference (the same secret directory the flows of
    // this binary share); iss/sub/aud are identifiers and go in literally.
    let dir = std::env::current_dir()
        .unwrap()
        .join("tmp/out/secrets-logs");
    std::fs::create_dir_all(&dir).unwrap();
    let key_pem = std::fs::read_to_string("src/testdata/rsa-test-key-pkcs8.pem").unwrap();
    let key_file = dir.join("oauth-jwt-key");
    std::fs::write(&key_file, &key_pem).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", dir.display().to_string());
    // a run of the key's base64 body: the marker that says the key bytes leaked
    let key_body: String = key_pem
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect::<String>();
    let key_marker = key_body[24..88].to_string();

    let mock = Mock::start().await;
    #[derive(Clone, Copy, PartialEq)]
    enum Phase {
        TokenEchoes,
        Unauthorized,
        Accepted,
    }
    let phase = Arc::new(Mutex::new(Phase::TokenEchoes));
    let tokens = Arc::new(AtomicUsize::new(0));
    {
        let (phase, tokens) = (phase.clone(), tokens.clone());
        mock.respond(move |r| match r.path.as_str() {
            "/token" => {
                let n = tokens.fetch_add(1, Ordering::SeqCst) + 1;
                if *phase.lock().unwrap() == Phase::TokenEchoes {
                    // the echo of the whole request: its `assertion=` field is the signed
                    // assertion — the credential of this grant for its 180 s of life
                    (
                        400,
                        format!(
                            r#"{{"error": "invalid_request", "error_description": "got {}"}}"#,
                            r.body
                        ),
                    )
                } else {
                    // Salesforce says no `expires_in`: the default lifetime applies and a
                    // shorter session is covered by the 401 drop below
                    (
                        200,
                        format!(
                            r#"{{"access_token": "{JWT_ACCESS_TOKEN}-{n}", "token_type": "Bearer"}}"#
                        ),
                    )
                }
            }
            "/hook" => match *phase.lock().unwrap() {
                Phase::TokenEchoes => (200, "ok".into()),
                Phase::Unauthorized => (
                    401,
                    format!(
                        r#"{{"error": "invalid_token", "authorization": "{}"}}"#,
                        r.header("authorization").unwrap_or_default()
                    ),
                ),
                Phase::Accepted => (200, "ok".into()),
            },
            _ => (404, "{}".into()),
        });
    }

    db.target(
        "wh",
        "http",
        &mock.url(),
        None,
        &json!({
            "oauth": "jwt_bearer",
            "token_url": format!("{}/token", mock.url()),
            "client_id": "3MVG9IssuerKey",
            "subject": "integration@company.test",
            "audience": "https://company.my.salesforce.com",
            "signing_key_ref": format!("file:{}", key_file.display()),
        })
        .to_string(),
    )
    .await;
    db.exec_params(
        "INSERT INTO lazy_wire_fire_rule (name, target, template, debounce_ms) VALUES (?, ?, ?, 50)",
        ("wh", "wh", "POST /hook HTTP/1.1\n\nok"),
    )
    .await;
    db.trigger("wh", "customers", Some("{{new.id}}"), None)
        .await;
    let inst = Instance::ready(config(&db, &[])).await;
    db.exec("INSERT INTO customers VALUES (1, 'Ann')").await;

    let hits = |path: &str| mock.requests().iter().filter(|r| r.path == path).count();

    // 1. the token endpoint echoes the assertion it was sent. The send fails; the key
    // waits; what the failure logs must carry neither the assertion, the key, nor a token.
    wait_for(Duration::from_secs(30), "a token request", || async {
        (hits("/token") >= 1).then_some(())
    })
    .await;
    let err = wait_for(
        Duration::from_secs(30),
        "the token failure on the rule",
        || async {
            db.status("wh")
                .await
                .and_then(|(_, _, _, e)| e)
                .filter(|e| e.contains("token endpoint"))
        },
    )
    .await;
    assert!(
        !err.contains(&key_marker),
        "the key escaped to last_error: {err}"
    );
    *phase.lock().unwrap() = Phase::Unauthorized;

    // 2. the API refuses the credential: the 401 speaks about the token, the key is kept,
    // the token dropped; the echoed 401 body must not carry the token out.
    wait_for(
        Duration::from_secs(30),
        "the action to reach the API",
        || async { (hits("/hook") >= 1).then_some(()) },
    )
    .await;
    let err = wait_for(Duration::from_secs(30), "the 401 on the rule", || async {
        db.status("wh")
            .await
            .and_then(|(_, _, _, e)| e)
            .filter(|e| e.contains("oauth access token"))
    })
    .await;
    assert!(
        !err.contains(JWT_ACCESS_TOKEN),
        "the access token reached last_error: {err}"
    );
    *phase.lock().unwrap() = Phase::Accepted;

    // 3. with a fresh token the same action goes through: nothing is left to retry, which
    // is the proof neither failure refused the key.
    inst.quiet().await;
    inst.stop().await;

    let reqs = mock.requests();
    let token_reqs: Vec<&Req> = reqs.iter().filter(|r| r.path == "/token").collect();
    assert!(
        token_reqs
            .iter()
            .all(|r| r.header("authorization").is_none()),
        "client authentication rode the assertion grant (RFC 7523 §2.1 needs none)"
    );
    let assertions: Vec<String> = token_reqs
        .iter()
        .filter_map(|r| {
            r.body
                .split("assertion=")
                .nth(1)
                .map(|a| a.split('&').next().unwrap_or_default().to_string())
        })
        .collect();
    assert!(
        token_reqs.iter().all(|r| r.body.starts_with(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="
        )),
        "the grant form was not what went out: {:?}",
        token_reqs.iter().map(|r| &r.body).collect::<Vec<_>>()
    );
    assert!(!assertions.is_empty(), "no assertion was ever sent");
    for a in &assertions {
        assert_eq!(a.matches('.').count(), 2, "not an assertion: {a}");
    }
    let hooks: Vec<&Req> = reqs.iter().filter(|r| r.path == "/hook").collect();
    let bearers: Vec<&str> = hooks
        .iter()
        .map(|r| r.header("authorization").unwrap_or_default())
        .collect();
    assert!(
        bearers.iter().all(|b| b.starts_with("Bearer ")),
        "an action without the bearer token: {bearers:?}"
    );
    assert!(
        bearers.windows(2).any(|w| w[0] != w[1]),
        "the 401 did not drop the cached token: {bearers:?}"
    );

    let out = captured();
    assert!(out.contains("starting"), "no engine log in the capture");
    assert!(
        out.contains("oauth token"),
        "the token endpoint's failure was never logged: it is what the flow ran on"
    );
    // the assertion is the credential of this grant: every one that was ever sent must be
    // absent from the capture, along with the key and the access token
    let mut secrets: Vec<(String, String)> = vec![
        ("jwt signing key".to_string(), key_marker.clone()),
        ("jwt access token".to_string(), JWT_ACCESS_TOKEN.to_string()),
    ];
    for (n, a) in assertions.into_iter().enumerate() {
        secrets.push((format!("jwt assertion #{n}"), a));
    }
    let refs: Vec<(&str, &str)> = secrets
        .iter()
        .map(|(w, s)| (w.as_str(), s.as_str()))
        .collect();
    let leaks = leaks(&out, &refs);
    if !leaks.is_empty() {
        dump(&out);
    }
    assert!(leaks.is_empty(), "secrets in the logs: {leaks:#?}");
}
