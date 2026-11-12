use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;
use crate::secrets::ENV_LOCK;

/// The environment of one test. Holds `ENV_LOCK`, clears every `AWS_*` variable and `HOME`
/// (so no shared file or instance of the host is read), then sets `vars`. Restored on drop.
struct Env {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    saved: Vec<(String, Option<String>)>,
}

impl Env {
    async fn set(vars: &[(&str, &str)]) -> Env {
        let lock = ENV_LOCK.lock().await;
        let mut env = Env {
            _lock: lock,
            saved: Vec::new(),
        };
        let names: Vec<String> = std::env::vars()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with("AWS_"))
            .collect();
        for k in names {
            env.touch(&k);
            std::env::remove_var(&k);
        }
        env.touch("HOME");
        std::env::set_var("HOME", scratch("home"));
        for (k, v) in vars {
            env.touch(k);
            std::env::set_var(k, v);
        }
        env
    }

    fn touch(&mut self, k: &str) {
        if !self.saved.iter().any(|(n, _)| n == k) {
            self.saved.push((k.to_string(), std::env::var(k).ok()));
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for (k, v) in self.saved.iter().rev() {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

/// A scratch directory of this test run under the repository's `tmp/`.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("unit")
        .join(format!("aws-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Reads one HTTP request (head and body) from `s`: its method and its path without query.
async fn read_request(s: &mut TcpStream) -> Option<(String, String)> {
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 4096];
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut len = 0usize;
    for line in head.lines() {
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            len = v.trim().parse().ok()?;
        }
    }
    while buf.len() < head_end + len {
        let mut chunk = [0u8; 4096];
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let mut parts = head.lines().next()?.split(' ');
    let method = parts.next()?.to_string();
    let path = parts.next()?.split('?').next()?.to_string();
    Some((method, path))
}

/// A plain-HTTP stand-in on 127.0.0.1. Answers each request whose method (`*` for any) and
/// path (without query) match a route with `200` and the route's body, else `404`. Returns
/// its base URL.
async fn stub(routes: Vec<(&'static str, &'static str, &'static str)>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let routes = routes.clone();
            tokio::spawn(async move {
                let Some((method, path)) = read_request(&mut s).await else {
                    return;
                };
                let hit = routes
                    .iter()
                    .find(|(m, p, _)| (*m == "*" || *m == method) && *p == path);
                let (status, body) = match hit {
                    Some((_, _, body)) => ("200 OK", *body),
                    None => ("404 Not Found", ""),
                };
                let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-aws-ec2-metadata-token-ttl-seconds: 21600\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    format!("http://{addr}")
}

fn access_key(c: &Credentials) -> &str {
    c.access_key_id()
}

#[tokio::test]
async fn environment_credentials() {
    let _env = Env::set(&[
        ("AWS_ACCESS_KEY_ID", "AKIDENV"),
        ("AWS_SECRET_ACCESS_KEY", "envsecret"),
        ("AWS_SESSION_TOKEN", "envtoken"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ])
    .await;
    let c = Provider::new(None, "eu-west-1").get().await.unwrap();
    assert_eq!(access_key(&c), "AKIDENV");
    assert_eq!(c.secret_access_key(), "envsecret");
    assert_eq!(c.session_token(), Some("envtoken"));
}

#[tokio::test]
async fn shared_profile_credentials() {
    let dir = scratch("profile");
    let file = dir.join("credentials");
    std::fs::write(
        &file,
        "[default]\naws_access_key_id = AKIDDEF\naws_secret_access_key = defsecret\n\
             [lwf]\naws_access_key_id = AKIDPROF\naws_secret_access_key = profsecret\n",
    )
    .unwrap();
    let file = file.display().to_string();
    let no_config = dir.join("no-config").display().to_string();
    {
        let _env = Env::set(&[
            ("AWS_SHARED_CREDENTIALS_FILE", file.as_str()),
            ("AWS_CONFIG_FILE", no_config.as_str()),
            ("AWS_PROFILE", "lwf"),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ])
        .await;
        let c = Provider::new(None, "eu-west-1").get().await.unwrap();
        assert_eq!(access_key(&c), "AKIDPROF");
        assert_eq!(c.secret_access_key(), "profsecret");
    }
    {
        let _env = Env::set(&[
            ("AWS_SHARED_CREDENTIALS_FILE", file.as_str()),
            ("AWS_CONFIG_FILE", no_config.as_str()),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ])
        .await;
        let c = Provider::new(None, "eu-west-1").get().await.unwrap();
        assert_eq!(access_key(&c), "AKIDDEF", "the default profile");
    }
}

#[tokio::test]
async fn container_credentials() {
    let base = stub(vec![(
            "GET",
            "/creds",
            r#"{"AccessKeyId":"AKIDCTR","SecretAccessKey":"ctrsecret","Token":"ctrtoken","Expiration":"2030-01-01T00:00:00Z"}"#,
        )])
        .await;
    let _env = Env::set(&[
        (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            format!("{base}/creds").as_str(),
        ),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ])
    .await;
    let c = Provider::new(None, "eu-west-1").get().await.unwrap();
    assert_eq!(access_key(&c), "AKIDCTR");
    assert_eq!(c.secret_access_key(), "ctrsecret");
    assert_eq!(c.session_token(), Some("ctrtoken"));
}

#[tokio::test]
async fn instance_metadata_credentials() {
    let base = stub(vec![
            ("PUT", "/latest/api/token", "imds-token"),
            ("GET", "/latest/meta-data/iam/security-credentials/", "lwf-role"),
            (
                "GET",
                "/latest/meta-data/iam/security-credentials/lwf-role",
                r#"{"AccessKeyId":"AKIDIMDS","SecretAccessKey":"imdssecret","Token":"imdstoken","Expiration":"2030-01-01T00:00:00Z"}"#,
            ),
        ])
        .await;
    let _env = Env::set(&[("AWS_EC2_METADATA_SERVICE_ENDPOINT", base.as_str())]).await;
    let c = Provider::new(None, "eu-west-1").get().await.unwrap();
    assert_eq!(access_key(&c), "AKIDIMDS");
    assert_eq!(c.session_token(), Some("imdstoken"));
}

#[tokio::test]
async fn web_identity_credentials_over_the_passed_client() {
    let base = stub(vec![(
            "*",
            "/",
            "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>ASIAWEBID</AccessKeyId><SecretAccessKey>webidsecret</SecretAccessKey><SessionToken>webidtoken</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>",
        )])
        .await;
    let dir = scratch("web-identity");
    let token = dir.join("token");
    std::fs::write(&token, "oidc-token").unwrap();
    let token = token.display().to_string();
    let _env = Env::set(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", token.as_str()),
        ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/lwf-test"),
        ("AWS_ENDPOINT_URL_STS", base.as_str()),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ])
    .await;
    let c = Provider::new(None, "eu-west-1").get().await.unwrap();
    assert_eq!(access_key(&c), "ASIAWEBID");
    assert_eq!(c.session_token(), Some("webidtoken"));
}

#[tokio::test]
async fn no_source_is_an_error() {
    let _env = Env::set(&[("AWS_EC2_METADATA_DISABLED", "true")]).await;
    assert!(Provider::new(None, "eu-west-1").get().await.is_err());
}

#[tokio::test]
async fn missing_credentials_message() {
    let _env = Env::set(&[("AWS_EC2_METADATA_DISABLED", "true")]).await;
    let err = Provider::new(None, "eu-west-1").get().await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.starts_with("no AWS credentials: "), "{msg}");
    assert!(msg.contains("access_key_ref / secret_key_ref"), "{msg}");
    assert!(
        msg.contains("(tried: environment, shared credentials and config files, web identity"),
        "{msg}"
    );
    assert!(
        msg.contains("EC2 instance metadata unless AWS_EC2_METADATA_DISABLED=true): "),
        "{msg}"
    );
}

#[tokio::test]
async fn references_are_read_on_every_call() {
    let dir = scratch("refs");
    let key_file = dir.join("secret-key");
    std::fs::write(&key_file, "first-secret\n").unwrap();
    let key_file = key_file.display().to_string();
    let dir_str = dir.display().to_string();
    let _env = Env::set(&[
        ("LWF_AWS_TEST_AK", "AKIDREF"),
        ("LAZYWF_SECRET_DIRS", dir_str.as_str()),
        // Would be used if the references were ignored.
        ("AWS_ACCESS_KEY_ID", "AKIDCHAIN"),
        ("AWS_SECRET_ACCESS_KEY", "chainsecret"),
    ])
    .await;
    let p = Provider::new(
        Some(Refs {
            access_key: "env:LWF_AWS_TEST_AK".into(),
            secret_key: format!("file:{key_file}"),
            session_token: None,
        }),
        "eu-west-1",
    );
    let first = p.get().await.unwrap();
    assert_eq!(access_key(&first), "AKIDREF");
    assert_eq!(first.secret_access_key(), "first-secret");

    std::fs::write(&key_file, "second-secret\n").unwrap();
    let second = p.get().await.unwrap();
    assert_eq!(
        second.secret_access_key(),
        "second-secret",
        "rotated file is read"
    );

    std::env::set_var("LWF_AWS_TEST_AK", "AKIDNEW");
    assert_eq!(access_key(&p.get().await.unwrap()), "AKIDNEW");
    std::env::remove_var("LWF_AWS_TEST_AK");
}

#[tokio::test]
async fn a_literal_where_a_reference_belongs_is_an_error() {
    let _env = Env::set(&[]).await;
    let p = Provider::new(
        Some(Refs {
            access_key: "AKIDLITERAL".into(),
            secret_key: "env:LWF_AWS_TEST_SK".into(),
            session_token: None,
        }),
        "eu-west-1",
    );
    let err = p.get().await.unwrap_err().to_string();
    assert!(err.contains("must be a reference"), "{err}");
}
