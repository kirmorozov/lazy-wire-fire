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

/// `Env::set` for one case of a test that only asks where the region and the credentials come
/// from: the case's own variables plus a credential pair and the metadata-off flag, so the case
/// can never leave the process. A credentials question asked while `ENV_LOCK` is held is not a
/// slow test, it is every env-dependent unit test on the box waiting behind it.
async fn env_with_creds(vars: &[(&str, &str)]) -> Env {
    const CREDENTIALS: [(&str, &str); 3] = [
        ("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE"),
        (
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];
    let mut all = vars.to_vec();
    all.extend_from_slice(&CREDENTIALS);
    Env::set(&all).await
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

// ——— the cache in front of the chain, and the four places a region comes from ————————————

fn creds(key: &str, expiry: Option<std::time::SystemTime>) -> Credentials {
    Credentials::new(key, "secret", None, expiry, "test")
}

#[tokio::test]
async fn a_credential_still_good_is_handed_back_without_the_chain() {
    let _env = Env::set(&[("AWS_EC2_METADATA_DISABLED", "true")]).await;
    let now = std::time::SystemTime::now();
    let secs = |ahead: u64| Some(now + std::time::Duration::from_secs(ahead));

    // A permanent credential: no expiry to run out, so the cache is the answer and the
    // chain is never built (which is the point — a target that sends continuously must not
    // ask the chain, or the instance metadata service, on every request).
    let p = Provider::new(None, "eu-west-1");
    *p.cache.lock().await = Some(creds("AKIDCACHED", None));
    assert_eq!(access_key(&p.get().await.unwrap()), "AKIDCACHED");
    assert!(p.chain.get().is_none(), "the chain was never asked");

    // Temporary credentials with their whole life ahead: the same.
    let p = Provider::new(None, "eu-west-1");
    *p.cache.lock().await = Some(creds("AKIDLONG", secs(3600)));
    assert_eq!(access_key(&p.get().await.unwrap()), "AKIDLONG");
    assert!(p.chain.get().is_none(), "the chain was never asked");

    // A credential due within `REFRESH_BEFORE` is not the answer any more: the cache is
    // walked past and the chain asked, so a token is never handed out at the moment it dies.
    let p = Provider::new(None, "eu-west-1");
    *p.cache.lock().await = Some(creds("AKIDDYING", secs(60)));
    let e = p.get().await.unwrap_err();
    assert!(
        format!("{e:#}").contains("no AWS credentials"),
        "the chain was asked, and with no source it says so: {e:#}"
    );
    assert!(p.chain.get().is_some(), "the chain was built");
}

#[tokio::test]
async fn the_region_of_an_endpoint_is_the_url_then_the_environment_then_the_config_file() {
    // The order the region is looked for in, one setting at a time. Each of the four is the
    // only one that can answer in its own case, so a deployment that sets two of them is
    // not guessing which wins. One `Env` per case, never a second inside the first: `Env`
    // holds `ENV_LOCK`, which is not reentrant, so a nested one waits forever on the guard
    // the same test is still holding — and every other env-dependent unit test waits too.
    let dir = scratch("region");
    let config = dir.join("config");
    std::fs::write(&config, "[default]\nregion = sa-east-1\n").unwrap();

    // The endpoint's own region beats every setting, and `AWS_REGION` beats the two below it.
    {
        let _env = env_with_creds(&[
            ("AWS_REGION", "eu-north-1"),
            ("AWS_DEFAULT_REGION", "ap-south-1"),
            ("AWS_CONFIG_FILE", config.display().to_string().as_str()),
        ])
        .await;
        assert_eq!(resolve_region(Some("eu-west-1")).unwrap(), "eu-west-1");
        assert_eq!(resolve_region(None).unwrap(), "eu-north-1");
    }
    // Then `AWS_DEFAULT_REGION`.
    {
        let _env = env_with_creds(&[("AWS_DEFAULT_REGION", "ap-south-1")]).await;
        assert_eq!(resolve_region(None).unwrap(), "ap-south-1");
    }
    // Then the named profile of the shared config file.
    {
        std::fs::write(
            &config,
            "[default]\nregion = sa-east-1\n\n[profile lwf]\nregion = eu-central-1\n",
        )
        .unwrap();
        let _env = env_with_creds(&[
            ("AWS_CONFIG_FILE", config.display().to_string().as_str()),
            ("AWS_PROFILE", "lwf"),
        ])
        .await;
        assert_eq!(resolve_region(None).unwrap(), "eu-central-1");
    }
    // And `[default]` when no profile is named.
    {
        let _env =
            env_with_creds(&[("AWS_CONFIG_FILE", config.display().to_string().as_str())]).await;
        assert_eq!(resolve_region(None).unwrap(), "sa-east-1");
    }
    // With no `AWS_CONFIG_FILE` to read, the file under `HOME` is the last place asked — and a
    // comment and a blank line and another profile's region do not answer for it.
    {
        let _env = env_with_creds(&[]).await;
        let home = scratch("region-home");
        std::fs::create_dir_all(home.join(".aws")).unwrap();
        std::fs::write(
            home.join(".aws/config"),
            "# the shared config\n[profile other]\nregion = us-east-2\n\n[default]\nregion = us-west-1\n",
        )
        .unwrap();
        std::env::set_var("HOME", &home);
        assert_eq!(resolve_region(None).unwrap(), "us-west-1");
    }
    // Nothing at all: the refusal is the instruction, and it names both places to put it.
    {
        let _env = env_with_creds(&[]).await;
        std::env::set_var("HOME", scratch("region-none"));
        let e = format!("{:#}", resolve_region(None).unwrap_err());
        assert!(e.contains("no AWS region"), "{e}");
        assert!(e.contains("aws-sqs://<region>"), "{e}");
        assert!(e.contains("AWS_DEFAULT_REGION"), "{e}");
    }
}

#[tokio::test]
async fn a_config_line_that_is_not_a_region_is_not_one() {
    // The same file with nothing to give: a profile that exists but says nothing, a region
    // with an empty value, and a commented-out one all answer `not here`, which is what
    // lets the next place be asked instead of a target failing on a blank region.
    let text = "# region = eu-west-1\n[default]\naws_secret_access_key = x\nregion =\n[profile other]\nregion = us-east-2\n";
    assert_eq!(config_region(text, "default"), None);
    assert_eq!(config_region(text, "other"), Some("us-east-2".into()));
    // A name is matched as a name: `[profile lwf]` is the profile `lwf`, and `[lwf]` is not.
    assert_eq!(config_region("[lwf]\nregion = us-east-2\n", "lwf"), None);
    assert_eq!(
        config_region("[profile lwf]\nregion = us-east-2\n", "lwf"),
        Some("us-east-2".into())
    );
}

/// The one spelling of the default profile our parser does not read, kept in the tree as the
/// acceptance criterion for the fix rather than dropped with the PR. See the ignore reason.
#[test]
#[ignore = "product bug (PR #191): config_region at src/aws.rs:521 accepts only [default] for the default profile, while the aws-config we depend on reads [profile default] and lets it override [default] (aws-runtime-1.10.4/src/env_config/normalize.rs:98 strips the profile prefix, :100 and :126-136 give the prefixed one priority)"]
fn the_bracketed_profile_default_spelling_of_the_default_profile_is_read_too() {
    // `aws-config` — in our tree as `aws_runtime::env_config`, re-exported by
    // `aws-config-1.13.2/src/profile.rs:24` — normalizes a config file's section names by
    // stripping the prefix (`aws-runtime-1.10.4/src/env_config/normalize.rs:98`: "When the
    // profile is a config file, strip `profile` and trim whitespace (`profile foo` => `foo`)"),
    // and `default` is no exception: :100 says "A profile named `profile default` takes priority
    // over a profile named `default`", and :126-136 drops `[default]` with
    // `profile `[default]` ignored because `[profile default]` was found which takes priority`.
    // Ours takes the other branch for this one name — src/aws.rs:521-526 is
    // `if profile == "default" { "default" } else { format!("profile {profile}") }`, so the
    // prefix that is required for every other profile is forbidden for this one. The consequence
    // is a user-visible start failure: a shared config whose only `region` sits under
    // `[profile default]`, which the user's other AWS tooling reads, answers `not here` here, and
    // `resolve_region` refuses with "no AWS region: give it in the endpoint". A dependency update
    // cannot fix that: `config_region` is a hand-written parser and nothing asks `aws-runtime`.
    // Un-ignore this when the parser accepts the form; the expectation is the AWS one, not ours.
    assert_eq!(
        config_region("[profile default]\nregion = us-east-2\n", "default"),
        Some("us-east-2".into()),
        "`[profile default]` is the profile `default` to aws-config, so it has to be one to us too"
    );
}

fn pool() -> Pool {
    Pool::new("mysql://u:p@127.0.0.1:1/x")
}

#[tokio::test]
async fn an_entry_the_answer_numbers_wrong_fails_the_whole_batch() {
    // The `Id` of an entry is how the answer says which key it talks about. An answer that
    // numbers it outside the batch, or not at all, cannot be mapped onto keys at all: the
    // one honest answer is that nothing was answered, so no key is dropped on it.
    let expect = Expect::new(None).unwrap();
    let pool = pool();
    let items = |answer: Value, n: usize| {
        let expect = &expect;
        let pool = &pool;
        async move {
            batch_items(
                &answer,
                n,
                expect,
                pool,
                200,
                &[],
                "https://sqs.eu-west-1.amazonaws.com/q",
            )
            .await
        }
    };

    // A number where a string of a number belongs: the same index, read either way.
    let got = items(
        serde_json::json!({"Successful": [{"Id": 1, "MessageId": "m"}]}),
        2,
    )
    .await
    .unwrap();
    assert!(
        matches!(got[1], Item::Ok),
        "the numeric `Id` named the second item: {got:?}"
    );
    assert!(
        matches!(&got[0], Item::Transient(why) if why.contains("no answer")),
        "the item the answer never named waits: {got:?}"
    );
    // An `Id` that is neither.
    let e = format!(
        "{:#}",
        items(serde_json::json!({"Failed": [{"Id": true}]}), 2)
            .await
            .unwrap_err()
    );
    assert!(e.contains("not one of 2"), "{e}");
    // And one that is a number the batch does not have.
    let e = format!(
        "{:#}",
        items(
            serde_json::json!({"Failed": [{"Id": "7", "Code": "X", "Message": "y"}]}),
            2
        )
        .await
        .unwrap_err()
    );
    assert!(e.contains("the answer names item"), "{e}");
    assert!(e.contains("not one of 2"), "{e}");
    // The same in the section that answers well: a batch is read whole or not at all, so a
    // good `Successful` entry cannot be believed past a `Failed` one that names nothing.
    let e = format!(
        "{:#}",
        items(
            serde_json::json!({
                "Successful": [{"Id": "0", "MessageId": "m"}],
                "Failed": [{"Id": "x", "Code": "X", "Message": "y"}],
            }),
            2,
        )
        .await
        .unwrap_err()
    );
    assert!(e.contains("not one of 2"), "{e}");
}
