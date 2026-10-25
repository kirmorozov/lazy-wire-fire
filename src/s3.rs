//! `http.s3` targets: addressing (virtual-hosted or path style) and AWS Signature Version 4.

use anyhow::{bail, Result};
use sha2::{Digest, Sha256};

use crate::aws::Credentials;
use crate::http::{BaseUrl, RawRequest};

/// Bucket addressing from a target's `vars`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Spec {
    pub bucket: String,
    pub region: String,
    pub path_style: bool,
}

impl S3Spec {
    /// The base URL to connect to and the path prefix for requests, given the service
    /// endpoint (`vars.endpoint`, an entry of `endpoints`, or AWS's regional one).
    pub fn address(&self, service: &BaseUrl) -> (BaseUrl, String) {
        if self.path_style {
            let prefix = format!("{}/{}", service.base, uri_encode(&self.bucket, true));
            (
                BaseUrl {
                    base: String::new(),
                    ..service.clone()
                },
                prefix,
            )
        } else {
            let host = format!("{}.{}", self.bucket, service.host);
            (
                BaseUrl {
                    host,
                    base: String::new(),
                    ..service.clone()
                },
                service.base.clone(),
            )
        }
    }

    pub fn aws_endpoint(&self) -> BaseUrl {
        BaseUrl {
            secure: true,
            host: format!("s3.{}.amazonaws.com", self.region),
            port: 443,
            base: String::new(),
        }
    }
}

/// SigV4 URI encoding: unreserved characters stay, `/` stays unless `encode_slash`.
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'.' | b'_' | b'~')
            || (b == b'/' && !encode_slash)
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Path and query in canonical (and wire) form: each part decoded and re-encoded, the query
/// sorted.
fn canonical_target(target: &str) -> (String, String) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = path
        .split('/')
        .map(|seg| uri_encode(&percent_decode_path(seg), true))
        .collect::<Vec<_>>()
        .join("/");
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (
                uri_encode(&percent_decode(k), true),
                uri_encode(&percent_decode(v), true),
            )
        })
        .collect();
    pairs.sort();
    let query = pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    (if path.is_empty() { "/".into() } else { path }, query)
}

/// Like `percent_decode`, but `+` is a literal plus in a path.
fn percent_decode_path(s: &str) -> String {
    percent_decode(&s.replace('+', "%2B"))
}

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    inner.update(msg);
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// Headers that are signed when present, besides `host` and every `x-amz-*`.
const SIGNED: &[&str] = &["content-type", "content-md5", "range", "date"];

/// Signs `req` for `service` in `region`: rewrites the target in canonical form (prefixed by
/// `prefix`), and sets `Host`, `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token`
/// (with a session token) and `Authorization`.
pub fn sign(
    req: &mut RawRequest,
    host_header: &str,
    prefix: &str,
    region: &str,
    service: &str,
    creds: &Credentials,
    unix_now: u64,
) -> Result<()> {
    if creds.access_key.is_empty() || creds.secret_key.is_empty() {
        bail!("empty AWS credentials");
    }
    let (date, stamp) = crate::time::amz_dates(unix_now);
    let (path, query) = canonical_target(&format!("{prefix}{}", req.target));
    req.target = if query.is_empty() {
        path.clone()
    } else {
        format!("{path}?{query}")
    };
    let payload = hex::encode(Sha256::digest(&req.body));
    req.set_header("Host", host_header.to_string());
    req.set_header("x-amz-date", stamp.clone());
    req.set_header("x-amz-content-sha256", payload.clone());
    match &creds.session_token {
        Some(t) => req.set_header("x-amz-security-token", t.clone()),
        None => req.remove_header("x-amz-security-token"),
    }
    req.remove_header("authorization");
    let mut signed: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(n, v)| {
            (
                n.to_ascii_lowercase(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .filter(|(n, _)| n == "host" || n.starts_with("x-amz-") || SIGNED.contains(&n.as_str()))
        .collect();
    signed.sort();
    let canonical_headers: String = signed.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let signed_names = signed
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{signed_names}\n{payload}",
        req.method.to_ascii_uppercase()
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", creds.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, to_sign.as_bytes()));
    req.set_header(
        "Authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, Signature={signature}",
            creds.access_key
        ),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> Credentials {
        Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
            expires: None,
        }
    }

    // Examples from the AWS S3 docs ("Signature Calculations for the Authorization Header:
    // Transferring Payload in a Single Chunk").
    #[test]
    fn aws_get_object_example() {
        let mut r = RawRequest::parse("GET /test.txt HTTP/1.1\nRange: bytes=0-9\n\n")
            .unwrap()
            .unwrap();
        sign(
            &mut r,
            "examplebucket.s3.amazonaws.com",
            "",
            "us-east-1",
            "s3",
            &creds(),
            1_369_353_600,
        )
        .unwrap();
        assert_eq!(
            r.header("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn aws_put_object_example() {
        let mut r = RawRequest::parse(
            "PUT /test$file.text HTTP/1.1\nDate: Fri, 24 May 2013 00:00:00 GMT\nx-amz-storage-class: REDUCED_REDUNDANCY\n\nWelcome to Amazon S3.",
        )
        .unwrap()
        .unwrap();
        sign(
            &mut r,
            "examplebucket.s3.amazonaws.com",
            "",
            "us-east-1",
            "s3",
            &creds(),
            1_369_353_600,
        )
        .unwrap();
        assert_eq!(r.target, "/test%24file.text");
        assert!(r.header("authorization").unwrap().ends_with(
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, \
             Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        ));
    }

    #[test]
    fn aws_list_objects_example() {
        let mut r = RawRequest::parse("GET /?max-keys=2&prefix=J HTTP/1.1\n\n")
            .unwrap()
            .unwrap();
        sign(
            &mut r,
            "examplebucket.s3.amazonaws.com",
            "",
            "us-east-1",
            "s3",
            &creds(),
            1_369_353_600,
        )
        .unwrap();
        assert!(r.header("authorization").unwrap().ends_with(
            "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        ));
    }

    #[test]
    fn addressing() {
        let s = S3Spec {
            bucket: "b".into(),
            region: "eu-west-1".into(),
            path_style: false,
        };
        let (base, prefix) = s.address(&s.aws_endpoint());
        assert_eq!(
            (base.host.as_str(), prefix.as_str()),
            ("b.s3.eu-west-1.amazonaws.com", "")
        );
        let p = S3Spec {
            path_style: true,
            ..s
        };
        let (base, prefix) = p.address(&BaseUrl::parse("http://minio:9000").unwrap());
        assert_eq!(
            (base.host_header(), prefix.as_str()),
            ("minio:9000".to_string(), "/b")
        );
        assert_eq!(
            canonical_target("/a b/c%2Fd+e?z=1&a=x%20y&b"),
            ("/a%20b/c%2Fd%2Be".into(), "a=x%20y&b=&z=1".into())
        );
    }

    #[test]
    fn hmac_rfc4231_case2() {
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
