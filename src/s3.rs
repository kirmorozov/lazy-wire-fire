//! `http.s3` targets: addressing (virtual-hosted or path style) and AWS Signature Version 4.

use crate::aws;
use crate::http::BaseUrl;

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
            let prefix = format!("{}/{}", service.base, aws::uri_encode(&self.bucket, true));
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
        aws::endpoint("s3", &self.region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
