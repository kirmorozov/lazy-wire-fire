//! TLS for targets, and the connection type shared by the HTTP and Redis clients.
//!
//! A target's `tls` column is empty (http: TLS for `https://` endpoints, verified; redis: TLS
//! for `rediss://` endpoints), a mode, or a JSON object:
//!
//! * `disabled` (`off`): plain TCP;
//! * `required`: TLS without certificate checks;
//! * `verify-ca`: the chain must lead to a trusted root, any host name;
//! * `verify-full` (`on`): chain and host name;
//! * `{"mode": "verify-full", "ca": "/etc/ssl/private-ca.pem", "server_name": "es.internal"}`:
//!   extra CA certificates (PEM, trusted next to the Mozilla roots) and the name to check.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Decided by the endpoint scheme (`https://`, `rediss://`), verified.
    Auto,
    Disabled,
    Required,
    VerifyCa,
    VerifyFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSpec {
    pub mode: Mode,
    pub ca: Vec<String>,
    pub server_name: Option<String>,
}

impl TlsSpec {
    pub fn parse(s: Option<&str>) -> Result<TlsSpec> {
        let s = s.unwrap_or("").trim();
        let mode = |m: &str| -> Result<Mode> {
            Ok(match m.to_ascii_lowercase().as_str() {
                "" | "auto" => Mode::Auto,
                "disabled" | "off" | "false" | "0" => Mode::Disabled,
                "required" | "insecure" => Mode::Required,
                "verify-ca" | "verify_ca" => Mode::VerifyCa,
                "verify-full" | "verify_full" | "verify" | "on" | "true" | "1" => Mode::VerifyFull,
                other => {
                    bail!("tls mode `{other}` (use disabled, required, verify-ca or verify-full)")
                }
            })
        };
        if !s.starts_with('{') {
            return Ok(TlsSpec {
                mode: mode(s)?,
                ca: Vec::new(),
                server_name: None,
            });
        }
        let v: Value = serde_json::from_str(s).context("tls is not valid JSON")?;
        let m = v.get("mode").and_then(Value::as_str).unwrap_or("");
        let ca = match v.get("ca") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::String(c)) => vec![c.clone()],
            Some(Value::Array(a)) => a
                .iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
            Some(_) => bail!("tls.ca must be a path or a list of paths"),
        };
        let ca = ca
            .into_iter()
            .map(|c| c.strip_prefix("file:").map(str::to_string).unwrap_or(c))
            .collect::<Vec<_>>();
        let mut mode = mode(m)?;
        if mode == Mode::Auto && !ca.is_empty() {
            mode = Mode::VerifyFull;
        }
        Ok(TlsSpec {
            mode,
            ca,
            server_name: v
                .get("server_name")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    /// The effective mode for an endpoint whose scheme asks for TLS (`secure`) or not.
    pub fn effective(&self, secure: bool) -> Mode {
        match self.mode {
            Mode::Auto if secure => Mode::VerifyFull,
            Mode::Auto => Mode::Disabled,
            m => m,
        }
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn roots(ca: &[String]) -> Result<RootCertStore> {
    let mut store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for path in ca {
        let mut n = 0;
        for cert in CertificateDer::pem_file_iter(path)
            .with_context(|| format!("reading CA file `{path}`"))?
        {
            let cert = cert.with_context(|| format!("parsing CA file `{path}`"))?;
            store
                .add(cert)
                .with_context(|| format!("adding a CA certificate from `{path}`"))?;
            n += 1;
        }
        if n == 0 {
            bail!("CA file `{path}` holds no certificate");
        }
    }
    Ok(store)
}

/// A client configuration for `mode` (not `Disabled`/`Auto`), with ALPN `alpn`.
pub fn client_config(spec: &TlsSpec, mode: Mode, alpn: &[&[u8]]) -> Result<Arc<ClientConfig>> {
    let p = provider();
    let builder = ClientConfig::builder_with_provider(p.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow!("TLS setup: {e}"))?;
    let store = Arc::new(roots(&spec.ca)?);
    let mut cfg = match mode {
        Mode::VerifyFull | Mode::Auto => {
            builder.with_root_certificates(store).with_no_client_auth()
        }
        Mode::VerifyCa => {
            let inner = WebPkiServerVerifier::builder_with_provider(store, p.clone())
                .build()
                .map_err(|e| anyhow!("TLS setup: {e}"))?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AnyName { inner }))
                .with_no_client_auth()
        }
        Mode::Required | Mode::Disabled => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCheck { provider: p }))
            .with_no_client_auth(),
    };
    cfg.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
    Ok(Arc::new(cfg))
}

/// `verify-ca`: the chain is checked, the name is not.
#[derive(Debug)]
struct AnyName {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for AnyName {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// `required`: encrypted, nothing checked.
#[derive(Debug)]
struct NoCheck {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for NoCheck {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A connected socket, plain or TLS.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

/// How to reach one host.
#[derive(Clone)]
pub struct Connector {
    pub host: String,
    pub port: u16,
    pub tls: Option<(Arc<ClientConfig>, ServerName<'static>)>,
}

impl Connector {
    pub fn new(
        host: &str,
        port: u16,
        spec: &TlsSpec,
        secure_scheme: bool,
        alpn: &[&[u8]],
    ) -> Result<Connector> {
        let mode = spec.effective(secure_scheme);
        let tls = if mode == Mode::Disabled {
            None
        } else {
            let cfg = client_config(spec, mode, alpn)?;
            let name = spec.server_name.clone().unwrap_or_else(|| host.to_string());
            let name = ServerName::try_from(name.clone())
                .map_err(|_| anyhow!("`{name}` is not a valid TLS server name"))?;
            Some((cfg, name))
        };
        Ok(Connector {
            host: host.to_string(),
            port,
            tls,
        })
    }

    pub async fn connect(&self, timeout: Duration) -> Result<Stream> {
        let addr = (self.host.as_str(), self.port);
        let tcp = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                anyhow!(
                    "connecting to {}:{} timed out after {timeout:?}",
                    self.host,
                    self.port
                )
            })?
            .with_context(|| format!("connecting to {}:{}", self.host, self.port))?;
        tcp.set_nodelay(true).ok();
        let Some((cfg, name)) = &self.tls else {
            return Ok(Stream::Plain(tcp));
        };
        let tls = tokio::time::timeout(
            timeout,
            TlsConnector::from(cfg.clone()).connect(name.clone(), tcp),
        )
        .await
        .map_err(|_| anyhow!("TLS handshake with {}:{} timed out", self.host, self.port))?
        .with_context(|| format!("TLS handshake with {}:{}", self.host, self.port))?;
        Ok(Stream::Tls(Box::new(tls)))
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specs() {
        assert_eq!(TlsSpec::parse(None).unwrap().mode, Mode::Auto);
        assert_eq!(
            TlsSpec::parse(Some("verify-ca")).unwrap().mode,
            Mode::VerifyCa
        );
        assert_eq!(
            TlsSpec::parse(Some("off")).unwrap().effective(true),
            Mode::Disabled
        );
        assert_eq!(
            TlsSpec::parse(Some("")).unwrap().effective(true),
            Mode::VerifyFull
        );
        let j = TlsSpec::parse(Some(r#"{"ca": "file:/x.pem", "server_name": "es"}"#)).unwrap();
        assert_eq!(j.mode, Mode::VerifyFull);
        assert_eq!(j.ca, vec!["/x.pem".to_string()]);
        assert_eq!(j.server_name.as_deref(), Some("es"));
        assert!(TlsSpec::parse(Some("sometimes")).is_err());
        for m in [Mode::Required, Mode::VerifyCa, Mode::VerifyFull] {
            client_config(&TlsSpec::parse(None).unwrap(), m, &[b"http/1.1"]).unwrap();
        }
    }
}
