use super::*;

use rcgen::{date_time_ymd, BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{CertificateError, ServerConfig};

/// A test certificate authority, generated at run time: nothing is written to disk.
struct Ca {
    params: CertificateParams,
    key: KeyPair,
    cert: rcgen::Certificate,
}

fn new_ca(name: &str) -> Ca {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Ca { params, key, cert }
}

/// A leaf certificate for `names`, signed by `ca`; `expired` makes it valid in 2001 only.
fn leaf(names: &[&str], ca: &Ca, expired: bool) -> rcgen::Certificate {
    let mut params =
        CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
    if expired {
        params.not_before = date_time_ymd(2001, 1, 1);
        params.not_after = date_time_ymd(2002, 1, 1);
    }
    let key = KeyPair::generate().unwrap();
    let issuer = Issuer::from_params(&ca.params, &ca.key);
    params.signed_by(&key, &issuer).unwrap()
}

/// A certificate for `names` that signs itself (not a CA).
fn self_signed(names: &[&str]) -> rcgen::Certificate {
    let params =
        CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
    params.self_signed(&KeyPair::generate().unwrap()).unwrap()
}

/// The `verify-full` verifier: the chain must lead to one of `trusted`, and the name matches.
fn webpki(trusted: &[&Ca]) -> Arc<WebPkiServerVerifier> {
    let mut store = RootCertStore::empty();
    for ca in trusted {
        store.add(ca.cert.der().clone()).unwrap();
    }
    WebPkiServerVerifier::builder_with_provider(Arc::new(store), provider())
        .build()
        .unwrap()
}

fn check(
    v: &impl ServerCertVerifier,
    cert: &rcgen::Certificate,
    host: &str,
) -> std::result::Result<ServerCertVerified, rustls::Error> {
    let name = ServerName::try_from(host).unwrap();
    v.verify_server_cert(cert.der(), &[], &name, &[], UnixTime::now())
}

fn rejection(r: std::result::Result<ServerCertVerified, rustls::Error>) -> CertificateError {
    match r {
        Err(rustls::Error::InvalidCertificate(e)) => e,
        other => panic!("expected a certificate rejection, got {other:?}"),
    }
}

#[test]
fn verify_full_accepts_a_chain_to_a_trusted_root_for_its_name() {
    let ca = new_ca("Test Root A");
    let cert = leaf(&["db.example.test"], &ca, false);
    assert!(check(&*webpki(&[&ca]), &cert, "db.example.test").is_ok());
}

#[test]
fn verify_full_rejects_a_wrong_host_name() {
    let ca = new_ca("Test Root A");
    let cert = leaf(&["other.example.test"], &ca, false);
    let e = rejection(check(&*webpki(&[&ca]), &cert, "db.example.test"));
    assert!(
        matches!(
            e,
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
        ),
        "{e:?}"
    );
}

#[test]
fn verify_full_rejects_an_unknown_ca() {
    let trusted = new_ca("Test Root A");
    let other = new_ca("Test Root B");
    let cert = leaf(&["db.example.test"], &other, false);
    let e = rejection(check(&*webpki(&[&trusted]), &cert, "db.example.test"));
    assert_eq!(e, CertificateError::UnknownIssuer);
}

#[test]
fn verify_full_rejects_a_self_signed_certificate() {
    let trusted = new_ca("Test Root A");
    let cert = self_signed(&["db.example.test"]);
    let e = rejection(check(&*webpki(&[&trusted]), &cert, "db.example.test"));
    assert_eq!(e, CertificateError::UnknownIssuer);
}

#[test]
fn verify_full_rejects_an_expired_certificate() {
    let ca = new_ca("Test Root A");
    let cert = leaf(&["db.example.test"], &ca, true);
    let e = rejection(check(&*webpki(&[&ca]), &cert, "db.example.test"));
    assert!(
        matches!(
            e,
            CertificateError::Expired | CertificateError::ExpiredContext { .. }
        ),
        "{e:?}"
    );
}

#[test]
fn verify_ca_checks_the_chain_but_not_the_name() {
    let ca = new_ca("Test Root A");
    let v = AnyName {
        inner: webpki(&[&ca]),
    };
    // Right chain, wrong name: accepted.
    let cert = leaf(&["other.example.test"], &ca, false);
    assert!(check(&v, &cert, "db.example.test").is_ok());
    // Right name, right chain: accepted.
    let cert = leaf(&["db.example.test"], &ca, false);
    assert!(check(&v, &cert, "db.example.test").is_ok());
    // The chain is still checked.
    let other = new_ca("Test Root B");
    let cert = leaf(&["db.example.test"], &other, false);
    assert_eq!(
        rejection(check(&v, &cert, "db.example.test")),
        CertificateError::UnknownIssuer
    );
    let cert = self_signed(&["db.example.test"]);
    assert_eq!(
        rejection(check(&v, &cert, "db.example.test")),
        CertificateError::UnknownIssuer
    );
    let cert = leaf(&["db.example.test"], &ca, true);
    assert!(matches!(
        rejection(check(&v, &cert, "db.example.test")),
        CertificateError::Expired | CertificateError::ExpiredContext { .. }
    ));
}

#[test]
fn required_accepts_anything() {
    // `required` encrypts without checks: even a self-signed certificate for another name.
    let v = NoCheck {
        provider: provider(),
    };
    let cert = self_signed(&["other.example.test"]);
    assert!(check(&v, &cert, "db.example.test").is_ok());
}

/// Condition 1 of the `rabbitmq-stream` dependency (owner, 2026-11-11): with **both** crypto
/// libraries compiled in — `rabbitmq-stream-client`'s tokio-rustls brings aws-lc-rs, which
/// cannot be turned off from here — the provider the process runs on is still lazywf's ring,
/// and an existing kind's TLS connection still works. `ClientConfig::builder()` is exactly
/// the call a dependency makes when it takes the process default (the stream client's own
/// config does, `client/options.rs`): with two providers compiled in it panics unless one is
/// installed as the default, so this asserts the installed default is ring, that the
/// dependency-style builder takes ring, and that a `Connector` handshake — the path the
/// http, amqp, smtp, redis … kinds take — completes against a server of the test's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_existing_kind_handshakes_on_ring_with_aws_lc_in_the_tree() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    crate::gcp::install_crypto_provider();
    let installed = CryptoProvider::get_default().expect("a process default provider");
    let ring = rustls::crypto::ring::default_provider();
    // The provider of the process is ring's, not the aws-lc one that is compiled in too:
    // the two carry a different randomness source, which is what their `Debug` prints.
    assert_eq!(
        format!("{:?}", installed.secure_random),
        format!("{:?}", ring.secure_random),
        "the process default provider must be ring, not the aws-lc one in the tree"
    );
    // The dependency-style builder — what the stream client calls (`ClientConfig::builder()`,
    // no provider passed, `client/options.rs`): with two providers compiled in that panics
    // unless one is installed as the process default, and it takes that default. Building it
    // is the check; the handshake below runs on it through the kind's own `Connector`.
    let dep_style = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    assert!(dep_style.enable_sni, "the built configuration is usable");

    // A server of the test's own (`required` verifies nothing, so the CA needs no file).
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .distinguished_name
        .push(DnType::CommonName, "test leaf");
    let cert = params.self_signed(&key).unwrap();
    let server = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert.der().to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server));
    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(tcp).await.expect("the server side");
        let mut buf = [0u8; 8];
        let n = tls.read(&mut buf).await.expect("a read");
        assert_eq!(&buf[..n], b"ping", "what the client sent");
        tls.write_all(b"pong").await.expect("a write");
        tls.flush().await.expect("a flush");
    });

    // The client connects to the address the test bound, not to "localhost": the name can
    // resolve to `::1` first and reach another test's IPv6 listener on the same port number
    // (the flake under `make coverage`). The TLS server name stays "localhost", the name the
    // certificate above is issued for (`spec.server_name` overrides the connect address for it).
    let mut spec = TlsSpec::parse(Some("required")).unwrap();
    spec.server_name = Some("localhost".to_string());
    let host = addr.ip().to_string();
    let mut stream = Connector::new(&host, addr.port(), &spec, true, &[])
        .unwrap()
        .connect(Duration::from_secs(10))
        .await
        .expect("a TLS handshake on the installed provider");
    stream.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
    server_task.await.unwrap();
}

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
