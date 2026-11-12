use super::*;

/// A connection whose peer sends `bytes` and then closes, over loopback.
async fn conn_with(bytes: &[u8]) -> Conn {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bytes = bytes.to_vec();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        s.write_all(&bytes).await.unwrap();
        let _ = s.shutdown().await;
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    Conn {
        stream: Stream::Plain(stream),
        buf: Vec::new(),
    }
}

#[tokio::test]
async fn chunked_body_joins_its_chunks() {
    let mut c = conn_with(b"4\r\nWiki\r\n5\r\npedia\r\n1\r\n!\r\n0\r\n\r\n").await;
    assert_eq!(read_chunked(&mut c).await.unwrap(), b"Wikipedia!");
}

#[tokio::test]
async fn chunked_body_skips_chunk_extensions() {
    let mut c = conn_with(b"4;name=val\r\nWiki\r\n5 ; x=\"y\"\r\npedia\r\n0;final\r\n\r\n").await;
    assert_eq!(read_chunked(&mut c).await.unwrap(), b"Wikipedia");
}

#[tokio::test]
async fn chunked_body_reads_and_drops_trailers() {
    let mut c = conn_with(b"4\r\nWiki\r\n0\r\nX-Checksum: abc\r\nX-Other: 1\r\n\r\n").await;
    assert_eq!(read_chunked(&mut c).await.unwrap(), b"Wiki");
}

#[tokio::test]
async fn chunked_empty_body() {
    let mut c = conn_with(b"0\r\n\r\n").await;
    assert_eq!(read_chunked(&mut c).await.unwrap(), b"");
}

#[tokio::test]
async fn chunked_malformed_size_is_an_error() {
    for bytes in [&b"zz\r\nabc\r\n0\r\n\r\n"[..], b"-1\r\n", b"\r\n"] {
        let mut c = conn_with(bytes).await;
        let err = read_chunked(&mut c).await.unwrap_err().to_string();
        assert!(err.starts_with("invalid chunk size `"), "{err}");
    }
}

#[tokio::test]
async fn chunked_size_over_the_body_limit_is_refused_before_reading_it() {
    // 0x10000001 bytes is one more than MAX_BODY (256 MiB); the size alone is enough.
    let mut c = conn_with(b"10000001\r\n").await;
    let err = read_chunked(&mut c).await.unwrap_err().to_string();
    assert_eq!(err, "chunked response body too large");
}

#[tokio::test]
async fn chunked_body_cut_short_is_an_error() {
    let mut c = conn_with(b"5\r\nab").await;
    let err = read_chunked(&mut c).await.unwrap_err().to_string();
    assert_eq!(err, "connection closed inside the response body");
}

#[test]
fn parse_requests() {
    let r = RawRequest::parse(
        "\n\nPOST /idx/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{\"a\":1}\n",
    )
    .unwrap()
    .unwrap();
    assert_eq!(r.method, "POST");
    assert_eq!(r.target, "/idx/_bulk");
    assert_eq!(r.header("content-type"), Some("application/x-ndjson"));
    assert_eq!(r.body, b"{\"a\":1}\n");

    let r = RawRequest::parse("PURGE / HTTP/1.1\r\nHost: v\r\nX-Cache-Tags-Pattern: .*\r\n")
        .unwrap()
        .unwrap();
    assert_eq!(r.headers.len(), 2);
    assert!(r.body.is_empty());
    let wire = String::from_utf8(r.to_bytes()).unwrap();
    assert_eq!(
        wire,
        "PURGE / HTTP/1.1\r\nHost: v\r\nX-Cache-Tags-Pattern: .*\r\n\r\n"
    );

    let r = RawRequest::parse("DELETE /p/1.json HTTP/1.1\n\n")
        .unwrap()
        .unwrap();
    assert!(r.headers.is_empty() && r.body.is_empty());
    let mut put = RawRequest::parse("PUT /x\nContent-Length: 99\n\nabc")
        .unwrap()
        .unwrap();
    put.set_header("Host", "h".into());
    assert_eq!(
        String::from_utf8(put.to_bytes()).unwrap(),
        "PUT /x HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc"
    );

    assert!(RawRequest::parse("  \n ").unwrap().is_none());
    assert!(RawRequest::parse("GET").is_err());
    assert!(RawRequest::parse("GET /a b HTTP/1.1").is_err());
    assert!(RawRequest::parse("GET x HTTP/1.1").is_err());
    assert!(RawRequest::parse("GET / HTTP/1.1\nbroken header\n").is_err());
    assert!(RawRequest::parse("GET / HTTP/1.1\nX-Tag: a\rX-Evil: 1\n").is_err());
    assert!(RawRequest::parse("GET /a\u{0}b HTTP/1.1\n").is_err());
    assert!(RawRequest::parse("GET / HTTP/1.1\nX-Tab: a\tb\n").is_ok());
}

#[test]
fn base_urls() {
    let b = BaseUrl::parse("https://es.example.com:9243/prefix/").unwrap();
    assert_eq!(
        (b.secure, b.host.as_str(), b.port, b.base.as_str()),
        (true, "es.example.com", 9243, "/prefix")
    );
    assert_eq!(b.host_header(), "es.example.com:9243");
    let b = BaseUrl::parse("varnish:6081").unwrap();
    assert_eq!((b.secure, b.port), (false, 6081));
    assert_eq!(
        BaseUrl::parse("http://[::1]").unwrap().host_header(),
        "[::1]"
    );
    assert_eq!(BaseUrl::parse("https://h").unwrap().url(), "https://h");
    assert!(BaseUrl::parse("ftp://h").is_err());
    assert!(BaseUrl::parse("http://u:p@h").is_err());
}
