use super::*;

/// The codes that say *this request* refuse; the codes that say *this moment* — and every
/// code not on either list — do not. A moment cannot cost a key.
#[test]
fn a_status_that_judges_the_request_refuses_and_one_that_is_a_moment_does_not() {
    for c in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::OutOfRange,
        Code::Unauthenticated,
    ] {
        let f = failure(c, "no");
        assert!(f.refuse, "{c:?} must refuse");
        assert!(f.why.contains(status_name(c)), "{c:?}: {}", f.why);
    }
    for c in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::Internal,
        Code::DataLoss,
        Code::Cancelled,
        Code::Unknown,
        Code::Ok,
    ] {
        assert!(!failure(c, "wait").refuse, "{c:?} must not refuse");
    }
    // A code no implementation is known to send is a moment too, never a verdict.
    assert!(!failure(Code::from_i32(77), "?").refuse);
    // The server's own words travel in the message.
    assert!(failure(Code::InvalidArgument, "bad sku")
        .why
        .contains("bad sku"));
    assert!(failure(Code::Unavailable, "").why.contains("UNAVAILABLE"));
}

/// The codec refusal is marked for the ladder to step down on; another `UNIMPLEMENTED`
/// (a method that is not there, say) is not — stepping down for it would take the target's
/// compression away over something compression has nothing to do with.
#[test]
fn only_a_codec_refusal_demotes() {
    assert!(
        failure(
            Code::Unimplemented,
            "Content is compressed with `zstd` which isn't supported"
        )
        .demote
    );
    assert!(failure(Code::Unimplemented, "Unsupported encoding: zstd").demote);
    assert!(!failure(Code::Unimplemented, "method not found").demote);
    assert!(!failure(Code::Internal, "compressed with zstd").demote);
}

/// The plain-HTTP answers of a proxy in front of the service, mapped: the statuses that
/// positively say *this method is not here* refuse, the ones that could be a moment do
/// not, and the message quotes who answered.
#[test]
fn an_http_answer_instead_of_grpc_is_judged_by_its_status() {
    let refuse = [
        (http::StatusCode::NOT_FOUND, Code::NotFound),
        (http::StatusCode::METHOD_NOT_ALLOWED, Code::NotFound),
        (http::StatusCode::GONE, Code::NotFound),
        (http::StatusCode::UNSUPPORTED_MEDIA_TYPE, Code::NotFound),
        (http::StatusCode::BAD_REQUEST, Code::InvalidArgument),
        (http::StatusCode::UNAUTHORIZED, Code::Unauthenticated),
        (http::StatusCode::FORBIDDEN, Code::PermissionDenied),
        (http::StatusCode::CONFLICT, Code::AlreadyExists),
    ];
    for (status, code) in refuse {
        let s = http_answer(status, "<html>no</html>");
        assert_eq!(s.code(), code, "{status}");
        let m = s.message();
        assert!(
            m.contains("not gRPC") && m.contains(&status.as_str().to_string()),
            "{status}: {m}"
        );
        assert!(m.contains("<html>no</html>"), "{status}: {m}");
    }
    // The statuses that could be a moment are Unavailable: retried forever, no strike.
    for status in [
        http::StatusCode::REQUEST_TIMEOUT,
        http::StatusCode::TOO_EARLY,
        http::StatusCode::TOO_MANY_REQUESTS,
        http::StatusCode::INTERNAL_SERVER_ERROR,
        http::StatusCode::BAD_GATEWAY,
        http::StatusCode::SERVICE_UNAVAILABLE,
        http::StatusCode::GATEWAY_TIMEOUT,
    ] {
        assert_eq!(
            http_answer(status, "").code(),
            Code::Unavailable,
            "{status}"
        );
    }
    // And a status nobody can explain is a moment too.
    assert_eq!(
        http_answer(http::StatusCode::IM_A_TEAPOT, "").code(),
        Code::Unknown
    );
    assert!(
        !failure(
            http_answer(http::StatusCode::SERVICE_UNAVAILABLE, "").code(),
            ""
        )
        .refuse
    );
    assert!(failure(http_answer(http::StatusCode::NOT_FOUND, "").code(), "").refuse);
}

/// A proxy that turns the call away at the door — a 401 for a credential it does not like,
/// a 403 for one it will not accept — is judged the way the same two codes are judged at
/// the gRPC level: the whole action is refused, so the owner sees a target whose credential
/// or ACL is wrong as strikes rather than as a target that is quietly always retrying. It is
/// the configuration's no, and a configuration fault has to reach an owner; only the
/// middlebox hiccups beside them are allowed to be a moment.
#[test]
fn a_proxy_that_refuses_the_credential_refuses_the_whole_action() {
    for status in [http::StatusCode::UNAUTHORIZED, http::StatusCode::FORBIDDEN] {
        let s = http_answer(status, "authorization failed");
        let f = failure(s.code(), s.message());
        assert!(
            f.refuse,
            "{status}: a peer that will not take the credential must refuse, not wait: {}",
            f.why
        );
        assert!(!f.demote, "{status}: this is not about a codec");
        assert!(
            f.why.contains(status.as_str()) && f.why.contains("authorization failed"),
            "{status}: the answer does not say who refused it: {}",
            f.why
        );
    }
    // The same proxy, overworked rather than unwilling: no verdict, no strike.
    for status in [
        http::StatusCode::TOO_MANY_REQUESTS,
        http::StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let s = http_answer(status, "");
        assert!(
            !failure(s.code(), s.message()).refuse,
            "{status}: a middlebox hiccup must not refuse"
        );
    }
}

/// A gzip message is a gzip member a gRPC server can read (its own gzip decoder reads it
/// back), a zstd message a zstd frame, and a message compression would not shrink is sent
/// as it is rather than flagged compressed.
#[test]
fn the_codecs_round_trip_and_small_messages_stay_as_they_are() {
    let body: Vec<u8> = br#"{"sku":"the same sku, repeated many times to be compressible: "}"#
        .iter()
        .cycle()
        .take(2048)
        .copied()
        .collect();
    for c in [Codec::Gzip, Codec::Zstd] {
        let (flag, packed) = c.pack(&body, 1024).unwrap();
        assert!(flag, "{c:?} must compress a 2 KiB repetitive body");
        assert!(packed.len() < body.len() / 2, "{c:?}: {}", packed.len());
        let back = match c {
            Codec::Gzip => {
                use std::io::Read as _;
                let mut d = flate2::read::GzDecoder::new(&packed[..]);
                let mut out = Vec::new();
                d.read_to_end(&mut out).unwrap();
                out
            }
            Codec::Zstd => zstd::decode_all(&packed[..]).unwrap(),
            Codec::Identity => unreachable!(),
        };
        assert_eq!(back, body, "{c:?} round trip");
    }
    // Under the threshold the bytes travel as they are, whatever the codec asked for.
    for c in [Codec::Gzip, Codec::Zstd] {
        let (flag, packed) = c.pack(b"tiny", 1024).unwrap();
        assert!(!flag && packed == b"tiny", "{c:?}");
    }
    // Identity never claims compression, even over the threshold.
    let (flag, packed) = Codec::Identity.pack(&body, 1).unwrap();
    assert!(!flag && packed == body);
    // And a message gzip would make bigger (incompressible bytes) is not sent compressed.
    // xorshift64: deterministic, and the bytes are as unkind to deflate as they come.
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let noise: Vec<u8> = (0..4096)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 33) as u8
        })
        .collect();
    let (flag, packed) = Codec::Gzip.pack(&noise, 1).unwrap();
    assert!(
        !flag && packed == noise,
        "gzip grew the message, so it was sent as it is"
    );
}

#[test]
fn a_reason_is_capped_by_chars_not_bytes() {
    let long = "é".repeat(500);
    let c = cap(&long, 300);
    assert_eq!(c.chars().count(), 301); // 300 kept plus the ellipsis
    assert!(c.ends_with('…'));
    assert_eq!(cap("  padded  ", 300), "padded");
}

/// A message that is not over the limit passes; one that is says what to change.
#[test]
fn a_message_over_the_limit_is_refused_before_it_is_sent() {
    assert!(ensure_frame_len(100, 128).is_ok());
    let e = ensure_frame_len(1000, 128).err().unwrap();
    let m = format!("{e:#}");
    assert!(
        m.contains("1000") && m.contains("128") && m.contains("max_message_bytes"),
        "{m}"
    );
}

#[test]
fn the_codec_names_are_the_wire_ones() {
    assert_eq!(Codec::Identity.wire_name(), "identity");
    assert_eq!(Codec::Gzip.wire_name(), "gzip");
    assert_eq!(Codec::Zstd.wire_name(), "zstd");
}
