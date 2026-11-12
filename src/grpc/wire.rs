//! The two things the call itself answers with: what a message is compressed with, and what a
//! whole call's status means.
//!
//! The message encodings are the two the gRPC wire names besides identity, gzip and zstd.
//! lazywf compresses only messages at or over the target's `compress_min_bytes`: below it the
//! CPU costs more than the bytes saved. The per-message flag and the length prefix are tonic's
//! (see [`crate::grpc::stream`] for where they are applied); what this module says is which
//! bytes a message's payload holds.
//!
//! The status of a whole call is judged by one rule, the rule every kind follows: only an
//! answer that is positively a verdict on *this request* may refuse it. The refusing codes are
//! the ones that say *this request*, not *this moment*: the arguments are wrong, the service
//! is not there, the credentials do not open it, the document already exists. Everything else
//! — the server busy, unwell, or answering in a way lazywf cannot read, and any code not on
//! the list — is a moment, and a moment cannot take a key.

use anyhow::Result;
use tonic::Code;

/// What lazywf may compress its messages with (`vars.compression`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    Identity,
    Gzip,
    Zstd,
}

impl Codec {
    /// The name in `grpc-encoding` / `grpc-accept-encoding`.
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Codec::Identity => "identity",
            Codec::Gzip => "gzip",
            Codec::Zstd => "zstd",
        }
    }

    /// The payload of a message whose body is `body`: the bytes themselves when the message is
    /// under `min` bytes or the codec is identity, the compressed body otherwise. The `bool`
    /// says which, and is the gRPC compressed-flag.
    ///
    /// The formats are the ones tonic (and every gRPC implementation) speaks: a gzip member as
    /// RFC 1952 says, level 6 as tonic compresses its own; a zstd frame at the default level.
    /// A codec that would make the message bigger is answered with the bytes themselves —
    /// compressing something that does not shrink is CPU spent for bytes not saved.
    pub(crate) fn pack(self, body: &[u8], min: usize) -> Result<(bool, Vec<u8>)> {
        if self == Codec::Identity || body.len() < min {
            return Ok((false, body.to_vec()));
        }
        let packed = match self {
            Codec::Identity => unreachable!("handled above"),
            Codec::Gzip => {
                use std::io::Write as _;
                let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
                e.write_all(body)?;
                e.finish()?
            }
            Codec::Zstd => zstd::encode_all(body, zstd::DEFAULT_COMPRESSION_LEVEL)?,
        };
        if packed.len() >= body.len() {
            return Ok((false, body.to_vec()));
        }
        Ok((true, packed))
    }
}

/// What the whole of a call answered when it did not answer with acknowledgements: whether the
/// keys of the flush take a strike or only wait, and what to say about it.
#[derive(Clone)]
pub(crate) struct Failure {
    /// The server's own verdict on the request itself (a refusing status code): waiting would
    /// bring the same answer, so the keys take a strike.
    pub(crate) refuse: bool,
    /// The refusal says the server does not have the codec lazywf sent with (`UNIMPLEMENTED`
    /// naming a compression): the `auto` policy steps one codec down and asks again rather
    /// than telling the keys anything. Only `auto` ever steps (see [`super::Compression`]).
    pub(crate) demote: bool,
    /// What the key's status says about the moment.
    pub(crate) why: String,
}

/// What a whole call's gRPC status means. `message` is the server's own, and is quoted.
pub(crate) fn failure(code: Code, message: &str) -> Failure {
    let name = status_name(code);
    let refuse = matches!(
        code,
        Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unauthenticated
    );
    // The codec refusal every gRPC implementation answers with when the request asks for a
    // `grpc-encoding` it does not have: tonic's server says "Content is compressed with `x`
    // which isn't supported", grpc-go's says "Unsupported encoding: x".
    let m = message.to_ascii_lowercase();
    let demote =
        code == Code::Unimplemented && (m.contains("compressed") || m.contains("encoding"));
    Failure {
        refuse,
        demote,
        why: if message.is_empty() {
            format!("the call failed: {name} ({})", code as i32)
        } else {
            format!("the call failed: {name} ({}): {message}", code as i32)
        },
    }
}

/// The answer to a call that came back as plain HTTP instead of gRPC: a proxy in front of
/// the service replying 404 or 503 with an HTML page and no `grpc-status` trailer at all.
/// tonic alone would call that the meaningless "missing expected content-type", so the
/// stream's transport step maps the HTTP status to the code that carries the right decision
/// and quotes the body's first bytes — the log then says who answered, not just that
/// something did. Only a *positive* no may refuse a destination, and a 404 is the clearest
/// such no short of a REFUSED row: the method is not there and no retry will put it there.
/// Everything that could be a middlebox hiccup (408, 425, 429, all of 5xx) is
/// `Unavailable`, which the stream layer retries forever. 401 and 403 keep the codes their gRPC
/// equivalents carry — `UNAUTHENTICATED` and `PERMISSION_DENIED` — and so refuse the whole
/// action, exactly as those two codes do at the gRPC level (see [`failure`]): a peer that will
/// not take the credential is the target's configuration saying no, and waiting brings the same
/// answer. A strike is how a configuration fault reaches an owner, where an endless retry would
/// hide it, which is why the status and the peer's own words go into the message.
pub(crate) fn http_answer(status: http::StatusCode, excerpt: &str) -> tonic::Status {
    let code = match status {
        http::StatusCode::BAD_REQUEST => Code::InvalidArgument,
        http::StatusCode::NOT_FOUND
        | http::StatusCode::METHOD_NOT_ALLOWED
        | http::StatusCode::GONE
        | http::StatusCode::UNSUPPORTED_MEDIA_TYPE => Code::NotFound,
        http::StatusCode::UNAUTHORIZED => Code::Unauthenticated,
        http::StatusCode::FORBIDDEN => Code::PermissionDenied,
        http::StatusCode::CONFLICT => Code::AlreadyExists,
        http::StatusCode::REQUEST_TIMEOUT
        | http::StatusCode::TOO_EARLY
        | http::StatusCode::TOO_MANY_REQUESTS => Code::Unavailable,
        other if other.is_server_error() => Code::Unavailable,
        _ => Code::Unknown,
    };
    tonic::Status::new(
        code,
        format!("the peer answered HTTP {status}, not gRPC: {excerpt:?}"),
    )
}

/// A moment: the stream ended, an acknowledgement never came, the deadline passed, the
/// reconnect is still waiting for its backoff. Never a strike.
pub(crate) fn moment(why: impl Into<String>) -> Failure {
    Failure {
        refuse: false,
        demote: false,
        why: why.into(),
    }
}

/// The name of a status code, so a message does not make the owner look up a number.
fn status_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// A `reason` or a status message, capped: a server may put a whole stack trace in one, and
/// it ends up in the key's status text.
pub(crate) fn cap(s: &str, cap: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= cap {
        return s.to_string();
    }
    let kept: String = s.chars().take(cap).collect();
    format!("{kept}…")
}

/// The cap of a `grpc-message` or an ack's `reason`.
pub(crate) const REASON_CHARS: usize = 300;

/// The largest message a gRPC body may claim to be before lazywf refuses the stream: the
/// frame's own length field, checked against the target's limit.
pub(crate) fn ensure_frame_len(len: usize, max: usize) -> Result<()> {
    anyhow::ensure!(
        len <= max,
        "one message of {len} bytes is over the target's limit of {max} bytes \
         (`vars.max_message_bytes`, and the rule's `batch_max_bytes`)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
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
}
