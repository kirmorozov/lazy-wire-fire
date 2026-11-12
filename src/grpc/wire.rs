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
mod tests;
