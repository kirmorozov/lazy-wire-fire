//! The log filter of the binary: `RUST_LOG`, with dependency targets that log secret material
//! capped whatever `RUST_LOG` says (see [`CAPPED`]).

use tracing_subscriber::EnvFilter;

/// Dependency targets whose `trace` / `debug` events carry secret material: aws-sigv4 logs the
/// signing request (with `x-amz-security-token`) and the string to sign, aws-config logs the
/// `credential_process` command line, lapin's `internal_rpc` trace formats every command with
/// the connection's `status` (which holds the AMQP username). A later directive in `RUST_LOG`
/// cannot raise them; `lapin` and `amq_protocol_tcp` are held at `debug` rather than `info`:
/// their debug events carry no credentials, and capping `amq_protocol_tcp` at `info` would
/// silence its connection diagnostics (a refused connection), leaving the engine with only its
/// own "no confirm within …" to say.
pub const CAPPED: &[&str] = &[
    "aws_sigv4=info",
    "aws_config=info",
    "aws_credential_types=info",
    "aws_smithy_runtime=info",
    "aws_smithy_runtime_api=info",
    "aws_smithy_http_client=info",
    "lapin=debug",
    "amq_protocol_tcp=debug",
];

/// The filter for a `RUST_LOG` value (`None`: unset, which means `info`). An unparsable value
/// falls back to `info`, as before.
pub fn filter(rust_log: Option<&str>) -> EnvFilter {
    let base = rust_log
        .and_then(|v| EnvFilter::try_new(v).ok())
        .unwrap_or_else(|| EnvFilter::new("info"));
    CAPPED.iter().fold(base, |f, cap| {
        f.add_directive(cap.parse().expect("CAPPED holds valid directives"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_hold_whatever_rust_log_says() {
        // RUST_LOG raises the target; the cap is added after it, and the raise is gone.
        let s = filter(Some("trace,aws_sigv4=trace")).to_string();
        assert!(s.contains("aws_sigv4=info"), "{s}");
        assert!(!s.contains("aws_sigv4=trace"), "{s}");
    }

    #[test]
    fn unset_and_unparsable_mean_info() {
        for f in [filter(None), filter(Some("[[not a filter"))] {
            let s = f.to_string();
            assert!(s.contains("info") && !s.contains("trace"), "{s}");
        }
    }
}
