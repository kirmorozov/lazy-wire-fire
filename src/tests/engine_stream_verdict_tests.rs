use super::*;

fn server(code: u16, message: &str) -> anyhow::Error {
    anyhow::Error::new(mysql_async::Error::Server(mysql_async::ServerError {
        code,
        message: message.into(),
        state: "HY000".into(),
    }))
}

fn position(n: u64) -> Position {
    Position {
        file: "binlog.000001".into(),
        pos: n,
    }
}

/// The server's code is what the stream loop decides with, and the errors reach it
/// wrapped in any number of `context` layers: a code that is not found through them
/// reads as "the stream failed for an unknown reason" and the worker reconnects
/// forever instead of taking the one step that fits.
#[test]
fn the_server_code_is_found_through_the_context_chain() {
    let e = server(1236, "Got an error reading communication packets");
    assert_eq!(server_code(&e), Some(1236));
    let twice = e.context("reading the binlog").context("rule orders");
    assert_eq!(
        server_code(&twice),
        Some(1236),
        "context must not hide the code"
    );
    assert_eq!(server_code(&anyhow!("the pool is gone")), None);
    // A client-side mysql error carries no server code.
    let io = anyhow::Error::new(mysql_async::Error::Io(mysql_async::IoError::Io(
        std::io::Error::other("the socket broke"),
    )));
    assert_eq!(server_code(&io), None);
}

/// Two answers of the dump thread both arrive as 1236 and mean opposite things: the
/// file is gone (the rules must run their reset steps, replaying is impossible) or
/// another reader holds this server id (only the id has to be rolled). Mixing them
/// up either resets every rule for nothing or leaves the feed stuck.
#[test]
fn a_purged_binlog_and_a_rival_reader_are_told_apart() {
    for wording in [
        "Cannot replicate because the master purged required binary logs",
        "Could not find first log file name in binary log index file",
        "binlog.000003 not found (ENOENT: No such file or directory)",
    ] {
        let e = server(1236, wording);
        assert!(binlog_unusable(&e), "{wording} must mean the file is gone");
    }
    // The reader's own error for the same case.
    let e = anyhow::Error::new(binlog::MissingBinlog {
        from: position(4),
        message: "gone".into(),
    });
    assert!(binlog_unusable(&e.context("starting the dump thread")));

    // Same code, other meaning: the binlog is there, a second reader is not.
    let rival = server(
        1236,
        "A slave with the same server_uuid/server_id as this slave has connected to the master",
    );
    assert!(
        !binlog_unusable(&rival),
        "this needs a new server id, not a reset"
    );
    let rival = server(1236, "Different slave with the same server_id connected");
    assert!(!binlog_unusable(&rival));

    // Any other server error is not about the binlog at all: the stream is
    // simply re-read, and the rules keep their positions.
    for (code, message) in [
        (1045u16, "Access denied for user 'lazywf'"),
        (1064, "You have an error in your SQL syntax"),
        (1236, "A slave with the same server_id has connected"),
    ] {
        assert!(
            !binlog_unusable(&server(code, message)),
            "{code} {message} must not reset the rules"
        );
    }
}

/// A `keys_sql` is written to the log by its first words: a statement of any length
/// must not push the rest of the line out of the log.
#[test]
fn a_query_is_quoted_by_its_first_words() {
    assert_eq!(first_words("SELECT id FROM lines"), "SELECT id FROM lines");
    assert_eq!(
        first_words("  SELECT   a,  b\n FROM t WHERE x = 1   "),
        "SELECT a, b FROM"
    );
    assert_eq!(first_words(""), "");
}
