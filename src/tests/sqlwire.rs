use super::*;

fn server_error(code: u16) -> mysql_async::Error {
    mysql_async::Error::Server(mysql_async::ServerError {
        code,
        message: "test".into(),
        state: "HY000".into(),
    })
}

#[test]
fn mysql_server_codes_for_not_now() {
    // Deadlock, lock wait timeout, read-only, lost/gone connection, overload, memory caps.
    for code in [
        1040, 1053, 1205, 1213, 1290, 1317, 1927, 1969, 2006, 2013, 3024, 4031,
    ] {
        assert!(
            mysql_transient(&server_error(code)),
            "{code} should be transient"
        );
    }
}

#[test]
fn mysql_server_codes_for_refusals() {
    // Access denied, unknown database/table/column, syntax, duplicate key, foreign key,
    // out-of-range value: the statement itself is refused.
    for code in [1045, 1049, 1054, 1062, 1064, 1146, 1452, 1690] {
        assert!(
            !mysql_transient(&server_error(code)),
            "{code} should be a refusal"
        );
    }
}

#[test]
fn mysql_connection_level_errors_are_transient() {
    let reset = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
    assert!(mysql_transient(&mysql_async::Error::from(reset)));
    assert!(mysql_transient(&mysql_async::Error::Driver(
        mysql_async::DriverError::ConnectionClosed
    )));
    assert!(!mysql_transient(&mysql_async::Error::Other("other".into())));
}

#[tokio::test]
async fn postgres_connection_failure_is_transient() {
    // A port nothing listens on: the connect fails before any server answers, and the
    // error carries no SQLSTATE, so it is classed as transient.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let conn = format!("host=127.0.0.1 port={port} connect_timeout=2");
    let err = match tokio_postgres::connect(&conn, tokio_postgres::NoTls).await {
        Ok(_) => panic!("nothing listens on the port"),
        Err(e) => e,
    };
    assert!(err.code().is_none());
    assert!(pg_transient(&err));
}

// ---- The Postgres line, over a wire the test owns -----------------------------------------
//
// `tokio_postgres::Error` is only ever made by the client itself: `Error::new` and
// `DbError::parse` are `pub(crate)`, so a SQLSTATE cannot be built in memory — it arrives on
// the wire or it is not a class at all. `Peer` is therefore a real, if small, PostgreSQL
// server: it answers the startup packet, the three session pins `PgClient::open` runs, and the
// statements of these tests, and it answers them with the `ErrorResponse` a server sends. The
// client parses that into a genuine `DbError`, prepares and executes over the extended
// protocol, and drops or keeps its pooled connection exactly as it would against the compose
// server: only the answer is ours.
//
// The classes named here are the ones a live server will only produce under global settings or
// while it dies — 53 (insufficient resources: memory, `max_connections`), 58 (system error:
// the disk), the 08 family (the connection going away) and 40 (the serialization conflicts a
// schedule, not a test, decides). The server is never reconfigured, stopped or loaded for a
// test; the classes it *can* be brought to produce honestly (40P01 by a real deadlock, 55P03
// by `FOR UPDATE NOWAIT`, 57014 by `pg_cancel_backend`, 23505 by a duplicate key) are proved
// against the real one in `tests/e2e.rs`.

/// What the peer answers a test's statement with. The pins of `open` (`SET client_encoding`,
/// `SHOW standard_conforming_strings`, `SET statement_timeout`) are always answered normally,
/// so the pooled connection can be established before a mode is armed; the mode covers what
/// the test runs afterwards on it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Answer {
    /// A normal answer: one row of one integer column, and a command tag that says one row.
    Done,
    /// A server error with this SQLSTATE and this message, as a server sends it.
    Error(&'static str, &'static str),
    /// Say nothing at all: the client's own timeout is what ends the wait.
    Silent,
    /// Drop the socket under the client, mid-statement.
    Cut,
    /// The typed rows of [`typed_row`], in the binary format the extended protocol asks for.
    Typed,
    /// Answer `SHOW standard_conforming_strings` with `off` (and everything else normally).
    StringsOff,
}

/// The columns of the typed answer: one per mapping `pg_value` makes, plus the two it does not.
///
/// `i8` holds 2^53+1, which an `f64` route would round: it can only come out right if the
/// `i64` branch ran. `nan` is a float8 the JSON number cannot hold, and `num` a `numeric` no
/// branch accepts — both must come out `null`, as the function's own doc says (cast others to
/// `text` in the query).
fn typed_columns() -> Vec<(&'static str, u32, i16)> {
    vec![
        ("t", 25, 1),   // text
        ("i2", 21, 1),  // int2
        ("i4", 23, 1),  // int4
        ("i8", 20, 1),  // int8
        ("f8", 701, 1), // float8
        ("b", 16, 1),   // bool
        ("t_null", 25, 1),
        ("i2_null", 21, 1),
        ("i4_null", 23, 1),
        ("i8_null", 20, 1),
        ("f8_null", 701, 1),
        ("b_null", 16, 1),
        ("nan", 701, 1),
        ("num", 1700, 1), // numeric: no branch maps it, so null
    ]
}

/// The one row of each, as the wire sends it (big endian, `None` is a NULL).
fn typed_row(first: bool) -> Vec<Option<Vec<u8>>> {
    let b = |v: i16| Some(v.to_be_bytes().to_vec());
    let i = |v: i32| Some(v.to_be_bytes().to_vec());
    let q = |v: i64| Some(v.to_be_bytes().to_vec());
    let d = |v: f64| Some(v.to_be_bytes().to_vec());
    if !first {
        // Every mapping also has its null: the same columns, all NULL.
        return vec![None; typed_columns().len()];
    }
    vec![
        Some("plain é text".as_bytes().to_vec()),
        b(-7),
        i(400),
        q(9_007_199_254_740_993),
        d(-2.25),
        Some(vec![1]),
        None,
        None,
        None,
        None,
        None,
        None,
        d(f64::NAN),
        Some(b"12.34".to_vec()),
    ]
}

/// An in-process PostgreSQL server for the tests below: enough of the protocol for
/// `PgClient` to live on, and nothing more.
struct Peer {
    port: u16,
    answer: std::sync::Arc<std::sync::Mutex<Answer>>,
    /// Connections that completed startup: how often `PgClient` had to open one, which is how
    /// a test sees the pooled connection being dropped after a transient and kept after a
    /// refusal.
    opens: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Every statement the client was asked to run, in order.
    statements: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Peer {
    fn start() -> Peer {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        l.set_nonblocking(true).unwrap();
        let peer = Peer {
            port,
            answer: std::sync::Arc::new(std::sync::Mutex::new(Answer::Done)),
            opens: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            statements: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            stop: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let (a, o, s, stop) = (
            peer.answer.clone(),
            peer.opens.clone(),
            peer.statements.clone(),
            peer.stop.clone(),
        );
        let born = std::time::Instant::now();
        std::thread::spawn(move || {
            // A peer lives for its test, not for the binary: it ends when asked, and in any
            // case after a minute, so a test that fails to close its client leaks a thread
            // for a minute and no longer.
            while !stop.load(std::sync::atomic::Ordering::Relaxed) && born.elapsed().as_secs() < 60
            {
                match l.accept() {
                    Ok((sock, _)) => {
                        // A socket accepted from a nonblocking listener is itself nonblocking,
                        // which would make a body that arrives in two segments look like an EOF:
                        // read it blocking, with the timeout `serve` sets.
                        let _ = sock.set_nonblocking(false);
                        // One thread per connection: a client that drops a pooled connection
                        // opens its replacement while the old one is still being read, and a
                        // peer that served connections one at a time would not answer the new
                        // startup until the old handler gave up.
                        let (a, o, st) = (a.clone(), o.clone(), s.clone());
                        std::thread::spawn(move || serve(sock, &a, &o, &st));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20))
                    }
                    Err(_) => return,
                }
            }
        });
        peer
    }

    fn answer(&self, a: Answer) {
        *self.answer.lock().unwrap() = a;
    }

    fn opens(&self) -> usize {
        self.opens.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn statements(&self) -> Vec<String> {
        self.statements.lock().unwrap().clone()
    }

    /// A client of this peer, on the plain protocol the `Disabled` tls mode uses.
    fn client(&self) -> PgClient {
        PgClient::new(
            SqlAddr {
                host: "127.0.0.1".into(),
                port: self.port,
                db: "cov".into(),
            },
            &TlsSpec::parse(None).unwrap(),
            Credentials {
                user: Some("lwf".into()),
                password: Some("lwf".into()),
            },
        )
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![t];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn cstr(s: &str) -> Vec<u8> {
    let mut out = s.as_bytes().to_vec();
    out.push(0);
    out
}

fn field(t: u8, v: &str) -> Vec<u8> {
    let mut out = vec![t];
    out.extend_from_slice(&cstr(v));
    out
}

fn row_description(cols: &[(&str, u32, i16)]) -> Vec<u8> {
    let mut body = (cols.len() as i16).to_be_bytes().to_vec();
    for (name, oid, format) in cols {
        body.extend_from_slice(&cstr(name));
        body.extend_from_slice(&0i32.to_be_bytes()); // table
        body.extend_from_slice(&0i16.to_be_bytes()); // column
        body.extend_from_slice(&(*oid as i32).to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
        body.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        body.extend_from_slice(&(*format).to_be_bytes());
    }
    msg(b'T', &body)
}

fn data_row(values: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut body = (values.len() as i16).to_be_bytes().to_vec();
    for v in values {
        match v {
            Some(v) => {
                body.extend_from_slice(&(v.len() as i32).to_be_bytes());
                body.extend_from_slice(v);
            }
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    msg(b'D', &body)
}

/// The error a server sends: `S`, `C` and `M` are the three fields `DbError::parse` insists on.
fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut body = field(b'S', "ERROR");
    body.extend_from_slice(&field(b'V', "ERROR"));
    body.extend_from_slice(&field(b'C', code));
    body.extend_from_slice(&field(b'M', message));
    body.push(0);
    msg(b'E', &body)
}

fn ready(state: u8) -> Vec<u8> {
    msg(b'Z', &[state])
}

/// The columns the peer describes for a statement, as the mode `a` answers it. `open`'s `SHOW`
/// is the one pin whose row the client reads back, so it gets its own single text column.
fn columns_for(a: Answer, pins: bool) -> Vec<(&'static str, u32, i16)> {
    if pins {
        return vec![("standard_conforming_strings", 25, 1)];
    }
    match a {
        Answer::Typed => typed_columns(),
        _ => vec![("?column?", 23, 1)],
    }
}

/// The rows an `Execute` is answered with, and the command tag that closes them.
fn rows_for(a: Answer, pins: bool) -> (Vec<Vec<Option<Vec<u8>>>>, &'static str) {
    if pins {
        let off = a == Answer::StringsOff;
        return (
            vec![vec![Some(
                if off { "off" } else { "on" }.as_bytes().to_vec(),
            )]],
            "SHOW",
        );
    }
    match a {
        Answer::Typed => (vec![typed_row(true), typed_row(false)], "SELECT 2"),
        _ => (vec![vec![Some(1i32.to_be_bytes().to_vec())]], "SELECT 1"),
    }
}

/// Whether the statement `name` this connection parsed is one of `open`'s own pins.
fn pin_of(parsed: &[(String, bool)], name: &str) -> bool {
    parsed.iter().any(|(n, p)| n == name && *p)
}

fn is_pin(q: &str) -> bool {
    let u = q.trim_start().to_ascii_uppercase();
    u.starts_with("SET ") || u.starts_with("SHOW ")
}

/// The tag a simple query's `CommandComplete` carries: what a server calls the statement.
fn tag_for(q: &str) -> &'static str {
    let u = q.trim_start().to_ascii_uppercase();
    if u.starts_with("SHOW") {
        "SHOW"
    } else if u.starts_with("SET") {
        "SET"
    } else {
        "SELECT 1"
    }
}

/// One connection, read and answered with the blocking std library on its own thread: the
/// client under test is the async one.
fn serve(
    sock: std::net::TcpStream,
    answer: &std::sync::Mutex<Answer>,
    opens: &std::sync::atomic::AtomicUsize,
    statements: &std::sync::Mutex<Vec<String>>,
) {
    use std::io::Write;
    let mut s = sock;
    // Long enough that no test waits on the peer, short enough that a silent mode cannot
    // hold this thread for the rest of the binary's life.
    let _ = s.set_read_timeout(Some(Duration::from_secs(30)));
    let mut buf = vec![0u8; 64 * 1024];
    // The startup: with the plain `NoTls` client the session still opens with an SSLRequest
    // (the config's default mode is `prefer`), so answer that with `N` — as the compose server
    // does — and only then read the startup packet itself.
    loop {
        let b = match read_body(&mut s, &mut buf, false) {
            Some(b) => b,
            None => return,
        };
        if b.len() < 4 {
            return;
        }
        match i32::from_be_bytes(b[..4].try_into().unwrap()) {
            80_877_103 => {
                if s.write_all(b"N").is_err() {
                    return;
                }
            }
            196_608 => break,
            _ => return,
        }
    }
    let mut out = Vec::new();
    // AuthenticationOk (one: the client ends authentication there) and the backend's key data,
    // as `read_info` accepts exactly those, `ParameterStatus`, `NoticeResponse` and
    // `ReadyForQuery`.
    out.extend_from_slice(&msg(b'R', &0i32.to_be_bytes()));
    let mut key = 1234i32.to_be_bytes().to_vec();
    key.extend_from_slice(&5678i32.to_be_bytes());
    out.extend_from_slice(&msg(b'K', &key));
    out.extend_from_slice(&msg(b'S', &{
        let mut b = cstr("server_version");
        b.extend_from_slice(&cstr("17.11"));
        b
    }));
    out.extend_from_slice(&msg(b'S', &{
        let mut b = cstr("client_encoding");
        b.extend_from_slice(&cstr("UTF8"));
        b
    }));
    out.extend_from_slice(&ready(b'I'));
    if s.write_all(&out).is_err() {
        return;
    }
    opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    // What the client has parsed on this connection, and whether each statement is one of
    // `open`'s own pins: those are always answered normally, so a test can arm its mode after
    // the connection is up. The client prepares in one batch and binds and executes in the
    // next, so the answer an `Execute` owes belongs to the statement `Bind` names, not to the
    // last `Parse`.
    let mut parsed: Vec<(String, bool)> = Vec::new();
    let mut pins = false;
    // The messages of the batch being read. A `Sync` with nothing in front of it is the one a
    // `Close` carries — the client sends that when a prepared statement is dropped, whenever
    // the test happens to drop it — and a dying peer must not mistake it for a statement.
    let mut batch = 0usize;
    loop {
        let Some(body) = read_body(&mut s, &mut buf, true) else {
            return;
        };
        let t = body[0];
        let body = &body[1..];
        let a = *answer.lock().unwrap();
        // A server that has decided to fail says nothing until `Sync` and then reports the
        // error once for the whole batch; a silent or dying peer says nothing at all. Only
        // `Sync`, and a simple query, gets an answer from those.
        let hold = matches!(a, Answer::Silent | Answer::Cut | Answer::Error(..));
        let mut send = |b: &[u8]| -> bool { s.write_all(b).is_ok() };
        match t {
            b'Q' => {
                batch += 1;
                let q = nul_terminated(body);
                statements.lock().unwrap().push(q.clone());
                let bytes = match a {
                    Answer::Silent => Vec::new(),
                    Answer::Cut => return,
                    Answer::Error(code, message) => {
                        let mut b = error_response(code, message);
                        b.extend_from_slice(&ready(b'E'));
                        b
                    }
                    _ => {
                        let mut b = Vec::new();
                        if q.trim_start().to_ascii_uppercase().starts_with("SHOW") {
                            b.extend_from_slice(&row_description(&columns_for(a, true)));
                            b.extend_from_slice(&data_row(&rows_for(a, true).0[0]));
                        }
                        b.extend_from_slice(&msg(b'C', &cstr(tag_for(&q))));
                        b.extend_from_slice(&ready(b'I'));
                        b
                    }
                };
                if !send(&bytes) {
                    return;
                }
            }
            b'P' => {
                // Parse: the statement name, then the query text.
                let name = nul_terminated(body);
                let query = after_nul(body).map(nul_terminated).unwrap_or_default();
                pins = is_pin(&query);
                batch += 1;
                parsed.push((name, pins));
                statements.lock().unwrap().push(query.clone());
                if a == Answer::Cut {
                    return;
                }
                if hold {
                    continue;
                }
                if !send(&msg(b'1', &[])) {
                    return;
                }
            }
            b'B' => {
                batch += 1;
                // Bind: the destination portal, then the statement it binds.
                pins = after_nul(body)
                    .map(|r| pin_of(&parsed, &nul_terminated(r)))
                    .unwrap_or(false);
                if !hold && !send(&msg(b'2', &[])) {
                    return;
                }
            }
            // Describe, of the statement (`S`: parameters, then columns) or of the portal
            // (`P`: columns). The client prepares before it binds, and expects exactly this.
            b'D' => {
                batch += 1;
                if hold {
                    continue;
                }
                let mut b = Vec::new();
                if body.first() == Some(&b'S') {
                    // Describe of a statement: the kind byte, then its name.
                    pins = pin_of(&parsed, &nul_terminated(&body[1..]));
                    b.extend_from_slice(&msg(b't', &0i16.to_be_bytes())); // no parameters
                }
                b.extend_from_slice(&row_description(&columns_for(a, pins)));
                if !send(&b) {
                    return;
                }
            }
            b'E' => {
                batch += 1;
                if hold {
                    continue;
                }
                let (rows, tag) = rows_for(a, pins);
                let mut b = Vec::new();
                for r in &rows {
                    b.extend_from_slice(&data_row(r));
                }
                b.extend_from_slice(&msg(b'C', &cstr(tag)));
                if !send(&b) {
                    return;
                }
            }
            b'S' => {
                // Sync: the end of a batch, and the one answer a held-back peer owes it.
                let bytes = match a {
                    // A `Sync` that ends a statement is the dying peer's cue; one that only
                    // closes a statement is answered as a server answers it.
                    Answer::Cut if batch > 0 => return,
                    Answer::Cut => {
                        let mut b = msg(b'3', &[]); // CloseComplete
                        b.extend_from_slice(&ready(b'I'));
                        b
                    }
                    Answer::Silent => Vec::new(),
                    Answer::Error(code, message) => {
                        let mut b = error_response(code, message);
                        b.extend_from_slice(&ready(b'E'));
                        b
                    }
                    _ => ready(b'I'),
                };
                batch = 0;
                if !send(&bytes) {
                    return;
                }
            }
            b'X' => return,
            _ => {}
        }
    }
}

/// One frontend message: `length + body`, with the `type byte + length + body` form when
/// `with_type` asks for it. `None` at EOF; a stalled socket is waited out (a silent peer is a
/// client thinking, not an error), up to the connection's read timeout, after which the
/// connection is given up on.
fn read_body(s: &mut impl std::io::Read, buf: &mut [u8], with_type: bool) -> Option<Vec<u8>> {
    let head_len = if with_type { 5 } else { 4 };
    let mut head = [0u8; 5];
    read_exact_to(s, &mut head[..head_len], buf, true)?;
    let at = usize::from(with_type);
    let len = i32::from_be_bytes(head[at..at + 4].try_into().unwrap()) as usize;
    if !(4..=64 * 1024).contains(&len) {
        return None;
    }
    let mut body = vec![0u8; len - 4];
    read_exact_to(s, &mut body, buf, false)?;
    let mut out = if with_type { vec![head[0]] } else { Vec::new() };
    out.extend_from_slice(&body);
    Some(out)
}

fn read_exact_to(
    s: &mut impl std::io::Read,
    into: &mut [u8],
    buf: &mut [u8],
    wait: bool,
) -> Option<()> {
    let mut done = 0;
    let waited = std::time::Instant::now();
    while done < into.len() {
        match s.read(&mut buf[..into.len() - done]) {
            Ok(0) => return None,
            Ok(n) => {
                into[done..done + n].copy_from_slice(&buf[..n]);
                done += n;
            }
            Err(e)
                if wait
                    && matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
            {
                // The socket has its own timeout; this is the wait for the *first* byte of a
                // header, which a quiet client can take its time about.
                if waited.elapsed() > Duration::from_secs(30) {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(_) => return None,
        }
    }
    Some(())
}

fn nul_terminated(b: &[u8]) -> String {
    let end = b.iter().position(|c| *c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

fn after_nul(b: &[u8]) -> Option<&[u8]> {
    b.iter().position(|c| *c == 0).map(|p| &b[p + 1..])
}

/// The transient classes of `pg_transient`, each with the message its server really writes.
/// Every one must come back as an ordinary (retryable) error naming the SQLSTATE and what the
/// server said, and must cost the pooled connection: the next statement opens a new one.
#[tokio::test]
async fn postgres_not_now_classes_drop_the_pooled_connection() {
    let peer = Peer::start();
    let c = peer.client();
    let t = Duration::from_secs(10);
    c.exec("SELECT 1", t).await.expect("the peer is up");
    assert_eq!(peer.opens(), 1, "one connection, established once");

    for (code, message) in [
        (
            "40001",
            "could not serialize access due to concurrent update",
        ),
        ("40P01", "deadlock detected"),
        (
            "08006",
            "could not receive data from client: connection reset by peer",
        ),
        ("08001", "could not connect to server: connection refused"),
        ("53000", "total memory limit for this process exceeded"),
        ("53100", "out of memory: failed on request of size 8192"),
        ("53300", "sorry, too many clients already"),
        ("57014", "canceling statement due to statement timeout"),
        (
            "57P01",
            "terminating connection due to administrator command",
        ),
        ("57P03", "the database system is starting up"),
        ("58000", "could not read block 0 in file \"base/1663/1234\""),
        ("58P01", "could not read block 3: read only 0 of 8192 bytes"),
        ("55P03", "canceling statement due to lock timeout"),
    ] {
        peer.answer(Answer::Error(code, message));
        let e = c
            .exec(&format!("SELECT /* {code} */ 1"), t)
            .await
            .expect_err("the peer refused the statement");
        peer.answer(Answer::Done);
        let text = format!("{e:#}");
        assert!(
            !crate::target::is_rejected(&e),
            "{code} is not the statement's fault: {text}"
        );
        assert_eq!(
            text,
            format!("postgres 127.0.0.1/cov: {code}: {message}"),
            "{code} names the class it reached and what the server said"
        );
        // The connection it poisoned is gone, and the next statement opens its own.
        let before = peer.opens();
        c.exec("SELECT 2", t)
            .await
            .expect("retried on a new connection");
        assert_eq!(peer.opens(), before + 1, "{code} dropped the pooled one");
    }
}

/// The other side of `pg_transient`: what the server refuses *as a statement* is a
/// `Rejected`, and it costs nothing but the answer — the pooled connection stays. `55P02`
/// and `5P000` sit next to the transient `55P03` and the transient `58` family on purpose:
/// the class list is the list, not a prefix habit.
#[tokio::test]
async fn postgres_statement_errors_are_classified_refusals() {
    let peer = Peer::start();
    let c = peer.client();
    let t = Duration::from_secs(10);
    c.exec("SELECT 1", t).await.expect("the peer is up");

    for (code, message) in [
        ("42601", "syntax error at or near \"FORM\""),
        ("42P01", "relation \"orders\" does not exist"),
        (
            "23505",
            "duplicate key value violates unique constraint \"orders_pkey\"",
        ),
        ("23503", "insert or update violates foreign key constraint"),
        ("22P02", "invalid input syntax for type integer: \"abc\""),
        ("42501", "permission denied for table orders"),
        ("55P02", "canceling statement due to conflict with recovery"),
        ("5P000", "unexpected internal error"),
        (
            "28000",
            "permission denied: user \"lwf\" is not a replication user",
        ),
    ] {
        peer.answer(Answer::Error(code, message));
        let e = c
            .exec(&format!("SELECT /* {code} */ 1"), t)
            .await
            .expect_err("the peer refused the statement");
        peer.answer(Answer::Done);
        let text = format!("{e:#}");
        assert!(
            crate::target::is_rejected(&e),
            "{code} is the statement's fault: {text}"
        );
        assert_eq!(
            text,
            format!("postgres 127.0.0.1/cov: {code}: {message}"),
            "{code} names the class it reached and what the server said"
        );
    }
    // None of them took a connection: every statement above ran on the one `open` made.
    assert_eq!(peer.opens(), 1, "a refusal keeps the pooled connection");
    assert!(
        peer.statements()
            .iter()
            .filter(|s| s.starts_with("SELECT /*"))
            .count()
            >= 9
    );
}

/// A connection that died under the client is transient however the client words it — and the
/// pooled connection is dropped, so the retry does not walk into the same dead socket.
#[tokio::test]
async fn postgres_a_cut_connection_is_transient() {
    let peer = Peer::start();
    let c = peer.client();
    let t = Duration::from_secs(10);
    c.exec("SELECT 1", t).await.expect("the peer is up");
    let before = peer.opens();

    peer.answer(Answer::Cut);
    let e = c
        .exec("SELECT /* cut */ 1", t)
        .await
        .expect_err("the socket went away mid-statement");
    peer.answer(Answer::Done);
    let text = format!("{e:#}");
    assert!(!crate::target::is_rejected(&e), "a dead connection: {text}");
    // The two and only two things the client can say about a connection that died; which one
    // it says depends on whether the socket or the connection task got there first.
    assert!(
        matches!(
            text.as_str(),
            "postgres 127.0.0.1/cov: connection closed"
                | "postgres 127.0.0.1/cov: error communicating with the server"
        ),
        "{text}"
    );
    assert!(
        peer.statements().iter().any(|s| s == "SELECT /* cut */ 1"),
        "the peer had the statement before it died: {:?}",
        peer.statements()
    );
    c.exec("SELECT 2", t)
        .await
        .expect("retried on a new connection");
    assert!(peer.opens() > before, "the dead one was dropped");
}

/// Both timeout arms: the wait is the caller's, not the server's, so a silent server ends it
/// at exactly the budget the action was given — and the connection it was waiting on is
/// dropped, because a server that did not answer may still be busy with it.
#[tokio::test]
async fn postgres_the_local_timeout_ends_exec_and_rows() {
    let peer = Peer::start();
    let c = peer.client();
    let long = Duration::from_secs(10);
    let short = Duration::from_millis(250);
    c.exec("SELECT 1", long).await.expect("the peer is up");

    // `exec`: the statement is sent, the server says nothing, the caller's clock ends it.
    let before = peer.opens();
    peer.answer(Answer::Silent);
    let e = c
        .exec("SELECT /* slow exec */ 1", short)
        .await
        .expect_err("the peer never answered");
    peer.answer(Answer::Done);
    assert_eq!(format!("{e:#}"), "no answer within 250 ms");
    assert!(
        !crate::target::is_rejected(&e),
        "a timeout is not a refusal: {e:#}"
    );
    c.exec("SELECT 2", long).await.expect("exec retried");
    assert_eq!(
        peer.opens(),
        before + 1,
        "the waiting connection was dropped"
    );

    // `rows`: the same arm of the listing path, with the same budget and the same care.
    let before = peer.opens();
    peer.answer(Answer::Silent);
    let e = c
        .rows("SELECT /* slow rows */ 1", short)
        .await
        .expect_err("the peer never answered");
    peer.answer(Answer::Done);
    assert_eq!(format!("{e:#}"), "no answer within 250 ms");
    assert!(
        !crate::target::is_rejected(&e),
        "a timeout is not a refusal: {e:#}"
    );
    c.exec("SELECT 3", long).await.expect("rows retried");
    assert_eq!(
        peer.opens(),
        before + 1,
        "the waiting connection was dropped"
    );

    // The peer saw all three of them: the waits were the client's, not lost connects.
    let seen = peer.statements();
    for want in ["SELECT /* slow exec */ 1", "SELECT /* slow rows */ 1"] {
        assert!(
            seen.iter().any(|s| s == want),
            "{want} never reached the server: {seen:?}"
        );
    }
}

/// `open` pins the session, and one of the pins is a *check*: with
/// `standard_conforming_strings` off, a backslash in a value would change meaning, so the
/// client refuses to run anything at all rather than quote unsafely.
#[tokio::test]
async fn postgres_refuses_to_quote_when_strings_are_not_conforming() {
    let peer = Peer::start();
    peer.answer(Answer::StringsOff);
    let c = peer.client();
    let e = c
        .exec("SELECT 1", Duration::from_secs(10))
        .await
        .expect_err("the session would not hold sql_quote safe");
    assert_eq!(
        format!("{e:#}"),
        "standard_conforming_strings is off: sql_quote would not be safe"
    );
    assert!(
        !crate::target::is_rejected(&e),
        "the server's setting is not a refusal"
    );
    // Nothing was pooled: the next call starts over (and gets through, now that the pin holds).
    peer.answer(Answer::Done);
    assert_eq!(
        c.exec("SELECT 1", Duration::from_secs(10)).await.unwrap(),
        1
    );
    assert_eq!(peer.opens(), 2);
}

/// `pg_value`, every mapping and both null shapes, over the extended protocol's binary
/// format — the format the real client asks the real server for.
#[tokio::test]
async fn postgres_rows_map_the_types_pg_value_knows() {
    let peer = Peer::start();
    let c = peer.client();
    let t = Duration::from_secs(10);
    c.exec("SELECT 1", t).await.expect("the peer is up");
    peer.answer(Answer::Typed);
    let got = c.rows("SELECT * FROM items", t).await.unwrap();
    assert_eq!(
        got,
        json!([
            {
                "t": "plain é text",
                "i2": -7,
                "i4": 400,
                "i8": 9_007_199_254_740_993i64,
                "f8": -2.25,
                "b": true,
                "t_null": null,
                "i2_null": null,
                "i4_null": null,
                "i8_null": null,
                "f8_null": null,
                "b_null": null,
                "nan": null,
                "num": null,
            },
            {
                "t": null, "i2": null, "i4": null, "i8": null, "f8": null, "b": null,
                "t_null": null, "i2_null": null, "i4_null": null, "i8_null": null,
                "f8_null": null, "b_null": null, "nan": null, "num": null,
            }
        ]),
        "{got}"
    );
    // Columns in the order the statement listed them, whatever the map's own order: the exact
    // comparison above already holds the text to it, serde_json keeps insertion order here.
    let keys: Vec<&str> = got[0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        typed_columns()
            .iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>()
    );
}

/// What a listing reports when the server answers with an error, for a transient class and
/// for a refusal: the pooled connection tells them apart, because the text cannot — `rows`
/// formats the client's own `Display`, which for a server error is the flat words `db error`.
/// (The product note goes to lwf-super with the report: a failed listing is undiagnosable from
/// `last_error` as it stands, where a failed statement names its SQLSTATE.)
#[tokio::test]
async fn postgres_listing_errors_keep_or_drop_the_connection_by_class() {
    let peer = Peer::start();
    let c = peer.client();
    let t = Duration::from_secs(10);
    c.exec("SELECT 1", t).await.expect("the peer is up");

    // Transient: the waiting connection goes, and the next listing runs on a fresh one.
    peer.answer(Answer::Error(
        "55P03",
        "canceling statement due to lock timeout",
    ));
    let e = c
        .rows("SELECT * FROM items", t)
        .await
        .expect_err("the listing failed");
    peer.answer(Answer::Done);
    assert_eq!(format!("{e:#}"), "postgres 127.0.0.1/cov: db error");
    let before = peer.opens();
    c.rows("SELECT * FROM items", t).await.expect("retried");
    assert_eq!(peer.opens(), before + 1, "the transient listing dropped it");

    // A refusal keeps it: the statement was the problem, not the connection.
    peer.answer(Answer::Error("42P01", "relation \"items\" does not exist"));
    let e = c
        .rows("SELECT * FROM missing", t)
        .await
        .expect_err("the listing failed");
    peer.answer(Answer::Done);
    assert_eq!(format!("{e:#}"), "postgres 127.0.0.1/cov: db error");
    c.rows("SELECT * FROM items", t).await.expect("retried");
    assert_eq!(peer.opens(), before + 1, "the refusal kept it");
}

#[test]
fn quoting() {
    let q = |d, v: Value| quote(d, &v).unwrap();
    assert_eq!(
        q(Dialect::MySql, json!("it's a \\ test")),
        "'it''s a \\\\ test'"
    );
    assert_eq!(
        q(Dialect::Postgres, json!("it's a \\ test")),
        "'it''s a \\ test'"
    );
    assert_eq!(q(Dialect::Postgres, json!(12.5)), "'12.5'");
    assert_eq!(q(Dialect::MySql, json!(true)), "'1'");
    assert_eq!(q(Dialect::MySql, Value::Null), "NULL");
    assert_eq!(q(Dialect::Postgres, json!({"a": [1]})), "'{\"a\":[1]}'");
    assert!(quote(Dialect::MySql, &json!("a\u{0}b")).is_err());
    assert_eq!(ident(Dialect::MySql, "we`ird").unwrap(), "`we``ird`");
    assert_eq!(
        ident(Dialect::Postgres, "we\"ird").unwrap(),
        "\"we\"\"ird\""
    );
}

#[test]
fn statements() {
    assert_eq!(
        single_statement("  INSERT INTO t VALUES ('a;b');  ")
            .unwrap()
            .as_deref(),
        Some("INSERT INTO t VALUES ('a;b')")
    );
    assert!(single_statement("DELETE FROM t; DROP TABLE t").is_err());
    assert_eq!(single_statement(" \n ").unwrap(), None);
}

#[test]
fn addresses() {
    assert_eq!(
        SqlAddr::parse("db:3307/sink", Dialect::MySql).unwrap(),
        SqlAddr {
            host: "db".into(),
            port: 3307,
            db: "sink".into()
        }
    );
    assert_eq!(
        SqlAddr::parse("postgres://pg/app", Dialect::Postgres)
            .unwrap()
            .port,
        5432
    );
    assert!(SqlAddr::parse("db:3306", Dialect::MySql).is_err());
    assert!(SqlAddr::parse("postgres://u:p@pg/app", Dialect::Postgres).is_err());
    assert!(SqlAddr::parse("mysql://db/app", Dialect::Postgres).is_err());
}
