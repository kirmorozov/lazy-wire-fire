use super::*;

/// A `MakeWriter` into a shared buffer, for reading back what a subscriber wrote.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Captured {
        self.clone()
    }
}

/// The drop event is one JSON line on `lazywf::undelivered` with the fields a pipeline
/// filters on, and no `op` (not known where the drop is decided).
#[test]
fn undelivered_event_carries_its_fields() {
    let out = Captured::default();
    let sub = tracing_subscriber::fmt()
        .json()
        .with_writer(out.clone())
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(sub, || {
        log_undelivered("orders_sync", "search", "42", "400 bad document", STRIKES);
    });
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "one event per key: {text}");
    let v: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["level"], "WARN");
    assert_eq!(v["target"], UNDELIVERED);
    assert_eq!(v["fields"]["message"], "undelivered key dropped");
    assert_eq!(v["fields"]["rule"], "orders_sync");
    assert_eq!(v["fields"]["target"], "search");
    assert_eq!(v["fields"]["key"], "42");
    assert_eq!(v["fields"]["reason"], "400 bad document");
    assert_eq!(v["fields"]["strikes"], STRIKES);
    assert!(v["fields"].get("op").is_none());
}

#[test]
fn managed_service_notes() {
    assert!(is_managed(Some("/rdsdbbin/mysql-8.4.8.R1/"), false));
    assert!(is_managed(None, true));
    assert!(!is_managed(Some("/usr/"), false));
    let notes = |r: std::result::Result<Option<u64>, String>, g: Option<&str>| managed_notes(&r, g);
    assert!(notes(Ok(Some(24)), Some("ON")).is_empty());
    let short = notes(Ok(Some(2)), Some("ON"));
    assert!(short.len() == 1 && short[0].0 && short[0].1.contains("2 hours"));
    assert!(notes(Ok(None), None)[0].1.contains("not set"));
    assert!(notes(Err("denied".into()), None)[0]
        .1
        .contains("could not be read (denied)"));
    let tip = notes(Ok(Some(48)), Some("OFF"));
    assert!(tip.len() == 1 && !tip[0].0 && tip[0].1.contains("gtid_mode=ON"));
    // MariaDB: no gtid_mode to read, no tip.
    assert!(notes(Ok(Some(48)), None).is_empty());
}

/// A server error as it arrives from the dump thread, wrapped the way the reader wraps it.
fn server_error(code: u16, message: &str) -> anyhow::Error {
    anyhow::Error::new(mysql_async::Error::Server(mysql_async::ServerError {
        code,
        message: message.into(),
        state: "HY000".into(),
    }))
}

/// A 1236 (the server's catch-all for a failed binlog dump) resets the rules only when its
/// own message says the requested position cannot be served. The wordings are the servers'
/// (MySQL 8.0/8.4/9.x `sql/rpl_binlog_sender.cc`, MariaDB 10.11–13.1 `sql/sql_repl.cc`);
/// a purged file or GTIDs is caught by `source::is_missing_binlog`, the rest by
/// `position_unavailable`.
#[test]
fn only_a_1236_that_says_the_position_is_gone_loses_continuity() {
    for wording in [
        // purged or gone binlog file
        "Could not find first log file name in binary log index file",
        "Could not open log file",
        "binlog.000003 not found (ENOENT: No such file or directory)",
        "could not find next log",
        // the position is outside the file
        "Client requested source to start replication from position > file size",
        "Client requested source to start replication from position < 4",
        "Client requested master to start replication from impossible position; the first \
             event 'mariadb-bin.000011' at 55212580, the last event read from \
             '/var/lib/mysql/mariadb-bin.000011' at 4, the last byte read from \
             '/var/lib/mysql/mariadb-bin.000011' at 4.",
        // the GTIDs the position needs are gone
        "Cannot replicate because the source purged required binary logs. Replicate the \
             missing transactions from elsewhere, or provision a new replica from backup.",
        "Could not find GTID state requested by slave in any binlog files. Probably the \
             slave state is too old and required binlog files have been purged.",
        "The binlog on the master is missing the GTID requested by the slave (even though \
             both a prior and a subsequent sequence number does exist), and GTID strict mode is \
             enabled.",
        "Error: connecting slave requested to start from GTID 0-1-9999, which is not in the \
             master's binlog. Since the master's binlog contains GTIDs with higher sequence \
             numbers, it probably means that the slave has diverged due to executing extra \
             erroneous transactions",
        "Replica has more GTIDs than the source has, using the source's SERVER_UUID. This \
             may indicate that the the last binary log file was truncated or lost",
    ] {
        assert!(
            binlog_unusable(&server_error(1236, wording)),
            "{wording:?} must mean the position is gone"
        );
    }
    // The reader's own error for a position the server no longer has.
    let e = anyhow::Error::new(source::MissingBinlog {
        from: Position {
            file: "binlog.000001".into(),
            pos: 4,
        },
        message: "gone".into(),
    });
    assert!(binlog_unusable(&e.context("starting the binlog dump")));
}

/// The other 1236 answers report a server-side failure to read its own binlog (an event
/// larger than `max_allowed_packet`, a damaged event, the binlog closed) and say nothing
/// about the position: the rules keep their positions and the dump is retried with the
/// capped backoff. An unknown message retries too — the default must never re-send a target.
#[test]
fn a_1236_about_the_servers_own_binlog_keeps_the_position() {
    for wording in [
        // an event the server cannot hand out: fix the server, keep the position
        "log event entry exceeded max_allowed_packet; Increase max_allowed_packet on source",
        "log event entry exceeded max_allowed_packet; Increase max_allowed_packet on master",
        "binlog truncated in the middle of event; consider out of disk space on source",
        "bogus data in log event",
        "memory allocation failed reading log event",
        "event read from binlog did not pass crc check",
        // the server or the request is misconfigured, the binlog is fine
        "Binary log is not open",
        "Binary log is not open and failed to open index file to retrieve next file.",
        "Misconfigured source - source server_id is 0",
        "The replication sender thread cannot start in AUTO_POSITION mode: this server has \
             GTID_MODE = OFF instead of ON.",
        "Replica can not handle replication events with the checksum that source is \
             configured to log",
        "Slave can not handle replication events with the checksum that master is \
             configured to log",
        // a message no server has ever sent: the safe default is to retry
        "Got an error reading communication packets",
        "unknown error reading log event on the master",
    ] {
        let e = server_error(1236, wording).context("starting the binlog dump");
        assert!(
            !binlog_unusable(&e),
            "{wording:?} says nothing about the position: retry, never reset"
        );
        assert!(!source::is_access_denied(&e));
    }
    // The duplicate server id keeps its own path (roll the id, keep the position).
    let rival = server_error(
        1236,
        "A replica with the same server_uuid/server_id as this replica has connected to \
             the source; the first event 'binlog.000009' at 4",
    );
    assert!(!binlog_unusable(&rival) && source::is_duplicate_server_id(&rival));
    // A server error that is not 1236 is not a lost position either.
    assert!(!binlog_unusable(&server_error(
        2013,
        "Lost connection to MySQL server during query"
    )));
}

#[test]
fn debounce_and_max_delay() {
    let mut b = Buffer::default();
    assert!(b
        .due(Duration::from_secs(1), Duration::from_secs(5))
        .is_none());
    let t0 = Instant::now();
    let p = |n| Position {
        file: "binlog.000001".into(),
        pos: n,
    };
    let (s, k) = b.locate("a");
    b.mark(s, k, &p(10));
    b.first = Some(t0);
    b.last = Some(t0 + Duration::from_secs(3));
    assert_eq!(
        b.due(Duration::from_secs(1), Duration::from_secs(5)),
        Some(t0 + Duration::from_secs(4))
    );
    b.last = Some(t0 + Duration::from_millis(4500));
    assert_eq!(
        b.due(Duration::from_secs(1), Duration::from_secs(5)),
        Some(t0 + Duration::from_secs(5))
    );
    // The floor of this cycle is its oldest start. Row marks arrive in binlog order,
    // but an XA's rows arrive late (at the COMMIT, with the XA's start) and `apply`
    // lowers the floor for them with the core's `pin` (the old `hold` min).
    assert_eq!(b.core.pinned(), Some(&p(10)));
}

#[test]
fn the_core_holds_the_position_and_the_retry_carries_it() {
    let p = |n| Position {
        file: "binlog.000001".into(),
        pos: n,
    };
    let mut b = Buffer::default();
    let (s, k) = b.locate("item_7");
    b.mark(s, k.clone(), &p(50));
    let (s2, k2) = b.locate("7");
    b.mark(s2, k2, &p(30));
    assert_eq!(b.pending_keys(), 2);
    assert_eq!(
        b.core.pinned(),
        Some(&p(50)),
        "the first mark pins the floor"
    );
    assert!(b.core.begin());
    let taken = b
        .core
        .take(usize::MAX, usize::MAX)
        .expect("the pending ids");
    assert_eq!(taken.origin, p(50));
    assert_eq!(b.pending_keys(), 0);
    let (sz, kz) = b.locate("z");
    b.mark(sz, kz.clone(), &p(60));
    assert_eq!(
        b.core.pinned(),
        Some(&p(50)),
        "a generation pins until its last chunk completed"
    );
    // A refused id goes back carrying the generation's origin (the old `hold`).
    let mut back = buffer::IdSet::default();
    back.insert(k);
    b.core.done(vec![(s, back)], Vec::new());
    assert_eq!(
        b.core.pinned(),
        Some(&p(50)),
        "the retry carries the origin"
    );
    assert_eq!(
        b.pending_keys(),
        2,
        "the retried id and the one marked meanwhile"
    );
    assert!(b.core.begin());
    b.core
        .take(usize::MAX, usize::MAX)
        .expect("the one pending id");
    b.core.done(Vec::new(), Vec::new());
    assert!(b.core.pinned().is_none(), "flushed: the floor is released");
}

#[test]
fn an_xa_start_late_at_the_commit_lowers_the_floor() {
    let p = |n| Position {
        file: "binlog.000001".into(),
        pos: n,
    };
    let mut b = Buffer::default();
    // A plain transaction marks at 50: the origin pins there.
    let (s, k) = b.locate("a");
    b.mark(s, k, &p(50));
    assert_eq!(b.core.pinned(), Some(&p(50)));
    // An XA that STARTED at 30 commits later: its marks arrive with the XA's start,
    // and `apply` pins it next to them. `mark` alone would not lower the floor.
    let (s, k) = b.locate("x");
    b.mark(s, k, &p(30));
    assert_eq!(
        b.core.pinned(),
        Some(&p(50)),
        "mark without pin keeps the first origin"
    );
    b.core.pin(&p(30));
    assert_eq!(
        b.core.pinned(),
        Some(&p(30)),
        "the floor is the minimum again"
    );
    // The same while a generation is in flight: `pinned` mins the pending origin
    // with the flight's, and a late pin reaches below both.
    assert!(b.core.begin());
    let (s, k) = b.locate("y");
    b.mark(s, k, &p(70));
    b.core.pin(&p(20));
    assert_eq!(
        b.core.pinned(),
        Some(&p(20)),
        "a late XA start lowers the floor even behind a flight"
    );
}

#[test]
fn queries_hold_their_position_and_ride_the_flush() {
    let p = |n| Position {
        file: "binlog.000001".into(),
        pos: n,
    };
    let mut b = Buffer::default();
    let q = KeyQuery {
        trigger: 1,
        sql: "SELECT 1".into(),
        params: vec![],
    };
    b.add_query(q.id(), q.clone(), &p(20));
    assert_eq!(b.pending_keys(), 0, "a query is not a key");
    assert!(!b.is_empty(), "but it holds the position like one");
    assert_eq!(b.core.pinned(), Some(&p(20)));
    assert!(b.core.begin());
    let taken = b.core.take(0, usize::MAX).expect("the item");
    assert_eq!(taken.items.len(), 1);
    let out = b.take_query_payloads(taken.items);
    assert_eq!(
        out,
        vec![("1:SELECT 1:[]".to_string(), q.clone())],
        "the payload rides the flush, keyed by the same id"
    );
    assert!(b.queries.is_empty());
    b.core.done(Vec::new(), Vec::new());
    assert!(
        b.core.pinned().is_none(),
        "a flushed query releases the floor"
    );
    // A failed flush puts payload and item back (the `keep_queries` retry).
    b.put_back_queries(vec![("1:SELECT 1:[]".to_string(), q)]);
    b.core.item(key::Item::User("1:SELECT 1:[]".into()), &p(20));
    assert!(!b.is_empty());
    assert_eq!(b.core.pinned(), Some(&p(20)));
    assert_eq!(b.pending_keys(), 0);
}

#[test]
fn backoff_grows_and_caps() {
    let mut b = Backoff::default();
    assert_eq!(b.fail(Duration::from_secs(60)), Duration::from_millis(500));
    assert_eq!(b.fail(Duration::from_secs(60)), Duration::from_secs(1));
    for _ in 0..20 {
        b.fail(Duration::from_secs(60));
    }
    assert_eq!(b.fail(Duration::from_secs(60)), Duration::from_secs(60));
    b.ok();
    assert!(b.blocked(Instant::now()).is_none());
}

/// The versions that stopped writing VIRTUAL generated columns into UPDATE and DELETE row images
/// are the ones lazywf refuses reads for — and only those: 10.6 and 10.11 (tested on 10.6.28 and
/// 10.11.19) and the MySQL family (tested on 8.0.46, 8.4.13, 9.7.4, Percona 8.4.11) send them.
#[test]
fn maria_skips_virtual_reads_the_version() {
    let skipped = [
        "11.4.0-MariaDB",
        "11.4.13-MariaDB-1:11.4.13+maria~ubu2404",
        "11.8.9-MariaDB-1:11.8.9+maria~ubu2410",
        "12.3.3-MariaDB",
        "13.2.0-preview",
        "11.4",
        // A replication proxy shadows the version it fronts with the old one.
        "5.5.5-11.4.13-MariaDB-1:11.4.13+maria~ubu2404",
        "5.5.5-13.0.2-MariaDB",
    ];
    for v in skipped {
        assert!(maria_skips_virtual(v), "{v} writes no VIRTUAL columns");
    }
    let writes = [
        "10.6.28-MariaDB-1:10.6.28+maria~ubu2204",
        "10.11.19-MariaDB-1:10.11.19+maria~ubu2404",
        "5.5.5-10.11.19-MariaDB-log",
        "11.3.2-MariaDB",
        "11",
        "8.4.13",
        "9.7.4",
        "",
        "MariaDB",
        "11.x.1",
        "unknown",
    ];
    for v in writes {
        assert!(!maria_skips_virtual(v), "{v} writes them");
    }
}

/// A rule is refused for the column it reads that the image shifted out — the VIRTUAL one itself
/// and everything after it — and left alone when it reads only columns before it. A STORED column
/// after a VIRTUAL one is refused too: the image skips the VIRTUAL one, so the STORED value moves
/// up a place as well.
#[test]
fn unreadable_read_refuses_reads_past_the_virtual_column() {
    // id, price, price_x2 (VIRTUAL), name, stock
    let all = [
        ("id", 0usize),
        ("price", 1),
        ("price_x2", 2),
        ("name", 3),
        ("stock", 4),
    ];
    assert_eq!(
        unreadable_read(&all, false, 2),
        Some(VirtualRead::Col("price_x2".into())),
        "the VIRTUAL column itself is the first read that cannot be carried"
    );
    assert_eq!(
        unreadable_read(&[("id", 0), ("price", 1)], false, 2),
        None,
        "columns before it are read as themselves"
    );
    assert_eq!(
        unreadable_read(&[("id", 0), ("price_stored", 3)], false, 2),
        Some(VirtualRead::Col("price_stored".into())),
        "a STORED column after a VIRTUAL one is shifted too"
    );
    assert_eq!(
        unreadable_read(&[], false, 0),
        None,
        "a trigger that reads no column marks nothing wrong"
    );
    assert_eq!(
        unreadable_read(&[("id", 0)], true, 4),
        Some(VirtualRead::Full),
        "a Full key renders the whole row, so a table with a VIRTUAL column is refused outright"
    );
    assert_eq!(
        unreadable_read(&[("stock", 4), ("name", 3)], false, 3),
        Some(VirtualRead::Col("stock".into())),
        "the first unreadable read of the rule is the one reported"
    );
}

/// The VIRTUAL generated column of a table, in the order the layout is built in: `information_schema`
/// says which columns are generated VIRTUAL, and the first of them is where the row image runs short.
#[test]
fn virtual_first_of_finds_the_generated_column() {
    let rows = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(n, e)| (n.to_string(), e.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        virtual_first_of(&rows(&[
            ("id", ""),
            ("price", "STATIC"),
            ("price_x2", "VIRTUAL GENERATED"),
            ("name", "on update CURRENT_TIMESTAMP"),
            ("stock", ""),
        ])),
        Some((2, "price_x2".to_string())),
        "the first VIRTUAL column, by its position among all columns"
    );
    assert_eq!(
        virtual_first_of(&rows(&[("id", ""), ("price_stored", "STORED GENERATED")])),
        None,
        "a STORED column is in the image, so nothing is refused"
    );
    assert_eq!(
        virtual_first_of(&rows(&[
            ("a", "STORED GENERATED"),
            ("b", "VIRTUAL GENERATED"),
            ("c", "VIRTUAL GENERATED"),
        ])),
        Some((1, "b".to_string())),
        "with two of them, the first decides"
    );
    assert_eq!(
        virtual_first_of(&rows(&[])),
        None,
        "a table with no columns"
    );
    assert_eq!(
        virtual_first_of(&rows(&[("id", ""), ("n", "virtual generated")])),
        Some((1, "n".to_string())),
        "the wording is matched whatever its case"
    );
}
