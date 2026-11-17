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

/// The statement-format DML warning is rate limited per (schema, table): the first event for a
/// table logs at once — an entry that has never logged has no time to compare against, which is
/// what keeps the clock off the path where subtracting the interval from `now` would panic below
/// the clock's origin, a host up for less than the interval — and an event inside the interval
/// folds into the line that follows it. Another table is never folded into that line.
#[test]
fn the_first_statement_dml_line_logs_at_once() {
    let t0 = Instant::now();
    let mut slot = (None, 0u64); // a table never warned about
    assert_eq!(
        stmt_dml_gate(&mut slot, t0),
        Some(1),
        "the first event logs at once, whatever the host's uptime"
    );
    assert_eq!(slot, (Some(t0), 0), "the line stamps its table");
    assert_eq!(
        stmt_dml_gate(&mut slot, t0 + Duration::from_secs(59)),
        None,
        "an event inside the interval folds instead of logging"
    );
    assert_eq!(
        slot,
        (Some(t0), 1),
        "the folded event waits for the next line"
    );
    assert_eq!(
        stmt_dml_gate(&mut slot, t0 + STMT_DML_LOG_INTERVAL),
        Some(2),
        "the line that follows carries the folded event too"
    );
    assert_eq!(slot, (Some(t0 + STMT_DML_LOG_INTERVAL), 0));
    assert_eq!(
        stmt_dml_gate(&mut slot, t0 + STMT_DML_LOG_INTERVAL * 2),
        Some(1),
        "a quiet interval reports one event"
    );
}

/// The same rate limit over the map the engine keeps: two tables of one schema owe two lines,
/// each with its own folded count, and the map holds one entry for each.
#[test]
fn the_statement_dml_lines_are_rate_limited_per_table() {
    let t0 = Instant::now();
    let mut map: HashMap<(String, String), (Option<Instant>, u64)> = HashMap::new();
    let mut capped = false;
    let mut line = |table: &str, at: Instant| -> Option<u64> {
        let slot = stmt_dml_slot(&mut map, ("shop".into(), table.into()), &mut capped)?;
        stmt_dml_gate(slot, at)
    };
    assert_eq!(line("orders", t0), Some(1));
    assert_eq!(
        line("items", t0),
        Some(1),
        "another table owes its own line"
    );
    assert_eq!(line("orders", t0 + Duration::from_secs(30)), None);
    assert_eq!(line("items", t0 + Duration::from_secs(30)), None);
    assert_eq!(
        line("orders", t0 + STMT_DML_LOG_INTERVAL),
        Some(2),
        "its own folded event, not the other table's"
    );
    assert_eq!(line("items", t0 + STMT_DML_LOG_INTERVAL), Some(2));
    assert_eq!(
        line("orders", t0 + STMT_DML_LOG_INTERVAL * 2),
        Some(1),
        "a quiet interval reports one event"
    );
    assert_eq!(map.len(), 2, "one entry per (schema, table)");
    assert!(!capped);
}

/// The map's ceiling: the key is a name the source states, so a new table beyond the cap gets no
/// line and no entry (its event is still counted, by the caller), while a table the map already
/// holds keeps its line. The cap says itself once, not once per refused table.
#[test]
fn the_statement_dml_table_set_stays_bounded() {
    let t0 = Instant::now();
    let mut map: HashMap<(String, String), (Option<Instant>, u64)> = HashMap::new();
    let mut capped = false;
    for i in 0..MAX_STMT_DML_TABLES {
        let slot = stmt_dml_slot(&mut map, ("shop".into(), format!("t{i}")), &mut capped)
            .expect("room while under the cap");
        assert_eq!(stmt_dml_gate(slot, t0), Some(1));
    }
    assert!(!capped, "a map that fits says nothing about its cap");
    assert!(
        stmt_dml_slot(&mut map, ("shop".into(), "late".into()), &mut capped).is_none(),
        "a new table past the cap gets no line"
    );
    assert!(capped, "the lines stopping is said out loud");
    assert_eq!(map.len(), MAX_STMT_DML_TABLES, "and the map did not grow");
    let slot = stmt_dml_slot(&mut map, ("shop".into(), "t0".into()), &mut capped)
        .expect("a table the map holds keeps its line");
    assert_eq!(stmt_dml_gate(slot, t0 + STMT_DML_LOG_INTERVAL), Some(1));
    assert!(stmt_dml_slot(&mut map, ("shop".into(), "later".into()), &mut capped).is_none());
}

/// The WARN line of a statement-format DML event names the schema, the table, the kind, the
/// binlog position and the folded count — and nothing of the statement, which in statement format
/// carries the row values: the only things [`warn_statement_dml`] is given are those five.
#[test]
fn the_statement_dml_line_carries_no_statement() {
    let out = Captured::default();
    let sub = tracing_subscriber::fmt()
        .json()
        .with_writer(out.clone())
        .with_max_level(tracing::Level::WARN)
        .finish();
    let d = crate::sql::statement_dml(
        "UPDATE orders SET note = 'unpaid-42-mark' WHERE id = 7",
        "shop",
    )
    .expect("an UPDATE in a QUERY event is statement-format DML");
    let at = Position {
        file: "binlog.000007".into(),
        pos: 4211,
    };
    tracing::subscriber::with_default(sub, || warn_statement_dml(&d, &at, 3));
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "one line per reported table: {text}");
    let v: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["level"], "WARN");
    assert_eq!(v["fields"]["kind"], "UPDATE");
    assert_eq!(v["fields"]["schema"], "shop");
    assert_eq!(v["fields"]["table"], "orders");
    assert_eq!(
        v["fields"]["at"], "binlog.000007:4211",
        "the binlog position"
    );
    assert_eq!(v["fields"]["count"], 3, "the events folded into this line");
    assert!(
        v["fields"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("not in the binlog")),
        "the consequence is stated: {}",
        v["fields"]["message"]
    );
    let upper = text.to_ascii_uppercase();
    assert!(
        !text.contains("unpaid-42-mark"),
        "a row value reached the log: {text}"
    );
    assert!(
        !upper.contains("UPDATE ORDERS") && !upper.contains("SET NOTE"),
        "the statement text reached the log: {text}"
    );
}
