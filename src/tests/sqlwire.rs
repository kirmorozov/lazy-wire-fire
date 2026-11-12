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
