use super::*;

/// A target whose transport is not a SQL endpoint is refused by name, at send and at
/// fetch: `Target::new` gives these two functions only mysql and pgsql transports today,
/// and any other — today's variant or tomorrow's — must become a refusal naming itself,
/// not a `unreachable!` on the send task.
#[tokio::test]
async fn a_non_sql_transport_is_refused_by_name() {
    let transport = Transport::HttpSecret {
        reference: "env:LWF_NO_SUCH_URL".into(),
        clients: std::sync::Mutex::new(std::collections::HashMap::new()),
    };
    let pool = Pool::new("mysql://u:p@127.0.0.1:1/x");
    let expect = Expect::new(None).unwrap();
    let sends = [
        (
            "send",
            send(
                &transport,
                "SELECT 1",
                Duration::from_secs(1),
                &expect,
                &pool,
            )
            .await
            .map(|_| ()),
        ),
        (
            "fetch",
            fetch(
                &transport,
                "SELECT 1",
                Duration::from_secs(1),
                Uses::default(),
            )
            .await
            .map(|_| ()),
        ),
    ];
    for (what, r) in sends {
        let e = r
            .err()
            .unwrap_or_else(|| panic!("{what} accepted a non-SQL transport"));
        assert!(crate::target::is_rejected(&e), "{what}: {e:#}");
        assert!(
            format!("{e:#}").contains("http secret"),
            "{what} names the transport: {e:#}"
        );
    }
}
