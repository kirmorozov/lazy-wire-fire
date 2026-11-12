use super::*;
use crate::model::RawTarget;

fn raw(vars: &str, endpoint: &str) -> RawTarget {
    RawTarget {
        name: "orders".into(),
        kind: Some("grpc".into()),
        endpoints: Some(endpoint.into()),
        vars: Some(vars.into()),
        ..Default::default()
    }
}

fn def(vars: &str, endpoint: &str) -> Result<TargetDef> {
    TargetDef::from_row(&raw(vars, endpoint))
}

#[test]
fn endpoints() {
    let a = Addr::parse("grpc://ingest.example.test:9000/catalog.v1.Ingest/Push").unwrap();
    assert_eq!(
        (
            a.secure,
            a.host.as_str(),
            a.port,
            a.service.as_str(),
            a.method.as_str()
        ),
        (
            false,
            "ingest.example.test",
            9000,
            "catalog.v1.Ingest",
            "Push"
        )
    );
    assert_eq!(
        a.url(),
        "grpc://ingest.example.test:9000/catalog.v1.Ingest/Push"
    );
    assert_eq!(a.authority(), "ingest.example.test:9000");
    assert_eq!(a.path(), "/catalog.v1.Ingest/Push");
    // The default ports, and a service of the default package.
    let a = Addr::parse("grpcs://ingest.example.test/orders.v1.Ingest/Stream").unwrap();
    assert_eq!((a.secure, a.port), (true, 443));
    let a = Addr::parse("grpc://h/Ingest/Push").unwrap();
    assert_eq!((a.port, a.service.as_str()), (80, "Ingest"));
    // `_` is a legal start of a name.
    assert!(Addr::parse("grpc://h/_v1.S/M").is_ok());
}

#[test]
fn a_refused_endpoint_names_the_endpoint() {
    for bad in [
        "http://h:9000/p.S/M",       // another scheme
        "grpc://h:9000/p.S",         // no method
        "grpc://h:9000/p.S/M/extra", // a path that is not service/method
        "grpc://h:9000//M",          // no service
        "grpc://h:9000/p.S/",        // no method
        "grpc://:9000/p.S/M",        // no host
        "grpc://@h:9000/p.S/M",      // credentials in the URL
        "grpc://h:9000/p.S/M?x=1",   // a query string
        "grpc://h:9000/p.S#M",       // a fragment
        "grpc://h:9000/p.9S/M",      // a component that is not a name
        "grpc://h:9000/p.S/9M",      // a method that is not a name
        "grpc://h:0x20/p.S/M",       // a port that is not a number
        "grpc://[::1]:9000/p.S/M",   // IPv6
    ] {
        let e = Addr::parse(bad)
            .err()
            .unwrap_or_else(|| panic!("accepted `{bad}`"));
        // The message names the endpoint as it was written, credentials aside.
        let shown = crate::secrets::redact_url(bad);
        assert!(e.to_string().contains(&shown), "{bad}: {e:#}");
    }
    // The userinfo never reaches the message in the clear.
    let e = Addr::parse("grpc://user:pw@h:9000/p.S/M").err().unwrap();
    assert!(!format!("{e:#}").contains("pw"), "{e:#}");
}

#[test]
fn a_target_loads_its_schema() {
    let (_s, ds) = write_descriptor_set();
    let vars = format!(
        r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","token":"env:LWF_UNIT_TOKEN"}}"#
    );
    let d = def(
        &vars,
        "grpcs://ingest.example.test:9000/catalog.v1.Ingest/Push",
    )
    .unwrap();
    let (ctx, client) = Client::new(&d, &d.endpoints[0]).unwrap();
    assert_eq!(ctx["host"], "ingest.example.test");
    assert_eq!(ctx["port"], 9000);
    assert_eq!(ctx["service"], "catalog.v1.Ingest");
    assert_eq!(ctx["method"], "Push");
    assert_eq!(ctx["item_type"], "orders.v1.Item");
    assert_eq!(client.addr.path(), "/catalog.v1.Ingest/Push");
    assert_eq!(client.mode, Some(Mode::VerifyFull));
    assert_eq!(client.opts.token.as_deref(), Some("env:LWF_UNIT_TOKEN"));
    assert_eq!(client.opts.compress_min_bytes, COMPRESS_MIN_BYTES);
    assert_eq!(client.opts.max_message_bytes, MAX_MESSAGE_BYTES);
    assert_eq!(client.opts.compression, Compression::Auto);
    // The template's context never carries the token, not even as a reference.
    assert!(!ctx.to_string().contains("token"), "{ctx}");
}

#[test]
fn a_target_without_tls_is_plain() {
    let (_s, ds) = write_descriptor_set();
    let vars = format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item"}}"#);
    let d = def(&vars, "grpc://h:9000/catalog.v1.Ingest/Push").unwrap();
    let (_, client) = Client::new(&d, &d.endpoints[0]).unwrap();
    assert_eq!(client.mode, None);
}

#[test]
fn a_grpcs_endpoint_with_tls_disabled_is_refused() {
    let (_s, ds) = write_descriptor_set();
    let vars = format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item"}}"#);
    let mut r = raw(&vars, "grpcs://h:9000/catalog.v1.Ingest/Push");
    r.tls = Some("disabled".into());
    let e = TargetDef::from_row(&r).err();
    let msg = format!("{:#}", e.unwrap_or_else(|| anyhow::anyhow!("not refused")));
    assert!(msg.contains("tls") && msg.contains("grpcs://"), "{msg}");
}

#[test]
fn a_target_needs_its_schema_vars_and_a_file_reference() {
    let (_s, ds) = write_descriptor_set();
    for (vars, want) in [
        (
            r#"{"item_type":"orders.v1.Item"}"#.to_string(),
            "vars.descriptor_set",
        ),
        (
            r#"{"descriptor_set":"file:x"}"#.to_string(),
            "vars.item_type",
        ),
        (
            r#"{"descriptor_set":"/tmp/x.desc","item_type":"orders.v1.Item"}"#.to_string(),
            "file:",
        ),
        (
            r#"{"descriptor_set":"env:X","item_type":"orders.v1.Item"}"#.to_string(),
            "file:",
        ),
        (
            format!(
                r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","token":"literal"}}"#
            ),
            "vars.token",
        ),
        (
            format!(
                r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","schema":"file:x"}}"#
            ),
            "vars.schema",
        ),
        (
            format!(
                r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","message_type":"x"}}"#
            ),
            "vars.message_type",
        ),
        (
            format!(
                r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","service":"a.b.C"}}"#
            ),
            "vars.service",
        ),
    ] {
        let e = def(&vars, "grpc://h:9000/p.S/M").err();
        let msg = format!(
            "{:#}",
            e.unwrap_or_else(|| anyhow::anyhow!("accepted `{vars}`"))
        );
        assert!(msg.contains(want), "`{vars}`: {msg}");
    }
}

#[test]
fn an_unknown_compression_or_size_is_refused() {
    let (_s, ds) = write_descriptor_set();
    for (k, v) in [
        ("compression", "brotli"),
        ("compress_min_bytes", "0x10"),
        ("max_message_bytes", "10"),
        ("max_message_bytes", "many"),
    ] {
        let vars =
            format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","{k}":"{v}"}}"#);
        let e = def(&vars, "grpc://h:9000/p.S/M").err();
        let msg = format!(
            "{:#}",
            e.unwrap_or_else(|| anyhow::anyhow!("accepted {k}={v}"))
        );
        assert!(msg.contains(k), "{k}={v}: {msg}");
    }
    for v in ["auto", "none", "gzip", "zstd", "identity", "GZIP"] {
        let vars = format!(
            r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","compression":"{v}"}}"#
        );
        assert!(def(&vars, "grpc://h:9000/p.S/M").is_ok(), "compression {v}");
    }
    // A number is as good as a string of digits, and the compression names are the wire's.
    let vars = format!(
        r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","compress_min_bytes":2048,"max_message_bytes":8192}}"#
    );
    let d = def(&vars, "grpc://h:9000/p.S/M").unwrap();
    let o = options(&d.vars).unwrap();
    assert_eq!((o.compress_min_bytes, o.max_message_bytes), (2048, 8192));
    assert_eq!(Compression::Gzip.wire_name(), "gzip");
    assert_eq!(Compression::Auto.wire_name(), "identity");
}

#[test]
fn the_descriptor_set_must_hold_the_item_type() {
    let (_s, ds) = write_descriptor_set();
    for item_type in ["orders.v1.Missing", "orders.v1.Price", "orders.v1"] {
        let vars = format!(r#"{{"descriptor_set":"file:{ds}","item_type":"{item_type}"}}"#);
        let e = def(&vars, "grpc://h:9000/catalog.v1.Ingest/Push")
            .and_then(|d| Client::new(&d, &d.endpoints[0]).map(|_| ()))
            .err();
        let msg = format!(
            "{:#}",
            e.unwrap_or_else(|| anyhow::anyhow!("accepted `{item_type}`"))
        );
        assert!(
            msg.contains(item_type) && msg.contains("item_type"),
            "`{item_type}`: {msg}"
        );
    }
    // Not a descriptor set at all.
    let dir = _s.dir.clone();
    let path = dir.join("not-a-desc");
    std::fs::write(&path, b"this is not protobuf").unwrap();
    let vars = format!(
        r#"{{"descriptor_set":"file:{}","item_type":"orders.v1.Item"}}"#,
        path.display()
    );
    let e = def(&vars, "grpc://h:9000/catalog.v1.Ingest/Push")
        .and_then(|d| Client::new(&d, &d.endpoints[0]).map(|_| ()))
        .err()
        .unwrap();
    assert!(format!("{e:#}").contains("descriptor"), "{e:#}");
}

/// The server's method must be the shape lazywf pushes to (`stream lazywf.v1.Envelope` in,
/// `stream lazywf.v1.Acks` out): a method of another shape would be sent bytes its server
/// cannot read, so it is refused at load and the error names the endpoint.
#[test]
fn the_endpoint_method_is_checked_against_the_descriptor_set() {
    let (_s, ds) = write_descriptor_set();
    let load = |service: &str, method: &str| {
        let vars = format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item"}}"#);
        let ep = format!("grpc://h:9000/{service}/{method}");
        let d = def(&vars, &ep)?;
        Client::new(&d, &d.endpoints[0]).map(|_| ())
    };
    // The descriptor set's `catalog.v1.Ingest/Push` is the contract's shape.
    assert!(load("catalog.v1.Ingest", "Push").is_ok());
    // lazywf's own contract is in the pool, so a server that implements it directly needs
    // no descriptor of its own.
    assert!(load(contract::SERVICE, contract::METHOD).is_ok());
    for (service, method) in [
        ("catalog.v1.Ingest", "Nope"),
        ("catalog.v1.Ingest", "Unary"),
        ("catalog.v1.Ingest", "ServerSide"),
        ("catalog.v1.Ingest", "ClientSide"),
        ("catalog.v1.Nope", "Push"),
    ] {
        let e = load(service, method)
            .err()
            .unwrap_or_else(|| panic!("accepted {service}/{method}"));
        let msg = format!("{e:#}");
        assert!(
            msg.contains(service) && msg.contains(method),
            "{service}/{method}: {msg}"
        );
    }
}

#[test]
fn a_grpc_endpoint_is_only_for_grpc_and_grpc_takes_only_it() {
    let e = TargetDef::from_row(&RawTarget {
        name: "t".into(),
        kind: Some("http".into()),
        endpoints: Some("grpc://h:9000/p.S/M".into()),
        vars: None,
        ..Default::default()
    })
    .err()
    .unwrap();
    assert!(format!("{e:#}").contains("kind grpc"), "{e:#}");
    let (_s, ds) = write_descriptor_set();
    let e = def(
        &format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item"}}"#),
        "https://h:9000/p.S/M",
    )
    .err()
    .unwrap();
    assert!(format!("{e:#}").contains("grpc://"), "{e:#}");
}

/// What the stream answers on its own, without a server to ask: a body that is not the
/// `docs` array, a document that does not fit the schema and a key that is not the engine's
/// are refused (the template or the schema is wrong, and sending again would be wrong
/// again), while a server that cannot be reached is transient for every key — the one
/// answer that cannot lose one or burn a strike.
#[test]
fn the_items_are_judged_before_the_wire_and_the_wire_never_loses_one() {
    let (_s, ds) = write_descriptor_set();
    let vars = format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item"}}"#);
    let d = def(&vars, "grpc://h:9000/catalog.v1.Ingest/Push").unwrap();
    let (_, client) = Client::new(&d, &d.endpoints[0]).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let keys = ["a".to_string(), "b".to_string()];
    let expect = Expect::new(None).unwrap();
    let pool = mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/x");
    let send = |rendered: &str, keys: &[String]| {
        rt.block_on(send_items(&client, rendered, keys, &expect, &pool))
    };

    // Nothing rendered: nothing to send, and no key is kept waiting for a stream that has
    // no items on it.
    let items = send("", &keys).unwrap();
    assert!(
        matches!(items.as_slice(), [Item::Ok, Item::Ok]),
        "{items:?}"
    );

    // Refused before the wire: these say the template or the schema is wrong, and every
    // retry would say it again.
    for (body, want) in [
        (r#"{"not":"docs"}"#, "docs"),
        (r#"[{"key":"a"}]"#, "doc"),
        (
            r#"[{"key":"WRONG","doc":{"id":1}},{"key":"b","doc":{"id":2}}]"#,
            "WRONG",
        ),
        (
            r#"[{"key":"a","doc":{"id":"seven"}},{"key":"b","doc":{"id":2}}]"#,
            "id",
        ),
        (
            r#"[{"key":"a","doc":{"id":1}},{"key":"b","doc":{"id":2}},"more"]"#,
            "for 2 keys",
        ),
    ] {
        let e = send(body, &keys)
            .err()
            .unwrap_or_else(|| panic!("accepted `{body}`"));
        assert!(crate::target::is_rejected(&e), "{body}: {e:#}");
        assert!(format!("{e:#}").contains(want), "{body}: {e:#}");
    }

    // A server lazywf cannot even reach: every key waits, no key is refused, and the error
    // is not a refusal of the action either — the same answer for a server that opens the
    // stream and never acknowledges, which the e2e covers with a real one.
    let d = {
        let mut d = def(&vars, "grpc://127.0.0.1:1/catalog.v1.Ingest/Push").unwrap();
        d.timeout = std::time::Duration::from_secs(5);
        d
    };
    let (_, client) = Client::new(&d, &d.endpoints[0]).unwrap();
    let body = r#"[{"key":"a","doc":{"id":1}},{"key":"b","doc":{"id":2}}]"#;
    let items = rt
        .block_on(send_items(&client, body, &keys, &expect, &pool))
        .unwrap();
    assert_eq!(items.len(), 2);
    assert!(
        items
            .iter()
            .all(|i| matches!(i, Item::Transient(w) if !w.is_empty())),
        "{items:?}"
    );
}

/// The rule-load check: columns that do not fit the item message name the column, the
/// field and the fix; a field no column fills is not an error.
#[test]
fn a_rule_is_checked_against_the_schema() {
    let (_s, ds) = write_descriptor_set();
    let vars = format!(r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item"}}"#);
    let d = def(&vars, "grpc://h:9000/catalog.v1.Ingest/Push").unwrap();
    let rule = |columns: &[(&str, docs::ValueKind)]| {
        // The columns of a select, with the `_key` one always has, and the document keys and
        // kinds as the plan derives them: what `kind_errors` is handed when a rule loads.
        let mut cols = vec![docs::Field::new("_key", None, false)];
        cols.extend(columns.iter().map(|(c, k)| docs::Field {
            column: (*c).into(),
            key: (*c).into(),
            kind: *k,
        }));
        let plan = docs::Plan::with_fields(&cols).expect("a plan");
        rule_errors(&rule_def(None), &d, plan.fields())
    };
    assert!(rule(&[
        ("id", docs::ValueKind::Int),
        ("sku", docs::ValueKind::Str),
        ("price", docs::ValueKind::Double),
        ("tags[]", docs::ValueKind::Array),
        ("status", docs::ValueKind::Str),
    ])
    .is_empty());
    let e = rule(&[("nosuch", docs::ValueKind::Str)]);
    assert!(
        e.iter().any(|e| e.contains("nosuch") && e.contains("sku")),
        "{e:?}"
    );
    let e = rule(&[("blob", docs::ValueKind::Blob)]);
    assert!(e.iter().any(|e| e.contains("TO_BASE64")), "{e:?}");
    // A batch over the target's limit is refused, and names the var that raises it.
    let e = rule_errors(&rule_def(Some(100_000_000)), &d, &[]);
    assert!(
        e.iter()
            .any(|e| e.contains("batch_max_bytes") && e.contains("max_message_bytes")),
        "{e:?}"
    );
}

fn rule_def(batch_max_bytes: Option<usize>) -> RuleDef {
    RuleDef {
        name: "orders".into(),
        enabled: true,
        target: "orders".into(),
        selects: Vec::new(),
        snapshot_keys_sql: None,
        template: "{{json docs}}".into(),
        fire_per: FirePer::Batch,
        batch_max_keys: None,
        batch_max_bytes,
        debounce: std::time::Duration::ZERO,
        max_delay: std::time::Duration::from_secs(1),
        reset_steps: Vec::new(),
        reset_requested: false,
        vars: Map::new(),
    }
}

/// A directory `secrets::read_bytes` accepts, with `LAZYWF_SECRET_DIRS` pointed at it for
/// the whole test: the var is process-wide and other tests set it too, so `ENV_LOCK` is held
/// as long as the `Secrets` is, not just across the call that sets it.
struct Secrets {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    dir: std::path::PathBuf,
}

fn secrets() -> Secrets {
    let _guard = crate::secrets::ENV_LOCK.blocking_lock();
    let dir = std::env::temp_dir().join(format!("lazywf-grpc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
    Secrets { _guard, dir }
}

/// Writes the test schema and returns its descriptor set's path: an item of every shape in
/// the app's own package, and a service of the contract's shape in another one, so a target
/// has to look in both. Keep the `Secrets` alive as long as the path is in use, or the
/// target cannot read it again.
fn write_descriptor_set() -> (Secrets, String) {
    let s = secrets();
    let dir = &s.dir;
    let inc = dir.join("include");
    std::fs::create_dir_all(inc.join("lazywf/v1")).unwrap();
    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/proto/lazywf/v1/lazywf.proto"),
        inc.join("lazywf/v1/lazywf.proto"),
    )
    .unwrap();
    // The document, in the owner's own package: the shapes a real schema has.
    std::fs::write(
        inc.join("item.proto"),
        r#"
            syntax = "proto3";
            package orders.v1;

            message Line {
              string sku = 1;
              int32 qty = 2;
            }
            enum Price {
              PRICE_UNSPECIFIED = 0;
              PRICE_CENTS = 1;
            }
            message Item {
              int64 id = 1;
              string sku = 2;
              double price = 3;
              bool live = 4;
              bytes blob = 5;
              Price status = 6;
              repeated string tags = 7;
              repeated Line lines = 8;
              map<string, string> labels = 9;
              Line one = 10;
            }
            "#,
    )
    .unwrap();
    // The server's own service, of the contract's shape.
    std::fs::write(
        inc.join("catalog.proto"),
        r#"
            syntax = "proto3";
            package catalog.v1;
            import "lazywf/v1/lazywf.proto";

            service Ingest {
              rpc Push(stream lazywf.v1.Envelope) returns (stream lazywf.v1.Acks);
              rpc Unary(lazywf.v1.Envelope) returns (lazywf.v1.Acks);
              rpc ServerSide(lazywf.v1.Envelope) returns (stream lazywf.v1.Acks);
              rpc ClientSide(stream lazywf.v1.Envelope) returns (lazywf.v1.Acks);
            }
            "#,
    )
    .unwrap();
    let bytes = protox::Compiler::new([inc.as_path(), contract::PROTO_DIR.as_ref()])
        .expect("compiler")
        .include_source_info(false)
        .include_imports(true)
        .open_files([inc.join("item.proto"), inc.join("catalog.proto")])
        .expect("compiles")
        .encode_file_descriptor_set();
    let path = dir.join("catalog.desc");
    std::fs::write(&path, bytes).unwrap();
    (s, path.display().to_string())
}
