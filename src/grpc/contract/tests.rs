use super::*;
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage};

/// The committed descriptor is the shipped `.proto` compiled.
#[test]
fn descriptor_matches_proto() {
    let want = protox::Compiler::new([PROTO_DIR])
        .expect("the proto directory opens")
        .include_source_info(false)
        .include_imports(true)
        .open_files(["lazywf/v1/lazywf.proto"])
        .expect("the shipped .proto compiles")
        .descriptor_pool();
    let got = DescriptorPool::decode(DESCRIPTOR).expect("the shipped descriptor decodes");
    let protos = |pool: &DescriptorPool| {
        pool.files()
            .map(|f| f.file_descriptor_proto().clone())
            .collect::<Vec<_>>()
    };
    let (mut a, mut b) = (protos(&want), protos(&got));
    for f in a.iter_mut().chain(b.iter_mut()) {
        // Source info is neither here nor on the wire.
        f.source_code_info = None;
    }
    assert_eq!(a, b, "proto/lazywf/v1/lazywf.desc is stale");
}

/// The pool holds the contract, and the Rust types agree with it.
#[test]
fn descriptor_holds_the_contract() {
    let pool = DescriptorPool::decode(DESCRIPTOR).expect("decodes");
    let env = pool
        .get_message_by_name(ENVELOPE)
        .expect("lazywf.v1.Envelope");
    let item = pool.get_message_by_name("lazywf.v1.Item").expect("Item");
    let acks = pool.get_message_by_name(ACKS).expect("lazywf.v1.Acks");
    assert_eq!(env.full_name(), ENVELOPE);
    // The four fields of an item, by number and type: what the descriptor and the prost
    // types above have to agree on.
    let kinds = [
        ("seq", 1, "uint64"),
        ("hash", 2, "uint64"),
        ("key", 3, "string"),
        ("item", 4, "bytes"),
    ];
    for (name, number, want) in kinds {
        let f = item
            .get_field_by_name(name)
            .unwrap_or_else(|| panic!("{name}"));
        assert_eq!(f.number(), number);
        assert_eq!(field_type(&f), want, "{name}");
    }
    let a = acks.get_field_by_name("acks").expect("acks");
    assert!(!a.is_map());
    assert_eq!(
        a.kind()
            .as_message()
            .map(|m| m.full_name())
            .unwrap_or_default(),
        "lazywf.v1.Ack"
    );
    let svc = pool.get_service_by_name(SERVICE).expect("Sink");
    let m = svc
        .methods()
        .find(|m| m.name() == METHOD)
        .expect("Push method");
    assert!(m.is_client_streaming() && m.is_server_streaming());
    assert_eq!(m.input().full_name(), ENVELOPE);
    assert_eq!(m.output().full_name(), ACKS);
    // The status is `Ack.Status`: the nested enum the .proto declares.
    let st = pool
        .get_enum_by_name("lazywf.v1.Ack.Status")
        .expect("lazywf.v1.Ack.Status");
    for (name, number) in [
        ("STATUS_UNSPECIFIED", 0),
        ("OK", 1),
        ("REFUSED", 2),
        ("RETRY", 3),
    ] {
        assert_eq!(
            st.get_value_by_name(name).expect("value").number(),
            number,
            "{name}"
        );
    }
    let status = pool
        .get_message_by_name("lazywf.v1.Ack")
        .expect("Ack")
        .get_field_by_name("status")
        .expect("status field");
    assert_eq!(
        status.kind().as_enum().map(|e| e.full_name()),
        Some("lazywf.v1.Ack.Status"),
        "the ack's status is the nested enum"
    );
}

/// The prost types encode and decode through the descriptor's own dynamic messages: the
/// two agree, which is what lets the stream module speak a descriptor's shapes.
#[test]
fn prost_types_agree_with_descriptor() {
    let pool = DescriptorPool::decode(DESCRIPTOR).expect("decodes");
    let env = pool
        .get_message_by_name(ENVELOPE)
        .expect("lazywf.v1.Envelope");
    let acks = pool.get_message_by_name(ACKS).expect("lazywf.v1.Acks");

    let e = Envelope {
        items: vec![
            Item {
                seq: 1,
                hash: hash(b"order-1", br#"{"a":1}"#),
                key: "order-1".into(),
                item: br#"{"a":1}"#.to_vec(),
            },
            Item {
                seq: 2,
                hash: hash(b"order-2", br#"{"b":2}"#),
                key: "order-2".into(),
                item: br#"{"b":2}"#.to_vec(),
            },
        ],
    };
    let bytes = e.encode_to_vec();
    let dyn_env = DynamicMessage::decode(env.clone(), &bytes[..]).expect("dynamic decodes");
    assert_eq!(
        prost::Message::encode_to_vec(&dyn_env),
        bytes,
        "re-encoding the dynamic message gives the same bytes"
    );

    let a = Acks {
        acks: vec![
            Ack {
                seq: 1,
                hash: 9,
                status: Status::Ok as i32,
                reason: String::new(),
            },
            Ack {
                seq: 0,
                hash: 7,
                status: Status::Refused as i32,
                reason: "no such sku".into(),
            },
        ],
    };
    let bytes = a.encode_to_vec();
    let dyn_acks = DynamicMessage::decode(acks, &bytes[..]).expect("decodes");
    assert_eq!(prost::Message::encode_to_vec(&dyn_acks), bytes);
}

/// The hash is xxh3-64 of the key's length, the key and the body: seed 0 (the published
/// xxHash value for the empty input pins the hash the framing is built on), the key counted
/// so that two keys carrying the same document cannot confirm each other, and the boundary
/// between the two parts part of the hash.
#[test]
fn hash_is_xxh3_of_the_pair() {
    // The xxHash specification's own value for XXH3_64 of "", default seed.
    assert_eq!(
        twox_hash::xxhash3_64::Hasher::oneshot(b""),
        0x2D068005_38D394C2
    );
    // The framing, written out: eight length bytes, the key, the body.
    assert_eq!(
        hash(b"", b""),
        twox_hash::xxhash3_64::Hasher::oneshot(&[0; 8])
    );
    assert_eq!(
        hash(b"abc", b""),
        twox_hash::xxhash3_64::Hasher::oneshot(&[3, 0, 0, 0, 0, 0, 0, 0, b'a', b'b', b'c'])
    );
    // So the split is not invisible: the same bytes divided in three different places are
    // three different hashes.
    assert_ne!(hash(b"ab", b"c"), hash(b"a", b"bc"));
    assert_ne!(hash(b"ab", b"c"), hash(b"abc", b""));
    assert_ne!(hash(b"", b"abc"), hash(b"abc", b""));
    // Same document, different key: a different hash, so a stale acknowledgement for one
    // key can never confirm the other.
    assert_ne!(hash(b"k1", b"x"), hash(b"k2", b"x"));
}

/// A field's type as the name the `.proto` writes it, for the checks above.
fn field_type(f: &prost_reflect::FieldDescriptor) -> &'static str {
    match f.kind() {
        prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => "uint64",
        prost_reflect::Kind::Int64
        | prost_reflect::Kind::Sint64
        | prost_reflect::Kind::Sfixed64 => "int64",
        prost_reflect::Kind::String => "string",
        prost_reflect::Kind::Bytes => "bytes",
        _ => "other",
    }
}

/// An unknown status number is `RETRY`, never `REFUSED`: a server that invents a status
/// must not cost anyone a key.
#[test]
fn unknown_status_is_retry() {
    assert_eq!(Status::from_wire(0), Status::Unspecified);
    assert_eq!(Status::from_wire(3), Status::Retry);
    assert_eq!(Status::from_wire(7), Status::Retry);
}
