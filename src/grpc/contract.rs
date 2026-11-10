//! lazywf's own gRPC contract: the method a `grpc` target pushes to, and the two message
//! shapes it exchanges.
//!
//! The wire type is always [`Envelope`] and the reply stream is always [`Acks`]; what an
//! [`Item`] carries is the schema, and that is configuration. The server may name any input
//! and output message in the descriptor set it ships, but each entry of `Envelope.items`
//! still means the same thing, and each [`Ack`] still names the `seq` and `hash` of the item
//! it answers. So the descriptor set is a schema lazywf reads, not a contract it has to be
//! given, and the pool it checks a target's method against always holds this file.
//!
//! `proto/lazywf/v1/lazywf.proto` is the source of truth; `lazywf.desc` beside it is the same
//! file compiled, which is what a deployment ships to a server that wants lazywf's own shape.
//! A test compiles the `.proto` with `protox` and compares the two, so the committed
//! descriptor cannot drift, and no build of lazywf needs a protoc.

use std::hash::Hasher as _;

/// One item of an envelope: the position, the content hash, the key, the body.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Item {
    /// The position of this item on this stream: from 1, by ones, restarting at 1 when the
    /// stream reconnects. The `Ack` answers it.
    #[prost(uint64, tag = "1")]
    pub seq: u64,
    /// xxh3-64 of `key` then `item`, the key's length written first (see [`hash`]). The `Ack`
    /// echoes it, so a stale or mixed-up acknowledgement cannot confirm an item it was not for.
    #[prost(uint64, tag = "2")]
    pub hash: u64,
    /// The key, as text: the same string the doc's `key` field holds. A server that sees a key
    /// and a hash it has already stored may acknowledge the duplicate without processing it
    /// again.
    #[prost(string, tag = "3")]
    pub key: String,
    /// The item message from the target's schema, encoded as protobuf: the rule's document
    /// mapped onto it field by field. Bytes rather than a `google.protobuf.Struct` because
    /// the whole point of the kind is that the schema is on the wire: field names become
    /// tags and numbers varints.
    #[prost(bytes = "vec", tag = "4")]
    pub item: Vec<u8>,
}

/// One push. The stream carries any number of these; each is a batch.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Envelope {
    #[prost(message, repeated, tag = "1")]
    pub items: Vec<Item>,
}

/// What the server says about one item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    /// Absent, or a value lazywf does not know: lazywf keeps the item and asks again.
    Unspecified = 0,
    /// The server took it.
    Ok = 1,
    /// It will not take this item however often it is asked: the key goes to the DLQ with
    /// `reason` as its status text.
    Refused = 2,
    /// Not now: the key goes back to the queue with the target's retry delay.
    Retry = 3,
}

impl Status {
    /// The status as the wire names it; an unknown number is `RETRY`, the one that cannot
    /// lose a key.
    pub fn from_wire(number: i32) -> Self {
        Status::try_from(number).unwrap_or(Status::Retry)
    }
}

/// The answer for one item, named by `seq` and `hash`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ack {
    /// The `seq` of the item this answers: its position on the stream, not within one envelope.
    #[prost(uint64, tag = "1")]
    pub seq: u64,
    /// The `hash` the item carried. An ack whose hash is not the one the item had is a
    /// server bug: lazywf keeps the item and asks again.
    #[prost(uint64, tag = "2")]
    pub hash: u64,
    #[prost(enumeration = "Status", tag = "3")]
    pub status: i32,
    /// Why it was refused, or why it should be retried: the status text of the key.
    #[prost(string, tag = "4")]
    pub reason: String,
}

/// One reply frame: any number of acks, in any order.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Acks {
    #[prost(message, repeated, tag = "1")]
    pub acks: Vec<Ack>,
}

/// The content hash of an item: xxh3-64 (default seed) of the key's length as 8 little-endian
/// bytes, then the key, then the body.
///
/// Pure Rust and fast; the point is that a server can check the body it was handed against the
/// hash its ack echoes, so a mix-up inside a batch is caught rather than accepted. The key
/// counts, so two keys carrying the same document cannot confirm each other, and its length is
/// written first so that the boundary between the two parts is part of the hash: `("ab", "c")`
/// and `("a", "bc")` are different content, as a server that re-hashes what it was given must
/// agree.
pub fn hash(key: &[u8], item: &[u8]) -> u64 {
    let mut h = twox_hash::xxhash3_64::Hasher::new();
    h.write(&(key.len() as u64).to_le_bytes());
    h.write(key);
    h.write(item);
    h.finish()
}

/// lazywf's own compiled contract, as a `FileDescriptorSet`: what a server that wants to
/// implement the method directly can generate from, and what the descriptor pool of every
/// `grpc` target is seeded with.
pub const DESCRIPTOR: &[u8] = include_bytes!("../../proto/lazywf/v1/lazywf.desc");

/// The protobuf full name of the envelope, so the module never repeats it.
pub const ENVELOPE: &str = "lazywf.v1.Envelope";
/// The protobuf full name of the ack list.
pub const ACKS: &str = "lazywf.v1.Acks";

/// The package and service of the method lazywf asks for.
pub const SERVICE: &str = "lazywf.v1.Sink";
/// The method it asks for.
pub const METHOD: &str = "Push";

/// The method's full name, as the endpoint names it.
pub const FULL_METHOD: &str = "lazywf.v1.Sink/Push";

/// The directory the contract's `.proto` files live in, as a compiler include path.
#[cfg(test)]
pub const PROTO_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/proto");

/// Compiles `proto/lazywf/v1/lazywf.proto` the way the committed descriptor was made:
/// `protox`, pure Rust, no source info, no protoc.
#[cfg(test)]
pub fn compile_proto() -> Vec<u8> {
    protox::Compiler::new([PROTO_DIR])
        .expect("the proto directory opens")
        .include_source_info(false)
        .include_imports(true)
        .open_files(["lazywf/v1/lazywf.proto"])
        .expect("the shipped .proto compiles")
        .encode_file_descriptor_set()
}

#[cfg(test)]
mod tests {
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
}
