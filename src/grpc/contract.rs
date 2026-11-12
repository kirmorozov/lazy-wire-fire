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

    /// The status as the `.proto` names it: what the `body` of an acknowledgement row holds, so
    /// that a rule's `expect` can say `r.body->>'$.status' = 'REFUSED'` and mean the same word
    /// the server sent.
    pub fn wire_name(self) -> &'static str {
        match self {
            Status::Unspecified => "STATUS_UNSPECIFIED",
            Status::Ok => "OK",
            Status::Refused => "REFUSED",
            Status::Retry => "RETRY",
        }
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

#[cfg(test)]
mod tests;
