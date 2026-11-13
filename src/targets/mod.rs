//! Target kinds: one kind per file. Each module here owns its kind's whole
//! behaviour — the endpoint and `vars` checks at load, the rule checks, the
//! client and the `send`/`send_items` paths. The outer contract is shared,
//! not per-kind: `send` answers `Sent`, `fetch` answers `Row`, every item
//! answers `Ok`/`Rejected`/`Transient`, and the dispatch that calls into
//! these modules is the matches in [`crate::target`]; `Kind` and its
//! property methods stay in [`crate::model`].
//!
//! Two shared modules are not kinds and so do not live here:
//! [`crate::http`] is the http client (the shared transport of many kinds, among them
//! the four plain http kinds of [`http`] below — the client and the kinds share the name
//! and are not the same thing), and [`crate::sql`] is SQL handling (placeholders,
//! expressions, DDL), used far beyond targets.

pub mod amqp;
pub mod eventbridge;
pub mod grpc;
pub mod http;
pub mod kafka;
pub mod kinesis;
pub mod lambda;
pub mod pubsub;
pub mod rabbitmq_stream;
pub mod redis;
pub mod s3;
pub mod servicebus;
pub mod smtp;
pub mod sns;
pub mod sql;
pub mod sqs;
