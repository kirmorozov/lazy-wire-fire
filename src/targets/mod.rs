//! Target kinds: one kind per file. Each module here owns its kind's whole
//! behaviour — the endpoint and `vars` checks at load, the rule checks, the
//! client and the `send`/`send_items` paths. The outer contract is shared,
//! not per-kind: `send` answers `Sent`, `fetch` answers `Row`, every item
//! answers `Ok`/`Rejected`/`Transient`, and the dispatch that calls into
//! these modules is the matches in [`crate::target`]; `Kind` and its
//! property methods stay in [`crate::model`].
//!
//! Two shared modules are not kinds and so do not live here:
//! [`crate::http`] is the http client (the shared transport of many kinds),
//! and [`crate::sql`] is SQL handling (placeholders, expressions, DDL),
//! used far beyond targets.

pub mod amqp;
pub mod grpc;
pub mod kafka;
pub mod pubsub;
pub mod rabbitmq_stream;
pub mod servicebus;
pub mod smtp;
