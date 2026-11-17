//! lazy_wire_fire (lazywf): row changes in registered tables mark keys; for the changed keys a
//! rule's queries run, one template renders an action, and it is fired at an HTTP, S3, queue,
//! stream, mail, Redis or SQL target. Rules, triggers and targets are rows in the tool's
//! tables.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

pub mod access;
pub mod action;
pub mod allow;
pub mod analyze;
pub mod analyzer;
pub mod aws;
pub mod binlog;
pub mod cascade;
pub mod catalog;
pub mod config;
pub mod datum;
pub mod dedup;
// The dialect seam is copied verbatim, and some of its answers serve callers that live in
// lazyview only (the mv layer's row normalization, its MySQL target's session setup and source
// reads): lazywf has no such call sites, so `pub(crate)` items here are never used here. They
// stay, byte for byte, for the day a lazywf caller appears; the allow is what keeps the copy
// unedited. (Same arrangement as the `#![allow(dead_code)]` copy-set files write for themselves.)
#[allow(dead_code)]
pub mod dialect;
pub mod docs;
pub mod engine;
pub mod fence;
pub mod filters;
pub mod gcp;
pub mod health;
pub mod http;
pub mod intset;
pub mod keyspace;
pub mod lock;
pub mod logging;
pub mod metrics;
pub mod model;
pub mod oauth;
pub mod path;
pub mod pgoutput;
pub mod response;
pub mod rows;
pub mod rule;
pub mod secrets;
pub mod sign;
// The source seam is copied verbatim, and its per-flavor visibility fence (FenceState, FenceReport,
// gtid_wait, CommitStep, commit_order, snapshot_position, snapshot_status) answers callers that live
// in lazyview only: lazywf has no call site for it yet, so `pub(crate)` items here are never used
// here. They stay, byte for byte, for the day a lazywf caller appears (the fence in `src/action.rs`
// is the candidate); the allow is what keeps the copy unedited, and everything under
// `src/source/**` is copy set, so it can never hide a lazywf-owned item. (Same arrangement as the
// `dialect` seam above, and the `#![allow(dead_code)]` copy-set files write for themselves.)
#[allow(dead_code)]
pub mod source;
pub mod sql;
pub mod sqlwire;
pub mod status;
pub mod target;
pub mod targets;
pub mod template;
pub mod time;
pub mod tls;

/// The test-only counting allocator (live bytes and per-thread allocation counts).
#[cfg(test)]
#[path = "tests/alloc_measure.rs"]
mod alloc_measure;
