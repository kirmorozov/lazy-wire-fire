//! lazy_wire_fire (lazywf): row changes in registered tables mark keys; for the changed keys a
//! rule's queries run, one template renders an action, and it is fired at an HTTP, S3, queue,
//! stream, mail, Redis or SQL target. Rules, triggers and targets are rows in the tool's
//! tables.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

pub mod access;
pub mod action;
pub mod allow;
pub mod amqp;
pub mod analyze;
pub mod analyzer;
pub mod aws;
pub mod binlog;
pub mod catalog;
pub mod config;
pub mod datum;
pub mod dedup;
pub mod docs;
pub mod engine;
pub mod fence;
pub mod filters;
pub mod gcp;
pub mod grpc;
pub mod health;
pub mod http;
pub mod intset;
pub mod kafka;
pub mod keyspace;
pub mod lock;
pub mod logging;
pub mod model;
pub mod path;
pub mod pubsub;
pub mod rabbitmq_stream;
pub mod redis;
pub mod response;
pub mod rows;
pub mod rule;
pub mod s3;
pub mod secrets;
pub mod servicebus;
pub mod sign;
pub mod smtp;
pub mod sns;
pub mod sql;
pub mod sqlwire;
pub mod status;
pub mod target;
pub mod template;
pub mod time;
pub mod tls;
