//! lazy_wire_fire (lazywf): row changes in registered tables mark keys; for the changed keys a
//! rule's queries run, one template renders an action, and it is fired at an HTTP or Redis
//! target. Rules, triggers and targets are rows in the tool's tables.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

pub mod action;
pub mod aws;
pub mod binlog;
pub mod config;
pub mod docs;
pub mod engine;
pub mod expect;
pub mod health;
pub mod http;
pub mod lock;
pub mod model;
pub mod path;
pub mod redis;
pub mod rows;
pub mod rule;
pub mod s3;
pub mod secrets;
pub mod sql;
pub mod status;
pub mod target;
pub mod template;
pub mod time;
pub mod tls;
