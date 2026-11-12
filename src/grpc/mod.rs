//! The `grpc` kind: the rule's documents as protobuf messages, pushed onto one long-lived
//! bidirectional stream per endpoint, acknowledged item by item. The point is size: on the wire
//! a field name becomes a tag and a number a varint, so the same document is several times
//! smaller than the JSON an `http` target sends.
//!
//! # Endpoint and credentials
//!
//! `grpc://<host>[:port]/<package.Service>/<Method>`, or `grpcs://` for TLS (default ports 80 /
//! 443). One endpoint per target: an action's items go to a single endpoint, as for sqs, kinesis
//! and kafka. No credentials in the URL (userinfo is a load error): `vars.token` is a reference
//! (`env:NAME` or `file:/path`, never a literal) whose value goes out as the `authorization:
//! Bearer <token>` metadata of every call. `tls` holds the TLS modes, as for `http` (see
//! [`crate::tls`]).
//!
//! # The schema: a descriptor set, not codegen
//!
//! A schema is configuration, so nothing is generated at build time and no `protoc` is needed:
//!
//! - `vars.descriptor_set` is a `file:` reference to a protobuf descriptor set (the
//!   `FileDescriptorSet` of `protoc --descriptor_set_out`, or of any other compiler), read when
//!   the target loads;
//! - `vars.item_type` names the message a document becomes (`catalog.v1.Item`), a message of
//!   that descriptor set.
//!
//! Messages are built and encoded at run time from the descriptors (prost + prost-reflect). A
//! descriptor set replaced in place is read when the target's row changes: touch the row (any
//! column) to make lazywf read new bytes.
//!
//! lazywf's own part of the wire is fixed and ships with the tool:
//! `proto/lazywf/v1/lazywf.proto` and its compiled form beside it (a unit test proves the two
//! agree, so neither needs `protoc` at build time). The method takes `stream lazywf.v1.Envelope`
//! and returns `stream lazywf.v1.Acks`: each item carries its per-stream sequence number, the
//! xxh3-64 hash of its encoded document, its document key, and the encoded document itself; the
//! server acknowledges by sequence number and echoes the hash (see [`contract`] for why a hash,
//! and the module's stream half for the stream). The pool a method is looked up in always holds
//! lazywf's own contract first, so a deployment's descriptor set only has to describe its item.
//!
//! # The rendered action
//!
//! The template renders the rule's documents, which are the items — one per key, in the order
//! the engine passes the keys:
//!
//! ```text
//! POST / HTTP/1.1
//! Content-Type: application/json
//!
//! {{json docs}}
//! ```
//!
//! The body is the `docs` array: `[{"key": …, "doc": {…}, "op": …}, …]`. Item i is `doc` of
//! element i, whose `key` must be `keys[i]`; `op` is not sent separately (an `op` field of the
//! item message is filled by an `_op` column like any other field). The item message is filled
//! by field name from the document, as [`schema`] describes.
//!
//! # Checked when the rule loads
//!
//! A rule's selects are prepared already, and a prepare answers with every column's name and
//! type without running the query. When the rule's target is `grpc`, each column is checked
//! against the item message (see [`schema::column_errors`]):
//!
//! - a column whose document key is not a field of the message is refused, naming the column and
//!   the fields it does have;
//! - a column whose type the field cannot hold is refused, naming both and what to do (a decimal
//!   column for an `int64` field, a scalar for a `repeated` field, a binary column for a `bytes`
//!   field, which wants `TO_BASE64`);
//! - a message field no column provides is a WARN: proto3 sends its default.
//!
//! So a schema that does not fit the rule never surfaces as refused documents at run time. What
//! a prepare cannot show is the *content* of a value: an enum filled from a text column is only
//! known to be wrong when a row is read, and that stays a transient failure, so the key waits
//! and goes again rather than being lost.
//!
//! # Outcomes
//!
//! Per item, from the acknowledgement (the outer contract of every kind, see [`crate::target`]):
//!
//! - `OK`: sent; `expect` then sees `status` 200, `text` `ok` and `body` the acknowledgement as
//!   JSON (`{"seq", "hash", "status", "reason"}`).
//! - `REFUSED`: refused (a strike, as the service has judged this document), with `status` 400,
//!   `text` `refused` and the same row.
//! - `RETRY`: transient: the key waits and is sent again.
//! - a whole call that fails, an acknowledgement that never comes, an acknowledgement lazywf
//!   cannot use (no status, an unknown sequence number, a hash that is not the one it sent):
//!   transient. Only an answer that is positively a verdict on this item may refuse it, so a
//!   key cannot be lost to a server lazywf cannot read.
//! - the gRPC status of a whole call: `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`,
//!   `NOT_FOUND`, `ALREADY_EXISTS`, `PERMISSION_DENIED`, `UNAUTHENTICATED` refuse the action;
//!   every other code (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`,
//!   `INTERNAL`, `DATA_LOSS`, `UNIMPLEMENTED`, …) is transient, as is anything unknown.

use anyhow::{bail, ensure, Context as _, Result};
use prost::Message as _;
use serde_json::{Map, Value};

use crate::docs;
use crate::model::{FirePer, RuleDef, TargetDef};
use crate::response::{Expect, Row, Uses};
use crate::secrets;
use crate::target::{rejected, var, Item, Sent};
use crate::tls::{self, Mode, TlsSpec};

pub mod contract;
pub mod schema;
mod stream;
mod wire;

use stream::{Call, Endpoint0, Outbound, Shared, Verdict};
use wire::Codec;

use schema::{pool, Method};

/// The default ports (plain / TLS).
const DEFAULT_PORT: u16 = 80;
const DEFAULT_SECURE_PORT: u16 = 443;

/// The largest message a server takes by default (the gRPC implementations' own limit;
/// `vars.max_message_bytes` raises it).
pub const MAX_MESSAGE_BYTES: usize = 4_194_304;

/// The size a message has to reach before compressing it pays (`vars.compress_min_bytes`):
/// below it the CPU costs more than the bytes saved.
const COMPRESS_MIN_BYTES: usize = 1_024;

/// An endpoint: `grpc(s)://<host>[:port]/<package.Service>/<Method>`, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub secure: bool,
    pub host: String,
    pub port: u16,
    /// The service to call, as written: `catalog.v1.Catalog`.
    pub service: String,
    pub method: String,
}

impl Addr {
    pub fn parse(s: &str) -> Result<Addr> {
        let shown = crate::secrets::redact_url(s);
        let s = s.trim();
        let (secure, rest) = if let Some(r) = s.strip_prefix("grpcs://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("grpc://") {
            (false, r)
        } else {
            bail!("endpoint `{shown}`: use grpc://host:port/package.Service/Method or grpcs://…");
        };
        if rest.contains('@') {
            bail!(
                "endpoint `{shown}`: credentials belong in vars.token (a reference), not in the \
                 URL"
            );
        }
        if rest.contains(['?', '#']) {
            bail!("endpoint `{shown}`: no query string or fragment (TLS is the target's `tls`)");
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        if authority.starts_with('[') {
            bail!("endpoint `{shown}`: IPv6 addresses are not supported");
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h,
                p.parse::<u16>()
                    .with_context(|| format!("endpoint `{shown}`: port `{p}`"))?,
            ),
            None if authority.is_empty() => bail!("endpoint `{shown}`: no host"),
            None => (
                authority,
                if secure {
                    DEFAULT_SECURE_PORT
                } else {
                    DEFAULT_PORT
                },
            ),
        };
        if host.is_empty() || host.bytes().all(|c| matches!(c, b'-' | b'.')) {
            bail!("endpoint `{shown}`: no host");
        }
        let mut parts = path.split('/');
        let (service, method, extra) = (
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
            parts.next(),
        );
        if extra.is_some() || service.is_empty() || method.is_empty() {
            bail!("endpoint `{shown}`: the path is /<package.Service>/<Method>");
        }
        for part in service.split('.') {
            check_identifier(part, "service", &shown)?;
        }
        check_identifier(method, "method", &shown)?;
        Ok(Addr {
            secure,
            host: host.to_string(),
            port,
            service: service.to_string(),
            method: method.to_string(),
        })
    }

    /// The endpoint as it should be written: what `endpoint.url` holds and messages name.
    pub fn url(&self) -> String {
        format!(
            "{}{}{}",
            if self.secure { "grpcs://" } else { "grpc://" },
            self.authority(),
            self.path()
        )
    }

    /// The HTTP/2 `:authority` and the address lazywf connects to.
    pub fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// The gRPC path of the call: `/<package.Service>/<Method>`, the HTTP/2 `:path`.
    pub fn path(&self) -> String {
        format!("/{}/{}", self.service, self.method)
    }
}

/// A protobuf identifier: a letter or `_`, then letters, digits or `_`.
fn check_identifier(s: &str, what: &str, shown: &str) -> Result<()> {
    let ok = !s.is_empty()
        && s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    ensure!(
        ok,
        "endpoint `{shown}`: {what} `{s}` is not a protobuf name (letters, digits and `_`, \
         starting with a letter or `_`)"
    );
    Ok(())
}

/// What a `grpc` target was configured with, read once when the target loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// The bearer token reference of `vars.token` (`None`: no `authorization` metadata). Never
    /// the value: it is resolved per call, so a rotated file is picked up without a reload.
    pub token: Option<String>,
    /// What `vars.compression` asks for.
    pub compression: Compression,
    /// The size a message must reach before it is worth compressing.
    pub compress_min_bytes: usize,
    /// The largest message the server takes.
    pub max_message_bytes: usize,
}

/// What lazywf may compress its messages with (`vars.compression`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// zstd when the server accepts it, gzip when it does not (neither when it accepts
    /// neither), as `vars.compression` is unset.
    #[default]
    Auto,
    None,
    Gzip,
    Zstd,
}

impl Compression {
    fn parse(v: Option<&str>) -> Result<Compression> {
        match v.unwrap_or("").trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Compression::Auto),
            "none" | "identity" => Ok(Compression::None),
            "gzip" => Ok(Compression::Gzip),
            "zstd" => Ok(Compression::Zstd),
            other => bail!("`vars.compression` `{other}` (use auto, none, gzip or zstd)"),
        }
    }

    /// The name in `grpc-encoding` / `grpc-accept-encoding`.
    pub fn wire_name(self) -> &'static str {
        match self {
            Compression::Auto | Compression::None => "identity",
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
        }
    }
}

/// A count of bytes from vars: a JSON number or a string of digits, or the default.
fn bytes_var(vars: &Map<String, Value>, k: &str, default: usize) -> Result<usize> {
    match vars.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(|n| n as usize)
            .with_context(|| format!("`vars.{k}` is not a count of bytes")),
        Some(Value::String(s)) => s
            .trim()
            .parse::<usize>()
            .with_context(|| format!("`vars.{k}`")),
        Some(other) => bail!("`vars.{k}` {other} is not a count of bytes"),
    }
}

/// The target's call options, from its vars.
pub fn options(vars: &Map<String, Value>) -> Result<Options> {
    let opts = Options {
        token: var(vars, "token").map(str::to_string),
        compression: Compression::parse(var(vars, "compression"))?,
        compress_min_bytes: bytes_var(vars, "compress_min_bytes", COMPRESS_MIN_BYTES)?,
        max_message_bytes: bytes_var(vars, "max_message_bytes", MAX_MESSAGE_BYTES)?,
    };
    ensure!(
        opts.max_message_bytes >= 64,
        "`vars.max_message_bytes` {} is smaller than one item can be",
        opts.max_message_bytes
    );
    Ok(opts)
}

/// An endpoint of this kind (the other kinds' schemes are refused by [`refuse_scheme`]).
pub fn is_grpc_url(endpoint: &str) -> bool {
    let e = endpoint.trim();
    e.starts_with("grpc://") || e.starts_with("grpcs://")
}

/// A `grpc(s)://` endpoint belongs to kind `grpc`, and kind `grpc` takes nothing else: the two
/// mistakes that would leave a target looking configured while calling the wrong thing.
pub fn refuse_scheme(is_grpc: bool, endpoints: &[String]) -> Result<()> {
    match (is_grpc, endpoints.iter().find(|e| is_grpc_url(e))) {
        (false, Some(e)) => bail!("`{e}` is an endpoint of kind grpc, not of kind another"),
        (true, None) => bail!(
            "kind grpc takes grpc://host:port/package.Service/Method or grpcs://… (TLS is the \
             scheme or `tls`)"
        ),
        _ => Ok(()),
    }
}

/// The TLS mode of an endpoint: `None` for `grpc://`, the mode `tls` resolves to for `grpcs://`.
/// A `grpcs://` endpoint with TLS disabled is refused: the scheme is the owner's statement that
/// the port speaks TLS, and sending plaintext there is a bug, not a fallback.
fn tls_mode(spec: &TlsSpec, secure: bool) -> Result<Option<Mode>> {
    if !secure {
        return Ok(None);
    }
    match spec.effective(true) {
        Mode::Disabled => bail!("a grpcs:// endpoint needs TLS, but `tls` says disabled"),
        Mode::Auto => bail!("tls mode `auto` was not resolved"),
        m => Ok(Some(m)),
    }
}

/// One endpoint of a `grpc` target: its address, the method it calls, and how to call it.
pub struct Client {
    /// The endpoint as written: the name in messages.
    pub url: String,
    pub addr: Addr,
    /// The method, with the message an item's document becomes.
    pub method: Method,
    /// The descriptors the method's types live in, kept for the stream to build from.
    pub pool: prost_reflect::DescriptorPool,
    pub opts: Options,
    /// The TLS mode (`None`: `grpc://`).
    pub mode: Option<Mode>,
    pub tls: TlsSpec,
    pub timeout: std::time::Duration,
    /// The one stream of this endpoint, opened by the first flush and kept after that (see
    /// [`stream`]).
    call: Call,
}

impl Client {
    /// Loads the target's descriptor set, checks the method and the item type against it, and
    /// builds the endpoint's context (`endpoint` in templates) and its client.
    ///
    /// Everything the schema needs is read here, at load: a descriptor set that is missing,
    /// unreadable, not a descriptor set, or does not hold the service, the method or the item
    /// type is a target that never started, said so, and says it again on every reload — rather
    /// than a target that connects and then has nothing to say.
    pub fn new(def: &TargetDef, endpoint: &str) -> Result<(Value, Client)> {
        let addr = Addr::parse(endpoint).context("`endpoints`")?;
        let mode = tls_mode(&def.tls, addr.secure)?;
        let item_type = var(&def.vars, "item_type")
            .ok_or_else(|| anyhow::anyhow!("kind grpc needs `vars.item_type` (the message a document becomes, e.g. catalog.v1.Item)"))?;
        let ds = var(&def.vars, "descriptor_set").ok_or_else(|| {
            anyhow::anyhow!(
                "kind grpc needs `vars.descriptor_set` (a `file:` reference to a descriptor set)"
            )
        })?;
        let bytes = secrets::read_bytes("vars.descriptor_set", ds)
            .with_context(|| format!("descriptor set for `vars.item_type` `{item_type}`"))?;
        let pool = pool(Some(&bytes)).with_context(|| format!("`vars.descriptor_set` `{ds}`"))?;
        let method = schema::check_method(&pool, &addr.service, &addr.method, Some(item_type))
            .with_context(|| format!("`{}`", addr.url()))?;
        let ctx = serde_json::json!({
            "host": addr.host,
            "port": addr.port,
            "url": addr.url(),
            "service": addr.service,
            "method": addr.method,
            "item_type": item_type,
        });
        // The connector is built once, at load: a `tls` that names an unreadable CA is the
        // target's load error, not the first flush's. ALPN asks for HTTP/2, which is all gRPC
        // speaks.
        let connector = tls::Connector::new(&addr.host, addr.port, &def.tls, addr.secure, &[b"h2"])
            .with_context(|| format!("`{}`", addr.url()))?;
        let opts = options(&def.vars)?;
        let call = Call::new(std::sync::Arc::new(Shared::new(Endpoint0 {
            url: addr.url(),
            path: addr.path(),
            secure: addr.secure,
            host: addr.host.clone(),
            port: addr.port,
            connector,
            token: opts.token.clone(),
            compression: match opts.compression {
                Compression::Auto => None,
                Compression::None => Some(Codec::Identity),
                Compression::Gzip => Some(Codec::Gzip),
                Compression::Zstd => Some(Codec::Zstd),
            },
            min_bytes: opts.compress_min_bytes,
            max_message_bytes: opts.max_message_bytes,
            timeout: def.timeout,
        })));
        Ok((
            ctx,
            Client {
                url: addr.url(),
                addr,
                method,
                pool,
                opts,
                mode,
                tls: def.tls.clone(),
                timeout: def.timeout,
                call,
            },
        ))
    }
}

/// Load checks for a target of kind `grpc` (from [`crate::model::TargetDef::from_row`]): its
/// endpoint, its TLS mode, its schema vars and its call options.
pub fn check(endpoints: &[String], vars: &Map<String, Value>, tls: Option<&str>) -> Result<()> {
    match endpoints {
        [url] => {
            let addr = Addr::parse(url)?;
            tls_mode(&TlsSpec::parse(tls)?, addr.secure)?;
        }
        _ => bail!(
            "kind grpc takes exactly one endpoint: \
             grpc://host:port/package.Service/Method"
        ),
    }
    for (k, need) in [
        (
            "descriptor_set",
            "a `file:` reference to a protobuf descriptor set",
        ),
        (
            "item_type",
            "the message a document becomes, by its full name, e.g. catalog.v1.Item",
        ),
    ] {
        ensure!(
            var(vars, k).is_some(),
            "kind grpc needs `vars.{k}` ({need})"
        );
    }
    let ds = var(vars, "descriptor_set").unwrap_or_default();
    ensure!(
        ds.starts_with("file:"),
        "`vars.descriptor_set` `{ds}` is not a `file:` reference to a descriptor set (a schema \
         is a file, never a value in the database)"
    );
    if let Some(t) = var(vars, "token") {
        ensure!(
            secrets::is_reference(t),
            "`vars.token` must be a reference (`env:NAME` or `file:/path`), never a literal"
        );
    }
    // A var lazywf does not read would silently do nothing, and these are the names an owner
    // reaches for when the schema is described elsewhere than here.
    for k in [
        "schema",
        "schema_ref",
        "message",
        "message_type",
        "request_type",
        "response_type",
        "service",
        "method",
    ] {
        ensure!(
            !vars.contains_key(k),
            "`vars.{k}` is not used by kind grpc: the schema is `vars.descriptor_set` (a `file:` \
             reference), the message is `vars.item_type`, and the service and method are the path \
             of the endpoint"
        );
    }
    options(vars)?;
    Ok(())
}

/// Sends a `once` reset action of a `grpc` target (no keys).
///
/// A `once` step has no keys, so what it sends is whatever its template rendered: the same
/// `docs` array as an action's, whose `key` fields are only the server's deduplication label
/// here. Nothing rendered is [`Sent::Skipped`], as every other kind answers a step that rendered
/// nothing.
pub async fn send(
    client: &Client,
    rendered: &str,
    expect: &Expect,
    pool: &mysql_async::Pool,
) -> Result<Sent> {
    let items = items_of(client, rendered, &[])?;
    if items.is_empty() {
        return Ok(Sent::Skipped);
    }
    let out = client.call.push(&items).await;
    if let Some(why) = out.refused {
        return Err(rejected(format!("`{}`: {why}", client.url)));
    }
    // One answer for the whole step: any item the server would not take fails it, and any item it
    // said nothing about leaves the step to be tried again. A `once` step has no keys to lose.
    let mut ack = None;
    for v in &out.verdicts {
        match v {
            Verdict::Ack {
                seq,
                hash,
                status,
                reason,
            } => match status {
                contract::Status::Ok => {
                    if ack.is_none() {
                        ack = Some((*seq, *hash, *status, reason.clone()));
                    }
                }
                contract::Status::Refused => {
                    return Err(rejected(format!(
                        "`{}`: the server refused item {seq}: {reason}",
                        client.url
                    )))
                }
                contract::Status::Retry => {
                    bail!(
                        "`{}`: the server asked for item {seq} again: {reason}",
                        client.url
                    )
                }
                contract::Status::Unspecified => {
                    unreachable!("an ack with no status is never a verdict")
                }
            },
            Verdict::Lost(why) => bail!("`{}`: {why}", client.url),
        }
    }
    // The step's row is the acknowledgement of the first item: what `expect` holds a `once` step
    // to, as for every other kind.
    let (seq, hash, status, reason) = ack.ok_or_else(|| {
        rejected(format!(
            "`{}`: the server acknowledged none of the {} items",
            client.url,
            out.verdicts.len()
        ))
    })?;
    expect
        .row(pool, |u| ack_row(u, seq, hash, status, &reason))
        .await
        .with_context(|| format!("`{}`", client.url))?
        .map_err(|e| rejected(format!("`{}`: {e}", client.url)))?;
    Ok(Sent::Ok)
}

/// Sends one action of a `grpc` target (item i is the document of key i).
///
/// The items go onto the target's stream as one envelope and the server's acknowledgements decide
/// the outcome of each key: see the module's outcomes. A verdict of the whole call (a status that
/// refuses, an HTTP peer that answered a proxy's own no) is the action's error and refuses every
/// key at once; anything the server left unanswered is that key's own transient, so a reconnect
/// costs the keys the server had already taken nothing.
pub async fn send_items(
    client: &Client,
    rendered: &str,
    keys: &[String],
    expect: &Expect,
    pool: &mysql_async::Pool,
) -> Result<Vec<Item>> {
    let items = items_of(client, rendered, keys)?;
    if items.is_empty() {
        return Ok(vec![Item::Ok; keys.len()]);
    }
    let out = client.call.push(&items).await;
    if let Some(why) = out.refused {
        return Err(rejected(format!("`{}`: {why}", client.url)));
    }
    let mut out_items = Vec::with_capacity(out.verdicts.len());
    for (i, v) in out.verdicts.iter().enumerate() {
        let at = format!("item {i} (key `{}`) of {}", keys[i], client.url);
        out_items.push(match v {
            Verdict::Ack {
                seq,
                hash,
                status,
                reason,
            } => match status {
                contract::Status::Ok => match expect
                    .row(pool, |u| ack_row(u, *seq, *hash, *status, reason))
                    .await
                    .with_context(|| at.clone())?
                {
                    Ok(()) => Item::Ok,
                    Err(why) => Item::Rejected(format!("{at}: {why}")),
                },
                contract::Status::Refused => {
                    Item::Rejected(format!("{at}: the server refused it: {reason}"))
                }
                contract::Status::Retry => {
                    Item::Transient(format!("{at}: the server asked for it again: {reason}"))
                }
                contract::Status::Unspecified => {
                    unreachable!("an ack with no status is never a verdict")
                }
            },
            Verdict::Lost(why) => Item::Transient(format!("{at}: {why}")),
        });
    }
    Ok(out_items)
}

/// The row `r` of one acknowledgement: `status` 200 and the acknowledgement as JSON for what the
/// server took, 400 and `refused` for what it would not. The rule's `expect` reads `body`, so the
/// `seq`, the `hash` and the server's `reason` are what a rule can hold the server to.
fn ack_row(uses: Uses, seq: u64, hash: u64, status: contract::Status, reason: &str) -> Row {
    let ok = status == contract::Status::Ok;
    let body = serde_json::json!({
        "seq": seq.to_string(),
        "hash": hash.to_string(),
        "status": status.wire_name(),
        "reason": reason,
    });
    Row::grpc(
        uses,
        if ok { 200 } else { 400 },
        if ok { "ok" } else { "refused" },
        &body,
    )
}

/// The rendered action's items: element i of the `docs` array becomes item i, its `doc` filled
/// onto the target's item message field by field. `keys` is empty for a `once` step, which has no
/// keys to line the items up against.
///
/// A body that is not the `docs` array, a document that does not fit the schema, and an item
/// count that is not the key count are all refused: the template or the schema is wrong, and
/// sending again would be wrong again.
fn items_of(client: &Client, rendered: &str, keys: &[String]) -> Result<Vec<Outbound>> {
    if rendered.trim().is_empty() {
        return Ok(Vec::new());
    }
    let body: Value = serde_json::from_str(rendered)
        .map_err(|e| rejected(format!("the body of `{}` is not JSON: {e}", client.url)))?;
    let docs = body.as_array().ok_or_else(|| {
        rejected(format!(
            "the body of `{}` must be the `docs` array",
            client.url
        ))
    })?;
    if !keys.is_empty() && docs.len() != keys.len() {
        return Err(rejected(format!(
            "the body of `{}` lists {} documents for {} keys (document i is the item of key i)",
            client.url,
            docs.len(),
            keys.len()
        )));
    }
    let body_of = client.method.body.as_ref().ok_or_else(|| {
        rejected(format!(
            "`{}` has no `vars.item_type`, so a document has nothing to become",
            client.url
        ))
    })?;
    let mut out = Vec::with_capacity(docs.len());
    for (i, d) in docs.iter().enumerate() {
        let at = format!(
            "document {} of `{}`",
            i,
            keys.get(i).map(String::as_str).unwrap_or("?")
        );
        let doc = d
            .as_object()
            .ok_or_else(|| rejected(format!("{at} is not an object")))?;
        let key = doc
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| rejected(format!("{at} has no `key` string")))?;
        if let Some(want) = keys.get(i) {
            if key != want {
                return Err(rejected(format!(
                    "{at}: its `key` is `{key}`, not the key `{want}` the engine gave"
                )));
            }
        }
        let doc = doc
            .get("doc")
            .and_then(Value::as_object)
            .ok_or_else(|| rejected(format!("{at} has no `doc` object")))?;
        let message = schema::build(body_of, doc)
            .with_context(|| format!("{at}, as a `{}`", body_of.full_name()))
            .map_err(|e| rejected(format!("{e:#}")))?;
        out.push(Outbound {
            key: key.to_string(),
            body: message.encode_to_vec(),
        });
    }
    Ok(out)
}

/// Rule-load refusals of a `grpc` rule (called from [`crate::rule::kind_errors`]): the rule's
/// columns against the target's item message, and a batch over the largest message the target
/// takes.
pub fn rule_errors(def: &RuleDef, target: &TargetDef, fields: &[docs::Field]) -> Vec<String> {
    let mut out = Vec::new();
    let limit = options(&target.vars)
        .map(|o| o.max_message_bytes)
        .unwrap_or(MAX_MESSAGE_BYTES);
    if def.fire_per == FirePer::Batch && def.batch_max_bytes.is_some_and(|n| n > limit) {
        out.push(format!(
            "`batch_max_bytes` {} is over {limit}, the largest message the target takes \
             (`vars.max_message_bytes`)",
            def.batch_max_bytes.unwrap()
        ));
    }
    let Some(url) = target.endpoints.first() else {
        return out;
    };
    // The schema check needs the descriptor set, which is a file: an unreadable one is the
    // target's own load error (already reported there), so here it is only reported, not
    // repeated as a rule error.
    match schema_of(target, url) {
        Err(e) => out.push(format!("{e:#}")),
        Ok((pool, addr)) => {
            let item = var(&target.vars, "item_type").unwrap_or_default();
            match schema::check_method(&pool, &addr.service, &addr.method, Some(item)) {
                Err(e) => out.push(format!("{e:#}")),
                Ok(m) => {
                    if let Some(body) = &m.body {
                        out.extend(schema::column_errors(body, fields));
                        for w in schema::uncovered(body, fields) {
                            tracing::warn!(
                                rule = %def.name,
                                target = %target.name,
                                item_type = %body.full_name(),
                                "grpc schema: {w}"
                            );
                        }
                    }
                }
            }
        }
    }
    out
}

/// The target's descriptor pool and endpoint address, for a check that does not build a client.
fn schema_of(target: &TargetDef, url: &str) -> Result<(prost_reflect::DescriptorPool, Addr)> {
    let addr = Addr::parse(url)?;
    let ds = var(&target.vars, "descriptor_set")
        .ok_or_else(|| anyhow::anyhow!("kind grpc needs `vars.descriptor_set`"))?;
    let bytes = secrets::read_bytes("vars.descriptor_set", ds)?;
    Ok((pool(Some(&bytes))?, addr))
}

#[cfg(test)]
mod tests {
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
            let vars = format!(
                r#"{{"descriptor_set":"file:{ds}","item_type":"orders.v1.Item","{k}":"{v}"}}"#
            );
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
}
