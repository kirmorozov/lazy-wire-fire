//! The `grpc` test server and the schema it serves, for the suites that drive the kind:
//! `tests/grpc_stream.rs` (what happens on the wire) and the `grpc` flow of
//! `tests/log_secrets.rs` (what the logs may say while it happens).
//!
//! The server is hand-rolled on `h2` rather than a gRPC framework — not to avoid a dependency
//! but because what the kind has to be shown is the parts a framework hides, and some of them a
//! framework would not let a test *do*: a proxy answering HTTP 404 of its own, a stream that ends
//! without a `grpc-status`, a stream reset mid-flight, an acknowledgement echoing a hash it was
//! not sent, a server without zstd. The client half is exactly the one the tool ships; the wire
//! between them is plain gRPC, so the server says nothing the client could not meet in a real
//! deployment.
//!
//! The schema is one message of the app's own package and a service of the contract's shape in
//! another, the way a real deployment's descriptor set is laid out: [`write_schema`] compiles it
//! and [`Server`] speaks it, so a target of these tests names [`ITEM_TYPE`] as its item type and
//! reaches [`METHOD`].

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::HeaderMap;
use lazy_wire_fire::targets::grpc::contract;
use prost::Message as _;
use serde_json::{json, Value};

/// The method the server serves: the contract's signature under a service of the app's own
/// package. A target's endpoint names it; a call for any other path is refused `UNIMPLEMENTED`.
pub const METHOD: &str = "catalog.v1.Ingest/Push";

/// The message a document becomes: what such a target's `vars.item_type` holds.
pub const ITEM_TYPE: &str = "orders.v1.Item";

/// Writes the schema's `.proto` files into `dir` and compiles them with `protox` (so no build of
/// lazywf, and no test, needs a protoc), and returns the path of the descriptor set — the value
/// of `vars.descriptor_set`, as `file:<path>`. `dir` has to be one the secret references may read
/// (`LAZYWF_SECRET_DIRS`), and be a directory this test binary alone writes: the `.proto` files
/// and the set go straight into it (`tests/grpc_stream.rs` compiles once for its parallel tests).
pub fn write_schema(dir: &Path) -> String {
    let inc = dir.join("include");
    std::fs::create_dir_all(inc.join("lazywf/v1")).unwrap();
    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/proto/lazywf/v1/lazywf.proto"),
        inc.join("lazywf/v1/lazywf.proto"),
    )
    .unwrap();
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
    std::fs::write(
        inc.join("catalog.proto"),
        r#"
        syntax = "proto3";
        package catalog.v1;
        import "lazywf/v1/lazywf.proto";

        service Ingest {
          rpc Push(stream lazywf.v1.Envelope) returns (stream lazywf.v1.Acks);
        }
        "#,
    )
    .unwrap();
    let bytes = protox::Compiler::new([
        inc.as_path(),
        std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/proto")),
    ])
    .expect("compiler")
    .include_source_info(false)
    .include_imports(true)
    .open_files([inc.join("item.proto"), inc.join("catalog.proto")])
    .expect("compiles")
    .encode_file_descriptor_set();
    let path = dir.join("catalog.desc");
    std::fs::write(&path, bytes).unwrap();
    path.display().to_string()
}

/// The document of item `i` of a flush of `keys`: what the rule's `docs` hold for that key, and
/// so what the server has to be given as the body of that item.
pub fn doc(keys: &[&str], i: usize) -> Value {
    json!({"id": i as i64 + 1, "sku": format!("sku-{}", keys[i]), "price": 9.99})
}

/// The rendered action of a flush of `keys`: the `docs` array, one document per key, in the order
/// the engine passes the keys.
pub fn rendered(keys: &[&str]) -> String {
    let docs: Vec<Value> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| json!({"key": k, "op": "update", "doc": doc(keys, i)}))
        .collect();
    serde_json::to_string(&docs).unwrap()
}

/// What a test's server does with what it is given.
#[derive(Clone)]
pub struct Policy {
    /// The `grpc-encoding` values the server has: anything else is answered `UNIMPLEMENTED`,
    /// which is how a real server without the codec answers.
    pub accept: Vec<String>,
    /// Compress the server's own messages with this.
    pub respond_with: Option<String>,
    /// Keys it refuses, with the reason it gives.
    pub refuse: Vec<(String, String)>,
    /// Keys it says `RETRY` for.
    pub retry: Vec<String>,
    /// Keys it says nothing about: the deadline is what answers for them.
    pub silent: Vec<String>,
    /// Acknowledge every item in its own message rather than one `Acks` for the envelope.
    pub one_by_one: bool,
    /// Reset the stream after this many items, acknowledgements sent (0: never).
    pub reset_after: usize,
    /// Echo a hash that is not the item's, for these sequence numbers: a server bug.
    pub wrong_hash: Vec<u64>,
    /// Send an acknowledgement for a sequence number that was never sent.
    pub ghost_ack: bool,
    /// Answer with an HTTP status and a body of its own: a proxy, not a gRPC server.
    pub http_status: Option<u16>,
    /// Answer the first envelope and then end the response without a `grpc-status` trailer.
    pub no_status: bool,
    /// Refuse the call with this gRPC status before reading anything.
    pub status: Option<i32>,
    /// The bearer token the call must carry.
    pub token: Option<String>,
}

impl Default for Policy {
    fn default() -> Policy {
        Policy {
            accept: vec!["identity".into(), "gzip".into(), "zstd".into()],
            respond_with: None,
            refuse: Vec::new(),
            retry: Vec::new(),
            silent: Vec::new(),
            one_by_one: false,
            reset_after: 0,
            wrong_hash: Vec::new(),
            ghost_ack: false,
            http_status: None,
            no_status: false,
            status: None,
            token: None,
        }
    }
}

/// The message encoding, spelled out here because the client's own (`grpc::wire`) is private and
/// a test server must be able to say the same thing in its own words.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cod {
    Identity,
    Gzip,
    Zstd,
}

impl Cod {
    fn name(self) -> &'static str {
        match self {
            Cod::Identity => "identity",
            Cod::Gzip => "gzip",
            Cod::Zstd => "zstd",
        }
    }
}

/// One message as it goes on the wire: the compression flag, the length, the body.
fn frame(body: &[u8], codec: Cod) -> Vec<u8> {
    let (flag, payload) = match codec {
        Cod::Identity => (0u8, body.to_vec()),
        Cod::Gzip => {
            use std::io::Write as _;
            let mut zip = Vec::new();
            let mut e = flate2::write::GzEncoder::new(&mut zip, flate2::Compression::default());
            e.write_all(body).unwrap();
            e.finish().unwrap();
            (1u8, zip)
        }
        Cod::Zstd => (1u8, zstd::encode_all(body, 0).unwrap()),
    };
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(flag);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

/// The next whole message at the front of a response buffer, decompressed with the codec the
/// call's `grpc-encoding` names.
fn take_frame(buf: &mut BytesMut, codec: Cod) -> anyhow::Result<Option<Vec<u8>>> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let head = [buf[0], buf[1], buf[2], buf[3], buf[4]];
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    if buf.len() < len + 5 {
        return Ok(None);
    }
    let payload = buf.split_to(len + 5).split_off(5);
    let payload = &payload[..];
    let body = match (head[0] & 1, codec) {
        (0, _) => payload.to_vec(),
        (_, Cod::Identity) => anyhow::bail!("a compressed message on an identity call"),
        (_, Cod::Gzip) => {
            use std::io::Read as _;
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(payload)
                .read_to_end(&mut out)
                .unwrap();
            out
        }
        (_, Cod::Zstd) => zstd::decode_all(payload).unwrap(),
    };
    Ok(Some(body))
}

/// The server: what it was given, and the task that serves its connections.
pub struct Server {
    addr: SocketAddr,
    /// Every item it was given, in the order it was given them.
    items: Arc<Mutex<Vec<contract::Item>>>,
    /// The `grpc-encoding` of each call it accepted, in order: the ladder is what a test reads.
    encodings: Arc<Mutex<Vec<String>>>,
    /// The headers of the most recent call.
    headers: Arc<Mutex<Vec<(String, String)>>>,
    /// How many calls it has served.
    calls: Arc<AtomicUsize>,
    accept: tokio::task::JoinHandle<()>,
}

/// What every task of the server shares: what to record, and how to answer.
#[derive(Clone)]
struct Ctx {
    items: Arc<Mutex<Vec<contract::Item>>>,
    encodings: Arc<Mutex<Vec<String>>>,
    headers: Arc<Mutex<Vec<(String, String)>>>,
    calls: Arc<AtomicUsize>,
    policy: Arc<Mutex<Policy>>,
}

impl Server {
    /// Starts a server on a port of its own, with this policy.
    pub async fn start(policy: Policy) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cx = Ctx {
            items: Arc::new(Mutex::new(Vec::new())),
            encodings: Arc::new(Mutex::new(Vec::new())),
            headers: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
            policy: Arc::new(Mutex::new(policy)),
        };
        let running = cx.clone();
        let accept = tokio::spawn(async move { accept_loop(listener, running).await });
        Server {
            addr,
            items: cx.items,
            encodings: cx.encodings,
            headers: cx.headers,
            calls: cx.calls,
            accept,
        }
    }

    /// The `grpc://` endpoint of this server, calling [`METHOD`].
    pub fn endpoint(&self) -> String {
        format!("grpc://{}:{}/{}", self.addr.ip(), self.addr.port(), METHOD)
    }

    /// What the server has been given so far.
    pub fn seen(&self) -> Vec<contract::Item> {
        self.items.lock().unwrap().clone()
    }

    /// The codecs it was asked for, in the order the calls came.
    pub fn asked(&self) -> Vec<String> {
        self.encodings.lock().unwrap().clone()
    }

    /// One header of the most recent call.
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .lock()
            .unwrap()
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    }

    /// How many calls it has served.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    /// Stops it. The tasks of a call that is still open go with it.
    pub fn stop(self) {
        self.accept.abort();
    }
}

/// Every connection of the server: one HTTP/2 connection, any number of calls on it.
async fn accept_loop(listener: tokio::net::TcpListener, cx: Ctx) {
    loop {
        let (io, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => return,
        };
        io.set_nodelay(true).ok();
        let cx = cx.clone();
        tokio::spawn(async move {
            let mut conn = match h2::server::handshake(io).await {
                Ok(c) => c,
                Err(_) => return,
            };
            while let Some(req) = conn.accept().await {
                let (req, respond) = match req {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let cx = cx.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve(req, respond, cx).await {
                        tracing::debug!("grpc test server: {e:#}");
                    }
                });
            }
        });
    }
}

/// One call: what it asks for first, then every envelope it is given, answered as the policy says.
async fn serve(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    cx: Ctx,
) -> anyhow::Result<()> {
    let policy = cx.policy.lock().unwrap().clone();
    cx.calls.fetch_add(1, Ordering::Relaxed);
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_ascii_lowercase(),
                v.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();
    *cx.headers.lock().unwrap() = headers.clone();
    let get = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    };

    // A peer that answers HTTP rather than gRPC: a proxy, an auth wall, a load balancer with no
    // backend behind it. What it says is its own verdict on the request.
    if let Some(status) = policy.http_status {
        let mut head = head(status);
        head.headers_mut()
            .insert("content-type", http::HeaderValue::from_static("text/plain"));
        let mut send = respond.send_response(head, false)?;
        send.send_data(Bytes::from(b"the proxy has no such backend".to_vec()), true)?;
        return Ok(());
    }
    // The path of the call, and the token of `vars.token`, are refused before anything is read:
    // a trailers-only response, which is how a server says no to a call it did not read.
    if req.uri().path() != format!("/{METHOD}") {
        return refuse(respond, 12, "no such method").await;
    }
    if let Some(want) = &policy.token {
        if get("authorization").as_deref() != Some(&format!("Bearer {want}")[..]) {
            return refuse(respond, 16, "no token").await;
        }
    }
    // The codec the call's messages are compressed with: a server without it answers the way
    // tonic's server and grpc-go do, naming the compression in an `UNIMPLEMENTED`, which is what
    // lazywf's ladder watches for.
    let asked = get("grpc-encoding").unwrap_or_else(|| "identity".to_string());
    cx.encodings.lock().unwrap().push(asked.clone());
    if !policy.accept.contains(&asked) {
        return refuse(
            respond,
            12,
            &format!("Content is compressed with `{asked}` which isn't supported"),
        )
        .await;
    }
    let codec = match asked.as_str() {
        "gzip" => Cod::Gzip,
        "zstd" => Cod::Zstd,
        _ => Cod::Identity,
    };
    let respond_with = match policy.respond_with.as_deref() {
        Some("gzip") => Cod::Gzip,
        Some("zstd") => Cod::Zstd,
        _ => Cod::Identity,
    };
    if let Some(code) = policy.status {
        return refuse(respond, code, "the service says no").await;
    }

    let mut head = head(200);
    head.headers_mut().insert(
        "content-type",
        http::HeaderValue::from_static("application/grpc+proto"),
    );
    if respond_with != Cod::Identity {
        head.headers_mut().insert(
            "grpc-encoding",
            http::HeaderValue::from_static(respond_with.name()),
        );
    }
    let mut send = respond.send_response(head, false)?;

    // The envelopes may arrive in pieces, at whatever pace the client and the window allow.
    let mut body = req.into_body();
    let mut buf = BytesMut::new();
    let mut seen = 0usize;
    loop {
        let next = tokio::time::timeout(Duration::from_secs(30), body.data()).await;
        let data = match next {
            Err(_) => return Ok(()),
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(d))) => d,
        };
        buf.extend_from_slice(&data);
        while let Some(msg) = take_frame(&mut buf, codec)? {
            let envelope = contract::Envelope::decode(&msg[..])?;
            let mut acks = Vec::new();
            for item in envelope.items {
                cx.items.lock().unwrap().push(item.clone());
                seen += 1;
                if let Some(ack) = ack_of(&policy, &item) {
                    if policy.one_by_one {
                        send_acks(&mut send, respond_with, vec![ack]).await?;
                    } else {
                        acks.push(ack);
                    }
                }
                if policy.ghost_ack {
                    // An acknowledgement for an item that was never sent: a server that has lost
                    // track, which lazywf must not believe.
                    send_acks(
                        &mut send,
                        respond_with,
                        vec![contract::Ack {
                            seq: 900_000 + item.seq,
                            hash: contract::hash(b"nobody", b"nothing"),
                            status: contract::Status::Ok as i32,
                            reason: String::new(),
                        }],
                    )
                    .await?;
                }
                if policy.reset_after != 0 && seen >= policy.reset_after {
                    // Whatever the server has already promised goes out first: a reset
                    // kills the stream, not the acknowledgements it had already made.
                    if !acks.is_empty() {
                        send_acks(&mut send, respond_with, std::mem::take(&mut acks)).await?;
                    }
                    // A reset drops what the connection has not written yet, so the
                    // connection task gets a moment to put the acknowledgements on the
                    // socket before the stream is cancelled underneath it.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    send.send_reset(h2::Reason::INTERNAL_ERROR);
                    return Ok(());
                }
            }
            if !acks.is_empty() {
                send_acks(&mut send, respond_with, acks).await?;
            }
            if policy.no_status {
                // The first envelope answered, the body ends and the trailers are not there: an
                // answer lazywf cannot use.
                send.send_data(Bytes::new(), true)?;
                return Ok(());
            }
        }
    }
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    send.send_trailers(trailers)?;
    Ok(())
}

/// The answer for one item, as the policy says: nothing at all for a key it stays silent about,
/// and the hash the item did not carry for the ones a test has poisoned.
fn ack_of(policy: &Policy, item: &contract::Item) -> Option<contract::Ack> {
    if policy.silent.contains(&item.key) {
        return None;
    }
    let (status, reason) =
        if let Some((_, why)) = policy.refuse.iter().find(|(k, _)| *k == item.key) {
            (contract::Status::Refused, why.clone())
        } else if policy.retry.contains(&item.key) {
            (contract::Status::Retry, format!("not now: {}", item.key))
        } else {
            (contract::Status::Ok, String::new())
        };
    let hash = if policy.wrong_hash.contains(&item.seq) {
        item.hash ^ 0xffff_0000_ffff_0000
    } else {
        item.hash
    };
    Some(contract::Ack {
        seq: item.seq,
        hash,
        status: status as i32,
        reason,
    })
}

/// One `Acks` message, framed and compressed as the response's `grpc-encoding` says.
async fn send_acks(
    send: &mut h2::SendStream<Bytes>,
    codec: Cod,
    acks: Vec<contract::Ack>,
) -> anyhow::Result<()> {
    let msg = contract::Acks { acks }.encode_to_vec();
    send.send_data(Bytes::from(frame(&msg, codec)), false)?;
    Ok(())
}

/// A response head: the status and the room to hang the gRPC headers on.
fn head(status: u16) -> http::Response<()> {
    http::Response::builder()
        .status(http::StatusCode::from_u16(status).unwrap())
        .body(())
        .unwrap()
}

/// A trailers-only response: the status of a call the server refused before reading it.
async fn refuse(
    mut respond: h2::server::SendResponse<Bytes>,
    code: i32,
    message: &str,
) -> anyhow::Result<()> {
    let mut head = head(200);
    head.headers_mut().insert(
        "content-type",
        http::HeaderValue::from_static("application/grpc+proto"),
    );
    head.headers_mut().insert(
        "grpc-status",
        http::HeaderValue::from_str(&code.to_string()).unwrap(),
    );
    head.headers_mut().insert(
        "grpc-message",
        http::HeaderValue::from_str(message).unwrap(),
    );
    respond.send_response(head, true)?;
    Ok(())
}
