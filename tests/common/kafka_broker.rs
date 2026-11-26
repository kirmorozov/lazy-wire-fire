//! An in-process Kafka-protocol broker for the `kafka` kind's tests: the smallest peer rskafka
//! will talk to, so the answer a produce gets can be driven from a test.
//!
//! # Why a broker and not a mock
//!
//! The `kafka` kind's verdicts come from the wire, not from a body a client-side stand-in could
//! write: rskafka mints the offset list of a produce itself (`client/partition.rs:756` — the
//! broker's base offset plus the record's position in the batch) and turns a partition missing from
//! the answer into a server error (`client/partition.rs:727`). So no stand-in can answer a produce
//! short, per record, or not at all. Only a peer that speaks the protocol can say "this partition:
//! `MessageTooLarge`", or say nothing. That is what this is.
//!
//! # What it speaks
//!
//! The three requests of a produce, read from rskafka 0.6's own client:
//!
//! - **ApiVersions** on every connection (`connection.rs:165`: connecting is `sync_versions` and,
//!   when SASL is configured, the handshake — these tests configure none). The answer lists exactly
//!   the versions rskafka 0.6 supports for the APIs below, so the version both sides pick is one
//!   the codec has, and the client's own range check (`messenger.rs:500`) is satisfied.
//! - **Metadata** (v0–4, all rskafka asks for, so no compact encoding): the declared topics with
//!   their partitions, every one led by the single broker this listener is. `Client::list_topics`
//!   (the target's topic check, `targets/kafka.rs:576`) and the two leader reads of
//!   `partition_client` (`client/partition.rs:457`, `:486`) go through it; because the only broker
//!   is this one, the connection to "the leader" lands here too.
//! - **Produce** (v3–7): the batch is decoded with rskafka's own record codec, so the test sees
//!   each record's key, value and headers however the target chose to compress them.
//!
//! Nothing else is on this path: no Fetch, no ListOffsets, no CreateTopics — a topic is declared
//! here, never created through the protocol, so a topic the test did not declare is simply not
//! listed, which is what "unknown to the broker" looks like (`targets/kafka.rs:577`).
//!
//! # Whose bytes these are
//!
//! Frames, headers and every scalar are rskafka's own — the crate's `protocol` module, which its
//! `unstable-fuzzing` feature publishes (enabled on a dev-dependency only, so the product build
//! does not see it). The four *message layouts* a broker needs are written here, because rskafka
//! publishes each message only in the direction its own client uses: a request it can write but not
//! read, a response it can read but not write, and a broker needs the opposite of both. Two things
//! keep those layouts from drifting from the protocol unnoticed: they are assembled from the crate's
//! primitives (an `Int32`, a `String_`, an `Array<Int32>` encode as the crate encodes them), and
//! every answer is read back with the crate's own reader — the reader the client uses — before it
//! goes out, so an answer this fake cannot encode is a panic in the test rather than a mystery.
//!
//! # Driving it from a test
//!
//! ```text
//! let b = KafkaBroker::start().await;
//! b.topic("orders", 1);
//! b.on_produce(|p| if p.has_key("2") {
//!     Answer::Code(10)          // MessageTooLarge: a refusal about the records the batch holds
//! } else {
//!     Answer::Accept            // take them, at the partition's next offset
//! });
//! // … run the engine against kafka://<b.endpoint()>/orders …
//! assert_eq!(b.keys("orders"), ["1", "3"]);
//! ```
//!
//! The broker keeps what it took (offset, key, value, headers) and a log of every produce request
//! with the answer it gave, so a test can assert which produce was split, which record the answer
//! named, and what the partition holds in the end.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rskafka::protocol::api_key::ApiKey;
use rskafka::protocol::api_version::ApiVersion;
use rskafka::protocol::frame::{AsyncMessageRead, AsyncMessageWrite};
use rskafka::protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, ApiVersionsResponseApiKey, MetadataResponse,
    ProduceResponse, ReadVersionedType, RequestBody, RequestHeader, ResponseHeader,
    WriteVersionedType,
};
use rskafka::protocol::primitives::{
    Boolean, Int16, Int32, Int64, NullableString, Records, String_,
};
use rskafka::protocol::record::{ControlBatchOrRecords, RecordBatch, RecordBatchCompression};
use rskafka::protocol::traits::{ReadType, WriteType};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;

/// The node id this broker answers for, in its own metadata and in every partition's leader.
const NODE: i32 = 1;

/// The largest frame the broker reads (a produce of a whole batch, with room to spare).
const MAX_FRAME: usize = 64 * 1024 * 1024;

/// The API versions the broker advertises: what rskafka 0.6 supports for the three requests of a
/// produce (`metadata.rs:36`, `produce.rs:124`), so the version the client picks from the
/// intersection of the two ranges is one its codec can write.
fn advertised(key: ApiKey) -> Option<(i16, i16)> {
    match key {
        ApiKey::ApiVersions => Some((0, 3)),
        ApiKey::Metadata => Some((0, 4)),
        ApiKey::Produce => Some((3, 7)),
        _ => None,
    }
}

/// One record of a produce, as the broker read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InRec {
    /// The record key — the engine's document key; `None` for a record without one.
    pub key: Option<String>,
    /// The record value: the rendered document.
    pub value: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl InRec {
    /// The value as text (the render is UTF-8 JSON; the bytes stay for a test that wants them).
    pub fn value_str(&self) -> String {
        String::from_utf8_lossy(&self.value).into_owned()
    }

    /// The value parsed: what the target sent as the record's document.
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.value).expect("the record value is the rendered JSON")
    }

    /// A header's value by name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// One produce request as the broker saw it: the topic, the partition, and the records of its one
/// batch (rskafka sends exactly one of each, `client/partition.rs:712`).
#[derive(Debug, Clone)]
pub struct Produce {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<InRec>,
    /// Whether any batch arrived compressed: the records are decoded by the crate's codec either
    /// way, so a test can pin that a compressed batch really was one.
    pub compressed: bool,
}

impl Produce {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The batch's keys, in the order the client sent them (`None`: a record without a key).
    pub fn keys(&self) -> Vec<Option<String>> {
        self.records.iter().map(|r| r.key.clone()).collect()
    }

    /// Whether the batch carries a record with this key — what an answer that speaks about one
    /// record of the batch is written against.
    pub fn has_key(&self, key: &str) -> bool {
        self.records.iter().any(|r| r.key.as_deref() == Some(key))
    }
}

/// What the fake answers one produce with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// Take the batch, at the partition's next offset; rskafka then answers `base + i` for its
    /// record `i`.
    Accept,
    /// Take the batch and name `base` as its first offset: how a test puts a chosen offset in front
    /// of `expect` for one record (the client adds the record's position in the batch).
    AcceptAt { base: i64 },
    /// Refuse the partition with wire code `code` and take nothing. `-1` is the broker's own
    /// `UnknownServerError`; anything else is the code a broker would answer (`3` unknown topic or
    /// partition, `7` request timed out, `10` message too large, `29` topic authorization failed,
    /// …), and a code that is not a Kafka one reaches the client as `Unknown(n)`
    /// (`protocol/error.rs:234`). `0` is not a refusal — that is [`Answer::Accept`].
    Code(i16),
    /// Never answer: the target's own time limit decides what the records are.
    Silence,
    /// Cut the connection instead of answering: the client sees a broken stream.
    HangUp,
    /// Take the batch but name another topic: the client cannot match the answer to its request
    /// (`partition.rs:731`), which is "not known to be in" — not "not in".
    WrongTopic,
    /// Take the batch but name another partition.
    WrongPartition,
    /// Take the batch but answer with no partition in the topic's answer.
    NoPartition,
}

/// The answer of one produce, as the test reads the log back.
#[derive(Debug, Clone, PartialEq)]
pub struct Produced {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<InRec>,
    pub answer: Answer,
    /// Whether any of its batches arrived compressed: the records are read with rskafka's own codec
    /// either way, so a test can pin that a compressed batch really was one.
    pub compressed: bool,
}

impl Produced {
    pub fn keys(&self) -> Vec<Option<String>> {
        self.records.iter().map(|r| r.key.clone()).collect()
    }

    pub fn has_key(&self, key: &str) -> bool {
        self.records.iter().any(|r| r.key.as_deref() == Some(key))
    }
}

/// One record the broker took.
#[derive(Debug, Clone, PartialEq)]
pub struct Stored {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl Stored {
    pub fn value_str(&self) -> String {
        String::from_utf8_lossy(&self.value).into_owned()
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.value).expect("the record value is the rendered JSON")
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// The script that answers produces. `Arc` so the handler can call it with the state unlocked: a
/// script is allowed to ask the broker questions (`produce_count`, `answer`).
type Script = Arc<dyn Fn(&Produce) -> Answer + Send + Sync + 'static>;

/// Everything the fake knows, shared by its listener and its connection tasks.
struct State {
    /// The port to advertise as the leader's (the listener's own, so "connect to the leader" comes
    /// back to this same listener).
    port: u16,
    /// The declared topics: name -> its partition ids, sorted.
    topics: BTreeMap<String, Vec<i32>>,
    /// The next offset of each `(topic, partition)`: the broker's own log end.
    next: BTreeMap<(String, i32), i64>,
    script: Option<Script>,
    /// Every produce seen, in order, with the answer it was given.
    produced: Vec<Produced>,
    /// Every record taken.
    stored: Vec<Stored>,
    /// Counts, for a test that wants to say "the target asked again": the ApiVersions, Metadata and
    /// Produce requests answered.
    api_versions: usize,
    metadata: usize,
    produces: usize,
}

impl State {
    fn new(port: u16) -> State {
        State {
            port,
            topics: BTreeMap::new(),
            next: BTreeMap::new(),
            script: None,
            produced: Vec::new(),
            stored: Vec::new(),
            api_versions: 0,
            metadata: 0,
            produces: 0,
        }
    }
}

/// The fake broker. Dropping the last handle stops accepting; the tasks of live connections end
/// with their sockets.
pub struct KafkaBroker {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    /// Held so the listener lives as long as the handle.
    _listener: Arc<TcpListener>,
}

impl KafkaBroker {
    /// Listens on an ephemeral port of the loopback and answers the three requests of a produce.
    pub async fn start() -> KafkaBroker {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::new(addr.port())));
        let l = Arc::new(listener);
        tokio::spawn({
            let (accept, s) = (Arc::clone(&l), Arc::clone(&state));
            async move {
                loop {
                    match accept.accept().await {
                        Ok((stream, _)) => {
                            let s = Arc::clone(&s);
                            tokio::spawn(handle(stream, s));
                        }
                        // The listener is only this code's, so an accept failure is a closed
                        // socket: the handle is gone and nothing is left to serve.
                        Err(_) => return,
                    }
                }
            }
        });
        KafkaBroker {
            addr,
            state,
            _listener: l,
        }
    }

    /// `host:port` for the target's endpoint: `kafka://<this>/<topic>`.
    pub fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// Declares a topic with partitions `0..n`, all led by this broker.
    pub fn topic(&self, name: &str, n: i32) {
        self.partitioned(name, &(0..n).collect::<Vec<i32>>());
    }

    /// Declares a topic with exactly these partition ids. They are sorted here because the target
    /// chooses a record's partition by *position* in the sorted list (`targets/kafka.rs:582`): the
    /// order the broker happens to list them in must not decide where a key goes.
    pub fn partitioned(&self, name: &str, parts: &[i32]) {
        let mut ps = parts.to_vec();
        ps.sort_unstable();
        self.state
            .lock()
            .unwrap()
            .topics
            .insert(name.to_string(), ps);
    }

    /// Takes a topic away: the broker no longer lists it, which is what a topic the target never
    /// created looks like.
    pub fn forget(&self, name: &str) {
        self.state.lock().unwrap().topics.remove(name);
    }

    /// Answers every produce with `a`.
    pub fn answer(&self, a: Answer) {
        self.on_produce(move |_| a);
    }

    /// Answers each produce from what it carries, so the test decides which record or partition the
    /// answer speaks about. Called with the broker's state unlocked: the closure may ask the broker
    /// questions.
    pub fn on_produce<F>(&self, f: F)
    where
        F: Fn(&Produce) -> Answer + Send + Sync + 'static,
    {
        self.state.lock().unwrap().script = Some(Arc::new(f));
    }

    /// Every produce seen so far, in the order the client sent them, each with the answer it was
    /// given. A split batch shows here as the parent and its halves.
    pub fn produced(&self) -> Vec<Produced> {
        self.state.lock().unwrap().produced.clone()
    }

    /// How many produce requests the broker has answered or was asked to leave alone.
    pub fn produce_count(&self) -> usize {
        self.state.lock().unwrap().produces
    }

    /// The records the broker holds for `topic`, in the order it took them.
    pub fn stored(&self, topic: &str) -> Vec<Stored> {
        let st = self.state.lock().unwrap();
        st.stored
            .iter()
            .filter(|s| s.topic == topic)
            .cloned()
            .collect()
    }

    /// How many records of `topic` the broker holds.
    pub fn stored_count(&self, topic: &str) -> usize {
        self.stored(topic).len()
    }

    /// The records of one partition, by offset.
    pub fn stored_partition(&self, topic: &str, partition: i32) -> Vec<Stored> {
        let mut v: Vec<Stored> = self
            .stored(topic)
            .into_iter()
            .filter(|s| s.partition == partition)
            .collect();
        v.sort_by_key(|s| s.offset);
        v
    }

    /// The keys the broker holds for `topic`, in the order they arrived.
    pub fn keys(&self, topic: &str) -> Vec<String> {
        self.stored(topic)
            .into_iter()
            .map(|s| s.key.unwrap_or_default())
            .collect()
    }

    /// How many times each key of `topic` was taken. A key taken twice is a key the engine sent
    /// twice — which is what an answer that never reached the client leaves behind.
    pub fn key_arrivals(&self, topic: &str) -> BTreeMap<String, usize> {
        let mut m = BTreeMap::new();
        for s in self.stored(topic) {
            *m.entry(s.key.unwrap_or_default()).or_insert(0) += 1;
        }
        m
    }

    /// Requests answered of each kind: `(ApiVersions, Metadata, Produce)`.
    pub fn requests(&self) -> (usize, usize, usize) {
        let st = self.state.lock().unwrap();
        (st.api_versions, st.metadata, st.produces)
    }

    /// Forgets the produce log and the records taken (the script and the topics stay).
    pub fn clear(&self) {
        let mut st = self.state.lock().unwrap();
        st.produced.clear();
        st.stored.clear();
    }
}

// ——— the wire ——————————————————————————————————————————————————————————————————————————

/// One connection: read frames and answer each on its own task, so a produce the broker is told to
/// leave unanswered does not stop that connection's other requests. Responses are matched by
/// correlation id (`messenger.rs:244`), so the answers need not leave in request order.
async fn handle(stream: TcpStream, state: Arc<Mutex<State>>) {
    let (mut rd, wr) = stream.into_split();
    let wr = Arc::new(AsyncMutex::new(Some(wr)));
    loop {
        let frame = match rd.read_message(MAX_FRAME).await {
            Ok(f) => f,
            // The client closed the connection (a target that dropped its client: the usual end).
            Err(_) => return,
        };
        let Some((header, body)) = read_request(&frame) else {
            return; // not a frame this broker can read: nothing sensible to answer
        };
        let (wr, s) = (Arc::clone(&wr), Arc::clone(&state));
        tokio::spawn(answer_request(header, body, s, wr));
    }
}

/// The request header of a frame, and the body bytes behind it.
///
/// The header's own version follows the request's version — 1, or 2 once the API carries tagged
/// fields — and rskafka derives both from the request type's constants, so this reads them from the
/// same constants rather than hard-coding a table that could drift from the crate's.
fn read_request(frame: &[u8]) -> Option<(RequestHeader, Vec<u8>)> {
    let mut peek = Cursor::new(frame.to_vec());
    let key = ApiKey::from(Int16::read(&mut peek).ok()?);
    let version = Int16::read(&mut peek).ok()?.0;
    let first_tagged = match key {
        ApiKey::ApiVersions => {
            <ApiVersionsRequest as RequestBody>::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION
        }
        // Both of these are `9`: higher than any version rskafka sends for them (v0–4, v3–7), so
        // their headers are read as version 1 — but from the constants, not from that fact.
        ApiKey::Metadata => ApiVersion(Int16(9)),
        ApiKey::Produce => ApiVersion(Int16(9)),
        _ => ApiVersion(Int16(i16::MAX)),
    };
    let header_version: i16 = if version >= first_tagged.0 .0 { 2 } else { 1 };
    let mut cur = Cursor::new(frame.to_vec());
    let header = RequestHeader::read_versioned(&mut cur, ApiVersion(Int16(header_version))).ok()?;
    let rest = frame[usize::try_from(cur.position()).ok()?..].to_vec();
    Some((header, rest))
}

/// One request's answer, on its own task.
async fn answer_request(
    header: RequestHeader,
    body: Vec<u8>,
    state: Arc<Mutex<State>>,
    wr: Arc<AsyncMutex<Option<OwnedWriteHalf>>>,
) {
    match header.request_api_key {
        ApiKey::ApiVersions => api_versions(&state, &wr, &header, body).await,
        ApiKey::Metadata => metadata(&state, &wr, &header, body).await,
        ApiKey::Produce => produce(&state, &wr, &header, body).await,
        // Nothing on the target's produce path asks for more. A test that makes the client ask for
        // another API will wait for an answer that never comes — the honest answer for a fake that
        // does not speak that API — so say what was asked, loudly, rather than hang quietly.
        other => {
            eprintln!(
                "fake kafka broker: not answering {:?} (this fake does not speak it)",
                other
            );
        }
    }
}

/// Wraps a body in the response header and sends it. The header is version 0 always: rskafka reads
/// it as version 0 and only looks for tagged fields in an API that has them in a response, which
/// none of these three does (`messenger.rs:235`, `api_versions.rs:113`).
async fn send(wr: &AsyncMutex<Option<OwnedWriteHalf>>, correlation: Int32, payload: &[u8]) -> bool {
    let mut buf = Vec::new();
    ResponseHeader {
        correlation_id: correlation,
        tagged_fields: None,
    }
    .write_versioned(&mut buf, ApiVersion(Int16(0)))
    .expect("a response header always writes");
    buf.extend_from_slice(payload);
    let mut guard = wr.lock().await;
    match guard.as_mut() {
        Some(half) => half.write_message(&buf).await.is_ok(),
        // Already hung up on this connection.
        None => false,
    }
}

/// Answers an ApiVersions request with the versions of the three APIs this broker speaks. Both
/// directions of this message are the crate's own, so nothing here is hand-written.
async fn api_versions(
    state: &Arc<Mutex<State>>,
    wr: &AsyncMutex<Option<OwnedWriteHalf>>,
    header: &RequestHeader,
    body: Vec<u8>,
) {
    // Read the request even though the answer does not depend on it: a body this codec cannot read
    // is a bug in the fake, and it should say so at the request that has it.
    let req =
        ApiVersionsRequest::read_versioned(&mut Cursor::new(body), header.request_api_version);
    assert!(
        req.is_ok(),
        "an ApiVersions request the codec wrote must read back"
    );
    let keys = [ApiKey::ApiVersions, ApiKey::Metadata, ApiKey::Produce]
        .into_iter()
        .map(|k| {
            let (lo, hi) = advertised(k).expect("the three APIs of a produce are advertised");
            ApiVersionsResponseApiKey {
                api_key: k,
                min_version: ApiVersion(Int16(lo)),
                max_version: ApiVersion(Int16(hi)),
                tagged_fields: None,
            }
        })
        .collect();
    let resp = ApiVersionsResponse {
        error_code: None,
        api_keys: keys,
        // Not zero would make the client sleep before answering (`messenger.rs:472`).
        throttle_time_ms: Some(Int32(0)),
        tagged_fields: None,
    };
    let mut payload = Vec::new();
    resp.write_versioned(&mut payload, header.request_api_version)
        .expect("the advertised versions always write");
    state.lock().unwrap().api_versions += 1;
    send(wr, header.correlation_id, &payload).await;
}

/// Answers a Metadata request from the declared topics: every partition led by this one broker, so
/// the leader connection the client opens next comes back here.
async fn metadata(
    state: &Arc<Mutex<State>>,
    wr: &AsyncMutex<Option<OwnedWriteHalf>>,
    header: &RequestHeader,
    body: Vec<u8>,
) {
    let version = header.request_api_version.0 .0;
    let asked = parse_metadata(&body);
    let (port, topics) = {
        let st = state.lock().unwrap();
        // `Asked::All` is answered with every topic this broker has; a named list with the named
        // topics this broker has — a topic it does not have is left out, which is what an unknown
        // topic reads as.
        let names: Vec<String> = match asked {
            Asked::All => st.topics.keys().cloned().collect(),
            Asked::Named(want) => want
                .into_iter()
                .filter(|n| st.topics.contains_key(n))
                .collect(),
        };
        (
            st.port,
            names
                .into_iter()
                .map(|name| {
                    let parts = st.topics.get(&name).cloned().unwrap_or_default();
                    (name, parts)
                })
                .collect::<Vec<(String, Vec<i32>)>>(),
        )
    };
    let payload = metadata_payload(version, port, &topics);
    // The answer is read back with the reader the client itself uses: a layout this fake cannot
    // write is caught here, in the test, rather than as a client that cannot say what went wrong.
    MetadataResponse::read_versioned(
        &mut Cursor::new(payload.clone()),
        ApiVersion(Int16(version)),
    )
    .expect("a metadata answer this fake wrote must read back with rskafka's own reader");
    state.lock().unwrap().metadata += 1;
    send(wr, header.correlation_id, &payload).await;
}

/// Answers a Produce request: decode its one batch, ask the script what to do with it, book what was
/// taken, and answer (or not, or wrongly) accordingly.
async fn produce(
    state: &Arc<Mutex<State>>,
    wr: &AsyncMutex<Option<OwnedWriteHalf>>,
    header: &RequestHeader,
    body: Vec<u8>,
) {
    let version = header.request_api_version.0 .0;
    let Some(p) = parse_produce(version, &body) else {
        eprintln!("fake kafka broker: a produce this fake cannot read, not answering");
        return;
    };

    // The script decides, with the state unlocked: it may ask the broker questions.
    let script = state.lock().unwrap().script.clone();
    let answer = match script {
        Some(f) => f(&p),
        // With no script this is a plain broker: take everything, at the partition's next offset.
        None => Answer::Accept,
    };
    {
        let mut st = state.lock().unwrap();
        st.produces += 1;
        st.produced.push(Produced {
            topic: p.topic.clone(),
            partition: p.partition,
            records: p.records.clone(),
            answer,
            compressed: p.compressed,
        });
    }

    if matches!(answer, Answer::HangUp) {
        // No answer, and no connection to answer it on: dropping the write half shuts the socket,
        // which is the broken stream the client then retries.
        drop(wr.lock().await.take());
        return;
    }
    if matches!(answer, Answer::Silence) {
        return;
    }

    // What the broker did with the batch, and what its answer says about it. A batch taken is booked
    // even when the answer cannot be matched to the request: the records went in, and the client's
    // inability to read that is exactly the "not known to be in" the engine is built for.
    let taken = matches!(
        answer,
        Answer::Accept
            | Answer::AcceptAt { .. }
            | Answer::WrongTopic
            | Answer::WrongPartition
            | Answer::NoPartition
    );
    let base = match answer {
        Answer::AcceptAt { base } => base,
        _ => {
            let st = state.lock().unwrap();
            st.next
                .get(&(p.topic.clone(), p.partition))
                .copied()
                .unwrap_or(0)
        }
    };
    if taken {
        let mut st = state.lock().unwrap();
        for (i, r) in p.records.iter().enumerate() {
            st.stored.push(Stored {
                topic: p.topic.clone(),
                partition: p.partition,
                offset: base + i as i64,
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
            });
        }
        st.next.insert(
            (p.topic.clone(), p.partition),
            base + i64::try_from(p.records.len()).unwrap_or(0),
        );
    }
    // The answer's own shape: a wrong name or index, or no partition at all, is what makes a taken
    // batch unverifiable for the client.
    let (name, parts) = match answer {
        Answer::Code(c) => {
            assert_ne!(
                c, 0,
                "`Answer::Code(0)` is not a refusal: ask for `Answer::Accept`"
            );
            (p.topic.clone(), vec![(p.partition, c, base)])
        }
        Answer::WrongTopic => (
            "lwf-not-the-requested-topic".to_string(),
            vec![(p.partition, 0, base)],
        ),
        Answer::WrongPartition => (p.topic.clone(), vec![(p.partition + 1000, 0, base)]),
        Answer::NoPartition => (p.topic.clone(), Vec::new()),
        _ => (p.topic.clone(), vec![(p.partition, 0, base)]),
    };
    let payload = produce_payload(version, &name, &parts);
    ProduceResponse::read_versioned(
        &mut Cursor::new(payload.clone()),
        ApiVersion(Int16(version)),
    )
    .expect("a produce answer this fake wrote must read back with rskafka's own reader");
    send(wr, header.correlation_id, &payload).await;
}

// ——— the four message layouts a broker needs ————————————————————————————————————————————————
//
// rskafka implements each message in the direction its own client needs (a request it writes, a
// response it reads) and a broker needs the opposite of both, so the field sequences are here — on
// the crate's own primitives, and read back by the crate's own reader before they go out (see
// [`metadata`] and [`produce`]). All four are in the non-compact encodings rskafka uses: Metadata
// v0–4 and Produce v3–7 are below the flexible versions (`metadata.rs:39`, `produce.rs:127`).

/// A cursor over a message body, reading and writing the crate's scalars by name.
struct Wire {
    cur: Cursor<Vec<u8>>,
}

impl Wire {
    fn new(bytes: Vec<u8>) -> Wire {
        Wire {
            cur: Cursor::new(bytes),
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.cur.into_inner()
    }

    fn i16(&mut self) -> i16 {
        Int16::read(&mut self.cur).expect("an i16 is there").0
    }

    fn i32(&mut self) -> i32 {
        Int32::read(&mut self.cur).expect("an i32 is there").0
    }

    fn i64(&mut self) -> i64 {
        Int64::read(&mut self.cur).expect("an i64 is there").0
    }

    fn string(&mut self) -> String {
        String_::read(&mut self.cur).expect("a string is there").0
    }

    fn nullable_string(&mut self) -> Option<String> {
        NullableString::read(&mut self.cur)
            .expect("a nullable string is there")
            .0
    }

    /// An array's length. `None` is the `-1` that means "no array at all", which a Metadata request
    /// carries when it asks for every topic; `Some(0)` is an empty array.
    fn array_len(&mut self) -> Option<usize> {
        match Int32::read(&mut self.cur)
            .expect("an array length is there")
            .0
        {
            -1 => None,
            n => Some(usize::try_from(n).expect("a non-negative array length")),
        }
    }

    /// A record set: the nullable bytes envelope and the batches inside it, decoded by the crate —
    /// including whatever compression the producer chose.
    fn records(&mut self) -> Records {
        Records::read(&mut self.cur).expect("the record set is there")
    }

    fn put_i16(&mut self, v: i16) {
        Int16(v).write(&mut self.cur).expect("an i16 always writes");
    }

    fn put_i32(&mut self, v: i32) {
        Int32(v).write(&mut self.cur).expect("an i32 always writes");
    }

    fn put_i64(&mut self, v: i64) {
        Int64(v).write(&mut self.cur).expect("an i64 always writes");
    }

    fn put_bool(&mut self, v: bool) {
        Boolean(v)
            .write(&mut self.cur)
            .expect("a boolean always writes");
    }

    fn put_string(&mut self, v: &str) {
        String_(v.to_string())
            .write(&mut self.cur)
            .expect("a string always writes");
    }

    fn put_nullable_string(&mut self, v: Option<&str>) {
        NullableString(v.map(str::to_string))
            .write(&mut self.cur)
            .expect("a nullable string always writes");
    }

    fn put_array_len(&mut self, n: usize) {
        Int32(i32::try_from(n).expect("an array of at most i32::MAX items"))
            .write(&mut self.cur)
            .expect("an array length always writes");
    }
}

/// The records of the batches a produce carried, and whether any batch arrived compressed.
fn read_batches(batches: &[RecordBatch]) -> (Vec<InRec>, bool) {
    let compressed = batches
        .iter()
        .any(|b| !matches!(b.compression, RecordBatchCompression::NoCompression));
    let records = batches
        .iter()
        .flat_map(|batch| match &batch.records {
            ControlBatchOrRecords::Records(rs) => rs
                .iter()
                .map(|r| InRec {
                    key: r
                        .key
                        .as_deref()
                        .map(|k| String::from_utf8_lossy(k).into_owned()),
                    value: r.value.clone().unwrap_or_default(),
                    headers: r
                        .headers
                        .iter()
                        .map(|h| {
                            (
                                h.key.clone(),
                                String::from_utf8_lossy(&h.value).into_owned(),
                            )
                        })
                        .collect(),
                })
                .collect::<Vec<_>>(),
            // A transaction marker is not a record the target sent.
            ControlBatchOrRecords::ControlBatch(_) => Vec::new(),
        })
        .collect();
    (records, compressed)
}

/// A produce request: its one topic, its one partition, and the records of its batch. `None` for a
/// request this fake was not written for (rskafka sends exactly one topic and one partition,
/// `client/partition.rs:712`).
fn parse_produce(version: i16, body: &[u8]) -> Option<Produce> {
    let mut w = Wire::new(body.to_vec());
    if version >= 3 {
        w.nullable_string(); // transactional_id: these tests produce no transaction
    }
    let _acks = w.i16(); // what the broker is asked to wait for; a fake answers when told
    let _timeout_ms = w.i32();
    let topics = w.array_len().unwrap_or_default();
    if topics != 1 {
        eprintln!("fake kafka broker: a produce of {topics} topics, expected 1");
        return None;
    }
    let topic = w.string();
    let parts = w.array_len().unwrap_or_default();
    if parts != 1 {
        eprintln!("fake kafka broker: a produce of {parts} partitions, expected 1");
        return None;
    }
    let partition = w.i32();
    let records = w.records();
    let (records, compressed) = read_batches(&records.0);
    Some(Produce {
        topic,
        partition,
        records,
        compressed,
    })
}

/// What a metadata request asks the broker to list.
enum Asked {
    /// Every topic the broker has: the request's `topics` is the `-1` array.
    All,
    /// Exactly these topics, in the order the request names them.
    Named(Vec<String>),
}

/// A metadata request. Its scalars are read by the crate's own readers, so a request this fake
/// cannot read is a bug in the fake and panics where it happens rather than going unanswered.
fn parse_metadata(body: &[u8]) -> Asked {
    let mut w = Wire::new(body.to_vec());
    match w.array_len() {
        None => Asked::All,
        Some(n) => Asked::Named((0..n).map(|_| w.string()).collect()),
    }
}

/// The metadata answer: this broker as the only one, `topics` with their partitions, every partition
/// led by this broker. `version` is the one the request came in (0–4), so each field is written
/// only where that version carries it.
fn metadata_payload(version: i16, port: u16, topics: &[(String, Vec<i32>)]) -> Vec<u8> {
    let mut w = Wire::new(Vec::new());
    if version >= 3 {
        w.put_i32(0); // throttle_time_ms: not zero and the client sleeps before answering
    }
    w.put_array_len(1); // brokers
    w.put_i32(NODE); // node_id
    w.put_string("127.0.0.1"); // host
    w.put_i32(i32::from(port)); // port: the listener's own, so the leader connection comes back
    if version >= 1 {
        w.put_nullable_string(None); // rack
    }
    if version >= 2 {
        w.put_nullable_string(Some("lwf-fake")); // cluster_id
    }
    if version >= 1 {
        w.put_i32(NODE); // controller_id
    }
    w.put_array_len(topics.len()); // topics
    for (name, parts) in topics {
        w.put_i16(0); // error: the topic is there
        w.put_string(name);
        if version >= 1 {
            w.put_bool(false); // is_internal: a listed topic the client must not filter
        }
        w.put_array_len(parts.len()); // partitions
        for p in parts {
            w.put_i16(0); // error: the partition is there and has a leader
            w.put_i32(*p); // partition_index
            w.put_i32(NODE); // leader_id
            w.put_array_len(1); // replica_nodes
            w.put_i32(NODE);
            w.put_array_len(1); // isr_nodes
            w.put_i32(NODE);
        }
    }
    w.into_inner()
}

/// The produce answer for one topic: `(partition, error code, base offset)` per partition, in the
/// order the answer lists them. `version` is the one the request came in (3–7).
fn produce_payload(version: i16, name: &str, parts: &[(i32, i16, i64)]) -> Vec<u8> {
    let mut w = Wire::new(Vec::new());
    w.put_array_len(1); // responses: one topic
    w.put_string(name);
    w.put_array_len(parts.len()); // partition_responses
    for (index, code, base) in parts {
        w.put_i32(*index);
        w.put_i16(*code); // 0 is "no error"; any other value the code the client reads
        w.put_i64(*base); // base_offset: the client adds each record's position in the batch
        if version >= 2 {
            w.put_i64(-1); // log_append_time_ms: -1 says CreateTime, which is what the client sent
        }
        if version >= 5 {
            w.put_i64(0); // log_start_offset
        }
    }
    if version >= 1 {
        w.put_i32(0); // throttle_time_ms
    }
    w.into_inner()
}
