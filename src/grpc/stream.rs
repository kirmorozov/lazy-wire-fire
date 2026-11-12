//! The call: one long-lived bidirectional stream per endpoint, over tonic's gRPC/HTTP/2.
//!
//! # Why the stream is long-lived
//!
//! A push is acknowledged by sequence number, so an item's answer can come whenever the server
//! has one: lazywf writes a batch, goes on to the next, and matches the acknowledgements as they
//! arrive. That only pays if the call stays open — one HTTP/2 stream, its headers sent once,
//! batches pipelined behind each other — so the stream is opened on the first flush and kept
//! until the server or the network ends it. The next flush opens another, and its sequence
//! numbers start at 1 again: the hash an item carries ([`crate::grpc::contract`]) is what tells
//! two streams' items apart, and a server that has a key and a hash it has already stored can
//! acknowledge the duplicate without doing the work again.
//!
//! # Who owns what
//!
//! tonic's channel runs the connection (and re-establishes it when the network ends it); on top
//! of it two tasks serve one stream, and the flush that opens the stream hands them what they
//! need:
//!
//! - the **request stream**: flushes hand whole envelopes to a queue bounded by the window
//!   below, so a server that stops reading backs the flushes up rather than filling memory;
//! - the **reader**: the response half, frames in, [`Acks`] out, each answer written into the
//!   batch it belongs to and the waiters woken, and the moment the stream ends the reason
//!   recorded for everyone.
//!
//! A flush does none of that itself: it reserves sequence numbers and window, registers its
//! items, hands over one envelope, and waits to be told. Many flushes can be in flight at
//! once, each waiting for its own answers, and the server chooses the pace. Nothing waits to
//! fill a batch: what is ready when a flush happens is what goes.
//!
//! # The window
//!
//! At most [`WINDOW_ITEMS`] items and [`WINDOW_BYTES`] bytes may be unacknowledged at a time,
//! through a ring buffer indexed by the sequence number: an acknowledgement is an array lookup,
//! never a hash. It is what turns a server that has stopped acknowledging into a flush that
//! waits rather than a client that queues work without end, and it bounds what a reconnect has
//! to send again. A single flush bigger than the window still goes — the engine's own batch
//! caps are tighter than these by default, and a flush must not deadlock on itself.
//!
//! # What the server's answer means
//!
//! Only a verdict on the item itself is allowed to refuse a key, so the rule the whole module
//! follows is: an item is refused when the server says `REFUSED` for its `seq` and `hash`, and
//! everything else — a stream that ends, an acknowledgement that never comes, an acknowledgement
//! that cannot belong to anything (an unknown `seq`, a `hash` that is not the one sent, a status
//! lazywf does not have), a deadline that passes, a reconnect still waiting on its backoff — is
//! a key waiting to be sent again.
//!
//! # Compression
//!
//! The design compresses above a size threshold only, and gRPC's own compression is a property
//! of a whole stream, not of one message. So the per-message half lives here, in [`Packer`], a
//! body between tonic's encoder and the channel: it parses the framed messages back out and
//! replaces those at or over `compress_min_bytes` with their compressed bytes and a set
//! compressed-flag — exactly the wire tonic itself would have written, decided per message
//! instead of per stream. Reading a server that compresses its answers is left to tonic
//! (`accept_compression` below). The `auto` policy starts at the codec that saves most and
//! steps down when the server answers `UNIMPLEMENTED` naming a compression it does not have.

use std::collections::VecDeque;
use std::future::Future as StdFuture;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::StreamExt as _;
use http::header::HeaderValue;
use http_body::{Body as HttpBody, Frame};
use prost::Message as _;
use tokio::sync::{mpsc, Mutex as TMutex, Notify, OnceCell};
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body as GrpcBody;
use tonic::client::Grpc;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request as TonicRequest, Status, Streaming};
use tower_service::Service as TowerService;

use crate::grpc::contract::{self, Ack, Acks, Envelope, Status as AckStatus};
use crate::grpc::wire::{self, Codec, Failure};
use crate::secrets;
use crate::tls;

/// How many items may be unacknowledged at once: deep enough that the round trip stops
/// mattering, shallow enough that a reconnect has little to send again. It is also the depth of
/// the flush queue, which is what makes a stalled server back the flushes up rather than fill
/// memory.
const WINDOW_ITEMS: usize = 512;

/// How much of it may be unacknowledged in bytes: the second bound, so a server that stops
/// acknowledging costs megabytes of re-send, not a machine.
const WINDOW_BYTES: usize = 8 << 20;

/// The ring: slots indexed by the low bits of the sequence number. Twice the item window, so a
/// number in the window cannot share a slot with another one in it; an acknowledgement for a
/// number the ring no longer holds finds its slot empty or holding a newer item, and is a
/// duplicate by either reading — and ignored.
const RING_SLOTS: usize = 2 * WINDOW_ITEMS;
const RING_MASK: usize = RING_SLOTS - 1;

/// The reconnect backoff of a whole endpoint: a server that is down is asked again on a curve,
/// not at every flush.
const BACKOFF_START: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The HTTP/2 flow-control windows lazywf advertises: what a server needs to keep an
/// acknowledgements stream moving without a window update every few kilobytes. The item side is
/// bounded by the window above and the target's message limit instead.
const STREAM_WINDOW: u32 = 1 << 20;
const CONNECTION_WINDOW: u32 = 8 << 20;

/// What the connection does while no flush is moving: ask every 30 s whether the other side is
/// there, so a load balancer that drops idle streams is noticed on the connection and the pipe
/// stays warm.
const KEEP_ALIVE: Duration = Duration::from_secs(30);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// The codecs `auto` walks, in the order it asks for them: the one that saves most, then the
/// one every implementation has, then nothing.
const LADDER: [Codec; 3] = [Codec::Zstd, Codec::Gzip, Codec::Identity];

/// One item of a flush: the key, and the document already encoded as the target's item message.
#[derive(Clone)]
pub(crate) struct Outbound {
    pub(crate) key: String,
    pub(crate) body: Vec<u8>,
}

/// What the server said about one item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The server's own answer, with the two numbers that identify the item it is about: the
    /// rule's `expect` has the last say on what `OK` means, as for every other kind.
    Ack {
        seq: u64,
        hash: u64,
        status: AckStatus,
        reason: String,
    },
    /// Nothing was said about it: the stream ended, the acknowledgement never came, the
    /// deadline passed. Never a strike.
    Lost(String),
}

impl Verdict {
    /// Whether the server said nothing about this item, which is the one outcome that must not
    /// cost a key.
    fn is_lost(&self) -> bool {
        matches!(self, Verdict::Lost(_))
    }
}

/// The answer to one flush.
pub(crate) struct Outcome {
    /// One per item, in the order they were given.
    pub(crate) verdicts: Vec<Verdict>,
    /// The whole call was judged wrong rather than this moment being bad: a status that refuses
    /// the request itself. The keys take a strike — waiting would bring the same answer.
    pub(crate) refused: Option<String>,
}

impl Outcome {
    /// Every item of a flush lost to one failure of the call. A refusal of the whole call
    /// travels once, in `refused`, for the caller to turn into the action's error.
    fn all(n: usize, f: &Failure) -> Outcome {
        Outcome {
            verdicts: (0..n).map(|_| Verdict::Lost(f.why.clone())).collect(),
            refused: f.refuse.then(|| f.why.clone()),
        }
    }

    /// Every item lost to a moment, with what to tell the keys.
    fn waiting(n: usize, why: String) -> Outcome {
        Outcome {
            verdicts: (0..n).map(|_| Verdict::Lost(why.clone())).collect(),
            refused: None,
        }
    }

    fn empty() -> Outcome {
        Outcome {
            verdicts: Vec::new(),
            refused: None,
        }
    }
}

/// What a stream is and how to call it: built once with the target, read by every task of every
/// stream it opens.
pub(crate) struct Shared {
    /// The endpoint as written: the name in every message.
    pub(crate) url: String,
    /// The gRPC path: `/<package.Service>/<Method>`.
    pq: http::uri::PathAndQuery,
    /// The origin the channel dials (`http(s)://host:port`): the scheme says TLS or not; the
    /// certificate, the verification mode and the server name are the connector's own, from
    /// `src/tls.rs`, as for every other kind.
    uri: String,
    connector: tls::Connector,
    /// The bearer token *reference* of `vars.token` (`None`: no `authorization` metadata),
    /// resolved when a stream is opened: a rotated file is picked up at the next reconnect,
    /// and the value itself is never kept.
    token: Option<String>,
    /// The codec `vars.compression` names, or `None` for `auto`, which walks [`LADDER`].
    ask: Option<Codec>,
    /// The size a message has to reach before compressing it pays.
    pub(crate) min_bytes: usize,
    /// The largest message the server takes.
    pub(crate) max_message_bytes: usize,
    /// How long one flush waits for its acknowledgements.
    pub(crate) timeout: Duration,
    /// Where `auto` has got to: an index into [`LADDER`], walked when a server answers that it
    /// does not have the codec lazywf asked for.
    step: AtomicUsize,
    /// The channel, built on first use (it wants a runtime, and a target loads without one).
    channel: OnceCell<Channel>,
}

/// What a target of kind `grpc` asks of its one endpoint. `None` compression means `auto`.
pub(crate) struct Endpoint0 {
    pub(crate) url: String,
    pub(crate) path: String,
    pub(crate) secure: bool,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) connector: tls::Connector,
    pub(crate) token: Option<String>,
    pub(crate) compression: Option<Codec>,
    pub(crate) min_bytes: usize,
    pub(crate) max_message_bytes: usize,
    pub(crate) timeout: Duration,
}

impl Shared {
    pub(crate) fn new(ep: Endpoint0) -> Shared {
        Shared {
            url: ep.url,
            pq: ep
                .path
                .parse()
                .expect("the endpoint path of `Addr` is a valid path and query"),
            uri: format!(
                "{}://{}:{}",
                if ep.secure { "https" } else { "http" },
                ep.host,
                ep.port
            ),
            connector: ep.connector,
            token: ep.token,
            ask: ep.compression,
            min_bytes: ep.min_bytes,
            max_message_bytes: ep.max_message_bytes,
            timeout: ep.timeout,
            step: AtomicUsize::new(0),
            channel: OnceCell::new(),
        }
    }

    /// The codec to send with now.
    fn codec(&self) -> Codec {
        match self.ask {
            Some(c) => c,
            None => LADDER[self.step.load(Ordering::Relaxed).min(LADDER.len() - 1)],
        }
    }

    /// One rung down the ladder, for the server that answered it does not have the codec: only
    /// `auto` walks, because a target that was *told* to use zstd should hear about a server
    /// without it rather than be sent as identity quietly. `false` at the bottom, where there
    /// is nothing left to take away.
    fn step_down(&self) -> bool {
        if self.ask.is_some() {
            return false;
        }
        let at = self.step.load(Ordering::Relaxed);
        if at + 1 >= LADDER.len() {
            return false;
        }
        self.step.store(at + 1, Ordering::Relaxed);
        true
    }

    /// The endpoint's channel, built on first use. Lazy: it spawns its worker, which wants a
    /// runtime, and a target is loaded (and its schema checked) without one being near.
    async fn channel(&self) -> Result<Channel, Failure> {
        self.channel
            .get_or_try_init(|| async {
                let mut ep = Endpoint::from_shared(self.uri.clone())
                    .map_err(|e| wire::moment(format!("`{}`: {e}", self.url)))?;
                ep = ep
                    .tcp_nodelay(true)
                    .initial_stream_window_size(STREAM_WINDOW)
                    .initial_connection_window_size(CONNECTION_WINDOW)
                    .http2_keep_alive_interval(KEEP_ALIVE)
                    .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
                    .keep_alive_while_idle(true);
                let ep = match ep
                    .clone()
                    .user_agent(format!("lazywf/{}", env!("CARGO_PKG_VERSION")))
                {
                    Ok(ep) => ep,
                    Err(_) => ep,
                };
                // The connector is lazywf's own (src/tls.rs): every TLS mode the `tls` column
                // can name, the CA files it can name, and the server name it can override —
                // none of which tonic's own TLS settings can say. It is asked per connection,
                // so a broken CA file says so when a stream opens, not never.
                Ok(ep.connect_with_connector_lazy(Pipe {
                    connector: self.connector.clone(),
                    timeout: self.timeout,
                }))
            })
            .await
            .cloned()
    }
}

/// The stream of one endpoint: opened on the first flush, replaced when it dies.
pub(crate) struct Call {
    shared: Arc<Shared>,
    /// The live stream, behind a lock so concurrent flushes open one stream rather than each
    /// their own — and so the codec the server is met with is decided by exactly one of them.
    live: TMutex<Option<Arc<Live>>>,
    /// How many streams this endpoint has had, for the log.
    opened: AtomicU64,
    /// The reconnect curve of failed stream openings (see [`BACKOFF_START`]).
    backoff: StdMutex<Backoff>,
}

struct Backoff {
    fails: u32,
    until: tokio::time::Instant,
}

impl Call {
    pub(crate) fn new(shared: Arc<Shared>) -> Call {
        Call {
            shared,
            live: TMutex::new(None),
            opened: AtomicU64::new(0),
            backoff: StdMutex::new(Backoff {
                fails: 0,
                until: tokio::time::Instant::now(),
            }),
        }
    }

    /// Sends one flush and waits until every item has a verdict.
    ///
    /// The sequence numbers are given out per stream, and the frame is one `Envelope` of the
    /// items in the order they were given. A flush whose stream died before its answers came
    /// reports every item it has no verdict for as lost: the keys go back to the queue and go
    /// again, on a stream that will be opened fresh — at least once, which is what the hash
    /// each item carries lets a server sort out again.
    pub(crate) async fn push(&self, items: &[Outbound]) -> Outcome {
        if items.is_empty() {
            return Outcome::empty();
        }
        for attempt in 0..2 {
            let live = match self.stream().await {
                Ok(l) => l,
                Err(f) => {
                    // The same second chance as below, for a server that answers as soon as
                    // the stream opens that it does not know the codec.
                    if attempt == 0 && f.demote && self.shared.step_down() {
                        tracing::warn!(
                            endpoint = %self.shared.url,
                            codec = self.shared.codec().wire_name(),
                            "grpc: the server does not have this codec, trying the next one down"
                        );
                        continue;
                    }
                    return Outcome::all(items.len(), &f);
                }
            };
            let out = push_once(&live, items).await;
            // The one retry worth having: the server answered that it does not have the codec
            // lazywf chose, and said so before it had taken anything. That is lazywf's mistake,
            // so lazywf fixes it and asks again rather than telling the keys anything.
            if attempt == 0
                && out.refused.is_none()
                && out.verdicts.iter().all(|v| v.is_lost())
                && live.codec != Codec::Identity
                && live.tail.demote.load(Ordering::Relaxed)
                && self.shared.step_down()
            {
                tracing::warn!(
                    endpoint = %self.shared.url,
                    codec = live.codec.wire_name(),
                    "grpc: the server does not have this codec, trying the next one down"
                );
                self.reset(&live).await;
                continue;
            }
            return out;
        }
        unreachable!("every pass of this loop returns")
    }

    /// The live stream, opening one if there is none or the last one is over.
    async fn stream(&self) -> Result<Arc<Live>, Failure> {
        let mut guard = self.live.lock().await;
        if let Some(live) = guard.as_ref() {
            if live.tail.over_now().is_none() {
                return Ok(Arc::clone(live));
            }
            // Over: dropped here, and `Drop` stops its tasks.
        }
        let wait = self.backoff.lock().unwrap().until;
        let now = tokio::time::Instant::now();
        if now < wait {
            let remaining = wait - now;
            if remaining > self.shared.timeout {
                return Err(wire::moment(format!(
                    "lazywf is waiting {remaining:?} before dialing `{}` again (the last attempt failed)",
                    self.shared.url
                )));
            }
            // A backoff short enough to sit out inside the flush's own budget is sat out:
            // one failed dial should cost the next flush its rest, not its life.
            tokio::time::sleep(remaining).await;
        }
        let id = self.opened.fetch_add(1, Ordering::Relaxed) + 1;
        match establish(Arc::clone(&self.shared), id).await {
            Ok(live) => {
                self.backoff.lock().unwrap().fails = 0;
                *guard = Some(Arc::clone(&live));
                Ok(live)
            }
            Err(f) => {
                // Only a stream that could not be dialed at all backs off: a server that
                // answered — even with UNIMPLEMENTED naming a codec lazywf must now drop —
                // is a server that is up, and the next pass wants to meet it now.
                if !f.demote && !f.refuse {
                    let mut b = self.backoff.lock().unwrap();
                    b.fails = b.fails.saturating_add(1);
                    let step = BACKOFF_START
                        .saturating_mul(1u32 << b.fails.min(8))
                        .min(BACKOFF_MAX);
                    b.until = tokio::time::Instant::now() + step;
                }
                Err(f)
            }
        }
    }

    /// Forgets a stream, so the next flush opens one rather than writing to a dead one.
    async fn reset(&self, live: &Arc<Live>) {
        let mut guard = self.live.lock().await;
        if guard.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, live)) {
            *guard = None;
        }
    }
}

/// One attempt on one stream: make room, write, wait.
async fn push_once(live: &Arc<Live>, items: &[Outbound]) -> Outcome {
    let tail = &live.tail;
    let shared = &tail.shared;
    let n = items.len();
    let deadline = tokio::time::Instant::now() + shared.timeout;

    // Room in the window first, before anything is reserved: a server that has stopped
    // acknowledging is a flush that waits, not a client with an unbounded queue of work.
    let reserved = loop {
        if let Some(f) = tail.over_now() {
            return Outcome::all(n, &f);
        }
        if let Some(r) = tail.reserve(items) {
            break r;
        }
        if !wait(tail, deadline).await {
            return Outcome::waiting(
                n,
                format!(
                    "the stream is full: {} items are still waiting for their acknowledgements \
                     (the target's window)",
                    tail.pending()
                ),
            );
        }
    };

    if let Err(e) = wire::ensure_frame_len(reserved.bytes, shared.max_message_bytes) {
        // Nothing of this flush ever reached the wire, so the keys are no worse off: they wait,
        // and nothing is said about them.
        tail.forget(&reserved.batch);
        return Outcome::waiting(n, format!("{e:#} ({n} items, {} bytes)", reserved.bytes));
    }
    if let Some(f) = tail.over_now() {
        tail.forget(&reserved.batch);
        return Outcome::all(n, &f);
    }

    // Hand the envelope over. The queue is bounded by the window, and the window has room
    // because it was just taken: this waits only while the connection itself is behind.
    if tokio::time::timeout_at(deadline, live.tx.send(reserved.envelope))
        .await
        .is_err()
    {
        tail.forget(&reserved.batch);
        return Outcome::waiting(n, "the server stopped reading the stream".to_string());
    }

    // And wait for the answers, on the one notification the reader rings.
    let mut late = None;
    loop {
        if reserved.batch.answered() {
            break;
        }
        if let Some(f) = tail.over_now() {
            reserved.batch.lose(&f.why);
            break;
        }
        let woken = tail.wake.notified();
        tokio::pin!(woken);
        // Registered before the next look, so an answer arriving in between is not missed.
        woken.as_mut().enable();
        if reserved.batch.answered() {
            break;
        }
        tokio::select! {
            _ = woken => {}
            _ = tokio::time::sleep_until(deadline) => {
                late = Some(format!(
                    "no acknowledgement for {} of {n} items within {:?}",
                    reserved.batch.unanswered(),
                    shared.timeout
                ));
                break;
            }
        }
    }

    // Whatever has no verdict is this flush's business to let go of: its items leave the
    // window and the ring, and an acknowledgement that arrives later for one of them is the
    // duplicate the reader ignores.
    tail.forget(&reserved.batch);
    let mut verdicts = reserved.batch.take();
    if let Some(why) = late {
        for v in verdicts.iter_mut().filter(|v| v.is_lost()) {
            *v = Verdict::Lost(why.clone());
        }
    }
    Outcome {
        verdicts,
        refused: None,
    }
}

/// One flush's worth of answers, shared by the flush that waits on them and the reader that
/// writes them.
struct Batch {
    /// One per item, in the order they were sent: `None` until the server has said something.
    answers: StdMutex<Vec<Option<Verdict>>>,
    /// What the batch took out of the window: the window comes back exactly when the flush is
    /// done with it, by these numbers and not by counting the ring.
    items: usize,
    bytes: usize,
}

impl Batch {
    fn new(items: usize, bytes: usize) -> Batch {
        Batch {
            answers: StdMutex::new((0..items).map(|_| None).collect()),
            items,
            bytes,
        }
    }

    fn set(&self, idx: usize, verdict: Verdict) {
        let mut a = self.answers.lock().unwrap();
        if let Some(slot) = a.get_mut(idx) {
            if slot.is_none() {
                *slot = Some(verdict);
            }
        }
    }

    fn answered(&self) -> bool {
        self.answers.lock().unwrap().iter().all(Option::is_some)
    }

    fn unanswered(&self) -> usize {
        self.answers
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v.is_none())
            .count()
    }

    /// The whole stream is over: whatever has no verdict has no verdict to wait for.
    fn lose(&self, why: &str) {
        let mut a = self.answers.lock().unwrap();
        for slot in a.iter_mut() {
            if slot.is_none() {
                *slot = Some(Verdict::Lost(why.to_string()));
            }
        }
    }

    fn take(&self) -> Vec<Verdict> {
        let mut a = self.answers.lock().unwrap();
        a.iter_mut()
            .map(|s| {
                s.take()
                    .unwrap_or_else(|| Verdict::Lost(ACK_NEVER.to_string()))
            })
            .collect()
    }
}

const ACK_NEVER: &str = "the stream ended before this item was acknowledged";

/// One registered item: the number and hash it was sent with, and where its flush waits. An
/// empty slot is one no item holds; `seq` 0 is never sent, so it means empty.
#[derive(Clone)]
struct Slot {
    seq: u64,
    hash: u64,
    batch: Option<Arc<Batch>>,
    idx: usize,
}

const EMPTY_SLOT: Slot = Slot {
    seq: 0,
    hash: 0,
    batch: None,
    idx: 0,
};

/// The half of a stream both its tasks and every waiting flush touch: who is in flight, and
/// whether the stream is over.
struct Tail {
    shared: Arc<Shared>,
    state: StdMutex<State>,
    /// Rings when an answer arrives, when the stream ends, and when the window takes something
    /// back out — everyone waiting re-looks at its own state.
    wake: Notify,
    /// The server answered `UNIMPLEMENTED` naming a compression: the ladder steps down (see
    /// [`Call::push`]).
    demote: AtomicBool,
}

struct State {
    /// The last sequence number given out on this stream: from 0, by ones, restarting when the
    /// stream does.
    next: u64,
    /// Why the stream is over, once it is.
    over: Option<Failure>,
    slots: Vec<Slot>,
    pending_items: usize,
    pending_bytes: usize,
}

/// What one flush reserved: the envelope to hand to the stream and where its answers land.
struct Reservation {
    envelope: Envelope,
    batch: Arc<Batch>,
    /// The size of the message as it goes on the wire, header included.
    bytes: usize,
}

impl Tail {
    fn new(shared: Arc<Shared>) -> Tail {
        Tail {
            shared,
            state: StdMutex::new(State {
                next: 0,
                over: None,
                slots: vec![EMPTY_SLOT; RING_SLOTS],
                pending_items: 0,
                pending_bytes: 0,
            }),
            wake: Notify::new(),
            demote: AtomicBool::new(false),
        }
    }

    fn over_now(&self) -> Option<Failure> {
        let st = self.state.lock().unwrap();
        st.over.as_ref().map(|f| Failure {
            refuse: f.refuse,
            demote: f.demote,
            why: f.why.clone(),
        })
    }

    fn pending(&self) -> usize {
        self.state.lock().unwrap().pending_items
    }

    /// Puts a flush's items in the ring and takes them out of the window, or answers `None`
    /// when the window has no room for all of them (or the stream is over): nothing changes
    /// in that case, sequence numbers included.
    ///
    /// One lookup per item, none of them a hash: the ring is indexed by the sequence number.
    fn reserve(&self, items: &[Outbound]) -> Option<Reservation> {
        let mut st = self.state.lock().unwrap();
        if st.over.is_some() {
            return None;
        }
        let n = items.len();
        let from = st.next + 1;
        let wire: Vec<contract::Item> = items
            .iter()
            .enumerate()
            .map(|(i, it)| {
                let seq = from + i as u64;
                let hash = contract::hash(it.key.as_bytes(), &it.body);
                contract::Item {
                    seq,
                    hash,
                    key: it.key.clone(),
                    item: it.body.clone(),
                }
            })
            .collect();
        let envelope = Envelope { items: wire };
        let bytes = envelope.encoded_len() + 5; // + the five bytes of the gRPC frame header
        let fits = st.pending_items + n <= WINDOW_ITEMS && st.pending_bytes + bytes <= WINDOW_BYTES;
        // A single flush bigger than the window still goes: a flush must not deadlock on
        // itself, and the engine's batch caps are the tighter bound in practice.
        if !fits && st.pending_items != 0 {
            return None;
        }
        // Every sequence number this flush may be answered for has to have its slot free: a
        // number in the window cannot share a slot with another number in it (the ring is
        // twice the window), so a busy slot means the ring and the counters have drifted and
        // the safe answer is to say the window is full.
        for item in &envelope.items {
            let slot = &st.slots[(item.seq as usize) & RING_MASK];
            if slot.batch.is_some() {
                tracing::warn!(
                    endpoint = %self.shared.url,
                    seq = item.seq,
                    "grpc: a ring slot was still held; treating the window as full"
                );
                return None;
            }
        }
        let batch = Arc::new(Batch::new(n, bytes));
        for (i, item) in envelope.items.iter().enumerate() {
            st.slots[(item.seq as usize) & RING_MASK] = Slot {
                seq: item.seq,
                hash: item.hash,
                batch: Some(Arc::clone(&batch)),
                idx: i,
            };
        }
        st.next = from + n as u64 - 1;
        st.pending_items += n;
        st.pending_bytes += bytes;
        Some(Reservation {
            envelope,
            batch,
            bytes,
        })
    }

    /// Takes a finished flush back out of the window and the ring. Answers that arrive for its
    /// items after this are the duplicates the reader ignores.
    fn forget(&self, batch: &Arc<Batch>) {
        let mut st = self.state.lock().unwrap();
        let mut freed = false;
        for slot in st.slots.iter_mut() {
            if slot.batch.as_ref().is_some_and(|b| Arc::ptr_eq(b, batch)) {
                slot.batch = None;
                slot.seq = 0;
                slot.hash = 0;
                slot.idx = 0;
                freed = true;
            }
        }
        if freed {
            // Exactly what `reserve` took: each flush accounts for its envelope once, so the
            // window never drifts and never goes below zero.
            st.pending_items -= batch.items;
            st.pending_bytes -= batch.bytes;
            drop(st);
            self.wake.notify_waiters();
        }
    }

    /// The stream is over, for whatever reason: nobody still waiting has an answer to wait
    /// for, and every item without one is lost — never refused, whoever closed the stream.
    fn close(&self, f: Failure) {
        let batches: Vec<Arc<Batch>> = {
            let mut st = self.state.lock().unwrap();
            if st.over.is_some() {
                return;
            }
            st.over = Some(f.clone());
            st.slots.iter().filter_map(|s| s.batch.clone()).collect()
        };
        for batch in &batches {
            batch.lose(&f.why);
        }
        self.wake.notify_waiters();
    }

    /// Writes one acknowledgement into the batch it belongs to, if it belongs to anything:
    /// an unknown `seq`, a slot holding a newer item, or a `hash` that is not the one the item
    /// was sent with all mean this is not an answer lazywf may believe. Believing one of
    /// those could cost a key twice over — a stale ack confirming a newer document — so all
    /// of them are ignored, and the deadline answers for the item instead.
    fn answer(&self, ack: &Ack) {
        let status = AckStatus::from_wire(ack.status);
        if status == AckStatus::Unspecified {
            // A server that answers with nothing has not judged the item: it keeps its place
            // and the deadline says the rest.
            tracing::debug!(
                endpoint = %self.shared.url,
                seq = ack.seq,
                "grpc: an acknowledgement with no status, ignored"
            );
            return;
        }
        let st = self.state.lock().unwrap();
        let slot = &st.slots[(ack.seq as usize) & RING_MASK];
        let Some(batch) = slot.batch.as_ref() else {
            tracing::debug!(
                endpoint = %self.shared.url,
                seq = ack.seq,
                "grpc: an acknowledgement for an item never sent (or already answered), ignored"
            );
            return;
        };
        if slot.seq != ack.seq {
            tracing::debug!(
                endpoint = %self.shared.url,
                ack_seq = ack.seq,
                slot_seq = slot.seq,
                "grpc: an acknowledgement for a sequence number the ring does not hold, ignored"
            );
            return;
        }
        if slot.hash != ack.hash {
            // The one server bug that used to be able to lose data: an ack matching the
            // position but not the content. It gets no verdict out of lazywf either way.
            tracing::warn!(
                endpoint = %self.shared.url,
                seq = ack.seq,
                "grpc: an acknowledgement whose hash is not the one the item was sent with, ignored"
            );
            return;
        }
        let (batch, idx) = (Arc::clone(batch), slot.idx);
        let (seq, hash) = (slot.seq, slot.hash);
        drop(st);
        batch.set(
            idx,
            Verdict::Ack {
                seq,
                hash,
                status,
                reason: wire::cap(&ack.reason, wire::REASON_CHARS),
            },
        );
    }
}

/// The stream itself: the open call, its queue of envelopes and its reader.
pub(crate) struct Live {
    tail: Arc<Tail>,
    tx: mpsc::Sender<Envelope>,
    /// The codec the server was met with when the stream opened: the demotion of
    /// [`Call::push`] is worth trying only when it was something.
    codec: Codec,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for Live {
    fn drop(&mut self) {
        self.reader.abort();
        self.tail.close(wire::moment(format!(
            "the stream to `{}` was closed",
            self.tail.shared.url
        )));
    }
}

/// Opens one stream: the call, and the reader that drains its acknowledgements.
async fn establish(shared: Arc<Shared>, id: u64) -> Result<Arc<Live>, Failure> {
    let channel = shared.channel().await?;
    let codec = shared.codec();
    let (tx, rx) = mpsc::channel::<Envelope>(WINDOW_ITEMS);
    let tail = Arc::new(Tail::new(Arc::clone(&shared)));

    let mut request = TonicRequest::new(ReceiverStream::new(rx));
    if let Some(reference) = &shared.token {
        // `resolve`, not `read_bytes`: `vars.token` is text and may be either reference form
        // (`env:NAME` or `file:/path`), which is exactly what `check` accepted at load; only
        // the descriptor set is bytes. Read at open, so a rotation is picked up by the next
        // stream, and the failure quotes the variable's name or the path, never the value.
        let value = secrets::resolve(reference)
            .map_err(|e| wire::moment(format!("reading `vars.token`: {e:#}")))?;
        let value = format!("Bearer {}", value.trim_end_matches(['\r', '\n']));
        // Sensitive: the value is never to appear in a debug of the map, the log or a panic.
        let mut value = MetadataValue::try_from(value.as_str())
            .map_err(|_| wire::moment("`vars.token` holds bytes no header may carry"))?;
        value.set_sensitive(true);
        request.metadata_mut().insert("authorization", value);
    }

    let mut grpc = Grpc::new(Wire::new(channel, codec, shared.min_bytes))
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(shared.max_message_bytes)
        .max_encoding_message_size(shared.max_message_bytes);
    let opened = tokio::time::timeout(
        shared.timeout,
        grpc.streaming(
            request,
            shared.pq.clone(),
            tonic_prost::ProstCodec::default(),
        ),
    )
    .await
    .map_err(|_| {
        wire::moment(format!(
            "`{}` did not open the stream within {:?}",
            shared.url, shared.timeout
        ))
    })?
    .map_err(|s| whole_call(&s))?;
    tracing::debug!(
        endpoint = %shared.url,
        stream = id,
        codec = codec.wire_name(),
        "grpc: the stream is open"
    );
    let (_, stream, _) = opened.into_parts();
    let reader = tokio::spawn(read(stream, Arc::clone(&tail)));
    Ok(Arc::new(Live {
        tail,
        tx,
        codec,
        reader,
    }))
}

/// A whole call's own answer, as far as the keys are concerned: refused only when the status
/// says the request itself, transient whenever it says the moment — and a status lazywf has
/// never heard of is always the moment. The codec refusal marks itself for the ladder.
fn whole_call(s: &Status) -> Failure {
    let f = wire::failure(s.code(), s.message());
    if f.demote {
        // Recorded here too: a server that answers before it reads anything fails the call,
        // and there is no reader to have noticed.
        tracing::debug!(code = ?s.code(), "grpc: the server refused the message codec");
    }
    f
}

/// The response half: acknowledgements into the ring, and the reason the stream ended when it
/// ends — by the server, by the network, or by an answer lazywf cannot read, which is a moment
/// and not a verdict whatever it is.
async fn read(mut stream: Streaming<Acks>, tail: Arc<Tail>) {
    while let Some(next) = stream.next().await {
        match next {
            Ok(acks) => {
                for ack in &acks.acks {
                    tail.answer(ack);
                }
                tail.wake.notify_waiters();
            }
            Err(status) => {
                let f = whole_call(&status);
                if f.demote {
                    tail.demote.store(true, Ordering::Relaxed);
                }
                tracing::debug!(
                    endpoint = %tail.shared.url,
                    code = ?status.code(),
                    "grpc: the stream ended"
                );
                tail.close(f);
                return;
            }
        }
    }
    tail.close(wire::moment(format!(
        "the server closed the acknowledgement stream of `{}`",
        tail.shared.url
    )));
}

/// Waits for a ring-up (an answer, the stream ending, the window freeing) or the deadline;
/// `false` when the deadline got there first.
async fn wait(tail: &Tail, deadline: tokio::time::Instant) -> bool {
    let woken = tail.wake.notified();
    tokio::pin!(woken);
    // Registered before the caller looks again, so a ring-up in between is not missed.
    woken.as_mut().enable();
    tokio::select! {
        _ = woken => true,
        _ = tokio::time::sleep_until(deadline) => false,
    }
}

/// The step between tonic's encoder and the channel: it says what the stream was opened with
/// (`grpc-encoding`), lets [`Packer`] decide per message whether it is worth compressing,
/// and intercepts the answer that is plain HTTP rather than gRPC — a proxy replying 404 or
/// 503 with a page and no `grpc-status` trailer, which tonic alone would report as the
/// meaningless "missing expected content-type". The error is tonic's boxed form because
/// `Status::from_error_generic` passes a boxed [`Status`] through as itself, which is how a
/// [`wire::http_answer`] reaches the outcome rules intact.
#[derive(Clone)]
struct Wire {
    channel: Channel,
    codec: Codec,
    min_bytes: usize,
}

/// How many bytes of a not-gRPC answer's body are quoted into the status: enough to tell an
/// nginx page from an Envoy one, not enough to turn a key's status into a web page.
const EXCERPT_BYTES: usize = 512;

/// tonic's own boxed error type, which it does not export: what a service handed to
/// `Grpc` must fail a call with, and what `Status::from_error` inspects — a boxed
/// [`Status`] comes back out of it as itself.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How long such a body is given to arrive: a peer that sends the headers of a fake HTTP
/// answer and then stalls must not hold the flush open.
const EXCERPT_WAIT: Duration = Duration::from_secs(1);

/// The first bytes of the body no gRPC answer should have, best effort in size and in time:
/// what is not there by the deadline is simply not quoted.
async fn excerpt(body: &mut GrpcBody) -> String {
    let read = async {
        let mut buf: Vec<u8> = Vec::new();
        let mut body = std::pin::pin!(body);
        while buf.len() < EXCERPT_BYTES {
            match std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
                Some(Ok(frame)) => {
                    if let Some(data) = frame.data_ref() {
                        let take = &data[..data.len().min(EXCERPT_BYTES - buf.len())];
                        buf.extend_from_slice(take);
                    }
                }
                // A broken or ended body says nothing more; the status alone stands.
                _ => break,
            }
        }
        buf
    };
    let buf = tokio::time::timeout(EXCERPT_WAIT, read)
        .await
        .unwrap_or_default();
    String::from_utf8_lossy(&buf).trim().to_string()
}

impl Wire {
    fn new(channel: Channel, codec: Codec, min_bytes: usize) -> Wire {
        Wire {
            channel,
            codec,
            min_bytes,
        }
    }
}

impl TowerService<http::Request<GrpcBody>> for Wire {
    type Response = http::Response<GrpcBody>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn StdFuture<Output = Result<http::Response<GrpcBody>, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        TowerService::<http::Request<GrpcBody>>::poll_ready(&mut self.channel, cx)
            .map_err(Into::into)
    }

    fn call(&mut self, request: http::Request<GrpcBody>) -> Self::Future {
        let (mut parts, body) = request.into_parts();
        if self.codec != Codec::Identity {
            // The codec the compressed messages of this stream are in, as the wire names it.
            // Messages under the threshold keep their flag clear, as the protocol allows.
            parts.headers.insert(
                "grpc-encoding",
                HeaderValue::from_static(self.codec.wire_name()),
            );
        }
        let body = GrpcBody::new(Packer {
            inner: Box::pin(body),
            codec: self.codec,
            min: self.min_bytes,
            carry: BytesMut::new(),
            out: VecDeque::new(),
            done: false,
        });
        let request = http::Request::from_parts(parts, body);
        let mut channel = self.channel.clone();
        Box::pin(async move {
            // The channel is a buffer service: it must be polled ready on the very handle
            // the request is then sent through, and this one is fresh from the clone —
            // tonic polled *Wire*, not this.
            std::future::poll_fn(|cx| {
                TowerService::<http::Request<GrpcBody>>::poll_ready(&mut channel, cx)
            })
            .await
            .map_err(Into::<BoxError>::into)?;
            let response = TowerService::<http::Request<GrpcBody>>::call(&mut channel, request)
                .await
                .map_err(Into::<BoxError>::into)?;
            if response.status() != http::StatusCode::OK {
                // Not gRPC at all. A gRPC answer travels under a 200 with its verdict in
                // the trailers; anything else is someone else's answer, and their HTTP
                // status is the whole of it.
                let status = response.status();
                let mut body = response.into_body();
                let quote = excerpt(&mut body).await;
                return Err(Box::new(wire::http_answer(status, &quote)) as BoxError);
            }
            Ok(response)
        })
    }
}

/// The per-message half of compression: tonic frames the messages of a whole stream with one
/// decision, and the design asks for one per message — above the threshold compressed, below
/// it as it is, because the CPU of compressing a one-item envelope costs more than the bytes
/// it would save. This parses the frames back out between the encoder and the connection and
/// replaces the ones worth it with their compressed bytes and a set flag; what comes back is
/// exactly the wire tonic would have written itself, decided per message.
struct Packer {
    inner: Pin<Box<GrpcBody>>,
    codec: Codec,
    min: usize,
    /// A message tonic's encoder is still writing.
    carry: BytesMut,
    /// Framed messages ready for the connection.
    out: VecDeque<Bytes>,
    done: bool,
}

impl Packer {
    /// Moves every whole message the buffer holds out of `carry` and into `out`, compressing
    /// the ones at or over the threshold (and worth it — a codec that would make a message
    /// bigger is answered with the message as it is, flag clear).
    fn pack_ready(&mut self) {
        loop {
            if self.carry.len() < 5 {
                return;
            }
            let flag = self.carry[0];
            let len =
                u32::from_be_bytes([self.carry[1], self.carry[2], self.carry[3], self.carry[4]])
                    as usize;
            if self.carry.len() < 5 + len {
                return;
            }
            let message = self.carry.split_to(5 + len);
            // Bytes tonic's own encoder wrote: the flag is 0 and the length is the truth. A
            // flag lazywf itself did not set would be a bug in this file, so anything set
            // travels as it is rather than being compressed again.
            if flag != 0 {
                self.out.push_back(message.freeze());
                continue;
            }
            // Ask the codec with a read of the body, then commit the message or its
            // compressed replacement: `pack` only borrows, so the original bytes are still
            // there to send when it says not worth it.
            let packed = self.codec.pack(&message[5..], self.min);
            match packed {
                Ok((false, _)) => self.out.push_back(message.freeze()),
                Ok((true, packed)) => {
                    let mut frame = BytesMut::with_capacity(5 + packed.len());
                    frame.put_u8(1);
                    frame.put_u32(packed.len() as u32);
                    frame.extend_from_slice(&packed);
                    self.out.push_back(frame.freeze());
                }
                Err(e) => {
                    // Compression failed: the message is worth sending more than it is worth
                    // compressing, and either way it is the server's to refuse or take.
                    tracing::warn!(
                        codec = self.codec.wire_name(),
                        "grpc: compression failed, sending the message uncompressed: {e:#}"
                    );
                    self.out.push_back(message.freeze());
                }
            }
        }
    }
}

impl HttpBody for Packer {
    type Data = Bytes;
    type Error = Status;

    fn is_end_stream(&self) -> bool {
        self.done && self.carry.is_empty() && self.out.is_empty()
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        loop {
            if let Some(chunk) = this.out.pop_front() {
                return Poll::Ready(Some(Ok(Frame::data(chunk))));
            }
            if this.done {
                // Tonic's encoder ends on message boundaries; anything left is a bug's bytes,
                // handed on rather than dropped silently.
                if !this.carry.is_empty() {
                    return Poll::Ready(Some(Ok(Frame::data(this.carry.split().freeze()))));
                }
                return Poll::Ready(None);
            }
            match this.inner.as_mut().poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => this.done = true,
                Poll::Ready(Some(Err(e))) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(frame))) => {
                    // A gRPC client's request has no trailers; pass through anything that is
                    // not data rather than eat it.
                    let data = match frame.into_data() {
                        Ok(d) => d,
                        Err(other) => return Poll::Ready(Some(Ok(other))),
                    };
                    this.carry.extend_from_slice(&data);
                    this.pack_ready();
                }
            }
        }
    }
}

/// The dial: lazywf's own connector (src/tls.rs), so every TLS mode, CA file and server name
/// the `tls` column can name applies, as for every other kind — and ring stays the only
/// libcrypto in the tree.
#[derive(Clone)]
struct Pipe {
    connector: tls::Connector,
    timeout: Duration,
}

impl TowerService<http::Uri> for Pipe {
    type Response = hyper_util::rt::TokioIo<tls::Stream>;
    type Error = anyhow::Error;
    type Future =
        Pin<Box<dyn StdFuture<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: http::Uri) -> Self::Future {
        let connector = self.connector.clone();
        let timeout = self.timeout;
        Box::pin(async move {
            connector
                .connect(timeout)
                .await
                .map(hyper_util::rt::TokioIo::new)
        })
    }
}

#[cfg(test)]
mod tests;
