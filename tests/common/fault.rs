//! Chaos harness (design §3): an in-process TCP fault proxy, the harness fault log, the
//! child-binary launcher and the property sampler. Consumers: `tests/chaos.rs`.
//!
//! Everything here is byte-transparent and lives inside the test process: one proxy per role
//! on an ephemeral port, so tests are parallel-safe and a fault transition is an event in the
//! test's own state, never a control-plane round trip. Fault *transitions* are gated on
//! observed events (`ProxyStat` counters, mock deliveries, `/status` fields), never on
//! wall-clock sleeps; every wait carries a generous deadline.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use mysql_async::prelude::*;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::{test_server_id, Db, Mock};

/// The monotonic, in-memory record of every fault the harness injected: proxy commands, mock
/// fault-mode firings and child kills, each with the instant it happened. P4's duplicate
/// windowing is "a fault event between the two deliveries"; the tail is printed on every
/// sampler violation so a chaos failure is self-explaining without a re-run.
#[derive(Clone)]
pub struct FaultLog {
    events: Arc<Mutex<Vec<(Instant, String)>>>,
    epoch: Instant,
}

impl FaultLog {
    pub fn new() -> FaultLog {
        FaultLog {
            events: Arc::new(Mutex::new(Vec::new())),
            epoch: Instant::now(),
        }
    }

    pub fn add(&self, msg: impl Into<String>) {
        self.events
            .lock()
            .unwrap()
            .push((Instant::now(), msg.into()));
    }

    /// The last `n` events, formatted `[t=4.2s] proxy binlog cut_all()`.
    pub fn tail(&self, n: usize) -> Vec<String> {
        let ev = self.events.lock().unwrap().clone();
        ev.iter()
            .rev()
            .take(n)
            .map(|(at, m)| {
                format!(
                    "[t={:.1}s] {m}",
                    at.duration_since(self.epoch).as_secs_f64()
                )
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Whether any event's text contains `needle`: gating a next step on a fault that has
    /// already been *observed* (the mock logged that it is holding the engine's request).
    pub fn has(&self, needle: &str) -> bool {
        self.events
            .lock()
            .unwrap()
            .iter()
            .any(|(_, m)| m.contains(needle))
    }

    /// How many events contain `needle`. Where `has` would be satisfied by an earlier round's
    /// identical line, a seed gates on the count *growing*: the transition it waits for is one
    /// this step caused.
    pub fn count(&self, needle: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, m)| m.contains(needle))
            .count()
    }

    /// Whether any event falls strictly between `a` and `b` (P4's window test).
    pub fn has_between(&self, a: Instant, b: Instant) -> bool {
        self.events
            .lock()
            .unwrap()
            .iter()
            .any(|(at, _)| *at > a && *at < b)
    }
}

/// A snapshot of a proxy's observability, the input to `FaultProxy::wait` gating.
pub struct ProxyStat {
    pub label: String,
    pub mode: String,
    pub conns: u64,
    pub bytes: u64,
    /// server→client bytes only: the size of what the server actually sent, which is what a
    /// binlog group costs on the wire.
    pub bytes_s2c: u64,
    /// server→client bytes the proxy actually handed to the client. Equal to `bytes_s2c` unless
    /// something is being held back: while a stall is armed this must not move at all, which is
    /// the claim S2 asserts (the engine's own position can still advance on bytes the proxy had
    /// already delivered a moment earlier, so the stall has to be proved at the proxy).
    pub delivered_s2c: u64,
    /// Dump connections reset by `flap` so far.
    pub flap_cuts: u64,
    /// Times `cut_after_bytes` has fired (it is a one-shot, so 0 or 1).
    pub byte_cuts: u64,
    /// How many server→client bytes had flowed past the arming point when the last
    /// `cut_after_bytes` fired (`0` before one has).
    pub bytes_at_cut: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Live,
    CutAll,
    Stall,
    Refuse,
    /// S3: every accepted connection is reset, until `flap_left` of them have been.
    Flap,
}

#[derive(Clone, Copy)]
struct Mode {
    kind: Kind,
    /// Per connection, server→client: RST once this many bytes have been relayed *since the
    /// command was armed* (see `Core::cut_arm`), then the mode clears itself.
    cut_after_bytes: Option<u64>,
    /// Per direction, per relayed chunk.
    latency_ms: u64,
    /// `Kind::Flap`: connections still to reset before the proxy goes live by itself.
    flap_left: u64,
}

struct Core {
    label: String,
    upstream: String,
    mode: Mutex<Mode>,
    /// Bumped by `cut_all`; relay tasks born before it hard-close themselves.
    gen: AtomicU64,
    /// Bumped by `cut_after_bytes`: a relay task catches up to it by taking its own server→client
    /// count as the baseline, so the byte bound is the distance *from the moment it was armed* —
    /// the only meaning on an established dump connection that has already carried a handshake,
    /// heartbeats and other schemas' events.
    cut_arm: AtomicU64,
    conns: AtomicU64,
    bytes: AtomicU64,
    bytes_s2c: AtomicU64,
    /// server→client bytes actually handed to the client. `bytes_s2c` counts what the server SENT
    /// (it is read at the moment of the upstream read, so a stall cannot stop it); this counts what
    /// the engine was ALLOWED to see, which is the only thing a stalled stream is a claim about.
    delivered_s2c: AtomicU64,
    flap_cuts: AtomicU64,
    byte_cuts: AtomicU64,
    /// How far past its arming point the last `cut_after_bytes` had relayed when it fired: the
    /// seed's proof of *where* in a group the reset landed, counted by the proxy.
    last_cut_at: AtomicU64,
    /// When each connection was accepted, in order: the spacing of the engine's reconnect
    /// attempts measured at the wire instead of guessed from the clock (`accept_marks`).
    accepts: Mutex<Vec<Instant>>,
    log: Option<FaultLog>,
}

/// An in-process TCP proxy (`127.0.0.1:<ephemeral>` → upstream) that injects wire faults
/// between lazywf and the servers / the mock. The mode is shared state the relay tasks re-read
/// between chunks, so `heal()` applies to established connections too.
pub struct FaultProxy {
    addr: SocketAddr,
    core: Arc<Core>,
}

impl FaultProxy {
    /// `upstream` is the real `host:port`; `label` names this role in the fault log.
    pub async fn start(upstream: &str, label: &str, log: Option<FaultLog>) -> FaultProxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let core = Arc::new(Core {
            label: label.to_string(),
            upstream: upstream.to_string(),
            mode: Mutex::new(Mode {
                kind: Kind::Live,
                cut_after_bytes: None,
                latency_ms: 0,
                flap_left: 0,
            }),
            gen: AtomicU64::new(0),
            cut_arm: AtomicU64::new(0),
            conns: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            bytes_s2c: AtomicU64::new(0),
            delivered_s2c: AtomicU64::new(0),
            flap_cuts: AtomicU64::new(0),
            byte_cuts: AtomicU64::new(0),
            last_cut_at: AtomicU64::new(0),
            accepts: Mutex::new(Vec::new()),
            log,
        });
        let c = core.clone();
        tokio::spawn(async move {
            let mut accept_errs = 0u64;
            loop {
                // An accept error is transient (EMFILE under an fd storm, a lost child): log it
                // and keep the listener — a dead accept loop refuses every later engine
                // connection, which reads as the proxy itself being down.
                let (cli, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        accept_errs += 1;
                        if accept_errs % 20 == 1 {
                            if let Some(l) = &c.log {
                                l.add(format!(
                                    "proxy {} accept error #{accept_errs}: {e}",
                                    c.label
                                ));
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                c.conns.fetch_add(1, Ordering::Relaxed);
                c.accepts.lock().unwrap().push(Instant::now());
                // The kind is *copied* out: a guard alive across the match would make the `Flap`
                // arm's own `lock()` self-deadlock the non-reentrant mutex (the trap the mock's
                // held-request comment warns about).
                let kind = c.mode.lock().unwrap().kind;
                match kind {
                    // "port dead": answer the connect with an RST (the client sees a dead port).
                    Kind::CutAll | Kind::Refuse => hard_close(cli),
                    // The flapping dump port: reset this connection and count it; when the last
                    // one of the storm has been reset the mode ends by itself, so the next
                    // connect is relayed ("cut every accepted connection 6 times, then let it
                    // through", design §2 S3 — counted by the proxy, never guessed by the test).
                    Kind::Flap => {
                        let (n, done) = {
                            let mut m = c.mode.lock().unwrap();
                            m.flap_left = m.flap_left.saturating_sub(1);
                            let done = m.flap_left == 0;
                            if done {
                                m.kind = Kind::Live;
                            }
                            (c.flap_cuts.fetch_add(1, Ordering::Relaxed) + 1, done)
                        };
                        hard_close(cli);
                        if let Some(l) = &c.log {
                            l.add(format!(
                                "proxy {} flap: dump connection #{n} reset{}",
                                c.label,
                                if done {
                                    " (storm over, live again)"
                                } else {
                                    ""
                                }
                            ));
                        }
                    }
                    Kind::Stall => {
                        // Accept and freeze: nothing is relayed (the MySQL handshake never
                        // completes); when the stall clears the connection starts relaying.
                        let c = c.clone();
                        tokio::spawn(async move {
                            while c.kind() == Kind::Stall {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                            let gen = c.gen.load(Ordering::Relaxed);
                            relay(cli, c, gen).await;
                        });
                    }
                    Kind::Live => {
                        let c = c.clone();
                        tokio::spawn(async move {
                            let gen = c.gen.load(Ordering::Relaxed);
                            relay(cli, c, gen).await;
                        });
                    }
                }
            }
        });
        FaultProxy { addr, core }
    }

    /// What goes into the DSN / endpoint in place of the real address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn conns(&self) -> u64 {
        self.core.conns.load(Ordering::Relaxed)
    }

    /// The instants this proxy accepted a connection, in order, starting one connection *before*
    /// `from` (so the first gap of a caller's `windows(2)` walk is the distance from the last
    /// pre-fault connection to the first faulted one). `from` is a `conns()` snapshot taken before
    /// the fault was injected: the returned spacing is the engine's own reconnect cadence, not the
    /// warm-up's.
    pub fn accept_marks(&self, from: u64) -> Vec<Instant> {
        let v = self.core.accepts.lock().unwrap();
        let at = (from as usize).min(v.len());
        v[at.saturating_sub(1)..].to_vec()
    }

    pub fn bytes(&self) -> u64 {
        self.core.bytes.load(Ordering::Relaxed)
    }

    /// What the server has sent through this proxy — the size of the binlog traffic, which is
    /// what S4 calibrates and verifies its cut point against.
    pub fn bytes_s2c(&self) -> u64 {
        self.core.bytes_s2c.load(Ordering::Relaxed)
    }

    /// What the proxy has actually let through to the client. During a stall this must not move:
    /// `bytes_s2c` cannot answer that question, because it counts at the moment of the upstream
    /// read and a stall cannot stop the server from sending.
    pub fn delivered_s2c(&self) -> u64 {
        self.core.delivered_s2c.load(Ordering::Relaxed)
    }

    fn set(&self, what: &str, f: impl FnOnce(&mut Mode)) {
        f(&mut self.core.mode.lock().unwrap());
        if let Some(l) = &self.core.log {
            l.add(format!("proxy {} {what}()", self.core.label));
        }
    }

    /// RST every relayed connection now, and every new connection while the cut holds.
    pub fn cut_all(&self) {
        self.core.gen.fetch_add(1, Ordering::Relaxed);
        self.set("cut_all", |m| m.kind = Kind::CutAll);
    }

    /// Server→client: RST the connection at the first sample past `n` bytes relayed **since this
    /// command was armed**, then clear the bound (one shot: the engine's re-read of the partial
    /// group must be able to come through — design §2 S4 re-reads from the boundary, it does not
    /// cut forever). Measured from arming, not from connection start, is what makes the bound
    /// mean "half of this group" on a dump connection that is already carrying a handshake,
    /// heartbeats and other schemas' events.
    pub fn cut_after_bytes(&self, n: u64) {
        self.core.cut_arm.fetch_add(1, Ordering::Relaxed);
        self.set(&format!("cut_after_bytes({n})"), |m| {
            m.cut_after_bytes = Some(n);
            m.kind = Kind::Live;
        });
    }

    /// The storm of design §2 S3: reset every connection this proxy accepts until `cuts` of them
    /// have been reset (the established one is reset at once too), then go live by itself. The
    /// count is the proxy's, so the test gates on observed cuts and never on the clock.
    pub fn flap(&self, cuts: u64) {
        self.core.gen.fetch_add(1, Ordering::Relaxed);
        self.set(&format!("flap({cuts})"), |m| {
            m.kind = Kind::Flap;
            m.flap_left = cuts;
            m.cut_after_bytes = None;
            m.latency_ms = 0;
        });
    }

    /// Accept but relay nothing (both directions), so even server heartbeats die.
    pub fn stall(&self, on: bool) {
        self.set(if on { "stall(true)" } else { "stall(false)" }, |m| {
            m.kind = if on { Kind::Stall } else { Kind::Live }
        });
    }

    /// Stop serving: every new connect is answered with an RST ("port dead").
    pub fn refuse(&self, on: bool) {
        self.set(if on { "refuse(true)" } else { "refuse(false)" }, |m| {
            m.kind = if on { Kind::Refuse } else { Kind::Live }
        });
    }

    pub fn latency(&self, ms: u64) {
        self.set(&format!("latency({ms})"), |m| {
            m.latency_ms = ms;
            m.kind = Kind::Live;
        });
    }

    pub fn heal(&self) {
        self.set("heal", |m| {
            m.kind = Kind::Live;
            m.cut_after_bytes = None;
            m.latency_ms = 0;
            m.flap_left = 0;
        });
    }

    pub fn stat(&self) -> ProxyStat {
        let m = *self.core.mode.lock().unwrap();
        ProxyStat {
            label: self.core.label.clone(),
            mode: match m.kind {
                Kind::Live if m.cut_after_bytes.is_some() => "cut_after_bytes",
                Kind::Live if m.latency_ms > 0 => "latency",
                Kind::Live => "live",
                Kind::CutAll => "cut_all",
                Kind::Stall => "stall",
                Kind::Refuse => "refuse",
                Kind::Flap => "flap",
            }
            .to_string(),
            conns: self.conns(),
            bytes: self.bytes(),
            bytes_s2c: self.core.bytes_s2c.load(Ordering::Relaxed),
            delivered_s2c: self.core.delivered_s2c.load(Ordering::Relaxed),
            flap_cuts: self.core.flap_cuts.load(Ordering::Relaxed),
            byte_cuts: self.core.byte_cuts.load(Ordering::Relaxed),
            bytes_at_cut: self.core.last_cut_at.load(Ordering::Relaxed),
        }
    }

    /// Event gating: poll the proxy's observability until `pred` holds (60 s budget).
    pub async fn wait(&self, pred: impl Fn(&ProxyStat) -> bool, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let st = self.stat();
            if pred(&st) {
                return;
            }
            if Instant::now() > deadline {
                panic!(
                    "proxy {}: timed out waiting for {what} (mode={}, conns={}, bytes={})",
                    st.label, st.mode, st.conns, st.bytes
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Core {
    fn kind(&self) -> Kind {
        self.mode.lock().unwrap().kind
    }
}

/// Drop with `SO_LINGER` 0: the kernel sends RST, so the peer sees a reset, not a clean FIN.
/// `with_timeout(0)` does not block the thread (the deprecation note's worry), and this is
/// harness code that never runs in the product.
#[allow(deprecated)]
fn hard_close(s: TcpStream) {
    let _ = s.set_linger(Some(Duration::ZERO));
    drop(s);
}

async fn relay(cli: TcpStream, core: Arc<Core>, my_gen: u64) {
    let (mut cli, mut up) = match tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(&core.upstream),
    )
    .await
    {
        Ok(Ok(up)) => (cli, up),
        _ => {
            hard_close(cli);
            return;
        }
    };
    let mut buf_c2s = [0u8; 16384];
    let mut buf_s2c = [0u8; 16384];
    // server→client bytes relayed on *this* connection, for the cut_after_bytes gate.
    let mut s2c: u64 = 0;
    // The cut bound is the distance from the moment it was armed: the task catches up to
    // `core.cut_arm` by taking its own count as the baseline (0 for a connection born after the
    // command, which is what "from connection start" used to mean).
    let mut my_cut_arm = core.cut_arm.load(Ordering::Relaxed);
    let mut cut_base: u64 = 0;
    // Bytes already taken off a socket when a stall was armed, held back until it clears. They are
    // here because of where the mode is read: once per iteration, *before* the `select!` — so a
    // chunk `read` had already returned (up to 16 KB, which is a whole transaction's group) would
    // still be delivered after `stall(true)`, and a seed that arms the stall immediately before a
    // write would watch that write arrive anyway. It is not hypothetical: on MariaDB the phase-2
    // group of S2 was read, applied and delivered 0.7 s after the stall was armed, which is the
    // proxy relaying one in-flight chunk (evidence/probe-s2-mariadb.log). Counters still count at
    // read time, so `bytes_s2c` keeps meaning "what the server sent"; only *delivery* is held, and
    // it is held in order and flushed before anything newer.
    let mut held_s2c: Vec<u8> = Vec::new();
    let mut held_c2s: Vec<u8> = Vec::new();
    loop {
        let mode = *core.mode.lock().unwrap();
        if mode.kind == Kind::CutAll
            || (mode.kind != Kind::CutAll && core.gen.load(Ordering::Relaxed) > my_gen)
        {
            hard_close(cli);
            hard_close(up);
            return;
        }
        if mode.kind == Kind::Stall {
            // Frozen both ways: read nothing, write nothing (the stream's heartbeats die).
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if !held_s2c.is_empty() || !held_c2s.is_empty() {
            if cli.write_all(&held_s2c).await.is_err() {
                return;
            }
            core.delivered_s2c
                .fetch_add(held_s2c.len() as u64, Ordering::Relaxed);
            held_s2c.clear();
            if up.write_all(&held_c2s).await.is_err() {
                return;
            }
            held_c2s.clear();
        }
        if core.cut_arm.load(Ordering::Relaxed) > my_cut_arm {
            my_cut_arm = core.cut_arm.load(Ordering::Relaxed);
            cut_base = s2c;
        }
        tokio::select! {
            r = cli.read(&mut buf_c2s) => match r {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    core.bytes.fetch_add(n as u64, Ordering::Relaxed);
                    // Re-read the mode *after* the read: a chunk that crossed while the stall was
                    // being armed is held, not delivered (see `held_s2c`).
                    if core.mode.lock().unwrap().kind == Kind::Stall {
                        held_c2s.extend_from_slice(&buf_c2s[..n]);
                        continue;
                    }
                    if mode.latency_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(mode.latency_ms)).await;
                    }
                    if up.write_all(&buf_c2s[..n]).await.is_err() {
                        return;
                    }
                }
            },
            r = up.read(&mut buf_s2c) => match r {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    core.bytes.fetch_add(n as u64, Ordering::Relaxed);
                    core.bytes_s2c.fetch_add(n as u64, Ordering::Relaxed);
                    s2c += n as u64;
                    if let Some(bound) = mode.cut_after_bytes {
                        if s2c - cut_base >= bound {
                            // One shot: the bound is cleared here, so the re-read of the partial
                            // group (which is the whole point of S4) comes through.
                            core.mode.lock().unwrap().cut_after_bytes = None;
                            let fired = core.byte_cuts.fetch_add(1, Ordering::Relaxed) + 1;
                            core.last_cut_at
                                .store(s2c - cut_base, Ordering::Relaxed);
                            if let Some(l) = &core.log {
                                l.add(format!(
                                    "proxy {} cut_after_bytes fired: connection #{fired} reset at \
                                     {} bytes past the arming point",
                                    core.label,
                                    s2c - cut_base
                                ));
                            }
                            hard_close(cli);
                            hard_close(up);
                            return;
                        }
                    }
                    // The same re-check as the other direction: the stall is a fact about what the
                    // engine may *see*, so nothing that arrived after it was armed is delivered.
                    if core.mode.lock().unwrap().kind == Kind::Stall {
                        held_s2c.extend_from_slice(&buf_s2c[..n]);
                        continue;
                    }
                    if mode.latency_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(mode.latency_ms)).await;
                    }
                    if cli.write_all(&buf_s2c[..n]).await.is_err() {
                        return;
                    }
                    core.delivered_s2c.fetch_add(n as u64, Ordering::Relaxed);
                }
            },
            // Poll the mode even on an idle connection, so a cut lands within ~50 ms.
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

// --------------------------------------------------------------------------- child launcher

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Lock descriptors inherited from the runner's shell (`exec 9>heavy-suite.lock; flock 9`) are the
/// one kind of descriptor that is not close-on-exec: Rust sets `CLOEXEC` on everything it opens
/// itself, so the proxies' and the mock's listeners never reach a child, but bash's do. A stray
/// engine child then held an open reference to the file the suite lock is taken on, and since an
/// advisory lock is held as long as *any* descriptor of that open file description stays open, a
/// leaked child blocked every later run on the heavy-suite lock (observed 2026-11-19: a run hung
/// 12 minutes with the lock's only remaining holder a `lazywf` from a test that had already failed).
///
/// They are marked close-on-exec rather than closed: the parent must keep its own reference (the
/// lock lives on the shared open file description), and closing descriptors by number is what made
/// the child abort with Rust's "owned file descriptor already closed" IO-safety check.
fn close_inherited_lock_fds() {
    // Declared here rather than pulled from `libc`: the test harness must not need a dependency.
    extern "C" {
        fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    }
    const F_SETFD: i32 = 2;
    const FD_CLOEXEC: i32 = 1;
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return; // nowhere to look: nothing is claimed, nothing is changed
    };
    for entry in entries.flatten() {
        let Ok(fd) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        if fd <= 2 {
            continue; // stdio belongs to the child's log
        }
        let Ok(target) = std::fs::read_link(entry.path()) else {
            continue; // closed under us
        };
        if !target.to_string_lossy().ends_with(".lock") {
            continue;
        }
        // SAFETY: `fcntl(F_SETFD)` on one of this process's own descriptors, with the two
        // constants the ABI defines for it; the return value is not needed (a descriptor that
        // refuses `F_SETFD` simply stays as it was, which is the pre-fix behavior).
        unsafe {
            fcntl(fd, F_SETFD, FD_CLOEXEC);
        }
    }
}

/// The engine as a real child process (design §3): one honest code path for every scenario,
/// with real signals and real env parsing. stdout and stderr go `Stdio::from(File)` straight
/// to `tmp/out/chaos/<test>.log` — no pipes exist, so no pipe-buffer deadlock and no drain
/// task is possible by construction.
pub struct Lazy {
    name: String,
    child: Arc<Mutex<Option<std::process::Child>>>,
    vars: Vec<(String, String)>,
    pub log_path: PathBuf,
    pub health: SocketAddr,
    log: Option<FaultLog>,
}

impl Lazy {
    /// Spawn the child. `extra` overrides the suite defaults (the proxy DSNs go here);
    /// `LAZYWF_INSTANCE` is unique per test name, `LAZYWF_SERVER_ID` is drawn from the same
    /// 4_000_000_000..=4_294_967_294 range `test_server_id()` uses (the shared servers leave
    /// that range to lazywf; the product default range overlaps other tenants'). Repeating
    /// `start` with one name (a restart after a kill) keeps the instance and takes the next
    /// log file `<name>.<n>.log`.
    pub fn start(name: &str, db: &Db, extra: &[(&str, &str)], log: Option<FaultLog>) -> Lazy {
        let mut vars: Vec<(String, String)> = vec![
            ("LAZYWF_DSN".into(), db.dsn.clone()),
            ("LAZYWF_STATUS_MS".into(), "200".into()),
            ("LAZYWF_CONFIG_POLL_MS".into(), "1000".into()),
            ("LAZYWF_MAX_BACKOFF_MS".into(), "800".into()),
            ("LAZYWF_HEARTBEAT_MS".into(), "1000".into()),
            ("LAZYWF_STOP_GRACE_MS".into(), "3000".into()),
            // The 0.3 defaults deny every target host; the suite allows what its mocks serve.
            ("LAZYWF_ALLOWED_HOSTS".into(), "*".into()),
            ("LAZYWF_LOG_FORMAT".into(), "json".into()),
            ("RUST_LOG".into(), "info".into()),
            ("LAZYWF_INSTANCE".into(), format!("chaos_{name}")),
        ];
        for (k, v) in extra {
            vars.retain(|(a, _)| a != k);
            vars.push((k.to_string(), v.to_string()));
        }
        let mut lazy = Lazy {
            name: name.to_string(),
            child: Arc::new(Mutex::new(None)),
            vars,
            log_path: PathBuf::new(),
            health: ([127, 0, 0, 1], 0).into(),
            log,
        };
        lazy.spawn();
        lazy
    }

    fn spawn(&mut self) {
        close_inherited_lock_fds();
        let port = free_port();
        self.vars
            .retain(|(k, _)| k != "LAZYWF_HEALTH_ADDR" && k != "LAZYWF_SERVER_ID");
        self.vars
            .push(("LAZYWF_HEALTH_ADDR".into(), format!("127.0.0.1:{port}")));
        self.vars
            .push(("LAZYWF_SERVER_ID".into(), test_server_id().to_string()));
        self.health = format!("127.0.0.1:{port}").parse().unwrap();
        self.log_path = next_log(&self.name);
        let file = std::fs::File::create(&self.log_path)
            .unwrap_or_else(|e| panic!("create {}: {e}", self.log_path.display()));
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_lazywf"));
        // The parent's environment must not leak configuration into the child.
        for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("LAZYWF_")) {
            cmd.env_remove(k);
        }
        for (k, v) in &self.vars {
            cmd.env(k, v);
        }
        cmd.stdout(std::process::Stdio::from(
            file.try_clone().expect("clone log file"),
        ))
        .stderr(std::process::Stdio::from(file));
        // No descriptor of ours may survive into the child (see `close_inherited_lock_fds`).
        close_inherited_lock_fds();
        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("spawn lazywf for {}: {e}", self.name));
        *self.child.lock().unwrap() = Some(child);
        if let Some(l) = &self.log {
            l.add(format!(
                "child {} started (log {})",
                self.name,
                self.log_path.display()
            ));
        }
    }

    /// GET /readyz == 200. The deadline is generous: 60 s at the first start, >= 30 s after a
    /// kill (lock release rides on MySQL noticing the dead GET_LOCK client). A child that lost
    /// its health port to a race re-probes once on a fresh port with a fresh child.
    pub async fn ready(&mut self, secs: u64) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut respawned = false;
        loop {
            if let Ok((200, _)) = http_get(self.health, "/readyz").await {
                return;
            }
            if self
                .child
                .lock()
                .unwrap()
                .as_mut()
                .is_some_and(|c| c.try_wait().ok().flatten().is_some())
            {
                panic!(
                    "the lazywf child {} exited before ready:\n{}",
                    self.name,
                    self.tail(40).join("\n")
                );
            }
            if !respawned && !self.log_lines_matching("cannot listen").is_empty() {
                respawned = true;
                self.hard_stop();
                self.spawn();
                continue;
            }
            if Instant::now() > deadline {
                panic!(
                    "the lazywf child {} was not ready in {secs} s:\n{}",
                    self.name,
                    self.tail(40).join("\n")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// GET /status as JSON (transient connection errors retry for ~2 s).
    pub async fn status(&self) -> Value {
        for i in 0..40 {
            match http_get(self.health, "/status").await {
                Ok((200, body)) => {
                    if let Ok(v) = serde_json::from_str(&body) {
                        if i > 0 {
                            eprintln!("lazy> status ok after {i} retries");
                        }
                        return v;
                    }
                    eprintln!(
                        "lazy> status attempt {i}: 200 but unparseable: {}",
                        short(&body)
                    );
                }
                Ok((code, body)) => eprintln!(
                    "lazy> status attempt {i}: code={code} body={}",
                    short(&body)
                ),
                Err(e) => eprintln!("lazy> status attempt {i}: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no /status answer from the lazywf child {}", self.name);
    }

    /// SIGKILL through `Child::kill`; the fault log records the window opener. Only ever this
    /// harness's own child, whose pid the log line names so a reviewer can see which process the
    /// seed was allowed to stop.
    pub fn kill(&self) {
        let pid = self.pid();
        self.hard_stop();
        if let Some(l) = &self.log {
            l.add(format!("child {pid} ({}) killed (SIGKILL)", self.name));
        }
    }

    /// The pid of the child this harness spawned (`0` once it is gone).
    pub fn pid(&self) -> u32 {
        self.child.lock().unwrap().as_ref().map_or(0, |c| c.id())
    }

    /// The child's peak resident set (`VmHWM` of `/proc/<pid>/status`, in KiB): the memory a
    /// chaos load is allowed to take. `-1` when the process is already gone (the peak of a
    /// killed child cannot be read afterwards, so a seed takes this before it kills).
    pub fn peak_rss_kib(&self) -> i64 {
        let pid = self.pid();
        if pid == 0 {
            return -1;
        }
        let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
            return -1;
        };
        text.lines()
            .find_map(|l| l.strip_prefix("VmHWM:"))
            .and_then(|v| v.trim().trim_end_matches(" kB").trim().parse().ok())
            .unwrap_or(-1)
    }

    fn hard_stop(&self) {
        let mut c = self.child.lock().unwrap();
        if let Some(child) = c.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// SIGTERM through `kill -TERM <pid>`, then wait; asserts exit 0 within the stop grace.
    pub async fn term(&self) -> i32 {
        let pid = self
            .child
            .lock()
            .unwrap()
            .as_ref()
            .expect("child running")
            .id()
            .to_string();
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("kill -TERM");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let st = self
                .child
                .lock()
                .unwrap()
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten());
            if let Some(st) = st {
                let code = st.code().unwrap_or(-1);
                assert_eq!(
                    code, 0,
                    "the lazywf child {} exited {code} on SIGTERM",
                    self.name
                );
                if let Some(l) = &self.log {
                    l.add(format!("child {} terminated (SIGTERM, exit 0)", self.name));
                }
                return code;
            }
            assert!(
                Instant::now() < deadline,
                "the child {} did not exit after SIGTERM",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn log_lines_matching(&self, pat: &str) -> Vec<String> {
        let Ok(text) = std::fs::read_to_string(&self.log_path) else {
            return Vec::new();
        };
        text.lines()
            .filter(|l| l.contains(pat))
            .map(String::from)
            .collect()
    }

    /// Everything the child has written so far. A seed that asks several questions of the log
    /// reads it once with this, instead of once per question.
    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    fn tail(&self, n: usize) -> Vec<String> {
        let all = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        all.lines()
            .rev()
            .take(n)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(String::from)
            .collect()
    }
}

/// A seed that panics — which is exactly what a failing chaos test does — must not leave the
/// engine running. A stray child holds the schema's `GET_LOCK`, keeps a binlog dump connection
/// open on a server every tenant shares, keeps appending to a log file another run will reuse, and
/// (before the descriptors were closed at exec, see `spawn`) kept the runner's flock alive. So the
/// child goes with its handle, reaped through the same path `kill` uses; a child the seed already
/// stopped or killed reports an exit status here and is left alone — no signal is ever sent to a
/// process that is not running.
impl Drop for Lazy {
    fn drop(&mut self) {
        let running = self
            .child
            .lock()
            .unwrap()
            .as_mut()
            .map_or(false, |c| c.try_wait().ok().flatten().is_none());
        if running {
            self.hard_stop();
            if let Some(l) = &self.log {
                l.add(format!(
                    "child {} killed on drop: the seed left it running",
                    self.name
                ));
            }
        }
    }
}

fn next_log(name: &str) -> PathBuf {
    let dir = Path::new("tmp/out/chaos");
    std::fs::create_dir_all(dir).expect("create tmp/out/chaos");
    let base = dir.join(format!("{name}.log"));
    if !base.exists() {
        return base;
    }
    (2..)
        .map(|n| dir.join(format!("{name}.{n}.log")))
        .find(|p| !p.exists())
        .expect("log file slot")
}

async fn http_get(addr: SocketAddr, path: &str) -> std::io::Result<(u16, String)> {
    let mut s = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect"))??;
    // Every leg is bounded: an unbounded write against a peer that stopped reading (a frozen
    // child) would park the test's own future forever, and a hung suite explains nothing.
    tokio::time::timeout(
        Duration::from_secs(3),
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        ),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "write"))??;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read"))??;
    let text = String::from_utf8_lossy(&buf).to_string();
    let code = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map_or("", |(_, b)| b)
        .to_string();
    Ok((code, body))
}

// --------------------------------------------------------------------------- sampler

struct Phase {
    rule: String,
    name: String,
    checkpoint: (String, u64),
    expected: BTreeSet<String>,
    mock: Mock,
    open: bool,
}

/// A declared floor for one rule's position while a set of keys is still unconfirmed. P6 says a
/// position never goes backwards — but an XA's marks arrive at its `XA COMMIT` and are pinned at
/// the transaction's *own start* (`apply(marks, &pos, &xa.start)` in `src/engine.rs`), which moves
/// the saved position back on purpose; without this the sampler would call the designed behavior a
/// violation. Built only by [`Sampler::allow_xa_pin`], so the floor is a position the server
/// confirms a branch begins at, never one a seed picks. The exemption is bounded on both sides so
/// it cannot hide a real regression: it applies only while `keys` are undelivered, and a position
/// *below* `floor` is still a violation (`h1_sampler_pin_floor_still_bites` proves that still
/// fires).
struct Pin {
    rule: String,
    name: String,
    floor: (String, u64),
    keys: BTreeSet<String>,
    mock: Mock,
}

/// The floor an XA puts on a rule's position — and the only thing that can produce one. The floor
/// is not an argument a seed chooses: building an `XaPin` asks the *server* where the branch's
/// group begins (`SHOW BINLOG EVENTS`, walked by what closes a group — the same read e2e's
/// `xa_group_start` does) and it refuses to exist for a transaction the binlog does not contain.
/// That is what scopes the sampler's P6 exemption to the XA case by construction: no XA the server
/// confirms, no pin, no exemption — see `Sampler::allow_xa_pin`.
pub struct XaPin {
    xid: String,
    pos: (String, u64),
}

impl XaPin {
    /// Reads branch `xid`'s group start from the binlog, starting at `from` — a snapshot taken
    /// before `XA START`, which is only a lower bound because another session can commit in
    /// between (this suite runs two threads against one server, and the engine's own status writes
    /// land there too).
    pub async fn read(db: &Db, from: &(String, u64), xid: &str) -> XaPin {
        let hex: String = xid.bytes().map(|b| format!("{b:02x}")).collect();
        let needle_hex = format!("X'{hex}'").to_uppercase();
        let needle_prepare = format!("XA PREPARE '{xid}'").to_uppercase();
        let needle_start = format!("XA START '{xid}'").to_uppercase();
        let mut c = db.conn().await;
        let rows: Vec<mysql_async::Row> = c
            .query(format!(
                "SHOW BINLOG EVENTS IN '{}' FROM {} LIMIT 10000",
                from.0.replace('\'', "''"),
                from.1
            ))
            .await
            .unwrap_or_else(|e| panic!("SHOW BINLOG EVENTS from {from:?} for '{xid}': {e}"));
        super::quit(c).await;
        // Walk by what *closes* a group (the same on both servers) rather than by what opens one
        // (which is not): what the engine pins is the boundary it was holding when the branch's
        // events arrived.
        let mut group = from.1;
        for row in rows {
            let kind: String = row.get(2).expect("Event_type");
            let info: String = row.get(5).unwrap_or_default();
            let up = info.to_uppercase();
            let ours = up.contains(&needle_hex)
                || up.contains(&needle_prepare)
                || up.contains(&needle_start);
            if ours {
                return XaPin {
                    xid: xid.to_string(),
                    pos: (from.0.clone(), group),
                };
            }
            if kind == "Xid"
                || kind.eq_ignore_ascii_case("XA_prepare")
                || (kind == "Query" && !up.starts_with("BEGIN"))
            {
                group = row.get(4).expect("End_log_pos");
            }
        }
        panic!(
            "no XA PREPARE/START of '{xid}' in {} after {from:?}: a floor needs a transaction the \
             server actually has, so a pin cannot be claimed for one that is not in the binlog",
            from.0
        );
    }

    /// The position the rule's marks are pinned at: the boundary just before the branch's group.
    pub fn pos(&self) -> &(String, u64) {
        &self.pos
    }

    pub fn xid(&self) -> &str {
        &self.xid
    }
}

struct Shared {
    dsn: String,
    fault_log: FaultLog,
    phases: Mutex<Vec<Phase>>,
    pins: Mutex<Vec<Pin>>,
    /// The P6 regressions the sampler let through because a declared pin explained them, so a run
    /// reports them instead of silently passing (and a seed can name them in its own summary).
    backslides: Mutex<Vec<String>>,
    logs: Mutex<Vec<PathBuf>>,
    proxies: Mutex<Vec<FaultProxy>>,
    last_pos: Mutex<BTreeMap<String, (String, u64)>>,
    violation: Mutex<Option<String>>,
    /// Completed passes of [`sample`], so a seed that wants to change state the sampler reads (h1
    /// forges a status row) can wait for the sampler to have *seen* the previous state instead of
    /// sleeping and hoping: a forge before the first sample is a forge nobody observed.
    samples: AtomicU64,
}

/// The property kernel (design §3): a 100 ms sampler over (a) the status table
/// (`rule, binlog_file, binlog_pos, reset_state, last_error`), (b) the mocks' delivered keys,
/// (c) the child logs — checking P2 (the position stays below every open checkpoint, and never
/// regresses), P4 (no second delivery of a write-once key without a fault-log event in
/// between) and P5 (zero `lazywf::undelivered` events). A violation is recorded once, with the
/// whole context (phase table, fault-log tail, status rows, child-log tails, proxy state), and
/// `check()` surfaces it in the test task — a panic inside the sampler task alone would not
/// fail the test.
pub struct Sampler {
    shared: Arc<Shared>,
    stop: Arc<AtomicBool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Sampler {
    pub fn start(db: &Db, fault_log: FaultLog) -> Sampler {
        let shared = Arc::new(Shared {
            dsn: db.dsn.clone(),
            fault_log,
            phases: Mutex::new(Vec::new()),
            pins: Mutex::new(Vec::new()),
            backslides: Mutex::new(Vec::new()),
            logs: Mutex::new(Vec::new()),
            proxies: Mutex::new(Vec::new()),
            last_pos: Mutex::new(BTreeMap::new()),
            violation: Mutex::new(None),
            samples: AtomicU64::new(0),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let sp = shared.clone();
        let st = stop.clone();
        let task = tokio::spawn(async move {
            // One admin connection for the sampler's life: opening one per 100 ms sample
            // (and cancelling a slow handshake) ate the process fd table until accept()
            // returned EMFILE, which reads as the proxy refusing every engine connection.
            let mut conn: Option<mysql_async::Conn> = None;
            while !st.load(Ordering::Relaxed) {
                sample(&sp, &mut conn).await;
                sp.samples.fetch_add(1, Ordering::Relaxed);
                if sp.violation.lock().unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if let Some(c) = conn {
                super::quit(c).await;
            }
        });
        Sampler {
            shared,
            stop,
            task: Mutex::new(Some(task)),
        }
    }

    /// Register a write phase: while its keys have not all been delivered to `mock`, `rule`'s
    /// status position must stay below `checkpoint` (P2), and its keys may not be delivered
    /// twice without a fault event in between (P4; write-once loads — a per-(key, version)
    /// churn phase puts the version in `expected`, keeping the assertion hard).
    pub fn add_phase(
        &self,
        rule: &str,
        name: &str,
        checkpoint: (String, u64),
        expected: BTreeSet<String>,
        mock: &Mock,
    ) {
        self.shared.phases.lock().unwrap().push(Phase {
            rule: rule.to_string(),
            name: name.to_string(),
            checkpoint,
            expected,
            mock: mock.clone(),
            open: true,
        });
    }

    /// Declare the legitimate floor an XA puts on `rule`'s position while `keys` are still
    /// undelivered: the position may move back *to* the branch's own start, and not below it,
    /// because the marks held since `XA PREPARE` are applied when the `COMMIT` is read and pinned at
    /// the transaction's start (`apply(marks, &pos, &xa.start)` in src/engine.rs).
    ///
    /// Two things keep this a narrowing of P6 rather than a mute. The floor cannot be chosen — it
    /// comes from an [`XaPin`], whose only constructor reads the branch out of the server's binlog,
    /// so there is no way to exempt a position without an XA behind it; and the exemption is spent
    /// as soon as `keys` reach the target. Below the floor P6 still fires, which
    /// `h1_sampler_pin_floor_still_bites` tests, and every regression this accepts is written to the
    /// fault log, listed by [`Sampler::backslides`] and printed by the seed that declared it.
    pub fn allow_xa_pin(
        &self,
        rule: &str,
        label: &str,
        pin: &XaPin,
        keys: BTreeSet<String>,
        mock: &Mock,
    ) {
        let name = format!(
            "{label}: the marks of '{}' at {}:{}",
            pin.xid(),
            pin.pos().0,
            pin.pos().1
        );
        self.shared.fault_log.add(format!(
            "sampler: rule {rule} may fall back to {:?} while {name} is unconfirmed",
            pin.pos()
        ));
        self.shared.pins.lock().unwrap().push(Pin {
            rule: rule.to_string(),
            name,
            floor: pin.pos().clone(),
            keys,
            mock: mock.clone(),
        });
    }

    /// The regressions the sampler accepted because a declared pin explained them (for the seed's
    /// own report: an accepted regression is something that happened, not a non-event).
    pub fn backslides(&self) -> Vec<String> {
        self.shared.backslides.lock().unwrap().clone()
    }

    /// The violation the sampler recorded, if it recorded one. `check` panics, which is right for a
    /// seed; a seed that is *testing the sampler* (h1) needs to read the verdict and say what it
    /// was waiting for, so this is the non-panicking view of the same field.
    pub fn violation(&self) -> Option<String> {
        self.shared.violation.lock().unwrap().clone()
    }

    /// How many passes the sampler has completed. A seed that forges state for the sampler to
    /// react to waits on this first: the alternative is a sleep, which the harness does not do.
    pub fn samples(&self) -> u64 {
        self.shared.samples.load(Ordering::Relaxed)
    }

    pub fn add_log(&self, path: &Path) {
        self.shared.logs.lock().unwrap().push(path.to_path_buf());
    }

    pub fn add_proxy(&self, p: &FaultProxy) {
        self.shared.proxies.lock().unwrap().push(FaultProxy {
            addr: p.addr,
            core: p.core.clone(),
        });
    }

    /// Panic with the recorded violation and its context, if any. Called at every checkpoint
    /// and inside `drain`.
    pub fn check(&self) {
        let v = self.shared.violation.lock().unwrap().clone();
        if let Some(v) = v {
            panic!("{v}");
        }
        // A sampler that died checks nothing at all, and its silence would read as a pass —
        // the one failure mode a chaos harness must never have. A panic inside a sampled check
        // (it happened with `Row::get` on a NULL position) surfaces here instead.
        if let Some(t) = self.task.lock().unwrap().as_ref() {
            if t.is_finished() {
                panic!(
                    "the chaos sampler task is dead (it panicked): no invariant was being \
                     checked; see the sampler's own output above"
                );
            }
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.task.lock().unwrap().take() {
            t.abort();
        }
    }

    /// The open phases as (rule, name, checkpoint, keys still missing at the mock).
    pub fn open_phases(&self) -> Vec<(String, String, (String, u64), usize)> {
        self.shared
            .phases
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.open)
            .map(|p| {
                let got = p.mock.keys_received();
                let missing = p.expected.iter().filter(|k| !got.contains(*k)).count();
                (
                    p.rule.clone(),
                    p.name.clone(),
                    p.checkpoint.clone(),
                    missing,
                )
            })
            .collect()
    }

    pub async fn context(&self) -> String {
        context(&self.shared).await
    }
}

/// Read the status table through `conn`, reconnecting once when it fails. A timeout drops
/// the connection rather than leaving a half-read socket behind.
async fn status_rows(
    dsn: &str,
    conn: &mut Option<mysql_async::Conn>,
) -> Vec<(String, String, u64)> {
    for attempt in 0..2 {
        if conn.is_none() {
            let opened = tokio::time::timeout(
                Duration::from_secs(5),
                mysql_async::Conn::new(mysql_async::Opts::from_url(dsn).expect("parse dsn")),
            )
            .await;
            match opened {
                Ok(Ok(c)) => *conn = Some(c),
                Ok(Err(e)) => {
                    eprintln!(
                        "sampler> connect failed (attempt {attempt}): {}",
                        short(&e.to_string())
                    );
                    return Vec::new();
                }
                Err(_) => {
                    eprintln!("sampler> connect TIMEOUT (5 s, attempt {attempt})");
                    return Vec::new();
                }
            }
        }
        let c = conn.as_mut().expect("connected");
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            c.query::<mysql_async::Row, _>(
                "SELECT rule, binlog_file, binlog_pos FROM lazy_wire_fire_status",
            ),
        )
        .await;
        match r {
            Ok(Ok(rows)) => {
                return rows
                    .into_iter()
                    .filter_map(|row| {
                        // `get_opt`, never `get`: a rule that has not streamed yet has a NULL
                        // position, and `Row::get` *panics* on that — which used to kill the
                        // sampler task in silence, switching every invariant off.
                        Some((
                            row.get_opt::<String, _>(0)?.ok()?,
                            row.get_opt::<String, _>(1)?.ok()?,
                            row.get_opt::<u64, _>(2)?.ok()?,
                        ))
                    })
                    .collect();
            }
            Ok(Err(e)) => {
                eprintln!(
                    "sampler> query failed (attempt {attempt}): {}",
                    short(&e.to_string())
                );
                *conn = None;
            }
            Err(_) => {
                eprintln!("sampler> query TIMEOUT (5 s, attempt {attempt})");
                *conn = None;
            }
        }
    }
    Vec::new()
}

/// One open or closed phase as the sampler reads it: index, rule, name, checkpoint, the keys
/// it must deliver, and the mock that sees them.
type SampledPhase = (usize, String, String, (String, u64), BTreeSet<String>, Mock);

async fn sample(sp: &Arc<Shared>, conn: &mut Option<mysql_async::Conn>) {
    // Keys are read *before* the status rows: a mock answers 2xx before the engine confirms
    // and persists, so "pos past an open checkpoint" can never be a read-order race — if the
    // row is already past the checkpoint, the delivery is certainly visible to us already.
    let snapshot: Vec<SampledPhase> = {
        let ph = sp.phases.lock().unwrap();
        ph.iter()
            .enumerate()
            .map(|(i, p)| {
                (
                    i,
                    p.rule.clone(),
                    p.name.clone(),
                    p.checkpoint.clone(),
                    p.expected.clone(),
                    p.mock.clone(),
                )
            })
            .collect()
    };
    let delivered: BTreeMap<String, BTreeSet<String>> =
        snapshot
            .iter()
            .fold(BTreeMap::new(), |mut m, (_, rule, _, _, _, mock)| {
                m.entry(rule.clone())
                    .or_insert_with(BTreeSet::new)
                    .extend(mock.keys_received());
                m
            });

    // P2: while a phase's keys are not all delivered, its rule's position stays below the
    // checkpoint. Closed phases (all keys delivered) are exempt from then on.
    let mut still_open: Vec<usize> = Vec::new();
    for (i, rule, _name, _cp, expected, _) in &snapshot {
        if !sp.phases.lock().unwrap()[*i].open {
            continue;
        }
        let got = delivered.get(rule).cloned().unwrap_or_default();
        if expected.iter().all(|k| got.contains(k)) {
            sp.phases.lock().unwrap()[*i].open = false;
            continue;
        }
        still_open.push(*i);
    }
    let rows = status_rows(&sp.dsn, conn).await;
    for (_i, rule, name, cp, expected, _) in
        snapshot.iter().filter(|(i, ..)| still_open.contains(i))
    {
        let got = delivered.get(rule).cloned().unwrap_or_default();
        if let Some((_, f, p)) = rows.iter().find(|(r, ..)| r == rule) {
            if (f.clone(), *p) >= *cp {
                let missing_n = expected
                    .iter()
                    .filter(|k| !got.contains(k.as_str()))
                    .count();
                let missing: Vec<_> = expected
                    .iter()
                    .filter(|k| !got.contains(k.as_str()))
                    .take(8)
                    .collect();
                return violate(
                    sp,
                    format!(
                        "P2: rule {rule} reached {f}:{p} while {name} is open \
                         (checkpoint {}:{}), {missing_n} keys still missing at the mock (e.g. {missing:?})",
                        cp.0, cp.1
                    ),
                )
                .await;
            }
        }
    }

    // P6: positions monotone per rule (the cross-restart check rides on the same rows). The
    // guard lives in its own block: no lock is held across an await in this future.
    let regressed = {
        let mut last = sp.last_pos.lock().unwrap();
        let mut bad = None;
        for (rule, f, p) in &rows {
            let cur = (f.clone(), *p);
            if let Some(prev) = last.get(rule) {
                if cur < *prev {
                    bad = Some((rule.clone(), prev.clone(), cur.clone()));
                    break;
                }
            }
            last.insert(rule.clone(), cur);
        }
        bad
    };
    if let Some((rule, prev, cur)) = regressed {
        // Three claims, not two. A regression that lands ON OR ABOVE a declared pin's floor is the
        // designed XA pin: an XA's marks arrive at its COMMIT and are pinned at the transaction's
        // own start (`apply(marks, &pos, &xa.start)` in src/engine.rs), which is the behavior S5
        // exists to prove. A regression that lands BELOW the floor is a violation the pin does not
        // excuse — the floor has to be able to fail, or it is not a floor. And a regression no
        // open pin covers at all is the plain lost-work case.
        let verdict =
            {
                let pins = sp.pins.lock().unwrap();
                // Only a pin whose keys are still unconfirmed excuses anything: once the keys are at
                // the target the marks have been applied and the exemption is over.
                let declared = pins.iter().find(|pin| {
                    pin.rule == rule
                        && !pin
                            .keys
                            .iter()
                            .all(|k| pin.mock.keys_received().contains(k))
                });
                match declared {
                    Some(pin) if cur >= pin.floor => Ok(pin.name.clone()),
                    Some(pin) => {
                        Err(format!(
                    "P6: rule {rule} position went back {}:{} -> {}:{}, BELOW the floor {}:{} \
                     declared for {name} — a pin excuses a position falling TO its floor, not \
                     past it",
                    prev.0, prev.1, cur.0, cur.1, pin.floor.0, pin.floor.1, name = pin.name
                ))
                    }
                    None => Err(format!(
                    "P6: rule {rule} position regressed {}:{} -> {}:{} (no declared pin explains \
                     it: a position that goes back without an XA pin behind it lost work)",
                    prev.0, prev.1, cur.0, cur.1
                )),
                }
            };
        match verdict {
            Ok(name) => {
                // Rebaseline: the pin IS the position now, so the next sample compares against it.
                // Without this the same accepted regression is reported on every sample and turns
                // into a violation the moment the pin's keys are delivered.
                sp.last_pos
                    .lock()
                    .unwrap()
                    .insert(rule.clone(), cur.clone());
                let line = format!(
                    "P6 accepted: rule {rule} position went back {}:{} -> {}:{} (the floor of \
                     {name}, the pin's keys were still unconfirmed)",
                    prev.0, prev.1, cur.0, cur.1
                );
                sp.backslides.lock().unwrap().push(line.clone());
                sp.fault_log.add(line);
            }
            Err(msg) => return violate(sp, msg).await,
        }
    }

    // P4 (every phase, open or closed): no second delivery of one expected key without a
    // fault event in between. Duplicates inside fault windows are counted and reported, never
    // failed — the counting lives in the test's end report, the hard edge lives here.
    for (_, rule, _, _, expected, mock) in &snapshot {
        let mut times: BTreeMap<String, Vec<Instant>> = BTreeMap::new();
        for (at, keys) in mock.deliveries() {
            for k in keys.iter().filter(|k| expected.contains(*k)) {
                times.entry(k.clone()).or_default().push(at);
            }
        }
        for (key, mut ts) in times {
            ts.sort();
            for w in ts.windows(2) {
                if !sp.fault_log.has_between(w[0], w[1]) {
                    return violate(
                        sp,
                        format!(
                            "P4: rule {rule} key {key} was delivered {} times with no fault event \
                             between two of them ({a:?} and {b:?})",
                            ts.len(),
                            a = w[0],
                            b = w[1]
                        ),
                    )
                    .await;
                }
            }
        }
    }

    // P5: no strike for a transient: zero `lazywf::undelivered` lines in any child log.
    let log_paths: Vec<PathBuf> = sp.logs.lock().unwrap().clone();
    for path in log_paths.iter() {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Some(line) = text.lines().find(|l| l.contains("lazywf::undelivered")) {
                return violate(
                    sp,
                    format!(
                        "P5: {} carries an undelivered event: {}",
                        path.display(),
                        short(line)
                    ),
                )
                .await;
            }
        }
    }
}

fn short(s: &str) -> String {
    let mut t: String = s.chars().take(400).collect();
    t.push('…');
    t
}

async fn violate(sp: &Arc<Shared>, what: String) {
    let ctx = context(sp).await;
    let mut v = sp.violation.lock().unwrap();
    if v.is_none() {
        *v = Some(format!("CHAOS SAMPLER VIOLATION: {what}\n{ctx}"));
    }
}

async fn context(sp: &Arc<Shared>) -> String {
    let mut out = String::new();
    out.push_str("--- phases:\n");
    for p in sp.phases.lock().unwrap().iter() {
        let got = p.mock.keys_received();
        let missing = p.expected.iter().filter(|k| !got.contains(*k)).count();
        out.push_str(&format!(
            "  rule {} {} checkpoint {}:{} {} ({} of {} keys missing)\n",
            p.rule,
            p.name,
            p.checkpoint.0,
            p.checkpoint.1,
            if p.open { "OPEN" } else { "closed" },
            missing,
            p.expected.len()
        ));
    }
    out.push_str("--- fault log (last 25):\n");
    for l in sp.fault_log.tail(25) {
        out.push_str(&format!("  {l}\n"));
    }
    out.push_str("--- status rows (rule file:pos reset last_error):\n");
    // Bounded like every other harness await: context() runs on the sampler task, and a task
    // parked on a server that never answers takes the sampler (and the suite's failure message)
    // with it.
    let mut c = match tokio::time::timeout(
        Duration::from_secs(5),
        mysql_async::Conn::new(mysql_async::Opts::from_url(&sp.dsn).expect("dsn")),
    )
    .await
    {
        Ok(Ok(c)) => Some(c),
        _ => None,
    };
    if let Some(c) = c.as_mut() {
        let rows: Result<Vec<mysql_async::Row>, mysql_async::Error> = match tokio::time::timeout(
            Duration::from_secs(5),
            c.query(
                "SELECT rule, binlog_file, binlog_pos, reset_state, last_error \
                 FROM lazy_wire_fire_status",
            ),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(mysql_async::Error::Other(
                "context: the status query timed out".into(),
            )),
        };
        if let Ok(rows) = rows {
            for row in rows {
                let rule: String = row.get_opt(0).and_then(|r| r.ok()).unwrap_or_default();
                let f: String = row.get_opt(1).and_then(|r| r.ok()).unwrap_or_default();
                let p: u64 = row.get_opt(2).and_then(|r| r.ok()).unwrap_or(0);
                let rs: String = row.get_opt(3).and_then(|r| r.ok()).unwrap_or_default();
                let le: Option<String> = row.get_opt(4).and_then(|r| r.ok());
                out.push_str(&format!(
                    "  {rule} {f}:{p} {rs} {}\n",
                    le.map(|e| short(&e)).unwrap_or_default()
                ));
            }
        }
    }
    out.push_str("--- proxies:\n");
    for p in sp.proxies.lock().unwrap().iter() {
        let s = p.stat();
        out.push_str(&format!(
            "  {} mode={} conns={} bytes={}\n",
            s.label, s.mode, s.conns, s.bytes
        ));
    }
    out.push_str("--- child log tails:\n");
    for path in sp.logs.lock().unwrap().iter() {
        out.push_str(&format!("  {}:\n", path.display()));
        if let Ok(text) = std::fs::read_to_string(path) {
            let lines: Vec<&str> = text.lines().rev().take(15).collect();
            for l in lines.into_iter().rev() {
                out.push_str(&format!("    {}\n", short(l)));
            }
        }
    }
    out
}

// --------------------------------------------------------------------------- XA hygiene
//
// Design §4: a prepared XA abandoned by a panicking chaos test holds locks and blocks the binlog
// purge for every tenant of a shared server, so S5 defends against it three ways: an xid unique
// per run, this module's `XaGuard` (commits or rolls back on every path, Drop included), and a
// suite-start `XA RECOVER` sweep of leftover `lwf_*` xids. The sweep is aware of the xids this
// process has under its own guard, so it can never roll back a transaction another running seed
// is holding open on purpose.

fn live_xids() -> &'static Mutex<BTreeSet<String>> {
    static LIVE: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(BTreeSet::new()))
}

/// A connection to the server root (no schema): `XA RECOVER`/`XA ROLLBACK` belong to no schema.
async fn root_conn(root: &str) -> mysql_async::Conn {
    let opts = mysql_async::Opts::from_url(&format!("{root}/?prefer_socket=false"))
        .unwrap_or_else(|e| panic!("parse the root dsn of {root} for the XA sweep: {e}"));
    match tokio::time::timeout(Duration::from_secs(20), mysql_async::Conn::new(opts)).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => panic!("connect to {root} for the XA sweep: {e}"),
        Err(_) => panic!("{root}: no answer to a new connection in 20 s (XA sweep)"),
    }
}

/// One prepared branch `XA RECOVER` shows: `name` is what the sweep reports (the `data` column,
/// the printable gtrid+bqual), while a rollback has to name the two parts separately — the joined
/// string of the data column is not an xid `XA ROLLBACK` takes back.
#[derive(Debug, Clone)]
pub struct XaLeft {
    pub name: String,
    pub gtrid: String,
    pub bqual: String,
}

fn sql_quote(s: &str) -> String {
    s.replace('\'', "''")
}

fn text(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

/// The prepared-but-unfinished `lwf_*` transactions of one server. No answer, or no right to ask,
/// is reported as none: nothing is claimed about transactions that cannot be seen.
pub async fn xa_recover_lwf(root: &str) -> Vec<XaLeft> {
    let mut c = root_conn(root).await;
    let rows: Vec<mysql_async::Row> = match tokio::time::timeout(
        Duration::from_secs(20),
        // CONVERT XID: the binary gtrid/bqual come back in printable form, so the name the sweep
        // reports and the strings it rolls back are the ones the server showed.
        c.query::<mysql_async::Row, _>("XA RECOVER CONVERT XID"),
    )
    .await
    {
        Ok(Ok(r)) => r,
        _ => Vec::new(),
    };
    let out = rows
        .into_iter()
        .filter_map(|row| {
            let gtrid = text(&row.get_opt::<Vec<u8>, _>(1)?.ok()?);
            let bqual = text(&row.get_opt::<Vec<u8>, _>(2)?.ok()?);
            let name = row
                .get_opt::<Vec<u8>, _>(3)
                .and_then(|v| v.ok())
                .map(|v| text(&v))
                .unwrap_or_else(|| format!("{gtrid},{bqual}"));
            (name.starts_with("lwf_") || gtrid.starts_with("lwf_")).then_some(XaLeft {
                name,
                gtrid,
                bqual,
            })
        })
        .collect();
    super::quit(c).await;
    out
}

/// Roll one branch back; the error of a branch that is not (or no longer) there is the expected
/// answer, so it is not a failure. The two-argument form goes first, because the data column's
/// joined string is not an xid a rollback takes; the single-string form is the fallback.
async fn xa_rollback(root: &str, xa: &XaLeft) {
    let mut c = root_conn(root).await;
    for q in [
        format!(
            "XA ROLLBACK '{}','{}'",
            sql_quote(&xa.gtrid),
            sql_quote(&xa.bqual)
        ),
        format!("XA ROLLBACK '{}'", sql_quote(&xa.name)),
    ] {
        let r = tokio::time::timeout(Duration::from_secs(20), c.query_drop(q)).await;
        if matches!(r, Ok(Ok(()))) {
            break;
        }
    }
    super::quit(c).await;
}

/// The suite-start sweep, once per test-binary run (the runner holds the heavy-suite lock
/// around the whole binary, so no other lazywf suite writes here while it runs). Every `lwf_*`
/// xid that no guard of this process owns is rolled back, and the list it leaves behind is what
/// the seed asserts on: a leftover the sweep should have removed fails the seed, which is what
/// keeps the sweep from being a `let _ =` line nobody checks.
pub async fn sweep_xa_once(root: &str, log: &FaultLog) -> Vec<String> {
    if SWEPT.get().is_none() {
        let mine = live_xids().lock().unwrap().clone();
        let found = xa_recover_lwf(root).await;
        let mut swept = 0usize;
        for xa in found
            .iter()
            .filter(|x| !mine.contains(&x.gtrid) && !mine.contains(&x.name))
        {
            xa_rollback(root, xa).await;
            swept += 1;
        }
        if swept > 0 {
            log.add(format!(
                "XA sweep: rolled back {swept} leftover lwf_* transaction(s)"
            ));
        }
        let _ = SWEPT.set(());
    }
    let mine = live_xids().lock().unwrap().clone();
    xa_recover_lwf(root)
        .await
        .into_iter()
        .filter(|x| !mine.contains(&x.gtrid) && !mine.contains(&x.name))
        .map(|x| x.name)
        .collect()
}

/// Whether this process has run the suite-start sweep. `S5` asserts it: a seed that skipped the
/// sweep would leave another tenant's binlog purge pinned, and that must be a failure, not a line
/// of code somebody deleted.
pub fn sweep_ran() -> bool {
    SWEPT.get().is_some()
}

static SWEPT: OnceLock<()> = OnceLock::new();

/// One XA transaction of a chaos seed: a name unique to this run, registered live (so the sweep
/// leaves it alone) and rolled back on every path out of the test, a panic included.
pub struct XaGuard {
    xid: String,
    root: String,
    finished: AtomicBool,
    log: Option<FaultLog>,
}

impl XaGuard {
    /// Takes the xid into protection *before* `XA START` runs: a seed that panics between the two
    /// still leaves nothing behind.
    pub fn new(root: &str, xid: &str, log: Option<FaultLog>) -> XaGuard {
        live_xids().lock().unwrap().insert(xid.to_string());
        XaGuard {
            xid: xid.to_string(),
            root: root.to_string(),
            finished: AtomicBool::new(false),
            log,
        }
    }

    pub fn xid(&self) -> &str {
        &self.xid
    }

    /// The seed finished with the branch itself (`XA COMMIT` or an explicit `XA ROLLBACK`): the
    /// guard still tries the rollback, which answers `ER_XAER_NOTA` and is ignored.
    pub fn mark_finished(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }
}

impl Drop for XaGuard {
    fn drop(&mut self) {
        let xid = self.xid.clone();
        let root = self.root.clone();
        let finished = self.finished.load(Ordering::Relaxed);
        live_xids().lock().unwrap().remove(&xid);
        if let Some(l) = &self.log {
            l.add(format!(
                "XA guard: {xid} released ({})",
                if finished {
                    "finished by the seed, safety-net rollback"
                } else {
                    "unfinished: rolled back"
                }
            ));
        }
        // Drop cannot await, so the cleanup runs on a thread with a runtime of its own (the same
        // shape `common::db_rtt` uses). Every statement on it is bounded, so a sick server slows
        // the test's end; it cannot hang it forever.
        let for_thread = xid.clone();
        let h = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async {
                let mut c = root_conn(&root).await;
                let _ = tokio::time::timeout(
                    Duration::from_secs(20),
                    c.query_drop(format!("XA ROLLBACK '{for_thread}'")),
                )
                .await;
                super::quit(c).await;
                Ok::<_, mysql_async::Error>(())
            })
        });
        if let Err(e) = h.join() {
            eprintln!("XA guard: the rollback of {xid} did not complete cleanly: {e:?}");
        }
    }
}

// --------------------------------------------------------------------------- drain

/// One rule's `(binlog_file, binlog_pos)` through a reused connection (same reasoning as
/// `status_rows`: the drain must not hand the process a fresh socket every 150 ms).
async fn rule_pos(
    dsn: &str,
    rule: &str,
    conn: &mut Option<mysql_async::Conn>,
) -> Option<(String, u64)> {
    for _attempt in 0..2 {
        if conn.is_none() {
            let opened = tokio::time::timeout(
                Duration::from_secs(5),
                mysql_async::Conn::new(mysql_async::Opts::from_url(dsn).expect("dsn")),
            )
            .await;
            match opened {
                Ok(Ok(c)) => *conn = Some(c),
                _ => return None,
            }
        }
        let c = conn.as_mut().expect("connected");
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            c.query::<mysql_async::Row, _>(format!(
                "SELECT binlog_file, binlog_pos FROM lazy_wire_fire_status WHERE rule = '{rule}'"
            )),
        )
        .await;
        match r {
            Ok(Ok(rows)) => {
                return rows
                    .first()
                    // A rule that has not streamed yet has NULL here: `get_opt` says so,
                    // `get` would panic the drain.
                    .and_then(|row| {
                        Some((
                            row.get_opt::<String, _>(0)?.ok()?,
                            row.get_opt::<u64, _>(1)?.ok()?,
                        ))
                    });
            }
            _ => *conn = None,
        }
    }
    None
}

/// Waiting for drain (design §3), conditions not sleeps: (1) every registered phase has its
/// expected keys at its mock, (2) `/status` shows `pending_keys == 0 && firing == false` for
/// every rule twice a status period apart (like `Instance::quiet`), (3) `rule`'s status row is
/// >= `last_cp`. Surfaces any sampler violation on the way (and on timeout, with the context).
pub async fn drain(
    sp: &Sampler,
    lazy: &Lazy,
    db: &Db,
    rule: &str,
    last_cp: (String, u64),
    secs: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut calm = 0;
    let mut conn: Option<mysql_async::Conn> = None;
    while calm < 2 {
        sp.check();
        let open = sp.open_phases();
        let s = lazy.status().await;
        let busy = s["rules"].as_object().is_none_or(|r| {
            r.values()
                .any(|v| v["pending_keys"] != 0 || v["firing"] == true)
        });
        let row_ok = rule_pos(&db.dsn, rule, &mut conn)
            .await
            .is_some_and(|(f, p)| (f, p) >= last_cp);
        if open.is_empty() && !busy && row_ok {
            calm += 1;
        } else {
            calm = 0;
        }
        if Instant::now() > deadline {
            sp.check();
            panic!(
                "drain timed out after {secs} s (open phases: {:?}, busy={busy}, \
                 status row of {rule} past checkpoint: {row_ok})\n{}",
                sp.open_phases(),
                sp.context().await
            );
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    sp.check();
}
