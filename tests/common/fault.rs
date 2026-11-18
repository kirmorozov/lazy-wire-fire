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
use std::sync::{Arc, Mutex};
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
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Live,
    CutAll,
    Stall,
    Refuse,
}

#[derive(Clone, Copy)]
struct Mode {
    kind: Kind,
    /// Per connection, server→client: RST once this many bytes have been relayed.
    cut_after_bytes: Option<u64>,
    /// Per direction, per relayed chunk.
    latency_ms: u64,
}

struct Core {
    label: String,
    upstream: String,
    mode: Mutex<Mode>,
    /// Bumped by `cut_all`; relay tasks born before it hard-close themselves.
    gen: AtomicU64,
    conns: AtomicU64,
    bytes: AtomicU64,
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
            }),
            gen: AtomicU64::new(0),
            conns: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
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
                match c.mode.lock().unwrap().kind {
                    // "port dead": answer the connect with an RST (the client sees a dead port).
                    Kind::CutAll | Kind::Refuse => hard_close(cli),
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

    pub fn bytes(&self) -> u64 {
        self.core.bytes.load(Ordering::Relaxed)
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

    /// Per connection, server→client: RST at the first sample past `n` relayed bytes.
    pub fn cut_after_bytes(&self, n: u64) {
        self.set(&format!("cut_after_bytes({n})"), |m| {
            m.cut_after_bytes = Some(n);
            m.kind = Kind::Live;
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
            }
            .to_string(),
            conns: self.conns(),
            bytes: self.bytes(),
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
        tokio::select! {
            r = cli.read(&mut buf_c2s) => match r {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if mode.latency_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(mode.latency_ms)).await;
                    }
                    if up.write_all(&buf_c2s[..n]).await.is_err() {
                        return;
                    }
                    core.bytes.fetch_add(n as u64, Ordering::Relaxed);
                }
            },
            r = up.read(&mut buf_s2c) => match r {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    core.bytes.fetch_add(n as u64, Ordering::Relaxed);
                    s2c += n as u64;
                    if let Some(bound) = mode.cut_after_bytes {
                        if s2c >= bound {
                            hard_close(cli);
                            hard_close(up);
                            return;
                        }
                    }
                    if mode.latency_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(mode.latency_ms)).await;
                    }
                    if cli.write_all(&buf_s2c[..n]).await.is_err() {
                        return;
                    }
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

    /// SIGKILL through `Child::kill`; the fault log records the window opener.
    pub fn kill(&self) {
        self.hard_stop();
        if let Some(l) = &self.log {
            l.add(format!("child {} killed (SIGKILL)", self.name));
        }
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

struct Shared {
    dsn: String,
    fault_log: FaultLog,
    phases: Mutex<Vec<Phase>>,
    logs: Mutex<Vec<PathBuf>>,
    proxies: Mutex<Vec<FaultProxy>>,
    last_pos: Mutex<BTreeMap<String, (String, u64)>>,
    violation: Mutex<Option<String>>,
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
            logs: Mutex::new(Vec::new()),
            proxies: Mutex::new(Vec::new()),
            last_pos: Mutex::new(BTreeMap::new()),
            violation: Mutex::new(None),
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
        return violate(
            sp,
            format!(
                "P6: rule {rule} position regressed {}:{} -> {}:{}",
                prev.0, prev.1, cur.0, cur.1
            ),
        )
        .await;
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
