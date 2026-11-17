//! lazywf: change-triggered actions from the MySQL binlog. Configured by `LAZYWF_*` variables.

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::watch;

const HELP: &str = "lazywf (lazy_wire_fire): change-triggered actions from the MySQL binlog

USAGE:
  lazywf              run (configured by environment variables)
  lazywf healthcheck  exit 0 if the running instance's /livez answers (container health checks)
  lazywf --version    print the version

CONNECTION (either):
  LAZYWF_HOST, LAZYWF_PORT (3306), LAZYWF_USER, LAZYWF_PASSWORD, LAZYWF_DATABASE
  LAZYWF_DSN=mysql://user:pass@host:3306/database      (mariadb:// accepted)
  LAZYWF_TLS=disabled|required|verify-ca|verify-full   LAZYWF_TLS_CA=/path/ca.pem
      (unset: TLS required for non-loopback hosts, plaintext on loopback)
  LAZYWF_BINLOG_USER / LAZYWF_BINLOG_PASSWORD (or LAZYWF_BINLOG_DSN): replication account

COMMON SETTINGS:
  LAZYWF_INSTANCE=default   LAZYWF_HEALTH_ADDR=127.0.0.1:8081 (or off)
  LAZYWF_STATUS_TOKEN=...   /status then needs the X-Lazywf-Token header (/livez, /readyz open)
  LAZYWF_ALLOWED_HOSTS=host,*.dom,10.0.0.0/8  (unset denies all target egress; * allows any)
  LAZYWF_METRICS_ADDR=127.0.0.1:8082 (opt-in Prometheus /metrics; off unless set)
  LAZYWF_LOG_FORMAT=text|json   RUST_LOG=info

Rules, triggers and targets are rows in lazy_wire_fire_rule, lazy_wire_fire_trigger and
lazy_wire_fire_target (created on start). See README.md.
";

fn chain(e: &anyhow::Error) -> String {
    let mut out: Vec<String> = Vec::new();
    for c in e.chain() {
        let s = c.to_string();
        if out.last().is_some_and(|l| l.contains(&s)) {
            continue;
        }
        out.push(s);
    }
    out.join(": ")
}

fn init_logging() {
    let rust_log = std::env::var("RUST_LOG").ok();
    let filter = lazy_wire_fire::logging::filter(rust_log.as_deref());
    let json = std::env::var("LAZYWF_LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json"));
    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(color)
            .init();
    }
}

/// Stack of the thread the runtime is driven from.
const MAIN_STACK: usize = 8 * 1024 * 1024;

fn main() -> ExitCode {
    // Not on the process's main thread: with musl its stack bounds read as only the pages
    // touched so far, and the template parser (pest, via `stacker`) then refuses every
    // template for lack of stack. A thread created with an explicit size reports its bounds.
    let thread = std::thread::Builder::new()
        .name("lazywf".into())
        .stack_size(MAIN_STACK)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map(|rt| rt.block_on(run()))
        });
    match thread.map(|t| t.join()) {
        Ok(Ok(Ok(code))) => code,
        Ok(Ok(Err(e))) => {
            eprintln!("lazywf: cannot start the runtime: {e}");
            ExitCode::FAILURE
        }
        // The engine task panicked. The panic hook has already written its own line to stderr;
        // this one names the cause, so a supervisor's restart of a crash loop reads as what it
        // is instead of as a silent exit 1.
        Ok(Err(_)) => {
            eprintln!("lazywf: the engine task panicked; see the message above");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("lazywf: cannot start: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("--version" | "-V" | "version") => {
            println!("lazywf {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Some("--help" | "-h" | "help") => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        Some("healthcheck") => {
            let addr = std::env::var("LAZYWF_HEALTH_ADDR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| ([127, 0, 0, 1], 8081).into());
            return healthcheck(addr).await;
        }
        Some(other) => {
            eprintln!("unknown argument `{other}`\n\n{HELP}");
            return ExitCode::from(2);
        }
        None => {}
    }
    init_logging();
    // The rustls provider of the process, before anything connects: ring, so every handshake
    // — including the rabbitmq-stream client's, whose tokio-rustls pulls aws-lc-rs into the
    // tree — runs on ring (the first install wins; `gcp::Auth` keeps its own call).
    lazy_wire_fire::gcp::install_crypto_provider();
    // Before the configuration: its DSN checks already use the binlog reader's messages.
    lazy_wire_fire::binlog::set_product("lazywf", "LAZYWF_");
    lazy_wire_fire::fence::set_table(lazy_wire_fire::model::FENCE);
    lazy_wire_fire::analyze::set_reserved(
        lazy_wire_fire::model::RESERVED_TABLES,
        &[lazy_wire_fire::model::RESERVED_PREFIX],
    );
    // The shutdown signal is registered here, on the path that can report a failure, and handed
    // to `start` as a value: the watcher that waits for it must not be the one to install it.
    start(register_sigterm()).await
}

/// The run path, from the shutdown signal's registration onwards. The registration arrives as a
/// parameter — `run()` hands over what [`register_sigterm`] returned — so that a failure to
/// register is a decision of this function, provable in a test without a signal that cannot be
/// installed, and not a panic in the watcher task below, which nobody joins.
async fn start(term: std::io::Result<Signal>) -> ExitCode {
    let term = match term {
        Ok(term) => term,
        Err(e) => {
            // An engine that cannot be told to stop must not run: the sends still in flight when
            // a SIGKILL lands are the duplicates its restart then repeats. The report is the one
            // a bad configuration gets (below): name the cause, leave a code a supervisor can
            // tell apart.
            tracing::error!("configuration error: registering the SIGTERM handler: {e}");
            return ExitCode::from(2);
        }
    };
    let cfg = match lazy_wire_fire::config::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("configuration error: {}", chain(&e));
            return ExitCode::from(2);
        }
    };
    let health = Arc::new(lazy_wire_fire::health::Health::new(
        cfg.status_token.clone(),
    ));
    if let Some(addr) = cfg.health_addr {
        tokio::spawn(lazy_wire_fire::health::serve(addr, health.clone()));
    }
    // Opt-in Prometheus exposition on its own listener (the unauthenticated status surface stays
    // narrow). Unset = off entirely: no listener, and the engine keeps no counters (a `None`
    // registry makes every flush/drop site a skipped branch).
    let metrics = cfg.metrics_addr.map(|addr| {
        let m = Arc::new(lazy_wire_fire::metrics::Metrics::default());
        tokio::spawn(lazy_wire_fire::metrics::serve(addr, m.clone()));
        m
    });
    let (tx, rx) = watch::channel(false);
    tokio::spawn(shutdown_signal(term, tx));
    match lazy_wire_fire::engine::run(cfg, health, metrics, rx).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("fatal: {}", chain(&e));
            ExitCode::FAILURE
        }
    }
}

/// Registers SIGTERM. `signal` fails with an `io::Error`, and the registration used to sit inside
/// the task that *waits* for the signal (the `tokio::spawn` in `start`, never joined): a failure
/// there panicked that task alone, so the process ran on with SIGTERM silently ignored — Ctrl-C
/// still answered — and a container was SIGKILLed after its grace period with sends in flight.
/// Here the same failure is `start`'s to report, and stops the startup.
fn register_sigterm() -> std::io::Result<Signal> {
    signal(SignalKind::terminate())
}

/// The shutdown watcher: waits for either signal and asks the engine to stop, once. Nothing in it
/// can fail any more — the one fallible step is [`register_sigterm`], which `run()` does before
/// this task exists.
async fn shutdown_signal(mut term: Signal, tx: watch::Sender<bool>) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutdown requested");
    let _ = tx.send(true);
}

/// `lazywf healthcheck`: exit 0 when the running instance's `/livez` answers, 1 when it does not
/// (the image's health check). The address is the caller's: the subcommand reads
/// `LAZYWF_HEALTH_ADDR`, a test hands in a port of its own.
async fn healthcheck(addr: SocketAddr) -> ExitCode {
    if lazy_wire_fire::health::probe(addr, "/livez").await {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Context};

    #[test]
    fn single_error_is_its_message() {
        assert_eq!(chain(&anyhow!("boom")), "boom");
    }

    #[test]
    fn causes_are_joined_outermost_first() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let e = anyhow::Error::new(io).context("connecting to the binlog host");
        assert_eq!(
            chain(&e),
            "connecting to the binlog host: connection refused"
        );

        let deep = Err::<(), _>(anyhow!("root cause"))
            .context("loading rule 7")
            .context("starting")
            .unwrap_err();
        assert_eq!(chain(&deep), "starting: loading rule 7: root cause");
    }

    #[test]
    fn a_cause_already_in_the_message_above_is_not_repeated() {
        // The outer message contains the inner one: the inner one is dropped.
        let e = Err::<(), _>(anyhow!("refused"))
            .context("connect: refused")
            .unwrap_err();
        assert_eq!(chain(&e), "connect: refused");
        // A different inner message is kept.
        let e = Err::<(), _>(anyhow!("timed out"))
            .context("connect: refused")
            .unwrap_err();
        assert_eq!(chain(&e), "connect: refused: timed out");
    }

    /// A `MakeWriter` into a shared buffer, for reading back what a subscriber wrote (the shape of
    /// `src/tests/engine.rs`'s).
    #[derive(Clone, Default)]
    struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;
        fn make_writer(&'a self) -> Captured {
            self.clone()
        }
    }

    /// A process environment variable, put back the way it was when the guard is dropped.
    struct Env(&'static str, Option<std::ffi::OsString>);

    impl Env {
        fn set(key: &'static str, value: &str) -> Env {
            let was = std::env::var_os(key);
            std::env::set_var(key, value);
            Env(key, was)
        }
    }

    impl Drop for Env {
        fn drop(&mut self) {
            match &self.1 {
                Some(v) => std::env::set_var(self.0, v),
                None => std::env::remove_var(self.0),
            }
        }
    }

    /// The current-thread runtime `start` expects, with the lines this test's own subscriber
    /// wrote read back as text.
    struct Logged {
        code: ExitCode,
        log: String,
    }

    fn logged(run: impl FnOnce() -> ExitCode) -> Logged {
        let out = Captured::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(out.clone())
            .with_ansi(false)
            .finish();
        let code = tracing::subscriber::with_default(sub, run);
        let log = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
        Logged { code, log }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// The audit's (S) site, out of the watcher: a SIGTERM that cannot be registered is a startup
    /// failure — the `configuration error:` line and the exit 2 a bad configuration gets — and not
    /// a panic in a task nobody joins, with the process left running on and SIGTERM ignored.
    #[test]
    fn a_sigterm_that_cannot_be_registered_is_a_startup_failure() {
        let rt = runtime();
        let r = logged(|| {
            rt.block_on(start(Err(std::io::Error::other(
                "no signal handler for you",
            ))))
        });
        assert_eq!(
            r.code,
            ExitCode::from(2),
            "the code a bad configuration gets:\n{}",
            r.log
        );
        assert!(
            r.log.contains("configuration error:") && r.log.contains("SIGTERM"),
            "the house line, naming the signal:\n{}",
            r.log
        );
        assert!(
            r.log.contains("no signal handler for you"),
            "and the reason the registration gave:\n{}",
            r.log
        );
    }

    /// The other half of the seam: a registration that works is not a startup failure — the
    /// startup walks past the signal and on to the configuration, which this test refuses on
    /// purpose. `LAZYWF_SERVER_ID=0` is the one value the configuration always rejects (and the
    /// DSN keeps a connection configured for it to reject), so the startup never opens a listener
    /// or dials a database from a unit test.
    #[test]
    fn a_registration_that_works_is_not_a_startup_failure() {
        let _dsn = Env::set("LAZYWF_DSN", "mysql://app:pw@127.0.0.1:3306/shop");
        let _server_id = Env::set("LAZYWF_SERVER_ID", "0");
        let rt = runtime();
        let r = logged(|| {
            rt.block_on(async {
                let term = register_sigterm().expect("SIGTERM registers on a normal system");
                start(Ok(term)).await
            })
        });
        assert!(
            !r.log.contains("registering the SIGTERM"),
            "a working registration takes no error path:\n{}",
            r.log
        );
        assert!(
            r.log.contains("configuration error:"),
            "the startup got past the signal and stopped at the configuration:\n{}",
            r.log
        );
        assert_eq!(
            r.code,
            ExitCode::from(2),
            "the configuration's code:\n{}",
            r.log
        );
    }

    /// The `healthcheck` subcommand's exit codes, unchanged: 0 while a running instance answers
    /// `/livez`, 1 when nothing does. The address is handed in, so no environment is needed.
    #[tokio::test]
    async fn healthcheck_exits_0_for_a_live_instance_and_1_without_one() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = live.local_addr().unwrap();
        let answer = tokio::spawn(async move {
            let (mut s, _) = live.accept().await.unwrap();
            // The request is read before the answer is written: a peer dropped while the request
            // is still in flight makes the probe's own write fail, which is not the story a live
            // instance tells.
            let mut req = [0u8; 256];
            let _ = s.read(&mut req).await;
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        assert_eq!(
            healthcheck(addr).await,
            ExitCode::SUCCESS,
            "a running instance: 0"
        );
        answer.await.unwrap();
        // Nothing listens on this one: bound only to be given a free port, then dropped, so the
        // connect is refused at once instead of after the probe's timeout.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        assert_eq!(
            healthcheck(dead_addr).await,
            ExitCode::FAILURE,
            "no instance: 1"
        );
    }
}
