//! lazywf: change-triggered actions from the MySQL binlog. Configured by `LAZYWF_*` variables.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

const HELP: &str = "lazywf (lazy_wire_fire): change-triggered actions from the MySQL binlog

USAGE:
  lazywf              run (configured by environment variables)
  lazywf healthcheck  exit 0 if the running instance's /livez answers (container health checks)
  lazywf --version    print the version

CONNECTION (either):
  LAZYWF_HOST, LAZYWF_PORT (3306), LAZYWF_USER, LAZYWF_PASSWORD, LAZYWF_DATABASE
  LAZYWF_DSN=mysql://user:pass@host:3306/database      (mariadb:// accepted)
  LAZYWF_TLS=disabled|required|verify-ca|verify-full   LAZYWF_TLS_CA=/path/ca.pem
  LAZYWF_BINLOG_USER / LAZYWF_BINLOG_PASSWORD (or LAZYWF_BINLOG_DSN): replication account

COMMON SETTINGS:
  LAZYWF_INSTANCE=default   LAZYWF_HEALTH_ADDR=127.0.0.1:8081 (or off)
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
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
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
        Ok(Err(_)) => ExitCode::FAILURE,
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
            return if lazy_wire_fire::health::probe(addr, "/livez").await {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
        Some(other) => {
            eprintln!("unknown argument `{other}`\n\n{HELP}");
            return ExitCode::from(2);
        }
        None => {}
    }
    init_logging();
    // Before the configuration: its DSN checks already use the binlog reader's messages.
    lazy_wire_fire::binlog::set_product("lazywf", "LAZYWF_");
    let cfg = match lazy_wire_fire::config::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("configuration error: {}", chain(&e));
            return ExitCode::from(2);
        }
    };
    let health = Arc::new(lazy_wire_fire::health::Health::default());
    if let Some(addr) = cfg.health_addr {
        tokio::spawn(lazy_wire_fire::health::serve(addr, health.clone()));
    }
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown requested");
        let _ = tx.send(true);
    });
    match lazy_wire_fire::engine::run(cfg, health, rx).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("fatal: {}", chain(&e));
            ExitCode::FAILURE
        }
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
