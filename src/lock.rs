//! The instance lock: one active lazywf per schema and instance name (`GET_LOCK` on the control
//! connection). A second copy stands by and takes over when the first one stops.

use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::health::Health;

/// `lazywf:<schema>:<instance>`, hashed when longer than the 64 characters a lock name may have.
pub fn name(schema: &str, instance: &str) -> String {
    let n = format!("lazywf:{schema}:{instance}");
    if n.chars().count() <= 64 {
        n
    } else {
        format!(
            "lazywf:{}",
            &hex::encode(Sha256::digest(n.as_bytes()))[..40]
        )
    }
}

/// Waits until this connection holds the lock. `Ok(false)`: shutdown requested meanwhile.
pub async fn acquire(
    conn: &mut Conn,
    opts: &Opts,
    lock: &str,
    health: &Health,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool> {
    let mut logged = false;
    let mut wait = Duration::from_millis(250);
    // One stall episode logs once (see `crate::engine::ctl`): the standby probes every few
    // seconds, and a held connection is one fact, not one per attempt.
    let stalled = AtomicBool::new(false);
    loop {
        // `GET_LOCK(?, 0)` answers at once on a responsive server, so the control
        // connection's per-statement bound fits the probe: anything slower is a stuck
        // connection, not a wait, and is treated as one (reconnect; the standby loop keeps
        // waiting and beating as it did).
        let got = crate::engine::ctl_my(
            &stalled,
            "SELECT GET_LOCK for the instance lock",
            crate::engine::CONTROL_STATEMENT_TIMEOUT,
            conn.exec_first::<Option<i64>, _, _>("SELECT GET_LOCK(?, 0)", (lock,)),
        )
        .await;
        match got {
            Ok(g) if g.flatten() == Some(1) => break,
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.downcast_ref::<mysql_async::Error>(),
                    Some(mysql_async::Error::Io(_)) | Some(mysql_async::Error::Driver(_))
                ) || e.downcast_ref::<crate::engine::ControlStalled>().is_some() =>
            {
                tracing::warn!("standby: control connection lost ({e}); reconnecting");
                if let Ok(Ok(c)) =
                    tokio::time::timeout(Duration::from_secs(10), Conn::new(opts.clone())).await
                {
                    *conn = c;
                }
            }
            Err(e) => return Err(e),
        }
        if !logged {
            tracing::info!("another lazywf instance holds `{lock}`; standing by to take over");
            logged = true;
        }
        health.set_phase("standby");
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(false) },
        }
        health.beat();
        wait = (wait * 2).min(Duration::from_secs(5));
    }
    if logged {
        tracing::info!("took over as the active instance");
    }
    Ok(true)
}

/// This connection's id, as the server names it and as [`holder`] names a holder. Text on
/// purpose: MySQL answers `CONNECTION_ID()` as a number and MariaDB as a string, and the cast
/// makes either one comparable in Rust — which is what lets the heal path recognise its own
/// session by the id it recorded when it took the lock (see `Engine::control_conn_id`).
pub async fn connection_id(conn: &mut Conn) -> Result<String> {
    let id: Option<Option<String>> = conn
        .exec_first("SELECT CAST(CONNECTION_ID() AS CHAR)", ())
        .await?;
    id.flatten()
        .ok_or_else(|| anyhow!("the server answered no connection id"))
}

/// Who holds `lock` right now: the holder's id as [`connection_id`] words it, or `None` when
/// nobody does. `GET_LOCK`'s refusal says only that the lock is taken; this says by whom, which
/// is the difference between a peer that has taken a node's place and the node's own session
/// that the server has not finished closing.
pub async fn holder(conn: &mut Conn, lock: &str) -> Result<Option<String>> {
    let holder: Option<Option<String>> = conn
        .exec_first("SELECT CAST(IS_USED_LOCK(?) AS CHAR)", (lock,))
        .await?;
    Ok(holder.flatten())
}

/// Fails unless this connection still holds the lock.
pub async fn verify(conn: &mut Conn, lock: &str) -> Result<()> {
    let ok: Option<Option<i64>> = conn
        .exec_first("SELECT IS_USED_LOCK(?) = CONNECTION_ID()", (lock,))
        .await?;
    if ok.flatten() != Some(1) {
        bail!("lost the instance lock `{lock}`");
    }
    Ok(())
}

/// The instance lock was still held when [`reacquire`] gave up waiting. Its own type so the
/// caller can count denials without matching on the wording, and ask [`holder`] who holds it
/// before it says anything: a refusal is only proof of a peer once the holder turns out not to
/// be this node's own closing session. When it is a peer, the copy an operator sees is the same
/// one the node has always logged when it loses its place.
#[derive(Debug)]
pub struct LockDenied {
    pub lock: String,
}

impl std::fmt::Display for LockDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the instance lock `{}` was taken by another instance while the control connection \
             was down",
            self.lock
        )
    }
}

impl std::error::Error for LockDenied {}

/// Takes the lock again on a new connection (the old one, and the lock with it, is gone).
///
/// `wait` is how long the server may hold the question open. It cannot be `0` here: a session
/// that died a moment ago keeps the lock until the server notices the connection went away,
/// and the heal path closes that session and asks for the lock in the same instant — a
/// `GET_LOCK(?, 0)` issued then loses its own FIN by a few milliseconds and reads as a peer
/// that has taken over, which is how a node used to die on a control outage that had already
/// healed. Fails with [`LockDenied`] when the lock is still held after `wait`; what that
/// refusal means is the caller's question to answer, with [`holder`].
pub async fn reacquire(conn: &mut Conn, lock: &str, wait: Duration) -> Result<()> {
    // `as_secs()` (a `u64`) rather than a cast to `i64`: the bound is a whole number of
    // seconds, the server takes it as a number, and this way the line is not a new
    // panic-capable site for `make lint`'s panic gate to ask about.
    let got: Option<Option<i64>> = conn
        .exec_first("SELECT GET_LOCK(?, ?)", (lock, wait.as_secs()))
        .await?;
    if got.flatten() != Some(1) {
        return Err(LockDenied {
            lock: lock.to_string(),
        }
        .into());
    }
    Ok(())
}

pub async fn release(conn: &mut Conn, lock: &str) {
    let _ = conn.exec_drop("DO RELEASE_LOCK(?)", (lock,)).await;
}

#[cfg(test)]
mod tests {
    /// The copy an operator reads when a peer really has taken over: the node has always
    /// logged it this way (docs quote it, the chaos seeds match on it), and `LockDenied` is a
    /// type only so the heal path can count denials without reading the wording.
    #[test]
    fn lock_denied_names_the_lock_it_could_not_take() {
        let said = super::LockDenied {
            lock: "lazywf:shop:default".into(),
        }
        .to_string();
        assert!(said.contains("lazywf:shop:default"), "{said}");
        assert!(
            said.contains("taken by another instance while the control connection was down"),
            "{said}"
        );
    }

    #[test]
    fn names() {
        assert_eq!(super::name("shop", "default"), "lazywf:shop:default");
        let long = super::name(&"s".repeat(64), "default");
        assert!(long.len() <= 64 && long.starts_with("lazywf:"));
    }
}
