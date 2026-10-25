//! The instance lock: one active lazywf per schema and instance name (`GET_LOCK` on the control
//! connection). A second copy stands by and takes over when the first one stops.

use std::time::Duration;

use anyhow::{bail, Result};
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
    loop {
        let got: Result<Option<Option<i64>>, _> =
            conn.exec_first("SELECT GET_LOCK(?, 0)", (lock,)).await;
        match got {
            Ok(g) if g.flatten() == Some(1) => break,
            Ok(_) => {}
            Err(e) if matches!(e, mysql_async::Error::Io(_) | mysql_async::Error::Driver(_)) => {
                tracing::warn!("standby: control connection lost ({e}); reconnecting");
                if let Ok(Ok(c)) =
                    tokio::time::timeout(Duration::from_secs(10), Conn::new(opts.clone())).await
                {
                    *conn = c;
                }
            }
            Err(e) => return Err(e.into()),
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

/// Takes the lock again on a new connection (the old one, and the lock with it, is gone).
/// Fails when another instance holds it now.
pub async fn reacquire(conn: &mut Conn, lock: &str) -> Result<()> {
    let got: Option<Option<i64>> = conn.exec_first("SELECT GET_LOCK(?, 0)", (lock,)).await?;
    if got.flatten() != Some(1) {
        bail!("the instance lock `{lock}` was taken by another instance while the control connection was down");
    }
    Ok(())
}

pub async fn release(conn: &mut Conn, lock: &str) {
    let _ = conn.exec_drop("DO RELEASE_LOCK(?)", (lock,)).await;
}

#[cfg(test)]
mod tests {
    #[test]
    fn names() {
        assert_eq!(super::name("shop", "default"), "lazywf:shop:default");
        let long = super::name(&"s".repeat(64), "default");
        assert!(long.len() <= 64 && long.starts_with("lazywf:"));
    }
}
