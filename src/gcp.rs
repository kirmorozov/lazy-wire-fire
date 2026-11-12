//! Google Cloud credentials for the `pubsub` kind and the `oauth=google` tokens of the http
//! kinds. A service-account key comes from a secret reference (read on every use, as the AWS
//! references are), or from Application Default Credentials through `google-cloud-auth`
//! (default features off: ring only, no aws-lc).
//!
//! lazywf keeps the token itself: it is reused until [`REFRESH_BEFORE`] before its lifetime
//! ends. The crate does not report the expiry, so the lifetime is Google's one-hour access
//! token, [`TOKEN_LIFETIME`]. A changed key (new bytes behind the reference) drops the cached
//! token at once. A 401 from the service drops it too (see [`Auth::invalidate`]). One fetch is
//! in flight per target: senders that arrive while it runs wait for it and share its token
//! (see [`Auth::token`]).

use std::sync::{Mutex, MutexGuard, Once};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use google_cloud_auth::credentials::{self, service_account};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;

use crate::secrets;

/// Lifetime of a Google access token, as the service issues it (not reported by the crate).
pub const TOKEN_LIFETIME: Duration = Duration::from_secs(3600);
/// A token is replaced this long before its lifetime ends.
pub const REFRESH_BEFORE: Duration = Duration::from_secs(300);

/// Installs the ring rustls provider, once. `google-cloud-auth` (and reqwest) with default
/// features off panic without a process-wide provider, so this runs before any credential is
/// built. A provider installed already (by another caller) is kept.
pub fn install_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Where a target's Google credentials come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// `vars.credentials_ref`: a reference (`env:` or `file:`) to a service-account key (JSON).
    Key(String),
    /// Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS, the gcloud file, the
    /// metadata server).
    Adc,
}

struct Cached {
    key_hash: [u8; 32],
    token: String,
    fetched: Instant,
}

/// The token of one target, cached until it is due for refresh.
pub struct Auth {
    source: Source,
    /// The scopes requested on the token, empty for the crate's default (what `pubsub` uses).
    scopes: Vec<String>,
    cache: Mutex<Option<Cached>>,
    /// The token most recently handed out. [`Auth::invalidate`] drops the cache, and a
    /// service can still echo that token back in the error of the very request that follows:
    /// kept for [`Auth::scrub`] alone, so it cannot reach a log line or `last_error`.
    issued: Mutex<Option<String>>,
    /// One fetch in flight at a time: the others wait behind it and take its token (the
    /// `aws::Provider` pattern), so a burst of sends costs one token request.
    flight: AsyncMutex<()>,
}

impl Auth {
    pub fn new(source: Source) -> Auth {
        Auth::with_scopes(source, Vec::new())
    }

    /// The same with the token's scopes requested explicitly (the `vars.scope` of an oauth
    /// target). `pubsub` keeps [`Auth::new`], which asks for nothing and gets the crate's
    /// default.
    pub fn with_scopes(source: Source, scopes: Vec<String>) -> Auth {
        install_crypto_provider();
        Auth {
            source,
            scopes,
            cache: Mutex::new(None),
            issued: Mutex::new(None),
            flight: AsyncMutex::new(()),
        }
    }

    /// A bearer token for one request. A reference is read again on every call; the cached
    /// token is used only while the key bytes are the same and the token is not due. One
    /// fetch runs at a time; senders that arrive while it runs wait and share its token.
    pub async fn token(&self) -> Result<String> {
        let (key_hash, key) = self.resolve()?;
        if let Some(token) = self.fresh(&key_hash, Instant::now()) {
            return Ok(token);
        }
        let _flight = self.flight.lock().await;
        // The holder before us may have fetched while we waited: ask the cache again before
        // paying for a second token.
        if let Some(token) = self.fresh(&key_hash, Instant::now()) {
            return Ok(token);
        }
        let creds = match key {
            Some(json) => {
                let value: serde_json::Value = serde_json::from_str(&json)
                    .map_err(|_| anyhow!("the Google service-account key is not JSON"))?;
                let mut builder = service_account::Builder::new(value);
                if !self.scopes.is_empty() {
                    builder = builder.with_access_specifier(
                        service_account::AccessSpecifier::from_scopes(self.scopes.clone()),
                    );
                }
                builder
                    .build_access_token_credentials()
                    .map_err(|e| anyhow!("Google service-account credentials: {e}"))?
            }
            None => {
                let mut builder = credentials::Builder::default();
                if !self.scopes.is_empty() {
                    builder = builder.with_scopes(self.scopes.clone());
                }
                builder
                    .build_access_token_credentials()
                    .map_err(|e| anyhow!("Google application default credentials: {e}"))?
            }
        };
        let fetched = creds
            .access_token()
            .await
            .map_err(|e| anyhow!("Google access token: {e}"))?;
        *self.lock() = Some(Cached {
            key_hash,
            token: fetched.token.clone(),
            fetched: Instant::now(),
        });
        *self.last() = Some(fetched.token.clone());
        Ok(fetched.token)
    }

    /// The reference read again, and the digest its bytes hash to (`[0; 32]` for ADC).
    fn resolve(&self) -> Result<([u8; 32], Option<String>)> {
        match &self.source {
            Source::Key(reference) => {
                let json = secrets::resolve(reference).context("vars.credentials_ref")?;
                Ok((digest(json.as_bytes()), Some(json)))
            }
            Source::Adc => Ok(([0u8; 32], None)),
        }
    }

    /// Drops the cached token, so the next request fetches a new one.
    pub fn invalidate(&self) {
        *self.lock() = None;
    }

    /// Whether a token is cached right now (for a `Debug` that may name the target, never
    /// the token).
    pub fn cached(&self) -> bool {
        self.lock().is_some()
    }

    /// Masks the cached token in a text the service's answer produced: a server that echoes
    /// the request cannot carry the token into a log line or `last_error`.
    pub fn scrub(&self, s: &str) -> String {
        let mut s = match &*self.lock() {
            Some(c) => s.replace(&c.token, "***"),
            None => s.to_string(),
        };
        if let Some(t) = &*self.last() {
            s = s.replace(t.as_str(), "***");
        }
        s
    }

    /// The cached token, when it belongs to `key_hash` and is not yet due at `now`.
    fn fresh(&self, key_hash: &[u8; 32], now: Instant) -> Option<String> {
        let guard = self.lock();
        let c = guard.as_ref()?;
        let age = now.checked_duration_since(c.fetched).unwrap_or_default();
        (c.key_hash == *key_hash && age < TOKEN_LIFETIME - REFRESH_BEFORE).then(|| c.token.clone())
    }

    fn lock(&self) -> MutexGuard<'_, Option<Cached>> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn last(&self) -> MutexGuard<'_, Option<String>> {
        self.issued.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cache entry fetched `age` ago for the key with digest `k`.
    fn cached(k: &[u8], age: Duration) -> (Auth, Instant) {
        let now = Instant::now();
        let auth = Auth::new(Source::Adc);
        *auth.lock() = Some(Cached {
            key_hash: digest(k),
            token: "tok".into(),
            fetched: now - age,
        });
        (auth, now)
    }

    #[test]
    fn token_is_reused_until_shortly_before_expiry() {
        let k = b"key";
        let (a, now) = cached(k, Duration::from_secs(3000));
        assert_eq!(a.fresh(&digest(k), now).as_deref(), Some("tok"));
        // 300 s before the one-hour lifetime: due.
        let (a, now) = cached(k, Duration::from_secs(3301));
        assert_eq!(a.fresh(&digest(k), now), None);
    }

    #[test]
    fn a_changed_key_drops_the_cached_token() {
        let (a, now) = cached(b"old key", Duration::from_secs(10));
        assert_eq!(a.fresh(&digest(b"new key"), now), None);
        assert_eq!(a.fresh(&digest(b"old key"), now).as_deref(), Some("tok"));
    }

    #[test]
    fn invalidate_forgets_the_token() {
        let (a, now) = cached(b"k", Duration::from_secs(1));
        a.invalidate();
        assert_eq!(a.fresh(&digest(b"k"), now), None);
    }

    #[test]
    fn an_empty_reference_fails_before_any_request() {
        // A reference that does not resolve is an error, never an anonymous request.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let auth = Auth::new(Source::Key("env:LWF_GCP_UNSET_FOR_TEST".into()));
        assert!(rt.block_on(auth.token()).is_err());
    }

    #[test]
    fn pubsub_keeps_the_default_access_and_scoped_auth_shares_the_cache() {
        // `Auth::new` (pubsub) asks for no scopes; `with_scopes` (oauth targets) differs from
        // it only in the scopes carried: the cache, its margin and the key drop are shared.
        let plain = Auth::new(Source::Adc);
        assert!(plain.scopes.is_empty());
        assert!(!plain.cached());
        let scoped = Auth::with_scopes(
            Source::Adc,
            vec!["https://www.googleapis.com/auth/spreadsheets".into()],
        );
        assert_eq!(scoped.scopes.len(), 1);
        let (a, now) = cached(b"key", Duration::from_secs(10));
        assert_eq!(a.fresh(&digest(b"key"), now).as_deref(), Some("tok"));
        assert!(a.cached());
    }

    #[tokio::test]
    async fn one_fetch_is_in_flight_and_the_waiters_take_the_cached_token() {
        // The single-flight: while the guard is held, `token` waits for it and then finds a
        // fresh entry (placed by the holder it waited behind) instead of fetching again — no
        // second token request, and no network (ADC would fail here: there is nothing to fetch).
        let auth = std::sync::Arc::new(Auth::new(Source::Adc));
        let guard = auth.flight.lock().await;
        let waiting = {
            let auth = auth.clone();
            tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(10), auth.token()).await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The holder fetched: a fresh entry for ADC (no key: the zero hash).
        *auth.lock() = Some(Cached {
            key_hash: [0u8; 32],
            token: "shared".into(),
            fetched: Instant::now(),
        });
        drop(guard);
        let got = waiting.await.unwrap().expect("the waiter finished");
        assert_eq!(got.unwrap(), "shared");
    }
}
