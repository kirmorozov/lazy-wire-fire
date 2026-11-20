//! How a test makes a mutex poisoned: a task that dies holding it, the way a job that panics
//! mid-update does in production. The sites that treat poison as data (`unwrap_or_else(|e|
//! e.into_inner())`) are worth nothing until the mutex really is poisoned, so every one of those
//! tests takes the lock through here.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

/// Panics while holding `m` and comes back once it is poisoned — the state the next locker meets.
/// The panic is caught here, not silenced: it crosses the guard and unwinds it, which is the whole
/// point. Panics if `m` was already poisoned (a test that poisons the same mutex twice has stopped
/// testing the first panic) or if the panic did not escape the guard.
#[track_caller]
pub fn poison<T>(m: &Mutex<T>) {
    let r = catch_unwind(AssertUnwindSafe(|| {
        let _held = m.lock().expect("poison: the mutex is free");
        panic!("poison: a task died holding this mutex");
    }));
    assert!(
        r.is_err(),
        "the panic must cross the guard, not be swallowed by it"
    );
    assert!(m.is_poisoned(), "the mutex must be poisoned by now");
}
