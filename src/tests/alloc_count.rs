//! The counting allocator the tests measure with: a wrapper around the system allocator,
//! installed as the `#[global_allocator]` of the test build only (the shipping binary is
//! unaffected). Two ways to read it:
//!
//! * [`live`], the bytes alive while [`ON`] is set (a global tally, what a key costs held);
//! * [`counted`], the allocations of the calling thread while it runs (what one row of the
//!   hot path costs — other threads, a runtime's workers, are not counted).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);
/// Tally every thread's live bytes (the keyspace cost test).
pub static ON: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// This thread counts its own allocations ([`counted`]). `const` initialisers: reading a
    /// thread local must not be an allocation itself.
    static TALLY: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

/// The bytes allocated since `ON` was set (minus what was freed meanwhile).
pub fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

/// Runs `f` on this thread and returns its value with the number of allocations it made.
pub fn counted<T>(f: impl FnOnce() -> T) -> (T, u64) {
    ALLOCS.with(|a| a.set(0));
    TALLY.with(|t| t.set(true));
    let v = f();
    let n = ALLOCS.with(|a| a.get());
    TALLY.with(|t| t.set(false));
    (v, n)
}

struct Counting;

// SAFETY: delegates every allocation to `System` and only tallies sizes and counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if ON.load(Ordering::Relaxed) && !p.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        if TALLY.with(|t| t.get()) {
            ALLOCS.with(|a| a.set(a.get() + 1));
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if ON.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        }
        unsafe { System.dealloc(p, layout) };
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;
