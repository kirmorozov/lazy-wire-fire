//! The unit tests' counting allocator, moved out of the keyspace tests verbatim and
//! extended with a per-thread allocation counter. `#[global_allocator]` here is compiled
//! only into the test binaries (`cfg(test)`, declared from `lib.rs`); the shipping binary
//! keeps the system allocator. While `ON`, every live heap byte is tallied (`live`,
//! `bytes`); `count` additionally runs a closure and reports how many allocations the
//! calling thread made while it ran.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);
pub static ON: AtomicBool = AtomicBool::new(false);

thread_local! {
    // Const-initialized and non-`Drop`: touching these from inside the allocator cannot
    // itself allocate (no lazy init, no TLS destructor to register).
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static TRACK: Cell<bool> = const { Cell::new(false) };
}

pub fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

/// Runs `f` on this thread, returning its value and the number of allocations this
/// thread made while it ran. Allocations of other threads are not counted, so parallel
/// `#[test]`s cannot perturb the number.
pub fn count<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ALLOCS.with(|c| c.set(0));
    TRACK.with(|t| t.set(true));
    let out = f();
    let n = ALLOCS.with(|c| c.get());
    TRACK.with(|t| t.set(false));
    (out, n)
}

struct Counting;

// SAFETY: delegates every allocation to `System` and only tallies sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if ON.load(Ordering::Relaxed) && !p.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        if !p.is_null() && TRACK.with(|t| t.get()) {
            ALLOCS.with(|c| c.set(c.get() + 1));
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if ON.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        }
        System.dealloc(p, layout);
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;
