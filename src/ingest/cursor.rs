//! Lock-free monotonic high-watermark cursor tracking for Jetstream subscriptions.
//!
//! Jetstream firehose feeds provide microsecond-precision timestamps (`time_us`)
//! on every commit and heartbeat event. To prevent missing events or replaying previously
//! processed records across reconnections, [`CursorTracker`] maintains a strictly monotonic
//! high watermark using atomic operations.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free monotonic high-watermark cursor tracker.
///
/// Ensures that the recorded subscription cursor (`time_us`) never regresses,
/// even when receiving out-of-order events, frames with clock skew across PDS nodes,
/// or pure heartbeat cursor advancement frames.
#[derive(Debug)]
pub struct CursorTracker {
    watermark: AtomicU64,
}

impl Default for CursorTracker {
    fn default() -> Self {
        Self::new(0)
    }
}

impl CursorTracker {
    /// Creates a new [`CursorTracker`] initialized to `initial` microseconds.
    #[must_use]
    pub const fn new(initial: u64) -> Self {
        Self {
            watermark: AtomicU64::new(initial),
        }
    }

    /// Creates a new [`CursorTracker`] from an optional initial timestamp in microseconds.
    #[must_use]
    pub fn from_option(initial: Option<u64>) -> Self {
        Self::new(initial.unwrap_or(0))
    }

    /// Updates the watermark monotonically.
    ///
    /// Returns `true` if `time_us` was strictly greater than the recorded watermark
    /// and the watermark was advanced; returns `false` if `time_us` was stale, equal,
    /// or zero.
    pub fn update(&self, time_us: u64) -> bool {
        if time_us == 0 {
            return false;
        }
        let prev = self.watermark.fetch_max(time_us, Ordering::SeqCst);
        time_us > prev
    }

    /// Returns the current high-watermark timestamp in microseconds.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.watermark.load(Ordering::SeqCst)
    }

    /// Returns the current high-watermark timestamp in microseconds, or `None` if uninitialized (0).
    #[must_use]
    pub fn get_opt(&self) -> Option<u64> {
        let val = self.get();
        if val == 0 {
            None
        } else {
            Some(val)
        }
    }

    /// Forces the cursor watermark to a specific value (e.g., for deliberate replay or rewind).
    pub fn set(&self, val: u64) {
        self.watermark.store(val, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_advancement() {
        let tracker = CursorTracker::new(100);
        assert_eq!(tracker.get(), 100);

        // Advance forward
        assert!(tracker.update(200));
        assert_eq!(tracker.get(), 200);

        assert!(tracker.update(250));
        assert_eq!(tracker.get(), 250);

        // Stale or duplicate timestamps do not advance
        assert!(!tracker.update(250));
        assert_eq!(tracker.get(), 250);

        assert!(!tracker.update(150));
        assert_eq!(tracker.get(), 250);

        assert!(!tracker.update(0));
        assert_eq!(tracker.get(), 250);
    }

    #[test]
    fn test_out_of_order_and_clock_skew() {
        let tracker = CursorTracker::new(1000);
        let seq = [1500, 1200, 1800, 1750, 1900, 1600, 2000];
        for ts in seq {
            tracker.update(ts);
        }
        assert_eq!(tracker.get(), 2000);
    }

    #[test]
    fn test_from_option_and_get_opt() {
        let t1 = CursorTracker::from_option(None);
        assert_eq!(t1.get(), 0);
        assert_eq!(t1.get_opt(), None);

        let t2 = CursorTracker::from_option(Some(5000));
        assert_eq!(t2.get(), 5000);
        assert_eq!(t2.get_opt(), Some(5000));

        t1.set(12345);
        assert_eq!(t1.get(), 12345);
        assert_eq!(t1.get_opt(), Some(12345));
    }
}
