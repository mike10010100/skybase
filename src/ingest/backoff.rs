//! Exponential reconnect backoff engine with jitter and frame reset for `skybase::ingest`.
//!
//! Provides clock-warp safe, integer-arithmetic exponential backoff with pseudo-random
//! jitter for resilient WebSocket firehose reconnections.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Default initial reconnect backoff delay (500 ms).
pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 500;

/// Default maximum reconnect backoff delay cap (30 seconds).
pub const DEFAULT_MAX_BACKOFF_SECS: u64 = 30;

/// Default jitter percentage applied to base delay (±20%).
pub const DEFAULT_JITTER_PERCENT: u32 = 20;

/// Minimum allowable backoff delay floor to avoid busy-spin loops (50 ms).
pub const MIN_BACKOFF_FLOOR_MS: u64 = 50;

/// Fallback nanoseconds for clock-warp situations where system time is prior to UNIX EPOCH.
const CLOCK_WARP_FALLBACK_NANOS: u64 = 123_456_789;

/// Multiplier constant for 64-bit Linear Congruential Generator.
const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;

/// Manages exponential backoff delays with random jitter for reconnect attempts.
///
/// Ensures clock-warp safety, integer overflow protection, and frame-level resets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackoffManager {
    initial_delay: Duration,
    max_delay: Duration,
    current_delay: Duration,
    consecutive_failures: u32,
    jitter_percent: u32,
    last_attempt_at: Option<Instant>,
}

impl Default for BackoffManager {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS),
            Duration::from_secs(DEFAULT_MAX_BACKOFF_SECS),
        )
    }
}

impl BackoffManager {
    /// Creates a new [`BackoffManager`] with specified initial and maximum delay caps.
    ///
    /// The initial delay is clamped to a minimum of [`MIN_BACKOFF_FLOOR_MS`].
    /// The maximum delay is clamped to be at least equal to `initial_delay`.
    #[must_use]
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        let safe_initial = initial_delay.max(Duration::from_millis(MIN_BACKOFF_FLOOR_MS));
        let safe_max = max_delay.max(safe_initial);
        Self {
            initial_delay: safe_initial,
            max_delay: safe_max,
            current_delay: safe_initial,
            consecutive_failures: 0,
            jitter_percent: DEFAULT_JITTER_PERCENT,
            last_attempt_at: None,
        }
    }

    /// Sets the jitter percentage (e.g., 20 for ±20% jitter). Clamped to at most 100%.
    #[must_use]
    pub fn with_jitter(mut self, jitter_percent: u32) -> Self {
        self.jitter_percent = jitter_percent.min(100);
        self
    }

    /// Computes the next backoff duration with exponential doubling and pseudo-random jitter.
    ///
    /// Updates internal state for subsequent calls, increments failure count,
    /// and records the attempt timestamp using the monotonic clock.
    pub fn next_backoff(&mut self) -> Duration {
        let base = self.current_delay;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_attempt_at = Some(Instant::now());

        // Exponential doubling for the subsequent step
        let next_ms = (self.current_delay.as_millis() as u64).saturating_mul(2);
        self.current_delay = Duration::from_millis(next_ms).min(self.max_delay);

        // Calculate pseudo-random jitter in [-jitter_percent, +jitter_percent]
        let base_ms = base.as_millis() as u64;

        // Clock-warp safe entropy gathering from SystemTime
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(CLOCK_WARP_FALLBACK_NANOS, |d| u64::from(d.subsec_nanos()));

        // Linear congruential pseudo-random number generator
        let pseudo_rand = (nanos
            .wrapping_mul(LCG_MULTIPLIER)
            .wrapping_add(u64::from(self.consecutive_failures))
            >> 32) as u32;

        let jitter_range_pct = i64::from(self.jitter_percent);
        let modulo = self.jitter_percent.saturating_mul(2).saturating_add(1);
        let jitter_offset_pct = i64::from(pseudo_rand % modulo) - jitter_range_pct;

        let jittered_ms = if jitter_offset_pct >= 0 {
            let add_ms = base_ms.saturating_mul(jitter_offset_pct as u64) / 100;
            base_ms.saturating_add(add_ms)
        } else {
            let sub_ms = base_ms.saturating_mul((-jitter_offset_pct) as u64) / 100;
            base_ms.saturating_sub(sub_ms)
        };

        Duration::from_millis(jittered_ms.max(MIN_BACKOFF_FLOOR_MS))
    }

    /// Resets the backoff state to the initial delay and clears the consecutive failure counter.
    ///
    /// Must be invoked whenever a valid frame (commit or heartbeat) is received from Jetstream.
    pub fn reset(&mut self) {
        self.current_delay = self.initial_delay;
        self.consecutive_failures = 0;
    }

    /// Returns the number of consecutive failed attempts.
    #[must_use]
    pub const fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Alias for [`Self::consecutive_failures`] for compatibility with test assertions.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.consecutive_failures
    }

    /// Returns the current base delay before jitter calculation.
    #[must_use]
    pub const fn current_delay(&self) -> Duration {
        self.current_delay
    }

    /// Returns the configured initial backoff delay.
    #[must_use]
    pub const fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    /// Returns the configured maximum backoff delay cap.
    #[must_use]
    pub const fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Returns the configured jitter percentage.
    #[must_use]
    pub const fn jitter_percent(&self) -> u32 {
        self.jitter_percent
    }

    /// Returns the timestamp of the last backoff calculation, if any.
    #[must_use]
    pub const fn last_attempt_at(&self) -> Option<Instant> {
        self.last_attempt_at
    }

    /// Calculates elapsed time since the last attempt in a clock-warp safe manner.
    #[must_use]
    pub fn elapsed_since_last_attempt(&self, now: Instant) -> Option<Duration> {
        self.last_attempt_at
            .map(|last| now.saturating_duration_since(last))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_initial_clamping() {
        // Less than floor clamps to MIN_BACKOFF_FLOOR_MS (50ms)
        let b1 = BackoffManager::new(Duration::from_millis(10), Duration::from_millis(200));
        assert_eq!(b1.initial_delay(), Duration::from_millis(50));
        assert_eq!(b1.max_delay(), Duration::from_millis(200));

        // Max smaller than initial clamps max to initial
        let b2 = BackoffManager::new(Duration::from_millis(500), Duration::from_millis(100));
        assert_eq!(b2.initial_delay(), Duration::from_millis(500));
        assert_eq!(b2.max_delay(), Duration::from_millis(500));
    }

    #[test]
    fn test_backoff_exponential_growth_and_jitter_bounds() {
        let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));

        assert_eq!(backoff.current_delay(), Duration::from_millis(500));
        assert_eq!(backoff.consecutive_failures(), 0);

        // Attempt 1: base = 500ms, jitter in [-20%, +20%] -> [400ms, 600ms]
        let b1 = backoff.next_backoff();
        assert!((400..=600).contains(&b1.as_millis()), "b1 was {:?}", b1);
        assert_eq!(backoff.consecutive_failures(), 1);
        assert_eq!(backoff.current_delay(), Duration::from_millis(1000));

        // Attempt 2: base = 1000ms, jitter in [-20%, +20%] -> [800ms, 1200ms]
        let b2 = backoff.next_backoff();
        assert!((800..=1200).contains(&b2.as_millis()), "b2 was {:?}", b2);
        assert_eq!(backoff.consecutive_failures(), 2);
        assert_eq!(backoff.current_delay(), Duration::from_millis(2000));

        // Attempt 3: base = 2000ms, jitter in [-20%, +20%] -> [1600ms, 2400ms]
        let b3 = backoff.next_backoff();
        assert!((1600..=2400).contains(&b3.as_millis()), "b3 was {:?}", b3);
        assert_eq!(backoff.consecutive_failures(), 3);
        assert_eq!(backoff.current_delay(), Duration::from_millis(4000));
    }

    #[test]
    fn test_backoff_max_delay_cap() {
        let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));

        // Advance 10 times to reach 30s cap
        for _ in 0..10 {
            backoff.next_backoff();
        }

        assert_eq!(backoff.current_delay(), Duration::from_secs(30));
        let b_cap = backoff.next_backoff();
        // 30,000ms ± 20% = [24,000ms, 36,000ms]
        assert!(
            (24_000..=36_000).contains(&b_cap.as_millis()),
            "b_cap was {:?}",
            b_cap
        );
    }

    #[test]
    fn test_backoff_reset_restores_initial_state() {
        let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));

        for _ in 0..5 {
            backoff.next_backoff();
        }
        assert!(backoff.consecutive_failures() > 0);
        assert!(backoff.current_delay() > Duration::from_millis(500));

        backoff.reset();
        assert_eq!(backoff.consecutive_failures(), 0);
        assert_eq!(backoff.current_delay(), Duration::from_millis(500));

        let b_reset = backoff.next_backoff();
        assert!((400..=600).contains(&b_reset.as_millis()));
    }

    #[test]
    fn test_elapsed_time_clock_warp_safety() {
        let mut backoff = BackoffManager::default();
        assert!(backoff.last_attempt_at().is_none());

        let _ = backoff.next_backoff();
        let last = backoff
            .last_attempt_at()
            .expect("should have recorded attempt");

        // Normal elapsed
        let later = last + Duration::from_secs(2);
        assert_eq!(
            backoff.elapsed_since_last_attempt(later),
            Some(Duration::from_secs(2))
        );

        // Simulated backwards clock warp (earlier timestamp)
        let earlier = last.checked_sub(Duration::from_secs(5)).unwrap_or(last);
        assert_eq!(
            backoff.elapsed_since_last_attempt(earlier),
            Some(Duration::ZERO)
        );
    }
}
