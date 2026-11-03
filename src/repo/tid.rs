//! Monotonically increasing AT Protocol Timestamp Identifier (TID) generator.
//!
//! Generates 13-character base32 sortable identifiers compliant with the ATProto
//! TID specification.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

/// Base32 sortable character set used by ATProto TIDs (`234567abcdefghijklmnopqrstuvwxyz`).
const BASE32_CHARS: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

/// Monotonically increasing AT Protocol Timestamp Identifier (TID) generator.
///
/// Ensures sequential, collision-free, sortable identifiers using lock-free
/// atomic compare-and-swap operations.
#[derive(Debug)]
pub struct TidGenerator {
    last_val: AtomicU64,
}

impl Default for TidGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TidGenerator {
    /// Creates a new TID generator initialized to zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last_val: AtomicU64::new(0),
        }
    }

    /// Generates a fresh, monotonically increasing 13-character TID string.
    ///
    /// Preserves strict monotonicity even across clock skew or rapid consecutive calls.
    pub fn next_tid(&self) -> String {
        let now_micros = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_micros() as u64,
            Err(_) => 0,
        };

        // Top bit is 0, 53 bits for microseconds, shifted by 10 for clock/sequence counter
        let candidate_base = (now_micros & 0x001f_ffff_ffff_ffff) << 10;

        let mut current = self.last_val.load(Ordering::Relaxed);
        let val = loop {
            let next = if candidate_base > current {
                candidate_base
            } else {
                current.saturating_add(1)
            };

            match self.last_val.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break next,
                Err(actual) => current = actual,
            }
        };

        encode_tid_base32(val)
    }
}

/// Formats a 64-bit value into a 13-character ATProto base32 string.
fn encode_tid_base32(mut val: u64) -> String {
    let mut buf = [b'2'; 13];
    for i in (0..13).rev() {
        buf[i] = BASE32_CHARS[(val & 0x1f) as usize];
        val >>= 5;
    }
    match std::str::from_utf8(&buf) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(&buf).into_owned(),
    }
}

/// Convenience function to generate a fresh TID using a shared global generator.
///
/// # Examples
/// ```
/// use skybase::repo::generate_tid;
///
/// let tid1 = generate_tid();
/// let tid2 = generate_tid();
/// assert_eq!(tid1.len(), 13);
/// assert!(tid1 < tid2);
/// ```
#[must_use]
pub fn generate_tid() -> String {
    static GLOBAL_TID_GEN: TidGenerator = TidGenerator::new();
    GLOBAL_TID_GEN.next_tid()
}

/// Minimum plausible ATProto record timestamp in Unix seconds (2022-11-17T00:00:00Z).
pub const MIN_PLAUSIBLE_TID_EPOCH_SECS: u64 = 1_668_643_200;

/// Maximum plausible ATProto record timestamp in Unix seconds (2100-01-01T00:00:00Z).
pub const MAX_PLAUSIBLE_TID_EPOCH_SECS: u64 = 4_102_444_800;

/// Decodes an AT Protocol 13-character base32-sortable TID into a Unix timestamp in microseconds.
///
/// Returns `None` if `tid` is not 13 characters, contains characters outside the ATProto
/// base32-sortable alphabet (`234567abcdefghijklmnopqrstuvwxyz`), or has the reserved top bit set.
#[must_use]
pub fn decode_tid_timestamp_us(tid: &str) -> Option<u64> {
    if tid.len() != 13 {
        return None;
    }
    let mut val: u64 = 0;
    for b in tid.bytes() {
        let digit: u64 = match b {
            b'2'..=b'7' => u64::from(b - b'2'),
            b'a'..=b'z' => u64::from(b - b'a') + 6,
            _ => return None,
        };
        val = (val << 5) | digit;
    }
    if (val >> 63) != 0 {
        return None;
    }
    Some(val >> 10)
}

/// Decodes an AT Protocol 13-character base32-sortable TID into a Unix timestamp in seconds,
/// validating that the decoded timestamp lies within [`MIN_PLAUSIBLE_TID_EPOCH_SECS`]..=[`MAX_PLAUSIBLE_TID_EPOCH_SECS`].
#[must_use]
pub fn decode_tid_timestamp_secs(tid: &str) -> Option<u64> {
    let timestamp_us = decode_tid_timestamp_us(tid)?;
    let timestamp_secs = timestamp_us / 1_000_000;
    if (MIN_PLAUSIBLE_TID_EPOCH_SECS..=MAX_PLAUSIBLE_TID_EPOCH_SECS).contains(&timestamp_secs) {
        Some(timestamp_secs)
    } else {
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_tid_length_and_charset() {
        let generator = TidGenerator::new();
        for _ in 0..100 {
            let tid = generator.next_tid();
            assert_eq!(tid.len(), 13, "TID must be 13 chars: {tid}");
            for b in tid.bytes() {
                assert!(
                    BASE32_CHARS.contains(&b),
                    "Character '{b}' not in base32 charset"
                );
            }
        }
    }

    #[test]
    fn test_tid_monotonicity_in_tight_loop() {
        let generator = TidGenerator::new();
        let mut prev = generator.next_tid();

        for _ in 0..10_000 {
            let curr = generator.next_tid();
            assert!(
                curr > prev,
                "TIDs must be strictly monotonic: prev={prev}, curr={curr}"
            );
            prev = curr;
        }
    }

    #[test]
    fn test_tid_concurrent_uniqueness() {
        let generator = Arc::new(TidGenerator::new());
        let mut handles = Vec::new();
        let thread_count = 8;
        let per_thread = 1_000;

        for _ in 0..thread_count {
            let gen = Arc::clone(&generator);
            handles.push(thread::spawn(move || {
                let mut tids = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    tids.push(gen.next_tid());
                }
                tids
            }));
        }

        let mut all_tids = HashSet::new();
        for handle in handles {
            let tids = handle.join().expect("thread join failed");
            for tid in tids {
                assert!(all_tids.insert(tid), "Duplicate TID detected!");
            }
        }

        assert_eq!(all_tids.len(), thread_count * per_thread);
    }

    #[test]
    fn test_global_generate_tid() {
        let t1 = generate_tid();
        let t2 = generate_tid();
        assert_eq!(t1.len(), 13);
        assert_eq!(t2.len(), 13);
        assert!(t1 < t2);
        let decoded_secs = decode_tid_timestamp_secs(&t1).expect("valid TID timestamp");
        assert!(decoded_secs >= MIN_PLAUSIBLE_TID_EPOCH_SECS);
        assert!(decode_tid_timestamp_secs("not-a-tid").is_none());
        assert!(decode_tid_timestamp_secs("2222222222222").is_none());
    }
}
