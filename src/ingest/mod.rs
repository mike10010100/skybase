//! Jetstream firehose consumer, monotonic cursor tracking, event parsing, and resilience.
//!
//! This module provides a complete, resilient ingestion pipeline for Bluesky Jetstream firehose
//! feeds:
//! - [`consumer::JetstreamConsumer`]: Asynchronous WebSocket client with decoupled reader and SQLite sync tasks.
//! - [`cursor::CursorTracker`]: Lock-free atomic monotonic high-watermark cursor tracker.
//! - [`events::JetstreamEvent`]: Lenient deserialization of commit mutations, heartbeats, and account events.
//! - [`backoff::BackoffManager`]: Exponential reconnect backoff with ±20% integer jitter and frame-based reset.
//! - [`mock::MockJetstreamServer`]: Hermetic, offline loopback WebSocket server fixture for testing.

pub mod backoff;
pub mod consumer;
pub mod cursor;
pub mod events;
pub mod mock;

pub use backoff::{
    BackoffManager, DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_JITTER_PERCENT, DEFAULT_MAX_BACKOFF_SECS,
    MIN_BACKOFF_FLOOR_MS,
};
pub use consumer::{
    build_subscription_url, build_subscription_url_full, ConsumerStats, IngesterConfig,
    JetstreamConsumer, JetstreamConsumerHandle,
};
pub use cursor::CursorTracker;
pub use events::{
    normalize_indexed_at, parse_frame_timestamp, parse_jetstream_commit, parse_jetstream_frame,
    sync_commit_to_store, CommitOperation, JetstreamCommit, JetstreamEvent, RawJetstreamCommit,
    RawJetstreamMessage,
};
pub use mock::{MockJetstreamServer, MockServerCommand};
