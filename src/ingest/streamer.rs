//! Storage-agnostic Jetstream WebSocket streaming client for live firehose ingestion.
//!
//! Connects to public ATProto Jetstream firehose endpoints, subscribes to configured
//! collections and DIDs, handles automatic reconnection with exponential backoff,
//! keepalive pings, idle stall detection, and 5-second cursor rewind overlap,
//! and dispatches parsed [`JetstreamCommit`] frames over a bounded [`tokio::sync::mpsc::Sender`].

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::form_urlencoded;

use crate::error::Result;
use crate::ingest::backoff::BackoffManager;
use crate::ingest::cursor::CursorTracker;
use crate::ingest::events::{parse_jetstream_frame, JetstreamCommit, JetstreamEvent};

/// Default public Jetstream endpoint.
pub const DEFAULT_JETSTREAM_ENDPOINT: &str = "wss://jetstream2.us-east.bsky.network/subscribe";

/// Initial backoff delay on connection failure (500ms).
pub const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

/// Maximum backoff delay cap on repeated failures (30s).
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Default idle read timeout before treating the Jetstream WebSocket as stalled (30s).
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default periodic WebSocket keepalive ping interval (15s).
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum Jetstream replay retention window (72 hours in microseconds).
pub const MAX_JETSTREAM_REPLAY_WINDOW_US: u64 = 72 * 3600 * 1_000_000;

/// Safety overlap rewind applied when resuming from a persisted or reconnected cursor (5 seconds in microseconds).
pub const CURSOR_REWIND_OVERLAP_US: u64 = 5_000_000;

/// Resolves a safe Jetstream replay cursor from a persisted microsecond timestamp,
/// rewinding by [`CURSOR_REWIND_OVERLAP_US`] (5s) and clamping to the maximum
/// [`MAX_JETSTREAM_REPLAY_WINDOW_US`] (72h) retention window prior to `now_us`.
#[must_use]
pub fn resolve_resume_cursor(persisted_cursor_us: u64, now_us: u64) -> Option<u64> {
    if persisted_cursor_us == 0 {
        return None;
    }
    let rewound = persisted_cursor_us.saturating_sub(CURSOR_REWIND_OVERLAP_US);
    let min_allowed = now_us.saturating_sub(MAX_JETSTREAM_REPLAY_WINDOW_US);
    Some(rewound.max(min_allowed)).filter(|c| *c > 0)
}

/// Constructs a canonical Jetstream WebSocket subscription URL with query parameters.
///
/// Handles repeated `wantedCollections` and optional `cursor`. Preserves existing query
/// parameters on `base_url` without duplicating.
#[must_use]
pub fn build_subscription_url(
    base_url: &str,
    wanted_collections: &[String],
    cursor: Option<u64>,
) -> String {
    build_subscription_url_full(base_url, wanted_collections, &[], cursor)
}

/// Constructs a Jetstream WebSocket URL with collection, DID, and cursor filters.
#[must_use]
pub fn build_subscription_url_full(
    base_url: &str,
    wanted_collections: &[String],
    wanted_dids: &[String],
    cursor: Option<u64>,
) -> String {
    let mut url = base_url.trim().to_string();

    // Ensure valid scheme exists (case-insensitive)
    let lower = url.to_ascii_lowercase();
    if !lower.starts_with("ws://") && !lower.starts_with("wss://") {
        url = format!("wss://{url}");
    }

    // Ensure valid path separator exists before appending query parameters
    if let Some(scheme_idx) = url.find("://") {
        let after_scheme = &url[scheme_idx + 3..];
        if !after_scheme.contains('/') && !after_scheme.contains('?') {
            url.push('/');
        }
    } else if !url.contains('/') && !url.contains('?') {
        url.push('/');
    }

    let has_query = url.contains('?');
    let mut query_params: Vec<String> = Vec::new();

    // 1. Append wantedCollections if not already present in base URL
    if !url.contains("wantedCollections=") {
        for col in wanted_collections {
            let trimmed = col.trim();
            if !trimmed.is_empty() {
                let encoded: String = form_urlencoded::byte_serialize(trimmed.as_bytes()).collect();
                query_params.push(format!("wantedCollections={encoded}"));
            }
        }
    }

    // 2. Append wantedDids if not already present
    if !url.contains("wantedDids=") {
        for did in wanted_dids {
            let trimmed = did.trim();
            if !trimmed.is_empty() {
                let encoded: String = form_urlencoded::byte_serialize(trimmed.as_bytes()).collect();
                let safe_did = encoded.replace("%3A", ":");
                query_params.push(format!("wantedDids={safe_did}"));
            }
        }
    }

    // 3. Append cursor if non-zero and not already present
    if let Some(c) = cursor {
        if c > 0 && !url.contains("cursor=") {
            query_params.push(format!("cursor={c}"));
        }
    }

    // 4. Join and attach query parameters
    if !query_params.is_empty() {
        let separator = if has_query { '&' } else { '?' };
        if !url.ends_with('?') && !url.ends_with('&') {
            url.push(separator);
        }
        url.push_str(&query_params.join("&"));
    }

    url
}

/// Configuration for the storage-agnostic Jetstream stream listener.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// WebSocket endpoint URL.
    pub endpoint: String,
    /// Collections to subscribe to at the edge.
    pub collections: Vec<String>,
    /// Optional target repository DIDs to filter for at the edge.
    pub wanted_dids: Vec<String>,
    /// Initial cursor timestamp in microseconds (`time_us`), if replaying from a point in time.
    pub cursor: Option<u64>,
    /// Maximum idle duration between WebSocket frames before forcing a reconnect.
    pub read_timeout: Duration,
    /// Optional periodic WebSocket keepalive ping interval.
    pub ping_interval: Option<Duration>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_JETSTREAM_ENDPOINT.to_string(),
            collections: Vec::new(),
            wanted_dids: Vec::new(),
            cursor: None,
            read_timeout: DEFAULT_READ_TIMEOUT,
            ping_interval: Some(DEFAULT_PING_INTERVAL),
        }
    }
}

impl StreamConfig {
    /// Creates a new streaming configuration targeting `endpoint`.
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    /// Replaces the target collection NSIDs from an iterator.
    #[must_use]
    pub fn with_collections<I, S>(mut self, collections: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.collections = collections.into_iter().map(Into::into).collect();
        self
    }

    /// Appends a target collection NSID to subscribe to.
    #[must_use]
    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collections.push(collection.into());
        self
    }

    /// Appends a target DID to filter for at the edge.
    #[must_use]
    pub fn with_did(mut self, did: impl Into<String>) -> Self {
        self.wanted_dids.push(did.into());
        self
    }

    /// Sets the replay cursor timestamp in microseconds.
    #[must_use]
    pub fn with_cursor(mut self, cursor: u64) -> Self {
        self.cursor = if cursor > 0 { Some(cursor) } else { None };
        self
    }

    /// Sets the idle read timeout before reconnecting on a silent WebSocket stall.
    #[must_use]
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout.max(Duration::from_millis(50));
        self
    }

    /// Sets the periodic WebSocket keepalive ping interval.
    #[must_use]
    pub const fn with_ping_interval(mut self, interval: Option<Duration>) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Constructs the full WebSocket subscription URL with query parameters.
    #[must_use]
    pub fn build_url(&self) -> String {
        build_subscription_url_full(
            &self.endpoint,
            &self.collections,
            &self.wanted_dids,
            self.cursor.filter(|c| *c > 0),
        )
    }
}

/// Connects to Jetstream and forwards parsed [`JetstreamCommit`] frames to `tx` until `cancel` is triggered.
///
/// Automatically reconnects with exponential backoff on connection drops, sends periodic keepalive pings,
/// and rewinds the last seen monotonic cursor by [`CURSOR_REWIND_OVERLAP_US`] (5s) on reconnect to prevent
/// dropped events across transient disconnects.
///
/// # Errors
/// Returns [`crate::error::SkybaseError`] if unrecoverable stream errors occur.
pub async fn run_jetstream_streamer(
    config: StreamConfig,
    tx: mpsc::Sender<JetstreamCommit>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut backoff = BackoffManager::new(INITIAL_BACKOFF, MAX_BACKOFF);
    let cursor_tracker = CursorTracker::from_option(config.cursor);
    let mut has_connected_once = false;

    while !cancel.is_cancelled() {
        let mut active_config = config.clone();
        if let Some(cursor) = cursor_tracker.get_opt() {
            active_config.cursor = if has_connected_once {
                Some(cursor.saturating_sub(CURSOR_REWIND_OVERLAP_US))
            } else {
                Some(cursor)
            };
        }
        has_connected_once = true;
        let url = active_config.build_url();
        info!(endpoint = %url, cursor = ?active_config.cursor, "Connecting to Jetstream firehose");

        match connect_async(&url).await {
            Ok((ws_stream, response)) => {
                debug!(status = %response.status(), "WebSocket connection established");
                let (mut write_half, mut read_half) = ws_stream.split();
                let ping_dur = active_config
                    .ping_interval
                    .unwrap_or(Duration::from_secs(3600));
                let mut ping_timer = tokio::time::interval_at(Instant::now() + ping_dur, ping_dur);

                loop {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {
                            info!("Streamer received cancellation signal; closing connection");
                            let _ = write_half.send(Message::Close(None)).await;
                            return Ok(());
                        }
                        _ = ping_timer.tick(), if active_config.ping_interval.is_some() => {
                            if let Err(e) = write_half.send(Message::Ping(Vec::new().into())).await {
                                warn!(error = %e, "Failed to send WebSocket keepalive ping; reconnecting");
                                break;
                            }
                        }
                        msg_res = tokio::time::timeout(active_config.read_timeout, read_half.next()) => {
                            match msg_res {
                                Ok(Some(Ok(Message::Text(text)))) => {
                                    if let Some(evt) = parse_jetstream_frame(&text) {
                                        cursor_tracker.update(evt.time_us());
                                        backoff.reset();
                                        if let JetstreamEvent::Commit(commit) = evt {
                                            if tx.send(commit).await.is_err() {
                                                debug!("Commit receiver dropped; stopping streamer");
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                                Ok(Some(Ok(Message::Binary(bytes)))) => {
                                    if let Ok(text) = std::str::from_utf8(&bytes) {
                                        if let Some(evt) = parse_jetstream_frame(text) {
                                            cursor_tracker.update(evt.time_us());
                                            backoff.reset();
                                            if let JetstreamEvent::Commit(commit) = evt {
                                                if tx.send(commit).await.is_err() {
                                                    debug!("Commit receiver dropped; stopping streamer");
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(Some(Ok(Message::Ping(payload)))) => {
                                    let _ = write_half.send(Message::Pong(payload)).await;
                                }
                                Ok(Some(Ok(Message::Pong(_)))) => {}
                                Ok(Some(Ok(Message::Close(frame)))) => {
                                    warn!(frame = ?frame, "Jetstream server sent close frame; reconnecting");
                                    break;
                                }
                                Ok(Some(Ok(Message::Frame(_)))) => {}
                                Ok(Some(Err(e))) => {
                                    warn!(error = %e, "WebSocket read error; reconnecting");
                                    break;
                                }
                                Ok(None) => {
                                    warn!("Jetstream stream ended unexpectedly; reconnecting");
                                    break;
                                }
                                Err(_elapsed) => {
                                    warn!(
                                        timeout = ?active_config.read_timeout,
                                        cursor = ?cursor_tracker.get_opt(),
                                        "Jetstream WebSocket read timed out (silent stall); closing and reconnecting"
                                    );
                                    let _ = tokio::time::timeout(
                                        Duration::from_secs(1),
                                        write_half.send(Message::Close(None)),
                                    )
                                    .await;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Jetstream");
            }
        }

        let delay = backoff.next_backoff();
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Ok(());
            }
            () = tokio::time::sleep(delay) => {}
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_resume_cursor() {
        let now_us = 1_800_000_000_000_000_u64;
        assert_eq!(resolve_resume_cursor(0, now_us), None);

        let recent = now_us - 600_000_000;
        assert_eq!(
            resolve_resume_cursor(recent, now_us),
            Some(recent - CURSOR_REWIND_OVERLAP_US)
        );

        let stale = now_us - (10 * 24 * 3600 * 1_000_000);
        assert_eq!(
            resolve_resume_cursor(stale, now_us),
            Some(now_us - MAX_JETSTREAM_REPLAY_WINDOW_US)
        );
    }

    #[test]
    fn test_stream_config_url_building() {
        let cfg = StreamConfig::new("wss://example.test/subscribe")
            .with_collection("app.bsky.feed.post")
            .with_cursor(1_700_000_000_000_000);
        let url = cfg.build_url();
        assert!(url.contains("wantedCollections=app.bsky.feed.post"));
        assert!(url.contains("cursor=1700000000000000"));
    }
}
