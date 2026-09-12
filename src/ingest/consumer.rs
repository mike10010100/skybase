//! Jetstream WebSocket firehose consumer, supervisor, and SQLite synchronization loop.
//!
//! Provides a resilient, decoupled architecture connecting to Bluesky Jetstream firehoses,
//! filtering collection mutations at the edge, maintaining monotonic cursors, and synchronizing
//! commit records directly into embedded SQLite WAL storage.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, SkybaseError};
use crate::index::{RecordInput, RecordStore};
use crate::ingest::backoff::BackoffManager;
use crate::ingest::cursor::CursorTracker;
use crate::ingest::events::{
    normalize_indexed_at, parse_jetstream_frame, CommitOperation, JetstreamCommit, JetstreamEvent,
};

/// Configuration options for the Jetstream firehose consumer.
#[derive(Debug, Clone)]
pub struct IngesterConfig {
    /// Jetstream WebSocket endpoint URL.
    /// Default: `"wss://jetstream2.us-east.bsky.network/subscribe"`.
    pub endpoint: String,

    /// Target ATProto collection NSIDs to subscribe to at the edge.
    /// If empty, subscribes to all collections emitted by the endpoint.
    /// Example: `vec!["app.bsky.feed.post".into(), "app.bsky.feed.like".into()]`.
    pub wanted_collections: Vec<String>,

    /// Optional target repository DIDs to filter for at the edge.
    pub wanted_dids: Vec<String>,

    /// Initial cursor timestamp in microseconds (`time_us`).
    /// If `None` or `0`, ingestion starts live from the current server stream.
    pub initial_cursor: Option<u64>,

    /// Bounded capacity for the internal MPSC channel between reader and storage sync.
    /// Default: `1024`.
    pub channel_capacity: usize,

    /// Inactivity watchdog duration. If no frames are received within this window,
    /// the connection is deemed dead/stalled and proactively closed to trigger reconnect.
    /// Default: `Duration::from_secs(30)`.
    pub inactivity_timeout: Duration,

    /// Periodic WebSocket keepalive ping interval.
    /// Default: `Some(Duration::from_secs(15))`.
    pub ping_interval: Option<Duration>,

    /// Initial reconnect backoff delay.
    /// Default: `Duration::from_millis(500)`.
    pub initial_backoff: Duration,

    /// Maximum reconnect backoff delay cap.
    /// Default: `Duration::from_secs(30)`.
    pub max_backoff: Duration,

    /// Optional batch size for SQLite transaction grouping during storage sync.
    /// Default: `1` (immediate sync per commit).
    pub batch_size: usize,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        Self {
            endpoint: "wss://jetstream2.us-east.bsky.network/subscribe".to_string(),
            wanted_collections: Vec::new(),
            wanted_dids: Vec::new(),
            initial_cursor: None,
            channel_capacity: 1024,
            inactivity_timeout: Duration::from_secs(30),
            ping_interval: Some(Duration::from_secs(15)),
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            batch_size: 1,
        }
    }
}

impl IngesterConfig {
    /// Creates a configuration targeting the specified endpoint URL.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    /// Appends a target collection NSID to filter for at the edge.
    #[must_use]
    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.wanted_collections.push(collection.into());
        self
    }

    /// Sets target collections from an iterator.
    #[must_use]
    pub fn with_collections<I, S>(mut self, collections: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.wanted_collections = collections.into_iter().map(Into::into).collect();
        self
    }

    /// Appends a target DID to filter for at the edge.
    #[must_use]
    pub fn with_did(mut self, did: impl Into<String>) -> Self {
        self.wanted_dids.push(did.into());
        self
    }

    /// Sets target DIDs from an iterator.
    #[must_use]
    pub fn with_dids<I, S>(mut self, dids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.wanted_dids = dids.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the initial replay cursor in microseconds.
    #[must_use]
    pub fn with_cursor(mut self, cursor: u64) -> Self {
        self.initial_cursor = if cursor > 0 { Some(cursor) } else { None };
        self
    }

    /// Sets the bounded MPSC channel capacity.
    #[must_use]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(16);
        self
    }

    /// Sets the inactivity watchdog duration.
    #[must_use]
    pub fn with_inactivity_timeout(mut self, timeout: Duration) -> Self {
        self.inactivity_timeout = timeout;
        self
    }

    /// Sets the WebSocket keepalive ping interval.
    #[must_use]
    pub fn with_ping_interval(mut self, interval: Option<Duration>) -> Self {
        self.ping_interval = interval;
        self
    }

    /// Sets the exponential reconnect backoff parameters.
    #[must_use]
    pub fn with_backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_backoff = initial;
        self.max_backoff = max;
        self
    }

    /// Sets the SQLite storage sync batch size.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }
}

/// Operational metrics and telemetry counters for [`JetstreamConsumer`].
#[derive(Debug, Default)]
pub struct ConsumerStats {
    /// Total raw WebSocket frames received.
    pub frames_received: AtomicU64,
    /// Total bytes received over WebSocket connections.
    pub bytes_received: AtomicU64,
    /// Total commit records upserted into SQLite storage.
    pub records_upserted: AtomicU64,
    /// Total records marked deleted in SQLite storage.
    pub records_deleted: AtomicU64,
    /// Total storage synchronization errors encountered.
    pub sync_errors: AtomicU64,
    /// Total reconnection events triggered (watchdog timeout, network drop, close frame).
    pub reconnect_count: AtomicU64,
    /// Monotonic unix timestamp (seconds) of the most recently received frame.
    pub last_activity_timestamp: AtomicU64,
}

impl ConsumerStats {
    /// Returns the total number of frames received.
    #[must_use]
    pub fn frames_received(&self) -> u64 {
        self.frames_received.load(Ordering::Relaxed)
    }

    /// Returns the total bytes received.
    #[must_use]
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    /// Returns the total number of upserts successfully processed.
    #[must_use]
    pub fn records_upserted(&self) -> u64 {
        self.records_upserted.load(Ordering::Relaxed)
    }

    /// Returns the total number of deletions processed.
    #[must_use]
    pub fn records_deleted(&self) -> u64 {
        self.records_deleted.load(Ordering::Relaxed)
    }

    /// Returns the total number of sync failures.
    #[must_use]
    pub fn sync_errors(&self) -> u64 {
        self.sync_errors.load(Ordering::Relaxed)
    }

    /// Returns the total number of reconnections.
    #[must_use]
    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count.load(Ordering::Relaxed)
    }

    /// Returns the timestamp in seconds of the last received frame.
    #[must_use]
    pub fn last_activity_timestamp(&self) -> u64 {
        self.last_activity_timestamp.load(Ordering::Relaxed)
    }
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

    // Ensure valid scheme exists
    if !url.starts_with("ws://") && !url.starts_with("wss://") {
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
            if !col.trim().is_empty() {
                query_params.push(format!("wantedCollections={col}"));
            }
        }
    }

    // 2. Append wantedDids if not already present
    if !url.contains("wantedDids=") {
        for did in wanted_dids {
            if !did.trim().is_empty() {
                query_params.push(format!("wantedDids={did}"));
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

/// High-performance ATProto Jetstream firehose consumer.
#[derive(Clone)]
pub struct JetstreamConsumer {
    config: IngesterConfig,
    store: RecordStore,
    cursor: Arc<CursorTracker>,
    stats: Arc<ConsumerStats>,
}

impl JetstreamConsumer {
    /// Creates a new [`JetstreamConsumer`] bound to the given configuration and SQLite store.
    pub fn new(config: IngesterConfig, store: RecordStore) -> Self {
        let initial_cursor = config.initial_cursor.unwrap_or(0);
        Self {
            config,
            store,
            cursor: Arc::new(CursorTracker::new(initial_cursor)),
            stats: Arc::new(ConsumerStats::default()),
        }
    }

    /// Overrides the cursor tracker with an existing shared [`CursorTracker`].
    #[must_use]
    pub fn with_cursor_tracker(mut self, cursor: Arc<CursorTracker>) -> Self {
        self.cursor = cursor;
        self
    }

    /// Returns a reference to the active configuration.
    #[must_use]
    pub fn config(&self) -> &IngesterConfig {
        &self.config
    }

    /// Returns the current high-watermark cursor timestamp in microseconds.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.cursor.get()
    }

    /// Returns a clone of the shared [`CursorTracker`].
    #[must_use]
    pub fn cursor_tracker(&self) -> Arc<CursorTracker> {
        Arc::clone(&self.cursor)
    }

    /// Returns a clone of the shared [`ConsumerStats`].
    #[must_use]
    pub fn stats(&self) -> Arc<ConsumerStats> {
        Arc::clone(&self.stats)
    }

    /// Runs the Jetstream consumer loop to completion or until cancelled.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if an unrecoverable internal error occurs.
    pub async fn run(&self, cancel: CancellationToken) -> Result<()> {
        if cancel.is_cancelled() {
            return Ok(());
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<JetstreamCommit>(self.config.channel_capacity);
        let mut join_set = tokio::task::JoinSet::new();

        // 1. Spawn Storage Sync Worker Task
        let worker_store = self.store.clone();
        let worker_stats = Arc::clone(&self.stats);
        let worker_token = cancel.child_token();
        let batch_size = self.config.batch_size;

        join_set.spawn(async move {
            run_storage_sync_worker(rx, worker_store, worker_stats, batch_size, worker_token).await;
            Ok(())
        });

        // 2. Spawn WebSocket Reader Reconnect Loop Task
        let reader_config = self.config.clone();
        let reader_cursor = Arc::clone(&self.cursor);
        let reader_stats = Arc::clone(&self.stats);
        let reader_token = cancel.child_token();

        join_set.spawn(async move {
            run_reader_reconnect_loop(reader_config, tx, reader_cursor, reader_stats, reader_token)
                .await
        });

        // 3. Supervise tasks
        while let Some(join_res) = join_set.join_next().await {
            match join_res {
                Ok(task_res) => task_res?,
                Err(join_err) => {
                    if !join_err.is_cancelled() {
                        return Err(SkybaseError::Event(format!(
                            "Ingestion task failed: {join_err}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Spawns the consumer as a managed background task tied to the cancellation token.
    pub fn start(&self, cancel: CancellationToken) -> JetstreamConsumerHandle {
        let this = self.clone();
        let cancel_child = cancel.child_token();
        let join_handle = tokio::spawn(async move { this.run(cancel_child).await });
        JetstreamConsumerHandle {
            join_handle,
            cancel,
        }
    }
}

/// A running background task handle for [`JetstreamConsumer`].
pub struct JetstreamConsumerHandle {
    join_handle: tokio::task::JoinHandle<Result<()>>,
    cancel: CancellationToken,
}

impl JetstreamConsumerHandle {
    /// Signals the consumer to shut down cleanly.
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    /// Awaits completion of the background consumer task.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if the background task encountered a fatal error.
    pub async fn join(self) -> Result<()> {
        match self.join_handle.await {
            Ok(res) => res,
            Err(join_err) => {
                if join_err.is_cancelled() {
                    Ok(())
                } else {
                    Err(SkybaseError::Event(format!(
                        "Consumer task join error: {join_err}"
                    )))
                }
            }
        }
    }
}

async fn run_reader_reconnect_loop(
    config: IngesterConfig,
    tx: tokio::sync::mpsc::Sender<JetstreamCommit>,
    cursor: Arc<CursorTracker>,
    stats: Arc<ConsumerStats>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut backoff = BackoffManager::new(config.initial_backoff, config.max_backoff);

    while !cancel.is_cancelled() {
        let current_cursor = cursor.get_opt();
        let url = build_subscription_url_full(
            &config.endpoint,
            &config.wanted_collections,
            &config.wanted_dids,
            current_cursor,
        );

        tracing::debug!(endpoint = %url, "Attempting connection to Jetstream firehose");

        let connect_res = tokio::select! {
            () = cancel.cancelled() => break,
            res = tokio_tungstenite::connect_async(&url) => res,
        };

        let (mut ws_stream, _) = match connect_res {
            Ok(pair) => pair,
            Err(err) => {
                stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_backoff();
                tracing::warn!(
                    error = %err,
                    retry_in = ?delay,
                    "Failed to connect to Jetstream. Retrying."
                );
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(delay) => continue,
                }
            }
        };

        tracing::info!(endpoint = %url, "Connected to Jetstream firehose");

        let mut last_activity = Instant::now();
        let mut ping_interval = config.ping_interval.map(tokio::time::interval);

        loop {
            let timeout_duration = config.inactivity_timeout;
            let sleep_watchdog = tokio::time::sleep_until(last_activity + timeout_duration);

            tokio::select! {
                () = cancel.cancelled() => {
                    let _ = ws_stream.close(Some(CloseFrame {
                        code: CloseCode::Normal,
                        reason: "Graceful shutdown".into(),
                    })).await;
                    return Ok(());
                }
                () = sleep_watchdog => {
                    stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "Inactivity watchdog triggered ({timeout_duration:?} elapsed without frames). Reconnecting."
                    );
                    let _ = ws_stream.close(None).await;
                    break;
                }
                _ = async {
                    if let Some(ref mut interval) = ping_interval {
                        interval.tick().await
                    } else {
                        futures_util::future::pending().await
                    }
                } => {
                    if ws_stream.send(Message::Ping(Vec::new())).await.is_err() {
                        stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("Failed to send keepalive ping; reconnecting.");
                        break;
                    }
                }
                msg_opt = ws_stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => {
                            last_activity = Instant::now();
                            let now_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map_or(0, |d| d.as_secs());
                            stats.last_activity_timestamp.store(now_secs, Ordering::Relaxed);

                            match msg {
                                Message::Text(text) => {
                                    stats.frames_received.fetch_add(1, Ordering::Relaxed);
                                    stats.bytes_received.fetch_add(text.len() as u64, Ordering::Relaxed);

                                    if let Some(event) = parse_jetstream_frame(&text) {
                                        // Reset backoff on any valid frame received
                                        backoff.reset();
                                        cursor.update(event.time_us());

                                        if let JetstreamEvent::Commit(commit) = event {
                                            // Edge filtering verification: ensure commit matches wanted collections
                                            let matches_collection = config.wanted_collections.is_empty()
                                                || config.wanted_collections.iter().any(|c| c == &commit.collection);

                                            let matches_did = config.wanted_dids.is_empty()
                                                || config.wanted_dids.iter().any(|d| d == &commit.did);

                                            if matches_collection
                                                && matches_did
                                                && tx.send(commit).await.is_err()
                                            {
                                                // Storage worker terminated
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                                Message::Binary(bin) => {
                                    stats.frames_received.fetch_add(1, Ordering::Relaxed);
                                    stats.bytes_received.fetch_add(bin.len() as u64, Ordering::Relaxed);
                                }
                                Message::Ping(data) => {
                                    let _ = ws_stream.send(Message::Pong(data)).await;
                                }
                                Message::Pong(_) => {}
                                Message::Close(_) => {
                                    stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                                    tracing::debug!("WebSocket close frame received; reconnecting");
                                    break;
                                }
                                Message::Frame(_) => {}
                            }
                        }
                        Some(Err(err)) => {
                            stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!("WebSocket error: {err}. Reconnecting.");
                            break;
                        }
                        None => {
                            stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!("WebSocket connection closed by remote peer. Reconnecting.");
                            break;
                        }
                    }
                }
            }
        }

        // Connection dropped; apply backoff delay before reconnecting
        if !cancel.is_cancelled() {
            let delay = backoff.next_backoff();
            tracing::info!(retry_in = ?delay, "Reconnecting to Jetstream firehose after backoff");
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(delay) => continue,
            }
        }
    }

    Ok(())
}

async fn run_storage_sync_worker(
    mut rx: tokio::sync::mpsc::Receiver<JetstreamCommit>,
    store: RecordStore,
    stats: Arc<ConsumerStats>,
    batch_size: usize,
    cancel: CancellationToken,
) {
    if batch_size <= 1 {
        // Immediate single-record sync mode
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    while let Ok(commit) = rx.try_recv() {
                        apply_single_commit(&commit, &store, &stats);
                    }
                    break;
                }
                commit_opt = rx.recv() => {
                    match commit_opt {
                        Some(commit) => apply_single_commit(&commit, &store, &stats),
                        None => break,
                    }
                }
            }
        }
    } else {
        // Batched sync mode
        let mut buffer = Vec::with_capacity(batch_size);
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    while let Ok(commit) = rx.try_recv() {
                        buffer.push(commit);
                    }
                    flush_commit_batch(&mut buffer, &store, &stats);
                    break;
                }
                count = rx.recv_many(&mut buffer, batch_size) => {
                    if count == 0 {
                        flush_commit_batch(&mut buffer, &store, &stats);
                        break;
                    }
                    flush_commit_batch(&mut buffer, &store, &stats);
                }
            }
        }
    }
}

fn apply_single_commit(commit: &JetstreamCommit, store: &RecordStore, stats: &ConsumerStats) {
    let uri = commit.uri();
    match commit.operation {
        CommitOperation::Create | CommitOperation::Update => {
            let input = RecordInput {
                uri: uri.clone(),
                cid: commit.cid.clone().unwrap_or_default(),
                did: commit.did.clone(),
                collection: commit.collection.clone(),
                rkey: commit.rkey.clone(),
                record_json: commit
                    .record
                    .clone()
                    .unwrap_or_else(|| serde_json::Value::Object(Default::default())),
                indexed_at: normalize_indexed_at(commit.time_us),
            };
            match store.upsert_record(&input) {
                Ok(()) => {
                    stats.records_upserted.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    stats.sync_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(uri = %input.uri, "Failed to upsert record: {err}");
                }
            }
        }
        CommitOperation::Delete => match store.soft_delete_record(&uri) {
            Ok(()) => {
                stats.records_deleted.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                stats.sync_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(uri = %uri, "Failed to soft delete record: {err}");
            }
        },
    }
}

fn flush_commit_batch(
    buffer: &mut Vec<JetstreamCommit>,
    store: &RecordStore,
    stats: &ConsumerStats,
) {
    for commit in buffer.drain(..) {
        apply_single_commit(&commit, store, stats);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_build_subscription_url_plain() {
        let url = build_subscription_url("ws://127.0.0.1:8080/subscribe", &[], None);
        assert_eq!(url, "ws://127.0.0.1:8080/subscribe");
    }

    #[test]
    fn test_build_subscription_url_with_collections_and_cursor() {
        let cols = vec![
            "app.bsky.feed.post".to_string(),
            "com.example.review".to_string(),
        ];
        let url = build_subscription_url(
            "ws://127.0.0.1:8080/subscribe",
            &cols,
            Some(1700000000000000),
        );
        assert_eq!(
            url,
            "ws://127.0.0.1:8080/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=com.example.review&cursor=1700000000000000"
        );
    }

    #[test]
    fn test_build_subscription_url_preserves_existing_query() {
        let cols = vec!["app.bsky.feed.post".to_string()];
        let url = build_subscription_url(
            "wss://jetstream.example.com/subscribe?compress=true",
            &cols,
            Some(12345),
        );
        assert_eq!(
            url,
            "wss://jetstream.example.com/subscribe?compress=true&wantedCollections=app.bsky.feed.post&cursor=12345"
        );
    }

    #[test]
    fn test_build_subscription_url_zero_cursor_omitted() {
        let cols = vec!["app.bsky.feed.post".to_string()];
        let url = build_subscription_url("ws://127.0.0.1:8080/subscribe", &cols, Some(0));
        assert_eq!(
            url,
            "ws://127.0.0.1:8080/subscribe?wantedCollections=app.bsky.feed.post"
        );
    }

    #[test]
    fn test_ingester_config_builder() {
        let config = IngesterConfig::new("ws://127.0.0.1:9000/subscribe")
            .with_collection("app.bsky.feed.post")
            .with_did("did:plc:alice")
            .with_cursor(1000)
            .with_channel_capacity(512)
            .with_inactivity_timeout(Duration::from_secs(45))
            .with_batch_size(10);

        assert_eq!(config.endpoint, "ws://127.0.0.1:9000/subscribe");
        assert_eq!(config.wanted_collections, vec!["app.bsky.feed.post"]);
        assert_eq!(config.wanted_dids, vec!["did:plc:alice"]);
        assert_eq!(config.initial_cursor, Some(1000));
        assert_eq!(config.channel_capacity, 512);
        assert_eq!(config.inactivity_timeout, Duration::from_secs(45));
        assert_eq!(config.batch_size, 10);
    }

    #[tokio::test]
    async fn test_consumer_stats_telemetry() {
        let stats = ConsumerStats::default();
        assert_eq!(stats.frames_received(), 0);
        assert_eq!(stats.records_upserted(), 0);

        stats.frames_received.fetch_add(5, Ordering::Relaxed);
        stats.records_upserted.fetch_add(3, Ordering::Relaxed);

        assert_eq!(stats.frames_received(), 5);
        assert_eq!(stats.records_upserted(), 3);
    }

    #[tokio::test]
    async fn test_jetstream_consumer_end_to_end_flow() {
        use crate::index::ChangeNotification;
        use crate::ingest::mock::MockJetstreamServer;
        use serde_json::json;

        let server = MockJetstreamServer::start()
            .await
            .expect("start mock server");
        let store = RecordStore::open_in_memory().expect("open store");
        let mut bus = store.subscribe();

        let config = IngesterConfig::new(server.ws_url())
            .with_collection("app.bsky.feed.post")
            .with_inactivity_timeout(Duration::from_secs(5));

        let consumer = JetstreamConsumer::new(config, store.clone());
        let cancel = CancellationToken::new();
        let handle = consumer.start(cancel.clone());

        tokio::time::sleep(Duration::from_millis(60)).await;

        let commit = JetstreamCommit {
            did: "did:plc:consumer_test".to_string(),
            time_us: 1_715_000_000_000_000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "post_1".to_string(),
            operation: CommitOperation::Create,
            cid: Some("bafyrei_test_cid".to_string()),
            record: Some(json!({ "text": "Consumer e2e test post" })),
        };

        server.emit_commit(&commit).expect("emit commit");

        let notification = tokio::time::timeout(Duration::from_secs(2), bus.recv())
            .await
            .expect("timeout waiting for bus notification")
            .expect("notification error");

        match notification {
            ChangeNotification::Upsert(row) => {
                assert_eq!(
                    row.uri,
                    "at://did:plc:consumer_test/app.bsky.feed.post/post_1"
                );
                assert_eq!(row.cid, "bafyrei_test_cid");
                assert_eq!(row.record_json["text"], "Consumer e2e test post");
            }
            _ => panic!("Expected ChangeNotification::Upsert"),
        }

        // Verify stored in SQLite
        let stored = store
            .get_record("at://did:plc:consumer_test/app.bsky.feed.post/post_1")
            .expect("get_record")
            .expect("record found");
        assert_eq!(stored.cid, "bafyrei_test_cid");

        // Verify cursor updated
        assert_eq!(consumer.cursor(), 1_715_000_000_000_000);
        assert_eq!(consumer.stats().records_upserted(), 1);

        // Stop consumer cleanly
        handle.stop();
        handle.join().await.expect("join failed");
    }

    #[tokio::test]
    async fn test_jetstream_consumer_filtering_ignores_unwanted_collections() {
        use crate::ingest::mock::MockJetstreamServer;
        use serde_json::json;

        let server = MockJetstreamServer::start()
            .await
            .expect("start mock server");
        let store = RecordStore::open_in_memory().expect("open store");

        let config = IngesterConfig::new(server.ws_url())
            .with_collection("app.bsky.feed.post")
            .with_inactivity_timeout(Duration::from_secs(5));

        let consumer = JetstreamConsumer::new(config, store.clone());
        let cancel = CancellationToken::new();
        let handle = consumer.start(cancel.clone());

        tokio::time::sleep(Duration::from_millis(60)).await;

        let ignored_commit = JetstreamCommit {
            did: "did:plc:other_user".to_string(),
            time_us: 1_715_000_000_100_000,
            collection: "unwanted.collection".to_string(),
            rkey: "rkey_ignored".to_string(),
            operation: CommitOperation::Create,
            cid: Some("cid_ignored".to_string()),
            record: Some(json!({ "text": "ignored" })),
        };

        server
            .emit_commit(&ignored_commit)
            .expect("emit ignored commit");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let stored = store
            .get_record("at://did:plc:other_user/unwanted.collection/rkey_ignored")
            .expect("get_record");
        assert_eq!(stored, None);

        handle.stop();
        handle.join().await.expect("clean join");
    }

    #[tokio::test]
    async fn test_jetstream_consumer_graceful_shutdown() {
        use crate::ingest::mock::MockJetstreamServer;

        let server = MockJetstreamServer::start()
            .await
            .expect("start mock server");
        let store = RecordStore::open_in_memory().expect("open store");

        let config = IngesterConfig::new(server.ws_url());
        let consumer = JetstreamConsumer::new(config, store);
        let cancel = CancellationToken::new();
        let handle = consumer.start(cancel.clone());

        tokio::time::sleep(Duration::from_millis(40)).await;
        handle.stop();
        let join_res = handle.join().await;
        assert!(join_res.is_ok());
    }

    #[tokio::test]
    async fn test_jetstream_consumer_inactivity_watchdog() {
        use crate::ingest::mock::MockJetstreamServer;

        let server = MockJetstreamServer::start()
            .await
            .expect("start mock server");
        let store = RecordStore::open_in_memory().expect("open store");

        // Set an aggressive inactivity timeout of 80ms and no pings
        let config = IngesterConfig::new(server.ws_url())
            .with_inactivity_timeout(Duration::from_millis(80))
            .with_ping_interval(None)
            .with_backoff(Duration::from_millis(50), Duration::from_millis(100));

        let consumer = JetstreamConsumer::new(config, store);
        let cancel = CancellationToken::new();
        let handle = consumer.start(cancel.clone());

        // Wait 250ms with zero frames emitted by mock server
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert!(
            consumer.stats().reconnect_count() >= 1,
            "Watchdog should have triggered at least one reconnect"
        );

        handle.stop();
        handle.join().await.expect("clean join");
    }
}
