//! Empirical Challenger Stress Tests for `skybase::ingest` (Milestone 2 - Challenger 1).
//!
//! Adversarially stress-tests:
//! 1. High-throughput commit streaming (thousands of commits) into in-memory and file-backed SQLite WAL.
//! 2. Abrupt socket aborts (`disconnect_all`), backoff progression, and seamless cursor resumption.
//! 3. Repeated network flapping / disconnect storms and recovery.
//! 4. Inactivity watchdog triggering on silent/stalled connections.
//! 5. At-least-once replay deduplication and idempotent upserts.
//! 6. Out-of-order commit timestamps and clock skew monotonicity.
//! 7. High-throughput edge collection and DID filtering.
//! 8. Rapid CRUD mutation lifecycles and tombstone resurrection under live firehose.
//! 9. Malformed and corrupted frame injection resilience.
//! 10. Clean cancellation and task teardown under heavy streaming load.
//! 11. Backoff jitter mathematical bounds and entropy distribution.
//! 12. SQLite concurrency under simultaneous reader queries and firehose ingest.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use skybase::index::{QueryOp, RecordStore, SortDirection};
use skybase::ingest::{
    BackoffManager, CommitOperation, IngesterConfig, JetstreamCommit, JetstreamConsumer,
    MockJetstreamServer,
};

// ============================================================================
// 1. High-Throughput Burst Streaming & SQLite Storage Verification
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_high_throughput_burst_streaming_in_memory() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open in-memory store");
    let mut bus = store.subscribe();

    let total_commits = 2000;
    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_channel_capacity(2048)
        .with_batch_size(1)
        .with_inactivity_timeout(Duration::from_secs(10));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    // Give consumer time to establish connection
    tokio::time::sleep(Duration::from_millis(80)).await;

    let base_time_us = 1_710_000_000_000_000u64;

    // Stream commits in bursts of 50 with small micro-yields to prevent mock server buffer saturation
    for batch_idx in 0..(total_commits / 50) {
        for i in 0..50 {
            let idx = batch_idx * 50 + i;
            let time_us = base_time_us + idx as u64 * 1000;
            let commit = JetstreamCommit {
                did: format!("did:plc:user_{}", idx % 20),
                time_us,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("post_{idx:05}"),
                operation: CommitOperation::Create,
                cid: Some(format!("bafyrei_{idx:05}")),
                record: Some(json!({
                    "text": format!("High throughput burst post {idx}"),
                    "seq": idx,
                    "batch": batch_idx,
                })),
            };
            server.emit_commit(&commit).expect("emit commit");
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // Wait for all commits to be upserted
    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < total_commits as u64 {
        if start_wait.elapsed() > Duration::from_secs(10) {
            panic!(
                "Timed out waiting for ingest. Upserted {}/{}",
                consumer.stats().records_upserted(),
                total_commits
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), total_commits as u64);
    assert_eq!(consumer.stats().sync_errors(), 0);

    // Verify cursor reached the final timestamp
    let expected_final_time = base_time_us + (total_commits - 1) as u64 * 1000;
    assert_eq!(consumer.cursor(), expected_final_time);

    // Spot-check records in RecordStore
    let sample_indices = [0, 1, 49, 50, 500, 1000, 1500, 1999];
    for idx in sample_indices {
        let uri = format!(
            "at://did:plc:user_{}/app.bsky.feed.post/post_{idx:05}",
            idx % 20
        );
        let record = store
            .get_record(&uri)
            .expect("get_record query")
            .unwrap_or_else(|| panic!("Missing record for index {idx} at {uri}"));
        assert_eq!(record.cid, format!("bafyrei_{idx:05}"));
        assert_eq!(record.record_json["seq"], idx);
    }

    // Query builder verification on total count
    let all_posts = store
        .query("app.bsky.feed.post")
        .limit(total_commits as u32 + 100)
        .execute()
        .expect("query all posts");
    assert_eq!(all_posts.len(), total_commits);

    // Verify broadcast bus received notifications (or lagged gracefully)
    let mut received_bus_events = 0;
    let mut lagged_events = 0;
    loop {
        match bus.try_recv() {
            Ok(_) => received_bus_events += 1,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                lagged_events += skipped;
            }
            Err(_) => break,
        }
    }
    assert!(
        received_bus_events as u64 + lagged_events > 0,
        "Broadcast bus should have delivered or lagged events (received: {received_bus_events}, lagged: {lagged_events})"
    );

    handle.stop();
    handle.join().await.expect("clean consumer join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_high_throughput_file_backed_wal_with_batching() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("ingest_stress_wal.db");

    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open(&db_path).expect("open file-backed store");

    let total_commits = 1500;
    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_channel_capacity(1024)
        .with_batch_size(50)
        .with_inactivity_timeout(Duration::from_secs(10));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(80)).await;

    let base_time_us = 1_720_000_000_000_000u64;

    for batch_idx in 0..(total_commits / 50) {
        for i in 0..50 {
            let idx = batch_idx * 50 + i;
            let time_us = base_time_us + idx as u64 * 1000;
            let commit = JetstreamCommit {
                did: format!("did:plc:wal_user_{}", idx % 10),
                time_us,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("wal_post_{idx:05}"),
                operation: CommitOperation::Create,
                cid: Some(format!("bafyrei_wal_{idx:05}")),
                record: Some(json!({
                    "title": format!("WAL post {idx}"),
                    "count": idx,
                    "active": true,
                })),
            };
            server.emit_commit(&commit).expect("emit commit");
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < total_commits as u64 {
        if start_wait.elapsed() > Duration::from_secs(10) {
            panic!(
                "Timed out waiting for WAL ingest. Upserted {}/{}",
                consumer.stats().records_upserted(),
                total_commits
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), total_commits as u64);
    assert_eq!(consumer.stats().sync_errors(), 0);

    // Verify stored count via JSON1 query
    let posts = store
        .query("app.bsky.feed.post")
        .where_json("active", QueryOp::Eq, json!(true))
        .limit(total_commits as u32 + 100)
        .execute()
        .expect("query posts");
    assert_eq!(posts.len(), total_commits);

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 2. Connection Disruption, Socket Aborts & Cursor Resumption
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_abrupt_socket_disconnect_and_cursor_resumption() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open store");

    // Fast backoff for testing: 50ms..150ms
    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_backoff(Duration::from_millis(50), Duration::from_millis(150))
        .with_inactivity_timeout(Duration::from_secs(5));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(80)).await;

    let base_time_us = 1_730_000_000_000_000u64;

    // 1. Emit Phase 1 commits (0..300)
    for i in 0..300 {
        let commit = JetstreamCommit {
            did: "did:plc:disconnect_test".to_string(),
            time_us: base_time_us + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("rkey_{i:04}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_p1_{i:04}")),
            record: Some(json!({ "val": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
        if i % 50 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    // Wait for all 300 to be upserted
    let start_p1 = Instant::now();
    while consumer.stats().records_upserted() < 300 {
        if start_p1.elapsed() > Duration::from_secs(4) {
            panic!("Timed out waiting for Phase 1 ingest");
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    let p1_cursor = consumer.cursor();
    assert_eq!(p1_cursor, base_time_us + 299 * 1000);

    // 2. Abruptly sever the connection (TCP RST simulation)
    server.disconnect_all().expect("disconnect_all failed");

    // Wait for reconnection to take place
    let start_recon = Instant::now();
    while consumer.stats().reconnect_count() == 0 {
        if start_recon.elapsed() > Duration::from_secs(3) {
            panic!("Consumer did not detect disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for consumer to establish new WebSocket session
    while server.active_connections() == 0 {
        if start_recon.elapsed() > Duration::from_secs(3) {
            panic!("Consumer did not reconnect to mock server");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify that the new handshake query included the cursor from Phase 1!
    let queries = server.query_history();
    assert!(
        queries.len() >= 2,
        "Should have recorded at least 2 connection handshakes, found: {:?}",
        queries
    );
    let latest_query = queries.last().unwrap();
    assert!(
        latest_query.contains(&format!("cursor={p1_cursor}")),
        "Latest subscription URL must include cursor={p1_cursor}, got: {latest_query}"
    );

    // 3. Emit Phase 2 commits (300..600) resuming from cursor
    for i in 300..600 {
        let commit = JetstreamCommit {
            did: "did:plc:disconnect_test".to_string(),
            time_us: base_time_us + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("rkey_{i:04}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_p2_{i:04}")),
            record: Some(json!({ "val": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
        if i % 50 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    // Wait for all 600 records to be upserted
    let start_p2 = Instant::now();
    while consumer.stats().records_upserted() < 600 {
        if start_p2.elapsed() > Duration::from_secs(4) {
            panic!(
                "Timed out waiting for Phase 2 ingest. Current: {}",
                consumer.stats().records_upserted()
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), 600);
    assert_eq!(consumer.cursor(), base_time_us + 599 * 1000);

    // Verify all 600 records are intact in SQLite
    let all_records = store
        .query("app.bsky.feed.post")
        .limit(1000)
        .execute()
        .expect("query records");
    assert_eq!(all_records.len(), 600);

    handle.stop();
    handle.join().await.expect("clean join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_repeated_disconnect_storm_flapping_network() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open store");

    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_backoff(Duration::from_millis(50), Duration::from_millis(100))
        .with_inactivity_timeout(Duration::from_secs(5));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(60)).await;

    let total_flaps = 6;
    let records_per_flap = 20;

    for flap_idx in 0..total_flaps {
        // Stream small burst
        for i in 0..records_per_flap {
            let seq = flap_idx * records_per_flap + i;
            let commit = JetstreamCommit {
                did: "did:plc:flapping_user".to_string(),
                time_us: 1_740_000_000_000_000 + seq as u64 * 1000,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("flap_post_{seq:04}"),
                operation: CommitOperation::Create,
                cid: Some(format!("cid_flap_{seq}")),
                record: Some(json!({ "flap": flap_idx, "seq": seq })),
            };
            server.emit_commit(&commit).expect("emit commit");
        }

        // Wait a short moment for commits to process
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Force connection drop
        server.disconnect_all().expect("disconnect");

        // Wait for reconnect
        tokio::time::sleep(Duration::from_millis(90)).await;
    }

    // After storm, stream final stable batch
    let final_batch_size = 50;
    let base_seq = total_flaps * records_per_flap;
    for i in 0..final_batch_size {
        let seq = base_seq + i;
        let commit = JetstreamCommit {
            did: "did:plc:flapping_user".to_string(),
            time_us: 1_740_000_000_000_000 + seq as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("flap_post_{seq:04}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_flap_{seq}")),
            record: Some(json!({ "flap": "final", "seq": seq })),
        };
        server.emit_commit(&commit).expect("emit final batch");
    }

    let expected_total = (total_flaps * records_per_flap + final_batch_size) as u64;
    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < expected_total {
        if start_wait.elapsed() > Duration::from_secs(5) {
            panic!(
                "Timed out waiting for flapping ingest to settle. Current: {}/{}",
                consumer.stats().records_upserted(),
                expected_total
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), expected_total);
    assert!(
        consumer.stats().reconnect_count() >= total_flaps as u64,
        "Reconnect count ({}) should be at least flaps ({total_flaps})",
        consumer.stats().reconnect_count()
    );

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 3. Inactivity Watchdog & Silent Connection Recovery
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_inactivity_watchdog_proactive_reconnect_and_resume() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open store");

    // Fast watchdog: 100ms timeout, no pings
    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_inactivity_timeout(Duration::from_millis(100))
        .with_ping_interval(None)
        .with_backoff(Duration::from_millis(50), Duration::from_millis(100));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Emit 10 initial commits
    for i in 0..10 {
        let commit = JetstreamCommit {
            did: "did:plc:watchdog_user".to_string(),
            time_us: 1_750_000_000_000_000 + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("wd_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_{i}")),
            record: Some(json!({ "i": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
    }

    // Wait for 10 commits
    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < 10 {
        if start_wait.elapsed() > Duration::from_secs(2) {
            panic!("Timed out waiting for initial commits");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let initial_reconnects = consumer.stats().reconnect_count();

    // 2. Go completely silent for 300ms (3x the 100ms watchdog timeout)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Watchdog should have triggered at least one reconnect
    assert!(
        consumer.stats().reconnect_count() > initial_reconnects,
        "Watchdog must trigger reconnect when socket is silent. Before: {initial_reconnects}, After: {}",
        consumer.stats().reconnect_count()
    );

    // Wait for the consumer to finish backoff and reconnect
    let start_recon = Instant::now();
    while server.active_connections() == 0 {
        if start_recon.elapsed() > Duration::from_secs(3) {
            panic!("Consumer failed to reconnect after watchdog trigger");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 3. After watchdog reconnect, stream 10 more commits
    for i in 10..20 {
        let commit = JetstreamCommit {
            did: "did:plc:watchdog_user".to_string(),
            time_us: 1_750_000_000_000_000 + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("wd_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_{i}")),
            record: Some(json!({ "i": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
    }

    // Wait for all 20 commits to be stored
    let start_wait2 = Instant::now();
    while consumer.stats().records_upserted() < 20 {
        if start_wait2.elapsed() > Duration::from_secs(3) {
            panic!(
                "Timed out waiting for post-watchdog commits. Upserted: {}",
                consumer.stats().records_upserted()
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), 20);

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 4. At-Least-Once Replay & Idempotency Stress
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_replay_deduplication_and_idempotent_upserts() {
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 100 unique commits
    for i in 0..100 {
        let commit = JetstreamCommit {
            did: "did:plc:replay_user".to_string(),
            time_us: 1_760_000_000_000_000 + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("replay_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_v1_{i}")),
            record: Some(json!({ "version": 1, "i": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
    }

    while consumer.stats().records_upserted() < 100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Now deliberately re-stream the SAME 100 records with updated version
    for i in 0..100 {
        let commit = JetstreamCommit {
            did: "did:plc:replay_user".to_string(),
            time_us: 1_760_000_000_000_000 + (100 + i) as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("replay_post_{i}"),
            operation: CommitOperation::Update,
            cid: Some(format!("cid_v2_{i}")),
            record: Some(json!({ "version": 2, "i": i })),
        };
        server.emit_commit(&commit).expect("emit replay commit");
    }

    while consumer.stats().records_upserted() < 200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify: exactly 100 unique rows exist in SQLite, all updated to version 2!
    let total_rows = store
        .query("app.bsky.feed.post")
        .limit(200)
        .execute()
        .expect("execute query");
    assert_eq!(
        total_rows.len(),
        100,
        "Idempotent upsert must not create duplicate rows"
    );

    for row in &total_rows {
        assert_eq!(row.record_json["version"], 2);
        assert!(row.cid.starts_with("cid_v2_"));
    }

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 5. Out-of-Order Timestamps & Clock Skew Monotonicity
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_out_of_order_timestamps_preserve_cursor_monotonicity() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open store");

    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_backoff(Duration::from_millis(50), Duration::from_millis(100));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Sequence of intentionally non-monotonic timestamps (clock skew simulation)
    let timestamps = [
        1_000_000u64,
        2_500_000,
        1_800_000, // skew backward
        3_000_000,
        2_200_000, // skew backward
        4_000_000, // max watermark
        3_500_000, // skew backward
        1_200_000, // extreme backward skew
    ];

    for (i, &ts) in timestamps.iter().enumerate() {
        let commit = JetstreamCommit {
            did: "did:plc:skew_user".to_string(),
            time_us: ts,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("skew_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_skew_{i}")),
            record: Some(json!({ "ts": ts })),
        };
        server.emit_commit(&commit).expect("emit commit");
    }

    while consumer.stats().records_upserted() < timestamps.len() as u64 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // High watermark must strictly equal the maximum seen timestamp (4_000_000)
    assert_eq!(consumer.cursor(), 4_000_000);

    // Disconnect and verify the resume query uses 4_000_000, not the last seen (1_200_000)
    server.disconnect_all().expect("disconnect");
    tokio::time::sleep(Duration::from_millis(120)).await;

    let queries = server.query_history();
    let latest_query = queries.last().expect("latest query");
    assert!(
        latest_query.contains("cursor=4000000"),
        "Reconnect query must use monotonic watermark 4000000, got: {latest_query}"
    );

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 6. Edge Filtering Under High-Throughput Stream
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_high_throughput_edge_collection_and_did_filtering() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open store");

    // Filter strictly for collection: "app.bsky.feed.post" AND DID: "did:plc:wanted"
    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_did("did:plc:wanted")
        .with_channel_capacity(1024)
        .with_inactivity_timeout(Duration::from_secs(5));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(60)).await;

    let wanted_count = 300;
    let wrong_did_count = 300;
    let wrong_col_count = 300;
    let total_streamed = wanted_count + wrong_did_count + wrong_col_count;

    let base_time_us = 1_770_000_000_000_000u64;

    for i in 0..total_streamed {
        let (did, collection) = match i % 3 {
            0 => ("did:plc:wanted", "app.bsky.feed.post"), // PASS
            1 => ("did:plc:unwanted_did", "app.bsky.feed.post"), // FILTER BY DID
            _ => ("did:plc:wanted", "app.bsky.feed.like"), // FILTER BY COLLECTION
        };

        let commit = JetstreamCommit {
            did: did.to_string(),
            time_us: base_time_us + i as u64 * 1000,
            collection: collection.to_string(),
            rkey: format!("filter_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_{i}")),
            record: Some(json!({ "i": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
        if i % 100 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    // Wait until cursor reaches the end (all frames processed by reader)
    let expected_cursor = base_time_us + (total_streamed - 1) as u64 * 1000;
    let start_wait = Instant::now();
    while consumer.cursor() < expected_cursor {
        if start_wait.elapsed() > Duration::from_secs(5) {
            panic!(
                "Timed out waiting for cursor to reach {expected_cursor}, got: {}",
                consumer.cursor()
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Give storage worker a moment to process matched frames
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify: Exactly wanted_count records were upserted
    assert_eq!(consumer.stats().records_upserted(), wanted_count as u64);

    let stored_wanted = store
        .query("app.bsky.feed.post")
        .did("did:plc:wanted")
        .limit(1000)
        .execute()
        .expect("query wanted");
    assert_eq!(stored_wanted.len(), wanted_count);

    let stored_unwanted_did = store
        .query("app.bsky.feed.post")
        .did("did:plc:unwanted_did")
        .execute()
        .expect("query unwanted did");
    assert_eq!(stored_unwanted_did.len(), 0);

    let stored_unwanted_col = store.query("app.bsky.feed.like").execute().expect("query");
    assert_eq!(stored_unwanted_col.len(), 0);

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 7. Rapid CRUD Mutation Lifecycle & Tombstone Resurrection Under Firehose
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_rapid_crud_lifecycle_and_tombstone_resurrection() {
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    let num_records = 50;
    let base_time_us = 1_780_000_000_000_000u64;

    // For each record: Create v1 -> Update v2 -> Delete -> Resurrect v3
    for i in 0..num_records {
        let rkey = format!("resurrect_post_{i}");

        // 1. Create v1
        server
            .emit_commit(&JetstreamCommit {
                did: "did:plc:resurrection".to_string(),
                time_us: base_time_us + i as u64 * 4000,
                collection: "app.bsky.feed.post".to_string(),
                rkey: rkey.clone(),
                operation: CommitOperation::Create,
                cid: Some("cid_v1".to_string()),
                record: Some(json!({ "ver": 1 })),
            })
            .expect("emit create");

        // 2. Update v2
        server
            .emit_commit(&JetstreamCommit {
                did: "did:plc:resurrection".to_string(),
                time_us: base_time_us + i as u64 * 4000 + 1000,
                collection: "app.bsky.feed.post".to_string(),
                rkey: rkey.clone(),
                operation: CommitOperation::Update,
                cid: Some("cid_v2".to_string()),
                record: Some(json!({ "ver": 2 })),
            })
            .expect("emit update");

        // 3. Delete
        server
            .emit_commit(&JetstreamCommit {
                did: "did:plc:resurrection".to_string(),
                time_us: base_time_us + i as u64 * 4000 + 2000,
                collection: "app.bsky.feed.post".to_string(),
                rkey: rkey.clone(),
                operation: CommitOperation::Delete,
                cid: None,
                record: None,
            })
            .expect("emit delete");

        // 4. Resurrect v3 (Create again)
        server
            .emit_commit(&JetstreamCommit {
                did: "did:plc:resurrection".to_string(),
                time_us: base_time_us + i as u64 * 4000 + 3000,
                collection: "app.bsky.feed.post".to_string(),
                rkey: rkey.clone(),
                operation: CommitOperation::Create,
                cid: Some("cid_v3".to_string()),
                record: Some(json!({ "ver": 3 })),
            })
            .expect("emit resurrect");
    }

    // Expected total: 50 * 3 upserts = 150 upserts, 50 deletes
    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < (num_records * 3) as u64
        || consumer.stats().records_deleted() < num_records as u64
    {
        if start_wait.elapsed() > Duration::from_secs(5) {
            panic!(
                "Timed out waiting for lifecycle ops. Upserted: {}, Deleted: {}",
                consumer.stats().records_upserted(),
                consumer.stats().records_deleted()
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Verify all 50 records exist in SQLite as active records with ver == 3
    for i in 0..num_records {
        let uri = format!("at://did:plc:resurrection/app.bsky.feed.post/resurrect_post_{i}");
        let record = store
            .get_record(&uri)
            .expect("get_record")
            .unwrap_or_else(|| panic!("Record {uri} should be active after resurrection"));
        assert!(!record.is_deleted);
        assert_eq!(record.cid, "cid_v3");
        assert_eq!(record.record_json["ver"], 3);
    }

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 8. Corrupted & Poisoned Frame Stream Resilience
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_poisoned_frames_do_not_crash_consumer() {
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    let valid_count = 100;
    let base_time_us = 1_790_000_000_000_000u64;

    for i in 0..valid_count {
        // Interleave valid commits with poisonous / garbage frames
        match i % 6 {
            0 => {
                let _ = server.emit_raw("{ corrupt json syntax: true, ");
            }
            1 => {
                let _ = server.emit_raw("");
            }
            2 => {
                let _ = server.emit_raw(r#"{"kind": "commit", "commit": null}"#);
            }
            3 => {
                let _ = server.emit_raw(r#"{"kind": "commit", "did": "", "commit": {"collection": "app.bsky.feed.post", "rkey": "r1"}}"#);
            }
            4 => {
                let _ = server.emit_raw(r#"{"kind": "commit", "did": "did:plc:x", "commit": {"collection": "app.bsky.feed.post", "rkey": "r1", "operation": "invalid_op"}}"#);
            }
            _ => {
                // Heartbeat frame advancing cursor
                let _ = server.emit_heartbeat(base_time_us + i as u64 * 1000 - 500);
            }
        }

        // Emit valid commit
        let commit = JetstreamCommit {
            did: "did:plc:poison_test".to_string(),
            time_us: base_time_us + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("valid_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_valid_{i}")),
            record: Some(json!({ "text": "Survived poison" })),
        };
        server.emit_commit(&commit).expect("emit valid commit");
    }

    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < valid_count as u64 {
        if start_wait.elapsed() > Duration::from_secs(4) {
            panic!(
                "Timed out waiting for valid records amid poison frames. Current: {}",
                consumer.stats().records_upserted()
            );
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), valid_count as u64);
    assert_eq!(consumer.stats().sync_errors(), 0);

    handle.stop();
    handle.join().await.expect("clean join");
}

// ============================================================================
// 9. Clean Cancellation & Teardown Under Heavy Stream
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_clean_cancellation_during_high_speed_burst() {
    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open_in_memory().expect("open store");

    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_channel_capacity(512);

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Spawn task flooding mock server with commits
    let server_clone = server.clone();
    let flood_task = tokio::spawn(async move {
        for i in 0..5000 {
            let commit = JetstreamCommit {
                did: "did:plc:flood_user".to_string(),
                time_us: 1_800_000_000_000_000 + i as u64 * 100,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("flood_post_{i}"),
                operation: CommitOperation::Create,
                cid: Some(format!("cid_{i}")),
                record: Some(json!({ "i": i })),
            };
            if server_clone.emit_commit(&commit).is_err() {
                break;
            }
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // Let the stream run for 100ms
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop consumer while under heavy load
    let stop_start = Instant::now();
    handle.stop();
    let join_res = tokio::time::timeout(Duration::from_secs(2), handle.join()).await;

    assert!(
        join_res.is_ok(),
        "Consumer handle.join() must complete cleanly in < 2s on cancel"
    );
    assert!(
        stop_start.elapsed() < Duration::from_millis(800),
        "Consumer stopped promptly without hanging: {:?}",
        stop_start.elapsed()
    );

    let _ = flood_task.await;
}

// ============================================================================
// 10. Backoff Jitter Bounds & Entropy Distribution
// ============================================================================

#[test]
fn test_challenger_backoff_jitter_bounds_and_statistical_entropy() {
    let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));

    // Calculate next backoff 100 times without reset to test growth and jitter bounds
    let mut observed_delays = Vec::new();
    for _ in 0..100 {
        let delay = backoff.next_backoff();
        observed_delays.push(delay);
    }

    // Once capped at max_delay (30s), all subsequent delays must be within 30s ± 20% = [24s, 36s]
    let capped_delays: Vec<Duration> = observed_delays.iter().skip(10).copied().collect();
    for delay in &capped_delays {
        let ms = delay.as_millis();
        assert!(
            (24_000..=36_000).contains(&ms),
            "Delay {ms}ms violated jitter bounds [24000, 36000]"
        );
    }

    // Verify entropy: not all values are identical
    let first = capped_delays[0];
    let has_variation = capped_delays.iter().any(|&d| d != first);
    assert!(
        has_variation,
        "Jitter must produce pseudo-random variation, but all delays were identical ({first:?})"
    );
}

// ============================================================================
// 11. Concurrency: Live Queries During Heavy Ingestion
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_challenger_concurrent_queries_during_firehose_ingestion() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("concurrent_wal_ingest.db");

    let server = MockJetstreamServer::start()
        .await
        .expect("start mock server");
    let store = RecordStore::open(&db_path).expect("open file store");

    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_channel_capacity(1024)
        .with_batch_size(10);

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(60)).await;

    let total_records = 1000;
    let base_time_us = 1_810_000_000_000_000u64;

    // Spawn concurrent reader tasks querying SQLite while ingest runs
    let store_reader = store.clone();
    let reader_cancel = cancel.child_token();
    let reader_task = tokio::spawn(async move {
        let mut query_count = 0;
        while !reader_cancel.is_cancelled() {
            let res = store_reader
                .query("app.bsky.feed.post")
                .where_json("tier", QueryOp::Eq, json!("gold"))
                .order_by("indexed_at", SortDirection::Desc)
                .limit(20)
                .execute();
            assert!(
                res.is_ok(),
                "Query during live ingest must never fail: {:?}",
                res.err()
            );
            query_count += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        query_count
    });

    // Ingestion stream
    for i in 0..total_records {
        let tier = if i % 2 == 0 { "gold" } else { "silver" };
        let commit = JetstreamCommit {
            did: format!("did:plc:user_{}", i % 5),
            time_us: base_time_us + i as u64 * 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("post_{i:04}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_{i}")),
            record: Some(json!({ "tier": tier, "seq": i })),
        };
        server.emit_commit(&commit).expect("emit commit");
        if i % 50 == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    let start_wait = Instant::now();
    while consumer.stats().records_upserted() < total_records as u64 {
        if start_wait.elapsed() > Duration::from_secs(8) {
            panic!(
                "Timed out waiting for concurrent ingest. Current: {}",
                consumer.stats().records_upserted()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(consumer.stats().records_upserted(), total_records as u64);

    // Stop reader and consumer
    handle.stop();
    handle.join().await.expect("consumer join");
    let queries_completed = reader_task.await.expect("reader task");
    assert!(
        queries_completed > 10,
        "Reader task should have completed multiple concurrent queries, got {queries_completed}"
    );

    // Final verification: 500 gold posts and 500 silver posts
    let gold_posts = store
        .query("app.bsky.feed.post")
        .where_json("tier", QueryOp::Eq, json!("gold"))
        .limit(1000)
        .execute()
        .expect("gold query");
    assert_eq!(gold_posts.len(), 500);

    let silver_posts = store
        .query("app.bsky.feed.post")
        .where_json("tier", QueryOp::Eq, json!("silver"))
        .limit(1000)
        .execute()
        .expect("silver query");
    assert_eq!(silver_posts.len(), 500);
}
