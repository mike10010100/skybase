//! Empirical Challenger Stress Tests for `skybase::ingest` (Milestone 2).
//!
//! Stress-tests:
//! 1. Cursor Monotonicity under Clock Skew & Out-of-Order Streams:
//!    - Backwards time jumps, duplicate timestamps, zero timestamps, and microsecond-level clock warp.
//!    - High-concurrency multi-threaded chaos hammering `CursorTracker`.
//! 2. Inactivity Watchdog & Keepalive Ping:
//!    - Frozen socket simulation (silent server): watchdog triggers, closes socket cleanly, and initiates reconnection.
//!    - Keepalive ping/pong prevents false stall timeouts during quiet periods.
//!    - Exponential backoff with ±20% jitter and reconnect loop recovery.
//! 3. Malformed Frame Injection & Fuzzing Matrix:
//!    - Pathological frames (truncated JSON, control chars, binary payloads, missing commit fields, unknown operations).
//!    - Active WebSocket consumer surviving malformed frame barrages while continuing to process valid commits.
//!    - Batched storage synchronization under interleaved fuzzing and clock skew.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    unused_imports
)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use skybase::index::RecordStore;
use skybase::ingest::backoff::BackoffManager;
use skybase::ingest::consumer::{build_subscription_url_full, IngesterConfig, JetstreamConsumer};
use skybase::ingest::cursor::CursorTracker;
use skybase::ingest::events::{
    normalize_indexed_at, parse_frame_timestamp, parse_jetstream_commit, parse_jetstream_frame,
    CommitOperation, JetstreamCommit, JetstreamEvent,
};
use skybase::ingest::mock::MockJetstreamServer;

// ============================================================================
// 1. Clock-Warp and Out-of-Order Cursor Monotonicity Stress Tests
// ============================================================================

#[test]
fn test_challenger_cursor_clock_skew_and_backwards_jumps() {
    let tracker = CursorTracker::new(100_000);
    assert_eq!(tracker.get(), 100_000);

    // Stream of timestamps including:
    // - normal advancement (150_000)
    // - backwards jumps (120_000, 80_000)
    // - forward jumps (200_000)
    // - duplicate timestamp (200_000)
    // - slight backwards jump (199_999)
    // - severe backwards jump (100_000)
    // - large forward jump (500_000)
    // - zero timestamp (0)
    // - tiny timestamp (50)
    // - large forward jump (1_000_000)
    // - equal timestamp (1_000_000)
    let sequence: Vec<(u64, bool, u64)> = vec![
        (150_000, true, 150_000),
        (120_000, false, 150_000),
        (80_000, false, 150_000),
        (200_000, true, 200_000),
        (200_000, false, 200_000),
        (199_999, false, 200_000),
        (100_000, false, 200_000),
        (500_000, true, 500_000),
        (0, false, 500_000),
        (50, false, 500_000),
        (1_000_000, true, 1_000_000),
        (1_000_000, false, 1_000_000),
        (u64::MAX, true, u64::MAX),
        (u64::MAX - 1, false, u64::MAX),
        (u64::MAX, false, u64::MAX),
    ];

    for (ts, expected_advancement, expected_watermark) in sequence {
        let advanced = tracker.update(ts);
        assert_eq!(
            advanced, expected_advancement,
            "Timestamp {ts} update returned {advanced}, expected {expected_advancement}"
        );
        assert_eq!(
            tracker.get(),
            expected_watermark,
            "Watermark mismatch after ts={ts}: got {}, expected {expected_watermark}",
            tracker.get()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_cursor_concurrent_multi_threaded_chaos() {
    let tracker = Arc::new(CursorTracker::new(0));
    let num_writer_tasks = 40;
    let ops_per_writer = 500;
    let num_reader_tasks = 20;

    let mut set = JoinSet::new();

    // Spawn concurrent writers hammering the tracker with random and out-of-order timestamps
    for writer_id in 0..num_writer_tasks {
        let tracker_clone = Arc::clone(&tracker);
        set.spawn(async move {
            let mut pseudo_seed = (writer_id as u64 + 1).wrapping_mul(1103515245);
            for i in 0..ops_per_writer {
                // Generate pseudo-random numbers covering low, medium, high, and zero values
                pseudo_seed = pseudo_seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1);
                let rand_val = match i % 5 {
                    0 => 0,                                              // Zero
                    1 => (pseudo_seed % 10_000) + 1,                     // Low timestamps
                    2 => 100_000 + (pseudo_seed % 500_000),              // Medium timestamps
                    3 => 1_000_000 + (pseudo_seed % 10_000_000),         // High timestamps
                    _ => (writer_id as u64) * 100_000 + (i as u64 * 10), // Monotonic per-thread
                };
                tracker_clone.update(rand_val);
                if i % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    // Spawn concurrent readers verifying that their observed watermark is non-decreasing
    for _ in 0..num_reader_tasks {
        let tracker_clone = Arc::clone(&tracker);
        set.spawn(async move {
            let mut last_observed = 0u64;
            for _ in 0..ops_per_writer {
                let current = tracker_clone.get();
                assert!(
                    current >= last_observed,
                    "Monotonicity violation: observed {current} after {last_observed}"
                );
                last_observed = current;
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("Task failed");
    }

    // Watermark must be strictly greater than 0 and at least equal to the guaranteed high timestamp
    assert!(tracker.get() >= 1_000_000);
}

#[test]
fn test_challenger_cursor_boundaries_and_resets() {
    let t = CursorTracker::from_option(None);
    assert_eq!(t.get(), 0);
    assert_eq!(t.get_opt(), None);

    // Setting cursor explicitly (for deliberate rewinds / resets)
    t.set(5000);
    assert_eq!(t.get(), 5000);
    assert_eq!(t.get_opt(), Some(5000));

    // Monotonic advance from explicit set
    assert!(t.update(6000));
    assert_eq!(t.get(), 6000);

    // Deliberate rewind via set
    t.set(2000);
    assert_eq!(t.get(), 2000);
    assert_eq!(t.get_opt(), Some(2000));

    // Stale update below 2000 rejected
    assert!(!t.update(1000));
    assert_eq!(t.get(), 2000);
}

#[tokio::test]
async fn test_challenger_consumer_reconnection_url_advances_cursor_param() {
    let server = MockJetstreamServer::start().await.expect("start server");
    let store = RecordStore::open_in_memory().expect("open store");

    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_cursor(100_000)
        .with_inactivity_timeout(Duration::from_millis(100))
        .with_ping_interval(None)
        .with_backoff(Duration::from_millis(20), Duration::from_millis(50));

    let consumer = JetstreamConsumer::new(config, store);
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    // Wait for initial connection
    tokio::time::sleep(Duration::from_millis(60)).await;
    let queries1 = server.query_history();
    assert!(!queries1.is_empty());
    assert!(queries1[0].contains("cursor=100000"));

    // Emit an event that advances the cursor to 250_000
    let commit = JetstreamCommit {
        did: "did:plc:cursor_advance".to_string(),
        time_us: 250_000,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "r1".to_string(),
        operation: CommitOperation::Create,
        cid: Some("cid_c1".to_string()),
        record: Some(json!({"text": "advanced cursor"})),
    };
    server.emit_commit(&commit).expect("emit commit");
    tokio::time::sleep(Duration::from_millis(40)).await;

    assert_eq!(consumer.cursor(), 250_000);

    // Disconnect all clients to trigger reconnect loop
    server.clear_query_history();
    server.disconnect_all().expect("disconnect");

    // Wait for reconnection
    tokio::time::sleep(Duration::from_millis(120)).await;

    let queries2 = server.query_history();
    assert!(
        !queries2.is_empty(),
        "Server should have received reconnected query"
    );
    assert!(
        queries2.iter().any(|q| q.contains("cursor=250000")),
        "Reconnection query must contain updated cursor 250000. Got queries: {:?}",
        queries2
    );

    handle.stop();
    handle.join().await.expect("join failed");
}

// ============================================================================
// 2. Inactivity Watchdog and Keepalive Ping Stress Tests
// ============================================================================

#[tokio::test]
async fn test_challenger_watchdog_detects_silent_stall_and_reconnects_multiple_times() {
    let server = MockJetstreamServer::start().await.expect("start server");
    let store = RecordStore::open_in_memory().expect("open store");

    // Set aggressive inactivity timeout of 60ms and fast backoff (20ms)
    let config = IngesterConfig::new(server.ws_url())
        .with_inactivity_timeout(Duration::from_millis(60))
        .with_ping_interval(None)
        .with_backoff(Duration::from_millis(20), Duration::from_millis(40));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    // Allow 260ms of total silence on the socket
    tokio::time::sleep(Duration::from_millis(260)).await;

    let stats = consumer.stats();
    let reconns = stats.reconnect_count();
    let total_server_conns = server.total_connections();

    assert!(
        reconns >= 2,
        "Watchdog should have triggered at least 2 reconnects due to inactivity. Got: {reconns}"
    );
    assert!(
        total_server_conns >= 2,
        "Server should have seen multiple accepted connections. Got: {total_server_conns}"
    );

    // Now emit a valid commit on the current reconnected session
    let valid_commit = JetstreamCommit {
        did: "did:plc:recovered_user".to_string(),
        time_us: 1_716_000_000_000_000,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "post_after_stall".to_string(),
        operation: CommitOperation::Create,
        cid: Some("bafyrei_recovered".to_string()),
        record: Some(json!({"text": "Ingest recovered after watchdog stall cycles"})),
    };
    server.emit_commit(&valid_commit).expect("emit commit");

    // Wait for ingest sync
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(stats.records_upserted(), 1);
    let record = store
        .get_record("at://did:plc:recovered_user/app.bsky.feed.post/post_after_stall")
        .expect("get_record")
        .expect("record must exist");
    assert_eq!(record.cid, "bafyrei_recovered");
    assert_eq!(consumer.cursor(), 1_716_000_000_000_000);

    handle.stop();
    handle.join().await.expect("join cleanly");
}

#[tokio::test]
async fn test_challenger_keepalive_pings_prevent_watchdog_stall_during_quiet_periods() {
    let server = MockJetstreamServer::start().await.expect("start server");
    let store = RecordStore::open_in_memory().expect("open store");

    // Inactivity timeout: 120ms.
    // Ping interval: 30ms (well below inactivity timeout).
    // The server handles Ping/Pong automatically in MockJetstreamServer.
    let config = IngesterConfig::new(server.ws_url())
        .with_inactivity_timeout(Duration::from_millis(120))
        .with_ping_interval(Some(Duration::from_millis(30)))
        .with_backoff(Duration::from_millis(30), Duration::from_millis(60));

    let consumer = JetstreamConsumer::new(config, store);
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    // Wait 250ms (more than 2x the inactivity timeout) with ZERO commit events emitted.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Because keepalive pings/pongs occurred every 30ms, last_activity stayed fresh!
    let stats = consumer.stats();
    assert_eq!(
        stats.reconnect_count(),
        0,
        "Keepalive pings should prevent watchdog timeouts during quiet periods"
    );
    assert_eq!(server.total_connections(), 1);

    handle.stop();
    handle.join().await.expect("join cleanly");
}

#[test]
fn test_challenger_backoff_jitter_and_saturation_limits() {
    let mut backoff =
        BackoffManager::new(Duration::from_millis(100), Duration::from_millis(800)).with_jitter(20);

    // Initial delay is 100ms
    assert_eq!(backoff.current_delay(), Duration::from_millis(100));

    // Attempt 1: base 100ms, jitter in [-20%, +20%] -> [80ms, 120ms]
    let d1 = backoff.next_backoff();
    assert!((80..=120).contains(&d1.as_millis()), "d1 was {d1:?}");
    assert_eq!(backoff.current_delay(), Duration::from_millis(200));

    // Attempt 2: base 200ms -> [160ms, 240ms]
    let d2 = backoff.next_backoff();
    assert!((160..=240).contains(&d2.as_millis()), "d2 was {d2:?}");
    assert_eq!(backoff.current_delay(), Duration::from_millis(400));

    // Attempt 3: base 400ms -> [320ms, 480ms]
    let d3 = backoff.next_backoff();
    assert!((320..=480).contains(&d3.as_millis()), "d3 was {d3:?}");
    assert_eq!(backoff.current_delay(), Duration::from_millis(800));

    // Attempt 4: capped at 800ms -> [640ms, 960ms]
    let d4 = backoff.next_backoff();
    assert!((640..=960).contains(&d4.as_millis()), "d4 was {d4:?}");
    assert_eq!(backoff.current_delay(), Duration::from_millis(800));

    // Attempt 5: still capped at 800ms
    let d5 = backoff.next_backoff();
    assert!((640..=960).contains(&d5.as_millis()), "d5 was {d5:?}");

    // Reset restores initial 100ms
    backoff.reset();
    assert_eq!(backoff.current_delay(), Duration::from_millis(100));
    assert_eq!(backoff.consecutive_failures(), 0);
}

// ============================================================================
// 3. Malformed Frame Injection and Fuzzing Matrix
// ============================================================================

#[test]
fn test_challenger_parse_jetstream_frame_adversarial_matrix() {
    // 1. Completely empty and whitespace frames
    assert_eq!(parse_jetstream_frame(""), None);
    assert_eq!(parse_jetstream_frame("     "), None);
    assert_eq!(parse_jetstream_frame("\n\t\r\n"), None);

    // 2. Non-JSON garbage and control characters
    let garbage_inputs = [
        "not json at all",
        "!!!???###$$$",
        "\0\0\0\0",
        "\x01\x02\x03\x04",
        "{\"unterminated_string: true",
        "{\"unclosed_brace\": 123",
        "[1, 2, 3, unterminated",
        "{ corrupt: json: here }",
        "<!DOCTYPE html><html><body>Error</body></html>",
        "HTTP/1.1 500 Internal Server Error\r\n\r\n",
    ];
    for garbage in garbage_inputs {
        assert_eq!(
            parse_jetstream_frame(garbage),
            None,
            "Garbage input '{garbage}' must safely return None"
        );
    }

    // 3. JSON primitives that are not objects
    let primitives = [
        "null",
        "true",
        "false",
        "12345",
        "-99.9",
        "\"just a string\"",
    ];
    for prim in primitives {
        assert_eq!(
            parse_jetstream_frame(prim),
            None,
            "Primitive '{prim}' must safely return None"
        );
    }

    // 4. JSON arrays
    assert_eq!(parse_jetstream_frame("[]"), None);
    assert_eq!(parse_jetstream_frame("[{\"kind\": \"commit\"}]"), None);

    // 5. Commit events with missing or empty required fields
    // Missing DID with time_us -> degrades to Heartbeat
    let no_did = r#"{"kind": "commit", "time_us": 12345, "commit": {"operation": "create", "collection": "app.bsky.feed.post", "rkey": "r1"}}"#;
    assert_eq!(
        parse_jetstream_frame(no_did),
        Some(JetstreamEvent::Heartbeat { time_us: 12345 })
    );

    // Empty whitespace DID with time_us -> degrades to Heartbeat
    let empty_did = r#"{"kind": "commit", "did": "   ", "time_us": 12345, "commit": {"operation": "create", "collection": "app.bsky.feed.post", "rkey": "r1"}}"#;
    assert_eq!(
        parse_jetstream_frame(empty_did),
        Some(JetstreamEvent::Heartbeat { time_us: 12345 })
    );

    // Missing commit object with time_us -> degrades to Heartbeat
    let no_commit_obj = r#"{"kind": "commit", "did": "did:plc:test", "time_us": 67890}"#;
    assert_eq!(
        parse_jetstream_frame(no_commit_obj),
        Some(JetstreamEvent::Heartbeat { time_us: 67890 })
    );

    // Empty collection with time_us -> degrades to Heartbeat
    let empty_col = r#"{"kind": "commit", "did": "did:plc:test", "time_us": 111, "commit": {"operation": "create", "collection": "", "rkey": "r1"}}"#;
    assert_eq!(
        parse_jetstream_frame(empty_col),
        Some(JetstreamEvent::Heartbeat { time_us: 111 })
    );

    // Empty rkey with time_us -> degrades to Heartbeat
    let empty_rkey = r#"{"kind": "commit", "did": "did:plc:test", "time_us": 222, "commit": {"operation": "create", "collection": "app.bsky.feed.post", "rkey": "   "}}"#;
    assert_eq!(
        parse_jetstream_frame(empty_rkey),
        Some(JetstreamEvent::Heartbeat { time_us: 222 })
    );

    // Unknown operation with time_us -> degrades to Heartbeat
    let unknown_op = r#"{"kind": "commit", "did": "did:plc:test", "time_us": 333, "commit": {"operation": "obliterate", "collection": "app.bsky.feed.post", "rkey": "r1"}}"#;
    assert_eq!(
        parse_jetstream_frame(unknown_op),
        Some(JetstreamEvent::Heartbeat { time_us: 333 })
    );

    // Missing commit fields with time_us = 0 -> returns None
    let zero_time_bad_commit = r#"{"kind": "commit", "did": "did:plc:test", "time_us": 0, "commit": {"operation": "unknown", "collection": "", "rkey": ""}}"#;
    assert_eq!(parse_jetstream_frame(zero_time_bad_commit), None);

    // 6. Non-commit events (identity, account, unknown)
    let identity_event = r#"{"kind": "identity", "did": "did:plc:alice", "time_us": 55555}"#;
    match parse_jetstream_frame(identity_event) {
        Some(JetstreamEvent::Other { kind, did, time_us }) => {
            assert_eq!(kind, "identity");
            assert_eq!(did.as_deref(), Some("did:plc:alice"));
            assert_eq!(time_us, 55555);
        }
        other => panic!("Expected JetstreamEvent::Other, got: {other:?}"),
    }

    let pure_heartbeat = r#"{"time_us": 999999}"#;
    assert_eq!(
        parse_jetstream_frame(pure_heartbeat),
        Some(JetstreamEvent::Heartbeat { time_us: 999999 })
    );
}

#[tokio::test]
async fn test_challenger_active_consumer_resilience_under_malformed_frame_barrage() {
    let server = MockJetstreamServer::start().await.expect("start server");
    let store = RecordStore::open_in_memory().expect("open store");

    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_inactivity_timeout(Duration::from_secs(5));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Barrage 50 malformed frames
    let malformed_burst = [
        "not json",
        "{ unclosed:",
        r#"{"kind": "commit", "did": ""}"#,
        r#"{"kind": "commit", "commit": null}"#,
        r#"{"kind": "commit", "commit": {"operation": "explode"}}"#,
        r#"{"unknown": true}"#,
        "",
        "null",
        "42",
        "{\"did\": \"did:plc:foo\", \"commit\": {\"operation\": \"create\", \"collection\": \"\", \"rkey\": \"\"}}",
    ];

    for _ in 0..5 {
        for frame in &malformed_burst {
            server.emit_raw(frame).expect("emit raw");
        }
    }

    // 2. Emit Valid Commit 1 (Create)
    let commit1 = JetstreamCommit {
        did: "did:plc:user1".to_string(),
        time_us: 1_710_000_000_000_001,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "post_1".to_string(),
        operation: CommitOperation::Create,
        cid: Some("cid_1".to_string()),
        record: Some(json!({"text": "First valid post"})),
    };
    server.emit_commit(&commit1).expect("emit commit 1");

    // 3. Barrage another 50 malformed frames
    for _ in 0..5 {
        for frame in &malformed_burst {
            server.emit_raw(frame).expect("emit raw");
        }
    }

    // 4. Emit Valid Commit 2 (Update)
    let commit2 = JetstreamCommit {
        did: "did:plc:user1".to_string(),
        time_us: 1_710_000_000_000_002,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "post_1".to_string(),
        operation: CommitOperation::Update,
        cid: Some("cid_2".to_string()),
        record: Some(json!({"text": "Updated post text"})),
    };
    server.emit_commit(&commit2).expect("emit commit 2");

    // 5. Barrage 30 malformed frames
    for _ in 0..3 {
        for frame in &malformed_burst {
            server.emit_raw(frame).expect("emit raw");
        }
    }

    // 6. Emit Valid Commit 3 (Delete)
    let commit3 = JetstreamCommit {
        did: "did:plc:user1".to_string(),
        time_us: 1_710_000_000_000_003,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "post_1".to_string(),
        operation: CommitOperation::Delete,
        cid: None,
        record: None,
    };
    server.emit_commit(&commit3).expect("emit commit 3");

    // Wait for consumer and storage worker to process frames
    tokio::time::sleep(Duration::from_millis(150)).await;

    let stats = consumer.stats();
    assert!(
        stats.frames_received() > 100,
        "Total frames received should exceed 100 (got {})",
        stats.frames_received()
    );
    assert_eq!(
        stats.records_upserted(),
        2,
        "Exactly 2 upserts must have succeeded"
    );
    assert_eq!(
        stats.records_deleted(),
        1,
        "Exactly 1 delete must have succeeded"
    );
    assert_eq!(
        stats.sync_errors(),
        0,
        "Zero sync errors should have occurred"
    );
    assert_eq!(
        consumer.cursor(),
        1_710_000_000_000_003,
        "Cursor must match latest valid commit timestamp"
    );

    // Verify storage reflects soft delete
    let row = store
        .get_record_including_deleted("at://did:plc:user1/app.bsky.feed.post/post_1")
        .expect("get_record")
        .expect("row must exist");
    assert!(row.is_deleted, "Record must be marked is_deleted = true");
    assert_eq!(row.cid, "cid_2");

    handle.stop();
    handle.join().await.expect("join cleanly");
}

#[tokio::test]
async fn test_challenger_consumer_batched_sync_with_interleaved_clock_warp() {
    let server = MockJetstreamServer::start().await.expect("start server");
    let store = RecordStore::open_in_memory().expect("open store");

    // Batch size = 10
    let config = IngesterConfig::new(server.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_batch_size(10)
        .with_inactivity_timeout(Duration::from_secs(5));

    let consumer = JetstreamConsumer::new(config, store.clone());
    let cancel = CancellationToken::new();
    let handle = consumer.start(cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 25 commits with out-of-order timestamps and interleaved heartbeats
    let timestamps = [
        1000, 900, 1100, 1050, 1200, 1150, 1300, 1250, 1400, 1350, // Batch 1
        1500, 1450, 1600, 1550, 1700, 1650, 1800, 1750, 1900, 1850, // Batch 2
        2000, 1950, 2100, 2050, 2200, // Partial Batch 3
    ];

    for (idx, &ts) in timestamps.iter().enumerate() {
        let commit = JetstreamCommit {
            did: "did:plc:batch_user".to_string(),
            time_us: ts,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("rkey_{idx}"),
            operation: CommitOperation::Create,
            cid: Some(format!("cid_{idx}")),
            record: Some(json!({"index": idx, "ts": ts})),
        };
        server.emit_commit(&commit).expect("emit commit");
    }

    // Stop consumer cleanly to flush partial batch
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.stop();
    handle.join().await.expect("join cleanly");

    // Verify all 25 records are in SQLite
    let rows = store
        .query("app.bsky.feed.post")
        .limit(50)
        .execute()
        .expect("query rows");
    assert_eq!(rows.len(), 25, "All 25 batch records must be stored");
    assert_eq!(
        consumer.stats().records_upserted(),
        25,
        "Total upsert count must be 25"
    );
    assert_eq!(
        consumer.cursor(),
        2200,
        "High watermark must be max timestamp (2200)"
    );
}
