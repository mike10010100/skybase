//! Empirical Challenger Integration & Stress Tests for Milestone 4 (Skybase Facade & Vertical Slice).
//!
//! Adversarially challenges:
//! 1. Concurrent closed-loop stress: Parallel sovereign PDS writes -> Jetstream commit emission ->
//!    active consumer ingestion -> SQLite WAL storage -> concurrent JSON1 readers with LIKE patterns ->
//!    multiple broadcast subscribers.
//! 2. Closed-loop interleaved CRUD lifecycle (create -> update -> soft-delete -> audit queries + bus).
//! 3. Dynamic store lifecycles (switching in-memory and file-backed SQLite stores under concurrent load).
//! 4. Fail-closed behavior on unconfigured store, missing Jetstream endpoints, and invalid configs.
//! 5. Sovereign PDS client automatic DPoP-nonce challenge/retry via the facade.
//! 6. Broadcast bus multi-subscriber fanout with fast and slow subscribers (lag handling and re-sync).
//! 7. Rich JSON1 query combinations, deep paths, SQL injection immunity, and LIKE patterns.
//! 8. Consumer resilience against malformed frames interleaved with valid commits.
//! 9. Consumer graceful cancellation, shutdown, and restart lifecycle.
//! 10. High-concurrency mixed read/write/delete race on file-backed SQLite WAL.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::MockPdsServer;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use skybase::{
    ChangeNotification, CommitOperation, JetstreamCommit, MockJetstreamServer, QueryOp,
    RecordInput, Skybase, SkybaseConfig, SkybaseError, SortDirection,
};

// ============================================================================
// 1. Concurrent Closed-Loop Stress: Writers -> Jetstream -> Consumer -> SQLite -> Readers -> Bus
// ============================================================================

/// Stress-tests the complete closed-loop pipeline under high multi-threaded concurrent load.
///
/// 10 concurrent writer tasks issue `create_record` calls to Mock PDS via `Skybase::repo_client_from_credentials`.
/// Emits corresponding commit frames to `MockJetstreamServer`.
/// `Skybase::consumer` ingests and persists frames into SQLite WAL storage.
/// 5 concurrent reader tasks continuously execute JSON1 queries with LIKE pattern matching.
/// 5 broadcast subscribers continuously drain live notifications.
///
/// Asserts zero dropped commits, zero corrupted transactions, zero panics, and zero deadlocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_concurrent_closed_loop_writers_ingest_readers_subscribers() {
    let mock_pds = MockPdsServer::start().await;
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("Mock Jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Challenger Closed Loop App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("com.foodapp.restaurant.review")
    .with_wanted_collection("app.bsky.feed.post");

    let skybase = Skybase::new(config).expect("Skybase facade init failed");

    // Start 5 broadcast subscribers
    let mut subscribers = Vec::new();
    for _ in 0..5 {
        subscribers.push(skybase.subscribe().expect("subscribe failed"));
    }

    // Start consumer background worker and retain stats for diagnostics
    let cancel = CancellationToken::new();
    let mut ingester_cfg = skybase::IngesterConfig::new(mock_jetstream.ws_url())
        .with_collection("com.foodapp.restaurant.review")
        .with_collection("app.bsky.feed.post");
    ingester_cfg.ping_interval = None;
    let consumer = skybase
        .consumer_with_config(ingester_cfg)
        .expect("consumer build failed");
    let stats = consumer.stats();
    let consumer_handle = consumer.start(cancel.clone());

    // Wait for consumer to establish WebSocket session with mock Jetstream server
    let start_wait = Instant::now();
    while mock_jetstream.active_connections() == 0 {
        if start_wait.elapsed() > Duration::from_secs(5) {
            panic!("Timed out waiting for consumer to connect to MockJetstreamServer");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let total_writers = 10;
    let records_per_writer = 15;
    let total_records = total_writers * records_per_writer;

    let is_running = Arc::new(AtomicBool::new(true));
    let reader_queries_executed = Arc::new(AtomicUsize::new(0));

    // Spawn 5 background concurrent readers
    let mut reader_tasks = JoinSet::new();
    for reader_idx in 0..5 {
        let skybase_clone = skybase.clone();
        let running_clone = Arc::clone(&is_running);
        let queries_clone = Arc::clone(&reader_queries_executed);

        reader_tasks.spawn(async move {
            while running_clone.load(Ordering::Relaxed) {
                // Execute query with JSON1 filter and LIKE pattern
                let res = skybase_clone
                    .collection("com.foodapp.restaurant.review")
                    .expect("collection builder failed")
                    .where_json("rating", QueryOp::Gte, 3)
                    .where_json("restaurantName", QueryOp::Like, "%Bistro%")
                    .order_by("indexed_at", SortDirection::Desc)
                    .limit(20)
                    .execute();

                assert!(
                    res.is_ok(),
                    "Concurrent reader {reader_idx} query failed: {:?}",
                    res.err()
                );
                queries_clone.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });
    }

    // Spawn 5 subscriber drainers
    let subscriber_events_received = Arc::new(AtomicUsize::new(0));
    let mut sub_tasks = JoinSet::new();
    for mut rx in subscribers {
        let running_clone = Arc::clone(&is_running);
        let events_clone = Arc::clone(&subscriber_events_received);

        sub_tasks.spawn(async move {
            let mut count = 0;
            while running_clone.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
                    Ok(Ok(_notif)) => {
                        count += 1;
                        events_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(broadcast::error::RecvError::Lagged(_n))) => {
                        // Acceptable under high burst; subscriber recovers
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => break,
                    Err(_) => {
                        // Timeout tick, loop again
                    }
                }
            }
            count
        });
    }

    let mock_jetstream = Arc::new(mock_jetstream);

    // Spawn 10 concurrent writers
    let mut writer_tasks = JoinSet::new();
    let base_time_us = 1_726_100_000_000_000u64;

    for writer_idx in 0..total_writers {
        let skybase_clone = skybase.clone();
        let mock_pds_uri = mock_pds.uri();
        let mock_js = Arc::clone(&mock_jetstream);
        let stats_clone = Arc::clone(&stats);

        writer_tasks.spawn(async move {
            let did = format!("did:plc:writer_{writer_idx}");
            let pds_client = skybase_clone
                .repo_client_from_credentials(&mock_pds_uri, &did, format!("token_{writer_idx}"))
                .expect("repo client creation failed");

            for r_idx in 0..records_per_writer {
                let rkey = format!("rev_{writer_idx}_{r_idx}");
                let rating = 3 + (r_idx % 3); // 3, 4, 5
                let name = format!("Decentralized Bistro #{writer_idx}-{r_idx}");
                let payload = json!({
                    "$type": "com.foodapp.restaurant.review",
                    "restaurantName": name,
                    "rating": rating,
                    "tags": ["bistro", "organic", "fusion"],
                    "seq": r_idx
                });

                // 1. Sovereign PDS Write
                let pds_res = pds_client
                    .create_record("com.foodapp.restaurant.review", Some(&rkey), &payload, true)
                    .await
                    .expect("PDS createRecord failed");

                // 2. Emit Commit to Mock Jetstream (with reconnection retry leeway)
                let commit_time = base_time_us + (writer_idx as u64 * 10_000) + r_idx as u64;
                let commit = JetstreamCommit {
                    did: did.clone(),
                    time_us: commit_time,
                    collection: "com.foodapp.restaurant.review".to_string(),
                    rkey,
                    operation: CommitOperation::Create,
                    cid: Some(pds_res.cid),
                    record: Some(payload),
                };

                let mut emitted = false;
                for _ in 0..50 {
                    if mock_js.emit_commit(&commit).is_ok() {
                        emitted = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if !emitted {
                    panic!(
                        "writer {writer_idx} record {r_idx} emit commit failed: active: {}, total: {}, frames: {}, upserts: {}, recons: {}, errors: {}",
                        mock_js.active_connections(),
                        mock_js.total_connections(),
                        stats_clone.frames_received(),
                        stats_clone.records_upserted(),
                        stats_clone.reconnect_count(),
                        stats_clone.sync_errors(),
                    );
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    // Await all writers
    while let Some(res) = writer_tasks.join_next().await {
        res.expect("writer task panicked");
    }

    // Wait for consumer to process all commits
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut ingested_count = 0;
    while Instant::now() < deadline {
        let records = skybase
            .collection("com.foodapp.restaurant.review")
            .expect("collection query failed")
            .execute()
            .expect("query execute failed");
        ingested_count = records.len();
        if ingested_count >= total_records {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        ingested_count, total_records,
        "All {total_records} commits must be ingested into SQLite without loss"
    );

    // Stop background readers and subscribers
    is_running.store(false, Ordering::Relaxed);
    while let Some(res) = reader_tasks.join_next().await {
        res.expect("reader task panicked");
    }
    while let Some(res) = sub_tasks.join_next().await {
        res.expect("subscriber task panicked");
    }

    assert!(
        reader_queries_executed.load(Ordering::Relaxed) > 10,
        "Concurrent readers should have executed multiple queries during writes"
    );
    assert!(
        subscriber_events_received.load(Ordering::Relaxed) > 0,
        "Subscribers should have received events during closed-loop execution"
    );

    // Verify LIKE and JSON1 filtering accuracy on the completed dataset
    let bistro_records = skybase
        .collection("com.foodapp.restaurant.review")
        .expect("collection query failed")
        .where_json("restaurantName", QueryOp::Like, "%Decentralized Bistro%")
        .execute()
        .expect("query failed");
    assert_eq!(bistro_records.len(), total_records);

    let five_star_reviews = skybase
        .collection("com.foodapp.restaurant.review")
        .expect("collection query failed")
        .where_json("rating", QueryOp::Eq, 5)
        .execute()
        .expect("query failed");
    assert_eq!(five_star_reviews.len(), total_records / 3);

    // Clean consumer shutdown
    consumer_handle.stop();
    consumer_handle.join().await.expect("consumer join failed");
}

// ============================================================================
// 2. Closed-Loop Interleaved CRUD Lifecycle: Create -> Update -> Delete -> Audit
// ============================================================================

/// Tests full closed-loop CRUD mutation lifecycle with tombstone audits and broadcast updates.
#[tokio::test]
async fn test_challenger_closed_loop_interleaved_crud_and_live_bus() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("Mock Jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "CRUD App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("com.notes.secure.entry");

    let skybase = Skybase::new(config).expect("Skybase facade init failed");
    let mut bus_sub = skybase.subscribe().expect("subscribe failed");

    let cancel = CancellationToken::new();
    let consumer_handle = skybase
        .start_consumer(cancel.clone())
        .expect("start_consumer failed");

    // Wait for consumer WebSocket connection
    let start_wait = Instant::now();
    while mock_jetstream.active_connections() == 0 {
        if start_wait.elapsed() > Duration::from_secs(5) {
            panic!("Timed out waiting for consumer to connect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let user_did = "did:plc:crud_tester";
    let base_time_us = 1_726_200_000_000_000u64;

    // Phase 1: Create 60 records
    for i in 0..60 {
        let commit = JetstreamCommit {
            did: user_did.to_string(),
            time_us: base_time_us + i,
            collection: "com.notes.secure.entry".to_string(),
            rkey: format!("note_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("bafy_cid_v1_{i}")),
            record: Some(json!({
                "title": format!("Draft Note #{i}"),
                "encrypted": false,
                "version": 1
            })),
        };
        mock_jetstream
            .emit_commit(&commit)
            .expect("emit create failed");
    }

    // Wait for Phase 1 ingest
    let phase1_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < phase1_deadline {
        let count = skybase
            .collection("com.notes.secure.entry")
            .unwrap()
            .execute()
            .unwrap()
            .len();
        if count >= 60 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Phase 2: Update 30 records (indices 0..30)
    for i in 0..30 {
        let commit = JetstreamCommit {
            did: user_did.to_string(),
            time_us: base_time_us + 1000 + i,
            collection: "com.notes.secure.entry".to_string(),
            rkey: format!("note_{i}"),
            operation: CommitOperation::Update,
            cid: Some(format!("bafy_cid_v2_{i}")),
            record: Some(json!({
                "title": format!("Encrypted Note #{i} (Finalized)"),
                "encrypted": true,
                "version": 2
            })),
        };
        mock_jetstream
            .emit_commit(&commit)
            .expect("emit update failed");
    }

    // Phase 3: Soft-Delete 20 records (indices 20..40)
    for i in 20..40 {
        let commit = JetstreamCommit {
            did: user_did.to_string(),
            time_us: base_time_us + 2000 + i,
            collection: "com.notes.secure.entry".to_string(),
            rkey: format!("note_{i}"),
            operation: CommitOperation::Delete,
            cid: None,
            record: None,
        };
        mock_jetstream
            .emit_commit(&commit)
            .expect("emit delete failed");
    }

    // Wait for Phase 2 & 3
    let audit_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < audit_deadline {
        let active = skybase
            .collection("com.notes.secure.entry")
            .unwrap()
            .execute()
            .unwrap();
        if active.len() == 40 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Default query excludes deleted items: 60 - 20 deleted = 40 active records
    let active_records = skybase
        .collection("com.notes.secure.entry")
        .expect("col failed")
        .execute()
        .expect("exec failed");
    assert_eq!(active_records.len(), 40);

    // Audit query includes deleted: all 60 records exist
    let audit_records = skybase
        .collection("com.notes.secure.entry")
        .expect("col failed")
        .include_deleted(true)
        .execute()
        .expect("exec failed");
    assert_eq!(audit_records.len(), 60);

    let deleted_count = audit_records.iter().filter(|r| r.is_deleted).count();
    assert_eq!(deleted_count, 20);

    // Updated records check: indices 0..20 were updated and NOT deleted
    let updated_encrypted = skybase
        .collection("com.notes.secure.entry")
        .expect("col failed")
        .where_json("encrypted", QueryOp::Eq, true)
        .execute()
        .expect("exec failed");
    assert_eq!(updated_encrypted.len(), 20); // 0..20 active and encrypted; 20..30 encrypted but deleted

    // Drain events from broadcast subscriber and verify presence
    let mut upsert_events = 0;
    let mut delete_events = 0;
    while let Ok(event) = bus_sub.try_recv() {
        match event {
            ChangeNotification::Upsert(_) => upsert_events += 1,
            ChangeNotification::Delete { .. } => delete_events += 1,
        }
    }
    assert!(
        upsert_events > 0,
        "Subscriber must have received Upsert notifications"
    );
    assert!(
        delete_events > 0,
        "Subscriber must have received Delete notifications"
    );

    // Clean shutdown
    consumer_handle.stop();
    consumer_handle.join().await.expect("join failed");
}

// ============================================================================
// 3. Dynamic Store Lifecycles: In-Memory, File-Backed WAL, and Store Swapping
// ============================================================================

/// Tests switching and opening stores dynamically under load on the `Skybase` facade.
#[tokio::test]
async fn test_challenger_dynamic_store_lifecycle_switching_under_load() {
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Lifecycle App",
    );

    let mut skybase = Skybase::in_memory(config).expect("init in_memory failed");

    // 1. Initial Store: Populate with 30 records
    let initial_store = skybase.require_store().expect("store").clone();
    for i in 0..30 {
        initial_store
            .upsert_record(&RecordInput::new(
                "did:plc:user1",
                "app.bsky.feed.post",
                format!("r_{i}"),
                format!("cid_{i}"),
                json!({ "text": format!("Store1 post {i}") }),
                1000 + i,
            ))
            .expect("upsert failed");
    }
    assert_eq!(
        skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len(),
        30
    );

    // 2. Dynamically replace with a new in-memory store
    let new_mem_store = skybase
        .open_in_memory_store()
        .expect("open_in_memory_store failed");
    assert_eq!(
        skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len(),
        0,
        "New store must be fresh and empty"
    );

    // Write 25 records to the new store
    for i in 0..25 {
        new_mem_store
            .upsert_record(&RecordInput::new(
                "did:plc:user2",
                "app.bsky.feed.post",
                format!("new_r_{i}"),
                format!("new_cid_{i}"),
                json!({ "text": format!("Store2 post {i}") }),
                2000 + i,
            ))
            .expect("upsert failed");
    }
    assert_eq!(
        skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len(),
        25
    );

    // Old store still holds its original 30 records independently
    assert_eq!(
        initial_store
            .query("app.bsky.feed.post")
            .execute()
            .unwrap()
            .len(),
        30
    );

    // 3. Dynamically replace with a persistent file-backed SQLite store
    let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
    let db_path = temp_dir.path().join("challenger_lifecycle.db");

    let file_store = skybase.open_store(&db_path).expect("open_store failed");
    assert_eq!(
        skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len(),
        0
    );

    // Write 40 records to persistent store
    for i in 0..40 {
        file_store
            .upsert_record(&RecordInput::new(
                "did:plc:user3",
                "app.bsky.feed.post",
                format!("file_r_{i}"),
                format!("file_cid_{i}"),
                json!({ "text": format!("File post {i}") }),
                3000 + i,
            ))
            .expect("upsert failed");
    }
    assert_eq!(
        skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len(),
        40
    );

    // 4. Create a second independent Skybase instance pointing to the same file path
    let persistent_config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Second Instance",
    )
    .with_storage_path(&db_path);

    let second_skybase = Skybase::new(persistent_config).expect("second instance init failed");
    let persisted_records = second_skybase
        .collection("app.bsky.feed.post")
        .expect("col failed")
        .execute()
        .expect("exec failed");

    assert_eq!(
        persisted_records.len(),
        40,
        "File-backed store must persist data across Skybase facade instances"
    );
}

// ============================================================================
// 4. Fail-Closed & Robust Error Handling: Unconfigured Store & Bad Inputs
// ============================================================================

/// Verifies that all operations fail-closed with strongly-typed `SkybaseError::Config`
/// and zero panics when storage or endpoints are not configured.
#[test]
fn test_challenger_fail_closed_unconfigured_components() {
    // 1. Completely unconfigured storage
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Unconfigured App",
    );
    let skybase = Skybase::new(config).expect("init failed");

    assert!(skybase.store().is_none());
    assert!(matches!(
        skybase.require_store(),
        Err(SkybaseError::Config(_))
    ));
    assert!(matches!(skybase.query("col"), Err(SkybaseError::Config(_))));
    assert!(matches!(
        skybase.collection("col"),
        Err(SkybaseError::Config(_))
    ));
    assert!(matches!(skybase.subscribe(), Err(SkybaseError::Config(_))));
    assert!(matches!(skybase.consumer(), Err(SkybaseError::Config(_))));

    let cancel = CancellationToken::new();
    assert!(matches!(
        skybase.start_consumer(cancel.clone()),
        Err(SkybaseError::Config(_))
    ));

    // 2. Storage configured, but missing jetstream_endpoint
    let config_with_store = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "No Endpoint App",
    )
    .with_in_memory_store();

    let skybase_with_store = Skybase::new(config_with_store).expect("init failed");
    assert!(skybase_with_store.store().is_some());
    assert!(skybase_with_store.query("col").is_ok());

    // Consumer requires jetstream_endpoint
    let consumer_res = skybase_with_store.consumer();
    assert!(
        matches!(consumer_res, Err(SkybaseError::Config(msg)) if msg.contains("jetstream_endpoint"))
    );

    let start_res = skybase_with_store.start_consumer(cancel);
    assert!(
        matches!(start_res, Err(SkybaseError::Config(msg)) if msg.contains("jetstream_endpoint"))
    );

    // 3. Invalid OAuth client configuration validation
    let empty_client = SkybaseConfig::new("", "https://app.example.com/callback", "App");
    assert!(matches!(
        Skybase::new(empty_client),
        Err(SkybaseError::Config(_))
    ));

    let whitespace_client = SkybaseConfig::new("   ", "https://app.example.com/callback", "App");
    assert!(matches!(
        Skybase::new(whitespace_client),
        Err(SkybaseError::Config(_))
    ));

    let empty_redirect = SkybaseConfig::new("https://app.example.com/client.json", "", "App");
    assert!(matches!(
        Skybase::new(empty_redirect),
        Err(SkybaseError::Config(_))
    ));
}

// ============================================================================
// 5. Sovereign PDS Client Nonce Challenge & Automatic Retry via Facade
// ============================================================================

/// Tests that the sovereign client created via `Skybase::repo_client_from_credentials`
/// automatically handles RFC 9449 HTTP 401 `use_dpop_nonce` challenges and retries transparently.
#[tokio::test]
async fn test_challenger_pds_client_automatic_dpop_nonce_retry_via_facade() {
    let mock_pds = MockPdsServer::start().await;

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Nonce Test App",
    );
    let skybase = Skybase::new(config).expect("init failed");

    let pds_client = skybase
        .repo_client_from_credentials(mock_pds.uri(), "did:plc:nonce_challenger", "token_xyz")
        .expect("repo client creation failed");

    // Mount one-time 401 nonce challenge on Mock PDS
    mock_pds
        .mount_nonce_challenge_once("nonce-challenge-round-1")
        .await;

    let payload = json!({
        "$type": "app.bsky.feed.post",
        "text": "Hello, challenge test with nonce recovery!"
    });

    // createRecord must succeed by catching 401, extracting nonce, and retrying once
    let create_res = pds_client
        .create_record("app.bsky.feed.post", Some("post_nonce_1"), &payload, true)
        .await
        .expect("createRecord should automatically recover from 401 nonce challenge");

    assert_eq!(
        create_res.uri,
        "at://did:plc:nonce_challenger/app.bsky.feed.post/post_nonce_1"
    );

    // Subsequent deleteRecord should succeed with the established session
    let delete_res = pds_client
        .delete_record("app.bsky.feed.post", "post_nonce_1")
        .await;
    assert!(delete_res.is_ok(), "deleteRecord should succeed");
}

// ============================================================================
// 6. Broadcast Bus Multi-Subscriber Fanout & Lag Contention
// ============================================================================

/// Tests high-fanout broadcast delivery with 20 simultaneous subscribers:
/// 10 "fast" subscribers reading immediately, and 10 "slow" subscribers waiting until buffer saturation.
/// Asserts slow subscribers receive `Lagged` without crashing or blocking the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_broadcast_fanout_slow_and_fast_subscribers() {
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Bus App",
    );
    let skybase = Skybase::in_memory(config).expect("init failed");

    // 10 Fast subscribers
    let mut fast_subs = Vec::new();
    for _ in 0..10 {
        fast_subs.push(skybase.subscribe().expect("subscribe fast"));
    }

    // 10 Slow subscribers (held unread)
    let mut slow_subs = Vec::new();
    for _ in 0..10 {
        slow_subs.push(skybase.subscribe().expect("subscribe slow"));
    }

    let fast_received = Arc::new(AtomicUsize::new(0));
    let is_bursting = Arc::new(AtomicBool::new(true));

    // Spawn fast subscriber drains
    let mut fast_tasks = JoinSet::new();
    for mut rx in fast_subs {
        let count_clone = Arc::clone(&fast_received);
        let bursting_clone = Arc::clone(&is_bursting);

        fast_tasks.spawn(async move {
            while bursting_clone.load(Ordering::Relaxed) {
                if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(20), rx.recv()).await
                {
                    count_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    // Burst 1,200 records into store (channel capacity is 1,024)
    let total_burst = 1_200;
    let store = skybase.require_store().expect("store");
    for i in 0..total_burst {
        store
            .upsert_record(&RecordInput::new(
                "did:plc:burst",
                "app.bsky.feed.like",
                format!("like_{i}"),
                format!("cid_{i}"),
                json!({ "subject": format!("post_{i}") }),
                i as u64,
            ))
            .expect("upsert failed");
    }

    // Stop fast subscribers
    is_bursting.store(false, Ordering::Relaxed);
    while let Some(res) = fast_tasks.join_next().await {
        res.expect("fast task panicked");
    }

    assert!(
        fast_received.load(Ordering::Relaxed) > 0,
        "Fast subscribers must receive events in flight"
    );

    // Verify all 10 slow subscribers received Lagged error cleanly
    for (idx, mut rx) in slow_subs.into_iter().enumerate() {
        let first_recv = rx.try_recv();
        assert!(
            matches!(first_recv, Err(broadcast::error::TryRecvError::Lagged(_))),
            "Slow subscriber {idx} must encounter Lagged on buffer overflow"
        );
    }

    // Verify state can be completely re-synced via QueryBuilder
    let all_likes = skybase
        .collection("app.bsky.feed.like")
        .expect("col failed")
        .execute()
        .expect("query failed");
    assert_eq!(all_likes.len(), total_burst);
}

// ============================================================================
// 7. Rich JSON1 Querying: Operators, LIKE Patterns, and SQL Injection Immunity
// ============================================================================

/// Tests rich JSON1 querying capabilities including `QueryOp::Like`, numeric boundaries,
/// deeply nested structures, and adversarial SQL injection payloads.
#[test]
fn test_challenger_query_json1_complex_filtering_and_like_patterns() {
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Query Stress App",
    );
    let skybase = Skybase::in_memory(config).expect("init failed");
    let store = skybase.require_store().expect("store");

    let entries = [
        (
            "user1",
            "r1",
            json!({"item": "Quantum Laptop", "price": 1200, "meta": {"city": "New York", "featured": true}}),
            100,
        ),
        (
            "user1",
            "r2",
            json!({"item": "Quantum Phone", "price": 800, "meta": {"city": "San Francisco", "featured": false}}),
            200,
        ),
        (
            "user2",
            "r3",
            json!({"item": "Classic Watch", "price": 250, "meta": {"city": "New York", "featured": true}}),
            300,
        ),
        (
            "user2",
            "r4",
            json!({"item": "Digital Tablet", "price": 600, "meta": {"city": "London", "featured": true}}),
            400,
        ),
        (
            "user3",
            "r5",
            json!({"item": "Quantum Keyboard", "price": 150, "meta": {"city": "Berlin", "featured": false}}),
            500,
        ),
    ];

    for (user, rkey, json_val, ts) in entries {
        store
            .upsert_record(&RecordInput::new(
                format!("did:plc:{user}"),
                "com.catalog.product",
                rkey,
                format!("cid_{rkey}"),
                json_val,
                ts,
            ))
            .expect("upsert failed");
    }

    // 1. LIKE substring match
    let quantum_items = skybase
        .collection("com.catalog.product")
        .unwrap()
        .where_json("item", QueryOp::Like, "%Quantum%")
        .execute()
        .unwrap();
    assert_eq!(quantum_items.len(), 3);

    // 2. LIKE prefix match
    let digital_items = skybase
        .collection("com.catalog.product")
        .unwrap()
        .where_json("item", QueryOp::Like, "Digital%")
        .execute()
        .unwrap();
    assert_eq!(digital_items.len(), 1);

    // 3. Combined JSON1 filters: numeric range + nested path + LIKE pattern
    let filtered = skybase
        .collection("com.catalog.product")
        .unwrap()
        .where_json("price", QueryOp::Gte, 500)
        .where_json("meta.featured", QueryOp::Eq, true)
        .where_json("meta.city", QueryOp::Like, "%York%")
        .execute()
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].rkey, "r1");

    // 4. SQL Injection payload resistance
    let injection_payloads = [
        "' OR '1'='1",
        "'; DROP TABLE records; --",
        "\" OR \"\"=\"",
        "1; SELECT * FROM records",
    ];

    for payload in injection_payloads {
        let safe_res = skybase
            .collection("com.catalog.product")
            .unwrap()
            .where_json("item", QueryOp::Like, payload)
            .execute();
        assert!(
            safe_res.is_ok(),
            "SQL injection payload should not cause syntax or query error"
        );
        assert_eq!(safe_res.unwrap().len(), 0);
    }

    // Verify records table was not dropped or corrupted
    let count_after = skybase
        .collection("com.catalog.product")
        .unwrap()
        .execute()
        .unwrap()
        .len();
    assert_eq!(count_after, 5);
}

// ============================================================================
// 8. Consumer Resilience: Malformed Frame Interleaving
// ============================================================================

/// Tests that `JetstreamConsumer` survives corrupted, truncated, and non-commit frames
/// interleaved with valid commit frames, maintaining steady ingestion.
#[tokio::test]
async fn test_challenger_consumer_resilience_to_malformed_frames_interleaved_with_valid_commits() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("Mock Jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Resilient Consumer App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("app.bsky.feed.post");

    let skybase = Skybase::new(config).expect("init failed");
    let cancel = CancellationToken::new();
    let consumer_handle = skybase
        .start_consumer(cancel.clone())
        .expect("start failed");

    // Wait for consumer WebSocket connection
    let start_wait = Instant::now();
    while mock_jetstream.active_connections() == 0 {
        if start_wait.elapsed() > Duration::from_secs(5) {
            panic!("Timed out waiting for consumer to connect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let total_valid = 50;
    let base_time_us = 1_726_300_000_000_000u64;

    for i in 0..total_valid {
        // 1. Emit valid commit
        let commit = JetstreamCommit {
            did: "did:plc:resilient_user".to_string(),
            time_us: base_time_us + i,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("valid_post_{i}"),
            operation: CommitOperation::Create,
            cid: Some(format!("bafy_cid_{i}")),
            record: Some(json!({ "text": format!("Valid post #{i}") })),
        };
        mock_jetstream
            .emit_commit(&commit)
            .expect("emit valid failed");

        // 2. Interleave malformed or non-commit frames
        if i % 5 == 0 {
            let _ = mock_jetstream.emit_raw("{not valid json syntax at all!");
        } else if i % 5 == 1 {
            let _ = mock_jetstream.emit_raw(r#"{"kind": "unknown_future_kind", "payload": 123}"#);
        } else if i % 5 == 2 {
            let _ = mock_jetstream.emit_heartbeat(base_time_us + i);
        } else if i % 5 == 3 {
            let _ = mock_jetstream.emit_raw(r#"{"kind": "commit"}"#); // Missing commit details
        }
    }

    // Wait for consumer to process frames
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut ingested = 0;
    while Instant::now() < deadline {
        let res = skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap();
        ingested = res.len();
        if ingested >= total_valid as usize {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        ingested, total_valid as usize,
        "Consumer must ingest all {total_valid} valid commits despite malformed frames"
    );

    consumer_handle.stop();
    consumer_handle.join().await.expect("join failed");
}

// ============================================================================
// 9. Consumer Teardown, Clean Cancellation & Reconfiguration Lifecycle
// ============================================================================

/// Tests starting, stopping, and restarting the `JetstreamConsumer` on the same `Skybase` instance.
#[tokio::test]
async fn test_challenger_consumer_graceful_cancellation_and_restart() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("Mock Jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Restart App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("app.bsky.feed.post");

    let skybase = Skybase::new(config).expect("init failed");

    // Cycle 1: Start consumer 1
    let cancel1 = CancellationToken::new();
    let handle1 = skybase
        .start_consumer(cancel1.clone())
        .expect("start 1 failed");

    let start_wait1 = Instant::now();
    while mock_jetstream.active_connections() == 0 {
        if start_wait1.elapsed() > Duration::from_secs(5) {
            panic!("Timed out waiting for consumer 1 connection");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Emit 25 commits
    for i in 0..25 {
        mock_jetstream
            .emit_commit(&JetstreamCommit {
                did: "did:plc:cycle_user".to_string(),
                time_us: 1000 + i,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("cycle1_{i}"),
                operation: CommitOperation::Create,
                cid: Some(format!("cid1_{i}")),
                record: Some(json!({ "text": format!("Cycle 1 text {i}") })),
            })
            .expect("emit failed");
    }

    // Wait for cycle 1 to ingest
    let deadline1 = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline1 {
        let count = skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len();
        if count >= 25 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len(),
        25
    );

    // Stop consumer 1 cleanly
    handle1.stop();
    handle1.join().await.expect("handle1 join failed");

    // Wait for server to see connection drop before cycle 2
    let drop_wait = Instant::now();
    while mock_jetstream.active_connections() > 0 {
        if drop_wait.elapsed() > Duration::from_secs(3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Cycle 2: Start consumer 2 on the SAME Skybase instance
    let cancel2 = CancellationToken::new();
    let handle2 = skybase
        .start_consumer(cancel2.clone())
        .expect("start 2 failed");

    let start_wait2 = Instant::now();
    while mock_jetstream.active_connections() == 0 {
        if start_wait2.elapsed() > Duration::from_secs(5) {
            panic!("Timed out waiting for consumer 2 connection");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Emit 25 more commits
    for i in 0..25 {
        mock_jetstream
            .emit_commit(&JetstreamCommit {
                did: "did:plc:cycle_user".to_string(),
                time_us: 2000 + i,
                collection: "app.bsky.feed.post".to_string(),
                rkey: format!("cycle2_{i}"),
                operation: CommitOperation::Create,
                cid: Some(format!("cid2_{i}")),
                record: Some(json!({ "text": format!("Cycle 2 text {i}") })),
            })
            .expect("emit failed");
    }

    // Wait for cycle 2 to ingest
    let deadline2 = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline2 {
        let count = skybase
            .collection("app.bsky.feed.post")
            .unwrap()
            .execute()
            .unwrap()
            .len();
        if count >= 50 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Verify cumulative records in store
    let total_records = skybase
        .collection("app.bsky.feed.post")
        .unwrap()
        .execute()
        .unwrap()
        .len();
    assert_eq!(
        total_records, 50,
        "Cumulative records across consumer restarts must be 50"
    );

    handle2.stop();
    handle2.join().await.expect("handle2 join failed");
}

// ============================================================================
// 10. High-Concurrency Mixed Read/Write/Delete Race on File-Backed SQLite WAL
// ============================================================================

/// Hammers a persistent SQLite WAL database across 20 concurrent tasks executing mixed
/// upserts, soft-deletes, and JSON1 queries simultaneously.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_file_backed_wal_concurrent_hammer_with_facade() {
    let temp_dir = tempfile::tempdir().expect("tempdir failed");
    let db_path = temp_dir.path().join("wal_hammer.db");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Hammer App",
    )
    .with_storage_path(&db_path);

    let skybase = Skybase::new(config).expect("init failed");
    let is_running = Arc::new(AtomicBool::new(true));

    let total_tasks = 20;
    let mut tasks = JoinSet::new();

    for task_idx in 0..total_tasks {
        let skybase_clone = skybase.clone();
        let running_clone = Arc::clone(&is_running);

        tasks.spawn(async move {
            let mut ops = 0;
            let did = format!("did:plc:hammer_{task_idx}");

            while running_clone.load(Ordering::Relaxed) {
                let op_type = ops % 3;
                match op_type {
                    0 => {
                        // Upsert
                        let rkey = format!("k_{ops}");
                        let input = RecordInput::new(
                            &did,
                            "com.hammer.item",
                            &rkey,
                            format!("cid_{ops}"),
                            json!({ "seq": ops, "score": ops * 10, "label": format!("item_{ops}") }),
                            ops as u64,
                        );
                        let _ = skybase_clone.require_store().unwrap().upsert_record(&input);
                    }
                    1 => {
                        // Soft delete an older item
                        if ops > 0 {
                            let del_uri = format!("at://{did}/com.hammer.item/k_{}", ops - 1);
                            let _ = skybase_clone.require_store().unwrap().soft_delete_record(&del_uri, ops as u64);
                        }
                    }
                    _ => {
                        // Query with JSON1
                        let _ = skybase_clone
                            .collection("com.hammer.item")
                            .unwrap()
                            .where_json("score", QueryOp::Gte, 50)
                            .where_json("label", QueryOp::Like, "item_%")
                            .limit(10)
                            .execute();
                    }
                }

                ops += 1;
                if ops % 20 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            ops
        });
    }

    // Run the hammer for 1 second
    tokio::time::sleep(Duration::from_millis(1000)).await;
    is_running.store(false, Ordering::Relaxed);

    let mut total_ops = 0;
    while let Some(res) = tasks.join_next().await {
        total_ops += res.expect("task panicked");
    }

    assert!(
        total_ops > 100,
        "Should have completed significant operations under WAL hammer"
    );

    // Verify SQLite WAL integrity check
    let store = skybase.require_store().unwrap();
    let rows = store
        .query("com.hammer.item")
        .include_deleted(true)
        .execute()
        .unwrap();
    assert!(!rows.is_empty(), "Store must contain records after hammer");
}
