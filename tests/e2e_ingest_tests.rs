//! End-to-End Tests: Jetstream Firehose Ingestion, Cursor Tracking, Backoff & Resiliency.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::json;
use skybase::index::{ChangeNotification, RecordInput, RecordStore};
use skybase::ingest::{
    BackoffManager, CommitOperation, CursorTracker, JetstreamCommit, MockJetstreamServer,
};
use tokio_tungstenite::connect_async;

// ============================================================================
// Tier 1: Feature Coverage (WebSocket connection, Commit Parsing, Cursor, Sync)
// ============================================================================

#[tokio::test]
async fn test_tier1_f01_mock_jetstream_server_connection_and_subscription_params() {
    let server = MockJetstreamServer::start().await.expect("start failed");
    let target_url = format!(
        "{}?wantedCollections=app.bsky.feed.post&wantedCollections=com.example.review&cursor=1700000000000000",
        server.ws_url()
    );

    let (ws_stream, _) = connect_async(&target_url)
        .await
        .expect("WebSocket connection failed");
    let stream = ws_stream;

    // Verify query parameters were recorded by mock server
    tokio::time::sleep(Duration::from_millis(50)).await;
    let queries = server.query_history();
    assert!(!queries.is_empty());
    let query_str = &queries[0];
    assert!(query_str.contains("wantedCollections=app.bsky.feed.post"));
    assert!(query_str.contains("wantedCollections=com.example.review"));
    assert!(query_str.contains("cursor=1700000000000000"));

    drop(stream);
}

#[tokio::test]
async fn test_tier1_f02_commit_event_emission_and_frame_deserialization() {
    let server = MockJetstreamServer::start().await.expect("start failed");
    let (ws_stream, _) = connect_async(&server.ws_url())
        .await
        .expect("connect failed");
    let mut stream = ws_stream;

    let commit = JetstreamCommit {
        did: "did:plc:alice123".to_string(),
        time_us: 1710000000123456,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "post_abc".to_string(),
        operation: CommitOperation::Create,
        cid: Some("bafyreicid1".to_string()),
        record: Some(json!({ "text": "Testing Jetstream commit parsing" })),
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    server.emit_commit(&commit).expect("emit failed");

    let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting for frame")
        .expect("stream ended")
        .expect("frame error");

    let text = msg.to_text().expect("not text");
    let parsed: serde_json::Value = serde_json::from_str(text).expect("invalid json");

    assert_eq!(parsed["kind"], "commit");
    assert_eq!(parsed["did"], "did:plc:alice123");
    assert_eq!(parsed["time_us"], 1710000000123456u64);
    assert_eq!(parsed["commit"]["operation"], "create");
    assert_eq!(parsed["commit"]["collection"], "app.bsky.feed.post");
    assert_eq!(parsed["commit"]["rkey"], "post_abc");
    assert_eq!(parsed["commit"]["cid"], "bafyreicid1");
    assert_eq!(
        parsed["commit"]["record"]["text"],
        "Testing Jetstream commit parsing"
    );
}

#[tokio::test]
async fn test_tier1_f03_monotonic_cursor_tracker_progression() {
    let tracker = CursorTracker::new(100);
    assert_eq!(tracker.get(), 100);

    // Advancing cursor succeeds
    assert!(tracker.update(200));
    assert_eq!(tracker.get(), 200);

    assert!(tracker.update(250));
    assert_eq!(tracker.get(), 250);

    // Stale or equal timestamp does not advance watermark
    assert!(!tracker.update(250));
    assert_eq!(tracker.get(), 250);

    assert!(!tracker.update(150));
    assert_eq!(tracker.get(), 250);
}

#[tokio::test]
async fn test_tier1_f04_heartbeat_timestamp_advancement() {
    let server = MockJetstreamServer::start().await.expect("start failed");
    let (ws_stream, _) = connect_async(&server.ws_url())
        .await
        .expect("connect failed");
    let mut stream = ws_stream;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let heartbeat_time = 1720000000999999u64;
    server.emit_heartbeat(heartbeat_time).expect("emit failed");

    let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("frame error");

    let parsed: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(parsed["time_us"], heartbeat_time);
    assert!(parsed.get("commit").is_none());

    let tracker = CursorTracker::new(0);
    tracker.update(parsed["time_us"].as_u64().unwrap());
    assert_eq!(tracker.get(), heartbeat_time);
}

#[tokio::test]
async fn test_tier1_f05_ingest_synchronization_to_sqlite_storage() {
    let store = RecordStore::open_in_memory().expect("open failed");
    let tracker = Arc::new(CursorTracker::new(0));

    // Simulate ingestion handler for commit
    let commit = JetstreamCommit {
        did: "did:plc:sync_user".to_string(),
        time_us: 1715000000000000,
        collection: "com.myapp.review".to_string(),
        rkey: "rev_01".to_string(),
        operation: CommitOperation::Create,
        cid: Some("bafyrei_sync_cid".to_string()),
        record: Some(json!({ "rating": 5, "comment": "Excellent AppView" })),
    };

    // Apply commit to storage
    let uri = format!("at://{}/{}/{}", commit.did, commit.collection, commit.rkey);
    store
        .upsert_record(&RecordInput {
            uri: uri.clone(),
            cid: commit.cid.clone().unwrap(),
            did: commit.did.clone(),
            collection: commit.collection.clone(),
            rkey: commit.rkey.clone(),
            record_json: commit.record.clone().unwrap(),
            indexed_at: commit.time_us / 1_000_000,
        })
        .expect("sync upsert failed");
    tracker.update(commit.time_us);

    // Verify record in SQLite
    let stored = store
        .get_record(&uri)
        .expect("get failed")
        .expect("missing");
    assert_eq!(stored.cid, "bafyrei_sync_cid");
    assert_eq!(stored.record_json["rating"], 5);
    assert_eq!(tracker.get(), 1715000000000000);
}

// ============================================================================
// Tier 2: Boundary, Adversarial & Resilience Cases
// ============================================================================

#[tokio::test]
async fn test_tier2_b01_out_of_order_commit_timestamps_and_clock_skew() {
    let tracker = CursorTracker::new(1000);

    // Sequence of timestamps with out-of-order arrivals
    let incoming_timestamps = [1500, 1200, 1800, 1750, 1900, 1600, 2000];

    for ts in incoming_timestamps {
        tracker.update(ts);
    }

    // High watermark must strictly be the maximum observed timestamp
    assert_eq!(tracker.get(), 2000);
}

#[tokio::test]
async fn test_tier2_b02_malformed_and_unparseable_json_frames() {
    let server = MockJetstreamServer::start().await.expect("start failed");
    let (ws_stream, _) = connect_async(&server.ws_url())
        .await
        .expect("connect failed");
    let mut stream = ws_stream;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send corrupted / malformed frames
    server.emit_raw("{ corrupt json:").expect("emit failed");
    server.emit_raw("").expect("emit failed");
    server.emit_raw("null").expect("emit failed");
    server
        .emit_raw(r#"{"kind": "unknown_event_kind", "foo": "bar"}"#)
        .expect("emit failed");

    // Then send a valid commit
    let valid_commit = JetstreamCommit {
        did: "did:plc:valid".to_string(),
        time_us: 1716000000000000,
        collection: "app.bsky.feed.post".to_string(),
        rkey: "rkey_valid".to_string(),
        operation: CommitOperation::Create,
        cid: Some("bafyrei_valid".to_string()),
        record: Some(json!({ "text": "survived bad frames" })),
    };
    server
        .emit_commit(&valid_commit)
        .expect("emit valid failed");

    // Read frames and verify parser can safely handle malformed frames and reach valid frame
    let mut parsed_valid = false;
    for _ in 0..5 {
        if let Ok(Some(Ok(msg))) =
            tokio::time::timeout(Duration::from_millis(200), stream.next()).await
        {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(msg.to_text().unwrap()) {
                if val.get("kind").and_then(|k| k.as_str()) == Some("commit") {
                    parsed_valid = true;
                    break;
                }
            }
        }
    }
    assert!(
        parsed_valid,
        "Parser should cleanly process valid commit following malformed frames"
    );
}

#[test]
fn test_tier2_b03_exponential_backoff_jitter_and_reset() {
    let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));

    assert_eq!(backoff.current_delay(), Duration::from_millis(500));

    // Progression of backoff delays
    let delay1 = backoff.next_backoff();
    // 500ms ±20% jitter = [400ms, 600ms]
    assert!(delay1 >= Duration::from_millis(400) && delay1 <= Duration::from_millis(600));

    let delay2 = backoff.next_backoff();
    // 1000ms ±20% jitter = [800ms, 1200ms]
    assert!(delay2 >= Duration::from_millis(800) && delay2 <= Duration::from_millis(1200));

    // Fast-forward attempts to check max cap
    for _ in 0..10 {
        backoff.next_backoff();
    }
    let capped_delay = backoff.next_backoff();
    // Clamped around max_delay (30s ± 20%)
    assert!(capped_delay <= Duration::from_secs(36));

    // Reset restores initial delay
    backoff.reset();
    assert_eq!(backoff.current_delay(), Duration::from_millis(500));
    assert_eq!(backoff.attempts(), 0);
}

#[tokio::test]
async fn test_tier2_b04_collection_filtering_ignores_unwanted_collections() {
    let store = RecordStore::open_in_memory().expect("open failed");
    let wanted_collections = ["app.bsky.feed.post".to_string()];

    let commits = vec![
        JetstreamCommit {
            did: "did:plc:user".to_string(),
            time_us: 1000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "post1".to_string(),
            operation: CommitOperation::Create,
            cid: Some("cid1".to_string()),
            record: Some(json!({ "text": "wanted" })),
        },
        JetstreamCommit {
            did: "did:plc:user".to_string(),
            time_us: 2000,
            collection: "app.bsky.graph.follow".to_string(),
            rkey: "follow1".to_string(),
            operation: CommitOperation::Create,
            cid: Some("cid2".to_string()),
            record: Some(json!({ "subject": "did:plc:other" })),
        },
    ];

    // Ingestion loop filtering by wanted_collections
    for c in commits {
        if wanted_collections.contains(&c.collection) {
            let uri = format!("at://{}/{}/{}", c.did, c.collection, c.rkey);
            store
                .upsert_record(&RecordInput {
                    uri,
                    cid: c.cid.unwrap(),
                    did: c.did,
                    collection: c.collection,
                    rkey: c.rkey,
                    record_json: c.record.unwrap(),
                    indexed_at: c.time_us,
                })
                .expect("upsert failed");
        }
    }

    // Only wanted post is indexed
    let posts = store.collection("app.bsky.feed.post").execute().unwrap();
    assert_eq!(posts.len(), 1);

    let follows = store.collection("app.bsky.graph.follow").execute().unwrap();
    assert_eq!(follows.len(), 0);
}

// ============================================================================
// Tier 3: Cross-Feature Combinations (Ingest + Storage + Broadcast)
// ============================================================================

#[tokio::test]
async fn test_tier3_p01_ingest_mutation_lifecycle_create_update_delete() {
    let store = RecordStore::open_in_memory().expect("open failed");
    let mut sub = store.subscribe();

    let did = "did:plc:lifecycle";
    let col = "com.myapp.review";
    let rkey = "rev1";
    let uri = format!("at://{did}/{col}/{rkey}");

    // 1. Ingest Create
    store
        .upsert_record(&RecordInput {
            uri: uri.clone(),
            cid: "cid_v1".to_string(),
            did: did.to_string(),
            collection: col.to_string(),
            rkey: rkey.to_string(),
            record_json: json!({ "rating": 3, "text": "Initial" }),
            indexed_at: 1000,
        })
        .expect("create upsert failed");

    let event1 = sub.try_recv().expect("event1 failed");
    match event1 {
        ChangeNotification::Upsert(r) => {
            assert_eq!(r.cid, "cid_v1");
            assert_eq!(r.record_json["rating"], 3);
        }
        _ => panic!("Expected Upsert event"),
    }

    // 2. Ingest Update
    store
        .upsert_record(&RecordInput {
            uri: uri.clone(),
            cid: "cid_v2".to_string(),
            did: did.to_string(),
            collection: col.to_string(),
            rkey: rkey.to_string(),
            record_json: json!({ "rating": 5, "text": "Updated" }),
            indexed_at: 2000,
        })
        .expect("update upsert failed");

    let event2 = sub.try_recv().expect("event2 failed");
    match event2 {
        ChangeNotification::Upsert(r) => {
            assert_eq!(r.cid, "cid_v2");
            assert_eq!(r.record_json["rating"], 5);
        }
        _ => panic!("Expected Upsert event"),
    }

    // 3. Ingest Delete
    store.soft_delete_record(&uri).expect("soft delete failed");
    let event3 = sub.try_recv().expect("event3 failed");
    match event3 {
        ChangeNotification::Delete { uri: u, .. } => assert_eq!(u, uri),
        _ => panic!("Expected Delete event"),
    }

    // Verify storage reflects soft delete
    assert!(store.get_record(&uri).unwrap().is_none());
    let final_record = store.get_record_including_deleted(&uri).unwrap().unwrap();
    assert!(final_record.is_deleted);
}
