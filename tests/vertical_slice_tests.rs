//! End-to-End Tests: Tier 4 Real-World Application Scenarios & Hermetic Vertical Slice.
//!
//! Connects:
//! DPoP Auth -> Sovereign PDS Write -> Mock Jetstream Emit -> Ingest Sync -> SQLite WAL Storage -> JSON1 Query -> Broadcast Bus.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::MockPdsServer;
use serde_json::json;
use skybase::{
    build_subscription_url, CancellationToken, ChangeNotification, CommitOperation, CursorTracker,
    JetstreamCommit, MockJetstreamServer, QueryOp, RecordInput, RecordStore, Skybase,
    SkybaseConfig,
};

// ============================================================================
// Tier 4: Real-World Application Scenarios (Vertical Slice Integration)
// ============================================================================

/// Scenario 1: Complete Closed-Loop Decentralized Micro-AppView Workflow.
///
/// Steps:
/// 1. Sovereign PDS client creates record with DPoP authentication on Mock PDS.
/// 2. Mock Jetstream server emits commit frame over WebSocket firehose.
/// 3. Ingest syncs frame into SQLite WAL storage.
/// 4. QueryBuilder retrieves record using JSON1 filter.
/// 5. In-memory broadcast subscriber receives live notification with full payload.
#[tokio::test]
async fn test_tier4_scenario1_complete_vertical_slice_loop() {
    // 1. Initialize Subsystems
    let mock_pds = MockPdsServer::start().await;
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("jetstream start failed");

    let user_did = "did:plc:alice_vertical";
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Vertical Slice App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("com.foodapp.restaurant.review");

    let skybase = Skybase::new(config).expect("skybase facade init failed");
    let mut live_query_sub = skybase.subscribe().expect("subscribe failed");

    let pds_client = skybase
        .repo_client_from_credentials(mock_pds.uri(), user_did, "oauth_dpop_token_alice")
        .expect("pds repo client creation failed");

    // 2. Step 1: Sovereign PDS Record Creation with DPoP
    let review_payload = json!({
        "$type": "com.foodapp.restaurant.review",
        "restaurantName": "The Decentralized Bistro",
        "rating": 5,
        "reviewText": "Superb atmosphere and zero custodial lock-in!",
        "createdAt": "2026-09-12T00:30:00Z"
    });

    let pds_res = pds_client
        .create_record(
            "com.foodapp.restaurant.review",
            Some("rev_bistro_1"),
            &review_payload,
            true,
        )
        .await
        .expect("PDS createRecord failed");

    assert_eq!(
        pds_res.uri,
        "at://did:plc:alice_vertical/com.foodapp.restaurant.review/rev_bistro_1"
    );

    // 3. Step 2 & 3: Jetstream Firehose Emission & Consumer Ingest Sync
    let cancel = CancellationToken::new();
    let consumer_handle = skybase
        .start_consumer(cancel.clone())
        .expect("start_consumer failed");

    let event_time_us = 1726099200000000u64;
    let commit = JetstreamCommit {
        did: user_did.to_string(),
        time_us: event_time_us,
        collection: "com.foodapp.restaurant.review".to_string(),
        rkey: "rev_bistro_1".to_string(),
        operation: CommitOperation::Create,
        cid: Some(pds_res.cid.clone()),
        record: Some(review_payload.clone()),
    };

    // Brief yield to allow consumer connection handshake with mock Jetstream server
    tokio::time::sleep(Duration::from_millis(100)).await;
    mock_jetstream.emit_commit(&commit).expect("emit failed");

    // 4. Step 5: Live Query Broadcast Event Verification
    let live_notification = tokio::time::timeout(Duration::from_secs(5), live_query_sub.recv())
        .await
        .expect("timed out waiting for live notification")
        .expect("broadcast receive failed");

    match live_notification {
        ChangeNotification::Upsert(row) => {
            assert_eq!(row.uri, pds_res.uri);
            assert_eq!(
                row.record_json["reviewText"],
                "Superb atmosphere and zero custodial lock-in!"
            );
        }
        _ => panic!("Expected ChangeNotification::Upsert"),
    }

    // 5. Step 4: Query Engine Retrieval with JSON1 Filtering
    let high_rated_reviews = skybase
        .collection("com.foodapp.restaurant.review")
        .expect("collection query failed")
        .where_json("rating", QueryOp::Gte, 4)
        .where_json("restaurantName", QueryOp::Like, "%Decentralized Bistro%")
        .execute()
        .expect("query failed");

    assert_eq!(high_rated_reviews.len(), 1);
    let matched = &high_rated_reviews[0];
    assert_eq!(matched.uri, pds_res.uri);
    assert_eq!(matched.cid, pds_res.cid);
    assert_eq!(matched.record_json["rating"], 5);

    // Clean shutdown of consumer task
    consumer_handle.stop();
    consumer_handle.join().await.expect("consumer join failed");
}

/// Scenario 2: Multi-Tenant Multi-Collection Firehose Indexing.
///
/// Ingests records across multiple DIDs and collections, verifying query isolation.
#[tokio::test]
async fn test_tier4_scenario2_multi_tenant_multi_collection_indexing() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    let tenants = ["alice", "bob", "charlie", "david", "eve"];

    for (i, tenant) in tenants.iter().enumerate() {
        let did = format!("did:plc:{tenant}");

        // Post record
        store
            .upsert_record(&RecordInput::new(
                did.clone(),
                "app.bsky.feed.post",
                format!("p_{tenant}"),
                format!("cid_post_{i}"),
                json!({ "text": format!("Post from {tenant}"), "author": tenant }),
                1000 + i as u64,
            ))
            .expect("post upsert failed");

        // Profile record
        store
            .upsert_record(&RecordInput::new(
                did.clone(),
                "app.bsky.actor.profile",
                "self",
                format!("cid_prof_{i}"),
                json!({ "displayName": tenant.to_uppercase(), "followerCount": (i + 1) * 10 }),
                1000 + i as u64,
            ))
            .expect("profile upsert failed");
    }

    // Verify collection isolation: posts vs profiles
    let all_posts = store.collection("app.bsky.feed.post").execute().unwrap();
    assert_eq!(all_posts.len(), 5);

    let all_profiles = store
        .collection("app.bsky.actor.profile")
        .execute()
        .unwrap();
    assert_eq!(all_profiles.len(), 5);

    // Query specific tenant profile with JSON1 filter
    let bob_profile = store
        .collection("app.bsky.actor.profile")
        .did("did:plc:bob")
        .execute()
        .unwrap();

    assert_eq!(bob_profile.len(), 1);
    assert_eq!(bob_profile[0].record_json["displayName"], "BOB");
    assert_eq!(bob_profile[0].record_json["followerCount"], 20);
}

/// Scenario 3: Sovereign Record Lifecycle with Deletion & Tombstone Audit.
#[tokio::test]
async fn test_tier4_scenario3_lifecycle_with_deletion_and_audit() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let mut bus_sub = store.subscribe();

    let uri = "at://did:plc:audit_user/com.myapp.note/note_1";

    // 1. Create
    store
        .upsert_record(&RecordInput::new(
            "did:plc:audit_user",
            "com.myapp.note",
            "note_1",
            "cid_note_v1",
            json!({ "title": "Secret Note", "encrypted": true }),
            100,
        ))
        .unwrap();

    let notif1 = bus_sub.try_recv().unwrap();
    assert!(matches!(notif1, ChangeNotification::Upsert(_)));

    // Active in regular queries
    let notes = store.collection("com.myapp.note").execute().unwrap();
    assert_eq!(notes.len(), 1);

    // 2. Update
    store
        .upsert_record(&RecordInput::new(
            "did:plc:audit_user",
            "com.myapp.note",
            "note_1",
            "cid_note_v2",
            json!({ "title": "Secret Note Updated", "encrypted": true }),
            200,
        ))
        .unwrap();

    let notif2 = bus_sub.try_recv().unwrap();
    assert!(matches!(notif2, ChangeNotification::Upsert(_)));
    let updated_notes = store.collection("com.myapp.note").execute().unwrap();
    assert_eq!(updated_notes[0].cid, "cid_note_v2");

    // 3. Delete
    store.soft_delete_record(uri).unwrap();
    let del_notification = bus_sub.try_recv().unwrap();
    assert!(matches!(
        del_notification,
        ChangeNotification::Delete { .. }
    ));

    // Excluded from standard queries
    let active_after_del = store.collection("com.myapp.note").execute().unwrap();
    assert_eq!(active_after_del.len(), 0);

    // Present in audit queries with include_deleted(true)
    let audit_query = store
        .collection("com.myapp.note")
        .include_deleted(true)
        .execute()
        .unwrap();
    assert_eq!(audit_query.len(), 1);
    assert!(audit_query[0].is_deleted);
}

/// Scenario 4: Fast Stream and Slow Consumer Lag Handling.
#[tokio::test]
async fn test_tier4_scenario4_slow_consumer_lag_handling() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let mut slow_sub = store.subscribe();

    // Overflow broadcast capacity (channel capacity is 1024)
    for i in 1..=1100 {
        store
            .upsert_record(&RecordInput::new(
                "did:plc:burst",
                "burst.col",
                i.to_string(),
                format!("cid_{i}"),
                json!({ "seq": i }),
                i as u64,
            ))
            .unwrap();
    }

    // Slow subscriber receives Lagged error without crashing or blocking the store
    let recv_result = slow_sub.try_recv();
    assert!(
        matches!(
            recv_result,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
        ),
        "Slow subscriber must receive Lagged without crashing"
    );

    // Slow subscriber re-syncs state cleanly via QueryBuilder
    let count = store.collection("burst.col").execute().unwrap().len();
    assert_eq!(count, 1100);
}

/// Scenario 5: Monotonic Cursor Resumption After Connection Drop.
#[tokio::test]
async fn test_tier4_scenario5_cursor_resumption_after_reconnect() {
    let tracker = Arc::new(CursorTracker::new(0));

    // Phase 1: Ingest batch 1
    for t in 1..=10 {
        tracker.update(t * 1000);
    }
    assert_eq!(tracker.get(), 10000);

    // Reconnection parameter formulation using production build_subscription_url
    let resume_cursor = tracker.get();
    let reconnect_url = build_subscription_url(
        "wss://jetstream.example.com/subscribe",
        &["app.bsky.feed.post".to_string()],
        Some(resume_cursor),
    );
    assert!(reconnect_url.contains("cursor=10000"));
    assert!(reconnect_url.contains("wantedCollections=app.bsky.feed.post"));

    // Phase 2: Ingest subsequent batch after reconnection
    for t in 11..=20 {
        tracker.update(t * 1000);
    }
    assert_eq!(tracker.get(), 20000);
}
