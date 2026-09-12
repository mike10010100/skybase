//! Tier 5 Adversarial Coverage Hardening Test Suite (Milestone 4).
//!
//! Empirical challenges stress-testing:
//! 1. Consumer cancellation races, pre-cancelled tokens, and rapid start/abort cycles.
//! 2. Configuration boundary conditions, conflicting options, and store lifecycles.
//! 3. Query engine edge cases, complex JSON1 LIKE patterns, wildcards (`%`, `_`), injection defense, and multi-clause chaining.
//! 4. Batched sync flush guarantees on shutdown and multi-consumer lifecycle chaos.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    missing_docs
)]

mod common;

use std::time::Duration;

use common::MockPdsServer;
use serde_json::json;
use skybase::{
    CancellationToken, CommitOperation, IngesterConfig, JetstreamCommit, MockJetstreamServer,
    QueryOp, RecordInput, RecordStore, RecordStoreConfig, Skybase, SkybaseConfig, SkybaseError,
    SortDirection,
};

// ============================================================================
// Group 1: Rapid Start/Stop & Consumer Cancellation Races
// ============================================================================

/// 1. Pre-cancelled CancellationToken terminates consumer task immediately without network traffic.
#[tokio::test]
async fn test_challenger_consumer_pre_cancelled_token_terminates_immediately() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Pre-cancelled App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store();

    let skybase = Skybase::new(config).expect("facade init failed");

    let cancel = CancellationToken::new();
    cancel.cancel(); // Pre-cancel before starting consumer

    let start_time = tokio::time::Instant::now();
    let handle = skybase
        .start_consumer(cancel)
        .expect("start_consumer failed");

    let join_result = tokio::time::timeout(Duration::from_millis(500), handle.join()).await;

    assert!(
        join_result.is_ok(),
        "Pre-cancelled consumer must exit immediately"
    );
    assert!(
        join_result.unwrap().is_ok(),
        "Consumer join should return Ok(())"
    );
    assert!(
        start_time.elapsed() < Duration::from_millis(200),
        "Elapsed time should be minimal"
    );
}

/// 2. Immediate cancellation after spawn terminates cleanly without panic or task leaks.
#[tokio::test]
async fn test_challenger_immediate_cancellation_after_spawn() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Immediate Cancel App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store();

    let skybase = Skybase::new(config).expect("facade init failed");

    let cancel = CancellationToken::new();
    let handle = skybase
        .start_consumer(cancel.clone())
        .expect("start_consumer failed");

    // Fire stop immediately with 0 delay
    handle.stop();

    let join_res = tokio::time::timeout(Duration::from_secs(2), handle.join()).await;
    assert!(
        join_res.is_ok(),
        "Consumer failed to terminate within 2 seconds"
    );
    assert!(
        join_res.unwrap().is_ok(),
        "Consumer task did not join cleanly"
    );
}

/// 3. Cancellation during active connection negotiation cleanly aborts without hanging.
#[tokio::test]
async fn test_challenger_cancellation_during_connection_handshake() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Handshake Cancel App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store();

    let skybase = Skybase::new(config).expect("facade init failed");

    for _ in 0..5 {
        let cancel = CancellationToken::new();
        let handle = skybase
            .start_consumer(cancel.clone())
            .expect("start_consumer failed");

        // Small random micro-yield to hit connection negotiation window
        tokio::time::sleep(Duration::from_millis(1)).await;
        handle.stop();

        let join_res = tokio::time::timeout(Duration::from_secs(2), handle.join()).await;
        assert!(
            join_res.is_ok(),
            "Consumer did not terminate in connection negotiation"
        );
        assert!(join_res.unwrap().is_ok());
    }
}

/// 4. Cancellation during reconnect backoff sleep against dead/unreachable endpoint wakes up instantly.
#[tokio::test]
async fn test_challenger_cancellation_during_backoff_sleep_against_unreachable_endpoint() {
    let unreachable_endpoint = "ws://127.0.0.1:1/subscribe";

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Unreachable App",
    )
    .with_jetstream_endpoint(unreachable_endpoint)
    .with_in_memory_store();

    let skybase = Skybase::new(config).expect("facade init failed");

    // Configure long initial backoff of 10 seconds
    let ingester_cfg = IngesterConfig::new(unreachable_endpoint)
        .with_backoff(Duration::from_secs(10), Duration::from_secs(30));

    let cancel = CancellationToken::new();
    let handle = skybase
        .start_consumer_with_config(ingester_cfg, cancel.clone())
        .expect("start_consumer_with_config failed");

    // Allow consumer to attempt connection, fail, and enter backoff sleep
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cancel_start = tokio::time::Instant::now();
    handle.stop();

    let join_res = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
    assert!(
        join_res.is_ok(),
        "Consumer backoff sleep did not abort on cancellation"
    );
    assert!(join_res.unwrap().is_ok());
    assert!(
        cancel_start.elapsed() < Duration::from_millis(500),
        "Consumer should break out of 10s sleep promptly on cancellation"
    );
}

/// 5. Rapid start/abort cycles hammer: 50 consecutive starts and stops in rapid succession.
#[tokio::test]
async fn test_challenger_rapid_start_abort_cycles_hammer() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Hammer App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store();

    let skybase = Skybase::new(config).expect("facade init failed");

    for i in 0..50 {
        let cancel = CancellationToken::new();
        let handle = skybase
            .start_consumer(cancel.clone())
            .expect("start_consumer failed");

        if i % 2 == 0 {
            handle.stop();
        } else {
            cancel.cancel();
        }

        let join_res = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            join_res.is_ok(),
            "Iteration {i} failed to join within 1 second"
        );
        assert!(join_res.unwrap().is_ok());
    }
}

/// 6. Mid-flight burst cancellation drains channel and preserves SQLite database consistency.
#[tokio::test]
async fn test_challenger_mid_flight_burst_cancellation_drains_and_preserves_consistency() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Burst Drain App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("app.bsky.feed.post");

    let skybase = Skybase::new(config).expect("facade init failed");

    let cancel = CancellationToken::new();
    let handle = skybase
        .start_consumer(cancel.clone())
        .expect("start_consumer failed");

    // Wait for consumer to establish active connection
    for _ in 0..50 {
        if mock_jetstream.active_connections() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Emit 100 commits in a fast burst
    for idx in 0..100 {
        let commit = JetstreamCommit {
            did: format!("did:plc:user_{idx}"),
            time_us: 1_700_000_000_000_000 + idx,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("post_{idx}"),
            operation: CommitOperation::Create,
            cid: Some(format!("bafy_cid_{idx}")),
            record: Some(json!({
                "text": format!("Burst post content {idx}"),
                "seq": idx
            })),
        };
        let _ = mock_jetstream.emit_commit(&commit);
    }

    // Allow some frames to be transferred over TCP socket before cancellation
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cancel mid-stream
    handle.stop();
    let join_res = tokio::time::timeout(Duration::from_secs(3), handle.join()).await;
    assert!(join_res.is_ok(), "Consumer failed to join after burst");
    assert!(join_res.unwrap().is_ok());

    // Storage must be consistent and searchable
    let store = skybase.require_store().expect("store required");
    let rows = store
        .collection("app.bsky.feed.post")
        .execute()
        .expect("query execution failed");

    assert!(
        !rows.is_empty(),
        "At least some burst commits should be persisted"
    );
    for row in rows {
        assert!(row.uri.starts_with("at://did:plc:user_"));
        assert!(row.record_json["text"].is_string());
    }
}

/// 7. Batched sync mode: cancellation flushes pending commit buffer to SQLite.
#[tokio::test]
async fn test_challenger_batched_sync_cancellation_flushes_pending_commits() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Batch Flush App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store()
    .with_wanted_collection("app.bsky.feed.post");

    let skybase = Skybase::new(config).expect("facade init failed");

    // Large batch size of 50, but we will emit only 15 commits
    let ingester_cfg = IngesterConfig::new(mock_jetstream.ws_url())
        .with_collection("app.bsky.feed.post")
        .with_batch_size(50);

    let cancel = CancellationToken::new();
    let handle = skybase
        .start_consumer_with_config(ingester_cfg, cancel.clone())
        .expect("start_consumer_with_config failed");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Emit 15 commits (less than batch size of 50)
    for idx in 0..15 {
        let commit = JetstreamCommit {
            did: "did:plc:batcher".to_string(),
            time_us: 1_700_000_000_000_000 + idx,
            collection: "app.bsky.feed.post".to_string(),
            rkey: format!("batch_item_{idx}"),
            operation: CommitOperation::Create,
            cid: Some(format!("bafy_batch_{idx}")),
            record: Some(json!({"index": idx, "tag": "flush_test"})),
        };
        mock_jetstream.emit_commit(&commit).expect("emit failed");
    }

    // Allow messages to reach consumer buffer
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop consumer - must flush pending 15 records upon exit
    handle.stop();
    let join_res = tokio::time::timeout(Duration::from_secs(3), handle.join()).await;
    assert!(join_res.is_ok());
    assert!(join_res.unwrap().is_ok());

    let store = skybase.require_store().expect("store required");
    let rows = store
        .collection("app.bsky.feed.post")
        .where_json("tag", QueryOp::Eq, "flush_test")
        .execute()
        .expect("query failed");

    assert_eq!(
        rows.len(),
        15,
        "All 15 buffered commits should have been flushed to SQLite on cancellation"
    );
}

/// 8. Consumer handle stop() is idempotent and safe to call concurrently.
#[tokio::test]
async fn test_challenger_consumer_handle_idempotent_and_concurrent_stop() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Idempotent Stop App",
    )
    .with_jetstream_endpoint(mock_jetstream.ws_url())
    .with_in_memory_store();

    let skybase = Skybase::new(config).expect("facade init failed");

    let cancel = CancellationToken::new();
    let handle = skybase
        .start_consumer(cancel.clone())
        .expect("start_consumer failed");

    // Call stop concurrently from 10 tasks
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let c = cancel.clone();
        tasks.push(tokio::spawn(async move {
            c.cancel();
        }));
    }

    for t in tasks {
        t.await.expect("join task failed");
    }

    // Also call handle.stop() multiple times
    handle.stop();
    handle.stop();

    let join_res = tokio::time::timeout(Duration::from_secs(2), handle.join()).await;
    assert!(join_res.is_ok());
    assert!(join_res.unwrap().is_ok());
}

/// 9. Multiple concurrent consumers running against independent and shared stores.
#[tokio::test]
async fn test_challenger_multiple_concurrent_consumers_lifecycle_chaos() {
    let mock_jetstream = MockJetstreamServer::start()
        .await
        .expect("mock jetstream start failed");

    let shared_store = RecordStore::open_in_memory().expect("open_in_memory failed");

    let mut handles = Vec::new();

    for idx in 0..5 {
        let store = if idx % 2 == 0 {
            shared_store.clone()
        } else {
            RecordStore::open_in_memory().expect("open_in_memory failed")
        };

        let ingester_cfg = IngesterConfig::new(mock_jetstream.ws_url())
            .with_collection(format!("app.collection.{idx}"));

        let consumer = skybase::JetstreamConsumer::new(ingester_cfg, store);
        let cancel = CancellationToken::new();
        let handle = consumer.start(cancel);
        handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Random staggered cancellation
    for (i, handle) in handles.into_iter().enumerate() {
        if i % 2 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.stop();
        let join_res = tokio::time::timeout(Duration::from_secs(2), handle.join()).await;
        assert!(
            join_res.is_ok(),
            "Consumer {i} failed to join within timeout"
        );
        assert!(join_res.unwrap().is_ok());
    }
}

// ============================================================================
// Group 2: Configuration Boundary Conditions & Facade Resilience
// ============================================================================

/// 10. SkybaseConfig validates whitespace-only and empty strings safely.
#[test]
fn test_challenger_config_whitespace_and_empty_validation() {
    let invalid_clients = vec!["", "   ", "\t", "\n\r", "   \t\n  "];
    for bad_client in invalid_clients {
        let config = SkybaseConfig::new(bad_client, "https://app.example.com/callback", "Test App");
        let res = Skybase::new(config);
        assert!(
            matches!(res, Err(SkybaseError::Config(_))),
            "Expected Config error for bad client_id: '{bad_client}'"
        );
    }

    let invalid_redirects = vec!["", "   ", "\t", "\n"];
    for bad_redirect in invalid_redirects {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            bad_redirect,
            "Test App",
        );
        let res = Skybase::new(config);
        assert!(
            matches!(res, Err(SkybaseError::Config(_))),
            "Expected Config error for bad redirect_uri: '{bad_redirect}'"
        );
    }

    // App name can be empty string without error
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "",
    );
    let res = Skybase::new(config);
    assert!(res.is_ok(), "Empty app_name should be accepted");
}

/// 11. Conflicting storage options resolve deterministically with documented precedence.
#[test]
fn test_challenger_config_conflicting_storage_options_precedence() {
    let temp_dir = tempfile::tempdir().expect("tempdir failed");
    let db_path = temp_dir.path().join("precedence_test.db");

    // 1. Both storage_path AND in_memory_store = true: storage_path takes precedence
    let config1 = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Precedence 1",
    )
    .with_storage_path(&db_path)
    .with_in_memory_store();

    let skybase1 = Skybase::new(config1).expect("Skybase::new failed");
    let store1 = skybase1.require_store().expect("store required").clone();

    let input = RecordInput::new(
        "did:plc:precedence",
        "app.bsky.feed.post",
        "p1",
        "cid1",
        json!({"persisted": true}),
        1_000,
    );
    store1.upsert_record(&input).expect("upsert failed");
    drop(skybase1);
    drop(store1);

    // Verify record actually persisted to file
    assert!(db_path.exists(), "File database should have been created");
    let store_reopened = RecordStore::open(&db_path).expect("reopen failed");
    let fetched = store_reopened
        .collection("app.bsky.feed.post")
        .execute()
        .expect("query failed");
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].uri, input.uri);

    // 2. Both store_config AND storage_path: store_config takes precedence
    let isolated_cfg = RecordStoreConfig::in_memory();
    let config2 = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Precedence 2",
    )
    .with_storage_path(&db_path)
    .with_store_config(isolated_cfg);

    let skybase2 = Skybase::new(config2).expect("Skybase::new failed");
    let store2 = skybase2.require_store().expect("store required");
    // store2 is in-memory, so it shouldn't contain the record from db_path
    let fetched2 = store2
        .collection("app.bsky.feed.post")
        .execute()
        .expect("query failed");
    assert_eq!(fetched2.len(), 0);
}

/// 12. Facade store replacement lifecycle: uninitialized -> attach -> replace in-memory -> replace file.
#[test]
fn test_challenger_facade_store_transition_and_replacement() {
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Lifecycle App",
    );
    let mut skybase = Skybase::new(config).expect("init failed");

    // All storage calls must return Config error when uninitialized
    assert!(skybase.store().is_none());
    assert!(matches!(
        skybase.require_store(),
        Err(SkybaseError::Config(_))
    ));
    assert!(matches!(
        skybase.query("test"),
        Err(SkybaseError::Config(_))
    ));
    assert!(matches!(
        skybase.collection("test"),
        Err(SkybaseError::Config(_))
    ));
    assert!(matches!(skybase.subscribe(), Err(SkybaseError::Config(_))));
    assert!(matches!(skybase.consumer(), Err(SkybaseError::Config(_))));

    // Attach initial store
    let mem_store = RecordStore::open_in_memory().expect("open_in_memory failed");
    skybase = skybase.with_store(mem_store);
    assert!(skybase.store().is_some());
    assert!(skybase.require_store().is_ok());

    // Replace with fresh in-memory store
    let mem_store2 = skybase
        .open_in_memory_store()
        .expect("open_in_memory_store failed");
    let _ = mem_store2;
    assert!(skybase.store().is_some());

    // Replace with persistent disk store
    let temp_dir = tempfile::tempdir().expect("tempdir failed");
    let path = temp_dir.path().join("transition.db");
    let file_store = skybase.open_store(&path).expect("open_store failed");
    let _ = file_store;
    assert!(skybase.store().is_some());
}

/// 13. Filter vectors containing blank and whitespace elements are sanitized cleanly.
#[test]
fn test_challenger_config_filter_vectors_edge_cases() {
    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Filter App",
    )
    .with_jetstream_endpoint("wss://jetstream.example.com/subscribe")
    .with_in_memory_store()
    .with_wanted_collections(vec![
        "",
        "   ",
        "app.bsky.feed.post",
        "\t",
        "app.bsky.feed.like",
    ])
    .with_wanted_dids(vec!["", "did:plc:valid_author", "   "]);

    let skybase = Skybase::new(config).expect("init failed");
    let consumer = skybase.consumer().expect("consumer creation failed");

    // Ensure consumer received config
    assert_eq!(consumer.config().wanted_collections.len(), 5);
    assert_eq!(consumer.config().wanted_dids.len(), 3);

    // Ensure URL builder strips empty and whitespace elements
    let url = skybase::build_subscription_url_full(
        &consumer.config().endpoint,
        &consumer.config().wanted_collections,
        &consumer.config().wanted_dids,
        None,
    );

    assert!(url.contains("wantedCollections=app.bsky.feed.post"));
    assert!(url.contains("wantedCollections=app.bsky.feed.like"));
    assert!(url.contains("wantedDids=did:plc:valid_author"));
    assert!(!url.contains("wantedCollections=&"));
    assert!(!url.contains("wantedCollections= "));
    assert!(!url.contains("wantedDids=&"));
}

/// 14. Sovereign PDS client from credentials handles custom endpoints and NSIDs.
#[tokio::test]
async fn test_challenger_repo_client_from_credentials_boundaries() {
    let mock_pds = MockPdsServer::start().await;

    let config = SkybaseConfig::new(
        "https://app.example.com/oauth/client-metadata.json",
        "https://app.example.com/oauth/callback",
        "Repo Client App",
    );
    let skybase = Skybase::new(config).expect("init failed");

    // Endpoint with trailing slash
    let endpoint_with_slash = format!("{}/", mock_pds.uri());
    let client = skybase
        .repo_client_from_credentials(
            &endpoint_with_slash,
            "did:plc:sovereign_author",
            "token_valid_123",
        )
        .expect("repo_client_from_credentials failed");

    assert_eq!(client.did(), "did:plc:sovereign_author");

    let record_payload = json!({
        "$type": "org.custom.project.item",
        "name": "Sovereign Item",
        "active": true
    });

    let res = client
        .create_record(
            "org.custom.project.item",
            Some("custom_key_1"),
            &record_payload,
            true,
        )
        .await
        .expect("create_record failed");

    assert_eq!(
        res.uri,
        "at://did:plc:sovereign_author/org.custom.project.item/custom_key_1"
    );

    // Delete record call
    client
        .delete_record("org.custom.project.item", "custom_key_1")
        .await
        .expect("delete_record failed");
}

// ============================================================================
// Group 3: Query Engine Edge Cases & Complex JSON1 LIKE Patterns
// ============================================================================

fn setup_query_test_store() -> RecordStore {
    let store = RecordStore::open_in_memory().expect("store init failed");

    let records = vec![
        (
            "post_1",
            json!({"title": "Decentralized AppView Engine", "category": "tech", "views": 100, "active": true, "author": "alice"}),
        ),
        (
            "post_2",
            json!({"title": "Decentralized Identity Protocol", "category": "crypto", "views": 250, "active": true, "author": "bob"}),
        ),
        (
            "post_3",
            json!({"title": "Centralized Cloud Architecture", "category": "legacy", "views": 50, "active": false, "author": "charlie"}),
        ),
        (
            "post_4",
            json!({"title": "AT Protocol Specifications", "category": "tech", "views": 500, "active": true, "author": "david"}),
        ),
        (
            "post_5",
            json!({"title": "post", "category": "social", "views": 10, "active": true, "author": "eve"}),
        ),
        (
            "post_6",
            json!({"title": "past", "category": "history", "views": 20, "active": false, "author": "frank"}),
        ),
        (
            "post_7",
            json!({"title": "pest", "category": "nature", "views": 5, "active": false, "author": "grace"}),
        ),
        (
            "post_8",
            json!({"title": "rust", "category": "programming", "views": 999, "active": true, "author": "heidi"}),
        ),
        (
            "post_9",
            json!({"title": "", "category": "empty", "views": 0, "active": true, "author": "ivan"}),
        ),
        (
            "post_10",
            json!({"title": serde_json::Value::Null, "category": "null_test", "views": 0, "active": false, "author": "judy"}),
        ),
    ];

    for (rkey, json_val) in records {
        let input = RecordInput::new(
            "did:plc:query_test",
            "app.bsky.feed.post",
            rkey,
            format!("cid_{rkey}"),
            json_val,
            1_000,
        );
        store.upsert_record(&input).expect("upsert failed");
    }

    store
}

/// 15. QueryOp::Like with prefix, suffix, and infix SQL wildcards (%).
#[test]
fn test_challenger_query_like_wildcard_patterns_prefix_suffix_infix() {
    let store = setup_query_test_store();

    // 1. Prefix wildcard: "Decentralized%"
    let res1 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "Decentralized%")
        .execute()
        .expect("query failed");
    assert_eq!(res1.len(), 2);
    let titles1: Vec<&str> = res1
        .iter()
        .map(|r| r.record_json["title"].as_str().unwrap())
        .collect();
    assert!(titles1.contains(&"Decentralized AppView Engine"));
    assert!(titles1.contains(&"Decentralized Identity Protocol"));

    // 2. Suffix wildcard: "%Protocol"
    let res2 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "%Protocol")
        .execute()
        .expect("query failed");
    assert_eq!(res2.len(), 1);
    assert_eq!(
        res2[0].record_json["title"],
        "Decentralized Identity Protocol"
    );

    // 3. Infix wildcard: "%AppView%"
    let res3 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "%AppView%")
        .execute()
        .expect("query failed");
    assert_eq!(res3.len(), 1);
    assert_eq!(res3[0].record_json["title"], "Decentralized AppView Engine");

    // 4. Exact match without wildcards: "post"
    let res4 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "post")
        .execute()
        .expect("query failed");
    assert_eq!(res4.len(), 1);
    assert_eq!(res4[0].record_json["title"], "post");

    // 5. Case insensitivity for ASCII in standard SQLite LIKE
    let res5 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "decentralized%")
        .execute()
        .expect("query failed");
    assert_eq!(
        res5.len(),
        2,
        "ASCII LIKE in SQLite should match case-insensitively"
    );
}

/// 16. QueryOp::Like with single-character (_) and multi-character underscore wildcards.
#[test]
fn test_challenger_query_like_single_and_multi_underscore_wildcards() {
    let store = setup_query_test_store();

    // 1. Single wildcard: "p_st" matches "post", "past", "pest"
    let res = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "p_st")
        .execute()
        .expect("query failed");

    assert_eq!(res.len(), 3);
    let titles: Vec<&str> = res
        .iter()
        .map(|r| r.record_json["title"].as_str().unwrap())
        .collect();
    assert!(titles.contains(&"post"));
    assert!(titles.contains(&"past"));
    assert!(titles.contains(&"pest"));

    // 2. Double wildcard: "r__t" matches "rust"
    let res2 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "r__t")
        .execute()
        .expect("query failed");
    assert_eq!(res2.len(), 1);
    assert_eq!(res2[0].record_json["title"], "rust");

    // 3. Combined % and _: "D%ed I_entity%" matches "Decentralized Identity Protocol"
    let res3 = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "D%ed I_entity%")
        .execute()
        .expect("query failed");
    assert_eq!(res3.len(), 1);
    assert_eq!(
        res3[0].record_json["title"],
        "Decentralized Identity Protocol"
    );
}

/// 17. QueryOp::Like with empty strings and all-wildcards (%) boundary behavior.
#[test]
fn test_challenger_query_like_empty_string_and_all_wildcards() {
    let store = setup_query_test_store();

    // 1. Empty pattern "" matches ONLY empty string record
    let res_empty = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "")
        .execute()
        .expect("query failed");
    assert_eq!(res_empty.len(), 1);
    assert_eq!(res_empty[0].record_json["title"], "");

    // 2. All-wildcard "%" matches all non-null strings (9 records, excludes null)
    let res_all = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "%")
        .execute()
        .expect("query failed");
    assert_eq!(
        res_all.len(),
        9,
        "LIKE '%' must match all 9 non-null string records"
    );

    // 3. Multiple consecutive wildcards "%%" is valid SQL
    let res_double = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "%%")
        .execute()
        .expect("query failed");
    assert_eq!(res_double.len(), 9);
}

/// 18. QueryOp::Like with quotes, escaped characters, newlines, tabs, and Unicode emojis.
#[test]
fn test_challenger_query_like_escaped_characters_quotes_and_unicode() {
    let store = RecordStore::open_in_memory().expect("store failed");

    let special_records = vec![
        ("s1", json!({"content": "O'Reilly Media ATProto Guide"})),
        ("s2", json!({"content": "He said: \"Hello World\""})),
        (
            "s3",
            json!({"content": "Path C:\\Program Files\\Skybase\\data"}),
        ),
        ("s4", json!({"content": "Line 1\nLine 2\tIndented Column"})),
        (
            "s5",
            json!({"content": "🚀 Skybase 火花 🌟 ATProto Decentralized"}),
        ),
        ("s6", json!({"content": "100% Guaranteed Reliability"})),
        ("s7", json!({"content": "identifier_with_underscores_123"})),
    ];

    for (rkey, val) in special_records {
        let input = RecordInput::new(
            "did:plc:special_user",
            "app.bsky.feed.post",
            rkey,
            "cid1",
            val,
            1_000,
        );
        store.upsert_record(&input).expect("upsert failed");
    }

    // 1. Single quote in search pattern
    let r1 = store
        .collection("app.bsky.feed.post")
        .where_json("content", QueryOp::Like, "%O'Reilly%")
        .execute()
        .expect("single quote query failed");
    assert_eq!(r1.len(), 1);

    // 2. Double quotes
    let r2 = store
        .collection("app.bsky.feed.post")
        .where_json("content", QueryOp::Like, "%\"Hello World\"%")
        .execute()
        .expect("double quote query failed");
    assert_eq!(r2.len(), 1);

    // 3. Backslashes
    let r3 = store
        .collection("app.bsky.feed.post")
        .where_json("content", QueryOp::Like, "%Program Files%")
        .execute()
        .expect("backslash query failed");
    assert_eq!(r3.len(), 1);

    // 4. Newlines and tabs
    let r4 = store
        .collection("app.bsky.feed.post")
        .where_json("content", QueryOp::Like, "%Line 2\tIndented%")
        .execute()
        .expect("newline query failed");
    assert_eq!(r4.len(), 1);

    // 5. Multi-byte UTF-8 emojis and Chinese characters
    let r5 = store
        .collection("app.bsky.feed.post")
        .where_json("content", QueryOp::Like, "%火花%")
        .execute()
        .expect("emoji query failed");
    assert_eq!(r5.len(), 1);

    let r5b = store
        .collection("app.bsky.feed.post")
        .where_json("content", QueryOp::Like, "%🚀%🌟%")
        .execute()
        .expect("emoji query failed");
    assert_eq!(r5b.len(), 1);
}

/// 19. Parameterized SQL injection payloads in QueryOp::Like are neutralized.
#[test]
fn test_challenger_query_like_sql_injection_parameterized_defense() {
    let store = setup_query_test_store();

    let injection_payloads = vec![
        "' OR '1'='1",
        "%' OR 1=1 --",
        "test'; DROP TABLE records; --",
        "' UNION SELECT 'a', 'b', 'c', 'd', 'e', '{}', 0, 0 --",
        "%' AND (SELECT count(*) FROM records) > 0 AND '%'='",
        "admin'--",
        "1' OR '1' = '1' /*",
    ];

    for payload in injection_payloads {
        let result = store
            .collection("app.bsky.feed.post")
            .where_json("title", QueryOp::Like, payload)
            .execute();

        assert!(
            result.is_ok(),
            "Query with injection payload '{payload}' must not fail with SQL syntax error"
        );
        let rows = result.unwrap();
        assert_eq!(
            rows.len(),
            0,
            "Injection payload '{payload}' must not match any records"
        );
    }

    // Verify table is still intact and holds all records
    let count = store
        .collection("app.bsky.feed.post")
        .include_deleted(true)
        .execute()
        .expect("verify table intact")
        .len();
    assert_eq!(count, 10, "Records table must remain intact");
}

/// 20. Chaining multiple where_json conditions combining Eq, Ne, Gt, Gte, Lt, Lte, Contains, Like.
#[test]
fn test_challenger_query_multi_clause_chaining_matrix() {
    let store = RecordStore::open_in_memory().expect("store init failed");

    let dataset = vec![
        (
            "p1",
            json!({
                "type": "article",
                "status": "published",
                "views": 250,
                "likes": 50,
                "title": "Learning Rust for Decentralized Systems",
                "summary": "Building AppViews with memory safety",
                "archived": null
            }),
        ),
        (
            "p2",
            json!({
                "type": "article",
                "status": "published",
                "views": 80,
                "likes": 15,
                "title": "Decentralized Protocols in Go",
                "summary": "Go vs Rust for networking",
                "archived": null
            }),
        ),
        (
            "p3",
            json!({
                "type": "article",
                "status": "draft", // fails status == published
                "views": 300,
                "likes": 60,
                "title": "Advanced Rust AppViews",
                "summary": "Deep dive into SQLite WAL",
                "archived": null
            }),
        ),
        (
            "p4",
            json!({
                "type": "article",
                "status": "published",
                "views": 1500, // fails views <= 1000
                "likes": 100,
                "title": "Rust Firehose Ingestion",
                "summary": "Scaling WebSocket consumers",
                "archived": null
            }),
        ),
        (
            "p5",
            json!({
                "type": "note", // fails type == article
                "status": "published",
                "views": 200,
                "likes": 40,
                "title": "Quick Rust tip",
                "summary": "Pattern matching in Rust",
                "archived": null
            }),
        ),
        (
            "p6",
            json!({
                "type": "article",
                "status": "published",
                "views": 400,
                "likes": 80,
                "title": "Rust & SQLite Embedded Architecture",
                "summary": "Zero-dependency memory safety",
                "archived": null
            }),
        ),
    ];

    for (rkey, val) in dataset {
        let input = RecordInput::new(
            "did:plc:author_chain",
            "app.bsky.feed.post",
            rkey,
            "cid",
            val,
            1_000,
        );
        store.upsert_record(&input).expect("upsert failed");
    }

    // Chain 8 distinct conditions
    let matching_records = store
        .collection("app.bsky.feed.post")
        .where_json("type", QueryOp::Eq, "article")
        .where_json("status", QueryOp::Ne, "draft")
        .where_json("views", QueryOp::Gt, 100)
        .where_json("views", QueryOp::Lte, 1000)
        .where_json("likes", QueryOp::Gte, 20)
        .where_json("title", QueryOp::Like, "%Rust%")
        .where_json("summary", QueryOp::Contains, "memory")
        .where_json("archived", QueryOp::Eq, serde_json::Value::Null)
        .order_by("views", SortDirection::Desc)
        .execute()
        .expect("multi-clause query execution failed");

    // Only p6 (views=400) and p1 (views=250) satisfy ALL 8 conditions
    assert_eq!(
        matching_records.len(),
        2,
        "Exactly 2 records should match the 8 combined clauses"
    );
    assert_eq!(matching_records[0].rkey, "p6");
    assert_eq!(matching_records[1].rkey, "p1");
}

/// 21. QueryOp::Like on non-string JSON values and non-existent paths.
#[test]
fn test_challenger_query_like_on_non_string_types_and_missing_fields() {
    let store = RecordStore::open_in_memory().expect("store init failed");

    let input1 = RecordInput::new(
        "did:plc:numbers",
        "app.bsky.feed.post",
        "num1",
        "cid1",
        json!({"code": 404, "score": 98.5, "flag": true, "nested": {"val": 123}}),
        1_000,
    );
    let input2 = RecordInput::new(
        "did:plc:numbers",
        "app.bsky.feed.post",
        "num2",
        "cid2",
        json!({"code": 500, "score": 45.0, "flag": false}),
        1_000,
    );
    store.upsert_record(&input1).expect("upsert failed");
    store.upsert_record(&input2).expect("upsert failed");

    // 1. LIKE on integer field: "40%" matches 404
    let r1 = store
        .collection("app.bsky.feed.post")
        .where_json("code", QueryOp::Like, "40%")
        .execute()
        .expect("query failed");
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].rkey, "num1");

    // 2. Non-existent field returns empty list without error
    let r2 = store
        .collection("app.bsky.feed.post")
        .where_json("missing_field", QueryOp::Like, "%anything%")
        .execute()
        .expect("query on missing field should succeed");
    assert_eq!(r2.len(), 0);

    // 3. Deeply nested path with QueryOp::Like
    let r3 = store
        .collection("app.bsky.feed.post")
        .where_json("nested.val", QueryOp::Like, "12%")
        .execute()
        .expect("deeply nested query failed");
    assert_eq!(r3.len(), 1);
    assert_eq!(r3[0].rkey, "num1");
}

/// 22. Ordering, pagination, and soft delete interactions with QueryOp::Like.
#[test]
fn test_challenger_query_like_with_sorting_pagination_and_soft_delete() {
    let store = RecordStore::open_in_memory().expect("store init failed");

    for idx in 0..10 {
        let input = RecordInput::new(
            "did:plc:pagination",
            "app.bsky.feed.post",
            format!("page_post_{idx}"),
            "cid",
            json!({
                "title": format!("Decentralized Post #{idx}"),
                "rank": idx * 10
            }),
            1_000 + idx,
        );
        store.upsert_record(&input).expect("upsert failed");
    }

    // Soft delete post #3 and #7
    store
        .soft_delete_record("at://did:plc:pagination/app.bsky.feed.post/page_post_3")
        .expect("soft delete failed");
    store
        .soft_delete_record("at://did:plc:pagination/app.bsky.feed.post/page_post_7")
        .expect("soft delete failed");

    // 1. Default query with LIKE: excludes soft-deleted records (8 active remain)
    let active_matching = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "Decentralized Post%")
        .execute()
        .expect("query failed");
    assert_eq!(active_matching.len(), 8);

    // 2. include_deleted(true) returns all 10 records matching pattern
    let all_matching = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "Decentralized Post%")
        .include_deleted(true)
        .execute()
        .expect("query failed");
    assert_eq!(all_matching.len(), 10);

    // 3. Pagination with LIKE and JSON order_by: limit 3, offset 2, ordered by rank DESC
    let paged = store
        .collection("app.bsky.feed.post")
        .where_json("title", QueryOp::Like, "%Post%")
        .order_by("rank", SortDirection::Desc)
        .limit(3)
        .offset(2)
        .execute()
        .expect("paged query failed");

    assert_eq!(paged.len(), 3);
    // Ranks active: 90 (post 9), 80 (post 8), [70 deleted], 60 (post 6), 50 (post 5), 40 (post 4)...
    // Offsets: offset 0 = 90, offset 1 = 80, offset 2 = 60, offset 3 = 50, offset 4 = 40.
    assert_eq!(paged[0].rkey, "page_post_6");
    assert_eq!(paged[1].rkey, "page_post_5");
    assert_eq!(paged[2].rkey, "page_post_4");
}
