//! Empirical Challenger Stress Tests for `skybase::index`.
//!
//! Stress-tests:
//! 1. High-throughput concurrent writes, reads, and queries across multi-threaded Tokio tasks.
//! 2. Tombstone resurrection cycles (upsert -> delete -> upsert -> delete -> upsert) verifying
//!    atomic state transitions, `is_deleted` flags, and broadcast bus notification correctness.
//! 3. Batch transactions under concurrent load, duplicate keys in batch, rollback atomicity on error.
//! 4. In-memory and file-backed SQLite WAL performance and deadlock-freedom.
//! 5. Broadcast bus ring-buffer saturation, slow receivers (`Lagged`), and multi-subscriber fanout.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;
use tokio::sync::broadcast;
use tokio::task::JoinSet;

use skybase::index::{
    ChangeNotification, QueryOp, RecordInput, RecordStore, RecordStoreConfig, SortDirection,
};

// ============================================================================
// 1. High-Throughput Storage & Concurrency Stress Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_high_concurrency_writes_and_reads_in_memory() {
    let store = RecordStore::open_in_memory().expect("in-memory store open failed");
    let num_tasks = 40;
    let ops_per_task = 50;

    let mut set = JoinSet::new();

    // Spawn 20 writer tasks
    for task_id in 0..(num_tasks / 2) {
        let store_clone = store.clone();
        set.spawn(async move {
            for i in 0..ops_per_task {
                let rkey = format!("task_{task_id}_post_{i}");
                let input = RecordInput::new(
                    format!("did:plc:task{task_id}"),
                    "app.bsky.feed.post",
                    &rkey,
                    format!("cid_{task_id}_{i}"),
                    json!({
                        "task": task_id,
                        "seq": i,
                        "text": format!("Concurrency test {task_id}-{i}"),
                        "count": task_id * 1000 + i
                    }),
                    1_700_000_000 + (task_id * 1000 + i) as u64,
                );
                store_clone
                    .upsert_record(&input)
                    .expect("concurrent upsert failed");
            }
        });
    }

    // Spawn 20 reader / query tasks concurrently
    for _ in 0..(num_tasks / 2) {
        let store_clone = store.clone();
        set.spawn(async move {
            for _ in 0..ops_per_task {
                // Execute mixed point lookups and range queries
                let _ = store_clone.query("app.bsky.feed.post").limit(10).execute();
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task join failed");
    }

    // Verify all written records exist and match exactly
    let total_written = (num_tasks / 2) * ops_per_task;
    let all_records = store
        .query("app.bsky.feed.post")
        .limit((total_written * 2) as u32)
        .execute()
        .expect("final query failed");

    assert_eq!(
        all_records.len(),
        total_written,
        "Every concurrently written record must be persisted"
    );

    // Spot-check individual records across tasks
    for task_id in 0..(num_tasks / 2) {
        let uri = format!("at://did:plc:task{task_id}/app.bsky.feed.post/task_{task_id}_post_0");
        let fetched = store
            .get_record(&uri)
            .expect("get_record failed")
            .expect("record missing");
        assert_eq!(fetched.record_json["task"], task_id);
        assert_eq!(fetched.record_json["seq"], 0);
        assert!(!fetched.is_deleted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_file_backed_wal_concurrent_hammer() {
    let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
    let db_path = temp_dir.path().join("wal_concurrency.db");

    let config = RecordStoreConfig::persistent(&db_path);
    let store = RecordStore::with_config(config).expect("persistent store open failed");

    let num_tasks = 30;
    let ops_per_task = 40;
    let mut set = JoinSet::new();

    for task_id in 0..num_tasks {
        let store_clone = store.clone();
        set.spawn(async move {
            for i in 0..ops_per_task {
                let rkey = format!("key_{task_id}_{i}");
                let input = RecordInput::new(
                    "did:plc:hammer",
                    "app.bsky.feed.post",
                    &rkey,
                    format!("cid_{task_id}_{i}"),
                    json!({
                        "task": task_id,
                        "i": i,
                        "payload": "x".repeat(128)
                    }),
                    1_700_000_000 + i as u64,
                );

                store_clone
                    .upsert_record(&input)
                    .expect("upsert on file-backed WAL failed");

                if i % 5 == 0 {
                    let fetched = store_clone
                        .get_record(&input.uri)
                        .expect("read failed")
                        .expect("record must exist");
                    assert_eq!(fetched.rkey, rkey);
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task join failed");
    }

    let count = store
        .query("app.bsky.feed.post")
        .limit((num_tasks * ops_per_task + 100) as u32)
        .execute()
        .expect("count query failed")
        .len();

    assert_eq!(
        count,
        num_tasks * ops_per_task,
        "Total count must match num_tasks * ops_per_task exactly"
    );
}

// ============================================================================
// 2. Tombstone Resurrection & Notification Correctness Stress Tests
// ============================================================================

#[tokio::test]
async fn test_challenger_tombstone_resurrection_lifecycle_and_notifications() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let mut rx = store.subscribe();

    let uri = "at://did:plc:alice/app.bsky.feed.post/post_lifecycle";

    // Cycle 1: Insert
    let v1 = RecordInput::with_uri(
        uri,
        "cid_v1",
        "did:plc:alice",
        "app.bsky.feed.post",
        "post_lifecycle",
        json!({"version": 1, "text": "Original"}),
        1000,
    );
    store.upsert_record(&v1).expect("insert failed");

    // Verify Cycle 1 state
    let row1 = store
        .get_record(uri)
        .expect("get failed")
        .expect("should exist");
    assert_eq!(row1.cid, "cid_v1");
    assert!(!row1.is_deleted);

    // Verify Cycle 1 event
    let event1 = rx.try_recv().expect("event1 missing");
    match event1 {
        ChangeNotification::Upsert(r) => {
            assert_eq!(r.uri, uri);
            assert_eq!(r.cid, "cid_v1");
            assert!(!r.is_deleted);
        }
        _ => panic!("Expected Upsert event"),
    }

    // Cycle 2: Soft Delete
    store
        .soft_delete_record(uri, 1100)
        .expect("soft delete failed");

    // Verify Cycle 2 state: active get returns None, get_including_deleted returns row with is_deleted=true
    assert!(store.get_record(uri).expect("get failed").is_none());
    let deleted_row = store
        .get_record_including_deleted(uri)
        .expect("get failed")
        .expect("row should exist");
    assert!(deleted_row.is_deleted);

    // Verify QueryBuilder excludes soft-deleted record by default
    let active_rows = store
        .query("app.bsky.feed.post")
        .execute()
        .expect("query failed");
    assert!(
        active_rows.is_empty(),
        "Soft deleted record must be excluded"
    );

    // Verify QueryBuilder includes it when include_deleted(true)
    let all_rows = store
        .query("app.bsky.feed.post")
        .include_deleted(true)
        .execute()
        .expect("query failed");
    assert_eq!(all_rows.len(), 1);
    assert!(all_rows[0].is_deleted);

    // Verify Cycle 2 event
    let event2 = rx.try_recv().expect("event2 missing");
    match event2 {
        ChangeNotification::Delete {
            uri: del_uri,
            did,
            collection,
            rkey,
        } => {
            assert_eq!(del_uri, uri);
            assert_eq!(did, "did:plc:alice");
            assert_eq!(collection, "app.bsky.feed.post");
            assert_eq!(rkey, "post_lifecycle");
        }
        _ => panic!("Expected Delete event"),
    }

    // Cycle 3: Idempotent Soft Delete (deleting an already deleted record)
    store
        .soft_delete_record(uri, 1200)
        .expect("second soft delete failed");
    // Should NOT emit a duplicate Delete event
    assert!(
        rx.try_recv().is_err(),
        "Repeated soft delete of already deleted record must NOT emit duplicate Delete event"
    );

    // Cycle 4: Resurrection (Re-upsert with updated content)
    let v2 = RecordInput::with_uri(
        uri,
        "cid_v2",
        "did:plc:alice",
        "app.bsky.feed.post",
        "post_lifecycle",
        json!({"version": 2, "text": "Resurrected!"}),
        2000,
    );
    store
        .upsert_record(&v2)
        .expect("resurrection upsert failed");

    // Verify Cycle 4 state
    let row2 = store
        .get_record(uri)
        .expect("get failed")
        .expect("resurrected record must exist");
    assert_eq!(row2.cid, "cid_v2");
    assert_eq!(row2.record_json["version"], 2);
    assert_eq!(row2.indexed_at, 2000);
    assert!(!row2.is_deleted, "is_deleted must be reset to false");

    // Verify Cycle 4 event
    let event3 = rx.try_recv().expect("event3 missing");
    match event3 {
        ChangeNotification::Upsert(r) => {
            assert_eq!(r.uri, uri);
            assert_eq!(r.cid, "cid_v2");
            assert_eq!(r.record_json["version"], 2);
            assert!(!r.is_deleted);
        }
        _ => panic!("Expected Upsert event for resurrection"),
    }

    // Cycle 5: Second Soft Delete
    store
        .soft_delete_record(uri, 2500)
        .expect("soft delete 2 failed");
    assert!(store.get_record(uri).expect("get failed").is_none());
    let event4 = rx.try_recv().expect("event4 missing");
    assert!(event4.is_delete());

    // Cycle 6: Second Resurrection
    let v3 = RecordInput::with_uri(
        uri,
        "cid_v3",
        "did:plc:alice",
        "app.bsky.feed.post",
        "post_lifecycle",
        json!({"version": 3, "text": "Resurrected Again!"}),
        3000,
    );
    store
        .upsert_record(&v3)
        .expect("second resurrection upsert failed");
    let row3 = store
        .get_record(uri)
        .expect("get failed")
        .expect("resurrected again");
    assert_eq!(row3.cid, "cid_v3");
    assert!(!row3.is_deleted);
    let event5 = rx.try_recv().expect("event5 missing");
    assert!(event5.is_upsert());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_tombstone_resurrection_race() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let uri = "at://did:plc:race/app.bsky.feed.post/race_target";

    let cycles = 200;
    let store_writer = store.clone();
    let store_deleter = store.clone();

    let writer_handle = tokio::spawn(async move {
        for i in 0..cycles {
            let input = RecordInput::with_uri(
                uri,
                format!("cid_{i}"),
                "did:plc:race",
                "app.bsky.feed.post",
                "race_target",
                json!({"cycle": i}),
                1000 + i as u64,
            );
            store_writer.upsert_record(&input).expect("race upsert");
            tokio::task::yield_now().await;
        }
    });

    let deleter_handle = tokio::spawn(async move {
        for i in 0..cycles {
            store_deleter
                .soft_delete_record(uri, 1000 + i as u64)
                .expect("race soft delete");
            tokio::task::yield_now().await;
        }
    });

    writer_handle.await.expect("writer panicked");
    deleter_handle.await.expect("deleter panicked");

    // Final state must be well-formed: either active or soft-deleted, but never corrupted
    let row_all = store
        .get_record_including_deleted(uri)
        .expect("get failed")
        .expect("record must exist in DB");
    assert_eq!(row_all.uri, uri);
    assert_eq!(row_all.did, "did:plc:race");
    assert_eq!(row_all.collection, "app.bsky.feed.post");
    assert_eq!(row_all.rkey, "race_target");

    // If active, get_record returns Some; if soft-deleted, get_record returns None
    let row_active = store.get_record(uri).expect("get active failed");
    if row_all.is_deleted {
        assert!(row_active.is_none());
    } else {
        assert!(row_active.is_some());
    }
}

// ============================================================================
// 3. Batch Transactions Under Heavy Concurrent Load
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_batch_transactions_concurrent_with_single_ops() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let mut set = JoinSet::new();

    let num_batch_tasks = 10;
    let batch_size = 50;

    // 10 concurrent batch writers (each writing 50 records in atomic transactions)
    for batch_id in 0..num_batch_tasks {
        let store_clone = store.clone();
        set.spawn(async move {
            let mut records = Vec::with_capacity(batch_size);
            for i in 0..batch_size {
                records.push(RecordInput::new(
                    format!("did:plc:batch_{batch_id}"),
                    "app.bsky.feed.post",
                    format!("post_{i}"),
                    format!("cid_{batch_id}_{i}"),
                    json!({"batch": batch_id, "index": i}),
                    (batch_id * 1000 + i) as u64,
                ));
            }
            store_clone
                .upsert_records_batch(&records)
                .expect("batch upsert failed");
        });
    }

    // 10 concurrent single readers / writers
    for single_id in 0..10 {
        let store_clone = store.clone();
        set.spawn(async move {
            for i in 0..20 {
                let input = RecordInput::new(
                    format!("did:plc:single_{single_id}"),
                    "app.bsky.feed.post",
                    format!("single_{i}"),
                    format!("cid_single_{single_id}_{i}"),
                    json!({"single": single_id, "i": i}),
                    100_000 + i as u64,
                );
                store_clone.upsert_record(&input).expect("single upsert");
                let _ = store_clone.get_record(&input.uri);
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task failed");
    }

    // Total expected: 10 * 50 = 500 batch records + 10 * 20 = 200 single records = 700 total
    let total_records = store
        .query("app.bsky.feed.post")
        .limit(1000)
        .execute()
        .expect("total query failed");
    assert_eq!(
        total_records.len(),
        700,
        "All 700 records from concurrent batch and single ops must be present"
    );
}

#[test]
fn test_challenger_batch_duplicate_keys_within_single_batch() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    // A batch containing multiple updates to the SAME record key
    let records = vec![
        RecordInput::new(
            "did:plc:dup",
            "app.bsky.feed.post",
            "key1",
            "cid_v1",
            json!({"v": 1}),
            100,
        ),
        RecordInput::new(
            "did:plc:dup",
            "app.bsky.feed.post",
            "key2",
            "cid_k2",
            json!({"v": 1}),
            101,
        ),
        RecordInput::new(
            "did:plc:dup",
            "app.bsky.feed.post",
            "key1", // Same key updated in same batch
            "cid_v2",
            json!({"v": 2}),
            200,
        ),
        RecordInput::new(
            "did:plc:dup",
            "app.bsky.feed.post",
            "key1", // Same key updated again
            "cid_v3",
            json!({"v": 3}),
            300,
        ),
    ];

    store
        .upsert_records_batch(&records)
        .expect("batch with duplicates failed");

    // Only 2 distinct records should exist
    let all = store
        .query("app.bsky.feed.post")
        .execute()
        .expect("query failed");
    assert_eq!(all.len(), 2);

    let key1_row = store
        .get_record("at://did:plc:dup/app.bsky.feed.post/key1")
        .expect("get failed")
        .expect("key1 must exist");
    assert_eq!(
        key1_row.cid, "cid_v3",
        "Final value in batch must win on conflict"
    );
    assert_eq!(key1_row.record_json["v"], 3);
    assert_eq!(key1_row.indexed_at, 300);
}

#[test]
fn test_challenger_batch_atomic_rollback_on_error() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    // Insert a baseline record
    let baseline = RecordInput::new(
        "did:plc:base",
        "app.bsky.feed.post",
        "base1",
        "cid_base",
        json!({"baseline": true}),
        500,
    );
    store.upsert_record(&baseline).expect("baseline upsert");

    let mut rx = store.subscribe();

    // Prepare a batch where one record has an invalid timestamp (u64::MAX overflows i64)
    let bad_batch = vec![
        RecordInput::new(
            "did:plc:bad",
            "app.bsky.feed.post",
            "item1",
            "cid1",
            json!({"v": 1}),
            1000,
        ),
        RecordInput::new(
            "did:plc:bad",
            "app.bsky.feed.post",
            "item2_bad",
            "cid2",
            json!({"v": 2}),
            u64::MAX, // Triggers i64::try_from error
        ),
        RecordInput::new(
            "did:plc:bad",
            "app.bsky.feed.post",
            "item3",
            "cid3",
            json!({"v": 3}),
            1002,
        ),
    ];

    let result = store.upsert_records_batch(&bad_batch);
    assert!(
        result.is_err(),
        "Batch with u64::MAX timestamp must return error"
    );

    // Verify atomic rollback: NONE of the batch records must be in the DB
    assert!(store
        .get_record("at://did:plc:bad/app.bsky.feed.post/item1")
        .expect("get failed")
        .is_none());
    assert!(store
        .get_record("at://did:plc:bad/app.bsky.feed.post/item2_bad")
        .expect("get failed")
        .is_none());
    assert!(store
        .get_record("at://did:plc:bad/app.bsky.feed.post/item3")
        .expect("get failed")
        .is_none());

    // Only the baseline record remains
    let rows = store
        .query("app.bsky.feed.post")
        .execute()
        .expect("query failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uri, baseline.uri);

    // Zero broadcast events must have been emitted for the failed batch
    assert!(
        rx.try_recv().is_err(),
        "No notifications must be emitted when batch rolls back"
    );
}

// ============================================================================
// 4. Broadcast Bus Saturation & Fanout Stress Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_broadcast_bus_saturation_and_slow_receivers() {
    let config = RecordStoreConfig {
        broadcast_capacity: 32, // Small capacity to force buffer overflow
        ..RecordStoreConfig::in_memory()
    };
    let store = RecordStore::with_config(config).expect("store config failed");

    let mut slow_rx = store.subscribe();
    let mut fast_rx = store.subscribe();

    // Fast receiver drains in background
    let fast_received_count = Arc::new(AtomicUsize::new(0));
    let fast_count_clone = fast_received_count.clone();
    let stop_signal = Arc::new(AtomicBool::new(false));
    let stop_signal_clone = stop_signal.clone();

    let fast_task = tokio::spawn(async move {
        while !stop_signal_clone.load(Ordering::Relaxed) {
            match fast_rx.try_recv() {
                Ok(_) => {
                    fast_count_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    tokio::task::yield_now().await;
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }
    });

    // Publish 200 records without slow_rx reading
    for i in 0..200 {
        let input = RecordInput::new(
            "did:plc:burst",
            "app.bsky.feed.post",
            format!("burst_{i}"),
            format!("cid_{i}"),
            json!({"i": i}),
            1_700_000_000 + i as u64,
        );
        store.upsert_record(&input).expect("upsert failed");
    }

    stop_signal.store(true, Ordering::SeqCst);
    let _ = fast_task.await;

    // Slow receiver should have lagged without panic or deadlock
    match slow_rx.recv().await {
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            assert!(
                skipped > 0,
                "Slow receiver must report lagged messages, skipped={skipped}"
            );
        }
        Ok(_) => panic!("Expected slow receiver to lag behind 200 events on capacity 32"),
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Next event after lag must be readable
    let next_event = slow_rx.recv().await.expect("next event readable");
    assert!(next_event.is_upsert());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_broadcast_fanout_50_subscribers() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let num_subscribers = 50;
    let num_events = 20;

    let mut receivers = Vec::with_capacity(num_subscribers);
    for _ in 0..num_subscribers {
        receivers.push(store.subscribe());
    }

    assert_eq!(store.broadcast_bus().receiver_count(), num_subscribers);

    let mut set = JoinSet::new();
    for (idx, mut rx) in receivers.into_iter().enumerate() {
        set.spawn(async move {
            let mut count = 0;
            while count < num_events {
                let notif = rx.recv().await.expect("recv failed");
                assert_eq!(notif.collection(), "app.bsky.feed.post");
                count += 1;
            }
            (idx, count)
        });
    }

    // Publish events
    for i in 0..num_events {
        let input = RecordInput::new(
            "did:plc:fanout",
            "app.bsky.feed.post",
            format!("f_{i}"),
            format!("cid_{i}"),
            json!({"seq": i}),
            2000 + i as u64,
        );
        store.upsert_record(&input).expect("fanout upsert");
    }

    while let Some(res) = set.join_next().await {
        let (_sub_id, count) = res.expect("subscriber task failed");
        assert_eq!(count, num_events);
    }
}

// ============================================================================
// 5. Query Builder Edge Cases & Type Coercion Under Stress
// ============================================================================

#[test]
fn test_challenger_query_builder_injection_resistance() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    // Attempt SQL injection via JSON path
    let bad_paths = vec![
        "text' OR '1'='1",
        "text); DROP TABLE records; --",
        "nested.field; DELETE FROM records;",
        "field\0nullbyte",
        "field/with/slashes",
        "",
        "   ",
    ];

    for bad_path in bad_paths {
        let result = store
            .query("app.bsky.feed.post")
            .where_json(bad_path, QueryOp::Eq, json!("test"))
            .execute();
        assert!(
            result.is_err(),
            "Malicious or invalid path '{bad_path}' must be rejected by validator"
        );
    }
}

#[test]
fn test_challenger_query_builder_all_json_operators() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    let items = vec![
        ("k1", 10, "apple", true),
        ("k2", 20, "banana", false),
        ("k3", 30, "cherry", true),
        ("k4", 40, "date", false),
        ("k5", 50, "elderberry", true),
    ];

    for (k, num, fruit, active) in items {
        let input = RecordInput::new(
            "did:plc:test",
            "app.bsky.feed.post",
            k,
            format!("cid_{k}"),
            json!({
                "num": num,
                "fruit": fruit,
                "active": active,
            }),
            1000,
        );
        store.upsert_record(&input).expect("insert");
    }

    // Test Gt
    let gt_rows = store
        .query("app.bsky.feed.post")
        .where_json("num", QueryOp::Gt, json!(25))
        .execute()
        .expect("query gt");
    assert_eq!(gt_rows.len(), 3);

    // Test Lte
    let lte_rows = store
        .query("app.bsky.feed.post")
        .where_json("num", QueryOp::Lte, json!(30))
        .execute()
        .expect("query lte");
    assert_eq!(lte_rows.len(), 3);

    // Test Contains
    let contains_rows = store
        .query("app.bsky.feed.post")
        .where_json("fruit", QueryOp::Contains, json!("an"))
        .execute()
        .expect("query contains");
    assert_eq!(contains_rows.len(), 1); // "banana"

    // Test Boolean
    let bool_rows = store
        .query("app.bsky.feed.post")
        .where_json("active", QueryOp::Eq, json!(true))
        .execute()
        .expect("query bool");
    assert_eq!(bool_rows.len(), 3);

    // Test Ne
    let ne_rows = store
        .query("app.bsky.feed.post")
        .where_json("fruit", QueryOp::Ne, json!("banana"))
        .execute()
        .expect("query ne");
    assert_eq!(ne_rows.len(), 4);
}

// ============================================================================
// 6. Dual-Connection WAL & Multi-Process Concurrency Simulation
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_dual_store_file_wal_concurrency() {
    let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
    let db_path = temp_dir.path().join("dual_store.db");

    // Open two independent RecordStore handles to the SAME underlying database file
    let store_a = RecordStore::open(&db_path).expect("store A open failed");
    let store_b = RecordStore::open(&db_path).expect("store B open failed");

    let count_per_task = 50;
    let mut set = JoinSet::new();

    // Store A writer
    let sa = store_a.clone();
    set.spawn(async move {
        for i in 0..count_per_task {
            let input = RecordInput::new(
                "did:plc:store_a",
                "app.bsky.feed.post",
                format!("a_{i}"),
                format!("cid_a_{i}"),
                json!({"writer": "A", "seq": i}),
                1000 + i as u64,
            );
            sa.upsert_record(&input).expect("store A upsert failed");
            tokio::task::yield_now().await;
        }
    });

    // Store B writer
    let sb = store_b.clone();
    set.spawn(async move {
        for i in 0..count_per_task {
            let input = RecordInput::new(
                "did:plc:store_b",
                "app.bsky.feed.post",
                format!("b_{i}"),
                format!("cid_b_{i}"),
                json!({"writer": "B", "seq": i}),
                2000 + i as u64,
            );
            sb.upsert_record(&input).expect("store B upsert failed");
            tokio::task::yield_now().await;
        }
    });

    // Store A and Store B concurrent readers
    let sa_reader = store_a.clone();
    let sb_reader = store_b.clone();
    set.spawn(async move {
        for _ in 0..count_per_task {
            let _ = sa_reader.query("app.bsky.feed.post").limit(10).execute();
            let _ = sb_reader.query("app.bsky.feed.post").limit(10).execute();
            tokio::task::yield_now().await;
        }
    });

    while let Some(res) = set.join_next().await {
        res.expect("task join failed");
    }

    // Verify both stores can see ALL records written by both handles
    let rows_from_a = store_a
        .query("app.bsky.feed.post")
        .limit(200)
        .execute()
        .expect("query from A");
    let rows_from_b = store_b
        .query("app.bsky.feed.post")
        .limit(200)
        .execute()
        .expect("query from B");

    assert_eq!(rows_from_a.len(), count_per_task * 2);
    assert_eq!(rows_from_b.len(), count_per_task * 2);
}

// ============================================================================
// 7. Massive Batch & Large Payload Stress Tests
// ============================================================================

#[test]
fn test_challenger_massive_batch_transaction_2000_records() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let count = 2000;

    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        batch.push(RecordInput::new(
            format!("did:plc:bulk_{}", i % 10),
            "app.bsky.feed.post",
            format!("post_{i}"),
            format!("cid_{i}"),
            json!({
                "index": i,
                "score": i * 3,
                "title": format!("Post #{i} title")
            }),
            i as u64,
        ));
    }

    store
        .upsert_records_batch(&batch)
        .expect("massive batch upsert failed");

    // Query with sorting and pagination
    let paged = store
        .query("app.bsky.feed.post")
        .order_by("indexed_at", SortDirection::Desc)
        .limit(50)
        .offset(100)
        .execute()
        .expect("paged query failed");

    assert_eq!(paged.len(), 50);
    assert_eq!(paged[0].indexed_at, (count - 101) as u64);
    assert_eq!(paged[49].indexed_at, (count - 150) as u64);
}

#[test]
fn test_challenger_large_payload_and_deep_json() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    // 200KB payload with unicode, emojis, and deeply nested objects
    let large_string = "🚀 Firehose & ATProto 🌐 — Unicode 測試 🌟 ".repeat(4000);
    let deep_json = json!({
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "level5": {
                            "level6": {
                                "level7": {
                                    "target_val": "deep_target_found",
                                    "numeric": 999999,
                                    "flag": true
                                }
                            }
                        }
                    }
                }
            }
        },
        "tags": ["alpha", "beta", "gamma", "delta", "epsilon"],
        "large_text": large_string,
        "emojis": "🔥🔥🔥🎉🚀"
    });

    let input = RecordInput::new(
        "did:plc:heavy",
        "app.bsky.feed.post",
        "deep_post",
        "cid_deep_large",
        deep_json,
        1_700_000_000,
    );

    store.upsert_record(&input).expect("insert heavy record");

    // Fetch by URI
    let fetched = store
        .get_record(&input.uri)
        .expect("get failed")
        .expect("record missing");
    assert_eq!(fetched.cid, "cid_deep_large");
    assert_eq!(
        fetched.record_json["level1"]["level2"]["level3"]["level4"]["level5"]["level6"]["level7"]
            ["target_val"],
        "deep_target_found"
    );

    // Query using deeply nested JSON path
    let query_res = store
        .query("app.bsky.feed.post")
        .where_json(
            "level1.level2.level3.level4.level5.level6.level7.target_val",
            QueryOp::Eq,
            json!("deep_target_found"),
        )
        .execute()
        .expect("deep query failed");
    assert_eq!(query_res.len(), 1);
    assert_eq!(query_res[0].uri, input.uri);

    // Query array index
    let array_res = store
        .query("app.bsky.feed.post")
        .where_json("tags[2]", QueryOp::Eq, json!("gamma"))
        .execute()
        .expect("array query failed");
    assert_eq!(array_res.len(), 1);
    assert_eq!(array_res[0].uri, input.uri);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_tombstones_and_pagination() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let total_records: usize = 300;
    let delete_interval: usize = 3; // Delete 1 in every 3 records (100 deleted, 200 active)

    for i in 0..total_records {
        let input = RecordInput::new(
            "did:plc:pagination_stress",
            "app.bsky.feed.post",
            format!("post_{i:04}"),
            format!("cid_{i}"),
            json!({"index": i}),
            i as u64,
        );
        store.upsert_record(&input).expect("insert");

        if i % delete_interval == 0 {
            store
                .soft_delete_record(&input.uri, i as u64)
                .expect("delete");
        }
    }

    // Concurrently page through the entire active collection in chunks of 25
    let mut paged_uris = Vec::new();
    let mut offset = 0;
    let page_size = 25;

    loop {
        let page = store
            .query("app.bsky.feed.post")
            .order_by("rkey", SortDirection::Asc)
            .limit(page_size)
            .offset(offset)
            .execute()
            .expect("page query");

        if page.is_empty() {
            break;
        }

        for row in page {
            assert!(!row.is_deleted, "Active query must never return tombstones");
            paged_uris.push(row.uri);
        }
        offset += page_size;
    }

    let expected_active_count = total_records - total_records.div_ceil(delete_interval);
    assert_eq!(
        paged_uris.len(),
        expected_active_count,
        "Pagination must retrieve exactly all active records without duplication or omission"
    );
}
