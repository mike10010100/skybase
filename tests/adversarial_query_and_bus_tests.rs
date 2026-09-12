//! Adversarial empirical challenge tests for QueryBuilder and Broadcast Bus.
//!
//! Evaluates:
//! 1. JSON1 query extraction with complex nested objects, arrays, special characters, unicode, emojis, and SQL injection payloads.
//! 2. Deep pagination stress (`limit 0`, `offset` overflow, `offset` without limit, negative number filtering/sorting).
//! 3. Broadcast bus ring-buffer overflow: blasting thousands of events to slow receivers and verifying non-blocking `RecvError::Lagged` behavior and recovery.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

use skybase::{parse_at_uri, BroadcastBus, QueryOp, RecordInput, RecordStore, SortDirection};

fn helper_make_record(
    did: &str,
    collection: &str,
    rkey: &str,
    payload: serde_json::Value,
    indexed_at: u64,
) -> RecordInput {
    RecordInput::new(
        did,
        collection,
        rkey,
        format!("cid_{rkey}"),
        payload,
        indexed_at,
    )
}

// ============================================================================
// 1. JSON1 Extraction Robustness & Complex Structure Edge Cases
// ============================================================================

#[test]
fn test_adv_json1_deeply_nested_objects_and_arrays() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.nested";

    // 10 levels of object nesting
    let record1 = helper_make_record(
        "did:plc:alice",
        coll,
        "deep1",
        json!({
            "l1": {
                "l2": {
                    "l3": {
                        "l4": {
                            "l5": {
                                "l6": {
                                    "l7": {
                                        "l8": {
                                            "l9": {
                                                "target": "found_at_l10",
                                                "score": 999
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }),
        1000,
    );

    let record2 = helper_make_record(
        "did:plc:bob",
        coll,
        "deep2",
        json!({
            "l1": {
                "l2": {
                    "l3": {
                        "l4": {
                            "l5": {
                                "l6": {
                                    "l7": {
                                        "l8": {
                                            "l9": {
                                                "target": "other_value",
                                                "score": 100
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }),
        2000,
    );

    store.upsert_record(&record1).expect("upsert record1");
    store.upsert_record(&record2).expect("upsert record2");

    // Query 10 levels deep
    let deep_path = "l1.l2.l3.l4.l5.l6.l7.l8.l9.target";
    let rows = store
        .query(coll)
        .where_json(deep_path, QueryOp::Eq, json!("found_at_l10"))
        .execute()
        .expect("query deep path");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].rkey, "deep1");

    // Numeric comparison at depth 10
    let score_path = "l1.l2.l3.l4.l5.l6.l7.l8.l9.score";
    let score_rows = store
        .query(coll)
        .where_json(score_path, QueryOp::Gte, json!(500))
        .execute()
        .expect("query deep score");

    assert_eq!(score_rows.len(), 1);
    assert_eq!(score_rows[0].rkey, "deep1");
}

#[test]
fn test_adv_json1_array_indexing_and_matrices() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.arrays";

    let r1 = helper_make_record(
        "did:plc:alice",
        coll,
        "arr1",
        json!({
            "tags": ["atproto", "rust", "sqlite"],
            "matrix": [
                [10, 20],
                [30, 40]
            ],
            "objects": [
                { "name": "first", "id": 1 },
                { "name": "second", "id": 2 }
            ]
        }),
        1000,
    );

    let r2 = helper_make_record(
        "did:plc:bob",
        coll,
        "arr2",
        json!({
            "tags": ["golang", "postgres"],
            "matrix": [
                [50, 60],
                [70, 80]
            ],
            "objects": [
                { "name": "alpha", "id": 10 },
                { "name": "beta", "id": 20 }
            ]
        }),
        2000,
    );

    store.upsert_record(&r1).expect("upsert r1");
    store.upsert_record(&r2).expect("upsert r2");

    // Query array index 0
    let res = store
        .query(coll)
        .where_json("tags[0]", QueryOp::Eq, json!("atproto"))
        .execute()
        .expect("query tags[0]");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].rkey, "arr1");

    // Query 2D matrix element: matrix[1][0] == 30 for arr1
    let mat_res = store
        .query(coll)
        .where_json("matrix[1][0]", QueryOp::Eq, json!(30))
        .execute()
        .expect("query matrix[1][0]");
    assert_eq!(mat_res.len(), 1);
    assert_eq!(mat_res[0].rkey, "arr1");

    // Query object within array: objects[1].name == "beta"
    let obj_arr_res = store
        .query(coll)
        .where_json("objects[1].name", QueryOp::Eq, json!("beta"))
        .execute()
        .expect("query objects[1].name");
    assert_eq!(obj_arr_res.len(), 1);
    assert_eq!(obj_arr_res[0].rkey, "arr2");

    // Out-of-bounds array indexing safely produces 0 results (no error, no panic)
    let oob_res = store
        .query(coll)
        .where_json("tags[999]", QueryOp::Eq, json!("something"))
        .execute()
        .expect("query tags[999] should succeed with empty results");
    assert_eq!(oob_res.len(), 0);

    // Out-of-bounds array indexing checking IS NULL
    let oob_null = store
        .query(coll)
        .where_json("tags[999]", QueryOp::Eq, serde_json::Value::Null)
        .execute()
        .expect("query tags[999] IS NULL");
    assert_eq!(oob_null.len(), 2);
}

#[test]
fn test_adv_json1_unicode_emojis_rtl_and_special_chars() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.unicode";

    let emoji_payload = "🚀 BlueSky on ATProto 🔥 🦀 💯";
    let cjk_payload = "日本語のテキスト • 中文测试 • 한국어 시험";
    let rtl_payload = "مرحبا بالعالم • שלום עולם";
    let special_escapes = "Quotes: \" ' ` | Backslash: \\ | Newline: \n\r | Tab: \t | Symbols: !@#$%^&*()_+-=[]{}|;:,.<>?/";

    let r1 = helper_make_record(
        "did:plc:user1",
        coll,
        "emoji",
        json!({ "text": emoji_payload, "tag": "🚀" }),
        1000,
    );
    let r2 = helper_make_record(
        "did:plc:user2",
        coll,
        "cjk",
        json!({ "text": cjk_payload, "tag": "日本語" }),
        2000,
    );
    let r3 = helper_make_record(
        "did:plc:user3",
        coll,
        "rtl",
        json!({ "text": rtl_payload, "tag": "مرحبا" }),
        3000,
    );
    let r4 = helper_make_record(
        "did:plc:user4",
        coll,
        "escapes",
        json!({ "text": special_escapes, "tag": "symbols" }),
        4000,
    );

    store.upsert_record(&r1).expect("upsert r1");
    store.upsert_record(&r2).expect("upsert r2");
    store.upsert_record(&r3).expect("upsert r3");
    store.upsert_record(&r4).expect("upsert r4");

    // Exact emoji match
    let res_emoji = store
        .query(coll)
        .where_json("tag", QueryOp::Eq, json!("🚀"))
        .execute()
        .expect("query emoji");
    assert_eq!(res_emoji.len(), 1);
    assert_eq!(res_emoji[0].rkey, "emoji");

    // Substring Contains with emoji
    let res_contains_emoji = store
        .query(coll)
        .where_json("text", QueryOp::Contains, json!("🦀"))
        .execute()
        .expect("contains emoji");
    assert_eq!(res_contains_emoji.len(), 1);
    assert_eq!(res_contains_emoji[0].rkey, "emoji");

    // Exact CJK match
    let res_cjk = store
        .query(coll)
        .where_json("tag", QueryOp::Eq, json!("日本語"))
        .execute()
        .expect("query cjk");
    assert_eq!(res_cjk.len(), 1);
    assert_eq!(res_cjk[0].rkey, "cjk");

    // Exact RTL match
    let res_rtl = store
        .query(coll)
        .where_json("tag", QueryOp::Eq, json!("مرحبا"))
        .execute()
        .expect("query rtl");
    assert_eq!(res_rtl.len(), 1);
    assert_eq!(res_rtl[0].rkey, "rtl");

    // Exact match on string with all special escapes and quotes
    let res_escapes = store
        .query(coll)
        .where_json("text", QueryOp::Eq, json!(special_escapes))
        .execute()
        .expect("query special escapes");
    assert_eq!(res_escapes.len(), 1);
    assert_eq!(res_escapes[0].rkey, "escapes");
}

#[test]
fn test_adv_json1_numeric_boundaries_and_negatives() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.numbers";

    let records = [
        ("n_min", json!({ "val": i64::MIN, "fval": -1e20 }), 100),
        ("n_neg100", json!({ "val": -100, "fval": -100.5 }), 200),
        ("n_neg1", json!({ "val": -1, "fval": -1.0 }), 300),
        ("n_zero", json!({ "val": 0, "fval": 0.0 }), 400),
        ("n_pos1", json!({ "val": 1, "fval": 1.0 }), 500),
        ("n_pos100", json!({ "val": 100, "fval": 100.5 }), 600),
        ("n_max", json!({ "val": i64::MAX, "fval": 1e20 }), 700),
    ];

    for (rkey, payload, ts) in records {
        store
            .upsert_record(&helper_make_record("did:plc:test", coll, rkey, payload, ts))
            .expect("upsert record");
    }

    // Query val < 0
    let negs = store
        .query(coll)
        .where_json("val", QueryOp::Lt, json!(0))
        .order_by("val", SortDirection::Asc)
        .execute()
        .expect("query val < 0");
    assert_eq!(negs.len(), 3);
    assert_eq!(negs[0].rkey, "n_min");
    assert_eq!(negs[1].rkey, "n_neg100");
    assert_eq!(negs[2].rkey, "n_neg1");

    // Query val >= -100 AND val <= 1
    let range = store
        .query(coll)
        .where_json("val", QueryOp::Gte, json!(-100))
        .where_json("val", QueryOp::Lte, json!(1))
        .order_by("val", SortDirection::Asc)
        .execute()
        .expect("query range");
    assert_eq!(range.len(), 4);
    assert_eq!(range[0].rkey, "n_neg100");
    assert_eq!(range[1].rkey, "n_neg1");
    assert_eq!(range[2].rkey, "n_zero");
    assert_eq!(range[3].rkey, "n_pos1");

    // Float comparison: fval > 0.0
    let pos_floats = store
        .query(coll)
        .where_json("fval", QueryOp::Gt, json!(0.0))
        .order_by("fval", SortDirection::Asc)
        .execute()
        .expect("query fval > 0.0");
    assert_eq!(pos_floats.len(), 3);
    assert_eq!(pos_floats[0].rkey, "n_pos1");
    assert_eq!(pos_floats[1].rkey, "n_pos100");
    assert_eq!(pos_floats[2].rkey, "n_max");
}

// ============================================================================
// 2. SQL Injection Fuzzing & Parameterization Resilience
// ============================================================================

#[test]
fn test_adv_sqli_fuzzing_in_json_paths_rejected() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.sqli_paths";

    store
        .upsert_record(&helper_make_record(
            "did:plc:victim",
            coll,
            "row1",
            json!({ "secret": "confidential_data" }),
            1000,
        ))
        .expect("upsert record");

    let malicious_paths = [
        "' OR 1=1 --",
        "'; DROP TABLE records; --",
        "secret' UNION SELECT * FROM records --",
        "a\"; DROP TABLE records; --",
        "path/*comment*/",
        "path; VACUUM;",
        "path' AND 1=0 UNION SELECT 1,2,3,4,5,6,7,8 --",
        "$$$invalid",
        "path with spaces",
        "path\nnewline",
        "path\0nullbyte",
    ];

    for bad_path in malicious_paths {
        let res = store
            .query(coll)
            .where_json(bad_path, QueryOp::Eq, json!("val"))
            .execute();

        assert!(
            res.is_err(),
            "Path '{bad_path}' MUST be rejected during query compilation"
        );
    }

    // Verify records table is completely unmolested
    let check = store.query(coll).execute().expect("table must exist");
    assert_eq!(check.len(), 1);
    assert_eq!(check[0].rkey, "row1");
}

#[test]
fn test_adv_sqli_fuzzing_in_json_values_bound_safely() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.sqli_values";

    // Insert innocent record
    store
        .upsert_record(&helper_make_record(
            "did:plc:target",
            coll,
            "innocent",
            json!({ "content": "normal user post", "author": "alice" }),
            1000,
        ))
        .expect("upsert innocent");

    // Insert record whose content literally IS a SQL injection payload
    let literal_sqli = "'; DROP TABLE records; --";
    store
        .upsert_record(&helper_make_record(
            "did:plc:hacker",
            coll,
            "sqli_post",
            json!({ "content": literal_sqli, "author": "mallory" }),
            2000,
        ))
        .expect("upsert sqli record");

    let sqli_test_payloads = [
        "' OR 1=1 --",
        "\" OR 1=1 --",
        "' OR 'x'='x",
        "'; DROP TABLE records; --",
        "'; DELETE FROM records; --",
        "admin' UNION SELECT 1,2,3,4,5,6,7,8 --",
        "1' OR '1'='1' /*",
        "'; ATTACH DATABASE '/tmp/pwn.db' AS pwn; --",
    ];

    for payload in sqli_test_payloads {
        let rows = store
            .query(coll)
            .where_json("content", QueryOp::Eq, json!(payload))
            .execute()
            .expect("query with SQLi payload in value must execute safely");

        if payload == literal_sqli {
            // Literal match
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].rkey, "sqli_post");
        } else {
            // No matches, zero injection
            assert_eq!(rows.len(), 0);
        }
    }

    // Substring Contains with SQL injection payload
    let contains_rows = store
        .query(coll)
        .where_json("content", QueryOp::Contains, json!("DROP TABLE"))
        .execute()
        .expect("Contains query with SQLi payload must execute safely");
    assert_eq!(contains_rows.len(), 1);
    assert_eq!(contains_rows[0].rkey, "sqli_post");

    // Verify all records in table survived
    let all = store.query(coll).execute().expect("all records intact");
    assert_eq!(all.len(), 2);
}

#[test]
fn test_adv_sqli_fuzzing_in_collection_did_and_order_by() {
    let store = RecordStore::open_in_memory().expect("store open");

    // Malicious collection string
    let evil_col = "app.bsky.feed.post' OR 1=1 --";
    let rows = store
        .query(evil_col)
        .execute()
        .expect("query with SQLi in collection must execute safely");
    assert_eq!(rows.len(), 0);

    // Empty collection string must be rejected with error
    let empty_col_res = store.query("").execute();
    assert!(empty_col_res.is_err());

    let spaces_col_res = store.query("    ").execute();
    assert!(spaces_col_res.is_err());

    // Malicious DID string
    let evil_did = "did:plc:alice' UNION SELECT * FROM records --";
    let did_rows = store
        .query("app.bsky.feed.post")
        .did(evil_did)
        .execute()
        .expect("query with SQLi in did must execute safely");
    assert_eq!(did_rows.len(), 0);

    // Malicious order_by field
    let evil_order_field = "indexed_at; DROP TABLE records; --";
    let order_res = store
        .query("app.bsky.feed.post")
        .order_by(evil_order_field, SortDirection::Asc)
        .execute();
    assert!(
        order_res.is_err(),
        "Invalid order_by field must be rejected"
    );
}

// ============================================================================
// 3. Deep Pagination & Ordering Stress
// ============================================================================

#[test]
fn test_adv_pagination_stress_and_boundaries() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.pagination";
    let total_records = 250;

    for i in 1..=total_records {
        store
            .upsert_record(&helper_make_record(
                "did:plc:pager",
                coll,
                &format!("item_{i:04}"),
                json!({ "index": i, "even": i % 2 == 0 }),
                i as u64 * 10,
            ))
            .expect("upsert record");
    }

    // 1. limit(0) returns empty result
    let res_zero = store
        .query(coll)
        .limit(0)
        .execute()
        .expect("limit(0) query");
    assert_eq!(res_zero.len(), 0);

    // 2. limit(0) with offset
    let res_zero_offset = store
        .query(coll)
        .limit(0)
        .offset(100)
        .execute()
        .expect("limit(0) offset(100)");
    assert_eq!(res_zero_offset.len(), 0);

    // 3. offset exactly equal to total_records
    let res_offset_exact = store
        .query(coll)
        .offset(total_records as u32)
        .execute()
        .expect("offset exact");
    assert_eq!(res_offset_exact.len(), 0);

    // 4. offset beyond total_records
    let res_offset_overflow = store
        .query(coll)
        .offset(10_000)
        .execute()
        .expect("offset 10000");
    assert_eq!(res_offset_overflow.len(), 0);

    // 5. extreme offset (u32::MAX)
    let res_offset_u32_max = store
        .query(coll)
        .offset(u32::MAX)
        .execute()
        .expect("offset u32::MAX");
    assert_eq!(res_offset_u32_max.len(), 0);

    // 6. offset without limit (SQLite LIMIT -1 OFFSET ?)
    let res_offset_only = store
        .query(coll)
        .order_by("indexed_at", SortDirection::Asc)
        .offset(200)
        .execute()
        .expect("offset 200 without limit");
    assert_eq!(res_offset_only.len(), 50);
    assert_eq!(res_offset_only[0].rkey, "item_0201");
    assert_eq!(res_offset_only[49].rkey, "item_0250");

    // 7. Full pagination loop over all records in pages of 37
    let page_size = 37;
    let mut collected = Vec::new();
    let mut current_offset = 0;

    loop {
        let page = store
            .query(coll)
            .order_by("indexed_at", SortDirection::Asc)
            .limit(page_size)
            .offset(current_offset)
            .execute()
            .expect("page fetch");

        if page.is_empty() {
            break;
        }

        current_offset += page.len() as u32;
        collected.extend(page);
    }

    assert_eq!(collected.len(), total_records);
    for (idx, row) in collected.iter().enumerate() {
        let expected_key = format!("item_{:04}", idx + 1);
        assert_eq!(row.rkey, expected_key);
    }
}

#[test]
fn test_adv_order_by_nested_json_paths_and_null_handling() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.order";

    let r1 = helper_make_record(
        "did:plc:a",
        coll,
        "k1",
        json!({ "meta": { "priority": 10 } }),
        100,
    );
    let r2 = helper_make_record(
        "did:plc:b",
        coll,
        "k2",
        json!({ "meta": { "priority": 50 } }),
        200,
    );
    let r3 = helper_make_record(
        "did:plc:c",
        coll,
        "k3",
        json!({ "meta": { "priority": 5 } }),
        300,
    );
    let r4 = helper_make_record(
        "did:plc:d",
        coll,
        "k4",
        json!({ "meta": { "other": "no priority" } }), // null priority
        400,
    );

    store.upsert_record(&r1).expect("upsert");
    store.upsert_record(&r2).expect("upsert");
    store.upsert_record(&r3).expect("upsert");
    store.upsert_record(&r4).expect("upsert");

    // Order by nested JSON path ASC (SQLite sorts NULL first)
    let asc_rows = store
        .query(coll)
        .order_by("meta.priority", SortDirection::Asc)
        .execute()
        .expect("order by meta.priority asc");

    assert_eq!(asc_rows.len(), 4);
    assert_eq!(asc_rows[0].rkey, "k4", "NULL priority sorts first in ASC");
    assert_eq!(asc_rows[1].rkey, "k3"); // 5
    assert_eq!(asc_rows[2].rkey, "k1"); // 10
    assert_eq!(asc_rows[3].rkey, "k2"); // 50

    // Order by nested JSON path DESC (50, 10, 5, NULL)
    let desc_rows = store
        .query(coll)
        .order_by("meta.priority", SortDirection::Desc)
        .execute()
        .expect("order by meta.priority desc");

    assert_eq!(desc_rows.len(), 4);
    assert_eq!(desc_rows[0].rkey, "k2"); // 50
    assert_eq!(desc_rows[1].rkey, "k1"); // 10
    assert_eq!(desc_rows[2].rkey, "k3"); // 5
    assert_eq!(desc_rows[3].rkey, "k4", "NULL priority sorts last in DESC");
}

// ============================================================================
// 4. Broadcast Bus Ring-Buffer Overflow & Concurrency Stress
// ============================================================================

#[tokio::test]
async fn test_adv_bus_ring_buffer_blast_thousands_lagged_overflow() {
    let capacity = 32;
    let bus = BroadcastBus::new(capacity).expect("bus creation");

    // Subscriber 1 is a slow receiver that never calls recv() during the blast
    let mut slow_rx = bus.subscribe();

    // Blast 10,000 events into the 32-slot ring buffer
    let blast_count = 10_000;
    for i in 0..blast_count {
        let delivered = bus.publish_delete(
            format!("at://did:plc:test/app.bsky.feed.post/{i}"),
            "did:plc:test",
            "app.bsky.feed.post",
            format!("{i}"),
        );
        assert_eq!(delivered, 1, "Must deliver to active subscriber");
    }

    // Receiver must lag without blocking publisher
    match slow_rx.recv().await {
        Err(RecvError::Lagged(skipped)) => {
            // Buffer capacity is 32, so skipped should be 10000 - 32 = 9968
            assert_eq!(
                skipped,
                (blast_count - capacity) as u64,
                "Must accurately report skipped count"
            );
        }
        other => panic!("Expected RecvError::Lagged, got {other:?}"),
    }

    // Immediately after Lagged, next call to recv() MUST succeed with the oldest
    // surviving message in the ring buffer
    let surviving_first = slow_rx
        .recv()
        .await
        .expect("next recv must succeed with surviving event");
    assert_eq!(
        surviving_first.rkey(),
        format!("{}", blast_count - capacity)
    );

    // Drain the remaining (capacity - 1) surviving events
    for i in (blast_count - capacity + 1)..blast_count {
        let event = slow_rx.recv().await.expect("drain surviving event");
        assert_eq!(event.rkey(), format!("{i}"));
    }

    // Publish 5 new events after catching up and verify immediate clean reception
    for i in 0..5 {
        bus.publish_delete(
            format!("at://did:plc:test/app.bsky.feed.post/post_catchup_{i}"),
            "did:plc:test",
            "app.bsky.feed.post",
            format!("catchup_{i}"),
        );
        let event = slow_rx.recv().await.expect("receive after catchup");
        assert_eq!(event.rkey(), format!("catchup_{i}"));
    }
}

#[tokio::test]
async fn test_adv_bus_heterogeneous_subscriber_speeds() {
    let bus = BroadcastBus::new(64).expect("bus creation");

    let mut fast_rx = bus.subscribe();
    let mut slow_rx = bus.subscribe();

    let blast_count = 500;
    let fast_received = Arc::new(AtomicUsize::new(0));
    let fast_received_clone = fast_received.clone();
    let fast_lagged = Arc::new(AtomicUsize::new(0));
    let fast_lagged_clone = fast_lagged.clone();

    // Fast receiver task drains continuously and handles Lagged
    let fast_handle = tokio::spawn(async move {
        loop {
            match fast_rx.recv().await {
                Ok(_event) => {
                    let count = fast_received_clone.fetch_add(1, Ordering::Relaxed);
                    if count + 1 == blast_count {
                        break;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    fast_lagged_clone.fetch_add(skipped as usize, Ordering::Relaxed);
                }
                Err(RecvError::Closed) => break,
            }
        }
    });

    // Yield so fast_handle starts listening
    tokio::task::yield_now().await;

    // Publish with cooperative yields so fast receiver drains concurrently
    for i in 0..blast_count {
        bus.publish_delete(
            format!("at://did:plc:test/col/{i}"),
            "did:plc:test",
            "col",
            format!("{i}"),
        );
        if i % 10 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // Wait for fast receiver
    let _ = tokio::time::timeout(Duration::from_millis(1000), fast_handle).await;

    let received = fast_received.load(Ordering::Relaxed);
    let lagged = fast_lagged.load(Ordering::Relaxed);
    assert_eq!(
        received + lagged,
        blast_count,
        "Total accounted events must equal blast_count"
    );
    assert!(
        received > 0,
        "Fast receiver must receive events concurrently"
    );

    // Slow receiver never read during blast, so it MUST lag
    match slow_rx.recv().await {
        Err(RecvError::Lagged(skipped)) => {
            assert!(
                skipped >= (blast_count - 64) as u64,
                "Slow receiver lagged as expected"
            );
        }
        other => panic!("Expected RecvError::Lagged for slow receiver, got {other:?}"),
    }
}

#[test]
fn test_adv_bus_zero_subscribers_high_throughput_blast() {
    let bus = BroadcastBus::new(128).expect("bus creation");
    assert_eq!(bus.receiver_count(), 0);

    // Blasting 10,000 events to 0 subscribers must return 0 without panics or memory leaks
    for i in 0..10_000 {
        let delivered = bus.publish_delete(
            format!("at://did:plc:ghost/col/{i}"),
            "did:plc:ghost",
            "col",
            format!("{i}"),
        );
        assert_eq!(delivered, 0);
    }
}

#[tokio::test]
async fn test_adv_bus_concurrent_multi_publisher_stress() {
    let bus = Arc::new(BroadcastBus::new(256).expect("bus creation"));
    let mut rx = bus.subscribe();

    let publisher_count = 10;
    let events_per_pub = 200;
    let mut pub_handles = Vec::new();

    for pub_id in 0..publisher_count {
        let bus_clone = bus.clone();
        pub_handles.push(tokio::spawn(async move {
            for i in 0..events_per_pub {
                bus_clone.publish_delete(
                    format!("at://did:plc:p{pub_id}/col/{i}"),
                    format!("did:plc:p{pub_id}"),
                    "col",
                    format!("{pub_id}_{i}"),
                );
            }
        }));
    }

    for handle in pub_handles {
        handle.await.expect("publisher task join");
    }

    // Drain receiver: total published is 2,000, capacity is 256.
    // It should report Lagged and then yield the remaining surviving events cleanly.
    let mut total_survived = 0;
    match rx.recv().await {
        Err(RecvError::Lagged(skipped)) => {
            assert!(skipped > 0, "Expected positive skipped count under load");
        }
        Ok(_) => {
            total_survived += 1;
        }
        Err(e) => panic!("Unexpected error {e:?}"),
    }

    while rx.try_recv().is_ok() {
        total_survived += 1;
    }

    assert!(
        total_survived <= 256,
        "Surviving messages cannot exceed buffer capacity"
    );
}

#[tokio::test]
async fn test_adv_record_store_integrated_broadcast_and_query_stress() {
    let store = RecordStore::open_in_memory().expect("store open");
    let mut rx = store.subscribe();

    let coll = "app.bsky.test.integrated";
    let count = 200;

    // Concurrently upsert 200 records
    for i in 0..count {
        let record = helper_make_record(
            "did:plc:concurrent",
            coll,
            &format!("k_{i}"),
            json!({ "seq": i }),
            i as u64,
        );
        store.upsert_record(&record).expect("upsert record");
    }

    // Verify all 200 are queryable
    let query_all = store.query(coll).execute().expect("query all");
    assert_eq!(query_all.len(), count);

    // Verify broadcast received events
    let mut received_count = 0;
    while let Ok(event) = rx.try_recv() {
        assert!(event.is_upsert());
        assert_eq!(event.collection(), coll);
        received_count += 1;
    }

    // In-memory default broadcast capacity is 1024, so all 200 must be received without lag
    assert_eq!(received_count, count);

    // Soft delete all 200
    for i in 0..count {
        store
            .soft_delete_record(&format!("at://did:plc:concurrent/{coll}/k_{i}"))
            .expect("soft delete");
    }

    // Query active records -> must be 0
    let active = store.query(coll).execute().expect("query active");
    assert_eq!(active.len(), 0);

    // Query including deleted -> must be 200
    let all = store
        .query(coll)
        .include_deleted(true)
        .execute()
        .expect("query all including deleted");
    assert_eq!(all.len(), count);

    // Verify 200 delete events were received
    let mut del_count = 0;
    while let Ok(event) = rx.try_recv() {
        assert!(event.is_delete());
        del_count += 1;
    }
    assert_eq!(del_count, count);
}

#[test]
fn test_adv_parse_at_uri_edge_cases() {
    // Valid AT URIs
    assert_eq!(
        parse_at_uri("at://did:plc:alice/app.bsky.feed.post/3kabc"),
        Some((
            "did:plc:alice".to_string(),
            "app.bsky.feed.post".to_string(),
            "3kabc".to_string()
        ))
    );

    // Invalid AT URIs: no prefix, missing segments, extra slashes, empty parts
    assert_eq!(parse_at_uri("https://bsky.app"), None);
    assert_eq!(parse_at_uri("at://"), None);
    assert_eq!(parse_at_uri("at://did:plc:alice"), None);
    assert_eq!(parse_at_uri("at://did:plc:alice/app.bsky.feed.post"), None);
    assert_eq!(
        parse_at_uri("at://did:plc:alice/app.bsky.feed.post/rkey/extra"),
        None
    );
    assert_eq!(parse_at_uri("at:///app.bsky.feed.post/rkey"), None);
    assert_eq!(parse_at_uri("at://did:plc:alice//rkey"), None);
    assert_eq!(parse_at_uri("at://did:plc:alice/app.bsky.feed.post/"), None);
    assert_eq!(parse_at_uri(""), None);
}

// ============================================================================
// 5. Extended Fuzzing & Combinatorial Stress Tests
// ============================================================================

#[test]
fn test_adv_json1_path_fuzzing_comprehensive_matrix() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.fuzz_paths";

    let fuzzed_invalid_paths = [
        "foo@bar", "foo#bar", "foo$bar", "foo%bar", "foo^bar", "foo&bar", "foo*bar", "foo(bar)",
        "foo+bar", "foo=bar", "foo{bar}", "foo|bar", "foo:bar", "foo\"bar", "foo'bar", "foo<bar>",
        "foo?bar", "foo,bar", "foo/bar", "foo\\bar", "foo`bar", "foo~bar", "foo!bar", "foo\nbar",
        "foo\tbar", "foo\rbar", "foo\0bar", "foo bar", " foo", "foo ", "foo..bar", ".foo", "foo.",
        "foo[", "foo]", "foo[]", "foo[abc]", "foo[-1]",
    ];

    for path in fuzzed_invalid_paths {
        // Even if some like foo[abc] might pass character filter, validate that the query fails or succeeds safely without panic
        let res = store
            .query(coll)
            .where_json(path, QueryOp::Eq, json!(1))
            .execute();
        // The query should either be rejected with SkybaseError::Index or execute with empty result
        if let Err(err) = res {
            assert!(
                matches!(
                    err,
                    skybase::SkybaseError::Index(_) | skybase::SkybaseError::Storage(_)
                ),
                "Path '{path}' error must be typed Index or Storage error: {err:?}"
            );
        }
    }
}

#[test]
fn test_adv_sqli_fuzzing_advanced_vectors() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.sqli_advanced";

    store
        .upsert_record(&helper_make_record(
            "did:plc:admin",
            coll,
            "secret_record",
            json!({ "api_key": "sk_live_12345", "role": "admin" }),
            5000,
        ))
        .expect("upsert record");

    let advanced_sqli = [
        "admin'--",
        "admin' /*",
        "' OR ''='",
        "1 OR 1=1",
        "') OR ('1'='1",
        "1' ORDER BY 1--",
        "1' GROUP BY 1--",
        "'; ATTACH DATABASE ':memory:' AS evil; --",
        "1; CREATE TABLE pwned(id INT);",
        "' UNION ALL SELECT null, null, null, null, null, null, null, null --",
    ];

    for payload in advanced_sqli {
        let res = store
            .query(coll)
            .where_json("role", QueryOp::Eq, json!(payload))
            .execute()
            .expect("query with advanced SQLi payload must not fail");
        assert_eq!(res.len(), 0);
    }

    // Verify records table is pristine
    let check = store.query(coll).execute().expect("check intact");
    assert_eq!(check.len(), 1);
    assert_eq!(check[0].rkey, "secret_record");
}

#[test]
fn test_adv_query_multiple_clauses_combinatorial() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.combinatorial";

    for i in 1..=50 {
        store
            .upsert_record(&helper_make_record(
                &format!("did:plc:user_{}", i % 5),
                coll,
                &format!("r_{i:03}"),
                json!({
                    "author": format!("user_{}", i % 5),
                    "views": i * 10,
                    "rating": i % 5 + 1,
                    "published": i % 2 == 0,
                    "metadata": {
                        "category": if i % 3 == 0 { "tech" } else { "news" },
                        "active": true
                    }
                }),
                i as u64 * 100,
            ))
            .expect("upsert record");
    }

    // Combine 5 JSON1 clauses + DID + sorting + limit + offset
    let results = store
        .query(coll)
        .did("did:plc:user_0")
        .where_json("published", QueryOp::Eq, json!(true))
        .where_json("views", QueryOp::Gte, json!(100))
        .where_json("rating", QueryOp::Lt, json!(5))
        .where_json("metadata.active", QueryOp::Eq, json!(true))
        .where_json("metadata.category", QueryOp::Eq, json!("tech"))
        .order_by("views", SortDirection::Desc)
        .limit(10)
        .offset(0)
        .execute()
        .expect("combinatorial query");

    // All results must satisfy all conditions
    for r in &results {
        assert_eq!(r.did, "did:plc:user_0");
        assert_eq!(r.record_json["published"], true);
        assert!(r.record_json["views"].as_i64().unwrap() >= 100);
        assert!(r.record_json["rating"].as_i64().unwrap() < 5);
        assert_eq!(r.record_json["metadata"]["active"], true);
        assert_eq!(r.record_json["metadata"]["category"], "tech");
    }
}

#[test]
fn test_adv_order_by_multi_column_hierarchical() {
    let store = RecordStore::open_in_memory().expect("store open");
    let coll = "app.bsky.test.multi_order";

    // Insert records with tie-breakers
    let records = [
        ("r1", json!({ "category": "A", "score": 10 }), 100),
        ("r2", json!({ "category": "A", "score": 20 }), 200),
        ("r3", json!({ "category": "A", "score": 10 }), 300),
        ("r4", json!({ "category": "B", "score": 10 }), 400),
        ("r5", json!({ "category": "B", "score": 30 }), 500),
    ];

    for (rkey, payload, ts) in records {
        store
            .upsert_record(&helper_make_record("did:plc:test", coll, rkey, payload, ts))
            .expect("upsert record");
    }

    // Sort by category ASC, score DESC, indexed_at ASC
    let rows = store
        .query(coll)
        .order_by("category", SortDirection::Asc)
        .order_by("score", SortDirection::Desc)
        .order_by("indexed_at", SortDirection::Asc)
        .execute()
        .expect("multi order query");

    assert_eq!(rows.len(), 5);
    // Category A: scores 20 (r2), then 10 (r1: ts 100, then r3: ts 300)
    assert_eq!(rows[0].rkey, "r2"); // A, 20
    assert_eq!(rows[1].rkey, "r1"); // A, 10, ts 100
    assert_eq!(rows[2].rkey, "r3"); // A, 10, ts 300
                                    // Category B: scores 30 (r5), then 10 (r4)
    assert_eq!(rows[3].rkey, "r5"); // B, 30
    assert_eq!(rows[4].rkey, "r4"); // B, 10
}

#[tokio::test]
async fn test_adv_bus_repeated_lags_and_recovery_cycles() {
    let capacity = 16;
    let bus = BroadcastBus::new(capacity).expect("bus creation");
    let mut rx = bus.subscribe();

    for cycle in 1..=5 {
        // Blast 100 events
        for i in 0..100 {
            bus.publish_delete(
                format!("at://did:plc:cycle/col/{cycle}_{i}"),
                "did:plc:cycle",
                "col",
                format!("{cycle}_{i}"),
            );
        }

        // Receiver must lag on each cycle
        match rx.recv().await {
            Err(RecvError::Lagged(skipped)) => {
                assert!(
                    skipped >= (100 - capacity) as u64,
                    "Cycle {cycle}: must lag with positive skipped count"
                );
            }
            other => panic!("Cycle {cycle}: expected Lagged, got {other:?}"),
        }

        // Must recover immediately and receive surviving events
        let surviving = rx.recv().await.expect("must receive after lag");
        assert!(
            surviving.rkey().starts_with(&format!("{cycle}_")),
            "Surviving event must match current cycle"
        );

        // Drain remainder
        while rx.try_recv().is_ok() {}
    }
}

#[tokio::test]
async fn test_adv_bus_concurrent_high_load_sqlite_and_subscribers() {
    let store = Arc::new(RecordStore::open_in_memory().expect("store open"));
    let subscriber_count = 5;
    let mut receivers = Vec::new();
    for _ in 0..subscriber_count {
        receivers.push(store.subscribe());
    }

    let coll = "app.bsky.test.heavy_load";
    let writer_count = 10;
    let records_per_writer = 50;
    let mut writer_handles = Vec::new();

    // Spawn 10 concurrent writers
    for w in 0..writer_count {
        let s = store.clone();
        writer_handles.push(tokio::spawn(async move {
            for i in 0..records_per_writer {
                let rec = helper_make_record(
                    &format!("did:plc:writer_{w}"),
                    coll,
                    &format!("rec_{w}_{i}"),
                    json!({ "writer": w, "item": i }),
                    (w * 1000 + i) as u64,
                );
                s.upsert_record(&rec).expect("concurrent upsert");
            }
        }));
    }

    for h in writer_handles {
        h.await.expect("writer task join");
    }

    // Verify all 500 records are present in SQLite
    let all_records = store.query(coll).execute().expect("query all");
    assert_eq!(all_records.len(), writer_count * records_per_writer);

    // Verify all subscribers can drain without deadlocks or panics
    for (idx, mut rx) in receivers.into_iter().enumerate() {
        let mut count = 0;
        let mut lagged = 0;
        loop {
            match rx.try_recv() {
                Ok(_) => count += 1,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(s)) => lagged += s,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            }
        }
        // Total accounted events must equal total published
        assert_eq!(
            count + lagged as usize,
            writer_count * records_per_writer,
            "Subscriber {idx} must account for all events"
        );
    }
}
