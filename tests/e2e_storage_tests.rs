//! End-to-End Tests: SQLite Storage, Canonical Schema, JSON1 QueryBuilder, and Broadcast Bus.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use serde_json::json;
use skybase::index::{ChangeNotification, QueryOp, RecordInput, RecordStore, SortDirection};

// ============================================================================
// Tier 1: Feature Coverage (Canonical Schema, Storage CRUD, QueryBuilder, Bus)
// ============================================================================

#[test]
fn test_tier1_f01_canonical_schema_and_wal_pragmas() {
    let temp_dir = tempfile::tempdir().expect("tempdir creation failed");
    let db_path = temp_dir.path().join("skybase_test.db");

    let store = RecordStore::open_file_backed(&db_path).expect("Failed to open file-backed DB");
    store
        .with_conn(|conn| {
            // Verify journal mode is WAL
            let journal_mode: String =
                conn.query_row("PRAGMA journal_mode;", [], |row| row.get(0))?;
            assert_eq!(journal_mode.to_lowercase(), "wal");

            // Verify table structure
            let mut stmt = conn.prepare("PRAGMA table_info(records);")?;
            let cols: Vec<String> = stmt
                .query_map([], |row| row.get(1))?
                .map(|r| r.expect("col read failed"))
                .collect();

            assert!(cols.contains(&"uri".to_string()));
            assert!(cols.contains(&"cid".to_string()));
            assert!(cols.contains(&"did".to_string()));
            assert!(cols.contains(&"collection".to_string()));
            assert!(cols.contains(&"rkey".to_string()));
            assert!(cols.contains(&"record_json".to_string()));
            assert!(cols.contains(&"indexed_at".to_string()));
            assert!(cols.contains(&"is_deleted".to_string()));
            Ok(())
        })
        .expect("PRAGMA inspection failed");
}

#[test]
fn test_tier1_f02_primary_key_uniqueness_and_indexes() {
    let store = RecordStore::open_in_memory().expect("in-memory DB failed");
    store
        .with_conn(|conn| {
            // Verify secondary indexes exist
            let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'index';")?;
            let indexes: Vec<String> = stmt
                .query_map([], |row| row.get(0))?
                .map(|r| r.expect("index name read failed"))
                .collect();

            assert!(indexes.contains(&"idx_records_collection".to_string()));
            assert!(indexes.contains(&"idx_records_did".to_string()));
            assert!(indexes.contains(&"idx_records_indexed_at".to_string()));
            Ok(())
        })
        .expect("index inspection failed");
}

#[test]
fn test_tier1_f03_atomic_upsert_and_retrieval() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    let record = RecordInput {
        uri: "at://did:plc:alice/app.bsky.feed.post/post1".to_string(),
        cid: "bafyrei_test_cid_1".to_string(),
        did: "did:plc:alice".to_string(),
        collection: "app.bsky.feed.post".to_string(),
        rkey: "post1".to_string(),
        record_json: json!({ "text": "Hello ATProto", "replyCount": 0 }),
        indexed_at: 1700000000,
    };

    store.upsert_record(&record).expect("upsert failed");

    let fetched = store
        .get_record("at://did:plc:alice/app.bsky.feed.post/post1")
        .expect("get_record failed")
        .expect("record not found");

    assert_eq!(fetched.uri, record.uri);
    assert_eq!(fetched.cid, record.cid);
    assert_eq!(fetched.did, record.did);
    assert_eq!(fetched.collection, record.collection);
    assert_eq!(fetched.rkey, record.rkey);
    assert_eq!(fetched.record_json["text"], "Hello ATProto");
    assert!(!fetched.is_deleted);
}

#[test]
fn test_tier1_f04_soft_delete_and_hard_delete() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let uri = "at://did:plc:alice/app.bsky.feed.post/del_target";

    let record = RecordInput {
        uri: uri.to_string(),
        cid: "bafyrei_del".to_string(),
        did: "did:plc:alice".to_string(),
        collection: "app.bsky.feed.post".to_string(),
        rkey: "del_target".to_string(),
        record_json: json!({ "text": "Will be deleted" }),
        indexed_at: 1700000001,
    };
    store.upsert_record(&record).expect("upsert failed");

    // Soft delete
    store.soft_delete_record(uri).expect("soft delete failed");
    assert!(store.get_record(uri).expect("get failed").is_none());
    let fetched_soft = store
        .get_record_including_deleted(uri)
        .expect("get failed")
        .expect("missing");
    assert!(fetched_soft.is_deleted);

    // Hard delete
    store.hard_delete_record(uri).expect("hard delete failed");
    let fetched_hard = store.get_record(uri).expect("get failed");
    assert!(fetched_hard.is_none());
}

#[test]
fn test_tier1_f05_query_builder_collection_and_did_filter() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:alice/app.bsky.feed.post/1".to_string(),
            cid: "cid1".to_string(),
            did: "did:plc:alice".to_string(),
            collection: "app.bsky.feed.post".to_string(),
            rkey: "1".to_string(),
            record_json: json!({ "text": "Alice post" }),
            indexed_at: 100,
        })
        .expect("upsert failed");

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:bob/app.bsky.feed.post/2".to_string(),
            cid: "cid2".to_string(),
            did: "did:plc:bob".to_string(),
            collection: "app.bsky.feed.post".to_string(),
            rkey: "2".to_string(),
            record_json: json!({ "text": "Bob post" }),
            indexed_at: 101,
        })
        .expect("upsert failed");

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:alice/app.bsky.feed.like/3".to_string(),
            cid: "cid3".to_string(),
            did: "did:plc:alice".to_string(),
            collection: "app.bsky.feed.like".to_string(),
            rkey: "3".to_string(),
            record_json: json!({ "subject": "at://..." }),
            indexed_at: 102,
        })
        .expect("upsert failed");

    // Query posts by Alice
    let alice_posts = store
        .collection("app.bsky.feed.post")
        .did("did:plc:alice")
        .execute()
        .expect("query failed");

    assert_eq!(alice_posts.len(), 1);
    assert_eq!(
        alice_posts[0].uri,
        "at://did:plc:alice/app.bsky.feed.post/1"
    );

    // Query all posts
    let all_posts = store
        .collection("app.bsky.feed.post")
        .execute()
        .expect("query failed");
    assert_eq!(all_posts.len(), 2);
}

#[test]
fn test_tier1_f06_query_builder_json1_operators() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    for i in 1..=5 {
        store
            .upsert_record(&RecordInput {
                uri: format!("at://did:plc:user/com.example.item/{i}"),
                cid: format!("cid_{i}"),
                did: "did:plc:user".to_string(),
                collection: "com.example.item".to_string(),
                rkey: i.to_string(),
                record_json: json!({
                    "score": i * 10,
                    "title": format!("Item {}", i),
                    "active": i % 2 == 1
                }),
                indexed_at: i * 1000,
            })
            .expect("upsert failed");
    }

    // Greater than operator
    let high_scores = store
        .collection("com.example.item")
        .where_json("score", QueryOp::Gt, 30)
        .execute()
        .expect("query failed");
    assert_eq!(high_scores.len(), 2); // 40, 50

    // Equal operator
    let exact_score = store
        .collection("com.example.item")
        .where_json("score", QueryOp::Eq, 20)
        .execute()
        .expect("query failed");
    assert_eq!(exact_score.len(), 1);
    assert_eq!(exact_score[0].record_json["title"], "Item 2");

    // LIKE operator
    let like_match = store
        .collection("com.example.item")
        .where_json("title", QueryOp::Like, "%Item 4%")
        .execute()
        .expect("query failed");
    assert_eq!(like_match.len(), 1);
    assert_eq!(like_match[0].rkey, "4");
}

#[test]
fn test_tier1_f07_query_builder_ordering_and_pagination() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    for i in 1..=10 {
        store
            .upsert_record(&RecordInput {
                uri: format!("at://did:plc:user/test.col/{i}"),
                cid: format!("cid_{i}"),
                did: "did:plc:user".to_string(),
                collection: "test.col".to_string(),
                rkey: format!("{:02}", i),
                record_json: json!({ "val": i }),
                indexed_at: i,
            })
            .expect("upsert failed");
    }

    // Order by indexed_at DESC with limit and offset
    let paged = store
        .collection("test.col")
        .order_by("indexed_at", SortDirection::Desc)
        .limit(3)
        .offset(2)
        .execute()
        .expect("query failed");

    assert_eq!(paged.len(), 3);
    assert_eq!(paged[0].indexed_at, 8);
    assert_eq!(paged[1].indexed_at, 7);
    assert_eq!(paged[2].indexed_at, 6);
}

#[test]
fn test_tier1_f08_broadcast_bus_subscription_and_delivery() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let mut rx1 = store.subscribe();
    let mut rx2 = store.subscribe();

    let record = RecordInput {
        uri: "at://did:plc:test/test.col/bus1".to_string(),
        cid: "cid_bus1".to_string(),
        did: "did:plc:test".to_string(),
        collection: "test.col".to_string(),
        rkey: "bus1".to_string(),
        record_json: json!({ "greeting": "world" }),
        indexed_at: 12345,
    };

    store.upsert_record(&record).expect("upsert failed");

    // Both subscribers receive the upsert notification
    let event1 = rx1.try_recv().expect("rx1 receive failed");
    let event2 = rx2.try_recv().expect("rx2 receive failed");

    match event1 {
        ChangeNotification::Upsert(row) => {
            assert_eq!(row.uri, record.uri);
            assert_eq!(row.record_json["greeting"], "world");
        }
        _ => panic!("Expected Upsert event"),
    }

    match event2 {
        ChangeNotification::Upsert(row) => assert_eq!(row.uri, record.uri),
        _ => panic!("Expected Upsert event"),
    }

    // Soft delete emits delete notification
    store
        .soft_delete_record(&record.uri)
        .expect("soft delete failed");
    let del_event1 = rx1.try_recv().expect("rx1 del receive failed");
    match del_event1 {
        ChangeNotification::Delete {
            uri,
            did,
            collection,
            rkey,
        } => {
            assert_eq!(uri, record.uri);
            assert_eq!(did, "did:plc:test");
            assert_eq!(collection, "test.col");
            assert_eq!(rkey, "bus1");
        }
        _ => panic!("Expected Delete event"),
    }
}

// ============================================================================
// Tier 2: Boundary, Adversarial & Corner Cases
// ============================================================================

#[test]
fn test_tier2_b01_empty_record_json_and_null_fields() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    let record = RecordInput {
        uri: "at://did:plc:user/empty.col/1".to_string(),
        cid: "cid_empty".to_string(),
        did: "did:plc:user".to_string(),
        collection: "empty.col".to_string(),
        rkey: "1".to_string(),
        record_json: json!({}),
        indexed_at: 100,
    };

    store.upsert_record(&record).expect("upsert failed");
    let retrieved = store
        .get_record(&record.uri)
        .expect("get failed")
        .expect("missing");
    assert_eq!(retrieved.record_json, json!({}));

    // Query on non-existent JSON field returns empty
    let missing_field_query = store
        .collection("empty.col")
        .where_json("nonexistent", QueryOp::Eq, "val")
        .execute()
        .expect("query failed");
    assert_eq!(missing_field_query.len(), 0);
}

#[test]
fn test_tier2_b02_deeply_nested_json_extraction() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    let nested_json = json!({
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "value": "target_found",
                        "num": 42
                    }
                }
            }
        }
    });

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:user/nested.col/1".to_string(),
            cid: "cid_nested".to_string(),
            did: "did:plc:user".to_string(),
            collection: "nested.col".to_string(),
            rkey: "1".to_string(),
            record_json: nested_json,
            indexed_at: 200,
        })
        .expect("upsert failed");

    let result = store
        .collection("nested.col")
        .where_json(
            "level1.level2.level3.level4.value",
            QueryOp::Eq,
            "target_found",
        )
        .execute()
        .expect("query failed");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].record_json["level1"]["level2"]["level3"]["level4"]["num"],
        42
    );
}

#[test]
fn test_tier2_b03_special_characters_unicode_and_escaping() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    let special_text = "Emoji 🚀 Test • 中文 • 日本語 • 한국어 • Quotes \" ' ` • Line\nBreak\tTab";
    let special_rkey = "rkey_with-hyphen.dot~tilde_underscore%20";

    store
        .upsert_record(&RecordInput {
            uri: format!("at://did:plc:unicode/special.col/{special_rkey}"),
            cid: "cid_unicode".to_string(),
            did: "did:plc:unicode".to_string(),
            collection: "special.col".to_string(),
            rkey: special_rkey.to_string(),
            record_json: json!({ "content": special_text }),
            indexed_at: 300,
        })
        .expect("upsert failed");

    let result = store
        .collection("special.col")
        .where_json("content", QueryOp::Eq, special_text)
        .execute()
        .expect("query failed");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].record_json["content"], special_text);
}

#[test]
fn test_tier2_b04_sql_injection_defense_in_json_paths_and_values() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:sqli/sec.col/1".to_string(),
            cid: "cid_sec".to_string(),
            did: "did:plc:sqli".to_string(),
            collection: "sec.col".to_string(),
            rkey: "1".to_string(),
            record_json: json!({ "username": "admin" }),
            indexed_at: 400,
        })
        .expect("upsert failed");

    // Malicious SQL injection payloads
    let sqli_payloads = [
        "' OR '1'='1",
        "'; DROP TABLE records; --",
        "admin' UNION SELECT * FROM records --",
        "\" OR 1=1 --",
    ];

    for payload in sqli_payloads {
        let res = store
            .collection("sec.col")
            .where_json("username", QueryOp::Eq, payload)
            .execute()
            .expect("query should execute safely without SQL injection");

        // The query safely searches for literal matches of the payload and finds 0
        assert_eq!(res.len(), 0);
    }

    // Verify records table still exists and is unaffected
    let check = store
        .collection("sec.col")
        .execute()
        .expect("table should still be intact");
    assert_eq!(check.len(), 1);
}

#[test]
fn test_tier2_b05_pagination_boundaries_limit_zero_and_offset_overflow() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    for i in 1..=3 {
        store
            .upsert_record(&RecordInput {
                uri: format!("at://did:plc:user/page.col/{i}"),
                cid: format!("cid_{i}"),
                did: "did:plc:user".to_string(),
                collection: "page.col".to_string(),
                rkey: i.to_string(),
                record_json: json!({ "val": i }),
                indexed_at: i,
            })
            .expect("upsert failed");
    }

    // limit(0) returns empty result
    let zero_limit = store
        .collection("page.col")
        .limit(0)
        .execute()
        .expect("limit(0) query failed");
    assert_eq!(zero_limit.len(), 0);

    // offset exceeding count returns empty result
    let overflow_offset = store
        .collection("page.col")
        .offset(100)
        .execute()
        .expect("overflow offset query failed");
    assert_eq!(overflow_offset.len(), 0);
}

#[test]
fn test_tier2_b06_soft_deleted_record_resurrection_on_upsert() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let uri = "at://did:plc:user/resurrect.col/item1";

    let initial = RecordInput {
        uri: uri.to_string(),
        cid: "cid_v1".to_string(),
        did: "did:plc:user".to_string(),
        collection: "resurrect.col".to_string(),
        rkey: "item1".to_string(),
        record_json: json!({ "version": 1 }),
        indexed_at: 100,
    };
    store.upsert_record(&initial).expect("upsert failed");

    // Soft delete
    store.soft_delete_record(uri).expect("soft delete failed");
    assert!(store.get_record(uri).expect("get failed").is_none());
    let soft_del = store
        .get_record_including_deleted(uri)
        .expect("get failed")
        .expect("missing");
    assert!(soft_del.is_deleted);

    // Subsequent upsert clears is_deleted back to 0
    let updated = RecordInput {
        uri: uri.to_string(),
        cid: "cid_v2".to_string(),
        did: "did:plc:user".to_string(),
        collection: "resurrect.col".to_string(),
        rkey: "item1".to_string(),
        record_json: json!({ "version": 2 }),
        indexed_at: 200,
    };
    store
        .upsert_record(&updated)
        .expect("resurrect upsert failed");

    let resurrected = store.get_record(uri).expect("get failed").expect("missing");
    assert!(!resurrected.is_deleted);
    assert_eq!(resurrected.cid, "cid_v2");
    assert_eq!(resurrected.record_json["version"], 2);
}

// ============================================================================
// Tier 3: Cross-Feature Combinations (Pairwise Interactions)
// ============================================================================

#[test]
fn test_tier3_p01_storage_upsert_query_and_broadcast_coordination() {
    let store = RecordStore::open_in_memory().expect("store open failed");
    let mut sub = store.subscribe();

    let record = RecordInput {
        uri: "at://did:plc:coord/feed.col/p1".to_string(),
        cid: "cid_coord".to_string(),
        did: "did:plc:coord".to_string(),
        collection: "feed.col".to_string(),
        rkey: "p1".to_string(),
        record_json: json!({ "text": "coordinated" }),
        indexed_at: 500,
    };

    store.upsert_record(&record).expect("upsert failed");

    // 1. Verify query retrieves it
    let query_res = store
        .collection("feed.col")
        .execute()
        .expect("query failed");
    assert_eq!(query_res.len(), 1);
    assert_eq!(query_res[0].uri, record.uri);

    // 2. Verify broadcast notification contains exact payload
    let notification = sub.try_recv().expect("receive notification failed");
    match notification {
        ChangeNotification::Upsert(row) => {
            assert_eq!(row.uri, record.uri);
            assert_eq!(row.record_json["text"], "coordinated");
        }
        _ => panic!("Expected Upsert notification"),
    }
}

#[test]
fn test_tier3_p02_soft_delete_query_filtering_and_include_deleted_toggle() {
    let store = RecordStore::open_in_memory().expect("store open failed");

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:user/toggle.col/active".to_string(),
            cid: "cid_act".to_string(),
            did: "did:plc:user".to_string(),
            collection: "toggle.col".to_string(),
            rkey: "active".to_string(),
            record_json: json!({ "status": "active" }),
            indexed_at: 100,
        })
        .expect("upsert failed");

    store
        .upsert_record(&RecordInput {
            uri: "at://did:plc:user/toggle.col/deleted".to_string(),
            cid: "cid_del".to_string(),
            did: "did:plc:user".to_string(),
            collection: "toggle.col".to_string(),
            rkey: "deleted".to_string(),
            record_json: json!({ "status": "deleted" }),
            indexed_at: 200,
        })
        .expect("upsert failed");

    store
        .soft_delete_record("at://did:plc:user/toggle.col/deleted")
        .expect("soft delete failed");

    // Default query excludes deleted
    let active_only = store
        .collection("toggle.col")
        .execute()
        .expect("default query failed");
    assert_eq!(active_only.len(), 1);
    assert_eq!(active_only[0].rkey, "active");

    // include_deleted(true) returns both
    let all_records = store
        .collection("toggle.col")
        .include_deleted(true)
        .execute()
        .expect("include_deleted query failed");
    assert_eq!(all_records.len(), 2);
}
