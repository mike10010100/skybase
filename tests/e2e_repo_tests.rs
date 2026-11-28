//! End-to-End Tests: Sovereign PDS Client, DPoP Signing, and Nonce Negotiation.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use common::MockPdsServer;
use serde_json::json;
use skyauth::SsrfFilter;
use skybase::repo::PdsRepoClient;

fn create_client(
    endpoint: impl Into<String>,
    did: impl Into<String>,
    token: impl Into<String>,
) -> PdsRepoClient {
    PdsRepoClient::from_credentials(endpoint, did, token)
        .expect("client creation failed")
        .with_ssrf_filter(SsrfFilter::new(true))
}

// ============================================================================
// Tier 1: Feature Coverage (PDS Client, createRecord, deleteRecord, DPoP)
// ============================================================================

#[tokio::test]
async fn test_tier1_f01_pds_client_create_record_with_explicit_rkey() {
    let mock_pds = MockPdsServer::start().await;
    let did = "did:plc:alice123";
    let token = "test_dpop_access_token_xyz";

    let client = create_client(mock_pds.uri(), did, token);

    let post_record = json!({
        "$type": "app.bsky.feed.post",
        "text": "Hello decentralized world!",
        "createdAt": "2026-09-12T00:00:00Z"
    });

    let res = client
        .create_record("app.bsky.feed.post", Some("post_123"), &post_record, true)
        .await
        .expect("create_record failed");

    assert_eq!(res.uri, "at://did:plc:alice123/app.bsky.feed.post/post_123");
    assert!(!res.cid.is_empty());
}

#[tokio::test]
async fn test_tier1_f02_pds_client_create_record_with_generated_rkey() {
    let mock_pds = MockPdsServer::start().await;
    let did = "did:plc:bob456";
    let token = "test_token_bob";

    let client = create_client(mock_pds.uri(), did, token);

    let like_record = json!({
        "$type": "app.bsky.feed.like",
        "subject": {
            "uri": "at://did:plc:alice123/app.bsky.feed.post/post_123",
            "cid": "bafyreicid1"
        },
        "createdAt": "2026-09-12T00:01:00Z"
    });

    let res = client
        .create_record("app.bsky.feed.like", None, &like_record, false)
        .await
        .expect("create_record failed");

    assert!(res
        .uri
        .starts_with("at://did:plc:bob456/app.bsky.feed.like/"));
    assert!(!res.cid.is_empty());
}

#[tokio::test]
async fn test_tier1_f03_pds_client_delete_record() {
    let mock_pds = MockPdsServer::start().await;
    let did = "did:plc:charlie789";
    let token = "test_token_charlie";

    let client = create_client(mock_pds.uri(), did, token);

    let del_result = client
        .delete_record("app.bsky.feed.post", "post_to_delete")
        .await;

    assert!(del_result.is_ok(), "delete_record should succeed");
}

#[tokio::test]
async fn test_tier1_f04_dpop_proof_generation_and_headers() {
    let mock_pds = MockPdsServer::start().await;
    let client = create_client(mock_pds.uri(), "did:plc:tester", "token_val");

    // createRecord sends DPoP proof and Authorization: DPoP token headers
    // MockPdsServer requires both headers; if missing, wiremock returns 404
    let res = client
        .create_record(
            "com.example.item",
            Some("rk1"),
            &json!({ "title": "test" }),
            true,
        )
        .await;

    assert!(
        res.is_ok(),
        "DPoP headers must be accepted by MockPdsServer"
    );
}

#[tokio::test]
async fn test_tier1_f05_custom_nsid_collections_and_schemas() {
    let mock_pds = MockPdsServer::start().await;
    let client = create_client(mock_pds.uri(), "did:plc:custom", "token_custom");

    let custom_record = json!({
        "venue": "Coffee Shop",
        "rating": 5,
        "tags": ["coffee", "wifi"]
    });

    let res = client
        .create_record(
            "com.specialapp.venue.review",
            Some("review_001"),
            &custom_record,
            true,
        )
        .await
        .expect("custom collection record creation failed");

    assert_eq!(
        res.uri,
        "at://did:plc:custom/com.specialapp.venue.review/review_001"
    );
}

// ============================================================================
// Tier 2: Boundary, Adversarial & Challenge Handling
// ============================================================================

#[tokio::test]
async fn test_tier2_b01_dpop_nonce_challenge_automatic_retry() {
    let mock_pds = MockPdsServer::start().await;
    let client = create_client(mock_pds.uri(), "did:plc:nonce_user", "nonce_token");

    // Mount one-time 401 use_dpop_nonce challenge
    let challenge_nonce = "dpop_nonce_challenge_1234567890";
    mock_pds.mount_nonce_challenge_once(challenge_nonce).await;

    // Client must automatically capture DPoP-Nonce header, regenerate proof, and succeed
    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("post_with_nonce"),
            &json!({ "text": "Testing DPoP nonce retry" }),
            true,
        )
        .await
        .expect("create_record should succeed on automatic nonce retry");

    assert_eq!(
        res.uri,
        "at://did:plc:nonce_user/app.bsky.feed.post/post_with_nonce"
    );
}

#[tokio::test]
async fn test_tier2_b02_complex_payload_serialization() {
    let mock_pds = MockPdsServer::start().await;
    let client = create_client(mock_pds.uri(), "did:plc:complex", "token_complex");

    let complex_payload = json!({
        "unicode": "🎉 Unicode • 🚀 Rocket • 汉语 / 漢語 • 日本語 • العربية",
        "quotes": "Double \" and Single ' and Backtick `",
        "numbers": [-999999999999i64, 0, std::f64::consts::PI, 1e10],
        "nested": {
            "array": [true, false, null, { "inner": 42 }]
        }
    });

    let res = client
        .create_record(
            "app.complex.record",
            Some("rk_complex"),
            &complex_payload,
            true,
        )
        .await
        .expect("complex payload creation failed");

    assert_eq!(
        res.uri,
        "at://did:plc:complex/app.complex.record/rk_complex"
    );
}

#[tokio::test]
async fn test_tier2_b03_unreachable_endpoint_network_error() {
    // Port 1 is reserved / unreachable on loopback
    let dead_endpoint = "http://127.0.0.1:1";
    let client = create_client(dead_endpoint, "did:plc:fail", "token");

    let res = client
        .create_record("app.bsky.feed.post", Some("1"), &json!({}), false)
        .await;

    assert!(
        res.is_err(),
        "Request to unreachable endpoint should return error"
    );
    assert!(matches!(
        res.unwrap_err(),
        skybase::SkybaseError::Network(_)
    ));
}

// ============================================================================
// Tier 3: Cross-Feature Combinations (PDS Client Write + URI Schema Verification)
// ============================================================================

#[tokio::test]
async fn test_tier3_p01_pds_write_lifecycle_create_and_delete() {
    let mock_pds = MockPdsServer::start().await;
    let did = "did:plc:full_cycle";
    let token = "token_cycle";

    let client = create_client(mock_pds.uri(), did, token);

    // 1. Create record
    let create_res = client
        .create_record(
            "app.bsky.feed.post",
            Some("cycle_p1"),
            &json!({ "text": "full cycle" }),
            true,
        )
        .await
        .expect("create failed");

    assert_eq!(
        create_res.uri,
        "at://did:plc:full_cycle/app.bsky.feed.post/cycle_p1"
    );

    // 2. Delete record
    let delete_res = client.delete_record("app.bsky.feed.post", "cycle_p1").await;
    assert!(delete_res.is_ok(), "delete should succeed");
}

#[tokio::test]
async fn test_tier1_f06_pds_client_put_record_with_swap_and_get_record() {
    use wiremock::matchers::{header_exists, method, path, query_param};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    let server = MockServer::start().await;
    let did = "did:plc:swap_user";
    let collection = "social.skybouncer.config";
    let rkey = "self";

    // Initial put_record (no swapRecord)
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.putRecord"))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(|req: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            match body.get("swapRecord").and_then(|v| v.as_str()) {
                None => ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:swap_user/social.skybouncer.config/self",
                    "cid": "bafyrei_v1"
                })),
                Some("bafyrei_v1") => ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:swap_user/social.skybouncer.config/self",
                    "cid": "bafyrei_v2"
                })),
                Some(_) => ResponseTemplate::new(400).set_body_json(json!({
                    "error": "InvalidSwap",
                    "message": "Record swap CID mismatch"
                })),
            }
        })
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.getRecord"))
        .and(query_param("repo", did))
        .and(query_param("collection", collection))
        .and(query_param("rkey", rkey))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "uri": "at://did:plc:swap_user/social.skybouncer.config/self",
            "cid": "bafyrei_v2",
            "value": { "enabled": true, "version": 2 }
        })))
        .mount(&server)
        .await;

    let client = create_client(server.uri(), did, "token_swap");

    let initial = client
        .put_record(
            collection,
            rkey,
            &json!({ "enabled": true, "version": 1 }),
            true,
        )
        .await
        .expect("initial put_record should succeed");
    assert_eq!(initial.cid, "bafyrei_v1");

    let updated = client
        .put_record_with_swap(
            collection,
            rkey,
            &json!({ "enabled": true, "version": 2 }),
            true,
            Some(&initial.cid),
        )
        .await
        .expect("put_record_with_swap matching CID should succeed");
    assert_eq!(updated.cid, "bafyrei_v2");

    let conflict = client
        .put_record_with_swap(
            collection,
            rkey,
            &json!({ "enabled": false, "version": 3 }),
            true,
            Some("bafyrei_stale"),
        )
        .await;
    assert!(conflict.is_err());
    let err_msg = conflict.unwrap_err().to_string();
    assert!(
        err_msg.contains("InvalidSwap"),
        "Expected InvalidSwap error, got: {err_msg}"
    );

    let fetched = client
        .get_record(did, collection, rkey)
        .await
        .expect("get_record should succeed");
    assert_eq!(fetched.cid, "bafyrei_v2");
    assert_eq!(fetched.value["version"], 2);
}

#[tokio::test]
async fn test_tier1_f07_pds_client_list_records_cursor_pagination() {
    use serde::Deserialize;
    use wiremock::matchers::{header_exists, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct ItemRecord {
        title: String,
    }

    let server = MockServer::start().await;
    let did = "did:plc:list_user";
    let collection = "com.example.item";

    // Page 2 (with cursor)
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listRecords"))
        .and(query_param("repo", did))
        .and(query_param("collection", collection))
        .and(query_param("limit", "2"))
        .and(query_param("cursor", "page_2_cursor"))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "records": [{
                "uri": "at://did:plc:list_user/com.example.item/r3",
                "cid": "cid_3",
                "value": { "title": "Item 3" }
            }]
        })))
        .mount(&server)
        .await;

    // Page 1 (no cursor)
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listRecords"))
        .and(query_param("repo", did))
        .and(query_param("collection", collection))
        .and(query_param("limit", "2"))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "records": [
                {
                    "uri": "at://did:plc:list_user/com.example.item/r1",
                    "cid": "cid_1",
                    "value": { "title": "Item 1" }
                },
                {
                    "uri": "at://did:plc:list_user/com.example.item/r2",
                    "cid": "cid_2",
                    "value": { "title": "Item 2" }
                }
            ],
            "cursor": "page_2_cursor"
        })))
        .mount(&server)
        .await;

    let client = create_client(server.uri(), did, "token_list");

    let page1 = client
        .list_records::<ItemRecord>(did, collection, 2, None)
        .await
        .expect("page 1 should succeed");
    assert_eq!(page1.records.len(), 2);
    assert_eq!(page1.records[0].value.title, "Item 1");
    assert_eq!(page1.records[1].value.title, "Item 2");
    assert_eq!(page1.cursor.as_deref(), Some("page_2_cursor"));

    let page2 = client
        .list_records::<ItemRecord>(did, collection, 2, page1.cursor.as_deref())
        .await
        .expect("page 2 should succeed");
    assert_eq!(page2.records.len(), 1);
    assert_eq!(page2.records[0].value.title, "Item 3");
    assert!(page2.cursor.is_none());
}
