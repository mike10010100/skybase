//! End-to-End Tests: Sovereign PDS Client, DPoP Signing, and Nonce Negotiation.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use common::MockPdsServer;
use serde_json::json;
use skybase::repo::PdsRepoClient;

fn create_client(
    endpoint: impl Into<String>,
    did: impl Into<String>,
    token: impl Into<String>,
) -> PdsRepoClient {
    PdsRepoClient::from_credentials(endpoint, did, token).expect("client creation failed")
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
