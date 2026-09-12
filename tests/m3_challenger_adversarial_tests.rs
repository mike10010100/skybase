//! Empirical Challenger Adversarial Test Suite for Milestone 3 (`skybase::repo`).
//!
//! Thoroughly tests:
//! 1. Adversarial Nonce Scenarios (double/infinite challenges, missing headers, mixed case, whitespace padding).
//! 2. Error Body Fuzzing & Memory DoS (10 MB giant error payload bounded reading, truncated JSON, HTML/binary/null bodies).
//! 3. Pre-flight Validation Fuzzing (invalid rkeys, path traversal, length boundaries, zero network calls).
//! 4. Operational Resilience (delete_record challenges, missing response fields, expired session fast-fail, concurrency).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    missing_docs
)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::WWW_AUTHENTICATE;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use skyauth::session::OAuthSession;
use skybase::error::SkybaseError;
use skybase::repo::{format_at_uri, generate_tid, validate_rkey, PdsRepoClient, TidGenerator};

// ============================================================================
// Category 1: Adversarial Nonce Scenarios
// ============================================================================

#[tokio::test]
async fn test_adv_nonce_double_challenge_infinite_loop_prevention() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // Mock server ALWAYS issues 401 use_dpop_nonce with fresh nonces
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(401)
                .insert_header("DPoP-Nonce", format!("server_nonce_attempt_{attempt}"))
                .set_body_json(json!({
                    "error": "use_dpop_nonce",
                    "message": "DPoP proof requires nonce"
                }))
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "infinite loop attack" }),
            true,
        )
        .await;

    assert!(res.is_err(), "Must terminate and fail closed");
    let err = res.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("retry limit exceeded"),
        "Error message must indicate retry limit exceeded: {err_msg}"
    );

    // Exactly 2 attempts (initial + 1 retry) must have occurred
    assert_eq!(
        request_count.load(Ordering::SeqCst),
        2,
        "Client must stop after exactly 2 attempts without infinite looping"
    );
}

#[tokio::test]
async fn test_adv_nonce_www_authenticate_double_challenge() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // Challenge via RFC 9449 WWW-Authenticate header on both attempts
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(401)
                .insert_header(
                    WWW_AUTHENTICATE,
                    "DPoP error=\"use_dpop_nonce\", error_description=\"Nonce required\"",
                )
                .insert_header("DPoP-Nonce", format!("header_nonce_{attempt}"))
                .set_body_string("Unauthorized")
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "www-authenticate double challenge" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("retry limit exceeded"),
        "Expected retry limit exceeded: {err_msg}"
    );
    assert_eq!(request_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_adv_nonce_400_bad_request_nonce_challenge_success() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // Attempt 1: 400 Bad Request with use_dpop_nonce
    // Attempt 2: 200 OK
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                ResponseTemplate::new(400)
                    .insert_header("DPoP-Nonce", "nonce_from_400")
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "Bad request due to missing/stale DPoP nonce"
                    }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:alice/app.bsky.feed.post/p1",
                    "cid": "bafyrei_400_recovered"
                }))
            }
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "400 recovery" }),
            true,
        )
        .await
        .expect("should transparently recover from 400 use_dpop_nonce");

    assert_eq!(res.uri, "at://did:plc:alice/app.bsky.feed.post/p1");
    assert_eq!(res.cid, "bafyrei_400_recovered");
    assert_eq!(request_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_adv_nonce_400_bad_request_double_challenge_fails() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // Server returns 400 use_dpop_nonce on both calls
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(400)
                .insert_header("DPoP-Nonce", format!("nonce_400_{attempt}"))
                .set_body_json(json!({
                    "error": "use_dpop_nonce",
                    "message": "Persistent 400 nonce challenge"
                }))
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "400 double challenge" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("retry limit exceeded"),
        "Expected retry limit exceeded: {err_msg}"
    );
    assert_eq!(request_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_adv_nonce_missing_header_fails_closed_zero_retry() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // Server claims use_dpop_nonce but omits DPoP-Nonce header
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(401).set_body_json(json!({
                "error": "use_dpop_nonce",
                "message": "Requires nonce but server forgot header"
            }))
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "missing header test" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("omitted DPoP-Nonce header"),
        "Expected missing header error, got: {err_msg}"
    );
    // Crucial: Client must fail immediately without making a useless second attempt
    assert_eq!(
        request_count.load(Ordering::SeqCst),
        1,
        "Must fail closed on attempt 1 when nonce header is missing"
    );
}

#[tokio::test]
async fn test_adv_nonce_empty_or_whitespace_header_fails_closed() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // Server sends DPoP-Nonce with whitespace only (spaces and tabs)
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(401)
                .insert_header("DPoP-Nonce", "   \t   ")
                .set_body_json(json!({
                    "error": "use_dpop_nonce",
                    "message": "Empty whitespace nonce"
                }))
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "empty nonce header" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("omitted DPoP-Nonce header"),
        "Whitespace-only nonce header must be treated as omitted: {err_msg}"
    );
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_adv_nonce_mixed_case_headers() {
    let casings = vec![
        ("dpop-nonce", "lowercase_nonce_val"),
        ("DPOP-NONCE", "uppercase_nonce_val"),
        ("dPoP-nOnCe", "mixed_case_nonce_val"),
    ];

    for (header_name, nonce_val) in casings {
        let server = MockServer::start().await;
        let request_count = Arc::new(AtomicU32::new(0));
        let counter_clone = request_count.clone();
        let nonce_val_str = nonce_val.to_string();

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(move |_req: &wiremock::Request| {
                let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    ResponseTemplate::new(401)
                        .insert_header(header_name, nonce_val_str.clone())
                        .set_body_json(json!({
                            "error": "use_dpop_nonce",
                            "message": "Nonce required"
                        }))
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "uri": "at://did:plc:alice/app.bsky.feed.post/casing_post",
                        "cid": "bafyrei_casing_success"
                    }))
                }
            })
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("casing_post"),
                &json!({ "text": "casing test" }),
                true,
            )
            .await
            .unwrap_or_else(|e| panic!("Failed for header {header_name}: {e}"));

        assert_eq!(res.cid, "bafyrei_casing_success");
        assert_eq!(request_count.load(Ordering::SeqCst), 2);

        // Verify nonce was stored in cache
        let origin = url::Url::parse(&server.uri())
            .unwrap()
            .origin()
            .ascii_serialization();
        assert_eq!(
            client.nonce_cache().get_nonce(&origin).as_deref(),
            Some(nonce_val)
        );
    }
}

#[tokio::test]
async fn test_adv_nonce_whitespace_padding_trimmed() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", "   \t clean_nonce_value \t  ")
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "Padded nonce"
                    }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:alice/app.bsky.feed.post/trimmed_post",
                    "cid": "bafyrei_trimmed"
                }))
            }
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("trimmed_post"),
            &json!({ "text": "trim test" }),
            true,
        )
        .await
        .expect("should succeed with trimmed nonce");

    assert_eq!(res.cid, "bafyrei_trimmed");

    let origin = url::Url::parse(&server.uri())
        .unwrap()
        .origin()
        .ascii_serialization();
    assert_eq!(
        client.nonce_cache().get_nonce(&origin).as_deref(),
        Some("clean_nonce_value"),
        "Cached nonce must have leading and trailing whitespace stripped"
    );
}

#[tokio::test]
async fn test_adv_nonce_delete_record_lifecycle() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    // 1. Single retry success on delete_record
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.deleteRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", "delete_nonce_1")
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "Nonce required for delete"
                    }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({}))
            }
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let del_res = client
        .delete_record("app.bsky.feed.post", "post_to_delete")
        .await;
    assert!(
        del_res.is_ok(),
        "delete_record should succeed after nonce retry"
    );
    assert_eq!(request_count.load(Ordering::SeqCst), 2);

    // 2. Double challenge on delete_record fails closed
    let server2 = MockServer::start().await;
    let request_count2 = Arc::new(AtomicU32::new(0));
    let counter_clone2 = request_count2.clone();

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.deleteRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = counter_clone2.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(401)
                .insert_header("DPoP-Nonce", format!("double_nonce_{attempt}"))
                .set_body_json(json!({
                    "error": "use_dpop_nonce",
                    "message": "Infinite delete challenge"
                }))
        })
        .mount(&server2)
        .await;

    let client2 = PdsRepoClient::from_credentials(server2.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let del_res2 = client2
        .delete_record("app.bsky.feed.post", "post_to_delete")
        .await;
    assert!(
        del_res2.is_err(),
        "delete_record must fail on double challenge"
    );
    assert!(del_res2
        .unwrap_err()
        .to_string()
        .contains("retry limit exceeded"));
    assert_eq!(request_count2.load(Ordering::SeqCst), 2);
}

// ============================================================================
// Category 2: Error Body Fuzzing, Bounded Memory DoS Protection & Response Validation
// ============================================================================

#[tokio::test]
async fn test_adv_error_body_giant_10mb_payload_bounded_reader() {
    let server = MockServer::start().await;

    // Construct a massive 10 MB error payload
    let giant_bytes = vec![b'E'; 10 * 1024 * 1024];

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(500).set_body_bytes(giant_bytes))
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let start = std::time::Instant::now();
    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "giant error payload" }),
            true,
        )
        .await;
    let elapsed = start.elapsed();

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();

    // Verify bounded read: MAX_ERROR_BODY_BYTES is 64 KB (65,536 bytes)
    // The formatted error message will be around 65,536 bytes plus status text prefix
    assert!(
        err_msg.len() <= 70_000,
        "Error message must be strictly bounded to prevent memory exhaustion, got {} bytes",
        err_msg.len()
    );
    assert!(
        err_msg.contains("500 Internal Server Error"),
        "Must preserve status code"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "Bounded read must terminate swiftly without buffering 10 MB, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_adv_error_body_truncated_json() {
    let server = MockServer::start().await;

    // Broken JSON that cuts off mid-string
    let broken_json = b"{\"error\": \"InvalidRequest\", \"message\": \"Schema validation cut off";

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(400).set_body_bytes(broken_json.to_vec()))
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "truncated json" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("400 Bad Request"),
        "Must format HTTP status: {err_msg}"
    );
    assert!(
        err_msg.contains("Schema validation cut off"),
        "Must fallback to plain text snippet: {err_msg}"
    );
}

#[tokio::test]
async fn test_adv_error_body_html_error_page() {
    let server = MockServer::start().await;

    let html_body = "<!DOCTYPE html><html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center><hr><center>cloudflare</center></body></html>";

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(
            ResponseTemplate::new(502)
                .insert_header("content-type", "text/html")
                .set_body_string(html_body),
        )
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "html error" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("502 Bad Gateway"),
        "Must format 502 status cleanly: {err_msg}"
    );
}

#[tokio::test]
async fn test_adv_error_body_binary_null_bytes() {
    let server = MockServer::start().await;

    let binary_body = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x00, 0xAA, 0x55];

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(
            ResponseTemplate::new(400)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(binary_body),
        )
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "binary null bytes" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("400 Bad Request"),
        "Must format 400 status cleanly without panic on non-UTF8: {err_msg}"
    );
}

#[tokio::test]
async fn test_adv_error_body_empty() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(500).set_body_bytes(Vec::new()))
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "empty error body" }),
            true,
        )
        .await;

    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("HTTP request failed with status: 500 Internal Server Error"),
        "Error message must contain status text: {err_msg}"
    );
}

#[tokio::test]
async fn test_adv_error_body_json_type_confusion() {
    let variations = vec![
        json!([1, 2, 3]),
        json!(9999),
        json!({ "error": 12345, "message": true }),
        json!({ "error": null, "message": null }),
        json!("string scalar"),
    ];

    for val in variations {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(400).set_body_json(val))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("p1"),
                &json!({ "text": "type confusion" }),
                true,
            )
            .await;

        assert!(
            res.is_err(),
            "Must gracefully handle non-standard JSON error"
        );
    }
}

#[tokio::test]
async fn test_adv_success_body_giant_response_bounded() {
    let server = MockServer::start().await;

    // Return a 5 MB JSON payload for a 200 OK response
    let giant_field = "x".repeat(5 * 1024 * 1024);
    let giant_json = format!(
        "{{\"uri\": \"at://did:plc:alice/app.bsky.feed.post/p1\", \"cid\": \"cid123\", \"payload\": \"{giant_field}\"}}"
    );

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(200).set_body_string(giant_json))
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "giant success body" }),
            true,
        )
        .await;

    // MAX_SUCCESS_BODY_BYTES is 1 MB. A 5 MB payload gets truncated at 1 MB,
    // which results in broken JSON and fails closed with a serialization error
    // instead of accumulating unbounded memory.
    assert!(
        res.is_err(),
        "Giant 5MB response truncated at 1MB must fail closed with decode error"
    );
    assert!(matches!(res.unwrap_err(), SkybaseError::Serialization(_)));
}

#[tokio::test]
async fn test_adv_success_body_missing_fields_fails_safely() {
    let defective_bodies = vec![
        json!({}),                                                    // missing both uri and cid
        json!({ "uri": "at://did:plc:alice/app.bsky.feed.post/p1" }), // missing cid
        json!({ "cid": "bafyrei_test_cid" }),                         // missing uri
        json!({ "uri": 12345, "cid": "bafyrei_test_cid" }),           // non-string uri
        json!({ "uri": "at://did:plc:alice/app.bsky.feed.post/p1", "cid": null }), // null cid
    ];

    for body in defective_bodies {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("p1"),
                &json!({ "text": "missing fields" }),
                true,
            )
            .await;

        assert!(
            res.is_err(),
            "Must fail closed when required fields are missing"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("Missing 'uri'") || err_msg.contains("Missing 'cid'"),
            "Error must specify missing field: {err_msg}"
        );
    }
}

// ============================================================================
// Category 3: Pre-flight Validation Fuzzing (Invalid rkeys & Zero Network Calls)
// ============================================================================

#[tokio::test]
async fn test_adv_rkey_path_traversal_fuzzing() {
    let server = MockServer::start().await;
    // Mock server must NOT receive ANY request
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    Mock::given(method("POST"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let traversal_vectors = vec![
        ".",
        "..",
        "../",
        "../../",
        "../../../etc/passwd",
        "..\\",
        "..\\windows\\system32",
        "/root",
        "path/to/key",
        "dir\\key",
        "%2e%2e",
        "..%2f",
        ".%2e",
        "foo/../bar",
    ];

    for vector in traversal_vectors {
        // Test direct validator
        assert!(
            validate_rkey(vector).is_err(),
            "validate_rkey must reject traversal vector: '{vector}'"
        );

        // Test create_record pre-flight rejection
        let create_res = client
            .create_record(
                "app.bsky.feed.post",
                Some(vector),
                &json!({ "text": "payload" }),
                true,
            )
            .await;
        assert!(
            create_res.is_err(),
            "create_record must reject pre-flight: '{vector}'"
        );

        // Test delete_record pre-flight rejection
        let delete_res = client.delete_record("app.bsky.feed.post", vector).await;
        assert!(
            delete_res.is_err(),
            "delete_record must reject pre-flight: '{vector}'"
        );
    }

    // Zero requests reached the network!
    assert_eq!(
        request_count.load(Ordering::SeqCst),
        0,
        "Pre-flight rejection must prevent ANY network calls"
    );
}

#[tokio::test]
async fn test_adv_rkey_length_boundaries_fuzzing() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    Mock::given(method("POST"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({
                "uri": "at://did:plc:alice/app.bsky.feed.post/rk",
                "cid": "cid1"
            }))
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    // 1. Boundary: 0 characters (empty string) -> Rejected
    assert!(validate_rkey("").is_err());
    assert!(client
        .create_record("app.bsky.feed.post", Some(""), &json!({}), true)
        .await
        .is_err());
    assert!(client
        .delete_record("app.bsky.feed.post", "")
        .await
        .is_err());

    // 2. Boundary: 1 character ("a") -> Valid
    assert!(validate_rkey("a").is_ok());

    // 3. Boundary: Exactly 512 characters -> Valid
    let rkey_512 = "k".repeat(512);
    assert!(validate_rkey(&rkey_512).is_ok());

    // 4. Boundary: 513 characters -> Rejected
    let rkey_513 = "k".repeat(513);
    assert!(validate_rkey(&rkey_513).is_err());
    assert!(client
        .create_record("app.bsky.feed.post", Some(&rkey_513), &json!({}), true)
        .await
        .is_err());
    assert!(client
        .delete_record("app.bsky.feed.post", &rkey_513)
        .await
        .is_err());

    // 5. Boundary: 4,096 characters -> Rejected
    let rkey_4096 = "k".repeat(4096);
    assert!(validate_rkey(&rkey_4096).is_err());
    assert!(client
        .create_record("app.bsky.feed.post", Some(&rkey_4096), &json!({}), true)
        .await
        .is_err());
    assert!(client
        .delete_record("app.bsky.feed.post", &rkey_4096)
        .await
        .is_err());

    // All rejections happened pre-flight, zero requests made
    assert_eq!(request_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_adv_rkey_character_set_fuzzing() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    Mock::given(method("POST"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
        .expect("client creation failed");

    let invalid_chars = vec![
        "space in key",
        " leading_space",
        "trailing_space ",
        "tab\tin\tkey",
        "newline\nin\nkey",
        "carriage\rin\rkey",
        "null\0byte",
        "bell\x07char",
        "esc\x1bchar",
        "emoji_🚀_fire",
        "café_latte",
        "日本語のキー",
        "русский_текст",
        "مرحبا",
        "\u{feff}bom_prefix",
        "exclamation!",
        "at@sign",
        "hash#tag",
        "dollar$sign",
        "percent%20",
        "caret^power",
        "ampersand&and",
        "asterisk*star",
        "parens(left)",
        "plus+sign",
        "equals=sign",
        "curly{brace}",
        "square[bracket]",
        "pipe|line",
        "colon:semi;",
        "quotes\"single'",
        "angle<bracket>",
        "comma,period",
        "question?mark",
    ];

    for invalid in invalid_chars {
        assert!(
            validate_rkey(invalid).is_err(),
            "validate_rkey must reject: '{invalid}'"
        );
        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some(invalid),
                &json!({ "text": "bad" }),
                true,
            )
            .await;
        assert!(res.is_err(), "create_record must reject: '{invalid}'");
    }

    assert_eq!(
        request_count.load(Ordering::SeqCst),
        0,
        "Zero network requests for invalid character sets"
    );
}

#[test]
fn test_adv_rkey_allowed_charset_exhaustive() {
    // ATProto spec: 1-512 ASCII alphanumeric or `.` `_` `~` `-`, not `.` or `..`
    let valid_full = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._~-";
    assert!(validate_rkey(valid_full).is_ok());

    // Single allowed characters
    for b in b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_~-" {
        let byte_arr = [*b];
        let s = std::str::from_utf8(&byte_arr).unwrap();
        assert!(validate_rkey(s).is_ok(), "Char '{s}' must be valid");
    }

    // Single dot is illegal
    assert!(validate_rkey(".").is_err());
    // Double dot is illegal
    assert!(validate_rkey("..").is_err());
    // Three dots is legal per grammar
    assert!(validate_rkey("...").is_ok());
    // Dot inside word is legal
    assert!(validate_rkey("record.key").is_ok());
    assert!(validate_rkey(".hidden").is_ok());
    assert!(validate_rkey("hidden.").is_ok());
}

// ============================================================================
// Category 4: Operational Resilience, Session Expiry, URL Variations & High Concurrency
// ============================================================================

#[tokio::test]
async fn test_adv_expired_session_fails_fast_zero_network() {
    let server = MockServer::start().await;
    let request_count = Arc::new(AtomicU32::new(0));
    let counter_clone = request_count.clone();

    Mock::given(method("POST"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;

    // Create session with 0 seconds expiry
    let dpop_key = skyauth::dpop::DPoPKey::generate();
    let session = OAuthSession::new(
        "did:plc:expired_user",
        "expired_token",
        None,
        "DPoP",
        None,
        Some(0), // 0 seconds duration: instantly expired
        dpop_key,
        Some(server.uri()),
        None,
        None,
    )
    .expect("session creation failed");

    // Sleep 5ms to guarantee SystemTime::now() > exp
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(session.is_expired(), "Session must be marked expired");

    let client = PdsRepoClient::from_session(Arc::new(session));

    let res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "text": "expired test" }),
            true,
        )
        .await;

    assert!(res.is_err(), "Must fail on expired session");
    assert!(
        matches!(res.unwrap_err(), SkybaseError::Auth(_)),
        "Must return typed SkybaseError::Auth error"
    );

    // Zero requests reached the network!
    assert_eq!(
        request_count.load(Ordering::SeqCst),
        0,
        "Expired session must fail-fast before network dispatch"
    );
}

#[tokio::test]
async fn test_adv_endpoint_url_formatting() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "uri": "at://did:plc:alice/app.bsky.feed.post/p1",
            "cid": "cid_url_test"
        })))
        .mount(&server)
        .await;

    // 1. Endpoint with multiple trailing slashes
    let endpoint_with_slashes = format!("{server_uri}////", server_uri = server.uri());
    let client = PdsRepoClient::from_credentials(endpoint_with_slashes, "did:plc:alice", "token")
        .expect("client creation failed");

    let res = client
        .create_record("app.bsky.feed.post", Some("p1"), &json!({}), false)
        .await
        .expect("should normalize trailing slashes");
    assert_eq!(res.cid, "cid_url_test");

    // 2. Malformed endpoint URL fails cleanly with typed error
    let bad_client =
        PdsRepoClient::from_credentials("invalid://[::1:bad_port", "did:plc:alice", "token")
            .expect("session can hold arbitrary string");
    let bad_res = bad_client
        .create_record("app.bsky.feed.post", Some("p1"), &json!({}), false)
        .await;
    assert!(bad_res.is_err());
    assert!(matches!(bad_res.unwrap_err(), SkybaseError::Repo(_)));
}

#[tokio::test]
async fn test_adv_create_record_omits_rkey_when_none() {
    let server = MockServer::start().await;

    // WireMock matcher verifying that request body does NOT contain "rkey" key
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(|req: &wiremock::Request| {
            let json_body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            assert!(
                json_body.get("rkey").is_none(),
                "Serialized JSON must omit 'rkey' when None"
            );
            assert_eq!(json_body["repo"], "did:plc:server_assigned");
            assert_eq!(json_body["collection"], "app.bsky.feed.post");

            ResponseTemplate::new(200).set_body_json(json!({
                "uri": "at://did:plc:server_assigned/app.bsky.feed.post/server_tid_123",
                "cid": "cid_server_assigned"
            }))
        })
        .mount(&server)
        .await;

    let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:server_assigned", "token")
        .expect("client creation failed");

    let res = client
        .create_record(
            "app.bsky.feed.post",
            None, // Server-assigned TID
            &json!({ "text": "no rkey" }),
            true,
        )
        .await
        .expect("create_record without rkey should succeed");

    assert_eq!(
        res.uri,
        "at://did:plc:server_assigned/app.bsky.feed.post/server_tid_123"
    );
}

#[tokio::test]
async fn test_adv_concurrent_clients_with_shared_nonce_cache() {
    let server = MockServer::start().await;
    let challenge_issued = Arc::new(AtomicU32::new(0));
    let challenge_clone = challenge_issued.clone();

    // Challenge the very first request with 401 use_dpop_nonce
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &wiremock::Request| {
            let count = challenge_clone.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", "shared_concurrent_nonce_999")
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "Initial challenge"
                    }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:alice/app.bsky.feed.post/concurrent",
                    "cid": "bafyrei_concurrent_ok"
                }))
            }
        })
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_123")
            .expect("client creation failed"),
    );

    let concurrency = 25;
    let mut tasks = Vec::with_capacity(concurrency);

    for i in 0..concurrency {
        let client_clone = client.clone();
        tasks.push(tokio::spawn(async move {
            let rkey = format!("c_post_{i}");
            client_clone
                .create_record(
                    "app.bsky.feed.post",
                    Some(&rkey),
                    &json!({ "index": i }),
                    true,
                )
                .await
        }));
    }

    for task in tasks {
        let res = task.await.expect("task join failed");
        assert!(
            res.is_ok(),
            "Every concurrent request must succeed: {res:?}"
        );
    }

    // Verify origin nonce was recorded in shared cache
    let origin = url::Url::parse(&server.uri())
        .unwrap()
        .origin()
        .ascii_serialization();
    assert_eq!(
        client.nonce_cache().get_nonce(&origin).as_deref(),
        Some("shared_concurrent_nonce_999")
    );
}

#[tokio::test]
async fn test_adv_tid_generator_high_concurrency_stress() {
    let generator = Arc::new(TidGenerator::new());
    let thread_count = 16;
    let iterations_per_thread = 2_000;
    let total_expected = thread_count * iterations_per_thread;

    let mut handles = Vec::with_capacity(thread_count);

    for _ in 0..thread_count {
        let gen = generator.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            let mut list = Vec::with_capacity(iterations_per_thread);
            for _ in 0..iterations_per_thread {
                list.push(gen.next_tid());
            }
            list
        }));
    }

    let mut all_tids = HashSet::with_capacity(total_expected);

    for handle in handles {
        let thread_tids = handle.await.expect("thread join failed");

        // Verify per-thread strict monotonicity
        for window in thread_tids.windows(2) {
            let prev = &window[0];
            let next = &window[1];
            assert!(
                prev < next,
                "TIDs must be strictly monotonic: prev={prev}, next={next}"
            );
        }

        // Verify 13 characters and base32 sortable charset
        for tid in &thread_tids {
            assert_eq!(tid.len(), 13, "TID must be exactly 13 characters: {tid}");
            for ch in tid.chars() {
                assert!(
                    matches!(ch, '2'..='7' | 'a'..='z'),
                    "TID character must be in base32 sortable alphabet: '{ch}' in '{tid}'"
                );
            }
            all_tids.insert(tid.clone());
        }
    }

    // Zero collisions across all threads!
    assert_eq!(
        all_tids.len(),
        total_expected,
        "All generated TIDs must be globally unique across all threads"
    );

    // Verify global generate_tid() also produces 13-char base32 TIDs
    let global_tid = generate_tid();
    assert_eq!(global_tid.len(), 13);
}

#[test]
fn test_adv_format_at_uri_combinations() {
    assert_eq!(
        format_at_uri("did:plc:alice", "app.bsky.feed.post", "3k2xyz"),
        "at://did:plc:alice/app.bsky.feed.post/3k2xyz"
    );
    assert_eq!(
        format_at_uri("did:web:example.com", "com.custom.collection", "key_1"),
        "at://did:web:example.com/com.custom.collection/key_1"
    );
    assert_eq!(
        format_at_uri("did:plc:bob", "app.bsky.graph.follow", "3l4..."),
        "at://did:plc:bob/app.bsky.graph.follow/3l4..."
    );
}
