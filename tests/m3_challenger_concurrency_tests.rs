//! Empirical Challenger Concurrency & Contention Stress Tests for `skybase::repo` (Milestone 3).
//!
//! Adversarially stress-tests:
//! 1. TID Monotonicity & Uniqueness under massive multi-threaded contention (100,000 TIDs across 32 threads).
//! 2. TID Sub-Microsecond Sequence Progression in tight loops.
//! 3. TID Base32 Lexicographical Sort-Order Invariance and chronological preservation.
//! 4. Global Singleton `generate_tid()` multi-threaded safety and collision resistance.
//! 5. Concurrent Multi-Threaded PDS Writes with Shared `Arc<PdsRepoClient>` (300 writes across 12 tasks).
//! 6. Interleaved Concurrent Creates and Deletes (320 operations across 16 tasks).
//! 7. Cloned Client Concurrent Hammer across disparate worker tasks.
//! 8. Thundering Herd Nonce Challenge Storm (12 concurrent unauthenticated tasks receiving 401 use_dpop_nonce simultaneously and recovering on single retry).
//! 9. Nonce Rotation In-Flight across concurrent workers.
//! 10. Persistent 401 Challenge Retry-Cap Defense (fail-closed after exactly 1 retry).
//! 11. Multi-Tenant Session Isolation & Credential Cross-Contamination Prevention.
//! 12. Nonce Cache Isolation across Distinct PDS Origins.
//! 13. Dynamic PDS Endpoint Override Integrity under Concurrency.
//! 14. Expired Session Fail-Closed Rejection without Network Dispatch.
//! 15. Bounded Error Body Mitigation (64 KB cap) under Concurrent Error Floods.
//! 16. Preflight RKey Validation Under Concurrency (Filtering Invalid Keys Before Wire Dispatch).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::manual_is_multiple_of,
    unused_imports,
    missing_docs
)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinSet;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use skyauth::dpop::{DPoPKey, DPoPNonceCache};
use skyauth::session::OAuthSession;
use skybase::error::SkybaseError;
use skybase::repo::{format_at_uri, generate_tid, validate_rkey, PdsRepoClient, TidGenerator};

/// Serializes MockServer port and socket allocation across tests to prevent
/// macOS process file-descriptor exhaustion (`ulimit -n 256`).
static WIREMOCK_LOCK: TokioMutex<()> = TokioMutex::const_new(());

// ============================================================================
// Helper Utilities for Pure-Rust DPoP Inspection
// ============================================================================

/// Decodes a URL-safe Base64 string without external crate dependencies.
fn decode_base64_url(input: &str) -> Option<Vec<u8>> {
    let mut s = input.replace('-', "+").replace('_', "/");
    while s.len() % 4 != 0 {
        s.push('=');
    }
    let chars: Vec<u8> = s.bytes().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let b0 = decode_b64_byte(chars[i])?;
        let b1 = decode_b64_byte(chars[i + 1])?;
        out.push((b0 << 2) | (b1 >> 4));
        if chars[i + 2] != b'=' {
            let b2 = decode_b64_byte(chars[i + 2])?;
            out.push(((b1 & 0x0f) << 4) | (b2 >> 2));
            if chars[i + 3] != b'=' {
                let b3 = decode_b64_byte(chars[i + 3])?;
                out.push(((b2 & 0x03) << 6) | b3);
            }
        }
        i += 4;
    }
    Some(out)
}

fn decode_b64_byte(b: u8) -> Option<u8> {
    match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        b'=' => Some(0),
        _ => None,
    }
}

/// Checks whether a compact DPoP JWT payload contains the specified `nonce` field.
fn dpop_jwt_contains_nonce(dpop_jwt: &str, expected_nonce: &str) -> bool {
    let parts: Vec<&str> = dpop_jwt.trim().split('.').collect();
    if parts.len() < 2 {
        return false;
    }
    if let Some(payload_bytes) = decode_base64_url(parts[1]) {
        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&payload_bytes) {
            return json_val
                .get("nonce")
                .and_then(|v| v.as_str())
                .is_some_and(|n| n == expected_nonce);
        }
    }
    false
}

// ============================================================================
// 1. TID Monotonicity, Uniqueness & Sort-Order Invariants
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_challenger_tid_massive_concurrency_uniqueness_and_monotonicity() {
    let generator = Arc::new(TidGenerator::new());
    let thread_count = 32;
    let per_thread = 3_125; // 32 * 3,125 = 100,000 total TIDs
    let mut set = JoinSet::new();

    for _ in 0..thread_count {
        let gen = Arc::clone(&generator);
        set.spawn(async move {
            let mut tids = Vec::with_capacity(per_thread);
            for _ in 0..per_thread {
                tids.push(gen.next_tid());
            }
            tids
        });
    }

    let mut all_tids = HashSet::with_capacity(thread_count * per_thread);
    let allowed_charset = b"234567abcdefghijklmnopqrstuvwxyz";

    while let Some(res) = set.join_next().await {
        let tids = res.expect("task join failed");

        // 1. Strict per-thread monotonicity
        for i in 0..tids.len().saturating_sub(1) {
            assert!(
                tids[i] < tids[i + 1],
                "Thread sequence violation: tids[{}] ({}) must be < tids[{}] ({})",
                i,
                tids[i],
                i + 1,
                tids[i + 1]
            );
        }

        // 2. Global uniqueness and charset compliance
        for tid in tids {
            assert_eq!(tid.len(), 13, "TID must be exactly 13 characters: {tid}");
            for b in tid.bytes() {
                assert!(
                    allowed_charset.contains(&b),
                    "Character '{b}' not in base32 charset in TID '{tid}'"
                );
            }
            assert!(
                all_tids.insert(tid.clone()),
                "Duplicate TID detected across threads: {tid}"
            );
        }
    }

    assert_eq!(
        all_tids.len(),
        thread_count * per_thread,
        "All 100,000 TIDs must be unique"
    );
}

#[test]
fn test_challenger_tid_tight_loop_sub_microsecond_sequence_monotonicity() {
    let generator = TidGenerator::new();
    let total_iterations = 10_000;
    let mut prev = generator.next_tid();

    for i in 1..total_iterations {
        let curr = generator.next_tid();
        assert!(
            curr > prev,
            "Iteration {i}: tight-loop TID monotonicity violation: prev={prev}, curr={curr}"
        );
        prev = curr;
    }
}

#[tokio::test]
async fn test_challenger_tid_base32_lexicographical_and_chronological_sort_invariance() {
    let generator = TidGenerator::new();
    let mut generated = Vec::new();

    for _ in 0..50 {
        generated.push(generator.next_tid());
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut sorted = generated.clone();
    sorted.sort();

    assert_eq!(
        generated, sorted,
        "Chronological generation order must be identical to string lexicographical sort order"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_global_generate_tid_concurrent_stress() {
    let mut set = JoinSet::new();
    let workers = 20;
    let per_worker = 1_000;

    for _ in 0..workers {
        set.spawn(async move {
            let mut batch = Vec::with_capacity(per_worker);
            for _ in 0..per_worker {
                batch.push(generate_tid());
            }
            batch
        });
    }

    let mut global_set = HashSet::with_capacity(workers * per_worker);
    while let Some(res) = set.join_next().await {
        let batch = res.expect("global worker join");
        for tid in batch {
            assert_eq!(tid.len(), 13);
            assert!(
                global_set.insert(tid),
                "Global generator produced duplicate TID"
            );
        }
    }

    assert_eq!(global_set.len(), workers * per_worker);
}

// ============================================================================
// 2. Concurrent Multi-Threaded Writes & Deletes
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_writes_shared_client_high_load() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(|req: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let repo = body["repo"].as_str().unwrap_or("did:plc:unknown");
            let collection = body["collection"].as_str().unwrap_or("unknown");
            let rkey = body["rkey"].as_str().unwrap_or("unknown_rkey");

            let uri = format!("at://{repo}/{collection}/{rkey}");
            let cid = format!("bafyrei_{rkey}_cid");

            ResponseTemplate::new(200).set_body_json(json!({
                "uri": uri,
                "cid": cid,
            }))
        })
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:alice_concurrent", "alice_token")
            .expect("client creation"),
    );

    let num_tasks = 12;
    let writes_per_task = 25; // 300 total writes
    let mut set = JoinSet::new();

    for task_id in 0..num_tasks {
        let client = Arc::clone(&client);
        set.spawn(async move {
            let mut results = Vec::with_capacity(writes_per_task);
            for w in 0..writes_per_task {
                let collection = match w % 4 {
                    0 => "app.bsky.feed.post",
                    1 => "app.bsky.feed.like",
                    2 => "app.bsky.graph.follow",
                    _ => "com.example.custom.record",
                };
                let rkey = format!("task_{task_id}_rkey_{w}");
                let payload = json!({
                    "task": task_id,
                    "write_index": w,
                    "timestamp": "2026-09-12T01:00:00Z"
                });

                let res = client
                    .create_record(collection, Some(&rkey), &payload, true)
                    .await;
                results.push((collection, rkey, res));
            }
            results
        });
    }

    let mut successful_writes = 0;
    while let Some(task_res) = set.join_next().await {
        let writes = task_res.expect("task join");
        for (col, rkey, res) in writes {
            let create_res = res.expect("concurrent create_record must succeed");
            assert_eq!(
                create_res.uri,
                format!("at://did:plc:alice_concurrent/{col}/{rkey}")
            );
            assert_eq!(create_res.cid, format!("bafyrei_{rkey}_cid"));
            successful_writes += 1;
        }
    }

    assert_eq!(successful_writes, num_tasks * writes_per_task);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_interleaved_creates_and_deletes() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(|req: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let repo = body["repo"].as_str().unwrap_or("did:plc:unknown");
            let collection = body["collection"].as_str().unwrap_or("unknown");
            let rkey = body["rkey"].as_str().unwrap_or("rkey");

            ResponseTemplate::new(200).set_body_json(json!({
                "uri": format!("at://{repo}/{collection}/{rkey}"),
                "cid": "bafyrei_commit_cid",
            }))
        })
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.deleteRecord"))
        .and(header_exists("authorization"))
        .and(header_exists("dpop"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(
            server.uri(),
            "did:plc:interleave_user",
            "interleave_token",
        )
        .expect("client creation"),
    );

    let num_tasks = 16;
    let ops_per_task = 20; // 16 * 20 = 320 total operations
    let mut set = JoinSet::new();

    for task_id in 0..num_tasks {
        let client = Arc::clone(&client);
        let is_creator = task_id % 2 == 0;

        set.spawn(async move {
            for op_id in 0..ops_per_task {
                let rkey = format!("key_t{task_id}_op{op_id}");
                if is_creator {
                    let res = client
                        .create_record(
                            "app.bsky.feed.post",
                            Some(&rkey),
                            &json!({ "msg": "interleaved" }),
                            true,
                        )
                        .await;
                    assert!(res.is_ok(), "create_record failed in interleaved test");
                } else {
                    let res = client.delete_record("app.bsky.feed.post", &rkey).await;
                    assert!(res.is_ok(), "delete_record failed in interleaved test");
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task join successful");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_cloned_clients_hammer() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "uri": "at://did:plc:clone_user/app.bsky.feed.post/cloned_rkey",
            "cid": "bafyrei_cloned_cid"
        })))
        .mount(&server)
        .await;

    let base_client =
        PdsRepoClient::from_credentials(server.uri(), "did:plc:clone_user", "clone_token")
            .expect("base client creation");

    let num_tasks = 12;
    let writes_per_task = 15;
    let mut set = JoinSet::new();

    for _ in 0..num_tasks {
        let cloned_client = base_client.clone();
        set.spawn(async move {
            for _ in 0..writes_per_task {
                let res = cloned_client
                    .create_record(
                        "app.bsky.feed.post",
                        Some("cloned_rkey"),
                        &json!({ "val": 42 }),
                        false,
                    )
                    .await;
                assert!(res.is_ok());
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task completed without panic");
    }
}

// ============================================================================
// 3. Nonce Challenge Storms, Concurrent Retries & Race Condition Elimination
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_nonce_challenge_storm_and_recovery() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;
    let challenge_nonce = "storm_nonce_challenge_999";

    let initial_challenges = Arc::new(AtomicUsize::new(0));
    let successful_retries = Arc::new(AtomicUsize::new(0));

    let challenges_counter = Arc::clone(&initial_challenges);
    let retries_counter = Arc::clone(&successful_retries);

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |req: &Request| {
            let dpop_header = req
                .headers
                .get("dpop")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            // If the DPoP proof carries the challenged nonce, accept it
            if dpop_jwt_contains_nonce(dpop_header, challenge_nonce) {
                retries_counter.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:storm_user/app.bsky.feed.post/storm_post",
                    "cid": "bafyrei_storm_cid"
                }))
            } else {
                // Otherwise challenge with 401 use_dpop_nonce
                challenges_counter.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", challenge_nonce)
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "DPoP proof requires nonce"
                    }))
            }
        })
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:storm_user", "storm_token")
            .expect("client creation"),
    );

    // Initial state: nonce cache is completely empty
    let origin = url::Url::parse(&server.uri())
        .unwrap()
        .origin()
        .ascii_serialization();
    assert_eq!(client.nonce_cache().get_nonce(&origin), None);

    // Blast 12 concurrent requests simultaneously
    let concurrent_tasks = 12;
    let mut set = JoinSet::new();

    for i in 0..concurrent_tasks {
        let client = Arc::clone(&client);
        set.spawn(async move {
            let rkey = format!("storm_post_{i}");
            client
                .create_record(
                    "app.bsky.feed.post",
                    Some(&rkey),
                    &json!({ "storm_id": i }),
                    true,
                )
                .await
        });
    }

    let mut success_count = 0;
    while let Some(res) = set.join_next().await {
        let create_res = res.expect("join").expect("all storm tasks must succeed");
        assert_eq!(create_res.cid, "bafyrei_storm_cid");
        success_count += 1;
    }

    assert_eq!(
        success_count, concurrent_tasks,
        "All 12 concurrent tasks must recover and succeed"
    );

    // Verify that the fresh nonce is stored in the cache
    assert_eq!(
        client.nonce_cache().get_nonce(&origin).as_deref(),
        Some(challenge_nonce)
    );

    // Initial challenges were fired and retries succeeded
    assert!(
        initial_challenges.load(Ordering::SeqCst) > 0,
        "At least one request was challenged"
    );
    assert_eq!(
        successful_retries.load(Ordering::SeqCst),
        concurrent_tasks,
        "Exactly 12 successful 200 responses were generated"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_nonce_rotation_during_flight() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;
    let nonce_phase = Arc::new(AtomicUsize::new(1));
    let nonce_a = "nonce_alpha_111";
    let nonce_b = "nonce_beta_222";

    let phase_ref = Arc::clone(&nonce_phase);

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |req: &Request| {
            let dpop_header = req
                .headers
                .get("dpop")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            let current_phase = phase_ref.load(Ordering::SeqCst);
            let expected_nonce = if current_phase == 1 { nonce_a } else { nonce_b };

            if dpop_jwt_contains_nonce(dpop_header, expected_nonce) {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:rot_user/app.bsky.feed.post/p1",
                    "cid": "bafyrei_rot_cid"
                }))
            } else {
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", expected_nonce)
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "Nonce rotated"
                    }))
            }
        })
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:rot_user", "rot_token")
            .expect("client creation"),
    );

    // Warm up client with Phase 1 (acquires Nonce A)
    let first_res = client
        .create_record(
            "app.bsky.feed.post",
            Some("p1"),
            &json!({ "phase": 1 }),
            true,
        )
        .await;
    assert!(first_res.is_ok());

    let origin = url::Url::parse(&server.uri())
        .unwrap()
        .origin()
        .ascii_serialization();
    assert_eq!(
        client.nonce_cache().get_nonce(&origin).as_deref(),
        Some(nonce_a)
    );

    // Rotate server to Phase 2 (now requires Nonce B)
    nonce_phase.store(2, Ordering::SeqCst);

    // Launch 10 concurrent tasks using the cached Nonce A
    // All will get 401 with Nonce B, re-cache Nonce B, retry, and succeed!
    let mut set = JoinSet::new();
    for i in 0..10 {
        let client = Arc::clone(&client);
        set.spawn(async move {
            let rk = format!("rot_p_{i}");
            client
                .create_record(
                    "app.bsky.feed.post",
                    Some(&rk),
                    &json!({ "phase": 2 }),
                    true,
                )
                .await
        });
    }

    while let Some(res) = set.join_next().await {
        assert!(res.expect("join").is_ok(), "rotation retry must succeed");
    }

    assert_eq!(
        client.nonce_cache().get_nonce(&origin).as_deref(),
        Some(nonce_b)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_concurrent_infinite_challenge_retry_cap_defense() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;

    // Server perpetually challenges with 401 on every attempt
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("DPoP-Nonce", "perpetual_nonce")
                .set_body_json(json!({
                    "error": "use_dpop_nonce",
                    "message": "Never satisfied"
                })),
        )
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:loop_user", "loop_token")
            .expect("client creation"),
    );

    let mut set = JoinSet::new();
    let workers = 10;

    for i in 0..workers {
        let client = Arc::clone(&client);
        set.spawn(async move {
            let rk = format!("loop_{i}");
            client
                .create_record("app.bsky.feed.post", Some(&rk), &json!({}), false)
                .await
        });
    }

    while let Some(res) = set.join_next().await {
        let write_res = res.expect("join");
        assert!(write_res.is_err());
        let err_msg = write_res.unwrap_err().to_string();
        assert!(
            err_msg.contains("retry limit exceeded"),
            "Error must terminate on retry limit exceeded, got: {err_msg}"
        );
    }
}

// ============================================================================
// 4. Session Isolation, Credentials Privacy & PDS Endpoint Overrides
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_session_isolation_independent_tenants() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server_alice = MockServer::start().await;
    let server_bob = MockServer::start().await;
    let server_charlie = MockServer::start().await;

    // Mount Alice's responder
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(|req: &Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let repo = body["repo"].as_str().unwrap_or("");

            if auth == "DPoP token_alice_secret" && repo == "did:plc:alice_tenant" {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:alice_tenant/app.bsky.feed.post/post_a",
                    "cid": "cid_a"
                }))
            } else {
                ResponseTemplate::new(403).set_body_json(json!({ "error": "Forbidden" }))
            }
        })
        .mount(&server_alice)
        .await;

    // Mount Bob's responder
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(|req: &Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let repo = body["repo"].as_str().unwrap_or("");

            if auth == "DPoP token_bob_secret" && repo == "did:plc:bob_tenant" {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:bob_tenant/app.bsky.feed.post/post_b",
                    "cid": "cid_b"
                }))
            } else {
                ResponseTemplate::new(403).set_body_json(json!({ "error": "Forbidden" }))
            }
        })
        .mount(&server_bob)
        .await;

    // Mount Charlie's responder
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(|req: &Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let repo = body["repo"].as_str().unwrap_or("");

            if auth == "DPoP token_charlie_secret" && repo == "did:plc:charlie_tenant" {
                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": "at://did:plc:charlie_tenant/app.bsky.feed.post/post_c",
                    "cid": "cid_c"
                }))
            } else {
                ResponseTemplate::new(403).set_body_json(json!({ "error": "Forbidden" }))
            }
        })
        .mount(&server_charlie)
        .await;

    let client_alice = Arc::new(
        PdsRepoClient::from_credentials(
            server_alice.uri(),
            "did:plc:alice_tenant",
            "token_alice_secret",
        )
        .expect("alice client"),
    );

    let client_bob = Arc::new(
        PdsRepoClient::from_credentials(server_bob.uri(), "did:plc:bob_tenant", "token_bob_secret")
            .expect("bob client"),
    );

    let client_charlie = Arc::new(
        PdsRepoClient::from_credentials(
            server_charlie.uri(),
            "did:plc:charlie_tenant",
            "token_charlie_secret",
        )
        .expect("charlie client"),
    );

    let mut set = JoinSet::new();
    let per_tenant = 6;

    for i in 0..per_tenant {
        let ca = Arc::clone(&client_alice);
        set.spawn(async move {
            ca.create_record(
                "app.bsky.feed.post",
                Some(&format!("post_a_{i}")),
                &json!({ "owner": "alice" }),
                true,
            )
            .await
        });

        let cb = Arc::clone(&client_bob);
        set.spawn(async move {
            cb.create_record(
                "app.bsky.feed.post",
                Some(&format!("post_b_{i}")),
                &json!({ "owner": "bob" }),
                true,
            )
            .await
        });

        let cc = Arc::clone(&client_charlie);
        set.spawn(async move {
            cc.create_record(
                "app.bsky.feed.post",
                Some(&format!("post_c_{i}")),
                &json!({ "owner": "charlie" }),
                true,
            )
            .await
        });
    }

    let mut total_success = 0;
    while let Some(res) = set.join_next().await {
        let write_res = res.expect("join");
        assert!(
            write_res.is_ok(),
            "Multi-tenant write must succeed with strict isolation"
        );
        total_success += 1;
    }

    assert_eq!(total_success, per_tenant * 3);
}

#[test]
fn test_challenger_nonce_cache_isolation_across_endpoints() {
    let cache = DPoPNonceCache::new();

    cache.set_nonce("https://pds1.example.com", "nonce_1");
    cache.set_nonce("https://pds2.example.com", "nonce_2");

    assert_eq!(
        cache.get_nonce("https://pds1.example.com").as_deref(),
        Some("nonce_1")
    );
    assert_eq!(
        cache.get_nonce("https://pds2.example.com").as_deref(),
        Some("nonce_2")
    );
    assert_eq!(cache.get_nonce("https://pds3.example.com"), None);

    // Clear origin 1, origin 2 must remain intact
    cache.clear_nonce("https://pds1.example.com");
    assert_eq!(cache.get_nonce("https://pds1.example.com"), None);
    assert_eq!(
        cache.get_nonce("https://pds2.example.com").as_deref(),
        Some("nonce_2")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_endpoint_override_preserves_session_integrity() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server_active = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "uri": "at://did:plc:override_user/app.bsky.feed.post/rk1",
            "cid": "cid_override"
        })))
        .mount(&server_active)
        .await;

    // Dummy base endpoint that would fail if accessed
    let client = PdsRepoClient::from_credentials(
        "https://non-existent-pds-endpoint.invalid",
        "did:plc:override_user",
        "token_override",
    )
    .expect("creation")
    .with_endpoint(server_active.uri());

    assert_eq!(
        client.pds_endpoint().expect("override"),
        server_active.uri()
    );

    let client_arc = Arc::new(client);
    let mut set = JoinSet::new();

    for i in 0..10 {
        let c = Arc::clone(&client_arc);
        set.spawn(async move {
            c.create_record(
                "app.bsky.feed.post",
                Some(&format!("p_{i}")),
                &json!({ "i": i }),
                false,
            )
            .await
        });
    }

    while let Some(res) = set.join_next().await {
        assert!(res.expect("join").is_ok());
    }
}

#[tokio::test]
async fn test_challenger_expired_session_fails_closed_without_network() {
    let session = OAuthSession::new(
        "did:plc:expired_user",
        "expired_token",
        None,
        "DPoP",
        None,
        Some(0), // 0 seconds lifetime
        DPoPKey::generate(),
        Some("https://pds.example.com".into()),
        None,
        None,
    )
    .expect("session creation");

    let client = PdsRepoClient::from_session(Arc::new(session));

    let res = client
        .create_record("app.bsky.feed.post", Some("post_1"), &json!({}), false)
        .await;

    assert!(res.is_err(), "Expired session must fail immediately");
    let err = res.unwrap_err();
    assert!(
        matches!(err, SkybaseError::Auth(_)),
        "Must return SkybaseError::Auth on expired token, got: {err}"
    );

    // Delete must also fail closed
    let del_res = client.delete_record("app.bsky.feed.post", "post_1").await;
    assert!(matches!(del_res.unwrap_err(), SkybaseError::Auth(_)));
}

// ============================================================================
// 5. Bounded Payloads, Memory Safety & Preflight Error Filtering Under Load
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_bounded_error_body_mitigation_under_concurrency() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;

    // Return a massive 256 KB error payload
    let massive_error_body = vec![b'X'; 256 * 1024];

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(ResponseTemplate::new(400).set_body_bytes(massive_error_body))
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:flood_user", "flood_token")
            .expect("client creation"),
    );

    let mut set = JoinSet::new();
    let workers = 10;

    for i in 0..workers {
        let client = Arc::clone(&client);
        set.spawn(async move {
            client
                .create_record(
                    "app.bsky.feed.post",
                    Some(&format!("flood_{i}")),
                    &json!({ "flood": true }),
                    true,
                )
                .await
        });
    }

    while let Some(res) = set.join_next().await {
        let write_res = res.expect("join");
        assert!(write_res.is_err());
        let err_msg = write_res.unwrap_err().to_string();
        assert!(
            err_msg.contains("HTTP request failed with status 400"),
            "Must report 400 status cleanly"
        );
        // Ensure error message does not blow up unbounded
        assert!(
            err_msg.len() <= 70_000,
            "Error message length must be bounded below 70 KB, got: {}",
            err_msg.len()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_challenger_preflight_invalid_rkey_filtering_under_concurrency() {
    let _lock = WIREMOCK_LOCK.lock().await;
    let server = MockServer::start().await;
    let request_counter = Arc::new(AtomicUsize::new(0));
    let req_counter = Arc::clone(&request_counter);

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .respond_with(move |_req: &Request| {
            req_counter.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({
                "uri": "at://did:plc:rkey_user/app.bsky.feed.post/valid_key",
                "cid": "bafyrei_valid_cid"
            }))
        })
        .mount(&server)
        .await;

    let client = Arc::new(
        PdsRepoClient::from_credentials(server.uri(), "did:plc:rkey_user", "rkey_token")
            .expect("client creation"),
    );

    let invalid_keys = vec![
        "",
        ".",
        "..",
        "has space",
        "invalid/slash",
        "hash#tag",
        "question?mark",
        "null\0byte",
    ];

    let valid_keys = vec![
        "valid-key-01",
        "valid_key_02",
        "valid.key.03",
        "valid~key~04",
        "3k2xyz987",
        "a",
        "profile",
        "post-100",
    ];

    let mut set = JoinSet::new();

    // Spawn invalid tasks (expect Err before network dispatch)
    for bad_key in invalid_keys {
        let client = Arc::clone(&client);
        let key = bad_key.to_string();
        set.spawn(async move {
            let res = client
                .create_record("app.bsky.feed.post", Some(&key), &json!({}), false)
                .await;
            (false, res)
        });
    }

    // Spawn valid tasks (expect Ok after network dispatch)
    for good_key in valid_keys {
        let client = Arc::clone(&client);
        let key = good_key.to_string();
        set.spawn(async move {
            let res = client
                .create_record("app.bsky.feed.post", Some(&key), &json!({}), false)
                .await;
            (true, res)
        });
    }

    let mut valid_success = 0;
    let mut invalid_failures = 0;

    while let Some(res) = set.join_next().await {
        let (is_valid_expected, op_res) = res.expect("join");
        if is_valid_expected {
            assert!(op_res.is_ok(), "Valid rkey must succeed");
            valid_success += 1;
        } else {
            assert!(op_res.is_err(), "Invalid rkey must fail preflight");
            assert!(matches!(op_res.unwrap_err(), SkybaseError::Repo(_)));
            invalid_failures += 1;
        }
    }

    assert_eq!(valid_success, 8);
    assert_eq!(invalid_failures, 8);

    // Mock server must have only received the 8 valid requests!
    assert_eq!(
        request_counter.load(Ordering::SeqCst),
        8,
        "Mock server must receive exactly 8 requests (zero invalid keys reached network)"
    );
}

#[test]
fn test_challenger_format_at_uri_and_validate_rkey_adversarial_matrix() {
    // 1. format_at_uri
    let uri = format_at_uri("did:plc:alice", "app.bsky.feed.post", "3k2abc");
    assert_eq!(uri, "at://did:plc:alice/app.bsky.feed.post/3k2abc");

    // 2. validate_rkey boundaries
    assert!(validate_rkey(&"a".repeat(512)).is_ok());
    assert!(validate_rkey(&"a".repeat(513)).is_err());
    assert!(validate_rkey("").is_err());
    assert!(validate_rkey(".").is_err());
    assert!(validate_rkey("..").is_err());
    assert!(validate_rkey("...").is_ok());
    assert!(validate_rkey("valid-rkey_123~abc.def").is_ok());

    // Disallowed characters
    for ch in ['/', ' ', '\\', '@', ':', '#', '?', '%', '&', '+', '='] {
        let bad_rkey = format!("bad{ch}key");
        assert!(
            validate_rkey(&bad_rkey).is_err(),
            "Char '{ch}' must be rejected in rkey"
        );
    }
}
