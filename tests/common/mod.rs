//! Shared test infrastructure and mock doubles for Skybase E2E tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::result_large_err,
    missing_docs,
    dead_code
)]

use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::json;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mock PDS XRPC server simulating ATProto repo mutations and DPoP authentication.
pub struct MockPdsServer {
    server: MockServer,
    created_records: Arc<Mutex<Vec<serde_json::Value>>>,
    deleted_records: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl MockPdsServer {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        let created_records = Arc::new(Mutex::new(Vec::new()));
        let deleted_records = Arc::new(Mutex::new(Vec::new()));

        // Mount default createRecord responder
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .and(header_exists("authorization"))
            .and(header_exists("dpop"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let repo = body["repo"].as_str().unwrap_or("did:plc:unknown");
                let collection = body["collection"].as_str().unwrap_or("unknown");
                let rkey = body["rkey"].as_str().unwrap_or("test_rkey");

                let uri = format!("at://{repo}/{collection}/{rkey}");
                let cid = "bafyreih5678mockcidvalue9876543210".to_string();

                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": uri,
                    "cid": cid,
                }))
            })
            .mount(&server)
            .await;

        // Mount default deleteRecord responder
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.deleteRecord"))
            .and(header_exists("authorization"))
            .and(header_exists("dpop"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        Self {
            server,
            created_records,
            deleted_records,
        }
    }

    pub fn uri(&self) -> String {
        self.server.uri()
    }

    /// Mounts a one-time DPoP nonce challenge (401 use_dpop_nonce) on createRecord.
    pub async fn mount_nonce_challenge_once(&self, nonce_value: &str) {
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", nonce_value)
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "DPoP proof requires nonce"
                    })),
            )
            .up_to_n_times(1)
            .mount(&self.server)
            .await;
    }
}
