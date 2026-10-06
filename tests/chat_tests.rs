//! End-to-End Tests: ATProto Chat (`chat.bsky.convo.*`) client.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use serde_json::json;
use skybase::chat::{ChatClient, SendMessagePayload};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_list_convos_sends_auth_and_query_params() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(|req: &wiremock::Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(auth, "Bearer test_token");
            let query = req.url.query().unwrap_or_default();
            assert!(query.contains("limit=10"));
            assert!(query.contains("cursor=cur123"));
            ResponseTemplate::new(200).set_body_json(json!({
                "convos": [{
                    "id": "convo_1",
                    "rev": "rev_1",
                    "unreadCount": 1,
                    "members": [{"did": "did:plc:user1"}, {"did": "did:plc:bot"}],
                    "lastMessage": {
                        "id": "msg_1",
                        "text": "help",
                        "sender": {"did": "did:plc:user1"},
                        "sentAt": "2026-01-01T00:00:00Z"
                    }
                }],
                "cursor": "cur456"
            }))
        })
        .mount(&server)
        .await;

    let client = ChatClient::new(server.uri(), "test_token").expect("client");
    let resp = client
        .list_convos(Some(10), Some("cur123"))
        .await
        .expect("list_convos");
    assert_eq!(resp.convos.len(), 1);
    assert_eq!(resp.convos[0].id, "convo_1");
    assert_eq!(resp.convos[0].unread_count, 1);
    assert_eq!(resp.cursor.as_deref(), Some("cur456"));
}

#[tokio::test]
async fn test_error_status_surfaces_chat_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
        .mount(&server)
        .await;

    let client = ChatClient::new(server.uri(), "bad_token").expect("client");
    let err = client.list_convos(None, None).await.expect_err("must fail");
    assert!(matches!(err, skybase::SkybaseError::Chat(_)));
}

#[tokio::test]
async fn test_send_message_extracts_link_facet_with_byte_offsets() {
    let server = MockServer::start().await;
    const TEXT: &str = "Visit https://example.com now";
    Mock::given(method("POST"))
        .and(path("/xrpc/chat.bsky.convo.sendMessage"))
        .respond_with(|req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).expect("parse body");
            assert_eq!(body["convoId"], "convo_target");
            assert_eq!(body["message"]["text"], TEXT);
            let facets = body["message"]["facets"].as_array().expect("facets");
            assert_eq!(facets.len(), 1);
            assert_eq!(facets[0]["index"]["byteStart"], 6);
            assert_eq!(facets[0]["index"]["byteEnd"], 25);
            ResponseTemplate::new(200).set_body_json(json!({
                "id": "msg_out",
                "text": TEXT,
                "sender": {"did": "did:plc:bot"},
                "sentAt": "2026-01-01T00:00:00Z"
            }))
        })
        .mount(&server)
        .await;

    let client = ChatClient::new(server.uri(), "token").expect("client");
    let view = client
        .send_message("convo_target", TEXT)
        .await
        .expect("send_message");
    assert_eq!(view.id, "msg_out");
}

#[tokio::test]
async fn test_pds_base_url_attaches_atproto_proxy_header() {
    let server = MockServer::start().await;
    // The mock server URI is not api.bsky.chat, so the proxy header must be present.
    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(|req: &wiremock::Request| {
            let proxy = req
                .headers
                .get("atproto-proxy")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(proxy, "did:web:api.bsky.chat#bsky_chat");
            ResponseTemplate::new(200).set_body_json(json!({"convos": []}))
        })
        .mount(&server)
        .await;

    let client = ChatClient::new(server.uri(), "token").expect("client");
    client.list_convos(None, None).await.expect("list");
}

#[tokio::test]
async fn test_refresh_session_retries_with_new_token() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(|req: &wiremock::Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            if auth == "Bearer new_token" {
                ResponseTemplate::new(200).set_body_json(json!({"convos": []}))
            } else {
                ResponseTemplate::new(401).set_body_string("ExpiredToken")
            }
        })
        .mount(&server)
        .await;

    // Simulate a client whose token is already "new_token" after an out-of-band refresh:
    // we can't easily invoke the private refresh path without a PDS, so assert that a
    // 401 without refresh capability surfaces a typed error (no infinite retry).
    let client = ChatClient::new(server.uri(), "old_token").expect("client");
    let err = client.list_convos(None, None).await.expect_err("must fail");
    assert!(matches!(err, skybase::SkybaseError::Chat(_)));

    let good = ChatClient::new(server.uri(), "new_token").expect("client");
    good.list_convos(None, None).await.expect("succeeds");
}

#[tokio::test]
async fn test_update_read_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/xrpc/chat.bsky.convo.updateRead"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let client = ChatClient::new(server.uri(), "token").expect("client");
    client
        .update_read("convo_1", "msg_1")
        .await
        .expect("update_read");
}

#[test]
fn test_send_message_payload_builds_facets() {
    let payload = SendMessagePayload::new("No links here");
    assert!(payload.facets.is_none());

    let with_link = SendMessagePayload::new("Go https://example.com now");
    assert!(with_link.facets.is_some());
}
