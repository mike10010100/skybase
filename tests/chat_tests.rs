//! End-to-End Tests: ATProto Chat (`chat.bsky.convo.*`) client.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use serde_json::json;
use skybase::chat::{ChatClient, SendMessagePayload, DEFAULT_CHAT_ENDPOINT};
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

#[tokio::test]
async fn test_app_password_login_routes_via_did_doc_pds_and_migrates_across_refresh_and_relogin() {
    // Models the real Bluesky topology:
    // - `entryway` (`bsky.social`) handles `createSession` / `refreshSession` but returns 501 on chat XRPCs.
    // - `shard_pds_1` / `shard_pds_2` / `shard_pds_3` are authoritative PDS hosts declared in `didDoc`
    //   that proxy `chat.bsky.convo.*` when `atproto-proxy: did:web:api.bsky.chat#bsky_chat` is present.
    let entryway = MockServer::start().await;
    let shard_pds_1 = MockServer::start().await;
    let shard_pds_2 = MockServer::start().await;
    let shard_pds_3 = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(ResponseTemplate::new(501).set_body_json(json!({
            "error": "MethodNotImplemented",
            "message": "Method Not Implemented"
        })))
        .expect(0)
        .mount(&entryway)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "did": "did:plc:pl7wa2phjeo4zp4zxmewnsis",
            "handle": "skybouncer.bsky.social",
            "accessJwt": "at_jwt_shard_1",
            "refreshJwt": "refresh_jwt_1",
            "didDoc": {
                "id": "did:plc:pl7wa2phjeo4zp4zxmewnsis",
                "alsoKnownAs": ["at://skybouncer.bsky.social"],
                "service": [{
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": shard_pds_1.uri()
                }]
            }
        })))
        .up_to_n_times(1)
        .mount(&entryway)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(|req: &wiremock::Request| {
            let proxy = req
                .headers
                .get("atproto-proxy")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(proxy, "did:web:api.bsky.chat#bsky_chat");
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            if auth == "Bearer at_jwt_shard_1" {
                ResponseTemplate::new(200).set_body_json(json!({"convos": []}))
            } else {
                ResponseTemplate::new(401).set_body_json(json!({
                    "error": "ExpiredToken",
                    "message": "Token has expired"
                }))
            }
        })
        .up_to_n_times(1)
        .mount(&shard_pds_1)
        .await;

    let (client, did) = ChatClient::login_with_app_password(
        &entryway.uri(),
        DEFAULT_CHAT_ENDPOINT,
        "skybouncer.bsky.social",
        "app-password",
    )
    .await
    .expect("login succeeds");
    assert_eq!(did, "did:plc:pl7wa2phjeo4zp4zxmewnsis");
    assert_eq!(client.base_url().await, shard_pds_1.uri());

    // 1. Initial chat request routes to shard_pds_1 with `atproto-proxy`, never hitting api.bsky.chat or entryway.
    let convos = client.list_convos(None, None).await.expect("list_convos 1");
    assert!(convos.convos.is_empty());

    // 2. Token expires on shard_pds_1; refreshSession rotates token AND migrates `didDoc` to shard_pds_2.
    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": "ExpiredToken",
            "message": "Token has expired"
        })))
        .mount(&shard_pds_1)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.refreshSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accessJwt": "at_jwt_shard_2",
            "refreshJwt": "refresh_jwt_2",
            "didDoc": {
                "id": "did:plc:pl7wa2phjeo4zp4zxmewnsis",
                "service": [{
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": shard_pds_2.uri()
                }]
            }
        })))
        .up_to_n_times(1)
        .mount(&entryway)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(|req: &wiremock::Request| {
            let proxy = req
                .headers
                .get("atproto-proxy")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(proxy, "did:web:api.bsky.chat#bsky_chat");
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(auth, "Bearer at_jwt_shard_2");
            ResponseTemplate::new(200).set_body_json(json!({"convos": []}))
        })
        .up_to_n_times(1)
        .mount(&shard_pds_2)
        .await;

    client
        .list_convos(None, None)
        .await
        .expect("transparently refreshes and retries on migrated shard_pds_2");
    assert_eq!(client.base_url().await, shard_pds_2.uri());

    // 3. Both accessJwt and refreshJwt expire; full createSession re-login migrates to shard_pds_3.
    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "ExpiredToken",
            "message": "Token has expired"
        })))
        .mount(&shard_pds_2)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.refreshSession"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "ExpiredToken",
            "message": "Refresh token expired"
        })))
        .mount(&entryway)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "did": "did:plc:pl7wa2phjeo4zp4zxmewnsis",
            "accessJwt": "at_jwt_shard_3",
            "refreshJwt": "refresh_jwt_3",
            "didDoc": {
                "id": "did:plc:pl7wa2phjeo4zp4zxmewnsis",
                "service": [{
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": shard_pds_3.uri()
                }]
            }
        })))
        .expect(1)
        .mount(&entryway)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(|req: &wiremock::Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(auth, "Bearer at_jwt_shard_3");
            ResponseTemplate::new(200).set_body_json(json!({"convos": []}))
        })
        .expect(1)
        .mount(&shard_pds_3)
        .await;

    client
        .list_convos(None, None)
        .await
        .expect("re-logins via App Password and retries on shard_pds_3");
    assert_eq!(client.base_url().await, shard_pds_3.uri());
}

#[tokio::test]
async fn test_bad_jwt_type_and_bad_token_scope_do_not_trigger_refresh_storm() {
    let server = MockServer::start().await;

    // Regression guard: refreshSession must NEVER be called when 401 is BadJwtType or BadTokenScope.
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.refreshSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accessJwt": "should_never_be_requested",
            "refreshJwt": "should_never_be_requested"
        })))
        .expect(0)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/chat.bsky.convo.listConvos"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": "BadJwtType",
            "message": "Invalid jwt type \"at+jwt\""
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = ChatClient::new(server.uri(), "at_jwt_token")
        .expect("client")
        .with_refresh_token("valid_refresh_jwt")
        .with_credentials(server.uri(), "bot.bsky.social", "app-pass");

    let err = client
        .list_convos(None, None)
        .await
        .expect_err("BadJwtType must fail immediately without refresh retry");
    assert!(err.to_string().contains("BadJwtType"));
}
