//! End-to-End Tests: Read-only AppView XRPC client.
//!
//! Tiers 1-3 verification following `TEST_INFRA.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use serde_json::json;
use skybase::appview::AppViewClient;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client_for(server: &MockServer) -> AppViewClient {
    AppViewClient::with_endpoint(server.uri())
}

#[tokio::test]
async fn test_resolve_handle_success_and_did_passthrough() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.identity.resolveHandle"))
        .and(query_param("handle", "alice.bsky.social"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"did": "did:plc:alice"})))
        .mount(&server)
        .await;

    let client = client_for(&server);
    assert_eq!(
        client.resolve_handle("alice.bsky.social").await,
        Some("did:plc:alice".to_string())
    );
    // DID passthrough performs no network call.
    assert_eq!(
        client.resolve_handle("did:plc:already").await,
        Some("did:plc:already".to_string())
    );
}

#[tokio::test]
async fn test_resolve_handle_strips_at_prefix() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.identity.resolveHandle"))
        .and(query_param("handle", "bob.bsky.social"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"did": "did:plc:bob"})))
        .mount(&server)
        .await;

    let client = client_for(&server);
    assert_eq!(
        client.resolve_handle("@bob.bsky.social").await,
        Some("did:plc:bob".to_string())
    );
}

#[tokio::test]
async fn test_fetch_profile_maps_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.actor.getProfile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "handle": "alice.bsky.social",
            "displayName": "Alice",
            "description": "bio",
            "followersCount": 10,
            "followsCount": 20,
            "createdAt": "2024-01-01T00:00:00Z"
        })))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let profile = client
        .fetch_profile("did:plc:alice")
        .await
        .expect("profile");
    assert_eq!(profile.handle.as_deref(), Some("alice.bsky.social"));
    assert_eq!(profile.display_name.as_deref(), Some("Alice"));
    assert_eq!(profile.followers_count, Some(10));
    assert_eq!(profile.follows_count, Some(20));
}

#[tokio::test]
async fn test_fetch_post_extracts_text_and_author() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getPosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": [{
                "author": {"did": "did:plc:parentauthor"},
                "record": {"text": "parent post body"},
                "cid": "bafyparent"
            }]
        })))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let post = client
        .fetch_post("at://did:plc:parentauthor/app.bsky.feed.post/1")
        .await
        .expect("post");
    assert_eq!(post.author_did, "did:plc:parentauthor");
    assert_eq!(post.text, "parent post body");
    assert_eq!(post.cid.as_deref(), Some("bafyparent"));
}

#[tokio::test]
async fn test_fetch_follows_paginates_until_cursor_exhausted() {
    let server = MockServer::start().await;
    // Page 1 returns a cursor; page 2 has no cursor.
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.graph.getFollows"))
        .and(query_param("cursor", "next-page"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "follows": [{"did": "did:plc:second"}],
            "cursor": null
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.graph.getFollows"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "follows": [{"did": "did:plc:first"}],
            "cursor": "next-page"
        })))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let follows = client.fetch_follows("did:plc:alice", 100).await;
    assert_eq!(
        follows,
        vec!["did:plc:first".to_string(), "did:plc:second".to_string()]
    );
}

#[tokio::test]
async fn test_non_success_status_yields_none() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.actor.getProfile"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let client = client_for(&server);
    assert!(client.fetch_profile("did:plc:missing").await.is_none());
}

#[tokio::test]
async fn test_generic_get_returns_typed_error_on_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.example.bad"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    #[derive(serde::Deserialize)]
    struct Anything {
        _unused: Option<u8>,
    }

    let client = client_for(&server);
    assert!(client
        .get::<Anything>("com.example.bad", &[])
        .await
        .is_err());
}

#[tokio::test]
async fn test_thumbnail_url_construction() {
    let client = AppViewClient::with_endpoints("https://appview.example", "https://cdn.example");
    assert_eq!(client.appview_url(), "https://appview.example");
    assert_eq!(
        client.thumbnail_url("did:plc:alice", "bafycid"),
        "https://cdn.example/img/feed_thumbnail/plain/did:plc:alice/bafycid@jpeg"
    );
}
