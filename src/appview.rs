//! Read-only AppView XRPC client for Bluesky/ATProto public queries.
//!
//! Provides typed access to common AppView reads (actor profiles, posts, follow
//! graphs, repository records) plus a generic cursor paginator and a raw JSON
//! `get`. Every method degrades gracefully — returning `None`/empty on transport
//! or decode failure — so callers can enrich opportunistically without coupling
//! their pipelines to AppView availability.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::SkybaseError;

/// Default public Bluesky AppView endpoint for read-only XRPC resolution.
pub const DEFAULT_APPVIEW_ENDPOINT: &str = "https://public.api.bsky.app";

/// Default public Bluesky CDN endpoint for image thumbnail retrieval.
pub const DEFAULT_CDN_ENDPOINT: &str = "https://cdn.bsky.app";

/// Default timeout for AppView requests.
pub const DEFAULT_APPVIEW_TIMEOUT_MS: u64 = 1500;

/// Maximum number of pages followed by [`AppViewClient::paginate`].
pub const DEFAULT_MAX_PAGES: usize = 100;

/// Profile metadata returned by `app.bsky.actor.getProfile`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct ActorProfile {
    /// Author handle (e.g. `"alice.bsky.social"`).
    pub handle: Option<String>,
    /// User-defined display name.
    pub display_name: Option<String>,
    /// Profile description / bio text.
    pub description: Option<String>,
    /// Count of accounts following this author.
    pub followers_count: Option<u64>,
    /// Count of accounts followed by this author.
    pub follows_count: Option<u64>,
    /// Account registration timestamp in ISO 8601 format.
    pub created_at: Option<String>,
}

/// A resolved post view returned by `app.bsky.feed.getPosts`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct PostView {
    /// DID of the post author.
    pub author_did: String,
    /// Post text content, if present.
    pub text: String,
    /// Content identifier (CID) of the post record.
    pub cid: Option<String>,
}

/// Read-only AppView XRPC client.
#[derive(Debug, Clone)]
pub struct AppViewClient {
    appview_url: String,
    cdn_url: String,
    http_client: reqwest::Client,
}

impl Default for AppViewClient {
    fn default() -> Self {
        Self::new()
    }
}

impl AppViewClient {
    /// Creates a client targeting the default public AppView and CDN.
    #[must_use]
    pub fn new() -> Self {
        Self::with_endpoints(DEFAULT_APPVIEW_ENDPOINT, DEFAULT_CDN_ENDPOINT)
    }

    /// Creates a client with a custom AppView endpoint and the default CDN.
    #[must_use]
    pub fn with_endpoint(endpoint: impl Into<String>) -> Self {
        Self::with_endpoints(endpoint, DEFAULT_CDN_ENDPOINT)
    }

    /// Creates a client with custom AppView and CDN endpoints.
    #[must_use]
    pub fn with_endpoints(
        appview_endpoint: impl Into<String>,
        cdn_endpoint: impl Into<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_millis(DEFAULT_APPVIEW_TIMEOUT_MS))
            .build()
            .unwrap_or_else(|_| {
                reqwest::Client::builder()
                    .timeout(Duration::from_millis(DEFAULT_APPVIEW_TIMEOUT_MS))
                    .build()
                    .unwrap_or_default()
            });

        Self {
            appview_url: appview_endpoint.into().trim_end_matches('/').to_string(),
            cdn_url: cdn_endpoint.into().trim_end_matches('/').to_string(),
            http_client: client,
        }
    }

    /// Returns the configured AppView base URL.
    #[must_use]
    pub fn appview_url(&self) -> &str {
        &self.appview_url
    }

    /// Returns the configured CDN base URL.
    #[must_use]
    pub fn cdn_url(&self) -> &str {
        &self.cdn_url
    }

    /// Returns a reference to the underlying HTTP client.
    #[must_use]
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Performs a GET against an AppView XRPC endpoint and decodes the JSON response.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Network`] on transport failure or
    /// [`SkybaseError::Serialization`] on a non-success status or decode failure.
    pub async fn get<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<T, SkybaseError> {
        let url = format!("{}/xrpc/{endpoint}", self.appview_url);
        let resp = self.http_client.get(&url).query(query).send().await?;
        if !resp.status().is_success() {
            return Err(SkybaseError::Internal(format!(
                "AppView {endpoint} returned HTTP {}",
                resp.status()
            )));
        }
        let body = resp.json::<T>().await?;
        Ok(body)
    }

    /// Paginates an AppView XRPC endpoint, following cursors for at most
    /// [`DEFAULT_MAX_PAGES`] pages.
    ///
    /// `extract` projects each decoded response into a page of items plus the next
    /// cursor. Stops on a transport/decode error, an empty page, or a missing cursor.
    pub async fn paginate<T, I, F>(
        &self,
        endpoint: &str,
        base_query: &[(&str, String)],
        page_limit: u8,
        extract: F,
    ) -> Vec<I>
    where
        T: DeserializeOwned,
        F: Fn(T) -> (Vec<I>, Option<String>),
    {
        let page_limit = page_limit.clamp(1, 100).to_string();
        let mut items = Vec::new();
        let mut cursor: Option<String> = None;

        for _ in 0..DEFAULT_MAX_PAGES {
            let mut query: Vec<(&str, String)> = base_query.to_vec();
            query.push(("limit", page_limit.clone()));
            if let Some(ref c) = cursor {
                query.push(("cursor", c.clone()));
            }

            let body: T = match self.get(endpoint, &query).await {
                Ok(body) => body,
                Err(_) => break,
            };

            let (page, next_cursor) = extract(body);
            let count = page.len();
            items.extend(page);
            if count == 0 || next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }

        items
    }

    /// Resolves an ATProto handle to a DID via `com.atproto.identity.resolveHandle`.
    ///
    /// Returns `None` on any failure. A DID input is returned unchanged.
    pub async fn resolve_handle(&self, handle: &str) -> Option<String> {
        let clean = handle.trim().trim_start_matches('@');
        if clean.starts_with("did:") {
            return Some(clean.to_string());
        }

        #[derive(Deserialize)]
        struct ResolveHandleResponse {
            did: String,
        }

        self.get::<ResolveHandleResponse>(
            "com.atproto.identity.resolveHandle",
            &[("handle", clean.to_string())],
        )
        .await
        .ok()
        .map(|r| r.did)
    }

    /// Fetches an actor profile via `app.bsky.actor.getProfile`.
    ///
    /// Returns `None` on any failure.
    pub async fn fetch_profile(&self, did: &str) -> Option<ActorProfile> {
        #[derive(Deserialize)]
        struct RawProfile {
            handle: Option<String>,
            #[serde(rename = "displayName")]
            display_name: Option<String>,
            description: Option<String>,
            #[serde(rename = "followersCount")]
            followers_count: Option<u64>,
            #[serde(rename = "followsCount")]
            follows_count: Option<u64>,
            #[serde(rename = "createdAt")]
            created_at: Option<String>,
        }

        let raw: RawProfile = self
            .get("app.bsky.actor.getProfile", &[("actor", did.to_string())])
            .await
            .ok()?;
        Some(ActorProfile {
            handle: raw.handle,
            display_name: raw.display_name,
            description: raw.description,
            followers_count: raw.followers_count,
            follows_count: raw.follows_count,
            created_at: raw.created_at,
        })
    }

    /// Fetches a single post view via `app.bsky.feed.getPosts`.
    ///
    /// Returns `None` on any failure or an empty response.
    pub async fn fetch_post(&self, uri: &str) -> Option<PostView> {
        #[derive(Deserialize)]
        struct RawRecord {
            text: Option<String>,
        }
        #[derive(Deserialize)]
        struct RawAuthor {
            did: String,
        }
        #[derive(Deserialize)]
        struct RawPostView {
            author: RawAuthor,
            record: serde_json::Value,
            cid: Option<String>,
        }
        #[derive(Deserialize)]
        struct RawPostsResponse {
            posts: Vec<RawPostView>,
        }

        let raw: RawPostsResponse = self
            .get("app.bsky.feed.getPosts", &[("uris", uri.to_string())])
            .await
            .ok()?;
        let first = raw.posts.into_iter().next()?;
        let text = serde_json::from_value::<RawRecord>(first.record)
            .ok()
            .and_then(|r| r.text)
            .unwrap_or_default();
        Some(PostView {
            author_did: first.author.did,
            text,
            cid: first.cid,
        })
    }

    /// Fetches followed DIDs for an actor via `app.bsky.graph.getFollows`.
    pub async fn fetch_follows(&self, actor: &str, limit: u8) -> Vec<String> {
        #[derive(Deserialize)]
        struct FollowProfile {
            did: String,
        }
        #[derive(Deserialize)]
        struct GetFollowsResponse {
            follows: Vec<FollowProfile>,
            cursor: Option<String>,
        }

        self.paginate::<GetFollowsResponse, _, _>(
            "app.bsky.graph.getFollows",
            &[("actor", actor.to_string())],
            limit,
            |body| {
                (
                    body.follows.into_iter().map(|f| f.did).collect(),
                    body.cursor,
                )
            },
        )
        .await
    }

    /// Fetches incoming follower DIDs for an actor via `app.bsky.graph.getFollowers`.
    pub async fn fetch_followers(&self, actor: &str, limit: u8) -> Vec<String> {
        #[derive(Deserialize)]
        struct FollowerProfile {
            did: String,
        }
        #[derive(Deserialize)]
        struct GetFollowersResponse {
            followers: Vec<FollowerProfile>,
            cursor: Option<String>,
        }

        self.paginate::<GetFollowersResponse, _, _>(
            "app.bsky.graph.getFollowers",
            &[("actor", actor.to_string())],
            limit,
            |body| {
                (
                    body.followers.into_iter().map(|f| f.did).collect(),
                    body.cursor,
                )
            },
        )
        .await
    }

    /// Fetches follow records `(rkey, followed_did)` for an actor via
    /// `com.atproto.repo.listRecords` on `app.bsky.graph.follow`.
    pub async fn fetch_follow_records(&self, actor: &str, limit: u8) -> Vec<(String, String)> {
        #[derive(Deserialize)]
        struct FollowValue {
            subject: Option<String>,
        }
        #[derive(Deserialize)]
        struct RecordItem {
            uri: String,
            value: FollowValue,
        }
        #[derive(Deserialize)]
        struct ListRecordsResp {
            records: Vec<RecordItem>,
            cursor: Option<String>,
        }

        self.paginate::<ListRecordsResp, _, _>(
            "com.atproto.repo.listRecords",
            &[
                ("repo", actor.to_string()),
                ("collection", "app.bsky.graph.follow".to_string()),
            ],
            limit,
            |body| {
                let mut items = Vec::new();
                for rec in body.records {
                    if let Some(subject) = rec.value.subject {
                        let rkey = rec.uri.rsplit('/').next().unwrap_or_default().to_string();
                        if !rkey.is_empty() && !subject.is_empty() {
                            items.push((rkey, subject));
                        }
                    }
                }
                (items, body.cursor)
            },
        )
        .await
    }

    /// Builds a CDN thumbnail URL for an author DID and image CID.
    #[must_use]
    pub fn thumbnail_url(&self, author_did: &str, cid: &str) -> String {
        format!(
            "{}/img/feed_thumbnail/plain/{author_did}/{cid}@jpeg",
            self.cdn_url
        )
    }
}
