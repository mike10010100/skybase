//! Sovereign PDS write client executing DPoP-signed mutations against an ATProto PDS.

use reqwest::{Client, Method};
use serde::Serialize;
use std::sync::Arc;
use url::Url;

use skyauth::client::AtprotoOAuthClient;
use skyauth::dpop::{extract_dpop_nonce, normalize_htu, DPoPNonceCache};
use skyauth::session::OAuthSession;

use crate::error::{Result, SkybaseError};
use crate::repo::types::{
    validate_rkey, CreateRecordRequest, CreateRecordResult, DeleteRecordRequest, XrpcErrorResponse,
};

/// Maximum bounded body size for error inspection (64 KB).
const MAX_ERROR_BODY_BYTES: usize = 65_536;

/// Maximum bounded body size for successful responses (1 MB).
const MAX_SUCCESS_BODY_BYTES: usize = 1_048_576;

/// Sovereign PDS repository write client.
///
/// Manages RFC 9449 DPoP authentication, cryptographic proof generation,
/// and transparent single-retry challenge recovery for ATProto record mutations.
#[derive(Clone)]
pub struct PdsRepoClient {
    session: Arc<OAuthSession>,
    oauth_client: Option<Arc<AtprotoOAuthClient>>,
    http_client: Client,
    nonce_cache: DPoPNonceCache,
    endpoint_override: Option<String>,
}

impl PdsRepoClient {
    /// Fallible constructor creating a `PdsRepoClient` with strict redirect policies.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Network`] if the underlying HTTP client cannot be built.
    pub fn try_new(session: Arc<OAuthSession>, client: Arc<AtprotoOAuthClient>) -> Result<Self> {
        let nonce_cache = client.nonce_cache().clone();
        let http_client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(SkybaseError::Network)?;

        Ok(Self {
            session,
            oauth_client: Some(client),
            http_client,
            nonce_cache,
            endpoint_override: None,
        })
    }

    /// Fallible constructor directly from an [`OAuthSession`] with a fresh [`DPoPNonceCache`].
    ///
    /// # Errors
    /// Returns [`SkybaseError::Network`] if the underlying HTTP client cannot be built.
    pub fn try_from_session(session: Arc<OAuthSession>) -> Result<Self> {
        let http_client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(SkybaseError::Network)?;

        Ok(Self {
            session,
            oauth_client: None,
            http_client,
            nonce_cache: DPoPNonceCache::new(),
            endpoint_override: None,
        })
    }

    /// Creates a new `PdsRepoClient` wrapping an authenticated [`OAuthSession`] and [`AtprotoOAuthClient`].
    ///
    /// Borrows and shares the [`DPoPNonceCache`] from the OAuth client.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Network`] if HTTP client creation fails.
    pub fn new(session: Arc<OAuthSession>, client: Arc<AtprotoOAuthClient>) -> Result<Self> {
        Self::try_new(session, client)
    }

    /// Creates a `PdsRepoClient` directly from an [`OAuthSession`] with a fresh [`DPoPNonceCache`].
    ///
    /// # Errors
    /// Returns [`SkybaseError::Network`] if HTTP client creation fails.
    pub fn from_session(session: Arc<OAuthSession>) -> Result<Self> {
        Self::try_from_session(session)
    }

    /// Convenience constructor creating an internal [`OAuthSession`] for manual credentials or testing.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Auth`] if session initialization fails, or [`SkybaseError::Network`]
    /// if HTTP client creation fails.
    pub fn from_credentials(
        pds_endpoint: impl Into<String>,
        repo_did: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Result<Self> {
        let endpoint = pds_endpoint.into();
        let did = repo_did.into();
        let token = access_token.into();

        let session = OAuthSession::new(
            did,
            token,
            None,
            "DPoP",
            None,
            None,
            skyauth::dpop::DPoPKey::generate(),
            Some(endpoint),
            None,
            None,
        )
        .map_err(SkybaseError::Auth)?;

        Self::try_from_session(Arc::new(session))
    }

    /// Overrides the PDS endpoint destination URL.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_override = Some(endpoint.into());
        self
    }

    /// Configures a custom [`reqwest::Client`].
    #[must_use]
    pub fn with_http_client(mut self, client: Client) -> Self {
        self.http_client = client;
        self
    }

    /// Configures a custom [`DPoPNonceCache`].
    #[must_use]
    pub fn with_nonce_cache(mut self, nonce_cache: DPoPNonceCache) -> Self {
        self.nonce_cache = nonce_cache;
        self
    }

    /// Returns a reference to the bound [`OAuthSession`].
    #[must_use]
    pub fn session(&self) -> &Arc<OAuthSession> {
        &self.session
    }

    /// Returns the subject DID of the repository owner.
    #[must_use]
    pub fn did(&self) -> &str {
        self.session.sub()
    }

    /// Resolves the active PDS endpoint URL string.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Repo`] if neither an override nor session PDS endpoint is present.
    pub fn pds_endpoint(&self) -> Result<&str> {
        if let Some(ref ep) = self.endpoint_override {
            return Ok(ep.as_str());
        }
        self.session.pds_endpoint().ok_or_else(|| {
            SkybaseError::Repo("No PDS endpoint found in OAuthSession or client".into())
        })
    }

    /// Returns a reference to the bound [`AtprotoOAuthClient`], if present.
    #[must_use]
    pub fn oauth_client(&self) -> Option<&Arc<AtprotoOAuthClient>> {
        self.oauth_client.as_ref()
    }

    /// Returns a reference to the internal [`DPoPNonceCache`].
    #[must_use]
    pub fn nonce_cache(&self) -> &DPoPNonceCache {
        &self.nonce_cache
    }

    /// Returns a reference to the underlying [`reqwest::Client`].
    #[must_use]
    pub fn http_client(&self) -> &Client {
        &self.http_client
    }

    /// Creates a record in the sovereign PDS repository via `com.atproto.repo.createRecord`.
    ///
    /// # Arguments
    /// - `collection`: The collection NSID (e.g. `"app.bsky.feed.post"`).
    /// - `rkey`: Optional record key. If `None`, the PDS assigns a server-generated TID.
    /// - `record`: The serializable record payload.
    /// - `validate`: Whether the PDS should validate the record against its Lexicon schema.
    ///
    /// # Returns
    /// A [`CreateRecordResult`] containing the canonical `uri` and `cid`.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Repo`] if validation fails or server rejects the request,
    /// [`SkybaseError::Network`] on transport errors, or [`SkybaseError::Serialization`] on JSON errors.
    pub async fn create_record<T: Serialize>(
        &self,
        collection: &str,
        rkey: Option<&str>,
        record: &T,
        validate: bool,
    ) -> Result<CreateRecordResult> {
        if let Some(rk) = rkey {
            validate_rkey(rk)?;
        }

        let did = self.did();
        let payload = CreateRecordRequest {
            repo: did,
            collection,
            rkey,
            validate,
            record,
            swap_commit: None,
        };

        let body_bytes = serde_json::to_vec(&payload).map_err(SkybaseError::Serialization)?;
        let url = self.build_xrpc_url("com.atproto.repo.createRecord")?;

        let resp = self
            .send_with_nonce_retry(Method::POST, &url, Some(body_bytes))
            .await?;

        let resp_bytes = read_bounded_bytes(resp, MAX_SUCCESS_BODY_BYTES).await?;

        let res_json: serde_json::Value =
            serde_json::from_slice(&resp_bytes).map_err(SkybaseError::Serialization)?;

        let uri = res_json
            .get("uri")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                SkybaseError::Repo("Missing or empty 'uri' in createRecord response".into())
            })?
            .to_string();

        let cid = res_json
            .get("cid")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                SkybaseError::Repo("Missing or empty 'cid' in createRecord response".into())
            })?
            .to_string();

        Ok(CreateRecordResult { uri, cid })
    }

    /// Deletes a record from the sovereign PDS repository via `com.atproto.repo.deleteRecord`.
    ///
    /// # Arguments
    /// - `collection`: The collection NSID (e.g. `"app.bsky.feed.post"`).
    /// - `rkey`: The record key to delete.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Repo`] if `rkey` is invalid or server rejects deletion,
    /// [`SkybaseError::Network`] on transport failure.
    pub async fn delete_record(&self, collection: &str, rkey: &str) -> Result<()> {
        validate_rkey(rkey)?;

        let did = self.did();
        let payload = DeleteRecordRequest {
            repo: did,
            collection,
            rkey,
            swap_record: None,
            swap_commit: None,
        };

        let body_bytes = serde_json::to_vec(&payload).map_err(SkybaseError::Serialization)?;
        let url = self.build_xrpc_url("com.atproto.repo.deleteRecord")?;

        let _resp = self
            .send_with_nonce_retry(Method::POST, &url, Some(body_bytes))
            .await?;

        Ok(())
    }

    /// Constructs the full target XRPC URL for a given NSID method.
    fn build_xrpc_url(&self, nsid: &str) -> Result<String> {
        let endpoint = self.pds_endpoint()?;
        let trimmed_endpoint = endpoint.trim_end_matches('/');
        let mut url = Url::parse(trimmed_endpoint).map_err(|e| {
            SkybaseError::Repo(format!(
                "Invalid PDS endpoint URL '{trimmed_endpoint}': {e}"
            ))
        })?;

        let base_path = url.path().trim_end_matches('/');
        let full_path = if base_path.is_empty() {
            format!("/xrpc/{nsid}")
        } else {
            format!("{base_path}/xrpc/{nsid}")
        };
        url.set_path(&full_path);

        Ok(url.to_string())
    }

    /// Executes an HTTP request against a PDS endpoint with DPoP signing and transparent
    /// single-retry challenge recovery.
    ///
    /// # Safety & Invariants
    /// - Max 1 retry: Exactly two attempts maximum (`attempts <= 2`).
    /// - Nonce caching: Cached nonces are refreshed on any response carrying `DPoP-Nonce`.
    /// - Lock safety: No locks are held across `.await` points.
    /// - Bounded reads: Error bodies are bounded to 64 KB.
    async fn send_with_nonce_retry(
        &self,
        method: Method,
        url_str: &str,
        body_bytes: Option<Vec<u8>>,
    ) -> Result<reqwest::Response> {
        if self.session.is_expired() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let exp = self
                .session
                .expires_at()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(now, |d| d.as_secs());
            return Err(SkybaseError::Auth(
                skyauth::error::TokenError::Expired { exp, now }.into(),
            ));
        }

        let parsed_url = Url::parse(url_str)
            .map_err(|e| SkybaseError::Repo(format!("Invalid request URL '{url_str}': {e}")))?;
        let server_origin = parsed_url.origin().ascii_serialization();
        let htu = normalize_htu(url_str).map_err(SkybaseError::from)?;

        let mut attempts: u32 = 0;

        loop {
            attempts = attempts.saturating_add(1);

            // 1. Retrieve current cached nonce for origin
            let current_nonce = self.nonce_cache.get_nonce(&server_origin);

            // 2. Generate signed DPoP proof bound to session's access token hash (ath)
            let dpop_proof = self
                .session
                .create_dpop_proof(method.as_str(), &htu, current_nonce.as_deref())
                .map_err(SkybaseError::from)?;

            // 3. Build HTTP request
            let mut req = self
                .http_client
                .request(method.clone(), parsed_url.clone())
                .header("Authorization", self.session.dpop_auth_header())
                .header("DPoP", dpop_proof)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");

            if let Some(ref bytes) = body_bytes {
                req = req.body(bytes.clone());
            }

            // 4. Dispatch request
            let resp = req.send().await.map_err(SkybaseError::Network)?;

            // 5. Opportunistically record fresh nonce from response headers (if present)
            let new_nonce = resp
                .headers()
                .get("DPoP-Nonce")
                .or_else(|| resp.headers().get("dpop-nonce"))
                .and_then(|h| h.to_str().ok())
                .and_then(|val| extract_dpop_nonce(Some(val)));

            if let Some(ref nonce_val) = new_nonce {
                self.nonce_cache
                    .set_nonce(&server_origin, nonce_val.clone());
            }

            let status = resp.status();

            // 6. Fast-path: Successful response (2xx)
            if status.is_success() {
                return Ok(resp);
            }

            // 7. Nonce Challenge Detection (HTTP 401 or HTTP 400)
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::BAD_REQUEST
            {
                let headers = resp.headers().clone();
                let is_challenge_header = is_rs_dpop_nonce_challenge(&headers);

                // If header doesn't explicitly confirm, buffer body to check JSON error field
                let (is_challenge, err_bytes) = if is_challenge_header {
                    (true, Vec::new())
                } else {
                    let err_bytes = read_error_bytes_bounded(resp, MAX_ERROR_BODY_BYTES).await;
                    let json_val: Option<serde_json::Value> =
                        serde_json::from_slice(&err_bytes).ok();
                    let is_error_field = is_use_dpop_nonce_error(json_val.as_ref());
                    (is_error_field, err_bytes)
                };

                if is_challenge {
                    if attempts >= 2 {
                        return Err(SkybaseError::Repo(
                            "DPoP nonce challenge retry limit exceeded (max 1 retry)".into(),
                        ));
                    }

                    // Verify we have a fresh nonce for the retry
                    let fresh_nonce = self.nonce_cache.get_nonce(&server_origin);
                    if fresh_nonce.is_none() {
                        return Err(SkybaseError::Repo(
                            "PDS issued use_dpop_nonce challenge but omitted DPoP-Nonce header"
                                .into(),
                        ));
                    }

                    tracing::debug!(
                        origin = %server_origin,
                        attempt = attempts,
                        "Received DPoP nonce challenge; retrying with fresh nonce"
                    );
                    continue;
                }

                // Not a DPoP nonce challenge
                let err_msg = format_xrpc_error(status, &err_bytes);
                return Err(SkybaseError::Repo(err_msg));
            }

            // 8. Other HTTP failure statuses (403, 404, 500, 502, etc.)
            let err_bytes = read_error_bytes_bounded(resp, MAX_ERROR_BODY_BYTES).await;
            let err_msg = format_xrpc_error(status, &err_bytes);
            return Err(SkybaseError::Repo(err_msg));
        }
    }
}

/// Checks whether a Resource Server response signals an explicit `use_dpop_nonce`
/// challenge via `WWW-Authenticate: DPoP error="use_dpop_nonce"` (RFC 9449 § 8.4).
fn is_rs_dpop_nonce_challenge(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get_all(reqwest::header::WWW_AUTHENTICATE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|challenge| {
            let challenge = challenge.trim();
            let Some(space) = challenge.find(' ') else {
                return false;
            };
            let (scheme, rest) = challenge.split_at(space);
            if !scheme.eq_ignore_ascii_case("DPoP") {
                return false;
            }
            let rest = rest.trim_start();
            rest.split(',').any(|param| {
                let param = param.trim();
                let Some((key, value)) = param.split_once('=') else {
                    return false;
                };
                if !key.trim().eq_ignore_ascii_case("error") {
                    return false;
                }
                let value = value.trim().trim_matches('"');
                value.eq_ignore_ascii_case("use_dpop_nonce")
            })
        })
}

/// Checks whether a parsed JSON error body contains `"error": "use_dpop_nonce"`.
fn is_use_dpop_nonce_error(json: Option<&serde_json::Value>) -> bool {
    json.and_then(|j| j.get("error"))
        .and_then(|e| e.as_str())
        .is_some_and(|err| err.eq_ignore_ascii_case("use_dpop_nonce"))
}

/// Reads an HTTP response body incrementally chunk-by-chunk up to `max_bytes`.
///
/// # Errors
/// Returns [`SkybaseError::Repo`] if the payload exceeds `max_bytes`, or
/// [`SkybaseError::Network`] on transport failure.
async fn read_bounded_bytes(mut resp: reqwest::Response, max_bytes: usize) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(SkybaseError::Network)? {
        if buffer.len().saturating_add(chunk.len()) > max_bytes {
            return Err(SkybaseError::Repo(format!(
                "HTTP response body exceeded maximum limit of {max_bytes} bytes"
            )));
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(buffer)
}

/// Reads an HTTP error response body incrementally chunk-by-chunk up to `max_bytes` without failing on truncation.
async fn read_error_bytes_bounded(mut resp: reqwest::Response, max_bytes: usize) -> Vec<u8> {
    let mut buffer = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        if buffer.len().saturating_add(chunk.len()) > max_bytes {
            let remaining = max_bytes.saturating_sub(buffer.len());
            buffer.extend_from_slice(&chunk[..remaining]);
            break;
        }
        buffer.extend_from_slice(&chunk);
    }
    buffer
}

/// Formats an XRPC or HTTP error payload into a readable error message.
fn format_xrpc_error(status: reqwest::StatusCode, bytes: &[u8]) -> String {
    if let Ok(xrpc_err) = serde_json::from_slice::<XrpcErrorResponse>(bytes) {
        let code = xrpc_err.error.unwrap_or_else(|| status.to_string());
        let msg = xrpc_err
            .message
            .unwrap_or_else(|| "Unknown error".to_string());
        format!("PDS request failed with status {status}: {code} - {msg}")
    } else {
        let text = String::from_utf8_lossy(bytes);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            format!("HTTP request failed with status: {status}")
        } else {
            format!("HTTP request failed with status {status}: {trimmed}")
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue, WWW_AUTHENTICATE};
    use serde_json::json;

    #[test]
    fn test_client_from_credentials() {
        let client = PdsRepoClient::from_credentials(
            "https://pds.example.com",
            "did:plc:alice",
            "access_token_123",
        )
        .expect("client creation from credentials should succeed");

        assert_eq!(client.did(), "did:plc:alice");
        assert_eq!(
            client.pds_endpoint().expect("endpoint should be present"),
            "https://pds.example.com"
        );
        assert_eq!(client.session().dpop_auth_header(), "DPoP access_token_123");
    }

    #[test]
    fn test_client_missing_endpoint_fails_closed() {
        let session = OAuthSession::new(
            "did:plc:alice",
            "token",
            None,
            "DPoP",
            None,
            Some(3600),
            skyauth::dpop::DPoPKey::generate(),
            None, // No endpoint
            None,
            None,
        )
        .expect("session creation failed");

        let client = PdsRepoClient::from_session(Arc::new(session)).expect("client creation");
        assert!(client.pds_endpoint().is_err());
    }

    #[test]
    fn test_client_endpoint_override() {
        let client =
            PdsRepoClient::from_credentials("https://pds.example.com", "did:plc:alice", "token")
                .expect("client creation failed")
                .with_endpoint("https://custom-pds.example.com");

        assert_eq!(
            client.pds_endpoint().expect("overridden endpoint"),
            "https://custom-pds.example.com"
        );
    }

    #[test]
    fn test_is_rs_dpop_nonce_challenge_detection() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static(
                "DPoP error=\"use_dpop_nonce\", error_description=\"Nonce expired\"",
            ),
        );
        assert!(is_rs_dpop_nonce_challenge(&headers));

        let mut headers_other = HeaderMap::new();
        headers_other.insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer error=\"invalid_token\""),
        );
        assert!(!is_rs_dpop_nonce_challenge(&headers_other));

        let headers_empty = HeaderMap::new();
        assert!(!is_rs_dpop_nonce_challenge(&headers_empty));
    }

    #[test]
    fn test_is_use_dpop_nonce_error_detection() {
        let json_nonce = json!({
            "error": "use_dpop_nonce",
            "message": "DPoP proof requires nonce"
        });
        assert!(is_use_dpop_nonce_error(Some(&json_nonce)));

        let json_invalid = json!({
            "error": "InvalidRequest",
            "message": "Missing field"
        });
        assert!(!is_use_dpop_nonce_error(Some(&json_invalid)));

        assert!(!is_use_dpop_nonce_error(None));
    }

    #[tokio::test]
    async fn test_client_create_record_network_roundtrip() {
        use wiremock::matchers::{header_exists, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .and(header_exists("authorization"))
            .and(header_exists("dpop"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "uri": "at://did:plc:test_user/app.bsky.feed.post/post_1",
                "cid": "bafyrei_test_cid_1"
            })))
            .mount(&server)
            .await;

        let client =
            PdsRepoClient::from_credentials(server.uri(), "did:plc:test_user", "test_access_token")
                .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("post_1"),
                &json!({ "text": "Hello, world!" }),
                true,
            )
            .await
            .expect("create_record should succeed");

        assert_eq!(res.uri, "at://did:plc:test_user/app.bsky.feed.post/post_1");
        assert_eq!(res.cid, "bafyrei_test_cid_1");
    }

    #[tokio::test]
    async fn test_client_delete_record_network_roundtrip() {
        use wiremock::matchers::{header_exists, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.deleteRecord"))
            .and(header_exists("authorization"))
            .and(header_exists("dpop"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let client =
            PdsRepoClient::from_credentials(server.uri(), "did:plc:test_user", "test_access_token")
                .expect("client creation failed");

        let res = client
            .delete_record("app.bsky.feed.post", "post_to_delete")
            .await;
        assert!(res.is_ok(), "delete_record should succeed");
    }

    #[tokio::test]
    async fn test_client_dpop_nonce_retry_roundtrip() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Mount 1-time 401 challenge
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", "fresh_nonce_123")
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "DPoP proof requires nonce"
                    })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Default 200 response
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "uri": "at://did:plc:alice/app.bsky.feed.post/retried_post",
                "cid": "bafyrei_retried_cid"
            })))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("retried_post"),
                &json!({ "text": "Retried successfully" }),
                true,
            )
            .await
            .expect("should transparently succeed on nonce retry");

        assert_eq!(
            res.uri,
            "at://did:plc:alice/app.bsky.feed.post/retried_post"
        );
        assert_eq!(res.cid, "bafyrei_retried_cid");

        // Verify that the fresh nonce is now cached
        let origin = url::Url::parse(&server.uri())
            .unwrap()
            .origin()
            .ascii_serialization();
        assert_eq!(
            client.nonce_cache().get_nonce(&origin).as_deref(),
            Some("fresh_nonce_123")
        );
    }

    #[tokio::test]
    async fn test_client_dpop_nonce_retry_limit_exceeded() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Always challenge with 401 use_dpop_nonce
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", "infinite_nonce")
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "Always challenging"
                    })),
            )
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("p1"),
                &json!({ "text": "Infinite loop test" }),
                true,
            )
            .await;

        assert!(res.is_err());
        let err_str = res.unwrap_err().to_string();
        assert!(
            err_str.contains("retry limit exceeded"),
            "Error must indicate retry limit exceeded, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_client_dpop_nonce_missing_header_fails() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Challenge with use_dpop_nonce but omit DPoP-Nonce header
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": "use_dpop_nonce",
                "message": "Requires nonce but forgot header"
            })))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("p1"),
                &json!({ "text": "Missing header test" }),
                true,
            )
            .await;

        assert!(res.is_err());
        let err_str = res.unwrap_err().to_string();
        assert!(
            err_str.contains("omitted DPoP-Nonce header"),
            "Error must indicate missing nonce header, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_client_non_challenge_401_fails_immediately() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": "invalid_token",
                "message": "Token expired or revoked"
            })))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record(
                "app.bsky.feed.post",
                Some("p1"),
                &json!({ "text": "Non-challenge test" }),
                true,
            )
            .await;

        assert!(res.is_err());
        let err_str = res.unwrap_err().to_string();
        assert!(
            err_str.contains("invalid_token"),
            "Error must contain invalid_token, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_client_xrpc_error_response_formatting() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "InvalidRequest",
                "message": "Schema validation failed: missing field 'text'"
            })))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record("app.bsky.feed.post", Some("p1"), &json!({}), true)
            .await;

        assert!(res.is_err());
        let err_str = res.unwrap_err().to_string();
        assert!(
            err_str.contains("InvalidRequest - Schema validation failed"),
            "Error must format XRPC error code and message, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_client_invalid_rkey_preflight_rejection() {
        let client = PdsRepoClient::from_credentials(
            "https://pds.example.com",
            "did:plc:alice",
            "token_xyz",
        )
        .expect("client creation failed");

        let create_res = client
            .create_record(
                "app.bsky.feed.post",
                Some("invalid..key"),
                &json!({ "text": "bad rkey" }),
                true,
            )
            .await;
        assert!(create_res.is_err());

        let delete_res = client
            .delete_record("app.bsky.feed.post", "invalid/slash")
            .await;
        assert!(delete_res.is_err());
    }

    #[tokio::test]
    async fn test_client_create_record_rejects_empty_uri_or_cid() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "uri": "   ",
                "cid": "bafyrei_valid_cid"
            })))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record("app.bsky.feed.post", None, &json!({ "text": "hi" }), true)
            .await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Missing or empty 'uri'"));
    }

    #[tokio::test]
    async fn test_client_create_record_rejects_oversized_response_body() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Construct a response body > 1MB (MAX_SUCCESS_BODY_BYTES)
        let huge_text = "x".repeat(MAX_SUCCESS_BODY_BYTES + 1024);
        let huge_body = format!(
            r#"{{"uri":"at://did:plc:alice/app.bsky.feed.post/1","cid":"bafy","extra":"{huge_text}"}}"#
        );

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(huge_body, "application/json"))
            .mount(&server)
            .await;

        let client = PdsRepoClient::from_credentials(server.uri(), "did:plc:alice", "token_xyz")
            .expect("client creation failed");

        let res = client
            .create_record("app.bsky.feed.post", None, &json!({ "text": "hi" }), true)
            .await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("exceeded maximum limit"));
    }

    #[test]
    fn test_client_try_constructors() {
        let session = Arc::new(
            OAuthSession::new(
                "did:plc:alice",
                "token",
                None,
                "DPoP",
                None,
                Some(3600),
                skyauth::dpop::DPoPKey::generate(),
                Some("https://pds.example.com".into()),
                None,
                None,
            )
            .expect("session creation failed"),
        );

        let client = PdsRepoClient::try_from_session(session)
            .expect("try_from_session should succeed with valid parameters");
        assert_eq!(client.did(), "did:plc:alice");
    }
}
