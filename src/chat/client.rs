//! Asynchronous HTTP client for ATProto Chat (`chat.bsky.convo.*`).
//!
//! Provides authenticated XRPC access to the Bluesky chat service, including
//! App-Password login, automatic single-flight session refresh/re-login, and
//! the conversation listing, request acceptance, message send/read, and
//! conversation-lookup operations used by DM bots.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::chat::types::{
    AcceptConvoRequest, AcceptConvoResponse, ConvoView, GetMessagesResponse,
    ListConvoRequestsResponse, ListConvosResponse, MessageView, SendMessagePayload,
    SendMessageRequest, UpdateReadRequest,
};
use crate::error::SkybaseError;

/// Default public Bluesky chat service endpoint.
pub const DEFAULT_CHAT_ENDPOINT: &str = "https://api.bsky.chat";

/// Service DID for Bluesky chat backend proxying through PDS.
pub const ATPROTO_CHAT_PROXY_DID: &str = "did:web:api.bsky.chat#bsky_chat";

#[derive(Debug, Deserialize)]
struct CreateSessionResponse {
    did: String,
    #[serde(rename = "accessJwt")]
    access_jwt: String,
    #[serde(rename = "refreshJwt", default)]
    refresh_jwt: Option<String>,
    #[serde(rename = "didDoc", default)]
    did_doc: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct RefreshSessionResponse {
    #[serde(rename = "accessJwt")]
    access_jwt: String,
    #[serde(rename = "refreshJwt", default)]
    refresh_jwt: Option<String>,
    #[serde(rename = "didDoc", default)]
    did_doc: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct BotCredentials {
    pds_endpoint: String,
    identifier: String,
    password: String,
}

#[derive(Debug, Clone)]
struct AuthState {
    base_url: String,
    allow_dynamic_pds_updates: bool,
    access_jwt: String,
    refresh_jwt: Option<String>,
    credentials: Option<BotCredentials>,
}

/// Returns `true` when `url_str` points directly at the Bluesky chat backend (`api.bsky.chat`)
/// rather than a Personal Data Server gateway.
fn is_direct_chat_endpoint(url_str: &str) -> bool {
    url::Url::parse(url_str).map_or_else(
        |_| url_str.contains("api.bsky.chat"),
        |u| {
            u.host_str()
                .is_some_and(|h| h.eq_ignore_ascii_case("api.bsky.chat"))
        },
    )
}

/// Returns `true` when `url_str` points at the public Bluesky entryway (`bsky.social`),
/// which authenticates sessions (`createSession` / `refreshSession`) but does not proxy
/// `chat.bsky.convo.*` XRPC calls.
fn is_bsky_entryway(url_str: &str) -> bool {
    url::Url::parse(url_str)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.eq_ignore_ascii_case("bsky.social")))
        .unwrap_or(false)
}

/// Extracts a validated `#atproto_pds` (`AtprotoPersonalDataServer`) endpoint URL from an
/// untrusted `didDoc` JSON payload returned by `createSession` or `refreshSession`.
fn extract_pds_endpoint_from_did_doc(did_doc: &serde_json::Value) -> Option<String> {
    if let Ok(parsed_doc) =
        serde_json::from_value::<skyauth::identity::DidDocument>(did_doc.clone())
    {
        if let Ok(endpoint) = parsed_doc.extract_pds_endpoint() {
            return Some(endpoint);
        }
    }

    let services = did_doc.get("service")?.as_array()?;
    for svc in services {
        let id = svc
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let svc_type = svc
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let is_pds = (id == "#atproto_pds" || id.ends_with("#atproto_pds"))
            && (svc_type.is_empty() || svc_type == "AtprotoPersonalDataServer");
        if !is_pds {
            continue;
        }
        let Some(raw_endpoint) = svc
            .get("serviceEndpoint")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let trimmed = raw_endpoint.trim().trim_end_matches('/');
        if let Ok(parsed) = url::Url::parse(trimmed) {
            if (parsed.scheme() == "https" || parsed.scheme() == "http")
                && parsed.host_str().is_some()
            {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

/// Resolves the XRPC base URL to use for an App-Password (`at+jwt`) chat session.
///
/// PDS user session tokens (`typ: "at+jwt"`) cannot be presented directly to
/// `https://api.bsky.chat` (`401 BadJwtType`) and cannot be proxied through the
/// `https://bsky.social` entryway (`501 MethodNotImplemented`); they must be routed through
/// the account's authoritative PDS (`#atproto_pds` in `didDoc`) with the
/// `atproto-proxy: did:web:api.bsky.chat#bsky_chat` header.
///
/// Returns `(resolved_base_url, allow_dynamic_pds_updates)`.
fn resolve_session_chat_base_url(
    pds_endpoint: &str,
    chat_endpoint: &str,
    did_doc: Option<&serde_json::Value>,
) -> (String, bool) {
    let pds_trimmed = pds_endpoint.trim().trim_end_matches('/');
    let chat_trimmed = chat_endpoint.trim().trim_end_matches('/');

    let has_custom_chat_override = !chat_trimmed.is_empty()
        && !is_direct_chat_endpoint(chat_trimmed)
        && !is_bsky_entryway(chat_trimmed)
        && chat_trimmed != pds_trimmed;

    if has_custom_chat_override {
        return (chat_trimmed.to_string(), false);
    }

    if let Some(discovered_pds) = did_doc.and_then(extract_pds_endpoint_from_did_doc) {
        return (discovered_pds, true);
    }

    if (chat_trimmed.is_empty() || is_direct_chat_endpoint(chat_trimmed))
        && !pds_trimmed.is_empty()
        && !is_bsky_entryway(pds_trimmed)
    {
        return (pds_trimmed.to_string(), true);
    }

    if !chat_trimmed.is_empty() {
        (chat_trimmed.to_string(), true)
    } else {
        (pds_trimmed.to_string(), true)
    }
}

/// Returns `true` when an XRPC failure represents a recoverable expired/unauthorized session
/// token rather than a permanent token-type or scope mismatch (`BadJwtType`, `BadTokenScope`).
fn is_recoverable_session_expiry(status: reqwest::StatusCode, body_text: &str) -> bool {
    if body_text.contains("BadJwtType") || body_text.contains("BadTokenScope") {
        return false;
    }
    status == reqwest::StatusCode::UNAUTHORIZED
        || (status == reqwest::StatusCode::BAD_REQUEST && body_text.contains("ExpiredToken"))
}

/// Asynchronous HTTP client for interacting with the ATProto Chat service.
#[derive(Clone)]
pub struct ChatClient {
    client: reqwest::Client,
    auth_state: Arc<RwLock<AuthState>>,
}

impl ChatClient {
    /// Creates a new [`ChatClient`] targeting the specified base URL and bearer token.
    ///
    /// If `base_url` is a PDS rather than `api.bsky.chat`, the required `atproto-proxy`
    /// header is automatically attached to route chat XRPC requests through the gateway.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use skybase::chat::ChatClient;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ChatClient::new("https://pds.example.com/", "at-jwt-token")?;
    /// assert_eq!(client.base_url().await, "https://pds.example.com");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns [`SkybaseError::Chat`] if HTTP client configuration or headers fail.
    pub fn new(
        base_url: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Result<Self, SkybaseError> {
        let base_url = base_url.into().trim().trim_end_matches('/').to_string();
        let token = access_token.into();

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| SkybaseError::Chat(format!("Failed to build chat HTTP client: {e}")))?;

        let auth_state = Arc::new(RwLock::new(AuthState {
            base_url,
            allow_dynamic_pds_updates: true,
            access_jwt: token,
            refresh_jwt: None,
            credentials: None,
        }));

        Ok(Self { client, auth_state })
    }

    /// Attaches a refresh JWT token for automatic session refresh upon token expiry.
    #[must_use]
    pub fn with_refresh_token(self, refresh_jwt: impl Into<String>) -> Self {
        let token = refresh_jwt.into();
        if !token.trim().is_empty() {
            if let Ok(mut state) = self.auth_state.try_write() {
                state.refresh_jwt = Some(token);
            }
        }
        self
    }

    /// Attaches bot credentials for automatic re-authentication if session tokens expire.
    #[must_use]
    pub fn with_credentials(
        self,
        pds_endpoint: impl Into<String>,
        identifier: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        if let Ok(mut state) = self.auth_state.try_write() {
            state.credentials = Some(BotCredentials {
                pds_endpoint: pds_endpoint.into().trim_end_matches('/').to_string(),
                identifier: identifier.into(),
                password: password.into(),
            });
        }
        self
    }

    /// Returns the currently active bearer access JWT.
    pub async fn current_access_token(&self) -> String {
        self.auth_state.read().await.access_jwt.clone()
    }

    /// Returns the active XRPC base URL used for chat requests (either the account's
    /// authoritative PDS gateway or an explicit chat service endpoint).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use skybase::chat::ChatClient;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ChatClient::new("https://chalciporus.us-west.host.bsky.network/", "tok")?;
    /// assert_eq!(
    ///     client.base_url().await,
    ///     "https://chalciporus.us-west.host.bsky.network"
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub async fn base_url(&self) -> String {
        self.auth_state.read().await.base_url.clone()
    }

    /// Authenticates with an ATProto account identifier and App Password via `com.atproto.server.createSession`.
    ///
    /// Automatically extracts the account's authoritative `#atproto_pds` service endpoint from
    /// `didDoc` when `chat_endpoint` is the default chat backend (`<https://api.bsky.chat>`) or
    /// the Bluesky entryway (`<https://bsky.social>`), routing chat XRPC calls through the PDS
    /// with `atproto-proxy: did:web:api.bsky.chat#bsky_chat` so `at+jwt` user session tokens are
    /// exchanged for inter-service JWTs by the PDS.
    ///
    /// Returns a configured [`ChatClient`] and the resolved account DID.
    /// The client retains credentials and refresh token to automatically recover from session expiry.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if authentication or session parsing fails.
    pub async fn login_with_app_password(
        pds_endpoint: &str,
        chat_endpoint: &str,
        identifier: &str,
        password: &str,
    ) -> Result<(Self, String), SkybaseError> {
        let pds_url = pds_endpoint.trim().trim_end_matches('/');
        let login_url = format!("{pds_url}/xrpc/com.atproto.server.createSession");

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| SkybaseError::Chat(format!("Failed to build login HTTP client: {e}")))?;

        let body = serde_json::json!({
            "identifier": identifier,
            "password": password,
        });

        let resp = http
            .post(&login_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| SkybaseError::Chat(format!("createSession request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().await.unwrap_or_default();
            return Err(SkybaseError::Chat(format!(
                "Failed to authenticate bot account ({identifier}) at {pds_url} (HTTP {status}): {err_body}"
            )));
        }

        let session: CreateSessionResponse = resp.json().await.map_err(|e| {
            SkybaseError::Chat(format!("Failed to parse createSession response: {e}"))
        })?;

        let (resolved_base_url, allow_dynamic_pds_updates) =
            resolve_session_chat_base_url(pds_url, chat_endpoint, session.did_doc.as_ref());
        debug!(
            did = %session.did,
            chat_base_url = %resolved_base_url,
            "Resolved ATProto chat XRPC base URL for App Password session"
        );

        let mut chat_client = Self::new(&resolved_base_url, &session.access_jwt)?;
        if let Ok(mut state) = chat_client.auth_state.try_write() {
            state.allow_dynamic_pds_updates = allow_dynamic_pds_updates;
        }
        if let Some(ref refresh) = session.refresh_jwt {
            chat_client = chat_client.with_refresh_token(refresh);
        }
        chat_client = chat_client.with_credentials(pds_url, identifier, password);

        Ok((chat_client, session.did))
    }

    /// Checks if token refresh or re-authentication is possible.
    async fn can_refresh(&self) -> bool {
        let state = self.auth_state.read().await;
        state.refresh_jwt.is_some() || state.credentials.is_some()
    }

    /// Attempts to refresh the session using `refreshJwt` or re-login with stored credentials.
    ///
    /// If the token was already refreshed by another concurrent request, this is a no-op.
    async fn refresh_session_or_relogin(&self, failed_token: &str) -> Result<(), SkybaseError> {
        let mut state = self.auth_state.write().await;
        // If another task already refreshed the token, do nothing
        if state.access_jwt != failed_token {
            debug!("ATProto chat session token was already refreshed by another task");
            return Ok(());
        }

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| SkybaseError::Chat(format!("Failed to build refresh HTTP client: {e}")))?;

        // Strategy 1: Attempt refreshSession via refreshJwt if available
        if let Some(ref refresh_jwt) = state.refresh_jwt {
            let refresh_base = state
                .credentials
                .as_ref()
                .map_or(state.base_url.as_str(), |c| c.pds_endpoint.as_str())
                .trim_end_matches('/');
            let refresh_url = format!("{refresh_base}/xrpc/com.atproto.server.refreshSession");
            match http
                .post(&refresh_url)
                .header(AUTHORIZATION, format!("Bearer {refresh_jwt}"))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<RefreshSessionResponse>().await {
                        Ok(refreshed) => {
                            info!("Successfully refreshed ATProto chat session token via refreshSession");
                            state.access_jwt = refreshed.access_jwt;
                            if let Some(new_refresh) = refreshed.refresh_jwt {
                                if !new_refresh.trim().is_empty() {
                                    state.refresh_jwt = Some(new_refresh);
                                }
                            }
                            if state.allow_dynamic_pds_updates {
                                if let Some(new_pds) = refreshed
                                    .did_doc
                                    .as_ref()
                                    .and_then(extract_pds_endpoint_from_did_doc)
                                {
                                    state.base_url = new_pds;
                                }
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "Failed to parse refreshSession response; falling back to full re-login"
                            );
                        }
                    }
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!(
                        status = %status,
                        body = %body,
                        "refreshSession failed; falling back to full re-login"
                    );
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Network error during refreshSession; falling back to full re-login"
                    );
                }
            }
        }

        // Strategy 2: Full re-login using stored App Password credentials
        if let Some(ref creds) = state.credentials {
            info!(
                identifier = %creds.identifier,
                "Re-authenticating ATProto chat session via App Password..."
            );
            let login_url = format!(
                "{}/xrpc/com.atproto.server.createSession",
                creds.pds_endpoint.trim_end_matches('/')
            );
            let body = serde_json::json!({
                "identifier": creds.identifier,
                "password": creds.password,
            });

            let resp = http
                .post(&login_url)
                .json(&body)
                .send()
                .await
                .map_err(|e| SkybaseError::Chat(format!("Re-login request failed: {e}")))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let err_body = resp.text().await.unwrap_or_default();
                return Err(SkybaseError::Chat(format!(
                    "Failed to re-authenticate bot session at {login_url} (HTTP {status}): {err_body}"
                )));
            }

            let session: CreateSessionResponse = resp.json().await.map_err(|e| {
                SkybaseError::Chat(format!("Failed to parse createSession response: {e}"))
            })?;

            info!("Successfully re-authenticated ATProto chat session via App Password");
            state.access_jwt = session.access_jwt;
            if let Some(new_refresh) = session.refresh_jwt {
                if !new_refresh.trim().is_empty() {
                    state.refresh_jwt = Some(new_refresh);
                }
            }
            if state.allow_dynamic_pds_updates {
                if let Some(new_pds) = session
                    .did_doc
                    .as_ref()
                    .and_then(extract_pds_endpoint_from_did_doc)
                {
                    state.base_url = new_pds;
                }
            }
            return Ok(());
        }

        Err(SkybaseError::Chat(
            "Session token expired, but no refresh token or credentials configured to refresh"
                .to_string(),
        ))
    }

    /// Internal request helper with automatic single-flight token refresh retry.
    async fn send_request(
        &self,
        op_name: &str,
        method: reqwest::Method,
        xrpc_path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<reqwest::Response, SkybaseError> {
        let (current_token, current_base_url) = {
            let state = self.auth_state.read().await;
            (state.access_jwt.clone(), state.base_url.clone())
        };
        let req = self.build_request(
            method.clone(),
            &current_base_url,
            xrpc_path,
            &current_token,
            body.as_ref(),
        );

        let resp = req
            .send()
            .await
            .map_err(|e| SkybaseError::Chat(format!("{op_name} request failed: {e}")))?;
        let status = resp.status();

        if status.is_success() {
            return Ok(resp);
        }

        let body_text = resp.text().await.unwrap_or_default();
        if is_recoverable_session_expiry(status, &body_text) && self.can_refresh().await {
            warn!(
                op = op_name,
                status = %status,
                "ATProto chat session token expired; refreshing and retrying..."
            );
            if let Err(e) = self.refresh_session_or_relogin(&current_token).await {
                error!(error = %e, "Failed to refresh ATProto chat session");
                return Err(SkybaseError::Chat(format!(
                    "{op_name} failed with status {status}: {body_text} (refresh failed: {e})"
                )));
            }

            let (new_token, new_base_url) = {
                let state = self.auth_state.read().await;
                (state.access_jwt.clone(), state.base_url.clone())
            };
            let retry_req =
                self.build_request(method, &new_base_url, xrpc_path, &new_token, body.as_ref());

            let retry_resp = retry_req
                .send()
                .await
                .map_err(|e| SkybaseError::Chat(format!("{op_name} retry failed: {e}")))?;
            if !retry_resp.status().is_success() {
                let retry_status = retry_resp.status();
                let retry_body = retry_resp.text().await.unwrap_or_default();
                return Err(SkybaseError::Chat(format!(
                    "{op_name} failed with status {retry_status}: {retry_body}"
                )));
            }

            return Ok(retry_resp);
        }

        Err(SkybaseError::Chat(format!(
            "{op_name} failed with status {status}: {body_text}"
        )))
    }

    /// Builds a bearer-authenticated XRPC request, attaching `atproto-proxy` when targeting
    /// a PDS gateway rather than `api.bsky.chat` directly.
    fn build_request(
        &self,
        method: reqwest::Method,
        base_url: &str,
        xrpc_path: &str,
        token: &str,
        body: Option<&serde_json::Value>,
    ) -> reqwest::RequestBuilder {
        let url = format!("{base_url}{xrpc_path}");
        let mut req = self
            .client
            .request(method, &url)
            .header(AUTHORIZATION, format!("Bearer {token}"));
        if !is_direct_chat_endpoint(base_url) {
            req = req.header(
                HeaderName::from_static("atproto-proxy"),
                HeaderValue::from_static(ATPROTO_CHAT_PROXY_DID),
            );
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        req
    }

    /// Lists active conversations for the authenticated user.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn list_convos(
        &self,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ListConvosResponse, SkybaseError> {
        let mut xrpc_path = "/xrpc/chat.bsky.convo.listConvos".to_string();
        let mut query_params = Vec::new();

        if let Some(limit) = limit {
            query_params.push(format!("limit={limit}"));
        }
        if let Some(cursor) = cursor {
            let encoded: String = url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect();
            query_params.push(format!("cursor={encoded}"));
        }
        if !query_params.is_empty() {
            xrpc_path.push('?');
            xrpc_path.push_str(&query_params.join("&"));
        }

        let resp = self
            .send_request("listConvos", reqwest::Method::GET, &xrpc_path, None)
            .await?;
        let result = resp
            .json::<ListConvosResponse>()
            .await
            .map_err(|e| SkybaseError::Chat(format!("Failed to parse listConvos response: {e}")))?;
        debug!(count = result.convos.len(), "Fetched chat conversations");
        Ok(result)
    }

    /// Lists incoming conversation requests for the authenticated user via `chat.bsky.convo.listConvoRequests`.
    ///
    /// Non-followed accounts initiating conversations land in the requests state until accepted.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn list_convo_requests(
        &self,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ListConvoRequestsResponse, SkybaseError> {
        let mut xrpc_path = "/xrpc/chat.bsky.convo.listConvoRequests".to_string();
        let mut query_params = Vec::new();

        if let Some(limit) = limit {
            query_params.push(format!("limit={limit}"));
        }
        if let Some(cursor) = cursor {
            let encoded: String = url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect();
            query_params.push(format!("cursor={encoded}"));
        }
        if !query_params.is_empty() {
            xrpc_path.push('?');
            xrpc_path.push_str(&query_params.join("&"));
        }

        let resp = self
            .send_request("listConvoRequests", reqwest::Method::GET, &xrpc_path, None)
            .await?;
        let result = resp
            .json::<ListConvoRequestsResponse>()
            .await
            .map_err(|e| {
                SkybaseError::Chat(format!("Failed to parse listConvoRequests response: {e}"))
            })?;
        debug!(
            count = result.requests.len(),
            "Fetched chat conversation requests"
        );
        Ok(result)
    }

    /// Accepts an incoming conversation request via `chat.bsky.convo.acceptConvo`.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn accept_convo(&self, convo_id: &str) -> Result<AcceptConvoResponse, SkybaseError> {
        let xrpc_path = "/xrpc/chat.bsky.convo.acceptConvo";
        let req = AcceptConvoRequest {
            convo_id: convo_id.to_string(),
        };
        let body = serde_json::to_value(&req)
            .map_err(|e| SkybaseError::Chat(format!("Failed to serialize acceptConvo: {e}")))?;

        let resp = self
            .send_request("acceptConvo", reqwest::Method::POST, xrpc_path, Some(body))
            .await?;
        let result = resp.json::<AcceptConvoResponse>().await.map_err(|e| {
            SkybaseError::Chat(format!("Failed to parse acceptConvo response: {e}"))
        })?;
        debug!(
            convo_id = %convo_id,
            rev = ?result.rev,
            "Accepted chat conversation request"
        );
        Ok(result)
    }

    /// Fetches messages for a specific conversation.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn get_messages(
        &self,
        convo_id: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<GetMessagesResponse, SkybaseError> {
        let encoded_convo_id: String =
            url::form_urlencoded::byte_serialize(convo_id.as_bytes()).collect();
        let mut xrpc_path = format!("/xrpc/chat.bsky.convo.getMessages?convoId={encoded_convo_id}");
        if let Some(limit) = limit {
            xrpc_path.push_str(&format!("&limit={limit}"));
        }
        if let Some(cursor) = cursor {
            let encoded_cursor: String =
                url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect();
            xrpc_path.push_str(&format!("&cursor={encoded_cursor}"));
        }

        let resp = self
            .send_request("getMessages", reqwest::Method::GET, &xrpc_path, None)
            .await?;
        let result = resp.json::<GetMessagesResponse>().await.map_err(|e| {
            SkybaseError::Chat(format!("Failed to parse getMessages response: {e}"))
        })?;
        Ok(result)
    }

    /// Sends a text message to the specified conversation, automatically extracting and attaching link facets.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn send_message(
        &self,
        convo_id: &str,
        text: &str,
    ) -> Result<MessageView, SkybaseError> {
        let payload = SendMessagePayload::new(text);
        self.send_message_payload(convo_id, payload).await
    }

    /// Sends a structured message payload via `chat.bsky.convo.sendMessage`.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn send_message_payload(
        &self,
        convo_id: &str,
        payload: SendMessagePayload,
    ) -> Result<MessageView, SkybaseError> {
        let xrpc_path = "/xrpc/chat.bsky.convo.sendMessage";
        let req = SendMessageRequest {
            convo_id: convo_id.to_string(),
            message: payload,
        };
        let body = serde_json::to_value(&req)
            .map_err(|e| SkybaseError::Chat(format!("Failed to serialize sendMessage: {e}")))?;

        let resp = self
            .send_request("sendMessage", reqwest::Method::POST, xrpc_path, Some(body))
            .await?;
        let result = resp.json::<MessageView>().await.map_err(|e| {
            SkybaseError::Chat(format!("Failed to parse sendMessage response: {e}"))
        })?;
        Ok(result)
    }

    /// Marks messages up to `message_id` as read in the conversation.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request fails.
    pub async fn update_read(&self, convo_id: &str, message_id: &str) -> Result<(), SkybaseError> {
        let xrpc_path = "/xrpc/chat.bsky.convo.updateRead";
        let req = UpdateReadRequest {
            convo_id: convo_id.to_string(),
            message_id: message_id.to_string(),
        };
        let body = serde_json::to_value(&req)
            .map_err(|e| SkybaseError::Chat(format!("Failed to serialize updateRead: {e}")))?;

        self.send_request("updateRead", reqwest::Method::POST, xrpc_path, Some(body))
            .await?;
        Ok(())
    }

    /// Finds or initializes a conversation for the specified member DIDs via `chat.bsky.convo.getConvoForMembers`.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if the HTTP request or JSON deserialization fails.
    pub async fn get_convo_for_members(&self, members: &[&str]) -> Result<ConvoView, SkybaseError> {
        let mut xrpc_path = "/xrpc/chat.bsky.convo.getConvoForMembers?".to_string();
        let params: Vec<String> = members
            .iter()
            .map(|m| {
                let encoded: String = url::form_urlencoded::byte_serialize(m.as_bytes()).collect();
                format!("members={encoded}")
            })
            .collect();
        xrpc_path.push_str(&params.join("&"));

        let resp = self
            .send_request("getConvoForMembers", reqwest::Method::GET, &xrpc_path, None)
            .await?;

        #[derive(Deserialize)]
        struct GetConvoResponse {
            convo: ConvoView,
        }

        let result = resp.json::<GetConvoResponse>().await.map_err(|e| {
            SkybaseError::Chat(format!("Failed to parse getConvoForMembers response: {e}"))
        })?;
        Ok(result.convo)
    }

    /// Attempts to find an existing conversation containing the specified DID or initialize one.
    ///
    /// # Errors
    /// Returns [`SkybaseError`] if conversation lookup fails.
    pub async fn find_or_create_convo_for_did(&self, did: &str) -> Result<String, SkybaseError> {
        // Try get_convo_for_members first
        if let Ok(convo) = self.get_convo_for_members(&[did]).await {
            return Ok(convo.id);
        }

        // Fallback: search existing conversations from listConvos
        let resp = self.list_convos(Some(50), None).await?;
        for convo in resp.convos {
            if convo.members.iter().any(|m| m.did == did) {
                return Ok(convo.id);
            }
        }

        Err(SkybaseError::Chat(format!(
            "Could not find or establish conversation with {did}"
        )))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_chat_client_login_and_endpoints_and_auto_refresh() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "did": "did:plc:bot123",
                "accessJwt": "tok_initial",
                "refreshJwt": "ref_initial"
            })))
            .mount(&server)
            .await;

        let (client, bot_did) = ChatClient::login_with_app_password(
            &server.uri(),
            &server.uri(),
            "bot.bsky.social",
            "app-pass",
        )
        .await
        .unwrap();
        assert_eq!(bot_did, "did:plc:bot123");
        assert_eq!(client.current_access_token().await, "tok_initial");

        // list_convos with limit & cursor
        Mock::given(method("GET"))
            .and(path("/xrpc/chat.bsky.convo.listConvos"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "convos": [{
                    "id": "convo-1",
                    "rev": "r1",
                    "members": [{"did": "did:plc:alice", "handle": "alice.bsky.social"}],
                    "unreadCount": 1
                }],
                "cursor": "c1"
            })))
            .mount(&server)
            .await;
        let convos = client.list_convos(Some(10), Some("c0")).await.unwrap();
        assert_eq!(convos.convos.len(), 1);

        // list_convo_requests & accept_convo
        Mock::given(method("GET"))
            .and(path("/xrpc/chat.bsky.convo.listConvoRequests"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "requests": [{"id": "req-1", "rev": "r1", "members": [], "unreadCount": 0}],
                "cursor": "rc1"
            })))
            .mount(&server)
            .await;
        let reqs = client
            .list_convo_requests(Some(5), Some("rc0"))
            .await
            .unwrap();
        assert_eq!(reqs.requests.len(), 1);

        Mock::given(method("POST"))
            .and(path("/xrpc/chat.bsky.convo.acceptConvo"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"rev": "r2"})),
            )
            .mount(&server)
            .await;
        let accepted = client.accept_convo("req-1").await.unwrap();
        assert_eq!(accepted.rev.as_deref(), Some("r2"));

        // get_messages, send_message, update_read
        Mock::given(method("GET"))
            .and(path("/xrpc/chat.bsky.convo.getMessages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "messages": [{
                    "$type": "chat.bsky.convo.defs#messageView",
                    "id": "m1",
                    "rev": "r1",
                    "text": "hello",
                    "sender": {"did": "did:plc:alice"},
                    "sentAt": "2026-11-03T00:00:00Z"
                }]
            })))
            .mount(&server)
            .await;
        let msgs = client
            .get_messages("convo-1", Some(10), Some("mc0"))
            .await
            .unwrap();
        assert_eq!(msgs.messages.len(), 1);

        Mock::given(method("POST"))
            .and(path("/xrpc/chat.bsky.convo.sendMessage"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "m2",
                "rev": "r2",
                "text": "reply https://example.com",
                "sender": {"did": "did:plc:bot123"},
                "sentAt": "2026-11-03T00:00:01Z"
            })))
            .mount(&server)
            .await;
        let sent = client
            .send_message("convo-1", "reply https://example.com")
            .await
            .unwrap();
        assert_eq!(sent.id, "m2");

        Mock::given(method("POST"))
            .and(path("/xrpc/chat.bsky.convo.updateRead"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        client.update_read("convo-1", "m2").await.unwrap();

        // find_or_create_convo_for_did: fallback to list_convos when getConvoForMembers returns 500
        Mock::given(method("GET"))
            .and(path("/xrpc/chat.bsky.convo.getConvoForMembers"))
            .respond_with(ResponseTemplate::new(500).set_body_string("err"))
            .mount(&server)
            .await;
        let found_id = client
            .find_or_create_convo_for_did("did:plc:alice")
            .await
            .unwrap();
        assert_eq!(found_id, "convo-1");
        assert!(client
            .find_or_create_convo_for_did("did:plc:unknown")
            .await
            .is_err());

        // Test refresh_session_or_relogin via refreshSession success
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.refreshSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "accessJwt": "tok_refreshed",
                "refreshJwt": "ref_rotated"
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        client
            .refresh_session_or_relogin("tok_initial")
            .await
            .unwrap();
        assert_eq!(client.current_access_token().await, "tok_refreshed");

        // Already refreshed token is a no-op
        client
            .refresh_session_or_relogin("tok_initial")
            .await
            .unwrap();

        // When refreshSession fails, falls back to createSession re-login
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.refreshSession"))
            .respond_with(ResponseTemplate::new(400).set_body_string("ExpiredToken"))
            .mount(&server)
            .await;
        client
            .refresh_session_or_relogin("tok_refreshed")
            .await
            .unwrap();
        assert_eq!(client.current_access_token().await, "tok_initial");
    }

    #[test]
    fn test_did_doc_pds_extraction_and_base_url_resolution_matrix() {
        let canonical_doc = serde_json::json!({
            "id": "did:plc:pl7wa2phjeo4zp4zxmewnsis",
            "alsoKnownAs": ["at://skybouncer.bsky.social"],
            "service": [
                {
                    "id": "#bsky_notif",
                    "type": "BskyNotificationService",
                    "serviceEndpoint": "https://notif.bsky.app"
                },
                {
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": "https://chalciporus.us-west.host.bsky.network/"
                }
            ]
        });
        assert_eq!(
            extract_pds_endpoint_from_did_doc(&canonical_doc).as_deref(),
            Some("https://chalciporus.us-west.host.bsky.network")
        );

        // Minimal partial didDoc without `id` field (lenient fallback path)
        let partial_doc = serde_json::json!({
            "service": [
                {"id": "#other", "type": "Other", "serviceEndpoint": "https://other.example"},
                {"id": "#atproto_pds", "serviceEndpoint": "https://pds-shard.example.com/"}
            ]
        });
        assert_eq!(
            extract_pds_endpoint_from_did_doc(&partial_doc).as_deref(),
            Some("https://pds-shard.example.com")
        );

        // Invalid non-HTTP serviceEndpoint schemes are rejected
        let bad_scheme_doc = serde_json::json!({
            "service": [
                {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "ftp://bad.example.com"}
            ]
        });
        assert!(extract_pds_endpoint_from_did_doc(&bad_scheme_doc).is_none());

        // Entryway + DEFAULT_CHAT_ENDPOINT + didDoc -> routes through authoritative PDS shard
        let (url, dynamic) = resolve_session_chat_base_url(
            "https://bsky.social",
            DEFAULT_CHAT_ENDPOINT,
            Some(&canonical_doc),
        );
        assert_eq!(url, "https://chalciporus.us-west.host.bsky.network");
        assert!(dynamic);

        // Entryway + entryway as chat_endpoint + didDoc -> routes through authoritative PDS shard
        let (url_entry, dyn_entry) = resolve_session_chat_base_url(
            "https://bsky.social",
            "https://bsky.social",
            Some(&canonical_doc),
        );
        assert_eq!(url_entry, "https://chalciporus.us-west.host.bsky.network");
        assert!(dyn_entry);

        // Self-hosted PDS without didDoc + DEFAULT_CHAT_ENDPOINT -> routes through self-hosted PDS
        let (url_self, dyn_self) = resolve_session_chat_base_url(
            "https://pds.alice.example/",
            DEFAULT_CHAT_ENDPOINT,
            None,
        );
        assert_eq!(url_self, "https://pds.alice.example");
        assert!(dyn_self);

        // Explicit custom chat proxy override is honored and pinned
        let (url_custom, dyn_custom) = resolve_session_chat_base_url(
            "https://bsky.social",
            "https://custom-chat-gateway.example.com/",
            Some(&canonical_doc),
        );
        assert_eq!(url_custom, "https://custom-chat-gateway.example.com");
        assert!(!dyn_custom);

        // Entryway without didDoc falls back cleanly without panicking
        let (url_fallback, _) =
            resolve_session_chat_base_url("https://bsky.social", DEFAULT_CHAT_ENDPOINT, None);
        assert_eq!(url_fallback, DEFAULT_CHAT_ENDPOINT);
        let (url_empty_chat, _) = resolve_session_chat_base_url("https://bsky.social", "", None);
        assert_eq!(url_empty_chat, "https://bsky.social");
    }

    #[test]
    fn test_recoverable_session_expiry_excludes_bad_jwt_type_and_scope() {
        assert!(is_recoverable_session_expiry(
            reqwest::StatusCode::UNAUTHORIZED,
            r#"{"error":"ExpiredToken","message":"Token has expired"}"#
        ));
        assert!(is_recoverable_session_expiry(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"error":"ExpiredToken","message":"Token has expired"}"#
        ));
        assert!(!is_recoverable_session_expiry(
            reqwest::StatusCode::UNAUTHORIZED,
            r#"{"error":"BadJwtType","message":"Invalid jwt type \"at+jwt\""}"#
        ));
        assert!(!is_recoverable_session_expiry(
            reqwest::StatusCode::UNAUTHORIZED,
            r#"{"error":"BadTokenScope","message":"Missing chat scope"}"#
        ));
        assert!(!is_recoverable_session_expiry(
            reqwest::StatusCode::FORBIDDEN,
            "Forbidden"
        ));
    }
}
