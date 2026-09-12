//! # `skybase`
//!
//! **The Turn-Key Micro-AppView Engine & Developer Platform for the AT Protocol.**
//!
//! `skybase` brings Firebase-like developer ergonomics to the decentralized AT Protocol ecosystem.
//! Rather than forcing developers to hand-roll custom firehose consumers, backfill pipelines, and
//! database schemas, `skybase` provides a turn-key, single-binary Micro-AppView engine that ingests
//! Bluesky Jetstream commits, indexes them into embedded SQLite WAL with JSON1 virtual columns and
//! FTS5 full-text search, and serves real-time REST and WebSocket live queries down to clients.
//!
//! Powered by [`skyauth`], `skybase` supports three distinct operating topologies:
//! - **Topology A (Client-Sovereign / Zero Custody)**: Client applications write directly to user PDSs;
//!   `skybase` acts as a zero-custody read and indexing engine holding zero private credentials.
//! - **Topology B (Daemon-Managed / Bots)**: Server-side daemons hold DPoP sessions with AES-256-GCM encryption at rest.
//! - **Topology C (Token-Mediated Session Proxy)**: Mediates confidential OAuth sessions for static frontends
//!   (GitHub Pages, Wisp, Tangled) to prevent 2-week session dropouts.

#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    missing_docs,
    rust_2018_idioms
)]

pub mod error;
pub mod index;
pub mod ingest;
pub mod repo;

pub use error::{Result, SkybaseError};
pub use index::{
    parse_at_uri, BroadcastBus, ChangeNotification, QueryBuilder, QueryOp, RecordInput, RecordRow,
    RecordStore, RecordStoreConfig, SortDirection, DEFAULT_BROADCAST_CAPACITY,
};
pub use ingest::{
    build_subscription_url, build_subscription_url_full, normalize_indexed_at,
    parse_frame_timestamp, parse_jetstream_commit, parse_jetstream_frame, sync_commit_to_store,
    BackoffManager, CommitOperation, ConsumerStats, CursorTracker, IngesterConfig, JetstreamCommit,
    JetstreamConsumer, JetstreamConsumerHandle, JetstreamEvent, MockJetstreamServer,
    MockServerCommand, RawJetstreamCommit, RawJetstreamMessage,
};
pub use repo::{
    format_at_uri, generate_tid, validate_rkey, CreateRecordRequest, CreateRecordResult,
    DeleteRecordRequest, PdsRepoClient, TidGenerator, XrpcErrorResponse,
};
pub use skyauth;
pub use tokio_util::sync::CancellationToken;

use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configuration options for initializing a [`Skybase`] engine instance.
#[derive(Debug, Clone)]
pub struct SkybaseConfig {
    /// OAuth Client ID URI (e.g., `<https://app.example.com/oauth/client-metadata.json>`).
    pub client_id: String,
    /// OAuth callback redirect URI (e.g., `<https://app.example.com/oauth/callback>`).
    pub redirect_uri: String,
    /// Application display name presented during consent dialogs.
    pub app_name: String,
    /// Optional Jetstream WebSocket endpoint for real-time firehose subscription.
    pub jetstream_endpoint: Option<String>,
    /// Optional filesystem path to SQLite database. If set, opens a persistent WAL store.
    pub storage_path: Option<PathBuf>,
    /// Whether to open an isolated in-memory SQLite database upon initialization.
    pub in_memory_store: bool,
    /// Optional explicit configuration for the embedded record store.
    pub store_config: Option<RecordStoreConfig>,
    /// Target collection NSIDs to filter for at the edge during Jetstream ingestion.
    pub wanted_collections: Vec<String>,
    /// Target repository DIDs to filter for at the edge during Jetstream ingestion.
    pub wanted_dids: Vec<String>,
}

impl SkybaseConfig {
    /// Creates a new [`SkybaseConfig`] with required OAuth parameters.
    pub fn new(
        client_id: impl Into<String>,
        redirect_uri: impl Into<String>,
        app_name: impl Into<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            redirect_uri: redirect_uri.into(),
            app_name: app_name.into(),
            jetstream_endpoint: None,
            storage_path: None,
            in_memory_store: false,
            store_config: None,
            wanted_collections: Vec::new(),
            wanted_dids: Vec::new(),
        }
    }

    /// Sets a custom Jetstream WebSocket endpoint for event subscription.
    pub fn with_jetstream_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.jetstream_endpoint = Some(endpoint.into());
        self
    }

    /// Configures a persistent SQLite database file path.
    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    /// Configures an isolated in-memory SQLite database.
    pub fn with_in_memory_store(mut self) -> Self {
        self.in_memory_store = true;
        self
    }

    /// Sets an explicit [`RecordStoreConfig`].
    pub fn with_store_config(mut self, config: RecordStoreConfig) -> Self {
        self.store_config = Some(config);
        self
    }

    /// Appends a target collection NSID to filter for at the edge.
    pub fn with_wanted_collection(mut self, collection: impl Into<String>) -> Self {
        self.wanted_collections.push(collection.into());
        self
    }

    /// Sets target collection NSIDs to filter for at the edge.
    pub fn with_wanted_collections<I, S>(mut self, collections: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.wanted_collections = collections.into_iter().map(Into::into).collect();
        self
    }

    /// Appends a target DID to filter for at the edge.
    pub fn with_wanted_did(mut self, did: impl Into<String>) -> Self {
        self.wanted_dids.push(did.into());
        self
    }

    /// Sets target DIDs to filter for at the edge.
    pub fn with_wanted_dids<I, S>(mut self, dids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.wanted_dids = dids.into_iter().map(Into::into).collect();
        self
    }
}

/// The unified Skybase backend engine and client facade.
#[derive(Clone)]
pub struct Skybase {
    config: Arc<SkybaseConfig>,
    auth_client: Arc<skyauth::client::AtprotoOAuthClient>,
    store: Option<RecordStore>,
}

impl Skybase {
    /// Initializes a new [`Skybase`] engine instance with the given configuration.
    ///
    /// If `storage_path`, `in_memory_store`, or `store_config` is configured, initializes
    /// the embedded SQLite [`RecordStore`] with canonical WAL schemas and indexes.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if metadata is invalid, [`SkybaseError::Auth`] on OAuth
    /// client failure, or [`SkybaseError::Storage`] if SQLite initialization fails.
    pub fn new(config: SkybaseConfig) -> Result<Self> {
        if config.client_id.trim().is_empty() {
            return Err(SkybaseError::Config("client_id cannot be empty".into()));
        }
        if config.redirect_uri.trim().is_empty() {
            return Err(SkybaseError::Config("redirect_uri cannot be empty".into()));
        }

        let metadata =
            skyauth::client::OAuthClientMetadata::new(&config.client_id, &config.redirect_uri)
                .with_client_name(&config.app_name);

        let auth_client = skyauth::client::AtprotoOAuthClient::builder()
            .client_metadata(metadata)
            .build()
            .map_err(SkybaseError::Auth)?;

        let store = if let Some(ref store_cfg) = config.store_config {
            Some(RecordStore::with_config(store_cfg.clone())?)
        } else if let Some(ref path) = config.storage_path {
            Some(RecordStore::open(path)?)
        } else if config.in_memory_store {
            Some(RecordStore::open_in_memory()?)
        } else {
            None
        };

        Ok(Self {
            config: Arc::new(config),
            auth_client: Arc::new(auth_client),
            store,
        })
    }

    /// Convenience constructor initializing [`Skybase`] with an in-memory SQLite store.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] or [`SkybaseError::Auth`] if client metadata is invalid,
    /// or [`SkybaseError::Storage`] if in-memory database initialization fails.
    pub fn in_memory(config: SkybaseConfig) -> Result<Self> {
        Self::new(config.with_in_memory_store())
    }

    /// Attaches an existing [`RecordStore`] to this [`Skybase`] instance.
    #[must_use]
    pub fn with_store(mut self, store: RecordStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Opens or replaces the active persistent [`RecordStore`] at the specified path.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] if opening the database fails.
    pub fn open_store(&mut self, path: impl AsRef<Path>) -> Result<RecordStore> {
        let store = RecordStore::open(path)?;
        self.store = Some(store.clone());
        Ok(store)
    }

    /// Opens or replaces the active [`RecordStore`] with an isolated in-memory database.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] if opening the in-memory database fails.
    pub fn open_in_memory_store(&mut self) -> Result<RecordStore> {
        let store = RecordStore::open_in_memory()?;
        self.store = Some(store.clone());
        Ok(store)
    }

    /// Returns a reference to the active [`SkybaseConfig`].
    #[must_use]
    pub fn config(&self) -> &SkybaseConfig {
        &self.config
    }

    /// Accesses the underlying [`skyauth::client::AtprotoOAuthClient`] identity and authentication engine.
    #[must_use]
    pub fn auth(&self) -> &skyauth::client::AtprotoOAuthClient {
        &self.auth_client
    }

    /// Returns a reference to the active [`RecordStore`], if initialized.
    #[must_use]
    pub fn store(&self) -> Option<&RecordStore> {
        self.store.as_ref()
    }

    /// Returns a reference to the active [`RecordStore`] or returns [`SkybaseError::Config`].
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if no record store is attached.
    pub fn require_store(&self) -> Result<&RecordStore> {
        self.store.as_ref().ok_or_else(|| {
            SkybaseError::Config("RecordStore is not initialized on this Skybase instance".into())
        })
    }

    /// Initiates a structured [`QueryBuilder`] scoped to the given collection NSID.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if no record store is attached.
    pub fn query(&self, collection: impl Into<String>) -> Result<QueryBuilder<'_>> {
        Ok(self.require_store()?.query(collection))
    }

    /// Initiates a structured [`QueryBuilder`] scoped to the given collection NSID (alias for [`query`]).
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if no record store is attached.
    pub fn collection(&self, collection: impl Into<String>) -> Result<QueryBuilder<'_>> {
        self.query(collection)
    }

    /// Subscribes to the live [`ChangeNotification`] broadcast stream.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if no record store is attached.
    pub fn subscribe(&self) -> Result<tokio::sync::broadcast::Receiver<ChangeNotification>> {
        Ok(self.require_store()?.subscribe())
    }

    /// Builds a [`JetstreamConsumer`] using the configured `jetstream_endpoint` and `wanted_collections`.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if `jetstream_endpoint` or `RecordStore` is not configured.
    pub fn consumer(&self) -> Result<JetstreamConsumer> {
        let endpoint = self.config.jetstream_endpoint.as_ref().ok_or_else(|| {
            SkybaseError::Config(
                "Cannot build JetstreamConsumer: 'jetstream_endpoint' is not configured".into(),
            )
        })?;

        let mut config = IngesterConfig::new(endpoint);
        if !self.config.wanted_collections.is_empty() {
            config = config.with_collections(self.config.wanted_collections.clone());
        }
        if !self.config.wanted_dids.is_empty() {
            config = config.with_dids(self.config.wanted_dids.clone());
        }

        self.consumer_with_config(config)
    }

    /// Builds a [`JetstreamConsumer`] with custom [`IngesterConfig`] bound to the internal [`RecordStore`].
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if no record store is attached.
    pub fn consumer_with_config(&self, config: IngesterConfig) -> Result<JetstreamConsumer> {
        let store = self.require_store()?.clone();
        Ok(JetstreamConsumer::new(config, store))
    }

    /// Spawns the default [`JetstreamConsumer`] as a managed background task.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if `jetstream_endpoint` or `RecordStore` is not configured.
    pub fn start_consumer(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<JetstreamConsumerHandle> {
        let consumer = self.consumer()?;
        Ok(consumer.start(cancel))
    }

    /// Spawns a [`JetstreamConsumer`] with custom configuration as a managed background task.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if no record store is attached.
    pub fn start_consumer_with_config(
        &self,
        config: IngesterConfig,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<JetstreamConsumerHandle> {
        let consumer = self.consumer_with_config(config)?;
        Ok(consumer.start(cancel))
    }

    /// Creates a sovereign PDS repository client bound to the provided OAuth session.
    #[must_use]
    pub fn repo_client(&self, session: Arc<skyauth::session::OAuthSession>) -> PdsRepoClient {
        PdsRepoClient::new(session, Arc::clone(&self.auth_client))
    }

    /// Creates a sovereign PDS repository client from explicit credentials or mock endpoint tokens.
    ///
    /// Leverages the facade's internal OAuth client for DPoP proof generation and nonce caching.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Auth`] if session creation fails.
    pub fn repo_client_from_credentials(
        &self,
        pds_endpoint: impl Into<String>,
        repo_did: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Result<PdsRepoClient> {
        let endpoint = pds_endpoint.into();
        let did = repo_did.into();
        let token = access_token.into();

        let session = skyauth::session::OAuthSession::new(
            did,
            token,
            None,
            "DPoP",
            None,
            Some(3600),
            skyauth::dpop::DPoPKey::generate(),
            Some(endpoint),
            None,
            None,
        )
        .map_err(SkybaseError::Auth)?;

        Ok(PdsRepoClient::new(
            Arc::new(session),
            Arc::clone(&self.auth_client),
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_skybase_initialization() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "Test App",
        );

        let skybase = Skybase::new(config).expect("Skybase should initialize successfully");
        assert_eq!(skybase.config().app_name, "Test App");
    }

    #[test]
    fn test_skybase_config_validation() {
        let invalid_config = SkybaseConfig::new("", "https://app.example.com/callback", "App");
        let result = Skybase::new(invalid_config);
        assert!(matches!(result, Err(SkybaseError::Config(_))));
    }

    #[test]
    fn test_skybase_repo_client_creation() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "Test App",
        );
        let skybase = Skybase::new(config).expect("Skybase init failed");

        let session = skyauth::session::OAuthSession::new(
            "did:plc:alice",
            "access_token",
            None,
            "DPoP",
            None,
            Some(3600),
            skyauth::dpop::DPoPKey::generate(),
            Some("https://pds.example.com".into()),
            None,
            None,
        )
        .expect("session creation failed");

        let client = skybase.repo_client(Arc::new(session));
        assert_eq!(client.did(), "did:plc:alice");
        assert_eq!(
            client.pds_endpoint().expect("endpoint"),
            "https://pds.example.com"
        );
    }

    #[test]
    fn test_skybase_config_builders() {
        let path = std::path::PathBuf::from("/tmp/skybase_test.db");
        let store_cfg = RecordStoreConfig::in_memory();
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "Builder Test App",
        )
        .with_jetstream_endpoint("wss://jetstream.example.com/subscribe")
        .with_storage_path(&path)
        .with_in_memory_store()
        .with_store_config(store_cfg)
        .with_wanted_collections(vec!["app.bsky.actor.profile", "app.bsky.feed.like"])
        .with_wanted_collection("app.bsky.feed.post")
        .with_wanted_dids(vec!["did:plc:bob", "did:plc:charlie"])
        .with_wanted_did("did:plc:alice");

        assert_eq!(
            config.jetstream_endpoint.as_deref(),
            Some("wss://jetstream.example.com/subscribe")
        );
        assert_eq!(config.storage_path.as_deref(), Some(path.as_path()));
        assert!(config.in_memory_store);
        assert!(config.store_config.is_some());
        assert_eq!(config.wanted_collections.len(), 3);
        assert_eq!(config.wanted_dids.len(), 3);
    }

    #[test]
    fn test_skybase_in_memory_and_store_delegation() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "In Memory App",
        );

        let skybase = Skybase::in_memory(config).expect("in_memory init failed");
        assert!(skybase.store().is_some());
        assert!(skybase.require_store().is_ok());

        // Test query and collection builder
        let q1 = skybase.query("app.bsky.feed.post").expect("query failed");
        let (sql1, _) = q1.build_sql().expect("sql build failed");
        assert!(sql1.contains("WHERE collection = ?"));

        let q2 = skybase
            .collection("app.bsky.feed.post")
            .expect("collection failed");
        let (sql2, _) = q2.build_sql().expect("sql build failed");
        assert_eq!(sql1, sql2);

        // Test subscribe
        let mut rx = skybase.subscribe().expect("subscribe failed");
        let input = RecordInput::new(
            "did:plc:alice",
            "app.bsky.feed.post",
            "post_facade_1",
            "cid_1",
            serde_json::json!({"text": "Hello facade"}),
            1_000,
        );
        skybase
            .require_store()
            .unwrap()
            .upsert_record(&input)
            .expect("upsert failed");

        let event = rx.try_recv().expect("broadcast event expected");
        match event {
            ChangeNotification::Upsert(row) => {
                assert_eq!(row.uri, input.uri);
                assert_eq!(row.record_json["text"], "Hello facade");
            }
            _ => panic!("Expected Upsert event"),
        }
    }

    #[test]
    fn test_skybase_uninitialized_store_errors() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "No Store App",
        );
        let skybase = Skybase::new(config).expect("init failed");
        assert!(skybase.store().is_none());
        assert!(matches!(
            skybase.require_store(),
            Err(SkybaseError::Config(_))
        ));
        assert!(matches!(skybase.query("col"), Err(SkybaseError::Config(_))));
        assert!(matches!(
            skybase.collection("col"),
            Err(SkybaseError::Config(_))
        ));
        assert!(matches!(skybase.subscribe(), Err(SkybaseError::Config(_))));
        assert!(matches!(skybase.consumer(), Err(SkybaseError::Config(_))));
    }

    #[test]
    fn test_skybase_with_store_and_open_store() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "Attach Store App",
        );
        let mut skybase = Skybase::new(config).expect("init failed");
        assert!(skybase.store().is_none());

        let store = RecordStore::open_in_memory().expect("store init failed");
        skybase = skybase.with_store(store);
        assert!(skybase.store().is_some());

        // Replace with open_in_memory_store
        let store2 = skybase
            .open_in_memory_store()
            .expect("open_in_memory_store failed");
        assert!(skybase.store().is_some());
        let _ = store2;

        // Replace with open_store in a tempdir
        let temp_dir = tempfile::tempdir().expect("tempdir failed");
        let db_path = temp_dir.path().join("skybase_facade.db");
        let store3 = skybase.open_store(&db_path).expect("open_store failed");
        assert!(skybase.store().is_some());
        let _ = store3;
    }

    #[tokio::test]
    async fn test_skybase_consumer_wiring() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "Consumer App",
        )
        .with_jetstream_endpoint("wss://jetstream.example.com/subscribe")
        .with_in_memory_store()
        .with_wanted_collection("app.bsky.feed.post")
        .with_wanted_did("did:plc:alice");

        let skybase = Skybase::new(config).expect("init failed");
        let consumer = skybase.consumer().expect("consumer build failed");
        assert_eq!(
            consumer.config().wanted_collections,
            vec!["app.bsky.feed.post"]
        );
        assert_eq!(consumer.config().wanted_dids, vec!["did:plc:alice"]);

        let cancel = CancellationToken::new();
        let handle = skybase
            .start_consumer(cancel.clone())
            .expect("start_consumer failed");
        handle.stop();
        let _ = handle.join().await;
    }

    #[test]
    fn test_skybase_repo_client_from_credentials() {
        let config = SkybaseConfig::new(
            "https://app.example.com/oauth/client-metadata.json",
            "https://app.example.com/oauth/callback",
            "Repo App",
        );
        let skybase = Skybase::new(config).expect("init failed");
        let client = skybase
            .repo_client_from_credentials(
                "https://pds.example.com",
                "did:plc:bob",
                "dpop_access_token_123",
            )
            .expect("repo_client_from_credentials failed");

        assert_eq!(client.did(), "did:plc:bob");
        assert_eq!(
            client.pds_endpoint().expect("endpoint"),
            "https://pds.example.com"
        );
    }
}
