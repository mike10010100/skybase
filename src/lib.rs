//! # `skybase`
//!
//! **The Open-Source Backend & Developer Platform for the AT Protocol.**
//!
//! `skybase` is the "Firebase for ATProto" — a modular, high-performance, `#![forbid(unsafe_code)]`
//! backend framework and SDK suite designed to make building decentralized applications on the AT Protocol
//! as simple as building on traditional cloud platforms.
//!
//! Powered by [`skyauth`], `skybase` integrates decentralized OAuth 2.1, DPoP (RFC 9449), PKCE (RFC 7636),
//! sovereign repository (MST) operations, micro-AppView firehose indexing, blob media management, and
//! reactive event triggers into an ergonomic, unified developer platform.

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

pub use error::{Result, SkybaseError};
pub use skyauth;

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
        }
    }

    /// Sets a custom Jetstream WebSocket endpoint for event subscription.
    pub fn with_jetstream_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.jetstream_endpoint = Some(endpoint.into());
        self
    }
}

/// The unified Skybase backend engine and client facade.
#[derive(Clone)]
pub struct Skybase {
    config: Arc<SkybaseConfig>,
    auth_client: Arc<skyauth::client::AtprotoOAuthClient>,
}

impl Skybase {
    /// Initializes a new [`Skybase`] engine instance with the given configuration.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] or [`SkybaseError::Auth`] if client metadata is invalid.
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

        Ok(Self {
            config: Arc::new(config),
            auth_client: Arc::new(auth_client),
        })
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
}
