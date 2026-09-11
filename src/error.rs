//! Strongly-typed error definitions for `skybase`.

use thiserror::Error;

/// Root error type encompassing all failure modes across the `skybase` library.
#[derive(Debug, Error)]
pub enum SkybaseError {
    /// Failure originating from the underlying `skyauth` authentication engine.
    #[error("Authentication error: {0}")]
    Auth(#[from] skyauth::error::AtprotoOAuthError),

    /// Failure executing XRPC or repository operations against a PDS.
    #[error("Repository error: {0}")]
    Repo(String),

    /// Failure during micro-AppView indexing or query execution.
    #[error("Index error: {0}")]
    Index(String),

    /// Failure during blob or media upload/download processing.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Failure in reactive event streaming or subscription handling.
    #[error("Event stream error: {0}")]
    Event(String),

    /// Invalid configuration supplied to Skybase engine or client.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Networking or transport failure.
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    /// Serialization or JSON parsing failure.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Internal error condition.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Convenience alias for `Result<T, SkybaseError>`.
pub type Result<T> = std::result::Result<T, SkybaseError>;
