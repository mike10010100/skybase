//! Jetstream firehose event models, lenient frame parsing, and storage mapping.
//!
//! Bluesky Jetstream firehoses emit line-delimited JSON text frames representing
//! repository mutations (commits), heartbeats, identity updates, and account events.
//! This module provides resilient, non-panicking deserialization and conversion into
//! canonical [`RecordInput`] mutations for [`RecordStore`].

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::index::{RecordInput, RecordStore};

/// Normalizes a Jetstream microsecond timestamp into seconds if needed.
///
/// Preserves or normalizes timestamps into canonical microseconds (`time_us`).
///
/// In ATProto Jetstream, event timestamps are natively microsecond precision.
/// If a non-zero timestamp is provided in seconds (< 10^11), it is scaled to microseconds.
/// Microsecond values (>= 10^11) are preserved directly with full sub-second fidelity.
#[must_use]
pub const fn normalize_indexed_at(time_us: u64) -> u64 {
    if time_us > 0 && time_us < 100_000_000_000 {
        time_us.saturating_mul(1_000_000)
    } else {
        time_us
    }
}

/// Commit operation type for Jetstream repository events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitOperation {
    /// Record creation.
    Create,
    /// Record update / replacement.
    Update,
    /// Record deletion.
    Delete,
}

impl CommitOperation {
    /// Parses an operation string case-insensitively.
    #[must_use]
    pub fn from_str_loose(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("create") {
            Some(Self::Create)
        } else if s.eq_ignore_ascii_case("update") {
            Some(Self::Update)
        } else if s.eq_ignore_ascii_case("delete") {
            Some(Self::Delete)
        } else {
            None
        }
    }

    /// Returns the canonical lower-case string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// Lenient raw wire envelope for Jetstream WebSocket messages.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawJetstreamMessage {
    /// Author DID of the Jetstream event (e.g., `did:plc:...`).
    #[serde(default)]
    pub did: Option<String>,

    /// Event timestamp in microseconds since Unix epoch.
    #[serde(default)]
    pub time_us: Option<u64>,

    /// Event kind string (e.g., "commit", "identity", "account", or omitted).
    #[serde(default)]
    pub kind: Option<String>,

    /// Commit payload if kind is "commit".
    #[serde(default)]
    pub commit: Option<RawJetstreamCommit>,
}

/// Lenient raw wire payload for Jetstream commit operations.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawJetstreamCommit {
    /// Commit operation type (e.g., "create", "update", "delete").
    #[serde(default)]
    pub operation: String,

    /// AT Protocol Lexicon NSID (e.g., "app.bsky.feed.post").
    #[serde(default)]
    pub collection: String,

    /// Record key identifier within the collection.
    #[serde(default)]
    pub rkey: String,

    /// Record content identifier (CID) string for create/update.
    #[serde(default)]
    pub cid: Option<String>,

    /// Arbitrary Lexicon JSON payload for create/update.
    #[serde(default)]
    pub record: Option<serde_json::Value>,

    /// Optional repository revision identifier.
    #[serde(default)]
    pub rev: Option<String>,
}

/// Structured Jetstream commit event representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JetstreamCommit {
    /// Repository owner DID (`did:plc:...`).
    pub did: String,
    /// Event timestamp in microseconds.
    pub time_us: u64,
    /// Lexicon NSID collection name (e.g., `app.bsky.feed.post`).
    pub collection: String,
    /// Record key within the collection.
    pub rkey: String,
    /// Operation type (`Create`, `Update`, `Delete`).
    pub operation: CommitOperation,
    /// Content identifier (CID) string, present on `Create` and `Update`.
    pub cid: Option<String>,
    /// Record JSON payload, present on `Create` and `Update`.
    pub record: Option<serde_json::Value>,
}

impl JetstreamCommit {
    /// Formats the canonical AT-URI (`at://{did}/{collection}/{rkey}`).
    #[must_use]
    pub fn uri(&self) -> String {
        format!("at://{}/{}/{}", self.did, self.collection, self.rkey)
    }

    /// Converts this commit into a [`RecordInput`] for storage in [`RecordStore`].
    ///
    /// Returns `None` if the operation is [`CommitOperation::Delete`] or if `record`
    /// is missing on a `Create` or `Update` operation.
    #[must_use]
    pub fn to_record_input(&self, indexed_at_timestamp: Option<u64>) -> Option<RecordInput> {
        if self.operation == CommitOperation::Delete {
            return None;
        }

        let record_json = self.record.clone()?;
        let cid = self.cid.clone().unwrap_or_default();
        let indexed_at = indexed_at_timestamp.unwrap_or_else(|| normalize_indexed_at(self.time_us));

        Some(RecordInput {
            uri: self.uri(),
            cid,
            did: self.did.clone(),
            collection: self.collection.clone(),
            rkey: self.rkey.clone(),
            record_json,
            indexed_at,
        })
    }
}

/// High-level event emitted after parsing a Jetstream WebSocket frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JetstreamEvent {
    /// Repository commit mutation.
    Commit(JetstreamCommit),
    /// Heartbeat frame advancing the cursor without record mutations.
    Heartbeat {
        /// Timestamp in microseconds.
        time_us: u64,
    },
    /// Other non-commit event (e.g., identity, account) that advances the cursor.
    Other {
        /// Event kind string.
        kind: String,
        /// Optional author DID.
        did: Option<String>,
        /// Timestamp in microseconds.
        time_us: u64,
    },
}

impl JetstreamEvent {
    /// Returns the event timestamp in microseconds.
    #[must_use]
    pub fn time_us(&self) -> u64 {
        match self {
            Self::Commit(c) => c.time_us,
            Self::Heartbeat { time_us } | Self::Other { time_us, .. } => *time_us,
        }
    }

    /// Returns a reference to the inner [`JetstreamCommit`] if this event is a commit.
    #[must_use]
    pub fn as_commit(&self) -> Option<&JetstreamCommit> {
        match self {
            Self::Commit(c) => Some(c),
            _ => None,
        }
    }
}

/// Parses a raw Jetstream WebSocket text frame into a strongly typed [`JetstreamEvent`].
///
/// This function is strictly non-panicking and resilient against corrupted JSON,
/// non-commit events, heartbeat frames, truncated streams, and unknown operations.
///
/// Returns `None` if the frame is completely unparseable or irrelevant.
#[must_use]
pub fn parse_jetstream_frame(text: &str) -> Option<JetstreamEvent> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let msg: RawJetstreamMessage = match serde_json::from_str(trimmed) {
        Ok(m) => m,
        Err(err) => {
            tracing::debug!(error = %err, "Discarding unparseable Jetstream JSON frame");
            return None;
        }
    };

    let time_us = msg.time_us.unwrap_or(0);

    // Case 1: Commit event
    if msg.kind.as_deref() == Some("commit") {
        let did = match msg.did {
            Some(d) if !d.trim().is_empty() => d,
            _ => {
                tracing::debug!("Discarding commit frame with missing DID");
                return if time_us > 0 {
                    Some(JetstreamEvent::Heartbeat { time_us })
                } else {
                    None
                };
            }
        };

        let commit = match msg.commit {
            Some(c) => c,
            None => {
                tracing::debug!("Commit event missing 'commit' payload");
                return if time_us > 0 {
                    Some(JetstreamEvent::Heartbeat { time_us })
                } else {
                    None
                };
            }
        };

        if commit.collection.trim().is_empty() || commit.rkey.trim().is_empty() {
            tracing::debug!("Discarding commit with empty collection or rkey");
            return if time_us > 0 {
                Some(JetstreamEvent::Heartbeat { time_us })
            } else {
                None
            };
        }

        let operation = match CommitOperation::from_str_loose(&commit.operation) {
            Some(op) => op,
            None => {
                tracing::debug!(
                    operation = %commit.operation,
                    "Discarding commit with unknown operation"
                );
                return if time_us > 0 {
                    Some(JetstreamEvent::Heartbeat { time_us })
                } else {
                    None
                };
            }
        };

        return Some(JetstreamEvent::Commit(JetstreamCommit {
            did,
            time_us,
            collection: commit.collection,
            rkey: commit.rkey,
            operation,
            cid: commit.cid,
            record: commit.record,
        }));
    }

    // Case 2: Heartbeat or timestamp-only frame
    if msg.kind.is_none() && time_us > 0 {
        return Some(JetstreamEvent::Heartbeat { time_us });
    }

    // Case 3: Other recognized frame types (identity, account) with time_us
    if let Some(kind) = msg.kind {
        if time_us > 0 {
            return Some(JetstreamEvent::Other {
                kind,
                did: msg.did,
                time_us,
            });
        }
    }

    None
}

/// Parses the microsecond timestamp from a raw Jetstream frame without requiring a full commit structure.
#[must_use]
pub fn parse_frame_timestamp(text: &str) -> Option<u64> {
    parse_jetstream_frame(text).map(|evt| evt.time_us())
}

/// Parses a raw Jetstream frame directly into an optional [`JetstreamCommit`].
#[must_use]
pub fn parse_jetstream_commit(text: &str) -> Option<JetstreamCommit> {
    match parse_jetstream_frame(text) {
        Some(JetstreamEvent::Commit(commit)) => Some(commit),
        _ => None,
    }
}

/// Applies a parsed [`JetstreamCommit`] to the SQLite [`RecordStore`].
///
/// - For [`CommitOperation::Create`] and [`CommitOperation::Update`], the record is upserted,
///   unmarking any previous soft-deletion tombstone and emitting an `Upsert` change notification.
/// - For [`CommitOperation::Delete`], the record is soft-deleted (`is_deleted = 1`) and a `Delete`
///   notification is emitted across the broadcast bus.
///
/// # Errors
/// Returns [`crate::error::SkybaseError::Storage`] or [`crate::error::SkybaseError::Serialization`] if the SQLite mutation fails.
pub fn sync_commit_to_store(store: &RecordStore, commit: &JetstreamCommit) -> Result<()> {
    let uri = commit.uri();

    match commit.operation {
        CommitOperation::Create | CommitOperation::Update => {
            let Some(input) = commit.to_record_input(None) else {
                return Err(crate::error::SkybaseError::Index(format!(
                    "Missing record payload for commit '{uri}'"
                )));
            };

            store.upsert_record(&input)?;
        }
        CommitOperation::Delete => {
            store.soft_delete_record(&uri)?;
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_commit_operation_loose_parsing() {
        assert_eq!(
            CommitOperation::from_str_loose("create"),
            Some(CommitOperation::Create)
        );
        assert_eq!(
            CommitOperation::from_str_loose("CREATE"),
            Some(CommitOperation::Create)
        );
        assert_eq!(
            CommitOperation::from_str_loose("Update"),
            Some(CommitOperation::Update)
        );
        assert_eq!(
            CommitOperation::from_str_loose("DELETE"),
            Some(CommitOperation::Delete)
        );
        assert_eq!(CommitOperation::from_str_loose("unknown"), None);
    }

    #[test]
    fn test_parse_valid_commit_frame() {
        let json_str = r#"{
            "did": "did:plc:alice",
            "time_us": 1710000000123456,
            "kind": "commit",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "post1",
                "cid": "bafyrei1",
                "record": { "text": "Hello world" }
            }
        }"#;

        let event = parse_jetstream_frame(json_str).expect("should parse");
        match event {
            JetstreamEvent::Commit(c) => {
                assert_eq!(c.did, "did:plc:alice");
                assert_eq!(c.time_us, 1710000000123456);
                assert_eq!(c.collection, "app.bsky.feed.post");
                assert_eq!(c.rkey, "post1");
                assert_eq!(c.operation, CommitOperation::Create);
                assert_eq!(c.cid.as_deref(), Some("bafyrei1"));
                assert_eq!(c.uri(), "at://did:plc:alice/app.bsky.feed.post/post1");
                let input = c.to_record_input(None).expect("to record input");
                assert_eq!(input.indexed_at, 1710000000123456);
            }
            _ => panic!("Expected commit event"),
        }
    }

    #[test]
    fn test_parse_heartbeat_frame() {
        let json_str = r#"{"time_us": 1720000000999999}"#;
        let event = parse_jetstream_frame(json_str).expect("should parse");
        match event {
            JetstreamEvent::Heartbeat { time_us } => {
                assert_eq!(time_us, 1720000000999999);
            }
            _ => panic!("Expected heartbeat"),
        }
    }

    #[test]
    fn test_parse_malformed_json_graceful() {
        assert_eq!(parse_jetstream_frame(""), None);
        assert_eq!(parse_jetstream_frame("   "), None);
        assert_eq!(parse_jetstream_frame("{ corrupt json:"), None);
        assert_eq!(parse_jetstream_frame("42"), None);
    }

    #[test]
    fn test_sync_commit_to_store() {
        let store = RecordStore::open_in_memory().expect("open store");
        let commit = JetstreamCommit {
            did: "did:plc:bob".to_string(),
            time_us: 1715000000000000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "r1".to_string(),
            operation: CommitOperation::Create,
            cid: Some("cid1".to_string()),
            record: Some(json!({ "text": "Testing sync" })),
        };

        sync_commit_to_store(&store, &commit).expect("sync commit");
        let rec = store
            .get_record("at://did:plc:bob/app.bsky.feed.post/r1")
            .expect("get record")
            .expect("record found");
        assert_eq!(rec.cid, "cid1");
        assert_eq!(rec.record_json["text"], "Testing sync");

        // Delete commit
        let delete_commit = JetstreamCommit {
            did: "did:plc:bob".to_string(),
            time_us: 1715000001000000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "r1".to_string(),
            operation: CommitOperation::Delete,
            cid: None,
            record: None,
        };
        sync_commit_to_store(&store, &delete_commit).expect("sync delete");
        assert_eq!(
            store
                .get_record("at://did:plc:bob/app.bsky.feed.post/r1")
                .expect("get record"),
            None
        );
        let rec_after = store
            .get_record_including_deleted("at://did:plc:bob/app.bsky.feed.post/r1")
            .expect("get record including deleted")
            .expect("record found");
        assert!(rec_after.is_deleted);
    }
}
