//! In-memory pub/sub broadcast bus for record change notifications.
//!
//! Provides real-time reactive notifications whenever AT Protocol records are created,
//! updated, or soft-deleted in the underlying SQLite store.

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::{self, Receiver, Sender};

use crate::error::{Result, SkybaseError};
use crate::index::store::RecordRow;

/// Default capacity for the in-memory broadcast ring buffer.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

/// Notification event emitted whenever a record is created, updated, or soft-deleted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeNotification {
    /// A record was created or updated.
    Upsert(RecordRow),
    /// A record was soft-deleted.
    Delete {
        /// Canonical AT-URI of the deleted record.
        uri: String,
        /// Repository DID.
        did: String,
        /// Lexicon collection NSID.
        collection: String,
        /// Record key.
        rkey: String,
    },
}

impl ChangeNotification {
    /// Returns the canonical AT-URI for this change event.
    #[must_use]
    pub fn uri(&self) -> &str {
        match self {
            Self::Upsert(row) => &row.uri,
            Self::Delete { uri, .. } => uri.as_str(),
        }
    }

    /// Returns the repository DID associated with this record.
    #[must_use]
    pub fn did(&self) -> &str {
        match self {
            Self::Upsert(row) => &row.did,
            Self::Delete { did, .. } => did.as_str(),
        }
    }

    /// Returns the Lexicon collection NSID.
    #[must_use]
    pub fn collection(&self) -> &str {
        match self {
            Self::Upsert(row) => &row.collection,
            Self::Delete { collection, .. } => collection.as_str(),
        }
    }

    /// Returns the record key (`rkey`).
    #[must_use]
    pub fn rkey(&self) -> &str {
        match self {
            Self::Upsert(row) => &row.rkey,
            Self::Delete { rkey, .. } => rkey.as_str(),
        }
    }

    /// Returns `true` if this event is an `Upsert`.
    #[must_use]
    pub fn is_upsert(&self) -> bool {
        matches!(self, Self::Upsert(_))
    }

    /// Returns `true` if this event is a `Delete`.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    /// Returns a reference to the inner [`RecordRow`] if this is an `Upsert`.
    #[must_use]
    pub fn record(&self) -> Option<&RecordRow> {
        match self {
            Self::Upsert(row) => Some(row),
            Self::Delete { .. } => None,
        }
    }

    /// Returns `true` if the event matches the specified collection NSID.
    #[must_use]
    pub fn matches_collection(&self, collection: &str) -> bool {
        self.collection() == collection
    }

    /// Returns `true` if the event matches the specified repository DID.
    #[must_use]
    pub fn matches_did(&self, did: &str) -> bool {
        self.did() == did
    }
}

/// In-memory pub/sub broadcast bus for record change notifications.
#[derive(Clone, Debug)]
pub struct BroadcastBus {
    sender: Sender<ChangeNotification>,
}

impl Default for BroadcastBus {
    fn default() -> Self {
        let (sender, _rx) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        Self { sender }
    }
}

/// Maximum allowed capacity for the in-memory broadcast ring buffer (65,536 events).
pub const MAX_BROADCAST_CAPACITY: usize = 65_536;

impl BroadcastBus {
    /// Creates a new broadcast bus with the given channel ring buffer capacity.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Config`] if `capacity` is 0 or exceeds [`MAX_BROADCAST_CAPACITY`].
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 || capacity > MAX_BROADCAST_CAPACITY {
            return Err(SkybaseError::Config(format!(
                "Broadcast bus capacity must be between 1 and {MAX_BROADCAST_CAPACITY}, got {capacity}"
            )));
        }
        let (sender, _rx) = broadcast::channel(capacity);
        Ok(Self { sender })
    }

    /// Subscribes to the broadcast channel, returning a new [`Receiver`].
    ///
    /// The receiver will receive all events published after subscription.
    #[must_use]
    pub fn subscribe(&self) -> Receiver<ChangeNotification> {
        self.sender.subscribe()
    }

    /// Broadcasts a [`ChangeNotification`] to all active subscribers.
    ///
    /// Delivery is non-blocking and will not fail if there are zero active subscribers.
    ///
    /// # Returns
    /// The number of active receivers that received the notification (0 if none were active).
    pub fn publish(&self, notification: ChangeNotification) -> usize {
        self.sender.send(notification).unwrap_or_default()
    }

    /// Broadcasts an `Upsert` change notification.
    pub fn publish_upsert(&self, row: RecordRow) -> usize {
        self.publish(ChangeNotification::Upsert(row))
    }

    /// Broadcasts a `Delete` change notification.
    pub fn publish_delete(
        &self,
        uri: impl Into<String>,
        did: impl Into<String>,
        collection: impl Into<String>,
        rkey: impl Into<String>,
    ) -> usize {
        self.publish(ChangeNotification::Delete {
            uri: uri.into(),
            did: did.into(),
            collection: collection.into(),
            rkey: rkey.into(),
        })
    }

    /// Returns the number of active receivers currently subscribed.
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    fn sample_record_row(rkey: &str) -> RecordRow {
        RecordRow {
            uri: format!("at://did:plc:test/app.bsky.feed.post/{rkey}"),
            cid: "bafyreitestcid".to_string(),
            did: "did:plc:test".to_string(),
            collection: "app.bsky.feed.post".to_string(),
            rkey: rkey.to_string(),
            record_json: serde_json::json!({ "text": "hello skybase" }),
            indexed_at: 1_700_000_000,
            is_deleted: false,
        }
    }

    #[test]
    fn test_zero_capacity_error() {
        let bus_res = BroadcastBus::new(0);
        assert!(matches!(bus_res, Err(SkybaseError::Config(_))));
    }

    #[test]
    fn test_zero_subscriber_delivery() {
        let bus = BroadcastBus::new(16).unwrap();
        assert_eq!(bus.receiver_count(), 0);

        let row = sample_record_row("post1");
        let delivered = bus.publish_upsert(row);
        assert_eq!(
            delivered, 0,
            "Zero active subscribers must return 0 without panicking"
        );
    }

    #[tokio::test]
    async fn test_single_subscriber_delivery() {
        let bus = BroadcastBus::new(16).unwrap();
        let mut rx = bus.subscribe();
        assert_eq!(bus.receiver_count(), 1);

        let row = sample_record_row("post1");
        let delivered = bus.publish_upsert(row.clone());
        assert_eq!(delivered, 1);

        let event = rx.recv().await.unwrap();
        assert_eq!(event, ChangeNotification::Upsert(row.clone()));
        assert_eq!(event.uri(), "at://did:plc:test/app.bsky.feed.post/post1");
        assert_eq!(event.did(), "did:plc:test");
        assert_eq!(event.collection(), "app.bsky.feed.post");
        assert_eq!(event.rkey(), "post1");
        assert!(event.is_upsert());
        assert!(!event.is_delete());
        assert_eq!(event.record(), Some(&row));
        assert!(event.matches_collection("app.bsky.feed.post"));
        assert!(!event.matches_collection("app.bsky.graph.follow"));
        assert!(event.matches_did("did:plc:test"));
        assert!(!event.matches_did("did:plc:other"));
    }

    #[tokio::test]
    async fn test_multi_subscriber_fanout() {
        let bus = BroadcastBus::new(16).unwrap();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        let mut rx3 = bus.subscribe();
        assert_eq!(bus.receiver_count(), 3);

        let delivered = bus.publish_delete(
            "at://did:plc:test/app.bsky.feed.post/del1",
            "did:plc:test",
            "app.bsky.feed.post",
            "del1",
        );
        assert_eq!(delivered, 3);

        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            let event = rx.recv().await.unwrap();
            assert_eq!(
                event,
                ChangeNotification::Delete {
                    uri: "at://did:plc:test/app.bsky.feed.post/del1".to_string(),
                    did: "did:plc:test".to_string(),
                    collection: "app.bsky.feed.post".to_string(),
                    rkey: "del1".to_string(),
                }
            );
            assert!(event.is_delete());
            assert!(!event.is_upsert());
            assert_eq!(event.record(), None);
        }
    }

    #[tokio::test]
    async fn test_lagged_receiver_non_blocking() {
        let bus = BroadcastBus::new(4).unwrap();
        let mut slow_rx = bus.subscribe();

        for i in 0..8 {
            let row = sample_record_row(&format!("key_{i}"));
            bus.publish_upsert(row);
        }

        match slow_rx.recv().await {
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                assert_eq!(skipped, 4, "Must indicate 4 messages were skipped");
            }
            other => panic!("Expected RecvError::Lagged, got {other:?}"),
        }

        let next_event = slow_rx.recv().await.unwrap();
        assert_eq!(next_event.rkey(), "key_4");
    }

    #[tokio::test]
    async fn test_closed_on_drop() {
        let bus = BroadcastBus::new(8).unwrap();
        let mut rx = bus.subscribe();
        drop(bus);

        let result = rx.recv().await;
        assert_eq!(result, Err(broadcast::error::RecvError::Closed));
    }

    #[test]
    fn test_change_notification_serde_roundtrip() {
        let row = sample_record_row("test_serde");
        let notif = ChangeNotification::Upsert(row);

        let serialized = serde_json::to_string(&notif).unwrap();
        let deserialized: ChangeNotification = serde_json::from_str(&serialized).unwrap();
        assert_eq!(notif, deserialized);
    }
}
