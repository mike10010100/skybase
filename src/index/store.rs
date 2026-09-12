//! Embedded SQLite record store providing WAL storage, JSON1 indexing,
//! and reactive change notifications.
//!
//! Handles database initialization, schema migration, atomic upserts, soft-deletions,
//! point lookups, and raw SQL queries for the query builder.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::{Result, SkybaseError};
use crate::index::broadcast::{BroadcastBus, ChangeNotification, DEFAULT_BROADCAST_CAPACITY};
use crate::index::query::QueryBuilder;

/// Input payload for creating or updating a record in [`RecordStore`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordInput {
    /// Canonical AT-URI (`at://{did}/{collection}/{rkey}`).
    pub uri: String,
    /// Content identifier CID string (e.g., `bafyreih3...`).
    pub cid: String,
    /// Decentralized identifier of the repository owner (`did:plc:...`).
    pub did: String,
    /// Lexicon NSID collection name (e.g., `app.bsky.feed.post`).
    pub collection: String,
    /// Record key within the collection (e.g., `3k6babc123`).
    pub rkey: String,
    /// Arbitrary Lexicon JSON payload.
    pub record_json: serde_json::Value,
    /// Monotonic timestamp in microseconds or milliseconds.
    pub indexed_at: u64,
}

impl RecordInput {
    /// Constructs a new [`RecordInput`], automatically generating the canonical `uri`
    /// from `did`, `collection`, and `rkey`.
    #[must_use]
    pub fn new(
        did: impl Into<String>,
        collection: impl Into<String>,
        rkey: impl Into<String>,
        cid: impl Into<String>,
        record_json: serde_json::Value,
        indexed_at: u64,
    ) -> Self {
        let did = did.into();
        let collection = collection.into();
        let rkey = rkey.into();
        let uri = format!("at://{did}/{collection}/{rkey}");
        Self {
            uri,
            cid: cid.into(),
            did,
            collection,
            rkey,
            record_json,
            indexed_at,
        }
    }

    /// Constructs a [`RecordInput`] with an explicit pre-formatted URI.
    #[must_use]
    pub fn with_uri(
        uri: impl Into<String>,
        cid: impl Into<String>,
        did: impl Into<String>,
        collection: impl Into<String>,
        rkey: impl Into<String>,
        record_json: serde_json::Value,
        indexed_at: u64,
    ) -> Self {
        Self {
            uri: uri.into(),
            cid: cid.into(),
            did: did.into(),
            collection: collection.into(),
            rkey: rkey.into(),
            record_json,
            indexed_at,
        }
    }
}

/// A canonical record row retrieved from [`RecordStore`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordRow {
    /// Canonical AT-URI (`at://{did}/{collection}/{rkey}`).
    pub uri: String,
    /// Content identifier CID string.
    pub cid: String,
    /// Decentralized identifier of the repository owner.
    pub did: String,
    /// Lexicon NSID collection name.
    pub collection: String,
    /// Record key within the collection.
    pub rkey: String,
    /// Arbitrary Lexicon JSON payload.
    pub record_json: serde_json::Value,
    /// Ingestion timestamp.
    pub indexed_at: u64,
    /// Soft-delete tombstone status (`true` if deleted).
    pub is_deleted: bool,
}

/// Configuration options for initializing a [`RecordStore`].
#[derive(Debug, Clone)]
pub struct RecordStoreConfig {
    /// Optional filesystem path to SQLite database. If `None`, `:memory:` is used.
    pub path: Option<PathBuf>,
    /// Capacity of the in-memory broadcast channel. Default: 1024.
    pub broadcast_capacity: usize,
    /// SQLite busy wait timeout in milliseconds. Default: 5000.
    pub busy_timeout_ms: u32,
}

impl Default for RecordStoreConfig {
    fn default() -> Self {
        Self {
            path: None,
            broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
            busy_timeout_ms: 5000,
        }
    }
}

impl RecordStoreConfig {
    /// Creates an in-memory database configuration.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Creates a persistent database configuration pointing to a file path.
    #[must_use]
    pub fn persistent(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            ..Self::default()
        }
    }
}

/// Embedded SQLite record store providing WAL storage, JSON1 indexing,
/// and reactive change notifications.
#[derive(Clone)]
pub struct RecordStore {
    inner: Arc<RecordStoreInner>,
}

struct RecordStoreInner {
    conn: Mutex<rusqlite::Connection>,
    bus: BroadcastBus,
}

impl RecordStore {
    /// Opens or creates a persistent SQLite record store at the given path.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] if SQLite fails to open the database file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let config = RecordStoreConfig::persistent(path.as_ref());
        Self::with_config(config)
    }

    /// Opens or creates a persistent SQLite record store at the given path (alias for [`RecordStore::open`]).
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] if SQLite fails to open the database file.
    pub fn open_file_backed(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path)
    }

    /// Opens an isolated in-memory SQLite record store (ideal for unit and integration testing).
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] if SQLite initialization fails.
    pub fn open_in_memory() -> Result<Self> {
        let config = RecordStoreConfig::in_memory();
        Self::with_config(config)
    }

    /// Opens a record store with custom configuration.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] or [`SkybaseError::Config`] on initialization failure.
    pub fn with_config(config: RecordStoreConfig) -> Result<Self> {
        let conn = match &config.path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            SkybaseError::Storage(format!(
                                "Failed to create database directory: {e}"
                            ))
                        })?;
                    }
                }
                rusqlite::Connection::open(path)?
            }
            None => rusqlite::Connection::open_in_memory()?,
        };

        Self::apply_pragmas(&conn, config.busy_timeout_ms)?;
        Self::init_schema(&conn)?;

        let bus = BroadcastBus::new(config.broadcast_capacity)?;

        Ok(Self {
            inner: Arc::new(RecordStoreInner {
                conn: Mutex::new(conn),
                bus,
            }),
        })
    }

    fn apply_pragmas(conn: &rusqlite::Connection, busy_timeout_ms: u32) -> Result<()> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "busy_timeout", busy_timeout_ms)?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.pragma_update(None, "mmap_size", 268435456_i64)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(())
    }

    fn init_schema(conn: &rusqlite::Connection) -> Result<()> {
        conn.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS records (
                 uri TEXT PRIMARY KEY,
                 cid TEXT NOT NULL,
                 did TEXT NOT NULL,
                 collection TEXT NOT NULL,
                 rkey TEXT NOT NULL,
                 record_json TEXT NOT NULL,
                 indexed_at INTEGER NOT NULL,
                 is_deleted INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS idx_records_collection ON records(collection);
             CREATE INDEX IF NOT EXISTS idx_records_did ON records(did);
             CREATE INDEX IF NOT EXISTS idx_records_indexed_at ON records(indexed_at);
             CREATE INDEX IF NOT EXISTS idx_records_collection_active ON records(collection, is_deleted, indexed_at DESC);
             COMMIT;",
        )?;
        Ok(())
    }

    /// Inserts or updates a record atomically, unmarking any previous soft-delete,
    /// and emits an `Upsert` change notification if the record was inserted or updated.
    ///
    /// Stale updates (where `input.indexed_at < records.indexed_at`) are discarded to prevent
    /// out-of-order firehose event delivery from clobbering newer state.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] or [`SkybaseError::Serialization`] on error.
    pub fn upsert_record(&self, input: &RecordInput) -> Result<()> {
        let record_json_str =
            serde_json::to_string(&input.record_json).map_err(SkybaseError::Serialization)?;
        let indexed_at_i64 = i64::try_from(input.indexed_at)
            .map_err(|e| SkybaseError::Index(format!("indexed_at out of i64 range: {e}")))?;

        let rows_affected = {
            let conn = self.inner.conn.lock();
            let mut stmt = conn.prepare_cached(
                "INSERT INTO records (uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)
                 ON CONFLICT(uri) DO UPDATE SET
                     cid = excluded.cid,
                     did = excluded.did,
                     collection = excluded.collection,
                     rkey = excluded.rkey,
                     record_json = excluded.record_json,
                     indexed_at = excluded.indexed_at,
                     is_deleted = 0
                 WHERE excluded.indexed_at >= records.indexed_at;",
            )?;

            stmt.execute(rusqlite::params![
                input.uri,
                input.cid,
                input.did,
                input.collection,
                input.rkey,
                record_json_str,
                indexed_at_i64,
            ])?
        };

        if rows_affected > 0 {
            let row = RecordRow {
                uri: input.uri.clone(),
                cid: input.cid.clone(),
                did: input.did.clone(),
                collection: input.collection.clone(),
                rkey: input.rkey.clone(),
                record_json: input.record_json.clone(),
                indexed_at: input.indexed_at,
                is_deleted: false,
            };
            self.inner.bus.publish_upsert(row);
        }

        Ok(())
    }

    /// Upserts a slice of records atomically in a single SQLite transaction.
    ///
    /// Only records that successfully insert or advance state emit `Upsert` notifications.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] or [`SkybaseError::Serialization`] on error.
    pub fn upsert_records_batch(&self, records: &[RecordInput]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut rows_to_notify = Vec::with_capacity(records.len());

        {
            let mut conn = self.inner.conn.lock();
            let tx = conn.transaction()?;

            {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO records (uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)
                     ON CONFLICT(uri) DO UPDATE SET
                         cid = excluded.cid,
                         did = excluded.did,
                         collection = excluded.collection,
                         rkey = excluded.rkey,
                         record_json = excluded.record_json,
                         indexed_at = excluded.indexed_at,
                         is_deleted = 0
                     WHERE excluded.indexed_at >= records.indexed_at;",
                )?;

                for input in records {
                    let record_json_str = serde_json::to_string(&input.record_json)
                        .map_err(SkybaseError::Serialization)?;
                    let indexed_at_i64 = i64::try_from(input.indexed_at).map_err(|e| {
                        SkybaseError::Index(format!("indexed_at out of i64 range: {e}"))
                    })?;

                    let rows_affected = stmt.execute(rusqlite::params![
                        input.uri,
                        input.cid,
                        input.did,
                        input.collection,
                        input.rkey,
                        record_json_str,
                        indexed_at_i64,
                    ])?;

                    if rows_affected > 0 {
                        rows_to_notify.push(RecordRow {
                            uri: input.uri.clone(),
                            cid: input.cid.clone(),
                            did: input.did.clone(),
                            collection: input.collection.clone(),
                            rkey: input.rkey.clone(),
                            record_json: input.record_json.clone(),
                            indexed_at: input.indexed_at,
                            is_deleted: false,
                        });
                    }
                }
            }

            tx.commit()?;
        }

        for row in rows_to_notify {
            self.inner.bus.publish_upsert(row);
        }

        Ok(())
    }

    /// Soft-deletes multiple records atomically in a single SQLite transaction.
    ///
    /// Emits `Delete` notifications only for records that were actively transitioned to deleted.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] on database failure.
    pub fn soft_delete_records_batch(&self, uris: &[String]) -> Result<()> {
        if uris.is_empty() {
            return Ok(());
        }

        let mut deleted_to_notify = Vec::new();

        {
            let mut conn = self.inner.conn.lock();
            let tx = conn.transaction()?;

            {
                let mut stmt = tx.prepare_cached(
                    "UPDATE records
                     SET is_deleted = 1
                     WHERE uri = ?1 AND is_deleted = 0
                     RETURNING did, collection, rkey;",
                )?;

                for uri in uris {
                    let mut rows = stmt.query(rusqlite::params![uri])?;
                    if let Some(row) = rows.next()? {
                        let did: String = row.get(0)?;
                        let collection: String = row.get(1)?;
                        let rkey: String = row.get(2)?;
                        deleted_to_notify.push((uri.clone(), did, collection, rkey));
                    }
                }
            }

            tx.commit()?;
        }

        for (uri, did, collection, rkey) in deleted_to_notify {
            self.inner.bus.publish_delete(uri, did, collection, rkey);
        }

        Ok(())
    }

    /// Soft-deletes a record by setting `is_deleted = 1` and emits a `Delete` notification.
    ///
    /// Only emits a `Delete` notification if the record was active and transitioned to deleted.
    /// Repeated calls for non-existent or already-deleted records are idempotent no-ops.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] on database failure.
    pub fn soft_delete_record(&self, uri: &str) -> Result<()> {
        let deleted_meta: Option<(String, String, String)> = {
            let conn = self.inner.conn.lock();
            let mut stmt = conn.prepare_cached(
                "UPDATE records
                 SET is_deleted = 1
                 WHERE uri = ?1 AND is_deleted = 0
                 RETURNING did, collection, rkey;",
            )?;

            let mut rows = stmt.query(rusqlite::params![uri])?;

            if let Some(row) = rows.next()? {
                let did: String = row.get(0)?;
                let collection: String = row.get(1)?;
                let rkey: String = row.get(2)?;
                Some((did, collection, rkey))
            } else {
                None
            }
        };

        if let Some((did, collection, rkey)) = deleted_meta {
            self.inner.bus.publish_delete(uri, did, collection, rkey);
        }

        Ok(())
    }

    /// Retrieves an active (non-deleted) record by its canonical URI.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] or [`SkybaseError::Serialization`] on error.
    pub fn get_record(&self, uri: &str) -> Result<Option<RecordRow>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare_cached(
            "SELECT uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted
             FROM records
             WHERE uri = ?1 AND is_deleted = 0;",
        )?;

        let mut rows = stmt.query(rusqlite::params![uri])?;
        if let Some(row) = rows.next()? {
            let record = map_record_row(row)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Retrieves a record by URI regardless of its soft-delete status.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] or [`SkybaseError::Serialization`] on error.
    pub fn get_record_including_deleted(&self, uri: &str) -> Result<Option<RecordRow>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare_cached(
            "SELECT uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted
             FROM records
             WHERE uri = ?1;",
        )?;

        let mut rows = stmt.query(rusqlite::params![uri])?;
        if let Some(row) = rows.next()? {
            let record = map_record_row(row)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Physically deletes a record from the database (hard delete).
    ///
    /// Returns `true` if a record was removed, or `false` if it did not exist.
    /// Emits a `Delete` notification if a record was removed.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] on database failure.
    pub fn hard_delete_record(&self, uri: &str) -> Result<bool> {
        let deleted_meta: Option<(String, String, String)> = {
            let conn = self.inner.conn.lock();
            let mut stmt = conn.prepare_cached(
                "DELETE FROM records WHERE uri = ?1 RETURNING did, collection, rkey;",
            )?;
            let mut rows = stmt.query(rusqlite::params![uri])?;
            if let Some(row) = rows.next()? {
                let did: String = row.get(0)?;
                let collection: String = row.get(1)?;
                let rkey: String = row.get(2)?;
                Some((did, collection, rkey))
            } else {
                None
            }
        };

        if let Some((did, collection, rkey)) = deleted_meta {
            self.inner.bus.publish_delete(uri, did, collection, rkey);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Subscribes to the live change notification broadcast bus.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeNotification> {
        self.inner.bus.subscribe()
    }

    /// Returns a reference to the internal [`BroadcastBus`].
    #[must_use]
    pub fn broadcast_bus(&self) -> &BroadcastBus {
        &self.inner.bus
    }

    /// Initiates a structured [`QueryBuilder`] scoped to the given collection NSID.
    #[must_use]
    pub fn query(&self, collection: impl Into<String>) -> QueryBuilder<'_> {
        QueryBuilder::new(self, collection)
    }

    /// Initiates a structured [`QueryBuilder`] scoped to the given collection NSID (alias for [`query`]).
    #[must_use]
    pub fn collection(&self, collection: impl Into<String>) -> QueryBuilder<'_> {
        self.query(collection)
    }

    /// Executes a raw SQL query with parameters and returns hydrated [`RecordRow`] items.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] or [`SkybaseError::Serialization`] on error.
    pub fn query_raw(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
    ) -> Result<Vec<RecordRow>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push(map_record_row(row)?);
        }
        Ok(results)
    }

    /// Executes a closure with a reference to the underlying SQLite connection.
    ///
    /// # Errors
    /// Returns the result of `f`, or [`SkybaseError::Storage`] on internal error.
    pub fn with_conn<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R>,
    {
        let conn = self.inner.conn.lock();
        f(&conn)
    }
}

/// Helper function to parse `did`, `collection`, and `rkey` from a canonical `at://` URI.
#[must_use]
pub fn parse_at_uri(uri: &str) -> Option<(String, String, String)> {
    let stripped = uri.strip_prefix("at://")?;
    let mut parts = stripped.split('/');
    let did = parts.next()?;
    let collection = parts.next()?;
    let rkey = parts.next()?;
    if parts.next().is_some() || did.is_empty() || collection.is_empty() || rkey.is_empty() {
        return None;
    }
    Some((did.to_string(), collection.to_string(), rkey.to_string()))
}

/// Maps a rusqlite row containing the 8 canonical record columns to a [`RecordRow`].
pub(crate) fn map_record_row(row: &rusqlite::Row<'_>) -> Result<RecordRow> {
    let uri: String = row.get(0)?;
    let cid: String = row.get(1)?;
    let did: String = row.get(2)?;
    let collection: String = row.get(3)?;
    let rkey: String = row.get(4)?;
    let record_json_str: String = row.get(5)?;
    let indexed_at_i64: i64 = row.get(6)?;
    let is_deleted_i64: i64 = row.get(7)?;

    let record_json: serde_json::Value =
        serde_json::from_str(&record_json_str).map_err(SkybaseError::Serialization)?;

    let indexed_at = u64::try_from(indexed_at_i64)
        .map_err(|e| SkybaseError::Index(format!("negative timestamp in database: {e}")))?;

    Ok(RecordRow {
        uri,
        cid,
        did,
        collection,
        rkey,
        record_json,
        indexed_at,
        is_deleted: is_deleted_i64 != 0,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_open_in_memory_and_schema() {
        let store = RecordStore::open_in_memory().unwrap();
        let table_exists: bool = store
            .with_conn(|conn| {
                let count: i64 = conn.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='records'",
                    [],
                    |r| r.get(0),
                )?;
                Ok(count == 1)
            })
            .unwrap();
        assert!(table_exists);

        let index_count: i64 = store
            .with_conn(|conn| {
                let count: i64 = conn.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='records'",
                    [],
                    |r| r.get(0),
                )?;
                Ok(count)
            })
            .unwrap();
        assert!(index_count >= 3);
    }

    #[test]
    fn test_upsert_and_get_record() {
        let store = RecordStore::open_in_memory().unwrap();
        let input = RecordInput::new(
            "did:plc:alice",
            "app.bsky.feed.post",
            "3k6b123",
            "bafyreih1",
            serde_json::json!({"text": "Hello Bluesky!"}),
            1_700_000_000,
        );

        store.upsert_record(&input).unwrap();

        let row = store
            .get_record(&input.uri)
            .unwrap()
            .expect("Record should exist");
        assert_eq!(row.uri, "at://did:plc:alice/app.bsky.feed.post/3k6b123");
        assert_eq!(row.cid, "bafyreih1");
        assert_eq!(row.did, "did:plc:alice");
        assert_eq!(row.collection, "app.bsky.feed.post");
        assert_eq!(row.rkey, "3k6b123");
        assert_eq!(row.record_json["text"], "Hello Bluesky!");
        assert_eq!(row.indexed_at, 1_700_000_000);
        assert!(!row.is_deleted);
    }

    #[test]
    fn test_upsert_conflict_update_and_resurrection() {
        let store = RecordStore::open_in_memory().unwrap();
        let input = RecordInput::new(
            "did:plc:bob",
            "app.bsky.feed.post",
            "1",
            "cid1",
            serde_json::json!({"v": 1}),
            100,
        );
        store.upsert_record(&input).unwrap();

        // Soft delete
        store.soft_delete_record(&input.uri).unwrap();
        assert!(store.get_record(&input.uri).unwrap().is_none());
        assert!(
            store
                .get_record_including_deleted(&input.uri)
                .unwrap()
                .unwrap()
                .is_deleted
        );

        // Re-upsert (resurrection)
        let input2 = RecordInput::new(
            "did:plc:bob",
            "app.bsky.feed.post",
            "1",
            "cid2",
            serde_json::json!({"v": 2}),
            200,
        );
        store.upsert_record(&input2).unwrap();

        let row = store
            .get_record(&input.uri)
            .unwrap()
            .expect("Resurrected record should exist");
        assert_eq!(row.cid, "cid2");
        assert_eq!(row.record_json["v"], 2);
        assert_eq!(row.indexed_at, 200);
        assert!(!row.is_deleted);
    }

    #[test]
    fn test_broadcast_notifications() {
        let store = RecordStore::open_in_memory().unwrap();
        let mut rx = store.subscribe();

        let input = RecordInput::new(
            "did:plc:carol",
            "app.bsky.feed.post",
            "xyz",
            "cid1",
            serde_json::json!({}),
            10,
        );
        store.upsert_record(&input).unwrap();

        let event = rx.try_recv().expect("Should receive upsert notification");
        match event {
            ChangeNotification::Upsert(row) => assert_eq!(row.uri, input.uri),
            _ => panic!("Expected Upsert event"),
        }

        store.soft_delete_record(&input.uri).unwrap();
        let event2 = rx.try_recv().expect("Should receive delete notification");
        match event2 {
            ChangeNotification::Delete {
                uri,
                did,
                collection,
                rkey,
            } => {
                assert_eq!(uri, input.uri);
                assert_eq!(did, "did:plc:carol");
                assert_eq!(collection, "app.bsky.feed.post");
                assert_eq!(rkey, "xyz");
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[test]
    fn test_zero_subscribers_safe() {
        let store = RecordStore::open_in_memory().unwrap();
        let input = RecordInput::new(
            "did:plc:dave",
            "app.bsky.feed.post",
            "1",
            "cid",
            serde_json::json!({}),
            1,
        );
        assert!(store.upsert_record(&input).is_ok());
        assert!(store.soft_delete_record(&input.uri).is_ok());
    }

    #[test]
    fn test_batch_upsert() {
        let store = RecordStore::open_in_memory().unwrap();
        let mut records = Vec::new();
        for i in 0..50 {
            records.push(RecordInput::new(
                "did:plc:bulk",
                "app.bsky.feed.post",
                format!("{i}"),
                format!("cid{i}"),
                serde_json::json!({"seq": i}),
                1000 + i,
            ));
        }

        store.upsert_records_batch(&records).unwrap();

        for i in 0..50 {
            let uri = format!("at://did:plc:bulk/app.bsky.feed.post/{i}");
            let row = store
                .get_record(&uri)
                .unwrap()
                .expect("Batch row should exist");
            assert_eq!(row.record_json["seq"], i);
        }
    }

    #[test]
    fn test_hard_delete() {
        let store = RecordStore::open_in_memory().unwrap();
        let input = RecordInput::new(
            "did:plc:hard",
            "app.bsky.feed.post",
            "1",
            "cid",
            serde_json::json!({}),
            1,
        );
        store.upsert_record(&input).unwrap();
        assert!(store.get_record(&input.uri).unwrap().is_some());

        let deleted = store.hard_delete_record(&input.uri).unwrap();
        assert!(deleted);
        assert!(store
            .get_record_including_deleted(&input.uri)
            .unwrap()
            .is_none());

        let deleted_again = store.hard_delete_record(&input.uri).unwrap();
        assert!(!deleted_again);
    }

    #[test]
    fn test_parse_at_uri() {
        let (did, coll, rkey) =
            parse_at_uri("at://did:plc:alice/app.bsky.feed.post/3k6b123").unwrap();
        assert_eq!(did, "did:plc:alice");
        assert_eq!(coll, "app.bsky.feed.post");
        assert_eq!(rkey, "3k6b123");

        assert!(parse_at_uri("https://example.com").is_none());
        assert!(parse_at_uri("at://did:plc:alice").is_none());
        assert!(parse_at_uri("at://did:plc:alice/collection").is_none());
        assert!(parse_at_uri("at://did:plc:alice/collection/rkey/extra").is_none());
    }

    #[test]
    fn test_wal_pragma_verification_on_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_wal.db");
        let store = RecordStore::open(&db_path).unwrap();

        store
            .with_conn(|conn| {
                let journal_mode: String =
                    conn.query_row("PRAGMA journal_mode;", [], |r| r.get(0))?;
                assert_eq!(journal_mode.to_lowercase(), "wal");

                let busy_timeout: u32 = conn.query_row("PRAGMA busy_timeout;", [], |r| r.get(0))?;
                assert_eq!(busy_timeout, 5000);
                Ok(())
            })
            .unwrap();
    }
}
