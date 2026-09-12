//! Shared test infrastructure, fixtures, and mock doubles for Skybase E2E tests.
//!
//! Provides hermetic, offline test implementations matching the exact interface
//! contracts specified in `PROJECT.md` and `TEST_INFRA.md`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::result_large_err,
    missing_docs,
    dead_code
)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::protocol::Message;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use skybase::error::SkybaseError;

// ============================================================================
// 1. Data Models & Interface Contracts (Matching PROJECT.md)
// ============================================================================

/// Record input payload for storage upserts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordInput {
    pub uri: String,
    pub cid: String,
    pub did: String,
    pub collection: String,
    pub rkey: String,
    pub record_json: serde_json::Value,
    pub indexed_at: u64,
}

/// Canonical record row retrieved from SQLite storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordRow {
    pub uri: String,
    pub cid: String,
    pub did: String,
    pub collection: String,
    pub rkey: String,
    pub record_json: serde_json::Value,
    pub indexed_at: u64,
    pub is_deleted: bool,
}

/// Change notification broadcast on record mutations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeNotification {
    Upsert(RecordRow),
    Delete {
        uri: String,
        did: String,
        collection: String,
        rkey: String,
    },
}

/// Query operators supported by QueryBuilder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    In,
}

impl QueryOp {
    pub fn as_sql(&self) -> &'static str {
        match self {
            QueryOp::Eq => "=",
            QueryOp::NotEq => "!=",
            QueryOp::Gt => ">",
            QueryOp::Gte => ">=",
            QueryOp::Lt => "<",
            QueryOp::Lte => "<=",
            QueryOp::Like => "LIKE",
            QueryOp::In => "IN",
        }
    }
}

/// Sort direction for query ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    pub fn as_sql(&self) -> &'static str {
        match self {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        }
    }
}

/// Commit operation type for Jetstream events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitOperation {
    Create,
    Update,
    Delete,
}

/// Jetstream commit event structure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JetstreamCommit {
    pub did: String,
    pub time_us: u64,
    pub collection: String,
    pub rkey: String,
    pub operation: CommitOperation,
    pub cid: Option<String>,
    pub record: Option<serde_json::Value>,
}

/// Result of sovereign record creation.
pub use skybase::repo::CreateRecordResult;

// ============================================================================
// 2. Embedded SQLite Storage Harness (TestRecordStore)
// ============================================================================

/// Embedded SQLite record store implementing canonical schema, WAL mode, and broadcast bus.
#[derive(Clone)]
pub struct TestRecordStore {
    conn: Arc<Mutex<Connection>>,
    bus: broadcast::Sender<ChangeNotification>,
}

impl TestRecordStore {
    /// Opens an in-memory SQLite store with canonical schema.
    pub fn open_in_memory() -> Result<Self, SkybaseError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| SkybaseError::Storage(format!("Failed to open in-memory DB: {e}")))?;

        // Configure SQLite PRAGMAs
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|e| SkybaseError::Storage(format!("Failed to set PRAGMAs: {e}")))?;

        Self::init_schema(&conn)?;

        let (bus, _) = broadcast::channel(1024);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            bus,
        })
    }

    /// Opens a file-backed SQLite store in WAL mode within a temp directory.
    pub fn open_file_backed(path: &std::path::Path) -> Result<Self, SkybaseError> {
        let conn = Connection::open(path)
            .map_err(|e| SkybaseError::Storage(format!("Failed to open file DB: {e}")))?;

        // Enforce WAL mode and concurrency settings
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|e| SkybaseError::Storage(format!("Failed to set WAL PRAGMAs: {e}")))?;

        Self::init_schema(&conn)?;

        let (bus, _) = broadcast::channel(1024);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            bus,
        })
    }

    fn init_schema(conn: &Connection) -> Result<(), SkybaseError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS records (
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
            CREATE INDEX IF NOT EXISTS idx_records_collection_did ON records(collection, did);",
        )
        .map_err(|e| SkybaseError::Storage(format!("Failed to initialize schema: {e}")))?;
        Ok(())
    }

    /// Upserts a record atomically using `ON CONFLICT(uri) DO UPDATE`.
    pub fn upsert_record(&self, record: &RecordInput) -> Result<(), SkybaseError> {
        let json_str =
            serde_json::to_string(&record.record_json).map_err(SkybaseError::Serialization)?;

        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO records (uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)
             ON CONFLICT(uri) DO UPDATE SET
                 cid = excluded.cid,
                 record_json = excluded.record_json,
                 indexed_at = excluded.indexed_at,
                 is_deleted = 0",
            params![
                record.uri,
                record.cid,
                record.did,
                record.collection,
                record.rkey,
                json_str,
                record.indexed_at as i64,
            ],
        )
        .map_err(|e| SkybaseError::Storage(format!("Upsert failed: {e}")))?;

        let row = RecordRow {
            uri: record.uri.clone(),
            cid: record.cid.clone(),
            did: record.did.clone(),
            collection: record.collection.clone(),
            rkey: record.rkey.clone(),
            record_json: record.record_json.clone(),
            indexed_at: record.indexed_at,
            is_deleted: false,
        };

        // Emit change notification to broadcast bus
        let _ = self.bus.send(ChangeNotification::Upsert(row));
        Ok(())
    }

    /// Soft-deletes a record by setting `is_deleted = 1`.
    pub fn soft_delete_record(&self, uri: &str) -> Result<(), SkybaseError> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT did, collection, rkey FROM records WHERE uri = ?1")
            .map_err(|e| SkybaseError::Storage(format!("Prepare failed: {e}")))?;

        let info: Option<(String, String, String)> = stmt
            .query_row(params![uri], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()
            .map_err(|e| SkybaseError::Storage(format!("Query failed: {e}")))?;

        conn.execute(
            "UPDATE records SET is_deleted = 1 WHERE uri = ?1",
            params![uri],
        )
        .map_err(|e| SkybaseError::Storage(format!("Soft delete failed: {e}")))?;

        if let Some((did, collection, rkey)) = info {
            let _ = self.bus.send(ChangeNotification::Delete {
                uri: uri.to_string(),
                did,
                collection,
                rkey,
            });
        }
        Ok(())
    }

    /// Hard-deletes a record permanently from SQLite.
    pub fn hard_delete_record(&self, uri: &str) -> Result<(), SkybaseError> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM records WHERE uri = ?1", params![uri])
            .map_err(|e| SkybaseError::Storage(format!("Hard delete failed: {e}")))?;
        Ok(())
    }

    /// Retrieves a single record by canonical AT-URI.
    pub fn get_record(&self, uri: &str) -> Result<Option<RecordRow>, SkybaseError> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted
                 FROM records WHERE uri = ?1",
            )
            .map_err(|e| SkybaseError::Storage(format!("Prepare failed: {e}")))?;

        stmt.query_row(params![uri], |row| {
            let record_str: String = row.get(5)?;
            let record_val: serde_json::Value =
                serde_json::from_str(&record_str).unwrap_or(serde_json::Value::Null);
            let indexed_at_i64: i64 = row.get(6)?;
            let is_deleted_i32: i32 = row.get(7)?;

            Ok(RecordRow {
                uri: row.get(0)?,
                cid: row.get(1)?,
                did: row.get(2)?,
                collection: row.get(3)?,
                rkey: row.get(4)?,
                record_json: record_val,
                indexed_at: indexed_at_i64.max(0) as u64,
                is_deleted: is_deleted_i32 != 0,
            })
        })
        .optional()
        .map_err(|e| SkybaseError::Storage(format!("Query failed: {e}")))
    }

    /// Returns a receiver handle for the in-memory broadcast bus.
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeNotification> {
        self.bus.subscribe()
    }

    /// Creates a fluent QueryBuilder bound to a specific collection.
    pub fn collection<'a>(&'a self, collection: impl Into<String>) -> TestQueryBuilder<'a> {
        TestQueryBuilder::new(self, collection)
    }

    /// Returns direct handle to SQLite connection for schema / pragma verification.
    pub fn raw_connection(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

// ============================================================================
// 3. Structured Query Builder with JSON1 Extraction (TestQueryBuilder)
// ============================================================================

struct JsonWhereClause {
    field_path: String,
    op: QueryOp,
    value: serde_json::Value,
}

/// Fluent query builder for SQLite records with JSON1 support.
pub struct TestQueryBuilder<'a> {
    store: &'a TestRecordStore,
    collection: String,
    did: Option<String>,
    where_clauses: Vec<JsonWhereClause>,
    order_by_col: Option<(String, SortDirection)>,
    limit: Option<u32>,
    offset: Option<u32>,
    include_deleted: bool,
}

#[derive(Debug, Clone)]
enum SqlParam {
    Text(String),
    Integer(i64),
    Real(f64),
    Null,
}

impl rusqlite::ToSql for SqlParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            SqlParam::Text(s) => Ok(s.as_str().into()),
            SqlParam::Integer(i) => Ok((*i).into()),
            SqlParam::Real(f) => Ok((*f).into()),
            SqlParam::Null => Ok(rusqlite::types::Null.into()),
        }
    }
}

impl<'a> TestQueryBuilder<'a> {
    pub fn new(store: &'a TestRecordStore, collection: impl Into<String>) -> Self {
        Self {
            store,
            collection: collection.into(),
            did: None,
            where_clauses: Vec::new(),
            order_by_col: None,
            limit: None,
            offset: None,
            include_deleted: false,
        }
    }

    pub fn did(mut self, did: impl Into<String>) -> Self {
        self.did = Some(did.into());
        self
    }

    pub fn where_json(
        mut self,
        field_path: impl Into<String>,
        op: QueryOp,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.where_clauses.push(JsonWhereClause {
            field_path: field_path.into(),
            op,
            value: value.into(),
        });
        self
    }

    pub fn order_by(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.order_by_col = Some((field.into(), direction));
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn include_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }

    /// Executes the query and returns matching records.
    pub fn execute(self) -> Result<Vec<RecordRow>, SkybaseError> {
        let mut sql = String::from(
            "SELECT uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted
             FROM records WHERE collection = ?1",
        );

        let mut param_index = 2;
        let mut params_list: Vec<SqlParam> = vec![SqlParam::Text(self.collection)];

        if !self.include_deleted {
            sql.push_str(" AND is_deleted = 0");
        }

        if let Some(did) = self.did {
            sql.push_str(&format!(" AND did = ?{param_index}"));
            params_list.push(SqlParam::Text(did));
            param_index += 1;
        }

        for clause in &self.where_clauses {
            let json_path = format!("$.{}", clause.field_path);
            let path_param_idx = param_index;
            param_index += 1;

            let val_param_idx = param_index;
            param_index += 1;

            sql.push_str(&format!(
                " AND json_extract(record_json, ?{path_param_idx}) {} ?{val_param_idx}",
                clause.op.as_sql()
            ));

            params_list.push(SqlParam::Text(json_path));

            let sql_param = match &clause.value {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        SqlParam::Integer(i)
                    } else if let Some(f) = n.as_f64() {
                        SqlParam::Real(f)
                    } else {
                        SqlParam::Text(n.to_string())
                    }
                }
                serde_json::Value::String(s) => SqlParam::Text(s.clone()),
                serde_json::Value::Bool(b) => SqlParam::Integer(if *b { 1 } else { 0 }),
                serde_json::Value::Null => SqlParam::Null,
                other => SqlParam::Text(other.to_string()),
            };
            params_list.push(sql_param);
        }

        if let Some((field, dir)) = self.order_by_col {
            let canonical = matches!(
                field.as_str(),
                "uri" | "cid" | "did" | "collection" | "rkey" | "indexed_at"
            );

            if canonical {
                sql.push_str(&format!(" ORDER BY {field} {}", dir.as_sql()));
            } else {
                let path_param_idx = param_index;
                sql.push_str(&format!(
                    " ORDER BY json_extract(record_json, ?{path_param_idx}) {}",
                    dir.as_sql()
                ));
                params_list.push(SqlParam::Text(format!("$.{field}")));
            }
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
            if let Some(offset) = self.offset {
                sql.push_str(&format!(" OFFSET {offset}"));
            }
        } else if let Some(offset) = self.offset {
            sql.push_str(&format!(" LIMIT -1 OFFSET {offset}"));
        }

        let conn = self.store.conn.lock();
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| SkybaseError::Storage(format!("Query compile error: {e}")))?;

        let rusqlite_params: Vec<&dyn rusqlite::ToSql> = params_list
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt
            .query_map(rusqlite_params.as_slice(), |row| {
                let record_str: String = row.get(5)?;
                let record_val: serde_json::Value =
                    serde_json::from_str(&record_str).unwrap_or(serde_json::Value::Null);
                let indexed_at_i64: i64 = row.get(6)?;
                let is_deleted_i32: i32 = row.get(7)?;

                Ok(RecordRow {
                    uri: row.get(0)?,
                    cid: row.get(1)?,
                    did: row.get(2)?,
                    collection: row.get(3)?,
                    rkey: row.get(4)?,
                    record_json: record_val,
                    indexed_at: indexed_at_i64.max(0) as u64,
                    is_deleted: is_deleted_i32 != 0,
                })
            })
            .map_err(|e| SkybaseError::Storage(format!("Query execute error: {e}")))?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(|e| SkybaseError::Storage(format!("Row decode error: {e}")))?);
        }
        Ok(results)
    }
}

// ============================================================================
// 4. Ingest & Mock Jetstream Server Harness
// ============================================================================

/// Atomic monotonic high-watermark cursor tracker.
#[derive(Debug, Default)]
pub struct TestCursorTracker {
    watermark: AtomicU64,
}

impl TestCursorTracker {
    pub fn new(initial: u64) -> Self {
        Self {
            watermark: AtomicU64::new(initial),
        }
    }

    /// Updates watermark monotonically: returns true if watermark was advanced.
    pub fn update(&self, time_us: u64) -> bool {
        let mut current = self.watermark.load(Ordering::Relaxed);
        while time_us > current {
            match self.watermark.compare_exchange_weak(
                current,
                time_us,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    pub fn get(&self) -> u64 {
        self.watermark.load(Ordering::SeqCst)
    }
}

/// Backoff manager calculating exponential backoff with pseudo-random jitter.
pub struct TestBackoffManager {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub current_delay: Duration,
    pub attempts: u32,
}

impl TestBackoffManager {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial_delay: initial,
            max_delay: max,
            current_delay: initial,
            attempts: 0,
        }
    }

    pub fn reset(&mut self) {
        self.current_delay = self.initial_delay;
        self.attempts = 0;
    }

    /// Calculates next backoff duration with ±20% pseudo-jitter.
    pub fn next_backoff(&mut self) -> Duration {
        self.attempts += 1;
        let base_ms = self.current_delay.as_millis() as u64;

        // Deterministic pseudo-jitter ±20% based on attempt count and base_ms
        let jitter_range = (base_ms * 20) / 100;
        let jitter = if jitter_range > 0 {
            (base_ms
                .wrapping_mul(6364136223846793005)
                .wrapping_add(self.attempts as u64)
                % (jitter_range * 2)) as i64
                - jitter_range as i64
        } else {
            0
        };

        let calculated_ms = (base_ms as i64 + jitter).max(10) as u64;
        self.current_delay = (self.current_delay * 2).min(self.max_delay);
        Duration::from_millis(calculated_ms)
    }
}

/// Synthetic Jetstream WebSocket mock server for offline hermetic testing.
pub struct MockJetstreamServer {
    addr: SocketAddr,
    tx: broadcast::Sender<String>,
    active_connections: Arc<AtomicU64>,
    received_queries: Arc<Mutex<Vec<String>>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockJetstreamServer {
    /// Starts a mock Jetstream server on loopback port 127.0.0.1:0.
    pub async fn start() -> Result<Self, SkybaseError> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| SkybaseError::Storage(format!("Bind failed: {e}")))?;

        let addr = listener
            .local_addr()
            .map_err(|e| SkybaseError::Storage(format!("Local addr error: {e}")))?;

        let (tx, _) = broadcast::channel::<String>(256);
        let active_connections = Arc::new(AtomicU64::new(0));
        let received_queries = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_tx = tx.clone();
        let server_active = active_connections.clone();
        let server_queries = received_queries.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok((stream, _)) = listener.accept() => {
                        let client_rx = server_tx.subscribe();
                        let client_active = server_active.clone();
                        let client_queries = server_queries.clone();

                        tokio::spawn(async move {
                            client_active.fetch_add(1, Ordering::SeqCst);
                            let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request, res: tokio_tungstenite::tungstenite::handshake::server::Response| {
                                let path = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("").to_string();
                                client_queries.lock().push(path);
                                Ok(res)
                            };

                            if let Ok(mut ws_stream) = tokio_tungstenite::accept_hdr_async(stream, callback).await {
                                let mut rx = client_rx;
                                while let Ok(msg) = rx.recv().await {
                                    if ws_stream.send(Message::Text(msg)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            client_active.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        Ok(Self {
            addr,
            tx,
            active_connections,
            received_queries,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// WebSocket URL string for consumer connection.
    pub fn ws_url(&self) -> String {
        format!("ws://{}/subscribe", self.addr)
    }

    /// Emits a structured Jetstream commit frame.
    pub fn emit_commit(&self, commit: &JetstreamCommit) -> Result<usize, SkybaseError> {
        let op_str = match commit.operation {
            CommitOperation::Create => "create",
            CommitOperation::Update => "update",
            CommitOperation::Delete => "delete",
        };

        let payload = json!({
            "did": commit.did,
            "time_us": commit.time_us,
            "kind": "commit",
            "commit": {
                "collection": commit.collection,
                "rkey": commit.rkey,
                "operation": op_str,
                "cid": commit.cid,
                "record": commit.record,
            }
        });

        self.tx
            .send(payload.to_string())
            .map_err(|e| SkybaseError::Event(format!("Emit failed: {e}")))
    }

    /// Emits raw JSON string for malformed frame testing.
    pub fn emit_raw(&self, text: &str) -> Result<usize, SkybaseError> {
        self.tx
            .send(text.to_string())
            .map_err(|e| SkybaseError::Event(format!("Emit raw failed: {e}")))
    }

    /// Emits a heartbeat frame with timestamp only.
    pub fn emit_heartbeat(&self, time_us: u64) -> Result<usize, SkybaseError> {
        let payload = json!({
            "time_us": time_us,
        });
        self.tx
            .send(payload.to_string())
            .map_err(|e| SkybaseError::Event(format!("Emit heartbeat failed: {e}")))
    }

    pub fn query_history(&self) -> Vec<String> {
        self.received_queries.lock().clone()
    }
}

impl Drop for MockJetstreamServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ============================================================================
// 5. Sovereign PDS Write Client Harness (MockPdsServer & TestPdsRepoClient)
// ============================================================================

/// Mock PDS XRPC server simulating ATProto repo mutations and DPoP authentication.
pub struct MockPdsServer {
    server: MockServer,
    created_records: Arc<Mutex<Vec<serde_json::Value>>>,
    deleted_records: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl MockPdsServer {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        let created_records = Arc::new(Mutex::new(Vec::new()));
        let deleted_records = Arc::new(Mutex::new(Vec::new()));

        // Mount default createRecord responder
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .and(header_exists("authorization"))
            .and(header_exists("dpop"))
            .respond_with(|req: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let repo = body["repo"].as_str().unwrap_or("did:plc:unknown");
                let collection = body["collection"].as_str().unwrap_or("unknown");
                let rkey = body["rkey"].as_str().unwrap_or("test_rkey");

                let uri = format!("at://{repo}/{collection}/{rkey}");
                let cid = "bafyreih5678mockcidvalue9876543210".to_string();

                ResponseTemplate::new(200).set_body_json(json!({
                    "uri": uri,
                    "cid": cid,
                }))
            })
            .mount(&server)
            .await;

        // Mount default deleteRecord responder
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.deleteRecord"))
            .and(header_exists("authorization"))
            .and(header_exists("dpop"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        Self {
            server,
            created_records,
            deleted_records,
        }
    }

    pub fn uri(&self) -> String {
        self.server.uri()
    }

    /// Mounts a one-time DPoP nonce challenge (401 use_dpop_nonce) on createRecord.
    pub async fn mount_nonce_challenge_once(&self, nonce_value: &str) {
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(
                ResponseTemplate::new(401)
                    .insert_header("DPoP-Nonce", nonce_value)
                    .set_body_json(json!({
                        "error": "use_dpop_nonce",
                        "message": "DPoP proof requires nonce"
                    })),
            )
            .up_to_n_times(1)
            .mount(&self.server)
            .await;
    }
}

/// Test client issuing DPoP-signed record mutations against an ATProto PDS,
/// delegating directly to the production [`skybase::repo::PdsRepoClient`].
pub struct TestPdsRepoClient {
    inner: skybase::repo::PdsRepoClient,
}

impl TestPdsRepoClient {
    pub fn new(
        pds_endpoint: impl Into<String>,
        repo_did: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        let inner =
            skybase::repo::PdsRepoClient::from_credentials(pds_endpoint, repo_did, access_token)
                .expect("Failed to initialize production PdsRepoClient in test double");
        Self { inner }
    }

    pub async fn create_record<T: Serialize>(
        &self,
        collection: &str,
        rkey: Option<&str>,
        record: &T,
        validate: bool,
    ) -> Result<CreateRecordResult, SkybaseError> {
        self.inner
            .create_record(collection, rkey, record, validate)
            .await
    }

    pub async fn delete_record(&self, collection: &str, rkey: &str) -> Result<(), SkybaseError> {
        self.inner.delete_record(collection, rkey).await
    }

    /// Accesses the underlying production client.
    pub fn inner(&self) -> &skybase::repo::PdsRepoClient {
        &self.inner
    }
}
