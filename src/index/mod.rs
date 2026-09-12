//! Embedded SQLite indexing and query engine for AT Protocol records.
//!
//! Provides the canonical SQLite storage layer with WAL mode ([`RecordStore`]),
//! type-safe JSON1 query builder ([`QueryBuilder`]), and in-memory change notification
//! broadcast bus ([`BroadcastBus`]).

pub mod broadcast;
pub mod query;
pub mod store;

pub use broadcast::{BroadcastBus, ChangeNotification, DEFAULT_BROADCAST_CAPACITY};
pub use query::{QueryBuilder, QueryOp, SortDirection};
pub use store::{parse_at_uri, RecordInput, RecordRow, RecordStore, RecordStoreConfig};
