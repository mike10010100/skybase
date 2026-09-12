//! Type-safe SQLite JSON1 query builder for AT Protocol records.
//!
//! Provides fluent construction of queries filtering by collection NSID, author DID,
//! JSON1 field expressions (`where_json`), custom ordering, and pagination.

use serde::{Deserialize, Serialize};

use crate::error::{Result, SkybaseError};
use crate::index::store::{RecordRow, RecordStore};

/// Comparison operators for JSON field queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryOp {
    /// Equality (`=`). Checks equality with the extracted JSON value.
    Eq,
    /// Inequality (`!=`). Checks inequality with the extracted JSON value.
    Ne,
    /// Greater than (`>`). Lexicographical for strings, numerical for numbers.
    Gt,
    /// Greater than or equal to (`>=`).
    Gte,
    /// Less than (`<`).
    Lt,
    /// Less than or equal to (`<=`).
    Lte,
    /// Substring / pattern matching (`LIKE '%' || ? || '%'`).
    Contains,
    /// Direct SQL LIKE pattern matching with user-supplied wildcards (`LIKE ?`).
    Like,
}

/// Sort direction for query result ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    /// Ascending order (lowest to highest, A-Z, 0-9).
    Asc,
    /// Descending order (highest to lowest, Z-A, 9-0).
    Desc,
}

impl SortDirection {
    /// Returns the SQL keyword for this direction.
    #[must_use]
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Clone)]
enum OrderClause {
    Column(&'static str, SortDirection),
    JsonPath(String, SortDirection),
}

#[derive(Debug, Clone)]
struct WhereJsonClause {
    field_path: String,
    op: QueryOp,
    value: serde_json::Value,
}

/// Fluent query builder for searching records stored in SQLite.
pub struct QueryBuilder<'a> {
    store: &'a RecordStore,
    collection: String,
    did: Option<String>,
    where_json_clauses: Vec<WhereJsonClause>,
    order_by_clauses: Vec<OrderClause>,
    limit: Option<u32>,
    offset: Option<u32>,
    include_deleted: bool,
}

impl<'a> QueryBuilder<'a> {
    /// Creates a new query builder scoped to a target ATProto collection NSID.
    pub fn new(store: &'a RecordStore, collection: impl Into<String>) -> Self {
        Self {
            store,
            collection: collection.into(),
            did: None,
            where_json_clauses: Vec::new(),
            order_by_clauses: Vec::new(),
            limit: None,
            offset: None,
            include_deleted: false,
        }
    }

    /// Filters records by author DID (`did:plc:...` or `did:web:...`).
    #[must_use]
    pub fn did(mut self, did: impl Into<String>) -> Self {
        self.did = Some(did.into());
        self
    }

    /// Adds a filter condition on a JSON field within `record_json`.
    ///
    /// Paths can be supplied as `text`, `subject.author`, or `$.rating`.
    /// They will be normalized to SQLite JSONPath format (`$.<path>`).
    #[must_use]
    pub fn where_json(
        mut self,
        field_path: &str,
        op: QueryOp,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.where_json_clauses.push(WhereJsonClause {
            field_path: field_path.to_string(),
            op,
            value: value.into(),
        });
        self
    }

    /// Specifies sort order by column name or JSON field path.
    ///
    /// Known canonical columns: `indexed_at`, `uri`, `did`, `collection`, `rkey`, `cid`.
    /// Any other field name is treated as a JSON path within `record_json`.
    #[must_use]
    pub fn order_by(mut self, field: &str, direction: SortDirection) -> Self {
        let clause = match field {
            "indexed_at" => OrderClause::Column("indexed_at", direction),
            "uri" => OrderClause::Column("uri", direction),
            "did" => OrderClause::Column("did", direction),
            "collection" => OrderClause::Column("collection", direction),
            "rkey" => OrderClause::Column("rkey", direction),
            "cid" => OrderClause::Column("cid", direction),
            other => OrderClause::JsonPath(other.to_string(), direction),
        };
        self.order_by_clauses.push(clause);
        self
    }

    /// Limits the maximum number of records returned.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skips the first `offset` matching records.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Controls whether soft-deleted records (`is_deleted = 1`) are included in results.
    /// Defaults to `false` (only active records returned).
    #[must_use]
    pub fn include_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }

    /// Compiles the query into parameterized SQL and parameter values.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Index`] if collection is empty or if any JSON path is malformed.
    pub fn build_sql(&self) -> Result<(String, Vec<rusqlite::types::Value>)> {
        let coll = self.collection.trim();
        if coll.is_empty() {
            return Err(SkybaseError::Index("Collection cannot be empty".into()));
        }

        let mut sql = String::with_capacity(256);
        sql.push_str("SELECT uri, cid, did, collection, rkey, record_json, indexed_at, is_deleted FROM records WHERE collection = ?");

        let mut params = Vec::new();
        params.push(rusqlite::types::Value::Text(coll.to_string()));

        if !self.include_deleted {
            sql.push_str(" AND is_deleted = 0");
        }

        if let Some(ref did) = self.did {
            let trimmed_did = did.trim();
            if !trimmed_did.is_empty() {
                sql.push_str(" AND did = ?");
                params.push(rusqlite::types::Value::Text(trimmed_did.to_string()));
            }
        }

        for clause in &self.where_json_clauses {
            let path = normalize_json_path(&clause.field_path)?;
            params.push(rusqlite::types::Value::Text(path));

            match clause.op {
                QueryOp::Eq if clause.value.is_null() => {
                    sql.push_str(" AND json_extract(record_json, ?) IS NULL");
                }
                QueryOp::Ne if clause.value.is_null() => {
                    sql.push_str(" AND json_extract(record_json, ?) IS NOT NULL");
                }
                QueryOp::Eq => {
                    sql.push_str(" AND json_extract(record_json, ?) = ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Ne => {
                    sql.push_str(" AND json_extract(record_json, ?) != ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Gt => {
                    sql.push_str(" AND json_extract(record_json, ?) > ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Gte => {
                    sql.push_str(" AND json_extract(record_json, ?) >= ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Lt => {
                    sql.push_str(" AND json_extract(record_json, ?) < ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Lte => {
                    sql.push_str(" AND json_extract(record_json, ?) <= ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Contains => {
                    sql.push_str(" AND json_extract(record_json, ?) LIKE ('%' || ? || '%')");
                    params.push(json_to_sqlite_value(&clause.value));
                }
                QueryOp::Like => {
                    sql.push_str(" AND json_extract(record_json, ?) LIKE ?");
                    params.push(json_to_sqlite_value(&clause.value));
                }
            }
        }

        if !self.order_by_clauses.is_empty() {
            sql.push_str(" ORDER BY ");
            for (idx, order) in self.order_by_clauses.iter().enumerate() {
                if idx > 0 {
                    sql.push_str(", ");
                }
                match order {
                    OrderClause::Column(col_name, direction) => {
                        sql.push_str(col_name);
                        sql.push(' ');
                        sql.push_str(direction.as_sql());
                    }
                    OrderClause::JsonPath(raw_path, direction) => {
                        let path = normalize_json_path(raw_path)?;
                        sql.push_str("json_extract(record_json, ?) ");
                        sql.push_str(direction.as_sql());
                        params.push(rusqlite::types::Value::Text(path));
                    }
                }
            }
        }

        match (self.limit, self.offset) {
            (Some(limit), Some(offset)) => {
                sql.push_str(" LIMIT ? OFFSET ?");
                params.push(rusqlite::types::Value::Integer(i64::from(limit)));
                params.push(rusqlite::types::Value::Integer(i64::from(offset)));
            }
            (Some(limit), None) => {
                sql.push_str(" LIMIT ?");
                params.push(rusqlite::types::Value::Integer(i64::from(limit)));
            }
            (None, Some(offset)) => {
                sql.push_str(" LIMIT -1 OFFSET ?");
                params.push(rusqlite::types::Value::Integer(i64::from(offset)));
            }
            (None, None) => {}
        }

        Ok((sql, params))
    }

    /// Executes the query and returns matching [`RecordRow`] records.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Index`], [`SkybaseError::Storage`], or [`SkybaseError::Serialization`]
    /// on query or parsing failure.
    pub fn execute(self) -> Result<Vec<RecordRow>> {
        let (sql, params) = self.build_sql()?;
        self.store.query_raw(&sql, &params)
    }
}

/// Converts a [`serde_json::Value`] into a [`rusqlite::types::Value`] tailored for SQLite JSON1 comparisons.
#[must_use]
pub fn json_to_sqlite_value(val: &serde_json::Value) -> rusqlite::types::Value {
    match val {
        serde_json::Value::Null => rusqlite::types::Value::Null,
        serde_json::Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        serde_json::Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(u) = num.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    rusqlite::types::Value::Integer(i)
                } else {
                    rusqlite::types::Value::Text(num.to_string())
                }
            } else if let Some(f) = num.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Text(num.to_string())
            }
        }
        serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            rusqlite::types::Value::Text(val.to_string())
        }
    }
}

/// Normalizes a user-provided field path into canonical SQLite JSONPath syntax `$.<path>`.
///
/// # Errors
/// Returns [`SkybaseError::Index`] if `path` is empty or contains disallowed characters.
pub fn normalize_json_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(SkybaseError::Index(
            "JSON field path cannot be empty".into(),
        ));
    }

    let normalized = if trimmed.starts_with('$') {
        trimmed.to_string()
    } else if trimmed.starts_with('[') {
        format!("${trimmed}")
    } else {
        format!("$.{trimmed}")
    };

    validate_json_path(&normalized)?;
    Ok(normalized)
}

fn validate_json_path(path: &str) -> Result<()> {
    for (idx, ch) in path.chars().enumerate() {
        if idx == 0 && ch == '$' {
            continue;
        }
        if !ch.is_ascii_alphanumeric()
            && ch != '_'
            && ch != '-'
            && ch != '.'
            && ch != '['
            && ch != ']'
        {
            return Err(SkybaseError::Index(format!(
                "Invalid character '{ch}' in JSON path '{path}'"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::index::store::RecordInput;
    use serde_json::json;

    fn setup_test_store() -> RecordStore {
        let store = RecordStore::open_in_memory().unwrap();

        let r1 = RecordInput::new(
            "did:plc:alice",
            "app.bsky.feed.post",
            "post1",
            "bafyreia1",
            json!({
                "text": "Hello world from Alice!",
                "rating": 5,
                "is_published": true,
                "metadata": { "views": 100 },
                "tags": ["bluesky", "atproto"],
                "note": null
            }),
            1000,
        );

        let r2 = RecordInput::new(
            "did:plc:bob",
            "app.bsky.feed.post",
            "post2",
            "bafyreia2",
            json!({
                "text": "ATProto is the future",
                "rating": 3,
                "is_published": false,
                "metadata": { "views": 25 },
                "tags": ["atproto"],
                "note": "important"
            }),
            2000,
        );

        let r3 = RecordInput::new(
            "did:plc:alice",
            "app.bsky.feed.post",
            "post3",
            "bafyreia3",
            json!({
                "text": "Deleted post",
                "rating": 1
            }),
            3000,
        );

        store.upsert_record(&r1).unwrap();
        store.upsert_record(&r2).unwrap();
        store.upsert_record(&r3).unwrap();
        store.soft_delete_record(&r3.uri).unwrap();

        store
    }

    #[test]
    fn test_path_normalization() {
        assert_eq!(normalize_json_path("text").unwrap(), "$.text");
        assert_eq!(normalize_json_path("$.text").unwrap(), "$.text");
        assert_eq!(
            normalize_json_path("subject.author.did").unwrap(),
            "$.subject.author.did"
        );
        assert_eq!(normalize_json_path("tags[0]").unwrap(), "$.tags[0]");
        assert_eq!(normalize_json_path("$[0]").unwrap(), "$[0]");
        assert!(normalize_json_path("").is_err());
        assert!(normalize_json_path("   ").is_err());
        assert!(normalize_json_path("text; DROP TABLE records; --").is_err());
        assert!(normalize_json_path("text' OR '1'='1").is_err());
    }

    #[test]
    fn test_build_sql_basic() {
        let store = RecordStore::open_in_memory().unwrap();
        let q = store
            .query("app.bsky.feed.post")
            .did("did:plc:alice")
            .where_json("rating", QueryOp::Gte, json!(3))
            .order_by("indexed_at", SortDirection::Desc)
            .limit(10)
            .offset(5);

        let (sql, params) = q.build_sql().unwrap();
        assert!(sql.contains("WHERE collection = ?"));
        assert!(sql.contains("AND is_deleted = 0"));
        assert!(sql.contains("AND did = ?"));
        assert!(sql.contains("AND json_extract(record_json, ?) >= ?"));
        assert!(sql.contains("ORDER BY indexed_at DESC"));
        assert!(sql.contains("LIMIT ? OFFSET ?"));
        assert_eq!(params.len(), 6);
    }

    #[test]
    fn test_equality_query() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .where_json("rating", QueryOp::Eq, json!(5))
            .execute()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uri, "at://did:plc:alice/app.bsky.feed.post/post1");
    }

    #[test]
    fn test_boolean_query_matches_sqlite() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .where_json("is_published", QueryOp::Eq, json!(true))
            .execute()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uri, "at://did:plc:alice/app.bsky.feed.post/post1");

        let false_rows = store
            .query("app.bsky.feed.post")
            .where_json("is_published", QueryOp::Eq, json!(false))
            .execute()
            .unwrap();

        assert_eq!(false_rows.len(), 1);
        assert_eq!(
            false_rows[0].uri,
            "at://did:plc:bob/app.bsky.feed.post/post2"
        );
    }

    #[test]
    fn test_nested_json_lookup() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .where_json("metadata.views", QueryOp::Eq, json!(100))
            .execute()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uri, "at://did:plc:alice/app.bsky.feed.post/post1");
    }

    #[test]
    fn test_substring_contains_query() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .where_json("text", QueryOp::Contains, json!("future"))
            .execute()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uri, "at://did:plc:bob/app.bsky.feed.post/post2");
    }

    #[test]
    fn test_ordering_and_pagination() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .order_by("indexed_at", SortDirection::Desc)
            .limit(1)
            .offset(0)
            .execute()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uri, "at://did:plc:bob/app.bsky.feed.post/post2");
    }

    #[test]
    fn test_soft_delete_exclusion_and_inclusion() {
        let store = setup_test_store();

        let active_rows = store.query("app.bsky.feed.post").execute().unwrap();
        assert_eq!(active_rows.len(), 2);

        let all_rows = store
            .query("app.bsky.feed.post")
            .include_deleted(true)
            .execute()
            .unwrap();
        assert_eq!(all_rows.len(), 3);
    }

    #[test]
    fn test_null_comparisons() {
        let store = setup_test_store();

        let null_rows = store
            .query("app.bsky.feed.post")
            .where_json("note", QueryOp::Eq, serde_json::Value::Null)
            .execute()
            .unwrap();
        assert_eq!(null_rows.len(), 1);
        assert_eq!(
            null_rows[0].uri,
            "at://did:plc:alice/app.bsky.feed.post/post1"
        );

        let not_null_rows = store
            .query("app.bsky.feed.post")
            .where_json("note", QueryOp::Ne, serde_json::Value::Null)
            .execute()
            .unwrap();
        assert_eq!(not_null_rows.len(), 1);
        assert_eq!(
            not_null_rows[0].uri,
            "at://did:plc:bob/app.bsky.feed.post/post2"
        );
    }

    #[test]
    fn test_numeric_comparisons() {
        let store = setup_test_store();

        let gt_rows = store
            .query("app.bsky.feed.post")
            .where_json("rating", QueryOp::Gt, json!(3))
            .execute()
            .unwrap();
        assert_eq!(gt_rows.len(), 1);
        assert_eq!(
            gt_rows[0].uri,
            "at://did:plc:alice/app.bsky.feed.post/post1"
        );

        let lte_rows = store
            .query("app.bsky.feed.post")
            .where_json("rating", QueryOp::Lte, json!(3))
            .execute()
            .unwrap();
        assert_eq!(lte_rows.len(), 1);
        assert_eq!(lte_rows[0].uri, "at://did:plc:bob/app.bsky.feed.post/post2");
    }

    #[test]
    fn test_order_by_json_path() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .order_by("rating", SortDirection::Asc)
            .execute()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].uri, "at://did:plc:bob/app.bsky.feed.post/post2");
        assert_eq!(rows[1].uri, "at://did:plc:alice/app.bsky.feed.post/post1");
    }

    #[test]
    fn test_like_query() {
        let store = setup_test_store();
        let rows = store
            .query("app.bsky.feed.post")
            .where_json("text", QueryOp::Like, "%future%")
            .execute()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].uri, "at://did:plc:bob/app.bsky.feed.post/post2");
    }
}
