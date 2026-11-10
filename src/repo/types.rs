//! Data transfer objects, Lexicon request/response models, and validation routines
//! for sovereign AT Protocol repository operations (`com.atproto.repo.*`).

use serde::{Deserialize, Serialize};

use crate::error::SkybaseError;

/// Request payload for `com.atproto.repo.createRecord`.
///
/// Creates a new record in the specified repository and collection.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRecordRequest<'a, T: Serialize> {
    /// The DID or handle of the target repository (e.g. `"did:plc:..."`).
    pub repo: &'a str,
    /// The NSID of the record collection (e.g. `"app.bsky.feed.post"`).
    pub collection: &'a str,
    /// Optional record key. If `None`, the PDS assigns a server-generated TID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rkey: Option<&'a str>,
    /// Whether to validate the record against the Lexicon schema.
    pub validate: bool,
    /// The record payload object.
    pub record: &'a T,
    /// Optional compare-and-swap commit CID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_commit: Option<&'a str>,
}

/// The result returned by a successful `com.atproto.repo.createRecord` invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRecordResult {
    /// Canonical AT-URI of the created record (`at://{did}/{collection}/{rkey}`).
    pub uri: String,
    /// Content identifier (CID) of the committed record.
    pub cid: String,
}

/// Request payload for `com.atproto.repo.putRecord`.
///
/// Writes or updates an existing record at a specified rkey in the repository.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PutRecordRequest<'a, T: Serialize> {
    /// The DID or handle of the target repository (e.g. `"did:plc:..."`).
    pub repo: &'a str,
    /// The NSID of the record collection (e.g. `"social.skybouncer.config"`).
    pub collection: &'a str,
    /// The record key (rkey) identifying the record (e.g. `"self"`).
    pub rkey: &'a str,
    /// Whether to validate the record against the Lexicon schema.
    pub validate: bool,
    /// The record payload object.
    pub record: &'a T,
    /// Optional compare-and-swap record CID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_record: Option<&'a str>,
    /// Optional compare-and-swap commit CID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_commit: Option<&'a str>,
}

/// The result returned by a successful `com.atproto.repo.putRecord` invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutRecordResult {
    /// Canonical AT-URI of the created or updated record (`at://{did}/{collection}/{rkey}`).
    pub uri: String,
    /// Content identifier (CID) of the committed record.
    pub cid: String,
}

/// The record returned by a successful `com.atproto.repo.getRecord` invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordView {
    /// Canonical AT-URI of the record.
    pub uri: String,
    /// Content identifier (CID) of the record.
    #[serde(default)]
    pub cid: String,
    /// The record data payload as a JSON value.
    pub value: serde_json::Value,
}

/// Request payload for `com.atproto.repo.deleteRecord`.
///
/// Removes an existing record from the specified repository and collection.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteRecordRequest<'a> {
    /// The DID or handle of the repository (e.g. `"did:plc:..."`).
    pub repo: &'a str,
    /// The NSID of the record collection (e.g. `"app.bsky.feed.post"`).
    pub collection: &'a str,
    /// The record key (rkey) identifying the record to delete.
    pub rkey: &'a str,
    /// Optional compare-and-swap record CID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_record: Option<&'a str>,
    /// Optional compare-and-swap commit CID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_commit: Option<&'a str>,
}

/// Standard AT Protocol XRPC error response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XrpcErrorResponse {
    /// Machine-readable error code (e.g. `"InvalidRequest"`, `"use_dpop_nonce"`, `"ExpiredToken"`).
    #[serde(default)]
    pub error: Option<String>,
    /// Human-readable description of the error.
    #[serde(default)]
    pub message: Option<String>,
}

/// Constructs a canonical AT-URI from component strings.
///
/// # Format
/// `at://{did}/{collection}/{rkey}`
///
/// # Examples
/// ```
/// use skybase::repo::format_at_uri;
///
/// let uri = format_at_uri("did:plc:alice", "app.bsky.feed.post", "3k2...xyz");
/// assert_eq!(uri, "at://did:plc:alice/app.bsky.feed.post/3k2...xyz");
/// ```
#[must_use]
pub fn format_at_uri(did: &str, collection: &str, rkey: &str) -> String {
    format!("at://{did}/{collection}/{rkey}")
}

/// Splits an `at://{authority}/{collection}/{rkey}` URI into `(authority, collection, rkey)`.
#[must_use]
pub fn split_at_uri(uri: &str) -> Option<(&str, &str, &str)> {
    let stripped = uri.strip_prefix("at://")?;
    let mut parts = stripped.split('/');
    let authority = parts.next()?;
    let collection = parts.next()?;
    let rkey = parts.next()?;
    if parts.next().is_some() || authority.is_empty() || collection.is_empty() || rkey.is_empty() {
        return None;
    }
    Some((authority, collection, rkey))
}

/// Extracts the DID portion from an AT-URI (`at://{did}/{collection}/{rkey}`).
///
/// Returns `None` if the URI does not start with `at://` or lacks a valid `did:` authority.
#[must_use]
pub fn extract_did_from_at_uri(uri: &str) -> Option<&str> {
    let stripped = uri.strip_prefix("at://")?;
    let did = stripped.split('/').next()?;
    if did.starts_with("did:") {
        Some(did)
    } else {
        None
    }
}

/// Extracts the authority DID from an AT-URI if and only if the collection matches `expected_collection`
/// and the record key (`rkey`) is non-empty.
#[must_use]
pub fn extract_did_for_collection<'a>(uri: &'a str, expected_collection: &str) -> Option<&'a str> {
    let (did, collection, rkey) = split_at_uri(uri)?;
    if !did.starts_with("did:") || collection != expected_collection || rkey.is_empty() {
        return None;
    }
    Some(did)
}

/// Parses an AT-URI (`at://{author}/app.bsky.feed.post/{rkey}`) into `(author, rkey)`.
#[must_use]
pub fn parse_at_post_uri(uri: &str) -> Option<(&str, &str)> {
    let (author, collection, rkey) = split_at_uri(uri)?;
    if author.is_empty() || collection != "app.bsky.feed.post" || rkey.is_empty() {
        return None;
    }
    Some((author, rkey))
}

/// Validates that a record key (rkey) complies with the AT Protocol record key specification.
///
/// Keys must consist of 1–512 ASCII alphanumeric or `.` `_` `~` `-` characters,
/// and cannot equal `.` or `..`.
///
/// # Errors
/// Returns [`SkybaseError::Repo`] if the key is empty, exceeds 512 characters,
/// equals `.` or `..`, or contains disallowed characters.
///
/// # Examples
/// ```
/// use skybase::repo::validate_rkey;
///
/// assert!(validate_rkey("3k2xyz123").is_ok());
/// assert!(validate_rkey("valid-rkey.name~1").is_ok());
/// assert!(validate_rkey("").is_err());
/// assert!(validate_rkey(".").is_err());
/// assert!(validate_rkey("..").is_err());
/// assert!(validate_rkey("invalid/slash").is_err());
/// ```
pub fn validate_rkey(rkey: &str) -> Result<(), SkybaseError> {
    if rkey.is_empty() || rkey.len() > 512 || rkey == "." || rkey == ".." {
        return Err(SkybaseError::Repo(format!(
            "Invalid rkey '{rkey}': length must be 1..=512 and not '.' or '..'"
        )));
    }
    for b in rkey.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b'~' | b'-' => {}
            _ => {
                return Err(SkybaseError::Repo(format!(
                    "Invalid rkey '{rkey}': illegal character '{}'",
                    b as char
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_format_at_uri() {
        let uri = format_at_uri("did:plc:12345", "app.bsky.feed.post", "rkey_abc");
        assert_eq!(uri, "at://did:plc:12345/app.bsky.feed.post/rkey_abc");
        assert_eq!(
            split_at_uri(&uri),
            Some(("did:plc:12345", "app.bsky.feed.post", "rkey_abc"))
        );
        assert_eq!(extract_did_from_at_uri(&uri), Some("did:plc:12345"));
        assert_eq!(
            extract_did_for_collection(&uri, "app.bsky.feed.post"),
            Some("did:plc:12345")
        );
        assert_eq!(
            extract_did_for_collection(&uri, "app.bsky.graph.list"),
            None
        );
        assert_eq!(parse_at_post_uri(&uri), Some(("did:plc:12345", "rkey_abc")));
    }

    #[test]
    fn test_validate_rkey_valid() {
        assert!(validate_rkey("valid-key").is_ok());
        assert!(validate_rkey("3k2abc123").is_ok());
        assert!(validate_rkey("user.profile_data~test-01").is_ok());
        assert!(validate_rkey("a").is_ok());
        let max_len_key = "a".repeat(512);
        assert!(validate_rkey(&max_len_key).is_ok());
    }

    #[test]
    fn test_validate_rkey_invalid() {
        assert!(validate_rkey("").is_err());
        assert!(validate_rkey(".").is_err());
        assert!(validate_rkey("..").is_err());
        assert!(validate_rkey("has space").is_err());
        assert!(validate_rkey("slash/in/key").is_err());
        assert!(validate_rkey("hash#tag").is_err());
        assert!(validate_rkey("question?mark").is_err());
        assert!(validate_rkey("null\0byte").is_err());
        let too_long = "a".repeat(513);
        assert!(validate_rkey(&too_long).is_err());
    }

    #[test]
    fn test_create_record_request_serialization() {
        let req = CreateRecordRequest {
            repo: "did:plc:alice",
            collection: "app.bsky.feed.post",
            rkey: Some("post1"),
            validate: true,
            record: &serde_json::json!({ "text": "Hello" }),
            swap_commit: None,
        };

        let json_val = serde_json::to_value(&req).expect("serialization failed");
        assert_eq!(json_val["repo"], "did:plc:alice");
        assert_eq!(json_val["collection"], "app.bsky.feed.post");
        assert_eq!(json_val["rkey"], "post1");
        assert_eq!(json_val["validate"], true);
        assert_eq!(json_val["record"]["text"], "Hello");
        assert!(json_val.get("swapCommit").is_none());
    }

    #[test]
    fn test_delete_record_request_serialization() {
        let req = DeleteRecordRequest {
            repo: "did:plc:bob",
            collection: "app.bsky.feed.like",
            rkey: "like_123",
            swap_record: Some("cid_record"),
            swap_commit: None,
        };

        let json_val = serde_json::to_value(&req).expect("serialization failed");
        assert_eq!(json_val["repo"], "did:plc:bob");
        assert_eq!(json_val["collection"], "app.bsky.feed.like");
        assert_eq!(json_val["rkey"], "like_123");
        assert_eq!(json_val["swapRecord"], "cid_record");
        assert!(json_val.get("swapCommit").is_none());
    }

    #[test]
    fn test_xrpc_error_response_deserialization() {
        let json_str = r#"{"error": "use_dpop_nonce", "message": "Proof requires nonce"}"#;
        let parsed: XrpcErrorResponse =
            serde_json::from_str(json_str).expect("deserialize xrpc error");
        assert_eq!(parsed.error.as_deref(), Some("use_dpop_nonce"));
        assert_eq!(parsed.message.as_deref(), Some("Proof requires nonce"));

        let empty_json = "{}";
        let empty_parsed: XrpcErrorResponse =
            serde_json::from_str(empty_json).expect("deserialize empty xrpc error");
        assert!(empty_parsed.error.is_none());
        assert!(empty_parsed.message.is_none());
    }
}
