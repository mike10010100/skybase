//! Strongly typed data structures for ATProto Chat (`chat.bsky.convo.*`).

use serde::{Deserialize, Serialize};

use crate::lexicon::{extract_link_facets, Facet};

/// Member within an ATProto Chat conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvoMember {
    /// Decentralized identifier (DID) of the member.
    pub did: String,
    /// Handle of the member if available.
    #[serde(default)]
    pub handle: Option<String>,
    /// Display name of the member if available.
    #[serde(rename = "displayName", default)]
    pub display_name: Option<String>,
}

/// Sender details for an individual chat message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageSender {
    /// DID of the message sender.
    pub did: String,
}

/// Individual message representation in `chat.bsky.convo.defs#messageView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageView {
    /// Unique message identifier string.
    pub id: String,
    /// Revision identifier.
    #[serde(default)]
    pub rev: String,
    /// Text payload of the message.
    pub text: String,
    /// Rich text facets associated with the message text if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<Facet>>,
    /// Sender metadata.
    pub sender: MessageSender,
    /// ISO 8601 timestamp string when the message was sent.
    #[serde(rename = "sentAt")]
    pub sent_at: String,
}

/// Conversation representation in `chat.bsky.convo.defs#convoView`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvoView {
    /// Unique conversation identifier string.
    pub id: String,
    /// Revision identifier.
    #[serde(default)]
    pub rev: String,
    /// Members participating in the conversation.
    #[serde(default)]
    pub members: Vec<ConvoMember>,
    /// Most recent message in the conversation, if any.
    #[serde(rename = "lastMessage", default)]
    pub last_message: Option<MessageView>,
    /// Number of unread messages for the authenticated caller.
    #[serde(rename = "unreadCount", default)]
    pub unread_count: u64,
    /// Status of the conversation for the caller ("request" | "accepted").
    #[serde(default)]
    pub status: Option<String>,
}

/// Response payload from `chat.bsky.convo.listConvos`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListConvosResponse {
    /// Conversations returned in the current page.
    #[serde(default)]
    pub convos: Vec<ConvoView>,
    /// Pagination cursor string if more conversations exist.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Response payload from `chat.bsky.convo.listConvoRequests`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListConvoRequestsResponse {
    /// Conversation requests returned in the current page.
    #[serde(default)]
    pub requests: Vec<ConvoView>,
    /// Pagination cursor string if more conversation requests exist.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Request body for `chat.bsky.convo.acceptConvo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptConvoRequest {
    /// Target conversation ID to accept.
    #[serde(rename = "convoId")]
    pub convo_id: String,
}

/// Response payload from `chat.bsky.convo.acceptConvo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptConvoResponse {
    /// Revision identifier when accepted, or None if already accepted.
    #[serde(default)]
    pub rev: Option<String>,
}

/// Response payload from `chat.bsky.convo.getMessages`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetMessagesResponse {
    /// Messages returned in the current page.
    #[serde(default)]
    pub messages: Vec<MessageView>,
    /// Pagination cursor string if more messages exist.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Request payload to send a message via `chat.bsky.convo.sendMessage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessagePayload {
    /// Content of the message.
    pub text: String,
    /// Rich text facets (links, mentions, tags) associated with the message text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<Facet>>,
}

impl SendMessagePayload {
    /// Creates a new message payload, automatically extracting link facets from `text`.
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        let facets = extract_link_facets(&text);
        Self {
            text,
            facets: if facets.is_empty() {
                None
            } else {
                Some(facets)
            },
        }
    }

    /// Creates a new message payload with explicitly provided facets.
    #[must_use]
    pub fn with_facets(text: impl Into<String>, facets: Option<Vec<Facet>>) -> Self {
        Self {
            text: text.into(),
            facets,
        }
    }
}

/// Request body for `chat.bsky.convo.sendMessage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageRequest {
    /// Target conversation ID.
    #[serde(rename = "convoId")]
    pub convo_id: String,
    /// Message details.
    pub message: SendMessagePayload,
}

/// Request body for `chat.bsky.convo.updateRead`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateReadRequest {
    /// Target conversation ID.
    #[serde(rename = "convoId")]
    pub convo_id: String,
    /// Message ID marked as read.
    #[serde(rename = "messageId")]
    pub message_id: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::lexicon::FacetFeature;

    #[test]
    fn test_send_message_payload_serialization() {
        let payload_no_links = SendMessagePayload::new("Plain text message");
        assert!(payload_no_links.facets.is_none());
        let json_no_links = serde_json::to_value(&payload_no_links).unwrap();
        assert_eq!(json_no_links["text"], "Plain text message");
        assert!(json_no_links.get("facets").is_none());

        let payload_with_link = SendMessagePayload::new("Visit https://example.com now");
        assert!(payload_with_link.facets.is_some());
        let json_with_link = serde_json::to_value(&payload_with_link).unwrap();
        assert_eq!(json_with_link["text"], "Visit https://example.com now");

        let facets = json_with_link["facets"].as_array().unwrap();
        assert_eq!(facets.len(), 1);
        assert_eq!(facets[0]["index"]["byteStart"], 6);
        assert_eq!(facets[0]["index"]["byteEnd"], 25);
        assert_eq!(
            facets[0]["features"][0]["$type"],
            "app.bsky.richtext.facet#link"
        );
        assert_eq!(facets[0]["features"][0]["uri"], "https://example.com");
    }

    #[test]
    fn test_send_message_payload_deserialization() {
        let json_str = r#"{
            "text": "Hello https://test.org",
            "facets": [
                {
                    "index": { "byteStart": 6, "byteEnd": 22 },
                    "features": [
                        { "$type": "app.bsky.richtext.facet#link", "uri": "https://test.org" }
                    ]
                }
            ]
        }"#;

        let payload: SendMessagePayload = serde_json::from_str(json_str).unwrap();
        assert_eq!(payload.text, "Hello https://test.org");
        let facets = payload.facets.unwrap();
        assert_eq!(facets.len(), 1);
        assert_eq!(facets[0].index.byte_start, 6);
        assert_eq!(facets[0].index.byte_end, 22);
        match &facets[0].features[0] {
            FacetFeature::Link { uri } => assert_eq!(uri, "https://test.org"),
            _ => panic!("expected Link"),
        }
    }
}
