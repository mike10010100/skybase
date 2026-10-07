//! ATProto Chat (`chat.bsky.convo.*`) client and data models.
//!
//! - [`ChatClient`]: authenticated XRPC client for the Bluesky chat service.
//! - Strongly typed request/response models for conversation listing, requests,
//!   message send/read, and conversation lookup.

pub mod client;
pub mod types;

pub use client::{ChatClient, ATPROTO_CHAT_PROXY_DID, DEFAULT_CHAT_ENDPOINT};
pub use types::{
    AcceptConvoRequest, AcceptConvoResponse, ConvoMember, ConvoView, GetMessagesResponse,
    ListConvoRequestsResponse, ListConvosResponse, MessageSender, MessageView, SendMessagePayload,
    SendMessageRequest, UpdateReadRequest,
};
