//! Sovereign Personal Data Server (PDS) write client and repository mutation primitives.
//!
//! Provides the [`PdsRepoClient`] for executing cryptographically bound, DPoP-signed
//! mutations (`createRecord`, `deleteRecord`) directly against a user's PDS under
//! Topology A (Client-Sovereign / Zero Custody).

pub mod client;
pub mod tid;
pub mod types;

pub use client::PdsRepoClient;
pub use tid::{
    decode_tid_timestamp_secs, decode_tid_timestamp_us, generate_tid, TidGenerator,
    MAX_PLAUSIBLE_TID_EPOCH_SECS, MIN_PLAUSIBLE_TID_EPOCH_SECS,
};
pub use types::{
    extract_did_for_collection, extract_did_from_at_uri, format_at_uri, parse_at_post_uri,
    split_at_uri, validate_rkey, CreateRecordRequest, CreateRecordResult, DeleteRecordRequest,
    PutRecordRequest, PutRecordResult, RecordView, XrpcErrorResponse,
};
