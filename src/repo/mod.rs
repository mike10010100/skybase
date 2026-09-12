//! Sovereign Personal Data Server (PDS) write client and repository mutation primitives.
//!
//! Provides the [`PdsRepoClient`] for executing cryptographically bound, DPoP-signed
//! mutations (`createRecord`, `deleteRecord`) directly against a user's PDS under
//! Topology A (Client-Sovereign / Zero Custody).

pub mod client;
pub mod tid;
pub mod types;

pub use client::PdsRepoClient;
pub use tid::{generate_tid, TidGenerator};
pub use types::{
    format_at_uri, validate_rkey, CreateRecordRequest, CreateRecordResult, DeleteRecordRequest,
    XrpcErrorResponse,
};
