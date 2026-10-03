# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-03

Initial open-source production release of `skybase`: the turn-key Micro-AppView engine and developer platform for the AT Protocol (Firebase for ATProto, powered by `skyauth`).

### Added

- **Jetstream Firehose Ingestion (`skybase::ingest`)**:
  - Resilient WebSocket connection to ATProto Jetstream relays with exponential backoff and jitter.
  - Automatic collection filtering and zstandard decompression.
  - Persistent cursor tracking with SQLite storage to resume seamlessly across restarts.
  - High-throughput asynchronous event dispatcher with channel backpressure.

- **Sovereign PDS Operations (`skybase::repo`)**:
  - `PdsRepoClient` with DPoP-signed XRPC authentication via `skyauth`.
  - Full record lifecycle support: `create_record`, `put_record`, `get_record`, and `delete_record`.
  - Strongly typed repository error handling and CID validation.

- **Embedded Micro-AppView Indexing (`skybase::index`)**:
  - Embedded SQLite WAL engine with JSON1 support and automated schema migrations.
  - High-performance sharded read/write connection pooling.
  - Record indexing with deduplication, collection queries, and cursor pagination.

- **Security & Resilience Guarantees**:
  - 100% safe Rust: crate-level `#![forbid(unsafe_code)]` with zero `unsafe` blocks.
  - Strict clippy safety guard denying all unwraps, expects, panics, and missing documentation.
  - Zero-cost architecture with typed `Result<T, SkybaseError>` across all public APIs.
