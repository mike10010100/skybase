# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-10-10

### Changed

- **Dependabot Configuration**: Added `.github/dependabot.yml` configured with weekly checks and grouped PR updates for `cargo` and `github-actions` dependencies.

## [0.1.1] - 2026-10-07

Release cut so downstream crates (`skybouncer`) can consume the read-only
AppView, chat, and lexicon primitives from crates.io rather than only from path
dependencies.

### Added

- **Read-only AppView client (`skybase::appview`)**: `AppViewClient` with
  `fetch_profile`, `fetch_post`, `fetch_follows`, `fetch_followers`,
  `fetch_follow_records`, `resolve_handle`, CDN image fetch, and the
  `ActorProfile` / `PostView` models.
- **ATProto Chat client (`skybase::chat`)**: `ChatClient` (login, list convos,
  send/update messages) and `ConvoView` / `MessageView` / `MessageSender` /
  `SendMessagePayload` models.
- **Lexicon models (`skybase::lexicon`)**: follow / list / listitem / listblock /
  modlist / post record types and facet helpers (`extract_link_facets`,
  `format_system_time_iso8601`, `now_iso8601`, and associated embed/facet types).
- **Error cause preservation**: `SkybaseError` variants for the AppView/chat
  paths.

### Changed

- No breaking API changes: additive modules only.

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
