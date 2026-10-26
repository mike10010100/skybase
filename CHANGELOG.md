# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.6] - 2026-10-26

### Security

- **Transitive Cryptographic & Runtime Advisory Remediation** (`Cargo.toml`, `Cargo.lock`, `deny.toml`): Bumped `skyauth` to `0.3.8`, upgraded `crypto-bigint` to `0.7.10` (`RUSTSEC-2026-0387`) and `ctutils` to `0.4.6` (`RUSTSEC-2026-0385`), and banned vulnerable prior releases (`crypto-bigint < 0.7.6`, `ctutils < 0.4.4`, `tokio < 1.53.3`) in `deny.toml`.

## [0.1.5] - 2026-10-21

### Security

- **eMudhra Intermediate CA Compromise Mitigation** (`Cargo.toml`, `Cargo.lock`, `deny.toml`, `tests/emudhra_tls_distrust_tests.rs`): Bumped `skyauth` to `0.3.7`, enforced `rustls-webpki >= 0.103.16` and `webpki-roots >= 1.0.10` in `[dependencies]`, and banned vulnerable versions (`rustls-webpki@<0.103.16`, `webpki-roots@<1.0.10`) in `deny.toml` so all outbound HTTPS/WSS connections (AppView, PDS XRPC, and Jetstream firehose ingestion) distrust the compromised eMudhra intermediate CAs across both `emSign` and Sectigo `AAA Certificate Services` cross-signed chains.

## [0.1.4] - 2026-10-18

### Added

- **Actor Typeahead & Profile Avatar Support** (`src/appview.rs`): Added `ActorSuggestion` and `AppViewClient::search_actors_typeahead` (`app.bsky.actor.searchActorsTypeahead`), enriched `ActorProfile` with `did` and `avatar`, and updated `AppViewClient::resolve_handle` to automatically assume `.bsky.social` for bare single-label handles.
- **Pre-Flight Login Identity Preview** (`src/lib.rs`): Added `LoginIdentityPreview` and `Skybase::preview_login` combining `skyauth`'s OAuth provider discovery (`OAuthLoginPreview`) with AppView profile enrichment.

## [0.1.3] - 2026-10-14

### Changed

- **Dependencies**: Upgraded `axum` (`0.8`), `thiserror` (`2.0`), `tokio-tungstenite` (`0.30`), and `rusqlite` (`0.39`) for compatibility with `skyauth 0.3.5`, and updated MSRV to Rust `1.89`.

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
