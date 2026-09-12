# 🤖 Agent Coding & Engineering Handover Guide — `skybase`

Welcome, Agent! This document is designed specifically for AI coding assistants (Antigravity, Cursor, Claude, Copilot, etc.) interacting with this codebase. It establishes the architectural standards, non-negotiable safety gates, and engineering invariants for [`skybase`](../skybase).

---

## 🎯 Repository Standards & Reference Blueprint

This project is built following the **Production-Grade Rust Best Practices & Architecture Standards** defined in the user's reference repository:
- **Reference Repo**: [`rust-best-practices`](/Users/mike10010100/git/rust-best-practices)
- **Architecture Guide**: [`BEST_PRACTICES.md`](/Users/mike10010100/git/rust-best-practices/BEST_PRACTICES.md)
- **Tooling Blueprint**: [`TOOLING.md`](/Users/mike10010100/git/rust-best-practices/TOOLING.md)
- **Agent Blueprint**: [`agents.md`](/Users/mike10010100/git/rust-best-practices/agents.md)
- **Sibling Ecosystem**: [`skyauth`](../skyauth) and [`for-your-consideration`](../for-your-consideration)

When working in `skybase`:
- Treat every pattern as a strict standard, not an incidental implementation detail.
- Never lower quality gates, weaken lint rules, or bypass defensive error handling for convenience.
- Any new features, modules, or refactors must adhere to the same uncompromising resilience standard.

---

## 🛡️ Core Non-Negotiable Invariants

To preserve the extreme quality and security of `skybase`, you **must** strictly adhere to the following rules:

### 1. Zero Unsafe Code
The crate root ([`src/lib.rs`](src/lib.rs)) and any future binary roots (`src/main.rs`) enforce:
```rust
#![forbid(unsafe_code)]
```
Never attempt to use `unsafe`, weaken this attribute, or introduce dependencies that circumvent compiler safety guarantees.

### 2. Strict Crate-Root Safety Guard
Every crate root enforces the strict compiler lint safety guard:
```rust
#![deny(
    clippy::all,
    clippy::unwrap_used,     // Deny unwrap(), force explicit error handling
    clippy::expect_used,     // Deny expect(), force structured errors
    clippy::panic,           // Deny panic!, force error bubbling
    clippy::todo,            // Deny todo! placeholders in production
    clippy::unimplemented,   // Deny unimplemented! macros
    missing_docs,            // Enforce public API documentation
    rust_2018_idioms         // Use modern Rust idioms
)]
```

### 3. Zero Production Panics & Typed Errors
- **Banned in production**: `.unwrap()`, `.expect()`, `panic!`, `todo!`, `unimplemented!`.
- All fallible operations must return a strongly typed `Result<T, SkybaseError>` using variants defined in [`src/error.rs`](src/error.rs).
- Use `?`, `match`, or `if let` to propagate errors safely to callers.
- In test modules (`#[cfg(test)]`), allow unwrap via `#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]`.

### 4. User Data Sovereignty Invariant
- **Never Custodial**: `skybase` never stores user-authored content exclusively in a proprietary database.
- **Sovereign Write First**: Records are signed with DPoP credentials and committed via XRPC (`com.atproto.repo.createRecord`, `applyWrites`) directly to the user's sovereign Personal Data Server (PDS) Merkle Search Tree (MST).
- **Ephemeral Read Projections**: The local `skybase-index` (Micro-AppView) is an ephemeral, rebuildable read projection of the global firehose (Jetstream), never a custodial walled garden.

### 5. Defensive Concurrency, Locks & Time
- **Clock-Warp Safety**: Always compute elapsed time using `now.saturating_duration_since(earlier)` or `.map_or(0, ...)`. Never use raw `.duration_since()` as monotonic clocks can jump backwards under VM migrations or NTP syncs.
- **Drift-Free Scheduling**: Recurring tasks (e.g. Jetstream cursor commits, cache evictions, token renewals) must calculate next runs relative to the previous anchor timestamp or use `tokio::time::interval`, not relative `Instant::now() + delay`.
- **Never Hold Locks Across `.await` Points**: Synchronous mutex or `RwLock` guards must always be dropped before executing any `.await`, `sleep()`, or network I/O to prevent cooperative task starvation.
- **SQLite WAL & Non-Blocking Partitioning**: Embedded SQLite storage operates in Write-Ahead Logging (WAL) mode with busy timeout handling, supporting concurrent read queries alongside atomic writes. High-concurrency live query fanout uses bounded, non-blocking broadcast channels (`tokio::sync::broadcast`).
- **Task Leak Prevention & Cancellation**: All background tasks (firehose ingestion, event dispatching, health checkers) must be tracked in a managed `tokio::task::JoinSet` tied to a `tokio_util::sync::CancellationToken`. On shutdown or timeout, tasks must be cleanly aborted and joined.
- **Panic Boundaries**: Worker tasks executing foreign or user-supplied event closures must wrap execution in `std::panic::AssertUnwindSafe(...).catch_unwind()`.

### 6. 100% Documentation Coverage
- All public structs, fields, constants, enums, modules, and functions must have descriptive documentation comments (`missing_docs` is denied).
- Bare URLs in documentation must be enclosed in angle brackets (e.g. `<https://bsky.social>`).

### 7. The "Engine vs. Interface" Mandate (Zero-Rust Barrier for App Developers)
- **Never Force App Developers to Write Rust**: While the core engine, daemon, and native crate are 100% pure Safe Rust, the primary developer interface for application developers must be **language-agnostic HTTP/WebSocket endpoints and the `@skybase/client` TypeScript SDK**.
- **The PocketBase / Supabase Blueprint**: Like PocketBase (Go engine, JS/Dart users) or Supabase (Elixir/Go/C engine, JS/Python users), Rust is an invisible engine superpower delivering sub-millisecond firehose filtering, 50MB RAM usage, and zero crashes.
- **Zero-Toolchain Local DX**: Application developers must never be required to install Rust, Cargo, or C++ compilers. The daemon must be distributable as a standalone precompiled binary (`npx skybase dev`, `brew install skybase`).
- **Extensibility Without Recompilation**: Event triggers and functions must support HTTP Webhook dispatching (e.g. to Next.js API routes or serverless handlers) and optional embedded scripting (QuickJS/Wasm), ensuring non-Rust developers can write backend logic without recompiling the daemon.

### 8. Scope Discipline vs. Safety Bar (The Narrow Wedge Mandate)
- **Never Compromise Safety for Schedule**: When faced with timeline pressure, never lower quality gates, skip test suites, allow `.unwrap()`, or bypass compiler safety. Instead, **ruthlessly narrow the functional scope**.
- **Phase 1.5 Priority**: Immediate engineering effort is focused on the **Thin Vertical Slice**:
  $$\text{DPoP Login} \longrightarrow \text{PDS write} \longrightarrow \text{Jetstream Ingest} \longrightarrow \text{SQLite Upsert} \longrightarrow \text{Live Query}$$
  De-risk this single loop end-to-end before expanding to peripheral pillars.

### 9. Three Operating Topologies & Honest Custody Boundaries
- **Topology A (Client-Sovereign, Zero Custody)**: The default for web and mobile frontends. Authentication and writes go directly from the client device to the user's PDS. `skybase` indexes public commits from Jetstream into SQLite WAL. **Skybase holds zero user credentials.**
- **Topology B (Daemon-Custodial)**: Used for autonomous bots and feed generators. In this mode, the daemon is an OAuth credential custodian. All refresh tokens and private keys stored at rest must be encrypted using **AES-256-GCM** (`SKYBASE_MASTER_KEY`).
- **Topology C (Token-Mediated Session Proxy)**: Used for static single-page apps hosted on GitHub Pages, Wisp, or Tangled. Skybase mediates confidential OAuth sessions, maintaining background refresh token rotation so user sessions remain active indefinitely (>2 weeks) without browser storage eviction killing logins.

---

## 🏗️ Architecture Quick Reference

| Component / Module | File / Directory | Responsibility | Status |
| :--- | :--- | :--- | :--- |
| **`Skybase`** | [`src/lib.rs`](src/lib.rs) | Unified engine facade and client entry point. | **Active (Phase 1.5)** |
| **`SkybaseConfig`** | [`src/lib.rs`](src/lib.rs) | Builder and configuration parameters (OAuth endpoints, Jetstream URL). | **Active (Phase 1.5)** |
| **`SkybaseError`** | [`src/error.rs`](src/error.rs) | Root strongly-typed error enum powered by `thiserror`. | **Active (Phase 1.5)** |
| **`skybase::index`** | [`src/index/`](src/index/) | Embedded SQLite WAL store, JSON1 query engine, and live query broadcast bus. | **Active (Phase 1.5)** |
| **`skybase::ingest`** | [`src/ingest/`](src/ingest/) | Resilient WebSocket Jetstream consumer, monotonic cursor tracker, exponential backoff, mock server. | **Active (Phase 1.5)** |
| **`skybase::repo`** | [`src/repo/`](src/repo/) | Sovereign PDS client, DPoP signing, nonce retry challenge recovery, TID generator. | **Active (Phase 1.5)** |
| **`skybase-auth`** | `src/auth/` | Session coordinator, auto-refresh workers, and Axum/Tower guards (wrapping `skyauth`). | Planned (Phase 2) |
| **`skybase-events`** | `src/events/` | Reactive event triggers (`on_create`, `on_delete`) with durable monotonic cursor persistence. | Planned (Phase 3) |
| **`skybase-storage`** | `src/storage/` | Sovereign PDS blob upload with SHA-256 CID verification, magic byte checks, and edge CDN proxying. | Planned (Phase 2) |
| **`skybase-server`** | `src/server/` | Standalone PocketBase-style daemon with REST/WS gateway and embedded Web Admin dashboard. | Planned (Phase 3) |
| **`skybase-rules`** | `src/rules/` | Declarative access control rules and schema policy guards. | Planned (Phase 3) |

---

## 🧪 Testing & Verification Standard for Agents

Whenever you introduce a new feature or modify existing logic in `skybase`:
1. **Unit & Edge-Case Tests**: Add corresponding test cases in `tests/` covering both happy paths and failure injection paths (e.g. network drops, malformed JSON, Jetstream disconnects).
2. **Doc-Tests**: All public functions and structs must include runnable doc-tests (`cargo test --doc`).
3. **Property Tests**: If manipulating time intervals, cursor sequences, or byte parsing, add a `proptest!` block.
4. **Hermetic Mock Testing**: Use wiremock or in-process mock servers to test OAuth 2.1, PAR, DPoP, and PDS record operations without hitting live production endpoints.
5. **Formal Invariant Preservation**: When interacting with `skyauth` cryptographic kernels (SSRF filters, constant-time comparisons, PKCE validators), preserve all formally verified invariants and anti-vacuity gates.

---

## ⚡ Mandatory Pre-Completion Checklist

Before reporting your work as complete, you **must execute and pass every step** of this pipeline:

```bash
# 1. Check code formatting
cargo fmt --all -- --check

# 2. Check strict clippy rules (must have 0 warnings with -D warnings)
cargo clippy --all-targets -- -D warnings

# 3. Run all unit and integration test suites
cargo test --all-targets

# 4. Run documentation tests
cargo test --doc

# 5. Dependency security & policy scan
cargo deny check
```

---

## 📚 Related Documentation

- **[`README.md`](README.md)**: High-level overview, quick start, and architecture summary.
- **[`PRD.md`](PRD.md)**: Comprehensive Product Requirements Document, Firebase deconstruction, and implementation roadmap.
- **[`skyauth`](../skyauth)**: High-assurance ATProto OAuth 2.1, DPoP, and PKCE identity engine.
- **[`for-your-consideration`](../for-your-consideration)**: Sibling ATProto high-throughput recommendation and feed generation engine.
- **[`rust-best-practices`](/Users/mike10010100/git/rust-best-practices)**: Authoritative architectural blueprint and tooling guide.
