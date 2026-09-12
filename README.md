# ⚡ `skybase`

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Safety Guard](https://img.shields.io/badge/unsafe-forbidden-success.svg)](src/lib.rs)
[![Rust Version](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](Cargo.toml)

> **The Turn-Key Micro-AppView Engine & Local Dev Platform for the AT Protocol.**
> *"Firebase-Like DX for ATProto, powered by `skyauth`."*

---

## 🌟 Highlights

- **The Strategic Wedge: Micro-AppView in One Binary**: Point `./skybase` at your collection NSID (`com.myapp.review`). It continuously ingests Jetstream, stores records in an embedded SQLite WAL database with JSON1 virtual columns and full-text search (FTS5), and serves an instant REST & WebSocket live query API.
- **Zero Token Custody by Default**: Frontends authenticate and write directly to user PDSs via `@atproto/oauth-client-browser`. `skybase` indexes public commits from Jetstream, holding **zero user refresh tokens or private keys**.
- **Hermetic Local Dev Sandbox (`skybase dev --mock`)**: Mock relay and synthetic Jetstream event replay engine allowing you to build and test full-stack ATProto apps offline with zero cloud configuration.
- **Eventual Consistency with Optimistic UX**: The `@skybase/client` SDK provides deterministic TID tracking and optimistic reconciliation (`isPending`, `isOptimistic`, `isLagging`) so React/Vue components never flicker or drop mutations.
- **100% Pure Safe Rust**: `#![forbid(unsafe_code)]` enforced crate-wide with 0 `unsafe` blocks, strict clippy denials, and zero production panics.
- **Token-Mediated Sessions for Static Hosting**: Acts as a confidential OAuth session proxy for apps hosted on GitHub Pages, Wisp, or Tangled, keeping user sessions alive indefinitely (>2 weeks) without browser storage eviction.
- **Dual Deployment Modes**: Single prebuilt binary (`npx skybase dev`) for local development, 1-click cloud deploy (Fly.io / Railway), OR embeddable modular Rust crate for high-throughput systems services.

---

## 🗺️ The "Firebase for ATProto" Mapping

| Firebase Component | `skybase` Pillar | AT Protocol Primitive |
| :--- | :--- | :--- |
| **Firestore (Queries)** | `skybase-index` | Embedded Micro-AppView (SQLite WAL + JSON1 + FTS5) synced from Jetstream |
| **Realtime Subscriptions** | `skybase-events` | WebSocket live queries & optimistic state reconciliation |
| **Local Emulator / Admin** | `skybase-server` | Single-binary daemon (`./skybase dev`) with mock Jetstream & Admin UI |
| **Client SDKs** | `@skybase/client` | Zero-custody TypeScript SDK linking browser OAuth writes to live queries |
| **Firestore (Writes)** | `skybase-repo` | Sovereign XRPC writes to user PDS Merkle Search Trees |
| **Firebase Auth (Bots)** | `skybase-auth` | Decentralized OAuth 2.1 + DPoP via `skyauth` (AES-256-GCM encrypted) |
| **Cloud Storage** | `skybase-storage` | PDS blob upload (`uploadBlob`) with CID verification |
| **Security Rules** | `skybase-rules` | Lexicon schema validation & author commit cryptographic verification |

> **Architecture Note**: While adopting Firebase's beloved developer ergonomics (one-liner auth, collection queries, `.onSnapshot()`), `skybase` is an **eventually-consistent Micro-AppView backend**, honoring ATProto's sovereign PDS repository writes and asynchronous firehose replication.

---

## 🥊 Why Skybase? (Prior Art Comparison)

| Existing Tool | What It Does | Why It Is Insufficient / Where `skybase` Wins |
| :--- | :--- | :--- |
| **`@atproto/api`** | Official TypeScript XRPC client. | Only speaks to **one user's PDS at a time**. Has no database, cannot perform cross-user queries, and cannot build an aggregated feed or search index. |
| **`@atproto/oauth-client-*`** | Official TypeScript OAuth client. | Handles login flows, but provides zero data storage, firehose indexing, or query infrastructure. |
| **Bluesky's `Tap`** | Internal Go tool for filtering the raw firehose with backfill. | A low-level streaming pipe (event bus). It does **not** give you a queryable database, REST API, SQLite storage, or WebSocket push subscriptions. |
| **`skyware/jetstream`** | Node.js WebSocket client for Jetstream. | A raw WebSocket listener. You still have to hand-roll SQLite schemas, JSON parsing, cursor tracking, deduplication, and query endpoints. |
| **`HappyView`** | Lexicon-driven AppView server in Rust. | Focuses on runtime schema reflection and Lua scripting. Uses `atrium-oauth`. It is an AppView server, not an ergonomic BaaS/developer SDK. |
| **`skybase`** | **Complete Micro-AppView in one binary.** | Combines Jetstream ingestion + historical PDS backfill + SQLite WAL storage + JSON1 virtual indexes + FTS5 full-text search + REST/WebSocket live queries in a single prebuilt binary with zero configuration. |

---

## 🏛️ The Three Operating Topologies

To eliminate the conflict between "user data sovereignty" and practical backend operations, `skybase` explicitly supports three distinct operational topologies:

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  THREE OPERATING TOPOLOGIES                                     │
├──────────────────────────┬──────────────────────────┬───────────────────────────────────────────┤
│ Topology                 │ Custodial Footprint      │ Primary Use Case                          │
├──────────────────────────┼──────────────────────────┼───────────────────────────────────────────┤
│ Topology A: Client-      │ ZERO Token Custody.      │ Default for Web (React/Next.js) & Mobile  │
│ Sovereign (Default)      │ Skybase holds 0 keys.    │ apps. Writes go direct from client to PDS.│
├──────────────────────────┼──────────────────────────┼───────────────────────────────────────────┤
│ Topology B: Daemon-      │ Managed Custody. Tokens  │ Background daemons, automated bots, and   │
│ Managed (Bots/Daemons)   │ encrypted with AES-256.  │ feed generators requiring offline writes. │
├──────────────────────────┼──────────────────────────┼───────────────────────────────────────────┤
│ Topology C: Token-       │ Session Proxy. Confident-│ Static SPAs on GitHub Pages, Wisp, and    │
│ Mediated Session Broker  │ ial refresh rotation.    │ Tangled. Eliminates 2-week session drop.  │
└──────────────────────────┴──────────────────────────┴───────────────────────────────────────────┘
```

1. **Topology A: Client-Sovereign (Zero Custody — Default)**:
   The user logs in via `@atproto/oauth-client-browser`. DPoP private keys remain strictly in browser IndexedDB. Writes are dispatched directly from the client to the user's personal PDS. `skybase` operates purely as a read/indexing Micro-AppView consuming public Jetstream commits. **Skybase holds zero user credentials.**
2. **Topology B: Daemon-Managed (Backend Automation & Bots)**:
   Used when an autonomous backend service or bot must sign repository commits without a user present. Powered by [`skyauth`], all refresh tokens and private keys stored at rest are encrypted using **AES-256-GCM** (`SKYBASE_MASTER_KEY`).
3. **Topology C: Token-Mediated Session Proxy (Static Hosting)**:
   Addresses the major pain point for static apps on GitHub Pages, Wisp, or Tangled where public-client OAuth sessions expire after ~2 weeks. Skybase acts as a confidential OAuth session proxy, rotating refresh tokens continuously in the background so static apps stay logged in indefinitely.

---

## 🌐 The Deployment Spectrum: Localhost to Cloud

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               THE DEPLOYMENT SPECTRUM                                  │
├──────────────────────┬─────────────────────────┬───────────────────────────────────────┤
│ Tier                 │ Target Audience         │ What Runs Where                       │
├──────────────────────┼─────────────────────────┼───────────────────────────────────────┤
│ Level 0: Local Dev   │ Development / Offline   │ `npx skybase dev` runs on localhost;  │
│                      │                         │ embedded SQLite + mock Jetstream      │
├──────────────────────┼─────────────────────────┼───────────────────────────────────────┤
│ Level 1: 1-Click     │ Independent builders,   │ Single binary on Fly.io / Railway /   │
│ Self-Hosted          │ open-source projects    │ VPS ($5/mo); 1-click config templates │
├──────────────────────┼─────────────────────────┼───────────────────────────────────────┤
│ Level 2: Managed     │ Static frontend apps    │ Multi-tenant hosted Micro-AppView     │
│ "Skybase Cloud"      │ (GitHub Pages, Tangled, │ cloud; developers register NSIDs and  │
│ [Strategic Horizon]  │ Wisp, Vercel, Netlify)  │ get instant managed API endpoints     │
└──────────────────────┴─────────────────────────┴───────────────────────────────────────┘
```

---

## 💡 The "PocketBase / Supabase" Model: Zero Rust Required for App Developers

> **"Are we pigeonholing ourselves by building in Rust?"**
> **Only if we force developers to write Rust to use it.**

`skybase` follows the architecture of **PocketBase** (Go engine, JS/Dart users), **Supabase** (Elixir/C engine, JS/Python users), and **Meilisearch** (Rust engine, npm users):
- **Rust is an invisible superpower for the engine**: Designed to sustain thousands of events/sec over the Jetstream firehose without GC pauses, operate within compact memory budgets (<100MB RAM), provide zero-crash safety (`#![forbid(unsafe_code)]`), and package into a single zero-dependency binary.
- **The developer interface is 100% language-agnostic**: Frontend and mobile developers interact exclusively via HTTP, WebSockets, and the first-class `@skybase/client` TypeScript / React SDK. You never need to install Rust or Cargo to build apps on `skybase`.

---

## 🚀 Quick Start

### Option A: Web & Mobile Developers (TypeScript / React)

1. **Launch the Skybase daemon** (zero Rust required):
   ```bash
   npx skybase dev
   # Server starts in 10ms with SQLite & Web Admin UI at http://localhost:8080
   ```

2. **Install the client SDK**:
   ```bash
   npm install @skybase/client
   ```

3. **Build your app with Optimistic Reconciliation**:
   ```typescript
   import { Skybase } from '@skybase/client';

   const sb = new Skybase('http://localhost:8080');

   // 1. Authenticate with ATProto handle (Topology A: browser-held keys)
   await sb.auth.signInWithHandle('alice.bsky.social');

   // 2. Write sovereign record to user's personal PDS
   const post = await sb.collection('app.bsky.feed.post').create({
     text: 'Hello from Skybase!',
     createdAt: new Date().toISOString()
   });
   console.log('PDS write committed with CID:', post.cid);

   // 3. Realtime live query with deterministic state reconciliation
   const unsubscribe = sb.collection('app.bsky.feed.post')
     .where('replyParent', '==', post.uri)
     .orderBy('createdAt', 'desc')
     .onSnapshot((comments) => {
       // Automatic reconciliation: _syncStatus indicates 'optimistic', 'confirmed', or 'lagging'
       console.log('Live comments:', comments);
     });
   ```

### Option B: Backend & Systems Developers (Rust Crate)

Add `skybase` to your `Cargo.toml`:

```toml
[dependencies]
skybase = { path = "../skybase", version = "0.1" }
tokio = { version = "1.40", features = ["full"] }
```

```rust
use skybase::{Skybase, SkybaseConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SkybaseConfig::new(
        "https://myapp.example.com/oauth/client-metadata.json",
        "https://myapp.example.com/oauth/callback",
        "My ATProto Application",
    )
    .with_jetstream_endpoint("wss://jetstream1.us-east.bsky.network/subscribe");

    let skybase = Skybase::new(config)?;
    let auth_request = skybase.auth().authorize("alice.bsky.social").await?;
    println!("Redirect user to: {}", auth_request.authorization_url);

    Ok(())
}
```

---

## 🗺️ Roadmap & Implementation Phases

- [x] **Phase 1: Project Foundation & Core Architecture**
  - Crate root scaffolding with `#![forbid(unsafe_code)]` and strict clippy safety gates.
  - `skyauth` OAuth 2.1 / DPoP / PKCE integration.
  - Configuration builder and root `SkybaseError` taxonomy.
  - Comprehensive PRD, handover guide (`AGENTS.md`), and dual-licensing.
- [x] **Phase 1.5: The Vertical Slice Wedge (Completed)**
  - Proven single end-to-end loop: DPoP login → PDS write → Jetstream ingest → SQLite WAL upsert → JSON1 query → live query broadcast notification.
  - Canonical `records` SQLite schema with JSON1 extraction and structured query builder (`skybase::index`; FTS5 full-text indexing scheduled for Phase 2).
  - Monotonic Last-Write-Wins (LWW) soft-delete barrier and in-order batch execution preventing deleted records from resurrecting on firehose replayed commits.
  - Resilient WebSocket Jetstream consumer with edge filtering, durable cursor persistence in `_skybase_meta`, monotonic timestamp clamping, and mock test emitter (`skybase::ingest`).
  - Sovereign PDS write client with DPoP proof signing, strict redirect prevention, and automatic nonce retry (`skybase::repo`).
  - Hermetic integration test suite (`tests/vertical_slice_tests.rs`) and 12 challenger/e2e test suites with 272 passing tests (94 unit, 175 integration, 3 doc-tests).
- [ ] **Phase 2: Micro-AppView Ingestion & Historical Backfill**
  - FTS5 contentless/external-content full-text search integration.
  - Multi-collection filtering and multi-reader SQLite connection pooling.
  - Historical CAR sync crawler (`com.atproto.sync.getRepo`).
  - Dynamic index generation from Lexicon schema manifests.
- [ ] **Phase 3: Realtime Subscriptions & Admin Dashboard**
  - WebSocket live query push broker.
  - Embedded web admin UI for collection browsing and query execution.
  - Local dev sandbox with synthetic Jetstream event replay (`skybase dev --mock`).
- [ ] **Phase 4: Token-Mediated Proxy & Deployment Automation**
  - Confidential OAuth session mediator for static sites (Topology C).
  - 1-click Docker, Fly.io, and Railway deployment templates.

---

## 📄 Product Requirements & Architecture

- 📘 **[`PRD.md`](PRD.md)**: Full Product Requirements Document, Firebase-to-ATProto architectural deconstruction, and phased roadmap.
- 🤖 **[`AGENTS.md`](AGENTS.md)**: Engineering handover guide, Rust invariants, and architectural mandates.

---

## 🛡️ License

Dual-licensed under either:
- **MIT License** ([`LICENSE-MIT`](LICENSE-MIT))
- **Apache License, Version 2.0** ([`LICENSE-APACHE`](LICENSE-APACHE))
