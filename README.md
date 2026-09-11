# ⚡ `skybase`

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Safety Guard](https://img.shields.io/badge/unsafe-forbidden-success.svg)](src/lib.rs)
[![Rust Version](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](Cargo.toml)

> **The Turn-Key Micro-AppView Engine & Local Dev Platform for the AT Protocol.**
> *"Firebase-Like DX for ATProto, powered by `skyauth`."*

---

## 🌟 Highlights

- **The Strategic Wedge: Micro-AppView in One Binary**: Point `./skybase` at your collection NSID (`com.myapp.review`). It continuously ingests Jetstream, stores records in an embedded SQLite WAL database with full-text search (FTS5), and serves an instant REST & WebSocket query API.
- **Zero Token Custody by Default**: Frontends authenticate and write directly to user PDSs via `@atproto/oauth-client-browser`. `skybase` indexes public commits from Jetstream, holding **zero user refresh tokens or private keys**.
- **Hermetic Local Dev Sandbox (`skybase dev --mock`)**: Mock relay and synthetic Jetstream event replay engine allowing you to build and test full-stack ATProto apps offline with zero cloud configuration.
- **Eventual Consistency with Optimistic UX**: The `@skybase/client` SDK provides deterministic TID tracking and optimistic reconciliation (`isPending`, `isOptimistic`, `isLagging`) so React/Vue components never flicker or drop mutations.
- **100% Pure Safe Rust**: `#![forbid(unsafe_code)]` enforced crate-wide with 0 `unsafe` blocks, strict clippy denials, and zero production panics.
- **Turn-Key Server-Side Bot Auth**: When background daemons need to write autonomously, `skybase` integrates [`skyauth`] with **AES-256-GCM encryption at rest** for refresh tokens.
- **Dual Deployment Modes**: Single prebuilt binary (PocketBase-style) for frontend developers OR embeddable modular Rust crate for high-throughput systems services.

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

## 💡 The "PocketBase / Supabase" Model: Zero Rust Required for App Developers

> **"Are we pigeonholing ourselves by building in Rust?"**
> **Only if we force developers to write Rust to use it.**

`skybase` follows the architecture of **PocketBase** (Go engine, JS/Dart users), **Supabase** (Elixir/C engine, JS/Python users), and **Meilisearch** (Rust engine, npm users):
- **Rust is an invisible superpower for the engine**: Designed to sustain thousands of events/sec over the Jetstream firehose without GC pauses, operate within compact memory budgets, provide zero-crash safety (`#![forbid(unsafe_code)]`), and package into a single zero-dependency binary.
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

3. **Build your app**:
   ```typescript
   import { Skybase } from '@skybase/client';

   const sb = new Skybase('http://localhost:8080');

   // 1. Authenticate with ATProto handle
   await sb.auth.signInWithHandle('alice.bsky.social');

   // 2. Write sovereign record to user's personal PDS
   const post = await sb.collection('app.bsky.feed.post').create({
     text: 'Hello from Skybase!',
     createdAt: new Date().toISOString()
   });

   // 3. Realtime live query (synced from Jetstream firehose into SQLite)
   const unsubscribe = sb.collection('app.bsky.feed.post')
     .where('replyParent', '==', post.uri)
     .orderBy('createdAt', 'desc')
     .onSnapshot((comments) => console.log('Live comments:', comments));
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

## 📄 Product Requirements & Architecture

- 📘 **[`PRD.md`](PRD.md)**: Full Product Requirements Document, Firebase-to-ATProto architectural deconstruction, and phased roadmap.
- 🤖 **[`AGENTS.md`](AGENTS.md)**: Engineering handover guide, Rust invariants, and architectural mandates.

---

## 🛡️ License

Dual-licensed under either:
- **MIT License** ([`LICENSE-MIT`](LICENSE-MIT))
- **Apache License, Version 2.0** ([`LICENSE-APACHE`](LICENSE-APACHE))
