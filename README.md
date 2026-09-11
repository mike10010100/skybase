# ⚡ `skybase`

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Safety Guard](https://img.shields.io/badge/unsafe-forbidden-success.svg)](src/lib.rs)
[![Rust Version](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](Cargo.toml)

> **The Open-Source Backend & Developer Platform for the AT Protocol.**
> *"Firebase for ATProto, powered by `skyauth`."*

---

## 🌟 Highlights

- **User Data Sovereignty First**: Never locks user data into proprietary databases. All records are written directly into users' personal PDS repositories (signed Merkle Search Trees).
- **Turn-Key ATProto OAuth 2.1**: Powered directly by [`skyauth`] — zero-panic, pure Safe Rust implementation of RFC 9449 DPoP, RFC 9126 PAR, RFC 7636 PKCE, and decentralized identity resolution (`did:plc`, `did:web`).
- **Embedded Micro-AppView Engine**: Subscribes directly to the Bluesky Jetstream firehose, filters for your application's specific collection NSIDs, and indexes records into an embedded high-concurrency database (SQLite WAL / PostgreSQL) with full-text search.
- **100% Pure Safe Rust**: `#![forbid(unsafe_code)]` enforced crate-wide with 0 `unsafe` blocks and zero production panics.
- **Reactive Triggers & Events**: Declarative event hooks (`on_record_created`, `on_record_deleted`) with durable monotonic cursor persistence and zero record drop.
- **Sovereign Blob Management**: Seamless blob upload to PDS with cryptographic CID digest calculation, MIME validation, and edge CDN caching.
- **Dual Deployment Modes**: Use as a modular Rust crate inside your Axum/Actix/Tower microservice OR run as a standalone zero-config daemon (like PocketBase or Supabase) with REST, WebSocket, and Web Admin dashboard.

---

## 🗺️ The "Firebase for ATProto" Mapping

| Firebase Component | `skybase` Pillar | AT Protocol Primitive |
| :--- | :--- | :--- |
| **Firebase Auth** | `skybase-auth` | Decentralized OAuth 2.1 + RFC 9449 DPoP via `skyauth` |
| **Firestore (Writes)** | `skybase-repo` | Sovereign XRPC writes to user PDS Merkle Search Trees |
| **Firestore (Queries)** | `skybase-index` | Embedded Micro-AppView (SQLite WAL) synced from Jetstream |
| **Realtime Subscriptions** | `skybase-events` | WebSocket live queries & reactive hooks off Jetstream |
| **Cloud Storage** | `skybase-storage` | PDS blob upload (`uploadBlob`) with CID verification & CDN |
| **Security Rules** | `skybase-rules` | Lexicon schema validation & commit cryptographic verification |
| **Local Emulator / Admin** | `skybase-server` | Single-binary daemon with embedded SQLite & Admin UI |
| **Client SDKs** | `skybase-sdk` | Rust crate + `@skybase/client` for TypeScript / React |

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
