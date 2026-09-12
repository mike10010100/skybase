//! Hermetic offline WebSocket server fixture for testing Jetstream firehose consumers.
//!
//! Provides a local, zero-network-dependency mock server binding to loopback `127.0.0.1:0`.
//! Used for testing WebSocket subscriptions, edge collection query filtering, commit event
//! deserialization, heartbeats, network disconnects, and exponential reconnect backoff.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

use crate::error::{Result, SkybaseError};
use crate::ingest::events::{CommitOperation, JetstreamCommit};

/// Internal command dispatched to active client connection worker tasks.
#[derive(Debug, Clone)]
pub enum MockServerCommand {
    /// Send a text message payload over WebSocket.
    Text(String),
    /// Send a WebSocket Close frame with specified status code and reason, then terminate.
    Close {
        /// WebSocket close status code.
        code: u16,
        /// Human-readable explanation.
        reason: String,
    },
    /// Abruptly drop the TCP connection without sending a WebSocket close frame.
    Abort,
}

/// Hermetic offline mock Jetstream WebSocket server.
///
/// Binds to `127.0.0.1:0` and provides event emission, query inspection,
/// and connection drop simulation for integration testing.
#[derive(Debug, Clone)]
pub struct MockJetstreamServer {
    addr: SocketAddr,
    cmd_tx: broadcast::Sender<MockServerCommand>,
    active_connections: Arc<AtomicU64>,
    total_connections: Arc<AtomicU64>,
    received_queries: Arc<Mutex<Vec<String>>>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl MockJetstreamServer {
    /// Starts a mock Jetstream server on loopback port `127.0.0.1:0`.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Storage`] if TCP socket binding or address resolution fails.
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| SkybaseError::Storage(format!("MockJetstreamServer bind failed: {e}")))?;

        let addr = listener
            .local_addr()
            .map_err(|e| SkybaseError::Storage(format!("Failed to retrieve local addr: {e}")))?;

        let (cmd_tx, _) = broadcast::channel::<MockServerCommand>(1024);
        let active_connections = Arc::new(AtomicU64::new(0));
        let total_connections = Arc::new(AtomicU64::new(0));
        let received_queries = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_cmd_tx = cmd_tx.clone();
        let server_active = active_connections.clone();
        let server_total = total_connections.clone();
        let server_queries = received_queries.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_res = listener.accept() => {
                        let (stream, _) = match accept_res {
                            Ok(conn) => conn,
                            Err(_) => break,
                        };

                        server_total.fetch_add(1, Ordering::SeqCst);
                        let mut client_rx = server_cmd_tx.subscribe();
                        let client_active = server_active.clone();
                        let client_queries = server_queries.clone();

                        tokio::spawn(async move {
                            // Intercept HTTP handshake to record subscription path and query parameters
                            #[allow(clippy::result_large_err)]
                            let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                            res: tokio_tungstenite::tungstenite::handshake::server::Response| {
                                let path = req.uri().path_and_query().map_or_else(
                                    || req.uri().path().to_string(),
                                    |pq| pq.as_str().to_string(),
                                );
                                client_queries.lock().push(path);
                                Ok(res)
                            };

                            let ws_res = tokio_tungstenite::accept_hdr_async(stream, callback).await;
                            let mut ws_stream = match ws_res {
                                Ok(ws) => ws,
                                Err(_) => return,
                            };

                            client_active.fetch_add(1, Ordering::SeqCst);

                            loop {
                                tokio::select! {
                                    cmd = client_rx.recv() => {
                                        match cmd {
                                            Ok(MockServerCommand::Text(text)) => {
                                                if ws_stream.send(Message::Text(text)).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Ok(MockServerCommand::Close { code, reason }) => {
                                                let _ = ws_stream.close(Some(CloseFrame {
                                                    code: CloseCode::from(code),
                                                    reason: reason.into(),
                                                })).await;
                                                break;
                                            }
                                            Ok(MockServerCommand::Abort) => {
                                                // Abrupt drop: exit loop without close handshake
                                                break;
                                            }
                                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                                // Subscriber lagged; continue receiving fresh commands
                                                continue;
                                            }
                                            Err(broadcast::error::RecvError::Closed) => {
                                                break;
                                            }
                                        }
                                    }
                                    msg = ws_stream.next() => {
                                        match msg {
                                            Some(Ok(Message::Ping(data))) => {
                                                let _ = ws_stream.send(Message::Pong(data)).await;
                                            }
                                            Some(Ok(Message::Close(_))) | None => {
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            client_active.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        Ok(Self {
            addr,
            cmd_tx,
            active_connections,
            total_connections,
            received_queries,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        })
    }

    /// Returns the WebSocket subscription URL string (e.g., `ws://127.0.0.1:54321/subscribe`).
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}/subscribe", self.addr)
    }

    /// Returns the assigned socket address.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the assigned ephemeral port.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Emits a structured Jetstream commit frame.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if broadcasting to client channels fails.
    pub fn emit_commit(&self, commit: &JetstreamCommit) -> Result<usize> {
        let op_str = match commit.operation {
            CommitOperation::Create => "create",
            CommitOperation::Update => "update",
            CommitOperation::Delete => "delete",
        };

        self.emit_commit_payload(
            &commit.did,
            commit.time_us,
            &commit.collection,
            &commit.rkey,
            op_str,
            commit.cid.as_deref(),
            commit.record.clone(),
        )
    }

    /// Emits a structured commit frame using JSON parameters.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if broadcasting to client channels fails.
    #[allow(clippy::too_many_arguments)]
    pub fn emit_commit_payload(
        &self,
        did: &str,
        time_us: u64,
        collection: &str,
        rkey: &str,
        operation: &str,
        cid: Option<&str>,
        record: Option<serde_json::Value>,
    ) -> Result<usize> {
        let payload = json!({
            "did": did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": collection,
                "rkey": rkey,
                "operation": operation,
                "cid": cid,
                "record": record,
            }
        });
        self.emit_json(&payload)
    }

    /// Emits a serialized JSON payload as a WebSocket Text frame.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if broadcasting to client channels fails.
    pub fn emit_json(&self, val: &serde_json::Value) -> Result<usize> {
        self.emit_raw(&val.to_string())
    }

    /// Emits a raw text payload over WebSocket for testing malformed, truncated, or custom JSON.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if broadcasting to client channels fails.
    pub fn emit_raw(&self, text: &str) -> Result<usize> {
        self.cmd_tx
            .send(MockServerCommand::Text(text.to_string()))
            .map_err(|e| SkybaseError::Event(format!("MockJetstreamServer emit failed: {e}")))
    }

    /// Emits a heartbeat frame with timestamp only.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if broadcasting to client channels fails.
    pub fn emit_heartbeat(&self, time_us: u64) -> Result<usize> {
        let payload = json!({
            "time_us": time_us,
        });
        self.emit_json(&payload)
    }

    /// Abruptly terminates all active client WebSocket connections without a close handshake.
    ///
    /// Simulates network partitions, sudden proxy crashes, and silent connection drops.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if dispatching the abort command fails.
    pub fn disconnect_all(&self) -> Result<usize> {
        self.cmd_tx
            .send(MockServerCommand::Abort)
            .map_err(|e| SkybaseError::Event(format!("Disconnect broadcast failed: {e}")))
    }

    /// Closes all active client WebSocket connections with a formal Close frame.
    ///
    /// # Errors
    /// Returns [`SkybaseError::Event`] if dispatching the close command fails.
    pub fn close_all(&self, code: u16, reason: &str) -> Result<usize> {
        self.cmd_tx
            .send(MockServerCommand::Close {
                code,
                reason: reason.to_string(),
            })
            .map_err(|e| SkybaseError::Event(format!("Close broadcast failed: {e}")))
    }

    /// Returns the count of currently connected WebSocket clients.
    #[must_use]
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Returns the total cumulative count of connections accepted since server start.
    #[must_use]
    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::SeqCst)
    }

    /// Returns a copy of all HTTP paths and query strings received during handshakes.
    #[must_use]
    pub fn query_history(&self) -> Vec<String> {
        self.received_queries.lock().clone()
    }

    /// Clears the recorded handshake query history.
    pub fn clear_query_history(&self) {
        self.received_queries.lock().clear();
    }

    /// Gracefully stops the mock server accept loop.
    pub fn shutdown(&self) {
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }
        let _ = self.disconnect_all();
    }
}

impl Drop for MockJetstreamServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_tungstenite::connect_async;

    #[tokio::test]
    async fn test_mock_server_start_and_query_capture() {
        let server = MockJetstreamServer::start().await.expect("start failed");
        let test_url = format!(
            "{}?wantedCollections=app.bsky.feed.post&cursor=1700000000000000",
            server.ws_url()
        );

        let (ws_stream, _) = connect_async(&test_url).await.expect("connect failed");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let queries = server.query_history();
        assert!(!queries.is_empty());
        assert!(queries[0].contains("wantedCollections=app.bsky.feed.post"));
        assert!(queries[0].contains("cursor=1700000000000000"));
        assert_eq!(server.active_connections(), 1);
        assert_eq!(server.total_connections(), 1);

        drop(ws_stream);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(server.active_connections(), 0);
    }

    #[tokio::test]
    async fn test_mock_server_emit_commit_and_heartbeat() {
        let server = MockJetstreamServer::start().await.expect("start failed");
        let (mut ws_stream, _) = connect_async(&server.ws_url())
            .await
            .expect("connect failed");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let commit = JetstreamCommit {
            did: "did:plc:alice".to_string(),
            time_us: 1_710_000_000_000_000,
            collection: "app.bsky.feed.post".to_string(),
            rkey: "rkey1".to_string(),
            operation: CommitOperation::Create,
            cid: Some("cid1".to_string()),
            record: Some(json!({ "text": "hello" })),
        };
        server.emit_commit(&commit).expect("emit commit failed");

        let msg1 = tokio::time::timeout(Duration::from_secs(1), ws_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let text1 = msg1.to_text().expect("not text");
        let parsed1: serde_json::Value = serde_json::from_str(text1).expect("invalid json");
        assert_eq!(parsed1["kind"], "commit");
        assert_eq!(parsed1["did"], "did:plc:alice");
        assert_eq!(parsed1["commit"]["rkey"], "rkey1");

        server
            .emit_heartbeat(1_720_000_000_000_000)
            .expect("emit heartbeat failed");
        let msg2 = tokio::time::timeout(Duration::from_secs(1), ws_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let text2 = msg2.to_text().expect("not text");
        let parsed2: serde_json::Value = serde_json::from_str(text2).expect("invalid json");
        assert_eq!(parsed2["time_us"], 1_720_000_000_000_000u64);
        assert!(parsed2.get("commit").is_none());
    }

    #[tokio::test]
    async fn test_mock_server_disconnect_all_simulates_network_drop() {
        let server = MockJetstreamServer::start().await.expect("start failed");
        let (mut ws_stream, _) = connect_async(&server.ws_url())
            .await
            .expect("connect failed");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(server.active_connections(), 1);

        server.disconnect_all().expect("disconnect failed");
        let next_msg = tokio::time::timeout(Duration::from_secs(1), ws_stream.next()).await;

        match next_msg {
            Ok(None) => {}
            Ok(Some(Err(_))) => {}
            Ok(Some(Ok(Message::Close(_)))) => {}
            other => panic!("Unexpected msg after disconnect: {:?}", other),
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(server.active_connections(), 0);
    }
}
