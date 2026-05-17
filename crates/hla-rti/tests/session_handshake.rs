//! End-to-end Rust↔Rust FedPro session-open handshake over a real TCP socket.
//!
//! Boots `RtiNode` on `127.0.0.1:0`, learns the OS-assigned port, opens a
//! plain `TcpStream` as the client, runs `client_open_session`, and asserts
//! the RTI-assigned `session_id` came back.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_rti::RtiNode;
use hla_wire::{SessionState, client_open_session};
use tokio::net::TcpStream;

async fn bind_and_serve_in_background() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.expect("bind");
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    addr
}

#[tokio::test]
async fn rust_client_handshakes_with_rust_server() {
    let addr = bind_and_serve_in_background().await;

    // Give the accept loop a tick to schedule itself.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let ack = client_open_session(&mut stream).await.expect("handshake");

    assert_eq!(ack.state, SessionState::Running);
    assert!(
        ack.session_id >= 1,
        "session_id must be non-zero (NO_SESSION_ID = 0 is reserved); got {}",
        ack.session_id
    );
}

#[tokio::test]
async fn multiple_clients_get_distinct_session_ids() {
    let addr = bind_and_serve_in_background().await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut ids = Vec::new();
    for _ in 0..4 {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let ack = client_open_session(&mut stream).await.expect("handshake");
        ids.push(ack.session_id);
    }

    let uniques: std::collections::HashSet<_> = ids.iter().copied().collect();
    assert_eq!(uniques.len(), ids.len(), "session ids not unique: {ids:?}");
}

#[tokio::test]
async fn shared_arc_rtinode_keeps_serving() {
    // Sanity: holding an extra Arc to the node doesn't break the accept loop.
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.expect("bind");
    let addr = node.bind_addr;
    let _retained = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let ack = client_open_session(&mut stream).await.expect("handshake");
    assert_eq!(ack.state, SessionState::Running);
}
