//! DoS-mitigation: connection-count caps.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_rti::{ConnectionLimits, RtiNode};
use hla_wire::client_open_session;
use tokio::net::TcpStream;

async fn boot(limits: ConnectionLimits) -> (SocketAddr, Arc<RtiNode>) {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_connection_limits(limits);
    let addr = node.bind_addr;
    let serve = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve.serve(listener).await;
    });
    (addr, node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_total_caps_concurrent_connections() {
    let (addr, node) = boot(ConnectionLimits {
        max_total: Some(2),
        max_per_ip: None,
    })
    .await;

    let mut socks = Vec::new();
    for _ in 0..2 {
        let mut s = TcpStream::connect(addr).await.unwrap();
        client_open_session(&mut s).await.unwrap();
        socks.push(s);
    }
    // Wait for both to be registered.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while node.connections.len() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(node.connections.len(), 2);

    // Third connect: accept may succeed at TCP level, but the server should
    // immediately close it, so the client's handshake read will fail.
    let third = TcpStream::connect(addr).await;
    if let Ok(mut s) = third {
        let result =
            tokio::time::timeout(Duration::from_millis(300), client_open_session(&mut s)).await;
        assert!(
            result.is_err() || result.unwrap().is_err(),
            "third connection beyond max_total should not complete handshake"
        );
    }

    // Confirm we never went above 2.
    assert!(node.connections.len() <= 2);

    drop(socks);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_per_ip_caps_connections_from_one_host() {
    let (addr, node) = boot(ConnectionLimits {
        max_total: None,
        max_per_ip: Some(1),
    })
    .await;

    let mut first = TcpStream::connect(addr).await.unwrap();
    client_open_session(&mut first).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while node.connections.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(node.connections.len(), 1);

    // Second connection from same IP: rejected pre-handshake.
    if let Ok(mut s) = TcpStream::connect(addr).await {
        let result =
            tokio::time::timeout(Duration::from_millis(300), client_open_session(&mut s)).await;
        assert!(
            result.is_err() || result.unwrap().is_err(),
            "second connection from same IP beyond max_per_ip should not complete"
        );
    }

    assert_eq!(node.connections.len(), 1);
}
