//! Graceful shutdown semantics: `RtiNode::shutdown()` makes every active
//! accept loop and the suspended-session janitor return `Ok(())` within a
//! bounded window. Mirrors the wiring that `rtiexec` uses for SIGTERM /
//! Ctrl-C in `crates/hla-cli/src/main.rs`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_rti::RtiNode;
use tokio::net::TcpStream;
use tokio::time::timeout;

async fn bind() -> (SocketAddr, Arc<RtiNode>, tokio::net::TcpListener) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(addr).await.unwrap();
    let actual = node.bind_addr;
    (actual, node, listener)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serve_returns_after_shutdown() {
    let (_addr, node, listener) = bind().await;
    let serve_node = Arc::clone(&node);
    let handle = tokio::spawn(async move { serve_node.serve(listener).await });

    // Give the accept loop one poll's worth of time, then signal.
    tokio::task::yield_now().await;
    node.shutdown();

    // Accept loop must finish promptly.
    let result = timeout(Duration::from_secs(2), handle)
        .await
        .expect("serve did not return within 2s after shutdown");
    assert!(result.expect("task panicked").is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_after_client_connect_still_unwinds() {
    let (addr, node, listener) = bind().await;
    let serve_node = Arc::clone(&node);
    let handle = tokio::spawn(async move { serve_node.serve(listener).await });

    // Bring up at least one live connection so the per-session task exists.
    // Run the FedPro handshake so we're in `run_session_loop`, not still
    // stuck on the handshake read.
    let mut client = TcpStream::connect(addr).await.unwrap();
    let _ack = hla_wire::client_open_session(&mut client).await.unwrap();

    node.shutdown();

    let result = timeout(Duration::from_secs(2), handle)
        .await
        .expect("serve did not return within 2s after shutdown with live conn");
    assert!(result.expect("task panicked").is_ok());

    // Per-session task must also unwind — closing the socket from the
    // server side surfaces as a clean EOF on the next client read.
    // Without per-session shutdown wiring the read would block until
    // heartbeat timeout (180s by default).
    let mut buf = [0u8; 1];
    let read = timeout(Duration::from_secs(2), {
        use tokio::io::AsyncReadExt;
        async move { client.read(&mut buf).await }
    })
    .await
    .expect("session task did not close the socket within 2s");
    // Server-side close is observed as Ok(0) or an Io error (depending on
    // FramedRead's drop order); either is acceptable evidence the per-
    // session task unwound.
    match read {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("unexpected non-zero read after shutdown: {n} bytes"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_session_janitor_stops_on_shutdown() {
    let (_addr, node, _listener) = bind().await;
    let janitor_node = Arc::clone(&node);
    let handle = tokio::spawn(async move { janitor_node.run_suspended_session_janitor().await });

    tokio::task::yield_now().await;
    node.shutdown();

    timeout(Duration::from_secs(2), handle)
        .await
        .expect("janitor did not return within 2s after shutdown")
        .expect("janitor task panicked");
}
