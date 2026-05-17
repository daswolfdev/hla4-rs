//! Heartbeat + liveness-timeout coverage.
//!
//! Configures the RTI with short intervals so tests run in <1s. Validates:
//!   * the server emits `CTRL_HEARTBEAT` at its configured cadence
//!   * the server replies to a client `CTRL_HEARTBEAT` with `CTRL_HEARTBEAT_RESPONSE`
//!   * a client that goes silent past `missing_timeout` is reaped (next
//!     write fails or read returns EOF, depending on timing)

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_rti::{HeartbeatConfig, RtiNode};
use hla_wire::{
    Frame, MessageHeader, MessageType, NO_SEQUENCE_NUMBER, client_open_session, read_frame,
    write_frame,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

async fn boot(hb: HeartbeatConfig) -> (SocketAddr, Arc<RtiNode>) {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_heartbeat_config(hb);
    let addr = node.bind_addr;
    let serve_node = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve_node.serve(listener).await;
    });
    (addr, node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_emits_heartbeat_at_configured_cadence() {
    let (addr, _node) = boot(HeartbeatConfig {
        interval: Duration::from_millis(80),
        missing_timeout: Duration::from_secs(5), // long, so we don't get reaped
        reconnect_window: Duration::from_secs(0),
    })
    .await;

    let mut sock = TcpStream::connect(addr).await.unwrap();
    let _ack = client_open_session(&mut sock).await.unwrap();

    // Within a small multiple of the interval, a CTRL_HEARTBEAT should arrive.
    let frame = tokio::time::timeout(Duration::from_millis(500), read_frame(&mut sock))
        .await
        .expect("no heartbeat within budget")
        .expect("read_frame error");
    assert_eq!(frame.header.message_type, MessageType::CtrlHeartbeat);
    assert_eq!(frame.payload.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_responds_to_client_heartbeat() {
    let (addr, _node) = boot(HeartbeatConfig {
        interval: Duration::from_secs(5), // long; we want only client-driven heartbeats
        missing_timeout: Duration::from_secs(5),
        reconnect_window: Duration::from_secs(0),
    })
    .await;

    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();

    let header = MessageHeader::with_payload_size(
        0,
        1,
        ack.session_id,
        NO_SEQUENCE_NUMBER,
        MessageType::CtrlHeartbeat,
    );
    write_frame(&mut sock, &Frame::new(header, Vec::new()))
        .await
        .unwrap();

    let frame = tokio::time::timeout(Duration::from_millis(500), read_frame(&mut sock))
        .await
        .expect("no heartbeat response within budget")
        .expect("read_frame error");
    assert_eq!(
        frame.header.message_type,
        MessageType::CtrlHeartbeatResponse
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_client_is_reaped_after_missing_timeout() {
    let (addr, node) = boot(HeartbeatConfig {
        // Long interval so the server doesn't push to us; we want to trip
        // *only* the missing-timeout branch.
        interval: Duration::from_secs(10),
        missing_timeout: Duration::from_millis(200),
        reconnect_window: Duration::from_secs(0),
    })
    .await;

    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();

    // Connection should be registered.
    assert!(node.has_connection(ack.session_id));

    // Stay silent long enough that the liveness check trips
    // (timeout=200ms, checked every 50ms).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Server should have removed the connection.
    assert!(
        !node.has_connection(ack.session_id),
        "connection should have been reaped after silent timeout"
    );

    // Subsequent writes should error out (server closed the socket).
    // Drain any pending bytes the server wrote during teardown, then write.
    let mut buf = [0u8; 1];
    let _ = sock.try_read(&mut buf); // ignore result
    let header = MessageHeader::with_payload_size(
        0,
        1,
        ack.session_id,
        NO_SEQUENCE_NUMBER,
        MessageType::CtrlHeartbeat,
    );
    // Write a frame; it may succeed initially due to TCP buffering, but the
    // connection should be unusable. Try shutdown to surface the broken pipe.
    let _ = write_frame(&mut sock, &Frame::new(header, Vec::new())).await;
    let _ = sock.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_client_is_not_reaped() {
    let (addr, node) = boot(HeartbeatConfig {
        interval: Duration::from_millis(50),
        missing_timeout: Duration::from_millis(200),
        reconnect_window: Duration::from_secs(0),
    })
    .await;

    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    let session_id = ack.session_id;

    // Pump: read each inbound heartbeat and respond with
    // CTRL_HEARTBEAT_RESPONSE so the server sees us alive.
    let pump = tokio::spawn(async move {
        loop {
            let frame = match read_frame(&mut sock).await {
                Ok(f) => f,
                Err(_) => return,
            };
            if frame.header.message_type == MessageType::CtrlHeartbeat {
                let header = MessageHeader::with_payload_size(
                    0,
                    NO_SEQUENCE_NUMBER,
                    session_id,
                    frame.header.sequence_number,
                    MessageType::CtrlHeartbeatResponse,
                );
                if write_frame(&mut sock, &Frame::new(header, Vec::new()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    });

    // Wait several heartbeat cycles.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        node.has_connection(session_id),
        "responsive client should NOT have been reaped"
    );

    pump.abort();
}
