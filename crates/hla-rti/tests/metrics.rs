//! Verifies the server's metrics counters track real activity.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use hla_wire::{ClientSeqState, client_open_session, send_hla_call};
use prost::Message;
use tokio::net::TcpStream;

const TRIVIAL: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>T</name></modelIdentification>
  <objects><objectClass><name>HLAobjectRoot</name></objectClass></objects>
  <interactions><interactionClass><name>HLAinteractionRoot</name>
    <transportation>HLAreliable</transportation><order>Receive</order>
  </interactionClass></interactions>
</objectModel>"#;

async fn boot() -> (SocketAddr, Arc<RtiNode>) {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(TRIVIAL).unwrap()]).unwrap());
    let addr = node.bind_addr;
    let serve = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_count_calls_and_sessions() {
    let (addr, node) = boot().await;
    let snapshot_before = node.metrics_snapshot();
    assert_eq!(snapshot_before.sessions_opened, 0);
    assert_eq!(snapshot_before.calls_dispatched, 0);

    // Connect and send a handful of calls.
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    let mut state = ClientSeqState::new(ack.session_id);

    let create = fedpro::CallRequest {
        call_request: Some(
            fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: "metrics-fed".into(),
                    fom_module: None,
                },
            ),
        ),
    }
    .encode_to_vec();
    send_hla_call(&mut sock, &mut state, create).await.unwrap();

    let list = fedpro::CallRequest {
        call_request: Some(
            fedpro::call_request::CallRequest::ListFederationExecutionsRequest(
                fedpro::ListFederationExecutionsRequest {},
            ),
        ),
    }
    .encode_to_vec();
    send_hla_call(&mut sock, &mut state, list).await.unwrap();
    // List triggers a reportFederationExecutions callback to the requester —
    // drain it so the next send_hla_call sees its response, not the callback.
    let _cb = hla_wire::read_frame(&mut sock).await.unwrap();

    // Second create on same name → exception, increments call_exceptions.
    let create_dup = fedpro::CallRequest {
        call_request: Some(
            fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: "metrics-fed".into(),
                    fom_module: None,
                },
            ),
        ),
    }
    .encode_to_vec();
    send_hla_call(&mut sock, &mut state, create_dup)
        .await
        .unwrap();

    // Allow counters to settle.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snap = node.metrics_snapshot();
    assert!(snap.connections_accepted >= 1, "{snap:?}");
    assert_eq!(snap.sessions_opened, 1, "{snap:?}");
    assert_eq!(snap.calls_dispatched, 3, "{snap:?}");
    assert_eq!(snap.call_exceptions, 1, "{snap:?}");
    assert!(snap.callbacks_emitted >= 1, "{snap:?}"); // reportFederationExecutions
    assert_eq!(snap.federations_live, 1, "{snap:?}");
    assert_eq!(snap.sessions_reaped, 0);

    drop(sock);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_count_rejections() {
    let (addr, node) = boot().await;
    node.set_connection_limits(hla_rti::ConnectionLimits {
        max_total: Some(1),
        max_per_ip: None,
    });

    // Saturate.
    let mut sock = TcpStream::connect(addr).await.unwrap();
    client_open_session(&mut sock).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Several beyond-limit attempts.
    for _ in 0..3 {
        if let Ok(mut s) = TcpStream::connect(addr).await {
            let _ =
                tokio::time::timeout(Duration::from_millis(200), client_open_session(&mut s)).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = node.metrics_snapshot();
    assert!(snap.connections_rejected >= 1, "{snap:?}");
}
