//! Resume session: federate reattaches to a suspended session within the
//! reconnect window, preserving federation membership.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_omt::{FomModule, MergedFom};
use hla_rti::{HeartbeatConfig, RtiNode};
use hla_wire::{
    ClientSeqState, MessageType, client_open_session, read_frame, resume_request_frame,
    send_hla_call, write_frame,
};
use prost::Message;
use tokio::net::TcpStream;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>R</name></modelIdentification>
  <objects><objectClass><name>HLAobjectRoot</name></objectClass></objects>
  <interactions><interactionClass><name>HLAinteractionRoot</name>
    <transportation>HLAreliable</transportation><order>Receive</order>
  </interactionClass></interactions>
</objectModel>"#;

async fn boot(reconnect_window: Duration) -> (SocketAddr, Arc<RtiNode>) {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(FOM).unwrap()]).unwrap());
    node.set_heartbeat_config(HeartbeatConfig {
        interval: Duration::from_secs(60),
        missing_timeout: Duration::from_secs(180),
        reconnect_window,
    });
    let addr = node.bind_addr;
    let serve = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve.serve(listener).await;
    });
    let janitor_node = Arc::clone(&node);
    tokio::spawn(async move {
        janitor_node.run_suspended_session_janitor().await;
    });
    (addr, node)
}

fn encode(v: fedpro::call_request::CallRequest) -> Vec<u8> {
    fedpro::CallRequest {
        call_request: Some(v),
    }
    .encode_to_vec()
}

fn decode(b: &[u8]) -> fedpro::call_response::CallResponse {
    fedpro::CallResponse::decode(b)
        .unwrap()
        .call_response
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_drop_within_window_preserves_membership() {
    let (addr, node) = boot(Duration::from_secs(5)).await;

    // Open session 1, create federation + join.
    let mut sock1 = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock1).await.unwrap();
    let session_id = ack.session_id;
    let mut state = ClientSeqState::new(session_id);

    send_hla_call(
        &mut sock1,
        &mut state,
        encode(
            fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: "resume-fed".into(),
                    fom_module: None,
                },
            ),
        ),
    )
    .await
    .unwrap();
    send_hla_call(
        &mut sock1,
        &mut state,
        encode(
            fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
                fedpro::JoinFederationExecutionRequest {
                    federate_type: "Resumer".into(),
                    federation_name: "resume-fed".into(),
                },
            ),
        ),
    )
    .await
    .unwrap();

    // Federation should have one federate.
    {
        let f = node._testing_federation("resume-fed").unwrap();
        assert_eq!(f._testing_federate_count(), 1);
    }

    // Drop the transport — server should suspend, not auto-resign.
    drop(sock1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Federation membership preserved in suspended_sessions; federates map
    // still has the entry (only janitor expiry removes it).
    assert!(node.has_suspended_session(session_id));
    {
        let f = node._testing_federation("resume-fed").unwrap();
        assert_eq!(
            f._testing_federate_count(),
            1,
            "federate should still be joined"
        );
    }

    // Reconnect and resume.
    let mut sock2 = TcpStream::connect(addr).await.unwrap();
    let resume = resume_request_frame(session_id, 0, 0);
    write_frame(&mut sock2, &resume).await.unwrap();
    let response = read_frame(&mut sock2).await.unwrap();
    assert_eq!(response.header.message_type, MessageType::CtrlResumeStatus);
    let status = hla_wire::ResumeStatusPayload::decode(&response.payload).unwrap();
    assert_eq!(status.reason, hla_wire::NewSessionStatusReason::Success);
    assert_eq!(response.header.session_id, session_id);

    // Suspended slot drained; federation membership still intact.
    assert!(!node.has_suspended_session(session_id));
    {
        let f = node._testing_federation("resume-fed").unwrap();
        assert_eq!(f._testing_federate_count(), 1);
    }

    // Subsequent calls work on the resumed session.
    let mut state2 = ClientSeqState::new(session_id);
    let resp = send_hla_call(
        &mut sock2,
        &mut state2,
        encode(
            fedpro::call_request::CallRequest::ResignFederationExecutionRequest(
                fedpro::ResignFederationExecutionRequest { resign_action: 0 },
            ),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::ResignFederationExecutionResponse(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_expiry_returns_failure() {
    let (addr, node) = boot(Duration::from_millis(100)).await;

    let mut sock1 = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock1).await.unwrap();
    let session_id = ack.session_id;
    let mut state = ClientSeqState::new(session_id);

    send_hla_call(
        &mut sock1,
        &mut state,
        encode(
            fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: "expire-fed".into(),
                    fom_module: None,
                },
            ),
        ),
    )
    .await
    .unwrap();
    send_hla_call(
        &mut sock1,
        &mut state,
        encode(
            fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
                fedpro::JoinFederationExecutionRequest {
                    federate_type: "X".into(),
                    federation_name: "expire-fed".into(),
                },
            ),
        ),
    )
    .await
    .unwrap();

    drop(sock1);
    // Wait past reconnect_window so the janitor expires the session.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!node.has_suspended_session(session_id));

    // Try to resume — should fail.
    let mut sock2 = TcpStream::connect(addr).await.unwrap();
    let resume = resume_request_frame(session_id, 0, 0);
    write_frame(&mut sock2, &resume).await.unwrap();
    let response = read_frame(&mut sock2).await.unwrap();
    assert_eq!(response.header.message_type, MessageType::CtrlResumeStatus);
    let status = hla_wire::ResumeStatusPayload::decode(&response.payload).unwrap();
    assert_eq!(status.reason, hla_wire::NewSessionStatusReason::OtherError);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_window_skips_suspension_for_compat() {
    let (addr, node) = boot(Duration::from_secs(0)).await;
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    let mut state = ClientSeqState::new(ack.session_id);

    send_hla_call(
        &mut sock,
        &mut state,
        encode(
            fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: "zero-win".into(),
                    fom_module: None,
                },
            ),
        ),
    )
    .await
    .unwrap();
    send_hla_call(
        &mut sock,
        &mut state,
        encode(
            fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
                fedpro::JoinFederationExecutionRequest {
                    federate_type: "X".into(),
                    federation_name: "zero-win".into(),
                },
            ),
        ),
    )
    .await
    .unwrap();
    drop(sock);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // No suspension; federate has been auto-resigned. Destroy succeeds.
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let _ = client_open_session(&mut admin).await.unwrap();
    let mut s = ClientSeqState::new(0); // session_id irrelevant for the destroy call here
    // We'll re-open properly:
    drop(admin);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let ack2 = client_open_session(&mut admin).await.unwrap();
    s.session_id = ack2.session_id;
    let resp = send_hla_call(
        &mut admin,
        &mut s,
        encode(
            fedpro::call_request::CallRequest::DestroyFederationExecutionRequest(
                fedpro::DestroyFederationExecutionRequest {
                    federation_name: "zero-win".into(),
                },
            ),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::DestroyFederationExecutionResponse(_)
    ));
    assert!(node.suspended_session_count() == 0);
}
