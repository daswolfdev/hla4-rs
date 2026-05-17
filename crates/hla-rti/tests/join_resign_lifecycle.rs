//! Join / Resign lifecycle and per-federate session state.
//!
//! Validates:
//!   * Happy-path join → resign with FederateHandle assignment
//!   * Join with explicit name (and `FederateNameAlreadyInUse` on conflict)
//!   * Double-join on same session → `FederateAlreadyExecutionMember`
//!   * Resign without joining → `FederateNotExecutionMember`
//!   * Destroy blocked while a federate is joined → `FederatesCurrentlyJoined`
//!   * Two federates joining the same federation get distinct handles
//!   * Auto-resign on TCP disconnect releases the federate

use std::net::SocketAddr;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_rti::RtiNode;
use hla_wire::{ClientSeqState, client_open_session, send_hla_call};
use prost::Message;
use tokio::net::TcpStream;

async fn boot() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

fn encode(variant: fedpro::call_request::CallRequest) -> Vec<u8> {
    fedpro::CallRequest {
        call_request: Some(variant),
    }
    .encode_to_vec()
}

fn decode(bytes: &[u8]) -> fedpro::call_response::CallResponse {
    fedpro::CallResponse::decode(bytes)
        .unwrap()
        .call_response
        .unwrap()
}

async fn open(addr: SocketAddr) -> (TcpStream, ClientSeqState) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    (sock, ClientSeqState::new(ack.session_id))
}

async fn create_federation(sock: &mut TcpStream, state: &mut ClientSeqState, name: &str) {
    let req = encode(
        fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
            fedpro::CreateFederationExecutionRequest {
                federation_name: name.into(),
                fom_module: None,
            },
        ),
    );
    let resp = send_hla_call(sock, state, req).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::CreateFederationExecutionResponse(_)
    ));
}

#[tokio::test]
async fn join_and_resign_happy_path() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;
    create_federation(&mut sock, &mut state, "alpha").await;

    let req = encode(
        fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "Producer".into(),
                federation_name: "alpha".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let inner = decode(&resp);
    let jr = match inner {
        fedpro::call_response::CallResponse::JoinFederationExecutionResponse(r) => {
            r.result.expect("result missing")
        }
        other => panic!("expected JoinResponse, got {other:?}"),
    };
    let fh = jr.federate_handle.expect("federate_handle missing");
    assert_eq!(fh.data.len(), 4, "FederateHandle should be 4 BE bytes");
    let raw = u32::from_be_bytes(fh.data.as_slice().try_into().unwrap());
    assert!(raw >= 1, "federate handle should start at 1, got {raw}");
    assert_eq!(jr.logical_time_implementation_name, "HLAfloat64Time");

    // Resign cleanly.
    let req = encode(
        fedpro::call_request::CallRequest::ResignFederationExecutionRequest(
            fedpro::ResignFederationExecutionRequest { resign_action: 0 },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::ResignFederationExecutionResponse(_)
    ));
}

#[tokio::test]
async fn join_unknown_federation_fails() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;

    let req = encode(
        fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "Anyone".into(),
                federation_name: "no-such-federation".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederationExecutionDoesNotExist");
}

#[tokio::test]
async fn double_join_fails() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;
    create_federation(&mut sock, &mut state, "beta").await;

    let req = encode(
        fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "F".into(),
                federation_name: "beta".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req.clone())
        .await
        .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::JoinFederationExecutionResponse(_)
    ));

    // Second join on same session → exception.
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederateAlreadyExecutionMember");
}

#[tokio::test]
async fn resign_without_joining_fails() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;
    let req = encode(
        fedpro::call_request::CallRequest::ResignFederationExecutionRequest(
            fedpro::ResignFederationExecutionRequest { resign_action: 0 },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederateNotExecutionMember");
}

#[tokio::test]
async fn destroy_blocked_while_joined() {
    let addr = boot().await;
    let (mut sock_a, mut state_a) = open(addr).await;
    create_federation(&mut sock_a, &mut state_a, "gamma").await;
    let join = encode(
        fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "F".into(),
                federation_name: "gamma".into(),
            },
        ),
    );
    send_hla_call(&mut sock_a, &mut state_a, join)
        .await
        .unwrap();

    // Open a second connection and try to destroy — should fail.
    let (mut sock_b, mut state_b) = open(addr).await;
    let destroy = encode(
        fedpro::call_request::CallRequest::DestroyFederationExecutionRequest(
            fedpro::DestroyFederationExecutionRequest {
                federation_name: "gamma".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock_b, &mut state_b, destroy.clone())
        .await
        .unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederatesCurrentlyJoined");

    // Resign first federate, then destroy succeeds.
    let resign = encode(
        fedpro::call_request::CallRequest::ResignFederationExecutionRequest(
            fedpro::ResignFederationExecutionRequest { resign_action: 0 },
        ),
    );
    send_hla_call(&mut sock_a, &mut state_a, resign)
        .await
        .unwrap();
    let resp = send_hla_call(&mut sock_b, &mut state_b, destroy)
        .await
        .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::DestroyFederationExecutionResponse(_)
    ));
}

#[tokio::test]
async fn two_federates_get_distinct_handles() {
    let addr = boot().await;
    let (mut admin, mut admin_state) = open(addr).await;
    create_federation(&mut admin, &mut admin_state, "delta").await;

    let join = encode(
        fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "F".into(),
                federation_name: "delta".into(),
            },
        ),
    );

    // Hold sockets in a Vec for the duration of the test so each federate
    // stays joined — the RTI keys its federate registry on the live TCP
    // connection. The previous version used `mem::forget(sock)`, which
    // leaked the FD; this is properly closed when `_sockets` drops.
    let mut _sockets: Vec<(TcpStream, ClientSeqState)> = Vec::new();
    let mut handles = Vec::new();
    for _ in 0..3 {
        let (mut sock, mut state) = open(addr).await;
        let resp = send_hla_call(&mut sock, &mut state, join.clone())
            .await
            .unwrap();
        let jr = match decode(&resp) {
            fedpro::call_response::CallResponse::JoinFederationExecutionResponse(r) => {
                r.result.unwrap()
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        };
        let fh = jr.federate_handle.unwrap();
        let raw = u32::from_be_bytes(fh.data.as_slice().try_into().unwrap());
        handles.push(raw);
        _sockets.push((sock, state));
    }

    let uniques: std::collections::HashSet<_> = handles.iter().copied().collect();
    assert_eq!(uniques.len(), 3, "expected 3 distinct handles: {handles:?}");
}

#[tokio::test]
async fn join_with_explicit_name_then_duplicate_fails() {
    let addr = boot().await;
    let (mut admin, mut admin_state) = open(addr).await;
    create_federation(&mut admin, &mut admin_state, "epsilon").await;

    let join_named = |name: &str| {
        encode(
            fedpro::call_request::CallRequest::JoinFederationExecutionWithNameRequest(
                fedpro::JoinFederationExecutionWithNameRequest {
                    federate_name: name.into(),
                    federate_type: "Producer".into(),
                    federation_name: "epsilon".into(),
                },
            ),
        )
    };

    let (mut sock_a, mut state_a) = open(addr).await;
    let resp = send_hla_call(&mut sock_a, &mut state_a, join_named("alice"))
        .await
        .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::JoinFederationExecutionWithNameResponse(_)
    ));

    let (mut sock_b, mut state_b) = open(addr).await;
    let resp = send_hla_call(&mut sock_b, &mut state_b, join_named("alice"))
        .await
        .unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederateNameAlreadyInUse");
}

#[tokio::test]
async fn auto_resign_on_disconnect_unblocks_destroy() {
    let addr = boot().await;
    let (mut admin, mut admin_state) = open(addr).await;
    create_federation(&mut admin, &mut admin_state, "zeta").await;

    // Open a second connection, join, then drop the socket without resigning.
    {
        let (mut sock, mut state) = open(addr).await;
        let join = encode(
            fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
                fedpro::JoinFederationExecutionRequest {
                    federate_type: "F".into(),
                    federation_name: "zeta".into(),
                },
            ),
        );
        send_hla_call(&mut sock, &mut state, join).await.unwrap();
        drop(sock);
        // Give the server's handler time to observe EOF and run cleanup.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Now destroy should succeed because auto-resign cleared the federate.
    let destroy = encode(
        fedpro::call_request::CallRequest::DestroyFederationExecutionRequest(
            fedpro::DestroyFederationExecutionRequest {
                federation_name: "zeta".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut admin, &mut admin_state, destroy)
        .await
        .unwrap();
    assert!(
        matches!(
            decode(&resp),
            fedpro::call_response::CallResponse::DestroyFederationExecutionResponse(_)
        ),
        "expected destroy to succeed after auto-resign"
    );
}
