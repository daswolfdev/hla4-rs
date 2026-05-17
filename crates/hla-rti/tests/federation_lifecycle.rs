//! End-to-end RTIambassador dispatch: create / conflict / destroy / not-found.
//!
//! This is the first test where actual protobuf-encoded `CallRequest`s flow
//! through the wire layer, hit the dispatch table, mutate the federation
//! registry, and come back as typed `CallResponse`s the client can match on.

use std::net::SocketAddr;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_rti::RtiNode;
use hla_wire::{ClientSeqState, client_open_session, send_hla_call};
use prost::Message;
use tokio::net::TcpStream;

/// Bind RtiNode on `127.0.0.1:0`, spawn the serve loop, return its address.
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

/// Wrap a request variant in a `CallRequest` envelope and serialize.
fn encode_request(variant: fedpro::call_request::CallRequest) -> Vec<u8> {
    let envelope = fedpro::CallRequest {
        call_request: Some(variant),
    };
    envelope.encode_to_vec()
}

/// Decode and return the inner `CallResponse` oneof variant.
fn decode_response(bytes: &[u8]) -> fedpro::call_response::CallResponse {
    let env = fedpro::CallResponse::decode(bytes).expect("decode CallResponse");
    env.call_response.expect("response envelope had no variant")
}

async fn open(addr: SocketAddr) -> (TcpStream, ClientSeqState) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    (sock, ClientSeqState::new(ack.session_id))
}

#[tokio::test]
async fn create_destroy_roundtrip() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;

    // 1. create "lifecycle-test" → expect CreateFederationExecutionResponse
    let body = encode_request(
        fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
            fedpro::CreateFederationExecutionRequest {
                federation_name: "lifecycle-test".to_string(),
                fom_module: None,
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, body).await.unwrap();
    assert!(matches!(
        decode_response(&resp),
        fedpro::call_response::CallResponse::CreateFederationExecutionResponse(_)
    ));

    // 2. destroy "lifecycle-test" → expect DestroyFederationExecutionResponse
    let body = encode_request(
        fedpro::call_request::CallRequest::DestroyFederationExecutionRequest(
            fedpro::DestroyFederationExecutionRequest {
                federation_name: "lifecycle-test".to_string(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, body).await.unwrap();
    assert!(matches!(
        decode_response(&resp),
        fedpro::call_response::CallResponse::DestroyFederationExecutionResponse(_)
    ));
}

#[tokio::test]
async fn duplicate_create_returns_already_exists() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;

    let body = encode_request(
        fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
            fedpro::CreateFederationExecutionRequest {
                federation_name: "dup".to_string(),
                fom_module: None,
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, body.clone())
        .await
        .unwrap();
    assert!(matches!(
        decode_response(&resp),
        fedpro::call_response::CallResponse::CreateFederationExecutionResponse(_)
    ));

    // Second create on same name → ExceptionData
    let resp = send_hla_call(&mut sock, &mut state, body).await.unwrap();
    let inner = decode_response(&resp);
    let exc = match inner {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected ExceptionData, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederationExecutionAlreadyExists");
    assert_eq!(exc.details, "dup");
}

#[tokio::test]
async fn destroy_unknown_federation_returns_does_not_exist() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;

    let body = encode_request(
        fedpro::call_request::CallRequest::DestroyFederationExecutionRequest(
            fedpro::DestroyFederationExecutionRequest {
                federation_name: "ghost".to_string(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, body).await.unwrap();
    let inner = decode_response(&resp);
    let exc = match inner {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected ExceptionData, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "FederationExecutionDoesNotExist");
}

#[tokio::test]
async fn list_federation_executions_returns_ack() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;

    // ListFederationExecutionsResponse is empty per IEEE 1516.1 — the actual
    // list is delivered as a `reportFederationExecutions` callback. For now
    // we just assert the synchronous ack came back.
    let body = encode_request(
        fedpro::call_request::CallRequest::ListFederationExecutionsRequest(
            fedpro::ListFederationExecutionsRequest {},
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, body).await.unwrap();
    assert!(matches!(
        decode_response(&resp),
        fedpro::call_response::CallResponse::ListFederationExecutionsResponse(_)
    ));
}

#[tokio::test]
async fn multiple_calls_in_one_session_increment_sequence() {
    let addr = boot().await;
    let (mut sock, mut state) = open(addr).await;

    for i in 0..5 {
        let name = format!("multi-{i}");
        let body = encode_request(
            fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
                fedpro::CreateFederationExecutionRequest {
                    federation_name: name.clone(),
                    fom_module: None,
                },
            ),
        );
        let resp = send_hla_call(&mut sock, &mut state, body).await.unwrap();
        assert!(matches!(
            decode_response(&resp),
            fedpro::call_response::CallResponse::CreateFederationExecutionResponse(_)
        ));
    }
    // 5 calls should have consumed seq 1..=5, leaving next_outbound = 6.
    assert_eq!(state.next_outbound, 6);
}
