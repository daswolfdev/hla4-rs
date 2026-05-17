//! Time Management — TAR/TAG, regulation, constrained, LBTS coordination.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_omt::{FomModule, MergedFom};
use hla_rti::{HeartbeatConfig, RtiNode};
use hla_wire::{ClientSeqState, MessageType, client_open_session, read_frame, send_hla_call};
use prost::Message;
use tokio::net::TcpStream;

const TRIVIAL_FOM: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>Trivial</name></modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
    </objectClass>
  </objects>
  <interactions>
    <interactionClass>
      <name>HLAinteractionRoot</name>
      <transportation>HLAreliable</transportation>
      <order>Receive</order>
    </interactionClass>
  </interactions>
</objectModel>"#;

async fn boot() -> (SocketAddr, Arc<RtiNode>) {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    // Long heartbeat so our time-management reads aren't interrupted by them.
    node.set_heartbeat_config(HeartbeatConfig {
        interval: Duration::from_secs(60),
        missing_timeout: Duration::from_secs(180),
        reconnect_window: Duration::from_secs(0),
    });
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(TRIVIAL_FOM).unwrap()]).unwrap());
    let addr = node.bind_addr;
    let serve = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, node)
}

fn encode(v: fedpro::call_request::CallRequest) -> Vec<u8> {
    fedpro::CallRequest {
        call_request: Some(v),
    }
    .encode_to_vec()
}

fn decode(b: &[u8]) -> fedpro::call_response::CallResponse {
    fedpro::CallResponse::decode(b).unwrap().call_response.unwrap()
}

async fn join(addr: SocketAddr, federation: &str) -> (TcpStream, ClientSeqState) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    let mut state = ClientSeqState::new(ack.session_id);
    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
            fedpro::CreateFederationExecutionRequest {
                federation_name: federation.into(),
                fom_module: None,
            },
        )),
    )
    .await
    .unwrap();
    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "TM".into(),
                federation_name: federation.into(),
            },
        )),
    )
    .await
    .unwrap();
    (sock, state)
}

async fn next_callback(sock: &mut TcpStream) -> fedpro::CallbackRequest {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), read_frame(sock))
            .await
            .expect("timeout waiting for callback")
            .expect("read_frame failed");
        match frame.header.message_type {
            MessageType::HlaCallbackRequest => {
                return fedpro::CallbackRequest::decode(&frame.payload[..]).unwrap();
            }
            MessageType::CtrlHeartbeat => continue,
            other => panic!("expected callback, got {other:?}"),
        }
    }
}

fn decode_time(opt: &Option<fedpro::LogicalTime>) -> f64 {
    let lt = opt.as_ref().unwrap();
    assert_eq!(lt.data.len(), 8);
    f64::from_be_bytes(lt.data[..].try_into().unwrap())
}

fn encode_lt(t: f64) -> fedpro::LogicalTime {
    fedpro::LogicalTime {
        data: t.to_be_bytes().to_vec(),
    }
}

fn encode_lti(d: f64) -> fedpro::LogicalTimeInterval {
    fedpro::LogicalTimeInterval {
        data: d.to_be_bytes().to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enable_time_regulation_grants_immediate_callback() {
    let (addr, _node) = boot().await;
    let (mut sock, mut state) = join(addr, "tm1").await;

    let resp = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(1.0)),
            },
        )),
    )
    .await
    .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::EnableTimeRegulationResponse(_)
    ));

    let cb = next_callback(&mut sock).await;
    match cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::TimeRegulationEnabled(tre) => {
            assert_eq!(decode_time(&tre.time), 0.0);
        }
        other => panic!("expected TimeRegulationEnabled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enable_time_constrained_grants_immediate_callback() {
    let (addr, _node) = boot().await;
    let (mut sock, mut state) = join(addr, "tm2").await;

    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::EnableTimeConstrainedRequest(
            fedpro::EnableTimeConstrainedRequest {},
        )),
    )
    .await
    .unwrap();

    let cb = next_callback(&mut sock).await;
    match cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::TimeConstrainedEnabled(tce) => {
            assert_eq!(decode_time(&tce.time), 0.0);
        }
        other => panic!("expected TimeConstrainedEnabled, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconstrained_advance_is_granted_immediately() {
    let (addr, _node) = boot().await;
    let (mut sock, mut state) = join(addr, "tm3").await;

    // No regulation, no constraint. TAR should grant immediately.
    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(5.0)),
            },
        )),
    )
    .await
    .unwrap();

    let cb = next_callback(&mut sock).await;
    match cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(g) => {
            assert_eq!(decode_time(&g.time), 5.0);
        }
        other => panic!("expected TAG, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constrained_with_no_regulator_advances_freely() {
    let (addr, _node) = boot().await;
    let (mut sock, mut state) = join(addr, "tm4").await;
    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::EnableTimeConstrainedRequest(
            fedpro::EnableTimeConstrainedRequest {},
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut sock).await; // TimeConstrainedEnabled

    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(10.0)),
            },
        )),
    )
    .await
    .unwrap();

    let cb = next_callback(&mut sock).await;
    match cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(g) => {
            assert_eq!(decode_time(&g.time), 10.0);
        }
        other => panic!("expected TAG, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regulator_lookahead_blocks_constrained_until_advanced() {
    let (addr, _node) = boot().await;

    // Federate R is regulating with lookahead=1.0, time=0 → LBTS=1.0
    let (mut r_sock, mut r_state) = join(addr, "tm5").await;
    let _ = send_hla_call(
        &mut r_sock,
        &mut r_state,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(1.0)),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut r_sock).await; // TimeRegulationEnabled

    // Federate C is constrained.
    let (mut c_sock, mut c_state) = join(addr, "tm5").await;
    let _ = send_hla_call(
        &mut c_sock,
        &mut c_state,
        encode(fedpro::call_request::CallRequest::EnableTimeConstrainedRequest(
            fedpro::EnableTimeConstrainedRequest {},
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut c_sock).await; // TimeConstrainedEnabled

    // C requests advance to 0.5 → should grant (0.5 <= LBTS=1.0)
    let _ = send_hla_call(
        &mut c_sock,
        &mut c_state,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(0.5)),
            },
        )),
    )
    .await
    .unwrap();
    let cb = next_callback(&mut c_sock).await;
    assert!(matches!(
        cb.callback_request,
        Some(fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(_))
    ));

    // C requests advance to 5.0 → blocked at LBTS=1.0 (no grant arrives)
    let _ = send_hla_call(
        &mut c_sock,
        &mut c_state,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(5.0)),
            },
        )),
    )
    .await
    .unwrap();
    let no_cb = tokio::time::timeout(Duration::from_millis(200), read_frame(&mut c_sock)).await;
    assert!(no_cb.is_err(), "TAG should be blocked but arrived");

    // R advances to 5.0 → LBTS = 5.0 + 1.0 = 6.0 → C's pending advance to 5.0 is now grantable.
    let _ = send_hla_call(
        &mut r_sock,
        &mut r_state,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(5.0)),
            },
        )),
    )
    .await
    .unwrap();
    // R, being unconstrained, gets its own grant first.
    let r_cb = next_callback(&mut r_sock).await;
    assert!(matches!(
        r_cb.callback_request,
        Some(fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(_))
    ));
    // C should now receive its previously-pending grant.
    let c_cb = next_callback(&mut c_sock).await;
    match c_cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(g) => {
            assert_eq!(decode_time(&g.time), 5.0);
        }
        other => panic!("expected TAG, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_regulators_compute_min_lbts() {
    let (addr, _node) = boot().await;

    // R1: lookahead=2.0, time=0 → contributes 2.0
    let (mut r1, mut s1) = join(addr, "tm6").await;
    let _ = send_hla_call(
        &mut r1,
        &mut s1,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(2.0)),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut r1).await;

    // R2: lookahead=0.5, time=0 → contributes 0.5 → min LBTS = 0.5
    let (mut r2, mut s2) = join(addr, "tm6").await;
    let _ = send_hla_call(
        &mut r2,
        &mut s2,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(0.5)),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut r2).await;

    // Constrained C: advance to 0.5 should grant (LBTS=0.5)
    let (mut c, mut cs) = join(addr, "tm6").await;
    let _ = send_hla_call(
        &mut c,
        &mut cs,
        encode(fedpro::call_request::CallRequest::EnableTimeConstrainedRequest(
            fedpro::EnableTimeConstrainedRequest {},
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut c).await;

    let _ = send_hla_call(
        &mut c,
        &mut cs,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(0.5)),
            },
        )),
    )
    .await
    .unwrap();
    let cb = next_callback(&mut c).await;
    assert!(matches!(
        cb.callback_request,
        Some(fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(_))
    ));

    // Advance to 1.0 must block — limited by R2's smaller LBTS contribution.
    let _ = send_hla_call(
        &mut c,
        &mut cs,
        encode(fedpro::call_request::CallRequest::TimeAdvanceRequestRequest(
            fedpro::TimeAdvanceRequestRequest {
                time: Some(encode_lt(1.0)),
            },
        )),
    )
    .await
    .unwrap();
    let no_cb = tokio::time::timeout(Duration::from_millis(150), read_frame(&mut c)).await;
    assert!(
        no_cb.is_err(),
        "TAG should be blocked by R2's lookahead, but arrived"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_logical_time_and_lookahead() {
    let (addr, _node) = boot().await;
    let (mut sock, mut state) = join(addr, "tm7").await;

    // Before enabling regulation, query_logical_time returns 0.0.
    let resp = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::QueryLogicalTimeRequest(
            fedpro::QueryLogicalTimeRequest {},
        )),
    )
    .await
    .unwrap();
    match decode(&resp) {
        fedpro::call_response::CallResponse::QueryLogicalTimeResponse(r) => {
            assert_eq!(decode_time(&r.result), 0.0);
        }
        other => panic!("got {other:?}"),
    }

    // Lookahead query before regulation fails.
    let resp = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::QueryLookaheadRequest(
            fedpro::QueryLookaheadRequest {},
        )),
    )
    .await
    .unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("got {other:?}"),
    };
    assert_eq!(exc.exception_name, "TimeRegulationIsNotEnabled");

    // Enable regulation, then query lookahead.
    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(2.5)),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut sock).await;

    let resp = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::QueryLookaheadRequest(
            fedpro::QueryLookaheadRequest {},
        )),
    )
    .await
    .unwrap();
    match decode(&resp) {
        fedpro::call_response::CallResponse::QueryLookaheadResponse(r) => {
            let lt = r.result.unwrap();
            assert_eq!(lt.data.len(), 8);
            assert_eq!(f64::from_be_bytes(lt.data[..].try_into().unwrap()), 2.5);
        }
        other => panic!("got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_enable_regulation_fails() {
    let (addr, _node) = boot().await;
    let (mut sock, mut state) = join(addr, "tm8").await;

    let _ = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(1.0)),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_callback(&mut sock).await;

    let resp = send_hla_call(
        &mut sock,
        &mut state,
        encode(fedpro::call_request::CallRequest::EnableTimeRegulationRequest(
            fedpro::EnableTimeRegulationRequest {
                lookahead: Some(encode_lti(1.0)),
            },
        )),
    )
    .await
    .unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("got {other:?}"),
    };
    assert_eq!(exc.exception_name, "TimeRegulationAlreadyEnabled");
}
