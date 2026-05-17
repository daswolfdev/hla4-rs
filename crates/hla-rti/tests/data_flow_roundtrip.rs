//! End-to-end data flow: publisher → RTI → subscriber callbacks.
//!
//! This is the milestone where two Rust federates actually exchange
//! application data via the RTI. Federate A publishes & registers an
//! object instance, then updates an attribute. Federate B (which
//! subscribed first) receives `discoverObjectInstance` followed by
//! `reflectAttributeValues`. A parallel test does the same for
//! `sendInteraction` / `receiveInteraction`.

use std::net::SocketAddr;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use hla_wire::{ClientSeqState, MessageType, client_open_session, read_frame, send_hla_call};
use prost::Message;
use tokio::net::TcpStream;

const SUSHI_LITE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>SushiLite</name></modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
      <objectClass>
        <name>Food</name>
        <objectClass>
          <name>Drink</name>
          <attribute>
            <name>NumberCups</name>
            <dataType>HLAinteger32BE</dataType>
            <updateType>Static</updateType>
            <ownership>NoTransfer</ownership>
            <sharing>PublishSubscribe</sharing>
            <transportation>HLAreliable</transportation>
            <order>Receive</order>
          </attribute>
        </objectClass>
      </objectClass>
    </objectClass>
  </objects>
  <interactions>
    <interactionClass>
      <name>HLAinteractionRoot</name>
      <transportation>HLAreliable</transportation>
      <order>Receive</order>
      <interactionClass>
        <name>FoodServed</name>
        <transportation>HLAreliable</transportation>
        <order>Receive</order>
        <parameter>
          <name>FoodType</name>
          <dataType>HLAunicodeString</dataType>
        </parameter>
      </interactionClass>
    </interactionClass>
  </interactions>
</objectModel>"#;

async fn boot() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    let fom = MergedFom::merge(vec![FomModule::parse(SUSHI_LITE).unwrap()]).unwrap();
    node.set_default_fom(fom);
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
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

async fn open_join_create(addr: SocketAddr, federation: &str) -> (TcpStream, ClientSeqState) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    let mut state = ClientSeqState::new(ack.session_id);
    let create = encode(
        fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
            fedpro::CreateFederationExecutionRequest {
                federation_name: federation.into(),
                fom_module: None,
            },
        ),
    );
    let _ = send_hla_call(&mut sock, &mut state, create).await.unwrap();
    let join = encode(
        fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
            fedpro::JoinFederationExecutionRequest {
                federate_type: "T".into(),
                federation_name: federation.into(),
            },
        ),
    );
    let _ = send_hla_call(&mut sock, &mut state, join).await.unwrap();
    (sock, state)
}

async fn lookup_drink_cups(
    sock: &mut TcpStream,
    state: &mut ClientSeqState,
) -> (fedpro::ObjectClassHandle, fedpro::AttributeHandle) {
    let drink_req = encode(
        fedpro::call_request::CallRequest::GetObjectClassHandleRequest(
            fedpro::GetObjectClassHandleRequest {
                object_class_name: "HLAobjectRoot.Food.Drink".into(),
            },
        ),
    );
    let resp = send_hla_call(sock, state, drink_req).await.unwrap();
    let drink = match decode(&resp) {
        fedpro::call_response::CallResponse::GetObjectClassHandleResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };
    let cups_req = encode(
        fedpro::call_request::CallRequest::GetAttributeHandleRequest(
            fedpro::GetAttributeHandleRequest {
                object_class: Some(drink.clone()),
                attribute_name: "NumberCups".into(),
            },
        ),
    );
    let resp = send_hla_call(sock, state, cups_req).await.unwrap();
    let cups = match decode(&resp) {
        fedpro::call_response::CallResponse::GetAttributeHandleResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };
    (drink, cups)
}

async fn lookup_food_served(
    sock: &mut TcpStream,
    state: &mut ClientSeqState,
) -> (fedpro::InteractionClassHandle, fedpro::ParameterHandle) {
    let ix_req = encode(
        fedpro::call_request::CallRequest::GetInteractionClassHandleRequest(
            fedpro::GetInteractionClassHandleRequest {
                interaction_class_name: "HLAinteractionRoot.FoodServed".into(),
            },
        ),
    );
    let resp = send_hla_call(sock, state, ix_req).await.unwrap();
    let ix = match decode(&resp) {
        fedpro::call_response::CallResponse::GetInteractionClassHandleResponse(r) => {
            r.result.unwrap()
        }
        _ => panic!(),
    };
    let param_req = encode(
        fedpro::call_request::CallRequest::GetParameterHandleRequest(
            fedpro::GetParameterHandleRequest {
                interaction_class: Some(ix.clone()),
                parameter_name: "FoodType".into(),
            },
        ),
    );
    let resp = send_hla_call(sock, state, param_req).await.unwrap();
    let param = match decode(&resp) {
        fedpro::call_response::CallResponse::GetParameterHandleResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };
    (ix, param)
}

async fn read_callback(sock: &mut TcpStream) -> fedpro::CallbackRequest {
    let frame = tokio::time::timeout(Duration::from_secs(2), read_frame(sock))
        .await
        .expect("timed out waiting for callback frame")
        .expect("read_frame error");
    assert_eq!(
        frame.header.message_type,
        MessageType::HlaCallbackRequest,
        "expected callback, got {:?}",
        frame.header.message_type
    );
    fedpro::CallbackRequest::decode(&frame.payload[..]).expect("decode CallbackRequest")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_to_subscriber_object_update() {
    let addr = boot().await;

    // Subscriber B connects + joins first, subscribes to Drink.NumberCups.
    let (mut b_sock, mut b_state) = open_join_create(addr, "feda").await;
    let (drink, cups) = lookup_drink_cups(&mut b_sock, &mut b_state).await;

    let sub = encode(
        fedpro::call_request::CallRequest::SubscribeObjectClassAttributesRequest(
            fedpro::SubscribeObjectClassAttributesRequest {
                object_class: Some(drink.clone()),
                attributes: Some(fedpro::AttributeHandleSet {
                    attribute_handle: vec![cups.clone()],
                }),
            },
        ),
    );
    let _ = send_hla_call(&mut b_sock, &mut b_state, sub).await.unwrap();

    // Publisher A connects + joins, publishes, registers, updates.
    let (mut a_sock, mut a_state) = open_join_create(addr, "feda").await;
    let (drink_a, cups_a) = lookup_drink_cups(&mut a_sock, &mut a_state).await;
    assert_eq!(
        drink, drink_a,
        "FOM handle assignment should be deterministic"
    );
    assert_eq!(cups, cups_a);

    let pub_req = encode(
        fedpro::call_request::CallRequest::PublishObjectClassAttributesRequest(
            fedpro::PublishObjectClassAttributesRequest {
                object_class: Some(drink_a.clone()),
                attributes: Some(fedpro::AttributeHandleSet {
                    attribute_handle: vec![cups_a.clone()],
                }),
            },
        ),
    );
    let _ = send_hla_call(&mut a_sock, &mut a_state, pub_req)
        .await
        .unwrap();

    let reg = encode(
        fedpro::call_request::CallRequest::RegisterObjectInstanceRequest(
            fedpro::RegisterObjectInstanceRequest {
                object_class: Some(drink_a.clone()),
            },
        ),
    );
    let resp = send_hla_call(&mut a_sock, &mut a_state, reg).await.unwrap();
    let instance = match decode(&resp) {
        fedpro::call_response::CallResponse::RegisterObjectInstanceResponse(r) => r.result.unwrap(),
        other => panic!("expected RegisterObjectInstanceResponse, got {other:?}"),
    };

    let update = encode(
        fedpro::call_request::CallRequest::UpdateAttributeValuesRequest(
            fedpro::UpdateAttributeValuesRequest {
                object_instance: Some(instance.clone()),
                attribute_values: Some(fedpro::AttributeHandleValueMap {
                    attribute_handle_value: vec![fedpro::AttributeHandleValue {
                        attribute_handle: Some(cups_a),
                        value: 42i32.to_be_bytes().to_vec(),
                    }],
                }),
                user_supplied_tag: b"first-pour".to_vec(),
            },
        ),
    );
    let resp = send_hla_call(&mut a_sock, &mut a_state, update)
        .await
        .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::UpdateAttributeValuesResponse(_)
    ));

    // Subscriber should now receive: discoverObjectInstance, then
    // reflectAttributeValues (in that order, per IEEE 1516.1).
    let cb1 = read_callback(&mut b_sock).await;
    let discover = match cb1.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::DiscoverObjectInstance(d) => d,
        other => panic!("expected DiscoverObjectInstance, got {other:?}"),
    };
    assert_eq!(
        discover.object_instance.as_ref().unwrap().data,
        instance.data,
        "instance handles should match"
    );
    assert_eq!(discover.object_class.unwrap().data, drink.data);

    let cb2 = read_callback(&mut b_sock).await;
    let reflect = match cb2.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::ReflectAttributeValues(r) => r,
        other => panic!("expected ReflectAttributeValues, got {other:?}"),
    };
    let values = reflect.attribute_values.unwrap();
    assert_eq!(values.attribute_handle_value.len(), 1);
    let entry = &values.attribute_handle_value[0];
    assert_eq!(entry.attribute_handle.as_ref().unwrap().data, cups.data);
    assert_eq!(entry.value, 42i32.to_be_bytes());
    assert_eq!(reflect.user_supplied_tag, b"first-pour");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_to_subscriber_interaction() {
    let addr = boot().await;

    let (mut b_sock, mut b_state) = open_join_create(addr, "fedi").await;
    let (food_served, food_type) = lookup_food_served(&mut b_sock, &mut b_state).await;
    let sub = encode(
        fedpro::call_request::CallRequest::SubscribeInteractionClassRequest(
            fedpro::SubscribeInteractionClassRequest {
                interaction_class: Some(food_served.clone()),
            },
        ),
    );
    let _ = send_hla_call(&mut b_sock, &mut b_state, sub).await.unwrap();

    let (mut a_sock, mut a_state) = open_join_create(addr, "fedi").await;
    let (food_served_a, food_type_a) = lookup_food_served(&mut a_sock, &mut a_state).await;
    let pub_req = encode(
        fedpro::call_request::CallRequest::PublishInteractionClassRequest(
            fedpro::PublishInteractionClassRequest {
                interaction_class: Some(food_served_a.clone()),
            },
        ),
    );
    let _ = send_hla_call(&mut a_sock, &mut a_state, pub_req)
        .await
        .unwrap();

    let send = encode(fedpro::call_request::CallRequest::SendInteractionRequest(
        fedpro::SendInteractionRequest {
            interaction_class: Some(food_served_a),
            parameter_values: Some(fedpro::ParameterHandleValueMap {
                parameter_handle_value: vec![fedpro::ParameterHandleValue {
                    parameter_handle: Some(food_type_a),
                    value: b"ramen".to_vec(),
                }],
            }),
            user_supplied_tag: b"order-1".to_vec(),
        },
    ));
    let resp = send_hla_call(&mut a_sock, &mut a_state, send)
        .await
        .unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::SendInteractionResponse(_)
    ));

    let cb = read_callback(&mut b_sock).await;
    let receive = match cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::ReceiveInteraction(r) => r,
        other => panic!("expected ReceiveInteraction, got {other:?}"),
    };
    assert_eq!(receive.interaction_class.unwrap().data, food_served.data);
    let params = receive.parameter_values.unwrap();
    assert_eq!(params.parameter_handle_value.len(), 1);
    assert_eq!(params.parameter_handle_value[0].value, b"ramen");
    assert_eq!(
        params.parameter_handle_value[0]
            .parameter_handle
            .as_ref()
            .unwrap()
            .data,
        food_type.data
    );
    assert_eq!(receive.user_supplied_tag, b"order-1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_object_instance_fires_remove_callback() {
    let addr = boot().await;

    let (mut b_sock, mut b_state) = open_join_create(addr, "feddel").await;
    let (drink, cups) = lookup_drink_cups(&mut b_sock, &mut b_state).await;
    let _ = send_hla_call(
        &mut b_sock,
        &mut b_state,
        encode(
            fedpro::call_request::CallRequest::SubscribeObjectClassAttributesRequest(
                fedpro::SubscribeObjectClassAttributesRequest {
                    object_class: Some(drink.clone()),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: vec![cups.clone()],
                    }),
                },
            ),
        ),
    )
    .await
    .unwrap();

    let (mut a_sock, mut a_state) = open_join_create(addr, "feddel").await;
    let (drink_a, cups_a) = lookup_drink_cups(&mut a_sock, &mut a_state).await;
    let _ = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::PublishObjectClassAttributesRequest(
                fedpro::PublishObjectClassAttributesRequest {
                    object_class: Some(drink_a.clone()),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: vec![cups_a],
                    }),
                },
            ),
        ),
    )
    .await
    .unwrap();
    let resp = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::RegisterObjectInstanceRequest(
                fedpro::RegisterObjectInstanceRequest {
                    object_class: Some(drink_a),
                },
            ),
        ),
    )
    .await
    .unwrap();
    let instance = match decode(&resp) {
        fedpro::call_response::CallResponse::RegisterObjectInstanceResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };

    // Subscriber receives discoverObjectInstance.
    let _discover = read_callback(&mut b_sock).await;

    // Publisher deletes; subscriber should receive RemoveObjectInstance.
    let _ = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::DeleteObjectInstanceRequest(
                fedpro::DeleteObjectInstanceRequest {
                    object_instance: Some(instance.clone()),
                    user_supplied_tag: b"closing-time".to_vec(),
                },
            ),
        ),
    )
    .await
    .unwrap();

    let cb = read_callback(&mut b_sock).await;
    let remove = match cb.callback_request.unwrap() {
        fedpro::callback_request::CallbackRequest::RemoveObjectInstance(r) => r,
        other => panic!("expected RemoveObjectInstance, got {other:?}"),
    };
    assert_eq!(remove.object_instance.unwrap().data, instance.data);
    assert_eq!(remove.user_supplied_tag, b"closing-time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_does_not_receive_own_callback() {
    let addr = boot().await;

    let (mut a_sock, mut a_state) = open_join_create(addr, "fedself").await;
    let (drink, cups) = lookup_drink_cups(&mut a_sock, &mut a_state).await;

    // A subscribes AND publishes — but A's own updates should not loop back.
    let _ = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::SubscribeObjectClassAttributesRequest(
                fedpro::SubscribeObjectClassAttributesRequest {
                    object_class: Some(drink.clone()),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: vec![cups.clone()],
                    }),
                },
            ),
        ),
    )
    .await
    .unwrap();
    let _ = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::PublishObjectClassAttributesRequest(
                fedpro::PublishObjectClassAttributesRequest {
                    object_class: Some(drink.clone()),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: vec![cups.clone()],
                    }),
                },
            ),
        ),
    )
    .await
    .unwrap();
    let resp = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::RegisterObjectInstanceRequest(
                fedpro::RegisterObjectInstanceRequest {
                    object_class: Some(drink),
                },
            ),
        ),
    )
    .await
    .unwrap();
    let instance = match decode(&resp) {
        fedpro::call_response::CallResponse::RegisterObjectInstanceResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };
    let _ = send_hla_call(
        &mut a_sock,
        &mut a_state,
        encode(
            fedpro::call_request::CallRequest::UpdateAttributeValuesRequest(
                fedpro::UpdateAttributeValuesRequest {
                    object_instance: Some(instance),
                    attribute_values: Some(fedpro::AttributeHandleValueMap {
                        attribute_handle_value: vec![fedpro::AttributeHandleValue {
                            attribute_handle: Some(cups),
                            value: 1i32.to_be_bytes().to_vec(),
                        }],
                    }),
                    user_supplied_tag: b"self-test".to_vec(),
                },
            ),
        ),
    )
    .await
    .unwrap();

    // Nothing should arrive on A's socket. Wait briefly with a short timeout
    // — if a callback comes in, that's a routing bug.
    let next = tokio::time::timeout(Duration::from_millis(200), read_frame(&mut a_sock)).await;
    assert!(
        next.is_err(),
        "producer should not receive its own callback, but got {:?}",
        next.unwrap().map(|f| f.header.message_type)
    );
}
