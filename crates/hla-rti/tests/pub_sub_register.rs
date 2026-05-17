//! Handle lookup + publish/subscribe + registerObjectInstance integration.
//!
//! Boots `RtiNode` with a Sushi-lite FOM, joins a federate, and exercises:
//!   * getObjectClassHandle / getAttributeHandle / their inverses
//!   * getInteractionClassHandle / getParameterHandle / their inverses
//!   * publish/subscribe state mutation (and the federation-level
//!     subscription matrix)
//!   * registerObjectInstance gating on prior publish
//!   * registerObjectInstanceWithName name-conflict path

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_fedpro_proto::fedpro;
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use hla_wire::{ClientSeqState, client_open_session, send_hla_call};
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
        <attribute>
          <name>Color</name>
          <dataType>HLAunicodeString</dataType>
          <updateType>Static</updateType>
          <ownership>NoTransfer</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
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

async fn boot_with_sushi() -> (SocketAddr, Arc<RtiNode>) {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    let fom = MergedFom::merge(vec![FomModule::parse(SUSHI_LITE).unwrap()]).unwrap();
    node.set_default_fom(fom);
    let addr = node.bind_addr;
    let serve_node = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve_node.serve(listener).await;
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

async fn open_and_join(
    addr: SocketAddr,
    federation: &str,
) -> (TcpStream, ClientSeqState) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let ack = client_open_session(&mut sock).await.unwrap();
    let mut state = ClientSeqState::new(ack.session_id);

    // Create-if-missing then join.
    let create = encode(fedpro::call_request::CallRequest::CreateFederationExecutionRequest(
        fedpro::CreateFederationExecutionRequest {
            federation_name: federation.into(),
            fom_module: None,
        },
    ));
    let _ = send_hla_call(&mut sock, &mut state, create).await.unwrap(); // ignore conflict

    let join = encode(fedpro::call_request::CallRequest::JoinFederationExecutionRequest(
        fedpro::JoinFederationExecutionRequest {
            federate_type: "Tester".into(),
            federation_name: federation.into(),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, join).await.unwrap();
    let _ = decode(&resp);
    (sock, state)
}

async fn get_object_class_handle(
    sock: &mut TcpStream,
    state: &mut ClientSeqState,
    name: &str,
) -> fedpro::call_response::CallResponse {
    let req = encode(fedpro::call_request::CallRequest::GetObjectClassHandleRequest(
        fedpro::GetObjectClassHandleRequest {
            object_class_name: name.into(),
        },
    ));
    let resp = send_hla_call(sock, state, req).await.unwrap();
    decode(&resp)
}

#[tokio::test]
async fn handle_lookup_roundtrip() {
    let (addr, _node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-1").await;

    // Object class lookup
    let resp = get_object_class_handle(&mut sock, &mut state, "HLAobjectRoot.Food").await;
    let food_handle = match resp {
        fedpro::call_response::CallResponse::GetObjectClassHandleResponse(r) => r.result.unwrap(),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(food_handle.data.len(), 4);

    // Inverse: handle → name
    let req = encode(fedpro::call_request::CallRequest::GetObjectClassNameRequest(
        fedpro::GetObjectClassNameRequest {
            object_class: Some(food_handle.clone()),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    match decode(&resp) {
        fedpro::call_response::CallResponse::GetObjectClassNameResponse(r) => {
            assert_eq!(r.result, "HLAobjectRoot.Food");
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Attribute lookup
    let req = encode(fedpro::call_request::CallRequest::GetAttributeHandleRequest(
        fedpro::GetAttributeHandleRequest {
            object_class: Some(food_handle.clone()),
            attribute_name: "Color".into(),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let color_handle = match decode(&resp) {
        fedpro::call_response::CallResponse::GetAttributeHandleResponse(r) => r.result.unwrap(),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(color_handle.data.len(), 4);

    // Unknown attribute name
    let req = encode(fedpro::call_request::CallRequest::GetAttributeHandleRequest(
        fedpro::GetAttributeHandleRequest {
            object_class: Some(food_handle),
            attribute_name: "NoSuch".into(),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception: {other:?}"),
    };
    assert_eq!(exc.exception_name, "NameNotFound");
}

#[tokio::test]
async fn interaction_handle_lookup() {
    let (addr, _node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-ix").await;

    let req = encode(
        fedpro::call_request::CallRequest::GetInteractionClassHandleRequest(
            fedpro::GetInteractionClassHandleRequest {
                interaction_class_name: "HLAinteractionRoot.FoodServed".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let ix_handle = match decode(&resp) {
        fedpro::call_response::CallResponse::GetInteractionClassHandleResponse(r) => {
            r.result.unwrap()
        }
        other => panic!("unexpected: {other:?}"),
    };

    let req = encode(fedpro::call_request::CallRequest::GetParameterHandleRequest(
        fedpro::GetParameterHandleRequest {
            interaction_class: Some(ix_handle),
            parameter_name: "FoodType".into(),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::GetParameterHandleResponse(_)
    ));
}

async fn lookup_handles(
    sock: &mut TcpStream,
    state: &mut ClientSeqState,
) -> (fedpro::ObjectClassHandle, fedpro::AttributeHandle) {
    let drink = match get_object_class_handle(sock, state, "HLAobjectRoot.Food.Drink").await {
        fedpro::call_response::CallResponse::GetObjectClassHandleResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };
    let req = encode(fedpro::call_request::CallRequest::GetAttributeHandleRequest(
        fedpro::GetAttributeHandleRequest {
            object_class: Some(drink.clone()),
            attribute_name: "NumberCups".into(),
        },
    ));
    let resp = send_hla_call(sock, state, req).await.unwrap();
    let attr = match decode(&resp) {
        fedpro::call_response::CallResponse::GetAttributeHandleResponse(r) => r.result.unwrap(),
        _ => panic!(),
    };
    (drink, attr)
}

#[tokio::test]
async fn publish_then_register_object_instance() {
    let (addr, node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-pub").await;
    let (drink, cups) = lookup_handles(&mut sock, &mut state).await;

    // Publish HLAobjectRoot.Food.Drink {NumberCups}
    let req = encode(
        fedpro::call_request::CallRequest::PublishObjectClassAttributesRequest(
            fedpro::PublishObjectClassAttributesRequest {
                object_class: Some(drink.clone()),
                attributes: Some(fedpro::AttributeHandleSet {
                    attribute_handle: vec![cups],
                }),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::PublishObjectClassAttributesResponse(_)
    ));

    // Register an instance — should now succeed.
    let req = encode(fedpro::call_request::CallRequest::RegisterObjectInstanceRequest(
        fedpro::RegisterObjectInstanceRequest {
            object_class: Some(drink.clone()),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let inst = match decode(&resp) {
        fedpro::call_response::CallResponse::RegisterObjectInstanceResponse(r) => r.result.unwrap(),
        other => panic!("expected RegisterObjectInstanceResponse, got {other:?}"),
    };
    assert_eq!(inst.data.len(), 8, "ObjectInstanceHandle is 8 BE bytes");

    // Verify the instance landed in the federation registry.
    let federations = node.federations.read();
    let fed = federations.get("fed-pub").unwrap();
    let instances = fed.object_instances.read();
    assert_eq!(instances.len(), 1);
    let (_, single) = instances.iter().next().unwrap();
    assert_eq!(single.name, "HLA1");
}

#[tokio::test]
async fn register_without_publish_fails() {
    let (addr, _node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-no-pub").await;
    let (drink, _cups) = lookup_handles(&mut sock, &mut state).await;

    let req = encode(fedpro::call_request::CallRequest::RegisterObjectInstanceRequest(
        fedpro::RegisterObjectInstanceRequest {
            object_class: Some(drink),
        },
    ));
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "ObjectClassNotPublished");
}

#[tokio::test]
async fn subscribe_populates_federation_subscription_matrix() {
    let (addr, node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-sub").await;
    let (drink, cups) = lookup_handles(&mut sock, &mut state).await;

    let req = encode(
        fedpro::call_request::CallRequest::SubscribeObjectClassAttributesRequest(
            fedpro::SubscribeObjectClassAttributesRequest {
                object_class: Some(drink.clone()),
                attributes: Some(fedpro::AttributeHandleSet {
                    attribute_handle: vec![cups.clone()],
                }),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::SubscribeObjectClassAttributesResponse(_)
    ));

    // Federation-side: the subscription matrix has an entry for (Drink, NumberCups)
    let drink_raw = u32::from_be_bytes(drink.data[..].try_into().unwrap());
    let cups_raw = u32::from_be_bytes(cups.data[..].try_into().unwrap());
    let federations = node.federations.read();
    let fed = federations.get("fed-sub").unwrap();
    let subs = fed.subscriptions.read();
    let key = (
        hla_core::ObjectClassHandle::new(drink_raw),
        hla_core::AttributeHandle::new(cups_raw),
    );
    let subscribers = subs.by_attribute.get(&key).expect("subscriber entry missing");
    assert_eq!(subscribers.len(), 1);
}

#[tokio::test]
async fn unsubscribe_clears_federation_subscription_matrix() {
    let (addr, node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-unsub").await;
    let (drink, cups) = lookup_handles(&mut sock, &mut state).await;

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
    let _ = send_hla_call(&mut sock, &mut state, sub).await.unwrap();

    let unsub = encode(
        fedpro::call_request::CallRequest::UnsubscribeObjectClassAttributesRequest(
            fedpro::UnsubscribeObjectClassAttributesRequest {
                object_class: Some(drink.clone()),
                attributes: Some(fedpro::AttributeHandleSet {
                    attribute_handle: vec![cups.clone()],
                }),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, unsub).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::UnsubscribeObjectClassAttributesResponse(_)
    ));

    let drink_raw = u32::from_be_bytes(drink.data[..].try_into().unwrap());
    let cups_raw = u32::from_be_bytes(cups.data[..].try_into().unwrap());
    let federations = node.federations.read();
    let fed = federations.get("fed-unsub").unwrap();
    let subs = fed.subscriptions.read();
    let key = (
        hla_core::ObjectClassHandle::new(drink_raw),
        hla_core::AttributeHandle::new(cups_raw),
    );
    assert!(
        subs.by_attribute.get(&key).is_none(),
        "subscription should have been removed"
    );
}

#[tokio::test]
async fn publish_subscribe_interaction_class() {
    let (addr, node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-ix-sub").await;

    let req = encode(
        fedpro::call_request::CallRequest::GetInteractionClassHandleRequest(
            fedpro::GetInteractionClassHandleRequest {
                interaction_class_name: "HLAinteractionRoot.FoodServed".into(),
            },
        ),
    );
    let resp = send_hla_call(&mut sock, &mut state, req).await.unwrap();
    let ix = match decode(&resp) {
        fedpro::call_response::CallResponse::GetInteractionClassHandleResponse(r) => {
            r.result.unwrap()
        }
        _ => panic!(),
    };

    let pub_req = encode(fedpro::call_request::CallRequest::PublishInteractionClassRequest(
        fedpro::PublishInteractionClassRequest {
            interaction_class: Some(ix.clone()),
        },
    ));
    assert!(matches!(
        decode(&send_hla_call(&mut sock, &mut state, pub_req).await.unwrap()),
        fedpro::call_response::CallResponse::PublishInteractionClassResponse(_)
    ));

    let sub_req = encode(fedpro::call_request::CallRequest::SubscribeInteractionClassRequest(
        fedpro::SubscribeInteractionClassRequest {
            interaction_class: Some(ix.clone()),
        },
    ));
    assert!(matches!(
        decode(&send_hla_call(&mut sock, &mut state, sub_req).await.unwrap()),
        fedpro::call_response::CallResponse::SubscribeInteractionClassResponse(_)
    ));

    // Federation-side: subscription matrix populated for interaction.
    let ix_raw = u32::from_be_bytes(ix.data[..].try_into().unwrap());
    let federations = node.federations.read();
    let fed = federations.get("fed-ix-sub").unwrap();
    let subs = fed.subscriptions.read();
    let ix_handle = hla_core::InteractionClassHandle::new(ix_raw);
    assert!(subs.by_interaction.contains_key(&ix_handle));
}

#[tokio::test]
async fn register_object_instance_with_name_conflict() {
    let (addr, _node) = boot_with_sushi().await;
    let (mut sock, mut state) = open_and_join(addr, "fed-named").await;
    let (drink, cups) = lookup_handles(&mut sock, &mut state).await;

    let pub_req = encode(
        fedpro::call_request::CallRequest::PublishObjectClassAttributesRequest(
            fedpro::PublishObjectClassAttributesRequest {
                object_class: Some(drink.clone()),
                attributes: Some(fedpro::AttributeHandleSet {
                    attribute_handle: vec![cups],
                }),
            },
        ),
    );
    let _ = send_hla_call(&mut sock, &mut state, pub_req).await.unwrap();

    let reg = |name: &str| {
        encode(fedpro::call_request::CallRequest::RegisterObjectInstanceWithNameRequest(
            fedpro::RegisterObjectInstanceWithNameRequest {
                object_class: Some(drink.clone()),
                object_instance_name: name.into(),
            },
        ))
    };

    let resp = send_hla_call(&mut sock, &mut state, reg("Cola")).await.unwrap();
    assert!(matches!(
        decode(&resp),
        fedpro::call_response::CallResponse::RegisterObjectInstanceWithNameResponse(_)
    ));

    // Second register with the same name → exception.
    let resp = send_hla_call(&mut sock, &mut state, reg("Cola")).await.unwrap();
    let exc = match decode(&resp) {
        fedpro::call_response::CallResponse::ExceptionData(e) => e,
        other => panic!("expected exception, got {other:?}"),
    };
    assert_eq!(exc.exception_name, "ObjectInstanceNameInUse");
    assert_eq!(exc.details, "Cola");
}
