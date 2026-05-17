//! End-to-end WebSocket transport: publisher + subscriber federate exchange
//! `discoverObjectInstance` + `reflectAttributeValues` callbacks over WS.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, ObjectClassHandle,
    ObjectInstanceHandle,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;
use tokio::net::TcpListener;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>WS</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Beacon</name>
        <attribute>
          <name>Power</name>
          <dataType>HLAinteger32BE</dataType>
          <updateType>Static</updateType>
          <ownership>NoTransfer</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
</objectModel>"#;

#[derive(Default)]
struct Recorder {
    discoveries: Mutex<u32>,
    reflects: Mutex<Vec<AttributeHandleValueMap>>,
}

impl FederateAmbassador for Recorder {
    async fn discover_object_instance(
        &self,
        _instance: ObjectInstanceHandle,
        _class: ObjectClassHandle,
        _name: String,
        _producing_federate: Option<FederateHandle>,
    ) {
        *self.discoveries.lock() += 1;
    }
    async fn reflect_attribute_values(
        &self,
        _instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        _tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.reflects.lock().push(values);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pub_sub_over_websocket() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(bind).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node = Arc::new(RtiNode::new(addr));
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(FOM).unwrap()]).unwrap());
    let serve = Arc::clone(&node);
    tokio::spawn(async move {
        let _ = serve.serve_ws(listener).await;
    });
    let url = format!("ws://{addr}");

    // Subscriber over WebSocket.
    let rec = Arc::new(Recorder::default());
    let sub = RtiAmbassador::connect_ws(&url, Arc::clone(&rec))
        .await
        .unwrap();
    sub.create_federation_execution("ws-fed").await.ok();
    sub.join_federation_execution("Sub", "ws-fed")
        .await
        .unwrap();
    let beacon = sub
        .get_object_class_handle("HLAobjectRoot.Beacon")
        .await
        .unwrap();
    let power = sub.get_attribute_handle(beacon, "Power").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(power);
    sub.subscribe_object_class_attributes(beacon, attrs.clone())
        .await
        .unwrap();

    // Publisher over WebSocket.
    let publisher = RtiAmbassador::connect_ws(&url, Arc::new(Recorder::default()))
        .await
        .unwrap();
    publisher.create_federation_execution("ws-fed").await.ok();
    publisher
        .join_federation_execution("Pub", "ws-fed")
        .await
        .unwrap();
    publisher
        .publish_object_class_attributes(beacon, attrs)
        .await
        .unwrap();
    let instance = publisher.register_object_instance(beacon).await.unwrap();
    let mut values = AttributeHandleValueMap::new();
    values.insert(power, 250i32.to_be_bytes().to_vec());
    publisher
        .update_attribute_values(instance, values, b"ws-tag")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if *rec.discoveries.lock() >= 1 && !rec.reflects.lock().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        *rec.discoveries.lock() >= 1 && !rec.reflects.lock().is_empty(),
        "no WebSocket callbacks: discoveries={} reflects={}",
        *rec.discoveries.lock(),
        rec.reflects.lock().len()
    );
    let reflects = rec.reflects.lock();
    assert_eq!(reflects[0].get(&power).unwrap(), &250i32.to_be_bytes());
}
