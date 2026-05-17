//! Verifies CreateFederationExecutionWithModules consumes federate-provided
//! FOMs (the realistic deployment path — federates ship the FOM in the
//! create request rather than the RTI being pre-configured).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, ObjectClassHandle,
    ObjectInstanceHandle,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const INBAND_FOM: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>InbandTest</name></modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
      <objectClass>
        <name>Robot</name>
        <attribute>
          <name>BatteryLevel</name>
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

async fn boot() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    // Deliberately do NOT install a default FOM — the federate provides one
    // in the create request.
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

#[derive(Default)]
struct Recorder {
    reflects: Mutex<Vec<AttributeHandleValueMap>>,
    discoveries: Mutex<u32>,
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
async fn create_with_inband_fom_then_publish_subscribe() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec = Arc::new(Recorder::default());
    let sub = RtiAmbassador::connect(&url, Arc::clone(&rec)).await.unwrap();

    // First federate creates with the in-band FOM.
    sub.create_federation_execution_with_modules(
        "inband-fed",
        vec![("InbandTest.xml".into(), INBAND_FOM.as_bytes().to_vec())],
    )
    .await
    .unwrap();
    sub.join_federation_execution("Subscriber", "inband-fed").await.unwrap();

    let robot = sub.get_object_class_handle("HLAobjectRoot.Robot").await.unwrap();
    let battery = sub.get_attribute_handle(robot, "BatteryLevel").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(battery);
    sub.subscribe_object_class_attributes(robot, attrs.clone())
        .await
        .unwrap();

    // Second federate joins the same federation. Should see the same handles.
    let publisher = RtiAmbassador::connect(&url, Arc::new(Recorder::default()))
        .await
        .unwrap();
    publisher
        .join_federation_execution("Publisher", "inband-fed")
        .await
        .unwrap();
    let robot_p = publisher.get_object_class_handle("HLAobjectRoot.Robot").await.unwrap();
    let battery_p = publisher.get_attribute_handle(robot_p, "BatteryLevel").await.unwrap();
    assert_eq!(robot, robot_p, "FOM handle assignment should be deterministic");
    assert_eq!(battery, battery_p);

    publisher
        .publish_object_class_attributes(robot_p, attrs)
        .await
        .unwrap();
    let instance = publisher.register_object_instance(robot_p).await.unwrap();
    let mut values = AttributeHandleValueMap::new();
    values.insert(battery_p, 87i32.to_be_bytes().to_vec());
    publisher
        .update_attribute_values(instance, values, b"")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if !rec.reflects.lock().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let reflects = rec.reflects.lock();
    assert_eq!(reflects.len(), 1, "should have received one reflect");
    assert_eq!(reflects[0].get(&battery).unwrap(), &87i32.to_be_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_inband_fom_returns_exception() {
    let addr = boot().await;
    let url = format!("rti://{addr}");
    let amb = RtiAmbassador::connect(&url, Arc::new(Recorder::default()))
        .await
        .unwrap();
    let err = amb
        .create_federation_execution_with_modules(
            "bad-fed",
            vec![("Bogus.xml".into(), b"<not-xml<<<".to_vec())],
        )
        .await
        .unwrap_err();
    match err {
        hla_federate::CallError::RtiException { name, .. } => {
            assert_eq!(name, "CouldNotOpenFDD");
        }
        other => panic!("expected RtiException, got {other:?}"),
    }
}
