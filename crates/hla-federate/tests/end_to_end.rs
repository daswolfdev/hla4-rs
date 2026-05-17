//! End-to-end federate library test.
//!
//! Boots an in-process RTI, then drives BOTH the publisher and subscriber
//! federates through the typed `RtiAmbassador` API. The subscriber's
//! `FederateAmbassador` impl records what it received; the test asserts on
//! the recorded events.
//!
//! This is the first test where federate code looks like federate code
//! rather than raw protobuf assembly.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, InteractionClassHandle,
    ObjectClassHandle, ObjectInstanceHandle, ParameterHandleValueMap, ResignAction,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

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

async fn boot_rti() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(SUSHI_LITE).unwrap()]).unwrap());
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    addr
}

/// Recording subscriber: stores every callback it gets, in order.
#[derive(Default)]
struct Recorder {
    discoveries: Mutex<Vec<(ObjectInstanceHandle, ObjectClassHandle, String)>>,
    reflects: Mutex<Vec<(ObjectInstanceHandle, AttributeHandleValueMap, Vec<u8>)>>,
    interactions: Mutex<Vec<(InteractionClassHandle, ParameterHandleValueMap, Vec<u8>)>>,
    removes: Mutex<Vec<(ObjectInstanceHandle, Vec<u8>)>>,
}

impl FederateAmbassador for Recorder {
    async fn discover_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        class: ObjectClassHandle,
        name: String,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.discoveries.lock().push((instance, class, name));
    }

    async fn reflect_attribute_values(
        &self,
        instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.reflects.lock().push((instance, values, tag));
    }

    async fn receive_interaction(
        &self,
        class: InteractionClassHandle,
        params: ParameterHandleValueMap,
        tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.interactions.lock().push((class, params, tag));
    }

    async fn remove_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.removes.lock().push((instance, tag));
    }
}

/// Wait until `predicate` returns true or `timeout` elapses.
async fn wait_for<F: Fn() -> bool>(timeout: Duration, predicate: F) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    predicate()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_pub_sub_via_federate_library() {
    let addr = boot_rti().await;
    let rti_url = format!("rti://{addr}");

    // ---- Subscriber ----
    let recorder = Arc::new(Recorder::default());
    let sub = RtiAmbassador::connect(&rti_url, Arc::clone(&recorder))
        .await
        .unwrap();
    sub.create_federation_execution("e2e-fed").await.ok(); // ok if exists
    let _sub_handle = sub
        .join_federation_execution("Subscriber", "e2e-fed")
        .await
        .unwrap();

    let drink_class = sub
        .get_object_class_handle("HLAobjectRoot.Food.Drink")
        .await
        .unwrap();
    let cups_attr = sub
        .get_attribute_handle(drink_class, "NumberCups")
        .await
        .unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(cups_attr);
    sub.subscribe_object_class_attributes(drink_class, attrs.clone())
        .await
        .unwrap();

    let food_served = sub
        .get_interaction_class_handle("HLAinteractionRoot.FoodServed")
        .await
        .unwrap();
    let food_type_param = sub
        .get_parameter_handle(food_served, "FoodType")
        .await
        .unwrap();
    sub.subscribe_interaction_class(food_served).await.unwrap();

    // ---- Publisher ----
    let pub_recorder = Arc::new(Recorder::default());
    let publisher = RtiAmbassador::connect(&rti_url, Arc::clone(&pub_recorder))
        .await
        .unwrap();
    publisher.create_federation_execution("e2e-fed").await.ok();
    let _pub_handle = publisher
        .join_federation_execution("Publisher", "e2e-fed")
        .await
        .unwrap();

    publisher
        .publish_object_class_attributes(drink_class, attrs)
        .await
        .unwrap();
    publisher
        .publish_interaction_class(food_served)
        .await
        .unwrap();

    // Register + update.
    let instance = publisher
        .register_object_instance(drink_class)
        .await
        .unwrap();
    let mut values = AttributeHandleValueMap::new();
    values.insert(cups_attr, 42i32.to_be_bytes().to_vec());
    publisher
        .update_attribute_values(instance, values, b"first-pour")
        .await
        .unwrap();

    // Send interaction.
    let mut params = ParameterHandleValueMap::new();
    params.insert(food_type_param, b"sushi".to_vec());
    publisher
        .send_interaction(food_served, params, b"order-1")
        .await
        .unwrap();

    // Wait for the subscriber to record everything.
    let got_everything = wait_for(Duration::from_secs(2), || {
        let d = recorder.discoveries.lock().len();
        let r = recorder.reflects.lock().len();
        let i = recorder.interactions.lock().len();
        d >= 1 && r >= 1 && i >= 1
    })
    .await;
    assert!(
        got_everything,
        "subscriber didn't receive all callbacks: discoveries={} reflects={} interactions={}",
        recorder.discoveries.lock().len(),
        recorder.reflects.lock().len(),
        recorder.interactions.lock().len()
    );

    // Validate contents. Each lock is scoped to a bare block so the guard's
    // lifetime ends before any subsequent `.await` — keeps the
    // `await_holding_lock` lint sound without a function-level allow.
    {
        let discoveries = recorder.discoveries.lock();
        assert_eq!(discoveries.len(), 1);
        assert_eq!(discoveries[0].0, instance);
        assert_eq!(discoveries[0].1, drink_class);
    }
    {
        let reflects = recorder.reflects.lock();
        assert_eq!(reflects.len(), 1);
        assert_eq!(reflects[0].0, instance);
        assert_eq!(reflects[0].1.get(&cups_attr).unwrap(), &42i32.to_be_bytes());
        assert_eq!(reflects[0].2, b"first-pour");
    }
    {
        let interactions = recorder.interactions.lock();
        assert_eq!(interactions.len(), 1);
        assert_eq!(interactions[0].0, food_served);
        assert_eq!(
            interactions[0].1.get(&food_type_param).unwrap(),
            &b"sushi".to_vec()
        );
        assert_eq!(interactions[0].2, b"order-1");
    }

    // Delete + verify remove callback.
    publisher
        .delete_object_instance(instance, b"all-done")
        .await
        .unwrap();
    let removed = wait_for(Duration::from_secs(1), || {
        !recorder.removes.lock().is_empty()
    })
    .await;
    assert!(removed, "remove callback never arrived");

    sub.resign_federation_execution(ResignAction::DeleteObjects)
        .await
        .unwrap();
    publisher
        .resign_federation_execution(ResignAction::DeleteObjects)
        .await
        .unwrap();
    sub.disconnect().await.unwrap();
    publisher.disconnect().await.unwrap();
}

/// Time Management roundtrip via the federate library: regulating publisher
/// + constrained subscriber.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_management_via_federate_library() {
    let addr = boot_rti().await;
    let rti_url = format!("rti://{addr}");

    #[derive(Default)]
    struct TimeRec {
        regulation_enabled_at: Mutex<Option<f64>>,
        constrained_enabled_at: Mutex<Option<f64>>,
        grants: Mutex<Vec<f64>>,
    }

    impl FederateAmbassador for TimeRec {
        async fn time_regulation_enabled(&self, time: f64) {
            *self.regulation_enabled_at.lock() = Some(time);
        }
        async fn time_constrained_enabled(&self, time: f64) {
            *self.constrained_enabled_at.lock() = Some(time);
        }
        async fn time_advance_grant(&self, time: f64) {
            self.grants.lock().push(time);
        }
    }

    let r1 = Arc::new(TimeRec::default());
    let reg = RtiAmbassador::connect(&rti_url, Arc::clone(&r1))
        .await
        .unwrap();
    reg.create_federation_execution("tm-e2e").await.ok();
    reg.join_federation_execution("Regulator", "tm-e2e")
        .await
        .unwrap();
    reg.enable_time_regulation(1.0).await.unwrap();
    assert!(
        wait_for(Duration::from_millis(500), || {
            r1.regulation_enabled_at.lock().is_some()
        })
        .await
    );
    assert_eq!(*r1.regulation_enabled_at.lock(), Some(0.0));

    let r2 = Arc::new(TimeRec::default());
    let con = RtiAmbassador::connect(&rti_url, Arc::clone(&r2))
        .await
        .unwrap();
    con.create_federation_execution("tm-e2e").await.ok();
    con.join_federation_execution("Constrained", "tm-e2e")
        .await
        .unwrap();
    con.enable_time_constrained().await.unwrap();
    assert!(
        wait_for(Duration::from_millis(500), || {
            r2.constrained_enabled_at.lock().is_some()
        })
        .await
    );

    // Constrained TAR to 0.5 — LBTS=1.0 so should grant immediately.
    con.time_advance_request(0.5).await.unwrap();
    assert!(
        wait_for(Duration::from_millis(500), || {
            !r2.grants.lock().is_empty()
        })
        .await
    );
    assert_eq!(r2.grants.lock()[0], 0.5);

    reg.disconnect().await.unwrap();
    con.disconnect().await.unwrap();
}
