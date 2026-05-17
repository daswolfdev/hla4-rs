//! Kitchen-sink integration test: exercises every major service category
//! through the federate library against an in-process RTI.
//!
//! The point is high-signal validation that the system holds together
//! end-to-end — if this passes, the major code paths integrate correctly.
//! It does not exhaustively test edge cases (each category has dedicated
//! tests for that).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use hla_core::{
    AttributeHandle, AttributeHandleSet, AttributeHandleValueMap, FederateHandle,
    InteractionClassHandle, ObjectClassHandle, ObjectInstanceHandle, ParameterHandleValueMap,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>Kitchen</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Sensor</name>
        <attribute>
          <name>Temp</name>
          <dataType>HLAfloat64BE</dataType>
          <updateType>Static</updateType>
          <ownership>DivestAcquire</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
  <interactions>
    <interactionClass><name>HLAinteractionRoot</name>
      <transportation>HLAreliable</transportation><order>Receive</order>
      <interactionClass><name>Alarm</name>
        <transportation>HLAreliable</transportation><order>Receive</order>
        <parameter><name>Level</name><dataType>HLAinteger32BE</dataType></parameter>
      </interactionClass>
    </interactionClass>
  </interactions>
</objectModel>"#;

async fn boot() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(FOM).unwrap()]).unwrap());
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    addr
}

#[derive(Default)]
struct Recorder {
    discoveries: AtomicU32,
    reflects: AtomicU32,
    interactions: AtomicU32,
    time_regulation_enabled: AtomicBool,
    time_constrained_enabled: AtomicBool,
    grants: Mutex<Vec<f64>>,
    sync_announced: AtomicBool,
    sync_completed: AtomicBool,
    saved: AtomicBool,
    ownership_acquired: AtomicBool,
    ownership_unavailable: AtomicBool,
}

impl FederateAmbassador for Recorder {
    async fn discover_object_instance(
        &self,
        _i: ObjectInstanceHandle,
        _c: ObjectClassHandle,
        _n: String,
        _p: Option<FederateHandle>,
    ) {
        self.discoveries.fetch_add(1, Ordering::Relaxed);
    }
    async fn reflect_attribute_values(
        &self,
        _i: ObjectInstanceHandle,
        _v: AttributeHandleValueMap,
        _t: Vec<u8>,
        _p: Option<FederateHandle>,
    ) {
        self.reflects.fetch_add(1, Ordering::Relaxed);
    }
    async fn receive_interaction(
        &self,
        _c: InteractionClassHandle,
        _p: ParameterHandleValueMap,
        _t: Vec<u8>,
        _f: Option<FederateHandle>,
    ) {
        self.interactions.fetch_add(1, Ordering::Relaxed);
    }
    async fn time_regulation_enabled(&self, _time: f64) {
        self.time_regulation_enabled.store(true, Ordering::Relaxed);
    }
    async fn time_constrained_enabled(&self, _time: f64) {
        self.time_constrained_enabled.store(true, Ordering::Relaxed);
    }
    async fn time_advance_grant(&self, time: f64) {
        self.grants.lock().push(time);
    }
    async fn announce_synchronization_point(&self, _label: String, _tag: Vec<u8>) {
        self.sync_announced.store(true, Ordering::Relaxed);
    }
    async fn federation_synchronized(&self, _label: String, _failed: HashSet<FederateHandle>) {
        self.sync_completed.store(true, Ordering::Relaxed);
    }
    async fn federation_saved(&self) {
        self.saved.store(true, Ordering::Relaxed);
    }
    async fn attribute_ownership_acquisition_notification(
        &self,
        _i: ObjectInstanceHandle,
        _a: Vec<AttributeHandle>,
        _t: Vec<u8>,
    ) {
        self.ownership_acquired.store(true, Ordering::Relaxed);
    }
    async fn attribute_ownership_unavailable(
        &self,
        _i: ObjectInstanceHandle,
        _a: Vec<AttributeHandle>,
        _t: Vec<u8>,
    ) {
        self.ownership_unavailable.store(true, Ordering::Relaxed);
    }
}

async fn wait_for<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    f()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn full_lifecycle_kitchen_sink() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // ---- Two federates: A (publisher + regulator), B (subscriber + constrained) ----
    let rec_a = Arc::new(Recorder::default());
    let rec_b = Arc::new(Recorder::default());
    let a = RtiAmbassador::connect(&url, Arc::clone(&rec_a))
        .await
        .unwrap();
    let b = RtiAmbassador::connect(&url, Arc::clone(&rec_b))
        .await
        .unwrap();

    a.create_federation_execution("kitchen").await.ok();
    a.join_federation_execution("Producer", "kitchen")
        .await
        .unwrap();
    b.join_federation_execution("Consumer", "kitchen")
        .await
        .unwrap();

    // ---- Handle lookups ----
    let sensor = a
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let temp = a.get_attribute_handle(sensor, "Temp").await.unwrap();
    let alarm = a
        .get_interaction_class_handle("HLAinteractionRoot.Alarm")
        .await
        .unwrap();
    let level = a.get_parameter_handle(alarm, "Level").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(temp);

    // ---- Time Management ----
    a.enable_time_regulation(1.0).await.unwrap();
    b.enable_time_constrained().await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_a.time_regulation_enabled.load(Ordering::Relaxed)
                && rec_b.time_constrained_enabled.load(Ordering::Relaxed)
        })
        .await
    );

    // ---- Pub/Sub + register + update ----
    a.publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    a.publish_interaction_class(alarm).await.unwrap();
    b.subscribe_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    b.subscribe_interaction_class(alarm).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await; // settle sub

    let instance = a.register_object_instance(sensor).await.unwrap();
    let mut values = AttributeHandleValueMap::new();
    values.insert(temp, 21.5f64.to_be_bytes().to_vec());
    a.update_attribute_values(instance, values, b"reading-1")
        .await
        .unwrap();

    let mut params = ParameterHandleValueMap::new();
    params.insert(level, 3i32.to_be_bytes().to_vec());
    a.send_interaction(alarm, params, b"alarm-1").await.unwrap();

    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_b.discoveries.load(Ordering::Relaxed) >= 1
                && rec_b.reflects.load(Ordering::Relaxed) >= 1
                && rec_b.interactions.load(Ordering::Relaxed) >= 1
        })
        .await
    );

    // ---- Sync point ----
    a.register_federation_synchronization_point("RoundStart", b"")
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_b.sync_announced.load(Ordering::Relaxed)
        })
        .await
    );
    a.synchronization_point_achieved("RoundStart", true)
        .await
        .unwrap();
    b.synchronization_point_achieved("RoundStart", true)
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_a.sync_completed.load(Ordering::Relaxed)
                && rec_b.sync_completed.load(Ordering::Relaxed)
        })
        .await
    );

    // ---- Federation save ----
    a.request_federation_save("snap").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    a.federate_save_begun().await.unwrap();
    b.federate_save_begun().await.unwrap();
    a.federate_save_complete().await.unwrap();
    b.federate_save_complete().await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_a.saved.load(Ordering::Relaxed) && rec_b.saved.load(Ordering::Relaxed)
        })
        .await
    );

    // ---- Ownership: A divests, B acquires-if-available ----
    a.unconditional_attribute_ownership_divestiture(instance, attrs.clone(), b"")
        .await
        .unwrap();
    b.attribute_ownership_acquisition_if_available(instance, attrs.clone(), b"")
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_b.ownership_acquired.load(Ordering::Relaxed)
        })
        .await
    );
    assert!(
        b.is_attribute_owned_by_federate(instance, temp)
            .await
            .unwrap()
    );

    // ---- Clean resign + disconnect ----
    a.resign_federation_execution(hla_core::ResignAction::DeleteObjects)
        .await
        .unwrap();
    b.resign_federation_execution(hla_core::ResignAction::DeleteObjects)
        .await
        .unwrap();
    a.disconnect().await.unwrap();
    b.disconnect().await.unwrap();
}
