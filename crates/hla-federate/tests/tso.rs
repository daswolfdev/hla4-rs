//! TSO message delivery: WithTime messages destined for a constrained
//! federate are held until that federate's logical time advances past
//! the message timestamp.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, InteractionClassHandle,
    ObjectInstanceHandle, ParameterHandleValueMap,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>TSO</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Ping</name>
        <attribute>
          <name>Seq</name>
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
  <interactions><interactionClass><name>HLAinteractionRoot</name>
    <transportation>HLAreliable</transportation><order>Receive</order>
  </interactionClass></interactions>
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
struct TsoRec {
    immediate_reflects: AtomicU32,
    timed_reflects: Mutex<Vec<f64>>,
    grants: Mutex<Vec<f64>>,
}

impl FederateAmbassador for TsoRec {
    async fn reflect_attribute_values(
        &self,
        _instance: ObjectInstanceHandle,
        _values: AttributeHandleValueMap,
        _tag: Vec<u8>,
        _producer: Option<FederateHandle>,
    ) {
        self.immediate_reflects.fetch_add(1, Ordering::Relaxed);
    }
    async fn reflect_attribute_values_with_time(
        &self,
        _instance: ObjectInstanceHandle,
        _values: AttributeHandleValueMap,
        _tag: Vec<u8>,
        _producer: Option<FederateHandle>,
        time: f64,
    ) {
        self.timed_reflects.lock().push(time);
    }
    async fn time_advance_grant(&self, time: f64) {
        self.grants.lock().push(time);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tso_messages_held_until_constrained_federate_advances() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Subscriber B is constrained, current_time = 0.
    let rec_b = Arc::new(TsoRec::default());
    let b = RtiAmbassador::connect(&url, Arc::clone(&rec_b))
        .await
        .unwrap();
    b.create_federation_execution("tso").await.ok();
    b.join_federation_execution("B", "tso").await.unwrap();
    let class = b
        .get_object_class_handle("HLAobjectRoot.Ping")
        .await
        .unwrap();
    let attr = b.get_attribute_handle(class, "Seq").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(attr);
    b.subscribe_object_class_attributes(class, attrs.clone())
        .await
        .unwrap();
    b.enable_time_constrained().await.unwrap();
    // Wait for TimeConstrainedEnabled callback to land.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publisher A is regulating with lookahead=1.0.
    let rec_a = Arc::new(TsoRec::default());
    let a = RtiAmbassador::connect(&url, Arc::clone(&rec_a))
        .await
        .unwrap();
    a.join_federation_execution("A", "tso").await.unwrap();
    a.publish_object_class_attributes(class, attrs)
        .await
        .unwrap();
    a.enable_time_regulation(1.0).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let instance = a.register_object_instance(class).await.unwrap();

    // A sends a TSO update at time=2.0.
    let mut values = AttributeHandleValueMap::new();
    values.insert(attr, 42i32.to_be_bytes().to_vec());
    a.call_with_time_update(instance, attr, 42, 2.0).await;

    // B is constrained, current_time=0, so the update with time=2.0 should
    // be QUEUED, not delivered.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        rec_b.timed_reflects.lock().len(),
        0,
        "TSO message should be held until grant"
    );

    // B requests advance to 2.0. A's LBTS is 0+1=1.0; A must advance for
    // B's TAR to be grantable. First A advances.
    a.time_advance_request(5.0).await.unwrap(); // A unconstrained → immediate grant
    // Wait for A's TAG.
    assert!(wait_for(Duration::from_secs(1), || !rec_a.grants.lock().is_empty()).await);

    // Now A's LBTS is 5+1=6, so B can advance to 2.0.
    b.time_advance_request(2.0).await.unwrap();

    // B should receive the queued reflect THEN the grant.
    assert!(
        wait_for(Duration::from_secs(1), || {
            !rec_b.timed_reflects.lock().is_empty() && !rec_b.grants.lock().is_empty()
        })
        .await
    );
    let reflects = rec_b.timed_reflects.lock();
    assert_eq!(reflects.len(), 1);
    assert_eq!(reflects[0], 2.0);
    assert_eq!(rec_b.grants.lock()[0], 2.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tso_unconstrained_federate_gets_immediate_delivery() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Subscriber is unconstrained.
    let rec_b = Arc::new(TsoRec::default());
    let b = RtiAmbassador::connect(&url, Arc::clone(&rec_b))
        .await
        .unwrap();
    b.create_federation_execution("tso2").await.ok();
    b.join_federation_execution("B", "tso2").await.unwrap();
    let class = b
        .get_object_class_handle("HLAobjectRoot.Ping")
        .await
        .unwrap();
    let attr = b.get_attribute_handle(class, "Seq").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(attr);
    b.subscribe_object_class_attributes(class, attrs.clone())
        .await
        .unwrap();

    let rec_a = Arc::new(TsoRec::default());
    let a = RtiAmbassador::connect(&url, Arc::clone(&rec_a))
        .await
        .unwrap();
    a.join_federation_execution("A", "tso2").await.unwrap();
    a.publish_object_class_attributes(class, attrs)
        .await
        .unwrap();
    a.enable_time_regulation(1.0).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let instance = a.register_object_instance(class).await.unwrap();
    a.call_with_time_update(instance, attr, 99, 5.0).await;

    // B is unconstrained — receives immediately.
    assert!(
        wait_for(Duration::from_secs(1), || {
            !rec_b.timed_reflects.lock().is_empty()
        })
        .await
    );
}

// Helper trait for sending WithTime updates (the federate library doesn't
// have a typed wrapper yet).
trait TsoExt {
    async fn call_with_time_update(
        &self,
        instance: ObjectInstanceHandle,
        attr: hla_core::AttributeHandle,
        value: i32,
        time: f64,
    );
}

impl TsoExt for RtiAmbassador {
    async fn call_with_time_update(
        &self,
        instance: ObjectInstanceHandle,
        attr: hla_core::AttributeHandle,
        value: i32,
        time: f64,
    ) {
        // Build the WithTime request via the raw call path. The federate
        // library doesn't yet expose a typed update_attribute_values_with_time.
        use hla_fedpro_proto::fedpro;
        let _ = self
            .raw_call_for_test(
                fedpro::call_request::CallRequest::UpdateAttributeValuesWithTimeRequest(
                    fedpro::UpdateAttributeValuesWithTimeRequest {
                        object_instance: Some(fedpro::ObjectInstanceHandle {
                            data: instance.raw().to_be_bytes().to_vec(),
                        }),
                        attribute_values: Some(fedpro::AttributeHandleValueMap {
                            attribute_handle_value: vec![fedpro::AttributeHandleValue {
                                attribute_handle: Some(fedpro::AttributeHandle {
                                    data: attr.raw().to_be_bytes().to_vec(),
                                }),
                                value: value.to_be_bytes().to_vec(),
                            }],
                        }),
                        user_supplied_tag: b"tso".to_vec(),
                        time: Some(fedpro::LogicalTime {
                            data: time.to_be_bytes().to_vec(),
                        }),
                    },
                ),
            )
            .await;
        // suppress unused-warn
        let _ = HashSet::<FederateHandle>::new();
        let _ = ParameterHandleValueMap::new();
        let _ = InteractionClassHandle::new(0);
    }
}
