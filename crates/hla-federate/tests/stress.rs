//! Concurrent stress test: N federates each register one object and send M
//! updates. Each update fans out to (N-1) other subscribers, so we expect
//! N * (N-1) * M total reflect callbacks. Verifies the routing layer doesn't
//! drop messages under concurrent load.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, ObjectClassHandle,
    ObjectInstanceHandle,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>Stress</name></modelIdentification>
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
</objectModel>"#;

const N_FEDERATES: usize = 8;
const M_UPDATES: usize = 50;

#[derive(Default)]
struct Counter {
    reflects: AtomicU32,
    discoveries: AtomicU32,
}

impl FederateAmbassador for Counter {
    async fn discover_object_instance(
        &self,
        _instance: ObjectInstanceHandle,
        _class: ObjectClassHandle,
        _name: String,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.discoveries.fetch_add(1, Ordering::Relaxed);
    }
    async fn reflect_attribute_values(
        &self,
        _instance: ObjectInstanceHandle,
        _values: AttributeHandleValueMap,
        _tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.reflects.fetch_add(1, Ordering::Relaxed);
    }
}

async fn boot() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(FOM).unwrap()]).unwrap());
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn n_federates_no_callbacks_lost() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Connect, create federation once, join everyone.
    let mut federates: Vec<(Arc<RtiAmbassador>, Arc<Counter>)> = Vec::new();
    for i in 0..N_FEDERATES {
        let counter = Arc::new(Counter::default());
        let amb = Arc::new(
            RtiAmbassador::connect(&url, Arc::clone(&counter))
                .await
                .unwrap(),
        );
        if i == 0 {
            amb.create_federation_execution("stress").await.unwrap();
        }
        amb.join_federation_execution(&format!("F{i}"), "stress")
            .await
            .unwrap();
        federates.push((amb, counter));
    }

    // Resolve handles + publish/subscribe everyone.
    let class = federates[0]
        .0
        .get_object_class_handle("HLAobjectRoot.Ping")
        .await
        .unwrap();
    let attr = federates[0]
        .0
        .get_attribute_handle(class, "Seq")
        .await
        .unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(attr);
    for (amb, _) in &federates {
        amb.subscribe_object_class_attributes(class, attrs.clone())
            .await
            .unwrap();
        amb.publish_object_class_attributes(class, attrs.clone())
            .await
            .unwrap();
    }

    // Each federate registers one instance.
    let mut instances = Vec::new();
    for (amb, _) in &federates {
        instances.push(amb.register_object_instance(class).await.unwrap());
    }

    // Settle: every federate should see (N-1) discoveries.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let min = federates
            .iter()
            .map(|(_, c)| c.discoveries.load(Ordering::Relaxed))
            .min()
            .unwrap();
        if min as usize >= N_FEDERATES - 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for (i, (_, c)) in federates.iter().enumerate() {
        assert_eq!(
            c.discoveries.load(Ordering::Relaxed) as usize,
            N_FEDERATES - 1,
            "federate {i} expected {} discoveries",
            N_FEDERATES - 1
        );
    }

    // Concurrently fire M updates from each federate.
    let start = Instant::now();
    let mut handles = Vec::new();
    for (i, (amb, _)) in federates.iter().cloned().enumerate() {
        let instance = instances[i];
        handles.push(tokio::spawn(async move {
            for k in 0..M_UPDATES {
                let mut values = AttributeHandleValueMap::new();
                values.insert(attr, (k as i32).to_be_bytes().to_vec());
                amb.update_attribute_values(instance, values, &[])
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let send_elapsed = start.elapsed();

    // Wait for all reflects to arrive.
    let expected_per_federate = (N_FEDERATES - 1) * M_UPDATES;
    let expected_total = N_FEDERATES * expected_per_federate;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let total: u32 = federates
            .iter()
            .map(|(_, c)| c.reflects.load(Ordering::Relaxed))
            .sum();
        if total as usize >= expected_total {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let recv_elapsed = start.elapsed();

    for (i, (_, c)) in federates.iter().enumerate() {
        let got = c.reflects.load(Ordering::Relaxed) as usize;
        assert_eq!(
            got, expected_per_federate,
            "federate {i} reflects: expected {expected_per_federate}, got {got}"
        );
    }

    let total = expected_total as f64;
    let throughput = total / recv_elapsed.as_secs_f64();
    eprintln!(
        "stress: {N_FEDERATES}f × {M_UPDATES}u → {expected_total} reflects, \
         send_elapsed={:?} recv_elapsed={:?} throughput={:.0} cb/s",
        send_elapsed, recv_elapsed, throughput
    );
    // Sanity: must do at least 1000 cb/s — a very loose bound to catch
    // pathological regressions, not a real perf target.
    assert!(
        throughput > 1000.0,
        "throughput {throughput:.0} cb/s below sanity floor"
    );
}
