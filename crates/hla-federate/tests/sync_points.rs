//! End-to-end synchronization point flow.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::FederateHandle;
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const TRIVIAL: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>T</name></modelIdentification>
  <objects><objectClass><name>HLAobjectRoot</name></objectClass></objects>
  <interactions><interactionClass><name>HLAinteractionRoot</name>
    <transportation>HLAreliable</transportation><order>Receive</order>
  </interactionClass></interactions>
</objectModel>"#;

async fn boot() -> SocketAddr {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (node, listener) = RtiNode::bind(bind).await.unwrap();
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(TRIVIAL).unwrap()]).unwrap());
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    addr
}

#[derive(Default)]
struct SyncRecorder {
    registration_succeeded: Mutex<Vec<String>>,
    registration_failed: Mutex<Vec<(String, i32)>>,
    announces: Mutex<Vec<(String, Vec<u8>)>>,
    synchronized: Mutex<Vec<(String, HashSet<FederateHandle>)>>,
}

impl FederateAmbassador for SyncRecorder {
    async fn synchronization_point_registration_succeeded(&self, label: String) {
        self.registration_succeeded.lock().push(label);
    }
    async fn synchronization_point_registration_failed(&self, label: String, reason: i32) {
        self.registration_failed.lock().push((label, reason));
    }
    async fn announce_synchronization_point(&self, label: String, tag: Vec<u8>) {
        self.announces.lock().push((label, tag));
    }
    async fn federation_synchronized(
        &self,
        label: String,
        failed_to_sync: HashSet<FederateHandle>,
    ) {
        self.synchronized.lock().push((label, failed_to_sync));
    }
}

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
async fn full_sync_point_flow() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Both federates join and observe the sync.
    let r1 = Arc::new(SyncRecorder::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("sp-fed").await.ok();
    f1.join_federation_execution("F1", "sp-fed").await.unwrap();

    let r2 = Arc::new(SyncRecorder::default());
    let f2 = RtiAmbassador::connect(&url, Arc::clone(&r2)).await.unwrap();
    f2.join_federation_execution("F2", "sp-fed").await.unwrap();

    // Small settle so both joins are registered before the sync.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // F1 registers the sync point.
    f1.register_federation_synchronization_point("ReadyToRun", b"go")
        .await
        .unwrap();

    // F1 should see Succeeded. Both should see Announce.
    assert!(
        wait_for(Duration::from_secs(1), || {
            r1.registration_succeeded.lock().len() == 1
                && r1.announces.lock().len() == 1
                && r2.announces.lock().len() == 1
        })
        .await
    );

    // F1 achieves; nothing happens yet (still waiting for F2).
    f1.synchronization_point_achieved("ReadyToRun", true)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(r1.synchronized.lock().is_empty());
    assert!(r2.synchronized.lock().is_empty());

    // F2 achieves → both get FederationSynchronized.
    f2.synchronization_point_achieved("ReadyToRun", true)
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            !r1.synchronized.lock().is_empty() && !r2.synchronized.lock().is_empty()
        })
        .await
    );
    assert_eq!(r1.synchronized.lock()[0].0, "ReadyToRun");
    assert!(
        r1.synchronized.lock()[0].1.is_empty(),
        "no failed federates"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_sync_point_label_fails() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let r = Arc::new(SyncRecorder::default());
    let f = RtiAmbassador::connect(&url, Arc::clone(&r)).await.unwrap();
    f.create_federation_execution("sp-dup").await.ok();
    f.join_federation_execution("F", "sp-dup").await.unwrap();

    f.register_federation_synchronization_point("Once", b"")
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            r.registration_succeeded.lock().len() == 1
        })
        .await
    );

    f.register_federation_synchronization_point("Once", b"")
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            r.registration_failed.lock().len() == 1
        })
        .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_to_sync_set_propagates() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let r1 = Arc::new(SyncRecorder::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("sp-fail").await.ok();
    let _ = f1.join_federation_execution("F1", "sp-fail").await.unwrap();

    let r2 = Arc::new(SyncRecorder::default());
    let f2 = RtiAmbassador::connect(&url, Arc::clone(&r2)).await.unwrap();
    let _ = f2.join_federation_execution("F2", "sp-fail").await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    f1.register_federation_synchronization_point("Maybe", b"")
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            r1.announces.lock().len() == 1 && r2.announces.lock().len() == 1
        })
        .await
    );

    // F1 fails to achieve, F2 achieves.
    f1.synchronization_point_achieved("Maybe", false)
        .await
        .unwrap();
    f2.synchronization_point_achieved("Maybe", true)
        .await
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            !r1.synchronized.lock().is_empty() && !r2.synchronized.lock().is_empty()
        })
        .await
    );
    assert_eq!(r1.synchronized.lock()[0].1.len(), 1, "one federate failed");
}
