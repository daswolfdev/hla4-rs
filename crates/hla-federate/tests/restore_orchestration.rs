//! Federation restore orchestration (callback flow only, no on-disk state).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use hla_core::FederateHandle;
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const TRIVIAL: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>R</name></modelIdentification>
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
    tokio::time::sleep(Duration::from_millis(30)).await;
    addr
}

#[derive(Default)]
struct Rec {
    succeeded: Mutex<Vec<String>>,
    begun: AtomicBool,
    initiate: Mutex<Vec<(String, String)>>,
    restored: AtomicBool,
    not_restored: AtomicU32,
}

impl FederateAmbassador for Rec {
    async fn request_federation_restore_succeeded(&self, label: String) {
        self.succeeded.lock().push(label);
    }
    async fn federation_restore_begun(&self) {
        self.begun.store(true, Ordering::Relaxed);
    }
    async fn initiate_federate_restore(
        &self,
        label: String,
        federate_name: String,
        _post: FederateHandle,
    ) {
        self.initiate.lock().push((label, federate_name));
    }
    async fn federation_restored(&self) {
        self.restored.store(true, Ordering::Relaxed);
    }
    async fn federation_not_restored(&self, _reason: i32) {
        self.not_restored.fetch_add(1, Ordering::Relaxed);
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
async fn restore_completes_when_all_done() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let r1 = Arc::new(Rec::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("rest-fed").await.ok();
    f1.join_federation_execution("F1", "rest-fed")
        .await
        .unwrap();

    let r2 = Arc::new(Rec::default());
    let f2 = RtiAmbassador::connect(&url, Arc::clone(&r2)).await.unwrap();
    f2.join_federation_execution("F2", "rest-fed")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    f1.request_federation_restore("snap-1").await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            !r1.succeeded.lock().is_empty()
                && r1.begun.load(Ordering::Relaxed)
                && r2.begun.load(Ordering::Relaxed)
                && !r1.initiate.lock().is_empty()
                && !r2.initiate.lock().is_empty()
        })
        .await
    );

    f1.federate_restore_complete().await.unwrap();
    f2.federate_restore_complete().await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            r1.restored.load(Ordering::Relaxed) && r2.restored.load(Ordering::Relaxed)
        })
        .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_reports_not_restored_on_any_failure() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let r1 = Arc::new(Rec::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("rest-fail").await.ok();
    f1.join_federation_execution("F1", "rest-fail")
        .await
        .unwrap();

    let r2 = Arc::new(Rec::default());
    let f2 = RtiAmbassador::connect(&url, Arc::clone(&r2)).await.unwrap();
    f2.join_federation_execution("F2", "rest-fail")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    f1.request_federation_restore("attempt").await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            !r1.succeeded.lock().is_empty()
        })
        .await
    );

    f1.federate_restore_not_complete().await.unwrap();
    f2.federate_restore_complete().await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            r1.not_restored.load(Ordering::Relaxed) >= 1
                && r2.not_restored.load(Ordering::Relaxed) >= 1
        })
        .await
    );
}
