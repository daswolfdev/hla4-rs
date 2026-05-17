//! Federation save orchestration (MVP: callback flow only, no on-disk state).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const TRIVIAL: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>S</name></modelIdentification>
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
struct SaveRec {
    initiate_count: AtomicU32,
    initiate_label: Mutex<Option<String>>,
    saved: AtomicBool,
    not_saved: AtomicU32,
}

impl FederateAmbassador for SaveRec {
    async fn initiate_federate_save(&self, label: String) {
        self.initiate_count.fetch_add(1, Ordering::Relaxed);
        *self.initiate_label.lock() = Some(label);
    }
    async fn federation_saved(&self) {
        self.saved.store(true, Ordering::Relaxed);
    }
    async fn federation_not_saved(&self, _reason: i32) {
        self.not_saved.fetch_add(1, Ordering::Relaxed);
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
async fn save_completes_when_all_federates_report_done() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let r1 = Arc::new(SaveRec::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("save-fed").await.ok();
    f1.join_federation_execution("F1", "save-fed").await.unwrap();

    let r2 = Arc::new(SaveRec::default());
    let f2 = RtiAmbassador::connect(&url, Arc::clone(&r2)).await.unwrap();
    f2.join_federation_execution("F2", "save-fed").await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    f1.request_federation_save("snapshot-1").await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || {
        r1.initiate_count.load(Ordering::Relaxed) >= 1
            && r2.initiate_count.load(Ordering::Relaxed) >= 1
    })
    .await);
    assert_eq!(*r1.initiate_label.lock(), Some("snapshot-1".into()));

    f1.federate_save_begun().await.unwrap();
    f2.federate_save_begun().await.unwrap();
    f1.federate_save_complete().await.unwrap();
    // Not done yet — F2 hasn't reported.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!r1.saved.load(Ordering::Relaxed));

    f2.federate_save_complete().await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || {
        r1.saved.load(Ordering::Relaxed) && r2.saved.load(Ordering::Relaxed)
    })
    .await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn save_reports_not_saved_when_any_federate_fails() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let r1 = Arc::new(SaveRec::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("fail-fed").await.ok();
    f1.join_federation_execution("F1", "fail-fed").await.unwrap();

    let r2 = Arc::new(SaveRec::default());
    let f2 = RtiAmbassador::connect(&url, Arc::clone(&r2)).await.unwrap();
    f2.join_federation_execution("F2", "fail-fed").await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    f1.request_federation_save("attempt").await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || {
        r1.initiate_count.load(Ordering::Relaxed) >= 1
            && r2.initiate_count.load(Ordering::Relaxed) >= 1
    })
    .await);

    f1.federate_save_begun().await.unwrap();
    f2.federate_save_begun().await.unwrap();
    f1.federate_save_not_complete().await.unwrap();
    f2.federate_save_complete().await.unwrap();

    assert!(wait_for(Duration::from_secs(1), || {
        r1.not_saved.load(Ordering::Relaxed) >= 1 && r2.not_saved.load(Ordering::Relaxed) >= 1
    })
    .await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_save_while_first_in_progress_returns_save_in_progress() {
    let addr = boot().await;
    let url = format!("rti://{addr}");
    let r1 = Arc::new(SaveRec::default());
    let f1 = RtiAmbassador::connect(&url, Arc::clone(&r1)).await.unwrap();
    f1.create_federation_execution("dup-save").await.ok();
    f1.join_federation_execution("F1", "dup-save").await.unwrap();

    f1.request_federation_save("first").await.unwrap();
    let err = f1.request_federation_save("second").await.unwrap_err();
    match err {
        hla_federate::CallError::RtiException { name, .. } => {
            assert_eq!(name, "SaveInProgress");
        }
        other => panic!("expected SaveInProgress, got {other:?}"),
    }
}
