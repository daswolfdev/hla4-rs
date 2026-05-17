//! Federation discovery callbacks: `reportFederationExecutions`,
//! `reportFederationExecutionMembers`, `reportFederationExecutionDoesNotExist`.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const TRIVIAL: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>D</name></modelIdentification>
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
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

type FederationMembersSnapshot = Vec<(String, Vec<(String, String)>)>;

#[derive(Default)]
struct DiscoveryRec {
    list: Mutex<Vec<Vec<String>>>,
    members: Mutex<FederationMembersSnapshot>,
    does_not_exist: AtomicU32,
}

impl FederateAmbassador for DiscoveryRec {
    async fn report_federation_executions(&self, federations: Vec<String>) {
        self.list.lock().push(federations);
    }
    async fn report_federation_execution_members(
        &self,
        federation_name: String,
        members: Vec<(String, String)>,
    ) {
        self.members.lock().push((federation_name, members));
    }
    async fn report_federation_execution_does_not_exist(&self, _federation_name: String) {
        self.does_not_exist.fetch_add(1, Ordering::Relaxed);
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
async fn list_federation_executions_callback_carries_names() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec = Arc::new(DiscoveryRec::default());
    let amb = RtiAmbassador::connect(&url, Arc::clone(&rec))
        .await
        .unwrap();
    amb.create_federation_execution("fed-a").await.unwrap();
    amb.create_federation_execution("fed-b").await.unwrap();

    amb.list_federation_executions().await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || !rec.list.lock().is_empty()).await);
    let lists = rec.list.lock();
    let names: HashSet<&str> = lists[0].iter().map(String::as_str).collect();
    assert!(names.contains("fed-a"), "got {names:?}");
    assert!(names.contains("fed-b"), "got {names:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_federation_execution_members_carries_federates() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Two federates join the same federation.
    let rec_a = Arc::new(DiscoveryRec::default());
    let a = RtiAmbassador::connect(&url, Arc::clone(&rec_a))
        .await
        .unwrap();
    a.create_federation_execution("mfed").await.unwrap();
    a.join_federation_execution("Producer", "mfed")
        .await
        .unwrap();

    let rec_b = Arc::new(DiscoveryRec::default());
    let b = RtiAmbassador::connect(&url, Arc::clone(&rec_b))
        .await
        .unwrap();
    b.join_federation_execution("Consumer", "mfed")
        .await
        .unwrap();

    // Settle so the federates table is observed in both joins.
    tokio::time::sleep(Duration::from_millis(50)).await;

    a.list_federation_execution_members("mfed").await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || !rec_a.members.lock().is_empty()).await);
    let m = rec_a.members.lock();
    assert_eq!(m[0].0, "mfed");
    assert_eq!(m[0].1.len(), 2, "expected 2 members, got {:?}", m[0].1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_members_for_nonexistent_federation_delivers_does_not_exist() {
    let addr = boot().await;
    let url = format!("rti://{addr}");
    let rec = Arc::new(DiscoveryRec::default());
    let amb = RtiAmbassador::connect(&url, Arc::clone(&rec))
        .await
        .unwrap();
    amb.list_federation_execution_members("nope").await.unwrap();
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec.does_not_exist.load(Ordering::Relaxed) >= 1
        })
        .await
    );
}
