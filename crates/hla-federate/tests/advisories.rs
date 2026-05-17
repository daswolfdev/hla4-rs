//! Advisory callbacks: StartRegistrationForObjectClass / TurnInteractionsOn
//! fire when a publisher's first subscriber appears, and the Stop/Off
//! variants fire when the last subscriber leaves.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hla_core::{AttributeHandleSet, InteractionClassHandle, ObjectClassHandle};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>Adv</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Thing</name>
        <attribute>
          <name>X</name>
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
  <interactions>
    <interactionClass><name>HLAinteractionRoot</name>
      <transportation>HLAreliable</transportation><order>Receive</order>
      <interactionClass><name>Beep</name>
        <transportation>HLAreliable</transportation><order>Receive</order>
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
    tokio::time::sleep(Duration::from_millis(30)).await;
    addr
}

#[derive(Default)]
struct AdvRec {
    start: AtomicU32,
    stop: AtomicU32,
    interactions_on: AtomicU32,
    interactions_off: AtomicU32,
}

impl FederateAmbassador for AdvRec {
    async fn start_registration_for_object_class(&self, _class: ObjectClassHandle) {
        self.start.fetch_add(1, Ordering::Relaxed);
    }
    async fn stop_registration_for_object_class(&self, _class: ObjectClassHandle) {
        self.stop.fetch_add(1, Ordering::Relaxed);
    }
    async fn turn_interactions_on(&self, _class: InteractionClassHandle) {
        self.interactions_on.fetch_add(1, Ordering::Relaxed);
    }
    async fn turn_interactions_off(&self, _class: InteractionClassHandle) {
        self.interactions_off.fetch_add(1, Ordering::Relaxed);
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
async fn start_registration_fires_on_first_subscriber() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let pub_rec = Arc::new(AdvRec::default());
    let publisher = RtiAmbassador::connect(&url, Arc::clone(&pub_rec)).await.unwrap();
    publisher.create_federation_execution("adv").await.ok();
    publisher.join_federation_execution("P", "adv").await.unwrap();
    let class = publisher.get_object_class_handle("HLAobjectRoot.Thing").await.unwrap();
    let attr = publisher.get_attribute_handle(class, "X").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(attr);
    publisher.publish_object_class_attributes(class, attrs.clone()).await.unwrap();

    // No subscriber yet — no advisory.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(pub_rec.start.load(Ordering::Relaxed), 0);

    // First subscriber appears — publisher should receive Start.
    let sub = RtiAmbassador::connect(&url, Arc::new(AdvRec::default())).await.unwrap();
    sub.join_federation_execution("S", "adv").await.unwrap();
    sub.subscribe_object_class_attributes(class, attrs.clone()).await.unwrap();

    assert!(wait_for(Duration::from_secs(1), || {
        pub_rec.start.load(Ordering::Relaxed) >= 1
    })
    .await);
    assert_eq!(pub_rec.stop.load(Ordering::Relaxed), 0);

    // Last subscriber leaves — publisher should receive Stop.
    sub.unsubscribe_object_class_attributes(class, attrs).await.unwrap();
    assert!(wait_for(Duration::from_secs(1), || {
        pub_rec.stop.load(Ordering::Relaxed) >= 1
    })
    .await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turn_interactions_on_fires_on_first_subscriber() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let pub_rec = Arc::new(AdvRec::default());
    let publisher = RtiAmbassador::connect(&url, Arc::clone(&pub_rec)).await.unwrap();
    publisher.create_federation_execution("adv-ix").await.ok();
    publisher.join_federation_execution("P", "adv-ix").await.unwrap();
    let class = publisher.get_interaction_class_handle("HLAinteractionRoot.Beep").await.unwrap();
    publisher.publish_interaction_class(class).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(pub_rec.interactions_on.load(Ordering::Relaxed), 0);

    let sub = RtiAmbassador::connect(&url, Arc::new(AdvRec::default())).await.unwrap();
    sub.join_federation_execution("S", "adv-ix").await.unwrap();
    sub.subscribe_interaction_class(class).await.unwrap();

    assert!(wait_for(Duration::from_secs(1), || {
        pub_rec.interactions_on.load(Ordering::Relaxed) >= 1
    })
    .await);
}
