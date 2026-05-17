//! Ownership Management MVP slice tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::{
    AttributeHandle, AttributeHandleSet, AttributeHandleValueMap, FederateHandle,
    ObjectInstanceHandle,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>Own</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Sensor</name>
        <attribute>
          <name>Reading</name>
          <dataType>HLAinteger32BE</dataType>
          <updateType>Static</updateType>
          <ownership>DivestAcquire</ownership>
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
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(FOM).unwrap()]).unwrap());
    let addr = node.bind_addr;
    tokio::spawn(async move {
        let _ = node.serve(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

#[derive(Default)]
struct OwnerRec {
    informs: Mutex<Vec<(ObjectInstanceHandle, Vec<AttributeHandle>, FederateHandle)>>,
    not_owned: Mutex<Vec<(ObjectInstanceHandle, Vec<AttributeHandle>)>>,
}

impl FederateAmbassador for OwnerRec {
    async fn inform_attribute_ownership(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
        owner: FederateHandle,
    ) {
        self.informs.lock().push((instance, attributes, owner));
    }
    async fn attribute_is_not_owned(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) {
        self.not_owned.lock().push((instance, attributes));
    }
}

async fn join_setup(
    url: &str,
    fed_name: &str,
    fed_type: &str,
    create: bool,
    recorder: Arc<OwnerRec>,
) -> RtiAmbassador {
    join_setup_generic(url, fed_name, fed_type, create, recorder).await
}

async fn join_setup_generic<A: hla_federate::FederateAmbassador + 'static>(
    url: &str,
    _fed_name: &str,
    fed_type: &str,
    create: bool,
    callbacks: A,
) -> RtiAmbassador {
    let amb = RtiAmbassador::connect(url, callbacks).await.unwrap();
    if create {
        amb.create_federation_execution("own-fed").await.ok();
    }
    amb.join_federation_execution(fed_type, "own-fed")
        .await
        .unwrap();
    amb
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registrar_owns_published_attributes() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec_pub = Arc::new(OwnerRec::default());
    let publisher = join_setup(&url, "P", "Publisher", true, rec_pub).await;

    let sensor = publisher
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let reading = publisher
        .get_attribute_handle(sensor, "Reading")
        .await
        .unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(reading);
    publisher
        .publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let instance = publisher.register_object_instance(sensor).await.unwrap();

    // Publisher owns Reading.
    assert!(
        publisher
            .is_attribute_owned_by_federate(instance, reading)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_owner_update_returns_attribute_not_owned() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec_a = Arc::new(OwnerRec::default());
    let amb_a = join_setup(&url, "A", "Publisher", true, rec_a).await;
    let sensor = amb_a
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let reading = amb_a.get_attribute_handle(sensor, "Reading").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(reading);
    amb_a
        .publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let instance = amb_a.register_object_instance(sensor).await.unwrap();

    // B joins and publishes the class (so registerObjectInstance would
    // succeed for it), but tries to update A's instance — should fail.
    let rec_b = Arc::new(OwnerRec::default());
    let amb_b = join_setup(&url, "B", "Other", false, rec_b).await;
    amb_b
        .publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let mut values = AttributeHandleValueMap::new();
    values.insert(reading, 7i32.to_be_bytes().to_vec());
    let err = amb_b
        .update_attribute_values(instance, values, &[])
        .await
        .unwrap_err();
    match err {
        hla_federate::CallError::RtiException { name, .. } => {
            assert_eq!(name, "AttributeNotOwned");
        }
        other => panic!("expected AttributeNotOwned, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divestiture_clears_ownership() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec = Arc::new(OwnerRec::default());
    let amb = join_setup(&url, "P", "Publisher", true, Arc::clone(&rec)).await;
    let sensor = amb
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let reading = amb.get_attribute_handle(sensor, "Reading").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(reading);
    amb.publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let instance = amb.register_object_instance(sensor).await.unwrap();
    assert!(
        amb.is_attribute_owned_by_federate(instance, reading)
            .await
            .unwrap()
    );

    amb.unconditional_attribute_ownership_divestiture(instance, attrs, b"goodbye")
        .await
        .unwrap();
    assert!(
        !amb.is_attribute_owned_by_federate(instance, reading)
            .await
            .unwrap()
    );

    // Subsequent update fails: attribute is no longer owned.
    let mut values = AttributeHandleValueMap::new();
    values.insert(reading, 1i32.to_be_bytes().to_vec());
    let err = amb
        .update_attribute_values(instance, values, &[])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        hla_federate::CallError::RtiException { ref name, .. } if name == "AttributeNotOwned"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acquisition_if_available_grants_unowned_attrs() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Owner A divests, then B acquires-if-available.
    let rec_a = Arc::new(OwnerRec::default());
    let amb_a = join_setup(&url, "A", "OrigOwner", true, Arc::clone(&rec_a)).await;
    let sensor = amb_a
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let reading = amb_a.get_attribute_handle(sensor, "Reading").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(reading);
    amb_a
        .publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let instance = amb_a.register_object_instance(sensor).await.unwrap();
    amb_a
        .unconditional_attribute_ownership_divestiture(instance, attrs.clone(), b"")
        .await
        .unwrap();

    use std::sync::atomic::{AtomicU32, Ordering};
    #[derive(Default)]
    struct AcqRec {
        notified: AtomicU32,
        unavailable: AtomicU32,
    }
    impl FederateAmbassador for AcqRec {
        async fn attribute_ownership_acquisition_notification(
            &self,
            _i: ObjectInstanceHandle,
            _a: Vec<AttributeHandle>,
            _t: Vec<u8>,
        ) {
            self.notified.fetch_add(1, Ordering::Relaxed);
        }
        async fn attribute_ownership_unavailable(
            &self,
            _i: ObjectInstanceHandle,
            _a: Vec<AttributeHandle>,
            _t: Vec<u8>,
        ) {
            self.unavailable.fetch_add(1, Ordering::Relaxed);
        }
    }
    let rec_b = Arc::new(AcqRec::default());
    let amb_b = join_setup_generic(&url, "B", "NewOwner", false, Arc::clone(&rec_b)).await;

    amb_b
        .attribute_ownership_acquisition_if_available(instance, attrs.clone(), b"")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < deadline {
        if rec_b.notified.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(rec_b.notified.load(Ordering::Relaxed) >= 1);
    assert!(
        amb_b
            .is_attribute_owned_by_federate(instance, reading)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acquisition_if_available_reports_owned_attrs_unavailable() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec_a = Arc::new(OwnerRec::default());
    let amb_a = join_setup(&url, "A", "Owner", true, Arc::clone(&rec_a)).await;
    let sensor = amb_a
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let reading = amb_a.get_attribute_handle(sensor, "Reading").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(reading);
    amb_a
        .publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let instance = amb_a.register_object_instance(sensor).await.unwrap();
    // A keeps ownership — does NOT divest.

    use std::sync::atomic::{AtomicU32, Ordering};
    #[derive(Default)]
    struct AcqRec {
        unavailable: AtomicU32,
    }
    impl FederateAmbassador for AcqRec {
        async fn attribute_ownership_unavailable(
            &self,
            _i: ObjectInstanceHandle,
            _a: Vec<AttributeHandle>,
            _t: Vec<u8>,
        ) {
            self.unavailable.fetch_add(1, Ordering::Relaxed);
        }
    }
    let rec_b = Arc::new(AcqRec::default());
    let amb_b = join_setup_generic(&url, "B", "WouldBe", false, Arc::clone(&rec_b)).await;
    amb_b
        .attribute_ownership_acquisition_if_available(instance, attrs, b"")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < deadline {
        if rec_b.unavailable.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(rec_b.unavailable.load(Ordering::Relaxed) >= 1);
    // A still owns.
    assert!(
        amb_a
            .is_attribute_owned_by_federate(instance, reading)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_ownership_callbacks() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec = Arc::new(OwnerRec::default());
    let amb = join_setup(&url, "P", "Publisher", true, Arc::clone(&rec)).await;
    let sensor = amb
        .get_object_class_handle("HLAobjectRoot.Sensor")
        .await
        .unwrap();
    let reading = amb.get_attribute_handle(sensor, "Reading").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(reading);
    amb.publish_object_class_attributes(sensor, attrs.clone())
        .await
        .unwrap();
    let instance = amb.register_object_instance(sensor).await.unwrap();

    amb.query_attribute_ownership(instance, attrs.clone())
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < deadline {
        if !rec.informs.lock().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let informs = rec.informs.lock();
    assert_eq!(informs.len(), 1);
    assert_eq!(informs[0].0, instance);
    assert_eq!(informs[0].1, vec![reading]);
    // owner field is the federate handle — we don't have a way to fetch our
    // own handle directly from the ambassador surface, so we just check
    // the raw bytes are non-zero.
    assert!(informs[0].2.raw() >= 1);
}
