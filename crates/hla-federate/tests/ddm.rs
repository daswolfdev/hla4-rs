//! DDM region lifecycle (create/commit/get/set/delete).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::DimensionHandle;
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>DDM</name></modelIdentification>
  <objects><objectClass><name>HLAobjectRoot</name></objectClass></objects>
  <interactions><interactionClass><name>HLAinteractionRoot</name>
    <transportation>HLAreliable</transportation><order>Receive</order>
  </interactionClass></interactions>
</objectModel>"#;

struct NoopFa;
impl FederateAmbassador for NoopFa {}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn region_lifecycle_create_set_commit_get_delete() {
    let addr = boot().await;
    let url = format!("rti://{addr}");
    let amb = RtiAmbassador::connect(&url, Arc::new(NoopFa))
        .await
        .unwrap();
    amb.create_federation_execution("ddm-fed").await.ok();
    amb.join_federation_execution("F", "ddm-fed").await.unwrap();

    let dim_x = DimensionHandle::new(1);
    let dim_y = DimensionHandle::new(2);
    let region = amb.create_region(&[dim_x, dim_y]).await.unwrap();
    assert!(region.raw() >= 1);

    // Initial committed bounds are (0, u32::MAX).
    let (lo, hi) = amb.get_range_bounds(region, dim_x).await.unwrap();
    assert_eq!((lo, hi), (0, u32::MAX));

    // Modify staged but not yet committed.
    amb.set_range_bounds(region, dim_x, 100, 200).await.unwrap();
    let (lo, hi) = amb.get_range_bounds(region, dim_x).await.unwrap();
    assert_eq!(
        (lo, hi),
        (0, u32::MAX),
        "should still see committed value before commit"
    );

    amb.commit_region_modifications(&[region]).await.unwrap();
    let (lo, hi) = amb.get_range_bounds(region, dim_x).await.unwrap();
    assert_eq!(
        (lo, hi),
        (100, 200),
        "after commit, committed reflects staged"
    );

    amb.delete_region(region).await.unwrap();
    // Get on deleted region should fail.
    let err = amb.get_range_bounds(region, dim_x).await.unwrap_err();
    assert!(matches!(err, hla_federate::CallError::RtiException { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_range_bounds_rejects_inverted() {
    let addr = boot().await;
    let url = format!("rti://{addr}");
    let amb = RtiAmbassador::connect(&url, Arc::new(NoopFa))
        .await
        .unwrap();
    amb.create_federation_execution("ddm2-fed").await.ok();
    amb.join_federation_execution("F", "ddm2-fed")
        .await
        .unwrap();
    let dim = DimensionHandle::new(1);
    let region = amb.create_region(&[dim]).await.unwrap();
    let err = amb
        .set_range_bounds(region, dim, 500, 100)
        .await
        .unwrap_err();
    match err {
        hla_federate::CallError::RtiException { name, .. } => {
            assert_eq!(name, "InvalidRangeBound");
        }
        other => panic!("got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_region_owned_by_other_federate_rejected() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let a = RtiAmbassador::connect(&url, Arc::new(NoopFa))
        .await
        .unwrap();
    a.create_federation_execution("ddm3-fed").await.ok();
    a.join_federation_execution("A", "ddm3-fed").await.unwrap();
    let dim = DimensionHandle::new(1);
    let region = a.create_region(&[dim]).await.unwrap();

    let b = RtiAmbassador::connect(&url, Arc::new(NoopFa))
        .await
        .unwrap();
    b.join_federation_execution("B", "ddm3-fed").await.unwrap();
    let err = b.delete_region(region).await.unwrap_err();
    match err {
        hla_federate::CallError::RtiException { name, .. } => {
            assert_eq!(name, "RegionNotCreatedByThisFederate");
        }
        other => panic!("got {other:?}"),
    }
}
