//! DDM region-overlap routing: subscribers only receive updates when their
//! subscribed region overlaps the publisher's instance region.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, DimensionHandle, FederateHandle,
    ObjectInstanceHandle,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;

const FOM: &str = r#"<?xml version="1.0"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>R</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Mover</name>
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
struct Rec {
    reflects: AtomicU32,
}
impl FederateAmbassador for Rec {
    async fn reflect_attribute_values(
        &self,
        _instance: ObjectInstanceHandle,
        _values: AttributeHandleValueMap,
        _tag: Vec<u8>,
        _producer: Option<FederateHandle>,
    ) {
        self.reflects.fetch_add(1, Ordering::Relaxed);
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
async fn overlapping_regions_route_update() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    // Subscriber's region [50, 150] on dim X.
    let rec_b = Arc::new(Rec::default());
    let b = RtiAmbassador::connect(&url, Arc::clone(&rec_b))
        .await
        .unwrap();
    b.create_federation_execution("region").await.ok();
    b.join_federation_execution("B", "region").await.unwrap();
    let class = b
        .get_object_class_handle("HLAobjectRoot.Mover")
        .await
        .unwrap();
    let x = b.get_attribute_handle(class, "X").await.unwrap();
    let dim_x = DimensionHandle::new(1);

    let sub_region = b.create_region(&[dim_x]).await.unwrap();
    b.set_range_bounds(sub_region, dim_x, 50, 150)
        .await
        .unwrap();
    b.commit_region_modifications(&[sub_region]).await.unwrap();

    // Use raw call for subscribe-with-regions.
    use hla_fedpro_proto::fedpro;
    let sub_req =
        fedpro::call_request::CallRequest::SubscribeObjectClassAttributesWithRegionsRequest(
            fedpro::SubscribeObjectClassAttributesWithRegionsRequest {
                object_class: Some(fedpro::ObjectClassHandle {
                    data: class.raw().to_be_bytes().to_vec(),
                }),
                active: true,
                attributes_and_regions: Some(fedpro::AttributeSetRegionSetPairList {
                    attribute_set_region_set_pair: vec![fedpro::AttributeSetRegionSetPair {
                        attribute_set: Some(fedpro::AttributeHandleSet {
                            attribute_handle: vec![fedpro::AttributeHandle {
                                data: x.raw().to_be_bytes().to_vec(),
                            }],
                        }),
                        region_set: Some(fedpro::RegionHandleSet {
                            region_handle: vec![fedpro::RegionHandle {
                                data: sub_region.raw().to_be_bytes().to_vec(),
                            }],
                        }),
                    }],
                }),
            },
        );
    b.raw_call_for_test(sub_req).await.unwrap();

    // Publisher creates an OVERLAPPING region [100, 200].
    let rec_a = Arc::new(Rec::default());
    let a = RtiAmbassador::connect(&url, Arc::clone(&rec_a))
        .await
        .unwrap();
    a.join_federation_execution("A", "region").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(x);
    a.publish_object_class_attributes(class, attrs)
        .await
        .unwrap();

    let pub_region = a.create_region(&[dim_x]).await.unwrap();
    a.set_range_bounds(pub_region, dim_x, 100, 200)
        .await
        .unwrap();
    a.commit_region_modifications(&[pub_region]).await.unwrap();

    let reg_req = fedpro::call_request::CallRequest::RegisterObjectInstanceWithRegionsRequest(
        fedpro::RegisterObjectInstanceWithRegionsRequest {
            object_class: Some(fedpro::ObjectClassHandle {
                data: class.raw().to_be_bytes().to_vec(),
            }),
            attributes_and_regions: Some(fedpro::AttributeSetRegionSetPairList {
                attribute_set_region_set_pair: vec![fedpro::AttributeSetRegionSetPair {
                    attribute_set: Some(fedpro::AttributeHandleSet {
                        attribute_handle: vec![fedpro::AttributeHandle {
                            data: x.raw().to_be_bytes().to_vec(),
                        }],
                    }),
                    region_set: Some(fedpro::RegionHandleSet {
                        region_handle: vec![fedpro::RegionHandle {
                            data: pub_region.raw().to_be_bytes().to_vec(),
                        }],
                    }),
                }],
            }),
        },
    );
    let resp = a.raw_call_for_test(reg_req).await.unwrap();
    let instance = match resp {
        hla_fedpro_proto::fedpro::call_response::CallResponse::RegisterObjectInstanceWithRegionsResponse(r) => {
            let h = r.result.unwrap();
            ObjectInstanceHandle::new(u64::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        other => panic!("unexpected: {other:?}"),
    };

    let mut values = AttributeHandleValueMap::new();
    values.insert(x, 1i32.to_be_bytes().to_vec());
    a.update_attribute_values(instance, values, b"overlap")
        .await
        .unwrap();

    // Subscriber should receive (regions overlap on X = [100,150]).
    assert!(
        wait_for(Duration::from_secs(1), || {
            rec_b.reflects.load(Ordering::Relaxed) >= 1
        })
        .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_overlapping_regions_filter_update() {
    let addr = boot().await;
    let url = format!("rti://{addr}");

    let rec_b = Arc::new(Rec::default());
    let b = RtiAmbassador::connect(&url, Arc::clone(&rec_b))
        .await
        .unwrap();
    b.create_federation_execution("region2").await.ok();
    b.join_federation_execution("B", "region2").await.unwrap();
    let class = b
        .get_object_class_handle("HLAobjectRoot.Mover")
        .await
        .unwrap();
    let x = b.get_attribute_handle(class, "X").await.unwrap();
    let dim_x = DimensionHandle::new(1);

    // Subscriber region [0, 50].
    let sub_region = b.create_region(&[dim_x]).await.unwrap();
    b.set_range_bounds(sub_region, dim_x, 0, 50).await.unwrap();
    b.commit_region_modifications(&[sub_region]).await.unwrap();

    use hla_fedpro_proto::fedpro;
    let sub_req =
        fedpro::call_request::CallRequest::SubscribeObjectClassAttributesWithRegionsRequest(
            fedpro::SubscribeObjectClassAttributesWithRegionsRequest {
                object_class: Some(fedpro::ObjectClassHandle {
                    data: class.raw().to_be_bytes().to_vec(),
                }),
                active: true,
                attributes_and_regions: Some(fedpro::AttributeSetRegionSetPairList {
                    attribute_set_region_set_pair: vec![fedpro::AttributeSetRegionSetPair {
                        attribute_set: Some(fedpro::AttributeHandleSet {
                            attribute_handle: vec![fedpro::AttributeHandle {
                                data: x.raw().to_be_bytes().to_vec(),
                            }],
                        }),
                        region_set: Some(fedpro::RegionHandleSet {
                            region_handle: vec![fedpro::RegionHandle {
                                data: sub_region.raw().to_be_bytes().to_vec(),
                            }],
                        }),
                    }],
                }),
            },
        );
    b.raw_call_for_test(sub_req).await.unwrap();

    // Publisher region [100, 200] — does NOT overlap [0, 50].
    let rec_a = Arc::new(Rec::default());
    let a = RtiAmbassador::connect(&url, Arc::clone(&rec_a))
        .await
        .unwrap();
    a.join_federation_execution("A", "region2").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(x);
    a.publish_object_class_attributes(class, attrs)
        .await
        .unwrap();
    let pub_region = a.create_region(&[dim_x]).await.unwrap();
    a.set_range_bounds(pub_region, dim_x, 100, 200)
        .await
        .unwrap();
    a.commit_region_modifications(&[pub_region]).await.unwrap();

    let reg_req = fedpro::call_request::CallRequest::RegisterObjectInstanceWithRegionsRequest(
        fedpro::RegisterObjectInstanceWithRegionsRequest {
            object_class: Some(fedpro::ObjectClassHandle {
                data: class.raw().to_be_bytes().to_vec(),
            }),
            attributes_and_regions: Some(fedpro::AttributeSetRegionSetPairList {
                attribute_set_region_set_pair: vec![fedpro::AttributeSetRegionSetPair {
                    attribute_set: Some(fedpro::AttributeHandleSet {
                        attribute_handle: vec![fedpro::AttributeHandle {
                            data: x.raw().to_be_bytes().to_vec(),
                        }],
                    }),
                    region_set: Some(fedpro::RegionHandleSet {
                        region_handle: vec![fedpro::RegionHandle {
                            data: pub_region.raw().to_be_bytes().to_vec(),
                        }],
                    }),
                }],
            }),
        },
    );
    let resp = a.raw_call_for_test(reg_req).await.unwrap();
    let instance = match resp {
        hla_fedpro_proto::fedpro::call_response::CallResponse::RegisterObjectInstanceWithRegionsResponse(r) => {
            let h = r.result.unwrap();
            ObjectInstanceHandle::new(u64::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => panic!(),
    };

    let mut values = AttributeHandleValueMap::new();
    values.insert(x, 1i32.to_be_bytes().to_vec());
    a.update_attribute_values(instance, values, b"no-overlap")
        .await
        .unwrap();

    // No overlap → subscriber should NOT receive.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rec_b.reflects.load(Ordering::Relaxed), 0);
}
