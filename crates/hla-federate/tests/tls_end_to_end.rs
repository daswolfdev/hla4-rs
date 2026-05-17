//! End-to-end TLS: federate connects to RTI over TLS with a self-signed cert.
//!
//! Uses `rcgen` to generate an ephemeral self-signed cert in-process. The
//! client trusts the generated cert via a one-cert root store. No
//! certificate verification disabled — this is the same TLS path real
//! deployments would use, just with a self-signed root.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, ObjectClassHandle,
    ObjectInstanceHandle,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};
use hla_omt::{FomModule, MergedFom};
use hla_rti::RtiNode;
use parking_lot::Mutex;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::net::TcpListener;

const SUSHI_LITE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification><name>SushiLite</name></modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
      <objectClass>
        <name>Drink</name>
        <attribute>
          <name>NumberCups</name>
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

struct GeneratedCert {
    cert_der: CertificateDer<'static>,
    key_der: PrivateKeyDer<'static>,
}

fn generate_self_signed(san: &str) -> GeneratedCert {
    let cert = rcgen::generate_simple_self_signed(vec![san.to_string()]).unwrap();
    GeneratedCert {
        cert_der: cert.cert.der().clone(),
        key_der: PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into()),
    }
}

fn server_config(cert: &GeneratedCert) -> Arc<rustls::ServerConfig> {
    // Install the rustls aws-lc-rs CryptoProvider if not already installed.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert_der.clone()], cert.key_der.clone_key())
            .unwrap(),
    )
}

fn client_config(cert: &GeneratedCert) -> Arc<rustls::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert.cert_der.clone()).unwrap();
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    )
}

#[derive(Default)]
struct Recorder {
    discoveries: Mutex<Vec<(ObjectInstanceHandle, ObjectClassHandle)>>,
    reflects: Mutex<Vec<(ObjectInstanceHandle, AttributeHandleValueMap, Vec<u8>)>>,
}

impl FederateAmbassador for Recorder {
    async fn discover_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        class: ObjectClassHandle,
        _name: String,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.discoveries.lock().push((instance, class));
    }
    async fn reflect_attribute_values(
        &self,
        instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) {
        self.reflects.lock().push((instance, values, tag));
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
async fn pub_sub_over_tls() {
    let cert = generate_self_signed("localhost");
    let server_cfg = server_config(&cert);
    let client_cfg = client_config(&cert);

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(bind).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node = Arc::new(RtiNode::new(addr));
    node.set_default_fom(MergedFom::merge(vec![FomModule::parse(SUSHI_LITE).unwrap()]).unwrap());

    let serve_node = Arc::clone(&node);
    let acceptor = Arc::new(tokio_rustls::TlsAcceptor::from(server_cfg));
    tokio::spawn(async move {
        let _ = serve_node.serve_tls(listener, acceptor).await;
    });
    let url = format!("rti://{addr}");
    let server_name = ServerName::try_from("localhost").unwrap();

    // ---- Subscriber over TLS ----
    let rec = Arc::new(Recorder::default());
    let sub = RtiAmbassador::connect_tls(
        &url,
        Arc::clone(&client_cfg),
        server_name.clone(),
        Arc::clone(&rec),
    )
    .await
    .unwrap();
    sub.create_federation_execution("tls-fed").await.ok();
    sub.join_federation_execution("Sub", "tls-fed")
        .await
        .unwrap();
    let drink = sub
        .get_object_class_handle("HLAobjectRoot.Drink")
        .await
        .unwrap();
    let cups = sub.get_attribute_handle(drink, "NumberCups").await.unwrap();
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(cups);
    sub.subscribe_object_class_attributes(drink, attrs.clone())
        .await
        .unwrap();

    // ---- Publisher over TLS ----
    let publisher = RtiAmbassador::connect_tls(
        &url,
        Arc::clone(&client_cfg),
        server_name,
        Arc::new(Recorder::default()),
    )
    .await
    .unwrap();
    publisher.create_federation_execution("tls-fed").await.ok();
    publisher
        .join_federation_execution("Pub", "tls-fed")
        .await
        .unwrap();
    publisher
        .publish_object_class_attributes(drink, attrs)
        .await
        .unwrap();
    let instance = publisher.register_object_instance(drink).await.unwrap();
    let mut values = AttributeHandleValueMap::new();
    values.insert(cups, 100i32.to_be_bytes().to_vec());
    publisher
        .update_attribute_values(instance, values, b"tls-tag")
        .await
        .unwrap();

    let got = wait_for(Duration::from_secs(2), || {
        !rec.discoveries.lock().is_empty() && !rec.reflects.lock().is_empty()
    })
    .await;
    assert!(got, "subscriber didn't receive callbacks over TLS");
    let reflects = rec.reflects.lock();
    assert_eq!(reflects[0].0, instance);
    assert_eq!(reflects[0].1.get(&cups).unwrap(), &100i32.to_be_bytes());
    assert_eq!(reflects[0].2, b"tls-tag");
}
