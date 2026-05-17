//! Generate Rust types from the vendored FedProClient `.proto` files.
//!
//! The protos live under `vendor/fedproclient/` and are redistributed under
//! Apache 2.0 (see `vendor/fedproclient/LICENSE`); the proto file headers
//! themselves additionally carry an IEEE royalty-free grant.

use std::path::PathBuf;

fn main() {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vendor/fedproclient");
    let protos = [
        proto_dir.join("datatypes.proto"),
        proto_dir.join("RTIambassador.proto"),
        proto_dir.join("FederateAmbassador.proto"),
    ];

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    let mut config = prost_build::Config::new();
    config
        .compile_protos(&protos, std::slice::from_ref(&proto_dir))
        .expect("failed to compile FedPro protos");
}
