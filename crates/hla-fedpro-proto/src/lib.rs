//! Generated protobuf types for the HLA 4 Federate Protocol.
//!
//! Package path mirrors the proto declaration `package rti1516_2025.fedpro;`.

pub mod rti1516_2025 {
    // All lints relaxed on the generated `include!` only — protobuf codegen
    // emits patterns that conflict with our workspace lint baseline.
    #[allow(
        clippy::all,
        clippy::pedantic,
        clippy::nursery,
        clippy::cargo,
        unreachable_pub
    )]
    pub mod fedpro {
        include!(concat!(env!("OUT_DIR"), "/rti1516_2025.fedpro.rs"));
    }
}

pub use rti1516_2025::fedpro;
