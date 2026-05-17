//! Federate-side HLA 4 library.
//!
//! Connects to an RTI over FedPro, demultiplexes responses vs callbacks,
//! and exposes a typed async [`RtiAmbassador`] surface plus a
//! [`FederateAmbassador`] trait the federate code implements to receive
//! callbacks.

pub mod ambassador;
pub mod callbacks;
// Internal: the handles module exposes `prost`-generated types. Keep it
// out of the public API per BESTPRACTICES §C2.
pub(crate) mod handles;

pub use ambassador::{CallError, ConnectError, RtiAmbassador};
pub use callbacks::FederateAmbassador;
