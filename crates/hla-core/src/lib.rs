//! Core HLA domain types shared across the workspace.
//!
//! Zero I/O. Zero protobuf. Zero XML. Anything reachable from this crate is
//! safe to use from any other crate without pulling in `tokio`, `prost`, etc.

pub mod error;
pub mod handle;
pub mod time;

pub use error::{
    DeclarationManagementError, FederationManagementError, ObjectManagementError, RtiError,
    TimeManagementError,
};
pub use handle::{
    AttributeHandle, AttributeHandleSet, AttributeHandleValueMap, DimensionHandle, FederateHandle,
    FederateHandleSet, InteractionClassHandle, ObjectClassHandle, ObjectInstanceHandle,
    ParameterHandle, ParameterHandleValueMap, RegionHandle,
};
pub use time::{HlaFloat64Interval, HlaFloat64Time, LogicalTime, LogicalTimeInterval};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[non_exhaustive]
pub enum OrderType {
    Receive,
    TimestampOrder,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[non_exhaustive]
pub enum TransportationType {
    Reliable,
    BestEffort,
}

/// Resign actions per IEEE 1516.1-2025 §4.10 — the variant set is fixed by
/// the standard, so this enum is intentionally exhaustive. Adding a variant
/// would be a non-trivial protocol change.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ResignAction {
    UnconditionallyDivestAttributes,
    DeleteObjects,
    CancelPendingOwnershipAcquisitions,
    DeleteObjectsThenDivest,
    CancelThenDeleteThenDivest,
    NoAction,
}
