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
pub enum OrderType {
    Receive,
    TimestampOrder,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum TransportationType {
    Reliable,
    BestEffort,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ResignAction {
    UnconditionallyDivestAttributes,
    DeleteObjects,
    CancelPendingOwnershipAcquisitions,
    DeleteObjectsThenDivest,
    CancelThenDeleteThenDivest,
    NoAction,
}
