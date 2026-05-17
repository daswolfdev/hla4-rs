//! Per-service error enums, modeled after the exception hierarchy in
//! IEEE 1516.1 §10. Each service category gets its own enum; `RtiError` wraps
//! them for code paths that don't care which category produced the failure.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FederationManagementError {
    #[error("federation execution {0:?} already exists")]
    FederationExecutionAlreadyExists(String),
    #[error("federation execution {0:?} does not exist")]
    FederationExecutionDoesNotExist(String),
    #[error("federate {0:?} is already an execution member")]
    FederateAlreadyExecutionMember(String),
    #[error("federate is not an execution member")]
    FederateNotExecutionMember,
    #[error("federates currently joined")]
    FederatesCurrentlyJoined,
    #[error("inconsistent FDD")]
    InconsistentFdd,
    #[error("could not open FDD: {0}")]
    CouldNotOpenFdd(String),
    #[error("error reading FDD: {0}")]
    ErrorReadingFdd(String),
    #[error("not connected")]
    NotConnected,
    #[error("RTI internal error: {0}")]
    RtiInternalError(String),
}

#[derive(Debug, Error)]
pub enum DeclarationManagementError {
    #[error("object class not defined: {0:?}")]
    ObjectClassNotDefined(String),
    #[error("attribute not defined")]
    AttributeNotDefined,
    #[error("interaction class not defined: {0:?}")]
    InteractionClassNotDefined(String),
    #[error("attribute not owned by federate")]
    AttributeNotOwned,
    #[error("federate not execution member")]
    FederateNotExecutionMember,
    #[error("save in progress")]
    SaveInProgress,
    #[error("restore in progress")]
    RestoreInProgress,
    #[error("RTI internal error: {0}")]
    RtiInternalError(String),
}

#[derive(Debug, Error)]
pub enum ObjectManagementError {
    #[error("object instance not known")]
    ObjectInstanceNotKnown,
    #[error("object class not published")]
    ObjectClassNotPublished,
    #[error("interaction class not published")]
    InteractionClassNotPublished,
    #[error("attribute not defined")]
    AttributeNotDefined,
    #[error("attribute not owned")]
    AttributeNotOwned,
    #[error("invalid object class handle")]
    InvalidObjectClassHandle,
    #[error("invalid interaction class handle")]
    InvalidInteractionClassHandle,
    #[error("object instance name in use: {0:?}")]
    ObjectInstanceNameInUse(String),
    #[error("federate not execution member")]
    FederateNotExecutionMember,
    #[error("RTI internal error: {0}")]
    RtiInternalError(String),
}

#[derive(Debug, Error)]
pub enum TimeManagementError {
    #[error("time regulation is already enabled")]
    TimeRegulationAlreadyEnabled,
    #[error("time constrained is already enabled")]
    TimeConstrainedAlreadyEnabled,
    #[error("logical time already passed")]
    LogicalTimeAlreadyPassed,
    #[error("invalid lookahead")]
    InvalidLookahead,
    #[error("in time advancing state")]
    InTimeAdvancingState,
    #[error("request for time regulation pending")]
    RequestForTimeRegulationPending,
    #[error("federate not execution member")]
    FederateNotExecutionMember,
    #[error("RTI internal error: {0}")]
    RtiInternalError(String),
}

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)] // matches the IEEE 1516.1 service-group taxonomy
pub enum RtiError {
    #[error(transparent)]
    FederationManagement(#[from] FederationManagementError),
    #[error(transparent)]
    DeclarationManagement(#[from] DeclarationManagementError),
    #[error(transparent)]
    ObjectManagement(#[from] ObjectManagementError),
    #[error(transparent)]
    TimeManagement(#[from] TimeManagementError),
}
