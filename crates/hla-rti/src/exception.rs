//! Strongly-typed HLA exception names.
//!
//! IEEE 1516.1-2025 specifies a fixed set of exception names that travel
//! over the wire in `fedpro::ExceptionData.exception_name`. Carrying them
//! as `&str` literals at every call site (200+ in dispatch) is a typo
//! waiting to happen. `HlaException::name()` is the single source of truth
//! and the compiler enforces that the variant exists.

use std::fmt;

/// HLA exception kind. The `Display` / [`Self::name`] form is the wire
/// string (`"FederateNotExecutionMember"` etc.) required by the FedPro
/// `ExceptionData` message and read by federate clients that match on it.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum HlaException {
    AttributeNotDefined,
    AttributeNotOwned,
    CouldNotOpenFdd,
    DeletePrivilegeNotHeld,
    ErrorReadingFdd,
    FederateAlreadyExecutionMember,
    FederateNameAlreadyInUse,
    FederateNotExecutionMember,
    FederateNotInSaveInitiated,
    FederateNotInSaveSet,
    FederateNotInSynchronizationGroup,
    FederatesCurrentlyJoined,
    FederationExecutionAlreadyExists,
    FederationExecutionDoesNotExist,
    InteractionClassNotDefined,
    InteractionClassNotPublished,
    InteractionParameterNotDefined,
    InTimeAdvancingState,
    InvalidAttributeHandle,
    InvalidDimension,
    InvalidDimensionHandle,
    InvalidFederateHandle,
    InvalidInteractionClassHandle,
    InvalidLogicalTime,
    InvalidLookahead,
    InvalidObjectClassHandle,
    InvalidObjectInstanceHandle,
    InvalidOrderName,
    InvalidOrderType,
    InvalidParameterHandle,
    InvalidRangeBound,
    InvalidRegion,
    InvalidResignAction,
    InvalidRestoreLabel,
    InvalidSaveLabel,
    InvalidSynchronizationPointLabel,
    InvalidTransportationName,
    InvalidTransportationTypeHandle,
    LogicalTimeAlreadyPassed,
    NameNotFound,
    ObjectClassNotDefined,
    ObjectClassNotPublished,
    ObjectInstanceNameInUse,
    ObjectInstanceNotKnown,
    RegionNotCreatedByThisFederate,
    RestoreInProgress,
    RestoreNotInProgress,
    RtiInternalError,
    SaveInProgress,
    SaveNotInProgress,
    SynchronizationPointLabelNotAnnounced,
    TimeConstrainedAlreadyEnabled,
    TimeConstrainedIsNotEnabled,
    TimeRegulationAlreadyEnabled,
    TimeRegulationIsNotEnabled,
}

impl HlaException {
    /// Canonical spec name as it appears on the wire.
    pub const fn name(self) -> &'static str {
        match self {
            Self::AttributeNotDefined => "AttributeNotDefined",
            Self::AttributeNotOwned => "AttributeNotOwned",
            Self::CouldNotOpenFdd => "CouldNotOpenFDD",
            Self::DeletePrivilegeNotHeld => "DeletePrivilegeNotHeld",
            Self::ErrorReadingFdd => "ErrorReadingFDD",
            Self::FederateAlreadyExecutionMember => "FederateAlreadyExecutionMember",
            Self::FederateNameAlreadyInUse => "FederateNameAlreadyInUse",
            Self::FederateNotExecutionMember => "FederateNotExecutionMember",
            Self::FederateNotInSaveInitiated => "FederateNotInSaveInitiated",
            Self::FederateNotInSaveSet => "FederateNotInSaveSet",
            Self::FederateNotInSynchronizationGroup => "FederateNotInSynchronizationGroup",
            Self::FederatesCurrentlyJoined => "FederatesCurrentlyJoined",
            Self::FederationExecutionAlreadyExists => "FederationExecutionAlreadyExists",
            Self::FederationExecutionDoesNotExist => "FederationExecutionDoesNotExist",
            Self::InteractionClassNotDefined => "InteractionClassNotDefined",
            Self::InteractionClassNotPublished => "InteractionClassNotPublished",
            Self::InteractionParameterNotDefined => "InteractionParameterNotDefined",
            Self::InTimeAdvancingState => "InTimeAdvancingState",
            Self::InvalidAttributeHandle => "InvalidAttributeHandle",
            Self::InvalidDimension => "InvalidDimension",
            Self::InvalidDimensionHandle => "InvalidDimensionHandle",
            Self::InvalidFederateHandle => "InvalidFederateHandle",
            Self::InvalidInteractionClassHandle => "InvalidInteractionClassHandle",
            Self::InvalidLogicalTime => "InvalidLogicalTime",
            Self::InvalidLookahead => "InvalidLookahead",
            Self::InvalidObjectClassHandle => "InvalidObjectClassHandle",
            Self::InvalidObjectInstanceHandle => "InvalidObjectInstanceHandle",
            Self::InvalidOrderName => "InvalidOrderName",
            Self::InvalidOrderType => "InvalidOrderType",
            Self::InvalidParameterHandle => "InvalidParameterHandle",
            Self::InvalidRangeBound => "InvalidRangeBound",
            Self::InvalidRegion => "InvalidRegion",
            Self::InvalidResignAction => "InvalidResignAction",
            Self::InvalidRestoreLabel => "InvalidRestoreLabel",
            Self::InvalidSaveLabel => "InvalidSaveLabel",
            Self::InvalidSynchronizationPointLabel => "InvalidSynchronizationPointLabel",
            Self::InvalidTransportationName => "InvalidTransportationName",
            Self::InvalidTransportationTypeHandle => "InvalidTransportationTypeHandle",
            Self::LogicalTimeAlreadyPassed => "LogicalTimeAlreadyPassed",
            Self::NameNotFound => "NameNotFound",
            Self::ObjectClassNotDefined => "ObjectClassNotDefined",
            Self::ObjectClassNotPublished => "ObjectClassNotPublished",
            Self::ObjectInstanceNameInUse => "ObjectInstanceNameInUse",
            Self::ObjectInstanceNotKnown => "ObjectInstanceNotKnown",
            Self::RegionNotCreatedByThisFederate => "RegionNotCreatedByThisFederate",
            Self::RestoreInProgress => "RestoreInProgress",
            Self::RestoreNotInProgress => "RestoreNotInProgress",
            Self::RtiInternalError => "RTIinternalError",
            Self::SaveInProgress => "SaveInProgress",
            Self::SaveNotInProgress => "SaveNotInProgress",
            Self::SynchronizationPointLabelNotAnnounced => "SynchronizationPointLabelNotAnnounced",
            Self::TimeConstrainedAlreadyEnabled => "TimeConstrainedAlreadyEnabled",
            Self::TimeConstrainedIsNotEnabled => "TimeConstrainedIsNotEnabled",
            Self::TimeRegulationAlreadyEnabled => "TimeRegulationAlreadyEnabled",
            Self::TimeRegulationIsNotEnabled => "TimeRegulationIsNotEnabled",
        }
    }
}

impl fmt::Display for HlaException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wire-string contract guard. Any drift between a variant identifier and
    /// the canonical IEEE 1516.1-2025 exception name belongs in this table,
    /// so the test fails the moment the mapping changes. Pay attention to the
    /// three non-trivial casings: `*Fdd`, `*Fdd`, and `RtiInternal`.
    #[test]
    fn wire_string_mapping_is_canonical() {
        let cases: &[(HlaException, &str)] = &[
            (HlaException::AttributeNotDefined, "AttributeNotDefined"),
            (HlaException::AttributeNotOwned, "AttributeNotOwned"),
            (HlaException::CouldNotOpenFdd, "CouldNotOpenFDD"),
            (
                HlaException::DeletePrivilegeNotHeld,
                "DeletePrivilegeNotHeld",
            ),
            (HlaException::ErrorReadingFdd, "ErrorReadingFDD"),
            (
                HlaException::FederateAlreadyExecutionMember,
                "FederateAlreadyExecutionMember",
            ),
            (
                HlaException::FederateNameAlreadyInUse,
                "FederateNameAlreadyInUse",
            ),
            (
                HlaException::FederateNotExecutionMember,
                "FederateNotExecutionMember",
            ),
            (
                HlaException::FederateNotInSaveInitiated,
                "FederateNotInSaveInitiated",
            ),
            (HlaException::FederateNotInSaveSet, "FederateNotInSaveSet"),
            (
                HlaException::FederateNotInSynchronizationGroup,
                "FederateNotInSynchronizationGroup",
            ),
            (
                HlaException::FederatesCurrentlyJoined,
                "FederatesCurrentlyJoined",
            ),
            (
                HlaException::FederationExecutionAlreadyExists,
                "FederationExecutionAlreadyExists",
            ),
            (
                HlaException::FederationExecutionDoesNotExist,
                "FederationExecutionDoesNotExist",
            ),
            (
                HlaException::InteractionClassNotDefined,
                "InteractionClassNotDefined",
            ),
            (
                HlaException::InteractionClassNotPublished,
                "InteractionClassNotPublished",
            ),
            (
                HlaException::InteractionParameterNotDefined,
                "InteractionParameterNotDefined",
            ),
            (HlaException::InTimeAdvancingState, "InTimeAdvancingState"),
            (
                HlaException::InvalidAttributeHandle,
                "InvalidAttributeHandle",
            ),
            (HlaException::InvalidDimension, "InvalidDimension"),
            (
                HlaException::InvalidDimensionHandle,
                "InvalidDimensionHandle",
            ),
            (HlaException::InvalidFederateHandle, "InvalidFederateHandle"),
            (
                HlaException::InvalidInteractionClassHandle,
                "InvalidInteractionClassHandle",
            ),
            (HlaException::InvalidLogicalTime, "InvalidLogicalTime"),
            (HlaException::InvalidLookahead, "InvalidLookahead"),
            (
                HlaException::InvalidObjectClassHandle,
                "InvalidObjectClassHandle",
            ),
            (
                HlaException::InvalidObjectInstanceHandle,
                "InvalidObjectInstanceHandle",
            ),
            (HlaException::InvalidOrderName, "InvalidOrderName"),
            (HlaException::InvalidOrderType, "InvalidOrderType"),
            (
                HlaException::InvalidParameterHandle,
                "InvalidParameterHandle",
            ),
            (HlaException::InvalidRangeBound, "InvalidRangeBound"),
            (HlaException::InvalidRegion, "InvalidRegion"),
            (HlaException::InvalidResignAction, "InvalidResignAction"),
            (HlaException::InvalidRestoreLabel, "InvalidRestoreLabel"),
            (HlaException::InvalidSaveLabel, "InvalidSaveLabel"),
            (
                HlaException::InvalidSynchronizationPointLabel,
                "InvalidSynchronizationPointLabel",
            ),
            (
                HlaException::InvalidTransportationName,
                "InvalidTransportationName",
            ),
            (
                HlaException::InvalidTransportationTypeHandle,
                "InvalidTransportationTypeHandle",
            ),
            (
                HlaException::LogicalTimeAlreadyPassed,
                "LogicalTimeAlreadyPassed",
            ),
            (HlaException::NameNotFound, "NameNotFound"),
            (HlaException::ObjectClassNotDefined, "ObjectClassNotDefined"),
            (
                HlaException::ObjectClassNotPublished,
                "ObjectClassNotPublished",
            ),
            (
                HlaException::ObjectInstanceNameInUse,
                "ObjectInstanceNameInUse",
            ),
            (
                HlaException::ObjectInstanceNotKnown,
                "ObjectInstanceNotKnown",
            ),
            (
                HlaException::RegionNotCreatedByThisFederate,
                "RegionNotCreatedByThisFederate",
            ),
            (HlaException::RestoreInProgress, "RestoreInProgress"),
            (HlaException::RestoreNotInProgress, "RestoreNotInProgress"),
            (HlaException::RtiInternalError, "RTIinternalError"),
            (HlaException::SaveInProgress, "SaveInProgress"),
            (HlaException::SaveNotInProgress, "SaveNotInProgress"),
            (
                HlaException::SynchronizationPointLabelNotAnnounced,
                "SynchronizationPointLabelNotAnnounced",
            ),
            (
                HlaException::TimeConstrainedAlreadyEnabled,
                "TimeConstrainedAlreadyEnabled",
            ),
            (
                HlaException::TimeConstrainedIsNotEnabled,
                "TimeConstrainedIsNotEnabled",
            ),
            (
                HlaException::TimeRegulationAlreadyEnabled,
                "TimeRegulationAlreadyEnabled",
            ),
            (
                HlaException::TimeRegulationIsNotEnabled,
                "TimeRegulationIsNotEnabled",
            ),
        ];
        for (variant, expected) in cases {
            assert_eq!(variant.name(), *expected, "wire string for {variant:?}");
            assert_eq!(variant.to_string(), *expected, "Display for {variant:?}");
        }
    }
}
