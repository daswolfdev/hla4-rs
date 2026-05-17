//! Cross-federate message routing for `HLA_CALLBACK_REQUEST` frames.
//!
//! When a federate calls `updateAttributeValues`, `sendInteraction`, or
//! `registerObjectInstance`, the RTI needs to push corresponding callbacks
//! (`reflectAttributeValues`, `receiveInteraction`, `discoverObjectInstance`)
//! at every subscriber. This module concentrates that fan-out so dispatch
//! handlers can stay focused on validation and state mutation.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use hla_core::{
    AttributeHandle, AttributeHandleValueMap, FederateHandle, InteractionClassHandle,
    ObjectClassHandle, ObjectInstanceHandle, ParameterHandleValueMap,
};
use hla_fedpro_proto::fedpro;
use hla_wire::{Frame, HEADER_SIZE, MessageHeader, MessageType, NO_SEQUENCE_NUMBER};
use prost::Message;

use crate::handles::{
    encode_attribute, encode_federate, encode_interaction_class, encode_object_class,
    encode_object_instance, encode_parameter,
};
use crate::{ConnectionHandle, Federation, RtiNode};

/// Walk `class`'s inheritance chain, returning every federate that subscribes
/// to *any* of `attrs` at any ancestor class (including `class` itself).
/// Optionally exclude `producer` so a federate's own updates don't loop back.
pub fn subscribers_for_attributes(
    federation: &Federation,
    class: ObjectClassHandle,
    attrs: &[AttributeHandle],
    exclude: Option<FederateHandle>,
) -> HashSet<FederateHandle> {
    let subs = federation.subscriptions.read();
    let mut out = HashSet::new();
    for ancestor in federation.fom.inheritance_chain(class) {
        for &attr in attrs {
            if let Some(set) = subs.by_attribute.get(&(ancestor, attr)) {
                for &fh in set {
                    if Some(fh) != exclude {
                        out.insert(fh);
                    }
                }
            }
        }
    }
    out
}

/// Symmetric for interaction classes — walk parent chain so a subscriber to
/// the root receives interactions on derived classes.
pub fn subscribers_for_interaction(
    federation: &Federation,
    class: InteractionClassHandle,
    exclude: Option<FederateHandle>,
) -> HashSet<FederateHandle> {
    let subs = federation.subscriptions.read();
    let mut out = HashSet::new();
    let mut current = Some(class);
    while let Some(c) = current {
        if let Some(set) = subs.by_interaction.get(&c) {
            for &fh in set {
                if Some(fh) != exclude {
                    out.insert(fh);
                }
            }
        }
        let def = federation.fom.interaction_class_def(c);
        current = def
            .and_then(|d| d.parent.as_deref())
            .and_then(|p| federation.fom.interaction_class_handle(p));
    }
    out
}

/// Resolve a set of `FederateHandle`s to live `ConnectionHandle`s, skipping
/// any federate whose connection is no longer registered (mid-disconnect).
pub fn live_connections(
    node: &Arc<RtiNode>,
    federation: &Federation,
    federates: &HashSet<FederateHandle>,
) -> Vec<Arc<ConnectionHandle>> {
    let federate_map = federation.federates.read();
    federates
        .iter()
        .filter_map(|fh| {
            let session_id = federate_map.get(fh)?.session_id;
            node.connections.get(&session_id).map(|c| Arc::clone(c.value()))
        })
        .collect()
}

/// A pending outbound `HLA_CALLBACK_REQUEST` to deliver after dispatch
/// returns. Carries the target connection handle and the already-encoded
/// protobuf body as a shared `Bytes`; sequence-number stamping happens at
/// send time so retries can be numbered freshly.
///
/// Holding the body as `Bytes` lets a single fan-out share one encoded
/// payload across every subscriber — `Bytes::clone()` is a refcount bump,
/// not a buffer copy. Combined with `fan_out` encoding the protobuf exactly
/// once per update, this turns the previous O(N) encode + O(N) struct-clone
/// cost (per recipient) into O(1) encode + O(N) cheap Bytes clones.
pub struct OutboundCallback {
    pub target: Arc<ConnectionHandle>,
    pub body: Bytes,
}

impl OutboundCallback {
    /// Stamp a fresh outbound sequence number and produce the wire frame.
    pub fn into_frame(self) -> Frame {
        let seq = hla_wire::claim_next_outbound_seq(&self.target.next_outbound_seq);
        let header = MessageHeader::with_payload_size(
            self.body.len() as u32,
            seq,
            self.target.session_id,
            NO_SEQUENCE_NUMBER,
            MessageType::HlaCallbackRequest,
        );
        let _ = HEADER_SIZE; // keep import live
        // `self.body` is shared across all fan-out recipients; passing it
        // through to the Frame is a refcount bump only — no copy.
        Frame::new(header, self.body)
    }
}

/// Encode `callback` exactly once and queue a delivery to every live
/// connection in `connections`. Every per-recipient `OutboundCallback`
/// carries a cheap `Bytes` clone of the same encoded payload.
pub fn fan_out(
    out: &mut Vec<OutboundCallback>,
    connections: &[Arc<ConnectionHandle>],
    callback: fedpro::CallbackRequest,
) {
    if connections.is_empty() {
        return;
    }
    let body = Bytes::from(callback.encode_to_vec());
    for conn in connections {
        out.push(OutboundCallback {
            target: Arc::clone(conn),
            body: body.clone(),
        });
    }
}

// -----------------------------------------------------------------------------
// Callback constructors — small helpers wrapping the typed proto messages
// in a `CallbackRequest` envelope so dispatch sites read clean.
// -----------------------------------------------------------------------------

pub fn discover_object_instance(
    instance: ObjectInstanceHandle,
    class: ObjectClassHandle,
    name: &str,
    producer: FederateHandle,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::DiscoverObjectInstance(
                fedpro::DiscoverObjectInstance {
                    object_instance: Some(encode_object_instance(instance)),
                    object_class: Some(encode_object_class(class)),
                    object_instance_name: name.to_string(),
                    producing_federate: Some(encode_federate(producer)),
                },
            ),
        ),
    }
}

pub fn reflect_attribute_values_with_time(
    instance: ObjectInstanceHandle,
    values: &AttributeHandleValueMap,
    tag: &[u8],
    producer: FederateHandle,
    time: f64,
) -> fedpro::CallbackRequest {
    let proto_map = fedpro::AttributeHandleValueMap {
        attribute_handle_value: values
            .iter()
            .map(|(h, v)| fedpro::AttributeHandleValue {
                attribute_handle: Some(encode_attribute(*h)),
                value: v.clone(),
            })
            .collect(),
    };
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReflectAttributeValuesWithTime(
                fedpro::ReflectAttributeValuesWithTime {
                    object_instance: Some(encode_object_instance(instance)),
                    attribute_values: Some(proto_map),
                    user_supplied_tag: tag.to_vec(),
                    transportation_type: None,
                    producing_federate: Some(encode_federate(producer)),
                    optional_sent_regions: None,
                    time: Some(encode_logical_time(time)),
                    optional_retraction: None,
                    sent_order_type: 1, // TimeStamp
                    received_order_type: 1,
                },
            ),
        ),
    }
}

pub fn receive_interaction_with_time(
    class: InteractionClassHandle,
    params: &ParameterHandleValueMap,
    tag: &[u8],
    producer: FederateHandle,
    time: f64,
) -> fedpro::CallbackRequest {
    let proto_map = fedpro::ParameterHandleValueMap {
        parameter_handle_value: params
            .iter()
            .map(|(h, v)| fedpro::ParameterHandleValue {
                parameter_handle: Some(encode_parameter(*h)),
                value: v.clone(),
            })
            .collect(),
    };
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReceiveInteractionWithTime(
                fedpro::ReceiveInteractionWithTime {
                    interaction_class: Some(encode_interaction_class(class)),
                    parameter_values: Some(proto_map),
                    user_supplied_tag: tag.to_vec(),
                    transportation_type: None,
                    producing_federate: Some(encode_federate(producer)),
                    optional_sent_regions: None,
                    time: Some(encode_logical_time(time)),
                    optional_retraction: None,
                    sent_order_type: 1,
                    received_order_type: 1,
                },
            ),
        ),
    }
}

pub fn remove_object_instance_with_time(
    instance: ObjectInstanceHandle,
    tag: &[u8],
    producer: FederateHandle,
    time: f64,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::RemoveObjectInstanceWithTime(
                fedpro::RemoveObjectInstanceWithTime {
                    object_instance: Some(encode_object_instance(instance)),
                    user_supplied_tag: tag.to_vec(),
                    producing_federate: Some(encode_federate(producer)),
                    time: Some(encode_logical_time(time)),
                    sent_order_type: 1,
                    received_order_type: 1,
                    optional_retraction: None,
                },
            ),
        ),
    }
}

pub fn reflect_attribute_values(
    instance: ObjectInstanceHandle,
    values: &AttributeHandleValueMap,
    tag: &[u8],
    producer: FederateHandle,
) -> fedpro::CallbackRequest {
    let proto_map = fedpro::AttributeHandleValueMap {
        attribute_handle_value: values
            .iter()
            .map(|(h, v)| fedpro::AttributeHandleValue {
                attribute_handle: Some(encode_attribute(*h)),
                value: v.clone(),
            })
            .collect(),
    };
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReflectAttributeValues(
                fedpro::ReflectAttributeValues {
                    object_instance: Some(encode_object_instance(instance)),
                    attribute_values: Some(proto_map),
                    user_supplied_tag: tag.to_vec(),
                    transportation_type: None,
                    producing_federate: Some(encode_federate(producer)),
                    optional_sent_regions: None,
                },
            ),
        ),
    }
}

pub fn receive_directed_interaction(
    class: InteractionClassHandle,
    instance: ObjectInstanceHandle,
    params: &ParameterHandleValueMap,
    tag: &[u8],
    producer: FederateHandle,
) -> fedpro::CallbackRequest {
    let proto_map = fedpro::ParameterHandleValueMap {
        parameter_handle_value: params
            .iter()
            .map(|(h, v)| fedpro::ParameterHandleValue {
                parameter_handle: Some(encode_parameter(*h)),
                value: v.clone(),
            })
            .collect(),
    };
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReceiveDirectedInteraction(
                fedpro::ReceiveDirectedInteraction {
                    interaction_class: Some(encode_interaction_class(class)),
                    object_instance: Some(encode_object_instance(instance)),
                    parameter_values: Some(proto_map),
                    user_supplied_tag: tag.to_vec(),
                    transportation_type: None,
                    producing_federate: Some(encode_federate(producer)),
                },
            ),
        ),
    }
}

pub fn receive_interaction(
    class: InteractionClassHandle,
    params: &ParameterHandleValueMap,
    tag: &[u8],
    producer: FederateHandle,
) -> fedpro::CallbackRequest {
    let proto_map = fedpro::ParameterHandleValueMap {
        parameter_handle_value: params
            .iter()
            .map(|(h, v)| fedpro::ParameterHandleValue {
                parameter_handle: Some(encode_parameter(*h)),
                value: v.clone(),
            })
            .collect(),
    };
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReceiveInteraction(
                fedpro::ReceiveInteraction {
                    interaction_class: Some(encode_interaction_class(class)),
                    parameter_values: Some(proto_map),
                    user_supplied_tag: tag.to_vec(),
                    transportation_type: None,
                    producing_federate: Some(encode_federate(producer)),
                    optional_sent_regions: None,
                },
            ),
        ),
    }
}

pub fn report_federation_execution_members(
    federation_name: &str,
    members: &[(String, String)],
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReportFederationExecutionMembers(
                fedpro::ReportFederationExecutionMembers {
                    federation_name: federation_name.to_string(),
                    report: Some(fedpro::FederationExecutionMemberInformationSet {
                        federation_execution_member_information: members
                            .iter()
                            .map(|(name, ty)| fedpro::FederationExecutionMemberInformation {
                                federate_name: name.clone(),
                                federate_type: ty.clone(),
                            })
                            .collect(),
                    }),
                },
            ),
        ),
    }
}

pub fn report_federation_execution_does_not_exist(
    federation_name: &str,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReportFederationExecutionDoesNotExist(
                fedpro::ReportFederationExecutionDoesNotExist {
                    federation_name: federation_name.to_string(),
                },
            ),
        ),
    }
}

pub fn report_federation_executions(
    federations: &[String],
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::ReportFederationExecutions(
                fedpro::ReportFederationExecutions {
                    report: Some(fedpro::FederationExecutionInformationSet {
                        federation_execution_information: federations
                            .iter()
                            .map(|name| fedpro::FederationExecutionInformation {
                                federation_execution_name: name.clone(),
                                logical_time_implementation_name: "HLAfloat64Time".into(),
                            })
                            .collect(),
                    }),
                },
            ),
        ),
    }
}

pub fn attribute_ownership_acquisition_notification(
    instance: ObjectInstanceHandle,
    secured: &[AttributeHandle],
    tag: &[u8],
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::AttributeOwnershipAcquisitionNotification(
                fedpro::AttributeOwnershipAcquisitionNotification {
                    object_instance: Some(encode_object_instance(instance)),
                    secured_attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: secured.iter().map(|a| encode_attribute(*a)).collect(),
                    }),
                    user_supplied_tag: tag.to_vec(),
                },
            ),
        ),
    }
}

pub fn attribute_ownership_unavailable(
    instance: ObjectInstanceHandle,
    unavailable: &[AttributeHandle],
    tag: &[u8],
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::AttributeOwnershipUnavailable(
                fedpro::AttributeOwnershipUnavailable {
                    object_instance: Some(encode_object_instance(instance)),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: unavailable.iter().map(|a| encode_attribute(*a)).collect(),
                    }),
                    user_supplied_tag: tag.to_vec(),
                },
            ),
        ),
    }
}

pub fn request_federation_restore_succeeded(label: &str) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::RequestFederationRestoreSucceeded(
                fedpro::RequestFederationRestoreSucceeded {
                    label: label.to_string(),
                },
            ),
        ),
    }
}

pub fn request_federation_restore_failed(label: &str) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::RequestFederationRestoreFailed(
                fedpro::RequestFederationRestoreFailed {
                    label: label.to_string(),
                },
            ),
        ),
    }
}

pub fn federation_restore_begun() -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::FederationRestoreBegun(
                fedpro::FederationRestoreBegun {},
            ),
        ),
    }
}

pub fn initiate_federate_restore(
    label: &str,
    federate_name: &str,
    post_restore_handle: FederateHandle,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::InitiateFederateRestore(
                fedpro::InitiateFederateRestore {
                    label: label.to_string(),
                    federate_name: federate_name.to_string(),
                    post_restore_federate_handle: Some(encode_federate(post_restore_handle)),
                },
            ),
        ),
    }
}

pub fn federation_restored() -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::FederationRestored(
                fedpro::FederationRestored {},
            ),
        ),
    }
}

pub fn federation_not_restored(reason: i32) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::FederationNotRestored(
                fedpro::FederationNotRestored { reason },
            ),
        ),
    }
}

pub fn initiate_federate_save(label: &str) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::InitiateFederateSave(
                fedpro::InitiateFederateSave {
                    label: label.to_string(),
                },
            ),
        ),
    }
}

pub fn federation_saved() -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::FederationSaved(
                fedpro::FederationSaved {},
            ),
        ),
    }
}

pub fn federation_not_saved(reason: i32) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::FederationNotSaved(
                fedpro::FederationNotSaved { reason },
            ),
        ),
    }
}

pub fn inform_attribute_ownership(
    instance: ObjectInstanceHandle,
    attributes: &[AttributeHandle],
    federate: FederateHandle,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::InformAttributeOwnership(
                fedpro::InformAttributeOwnership {
                    object_instance: Some(encode_object_instance(instance)),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: attributes.iter().map(|a| encode_attribute(*a)).collect(),
                    }),
                    federate: Some(encode_federate(federate)),
                },
            ),
        ),
    }
}

pub fn attribute_is_not_owned(
    instance: ObjectInstanceHandle,
    attributes: &[AttributeHandle],
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::AttributeIsNotOwned(
                fedpro::AttributeIsNotOwned {
                    object_instance: Some(encode_object_instance(instance)),
                    attributes: Some(fedpro::AttributeHandleSet {
                        attribute_handle: attributes.iter().map(|a| encode_attribute(*a)).collect(),
                    }),
                },
            ),
        ),
    }
}

pub fn synchronization_point_registration_succeeded(label: &str) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::SynchronizationPointRegistrationSucceeded(
                fedpro::SynchronizationPointRegistrationSucceeded {
                    synchronization_point_label: label.to_string(),
                },
            ),
        ),
    }
}

pub fn synchronization_point_registration_failed(
    label: &str,
    reason: i32,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::SynchronizationPointRegistrationFailed(
                fedpro::SynchronizationPointRegistrationFailed {
                    synchronization_point_label: label.to_string(),
                    reason,
                },
            ),
        ),
    }
}

pub fn announce_synchronization_point(label: &str, tag: &[u8]) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::AnnounceSynchronizationPoint(
                fedpro::AnnounceSynchronizationPoint {
                    synchronization_point_label: label.to_string(),
                    user_supplied_tag: tag.to_vec(),
                },
            ),
        ),
    }
}

pub fn federation_synchronized(
    label: &str,
    failed_set: &std::collections::HashSet<FederateHandle>,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::FederationSynchronized(
                fedpro::FederationSynchronized {
                    synchronization_point_label: label.to_string(),
                    failed_to_sync_set: Some(fedpro::FederateHandleSet {
                        federate_handle: failed_set.iter().map(|h| encode_federate(*h)).collect(),
                    }),
                },
            ),
        ),
    }
}

pub fn start_registration_for_object_class(class: ObjectClassHandle) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::StartRegistrationForObjectClass(
                fedpro::StartRegistrationForObjectClass {
                    object_class: Some(encode_object_class(class)),
                },
            ),
        ),
    }
}

pub fn stop_registration_for_object_class(class: ObjectClassHandle) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::StopRegistrationForObjectClass(
                fedpro::StopRegistrationForObjectClass {
                    object_class: Some(encode_object_class(class)),
                },
            ),
        ),
    }
}

pub fn turn_interactions_on(class: InteractionClassHandle) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::TurnInteractionsOn(
                fedpro::TurnInteractionsOn {
                    interaction_class: Some(encode_interaction_class(class)),
                },
            ),
        ),
    }
}

pub fn turn_interactions_off(class: InteractionClassHandle) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::TurnInteractionsOff(
                fedpro::TurnInteractionsOff {
                    interaction_class: Some(encode_interaction_class(class)),
                },
            ),
        ),
    }
}

pub fn time_regulation_enabled(time: f64) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::TimeRegulationEnabled(
                fedpro::TimeRegulationEnabled {
                    time: Some(encode_logical_time(time)),
                },
            ),
        ),
    }
}

pub fn time_constrained_enabled(time: f64) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::TimeConstrainedEnabled(
                fedpro::TimeConstrainedEnabled {
                    time: Some(encode_logical_time(time)),
                },
            ),
        ),
    }
}

pub fn time_advance_grant(time: f64) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::TimeAdvanceGrant(
                fedpro::TimeAdvanceGrant {
                    time: Some(encode_logical_time(time)),
                },
            ),
        ),
    }
}

pub fn encode_logical_time(t: f64) -> fedpro::LogicalTime {
    fedpro::LogicalTime {
        data: t.to_be_bytes().to_vec(),
    }
}

pub fn encode_logical_time_interval(d: f64) -> fedpro::LogicalTimeInterval {
    fedpro::LogicalTimeInterval {
        data: d.to_be_bytes().to_vec(),
    }
}

pub fn decode_logical_time(p: &fedpro::LogicalTime) -> Result<f64, &'static str> {
    if p.data.len() != 8 {
        return Err("InvalidLogicalTime");
    }
    Ok(f64::from_be_bytes(p.data[..].try_into().unwrap()))
}

pub fn decode_logical_time_interval(p: &fedpro::LogicalTimeInterval) -> Result<f64, &'static str> {
    if p.data.len() != 8 {
        return Err("InvalidLookahead");
    }
    Ok(f64::from_be_bytes(p.data[..].try_into().unwrap()))
}

pub fn remove_object_instance(
    instance: ObjectInstanceHandle,
    tag: &[u8],
    producer: FederateHandle,
) -> fedpro::CallbackRequest {
    fedpro::CallbackRequest {
        callback_request: Some(
            fedpro::callback_request::CallbackRequest::RemoveObjectInstance(
                fedpro::RemoveObjectInstance {
                    object_instance: Some(encode_object_instance(instance)),
                    user_supplied_tag: tag.to_vec(),
                    producing_federate: Some(encode_federate(producer)),
                },
            ),
        ),
    }
}
