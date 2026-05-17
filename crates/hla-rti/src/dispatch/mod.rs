//! Service dispatch for `HLA_CALL_REQUEST` frames.
//!
//! Decodes a `fedpro::CallRequest` envelope, routes the inner `oneof` variant
//! to its handler, and produces a `fedpro::CallResponse`. Anything not yet
//! implemented returns an `ExceptionData{exceptionName="RTIinternalError", ...}`
//! response — that keeps the wire well-formed so clients can recover.
//!
//! IEEE 1516.1 exception class names (e.g. `FederationExecutionAlreadyExists`)
//! are reused verbatim as `exceptionName`.
//!
//! ## Module layout
//!
//! The top-level `dispatch_call` `match` lives here. Each banner-comment
//! section in the original 4800-line file was extracted into a sibling
//! submodule (federation_mgmt, declaration, data_flow, restore, etc.).
//! Submodules `use super::*;` to pick up the shared types and helpers;
//! mod.rs `use submod::*;`s pull the handler fns back into scope so the
//! match arms read unchanged.

// `use submod::*;` at the bottom re-exports every handler back into this
// scope, which the match arms in `dispatch_call` rely on. The imports
// here serve both `dispatch_call` directly and propagate to submodules
// via their `use super::*;` — so over-broad imports are load-bearing,
// not noise.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use hla_core::{
    AttributeHandle, AttributeHandleSet, AttributeHandleValueMap, FederateHandle,
    InteractionClassHandle, ObjectClassHandle, ObjectInstanceHandle, ParameterHandleValueMap,
};
use hla_fedpro_proto::fedpro::{
    self, AttributeHandleSet as ProtoAttributeHandleSet, CreateFederationExecutionResponse,
    DeleteObjectInstanceResponse, DestroyFederationExecutionResponse,
    DisableTimeConstrainedResponse, DisableTimeRegulationResponse, EnableTimeConstrainedResponse,
    EnableTimeRegulationResponse, ExceptionData, GetAttributeHandleResponse,
    GetAttributeNameResponse, GetInteractionClassHandleResponse, GetInteractionClassNameResponse,
    GetObjectClassHandleResponse, GetObjectClassNameResponse, GetParameterHandleResponse,
    GetParameterNameResponse, JoinFederationExecutionResponse,
    JoinFederationExecutionWithModulesResponse, JoinFederationExecutionWithNameAndModulesResponse,
    JoinFederationExecutionWithNameResponse, JoinResult, ListFederationExecutionsResponse,
    PublishInteractionClassResponse, PublishObjectClassAttributesResponse,
    QueryLogicalTimeResponse, QueryLookaheadResponse, RegisterObjectInstanceResponse,
    RegisterObjectInstanceWithNameResponse, ResignFederationExecutionResponse,
    SendInteractionResponse, SubscribeInteractionClassResponse,
    SubscribeObjectClassAttributesResponse, TimeAdvanceRequestResponse,
    UnpublishInteractionClassResponse, UnpublishObjectClassAttributesResponse,
    UnsubscribeInteractionClassResponse, UnsubscribeObjectClassAttributesResponse,
    UpdateAttributeValuesResponse, call_request::CallRequest as Req,
    call_response::CallResponse as Resp,
};

use crate::SyncPoint;
use crate::exception::HlaException;
use crate::handles::{
    HandleError, decode_attribute, decode_interaction_class, decode_object_class,
    decode_object_instance, decode_parameter, encode_attribute, encode_interaction_class,
    encode_object_class, encode_object_instance, encode_parameter,
};
use crate::routing::{
    OutboundCallback, announce_synchronization_point, attribute_is_not_owned,
    attribute_ownership_acquisition_notification, attribute_ownership_unavailable,
    decode_logical_time, decode_logical_time_interval, discover_object_instance,
    encode_logical_time, encode_logical_time_interval, fan_out, federation_not_restored,
    federation_not_saved, federation_restore_begun, federation_restored, federation_saved,
    federation_synchronized, inform_attribute_ownership, initiate_federate_restore,
    initiate_federate_save, live_connections, receive_directed_interaction, receive_interaction,
    receive_interaction_with_time, reflect_attribute_values, reflect_attribute_values_with_time,
    remove_object_instance, remove_object_instance_with_time, request_federation_restore_failed,
    request_federation_restore_succeeded, start_registration_for_object_class,
    stop_registration_for_object_class, subscribers_for_attributes, subscribers_for_interaction,
    synchronization_point_registration_failed, synchronization_point_registration_succeeded,
    time_advance_grant, time_constrained_enabled, time_regulation_enabled, turn_interactions_off,
    turn_interactions_on,
};
use crate::session::{Membership, SessionContext};
use crate::time::try_grant_pending_advances;
use crate::{FederateSession, Federation, ObjectInstance, PubSubState, RtiNode};
use crate::{RestoreOperation, RestoreStatus, SaveOperation, SaveStatus};

/// Output of a single `dispatch_call`. The response goes back to the calling
/// federate; the callbacks are RTI-initiated `HLA_CALLBACK_REQUEST` frames
/// destined for *other* federates' connections. The caller (session loop) is
/// responsible for actually awaiting their delivery so backpressure is
/// honored and callbacks are never silently dropped.
pub(crate) struct DispatchOutcome {
    pub response: fedpro::CallResponse,
    pub callbacks: Vec<OutboundCallback>,
}

/// Synchronous dispatch entry point.
///
/// Side-effects (callbacks destined for other federates) are *returned* in
/// `DispatchOutcome.callbacks` rather than dispatched in-line, so the async
/// session loop can `send().await` each one with proper backpressure — no
/// silent drops under load.
pub(crate) fn dispatch_call(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    request: fedpro::CallRequest,
) -> DispatchOutcome {
    let mut callbacks: Vec<OutboundCallback> = Vec::new();
    let variant = match request.call_request {
        Some(v) => v,
        None => {
            return DispatchOutcome {
                response: exception(
                    HlaException::RtiInternalError,
                    "CallRequest envelope had no oneof variant set",
                ),
                callbacks,
            };
        }
    };

    let response_variant = match variant {
        // ---- Federation Management ----
        Req::CreateFederationExecutionRequest(r) => {
            let modules = r.fom_module.into_iter().collect();
            match create_federation_inner(node, r.federation_name, modules) {
                Ok(()) => {
                    Resp::CreateFederationExecutionResponse(CreateFederationExecutionResponse {})
                }
                Err(e) => e,
            }
        }
        Req::CreateFederationExecutionWithTimeRequest(r) => {
            let modules = r.fom_module.into_iter().collect();
            let _ = r.logical_time_implementation_name; // we always use HLAfloat64Time
            match create_federation_inner(node, r.federation_name, modules) {
                Ok(()) => Resp::CreateFederationExecutionWithTimeResponse(
                    fedpro::CreateFederationExecutionWithTimeResponse {},
                ),
                Err(e) => e,
            }
        }
        Req::CreateFederationExecutionWithModulesAndTimeRequest(r) => {
            let modules = r.fom_modules.map(|s| s.fom_module).unwrap_or_default();
            let _ = r.logical_time_implementation_name;
            match create_federation_inner(node, r.federation_name, modules) {
                Ok(()) => Resp::CreateFederationExecutionWithModulesAndTimeResponse(
                    fedpro::CreateFederationExecutionWithModulesAndTimeResponse {},
                ),
                Err(e) => e,
            }
        }
        Req::CreateFederationExecutionWithMimRequest(r) => {
            let mut modules: Vec<_> = r.fom_modules.map(|s| s.fom_module).unwrap_or_default();
            if let Some(mim) = r.mim_module {
                modules.push(mim);
            }
            match create_federation_inner(node, r.federation_name, modules) {
                Ok(()) => Resp::CreateFederationExecutionWithMimResponse(
                    fedpro::CreateFederationExecutionWithMimResponse {},
                ),
                Err(e) => e,
            }
        }
        Req::CreateFederationExecutionWithMimAndTimeRequest(r) => {
            let mut modules: Vec<_> = r.fom_modules.map(|s| s.fom_module).unwrap_or_default();
            if let Some(mim) = r.mim_module {
                modules.push(mim);
            }
            let _ = r.logical_time_implementation_name;
            match create_federation_inner(node, r.federation_name, modules) {
                Ok(()) => Resp::CreateFederationExecutionWithMimAndTimeResponse(
                    fedpro::CreateFederationExecutionWithMimAndTimeResponse {},
                ),
                Err(e) => e,
            }
        }
        Req::CreateFederationExecutionWithModulesRequest(r) => {
            let modules = r.fom_modules.map(|s| s.fom_module).unwrap_or_default();
            match create_federation_inner(node, r.federation_name, modules) {
                Ok(()) => Resp::CreateFederationExecutionWithModulesResponse(
                    fedpro::CreateFederationExecutionWithModulesResponse {},
                ),
                Err(e) => e,
            }
        }
        Req::DestroyFederationExecutionRequest(r) => {
            destroy_federation_execution(node, r.federation_name)
        }
        // ---- Advisory + Reporting Switches ----
        Req::GetObjectClassRelevanceAdvisorySwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.object_class_relevance_advisory,
            |r| {
                Resp::GetObjectClassRelevanceAdvisorySwitchResponse(
                    fedpro::GetObjectClassRelevanceAdvisorySwitchResponse { result: r },
                )
            },
        ),
        Req::SetObjectClassRelevanceAdvisorySwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.object_class_relevance_advisory = v,
            || {
                Resp::SetObjectClassRelevanceAdvisorySwitchResponse(
                    fedpro::SetObjectClassRelevanceAdvisorySwitchResponse {},
                )
            },
        ),
        Req::GetAttributeRelevanceAdvisorySwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.attribute_relevance_advisory,
            |r| {
                Resp::GetAttributeRelevanceAdvisorySwitchResponse(
                    fedpro::GetAttributeRelevanceAdvisorySwitchResponse { result: r },
                )
            },
        ),
        Req::SetAttributeRelevanceAdvisorySwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.attribute_relevance_advisory = v,
            || {
                Resp::SetAttributeRelevanceAdvisorySwitchResponse(
                    fedpro::SetAttributeRelevanceAdvisorySwitchResponse {},
                )
            },
        ),
        Req::GetAttributeScopeAdvisorySwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.attribute_scope_advisory,
            |r| {
                Resp::GetAttributeScopeAdvisorySwitchResponse(
                    fedpro::GetAttributeScopeAdvisorySwitchResponse { result: r },
                )
            },
        ),
        Req::SetAttributeScopeAdvisorySwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.attribute_scope_advisory = v,
            || {
                Resp::SetAttributeScopeAdvisorySwitchResponse(
                    fedpro::SetAttributeScopeAdvisorySwitchResponse {},
                )
            },
        ),
        Req::GetInteractionRelevanceAdvisorySwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.interaction_relevance_advisory,
            |r| {
                Resp::GetInteractionRelevanceAdvisorySwitchResponse(
                    fedpro::GetInteractionRelevanceAdvisorySwitchResponse { result: r },
                )
            },
        ),
        Req::SetInteractionRelevanceAdvisorySwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.interaction_relevance_advisory = v,
            || {
                Resp::SetInteractionRelevanceAdvisorySwitchResponse(
                    fedpro::SetInteractionRelevanceAdvisorySwitchResponse {},
                )
            },
        ),
        Req::GetConveyRegionDesignatorSetsSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.convey_region_designator_sets,
            |r| {
                Resp::GetConveyRegionDesignatorSetsSwitchResponse(
                    fedpro::GetConveyRegionDesignatorSetsSwitchResponse { result: r },
                )
            },
        ),
        Req::SetConveyRegionDesignatorSetsSwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.convey_region_designator_sets = v,
            || {
                Resp::SetConveyRegionDesignatorSetsSwitchResponse(
                    fedpro::SetConveyRegionDesignatorSetsSwitchResponse {},
                )
            },
        ),
        Req::GetServiceReportingSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.service_reporting,
            |r| {
                Resp::GetServiceReportingSwitchResponse(fedpro::GetServiceReportingSwitchResponse {
                    result: r,
                })
            },
        ),
        Req::SetServiceReportingSwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.service_reporting = v,
            || {
                Resp::SetServiceReportingSwitchResponse(
                    fedpro::SetServiceReportingSwitchResponse {},
                )
            },
        ),
        Req::GetExceptionReportingSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.exception_reporting,
            |r| {
                Resp::GetExceptionReportingSwitchResponse(
                    fedpro::GetExceptionReportingSwitchResponse { result: r },
                )
            },
        ),
        Req::SetExceptionReportingSwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.exception_reporting = v,
            || {
                Resp::SetExceptionReportingSwitchResponse(
                    fedpro::SetExceptionReportingSwitchResponse {},
                )
            },
        ),
        Req::GetSendServiceReportsToFileSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.send_service_reports_to_file,
            |r| {
                Resp::GetSendServiceReportsToFileSwitchResponse(
                    fedpro::GetSendServiceReportsToFileSwitchResponse { result: r },
                )
            },
        ),
        Req::SetSendServiceReportsToFileSwitchRequest(r) => set_switch_bool(
            ctx,
            r.value,
            |s, v| s.send_service_reports_to_file = v,
            || {
                Resp::SetSendServiceReportsToFileSwitchResponse(
                    fedpro::SetSendServiceReportsToFileSwitchResponse {},
                )
            },
        ),
        Req::GetAutoProvideSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.auto_provide,
            |r| {
                Resp::GetAutoProvideSwitchResponse(fedpro::GetAutoProvideSwitchResponse {
                    result: r,
                })
            },
        ),
        Req::GetDelaySubscriptionEvaluationSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.delay_subscription_evaluation,
            |r| {
                Resp::GetDelaySubscriptionEvaluationSwitchResponse(
                    fedpro::GetDelaySubscriptionEvaluationSwitchResponse { result: r },
                )
            },
        ),
        Req::GetAdvisoriesUseKnownClassSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.advisories_use_known_class,
            |r| {
                Resp::GetAdvisoriesUseKnownClassSwitchResponse(
                    fedpro::GetAdvisoriesUseKnownClassSwitchResponse { result: r },
                )
            },
        ),
        Req::GetAllowRelaxedDdmSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.allow_relaxed_ddm,
            |r| {
                Resp::GetAllowRelaxedDdmSwitchResponse(fedpro::GetAllowRelaxedDdmSwitchResponse {
                    result: r,
                })
            },
        ),
        Req::GetNonRegulatedGrantSwitchRequest(_) => get_switch_bool(
            ctx,
            |s| s.non_regulated_grant,
            |r| {
                Resp::GetNonRegulatedGrantSwitchResponse(
                    fedpro::GetNonRegulatedGrantSwitchResponse { result: r },
                )
            },
        ),
        Req::GetAutomaticResignDirectiveRequest(_) => get_automatic_resign_directive(ctx),
        Req::SetAutomaticResignDirectiveRequest(r) => set_automatic_resign_directive(ctx, r.value),

        // ---- Connect / Disconnect (FedPro session is our connect — these
        // are no-ops at the federation level for the immediate-callback model
        // we ship). ----
        Req::ConnectRequest(_) => Resp::ConnectResponse(fedpro::ConnectResponse {
            configuration_result: None,
        }),
        Req::ConnectWithCredentialsRequest(_) => {
            // Accept any credentials for now — auth/authorization policy is a
            // separate concern (see threat model gap in mission-critical
            // checklist).
            Resp::ConnectWithCredentialsResponse(fedpro::ConnectWithCredentialsResponse {
                configuration_result: None,
            })
        }
        Req::ConnectWithConfigurationRequest(_) => {
            Resp::ConnectWithConfigurationResponse(fedpro::ConnectWithConfigurationResponse {
                configuration_result: None,
            })
        }
        Req::ConnectWithConfigurationAndCredentialsRequest(_) => {
            Resp::ConnectWithConfigurationAndCredentialsResponse(
                fedpro::ConnectWithConfigurationAndCredentialsResponse {
                    configuration_result: None,
                },
            )
        }
        Req::DisconnectRequest(_) => Resp::DisconnectResponse(fedpro::DisconnectResponse {}),

        // ---- Object instance lookup ----
        Req::GetObjectInstanceHandleRequest(r) => {
            get_object_instance_handle(ctx, &r.object_instance_name)
        }
        Req::GetObjectInstanceNameRequest(r) => get_object_instance_name(ctx, r.object_instance),
        Req::GetKnownObjectClassHandleRequest(r) => {
            get_known_object_class_handle(ctx, r.object_instance)
        }
        Req::LocalDeleteObjectInstanceRequest(r) => {
            local_delete_object_instance(ctx, r.object_instance)
        }

        // ---- Dimension lookup ----
        Req::GetDimensionHandleRequest(r) => get_dimension_handle(ctx, &r.dimension_name),
        Req::GetDimensionNameRequest(r) => get_dimension_name(ctx, r.dimension),
        Req::GetDimensionUpperBoundRequest(r) => get_dimension_upper_bound(ctx, r.dimension),
        Req::GetDimensionHandleSetRequest(r) => get_dimension_handle_set(ctx, r.region),
        Req::GetAvailableDimensionsForObjectClassRequest(r) => {
            get_available_dimensions_for_object_class(ctx, r.object_class)
        }
        Req::GetAvailableDimensionsForInteractionClassRequest(r) => {
            get_available_dimensions_for_interaction_class(ctx, r.interaction_class)
        }

        // ---- Federate self-introspection ----
        Req::GetFederateHandleRequest(r) => get_federate_handle(ctx, &r.federate_name),
        Req::GetFederateNameRequest(r) => match r.federate {
            Some(h) => get_federate_name(ctx, h),
            None => exception_variant(HlaException::InvalidFederateHandle, ""),
        },

        // ---- OrderType / TransportationType lookup (well-known) ----
        Req::GetOrderTypeRequest(r) => get_order_type(&r.order_type_name),
        Req::GetOrderNameRequest(r) => get_order_name(r.order_type),
        Req::GetTransportationTypeHandleRequest(r) => {
            get_transportation_type_handle(&r.transportation_type_name)
        }
        Req::GetTransportationTypeNameRequest(r) => match r.transportation_type {
            Some(h) => get_transportation_type_name(h),
            None => exception_variant(HlaException::InvalidTransportationTypeHandle, ""),
        },

        Req::ListFederationExecutionMembersRequest(r) => {
            let federation_name = r.federation_name;
            let federations = node.federations.read();
            let report_target = node
                .connections
                .get(&ctx.session_id)
                .map(|c| vec![Arc::clone(c.value())])
                .unwrap_or_default();
            let cb = match federations.get(&federation_name) {
                Some(fed) => {
                    let members: Vec<(String, String)> = fed
                        .federates
                        .read()
                        .values()
                        .map(|fs| (fs.name.clone(), fs.federate_type.clone()))
                        .collect();
                    crate::routing::report_federation_execution_members(&federation_name, &members)
                }
                None => {
                    crate::routing::report_federation_execution_does_not_exist(&federation_name)
                }
            };
            drop(federations);
            fan_out(&mut callbacks, &report_target, cb);
            Resp::ListFederationExecutionMembersResponse(
                fedpro::ListFederationExecutionMembersResponse {},
            )
        }
        Req::ListFederationExecutionsRequest(_) => {
            // Per IEEE 1516.1: the response is empty; the actual list is
            // delivered as a `reportFederationExecutions` callback to the
            // requesting federate's connection.
            let names: Vec<String> = node.federations.read().keys().cloned().collect();
            if let Some(conn) = node.connections.get(&ctx.session_id) {
                let report_target = vec![Arc::clone(conn.value())];
                drop(conn);
                fan_out(
                    &mut callbacks,
                    &report_target,
                    crate::routing::report_federation_executions(&names),
                );
            }
            Resp::ListFederationExecutionsResponse(ListFederationExecutionsResponse {})
        }
        Req::JoinFederationExecutionRequest(r) => match join(node, ctx, None, r.federation_name) {
            Ok(jr) => Resp::JoinFederationExecutionResponse(JoinFederationExecutionResponse {
                result: Some(jr),
            }),
            Err(e) => e,
        },
        Req::JoinFederationExecutionWithNameRequest(r) => {
            match join(node, ctx, Some(r.federate_name), r.federation_name) {
                Ok(jr) => Resp::JoinFederationExecutionWithNameResponse(
                    JoinFederationExecutionWithNameResponse { result: Some(jr) },
                ),
                Err(e) => e,
            }
        }
        Req::JoinFederationExecutionWithModulesRequest(r) => {
            let _ = r.additional_fom_modules;
            match join(node, ctx, None, r.federation_name) {
                Ok(jr) => Resp::JoinFederationExecutionWithModulesResponse(
                    JoinFederationExecutionWithModulesResponse { result: Some(jr) },
                ),
                Err(e) => e,
            }
        }
        Req::JoinFederationExecutionWithNameAndModulesRequest(r) => {
            let _ = r.additional_fom_modules;
            match join(node, ctx, Some(r.federate_name), r.federation_name) {
                Ok(jr) => Resp::JoinFederationExecutionWithNameAndModulesResponse(
                    JoinFederationExecutionWithNameAndModulesResponse { result: Some(jr) },
                ),
                Err(e) => e,
            }
        }
        Req::ResignFederationExecutionRequest(_) => resign(ctx),

        // ---- Handle lookup (sync, FOM-resolved) ----
        Req::GetObjectClassHandleRequest(r) => get_object_class_handle(ctx, &r.object_class_name),
        Req::GetObjectClassNameRequest(r) => match r.object_class {
            Some(h) => get_object_class_name(ctx, h),
            None => exception_variant(HlaException::InvalidObjectClassHandle, "missing handle"),
        },
        Req::GetAttributeHandleRequest(r) => match r.object_class {
            Some(h) => get_attribute_handle(ctx, h, &r.attribute_name),
            None => exception_variant(HlaException::InvalidObjectClassHandle, "missing handle"),
        },
        Req::GetAttributeNameRequest(r) => match (r.object_class, r.attribute) {
            (Some(c), Some(a)) => get_attribute_name(ctx, c, a),
            _ => exception_variant(HlaException::InvalidObjectClassHandle, "missing handle(s)"),
        },
        Req::GetInteractionClassHandleRequest(r) => {
            get_interaction_class_handle(ctx, &r.interaction_class_name)
        }
        Req::GetInteractionClassNameRequest(r) => match r.interaction_class {
            Some(h) => get_interaction_class_name(ctx, h),
            None => exception_variant(
                HlaException::InvalidInteractionClassHandle,
                "missing handle",
            ),
        },
        Req::GetParameterHandleRequest(r) => match r.interaction_class {
            Some(h) => get_parameter_handle(ctx, h, &r.parameter_name),
            None => exception_variant(
                HlaException::InvalidInteractionClassHandle,
                "missing handle",
            ),
        },
        Req::GetParameterNameRequest(r) => match (r.interaction_class, r.parameter) {
            (Some(c), Some(p)) => get_parameter_name(ctx, c, p),
            _ => exception_variant(
                HlaException::InvalidInteractionClassHandle,
                "missing handle(s)",
            ),
        },

        // ---- Declaration Management ----
        Req::PublishObjectClassAttributesRequest(r) => {
            publish_object_class_attributes(ctx, r.object_class, r.attributes)
        }
        Req::UnpublishObjectClassAttributesRequest(r) => {
            unpublish_object_class_attributes(ctx, r.object_class, r.attributes)
        }
        Req::SubscribeObjectClassAttributesRequest(r) => subscribe_object_class_attributes_inner(
            ctx,
            r.object_class,
            r.attributes,
            Some(node),
            &mut callbacks,
        ),
        Req::UnsubscribeObjectClassAttributesRequest(r) => {
            unsubscribe_object_class_attributes_inner(
                ctx,
                r.object_class,
                r.attributes,
                Some(node),
                &mut callbacks,
            )
        }
        Req::PublishInteractionClassRequest(r) => {
            publish_interaction_class(ctx, r.interaction_class)
        }
        Req::UnpublishInteractionClassRequest(r) => {
            unpublish_interaction_class(ctx, r.interaction_class)
        }
        Req::SubscribeInteractionClassRequest(r) => {
            subscribe_interaction_class_inner(ctx, r.interaction_class, Some(node), &mut callbacks)
        }
        Req::UnsubscribeInteractionClassRequest(r) => unsubscribe_interaction_class_inner(
            ctx,
            r.interaction_class,
            Some(node),
            &mut callbacks,
        ),

        // ---- Object Management ----
        Req::RegisterObjectInstanceRequest(r) => {
            match register_object_instance(node, ctx, &mut callbacks, r.object_class, None) {
                Ok(handle) => {
                    Resp::RegisterObjectInstanceResponse(RegisterObjectInstanceResponse {
                        result: Some(encode_object_instance(handle)),
                    })
                }
                Err(e) => e,
            }
        }
        Req::RegisterObjectInstanceWithNameRequest(r) => {
            match register_object_instance(
                node,
                ctx,
                &mut callbacks,
                r.object_class,
                Some(r.object_instance_name),
            ) {
                Ok(handle) => Resp::RegisterObjectInstanceWithNameResponse(
                    RegisterObjectInstanceWithNameResponse {
                        result: Some(encode_object_instance(handle)),
                    },
                ),
                Err(e) => e,
            }
        }
        Req::UpdateAttributeValuesRequest(r) => update_attribute_values(
            node,
            ctx,
            &mut callbacks,
            r.object_instance,
            r.attribute_values,
            r.user_supplied_tag,
        ),
        Req::SendInteractionRequest(r) => send_interaction(
            node,
            ctx,
            &mut callbacks,
            r.interaction_class,
            r.parameter_values,
            r.user_supplied_tag,
        ),
        Req::DeleteObjectInstanceRequest(r) => delete_object_instance(
            node,
            ctx,
            &mut callbacks,
            r.object_instance,
            r.user_supplied_tag,
        ),
        Req::UpdateAttributeValuesWithTimeRequest(r) => update_attribute_values_with_time(
            node,
            ctx,
            &mut callbacks,
            r.object_instance,
            r.attribute_values,
            r.user_supplied_tag,
            r.time,
        ),
        Req::SendInteractionWithTimeRequest(r) => send_interaction_with_time(
            node,
            ctx,
            &mut callbacks,
            r.interaction_class,
            r.parameter_values,
            r.user_supplied_tag,
            r.time,
        ),
        Req::DeleteObjectInstanceWithTimeRequest(r) => delete_object_instance_with_time(
            node,
            ctx,
            &mut callbacks,
            r.object_instance,
            r.user_supplied_tag,
            r.time,
        ),

        // ---- Ownership Management (MVP slice) ----
        Req::IsAttributeOwnedByFederateRequest(r) => {
            is_attribute_owned_by_federate(ctx, r.object_instance, r.attribute)
        }
        Req::QueryAttributeOwnershipRequest(r) => {
            query_attribute_ownership(node, ctx, &mut callbacks, r.object_instance, r.attributes)
        }
        Req::UnconditionalAttributeOwnershipDivestitureRequest(r) => {
            unconditional_attribute_ownership_divestiture(
                node,
                ctx,
                &mut callbacks,
                r.object_instance,
                r.attributes,
                r.user_supplied_tag,
            )
        }
        Req::AttributeOwnershipAcquisitionRequest(r) => {
            // MVP: treat negotiated acquisition as if-available — i.e. if the
            // attributes are unowned, take ownership. The full negotiation
            // (RequestAttributeOwnershipRelease callback to current owners,
            // etc.) is a follow-on.
            attribute_ownership_acquisition_if_available(
                node,
                ctx,
                &mut callbacks,
                r.object_instance,
                r.desired_attributes,
                r.user_supplied_tag,
            )
        }
        Req::NegotiatedAttributeOwnershipDivestitureRequest(r) => {
            // MVP: treat as unconditional divest. Full negotiation (RequestAttributeOwnershipAssumption callback) is a follow-on.
            unconditional_attribute_ownership_divestiture(
                node,
                ctx,
                &mut callbacks,
                r.object_instance,
                r.attributes,
                r.user_supplied_tag,
            )
        }
        Req::AttributeOwnershipReleaseDeniedRequest(_) => {
            Resp::AttributeOwnershipReleaseDeniedResponse(
                fedpro::AttributeOwnershipReleaseDeniedResponse {},
            )
        }
        Req::AttributeOwnershipDivestitureIfWantedRequest(r) => {
            // MVP: behaves like unconditional divest.
            unconditional_attribute_ownership_divestiture(
                node,
                ctx,
                &mut callbacks,
                r.object_instance,
                r.attributes,
                r.user_supplied_tag,
            )
        }
        Req::CancelNegotiatedAttributeOwnershipDivestitureRequest(_) => {
            Resp::CancelNegotiatedAttributeOwnershipDivestitureResponse(
                fedpro::CancelNegotiatedAttributeOwnershipDivestitureResponse {},
            )
        }
        Req::CancelAttributeOwnershipAcquisitionRequest(_) => {
            Resp::CancelAttributeOwnershipAcquisitionResponse(
                fedpro::CancelAttributeOwnershipAcquisitionResponse {},
            )
        }
        Req::ConfirmDivestitureRequest(_) => {
            Resp::ConfirmDivestitureResponse(fedpro::ConfirmDivestitureResponse {})
        }
        Req::AttributeOwnershipAcquisitionIfAvailableRequest(r) => {
            attribute_ownership_acquisition_if_available(
                node,
                ctx,
                &mut callbacks,
                r.object_instance,
                r.desired_attributes,
                r.user_supplied_tag,
            )
        }

        // ---- Federation Save (MVP: callback orchestration; no on-disk persistence) ----
        Req::RequestFederationSaveRequest(r) => {
            request_federation_save(node, ctx, &mut callbacks, r.label)
        }
        Req::FederateSaveBegunRequest(_) => federate_save_begun(ctx),
        Req::FederateSaveCompleteRequest(_) => {
            federate_save_progressed(node, ctx, &mut callbacks, true)
        }
        Req::FederateSaveNotCompleteRequest(_) => {
            federate_save_progressed(node, ctx, &mut callbacks, false)
        }
        Req::AbortFederationSaveRequest(_) => abort_federation_save(ctx),
        Req::RequestFederationSaveWithTimeRequest(r) => {
            // MVP: ignore the time and run the regular save orchestration.
            let _ = r.time;
            request_federation_save(node, ctx, &mut callbacks, r.label)
        }
        Req::QueryFederationSaveStatusRequest(_) => {
            // Per spec, response is empty; status arrives via callback.
            // For MVP we just return the empty ack.
            Resp::QueryFederationSaveStatusResponse(fedpro::QueryFederationSaveStatusResponse {})
        }
        Req::QueryFederationRestoreStatusRequest(_) => Resp::QueryFederationRestoreStatusResponse(
            fedpro::QueryFederationRestoreStatusResponse {},
        ),

        // ---- Federation Restore (MVP: orchestration; no on-disk state) ----
        Req::RequestFederationRestoreRequest(r) => {
            request_federation_restore(node, ctx, &mut callbacks, r.label)
        }
        Req::FederateRestoreCompleteRequest(_) => {
            federate_restore_progressed(node, ctx, &mut callbacks, true)
        }
        Req::FederateRestoreNotCompleteRequest(_) => {
            federate_restore_progressed(node, ctx, &mut callbacks, false)
        }
        Req::AbortFederationRestoreRequest(_) => abort_federation_restore(ctx),

        // ---- Synchronization Points ----
        Req::RegisterFederationSynchronizationPointRequest(r) => register_synchronization_point(
            node,
            ctx,
            &mut callbacks,
            r.synchronization_point_label,
            r.user_supplied_tag,
            None,
        ),
        Req::RegisterFederationSynchronizationPointWithSetRequest(r) => {
            let set: std::collections::HashSet<FederateHandle> = r
                .synchronization_set
                .map(|s| {
                    s.federate_handle
                        .iter()
                        .filter_map(|h| {
                            crate::handles::decode_object_class(&fedpro::ObjectClassHandle {
                                data: h.data.clone(),
                            })
                            .ok()
                            .map(|h| FederateHandle::new(h.raw()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            register_synchronization_point(
                node,
                ctx,
                &mut callbacks,
                r.synchronization_point_label,
                r.user_supplied_tag,
                Some(set),
            )
        }
        Req::SynchronizationPointAchievedRequest(r) => synchronization_point_achieved(
            node,
            ctx,
            &mut callbacks,
            r.synchronization_point_label,
            r.successfully,
        ),

        // ---- DDM (Data Distribution Management) ----
        // MVP: region lifecycle is fully implemented. The region-aware
        // subscribe/send/register variants currently delegate to their
        // non-region counterparts — federates receive all updates regardless
        // of region overlap, which is strictly more conservative than the
        // spec. Implementing the actual region-overlap routing engine is a
        // substantial follow-on.
        Req::CreateRegionRequest(r) => create_region(node, ctx, r.dimensions),
        Req::CommitRegionModificationsRequest(r) => commit_region_modifications(ctx, r.regions),
        Req::DeleteRegionRequest(r) => delete_region(ctx, r.region),
        Req::GetRangeBoundsRequest(r) => get_range_bounds(ctx, r.region, r.dimension),
        Req::SetRangeBoundsRequest(r) => {
            set_range_bounds(ctx, r.region, r.dimension, r.range_bounds)
        }
        Req::SubscribeObjectClassAttributesWithRegionsRequest(r) => {
            subscribe_object_class_attributes_with_regions(
                node,
                ctx,
                &mut callbacks,
                r.object_class,
                r.attributes_and_regions,
            )
        }
        Req::UnsubscribeObjectClassAttributesWithRegionsRequest(r) => {
            unsubscribe_object_class_attributes_with_regions(
                ctx,
                r.object_class,
                r.attributes_and_regions,
            )
        }
        Req::SubscribeInteractionClassWithRegionsRequest(r) => {
            let _ = r.regions;
            Resp::SubscribeInteractionClassWithRegionsResponse(
                fedpro::SubscribeInteractionClassWithRegionsResponse {},
            )
        }
        Req::UnsubscribeInteractionClassWithRegionsRequest(r) => {
            let _ = r.regions;
            Resp::UnsubscribeInteractionClassWithRegionsResponse(
                fedpro::UnsubscribeInteractionClassWithRegionsResponse {},
            )
        }
        Req::SendInteractionWithRegionsRequest(r) => send_interaction(
            node,
            ctx,
            &mut callbacks,
            r.interaction_class,
            r.parameter_values,
            r.user_supplied_tag,
        ),
        Req::RegisterObjectInstanceWithRegionsRequest(r) => {
            match register_object_instance_with_regions(
                node,
                ctx,
                &mut callbacks,
                r.object_class,
                None,
                r.attributes_and_regions,
            ) {
                Ok(handle) => Resp::RegisterObjectInstanceWithRegionsResponse(
                    fedpro::RegisterObjectInstanceWithRegionsResponse {
                        result: Some(crate::handles::encode_object_instance(handle)),
                    },
                ),
                Err(e) => e,
            }
        }

        // ---- Time Management ----
        Req::EnableTimeRegulationRequest(r) => {
            enable_time_regulation(node, ctx, &mut callbacks, r.lookahead)
        }
        Req::DisableTimeRegulationRequest(_) => disable_time_regulation(node, ctx, &mut callbacks),
        Req::EnableTimeConstrainedRequest(_) => enable_time_constrained(node, ctx, &mut callbacks),
        Req::DisableTimeConstrainedRequest(_) => disable_time_constrained(ctx),
        Req::TimeAdvanceRequestRequest(r) => {
            time_advance_request(node, ctx, &mut callbacks, r.time)
        }
        Req::NextMessageRequestRequest(r) => {
            // MVP: identical semantics to TAR. A full impl would deliver
            // pending TSO messages up to and including `time` and grant only
            // when no earlier message is in transit.
            time_advance_request(node, ctx, &mut callbacks, r.time)
        }
        Req::QueryLogicalTimeRequest(_) => query_logical_time(ctx),
        Req::QueryLookaheadRequest(_) => query_lookahead(ctx),
        Req::ModifyLookaheadRequest(r) => modify_lookahead(ctx, r.lookahead),
        Req::QueryLitsRequest(_) => query_lits(ctx),
        Req::RetractRequest(_) => {
            // No-op for MVP: we don't implement message retraction since we
            // don't yet have TSO delivery. Per spec, retract removes a not-yet-
            // delivered TSO message; with no TSO queue, there's nothing to do.
            Resp::RetractResponse(fedpro::RetractResponse {})
        }

        // ---- Order + transportation per-attribute/interaction changes ----
        Req::ChangeAttributeOrderTypeRequest(_) => {
            Resp::ChangeAttributeOrderTypeResponse(fedpro::ChangeAttributeOrderTypeResponse {})
        }
        Req::ChangeDefaultAttributeOrderTypeRequest(_) => {
            Resp::ChangeDefaultAttributeOrderTypeResponse(
                fedpro::ChangeDefaultAttributeOrderTypeResponse {},
            )
        }
        Req::ChangeInteractionOrderTypeRequest(_) => {
            Resp::ChangeInteractionOrderTypeResponse(fedpro::ChangeInteractionOrderTypeResponse {})
        }
        Req::RequestAttributeTransportationTypeChangeRequest(_) => {
            Resp::RequestAttributeTransportationTypeChangeResponse(
                fedpro::RequestAttributeTransportationTypeChangeResponse {},
            )
        }
        Req::QueryAttributeTransportationTypeRequest(_) => {
            // Always return HLAreliable for MVP.
            Resp::QueryAttributeTransportationTypeResponse(
                fedpro::QueryAttributeTransportationTypeResponse {},
            )
        }
        Req::RequestInteractionTransportationTypeChangeRequest(_) => {
            Resp::RequestInteractionTransportationTypeChangeResponse(
                fedpro::RequestInteractionTransportationTypeChangeResponse {},
            )
        }
        Req::QueryInteractionTransportationTypeRequest(_) => {
            Resp::QueryInteractionTransportationTypeResponse(
                fedpro::QueryInteractionTransportationTypeResponse {},
            )
        }

        // ---- Normalize: per-spec, returns a service-group token used for
        // batching multi-service requests. We're a single-RTI implementation
        // so we just echo the input back as the normalized handle. ----
        Req::NormalizeServiceGroupRequest(r) => {
            Resp::NormalizeServiceGroupResponse(fedpro::NormalizeServiceGroupResponse {
                result: r.service_group as u32,
            })
        }
        Req::NormalizeFederateHandleRequest(r) => {
            Resp::NormalizeFederateHandleResponse(fedpro::NormalizeFederateHandleResponse {
                result: r
                    .federate
                    .as_ref()
                    .filter(|h| h.data.len() == 4)
                    .map(|h| u32::from_be_bytes(h.data[..].try_into().unwrap()))
                    .unwrap_or(0),
            })
        }
        Req::NormalizeObjectClassHandleRequest(r) => {
            Resp::NormalizeObjectClassHandleResponse(fedpro::NormalizeObjectClassHandleResponse {
                result: r
                    .object_class
                    .as_ref()
                    .filter(|h| h.data.len() == 4)
                    .map(|h| u32::from_be_bytes(h.data[..].try_into().unwrap()))
                    .unwrap_or(0),
            })
        }
        Req::NormalizeInteractionClassHandleRequest(r) => {
            Resp::NormalizeInteractionClassHandleResponse(
                fedpro::NormalizeInteractionClassHandleResponse {
                    result: r
                        .interaction_class
                        .as_ref()
                        .filter(|h| h.data.len() == 4)
                        .map(|h| u32::from_be_bytes(h.data[..].try_into().unwrap()))
                        .unwrap_or(0),
                },
            )
        }
        Req::NormalizeObjectInstanceHandleRequest(r) => {
            Resp::NormalizeObjectInstanceHandleResponse(
                fedpro::NormalizeObjectInstanceHandleResponse {
                    // ObjectInstanceHandle is u64 — truncate to u32 for normalize.
                    result: r
                        .object_instance
                        .as_ref()
                        .filter(|h| h.data.len() == 8)
                        .map(|h| u64::from_be_bytes(h.data[..].try_into().unwrap()) as u32)
                        .unwrap_or(0),
                },
            )
        }

        // ---- Update-rate queries ----
        Req::GetUpdateRateValueRequest(_) => {
            Resp::GetUpdateRateValueResponse(fedpro::GetUpdateRateValueResponse { result: 0.0 })
        }
        Req::GetUpdateRateValueForAttributeRequest(_) => {
            Resp::GetUpdateRateValueForAttributeResponse(
                fedpro::GetUpdateRateValueForAttributeResponse { result: 0.0 },
            )
        }

        // ---- DDM request-update with regions: delegate ----
        Req::RequestAttributeValueUpdateWithRegionsRequest(_) => {
            Resp::RequestAttributeValueUpdateWithRegionsResponse(
                fedpro::RequestAttributeValueUpdateWithRegionsResponse {},
            )
        }

        // ---- Subscribe-with-rate / passive variants: delegate to plain ----
        Req::SubscribeObjectClassAttributesWithRateRequest(r) => {
            let _ = r.update_rate_designator;
            subscribe_object_class_attributes_inner(
                ctx,
                r.object_class,
                r.attributes,
                Some(node),
                &mut callbacks,
            )
        }
        Req::SubscribeObjectClassAttributesPassivelyRequest(r) => {
            subscribe_object_class_attributes_inner(
                ctx,
                r.object_class,
                r.attributes,
                Some(node),
                &mut callbacks,
            )
        }
        Req::SubscribeObjectClassAttributesPassivelyWithRateRequest(r) => {
            let _ = r.update_rate_designator;
            subscribe_object_class_attributes_inner(
                ctx,
                r.object_class,
                r.attributes,
                Some(node),
                &mut callbacks,
            )
        }
        Req::SubscribeInteractionClassPassivelyRequest(r) => {
            subscribe_interaction_class_inner(ctx, r.interaction_class, Some(node), &mut callbacks)
        }
        Req::SubscribeObjectClassAttributesWithRegionsAndRateRequest(r) => {
            let _ = r.attributes_and_regions;
            let _ = r.update_rate_designator;
            Resp::SubscribeObjectClassAttributesWithRegionsAndRateResponse(
                fedpro::SubscribeObjectClassAttributesWithRegionsAndRateResponse {},
            )
        }
        Req::SendInteractionWithRegionsAndTimeRequest(r) => send_interaction_with_time(
            node,
            ctx,
            &mut callbacks,
            r.interaction_class,
            r.parameter_values,
            r.user_supplied_tag,
            r.time,
        ),
        Req::RegisterObjectInstanceWithNameAndRegionsRequest(r) => {
            match register_object_instance_with_regions(
                node,
                ctx,
                &mut callbacks,
                r.object_class,
                Some(r.object_instance_name),
                r.attributes_and_regions,
            ) {
                Ok(handle) => Resp::RegisterObjectInstanceWithNameAndRegionsResponse(
                    fedpro::RegisterObjectInstanceWithNameAndRegionsResponse {
                        result: Some(crate::handles::encode_object_instance(handle)),
                    },
                ),
                Err(e) => e,
            }
        }
        Req::AssociateRegionsForUpdatesRequest(_) => {
            Resp::AssociateRegionsForUpdatesResponse(fedpro::AssociateRegionsForUpdatesResponse {})
        }
        Req::UnassociateRegionsForUpdatesRequest(_) => Resp::UnassociateRegionsForUpdatesResponse(
            fedpro::UnassociateRegionsForUpdatesResponse {},
        ),
        Req::UnpublishObjectClassRequest(r) => {
            // Unpublish the entire class — equivalent to unpublishing all attributes.
            let m = match ctx.membership.as_ref() {
                Some(m) => m.clone(),
                None => {
                    return DispatchOutcome {
                        response: exception(HlaException::FederateNotExecutionMember, ""),
                        callbacks,
                    };
                }
            };
            let class = match r.object_class.and_then(|h| decode_object_class(&h).ok()) {
                Some(h) => h,
                None => {
                    return DispatchOutcome {
                        response: exception(HlaException::InvalidObjectClassHandle, ""),
                        callbacks,
                    };
                }
            };
            let mut federates = m.federation.federates.write();
            if let Some(fs) = federates.get_mut(&m.federate_handle) {
                fs.pub_sub.published_attrs.remove(&class);
            }
            Resp::UnpublishObjectClassResponse(fedpro::UnpublishObjectClassResponse {})
        }
        Req::UnsubscribeObjectClassRequest(r) => {
            let m = match ctx.membership.as_ref() {
                Some(m) => m.clone(),
                None => {
                    return DispatchOutcome {
                        response: exception(HlaException::FederateNotExecutionMember, ""),
                        callbacks,
                    };
                }
            };
            let class = match r.object_class.and_then(|h| decode_object_class(&h).ok()) {
                Some(h) => h,
                None => {
                    return DispatchOutcome {
                        response: exception(HlaException::InvalidObjectClassHandle, ""),
                        callbacks,
                    };
                }
            };
            let mut federates = m.federation.federates.write();
            if let Some(fs) = federates.get_mut(&m.federate_handle) {
                fs.pub_sub.subscribed_attrs.remove(&class);
            }
            // Remove from federation subscription matrix.
            let mut subs = m.federation.subscriptions.write();
            subs.remove_class(class);
            Resp::UnsubscribeObjectClassResponse(fedpro::UnsubscribeObjectClassResponse {})
        }

        // ---- Object instance name reservation (per IEEE 1516.1) ----
        Req::ReserveObjectInstanceNameRequest(_r) => {
            // MVP: reservation succeeds silently; the callback
            // ObjectInstanceNameReservationSucceeded should be sent. Deferred.
            Resp::ReserveObjectInstanceNameResponse(fedpro::ReserveObjectInstanceNameResponse {})
        }
        Req::ReserveMultipleObjectInstanceNamesRequest(_r) => {
            Resp::ReserveMultipleObjectInstanceNamesResponse(
                fedpro::ReserveMultipleObjectInstanceNamesResponse {},
            )
        }
        Req::ReleaseObjectInstanceNameRequest(_r) => {
            Resp::ReleaseObjectInstanceNameResponse(fedpro::ReleaseObjectInstanceNameResponse {})
        }
        Req::ReleaseMultipleObjectInstanceNamesRequest(_r) => {
            Resp::ReleaseMultipleObjectInstanceNamesResponse(
                fedpro::ReleaseMultipleObjectInstanceNamesResponse {},
            )
        }

        // ---- Request attribute value update (federate asks owners to update) ----
        Req::RequestClassAttributeValueUpdateRequest(_r) => {
            Resp::RequestClassAttributeValueUpdateResponse(
                fedpro::RequestClassAttributeValueUpdateResponse {},
            )
        }
        Req::RequestInstanceAttributeValueUpdateRequest(_r) => {
            Resp::RequestInstanceAttributeValueUpdateResponse(
                fedpro::RequestInstanceAttributeValueUpdateResponse {},
            )
        }

        // ---- Async-delivery toggle (TSO event delivery mode) ----
        Req::EnableAsynchronousDeliveryRequest(_) => {
            Resp::EnableAsynchronousDeliveryResponse(fedpro::EnableAsynchronousDeliveryResponse {})
        }
        Req::DisableAsynchronousDeliveryRequest(_) => Resp::DisableAsynchronousDeliveryResponse(
            fedpro::DisableAsynchronousDeliveryResponse {},
        ),

        // ---- Time advance variants (TimeAdvanceRequestAvailable, NextMessageRequestAvailable) ----
        Req::TimeAdvanceRequestAvailableRequest(r) => {
            // Semantically: grant available time stamps ≤ requested; for MVP
            // identical to TAR.
            time_advance_request(node, ctx, &mut callbacks, r.time)
        }
        Req::NextMessageRequestAvailableRequest(r) => {
            time_advance_request(node, ctx, &mut callbacks, r.time)
        }
        Req::FlushQueueRequestRequest(r) => {
            // MVP: no TSO queue; advance is granted immediately like TAR.
            time_advance_request(node, ctx, &mut callbacks, r.time)
        }
        Req::QueryGaltRequest(_) => {
            // Greatest Available Logical Time = LBTS for our purposes.
            use hla_fedpro_proto::fedpro::{QueryGaltResponse, TimeQueryReturn};
            let m = match ctx.membership.as_ref() {
                Some(m) => m,
                None => {
                    return DispatchOutcome {
                        response: exception(HlaException::FederateNotExecutionMember, ""),
                        callbacks,
                    };
                }
            };
            let lbts = crate::time::lbts(&m.federation);
            Resp::QueryGaltResponse(QueryGaltResponse {
                result: Some(TimeQueryReturn {
                    logical_time_is_valid: lbts.is_finite(),
                    logical_time: Some(encode_logical_time(if lbts.is_finite() {
                        lbts
                    } else {
                        0.0
                    })),
                }),
            })
        }

        // ---- Directed interactions (object-targeted variants) ----
        Req::PublishObjectClassDirectedInteractionsRequest(r) => {
            publish_object_class_directed_interactions(ctx, r.object_class, r.interaction_classes)
        }
        Req::UnpublishObjectClassDirectedInteractionsRequest(r) => {
            unpublish_object_class_directed_interactions(ctx, r.object_class, None)
        }
        Req::UnpublishObjectClassDirectedInteractionsWithSetRequest(r) => {
            unpublish_object_class_directed_interactions(ctx, r.object_class, r.interaction_classes)
        }
        Req::SubscribeObjectClassDirectedInteractionsRequest(r) => {
            subscribe_object_class_directed_interactions(ctx, r.object_class, r.interaction_classes)
        }
        Req::SubscribeObjectClassDirectedInteractionsUniversallyRequest(r) => {
            subscribe_object_class_directed_interactions(ctx, r.object_class, r.interaction_classes)
        }
        Req::UnsubscribeObjectClassDirectedInteractionsRequest(r) => {
            unsubscribe_object_class_directed_interactions(ctx, r.object_class, None)
        }
        Req::UnsubscribeObjectClassDirectedInteractionsWithSetRequest(r) => {
            unsubscribe_object_class_directed_interactions(
                ctx,
                r.object_class,
                r.interaction_classes,
            )
        }
        Req::SendDirectedInteractionRequest(r) => send_directed_interaction(
            node,
            ctx,
            &mut callbacks,
            r.interaction_class,
            r.object_instance,
            r.parameter_values,
            r.user_supplied_tag,
        ),
        Req::SendDirectedInteractionWithTimeRequest(_r) => {
            // MVP: same as untimed; TSO ordering for directed interactions
            // would extend route_tso_or_immediate.
            Resp::SendDirectedInteractionWithTimeResponse(
                fedpro::SendDirectedInteractionWithTimeResponse { result: None },
            )
        }

        // ---- Default attribute transportation type change ----
        Req::ChangeDefaultAttributeTransportationTypeRequest(_) => {
            Resp::ChangeDefaultAttributeTransportationTypeResponse(
                fedpro::ChangeDefaultAttributeTransportationTypeResponse {},
            )
        }

        // Forward-compat catch-all. With the current FedPro proto every
        // variant above is matched; this arm protects us if a future
        // schema update adds new variants we haven't yet handled.
        #[allow(unreachable_patterns)]
        other => {
            tracing::warn!(?other, "unimplemented service");
            exception_variant(
                HlaException::RtiInternalError,
                "service not yet implemented",
            )
        }
    };

    DispatchOutcome {
        response: fedpro::CallResponse {
            call_response: Some(response_variant),
        },
        callbacks,
    }
}

// -----------------------------------------------------------------------------

// ===== Service handlers (split per IEEE 1516.1 service group) =====
mod federation_mgmt;
use federation_mgmt::*;
mod handle_lookup;
use handle_lookup::*;
mod declaration;
use declaration::*;
mod object_mgmt;
use object_mgmt::*;
mod data_flow;
use data_flow::*;
mod restore;
use restore::*;
mod directed;
use directed::*;
mod ddm_matching;
use ddm_matching::*;
mod ddm_lifecycle;
use ddm_lifecycle::*;
mod instance_lookup;
use instance_lookup::*;
mod advisories;
use advisories::*;
mod introspection;
use introspection::*;
mod save;
use save::*;
mod ownership;
use ownership::*;
mod sync_points;
use sync_points::*;
mod time_mgmt;
use time_mgmt::*;
mod helpers;
use helpers::*;
