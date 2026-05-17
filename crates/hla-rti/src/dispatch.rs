//! Service dispatch for `HLA_CALL_REQUEST` frames.
//!
//! Decodes a `fedpro::CallRequest` envelope, routes the inner `oneof` variant
//! to its handler, and produces a `fedpro::CallResponse`. Anything not yet
//! implemented returns an `ExceptionData{exceptionName="RTIinternalError", ...}`
//! response — that keeps the wire well-formed so clients can recover.
//!
//! IEEE 1516.1 exception class names (e.g. `FederationExecutionAlreadyExists`)
//! are reused verbatim as `exceptionName`.

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
// Federation Management
// -----------------------------------------------------------------------------

fn create_federation_inner(
    node: &Arc<RtiNode>,
    federation_name: String,
    proto_modules: Vec<fedpro::FomModule>,
) -> Result<(), Resp> {
    if federation_name.is_empty() {
        return Err(exception_variant(
            HlaException::ErrorReadingFdd,
            "federationName must not be empty",
        ));
    }
    let mut federations = node.federations.write();
    if federations.contains_key(&federation_name) {
        return Err(exception_variant(
            HlaException::FederationExecutionAlreadyExists,
            &federation_name,
        ));
    }
    let mut modules = Vec::new();
    for pm in &proto_modules {
        match decode_fom_module(pm) {
            Ok(m) => modules.push(m),
            Err(e) => return Err(exception_variant(HlaException::CouldNotOpenFdd, &e)),
        }
    }
    let fom = if modules.is_empty() {
        Arc::clone(&*node.default_fom.read())
    } else {
        match hla_omt::MergedFom::merge(modules) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                return Err(exception_variant(
                    HlaException::ErrorReadingFdd,
                    &e.to_string(),
                ));
            }
        }
    };
    let federation = Arc::new(Federation::new(federation_name.clone(), fom));
    federations.insert(federation_name, federation);
    Ok(())
}

/// Extract the XML text from a `FomModule` oneof and parse it.
/// MVP supports `FileFomModule` (inline name + content) only. `compressedModule`
/// and `url` return `Err("unsupported FOM module form")`.
fn decode_fom_module(pm: &fedpro::FomModule) -> Result<hla_omt::FomModule, String> {
    use fedpro::fom_module::FomModule as Variant;
    match pm.fom_module.as_ref() {
        Some(Variant::File(f)) => {
            let text = std::str::from_utf8(&f.content)
                .map_err(|e| format!("FOM module {:?} content is not UTF-8: {}", f.name, e))?;
            hla_omt::FomModule::parse(text).map_err(|e| format!("{}: {}", f.name, e))
        }
        Some(Variant::CompressedModule(_)) => {
            Err("compressed FOM modules not yet supported".into())
        }
        Some(Variant::Url(_)) => Err("URL FOM modules not yet supported".into()),
        None => Err("FomModule has no oneof variant set".into()),
    }
}

fn destroy_federation_execution(node: &Arc<RtiNode>, federation_name: String) -> Resp {
    let mut federations = node.federations.write();
    let entry = match federations.get(&federation_name) {
        Some(e) => e,
        None => {
            return exception_variant(
                HlaException::FederationExecutionDoesNotExist,
                &federation_name,
            );
        }
    };
    if !entry.federates.read().is_empty() {
        return exception_variant(HlaException::FederatesCurrentlyJoined, &federation_name);
    }
    federations.remove(&federation_name);
    Resp::DestroyFederationExecutionResponse(DestroyFederationExecutionResponse {})
}

fn join(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    requested_name: Option<String>,
    federation_name: String,
) -> Result<JoinResult, Resp> {
    if ctx.is_joined() {
        return Err(exception_variant(
            HlaException::FederateAlreadyExecutionMember,
            &format!("session {}", ctx.session_id),
        ));
    }
    let federation = {
        let federations = node.federations.read();
        match federations.get(&federation_name) {
            Some(f) => Arc::clone(f),
            None => {
                return Err(exception_variant(
                    HlaException::FederationExecutionDoesNotExist,
                    &federation_name,
                ));
            }
        }
    };

    let raw_handle = federation.next_federate_id.fetch_add(1, Ordering::Relaxed);
    let federate_handle = FederateHandle::new(raw_handle);
    let federate_name = match requested_name {
        Some(n) => {
            let federates = federation.federates.read();
            if federates.values().any(|f| f.name == n) {
                return Err(exception_variant(
                    HlaException::FederateNameAlreadyInUse,
                    &n,
                ));
            }
            n
        }
        None => format!("federate-{raw_handle:08X}"),
    };

    federation.federates.write().insert(
        federate_handle,
        FederateSession {
            handle: federate_handle,
            name: federate_name.clone(),
            federate_type: String::new(),
            session_id: ctx.session_id,
            pub_sub: PubSubState::default(),
            time: crate::TimeState::default(),
            switches: crate::Switches::default(),
            tso_queue: Vec::new(),
        },
    );

    ctx.membership = Some(Membership {
        federation,
        federate_handle,
        federate_name,
    });

    Ok(JoinResult {
        federate_handle: Some(crate::handles::encode_federate(federate_handle)),
        logical_time_implementation_name: "HLAfloat64Time".to_string(),
    })
}

fn resign(ctx: &mut SessionContext) -> Resp {
    let membership = match ctx.membership.take() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    membership
        .federation
        .federates
        .write()
        .remove(&membership.federate_handle);
    Resp::ResignFederationExecutionResponse(ResignFederationExecutionResponse {})
}

// -----------------------------------------------------------------------------
// Handle lookup
// -----------------------------------------------------------------------------

fn need_membership(ctx: &SessionContext) -> Result<&Membership, Resp> {
    ctx.membership
        .as_ref()
        .ok_or_else(|| exception_variant(HlaException::FederateNotExecutionMember, ""))
}

fn get_object_class_handle(ctx: &SessionContext, name: &str) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    match m.federation.fom.object_class_handle(name) {
        Some(h) => Resp::GetObjectClassHandleResponse(GetObjectClassHandleResponse {
            result: Some(encode_object_class(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

fn get_object_class_name(ctx: &SessionContext, h: fedpro::ObjectClassHandle) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let handle = match decode_object_class(&h) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    match m.federation.fom.object_class_def(handle) {
        Some(d) => Resp::GetObjectClassNameResponse(GetObjectClassNameResponse {
            result: d.name.clone(),
        }),
        None => exception_variant(
            HlaException::InvalidObjectClassHandle,
            &format!("{handle:?}"),
        ),
    }
}

fn get_attribute_handle(
    ctx: &SessionContext,
    class: fedpro::ObjectClassHandle,
    attr_name: &str,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_object_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    if m.federation.fom.object_class_def(class).is_none() {
        return exception_variant(
            HlaException::InvalidObjectClassHandle,
            &format!("{class:?}"),
        );
    }
    match m.federation.fom.attribute_handle(class, attr_name) {
        Some(h) => Resp::GetAttributeHandleResponse(GetAttributeHandleResponse {
            result: Some(encode_attribute(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, attr_name),
    }
}

fn get_attribute_name(
    ctx: &SessionContext,
    class: fedpro::ObjectClassHandle,
    attr: fedpro::AttributeHandle,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_object_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let attr = match decode_attribute(&attr) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let def = match m.federation.fom.object_class_def(class) {
        Some(d) => d,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    // Linear scan; small N. We don't currently store attribute names indexed
    // by handle directly — `attribute_table` is name→handle.
    for a in &def.attributes {
        if let Some(h) = m.federation.fom.attribute_handle(class, &a.name)
            && h == attr
        {
            return Resp::GetAttributeNameResponse(GetAttributeNameResponse {
                result: a.name.clone(),
            });
        }
    }
    exception_variant(HlaException::AttributeNotDefined, &format!("{attr:?}"))
}

fn get_interaction_class_handle(ctx: &SessionContext, name: &str) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    match m.federation.fom.interaction_class_handle(name) {
        Some(h) => Resp::GetInteractionClassHandleResponse(GetInteractionClassHandleResponse {
            result: Some(encode_interaction_class(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

fn get_interaction_class_name(ctx: &SessionContext, h: fedpro::InteractionClassHandle) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let handle = match decode_interaction_class(&h) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    match m.federation.fom.interaction_class_def(handle) {
        Some(d) => Resp::GetInteractionClassNameResponse(GetInteractionClassNameResponse {
            result: d.name.clone(),
        }),
        None => exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    }
}

fn get_parameter_handle(
    ctx: &SessionContext,
    class: fedpro::InteractionClassHandle,
    param_name: &str,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_interaction_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    if m.federation.fom.interaction_class_def(class).is_none() {
        return exception_variant(HlaException::InvalidInteractionClassHandle, "");
    }
    match m.federation.fom.parameter_handle(class, param_name) {
        Some(h) => Resp::GetParameterHandleResponse(GetParameterHandleResponse {
            result: Some(encode_parameter(h)),
        }),
        None => exception_variant(HlaException::NameNotFound, param_name),
    }
}

fn get_parameter_name(
    ctx: &SessionContext,
    class: fedpro::InteractionClassHandle,
    param: fedpro::ParameterHandle,
) -> Resp {
    let m = match need_membership(ctx) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let class = match decode_interaction_class(&class) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let param = match crate::handles::decode_parameter(&param) {
        Ok(h) => h,
        Err(HandleError::Invalid(n)) => return exception_variant(n, ""),
    };
    let def = match m.federation.fom.interaction_class_def(class) {
        Some(d) => d,
        None => return exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    };
    for p in &def.parameters {
        if let Some(h) = m.federation.fom.parameter_handle(class, &p.name)
            && h == param
        {
            return Resp::GetParameterNameResponse(GetParameterNameResponse {
                result: p.name.clone(),
            });
        }
    }
    exception_variant(HlaException::InteractionParameterNotDefined, "")
}

// -----------------------------------------------------------------------------
// Declaration Management
// -----------------------------------------------------------------------------

fn decode_handle_set(set: Option<ProtoAttributeHandleSet>) -> Result<AttributeHandleSet, Resp> {
    let mut out = AttributeHandleSet::new();
    let Some(set) = set else {
        return Ok(out);
    };
    for a in set.attribute_handle {
        out.insert(
            decode_attribute(&a).map_err(|HandleError::Invalid(n)| exception_variant(n, ""))?,
        );
    }
    Ok(out)
}

fn publish_object_class_attributes(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    attrs: Option<ProtoAttributeHandleSet>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    if m.federation.fom.object_class_def(class).is_none() {
        return exception_variant(HlaException::ObjectClassNotDefined, "");
    }
    let set = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub
            .published_attrs
            .entry(class)
            .or_default()
            .extend(set);
    }
    Resp::PublishObjectClassAttributesResponse(PublishObjectClassAttributesResponse {})
}

fn unpublish_object_class_attributes(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    attrs: Option<ProtoAttributeHandleSet>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let set = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle)
        && let Some(existing) = fs.pub_sub.published_attrs.get_mut(&class)
    {
        for a in &set {
            existing.remove(a);
        }
        if existing.is_empty() {
            fs.pub_sub.published_attrs.remove(&class);
        }
    }
    Resp::UnpublishObjectClassAttributesResponse(UnpublishObjectClassAttributesResponse {})
}

fn subscribe_object_class_attributes_inner(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    attrs: Option<ProtoAttributeHandleSet>,
    node: Option<&Arc<RtiNode>>,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    if m.federation.fom.object_class_def(class).is_none() {
        return exception_variant(HlaException::ObjectClassNotDefined, "");
    }
    let set = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Snapshot pre-state for advisory transitions.
    let was_subscribed = class_has_subscribers(&m.federation, class);

    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub
            .subscribed_attrs
            .entry(class)
            .or_default()
            .extend(set.iter().copied());
    }
    drop(federates);
    let mut subs = m.federation.subscriptions.write();
    for attr in &set {
        subs.subscribe_attribute(class, *attr, m.federate_handle);
    }
    drop(subs);

    // Emit StartRegistrationForObjectClass to publishers if this is the
    // first subscriber to the class.
    if let Some(node) = node
        && !was_subscribed
        && class_has_subscribers(&m.federation, class)
    {
        emit_start_registration(node, &m.federation, callbacks, class, m.federate_handle);
    }

    Resp::SubscribeObjectClassAttributesResponse(SubscribeObjectClassAttributesResponse {})
}

/// True if any federate currently subscribes to any attribute of `class`.
fn class_has_subscribers(federation: &Federation, class: ObjectClassHandle) -> bool {
    let subs = federation.subscriptions.read();
    subs.class_has_subscribers(class)
}

fn interaction_has_subscribers(federation: &Federation, class: InteractionClassHandle) -> bool {
    let subs = federation.subscriptions.read();
    subs.by_interaction.contains_key(&class)
}

fn emit_start_registration(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    class: ObjectClassHandle,
    exclude: FederateHandle,
) {
    let federates = federation.federates.read();
    let targets: std::collections::HashSet<FederateHandle> = federates
        .values()
        .filter(|fs| {
            fs.handle != exclude
                && fs.switches.object_class_relevance_advisory
                && fs.pub_sub.published_attrs.contains_key(&class)
        })
        .map(|fs| fs.handle)
        .collect();
    drop(federates);
    if targets.is_empty() {
        return;
    }
    let conns = live_connections(node, federation, &targets);
    fan_out(
        callbacks,
        &conns,
        start_registration_for_object_class(class),
    );
}

fn emit_stop_registration(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    class: ObjectClassHandle,
) {
    let federates = federation.federates.read();
    let targets: std::collections::HashSet<FederateHandle> = federates
        .values()
        .filter(|fs| {
            fs.switches.object_class_relevance_advisory
                && fs.pub_sub.published_attrs.contains_key(&class)
        })
        .map(|fs| fs.handle)
        .collect();
    drop(federates);
    if targets.is_empty() {
        return;
    }
    let conns = live_connections(node, federation, &targets);
    fan_out(callbacks, &conns, stop_registration_for_object_class(class));
}

fn emit_turn_interactions_on(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    class: InteractionClassHandle,
    exclude: FederateHandle,
) {
    let federates = federation.federates.read();
    let targets: std::collections::HashSet<FederateHandle> = federates
        .values()
        .filter(|fs| {
            fs.handle != exclude
                && fs.switches.interaction_relevance_advisory
                && fs.pub_sub.published_interactions.contains(&class)
        })
        .map(|fs| fs.handle)
        .collect();
    drop(federates);
    if targets.is_empty() {
        return;
    }
    let conns = live_connections(node, federation, &targets);
    fan_out(callbacks, &conns, turn_interactions_on(class));
}

fn emit_turn_interactions_off(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    class: InteractionClassHandle,
) {
    let federates = federation.federates.read();
    let targets: std::collections::HashSet<FederateHandle> = federates
        .values()
        .filter(|fs| {
            fs.switches.interaction_relevance_advisory
                && fs.pub_sub.published_interactions.contains(&class)
        })
        .map(|fs| fs.handle)
        .collect();
    drop(federates);
    if targets.is_empty() {
        return;
    }
    let conns = live_connections(node, federation, &targets);
    fan_out(callbacks, &conns, turn_interactions_off(class));
}

fn unsubscribe_object_class_attributes_inner(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    attrs: Option<ProtoAttributeHandleSet>,
    node: Option<&Arc<RtiNode>>,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let set = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let was_subscribed = class_has_subscribers(&m.federation, class);
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle)
        && let Some(existing) = fs.pub_sub.subscribed_attrs.get_mut(&class)
    {
        for a in &set {
            existing.remove(a);
        }
        if existing.is_empty() {
            fs.pub_sub.subscribed_attrs.remove(&class);
        }
    }
    drop(federates);
    let mut subs = m.federation.subscriptions.write();
    for attr in &set {
        subs.unsubscribe_attribute(class, *attr, m.federate_handle);
    }
    drop(subs);

    if let Some(node) = node
        && was_subscribed
        && !class_has_subscribers(&m.federation, class)
    {
        emit_stop_registration(node, &m.federation, callbacks, class);
    }

    Resp::UnsubscribeObjectClassAttributesResponse(UnsubscribeObjectClassAttributesResponse {})
}

fn decode_interaction_or_err(
    h: Option<fedpro::InteractionClassHandle>,
) -> Result<InteractionClassHandle, Resp> {
    h.and_then(|x| decode_interaction_class(&x).ok())
        .ok_or_else(|| exception_variant(HlaException::InvalidInteractionClassHandle, ""))
}

fn publish_interaction_class(
    ctx: &mut SessionContext,
    class: Option<fedpro::InteractionClassHandle>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match decode_interaction_or_err(class) {
        Ok(c) => c,
        Err(e) => return e,
    };
    if m.federation.fom.interaction_class_def(class).is_none() {
        return exception_variant(HlaException::InteractionClassNotDefined, "");
    }
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub.published_interactions.insert(class);
    }
    Resp::PublishInteractionClassResponse(PublishInteractionClassResponse {})
}

fn unpublish_interaction_class(
    ctx: &mut SessionContext,
    class: Option<fedpro::InteractionClassHandle>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match decode_interaction_or_err(class) {
        Ok(c) => c,
        Err(e) => return e,
    };
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub.published_interactions.remove(&class);
    }
    Resp::UnpublishInteractionClassResponse(UnpublishInteractionClassResponse {})
}

fn subscribe_interaction_class_inner(
    ctx: &mut SessionContext,
    class: Option<fedpro::InteractionClassHandle>,
    node: Option<&Arc<RtiNode>>,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match decode_interaction_or_err(class) {
        Ok(c) => c,
        Err(e) => return e,
    };
    if m.federation.fom.interaction_class_def(class).is_none() {
        return exception_variant(HlaException::InteractionClassNotDefined, "");
    }
    let was_subscribed = interaction_has_subscribers(&m.federation, class);
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub.subscribed_interactions.insert(class);
    }
    drop(federates);
    m.federation
        .subscriptions
        .write()
        .by_interaction
        .entry(class)
        .or_default()
        .insert(m.federate_handle);

    if let Some(node) = node
        && !was_subscribed
        && interaction_has_subscribers(&m.federation, class)
    {
        emit_turn_interactions_on(node, &m.federation, callbacks, class, m.federate_handle);
    }

    Resp::SubscribeInteractionClassResponse(SubscribeInteractionClassResponse {})
}

fn unsubscribe_interaction_class_inner(
    ctx: &mut SessionContext,
    class: Option<fedpro::InteractionClassHandle>,
    node: Option<&Arc<RtiNode>>,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match decode_interaction_or_err(class) {
        Ok(c) => c,
        Err(e) => return e,
    };
    let was_subscribed = interaction_has_subscribers(&m.federation, class);
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub.subscribed_interactions.remove(&class);
    }
    drop(federates);
    let mut subs = m.federation.subscriptions.write();
    if let Some(set) = subs.by_interaction.get_mut(&class) {
        set.remove(&m.federate_handle);
        if set.is_empty() {
            subs.by_interaction.remove(&class);
        }
    }
    drop(subs);

    if let Some(node) = node
        && was_subscribed
        && !interaction_has_subscribers(&m.federation, class)
    {
        emit_turn_interactions_off(node, &m.federation, callbacks, class);
    }

    Resp::UnsubscribeInteractionClassResponse(UnsubscribeInteractionClassResponse {})
}

// -----------------------------------------------------------------------------
// Object Management
// -----------------------------------------------------------------------------

fn register_object_instance(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    class: Option<fedpro::ObjectClassHandle>,
    requested_name: Option<String>,
) -> Result<ObjectInstanceHandle, Resp> {
    let m = ctx
        .membership
        .as_ref()
        .ok_or_else(|| exception_variant(HlaException::FederateNotExecutionMember, ""))?
        .clone();
    let class = class
        .and_then(|h| decode_object_class(&h).ok())
        .ok_or_else(|| exception_variant(HlaException::InvalidObjectClassHandle, ""))?;
    if m.federation.fom.object_class_def(class).is_none() {
        return Err(exception_variant(HlaException::ObjectClassNotDefined, ""));
    }
    {
        let federates = m.federation.federates.read();
        let fs = federates
            .get(&m.federate_handle)
            .ok_or_else(|| exception_variant(HlaException::FederateNotExecutionMember, ""))?;
        if !fs.pub_sub.published_attrs.contains_key(&class) {
            return Err(exception_variant(HlaException::ObjectClassNotPublished, ""));
        }
    }

    let raw_id = m.federation.next_object_id.fetch_add(1, Ordering::Relaxed);
    let handle = ObjectInstanceHandle::new(raw_id);

    let name = match requested_name {
        Some(n) => {
            let instances = m.federation.object_instances.read();
            if instances.values().any(|inst| inst.name == n) {
                return Err(exception_variant(HlaException::ObjectInstanceNameInUse, &n));
            }
            n
        }
        None => format!("HLA{raw_id}"),
    };

    // Seed attribute ownership from the registrar's currently-published
    // attribute set for this class. (More precisely, from any ancestor of
    // this class that has a published attribute — but simpler: we use the
    // direct class's published set.) Per IEEE 1516.1 §7, attributes not
    // published by the registrar are not owned by anyone at registration
    // time.
    let attribute_owners: HashMap<AttributeHandle, FederateHandle> = {
        let federates = m.federation.federates.read();
        let fs = federates
            .get(&m.federate_handle)
            .ok_or_else(|| exception_variant(HlaException::FederateNotExecutionMember, ""))?;
        fs.pub_sub
            .published_attrs
            .get(&class)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|attr| (attr, m.federate_handle))
            .collect()
    };

    m.federation.object_instances.write().insert(
        handle,
        ObjectInstance {
            handle,
            class,
            name: name.clone(),
            registrar: m.federate_handle,
            attribute_owners,
            attribute_regions: HashMap::new(),
        },
    );

    // Fan-out `discoverObjectInstance` to every federate subscribed to this
    // class (or any ancestor). Subscribers are determined by the union of
    // subscriptions across attributes — any subscriber to the class hierarchy
    // should learn about the instance.
    notify_subscribers_of_registration(
        node,
        &m.federation,
        callbacks,
        handle,
        class,
        &name,
        m.federate_handle,
    );

    Ok(handle)
}

/// Identify every federate that has subscribed to *any* attribute of `class`
/// or any ancestor class, mark `instance` as discovered in their PubSubState,
/// and enqueue a `DiscoverObjectInstance` callback to each.
fn notify_subscribers_of_registration(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    instance: ObjectInstanceHandle,
    class: ObjectClassHandle,
    name: &str,
    producer: FederateHandle,
) {
    // O(subscribers) per ancestor via the `class_subscribers` reverse index,
    // not O(total subscription entries) which the old walk paid.
    let candidates = {
        let subs = federation.subscriptions.read();
        let mut out = std::collections::HashSet::new();
        for ancestor in federation.fom.inheritance_chain(class) {
            for fh in subs.subscribers_of_class(ancestor) {
                if fh != producer {
                    out.insert(fh);
                }
            }
        }
        out
    };

    let mut to_notify = Vec::new();
    {
        let mut federates = federation.federates.write();
        for fh in &candidates {
            if let Some(fs) = federates.get_mut(fh)
                && fs.pub_sub.discovered_instances.insert(instance)
            {
                to_notify.push(*fh);
            }
        }
    }
    let targets: std::collections::HashSet<FederateHandle> = to_notify.into_iter().collect();
    if targets.is_empty() {
        return;
    }

    let connections = live_connections(node, federation, &targets);
    fan_out(
        callbacks,
        &connections,
        discover_object_instance(instance, class, name, producer),
    );
}

// -----------------------------------------------------------------------------
// updateAttributeValues / sendInteraction / deleteObjectInstance
// -----------------------------------------------------------------------------

fn decode_attribute_value_map(
    map: Option<fedpro::AttributeHandleValueMap>,
) -> Result<AttributeHandleValueMap, Resp> {
    let mut out = AttributeHandleValueMap::new();
    let Some(map) = map else {
        return Ok(out);
    };
    for entry in map.attribute_handle_value {
        let handle = entry.attribute_handle.as_ref().ok_or_else(|| {
            exception_variant(HlaException::InvalidAttributeHandle, "missing handle")
        })?;
        let h =
            decode_attribute(handle).map_err(|HandleError::Invalid(n)| exception_variant(n, ""))?;
        out.insert(h, entry.value);
    }
    Ok(out)
}

fn decode_parameter_value_map(
    map: Option<fedpro::ParameterHandleValueMap>,
) -> Result<ParameterHandleValueMap, Resp> {
    let mut out = ParameterHandleValueMap::new();
    let Some(map) = map else {
        return Ok(out);
    };
    for entry in map.parameter_handle_value {
        let handle = entry.parameter_handle.as_ref().ok_or_else(|| {
            exception_variant(HlaException::InvalidParameterHandle, "missing handle")
        })?;
        let h =
            decode_parameter(handle).map_err(|HandleError::Invalid(n)| exception_variant(n, ""))?;
        out.insert(h, entry.value);
    }
    Ok(out)
}

fn update_attribute_values(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    values: Option<fedpro::AttributeHandleValueMap>,
    tag: Vec<u8>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_handle = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, "missing handle"),
    };
    let values = match decode_attribute_value_map(values) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Validate: instance exists; for *each* attribute being updated, this
    // federate is the current per-attribute owner. (More restrictive than
    // the previous "instance owner" check, which was incorrect — ownership
    // is per-attribute per IEEE 1516.1 §7.)
    let (class, attrs_list) = {
        let instances = m.federation.object_instances.read();
        let inst = match instances.get(&instance_handle) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        for attr in values.keys() {
            match inst.attribute_owners.get(attr) {
                Some(owner) if *owner == m.federate_handle => {}
                _ => return exception_variant(HlaException::AttributeNotOwned, ""),
            }
        }
        let attrs: Vec<AttributeHandle> = values.keys().copied().collect();
        (inst.class, attrs)
    };

    // Look up subscribers (excluding the producer) and fan out.
    let mut subscribers =
        subscribers_for_attributes(&m.federation, class, &attrs_list, Some(m.federate_handle));

    // DDM filter: intersect across all attributes — a subscriber must match
    // on at least one of the updated attributes to receive the reflection.
    let mut matched: std::collections::HashSet<FederateHandle> = std::collections::HashSet::new();
    for attr in &attrs_list {
        let filtered = filter_subscribers_by_regions(
            &m.federation,
            subscribers.clone(),
            instance_handle,
            class,
            *attr,
        );
        matched.extend(filtered);
    }
    subscribers = matched;

    notify_subscribers_of_registration_subset(
        node,
        &m.federation,
        callbacks,
        instance_handle,
        class,
        m.federate_handle,
        &subscribers,
    );

    let connections = live_connections(node, &m.federation, &subscribers);
    fan_out(
        callbacks,
        &connections,
        reflect_attribute_values(instance_handle, &values, &tag, m.federate_handle),
    );

    Resp::UpdateAttributeValuesResponse(UpdateAttributeValuesResponse {})
}

/// Like `notify_subscribers_of_registration` but restricted to a
/// known-subscriber set — used by `update_attribute_values` to retro-discover
/// for late subscribers.
fn notify_subscribers_of_registration_subset(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    instance: ObjectInstanceHandle,
    class: ObjectClassHandle,
    producer: FederateHandle,
    candidates: &std::collections::HashSet<FederateHandle>,
) {
    if candidates.is_empty() {
        return;
    }
    let (name, to_notify) = {
        let instances = federation.object_instances.read();
        let inst = match instances.get(&instance) {
            Some(i) => i,
            None => return,
        };
        let name = inst.name.clone();
        drop(instances);

        let mut federates = federation.federates.write();
        let mut to_notify = Vec::new();
        for fh in candidates {
            if let Some(fs) = federates.get_mut(fh)
                && fs.pub_sub.discovered_instances.insert(instance)
            {
                to_notify.push(*fh);
            }
        }
        (name, to_notify)
    };
    let targets: std::collections::HashSet<FederateHandle> = to_notify.into_iter().collect();
    if targets.is_empty() {
        return;
    }
    let connections = live_connections(node, federation, &targets);
    fan_out(
        callbacks,
        &connections,
        discover_object_instance(instance, class, &name, producer),
    );
}

fn update_attribute_values_with_time(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    values: Option<fedpro::AttributeHandleValueMap>,
    tag: Vec<u8>,
    time: Option<fedpro::LogicalTime>,
) -> Resp {
    use hla_fedpro_proto::fedpro::UpdateAttributeValuesWithTimeResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_handle = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let values = match decode_attribute_value_map(values) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let time = match time.as_ref().map(decode_logical_time) {
        Some(Ok(t)) => t,
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLogicalTime, ""),
    };

    // Validate ownership per-attribute.
    let (class, attrs_list) = {
        let instances = m.federation.object_instances.read();
        let inst = match instances.get(&instance_handle) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        for attr in values.keys() {
            match inst.attribute_owners.get(attr) {
                Some(owner) if *owner == m.federate_handle => {}
                _ => return exception_variant(HlaException::AttributeNotOwned, ""),
            }
        }
        let attrs: Vec<AttributeHandle> = values.keys().copied().collect();
        (inst.class, attrs)
    };

    // Validate time: must be >= federate's current_time + lookahead.
    {
        let federates = m.federation.federates.read();
        if let Some(fs) = federates.get(&m.federate_handle)
            && fs.time.is_regulating
        {
            let lbts = fs.time.current_time + fs.time.lookahead;
            if time < lbts {
                return exception_variant(
                    HlaException::InvalidLogicalTime,
                    &format!("time {time} < current+lookahead {lbts}"),
                );
            }
        }
    }

    let subscribers =
        subscribers_for_attributes(&m.federation, class, &attrs_list, Some(m.federate_handle));
    notify_subscribers_of_registration_subset(
        node,
        &m.federation,
        callbacks,
        instance_handle,
        class,
        m.federate_handle,
        &subscribers,
    );
    // Per-subscriber TSO routing: constrained subscribers whose current_time
    // is behind the message timestamp queue rather than receive immediately.
    let callback =
        reflect_attribute_values_with_time(instance_handle, &values, &tag, m.federate_handle, time);
    route_tso_or_immediate(node, &m.federation, callbacks, &subscribers, time, callback);
    Resp::UpdateAttributeValuesWithTimeResponse(UpdateAttributeValuesWithTimeResponse {
        result: None,
    })
}

/// Per-IEEE 1516.1: a TSO message destined for a constrained federate must
/// be held until that federate's logical time advances to ≥ message
/// timestamp. Unconstrained subscribers always receive immediately.
fn route_tso_or_immediate(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    subscribers: &std::collections::HashSet<FederateHandle>,
    time: f64,
    callback: fedpro::CallbackRequest,
) {
    let mut immediate: std::collections::HashSet<FederateHandle> = std::collections::HashSet::new();
    let mut queued: Vec<FederateHandle> = Vec::new();
    {
        let federates = federation.federates.read();
        for &fh in subscribers {
            match federates.get(&fh) {
                Some(fs) if fs.time.is_constrained && time > fs.time.current_time => {
                    queued.push(fh);
                }
                Some(_) => {
                    immediate.insert(fh);
                }
                None => {}
            }
        }
    }
    if !immediate.is_empty() {
        let conns = live_connections(node, federation, &immediate);
        fan_out(callbacks, &conns, callback.clone());
    }
    if !queued.is_empty() {
        let mut federates = federation.federates.write();
        for fh in queued {
            if let Some(fs) = federates.get_mut(&fh) {
                // Keep tso_queue sorted by time ascending.
                let pos = fs.tso_queue.partition_point(|m| m.time <= time);
                fs.tso_queue.insert(
                    pos,
                    crate::TsoMessage {
                        time,
                        callback: callback.clone(),
                    },
                );
            }
        }
    }
}

/// Drain all TSO messages with timestamp ≤ `up_to` from `federate`'s queue,
/// pushing them as immediate callbacks to that federate's connection.
fn drain_tso_up_to(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
    federate: FederateHandle,
    up_to: f64,
) {
    let drained: Vec<TsoMessage> = {
        let mut federates = federation.federates.write();
        let fs = match federates.get_mut(&federate) {
            Some(f) => f,
            None => return,
        };
        let cut = fs.tso_queue.partition_point(|m| m.time <= up_to);
        fs.tso_queue.drain(..cut).collect()
    };
    if drained.is_empty() {
        return;
    }
    let mut target = std::collections::HashSet::new();
    target.insert(federate);
    let conns = live_connections(node, federation, &target);
    for msg in drained {
        fan_out(callbacks, &conns, msg.callback);
    }
}

use crate::TsoMessage;

fn send_interaction_with_time(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    class: Option<fedpro::InteractionClassHandle>,
    params: Option<fedpro::ParameterHandleValueMap>,
    tag: Vec<u8>,
    time: Option<fedpro::LogicalTime>,
) -> Resp {
    use hla_fedpro_proto::fedpro::SendInteractionWithTimeResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_interaction_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    };
    if m.federation.fom.interaction_class_def(class).is_none() {
        return exception_variant(HlaException::InteractionClassNotDefined, "");
    }
    {
        let federates = m.federation.federates.read();
        let fs = match federates.get(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if !fs.pub_sub.published_interactions.contains(&class) {
            return exception_variant(HlaException::InteractionClassNotPublished, "");
        }
    }
    let params = match decode_parameter_value_map(params) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let time = match time.as_ref().map(decode_logical_time) {
        Some(Ok(t)) => t,
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLogicalTime, ""),
    };

    let subscribers = subscribers_for_interaction(&m.federation, class, Some(m.federate_handle));
    let callback = receive_interaction_with_time(class, &params, &tag, m.federate_handle, time);
    route_tso_or_immediate(node, &m.federation, callbacks, &subscribers, time, callback);
    Resp::SendInteractionWithTimeResponse(SendInteractionWithTimeResponse { result: None })
}

fn delete_object_instance_with_time(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    tag: Vec<u8>,
    time: Option<fedpro::LogicalTime>,
) -> Resp {
    use hla_fedpro_proto::fedpro::DeleteObjectInstanceWithTimeResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_handle = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let time = match time.as_ref().map(decode_logical_time) {
        Some(Ok(t)) => t,
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLogicalTime, ""),
    };

    let class = {
        let mut instances = m.federation.object_instances.write();
        let inst = match instances.get(&instance_handle) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        if inst.registrar != m.federate_handle {
            return exception_variant(HlaException::DeletePrivilegeNotHeld, "");
        }
        let class = inst.class;
        instances.remove(&instance_handle);
        class
    };

    let to_notify: std::collections::HashSet<FederateHandle> = {
        let mut federates = m.federation.federates.write();
        let mut s = std::collections::HashSet::new();
        for (&fh, fs) in federates.iter_mut() {
            if fh == m.federate_handle {
                continue;
            }
            if fs.pub_sub.discovered_instances.remove(&instance_handle) {
                s.insert(fh);
            }
        }
        s
    };
    let _ = class;
    let callback = remove_object_instance_with_time(instance_handle, &tag, m.federate_handle, time);
    route_tso_or_immediate(node, &m.federation, callbacks, &to_notify, time, callback);

    Resp::DeleteObjectInstanceWithTimeResponse(DeleteObjectInstanceWithTimeResponse {
        result: None,
    })
}

fn send_interaction(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    class: Option<fedpro::InteractionClassHandle>,
    params: Option<fedpro::ParameterHandleValueMap>,
    tag: Vec<u8>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_interaction_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    };
    if m.federation.fom.interaction_class_def(class).is_none() {
        return exception_variant(HlaException::InteractionClassNotDefined, "");
    }
    {
        let federates = m.federation.federates.read();
        let fs = match federates.get(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if !fs.pub_sub.published_interactions.contains(&class) {
            return exception_variant(HlaException::InteractionClassNotPublished, "");
        }
    }
    let params = match decode_parameter_value_map(params) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let subscribers = subscribers_for_interaction(&m.federation, class, Some(m.federate_handle));
    let connections = live_connections(node, &m.federation, &subscribers);
    fan_out(
        callbacks,
        &connections,
        receive_interaction(class, &params, &tag, m.federate_handle),
    );
    Resp::SendInteractionResponse(SendInteractionResponse {})
}

fn delete_object_instance(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    tag: Vec<u8>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_handle = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    // Remove + record the class for subscriber lookup.
    let class = {
        let mut instances = m.federation.object_instances.write();
        let inst = match instances.get(&instance_handle) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        if inst.registrar != m.federate_handle {
            return exception_variant(HlaException::DeletePrivilegeNotHeld, "");
        }
        let class = inst.class;
        instances.remove(&instance_handle);
        class
    };

    // Fan-out RemoveObjectInstance to all federates that had discovered it.
    let to_notify: std::collections::HashSet<FederateHandle> = {
        let mut federates = m.federation.federates.write();
        let mut s = std::collections::HashSet::new();
        for (&fh, fs) in federates.iter_mut() {
            if fh == m.federate_handle {
                continue;
            }
            if fs.pub_sub.discovered_instances.remove(&instance_handle) {
                s.insert(fh);
            }
        }
        s
    };
    let _ = class;
    let connections = live_connections(node, &m.federation, &to_notify);
    fan_out(
        callbacks,
        &connections,
        remove_object_instance(instance_handle, &tag, m.federate_handle),
    );

    Resp::DeleteObjectInstanceResponse(DeleteObjectInstanceResponse {})
}

// -----------------------------------------------------------------------------
// Federation Restore (MVP: orchestration only)
// -----------------------------------------------------------------------------

fn request_federation_restore(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    label: String,
) -> Resp {
    use hla_fedpro_proto::fedpro::RequestFederationRestoreResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if label.is_empty() {
        return exception_variant(HlaException::InvalidRestoreLabel, "");
    }

    // Try to load the snapshot from disk before initiating restore. If
    // the snapshot doesn't exist, restoration still proceeds (using the
    // current in-memory state) but no actual state is reset — this matches
    // a "no save was made" call.
    let snap = {
        let dir = node.save_dir.read().clone();
        match crate::persistence::read_snapshot(&dir, &m.federation.name, &label) {
            Ok(s) => Some(s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot read failed; failing restore");
                let registrant = live_connections(node, &m.federation, &single(m.federate_handle));
                fan_out(
                    callbacks,
                    &registrant,
                    request_federation_restore_failed(&label),
                );
                return Resp::RequestFederationRestoreResponse(RequestFederationRestoreResponse {});
            }
        }
    };

    let participants: Vec<(FederateHandle, String)> = {
        m.federation
            .federates
            .read()
            .values()
            .map(|fs| (fs.handle, fs.name.clone()))
            .collect()
    };

    let initiated = {
        let mut current = m.federation.current_restore.write();
        if current.is_some() {
            return exception_variant(HlaException::RestoreInProgress, "");
        }
        *current = Some(RestoreOperation {
            label: label.clone(),
            statuses: participants
                .iter()
                .map(|(h, _)| (*h, RestoreStatus::Initiated))
                .collect(),
        });
        true
    };

    // Apply the loaded snapshot now — this rewrites instances + sync points
    // before federates are told the restore has begun. Federate handles in
    // the snapshot are mapped to currently-joined federate handles by name
    // (see `Federation::apply_snapshot`), which is how handle reassignment
    // is realized in this MVP.
    if let Some(snap) = snap {
        m.federation.apply_snapshot(&snap);
        tracing::info!(label, "snapshot applied");
    }

    let requester_conn = live_connections(node, &m.federation, &single(m.federate_handle));
    if initiated {
        fan_out(
            callbacks,
            &requester_conn,
            request_federation_restore_succeeded(&label),
        );
        let all_set: std::collections::HashSet<FederateHandle> =
            participants.iter().map(|(h, _)| *h).collect();
        let conns = live_connections(node, &m.federation, &all_set);
        // FederationRestoreBegun first.
        fan_out(callbacks, &conns, federation_restore_begun());
        // Then per-federate InitiateFederateRestore.
        for (fh, name) in &participants {
            let conn = live_connections(node, &m.federation, &single(*fh));
            fan_out(
                callbacks,
                &conn,
                initiate_federate_restore(&label, name, *fh),
            );
        }
    } else {
        fan_out(
            callbacks,
            &requester_conn,
            request_federation_restore_failed(&label),
        );
    }

    Resp::RequestFederationRestoreResponse(RequestFederationRestoreResponse {})
}

fn federate_restore_progressed(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    successfully: bool,
) -> Resp {
    use hla_fedpro_proto::fedpro::{
        FederateRestoreCompleteResponse, FederateRestoreNotCompleteResponse,
    };
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };

    let outcome = {
        let mut current = m.federation.current_restore.write();
        let restore = match current.as_mut() {
            Some(r) => r,
            None => return exception_variant(HlaException::RestoreNotInProgress, ""),
        };
        let new_status = if successfully {
            RestoreStatus::Complete
        } else {
            RestoreStatus::NotComplete
        };
        restore.statuses.insert(m.federate_handle, new_status);
        let any_failed = restore
            .statuses
            .values()
            .any(|s| *s == RestoreStatus::NotComplete);
        let all_done = restore
            .statuses
            .values()
            .all(|s| matches!(s, RestoreStatus::Complete | RestoreStatus::NotComplete));
        if all_done {
            current.take();
            Some(any_failed)
        } else {
            None
        }
    };

    if let Some(any_failed) = outcome {
        let participants: std::collections::HashSet<FederateHandle> =
            m.federation.federates.read().keys().copied().collect();
        let conns = live_connections(node, &m.federation, &participants);
        let cb = if any_failed {
            federation_not_restored(0)
        } else {
            federation_restored()
        };
        fan_out(callbacks, &conns, cb);
    }

    if successfully {
        Resp::FederateRestoreCompleteResponse(FederateRestoreCompleteResponse {})
    } else {
        Resp::FederateRestoreNotCompleteResponse(FederateRestoreNotCompleteResponse {})
    }
}

fn abort_federation_restore(ctx: &mut SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::AbortFederationRestoreResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut current = m.federation.current_restore.write();
    if current.is_none() {
        return exception_variant(HlaException::RestoreNotInProgress, "");
    }
    current.take();
    Resp::AbortFederationRestoreResponse(AbortFederationRestoreResponse {})
}

/// Decode an `AttributeSetRegionSetPairList` into a flat
/// `(attr, regions)` map.
fn decode_attr_region_pairs(
    pairs: Option<fedpro::AttributeSetRegionSetPairList>,
) -> HashMap<AttributeHandle, std::collections::HashSet<hla_core::RegionHandle>> {
    let mut out = HashMap::<AttributeHandle, std::collections::HashSet<_>>::new();
    let Some(list) = pairs else { return out };
    for pair in list.attribute_set_region_set_pair {
        let attrs: Vec<AttributeHandle> = pair
            .attribute_set
            .map(|s| {
                s.attribute_handle
                    .iter()
                    .filter_map(|h| decode_attribute(h).ok())
                    .collect()
            })
            .unwrap_or_default();
        let regions: std::collections::HashSet<hla_core::RegionHandle> = pair
            .region_set
            .map(|s| {
                s.region_handle
                    .iter()
                    .filter_map(|h| {
                        if h.data.len() == 8 {
                            Some(hla_core::RegionHandle::new(u64::from_be_bytes(
                                h.data[..].try_into().unwrap(),
                            )))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        for a in attrs {
            out.entry(a).or_default().extend(regions.iter().copied());
        }
    }
    out
}

fn subscribe_object_class_attributes_with_regions(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    class: Option<fedpro::ObjectClassHandle>,
    pairs: Option<fedpro::AttributeSetRegionSetPairList>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    if m.federation.fom.object_class_def(class).is_none() {
        return exception_variant(HlaException::ObjectClassNotDefined, "");
    }
    let attr_regions = decode_attr_region_pairs(pairs);
    let was_subscribed = class_has_subscribers(&m.federation, class);
    {
        let mut federates = m.federation.federates.write();
        if let Some(fs) = federates.get_mut(&m.federate_handle) {
            for (attr, regions) in &attr_regions {
                fs.pub_sub
                    .subscribed_attrs
                    .entry(class)
                    .or_default()
                    .insert(*attr);
                fs.pub_sub
                    .subscribed_attrs_regions
                    .entry((class, *attr))
                    .or_default()
                    .extend(regions.iter().copied());
            }
        }
    }
    {
        let mut subs = m.federation.subscriptions.write();
        for attr in attr_regions.keys() {
            subs.subscribe_attribute(class, *attr, m.federate_handle);
        }
    }

    if !was_subscribed && class_has_subscribers(&m.federation, class) {
        emit_start_registration(node, &m.federation, callbacks, class, m.federate_handle);
    }
    Resp::SubscribeObjectClassAttributesWithRegionsResponse(
        fedpro::SubscribeObjectClassAttributesWithRegionsResponse {},
    )
}

fn unsubscribe_object_class_attributes_with_regions(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    pairs: Option<fedpro::AttributeSetRegionSetPairList>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let attr_regions = decode_attr_region_pairs(pairs);
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        for (attr, regions) in &attr_regions {
            if let Some(existing) = fs.pub_sub.subscribed_attrs_regions.get_mut(&(class, *attr)) {
                for r in regions {
                    existing.remove(r);
                }
                if existing.is_empty() {
                    fs.pub_sub.subscribed_attrs_regions.remove(&(class, *attr));
                }
            }
        }
    }
    drop(federates);
    Resp::UnsubscribeObjectClassAttributesWithRegionsResponse(
        fedpro::UnsubscribeObjectClassAttributesWithRegionsResponse {},
    )
}

fn register_object_instance_with_regions(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    class: Option<fedpro::ObjectClassHandle>,
    requested_name: Option<String>,
    pairs: Option<fedpro::AttributeSetRegionSetPairList>,
) -> Result<ObjectInstanceHandle, Resp> {
    let attr_regions = decode_attr_region_pairs(pairs);
    let handle = register_object_instance(node, ctx, callbacks, class, requested_name)?;
    // Attach the region associations to the newly-created instance.
    if let Some(m) = ctx.membership.as_ref() {
        let mut instances = m.federation.object_instances.write();
        if let Some(inst) = instances.get_mut(&handle) {
            inst.attribute_regions = attr_regions;
        }
    }
    Ok(handle)
}

// -----------------------------------------------------------------------------
// Directed interactions (per-instance routing)
// -----------------------------------------------------------------------------

fn decode_interaction_class_set(
    set: Option<fedpro::InteractionClassHandleSet>,
) -> Vec<InteractionClassHandle> {
    set.map(|s| {
        s.interaction_class_handle
            .iter()
            .filter_map(|h| decode_interaction_class(h).ok())
            .collect()
    })
    .unwrap_or_default()
}

fn publish_object_class_directed_interactions(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    interactions: Option<fedpro::InteractionClassHandleSet>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let ix = decode_interaction_class_set(interactions);
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub
            .published_directed_interactions
            .entry(class)
            .or_default()
            .extend(ix);
    }
    Resp::PublishObjectClassDirectedInteractionsResponse(
        fedpro::PublishObjectClassDirectedInteractionsResponse {},
    )
}

fn unpublish_object_class_directed_interactions(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    interactions: Option<fedpro::InteractionClassHandleSet>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        match interactions {
            Some(set) => {
                let to_remove = decode_interaction_class_set(Some(set));
                if let Some(existing) = fs.pub_sub.published_directed_interactions.get_mut(&class) {
                    for ic in to_remove {
                        existing.remove(&ic);
                    }
                    if existing.is_empty() {
                        fs.pub_sub.published_directed_interactions.remove(&class);
                    }
                }
            }
            None => {
                fs.pub_sub.published_directed_interactions.remove(&class);
            }
        }
    }
    Resp::UnpublishObjectClassDirectedInteractionsResponse(
        fedpro::UnpublishObjectClassDirectedInteractionsResponse {},
    )
}

fn subscribe_object_class_directed_interactions(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    interactions: Option<fedpro::InteractionClassHandleSet>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let ix = decode_interaction_class_set(interactions);
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub
            .subscribed_directed_interactions
            .entry(class)
            .or_default()
            .extend(ix);
    }
    Resp::SubscribeObjectClassDirectedInteractionsResponse(
        fedpro::SubscribeObjectClassDirectedInteractionsResponse {},
    )
}

fn unsubscribe_object_class_directed_interactions(
    ctx: &mut SessionContext,
    class: Option<fedpro::ObjectClassHandle>,
    interactions: Option<fedpro::InteractionClassHandleSet>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_object_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidObjectClassHandle, ""),
    };
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        match interactions {
            Some(set) => {
                let to_remove = decode_interaction_class_set(Some(set));
                if let Some(existing) = fs.pub_sub.subscribed_directed_interactions.get_mut(&class)
                {
                    for ic in to_remove {
                        existing.remove(&ic);
                    }
                    if existing.is_empty() {
                        fs.pub_sub.subscribed_directed_interactions.remove(&class);
                    }
                }
            }
            None => {
                fs.pub_sub.subscribed_directed_interactions.remove(&class);
            }
        }
    }
    Resp::UnsubscribeObjectClassDirectedInteractionsResponse(
        fedpro::UnsubscribeObjectClassDirectedInteractionsResponse {},
    )
}

fn send_directed_interaction(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    class: Option<fedpro::InteractionClassHandle>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    params: Option<fedpro::ParameterHandleValueMap>,
    tag: Vec<u8>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let class = match class.and_then(|h| decode_interaction_class(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidInteractionClassHandle, ""),
    };
    let instance_h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let params = match decode_parameter_value_map(params) {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Look up the target instance to find its class and owner.
    let (object_class, owner) = {
        let instances = m.federation.object_instances.read();
        match instances.get(&instance_h) {
            Some(i) => (i.class, i.registrar),
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        }
    };

    // Per IEEE 1516.1: directed interaction is delivered to:
    //  1. the federate that REGISTERED the instance (the owner of the
    //     instance's privilegeToDelete) — the canonical recipient
    //  2. every federate that has SubscribeObjectClassDirectedInteractions
    //     for this (object_class, interaction_class) pair
    let mut targets = std::collections::HashSet::new();
    if owner != m.federate_handle {
        targets.insert(owner);
    }
    {
        let federates = m.federation.federates.read();
        for fs in federates.values() {
            if fs.handle == m.federate_handle {
                continue;
            }
            if let Some(ix_set) = fs
                .pub_sub
                .subscribed_directed_interactions
                .get(&object_class)
                && ix_set.contains(&class)
            {
                targets.insert(fs.handle);
            }
        }
    }

    let conns = live_connections(node, &m.federation, &targets);
    fan_out(
        callbacks,
        &conns,
        receive_directed_interaction(class, instance_h, &params, &tag, m.federate_handle),
    );

    Resp::SendDirectedInteractionResponse(fedpro::SendDirectedInteractionResponse {})
}

// -----------------------------------------------------------------------------
// DDM region-overlap matching
// -----------------------------------------------------------------------------

/// Returns true if `pub_regions` and `sub_regions` have ANY pair of regions
/// that overlap. Empty `pub_regions` OR empty `sub_regions` is treated as
/// "unrestricted" and always matches (legacy non-DDM subscription/update).
fn regions_overlap_any(
    federation: &Federation,
    pub_regions: &std::collections::HashSet<hla_core::RegionHandle>,
    sub_regions: &std::collections::HashSet<hla_core::RegionHandle>,
) -> bool {
    if pub_regions.is_empty() || sub_regions.is_empty() {
        return true;
    }
    let regions = federation.regions.read();
    for p in pub_regions {
        for s in sub_regions {
            let (Some(rp), Some(rs)) = (regions.get(p), regions.get(s)) else {
                continue;
            };
            // For each dimension shared by both, ranges must intersect.
            // Dimensions only in one are treated as full-range, which
            // always intersects.
            let shared: Vec<&hla_core::DimensionHandle> = rp
                .committed
                .keys()
                .filter(|d| rs.committed.contains_key(d))
                .collect();
            let all_intersect = shared.iter().all(|d| {
                let (lp, up) = rp.committed[d];
                let (ls, us) = rs.committed[d];
                lp <= us && ls <= up
            });
            if all_intersect {
                return true;
            }
        }
    }
    false
}

/// Filter `subscribers` down to those whose region-restricted subscription
/// to (class, attr) overlaps the instance's regions for that attribute.
/// Subscribers without region restrictions pass through unfiltered.
fn filter_subscribers_by_regions(
    federation: &Federation,
    subscribers: std::collections::HashSet<FederateHandle>,
    instance: ObjectInstanceHandle,
    class: ObjectClassHandle,
    attr: AttributeHandle,
) -> std::collections::HashSet<FederateHandle> {
    let pub_regions = {
        let instances = federation.object_instances.read();
        match instances.get(&instance) {
            Some(i) => i.attribute_regions.get(&attr).cloned().unwrap_or_default(),
            None => return std::collections::HashSet::new(),
        }
    };
    if pub_regions.is_empty() {
        // Unrestricted publisher: everyone matches.
        return subscribers;
    }
    let federates = federation.federates.read();
    subscribers
        .into_iter()
        .filter(|fh| {
            let fs = match federates.get(fh) {
                Some(f) => f,
                None => return false,
            };
            let sub_regions = fs
                .pub_sub
                .subscribed_attrs_regions
                .get(&(class, attr))
                .cloned()
                .unwrap_or_default();
            regions_overlap_any(federation, &pub_regions, &sub_regions)
        })
        .collect()
}

// -----------------------------------------------------------------------------
// DDM region lifecycle
// -----------------------------------------------------------------------------

fn create_region(
    node: &Arc<RtiNode>,
    ctx: &SessionContext,
    dimensions: Option<fedpro::DimensionHandleSet>,
) -> Resp {
    use hla_fedpro_proto::fedpro::CreateRegionResponse;
    let _ = node;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let dims: HashMap<hla_core::DimensionHandle, (u32, u32)> = dimensions
        .map(|d| {
            d.dimension_handle
                .iter()
                .filter_map(|h| {
                    if h.data.len() == 4 {
                        let raw = u32::from_be_bytes(h.data[..].try_into().unwrap());
                        Some((hla_core::DimensionHandle::new(raw), (0u32, u32::MAX)))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let raw_id = m.federation.next_region_id.fetch_add(1, Ordering::Relaxed);
    let handle = hla_core::RegionHandle::new(raw_id);
    m.federation.regions.write().insert(
        handle,
        crate::Region {
            handle,
            owner: m.federate_handle,
            committed: dims.clone(),
            staged: dims,
        },
    );
    Resp::CreateRegionResponse(CreateRegionResponse {
        result: Some(fedpro::RegionHandle {
            data: raw_id.to_be_bytes().to_vec(),
        }),
    })
}

fn commit_region_modifications(
    ctx: &SessionContext,
    regions: Option<fedpro::RegionHandleSet>,
) -> Resp {
    use hla_fedpro_proto::fedpro::CommitRegionModificationsResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let region_handles: Vec<hla_core::RegionHandle> = regions
        .map(|s| {
            s.region_handle
                .iter()
                .filter_map(|h| {
                    if h.data.len() == 8 {
                        Some(hla_core::RegionHandle::new(u64::from_be_bytes(
                            h.data[..].try_into().unwrap(),
                        )))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let mut regions = m.federation.regions.write();
    for rh in region_handles {
        if let Some(r) = regions.get_mut(&rh) {
            if r.owner != m.federate_handle {
                return exception_variant(HlaException::RegionNotCreatedByThisFederate, "");
            }
            r.committed = r.staged.clone();
        }
    }
    Resp::CommitRegionModificationsResponse(CommitRegionModificationsResponse {})
}

fn delete_region(ctx: &SessionContext, region: Option<fedpro::RegionHandle>) -> Resp {
    use hla_fedpro_proto::fedpro::DeleteRegionResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let rh = match region {
        Some(h) if h.data.len() == 8 => {
            hla_core::RegionHandle::new(u64::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => return exception_variant(HlaException::InvalidRegion, ""),
    };
    let mut regions = m.federation.regions.write();
    match regions.get(&rh) {
        Some(r) if r.owner == m.federate_handle => {
            regions.remove(&rh);
            Resp::DeleteRegionResponse(DeleteRegionResponse {})
        }
        Some(_) => exception_variant(HlaException::RegionNotCreatedByThisFederate, ""),
        None => exception_variant(HlaException::InvalidRegion, ""),
    }
}

fn get_range_bounds(
    ctx: &SessionContext,
    region: Option<fedpro::RegionHandle>,
    dimension: Option<fedpro::DimensionHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::{GetRangeBoundsResponse, RangeBounds};
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let rh = match region {
        Some(h) if h.data.len() == 8 => {
            hla_core::RegionHandle::new(u64::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => return exception_variant(HlaException::InvalidRegion, ""),
    };
    let dh = match dimension {
        Some(h) if h.data.len() == 4 => {
            hla_core::DimensionHandle::new(u32::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => return exception_variant(HlaException::InvalidDimension, ""),
    };
    let regions = m.federation.regions.read();
    let r = match regions.get(&rh) {
        Some(r) => r,
        None => return exception_variant(HlaException::InvalidRegion, ""),
    };
    let bounds = r.committed.get(&dh).copied().unwrap_or((0, u32::MAX));
    Resp::GetRangeBoundsResponse(GetRangeBoundsResponse {
        result: Some(RangeBounds {
            lower: bounds.0,
            upper: bounds.1,
        }),
    })
}

fn set_range_bounds(
    ctx: &SessionContext,
    region: Option<fedpro::RegionHandle>,
    dimension: Option<fedpro::DimensionHandle>,
    bounds: Option<fedpro::RangeBounds>,
) -> Resp {
    use hla_fedpro_proto::fedpro::SetRangeBoundsResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let rh = match region {
        Some(h) if h.data.len() == 8 => {
            hla_core::RegionHandle::new(u64::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => return exception_variant(HlaException::InvalidRegion, ""),
    };
    let dh = match dimension {
        Some(h) if h.data.len() == 4 => {
            hla_core::DimensionHandle::new(u32::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => return exception_variant(HlaException::InvalidDimension, ""),
    };
    let bounds = match bounds {
        Some(b) if b.lower <= b.upper => (b.lower, b.upper),
        Some(_) => return exception_variant(HlaException::InvalidRangeBound, "lower > upper"),
        None => return exception_variant(HlaException::InvalidRangeBound, "missing"),
    };
    let mut regions = m.federation.regions.write();
    match regions.get_mut(&rh) {
        Some(r) if r.owner == m.federate_handle => {
            r.staged.insert(dh, bounds);
            Resp::SetRangeBoundsResponse(SetRangeBoundsResponse {})
        }
        Some(_) => exception_variant(HlaException::RegionNotCreatedByThisFederate, ""),
        None => exception_variant(HlaException::InvalidRegion, ""),
    }
}

// -----------------------------------------------------------------------------
// Object instance + dimension lookup
// -----------------------------------------------------------------------------

fn get_object_instance_handle(ctx: &SessionContext, name: &str) -> Resp {
    use hla_fedpro_proto::fedpro::GetObjectInstanceHandleResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instances = m.federation.object_instances.read();
    match instances.values().find(|i| i.name == name) {
        Some(inst) => Resp::GetObjectInstanceHandleResponse(GetObjectInstanceHandleResponse {
            result: Some(crate::handles::encode_object_instance(inst.handle)),
        }),
        None => exception_variant(HlaException::ObjectInstanceNotKnown, name),
    }
}

fn get_object_instance_name(
    ctx: &SessionContext,
    instance: Option<fedpro::ObjectInstanceHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::GetObjectInstanceNameResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let instances = m.federation.object_instances.read();
    match instances.get(&h) {
        Some(inst) => Resp::GetObjectInstanceNameResponse(GetObjectInstanceNameResponse {
            result: inst.name.clone(),
        }),
        None => exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    }
}

fn get_known_object_class_handle(
    ctx: &SessionContext,
    instance: Option<fedpro::ObjectInstanceHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::GetKnownObjectClassHandleResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let instances = m.federation.object_instances.read();
    match instances.get(&h) {
        Some(inst) => Resp::GetKnownObjectClassHandleResponse(GetKnownObjectClassHandleResponse {
            result: Some(crate::handles::encode_object_class(inst.class)),
        }),
        None => exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    }
}

fn local_delete_object_instance(
    ctx: &SessionContext,
    instance: Option<fedpro::ObjectInstanceHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::LocalDeleteObjectInstanceResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    // Per IEEE 1516.1: federate forgets about the instance locally (no
    // cross-federation effect). Remove from `discovered_instances`.
    let mut federates = m.federation.federates.write();
    if let Some(fs) = federates.get_mut(&m.federate_handle) {
        fs.pub_sub.discovered_instances.remove(&h);
    }
    Resp::LocalDeleteObjectInstanceResponse(LocalDeleteObjectInstanceResponse {})
}

fn get_dimension_handle(ctx: &SessionContext, name: &str) -> Resp {
    use hla_fedpro_proto::fedpro::GetDimensionHandleResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    match m.federation.fom.dimension_handle(name) {
        Some(h) => Resp::GetDimensionHandleResponse(GetDimensionHandleResponse {
            result: Some(fedpro::DimensionHandle {
                data: h.raw().to_be_bytes().to_vec(),
            }),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

fn get_dimension_name(ctx: &SessionContext, dimension: Option<fedpro::DimensionHandle>) -> Resp {
    use hla_fedpro_proto::fedpro::GetDimensionNameResponse;
    let _ = ctx;
    let h = match dimension {
        Some(d) if d.data.len() == 4 => u32::from_be_bytes(d.data[..].try_into().unwrap()),
        _ => return exception_variant(HlaException::InvalidDimensionHandle, ""),
    };
    // MVP: we don't yet store dimension names by handle. Return the raw id.
    Resp::GetDimensionNameResponse(GetDimensionNameResponse {
        result: format!("Dim{h}"),
    })
}

fn get_dimension_upper_bound(
    ctx: &SessionContext,
    _dimension: Option<fedpro::DimensionHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::GetDimensionUpperBoundResponse;
    let _ = ctx;
    // FOM-defined upper bound; default u32::MAX for MVP.
    Resp::GetDimensionUpperBoundResponse(GetDimensionUpperBoundResponse { result: u32::MAX })
}

fn get_dimension_handle_set(ctx: &SessionContext, region: Option<fedpro::RegionHandle>) -> Resp {
    use hla_fedpro_proto::fedpro::{DimensionHandleSet, GetDimensionHandleSetResponse};
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let rh = match region {
        Some(h) if h.data.len() == 8 => {
            hla_core::RegionHandle::new(u64::from_be_bytes(h.data[..].try_into().unwrap()))
        }
        _ => return exception_variant(HlaException::InvalidRegion, ""),
    };
    let regions = m.federation.regions.read();
    let r = match regions.get(&rh) {
        Some(r) => r,
        None => return exception_variant(HlaException::InvalidRegion, ""),
    };
    let dims: Vec<fedpro::DimensionHandle> = r
        .committed
        .keys()
        .map(|d| fedpro::DimensionHandle {
            data: d.raw().to_be_bytes().to_vec(),
        })
        .collect();
    Resp::GetDimensionHandleSetResponse(GetDimensionHandleSetResponse {
        result: Some(DimensionHandleSet {
            dimension_handle: dims,
        }),
    })
}

fn get_available_dimensions_for_object_class(
    ctx: &SessionContext,
    _class: Option<fedpro::ObjectClassHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::{
        DimensionHandleSet, GetAvailableDimensionsForObjectClassResponse,
    };
    let _ = ctx;
    // MVP: we don't yet store per-class dimension associations. Empty set.
    Resp::GetAvailableDimensionsForObjectClassResponse(
        GetAvailableDimensionsForObjectClassResponse {
            result: Some(DimensionHandleSet {
                dimension_handle: Vec::new(),
            }),
        },
    )
}

fn get_available_dimensions_for_interaction_class(
    ctx: &SessionContext,
    _class: Option<fedpro::InteractionClassHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::{
        DimensionHandleSet, GetAvailableDimensionsForInteractionClassResponse,
    };
    let _ = ctx;
    Resp::GetAvailableDimensionsForInteractionClassResponse(
        GetAvailableDimensionsForInteractionClassResponse {
            result: Some(DimensionHandleSet {
                dimension_handle: Vec::new(),
            }),
        },
    )
}

// -----------------------------------------------------------------------------
// Advisory / Reporting Switches (per IEEE 1516.1 §6)
// -----------------------------------------------------------------------------

fn get_switch_bool<G, W>(ctx: &SessionContext, getter: G, wrap: W) -> Resp
where
    G: Fn(&crate::Switches) -> bool,
    W: Fn(bool) -> Resp,
{
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    match federates.get(&m.federate_handle) {
        Some(fs) => wrap(getter(&fs.switches)),
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

fn set_switch_bool<S, W>(ctx: &mut SessionContext, value: bool, setter: S, wrap: W) -> Resp
where
    S: Fn(&mut crate::Switches, bool),
    W: Fn() -> Resp,
{
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut federates = m.federation.federates.write();
    match federates.get_mut(&m.federate_handle) {
        Some(fs) => {
            setter(&mut fs.switches, value);
            wrap()
        }
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

fn get_automatic_resign_directive(ctx: &SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::GetAutomaticResignDirectiveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    let fs = match federates.get(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    Resp::GetAutomaticResignDirectiveResponse(GetAutomaticResignDirectiveResponse {
        result: encode_resign_action(fs.switches.automatic_resign_directive),
    })
}

fn set_automatic_resign_directive(ctx: &mut SessionContext, value: i32) -> Resp {
    use hla_fedpro_proto::fedpro::SetAutomaticResignDirectiveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let action = match value {
        0 => hla_core::ResignAction::UnconditionallyDivestAttributes,
        1 => hla_core::ResignAction::DeleteObjects,
        2 => hla_core::ResignAction::CancelPendingOwnershipAcquisitions,
        3 => hla_core::ResignAction::DeleteObjectsThenDivest,
        4 => hla_core::ResignAction::CancelThenDeleteThenDivest,
        5 => hla_core::ResignAction::NoAction,
        other => return exception_variant(HlaException::InvalidResignAction, &other.to_string()),
    };
    let mut federates = m.federation.federates.write();
    match federates.get_mut(&m.federate_handle) {
        Some(fs) => {
            fs.switches.automatic_resign_directive = action;
            Resp::SetAutomaticResignDirectiveResponse(SetAutomaticResignDirectiveResponse {})
        }
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

fn encode_resign_action(a: hla_core::ResignAction) -> i32 {
    match a {
        hla_core::ResignAction::UnconditionallyDivestAttributes => 0,
        hla_core::ResignAction::DeleteObjects => 1,
        hla_core::ResignAction::CancelPendingOwnershipAcquisitions => 2,
        hla_core::ResignAction::DeleteObjectsThenDivest => 3,
        hla_core::ResignAction::CancelThenDeleteThenDivest => 4,
        hla_core::ResignAction::NoAction => 5,
    }
}

// -----------------------------------------------------------------------------
// Federate self-introspection + well-known enum/string lookups
// -----------------------------------------------------------------------------

fn get_federate_handle(ctx: &SessionContext, name: &str) -> Resp {
    use crate::handles::encode_federate;
    use hla_fedpro_proto::fedpro::GetFederateHandleResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    match federates.values().find(|f| f.name == name) {
        Some(fs) => Resp::GetFederateHandleResponse(GetFederateHandleResponse {
            result: Some(encode_federate(fs.handle)),
        }),
        None => exception_variant(HlaException::NameNotFound, name),
    }
}

fn get_federate_name(ctx: &SessionContext, handle: fedpro::FederateHandle) -> Resp {
    use hla_fedpro_proto::fedpro::GetFederateNameResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let fh = match handle.data.len() {
        4 => FederateHandle::new(u32::from_be_bytes(handle.data[..].try_into().unwrap())),
        _ => return exception_variant(HlaException::InvalidFederateHandle, ""),
    };
    let federates = m.federation.federates.read();
    match federates.get(&fh) {
        Some(fs) => Resp::GetFederateNameResponse(GetFederateNameResponse {
            result: fs.name.clone(),
        }),
        None => exception_variant(HlaException::InvalidFederateHandle, ""),
    }
}

/// Well-known order types per IEEE 1516.1: Receive (0), TimeStamp (1).
fn get_order_type(name: &str) -> Resp {
    use hla_fedpro_proto::fedpro::GetOrderTypeResponse;
    let raw = match name {
        "Receive" => 0i32,
        "TimeStamp" | "TimestampOrder" => 1i32,
        _ => return exception_variant(HlaException::InvalidOrderName, name),
    };
    Resp::GetOrderTypeResponse(GetOrderTypeResponse { result: raw })
}

fn get_order_name(order_type: i32) -> Resp {
    use hla_fedpro_proto::fedpro::GetOrderNameResponse;
    let name = match order_type {
        0 => "Receive",
        1 => "TimeStamp",
        _ => return exception_variant(HlaException::InvalidOrderType, &order_type.to_string()),
    };
    Resp::GetOrderNameResponse(GetOrderNameResponse {
        result: name.to_string(),
    })
}

/// Well-known transportation types per IEEE 1516.1: HLAreliable (1), HLAbestEffort (2).
fn get_transportation_type_handle(name: &str) -> Resp {
    use hla_fedpro_proto::fedpro::{GetTransportationTypeHandleResponse, TransportationTypeHandle};
    let raw: u32 = match name {
        "HLAreliable" => 1,
        "HLAbestEffort" => 2,
        _ => return exception_variant(HlaException::InvalidTransportationName, name),
    };
    Resp::GetTransportationTypeHandleResponse(GetTransportationTypeHandleResponse {
        result: Some(TransportationTypeHandle {
            data: raw.to_be_bytes().to_vec(),
        }),
    })
}

fn get_transportation_type_name(handle: fedpro::TransportationTypeHandle) -> Resp {
    use hla_fedpro_proto::fedpro::GetTransportationTypeNameResponse;
    if handle.data.len() != 4 {
        return exception_variant(HlaException::InvalidTransportationTypeHandle, "");
    }
    let raw = u32::from_be_bytes(handle.data[..].try_into().unwrap());
    let name = match raw {
        1 => "HLAreliable",
        2 => "HLAbestEffort",
        _ => {
            return exception_variant(
                HlaException::InvalidTransportationTypeHandle,
                &raw.to_string(),
            );
        }
    };
    Resp::GetTransportationTypeNameResponse(GetTransportationTypeNameResponse {
        result: name.to_string(),
    })
}

// -----------------------------------------------------------------------------
// Federation Save (MVP: orchestration only)
// -----------------------------------------------------------------------------

fn request_federation_save(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    label: String,
) -> Resp {
    use hla_fedpro_proto::fedpro::RequestFederationSaveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if label.is_empty() {
        return exception_variant(HlaException::InvalidSaveLabel, "");
    }

    let participants: Vec<FederateHandle> =
        { m.federation.federates.read().keys().copied().collect() };

    {
        let mut current = m.federation.current_save.write();
        if current.is_some() {
            return exception_variant(HlaException::SaveInProgress, "");
        }
        *current = Some(SaveOperation {
            label: label.clone(),
            statuses: participants
                .iter()
                .copied()
                .map(|fh| (fh, SaveStatus::Initiated))
                .collect(),
        });
        *m.federation.last_save_label.write() = Some(label.clone());
    }

    // Broadcast InitiateFederateSave to every participant.
    let target_set: std::collections::HashSet<FederateHandle> =
        participants.iter().copied().collect();
    let conns = live_connections(node, &m.federation, &target_set);
    fan_out(callbacks, &conns, initiate_federate_save(&label));

    Resp::RequestFederationSaveResponse(RequestFederationSaveResponse {})
}

fn federate_save_begun(ctx: &mut SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::FederateSaveBegunResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut current = m.federation.current_save.write();
    let save = match current.as_mut() {
        Some(s) => s,
        None => return exception_variant(HlaException::SaveNotInProgress, ""),
    };
    match save.statuses.get(&m.federate_handle) {
        Some(SaveStatus::Initiated) => {
            save.statuses
                .insert(m.federate_handle, SaveStatus::BegunSave);
            Resp::FederateSaveBegunResponse(FederateSaveBegunResponse {})
        }
        Some(_) => exception_variant(HlaException::FederateNotInSaveInitiated, ""),
        None => exception_variant(HlaException::FederateNotInSaveSet, ""),
    }
}

fn federate_save_progressed(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    successfully: bool,
) -> Resp {
    use hla_fedpro_proto::fedpro::{FederateSaveCompleteResponse, FederateSaveNotCompleteResponse};
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };

    let outcome = {
        let mut current = m.federation.current_save.write();
        let save = match current.as_mut() {
            Some(s) => s,
            None => return exception_variant(HlaException::SaveNotInProgress, ""),
        };
        let new_status = if successfully {
            SaveStatus::SaveComplete
        } else {
            SaveStatus::SaveNotComplete
        };
        save.statuses.insert(m.federate_handle, new_status);
        let any_failed = save
            .statuses
            .values()
            .any(|s| *s == SaveStatus::SaveNotComplete);
        let all_done = save
            .statuses
            .values()
            .all(|s| matches!(s, SaveStatus::SaveComplete | SaveStatus::SaveNotComplete));
        if all_done {
            current.take();
            Some(any_failed)
        } else {
            None
        }
    };

    if let Some(any_failed) = outcome {
        let participants: std::collections::HashSet<FederateHandle> =
            m.federation.federates.read().keys().copied().collect();
        let conns = live_connections(node, &m.federation, &participants);
        let cb = if any_failed {
            federation_not_saved(0)
        } else {
            // Successful save → write the snapshot to disk before notifying
            // federates. Use the label captured from current_save (consumed
            // already above), so recover from federation.current_save... no,
            // it's already been taken. We need to re-derive the label.
            // For simplicity, when the orchestration completes, look at the
            // last save label by reading what was just written.
            // Actually we need to know the label. Let me capture it differently.
            persist_federation_snapshot_on_success(node, &m.federation);
            federation_saved()
        };
        fan_out(callbacks, &conns, cb);
    }

    if successfully {
        Resp::FederateSaveCompleteResponse(FederateSaveCompleteResponse {})
    } else {
        Resp::FederateSaveNotCompleteResponse(FederateSaveNotCompleteResponse {})
    }
}

/// On successful federation-save completion, persist a snapshot to disk.
/// The label is recovered from the recently-cleared `current_save` — but
/// since the dispatch arm that called this already cleared it, we read
/// from a side-channel: we tag the federation with `last_save_label` when
/// save begins, and use that here.
fn persist_federation_snapshot_on_success(node: &Arc<RtiNode>, federation: &Federation) {
    let label = federation.last_save_label.read().clone();
    let Some(label) = label else { return };
    let dir = node.save_dir.read().clone();
    let snap = federation.snapshot();
    if let Err(e) = crate::persistence::write_snapshot(&dir, &federation.name, &label, &snap) {
        tracing::warn!(error = %e, "snapshot write failed");
    } else {
        tracing::info!(label, "snapshot written");
    }
}

fn abort_federation_save(ctx: &mut SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::AbortFederationSaveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut current = m.federation.current_save.write();
    if current.is_none() {
        return exception_variant(HlaException::SaveNotInProgress, "");
    }
    current.take();
    Resp::AbortFederationSaveResponse(AbortFederationSaveResponse {})
}

// -----------------------------------------------------------------------------
// Ownership Management (MVP slice)
// -----------------------------------------------------------------------------

fn is_attribute_owned_by_federate(
    ctx: &SessionContext,
    instance: Option<fedpro::ObjectInstanceHandle>,
    attribute: Option<fedpro::AttributeHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::IsAttributeOwnedByFederateResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let attribute = match attribute.and_then(|h| decode_attribute(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::InvalidAttributeHandle, ""),
    };
    let instances = m.federation.object_instances.read();
    let inst = match instances.get(&instance) {
        Some(i) => i,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let owned = inst.attribute_owners.get(&attribute) == Some(&m.federate_handle);
    Resp::IsAttributeOwnedByFederateResponse(IsAttributeOwnedByFederateResponse { result: owned })
}

fn query_attribute_ownership(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    attrs: Option<fedpro::AttributeHandleSet>,
) -> Resp {
    use hla_fedpro_proto::fedpro::QueryAttributeOwnershipResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let attrs = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Group attributes by owner; emit InformAttributeOwnership per owner.
    let owner_groups: std::collections::HashMap<Option<FederateHandle>, Vec<AttributeHandle>> = {
        let instances = m.federation.object_instances.read();
        let inst = match instances.get(&instance_h) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        let mut groups: std::collections::HashMap<Option<FederateHandle>, Vec<AttributeHandle>> =
            std::collections::HashMap::new();
        for a in &attrs {
            let owner = inst.attribute_owners.get(a).copied();
            groups.entry(owner).or_default().push(*a);
        }
        groups
    };

    let registrant = live_connections(node, &m.federation, &single(m.federate_handle));
    for (owner, attrs_for_owner) in owner_groups {
        let cb = match owner {
            Some(o) => inform_attribute_ownership(instance_h, &attrs_for_owner, o),
            None => attribute_is_not_owned(instance_h, &attrs_for_owner),
        };
        fan_out(callbacks, &registrant, cb);
    }
    Resp::QueryAttributeOwnershipResponse(QueryAttributeOwnershipResponse {})
}

fn attribute_ownership_acquisition_if_available(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    attrs: Option<fedpro::AttributeHandleSet>,
    tag: Vec<u8>,
) -> Resp {
    use hla_fedpro_proto::fedpro::AttributeOwnershipAcquisitionIfAvailableResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let attrs = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let (secured, unavailable) = {
        let mut instances = m.federation.object_instances.write();
        let inst = match instances.get_mut(&instance_h) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        let mut secured = Vec::new();
        let mut unavailable = Vec::new();
        for a in &attrs {
            match inst.attribute_owners.get(a) {
                Some(_) => unavailable.push(*a), // already owned
                None => {
                    inst.attribute_owners.insert(*a, m.federate_handle);
                    secured.push(*a);
                }
            }
        }
        (secured, unavailable)
    };

    let self_conn = live_connections(node, &m.federation, &single(m.federate_handle));
    if !secured.is_empty() {
        fan_out(
            callbacks,
            &self_conn,
            attribute_ownership_acquisition_notification(instance_h, &secured, &tag),
        );
    }
    if !unavailable.is_empty() {
        fan_out(
            callbacks,
            &self_conn,
            attribute_ownership_unavailable(instance_h, &unavailable, &tag),
        );
    }

    Resp::AttributeOwnershipAcquisitionIfAvailableResponse(
        AttributeOwnershipAcquisitionIfAvailableResponse {},
    )
}

fn unconditional_attribute_ownership_divestiture(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    instance: Option<fedpro::ObjectInstanceHandle>,
    attrs: Option<fedpro::AttributeHandleSet>,
    _tag: Vec<u8>,
) -> Resp {
    use hla_fedpro_proto::fedpro::UnconditionalAttributeOwnershipDivestitureResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let instance_h = match instance.and_then(|h| decode_object_instance(&h).ok()) {
        Some(h) => h,
        None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
    };
    let attrs = match decode_handle_set(attrs) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Remove ownership for each attribute we currently own.
    let divested: Vec<AttributeHandle> = {
        let mut instances = m.federation.object_instances.write();
        let inst = match instances.get_mut(&instance_h) {
            Some(i) => i,
            None => return exception_variant(HlaException::ObjectInstanceNotKnown, ""),
        };
        let mut divested = Vec::new();
        for a in &attrs {
            match inst.attribute_owners.get(a) {
                Some(o) if *o == m.federate_handle => {
                    inst.attribute_owners.remove(a);
                    divested.push(*a);
                }
                _ => {
                    // Silently skip — divesting an attribute we don't own
                    // is per-spec not an error; subset semantics apply.
                }
            }
        }
        divested
    };

    if !divested.is_empty() {
        // Notify every subscriber to (class, attr) that those attributes
        // are now unowned. Real impl would also offer them to candidates via
        // RequestAttributeOwnershipAssumption — deferred for MVP.
        let class = {
            let instances = m.federation.object_instances.read();
            instances.get(&instance_h).map(|i| i.class)
        };
        if let Some(class) = class {
            let subscribers = subscribers_for_attributes(&m.federation, class, &divested, None);
            let conns = live_connections(node, &m.federation, &subscribers);
            fan_out(
                callbacks,
                &conns,
                attribute_is_not_owned(instance_h, &divested),
            );
        }
    }

    Resp::UnconditionalAttributeOwnershipDivestitureResponse(
        UnconditionalAttributeOwnershipDivestitureResponse {},
    )
}

// -----------------------------------------------------------------------------
// Synchronization Points
// -----------------------------------------------------------------------------

fn register_synchronization_point(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    label: String,
    tag: Vec<u8>,
    participant_subset: Option<std::collections::HashSet<FederateHandle>>,
) -> Resp {
    use hla_fedpro_proto::fedpro::RegisterFederationSynchronizationPointResponse;

    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if label.is_empty() {
        return exception_variant(
            HlaException::InvalidSynchronizationPointLabel,
            "label must not be empty",
        );
    }

    // Determine participants: explicit subset or all currently-joined federates.
    let participants: std::collections::HashSet<FederateHandle> = match participant_subset {
        Some(s) if !s.is_empty() => s,
        _ => m.federation.federates.read().keys().copied().collect(),
    };

    // Reserve the slot under write lock so concurrent registers fail.
    let inserted = {
        let mut sync_points = m.federation.sync_points.write();
        if sync_points.contains_key(&label) {
            false
        } else {
            sync_points.insert(
                label.clone(),
                SyncPoint {
                    label: label.clone(),
                    tag: tag.clone(),
                    participants: participants.clone(),
                    achieved: std::collections::HashSet::new(),
                    failed_to_sync: std::collections::HashSet::new(),
                },
            );
            true
        }
    };

    if !inserted {
        // Failed-registration callback → only to the registrant.
        let registrant = live_connections(node, &m.federation, &single(m.federate_handle));
        fan_out(
            callbacks,
            &registrant,
            synchronization_point_registration_failed(&label, 0), // 0 = LABEL_NOT_UNIQUE
        );
    } else {
        // Succeeded callback → to the registrant.
        let registrant = live_connections(node, &m.federation, &single(m.federate_handle));
        fan_out(
            callbacks,
            &registrant,
            synchronization_point_registration_succeeded(&label),
        );
        // Announce callback → to every participant.
        let participants_conns = live_connections(node, &m.federation, &participants);
        fan_out(
            callbacks,
            &participants_conns,
            announce_synchronization_point(&label, &tag),
        );
    }

    Resp::RegisterFederationSynchronizationPointResponse(
        RegisterFederationSynchronizationPointResponse {},
    )
}

fn synchronization_point_achieved(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    label: String,
    successfully: bool,
) -> Resp {
    use hla_fedpro_proto::fedpro::SynchronizationPointAchievedResponse;

    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };

    let synced_now = {
        let mut sync_points = m.federation.sync_points.write();
        let sp = match sync_points.get_mut(&label) {
            Some(s) => s,
            None => {
                return exception_variant(
                    HlaException::SynchronizationPointLabelNotAnnounced,
                    &label,
                );
            }
        };
        if !sp.participants.contains(&m.federate_handle) {
            return exception_variant(
                HlaException::FederateNotInSynchronizationGroup,
                &format!(
                    "federate {} not in sync set for {label}",
                    m.federate_handle.raw()
                ),
            );
        }
        sp.achieved.insert(m.federate_handle);
        if !successfully {
            sp.failed_to_sync.insert(m.federate_handle);
        }
        if sp.achieved.len() == sp.participants.len() {
            // All achieved — pull the sync point out and emit FederationSynchronized.
            Some(sync_points.remove(&label).unwrap())
        } else {
            None
        }
    };

    if let Some(sp) = synced_now {
        let conns = live_connections(node, &m.federation, &sp.participants);
        fan_out(
            callbacks,
            &conns,
            federation_synchronized(&label, &sp.failed_to_sync),
        );
    }

    Resp::SynchronizationPointAchievedResponse(SynchronizationPointAchievedResponse {})
}

// -----------------------------------------------------------------------------
// Time Management
// -----------------------------------------------------------------------------

fn enable_time_regulation(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    lookahead: Option<fedpro::LogicalTimeInterval>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let lookahead = match lookahead.as_ref().map(decode_logical_time_interval) {
        Some(Ok(v)) if v.is_finite() && v >= 0.0 => v,
        Some(Ok(v)) => return exception_variant(HlaException::InvalidLookahead, &v.to_string()),
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLookahead, "missing"),
    };

    let now = {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if fs.time.is_regulating {
            return exception_variant(HlaException::TimeRegulationAlreadyEnabled, "");
        }
        fs.time.is_regulating = true;
        fs.time.lookahead = lookahead;
        fs.time.current_time
    };

    // Emit TimeRegulationEnabled at the federate's current_time.
    let connections = live_connections(node, &m.federation, &single(m.federate_handle));
    fan_out(callbacks, &connections, time_regulation_enabled(now));

    // New regulator may have raised LBTS (it was previously ∞ if nobody was
    // regulating) or, with a small lookahead at time 0, lowered it. Either
    // way, re-evaluate pending advances.
    try_grant_pending_advances(node, &m.federation, callbacks);

    Resp::EnableTimeRegulationResponse(EnableTimeRegulationResponse {})
}

fn disable_time_regulation(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if !fs.time.is_regulating {
            return exception_variant(HlaException::TimeRegulationIsNotEnabled, "");
        }
        fs.time.is_regulating = false;
    }
    try_grant_pending_advances(node, &m.federation, callbacks);
    Resp::DisableTimeRegulationResponse(DisableTimeRegulationResponse {})
}

fn enable_time_constrained(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let now = {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if fs.time.is_constrained {
            return exception_variant(HlaException::TimeConstrainedAlreadyEnabled, "");
        }
        fs.time.is_constrained = true;
        fs.time.current_time
    };
    let connections = live_connections(node, &m.federation, &single(m.federate_handle));
    fan_out(callbacks, &connections, time_constrained_enabled(now));
    Resp::EnableTimeConstrainedResponse(EnableTimeConstrainedResponse {})
}

fn disable_time_constrained(ctx: &mut SessionContext) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut federates = m.federation.federates.write();
    let fs = match federates.get_mut(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if !fs.time.is_constrained {
        return exception_variant(HlaException::TimeConstrainedIsNotEnabled, "");
    }
    fs.time.is_constrained = false;
    Resp::DisableTimeConstrainedResponse(DisableTimeConstrainedResponse {})
}

fn time_advance_request(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    time: Option<fedpro::LogicalTime>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let requested = match time.as_ref().map(decode_logical_time) {
        Some(Ok(v)) if v.is_finite() => v,
        Some(Ok(v)) => {
            return exception_variant(HlaException::LogicalTimeAlreadyPassed, &v.to_string());
        }
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLogicalTime, "missing"),
    };

    let (already_grantable, grant_time) = {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if fs.time.pending_advance.is_some() {
            return exception_variant(HlaException::InTimeAdvancingState, "");
        }
        if requested < fs.time.current_time {
            return exception_variant(HlaException::LogicalTimeAlreadyPassed, "");
        }
        fs.time.pending_advance = Some(requested);
        (!fs.time.is_constrained, requested)
    };

    // Unconstrained federates: grant immediately. Even unconstrained
    // federates may have queued TSO messages (if they were briefly
    // constrained earlier), so we still drain.
    if already_grantable {
        let mut federates = m.federation.federates.write();
        if let Some(fs) = federates.get_mut(&m.federate_handle)
            && fs.time.pending_advance == Some(grant_time)
        {
            fs.time.current_time = grant_time;
            fs.time.pending_advance = None;
        }
        drop(federates);
        // Drain TSO queue first, then deliver TAG. IEEE 1516.1 §8: TSO
        // messages with timestamp ≤ grant_time must arrive before the grant.
        drain_tso_up_to(
            node,
            &m.federation,
            callbacks,
            m.federate_handle,
            grant_time,
        );
        let connections = live_connections(node, &m.federation, &single(m.federate_handle));
        fan_out(callbacks, &connections, time_advance_grant(grant_time));
        try_grant_pending_advances_with_tso(node, &m.federation, callbacks);
        return Resp::TimeAdvanceRequestResponse(TimeAdvanceRequestResponse {});
    }

    // Constrained: see if LBTS already permits.
    try_grant_pending_advances_with_tso(node, &m.federation, callbacks);

    Resp::TimeAdvanceRequestResponse(TimeAdvanceRequestResponse {})
}

/// Wrapper around `try_grant_pending_advances` that also drains each
/// granted federate's TSO queue *before* the TAG callback is emitted.
fn try_grant_pending_advances_with_tso(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
) {
    // Snapshot pending advances pre-grant so we know which federates will
    // be granted and at what time.
    let pending: Vec<(FederateHandle, f64)> = {
        let federates = federation.federates.read();
        federates
            .iter()
            .filter_map(|(&fh, fs)| fs.time.pending_advance.map(|t| (fh, t)))
            .collect()
    };
    let bound = crate::time::lbts(federation);
    let to_grant: Vec<(FederateHandle, f64)> = pending
        .into_iter()
        .filter(|(fh, t)| {
            let federates = federation.federates.read();
            let fs = match federates.get(fh) {
                Some(f) => f,
                None => return false,
            };
            !fs.time.is_constrained || *t <= bound
        })
        .collect();

    // Drain TSO before grant for each.
    for (fh, t) in &to_grant {
        drain_tso_up_to(node, federation, callbacks, *fh, *t);
    }
    // Then run the normal grant machinery (emits TAG).
    crate::time::try_grant_pending_advances(node, federation, callbacks);
}

fn query_logical_time(ctx: &SessionContext) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    let fs = match federates.get(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    Resp::QueryLogicalTimeResponse(QueryLogicalTimeResponse {
        result: Some(encode_logical_time(fs.time.current_time)),
    })
}

fn modify_lookahead(
    ctx: &mut SessionContext,
    lookahead: Option<fedpro::LogicalTimeInterval>,
) -> Resp {
    use hla_fedpro_proto::fedpro::ModifyLookaheadResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let lookahead = match lookahead.as_ref().map(decode_logical_time_interval) {
        Some(Ok(v)) if v.is_finite() && v >= 0.0 => v,
        _ => return exception_variant(HlaException::InvalidLookahead, ""),
    };
    let mut federates = m.federation.federates.write();
    match federates.get_mut(&m.federate_handle) {
        Some(fs) => {
            if !fs.time.is_regulating {
                return exception_variant(HlaException::TimeRegulationIsNotEnabled, "");
            }
            fs.time.lookahead = lookahead;
            Resp::ModifyLookaheadResponse(ModifyLookaheadResponse {})
        }
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

fn query_lits(ctx: &SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::{QueryLitsResponse, TimeQueryReturn};
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let lbts = crate::time::lbts(&m.federation);
    Resp::QueryLitsResponse(QueryLitsResponse {
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

fn query_lookahead(ctx: &SessionContext) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    let fs = match federates.get(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if !fs.time.is_regulating {
        return exception_variant(HlaException::TimeRegulationIsNotEnabled, "");
    }
    Resp::QueryLookaheadResponse(QueryLookaheadResponse {
        result: Some(encode_logical_time_interval(fs.time.lookahead)),
    })
}

fn single(fh: FederateHandle) -> std::collections::HashSet<FederateHandle> {
    let mut s = std::collections::HashSet::new();
    s.insert(fh);
    s
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

use crate::exception::HlaException;

fn exception(kind: HlaException, details: &str) -> fedpro::CallResponse {
    fedpro::CallResponse {
        call_response: Some(exception_variant(kind, details)),
    }
}

fn exception_variant(kind: HlaException, details: &str) -> Resp {
    Resp::ExceptionData(ExceptionData {
        exception_name: kind.name().to_string(),
        details: details.to_string(),
    })
}

// Silence "imported but not used" if at some point we trim Re-exports.
#[allow(dead_code)]
fn _force_use_objectclass_handle_type(_: ObjectClassHandle) {}
