//! `FederateAmbassador` trait — the federate's inbound callback surface.
//!
//! All methods have default no-op implementations so a federate can override
//! only what it needs. The pump task in [`crate::ambassador`] decodes
//! `HLA_CALLBACK_REQUEST` frames and dispatches them to the appropriate
//! method via this trait.
//!
//! ## No `async_trait`
//!
//! Methods use native AFIT (async fn in traits, stable since Rust 1.75) with
//! an explicit `-> impl Future<Output = ()> + Send + '_` return type so the
//! generic dispatch path can `tokio::spawn` callback futures without a
//! per-call `Box<dyn Future>` allocation. User impls may either write
//! `async fn foo(&self, ...) { ... }` (the compiler matches the trait's
//! return type as long as the body is `Send`) or the explicit
//! `-> impl Future + Send + '_` form.

use std::collections::HashSet;
use std::future::Future;

use hla_core::{
    AttributeHandle, AttributeHandleValueMap, FederateHandle, InteractionClassHandle,
    ObjectClassHandle, ObjectInstanceHandle, ParameterHandleValueMap,
};

/// Inbound RTI → federate callbacks. Implemented by federate code.
///
/// Default implementations are no-ops; override the methods relevant to the
/// federate's logic.
#[allow(clippy::too_many_arguments)]
pub trait FederateAmbassador: Send + Sync + 'static {
    fn discover_object_instance(
        &self,
        _instance: ObjectInstanceHandle,
        _class: ObjectClassHandle,
        _name: String,
        _producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn reflect_attribute_values(
        &self,
        _instance: ObjectInstanceHandle,
        _values: AttributeHandleValueMap,
        _user_supplied_tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn receive_interaction(
        &self,
        _class: InteractionClassHandle,
        _params: ParameterHandleValueMap,
        _user_supplied_tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn remove_object_instance(
        &self,
        _instance: ObjectInstanceHandle,
        _user_supplied_tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn time_regulation_enabled(&self, _time: f64) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn time_constrained_enabled(&self, _time: f64) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn time_advance_grant(&self, _time: f64) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn inform_attribute_ownership(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
        _owner: FederateHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn report_federation_executions(
        &self,
        _federations: Vec<String>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn report_federation_execution_members(
        &self,
        _federation_name: String,
        _members: Vec<(String, String)>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn report_federation_execution_does_not_exist(
        &self,
        _federation_name: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn initiate_federate_save(&self, _label: String) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_saved(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_not_saved(&self, _reason: i32) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn request_federation_restore_succeeded(
        &self,
        _label: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn request_federation_restore_failed(
        &self,
        _label: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_restore_begun(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn initiate_federate_restore(
        &self,
        _label: String,
        _federate_name: String,
        _post_restore_handle: FederateHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_restored(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_not_restored(&self, _reason: i32) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn attribute_is_not_owned(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn attribute_ownership_acquisition_notification(
        &self,
        _instance: ObjectInstanceHandle,
        _secured: Vec<AttributeHandle>,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn attribute_ownership_unavailable(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    fn synchronization_point_registration_succeeded(
        &self,
        _label: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn synchronization_point_registration_failed(
        &self,
        _label: String,
        _reason: i32,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn announce_synchronization_point(
        &self,
        _label: String,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_synchronized(
        &self,
        _label: String,
        _failed_to_sync: HashSet<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Time-stamped variants of the core data-flow callbacks.
    fn reflect_attribute_values_with_time(
        &self,
        _instance: ObjectInstanceHandle,
        _values: AttributeHandleValueMap,
        _user_supplied_tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
        _time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn receive_interaction_with_time(
        &self,
        _class: InteractionClassHandle,
        _params: ParameterHandleValueMap,
        _user_supplied_tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
        _time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn remove_object_instance_with_time(
        &self,
        _instance: ObjectInstanceHandle,
        _user_supplied_tag: Vec<u8>,
        _producing_federate: Option<FederateHandle>,
        _time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Reservation outcomes.
    fn object_instance_name_reservation_failed(
        &self,
        _name: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn multiple_object_instance_name_reservation_succeeded(
        &self,
        _names: Vec<String>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn multiple_object_instance_name_reservation_failed(
        &self,
        _names: Vec<String>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Advisory callbacks (publish-side).
    fn start_registration_for_object_class(
        &self,
        _class: ObjectClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn stop_registration_for_object_class(
        &self,
        _class: ObjectClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn turn_interactions_on(
        &self,
        _class: InteractionClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn turn_interactions_off(
        &self,
        _class: InteractionClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn turn_updates_on_for_object_instance(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn turn_updates_off_for_object_instance(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn attributes_in_scope(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn attributes_out_of_scope(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn provide_attribute_value_update(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Ownership negotiation callbacks (full state machine).
    fn request_attribute_ownership_assumption(
        &self,
        _instance: ObjectInstanceHandle,
        _offered: Vec<AttributeHandle>,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn request_attribute_ownership_release(
        &self,
        _instance: ObjectInstanceHandle,
        _candidate: Vec<AttributeHandle>,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn request_divestiture_confirmation(
        &self,
        _instance: ObjectInstanceHandle,
        _released: Vec<AttributeHandle>,
        _user_supplied_tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn attribute_is_owned_by_rti(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn confirm_attribute_ownership_acquisition_cancellation(
        &self,
        _instance: ObjectInstanceHandle,
        _attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Time-management retraction + flush.
    fn request_retraction(
        &self,
        _retraction_handle: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn flush_queue_grant(
        &self,
        _time: f64,
        _optional_next_message: Option<f64>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Save-status / restore-status / connection-lost / federate-resigned / save-with-time.
    fn initiate_federate_save_with_time(
        &self,
        _label: String,
        _time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_save_status_response(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federation_restore_status_response(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn connection_lost(&self, _reason: String) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn federate_resigned(&self, _reason: String) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    // Transportation-type reports.
    fn report_attribute_transportation_type(
        &self,
        _instance: ObjectInstanceHandle,
        _attribute: AttributeHandle,
        _transportation: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
    fn report_interaction_transportation_type(
        &self,
        _federate: FederateHandle,
        _interaction: InteractionClassHandle,
        _transportation: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }

    /// Catch-all for callbacks we haven't typed yet.
    fn raw_callback<'a>(&'a self, _kind: &'a str) -> impl Future<Output = ()> + Send + 'a {
        async {}
    }
}

// Blanket impl for `Arc<T>` so callers can share state across threads by
// passing an `Arc<MyAmbassador>` directly. Each method just delegates.
impl<T: FederateAmbassador> FederateAmbassador for std::sync::Arc<T> {
    fn discover_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        class: ObjectClassHandle,
        name: String,
        producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).discover_object_instance(instance, class, name, producing_federate)
    }

    fn reflect_attribute_values(
        &self,
        instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        user_supplied_tag: Vec<u8>,
        producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).reflect_attribute_values(instance, values, user_supplied_tag, producing_federate)
    }

    fn receive_interaction(
        &self,
        class: InteractionClassHandle,
        params: ParameterHandleValueMap,
        user_supplied_tag: Vec<u8>,
        producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).receive_interaction(class, params, user_supplied_tag, producing_federate)
    }

    fn remove_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        user_supplied_tag: Vec<u8>,
        producing_federate: Option<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).remove_object_instance(instance, user_supplied_tag, producing_federate)
    }

    fn time_regulation_enabled(&self, time: f64) -> impl Future<Output = ()> + Send + '_ {
        (**self).time_regulation_enabled(time)
    }
    fn time_constrained_enabled(&self, time: f64) -> impl Future<Output = ()> + Send + '_ {
        (**self).time_constrained_enabled(time)
    }
    fn time_advance_grant(&self, time: f64) -> impl Future<Output = ()> + Send + '_ {
        (**self).time_advance_grant(time)
    }
    fn inform_attribute_ownership(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
        owner: FederateHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).inform_attribute_ownership(instance, attributes, owner)
    }
    fn report_federation_executions(
        &self,
        federations: Vec<String>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).report_federation_executions(federations)
    }
    fn report_federation_execution_members(
        &self,
        federation_name: String,
        members: Vec<(String, String)>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).report_federation_execution_members(federation_name, members)
    }
    fn report_federation_execution_does_not_exist(
        &self,
        federation_name: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).report_federation_execution_does_not_exist(federation_name)
    }
    fn initiate_federate_save(&self, label: String) -> impl Future<Output = ()> + Send + '_ {
        (**self).initiate_federate_save(label)
    }
    fn request_federation_restore_succeeded(
        &self,
        label: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).request_federation_restore_succeeded(label)
    }
    fn request_federation_restore_failed(
        &self,
        label: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).request_federation_restore_failed(label)
    }
    fn federation_restore_begun(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_restore_begun()
    }
    fn initiate_federate_restore(
        &self,
        label: String,
        federate_name: String,
        post_restore_handle: FederateHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).initiate_federate_restore(label, federate_name, post_restore_handle)
    }
    fn federation_restored(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_restored()
    }
    fn federation_not_restored(&self, reason: i32) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_not_restored(reason)
    }
    fn federation_saved(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_saved()
    }
    fn federation_not_saved(&self, reason: i32) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_not_saved(reason)
    }
    fn attribute_is_not_owned(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).attribute_is_not_owned(instance, attributes)
    }
    fn attribute_ownership_acquisition_notification(
        &self,
        instance: ObjectInstanceHandle,
        secured: Vec<AttributeHandle>,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).attribute_ownership_acquisition_notification(instance, secured, tag)
    }
    fn attribute_ownership_unavailable(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).attribute_ownership_unavailable(instance, attributes, tag)
    }
    fn synchronization_point_registration_succeeded(
        &self,
        label: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).synchronization_point_registration_succeeded(label)
    }
    fn synchronization_point_registration_failed(
        &self,
        label: String,
        reason: i32,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).synchronization_point_registration_failed(label, reason)
    }
    fn announce_synchronization_point(
        &self,
        label: String,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).announce_synchronization_point(label, tag)
    }
    fn federation_synchronized(
        &self,
        label: String,
        failed_to_sync: HashSet<FederateHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_synchronized(label, failed_to_sync)
    }
    fn reflect_attribute_values_with_time(
        &self,
        instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        tag: Vec<u8>,
        producer: Option<FederateHandle>,
        time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).reflect_attribute_values_with_time(instance, values, tag, producer, time)
    }
    fn receive_interaction_with_time(
        &self,
        class: InteractionClassHandle,
        params: ParameterHandleValueMap,
        tag: Vec<u8>,
        producer: Option<FederateHandle>,
        time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).receive_interaction_with_time(class, params, tag, producer, time)
    }
    fn remove_object_instance_with_time(
        &self,
        instance: ObjectInstanceHandle,
        tag: Vec<u8>,
        producer: Option<FederateHandle>,
        time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).remove_object_instance_with_time(instance, tag, producer, time)
    }
    fn object_instance_name_reservation_failed(
        &self,
        name: String,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).object_instance_name_reservation_failed(name)
    }
    fn multiple_object_instance_name_reservation_succeeded(
        &self,
        names: Vec<String>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).multiple_object_instance_name_reservation_succeeded(names)
    }
    fn multiple_object_instance_name_reservation_failed(
        &self,
        names: Vec<String>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).multiple_object_instance_name_reservation_failed(names)
    }
    fn start_registration_for_object_class(
        &self,
        class: ObjectClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).start_registration_for_object_class(class)
    }
    fn stop_registration_for_object_class(
        &self,
        class: ObjectClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).stop_registration_for_object_class(class)
    }
    fn turn_interactions_on(
        &self,
        class: InteractionClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).turn_interactions_on(class)
    }
    fn turn_interactions_off(
        &self,
        class: InteractionClassHandle,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).turn_interactions_off(class)
    }
    fn turn_updates_on_for_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).turn_updates_on_for_object_instance(instance, attributes)
    }
    fn turn_updates_off_for_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).turn_updates_off_for_object_instance(instance, attributes)
    }
    fn attributes_in_scope(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).attributes_in_scope(instance, attributes)
    }
    fn attributes_out_of_scope(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).attributes_out_of_scope(instance, attributes)
    }
    fn provide_attribute_value_update(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).provide_attribute_value_update(instance, attributes, tag)
    }
    fn request_attribute_ownership_assumption(
        &self,
        instance: ObjectInstanceHandle,
        offered: Vec<AttributeHandle>,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).request_attribute_ownership_assumption(instance, offered, tag)
    }
    fn request_attribute_ownership_release(
        &self,
        instance: ObjectInstanceHandle,
        candidate: Vec<AttributeHandle>,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).request_attribute_ownership_release(instance, candidate, tag)
    }
    fn request_divestiture_confirmation(
        &self,
        instance: ObjectInstanceHandle,
        released: Vec<AttributeHandle>,
        tag: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).request_divestiture_confirmation(instance, released, tag)
    }
    fn attribute_is_owned_by_rti(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).attribute_is_owned_by_rti(instance, attributes)
    }
    fn confirm_attribute_ownership_acquisition_cancellation(
        &self,
        instance: ObjectInstanceHandle,
        attributes: Vec<AttributeHandle>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).confirm_attribute_ownership_acquisition_cancellation(instance, attributes)
    }
    fn request_retraction(
        &self,
        retraction_handle: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).request_retraction(retraction_handle)
    }
    fn flush_queue_grant(
        &self,
        time: f64,
        next: Option<f64>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).flush_queue_grant(time, next)
    }
    fn initiate_federate_save_with_time(
        &self,
        label: String,
        time: f64,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).initiate_federate_save_with_time(label, time)
    }
    fn federation_save_status_response(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_save_status_response()
    }
    fn federation_restore_status_response(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).federation_restore_status_response()
    }
    fn connection_lost(&self, reason: String) -> impl Future<Output = ()> + Send + '_ {
        (**self).connection_lost(reason)
    }
    fn federate_resigned(&self, reason: String) -> impl Future<Output = ()> + Send + '_ {
        (**self).federate_resigned(reason)
    }
    fn report_attribute_transportation_type(
        &self,
        instance: ObjectInstanceHandle,
        attribute: AttributeHandle,
        transportation: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).report_attribute_transportation_type(instance, attribute, transportation)
    }
    fn report_interaction_transportation_type(
        &self,
        federate: FederateHandle,
        interaction: InteractionClassHandle,
        transportation: Vec<u8>,
    ) -> impl Future<Output = ()> + Send + '_ {
        (**self).report_interaction_transportation_type(federate, interaction, transportation)
    }
    fn raw_callback<'a>(&'a self, kind: &'a str) -> impl Future<Output = ()> + Send + 'a {
        (**self).raw_callback(kind)
    }
}
