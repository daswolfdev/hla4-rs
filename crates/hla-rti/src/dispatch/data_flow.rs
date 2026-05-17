//! Dispatch handlers for the data flow service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn decode_attribute_value_map(
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

pub(super) fn decode_parameter_value_map(
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

pub(super) fn update_attribute_values(
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
pub(super) fn notify_subscribers_of_registration_subset(
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

pub(super) fn update_attribute_values_with_time(
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
pub(super) fn route_tso_or_immediate(
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
pub(super) fn drain_tso_up_to(
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

pub(super) fn send_interaction_with_time(
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

pub(super) fn delete_object_instance_with_time(
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

pub(super) fn send_interaction(
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

pub(super) fn delete_object_instance(
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
