//! Dispatch handlers for the Directed interactions service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn decode_interaction_class_set(
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

pub(super) fn publish_object_class_directed_interactions(
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

pub(super) fn unpublish_object_class_directed_interactions(
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

pub(super) fn subscribe_object_class_directed_interactions(
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

pub(super) fn unsubscribe_object_class_directed_interactions(
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

pub(super) fn send_directed_interaction(
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
