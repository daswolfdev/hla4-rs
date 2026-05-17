//! Dispatch handlers for the Declaration Management service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn decode_handle_set(
    set: Option<ProtoAttributeHandleSet>,
) -> Result<AttributeHandleSet, Resp> {
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

pub(super) fn publish_object_class_attributes(
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

pub(super) fn unpublish_object_class_attributes(
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

pub(super) fn subscribe_object_class_attributes_inner(
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
pub(super) fn class_has_subscribers(federation: &Federation, class: ObjectClassHandle) -> bool {
    let subs = federation.subscriptions.read();
    subs.class_has_subscribers(class)
}

pub(super) fn interaction_has_subscribers(
    federation: &Federation,
    class: InteractionClassHandle,
) -> bool {
    let subs = federation.subscriptions.read();
    subs.by_interaction.contains_key(&class)
}

pub(super) fn emit_start_registration(
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

pub(super) fn emit_stop_registration(
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

pub(super) fn emit_turn_interactions_on(
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

pub(super) fn emit_turn_interactions_off(
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

pub(super) fn unsubscribe_object_class_attributes_inner(
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

pub(super) fn decode_interaction_or_err(
    h: Option<fedpro::InteractionClassHandle>,
) -> Result<InteractionClassHandle, Resp> {
    h.and_then(|x| decode_interaction_class(&x).ok())
        .ok_or_else(|| exception_variant(HlaException::InvalidInteractionClassHandle, ""))
}

pub(super) fn publish_interaction_class(
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

pub(super) fn unpublish_interaction_class(
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

pub(super) fn subscribe_interaction_class_inner(
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

pub(super) fn unsubscribe_interaction_class_inner(
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
