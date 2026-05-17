//! Dispatch handlers for the Ownership Management service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn is_attribute_owned_by_federate(
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

pub(super) fn query_attribute_ownership(
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

pub(super) fn attribute_ownership_acquisition_if_available(
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

pub(super) fn unconditional_attribute_ownership_divestiture(
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
