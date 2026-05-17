//! Dispatch handlers for the Object instance + dimension lookup service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn get_object_instance_handle(ctx: &SessionContext, name: &str) -> Resp {
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

pub(super) fn get_object_instance_name(
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

pub(super) fn get_known_object_class_handle(
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

pub(super) fn local_delete_object_instance(
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

pub(super) fn get_dimension_handle(ctx: &SessionContext, name: &str) -> Resp {
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

pub(super) fn get_dimension_name(
    ctx: &SessionContext,
    dimension: Option<fedpro::DimensionHandle>,
) -> Resp {
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

pub(super) fn get_dimension_upper_bound(
    ctx: &SessionContext,
    _dimension: Option<fedpro::DimensionHandle>,
) -> Resp {
    use hla_fedpro_proto::fedpro::GetDimensionUpperBoundResponse;
    let _ = ctx;
    // FOM-defined upper bound; default u32::MAX for MVP.
    Resp::GetDimensionUpperBoundResponse(GetDimensionUpperBoundResponse { result: u32::MAX })
}

pub(super) fn get_dimension_handle_set(
    ctx: &SessionContext,
    region: Option<fedpro::RegionHandle>,
) -> Resp {
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

pub(super) fn get_available_dimensions_for_object_class(
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

pub(super) fn get_available_dimensions_for_interaction_class(
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
