//! Dispatch handlers for the DDM region lifecycle service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn create_region(
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

pub(super) fn commit_region_modifications(
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

pub(super) fn delete_region(ctx: &SessionContext, region: Option<fedpro::RegionHandle>) -> Resp {
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

pub(super) fn get_range_bounds(
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

pub(super) fn set_range_bounds(
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
