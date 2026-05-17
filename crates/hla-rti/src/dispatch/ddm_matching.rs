//! Dispatch handlers for the DDM region-overlap matching service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

/// Returns true if `pub_regions` and `sub_regions` have ANY pair of regions
/// that overlap. Empty `pub_regions` OR empty `sub_regions` is treated as
/// "unrestricted" and always matches (legacy non-DDM subscription/update).
pub(super) fn regions_overlap_any(
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
pub(super) fn filter_subscribers_by_regions(
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
