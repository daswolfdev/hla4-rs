//! Dispatch handlers for the Federation Restore service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn request_federation_restore(
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
        // `read_snapshot` performs blocking `std::fs::read_to_string`. The
        // dispatch loop runs on a Tokio worker thread, so we use
        // `block_in_place` to tell the runtime to migrate other tasks
        // off this thread for the duration. Requires multi-thread
        // runtime (which `rtiexec` and every integration test use).
        match tokio::task::block_in_place(|| {
            crate::persistence::read_snapshot(&dir, &m.federation.name, &label)
        }) {
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

pub(super) fn federate_restore_progressed(
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

pub(super) fn abort_federation_restore(ctx: &mut SessionContext) -> Resp {
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
pub(super) fn decode_attr_region_pairs(
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

pub(super) fn subscribe_object_class_attributes_with_regions(
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

pub(super) fn unsubscribe_object_class_attributes_with_regions(
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

pub(super) fn register_object_instance_with_regions(
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
