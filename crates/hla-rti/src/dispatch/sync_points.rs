//! Dispatch handlers for the Synchronization Points service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn register_synchronization_point(
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

pub(super) fn synchronization_point_achieved(
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
