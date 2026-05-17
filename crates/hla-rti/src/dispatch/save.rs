//! Dispatch handlers for the Federation Save service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn request_federation_save(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    label: String,
) -> Resp {
    use hla_fedpro_proto::fedpro::RequestFederationSaveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if label.is_empty() {
        return exception_variant(HlaException::InvalidSaveLabel, "");
    }

    let participants: Vec<FederateHandle> =
        { m.federation.federates.read().keys().copied().collect() };

    {
        let mut current = m.federation.current_save.write();
        if current.is_some() {
            return exception_variant(HlaException::SaveInProgress, "");
        }
        *current = Some(SaveOperation {
            label: label.clone(),
            statuses: participants
                .iter()
                .copied()
                .map(|fh| (fh, SaveStatus::Initiated))
                .collect(),
        });
        *m.federation.last_save_label.write() = Some(label.clone());
    }

    // Broadcast InitiateFederateSave to every participant.
    let target_set: std::collections::HashSet<FederateHandle> =
        participants.iter().copied().collect();
    let conns = live_connections(node, &m.federation, &target_set);
    fan_out(callbacks, &conns, initiate_federate_save(&label));

    Resp::RequestFederationSaveResponse(RequestFederationSaveResponse {})
}

pub(super) fn federate_save_begun(ctx: &mut SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::FederateSaveBegunResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut current = m.federation.current_save.write();
    let save = match current.as_mut() {
        Some(s) => s,
        None => return exception_variant(HlaException::SaveNotInProgress, ""),
    };
    match save.statuses.get(&m.federate_handle) {
        Some(SaveStatus::Initiated) => {
            save.statuses
                .insert(m.federate_handle, SaveStatus::BegunSave);
            Resp::FederateSaveBegunResponse(FederateSaveBegunResponse {})
        }
        Some(_) => exception_variant(HlaException::FederateNotInSaveInitiated, ""),
        None => exception_variant(HlaException::FederateNotInSaveSet, ""),
    }
}

pub(super) fn federate_save_progressed(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    successfully: bool,
) -> Resp {
    use hla_fedpro_proto::fedpro::{FederateSaveCompleteResponse, FederateSaveNotCompleteResponse};
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };

    let outcome = {
        let mut current = m.federation.current_save.write();
        let save = match current.as_mut() {
            Some(s) => s,
            None => return exception_variant(HlaException::SaveNotInProgress, ""),
        };
        let new_status = if successfully {
            SaveStatus::SaveComplete
        } else {
            SaveStatus::SaveNotComplete
        };
        save.statuses.insert(m.federate_handle, new_status);
        let any_failed = save
            .statuses
            .values()
            .any(|s| *s == SaveStatus::SaveNotComplete);
        let all_done = save
            .statuses
            .values()
            .all(|s| matches!(s, SaveStatus::SaveComplete | SaveStatus::SaveNotComplete));
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
            federation_not_saved(0)
        } else {
            // Successful save → write the snapshot to disk before notifying
            // federates. Use the label captured from current_save (consumed
            // already above), so recover from federation.current_save... no,
            // it's already been taken. We need to re-derive the label.
            // For simplicity, when the orchestration completes, look at the
            // last save label by reading what was just written.
            // Actually we need to know the label. Let me capture it differently.
            persist_federation_snapshot_on_success(node, &m.federation);
            federation_saved()
        };
        fan_out(callbacks, &conns, cb);
    }

    if successfully {
        Resp::FederateSaveCompleteResponse(FederateSaveCompleteResponse {})
    } else {
        Resp::FederateSaveNotCompleteResponse(FederateSaveNotCompleteResponse {})
    }
}

/// On successful federation-save completion, persist a snapshot to disk.
/// The label is recovered from the recently-cleared `current_save` — but
/// since the dispatch arm that called this already cleared it, we read
/// from a side-channel: we tag the federation with `last_save_label` when
/// save begins, and use that here.
pub(super) fn persist_federation_snapshot_on_success(node: &Arc<RtiNode>, federation: &Federation) {
    let label = federation.last_save_label.read().clone();
    let Some(label) = label else { return };
    let dir = node.save_dir.read().clone();
    let snap = federation.snapshot();
    // Blocking `std::fs::write` on a Tokio worker — see the matching
    // comment on `read_snapshot` above.
    let result = tokio::task::block_in_place(|| {
        crate::persistence::write_snapshot(&dir, &federation.name, &label, &snap)
    });
    if let Err(e) = result {
        tracing::warn!(error = %e, "snapshot write failed");
    } else {
        tracing::info!(label, "snapshot written");
    }
}

pub(super) fn abort_federation_save(ctx: &mut SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::AbortFederationSaveResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut current = m.federation.current_save.write();
    if current.is_none() {
        return exception_variant(HlaException::SaveNotInProgress, "");
    }
    current.take();
    Resp::AbortFederationSaveResponse(AbortFederationSaveResponse {})
}
