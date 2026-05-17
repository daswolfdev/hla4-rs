//! Dispatch handlers for the Time Management service group.
//!
//! All shared imports live in `super` (`dispatch/mod.rs`) and are
//! picked up via `use super::*;` — see the module-layout note there.

#![allow(clippy::wildcard_imports)]

use super::*;

pub(super) fn enable_time_regulation(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    lookahead: Option<fedpro::LogicalTimeInterval>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let lookahead = match lookahead.as_ref().map(decode_logical_time_interval) {
        Some(Ok(v)) if v.is_finite() && v >= 0.0 => v,
        Some(Ok(v)) => return exception_variant(HlaException::InvalidLookahead, &v.to_string()),
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLookahead, "missing"),
    };

    let now = {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if fs.time.is_regulating {
            return exception_variant(HlaException::TimeRegulationAlreadyEnabled, "");
        }
        fs.time.is_regulating = true;
        fs.time.lookahead = lookahead;
        fs.time.current_time
    };

    // Emit TimeRegulationEnabled at the federate's current_time.
    let connections = live_connections(node, &m.federation, &single(m.federate_handle));
    fan_out(callbacks, &connections, time_regulation_enabled(now));

    // New regulator may have raised LBTS (it was previously ∞ if nobody was
    // regulating) or, with a small lookahead at time 0, lowered it. Either
    // way, re-evaluate pending advances.
    try_grant_pending_advances(node, &m.federation, callbacks);

    Resp::EnableTimeRegulationResponse(EnableTimeRegulationResponse {})
}

pub(super) fn disable_time_regulation(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if !fs.time.is_regulating {
            return exception_variant(HlaException::TimeRegulationIsNotEnabled, "");
        }
        fs.time.is_regulating = false;
    }
    try_grant_pending_advances(node, &m.federation, callbacks);
    Resp::DisableTimeRegulationResponse(DisableTimeRegulationResponse {})
}

pub(super) fn enable_time_constrained(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let now = {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if fs.time.is_constrained {
            return exception_variant(HlaException::TimeConstrainedAlreadyEnabled, "");
        }
        fs.time.is_constrained = true;
        fs.time.current_time
    };
    let connections = live_connections(node, &m.federation, &single(m.federate_handle));
    fan_out(callbacks, &connections, time_constrained_enabled(now));
    Resp::EnableTimeConstrainedResponse(EnableTimeConstrainedResponse {})
}

pub(super) fn disable_time_constrained(ctx: &mut SessionContext) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let mut federates = m.federation.federates.write();
    let fs = match federates.get_mut(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if !fs.time.is_constrained {
        return exception_variant(HlaException::TimeConstrainedIsNotEnabled, "");
    }
    fs.time.is_constrained = false;
    Resp::DisableTimeConstrainedResponse(DisableTimeConstrainedResponse {})
}

pub(super) fn time_advance_request(
    node: &Arc<RtiNode>,
    ctx: &mut SessionContext,
    callbacks: &mut Vec<OutboundCallback>,
    time: Option<fedpro::LogicalTime>,
) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m.clone(),
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let requested = match time.as_ref().map(decode_logical_time) {
        Some(Ok(v)) if v.is_finite() => v,
        Some(Ok(v)) => {
            return exception_variant(HlaException::LogicalTimeAlreadyPassed, &v.to_string());
        }
        Some(Err(n)) => return exception_variant(n, ""),
        None => return exception_variant(HlaException::InvalidLogicalTime, "missing"),
    };

    let (already_grantable, grant_time) = {
        let mut federates = m.federation.federates.write();
        let fs = match federates.get_mut(&m.federate_handle) {
            Some(f) => f,
            None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
        };
        if fs.time.pending_advance.is_some() {
            return exception_variant(HlaException::InTimeAdvancingState, "");
        }
        if requested < fs.time.current_time {
            return exception_variant(HlaException::LogicalTimeAlreadyPassed, "");
        }
        fs.time.pending_advance = Some(requested);
        (!fs.time.is_constrained, requested)
    };

    // Unconstrained federates: grant immediately. Even unconstrained
    // federates may have queued TSO messages (if they were briefly
    // constrained earlier), so we still drain.
    if already_grantable {
        let mut federates = m.federation.federates.write();
        if let Some(fs) = federates.get_mut(&m.federate_handle)
            && fs.time.pending_advance == Some(grant_time)
        {
            fs.time.current_time = grant_time;
            fs.time.pending_advance = None;
        }
        drop(federates);
        // Drain TSO queue first, then deliver TAG. IEEE 1516.1 §8: TSO
        // messages with timestamp ≤ grant_time must arrive before the grant.
        drain_tso_up_to(
            node,
            &m.federation,
            callbacks,
            m.federate_handle,
            grant_time,
        );
        let connections = live_connections(node, &m.federation, &single(m.federate_handle));
        fan_out(callbacks, &connections, time_advance_grant(grant_time));
        try_grant_pending_advances_with_tso(node, &m.federation, callbacks);
        return Resp::TimeAdvanceRequestResponse(TimeAdvanceRequestResponse {});
    }

    // Constrained: see if LBTS already permits.
    try_grant_pending_advances_with_tso(node, &m.federation, callbacks);

    Resp::TimeAdvanceRequestResponse(TimeAdvanceRequestResponse {})
}

/// Wrapper around `try_grant_pending_advances` that also drains each
/// granted federate's TSO queue *before* the TAG callback is emitted.
pub(super) fn try_grant_pending_advances_with_tso(
    node: &Arc<RtiNode>,
    federation: &Federation,
    callbacks: &mut Vec<OutboundCallback>,
) {
    // Snapshot pending advances pre-grant so we know which federates will
    // be granted and at what time.
    let pending: Vec<(FederateHandle, f64)> = {
        let federates = federation.federates.read();
        federates
            .iter()
            .filter_map(|(&fh, fs)| fs.time.pending_advance.map(|t| (fh, t)))
            .collect()
    };
    let bound = crate::time::lbts(federation);
    let to_grant: Vec<(FederateHandle, f64)> = pending
        .into_iter()
        .filter(|(fh, t)| {
            let federates = federation.federates.read();
            let fs = match federates.get(fh) {
                Some(f) => f,
                None => return false,
            };
            !fs.time.is_constrained || *t <= bound
        })
        .collect();

    // Drain TSO before grant for each.
    for (fh, t) in &to_grant {
        drain_tso_up_to(node, federation, callbacks, *fh, *t);
    }
    // Then run the normal grant machinery (emits TAG).
    crate::time::try_grant_pending_advances(node, federation, callbacks);
}

pub(super) fn query_logical_time(ctx: &SessionContext) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    let fs = match federates.get(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    Resp::QueryLogicalTimeResponse(QueryLogicalTimeResponse {
        result: Some(encode_logical_time(fs.time.current_time)),
    })
}

pub(super) fn modify_lookahead(
    ctx: &mut SessionContext,
    lookahead: Option<fedpro::LogicalTimeInterval>,
) -> Resp {
    use hla_fedpro_proto::fedpro::ModifyLookaheadResponse;
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let lookahead = match lookahead.as_ref().map(decode_logical_time_interval) {
        Some(Ok(v)) if v.is_finite() && v >= 0.0 => v,
        _ => return exception_variant(HlaException::InvalidLookahead, ""),
    };
    let mut federates = m.federation.federates.write();
    match federates.get_mut(&m.federate_handle) {
        Some(fs) => {
            if !fs.time.is_regulating {
                return exception_variant(HlaException::TimeRegulationIsNotEnabled, "");
            }
            fs.time.lookahead = lookahead;
            Resp::ModifyLookaheadResponse(ModifyLookaheadResponse {})
        }
        None => exception_variant(HlaException::FederateNotExecutionMember, ""),
    }
}

pub(super) fn query_lits(ctx: &SessionContext) -> Resp {
    use hla_fedpro_proto::fedpro::{QueryLitsResponse, TimeQueryReturn};
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let lbts = crate::time::lbts(&m.federation);
    Resp::QueryLitsResponse(QueryLitsResponse {
        result: Some(TimeQueryReturn {
            logical_time_is_valid: lbts.is_finite(),
            logical_time: Some(encode_logical_time(if lbts.is_finite() {
                lbts
            } else {
                0.0
            })),
        }),
    })
}

pub(super) fn query_lookahead(ctx: &SessionContext) -> Resp {
    let m = match ctx.membership.as_ref() {
        Some(m) => m,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    let federates = m.federation.federates.read();
    let fs = match federates.get(&m.federate_handle) {
        Some(f) => f,
        None => return exception_variant(HlaException::FederateNotExecutionMember, ""),
    };
    if !fs.time.is_regulating {
        return exception_variant(HlaException::TimeRegulationIsNotEnabled, "");
    }
    Resp::QueryLookaheadResponse(QueryLookaheadResponse {
        result: Some(encode_logical_time_interval(fs.time.lookahead)),
    })
}

pub(super) fn single(fh: FederateHandle) -> std::collections::HashSet<FederateHandle> {
    let mut s = std::collections::HashSet::new();
    s.insert(fh);
    s
}
