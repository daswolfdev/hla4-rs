//! Time Management coordinator: LBTS computation and TAR grant logic.
//!
//! Models the subset of IEEE 1516.1 §8 needed for two-federate exchanges:
//!   * `enableTimeRegulation(lookahead)` — federate commits to not sending
//!     TSO events earlier than its time + lookahead. Contributes to LBTS.
//!   * `enableTimeConstrained` — federate's `timeAdvanceRequest`s must wait
//!     for LBTS to reach the requested time.
//!   * `timeAdvanceRequest(T)` — produces a `TimeAdvanceGrant(T)` callback
//!     once it's safe.
//!
//! LBTS (Lower Bound on Time Stamp) = `min` over all regulating federates of
//! `(their current_time + their lookahead)`. With no regulating federates,
//! LBTS = +∞ (constrained federates advance freely).
//!
//! Out of MVP scope: optimistic execution, `nextMessageRequest` semantics
//! distinct from TAR, retraction, time-stamp-order delivery of HLA messages.

use std::collections::HashMap;
use std::sync::Arc;

use hla_core::FederateHandle;

use crate::routing::{OutboundCallback, fan_out, live_connections};
use crate::{Federation, RtiNode};

/// Lower Bound on Time Stamp across the federation's regulating federates.
/// Returns `f64::INFINITY` if nobody is regulating.
pub(crate) fn lbts(federation: &Federation) -> f64 {
    let federates = federation.federates.read();
    federates
        .values()
        .filter(|f| f.time.is_regulating)
        .map(|f| f.time.current_time + f.time.lookahead)
        .fold(f64::INFINITY, f64::min)
}

/// After any state change that could move LBTS forward (regulating federate
/// advances, regulating federate disables regulation, or new regulating
/// federate enabled with a higher-time grant), walk all constrained federates
/// with a pending advance and grant whichever are now safe.
///
/// Pushes `TimeAdvanceGrant` callbacks onto `out`. Caller is responsible for
/// delivering them via the connection's mpsc.
pub(crate) fn try_grant_pending_advances(
    node: &Arc<RtiNode>,
    federation: &Federation,
    out: &mut Vec<OutboundCallback>,
) {
    let bound = lbts(federation);
    // First pass: compute who can be granted (read lock, no mutation).
    let to_grant: Vec<(FederateHandle, f64)> = {
        let federates = federation.federates.read();
        federates
            .iter()
            .filter_map(|(&fh, fs)| {
                let pending = fs.time.pending_advance?;
                // Unconstrained federates were granted at request time; if a
                // pending is set here it means the federate is constrained.
                if !fs.time.is_constrained || pending <= bound {
                    Some((fh, pending))
                } else {
                    None
                }
            })
            .collect()
    };

    if to_grant.is_empty() {
        return;
    }

    // Second pass: apply grants under write lock and collect target federates.
    let granted: Vec<(FederateHandle, f64)> = {
        let mut federates = federation.federates.write();
        let mut g = Vec::new();
        for (fh, time) in &to_grant {
            if let Some(fs) = federates.get_mut(fh)
                && fs.time.pending_advance == Some(*time)
            {
                fs.time.current_time = *time;
                fs.time.pending_advance = None;
                g.push((*fh, *time));
            }
        }
        g
    };

    for (fh, time) in granted {
        let connections = live_connections(node, federation, &single_set(fh));
        let callback = crate::routing::time_advance_grant(time);
        fan_out(out, &connections, callback);
    }

    // A grant moves the granted federate's current_time forward. If that
    // federate was regulating, LBTS may now have *receded*… wait, no: a
    // grant only increases current_time, never decreases. So LBTS is
    // non-decreasing. But the granted federate's *post-grant* contribution to
    // LBTS is `granted_time + lookahead`, which may unlock more federates.
    // Recurse once (depth-bounded by the number of federates).
    //
    // Avoiding infinite recursion: each recursion grants at least one federate
    // (or returns). Number of grants per federation iteration is bounded by
    // |federates|.
    let new_bound = lbts(federation);
    if new_bound > bound {
        try_grant_pending_advances(node, federation, out);
    }
}

fn single_set(fh: FederateHandle) -> std::collections::HashSet<FederateHandle> {
    let mut s = std::collections::HashSet::new();
    s.insert(fh);
    s
}

/// Convenience: snapshot of every federate's time state, for tests / MOM.
#[allow(dead_code)]
pub(crate) fn snapshot(federation: &Federation) -> HashMap<FederateHandle, (f64, f64, bool, bool)> {
    let federates = federation.federates.read();
    federates
        .iter()
        .map(|(&fh, fs)| {
            (
                fh,
                (
                    fs.time.current_time,
                    fs.time.lookahead,
                    fs.time.is_regulating,
                    fs.time.is_constrained,
                ),
            )
        })
        .collect()
}
