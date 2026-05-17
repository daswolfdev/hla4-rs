//! Per-connection (per-FedPro-session) state on the RTI side.
//!
//! Each TCP connection has exactly one `SessionContext` for its lifetime. The
//! dispatch loop owns it and threads `&mut` into service handlers, so e.g.
//! `JoinFederationExecution` can record federation membership and subsequent
//! calls (publish/subscribe/...) can scope themselves to that membership.

use std::sync::Arc;

use hla_core::FederateHandle;

use crate::Federation;

/// Live state for one federate connection.
pub struct SessionContext {
    pub session_id: u64,
    /// `Some` once this session has joined a federation, cleared on resign.
    /// Holding `Arc<Federation>` lets handlers operate on the federation
    /// without re-locking the top-level registry on every call.
    pub membership: Option<Membership>,
}

#[derive(Clone)]
pub struct Membership {
    pub federation: Arc<Federation>,
    pub federate_handle: FederateHandle,
    pub federate_name: String,
}

impl SessionContext {
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            membership: None,
        }
    }

    pub fn is_joined(&self) -> bool {
        self.membership.is_some()
    }
}
