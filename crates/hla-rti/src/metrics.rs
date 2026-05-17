//! Server-wide metrics for ops visibility.
//!
//! All counters are `AtomicU64` and incremented on the hot path with `Relaxed`
//! ordering. Snapshot is a plain struct readable for export to Prometheus,
//! OpenTelemetry, or a JSON `/metrics` endpoint.
//!
//! For mission-critical deployment these should be supplemented with:
//!   * per-federation, per-federate, per-class counters (cardinality limited)
//!   * latency histograms (e.g., via `hdrhistogram`)
//!   * resource gauges (mpsc backlog depths, RwLock contention)

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct ServerMetrics {
    /// Total `HLA_CALL_REQUEST` frames dispatched.
    pub calls_dispatched: AtomicU64,
    /// Calls that returned an `ExceptionData` response variant.
    pub call_exceptions: AtomicU64,
    /// Total `HLA_CALLBACK_REQUEST` frames emitted to subscribers.
    pub callbacks_emitted: AtomicU64,
    /// Connections accepted (TCP or TLS), pre-handshake.
    pub connections_accepted: AtomicU64,
    /// Connections rejected by `check_limits`.
    pub connections_rejected: AtomicU64,
    /// FedPro session-open handshakes completed successfully.
    pub sessions_opened: AtomicU64,
    /// Sessions reaped because the federate stopped sending frames.
    pub sessions_reaped: AtomicU64,
    /// Federations currently in the registry (snapshot value).
    pub federations_live: AtomicU64,
}

impl ServerMetrics {
    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            calls_dispatched: self.calls_dispatched.load(Ordering::Relaxed),
            call_exceptions: self.call_exceptions.load(Ordering::Relaxed),
            callbacks_emitted: self.callbacks_emitted.load(Ordering::Relaxed),
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            sessions_opened: self.sessions_opened.load(Ordering::Relaxed),
            sessions_reaped: self.sessions_reaped.load(Ordering::Relaxed),
            federations_live: self.federations_live.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MetricsSnapshot {
    pub calls_dispatched: u64,
    pub call_exceptions: u64,
    pub callbacks_emitted: u64,
    pub connections_accepted: u64,
    pub connections_rejected: u64,
    pub sessions_opened: u64,
    pub sessions_reaped: u64,
    pub federations_live: u64,
}
