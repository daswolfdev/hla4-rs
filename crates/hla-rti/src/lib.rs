//! HLA 4 RTI server.
//!
//! MVP architecture: single central `RtiNode` listening on a TCP socket.
//! Each accepted connection becomes a `FederateSession` via the FedPro
//! handshake. Federations are tracked in a top-level registry; `Federation`
//! owns the subscription matrix, object instance table, and time coordinator.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hla_core::{
    AttributeHandle, AttributeHandleSet, FederateHandle, InteractionClassHandle, ObjectClassHandle,
    ObjectInstanceHandle,
};
use hla_fedpro_proto::fedpro;
use hla_omt::MergedFom;
use hla_wire::{
    AsyncReadSource, AsyncWriteSink, Frame, FrameSink, FrameSource, HlaCallResponsePayload,
    MessageHeader, MessageType, NO_SEQUENCE_NUMBER, SessionError,
};
use parking_lot::{Mutex, RwLock};
use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

// Internal modules. The public surface is the `pub use` re-exports below;
// nothing outside the crate should reach into these. In particular,
// `dispatch` and `routing` produce `prost`-generated types that must not
// leak through the public API — see BESTPRACTICES §C2.
pub(crate) mod dispatch;
pub(crate) mod exception;
pub(crate) mod handles;
pub(crate) mod metrics;
pub(crate) mod persistence;
pub(crate) mod routing;
pub(crate) mod session;
pub(crate) mod time;

pub use exception::HlaException;
pub use metrics::{MetricsSnapshot, ServerMetrics};
// `Membership` and `SessionContext` are part of dispatch-internal state.
// They have no public constructors, so re-exporting them only adds noise
// to the rustdoc — keep them crate-private until there's a real consumer.

/// Per-connection writer handle. Owned via `Arc` so dispatch handlers (which
/// fire callbacks at *other* federates' connections) can clone the handle
/// out of [`RtiNode::connections`] and push frames without holding the lock.
pub struct ConnectionHandle {
    pub session_id: u64,
    pub frame_tx: mpsc::Sender<Frame>,
    /// Monotonic sequence number for `HLA_CALLBACK_REQUEST` frames the RTI
    /// emits to this federate. Independent of the client's outbound seq.
    pub next_outbound_seq: AtomicI32,
}

/// Connection limits for DoS mitigation.
///
/// Both bounds are advisory and checked at accept time. A connection that
/// exceeds either limit is closed immediately (no FedPro handshake attempted).
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionLimits {
    /// Maximum total live connections across the node. `None` = unlimited.
    pub max_total: Option<usize>,
    /// Maximum live connections per remote IP address. `None` = unlimited.
    pub max_per_ip: Option<usize>,
}

/// Suspended-session state: a SessionContext held in waiting after the
/// federate's transport dropped, eligible to be resumed via
/// `CTRL_RESUME_REQUEST` before `deadline`. When the deadline passes the
/// janitor performs cleanup (auto-resign on any joined federation).
// PR 4 (audit M2) will hide `SuspendedSession` and the field on `RtiNode`
// that holds it. Until then, both must stay `pub` because integration tests
// reach into `node.federations` / `node.connections` / etc. directly.
pub struct SuspendedSession {
    pub membership: Option<session::Membership>,
    pub deadline: Instant,
}

/// Configuration for liveness detection.
///
/// Defaults follow FedProClient's `doc/Components.md`:
///   * server-initiated heartbeat every 60s
///   * drop connection after 180s without any inbound traffic
///
/// Tests should override to short intervals.
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub missing_timeout: Duration,
    /// How long to keep a session's membership in `suspended_sessions`
    /// after the transport drops, before final cleanup. Default 0 =
    /// immediate auto-resign (current behavior); set to e.g. 600s in
    /// production to allow `CTRL_RESUME_REQUEST` reattach.
    pub reconnect_window: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            missing_timeout: Duration::from_secs(180),
            reconnect_window: Duration::from_secs(0),
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RtiServerError {
    #[error("bind failed: {0}")]
    Bind(std::io::Error),
    #[error("accept failed: {0}")]
    Accept(std::io::Error),
}

#[derive(Debug, Default)]
pub struct SubscriptionMatrix {
    pub by_attribute: HashMap<(ObjectClassHandle, AttributeHandle), HashSet<FederateHandle>>,
    pub by_interaction: HashMap<InteractionClassHandle, HashSet<FederateHandle>>,
    /// Reverse index: per object class, a refcount per federate. The count
    /// is the number of *distinct attributes* of that class the federate is
    /// subscribed to. Maintained alongside `by_attribute` so the hot
    /// "who-cares-about-this-class" lookup (called per object discovery /
    /// instance registration) runs in O(subscribers) instead of scanning
    /// the entire matrix.
    pub class_subscribers: HashMap<ObjectClassHandle, HashMap<FederateHandle, u32>>,
}

impl SubscriptionMatrix {
    /// Subscribe `federate` to `(class, attr)`. Returns `true` if this was
    /// the federate's *first* attribute subscription to `class` — useful
    /// for first-subscriber bookkeeping (e.g. StartRegistrationForObjectClass).
    pub fn subscribe_attribute(
        &mut self,
        class: ObjectClassHandle,
        attr: AttributeHandle,
        federate: FederateHandle,
    ) -> bool {
        let newly_added_to_attr = self
            .by_attribute
            .entry((class, attr))
            .or_default()
            .insert(federate);
        if !newly_added_to_attr {
            return false;
        }
        let class_map = self.class_subscribers.entry(class).or_default();
        let count = class_map.entry(federate).or_insert(0);
        *count += 1;
        *count == 1
    }

    /// Unsubscribe `federate` from `(class, attr)`. Returns `true` if this
    /// removed the federate's *last* attribute subscription to `class`.
    pub fn unsubscribe_attribute(
        &mut self,
        class: ObjectClassHandle,
        attr: AttributeHandle,
        federate: FederateHandle,
    ) -> bool {
        let removed = match self.by_attribute.get_mut(&(class, attr)) {
            Some(set) => {
                let r = set.remove(&federate);
                if set.is_empty() {
                    self.by_attribute.remove(&(class, attr));
                }
                r
            }
            None => false,
        };
        if !removed {
            return false;
        }
        let Some(class_map) = self.class_subscribers.get_mut(&class) else {
            return false;
        };
        let Some(count) = class_map.get_mut(&federate) else {
            return false;
        };
        *count -= 1;
        if *count == 0 {
            class_map.remove(&federate);
            if class_map.is_empty() {
                self.class_subscribers.remove(&class);
            }
            return true;
        }
        false
    }

    /// Remove every attribute subscription whose class is `class` —
    /// used when an object class is destroyed.
    pub fn remove_class(&mut self, class: ObjectClassHandle) {
        self.by_attribute.retain(|(c, _), _| *c != class);
        self.class_subscribers.remove(&class);
    }

    /// True iff some federate subscribes to any attribute of `class`.
    pub fn class_has_subscribers(&self, class: ObjectClassHandle) -> bool {
        self.class_subscribers.contains_key(&class)
    }

    /// Iterator over every federate that subscribes to any attribute of
    /// `class`. O(subscribers).
    pub fn subscribers_of_class(
        &self,
        class: ObjectClassHandle,
    ) -> impl Iterator<Item = FederateHandle> + '_ {
        self.class_subscribers
            .get(&class)
            .into_iter()
            .flat_map(|m| m.keys().copied())
    }
}

pub struct ObjectInstance {
    pub handle: ObjectInstanceHandle,
    pub class: ObjectClassHandle,
    pub name: String,
    /// The federate that originally registered this instance — holds the
    /// `privilegeToDelete` attribute by default (deletion authority).
    pub registrar: FederateHandle,
    /// Per-attribute ownership. Initially populated at registration time
    /// from the registrar's published-attribute set. Updated by ownership
    /// management operations (divestiture / acquisition).
    pub attribute_owners: HashMap<AttributeHandle, FederateHandle>,
    /// DDM: per-attribute region set used for routing-space matching.
    /// Empty means the attribute is in the unrestricted (default) region.
    pub attribute_regions: HashMap<AttributeHandle, HashSet<hla_core::RegionHandle>>,
}

#[derive(Clone, Debug, Default)]
pub struct PubSubState {
    /// Per object class, which attributes this federate publishes.
    pub published_attrs: HashMap<ObjectClassHandle, AttributeHandleSet>,
    /// Per object class, which attributes this federate subscribes to.
    pub subscribed_attrs: HashMap<ObjectClassHandle, AttributeHandleSet>,
    /// Interaction classes this federate publishes.
    pub published_interactions: HashSet<InteractionClassHandle>,
    /// Interaction classes this federate subscribes to.
    pub subscribed_interactions: HashSet<InteractionClassHandle>,
    /// Object instances this federate has been told about via
    /// `discoverObjectInstance`. Prevents duplicate discovery callbacks
    /// when an instance updates attributes the federate already saw.
    pub discovered_instances: HashSet<ObjectInstanceHandle>,
    /// DDM: per-(class, attribute), the regions this federate's subscription
    /// is restricted to. Empty/missing entry means an unrestricted (legacy)
    /// subscription which matches all updates regardless of publisher regions.
    pub subscribed_attrs_regions:
        HashMap<(ObjectClassHandle, AttributeHandle), HashSet<hla_core::RegionHandle>>,
    /// Directed-interaction subscriptions: per object class, the set of
    /// interaction classes this federate wants directed interactions on.
    pub subscribed_directed_interactions:
        HashMap<ObjectClassHandle, HashSet<InteractionClassHandle>>,
    /// Directed-interaction publications: per object class, the set of
    /// interaction classes this federate may send directed interactions on.
    pub published_directed_interactions:
        HashMap<ObjectClassHandle, HashSet<InteractionClassHandle>>,
}

/// Per-federate advisory + reporting switches per IEEE 1516.1 §6.
///
/// Defaults mirror the spec recommendations for federates that don't
/// explicitly opt in.
#[derive(Clone, Debug)]
pub struct Switches {
    pub object_class_relevance_advisory: bool,
    pub attribute_relevance_advisory: bool,
    pub attribute_scope_advisory: bool,
    pub interaction_relevance_advisory: bool,
    pub convey_region_designator_sets: bool,
    pub service_reporting: bool,
    pub exception_reporting: bool,
    pub send_service_reports_to_file: bool,
    /// FOM-derived; default false for MVP.
    pub auto_provide: bool,
    /// FOM-derived; default false.
    pub delay_subscription_evaluation: bool,
    /// FOM-derived; default false.
    pub advisories_use_known_class: bool,
    /// FOM-derived; default true (most federations allow relaxed DDM).
    pub allow_relaxed_ddm: bool,
    /// FOM-derived; default false.
    pub non_regulated_grant: bool,
    pub automatic_resign_directive: hla_core::ResignAction,
}

impl Default for Switches {
    fn default() -> Self {
        Self {
            object_class_relevance_advisory: true,
            attribute_relevance_advisory: true,
            attribute_scope_advisory: true,
            interaction_relevance_advisory: true,
            convey_region_designator_sets: false,
            service_reporting: false,
            exception_reporting: false,
            send_service_reports_to_file: false,
            auto_provide: false,
            delay_subscription_evaluation: false,
            advisories_use_known_class: false,
            allow_relaxed_ddm: true,
            non_regulated_grant: false,
            automatic_resign_directive: hla_core::ResignAction::NoAction,
        }
    }
}

/// A queued Time-Stamp Ordered message waiting for the receiving federate to
/// advance past `time`. Held in `FederateSession.tso_queue` (sorted by time).
#[derive(Clone, Debug)]
pub struct TsoMessage {
    pub time: f64,
    pub callback: hla_fedpro_proto::fedpro::CallbackRequest,
}

/// Per-federate Time Management state. Logical time is `HLAfloat64Time`
/// internally — encoded on the wire as 8-byte big-endian IEEE 754 doubles.
///
/// Initial state matches IEEE 1516.1 §8: federate joins at logical time 0,
/// neither regulating nor constrained.
#[derive(Clone, Debug)]
pub struct TimeState {
    pub is_regulating: bool,
    pub is_constrained: bool,
    /// The federate's current granted logical time. Monotonic.
    pub current_time: f64,
    /// Only meaningful when `is_regulating`. The federate has committed
    /// not to send TSO events with timestamps below `current_time + lookahead`.
    pub lookahead: f64,
    /// When a `timeAdvanceRequest` is in flight but not yet grantable, the
    /// requested time sits here. Cleared on grant.
    pub pending_advance: Option<f64>,
}

impl Default for TimeState {
    fn default() -> Self {
        Self {
            is_regulating: false,
            is_constrained: false,
            current_time: 0.0,
            lookahead: 0.0,
            pending_advance: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FederateSession {
    pub handle: FederateHandle,
    pub name: String,
    pub federate_type: String,
    /// Session ID of the FedPro session that owns this federate. Used to
    /// reverse-look-up the connection when cleaning up on disconnect.
    pub session_id: u64,
    pub pub_sub: PubSubState,
    pub time: TimeState,
    pub switches: Switches,
    /// TSO message queue. Messages are held here when the federate is
    /// constrained and their timestamp exceeds the federate's current time;
    /// released on TAR grant in time order.
    pub tso_queue: Vec<TsoMessage>,
}

pub struct TimeCoordinator {
    // populated when time management lands.
}

/// Per-federate participation status in an in-progress federation save.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SaveStatus {
    Initiated,
    BegunSave,
    SaveComplete,
    SaveNotComplete,
}

/// Per-federate participation status in an in-progress federation restore.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RestoreStatus {
    Initiated,
    Complete,
    NotComplete,
}

/// In-progress federation restore orchestration per IEEE 1516.1 §4.11.
#[derive(Clone, Debug)]
pub struct RestoreOperation {
    pub label: String,
    pub statuses: HashMap<FederateHandle, RestoreStatus>,
}

/// In-progress federation save orchestration per IEEE 1516.1 §4.10.
/// MVP scope: tracks the callback orchestration (InitiateFederateSave →
/// FederateSaveBegun/Complete → FederationSaved). Actual federation-state
/// serialization to disk is deferred to a pluggable persistence backend.
#[derive(Clone, Debug)]
pub struct SaveOperation {
    pub label: String,
    pub statuses: HashMap<FederateHandle, SaveStatus>,
}

/// DDM region per IEEE 1516.1 §9. Bounds per dimension. `committed` is what's
/// active for matching; `staged` is the in-flight modifications applied by
/// `setRangeBounds` and committed by `commitRegionModifications`.
#[derive(Clone, Debug)]
pub struct Region {
    pub handle: hla_core::RegionHandle,
    pub owner: FederateHandle,
    pub committed: HashMap<hla_core::DimensionHandle, (u32, u32)>,
    pub staged: HashMap<hla_core::DimensionHandle, (u32, u32)>,
}

/// Per-sync-point state per IEEE 1516.1 §4.7. Lifecycle:
///   * `register` — opens the point; if label is unique, marks `announced`
///     after the per-federate Announce callbacks dispatch
///   * each participant calls `synchronization_point_achieved`
///   * when `achieved.len() == participants.len()`, the FederationSynchronized
///     callback fires for every participant and the entry is removed
#[derive(Clone, Debug)]
pub struct SyncPoint {
    pub label: String,
    pub tag: Vec<u8>,
    pub participants: HashSet<FederateHandle>,
    pub achieved: HashSet<FederateHandle>,
    pub failed_to_sync: HashSet<FederateHandle>,
}

pub struct Federation {
    pub name: String,
    pub fom: Arc<MergedFom>,
    pub federates: RwLock<HashMap<FederateHandle, FederateSession>>,
    pub object_instances: RwLock<HashMap<ObjectInstanceHandle, ObjectInstance>>,
    pub subscriptions: RwLock<SubscriptionMatrix>,
    pub time_coordinator: Mutex<TimeCoordinator>,
    pub sync_points: RwLock<HashMap<String, SyncPoint>>,
    /// DDM regions, keyed by RegionHandle.
    pub regions: RwLock<HashMap<hla_core::RegionHandle, Region>>,
    pub next_region_id: AtomicU64,
    /// An in-progress federation save, if any. Only one at a time.
    pub current_save: RwLock<Option<SaveOperation>>,
    /// An in-progress federation restore, if any. Only one at a time.
    pub current_restore: RwLock<Option<RestoreOperation>>,
    /// Label of the most recent save attempt, used so the persistence
    /// callback can derive the snapshot path after `current_save` is cleared.
    pub last_save_label: RwLock<Option<String>>,
    pub next_object_id: AtomicU64,
    pub next_federate_id: AtomicU32,
}

impl Federation {
    pub fn new(name: String, fom: Arc<MergedFom>) -> Self {
        Self {
            name,
            fom,
            federates: RwLock::new(HashMap::new()),
            object_instances: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(SubscriptionMatrix::default()),
            time_coordinator: Mutex::new(TimeCoordinator {}),
            sync_points: RwLock::new(HashMap::new()),
            regions: RwLock::new(HashMap::new()),
            next_region_id: AtomicU64::new(1),
            current_save: RwLock::new(None),
            current_restore: RwLock::new(None),
            last_save_label: RwLock::new(None),
            next_object_id: AtomicU64::new(1),
            next_federate_id: AtomicU32::new(1),
        }
    }
}

/// HLA 4 RTI server node.
///
/// # Runtime requirements
///
/// `RtiNode` must run on a **multi-thread** Tokio runtime. The save /
/// restore dispatch path calls `tokio::task::block_in_place` to keep
/// the worker available during blocking filesystem I/O, which panics
/// on `current_thread`. Use `#[tokio::main(flavor = "multi_thread")]`
/// or `#[tokio::test(flavor = "multi_thread")]` accordingly. `rtiexec`
/// pins this in `crates/hla-cli/src/main.rs`.
///
/// # Lifecycle
///
/// `bind` → `serve` (or `serve_ws` / `serve_tls`) → `shutdown`. Call
/// `shutdown` from any thread; accept loops and the suspended-session
/// janitor unwind cooperatively. See [`Self::shutdown`].
pub struct RtiNode {
    pub bind_addr: SocketAddr,
    pub federations: RwLock<HashMap<String, Arc<Federation>>>,
    /// Monotonic session ID allocator. Starts at 1 so 0 stays reserved as
    /// `NO_SESSION_ID` per the FedPro protocol.
    pub next_session_id: AtomicU64,
    /// Template FOM cloned into each new `Federation` on
    /// `CreateFederationExecution`. Until `CreateFederationExecutionWithModules`
    /// is fully wired, this is the only path for the RTI to know about a FOM.
    /// Wrapped in `RwLock<Arc<_>>` so callers can swap it after binding without
    /// rebuilding the whole node.
    pub default_fom: RwLock<Arc<MergedFom>>,
    /// Live connections, keyed by session id. Inserted in `handle_connection`
    /// just after the handshake and removed on disconnect. Dispatch handlers
    /// clone the inner `Arc<ConnectionHandle>` to push `HLA_CALLBACK_REQUEST`
    /// frames at subscriber federates.
    pub connections: DashMap<u64, Arc<ConnectionHandle>>,
    /// Heartbeat / liveness-timeout configuration. Behind an RwLock so tests
    /// (and operators) can adjust without rebuilding the node.
    pub heartbeat: RwLock<HeartbeatConfig>,
    /// DoS-protection connection limits.
    pub limits: RwLock<ConnectionLimits>,
    /// Live connection count per remote IP, for `limits.max_per_ip`.
    pub connections_per_ip: DashMap<std::net::IpAddr, usize>,
    /// Server-wide metrics for ops / observability.
    pub metrics: ServerMetrics,
    /// Directory where federation save snapshots are persisted.
    pub save_dir: RwLock<std::path::PathBuf>,
    /// Sessions whose transport has dropped but which are still eligible
    /// to be resumed within `heartbeat.reconnect_window`.
    pub suspended_sessions: DashMap<u64, SuspendedSession>,
    /// Cooperative shutdown signal. Every accept loop, per-connection
    /// reader, writer task, heartbeat task, and the suspended-session
    /// janitor watch this token; cancelling it drives a graceful
    /// drain across the node. Call [`Self::shutdown`] to trigger.
    shutdown: tokio_util::sync::CancellationToken,
}

impl RtiNode {
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            federations: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            default_fom: RwLock::new(Arc::new(MergedFom::new())),
            connections: DashMap::new(),
            heartbeat: RwLock::new(HeartbeatConfig::default()),
            limits: RwLock::new(ConnectionLimits::default()),
            connections_per_ip: DashMap::new(),
            metrics: ServerMetrics::default(),
            suspended_sessions: DashMap::new(),
            save_dir: RwLock::new(std::path::PathBuf::from("./hla4-saves")),
            shutdown: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// Trigger a graceful shutdown. Accept loops stop pulling new
    /// connections and the suspended-session janitor exits. `serve` /
    /// `serve_ws` / `serve_tls` return `Ok(())` once their loops
    /// observe the cancel.
    ///
    /// Per-connection reader / writer / heartbeat tasks still unwind
    /// only on peer EOF or `heartbeat.missing_timeout` — wiring the
    /// shutdown token into `run_session_loop` is tracked as follow-up
    /// work (see the `TODO(H4)` at the select! site in
    /// `run_session_loop`). Idempotent — safe to call concurrently.
    pub fn shutdown(&self) {
        tracing::info!("shutdown requested");
        self.shutdown.cancel();
    }

    /// Shutdown token for callers that want to bind their own tasks
    /// to the node's lifetime (e.g., metrics exporters in `rtiexec`).
    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.clone()
    }

    /// Override the directory used for save/restore snapshot files.
    pub fn set_save_dir(&self, dir: std::path::PathBuf) {
        *self.save_dir.write() = dir;
    }

    /// Snapshot the server's metric counters. Cheap — atomic loads only.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        // Federation count is a snapshot at call time.
        self.metrics
            .federations_live
            .store(self.federations.read().len() as u64, Ordering::Relaxed);
        self.metrics.snapshot()
    }

    /// Set DoS-mitigation connection limits.
    pub fn set_connection_limits(&self, limits: ConnectionLimits) {
        *self.limits.write() = limits;
    }

    /// Returns `Ok(())` if accepting one more connection from `peer.ip()`
    /// is within configured limits. Otherwise `Err(reason)` and the caller
    /// should close the socket immediately.
    fn check_limits(&self, peer: SocketAddr) -> Result<(), &'static str> {
        let limits = *self.limits.read();
        if let Some(max) = limits.max_total
            && self.connections.len() >= max
        {
            return Err("max_total");
        }
        if let Some(max_per_ip) = limits.max_per_ip {
            let ip = peer.ip();
            let count = self.connections_per_ip.get(&ip).map(|c| *c).unwrap_or(0);
            if count >= max_per_ip {
                return Err("max_per_ip");
            }
        }
        Ok(())
    }

    /// Swap in a FOM that subsequent `CreateFederationExecution` calls will
    /// clone into their new `Federation`. Existing federations are unaffected.
    pub fn set_default_fom(&self, fom: MergedFom) {
        *self.default_fom.write() = Arc::new(fom);
    }

    /// Override the heartbeat configuration. Affects connections opened after
    /// this call; live connections keep the value snapshotted at handshake.
    pub fn set_heartbeat_config(&self, cfg: HeartbeatConfig) {
        *self.heartbeat.write() = cfg;
    }

    /// Bind without starting to accept. Useful for tests that need the
    /// actual local address (when bound to port 0).
    pub async fn bind(bind_addr: SocketAddr) -> Result<(Arc<Self>, TcpListener), RtiServerError> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(RtiServerError::Bind)?;
        let actual = listener.local_addr().map_err(RtiServerError::Bind)?;
        let node = Arc::new(Self::new(actual));
        Ok((node, listener))
    }

    /// Accept loop. Each connection gets its own task; the connection task
    /// performs the FedPro session-open handshake and then would dispatch
    /// further frames (HLA call requests, heartbeats, ...) once those land.
    pub async fn run(self: Arc<Self>) -> Result<(), RtiServerError> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(RtiServerError::Bind)?;
        tracing::info!(addr = %self.bind_addr, "rtinode listening");
        self.accept_loop(listener).await
    }

    /// Variant of `run` that takes a pre-bound listener — for tests that
    /// allocate the port themselves.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<(), RtiServerError> {
        self.accept_loop(listener).await
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) -> Result<(), RtiServerError> {
        loop {
            let accept = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    tracing::info!("accept loop draining on shutdown");
                    return Ok(());
                }
                r = listener.accept() => r,
            };
            let (sock, peer) = accept.map_err(RtiServerError::Accept)?;
            if let Err(reason) = self.check_limits(peer) {
                self.metrics
                    .connections_rejected
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(peer = %peer, reason, "rejecting connection (limit)");
                drop(sock);
                continue;
            }
            self.metrics
                .connections_accepted
                .fetch_add(1, Ordering::Relaxed);
            let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
            tracing::info!(peer = %peer, session_id, "federate connected");
            let ip = peer.ip();
            *self.connections_per_ip.entry(ip).or_insert(0) += 1;
            let node = Arc::clone(&self);
            tokio::spawn(async move {
                let r = Arc::clone(&node).handle_connection(sock, session_id).await;
                if let Some(mut entry) = node.connections_per_ip.get_mut(&ip) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        drop(entry);
                        node.connections_per_ip.remove(&ip);
                    }
                }
                if let Err(e) = r {
                    tracing::warn!(peer = %peer, session_id, error = %e, "connection failed");
                }
            });
        }
    }

    /// WebSocket variant of `accept_loop`: each accepted TCP socket goes
    /// through a WebSocket handshake before FedPro framing begins. Per HLA 4
    /// spec, each WebSocket Binary message carries exactly one FedPro frame.
    pub async fn serve_ws(self: Arc<Self>, listener: TcpListener) -> Result<(), RtiServerError> {
        loop {
            let accept = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    tracing::info!("ws accept loop draining on shutdown");
                    return Ok(());
                }
                r = listener.accept() => r,
            };
            let (sock, peer) = accept.map_err(RtiServerError::Accept)?;
            if let Err(reason) = self.check_limits(peer) {
                self.metrics
                    .connections_rejected
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(peer = %peer, reason, "rejecting WS connection (limit)");
                continue;
            }
            self.metrics
                .connections_accepted
                .fetch_add(1, Ordering::Relaxed);
            let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
            let ip = peer.ip();
            *self.connections_per_ip.entry(ip).or_insert(0) += 1;
            let node = Arc::clone(&self);
            tokio::spawn(async move {
                let ws = match tokio_tungstenite::accept_async(sock).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(peer = %peer, error = %e, "WS handshake failed");
                        if let Some(mut entry) = node.connections_per_ip.get_mut(&ip) {
                            *entry = entry.saturating_sub(1);
                        }
                        return;
                    }
                };
                tracing::info!(peer = %peer, session_id, "federate connected (WS)");
                let (source, sink) = hla_wire::split_ws(ws);
                let r = Arc::clone(&node)
                    .handle_connection_via_transport(Box::new(source), Box::new(sink), session_id)
                    .await;
                if let Some(mut entry) = node.connections_per_ip.get_mut(&ip) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        drop(entry);
                        node.connections_per_ip.remove(&ip);
                    }
                }
                if let Err(e) = r {
                    tracing::warn!(peer = %peer, session_id, error = %e, "WS connection failed");
                }
            });
        }
    }

    /// TLS variant of `accept_loop`: each accepted TCP socket is upgraded via
    /// the provided `TlsAcceptor` before any FedPro bytes flow.
    pub async fn serve_tls(
        self: Arc<Self>,
        listener: TcpListener,
        acceptor: Arc<tokio_rustls::TlsAcceptor>,
    ) -> Result<(), RtiServerError> {
        loop {
            let accept = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    tracing::info!("tls accept loop draining on shutdown");
                    return Ok(());
                }
                r = listener.accept() => r,
            };
            let (sock, peer) = accept.map_err(RtiServerError::Accept)?;
            if let Err(reason) = self.check_limits(peer) {
                tracing::warn!(peer = %peer, reason, "rejecting TLS connection (limit)");
                drop(sock);
                continue;
            }
            let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
            let ip = peer.ip();
            *self.connections_per_ip.entry(ip).or_insert(0) += 1;
            let node = Arc::clone(&self);
            let acceptor = Arc::clone(&acceptor);
            tokio::spawn(async move {
                let tls = match acceptor.accept(sock).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(peer = %peer, error = %e, "TLS handshake failed");
                        if let Some(mut entry) = node.connections_per_ip.get_mut(&ip) {
                            *entry = entry.saturating_sub(1);
                        }
                        return;
                    }
                };
                tracing::info!(peer = %peer, session_id, "federate connected (TLS)");
                let r = Arc::clone(&node).handle_connection(tls, session_id).await;
                if let Some(mut entry) = node.connections_per_ip.get_mut(&ip) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        drop(entry);
                        node.connections_per_ip.remove(&ip);
                    }
                }
                if let Err(e) = r {
                    tracing::warn!(peer = %peer, session_id, error = %e, "connection failed");
                }
            });
        }
    }

    async fn handle_connection<S>(
        self: Arc<Self>,
        sock: S,
        session_id: u64,
    ) -> Result<(), SessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // Wrap the byte stream into our frame-transport abstraction.
        let (read_half, write_half) = tokio::io::split(sock);
        let source: Box<dyn FrameSource + Send + 'static> =
            Box::new(AsyncReadSource::new(read_half));
        let sink: Box<dyn FrameSink + Send + 'static> = Box::new(AsyncWriteSink::new(write_half));
        self.handle_connection_via_transport(source, sink, session_id)
            .await
    }

    /// Transport-agnostic connection handler. Receives already-wrapped
    /// `FrameSource` / `FrameSink` halves; agnostic to TCP vs TLS vs WS.
    /// Handles both `CTRL_NEW_SESSION` (fresh handshake) and
    /// `CTRL_RESUME_REQUEST` (reattach to a suspended session).
    async fn handle_connection_via_transport(
        self: Arc<Self>,
        mut source: Box<dyn FrameSource + Send + 'static>,
        mut sink: Box<dyn FrameSink + Send + 'static>,
        candidate_session_id: u64,
    ) -> Result<(), SessionError> {
        // Peek the first frame to decide between new-session and resume.
        let first = source.recv_frame().await?;
        let (session_id, restored_membership) = match first.header.message_type {
            MessageType::CtrlNewSession => {
                // Validate payload + reply with status.
                let payload = hla_wire::NewSessionPayload::decode(&first.payload)
                    .map_err(|e| SessionError::Codec(hla_wire::CodecError::Frame(e)))?;
                let reason = if payload.protocol_version == hla_wire::FEDERATE_PROTOCOL_VERSION {
                    hla_wire::NewSessionStatusReason::Success
                } else {
                    hla_wire::NewSessionStatusReason::UnsupportedProtocolVersion
                };
                let status = hla_wire::NewSessionStatusPayload { reason };
                let header = MessageHeader::with_payload_size(
                    hla_wire::NewSessionStatusPayload::SIZE as u32,
                    NO_SEQUENCE_NUMBER,
                    candidate_session_id,
                    first.header.sequence_number,
                    MessageType::CtrlNewSessionStatus,
                );
                sink.send_frame(&Frame::new(header, status.encode().to_vec()))
                    .await?;
                if reason != hla_wire::NewSessionStatusReason::Success {
                    return Err(SessionError::UnsupportedProtocolVersion(
                        payload.protocol_version,
                    ));
                }
                (candidate_session_id, None)
            }
            MessageType::CtrlResumeRequest => {
                let requested_id = first.header.session_id;
                let restored = self.suspended_sessions.remove(&requested_id);
                let (reason, restored_membership) = match restored {
                    Some((_, mut s)) if s.deadline > Instant::now() => {
                        tracing::info!(requested_id, "resuming suspended session");
                        (
                            hla_wire::NewSessionStatusReason::Success,
                            s.membership.take(),
                        )
                    }
                    _ => {
                        tracing::warn!(
                            requested_id,
                            "resume request for unknown or expired session"
                        );
                        (hla_wire::NewSessionStatusReason::OtherError, None)
                    }
                };
                let status = hla_wire::ResumeStatusPayload { reason };
                let resume_session_id = if reason == hla_wire::NewSessionStatusReason::Success {
                    requested_id
                } else {
                    hla_wire::NO_SESSION_ID
                };
                let header = MessageHeader::with_payload_size(
                    hla_wire::ResumeStatusPayload::SIZE as u32,
                    NO_SEQUENCE_NUMBER,
                    resume_session_id,
                    first.header.sequence_number,
                    MessageType::CtrlResumeStatus,
                );
                sink.send_frame(&Frame::new(header, status.encode().to_vec()))
                    .await?;
                if reason != hla_wire::NewSessionStatusReason::Success {
                    return Ok(());
                }
                (requested_id, restored_membership)
            }
            other => {
                return Err(SessionError::UnexpectedMessageType {
                    got: other,
                    expected: MessageType::CtrlNewSession,
                });
            }
        };
        self.metrics.sessions_opened.fetch_add(1, Ordering::Relaxed);
        tracing::info!(session_id, "session opened");

        let hb_config = *self.heartbeat.read();

        // Spin up writer task driven by mpsc.
        let (frame_tx, mut frame_rx) = mpsc::channel::<Frame>(128);
        let writer_session = session_id;
        let writer = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                if let Err(e) = sink.send_frame(&frame).await {
                    tracing::warn!(session_id = writer_session, error = %e, "writer task failed");
                    break;
                }
            }
            tracing::debug!(session_id = writer_session, "writer task exiting");
        });

        let connection = Arc::new(ConnectionHandle {
            session_id,
            frame_tx: frame_tx.clone(),
            next_outbound_seq: AtomicI32::new(0),
        });
        self.connections.insert(session_id, Arc::clone(&connection));

        // Heartbeat sender — periodically pushes CTRL_HEARTBEAT to the
        // federate via the outbound mpsc. If the channel is closed (writer
        // gone), the task exits.
        let hb_conn = Arc::clone(&connection);
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(hb_config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately — skip it to let the connection
            // settle.
            interval.tick().await;
            loop {
                interval.tick().await;
                let seq = hla_wire::claim_next_outbound_seq(&hb_conn.next_outbound_seq);
                let header = MessageHeader::with_payload_size(
                    0,
                    seq,
                    hb_conn.session_id,
                    NO_SEQUENCE_NUMBER,
                    MessageType::CtrlHeartbeat,
                );
                if hb_conn
                    .frame_tx
                    .send(Frame::new(header, Vec::new()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let mut ctx = session::SessionContext::new(session_id);
        ctx.membership = restored_membership;
        let result =
            Self::run_session_loop(&self, &mut *source, &connection, &mut ctx, hb_config).await;

        let reconnect_window = hb_config.reconnect_window;
        if reconnect_window > Duration::ZERO && ctx.membership.is_some() {
            // Suspend rather than immediately auto-resign — federate may
            // reattach via CTRL_RESUME_REQUEST within the window.
            tracing::info!(
                session_id,
                window_ms = reconnect_window.as_millis() as u64,
                "suspending session for resume"
            );
            self.suspended_sessions.insert(
                session_id,
                SuspendedSession {
                    membership: ctx.membership.take(),
                    deadline: Instant::now() + reconnect_window,
                },
            );
        } else if let Some(membership) = ctx.membership.take() {
            tracing::info!(
                session_id,
                federation = %membership.federation.name,
                federate = %membership.federate_name,
                "auto-resigning on connection close"
            );
            membership
                .federation
                .federates
                .write()
                .remove(&membership.federate_handle);
        }

        self.connections.remove(&session_id);
        heartbeat_task.abort();
        drop(frame_tx);
        drop(connection);
        let _ = writer.await;
        let _ = heartbeat_task.await;

        result
    }

    /// Background janitor: expires entries from `suspended_sessions` whose
    /// `deadline` has passed and performs the deferred auto-resign cleanup.
    /// Should be spawned alongside `serve()` / `serve_tls()` / `serve_ws()`
    /// when `heartbeat.reconnect_window` > 0.
    pub async fn run_suspended_session_janitor(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Discard the immediate first tick.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    tracing::info!("suspended-session janitor stopping on shutdown");
                    return;
                }
                _ = ticker.tick() => {}
            }
            let now = Instant::now();
            let expired: Vec<u64> = self
                .suspended_sessions
                .iter()
                .filter(|e| e.value().deadline <= now)
                .map(|e| *e.key())
                .collect();
            for sid in expired {
                if let Some((_, mut s)) = self.suspended_sessions.remove(&sid)
                    && let Some(m) = s.membership.take()
                {
                    tracing::info!(
                        session_id = sid,
                        "suspended session expired — auto-resigning"
                    );
                    m.federation.federates.write().remove(&m.federate_handle);
                }
            }
        }
    }

    /// Pump frames after a successful handshake. Translates `HLA_CALL_REQUEST`
    /// frames through `dispatch_call` and writes back `HLA_CALL_RESPONSE`
    /// (via the connection's outbound mpsc — same path as RTI-initiated
    /// callback frames).
    ///
    /// Tracks last-inbound timestamp; if no frame arrives within
    /// `hb_config.missing_timeout`, the session is forcibly closed.
    async fn run_session_loop<R>(
        node: &Arc<Self>,
        source: &mut R,
        connection: &Arc<ConnectionHandle>,
        ctx: &mut session::SessionContext,
        hb_config: HeartbeatConfig,
    ) -> Result<(), SessionError>
    where
        R: FrameSource + ?Sized,
    {
        let session_id = ctx.session_id;
        let mut last_received_seq: i32;
        let mut last_inbound = Instant::now();
        // Check liveness 4x more often than the timeout so detection latency
        // is bounded at timeout/4.
        let liveness_tick = (hb_config.missing_timeout / 4).max(Duration::from_millis(50));
        let mut liveness_interval = tokio::time::interval(liveness_tick);
        liveness_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Discard the immediate first tick.
        liveness_interval.tick().await;
        loop {
            // TODO(H4): add a third select! branch on
            // `node.shutdown.cancelled()` so in-flight sessions unwind
            // promptly on RTI shutdown rather than waiting for peer EOF
            // or `heartbeat.missing_timeout`.
            let frame = tokio::select! {
                biased;
                _ = liveness_interval.tick() => {
                    if last_inbound.elapsed() > hb_config.missing_timeout {
                        node.metrics.sessions_reaped.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            session_id,
                            since_inbound_ms = last_inbound.elapsed().as_millis() as u64,
                            "federate missed heartbeats — closing session"
                        );
                        return Ok(());
                    }
                    continue;
                }
                frame = source.recv_frame() => {
                    match frame {
                        Ok(f) => f,
                        Err(hla_wire::CodecError::Io(e))
                            if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                        {
                            tracing::info!(session_id, "peer closed connection");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            };
            last_inbound = Instant::now();
            last_received_seq = frame.header.sequence_number;

            match frame.header.message_type {
                MessageType::HlaCallRequest => {
                    node.metrics
                        .calls_dispatched
                        .fetch_add(1, Ordering::Relaxed);
                    let outcome = match fedpro::CallRequest::decode(&frame.payload[..]) {
                        Ok(req) => dispatch::dispatch_call(node, ctx, req),
                        Err(e) => {
                            tracing::warn!(session_id, error = %e, "malformed CallRequest");
                            dispatch::DispatchOutcome {
                                response: fedpro::CallResponse {
                                    call_response: Some(
                                        fedpro::call_response::CallResponse::ExceptionData(
                                            fedpro::ExceptionData {
                                                exception_name: HlaException::RtiInternalError
                                                    .name()
                                                    .to_string(),
                                                details: format!("malformed CallRequest: {e}"),
                                            },
                                        ),
                                    ),
                                },
                                callbacks: Vec::new(),
                            }
                        }
                    };

                    // 1. Send the producer's response first. Callbacks to
                    //    *other* federates' connections are on different
                    //    sockets, so their delivery is concurrent regardless.
                    //    Same-federate callbacks (e.g. TimeRegulationEnabled)
                    //    follow the response on this socket, allowing simple
                    //    clients to do request/response reads without
                    //    interleaving logic.
                    // Tally exceptions for ops dashboards.
                    if let Some(fedpro::call_response::CallResponse::ExceptionData(_)) =
                        outcome.response.call_response.as_ref()
                    {
                        node.metrics.call_exceptions.fetch_add(1, Ordering::Relaxed);
                    }
                    node.metrics
                        .callbacks_emitted
                        .fetch_add(outcome.callbacks.len() as u64, Ordering::Relaxed);

                    let body = outcome.response.encode_to_vec();
                    let payload = HlaCallResponsePayload {
                        response_to_sequence_number: frame.header.sequence_number,
                        body,
                    };
                    let encoded = payload.encode();
                    let header = MessageHeader::with_payload_size(
                        encoded.len() as u32,
                        NO_SEQUENCE_NUMBER,
                        session_id,
                        last_received_seq,
                        MessageType::HlaCallResponse,
                    );
                    if connection
                        .frame_tx
                        .send(Frame::new(header, encoded))
                        .await
                        .is_err()
                    {
                        tracing::warn!(session_id, "writer task gone — closing session");
                        return Ok(());
                    }

                    // 2. Deliver callbacks (cross-federate or same-federate).
                    //    `send().await` applies backpressure — no silent drops.
                    for cb in outcome.callbacks {
                        let target_session = cb.target.session_id;
                        let target_tx = cb.target.frame_tx.clone();
                        let frame = cb.into_frame();
                        if target_tx.send(frame).await.is_err() {
                            tracing::debug!(
                                session_id,
                                target_session,
                                "callback target writer gone"
                            );
                        }
                    }
                }
                MessageType::CtrlTerminateSession => {
                    tracing::info!(session_id, "client requested terminate");
                    let header = MessageHeader::with_payload_size(
                        0,
                        NO_SEQUENCE_NUMBER,
                        session_id,
                        last_received_seq,
                        MessageType::CtrlSessionTerminated,
                    );
                    let _ = connection
                        .frame_tx
                        .send(Frame::new(header, Vec::new()))
                        .await;
                    return Ok(());
                }
                MessageType::HlaCallbackResponse => {
                    // For now the RTI doesn't track in-flight callbacks awaiting
                    // acks; just log and drop.
                    tracing::trace!(session_id, "received callback response (ignored in MVP)");
                }
                MessageType::CtrlHeartbeat => {
                    // Client-initiated heartbeat — echo back as
                    // CTRL_HEARTBEAT_RESPONSE so the federate's own liveness
                    // detector sees us alive.
                    let header = MessageHeader::with_payload_size(
                        0,
                        NO_SEQUENCE_NUMBER,
                        session_id,
                        last_received_seq,
                        MessageType::CtrlHeartbeatResponse,
                    );
                    let _ = connection
                        .frame_tx
                        .send(Frame::new(header, Vec::new()))
                        .await;
                }
                MessageType::CtrlHeartbeatResponse => {
                    // Response to our own server-initiated heartbeat; the
                    // `last_inbound` update above already records liveness.
                    tracing::trace!(session_id, "heartbeat ack");
                }
                other => {
                    tracing::warn!(session_id, ?other, "ignoring unexpected frame type");
                }
            }
        }
    }
}
