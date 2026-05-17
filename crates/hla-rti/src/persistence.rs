//! Federation-state persistence for Save/Restore.
//!
//! MVP backend: one JSON file per `(save_dir, label)`. Captures object
//! instances + sync points + attribute ownership (keyed by federate **name**
//! so handle reassignment on restore is unambiguous). Federate identities,
//! pub/sub state, and connections are re-established by the federates
//! themselves on the post-restore re-join.
//!
//! This MVP intentionally does not persist:
//!   * federation FOM (assumed identical pre/post-save)
//!   * federate pub/sub state (federates re-publish/subscribe post-restore)
//!   * DDM regions (federates re-create)
//!   * time-management state (federates re-enable regulation/constrained)
//!   * TSO queues (drained or lost on restore)
//!
//! These omissions are documented; a production backend would extend the
//! schema to cover them.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hla_core::{AttributeHandle, ObjectClassHandle, ObjectInstanceHandle};
use serde::{Deserialize, Serialize};

use crate::Federation;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct FederationSnapshot {
    pub federation_name: String,
    pub instances: Vec<SerInstance>,
    pub sync_points: Vec<SerSyncPoint>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct SerInstance {
    pub handle: u64,
    pub class: u32,
    pub name: String,
    pub registrar_name: String,
    /// Each attribute's owner federate, by NAME so handle reassignment works.
    pub attribute_owners: Vec<(u32, String)>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct SerSyncPoint {
    pub label: String,
    pub tag: Vec<u8>,
    /// Participant federate **names** (handle-independent).
    pub participants: Vec<String>,
}

impl Federation {
    /// Capture a snapshot of the federation's restorable state. Federate
    /// identity is keyed by name (not handle) so handle reassignment on
    /// restore is unambiguous.
    pub(crate) fn snapshot(&self) -> FederationSnapshot {
        let federates = self.federates.read();
        // handle → name for fast reverse lookup.
        let handle_to_name: HashMap<_, String> = federates
            .values()
            .map(|fs| (fs.handle, fs.name.clone()))
            .collect();

        let instances: Vec<SerInstance> = {
            let map = self.object_instances.read();
            map.values()
                .map(|inst| SerInstance {
                    handle: inst.handle.raw(),
                    class: inst.class.raw(),
                    name: inst.name.clone(),
                    registrar_name: handle_to_name
                        .get(&inst.registrar)
                        .cloned()
                        .unwrap_or_else(|| String::from("<unknown>")),
                    attribute_owners: inst
                        .attribute_owners
                        .iter()
                        .filter_map(|(attr, fed)| {
                            handle_to_name.get(fed).map(|n| (attr.raw(), n.clone()))
                        })
                        .collect(),
                })
                .collect()
        };

        let sync_points: Vec<SerSyncPoint> = {
            let sps = self.sync_points.read();
            sps.values()
                .map(|sp| SerSyncPoint {
                    label: sp.label.clone(),
                    tag: sp.tag.clone(),
                    participants: sp
                        .participants
                        .iter()
                        .filter_map(|fh| handle_to_name.get(fh).cloned())
                        .collect(),
                })
                .collect()
        };

        FederationSnapshot {
            federation_name: self.name.clone(),
            instances,
            sync_points,
        }
    }

    /// Apply a snapshot to this federation. Existing instances are cleared
    /// and replaced with the snapshot's. Federate names in the snapshot are
    /// mapped to currently-joined federate handles; ownership entries for
    /// federates not present today are dropped (their attributes become
    /// unowned).
    pub(crate) fn apply_snapshot(&self, snap: &FederationSnapshot) {
        let federates = self.federates.read();
        let name_to_handle: HashMap<String, hla_core::FederateHandle> = federates
            .values()
            .map(|fs| (fs.name.clone(), fs.handle))
            .collect();
        drop(federates);

        let mut instances = self.object_instances.write();
        instances.clear();
        for ser in &snap.instances {
            let handle = ObjectInstanceHandle::new(ser.handle);
            let class = ObjectClassHandle::new(ser.class);
            let registrar = match name_to_handle.get(&ser.registrar_name) {
                Some(h) => *h,
                None => continue, // registrar federate not present — skip
            };
            let attribute_owners: HashMap<AttributeHandle, hla_core::FederateHandle> = ser
                .attribute_owners
                .iter()
                .filter_map(|(attr, name)| {
                    name_to_handle
                        .get(name)
                        .map(|h| (AttributeHandle::new(*attr), *h))
                })
                .collect();
            instances.insert(
                handle,
                crate::ObjectInstance {
                    handle,
                    class,
                    name: ser.name.clone(),
                    registrar,
                    attribute_owners,
                    attribute_regions: HashMap::new(),
                },
            );
            // Advance next_object_id past restored handles.
            let cur = self
                .next_object_id
                .load(std::sync::atomic::Ordering::Relaxed);
            if cur <= ser.handle {
                self.next_object_id
                    .store(ser.handle + 1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        drop(instances);

        let mut sync_points = self.sync_points.write();
        sync_points.clear();
        for ser in &snap.sync_points {
            let participants: std::collections::HashSet<_> = ser
                .participants
                .iter()
                .filter_map(|n| name_to_handle.get(n).copied())
                .collect();
            sync_points.insert(
                ser.label.clone(),
                crate::SyncPoint {
                    label: ser.label.clone(),
                    tag: ser.tag.clone(),
                    participants,
                    achieved: std::collections::HashSet::new(),
                    failed_to_sync: std::collections::HashSet::new(),
                },
            );
        }
    }
}

/// Filesystem path for a snapshot file. One file per (save_dir, federation, label).
pub(crate) fn snapshot_path(save_dir: &Path, federation: &str, label: &str) -> PathBuf {
    let safe_fed = federation.replace(['/', '\\'], "_");
    let safe_label = label.replace(['/', '\\'], "_");
    save_dir.join(format!("{safe_fed}__{safe_label}.json"))
}

pub(crate) fn write_snapshot(
    save_dir: &Path,
    federation: &str,
    label: &str,
    snap: &FederationSnapshot,
) -> io::Result<()> {
    fs::create_dir_all(save_dir)?;
    let path = snapshot_path(save_dir, federation, label);
    let json = serde_json::to_string_pretty(snap)
        .map_err(|e| io::Error::other(format!("serialize: {e}")))?;
    fs::write(path, json)
}

pub(crate) fn read_snapshot(
    save_dir: &Path,
    federation: &str,
    label: &str,
) -> io::Result<FederationSnapshot> {
    let path = snapshot_path(save_dir, federation, label);
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(|e| io::Error::other(format!("deserialize: {e}")))
}
