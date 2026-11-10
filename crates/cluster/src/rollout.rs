//! Agent upgrades driven by the control plane (`pumasi upgrade rollout`).
//!
//! The owner uploads one binary per platform; the control plane keeps them
//! in `<cluster dir>/releases/<sha256>` and signs a [`Release`] for each.
//! Nodes that accept upgrades (the owner's machines by default, provider
//! machines only with `--auto-upgrade`) get the release for their platform
//! once they are idle. A busy node is drained (no new tasks) one at a time
//! so a rollout finishes without stalling the whole cluster. Agents fetch
//! the binary over their own connection, verify it and restart; a node
//! that reports a failure keeps its binary and is not offered that release
//! again.

use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{
    PROTOCOL_VERSION,
    msg::{NodeUpgradeView, Role, RolloutView, ToAgent, UpgradeState},
    release::{MAX_BINARY_BYTES, Release, ReleaseFile, SignedRelease, Target, hex, is_older},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{NodeRuntime, State, now_unix};

const ROLLOUT_META: &str = "rollout";
/// A node that got a release but neither reconnected with it nor reported
/// a failure within this time is considered failed.
const UPGRADE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// The current rollout, persisted in `meta`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutRecord {
    pub releases: Vec<SignedRelease>,
    pub started_unix: u64,
}

impl RolloutRecord {
    pub fn release_for(&self, target: &Target) -> Option<&SignedRelease> {
        self.releases.iter().find(|r| &r.file().target == target)
    }
}

impl State {
    pub(crate) fn load_rollout(&self) -> Result<Option<RolloutRecord>> {
        match self.store.get_meta(ROLLOUT_META)? {
            Some(b) => Ok(Some(
                postcard::from_bytes(&b).context("corrupt rollout record")?,
            )),
            None => Ok(None),
        }
    }

    /// Starts a rollout of uploaded binaries. `uploaded` says whether a
    /// file's binary is in the release store.
    pub fn start_rollout(
        &mut self,
        files: Vec<ReleaseFile>,
        force: bool,
        uploaded: impl Fn(&ReleaseFile) -> bool,
    ) -> Result<RolloutView> {
        ensure!(!files.is_empty(), "a rollout needs at least one binary");
        let mut targets = std::collections::HashSet::new();
        for f in &files {
            ensure!(
                targets.insert(f.target.clone()),
                "two binaries for {}",
                f.target
            );
            ensure!(
                !f.version.is_empty() && f.size > 0 && f.size <= MAX_BINARY_BYTES,
                "bad binary for {}",
                f.target
            );
            if f.protocol != PROTOCOL_VERSION && !force {
                bail!(
                    "the {} binary speaks protocol {}, this control plane {PROTOCOL_VERSION}: \
                     its agents could not reconnect (they would roll back). Upgrade the \
                     control plane first, or pass --force",
                    f.target,
                    f.protocol
                );
            }
            ensure!(
                uploaded(f),
                "the {} binary ({}) was not uploaded",
                f.target,
                hex(&f.sha256)
            );
        }
        let cluster = *self.key.public().as_bytes();
        let now = now_unix();
        let releases = files
            .into_iter()
            .map(|file| {
                SignedRelease::sign(
                    Release {
                        cluster,
                        file,
                        created_unix: now,
                    },
                    &self.key,
                )
            })
            .collect();
        let record = RolloutRecord {
            releases,
            started_unix: now,
        };
        self.store.put_meta(
            ROLLOUT_META,
            &postcard::to_stdvec(&record).expect("rollout encodes"),
        )?;
        for r in &record.releases {
            info!(target = %r.file().target, version = %r.file().version, sha256 = %hex(&r.file().sha256), "rollout started");
        }
        self.rollout = Some(record);
        self.upgrade_failures.clear();
        self.upgrading.clear();
        for n in self.nodes.values_mut() {
            n.draining = false;
        }
        self.schedule();
        Ok(self.rollout_view())
    }

    pub fn cancel_rollout(&mut self) -> Result<()> {
        self.store.delete_meta(ROLLOUT_META)?;
        self.rollout = None;
        self.upgrade_failures.clear();
        self.upgrading.clear();
        for n in self.nodes.values_mut() {
            n.draining = false;
        }
        self.schedule();
        Ok(())
    }

    /// Release binaries the agents may fetch.
    pub fn rollout_hashes(&self) -> Vec<[u8; 32]> {
        self.rollout
            .iter()
            .flat_map(|r| r.releases.iter().map(|r| r.file().sha256))
            .collect()
    }

    /// Whether `node` may download the binary `sha256`.
    pub fn may_fetch(&self, node: &str, sha256: &[u8; 32]) -> bool {
        self.members.contains_key(node) && self.rollout_hashes().contains(sha256)
    }

    pub fn on_upgrade_failed(
        &mut self,
        node: &str,
        generation: u64,
        sha256: [u8; 32],
        reason: String,
    ) {
        let Some(n) = self.nodes.get_mut(node) else {
            return;
        };
        if n.generation != generation {
            return;
        }
        warn!(node = %node, sha256 = %hex(&sha256), "upgrade failed: {reason}");
        n.draining = false;
        self.upgrading.remove(node);
        let name = self.node_name(node);
        self.event(
            pumasi_proto::monitor::EventLevel::Warning,
            "upgrade.failed",
            Some(node),
            None,
            format!("upgrade of {name} failed: {reason}"),
        );
        self.upgrade_failures
            .insert(node.to_string(), (sha256, reason));
        self.schedule();
    }

    pub(crate) fn expire_upgrades(&mut self) {
        let now = Instant::now();
        let expired: Vec<(String, [u8; 32])> = self
            .upgrading
            .iter()
            .filter(|(_, (_, since))| now.duration_since(*since) > UPGRADE_TIMEOUT)
            .map(|(id, (sha, _))| (id.clone(), *sha))
            .collect();
        for (id, sha) in expired {
            self.upgrading.remove(&id);
            self.upgrade_failures
                .insert(id, (sha, "did not come back with the new binary".into()));
        }
    }

    fn upgrade_state(&self, id: &str, n: &NodeRuntime) -> UpgradeState {
        let target = Target::new(&n.info.os, &n.info.arch);
        let release = self.rollout.as_ref().and_then(|r| r.release_for(&target));
        if !n.online {
            // Restarting into the new binary.
            return match (release, self.upgrading.get(id)) {
                (Some(r), Some((sha, _))) if *sha == r.file().sha256 => UpgradeState::Upgrading,
                _ => UpgradeState::Offline,
            };
        }
        if self.rollout.is_none() {
            return UpgradeState::Current;
        }
        let Some(release) = release else {
            return UpgradeState::NoRelease;
        };
        let sha = release.file().sha256;
        let Some(up) = &n.upgrade else {
            return UpgradeState::Unsupported;
        };
        if up.exe_sha256 == sha {
            return UpgradeState::Current;
        }
        let role = if n.trusted {
            Role::Owner
        } else {
            Role::Provider
        };
        if !up.accepts(role) {
            return UpgradeState::OptedOut;
        }
        if let Some((failed, reason)) = self.upgrade_failures.get(id)
            && *failed == sha
        {
            return UpgradeState::Failed(reason.clone());
        }
        // Never a downgrade: a node already past the rollout's version (say,
        // upgraded by hand while the rollout was still on) keeps its binary.
        if is_older(&release.file().version, &n.info.agent_version) {
            return UpgradeState::Current;
        }
        if self.upgrading.get(id).is_some_and(|(s, _)| *s == sha) {
            return UpgradeState::Upgrading;
        }
        if n.draining {
            UpgradeState::Draining
        } else {
            UpgradeState::Waiting
        }
    }

    /// Offers the release to idle nodes that need it and drains one busy
    /// node at a time. Called at the start of every scheduling pass.
    pub(crate) fn drive_rollout(&mut self) {
        let Some(rollout) = &self.rollout else {
            return;
        };
        let mut offers = Vec::new();
        let mut busy = Vec::new();
        let mut draining = 0;
        for (id, n) in &self.nodes {
            let state = self.upgrade_state(id, n);
            if !matches!(state, UpgradeState::Waiting | UpgradeState::Draining) {
                continue;
            }
            if n.running.is_empty() {
                let target = Target::new(&n.info.os, &n.info.arch);
                if let Some(r) = rollout.release_for(&target) {
                    offers.push((id.clone(), r.clone()));
                }
            } else if n.draining {
                draining += 1;
            } else {
                busy.push(id.clone());
            }
        }
        for (id, release) in offers {
            let Some(n) = self.nodes.get_mut(&id) else {
                continue;
            };
            let sha = release.file().sha256;
            if let Some(tx) = &n.tx
                && tx.send(ToAgent::Upgrade(Box::new(release))).is_ok()
            {
                info!(node = %id, sha256 = %hex(&sha), "offered upgrade");
                n.draining = false;
                self.upgrading.insert(id, (sha, Instant::now()));
            }
        }
        if draining == 0 {
            busy.sort();
            if let Some(id) = busy.first()
                && let Some(n) = self.nodes.get_mut(id)
            {
                info!(node = %id, "draining for upgrade");
                n.draining = true;
            }
        }
    }

    pub fn rollout_view(&self) -> RolloutView {
        let mut nodes: Vec<NodeUpgradeView> = self
            .members
            .values()
            .map(|m| {
                let (exe_sha256, state) = match self.nodes.get(&m.id) {
                    Some(n) => (
                        n.upgrade.as_ref().map(|u| u.exe_sha256),
                        self.upgrade_state(&m.id, n),
                    ),
                    None => (None, UpgradeState::Offline),
                };
                NodeUpgradeView {
                    id: m.id.clone(),
                    name: m.info.name.clone(),
                    target: Target::new(&m.info.os, &m.info.arch),
                    version: m.info.agent_version.clone(),
                    exe_sha256,
                    state,
                }
            })
            .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        RolloutView {
            releases: self
                .rollout
                .as_ref()
                .map(|r| r.releases.clone())
                .unwrap_or_default(),
            started_unix: self.rollout.as_ref().map(|r| r.started_unix).unwrap_or(0),
            nodes,
        }
    }

    /// Sends a release as is, signed or not. Lets tests check that agents
    /// refuse forged releases.
    #[doc(hidden)]
    pub fn send_release_for_tests(&mut self, node: &str, release: SignedRelease) -> Result<()> {
        let n = self.nodes.get(node).context("no such node")?;
        let tx = n.tx.as_ref().context("node offline")?;
        tx.send(ToAgent::Upgrade(Box::new(release)))
            .map_err(|_| anyhow::anyhow!("node offline"))
    }
}

/// Uploaded release binaries, by SHA-256.
#[derive(Debug, Clone)]
pub struct ReleaseStore {
    dir: PathBuf,
}

impl ReleaseStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn path(&self, sha256: &[u8; 32]) -> PathBuf {
        self.dir.join(hex(sha256))
    }

    pub fn path_of(&self, sha256: &[u8; 32]) -> PathBuf {
        self.path(sha256)
    }

    pub fn has(&self, sha256: &[u8; 32], size: u64) -> bool {
        std::fs::metadata(self.path(sha256)).is_ok_and(|m| m.len() == size)
    }

    /// Appends one chunk. Returns how many bytes are stored: `size` once
    /// the binary is complete (also right away when it was uploaded
    /// before).
    pub fn upload(&self, sha256: &[u8; 32], size: u64, offset: u64, bytes: &[u8]) -> Result<u64> {
        ensure!(
            size > 0 && size <= MAX_BINARY_BYTES,
            "binary must be 1 byte to {} MiB",
            MAX_BINARY_BYTES >> 20
        );
        if self.has(sha256, size) {
            return Ok(size);
        }
        let end = offset + bytes.len() as u64;
        ensure!(end <= size, "chunk runs past the end of the binary");
        std::fs::create_dir_all(&self.dir)?;
        let part = self.dir.join(format!("{}.part", hex(sha256)));
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(offset == 0)
            .open(&part)
            .with_context(|| format!("open {}", part.display()))?;
        let have = f.metadata()?.len();
        if have != offset {
            bail!("upload out of order: have {have} bytes, got a chunk at {offset}; start again");
        }
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        if end < size {
            return Ok(end);
        }
        let got = pumasi_proto::release::sha256_file(&part)?;
        if &got != sha256 {
            let _ = std::fs::remove_file(&part);
            bail!(
                "uploaded binary has SHA-256 {}, expected {}",
                hex(&got),
                hex(sha256)
            );
        }
        std::fs::rename(&part, self.path(sha256))?;
        Ok(size)
    }

    /// Reads one chunk of a stored binary.
    pub fn read(&self, sha256: &[u8; 32], offset: u64, len: u32) -> Result<Vec<u8>> {
        let path = self.path(sha256);
        let mut f = std::fs::File::open(&path).context("release binary not found")?;
        let size = f.metadata()?.len();
        ensure!(offset <= size, "offset past the end");
        let len = (len as u64).min(size - offset) as usize;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Deletes binaries and partial uploads not in `keep`.
    pub fn prune(&self, keep: &[[u8; 32]]) {
        let keep: Vec<String> = keep.iter().map(|h| hex(h)).collect();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name.trim_end_matches(".part");
            if !keep.iter().any(|k| k == stem) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
}

/// The directory of the release store inside a cluster directory.
pub fn release_dir(cluster_dir: &Path) -> PathBuf {
    cluster_dir.join("releases")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::release::sha256;

    #[test]
    fn uploads_in_chunks_and_checks_the_hash() {
        let dir = std::env::temp_dir().join(format!("pumasi-rel-{}", rand::random::<u64>()));
        let store = ReleaseStore::new(dir.clone());
        let bin = b"0123456789".to_vec();
        let sha = sha256(&bin);
        assert_eq!(store.upload(&sha, 10, 0, &bin[..4]).unwrap(), 4);
        // Out of order.
        assert!(store.upload(&sha, 10, 8, &bin[8..]).is_err());
        assert_eq!(store.upload(&sha, 10, 0, &bin[..4]).unwrap(), 4);
        assert_eq!(store.upload(&sha, 10, 4, &bin[4..]).unwrap(), 10);
        assert!(store.has(&sha, 10));
        assert_eq!(store.read(&sha, 3, 4).unwrap(), b"3456");
        assert_eq!(store.read(&sha, 8, 100).unwrap(), b"89");
        // Already there.
        assert_eq!(store.upload(&sha, 10, 0, &[]).unwrap(), 10);

        // Wrong contents for the claimed hash.
        let other = sha256(b"something else");
        assert!(store.upload(&other, 3, 0, b"abc").is_err());
        assert!(!store.has(&other, 3));

        store.prune(&[]);
        assert!(!store.has(&sha, 10));
        let _ = std::fs::remove_dir_all(dir);
    }
}
