//! Upgrades that reach the control plane (1.13): which binary a replica
//! installs over its own, and when it may restart into it.
//!
//! The agents go first (the rollout as before). Then the followers, one at
//! a time: a follower with a newer binary asks the leader for its turn
//! ([`ha::RaftReq::Turn`]), and the leader grants it only while no other
//! replica is upgrading and every other voter answers, so the quorum holds
//! while it is gone. The turn ends once the replica reports the new version
//! (the leader asks every replica's version every few seconds,
//! [`ha::RaftReq::Version`]) or after [`TURN_TIMEOUT`]. Last the leader
//! itself, once every other replica runs the new version: it steps down by
//! restarting, the others elect a new leader within a couple of seconds,
//! and it comes back as a follower. A single control plane restarts once
//! the agents are done, as before.

use std::collections::HashMap;

use iroh::EndpointId;

use pumasi_proto::release::{
    ControlPlaneUpgrade, ControlPlaneView, ReleaseFile, ReplicaVersion, Target, is_older,
};

use super::*;

/// How long a replica's turn lasts at most: its trial and some margin.
const TURN_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// A replica's reported version is trusted this long.
const VERSION_FRESH: Duration = Duration::from_secs(60);
/// How often the leader asks the replicas' versions.
pub(super) const VERSIONS_EVERY: Duration = Duration::from_secs(5);
/// A voter that answered within this long counts as up.
const UP_WITHIN_MS: u64 = 3_000;

/// A replica the leader let restart, into what, and when.
#[derive(Debug, Clone)]
pub(crate) struct Grant {
    peer: EndpointId,
    version: String,
    at: Instant,
}

/// What the leader knows about the replicas' own upgrades.
#[derive(Debug, Default)]
pub(crate) struct OwnRollout {
    grant: Option<Grant>,
    versions: HashMap<EndpointId, (Option<ReplicaVersion>, Instant)>,
    /// Why this replica is not restarting yet, for the views; `None` while
    /// it has nothing to restart into.
    mine: Option<ControlPlaneUpgrade>,
    /// The version this replica says it runs, in tests (all replicas of a
    /// test run one build).
    reported: Option<String>,
}

impl Shared {
    /// The current rollout: the leader's state, or on a follower the
    /// replicated database.
    fn rollout_record(&self) -> Option<crate::rollout::RolloutRecord> {
        match self.active() {
            Some(inner) => inner.state().rollout.clone(),
            None => crate::rollout::load_rollout_from(&self.store)
                .ok()
                .flatten(),
        }
    }

    /// The rollout's binary for `target`.
    fn release_for(&self, target: &Target) -> Option<ReleaseFile> {
        self.rollout_record()?
            .release_for(target)
            .map(|r| r.file().clone())
    }

    /// No release download, and no agent waiting for, draining for or
    /// installing the rollout. True on a follower.
    pub(super) fn agents_settled(&self) -> bool {
        if matches!(
            self.latest.lock().unwrap().fetch,
            pumasi_proto::release::FetchState::Running { .. }
        ) {
            return false;
        }
        let Some(inner) = self.active() else {
            return true;
        };
        let view = inner.state().rollout_view();
        !view.nodes.iter().any(|n| {
            matches!(
                n.state,
                pumasi_proto::msg::UpgradeState::Waiting
                    | pumasi_proto::msg::UpgradeState::Draining
                    | pumasi_proto::msg::UpgradeState::Upgrading
            )
        })
    }

    /// What this replica answers [`ha::RaftReq::Version`] with.
    pub(super) fn own_version(&self) -> ReplicaVersion {
        let reported = self.own_rollout.lock().unwrap().reported.clone();
        ReplicaVersion {
            version: reported.unwrap_or_else(|| pumasi_proto::version().to_string()),
            target: Target::host(),
            failed: self.own.last_failure(),
        }
    }

    fn fresh_version(&self, id: &EndpointId) -> Option<ReplicaVersion> {
        let r = self.own_rollout.lock().unwrap();
        let (v, at) = r.versions.get(id)?;
        (at.elapsed() < VERSION_FRESH).then(|| v.clone()).flatten()
    }

    /// Whether `version` is `v` or older.
    fn reached(v: &str, version: &str) -> bool {
        !is_older(v, version)
    }

    /// The other voters that answered lately, and how many voters there
    /// are, with replication on.
    fn quorum_after_leaving(&self, ha: &Ha, leaving: &str) -> Result<(), String> {
        let view = ha.view();
        let voters = view.replicas.iter().filter(|r| r.voter).count();
        let up = view.voters_up(UP_WITHIN_MS);
        let leaving_counts = view.replicas.iter().any(|r| {
            r.id == leaving
                && r.voter
                && (r.leader || r.last_seen_ms.is_some_and(|t| t <= UP_WITHIN_MS))
        });
        let after = up - usize::from(leaving_counts);
        let need = voters / 2 + 1;
        if after < need {
            return Err(format!(
                "{up} of {voters} voting replicas answer; restarting this one would leave {after}, and a majority needs {need}"
            ));
        }
        Ok(())
    }

    /// Whether the replica the leader let restart is done (runs the
    /// version), or its turn ran out.
    fn grant_over(&self, g: &Grant) -> bool {
        g.at.elapsed() >= TURN_TIMEOUT
            || self
                .fresh_version(&g.peer)
                .is_some_and(|v| Self::reached(&v.version, &g.version))
    }

    /// On the leader: may replica `peer` restart into `version` now?
    pub(super) fn grant_turn(&self, peer: EndpointId, version: &str) -> Result<(), String> {
        let ha = self.ha().ok_or("replication is off")?;
        if self.active().is_none() || !ha.is_leader() {
            return Err("this replica does not lead".into());
        }
        if !self.agents_settled() {
            return Err("the agents' rollout is still under way".into());
        }
        let mut r = self.own_rollout.lock().unwrap();
        if let Some(g) = r.grant.clone()
            && g.peer != peer
        {
            drop(r);
            if !self.grant_over(&g) {
                return Err(format!(
                    "replica {} is restarting into {}",
                    g.peer.fmt_short(),
                    g.version
                ));
            }
            r = self.own_rollout.lock().unwrap();
        }
        drop(r);
        self.quorum_after_leaving(&ha, &peer.to_string())?;
        let mut r = self.own_rollout.lock().unwrap();
        r.grant = Some(Grant {
            peer,
            version: version.to_string(),
            at: Instant::now(),
        });
        info!(replica = %peer.fmt_short(), %version, "a control plane replica may restart into a new version");
        Ok(())
    }

    /// On the leader, after the agents and every other replica: may this
    /// one restart into `version` now (stepping down)?
    fn leader_may_restart(&self, version: &str) -> Result<(), String> {
        if !self.agents_settled() {
            return Err("the agents' rollout is still under way".into());
        }
        let Some(ha) = self.ha() else {
            return Ok(());
        };
        let me = self.endpoint.id();
        for (_, n) in ha.members() {
            let Some(addr) = n.addr else { continue };
            if addr.id == me {
                continue;
            }
            match self.fresh_version(&addr.id) {
                Some(v) if Self::reached(&v.version, version) => {}
                Some(v) => {
                    return Err(format!(
                        "replica {} still runs {} (followers go first)",
                        addr.id.fmt_short(),
                        v.version
                    ));
                }
                None => {
                    return Err(format!(
                        "replica {} has not said what it runs (down, or older than 1.13: upgrade it by hand)",
                        addr.id.fmt_short()
                    ));
                }
            }
        }
        if let Some(g) = self.own_rollout.lock().unwrap().grant.clone()
            && !self.grant_over(&g)
        {
            return Err(format!("replica {} is restarting", g.peer.fmt_short()));
        }
        self.quorum_after_leaving(&ha, &me.to_string())
    }

    /// Asks every replica what it runs (on the leader).
    pub(super) async fn poll_versions(&self, ha: &Ha) {
        let got = ha.versions().await;
        let now = Instant::now();
        let mut r = self.own_rollout.lock().unwrap();
        r.versions.retain(|id, _| got.iter().any(|(g, _)| g == id));
        for (id, v) in got {
            r.versions.insert(id, (v, now));
        }
        if let Some(g) = r.grant.clone() {
            let done = r
                .versions
                .get(&g.peer)
                .and_then(|(v, _)| v.as_ref())
                .is_some_and(|v| Self::reached(&v.version, &g.version));
            if done || g.at.elapsed() >= TURN_TIMEOUT {
                r.grant = None;
            }
        }
    }

    /// Each replica's version and where it is with the rollout.
    pub(super) fn control_planes(&self) -> Vec<ControlPlaneView> {
        let record = self.rollout_record();
        let state_of =
            |version: Option<&str>, target: Option<&Target>, failed: Option<&(String, String)>| {
                let Some(record) = &record else {
                    return ControlPlaneUpgrade::Current;
                };
                let (Some(version), Some(target)) = (version, target) else {
                    return ControlPlaneUpgrade::Unknown;
                };
                let Some(release) = record.release_for(target) else {
                    return ControlPlaneUpgrade::NoRelease;
                };
                let want = &release.file().version;
                if Self::reached(version, want) {
                    return ControlPlaneUpgrade::Current;
                }
                if let Some((v, why)) = failed
                    && v == want
                {
                    return ControlPlaneUpgrade::Failed(why.clone());
                }
                ControlPlaneUpgrade::Waiting(
                    "its turn comes after the agents, one replica at a time".into(),
                )
            };
        let me = self.endpoint.id();
        let mine = self.own_version();
        let mut my_state = state_of(
            Some(&mine.version),
            Some(&mine.target),
            mine.failed.as_ref(),
        );
        if matches!(my_state, ControlPlaneUpgrade::Waiting(_))
            && let Some(s) = self.own_rollout.lock().unwrap().mine.clone()
        {
            my_state = s;
        }
        let mut out = vec![ControlPlaneView {
            id: me.to_string(),
            leader: true,
            version: Some(mine.version),
            target: Some(mine.target),
            state: my_state,
        }];
        let Some(ha) = self.ha() else {
            return out;
        };
        let grant = self.own_rollout.lock().unwrap().grant.clone();
        for (_, n) in ha.members() {
            let Some(addr) = n.addr else { continue };
            if addr.id == me {
                continue;
            }
            let v = self.fresh_version(&addr.id);
            let mut state = state_of(
                v.as_ref().map(|v| v.version.as_str()),
                v.as_ref().map(|v| &v.target),
                v.as_ref().and_then(|v| v.failed.as_ref()),
            );
            if matches!(
                state,
                ControlPlaneUpgrade::Waiting(_) | ControlPlaneUpgrade::Unknown
            ) && grant
                .as_ref()
                .is_some_and(|g| g.peer == addr.id && !self.grant_over(g))
            {
                state = ControlPlaneUpgrade::Upgrading;
            }
            out.push(ControlPlaneView {
                id: addr.id.to_string(),
                leader: false,
                version: v.as_ref().map(|v| v.version.clone()),
                target: v.map(|v| v.target),
                state,
            });
        }
        out
    }

    /// The platforms of replicas that run a version older than `tag`, for
    /// `upgrade latest`.
    pub(super) fn replica_targets_older_than(&self, tag: &str) -> Vec<Target> {
        let r = self.own_rollout.lock().unwrap();
        r.versions
            .values()
            .filter_map(|(v, at)| v.as_ref().filter(|_| at.elapsed() < VERSION_FRESH))
            .filter(|v| is_older(&v.version, tag))
            .map(|v| v.target.clone())
            .collect()
    }
}

impl Cluster {
    /// The binary this control plane should install over its own: the
    /// current rollout's release for this platform, newer than the running
    /// version, kept in the release store, and not rolled back from before.
    /// `local_node` is the node id of an agent on this machine, if it has
    /// one; while that agent is a member of the cluster it upgrades the
    /// shared binary itself (and rolls back if it fails), so this returns
    /// `None`. Followers answer too (1.13), from the replicated rollout.
    pub fn own_upgrade(&self, local_node: Option<&str>) -> Option<OwnUpgrade> {
        let shared = &self.shared;
        if let Some(id) = local_node {
            let member = match shared.active() {
                Some(inner) => inner.state().members.contains_key(id),
                // A follower has no state loaded; the agent's own upgrade
                // record in the database says whether it is a member.
                None => shared.store.is_member(id),
            };
            if member {
                return None;
            }
        }
        if shared.ha().is_some_and(|h| !h.in_step()) {
            return None;
        }
        let file = shared.release_for(&Target::host())?;
        if !is_older(pumasi_proto::version(), &file.version)
            || !shared.releases.has(&file.sha256, file.size)
            || shared.own.failed_with(&file.sha256).is_some()
        {
            return None;
        }
        Some(OwnUpgrade {
            path: shared.releases.path_of(&file.sha256),
            file,
        })
    }

    /// Whether this control plane may restart into `version` now: a single
    /// one once the agents' rollout settled; with replicas, followers one
    /// at a time with the leader's leave, and the leader last. `Err` says
    /// why not yet.
    pub async fn may_restart(&self, version: &str) -> Result<(), String> {
        let shared = &self.shared;
        let res = match shared.ha() {
            None => {
                if shared.agents_settled() {
                    Ok(())
                } else {
                    Err("the agents' rollout is still under way".into())
                }
            }
            Some(ha) if ha.is_leader() && shared.active().is_some() => {
                shared.leader_may_restart(version)
            }
            Some(ha) => ha.ask_turn(version).await,
        };
        shared.own_rollout.lock().unwrap().mine = Some(match &res {
            Ok(()) => ControlPlaneUpgrade::Upgrading,
            Err(why) => ControlPlaneUpgrade::Waiting(why.clone()),
        });
        res
    }

    /// Whether this control plane works: a single one serves; a replica
    /// leads and serves, or follows a leader and is caught up.
    pub fn healthy(&self) -> bool {
        match self.shared.ha() {
            None => self.shared.active().is_some(),
            Some(ha) => {
                if ha.is_leader() {
                    self.shared.active().is_some()
                } else {
                    ha.in_step()
                }
            }
        }
    }

    /// Makes this replica say it runs `version` (tests: every replica in
    /// one process runs the same build).
    #[doc(hidden)]
    pub fn report_version_for_tests(&self, version: &str) {
        self.shared.own_rollout.lock().unwrap().reported = Some(version.to_string());
    }

    /// The records of this control plane's own upgrades.
    pub fn own_trial(&self) -> &crate::own::OwnTrial {
        &self.shared.own
    }

    /// Each replica's version and where it is with the rollout (the
    /// leader's view; a follower sees only itself).
    pub fn control_planes(&self) -> Vec<ControlPlaneView> {
        self.shared.control_planes()
    }
}
