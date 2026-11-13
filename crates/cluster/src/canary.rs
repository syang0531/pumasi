//! Canary rollouts and node settings from the dashboard (1.7). Extends
//! [`State`].
//!
//! A canary rollout ([`AdminOp::CanaryRollout`], `pumasi upgrade latest
//! --canary`) is an ordinary rollout ([`crate::rollout`]) that offers the
//! release only to the nodes its selector picked when it started. The
//! others wait ("held") until every canary that can take the release runs
//! it and, for the soak time since the last of them came back, is online
//! and has finished the required tasks without a failure of its own (an
//! `Outcome::Error`, which is the node failing to run a task rather than
//! the program failing). Then the rollout goes on to every node, as
//! before. A canary that fails to install the release (or rolls back) or
//! fails a task halts the rollout at the canaries; an admin promotes it
//! anyway (`pumasi upgrade promote`), adds nodes to the canaries, or
//! cancels it. The canary is kept beside the rollout record in `meta`
//! (`rollout_canary`), so it survives a restart and reaches replicas.
//!
//! Node settings: agents of minor 3 report their availability settings
//! after the welcome; [`AdminOp::SetNodeSettings`] sends new ones, the
//! agent writes them to its service's options file and answers, and
//! restarts with them once its running tasks have ended.
//!
//! [`AdminOp::CanaryRollout`]: pumasi_proto::msg::AdminOp::CanaryRollout
//! [`AdminOp::SetNodeSettings`]: pumasi_proto::msg::AdminOp::SetNodeSettings

use std::collections::HashMap;

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{
    SETTINGS_MINOR,
    monitor::EventLevel,
    msg::{Outcome, ToAgent, UpgradeState},
    monitor::human_duration,
    release::{CanaryNodeView, CanaryPhase, CanarySpec, CanaryView, Target},
    ui::{NodeSettings, NodeSettingsView, SettingsReport},
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::state::{State, now_unix};

const CANARY_META: &str = "rollout_canary";

/// A canary of the current rollout, persisted in `meta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryRecord {
    pub spec: CanarySpec,
    pub started_unix: u64,
    /// The binaries of the rollout it belongs to: a canary never outlives
    /// its rollout.
    pub shas: Vec<[u8; 32]>,
    pub canaries: Vec<CanaryNode>,
    pub phase: CanaryPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryNode {
    pub id: String,
    pub upgraded_unix: Option<u64>,
    pub tasks_ok: u32,
    pub tasks_failed: u32,
}

impl CanaryRecord {
    fn is_canary(&self, id: &str) -> bool {
        self.canaries.iter().any(|c| c.id == id)
    }
}

/// What a settings change waits on: the agent's answer.
pub(crate) type SettingsReply = oneshot::Receiver<Result<SettingsReport, String>>;

/// The node settings part of [`State`].
#[derive(Default)]
pub(crate) struct SettingsState {
    /// What each agent of minor 3 last said, by node id.
    reports: HashMap<String, SettingsReport>,
    waiters: HashMap<String, Vec<oneshot::Sender<Result<SettingsReport, String>>>>,
}

impl SettingsState {
    pub(crate) fn drop_waiters(&mut self, id: &str) {
        self.waiters.remove(id);
    }
}

impl State {
    pub(crate) fn load_canary(&self) -> Result<Option<CanaryRecord>> {
        let Some(c) = self.store.meta_record::<CanaryRecord>(CANARY_META)? else {
            return Ok(None);
        };
        // Only with the rollout it was made for.
        let shas = self.rollout_hashes();
        Ok((!shas.is_empty() && shas == c.shas).then_some(c))
    }

    fn save_canary(&self) {
        let res = match &self.canary {
            Some(c) => self.store.put_meta_record(CANARY_META, c),
            None => self.store.delete_meta(CANARY_META),
        };
        if let Err(e) = res {
            warn!("persist the canary: {e:#}");
        }
    }

    /// The canary record for a rollout of `shas` that starts now: the
    /// members the selector picks (by their labels, `name=` included).
    pub(crate) fn new_canary(&self, spec: CanarySpec, shas: Vec<[u8; 32]>) -> Result<CanaryRecord> {
        spec.validate()?;
        let mut ids: Vec<&String> = self
            .members
            .keys()
            .filter(|id| spec.nodes.matches(&self.node_labels(id)))
            .collect();
        ids.sort();
        ensure!(
            !ids.is_empty(),
            "no node matches the canary selector {}",
            spec.nodes
        );
        ensure!(
            ids.len() < self.members.len(),
            "the canary selector {} picks every node; leave out --canary for that",
            spec.nodes
        );
        Ok(CanaryRecord {
            spec,
            started_unix: now_unix(),
            shas,
            canaries: ids
                .into_iter()
                .map(|id| CanaryNode {
                    id: id.clone(),
                    upgraded_unix: None,
                    tasks_ok: 0,
                    tasks_failed: 0,
                })
                .collect(),
            phase: CanaryPhase::Canary,
        })
    }

    /// Sets (or clears) the canary of the rollout that just started.
    pub(crate) fn set_canary(&mut self, c: Option<CanaryRecord>) {
        self.canary = c;
        self.save_canary();
    }

    /// Whether the rollout passes `id` by for now: it is not a canary of
    /// a canary that has not been promoted.
    pub(crate) fn held_back(&self, id: &str) -> bool {
        self.canary
            .as_ref()
            .is_some_and(|c| !matches!(c.phase, CanaryPhase::Promoted { .. }) && !c.is_canary(id))
    }

    /// Notes a task a node finished, for its canary record.
    pub(crate) fn canary_note_run(&mut self, node: &str, outcome: &Outcome) {
        let Some(c) = &mut self.canary else {
            return;
        };
        if !matches!(c.phase, CanaryPhase::Canary) {
            return;
        }
        let Some(n) = c.canaries.iter_mut().find(|n| n.id == node) else {
            return;
        };
        if n.upgraded_unix.is_none() {
            return;
        }
        let failed = match outcome {
            Outcome::Error(e) => Some(e.clone()),
            _ => {
                n.tasks_ok += 1;
                None
            }
        };
        if let Some(e) = failed {
            n.tasks_failed += 1;
            let name = self.node_name(node);
            self.halt_canary(format!("canary {name} failed a task: {e}"));
        } else {
            self.save_canary();
        }
    }

    fn halt_canary(&mut self, reason: String) {
        let Some(c) = &mut self.canary else {
            return;
        };
        warn!("canary rollout halted: {reason}");
        c.phase = CanaryPhase::Halted {
            reason: reason.clone(),
        };
        self.save_canary();
        self.event(
            EventLevel::Warning,
            "rollout.halted",
            None,
            None,
            format!("rollout halted at the canaries: {reason}"),
        );
    }

    fn promote(&mut self, by: String) {
        let Some(c) = &mut self.canary else {
            return;
        };
        info!(%by, "canary rollout promoted");
        c.phase = CanaryPhase::Promoted {
            unix: now_unix(),
            by: by.clone(),
        };
        self.save_canary();
        self.event(
            EventLevel::Info,
            "rollout.promoted",
            None,
            None,
            format!("the canaries passed; the rollout goes on to every node ({by})"),
        );
    }

    /// Moves the canary along: records canaries that came back with the
    /// release, halts on failures, and promotes once the soak is over.
    /// Called at the start of every scheduling pass with a rollout.
    pub(crate) fn drive_canary(&mut self) {
        let Some(c) = &self.canary else {
            return;
        };
        if !matches!(c.phase, CanaryPhase::Canary) {
            return;
        }
        let (waiting, failure, upgraded) = self.canary_progress();
        let now = now_unix();
        if !upgraded.is_empty() {
            if let Some(c) = &mut self.canary {
                for n in &mut c.canaries {
                    if upgraded.contains(&n.id) {
                        n.upgraded_unix = Some(now);
                    }
                }
            }
            self.save_canary();
        }
        if let Some(reason) = failure {
            self.halt_canary(reason);
            return;
        }
        if waiting.is_none() {
            self.promote("the canaries passed".into());
        }
    }

    /// What the canary waits for (`None`: it passed), a failure that
    /// halts it, and canaries that just came back with the release.
    fn canary_progress(&self) -> (Option<String>, Option<String>, Vec<String>) {
        let Some(c) = &self.canary else {
            return (None, None, Vec::new());
        };
        let now = now_unix();
        let mut upgraded = Vec::new();
        let mut waits = Vec::new();
        let mut applicable = 0;
        let mut last_up = 0;
        for n in &c.canaries {
            let name = self.node_name(&n.id);
            let state = match self.nodes.get(&n.id) {
                Some(rt) => self.upgrade_state(&n.id, rt),
                None => UpgradeState::Offline,
            };
            let online = self.nodes.get(&n.id).is_some_and(|rt| rt.online);
            match state {
                UpgradeState::NoRelease | UpgradeState::OptedOut | UpgradeState::Unsupported => {
                    continue;
                }
                UpgradeState::Failed(reason) => {
                    return (
                        None,
                        Some(format!("canary {name} did not take the release: {reason}")),
                        upgraded,
                    );
                }
                _ => {}
            }
            applicable += 1;
            let up = match n.upgraded_unix {
                Some(t) => Some(t),
                None if online && state == UpgradeState::Current && self.runs_release(&n.id) => {
                    upgraded.push(n.id.clone());
                    Some(now)
                }
                None => None,
            };
            match up {
                None => waits.push(format!("{name} to upgrade")),
                Some(t) => {
                    last_up = last_up.max(t);
                    if !online {
                        waits.push(format!("{name} to come back online"));
                    } else if n.tasks_ok < c.spec.min_tasks {
                        waits.push(format!(
                            "{name} to finish {} more task(s)",
                            c.spec.min_tasks - n.tasks_ok
                        ));
                    }
                }
            }
        }
        if applicable == 0 {
            return (
                None,
                Some("none of the canaries can take this release".into()),
                upgraded,
            );
        }
        if waits.is_empty() {
            let soak_end = last_up + c.spec.soak_secs;
            if now < soak_end {
                waits.push(format!(
                    "the canaries to soak for {} more",
                    human_duration(soak_end - now)
                ));
            } else if c.spec.hold {
                waits.push("an admin to promote it (pumasi upgrade promote)".into());
            }
        }
        let waiting = (!waits.is_empty()).then(|| waits.join(", "));
        (waiting, None, upgraded)
    }

    /// Whether the node runs the binary the rollout has for its platform.
    fn runs_release(&self, id: &str) -> bool {
        let Some(rt) = self.nodes.get(id) else {
            return false;
        };
        let target = Target::new(&rt.info.os, &rt.info.arch);
        match (
            self.rollout.as_ref().and_then(|r| r.release_for(&target)),
            &rt.upgrade,
        ) {
            (Some(r), Some(up)) => up.exe_sha256 == r.file().sha256,
            _ => false,
        }
    }

    pub fn canary_view(&self) -> Option<CanaryView> {
        let c = self.canary.as_ref()?;
        let (waiting_for, _, _) = match c.phase {
            CanaryPhase::Canary => self.canary_progress(),
            _ => (None, None, Vec::new()),
        };
        let mut held: Vec<String> = self
            .members
            .keys()
            .filter(|id| self.held_back(id))
            .map(|id| self.node_name(id))
            .collect();
        held.sort();
        Some(CanaryView {
            spec: c.spec.clone(),
            started_unix: c.started_unix,
            phase: c.phase.clone(),
            canaries: c
                .canaries
                .iter()
                .map(|n| CanaryNodeView {
                    id: n.id.clone(),
                    name: self.node_name(&n.id),
                    upgraded_unix: n.upgraded_unix,
                    tasks_ok: n.tasks_ok,
                    tasks_failed: n.tasks_failed,
                })
                .collect(),
            held,
            waiting_for,
        })
    }

    /// Lets more nodes have the release: `nodes` join the canaries, or
    /// with none the canary ends and every node gets it.
    pub fn promote_canary(&mut self, nodes: &[String], by: &str) -> Result<Option<CanaryView>> {
        let Some(c) = &self.canary else {
            bail!("the current rollout has no canary (nothing to promote)");
        };
        if matches!(c.phase, CanaryPhase::Promoted { .. }) {
            bail!("the canary was promoted already; every node gets the release");
        }
        if nodes.is_empty() {
            self.promote(format!("promoted by {by}"));
        } else {
            let ids = nodes
                .iter()
                .map(|n| self.resolve_node(n))
                .collect::<Result<Vec<_>>>()?;
            let mut names = Vec::new();
            if let Some(c) = &mut self.canary {
                for id in ids {
                    if !c.is_canary(&id) {
                        c.canaries.push(CanaryNode {
                            id: id.clone(),
                            upgraded_unix: None,
                            tasks_ok: 0,
                            tasks_failed: 0,
                        });
                        names.push(id);
                    }
                }
            }
            let names: Vec<String> = names.iter().map(|id| self.node_name(id)).collect();
            self.save_canary();
            if !names.is_empty() {
                self.event(
                    EventLevel::Info,
                    "rollout.canary",
                    None,
                    None,
                    format!("{} added to the canaries by {by}", names.join(", ")),
                );
            }
        }
        self.schedule();
        Ok(self.canary_view())
    }

    // -- node settings ------------------------------------------------------

    /// An agent's settings: after the welcome, or in answer to a change.
    pub fn on_settings(
        &mut self,
        id: &str,
        generation: u64,
        report: SettingsReport,
        error: Option<String>,
    ) {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation && n.online)
        {
            return;
        }
        let changed = self.settings.reports.get(id) != Some(&report);
        self.settings.reports.insert(id.to_string(), report.clone());
        let waiters = self.settings.waiters.remove(id).unwrap_or_default();
        if !waiters.is_empty() {
            let name = self.node_name(id);
            match &error {
                None if changed => self.event(
                    EventLevel::Info,
                    "node.settings",
                    Some(id),
                    None,
                    format!(
                        "{name}: settings changed to {}{}",
                        report.settings.describe(),
                        if report.restarting {
                            "; restarts once its tasks end"
                        } else {
                            ""
                        }
                    ),
                ),
                None => {}
                Some(e) => self.event(
                    EventLevel::Warning,
                    "node.settings",
                    Some(id),
                    None,
                    format!("{name}: settings not changed: {e}"),
                ),
            }
        }
        for w in waiters {
            let _ = w.send(match &error {
                None => Ok(report.clone()),
                Some(e) => Err(e.clone()),
            });
        }
    }

    pub fn node_settings(&self, node: &str) -> Result<NodeSettingsView> {
        let id = self.resolve_node(node)?;
        let online = self.nodes.get(&id).is_some_and(|n| n.online);
        Ok(NodeSettingsView {
            name: self.node_name(&id),
            online,
            report: self.settings.reports.get(&id).cloned(),
            id,
        })
    }

    /// Sends new settings to a node's agent; the receiver gets its answer.
    pub(crate) fn request_settings(
        &mut self,
        node: &str,
        settings: NodeSettings,
    ) -> Result<(String, SettingsReply)> {
        settings.validate()?;
        let id = self.resolve_node(node)?;
        let name = self.node_name(&id);
        let rt = self
            .nodes
            .get(&id)
            .filter(|n| n.online)
            .with_context(|| format!("{name} is offline; its settings change when it is online"))?;
        ensure!(
            self.node_minor(&id) >= SETTINGS_MINOR,
            "{name} runs an agent older than 1.7, which cannot change its settings from here; on that machine run `pumasi service set agent -- {}`",
            settings.changes().join(" ")
        );
        if let Some(r) = self.settings.reports.get(&id)
            && r.file.is_none()
        {
            bail!(
                "{name}'s agent was started by hand with its options on the command line, so it cannot keep new ones; install it as a service (pumasi service install agent) or restart it with: {}",
                settings.changes().join(" ")
            );
        }
        let tx = rt.tx.as_ref().context("node offline")?;
        tx.send(ToAgent::Settings(Box::new(settings)))
            .map_err(|_| anyhow::anyhow!("{name} went offline"))?;
        let (send, recv) = oneshot::channel();
        self.settings
            .waiters
            .entry(id.clone())
            .or_default()
            .push(send);
        Ok((id, recv))
    }

    /// Forgets a node's settings when it is removed.
    pub(crate) fn forget_settings(&mut self, id: &str) {
        self.settings.reports.remove(id);
        self.settings.waiters.remove(id);
    }
}
