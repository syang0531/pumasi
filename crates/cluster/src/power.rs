//! Machines that sleep (1.5): nodes that say they are about to sleep,
//! Wake-on-LAN, and `--on-pause requeue`.
//!
//! A node that announces its sleep ([`FromAgent::Sleeping`]) has its runs
//! moved back to the queue at once, without counting against their
//! attempts, and shows as asleep: no `node.offline` event and no
//! node-offline alert. Its report ([`PowerReport`]) is kept in `meta`, so
//! the control plane still knows its network cards while it sleeps (and
//! after its own restart).
//!
//! When queued work can run on no online node, or has waited longer than
//! `--wake-after` for a busy one, the control plane wakes a sleeping node
//! that opted in and would take it: it sends the magic packet itself when
//! it is on the node's LAN, and asks online agents there to send it.
//!
//! [`FromAgent::Sleeping`]: pumasi_proto::msg::FromAgent::Sleeping

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    POWER_MINOR,
    manage::LabelSet,
    monitor::EventLevel,
    msg::{Role, TaskRef, ToAgent},
    power::{LanIface, NodePowerView, OnPause, PowerReport, SystemWake, WakeSender, format_mac},
};
use tracing::{info, warn};

use crate::state::{Refusal, State, now_unix};

const POWER_META: &str = "node_power";
/// The shortest time between two magic packets the cluster sends a node
/// on its own; doubled after each one that did not wake it.
const WAKE_EVERY: Duration = Duration::from_secs(60);
const WAKE_EVERY_MAX: Duration = Duration::from_secs(900);
/// Packets after which the cluster says the node does not wake.
const WAKE_WARN_AFTER: u32 = 3;
/// Queued runs looked at per tick.
const WAKE_SCAN: usize = 256;
/// Agents asked to send one node's packet.
const WAKE_SENDERS: usize = 2;

#[derive(Default)]
pub(crate) struct PowerRecord {
    pub report: Option<PowerReport>,
    pub asleep_since: Option<u64>,
    /// The minor of its current (or last) session.
    pub minor: u32,
    /// The last magic packet: when, and through which machine.
    pub last_wake: Option<(Instant, u64, String)>,
    /// Packets the cluster sent on its own since the node fell asleep.
    pub wake_tries: u32,
}

pub(crate) struct PowerState {
    pub nodes: HashMap<String, PowerRecord>,
    /// Wake a sleeping node for work that has waited this long for a busy
    /// online node (`--wake-after`). `None`: only for work no online node
    /// can run.
    pub wake_after: Option<Duration>,
    /// Sends magic packets from the control plane's machine.
    pub sender: Arc<dyn WakeSender>,
}

impl Default for PowerState {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            wake_after: None,
            sender: Arc::new(SystemWake),
        }
    }
}

type Saved = Vec<(String, Option<PowerReport>, Option<u64>)>;

impl State {
    pub(crate) fn load_power(&self) -> Result<HashMap<String, PowerRecord>> {
        let Some(b) = self.store.get_meta(POWER_META)? else {
            return Ok(HashMap::new());
        };
        let rows: Saved = postcard::from_bytes(&b).context("corrupt node power records")?;
        Ok(rows
            .into_iter()
            .map(|(id, report, asleep_since)| {
                (
                    id,
                    PowerRecord {
                        report,
                        asleep_since,
                        ..Default::default()
                    },
                )
            })
            .collect())
    }

    fn save_power(&self) {
        let mut rows: Saved = self
            .power
            .nodes
            .iter()
            .filter(|(_, r)| r.report.is_some() || r.asleep_since.is_some())
            .map(|(id, r)| (id.clone(), r.report.clone(), r.asleep_since))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let res = if rows.is_empty() {
            self.store.delete_meta(POWER_META)
        } else {
            self.store.put_meta(
                POWER_META,
                &postcard::to_stdvec(&rows).expect("power encodes"),
            )
        };
        if let Err(e) = res {
            warn!("persist node power records: {e:#}");
        }
    }

    pub(crate) fn forget_power(&mut self, id: &str) {
        if self.power.nodes.remove(id).is_some() {
            self.save_power();
        }
    }

    /// Whether the node said it sleeps (and has not come back).
    pub fn asleep(&self, id: &str) -> bool {
        self.power
            .nodes
            .get(id)
            .is_some_and(|r| r.asleep_since.is_some())
    }

    /// A node connected: it is awake.
    pub(crate) fn power_on_connect(&mut self, id: &str) {
        let Some(r) = self.power.nodes.get_mut(id) else {
            return;
        };
        r.minor = 0;
        r.wake_tries = 0;
        if r.asleep_since.take().is_some() {
            self.save_power();
        }
    }

    fn current(&self, id: &str, generation: u64) -> bool {
        self.nodes
            .get(id)
            .is_some_and(|n| n.generation == generation && n.online)
    }

    /// What the agent's hello said it knows beyond the protocol version.
    pub fn on_minor(&mut self, id: &str, generation: u64, minor: u32) {
        if self.current(id, generation) {
            self.power.nodes.entry(id.to_string()).or_default().minor = minor;
        }
    }

    /// The node's report of how it sleeps and wakes.
    pub fn on_power(&mut self, id: &str, generation: u64, mut report: PowerReport) {
        if !self.current(id, generation) {
            return;
        }
        report.lan.truncate(16);
        report.notes.truncate(8);
        let r = self.power.nodes.entry(id.to_string()).or_default();
        if r.report.as_ref() != Some(&report) {
            r.report = Some(report);
            self.save_power();
        }
    }

    /// The node's machine is about to sleep: its runs go back to the
    /// queue now (not counted against their attempts), and it is asleep
    /// rather than offline.
    pub fn on_sleeping(&mut self, id: &str, generation: u64) {
        if !self.current(id, generation) {
            return;
        }
        // The agent stopped its checkpointing runs before it said so.
        let moved = self.move_runs(id, "node went to sleep", false);
        let r = self.power.nodes.entry(id.to_string()).or_default();
        r.asleep_since = Some(now_unix());
        r.wake_tries = 0;
        r.last_wake = None;
        self.save_power();
        let name = self.node_name(id);
        info!(node = %id, moved, "node went to sleep");
        let msg = match moved {
            0 => format!("node {name} went to sleep"),
            n => format!("node {name} went to sleep; {n} runs moved back to the queue"),
        };
        self.event(EventLevel::Info, "node.asleep", Some(id), None, msg);
        self.on_disconnect(id, generation);
    }

    /// Whether the node's owner asked for its runs to move when it pauses.
    pub(crate) fn requeues_on_pause(&self, id: &str) -> bool {
        self.power
            .nodes
            .get(id)
            .and_then(|r| r.report.as_ref())
            .is_some_and(|r| r.on_pause == OnPause::Requeue)
    }

    /// Stops the node's runs and puts them back in the queue, without
    /// counting them against their jobs. Returns how many. With
    /// `graceful`, runs that keep checkpoints save them first (1.9).
    pub(crate) fn move_runs(&mut self, id: &str, reason: &str, graceful: bool) -> u32 {
        let running: Vec<TaskRef> = self
            .nodes
            .get(id)
            .map(|n| n.running.keys().copied().collect())
            .unwrap_or_default();
        let mut moved = 0;
        for t in running {
            if self.evict_with(id, t, reason, graceful) {
                moved += 1;
            }
        }
        moved
    }

    /// Online agents that can send a packet on the LAN of `target`, the
    /// owner's machines first.
    fn wake_agents(&self, target: &str, lan: &[LanIface]) -> Vec<String> {
        let mut agents: Vec<(bool, String)> = self
            .nodes
            .iter()
            .filter(|(id, n)| id.as_str() != target && n.online && n.tx.is_some())
            .filter(|(id, _)| {
                self.power.nodes.get(id.as_str()).is_some_and(|r| {
                    r.minor >= POWER_MINOR
                        && r.report.as_ref().is_some_and(|rep| {
                            rep.lan.iter().any(|a| lan.iter().any(|t| a.same_lan(t)))
                        })
                })
            })
            .map(|(id, _)| {
                let owner = self.members.get(id).is_some_and(|m| m.role == Role::Owner);
                (!owner, id.clone())
            })
            .collect();
        agents.sort();
        agents.into_iter().map(|(_, id)| id).collect()
    }

    /// Sends the node's magic packet from every machine of the cluster
    /// that is on its LAN: the control plane's own, and up to two online
    /// agents. Returns what was sent, one line each.
    fn send_wake(&mut self, id: &str) -> Result<Vec<String>> {
        let name = self.node_name(id);
        let report = self
            .power
            .nodes
            .get(id)
            .and_then(|r| r.report.clone())
            .with_context(|| {
                format!(
                    "node {name} has not reported its network cards (its agent is older than 1.5)"
                )
            })?;
        if !report.wake_on_lan {
            bail!(
                "node {name} did not opt in to Wake-on-LAN: on that machine, `pumasi service set -- --wake-on-lan` (or start its agent with --wake-on-lan)"
            );
        }
        if report.lan.is_empty() {
            bail!("node {name} reported no LAN network card");
        }
        let macs: Vec<_> = {
            let mut m: Vec<_> = report.lan.iter().map(|i| i.mac).collect();
            m.dedup();
            m
        };
        let mut lines = Vec::new();
        let mut by = Vec::new();
        // The control plane's own machine.
        let sender = self.power.sender.clone();
        for from in sender.lan() {
            for t in report.lan.iter().filter(|t| from.same_lan(t)) {
                match sender.send(&t.mac, &from) {
                    Ok(()) => {
                        lines.push(format!(
                            "sent the magic packet for {} from the control plane ({})",
                            format_mac(&t.mac),
                            from.cidr()
                        ));
                        by.push("the control plane".to_string());
                    }
                    Err(e) => warn!(node = %id, "magic packet from {}: {e:#}", from.cidr()),
                }
            }
        }
        for agent in self
            .wake_agents(id, &report.lan)
            .into_iter()
            .take(WAKE_SENDERS)
        {
            let Some(tx) = self.nodes.get(&agent).and_then(|n| n.tx.as_ref()) else {
                continue;
            };
            if tx.send(ToAgent::Wake { macs: macs.clone() }).is_ok() {
                let agent_name = self.node_name(&agent);
                lines.push(format!(
                    "asked node {agent_name} to send the magic packet for {}",
                    macs.iter().map(format_mac).collect::<Vec<_>>().join(", ")
                ));
                by.push(agent_name);
            }
        }
        if lines.is_empty() {
            let lans: Vec<String> = report.lan.iter().map(|i| i.cidr()).collect();
            bail!(
                "no machine of the cluster is online on node {name}'s LAN ({}): the magic packet does not cross routers",
                lans.join(", ")
            );
        }
        by.dedup();
        let by = by.join(", ");
        if let Some(r) = self.power.nodes.get_mut(id) {
            r.last_wake = Some((Instant::now(), now_unix(), by.clone()));
        }
        info!(node = %id, %by, "magic packet sent");
        self.event(
            EventLevel::Info,
            "node.wake",
            Some(id),
            None,
            format!("magic packet for node {name} sent by {by}"),
        );
        Ok(lines)
    }

    /// `pumasi node wake NODE`.
    pub fn wake_node(&mut self, node: &str) -> Result<Vec<String>> {
        let id = self.resolve_node(node)?;
        if self.nodes.get(&id).is_some_and(|n| n.online) {
            bail!("node {} is online", self.node_name(&id));
        }
        self.send_wake(&id)
    }

    /// Wakes sleeping nodes for queued runs that need them.
    pub(crate) fn tick_wake(&mut self) {
        let asleep: Vec<String> = self
            .power
            .nodes
            .iter()
            .filter(|(id, r)| {
                r.asleep_since.is_some()
                    && r.report.as_ref().is_some_and(|rep| rep.wake_on_lan)
                    && self.nodes.get(id.as_str()).is_some_and(|n| !n.online)
            })
            .map(|(id, _)| id.clone())
            .collect();
        if asleep.is_empty() || self.queue.is_empty() {
            return;
        }
        let labels: HashMap<String, LabelSet> = self
            .nodes
            .keys()
            .map(|id| (id.clone(), self.node_labels(id)))
            .collect();
        let usage = self.usage();
        let items: Vec<_> = self.queue.iter().take(WAKE_SCAN).copied().collect();
        let mut claimed: HashMap<String, u32> = HashMap::new();
        let mut wake = BTreeSet::new();
        for item in items {
            let Some(need) = self.need_for(&item) else {
                continue;
            };
            if !self.within_quota(&need, &usage) {
                continue;
            }
            // What the online nodes say: one takes it now (the scheduler
            // places it), one is only busy (wait `--wake-after`), or none
            // ever would.
            let mut busy = false;
            let mut ready = false;
            for (id, n) in self.nodes.iter().filter(|(_, n)| n.online) {
                match self.refusal(id, n, &need, &labels, &crate::sched::Room::default()) {
                    None => ready = true,
                    Some(
                        Refusal::Full
                        | Refusal::Gpu
                        | Refusal::GpuCheck
                        | Refusal::Benchmarking
                        | Refusal::Upgrading,
                    ) => busy = true,
                    Some(_) => {}
                }
            }
            if ready
                || busy
                    && !self
                        .power
                        .wake_after
                        .is_some_and(|d| item.since.elapsed() >= d)
            {
                continue;
            }
            let mut fits: Vec<&String> = asleep
                .iter()
                .filter(|id| {
                    let n = &self.nodes[id.as_str()];
                    claimed.get(id.as_str()).copied().unwrap_or(0) < n.info.slots
                        && self
                            .refusal_as(id, n, &need, &labels, &crate::sched::Room::default(), true)
                            .is_none()
                })
                .collect();
            // The consumer's own machines first.
            fits.sort_by_key(|id| {
                (
                    self.nodes[id.as_str()].account != need.consumer,
                    (*id).clone(),
                )
            });
            if let Some(id) = fits.first() {
                *claimed.entry((*id).clone()).or_default() += 1;
                wake.insert((*id).clone());
            }
        }
        for id in wake {
            let (due, tries) = {
                let r = &self.power.nodes[&id];
                let every = WAKE_EVERY
                    .saturating_mul(1 << r.wake_tries.min(8))
                    .min(WAKE_EVERY_MAX);
                (
                    r.last_wake
                        .as_ref()
                        .is_none_or(|(at, ..)| at.elapsed() >= every),
                    r.wake_tries,
                )
            };
            if !due {
                continue;
            }
            match self.send_wake(&id) {
                Ok(_) => {
                    if let Some(r) = self.power.nodes.get_mut(&id) {
                        r.wake_tries = tries + 1;
                    }
                    if tries + 1 == WAKE_WARN_AFTER {
                        let name = self.node_name(&id);
                        self.event(
                            EventLevel::Warning,
                            "node.wake_failed",
                            Some(&id),
                            None,
                            format!(
                                "node {name} has not woken after {WAKE_WARN_AFTER} magic packets; check Wake-on-LAN in its firmware and OS settings"
                            ),
                        );
                    }
                }
                Err(e) => {
                    // Nobody on its LAN right now: look again later.
                    if let Some(r) = self.power.nodes.get_mut(&id) {
                        r.last_wake = Some((Instant::now(), now_unix(), String::new()));
                    }
                    info!(node = %id, "cannot wake: {e:#}");
                }
            }
        }
    }

    /// Every node's sleep and wake state.
    pub fn node_power(&self) -> Vec<NodePowerView> {
        let cp_lan = self.power.sender.lan();
        let mut v: Vec<NodePowerView> = self
            .members
            .keys()
            .map(|id| {
                let r = self.power.nodes.get(id);
                let report = r.and_then(|r| r.report.clone());
                let can_wake = report.as_ref().is_some_and(|rep| {
                    rep.wake_on_lan
                        && (cp_lan.iter().any(|c| rep.lan.iter().any(|t| c.same_lan(t)))
                            || !self.wake_agents(id, &rep.lan).is_empty())
                });
                let last = r
                    .and_then(|r| r.last_wake.as_ref())
                    .filter(|(_, _, by)| !by.is_empty());
                NodePowerView {
                    id: id.clone(),
                    asleep_since_unix: r.and_then(|r| r.asleep_since),
                    report,
                    last_wake_unix: last.map(|(_, unix, _)| *unix),
                    last_wake_by: last.map(|(_, _, by)| by.clone()),
                    can_wake,
                }
            })
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    /// The executor each task of a job runs (or ran) with.
    pub fn task_executors(
        &self,
        id: pumasi_proto::job::JobId,
    ) -> Option<Vec<Option<pumasi_proto::job::Executor>>> {
        let job = self.jobs.get(&id)?;
        Some(
            job.tasks
                .iter()
                .map(|t| match job.meta.executor {
                    pumasi_proto::job::Executor::Image => t.ran,
                    e => Some(e),
                })
                .collect(),
        )
    }
}
