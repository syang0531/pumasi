//! Scheduling beyond priorities and selectors (1.9): gangs, spreading and
//! affinity between jobs, CPU and memory quotas (also on the nodes a label
//! picks), and checkpoints. Extends [`State`].
//!
//! Checkpoints: a run that has to move (preemption, drain, `--on-pause
//! requeue`) is told to stop ([`ToAgent::Stop`]) instead of cancelled. It
//! keeps its slot and GPUs (`NodeRuntime::stopping`) until the agent
//! answers with [`pumasi_proto::msg::FromAgent::Stopped`] and the
//! checkpoint it pushed to the file store, or until its grace period and
//! [`STOP_MARGIN`] pass; then it goes back to the queue, and its next run
//! gets the checkpoint back ([`CheckpointTask::restore`]).

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::Result;
use pumasi_proto::{
    economy::vcpus,
    job::{Executor, JobId},
    manage::{LabelSet, Selector},
    monitor::EventLevel,
    msg::{CheckpointTask, TaskRef, TaskState, ToAgent},
    sched::{Domain, PlacementView, ResourceQuota, ResourceUse, ResourceView, Spread},
    store::{BlobHash, Mount, StoredOutput, format_size},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{Need, NodeRuntime, QueueItem, Refusal, State, now_unix};

/// How long past its grace period the control plane waits for a stopping
/// run's checkpoint (the agent pushes it to the file store first).
pub const STOP_MARGIN: Duration = Duration::from_secs(300);

/// A task's latest checkpoint, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub root: BlobHash,
    pub size: u64,
    pub files: u32,
    pub saved_unix: u64,
    /// The node that wrote it (id).
    pub node: String,
    /// The run that wrote it.
    pub attempt: u32,
    /// Runs that started from a checkpoint.
    pub resumed: u32,
}

#[derive(Default)]
pub(crate) struct SchedState {
    /// CPU and memory quotas, by account.
    pub resources: HashMap<String, ResourceQuota>,
    pub checkpoints: HashMap<(JobId, u32), CheckpointRecord>,
    /// Why jobs whose placement holds them wait, from the last pass.
    pub waiting: HashMap<JobId, String>,
}

/// What a node would have if some of its runs stopped (`freed_*`) and runs
/// planned before were on it (`taken_*`).
#[derive(Debug, Clone, Default)]
pub(crate) struct Room {
    pub freed_slots: u32,
    pub taken_slots: u32,
    pub freed_gpus: Vec<u32>,
    pub taken_gpus: Vec<u32>,
}

impl Room {
    /// The node's busy GPUs with this room's changes.
    pub fn busy_gpus(&self, n: &NodeRuntime) -> HashSet<u32> {
        let mut busy = n.gpu.busy_set();
        for g in &self.freed_gpus {
            busy.remove(g);
        }
        busy.extend(self.taken_gpus.iter().copied());
        busy
    }
}

/// What runs hold, by account and by (node, account).
#[derive(Debug, Clone, Default)]
pub(crate) struct Usage {
    pub accounts: HashMap<String, ResourceUse>,
    pub nodes: HashMap<(String, String), ResourceUse>,
}

impl Usage {
    pub fn account(&self, account: &str) -> ResourceUse {
        self.accounts.get(account).copied().unwrap_or_default()
    }

    /// A run of `need` placed on `node` with `gpus` GPUs.
    pub fn add(&mut self, need: &Need, node: &str, gpus: u32) {
        let u = use_of(need, gpus);
        self.accounts
            .entry(need.consumer.clone())
            .or_default()
            .add(&u);
        self.nodes
            .entry((node.to_string(), need.consumer.clone()))
            .or_default()
            .add(&u);
    }
}

/// What one run of `need` holds; `gpus` 0 counts what it asks for.
pub(crate) fn use_of(need: &Need, gpus: u32) -> ResourceUse {
    ResourceUse {
        runs: 1,
        cpus: need.cpus,
        memory_bytes: need.memory,
        gpus: if gpus > 0 {
            gpus
        } else {
            need.gpu.as_ref().map_or(0, |g| g.count.max(1))
        },
    }
}

/// Where a job's runs and the runs of the jobs it names are.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlaceNeed {
    pub spread: Option<Spread>,
    /// The job's runs in each spread group.
    pub counts: HashMap<String, u32>,
    pub by: Domain,
    /// Each job of `near` and the groups its runs are in.
    pub near: Vec<(String, HashSet<String>)>,
    pub away: Vec<(String, HashSet<String>)>,
}

fn label_key(d: &Domain) -> &str {
    match d {
        Domain::Node => "name",
        Domain::Label(k) => k,
    }
}

impl PlaceNeed {
    /// Why the job's placement keeps it off node `id`.
    pub fn refusal<'a>(
        &'a self,
        id: &str,
        labels: &HashMap<String, LabelSet>,
    ) -> Option<Refusal<'a>> {
        let empty = LabelSet::new();
        let l = labels.get(id).unwrap_or(&empty);
        if let Some(sp) = &self.spread {
            let Some(g) = sp.by.of(id, l) else {
                return Some(Refusal::NoLabel(label_key(&sp.by)));
            };
            if let Some(max) = sp.max
                && self.counts.get(&g).copied().unwrap_or(0) >= max
            {
                return Some(Refusal::Spread(max, &sp.by));
            }
        }
        if !self.near.is_empty() || !self.away.is_empty() {
            let Some(g) = self.by.of(id, l) else {
                return Some(Refusal::NoLabel(label_key(&self.by)));
            };
            if let Some((name, _)) = self.near.iter().find(|(_, gs)| !gs.contains(&g)) {
                return Some(Refusal::NotNear(name));
            }
            if let Some((name, _)) = self.away.iter().find(|(_, gs)| gs.contains(&g)) {
                return Some(Refusal::Away(name));
            }
        }
        None
    }

    /// The job's runs in node `id`'s spread group (for preferring the
    /// emptiest).
    pub fn spread_count(&self, id: &str, labels: &HashMap<String, LabelSet>) -> u32 {
        let Some(sp) = &self.spread else {
            return 0;
        };
        let empty = LabelSet::new();
        sp.by
            .of(id, labels.get(id).unwrap_or(&empty))
            .and_then(|g| self.counts.get(&g).copied())
            .unwrap_or(0)
    }

    /// A run of the job planned on node `id`.
    pub fn note(&mut self, id: &str, labels: &HashMap<String, LabelSet>) {
        if let Some(sp) = &self.spread {
            let empty = LabelSet::new();
            if let Some(g) = sp.by.of(id, labels.get(id).unwrap_or(&empty)) {
                *self.counts.entry(g).or_default() += 1;
            }
        }
    }
}

/// Runs to stop, with their nodes.
pub(crate) type Victims = Vec<(String, TaskRef)>;

/// What [`State::preempt_for`] found.
pub(crate) enum Preempt {
    /// Room on this node now.
    Room(String),
    /// Room soon: runs are stopping to make it.
    Wait,
    None,
}

/// What [`State::place_gang`] did.
pub(crate) enum GangOutcome {
    Placed(Vec<(JobId, u32)>),
    Wait(String),
    /// None of the runs is waiting any more.
    Stale,
}

impl State {
    pub(crate) fn load_sched(
        &mut self,
        checkpoints: Vec<((u128, u32), CheckpointRecord)>,
        resources: Vec<(String, ResourceQuota)>,
    ) {
        self.sched.checkpoints = checkpoints
            .into_iter()
            .map(|((j, i), r)| ((JobId(j), i), r))
            .collect();
        self.sched.resources = resources.into_iter().collect();
    }

    /// The minor the node's agent said it speaks (0 before 1.5).
    pub(crate) fn agent_minor(&self, id: &str) -> u32 {
        self.power.nodes.get(id).map_or(0, |r| r.minor)
    }

    pub(crate) fn is_gang(&self, job: JobId) -> bool {
        self.jobs.get(&job).is_some_and(|j| j.meta.place.gang)
    }

    /// What runs hold now, by account and node.
    pub(crate) fn usage(&self) -> Usage {
        let mut u = Usage::default();
        for (id, n) in &self.nodes {
            for t in n.running.keys() {
                let Some(j) = self.jobs.get(&t.job) else {
                    continue;
                };
                let r = ResourceUse {
                    runs: 1,
                    cpus: match j.meta.executor {
                        Executor::Wasm => 1,
                        _ => vcpus(j.meta.limits.cpu_millis),
                    },
                    memory_bytes: j.meta.limits.memory_bytes,
                    gpus: n.gpu.busy.get(t).map_or(0, |g| g.len() as u32),
                };
                u.accounts
                    .entry(j.meta.account.clone())
                    .or_default()
                    .add(&r);
                u.nodes
                    .entry((id.clone(), j.meta.account.clone()))
                    .or_default()
                    .add(&r);
            }
        }
        u
    }

    /// Where the job's runs and the runs of the jobs it names are, for a
    /// job whose placement says where its runs go.
    pub(crate) fn place_need(&self, job: JobId) -> Option<PlaceNeed> {
        let j = self.jobs.get(&job)?;
        let p = &j.meta.place;
        if !p.constrains_nodes() {
            return None;
        }
        let account = &j.meta.account;
        let mut labels: HashMap<&str, LabelSet> = HashMap::new();
        let mut out = PlaceNeed {
            spread: p.spread.clone(),
            by: p.by.clone(),
            near: p.near.iter().map(|n| (n.clone(), HashSet::new())).collect(),
            away: p.away.iter().map(|n| (n.clone(), HashSet::new())).collect(),
            ..Default::default()
        };
        for (id, n) in &self.nodes {
            for t in n.running.keys().filter(|t| !t.check) {
                let l = labels
                    .entry(id.as_str())
                    .or_insert_with(|| self.node_labels(id));
                if t.job == job {
                    if let Some(g) = p.spread.as_ref().and_then(|sp| sp.by.of(id, l)) {
                        *out.counts.entry(g).or_default() += 1;
                    }
                    continue;
                }
                let Some(other) = self.jobs.get(&t.job) else {
                    continue;
                };
                if other.meta.account != *account {
                    continue;
                }
                let names =
                    |want: &str| other.meta.name == want || other.meta.id.to_string() == want;
                let Some(g) = p.by.of(id, l) else {
                    continue;
                };
                for (want, groups) in out.near.iter_mut().chain(out.away.iter_mut()) {
                    if names(want) {
                        groups.insert(g.clone());
                    }
                }
            }
        }
        Some(out)
    }

    /// A quota per node label of the run's account that node `id` is over.
    pub(crate) fn label_quota_refusal<'a>(
        &'a self,
        id: &str,
        need: &Need,
        labels: &HashMap<String, LabelSet>,
        usage: &Usage,
    ) -> Option<&'a Selector> {
        let q = self.sched.resources.get(&need.consumer)?;
        if q.labels.is_empty() {
            return None;
        }
        let empty = LabelSet::new();
        let here = labels.get(id).unwrap_or(&empty);
        let more = use_of(need, 0);
        for lq in &q.labels {
            if !lq.selector.matches(here) {
                continue;
            }
            let mut used = ResourceUse::default();
            for ((node, account), u) in &usage.nodes {
                if *account == need.consumer
                    && labels.get(node).is_some_and(|l| lq.selector.matches(l))
                {
                    used.add(u);
                }
            }
            if lq.over(&used, &more).is_some() {
                return Some(&lq.selector);
            }
        }
        None
    }

    /// The node that takes the run now, with `rooms`' changes: own (or for
    /// check runs, trusted) machines first, then the least used spread
    /// group, then the most free slots.
    pub(crate) fn best_node(
        &self,
        need: &Need,
        labels: &HashMap<String, LabelSet>,
        usage: &Usage,
        rooms: &HashMap<String, Room>,
    ) -> Option<String> {
        let none = Room::default();
        self.nodes
            .iter()
            .filter(|(id, n)| {
                let room = rooms.get(id.as_str()).unwrap_or(&none);
                self.fits(id, n, need, labels, room)
                    && self.label_quota_refusal(id, need, labels, usage).is_none()
            })
            .max_by_key(|(id, n)| {
                let room = rooms.get(id.as_str()).unwrap_or(&none);
                let spread = need
                    .place
                    .as_ref()
                    .map_or(0, |p| p.spread_count(id, labels));
                let pref = if need.item.check {
                    n.trusted
                } else {
                    n.account == need.consumer
                };
                let free = (n.free_slots() + room.freed_slots).saturating_sub(room.taken_slots);
                (Reverse(spread), pref, free)
            })
            .map(|(id, _)| id.clone())
    }

    /// Which runs of lower priority to stop for `need`, on which node:
    /// the fewest, lowest-priority ones, newest first among equals. Runs
    /// already stopping count as gone: with only those, the victims are
    /// none. A victim in a gang brings its running gang mates. `chosen`
    /// are victims planned already (for other runs of a gang).
    pub(crate) fn plan_preempt(
        &self,
        need: &Need,
        labels: &HashMap<String, LabelSet>,
        usage: &Usage,
        rooms: &HashMap<String, Room>,
        chosen: &HashSet<TaskRef>,
    ) -> Option<(String, Victims)> {
        let mut best: Option<((i32, usize), String, Victims)> = None;
        for (id, n) in &self.nodes {
            if self.label_quota_refusal(id, need, labels, usage).is_some() {
                continue;
            }
            let mut room = rooms.get(id).cloned().unwrap_or_default();
            // Runs stopping now, or already chosen for the gang, free
            // their room without stopping anything more.
            for t in n.running.keys() {
                if n.stopping.contains_key(t) || chosen.contains(t) {
                    room.freed_slots += 1;
                    room.freed_gpus
                        .extend(n.gpu.busy.get(t).cloned().unwrap_or_default());
                }
            }
            if self.fits(id, n, need, labels, &room) {
                let score = (i32::MIN, 0);
                if best.as_ref().is_none_or(|(s, _, _)| score < *s) {
                    best = Some((score, id.clone(), Vec::new()));
                }
                continue;
            }
            let mut cands: Vec<(i32, Instant, TaskRef)> = n
                .running
                .iter()
                .filter(|(t, _)| !n.stopping.contains_key(*t) && !chosen.contains(*t))
                .map(|(t, started)| (self.priority_of(t.job), *started, *t))
                .filter(|(p, _, _)| *p < need.item.priority)
                .collect();
            if cands.is_empty() {
                continue;
            }
            cands.sort_by_key(|(p, started, _)| (*p, Reverse(*started)));
            let mut victims: Vec<(String, TaskRef)> = Vec::new();
            let mut worst = i32::MIN;
            for (p, _, t) in cands {
                if victims.iter().any(|(_, v)| *v == t) {
                    continue;
                }
                worst = worst.max(p);
                for (node, v) in std::iter::once((id.clone(), t)).chain(self.gang_mates(t)) {
                    if victims.iter().any(|(_, x)| *x == v) {
                        continue;
                    }
                    if node == *id {
                        room.freed_slots += 1;
                        room.freed_gpus
                            .extend(n.gpu.busy.get(&v).cloned().unwrap_or_default());
                    }
                    victims.push((node, v));
                }
                if self.fits(id, n, need, labels, &room) {
                    let score = (worst, victims.len());
                    if best.as_ref().is_none_or(|(s, _, _)| score < *s) {
                        best = Some((score, id.clone(), victims.clone()));
                    }
                    break;
                }
            }
        }
        best.map(|(_, id, v)| (id, v))
    }

    /// The other running runs of `t`'s job when it is a gang, with their
    /// nodes.
    fn gang_mates(&self, t: TaskRef) -> Vec<(String, TaskRef)> {
        if t.check || !self.is_gang(t.job) {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (id, n) in &self.nodes {
            for m in n.running.keys() {
                if m.job == t.job && !m.check && *m != t && !n.stopping.contains_key(m) {
                    out.push((id.clone(), *m));
                }
            }
        }
        out
    }

    /// Places every queued run of a gang at once, or none: on free room
    /// first, then by planning which lower-priority runs to stop. When
    /// that takes runs that stop with a grace period (or runs already
    /// stopping), the gang waits for them.
    pub(crate) fn place_gang(
        &mut self,
        items: &[QueueItem],
        labels: &HashMap<String, LabelSet>,
        usage: &mut Usage,
    ) -> GangOutcome {
        let mut needs: Vec<Need> = items.iter().filter_map(|i| self.need_for(i)).collect();
        if needs.is_empty() {
            return GangOutcome::Stale;
        }
        let total = needs.len();
        let mut place = needs[0].place.take();
        let mut trial = usage.clone();
        let mut rooms: HashMap<String, Room> = HashMap::new();
        let mut chosen: HashSet<TaskRef> = HashSet::new();
        let mut victims: Vec<(String, TaskRef)> = Vec::new();
        let mut plan: Vec<String> = Vec::new();
        let mut waits_for_stops = false;
        for need in needs.iter_mut() {
            need.place = place.clone();
            if !self.within_quota(need, &trial) {
                return GangOutcome::Wait(format!(
                    "account {} has no quota left for all {total} tasks of the gang at once",
                    need.consumer
                ));
            }
            let node = match self.best_node(need, labels, &trial, &rooms) {
                Some(id) => id,
                None => match self.plan_preempt(need, labels, &trial, &rooms, &chosen) {
                    Some((id, vs)) => {
                        if vs.is_empty()
                            || self.nodes.get(&id).is_some_and(|n| !n.stopping.is_empty())
                        {
                            waits_for_stops = true;
                        }
                        // What stopping (or the stops under way) frees on
                        // that node.
                        let n = &self.nodes[&id];
                        let room = rooms.entry(id.clone()).or_default();
                        for t in n.running.keys() {
                            if (n.stopping.contains_key(t) || vs.iter().any(|(_, v)| v == t))
                                && !chosen.contains(t)
                            {
                                room.freed_slots += 1;
                                room.freed_gpus
                                    .extend(n.gpu.busy.get(t).cloned().unwrap_or_default());
                                chosen.insert(*t);
                            }
                        }
                        for (vn, t) in vs {
                            chosen.insert(t);
                            victims.push((vn, t));
                        }
                        id
                    }
                    None => {
                        return GangOutcome::Wait(format!(
                            "only {} of its {total} tasks fit at once",
                            plan.len()
                        ));
                    }
                },
            };
            let n = &self.nodes[&node];
            let room = rooms.entry(node.clone()).or_default();
            let gpus = need
                .gpu
                .as_ref()
                .and_then(|req| {
                    let run_as = self.run_as(&node, n, need).ok()?;
                    self.pick_gpus(&node, req, run_as, &room.busy_gpus(n))
                })
                .unwrap_or_default();
            room.taken_slots += 1;
            room.taken_gpus.extend(gpus.iter().copied());
            trial.add(need, &node, gpus.len() as u32);
            if let Some(p) = place.as_mut() {
                p.note(&node, labels);
            }
            plan.push(node);
        }
        for (node, t) in &victims {
            self.evict(node, *t, "preempted by a higher-priority gang");
            if self
                .nodes
                .get(node)
                .is_some_and(|n| n.running.contains_key(t))
            {
                waits_for_stops = true;
            }
        }
        if waits_for_stops {
            return GangOutcome::Wait(format!(
                "waiting for runs to stop to make room for all {total} tasks"
            ));
        }
        let mut placed = Vec::new();
        for (need, node) in needs.iter().zip(&plan) {
            if let Some(gpus) = self.place(need, node) {
                usage.add(need, node, gpus);
                placed.push((need.item.job, need.item.index));
            }
        }
        if placed.len() < total {
            warn!(job = %needs[0].item.job, placed = placed.len(), total, "a gang was only partly placed");
        } else {
            info!(job = %needs[0].item.job, tasks = total, "gang placed");
        }
        GangOutcome::Placed(placed)
    }

    /// Why the run fits no node now, in a few words per node (for the
    /// placement view).
    pub(crate) fn why_unplaced_now(
        &self,
        need: &Need,
        labels: &HashMap<String, LabelSet>,
    ) -> String {
        let usage = self.usage();
        let mut parts: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.online)
            .map(|(id, n)| {
                let why = match self.refusal(id, n, need, labels, &Room::default()) {
                    Some(r) => r.to_string(),
                    None => match self.label_quota_refusal(id, need, labels, &usage) {
                        Some(s) => Refusal::LabelQuota(s).to_string(),
                        None => "busy".into(),
                    },
                };
                format!("{} ({why})", self.node_name(id))
            })
            .collect();
        parts.sort();
        if parts.is_empty() {
            "no node is online".into()
        } else {
            parts.join(", ")
        }
    }

    // -- checkpoints ------------------------------------------------------------

    /// The grace period run `t` on `node` gets when it has to move: only a
    /// checkpointing run, on an agent that can stop it gently.
    pub(crate) fn stop_grace(&self, node: &str, t: &TaskRef) -> Option<Duration> {
        if t.check {
            return None;
        }
        let c = self.jobs.get(&t.job)?.meta.place.checkpoint.as_ref()?;
        let n = self.nodes.get(node)?;
        if !n.online || n.tx.is_none() || self.agent_minor(node) < pumasi_proto::CHECKPOINT_MINOR {
            return None;
        }
        Some(Duration::from_millis(c.grace_ms))
    }

    /// What a checkpointing run of the task gets: its grace period and its
    /// last checkpoint.
    pub(crate) fn checkpoint_task(&self, job: JobId, index: u32) -> Option<CheckpointTask> {
        let c = self.jobs.get(&job)?.meta.place.checkpoint.as_ref()?;
        let restore = self.sched.checkpoints.get(&(job, index)).map(|r| Mount {
            root: r.root,
            select: None,
            at: String::new(),
            label: format!("checkpoint of task {job}/{index}"),
            size: r.size,
        });
        Some(CheckpointTask {
            grace_ms: c.grace_ms,
            restore,
        })
    }

    /// A run started from the task's checkpoint.
    pub(crate) fn note_resumed(&mut self, job: JobId, index: u32, node: &str) {
        let Some(r) = self.sched.checkpoints.get_mut(&(job, index)) else {
            return;
        };
        r.resumed += 1;
        let size = r.size;
        if let Err(e) = self.store.put_checkpoint(job.0, index, Some(r)) {
            warn!("persist checkpoint: {e:#}");
        }
        let name = self.node_name(node);
        self.event(
            EventLevel::Info,
            "task.resumed",
            Some(node),
            Some(job),
            format!(
                "task {job}/{index} resumes on {name} from its checkpoint ({})",
                format_size(size)
            ),
        );
    }

    fn save_checkpoint(&mut self, t: TaskRef, node: &str, c: StoredOutput) {
        let resumed = self
            .sched
            .checkpoints
            .get(&(t.job, t.index))
            .map_or(0, |r| r.resumed);
        let r = CheckpointRecord {
            root: c.root,
            size: c.size,
            files: c.files,
            saved_unix: now_unix(),
            node: node.to_string(),
            attempt: t.attempt,
            resumed,
        };
        self.store_cmd(crate::datasets::StoreCmd::Tag(
            crate::files::ckpt_tag(t.job.0, t.index),
            c.root,
        ));
        // Its tags go when the job's store work ends.
        self.files.open_jobs.insert(t.job);
        if let Err(e) = self.store.put_checkpoint(t.job.0, t.index, Some(&r)) {
            warn!("persist checkpoint: {e:#}");
        }
        self.sched.checkpoints.insert((t.job, t.index), r);
        let name = self.node_name(node);
        self.event(
            EventLevel::Info,
            "task.checkpoint",
            Some(node),
            Some(t.job),
            format!(
                "task {}/{} saved a checkpoint on {name}: {} files, {}",
                t.job,
                t.index,
                c.files,
                format_size(c.size)
            ),
        );
    }

    /// A run stopped (after [`ToAgent::Stop`], or because its machine is
    /// about to sleep) with its checkpoint, checked complete in the store
    /// by the caller.
    pub fn on_stopped(
        &mut self,
        node_id: &str,
        generation: u64,
        task: TaskRef,
        checkpoint: Option<StoredOutput>,
        error: Option<String>,
    ) {
        let Some(n) = self.nodes.get_mut(node_id) else {
            return;
        };
        if n.generation != generation {
            return;
        }
        let asked = n.stopping.remove(&task).is_some();
        if n.running.remove(&task).is_none() {
            return;
        }
        n.gpu.busy.remove(&task);
        let current = self
            .jobs
            .get(&task.job)
            .and_then(|j| j.tasks.get(task.index as usize))
            .is_some_and(|t| {
                t.attempts == task.attempt && matches!(t.state, TaskState::Running { .. })
            });
        if current && !task.check {
            if let Some(c) = checkpoint {
                self.save_checkpoint(task, node_id, c);
            }
            if let Some(e) = &error {
                let name = self.node_name(node_id);
                warn!(node = %node_id, task = %task, "no checkpoint: {e}");
                self.event(
                    EventLevel::Warning,
                    "task.checkpoint_failed",
                    Some(node_id),
                    Some(task.job),
                    format!(
                        "task {}/{} on {name} stopped without a new checkpoint: {e}",
                        task.job, task.index
                    ),
                );
            }
        }
        if !asked {
            // Its agent stopped it (the machine sleeps): moved, not failed.
            let key = (task.job, task.index, task.check);
            let c = self.mgmt.preempted.entry(key).or_default();
            *c += 1;
            let c = *c;
            if let Err(e) = self
                .store
                .put_preempted((task.job.0, task.index, task.check), c)
            {
                warn!("persist preemption: {e:#}");
            }
        }
        self.requeue_run(task, "stopped to move it");
        self.schedule();
    }

    /// Runs that did not stop within their grace period and the margin:
    /// cancelled, and back in the queue with the checkpoint they had.
    pub(crate) fn tick_stopping(&mut self) {
        let now = Instant::now();
        let late: Vec<(String, TaskRef)> = self
            .nodes
            .iter()
            .flat_map(|(id, n)| {
                n.stopping
                    .iter()
                    .filter(|(_, d)| **d <= now)
                    .map(|(t, _)| (id.clone(), *t))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (id, t) in late {
            if let Some(n) = self.nodes.get_mut(&id) {
                n.stopping.remove(&t);
                n.running.remove(&t);
                n.gpu.busy.remove(&t);
                if let Some(tx) = &n.tx {
                    let _ = tx.send(ToAgent::Cancel(t));
                }
            }
            let name = self.node_name(&id);
            warn!(node = %id, task = %t, "a stopping run did not save its checkpoint in time");
            self.event(
                EventLevel::Warning,
                "task.checkpoint_failed",
                Some(&id),
                Some(t.job),
                format!(
                    "task {}/{} on {name} did not stop and save its checkpoint in time; cancelled",
                    t.job, t.index
                ),
            );
            self.requeue_run(t, "did not stop in time");
        }
    }

    /// When a run of a gang goes back to the queue, its running mates stop
    /// too (gently when they keep checkpoints), so the gang starts again
    /// together.
    pub(crate) fn stop_gang_mates(&mut self, job: JobId, reason: &str) {
        if !self.is_gang(job) {
            return;
        }
        let mates: Vec<(String, TaskRef)> = self
            .nodes
            .iter()
            .flat_map(|(id, n)| {
                n.running
                    .keys()
                    .filter(|t| t.job == job && !t.check && !n.stopping.contains_key(t))
                    .map(|t| (id.clone(), *t))
                    .collect::<Vec<_>>()
            })
            .collect();
        let why = format!("its gang starts again ({reason})");
        for (id, t) in mates {
            self.evict(&id, t, &why);
        }
    }

    // -- quotas -----------------------------------------------------------------

    pub fn set_resources(&mut self, account: &str, q: ResourceQuota) -> Result<()> {
        pumasi_proto::economy::validate_account(account)?;
        q.validate()?;
        if q.is_empty() {
            self.store.put_account_resources(account, None)?;
            self.sched.resources.remove(account);
        } else {
            self.store.put_account_resources(account, Some(&q))?;
            self.sched.resources.insert(account.to_string(), q.clone());
        }
        info!(%account, quota = %q.describe(), "CPU and memory quota set");
        self.schedule();
        Ok(())
    }

    pub fn account_resources(&self, account: &str) -> ResourceQuota {
        self.sched
            .resources
            .get(account)
            .cloned()
            .unwrap_or_default()
    }

    /// Every account's CPU and memory quota and what it holds.
    pub fn resources(&self) -> Vec<ResourceView> {
        let usage = self.usage();
        let labels: HashMap<String, LabelSet> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.online)
            .map(|(id, _)| (id.clone(), self.node_labels(id)))
            .collect();
        let mut names: Vec<&String> = self
            .accounts
            .keys()
            .chain(self.sched.resources.keys())
            .collect();
        names.sort();
        names.dedup();
        names
            .into_iter()
            .map(|a| {
                let quota = self.account_resources(a);
                let labels_used = quota
                    .labels
                    .iter()
                    .map(|lq| {
                        let mut u = ResourceUse::default();
                        for ((node, account), used) in &usage.nodes {
                            if account == a
                                && labels.get(node).is_some_and(|l| lq.selector.matches(l))
                            {
                                u.add(used);
                            }
                        }
                        u
                    })
                    .collect();
                ResourceView {
                    account: a.clone(),
                    used: usage.account(a),
                    quota,
                    labels_used,
                }
            })
            .collect()
    }

    // -- views ------------------------------------------------------------------

    /// A job's placement, its runs stopping now and its tasks' checkpoints.
    pub fn placement_view(&self, id: JobId) -> Option<PlacementView> {
        let job = self.jobs.get(&id)?;
        let mut stopping: Vec<u32> = self
            .nodes
            .values()
            .flat_map(|n| n.stopping.keys())
            .filter(|t| t.job == id && !t.check)
            .map(|t| t.index)
            .collect();
        stopping.sort();
        let mut checkpoints: Vec<_> = self
            .sched
            .checkpoints
            .iter()
            .filter(|((j, _), _)| *j == id)
            .map(|((_, i), r)| pumasi_proto::sched::CheckpointView {
                index: *i,
                size: r.size,
                files: r.files,
                saved_unix: r.saved_unix,
                node: self.node_name(&r.node),
                resumed: r.resumed,
            })
            .collect();
        checkpoints.sort_by_key(|c| c.index);
        Some(PlacementView {
            placement: job.meta.place.clone(),
            stopping,
            checkpoints,
            waiting: self.sched.waiting.get(&id).cloned(),
        })
    }
}
