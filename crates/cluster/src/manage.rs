//! Cluster management (phase 10): node labels, taints, cordon and drain,
//! priorities and preemption, account quotas, job dependencies, access
//! tickets with roles, schedules and `apply`. Extends [`State`].

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{
    Secret,
    economy::{MilliCu, validate_account},
    job::{JobId, JobSubmission},
    manage::{
        AccessRole, AccessView, AccountQuota, ApplySpec, Cron, LabelSet, MAX_PRIORITY, NodeConfig,
        NodeEdit, QuotaView, ScheduleSpec, ScheduleView, Selector, Toleration, access_id,
        validate_access_name, validate_schedule_name,
    },
    msg::{JobState, OWNER_ACCOUNT, Outcome, TaskRef, TaskState, ToAgent},
    secrets_eq,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{QueueItem, State, now_unix};

/// How far back the daily CU quota looks.
const DAY_SECS: u64 = 24 * 3600;

/// A job's scheduling settings (kept in their own table).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSched {
    pub priority: i32,
    pub selector: Selector,
    pub tolerations: Vec<Toleration>,
    pub after: Vec<JobId>,
    /// The schedule that submitted the job.
    pub schedule: Option<String>,
}

impl JobSched {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// An access ticket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessRecord {
    pub secret: Secret,
    pub name: String,
    pub role: AccessRole,
    pub created_unix: u64,
    pub expires_unix: Option<u64>,
}

impl AccessRecord {
    pub fn view(&self) -> AccessView {
        AccessView {
            id: access_id(&self.secret),
            name: self.name.clone(),
            role: self.role.clone(),
            created_unix: self.created_unix,
            expires_unix: self.expires_unix,
        }
    }

    fn live(&self, now: u64) -> bool {
        self.expires_unix.is_none_or(|e| e > now)
    }
}

/// A schedule and how its runs went.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub spec: ScheduleSpec,
    pub next_unix: Option<u64>,
    pub last_unix: Option<u64>,
    pub last_job: Option<JobId>,
    pub last_error: Option<String>,
    pub runs: u64,
}

impl ScheduleRecord {
    fn view(&self) -> ScheduleView {
        ScheduleView {
            name: self.spec.name.clone(),
            cron: self.spec.cron.clone(),
            account: self
                .spec
                .job
                .account
                .clone()
                .unwrap_or_else(|| OWNER_ACCOUNT.into()),
            job_name: self.spec.job.name.clone(),
            tasks: self.spec.job.tasks.len() as u32,
            priority: self.spec.job.priority,
            overlap: self.spec.overlap,
            next_unix: self.next_unix,
            last_unix: self.last_unix,
            last_job: self.last_job,
            last_error: self.last_error.clone(),
            runs: self.runs,
        }
    }
}

/// Cluster management state.
#[derive(Default)]
pub(crate) struct Mgmt {
    pub node_config: HashMap<String, NodeConfig>,
    /// Quotas other than storage (that one is in the file store's state).
    pub quotas: HashMap<String, AccountQuota>,
    /// What each account spent in the last day: (unix, milli-CU).
    pub spend: HashMap<String, VecDeque<(u64, MilliCu)>>,
    /// Placements of a run (job, task, check) that were stopped to make
    /// room and do not count against the job's attempts.
    pub preempted: HashMap<(JobId, u32, bool), u32>,
    /// Jobs whose dependencies have not finished.
    pub waiting: HashSet<JobId>,
    pub access: Vec<AccessRecord>,
    pub schedules: BTreeMap<String, ScheduleRecord>,
    /// What each account's jobs may be (phase 15).
    pub policies: HashMap<String, pumasi_proto::manage::AccountPolicy>,
    /// When jobs with a queue timeout could first run (phase 15).
    pub ready_since: HashMap<JobId, std::time::Instant>,
}

/// Who made an admin call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// The cluster's admin ticket, or an access ticket with the admin role.
    Admin {
        name: String,
    },
    Submitter {
        name: String,
        account: String,
    },
    Viewer {
        name: String,
    },
}

impl Caller {
    pub fn name(&self) -> &str {
        match self {
            Caller::Admin { name } | Caller::Submitter { name, .. } | Caller::Viewer { name } => {
                name
            }
        }
    }

    pub fn role(&self) -> AccessRole {
        match self {
            Caller::Admin { .. } => AccessRole::Admin,
            Caller::Submitter { account, .. } => AccessRole::Submitter {
                account: account.clone(),
            },
            Caller::Viewer { .. } => AccessRole::Viewer,
        }
    }
}

/// Whether a job's dependencies let it run.
enum Deps {
    Ready,
    Waiting(JobId),
    Failed(String),
}

impl State {
    pub(crate) fn load_mgmt(&mut self, snap: MgmtSnapshot) -> Result<()> {
        self.mgmt.node_config = snap.node_config.into_iter().collect();
        self.mgmt.quotas = snap.account_quotas.into_iter().collect();
        self.mgmt.policies = snap.account_policies.into_iter().collect();
        self.mgmt.preempted = snap
            .preempted
            .into_iter()
            .map(|((j, i, c), n)| ((JobId(j), i, c), n))
            .collect();
        self.mgmt.access = snap.access;
        self.mgmt.schedules = snap
            .schedules
            .into_iter()
            .map(|s| (s.spec.name.clone(), s))
            .collect();
        let now = now_unix();
        for (account, unix, amount) in self.store.spent_since(now.saturating_sub(DAY_SECS))? {
            self.mgmt
                .spend
                .entry(account)
                .or_default()
                .push_back((unix, amount));
        }
        // Schedules missed while the control plane was down run once now.
        for s in self.mgmt.schedules.values_mut() {
            if s.next_unix.is_none()
                && let Ok(c) = s.spec.cron.parse::<Cron>()
            {
                s.next_unix = c.next_local(now as i64).map(|t| t as u64);
            }
        }
        // Jobs that wait for others keep their tasks out of the queue.
        let waiting: Vec<JobId> = self
            .jobs
            .values()
            .filter(|j| !j.meta.cancelled && !j.meta.sched.after.is_empty())
            .map(|j| j.meta.id)
            .collect();
        for id in waiting {
            if !matches!(self.deps(id), Deps::Ready) {
                self.queue.retain(|q| q.job != id);
                self.mgmt.waiting.insert(id);
            }
        }
        Ok(())
    }

    // -- the queue -------------------------------------------------------

    pub(crate) fn priority_of(&self, job: JobId) -> i32 {
        self.jobs.get(&job).map_or(0, |j| j.meta.sched.priority)
    }

    /// Puts a run in the queue, which is ordered by priority: at the end
    /// of its priority, or with `front` (retries) at its start.
    pub(crate) fn push_run(&mut self, job: JobId, index: u32, check: bool, front: bool) {
        let item = QueueItem {
            job,
            index,
            check,
            priority: self.priority_of(job),
        };
        let pos = if front {
            self.queue.partition_point(|q| q.priority > item.priority)
        } else {
            self.queue.partition_point(|q| q.priority >= item.priority)
        };
        self.queue.insert(pos, item);
    }

    /// Queues every pending task of a job, after the runs of its priority.
    pub(crate) fn queue_job(&mut self, id: JobId) {
        let Some(job) = self.jobs.get(&id) else {
            return;
        };
        let priority = job.meta.sched.priority;
        let items: Vec<QueueItem> = job
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.state == TaskState::Pending)
            .map(|(i, _)| QueueItem {
                job: id,
                index: i as u32,
                check: false,
                priority,
            })
            .collect();
        let pos = self.queue.partition_point(|q| q.priority >= priority);
        let tail = self.queue.split_off(pos);
        self.queue.extend(items);
        self.queue.extend(tail);
    }

    /// Placements of a run that do not count against its attempts.
    pub(crate) fn free_attempts(&self, job: JobId, index: u32, check: bool) -> u32 {
        self.mgmt
            .preempted
            .get(&(job, index, check))
            .copied()
            .unwrap_or(0)
    }

    /// Stops a run to make room and puts it back in the queue without
    /// counting it against the job. Returns whether it was running there.
    pub(crate) fn evict(&mut self, node_id: &str, t: TaskRef, reason: &str) -> bool {
        let Some(n) = self.nodes.get_mut(node_id) else {
            return false;
        };
        if n.running.remove(&t).is_none() {
            return false;
        }
        n.gpu.busy.remove(&t);
        if let Some(tx) = &n.tx {
            let _ = tx.send(ToAgent::Cancel(t));
        }
        let key = (t.job, t.index, t.check);
        let count = {
            let c = self.mgmt.preempted.entry(key).or_default();
            *c += 1;
            *c
        };
        if let Err(e) = self.store.put_preempted((t.job.0, t.index, t.check), count) {
            warn!("persist preemption: {e:#}");
        }
        info!(node = %node_id, task = %t, "{reason}; run moved back to the queue");
        self.monitor.counters.preemptions += 1;
        let name = self.node_name(node_id);
        self.event(
            pumasi_proto::monitor::EventLevel::Info,
            "task.preempted",
            Some(node_id),
            Some(t.job),
            format!("task {}/{} on {name}: {reason}", t.job, t.index),
        );
        self.requeue_run(t, reason);
        true
    }

    /// Frees room for a queued run by stopping runs of lower priority on
    /// one node: the node that needs the fewest, lowest-priority runs
    /// stopped. Returns that node.
    pub(crate) fn preempt_for(
        &mut self,
        need: &crate::state::Need,
        labels: &HashMap<String, LabelSet>,
    ) -> Option<String> {
        let mut best: Option<((i32, usize), String, Vec<TaskRef>)> = None;
        for (id, n) in &self.nodes {
            let mut cands: Vec<(i32, std::time::Instant, TaskRef)> = n
                .running
                .iter()
                .map(|(t, started)| (self.priority_of(t.job), *started, *t))
                .filter(|(p, _, _)| *p < need.item.priority)
                .collect();
            if cands.is_empty() {
                continue;
            }
            // Lowest priority first, and among those the newest (least
            // work lost).
            cands.sort_by_key(|(p, started, _)| (*p, Reverse(*started)));
            let mut victims = Vec::new();
            let mut freed: Vec<u32> = Vec::new();
            let mut worst = i32::MIN;
            for (p, _, t) in cands {
                victims.push(t);
                worst = worst.max(p);
                freed.extend(n.gpu.busy.get(&t).cloned().unwrap_or_default());
                if self.fits(id, n, need, labels, victims.len() as u32, &freed) {
                    let score = (worst, victims.len());
                    if best.as_ref().is_none_or(|(s, _, _)| score < *s) {
                        best = Some((score, id.clone(), victims.clone()));
                    }
                    break;
                }
            }
        }
        let (_, id, victims) = best?;
        for t in victims {
            self.evict(&id, t, "preempted by a higher-priority job");
        }
        Some(id)
    }

    /// Whether any run in the cluster has a lower priority than `p`.
    pub(crate) fn lower_priority_running(&self, p: i32) -> bool {
        self.nodes
            .values()
            .flat_map(|n| n.running.keys())
            .any(|t| self.priority_of(t.job) < p)
    }

    // -- nodes ------------------------------------------------------------

    /// A node by name (exact) or id prefix.
    pub fn resolve_node(&self, node: &str) -> Result<String> {
        let node = node.trim();
        let by_name: Vec<&String> = self
            .members
            .values()
            .filter(|m| m.info.name == node)
            .map(|m| &m.id)
            .collect();
        match by_name.as_slice() {
            [one] => return Ok((*one).clone()),
            [] => {}
            _ => bail!(
                "{} nodes are named {node:?}; use the node id",
                by_name.len()
            ),
        }
        self.resolve_member(node)
            .with_context(|| format!("no node named {node:?}"))
    }

    pub(crate) fn node_config(&self, id: &str) -> Option<&NodeConfig> {
        self.mgmt.node_config.get(id)
    }

    /// Every label of a node: the built-in ones, then the admin's.
    pub fn node_labels(&self, id: &str) -> LabelSet {
        let mut l = LabelSet::new();
        let Some(m) = self.members.get(id) else {
            return l;
        };
        let info = self.nodes.get(id).map(|n| &n.info).unwrap_or(&m.info);
        let mut put = |k: &str, v: String| {
            if !v.is_empty() {
                let vals = l.entry(k.to_string()).or_default();
                if !vals.contains(&v) {
                    vals.push(v);
                }
            }
        };
        put("name", info.name.clone());
        put("os", info.os.clone());
        put("arch", info.arch.clone());
        put("role", m.role.to_string());
        put("account", m.account.clone());
        for e in m.executors(&info.executors) {
            put("executor", e.to_string());
        }
        let gpus = self.node_gpus(id);
        for g in gpus {
            put("gpu", g.vendor.to_string());
        }
        if !gpus.is_empty() {
            put("gpus", gpus.len().to_string());
        }
        if let Some(w) = self
            .webgpu
            .get(id)
            .filter(|w| !w.profile.adapters.is_empty())
        {
            put("webgpu", w.profile.adapters.len().to_string());
        }
        if let Some(d) = m.perf.as_ref().and_then(|p| p.profile.disk.as_ref()) {
            put("disk", d.class.to_string());
        }
        if let Some(c) = self.mgmt.node_config.get(id) {
            for (k, v) in &c.labels {
                put(k, v.clone());
            }
        }
        l
    }

    pub(crate) fn set_node_config(&mut self, id: &str, c: NodeConfig) -> Result<()> {
        c.validate()?;
        if c.is_empty() {
            self.store.put_node_config(id, None)?;
            self.mgmt.node_config.remove(id);
        } else {
            self.store.put_node_config(id, Some(&c))?;
            self.mgmt.node_config.insert(id.to_string(), c);
        }
        Ok(())
    }

    /// Changes a node's labels, taints or cordon.
    pub fn edit_node(&mut self, node: &str, edit: &NodeEdit) -> Result<NodeConfig> {
        let id = self.resolve_node(node)?;
        let cur = self.mgmt.node_config.get(&id).cloned().unwrap_or_default();
        let new = edit.apply(&cur)?;
        if new != cur {
            info!(node = %id, labels = ?new.labels, taints = new.taints.len(), cordoned = new.cordoned, "node configuration changed");
            self.set_node_config(&id, new.clone())?;
            self.schedule();
        }
        Ok(new)
    }

    /// Cordons a node and moves its runs elsewhere. Returns how many.
    pub fn drain(&mut self, node: &str) -> Result<u32> {
        let id = self.resolve_node(node)?;
        let mut c = self.mgmt.node_config.get(&id).cloned().unwrap_or_default();
        if !c.cordoned {
            c.cordoned = true;
            self.set_node_config(&id, c)?;
        }
        let running: Vec<TaskRef> = self
            .nodes
            .get(&id)
            .map(|n| n.running.keys().copied().collect())
            .unwrap_or_default();
        let mut moved = 0;
        for t in running {
            if self.evict(&id, t, "node drained") {
                moved += 1;
            }
        }
        info!(node = %id, moved, "node drained");
        self.schedule();
        Ok(moved)
    }

    // -- quotas -----------------------------------------------------------

    /// An account's whole quota, storage included.
    pub fn account_quota(&self, account: &str) -> AccountQuota {
        let mut q = self.mgmt.quotas.get(account).cloned().unwrap_or_default();
        q.storage_bytes = self.files.quotas.get(account).copied();
        q
    }

    pub fn set_account_quota(&mut self, account: &str, mut q: AccountQuota) -> Result<()> {
        validate_account(account)?;
        q.validate()?;
        if q.storage_bytes != self.files.quotas.get(account).copied() {
            self.set_quota(account, q.storage_bytes)?;
        }
        q.storage_bytes = None;
        if q.is_empty() {
            self.store.put_account_quota(account, None)?;
            self.mgmt.quotas.remove(account);
        } else {
            self.store.put_account_quota(account, Some(&q))?;
            self.mgmt.quotas.insert(account.to_string(), q);
        }
        info!(%account, quota = %self.account_quota(account).describe(), "quota set");
        self.schedule();
        Ok(())
    }

    /// Records CU an account spent, for its daily quota.
    pub(crate) fn note_spend(&mut self, account: &str, amount: MilliCu) {
        if amount <= 0 {
            return;
        }
        let now = now_unix();
        let q = self.mgmt.spend.entry(account.to_string()).or_default();
        q.push_back((now, amount));
        while q.front().is_some_and(|(t, _)| *t + DAY_SECS <= now) {
            q.pop_front();
        }
    }

    pub(crate) fn spent_day(&self, account: &str) -> MilliCu {
        let since = now_unix().saturating_sub(DAY_SECS);
        self.mgmt.spend.get(account).map_or(0, |q| {
            q.iter().filter(|(t, _)| *t > since).map(|(_, a)| a).sum()
        })
    }

    /// The account spent its CU for the day: only its own machines run its
    /// work until older spending drops out of the window.
    pub(crate) fn over_daily(&self, account: &str) -> bool {
        self.mgmt
            .quotas
            .get(account)
            .and_then(|q| q.cu_per_day)
            .is_some_and(|max| self.spent_day(account) >= max)
    }

    /// Runs and GPUs in use per account.
    pub(crate) fn account_usage(&self) -> HashMap<String, (u32, u32)> {
        let mut out: HashMap<String, (u32, u32)> = HashMap::new();
        for n in self.nodes.values() {
            for t in n.running.keys() {
                if let Some(j) = self.jobs.get(&t.job) {
                    let e = out.entry(j.meta.account.clone()).or_default();
                    e.0 += 1;
                    e.1 += n.gpu.busy.get(t).map_or(0, |g| g.len() as u32);
                }
            }
        }
        out
    }

    /// Whether the account may start one more run of this kind.
    pub(crate) fn within_quota(
        &self,
        need: &crate::state::Need,
        usage: &HashMap<String, (u32, u32)>,
    ) -> bool {
        let Some(q) = self.mgmt.quotas.get(&need.consumer) else {
            return true;
        };
        let (runs, gpus) = usage.get(&need.consumer).copied().unwrap_or_default();
        if q.slots.is_some_and(|max| runs >= max) {
            return false;
        }
        if let (Some(max), Some(req)) = (q.gpus, &need.gpu) {
            return gpus + req.count.max(1) <= max;
        }
        true
    }

    pub fn quotas(&self) -> Vec<QuotaView> {
        let usage = self.account_usage();
        let storage = self.usage_by_account();
        let mut queued: HashMap<&str, u32> = HashMap::new();
        for q in &self.queue {
            if let Some(j) = self.jobs.get(&q.job) {
                *queued.entry(j.meta.account.as_str()).or_default() += 1;
            }
        }
        let mut names: Vec<&String> = self
            .accounts
            .keys()
            .chain(self.mgmt.quotas.keys())
            .chain(self.mgmt.policies.keys())
            .chain(self.files.quotas.keys())
            .collect();
        names.sort();
        names.dedup();
        names
            .into_iter()
            .map(|a| {
                let (running, gpus) = usage.get(a).copied().unwrap_or_default();
                QuotaView {
                    account: a.clone(),
                    quota: self.account_quota(a),
                    running,
                    gpus,
                    spent_day: self.spent_day(a),
                    storage_bytes: storage.get(a).map_or(0, |(b, _)| *b),
                    queued: queued.get(a.as_str()).copied().unwrap_or(0),
                    policy: self.mgmt.policies.get(a).cloned().unwrap_or_default(),
                }
            })
            .collect()
    }

    /// The highest priority a submission for `account` may ask for.
    pub(crate) fn priority_cap(&self, account: &str, submitter: bool) -> i32 {
        self.mgmt
            .quotas
            .get(account)
            .and_then(|q| q.max_priority)
            .unwrap_or(if submitter { 0 } else { MAX_PRIORITY })
    }

    // -- dependencies -----------------------------------------------------

    fn deps(&self, id: JobId) -> Deps {
        let Some(job) = self.jobs.get(&id) else {
            return Deps::Failed("the job is gone".into());
        };
        for dep in &job.meta.sched.after {
            match self.jobs.get(dep).map(Self::summary) {
                None => return Deps::Failed(format!("job {dep} does not exist")),
                Some(s) => match s.state {
                    JobState::Succeeded => {}
                    JobState::Failed | JobState::Cancelled => {
                        return Deps::Failed(format!("job {dep} {}", s.state));
                    }
                    JobState::Queued | JobState::Running => return Deps::Waiting(*dep),
                },
            }
        }
        Deps::Ready
    }

    /// Checks a new job's dependencies: an error if one already failed,
    /// whether it has to wait otherwise.
    pub(crate) fn check_deps(&self, after: &[JobId]) -> Result<bool> {
        let mut wait = false;
        for dep in after {
            let job = self
                .jobs
                .get(dep)
                .with_context(|| format!("no job {dep} to wait for"))?;
            match Self::summary(job).state {
                JobState::Succeeded => {}
                s @ (JobState::Failed | JobState::Cancelled) => {
                    bail!("job {dep} {s}; nothing to wait for")
                }
                JobState::Queued | JobState::Running => wait = true,
            }
        }
        Ok(wait)
    }

    pub(crate) fn waiting_reason(&self, id: JobId) -> Option<String> {
        if !self.mgmt.waiting.contains(&id) {
            return None;
        }
        match self.deps(id) {
            Deps::Waiting(dep) => Some(format!("waiting for job {dep}")),
            _ => Some("waiting for its dependencies".into()),
        }
    }

    /// Queues jobs whose dependencies succeeded, and fails those with a
    /// dependency that did not.
    pub(crate) fn release_waiting(&mut self) {
        if self.mgmt.waiting.is_empty() {
            return;
        }
        let ids: Vec<JobId> = self.mgmt.waiting.iter().copied().collect();
        for id in ids {
            if self.jobs.get(&id).is_none_or(|j| j.meta.cancelled) {
                self.mgmt.waiting.remove(&id);
                continue;
            }
            match self.deps(id) {
                Deps::Waiting(_) => {}
                Deps::Ready => {
                    self.mgmt.waiting.remove(&id);
                    info!(job = %id, "dependencies succeeded; job queued");
                    self.queue_job(id);
                }
                Deps::Failed(why) => {
                    self.mgmt.waiting.remove(&id);
                    warn!(job = %id, "not run: {why}");
                    let Some(job) = self.jobs.get_mut(&id) else {
                        continue;
                    };
                    let mut changed = Vec::new();
                    for (i, t) in job.tasks.iter_mut().enumerate() {
                        if t.state == TaskState::Pending {
                            t.state = TaskState::Failed(Outcome::Error(format!("not run: {why}")));
                            changed.push(i as u32);
                        }
                    }
                    let refs: Vec<(u32, &crate::state::TaskRecord)> = changed
                        .iter()
                        .map(|&i| (i, &job.tasks[i as usize]))
                        .collect();
                    if let Err(e) = self.store.put_tasks(id.0, &refs) {
                        warn!("persist tasks: {e:#}");
                    }
                }
            }
        }
    }

    // -- access tickets -----------------------------------------------------

    /// The access ticket with this secret, if it is valid.
    pub fn access_caller(&self, secret: &Secret) -> Option<Caller> {
        let now = now_unix();
        let r = self
            .mgmt
            .access
            .iter()
            .find(|r| secrets_eq(&r.secret, secret) && r.live(now))?;
        Some(match &r.role {
            AccessRole::Admin => Caller::Admin {
                name: r.name.clone(),
            },
            AccessRole::Submitter { account } => Caller::Submitter {
                name: r.name.clone(),
                account: account.clone(),
            },
            AccessRole::Viewer => Caller::Viewer {
                name: r.name.clone(),
            },
        })
    }

    pub fn create_access(
        &mut self,
        name: &str,
        role: AccessRole,
        ttl: Option<Duration>,
    ) -> Result<AccessRecord> {
        validate_access_name(name)?;
        role.validate()?;
        if let AccessRole::Submitter { account } = &role {
            self.ensure_account(account)?;
        }
        let now = now_unix();
        let r = AccessRecord {
            secret: pumasi_proto::new_secret(),
            name: name.trim().to_string(),
            role,
            created_unix: now,
            expires_unix: ttl.map(|t| now + t.as_secs()),
        };
        self.store.put_access(&r)?;
        info!(name = %r.name, role = %r.role, "access ticket created");
        self.mgmt.access.push(r.clone());
        Ok(r)
    }

    pub fn list_access(&self) -> Vec<AccessView> {
        let now = now_unix();
        let mut v: Vec<AccessView> = self
            .mgmt
            .access
            .iter()
            .filter(|r| r.live(now))
            .map(AccessRecord::view)
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        v
    }

    /// Revokes the tickets whose id starts with `id`, or that are named
    /// `id`. Returns them.
    pub fn revoke_access(&mut self, id: &str) -> Result<Vec<AccessView>> {
        let id = id.trim();
        ensure!(!id.is_empty(), "which ticket?");
        let by_id: Vec<usize> = (0..self.mgmt.access.len())
            .filter(|&i| access_id(&self.mgmt.access[i].secret).starts_with(id))
            .collect();
        let hits = if by_id.is_empty() {
            (0..self.mgmt.access.len())
                .filter(|&i| self.mgmt.access[i].name == id)
                .collect()
        } else {
            ensure!(
                by_id.len() == 1,
                "ticket id {id:?} is ambiguous; give more characters"
            );
            by_id
        };
        ensure!(!hits.is_empty(), "no access ticket {id:?}");
        let mut out = Vec::new();
        for i in hits.into_iter().rev() {
            let r = self.mgmt.access.remove(i);
            self.store.remove_access(&r.secret)?;
            info!(name = %r.name, role = %r.role, "access ticket revoked");
            out.push(r.view());
        }
        Ok(out)
    }

    // -- schedules ----------------------------------------------------------

    pub fn set_schedule(&mut self, spec: ScheduleSpec) -> Result<()> {
        spec.validate()?;
        let account = spec
            .job
            .account
            .clone()
            .unwrap_or_else(|| OWNER_ACCOUNT.into());
        self.ensure_account(&account)?;
        let cron: Cron = spec.cron.parse()?;
        let now = now_unix();
        let rec = match self.mgmt.schedules.get(&spec.name) {
            Some(old) if old.spec == spec => return Ok(()),
            Some(old) => ScheduleRecord {
                next_unix: if old.spec.cron == spec.cron {
                    old.next_unix
                } else {
                    cron.next_local(now as i64).map(|t| t as u64)
                },
                spec,
                ..old.clone()
            },
            None => ScheduleRecord {
                next_unix: cron.next_local(now as i64).map(|t| t as u64),
                spec,
                last_unix: None,
                last_job: None,
                last_error: None,
                runs: 0,
            },
        };
        self.store.put_schedule(&rec.spec.name, Some(&rec))?;
        info!(schedule = %rec.spec.name, cron = %rec.spec.cron, %account, "schedule set");
        self.mgmt.schedules.insert(rec.spec.name.clone(), rec);
        Ok(())
    }

    pub fn remove_schedule(&mut self, name: &str) -> Result<()> {
        validate_schedule_name(name)?;
        ensure!(self.mgmt.schedules.contains_key(name), "no schedule {name}");
        self.store.put_schedule(name, None)?;
        self.mgmt.schedules.remove(name);
        info!(schedule = %name, "schedule removed");
        Ok(())
    }

    pub fn schedules(&self) -> Vec<ScheduleView> {
        self.mgmt.schedules.values().map(|s| s.view()).collect()
    }

    pub(crate) fn schedule_account(&self, name: &str) -> Option<String> {
        self.mgmt.schedules.get(name).map(|s| {
            s.spec
                .job
                .account
                .clone()
                .unwrap_or_else(|| OWNER_ACCOUNT.into())
        })
    }

    /// Submits a schedule's job now (`manual`), or because it is due.
    pub fn run_schedule(&mut self, name: &str, manual: bool) -> Result<JobId> {
        let rec = self
            .mgmt
            .schedules
            .get(name)
            .cloned()
            .with_context(|| format!("no schedule {name}"))?;
        let now = now_unix();
        let result = match rec.last_job.and_then(|id| self.jobs.get(&id)) {
            Some(j) if !rec.spec.overlap && !Self::summary(j).state.is_terminal() => Err(
                anyhow::anyhow!("the previous run ({}) has not finished", j.meta.id),
            ),
            _ => self.submit_from(rec.spec.job.clone(), false, Some(name.to_string())),
        };
        let mut rec = rec;
        if !manual {
            rec.next_unix = rec
                .spec
                .cron
                .parse::<Cron>()
                .ok()
                .and_then(|c| c.next_local(now as i64))
                .map(|t| t as u64);
        }
        rec.last_unix = Some(now);
        match &result {
            Ok(id) => {
                rec.last_job = Some(*id);
                rec.last_error = None;
                rec.runs += 1;
                info!(schedule = %name, job = %id, "scheduled job submitted");
            }
            Err(e) => {
                warn!(schedule = %name, "scheduled job not submitted: {e:#}");
                rec.last_error = Some(format!("{e:#}"));
            }
        }
        if let Err(e) = self.store.put_schedule(name, Some(&rec)) {
            warn!("persist schedule: {e:#}");
        }
        self.mgmt.schedules.insert(name.to_string(), rec);
        result
    }

    pub(crate) fn tick_schedules(&mut self, now: u64) {
        let due: Vec<String> = self
            .mgmt
            .schedules
            .values()
            .filter(|s| s.next_unix.is_some_and(|t| t <= now))
            .map(|s| s.spec.name.clone())
            .collect();
        for name in due {
            let _ = self.run_schedule(&name, false);
        }
    }

    // -- apply --------------------------------------------------------------

    /// Makes nodes, quotas, schedules, alert rules and webhooks match
    /// `spec`. Applying the same
    /// spec again changes nothing. Returns one line per change.
    pub fn apply(&mut self, spec: ApplySpec) -> Result<Vec<String>> {
        spec.validate()?;
        let dry = spec.dry_run;
        let mut changes = Vec::new();
        for n in &spec.nodes {
            let id = match self.resolve_node(&n.node) {
                Ok(id) => id,
                Err(e) => {
                    changes.push(format!("node {}: skipped ({e:#})", n.node));
                    continue;
                }
            };
            let cur = self.mgmt.node_config.get(&id).cloned().unwrap_or_default();
            let mut taints = n.taints.clone();
            taints.sort();
            let want = NodeConfig {
                labels: n.labels.clone(),
                taints,
                cordoned: n.cordoned.unwrap_or(cur.cordoned),
            };
            if want != cur {
                changes.push(format!("node {}: {}", n.node, node_diff(&cur, &want)));
                if !dry {
                    self.set_node_config(&id, want)?;
                }
            }
        }
        for (account, q) in &spec.accounts {
            let cur = self.account_quota(account);
            if cur != *q {
                changes.push(format!(
                    "account {account}: quota {} -> {}",
                    cur.describe(),
                    q.describe()
                ));
                if !dry {
                    self.set_account_quota(account, q.clone())?;
                }
            }
        }
        for (account, p) in &spec.policies {
            let cur = self.account_policy(account);
            if cur != *p {
                changes.push(format!(
                    "account {account}: policy {} -> {}",
                    cur.describe(),
                    p.describe()
                ));
                if !dry {
                    self.set_account_policy(account, p.clone())?;
                }
            }
        }
        for s in &spec.schedules {
            match self.mgmt.schedules.get(&s.name) {
                Some(r) if r.spec == *s => continue,
                Some(_) => changes.push(format!("schedule {}: updated", s.name)),
                None => changes.push(format!("schedule {}: created ({})", s.name, s.cron)),
            }
            if !dry {
                self.set_schedule(s.clone())?;
            }
        }
        if let Some(p) = &spec.alerts
            && *p != self.monitor.policy
        {
            changes.push("alerts: policy updated".to_string());
            if !dry {
                self.set_alert_policy(p.clone())?;
            }
        }
        let current = self.webhook_specs();
        for w in &spec.webhooks {
            match current.iter().find(|c| c.name == w.name) {
                Some(c) if c == w => continue,
                Some(_) => changes.push(format!("webhook {}: updated", w.name)),
                None => changes.push(format!(
                    "webhook {}: created ({})",
                    w.name,
                    pumasi_proto::monitor::redact_url(&w.url)
                )),
            }
            if !dry {
                self.set_webhook(w.clone())?;
            }
        }
        if spec.prune {
            for c in &current {
                if !spec.webhooks.iter().any(|w| w.name == c.name) {
                    changes.push(format!("webhook {}: removed", c.name));
                    if !dry {
                        self.remove_webhook(&c.name)?;
                    }
                }
            }
        }
        if spec.prune {
            let keep: HashSet<&str> = spec.schedules.iter().map(|s| s.name.as_str()).collect();
            let gone: Vec<String> = self
                .mgmt
                .schedules
                .keys()
                .filter(|k| !keep.contains(k.as_str()))
                .cloned()
                .collect();
            for name in gone {
                changes.push(format!("schedule {name}: removed"));
                if !dry {
                    self.remove_schedule(&name)?;
                }
            }
        }
        if !dry && !changes.is_empty() {
            self.schedule();
        }
        Ok(changes)
    }
}

/// `labels {zone=home} -> {zone=lab}, cordoned`.
fn node_diff(a: &NodeConfig, b: &NodeConfig) -> String {
    let fmt_labels = |c: &NodeConfig| {
        let v: Vec<String> = c.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
        format!("{{{}}}", v.join(","))
    };
    let fmt_taints = |c: &NodeConfig| {
        let v: Vec<String> = c.taints.iter().map(|t| t.to_string()).collect();
        format!("[{}]", v.join(","))
    };
    let mut parts = Vec::new();
    if a.labels != b.labels {
        parts.push(format!("labels {} -> {}", fmt_labels(a), fmt_labels(b)));
    }
    if a.taints != b.taints {
        parts.push(format!("taints {} -> {}", fmt_taints(a), fmt_taints(b)));
    }
    if a.cordoned != b.cordoned {
        parts.push(if b.cordoned { "cordoned" } else { "uncordoned" }.to_string());
    }
    parts.join(", ")
}

/// The management part of the startup snapshot.
#[derive(Default)]
pub(crate) struct MgmtSnapshot {
    pub node_config: Vec<(String, NodeConfig)>,
    pub account_quotas: Vec<(String, AccountQuota)>,
    pub preempted: Vec<((u128, u32, bool), u32)>,
    pub access: Vec<AccessRecord>,
    pub schedules: Vec<ScheduleRecord>,
    pub account_policies: Vec<(String, pumasi_proto::manage::AccountPolicy)>,
}

/// Fills in what a submission leaves to the cluster.
pub(crate) fn sched_of(sub: &JobSubmission, schedule: Option<String>) -> JobSched {
    JobSched {
        priority: sub.priority,
        selector: sub.selector.clone(),
        tolerations: sub.tolerations.clone(),
        after: sub.after.clone(),
        schedule,
    }
}
