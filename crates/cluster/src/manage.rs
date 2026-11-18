//! Cluster management (phase 10): node labels, taints, cordon and drain,
//! priorities and preemption, account quotas, job dependencies, access
//! tickets with roles, schedules and `apply`. Extends [`State`].

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{
    Secret,
    economy::{MilliCu, validate_account},
    job::{JobId, JobSubmission},
    manage::{
        AccessRole, AccessView, AccountQuota, ApplySpec, LabelSet, MAX_PRIORITY, NodeConfig,
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

    pub(crate) fn live(&self, now: u64) -> bool {
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
            // 1.9: the time zone, when it has one, in the same field.
            cron: match &self.spec.tz {
                Some(tz) => format!("{} ({tz})", self.spec.cron),
                None => self.spec.cron.clone(),
            },
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
    /// Caps on each job of an account (1.19).
    pub job_caps: HashMap<String, pumasi_proto::manage::JobCaps>,
    /// When jobs with a queue timeout could first run (phase 15).
    pub ready_since: HashMap<JobId, std::time::Instant>,
    /// What a node said its run's parts took, until the run's report is
    /// applied (1.4).
    pub report_timing: HashMap<TaskRef, pumasi_proto::msg::TaskTiming>,
    /// What a node said its run's network carried, signed, until the
    /// run's report is applied (1.18).
    pub report_net: HashMap<TaskRef, pumasi_proto::receipt::SignedNet>,
    /// The cache settings each node was sent last (1.18).
    pub cache_sent: HashMap<String, pumasi_proto::store::CachePolicy>,
    /// When the queue's mounts were last looked at for them.
    pub cache_tick: Option<std::time::Instant>,
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

/// The meta key of every account's caps per job (1.19).
const JOB_CAPS_META: &str = "job_caps";

impl State {
    /// Every account's caps per job, by name.
    pub fn job_caps_view(&self) -> Vec<(String, pumasi_proto::manage::JobCaps)> {
        let mut v: Vec<_> = self
            .mgmt
            .job_caps
            .iter()
            .map(|(a, c)| (a.clone(), *c))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn job_caps(&self, account: &str) -> pumasi_proto::manage::JobCaps {
        self.mgmt.job_caps.get(account).copied().unwrap_or_default()
    }

    /// Sets (replaces) an account's caps per job.
    pub fn set_job_caps(
        &mut self,
        account: &str,
        caps: pumasi_proto::manage::JobCaps,
    ) -> Result<()> {
        pumasi_proto::economy::validate_account(account)?;
        caps.validate()?;
        let mut all = self.mgmt.job_caps.clone();
        if caps.is_empty() {
            all.remove(account);
        } else {
            all.insert(account.to_string(), caps);
        }
        let mut rows: Vec<_> = all.iter().map(|(a, c)| (a.clone(), *c)).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        self.store.put_meta_record(JOB_CAPS_META, &rows)?;
        self.mgmt.job_caps = all;
        // (Who for is in the audit log.)
        info!(caps = %caps.describe(), "caps per job set");
        Ok(())
    }

    /// Why a job of `account` whose tasks each ask for `cpus` and
    /// `memory_bytes` can never run under the account's caps per job.
    pub(crate) fn check_job_caps(&self, account: &str, cpus: u32, memory_bytes: u64) -> Result<()> {
        if let Some(why) = self.job_caps(account).refuse_task(cpus, memory_bytes) {
            anyhow::bail!("{why} (`pumasi quota {account} --max-job-cpus/--max-job-memory`)");
        }
        Ok(())
    }

    /// The account's CU cap per job, once job `id` has spent it (1.19).
    pub(crate) fn job_over_cu(&self, id: JobId) -> Option<pumasi_proto::economy::MilliCu> {
        let j = self.jobs.get(&id)?;
        let cap = self.mgmt.job_caps.get(&j.meta.account)?.cu?;
        let spent: pumasi_proto::economy::MilliCu = j.tasks.iter().map(|t| t.cost).sum();
        (spent >= cap).then_some(cap)
    }

    /// Fails the tasks of job `id` that have not run yet, once it has spent
    /// its account's CU cap per job; runs under way finish (1.19).
    pub(crate) fn stop_at_cu_cap(&mut self, id: JobId) {
        let Some(cap) = self.job_over_cu(id) else {
            return;
        };
        let Some(job) = self.jobs.get_mut(&id) else {
            return;
        };
        if job.meta.cancelled {
            return;
        }
        let spent: pumasi_proto::economy::MilliCu = job.tasks.iter().map(|t| t.cost).sum();
        let msg = format!(
            "the job spent {} of the {} its account's jobs may each spend",
            pumasi_proto::economy::format_cu(spent),
            pumasi_proto::economy::format_cu(cap)
        );
        let stopped: Vec<u32> = (0..job.tasks.len() as u32)
            .filter(|&i| job.tasks[i as usize].state == TaskState::Pending)
            .collect();
        if stopped.is_empty() {
            return;
        }
        for &i in &stopped {
            job.tasks[i as usize].state =
                TaskState::Failed(pumasi_proto::msg::Outcome::Error(msg.clone()));
        }
        let refs: Vec<(u32, &crate::state::TaskRecord)> = stopped
            .iter()
            .map(|&i| (i, &job.tasks[i as usize]))
            .collect();
        if let Err(e) = self.store.put_tasks(id.0, &refs) {
            warn!("persist tasks: {e:#}");
        }
        self.queue
            .retain(|q| !(q.job == id && !q.check && stopped.contains(&q.index)));
        warn!(job = %id, tasks = stopped.len(), "{msg}");
        self.event(
            pumasi_proto::monitor::EventLevel::Warning,
            "job.cu_cap",
            None,
            Some(id),
            pumasi_proto::text::Text::new(
                "job.cu_cap",
                [
                    id.to_string(),
                    pumasi_proto::economy::format_cu(spent),
                    pumasi_proto::economy::format_cu(cap),
                    stopped.len().to_string(),
                ],
            ),
        );
    }

    pub(crate) fn load_mgmt(&mut self, snap: MgmtSnapshot) -> Result<()> {
        self.mgmt.node_config = snap.node_config.into_iter().collect();
        self.mgmt.quotas = snap.account_quotas.into_iter().collect();
        self.mgmt.policies = snap.account_policies.into_iter().collect();
        self.mgmt.job_caps = self
            .store
            .meta_record::<Vec<(String, pumasi_proto::manage::JobCaps)>>(JOB_CAPS_META)?
            .unwrap_or_default()
            .into_iter()
            .collect();
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
            if s.next_unix.is_none() {
                s.next_unix = s.spec.next_after(now as i64).map(|t| t as u64);
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
            since: Instant::now(),
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
        let since = Instant::now();
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
                since,
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
    /// A run that keeps checkpoints (on an agent that can) gets the stop
    /// signal and its grace period instead, and goes back to the queue
    /// once it has saved its checkpoint (1.9).
    pub(crate) fn evict(&mut self, node_id: &str, t: TaskRef, reason: &str) -> bool {
        self.evict_with(node_id, t, reason, true)
    }

    /// [`Self::evict`], with `graceful` false stopping it at once.
    pub(crate) fn evict_with(
        &mut self,
        node_id: &str,
        t: TaskRef,
        reason: &str,
        graceful: bool,
    ) -> bool {
        let grace = if graceful {
            self.stop_grace(node_id, &t)
        } else {
            None
        };
        let Some(n) = self.nodes.get_mut(node_id) else {
            return false;
        };
        if !n.running.contains_key(&t) || n.stopping.contains_key(&t) {
            return false;
        }
        if let Some(grace) = grace {
            n.stopping
                .insert(t, Instant::now() + grace + crate::sched::STOP_MARGIN);
            if let Some(tx) = &n.tx {
                let _ = tx.send(ToAgent::Stop {
                    task: t,
                    grace_ms: grace.as_millis() as u64,
                });
            }
        } else {
            n.running.remove(&t);
            n.gpu.release(&t);
            n.stopping.remove(&t);
            if let Some(tx) = &n.tx {
                let _ = tx.send(ToAgent::Cancel(t));
            }
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
        self.monitor.counters.preemptions += 1;
        let name = self.node_name(node_id);
        let msg = match grace {
            Some(g) => pumasi_proto::text::Text::new(
                "task.preempted.grace",
                [
                    t.job.to_string(),
                    t.index.to_string(),
                    name.clone(),
                    reason.to_string(),
                    pumasi_proto::job::format_duration_ms(g.as_millis() as u64),
                ],
            ),
            None => pumasi_proto::text::Text::new(
                "task.preempted",
                [
                    t.job.to_string(),
                    t.index.to_string(),
                    name.clone(),
                    reason.to_string(),
                ],
            ),
        };
        info!(node = %node_id, task = %t, graceful = grace.is_some(), "{reason}; run moved back to the queue");
        self.event(
            pumasi_proto::monitor::EventLevel::Info,
            "task.preempted",
            Some(node_id),
            Some(t.job),
            msg,
        );
        match grace {
            // Back in the queue once it has stopped; its gang stops with
            // it now.
            Some(_) => self.stop_gang_mates(t.job, reason),
            None => self.requeue_run(t, reason),
        }
        true
    }

    /// Frees room for a queued run by stopping runs of lower priority on
    /// one node: the node that needs the fewest, lowest-priority runs
    /// stopped. [`crate::sched::Preempt::Wait`] when runs already stopping
    /// will free enough, or when the ones it stops now take a grace period.
    pub(crate) fn preempt_for(
        &mut self,
        need: &crate::state::Need,
        labels: &HashMap<String, LabelSet>,
        usage: &crate::sched::Usage,
    ) -> crate::sched::Preempt {
        let Some((id, victims)) =
            self.plan_preempt(need, labels, usage, &HashMap::new(), &HashSet::new())
        else {
            return crate::sched::Preempt::None;
        };
        if victims.is_empty() {
            return crate::sched::Preempt::Wait;
        }
        let mut now = true;
        for (node, t) in victims {
            self.evict(&node, t, "preempted by a higher-priority job");
            if self
                .nodes
                .get(&node)
                .is_some_and(|n| n.running.contains_key(&t))
            {
                now = false;
            }
        }
        if now {
            crate::sched::Preempt::Room(id)
        } else {
            crate::sched::Preempt::Wait
        }
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
        // Its Docker GPU tasks can draw (phase 15).
        if self.nodes.get(id).is_some_and(|n| n.gpu_graphics) {
            put("gpu.graphics", "true".into());
        }
        if let Some(w) = self
            .webgpu
            .get(id)
            .filter(|w| !w.profile.adapters.is_empty())
        {
            put("webgpu", w.profile.adapters.len().to_string());
        }
        // From the cluster's timing on providers' machines (1.20).
        if let Some(class) = self.disk_label(id) {
            put("disk", class.to_string());
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

    /// Whether the account may start one more run of this kind: its slots,
    /// GPUs, CPUs and memory (not its quotas per node label, which depend
    /// on the node: [`Self::label_quota_refusal`]).
    pub(crate) fn within_quota(
        &self,
        need: &crate::state::Need,
        usage: &crate::sched::Usage,
    ) -> bool {
        let used = usage.account(&need.consumer);
        if let Some(q) = self.mgmt.quotas.get(&need.consumer) {
            if q.slots.is_some_and(|max| used.runs >= max) {
                return false;
            }
            if let (Some(max), Some(req)) = (q.gpus, &need.gpu)
                && used.gpus + req.count.max(1) > max
            {
                return false;
            }
        }
        if let Some(r) = self.sched.resources.get(&need.consumer)
            && r.over(&used, &crate::sched::use_of(need, 0)).is_some()
        {
            return false;
        }
        // What one job may hold at once (1.19).
        if !need.item.check
            && let Some(caps) = self.mgmt.job_caps.get(&need.consumer)
        {
            let held = usage.job(need.item.job);
            if caps.cpus.is_some_and(|max| held.cpus + need.cpus > max)
                || caps
                    .memory_bytes
                    .is_some_and(|max| held.memory_bytes + need.memory > max)
            {
                return false;
            }
        }
        true
    }

    pub fn quotas(&self) -> Vec<QuotaView> {
        let usage = self.usage();
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
                let used = usage.account(a);
                let (running, gpus) = (used.runs, used.gpus);
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
        let now = now_unix();
        let next = spec.next_after(now as i64).map(|t| t as u64);
        let rec = match self.mgmt.schedules.get(&spec.name) {
            Some(old) if old.spec == spec => return Ok(()),
            Some(old) => ScheduleRecord {
                next_unix: if old.spec.cron == spec.cron && old.spec.tz == spec.tz {
                    old.next_unix
                } else {
                    next
                },
                spec,
                ..old.clone()
            },
            None => ScheduleRecord {
                next_unix: next,
                spec,
                last_unix: None,
                last_job: None,
                last_error: None,
                runs: 0,
            },
        };
        self.store.put_schedule(&rec.spec.name, Some(&rec))?;
        info!(schedule = %rec.spec.name, cron = %rec.spec.cron, tz = ?rec.spec.tz, %account, "schedule set");
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
            rec.next_unix = rec.spec.next_after(now as i64).map(|t| t as u64);
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
        // CPU and memory quotas (1.9): an account the file lists without
        // them has none.
        let mut resources: BTreeMap<&str, pumasi_proto::sched::ResourceQuota> = spec
            .accounts
            .iter()
            .map(|(a, _)| (a.as_str(), Default::default()))
            .collect();
        for (a, q) in &spec.resources {
            resources.insert(a.as_str(), q.clone());
        }
        for (account, q) in resources {
            let cur = self.account_resources(account);
            if cur != q {
                changes.push(format!(
                    "account {account}: CPU and memory quota {} -> {}",
                    cur.describe(),
                    q.describe()
                ));
                if !dry {
                    self.set_resources(account, q)?;
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
            // A schedule listed without a job keeps the one it has (1.15).
            let s = if spec.keep_jobs.contains(&s.name) {
                let Some(r) = self.mgmt.schedules.get(&s.name) else {
                    changes.push(format!(
                        "schedule {}: skipped (no such schedule; give it a job)",
                        s.name
                    ));
                    continue;
                };
                let mut kept = s.clone();
                kept.job = r.spec.job.clone();
                if s.job.account.is_some() {
                    kept.job.account = s.job.account.clone();
                }
                kept
            } else {
                s.clone()
            };
            match self.mgmt.schedules.get(&s.name) {
                Some(r) if r.spec == s => continue,
                Some(r) => changes.push(format!(
                    "schedule {}: updated{}",
                    s.name,
                    schedule_diff(&r.spec, &s)
                )),
                None => changes.push(format!("schedule {}: created ({})", s.name, s.cron)),
            }
            if !dry {
                self.set_schedule(s)?;
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
        // Repeat and resolved notices and per-node thresholds (1.15).
        if let Some(t) = &spec.alert_tuning
            && *t != self.monitor.tuning
        {
            changes.extend(tuning_diff(&self.monitor.tuning, t));
            if !dry {
                self.set_alert_tuning(t.clone())?;
            }
        }
        for f in &spec.silences {
            if let Some(line) = self.apply_file_silence(f, dry)? {
                changes.push(line);
            }
        }
        if spec.prune {
            let keep: Vec<String> = spec.silences.iter().map(|f| f.id()).collect();
            for id in self.file_silences_not_in(&keep) {
                changes.push(format!("silence {id}: removed"));
                if !dry {
                    self.remove_silence(&id)?;
                }
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

/// ` (cron 0 3 * * * -> 0 4 * * *)`: what changed in a schedule, besides
/// its job.
fn schedule_diff(a: &ScheduleSpec, b: &ScheduleSpec) -> String {
    let mut parts = Vec::new();
    if a.cron != b.cron {
        parts.push(format!("cron {} -> {}", a.cron, b.cron));
    }
    if a.tz != b.tz {
        let tz = |t: &Option<String>| t.clone().unwrap_or_else(|| "local".into());
        parts.push(format!("time zone {} -> {}", tz(&a.tz), tz(&b.tz)));
    }
    if a.overlap != b.overlap {
        parts.push(format!("overlap {} -> {}", a.overlap, b.overlap));
    }
    if a.job.account != b.job.account {
        let acc = |x: &Option<String>| x.clone().unwrap_or_else(|| "-".into());
        parts.push(format!(
            "account {} -> {}",
            acc(&a.job.account),
            acc(&b.job.account)
        ));
    }
    let mut ja = a.job.clone();
    ja.account = b.job.account.clone();
    if ja != b.job {
        parts.push("job".into());
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(", "))
    }
}

/// One line per change between two alert tunings (1.15).
fn tuning_diff(
    a: &pumasi_proto::monitor::AlertTuning,
    b: &pumasi_proto::monitor::AlertTuning,
) -> Vec<String> {
    use pumasi_proto::monitor::human_duration;
    let mut out = Vec::new();
    let rep = |r: Option<u64>| {
        r.map_or("once".to_string(), |s| {
            format!("every {}", human_duration(s))
        })
    };
    if a.repeat_secs != b.repeat_secs {
        out.push(format!(
            "alerts: repeat {} -> {}",
            rep(a.repeat_secs),
            rep(b.repeat_secs)
        ));
    }
    if a.resolved != b.resolved {
        out.push(format!(
            "alerts: resolved notices {} -> {}",
            a.resolved, b.resolved
        ));
    }
    for o in &b.overrides {
        match a.overrides.iter().find(|x| x.nodes == o.nodes) {
            Some(x) if x == o => {}
            Some(x) => out.push(format!(
                "alerts on {}: {} -> {}",
                o.nodes,
                x.describe(false),
                o.describe(false)
            )),
            None => out.push(format!("alerts on {}: {}", o.nodes, o.describe(false))),
        }
    }
    for x in &a.overrides {
        if !b.overrides.iter().any(|o| o.nodes == x.nodes) {
            out.push(format!("alerts on {}: removed", x.nodes));
        }
    }
    if out.is_empty() {
        out.push("alerts: thresholds reordered".into());
    }
    out
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
