//! Control-plane state machine: membership, the task queue and scheduling.
//!
//! All mutation goes through [`State`] while holding one mutex. Methods never
//! block on I/O other than short redb writes; messages to agents go through
//! unbounded channels.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    ModuleHash, Secret,
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{
        ClusterStatus, JobState, JobSummary, JobView, NodeInfo, NodeView, Outcome, TaskAssignment,
        TaskRef, TaskReport, TaskState, TaskView, ToAgent,
    },
    secrets_eq,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::store::Store;

/// Extra time past a task's own timeout before the control plane assumes
/// the node hung and reschedules the task elsewhere.
const LEASE_GRACE: Duration = Duration::from_secs(30);

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Persisted records
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    /// The node's iroh endpoint id (its public key).
    pub id: String,
    pub info: NodeInfo,
    pub joined_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub secret: Secret,
    pub expires_unix: u64,
    pub uses_left: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMeta {
    pub id: JobId,
    pub name: String,
    pub executor: Executor,
    /// WASM module (wasm executor).
    pub module_hash: Option<ModuleHash>,
    pub docker: Option<DockerSpec>,
    /// Postcard-encoded `Vec<FileEntry>` shared by every task, if any.
    pub files_hash: Option<ModuleHash>,
    pub limits: Limits,
    pub tier: Tier,
    pub max_attempts: u32,
    pub max_price_per_hour: Option<f64>,
    pub submitted_unix: u64,
    pub cancelled: bool,
    /// Submission order, for FIFO scheduling after a restart.
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub input: TaskInput,
    pub state: TaskState,
    pub attempts: u32,
    pub node: Option<String>,
    pub fuel_used: u64,
    pub wall_ms: u64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub output_files: Vec<FileEntry>,
}

// ---------------------------------------------------------------------------
// Runtime-only state
// ---------------------------------------------------------------------------

struct Job {
    meta: JobMeta,
    tasks: Vec<TaskRecord>,
}

struct NodeRuntime {
    info: NodeInfo,
    online: bool,
    /// Bumped on every new connection so a stale session cannot clobber a
    /// fresh one.
    generation: u64,
    tx: Option<mpsc::UnboundedSender<ToAgent>>,
    kill: Option<oneshot::Sender<()>>,
    /// Running tasks and when they were placed.
    running: HashMap<TaskRef, Instant>,
    /// Content blobs (modules, file bundles) already sent over the current
    /// connection.
    sent_modules: HashSet<ModuleHash>,
    last_seen: Instant,
    last_seen_unix: u64,
    completed: u64,
}

impl NodeRuntime {
    fn free_slots(&self) -> u32 {
        self.info.slots.saturating_sub(self.running.len() as u32)
    }
}

pub struct State {
    pub cluster_name: String,
    pub cluster_id: String,
    store: Store,
    members: HashMap<String, Member>,
    tokens: Vec<TokenRecord>,
    nodes: HashMap<String, NodeRuntime>,
    jobs: HashMap<JobId, Job>,
    /// Content-addressed blobs: WASM modules and encoded file bundles.
    modules: HashMap<ModuleHash, Arc<Vec<u8>>>,
    /// Tasks waiting for a node, in scheduling order.
    queue: VecDeque<(JobId, u32)>,
    next_seq: u64,
    next_generation: u64,
}

impl State {
    pub fn load(cluster_name: String, cluster_id: String, store: Store) -> Result<Self> {
        let snap = store.load()?;
        let mut state = Self {
            cluster_name,
            cluster_id,
            store,
            members: snap
                .members
                .into_iter()
                .map(|m| (m.id.clone(), m))
                .collect(),
            tokens: snap.tokens,
            nodes: HashMap::new(),
            jobs: HashMap::new(),
            modules: snap
                .modules
                .into_iter()
                .map(|(h, b)| (h, Arc::new(b)))
                .collect(),
            queue: VecDeque::new(),
            next_seq: 0,
            next_generation: 1,
        };
        let mut jobs = snap.jobs;
        jobs.sort_by_key(|(m, _)| m.seq);
        for (meta, mut tasks) in jobs {
            state.next_seq = state.next_seq.max(meta.seq + 1);
            let mut changed = Vec::new();
            for (i, t) in tasks.iter_mut().enumerate() {
                // Nothing is connected yet, so anything "running" was
                // interrupted by the restart.
                if matches!(t.state, TaskState::Running { .. }) {
                    t.state = TaskState::Pending;
                    t.node = None;
                    changed.push(i as u32);
                }
                if t.state == TaskState::Pending && !meta.cancelled {
                    state.queue.push_back((meta.id, i as u32));
                }
            }
            let refs: Vec<(u32, &TaskRecord)> =
                changed.iter().map(|&i| (i, &tasks[i as usize])).collect();
            state.store.put_tasks(meta.id.0, &refs)?;
            state.jobs.insert(meta.id, Job { meta, tasks });
        }
        let now = now_unix();
        state.tokens.retain(|t| t.expires_unix > now);
        info!(
            members = state.members.len(),
            jobs = state.jobs.len(),
            queued = state.queue.len(),
            "state loaded"
        );
        Ok(state)
    }

    // -- membership ---------------------------------------------------------

    pub fn create_token(&mut self, ttl: Duration, uses: Option<u32>) -> Result<TokenRecord> {
        let token = TokenRecord {
            secret: pumasi_proto::new_secret(),
            expires_unix: now_unix() + ttl.as_secs(),
            uses_left: uses,
        };
        self.store.put_token(&token)?;
        self.tokens.push(token.clone());
        Ok(token)
    }

    /// Decides whether a connecting node may join. Known members are always
    /// allowed; new nodes need a valid, unexpired join secret.
    pub fn authorize(&mut self, id: &str, info: &NodeInfo, secret: Option<&Secret>) -> Result<()> {
        if let Some(m) = self.members.get_mut(id) {
            if m.info != *info {
                m.info = info.clone();
                let m = m.clone();
                self.store.put_member(&m)?;
            }
            return Ok(());
        }
        let Some(secret) = secret else {
            bail!("unknown node; join with a ticket first");
        };
        let now = now_unix();
        let Some(pos) = self
            .tokens
            .iter()
            .position(|t| secrets_eq(&t.secret, secret) && t.expires_unix > now)
        else {
            bail!("join ticket is invalid or expired");
        };
        let token = &mut self.tokens[pos];
        if let Some(n) = token.uses_left.as_mut() {
            *n = n.saturating_sub(1);
            if *n == 0 {
                let secret = token.secret;
                self.tokens.remove(pos);
                self.store.remove_token(&secret)?;
            } else {
                let t = token.clone();
                self.store.put_token(&t)?;
            }
        }
        let member = Member {
            id: id.to_string(),
            info: info.clone(),
            joined_unix: now,
        };
        self.store.put_member(&member)?;
        self.members.insert(id.to_string(), member);
        info!(node = %id, name = %info.name, "new node joined");
        Ok(())
    }

    /// Removes a member. `id` may be a unique prefix, as `pumasi status`
    /// shows only the first characters.
    pub fn remove_node(&mut self, id: &str) -> Result<()> {
        let id = id.trim();
        let matches: Vec<String> = self
            .members
            .keys()
            .filter(|k| !id.is_empty() && k.starts_with(id))
            .cloned()
            .collect();
        let id = match matches.as_slice() {
            [one] => one.clone(),
            [] => bail!("no such node"),
            _ => bail!("node id prefix {id:?} is ambiguous; give more characters"),
        };
        let id = id.as_str();
        self.members.remove(id);
        self.store.remove_member(id)?;
        if let Some(n) = self.nodes.get_mut(id)
            && let Some(kill) = n.kill.take()
        {
            let _ = kill.send(());
        }
        let generation = self.nodes.get(id).map(|n| n.generation).unwrap_or(0);
        self.on_disconnect(id, generation);
        self.nodes.remove(id);
        Ok(())
    }

    /// Registers a live connection and returns its generation.
    pub fn on_connect(
        &mut self,
        id: &str,
        info: NodeInfo,
        tx: mpsc::UnboundedSender<ToAgent>,
        kill: oneshot::Sender<()>,
    ) -> u64 {
        // A reconnect replaces any previous session from the same node.
        if let Some(old) = self.nodes.get(id)
            && old.online
        {
            let g = old.generation;
            self.on_disconnect(id, g);
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        let prev_completed = self.nodes.get(id).map(|n| n.completed).unwrap_or(0);
        if let Some(mut old) = self.nodes.remove(id)
            && let Some(kill) = old.kill.take()
        {
            let _ = kill.send(());
        }
        self.nodes.insert(
            id.to_string(),
            NodeRuntime {
                info,
                online: true,
                generation,
                tx: Some(tx),
                kill: Some(kill),
                running: HashMap::new(),
                sent_modules: HashSet::new(),
                last_seen: Instant::now(),
                last_seen_unix: now_unix(),
                completed: prev_completed,
            },
        );
        self.schedule();
        generation
    }

    /// Marks a session as gone and puts its tasks back in the queue.
    pub fn on_disconnect(&mut self, id: &str, generation: u64) {
        let Some(node) = self.nodes.get_mut(id) else {
            return;
        };
        if node.generation != generation || !node.online {
            return;
        }
        node.online = false;
        node.tx = None;
        if let Some(kill) = node.kill.take() {
            let _ = kill.send(());
        }
        let lost: Vec<TaskRef> = node.running.drain().map(|(t, _)| t).collect();
        if !lost.is_empty() {
            warn!(node = %id, tasks = lost.len(), "node lost; rescheduling its tasks");
        }
        for t in lost {
            self.requeue(t, "node disconnected");
        }
        self.schedule();
    }

    pub fn on_heartbeat(&mut self, id: &str, generation: u64) {
        if let Some(n) = self.nodes.get_mut(id)
            && n.generation == generation
        {
            n.last_seen = Instant::now();
            n.last_seen_unix = now_unix();
        }
    }

    /// Periodic housekeeping: drop silent nodes, reclaim hung tasks.
    pub fn tick(&mut self, node_timeout: Duration) {
        let now = Instant::now();
        let silent: Vec<(String, u64)> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.online && now.duration_since(n.last_seen) > node_timeout)
            .map(|(id, n)| (id.clone(), n.generation))
            .collect();
        for (id, g) in silent {
            warn!(node = %id, "node missed heartbeats");
            self.on_disconnect(&id, g);
        }

        let mut hung = Vec::new();
        for (id, n) in &self.nodes {
            for (t, started) in &n.running {
                let limit = self
                    .jobs
                    .get(&t.job)
                    .map(|j| Duration::from_millis(j.meta.limits.timeout_ms))
                    .unwrap_or_default()
                    + LEASE_GRACE;
                if now.duration_since(*started) > limit {
                    hung.push((id.clone(), *t));
                }
            }
        }
        for (id, t) in hung {
            warn!(node = %id, task = %t, "task exceeded its lease; rescheduling");
            if let Some(n) = self.nodes.get_mut(&id) {
                n.running.remove(&t);
                if let Some(tx) = &n.tx {
                    let _ = tx.send(ToAgent::Cancel(t));
                }
            }
            self.requeue(t, "task lease expired");
        }
        if !self.queue.is_empty() {
            self.schedule();
        }
    }

    // -- jobs -----------------------------------------------------------------

    pub fn submit(&mut self, mut sub: JobSubmission) -> Result<JobId> {
        sub.validate()?;
        let mut blobs: Vec<(ModuleHash, Vec<u8>)> = Vec::new();
        let module_hash = match sub.executor {
            Executor::Wasm => {
                let h = pumasi_proto::module_hash(&sub.module);
                blobs.push((h, std::mem::take(&mut sub.module)));
                Some(h)
            }
            Executor::Docker => None,
        };
        let files_hash = if sub.files.is_empty() {
            None
        } else {
            let bundle = postcard::to_stdvec(&sub.files)?;
            let h = pumasi_proto::module_hash(&bundle);
            blobs.push((h, bundle));
            Some(h)
        };
        blobs.retain(|(h, _)| !self.modules.contains_key(h));
        let id = loop {
            let id = JobId::random();
            if !self.jobs.contains_key(&id) {
                break id;
            }
        };
        let meta = JobMeta {
            id,
            name: sub.name,
            executor: sub.executor,
            module_hash,
            docker: sub.docker,
            files_hash,
            limits: sub.limits,
            tier: sub.tier,
            max_attempts: sub.max_attempts,
            max_price_per_hour: sub.max_price_per_hour,
            submitted_unix: now_unix(),
            cancelled: false,
            seq: self.next_seq,
        };
        self.next_seq += 1;
        let tasks: Vec<TaskRecord> = sub
            .tasks
            .into_iter()
            .map(|input| TaskRecord {
                input,
                state: TaskState::Pending,
                attempts: 0,
                node: None,
                fuel_used: 0,
                wall_ms: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
                output_files: Vec::new(),
            })
            .collect();
        let blob_refs: Vec<(ModuleHash, &[u8])> =
            blobs.iter().map(|(h, b)| (*h, b.as_slice())).collect();
        self.store.put_new_job(&meta, &tasks, &blob_refs)?;
        for (h, b) in blobs {
            self.modules.insert(h, Arc::new(b));
        }
        for i in 0..tasks.len() {
            self.queue.push_back((id, i as u32));
        }
        info!(job = %id, name = %meta.name, tasks = tasks.len(), "job submitted");
        self.jobs.insert(id, Job { meta, tasks });
        self.schedule();
        Ok(id)
    }

    pub fn cancel(&mut self, id: JobId) -> Result<()> {
        let Some(job) = self.jobs.get_mut(&id) else {
            bail!("no such job")
        };
        if job.meta.cancelled {
            return Ok(());
        }
        job.meta.cancelled = true;
        let mut changed = Vec::new();
        for (i, t) in job.tasks.iter_mut().enumerate() {
            match &t.state {
                TaskState::Pending => {
                    t.state = TaskState::Cancelled;
                    changed.push(i as u32);
                }
                TaskState::Running { node } => {
                    let r = TaskRef {
                        job: id,
                        index: i as u32,
                        attempt: t.attempts,
                    };
                    if let Some(tx) = self.nodes.get(node).and_then(|n| n.tx.as_ref()) {
                        let _ = tx.send(ToAgent::Cancel(r));
                    }
                }
                _ => {}
            }
        }
        let meta = job.meta.clone();
        let refs: Vec<(u32, &TaskRecord)> = changed
            .iter()
            .map(|&i| (i, &job.tasks[i as usize]))
            .collect();
        self.store.put_tasks(id.0, &refs)?;
        self.store.put_job_meta(&meta)?;
        self.queue.retain(|(j, _)| *j != id);
        info!(job = %id, "job cancelled");
        Ok(())
    }

    /// Applies a task report from a node.
    pub fn on_report(&mut self, node_id: &str, generation: u64, report: TaskReport) {
        let Some(node) = self.nodes.get_mut(node_id) else {
            return;
        };
        if node.generation != generation {
            return;
        }
        if node.running.remove(&report.task).is_none() {
            // Stale: the task was already rescheduled elsewhere.
            return;
        }
        node.completed += 1;
        let r = report.task;
        let Some(job) = self.jobs.get_mut(&r.job) else {
            return;
        };
        let Some(task) = job.tasks.get_mut(r.index as usize) else {
            return;
        };
        if task.attempts != r.attempt {
            return;
        }
        task.fuel_used = report.fuel_used;
        task.wall_ms = report.wall_ms;
        task.stdout = report.stdout;
        task.stderr = report.stderr;
        task.output_files = report.files;
        task.state = match report.outcome {
            Outcome::Exited(0) => TaskState::Succeeded,
            Outcome::Cancelled => TaskState::Cancelled,
            other => TaskState::Failed(other),
        };
        if let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)]) {
            warn!("persist task: {e:#}");
        }
        self.schedule();
    }

    fn requeue(&mut self, r: TaskRef, reason: &str) {
        let Some(job) = self.jobs.get_mut(&r.job) else {
            return;
        };
        let max = job.meta.max_attempts;
        let cancelled = job.meta.cancelled;
        let Some(task) = job.tasks.get_mut(r.index as usize) else {
            return;
        };
        if task.attempts != r.attempt || !matches!(task.state, TaskState::Running { .. }) {
            return;
        }
        task.node = None;
        if cancelled {
            task.state = TaskState::Cancelled;
        } else if task.attempts >= max {
            task.state = TaskState::Failed(Outcome::Error(format!(
                "{reason}; gave up after {} attempts",
                task.attempts
            )));
        } else {
            task.state = TaskState::Pending;
            // Retries go first so a job is not starved by later jobs.
            self.queue.push_front((r.job, r.index));
        }
        if let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)]) {
            warn!("persist task: {e:#}");
        }
    }

    /// Places as many queued tasks as free slots allow.
    pub fn schedule(&mut self) {
        let mut skipped = VecDeque::new();
        let mut dirty: Vec<(JobId, u32)> = Vec::new();
        while let Some((job_id, index)) = self.queue.pop_front() {
            let Some(job) = self.jobs.get_mut(&job_id) else {
                continue;
            };
            let Some(task) = job.tasks.get_mut(index as usize) else {
                continue;
            };
            if task.state != TaskState::Pending {
                continue;
            }
            let need = job.meta.limits.memory_bytes;
            let executor = job.meta.executor;
            // Most free slots first spreads work across nodes.
            let best = self
                .nodes
                .iter_mut()
                .filter(|(_, n)| {
                    n.online
                        && n.free_slots() > 0
                        && n.info.memory_bytes >= need
                        && n.info.executors.contains(&executor)
                })
                .max_by_key(|(_, n)| n.free_slots());
            let Some((node_id, node)) = best else {
                skipped.push_back((job_id, index));
                if !self.nodes.values().any(|n| n.online && n.free_slots() > 0) {
                    break;
                }
                continue;
            };
            task.attempts += 1;
            let r = TaskRef {
                job: job_id,
                index,
                attempt: task.attempts,
            };
            let module_hash = job.meta.module_hash;
            let module = module_hash
                .filter(|h| !node.sent_modules.contains(h))
                .and_then(|h| self.modules.get(&h))
                .map(|m| m.as_ref().clone());
            let files_hash = job.meta.files_hash;
            let files = files_hash
                .filter(|h| !node.sent_modules.contains(h))
                .and_then(|h| self.modules.get(&h))
                .and_then(|b| postcard::from_bytes::<Vec<FileEntry>>(b).ok());
            let assignment = TaskAssignment {
                task: r,
                program: job.meta.name.clone(),
                executor: job.meta.executor,
                module_hash,
                module,
                docker: job.meta.docker.clone(),
                files_hash,
                files,
                input: task.input.clone(),
                limits: job.meta.limits,
            };
            let Some(tx) = &node.tx else {
                task.attempts -= 1;
                skipped.push_back((job_id, index));
                continue;
            };
            if tx.send(ToAgent::Assign(Box::new(assignment))).is_err() {
                task.attempts -= 1;
                skipped.push_back((job_id, index));
                continue;
            }
            node.sent_modules.extend(module_hash);
            node.sent_modules.extend(files_hash);
            node.running.insert(r, Instant::now());
            task.state = TaskState::Running {
                node: node_id.clone(),
            };
            task.node = Some(node_id.clone());
            dirty.push((job_id, index));
        }
        // Whatever could not be placed keeps its order at the front.
        while let Some(item) = skipped.pop_back() {
            self.queue.push_front(item);
        }
        for (job_id, index) in dirty {
            if let Some(task) = self
                .jobs
                .get(&job_id)
                .and_then(|j| j.tasks.get(index as usize))
                && let Err(e) = self.store.put_tasks(job_id.0, &[(index, task)])
            {
                warn!("persist task: {e:#}");
            }
        }
    }

    // -- views ------------------------------------------------------------------

    fn summary(job: &Job) -> JobSummary {
        let mut s = JobSummary {
            id: job.meta.id,
            name: job.meta.name.clone(),
            state: JobState::Queued,
            tasks: job.tasks.len() as u32,
            succeeded: 0,
            failed: 0,
            running: 0,
            submitted_unix: job.meta.submitted_unix,
        };
        let mut cancelled = 0;
        for t in &job.tasks {
            match t.state {
                TaskState::Succeeded => s.succeeded += 1,
                TaskState::Failed(_) => s.failed += 1,
                TaskState::Running { .. } => s.running += 1,
                TaskState::Cancelled => cancelled += 1,
                TaskState::Pending => {}
            }
        }
        let done = s.succeeded + s.failed + cancelled;
        s.state = if done == s.tasks {
            if job.meta.cancelled {
                JobState::Cancelled
            } else if s.failed > 0 {
                JobState::Failed
            } else {
                JobState::Succeeded
            }
        } else if s.running > 0 || done > 0 {
            JobState::Running
        } else {
            JobState::Queued
        };
        s
    }

    pub fn status(&self) -> ClusterStatus {
        let mut nodes: Vec<NodeView> = self
            .members
            .values()
            .map(|m| {
                let rt = self.nodes.get(&m.id);
                NodeView {
                    id: m.id.clone(),
                    info: rt.map(|n| n.info.clone()).unwrap_or_else(|| m.info.clone()),
                    online: rt.is_some_and(|n| n.online),
                    running: rt.map(|n| n.running.len() as u32).unwrap_or(0),
                    completed: rt.map(|n| n.completed).unwrap_or(0),
                    last_seen_unix: rt.map(|n| n.last_seen_unix).unwrap_or(0),
                }
            })
            .collect();
        nodes.sort_by(|a, b| a.info.name.cmp(&b.info.name).then(a.id.cmp(&b.id)));
        let mut jobs: Vec<(u64, JobSummary)> = self
            .jobs
            .values()
            .map(|j| (j.meta.seq, Self::summary(j)))
            .collect();
        jobs.sort_by_key(|(seq, _)| std::cmp::Reverse(*seq));
        ClusterStatus {
            cluster_name: self.cluster_name.clone(),
            cluster_id: self.cluster_id.clone(),
            nodes,
            jobs: jobs.into_iter().map(|(_, s)| s).collect(),
        }
    }

    pub fn task_files(&self, id: JobId, index: u32) -> Result<Vec<FileEntry>> {
        let job = self.jobs.get(&id).context("no such job")?;
        let task = job.tasks.get(index as usize).context("no such task")?;
        Ok(task.output_files.clone())
    }

    pub fn job_view(&self, id: JobId, with_output: bool) -> Option<JobView> {
        let job = self.jobs.get(&id)?;
        let tasks = job
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| TaskView {
                index: i as u32,
                state: t.state.clone(),
                attempts: t.attempts,
                node: t.node.clone(),
                fuel_used: t.fuel_used,
                wall_ms: t.wall_ms,
                stdout: with_output.then(|| t.stdout.clone()),
                stderr: with_output.then(|| t.stderr.clone()),
                output_files: t
                    .output_files
                    .iter()
                    .map(|f| (f.path.clone(), f.data.len() as u64))
                    .collect(),
            })
            .collect();
        Some(JobView {
            summary: Self::summary(job),
            executor: job.meta.executor,
            image: job.meta.docker.as_ref().map(|d| d.image.clone()),
            limits: job.meta.limits,
            total_fuel: job.tasks.iter().map(|t| t.fuel_used).sum(),
            tasks,
        })
    }
}
