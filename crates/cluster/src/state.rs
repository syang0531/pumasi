//! Control-plane state machine: membership, the task queue and scheduling.
//!
//! All mutation goes through [`State`] while holding one mutex. Methods never
//! block on I/O other than short redb writes; messages to agents go through
//! unbounded channels. Accounts and settlement live in `ledger.rs`, result
//! checks in `check.rs`; both extend [`State`].

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use iroh::{PublicKey, SecretKey};
use pumasi_proto::{
    ModuleHash, Secret,
    economy::{Metered, MilliCu, Policy, validate_account, vcpus},
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{
        AccountView, Availability, ClusterStatus, Invite, JobState, JobSummary, JobView, NodeInfo,
        NodeView, OWNER_ACCOUNT, Outcome, Role, TaskAssignment, TaskRef, TaskReport, TaskState,
        TaskView, ToAgent, UpgradeHello, Verification,
    },
    perf::SignedPerf,
    receipt::{Billing, Receipt, check_usage, result_digest},
    secrets_eq,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::{
    perf::{PerfCheck, PerfRuntime, cpu_score},
    rollout::RolloutRecord,
    store::Store,
};

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
    pub role: Role,
    /// Account the node earns for (`owner` for the owner's machines).
    pub account: String,
    /// Provider machines only: the invite allowed Docker jobs.
    pub allow_docker: bool,
    /// Checks this node's runs passed / failed.
    pub checks_passed: u32,
    pub strikes: u32,
    /// The node's latest signed performance profile.
    pub perf: Option<SignedPerf>,
    /// The cluster's own timing of its CPU benchmark.
    pub perf_check: Option<PerfCheck>,
}

impl Member {
    /// Executors the cluster sends this node: what it offers, minus Docker
    /// on provider machines whose invite did not allow it. WASM and microVM
    /// tasks are sandboxed from the provider's machine, so every node that
    /// offers them gets them.
    pub fn executors(&self, offered: &[Executor]) -> Vec<Executor> {
        offered
            .iter()
            .copied()
            .filter(|e| self.role == Role::Owner || *e != Executor::Docker || self.allow_docker)
            .collect()
    }

    pub fn trusted(&self) -> bool {
        self.role == Role::Owner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub secret: Secret,
    pub expires_unix: u64,
    pub uses_left: Option<u32>,
    /// Set for invites of other people's machines.
    pub invite: Option<Invite>,
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
    /// Account paying for runs on other accounts' machines.
    pub account: String,
    pub deterministic: bool,
}

/// What a run produced, kept for a check run until the check settles in
/// case its result is the one accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub files: Vec<FileEntry>,
    pub fuel_used: u64,
    pub wall_ms: u64,
}

/// One finished run that takes part in a check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub task: TaskRef,
    pub node: String,
    pub account: String,
    pub trusted: bool,
    pub agent_version: String,
    pub outcome: Outcome,
    pub digest: [u8; 32],
    pub fuel_used: u64,
    /// `None` for the first run, whose output is in the task itself.
    pub output: Option<RunOutput>,
}

/// A sampled task being checked by re-running it on other nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckRecord {
    /// The first run, then each finished check run.
    pub runs: Vec<RunRecord>,
    /// Placements of check runs so far (their `TaskRef::attempt`).
    pub attempts: u32,
    /// Node a check run is on right now.
    pub running_on: Option<String>,
    pub status: Verification,
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
    pub check: Option<CheckRecord>,
    /// Milli-CU charged for this task's runs.
    pub cost: MilliCu,
}

/// A provider earning waiting out the hold period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hold {
    pub task: TaskRef,
    pub consumer: String,
    pub provider: String,
    pub amount: MilliCu,
    pub release_after_unix: u64,
}

// ---------------------------------------------------------------------------
// Runtime-only state
// ---------------------------------------------------------------------------

pub(crate) struct Job {
    pub meta: JobMeta,
    pub tasks: Vec<TaskRecord>,
}

pub(crate) struct NodeRuntime {
    pub info: NodeInfo,
    pub online: bool,
    /// Bumped on every new connection so a stale session cannot clobber a
    /// fresh one.
    pub generation: u64,
    pub tx: Option<mpsc::UnboundedSender<ToAgent>>,
    pub kill: Option<oneshot::Sender<()>>,
    /// Running runs and when they were placed.
    pub running: HashMap<TaskRef, Instant>,
    /// Content blobs (modules, file bundles) already sent over the current
    /// connection.
    pub sent_modules: HashSet<ModuleHash>,
    pub last_seen: Instant,
    pub last_seen_unix: u64,
    pub completed: u64,
    // Copied from the member record for scheduling.
    pub account: String,
    pub trusted: bool,
    pub executors: Vec<Executor>,
    pub suspended: bool,
    /// Present when the agent can replace its own binary.
    pub upgrade: Option<UpgradeHello>,
    /// Gets no new tasks so it can be upgraded once idle.
    pub draining: bool,
    /// Its availability policy says no new tasks now (on battery, in use,
    /// outside its hours), and why.
    pub paused: Option<String>,
    /// Protocol, round-trip time and benchmark checks.
    pub perf: PerfRuntime,
}

impl NodeRuntime {
    fn free_slots(&self) -> u32 {
        self.info.slots.saturating_sub(self.running.len() as u32)
    }
}

/// A unit of work waiting for a node: a task's own run, or a check run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueItem {
    pub job: JobId,
    pub index: u32,
    pub check: bool,
}

pub struct State {
    pub cluster_name: String,
    pub cluster_id: String,
    pub(crate) key: SecretKey,
    pub(crate) store: Store,
    pub(crate) policy: Policy,
    pub(crate) members: HashMap<String, Member>,
    pub(crate) tokens: Vec<TokenRecord>,
    pub(crate) nodes: HashMap<String, NodeRuntime>,
    pub(crate) jobs: HashMap<JobId, Job>,
    /// Content-addressed blobs: WASM modules and encoded file bundles.
    pub(crate) modules: HashMap<ModuleHash, Arc<Vec<u8>>>,
    /// Work waiting for a node, in scheduling order.
    pub(crate) queue: VecDeque<QueueItem>,
    pub(crate) accounts: HashMap<String, AccountView>,
    pub(crate) holds: HashMap<TaskRef, Hold>,
    /// Micro-CU owed between two accounts (consumer, provider) that did not
    /// make a whole milli-CU yet.
    pub(crate) carry: HashMap<(String, String), i64>,
    pub(crate) next_ledger_seq: u64,
    pub(crate) next_seq: u64,
    pub(crate) next_generation: u64,
    /// Agent binaries being rolled out, one per platform.
    pub(crate) rollout: Option<RolloutRecord>,
    /// Nodes that refused or failed to install a release: the release's
    /// SHA-256 and why. Not offered that release again.
    pub(crate) upgrade_failures: HashMap<String, ([u8; 32], String)>,
    /// Nodes sent a release (its SHA-256) and when. They get no tasks
    /// until they come back; kept across the restart's disconnect.
    pub(crate) upgrading: HashMap<String, ([u8; 32], Instant)>,
}

/// The seed a deterministic task runs with. The same for every run of the
/// task, so honest nodes agree.
pub fn task_seed(job: JobId, index: u32) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/seed/v1\0");
    h.update(&job.0.to_le_bytes());
    h.update(&index.to_le_bytes());
    *h.finalize().as_bytes()
}

/// Outcomes that a deterministic task reproduces exactly, so a check run
/// can compare them. Timeouts and agent errors depend on the machine.
pub(crate) fn reproducible(o: &Outcome) -> bool {
    matches!(
        o,
        Outcome::Exited(_) | Outcome::OutOfFuel | Outcome::Trap(_)
    )
}

pub(crate) fn final_state(o: &Outcome) -> TaskState {
    match o {
        Outcome::Exited(0) => TaskState::Succeeded,
        Outcome::Cancelled => TaskState::Cancelled,
        other => TaskState::Failed(other.clone()),
    }
}

impl State {
    pub fn load(
        cluster_name: String,
        cluster_id: String,
        store: Store,
        key: SecretKey,
    ) -> Result<Self> {
        let snap = store.load()?;
        let mut state = Self {
            cluster_name,
            cluster_id,
            key,
            store,
            policy: snap.policy.unwrap_or_default(),
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
            accounts: snap
                .accounts
                .into_iter()
                .map(|a| (a.name.clone(), a))
                .collect(),
            holds: snap.holds.into_iter().map(|h| (h.task, h)).collect(),
            carry: snap.carry.into_iter().collect(),
            next_ledger_seq: snap.next_ledger_seq,
            next_seq: 0,
            next_generation: 1,
            rollout: None,
            upgrade_failures: HashMap::new(),
            upgrading: HashMap::new(),
        };
        state.rollout = state.load_rollout()?;
        state.convert_credit_receipts()?;
        state.ensure_account(OWNER_ACCOUNT)?;
        let mut jobs = snap.jobs;
        jobs.sort_by_key(|(m, _)| m.seq);
        for (meta, mut tasks) in jobs {
            state.next_seq = state.next_seq.max(meta.seq + 1);
            let mut changed = Vec::new();
            for (i, t) in tasks.iter_mut().enumerate() {
                let index = i as u32;
                // Nothing is connected yet, so anything "running" was
                // interrupted by the restart.
                if matches!(t.state, TaskState::Running { .. }) {
                    t.state = TaskState::Pending;
                    t.node = None;
                    changed.push(index);
                }
                if let Some(c) = t.check.as_mut()
                    && c.running_on.take().is_some()
                {
                    changed.push(index);
                }
                if meta.cancelled {
                    continue;
                }
                match t.state {
                    TaskState::Pending => state.queue.push_back(QueueItem {
                        job: meta.id,
                        index,
                        check: false,
                    }),
                    TaskState::Verifying => state.queue.push_back(QueueItem {
                        job: meta.id,
                        index,
                        check: true,
                    }),
                    _ => {}
                }
            }
            changed.dedup();
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

    pub fn policy(&self) -> Policy {
        self.policy
    }

    pub fn set_policy(&mut self, p: Policy) -> Result<()> {
        p.validate()?;
        self.store.put_policy(&p)?;
        self.policy = p;
        for (id, n) in self.nodes.iter_mut() {
            if let Some(m) = self.members.get(id) {
                n.suspended = !m.trusted() && m.strikes >= p.max_strikes;
            }
        }
        self.schedule();
        Ok(())
    }

    // -- membership ---------------------------------------------------------

    pub fn create_token(
        &mut self,
        ttl: Duration,
        uses: Option<u32>,
        invite: Option<Invite>,
    ) -> Result<TokenRecord> {
        if let Some(i) = &invite {
            validate_account(&i.account)?;
            if i.account == OWNER_ACCOUNT {
                bail!("`{OWNER_ACCOUNT}` is the cluster owner's account; pick the provider's name");
            }
        }
        let token = TokenRecord {
            secret: pumasi_proto::new_secret(),
            expires_unix: now_unix() + ttl.as_secs(),
            uses_left: uses,
            invite,
        };
        self.store.put_token(&token)?;
        self.tokens.push(token.clone());
        Ok(token)
    }

    /// Decides whether a connecting node may join, and as what. Known
    /// members are always allowed; new nodes need a valid, unexpired join
    /// secret, which also fixes their role and account.
    pub fn authorize(
        &mut self,
        id: &str,
        info: &NodeInfo,
        secret: Option<&Secret>,
    ) -> Result<(Role, String)> {
        if let Some(m) = self.members.get_mut(id) {
            if m.info != *info {
                m.info = info.clone();
                let m = m.clone();
                self.store.put_member(&m)?;
            }
            let m = &self.members[id];
            return Ok((m.role, m.account.clone()));
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
        let invite = token.invite.clone();
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
        let member = match invite {
            Some(i) => Member {
                id: id.to_string(),
                info: info.clone(),
                joined_unix: now,
                role: Role::Provider,
                account: i.account,
                allow_docker: i.allow_docker,
                checks_passed: 0,
                strikes: 0,
                perf: None,
                perf_check: None,
            },
            None => Member {
                id: id.to_string(),
                info: info.clone(),
                joined_unix: now,
                role: Role::Owner,
                account: OWNER_ACCOUNT.into(),
                allow_docker: true,
                checks_passed: 0,
                strikes: 0,
                perf: None,
                perf_check: None,
            },
        };
        self.ensure_account(&member.account)?;
        self.store.put_member(&member)?;
        info!(node = %id, name = %info.name, role = %member.role, account = %member.account, "new node joined");
        let out = (member.role, member.account.clone());
        self.members.insert(id.to_string(), member);
        Ok(out)
    }

    /// Removes a member. `id` may be a unique prefix, as `pumasi status`
    /// shows only the first characters.
    pub fn remove_node(&mut self, id: &str) -> Result<()> {
        let id = self.resolve_member(id)?;
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

    pub(crate) fn resolve_member(&self, id: &str) -> Result<String> {
        let id = id.trim();
        let matches: Vec<&String> = self
            .members
            .keys()
            .filter(|k| !id.is_empty() && k.starts_with(id))
            .collect();
        match matches.as_slice() {
            [one] => Ok((*one).clone()),
            [] => bail!("no such node"),
            _ => bail!("node id prefix {id:?} is ambiguous; give more characters"),
        }
    }

    /// Registers a live connection and returns its generation.
    #[allow(clippy::too_many_arguments)]
    pub fn on_connect(
        &mut self,
        id: &str,
        protocol: u32,
        info: NodeInfo,
        upgrade: Option<UpgradeHello>,
        availability: Availability,
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
        // Back from an upgrade's restart (with the new binary, or rolled
        // back: then the release is offered again and the agent says why
        // it failed).
        self.upgrading.remove(id);
        let generation = self.next_generation;
        self.next_generation += 1;
        let prev_completed = self.nodes.get(id).map(|n| n.completed).unwrap_or(0);
        if let Some(mut old) = self.nodes.remove(id)
            && let Some(kill) = old.kill.take()
        {
            let _ = kill.send(());
        }
        let (account, trusted, executors, suspended) = match self.members.get(id) {
            Some(m) => (
                m.account.clone(),
                m.trusted(),
                m.executors(&info.executors),
                !m.trusted() && m.strikes >= self.policy.max_strikes,
            ),
            None => (OWNER_ACCOUNT.into(), true, info.executors.clone(), false),
        };
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
                account,
                trusted,
                executors,
                suspended,
                upgrade,
                draining: false,
                paused: availability.paused_reason().map(String::from),
                perf: PerfRuntime {
                    protocol,
                    ..Default::default()
                },
            },
        );
        self.schedule();
        generation
    }

    /// The node's availability policy paused or resumed it. Running tasks
    /// stay where they are; a paused node only gets no new ones.
    pub fn on_availability(&mut self, id: &str, generation: u64, a: Availability) {
        let Some(n) = self.nodes.get_mut(id) else {
            return;
        };
        if n.generation != generation {
            return;
        }
        let paused = a.paused_reason().map(String::from);
        if n.paused == paused {
            return;
        }
        match &paused {
            Some(reason) => info!(node = %id, %reason, "node paused"),
            None => info!(node = %id, "node available again"),
        }
        n.paused = paused;
        self.schedule();
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
        node.perf.challenge = None;
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

    /// Periodic housekeeping: drop silent nodes, reclaim hung tasks,
    /// release held earnings.
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
        self.release_holds(now_unix());
        self.expire_upgrades();
        self.expire_challenges();
        if !self.queue.is_empty() || self.rollout.is_some() {
            self.schedule();
        }
    }

    // -- jobs -----------------------------------------------------------------

    pub fn submit(&mut self, mut sub: JobSubmission) -> Result<JobId> {
        sub.validate()?;
        let account = sub.account.take().unwrap_or_else(|| OWNER_ACCOUNT.into());
        self.ensure_account(&account)?;
        let mut blobs: Vec<(ModuleHash, Vec<u8>)> = Vec::new();
        let module_hash = match sub.executor {
            Executor::Wasm => {
                let h = pumasi_proto::module_hash(&sub.module);
                blobs.push((h, std::mem::take(&mut sub.module)));
                Some(h)
            }
            Executor::Docker | Executor::Microvm => None,
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
            account,
            deterministic: sub.deterministic,
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
                check: None,
                cost: 0,
            })
            .collect();
        let blob_refs: Vec<(ModuleHash, &[u8])> =
            blobs.iter().map(|(h, b)| (*h, b.as_slice())).collect();
        self.store.put_new_job(&meta, &tasks, &blob_refs)?;
        for (h, b) in blobs {
            self.modules.insert(h, Arc::new(b));
        }
        for i in 0..tasks.len() {
            self.queue.push_back(QueueItem {
                job: id,
                index: i as u32,
                check: false,
            });
        }
        info!(job = %id, name = %meta.name, tasks = tasks.len(), account = %meta.account, "job submitted");
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
                        check: false,
                    };
                    if let Some(tx) = self.nodes.get(node).and_then(|n| n.tx.as_ref()) {
                        let _ = tx.send(ToAgent::Cancel(r));
                    }
                }
                TaskState::Verifying => {
                    // Keep the first result; the check is dropped.
                    if let Some(c) = t.check.as_mut() {
                        if let Some(node) = c.running_on.take() {
                            let r = TaskRef {
                                job: id,
                                index: i as u32,
                                attempt: c.attempts,
                                check: true,
                            };
                            if let Some(tx) = self.nodes.get(&node).and_then(|n| n.tx.as_ref()) {
                                let _ = tx.send(ToAgent::Cancel(r));
                            }
                        }
                        c.status = Verification::Skipped("job cancelled".into());
                        t.state = final_state(&c.runs[0].outcome);
                    }
                    changed.push(i as u32);
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
        self.queue.retain(|q| q.job != id);
        info!(job = %id, "job cancelled");
        Ok(())
    }

    /// Applies a task report from a node: checks and countersigns its
    /// usage receipt, settles the charge, records the result and decides
    /// whether to check it.
    pub fn on_report(&mut self, node_id: &str, generation: u64, report: TaskReport) {
        let Some(node) = self.nodes.get_mut(node_id) else {
            return;
        };
        if node.generation != generation {
            return;
        }
        let Some(placed) = node.running.remove(&report.task) else {
            // Stale: the task was already rescheduled elsewhere.
            return;
        };
        node.completed += 1;
        let observed_ms = placed.elapsed().as_millis() as u64;
        let r = report.task;
        let Some(job) = self.jobs.get(&r.job) else {
            return;
        };
        let Some(task) = job.tasks.get(r.index as usize) else {
            return;
        };
        let current = if r.check {
            task.check.as_ref().map(|c| c.attempts) == Some(r.attempt)
        } else {
            task.attempts == r.attempt
        };
        if !current {
            return;
        }

        // The receipt must be the node's own, for this run, and describe
        // exactly the result it sent.
        let digest = result_digest(&report.outcome, &report.stdout, &report.files);
        let receipt_check = self.check_receipt(node_id, &job.meta, &report, &digest);
        if let Err(e) = receipt_check {
            warn!(node = %node_id, task = %r, "rejected report: {e:#}");
            self.strike(node_id, &format!("invalid receipt: {e:#}"));
            self.requeue_run(r, "invalid receipt");
            self.schedule();
            return;
        }
        if let Some(signed) = report.usage.clone() {
            self.settle_run(node_id, signed, observed_ms);
        }

        if r.check {
            self.on_check_report(node_id, report, digest);
        } else {
            self.on_primary_report(node_id, report, digest);
        }
        self.schedule();
    }

    fn check_receipt(
        &self,
        node_id: &str,
        meta: &JobMeta,
        report: &TaskReport,
        digest: &[u8; 32],
    ) -> Result<()> {
        let Some(signed) = &report.usage else {
            if matches!(report.outcome, Outcome::Error(_)) {
                return Ok(());
            }
            bail!("report has no usage receipt");
        };
        let node: PublicKey = node_id.parse().context("bad node id")?;
        check_usage(signed, &node, &self.key.public(), report.task, digest)?;
        let u = &signed.usage;
        anyhow::ensure!(
            u.executor == meta.executor,
            "receipt names another executor"
        );
        anyhow::ensure!(
            u.fuel_used == report.fuel_used && u.wall_ms == report.wall_ms,
            "receipt numbers differ from the report"
        );
        anyhow::ensure!(
            u.memory_limit_bytes == meta.limits.memory_bytes
                && u.cpu_millis == meta.limits.cpu_millis,
            "receipt limits differ from the job"
        );
        if let Some(limit) = meta.limits.fuel {
            anyhow::ensure!(u.fuel_used <= limit, "fuel above the job's limit");
        }
        Ok(())
    }

    /// Countersigns a checked receipt, stores it and moves the money.
    fn settle_run(
        &mut self,
        node_id: &str,
        signed: pumasi_proto::receipt::SignedUsage,
        observed_ms: u64,
    ) {
        let r = signed.usage.task;
        let Some(job) = self.jobs.get(&r.job) else {
            return;
        };
        let provider = self
            .nodes
            .get(node_id)
            .map(|n| n.account.clone())
            .unwrap_or_else(|| OWNER_ACCOUNT.into());
        let consumer = job.meta.account.clone();
        let billed_wall_ms = signed.usage.wall_ms.min(observed_ms);
        let cpu_score = self.billing_score_of(node_id);
        let exact_ucu = if provider == consumer {
            0
        } else {
            self.policy.rates.charge(&Metered {
                executor: signed.usage.executor,
                fuel_used: signed.usage.fuel_used,
                wall_ms: billed_wall_ms,
                memory_bytes: signed.usage.memory_limit_bytes,
                cpu_millis: signed.usage.cpu_millis,
                cpu_score,
            })
        };
        let amount = self.carry_forward(&consumer, &provider, exact_ucu);
        if signed.usage.executor == Executor::Wasm {
            self.note_run_speed(node_id, signed.usage.fuel_used, billed_wall_ms);
        }
        let billing = Billing {
            consumer,
            provider,
            billed_wall_ms,
            cpu_score,
            exact_ucu,
            amount,
            check: r.check,
            recorded_unix: now_unix(),
        };
        let receipt = Receipt::countersign(signed, billing, &self.key);
        if let Some(task) = self
            .jobs
            .get_mut(&r.job)
            .and_then(|j| j.tasks.get_mut(r.index as usize))
        {
            task.cost += amount;
        }
        self.charge(&receipt);
    }

    fn on_primary_report(&mut self, node_id: &str, report: TaskReport, digest: [u8; 32]) {
        let r = report.task;
        let (trusted, account, version, passes, strikes) = self.node_record(node_id);
        let rate = self.policy.sample_permille(passes, strikes);
        let Some(job) = self.jobs.get_mut(&r.job) else {
            return;
        };
        let sample = job.meta.deterministic
            && job.meta.executor == Executor::Wasm
            && !job.meta.cancelled
            && !trusted
            && account != job.meta.account
            && reproducible(&report.outcome)
            && rand::random_range(0..1000) < rate;
        let Some(task) = job.tasks.get_mut(r.index as usize) else {
            return;
        };
        task.fuel_used = report.fuel_used;
        task.wall_ms = report.wall_ms;
        task.stdout = report.stdout;
        task.stderr = report.stderr;
        task.output_files = report.files;
        if sample {
            task.state = TaskState::Verifying;
            task.check = Some(CheckRecord {
                runs: vec![RunRecord {
                    task: r,
                    node: node_id.to_string(),
                    account,
                    trusted,
                    agent_version: version,
                    outcome: report.outcome,
                    digest,
                    fuel_used: report.fuel_used,
                    output: None,
                }],
                attempts: 0,
                running_on: None,
                status: Verification::Pending,
            });
            self.queue.push_back(QueueItem {
                job: r.job,
                index: r.index,
                check: true,
            });
            info!(task = %r, node = %node_id, "result picked for a check run");
        } else {
            task.state = final_state(&report.outcome);
        }
        if let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)]) {
            warn!("persist task: {e:#}");
        }
    }

    pub(crate) fn node_record(&self, node_id: &str) -> (bool, String, String, u32, u32) {
        let version = self
            .nodes
            .get(node_id)
            .map(|n| n.info.agent_version.clone())
            .unwrap_or_default();
        match self.members.get(node_id) {
            Some(m) => (
                m.trusted(),
                m.account.clone(),
                version,
                m.checks_passed,
                m.strikes,
            ),
            None => (true, OWNER_ACCOUNT.into(), version, 0, 0),
        }
    }

    /// Records a failed check (or a forged receipt) against a node.
    pub(crate) fn strike(&mut self, node_id: &str, reason: &str) {
        let max = self.policy.max_strikes;
        let Some(m) = self.members.get_mut(node_id) else {
            return;
        };
        if m.trusted() {
            warn!(node = %node_id, "owner machine: {reason}");
            return;
        }
        m.strikes += 1;
        let suspended = m.strikes >= max;
        warn!(node = %node_id, strikes = m.strikes, "strike: {reason}");
        let m = m.clone();
        if let Err(e) = self.store.put_member(&m) {
            warn!("persist member: {e:#}");
        }
        if suspended && let Some(n) = self.nodes.get_mut(node_id) {
            if !n.suspended {
                warn!(node = %node_id, "too many failed checks; node suspended");
            }
            n.suspended = true;
        }
    }

    pub(crate) fn credit_pass(&mut self, node_id: &str) {
        if let Some(m) = self.members.get_mut(node_id)
            && !m.trusted()
        {
            m.checks_passed += 1;
            let m = m.clone();
            if let Err(e) = self.store.put_member(&m) {
                warn!("persist member: {e:#}");
            }
        }
    }

    /// Puts a run back in the queue after its node vanished or misbehaved.
    fn requeue_run(&mut self, r: TaskRef, reason: &str) {
        if r.check {
            self.requeue_check(r, reason);
        } else {
            self.requeue(r, reason);
        }
    }

    fn requeue(&mut self, r: TaskRef, reason: &str) {
        if r.check {
            return self.requeue_check(r, reason);
        }
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
            self.queue.push_front(QueueItem {
                job: r.job,
                index: r.index,
                check: false,
            });
        }
        if let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)]) {
            warn!("persist task: {e:#}");
        }
    }

    /// Places as many queued runs as free slots allow.
    pub fn schedule(&mut self) {
        // Idle nodes due for an upgrade get it before any new task, and
        // idle nodes due for a benchmark check get that.
        self.drive_rollout();
        self.drive_challenges();
        let mut skipped = VecDeque::new();
        let mut dirty: Vec<(JobId, u32)> = Vec::new();
        while let Some(item) = self.queue.pop_front() {
            let Some(job) = self.jobs.get_mut(&item.job) else {
                continue;
            };
            let Some(task) = job.tasks.get_mut(item.index as usize) else {
                continue;
            };
            // Nodes (and accounts) that already ran this task are out for a
            // check run.
            let mut exclude_nodes: Vec<&str> = Vec::new();
            let mut exclude_accounts: Vec<&str> = Vec::new();
            if item.check {
                let Some(check) = task.check.as_ref() else {
                    continue;
                };
                if task.state != TaskState::Verifying
                    || check.status != Verification::Pending
                    || check.running_on.is_some()
                {
                    continue;
                }
                for run in &check.runs {
                    exclude_nodes.push(&run.node);
                    exclude_accounts.push(&run.account);
                }
            } else if task.state != TaskState::Pending {
                continue;
            }
            let need = job.meta.limits.memory_bytes;
            let executor = job.meta.executor;
            // Image jobs get their CPUs (a WASM task uses one): a node
            // offers as many as it reports, which its CPU cap lowers.
            let need_cpus = match executor {
                Executor::Wasm => 1,
                _ => vcpus(job.meta.limits.cpu_millis),
            };
            let consumer = job.meta.account.as_str();
            let floor = self.policy.ledger.floor();
            let can_pay = self
                .accounts
                .get(consumer)
                .is_some_and(|a| a.balance > floor);
            // Free (same account) and trusted nodes first, then most free
            // slots to spread work.
            let best = self
                .nodes
                .iter_mut()
                .filter(|(id, n)| {
                    n.online
                        && !n.suspended
                        && !n.draining
                        && n.paused.is_none()
                        && !n.perf.wanted
                        && n.perf.challenge.is_none()
                        && !self.upgrading.contains_key(id.as_str())
                        && n.free_slots() > 0
                        && n.info.memory_bytes >= need
                        && n.info.cpus >= need_cpus
                        && n.executors.contains(&executor)
                        && (n.account == consumer || can_pay)
                        && !exclude_nodes.contains(&id.as_str())
                        && !exclude_accounts.contains(&n.account.as_str())
                })
                .max_by_key(|(_, n)| {
                    let pref = if item.check {
                        n.trusted
                    } else {
                        n.account == consumer
                    };
                    (pref, n.free_slots())
                });
            let Some((node_id, node)) = best else {
                skipped.push_back(item);
                if !self
                    .nodes
                    .values()
                    .any(|n| n.online && n.paused.is_none() && n.free_slots() > 0)
                {
                    break;
                }
                continue;
            };
            let Some(tx) = &node.tx else {
                skipped.push_back(item);
                continue;
            };
            let attempt = if item.check {
                task.check.as_ref().map(|c| c.attempts + 1).unwrap_or(1)
            } else {
                task.attempts + 1
            };
            let r = TaskRef {
                job: item.job,
                index: item.index,
                attempt,
                check: item.check,
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
                seed: job
                    .meta
                    .deterministic
                    .then(|| task_seed(item.job, item.index)),
            };
            if tx.send(ToAgent::Assign(Box::new(assignment))).is_err() {
                skipped.push_back(item);
                continue;
            }
            node.sent_modules.extend(module_hash);
            node.sent_modules.extend(files_hash);
            node.running.insert(r, Instant::now());
            if item.check {
                if let Some(c) = task.check.as_mut() {
                    c.attempts = attempt;
                    c.running_on = Some(node_id.clone());
                }
            } else {
                task.attempts = attempt;
                task.state = TaskState::Running {
                    node: node_id.clone(),
                };
                task.node = Some(node_id.clone());
            }
            dirty.push((item.job, item.index));
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
            account: job.meta.account.clone(),
            cost: job.tasks.iter().map(|t| t.cost).sum(),
        };
        let mut cancelled = 0;
        for t in &job.tasks {
            match t.state {
                TaskState::Succeeded => s.succeeded += 1,
                TaskState::Failed(_) => s.failed += 1,
                TaskState::Running { .. } | TaskState::Verifying => s.running += 1,
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
                let info = rt.map(|n| n.info.clone()).unwrap_or_else(|| m.info.clone());
                let (cpu_score, perf_note) = cpu_score(m);
                let online = rt.filter(|n| n.online);
                NodeView {
                    id: m.id.clone(),
                    executors: m.executors(&info.executors),
                    info,
                    role: m.role,
                    account: m.account.clone(),
                    checks_passed: m.checks_passed,
                    strikes: m.strikes,
                    suspended: !m.trusted() && m.strikes >= self.policy.max_strikes,
                    online: rt.is_some_and(|n| n.online),
                    running: rt.map(|n| n.running.len() as u32).unwrap_or(0),
                    completed: rt.map(|n| n.completed).unwrap_or(0),
                    last_seen_unix: rt.map(|n| n.last_seen_unix).unwrap_or(0),
                    paused: online.and_then(|n| n.paused.clone()),
                    perf: m.perf.as_ref().map(|p| p.profile.clone()),
                    cpu_score,
                    perf_note,
                    rtt_us: online.and_then(|n| n.perf.rtt_us),
                    relayed: online.is_some_and(|n| n.perf.relayed),
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
                verification: t
                    .check
                    .as_ref()
                    .map(|c| c.status.clone())
                    .unwrap_or(Verification::NotChecked),
                cost: t.cost,
            })
            .collect();
        Some(JobView {
            summary: Self::summary(job),
            executor: job.meta.executor,
            image: job.meta.docker.as_ref().map(|d| d.image.clone()),
            limits: job.meta.limits,
            total_fuel: job.tasks.iter().map(|t| t.fuel_used).sum(),
            deterministic: job.meta.deterministic,
            tasks,
        })
    }

    pub fn receipts(&self, id: JobId) -> Result<Vec<Receipt>> {
        anyhow::ensure!(self.jobs.contains_key(&id), "no such job");
        self.store.receipts(id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> State {
        let path =
            std::env::temp_dir().join(format!("pumasi-state-{}.redb", rand::random::<u64>()));
        let key = SecretKey::generate();
        State::load(
            "t".into(),
            key.public().to_string(),
            Store::open(&path).unwrap(),
            key,
        )
        .unwrap()
    }

    fn info(name: &str, executors: Vec<Executor>) -> NodeInfo {
        NodeInfo {
            name: name.into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            cpus: 4,
            memory_bytes: 8 << 30,
            slots: 2,
            agent_version: "test".into(),
            executors,
        }
    }

    /// Joins a node with a fresh token and connects it.
    fn join(
        st: &mut State,
        name: &str,
        executors: Vec<Executor>,
        invite: Option<Invite>,
    ) -> (String, mpsc::UnboundedReceiver<ToAgent>) {
        let id = SecretKey::generate().public().to_string();
        let token = st
            .create_token(Duration::from_secs(60), Some(1), invite)
            .unwrap();
        let info = info(name, executors);
        st.authorize(&id, &info, Some(&token.secret)).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let (kill, _) = oneshot::channel();
        st.on_connect(
            &id,
            pumasi_proto::PROTOCOL_VERSION,
            info,
            None,
            Availability::Available,
            tx,
            kill,
        );
        (id, rx)
    }

    fn job(executor: Executor, tasks: usize) -> JobSubmission {
        JobSubmission {
            name: "j".into(),
            executor,
            module: match executor {
                Executor::Wasm => b"\0asm\x01\0\0\0".to_vec(),
                Executor::Docker | Executor::Microvm => vec![],
            },
            docker: executor.runs_images().then(|| DockerSpec {
                image: "busybox".into(),
                ..Default::default()
            }),
            files: vec![],
            tasks: vec![TaskInput::default(); tasks],
            limits: Limits::default(),
            tier: Tier::Open,
            max_attempts: 3,
            max_price_per_hour: None,
            account: None,
            deterministic: executor == Executor::Wasm,
        }
    }

    fn assigned(rx: &mut mpsc::UnboundedReceiver<ToAgent>) -> Vec<TaskAssignment> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let ToAgent::Assign(a) = m {
                out.push(*a);
            }
        }
        out
    }

    fn bob(allow_docker: bool) -> Option<Invite> {
        Some(Invite {
            account: "bob".into(),
            allow_docker,
        })
    }

    #[test]
    fn provider_machines_get_wasm_only_unless_invited_for_docker() {
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        // Bob's machine offers docker, but the invite did not allow it.
        let (bob_id, mut bob_rx) = join(
            &mut st,
            "bob",
            vec![Executor::Wasm, Executor::Docker],
            bob(false),
        );
        let view = st
            .status()
            .nodes
            .into_iter()
            .find(|n| n.id == bob_id)
            .unwrap();
        assert_eq!(view.role, Role::Provider);
        assert_eq!(view.executors, vec![Executor::Wasm]);

        let id = st.submit(job(Executor::Docker, 1)).unwrap();
        assert!(
            assigned(&mut bob_rx).is_empty(),
            "docker task sent to a provider"
        );
        assert_eq!(st.jobs[&id].tasks[0].state, TaskState::Pending);

        // An owner machine with docker takes it.
        let (_, mut own_rx) = join(&mut st, "own", vec![Executor::Wasm, Executor::Docker], None);
        let got = assigned(&mut own_rx);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].executor, Executor::Docker);
        assert_eq!(got[0].seed, None, "docker tasks are not deterministic");

        // Wasm tasks do go to bob, deterministic with a per-task seed.
        st.submit(job(Executor::Wasm, 1)).unwrap();
        let mut got = assigned(&mut bob_rx);
        got.extend(assigned(&mut own_rx));
        assert_eq!(got.len(), 1);
        assert!(got[0].seed.is_some());

        // An invite can allow docker explicitly.
        let mut st = state();
        let (_, mut rx) = join(
            &mut st,
            "carol",
            vec![Executor::Wasm, Executor::Docker],
            bob(true),
        );
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        st.submit(job(Executor::Docker, 1)).unwrap();
        assert_eq!(assigned(&mut rx).len(), 1);
    }

    #[test]
    fn providers_get_microvm_jobs_without_a_docker_invite() {
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        let (bob_id, mut bob_rx) = join(
            &mut st,
            "bob",
            vec![Executor::Wasm, Executor::Docker, Executor::Microvm],
            bob(false),
        );
        let view = st
            .status()
            .nodes
            .into_iter()
            .find(|n| n.id == bob_id)
            .unwrap();
        assert_eq!(view.executors, vec![Executor::Wasm, Executor::Microvm]);
        st.submit(job(Executor::Docker, 1)).unwrap();
        assert!(assigned(&mut bob_rx).is_empty(), "docker needs the invite");
        st.submit(job(Executor::Microvm, 2)).unwrap();
        let got = assigned(&mut bob_rx);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|a| a.executor == Executor::Microvm));
        assert_eq!(got[0].docker.as_ref().unwrap().image, "busybox");
        assert_eq!(got[0].seed, None, "microvm tasks are not deterministic");
        // A machine that does not offer microVMs never gets them.
        let mut st = state();
        let (_, mut rx) = join(&mut st, "own", vec![Executor::Wasm, Executor::Docker], None);
        st.submit(job(Executor::Microvm, 1)).unwrap();
        assert!(assigned(&mut rx).is_empty());
    }

    #[test]
    fn provider_work_needs_credit_and_own_machines_come_first() {
        let mut st = state();
        let (_, mut bob_rx) = join(&mut st, "bob", vec![Executor::Wasm], bob(false));
        st.submit(job(Executor::Wasm, 3)).unwrap();
        assert!(
            assigned(&mut bob_rx).is_empty(),
            "no credit, no provider work"
        );

        // Bob can run his own account's jobs for free.
        let mut own = job(Executor::Wasm, 1);
        own.account = Some("bob".into());
        st.submit(own).unwrap();
        assert_eq!(assigned(&mut bob_rx).len(), 1);

        // An owner machine with free slots is preferred over paying bob.
        let (_, mut own_rx) = join(&mut st, "own", vec![Executor::Wasm], None);
        assert_eq!(assigned(&mut own_rx).len(), 2);
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        let to_bob = assigned(&mut bob_rx);
        assert_eq!(
            to_bob.len(),
            1,
            "the third task overflows to bob once there is credit"
        );
        assert!(!to_bob[0].task.check);
    }
}
