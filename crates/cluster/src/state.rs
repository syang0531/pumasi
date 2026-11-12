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
    DEVICES_PROTOCOL, ModuleHash, STORE_PROTOCOL, Secret,
    economy::{Metered, MilliCu, Policy, validate_account, vcpus},
    gpu::{GpuRequest, GpuVendor},
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput, Tier},
    manage::{LabelSet, Selector, Taint, Toleration},
    monitor::EventLevel,
    msg::{
        AccountView, Availability, ClusterStatus, Invite, JobState, JobSummary, JobView, NodeInfo,
        NodeView, OWNER_ACCOUNT, Outcome, Role, TaskAssignment, TaskRef, TaskReport, TaskState,
        TaskView, ToAgent, UpgradeHello, Verification,
    },
    perf::SignedPerf,
    receipt::{Billing, Receipt, check_usage, output_digest},
    secrets_eq,
    store::{BlobHash, ImageBlob, OutputMode, StoredOutput, format_size},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub use crate::datasets::{DatasetRecord, ImageState, JobExt, TaskExt, UploadRecord};
use crate::{
    datasets::{FileState, oci_arch_of},
    gpu::{DeviceRecord, GpuRuntime},
    manage::{JobSched, Mgmt, MgmtSnapshot, sched_of},
    monitor::Monitor,
    perf::{PerfCheck, PerfRuntime, cpu_score},
    rollout::RolloutRecord,
    store::Store,
};

/// Extra time past a task's own timeout before the control plane assumes
/// the node hung and reschedules the task elsewhere.
const LEASE_GRACE: Duration = Duration::from_secs(30);
/// Extra lease for a run on a node that uses the file store (uploading a
/// large output), plus one second per this many bytes it mounts.
const STORE_LEASE_EXTRA: Duration = Duration::from_secs(600);
const STORE_LEASE_RATE: u64 = 4 << 20;

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

impl TokenRecord {
    pub fn view(&self) -> pumasi_proto::msg::JoinTokenView {
        pumasi_proto::msg::JoinTokenView {
            id: pumasi_proto::msg::join_token_id(&self.secret),
            expires_unix: self.expires_unix,
            uses_left: self.uses_left,
            invite: self.invite.clone(),
        }
    }
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
    /// File store details, persisted in their own table.
    #[serde(skip)]
    pub ext: JobExt,
    /// The GPUs each task needs, persisted in their own table.
    #[serde(skip)]
    pub gpu: Option<GpuRequest>,
    /// Priority, selector, tolerations and dependencies, persisted in
    /// their own table.
    #[serde(skip)]
    pub sched: JobSched,
    /// Who submitted it and the rest of phase 15's job details, persisted
    /// in their own table.
    #[serde(skip)]
    pub origin: crate::remote::JobOrigin,
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
    /// File store details, persisted in their own table.
    #[serde(skip)]
    pub ext: TaskExt,
    /// How long the parts of its run took (1.4), persisted in their own
    /// table.
    #[serde(skip)]
    pub times: TaskTimes,
}

/// How long the parts of a task's run took: of the run that counts (the
/// last one placed). `wall_ms` is in the task record itself. The layout
/// is stored: new figures go in a new table, not here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTimes {
    /// In the queue before the run was placed on a node.
    pub queued_ms: u64,
    /// As the node reported them ([`pumasi_proto::msg::TaskTiming`]); 0
    /// from agents older than 1.4.
    pub fetch_ms: u64,
    pub pull_ms: u64,
    pub upload_ms: u64,
}

impl TaskTimes {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
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
    /// GPUs in use and GPU checks.
    pub gpu: GpuRuntime,
    /// Executors its owner allowed that cannot run now, and why (protocol
    /// 13 agents report them).
    pub unavailable: Vec<(Executor, String)>,
    /// Its Docker GPU tasks can draw.
    pub gpu_graphics: bool,
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
    /// The job's priority: the queue is ordered by it, highest first.
    pub priority: i32,
    /// When it entered the queue (after a restart of the control plane,
    /// when that started).
    pub since: Instant,
}

/// What a queued run needs from a node.
pub(crate) struct Need {
    pub item: QueueItem,
    pub executor: Executor,
    pub memory: u64,
    pub cpus: u32,
    pub store_job: bool,
    pub gpu: Option<GpuRequest>,
    pub vm_network: bool,
    /// Private image jobs: the architectures it is ready for.
    pub images: Option<Vec<(String, ImageBlob)>>,
    pub consumer: String,
    pub can_pay: bool,
    pub exclude_nodes: Vec<String>,
    pub exclude_accounts: Vec<String>,
    pub selector: Selector,
    pub tolerations: Vec<Toleration>,
    /// Its GPU tasks ask for driver capabilities (protocol 13 agents).
    pub gpu_caps: bool,
    /// Its GPU tasks draw: only nodes whose Docker GPUs can.
    pub gpu_graphics: bool,
    /// `tier = "trusted"`: only the owner's machines and trusted nodes.
    pub trusted_only: bool,
    /// `tier = "confidential"`: only TEE nodes, once sealed (phase 16).
    pub confidential: Option<crate::confidential::ConfNeed>,
}

/// Why a node cannot take a run, for `queue_timeout` failures and the
/// status's capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal<'a> {
    Offline,
    Suspended,
    Upgrading,
    Paused(&'a str),
    Benchmarking,
    Cordoned,
    /// It does not run this executor; why, when its owner allowed it.
    Executor(Executor, Option<&'a str>),
    RanItAlready,
    CannotPay(&'a str),
    Memory(u64),
    Cpus(u32),
    OldAgent,
    ImageArch,
    VmNetwork,
    Taint(&'a Taint),
    Selector(&'a Selector),
    GpuGraphics,
    GpuCheck,
    Gpu,
    Full,
    /// `tier = "trusted"` on a provider's node no admin trusts.
    Untrusted,
    /// No confidential VMs here, and why when its owner turned them on.
    Tee(Option<&'a str>),
    /// The confidential image or input is not sealed yet.
    Sealing(&'static str),
}

impl std::fmt::Display for Refusal<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Offline => f.write_str("offline"),
            Refusal::Suspended => f.write_str("suspended after failed checks"),
            Refusal::Upgrading => f.write_str("upgrading"),
            Refusal::Paused(why) => write!(f, "paused: {why}"),
            Refusal::Benchmarking => f.write_str("being benchmarked"),
            Refusal::Cordoned => f.write_str("cordoned"),
            Refusal::Executor(e, Some(why)) => write!(f, "{e} unavailable: {why}"),
            Refusal::Executor(e, None) => write!(f, "does not run {e} jobs"),
            Refusal::RanItAlready => f.write_str("ran the task being checked"),
            Refusal::CannotPay(a) => write!(
                f,
                "account {a} has no CU left (or used its daily CU) for other accounts' machines"
            ),
            Refusal::Memory(m) => write!(f, "less than {} of memory", format_size(*m)),
            Refusal::Cpus(c) => write!(f, "fewer than {c} CPUs"),
            Refusal::OldAgent => f.write_str("its agent is too old for this job (upgrade it)"),
            Refusal::ImageArch => f.write_str("the private image is not ready for its architecture"),
            Refusal::VmNetwork => f.write_str("its microVMs cannot have a network"),
            Refusal::Taint(t) => write!(f, "taint {t} not tolerated"),
            Refusal::Selector(s) => write!(f, "does not match the selector {s}"),
            Refusal::GpuGraphics => f.write_str(
                "its Docker GPUs cannot draw (no gpu.graphics; see `pumasi gpu check --docker-run`)",
            ),
            Refusal::GpuCheck => f.write_str("its GPUs are being checked"),
            Refusal::Gpu => f.write_str("no free GPU that matches"),
            Refusal::Full => f.write_str("every slot busy"),
            Refusal::Untrusted => f.write_str(
                "not a trusted host for tier = \"trusted\" (a provider's machine; `pumasi trust NODE` lets it)",
            ),
            Refusal::Tee(Some(why)) => write!(f, "confidential VMs unavailable: {why}"),
            Refusal::Tee(None) => f.write_str("does not start confidential VMs (SEV-SNP or TDX)"),
            Refusal::Sealing(what) => f.write_str(what),
        }
    }
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
    /// Datasets, quotas, secrets and the rest of the file store.
    pub(crate) files: FileState,
    /// Nodes' device profiles (GPUs) and the cluster's GPU checks.
    pub(crate) devices: HashMap<String, DeviceRecord>,
    /// Nodes' signed WebGPU adapters (phase 14).
    pub(crate) webgpu: HashMap<String, pumasi_proto::webgpu::SignedWebGpu>,
    /// Labels, quotas, access tickets, schedules and the rest of cluster
    /// management.
    pub(crate) mgmt: Mgmt,
    /// Metrics, events, live output and alerts.
    pub(crate) monitor: Monitor,
    /// TEE nodes, leases, sealed inputs and measurements (phase 16).
    pub(crate) conf: crate::confidential::ConfState,
    /// The audit log's next number and newest hash (phase 17).
    pub(crate) audit_tip: crate::audit::AuditTip,
    /// SSO settings and sign-ins under way (phase 17).
    pub(crate) sso: crate::sso::SsoState,
    /// Display names an admin gave nodes (1.3), by node id.
    pub(crate) names: std::collections::BTreeMap<String, crate::names::NameOverride>,
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
            files: FileState::default(),
            devices: snap.devices.into_iter().collect(),
            webgpu: snap.webgpu.into_iter().collect(),
            mgmt: Mgmt::default(),
            monitor: Monitor::default(),
            conf: Default::default(),
            audit_tip: Default::default(),
            sso: Default::default(),
            names: Default::default(),
        };
        let mut job_sched: HashMap<u128, JobSched> = snap.job_sched.into_iter().collect();
        let mut job_gpu: HashMap<u128, GpuRequest> = snap.job_gpu.into_iter().collect();
        let mut job_ext: HashMap<u128, JobExt> = snap.job_ext.into_iter().collect();
        let mut task_ext: HashMap<(u128, u32), TaskExt> = snap.task_ext.into_iter().collect();
        let mut task_times: HashMap<(u128, u32), TaskTimes> = snap.task_times.into_iter().collect();
        let loaded = Instant::now();
        let mut job_origin: HashMap<u128, crate::remote::JobOrigin> =
            snap.job_origin.into_iter().collect();
        state.rollout = state.load_rollout()?;
        state.names = state.load_node_names()?;
        state.convert_credit_receipts()?;
        state.ensure_account(OWNER_ACCOUNT)?;
        let mut jobs = snap.jobs;
        jobs.sort_by_key(|(m, _)| m.seq);
        for (mut meta, mut tasks) in jobs {
            state.next_seq = state.next_seq.max(meta.seq + 1);
            meta.ext = job_ext.remove(&meta.id.0).unwrap_or_default();
            meta.sched = job_sched.remove(&meta.id.0).unwrap_or_default();
            meta.origin = job_origin.remove(&meta.id.0).unwrap_or_default();
            meta.gpu = job_gpu.remove(&meta.id.0).or_else(|| {
                // Jobs from before phase 9 passed `--gpus` to docker: that
                // was every NVIDIA GPU of the node.
                meta.docker
                    .as_ref()
                    .and_then(|d| d.gpus.as_ref())
                    .map(|_| GpuRequest {
                        count: 0,
                        vendor: Some(GpuVendor::Nvidia),
                        min_vram_mib: 0,
                    })
            });
            if let Some(d) = meta.docker.as_mut() {
                d.gpus = None;
            }
            for (i, t) in tasks.iter_mut().enumerate() {
                t.ext = task_ext.remove(&(meta.id.0, i as u32)).unwrap_or_default();
                t.times = task_times
                    .remove(&(meta.id.0, i as u32))
                    .unwrap_or_default();
            }
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
                        priority: meta.sched.priority,
                        since: loaded,
                    }),
                    TaskState::Verifying => state.queue.push_back(QueueItem {
                        job: meta.id,
                        index,
                        check: true,
                        priority: meta.sched.priority,
                        since: loaded,
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
        // Highest priority first; submission order within a priority.
        state
            .queue
            .make_contiguous()
            .sort_by_key(|q| std::cmp::Reverse(q.priority));
        state.load_files(snap.datasets, snap.quotas, snap.secrets, snap.uploads)?;
        state.load_mgmt(MgmtSnapshot {
            node_config: snap.node_config,
            account_quotas: snap.account_quotas,
            preempted: snap.preempted,
            access: snap.access,
            schedules: snap.schedules,
            account_policies: snap.account_policies,
        })?;
        state.load_monitor(Duration::from_secs(7 * 86_400))?;
        state.load_conf(snap.measurements, snap.trusted_nodes);
        state.load_audit()?;
        state.load_sso()?;
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

    /// The join tickets and invites that can still be used, soonest to
    /// expire first.
    pub fn list_tokens(&self) -> Vec<pumasi_proto::msg::JoinTokenView> {
        let now = now_unix();
        let mut v: Vec<_> = self
            .tokens
            .iter()
            .filter(|t| t.expires_unix > now)
            .map(TokenRecord::view)
            .collect();
        v.sort_by(|a, b| a.expires_unix.cmp(&b.expires_unix).then(a.id.cmp(&b.id)));
        v
    }

    /// Revokes the join ticket whose id starts with `id`, or the ticket
    /// `id` itself. Returns it.
    pub fn revoke_token(&mut self, id: &str) -> Result<pumasi_proto::msg::JoinTokenView> {
        let id = id.trim();
        anyhow::ensure!(!id.is_empty(), "which ticket?");
        let hits: Vec<usize> = match id.parse::<pumasi_proto::ticket::JoinTicket>() {
            Ok(t) => (0..self.tokens.len())
                .filter(|&i| secrets_eq(&self.tokens[i].secret, &t.secret))
                .collect(),
            Err(_) => (0..self.tokens.len())
                .filter(|&i| {
                    pumasi_proto::msg::join_token_id(&self.tokens[i].secret).starts_with(id)
                })
                .collect(),
        };
        anyhow::ensure!(
            hits.len() < 2,
            "ticket id {id:?} is ambiguous; give more characters"
        );
        let Some(&i) = hits.first() else {
            bail!(
                "no join ticket {id:?} (`pumasi token ls` lists them; used-up and expired ones are gone)"
            );
        };
        let t = self.tokens.remove(i);
        self.store.remove_token(&t.secret)?;
        info!(id = %pumasi_proto::msg::join_token_id(&t.secret), "join ticket revoked");
        Ok(t.view())
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
        let mut info = info.clone();
        self.apply_node_name(id, &mut info);
        let info = &info;
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
        let msg = format!(
            "node {} joined ({}, account {})",
            info.name, member.role, member.account
        );
        self.members.insert(id.to_string(), member);
        self.event(EventLevel::Info, "node.joined", Some(id), None, msg.clone());
        let who = crate::audit::Actor {
            name: format!("node:{}", info.name),
            role: "node".into(),
            peer: id.chars().take(10).collect(),
        };
        self.audit(&who, "node.join", id, msg, true);
        Ok(out)
    }

    /// Removes a member. `id` is a node name or a unique id prefix, as
    /// `pumasi status` shows only the first characters.
    pub fn remove_node(&mut self, id: &str) -> Result<()> {
        let id = self.resolve_node(id)?;
        let id = id.as_str();
        let name = self.node_name(id);
        self.event(
            EventLevel::Info,
            "node.removed",
            Some(id),
            None,
            format!("node {name} removed"),
        );
        self.members.remove(id);
        self.devices.remove(id);
        self.webgpu.remove(id);
        self.conf.trusted_nodes.remove(id);
        self.conf.tees.remove(id);
        self.mgmt.node_config.remove(id);
        self.store.remove_member(id)?;
        self.forget_node_name(id)?;
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
        let mut info = info;
        self.apply_node_name(id, &mut info);
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
                gpu: GpuRuntime::default(),
                unavailable: Vec::new(),
                gpu_graphics: false,
            },
        );
        let name = self.node_name(id);
        self.event(
            EventLevel::Info,
            "node.online",
            Some(id),
            None,
            format!("node {name} online"),
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
        n.paused = paused.clone();
        let name = self.node_name(id);
        match paused {
            Some(reason) => self.event(
                EventLevel::Info,
                "node.paused",
                Some(id),
                None,
                format!("node {name} paused: {reason}"),
            ),
            None => self.event(
                EventLevel::Info,
                "node.resumed",
                Some(id),
                None,
                format!("node {name} takes tasks again"),
            ),
        }
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
        self.files.caches.remove(id);
        node.perf.challenge = None;
        node.gpu = GpuRuntime::default();
        if let Some(kill) = node.kill.take() {
            let _ = kill.send(());
        }
        let lost: Vec<TaskRef> = node.running.drain().map(|(t, _)| t).collect();
        if !lost.is_empty() {
            warn!(node = %id, tasks = lost.len(), "node lost; rescheduling its tasks");
        }
        let name = self.node_name(id);
        let msg = match lost.len() {
            0 => format!("node {name} offline"),
            n => format!("node {name} offline; {n} runs moved back to the queue"),
        };
        self.event(EventLevel::Warning, "node.offline", Some(id), None, msg);
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
    /// Tells connected agents that follow replicas (protocol 11) which
    /// control plane replicas there are.
    pub fn send_replicas(&self, replicas: &[iroh::EndpointAddr]) {
        for n in self.nodes.values() {
            if n.online
                && n.perf.protocol >= pumasi_proto::HA_PROTOCOL
                && let Some(tx) = &n.tx
            {
                let _ = tx.send(ToAgent::Replicas(replicas.to_vec()));
            }
        }
    }

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
                    .map(|j| {
                        let mut d = Duration::from_millis(j.meta.limits.timeout_ms);
                        // Nodes that use the file store also download what
                        // the task mounts and upload large outputs.
                        if n.perf.protocol >= STORE_PROTOCOL {
                            let mounted: u64 = j
                                .meta
                                .ext
                                .mounts
                                .iter()
                                .chain(
                                    j.tasks
                                        .get(t.index as usize)
                                        .map(|t| t.ext.mounts.as_slice())
                                        .unwrap_or_default(),
                                )
                                .map(|m| m.size)
                                .sum();
                            d +=
                                STORE_LEASE_EXTRA + Duration::from_secs(mounted / STORE_LEASE_RATE);
                        }
                        d
                    })
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
                n.gpu.busy.remove(&t);
                if let Some(tx) = &n.tx {
                    let _ = tx.send(ToAgent::Cancel(t));
                }
            }
            self.requeue(t, "task lease expired");
        }
        self.release_holds(now_unix());
        self.tick_schedules(now_unix());
        self.tick_queue_timeouts();
        self.tick_files();
        self.expire_upgrades();
        self.expire_challenges();
        self.expire_gpu_challenges();
        if !self.queue.is_empty() || self.rollout.is_some() {
            self.schedule();
        } else {
            // A re-timing asked for within the gap between two challenges
            // waits for a tick, not for the next job.
            self.drive_challenges();
            self.drive_gpu_challenges();
        }
        self.tick_monitor();
    }

    // -- jobs -----------------------------------------------------------------

    pub fn submit(&mut self, sub: JobSubmission) -> Result<JobId> {
        self.submit_from(sub, false, None)
    }

    /// Submits a job for a submitter ticket (`submitter`: priorities above
    /// 0 need the account's quota to allow them) or a schedule.
    pub fn submit_from(
        &mut self,
        sub: JobSubmission,
        submitter: bool,
        schedule: Option<String>,
    ) -> Result<JobId> {
        let by = match &schedule {
            Some(s) => format!("schedule {s}"),
            None => "cluster admin".to_string(),
        };
        self.submit_by(sub, submitter, schedule, by)
    }

    /// [`Self::submit_from`], recording the ticket that submitted the job.
    pub fn submit_by(
        &mut self,
        mut sub: JobSubmission,
        submitter: bool,
        schedule: Option<String>,
        by: String,
    ) -> Result<JobId> {
        sub.validate()?;
        let account = sub.account.take().unwrap_or_else(|| OWNER_ACCOUNT.into());
        validate_account(&account)?;
        let cap = self.priority_cap(&account, submitter);
        anyhow::ensure!(
            sub.priority <= cap,
            "priority {} is above the {cap} account {account} may use (`pumasi quota {account} --max-priority`)",
            sub.priority
        );
        let wait = self.check_deps(&sub.after)?;
        self.ensure_account(&account)?;
        let (ext, task_exts) = self.prepare_store(&sub, &account)?;
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
        let sched = sched_of(&sub, schedule);
        let origin = crate::remote::JobOrigin {
            submitted_by: by,
            queue_timeout_ms: sub.queue_timeout_ms,
            gpu_capabilities: sub.gpu_capabilities.clone(),
            image_digests: Vec::new(),
        };
        let meta = JobMeta {
            origin,
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
            ext,
            sched,
            gpu: sub.gpu,
        };
        self.next_seq += 1;
        let tasks: Vec<TaskRecord> = sub
            .tasks
            .into_iter()
            .zip(task_exts)
            .map(|(input, ext)| TaskRecord {
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
                ext,
                times: TaskTimes::default(),
            })
            .collect();
        let blob_refs: Vec<(ModuleHash, &[u8])> =
            blobs.iter().map(|(h, b)| (*h, b.as_slice())).collect();
        self.store.put_new_job(&meta, &tasks, &blob_refs, &[])?;
        for (h, b) in blobs {
            self.modules.insert(h, Arc::new(b));
        }
        info!(job = %id, name = %meta.name, tasks = tasks.len(), account = %meta.account, priority = meta.sched.priority, "job submitted");
        let msg = format!(
            "job {id} ({}) submitted by {}: {} tasks, account {}{}",
            meta.name,
            meta.origin.submitted_by,
            tasks.len(),
            meta.account,
            meta.docker
                .as_ref()
                .map(|d| format!(", image {}", d.image))
                .unwrap_or_default()
        );
        self.jobs.insert(id, Job { meta, tasks });
        self.monitor.active_jobs.insert(id);
        self.event(EventLevel::Info, "job.submitted", None, Some(id), msg);
        if wait {
            self.mgmt.waiting.insert(id);
        } else {
            self.queue_job(id);
        }
        self.after_submit(id);
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
        self.mgmt.waiting.remove(&id);
        info!(job = %id, "job cancelled");
        Ok(())
    }

    /// Applies a task report from a node: checks and countersigns its
    /// usage receipt, settles the charge, records the result and decides
    /// whether to check it.
    pub fn on_report(&mut self, node_id: &str, generation: u64, report: TaskReport) {
        self.on_report_stored(node_id, generation, report, None)
    }

    /// [`Self::on_report`] for a run whose output files may be in the
    /// store (checked complete there by the caller).
    pub fn on_report_stored(
        &mut self,
        node_id: &str,
        generation: u64,
        report: TaskReport,
        stored: Option<StoredOutput>,
    ) {
        self.on_report_opened(node_id, generation, report, stored, None)
    }

    /// [`Self::on_report_stored`] for a confidential run: the receipt is
    /// checked against the report as the node sent it (sealed), then the
    /// opened result counts in its place.
    pub fn on_report_opened(
        &mut self,
        node_id: &str,
        generation: u64,
        report: TaskReport,
        stored: Option<StoredOutput>,
        opened: Option<crate::confidential::Opened>,
    ) {
        let timing = self.mgmt.report_timing.remove(&report.task);
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
        let gpus = node.gpu.busy.remove(&report.task).unwrap_or_default();
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
        let digest = output_digest(
            &report.outcome,
            &report.stdout,
            &report.files,
            stored.as_ref(),
        );
        let receipt_check = self.check_receipt(node_id, &job.meta, &report, &digest);
        if let Err(e) = receipt_check {
            warn!(node = %node_id, task = %r, "rejected report: {e:#}");
            self.strike(node_id, &format!("invalid receipt: {e:#}"));
            self.requeue_run(r, "invalid receipt");
            self.schedule();
            return;
        }
        if let Some(signed) = report.usage.clone() {
            self.settle_run(node_id, signed, observed_ms, &gpus);
            self.sample_gpus(node_id, &gpus);
        }
        self.conf.leases.remove(&r);
        let (report, stored, digest) = match opened {
            Some(o) => {
                let digest = output_digest(
                    &o.report.outcome,
                    &o.report.stdout,
                    &o.report.files,
                    o.stored.as_ref(),
                );
                (o.report, o.stored, digest)
            }
            None => (report, stored, digest),
        };

        if r.check {
            if let Some(s) = stored {
                self.add_check_stored(r.job, r.index, r.attempt, s);
            }
            self.on_check_report(node_id, report, digest);
        } else {
            self.set_stored(r.job, r.index, stored);
            self.note_run(node_id, r, &report.outcome);
            // No part of a run took longer than the run was out.
            if let Some(t) = timing
                && let Some(task) = self
                    .jobs
                    .get_mut(&r.job)
                    .and_then(|j| j.tasks.get_mut(r.index as usize))
            {
                task.times.fetch_ms = t.fetch_ms.min(observed_ms);
                task.times.pull_ms = t.pull_ms.min(observed_ms);
                task.times.upload_ms = t.upload_ms.min(observed_ms);
            }
            self.on_primary_report(node_id, report, digest);
        }
        self.schedule();
    }

    /// A report whose stored output is not (completely) in the store: the
    /// run is done again, without counting against the node.
    pub fn on_report_rejected(&mut self, node_id: &str, generation: u64, task: TaskRef, why: &str) {
        self.mgmt.report_timing.remove(&task);
        let Some(node) = self.nodes.get_mut(node_id) else {
            return;
        };
        if node.generation != generation || node.running.remove(&task).is_none() {
            return;
        }
        node.gpu.busy.remove(&task);
        warn!(node = %node_id, task = %task, "report rejected: {why}");
        self.requeue_run(task, why);
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
        gpus: &[u32],
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
        let mut gpu = self.gpu_billing(node_id, signed.usage.executor, gpus);
        let metered = Metered {
            executor: signed.usage.executor,
            fuel_used: signed.usage.fuel_used,
            wall_ms: billed_wall_ms,
            memory_bytes: signed.usage.memory_limit_bytes,
            cpu_millis: signed.usage.cpu_millis,
            cpu_score,
            gpu_gflops: gpu.as_ref().map_or(0, |g| g.gflops),
            gpu_vram_mib: gpu.as_ref().map_or(0, |g| g.vram_mib),
        };
        let exact_ucu = if provider == consumer {
            0
        } else {
            self.policy.rates.charge(&metered)
        };
        if let Some(g) = gpu.as_mut()
            && provider != consumer
        {
            g.exact_ucu = self.policy.rates.charge_gpu(&metered);
        }
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
        let receipt = Receipt::countersign_gpu(signed, billing, gpu, &self.key);
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
        let mut queue_check = false;
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
            queue_check = true;
            info!(task = %r, node = %node_id, "result picked for a check run");
        } else {
            task.state = final_state(&report.outcome);
        }
        if let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)]) {
            warn!("persist task: {e:#}");
        }
        if queue_check {
            self.push_run(r.job, r.index, true, false);
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
            let newly = !n.suspended;
            n.suspended = true;
            if newly {
                warn!(node = %node_id, "too many failed checks; node suspended");
                let name = self.node_name(node_id);
                self.event(
                    EventLevel::Critical,
                    "node.suspended",
                    Some(node_id),
                    None,
                    format!("node {name} suspended after {max} failed checks"),
                );
            }
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
    pub(crate) fn requeue_run(&mut self, r: TaskRef, reason: &str) {
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
        let free = self
            .mgmt
            .preempted
            .get(&(r.job, r.index, false))
            .copied()
            .unwrap_or(0);
        let Some(task) = job.tasks.get_mut(r.index as usize) else {
            return;
        };
        if task.attempts != r.attempt || !matches!(task.state, TaskState::Running { .. }) {
            return;
        }
        task.node = None;
        let mut retry = false;
        let mut gave_up = None;
        if cancelled {
            task.state = TaskState::Cancelled;
        } else if task.attempts.saturating_sub(free) >= max {
            let why = format!(
                "{reason}; gave up after {} attempts",
                task.attempts.saturating_sub(free)
            );
            task.state = TaskState::Failed(Outcome::Error(why.clone()));
            gave_up = Some(why);
        } else {
            task.state = TaskState::Pending;
            retry = true;
        }
        if let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)]) {
            warn!("persist task: {e:#}");
        }
        if retry {
            // Retries go first (within their priority) so a job is not
            // starved by later jobs.
            self.push_run(r.job, r.index, false, true);
        }
        if let Some(why) = gave_up {
            self.event(
                EventLevel::Warning,
                "task.failed",
                None,
                Some(r.job),
                format!("task {}/{} failed: {why}", r.job, r.index),
            );
        }
    }

    /// What a queued run needs from a node, or `None` when the item is
    /// stale (the task moved on).
    pub(crate) fn need_for(&mut self, item: &QueueItem) -> Option<Need> {
        // A private image is pulled into the store per architecture; only
        // nodes of an architecture it is ready for can run it.
        let tier = self.jobs.get(&item.job).map(|j| j.meta.tier)?;
        let confidential =
            (tier == Tier::Confidential).then(|| self.conf_need(item.job, item.index));
        let images = self
            .jobs
            .get(&item.job)
            .is_some_and(|j| j.meta.ext.private_image && tier != Tier::Confidential)
            .then(|| self.want_images(item.job));
        let job = self.jobs.get(&item.job)?;
        let task = job.tasks.get(item.index as usize)?;
        // Nodes (and accounts) that already ran this task are out for a
        // check run.
        let mut exclude_nodes = Vec::new();
        let mut exclude_accounts = Vec::new();
        if item.check {
            let check = task.check.as_ref()?;
            if task.state != TaskState::Verifying
                || check.status != Verification::Pending
                || check.running_on.is_some()
            {
                return None;
            }
            for run in &check.runs {
                exclude_nodes.push(run.node.clone());
                exclude_accounts.push(run.account.clone());
            }
        } else if task.state != TaskState::Pending {
            return None;
        }
        let executor = job.meta.executor;
        let consumer = job.meta.account.clone();
        let floor = self.policy.ledger.floor();
        let can_pay = self
            .accounts
            .get(&consumer)
            .is_some_and(|a| a.balance > floor)
            && !self.over_daily(&consumer);
        let extra_memory = match &confidential {
            Some(crate::confidential::ConfNeed::Ready(s)) => s.extra_mib << 20,
            _ => 0,
        };
        Some(Need {
            item: *item,
            executor,
            memory: job.meta.limits.memory_bytes + extra_memory,
            // Image jobs get their CPUs (a WASM task uses one): a node
            // offers as many as it reports, which its CPU cap lowers.
            cpus: match executor {
                Executor::Wasm => 1,
                _ => vcpus(job.meta.limits.cpu_millis),
            },
            store_job: job.meta.ext.needs_store,
            gpu: job.meta.gpu.clone(),
            vm_network: executor == Executor::Microvm
                && job.meta.docker.as_ref().is_some_and(|d| d.network),
            images,
            consumer,
            can_pay,
            exclude_nodes,
            exclude_accounts,
            selector: job.meta.sched.selector.clone(),
            tolerations: job.meta.sched.tolerations.clone(),
            gpu_caps: !job.meta.origin.gpu_capabilities.is_empty(),
            gpu_graphics: job
                .meta
                .origin
                .gpu_capabilities
                .contains(&pumasi_proto::job::GpuCapability::Graphics),
            trusted_only: tier == Tier::Trusted,
            confidential,
        })
    }

    /// Whether node `id` can take the run, with `extra_slots` more free
    /// slots and the GPUs in `freed` free (what stopping some of its runs
    /// would give).
    pub(crate) fn fits(
        &self,
        id: &str,
        n: &NodeRuntime,
        need: &Need,
        labels: &HashMap<String, LabelSet>,
        extra_slots: u32,
        freed: &[u32],
    ) -> bool {
        self.refusal(id, n, need, labels, extra_slots, freed)
            .is_none()
    }

    /// Why node `id` cannot take the run (the first reason found), or
    /// `None` when it can. See [`Self::fits`].
    pub(crate) fn refusal<'a>(
        &'a self,
        id: &str,
        n: &'a NodeRuntime,
        need: &'a Need,
        labels: &HashMap<String, LabelSet>,
        extra_slots: u32,
        freed: &[u32],
    ) -> Option<Refusal<'a>> {
        use Refusal as R;
        let config = self.node_config(id);
        if !n.online {
            return Some(R::Offline);
        }
        if n.suspended {
            return Some(R::Suspended);
        }
        if n.draining || self.upgrading.contains_key(id) {
            return Some(R::Upgrading);
        }
        if let Some(why) = &n.paused {
            return Some(R::Paused(why));
        }
        if n.perf.wanted || n.perf.challenge.is_some() {
            return Some(R::Benchmarking);
        }
        if config.is_some_and(|c| c.cordoned) {
            return Some(R::Cordoned);
        }
        if !n.executors.contains(&need.executor) {
            let why = n
                .unavailable
                .iter()
                .find(|(e, _)| *e == need.executor)
                .map(|(_, w)| w.as_str());
            return Some(R::Executor(need.executor, why));
        }
        if need.exclude_nodes.iter().any(|x| x == id) || need.exclude_accounts.contains(&n.account)
        {
            return Some(R::RanItAlready);
        }
        if need.trusted_only && !self.trusted_host(id) {
            return Some(R::Untrusted);
        }
        if let Some(c) = &need.confidential {
            if n.perf.protocol < pumasi_proto::CONFIDENTIAL_PROTOCOL {
                return Some(R::OldAgent);
            }
            if self.conf.usable_tees(id).is_empty() {
                let why = self
                    .conf
                    .tees
                    .get(id)
                    .and_then(|r| r.unavailable.as_deref());
                return Some(R::Tee(why));
            }
            if let crate::confidential::ConfNeed::Sealing(what) = c {
                return Some(R::Sealing(what));
            }
        }
        if !(n.account == need.consumer || need.can_pay) {
            return Some(R::CannotPay(&need.consumer));
        }
        if n.info.memory_bytes < need.memory {
            return Some(R::Memory(need.memory));
        }
        if n.info.cpus < need.cpus {
            return Some(R::Cpus(need.cpus));
        }
        if need.store_job && n.perf.protocol < STORE_PROTOCOL
            || need.gpu.is_some() && n.perf.protocol < DEVICES_PROTOCOL
            || need.gpu_caps && n.perf.protocol < pumasi_proto::REMOTE_PROTOCOL
        {
            return Some(R::OldAgent);
        }
        if need.images.as_ref().is_some_and(|imgs| {
            let arch = oci_arch_of(&n.info.arch);
            !imgs.iter().any(|(a, _)| *a == arch)
        }) {
            return Some(R::ImageArch);
        }
        if need.vm_network
            && !(n.perf.protocol >= DEVICES_PROTOCOL
                && self
                    .devices
                    .get(id)
                    .is_some_and(|d| d.signed.profile.microvm_network))
        {
            return Some(R::VmNetwork);
        }
        if let Some(t) = config
            .map(|c| c.taints.as_slice())
            .unwrap_or_default()
            .iter()
            .find(|t| !need.tolerations.iter().any(|tol| tol.tolerates(t)))
        {
            return Some(R::Taint(t));
        }
        if !need.selector.is_empty() && !labels.get(id).is_some_and(|l| need.selector.matches(l)) {
            return Some(R::Selector(&need.selector));
        }
        if need.gpu_graphics && !n.gpu_graphics {
            return Some(R::GpuGraphics);
        }
        if let Some(req) = &need.gpu {
            if n.gpu.checking() {
                return Some(R::GpuCheck);
            }
            let mut busy = n.gpu.busy_set();
            for g in freed {
                busy.remove(g);
            }
            if self.pick_gpus(id, req, need.executor, &busy).is_none() {
                return Some(R::Gpu);
            }
        }
        if n.free_slots() + extra_slots == 0 {
            return Some(R::Full);
        }
        None
    }

    /// Places as many queued runs as free slots allow, highest priority
    /// first, stopping lower-priority runs when a higher one finds no room.
    pub fn schedule(&mut self) {
        // Idle nodes due for an upgrade get it before any new task, and
        // idle nodes due for a benchmark check get that.
        self.drive_rollout();
        self.drive_challenges();
        self.drive_gpu_challenges();
        self.release_waiting();
        self.sweep_leases();
        if self.queue.is_empty() {
            self.monitor.quota_blocked = 0;
            return;
        }
        let labels: HashMap<String, LabelSet> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.online)
            .map(|(id, _)| (id.clone(), self.node_labels(id)))
            .collect();
        let mut usage = self.account_usage();
        let mut skipped = VecDeque::new();
        let mut dirty: Vec<(JobId, u32)> = Vec::new();
        let mut quota_blocked = 0;
        while let Some(item) = self.queue.pop_front() {
            let Some(need) = self.need_for(&item) else {
                continue;
            };
            // Over its account's quota: waits, and so do the account's
            // other runs behind it.
            if !self.within_quota(&need, &usage) {
                quota_blocked += 1;
                skipped.push_back(item);
                continue;
            }
            // Free (same account) and trusted nodes first, then most free
            // slots to spread work.
            let best = self
                .nodes
                .iter()
                .filter(|(id, n)| self.fits(id, n, &need, &labels, 0, &[]))
                .max_by_key(|(_, n)| {
                    let pref = if item.check {
                        n.trusted
                    } else {
                        n.account == need.consumer
                    };
                    (pref, n.free_slots())
                })
                .map(|(id, _)| id.clone());
            let node_id = match best {
                Some(id) => id,
                None => match self.preempt_for(&need, &labels) {
                    Some(id) => id,
                    None => {
                        skipped.push_back(item);
                        // Nothing behind it can preempt either: the queue
                        // is in priority order.
                        if !self
                            .nodes
                            .values()
                            .any(|n| n.online && n.paused.is_none() && n.free_slots() > 0)
                            && !self.lower_priority_running(item.priority)
                        {
                            break;
                        }
                        continue;
                    }
                },
            };
            match self.place(&need, &node_id) {
                Some(gpus) => {
                    let e = usage.entry(need.consumer.clone()).or_default();
                    e.0 += 1;
                    e.1 += gpus;
                    dirty.push((item.job, item.index));
                }
                None => skipped.push_back(item),
            }
        }
        self.monitor.quota_blocked = quota_blocked;
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

    /// Sends a run to a node. Returns the number of GPUs it got, or `None`
    /// if the node could not take it after all.
    fn place(&mut self, need: &Need, node_id: &str) -> Option<u32> {
        let item = need.item;
        // A confidential run gets a lease (its nonce) before it goes out.
        let conf_task = match &need.confidential {
            Some(crate::confidential::ConfNeed::Ready(sealed)) => {
                let job = self.jobs.get(&item.job)?;
                let task = job.tasks.get(item.index as usize)?;
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
                let keep = job.meta.ext.output_dataset.is_some();
                let sealed = sealed.clone();
                Some(self.conf_assign(r, node_id, &sealed, keep)?)
            }
            Some(_) => return None,
            None => None,
        };
        let job = self.jobs.get_mut(&item.job)?;
        let task = job.tasks.get_mut(item.index as usize)?;
        let node = self.nodes.get_mut(node_id)?;
        let tx = node.tx.as_ref()?;
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
        let speaks_store = node.perf.protocol >= STORE_PROTOCOL;
        let mounts: Vec<_> = job
            .meta
            .ext
            .mounts
            .iter()
            .chain(task.ext.mounts.iter())
            .cloned()
            .collect();
        let image = need.images.as_ref().and_then(|imgs| {
            let arch = oci_arch_of(&node.info.arch);
            imgs.iter()
                .find(|(a, _)| *a == arch)
                .map(|(_, i)| i.clone())
        });
        let mut roots: Vec<BlobHash> = mounts.iter().map(|m| m.root).collect();
        roots.extend(image.as_ref().map(|i| i.root));
        let peers = crate::datasets::peers_for(&self.files.caches, node_id, &roots);
        let output = if !speaks_store {
            OutputMode::Inline
        } else if job.meta.ext.output_dataset.is_some() {
            OutputMode::Store
        } else {
            OutputMode::Auto
        };
        let gpus = need
            .gpu
            .as_ref()
            .and_then(|req| {
                crate::gpu::pick_gpus(
                    &self.devices,
                    &self.webgpu,
                    node_id,
                    req,
                    need.executor,
                    &node.gpu.busy_set(),
                )
            })
            .unwrap_or_default();
        let assignment = TaskAssignment {
            gpus: gpus.clone(),
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
            mounts,
            output,
            image,
            peers,
            gpu_capabilities: job.meta.origin.gpu_capabilities.clone(),
            confidential: None,
        };
        let mut assignment = assignment;
        if let Some(c) = conf_task {
            // The node learns nothing of the job but its limits: the
            // image, command, environment and inputs come sealed.
            let roots = [c.image.root, c.input.root];
            assignment = TaskAssignment {
                docker: Some(DockerSpec::default()),
                module_hash: None,
                module: None,
                files_hash: None,
                files: None,
                input: TaskInput::default(),
                mounts: vec![],
                image: None,
                output: OutputMode::Store,
                peers: crate::datasets::peers_for(&self.files.caches, node_id, &roots),
                confidential: Some(c),
                ..assignment
            };
        }
        if tx.send(ToAgent::Assign(Box::new(assignment))).is_err() {
            return None;
        }
        node.sent_modules.extend(module_hash);
        node.sent_modules.extend(files_hash);
        node.running.insert(r, Instant::now());
        let n_gpus = gpus.len() as u32;
        if !gpus.is_empty() {
            node.gpu.busy.insert(r, gpus);
        }
        if item.check {
            if let Some(c) = task.check.as_mut() {
                c.attempts = attempt;
                c.running_on = Some(node_id.to_string());
            }
        } else {
            task.attempts = attempt;
            task.state = TaskState::Running {
                node: node_id.to_string(),
            };
            task.node = Some(node_id.to_string());
            // A new run: its own wait, and nothing from the node yet.
            task.times = TaskTimes {
                queued_ms: item.since.elapsed().as_millis() as u64,
                ..Default::default()
            };
        }
        Some(n_gpus)
    }

    // -- views ------------------------------------------------------------------

    pub(crate) fn summary(job: &Job) -> JobSummary {
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
            priority: job.meta.sched.priority,
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
        self.status_for(None)
    }

    /// The status, with which nodes can take `account`'s work now.
    pub fn status_for(&self, account: Option<&str>) -> ClusterStatus {
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
                    gpus: self.gpu_views(m),
                    webgpu: self.webgpu_views(m),
                    microvm_network: self
                        .devices
                        .get(&m.id)
                        .is_some_and(|d| d.signed.profile.microvm_network),
                    config: self.node_config(&m.id).cloned().unwrap_or_default(),
                    labels: self.node_labels(&m.id),
                    unavailable: online.map(|n| n.unavailable.clone()).unwrap_or_default(),
                    gpu_graphics: online.is_some_and(|n| n.gpu_graphics),
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
            capacity: account.map(|a| self.capacity(a)),
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
                stored: t.ext.stored,
                preempted: self.free_attempts(id, i as u32, false)
                    + self.free_attempts(id, i as u32, true),
            })
            .collect();
        let ext = &job.meta.ext;
        let mut mounts: Vec<String> = ext
            .mounts
            .iter()
            .chain(job.tasks.iter().flat_map(|t| t.ext.mounts.iter()))
            .map(|m| m.label.clone())
            .collect();
        mounts.sort();
        mounts.dedup();
        let preparing = ext.images.iter().find_map(|(arch, s)| match s {
            ImageState::Pulling => Some(format!("pulling the private image ({arch})")),
            ImageState::Failed(e) => {
                Some(format!("pulling the private image ({arch}) failed: {e}"))
            }
            ImageState::Ready(_) => None,
        });
        Some(JobView {
            summary: Self::summary(job),
            executor: job.meta.executor,
            tier: job.meta.tier,
            image: job.meta.docker.as_ref().map(|d| d.image.clone()),
            limits: job.meta.limits,
            total_fuel: job.tasks.iter().map(|t| t.fuel_used).sum(),
            deterministic: job.meta.deterministic,
            tasks,
            mounts,
            output_dataset: ext.finished.clone().filter(|f| !f.is_empty()),
            preparing,
            gpu: job.meta.gpu.clone(),
            network: job.meta.docker.as_ref().is_some_and(|d| d.network),
            selector: job.meta.sched.selector.clone(),
            tolerations: job.meta.sched.tolerations.clone(),
            after: job.meta.sched.after.clone(),
            waiting: self.waiting_reason(id),
            schedule: job.meta.sched.schedule.clone(),
            submitted_by: job.meta.origin.submitted_by.clone(),
            image_digests: job.meta.origin.image_digests.clone(),
            queue_timeout_ms: job.meta.origin.queue_timeout_ms,
            gpu_capabilities: job.meta.origin.gpu_capabilities.clone(),
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

    #[test]
    fn old_finished_jobs_are_forgotten() {
        let path =
            std::env::temp_dir().join(format!("pumasi-state-{}.redb", rand::random::<u64>()));
        let key = SecretKey::generate();
        let open = |key: &SecretKey| {
            State::load(
                "t".into(),
                key.public().to_string(),
                Store::open(&path).unwrap(),
                key.clone(),
            )
            .unwrap()
        };
        let mut st = open(&key);
        let mut unique = job(Executor::Wasm, 2);
        unique.module = b"\0asm\x01\0\0\0\0".to_vec();
        let old = st.submit(unique).unwrap();
        let old_shared = st.submit(job(Executor::Wasm, 1)).unwrap();
        let queued = st.submit(job(Executor::Wasm, 1)).unwrap();
        let recent = st.submit(job(Executor::Wasm, 1)).unwrap();
        for id in [old, old_shared, recent] {
            st.cancel(id).unwrap();
        }
        let long_ago = now_unix() - 40 * 86_400;
        for id in [old, old_shared, queued] {
            st.jobs.get_mut(&id).unwrap().meta.submitted_unix = long_ago;
        }
        let unique_hash = st.jobs[&old].meta.module_hash.unwrap();
        let shared_hash = st.jobs[&queued].meta.module_hash.unwrap();
        let day = Duration::from_secs(86_400);

        assert_eq!(st.expire_jobs(Duration::ZERO, now_unix()), 0);
        assert_eq!(st.expire_jobs(30 * day, now_unix()), 2);
        assert!(!st.jobs.contains_key(&old) && !st.jobs.contains_key(&old_shared));
        // Not finished, or not old enough.
        assert!(st.jobs.contains_key(&queued) && st.jobs.contains_key(&recent));
        assert!(!st.modules.contains_key(&unique_hash));
        assert!(
            st.modules.contains_key(&shared_hash),
            "a queued job still uses it"
        );
        assert_eq!(st.expire_jobs(30 * day, now_unix()), 0);
        let entries = st.audit_entries(0, 10).unwrap();
        assert!(
            entries
                .iter()
                .any(|e| e.action == "jobs.expire" && e.target == "2 jobs")
        );

        // Gone from the database too.
        drop(st);
        let st = open(&key);
        assert!(!st.jobs.contains_key(&old) && st.jobs.contains_key(&queued));
        assert!(!st.modules.contains_key(&unique_hash) && st.modules.contains_key(&shared_hash));
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
            mounts: vec![],
            task_mounts: vec![],
            output_dataset: None,
            gpu: None,
            priority: 0,
            selector: Default::default(),
            tolerations: vec![],
            after: vec![],
            queue_timeout_ms: None,
            gpu_capabilities: vec![],
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

    // -----------------------------------------------------------------------
    // GPUs (phase 9)
    // -----------------------------------------------------------------------

    fn gpu(index: u32, gflops: u32, hash_rate: u64) -> pumasi_proto::gpu::GpuInfo {
        pumasi_proto::gpu::GpuInfo {
            index,
            vendor: GpuVendor::Nvidia,
            name: format!("Test GPU {index}"),
            uuid: String::new(),
            pci: format!("0000:0{}:00.0", index + 1),
            vram_mib: 12 * 1024,
            driver: "570.86".into(),
            compute: "8.6".into(),
            display: false,
            fp32_gflops: gflops,
            measured: true,
            mem_bw_gb_s: 360,
            hash_rate,
            docker: true,
            vfio: false,
            render_node: String::new(),
        }
    }

    /// Joins a Docker node and sends its signed device profile.
    fn join_gpus(
        st: &mut State,
        name: &str,
        gpus: Vec<pumasi_proto::gpu::GpuInfo>,
        invite: Option<Invite>,
    ) -> (String, SecretKey, mpsc::UnboundedReceiver<ToAgent>) {
        let key = SecretKey::generate();
        let id = key.public().to_string();
        let token = st
            .create_token(Duration::from_secs(60), Some(1), invite)
            .unwrap();
        let info = info(name, vec![Executor::Wasm, Executor::Docker]);
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
        let generation = st.nodes[&id].generation;
        let profile = pumasi_proto::gpu::DeviceProfile {
            bench_version: pumasi_proto::gpu::GPU_BENCH_VERSION,
            node: *key.public().as_bytes(),
            measured_unix: 0,
            gpus,
            microvm_network: false,
            notes: vec![],
        };
        st.on_devices(
            &id,
            generation,
            pumasi_proto::gpu::SignedDevices::sign(profile, &key),
        )
        .unwrap();
        (id, key, rx)
    }

    fn gpu_job(count: u32, tasks: usize) -> JobSubmission {
        JobSubmission {
            gpu: Some(GpuRequest {
                count,
                vendor: None,
                min_vram_mib: 0,
            }),
            ..job(Executor::Docker, tasks)
        }
    }

    #[test]
    fn gpu_jobs_go_only_to_nodes_with_enough_free_gpus() {
        let mut st = state();
        let (_, _, mut one_rx) = join_gpus(&mut st, "one-gpu", vec![gpu(0, 12_700, 0)], None);
        let (_, _, mut none_rx) = join_gpus(&mut st, "no-gpu", vec![], None);
        let two = st.submit(gpu_job(2, 1)).unwrap();
        assert!(assigned(&mut one_rx).is_empty(), "2 GPUs asked, 1 there");
        assert!(assigned(&mut none_rx).is_empty(), "no GPUs there");

        let one = st.submit(gpu_job(1, 2)).unwrap();
        let got = assigned(&mut one_rx);
        assert_eq!(got.len(), 1, "one task per free GPU");
        assert_eq!(got[0].gpus, vec![0]);
        assert_eq!(got[0].task.job, one);
        assert!(assigned(&mut none_rx).is_empty());
        let view = st.status();
        let busy = view
            .nodes
            .iter()
            .find(|n| n.info.name == "one-gpu")
            .unwrap();
        assert_eq!(busy.gpus[0].busy, Some(got[0].task));

        // A node with two GPUs takes the two-GPU job.
        let (_, _, mut two_rx) = join_gpus(
            &mut st,
            "two-gpus",
            vec![gpu(0, 12_700, 0), gpu(1, 12_700, 0)],
            None,
        );
        let got = assigned(&mut two_rx);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].task.job, two);
        assert_eq!(got[0].gpus, vec![0, 1]);

        // Plain jobs still run on the GPU-less node.
        st.submit(job(Executor::Docker, 1)).unwrap();
        assert_eq!(assigned(&mut none_rx).len(), 1);

        // Asking for more memory than any GPU has never matches.
        let mut big = gpu_job(1, 1);
        big.gpu.as_mut().unwrap().min_vram_mib = 80 * 1024;
        let id = st.submit(big).unwrap();
        assert_eq!(st.jobs[&id].tasks[0].state, TaskState::Pending);
    }

    #[test]
    fn graphics_jobs_need_nodes_that_draw() {
        let mut st = state();
        let (draws, _, mut draws_rx) = join_gpus(&mut st, "draws", vec![gpu(0, 12_700, 0)], None);
        let (_, _, mut plain_rx) = join_gpus(&mut st, "plain", vec![gpu(0, 12_700, 0)], None);
        let cap = st.capacity("owner");
        assert_eq!((cap.ready_gpu, cap.ready_gpu_graphics), (2, 0));
        let mut eevee = gpu_job(1, 1);
        eevee.gpu_capabilities = vec![pumasi_proto::job::GpuCapability::Graphics];
        let id = st.submit(eevee).unwrap();
        assert!(assigned(&mut draws_rx).is_empty());
        assert!(assigned(&mut plain_rx).is_empty());
        assert!(st.why_unplaced(id, 0).contains("cannot draw"));

        // `pumasi gpu check --docker-run` found that it draws.
        let generation = st.nodes[&draws].generation;
        st.on_executors(
            &draws,
            generation,
            pumasi_proto::msg::ExecutorReport {
                executors: vec![Executor::Wasm, Executor::Docker],
                unavailable: vec![],
                gpu_graphics: true,
            },
        );
        let got = assigned(&mut draws_rx);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].task.job, id);
        assert_eq!(
            got[0].gpu_capabilities,
            vec![pumasi_proto::job::GpuCapability::Graphics]
        );
        assert!(assigned(&mut plain_rx).is_empty());
        assert_eq!(
            st.node_labels(&draws).get("gpu.graphics"),
            Some(&vec!["true".to_string()])
        );
        // A CUDA job takes the other node.
        st.submit(gpu_job(1, 1)).unwrap();
        assert_eq!(assigned(&mut plain_rx).len(), 1);
    }

    #[test]
    fn nodes_report_docker_going_away_and_coming_back() {
        let mut st = state();
        let (id, mut rx) = join(&mut st, "win", vec![Executor::Wasm, Executor::Docker], None);
        let generation = st.nodes[&id].generation;
        let report = |up: bool| pumasi_proto::msg::ExecutorReport {
            executors: if up {
                vec![Executor::Wasm, Executor::Docker]
            } else {
                vec![Executor::Wasm]
            },
            unavailable: if up {
                vec![]
            } else {
                vec![(Executor::Docker, "Docker Desktop is not running".into())]
            },
            gpu_graphics: false,
        };
        st.on_executors(&id, generation, report(false));
        let view = st.status();
        let node = view.nodes.iter().find(|n| n.id == id).unwrap();
        assert_eq!(node.unavailable.len(), 1);
        let jid = st.submit(job(Executor::Docker, 1)).unwrap();
        assert_eq!(st.jobs[&jid].tasks[0].state, TaskState::Pending);
        let cap = st.capacity("owner");
        assert_eq!(cap.ready_for(Executor::Docker), 0);
        assert_eq!(cap.ready_for(Executor::Wasm), 1);
        assert!(
            st.why_unplaced(jid, 0).contains("docker"),
            "{}",
            st.why_unplaced(jid, 0)
        );
        assert!(
            st.events(0, 0, 100)
                .iter()
                .any(|e| e.kind == "node.executor_unavailable")
        );
        // Docker Desktop started: the waiting task goes there.
        st.on_executors(&id, generation, report(true));
        let got = assigned(&mut rx);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].task.job, jid);
        // A report from an older connection is ignored.
        st.on_executors(&id, generation + 1, report(false));
        assert!(st.status().nodes[0].unavailable.is_empty());
    }

    #[test]
    fn gpu_runs_are_billed_and_receipts_record_them() {
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        // A provider whose GPU cannot run the cluster's check: billed at
        // the reference throughput at most.
        let (bob_id, bob_key, mut rx) =
            join_gpus(&mut st, "bob", vec![gpu(0, 20_000, 0)], bob(true));
        let id = st.submit(gpu_job(1, 1)).unwrap();
        let a = assigned(&mut rx).pop().expect("assigned to bob");
        assert_eq!(a.gpus, vec![0]);
        std::thread::sleep(Duration::from_millis(200));
        let outcome = Outcome::Exited(0);
        let digest = pumasi_proto::receipt::result_digest(&outcome, b"ok", &[]);
        let usage = pumasi_proto::receipt::Usage {
            cluster: *st.key.public().as_bytes(),
            node: *bob_key.public().as_bytes(),
            task: a.task,
            executor: Executor::Docker,
            outcome: outcome.clone(),
            fuel_used: 0,
            wall_ms: 200,
            memory_limit_bytes: a.limits.memory_bytes,
            peak_memory_bytes: None,
            cpu_millis: a.limits.cpu_millis,
            result_digest: digest,
            finished_unix: now_unix(),
        };
        let generation = st.nodes[&bob_id].generation;
        st.on_report(
            &bob_id,
            generation,
            TaskReport {
                task: a.task,
                outcome,
                stdout: b"ok".to_vec(),
                stderr: vec![],
                files: vec![],
                fuel_used: 0,
                wall_ms: 200,
                usage: Some(pumasi_proto::receipt::SignedUsage::sign(usage, &bob_key)),
            },
        );
        assert_eq!(st.jobs[&id].tasks[0].state, TaskState::Succeeded);
        let receipts = st.receipts(id).unwrap();
        assert_eq!(receipts.len(), 1);
        let r = &receipts[0];
        r.verify().unwrap();
        let g = r.gpu.as_ref().expect("GPU billing on the receipt");
        assert_eq!(g.names, vec!["Test GPU 0".to_string()]);
        assert_eq!(g.gflops, pumasi_proto::gpu::REFERENCE_GPU_GFLOPS as u64);
        assert_eq!(g.vram_mib, 12 * 1024);
        assert!(g.exact_ucu > 0, "{g:?}");
        assert!(r.billing.exact_ucu > g.exact_ucu, "CPU and memory on top");
        let view = st.status();
        let node = view.nodes.iter().find(|n| n.id == bob_id).unwrap();
        assert_eq!(node.gpus[0].busy, None, "freed after the run");
        assert_eq!(
            node.gpus[0].billed_gflops,
            pumasi_proto::gpu::REFERENCE_GPU_GFLOPS
        );
        assert!(node.gpus[0].note.is_some());
    }

    // -----------------------------------------------------------------------
    // WebGPU (phase 14)
    // -----------------------------------------------------------------------

    fn adapter(index: u32, gpu: Option<u32>, gflops: u32) -> pumasi_proto::webgpu::WebGpuAdapter {
        pumasi_proto::webgpu::WebGpuAdapter {
            index,
            name: format!("Test adapter {index}"),
            vendor: GpuVendor::Nvidia,
            backend: pumasi_proto::webgpu::WebGpuBackend::Vulkan,
            device_type: pumasi_proto::webgpu::WebGpuDeviceType::Discrete,
            driver: String::new(),
            gpu,
            pci: String::new(),
            max_buffer_mib: 1024,
            max_binding_mib: 128,
            fp32_gflops: gflops,
        }
    }

    fn send_webgpu(
        st: &mut State,
        id: &str,
        key: &SecretKey,
        adapters: Vec<pumasi_proto::webgpu::WebGpuAdapter>,
    ) {
        let generation = st.nodes[id].generation;
        let profile = pumasi_proto::webgpu::WebGpuProfile {
            bench_version: pumasi_proto::webgpu::WEBGPU_BENCH_VERSION,
            node: *key.public().as_bytes(),
            measured_unix: 0,
            adapters,
            notes: vec![],
        };
        st.on_webgpu(
            id,
            generation,
            pumasi_proto::webgpu::SignedWebGpu::sign(profile, key),
        )
        .unwrap();
    }

    fn webgpu_job(tasks: usize) -> JobSubmission {
        JobSubmission {
            gpu: Some(GpuRequest {
                count: 1,
                vendor: None,
                min_vram_mib: 0,
            }),
            deterministic: false,
            ..job(Executor::Wasm, tasks)
        }
    }

    #[test]
    fn webgpu_jobs_share_gpu_slots_and_are_billed() {
        use pumasi_proto::webgpu::{UNCHECKED_WEBGPU_GFLOPS, WEBGPU_SLOT_BASE};
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        // A provider with one NVIDIA GPU (also reachable over WebGPU) and an
        // integrated GPU that only WebGPU reaches.
        let (bob_id, bob_key, mut rx) =
            join_gpus(&mut st, "bob", vec![gpu(0, 20_000, 0)], bob(true));
        let (_, _, mut plain_rx) = join_gpus(&mut st, "plain", vec![], None);
        let id = st.submit(webgpu_job(3)).unwrap();
        assert!(assigned(&mut rx).is_empty(), "no WebGPU profile yet");
        send_webgpu(
            &mut st,
            &bob_id,
            &bob_key,
            vec![adapter(0, Some(0), 5_000), adapter(1, None, 800)],
        );
        let got = assigned(&mut rx);
        assert_eq!(got.len(), 2, "one task per adapter: {got:?}");
        assert_eq!(got[0].gpus, vec![0], "fastest first, in its GPU's slot");
        assert_eq!(got[1].gpus, vec![WEBGPU_SLOT_BASE + 1]);
        assert!(assigned(&mut plain_rx).is_empty(), "no WebGPU there");
        assert_eq!(st.jobs[&id].tasks[2].state, TaskState::Pending);

        // The GPU is taken by the WebGPU task, so a Docker GPU job waits.
        let docker = st.submit(gpu_job(1, 1)).unwrap();
        assert!(assigned(&mut rx).is_empty());
        assert_eq!(st.jobs[&docker].tasks[0].state, TaskState::Pending);
        let view = st.status();
        let node = view.nodes.iter().find(|n| n.id == bob_id).unwrap();
        assert_eq!(node.webgpu.len(), 2);
        assert_eq!(node.webgpu[0].busy, Some(got[0].task));
        assert_eq!(node.webgpu[0].billed_gflops, UNCHECKED_WEBGPU_GFLOPS);
        assert!(node.webgpu[0].note.is_some());
        assert_eq!(node.webgpu[1].billed_gflops, 800, "under the cap");
        assert!(node.labels["webgpu"].contains(&"2".to_string()));

        // The run on the GPU's adapter ends: billed at the WebGPU cap, and
        // the freed slot goes to the waiting WebGPU task first (same
        // priority, submitted earlier).
        let a = &got[0];
        let outcome = Outcome::Exited(0);
        let digest = pumasi_proto::receipt::result_digest(&outcome, b"ok", &[]);
        let usage = pumasi_proto::receipt::Usage {
            cluster: *st.key.public().as_bytes(),
            node: *bob_key.public().as_bytes(),
            task: a.task,
            executor: Executor::Wasm,
            outcome: outcome.clone(),
            fuel_used: 1000,
            wall_ms: 100,
            memory_limit_bytes: a.limits.memory_bytes,
            peak_memory_bytes: Some(65536),
            cpu_millis: None,
            result_digest: digest,
            finished_unix: now_unix(),
        };
        std::thread::sleep(Duration::from_millis(120));
        let generation = st.nodes[&bob_id].generation;
        st.on_report(
            &bob_id,
            generation,
            TaskReport {
                task: a.task,
                outcome,
                stdout: b"ok".to_vec(),
                stderr: vec![],
                files: vec![],
                fuel_used: 1000,
                wall_ms: 100,
                usage: Some(pumasi_proto::receipt::SignedUsage::sign(usage, &bob_key)),
            },
        );
        let receipts = st.receipts(id).unwrap();
        let r = &receipts[0];
        r.verify().unwrap();
        let g = r.gpu.as_ref().expect("GPU billing on the receipt");
        assert_eq!(g.names, vec!["Test adapter 0 (WebGPU, vulkan)".to_string()]);
        assert_eq!(g.gflops, UNCHECKED_WEBGPU_GFLOPS as u64);
        assert!(g.exact_ucu > 0);
        let next = assigned(&mut rx);
        assert_eq!(next.len(), 1, "{next:?}");
        assert_eq!(next[0].task.job, id);
        assert_eq!(next[0].gpus, vec![0]);

        // A node that stops offering WebGPU gets no more of these tasks.
        send_webgpu(&mut st, &bob_id, &bob_key, vec![]);
        let view = st.status();
        let node = view.nodes.iter().find(|n| n.id == bob_id).unwrap();
        assert!(node.webgpu.is_empty());
    }

    #[test]
    fn untrusted_gpus_are_timed_before_they_get_work() {
        for honest in [true, false] {
            let mut st = state();
            st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
            let (bob_id, _, mut rx) =
                join_gpus(&mut st, "bob", vec![gpu(0, 12_700, 2_000_000)], bob(true));
            let challenge = loop {
                match rx.try_recv() {
                    Ok(ToAgent::GpuChallenge(c)) => break c,
                    Ok(_) => continue,
                    Err(_) => panic!("no GPU challenge"),
                }
            };
            st.submit(gpu_job(1, 1)).unwrap();
            assert!(assigned(&mut rx).is_empty(), "no GPU work while timing");
            let mut answer =
                pumasi_proto::gpu::solve_cpu(challenge.seed, challenge.target, challenge.solutions);
            if !honest {
                answer[0] ^= 1;
            }
            let generation = st.nodes[&bob_id].generation;
            st.on_gpu_result(&bob_id, generation, challenge.nonce, answer);
            let view = st.status();
            let node = view.nodes.iter().find(|n| n.id == bob_id).unwrap();
            if honest {
                assert_eq!(node.gpus[0].billed_gflops, 12_700);
                assert_eq!(node.strikes, 0);
                let got = assigned(&mut rx);
                assert_eq!(got.len(), 1);
                assert_eq!(got[0].gpus, vec![0]);
            } else {
                assert_eq!(node.gpus[0].billed_gflops, 0, "wrong answers bill nothing");
                assert_eq!(node.strikes, 1);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Cluster management (phase 10)
    // -----------------------------------------------------------------------

    use pumasi_proto::manage::{
        AccessRole, AccountQuota, ApplySpec, NodeConfig, NodeEdit, NodeSpec, ScheduleSpec,
    };
    use pumasi_proto::msg::AdminOp;

    /// Joins an owner machine and keeps its key, to sign reports.
    fn join_keyed(
        st: &mut State,
        name: &str,
        slots: u32,
    ) -> (String, SecretKey, mpsc::UnboundedReceiver<ToAgent>) {
        let key = SecretKey::generate();
        let id = key.public().to_string();
        let token = st
            .create_token(Duration::from_secs(60), Some(1), None)
            .unwrap();
        let mut info = info(name, vec![Executor::Wasm]);
        info.slots = slots;
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
        (id, key, rx)
    }

    /// Reports a run as finished (exit 0) with a signed receipt.
    fn finish(st: &mut State, node: &str, key: &SecretKey, a: &TaskAssignment) {
        let outcome = Outcome::Exited(0);
        let digest = pumasi_proto::receipt::result_digest(&outcome, b"", &[]);
        let usage = pumasi_proto::receipt::Usage {
            cluster: *st.key.public().as_bytes(),
            node: *key.public().as_bytes(),
            task: a.task,
            executor: a.executor,
            outcome: outcome.clone(),
            fuel_used: 0,
            wall_ms: 1,
            memory_limit_bytes: a.limits.memory_bytes,
            peak_memory_bytes: None,
            cpu_millis: a.limits.cpu_millis,
            result_digest: digest,
            finished_unix: now_unix(),
        };
        let generation = st.nodes[node].generation;
        st.on_report(
            node,
            generation,
            TaskReport {
                task: a.task,
                outcome,
                stdout: vec![],
                stderr: vec![],
                files: vec![],
                fuel_used: 0,
                wall_ms: 1,
                usage: Some(pumasi_proto::receipt::SignedUsage::sign(usage, key)),
            },
        );
    }

    fn cancels(rx: &mut mpsc::UnboundedReceiver<ToAgent>) -> (Vec<TaskAssignment>, Vec<TaskRef>) {
        let (mut a, mut c) = (Vec::new(), Vec::new());
        while let Ok(m) = rx.try_recv() {
            match m {
                ToAgent::Assign(x) => a.push(*x),
                ToAgent::Cancel(t) => c.push(t),
                _ => {}
            }
        }
        (a, c)
    }

    fn labels(st: &mut State, node: &str, pairs: &[(&str, &str)]) {
        let edit = NodeEdit {
            set_labels: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        };
        st.edit_node(node, &edit).unwrap();
    }

    #[test]
    fn selectors_and_taints_choose_nodes() {
        let mut st = state();
        let (a_id, mut a_rx) = join(&mut st, "a", vec![Executor::Wasm], None);
        let (_, mut b_rx) = join(&mut st, "b", vec![Executor::Wasm], None);
        labels(&mut st, "a", &[("zone", "home")]);
        st.edit_node(
            "b",
            &NodeEdit {
                add_taints: vec!["gpu-only".parse().unwrap()],
                ..Default::default()
            },
        )
        .unwrap();
        let view = st.status();
        let a = view.nodes.iter().find(|n| n.id == a_id).unwrap();
        assert_eq!(a.labels["zone"], vec!["home".to_string()]);
        assert_eq!(a.labels["os"], vec!["linux".to_string()]);
        assert_eq!(a.labels["executor"], vec!["wasm".to_string()]);

        // Untolerated taint: everything goes to a, and the rest waits.
        let id = st.submit(job(Executor::Wasm, 3)).unwrap();
        assert_eq!(assigned(&mut a_rx).len(), 2);
        assert!(assigned(&mut b_rx).is_empty(), "b is tainted");
        assert_eq!(st.jobs[&id].tasks[2].state, TaskState::Pending);
        st.cancel(id).unwrap();

        // A selector nobody matches keeps the job queued.
        let mut sub = job(Executor::Wasm, 1);
        sub.selector = "zone=lab".parse().unwrap();
        let id = st.submit(sub).unwrap();
        assert!(assigned(&mut a_rx).is_empty() && assigned(&mut b_rx).is_empty());
        assert_eq!(st.jobs[&id].tasks[0].state, TaskState::Pending);
        // Labelling b matches it, but b's taint still keeps it off.
        labels(&mut st, "b", &[("zone", "lab")]);
        assert!(assigned(&mut b_rx).is_empty());
        st.cancel(id).unwrap();
        let mut sub = job(Executor::Wasm, 1);
        sub.selector = "zone=lab".parse().unwrap();
        sub.tolerations = vec!["gpu-only".parse().unwrap()];
        st.submit(sub).unwrap();
        assert_eq!(assigned(&mut b_rx).len(), 1);
    }

    #[test]
    fn drain_moves_runs_and_cordon_keeps_new_ones_off() {
        let mut st = state();
        let (_, mut a_rx) = join(&mut st, "a", vec![Executor::Wasm], None);
        let mut sub = job(Executor::Wasm, 2);
        sub.max_attempts = 1;
        let id = st.submit(sub).unwrap();
        assert_eq!(assigned(&mut a_rx).len(), 2);
        assert_eq!(st.drain("a").unwrap(), 2);
        let (got, cancelled) = cancels(&mut a_rx);
        assert!(got.is_empty(), "a drained node gets nothing new");
        assert_eq!(cancelled.len(), 2);
        // Moved runs do not count against max_attempts = 1.
        assert!(
            st.jobs[&id]
                .tasks
                .iter()
                .all(|t| t.state == TaskState::Pending)
        );
        let (_, mut b_rx) = join(&mut st, "b", vec![Executor::Wasm], None);
        let got = assigned(&mut b_rx);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|a| a.task.attempt == 2));
        let view = st.job_view(id, false).unwrap();
        assert!(view.tasks.iter().all(|t| t.preempted == 1));
        // Uncordon: a takes work again.
        st.edit_node(
            "a",
            &NodeEdit {
                cordon: Some(false),
                ..Default::default()
            },
        )
        .unwrap();
        st.submit(job(Executor::Wasm, 1)).unwrap();
        assert_eq!(assigned(&mut a_rx).len(), 1);
    }

    #[test]
    fn higher_priority_preempts_lower() {
        let mut st = state();
        let (_, mut rx) = join(&mut st, "a", vec![Executor::Wasm], None);
        let low = st.submit(job(Executor::Wasm, 2)).unwrap();
        assert_eq!(assigned(&mut rx).len(), 2);
        // Same priority: waits.
        let same = st.submit(job(Executor::Wasm, 1)).unwrap();
        let (got, cancelled) = cancels(&mut rx);
        assert!(got.is_empty() && cancelled.is_empty());
        // Higher priority: one low run makes room, and the high run goes
        // ahead of the waiting job.
        let mut sub = job(Executor::Wasm, 1);
        sub.priority = 10;
        let high = st.submit(sub).unwrap();
        let (got, cancelled) = cancels(&mut rx);
        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0].job, low);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].task.job, high);
        let v = st.job_view(low, false).unwrap();
        assert_eq!(
            v.tasks
                .iter()
                .filter(|t| t.state == TaskState::Pending)
                .count(),
            1
        );
        assert_eq!(v.tasks.iter().map(|t| t.preempted).sum::<u32>(), 1);
        // Queue order: the preempted low run is back before the later
        // job of the same priority.
        assert_eq!(st.queue.front().map(|q| q.job), Some(low));
        assert_eq!(st.queue.back().map(|q| q.job), Some(same));
        assert_eq!(
            st.status()
                .jobs
                .iter()
                .find(|j| j.id == high)
                .unwrap()
                .priority,
            10
        );
    }

    #[test]
    fn quotas_hold_work_and_cap_priorities() {
        let mut st = state();
        let (_, mut rx) = join(&mut st, "a", vec![Executor::Wasm], None);
        st.deposit("alice", 1_000_000, "").unwrap();
        st.set_account_quota(
            "alice",
            AccountQuota {
                slots: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        let mut sub = job(Executor::Wasm, 3);
        sub.account = Some("alice".into());
        st.submit(sub).unwrap();
        assert_eq!(assigned(&mut rx).len(), 1, "one slot for alice");
        // Other accounts still get the free slot.
        st.submit(job(Executor::Wasm, 1)).unwrap();
        assert_eq!(assigned(&mut rx).len(), 1);
        let q = st.quotas();
        let alice = q.iter().find(|q| q.account == "alice").unwrap();
        assert_eq!((alice.running, alice.queued), (1, 2));
        assert_eq!(alice.quota.slots, Some(1));

        // Submitters stay at priority 0 unless the quota allows more; the
        // admin may use any priority.
        let mut sub = job(Executor::Wasm, 1);
        sub.account = Some("alice".into());
        sub.priority = 5;
        assert!(st.submit_from(sub.clone(), true, None).is_err());
        st.submit_from(sub.clone(), false, None).unwrap();
        st.set_account_quota(
            "alice",
            AccountQuota {
                max_priority: Some(5),
                ..Default::default()
            },
        )
        .unwrap();
        st.submit_from(sub.clone(), true, None).unwrap();
        sub.priority = 6;
        assert!(
            st.submit_from(sub, false, None).is_err(),
            "the quota caps everyone"
        );

        // A daily CU limit that is used up keeps paid work off other
        // accounts' machines.
        st.set_account_quota(
            "alice",
            AccountQuota {
                cu_per_day: Some(1_000),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!st.over_daily("alice"));
        st.note_spend("alice", 1_000);
        assert!(st.over_daily("alice"));
    }

    #[test]
    fn dependent_jobs_wait_and_fail_with_their_dependency() {
        let mut st = state();
        let (a, key, mut rx) = join_keyed(&mut st, "a", 4);
        let first = st.submit(job(Executor::Wasm, 1)).unwrap();
        let run = assigned(&mut rx).pop().unwrap();
        let mut sub = job(Executor::Wasm, 2);
        sub.after = vec![first];
        let second = st.submit(sub).unwrap();
        assert!(assigned(&mut rx).is_empty(), "waits for the first job");
        let v = st.job_view(second, false).unwrap();
        assert_eq!(
            v.waiting.as_deref(),
            Some(&*format!("waiting for job {first}"))
        );
        assert_eq!(v.after, vec![first]);
        finish(&mut st, &a, &key, &run);
        assert_eq!(
            assigned(&mut rx).len(),
            2,
            "queued once the first succeeded"
        );
        assert!(st.job_view(second, false).unwrap().waiting.is_none());

        // A failed dependency fails the jobs after it, down the chain. (The
        // first one waits for a node nobody has, so cancelling ends it.)
        let mut sub = job(Executor::Wasm, 1);
        sub.selector = "zone=nowhere".parse().unwrap();
        let third = st.submit(sub).unwrap();
        let mut sub = job(Executor::Wasm, 1);
        sub.after = vec![third];
        let fourth = st.submit(sub).unwrap();
        let mut sub = job(Executor::Wasm, 1);
        sub.after = vec![fourth];
        let fifth = st.submit(sub).unwrap();
        st.cancel(third).unwrap();
        st.schedule();
        st.schedule();
        assert_eq!(Self_summary(&st, fourth), JobState::Failed);
        assert_eq!(Self_summary(&st, fifth), JobState::Failed);
        // Nothing to wait for: refused.
        let mut sub = job(Executor::Wasm, 1);
        sub.after = vec![third];
        assert!(st.submit(sub).is_err());
        let mut sub = job(Executor::Wasm, 1);
        sub.after = vec![JobId(42)];
        assert!(st.submit(sub).is_err());
    }

    #[allow(non_snake_case)]
    fn Self_summary(st: &State, id: JobId) -> JobState {
        State::summary(&st.jobs[&id]).state
    }

    #[test]
    fn schedules_submit_on_time_without_overlap() {
        let mut st = state();
        let spec = ScheduleSpec {
            name: "nightly".into(),
            cron: "0 3 * * *".into(),
            job: job(Executor::Wasm, 2),
            overlap: false,
        };
        st.set_schedule(spec.clone()).unwrap();
        let next = st.schedules()[0].next_unix.unwrap();
        assert!(next > now_unix() && next <= now_unix() + 24 * 3600 + 3600);
        // Setting the same schedule again changes nothing.
        st.set_schedule(spec).unwrap();
        assert_eq!(st.schedules()[0].next_unix, Some(next));
        // Due: submitted with the schedule's name.
        st.mgmt.schedules.get_mut("nightly").unwrap().next_unix = Some(now_unix() - 1);
        st.tick_schedules(now_unix());
        let v = st.schedules();
        let id = v[0].last_job.expect("submitted");
        assert_eq!(v[0].runs, 1);
        assert!(v[0].next_unix.unwrap() > now_unix());
        let job = st.job_view(id, false).unwrap();
        assert_eq!(job.schedule.as_deref(), Some("nightly"));
        assert_eq!(job.summary.tasks, 2);
        // Due again while the last run has not finished: skipped.
        st.mgmt.schedules.get_mut("nightly").unwrap().next_unix = Some(now_unix() - 1);
        st.tick_schedules(now_unix());
        let v = st.schedules();
        assert_eq!(v[0].runs, 1);
        assert!(
            v[0].last_error
                .as_deref()
                .unwrap()
                .contains("has not finished")
        );
        st.remove_schedule("nightly").unwrap();
        assert!(st.schedules().is_empty());
    }

    #[test]
    fn apply_twice_changes_nothing_the_second_time() {
        let mut st = state();
        join(&mut st, "a", vec![Executor::Wasm], None);
        let spec = ApplySpec {
            nodes: vec![
                NodeSpec {
                    node: "a".into(),
                    labels: [("zone".to_string(), "home".to_string())].into(),
                    taints: vec!["gpu-only".parse().unwrap()],
                    cordoned: Some(true),
                },
                NodeSpec {
                    node: "missing".into(),
                    labels: Default::default(),
                    taints: vec![],
                    cordoned: None,
                },
            ],
            accounts: vec![(
                "alice".into(),
                AccountQuota {
                    slots: Some(2),
                    storage_bytes: Some(1 << 30),
                    ..Default::default()
                },
            )],
            schedules: vec![ScheduleSpec {
                name: "hourly".into(),
                cron: "@hourly".into(),
                job: job(Executor::Wasm, 1),
                overlap: true,
            }],
            prune: true,
            dry_run: false,
            ..Default::default()
        };
        let mut dry = spec.clone();
        dry.dry_run = true;
        let planned = st.apply(dry).unwrap();
        assert_eq!(planned.len(), 4, "{planned:?}");
        assert!(st.schedules().is_empty(), "a dry run changes nothing");
        let first = st.apply(spec.clone()).unwrap();
        assert_eq!(first, planned);
        assert!(first[0].contains("labels {} -> {zone=home}"), "{first:?}");
        assert!(first[1].contains("skipped"), "{first:?}");
        let second = st.apply(spec.clone()).unwrap();
        assert_eq!(
            second,
            vec![first[1].clone()],
            "only the skipped node again"
        );
        assert_eq!(st.account_quota("alice").storage_bytes, Some(1 << 30));
        assert!(st.status().nodes[0].config.cordoned);
        // Pruning removes schedules the file no longer lists.
        st.set_schedule(ScheduleSpec {
            name: "extra".into(),
            ..spec.schedules[0].clone()
        })
        .unwrap();
        let third = st.apply(spec).unwrap();
        assert!(
            third.iter().any(|c| c == "schedule extra: removed"),
            "{third:?}"
        );
        assert_eq!(st.schedules().len(), 1);
    }

    #[test]
    fn access_tickets_have_roles() {
        use crate::manage::Caller;
        let mut st = state();
        let (node, _) = join(&mut st, "a", vec![Executor::Wasm], None);
        let sub = st
            .create_access(
                "alice laptop",
                AccessRole::Submitter {
                    account: "alice".into(),
                },
                None,
            )
            .unwrap();
        let viewer = st
            .create_access(
                "dashboard",
                AccessRole::Viewer,
                Some(Duration::from_secs(60)),
            )
            .unwrap();
        let alice = st.access_caller(&sub.secret).unwrap();
        assert_eq!(
            alice,
            Caller::Submitter {
                name: "alice laptop".into(),
                account: "alice".into()
            }
        );
        assert!(st.access_caller(&pumasi_proto::new_secret()).is_none());
        // A submitter cannot remove nodes, and submits only for its account.
        let err = crate::access::admit(&st, &alice, AdminOp::RemoveNode { id: node.clone() })
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("cannot remove nodes"),
            "{err:#}"
        );
        let AdminOp::Submit(pinned) = crate::access::admit(
            &st,
            &alice,
            AdminOp::Submit(Box::new(job(Executor::Wasm, 1))),
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(pinned.account.as_deref(), Some("alice"));
        let mut other = job(Executor::Wasm, 1);
        other.account = Some("bob".into());
        assert!(crate::access::admit(&st, &alice, AdminOp::Submit(Box::new(other))).is_err());
        // Other accounts' jobs are out of reach.
        let owners = st.submit(job(Executor::Wasm, 1)).unwrap();
        assert!(crate::access::admit(&st, &alice, AdminOp::Cancel { id: owners }).is_err());
        let mine = st.submit(*pinned).unwrap();
        assert!(crate::access::admit(&st, &alice, AdminOp::Cancel { id: mine }).is_ok());
        let status = crate::access::scope(
            &alice,
            pumasi_proto::msg::AdminResponse::Status(st.status()),
        );
        let pumasi_proto::msg::AdminResponse::Status(status) = status else {
            panic!()
        };
        assert_eq!(status.jobs.len(), 1);
        assert_eq!(status.jobs[0].id, mine);
        // A viewer reads but changes nothing.
        let v = st.access_caller(&viewer.secret).unwrap();
        assert!(crate::access::admit(&st, &v, AdminOp::Status).is_ok());
        assert!(crate::access::admit(&st, &v, AdminOp::Cancel { id: mine }).is_err());
        assert_eq!(st.list_access().len(), 2);
        let gone = st.revoke_access("alice laptop").unwrap();
        assert_eq!(gone.len(), 1);
        assert!(st.access_caller(&sub.secret).is_none());
    }

    // -- monitoring (phase 11) -----------------------------------------------

    fn events_of(st: &State) -> Vec<String> {
        st.events(0, 0, 1000).into_iter().map(|e| e.kind).collect()
    }

    #[test]
    fn alerts_fire_and_resolve() {
        let mut st = state();
        st.set_alert_policy(pumasi_proto::monitor::AlertPolicy {
            node_offline_secs: Some(0),
            failure_rate: Some(pumasi_proto::monitor::FailureRate {
                percent: 50,
                window_secs: 600,
                min_runs: 2,
            }),
            ..Default::default()
        })
        .unwrap();
        let (a, _rx) = join(&mut st, "a", vec![Executor::Wasm], None);
        st.tick_monitor();
        assert!(st.alerts_view().firing.is_empty());

        let generation = st.nodes[&a].generation;
        st.on_disconnect(&a, generation);
        st.tick_monitor();
        let firing = st.alerts_view().firing;
        assert_eq!(firing.len(), 1, "{firing:?}");
        assert_eq!(firing[0].rule, "node_offline");
        assert_eq!(firing[0].subject, "a");
        let n = st.recent_notices();
        assert_eq!(n.last().unwrap().status, "firing");
        assert!(n.last().unwrap().message.contains("node a offline"));
        // Still offline: no second notice.
        st.tick_monitor();
        assert_eq!(st.recent_notices().len(), 1);

        // Back online: resolved.
        let (tx, _rx2) = mpsc::unbounded_channel();
        let (kill, _) = oneshot::channel();
        st.on_connect(
            &a,
            pumasi_proto::PROTOCOL_VERSION,
            info("a", vec![Executor::Wasm]),
            None,
            Availability::Available,
            tx,
            kill,
        );
        st.tick_monitor();
        assert!(st.alerts_view().firing.is_empty());
        assert_eq!(st.recent_notices().last().unwrap().status, "resolved");

        // A cordoned node may go away quietly.
        let c = NodeConfig {
            cordoned: true,
            ..Default::default()
        };
        st.set_node_config(&a, c).unwrap();
        let generation = st.nodes[&a].generation;
        st.on_disconnect(&a, generation);
        st.tick_monitor();
        assert!(st.alerts_view().firing.is_empty());

        // Half the runs failing.
        let t = TaskRef {
            job: JobId(1),
            index: 0,
            attempt: 1,
            check: false,
        };
        st.note_run(&a, t, &Outcome::Exited(0));
        st.note_run(&a, t, &Outcome::Exited(1));
        st.tick_monitor();
        let firing = st.alerts_view().firing;
        assert_eq!(firing[0].rule, "failure_rate", "{firing:?}");
        assert!(firing[0].message.contains("1 of 2 runs failed"));

        let kinds = events_of(&st);
        for k in [
            "node.joined",
            "node.online",
            "node.offline",
            "alert.firing",
            "alert.resolved",
            "task.failed",
        ] {
            assert!(kinds.iter().any(|x| x == k), "no {k} in {kinds:?}");
        }
    }

    #[test]
    fn disk_temperature_and_smart_alerts_from_node_reports() {
        let mut st = state();
        let (a, _rx) = join(&mut st, "a", vec![Executor::Wasm], None);
        let generation = st.nodes[&a].generation;
        st.on_metrics(
            &a,
            generation,
            pumasi_proto::monitor::NodeMetrics {
                disk_used: 95,
                disk_total: 100,
                temp_c: Some(95.0),
                ..Default::default()
            },
        );
        st.monitor_cp_disk(Some((10, 100)));
        st.tick_monitor();
        let rules: Vec<String> = st
            .alerts_view()
            .firing
            .into_iter()
            .map(|a| a.rule)
            .collect();
        assert_eq!(rules, ["disk", "temperature"]);
        st.monitor_cp_disk(Some((99, 100)));
        st.tick_monitor();
        assert!(
            st.alerts_view()
                .firing
                .iter()
                .any(|a| a.subject == "control-plane")
        );
    }

    #[test]
    fn live_logs_follow_a_running_task() {
        let mut st = state();
        let (a, key, mut rx) = join_keyed(&mut st, "a", 1);
        let id = st.submit(job(Executor::Wasm, 1)).unwrap();
        let run = assigned(&mut rx).pop().unwrap();
        let generation = st.nodes[&a].generation;
        let first = st.logs(id, 0, 0, 0, 0).unwrap();
        assert_eq!(first.attempt, 1);
        assert!(first.stdout.is_empty() && !first.done);

        st.on_logs(&a, generation, run.task, b"hel".to_vec(), b"warn".to_vec());
        // Another node cannot write into it.
        let (b, _rxb) = join(&mut st, "b", vec![Executor::Wasm], None);
        let gb = st.nodes[&b].generation;
        st.on_logs(&b, gb, run.task, b"forged".to_vec(), vec![]);
        let c = st.logs(id, 0, 1, 0, 0).unwrap();
        assert_eq!(c.stdout, b"hel");
        assert_eq!(c.stderr, b"warn");
        assert!(c.live);
        st.on_logs(&a, generation, run.task, b"lo".to_vec(), vec![]);
        let c = st.logs(id, 0, 1, 3, 4).unwrap();
        assert_eq!((c.stdout_from, c.stdout.as_slice()), (3, b"lo".as_slice()));
        assert!(c.stderr.is_empty());
        // Asking for an older run starts again from the beginning.
        let c = st.logs(id, 0, 0, 3, 4).unwrap();
        assert_eq!(c.stdout, b"hello");

        finish(&mut st, &a, &key, &run);
        let c = st.logs(id, 0, 1, 0, 0).unwrap();
        assert!(c.done);
        assert_eq!(c.state, TaskState::Succeeded);
        assert!(
            st.monitor.live.is_empty(),
            "the live copy goes with the run"
        );
        st.tick_monitor();
        st.monitor.last_jobs_check -= Duration::from_secs(2);
        st.tick_monitor();
        assert!(events_of(&st).iter().any(|k| k == "job.succeeded"));
        assert_eq!(st.monitor.counters.runs_succeeded, 1);
    }

    #[test]
    fn prometheus_text_lints() {
        let mut st = state();
        let (a, _rx) = join(&mut st, "a \"quoted\"", vec![Executor::Wasm], None);
        let generation = st.nodes[&a].generation;
        st.on_metrics(
            &a,
            generation,
            pumasi_proto::monitor::NodeMetrics {
                cpu_percent: 50.0,
                memory_used: 1 << 30,
                memory_total: 2 << 30,
                disk_used: 1,
                disk_total: 2,
                net_rx: 10,
                net_tx: 20,
                temp_c: Some(40.0),
                gpus: vec![pumasi_proto::monitor::GpuMetrics {
                    index: 0,
                    util_percent: 30.0,
                    mem_used_mib: 100,
                    mem_total_mib: 8000,
                    temp_c: Some(55.0),
                    power_w: Some(120.5),
                }],
                ..Default::default()
            },
        );
        st.submit(job(Executor::Docker, 3)).unwrap();
        let text = st.prometheus();
        let n = pumasi_proto::monitor::lint_exposition(&text)
            .unwrap_or_else(|e| panic!("{e:#}\n{text}"));
        assert!(n > 20, "{text}");
        assert!(
            text.contains("pumasi_node_cpu_usage_ratio{node=\"a \\\"quoted\\\"\""),
            "{text}"
        );
        assert!(text.contains("pumasi_gpu_power_watts{"), "{text}");
        assert!(
            text.contains("pumasi_queued_runs{priority=\"0\"} 3"),
            "{text}"
        );
        assert!(text.contains("pumasi_jobs{state=\"queued\"} 1"), "{text}");
        assert!(text.contains("pumasi_node_up{"), "{text}");
    }

    #[test]
    fn monitoring_calls_and_roles() {
        let mut st = state();
        let sub = st
            .create_access(
                "alice",
                AccessRole::Submitter {
                    account: "alice".into(),
                },
                None,
            )
            .unwrap();
        let viewer = st.create_access("wall", AccessRole::Viewer, None).unwrap();
        let alice = st.access_caller(&sub.secret).unwrap();
        let v = st.access_caller(&viewer.secret).unwrap();
        let owners = st.submit(job(Executor::Wasm, 1)).unwrap();
        let logs = |id| AdminOp::Logs {
            job: id,
            index: 0,
            attempt: 0,
            stdout_from: 0,
            stderr_from: 0,
        };
        for op in [
            AdminOp::Metrics {
                node: None,
                since_secs: 60,
            },
            AdminOp::Events {
                after: 0,
                since_unix: 0,
                limit: 10,
            },
            AdminOp::Alerts,
            logs(owners),
        ] {
            assert!(crate::access::admit(&st, &v, op).is_ok());
        }
        assert!(crate::access::admit(&st, &alice, logs(owners)).is_err());
        assert!(crate::access::admit(&st, &alice, AdminOp::Alerts).is_ok());
        let hook = AdminOp::SetWebhook(Box::new(pumasi_proto::monitor::WebhookSpec {
            name: "ops".into(),
            url: "https://hooks.slack.com/services/x".into(),
            format: pumasi_proto::monitor::WebhookFormat::Slack,
        }));
        let err = crate::access::admit(&st, &v, hook).unwrap_err();
        assert!(
            format!("{err:#}").contains("cannot change webhooks"),
            "{err:#}"
        );
        // A submitter sees node events and its own jobs' events only.
        let mut mine = job(Executor::Wasm, 1);
        mine.account = Some("alice".into());
        st.submit(mine).unwrap();
        let resp = crate::access::scope(
            &alice,
            pumasi_proto::msg::AdminResponse::Events(st.events(0, 0, 100)),
        );
        let pumasi_proto::msg::AdminResponse::Events(seen) = resp else {
            panic!()
        };
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert_eq!(seen[0].account.as_deref(), Some("alice"));
    }

    // -- phase 16: tiers and the key broker ----------------------------------

    fn tiered(tier: Tier) -> JobSubmission {
        JobSubmission {
            tier,
            max_attempts: 10,
            ..job(Executor::Microvm, 1)
        }
    }

    #[test]
    fn trusted_jobs_run_on_owner_machines_and_trusted_providers() {
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        let (bob_id, mut bob_rx) = join(
            &mut st,
            "bob",
            vec![Executor::Wasm, Executor::Microvm],
            bob(false),
        );
        let id = st.submit(tiered(Tier::Trusted)).unwrap();
        assert!(
            assigned(&mut bob_rx).is_empty(),
            "a provider got a trusted job"
        );
        assert_eq!(st.jobs[&id].tasks[0].state, TaskState::Pending);
        st.trust_node("bob", true).unwrap();
        assert_eq!(assigned(&mut bob_rx).len(), 1);
        assert!(
            st.confidential_view()
                .nodes
                .iter()
                .any(|n| n.id == bob_id && n.trusted_by_admin)
        );
        // Kept across a restart.
        let snap = st.store.load().unwrap();
        assert_eq!(snap.trusted_nodes, vec![bob_id]);
    }

    /// A confidential job whose image and first input are sealed (what
    /// the store worker would have done).
    fn sealed_job(st: &mut State) -> JobId {
        let id = st.submit(tiered(Tier::Confidential)).unwrap();
        let job = st.jobs.get_mut(&id).unwrap();
        assert!(
            job.meta.ext.private_image,
            "confidential images go through the store"
        );
        job.meta.ext.images.insert(
            "amd64".into(),
            crate::datasets::ImageState::Ready(ImageBlob {
                root: [1; 32],
                digest: "sha256:x".into(),
                reference: "busybox".into(),
            }),
        );
        let sealed = crate::confidential::SealedInput {
            image: pumasi_proto::confidential::SealedBlob {
                root: [1; 32],
                size: 10,
            },
            input: pumasi_proto::confidential::SealedBlob {
                root: [2; 32],
                size: 20,
            },
            output_bytes: 1 << 20,
            extra_mib: 64,
        };
        st.conf.inputs.insert((id, 0), sealed);
        id
    }

    fn test_tee(st: &mut State, allowed: bool) {
        st.attach_verifier(Arc::new(crate::attest::Verifier::new(allowed).unwrap()));
    }

    fn tee_report(st: &mut State, node: &str, tees: Vec<pumasi_proto::confidential::TeeKind>) {
        let generation = st.nodes[node].generation;
        st.on_tee(
            node,
            generation,
            pumasi_proto::confidential::TeeReport {
                tees,
                unavailable: None,
            },
        );
    }

    /// What a test-TEE guest would send for `nonce`: its secret key and
    /// the evidence.
    fn guest_evidence(
        nonce: &[u8; 32],
        measurement: u8,
    ) -> ([u8; 32], pumasi_proto::confidential::Evidence) {
        let (secret, public) = pumasi_guest::seal::key_pair();
        let ev = pumasi_proto::confidential::Evidence {
            tee: pumasi_proto::confidential::TeeKind::Test,
            report: vec![measurement; 48],
            aux: vec![],
            key: public,
            report_data: pumasi_guest::seal::report_data(nonce, &public).to_vec(),
        };
        (secret, ev)
    }

    #[test]
    fn confidential_runs_go_only_to_tee_nodes_and_carry_nothing_in_the_clear() {
        use pumasi_proto::confidential::TeeKind;
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        test_tee(&mut st, false);
        let (node, mut rx) = join(
            &mut st,
            "own",
            vec![Executor::Wasm, Executor::Microvm],
            None,
        );
        let id = sealed_job(&mut st);
        st.schedule();
        assert!(
            assigned(&mut rx).is_empty(),
            "no TEE, yet it got a confidential run"
        );
        // The test stand-in counts only on a control plane that allows it.
        tee_report(&mut st, &node, vec![TeeKind::Test]);
        assert!(assigned(&mut rx).is_empty());
        let need = st
            .need_for(&QueueItem {
                job: id,
                index: 0,
                check: false,
                priority: 0,
                since: Instant::now(),
            })
            .unwrap();
        let n = &st.nodes[&node];
        let why = st.refusal(&node, n, &need, &HashMap::new(), 0, &[]);
        assert!(matches!(why, Some(Refusal::Tee(_))), "{why:?}");
        test_tee(&mut st, true);
        st.schedule();
        let got = assigned(&mut rx);
        assert_eq!(got.len(), 1);
        let a = &got[0];
        let c = a.confidential.as_ref().expect("a confidential assignment");
        assert_eq!(c.tee, TeeKind::Test);
        assert_eq!(c.input.root, [2; 32]);
        assert_eq!(
            a.docker.as_ref().unwrap().image,
            "",
            "the image name went out"
        );
        assert_eq!(a.input, TaskInput::default());
        assert!(a.files.is_none() && a.mounts.is_empty() && a.image.is_none());
        assert_eq!(a.output, OutputMode::Store);
        assert!(st.conf.leases.contains_key(&a.task));
    }

    /// The gate: keys go only to a fresh attestation, for this run, from
    /// the node it was placed on, once, with a measurement an admin
    /// trusts. Everything else gets nothing.
    #[test]
    fn keys_only_for_fresh_genuine_trusted_attestations() {
        use crate::attest::check_test;
        use pumasi_proto::confidential::TeeKind;
        let mut st = state();
        st.deposit(OWNER_ACCOUNT, 1_000_000, "").unwrap();
        test_tee(&mut st, true);
        let (node, mut rx) = join(
            &mut st,
            "own",
            vec![Executor::Wasm, Executor::Microvm],
            None,
        );
        let (other, _other_rx) = join(&mut st, "other", vec![Executor::Wasm], None);
        tee_report(&mut st, &node, vec![TeeKind::Test]);
        let id = sealed_job(&mut st);
        st.schedule();
        let a = assigned(&mut rx).pop().expect("placed");
        let nonce = a.confidential.as_ref().unwrap().nonce;
        let generation = st.nodes[&node].generation;
        let other_gen = st.nodes[&other].generation;
        let (_, ev) = guest_evidence(&nonce, 7);

        // Another node cannot ask for this run's keys.
        assert!(st.begin_attest(&other, other_gen, a.task, &ev).is_err());
        // Evidence from another TEE than the run was placed for.
        let snp = pumasi_proto::confidential::Evidence {
            tee: TeeKind::SevSnp,
            ..ev.clone()
        };
        assert!(st.begin_attest(&node, generation, a.task, &snp).is_err());
        // That used the lease up: no second try for this run.
        assert!(st.begin_attest(&node, generation, a.task, &ev).is_err());

        // The next run (the first one failed).
        st.on_report_rejected(&node, generation, a.task, "test");
        let a = assigned(&mut rx).pop().expect("placed again");
        let nonce = a.confidential.as_ref().unwrap().nonce;
        // A replayed attestation (the last run's nonce) does not check out.
        let (expected, _) = st.begin_attest(&node, generation, a.task, &ev).unwrap();
        let checked = check_test(&ev, &expected, true);
        assert!(checked.is_err());
        assert!(st.finish_attest(&node, a.task, &ev, checked).is_err());

        // A fresh one checks out, but no admin trusts its measurement yet.
        st.on_report_rejected(&node, generation, a.task, "test");
        let a = assigned(&mut rx).pop().expect("placed again");
        let nonce_b = a.confidential.as_ref().unwrap().nonce;
        assert_ne!(nonce, nonce_b, "every run gets its own nonce");
        let (_, ev) = guest_evidence(&nonce_b, 7);
        let (expected, _) = st.begin_attest(&node, generation, a.task, &ev).unwrap();
        let checked = check_test(&ev, &expected, true);
        let why = st.finish_attest(&node, a.task, &ev, checked).unwrap_err();
        assert!(why.contains("not trusted"), "{why}");
        let view = st.confidential_view();
        assert_eq!(view.measurements.len(), 1);
        let m = view.measurements[0].clone();
        assert!(!m.trusted);
        assert_eq!(m.seen, 1);
        // Too short a prefix, or one that matches nothing.
        assert!(st.trust_measurement(&m.id[..4], true, "").is_err());
        assert!(st.trust_measurement("ffffffffff", true, "").is_err());
        st.trust_measurement(&m.id[..12], true, "test guest")
            .unwrap();

        // Trusted, fresh and for this run: the keys, sealed to the guest.
        st.on_report_rejected(&node, generation, a.task, "test");
        let a = assigned(&mut rx).pop().expect("placed again");
        let nonce = a.confidential.as_ref().unwrap().nonce;
        let (secret, ev) = guest_evidence(&nonce, 7);
        let (expected, _) = st.begin_attest(&node, generation, a.task, &ev).unwrap();
        let checked = check_test(&ev, &expected, true);
        let wrapped = st.finish_attest(&node, a.task, &ev, checked).unwrap();
        let keys = pumasi_guest::seal::unwrap_keys(&secret, &nonce, &wrapped).unwrap();
        let job_key = st.job_key(id);
        assert_eq!(keys.task, pumasi_guest::seal::task_key(&job_key, 0));
        assert_eq!(keys.image, pumasi_guest::seal::image_key(&job_key));
        // Nobody else can open them.
        let (stranger, _) = pumasi_guest::seal::key_pair();
        assert!(pumasi_guest::seal::unwrap_keys(&stranger, &nonce, &wrapped).is_err());

        // Another measurement (a changed guest) is not trusted.
        st.on_report_rejected(&node, generation, a.task, "test");
        let a = assigned(&mut rx).pop().expect("placed again");
        let nonce = a.confidential.as_ref().unwrap().nonce;
        let (_, ev) = guest_evidence(&nonce, 8);
        let (expected, _) = st.begin_attest(&node, generation, a.task, &ev).unwrap();
        let checked = check_test(&ev, &expected, true);
        assert!(st.finish_attest(&node, a.task, &ev, checked).is_err());

        // Untrusting stops the keys; trust survives a restart.
        st.trust_measurement(&m.id, false, "").unwrap();
        let snap = st.store.load().unwrap();
        assert_eq!(snap.measurements.len(), 2);
        assert!(snap.measurements.iter().all(|(_, r)| !r.trusted));
    }

    // -----------------------------------------------------------------------
    // Release 1.4: task timings, node names, checking a job
    // -----------------------------------------------------------------------

    #[test]
    fn task_times_and_node_names_are_kept() {
        use pumasi_proto::msg::TaskTiming;
        let path =
            std::env::temp_dir().join(format!("pumasi-state-{}.redb", rand::random::<u64>()));
        let key = SecretKey::generate();
        let open = |key: &SecretKey| {
            State::load(
                "t".into(),
                key.public().to_string(),
                Store::open(&path).unwrap(),
                key.clone(),
            )
            .unwrap()
        };
        let mut st = open(&key);
        let (node, node_key, mut rx) = join_keyed(&mut st, "render-1", 1);
        let id = st.submit(job(Executor::Wasm, 2)).unwrap();
        let first = assigned(&mut rx).pop().expect("one slot, one run");
        // Nothing measured yet; the node already has its name.
        let d = st.task_details(id).unwrap();
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].node_name.as_deref(), Some("render-1"));
        assert_eq!(d[1].node_name, None);
        assert_eq!((d[0].pull_ms, d[0].run_ms), (0, 0));

        // The second task waits for the slot while the first runs.
        std::thread::sleep(Duration::from_millis(40));
        let generation = st.nodes[&node].generation;
        // What the node measured comes with its report. It is the node's
        // word, so nothing is longer than the run was seen to take.
        st.note_timing(
            &node,
            generation,
            first.task,
            TaskTiming {
                fetch_ms: 5,
                pull_ms: 20,
                upload_ms: 3_600_000,
            },
        );
        finish(&mut st, &node, &node_key, &first);
        let d = st.task_details(id).unwrap();
        assert_eq!((d[0].fetch_ms, d[0].pull_ms), (5, 20));
        assert!(d[0].upload_ms < 60_000, "{}", d[0].upload_ms);
        assert_eq!(d[0].run_ms, 1, "the run is the report's wall time");
        assert_eq!(d[0].run_ms, st.jobs[&id].tasks[0].wall_ms);

        let second = assigned(&mut rx).pop().expect("the slot is free");
        let d = st.task_details(id).unwrap();
        assert!(d[1].queued_ms >= 40, "{}", d[1].queued_ms);
        assert!(d[0].queued_ms < d[1].queued_ms);
        // A report without timings (an agent older than 1.4) leaves them 0.
        finish(&mut st, &node, &node_key, &second);
        let d = st.task_details(id).unwrap();
        assert_eq!((d[1].fetch_ms, d[1].pull_ms, d[1].upload_ms), (0, 0, 0));
        // Timings for a run the node does not have are dropped.
        st.note_timing(&node, generation, second.task, TaskTiming::default());
        assert!(st.mgmt.report_timing.is_empty());
        assert!(st.task_details(JobId(1)).is_none());

        // They survive a restart, and go with the job.
        let before = st.task_details(id).unwrap();
        drop(st);
        let st = open(&key);
        // The node is offline now; its name is still the member's.
        let after = st.task_details(id).unwrap();
        assert_eq!(after[0].node_name.as_deref(), Some("render-1"));
        assert_eq!(after, before);
        assert_eq!(st.store.load().unwrap().task_times.len(), 2);
        st.store.delete_jobs(&[(id.0, 2)], &[]).unwrap();
        assert!(st.store.load().unwrap().task_times.is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn checking_a_job_answers_like_submitting_it() {
        use crate::manage::Caller;
        use pumasi_proto::manage::AccountPolicy;
        let mut st = state();
        st.deposit("ci", 1_000_000, "").unwrap();
        let (_, _rx) = join(
            &mut st,
            "docker-1",
            vec![Executor::Wasm, Executor::Docker],
            None,
        );
        let (_, _rx2) = join(&mut st, "wasm-1", vec![Executor::Wasm], None);
        st.set_account_policy(
            "ci",
            AccountPolicy {
                images: vec!["python:3.12-slim".into()],
                deny_network: true,
                ..Default::default()
            },
        )
        .unwrap();
        let ticket = st
            .create_access(
                "ci bot",
                AccessRole::Submitter {
                    account: "ci".into(),
                },
                None,
            )
            .unwrap();
        let caller = st.access_caller(&ticket.secret).unwrap();
        assert!(matches!(caller, Caller::Submitter { .. }));
        let check = |st: &State, sub: JobSubmission| -> Result<pumasi_proto::msg::JobCheck> {
            let op = crate::access::admit(
                st,
                &caller,
                AdminOp::CheckJob {
                    sub: Box::new(sub),
                    upload_bytes: 0,
                },
            )?;
            let AdminOp::CheckJob { sub, upload_bytes } = op else {
                panic!()
            };
            st.check_job(*sub, upload_bytes, true)
        };
        let submit = |st: &mut State, sub: JobSubmission| -> Result<JobId> {
            let AdminOp::Submit(sub) =
                crate::access::admit(st, &caller, AdminOp::Submit(Box::new(sub)))?
            else {
                panic!()
            };
            st.submit_from(*sub, true, None)
        };
        let image = |name: &str| {
            let mut j = job(Executor::Docker, 3);
            j.docker.as_mut().unwrap().image = name.into();
            j
        };

        // Allowed: says which nodes would take it, and creates nothing.
        let jobs = st.jobs.len();
        let ok = check(&st, image("python:3.12-slim")).unwrap();
        assert_eq!(ok.account, "ci");
        assert_eq!(ok.tasks, 3);
        assert_eq!(ok.ready_nodes, ["docker-1"]);
        assert_eq!(ok.waiting, None);
        assert_eq!(st.jobs.len(), jobs);
        assert!(st.queue.is_empty());

        // Refused with the words the submission is refused with.
        for bad in [
            image("blender:latest"),
            {
                let mut j = image("python:3.12-slim");
                j.docker.as_mut().unwrap().network = true;
                j
            },
            {
                let mut j = image("python:3.12-slim");
                j.priority = 5;
                j
            },
            {
                let mut j = image("python:3.12-slim");
                j.account = Some("someone-else".into());
                j
            },
            {
                let mut j = image("python:3.12-slim");
                j.after = vec![JobId(42)];
                j
            },
        ] {
            let checked = format!("{:#}", check(&st, bad.clone()).unwrap_err());
            let submitted = format!("{:#}", submit(&mut st, bad).unwrap_err());
            assert_eq!(checked, submitted);
        }
        let err = format!("{:#}", check(&st, image("blender:latest")).unwrap_err());
        assert!(
            err.contains("account's policy does not allow the image"),
            "{err}"
        );
        assert_eq!(st.jobs.len(), jobs);

        // Accepted, but nothing would run it now: the answer says why.
        let wasm_only = check(&st, job(Executor::Wasm, 1)).unwrap();
        assert_eq!(wasm_only.ready_nodes, ["docker-1", "wasm-1"]);
        let mut gpu = image("python:3.12-slim");
        gpu.gpu = Some(GpuRequest {
            count: 1,
            vendor: None,
            min_vram_mib: 0,
        });
        let waits = check(&st, gpu).unwrap();
        assert!(waits.ready_nodes.is_empty());
        let why = waits.waiting.unwrap();
        assert!(why.contains("docker-1") && why.contains("wasm-1"));

        // At its quota the job would be accepted and wait.
        st.set_account_quota(
            "ci",
            AccountQuota {
                slots: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        submit(&mut st, image("python:3.12-slim")).unwrap();
        let waits = check(&st, image("python:3.12-slim")).unwrap();
        assert!(waits.ready_nodes.is_empty());
        assert!(waits.waiting.unwrap().contains("quota"));

        // A job that waits for another says so.
        let first = *st.jobs.keys().next().unwrap();
        let mut after = image("python:3.12-slim");
        after.after = vec![first];
        let waits = check(&st, after).unwrap();
        assert!(waits.waiting.unwrap().contains("waits for job"));

        // Viewers may not check (they may not submit); admins are not
        // held to the policy.
        let viewer = st.create_access("v", AccessRole::Viewer, None).unwrap();
        let viewer = st.access_caller(&viewer.secret).unwrap();
        assert!(
            crate::access::admit(
                &st,
                &viewer,
                AdminOp::CheckJob {
                    sub: Box::new(job(Executor::Wasm, 1)),
                    upload_bytes: 0,
                },
            )
            .is_err()
        );
        let mut admins = image("blender:latest");
        admins.account = Some("ci".into());
        assert!(st.check_job(admins, 0, false).is_ok());
    }

    #[test]
    fn capacity_counts_only_what_the_policy_allows() {
        use pumasi_proto::manage::AccountPolicy;
        let mut st = state();
        st.deposit("ci", 1_000_000, "").unwrap();
        let (a, _, _rx) = join_gpus(&mut st, "gpu-a", vec![gpu(0, 12_700, 0)], None);
        let (_, _, _rx2) = join_gpus(&mut st, "gpu-b", vec![gpu(0, 12_700, 0)], None);
        labels(&mut st, &a, &[("zone", "lab")]);
        let cap = st.capacity("ci");
        assert_eq!(cap.ready_for(Executor::Docker), 2);
        assert_eq!(cap.ready_gpu, 2);

        // Executors and the selector the policy adds to every job.
        st.set_account_policy(
            "ci",
            AccountPolicy {
                executors: vec![Executor::Docker],
                selector: "zone=lab".parse().unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let cap = st.capacity("ci");
        assert_eq!(cap.ready.len(), 1, "only the executors it may use");
        assert_eq!(cap.ready_for(Executor::Docker), 1);
        assert_eq!(cap.ready_gpu, 1);
        let other = cap.nodes.iter().find(|n| n.node == "gpu-b").unwrap();
        assert!(other.executors.is_empty());
        assert!(other.reason.is_some());

        // A policy that gives tasks no GPU has no GPU node ready.
        st.set_account_policy(
            "ci",
            AccountPolicy {
                max_gpus_per_task: Some(0),
                ..Default::default()
            },
        )
        .unwrap();
        let cap = st.capacity("ci");
        assert_eq!(cap.ready_for(Executor::Docker), 2);
        assert_eq!((cap.ready_gpu, cap.ready_gpu_graphics), (0, 0));
        // Other accounts are not held to it.
        assert_eq!(st.capacity("owner").ready_gpu, 2);
    }
}
