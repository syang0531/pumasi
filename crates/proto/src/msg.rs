//! Messages exchanged over the agent and admin protocols.
//!
//! Agent protocol (`pumasi/agent/0`): the agent dials the control plane,
//! opens one bidirectional stream, sends [`AgentHello`] and receives
//! [`HelloReply`]. After that both sides stream [`ToAgent`] / [`FromAgent`]
//! messages until the connection drops.
//!
//! Admin protocol (`pumasi/admin/0`): one [`AdminRequest`] and one
//! [`AdminResponse`] per bidirectional stream.
//!
//! File store (iroh-blobs' ALPN, see [`crate::BLOBS_ALPN`]): agents fetch
//! mounted datasets and push stored outputs on their own connection; the
//! CLI pushes uploads and fetches downloads after an admin call allowed
//! its endpoint.

use serde::{Deserialize, Serialize};

use iroh::EndpointAddr;

use crate::{
    ModuleHash, Secret,
    economy::{MilliCu, Policy},
    gpu::{GpuChallenge, GpuInfo, GpuRequest, SignedDevices},
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput},
    manage::{
        AccessRole, AccessView, AccountQuota, ApplySpec, LabelSet, NodeConfig, NodeEdit, QuotaView,
        ScheduleSpec, ScheduleView, Selector, Toleration,
    },
    monitor::{AlertPolicy, LogChunk, NodeMetrics, WebhookSpec},
    perf::{BenchChallenge, PerfProfile, SignedPerf},
    receipt::{Receipt, SignedUsage},
    release::{ReleaseFile, SignedRelease, Target},
    store::{
        BlobHash, DatasetView, ImageBlob, Mount, OutputMode, StoragePolicy, StoreItem, StoreTarget,
        StoreUsageView, StoredOutput,
    },
};

/// What a member machine is to the cluster.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// One of the cluster owner's own machines. Trusted: runs every
    /// executor it offers, and its results settle checks.
    #[default]
    Owner,
    /// Someone else's machine, admitted with an invite. Runs WASM and
    /// microVM tasks (Docker only if the invite allowed it), earns CU, and
    /// has a sample of its results re-run elsewhere.
    Provider,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Owner => "owner",
            Role::Provider => "provider",
        })
    }
}

/// The account of the cluster owner: owns every `Role::Owner` machine and
/// is the default consumer for submitted jobs.
pub const OWNER_ACCOUNT: &str = "owner";

// ---------------------------------------------------------------------------
// Agent protocol
// ---------------------------------------------------------------------------

/// Static facts about a node, reported when it connects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub os: String,
    pub arch: String,
    pub cpus: u32,
    pub memory_bytes: u64,
    /// How many tasks the node runs at once.
    pub slots: u32,
    pub agent_version: String,
    /// Executors this node accepts. Always includes `Wasm`.
    pub executors: Vec<Executor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHello {
    pub protocol_version: u32,
    /// Required the first time a node joins; omitted when reconnecting.
    pub join_secret: Option<Secret>,
    pub node: NodeInfo,
    /// Set when the agent can replace its own binary.
    pub upgrade: Option<UpgradeHello>,
    /// Whether the node takes new tasks right now (its availability
    /// policy). Protocol 6; protocol 5 hellos end before it, see
    /// [`AgentHello::decode`].
    pub availability: Availability,
}

/// The protocol 5 hello: [`AgentHello`] without `availability`.
#[derive(Deserialize)]
struct AgentHelloV5 {
    protocol_version: u32,
    join_secret: Option<Secret>,
    node: NodeInfo,
    upgrade: Option<UpgradeHello>,
}

impl AgentHello {
    /// Decodes a hello of this protocol or of protocol 5 (no availability,
    /// so always available), which lets older agents connect and be
    /// upgraded by a rollout.
    pub fn decode(bytes: &[u8]) -> postcard::Result<Self> {
        postcard::from_bytes::<Self>(bytes).or_else(|e| {
            let old: AgentHelloV5 = match postcard::from_bytes(bytes) {
                Ok(
                    old @ AgentHelloV5 {
                        protocol_version: ..=5,
                        ..
                    },
                ) => old,
                _ => return Err(e),
            };
            Ok(Self {
                protocol_version: old.protocol_version,
                join_secret: old.join_secret,
                node: old.node,
                upgrade: old.upgrade,
                availability: Availability::Available,
            })
        })
    }
}

/// Whether a node takes new tasks, as its owner's availability policy says
/// (`pumasi agent start --only-on-ac`, `--idle-minutes`, `--hours`). Tasks
/// already running when a node pauses finish.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Availability {
    #[default]
    Available,
    /// Takes no new tasks, for this reason (e.g. "on battery power").
    Paused { reason: String },
}

impl Availability {
    pub fn is_available(&self) -> bool {
        matches!(self, Availability::Available)
    }

    pub fn paused_reason(&self) -> Option<&str> {
        match self {
            Availability::Available => None,
            Availability::Paused { reason } => Some(reason),
        }
    }
}

/// What an agent tells the cluster about upgrading itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeHello {
    /// SHA-256 of the binary the agent runs.
    pub exe_sha256: [u8; 32],
    /// `Some(true)`: the node opted in (`--auto-upgrade`), needed on
    /// provider machines. `Some(false)`: opted out. `None`: the default,
    /// on for the owner's machines and off for providers.
    pub auto: Option<bool>,
}

impl UpgradeHello {
    pub fn accepts(&self, role: Role) -> bool {
        self.auto.unwrap_or(role == Role::Owner)
    }
}

/// A chunk request for a release binary, sent on its own stream of the
/// agent connection. The reply is one [`FetchReply`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub sha256: [u8; 32],
    pub offset: u64,
    pub len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FetchReply {
    Data(Vec<u8>),
    Error(String),
}

/// Largest chunk a [`FetchRequest`] or an upload may carry.
pub const RELEASE_CHUNK: u32 = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HelloReply {
    Welcome {
        cluster_name: String,
        /// How often the agent must send a heartbeat.
        heartbeat_ms: u64,
        role: Role,
        /// Account this node earns for.
        account: String,
    },
    Rejected {
        reason: String,
    },
}

/// One placement of one task. `attempt` distinguishes retries so stale
/// reports from a node that was presumed dead can be ignored. A check run
/// (a replica that verifies another node's result) has its own attempt
/// counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskRef {
    pub job: JobId,
    pub index: u32,
    pub attempt: u32,
    pub check: bool,
}

impl std::fmt::Display for TaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}#{}", self.job, self.index, self.attempt)?;
        if self.check {
            f.write_str("(check)")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: TaskRef,
    /// argv[0] for the module.
    pub program: String,
    pub executor: Executor,
    /// WASM module hash (wasm executor).
    pub module_hash: Option<ModuleHash>,
    /// Sent only the first time this connection sees the module.
    pub module: Option<Vec<u8>>,
    /// Docker settings (docker executor).
    pub docker: Option<DockerSpec>,
    /// Hash of the job's shared input files, if it has any.
    pub files_hash: Option<ModuleHash>,
    /// The shared files, sent only the first time this connection sees them.
    pub files: Option<Vec<FileEntry>>,
    pub input: TaskInput,
    pub limits: Limits,
    /// Run in deterministic mode with this seed: a virtual clock and seeded
    /// random numbers, so every honest node produces the same result (WASM
    /// only).
    pub seed: Option<[u8; 32]>,
    // Protocol 8 from here. Older agents decode the fields above and ignore
    // the rest (postcard leaves trailing bytes alone); jobs that need them
    // are never sent to older agents.
    /// Datasets placed under `/input`, fetched from the store.
    pub mounts: Vec<Mount>,
    /// Where `/output` goes.
    pub output: OutputMode,
    /// A private image the control plane pulled into the store; the node
    /// loads it from there instead of the registry.
    pub image: Option<ImageBlob>,
    /// Other nodes that have the mounted collections (and the image)
    /// cached; tried before the control plane.
    pub peers: Vec<EndpointAddr>,
    // Protocol 9 from here; only protocol 9 agents get GPU jobs.
    /// The node's GPUs (by [`GpuInfo::index`]) this task may use.
    pub gpus: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToAgent {
    Assign(Box<TaskAssignment>),
    Cancel(TaskRef),
    /// Replace the agent binary with this release, then restart. Sent only
    /// to idle nodes that accept upgrades.
    Upgrade(Box<SignedRelease>),
    /// Run the CPU benchmark kernel with this seed now and answer with
    /// [`FromAgent::BenchResult`]; the control plane times it (protocol 7).
    Benchmark(BenchChallenge),
    /// Measure this machine again and send a new profile (protocol 7).
    Rebenchmark,
    /// Solve this GPU challenge now and answer with
    /// [`FromAgent::GpuResult`]; the control plane times it (protocol 9).
    GpuChallenge(GpuChallenge),
}

/// How a task ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// The program exited; 0 means success.
    Exited(i32),
    /// Fuel limit reached.
    OutOfFuel,
    /// Wall-clock limit reached.
    Timeout,
    /// The module trapped (crash, memory limit, ...).
    Trap(String),
    /// The agent could not run the task (bad module, missing export, ...).
    Error(String),
    /// Cancelled on request.
    Cancelled,
}

impl Outcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Outcome::Exited(0))
    }
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Exited(c) => write!(f, "exit {c}"),
            Outcome::OutOfFuel => write!(f, "out of fuel"),
            Outcome::Timeout => write!(f, "timeout"),
            Outcome::Trap(m) => write!(f, "trap: {m}"),
            Outcome::Error(m) => write!(f, "error: {m}"),
            Outcome::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Result of one task placement, including metered usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReport {
    pub task: TaskRef,
    pub outcome: Outcome,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// Files the task wrote to `/output`.
    pub files: Vec<FileEntry>,
    /// Fuel consumed: a hardware-independent measure of work done (WASM).
    pub fuel_used: u64,
    pub wall_ms: u64,
    /// The node's signed usage. Absent only when the agent could not start
    /// the task at all (`Outcome::Error`).
    pub usage: Option<SignedUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FromAgent {
    Heartbeat {
        running: Vec<TaskRef>,
    },
    Done(Box<TaskReport>),
    /// The agent refused or could not install an offered release. It
    /// keeps running its current binary.
    UpgradeFailed {
        sha256: [u8; 32],
        reason: String,
    },
    /// The node's availability changed (protocol 6).
    Availability(Availability),
    /// The node's signed performance profile, sent after the welcome and
    /// whenever it measured again (protocol 7).
    Perf(Box<SignedPerf>),
    /// The answer to a [`ToAgent::Benchmark`] challenge (protocol 7).
    BenchResult {
        nonce: u64,
        result: u64,
    },
    /// A task report whose output files may be in the store: the agent
    /// pushed the collection before sending this (protocol 8).
    Report {
        report: Box<TaskReport>,
        stored: Option<StoredOutput>,
    },
    /// What the node's store cache holds, so the control plane can point
    /// other nodes at it, and the address they dial (protocol 8). `full`
    /// replaces the previous list; otherwise `added` and `removed` update
    /// it.
    StoreState {
        addr: Option<EndpointAddr>,
        full: bool,
        added: Vec<BlobHash>,
        removed: Vec<BlobHash>,
    },
    /// The node's signed device profile (GPUs, microVM networking), sent
    /// after the welcome and whenever it changes (protocol 9).
    Devices(Box<SignedDevices>),
    /// The answer to a [`ToAgent::GpuChallenge`]: the solutions found, or
    /// none if the GPU could not run it (protocol 9).
    GpuResult {
        nonce: u64,
        solutions: Vec<u64>,
    },
    /// A sample of the machine's CPU, memory, disk, network, temperature
    /// and GPUs, every few seconds (protocol 10).
    Metrics(Box<NodeMetrics>),
    /// What a running task printed since the last chunk (protocol 10).
    Logs {
        task: TaskRef,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
}

// ---------------------------------------------------------------------------
// Admin protocol
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRequest {
    pub secret: Secret,
    pub op: AdminOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminOp {
    /// A ticket for one of the owner's own machines, or with `invite`, for
    /// someone else's machine joining as a provider.
    CreateJoinToken {
        ttl_secs: u64,
        uses: Option<u32>,
        invite: Option<Invite>,
    },
    Submit(Box<JobSubmission>),
    Status,
    Job {
        id: JobId,
        with_output: bool,
    },
    /// Output files of one task.
    TaskFiles {
        id: JobId,
        index: u32,
    },
    Cancel {
        id: JobId,
    },
    RemoveNode {
        id: String,
    },
    /// Countersigned usage receipts of one job.
    Receipts {
        id: JobId,
    },
    /// Account balances.
    Accounts,
    /// Ledger entries, newest first.
    Ledger {
        account: Option<String>,
        limit: u32,
    },
    /// Grants CU to an account (prepaid mode only; bookkeeping, no money
    /// moves).
    Deposit {
        account: String,
        amount: MilliCu,
        memo: String,
    },
    /// Records CU leaving an account's available balance (prepaid mode
    /// only).
    Payout {
        account: String,
        amount: MilliCu,
        memo: String,
    },
    /// Asks a node (id or unique prefix) to measure itself again.
    Benchmark {
        id: String,
    },
    GetPolicy,
    SetPolicy(Policy),
    /// One chunk of a release binary. Chunks are appended in order; the
    /// last one completes the upload, which the control plane checks
    /// against `sha256`.
    UploadRelease {
        sha256: [u8; 32],
        size: u64,
        offset: u64,
        bytes: Vec<u8>,
    },
    /// Replace the current rollout with these uploaded binaries (one per
    /// target). `force` allows binaries of another protocol version.
    Rollout {
        files: Vec<ReleaseFile>,
        force: bool,
    },
    RolloutStatus,
    /// Stop offering the current rollout.
    CancelRollout,
    /// Lets `uploader` (the CLI's endpoint) push the collection `root` to
    /// the store for the next hour; answers with what the store is still
    /// missing of it, so an interrupted upload resumes.
    StoreUpload {
        uploader: [u8; 32],
        root: BlobHash,
    },
    /// Records an uploaded collection: as a new version of dataset `name`,
    /// or, without a name, as a job input the next submission refers to.
    StoreCommit {
        root: BlobHash,
        name: Option<String>,
        account: Option<String>,
    },
    /// Datasets (every version of `name`, or the latest of each).
    StoreList {
        name: Option<String>,
    },
    /// Deletes one version of a dataset, or all of them. The space comes
    /// back once nothing else refers to the files.
    StoreRemove {
        name: String,
        version: Option<u32>,
    },
    /// Lets `reader` (the CLI's endpoint) download a dataset or a job's
    /// stored outputs for the next hour, and says what to fetch.
    StoreOpen {
        reader: [u8; 32],
        target: StoreTarget,
    },
    /// What each account stores, and its quota.
    StoreUsage,
    /// Sets (or with `None` clears) an account's storage quota in bytes.
    StoreQuota {
        account: String,
        bytes: Option<u64>,
    },
    /// Stores a secret on the control plane. `registry/<host>` secrets
    /// (`user:password` or a token) log in to private registries.
    SecretSet {
        name: String,
        value: String,
    },
    /// Secret names (never their values).
    SecretList,
    SecretRemove {
        name: String,
    },
    GetStoragePolicy,
    SetStoragePolicy(StoragePolicy),
    // Phase 10 from here.
    /// Changes a node's labels, taints or cordon (`node`: name or id
    /// prefix).
    EditNode {
        node: String,
        edit: Box<NodeEdit>,
    },
    /// Cordons a node and moves its running tasks to other nodes.
    Drain {
        node: String,
    },
    /// Sets (replaces) an account's quota.
    SetQuota {
        account: String,
        quota: AccountQuota,
    },
    /// Every account's quota and usage.
    Quotas,
    /// Makes an access ticket with this role.
    CreateAccess {
        name: String,
        role: AccessRole,
        ttl_secs: Option<u64>,
    },
    ListAccess,
    /// Revokes access tickets by id (or unique prefix) or name.
    RevokeAccess {
        id: String,
    },
    /// The role of the ticket making the call.
    WhoAmI,
    /// Creates or replaces a schedule.
    SetSchedule(Box<ScheduleSpec>),
    Schedules,
    RemoveSchedule {
        name: String,
    },
    /// Submits a schedule's job now.
    RunSchedule {
        name: String,
    },
    /// Makes the cluster match a `cluster.toml`.
    Apply(Box<ApplySpec>),
    // Phase 11 from here.
    /// Nodes' latest metrics and, with `since_secs`, their time series
    /// over that long (`node`: name or id prefix, or every node).
    Metrics {
        node: Option<String>,
        since_secs: u64,
    },
    /// Cluster events after sequence number `after` (and not older than
    /// `since_unix`), oldest first; with `after` 0, the newest `limit`.
    Events {
        after: u64,
        since_unix: u64,
        limit: u32,
    },
    /// A task's output from these offsets of run `attempt`.
    Logs {
        job: JobId,
        index: u32,
        attempt: u32,
        stdout_from: u64,
        stderr_from: u64,
    },
    /// Alert rules, firing alerts and webhooks.
    Alerts,
    SetAlertPolicy(Box<AlertPolicy>),
    /// Creates or replaces a webhook.
    SetWebhook(Box<WebhookSpec>),
    RemoveWebhook {
        name: String,
    },
    /// Sends a test alert to a webhook now and waits for the answer.
    TestWebhook {
        name: String,
    },
}

/// Terms of an invite for someone else's machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invite {
    /// Account the machine earns for, usually its owner's name.
    pub account: String,
    /// Let this machine run Docker jobs too (it must also opt in with
    /// `--allow-docker`). Off by default: containers share the provider's
    /// kernel, and the provider can see and alter everything in them.
    pub allow_docker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    JoinToken {
        ticket: String,
        expires_unix: u64,
    },
    Submitted {
        id: JobId,
    },
    Status(ClusterStatus),
    Job(Box<JobView>),
    Files(Vec<FileEntry>),
    Receipts(Vec<Receipt>),
    Accounts(Vec<AccountView>),
    Ledger(Vec<LedgerEntry>),
    Policy(Policy),
    /// Upload progress: bytes stored so far.
    Uploaded {
        have: u64,
    },
    Rollout(RolloutView),
    Ok,
    Error(String),
    /// What the store lacks of an upload (an iroh-blobs request for it).
    StoreMissing {
        missing: iroh_blobs::protocol::ChunkRangesSeq,
        have: bool,
    },
    Datasets(Vec<DatasetView>),
    /// The collections to fetch for a download, and the account paying
    /// for the egress.
    StoreItems(Vec<StoreItem>),
    StoreUsage {
        usage: Vec<StoreUsageView>,
        /// Bytes on the control plane's disk, after sharing between
        /// versions.
        disk_bytes: u64,
    },
    Secrets(Vec<SecretView>),
    StoragePolicy {
        policy: StoragePolicy,
        /// The control plane's disk class, which picks the rate.
        class: crate::perf::DiskClass,
    },
    // Phase 10 from here.
    Quotas(Vec<QuotaView>),
    /// A new access ticket (the only time its secret is shown).
    AccessTicket {
        ticket: String,
        view: AccessView,
    },
    AccessList(Vec<AccessView>),
    WhoAmI {
        name: String,
        role: AccessRole,
    },
    Schedules(Vec<ScheduleView>),
    /// What `apply` changed (or would change), one line each.
    Applied(Vec<String>),
    /// Runs a drain moved off the node.
    Drained {
        moved: u32,
    },
    // Phase 11 from here.
    Metrics(Box<crate::monitor::MetricsView>),
    Events(Vec<crate::monitor::EventView>),
    Logs(Box<LogChunk>),
    Alerts(Box<crate::monitor::AlertsView>),
}

/// A secret's name and when it was set; the value never leaves the
/// control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretView {
    pub name: String,
    pub set_unix: u64,
}

/// The current rollout and where each node stands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutView {
    pub releases: Vec<SignedRelease>,
    pub started_unix: u64,
    pub nodes: Vec<NodeUpgradeView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeUpgradeView {
    pub id: String,
    pub name: String,
    pub target: Target,
    pub version: String,
    pub exe_sha256: Option<[u8; 32]>,
    pub state: UpgradeState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpgradeState {
    /// No rollout, or the node runs the release binary.
    Current,
    /// Waits for its running tasks to finish (gets no new tasks).
    Draining,
    /// Waits its turn: another node is draining.
    Waiting,
    /// Got the release and is installing it.
    Upgrading,
    /// Refused or failed to install the release; keeps its binary.
    Failed(String),
    /// The rollout has no binary for this node's platform.
    NoRelease,
    /// A provider machine that did not pass `--auto-upgrade`, or a node
    /// that passed `--no-auto-upgrade`.
    OptedOut,
    /// The agent cannot replace its own binary.
    Unsupported,
    Offline,
}

impl std::fmt::Display for UpgradeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpgradeState::Current => f.write_str("current"),
            UpgradeState::Draining => f.write_str("draining"),
            UpgradeState::Waiting => f.write_str("waiting"),
            UpgradeState::Upgrading => f.write_str("upgrading"),
            UpgradeState::Failed(r) => write!(f, "failed: {r}"),
            UpgradeState::NoRelease => f.write_str("no binary for platform"),
            UpgradeState::OptedOut => f.write_str("opted out"),
            UpgradeState::Unsupported => f.write_str("cannot self-upgrade"),
            UpgradeState::Offline => f.write_str("offline"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountView {
    pub name: String,
    /// Spendable milli-CU. Negative when charges overran the prepaid
    /// amount, or in mutual mode when the account used more than it lent.
    pub balance: MilliCu,
    /// Earnings waiting out the hold period.
    pub held: MilliCu,
    /// Totals since the account was created.
    pub spent: MilliCu,
    pub earned: MilliCu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerKind {
    /// CU granted by the owner (prepaid mode).
    Deposit,
    /// CU taken out of the cluster's books (prepaid mode).
    Payout,
    /// A consumer paying for a run.
    Charge,
    /// A provider's earning entering the hold.
    Hold,
    /// A held earning becoming spendable.
    Release,
    /// A held earning cancelled because the run failed a check.
    Forfeit,
    /// A consumer getting a forfeited charge back.
    Refund,
    /// The opening balance after converting the phase 2 credit ledger to
    /// milli-CU.
    Convert,
    /// Storage in the cluster file store: the account paying (negative)
    /// and the control plane's owner receiving (positive).
    Storage,
    /// Data downloaded out of the store (`pumasi store get`), same sides.
    Egress,
}

impl std::fmt::Display for LedgerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LedgerKind::Deposit => "deposit",
            LedgerKind::Payout => "payout",
            LedgerKind::Charge => "charge",
            LedgerKind::Hold => "hold",
            LedgerKind::Release => "release",
            LedgerKind::Forfeit => "forfeit",
            LedgerKind::Refund => "refund",
            LedgerKind::Convert => "convert",
            LedgerKind::Storage => "storage",
            LedgerKind::Egress => "egress",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub seq: u64,
    pub unix: u64,
    pub account: String,
    pub kind: LedgerKind,
    /// Change to the account's spendable balance in milli-CU (`Hold` and
    /// `Forfeit` change the held amount instead).
    pub amount: MilliCu,
    /// The run this entry is about, if any.
    pub task: Option<TaskRef>,
    pub memo: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        )
    }
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Pending,
    Running {
        node: String,
    },
    Succeeded,
    Failed(Outcome),
    Cancelled,
    /// Finished on a provider; a check run on another node is pending.
    Verifying,
}

/// Whether a task's result was checked by running it again elsewhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verification {
    /// Ran on a trusted node, or was not picked for a check.
    NotChecked,
    /// Picked; the check run has not finished yet.
    Pending,
    /// The check run agreed.
    Passed,
    /// A run disagreed with the accepted result; `bad` lists the nodes
    /// whose runs were rejected (their earnings are forfeited).
    Mismatch { bad: Vec<String> },
    /// Runs disagreed and no majority or trusted run settled it.
    Inconclusive,
    /// The check was dropped (job cancelled, no node to run it).
    Skipped(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeView {
    pub id: String,
    pub info: NodeInfo,
    pub role: Role,
    pub account: String,
    /// Executors the cluster will actually send this node.
    pub executors: Vec<Executor>,
    /// Checks this node's runs passed and failed.
    pub checks_passed: u32,
    pub strikes: u32,
    /// Too many failed checks: gets no tasks.
    pub suspended: bool,
    pub online: bool,
    pub running: u32,
    pub completed: u64,
    pub last_seen_unix: u64,
    /// Online but taking no new tasks, and why (its availability policy).
    pub paused: Option<String>,
    /// The node's latest signed performance profile.
    pub perf: Option<PerfProfile>,
    /// CPU score its runs are billed at (1000 = the reference vCPU).
    pub cpu_score: u32,
    /// Why the billed score differs from the profile's, or what the
    /// cluster's own timing found.
    pub perf_note: Option<String>,
    /// Round-trip time to the control plane, as the control plane measures
    /// it on the connection, and whether it goes through a relay.
    pub rtt_us: Option<u32>,
    pub relayed: bool,
    /// The node's GPUs (protocol 9 agents), and which are busy.
    pub gpus: Vec<GpuView>,
    /// MicroVMs on this node can have a network.
    pub microvm_network: bool,
    // Phase 10 from here.
    /// Labels, taints and cordon an admin set.
    pub config: NodeConfig,
    /// Every label: the built-in ones and the admin's.
    pub labels: LabelSet,
}

/// A GPU in the cluster status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuView {
    pub info: GpuInfo,
    /// FP32 GFLOPS its runs are billed at.
    pub billed_gflops: u32,
    /// Why the billed figure differs from the profile's.
    pub note: Option<String>,
    /// The task using it right now.
    pub busy: Option<TaskRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: JobId,
    pub name: String,
    pub state: JobState,
    pub tasks: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub running: u32,
    pub submitted_unix: u64,
    pub account: String,
    /// Milli-CU charged so far.
    pub cost: MilliCu,
    // Phase 10 from here.
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub cluster_name: String,
    pub cluster_id: String,
    pub nodes: Vec<NodeView>,
    pub jobs: Vec<JobSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskView {
    pub index: u32,
    pub state: TaskState,
    pub attempts: u32,
    pub node: Option<String>,
    pub fuel_used: u64,
    pub wall_ms: u64,
    /// Present only when requested.
    pub stdout: Option<Vec<u8>>,
    pub stderr: Option<Vec<u8>>,
    /// Names and sizes of the files the task wrote to `/output`.
    pub output_files: Vec<(String, u64)>,
    pub verification: Verification,
    /// Milli-CU charged for this task's runs (including check runs).
    pub cost: MilliCu,
    /// Output files kept in the store instead of inline.
    pub stored: Option<StoredOutput>,
    // Phase 10 from here.
    /// Times a run was stopped to make room (preemption, drain); these do
    /// not count against the job's attempts.
    pub preempted: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobView {
    pub summary: JobSummary,
    pub executor: Executor,
    pub image: Option<String>,
    pub limits: Limits,
    pub tasks: Vec<TaskView>,
    pub total_fuel: u64,
    pub deterministic: bool,
    /// Datasets the job mounts (`corpus@3`).
    pub mounts: Vec<String>,
    /// The dataset version holding the job's stored outputs, once made.
    pub output_dataset: Option<String>,
    /// What the job waits for before its tasks can run (pulling a private
    /// image into the store), or why it failed.
    pub preparing: Option<String>,
    /// The GPUs each task needs.
    pub gpu: Option<GpuRequest>,
    /// Image jobs: the tasks may reach the internet.
    pub network: bool,
    // Phase 10 from here.
    pub selector: Selector,
    pub tolerations: Vec<Toleration>,
    pub after: Vec<JobId>,
    /// Why its tasks are not in the queue yet (waiting for other jobs).
    pub waiting: Option<String>,
    /// The schedule that submitted it.
    pub schedule: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Clippy (`result_large_err`) refuses results whose error is an
    /// `AdminOp` of 128 bytes or more: box large variant fields instead.
    #[test]
    fn admin_op_stays_small() {
        assert!(
            std::mem::size_of::<AdminOp>() < 128,
            "AdminOp is {} bytes",
            std::mem::size_of::<AdminOp>()
        );
    }

    fn node() -> NodeInfo {
        NodeInfo {
            name: "n".into(),
            os: "macos".into(),
            arch: "x86_64".into(),
            cpus: 4,
            memory_bytes: 8 << 30,
            slots: 2,
            agent_version: "0.5.0".into(),
            executors: vec![Executor::Wasm],
        }
    }

    #[test]
    fn hello_decodes_this_and_the_previous_protocol() {
        #[derive(Serialize)]
        struct V5 {
            protocol_version: u32,
            join_secret: Option<Secret>,
            node: NodeInfo,
            upgrade: Option<UpgradeHello>,
        }
        let old = postcard::to_stdvec(&V5 {
            protocol_version: 5,
            join_secret: None,
            node: node(),
            upgrade: Some(UpgradeHello {
                exe_sha256: [7; 32],
                auto: None,
            }),
        })
        .unwrap();
        let h = AgentHello::decode(&old).unwrap();
        assert_eq!(h.protocol_version, 5);
        assert_eq!(h.node, node());
        assert_eq!(h.availability, Availability::Available);
        assert_eq!(h.upgrade.unwrap().exe_sha256, [7; 32]);

        let paused = Availability::Paused {
            reason: "on battery power".into(),
        };
        let new = postcard::to_stdvec(&AgentHello {
            protocol_version: crate::PROTOCOL_VERSION,
            join_secret: None,
            node: node(),
            upgrade: None,
            availability: paused.clone(),
        })
        .unwrap();
        let h = AgentHello::decode(&new).unwrap();
        assert_eq!(h.availability, paused);
        assert!(h.upgrade.is_none());
        // A protocol 5 control plane reads the new hello's version (and
        // rejects it with a message) instead of failing to decode it.
        let seen: V5Read = postcard::from_bytes(&new).unwrap();
        assert_eq!(seen.protocol_version, crate::PROTOCOL_VERSION);
        assert!(AgentHello::decode(&new[..new.len() - 3]).is_err());
    }

    #[derive(Deserialize)]
    struct V5Read {
        protocol_version: u32,
        #[allow(dead_code)]
        node_and_rest: (Option<Secret>, NodeInfo, Option<UpgradeHello>),
    }
}
