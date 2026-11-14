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
        AccessRole, AccessView, AccountPolicy, AccountQuota, ApplySpec, LabelSet, NodeConfig,
        NodeEdit, QuotaView, ScheduleSpec, ScheduleView, Selector, Toleration,
    },
    monitor::{AlertPolicy, LogChunk, NodeMetrics, WebhookSpec},
    perf::{BenchChallenge, PerfProfile, SignedPerf},
    receipt::{Receipt, SignedUsage},
    release::{ReleaseFile, SignedRelease, Target},
    store::{
        BlobHash, DatasetView, ImageBlob, Mount, OutputMode, StoragePolicy, StoreItem, StoreTarget,
        StoreUsageView, StoredOutput,
    },
    webgpu::{SignedWebGpu, WebGpuAdapter},
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

/// What follows an [`AgentHello`] in its frame (1.5). A control plane
/// that does not know it ignores it. Fields are only ever added at the
/// end.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloTail {
    /// The agent's [`crate::PROTOCOL_MINOR`].
    pub minor: u32,
}

/// What follows a [`HelloReply::Welcome`] in its frame (1.5). An agent
/// that does not know it ignores it. Fields are only ever added at the
/// end.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WelcomeTail {
    /// The control plane's [`crate::PROTOCOL_MINOR`].
    pub minor: u32,
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
    /// This control plane replica is not the leader (sent to protocol 11
    /// agents only; older ones get a closed connection). `leader` is where
    /// to go, when this replica knows; `replicas` are all of them.
    NotLeader {
        leader: Option<EndpointAddr>,
        replicas: Vec<EndpointAddr>,
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
    // Protocol 13 from here; only protocol 13 agents get jobs with
    // capabilities.
    /// NVIDIA driver capabilities beyond compute and utility.
    pub gpu_capabilities: Vec<crate::job::GpuCapability>,
    // Protocol 14 from here; only protocol 14 agents get confidential
    // tasks.
    /// A confidential task: the job's image, command, environment and
    /// inputs above are left out, and come sealed instead.
    pub confidential: Option<crate::confidential::ConfidentialTask>,
    // Minor 5 (1.9) from here; only agents of minor 5 get checkpointing
    // tasks.
    /// The task keeps checkpoints: how to stop it, and what to put back in
    /// its checkpoint directory first.
    pub checkpoint: Option<CheckpointTask>,
}

/// How a checkpointing task runs (minor 5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTask {
    /// From the stop signal to the kill when [`ToAgent::Stop`] comes.
    pub grace_ms: u64,
    /// The last checkpoint, fetched from the file store into
    /// [`crate::sched::CHECKPOINT_DIR`] before the run starts.
    pub restore: Option<Mount>,
}

/// Which executors a node can run right now, and why the others it was
/// started with cannot (protocol 13). Sent after the welcome and whenever
/// it changes: Docker Desktop stopped or started, the daemon gone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorReport {
    /// Executors it runs now. Always includes `Wasm`.
    pub executors: Vec<Executor>,
    /// Executors its owner allowed that cannot run now, and why
    /// ("Docker Desktop is not running").
    pub unavailable: Vec<(Executor, String)>,
    /// Docker GPU tasks here can use the GPU for graphics (OpenGL, EGL,
    /// Vulkan), as `pumasi gpu check --docker-run` found or the owner said.
    pub gpu_graphics: bool,
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
    /// The control plane replicas, after the welcome and whenever they
    /// change; the agent tries them when the connection drops (protocol
    /// 11).
    Replicas(Vec<EndpointAddr>),
    /// The answer to [`FromAgent::Attest`]: the task's keys sealed to the
    /// guest, or why the control plane refused (protocol 14).
    Keys {
        task: TaskRef,
        reply: Result<Vec<u8>, String>,
    },
    // Minor 1 (1.5) from here: only for agents whose hello said they know
    // them ([`HelloTail`]).
    /// Send the Wake-on-LAN magic packet of each of these network cards
    /// on this machine's LAN: another node of the cluster sleeps there.
    Wake {
        macs: Vec<crate::power::Mac>,
    },
    // Minor 2 (1.6) from here.
    /// Measure the network to another node now ([`crate::PROBE_ALPN`]):
    /// round trips, then `bytes` sent. Answered with
    /// [`FromAgent::Probe`].
    Probe {
        /// The node's id, which the answer names.
        node: String,
        addr: EndpointAddr,
        bytes: u32,
    },
    // Minor 3 (1.7) from here.
    /// Change the agent's availability settings: write them to its
    /// service's options file and restart with them once its running
    /// tasks end. Answered with [`FromAgent::Settings`].
    Settings(Box<crate::ui::NodeSettings>),
    // Minor 4 (1.8) from here.
    /// The dataset collections this node keeps as replicas: every one of
    /// them (the list replaces the last one). It fetches what it lacks,
    /// from `sources` (other nodes holding some of them) and then the
    /// control plane, keeps them out of its cache's eviction, drops
    /// replicas no longer listed, and answers with [`FromAgent::Held`].
    Hold {
        roots: Vec<BlobHash>,
        sources: Vec<EndpointAddr>,
    },
    /// Check every replica against its hashes now; answered with
    /// [`FromAgent::Held`] carrying the scrub report.
    Scrub,
    // Minor 5 (1.9) from here.
    /// Stop a checkpointing run so it can move: the stop signal, at most
    /// `grace_ms` for it to end, then the kill. The agent pushes the run's
    /// checkpoint directory to the file store and answers with
    /// [`FromAgent::Stopped`] instead of a report.
    Stop {
        task: TaskRef,
        grace_ms: u64,
    },
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

/// How long the parts of one run took on the node, which only the node
/// can measure (1.4). It follows a [`FromAgent::Report`] in the same frame
/// ([`ReportTail`]), so the protocol version stays what it was: an older
/// control plane ignores the bytes after the message, and an older agent
/// sends none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTiming {
    /// Fetching what the task mounts (datasets, large inputs, a private
    /// image) from the file store before the run. Not part of `wall_ms`.
    pub fetch_ms: u64,
    /// Getting the image ready on the node: pulling it from the registry
    /// (for a microVM also building its disk) or loading a private image.
    /// 0 when the node already had it. Not part of `wall_ms`.
    pub pull_ms: u64,
    /// Pushing the output files to the file store after the run. 0 when
    /// they went in the report. Not part of `wall_ms`.
    pub upload_ms: u64,
}

/// What follows a [`FromAgent::Report`] in its frame (1.4). Fields are
/// only ever added at the end.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportTail {
    pub timing: TaskTiming,
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
    /// The node's signed WebGPU adapters, sent after the welcome (protocol
    /// 12). Empty when the node offers none.
    WebGpu(Box<SignedWebGpu>),
    /// The executors it can run now (protocol 13).
    Executors(Box<ExecutorReport>),
    /// The image a task runs, by digest, sent before its report (protocol
    /// 13): `repo@sha256:...` from the registry, or the image id when it
    /// has no registry digest.
    Image {
        task: TaskRef,
        image: String,
        digest: String,
    },
    /// The TEEs the node starts confidential VMs in, sent after the
    /// welcome (protocol 14).
    Tee(Box<crate::confidential::TeeReport>),
    /// A confidential task's guest asks for its keys with this evidence
    /// (protocol 14). Answered with [`ToAgent::Keys`].
    Attest {
        task: TaskRef,
        evidence: Box<crate::confidential::Evidence>,
    },
    // Minor 1 (1.5) from here: only for a control plane whose welcome said
    // it knows them ([`WelcomeTail`]).
    /// The machine is about to sleep: its runs go back to the queue now,
    /// and the node shows as asleep. The agent ends the session after it.
    Sleeping,
    /// How the machine sleeps and wakes, sent after the welcome.
    Power(Box<crate::power::PowerReport>),
    // Minor 2 (1.6) from here.
    /// What a [`ToAgent::Probe`] measured.
    Probe(Box<crate::monitor::ProbeResult>),
    // Minor 3 (1.7) from here.
    /// The agent's availability settings: after the welcome, and in
    /// answer to [`ToAgent::Settings`] (`error` says why a change was
    /// not made).
    Settings {
        report: Box<crate::ui::SettingsReport>,
        error: Option<String>,
    },
    // Minor 4 (1.8) from here.
    /// The replicas this node has complete, in answer to
    /// [`ToAgent::Hold`] and whenever a download finishes; `failed` says
    /// why some could not be fetched, and `scrub` is set after a
    /// [`ToAgent::Scrub`].
    Held {
        roots: Vec<BlobHash>,
        failed: Vec<(BlobHash, String)>,
        scrub: Option<crate::replica::ScrubReport>,
    },
    // Minor 5 (1.9) from here.
    /// A run stopped after [`ToAgent::Stop`] (or because the machine is
    /// about to sleep): its checkpoint, pushed to the file store before
    /// this, when it left one; `error` says why there is none.
    Stopped {
        task: TaskRef,
        checkpoint: Option<StoredOutput>,
        error: Option<String>,
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
    // Phase 13 from here.
    /// The control plane replicas and the Raft state.
    HaStatus,
    /// A replica ticket for one more control plane replica, valid for
    /// `ttl_secs`. Turns replication on if it is off.
    HaInvite {
        ttl_secs: u64,
    },
    /// Takes a replica out of the control plane (id or prefix).
    HaRemove {
        id: String,
    },
    /// A consistent copy of the control plane's state, from `offset`
    /// (made when `offset` is 0, kept for a while for the next chunks).
    Backup {
        offset: u64,
    },
    // Phase 15 from here.
    /// Sets what an account's jobs may be (its submitter tickets).
    SetAccountPolicy {
        account: String,
        policy: Box<AccountPolicy>,
    },
    // Phase 16 from here.
    /// TEE nodes, trusted hosts and the launch measurements seen.
    Confidential,
    /// Trusts (or stops trusting) a launch measurement, by id or a unique
    /// prefix of at least 8 characters.
    TrustMeasurement {
        id: String,
        trusted: bool,
        note: String,
    },
    /// Lets a provider's node run `tier = "trusted"` jobs, or stops it.
    TrustNode {
        node: String,
        trusted: bool,
    },
    // Phase 17 from here.
    /// Audit log entries after `after`, oldest first; with `after` 0, the
    /// newest `limit`.
    Audit {
        after: u64,
        limit: u32,
    },
    /// Checks the whole audit chain and answers with its head, signed by
    /// the cluster key.
    AuditVerify,
    /// Replaces the cluster's admin secret: every copy of the old admin
    /// ticket stops working. Answers with the new ticket.
    RotateAdminSecret,
    /// The SSO settings (without secrets).
    SsoGet,
    /// Changes the SSO settings.
    SsoSet(Box<crate::sso::SsoUpdate>),
    // The calls below need no ticket: they are how people without one
    // sign in. The control plane checks the identity provider's answer.
    /// Which sign-in methods there are.
    SsoInfo,
    /// Starts a browser sign-in: answers with the provider's URL to send
    /// the browser to, which comes back to `redirect_uri`.
    SsoStart {
        redirect_uri: String,
    },
    /// Finishes a browser sign-in with what came back to `redirect_uri`.
    /// Answers with an access ticket for the session.
    SsoFinish {
        state: String,
        code: String,
    },
    /// Starts a device sign-in (`pumasi login --sso`).
    SsoDeviceStart,
    /// Asks whether the device sign-in is done: an access ticket when it
    /// is, `SsoPending` while the user has not finished.
    SsoDevicePoll {
        handle: String,
    },
    /// Signs in with an LDAP user name and password.
    LdapLogin {
        user: String,
        password: crate::sso::Hidden,
    },
    // 1.2 from here.
    /// The join tickets and invites that can still be used.
    ListJoinTokens,
    /// Revokes a join ticket or invite by id (or unique prefix), or by
    /// the ticket itself.
    RevokeJoinToken {
        id: String,
    },
    // 1.3 from here.
    /// Gives a node a display name (`None`: back to the name its agent
    /// reports). `node` is a name or an id prefix. The name shows in the
    /// status, the dashboard, events and the built-in `name` label, and
    /// survives the agent restarting with another `--name`.
    RenameNode {
        node: String,
        name: Option<String>,
    },
    /// The newest published pumasi release and the state of any
    /// download for [`AdminOp::UpgradeToLatest`]. With `check`, asks the
    /// release site first (otherwise answers with what it last saw).
    LatestRelease {
        check: bool,
    },
    /// Downloads a published release (`version`, default the newest) for
    /// every platform in the cluster, checks it against the release's
    /// SHA256SUMS and the binaries' build markers, and rolls it out like
    /// `pumasi upgrade rollout`. Runs in the background: answers at once,
    /// then [`AdminOp::LatestRelease`] tells how far it got.
    UpgradeToLatest {
        version: Option<String>,
        force: bool,
    },
    // 1.4 from here.
    /// Where each task of a job ran, by node name, and how long its parts
    /// took (the task views of [`AdminOp::Job`] keep their layout, so
    /// older CLIs still read them).
    TaskDetails {
        id: JobId,
    },
    /// Checks a job as [`AdminOp::Submit`] would (the ticket's role, the
    /// account's policy, priorities, datasets, dependencies) without
    /// creating it, and says which nodes would take it now.
    /// `upload_bytes` is the size of the input files the submission
    /// would put in the file store first (they are not sent for a check).
    CheckJob {
        sub: Box<JobSubmission>,
        upload_bytes: u64,
    },
    // 1.5 from here.
    /// Every node's sleep and wake state (the node views of
    /// [`AdminOp::Status`] keep their layout, so older CLIs still read
    /// them).
    NodePower,
    /// Sends a node's Wake-on-LAN magic packet from a machine on its LAN
    /// (`node`: name or id prefix). Answers with what was sent, one line
    /// each ([`AdminResponse::Applied`]).
    WakeNode {
        node: String,
    },
    /// The executor each task of a job ran with (or runs with), for
    /// `executor = "image"` jobs: `None` until its first run is placed.
    TaskExecutors {
        id: JobId,
    },
    // 1.6 from here.
    /// Repeat and resolved notices, per-node thresholds and silences (the
    /// answer of [`AdminOp::Alerts`] keeps its layout, so older CLIs still
    /// read it).
    AlertTuning,
    /// Replaces the repeat and resolved settings and the per-node
    /// thresholds. Answers like [`AdminOp::AlertTuning`].
    SetAlertTuning(Box<crate::monitor::AlertTuning>),
    /// Keeps matching alerts quiet for a while. Answers like
    /// [`AdminOp::AlertTuning`].
    Silence(Box<crate::monitor::SilenceSpec>),
    /// Ends a silence (id or unique prefix). Answers like
    /// [`AdminOp::AlertTuning`].
    Unsilence {
        id: String,
    },
    /// What the nodes measured of the network between each other.
    Net,
    // 1.7 from here.
    /// Starts a dashboard session for the calling ticket: a new secret
    /// that acts as that ticket until it ends, is revoked, goes unused for
    /// [`crate::ui::SESSION_IDLE_SECS`], or the ticket is revoked.
    /// `label` names the browser for the session list.
    StartSession {
        label: String,
    },
    /// The signed-in sessions: every one for an admin, the caller's own
    /// ticket's otherwise.
    Sessions,
    /// Ends a session (id or unique prefix); answers like
    /// [`AdminOp::Sessions`].
    RevokeSession {
        id: String,
    },
    /// Ends the calling session (signing out).
    EndSession,
    /// A node's availability settings (`node`: name or id prefix).
    NodeSettings {
        node: String,
    },
    /// Changes a node's availability settings: its agent writes them to
    /// `agent.toml` and restarts with them once idle. Answers when the
    /// agent has, like [`AdminOp::NodeSettings`].
    SetNodeSettings {
        node: String,
        settings: Box<crate::ui::NodeSettings>,
    },
    /// [`AdminOp::Rollout`] with canaries: the nodes `canary` selects
    /// upgrade first.
    CanaryRollout {
        files: Vec<ReleaseFile>,
        force: bool,
        canary: Box<crate::release::CanarySpec>,
    },
    /// [`AdminOp::UpgradeToLatest`] with canaries.
    CanaryUpgradeToLatest {
        version: Option<String>,
        force: bool,
        canary: Box<crate::release::CanarySpec>,
    },
    /// The canary of the current rollout, if it has one.
    Canary,
    /// Lets more nodes have the release now: these nodes (names or id
    /// prefixes), or with none every node (ends the canary).
    PromoteCanary {
        nodes: Vec<String>,
    },
    // 1.8 from here.
    /// Where the copies of each dataset version are (one dataset, or all)
    /// and the storage nodes.
    Replication {
        name: Option<String>,
    },
    /// How many copies of a dataset to keep, control planes included; or,
    /// without a name, the default for datasets without their own.
    /// `None` gives a dataset the default again.
    SetReplication {
        name: Option<String>,
        factor: Option<u8>,
    },
    /// Checks every collection on the control plane, or on one node,
    /// against its hashes now.
    Scrub {
        node: Option<String>,
    },
    /// The files of a dataset version (the latest without `version`),
    /// with what the S3 gateway recorded about them.
    StoreFiles {
        name: String,
        version: Option<u32>,
    },
    /// Records an uploaded collection as a new version of `name`, like
    /// [`AdminOp::StoreCommit`], unless the latest version is no longer
    /// `base` (another write came first: the caller builds on the new
    /// one and tries again). `meta` sets (or with `None` drops) what the
    /// S3 gateway keeps about a path. Answers once the version is on as
    /// many copies as it can have.
    S3Commit {
        name: String,
        base: Option<u32>,
        root: BlobHash,
        account: Option<String>,
        meta: Vec<(String, Option<crate::replica::S3Meta>)>,
    },
    /// A new S3 access key acting as the calling ticket.
    S3KeyCreate {
        label: String,
    },
    S3Keys,
    S3KeyRevoke {
        id: String,
    },
    /// Checks a SigV4 signature made with the secret of S3 key `key` over
    /// `string_to_sign`, whose scope is `date` (YYYYMMDD) and `region`.
    /// Answers with a credential that acts as the key's ticket for a few
    /// minutes, and the day's signing key for the request's chunk
    /// signatures.
    /// The day and region come from the string to sign's scope.
    S3Auth {
        key: String,
        string_to_sign: String,
        signature: String,
        /// A presigned URL's lifetime.
        expires: Option<u32>,
    },
    // 1.9 from here. The first four carry what postcard leaves out of a
    // submission, a schedule or an `apply` (see [`crate::sched`]); a
    // client sends them only when there is something to carry
    // ([`AdminOp::submit`] and friends), so a 1.8 control plane takes the
    // rest as before. The control plane turns them back into the plain
    // calls ([`AdminOp::joined`]) before anything else looks at them.
    /// [`AdminOp::Submit`] with a placement.
    SubmitPlaced {
        sub: Box<JobSubmission>,
        placement: Box<crate::sched::Placement>,
    },
    /// [`AdminOp::CheckJob`] with a placement.
    CheckJobPlaced {
        sub: Box<JobSubmission>,
        placement: Box<crate::sched::Placement>,
        upload_bytes: u64,
    },
    /// [`AdminOp::SetSchedule`] with a time zone or a placement.
    SetScheduleExt {
        spec: Box<ScheduleSpec>,
        ext: Box<crate::sched::ScheduleExt>,
    },
    /// [`AdminOp::Apply`] with CPU and memory quotas or schedules' time
    /// zones and placements.
    ApplyExt {
        spec: Box<ApplySpec>,
        ext: Box<crate::sched::ApplyExt>,
    },
    /// Every account's CPU and memory quota and what it holds.
    Resources,
    /// Sets (replaces) an account's CPU and memory quota; answers like
    /// [`AdminOp::Resources`].
    SetResources {
        account: String,
        quota: Box<crate::sched::ResourceQuota>,
    },
    /// A job's placement, its runs being stopped and its tasks'
    /// checkpoints.
    Placement {
        id: JobId,
    },
}

impl AdminOp {
    /// The call that submits `sub`: [`AdminOp::SubmitPlaced`] when it has a
    /// placement, else [`AdminOp::Submit`].
    pub fn submit(mut sub: JobSubmission) -> AdminOp {
        if sub.placement.is_empty() {
            return AdminOp::Submit(Box::new(sub));
        }
        let placement = Box::new(std::mem::take(&mut sub.placement));
        AdminOp::SubmitPlaced {
            sub: Box::new(sub),
            placement,
        }
    }

    /// The call that checks `sub` ([`AdminOp::CheckJob`]).
    pub fn check_job(mut sub: JobSubmission, upload_bytes: u64) -> AdminOp {
        if sub.placement.is_empty() {
            return AdminOp::CheckJob {
                sub: Box::new(sub),
                upload_bytes,
            };
        }
        let placement = Box::new(std::mem::take(&mut sub.placement));
        AdminOp::CheckJobPlaced {
            sub: Box::new(sub),
            placement,
            upload_bytes,
        }
    }

    /// The call that sets a schedule ([`AdminOp::SetSchedule`]).
    pub fn set_schedule(spec: ScheduleSpec) -> AdminOp {
        let ext = spec.ext();
        if ext.is_empty() {
            AdminOp::SetSchedule(Box::new(spec))
        } else {
            AdminOp::SetScheduleExt {
                spec: Box::new(spec),
                ext: Box::new(ext),
            }
        }
    }

    /// The call that applies a `cluster.toml` ([`AdminOp::Apply`]). A plain
    /// `Apply` means no CPU or memory quota for the accounts it lists and
    /// no time zone or placement for its schedules, which is what a file
    /// an older CLI could read says.
    pub fn apply(spec: ApplySpec) -> AdminOp {
        let ext = spec.ext();
        if ext.is_empty() {
            AdminOp::Apply(Box::new(spec))
        } else {
            AdminOp::ApplyExt {
                spec: Box::new(spec),
                ext: Box::new(ext),
            }
        }
    }

    /// The plain call with what the 1.9 calls carry put back in place
    /// (the control plane's side of [`AdminOp::submit`] and friends).
    pub fn joined(self) -> AdminOp {
        match self {
            AdminOp::SubmitPlaced { mut sub, placement } => {
                sub.placement = *placement;
                AdminOp::Submit(sub)
            }
            AdminOp::CheckJobPlaced {
                mut sub,
                placement,
                upload_bytes,
            } => {
                sub.placement = *placement;
                AdminOp::CheckJob { sub, upload_bytes }
            }
            AdminOp::SetScheduleExt { mut spec, ext } => {
                spec.set_ext(*ext);
                AdminOp::SetSchedule(spec)
            }
            AdminOp::ApplyExt { mut spec, ext } => {
                spec.set_ext(*ext);
                AdminOp::Apply(spec)
            }
            op => op,
        }
    }

    /// Calls anyone may make, without a ticket (sign-in).
    pub fn is_sign_in(&self) -> bool {
        matches!(
            self,
            AdminOp::SsoInfo
                | AdminOp::SsoStart { .. }
                | AdminOp::SsoFinish { .. }
                | AdminOp::SsoDeviceStart
                | AdminOp::SsoDevicePoll { .. }
                | AdminOp::LdapLogin { .. }
        )
    }
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
    // Phase 13 from here.
    /// This control plane replica is not the leader; nothing was done.
    /// `leader` is where to ask, when this replica knows.
    NotLeader {
        leader: Option<EndpointAddr>,
    },
    Ha(Box<crate::ha::HaView>),
    /// A chunk of a backup: its total size and BLAKE3 hash (the same for
    /// every chunk of one backup).
    Backup {
        total: u64,
        digest: [u8; 32],
        bytes: Vec<u8>,
    },
    // Phase 16 from here.
    Confidential(Box<crate::confidential::ConfidentialView>),
    // Phase 17 from here.
    Audit(Vec<crate::audit::AuditEntry>),
    AuditHead(Box<crate::audit::AuditHead>),
    /// The cluster's new admin ticket after `RotateAdminSecret`.
    AdminTicket {
        ticket: String,
    },
    Sso(Box<crate::sso::SsoView>),
    SsoInfo(crate::sso::SsoInfo),
    /// Where to send the browser; `state` comes back with it.
    SsoRedirect {
        url: String,
        state: String,
    },
    SsoDevice(Box<crate::sso::DeviceStart>),
    /// The device sign-in is not finished yet; ask again after `interval`
    /// seconds.
    SsoPending {
        interval_secs: u64,
    },
    // 1.2 from here.
    JoinTokens(Vec<JoinTokenView>),
    // 1.3 from here.
    LatestRelease(Box<crate::release::LatestReleaseView>),
    // 1.4 from here.
    TaskDetails(Vec<TaskDetail>),
    JobCheck(Box<JobCheck>),
    // 1.5 from here.
    NodePower(Vec<crate::power::NodePowerView>),
    TaskExecutors(Vec<Option<Executor>>),
    // 1.6 from here.
    AlertTuning(Box<crate::monitor::AlertTuningView>),
    Net(Box<crate::monitor::NetView>),
    // 1.7 from here.
    /// A new session; its secret is shown only here.
    Session {
        secret: crate::Secret,
        view: crate::ui::SessionView,
    },
    Sessions(Vec<crate::ui::SessionView>),
    NodeSettings(Box<crate::ui::NodeSettingsView>),
    Canary(Option<Box<crate::release::CanaryView>>),
    // 1.8 from here.
    Replication(Box<crate::replica::ReplicationView>),
    StoreFiles {
        dataset: crate::store::DatasetView,
        files: Vec<crate::replica::FileView>,
        s3: Vec<(String, crate::replica::S3Meta)>,
    },
    /// A new S3 key; its secret is shown only here.
    S3Key {
        secret: String,
        view: crate::replica::S3KeyView,
    },
    S3Keys(Vec<crate::replica::S3KeyView>),
    S3Auth {
        token: crate::Secret,
        signing_key: [u8; 32],
        name: String,
        role: crate::manage::AccessRole,
    },
    // 1.9 from here.
    Resources(Vec<crate::sched::ResourceView>),
    Placement(Box<crate::sched::PlacementView>),
}

/// One task of a job: the node it ran on by name, and how long each part
/// of the run that counted took (1.4). All times are 0 until known.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDetail {
    pub index: u32,
    /// The name of the node in the task view's `node`.
    pub node_name: Option<String>,
    /// Waiting in the queue before the run was placed on a node (since
    /// the job was submitted, its dependencies finished, or the run
    /// before it failed).
    pub queued_ms: u64,
    /// See [`TaskTiming`].
    pub fetch_ms: u64,
    pub pull_ms: u64,
    /// The run itself, which is the task's `wall_ms` and what the receipt
    /// bills: creating the container or VM, copying inputs in, the
    /// program, and copying outputs out. Waiting, fetching, pulling and
    /// uploading are not in it.
    pub run_ms: u64,
    pub upload_ms: u64,
}

/// The answer to [`AdminOp::CheckJob`]: the job would be accepted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCheck {
    /// The account that would pay for it.
    pub account: String,
    pub tasks: u32,
    /// Nodes that would start one of its tasks right now.
    pub ready_nodes: Vec<String>,
    /// Why no task would start right now (the job would wait in the
    /// queue): the jobs it waits for, the account's quota, or each node
    /// with its reason. `None` when a node is ready.
    pub waiting: Option<String>,
}

/// A join ticket or invite that can still be used; its secret is not
/// shown again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinTokenView {
    /// Short id derived from the secret, to revoke it by.
    pub id: String,
    pub expires_unix: u64,
    /// Machines that can still join with it; `None` for any number.
    pub uses_left: Option<u32>,
    /// Set for invites of other people's machines.
    pub invite: Option<Invite>,
}

/// The id of a join secret: the first bytes of its hash, in hex.
pub fn join_token_id(secret: &crate::Secret) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/join-id/v1\0");
    h.update(secret);
    crate::short_hex(h.finalize().as_bytes())
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
    /// No rollout, the node runs the release binary, or it runs a newer
    /// version than the release (a rollout never downgrades).
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
    // Phase 14 from here.
    /// WebGPU adapters wasm jobs can use (protocol 12 agents).
    pub webgpu: Vec<WebGpuView>,
    // Phase 15 from here.
    /// Executors its owner allowed that cannot run now, and why.
    pub unavailable: Vec<(Executor, String)>,
    /// Its Docker GPU tasks can draw (OpenGL, EGL, Vulkan).
    pub gpu_graphics: bool,
}

impl NodeView {
    /// How a GPU no job can use here would become usable, in English and
    /// Korean: empty when every GPU is usable by some executor.
    pub fn gpu_hints(&self) -> Vec<(&'static str, &'static str)> {
        if !self.gpus.iter().any(|g| !g.info.docker && !g.info.vfio) {
            return Vec::new();
        }
        let mut v = Vec::new();
        let docker = self.executors.contains(&Executor::Docker)
            || self.unavailable.iter().any(|(e, _)| *e == Executor::Docker);
        match self.info.os.as_str() {
            "macos" => v.push((
                "macOS lends GPUs to no container or microVM; wasm jobs can use it through WebGPU (agent option --allow-webgpu, experimental).",
                "macOS는 컨테이너나 microVM에 GPU를 넘겨주지 못합니다. wasm 작업은 WebGPU로 쓸 수 있습니다(에이전트 옵션 --allow-webgpu, 실험).",
            )),
            os => {
                if docker {
                    v.push((
                        "Docker cannot reach it: on Linux install the NVIDIA Container Toolkit, on Windows use Docker Desktop's WSL 2 backend; `pumasi gpu check --docker-run` tests it.",
                        "Docker가 GPU를 못 씁니다. Linux는 NVIDIA Container Toolkit을, Windows는 Docker Desktop의 WSL 2 백엔드를 쓰세요. `pumasi gpu check --docker-run`으로 확인합니다.",
                    ));
                } else {
                    v.push((
                        "Docker GPU jobs (gpus = 1) need Docker on this machine: start its agent with --allow-docker (Docker Desktop on Windows).",
                        "Docker GPU 작업(gpus = 1)은 이 기기에 Docker가 있어야 합니다. 에이전트를 --allow-docker로 시작하세요(Windows는 Docker Desktop).",
                    ));
                }
                if os != "linux" {
                    v.push((
                        "MicroVM GPU passthrough is Linux only.",
                        "microVM GPU 패스스루는 Linux에서만 됩니다.",
                    ));
                }
            }
        }
        if self.webgpu.is_empty() && self.info.os != "macos" {
            v.push((
                "Wasm jobs can use it through WebGPU without Docker: agent option --allow-webgpu (experimental).",
                "Docker 없이도 wasm 작업이 WebGPU로 쓸 수 있습니다: 에이전트 옵션 --allow-webgpu(실험).",
            ));
        }
        v
    }
}

/// A WebGPU adapter in the cluster status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebGpuView {
    pub adapter: WebGpuAdapter,
    /// FP32 GFLOPS its runs are billed at.
    pub billed_gflops: u32,
    /// Why the billed figure differs from the profile's.
    pub note: Option<String>,
    /// The task using it (or the GPU it is) right now.
    pub busy: Option<TaskRef>,
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
    // Phase 15 from here.
    /// Which nodes can take the caller's account's work right now.
    pub capacity: Option<Capacity>,
}

/// Which nodes can take an account's work right now, and why the others
/// cannot: what a remote client looks at before sending work (or to fall
/// back to running it elsewhere).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub account: String,
    /// Nodes that would take a new task of the account now (with a free
    /// slot), by executor (the executors the account may use).
    pub ready: std::collections::BTreeMap<Executor, u32>,
    /// Of those, nodes with a free GPU for image jobs.
    pub ready_gpu: u32,
    /// Of those, nodes whose Docker GPU tasks can draw.
    pub ready_gpu_graphics: u32,
    pub nodes: Vec<NodeCapacity>,
}

impl Capacity {
    /// Nodes ready for `executor`.
    pub fn ready_for(&self, executor: Executor) -> u32 {
        self.ready.get(&executor).copied().unwrap_or(0)
    }
}

/// One node, as the account's work sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub node: String,
    /// Executors it would run the account's tasks with now.
    pub executors: Vec<Executor>,
    pub free_slots: u32,
    pub free_gpus: u32,
    /// Why it takes none of the account's work now (offline, paused,
    /// cordoned, Docker unavailable, full, not allowed by the account's
    /// policy...). `None` when it takes some.
    pub reason: Option<String>,
    /// Executors its owner allowed that cannot run now, and why.
    pub unavailable: Vec<(Executor, String)>,
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
    // Phase 15 from here.
    /// The ticket that submitted it: an access ticket's name, `cluster
    /// admin`, or `schedule NAME`.
    pub submitted_by: String,
    /// The images its tasks ran, by digest (as the nodes pulled them).
    pub image_digests: Vec<String>,
    pub queue_timeout_ms: Option<u64>,
    pub gpu_capabilities: Vec<crate::job::GpuCapability>,
    // Phase 16 from here.
    pub tier: crate::job::Tier,
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

    /// 1.9 fields ride in new calls only when set, and come back intact
    /// through postcard.
    #[test]
    fn placements_and_time_zones_travel_beside_old_calls() {
        use crate::sched::{Checkpoint, Placement};
        let sub = crate::job::JobSubmission {
            name: "j".into(),
            executor: crate::job::Executor::Wasm,
            module: vec![],
            docker: None,
            files: vec![],
            tasks: vec![],
            limits: Default::default(),
            tier: crate::job::Tier::Open,
            max_attempts: 1,
            max_price_per_hour: None,
            account: None,
            deterministic: false,
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
            placement: Default::default(),
        };
        assert!(matches!(AdminOp::submit(sub.clone()), AdminOp::Submit(_)));
        let mut placed = sub.clone();
        placed.placement = Placement {
            gang: true,
            checkpoint: Some(Checkpoint { grace_ms: 9_000 }),
            ..Default::default()
        };
        let op = AdminOp::submit(placed.clone());
        assert!(matches!(op, AdminOp::SubmitPlaced { .. }));
        let bytes = postcard::to_stdvec(&op).unwrap();
        let back: AdminOp = postcard::from_bytes(&bytes).unwrap();
        let AdminOp::Submit(got) = back.joined() else {
            panic!("not joined");
        };
        assert_eq!(got.placement, placed.placement);

        let spec = crate::manage::ScheduleSpec {
            name: "s".into(),
            cron: "0 9 * * *".into(),
            job: sub,
            overlap: false,
            tz: Some("Asia/Seoul".into()),
        };
        let op = AdminOp::set_schedule(spec.clone());
        let bytes = postcard::to_stdvec(&op).unwrap();
        let back: AdminOp = postcard::from_bytes(&bytes).unwrap();
        let AdminOp::SetSchedule(got) = back.joined() else {
            panic!("not joined");
        };
        assert_eq!(*got, spec);
        let mut plain = spec;
        plain.tz = None;
        assert!(matches!(
            AdminOp::set_schedule(plain),
            AdminOp::SetSchedule(_)
        ));
    }

    #[test]
    fn cron_runs_in_its_time_zone() {
        let c: crate::manage::Cron = "0 9 * * *".parse().unwrap();
        // 2026-03-01 00:00 UTC; 09:00 in Seoul is 00:00 UTC.
        let t0 = 1_772_323_200;
        assert_eq!(c.next_in(t0, Some("Asia/Seoul")), Some(t0 + 86_400));
        assert_eq!(c.next_in(t0, Some("UTC")), Some(t0 + 9 * 3600));
        assert_eq!(c.next_in(t0, Some("Mars/Base")), None);
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
