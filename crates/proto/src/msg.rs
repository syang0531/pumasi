//! Messages exchanged over the agent and admin protocols.
//!
//! Agent protocol (`pumasi/agent/0`): the agent dials the control plane,
//! opens one bidirectional stream, sends [`AgentHello`] and receives
//! [`HelloReply`]. After that both sides stream [`ToAgent`] / [`FromAgent`]
//! messages until the connection drops.
//!
//! Admin protocol (`pumasi/admin/0`): one [`AdminRequest`] and one
//! [`AdminResponse`] per bidirectional stream.

use serde::{Deserialize, Serialize};

use crate::{
    ModuleHash, Secret,
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput},
};

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HelloReply {
    Welcome {
        cluster_name: String,
        /// How often the agent must send a heartbeat.
        heartbeat_ms: u64,
    },
    Rejected {
        reason: String,
    },
}

/// One placement of one task. `attempt` distinguishes retries so stale
/// reports from a node that was presumed dead can be ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskRef {
    pub job: JobId,
    pub index: u32,
    pub attempt: u32,
}

impl std::fmt::Display for TaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}#{}", self.job, self.index, self.attempt)
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToAgent {
    Assign(Box<TaskAssignment>),
    Cancel(TaskRef),
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FromAgent {
    Heartbeat { running: Vec<TaskRef> },
    Done(Box<TaskReport>),
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
    CreateJoinToken {
        ttl_secs: u64,
        uses: Option<u32>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    JoinToken { ticket: String, expires_unix: u64 },
    Submitted { id: JobId },
    Status(ClusterStatus),
    Job(Box<JobView>),
    Files(Vec<FileEntry>),
    Ok,
    Error(String),
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
    Running { node: String },
    Succeeded,
    Failed(Outcome),
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeView {
    pub id: String,
    pub info: NodeInfo,
    pub online: bool,
    pub running: u32,
    pub completed: u64,
    pub last_seen_unix: u64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobView {
    pub summary: JobSummary,
    pub executor: Executor,
    pub image: Option<String>,
    pub limits: Limits,
    pub tasks: Vec<TaskView>,
    pub total_fuel: u64,
}
