//! Control plane high availability (phase 13): what `pumasi ha status`
//! shows.

use serde::{Deserialize, Serialize};

/// The control plane replicas and the Raft state, as the leader sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaView {
    /// Replication is on (the cluster has, or had, more than one replica).
    pub enabled: bool,
    /// The endpoint id of the replica answering (the leader).
    pub leader: String,
    pub term: u64,
    /// The last log index a majority has.
    pub committed: Option<u64>,
    /// The last log index this replica applied to its state.
    pub applied: Option<u64>,
    /// The last index in a snapshot.
    pub snapshot: Option<u64>,
    pub replicas: Vec<ReplicaView>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaView {
    /// Endpoint id.
    pub id: String,
    /// Votes in elections and counts toward the majority. A replica that
    /// is still catching up is a learner.
    pub voter: bool,
    pub leader: bool,
    /// The last log index known to be on that replica.
    pub matched: Option<u64>,
    /// How long since that replica last answered the leader.
    pub last_seen_ms: Option<u64>,
}

impl HaView {
    /// Voters needed for a majority.
    pub fn quorum(&self) -> usize {
        self.replicas.iter().filter(|r| r.voter).count() / 2 + 1
    }

    /// Voters that answered in the last `within_ms`.
    pub fn voters_up(&self, within_ms: u64) -> usize {
        self.replicas
            .iter()
            .filter(|r| r.voter && (r.leader || r.last_seen_ms.is_some_and(|t| t <= within_ms)))
            .count()
    }
}

/// What a leader hands its followers every couple of seconds so they can
/// answer reads themselves (1.12): the status, the newest events and the
/// latest metrics, as an admin sees them. A follower answers a
/// [`crate::msg::AdminOp::FollowerRead`] from this while it is fresh, and
/// sends the caller to the leader otherwise.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadCache {
    /// When the leader made it (UNIX milliseconds).
    pub at_unix_ms: u64,
    pub status: crate::msg::ClusterStatus,
    /// The newest events, oldest first.
    pub events: Vec<crate::monitor::EventView>,
    /// Whether older events exist than the first one here.
    pub events_cut: bool,
    /// The latest readings of every node (`since_secs` 0).
    pub metrics: crate::monitor::MetricsView,
}

/// Events a [`ReadCache`] carries at most.
pub const READ_CACHE_EVENTS: usize = 500;
/// A follower answers from a cache at most this old (milliseconds).
pub const READ_CACHE_FRESH_MS: u64 = 6_000;

/// What a 1.19 leader hands its followers beside the [`ReadCache`], so
/// they answer what the API and the CLI read too: the jobs that are still
/// going or just ended, what each account with a submitter ticket sees as
/// its capacity, and the nodes' power states.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReadMore {
    /// Running and queued jobs, and jobs that ended in the last
    /// [`READ_JOBS_DONE_SECS`], newest first, while their tasks add up to
    /// at most [`READ_JOBS_TASKS`].
    pub jobs: Vec<JobRead>,
    /// The capacity in each such account's status.
    pub capacity: Vec<(String, crate::msg::Capacity)>,
    pub power: Vec<crate::power::NodePowerView>,
}

/// One job as [`crate::msg::AdminOp::Job`] (without output),
/// [`crate::msg::AdminOp::TaskDetails`] and
/// [`crate::msg::AdminOp::TaskExecutors`] answer it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRead {
    pub view: crate::msg::JobView,
    pub details: Vec<crate::msg::TaskDetail>,
    pub executors: Vec<Option<crate::job::Executor>>,
}

/// How long after it ended a job stays in [`ReadMore`].
pub const READ_JOBS_DONE_SECS: u64 = 600;
/// Tasks the jobs in [`ReadMore`] have at most together.
pub const READ_JOBS_TASKS: usize = 20_000;
/// Accounts whose capacity [`ReadMore`] carries at most.
pub const READ_CAPACITY_ACCOUNTS: usize = 64;
