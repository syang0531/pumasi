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
