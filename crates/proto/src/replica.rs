//! Copies of the file store on several machines (1.8): replicas of
//! dataset versions kept on owner nodes labelled for storage, what the
//! control plane reports about them, and the records of the S3 gateway in
//! `pumasi ui`.
//!
//! A dataset version is wanted on `factor` copies (2 unless the dataset or
//! the cluster says otherwise). Every control plane replica counts as one
//! copy: the leader keeps every version, followers copy the leader's store.
//! The rest are nodes with the [`STORAGE_LABEL`] label, whose value is the
//! most the node keeps (`storage=200G`; `storage=yes` keeps to 90% of the
//! node's disk).

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{manage::AccessRole, store::BlobHash};

/// The node label that offers a node's disk for replicas.
pub const STORAGE_LABEL: &str = "storage";
/// Copies of each dataset version unless the dataset or the cluster sets
/// another number.
pub const DEFAULT_REPLICAS: u8 = 2;
/// The most copies a dataset may ask for.
pub const MAX_REPLICAS: u8 = 9;

/// The quota a `storage` label's value gives: a size, or `None` for "up to
/// 90% of the disk" (`yes`, `true`, `on` or empty).
pub fn storage_quota(value: &str) -> Result<Option<u64>> {
    let v = value.trim().to_ascii_lowercase();
    if matches!(v.as_str(), "" | "yes" | "true" | "on" | "1") {
        return Ok(None);
    }
    let bytes = crate::store::parse_size(&v)?;
    ensure!(bytes > 0, "a storage quota must be more than 0 bytes");
    Ok(Some(bytes))
}

pub fn validate_factor(factor: u8) -> Result<()> {
    ensure!(
        (1..=MAX_REPLICAS).contains(&factor),
        "the number of copies must be 1 to {MAX_REPLICAS}"
    );
    Ok(())
}

/// Where one replica stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Placed; the node is fetching it.
    Copying,
    /// The node has all of it.
    Held,
}

/// One node's copy of a dataset version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaView {
    pub node: String,
    /// The node's display name.
    pub name: String,
    pub state: ReplicaState,
    pub online: bool,
    /// Counted as a copy: held, and the node is a storage node that is
    /// online or has not been offline long enough to be replaced.
    pub counted: bool,
    pub since_unix: u64,
    /// Why the last download stopped, while copying.
    pub error: Option<String>,
}

/// The copies of one dataset version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetReplicas {
    pub name: String,
    pub version: u32,
    pub account: String,
    pub root: BlobHash,
    pub size: u64,
    /// Copies wanted, control planes included.
    pub factor: u8,
    /// Whether the dataset has its own factor (else the cluster default).
    pub own_factor: bool,
    /// Copies there are now: control planes that have it, and counted node
    /// replicas.
    pub copies: u8,
    /// The leader has all of it (it may be fetching it back from nodes).
    pub leader_has: bool,
    pub nodes: Vec<ReplicaView>,
}

/// A node that offers (or could offer) its disk for replicas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageNodeView {
    pub node: String,
    pub name: String,
    pub online: bool,
    /// Why it gets no new replicas (offline, an agent older than 1.8, ...).
    pub not_used: Option<String>,
    /// The label's quota; `None`: up to 90% of its disk.
    pub quota: Option<u64>,
    /// Bytes of the replicas placed on it.
    pub used: u64,
    pub replicas: u32,
    /// Its disk, from its latest metrics.
    pub disk_total: Option<u64>,
    pub disk_free: Option<u64>,
    pub scrubbed_unix: Option<u64>,
    /// Collections its last scrub found damaged.
    pub bad: u32,
}

/// Where the file store's copies are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationView {
    pub default_factor: u8,
    /// Control plane replicas (1 without replication).
    pub control_planes: u8,
    pub datasets: Vec<DatasetReplicas>,
    pub nodes: Vec<StorageNodeView>,
    /// Dataset versions on fewer copies than wanted.
    pub under: u32,
    /// Dataset versions the leader lacks and is fetching from nodes.
    pub leader_missing: u32,
    pub scrubbed_unix: Option<u64>,
    /// Collections the leader's last scrub found damaged.
    pub bad: u32,
}

/// What a scrub found: every collection read back and checked against its
/// BLAKE3 hashes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubReport {
    pub checked: u32,
    pub bytes: u64,
    /// Collections with a blob whose data no longer matches its hash.
    pub bad: Vec<BlobHash>,
}

/// One file of a dataset version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileView {
    pub path: String,
    pub size: u64,
    pub hash: BlobHash,
}

/// What the S3 gateway keeps about an object it wrote: S3 clients compare
/// ETags (the MD5 of a single upload) and read back the metadata they set
/// (rclone keeps modification times in `x-amz-meta-mtime`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Meta {
    /// The file's content when this was recorded; ignored once the file
    /// changed by other means (`pumasi store put`).
    pub hash: BlobHash,
    /// Without quotes.
    pub etag: String,
    pub content_type: Option<String>,
    /// `x-amz-meta-*` headers, names lowercased without the prefix.
    pub meta: Vec<(String, String)>,
    pub modified_unix: u64,
}

/// Most `x-amz-meta-*` bytes per object (S3's own limit).
pub const MAX_S3_META_BYTES: usize = 2048;

/// An S3 access key. It acts as the ticket it was made with, which must
/// stay valid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3KeyView {
    /// The access key id (`PUMA...`).
    pub id: String,
    /// The name of its ticket.
    pub name: String,
    pub role: AccessRole,
    pub label: String,
    pub created_unix: u64,
    pub last_used_unix: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_label_values() {
        assert_eq!(storage_quota("yes").unwrap(), None);
        assert_eq!(storage_quota("").unwrap(), None);
        assert_eq!(storage_quota("200G").unwrap(), Some(200 << 30));
        assert_eq!(storage_quota("1.5TiB").unwrap(), Some(3 << 39));
        assert!(storage_quota("0").is_err());
        assert!(storage_quota("lots").is_err());
        assert!(validate_factor(0).is_err());
        assert!(validate_factor(2).is_ok());
    }
}
