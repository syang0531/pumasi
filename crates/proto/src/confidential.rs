//! The confidential tier (phase 16): tasks that run in a microVM whose
//! memory the node's owner cannot read (AMD SEV-SNP or Intel TDX), with
//! their image, input and output sealed to keys the control plane releases
//! only to a guest whose attestation it checked.
//!
//! See `pumasi_guest::seal` for the sealed streams and the key release,
//! and `docs/architecture.md` (phase 16) for the whole flow.

use serde::{Deserialize, Serialize};

pub use pumasi_guest::seal::{Evidence, TeeKind};

use crate::store::BlobHash;

/// The one file in the collection of a sealed image.
pub const SEALED_IMAGE_FILE: &str = "image.sealed";
/// The one file in the collection of a task's sealed input.
pub const SEALED_INPUT_FILE: &str = "input.sealed";
/// The one file a confidential run reports as its output (inline, or as
/// a stored collection): the guest's sealed result.
pub const SEALED_OUTPUT_FILE: &str = "output.sealed";

/// A sealed stream in the file store: a collection holding one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBlob {
    pub root: BlobHash,
    /// Bytes of the sealed file.
    pub size: u64,
}

/// What an assignment of a confidential task carries besides the redacted
/// job: which TEE to start the VM in, the run's nonce (bound into the
/// attestation and the output), and the sealed image and input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfidentialTask {
    pub tee: TeeKind,
    pub nonce: [u8; 32],
    pub image: SealedBlob,
    pub input: SealedBlob,
    /// Size of the output disk the guest writes its sealed result to.
    pub output_bytes: u64,
    /// Memory the VM needs on top of the task's limit: the image, input
    /// and output live in its (encrypted) memory, not on host disks.
    pub extra_mib: u64,
}

/// Which TEEs a node can start confidential VMs in, and why not when its
/// owner asked for them but it cannot (protocol 14).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeeReport {
    pub tees: Vec<TeeKind>,
    pub unavailable: Option<String>,
}

/// The confidentiality tiers, the TEE nodes, and the measurements the
/// cluster has seen and trusts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfidentialView {
    /// The control plane accepts the test stand-in for a TEE
    /// (`--insecure-test-tee`): nothing confidential is protected.
    pub test_tee: bool,
    pub nodes: Vec<TeeNodeView>,
    pub measurements: Vec<MeasurementView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeeNodeView {
    pub id: String,
    pub name: String,
    pub online: bool,
    /// The TEEs it starts confidential VMs in.
    pub tees: Vec<TeeKind>,
    /// Why it starts none, when its owner turned confidential VMs on.
    pub unavailable: Option<String>,
    /// Runs `tier = "trusted"` jobs: one of the owner's machines, or a
    /// provider the admin trusts (`pumasi trust`).
    pub trusted_host: bool,
    /// The admin trusts it (as opposed to it being the owner's).
    pub trusted_by_admin: bool,
}

/// A launch measurement a guest reported: the firmware, kernel, initramfs
/// (with the guest init) and command line of the confidential VM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementView {
    /// SEV-SNP: the `MEASUREMENT` field, hex. TDX: SHA-384 of MRTD,
    /// RTMR1 and RTMR2, hex (the raw registers are in `detail`).
    pub id: String,
    pub tee: TeeKind,
    pub trusted: bool,
    pub note: String,
    /// Where it was seen first and last.
    pub node: String,
    pub agent_version: String,
    pub vcpus: u32,
    pub first_seen_unix: u64,
    pub last_seen_unix: u64,
    /// Attestations that reported it.
    pub seen: u64,
    /// Firmware versions, TDX registers and the like.
    pub detail: String,
}

/// The shortest unique form of a measurement id an admin may type.
pub const MIN_MEASUREMENT_PREFIX: usize = 8;
