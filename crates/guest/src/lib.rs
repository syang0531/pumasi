//! What the agent and the guest init (`pumasi-guest`, PID 1 inside every
//! microVM) agree on.
//!
//! The guest has no network and no shared folders. Everything crosses the
//! VM boundary as virtio block devices, and the guest finds each one by
//! what is on it rather than by its device name (VMMs attach them in
//! different orders):
//!
//! | Disk | Contents | Found by |
//! | --- | --- | --- |
//! | base | the image's root filesystem, ext4, read-only while tasks run | ext4 label [`BASE_LABEL`] |
//! | scratch | empty ext4, the writable layer of one task | ext4 label [`SCRATCH_LABEL`] |
//! | layers | tar of the image's layer blobs (build mode) | tar whose first entry is [`DISK_ROLE_ENTRY`] = `layers` |
//! | input | tar of the run spec, stdin and `/input` files (run mode) | same, `input` |
//! | output | written by the guest: a tar of the result | starts with [`OUTPUT_MARK`] until the guest overwrites it |
//!
//! The mode comes from the kernel command line (`pumasi.mode=build|run|probe`).
//! In build mode the guest unpacks the layers onto the empty base disk, so
//! the untrusted image tarballs are never unpacked on the host. In run mode
//! it stacks an overlay of the scratch disk on the read-only base, copies
//! the inputs in, runs the command, and writes the result. In probe mode
//! it copies the input files to the output, which tells the host that disk
//! I/O works under its accelerator.

use serde::{Deserialize, Serialize};

/// Label of the ext4 filesystem holding an image's root filesystem.
pub const BASE_LABEL: &str = "pumasi-base";
/// Label of a task's empty writable disk.
pub const SCRATCH_LABEL: &str = "pumasi-scratch";
/// First entry of the layers and input tars; its contents name the role.
pub const DISK_ROLE_ENTRY: &str = ".pumasi-disk";
pub const ROLE_LAYERS: &str = "layers";
pub const ROLE_INPUT: &str = "input";
/// The host fills the start of the output disk with this; the guest
/// replaces it with a tar. A disk that still starts with it after the VM
/// exits means the guest never got to write a result.
pub const OUTPUT_MARK: &[u8] = b"PUMASI-OUTPUT-EMPTY";
/// Kernel command line parameter that selects the mode.
pub const MODE_PARAM: &str = "pumasi.mode";

/// Entries of the input tar besides [`DISK_ROLE_ENTRY`].
pub const SPEC_ENTRY: &str = "spec.json";
pub const STDIN_ENTRY: &str = "stdin";
/// Prefix of input files; `input/a/b.txt` becomes `/input/a/b.txt`.
pub const INPUT_PREFIX: &str = "input/";

/// Entries of the output tar.
pub const RESULT_ENTRY: &str = "result.json";
pub const STDOUT_ENTRY: &str = "stdout";
pub const STDERR_ENTRY: &str = "stderr";
/// Prefix of output files; `/output/x.csv` is `output/x.csv`.
pub const OUTPUT_PREFIX: &str = "output/";

/// Caps the guest applies before writing the result (the host checks them
/// again). Kept equal to the limits in `pumasi_proto::job`.
pub const MAX_STREAM_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_OUTPUT_FILES_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_OUTPUT_FILES: usize = 10_000;
/// Size of the output disk: the caps plus tar headers for every file.
pub const OUTPUT_DISK_BYTES: u64 =
    2 * MAX_STREAM_BYTES + MAX_OUTPUT_FILES_BYTES + (MAX_OUTPUT_FILES as u64 + 16) * 1024;

/// What to run, decided on the host from the job and the image config.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    /// Full argv; `argv[0]` is looked up in `PATH` unless it has a `/`.
    pub argv: Vec<String>,
    /// `KEY=value` pairs, already merged (image, then job).
    pub env: Vec<String>,
    /// Working directory inside the guest.
    pub workdir: String,
    /// The image's `User` (`name`, `uid`, `name:group`, `uid:gid`), empty
    /// for root.
    pub user: String,
    pub hostname: String,
}

/// How the guest's work ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GuestOutcome {
    /// The command exited with this code (build mode: 0 means success).
    Exited(i32),
    /// The command was killed by this signal.
    Signaled(i32),
    /// The kernel's OOM killer ended the command.
    OutOfMemory,
    /// The guest could not do its part (bad disk, mount failure, ...).
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestResult {
    pub outcome: GuestOutcome,
    /// Bytes of stdout / stderr that did not fit.
    pub stdout_dropped: u64,
    pub stderr_dropped: u64,
    /// Output files left out because of the caps.
    pub files_dropped: u32,
}

impl GuestResult {
    pub fn failed(msg: impl Into<String>) -> Self {
        Self {
            outcome: GuestOutcome::Failed(msg.into()),
            stdout_dropped: 0,
            stderr_dropped: 0,
            files_dropped: 0,
        }
    }
}
