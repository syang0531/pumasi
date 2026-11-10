//! What the agent and the guest init (`pumasi-guest`, PID 1 inside every
//! microVM) agree on.
//!
//! The guest has no shared folders, and no network unless the job asks for
//! it ([`RunSpec::network`]: a virtio-net card whose other end is the
//! agent's filtering gateway). Everything crosses the
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
//! | gpu | the GPU driver for the GPU guest kernel, ext4, read-only (GPU tasks only) | ext4 label [`GPU_LABEL`] |
//!
//! The mode comes from the kernel command line (`pumasi.mode=build|run|probe`).
//! In build mode the guest unpacks the layers onto the empty base disk, so
//! the untrusted image tarballs are never unpacked on the host. In run mode
//! it stacks an overlay of the scratch disk on the read-only base, copies
//! the inputs in, runs the command, and writes the result. In probe mode
//! it copies the input files to the output, which tells the host that disk
//! I/O works under its accelerator.

use serde::{Deserialize, Serialize};

pub mod seal;

/// Label of the ext4 filesystem holding an image's root filesystem.
pub const BASE_LABEL: &str = "pumasi-base";
/// Label of a task's empty writable disk.
pub const SCRATCH_LABEL: &str = "pumasi-scratch";
/// Label of the GPU driver disk: `modules.order` lists the kernel modules
/// under `modules/` in load order, `lib/` holds the driver's user-space
/// libraries (mounted at `/usr/local/nvidia/lib64` in the task's root, where
/// CUDA images look) and `bin/` its tools (`/usr/local/nvidia/bin`).
pub const GPU_LABEL: &str = "pumasi-gpu";
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
/// Kernel command line parameter that picks how the guest ends the VM:
/// [`EXIT_POWER_OFF`], or else a reboot (which `reboot=k` and QEMU's
/// `-no-reboot` turn into the VMM exiting). Virtualization.framework
/// boots a rebooting guest again, so the vz backend asks for a power off.
pub const EXIT_PARAM: &str = "pumasi.exit";
pub const EXIT_POWER_OFF: &str = "poweroff";
/// The console line the guest prints right before it ends the VM (`log`
/// in init.rs adds the `pumasi-guest: ` prefix to "done").
pub const DONE_LINE: &str = "pumasi-guest: done";

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
    /// Bytes of `/output` files the guest may write (0: the inline limit,
    /// [`MAX_OUTPUT_FILES_BYTES`]). Larger when the output goes to the
    /// file store; the host sizes the output disk to match.
    #[serde(default)]
    pub output_limit: u64,
    /// Most `/output` files (0: [`MAX_OUTPUT_FILES`]).
    #[serde(default)]
    pub output_files: u32,
    /// Bring up the network card with these settings (`network = true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<GuestNet>,
}

/// The guest's network settings: one card (`eth0`), a default route
/// through the gateway, which is also the DNS server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestNet {
    pub address: [u8; 4],
    pub prefix: u8,
    pub gateway: [u8; 4],
    pub dns: [u8; 4],
}

impl RunSpec {
    pub fn files_limit(&self) -> u64 {
        if self.output_limit == 0 {
            MAX_OUTPUT_FILES_BYTES
        } else {
            self.output_limit
        }
    }

    pub fn count_limit(&self) -> usize {
        if self.output_files == 0 {
            MAX_OUTPUT_FILES
        } else {
            self.output_files as usize
        }
    }

    /// The output disk this spec needs: both streams, the files and a tar
    /// header for every file.
    pub fn output_disk_bytes(&self) -> u64 {
        2 * MAX_STREAM_BYTES + self.files_limit() + (self.count_limit() as u64 + 16) * 1024
    }
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
