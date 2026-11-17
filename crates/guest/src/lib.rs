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
//! | control | the host's messages while the VM runs (1.14): a task for a warm guest, a stop request | starts with [`CONTROL_MAGIC`] |
//!
//! The mode comes from the kernel command line (`pumasi.mode=build|run|probe|pool`).
//! In build mode the guest unpacks the layers onto the empty base disk, so
//! the untrusted image tarballs are never unpacked on the host. In run mode
//! it stacks an overlay of the scratch disk on the read-only base, copies
//! the inputs in, runs the command, and writes the result. In probe mode
//! it copies the input files to the output, which tells the host that disk
//! I/O works under its accelerator.
//!
//! In pool mode (1.14) the guest boots and mounts its disks like run mode,
//! prints [`POOL_READY_LINE`] and waits: the host keeps it warm until a task
//! of the same image comes, then writes the input tar onto the control disk
//! ([`ControlLayout`]) and the guest goes on as in run mode. A guest runs
//! one task and powers off, warm or not.
//!
//! The control disk also carries a stop request (checkpoints, 1.14): the
//! guest sends the task `SIGTERM` when it sees [`StopRequest::Term`] and
//! `SIGKILL` at [`StopRequest::Kill`], then writes the result with what the
//! task left in `/checkpoint` ([`CHECKPOINT_PREFIX`]). The guest reads the
//! control disk with `O_DIRECT`, so it sees what the host wrote since.

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

/// Prefix of the checkpoint files a checkpointing run gets back (input
/// tar) and leaves (output tar): `checkpoint/a.bin` is `/checkpoint/a.bin`.
pub const CHECKPOINT_PREFIX: &str = "checkpoint/";
/// Where the task finds and leaves its checkpoint.
pub const CHECKPOINT_DIR: &str = "/checkpoint";
/// Most files a checkpoint keeps.
pub const MAX_CHECKPOINT_FILES: usize = 100_000;

/// The console line a warm guest prints once its disks are mounted and it
/// waits for a task, and the one it prints when the task came.
pub const POOL_READY_LINE: &str = "pumasi-guest: ready";
pub const POOL_TASK_LINE: &str = "pumasi-guest: task received";

/// The control disk starts with this.
pub const CONTROL_MAGIC: &[u8; 16] = b"PUMASI-CONTROL01";
const TASK_MAGIC: &[u8; 16] = b"PUMASI-TASK-0001";
const STOP_MAGIC: &[u8; 12] = b"PUMASI-STOP\0";

/// Where things are on the control disk. Everything the guest reads is
/// 4 KiB-aligned (`O_DIRECT`).
pub struct ControlLayout;

impl ControlLayout {
    /// The stop request, one block.
    pub const STOP_AT: u64 = 4096;
    /// The task header (length and SHA-256 of the input tar), one block.
    pub const TASK_AT: u64 = 8192;
    /// The input tar of the task, up to the end of the disk.
    pub const DATA_AT: u64 = 1 << 20;
}

/// The control disk's first block, as the host writes it: a warm guest
/// that gets no task within `idle_secs` (0: no limit) powers off, so a
/// host that went away without stopping it leaves no VM running for long.
pub fn control_head(idle_secs: u64) -> Vec<u8> {
    let mut b = vec![0u8; 4096];
    b[..16].copy_from_slice(CONTROL_MAGIC);
    b[16..24].copy_from_slice(&idle_secs.to_le_bytes());
    b
}

/// The idle limit in a control disk's first block.
pub fn control_idle_secs(b: &[u8]) -> Option<u64> {
    if b.len() < 24 || &b[..16] != CONTROL_MAGIC {
        return None;
    }
    Some(u64::from_le_bytes(b[16..24].try_into().ok()?))
}

/// The task header block: the input tar of `len` bytes with this SHA-256
/// is at [`ControlLayout::DATA_AT`].
pub fn task_header(len: u64, sha256: &[u8; 32]) -> Vec<u8> {
    let mut b = vec![0u8; 4096];
    b[..16].copy_from_slice(TASK_MAGIC);
    b[16..24].copy_from_slice(&len.to_le_bytes());
    b[24..56].copy_from_slice(sha256);
    b
}

/// The length and SHA-256 in a task header, if the block holds one.
pub fn parse_task_header(b: &[u8]) -> Option<(u64, [u8; 32])> {
    if b.len() < 56 || &b[..16] != TASK_MAGIC {
        return None;
    }
    let len = u64::from_le_bytes(b[16..24].try_into().ok()?);
    let hash = b[24..56].try_into().ok()?;
    Some((len, hash))
}

/// What the host asks of a running task through the control disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopRequest {
    /// Save a checkpoint and exit: the guest sends `SIGTERM`.
    Term,
    /// The grace period is over: the guest sends `SIGKILL` and writes the
    /// result with whatever is in `/checkpoint`.
    Kill,
}

impl StopRequest {
    pub fn block(self) -> Vec<u8> {
        let mut b = vec![0u8; 4096];
        b[..12].copy_from_slice(STOP_MAGIC);
        b[12] = match self {
            StopRequest::Term => 1,
            StopRequest::Kill => 2,
        };
        b
    }

    pub fn parse(b: &[u8]) -> Option<Self> {
        if b.len() < 13 || &b[..12] != STOP_MAGIC {
            return None;
        }
        match b[12] {
            1 => Some(StopRequest::Term),
            2 => Some(StopRequest::Kill),
            _ => None,
        }
    }
}

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
    /// Print the task's output on the console as it comes (1.6), in
    /// [`live_frame`] lines, for the agent to stream while the task runs.
    /// The result on the output disk still has all of it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub live: bool,
    /// A checkpointing run (1.14): `/checkpoint` holds the input tar's
    /// [`CHECKPOINT_PREFIX`] files, is writable by any user, and goes back
    /// in the output tar, up to this many bytes. 0: no checkpoint.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub checkpoint_limit: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
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
        let checkpoint = if self.checkpoint_limit > 0 {
            self.checkpoint_limit + (MAX_CHECKPOINT_FILES as u64) * 1024
        } else {
            0
        };
        2 * MAX_STREAM_BYTES
            + self.files_limit()
            + (self.count_limit() as u64 + 16) * 1024
            + checkpoint
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

// ---------------------------------------------------------------------------
// Live output on the console (1.6)
// ---------------------------------------------------------------------------

/// Start of a console line that carries task output. The record separator
/// keeps it apart from anything the kernel or a program prints.
pub const LIVE_PREFIX: &str = "\u{1e}pumasi-out ";
/// Output the guest prints live per stream; the rest comes with the
/// result only. What is streamed is always the start of the output, so
/// the final output continues it.
pub const LIVE_MAX_BYTES: u64 = 1 << 20;
/// Bytes of output in one console line.
pub const LIVE_CHUNK: usize = 3 * 1024;

/// One console line of live output: stream 1 (stdout) or 2 (stderr), the
/// offset of `data` in that stream, the data in base64 and a checksum.
pub fn live_frame(stream: u8, offset: u64, data: &[u8]) -> String {
    format!(
        "{LIVE_PREFIX}{stream} {offset} {} {:08x}\n",
        base64_encode(data),
        fnv1a(data)
    )
}

/// The stream, offset and data of a [`live_frame`] line (without its line
/// end; a `\r` before it is allowed). `None` for any other line, or a
/// line that came through damaged.
pub fn parse_live_frame(line: &str) -> Option<(u8, u64, Vec<u8>)> {
    let rest = line.trim_end_matches(['\r', '\n']);
    let rest = &rest[rest.find(LIVE_PREFIX)? + LIVE_PREFIX.len()..];
    let mut parts = rest.split(' ');
    let stream: u8 = parts.next()?.parse().ok()?;
    let offset: u64 = parts.next()?.parse().ok()?;
    let data = base64_decode(parts.next()?)?;
    let sum = u32::from_str_radix(parts.next()?, 16).ok()?;
    (parts.next().is_none() && (1..=2).contains(&stream) && sum == fnv1a(&data))
        .then_some((stream, offset, data))
}

fn fnv1a(data: &[u8]) -> u32 {
    data.iter().fold(0x811c_9dc5u32, |h, b| {
        (h ^ *b as u32).wrapping_mul(0x0100_0193)
    })
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.as_bytes();
    if !s.len().is_multiple_of(4) {
        return None;
    }
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    };
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for (i, q) in s.chunks(4).enumerate() {
        let last = i == s.len() / 4 - 1;
        let pad = q.iter().rev().take_while(|c| **c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return None;
        }
        let mut n = 0u32;
        for (j, c) in q.iter().enumerate() {
            let v = if j >= 4 - pad { 0 } else { val(*c)? };
            n = n << 6 | v;
        }
        let bytes = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        out.extend_from_slice(&bytes[..3 - pad]);
    }
    Some(out)
}

#[cfg(test)]
mod live_tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        for data in [&b""[..], b"a", b"ab", b"abc", b"hello\nworld\r\n\x00\xff"] {
            let line = live_frame(2, 42, data);
            assert!(line.ends_with('\n') && !line[..line.len() - 1].contains('\n'));
            assert_eq!(
                parse_live_frame(&line.replace('\n', "\r\n")),
                Some((2, 42, data.to_vec()))
            );
        }
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
    }

    #[test]
    fn damaged_frames_are_refused() {
        let line = live_frame(1, 0, b"some output");
        assert!(parse_live_frame("[    1.234] random kernel line").is_none());
        // A kernel message landed inside the line.
        let mut bad = line.clone();
        bad.insert_str(20, "xx");
        assert!(parse_live_frame(&bad).is_none());
        let bad = line.replace(" 1 0 ", " 3 0 ");
        assert!(parse_live_frame(&bad).is_none());
        // Text before the prefix (a partial kernel line) does not matter.
        assert!(parse_live_frame(&format!("junk{line}")).is_some());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_blocks_round_trip() {
        let hash = [7u8; 32];
        let b = task_header(12345, &hash);
        assert_eq!(b.len(), 4096);
        assert_eq!(parse_task_header(&b), Some((12345, hash)));
        assert_eq!(parse_task_header(&control_head(60)), None);
        assert_eq!(control_idle_secs(&control_head(60)), Some(60));
        assert_eq!(control_idle_secs(&[0u8; 4096]), None);
        assert_eq!(parse_task_header(&[0u8; 4096]), None);
        for r in [StopRequest::Term, StopRequest::Kill] {
            assert_eq!(StopRequest::parse(&r.block()), Some(r));
        }
        assert_eq!(StopRequest::parse(&[0u8; 4096]), None);
        assert!(control_head(0).starts_with(CONTROL_MAGIC));
    }

    #[test]
    fn checkpoints_make_room_on_the_output_disk() {
        let plain = RunSpec::default();
        let ckpt = RunSpec {
            checkpoint_limit: 1 << 30,
            ..Default::default()
        };
        assert_eq!(plain.output_disk_bytes(), OUTPUT_DISK_BYTES);
        assert!(ckpt.output_disk_bytes() > OUTPUT_DISK_BYTES + (1 << 30));
        // An older guest's spec has no such field.
        let json = serde_json::to_string(&plain).unwrap();
        assert!(!json.contains("checkpoint"));
    }
}
