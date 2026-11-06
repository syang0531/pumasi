//! Job specification.
//!
//! Users describe a job in a TOML file ([`JobFile`]). The CLI resolves it
//! into a [`JobSubmission`] (module bytes and concrete task inputs) that is
//! sent to the control plane.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::gpu::{GpuRequest, GpuVendor};
use crate::manage::{MAX_PRIORITY, MIN_PRIORITY, Selector, SelectorValue, Taint, Toleration};
use crate::store::{
    MountRequest, MountSource, STORE_PREFIX, StoreRef, expand_shard, validate_dataset_name,
};

/// Default per-task memory limit.
pub const DEFAULT_MEMORY_MB: u64 = 256;
/// Default per-task wall-clock timeout.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;
/// Default number of placements a task gets before it is marked failed
/// because nodes kept disappearing.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Cap on captured stdout/stderr per task.
pub const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
/// Cap on the total size of files a task writes to `/output`.
pub const MAX_OUTPUT_FILES_BYTES: usize = 32 * 1024 * 1024;
/// Cap on all input files of one job (shared plus per-task). Keeps the
/// submission well under the wire frame limit.
pub const MAX_INPUT_FILES_BYTES: usize = 48 * 1024 * 1024;
/// Guest path where input files appear.
pub const INPUT_DIR: &str = "/input";
/// Guest path whose contents are collected after a task.
pub const OUTPUT_DIR: &str = "/output";

/// Identifies a job. Random, assigned by the control plane.
///
/// On the wire and on disk (postcard) it is a number; in human-readable
/// formats such as the dashboard's JSON it is the hex string the CLI
/// shows, since JSON numbers lose precision beyond 2^53.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(pub u128);

impl Serialize for JobId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.collect_str(self)
        } else {
            s.serialize_u128(self.0)
        }
    }
}

impl<'de> Deserialize<'de> for JobId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            u128::deserialize(d).map(JobId)
        }
    }
}

impl JobId {
    pub fn random() -> Self {
        // 64 bits is plenty for a private cluster and keeps ids short.
        Self(rand::random::<u64>() as u128)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for JobId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(Self(
            u128::from_str_radix(s.trim(), 16).context("job id must be hex")?,
        ))
    }
}

/// Confidentiality tier: who could read the job's code and data.
///
/// - `Open`: any node. Sandboxes protect the node from the job, not the
///   job from the node's owner, who can read everything.
/// - `Trusted`: only the cluster owner's machines and providers the admin
///   trusts (`pumasi trust`). Their owners could still read the job;
///   the protection is that you know and trust them.
/// - `Confidential`: microVM jobs only, each task in a VM whose memory the
///   node's owner cannot read (AMD SEV-SNP or Intel TDX), with the image,
///   input and output sealed to keys the control plane releases only after
///   checking the VM's attestation. The node sees the job's name, run time
///   and resource use. Physical attacks on the machine (memory bus
///   interposers) are out of scope.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    #[default]
    Open,
    // Phase 16 from here (appended: postcard numbers variants in order).
    Trusted,
    Confidential,
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Tier::Open => "open",
            Tier::Trusted => "trusted",
            Tier::Confidential => "confidential",
        })
    }
}

impl std::str::FromStr for Tier {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "open" => Ok(Tier::Open),
            "trusted" => Ok(Tier::Trusted),
            "confidential" => Ok(Tier::Confidential),
            other => bail!("unknown tier {other:?} (open, trusted or confidential)"),
        }
    }
}

/// Which executor runs the task.
///
/// - `Wasm`: Wasmtime sandbox, runs on every node, no network.
/// - `Docker`: an OCI image run by the node's Docker (or Podman). Any
///   language, packages and optional network. Only nodes that opted in with
///   `--allow-docker` get these tasks; meant for machines you own.
/// - `Microvm`: the same OCI images, each task in its own lightweight VM
///   (Firecracker, QEMU or Virtualization.framework) with its own kernel.
///   No network unless the job asks for one, and then only to the
///   internet. Safe to send to other people's machines, so providers that
///   opted in with `--allow-microvm` get these tasks.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Executor {
    #[default]
    Wasm,
    Docker,
    Microvm,
}

impl Executor {
    /// Docker and microVM jobs run OCI images and share [`DockerSpec`].
    pub fn runs_images(self) -> bool {
        matches!(self, Executor::Docker | Executor::Microvm)
    }
}

impl fmt::Display for Executor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Executor::Wasm => "wasm",
            Executor::Docker => "docker",
            Executor::Microvm => "microvm",
        })
    }
}

/// A file handed to or produced by a task. `path` is relative, with `/`
/// separators, and never escapes its directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub data: Vec<u8>,
}

/// Checks that a relative path stays inside its directory.
pub fn validate_rel_path(path: &str) -> Result<()> {
    ensure!(!path.is_empty(), "empty file path");
    ensure!(
        !path.starts_with('/') && !path.contains('\\'),
        "file path must be relative: {path}"
    );
    ensure!(
        !path.contains(':'),
        "file path must not contain ':': {path}"
    );
    for part in path.split('/') {
        ensure!(
            !part.is_empty() && part != "." && part != "..",
            "invalid file path: {path}"
        );
    }
    Ok(())
}

/// Writes entries under `dir`, creating parent directories.
pub fn write_files(dir: &Path, files: &[FileEntry]) -> Result<()> {
    for f in files {
        validate_rel_path(&f.path)?;
        let target = dir.join(&f.path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&target, &f.data).with_context(|| format!("write {}", target.display()))?;
    }
    Ok(())
}

/// Reads every regular file under `dir` (recursively) as entries relative
/// to it, sorted by path. Stops at `limit` total bytes and reports whether
/// anything was left out.
pub fn read_files(dir: &Path, limit: usize) -> Result<(Vec<FileEntry>, bool)> {
    let mut out = Vec::new();
    let mut total = 0usize;
    let mut truncated = false;
    let mut stack = vec![dir.to_path_buf()];
    let mut paths = Vec::new();
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries {
            let e = e?;
            let ft = e.file_type()?;
            if ft.is_dir() {
                stack.push(e.path());
            } else if ft.is_file() {
                paths.push(e.path());
            }
        }
    }
    paths.sort();
    for p in paths {
        let rel = p.strip_prefix(dir).expect("under dir");
        let rel: Vec<String> = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        let size = std::fs::metadata(&p)?.len() as usize;
        if total + size > limit {
            truncated = true;
            continue;
        }
        total += size;
        out.push(FileEntry {
            path: rel.join("/"),
            data: std::fs::read(&p)?,
        });
    }
    Ok((out, truncated))
}

/// Resource limits enforced on every task of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum linear memory, in bytes.
    pub memory_bytes: u64,
    /// Maximum fuel (roughly, WASM instructions). `None` means unlimited.
    pub fuel: Option<u64>,
    /// Wall-clock timeout in milliseconds.
    pub timeout_ms: u64,
    /// CPU share in thousandths of a core (Docker only). `None` = no cap.
    pub cpu_millis: Option<u32>,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: DEFAULT_MEMORY_MB * 1024 * 1024,
            fuel: None,
            timeout_ms: DEFAULT_TIMEOUT_SECS * 1000,
            cpu_millis: None,
        }
    }
}

/// Concrete input of a single task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInput {
    /// argv[1..]; argv[0] is the job name.
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub stdin: Vec<u8>,
    /// Files placed in `/input` for this task only (in addition to the
    /// job's shared files).
    pub files: Vec<FileEntry>,
}

/// Settings of an image job (the docker and microvm executors).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerSpec {
    pub image: String,
    /// Replaces the image's default command. Task args are appended.
    pub command: Vec<String>,
    /// Allow outbound network access (default: none). Docker: the bridge
    /// network. MicroVM: a network device that reaches only the internet
    /// (never the host, its LAN or other private addresses).
    pub network: bool,
    /// Jobs from before phase 9 only: passed to `docker run --gpus`.
    /// Submissions now carry a [`GpuRequest`] instead.
    pub gpus: Option<String>,
}

/// What a Docker GPU task may use of an NVIDIA GPU besides its memory
/// (`gpu = { capabilities = ["compute", "graphics"] }`): the NVIDIA
/// Container Toolkit's driver capabilities. Without any, a task gets
/// `compute` and `utility` (CUDA and `nvidia-smi`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GpuCapability {
    /// CUDA and OpenCL.
    Compute,
    /// `nvidia-smi` and NVML.
    Utility,
    /// OpenGL, EGL and Vulkan (Blender EEVEE, OptiX denoising, games).
    /// Only nodes whose Docker GPU check found graphics (label
    /// `gpu.graphics=true`) get these tasks.
    Graphics,
    /// NVENC and NVDEC video encoding and decoding.
    Video,
}

impl GpuCapability {
    pub const ALL: [GpuCapability; 4] = [
        GpuCapability::Compute,
        GpuCapability::Utility,
        GpuCapability::Graphics,
        GpuCapability::Video,
    ];

    /// `NVIDIA_DRIVER_CAPABILITIES` for a task that asked for `caps`:
    /// always with `compute` and `utility`.
    pub fn driver_env(caps: &[GpuCapability]) -> String {
        let mut all: Vec<GpuCapability> = vec![GpuCapability::Compute, GpuCapability::Utility];
        for c in caps {
            if !all.contains(c) {
                all.push(*c);
            }
        }
        all.sort();
        all.iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl fmt::Display for GpuCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            GpuCapability::Compute => "compute",
            GpuCapability::Utility => "utility",
            GpuCapability::Graphics => "graphics",
            GpuCapability::Video => "video",
        })
    }
}

impl std::str::FromStr for GpuCapability {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|c| c.to_string() == s.trim().to_ascii_lowercase())
            .with_context(|| {
                format!("unknown GPU capability {s:?} (compute, utility, graphics or video)")
            })
    }
}

/// What the CLI sends to the control plane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSubmission {
    pub name: String,
    pub executor: Executor,
    /// WASM module (wasm executor only).
    pub module: Vec<u8>,
    /// Image settings (docker and microvm executors).
    pub docker: Option<DockerSpec>,
    /// Files placed in every task's `/input`.
    pub files: Vec<FileEntry>,
    pub tasks: Vec<TaskInput>,
    pub limits: Limits,
    pub tier: Tier,
    pub max_attempts: u32,
    /// Recorded for the future marketplace; ignored in a private cluster.
    pub max_price_per_hour: Option<f64>,
    /// Account that pays for runs on other people's machines. `None` means
    /// the cluster owner.
    pub account: Option<String>,
    /// WASM only: give every task a virtual clock and seeded random numbers
    /// so any honest node computes the same result. Needed for checking
    /// provider results by re-running them.
    pub deterministic: bool,
    /// Datasets (or uploaded collections) in every task's `/input`.
    pub mounts: Vec<MountRequest>,
    /// Each task's own mounts: empty, or one list per task.
    pub task_mounts: Vec<Vec<MountRequest>>,
    /// Write every task's `/output` to the store and, when the job ends,
    /// make a new version of this dataset from them (`task-<i>/...`).
    pub output_dataset: Option<String>,
    /// The GPUs each task needs (docker and microvm jobs).
    pub gpu: Option<GpuRequest>,
    // Phase 10 from here.
    /// Higher runs first and may preempt lower ([`MIN_PRIORITY`] to
    /// [`MAX_PRIORITY`], default 0).
    pub priority: i32,
    /// Only nodes whose labels match run the job.
    pub selector: Selector,
    /// Taints of nodes the job may run on anyway.
    pub tolerations: Vec<Toleration>,
    /// Jobs that must succeed before this one starts.
    pub after: Vec<JobId>,
    // Phase 15 from here.
    /// A task that no node has taken this long after the job could start
    /// fails, with the reason no node took it (instead of waiting in the
    /// queue for as long as it takes).
    pub queue_timeout_ms: Option<u64>,
    /// Driver capabilities its Docker GPU tasks need beyond `compute` and
    /// `utility` (NVIDIA).
    pub gpu_capabilities: Vec<GpuCapability>,
}

impl JobSubmission {
    /// Whether its tasks need a node whose Docker GPUs can draw.
    pub fn needs_gpu_graphics(&self) -> bool {
        self.gpu_capabilities.contains(&GpuCapability::Graphics)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.tasks.is_empty(), "job has no tasks");
        ensure!(self.tasks.len() <= 100_000, "too many tasks (max 100000)");
        match self.executor {
            Executor::Wasm => {
                ensure!(!self.module.is_empty(), "module is empty");
                ensure!(
                    self.module.starts_with(b"\0asm"),
                    "module is not a WebAssembly binary"
                );
            }
            Executor::Docker | Executor::Microvm => {
                let kind = self.executor;
                let d = self
                    .docker
                    .as_ref()
                    .with_context(|| format!("{kind} job needs an image"))?;
                ensure!(!d.image.trim().is_empty(), "{kind} job needs an image");
                ensure!(
                    !d.image.starts_with('-') && !d.image.contains(char::is_whitespace),
                    "invalid image name"
                );
                ensure!(
                    d.gpus.is_none(),
                    "ask for GPUs with a GPU request, not docker's --gpus"
                );
            }
        }
        if self.tier == Tier::Confidential {
            ensure!(
                self.executor == Executor::Microvm,
                "confidential jobs run in microVMs: set executor = \"microvm\""
            );
            ensure!(
                self.gpu.is_none(),
                "confidential jobs cannot have GPUs yet (that needs NVIDIA confidential computing on H100 or later)"
            );
            ensure!(
                !self.docker.as_ref().is_some_and(|d| d.network),
                "confidential jobs have no network: the node would carry the traffic"
            );
        }
        if let Some(g) = &self.gpu {
            g.validate()?;
            if self.executor == Executor::Wasm {
                g.validate_webgpu()?;
                ensure!(
                    !self.deterministic,
                    "GPU results differ from GPU to GPU, so a wasm job with a GPU cannot be deterministic"
                );
            }
        }
        for f in self
            .files
            .iter()
            .chain(self.tasks.iter().flat_map(|t| t.files.iter()))
        {
            validate_rel_path(&f.path)?;
        }
        ensure!(
            self.task_mounts.is_empty() || self.task_mounts.len() == self.tasks.len(),
            "task mounts do not match the tasks"
        );
        for m in self.mounts.iter().chain(self.task_mounts.iter().flatten()) {
            if let MountSource::Dataset { name, .. } = &m.source {
                validate_dataset_name(name)?;
            }
            if let Some(sel) = &m.select {
                validate_rel_path(sel)?;
            }
            if !m.at.is_empty() {
                validate_rel_path(&m.at)?;
            }
        }
        if let Some(name) = &self.output_dataset {
            validate_dataset_name(name)?;
        }
        // The whole submission travels in one frame, so cap all files together.
        let total: usize = self
            .files
            .iter()
            .chain(self.tasks.iter().flat_map(|t| t.files.iter()))
            .map(|f| f.data.len())
            .sum();
        ensure!(
            total <= MAX_INPUT_FILES_BYTES,
            "input files total {} MiB, the limit is {} MiB",
            total >> 20,
            MAX_INPUT_FILES_BYTES >> 20
        );
        ensure!(self.max_attempts >= 1, "max_attempts must be at least 1");
        ensure!(
            !self.deterministic || self.executor == Executor::Wasm,
            "deterministic mode is only for wasm jobs"
        );
        if let Some(a) = &self.account {
            crate::economy::validate_account(a)?;
        }
        ensure!(
            (MIN_PRIORITY..=MAX_PRIORITY).contains(&self.priority),
            "priority must be between {MIN_PRIORITY} and {MAX_PRIORITY}"
        );
        self.selector.validate()?;
        ensure!(self.tolerations.len() <= 32, "at most 32 tolerations");
        for t in &self.tolerations {
            t.validate()?;
        }
        ensure!(self.after.len() <= 64, "a job waits for at most 64 jobs");
        if let Some(t) = self.queue_timeout_ms {
            ensure!(t >= 1000, "queue_timeout must be at least 1s");
        }
        if !self.gpu_capabilities.is_empty() {
            ensure!(
                self.executor == Executor::Docker,
                "GPU capabilities are for docker jobs (a microVM gets the whole GPU)"
            );
            let g = self
                .gpu
                .as_ref()
                .context("GPU capabilities need a GPU request (`gpus = 1`)")?;
            ensure!(
                g.vendor.is_none_or(|v| v == GpuVendor::Nvidia),
                "GPU capabilities are for NVIDIA GPUs"
            );
        }
        ensure!(self.limits.timeout_ms > 0, "timeout must be positive");
        ensure!(
            self.limits.memory_bytes >= 64 * 1024,
            "memory limit must be at least 64 KiB"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TOML job file
// ---------------------------------------------------------------------------

/// The `job.toml` users write.
///
/// WASM job:
///
/// ```toml
/// name = "estimate-pi"
/// module = "montecarlo.wasm"
/// shards = 8
/// args = ["sample", "{shard}", "2000000"]
/// ```
///
/// Docker job:
///
/// ```toml
/// image = "python:3.12-slim"
/// command = ["python", "count.py", "{shard}", "{shards}"]
/// files = ["count.py", "texts/"]
/// shards = 4
/// network = false
/// ```
///
/// The same job in a microVM (on any node that allows microVMs, including
/// provider machines) only adds `executor = "microvm"`.
///
/// Either `shards` (N copies of `args`/`command`, with `{shard}` /
/// `{shards}` substituted) or an explicit `[[task]]` list may be given. With
/// neither, the job has a single task.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFile {
    pub name: Option<String>,
    /// `store:NAME`: write every task's `/output` to the file store and
    /// make a new version of dataset NAME from them when the job ends.
    pub output: Option<String>,
    /// WASM module path (wasm executor).
    pub module: Option<PathBuf>,
    /// OCI image (docker and microvm executors).
    pub image: Option<String>,
    /// Overrides the image's default command (docker and microvm).
    #[serde(default)]
    pub command: Vec<String>,
    /// Defaults to `docker` when `image` is set, else `wasm`. Set
    /// `microvm` to run the image in a VM.
    pub executor: Option<Executor>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Path to a file fed to every task's stdin.
    pub stdin: Option<PathBuf>,
    /// Files and directories copied into every task's `/input`, and
    /// datasets from the file store (`store:NAME[@VERSION][/PATH]`, placed
    /// under `/input/NAME/`; `{shard}` in it picks one per task).
    #[serde(default)]
    pub files: Vec<PathBuf>,
    /// Allow network access (docker and microvm executors). MicroVMs reach
    /// only the internet, never private addresses.
    #[serde(default)]
    pub network: bool,
    /// GPUs each task needs (docker and microvm executors): a number, or
    /// "all" for every GPU of the node.
    pub gpus: Option<GpusField>,
    /// Which GPUs: `{ vendor = "nvidia", min_vram_gb = 16 }`. Implies
    /// `gpus = 1` when `gpus` is not given.
    pub gpu: Option<GpuFile>,
    pub shards: Option<u32>,
    #[serde(default, rename = "task")]
    pub tasks: Vec<TaskFile>,
    #[serde(default)]
    pub resources: ResourcesFile,
    #[serde(default)]
    pub tier: Tier,
    pub max_attempts: Option<u32>,
    pub max_price_per_hour: Option<f64>,
    /// Deterministic mode (wasm only, default on unless the job asks for a
    /// GPU): a virtual clock and seeded random numbers, so results can be
    /// checked by re-running.
    pub deterministic: Option<bool>,
    /// Higher runs first and may preempt lower (-1000 to 1000, default 0).
    pub priority: Option<i32>,
    /// Labels a node must have: `os = "linux"`, `disk = ["nvme", "ssd"]`,
    /// `gpu = "!amd"`, `zone = true`.
    #[serde(default)]
    pub selector: BTreeMap<String, SelectorValue>,
    /// Node taints the job accepts: `"gpu-only"`, `"owner=alice"`.
    #[serde(default)]
    pub tolerations: Vec<String>,
    /// Ids of jobs that must succeed first.
    #[serde(default)]
    pub after: Vec<String>,
    /// Fail a task no node took within this long (`"2m"`), with the
    /// reason, instead of leaving it in the queue.
    pub queue_timeout: Option<String>,
}

/// `gpus = 2` or `gpus = "all"`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum GpusField {
    Count(u32),
    Text(String),
}

impl GpusField {
    /// GPUs per task, 0 for all of them.
    pub fn count(&self) -> Result<u32> {
        match self {
            GpusField::Count(n) => {
                ensure!(*n >= 1, "`gpus` must be at least 1 (or \"all\")");
                Ok(*n)
            }
            GpusField::Text(t) if t.trim() == "all" => Ok(0),
            GpusField::Text(t) => match t.trim().parse::<u32>() {
                Ok(n) if n >= 1 => Ok(n),
                _ => bail!(
                    "`gpus` is a number of GPUs or \"all\", not {t:?} (the cluster picks the devices)"
                ),
            },
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GpuFile {
    pub vendor: Option<String>,
    /// Smallest GPU memory, in GB (GiB).
    pub min_vram_gb: Option<f64>,
    /// Driver capabilities beyond `compute` and `utility`: `graphics`
    /// (OpenGL, EGL, Vulkan) and `video` (NVENC, NVDEC). Docker, NVIDIA.
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFile {
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub stdin: Option<PathBuf>,
    /// Extra files and directories for this task's `/input`.
    #[serde(default)]
    pub files: Vec<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcesFile {
    pub memory_mb: Option<u64>,
    pub fuel: Option<u64>,
    /// Human duration such as "90s", "5m".
    pub timeout: Option<String>,
    /// CPU cores, fractional allowed (docker executor only).
    pub cpus: Option<f64>,
}

/// Collects files and directories (recursively) relative to `base`. A file
/// `data/a.csv` keeps that path; a directory `data/` contributes every file
/// under it with its path relative to `base`. `store:` entries are not
/// files and are skipped (see [`store_refs`]).
pub fn collect_files(base: &Path, paths: &[PathBuf]) -> Result<Vec<FileEntry>> {
    collect_paths(base, paths)?
        .into_iter()
        .map(|(path, full, _)| {
            let data = std::fs::read(&full).with_context(|| format!("read {}", full.display()))?;
            Ok(FileEntry { path, data })
        })
        .collect()
}

/// Like [`collect_files`], but only lists the files: their path in
/// `/input`, where they are, and their size.
pub fn collect_paths(base: &Path, paths: &[PathBuf]) -> Result<Vec<(String, PathBuf, u64)>> {
    let mut out: BTreeMap<String, (PathBuf, u64)> = BTreeMap::new();
    for p in paths.iter().filter(|p| store_ref_of(p).is_none()) {
        let full = base.join(p);
        let meta = std::fs::metadata(&full).with_context(|| format!("read {}", full.display()))?;
        let root_rel = rel_string(p)?;
        if meta.is_dir() {
            let mut stack = vec![full.clone()];
            while let Some(d) = stack.pop() {
                for e in std::fs::read_dir(&d).with_context(|| format!("read {}", d.display()))? {
                    let e = e?;
                    let ft = e.file_type()?;
                    if ft.is_dir() {
                        stack.push(e.path());
                    } else if ft.is_file() {
                        let rel = e.path();
                        let rel = rel.strip_prefix(&full).expect("under dir");
                        let rel: Vec<String> = rel
                            .components()
                            .map(|c| c.as_os_str().to_string_lossy().into_owned())
                            .collect();
                        let rel = rel.join("/");
                        let path = if root_rel.is_empty() {
                            rel
                        } else {
                            format!("{root_rel}/{rel}")
                        };
                        out.insert(path, (e.path(), e.metadata()?.len()));
                    }
                }
            }
        } else {
            ensure!(!root_rel.is_empty(), "invalid file path {}", p.display());
            out.insert(root_rel, (full, meta.len()));
        }
    }
    let files: Vec<(String, PathBuf, u64)> =
        out.into_iter().map(|(p, (full, n))| (p, full, n)).collect();
    for (p, _, _) in &files {
        validate_rel_path(p)?;
    }
    Ok(files)
}

/// Every regular file under `dir`: its path relative to `dir`, where it
/// is, and its size.
pub fn dir_files(dir: &Path) -> Result<Vec<(String, PathBuf, u64)>> {
    collect_paths(dir, &[PathBuf::from(".")])
}

/// The `store:` reference a `files` entry stands for, if it is one.
fn store_ref_of(p: &Path) -> Option<&str> {
    p.to_str().filter(|s| s.starts_with(STORE_PREFIX))
}

/// The `store:` references among `files` entries, as written.
pub fn store_refs(paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .filter_map(|p| store_ref_of(p).map(String::from))
        .collect()
}

/// Mounts for `store:` references, after expanding shard placeholders.
fn mounts_for(refs: &[String], shard: Option<(u32, u32)>) -> Result<Vec<MountRequest>> {
    refs.iter()
        .map(|r| {
            let r = match shard {
                Some((i, n)) => expand_shard(r, i, n),
                None => r.clone(),
            };
            let parsed: StoreRef = r.parse().with_context(|| format!("bad file entry {r:?}"))?;
            Ok(MountRequest::from_ref(&parsed))
        })
        .collect()
}

/// Turns a user-given relative path into `a/b/c` form.
fn rel_string(p: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for c in p.components() {
        match c {
            std::path::Component::Normal(s) => parts.push(s.to_string_lossy().into_owned()),
            std::path::Component::CurDir => {}
            _ => bail!(
                "file paths in a job must be relative and stay inside its folder: {}",
                p.display()
            ),
        }
    }
    Ok(parts.join("/"))
}

impl JobFile {
    pub fn parse(text: &str) -> Result<Self> {
        toml::from_str(text).context("invalid job file")
    }

    /// Loads the file, reads the module, stdin and input files relative to
    /// it, and expands shards into concrete tasks.
    pub fn load(path: &Path) -> Result<JobSubmission> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let file = Self::parse(&text)?;
        let base = path.parent().unwrap_or(Path::new("."));
        file.resolve(base)
    }

    pub fn resolve(self, base: &Path) -> Result<JobSubmission> {
        Ok(self.resolve_with(base, None)?.0)
    }

    /// Like [`Self::load`], but when the shared `files` add up to more than
    /// `inline_limit` bytes they are not read: they come back as (path in
    /// `/input`, file) for uploading to the file store, and the
    /// submission's `files` is empty.
    pub fn load_split(
        path: &Path,
        inline_limit: u64,
    ) -> Result<(JobSubmission, Vec<(String, PathBuf)>)> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let file = Self::parse(&text)?;
        let base = path.parent().unwrap_or(Path::new("."));
        file.resolve_with(base, Some(inline_limit))
    }

    /// Like [`Self::load_split`], for a job file someone else wrote (the
    /// dashboard's uploads): every local path (`module`, `stdin`, `files`
    /// and the tasks' own) must stay inside the job's folder. `pumasi run`
    /// reads whatever its own user names, `../build/m.wasm` included; a
    /// server reading a stranger's job file must not, or `stdin =
    /// "/home/admin/.pumasi/admin.ticket"` hands that file to the job.
    pub fn load_split_confined(
        path: &Path,
        inline_limit: u64,
    ) -> Result<(JobSubmission, Vec<(String, PathBuf)>)> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let file = Self::parse(&text)?;
        file.check_confined()?;
        let base = path.parent().unwrap_or(Path::new("."));
        file.resolve_with(base, Some(inline_limit))
    }

    /// Refuses local paths that are absolute or climb out with `..`.
    pub fn check_confined(&self) -> Result<()> {
        let local = |ps: &'_ [PathBuf]| -> Vec<PathBuf> {
            ps.iter()
                .filter(|p| store_ref_of(p).is_none())
                .cloned()
                .collect()
        };
        let mut paths: Vec<PathBuf> = Vec::new();
        paths.extend(self.module.iter().cloned());
        paths.extend(self.stdin.iter().cloned());
        paths.extend(local(&self.files));
        for t in &self.tasks {
            paths.extend(t.stdin.iter().cloned());
            paths.extend(local(&t.files));
        }
        for p in &paths {
            rel_string(p)?;
            // `C:x` and `\\server\x` are prefixes on Windows only; refuse
            // them everywhere so the check does not depend on the host.
            let s = p.to_string_lossy();
            ensure!(
                !s.contains(':') && !s.starts_with('\\'),
                "file paths in a job must be relative and stay inside its folder: {s}"
            );
        }
        Ok(())
    }

    fn resolve_with(
        self,
        base: &Path,
        inline_limit: Option<u64>,
    ) -> Result<(JobSubmission, Vec<(String, PathBuf)>)> {
        let read = |p: &Path| -> Result<Vec<u8>> {
            let full = base.join(p);
            std::fs::read(&full).with_context(|| format!("read {}", full.display()))
        };
        let executor = self.executor.unwrap_or(if self.image.is_some() {
            Executor::Docker
        } else {
            Executor::Wasm
        });
        let (module, docker_base) = match executor {
            Executor::Wasm => {
                ensure!(
                    self.image.is_none(),
                    "`image` is only for docker and microvm jobs"
                );
                ensure!(
                    self.command.is_empty(),
                    "`command` is only for docker and microvm jobs; use `args`"
                );
                ensure!(
                    !self.network,
                    "wasm tasks cannot use the network; use a docker or microvm job"
                );
                let m = self.module.as_ref().context("wasm job needs `module`")?;
                (read(m)?, None)
            }
            Executor::Docker | Executor::Microvm => {
                ensure!(self.module.is_none(), "`module` is only for wasm jobs");
                let image = self
                    .image
                    .clone()
                    .with_context(|| format!("{executor} job needs `image`"))?;
                (Vec::new(), Some(image))
            }
        };
        let shared_stdin = match &self.stdin {
            Some(p) => read(p)?,
            None => Vec::new(),
        };
        let listed = collect_paths(base, &self.files)?;
        let total: u64 = listed.iter().map(|(_, _, n)| n).sum();
        let (files, upload) = match inline_limit {
            Some(limit) if total > limit => (
                Vec::new(),
                listed.into_iter().map(|(p, f, _)| (p, f)).collect(),
            ),
            _ => (collect_files(base, &self.files)?, Vec::new()),
        };
        let refs = store_refs(&self.files);
        // `{shard}` in a reference picks something per task.
        let (shared_refs, shard_refs): (Vec<String>, Vec<String>) =
            refs.into_iter().partition(|r| !r.contains("{shard"));
        let mounts = mounts_for(&shared_refs, None)?;
        let mut task_mounts: Vec<Vec<MountRequest>> = Vec::new();

        let mut command = self.command.clone();
        let tasks = if !self.tasks.is_empty() {
            if self.shards.is_some() {
                bail!("use either `shards` or `[[task]]`, not both");
            }
            ensure!(
                shard_refs.is_empty(),
                "`{{shard}}` in `files` needs `shards`; give each `[[task]]` its own `files`"
            );
            task_mounts = self
                .tasks
                .iter()
                .map(|t| mounts_for(&store_refs(&t.files), None))
                .collect::<Result<_>>()?;
            if task_mounts.iter().all(|m| m.is_empty()) {
                task_mounts.clear();
            }
            self.tasks
                .iter()
                .map(|t| {
                    let mut env = self.env.clone();
                    env.extend(t.env.clone());
                    Ok(TaskInput {
                        args: if t.args.is_empty() {
                            self.args.clone()
                        } else {
                            t.args.clone()
                        },
                        env,
                        stdin: match &t.stdin {
                            Some(p) => read(p)?,
                            None => shared_stdin.clone(),
                        },
                        files: collect_files(base, &t.files)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            let n = self.shards.unwrap_or(1);
            ensure!(n >= 1, "shards must be at least 1");
            if !shard_refs.is_empty() {
                task_mounts = (0..n)
                    .map(|i| mounts_for(&shard_refs, Some((i, n))))
                    .collect::<Result<_>>()?;
            }
            // `{shard}` in the command is per task, so move the command into
            // the task args when it uses placeholders.
            let templated = command.iter().any(|c| c.contains("{shard"));
            let cmd_template = if templated {
                std::mem::take(&mut command)
            } else {
                Vec::new()
            };
            (0..n)
                .map(|i| TaskInput {
                    args: cmd_template
                        .iter()
                        .chain(self.args.iter())
                        .map(|a| expand(a, i, n))
                        .collect(),
                    env: self
                        .env
                        .iter()
                        .map(|(k, v)| (k.clone(), expand(v, i, n)))
                        .collect(),
                    stdin: shared_stdin.clone(),
                    files: Vec::new(),
                })
                .collect()
        };
        // An explicit command with `[[task]]`s keeps placeholders literal.

        let mut limits = Limits::default();
        if let Some(mb) = self.resources.memory_mb {
            limits.memory_bytes = mb * 1024 * 1024;
        }
        limits.fuel = self.resources.fuel;
        if let Some(t) = &self.resources.timeout {
            limits.timeout_ms = parse_duration_ms(t)?;
        }
        if let Some(c) = self.resources.cpus {
            ensure!(
                c > 0.0 && c <= 1024.0,
                "resources.cpus must be between 0 and 1024"
            );
            limits.cpu_millis = Some((c * 1000.0).round() as u32);
        }

        let name = self.name.clone().unwrap_or_else(|| {
            if let Some(m) = &self.module
                && let Some(stem) = m.file_stem()
            {
                return stem.to_string_lossy().into_owned();
            }
            if let Some(img) = &docker_base {
                let last = img.rsplit('/').next().unwrap_or(img);
                return last.split([':', '@']).next().unwrap_or(last).to_string();
            }
            "job".into()
        });
        let docker = docker_base.map(|image| DockerSpec {
            image,
            command,
            network: self.network,
            gpus: None,
        });
        let gpu = match (&self.gpus, &self.gpu) {
            (None, None) => None,
            (count, which) => {
                let which = which.clone().unwrap_or_default();
                let min_vram_mib = match which.min_vram_gb {
                    Some(gb) => {
                        ensure!(
                            (0.0..=1024.0).contains(&gb),
                            "gpu.min_vram_gb must be between 0 and 1024"
                        );
                        (gb * 1024.0).round() as u64
                    }
                    None => 0,
                };
                Some(GpuRequest {
                    count: match count {
                        Some(c) => c.count()?,
                        None => 1,
                    },
                    vendor: which
                        .vendor
                        .as_deref()
                        .filter(|v| *v != "any")
                        .map(str::parse::<GpuVendor>)
                        .transpose()?,
                    min_vram_mib,
                })
            }
        };
        let gpu_capabilities = {
            let mut caps = self
                .gpu
                .as_ref()
                .map(|g| {
                    g.capabilities
                        .iter()
                        .map(|c| c.parse::<GpuCapability>())
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();
            // compute and utility are always there.
            caps.retain(|c| !matches!(c, GpuCapability::Compute | GpuCapability::Utility));
            caps.sort();
            caps.dedup();
            caps
        };
        let queue_timeout_ms = self
            .queue_timeout
            .as_deref()
            .map(parse_duration_ms)
            .transpose()
            .context("queue_timeout")?;
        let wants_gpu = gpu.is_some();
        let output_dataset = match &self.output {
            Some(o) => {
                let name = o
                    .strip_prefix(STORE_PREFIX)
                    .context("`output` must be `store:NAME`")?;
                validate_dataset_name(name)?;
                Some(name.to_string())
            }
            None => None,
        };
        let sub = JobSubmission {
            queue_timeout_ms,
            gpu_capabilities,
            priority: self.priority.unwrap_or(0),
            selector: Selector::from_table(&self.selector)?,
            tolerations: self
                .tolerations
                .iter()
                .map(|t| t.parse::<Taint>())
                .collect::<Result<_>>()?,
            after: self
                .after
                .iter()
                .map(|a| a.parse::<JobId>())
                .collect::<Result<_>>()?,
            gpu,
            mounts,
            task_mounts,
            output_dataset,
            name,
            executor,
            module,
            docker,
            files,
            tasks,
            limits,
            tier: self.tier,
            max_attempts: self.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS),
            max_price_per_hour: self.max_price_per_hour,
            account: None,
            deterministic: match executor {
                // GPU results are not bit-identical across GPUs.
                Executor::Wasm => self.deterministic.unwrap_or(!wants_gpu),
                Executor::Docker | Executor::Microvm => {
                    ensure!(
                        self.deterministic != Some(true),
                        "deterministic mode is only for wasm jobs"
                    );
                    false
                }
            },
        };
        sub.validate()?;
        Ok((sub, upload))
    }
}

fn expand(s: &str, shard: u32, shards: u32) -> String {
    expand_shard(s, shard, shards)
}

/// Parses "250ms", "90s", "5m", "1h", "7d" or a bare number of seconds.
pub fn parse_duration_ms(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 86_400_000)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (s, 1000)
    };
    let n: u64 = num
        .trim()
        .parse()
        .with_context(|| format!("invalid duration {s:?}"))?;
    n.checked_mul(mult)
        .with_context(|| format!("duration {s:?} is too long"))
}

/// `90s`, `5m`, `1h30m`, `2d`: the shortest of [`parse_duration_ms`]'s
/// forms that is exact (milliseconds only when needed).
pub fn format_duration_ms(ms: u64) -> String {
    if ms == 0 {
        return "0s".into();
    }
    if !ms.is_multiple_of(1000) {
        return format!("{ms}ms");
    }
    let mut secs = ms / 1000;
    let mut out = String::new();
    for (unit, len) in [("d", 86_400), ("h", 3600), ("m", 60), ("s", 1)] {
        if secs >= len {
            out.push_str(&format!("{}{unit}", secs / len));
            secs %= len;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const WASM_HEADER: &[u8] = b"\0asm\x01\0\0\0";

    fn write_module(dir: &Path) {
        std::fs::write(dir.join("m.wasm"), WASM_HEADER).unwrap();
    }

    #[test]
    fn job_ids_are_numbers_in_postcard_and_hex_in_json() {
        let id = JobId(0x00ab_cdef_0123_4567);
        // Same bytes as the plain u128 older builds wrote.
        assert_eq!(
            postcard::to_stdvec(&id).unwrap(),
            postcard::to_stdvec(&id.0).unwrap()
        );
        let back: JobId = postcard::from_bytes(&postcard::to_stdvec(&id).unwrap()).unwrap();
        assert_eq!(back, id);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"00abcdef01234567\"");
        assert_eq!(serde_json::from_str::<JobId>(&json).unwrap(), id);
        assert!(serde_json::from_str::<JobId>("\"xyz\"").is_err());
    }

    #[test]
    fn shards_expand() {
        let dir = tempdir();
        write_module(&dir);
        let f = JobFile::parse(
            r#"
            module = "m.wasm"
            shards = 3
            args = ["--seed", "{shard}", "of", "{shards}"]
            [resources]
            memory_mb = 32
            timeout = "90s"
            fuel = 1000
            "#,
        )
        .unwrap();
        let sub = f.resolve(&dir).unwrap();
        assert_eq!(sub.name, "m");
        assert_eq!(sub.tasks.len(), 3);
        assert_eq!(sub.tasks[2].args, vec!["--seed", "2", "of", "3"]);
        assert_eq!(sub.limits.memory_bytes, 32 * 1024 * 1024);
        assert_eq!(sub.limits.timeout_ms, 90_000);
        assert_eq!(sub.limits.fuel, Some(1000));
        assert!(sub.deterministic, "wasm jobs are deterministic by default");
        let off = JobFile::parse("module = \"m.wasm\"\ndeterministic = false").unwrap();
        assert!(!off.resolve(&dir).unwrap().deterministic);
        let docker = JobFile::parse("image = \"busybox\"\ndeterministic = true").unwrap();
        assert!(docker.resolve(&dir).is_err());
    }

    #[test]
    fn explicit_tasks() {
        let dir = tempdir();
        write_module(&dir);
        std::fs::write(dir.join("in.txt"), b"data").unwrap();
        let f = JobFile::parse(
            r#"
            name = "x"
            module = "m.wasm"
            env = { A = "1" }
            [[task]]
            args = ["a"]
            [[task]]
            args = ["b"]
            stdin = "in.txt"
            env = { B = "2" }
            "#,
        )
        .unwrap();
        let sub = f.resolve(&dir).unwrap();
        assert_eq!(sub.tasks.len(), 2);
        assert_eq!(sub.tasks[1].stdin, b"data");
        assert_eq!(sub.tasks[1].env.get("A").unwrap(), "1");
        assert_eq!(sub.tasks[1].env.get("B").unwrap(), "2");
    }

    #[test]
    fn rejects_non_wasm() {
        let dir = tempdir();
        std::fs::write(dir.join("m.wasm"), b"not wasm").unwrap();
        let f = JobFile::parse(r#"module = "m.wasm""#).unwrap();
        assert!(f.resolve(&dir).is_err());
    }

    #[test]
    fn docker_job_with_files() {
        let dir = tempdir();
        std::fs::write(dir.join("count.py"), b"print(1)").unwrap();
        std::fs::create_dir_all(dir.join("texts/sub")).unwrap();
        std::fs::write(dir.join("texts/a.txt"), b"a").unwrap();
        std::fs::write(dir.join("texts/sub/b.txt"), b"b").unwrap();
        let f = JobFile::parse(
            r#"
            image = "ghcr.io/acme/py:3.12"
            command = ["python", "count.py", "{shard}", "{shards}"]
            files = ["count.py", "texts/"]
            shards = 2
            network = true
            [resources]
            cpus = 1.5
            "#,
        )
        .unwrap();
        let sub = f.resolve(&dir).unwrap();
        assert_eq!(sub.executor, Executor::Docker);
        assert_eq!(sub.name, "py");
        let d = sub.docker.as_ref().unwrap();
        assert!(d.network);
        assert!(
            d.command.is_empty(),
            "templated command moves into task args"
        );
        assert_eq!(sub.tasks[1].args, vec!["python", "count.py", "1", "2"]);
        let paths: Vec<&str> = sub.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["count.py", "texts/a.txt", "texts/sub/b.txt"]);
        assert_eq!(sub.limits.cpu_millis, Some(1500));
    }

    #[test]
    fn microvm_job_uses_the_docker_fields() {
        let dir = tempdir();
        let f = JobFile::parse(
            r#"
            executor = "microvm"
            image = "python:3.12-slim"
            command = ["python", "-c", "print({shard})"]
            shards = 2
            "#,
        )
        .unwrap();
        let sub = f.resolve(&dir).unwrap();
        assert_eq!(sub.executor, Executor::Microvm);
        assert!(!sub.deterministic);
        assert_eq!(sub.tasks[1].args, vec!["python", "-c", "print(1)"]);
        assert_eq!(sub.docker.unwrap().image, "python:3.12-slim");
        let net =
            JobFile::parse("executor = \"microvm\"\nimage = \"busybox\"\nnetwork = true").unwrap();
        assert!(net.resolve(&dir).unwrap().docker.unwrap().network);
        let gpu =
            JobFile::parse("executor = \"microvm\"\nimage = \"busybox\"\ngpus = \"all\"").unwrap();
        assert_eq!(gpu.resolve(&dir).unwrap().gpu.unwrap().count, 0);
        let none = JobFile::parse("executor = \"microvm\"").unwrap();
        assert!(none.resolve(&dir).is_err(), "needs an image");
    }

    #[test]
    fn gpu_examples_resolve() {
        let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/pytorch-cuda");
        for (file, executor, count) in [
            ("job.toml", Executor::Docker, 1),
            ("job-microvm.toml", Executor::Microvm, 1),
        ] {
            let text = std::fs::read_to_string(examples.join(file)).unwrap();
            let sub = JobFile::parse(&text).unwrap().resolve(&examples).unwrap();
            assert_eq!(sub.executor, executor, "{file}");
            assert_eq!(sub.gpu.as_ref().map(|g| g.count), Some(count), "{file}");
            sub.validate().unwrap();
        }
    }

    #[test]
    fn blender_examples_resolve() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/blender-render");
        for (file, graphics) in [("job-cycles.toml", false), ("job-eevee.toml", true)] {
            let text = std::fs::read_to_string(dir.join(file)).unwrap();
            let sub = JobFile::parse(&text).unwrap().resolve(&dir).unwrap();
            sub.validate().unwrap();
            assert_eq!(sub.needs_gpu_graphics(), graphics, "{file}");
            assert_eq!(sub.queue_timeout_ms, Some(30 * 60_000), "{file}");
            assert_eq!(sub.tasks.len(), 4);
        }
    }

    #[test]
    fn gpu_capabilities_and_queue_timeout() {
        let dir = tempdir();
        let sub = JobFile::parse(
            "image = \"x\"\nqueue_timeout = \"90s\"\n[gpu]\nvendor = \"nvidia\"\ncapabilities = [\"video\", \"compute\", \"graphics\", \"video\"]",
        )
        .unwrap()
        .resolve(&dir)
        .unwrap();
        sub.validate().unwrap();
        assert_eq!(
            sub.gpu_capabilities,
            vec![GpuCapability::Graphics, GpuCapability::Video]
        );
        assert_eq!(sub.queue_timeout_ms, Some(90_000));
        assert_eq!(
            GpuCapability::driver_env(&sub.gpu_capabilities),
            "compute,utility,graphics,video"
        );
        assert_eq!(GpuCapability::driver_env(&[]), "compute,utility");
        for (text, why) in [
            (
                "image = \"x\"\n[gpu]\ncapabilities = [\"opengl\"]",
                "unknown GPU capability",
            ),
            (
                "image = \"x\"\ngpus = 1\n[gpu]\nvendor = \"amd\"\ncapabilities = [\"graphics\"]",
                "NVIDIA",
            ),
            ("image = \"x\"\nqueue_timeout = \"500ms\"", "at least 1s"),
        ] {
            let e = JobFile::parse(text)
                .and_then(|f| f.resolve(&dir))
                .and_then(|s| s.validate());
            let e = e.unwrap_err();
            assert!(format!("{e:#}").contains(why), "{text}: {e:#}");
        }
    }

    #[test]
    fn durations_format_back() {
        for (ms, s) in [
            (0, "0s"),
            (1500, "1500ms"),
            (90_000, "1m30s"),
            (5_400_000, "1h30m"),
            (172_800_000, "2d"),
        ] {
            assert_eq!(format_duration_ms(ms), s);
            assert_eq!(parse_duration_ms(s).unwrap_or(ms), ms, "{s}");
        }
    }

    #[test]
    fn gpu_requests() {
        let dir = tempdir();
        let two = JobFile::parse("image = \"pytorch/pytorch\"\ngpus = 2").unwrap();
        let sub = two.resolve(&dir).unwrap();
        assert_eq!(
            sub.gpu,
            Some(GpuRequest {
                count: 2,
                vendor: None,
                min_vram_mib: 0
            })
        );
        assert_eq!(sub.docker.unwrap().gpus, None);
        let picky = JobFile::parse(
            "image = \"pytorch/pytorch\"\n[gpu]\nvendor = \"nvidia\"\nmin_vram_gb = 15.5",
        )
        .unwrap();
        assert_eq!(
            picky.resolve(&dir).unwrap().gpu,
            Some(GpuRequest {
                count: 1,
                vendor: Some(GpuVendor::Nvidia),
                min_vram_mib: 15_872
            })
        );
        let any =
            JobFile::parse("image = \"x\"\ngpus = \"all\"\ngpu = { vendor = \"any\" }").unwrap();
        assert_eq!(any.resolve(&dir).unwrap().gpu.unwrap().vendor, None);
        for bad in [
            "image = \"x\"\ngpus = 0",
            "image = \"x\"\ngpus = \"device=0\"",
            "image = \"x\"\ngpus = 17",
            "image = \"x\"\ngpu = { vendor = \"matrox\" }",
            "image = \"x\"\ngpu = { min_vram_gb = -1 }",
        ] {
            assert!(JobFile::parse(bad).unwrap().resolve(&dir).is_err(), "{bad}");
        }
        write_module(&dir);
        // A wasm job gets one GPU through WebGPU.
        let wasm = JobFile::parse("module = \"m.wasm\"\ngpus = 1\ngpu = { vendor = \"intel\" }")
            .unwrap()
            .resolve(&dir)
            .unwrap();
        assert_eq!(wasm.gpu.as_ref().map(|g| g.count), Some(1));
        wasm.validate().unwrap();
        for bad in [
            "module = \"m.wasm\"\ngpus = 2",
            "module = \"m.wasm\"\ngpus = \"all\"",
            "module = \"m.wasm\"\ngpus = 1\ngpu = { min_vram_gb = 4 }",
            "module = \"m.wasm\"\ngpus = 1\ndeterministic = true",
        ] {
            let resolved = JobFile::parse(bad).unwrap().resolve(&dir);
            assert!(resolved.is_err(), "{bad}");
        }
    }

    #[test]
    fn docker_command_without_placeholders_is_kept() {
        let dir = tempdir();
        let f = JobFile::parse(
            r#"
            image = "busybox"
            command = ["sh", "-c"]
            [[task]]
            args = ["echo one"]
            [[task]]
            args = ["echo two"]
            "#,
        )
        .unwrap();
        let sub = f.resolve(&dir).unwrap();
        assert_eq!(sub.docker.unwrap().command, vec!["sh", "-c"]);
        assert_eq!(sub.tasks[1].args, vec!["echo two"]);
    }

    #[test]
    fn rejects_mixed_and_escaping_specs() {
        let dir = tempdir();
        write_module(&dir);
        let net = JobFile::parse("module = \"m.wasm\"\nnetwork = true").unwrap();
        assert!(net.resolve(&dir).is_err(), "wasm cannot have network");
        let up = JobFile::parse("image = \"busybox\"\nfiles = [\"../secret\"]").unwrap();
        assert!(
            up.resolve(&dir).is_err(),
            "files must stay inside the job folder"
        );
        let none = JobFile::parse("name = \"x\"").unwrap();
        assert!(none.resolve(&dir).is_err(), "needs module or image");
    }

    #[test]
    fn confined_job_files_read_only_their_folder() {
        let dir = tempdir();
        write_module(&dir);
        std::fs::write(dir.join("in.txt"), "x").unwrap();
        let outside = tempdir().join("secret");
        std::fs::write(&outside, "admin ticket").unwrap();
        let job = dir.join("job.toml");
        let load = |text: String| {
            std::fs::write(&job, text).unwrap();
            JobFile::load_split_confined(&job, 1 << 20)
        };
        load(
            "module = \"m.wasm\"\nstdin = \"in.txt\"\nfiles = [\"in.txt\", \"store:data\"]".into(),
        )
        .unwrap();
        let o = outside.display().to_string().replace('\\', "/");
        for bad in [
            format!("module = \"{o}\""),
            format!("module = \"m.wasm\"\nstdin = \"{o}\""),
            "module = \"m.wasm\"\nstdin = \"../secret\"".to_string(),
            format!("image = \"busybox\"\nfiles = [\"{o}\"]"),
            format!("module = \"m.wasm\"\n[[task]]\nstdin = \"{o}\""),
            "module = \"m.wasm\"\n[[task]]\nfiles = [\"a/../../x\"]".to_string(),
            "module = \"C:m.wasm\"".to_string(),
        ] {
            let err = load(bad.clone()).expect_err(&bad);
            assert!(
                format!("{err:#}").contains("inside its folder"),
                "{bad}: {err:#}"
            );
        }
        // `pumasi run` still reads paths its own user wrote.
        std::fs::write(&job, format!("module = \"m.wasm\"\nstdin = \"{o}\"")).unwrap();
        let (sub, _) = JobFile::load_split(&job, 1 << 20).unwrap();
        assert_eq!(sub.tasks[0].stdin, b"admin ticket");
    }

    #[test]
    fn rel_paths_and_file_roundtrip() {
        assert!(validate_rel_path("a/b.txt").is_ok());
        for bad in ["", "/etc/passwd", "../x", "a/../b", "a//b", "C:x", "a\\b"] {
            assert!(
                validate_rel_path(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        let dir = tempdir();
        let files = vec![
            FileEntry {
                path: "x/y.txt".into(),
                data: b"1".to_vec(),
            },
            FileEntry {
                path: "z.bin".into(),
                data: vec![0; 10],
            },
        ];
        write_files(&dir, &files).unwrap();
        let (back, truncated) = read_files(&dir, usize::MAX).unwrap();
        assert_eq!(back, files);
        assert!(!truncated);
        let (some, truncated) = read_files(&dir, 5).unwrap();
        assert_eq!(some.len(), 1);
        assert!(truncated);
    }

    #[test]
    fn example_jobs_parse() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        let sub = JobFile::load(&root.join("python-wordcount/job.toml")).unwrap();
        assert_eq!(sub.executor, Executor::Docker);
        assert_eq!(sub.tasks.len(), 4);
        assert!(sub.files.iter().any(|f| f.path == "count.py"));
        assert!(sub.files.iter().any(|f| f.path.starts_with("texts/")));
        assert_eq!(sub.limits.cpu_millis, Some(1000));
        let vm = JobFile::load(&root.join("python-wordcount/job-microvm.toml")).unwrap();
        assert_eq!(vm.executor, Executor::Microvm);
        assert_eq!(vm.tasks, sub.tasks);
    }

    #[test]
    fn rejects_oversized_input() {
        let dir = tempdir();
        let f = JobFile::parse(r#"image = "busybox""#).unwrap();
        let mut sub = f.resolve(&dir).unwrap();
        sub.files.push(FileEntry {
            path: "big.bin".into(),
            data: vec![0; MAX_INPUT_FILES_BYTES + 1],
        });
        assert!(sub.validate().is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration_ms("250ms").unwrap(), 250);
        assert_eq!(parse_duration_ms("2m").unwrap(), 120_000);
        assert_eq!(parse_duration_ms("7").unwrap(), 7000);
        assert_eq!(parse_duration_ms("2d").unwrap(), 172_800_000);
        assert!(parse_duration_ms("abc").is_err());
        // Found by the config_files fuzz target: it used to overflow.
        assert!(parse_duration_ms("006726510560476608740").is_err());
        assert!(parse_duration_ms("18446744073709551615d").is_err());
    }

    #[test]
    fn job_id_roundtrip() {
        let id = JobId::random();
        assert_eq!(id.to_string().parse::<JobId>().unwrap(), id);
    }

    fn tempdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("pumasi-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
