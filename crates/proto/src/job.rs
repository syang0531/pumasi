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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobId(pub u128);

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

/// Confidentiality tier. Only `Open` exists in phase 1: the provider can in
/// principle read the code and data. See the research doc for why stronger
/// tiers need TEE hardware.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    #[default]
    Open,
}

/// Which executor runs the task.
///
/// - `Wasm`: Wasmtime sandbox, runs on every node, no network.
/// - `Docker`: an OCI image run by the node's Docker (or Podman). Any
///   language, packages and optional network. Only nodes that opted in with
///   `--allow-docker` get these tasks; meant for machines you own.
/// - `Microvm`: the same OCI images, each task in its own lightweight VM
///   (Firecracker, or QEMU's microvm machine) with its own kernel. No
///   network. Safe to send to other people's machines, so providers that
///   opted in with `--allow-microvm` get these tasks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Allow outbound network access (default: none). Docker only.
    pub network: bool,
    /// Passed to `docker run --gpus`, e.g. "all". Docker only.
    pub gpus: Option<String>,
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
}

impl JobSubmission {
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
                if kind == Executor::Microvm {
                    ensure!(
                        !d.network,
                        "microvm jobs have no network; bake what the job needs into the image, or use a docker job"
                    );
                    ensure!(
                        d.gpus.is_none(),
                        "microvm jobs cannot use GPUs yet; use a docker job"
                    );
                }
            }
        }
        for f in self
            .files
            .iter()
            .chain(self.tasks.iter().flat_map(|t| t.files.iter()))
        {
            validate_rel_path(&f.path)?;
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
    /// Files and directories copied into every task's `/input`.
    #[serde(default)]
    pub files: Vec<PathBuf>,
    /// Allow network access (docker executor only).
    #[serde(default)]
    pub network: bool,
    /// GPUs for `docker run --gpus` (docker executor only), e.g. "all".
    pub gpus: Option<String>,
    pub shards: Option<u32>,
    #[serde(default, rename = "task")]
    pub tasks: Vec<TaskFile>,
    #[serde(default)]
    pub resources: ResourcesFile,
    #[serde(default)]
    pub tier: Tier,
    pub max_attempts: Option<u32>,
    pub max_price_per_hour: Option<f64>,
    /// Deterministic mode (wasm only, default on): a virtual clock and
    /// seeded random numbers, so results can be checked by re-running.
    pub deterministic: Option<bool>,
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
/// under it with its path relative to `base`.
pub fn collect_files(base: &Path, paths: &[PathBuf]) -> Result<Vec<FileEntry>> {
    let mut out: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for p in paths {
        let full = base.join(p);
        let meta = std::fs::metadata(&full).with_context(|| format!("read {}", full.display()))?;
        let root_rel = rel_string(p)?;
        if meta.is_dir() {
            let (entries, _) = read_files(&full, usize::MAX)?;
            for e in entries {
                let path = if root_rel.is_empty() {
                    e.path
                } else {
                    format!("{root_rel}/{}", e.path)
                };
                out.insert(path, e.data);
            }
        } else {
            ensure!(!root_rel.is_empty(), "invalid file path {}", p.display());
            out.insert(root_rel, std::fs::read(&full)?);
        }
    }
    let files: Vec<FileEntry> = out
        .into_iter()
        .map(|(path, data)| FileEntry { path, data })
        .collect();
    for f in &files {
        validate_rel_path(&f.path)?;
    }
    Ok(files)
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
                    "wasm tasks cannot use the network; use a docker job"
                );
                ensure!(self.gpus.is_none(), "`gpus` is only for docker jobs");
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
        let files = collect_files(base, &self.files)?;

        let mut command = self.command.clone();
        let tasks = if !self.tasks.is_empty() {
            if self.shards.is_some() {
                bail!("use either `shards` or `[[task]]`, not both");
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
            gpus: self.gpus.clone(),
        });
        let sub = JobSubmission {
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
                Executor::Wasm => self.deterministic.unwrap_or(true),
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
        Ok(sub)
    }
}

fn expand(s: &str, shard: u32, shards: u32) -> String {
    s.replace("{shard}", &shard.to_string())
        .replace("{shards}", &shards.to_string())
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
    Ok(n * mult)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WASM_HEADER: &[u8] = b"\0asm\x01\0\0\0";

    fn write_module(dir: &Path) {
        std::fs::write(dir.join("m.wasm"), WASM_HEADER).unwrap();
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
        assert!(net.resolve(&dir).is_err(), "microvm jobs have no network");
        let gpu =
            JobFile::parse("executor = \"microvm\"\nimage = \"busybox\"\ngpus = \"all\"").unwrap();
        assert!(gpu.resolve(&dir).is_err(), "no GPU passthrough yet");
        let none = JobFile::parse("executor = \"microvm\"").unwrap();
        assert!(none.resolve(&dir).is_err(), "needs an image");
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
