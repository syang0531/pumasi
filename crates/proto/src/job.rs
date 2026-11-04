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

/// Which executor runs the task. Phase 1 ships only `Wasm`; `MicroVm`
/// (OCI images, GPU) arrives in a later phase behind the same job format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Executor {
    #[default]
    Wasm,
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
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: DEFAULT_MEMORY_MB * 1024 * 1024,
            fuel: None,
            timeout_ms: DEFAULT_TIMEOUT_SECS * 1000,
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
}

/// What the CLI sends to the control plane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSubmission {
    pub name: String,
    pub executor: Executor,
    pub module: Vec<u8>,
    pub tasks: Vec<TaskInput>,
    pub limits: Limits,
    pub tier: Tier,
    pub max_attempts: u32,
    /// Recorded for the future marketplace; ignored in a private cluster.
    pub max_price_per_hour: Option<f64>,
}

impl JobSubmission {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.tasks.is_empty(), "job has no tasks");
        ensure!(self.tasks.len() <= 100_000, "too many tasks (max 100000)");
        ensure!(!self.module.is_empty(), "module is empty");
        ensure!(
            self.module.starts_with(b"\0asm"),
            "module is not a WebAssembly binary"
        );
        ensure!(self.max_attempts >= 1, "max_attempts must be at least 1");
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
/// ```toml
/// name = "estimate-pi"
/// module = "montecarlo.wasm"
/// shards = 8
/// args = ["--seed", "{shard}", "--samples", "2000000"]
///
/// [resources]
/// memory_mb = 64
/// timeout = "60s"
/// ```
///
/// Either `shards` (N copies of `args`, with `{shard}` / `{shards}`
/// substituted) or an explicit `[[task]]` list may be given. With neither,
/// the job has a single task.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFile {
    pub name: Option<String>,
    pub module: PathBuf,
    #[serde(default)]
    pub executor: Executor,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Path to a file fed to every task's stdin.
    pub stdin: Option<PathBuf>,
    pub shards: Option<u32>,
    #[serde(default, rename = "task")]
    pub tasks: Vec<TaskFile>,
    #[serde(default)]
    pub resources: ResourcesFile,
    #[serde(default)]
    pub tier: Tier,
    pub max_attempts: Option<u32>,
    pub max_price_per_hour: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFile {
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub stdin: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcesFile {
    pub memory_mb: Option<u64>,
    pub fuel: Option<u64>,
    /// Human duration such as "90s", "5m".
    pub timeout: Option<String>,
}

impl JobFile {
    pub fn parse(text: &str) -> Result<Self> {
        toml::from_str(text).context("invalid job file")
    }

    /// Loads the file, reads the module and stdin files relative to it, and
    /// expands shards into concrete tasks.
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
        let module = read(&self.module)?;
        let shared_stdin = match &self.stdin {
            Some(p) => read(p)?,
            None => Vec::new(),
        };

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
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            let n = self.shards.unwrap_or(1);
            ensure!(n >= 1, "shards must be at least 1");
            (0..n)
                .map(|i| TaskInput {
                    args: self.args.iter().map(|a| expand(a, i, n)).collect(),
                    env: self
                        .env
                        .iter()
                        .map(|(k, v)| (k.clone(), expand(v, i, n)))
                        .collect(),
                    stdin: shared_stdin.clone(),
                })
                .collect()
        };

        let mut limits = Limits::default();
        if let Some(mb) = self.resources.memory_mb {
            limits.memory_bytes = mb * 1024 * 1024;
        }
        limits.fuel = self.resources.fuel;
        if let Some(t) = &self.resources.timeout {
            limits.timeout_ms = parse_duration_ms(t)?;
        }

        let name = self.name.unwrap_or_else(|| {
            self.module
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "job".into())
        });
        let sub = JobSubmission {
            name,
            executor: self.executor,
            module,
            tasks,
            limits,
            tier: self.tier,
            max_attempts: self.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS),
            max_price_per_hour: self.max_price_per_hour,
        };
        sub.validate()?;
        Ok(sub)
    }
}

fn expand(s: &str, shard: u32, shards: u32) -> String {
    s.replace("{shard}", &shard.to_string())
        .replace("{shards}", &shards.to_string())
}

/// Parses "250ms", "90s", "5m", "1h" or a bare number of seconds.
pub fn parse_duration_ms(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1)
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
    fn durations() {
        assert_eq!(parse_duration_ms("250ms").unwrap(), 250);
        assert_eq!(parse_duration_ms("2m").unwrap(), 120_000);
        assert_eq!(parse_duration_ms("7").unwrap(), 7000);
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
