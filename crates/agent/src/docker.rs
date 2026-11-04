//! The Docker executor: runs a task in a container on the node's Docker
//! (or Podman, via `--docker-bin podman`).
//!
//! Meant for machines you own: containers share the host kernel, so this
//! executor is opt-in per agent (`--allow-docker`). For each task:
//!
//! 1. pull the image if it is missing (not counted against the timeout),
//! 2. `docker create` with memory/CPU/pid caps, no network unless the job
//!    asks for it, and `no-new-privileges`,
//! 3. copy input files into `/input` and an empty `/output` with
//!    `docker cp` (no bind mounts, so it works the same on Docker Desktop
//!    and leaves no root-owned files on the host),
//! 4. `docker start -a -i` with the task's stdin, capturing stdout/stderr,
//!    killing the container on timeout or cancellation,
//! 5. copy `/output` back and remove the container.

use std::{
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    job::{DockerSpec, FileEntry, Limits, MAX_OUTPUT_BYTES, TaskInput, write_files},
    msg::{Outcome, TaskRef},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tracing::{debug, info, warn};

use crate::{executor::RunResult, executor::collect_output, tempdir::TempDir};

const PULL_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const CLI_TIMEOUT: Duration = Duration::from_secs(120);
/// Docker refuses memory limits below 6 MiB.
const MIN_MEMORY: u64 = 6 * 1024 * 1024;

#[derive(Clone)]
pub struct DockerExecutor {
    bin: String,
    /// Marks this agent's containers so leftovers can be cleaned up.
    label: String,
    /// One pull at a time: parallel tasks on a fresh image would otherwise
    /// all pull it at once, which registries rate-limit.
    pull_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DockerExecutor {
    /// Checks that the Docker CLI works and the daemon answers.
    pub async fn detect(bin: &str, label: &str) -> Result<Self> {
        let out = tokio::time::timeout(
            Duration::from_secs(15),
            Command::new(bin)
                .args(["version", "--format", "{{.Server.Version}}"])
                .output(),
        )
        .await
        .context("docker did not answer within 15s")?
        .with_context(|| format!("could not run `{bin}`; is Docker installed?"))?;
        if !out.status.success() {
            bail!(
                "`{bin} version` failed (is the Docker daemon running?): {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
        info!(%version, "docker executor enabled");
        let exec = Self {
            bin: bin.to_string(),
            label: label.to_string(),
            pull_lock: Default::default(),
        };
        exec.cleanup_stale().await;
        Ok(exec)
    }

    /// Removes containers this agent left behind (e.g. after a crash).
    pub async fn cleanup_stale(&self) {
        let filter = format!("label=pumasi.agent={}", self.label);
        let Ok(out) = self.cli(&["ps", "-aq", "--filter", &filter]).await else {
            return;
        };
        let ids: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .map(String::from)
            .collect();
        if !ids.is_empty() {
            warn!(count = ids.len(), "removing leftover task containers");
            let mut args = vec!["rm", "-f"];
            args.extend(ids.iter().map(String::as_str));
            let _ = self.cli(&args).await;
        }
    }

    async fn cli(&self, args: &[&str]) -> Result<std::process::Output> {
        let out = tokio::time::timeout(
            CLI_TIMEOUT,
            Command::new(&self.bin)
                .args(args)
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .with_context(|| format!("`docker {}` timed out", args.first().unwrap_or(&"")))??;
        Ok(out)
    }

    async fn has_image(&self, image: &str) -> Result<bool> {
        let inspect = self
            .cli(&["image", "inspect", "--format", "{{.Id}}", image])
            .await?;
        Ok(inspect.status.success())
    }

    async fn ensure_image(&self, image: &str, cancel: &AtomicBool) -> Result<()> {
        if self.has_image(image).await? {
            return Ok(());
        }
        let _pull = self.pull_lock.lock().await;
        // Another task may have pulled it while we waited.
        if self.has_image(image).await? {
            return Ok(());
        }
        let mut delay = Duration::from_secs(2);
        let mut attempt = 1;
        loop {
            match self.pull(image, cancel).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < 3 && !cancel.load(Ordering::Relaxed) => {
                    warn!(%image, "pull failed (attempt {attempt}), retrying: {e:#}");
                    tokio::time::sleep(delay).await;
                    delay *= 3;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn pull(&self, image: &str, cancel: &AtomicBool) -> Result<()> {
        info!(%image, "pulling image");
        let mut child = Command::new(&self.bin)
            .args(["pull", "--quiet", image])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut stderr = child.stderr.take().expect("piped");
        let err_task = tokio::spawn(async move { read_capped(&mut stderr, 64 * 1024).await });
        let deadline = Instant::now() + PULL_TIMEOUT;
        let status = loop {
            tokio::select! {
                s = child.wait() => break s?,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    if cancel.load(Ordering::Relaxed) {
                        let _ = child.kill().await;
                        bail!("cancelled while pulling {image}");
                    }
                    if Instant::now() > deadline {
                        let _ = child.kill().await;
                        bail!("pulling {image} took longer than {} minutes", PULL_TIMEOUT.as_secs() / 60);
                    }
                }
            }
        };
        if !status.success() {
            let err = err_task.await.unwrap_or_default();
            bail!(
                "could not pull {image}: {}",
                String::from_utf8_lossy(&err).trim()
            );
        }
        Ok(())
    }

    /// Runs one task to completion.
    pub async fn run(
        &self,
        task: TaskRef,
        spec: &DockerSpec,
        input: &TaskInput,
        shared_files: &[FileEntry],
        limits: &Limits,
        cancel: Arc<AtomicBool>,
    ) -> RunResult {
        let start = Instant::now();
        match self
            .run_inner(task, spec, input, shared_files, limits, &cancel, start)
            .await
        {
            Ok(r) => r,
            Err(e) => RunResult {
                outcome: if cancel.load(Ordering::Relaxed) {
                    Outcome::Cancelled
                } else {
                    Outcome::Error(format!("{e:#}"))
                },
                stdout: vec![],
                stderr: vec![],
                files: vec![],
                fuel_used: 0,
                wall_ms: start.elapsed().as_millis() as u64,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_inner(
        &self,
        task: TaskRef,
        spec: &DockerSpec,
        input: &TaskInput,
        shared_files: &[FileEntry],
        limits: &Limits,
        cancel: &AtomicBool,
        start: Instant,
    ) -> Result<RunResult> {
        self.ensure_image(&spec.image, cancel).await?;
        // The timeout covers the run itself, not the image pull.
        let run_start = Instant::now();

        let scratch = TempDir::new("docker")?;
        let tree = scratch.path().join("tree");
        std::fs::create_dir_all(tree.join("input"))?;
        std::fs::create_dir_all(tree.join("output"))?;
        write_files(&tree.join("input"), shared_files)?;
        write_files(&tree.join("input"), &input.files)?;

        let name = format!(
            "pumasi-{}-{}-{}-{:08x}",
            task.job,
            task.index,
            task.attempt,
            rand_u32()
        );
        let args = create_args(&name, &self.label, spec, input, limits);
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let created = self.cli(&arg_refs).await?;
        if !created.status.success() {
            bail!(
                "docker create failed: {}",
                String::from_utf8_lossy(&created.stderr).trim()
            );
        }
        let _guard = ContainerGuard {
            bin: self.bin.clone(),
            name: name.clone(),
        };

        let src = format!("{}{}.", tree.display(), std::path::MAIN_SEPARATOR);
        let copied = self.cli(&["cp", &src, &format!("{name}:/")]).await?;
        if !copied.status.success() {
            bail!(
                "copy input files: {}",
                String::from_utf8_lossy(&copied.stderr).trim()
            );
        }

        let mut child = Command::new(&self.bin)
            .args(["start", "--attach", "--interactive", &name])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("docker start")?;
        let mut stdin = child.stdin.take().expect("piped");
        let stdin_data = input.stdin.clone();
        tokio::spawn(async move {
            let _ = stdin.write_all(&stdin_data).await;
            let _ = stdin.shutdown().await;
        });
        let mut out = child.stdout.take().expect("piped");
        let mut err = child.stderr.take().expect("piped");
        let out_task = tokio::spawn(async move { read_capped(&mut out, MAX_OUTPUT_BYTES).await });
        let err_task = tokio::spawn(async move { read_capped(&mut err, MAX_OUTPUT_BYTES).await });

        let deadline = run_start + Duration::from_millis(limits.timeout_ms);
        let mut stopped: Option<Outcome> = None;
        let status = loop {
            tokio::select! {
                s = child.wait() => break s?,
                _ = tokio::time::sleep(Duration::from_millis(100)), if stopped.is_none() => {
                    if cancel.load(Ordering::Relaxed) {
                        stopped = Some(Outcome::Cancelled);
                    } else if Instant::now() >= deadline {
                        stopped = Some(Outcome::Timeout);
                    }
                    if stopped.is_some() {
                        debug!(container = %name, "killing container");
                        let _ = self.cli(&["kill", &name]).await;
                    }
                }
            }
        };
        let stdout = out_task.await.unwrap_or_default();
        let mut stderr = err_task.await.unwrap_or_default();

        let outcome = match stopped {
            Some(o) => o,
            None => {
                let code = status.code().unwrap_or(-1);
                if code != 0 && self.oom_killed(&name).await {
                    Outcome::Trap(format!(
                        "out of memory (limit {} MiB)",
                        limits.memory_bytes / 1024 / 1024
                    ))
                } else {
                    Outcome::Exited(code)
                }
            }
        };

        let out_dir = scratch.path().join("out");
        let copied = self
            .cli(&[
                "cp",
                &format!("{name}:/output/."),
                &out_dir.display().to_string(),
            ])
            .await;
        let files = match copied {
            Ok(o) if o.status.success() => collect_output(&out_dir, &mut stderr),
            _ => Vec::new(),
        };

        Ok(RunResult {
            outcome,
            stdout,
            stderr,
            files,
            fuel_used: 0,
            wall_ms: start.elapsed().as_millis() as u64,
        })
    }

    async fn oom_killed(&self, name: &str) -> bool {
        match self
            .cli(&["inspect", "--format", "{{.State.OOMKilled}}", name])
            .await
        {
            Ok(o) => String::from_utf8_lossy(&o.stdout).trim() == "true",
            Err(_) => false,
        }
    }
}

fn create_args(
    name: &str,
    label: &str,
    spec: &DockerSpec,
    input: &TaskInput,
    limits: &Limits,
) -> Vec<String> {
    let memory = limits.memory_bytes.max(MIN_MEMORY);
    let mut a: Vec<String> = [
        "create",
        "--name",
        name,
        "--label",
        "pumasi=1",
        "--label",
        &format!("pumasi.agent={label}"),
        "--interactive",
        "--init",
        "--workdir",
        "/input",
        "--network",
        if spec.network { "bridge" } else { "none" },
        "--memory",
        &format!("{memory}b"),
        "--memory-swap",
        &format!("{memory}b"),
        "--pids-limit",
        "4096",
        "--security-opt",
        "no-new-privileges",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if let Some(m) = limits.cpu_millis {
        a.push("--cpus".into());
        a.push(format!("{:.3}", m as f64 / 1000.0));
    }
    if let Some(g) = &spec.gpus {
        a.push("--gpus".into());
        a.push(g.clone());
    }
    for (k, v) in &input.env {
        a.push("--env".into());
        a.push(format!("{k}={v}"));
    }
    a.push(spec.image.clone());
    a.extend(spec.command.iter().cloned());
    a.extend(input.args.iter().cloned());
    a
}

/// Removes the container when the task ends, however it ends.
struct ContainerGuard {
    bin: String,
    name: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new(&self.bin)
            .args(["rm", "-f", &self.name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Reads a stream to the end, keeping at most `cap` bytes.
async fn read_capped<R: AsyncRead + Unpin>(r: &mut R, cap: usize) -> Vec<u8> {
    let mut kept = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let room = cap.saturating_sub(kept.len());
                kept.extend_from_slice(&buf[..n.min(room)]);
            }
        }
    }
    kept
}

fn rand_u32() -> u32 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::SystemTime::now()) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_args_are_locked_down_by_default() {
        let spec = DockerSpec {
            image: "python:3.12-slim".into(),
            command: vec!["python".into(), "main.py".into()],
            network: false,
            gpus: None,
        };
        let input = TaskInput {
            args: vec!["7".into()],
            env: [("A".to_string(), "1".to_string())].into(),
            ..Default::default()
        };
        let limits = Limits {
            memory_bytes: 64 << 20,
            cpu_millis: Some(1500),
            ..Default::default()
        };
        let a = create_args("c1", "node", &spec, &input, &limits);
        let joined = a.join(" ");
        assert!(joined.contains("--network none"));
        assert!(joined.contains("--memory 67108864b"));
        assert!(joined.contains("--cpus 1.500"));
        assert!(joined.contains("--security-opt no-new-privileges"));
        assert!(joined.contains("--env A=1"));
        assert!(joined.ends_with("python:3.12-slim python main.py 7"));
        assert!(!joined.contains("--gpus"));

        let net = DockerSpec {
            network: true,
            gpus: Some("all".into()),
            ..spec
        };
        let joined = create_args(
            "c1",
            "node",
            &net,
            &TaskInput::default(),
            &Limits::default(),
        )
        .join(" ");
        assert!(joined.contains("--network bridge"));
        assert!(joined.contains("--gpus all"));
    }
}
