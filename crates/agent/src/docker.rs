//! The Docker executor: runs a task in a container on the node's Docker
//! (or Podman, via `--docker-bin podman`).
//!
//! Meant for machines you own: containers share the host kernel, so this
//! executor is opt-in per agent (`--allow-docker`). For each task:
//!
//! 1. pull the image if it is missing (not counted against the timeout),
//! 2. `docker create` with memory/CPU/pid caps, no network unless the job
//!    asks for it, and `no-new-privileges`,
//! 3. stream a tar of the input files (`/input`) and an empty `/output`
//!    into the container with `docker cp -`: no bind mounts and no host
//!    paths, so it works the same on Docker Desktop (Windows, macOS) and
//!    remote daemons, and leaves no root-owned files on the host,
//! 4. `docker start -a -i` with the task's stdin, capturing stdout/stderr,
//!    killing the container on timeout or cancellation,
//! 5. read `/output` back as a tar stream (`docker cp ... -`), parsed in
//!    memory within the output cap, and remove the container.

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
    job::{
        DockerSpec, FileEntry, Limits, MAX_OUTPUT_BYTES, MAX_OUTPUT_FILES_BYTES, TaskInput,
        validate_rel_path,
    },
    msg::{Outcome, TaskRef},
    store::{IMAGE_CONFIG, ImageManifest, MAX_STORE_OUTPUT_BYTES, docker_tag, image_layer_name},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tracing::{debug, info, warn};

use crate::{
    executor::{LiveOutput, LocalImage, RunResult, TaskIo},
    platform::ProcessJob,
};

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
    /// The node's CPU cap: containers of jobs that set no CPU limit get
    /// this many.
    cpu_cap: Option<u32>,
    /// Give NVIDIA graphics tasks Docker Desktop's `/usr/lib/wsl`
    /// (read-only), which it leaves out of `--gpus` containers.
    wsl_libs: bool,
}

impl DockerExecutor {
    /// The Docker CLI this executor runs.
    pub fn bin(&self) -> &str {
        &self.bin
    }

    /// Limits containers of jobs without a CPU limit to `cpus` (the node's
    /// CPU cap; jobs with a limit are placed only within it).
    pub fn with_cpu_cap(mut self, cpus: Option<u32>) -> Self {
        self.cpu_cap = cpus;
        self
    }

    /// Lends NVIDIA tasks that ask for `graphics` the WSL 2 driver
    /// libraries ([`crate::gpu::docker_check::WSL_LIBS`], read-only): the
    /// only host folder a container gets.
    pub fn with_wsl_libs(mut self, on: bool) -> Self {
        self.wsl_libs = on;
        self
    }

    /// Checks that the Docker CLI works, the daemon answers, and it runs
    /// Linux containers.
    pub async fn detect(bin: &str, label: &str) -> Result<Self> {
        // A Windows service may start before Docker Desktop's directory is
        // on its PATH; look in the default install location too.
        let resolved = if bin == "docker" && cfg!(windows) {
            crate::microvm::vmm::find_program(
                "docker",
                None,
                &[r"C:\Program Files\Docker\Docker\resources\bin"],
            )
            .map(|p| p.display().to_string())
        } else {
            None
        };
        let bin = resolved.as_deref().unwrap_or(bin);
        let out = tokio::time::timeout(
            Duration::from_secs(15),
            Command::new(bin)
                .args(["version", "--format", "{{.Server.Version}} {{.Server.Os}}"])
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .context("docker did not answer within 15s")?
        .with_context(|| format!("could not run `{bin}`; is Docker installed?"))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let hint = if cfg!(windows) {
                " (is Docker Desktop running? It must be started once in the signed-in session, and the agent's account must be in the docker-users group)"
            } else {
                " (is the Docker daemon running?)"
            };
            bail!(
                "{} (`{bin} version` failed{hint}: {})",
                not_running(&stderr),
                stderr.trim()
            );
        }
        let answer = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let (version, os) = parse_server(&answer);
        if os != "linux" {
            bail!(
                "the Docker daemon runs {os} containers; jobs need Linux containers (Docker Desktop: right-click the tray icon and choose \"Switch to Linux containers\", with the WSL 2 backend)"
            );
        }
        info!(%version, "docker executor enabled");
        let exec = Self {
            bin: bin.to_string(),
            label: label.to_string(),
            pull_lock: Default::default(),
            cpu_cap: None,
            wsl_libs: false,
        };
        exec.cleanup_stale().await;
        Ok(exec)
    }

    /// Whether the daemon still answers and runs Linux containers (Docker
    /// Desktop can stop while the agent runs). The error says why not.
    pub async fn alive(&self) -> Result<()> {
        let out = tokio::time::timeout(
            Duration::from_secs(15),
            Command::new(&self.bin)
                .args(["version", "--format", "{{.Server.Os}}"])
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .context("docker did not answer within 15s")?
        .with_context(|| format!("could not run `{}`", self.bin))?;
        if !out.status.success() {
            bail!("{}", not_running(&String::from_utf8_lossy(&out.stderr)));
        }
        let os = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if os != "linux" {
            bail!("the Docker daemon runs {os} containers, not Linux ones");
        }
        Ok(())
    }

    /// The image's digest: `repo@sha256:...` from the registry it came
    /// from, or its image id when it has none (built locally).
    pub async fn image_digest(&self, image: &str) -> Option<String> {
        let out = self
            .cli(&[
                "image",
                "inspect",
                "--format",
                "{{json .RepoDigests}} {{.Id}}",
                image,
            ])
            .await
            .ok()?;
        if !out.status.success() {
            return None;
        }
        pick_digest(image, String::from_utf8_lossy(&out.stdout).trim())
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

    /// Whether an image from the file store (by manifest digest) is
    /// already loaded.
    pub async fn has_local_image(&self, digest: &str) -> bool {
        self.has_image(&docker_tag(digest)).await.unwrap_or(false)
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

    /// Loads an image taken from the file store into Docker (as
    /// `docker save` would have written it) and returns its local tag.
    async fn load_local_image(&self, image: &LocalImage) -> Result<String> {
        let tag = docker_tag(&image.digest);
        if self.has_image(&tag).await? {
            return Ok(tag);
        }
        let _pull = self.pull_lock.lock().await;
        if self.has_image(&tag).await? {
            return Ok(tag);
        }
        info!(image = %tag, "loading a private image from the file store");
        let manifest: ImageManifest = serde_json::from_slice(
            &std::fs::read(image.dir.join(pumasi_proto::store::IMAGE_MANIFEST))
                .context("read the image manifest")?,
        )
        .context("bad image manifest")?;
        let mut child = Command::new(&self.bin)
            .args(["load", "--quiet"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("docker load")?;
        let stdin = child.stdin.take().expect("piped");
        let dir = image.dir.clone();
        let tag2 = tag.clone();
        let feed = tokio::task::spawn_blocking(move || -> Result<()> {
            let w = tokio_util::io::SyncIoBridge::new(stdin);
            let mut b = tar::Builder::new(std::io::BufWriter::with_capacity(1 << 20, w));
            b.mode(tar::HeaderMode::Deterministic);
            let config_name = format!(
                "{}.json",
                manifest.config_digest.trim_start_matches("sha256:")
            );
            let layers: Vec<String> = (0..manifest.layers.len())
                .map(|i| format!("{}.tar", image_layer_name(i)))
                .collect();
            let index = serde_json::json!([{
                "Config": config_name,
                "RepoTags": [tag2],
                "Layers": layers,
            }]);
            let mut f = std::fs::File::open(dir.join(IMAGE_CONFIG))?;
            b.append_file(&config_name, &mut f)?;
            for (i, name) in layers.iter().enumerate() {
                let mut f = std::fs::File::open(dir.join(image_layer_name(i)))?;
                b.append_file(name, &mut f)?;
            }
            let json = serde_json::to_vec(&index)?;
            let mut h = tar::Header::new_gnu();
            h.set_size(json.len() as u64);
            h.set_mode(0o644);
            b.append_data(&mut h, "manifest.json", json.as_slice())?;
            let mut w = b.into_inner()?;
            std::io::Write::flush(&mut w)?;
            drop(w);
            Ok(())
        });
        let out = tokio::time::timeout(PULL_TIMEOUT, child.wait_with_output())
            .await
            .context("docker load timed out")??;
        let fed = feed.await;
        if !out.status.success() {
            bail!(
                "docker load: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        fed.context("docker load")??;
        Ok(tag)
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
        self.run_io(
            task,
            spec,
            input,
            shared_files,
            limits,
            cancel,
            &TaskIo::default(),
        )
        .await
    }

    /// [`Self::run`] with mounted files, outputs that may go to the file
    /// store and private images from it.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_io(
        &self,
        task: TaskRef,
        spec: &DockerSpec,
        input: &TaskInput,
        shared_files: &[FileEntry],
        limits: &Limits,
        cancel: Arc<AtomicBool>,
        io: &TaskIo,
    ) -> RunResult {
        let start = Instant::now();
        match self
            .run_inner(task, spec, input, shared_files, limits, &cancel, start, io)
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
                peak_memory_bytes: None,
                out_dir: None,
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
        io: &TaskIo,
    ) -> Result<RunResult> {
        let local;
        let spec = match &io.image {
            Some(img) => {
                local = DockerSpec {
                    image: self.load_local_image(img).await?,
                    ..spec.clone()
                };
                &local
            }
            None => {
                self.ensure_image(&spec.image, cancel).await?;
                spec
            }
        };
        if let Some(r) = &io.image_report {
            match &io.image {
                // A private image from the store is named by its digest.
                Some(img) => r.send(&spec.image, img.digest.clone()),
                None => {
                    if let Some(d) = self.image_digest(&spec.image).await {
                        r.send(&spec.image, d);
                    }
                }
            }
        }
        // The timeout covers the run itself, not the image pull.
        let run_start = Instant::now();

        let name = format!(
            "pumasi-{}-{}-{}-{:08x}",
            task.job,
            task.index,
            task.attempt,
            rand_u32()
        );
        let mut args = create_args(&name, &self.label, spec, input, limits, self.cpu_cap);
        let image_at = args.len() - 1 - spec.command.len() - input.args.len();
        let gpu_args = gpu_args(&io.gpus, &io.gpu_capabilities, self.wsl_libs);
        args.splice(image_at..image_at, gpu_args);
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

        self.copy_in(&name, shared_files, &input.files, io.staged.as_deref())
            .await?;

        let mut child = Command::new(&self.bin)
            .args(["start", "--attach", "--interactive", &name])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("docker start")?;
        // Windows: the attached CLI dies with the agent instead of
        // lingering; the container itself is removed at the next start.
        let _job = ProcessJob::new()
            .and_then(|job| job.assign(&child).map(|()| job))
            .ok();
        let mut stdin = child.stdin.take().expect("piped");
        let stdin_data = input.stdin.clone();
        tokio::spawn(async move {
            let _ = stdin.write_all(&stdin_data).await;
            let _ = stdin.shutdown().await;
        });
        let mut out = child.stdout.take().expect("piped");
        let mut err = child.stderr.take().expect("piped");
        let (live_out, live_err) = (io.live.clone(), io.live.clone());
        let out_task = tokio::spawn(async move {
            read_capped_live(&mut out, MAX_OUTPUT_BYTES, live_out.as_ref(), false).await
        });
        let err_task = tokio::spawn(async move {
            read_capped_live(&mut err, MAX_OUTPUT_BYTES, live_err.as_ref(), true).await
        });

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

        let (files, out_dir) = if io.keeps_output() {
            let dir = io.out_dir("out")?;
            self.copy_out_dir(&name, dir.path(), &mut stderr).await;
            (Vec::new(), Some(Arc::new(dir)))
        } else {
            (self.copy_out(&name, &mut stderr).await, None)
        };

        Ok(RunResult {
            outcome,
            stdout,
            stderr,
            files,
            fuel_used: 0,
            wall_ms: start.elapsed().as_millis() as u64,
            peak_memory_bytes: None,
            out_dir,
        })
    }

    /// Streams a tar of the input files (and `staged`, the files mounted
    /// from the store, read from disk as they go) into the container's
    /// root (`docker cp - name:/`).
    async fn copy_in(
        &self,
        name: &str,
        shared: &[FileEntry],
        own: &[FileEntry],
        staged: Option<&std::path::Path>,
    ) -> Result<()> {
        let staged_files = match staged {
            Some(d) => pumasi_proto::job::dir_files(d)?,
            None => Vec::new(),
        };
        let bytes: u64 = staged_files.iter().map(|(_, _, n)| n).sum::<u64>()
            + shared
                .iter()
                .chain(own)
                .map(|f| f.data.len() as u64)
                .sum::<u64>();
        // Plenty for a slow disk: 10 MiB/s on top of the usual timeout.
        let timeout = CLI_TIMEOUT + Duration::from_secs(bytes / (10 << 20));
        let mut child = Command::new(&self.bin)
            .args(["cp", "-", &format!("{name}:/")])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("docker cp")?;
        let stdin = child.stdin.take().expect("piped");
        let (shared, own) = (shared.to_vec(), own.to_vec());
        let feed = tokio::task::spawn_blocking(move || -> Result<()> {
            let w = tokio_util::io::SyncIoBridge::new(stdin);
            let mut w = std::io::BufWriter::with_capacity(1 << 20, w);
            write_input_tar(&mut w, &shared, &own, &staged_files)?;
            std::io::Write::flush(&mut w)?;
            Ok(())
        });
        let out = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .context("copying input files timed out")??;
        let fed = feed.await;
        if !out.status.success() {
            bail!(
                "copy input files: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        fed.context("copy input files")?
            .context("copy input files")?;
        Ok(())
    }

    /// Extracts `/output` into `dir` (`docker cp name:/output -`), up to
    /// the file store's output limit.
    async fn copy_out_dir(&self, name: &str, dir: &std::path::Path, stderr: &mut Vec<u8>) {
        let child = Command::new(&self.bin)
            .args(["cp", &format!("{name}:/output"), "-"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();
        let Ok(mut child) = child else {
            stderr.extend_from_slice(b"\n[pumasi] could not read output files\n");
            return;
        };
        let out = child.stdout.take().expect("piped");
        let dir = dir.to_path_buf();
        let res = tokio::task::spawn_blocking(move || {
            extract_output_tar(tokio_util::io::SyncIoBridge::new(out), &dir)
        })
        .await;
        let _ = child.kill().await;
        match res {
            Ok(Ok(dropped)) if dropped > 0 => stderr.extend_from_slice(
                format!(
                    "\n[pumasi] output files exceeded {} GiB; {dropped} were dropped\n",
                    MAX_STORE_OUTPUT_BYTES >> 30
                )
                .as_bytes(),
            ),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => stderr.extend_from_slice(
                format!("\n[pumasi] could not read output files: {e:#}\n").as_bytes(),
            ),
            Err(e) => stderr.extend_from_slice(
                format!("\n[pumasi] could not read output files: {e}\n").as_bytes(),
            ),
        }
    }

    /// Reads `/output` as a tar stream (`docker cp name:/output -`) and
    /// keeps its regular files within the output cap.
    async fn copy_out(&self, name: &str, stderr: &mut Vec<u8>) -> Vec<FileEntry> {
        let child = Command::new(&self.bin)
            .args(["cp", &format!("{name}:/output"), "-"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();
        let Ok(mut child) = child else {
            return Vec::new();
        };
        let mut out = child.stdout.take().expect("piped");
        // Room for the files plus tar headers; a bigger stream is cut off
        // and what fits is kept.
        let cap = MAX_OUTPUT_FILES_BYTES * 2 + (1 << 20);
        let read = tokio::time::timeout(CLI_TIMEOUT, read_capped_flag(&mut out, cap)).await;
        let _ = child.kill().await;
        let Ok((tar, cut)) = read else {
            stderr.extend_from_slice(b"\n[pumasi] reading output files timed out\n");
            return Vec::new();
        };
        let (files, dropped) = parse_output_tar(&tar);
        if dropped || cut {
            stderr.extend_from_slice(
                format!(
                    "\n[pumasi] output files exceeded {} MiB; some were dropped\n",
                    MAX_OUTPUT_FILES_BYTES / 1024 / 1024
                )
                .as_bytes(),
            );
        }
        files
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
    cpu_cap: Option<u32>,
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
    if let Some(m) = limits.cpu_millis.or(cpu_cap.map(|c| c * 1000)) {
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

/// A short reason for `docker version` failing, for the cluster's status.
fn not_running(stderr: &str) -> String {
    let s = stderr.to_ascii_lowercase();
    if cfg!(windows) || s.contains("dockerdesktop") || s.contains("docker_engine") {
        "Docker Desktop is not running".into()
    } else if s.contains("permission denied") {
        "no permission to use the Docker daemon".into()
    } else {
        "the Docker daemon is not running".into()
    }
}

/// Picks the digest from `docker image inspect`'s `RepoDigests` (as JSON)
/// and `Id`: the repo digest of the image's own repository, any repo
/// digest, or the id.
fn pick_digest(image: &str, answer: &str) -> Option<String> {
    let (json, id) = answer.rsplit_once(' ').unwrap_or((answer, ""));
    let digests: Vec<String> = serde_json::from_str(json.trim()).unwrap_or_default();
    let repo = image.split('@').next().unwrap_or(image);
    let repo = match repo.rsplit_once(':') {
        Some((r, t)) if !t.contains('/') => r,
        _ => repo,
    };
    // Without a tag, the full name ends in `:latest`.
    let full_repo = |r: &str| {
        let f = pumasi_proto::manage::full_image_name(r);
        f.strip_suffix(":latest").unwrap_or(&f).to_string()
    };
    let want = full_repo(repo);
    digests
        .iter()
        .find(|d| full_repo(d.split('@').next().unwrap_or("")) == want)
        .or(digests.first())
        .cloned()
        .or_else(|| (!id.trim().is_empty()).then(|| id.trim().to_string()))
}

/// Options that give a container its GPUs: `--gpus` with the NVIDIA
/// GPUs' UUIDs (NVIDIA Container Toolkit, or Docker Desktop), AMD GPUs as
/// `/dev/kfd` plus their render nodes, and `PUMASI_GPUS` with the node's
/// numbers for them either way.
/// NVIDIA tasks also get `NVIDIA_DRIVER_CAPABILITIES` for the
/// capabilities the job asked for (phase 15), and with `wsl_libs` those
/// that ask for `graphics` get Docker Desktop's `/usr/lib/wsl` read-only,
/// where the D3D12 libraries Mesa draws with on WSL 2 are.
fn gpu_args(
    gpus: &[pumasi_proto::gpu::GpuInfo],
    caps: &[pumasi_proto::job::GpuCapability],
    wsl_libs: bool,
) -> Vec<String> {
    use pumasi_proto::gpu::GpuVendor;
    if gpus.is_empty() {
        return Vec::new();
    }
    let mut a = Vec::new();
    let nvidia: Vec<&str> = gpus
        .iter()
        .filter(|g| g.vendor == GpuVendor::Nvidia)
        .map(|g| g.uuid.as_str())
        .collect();
    if !nvidia.is_empty() {
        a.push("--gpus".into());
        if nvidia.iter().any(|u| u.is_empty()) {
            a.push("all".into());
        } else {
            // Docker's CSV syntax: the quotes keep the commas in one value.
            a.push(format!("\"device={}\"", nvidia.join(",")));
        }
        // The NVIDIA Container Toolkit mounts the driver libraries for
        // these (graphics: EGL, OpenGL and Vulkan; video: NVENC).
        if !caps.is_empty() {
            a.extend([
                "--env".into(),
                format!(
                    "NVIDIA_DRIVER_CAPABILITIES={}",
                    pumasi_proto::job::GpuCapability::driver_env(caps)
                ),
            ]);
        }
        if wsl_libs && caps.contains(&pumasi_proto::job::GpuCapability::Graphics) {
            a.extend(crate::gpu::docker_check::wsl_libs_args());
        }
    }
    let amd: Vec<&str> = gpus
        .iter()
        .filter(|g| g.vendor == GpuVendor::Amd && !g.render_node.is_empty())
        .map(|g| g.render_node.as_str())
        .collect();
    if !amd.is_empty() {
        a.extend(["--device".into(), "/dev/kfd".into()]);
        for r in amd {
            a.extend(["--device".into(), r.to_string()]);
        }
        // ROCm needs the render and video groups' access to the nodes.
        for node in ["/dev/kfd", gpus[0].render_node.as_str()] {
            if let Some(gid) = group_of(node) {
                a.extend(["--group-add".into(), gid.to_string()]);
            }
        }
    }
    let list: Vec<String> = gpus.iter().map(|g| g.index.to_string()).collect();
    a.extend(["--env".into(), format!("PUMASI_GPUS={}", list.join(","))]);
    a
}

#[cfg(unix)]
fn group_of(path: &str) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.gid())
}

#[cfg(not(unix))]
fn group_of(_: &str) -> Option<u32> {
    None
}

/// Splits `docker version`'s "<version> <os>" answer. Old daemons that
/// do not report an OS are taken to be Linux.
fn parse_server(answer: &str) -> (&str, &str) {
    match answer.split_once(' ') {
        Some((v, os)) if !os.is_empty() && !os.starts_with('<') => (v.trim(), os),
        _ => (answer, "linux"),
    }
}

/// The tar `docker cp -` unpacks at `/`: `/input` with the job's shared
/// files and then the task's own (which win), and an empty `/output` any
/// container user can write to.
#[cfg(test)]
fn input_tar(shared: &[FileEntry], own: &[FileEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_input_tar(&mut out, shared, own, &[])?;
    Ok(out)
}

/// Writes the input tar: `/input` with the files mounted from the store
/// (`staged`: path in `/input`, file on disk, size), then the job's shared
/// files and the task's own (later ones win), and an empty `/output`.
fn write_input_tar<W: std::io::Write>(
    w: W,
    shared: &[FileEntry],
    own: &[FileEntry],
    staged: &[(String, std::path::PathBuf, u64)],
) -> Result<()> {
    let mut b = tar::Builder::new(w);
    b.mode(tar::HeaderMode::Deterministic);
    let dir = |b: &mut tar::Builder<W>, path: &str, mode: u32| -> Result<()> {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Directory);
        h.set_mode(mode);
        h.set_size(0);
        b.append_data(&mut h, path, std::io::empty())?;
        Ok(())
    };
    dir(&mut b, "input/", 0o755)?;
    dir(&mut b, "output/", 0o777)?;
    let mut dirs = std::collections::BTreeSet::new();
    let mut parents = |b: &mut tar::Builder<W>, path: &str| -> Result<()> {
        let mut parent = String::new();
        let parts: Vec<&str> = path.split('/').collect();
        for part in &parts[..parts.len() - 1] {
            parent.push_str(part);
            parent.push('/');
            if dirs.insert(parent.clone()) {
                dir(b, &format!("input/{parent}"), 0o755)?;
            }
        }
        Ok(())
    };
    let mut latest = std::collections::BTreeMap::new();
    for f in shared.iter().chain(own) {
        validate_rel_path(&f.path)?;
        latest.insert(f.path.as_str(), &f.data);
    }
    for (path, file, size) in staged {
        if latest.contains_key(path.as_str()) {
            continue;
        }
        validate_rel_path(path)?;
        parents(&mut b, path)?;
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_mode(0o644);
        h.set_size(*size);
        let f = std::fs::File::open(file).with_context(|| format!("read {}", file.display()))?;
        b.append_data(
            &mut h,
            format!("input/{path}"),
            std::io::Read::take(f, *size),
        )?;
    }
    for (path, data) in latest {
        parents(&mut b, path)?;
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_mode(0o644);
        h.set_size(data.len() as u64);
        b.append_data(&mut h, format!("input/{path}"), data.as_slice())?;
    }
    b.into_inner()?;
    Ok(())
}

/// Writes the regular files under `output/` of a tar stream into `dir`,
/// within the file store's output limits. Returns how many were left out.
pub(crate) fn extract_output_tar<R: std::io::Read>(r: R, dir: &std::path::Path) -> Result<u32> {
    let mut ar = tar::Archive::new(r);
    let mut total = 0u64;
    let mut kept = 0usize;
    let mut dropped = 0u32;
    for entry in ar.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let name = entry.path()?.to_string_lossy().replace('\\', "/");
        let Some(rel) = name.strip_prefix("output/") else {
            continue;
        };
        let rel = rel.to_string();
        if validate_rel_path(&rel).is_err() {
            continue;
        }
        let size = entry.header().size()?;
        if total + size > MAX_STORE_OUTPUT_BYTES
            || kept >= pumasi_proto::blobs::MAX_COLLECTION_ENTRIES
        {
            dropped += 1;
            continue;
        }
        let target = dir.join(&rel);
        if let Some(p) = target.parent() {
            std::fs::create_dir_all(p)?;
        }
        let mut f = std::fs::File::create(&target)?;
        let n = std::io::copy(&mut std::io::Read::take(&mut entry, size), &mut f)?;
        total += n;
        kept += 1;
    }
    Ok(dropped)
}

/// The regular files under `output/` in a `docker cp` tar stream, sorted by
/// path, at most [`MAX_OUTPUT_FILES_BYTES`] in total; `true` if some were
/// left out. A cut-off stream yields the files before the cut.
fn parse_output_tar(tar: &[u8]) -> (Vec<FileEntry>, bool) {
    use std::io::Read;
    let mut files = Vec::new();
    let mut dropped = false;
    let mut total = 0usize;
    let mut ar = tar::Archive::new(tar);
    let Ok(entries) = ar.entries() else {
        return (files, false);
    };
    for entry in entries {
        let Ok(mut entry) = entry else { break };
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let Ok(path) = entry.path() else { continue };
        let name = path.to_string_lossy().replace('\\', "/");
        let Some(rel) = name.strip_prefix("output/") else {
            continue;
        };
        let rel = rel.to_string();
        if validate_rel_path(&rel).is_err() {
            continue;
        }
        let size = entry.header().size().unwrap_or(u64::MAX);
        if total as u64 + size > MAX_OUTPUT_FILES_BYTES as u64 {
            dropped = true;
            continue;
        }
        let mut data = Vec::with_capacity(size as usize);
        if entry.read_to_end(&mut data).is_err() {
            dropped = true;
            break;
        }
        total += data.len();
        files.push(FileEntry { path: rel, data });
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    (files, dropped)
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

/// Like [`read_capped`], and passes what it reads on to the task's live
/// output as it comes.
async fn read_capped_live<R: AsyncRead + Unpin>(
    r: &mut R,
    cap: usize,
    live: Option<&LiveOutput>,
    stderr: bool,
) -> Vec<u8> {
    let mut kept = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let room = cap.saturating_sub(kept.len());
                let take = &buf[..n.min(room)];
                if let Some(l) = live {
                    l.push(stderr, take);
                }
                kept.extend_from_slice(take);
            }
        }
    }
    kept
}

/// Like [`read_capped`], and says whether the stream went past `cap`
/// (reading stops there).
async fn read_capped_flag<R: AsyncRead + Unpin>(r: &mut R, cap: usize) -> (Vec<u8>, bool) {
    let mut kept = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => return (kept, false),
            Ok(n) => {
                if kept.len() + n > cap {
                    kept.extend_from_slice(&buf[..cap - kept.len()]);
                    return (kept, true);
                }
                kept.extend_from_slice(&buf[..n]);
            }
        }
    }
}

fn rand_u32() -> u32 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::SystemTime::now()) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_os() {
        assert_eq!(parse_server("27.3.1 linux"), ("27.3.1", "linux"));
        assert_eq!(parse_server("29.7.2 windows"), ("29.7.2", "windows"));
        assert_eq!(
            parse_server("20.10.0 <no value>"),
            ("20.10.0 <no value>", "linux")
        );
        assert_eq!(parse_server("20.10.0"), ("20.10.0", "linux"));
    }

    #[test]
    fn input_and_output_tars() {
        let f = |p: &str, d: &str| FileEntry {
            path: p.into(),
            data: d.as_bytes().to_vec(),
        };
        let tar = input_tar(
            &[f("a/b.txt", "shared"), f("c", "1")],
            &[f("a/b.txt", "own")],
        )
        .unwrap();
        let mut seen = Vec::new();
        let mut ar = tar::Archive::new(tar.as_slice());
        for e in ar.entries().unwrap() {
            let mut e = e.unwrap();
            let mut body = String::new();
            std::io::Read::read_to_string(&mut e, &mut body).unwrap();
            let mode = e.header().mode().unwrap();
            seen.push(format!("{} {mode:o} {body}", e.path().unwrap().display()));
        }
        assert_eq!(
            seen,
            [
                "input/ 755 ",
                "output/ 777 ",
                "input/a/ 755 ",
                "input/a/b.txt 644 own",
                "input/c 644 1",
            ]
        );
        assert!(input_tar(&[f("../x", "")], &[]).is_err());

        // What `docker cp name:/output -` sends back.
        let mut b = tar::Builder::new(Vec::new());
        let mut add = |path: &str, data: &[u8], kind: tar::EntryType| {
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(kind);
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            b.append_data(&mut h, path, data).unwrap();
        };
        add("output/", b"", tar::EntryType::Directory);
        add("output/z.txt", b"zz", tar::EntryType::Regular);
        add("output/d/a.txt", b"aa", tar::EntryType::Regular);
        add("input/x", b"no", tar::EntryType::Regular);
        let big = vec![0u8; MAX_OUTPUT_FILES_BYTES];
        add("output/huge", &big, tar::EntryType::Regular);
        let tar = b.into_inner().unwrap();
        let (files, dropped) = parse_output_tar(&tar);
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["d/a.txt", "z.txt"]);
        assert!(dropped);
        // A stream cut off mid-file keeps what came before.
        let (files, _) = parse_output_tar(&tar[..3000]);
        assert_eq!(files.len(), 2);
    }

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
        let a = create_args("c1", "node", &spec, &input, &limits, None);
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
            Some(2),
        )
        .join(" ");
        assert!(joined.contains("--network bridge"));
        assert!(joined.contains("--gpus all"));
        assert!(joined.contains("--cpus 2.000"));
    }

    #[test]
    fn gpus_become_device_options() {
        use pumasi_proto::gpu::{GpuInfo, GpuVendor};
        let g = |index, vendor, uuid: &str, render: &str| GpuInfo {
            index,
            vendor,
            name: "g".into(),
            uuid: uuid.into(),
            pci: String::new(),
            vram_mib: 0,
            driver: String::new(),
            compute: String::new(),
            display: false,
            fp32_gflops: 0,
            measured: false,
            mem_bw_gb_s: 0,
            hash_rate: 0,
            docker: true,
            vfio: false,
            render_node: render.into(),
        };
        assert!(gpu_args(&[], &[], true).is_empty());
        let two = [
            g(0, GpuVendor::Nvidia, "GPU-a", ""),
            g(2, GpuVendor::Nvidia, "GPU-b", ""),
        ];
        let a = gpu_args(&two, &[], true).join(" ");
        assert_eq!(a, "--gpus \"device=GPU-a,GPU-b\" --env PUMASI_GPUS=0,2");
        // Graphics: the toolkit's capabilities, compute and utility kept.
        let graphics = [pumasi_proto::job::GpuCapability::Graphics];
        let a = gpu_args(&two, &graphics, false).join(" ");
        assert_eq!(
            a,
            "--gpus \"device=GPU-a,GPU-b\" --env NVIDIA_DRIVER_CAPABILITIES=compute,utility,graphics --env PUMASI_GPUS=0,2"
        );
        // Docker Desktop: graphics tasks borrow the WSL driver libraries.
        let a = gpu_args(&two, &graphics, true).join(" ");
        assert_eq!(
            a,
            "--gpus \"device=GPU-a,GPU-b\" --env NVIDIA_DRIVER_CAPABILITIES=compute,utility,graphics --volume /usr/lib/wsl:/usr/lib/wsl:ro --env PUMASI_GPUS=0,2"
        );
        let video = [pumasi_proto::job::GpuCapability::Video];
        assert!(
            !gpu_args(&two, &video, true)
                .join(" ")
                .contains("/usr/lib/wsl")
        );
        let a = gpu_args(
            &[g(1, GpuVendor::Amd, "", "/dev/dri/renderD129")],
            &graphics,
            true,
        )
        .join(" ");
        assert!(
            a.starts_with("--device /dev/kfd --device /dev/dri/renderD129"),
            "{a}"
        );
        assert!(a.ends_with("--env PUMASI_GPUS=1"));
        assert!(!a.contains("/usr/lib/wsl"));
        // Test GPUs (vendor Other) only get the variable.
        assert_eq!(
            gpu_args(&[g(0, GpuVendor::Other, "", "")], &[], true).join(" "),
            "--env PUMASI_GPUS=0"
        );
    }

    #[test]
    fn image_digests_prefer_the_images_own_repository() {
        let d = r#"["mirror.example/library/python@sha256:aa","python@sha256:bb"] sha256:cc"#;
        assert_eq!(
            pick_digest("python:3.12", d).as_deref(),
            Some("python@sha256:bb")
        );
        assert_eq!(
            pick_digest("docker.io/library/python:3.12", d).as_deref(),
            Some("python@sha256:bb")
        );
        assert_eq!(
            pick_digest("other:1", d).as_deref(),
            Some("mirror.example/library/python@sha256:aa")
        );
        // Built here: no registry digest, the image id.
        assert_eq!(
            pick_digest("mine:dev", "[] sha256:cc").as_deref(),
            Some("sha256:cc")
        );
        assert_eq!(pick_digest("x", "null "), None);
    }
}
