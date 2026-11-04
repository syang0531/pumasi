//! The microVM executor: runs the same OCI images as the Docker executor,
//! but every task boots its own lightweight VM with its own kernel, so the
//! job never touches the host kernel. This is what lets other people's
//! machines run container jobs.
//!
//! Per image (once, cached by manifest digest):
//!
//! 1. resolve the reference and download the layers ([`registry`]),
//! 2. boot a *build* VM that unpacks the layers onto an empty ext4 disk.
//!    The untrusted tarballs are unpacked inside the VM, never on the host.
//!
//! Per task:
//!
//! 1. make an empty ext4 scratch disk (the task's writable layer), a tar
//!    disk with the run spec, stdin and input files, and an output disk,
//! 2. boot a *run* VM: the guest init stacks an overlay of the scratch
//!    disk on the read-only image disk, runs the command, and writes stdout,
//!    stderr, `/output` and the exit status to the output disk,
//! 3. read the output disk back (as data: nothing from it is unpacked).
//!
//! The VM has no network device. Timeouts and cancellation kill the VMM
//! process. See `pumasi_guest` for the disk protocol.

pub mod ext4;
pub mod registry;
pub mod setup;
pub mod vmm;

use std::{
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_guest::{
    BASE_LABEL, DISK_ROLE_ENTRY, GuestOutcome, GuestResult, INPUT_PREFIX, OUTPUT_DISK_BYTES,
    OUTPUT_MARK, OUTPUT_PREFIX, RESULT_ENTRY, ROLE_INPUT, ROLE_LAYERS, RunSpec, SCRATCH_LABEL,
    SPEC_ENTRY, STDERR_ENTRY, STDIN_ENTRY, STDOUT_ENTRY,
};
use pumasi_proto::{
    economy::vcpus,
    job::{
        DockerSpec, FileEntry, Limits, MAX_OUTPUT_BYTES, MAX_OUTPUT_FILES_BYTES, TaskInput,
        validate_rel_path,
    },
    msg::{Outcome, TaskRef},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

pub use registry::{ContainerConfig, Registry};
pub use vmm::{Backend, Disk, VmSpec, Vmm};

use crate::{executor::RunResult, tempdir::TempDir};

/// Guest kernel file name in the VM directory.
pub const KERNEL_FILE: &str = "vmlinux";
/// Guest init file name (in the VM directory or next to the agent).
pub const GUEST_FILE: &str = "pumasi-guest";
/// Firecracker binary name (in the VM directory or on `PATH`).
pub const FIRECRACKER_FILE: &str = "firecracker";

/// Memory the guest kernel and init need on top of the task's limit.
const VM_OVERHEAD_MIB: u64 = 96;
const MIN_VM_MIB: u64 = 128;
/// Size of a task's writable layer. The file is sparse; only what the task
/// writes takes space.
const SCRATCH_BYTES: u64 = 10 << 30;
const BUILD_VM_MIB: u64 = 512;
const BUILD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_VCPUS: u32 = 32;
/// Last part of the VM console kept for error messages.
const CONSOLE_TAIL: usize = 2048;

/// Where the executor finds its kernel and init, and keeps its cache.
#[derive(Debug, Clone)]
pub struct MicrovmConfig {
    /// Holds `vmlinux`, optionally `pumasi-guest` and `firecracker`, and the
    /// image cache (`images/`) and per-task scratch space (`run/`).
    pub dir: PathBuf,
    /// `None` picks Firecracker when it is installed and KVM is usable,
    /// otherwise QEMU.
    pub backend: Option<Backend>,
}

// `EMBEDDED_GUEST`: the guest init built into this binary, if any.
include!(concat!(env!("OUT_DIR"), "/guest.rs"));

/// Where the guest init comes from.
#[derive(Debug, Clone)]
pub enum GuestSource {
    /// Built into this binary (release builds), so it always matches the
    /// agent's version.
    Embedded,
    File(PathBuf),
}

impl std::fmt::Display for GuestSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GuestSource::Embedded => write!(
                f,
                "built into this binary ({} KiB)",
                EMBEDDED_GUEST.map_or(0, |g| g.len() >> 10)
            ),
            GuestSource::File(p) => write!(f, "{}", p.display()),
        }
    }
}

impl GuestSource {
    pub fn read(&self) -> Result<Vec<u8>> {
        match self {
            GuestSource::Embedded => Ok(EMBEDDED_GUEST.unwrap_or_default().to_vec()),
            GuestSource::File(p) => {
                std::fs::read(p).with_context(|| format!("read {}", p.display()))
            }
        }
    }
}

/// Where `pumasi vm setup` installs QEMU on Windows, inside the VM directory.
pub const QEMU_DIR: &str = "qemu";

/// What is installed, for `pumasi vm setup` and error messages.
#[derive(Debug, Clone)]
pub struct Assets {
    pub kernel: Option<PathBuf>,
    pub guest: Option<GuestSource>,
    pub firecracker: Option<PathBuf>,
    pub qemu: Option<PathBuf>,
    pub kvm: bool,
    /// Windows only: whether QEMU can use the Windows Hypervisor Platform,
    /// or why not.
    pub whpx: Option<std::result::Result<(), String>>,
}

impl Assets {
    pub fn find(dir: &Path) -> Self {
        let beside_exe = std::env::current_exe()
            .ok()
            .and_then(|e| e.parent().map(|p| p.join(GUEST_FILE)))
            .filter(|p| p.is_file());
        let guest = if EMBEDDED_GUEST.is_some() {
            Some(GuestSource::Embedded)
        } else {
            Some(dir.join(GUEST_FILE))
                .filter(|p| p.is_file())
                .or(beside_exe)
                .map(GuestSource::File)
        };
        let qemu_extra: &[&str] = if cfg!(windows) {
            &[r"C:\Program Files\qemu"]
        } else {
            &["/opt/homebrew/bin", "/usr/local/bin", "/usr/libexec"]
        };
        Self {
            kernel: Some(dir.join(KERNEL_FILE)).filter(|p| p.is_file()),
            guest,
            firecracker: if cfg!(target_os = "linux") {
                vmm::find_program(FIRECRACKER_FILE, Some(dir), &[])
            } else {
                None
            },
            qemu: vmm::find_program(&vmm::qemu_name(), Some(&dir.join(QEMU_DIR)), qemu_extra),
            kvm: vmm::kvm_usable(),
            whpx: cfg!(windows).then(crate::platform::whpx_status),
        }
    }

    /// Whether QEMU gets a hardware accelerator here.
    pub fn qemu_accelerated(&self) -> bool {
        self.kvm
            || cfg!(target_os = "macos")
            || (cfg!(target_arch = "x86_64") && matches!(self.whpx, Some(Ok(()))))
    }

    /// The VMM to use, or why there is none.
    pub fn pick(&self, want: Option<Backend>) -> Result<Vmm> {
        let firecracker = |bin: &PathBuf| -> Result<Vmm> {
            ensure!(
                self.kvm,
                "Firecracker needs /dev/kvm, which this user cannot open (is KVM enabled, and is the user in the `kvm` group?)"
            );
            Ok(Vmm {
                backend: Backend::Firecracker,
                bin: bin.clone(),
                accelerated: true,
                tsc_khz: None,
            })
        };
        let qemu = |bin: &PathBuf| {
            let accelerated = self.qemu_accelerated();
            Vmm {
                backend: Backend::Qemu,
                bin: bin.clone(),
                accelerated,
                tsc_khz: (cfg!(windows) && accelerated)
                    .then(|| crate::platform::measure_tsc_khz(Duration::from_millis(50)))
                    .flatten(),
            }
        };
        let no_qemu = if cfg!(windows) {
            "QEMU is not installed; run `pumasi vm setup`"
        } else {
            "QEMU is not installed (Linux: the qemu-system package; macOS: `brew install qemu`)"
        };
        match want {
            Some(Backend::Firecracker) => match &self.firecracker {
                Some(b) => firecracker(b),
                None if cfg!(target_os = "linux") => {
                    bail!("Firecracker is not installed; run `pumasi vm setup`")
                }
                None => bail!("Firecracker runs only on Linux; use the qemu backend"),
            },
            Some(Backend::Qemu) => match &self.qemu {
                Some(b) => Ok(qemu(b)),
                None => bail!("{no_qemu}"),
            },
            None => match (&self.firecracker, &self.qemu) {
                (Some(b), _) if self.kvm => firecracker(b),
                (_, Some(b)) => Ok(qemu(b)),
                (Some(b), None) => firecracker(b),
                (None, None) if cfg!(target_os = "linux") => bail!(
                    "no VM monitor found: run `pumasi vm setup` (Firecracker) or install QEMU"
                ),
                (None, None) => bail!("no VM monitor found: {no_qemu}"),
            },
        }
    }
}

/// A pulled image, ready to boot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageEntry {
    digest: String,
    config: ContainerConfig,
    #[serde(skip)]
    rootfs: PathBuf,
}

#[derive(Clone)]
pub struct MicrovmExecutor {
    vmm: Vmm,
    kernel: PathBuf,
    initrd: PathBuf,
    images: PathBuf,
    /// This agent's scratch space (cleared at start).
    run_dir: PathBuf,
    registry: Registry,
    /// One image pull and build at a time.
    pull_lock: Arc<tokio::sync::Mutex<()>>,
}

/// How a VM run ended, as seen from the host.
enum VmEnd {
    /// The guest shut down: the VMM exited by itself (with this status), or
    /// the guest console said it was restarting and the host stopped a VMM
    /// that did not exit.
    Exited(String),
    Timeout,
    Cancelled,
}

/// Lines the guest kernel prints (at emergency level, so even with
/// `quiet`) when it is done.
const GUEST_DONE: &[&str] = &[
    "reboot: Restarting system",
    "reboot: Power down",
    "Kernel panic",
];
/// How long a VMM may keep running after the guest said it was done.
const AFTER_GUEST_DONE: Duration = Duration::from_secs(3);

/// Whether the end of the serial log says the guest is done.
fn guest_said_done(serial: &Path) -> bool {
    use std::io::{Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(serial) else {
        return false;
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let _ = f.seek(SeekFrom::Start(len.saturating_sub(4096)));
    let mut tail = Vec::new();
    let _ = f.read_to_end(&mut tail);
    let tail = String::from_utf8_lossy(&tail);
    GUEST_DONE.iter().any(|l| tail.contains(l))
}

impl MicrovmExecutor {
    /// Checks that a VMM and the guest kernel and init are
    /// available. `label` keeps this agent's scratch space apart.
    pub async fn detect(config: &MicrovmConfig, label: &str) -> Result<Self> {
        std::fs::create_dir_all(&config.dir)
            .with_context(|| format!("create {}", config.dir.display()))?;
        // Task files live about 100 characters below the VM directory, and
        // QEMU on Windows is not long-path aware (260 characters).
        if cfg!(windows) && config.dir.as_os_str().len() > 140 {
            warn!(
                "the VM directory path is long ({} characters); QEMU may fail to open disks under it. Use a shorter --vm-dir",
                config.dir.as_os_str().len()
            );
        }
        let assets = Assets::find(&config.dir);
        let kernel = assets.kernel.clone().with_context(|| {
            format!(
                "no guest kernel at {}; run `pumasi vm setup`",
                config.dir.join(KERNEL_FILE).display()
            )
        })?;
        let guest = assets.guest.clone().with_context(|| {
            format!(
                "no guest init ({GUEST_FILE}) in {} or next to this binary; run `pumasi vm setup --guest <path>`",
                config.dir.display()
            )
        })?;
        let vmm = assets.pick(config.backend)?;
        let init = guest.read()?;
        ensure!(
            init.starts_with(b"\x7fELF"),
            "{guest} is not a Linux binary"
        );
        let cpio = vmm::initramfs(&init);
        let initrd = config.dir.join(format!(
            "initrd-{}.cpio",
            &blake3::hash(&cpio).to_hex()[..16]
        ));
        if !initrd.exists() {
            let tmp = initrd.with_extension(format!("tmp{}", std::process::id()));
            std::fs::write(&tmp, &cpio)?;
            std::fs::rename(&tmp, &initrd)?;
        }
        let images = config.dir.join("images");
        std::fs::create_dir_all(images.join("refs"))?;
        let run_dir = config.dir.join("run").join(label);
        if run_dir.exists() {
            let _ = std::fs::remove_dir_all(&run_dir);
        }
        std::fs::create_dir_all(&run_dir)?;
        info!(
            backend = %vmm.backend,
            vmm = %vmm.bin.display(),
            accelerated = vmm.accelerated,
            "microvm executor enabled"
        );
        if !vmm.accelerated {
            warn!(
                "no hardware virtualization (KVM, HVF or WHPX): microVMs run in software emulation and will be slow"
            );
        }
        Ok(Self {
            vmm,
            kernel,
            initrd,
            images,
            run_dir,
            registry: Registry::new()?,
            pull_lock: Default::default(),
        })
    }

    pub fn backend(&self) -> Backend {
        self.vmm.backend
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
            .run_inner(task, spec, input, shared_files, limits, &cancel)
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
    ) -> Result<RunResult> {
        ensure!(
            !spec.network,
            "microvm tasks have no network; use a docker job"
        );
        ensure!(spec.gpus.is_none(), "microvm tasks cannot use GPUs yet");
        let image = self.ensure_image(&spec.image, cancel).await?;
        // The timeout covers the run itself, not the image pull.
        let run_start = Instant::now();
        let scratch = TempDir::new_in(&self.run_dir, &format!("{}-{}", task.job, task.index))?;
        let dir = scratch.path().to_path_buf();
        let run_spec = run_spec(&image.config, spec, input);
        let disks = {
            let dir = dir.clone();
            let input = input.clone();
            let shared = shared_files.to_vec();
            tokio::task::spawn_blocking(move || -> Result<[PathBuf; 3]> {
                let scratch = dir.join("scratch.ext4");
                ext4::mkfs(&scratch, SCRATCH_BYTES, SCRATCH_LABEL)?;
                let input_disk = dir.join("input.tar");
                write_input_disk(&input_disk, &run_spec, &input.stdin, &shared, &input.files)?;
                let output = dir.join("output.img");
                make_output_disk(&output)?;
                Ok([scratch, input_disk, output])
            })
            .await??
        };
        let [scratch_disk, input_disk, output_disk] = disks;
        let vm = VmSpec {
            kernel: self.kernel.clone(),
            initrd: self.initrd.clone(),
            mode: "run",
            disks: vec![
                Disk {
                    path: image.rootfs.clone(),
                    read_only: true,
                },
                Disk {
                    path: scratch_disk,
                    read_only: false,
                },
                Disk {
                    path: input_disk,
                    read_only: true,
                },
                Disk {
                    path: output_disk.clone(),
                    read_only: false,
                },
            ],
            vcpus: vcpus(limits.cpu_millis).min(MAX_VCPUS),
            mem_mib: (limits.memory_bytes.div_ceil(1 << 20) + VM_OVERHEAD_MIB).max(MIN_VM_MIB),
        };
        let deadline = run_start + Duration::from_millis(limits.timeout_ms);
        debug!(task = %task, image = %spec.image, "booting microvm");
        let end = self.boot(&vm, &dir, Some(deadline), cancel).await?;
        // Billed time starts at boot: pulling an image is not the job's run.
        let wall_ms = run_start.elapsed().as_millis() as u64;
        let outcome_for = |o: Outcome| RunResult {
            outcome: o,
            stdout: vec![],
            stderr: vec![],
            files: vec![],
            fuel_used: 0,
            wall_ms,
            peak_memory_bytes: None,
        };
        if !matches!(end, VmEnd::Exited(_)) {
            debug!(task = %task, console = %console_tail(&dir), "microvm stopped by the host");
        }
        let status = match end {
            VmEnd::Timeout => return Ok(outcome_for(Outcome::Timeout)),
            VmEnd::Cancelled => return Ok(outcome_for(Outcome::Cancelled)),
            VmEnd::Exited(status) => status,
        };
        let Some(out) = read_output_disk(&output_disk)? else {
            bail!(
                "the microVM stopped without a result ({} {status}); console:\n{}",
                self.vmm.backend,
                console_tail(&dir)
            );
        };
        let mut stderr = out.stderr;
        note_dropped(&mut stderr, &out.result);
        let outcome = match out.result.outcome {
            GuestOutcome::Exited(code) => Outcome::Exited(code),
            // Same exit status a shell or `docker run` reports.
            GuestOutcome::Signaled(sig) => Outcome::Exited(128 + sig),
            GuestOutcome::OutOfMemory => Outcome::Trap(format!(
                "out of memory (limit {} MiB)",
                limits.memory_bytes >> 20
            )),
            GuestOutcome::Failed(msg) => Outcome::Error(format!("microVM: {msg}")),
        };
        Ok(RunResult {
            outcome,
            stdout: out.stdout,
            stderr,
            files: out.files,
            fuel_used: 0,
            wall_ms,
            peak_memory_bytes: None,
        })
    }

    /// Boots `vm` and waits for it to power off, killing it at `deadline`
    /// or on cancellation.
    async fn boot(
        &self,
        vm: &VmSpec,
        dir: &Path,
        deadline: Option<Instant>,
        cancel: &AtomicBool,
    ) -> Result<VmEnd> {
        let console = std::fs::File::create(dir.join("console.log"))?;
        let mut cmd = self.vmm.command(vm, dir)?;
        cmd.stdin(std::process::Stdio::null())
            .stdout(console.try_clone()?)
            .stderr(console)
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .with_context(|| format!("start {}", self.vmm.bin.display()))?;
        // Windows: the VMM dies with its job, so not even a crashed agent
        // leaves VMs running.
        let _job = crate::platform::ProcessJob::new()
            .and_then(|job| job.assign(&child).map(|()| job))
            .map_err(|e| warn!("could not put the VMM in a job object: {e}"))
            .ok();
        // When QEMU writes the console to a file (Windows), watch it: under
        // WHPX a guest reset can leave QEMU stuck instead of exiting.
        let serial = dir.join(vmm::SERIAL_LOG);
        let watch_serial = cfg!(windows) && self.vmm.backend == Backend::Qemu;
        let mut done_since: Option<Instant> = None;
        let mut ticks = 0u32;
        loop {
            tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    if !status.success() {
                        debug!(%status, "vmm exited");
                    }
                    return Ok(VmEnd::Exited(status.to_string()));
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    ticks += 1;
                    if watch_serial && done_since.is_none() && ticks.is_multiple_of(10) && guest_said_done(&serial) {
                        done_since = Some(Instant::now());
                    }
                    let end = if cancel.load(Ordering::Relaxed) {
                        Some(VmEnd::Cancelled)
                    } else if deadline.is_some_and(|d| Instant::now() >= d) {
                        Some(VmEnd::Timeout)
                    } else if done_since.is_some_and(|t| t.elapsed() >= AFTER_GUEST_DONE) {
                        warn!("the guest restarted but the VMM did not exit; stopping it");
                        Some(VmEnd::Exited("stopped by the host after the guest restarted".into()))
                    } else {
                        None
                    };
                    if let Some(end) = end {
                        let _ = child.kill().await;
                        return Ok(end);
                    }
                }
            }
        }
    }

    /// The cached image for `reference`, pulling and building it if needed.
    async fn ensure_image(&self, reference: &str, cancel: &AtomicBool) -> Result<ImageEntry> {
        if let Some(e) = self.cached(reference) {
            return Ok(e);
        }
        let _pull = self.pull_lock.lock().await;
        if let Some(e) = self.cached(reference) {
            return Ok(e);
        }
        let resolved = self.registry.resolve(reference).await?;
        if let Some(e) = self.load_image(&resolved.digest) {
            self.save_ref(reference, &resolved.digest)?;
            return Ok(e);
        }
        info!(image = %reference, digest = %resolved.digest, layers = resolved.layers.len(), "pulling image");
        let work = TempDir::new_in(&self.run_dir, "pull")?;
        let mut blobs = Vec::new();
        for (i, layer) in resolved.layers.iter().enumerate() {
            let path = work.path().join(format!("layer-{i:03}"));
            self.registry
                .download(&resolved.image, layer, &path, cancel)
                .await
                .with_context(|| format!("pull {reference}"))?;
            blobs.push(path);
        }
        info!(image = %reference, "unpacking image in a build VM");
        let compressed: u64 = resolved.layers.iter().map(|l| l.size).sum();
        // Layers expand a few times when unpacked; the file is sparse.
        let size = (compressed * 6 + (1 << 30)).max(2 << 30);
        let base = work.path().join("rootfs.ext4");
        let layers_disk = work.path().join("layers.tar");
        let output = work.path().join("output.img");
        {
            let (base, layers_disk, output) = (base.clone(), layers_disk.clone(), output.clone());
            tokio::task::spawn_blocking(move || -> Result<()> {
                ext4::mkfs(&base, size, BASE_LABEL)?;
                write_layers_disk(&layers_disk, &blobs)?;
                for b in &blobs {
                    let _ = std::fs::remove_file(b);
                }
                make_output_disk(&output)
            })
            .await??;
        }
        let vm = VmSpec {
            kernel: self.kernel.clone(),
            initrd: self.initrd.clone(),
            mode: "build",
            disks: vec![
                Disk {
                    path: base.clone(),
                    read_only: false,
                },
                Disk {
                    path: layers_disk.clone(),
                    read_only: true,
                },
                Disk {
                    path: output.clone(),
                    read_only: false,
                },
            ],
            vcpus: 1,
            mem_mib: BUILD_VM_MIB,
        };
        let deadline = Instant::now() + BUILD_TIMEOUT;
        let status = match self.boot(&vm, work.path(), Some(deadline), cancel).await? {
            VmEnd::Exited(status) => status,
            VmEnd::Timeout => bail!("unpacking {reference} took longer than 30 minutes"),
            VmEnd::Cancelled => bail!("cancelled while unpacking {reference}"),
        };
        let result = read_output_disk(&output)?.map(|o| o.result);
        match result.map(|r| r.outcome) {
            Some(GuestOutcome::Exited(0)) => {}
            Some(GuestOutcome::Failed(msg)) => bail!("unpack {reference}: {msg}"),
            other => bail!(
                "unpack {reference}: the build VM ended without success ({other:?}, {} {status}); console:\n{}",
                self.vmm.backend,
                console_tail(work.path())
            ),
        }
        let _ = std::fs::remove_file(&layers_disk);
        let dest = self.image_dir(&resolved.digest);
        let entry = ImageEntry {
            digest: resolved.digest.clone(),
            config: resolved.config.clone(),
            rootfs: dest.join("rootfs.ext4"),
        };
        let staged = work.path().join("image");
        std::fs::create_dir_all(&staged)?;
        std::fs::rename(&base, staged.join("rootfs.ext4"))?;
        std::fs::write(staged.join("image.json"), serde_json::to_vec(&entry)?)?;
        if let Err(e) = std::fs::rename(&staged, &dest) {
            // Another agent sharing this directory finished first.
            if self.load_image(&resolved.digest).is_none() {
                return Err(e).context("store image");
            }
        }
        self.save_ref(reference, &resolved.digest)?;
        info!(image = %reference, "image ready");
        self.load_image(&resolved.digest)
            .context("image vanished after building it")
    }

    fn image_dir(&self, digest: &str) -> PathBuf {
        self.images.join(digest.replace(':', "-"))
    }

    fn ref_path(&self, reference: &str) -> PathBuf {
        self.images
            .join("refs")
            .join(&blake3::hash(reference.trim().as_bytes()).to_hex()[..32])
    }

    fn save_ref(&self, reference: &str, digest: &str) -> Result<()> {
        std::fs::write(self.ref_path(reference), digest)?;
        Ok(())
    }

    /// The image a reference was last resolved to, like `docker run` uses a
    /// local image without pulling again.
    fn cached(&self, reference: &str) -> Option<ImageEntry> {
        let digest = std::fs::read_to_string(self.ref_path(reference)).ok()?;
        self.load_image(digest.trim())
    }

    fn load_image(&self, digest: &str) -> Option<ImageEntry> {
        let dir = self.image_dir(digest);
        let mut e: ImageEntry =
            serde_json::from_slice(&std::fs::read(dir.join("image.json")).ok()?).ok()?;
        e.rootfs = dir.join("rootfs.ext4");
        e.rootfs.is_file().then_some(e)
    }
}

/// What the guest runs: the image's entrypoint, then the job's command and
/// task args (or the image's `Cmd` when both are empty), like `docker run`.
fn run_spec(config: &ContainerConfig, spec: &DockerSpec, input: &TaskInput) -> RunSpec {
    let override_cmd: Vec<String> = spec
        .command
        .iter()
        .chain(input.args.iter())
        .cloned()
        .collect();
    let cmd = if override_cmd.is_empty() {
        config.cmd.clone().unwrap_or_default()
    } else {
        override_cmd
    };
    let mut argv = config.entrypoint.clone().unwrap_or_default();
    argv.extend(cmd);
    let mut env: Vec<String> = config.env.clone().unwrap_or_default();
    let mut set = |k: &str, v: &str| {
        let prefix = format!("{k}=");
        env.retain(|e| !e.starts_with(&prefix));
        env.push(format!("{k}={v}"));
    };
    set("HOSTNAME", "pumasi");
    for (k, v) in &input.env {
        set(k, v);
    }
    RunSpec {
        argv,
        env,
        workdir: pumasi_proto::job::INPUT_DIR.into(),
        user: config.user.clone().unwrap_or_default(),
        hostname: "pumasi".into(),
    }
}

fn tar_entry<W: Write>(b: &mut tar::Builder<W>, name: &str, data: &[u8]) -> Result<()> {
    let mut h = tar::Header::new_gnu();
    h.set_size(data.len() as u64);
    h.set_mode(0o644);
    h.set_entry_type(tar::EntryType::Regular);
    b.append_data(&mut h, name, data)?;
    Ok(())
}

/// Pads a disk image to whole 512-byte sectors.
fn pad_to_sector(path: &Path) -> Result<()> {
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    let len = f.metadata()?.len();
    let padded = len.div_ceil(4096).max(1) * 4096;
    if padded != len {
        f.set_len(padded)?;
    }
    Ok(())
}

fn write_layers_disk(path: &Path, blobs: &[PathBuf]) -> Result<()> {
    let f = std::fs::File::create(path)?;
    let mut b = tar::Builder::new(std::io::BufWriter::with_capacity(1 << 20, f));
    b.mode(tar::HeaderMode::Deterministic);
    tar_entry(&mut b, DISK_ROLE_ENTRY, ROLE_LAYERS.as_bytes())?;
    for (i, blob) in blobs.iter().enumerate() {
        let mut f = std::fs::File::open(blob)?;
        b.append_file(format!("layer-{i:03}"), &mut f)?;
    }
    b.into_inner()?.flush()?;
    pad_to_sector(path)
}

fn write_input_disk(
    path: &Path,
    spec: &RunSpec,
    stdin: &[u8],
    shared: &[FileEntry],
    own: &[FileEntry],
) -> Result<()> {
    let f = std::fs::File::create(path)?;
    let mut b = tar::Builder::new(std::io::BufWriter::with_capacity(1 << 20, f));
    b.mode(tar::HeaderMode::Deterministic);
    tar_entry(&mut b, DISK_ROLE_ENTRY, ROLE_INPUT.as_bytes())?;
    tar_entry(&mut b, SPEC_ENTRY, &serde_json::to_vec(spec)?)?;
    tar_entry(&mut b, STDIN_ENTRY, stdin)?;
    // The task's own files come last and win over shared ones.
    for f in shared.iter().chain(own) {
        validate_rel_path(&f.path)?;
        tar_entry(&mut b, &format!("{INPUT_PREFIX}{}", f.path), &f.data)?;
    }
    b.into_inner()?.flush()?;
    pad_to_sector(path)
}

fn make_output_disk(path: &Path) -> Result<()> {
    let mut f = create_sparse(path, OUTPUT_DISK_BYTES.div_ceil(4096) * 4096)?;
    f.write_all(OUTPUT_MARK)?;
    Ok(())
}

struct GuestOutput {
    result: GuestResult,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    files: Vec<FileEntry>,
}

/// Parses the output disk. `None` if the guest never wrote it. The guest
/// is untrusted: entries are only read into memory, within the caps.
fn read_output_disk(path: &Path) -> Result<Option<GuestOutput>> {
    let mut f = std::fs::File::open(path)?;
    let mut head = vec![0u8; OUTPUT_MARK.len()];
    f.read_exact(&mut head)?;
    if head == OUTPUT_MARK {
        return Ok(None);
    }
    let f = std::fs::File::open(path)?;
    let mut ar = tar::Archive::new(BufReader::new(f));
    let mut result = None;
    let (mut stdout, mut stderr, mut files) = (Vec::new(), Vec::new(), Vec::new());
    let mut total = 0usize;
    let read = |e: &mut tar::Entry<_>, cap: usize| -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        e.take(cap as u64).read_to_end(&mut buf)?;
        Ok(buf)
    };
    for entry in ar.entries().context("read output disk")? {
        let mut entry = entry.context("read output disk")?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let name = entry.path()?.to_string_lossy().into_owned();
        if name == RESULT_ENTRY {
            result = Some(
                serde_json::from_slice::<GuestResult>(&read(&mut entry, 64 * 1024)?)
                    .context("bad result from the guest")?,
            );
        } else if name == STDOUT_ENTRY {
            stdout = read(&mut entry, MAX_OUTPUT_BYTES)?;
        } else if name == STDERR_ENTRY {
            stderr = read(&mut entry, MAX_OUTPUT_BYTES)?;
        } else if let Some(rel) = name.strip_prefix(OUTPUT_PREFIX) {
            if validate_rel_path(rel).is_err() {
                continue;
            }
            let size = entry.header().size()? as usize;
            if total + size > MAX_OUTPUT_FILES_BYTES {
                continue;
            }
            let data = read(&mut entry, size)?;
            total += data.len();
            files.push(FileEntry {
                path: rel.to_string(),
                data,
            });
        }
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let result = result.context("the guest's result is incomplete")?;
    Ok(Some(GuestOutput {
        result,
        stdout,
        stderr,
        files,
    }))
}

fn note_dropped(stderr: &mut Vec<u8>, r: &GuestResult) {
    if r.stdout_dropped > 0 || r.stderr_dropped > 0 {
        stderr.extend_from_slice(
            format!(
                "\n[pumasi] output over {} MiB was dropped ({} bytes of stdout, {} of stderr)\n",
                MAX_OUTPUT_BYTES >> 20,
                r.stdout_dropped,
                r.stderr_dropped
            )
            .as_bytes(),
        );
    }
    if r.files_dropped > 0 {
        stderr.extend_from_slice(
            format!(
                "\n[pumasi] output files exceeded {} MiB; {} were dropped\n",
                MAX_OUTPUT_FILES_BYTES >> 20,
                r.files_dropped
            )
            .as_bytes(),
        );
    }
}

/// Creates `path` as a sparse file of `len` bytes (zeros that take no
/// space until written; NTFS needs to be asked for that).
pub(crate) fn create_sparse(path: &Path, len: u64) -> Result<std::fs::File> {
    let f = std::fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
    if let Err(e) = crate::platform::set_sparse(&f) {
        warn!(
            "{} is not sparse and takes its full size: {e}",
            path.display()
        );
    }
    f.set_len(len)
        .with_context(|| format!("size {} to {len} bytes", path.display()))?;
    Ok(f)
}

fn console_tail(dir: &Path) -> String {
    // The VMM's own output, then the guest console when QEMU writes that
    // to a file (Windows).
    let mut text = std::fs::read(dir.join("console.log")).unwrap_or_default();
    if let Ok(serial) = std::fs::read(dir.join(vmm::SERIAL_LOG)) {
        text.extend_from_slice(&serial);
    }
    let start = text.len().saturating_sub(CONSOLE_TAIL);
    String::from_utf8_lossy(&text[start..]).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_match_the_guest() {
        assert_eq!(pumasi_guest::MAX_STREAM_BYTES as usize, MAX_OUTPUT_BYTES);
        assert_eq!(
            pumasi_guest::MAX_OUTPUT_FILES_BYTES as usize,
            MAX_OUTPUT_FILES_BYTES
        );
    }

    #[test]
    fn run_spec_follows_docker_semantics() {
        let config = ContainerConfig {
            env: Some(vec!["PATH=/opt/bin:/usr/bin".into(), "A=image".into()]),
            entrypoint: Some(vec!["/entry".into()]),
            cmd: Some(vec!["default".into()]),
            working_dir: Some("/app".into()),
            user: Some("app".into()),
        };
        let spec = DockerSpec {
            image: "x".into(),
            ..Default::default()
        };
        let input = TaskInput {
            env: [("A".to_string(), "job".to_string())].into(),
            ..Default::default()
        };
        let s = run_spec(&config, &spec, &input);
        assert_eq!(s.argv, vec!["/entry", "default"]);
        assert!(s.env.contains(&"A=job".to_string()));
        assert!(!s.env.contains(&"A=image".to_string()));
        assert!(s.env.contains(&"PATH=/opt/bin:/usr/bin".to_string()));
        assert_eq!(s.workdir, "/input");
        assert_eq!(s.user, "app");
        let spec = DockerSpec {
            command: vec!["python".into()],
            ..spec
        };
        let input = TaskInput {
            args: vec!["main.py".into()],
            ..Default::default()
        };
        assert_eq!(
            run_spec(&config, &spec, &input).argv,
            vec!["/entry", "python", "main.py"]
        );
    }

    #[test]
    fn output_disk_roundtrip() {
        let dir = TempDir::new("microvm-test").unwrap();
        let path = dir.path().join("out.img");
        make_output_disk(&path).unwrap();
        assert!(read_output_disk(&path).unwrap().is_none(), "empty disk");
        // Write what a guest would, including entries the host must ignore.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let mut b = tar::Builder::new(f);
        tar_entry(&mut b, STDOUT_ENTRY, b"hello\n").unwrap();
        tar_entry(&mut b, "output/b.txt", b"b").unwrap();
        tar_entry(&mut b, "output/a/x.txt", b"x").unwrap();
        // A hostile guest can write names tar's builder would refuse.
        let mut h = tar::Header::new_old();
        h.as_old_mut().name[..16].copy_from_slice(b"output/../escape");
        h.set_size(2);
        h.set_mode(0o644);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        b.append(&h, &b"no"[..]).unwrap();
        let result = GuestResult {
            outcome: GuestOutcome::Exited(3),
            stdout_dropped: 0,
            stderr_dropped: 5,
            files_dropped: 0,
        };
        tar_entry(&mut b, RESULT_ENTRY, &serde_json::to_vec(&result).unwrap()).unwrap();
        b.into_inner().unwrap();
        let out = read_output_disk(&path).unwrap().unwrap();
        assert_eq!(out.result, result);
        assert_eq!(out.stdout, b"hello\n");
        let paths: Vec<&str> = out.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["a/x.txt", "b.txt"]);
    }

    #[test]
    fn input_disk_has_role_spec_and_files() {
        let dir = TempDir::new("microvm-test").unwrap();
        let path = dir.path().join("in.tar");
        let spec = RunSpec {
            argv: vec!["true".into()],
            ..Default::default()
        };
        let shared = vec![FileEntry {
            path: "a.txt".into(),
            data: b"shared".to_vec(),
        }];
        write_input_disk(&path, &spec, b"in", &shared, &[]).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len() % 4096, 0);
        let mut ar = tar::Archive::new(std::fs::File::open(&path).unwrap());
        let names: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec![DISK_ROLE_ENTRY, SPEC_ENTRY, STDIN_ENTRY, "input/a.txt"]
        );
        let bad = vec![FileEntry {
            path: "../x".into(),
            data: vec![],
        }];
        assert!(write_input_disk(&path, &spec, b"", &bad, &[]).is_err());
    }
}
