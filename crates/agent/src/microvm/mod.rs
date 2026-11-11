//! The microVM executor: runs the same OCI images as the Docker executor,
//! but every task boots its own lightweight VM with its own kernel, so the
//! job never touches the host kernel. This is what lets other people's
//! machines run container jobs.
//!
//! Per image (once, cached by manifest digest):
//!
//! 1. resolve the reference and download the layers
//!    ([`pumasi_proto::registry`]), or take them from the cluster's file
//!    store for a private image,
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
//! The VM has no network device unless the job asks for one
//! (`network = true`): then QEMU or vz gives it a virtio-net card whose
//! other end is the agent's filtering gateway ([`net`]), which lets it
//! reach public addresses only. Timeouts and cancellation kill the VMM
//! process. See `pumasi_guest` for the disk protocol.

pub mod ext4;
pub mod net;
pub mod setup;
pub mod tee;
pub mod vmm;
pub mod vz;

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
    BASE_LABEL, GuestOutcome, OUTPUT_DISK_BYTES, OUTPUT_MARK, RunSpec, SCRATCH_LABEL,
};
use pumasi_proto::{
    economy::vcpus,
    job::{DockerSpec, FileEntry, Limits, TaskInput},
    msg::{Outcome, TaskRef},
    store::{IMAGE_MANIFEST, ImageManifest, MAX_STORE_OUTPUT_BYTES, image_layer_name},
    vmdisk::{
        GuestOutput, note_dropped, outcome_of, parse_output, run_spec, write_input_tar,
        write_layers_tar,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

pub use pumasi_proto::registry::{ContainerConfig, Registry};
pub use vmm::{Backend, Disk, TeeVm, VmSpec, Vmm};

use crate::{
    executor::{ConfidentialIo, LocalImage, RunResult, TaskIo},
    tempdir::TempDir,
};

/// Guest kernel file name in the VM directory. macOS needs another kernel
/// (virtio-pci for Virtualization.framework, a bzImage on Intel), so it has
/// its own name there and a phase 3 install is not mistaken for it.
pub const KERNEL_FILE: &str = if cfg!(target_os = "macos") {
    "vz-kernel"
} else {
    "vmlinux"
};
/// Guest init file name (in the VM directory or next to the agent).
pub const GUEST_FILE: &str = "pumasi-guest";
/// The GPU guest kernel and driver disk (`pumasi vm setup --gpu-bundle`),
/// in this subdirectory of the VM directory.
pub const GPU_DIR: &str = "gpu";
pub const GPU_KERNEL_FILE: &str = "vmlinux";
pub const GPU_DRIVER_FILE: &str = "drivers.ext4";
/// Memory a GPU VM needs on top of the task's limit (the driver).
const GPU_VM_OVERHEAD_MIB: u64 = 512;
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
/// What the probe VM copies, and how long it may take.
const PROBE_BYTES: usize = 4 << 20;
const PROBE_TIMEOUT: Duration = Duration::from_secs(120);

/// Where the executor finds its kernel and init, and keeps its cache.
#[derive(Debug, Clone)]
pub struct MicrovmConfig {
    /// Holds `vmlinux`, optionally `pumasi-guest` and `firecracker`, and the
    /// image cache (`images/`) and per-task scratch space (`run/`).
    pub dir: PathBuf,
    /// `None` picks Firecracker when it is installed and KVM is usable,
    /// otherwise QEMU.
    pub backend: Option<Backend>,
    /// Confidential VMs (phase 16).
    pub confidential: tee::TeeOptions,
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
    /// macOS only: whether Virtualization.framework can run VMs, or why
    /// not.
    pub vz: Option<std::result::Result<(), String>>,
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
            vz: cfg!(target_os = "macos").then(vz::status),
        }
    }

    /// Whether QEMU gets a hardware accelerator here.
    pub fn qemu_accelerated(&self) -> bool {
        self.kvm
            || cfg!(target_os = "macos")
            || (cfg!(target_arch = "x86_64") && matches!(self.whpx, Some(Ok(()))))
    }

    /// The VMM to use, or why there is none. `dir` is the VM directory,
    /// where the Virtualization.framework helper lives.
    pub fn pick(&self, want: Option<Backend>, dir: &Path) -> Result<Vmm> {
        let vz = || -> Result<Vmm> {
            match &self.vz {
                None => bail!("the vz backend (Virtualization.framework) runs only on macOS"),
                Some(Err(why)) => bail!("{why}"),
                Some(Ok(())) => Ok(Vmm {
                    backend: Backend::Vz,
                    bin: vz::ensure_helper(dir)?,
                    accelerated: true,
                    tsc_khz: None,
                }),
            }
        };
        if want == Some(Backend::Vz) || (want.is_none() && matches!(self.vz, Some(Ok(())))) {
            return vz();
        }
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
            Some(Backend::Vz) => unreachable!("handled above"),
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
                (None, None) if cfg!(target_os = "macos") => {
                    let why = match &self.vz {
                        Some(Err(why)) => why.clone(),
                        _ => "Virtualization.framework is not usable".into(),
                    };
                    bail!("no VM monitor: {why}")
                }
                (None, None) => bail!("no VM monitor found: {no_qemu}"),
            },
        }
    }
}

/// The GPU guest kernel, its driver disk, and QEMU with KVM to boot them.
#[derive(Debug, Clone)]
struct GpuAssets {
    kernel: PathBuf,
    drivers: PathBuf,
    vmm: Vmm,
}

impl GpuAssets {
    fn find(dir: &Path, assets: &Assets) -> Option<Self> {
        if !cfg!(target_os = "linux") || !assets.kvm {
            return None;
        }
        let gpu = dir.join(GPU_DIR);
        let kernel = Some(gpu.join(GPU_KERNEL_FILE)).filter(|p| p.is_file())?;
        let drivers = Some(gpu.join(GPU_DRIVER_FILE)).filter(|p| p.is_file())?;
        let vmm = Vmm {
            backend: Backend::Qemu,
            bin: assets.qemu.clone()?,
            accelerated: true,
            tsc_khz: None,
        };
        Some(Self {
            kernel,
            drivers,
            vmm,
        })
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
    /// The VMM for VMs with a network card (QEMU or vz: the main one; or
    /// the QEMU next to Firecracker), if any.
    net_vmm: Option<Vmm>,
    /// What GPU passthrough needs, when it is all there.
    gpu: Option<GpuAssets>,
    kernel: PathBuf,
    initrd: PathBuf,
    images: PathBuf,
    /// This agent's scratch space (cleared at start).
    run_dir: PathBuf,
    registry: Registry,
    /// One image pull and build at a time.
    pull_lock: Arc<tokio::sync::Mutex<()>>,
    /// The TEEs confidential tasks can run in here, and QEMU for them.
    tee: tee::TeeAssets,
    tee_vmm: Option<Vmm>,
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
    pumasi_guest::DONE_LINE,
    "reboot: Restarting system",
    "reboot: Power down",
    "Kernel panic",
];
/// How long a VMM may keep running after the guest said it was done.
const AFTER_GUEST_DONE: Duration = Duration::from_secs(3);

/// Whether the serial log says the guest is done, looking at what was
/// written since `*seen` (bytes already read). Everything new is read, so
/// a guest that boots again right away (and buries the line) is caught.
fn guest_said_done(serial: &Path, seen: &mut u64) -> bool {
    use std::io::{Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(serial) else {
        return false;
    };
    // A little overlap, for a line split across two reads.
    let from = seen.saturating_sub(64);
    if f.seek(SeekFrom::Start(from)).is_err() {
        return false;
    }
    let mut new = Vec::new();
    let _ = f.read_to_end(&mut new);
    *seen = from + new.len() as u64;
    let new = String::from_utf8_lossy(&new);
    GUEST_DONE.iter().any(|l| new.contains(l))
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
        // An upgrade can pin a new macOS guest kernel; fetch it here so
        // agents need no second `pumasi vm setup`.
        match setup::refresh_vz_kernel(&config.dir).await {
            Ok(Some(k)) => info!("replaced the guest kernel with the pinned one: {k}"),
            Ok(None) => {}
            Err(e) => {
                warn!("could not replace an older guest kernel (run `pumasi vm setup`): {e:#}")
            }
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
        let vmm = assets.pick(config.backend, &config.dir)?;
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
            // Unique per executor: several can start at once in a process.
            let tmp = initrd.with_extension(format!(
                "tmp{}-{:x}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
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
        // Firecracker's network cards need TAP devices (root); QEMU's and
        // vz's talk to the gateway, so network tasks on Firecracker nodes
        // use QEMU when it is there.
        let net_vmm = match vmm.backend {
            Backend::Firecracker => assets.qemu.clone().map(|bin| Vmm {
                backend: Backend::Qemu,
                bin,
                accelerated: assets.kvm,
                tsc_khz: None,
            }),
            // QEMU's is set below, after the WHPX probe may change it.
            Backend::Qemu => None,
            Backend::Vz => Some(vmm.clone()),
        };
        let gpu = GpuAssets::find(&config.dir, &assets);
        let tee = tee::TeeAssets::detect(
            &config.confidential,
            &config.dir,
            assets.qemu.is_some(),
            assets.kvm,
        );
        if let Some(why) = &tee.unavailable {
            warn!("confidential VMs unavailable: {why}");
        }
        let tee_vmm = assets.qemu.clone().filter(|_| assets.kvm).map(|bin| Vmm {
            backend: Backend::Qemu,
            bin,
            accelerated: true,
            tsc_khz: None,
        });
        let mut exec = Self {
            vmm,
            net_vmm,
            gpu,
            kernel,
            initrd,
            images,
            run_dir,
            registry: Registry::new()?,
            pull_lock: Default::default(),
            tee,
            tee_vmm,
        };
        // WHPX is not equally good everywhere: nested under Hyper-V (cloud
        // Windows machines, CI runners) QEMU's virtio disks can break under
        // it. One tiny VM tells, and the slow software emulation still works.
        let accel_override = std::env::var("PUMASI_QEMU_ACCEL").ok();
        if exec.vmm.backend == Backend::Qemu
            && accel_override
                .as_deref()
                .is_some_and(|a| a.starts_with("tcg"))
        {
            // Software emulation asked for: the guest gets the settings
            // that go with it (fixed TSC rate, emulated timers).
            exec.vmm.accelerated = false;
            exec.vmm.tsc_khz = None;
        }
        let whpx = cfg!(windows) && exec.vmm.backend == Backend::Qemu && exec.vmm.accelerated;
        if whpx && accel_override.is_none() {
            let start = Instant::now();
            match exec.probe().await {
                Ok(()) => debug!(
                    ms = start.elapsed().as_millis() as u64,
                    "WHPX probe VM passed"
                ),
                Err(e) => {
                    warn!(
                        "microVMs do not work under WHPX on this machine, so they run in software emulation (slow); set PUMASI_QEMU_ACCEL=whpx to use WHPX anyway. The probe VM failed: {e:#}"
                    );
                    exec.vmm.accelerated = false;
                    exec.vmm.tsc_khz = None;
                }
            }
        }
        if exec.vmm.backend == Backend::Qemu {
            exec.net_vmm = Some(exec.vmm.clone());
        }
        let vmm = &exec.vmm;
        info!(
            backend = %vmm.backend,
            vmm = %vmm.bin.display(),
            accelerated = vmm.accelerated,
            network = exec.net_vmm.is_some(),
            gpu_passthrough = exec.gpu.is_some(),
            "microvm executor enabled"
        );
        if !vmm.accelerated && !whpx && accel_override.is_none() {
            warn!(
                "no hardware virtualization (KVM, HVF or WHPX): microVMs run in software emulation and will be slow"
            );
        }
        Ok(exec)
    }

    /// Boots a small VM that copies a few MiB from one disk to another, and
    /// checks the copy. `Err` says how it went wrong.
    pub async fn probe(&self) -> Result<()> {
        let scratch = TempDir::new_in(&self.run_dir, "probe")?;
        let dir = scratch.path().to_path_buf();
        let mut data = vec![0u8; PROBE_BYTES];
        blake3::Hasher::new()
            .update(b"pumasi probe")
            .finalize_xof()
            .fill(&mut data);
        let input = dir.join("input.tar");
        let output = dir.join("output.img");
        let file = FileEntry {
            path: "probe.bin".into(),
            data,
        };
        write_input_disk(
            &input,
            &RunSpec::default(),
            b"",
            &[],
            std::slice::from_ref(&file),
            &[],
        )?;
        make_output_disk(&output, OUTPUT_DISK_BYTES)?;
        let vm = VmSpec {
            kernel: self.kernel.clone(),
            initrd: self.initrd.clone(),
            mode: "probe",
            disks: vec![
                Disk {
                    path: input,
                    read_only: true,
                },
                Disk {
                    path: output.clone(),
                    read_only: false,
                },
            ],
            vcpus: 1,
            mem_mib: MIN_VM_MIB,
            net: None,
            pci: vec![],
            tee: None,
        };
        let deadline = Instant::now() + PROBE_TIMEOUT;
        let status = match self
            .boot(&vm, &dir, Some(deadline), &AtomicBool::new(false))
            .await?
        {
            VmEnd::Exited(status) => status,
            _ => bail!(
                "no answer within {} s; console:\n{}",
                PROBE_TIMEOUT.as_secs(),
                console_tail(&dir)
            ),
        };
        let out = read_output_disk(&output, None)?.with_context(|| {
            format!(
                "no result ({} {status}); console:\n{}",
                self.vmm.backend,
                console_tail(&dir)
            )
        })?;
        match out.result.outcome {
            GuestOutcome::Exited(0) => {}
            GuestOutcome::Failed(msg) => bail!("{msg}"),
            other => bail!("unexpected result {other:?}"),
        }
        ensure!(
            out.files.len() == 1 && out.files[0] == file,
            "the copy came back different"
        );
        Ok(())
    }

    pub fn backend(&self) -> Backend {
        self.vmm.backend
    }

    /// Whether microVMs here can have a network device (`network = true`).
    pub fn network_capable(&self) -> bool {
        self.net_vmm.is_some()
    }

    /// Whether GPUs can be passed through to microVMs here.
    pub fn gpu_passthrough(&self) -> bool {
        self.gpu.is_some()
    }

    /// The TEEs confidential tasks can run in here (protocol 14).
    pub fn tee_report(&self) -> pumasi_proto::confidential::TeeReport {
        self.tee.report()
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

    /// Whether an image (by manifest digest) is built and cached here.
    pub fn has_image(&self, digest: &str) -> bool {
        self.load_image(digest).is_some()
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
            .run_inner(task, spec, input, shared_files, limits, &cancel, io)
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
        io: &TaskIo,
    ) -> Result<RunResult> {
        if let Some(c) = &io.confidential {
            return self.run_confidential(task, c, limits, cancel, io).await;
        }
        ensure!(spec.gpus.is_none(), "ask for GPUs with a GPU request");
        let gpu = if io.gpus.is_empty() {
            None
        } else {
            let assets = self.gpu.as_ref().context(
                "GPU passthrough is not set up here (QEMU with KVM and `pumasi vm setup --gpu-bundle`)",
            )?;
            let mut pci = Vec::new();
            for g in &io.gpus {
                crate::gpu::vfio::ready(&g.pci).map_err(|why| {
                    anyhow::anyhow!("GPU {} cannot be passed through: {why}", g.pci)
                })?;
                for d in crate::gpu::vfio::passthrough_set(&g.pci) {
                    if !pci.contains(&d) {
                        pci.push(d);
                    }
                }
            }
            Some((assets, pci))
        };
        let net_vmm = if spec.network {
            Some(self.net_vmm.as_ref().context(
                    "microVMs on this machine cannot have a network card (Firecracker needs QEMU next to it for that)",
                )?)
        } else {
            None
        };
        let image = match &io.image {
            Some(img) => self.ensure_local_image(img, cancel).await?,
            None => self.ensure_image(&spec.image, cancel).await?,
        };
        // The timeout covers the run itself, not the image pull.
        let run_start = Instant::now();
        let scratch = TempDir::new_in(&self.run_dir, &format!("{}-{}", task.job, task.index))?;
        let dir = scratch.path().to_path_buf();
        let mut run_spec = run_spec(&image.config, spec, input);
        let gateway = match net_vmm {
            Some(_) => {
                run_spec.network = Some(pumasi_guest::GuestNet {
                    address: net::GUEST.octets(),
                    prefix: net::PREFIX,
                    gateway: net::GATEWAY.octets(),
                    dns: net::GATEWAY.octets(),
                });
                Some(net::Gateway::start().await?)
            }
            None => None,
        };
        if io.keeps_output() {
            run_spec.output_limit = MAX_STORE_OUTPUT_BYTES;
            run_spec.output_files = 1_000_000;
        }
        let staged = match &io.staged {
            Some(d) => pumasi_proto::job::dir_files(d)?,
            None => Vec::new(),
        };
        // The scratch disk holds the input files and what the task writes
        // (its output too); the file is sparse.
        let scratch_bytes = SCRATCH_BYTES
            + staged.iter().map(|(_, _, n)| n).sum::<u64>()
            + if io.keeps_output() {
                run_spec.files_limit()
            } else {
                0
            };
        let disks = {
            let dir = dir.clone();
            let input = input.clone();
            let shared = shared_files.to_vec();
            let run_spec = run_spec.clone();
            tokio::task::spawn_blocking(move || -> Result<[PathBuf; 3]> {
                let scratch = dir.join("scratch.ext4");
                ext4::mkfs(&scratch, scratch_bytes, SCRATCH_LABEL)?;
                let input_disk = dir.join("input.tar");
                write_input_disk(
                    &input_disk,
                    &run_spec,
                    &input.stdin,
                    &shared,
                    &input.files,
                    &staged,
                )?;
                let output = dir.join("output.img");
                make_output_disk(&output, run_spec.output_disk_bytes())?;
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
            net: gateway.as_ref().map(|g| g.addr),
            pci: vec![],
            tee: None,
        };
        let mut vm = vm;
        if let Some((assets, pci)) = &gpu {
            vm.kernel = assets.kernel.clone();
            vm.disks.push(Disk {
                path: assets.drivers.clone(),
                read_only: true,
            });
            vm.pci = pci.clone();
            vm.mem_mib += GPU_VM_OVERHEAD_MIB;
        }
        let deadline = run_start + Duration::from_millis(limits.timeout_ms);
        debug!(task = %task, image = %spec.image, network = spec.network, "booting microvm");
        let vmm = match &gpu {
            Some((assets, _)) => &assets.vmm,
            None => net_vmm.unwrap_or(&self.vmm),
        };
        let end = self.boot_on(vmm, &vm, &dir, Some(deadline), cancel).await?;
        drop(gateway);
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
            out_dir: None,
        };
        if !matches!(end, VmEnd::Exited(_)) {
            debug!(task = %task, console = %console_tail(&dir), "microvm stopped by the host");
        }
        let status = match end {
            VmEnd::Timeout => return Ok(outcome_for(Outcome::Timeout)),
            VmEnd::Cancelled => return Ok(outcome_for(Outcome::Cancelled)),
            VmEnd::Exited(status) => status,
        };
        let kept = if io.keeps_output() {
            Some(Arc::new(io.out_dir("out")?))
        } else {
            None
        };
        let out = {
            let (output_disk, kept_dir) = (
                output_disk.clone(),
                kept.as_ref().map(|d| d.path().to_path_buf()),
            );
            tokio::task::spawn_blocking(move || read_output_disk(&output_disk, kept_dir.as_deref()))
                .await??
        };
        let Some(out) = out else {
            bail!(
                "the microVM stopped without a result ({} {status}); console:\n{}",
                self.vmm.backend,
                console_tail(&dir)
            );
        };
        let mut stderr = out.stderr;
        note_dropped(&mut stderr, &out.result);
        let outcome = outcome_of(&out.result.outcome, limits.memory_bytes);
        Ok(RunResult {
            outcome,
            stdout: out.stdout,
            stderr,
            files: out.files,
            fuel_used: 0,
            wall_ms,
            peak_memory_bytes: None,
            out_dir: kept,
        })
    }

    /// Runs a confidential task (phase 16): a VM in the TEE the cluster
    /// placed it for, booting the sealed image and input. Its guest attests
    /// over the attest disk; the evidence goes to the cluster and the keys,
    /// sealed to the guest, come back the same way. What comes out is
    /// sealed too, and goes to the cluster as `output.sealed`.
    async fn run_confidential(
        &self,
        task: TaskRef,
        c: &ConfidentialIo,
        limits: &Limits,
        cancel: &AtomicBool,
        io: &TaskIo,
    ) -> Result<RunResult> {
        use pumasi_guest::seal::{
            ATTEST_DISK_BYTES, AttestReply, AttestRequest, EVIDENCE_AT, Evidence, REPLY_AT,
        };
        use pumasi_proto::confidential::{SEALED_OUTPUT_FILE, TeeKind};
        let t = &c.task;
        let (vmm, kernel, tee_vm) = match t.tee {
            TeeKind::Test => {
                ensure!(self.tee.test, "this node does not offer the test TEE");
                (&self.vmm, self.kernel.clone(), None)
            }
            TeeKind::SevSnp | TeeKind::Tdx => {
                let boot = match t.tee {
                    TeeKind::SevSnp => &self.tee.snp,
                    _ => &self.tee.tdx,
                };
                let boot = boot
                    .as_ref()
                    .with_context(|| format!("{} VMs are not available here", t.tee))?;
                let vmm = self
                    .tee_vmm
                    .as_ref()
                    .context("confidential VMs need QEMU with KVM")?;
                let vm = TeeVm {
                    kind: t.tee,
                    firmware: boot.firmware.clone(),
                    cbitpos: tee::cbitpos(),
                };
                (vmm, boot.kernel.clone(), Some(vm))
            }
        };
        let run_start = Instant::now();
        let scratch = TempDir::new_in(&self.run_dir, &format!("{}-{}-c", task.job, task.index))?;
        let dir = scratch.path().to_path_buf();
        let attest = dir.join("attest.img");
        let output = dir.join("output.img");
        {
            let (attest, output) = (attest.clone(), output.clone());
            let (image, input) = (c.image.clone(), c.input.clone());
            let req = AttestRequest {
                tee: t.tee,
                nonce: t.nonce,
            }
            .encode();
            let out_bytes = t.output_bytes;
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut f = create_sparse(&attest, ATTEST_DISK_BYTES)?;
                f.write_all(&req)?;
                make_output_disk(&output, out_bytes)?;
                pad_to_sector(&image)?;
                pad_to_sector(&input)?;
                Ok(())
            })
            .await??;
        }
        let disk = |path: &Path, read_only: bool| Disk {
            path: path.to_path_buf(),
            read_only,
        };
        let vm = VmSpec {
            kernel,
            initrd: self.initrd.clone(),
            mode: "sealed",
            disks: vec![
                disk(&c.image, true),
                disk(&c.input, true),
                disk(&attest, false),
                disk(&output, false),
            ],
            vcpus: vcpus(limits.cpu_millis).min(MAX_VCPUS),
            mem_mib: (limits.memory_bytes.div_ceil(1 << 20) + VM_OVERHEAD_MIB + t.extra_mib)
                .max(MIN_VM_MIB),
            net: None,
            pci: vec![],
            tee: tee_vm,
        };
        // Passes the guest's evidence on, and the answer back.
        let watcher = {
            let (attest, keys) = (attest.clone(), c.keys.clone());
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let a = attest.clone();
                    let Ok(Ok(b)) = tokio::task::spawn_blocking(move || {
                        read_at(&a, EVIDENCE_AT, (REPLY_AT - EVIDENCE_AT) as usize)
                    })
                    .await
                    else {
                        continue;
                    };
                    let reply = match Evidence::unframe(&b) {
                        None => continue,
                        Some(Err(e)) => AttestReply::Refused(format!("bad evidence: {e}")),
                        Some(Ok(ev)) => match keys.ask(ev).await {
                            Ok(w) => AttestReply::Keys(w),
                            Err(why) => AttestReply::Refused(why),
                        },
                    };
                    let a = attest.clone();
                    let _ =
                        tokio::task::spawn_blocking(move || write_at(&a, REPLY_AT, &reply.frame()))
                            .await;
                    return;
                }
            })
        };
        let deadline = run_start + Duration::from_millis(limits.timeout_ms);
        debug!(task = %task, tee = %t.tee, "booting confidential microvm");
        let end = self.boot_on(vmm, &vm, &dir, Some(deadline), cancel).await;
        watcher.abort();
        let end = end?;
        let wall_ms = run_start.elapsed().as_millis() as u64;
        let result = |outcome: Outcome| RunResult {
            outcome,
            stdout: vec![],
            stderr: vec![],
            files: vec![],
            fuel_used: 0,
            wall_ms,
            peak_memory_bytes: None,
            out_dir: None,
        };
        let status = match end {
            VmEnd::Timeout => return Ok(result(Outcome::Timeout)),
            VmEnd::Cancelled => return Ok(result(Outcome::Cancelled)),
            VmEnd::Exited(status) => status,
        };
        // Sealed: kept as it is for the cluster. Not sealed: the guest
        // failed before it had keys, and says why in the clear.
        let kept = io.out_dir("sealed")?;
        let sealed = {
            let (output, target) = (output.clone(), kept.path().join(SEALED_OUTPUT_FILE));
            tokio::task::spawn_blocking(move || copy_sealed_output(&output, &target)).await??
        };
        if sealed {
            return Ok(RunResult {
                out_dir: Some(Arc::new(kept)),
                ..result(Outcome::Exited(0))
            });
        }
        let out = {
            let output = output.clone();
            tokio::task::spawn_blocking(move || read_output_disk(&output, None)).await??
        };
        let Some(out) = out else {
            bail!(
                "the confidential microVM stopped without a result ({} {status}); console:\n{}",
                vmm.backend,
                console_tail(&dir)
            );
        };
        let outcome = match outcome_of(&out.result.outcome, limits.memory_bytes) {
            o @ Outcome::Error(_) => o,
            other => Outcome::Error(format!("the guest returned {other} without sealing it")),
        };
        Ok(RunResult {
            stderr: out.stderr,
            ..result(outcome)
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
        self.boot_on(&self.vmm, vm, dir, deadline, cancel).await
    }

    async fn boot_on(
        &self,
        vmm: &Vmm,
        vm: &VmSpec,
        dir: &Path,
        deadline: Option<Instant>,
        cancel: &AtomicBool,
    ) -> Result<VmEnd> {
        let console = std::fs::File::create(dir.join("console.log"))?;
        let mut cmd = vmm.command(vm, dir)?;
        cmd.stdin(std::process::Stdio::null())
            .stdout(console.try_clone()?)
            .stderr(console)
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .with_context(|| format!("start {}", vmm.bin.display()))?;
        // Windows: the VMM dies with its job, so not even a crashed agent
        // leaves VMs running.
        let _job = crate::platform::ProcessJob::new()
            .and_then(|job| job.assign(&child).map(|()| job))
            .map_err(|e| warn!("could not put the VMM in a job object: {e}"))
            .ok();
        // When QEMU writes the console to a file (Windows), watch it: under
        // WHPX a guest reset can leave QEMU stuck instead of exiting. The
        // vz helper writes the console to its stdout; should a guest reboot
        // not stop the VM, the same watch ends it.
        let (serial, watch_serial) = match vmm.backend {
            Backend::Qemu if cfg!(windows) => (dir.join(vmm::SERIAL_LOG), true),
            Backend::Vz => (dir.join("console.log"), true),
            _ => (PathBuf::new(), false),
        };
        let mut done_since: Option<Instant> = None;
        let mut ticks = 0u32;
        let mut serial_seen = 0u64;
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
                    if watch_serial && done_since.is_none() && ticks.is_multiple_of(10) && guest_said_done(&serial, &mut serial_seen) {
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
        let compressed: u64 = resolved.layers.iter().map(|l| l.size).sum();
        self.build_image(
            reference,
            &resolved.digest,
            &resolved.config,
            blobs,
            compressed,
            work,
            cancel,
        )
        .await
    }

    /// A private image taken from the file store: built from its layers
    /// once, then cached by digest like a pulled one.
    async fn ensure_local_image(
        &self,
        img: &LocalImage,
        cancel: &AtomicBool,
    ) -> Result<ImageEntry> {
        if let Some(e) = self.load_image(&img.digest) {
            return Ok(e);
        }
        let _pull = self.pull_lock.lock().await;
        if let Some(e) = self.load_image(&img.digest) {
            return Ok(e);
        }
        let manifest: ImageManifest = serde_json::from_slice(
            &std::fs::read(img.dir.join(IMAGE_MANIFEST)).context("read the image manifest")?,
        )
        .context("bad image manifest")?;
        ensure!(
            manifest.digest == img.digest,
            "image manifest does not match"
        );
        let work = TempDir::new_in(&self.run_dir, "pull")?;
        let mut blobs = Vec::new();
        for i in 0..manifest.layers.len() {
            let path = work.path().join(image_layer_name(i));
            std::fs::rename(img.dir.join(image_layer_name(i)), &path)
                .or_else(|_| std::fs::copy(img.dir.join(image_layer_name(i)), &path).map(|_| ()))
                .context("take a layer of the private image")?;
            blobs.push(path);
        }
        let compressed: u64 = manifest.layers.iter().map(|l| l.size).sum();
        self.build_image(
            &manifest.reference,
            &manifest.digest,
            &manifest.config,
            blobs,
            compressed,
            work,
            cancel,
        )
        .await
    }

    /// Unpacks downloaded layers into a root filesystem in a build VM and
    /// caches it.
    #[allow(clippy::too_many_arguments)]
    async fn build_image(
        &self,
        reference: &str,
        digest: &str,
        config: &ContainerConfig,
        blobs: Vec<PathBuf>,
        compressed: u64,
        work: TempDir,
        cancel: &AtomicBool,
    ) -> Result<ImageEntry> {
        info!(image = %reference, "unpacking image in a build VM");
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
                make_output_disk(&output, OUTPUT_DISK_BYTES)
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
            net: None,
            pci: vec![],
            tee: None,
        };
        let deadline = Instant::now() + BUILD_TIMEOUT;
        let status = match self.boot(&vm, work.path(), Some(deadline), cancel).await? {
            VmEnd::Exited(status) => status,
            VmEnd::Timeout => bail!("unpacking {reference} took longer than 30 minutes"),
            VmEnd::Cancelled => bail!("cancelled while unpacking {reference}"),
        };
        let result = read_output_disk(&output, None)?.map(|o| o.result);
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
        let dest = self.image_dir(digest);
        let entry = ImageEntry {
            digest: digest.to_string(),
            config: config.clone(),
            rootfs: dest.join("rootfs.ext4"),
        };
        let staged = work.path().join("image");
        std::fs::create_dir_all(&staged)?;
        std::fs::rename(&base, staged.join("rootfs.ext4"))?;
        std::fs::write(staged.join("image.json"), serde_json::to_vec(&entry)?)?;
        if let Err(e) = std::fs::rename(&staged, &dest) {
            // Another agent sharing this directory finished first.
            if self.load_image(digest).is_none() {
                return Err(e).context("store image");
            }
        }
        self.save_ref(reference, digest)?;
        info!(image = %reference, "image ready");
        self.load_image(digest)
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

#[cfg(test)]
fn tar_entry<W: Write>(b: &mut tar::Builder<W>, name: &str, data: &[u8]) -> Result<()> {
    let mut h = tar::Header::new_gnu();
    h.set_size(data.len() as u64);
    h.set_mode(0o644);
    h.set_entry_type(tar::EntryType::Regular);
    b.append_data(&mut h, name, data)?;
    Ok(())
}

/// Pads a disk image to whole 4 KiB blocks.
pub(crate) fn pad_to_sector(path: &Path) -> Result<()> {
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
    write_layers_tar(std::io::BufWriter::with_capacity(1 << 20, f), blobs)?.flush()?;
    pad_to_sector(path)
}

fn write_input_disk(
    path: &Path,
    spec: &RunSpec,
    stdin: &[u8],
    shared: &[FileEntry],
    own: &[FileEntry],
    staged: &[(String, PathBuf, u64)],
) -> Result<()> {
    let f = std::fs::File::create(path)?;
    write_input_tar(
        std::io::BufWriter::with_capacity(1 << 20, f),
        spec,
        stdin,
        shared,
        own,
        staged,
    )?
    .flush()?;
    pad_to_sector(path)
}

pub(crate) fn make_output_disk(path: &Path, size: u64) -> Result<()> {
    let mut f = create_sparse(path, size.max(OUTPUT_DISK_BYTES).div_ceil(4096) * 4096)?;
    f.write_all(OUTPUT_MARK)?;
    Ok(())
}

/// Parses the output disk. `None` if the guest never wrote it.
fn read_output_disk(path: &Path, kept: Option<&Path>) -> Result<Option<GuestOutput>> {
    let mut f = std::fs::File::open(path)?;
    let mut head = vec![0u8; OUTPUT_MARK.len()];
    f.read_exact(&mut head)?;
    if head == OUTPUT_MARK {
        return Ok(None);
    }
    let f = std::fs::File::open(path)?;
    parse_output(BufReader::new(f), kept)
        .context("read output disk")
        .map(Some)
}

/// Copies a sealed output from the output disk to `target`. `false` when
/// the disk holds no sealed output.
fn copy_sealed_output(disk: &Path, target: &Path) -> Result<bool> {
    use pumasi_guest::seal::{HEADER, Kind, sealed_kind, sealed_len};
    let mut f = std::fs::File::open(disk)?;
    let mut head = vec![0u8; HEADER];
    f.read_exact(&mut head)?;
    if sealed_kind(&head) != Some(Kind::Output) {
        return Ok(false);
    }
    let plain = u64::from_le_bytes(head[16..24].try_into().expect("8 bytes"));
    let disk_len = f.metadata()?.len();
    let len = sealed_len(plain).min(disk_len);
    let f = std::fs::File::open(disk)?;
    let mut out = std::fs::File::create(target)?;
    std::io::copy(&mut f.take(len), &mut out)?;
    Ok(true)
}

fn read_at(path: &Path, at: u64, len: usize) -> std::io::Result<Vec<u8>> {
    use std::io::{Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(at))?;
    let mut b = Vec::with_capacity(len);
    f.take(len as u64).read_to_end(&mut b)?;
    Ok(b)
}

fn write_at(path: &Path, at: u64, data: &[u8]) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.seek(SeekFrom::Start(at))?;
    f.write_all(data)?;
    f.sync_all()
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
    // Debugging aid: PUMASI_VM_CONSOLE_TAIL=65536 keeps more of it.
    let keep = std::env::var("PUMASI_VM_CONSOLE_TAIL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(CONSOLE_TAIL);
    let start = text.len().saturating_sub(keep);
    String::from_utf8_lossy(&text[start..]).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_guest::{
        DISK_ROLE_ENTRY, GuestResult, RESULT_ENTRY, SPEC_ENTRY, STDIN_ENTRY, STDOUT_ENTRY,
    };

    /// Needs a VM directory set up with `pumasi vm setup`.
    #[tokio::test]
    async fn probe_vm_copies_its_input() {
        let Some(dir) = std::env::var_os("PUMASI_TEST_VM_DIR") else {
            eprintln!("skipping: PUMASI_TEST_VM_DIR is not set");
            return;
        };
        let config = MicrovmConfig {
            dir: dir.into(),
            backend: None,
            confidential: Default::default(),
        };
        let exec = MicrovmExecutor::detect(&config, "probe-test")
            .await
            .unwrap();
        exec.probe().await.unwrap();
    }

    #[test]
    fn limits_match_the_guest() {
        use pumasi_proto::job::{MAX_OUTPUT_BYTES, MAX_OUTPUT_FILES_BYTES};
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
        make_output_disk(&path, OUTPUT_DISK_BYTES).unwrap();
        assert!(
            read_output_disk(&path, None).unwrap().is_none(),
            "empty disk"
        );
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
        let out = read_output_disk(&path, None).unwrap().unwrap();
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
        write_input_disk(&path, &spec, b"in", &shared, &[], &[]).unwrap();
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
        assert!(write_input_disk(&path, &spec, b"", &bad, &[], &[]).is_err());
    }
}
