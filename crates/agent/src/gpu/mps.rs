//! Memory limits for parts of NVIDIA GPUs (1.16), through the CUDA
//! Multi-Process Service.
//!
//! A run that holds part of a GPU (`gpus = 0.5`) shares it with others by
//! time slicing, and before 1.16 shared its memory on trust: PyTorch's
//! caching allocator or a TensorFlow without `allow_growth` could take all
//! of it. With MPS, every CUDA process of a part becomes a client of one
//! MPS server per GPU, and the client's environment sets
//! `CUDA_MPS_PINNED_DEVICE_MEM_LIMIT` (its part of the GPU's memory; an
//! allocation past it fails with an out-of-memory error) and
//! `CUDA_MPS_ACTIVE_THREAD_PERCENTAGE` (its part of the GPU's SMs).
//!
//! The agent starts the control daemon (`nvidia-cuda-mps-control -d`) with
//! a pipe directory of its own, or uses one an administrator already runs
//! at the default `/tmp/nvidia-mps`. A part's container gets that
//! directory at `/tmp/nvidia-mps`, the host's IPC namespace and the two
//! variables; whole-GPU runs never see it and use the GPU as before.
//!
//! What it needs, and otherwise parts stay time-sliced (the cluster's GPU
//! view says which): Linux (MPS does not exist on Windows or under WSL), an
//! agent running as root (a daemon started by another user serves only
//! that user, and containers run as root), Volta or newer (older GPUs
//! ignore the memory limit) and the daemon's binary, which comes with the
//! NVIDIA driver packages. The limits hold for programs that use CUDA as
//! it is; a container can still unset the variables of its own processes,
//! so MPS stops accidents, not a hostile task (MIG slices do that).

use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use pumasi_proto::gpu::{GpuInfo, GpuSharing, GpuVendor};
use tracing::{info, warn};

/// Where a part's container finds the MPS pipes.
pub const CONTAINER_PIPE: &str = "/tmp/nvidia-mps";
/// Overrides the daemon's binary (tests use a stand-in).
const CONTROL_ENV: &str = "PUMASI_MPS_CONTROL";

/// The MPS control daemon the agent uses.
#[derive(Debug)]
pub struct Mps {
    control: PathBuf,
    pipe: PathBuf,
    /// The agent started the daemon, and stops it when it goes.
    owned: bool,
}

/// What a part's container gets to run under MPS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpsPart {
    /// The daemon's pipe directory on the host.
    pub pipe: PathBuf,
    /// The part's memory, in MiB.
    pub memory_mib: u64,
    /// The part's share of the GPU's threads, 1 to 100.
    pub threads_percent: u32,
}

impl MpsPart {
    /// The `docker create` options.
    pub fn docker_args(&self) -> Vec<String> {
        vec![
            "--ipc=host".into(),
            "--volume".into(),
            format!("{}:{CONTAINER_PIPE}", self.pipe.display()),
            "--env".into(),
            format!("CUDA_MPS_PIPE_DIRECTORY={CONTAINER_PIPE}"),
            "--env".into(),
            // The container sees its GPU as device 0.
            format!("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT=0={}M", self.memory_mib),
            "--env".into(),
            format!("CUDA_MPS_ACTIVE_THREAD_PERCENTAGE={}", self.threads_percent),
        ]
    }
}

/// GPUs whose parts MPS can limit: NVIDIA, whole (MIG slices are limited
/// already), Volta (compute capability 7) or newer, with a driver.
fn eligible(g: &GpuInfo) -> bool {
    g.vendor == GpuVendor::Nvidia
        && !g.mig()
        && g.driver != "vfio-pci"
        && g.compute
            .split('.')
            .next()
            .and_then(|m| m.parse::<u32>().ok())
            .is_some_and(|major| major >= 7)
}

impl Mps {
    /// Starts (or finds) the daemon for this machine's GPUs, with what the
    /// cluster is told either way. Never fails: what stops it goes in the
    /// note.
    pub fn start(enabled: bool, gpus: &[GpuInfo], data_dir: &Path) -> (Option<Mps>, GpuSharing) {
        let nvidia: Vec<&GpuInfo> = gpus
            .iter()
            .filter(|g| g.vendor == GpuVendor::Nvidia && !g.mig())
            .collect();
        if nvidia.is_empty() {
            return (None, GpuSharing::default());
        }
        let none = |note: String| {
            info!("parts of GPUs share memory on trust: {note}");
            (
                None,
                GpuSharing {
                    mps: Vec::new(),
                    note,
                },
            )
        };
        if !enabled {
            return none("MPS is off (--no-gpu-mps)".into());
        }
        if !cfg!(target_os = "linux") {
            return none("MPS needs Linux (Windows and WSL have none)".into());
        }
        if !is_root() {
            return none("MPS needs the agent to run as root".into());
        }
        let limited: Vec<u32> = nvidia
            .iter()
            .filter(|g| eligible(g))
            .map(|g| g.index)
            .collect();
        if limited.is_empty() {
            return none("MPS memory limits need a Volta or newer GPU".into());
        }
        let Some(control) = control_binary() else {
            return none("nvidia-cuda-mps-control was not found".into());
        };
        match Self::open(control, data_dir) {
            Ok(m) => {
                info!(pipe = %m.pipe.display(), gpus = ?limited, "MPS: parts of GPUs get memory limits");
                (
                    Some(m),
                    GpuSharing {
                        mps: limited,
                        note: String::new(),
                    },
                )
            }
            Err(e) => {
                warn!("MPS: {e:#}");
                none(format!("MPS did not start: {e:#}"))
            }
        }
    }

    /// Uses the daemon at the default pipe directory when one runs, else
    /// starts one with a directory under `data_dir`.
    fn open(control: PathBuf, data_dir: &Path) -> Result<Mps> {
        let default = std::env::var_os("CUDA_MPS_PIPE_DIRECTORY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(CONTAINER_PIPE));
        if default.join("control").exists() && ask(&control, &default, "get_server_list").is_ok() {
            return Ok(Mps {
                control,
                pipe: default,
                owned: false,
            });
        }
        let pipe = data_dir.join("mps").join("pipe");
        let log = data_dir.join("mps").join("log");
        std::fs::create_dir_all(&pipe).context("create the MPS pipe directory")?;
        std::fs::create_dir_all(&log).context("create the MPS log directory")?;
        let status = Command::new(&control)
            .arg("-d")
            .env("CUDA_MPS_PIPE_DIRECTORY", &pipe)
            .env("CUDA_MPS_LOG_DIRECTORY", &log)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .with_context(|| format!("run {}", control.display()))?;
        if !status.status.success() {
            bail!(
                "{} -d: {}",
                control.display(),
                String::from_utf8_lossy(&status.stderr).trim()
            );
        }
        let until = Instant::now() + Duration::from_secs(5);
        while !pipe.join("control").exists() {
            if Instant::now() > until {
                bail!("the MPS daemon made no control pipe in {}", pipe.display());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let m = Mps {
            control,
            pipe,
            owned: true,
        };
        ask(&m.control, &m.pipe, "get_server_list").context("the MPS daemon does not answer")?;
        Ok(m)
    }

    /// What a part of `share` hundredths of `gpu` runs with.
    pub fn part(&self, gpu: &GpuInfo, share: u32) -> MpsPart {
        let share = share.clamp(1, 100);
        MpsPart {
            pipe: self.pipe.clone(),
            memory_mib: (gpu.vram_mib * share as u64 / 100).max(1),
            threads_percent: share,
        }
    }

    pub fn pipe(&self) -> &Path {
        &self.pipe
    }
}

impl Drop for Mps {
    fn drop(&mut self) {
        if self.owned
            && let Err(e) = ask(&self.control, &self.pipe, "quit")
        {
            warn!("could not stop the MPS daemon: {e:#}");
        }
    }
}

/// Sends one command to the daemon (`nvidia-cuda-mps-control` reads them
/// on stdin).
fn ask(control: &Path, pipe: &Path, command: &str) -> Result<String> {
    use std::io::Write;
    let mut child = Command::new(control)
        .env("CUDA_MPS_PIPE_DIRECTORY", pipe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("run {}", control.display()))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(format!("{command}\n").as_bytes())?;
    }
    let out = child.wait_with_output()?;
    if !out.status.success() {
        bail!("{command}: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn control_binary() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os(CONTROL_ENV) {
        return Some(PathBuf::from(p));
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .chain(["/usr/bin".into(), "/usr/local/cuda/bin".into()])
        .map(|d| d.join("nvidia-cuda-mps-control"))
        .find(|p| p.is_file())
}

#[cfg(unix)]
fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn is_root() -> bool {
    false
}

/// The part (in hundredths) the cluster gave a run, from its environment.
pub fn share_of(env: &std::collections::BTreeMap<String, String>) -> Option<u32> {
    let f: f64 = env.get("PUMASI_GPU_FRACTION")?.trim().parse().ok()?;
    let share = (f * 100.0).round();
    (1.0..100.0).contains(&share).then_some(share as u32)
}

/// Whether an image's `User` runs as root: MPS serves one user, and the
/// daemon serves root's containers.
pub fn runs_as_root(user: &str) -> bool {
    let name = user.trim().split(':').next().unwrap_or("");
    name.is_empty() || name == "0" || name == "root"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(index: u32, compute: &str) -> GpuInfo {
        GpuInfo {
            index,
            vendor: GpuVendor::Nvidia,
            name: "NVIDIA GeForce RTX 3080".into(),
            uuid: format!("GPU-{index}"),
            pci: String::new(),
            vram_mib: 10_240,
            driver: "560.35".into(),
            compute: compute.into(),
            display: false,
            fp32_gflops: 29_000,
            measured: true,
            mem_bw_gb_s: 0,
            hash_rate: 0,
            docker: true,
            vfio: false,
            render_node: String::new(),
        }
    }

    #[test]
    fn parts_get_their_share_of_memory_and_threads() {
        let m = Mps {
            control: "/bin/true".into(),
            pipe: "/var/lib/pumasi/mps/pipe".into(),
            owned: false,
        };
        let p = m.part(&gpu(0, "8.6"), 50);
        assert_eq!(p.memory_mib, 5_120);
        assert_eq!(
            p.docker_args().join(" "),
            "--ipc=host --volume /var/lib/pumasi/mps/pipe:/tmp/nvidia-mps \
             --env CUDA_MPS_PIPE_DIRECTORY=/tmp/nvidia-mps \
             --env CUDA_MPS_PINNED_DEVICE_MEM_LIMIT=0=5120M \
             --env CUDA_MPS_ACTIVE_THREAD_PERCENTAGE=50"
        );
        assert!(eligible(&gpu(0, "7.0")));
        assert!(!eligible(&gpu(0, "6.1")), "Pascal ignores the limit");
        let mut slice = gpu(0, "8.0");
        slice.uuid = "MIG-x".into();
        assert!(!eligible(&slice));

        let env = |v: &str| {
            std::collections::BTreeMap::from([("PUMASI_GPU_FRACTION".to_string(), v.to_string())])
        };
        assert_eq!(share_of(&env("0.5")), Some(50));
        assert_eq!(share_of(&env("0.25")), Some(25));
        assert_eq!(share_of(&env("1")), None);
        assert_eq!(share_of(&Default::default()), None);
        assert!(runs_as_root("") && runs_as_root("0:0") && runs_as_root("root"));
        assert!(!runs_as_root("1000") && !runs_as_root("app:app"));
    }

    #[test]
    fn without_nvidia_gpus_nothing_starts() {
        let dir = crate::tempdir::TempDir::new("mps").unwrap();
        let (m, s) = Mps::start(true, &[], dir.path());
        assert!(m.is_none());
        assert_eq!(s, GpuSharing::default());
        let (m, s) = Mps::start(false, &[gpu(0, "8.6")], dir.path());
        assert!(m.is_none());
        assert!(s.mps.is_empty() && s.note.contains("off"), "{s:?}");
    }

    /// The daemon's life against a stand-in for `nvidia-cuda-mps-control`
    /// that keeps the commands it gets: `-d` makes the control pipe,
    /// `get_server_list` answers, and `quit` comes when the agent goes.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_daemon_starts_answers_and_stops() {
        let dir = crate::tempdir::TempDir::new("mps").unwrap();
        let log = dir.path().join("commands");
        let stand_in = dir.path().join("nvidia-cuda-mps-control");
        std::fs::write(
            &stand_in,
            format!(
                "#!/bin/sh\n\
                 if [ \"$1\" = -d ]; then touch \"$CUDA_MPS_PIPE_DIRECTORY/control\"; echo \"-d $CUDA_MPS_PIPE_DIRECTORY\" >> {log}; exit 0; fi\n\
                 read cmd; echo \"$cmd\" >> {log}\n",
                log = log.display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stand_in, std::fs::Permissions::from_mode(0o755)).unwrap();
        let m = Mps::open(stand_in, dir.path()).unwrap();
        assert!(m.owned);
        assert_eq!(m.pipe(), dir.path().join("mps/pipe"));
        drop(m);
        let commands = std::fs::read_to_string(&log).unwrap();
        let lines: Vec<&str> = commands.lines().collect();
        assert_eq!(lines.len(), 3, "{commands}");
        assert!(lines[0].starts_with("-d ") && lines[0].ends_with("mps/pipe"));
        assert_eq!(&lines[1..], ["get_server_list", "quit"]);
    }
}
