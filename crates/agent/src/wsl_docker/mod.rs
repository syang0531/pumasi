//! Docker on Windows without Docker Desktop (phase 27, 1.11).
//!
//! `pumasi docker setup` (an administrator terminal) gives the machine a
//! Docker daemon that runs before anyone signs in:
//!
//! - a small WSL 2 distro, [`DISTRO`], made from Ubuntu Base (pinned here)
//!   with Ubuntu's `docker.io` in it, and the NVIDIA Container Toolkit
//!   when the Windows NVIDIA driver lends WSL its CUDA libraries;
//! - a hidden local account, [`ACCOUNT`], that owns it: WSL does not run as
//!   LocalSystem (the agent service's account), and distros belong to one
//!   user;
//! - a scheduled task, [`TASK`], that logs that account on at boot (no one
//!   signed in) and runs `pumasi __wsl-docker-run`, which imports and sets
//!   up the distro when needed and keeps `dockerd` running in it. The agent
//!   starts the task again when Docker stops answering;
//! - `dockerd` on `127.0.0.1:2376` inside the VM, which WSL forwards to the
//!   Windows loopback, with mutual TLS: only holders of the client key
//!   (SYSTEM and administrators) reach it, not every local user;
//! - Docker's static Windows CLI, which the agent runs with `DOCKER_HOST`
//!   and the client certificate ([`docker_command`]).
//!
//! The distro has no Windows drives mounted and no Windows interop: a
//! container that escaped to the distro's root would find no host files.
//! Everything lives in `%ProgramData%\Pumasi\wsl-docker` ([`Layout`]),
//! readable by SYSTEM, administrators and the account only.
//!
//! Docker Desktop stays supported: an agent uses the Pumasi distro when it
//! is set up and `--docker-bin` is the default, and Docker Desktop's CLI
//! when `--docker-bin` names it.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub mod tls;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{kick, remove, run_task, setup, status};

/// `pumasi docker setup`'s options.
#[derive(Debug, Clone)]
pub struct SetupOptions {
    /// The daemon's port on the loopback.
    pub port: u16,
    /// Wait until Docker answers (the first start installs it).
    pub wait: bool,
    /// Replace the TLS certificates (the distro is set up again).
    pub new_certificates: bool,
}

#[cfg(not(windows))]
const WINDOWS_ONLY: &str = "`pumasi docker` sets up Docker in WSL on Windows; on Linux and macOS install Docker (or Podman) as usual";

#[cfg(not(windows))]
pub async fn setup(_opts: SetupOptions) -> Result<i32> {
    anyhow::bail!("{WINDOWS_ONLY}")
}

#[cfg(not(windows))]
pub async fn status() -> Result<i32> {
    anyhow::bail!("{WINDOWS_ONLY}")
}

#[cfg(not(windows))]
pub async fn remove() -> Result<i32> {
    anyhow::bail!("{WINDOWS_ONLY}")
}

#[cfg(not(windows))]
pub async fn run_task(_dir: &Path) -> Result<i32> {
    anyhow::bail!("{WINDOWS_ONLY}")
}

/// Starts the task again (Windows); nothing elsewhere.
#[cfg(not(windows))]
pub fn kick() {}

/// The WSL distro's name.
pub const DISTRO: &str = "pumasi-docker";
/// The local account that owns the distro and runs the task.
pub const ACCOUNT: &str = "pumasi-wsl";
/// The scheduled task that runs [`RUN_ARG`] as [`ACCOUNT`] at boot.
pub const TASK: &str = r"\Pumasi\WSL Docker";
/// `pumasi __wsl-docker-run --dir DIR`: what the task runs.
pub const RUN_ARG: &str = "__wsl-docker-run";
/// Where `dockerd` listens (TLS), inside the VM and, forwarded by WSL, on
/// the Windows loopback.
pub const DEFAULT_PORT: u16 = 2376;
/// Bumped when the distro's setup changes, so the task sets it up again.
pub const PROVISION_VERSION: u32 = 1;

/// A file `pumasi docker setup` downloads, checked against its SHA-256.
#[derive(Debug, Clone, Copy)]
pub struct Pin {
    pub file: &'static str,
    pub label: &'static str,
    pub urls: &'static [&'static str],
    pub sha256: &'static str,
    pub size: u64,
}

/// Ubuntu Base 24.04.5 (the root file system Ubuntu's own images start
/// from, 30 MB) for the distro. Ubuntu is what NVIDIA documents CUDA on
/// WSL with.
pub fn rootfs_pin(arch: &str) -> Option<Pin> {
    match arch {
        "x86_64" => Some(Pin {
            file: "ubuntu-base-24.04.5-base-amd64.tar.gz",
            label: "Ubuntu Base 24.04.5 (amd64)",
            urls: &[
                "https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.5-base-amd64.tar.gz",
                "https://cdimage.ubuntu.com/ubuntu-base/releases/24.04.5/release/ubuntu-base-24.04.5-base-amd64.tar.gz",
            ],
            sha256: "e77b6f10c2590cef872b33ee9f635a0e3fd1f57fb074c0e52b5c7f56147a0c86",
            size: 30_028_293,
        }),
        "aarch64" => Some(Pin {
            file: "ubuntu-base-24.04.5-base-arm64.tar.gz",
            label: "Ubuntu Base 24.04.5 (arm64)",
            urls: &[
                "https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.5-base-arm64.tar.gz",
                "https://cdimage.ubuntu.com/ubuntu-base/releases/24.04.5/release/ubuntu-base-24.04.5-base-arm64.tar.gz",
            ],
            sha256: "a91d5a93010193712d346d761372b7c9db6dfcf093893161c64ca107f05914f2",
            size: 29_936_675,
        }),
        _ => None,
    }
}

/// Docker's static Windows binaries (the CLI is all that is used). There
/// is no Arm build; Windows on Arm runs this one emulated.
pub const DOCKER_CLI: Pin = Pin {
    file: "docker-29.9.3.zip",
    label: "Docker CLI 29.9.3",
    urls: &["https://download.docker.com/win/static/stable/x86_64/docker-29.9.3.zip"],
    sha256: "7c4ff7bbb31579046cd34dd8b91f7cbd3ab3482a1a85330170be726ef792d751",
    size: 49_715_375,
};

/// The files of a set-up machine, under one directory
/// (`%ProgramData%\Pumasi\wsl-docker`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    root: PathBuf,
}

impl Layout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The machine's: `%ProgramData%\Pumasi\wsl-docker`. `None` off
    /// Windows.
    pub fn system() -> Option<Self> {
        if !cfg!(windows) {
            return None;
        }
        let base = std::env::var_os("ProgramData")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"));
        Some(Self::new(base.join("Pumasi").join("wsl-docker")))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn config_file(&self) -> PathBuf {
        self.root.join("config.json")
    }
    pub fn bin_dir(&self) -> PathBuf {
        self.root.join("bin")
    }
    pub fn docker_exe(&self) -> PathBuf {
        self.bin_dir().join("docker.exe")
    }
    /// The copy of `pumasi.exe` the task runs: the account may not read
    /// the one in an administrator's profile.
    pub fn pumasi_exe(&self) -> PathBuf {
        self.bin_dir().join("pumasi.exe")
    }
    /// The distro's disk (`ext4.vhdx`).
    pub fn distro_dir(&self) -> PathBuf {
        self.root.join("distro")
    }
    pub fn downloads(&self) -> PathBuf {
        self.root.join("downloads")
    }
    /// The CA and the server's certificate and key, which go into the
    /// distro.
    pub fn server_tls(&self) -> PathBuf {
        self.root.join("tls").join("server")
    }
    /// `ca.pem`, `cert.pem` and `key.pem` for the CLI (`DOCKER_CERT_PATH`).
    pub fn client_tls(&self) -> PathBuf {
        self.root.join("tls").join("client")
    }
    pub fn logs(&self) -> PathBuf {
        self.root.join("logs")
    }
    pub fn log_file(&self) -> PathBuf {
        self.logs().join("wsl-docker.log")
    }
    pub fn task_xml(&self) -> PathBuf {
        self.root.join("task.xml")
    }
}

/// What `pumasi docker setup` chose, for the task and the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    /// The root file system's file name in `downloads`.
    pub rootfs: String,
    /// The release that set it up.
    pub version: String,
}

impl Config {
    pub fn read(layout: &Layout) -> Result<Self> {
        let path = layout.config_file();
        let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
    }

    pub fn write(&self, layout: &Layout) -> Result<()> {
        let path = layout.config_file();
        std::fs::write(&path, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("write {}", path.display()))
    }
}

/// A machine where `pumasi docker setup` ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installed {
    pub layout: Layout,
    pub config: Config,
}

impl Installed {
    /// What `layout` holds, when it is set up (config and CLI present).
    pub fn at(layout: Layout) -> Option<Self> {
        if !layout.docker_exe().is_file() {
            return None;
        }
        let config = Config::read(&layout).ok()?;
        Some(Self { layout, config })
    }

    /// The CLI's environment: the daemon in the distro, over TLS.
    pub fn env(&self) -> Vec<(&'static str, OsString)> {
        vec![
            (
                "DOCKER_HOST",
                format!("tcp://127.0.0.1:{}", self.config.port).into(),
            ),
            ("DOCKER_TLS_VERIFY", "1".into()),
            (
                "DOCKER_CERT_PATH",
                self.layout.client_tls().into_os_string(),
            ),
        ]
    }

    /// Whether the CLI `bin` means this Docker: `docker` (the default) or
    /// its own path.
    pub fn serves(&self, bin: &str) -> bool {
        bin == "docker" || Path::new(bin) == self.layout.docker_exe()
    }
}

/// The machine's Pumasi Docker, when set up (Windows only).
pub fn installed() -> Option<Installed> {
    Installed::at(Layout::system()?)
}

/// The Docker CLI `bin`, and the environment it needs when it is the
/// Pumasi distro's.
fn resolve(bin: &str) -> (OsString, Vec<(&'static str, OsString)>) {
    match installed().filter(|i| i.serves(bin)) {
        Some(i) => (i.layout.docker_exe().into_os_string(), i.env()),
        None => (bin.into(), Vec::new()),
    }
}

/// A command running the Docker CLI `bin`: on a machine set up with
/// `pumasi docker setup`, `docker` is the Pumasi distro's CLI, with its
/// daemon and certificate.
pub fn docker_command(bin: &str) -> tokio::process::Command {
    let (program, env) = resolve(bin);
    let mut cmd = tokio::process::Command::new(program);
    cmd.envs(env);
    cmd
}

/// [`docker_command`], blocking.
pub fn docker_std_command(bin: &str) -> std::process::Command {
    let (program, env) = resolve(bin);
    let mut cmd = std::process::Command::new(program);
    cmd.envs(env);
    cmd
}

/// What the task writes into the distro, and checks before each start:
/// the setup version, the certificate it serves and whether it has the
/// NVIDIA toolkit.
pub fn provision_marker(server_pems: &tls::ServerPems, gpu: bool) -> String {
    let hash = blake3::hash(
        [&server_pems.ca, &server_pems.cert, &server_pems.key]
            .map(String::as_bytes)
            .concat()
            .as_slice(),
    );
    format!(
        "pumasi-docker v{PROVISION_VERSION} tls={} gpu={}",
        &hash.to_hex()[..16],
        if gpu { "nvidia" } else { "none" }
    )
}

/// `/etc/wsl.conf`: no Windows drives, no Windows programs, no systemd
/// (dockerd is started by the task), root by default.
pub const WSL_CONF: &str = "\
# Written by `pumasi docker setup`.
[boot]
systemd=false

[automount]
enabled=false
mountFsTab=false

[interop]
enabled=false
appendWindowsPath=false

[user]
default=root
";

/// `%USERPROFILE%\.wslconfig` of [`ACCOUNT`]: its VM forwards loopback
/// ports to Windows, and has no graphical apps (WSLg) to start.
pub const WSL_CONFIG: &str = "\
# Written by `pumasi __wsl-docker-run` for the Pumasi Docker distro.
[wsl2]
localhostForwarding=true
guiApplications=false
";

/// Where the provisioning puts what `dockerd` needs.
const TLS_DIR: &str = "/etc/pumasi/tls";
const START_SCRIPT: &str = "/usr/local/sbin/pumasi-dockerd";
pub const MARKER_FILE: &str = "/etc/pumasi/provisioned";

/// The CUDA library the Windows NVIDIA driver lends every WSL 2 distro.
pub const WSL_CUDA: &str = "/usr/lib/wsl/lib/libcuda.so.1";

/// The script that starts `dockerd` in the foreground (the task waits on
/// it): iptables that work on this kernel, cgroups, then dockerd with TLS.
pub fn start_script(port: u16) -> String {
    format!(
        r#"#!/bin/sh
# Written by `pumasi docker setup`; started by the scheduled task.
set -eu
# Docker's bridge network needs iptables; fall back to the legacy backend
# where this WSL kernel lacks nftables.
if ! iptables -n -L >/dev/null 2>&1; then
  update-alternatives --set iptables /usr/sbin/iptables-legacy
  update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
fi
if ! mountpoint -q /sys/fs/cgroup; then
  mount -t cgroup2 cgroup2 /sys/fs/cgroup
fi
rm -f /var/run/docker.pid /var/run/docker/containerd/containerd.pid
exec /usr/bin/dockerd \
  --host=unix:///var/run/docker.sock \
  --host=tcp://127.0.0.1:{port} \
  --tlsverify \
  --tlscacert={TLS_DIR}/ca.pem \
  --tlscert={TLS_DIR}/server-cert.pem \
  --tlskey={TLS_DIR}/server-key.pem
"#
    )
}

/// The setup script the task runs as root in the distro (`sh -s`): Docker,
/// the NVIDIA toolkit when `gpu`, the TLS files, `wsl.conf`, the start
/// script, and the marker last (so a failed run is retried).
pub fn provision_script(server: &tls::ServerPems, port: u16, gpu: bool, marker: &str) -> String {
    let mut s = String::from(
        r#"set -eu
export DEBIAN_FRONTEND=noninteractive
say() { echo "pumasi-docker: $*"; }
say "installing Docker (apt-get)"
apt-get update -q
apt-get install -y -q --no-install-recommends docker.io iptables ca-certificates curl gnupg util-linux
"#,
    );
    if gpu {
        s.push_str(
            r#"say "the Windows NVIDIA driver is there: installing the NVIDIA Container Toolkit"
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | gpg --dearmor --yes -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  > /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update -q
apt-get install -y -q nvidia-container-toolkit
nvidia-ctk runtime configure --runtime=docker
"#,
        );
    }
    s.push_str(&format!(
        "say \"writing the TLS files and wsl.conf\"\nmkdir -p {TLS_DIR} /etc/pumasi\nchmod 700 {TLS_DIR}\n"
    ));
    for (name, pem) in [
        ("ca.pem", &server.ca),
        ("server-cert.pem", &server.cert),
        ("server-key.pem", &server.key),
    ] {
        s.push_str(&heredoc(&format!("{TLS_DIR}/{name}"), pem));
    }
    s.push_str(&format!("chmod 600 {TLS_DIR}/*\n"));
    s.push_str(&heredoc("/etc/wsl.conf", WSL_CONF));
    s.push_str(&heredoc(START_SCRIPT, &start_script(port)));
    s.push_str(&format!("chmod 755 {START_SCRIPT}\n"));
    s.push_str(&format!("echo '{marker}' > {MARKER_FILE}\nsay \"ready\"\n"));
    s
}

/// `cat > path <<'EOF' ... EOF`, quoted so nothing in `body` expands.
fn heredoc(path: &str, body: &str) -> String {
    let body = if body.ends_with('\n') {
        body.to_string()
    } else {
        format!("{body}\n")
    };
    format!("cat > {path} <<'PUMASI_EOF'\n{body}PUMASI_EOF\n")
}

/// The `wsl.exe` arguments that run [`start_script`].
pub fn start_command() -> [&'static str; 6] {
    ["-d", DISTRO, "-u", "root", "--exec", START_SCRIPT]
}

/// Text `wsl.exe` printed: UTF-16LE (its default, with or without a BOM)
/// or UTF-8 (with `WSL_UTF8=1`), NULs and carriage returns dropped.
pub fn wsl_text(bytes: &[u8]) -> String {
    let utf16 = bytes.len() >= 2
        && (bytes.starts_with(&[0xFF, 0xFE])
            || bytes.iter().skip(1).step_by(2).take(8).all(|&b| b == 0));
    let text = if utf16 {
        let units: Vec<u16> = bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&c| u16::from_le_bytes(c))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    };
    text.trim_start_matches('\u{feff}')
        .chars()
        .filter(|&c| c != '\0' && c != '\r')
        .collect()
}

/// Whether `wsl.exe --list --quiet` printed [`DISTRO`].
pub fn lists_distro(list: &[u8]) -> bool {
    wsl_text(list).lines().any(|l| l.trim() == DISTRO)
}

/// Escapes text for an XML element.
fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The scheduled task: at boot, as [`ACCOUNT`] (password logon, so no one
/// needs to sign in), never stopped for time, batteries or idleness,
/// restarted when it fails to start, one instance.
pub fn task_xml(layout: &Layout) -> String {
    let exe = xml(&layout.pumasi_exe().display().to_string());
    let args = xml(&format!("{RUN_ARG} --dir \"{}\"", layout.root().display()));
    let dir = xml(&layout.root().display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>Pumasi</Author>
    <Description>Keeps dockerd running in the {DISTRO} WSL distro for the Pumasi agent, from boot and before anyone signs in (pumasi docker setup).</Description>
  </RegistrationInfo>
  <Triggers>
    <BootTrigger>
      <Enabled>true</Enabled>
    </BootTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{ACCOUNT}</UserId>
      <LogonType>Password</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>5</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{exe}</Command>
      <Arguments>{args}</Arguments>
      <WorkingDirectory>{dir}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"#
    )
}

/// UTF-16LE with a byte order mark, which `schtasks /XML` reads reliably.
pub fn utf16_file(text: &str) -> Vec<u8> {
    let mut out = vec![0xFF, 0xFE];
    for unit in text.encode_utf16() {
        out.extend_from_slice(&unit.to_le_bytes());
    }
    out
}

/// A password for [`ACCOUNT`] that meets Windows' complexity rules; nobody
/// types it, Task Scheduler keeps it.
pub fn new_password() -> String {
    use rand010::RngExt;
    const LOWER: &[u8] = b"abcdefghijkmnopqrstuvwxyz";
    const UPPER: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ";
    const DIGIT: &[u8] = b"23456789";
    const SYMBOL: &[u8] = b"!#%+-.=@_~";
    let mut rng = rand010::rng();
    let mut pick = |set: &[u8]| set[rng.random_range(0..set.len())] as char;
    let mut p: String = (0..28)
        .map(|i| match i % 4 {
            0 => pick(LOWER),
            1 => pick(UPPER),
            2 => pick(DIGIT),
            _ => pick(SYMBOL),
        })
        .collect();
    // Shuffle so the classes are not at fixed positions.
    let mut chars: Vec<char> = p.drain(..).collect();
    for i in (1..chars.len()).rev() {
        let j = rng.random_range(0..=i);
        chars.swap(i, j);
    }
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pems() -> tls::ServerPems {
        tls::ServerPems {
            ca: "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n".into(),
            cert: "-----BEGIN CERTIFICATE-----\nSRV\n-----END CERTIFICATE-----\n".into(),
            key: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n".into(),
        }
    }

    #[test]
    fn pins() {
        for arch in ["x86_64", "aarch64"] {
            let p = rootfs_pin(arch).unwrap();
            assert_eq!(p.sha256.len(), 64);
            assert!(
                p.urls
                    .iter()
                    .all(|u| u.starts_with("https://") && u.ends_with(p.file))
            );
            assert!(p.size > 20 << 20);
        }
        assert!(rootfs_pin("riscv64").is_none());
        assert!(DOCKER_CLI.urls[0].ends_with(DOCKER_CLI.file));
        assert_eq!(DOCKER_CLI.sha256.len(), 64);
    }

    #[test]
    fn layout_and_env() {
        let l = Layout::new(r"C:\ProgramData\Pumasi\wsl-docker");
        let i = Installed {
            layout: l.clone(),
            config: Config {
                port: 2399,
                rootfs: "x.tar.gz".into(),
                version: "1.11.0".into(),
            },
        };
        let env = i.env();
        assert_eq!(env[0], ("DOCKER_HOST", "tcp://127.0.0.1:2399".into()));
        assert_eq!(env[1], ("DOCKER_TLS_VERIFY", "1".into()));
        assert_eq!(env[2].1, l.client_tls().into_os_string());
        assert!(i.serves("docker"));
        assert!(i.serves(&l.docker_exe().display().to_string()));
        assert!(!i.serves(r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"));
        assert!(!i.serves("podman"));
        // Not set up here (or not Windows): the CLI is run as given.
        let (prog, env) = resolve("podman");
        assert_eq!(prog, "podman");
        assert!(env.is_empty());
    }

    #[test]
    fn installed_needs_the_cli_and_the_config() {
        let dir = crate::tempdir::TempDir::new("wsl-docker").unwrap();
        let l = Layout::new(dir.path());
        assert!(Installed::at(l.clone()).is_none());
        std::fs::create_dir_all(l.bin_dir()).unwrap();
        std::fs::write(l.docker_exe(), b"MZ").unwrap();
        assert!(Installed::at(l.clone()).is_none(), "no config yet");
        let c = Config {
            port: DEFAULT_PORT,
            rootfs: "r.tar.gz".into(),
            version: "1.11.0".into(),
        };
        c.write(&l).unwrap();
        assert_eq!(Installed::at(l.clone()).unwrap().config, c);
    }

    #[test]
    fn provisioning() {
        let marker = provision_marker(&pems(), true);
        assert!(marker.starts_with(&format!("pumasi-docker v{PROVISION_VERSION} tls=")));
        assert!(marker.ends_with("gpu=nvidia"));
        assert_ne!(marker, provision_marker(&pems(), false));
        let mut other = pems();
        other.key.push('x');
        assert_ne!(
            provision_marker(&other, true),
            marker,
            "a new certificate sets the distro up again"
        );

        let s = provision_script(&pems(), 2376, true, &marker);
        assert!(s.starts_with("set -eu\n"));
        assert!(s.contains("apt-get install -y -q --no-install-recommends docker.io"));
        assert!(s.contains("nvidia-container-toolkit"));
        assert!(s.contains("nvidia-ctk runtime configure --runtime=docker"));
        assert!(s.contains("cat > /etc/pumasi/tls/server-key.pem <<'PUMASI_EOF'\n-----BEGIN PRIVATE KEY-----\nKEY\n"));
        assert!(s.contains("[automount]\nenabled=false"));
        assert!(s.contains("[interop]\nenabled=false"));
        assert!(s.contains("--host=tcp://127.0.0.1:2376"));
        assert!(s.contains("--tlsverify"));
        // The marker goes in last, once everything else worked.
        let tail = s.trim_end().lines().rev().take(2).collect::<Vec<_>>();
        assert_eq!(tail[1], format!("echo '{marker}' > {MARKER_FILE}"));
        // Every heredoc is closed.
        assert_eq!(
            s.matches("<<'PUMASI_EOF'").count(),
            s.lines().filter(|l| *l == "PUMASI_EOF").count()
        );

        let plain = provision_marker(&pems(), false);
        let s = provision_script(&pems(), 2376, false, &plain);
        assert!(!s.contains("nvidia"));
    }

    #[test]
    fn provision_script_is_valid_sh() {
        // `sh -n` parses without running.
        let script = provision_script(&pems(), 2376, true, "m");
        let Ok(out) = std::process::Command::new("sh")
            .arg("-n")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin.take().unwrap().write_all(script.as_bytes())?;
                c.wait_with_output()
            })
        else {
            return; // no sh (Windows)
        };
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let start = start_script(2376);
        let out = std::process::Command::new("sh")
            .args(["-n", "-c", &start])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn reads_wsl_output() {
        let utf16: Vec<u8> = "Ubuntu\r\npumasi-docker\r\n"
            .encode_utf16()
            .flat_map(|u| u.to_le_bytes())
            .collect();
        assert!(lists_distro(&utf16));
        let mut bom = vec![0xFF, 0xFE];
        bom.extend_from_slice(&utf16);
        assert!(lists_distro(&bom));
        assert!(lists_distro(b"docker-desktop\npumasi-docker\n"));
        assert!(!lists_distro(b"pumasi-docker-old\n"));
        assert!(!lists_distro(b""));
        assert_eq!(
            wsl_text(b"WSL version: 2.6.1.0\r\n"),
            "WSL version: 2.6.1.0\n"
        );
    }

    #[test]
    fn scheduled_task() {
        let l = Layout::new(r"C:\Program Data & Co\Pumasi\wsl-docker");
        let x = task_xml(&l);
        assert!(x.contains("<BootTrigger>"));
        assert!(x.contains(&format!("<UserId>{ACCOUNT}</UserId>")));
        assert!(x.contains("<LogonType>Password</LogonType>"));
        assert!(x.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"));
        assert!(x.contains("<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>"));
        let exe = l.pumasi_exe().display().to_string().replace('&', "&amp;");
        assert!(exe.starts_with(r"C:\Program Data &amp; Co\Pumasi\wsl-docker"));
        assert!(x.contains(&format!("<Command>{exe}</Command>")), "{x}");
        assert!(x.contains(&format!(
            r"<Arguments>{RUN_ARG} --dir &quot;C:\Program Data &amp; Co\Pumasi\wsl-docker&quot;</Arguments>"
        )));
        let bytes = utf16_file("<a/>");
        assert_eq!(&bytes[..4], &[0xFF, 0xFE, b'<', 0]);
    }

    #[test]
    fn passwords() {
        let a = new_password();
        assert_eq!(a.chars().count(), 28);
        assert!(a.chars().any(|c| c.is_ascii_lowercase()));
        assert!(a.chars().any(|c| c.is_ascii_uppercase()));
        assert!(a.chars().any(|c| c.is_ascii_digit()));
        assert!(a.chars().any(|c| !c.is_ascii_alphanumeric()));
        assert!(!a.contains(ACCOUNT));
        assert_ne!(a, new_password());
    }
}
