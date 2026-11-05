//! `pumasi vm setup`: downloads a pinned guest kernel (and Firecracker on
//! Linux, QEMU on Windows) into the VM directory, checking each download
//! against a SHA-256 recorded here. On macOS it also signs the
//! Virtualization.framework helper.

use std::{io::Read, path::Path};

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use pumasi_proto::registry;

use super::{EMBEDDED_GUEST, FIRECRACKER_FILE, GUEST_FILE, KERNEL_FILE, QEMU_DIR, vmm};

/// QEMU for Windows (x86_64): Stefan Weil's builds, the ones qemu.org links
/// to. The NSIS installer runs silently into `<vm dir>/qemu`. Its build has
/// the `microvm` machine and the `whpx` accelerator (checked in CI). Older
/// builds move from `w64/` to `w64/<year>/`, so both are tried.
pub struct QemuInstaller {
    pub file: &'static str,
    pub version: &'static str,
    pub urls: &'static [&'static str],
    pub sha256: &'static str,
    pub size: u64,
}

pub const QEMU_WINDOWS: QemuInstaller = QemuInstaller {
    file: "qemu-w64-setup-20260811.exe",
    version: "11.1.0 (20260811)",
    urls: &[
        "https://qemu.weilnetz.de/w64/qemu-w64-setup-20260811.exe",
        "https://qemu.weilnetz.de/w64/2026/qemu-w64-setup-20260811.exe",
    ],
    sha256: "f98a8aeb5f7faea9765b6dee28316c266cd179d80354a2fed8e50176f9a2e59f",
    size: 206_615_928,
};

/// Names the installer QEMU in `<vm dir>/qemu` came from, so a newer pin
/// replaces it.
const QEMU_MARKER: &str = "pumasi-installer.txt";

/// Firecracker's CI guest kernel (virtio-mmio, ext4, overlayfs, initramfs).
const KERNEL_VERSION: &str = "6.1.102";

/// macOS kernels: Virtualization.framework has only virtio-pci devices,
/// which Firecracker's CI kernels have since its 1.15 configuration. Apple
/// Silicon boots its arm64 Image as is; Intel Macs need a bzImage, built
/// from the same configuration by `.github/workflows/guest-kernel.yml`
/// (`scripts/kernel/build.sh`) and published as a prerelease.
const VZ_KERNEL_VERSION: &str = "6.1.155";
const VZ_KERNEL_AARCH64_URL: &str =
    "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/aarch64/vmlinux-6.1.155";
const VZ_KERNEL_AARCH64_SHA256: &str =
    "e3544b10603acbf3db492cb52e000d22ba202cb4b63b9add027565683e11c591";
const VZ_KERNEL_X86_64_URL: &str =
    "https://github.com/syang0531/pumasi/releases/download/guest-kernel-6.1.155-1/bzImage-x86_64";
const VZ_KERNEL_X86_64_SHA256: &str =
    "a18b8737ab2f12b9cce9304c3f5295ef0a37492affa38ee57d46a57b9ae6a014";

/// Where the guest kernel for `os` and `arch` comes from, its SHA-256 and
/// a label.
fn kernel_source(os: &str, arch: &str) -> Option<(String, &'static str, String)> {
    if os == "macos" {
        return match arch {
            "aarch64" => Some((
                VZ_KERNEL_AARCH64_URL.into(),
                VZ_KERNEL_AARCH64_SHA256,
                format!("Linux {VZ_KERNEL_VERSION}, arm64 Image"),
            )),
            "x86_64" => Some((
                VZ_KERNEL_X86_64_URL.into(),
                VZ_KERNEL_X86_64_SHA256,
                format!("Linux {VZ_KERNEL_VERSION}, bzImage"),
            )),
            _ => None,
        };
    }
    let pin = PINNED.iter().find(|p| p.arch == arch)?;
    Some((
        format!(
            "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/{arch}/vmlinux-{KERNEL_VERSION}"
        ),
        pin.kernel_sha256,
        format!("Linux {KERNEL_VERSION}"),
    ))
}
const FIRECRACKER_VERSION: &str = "v1.10.1";

struct Pinned {
    arch: &'static str,
    kernel_sha256: &'static str,
    firecracker_sha256: &'static str,
}

const PINNED: &[Pinned] = &[
    Pinned {
        arch: "x86_64",
        kernel_sha256: "49ba99a5299444ac59dda2efc3569cc2d58a5d72ea6475a6bfc37aa0bf322e54",
        firecracker_sha256: "36112969952b0e34fadcfca769d48a55dc22cbba99af17e02bd0e24fc35adc77",
    },
    Pinned {
        arch: "aarch64",
        kernel_sha256: "bb1f50912d63a8ca5e92d488984875e1177eb9283050ffa592a8cb455cada52d",
        firecracker_sha256: "9e3641071de140979afaac0c52fdc107baeba398bdb5709c12f77ee469207fcd",
    },
];

/// What `setup` did, for printing.
#[derive(Debug, Default)]
pub struct SetupReport {
    pub installed: Vec<String>,
    pub kept: Vec<String>,
}

/// Installs the kernel, Firecracker (Linux only) and the guest init into
/// `dir`. Files already present are kept unless `force`.
pub async fn setup(dir: &Path, guest: Option<&Path>, force: bool) -> Result<SetupReport> {
    let arch = std::env::consts::ARCH;
    let (kernel_url, kernel_sha256, kernel_label) = kernel_source(std::env::consts::OS, arch)
        .with_context(|| format!("no microVM kernel for {arch}"))?;
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let http = registry::http_client()?;
    let mut report = SetupReport::default();

    // macOS 11+: the signed copy of this binary that runs the VMs. Made
    // even where the hardware cannot virtualize, which `vm status` reports.
    if cfg!(target_os = "macos") {
        if super::vz::framework_present() {
            let helper = super::vz::ensure_helper(dir)?;
            report.installed.push(format!(
                "{} (this binary, signed for Virtualization.framework)",
                helper.display()
            ));
        } else {
            eprintln!(
                "note: macOS 10.15 has no Virtualization.framework, so this Mac runs WASM jobs only; the kernel is kept for a later macOS"
            );
        }
    }

    let kernel = dir.join(KERNEL_FILE);
    if kernel.exists() && !force {
        report.kept.push(kernel.display().to_string());
    } else {
        let bytes = fetch(&http, &kernel_url, kernel_sha256).await?;
        write_atomic(&kernel, &bytes, 0o644)?;
        report
            .installed
            .push(format!("{} ({kernel_label})", kernel.display()));
    }

    if cfg!(target_os = "linux") {
        let pin = PINNED
            .iter()
            .find(|p| p.arch == arch)
            .with_context(|| format!("no Firecracker for {arch}"))?;
        let fc = dir.join(FIRECRACKER_FILE);
        if fc.exists() && !force {
            report.kept.push(fc.display().to_string());
        } else {
            let url = format!(
                "https://github.com/firecracker-microvm/firecracker/releases/download/{FIRECRACKER_VERSION}/firecracker-{FIRECRACKER_VERSION}-{arch}.tgz"
            );
            let tgz = fetch(&http, &url, pin.firecracker_sha256).await?;
            let want = format!(
                "release-{FIRECRACKER_VERSION}-{arch}/firecracker-{FIRECRACKER_VERSION}-{arch}"
            );
            let bin = extract(&tgz, &want)?;
            write_atomic(&fc, &bin, 0o755)?;
            report
                .installed
                .push(format!("{} ({FIRECRACKER_VERSION})", fc.display()));
        }
    }

    if cfg!(windows) && arch == "x86_64" {
        install_qemu_windows(&http, dir, force, &mut report).await?;
    }

    let dest = dir.join(GUEST_FILE);
    match guest {
        Some(src) => {
            let bytes = std::fs::read(src).with_context(|| format!("read {}", src.display()))?;
            ensure!(
                bytes.starts_with(b"\x7fELF"),
                "{} is not a Linux binary; build it with `cargo build --release -p pumasi-guest --target {arch}-unknown-linux-musl`",
                src.display()
            );
            write_atomic(&dest, &bytes, 0o755)?;
            let note = if EMBEDDED_GUEST.is_some() {
                " (unused: this binary has its own guest init built in)"
            } else {
                ""
            };
            report.installed.push(format!("{}{note}", dest.display()));
        }
        None if EMBEDDED_GUEST.is_some() => report
            .kept
            .push("guest init (built into this binary)".into()),
        None if dest.exists() => report.kept.push(dest.display().to_string()),
        None => {}
    }
    Ok(report)
}

/// Downloads the pinned QEMU installer and runs it silently into
/// `<dir>/qemu`. It installs for all users, so it needs an administrator
/// shell.
async fn install_qemu_windows(
    http: &reqwest::Client,
    dir: &Path,
    force: bool,
    report: &mut SetupReport,
) -> Result<()> {
    let pin = &QEMU_WINDOWS;
    let target = dir.join(QEMU_DIR);
    let exe = target.join(format!("{}.exe", vmm::qemu_name()));
    let marker = target.join(QEMU_MARKER);
    let current = std::fs::read_to_string(&marker).unwrap_or_default();
    if exe.is_file() && current.trim() == pin.file && !force {
        report
            .kept
            .push(format!("{} (QEMU {})", exe.display(), pin.version));
        return Ok(());
    }
    let installer = dir.join(pin.file);
    eprintln!(
        "downloading QEMU {} for Windows ({} MiB)...",
        pin.version,
        pin.size >> 20
    );
    download_to(http, pin.urls, pin.sha256, pin.size, &installer).await?;
    eprintln!("installing QEMU into {}...", target.display());
    let status = run_installer(&installer, &target).await;
    let _ = std::fs::remove_file(&installer);
    let status = status?;
    ensure!(
        status.success(),
        "the QEMU installer failed ({status}); run `pumasi vm setup` again from an administrator terminal"
    );
    ensure!(
        exe.is_file(),
        "the QEMU installer finished but {} is missing",
        exe.display()
    );
    std::fs::write(&marker, pin.file)?;
    report
        .installed
        .push(format!("{} (QEMU {})", exe.display(), pin.version));
    Ok(())
}

#[cfg(windows)]
async fn run_installer(installer: &Path, target: &Path) -> Result<std::process::ExitStatus> {
    const INSTALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);
    // NSIS wants `/D=` last and unquoted, even with spaces in the path.
    let mut cmd = tokio::process::Command::new(installer);
    cmd.arg("/S")
        .raw_arg(format!("/D={}", target.display()))
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        // ERROR_ELEVATION_REQUIRED: the installer asks for administrator
        // rights, which a plain process start cannot grant.
        Err(e) if e.raw_os_error() == Some(740) => bail!(
            "installing QEMU needs administrator rights: run `pumasi vm setup` from a terminal opened with \"Run as administrator\""
        ),
        Err(e) => return Err(e).with_context(|| format!("run {}", installer.display())),
    };
    match tokio::time::timeout(INSTALL_TIMEOUT, child.wait()).await {
        Ok(status) => Ok(status?),
        Err(_) => bail!(
            "the QEMU installer did not finish within {} minutes",
            INSTALL_TIMEOUT.as_secs() / 60
        ),
    }
}

#[cfg(not(windows))]
async fn run_installer(_installer: &Path, _target: &Path) -> Result<std::process::ExitStatus> {
    bail!("the QEMU installer runs only on Windows")
}

/// Streams the first URL that answers into `dest`, checking size and
/// SHA-256 before the file gets its final name.
async fn download_to(
    http: &reqwest::Client,
    urls: &[&str],
    sha256: &str,
    size: u64,
    dest: &Path,
) -> Result<()> {
    let tmp = dest.with_extension("download");
    let mut last_err = None;
    for url in urls {
        let resp = match http.get(*url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                last_err = Some(anyhow::anyhow!("GET {url}: {}", r.status()));
                continue;
            }
            Err(e) => {
                last_err = Some(anyhow::Error::new(e).context(format!("GET {url}")));
                continue;
            }
        };
        let mut resp = resp;
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("create {}", tmp.display()))?;
        let mut hasher = Sha256::new();
        let mut n = 0u64;
        while let Some(chunk) = resp.chunk().await.with_context(|| format!("GET {url}"))? {
            n += chunk.len() as u64;
            ensure!(n <= size, "{url}: larger than the pinned {size} bytes");
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        drop(file);
        let got = registry::hex(&hasher.finalize());
        if n != size || got != sha256 {
            let _ = std::fs::remove_file(&tmp);
            bail!(
                "{url}: checksum mismatch (got {got}, {n} bytes; expected {sha256}, {size} bytes)"
            );
        }
        std::fs::rename(&tmp, dest)?;
        return Ok(());
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no download URL")))
}

async fn fetch(http: &reqwest::Client, url: &str, sha256: &str) -> Result<Vec<u8>> {
    let resp = http
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    ensure!(resp.status().is_success(), "GET {url}: {}", resp.status());
    let bytes = resp.bytes().await.with_context(|| format!("GET {url}"))?;
    let got = registry::hex(&Sha256::digest(&bytes));
    if got != sha256 {
        bail!("{url}: checksum mismatch (got {got}, expected {sha256})");
    }
    Ok(bytes.to_vec())
}

fn extract(tgz: &[u8], want: &str) -> Result<Vec<u8>> {
    let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(tgz));
    for e in ar.entries()? {
        let mut e = e?;
        if e.path()?.to_string_lossy() == want {
            let mut out = Vec::new();
            e.read_to_end(&mut out)?;
            return Ok(out);
        }
    }
    bail!("{want} not found in the Firecracker release")
}

fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let tmp = path.with_extension("download");
    std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_platform_has_a_pinned_kernel() {
        for (os, arch) in [
            ("linux", "x86_64"),
            ("linux", "aarch64"),
            ("windows", "x86_64"),
            ("macos", "x86_64"),
            ("macos", "aarch64"),
        ] {
            let (url, sha, _) = kernel_source(os, arch).unwrap();
            assert!(url.starts_with("https://"), "{url}");
            assert_eq!(sha.len(), 64);
        }
        let (url, _, label) = kernel_source("macos", "x86_64").unwrap();
        assert!(url.ends_with("bzImage-x86_64") && label.contains("bzImage"));
        assert!(kernel_source("linux", "riscv64").is_none());
    }
}
