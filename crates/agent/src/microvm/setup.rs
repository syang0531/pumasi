//! `pumasi vm setup`: downloads a pinned guest kernel (and Firecracker on
//! Linux) into the VM directory, checking each download against a SHA-256
//! recorded here.

use std::{io::Read, path::Path};

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};

use super::{FIRECRACKER_FILE, GUEST_FILE, KERNEL_FILE, registry};

/// Firecracker's CI guest kernel (virtio-mmio, ext4, overlayfs, initramfs).
const KERNEL_VERSION: &str = "6.1.102";
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
    let pin = PINNED
        .iter()
        .find(|p| p.arch == arch)
        .with_context(|| format!("no microVM kernel for {arch}"))?;
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let http = registry::http_client()?;
    let mut report = SetupReport::default();

    let kernel = dir.join(KERNEL_FILE);
    if kernel.exists() && !force {
        report.kept.push(kernel.display().to_string());
    } else {
        let url = format!(
            "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/{arch}/vmlinux-{KERNEL_VERSION}"
        );
        let bytes = fetch(&http, &url, pin.kernel_sha256).await?;
        write_atomic(&kernel, &bytes, 0o644)?;
        report
            .installed
            .push(format!("{} (Linux {KERNEL_VERSION})", kernel.display()));
    }

    if cfg!(target_os = "linux") {
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
            report.installed.push(dest.display().to_string());
        }
        None if dest.exists() => report.kept.push(dest.display().to_string()),
        None => {}
    }
    Ok(report)
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
