//! Linux PCI GPUs from sysfs, and whether one can be passed through to a
//! microVM with VFIO.
//!
//! Passthrough needs, for each GPU:
//!
//! - the IOMMU on (`intel_iommu=on` or `amd_iommu=on` on the kernel
//!   command line; AMD usually has it on), so the GPU has an IOMMU group,
//! - the GPU bound to `vfio-pci` instead of its graphics driver, so the
//!   host does not use it (and it drives no display),
//! - every other device of its IOMMU group bound to `vfio-pci` too (its
//!   HDMI audio function, typically), or a PCI bridge, or driverless,
//! - `/dev/vfio/<group>` usable by the agent's user.
//!
//! `pumasi gpu check` prints what is missing and the commands that fix it;
//! the agent never rebinds drivers itself.

use std::path::{Path, PathBuf};

use pumasi_proto::gpu::{GpuInfo, GpuVendor};

const PCI: &str = "/sys/bus/pci/devices";
const KFD_NODES: &str = "/sys/class/kfd/kfd/topology/nodes";

fn read(p: impl AsRef<Path>) -> Option<String> {
    std::fs::read_to_string(p)
        .ok()
        .map(|s| s.trim().to_string())
}

fn hex(p: impl AsRef<Path>) -> Option<u64> {
    read(p).and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
}

/// The kernel driver bound to a PCI device, if any.
pub fn driver_of(pci: &str) -> Option<String> {
    if pci.is_empty() {
        return None;
    }
    driver_in(Path::new(PCI), pci)
}

fn driver_in(root: &Path, pci: &str) -> Option<String> {
    let link = std::fs::read_link(root.join(pci).join("driver")).ok()?;
    link.file_name().map(|n| n.to_string_lossy().into_owned())
}

/// Display controllers (PCI class 0x03) other than NVIDIA ones NVML
/// already listed; NVIDIA GPUs appear here when bound to `vfio-pci`.
pub fn pci_gpus() -> Vec<GpuInfo> {
    pci_gpus_in(Path::new(PCI), Path::new(KFD_NODES))
}

fn pci_gpus_in(root: &Path, kfd: &Path) -> Vec<GpuInfo> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut addrs: Vec<String> = entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    addrs.sort();
    let mut out = Vec::new();
    for addr in addrs {
        let dev = root.join(&addr);
        let Some(class) = hex(dev.join("class")) else {
            continue;
        };
        if class >> 16 != 0x03 {
            continue;
        }
        let vendor_id = hex(dev.join("vendor")).unwrap_or(0) as u16;
        let device_id = hex(dev.join("device")).unwrap_or(0) as u16;
        let vendor = GpuVendor::from_pci(vendor_id);
        let driver = driver_in(root, &addr).unwrap_or_default();
        let drm = drm_card(&dev);
        let name = read(dev.join("product_name"))
            .or_else(|| read(dev.join("label")))
            .unwrap_or_else(|| {
                let v = match vendor {
                    GpuVendor::Nvidia => "NVIDIA",
                    GpuVendor::Amd => "AMD",
                    GpuVendor::Intel => "Intel",
                    _ => "PCI",
                };
                format!("{v} GPU {device_id:04x}")
            });
        let vram_mib = read(dev.join("mem_info_vram_total"))
            .and_then(|s| s.parse::<u64>().ok())
            .map(|b| b >> 20)
            .unwrap_or(0);
        let render_node = render_node(&dev)
            .map(|r| format!("/dev/dri/{r}"))
            .unwrap_or_default();
        let display = drm.as_ref().is_some_and(|c| connected(&dev, c));
        let (fp32_gflops, compute) = if vendor == GpuVendor::Amd {
            amd_estimate(kfd, &addr).unwrap_or((0, String::new()))
        } else {
            (0, String::new())
        };
        out.push(GpuInfo {
            index: 0,
            vendor,
            name,
            uuid: String::new(),
            pci: addr,
            vram_mib,
            driver,
            compute,
            display,
            fp32_gflops,
            measured: false,
            mem_bw_gb_s: 0,
            hash_rate: 0,
            docker: false,
            vfio: false,
            render_node,
        });
    }
    out
}

fn drm_card(dev: &Path) -> Option<String> {
    std::fs::read_dir(dev.join("drm"))
        .ok()?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .find(|n| n.starts_with("card"))
}

fn render_node(dev: &Path) -> Option<String> {
    std::fs::read_dir(dev.join("drm"))
        .ok()?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .find(|n| n.starts_with("renderD"))
}

/// Whether any connector of the card has a monitor.
fn connected(dev: &Path, card: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dev.join("drm").join(card)) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with(&format!("{card}-"))
            && read(e.path().join("status")).as_deref() == Some("connected")
    })
}

/// FP32 GFLOPS of an AMD GPU from the KFD topology: SIMDs times lanes
/// (16 on GCN and CDNA, 32 on RDNA) times 2 FLOPs per clock. RDNA 3's
/// dual issue is not counted.
fn amd_estimate(kfd: &Path, pci: &str) -> Option<(u32, String)> {
    let location = pci_location(pci)?;
    for e in std::fs::read_dir(kfd).ok()?.flatten() {
        let props = read(e.path().join("properties"))?;
        let field = |k: &str| {
            props.lines().find_map(|l| {
                let (key, v) = l.split_once(' ')?;
                (key == k).then(|| v.trim().parse::<u64>().ok()).flatten()
            })
        };
        if field("location_id") != Some(location) {
            continue;
        }
        let simds = field("simd_count")?;
        let mhz = field("max_engine_clk_fcompute")?;
        let version = field("gfx_target_version").unwrap_or(0);
        let lanes = if version >= 100_000 { 32 } else { 16 };
        let gflops = simds * lanes * 2 * mhz / 1000;
        let gfx = if version > 0 {
            format!(
                "gfx{}{:x}{:x}",
                version / 10_000,
                (version / 100) % 100,
                version % 100
            )
        } else {
            String::new()
        };
        return Some((gflops.min(u32::MAX as u64) as u32, gfx));
    }
    None
}

/// KFD's `location_id`: bus << 8 | device << 3 | function.
fn pci_location(pci: &str) -> Option<u64> {
    let mut parts = pci.rsplitn(3, ':');
    let devfn = parts.next()?;
    let bus = u64::from_str_radix(parts.next()?, 16).ok()?;
    let (dev, func) = devfn.split_once('.')?;
    let dev = u64::from_str_radix(dev, 16).ok()?;
    let func = u64::from_str_radix(func, 16).ok()?;
    Some(bus << 8 | dev << 3 | func)
}

/// The IOMMU group of a PCI device.
pub fn iommu_group(pci: &str) -> Option<String> {
    group_in(Path::new(PCI), pci)
}

fn group_in(root: &Path, pci: &str) -> Option<String> {
    let link = std::fs::read_link(root.join(pci).join("iommu_group")).ok()?;
    link.file_name().map(|n| n.to_string_lossy().into_owned())
}

/// The other devices of a device's IOMMU group.
fn group_members(root: &Path, pci: &str) -> Vec<String> {
    let dir: PathBuf = root.join(pci).join("iommu_group").join("devices");
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n != pci)
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// Whether the GPU at `pci` can be passed through now, or why not.
pub fn ready(pci: &str) -> Result<(), String> {
    ready_in(Path::new(PCI), Path::new("/dev/vfio"), pci)
}

fn ready_in(root: &Path, dev_vfio: &Path, pci: &str) -> Result<(), String> {
    if pci.is_empty() {
        return Err("no PCI address".into());
    }
    let group = group_in(root, pci)
        .ok_or("no IOMMU group: turn the IOMMU on (intel_iommu=on or amd_iommu=on)")?;
    match driver_in(root, pci).as_deref() {
        Some("vfio-pci") => {}
        Some(d) => return Err(format!("bound to {d}, not vfio-pci")),
        None => return Err("bound to no driver, not vfio-pci".into()),
    }
    for other in group_members(root, pci) {
        let bridge = hex(root.join(&other).join("class")).is_some_and(|c| c >> 8 == 0x0604);
        match driver_in(root, &other).as_deref() {
            None | Some("vfio-pci") | Some("pcieport") => {}
            _ if bridge => {}
            Some(d) => {
                return Err(format!(
                    "IOMMU group {group} also holds {other} (bound to {d}); bind it to vfio-pci too"
                ));
            }
        }
    }
    let node = dev_vfio.join(&group);
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&node)
        .map_err(|e| format!("cannot open {}: {e}", node.display()))?;
    Ok(())
}

/// Every PCI function of the GPU's slot and IOMMU group that must go to
/// `vfio-pci` with it.
pub fn passthrough_set(pci: &str) -> Vec<String> {
    let mut v = vec![pci.to_string()];
    let root = Path::new(PCI);
    for other in group_members(root, pci) {
        let bridge = hex(root.join(&other).join("class")).is_some_and(|c| c >> 8 == 0x0604);
        if !bridge {
            v.push(other);
        }
    }
    v
}

/// Commands that bind a GPU (and its group) to `vfio-pci` until the next
/// reboot, for `pumasi gpu check`.
pub fn bind_commands(pci: &str) -> Vec<String> {
    let mut cmds = vec!["sudo modprobe vfio-pci".to_string()];
    for d in passthrough_set(pci) {
        cmds.push(format!(
            "echo vfio-pci | sudo tee /sys/bus/pci/devices/{d}/driver_override"
        ));
        if driver_of(&d).is_some() {
            cmds.push(format!(
                "echo {d} | sudo tee /sys/bus/pci/devices/{d}/driver/unbind"
            ));
        }
        cmds.push(format!("echo {d} | sudo tee /sys/bus/pci/drivers_probe"));
    }
    cmds
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn fake_dev(
        root: &Path,
        addr: &str,
        class: &str,
        vendor: &str,
        driver: Option<&str>,
        group: &str,
    ) {
        let d = root.join(addr);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("class"), class).unwrap();
        std::fs::write(d.join("vendor"), vendor).unwrap();
        std::fs::write(d.join("device"), "0x2504").unwrap();
        if let Some(drv) = driver {
            let target = root.join("drivers").join(drv);
            std::fs::create_dir_all(&target).unwrap();
            symlink(&target, d.join("driver")).unwrap();
        }
        let g = root.join("groups").join(group);
        std::fs::create_dir_all(g.join("devices")).unwrap();
        symlink(&g, d.join("iommu_group")).unwrap();
        let _ = symlink(&d, g.join("devices").join(addr));
    }

    #[test]
    fn passthrough_readiness() {
        let tmp = crate::tempdir::TempDir::new("vfio").unwrap();
        let root = tmp.path().join("pci");
        let devv = tmp.path().join("dev");
        std::fs::create_dir_all(&devv).unwrap();
        fake_dev(
            &root,
            "0000:01:00.0",
            "0x030000",
            "0x10de",
            Some("vfio-pci"),
            "13",
        );
        fake_dev(
            &root,
            "0000:01:00.1",
            "0x040300",
            "0x10de",
            Some("snd_hda_intel"),
            "13",
        );
        fake_dev(
            &root,
            "0000:02:00.0",
            "0x030000",
            "0x1002",
            Some("amdgpu"),
            "14",
        );
        let err = ready_in(&root, &devv, "0000:01:00.0").unwrap_err();
        assert!(err.contains("0000:01:00.1"), "{err}");
        // Bind the audio function too.
        std::fs::remove_file(root.join("0000:01:00.1/driver")).unwrap();
        symlink(
            root.join("drivers/vfio-pci"),
            root.join("0000:01:00.1/driver"),
        )
        .unwrap();
        let err = ready_in(&root, &devv, "0000:01:00.0").unwrap_err();
        assert!(err.contains("cannot open"), "{err}");
        std::fs::write(devv.join("13"), b"").unwrap();
        ready_in(&root, &devv, "0000:01:00.0").unwrap();
        let err = ready_in(&root, &devv, "0000:02:00.0").unwrap_err();
        assert!(err.contains("amdgpu"), "{err}");

        let gpus = pci_gpus_in(&root, &tmp.path().join("kfd"));
        assert_eq!(gpus.len(), 2, "the audio function is not a GPU");
        assert_eq!(gpus[0].vendor, GpuVendor::Nvidia);
        assert_eq!(gpus[0].driver, "vfio-pci");
        assert_eq!(gpus[1].vendor, GpuVendor::Amd);
    }

    #[test]
    fn amd_throughput_from_kfd() {
        let tmp = crate::tempdir::TempDir::new("kfd").unwrap();
        let node = tmp.path().join("1");
        std::fs::create_dir_all(&node).unwrap();
        // An RX 6800 XT: 144 SIMD32 units at 2250 MHz, about 20.7 TFLOPS.
        std::fs::write(
            node.join("properties"),
            "simd_count 144\nlocation_id 768\nmax_engine_clk_fcompute 2250\ngfx_target_version 100300\n",
        )
        .unwrap();
        assert_eq!(pci_location("0000:03:00.0"), Some(768));
        let (gflops, gfx) = amd_estimate(tmp.path(), "0000:03:00.0").unwrap();
        assert_eq!(gflops, 20_736);
        assert_eq!(gfx, "gfx1030");
        assert!(amd_estimate(tmp.path(), "0000:04:00.0").is_none());
    }
}
