//! GPUs on this machine: what they are, what runs on them, and how fast.
//!
//! - NVIDIA: NVML (loaded at run time) for the list, the CUDA driver for
//!   the benchmark and the GPU challenge ([`cuda`]).
//! - AMD (Linux): sysfs and the KFD topology; FP32 is estimated from the
//!   SIMD count and clock. Docker jobs get `/dev/kfd` and the GPU's render
//!   node.
//! - Intel and Apple GPUs are listed for information only: no executor
//!   can use them yet (the WebGPU executor of a later phase will).
//! - Any GPU bound to `vfio-pci` on Linux, with a clean IOMMU group, can
//!   be passed through to microVMs ([`vfio`]).

pub mod cuda;
pub mod docker_check;
pub mod nvml;
pub mod vfio;

use std::{collections::HashSet, path::Path, time::Duration};

use pumasi_proto::gpu::{DeviceProfile, GPU_BENCH_VERSION, GpuInfo, GpuVendor};
use tracing::{debug, info, warn};

/// What decides which GPUs an executor may use here.
#[derive(Debug, Clone, Default)]
pub struct GpuEnv {
    /// The Docker executor is on, and Docker can reach NVIDIA GPUs
    /// (`--gpus`: NVIDIA Container Toolkit, or Docker Desktop on Windows).
    pub docker_nvidia: bool,
    /// The Docker executor is on and runs on this machine's kernel (Linux),
    /// so AMD GPUs can be handed to containers as devices.
    pub docker_amd: bool,
    /// The microVM executor can pass GPUs through (QEMU with KVM and the
    /// GPU guest kernel installed).
    pub vfio: bool,
    /// Measure the GPUs (FP32, memory bandwidth, hash rate).
    pub bench: bool,
}

/// Lists this machine's GPUs and marks what each can run. Never fails:
/// what could not be found goes to `notes`.
pub fn detect(env: &GpuEnv, notes: &mut Vec<String>) -> Vec<GpuInfo> {
    let mut gpus = match nvml::list(0) {
        Ok(g) => g,
        Err(e) => {
            notes.push(format!("NVIDIA: {e:#}"));
            Vec::new()
        }
    };
    if cfg!(target_os = "linux") {
        let known: HashSet<String> = gpus.iter().map(|g| g.pci.clone()).collect();
        for g in vfio::pci_gpus() {
            if !known.contains(&g.pci) {
                gpus.push(g);
            }
        }
        for g in gpus.iter_mut() {
            if let Some(d) = vfio::driver_of(&g.pci) {
                // NVML reports the driver version; keep it, the kernel
                // driver matters only when it is vfio-pci.
                if d == "vfio-pci" || g.driver.is_empty() {
                    g.driver = d;
                }
            }
        }
    } else if cfg!(target_os = "macos") {
        gpus.extend(macos_gpus());
    }
    for (i, g) in gpus.iter_mut().enumerate() {
        g.index = i as u32;
    }
    mark_docker(&mut gpus, env);
    for g in gpus.iter_mut() {
        if cfg!(target_os = "linux") && env.vfio && matches!(g.vendor, GpuVendor::Nvidia) {
            match vfio::ready(&g.pci) {
                Ok(()) if g.display => notes.push(format!(
                    "GPU {} drives a display; not passed through",
                    g.pci
                )),
                Ok(()) => g.vfio = true,
                Err(why) => debug!(gpu = %g.pci, "no passthrough: {why}"),
            }
        }
    }
    if env.bench {
        for g in gpus.iter_mut() {
            if g.vendor != GpuVendor::Nvidia || g.driver == "vfio-pci" || g.pci.is_empty() {
                continue;
            }
            match cuda::Gpu::open(&g.pci).and_then(|c| c.measure(g.vram_mib)) {
                Ok(m) => {
                    info!(
                        gpu = %g.name,
                        gflops = m.fp32_gflops,
                        mem_bw = m.mem_bw_gb_s,
                        hash_rate = m.hash_rate,
                        "GPU benchmark"
                    );
                    if m.fp32_gflops > 0 {
                        g.fp32_gflops = m.fp32_gflops;
                        g.measured = true;
                    }
                    g.mem_bw_gb_s = m.mem_bw_gb_s;
                    g.hash_rate = m.hash_rate;
                }
                Err(e) => {
                    warn!(gpu = %g.name, "GPU benchmark failed: {e:#}");
                    notes.push(format!("{}: benchmark failed: {e:#}", g.name));
                }
            }
        }
    }
    gpus
}

/// Marks which GPUs Docker tasks can use, as `env` says (again when
/// Docker starts after the agent).
pub fn mark_docker(gpus: &mut [GpuInfo], env: &GpuEnv) {
    for g in gpus.iter_mut() {
        match g.vendor {
            GpuVendor::Nvidia => g.docker = env.docker_nvidia && g.driver != "vfio-pci",
            GpuVendor::Amd => {
                g.docker = env.docker_amd
                    && !g.render_node.is_empty()
                    && Path::new("/dev/kfd").exists()
                    && Path::new(&g.render_node).exists()
            }
            _ => {}
        }
    }
}

/// The device profile (unsigned): GPUs and microVM networking.
pub fn profile(
    node: [u8; 32],
    gpus: Vec<GpuInfo>,
    microvm_network: bool,
    notes: Vec<String>,
) -> DeviceProfile {
    DeviceProfile {
        bench_version: GPU_BENCH_VERSION,
        node,
        measured_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        gpus,
        microvm_network,
        notes,
    }
}

/// Answers a GPU challenge on the GPU with this PCI address. Empty when
/// the GPU cannot run it.
pub fn solve(pci: &str, c: &pumasi_proto::gpu::GpuChallenge) -> Vec<u64> {
    match cuda::Gpu::open(pci).and_then(|g| g.solve(c)) {
        Ok(xs) => xs,
        Err(e) => {
            warn!("GPU check failed: {e:#}");
            Vec::new()
        }
    }
}

/// Whether Docker can give containers NVIDIA GPUs: Docker Desktop on
/// Windows does (WSL 2), elsewhere the NVIDIA Container Toolkit must be
/// installed (an `nvidia` runtime, a CDI spec, or its CLI).
pub async fn docker_nvidia(bin: &str) -> bool {
    if cfg!(windows) {
        return true;
    }
    let out = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new(bin)
            .args(["info", "--format", "{{json .Runtimes}}"])
            .stdin(std::process::Stdio::null())
            .output(),
    )
    .await;
    if let Ok(Ok(o)) = out
        && String::from_utf8_lossy(&o.stdout).contains("nvidia")
    {
        return true;
    }
    ["/etc/cdi/nvidia.yaml", "/var/run/cdi/nvidia.yaml"]
        .iter()
        .any(|p| Path::new(p).exists())
        || crate::microvm::vmm::find_program("nvidia-container-cli", None, &[]).is_some()
}

/// GPUs macOS reports (information only).
fn macos_gpus() -> Vec<GpuInfo> {
    let Ok(out) = std::process::Command::new("/usr/sbin/system_profiler")
        .args(["SPDisplaysDataType", "-json"])
        .output()
    else {
        return Vec::new();
    };
    parse_system_profiler(&String::from_utf8_lossy(&out.stdout))
}

fn parse_system_profiler(json: &str) -> Vec<GpuInfo> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    let Some(list) = v.get("SPDisplaysDataType").and_then(|l| l.as_array()) else {
        return Vec::new();
    };
    list.iter()
        .map(|d| {
            let s = |k: &str| d.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
            let name = Some(s("sppci_model"))
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| s("_name"));
            let vendor_text = format!("{} {}", s("spdisplays_vendor"), name).to_lowercase();
            let vendor = if vendor_text.contains("nvidia") {
                GpuVendor::Nvidia
            } else if vendor_text.contains("amd") || vendor_text.contains("ati") {
                GpuVendor::Amd
            } else if vendor_text.contains("intel") {
                GpuVendor::Intel
            } else if vendor_text.contains("apple") {
                GpuVendor::Apple
            } else {
                GpuVendor::Other
            };
            let vram = Some(s("spdisplays_vram"))
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| s("spdisplays_vram_shared"));
            GpuInfo {
                index: 0,
                vendor,
                name,
                uuid: String::new(),
                pci: String::new(),
                vram_mib: parse_vram(&vram),
                driver: String::new(),
                compute: s("spdisplays_mtlgpufamilysupport"),
                display: d.get("spdisplays_ndrvs").is_some(),
                fp32_gflops: 0,
                measured: false,
                mem_bw_gb_s: 0,
                hash_rate: 0,
                docker: false,
                vfio: false,
                render_node: String::new(),
            }
        })
        .collect()
}

/// `1536 MB`, `4 GB` to MiB.
fn parse_vram(s: &str) -> u64 {
    let mut it = s.split_whitespace();
    let n: f64 = it.next().and_then(|n| n.parse().ok()).unwrap_or(0.0);
    match it.next().map(|u| u.to_ascii_uppercase()) {
        Some(u) if u.starts_with('G') => (n * 1024.0) as u64,
        _ => n as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mac_gpus_from_system_profiler() {
        // A 2013 MacBook Pro with a discrete GPU.
        let json = r#"{"SPDisplaysDataType":[
          {"_name":"Intel Iris Pro","sppci_model":"Intel Iris Pro","spdisplays_vram_shared":"1536 MB","spdisplays_vendor":"Intel"},
          {"_name":"NVIDIA GeForce GT 750M","sppci_model":"NVIDIA GeForce GT 750M","spdisplays_vram":"2 GB","spdisplays_vendor":"NVIDIA (0x10de)","spdisplays_ndrvs":[{}]}
        ]}"#;
        let g = parse_system_profiler(json);
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].vendor, GpuVendor::Intel);
        assert_eq!(g[0].vram_mib, 1536);
        assert_eq!(g[1].vendor, GpuVendor::Nvidia);
        assert_eq!(g[1].vram_mib, 2048);
        assert!(g[1].display);
        assert!(!g[1].usable(), "information only");
        assert!(parse_system_profiler("not json").is_empty());
    }

    #[test]
    fn detect_never_fails() {
        let mut notes = Vec::new();
        let gpus = detect(&GpuEnv::default(), &mut notes);
        for (i, g) in gpus.iter().enumerate() {
            assert_eq!(g.index, i as u32);
            assert!(!g.docker && !g.vfio, "nothing enabled");
        }
    }
}
