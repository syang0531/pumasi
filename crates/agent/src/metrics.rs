//! Samples this machine for the cluster's monitoring (phase 11): CPU,
//! memory, the disk under the data directory, network counters, the
//! hottest temperature sensor and each GPU's use, memory, temperature and
//! power.

use std::path::{Path, PathBuf};

use pumasi_proto::{
    gpu::{GpuInfo, GpuVendor},
    monitor::{GpuMetrics, NodeMetrics},
};
use sysinfo::{Components, Disks, Networks, System};

/// Keeps what sysinfo needs between samples (CPU use is measured between
/// two refreshes).
pub struct Sampler {
    data_dir: PathBuf,
    system: System,
    disks: Disks,
    networks: Networks,
    /// None on Windows: sysinfo reads sensors there through WMI (COM),
    /// which crashes when the sampler moves between threads, and the
    /// sensors usually need administrator rights anyway.
    components: Option<Components>,
    gpus: Vec<GpuInfo>,
    nvml: Option<crate::gpu::nvml::Sampler>,
    samples: u64,
}

impl Sampler {
    /// `gpus`: the machine's GPUs, as its device profile lists them.
    pub fn new(data_dir: &Path, gpus: Vec<GpuInfo>) -> Self {
        let mut system = System::new();
        system.refresh_cpu_usage();
        let nvml = gpus
            .iter()
            .any(|g| g.vendor == GpuVendor::Nvidia && g.driver != "vfio-pci")
            .then(crate::gpu::nvml::Sampler::open)
            .flatten();
        Self {
            data_dir: data_dir.to_path_buf(),
            system,
            disks: Disks::new_with_refreshed_list(),
            networks: Networks::new_with_refreshed_list(),
            components: (!cfg!(windows)).then(Components::new_with_refreshed_list),
            gpus,
            nvml,
            samples: 0,
        }
    }

    pub fn sample(&mut self, running: u32) -> NodeMetrics {
        self.samples += 1;
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        // Disks come and go rarely; list them again now and then.
        if self.samples.is_multiple_of(60) {
            self.disks.refresh(true);
            self.networks.refresh(true);
            if let Some(c) = &mut self.components {
                c.refresh(true);
            }
        } else {
            self.disks.refresh(false);
            self.networks.refresh(false);
            if let Some(c) = &mut self.components {
                c.refresh(false);
            }
        }
        let (disk_used, disk_total) = disk_of(&self.disks, &self.data_dir);
        let (mut net_rx, mut net_tx) = (0u64, 0u64);
        for (name, data) in self.networks.iter() {
            if is_loopback(name) {
                continue;
            }
            net_rx = net_rx.saturating_add(data.total_received());
            net_tx = net_tx.saturating_add(data.total_transmitted());
        }
        let temp_c = self
            .components
            .iter()
            .flat_map(|c| c.iter())
            .filter_map(|c| c.temperature())
            // Sensors that are not there read 0 or nonsense.
            .filter(|t| t.is_finite() && *t > 1.0 && *t < 150.0)
            .fold(None, |a: Option<f32>, t| Some(a.map_or(t, |a| a.max(t))));
        let load1 = if cfg!(windows) {
            0.0
        } else {
            System::load_average().one as f32
        };
        let memory_total = self.system.total_memory();
        let memory_used = memory_total.saturating_sub(self.system.available_memory());
        let cpu = self.system.global_cpu_usage();
        NodeMetrics {
            cpu_percent: if cpu.is_finite() {
                cpu.clamp(0.0, 100.0)
            } else {
                0.0
            },
            load1,
            memory_used,
            memory_total,
            disk_used,
            disk_total,
            net_rx,
            net_tx,
            temp_c,
            gpus: self.gpu_metrics(),
            running,
        }
    }

    fn gpu_metrics(&mut self) -> Vec<GpuMetrics> {
        let nvidia = self.nvml.as_mut().map(|n| n.sample()).unwrap_or_default();
        let mut out = Vec::new();
        for g in &self.gpus {
            let m = match g.vendor {
                GpuVendor::Nvidia => {
                    nvidia
                        .iter()
                        .find(|(pci, _)| *pci == g.pci)
                        .map(|(_, m)| GpuMetrics {
                            index: g.index,
                            ..m.clone()
                        })
                }
                GpuVendor::Amd if cfg!(target_os = "linux") => amd_sysfs(g),
                _ => None,
            };
            out.extend(m);
        }
        out
    }
}

fn is_loopback(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n == "lo" || n.starts_with("lo0") || n.contains("loopback")
}

/// Used and total bytes of the disk holding `dir`: the one with the
/// longest mount point that is a prefix of it.
fn disk_of(disks: &Disks, dir: &Path) -> (u64, u64) {
    let dir = dir
        .canonicalize()
        .map(|d| pumasi_proto::without_verbatim(&d))
        .unwrap_or_else(|_| dir.to_path_buf());
    disks
        .iter()
        .filter(|d| dir.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| {
            (
                d.total_space().saturating_sub(d.available_space()),
                d.total_space(),
            )
        })
        .unwrap_or((0, 0))
}

/// An AMD GPU's use from the amdgpu driver's sysfs files.
fn amd_sysfs(g: &GpuInfo) -> Option<GpuMetrics> {
    if g.pci.is_empty() {
        return None;
    }
    let dev = Path::new("/sys/bus/pci/devices").join(&g.pci);
    let num = |p: &Path| -> Option<u64> { std::fs::read_to_string(p).ok()?.trim().parse().ok() };
    let util = num(&dev.join("gpu_busy_percent"))?;
    let hwmon = std::fs::read_dir(dev.join("hwmon"))
        .ok()
        .and_then(|mut d| d.next())
        .and_then(|e| e.ok())
        .map(|e| e.path());
    let temp_c = hwmon
        .as_ref()
        .and_then(|h| num(&h.join("temp1_input")))
        .map(|m| m as f32 / 1000.0);
    let power_w = hwmon
        .as_ref()
        .and_then(|h| num(&h.join("power1_average")).or_else(|| num(&h.join("power1_input"))))
        .map(|u| u as f32 / 1_000_000.0);
    Some(GpuMetrics {
        index: g.index,
        util_percent: util as f32,
        mem_used_mib: num(&dev.join("mem_info_vram_used")).unwrap_or(0) >> 20,
        mem_total_mib: num(&dev.join("mem_info_vram_total")).unwrap_or(g.vram_mib << 20) >> 20,
        temp_c,
        power_w,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_this_machine() {
        let dir = std::env::temp_dir();
        let mut s = Sampler::new(&dir, Vec::new());
        std::thread::sleep(std::time::Duration::from_millis(250));
        let m = s.sample(3);
        assert!(m.memory_total > 0);
        assert!(m.memory_used <= m.memory_total);
        assert!(m.disk_total > 0, "the temp directory's disk is found");
        assert!(m.disk_used <= m.disk_total);
        assert!((0.0..=100.0).contains(&m.cpu_percent));
        assert_eq!(m.running, 3);
        assert!(m.gpus.is_empty());
    }

    #[test]
    fn loopback_names() {
        assert!(is_loopback("lo"));
        assert!(is_loopback("lo0"));
        assert!(is_loopback("Loopback Pseudo-Interface 1"));
        assert!(!is_loopback("eth0"));
        assert!(!is_loopback("en0"));
    }
}
