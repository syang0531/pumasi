//! Samples this machine for the cluster's monitoring (phase 11): CPU,
//! memory, the disk under the data directory, network counters, the
//! hottest temperature sensor and each GPU's use, memory, temperature and
//! power. Since 1.6 every GPU vendor reports its use
//! ([`crate::gpu::usage`]), and Windows its temperatures (WMI on a thread
//! of its own: ACPI thermal zones, and LibreHardwareMonitor's sensors when
//! it runs).

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
    /// which crashes when the sampler moves between threads; [`wmi_temps`]
    /// reads them on a thread of its own instead.
    components: Option<Components>,
    #[cfg(windows)]
    wmi: wmi_temps::Temps,
    gpus: Vec<GpuInfo>,
    nvml: Option<crate::gpu::nvml::Sampler>,
    /// The GPUs NVML does not cover (1.6).
    usage: crate::gpu::usage::GpuUsage,
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
            #[cfg(windows)]
            wmi: wmi_temps::Temps::start(),
            usage: crate::gpu::usage::GpuUsage::new(&gpus),
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
        #[cfg(windows)]
        let temp_c = temp_c.or_else(|| self.wmi.hottest());
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
        let mut rest = Vec::new();
        for g in &self.gpus {
            if g.driver == "vfio-pci" {
                // Given to microVMs: the host does not drive it.
                continue;
            }
            let m = match g.vendor {
                GpuVendor::Nvidia if self.nvml.is_some() => nvidia
                    .iter()
                    .find(|(pci, _)| *pci == g.pci)
                    .map(|(_, m)| GpuMetrics {
                        index: g.index,
                        ..m.clone()
                    }),
                GpuVendor::Amd if cfg!(target_os = "linux") => amd_sysfs(g),
                _ => None,
            };
            match m {
                Some(m) => out.push(m),
                None => rest.push(g),
            }
        }
        out.extend(self.usage.sample(&rest));
        out.sort_by_key(|m| m.index);
        out
    }
}

/// Temperatures through WMI on Windows (1.6). COM objects must stay on the
/// thread that made them, so one thread owns the connections and reads the
/// sensors every 10 seconds; the sampler takes the latest reading.
#[cfg(windows)]
mod wmi_temps {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use tracing::debug;
    use wmi::{Variant, WMIConnection};

    #[derive(Clone)]
    pub struct Temps(Arc<Mutex<Option<(Instant, f32)>>>);

    impl Temps {
        pub fn start() -> Self {
            let t = Temps(Arc::new(Mutex::new(None)));
            let latest = t.0.clone();
            let spawned = std::thread::Builder::new()
                .name("pumasi-wmi".into())
                .spawn(move || {
                    let acpi = WMIConnection::with_namespace_path("root\\WMI")
                        .map_err(|e| debug!("WMI root\\WMI: {e}"))
                        .ok();
                    let mut lhm = None;
                    let mut tries = 0u32;
                    loop {
                        // LibreHardwareMonitor may start after the agent.
                        if lhm.is_none() && tries.is_multiple_of(30) {
                            lhm = WMIConnection::with_namespace_path("root\\LibreHardwareMonitor")
                                .ok();
                        }
                        tries += 1;
                        let hottest = [
                            acpi.as_ref().and_then(read_acpi),
                            lhm.as_ref().and_then(read_lhm),
                        ]
                        .into_iter()
                        .flatten()
                        .filter(|t| t.is_finite() && *t > 1.0 && *t < 150.0)
                        .fold(None, |a: Option<f32>, t| Some(a.map_or(t, |a| a.max(t))));
                        *latest.lock().unwrap() = hottest.map(|t| (Instant::now(), t));
                        if Arc::strong_count(&latest) == 1 {
                            return;
                        }
                        std::thread::sleep(Duration::from_secs(10));
                    }
                });
            if let Err(e) = spawned {
                debug!("no WMI thread: {e}");
            }
            t
        }

        /// The hottest sensor read in the last minute.
        pub fn hottest(&self) -> Option<f32> {
            self.0
                .lock()
                .unwrap()
                .filter(|(at, _)| at.elapsed() < Duration::from_secs(60))
                .map(|(_, t)| t)
        }
    }

    fn number(v: &Variant) -> Option<f64> {
        Some(match v {
            Variant::R4(f) => *f as f64,
            Variant::R8(f) => *f,
            Variant::I4(n) => *n as f64,
            Variant::UI4(n) => *n as f64,
            Variant::I8(n) => *n as f64,
            Variant::UI8(n) => *n as f64,
            _ => return None,
        })
    }

    /// ACPI thermal zones, in tenths of a kelvin (needs administrator
    /// rights, which the service has).
    fn read_acpi(c: &WMIConnection) -> Option<f32> {
        let rows: Vec<HashMap<String, Variant>> = c
            .raw_query("SELECT CurrentTemperature FROM MSAcpi_ThermalZoneTemperature")
            .ok()?;
        rows.iter()
            .filter_map(|r| number(r.get("CurrentTemperature")?))
            .map(|dk| (dk / 10.0 - 273.15) as f32)
            .fold(None, |a: Option<f32>, t| Some(a.map_or(t, |a| a.max(t))))
    }

    /// LibreHardwareMonitor's temperature sensors (CPU cores, GPUs,
    /// drives), while it runs.
    fn read_lhm(c: &WMIConnection) -> Option<f32> {
        let rows: Vec<HashMap<String, Variant>> = c
            .raw_query("SELECT Value FROM Sensor WHERE SensorType = 'Temperature'")
            .ok()?;
        rows.iter()
            .filter_map(|r| number(r.get("Value")?))
            .map(|t| t as f32)
            .fold(None, |a: Option<f32>, t| Some(a.map_or(t, |a| a.max(t))))
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
