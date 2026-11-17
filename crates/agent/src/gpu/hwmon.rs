//! GPU temperatures and power from hwmon, and CPU package power from RAPL,
//! on Linux (1.15).
//!
//! - A GPU's own hwmon directory (`/sys/bus/pci/devices/ADDR/hwmon/hwmonN`):
//!   amdgpu has `temp1_input` (edge) and `power1_average` or
//!   `power1_input`; Intel's discrete GPUs (i915 on Arc, xe) have an energy
//!   counter (`energy1_input`, microjoules) and, on newer kernels, a
//!   package temperature. Power from a counter is the rate between two
//!   samples.
//! - Intel's integrated GPUs have no hwmon of their own: they sit in the
//!   CPU package, so they get the package temperature (coretemp) and the
//!   `uncore` RAPL domain, which is mostly the GPU on client chips.
//! - RAPL (`/sys/class/powercap/intel-rapl:N`): the `package-N` domains are
//!   the CPU package (Intel, and AMD from Zen on with Linux 5.8). Its
//!   counters are readable by root only on most distributions since 2020;
//!   an agent running as another user reports no CPU power.
//!
//! Everything reads under a root directory, so tests use a fake sysfs.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Instant,
};

/// Readings that need a previous sample (energy counters).
#[derive(Debug, Default)]
struct Counters {
    last: HashMap<PathBuf, (Instant, u64)>,
}

impl Counters {
    /// Watts from a microjoule counter since the last call; `wrap` is the
    /// counter's range, when known.
    fn watts(&mut self, file: &Path, now: Instant, wrap: Option<u64>) -> Option<f32> {
        let uj = read_num(file)?;
        let prev = self.last.insert(file.to_path_buf(), (now, uj))?;
        let secs = now.duration_since(prev.0).as_secs_f64();
        if secs < 0.2 {
            return None;
        }
        let delta = if uj >= prev.1 {
            uj - prev.1
        } else {
            // The counter wrapped (or reset): without its range, skip.
            wrap?.checked_sub(prev.1)? + uj
        };
        let w = (delta as f64 / 1e6 / secs) as f32;
        plausible_power(w).then_some(w)
    }
}

fn read_num(p: &Path) -> Option<u64> {
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

fn read_text(p: &Path) -> Option<String> {
    Some(std::fs::read_to_string(p).ok()?.trim().to_string())
}

fn plausible_temp(t: f32) -> bool {
    t.is_finite() && t > 1.0 && t < 150.0
}

fn plausible_power(w: f32) -> bool {
    w.is_finite() && (0.0..2000.0).contains(&w)
}

/// What a GPU's hwmon says.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct GpuSensors {
    pub temp_c: Option<f32>,
    pub power_w: Option<f32>,
}

/// What RAPL says.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RaplReading {
    /// Every `package-N` domain together.
    pub package_w: Option<f32>,
    /// The `uncore` domains (an integrated GPU, on client chips).
    pub uncore_w: Option<f32>,
}

/// Reads hwmon and RAPL under a root (`/` on a real machine).
#[derive(Debug)]
pub struct Sensors {
    root: PathBuf,
    counters: Counters,
}

impl Default for Sensors {
    fn default() -> Self {
        Self::with_root("/")
    }
}

impl Sensors {
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            counters: Counters::default(),
        }
    }

    /// The hwmon directory of the GPU at PCI address `pci`, if it has one.
    fn gpu_hwmon(&self, pci: &str) -> Option<PathBuf> {
        if pci.is_empty() {
            return None;
        }
        let dir = self.root.join("sys/bus/pci/devices").join(pci).join("hwmon");
        let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
            .ok()?
            .flatten()
            .map(|e| e.path())
            .collect();
        found.sort();
        found.into_iter().next()
    }

    /// Whether the GPU at `pci` has a hwmon directory of its own.
    pub fn has_hwmon(&self, pci: &str) -> bool {
        self.gpu_hwmon(pci).is_some()
    }

    /// The temperature and power of the GPU at PCI address `pci`.
    pub fn gpu(&mut self, pci: &str, now: Instant) -> GpuSensors {
        let Some(h) = self.gpu_hwmon(pci) else {
            return GpuSensors::default();
        };
        GpuSensors {
            temp_c: gpu_temp(&h),
            power_w: read_num(&h.join("power1_average"))
                .or_else(|| read_num(&h.join("power1_input")))
                .map(|uw| uw as f32 / 1e6)
                .filter(|w| plausible_power(*w) && *w > 0.0)
                .or_else(|| self.counters.watts(&h.join("energy1_input"), now, None)),
        }
    }

    /// The CPU package and uncore power.
    pub fn rapl(&mut self, now: Instant) -> RaplReading {
        let base = self.root.join("sys/class/powercap");
        let Ok(dir) = std::fs::read_dir(&base) else {
            return RaplReading::default();
        };
        let mut zones: Vec<PathBuf> = dir
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("intel-rapl:"))
            })
            .collect();
        zones.sort();
        let (mut package, mut uncore) = (None::<f32>, None::<f32>);
        let add = |sum: &mut Option<f32>, w: f32| *sum = Some(sum.unwrap_or(0.0) + w);
        for z in zones {
            let Some(name) = read_text(&z.join("name")) else {
                continue;
            };
            let wrap = read_num(&z.join("max_energy_range_uj"));
            let energy = z.join("energy_uj");
            let sum = if name.starts_with("package") {
                &mut package
            } else if name == "uncore" {
                &mut uncore
            } else {
                continue;
            };
            if let Some(w) = self.counters.watts(&energy, now, wrap) {
                add(sum, w);
            }
        }
        RaplReading {
            package_w: package,
            uncore_w: uncore,
        }
    }
}

/// The GPU's main temperature: the sensor labelled `edge` (amdgpu), `pkg`
/// or `gpu` (Intel), else `temp1`, else the first there is.
fn gpu_temp(h: &Path) -> Option<f32> {
    let mut inputs: Vec<(u32, Option<String>)> = std::fs::read_dir(h)
        .ok()?
        .flatten()
        .filter_map(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            let i: u32 = n.strip_prefix("temp")?.strip_suffix("_input")?.parse().ok()?;
            Some((i, read_text(&h.join(format!("temp{i}_label")))))
        })
        .collect();
    inputs.sort();
    let pick = inputs
        .iter()
        .find(|(_, l)| {
            l.as_deref()
                .is_some_and(|l| matches!(l.to_ascii_lowercase().as_str(), "edge" | "pkg" | "gpu"))
        })
        .or_else(|| inputs.iter().find(|(i, _)| *i == 1))
        .or_else(|| inputs.first())?;
    let t = read_num(&h.join(format!("temp{}_input", pick.0)))? as f32 / 1000.0;
    plausible_temp(t).then_some(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn put(root: &Path, rel: &str, text: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, text).unwrap();
    }

    #[test]
    fn amd_and_intel_gpus() {
        let dir = crate::tempdir::TempDir::new("hwmon").unwrap();
        let root = dir.path();
        // amdgpu: edge, junction and memory temperatures, average power.
        let amd = "sys/bus/pci/devices/0000:03:00.0/hwmon/hwmon4";
        put(root, &format!("{amd}/temp1_input"), "52000\n");
        put(root, &format!("{amd}/temp1_label"), "edge\n");
        put(root, &format!("{amd}/temp2_input"), "71000\n");
        put(root, &format!("{amd}/temp2_label"), "junction\n");
        put(root, &format!("{amd}/power1_average"), "123000000\n");
        // xe (Arc): a package temperature and an energy counter.
        let arc = "sys/bus/pci/devices/0000:04:00.0/hwmon/hwmon5";
        put(root, &format!("{arc}/temp2_input"), "48000\n");
        put(root, &format!("{arc}/temp2_label"), "pkg\n");
        put(root, &format!("{arc}/energy1_input"), "1000000\n");

        let mut s = Sensors::with_root(root);
        let t0 = Instant::now();
        let a = s.gpu("0000:03:00.0", t0);
        assert_eq!(a.temp_c, Some(52.0));
        assert_eq!(a.power_w, Some(123.0));
        let b = s.gpu("0000:04:00.0", t0);
        assert_eq!(b.temp_c, Some(48.0));
        assert_eq!(b.power_w, None, "a counter needs two samples");
        put(root, &format!("{arc}/energy1_input"), "31000000\n");
        let b = s.gpu("0000:04:00.0", t0 + Duration::from_secs(2));
        assert_eq!(b.power_w, Some(15.0), "30 J in 2 s");
        // An integrated GPU has no hwmon of its own.
        assert!(!s.has_hwmon("0000:00:02.0"));
        assert_eq!(s.gpu("0000:00:02.0", t0), GpuSensors::default());
    }

    #[test]
    fn rapl_package_and_uncore() {
        let dir = crate::tempdir::TempDir::new("rapl").unwrap();
        let root = dir.path();
        let z = "sys/class/powercap";
        put(root, &format!("{z}/intel-rapl:0/name"), "package-0\n");
        put(root, &format!("{z}/intel-rapl:0/energy_uj"), "262000000000\n");
        put(root, &format!("{z}/intel-rapl:0/max_energy_range_uj"), "262143328850\n");
        put(root, &format!("{z}/intel-rapl:0:1/name"), "uncore\n");
        put(root, &format!("{z}/intel-rapl:0:1/energy_uj"), "5000000\n");
        put(root, &format!("{z}/intel-rapl:0:0/name"), "core\n");
        put(root, &format!("{z}/intel-rapl:0:0/energy_uj"), "1\n");
        // Not RAPL.
        put(root, &format!("{z}/dtpm/name"), "package-0\n");
        let mut s = Sensors::with_root(root);
        let t0 = Instant::now();
        assert_eq!(s.rapl(t0), RaplReading::default());
        // The package counter wraps: 143328850 µJ to the top, 56671150
        // after it, 200 J in 10 s.
        put(root, &format!("{z}/intel-rapl:0/energy_uj"), "56671150\n");
        put(root, &format!("{z}/intel-rapl:0:1/energy_uj"), "35000000\n");
        let r = s.rapl(t0 + Duration::from_secs(10));
        assert!((r.package_w.unwrap() - 20.0).abs() < 0.01, "{r:?}");
        assert!((r.uncore_w.unwrap() - 3.0).abs() < 0.01, "{r:?}");
        // No powercap at all (macOS, Windows, a container).
        let mut none = Sensors::with_root(root.join("nothing"));
        assert_eq!(none.rapl(t0), RaplReading::default());
    }
}
