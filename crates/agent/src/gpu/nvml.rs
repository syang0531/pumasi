//! NVIDIA GPUs through NVML, loaded at run time (`libnvidia-ml.so.1` on
//! Linux, `nvml.dll` on Windows). Machines without the NVIDIA driver have
//! no NVML and simply report no NVIDIA GPUs.

use std::ffi::{c_char, c_int, c_uint, c_ulonglong, c_void};

use anyhow::{Context, Result, bail};
use libloading::{Library, Symbol};
use pumasi_proto::gpu::{GpuInfo, GpuVendor};

type Device = *mut c_void;
type Ret = c_int;

#[repr(C)]
struct Memory {
    total: c_ulonglong,
    free: c_ulonglong,
    used: c_ulonglong,
}

/// `nvmlPciInfo_t` (v3 layout, the one `nvmlDeviceGetPciInfo_v3` fills).
#[repr(C)]
struct PciInfo {
    bus_id_legacy: [c_char; 16],
    domain: c_uint,
    bus: c_uint,
    device: c_uint,
    pci_device_id: c_uint,
    pci_subsystem_id: c_uint,
    bus_id: [c_char; 32],
}

const NVML_CLOCK_SM: c_uint = 1;

/// `nvmlDeviceAttributes_t`: what a MIG slice has of its GPU.
#[repr(C)]
#[derive(Default)]
struct Attributes {
    multiprocessor_count: c_uint,
    shared_copy_engine_count: c_uint,
    shared_decoder_count: c_uint,
    shared_encoder_count: c_uint,
    shared_jpeg_count: c_uint,
    shared_ofa_count: c_uint,
    gpu_instance_slice_count: c_uint,
    compute_instance_slice_count: c_uint,
    memory_size_mb: c_ulonglong,
}

/// One MIG slice as NVML describes it (1.10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigSlice {
    pub uuid: String,
    pub vram_mib: u64,
    pub sms: u32,
    pub gpu_slices: u32,
    pub compute_slices: u32,
}

/// The slices of a GPU in MIG mode as GPUs of their own: what Docker
/// tasks get (`--gpus device=MIG-...`), each with its share of the GPU's
/// memory and, estimated from its multiprocessors, of its FP32 rate.
/// The GPU itself runs no CUDA in MIG mode, so it is left out.
pub(crate) fn mig_gpus(parent: &GpuInfo, parent_sms: u32, slices: &[MigSlice]) -> Vec<GpuInfo> {
    slices
        .iter()
        .map(|m| {
            let mem_gb = (m.vram_mib + 512) >> 10;
            let profile = if m.compute_slices > 0 && m.compute_slices != m.gpu_slices {
                format!("{}c.{}g.{mem_gb}gb", m.compute_slices, m.gpu_slices)
            } else {
                format!("{}g.{mem_gb}gb", m.gpu_slices)
            };
            let fp32 = if parent_sms > 0 && m.sms > 0 {
                (parent.fp32_gflops as u64 * m.sms as u64 / parent_sms as u64) as u32
            } else {
                0
            };
            GpuInfo {
                name: format!("{} MIG {profile}", parent.name),
                uuid: m.uuid.clone(),
                vram_mib: m.vram_mib,
                fp32_gflops: fp32,
                measured: false,
                display: false,
                ..parent.clone()
            }
        })
        .collect()
}

fn candidates() -> Vec<String> {
    if cfg!(windows) {
        let mut v = vec!["nvml.dll".to_string()];
        if let Some(pf) = std::env::var_os("ProgramW6432").or(std::env::var_os("ProgramFiles")) {
            v.push(format!(
                "{}\\NVIDIA Corporation\\NVSMI\\nvml.dll",
                pf.to_string_lossy()
            ));
        }
        if let Some(root) = std::env::var_os("SystemRoot") {
            v.push(format!("{}\\System32\\nvml.dll", root.to_string_lossy()));
        }
        v
    } else {
        vec!["libnvidia-ml.so.1".into(), "libnvidia-ml.so".into()]
    }
}

fn cstr(buf: &[c_char]) -> String {
    let bytes: Vec<u8> = buf
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).trim().to_string()
}

/// NVIDIA GPUs, numbered from `first`. `Ok(empty)` when NVML is missing.
pub fn list(first: u32) -> Result<Vec<GpuInfo>> {
    let lib = candidates()
        .into_iter()
        .find_map(|name| unsafe { Library::new(&name) }.ok());
    let Some(lib) = lib else {
        return Ok(Vec::new());
    };
    unsafe { list_with(&lib, first) }
}

unsafe fn list_with(lib: &Library, first: u32) -> Result<Vec<GpuInfo>> {
    unsafe {
        let init: Symbol<unsafe extern "C" fn() -> Ret> = lib
            .get(b"nvmlInit_v2\0")
            .context("NVML has no nvmlInit_v2")?;
        let shutdown: Symbol<unsafe extern "C" fn() -> Ret> = lib.get(b"nvmlShutdown\0")?;
        let r = init();
        if r != 0 {
            bail!("nvmlInit failed ({r}); is the NVIDIA driver loaded?");
        }
        let res = query(lib, first);
        shutdown();
        res
    }
}

unsafe fn query(lib: &Library, first: u32) -> Result<Vec<GpuInfo>> {
    unsafe {
        let count: Symbol<unsafe extern "C" fn(*mut c_uint) -> Ret> =
            lib.get(b"nvmlDeviceGetCount_v2\0")?;
        let handle: Symbol<unsafe extern "C" fn(c_uint, *mut Device) -> Ret> =
            lib.get(b"nvmlDeviceGetHandleByIndex_v2\0")?;
        let name: Symbol<unsafe extern "C" fn(Device, *mut c_char, c_uint) -> Ret> =
            lib.get(b"nvmlDeviceGetName\0")?;
        let uuid: Symbol<unsafe extern "C" fn(Device, *mut c_char, c_uint) -> Ret> =
            lib.get(b"nvmlDeviceGetUUID\0")?;
        let memory: Symbol<unsafe extern "C" fn(Device, *mut Memory) -> Ret> =
            lib.get(b"nvmlDeviceGetMemoryInfo\0")?;
        let driver: Symbol<unsafe extern "C" fn(*mut c_char, c_uint) -> Ret> =
            lib.get(b"nvmlSystemGetDriverVersion\0")?;
        // Optional entry points (older drivers lack some).
        let pci: Option<Symbol<unsafe extern "C" fn(Device, *mut PciInfo) -> Ret>> =
            lib.get(b"nvmlDeviceGetPciInfo_v3\0").ok();
        let cc: Option<Symbol<unsafe extern "C" fn(Device, *mut c_int, *mut c_int) -> Ret>> =
            lib.get(b"nvmlDeviceGetCudaComputeCapability\0").ok();
        let cores: Option<Symbol<unsafe extern "C" fn(Device, *mut c_uint) -> Ret>> =
            lib.get(b"nvmlDeviceGetNumGpuCores\0").ok();
        let clock: Option<Symbol<unsafe extern "C" fn(Device, c_uint, *mut c_uint) -> Ret>> =
            lib.get(b"nvmlDeviceGetMaxClockInfo\0").ok();
        let display: Option<Symbol<unsafe extern "C" fn(Device, *mut c_int) -> Ret>> =
            lib.get(b"nvmlDeviceGetDisplayActive\0").ok();
        // MIG (1.10): Ampere and newer data center GPUs.
        type MigModeFn = unsafe extern "C" fn(Device, *mut c_uint, *mut c_uint) -> Ret;
        let mig_mode: Option<Symbol<MigModeFn>> = lib.get(b"nvmlDeviceGetMigMode\0").ok();
        let mig_max: Option<Symbol<unsafe extern "C" fn(Device, *mut c_uint) -> Ret>> =
            lib.get(b"nvmlDeviceGetMaxMigDeviceCount\0").ok();
        type MigHandleFn = unsafe extern "C" fn(Device, c_uint, *mut Device) -> Ret;
        let mig_handle: Option<Symbol<MigHandleFn>> =
            lib.get(b"nvmlDeviceGetMigDeviceHandleByIndex\0").ok();
        let attributes: Option<Symbol<unsafe extern "C" fn(Device, *mut Attributes) -> Ret>> =
            lib.get(b"nvmlDeviceGetAttributes_v2\0").ok();

        let mut buf = [0 as c_char; 96];
        let drv = if driver(buf.as_mut_ptr(), buf.len() as c_uint) == 0 {
            cstr(&buf)
        } else {
            String::new()
        };
        let mut n: c_uint = 0;
        let r = count(&mut n);
        if r != 0 {
            bail!("nvmlDeviceGetCount failed ({r})");
        }
        let mut out = Vec::new();
        for i in 0..n {
            let mut d: Device = std::ptr::null_mut();
            if handle(i, &mut d) != 0 {
                continue;
            }
            let mut b = [0 as c_char; 96];
            let gpu_name = if name(d, b.as_mut_ptr(), b.len() as c_uint) == 0 {
                cstr(&b)
            } else {
                format!("NVIDIA GPU {i}")
            };
            let mut b = [0 as c_char; 96];
            let gpu_uuid = if uuid(d, b.as_mut_ptr(), b.len() as c_uint) == 0 {
                cstr(&b)
            } else {
                String::new()
            };
            let mut m = Memory {
                total: 0,
                free: 0,
                used: 0,
            };
            let vram_mib = if memory(d, &mut m) == 0 {
                m.total >> 20
            } else {
                0
            };
            let pci_addr = pci
                .as_ref()
                .and_then(|f| {
                    let mut p: PciInfo = std::mem::zeroed();
                    (f(d, &mut p) == 0).then(|| normalize_pci(&cstr(&p.bus_id)))
                })
                .unwrap_or_default();
            let (major, minor) = cc
                .as_ref()
                .and_then(|f| {
                    let (mut a, mut b) = (0, 0);
                    (f(d, &mut a, &mut b) == 0).then_some((a, b))
                })
                .unwrap_or((0, 0));
            let core_count = cores
                .as_ref()
                .and_then(|f| {
                    let mut c = 0;
                    (f(d, &mut c) == 0).then_some(c)
                })
                .unwrap_or(0);
            let sm_mhz = clock
                .as_ref()
                .and_then(|f| {
                    let mut c = 0;
                    (f(d, NVML_CLOCK_SM, &mut c) == 0).then_some(c)
                })
                .unwrap_or(0);
            let shows = display
                .as_ref()
                .and_then(|f| {
                    let mut a = 0;
                    (f(d, &mut a) == 0).then_some(a != 0)
                })
                .unwrap_or(false);
            let gpu = GpuInfo {
                index: first + out.len() as u32,
                vendor: GpuVendor::Nvidia,
                name: gpu_name,
                uuid: gpu_uuid,
                pci: pci_addr,
                vram_mib,
                driver: drv.clone(),
                compute: if major > 0 {
                    format!("{major}.{minor}")
                } else {
                    String::new()
                },
                display: shows,
                // Two FLOPs (one fused multiply-add) per core per clock.
                fp32_gflops: (core_count as u64 * sm_mhz as u64 * 2 / 1000) as u32,
                measured: false,
                mem_bw_gb_s: 0,
                hash_rate: 0,
                docker: false,
                vfio: false,
                render_node: String::new(),
            };
            let in_mig = mig_mode.as_ref().is_some_and(|f| {
                let (mut cur, mut pending) = (0, 0);
                f(d, &mut cur, &mut pending) == 0 && cur == 1
            });
            if !in_mig {
                out.push(gpu);
                continue;
            }
            let attrs = |h: Device| -> Option<Attributes> {
                let f = attributes.as_ref()?;
                let mut a = Attributes::default();
                (f(h, &mut a) == 0).then_some(a)
            };
            let mut max = 0;
            if let Some(f) = &mig_max {
                f(d, &mut max);
            }
            let mut slices = Vec::new();
            for m in 0..max {
                let mut h: Device = std::ptr::null_mut();
                if mig_handle.as_ref().is_none_or(|f| f(d, m, &mut h) != 0) {
                    continue; // an empty slot
                }
                let mut b = [0 as c_char; 96];
                if uuid(h, b.as_mut_ptr(), b.len() as c_uint) != 0 {
                    continue;
                }
                let mut mem = Memory {
                    total: 0,
                    free: 0,
                    used: 0,
                };
                let a = attrs(h).unwrap_or_default();
                slices.push(MigSlice {
                    uuid: cstr(&b),
                    vram_mib: if memory(h, &mut mem) == 0 {
                        mem.total >> 20
                    } else {
                        a.memory_size_mb
                    },
                    sms: a.multiprocessor_count,
                    gpu_slices: a.gpu_instance_slice_count,
                    compute_slices: a.compute_instance_slice_count,
                });
            }
            // The whole GPU's multiprocessors: from its cores (64 FP32
            // cores per SM on the A100 and A30, 128 on Hopper and later).
            let per_sm = if major == 8 { 64 } else { 128 };
            let parent_sms = core_count / per_sm;
            for mut g in mig_gpus(&gpu, parent_sms, &slices) {
                g.index = first + out.len() as u32;
                out.push(g);
            }
        }
        Ok(out)
    }
}

#[repr(C)]
struct Utilization {
    gpu: c_uint,
    memory: c_uint,
}

const NVML_TEMPERATURE_GPU: c_uint = 0;

type DeviceFn<T> = unsafe extern "C" fn(Device, *mut T) -> Ret;

/// NVML kept open between samples for the monitoring (phase 11).
pub struct Sampler {
    // The function pointers below point into this library.
    _lib: Library,
    shutdown: unsafe extern "C" fn() -> Ret,
    count: unsafe extern "C" fn(*mut c_uint) -> Ret,
    handle: unsafe extern "C" fn(c_uint, *mut Device) -> Ret,
    memory: DeviceFn<Memory>,
    pci: Option<DeviceFn<PciInfo>>,
    util: Option<DeviceFn<Utilization>>,
    temp: Option<unsafe extern "C" fn(Device, c_uint, *mut c_uint) -> Ret>,
    power: Option<DeviceFn<c_uint>>,
}

// NVML is thread-safe; the pointers are into a library this owns.
unsafe impl Send for Sampler {}

impl Sampler {
    /// `None` when NVML is missing or does not start.
    pub fn open() -> Option<Self> {
        let lib = candidates()
            .into_iter()
            .find_map(|name| unsafe { Library::new(&name) }.ok())?;
        unsafe {
            let init: unsafe extern "C" fn() -> Ret = *lib.get(b"nvmlInit_v2\0").ok()?;
            let shutdown = *lib.get(b"nvmlShutdown\0").ok()?;
            let count = *lib.get(b"nvmlDeviceGetCount_v2\0").ok()?;
            let handle = *lib.get(b"nvmlDeviceGetHandleByIndex_v2\0").ok()?;
            let memory = *lib.get(b"nvmlDeviceGetMemoryInfo\0").ok()?;
            let pci = lib.get(b"nvmlDeviceGetPciInfo_v3\0").ok().map(|s| *s);
            let util = lib.get(b"nvmlDeviceGetUtilizationRates\0").ok().map(|s| *s);
            let temp = lib.get(b"nvmlDeviceGetTemperature\0").ok().map(|s| *s);
            let power = lib.get(b"nvmlDeviceGetPowerUsage\0").ok().map(|s| *s);
            if init() != 0 {
                return None;
            }
            Some(Self {
                _lib: lib,
                shutdown,
                count,
                handle,
                memory,
                pci,
                util,
                temp,
                power,
            })
        }
    }

    /// Each NVIDIA GPU by PCI address (`index` left 0).
    pub fn sample(&mut self) -> Vec<(String, pumasi_proto::monitor::GpuMetrics)> {
        let mut out = Vec::new();
        unsafe {
            let mut n: c_uint = 0;
            if (self.count)(&mut n) != 0 {
                return out;
            }
            for i in 0..n {
                let mut d: Device = std::ptr::null_mut();
                if (self.handle)(i, &mut d) != 0 {
                    continue;
                }
                let Some(pci) = self.pci.and_then(|f| {
                    let mut p: PciInfo = std::mem::zeroed();
                    (f(d, &mut p) == 0).then(|| normalize_pci(&cstr(&p.bus_id)))
                }) else {
                    continue;
                };
                let mut m = Memory {
                    total: 0,
                    free: 0,
                    used: 0,
                };
                let have_mem = (self.memory)(d, &mut m) == 0;
                let util = self.util.and_then(|f| {
                    let mut u = Utilization { gpu: 0, memory: 0 };
                    (f(d, &mut u) == 0).then_some(u.gpu)
                });
                let temp_c = self.temp.and_then(|f| {
                    let mut t: c_uint = 0;
                    (f(d, NVML_TEMPERATURE_GPU, &mut t) == 0).then_some(t as f32)
                });
                let power_w = self.power.and_then(|f| {
                    let mut mw: c_uint = 0;
                    (f(d, &mut mw) == 0).then_some(mw as f32 / 1000.0)
                });
                out.push((
                    pci,
                    pumasi_proto::monitor::GpuMetrics {
                        index: 0,
                        util_percent: util.unwrap_or(0) as f32,
                        mem_used_mib: if have_mem { m.used >> 20 } else { 0 },
                        mem_total_mib: if have_mem { m.total >> 20 } else { 0 },
                        temp_c,
                        power_w,
                    },
                ));
            }
        }
        out
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        unsafe {
            (self.shutdown)();
        }
    }
}

/// NVML says `00000000:01:00.0`; sysfs and QEMU say `0000:01:00.0`.
pub fn normalize_pci(s: &str) -> String {
    let s = s.trim().to_ascii_lowercase();
    match s.split_once(':') {
        Some((domain, rest)) if domain.len() == 8 => format!("{}:{rest}", &domain[4..]),
        _ => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pci_addresses() {
        assert_eq!(normalize_pci("00000000:01:00.0"), "0000:01:00.0");
        assert_eq!(normalize_pci("0000:2B:00.0"), "0000:2b:00.0");
    }

    #[test]
    fn listing_without_nvml_is_empty_or_works() {
        // CI machines have no NVIDIA driver; either way this must not fail
        // loudly.
        let _ = list(0);
    }
}

#[cfg(test)]
mod mig_tests {
    use super::*;

    #[test]
    fn mig_slices_are_gpus_of_their_own() {
        let a100 = GpuInfo {
            index: 0,
            vendor: GpuVendor::Nvidia,
            name: "NVIDIA A100-SXM4-40GB".into(),
            uuid: "GPU-1".into(),
            pci: "0000:07:00.0".into(),
            vram_mib: 40 << 10,
            driver: "570".into(),
            compute: "8.0".into(),
            display: false,
            fp32_gflops: 19_500,
            measured: false,
            mem_bw_gb_s: 0,
            hash_rate: 0,
            docker: false,
            vfio: false,
            render_node: String::new(),
        };
        let slices = [
            MigSlice {
                uuid: "MIG-a".into(),
                vram_mib: 4864,
                sms: 14,
                gpu_slices: 1,
                compute_slices: 1,
            },
            MigSlice {
                uuid: "MIG-b".into(),
                vram_mib: 19_968,
                sms: 42,
                gpu_slices: 3,
                compute_slices: 3,
            },
        ];
        let g = mig_gpus(&a100, 108, &slices);
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].name, "NVIDIA A100-SXM4-40GB MIG 1g.5gb");
        assert_eq!(g[1].name, "NVIDIA A100-SXM4-40GB MIG 3g.20gb");
        assert!(g[0].mig() && g[1].mig());
        assert_eq!(g[0].vram_mib, 4864);
        assert_eq!(g[0].fp32_gflops, 19_500 * 14 / 108);
        assert_eq!(g[1].pci, a100.pci, "the slices are on the GPU's PCI device");
        assert!(!g[0].measured);
    }
}
