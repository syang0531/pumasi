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
            out.push(GpuInfo {
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
            });
        }
        Ok(out)
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
