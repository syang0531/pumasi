//! GPU use for the GPUs NVML does not cover (1.6): Intel, AMD and Apple
//! GPUs, and any GPU on Windows.
//!
//! - Linux: AMD through the amdgpu driver's sysfs files
//!   ([`crate::metrics`]); any GPU through DRM fdinfo, the per-client
//!   engine times every DRM driver (i915, xe, amdgpu, ...) prints in
//!   `/proc/PID/fdinfo` (only the processes the agent may read count, so
//!   an agent running as root sees all of them).
//! - macOS: the `PerformanceStatistics` of each `IOAccelerator` in the I/O
//!   registry (`Device Utilization %`, memory in use).
//! - Windows: the `GPU Engine` and `GPU Adapter Memory` performance
//!   counters, by adapter LUID, which DXGI maps to the listed GPUs.

use pumasi_proto::{gpu::GpuInfo, monitor::GpuMetrics};

/// Measures the GPUs given to [`GpuUsage::new`] that NVML does not.
pub struct GpuUsage {
    #[cfg(target_os = "linux")]
    drm: drm::DrmUsage,
    #[cfg(target_os = "macos")]
    _mac: (),
    #[cfg(windows)]
    win: Option<windows::WinGpu>,
}

impl GpuUsage {
    pub fn new(gpus: &[GpuInfo]) -> Self {
        let _ = gpus;
        Self {
            #[cfg(target_os = "linux")]
            drm: drm::DrmUsage::default(),
            #[cfg(target_os = "macos")]
            _mac: (),
            #[cfg(windows)]
            win: windows::WinGpu::open(gpus),
        }
    }

    /// One sample of each of `gpus` this can measure (by their index).
    pub fn sample(&mut self, gpus: &[&GpuInfo]) -> Vec<GpuMetrics> {
        if gpus.is_empty() {
            return Vec::new();
        }
        #[cfg(target_os = "linux")]
        {
            self.drm.sample(gpus)
        }
        #[cfg(target_os = "macos")]
        {
            mac::sample(gpus)
        }
        #[cfg(windows)]
        {
            self.win
                .as_mut()
                .map(|w| w.sample(gpus))
                .unwrap_or_default()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        {
            Vec::new()
        }
    }
}

/// `1234 KiB`, `5 MiB`, `100` (bytes) to bytes.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_size(s: &str) -> Option<u64> {
    let mut it = s.split_whitespace();
    let n: u64 = it.next()?.parse().ok()?;
    Some(match it.next() {
        Some("KiB") => n << 10,
        Some("MiB") => n << 20,
        Some("GiB") => n << 30,
        _ => n,
    })
}

#[cfg(target_os = "linux")]
mod drm {
    //! DRM fdinfo (`Documentation/gpu/drm-usage-stats.rst`).

    use std::{
        collections::{HashMap, HashSet},
        time::Instant,
    };

    use pumasi_proto::{gpu::GpuInfo, monitor::GpuMetrics};

    /// What all clients of one GPU used, summed.
    #[derive(Debug, Default, Clone, PartialEq)]
    pub(super) struct Totals {
        /// Busy nanoseconds by engine (`drm-engine-NAME`).
        pub engine_ns: HashMap<String, u64>,
        /// Engine instances (`drm-engine-capacity-NAME`), when above 1.
        pub capacity: HashMap<String, u64>,
        /// Busy and total GPU cycles by engine (xe: `drm-cycles-NAME`,
        /// `drm-total-cycles-NAME`).
        pub cycles: HashMap<String, (u64, u64)>,
        /// Memory in use, in bytes (`drm-resident-REGION`).
        pub resident: u64,
    }

    #[derive(Default)]
    pub struct DrmUsage {
        last: Option<(Instant, HashMap<String, Totals>)>,
    }

    impl DrmUsage {
        pub fn sample(&mut self, gpus: &[&GpuInfo]) -> Vec<GpuMetrics> {
            let wanted: HashSet<&str> = gpus
                .iter()
                .filter(|g| !g.pci.is_empty())
                .map(|g| g.pci.as_str())
                .collect();
            if wanted.is_empty() {
                return Vec::new();
            }
            let now = Instant::now();
            let totals = read_all(&wanted);
            let prev = self.last.replace((now, totals.clone()));
            let mut out = Vec::new();
            for g in gpus {
                let Some(t) = totals.get(&g.pci) else {
                    continue;
                };
                let util = prev.as_ref().and_then(|(then, p)| {
                    let wall = now.duration_since(*then).as_nanos() as u64;
                    utilization(p.get(&g.pci)?, t, wall)
                });
                let Some(util) = util else {
                    continue;
                };
                out.push(GpuMetrics {
                    index: g.index,
                    util_percent: util,
                    mem_used_mib: t.resident >> 20,
                    mem_total_mib: g.vram_mib,
                    temp_c: None,
                    power_w: None,
                });
            }
            out
        }
    }

    /// Busy share of the busiest engine between two samples `wall`
    /// nanoseconds apart, 0 to 100.
    pub(super) fn utilization(prev: &Totals, now: &Totals, wall: u64) -> Option<f32> {
        if wall == 0 {
            return None;
        }
        let mut best: Option<f64> = None;
        let mut put = |v: f64| best = Some(best.map_or(v, |b| b.max(v)));
        for (engine, ns) in &now.engine_ns {
            let before = prev.engine_ns.get(engine).copied().unwrap_or(*ns);
            let cap = now.capacity.get(engine).copied().unwrap_or(1).max(1);
            put(ns.saturating_sub(before) as f64 / (wall * cap) as f64);
        }
        for (engine, (busy, total)) in &now.cycles {
            let Some((b0, t0)) = prev.cycles.get(engine) else {
                continue;
            };
            let dt = total.saturating_sub(*t0);
            if dt > 0 {
                put(busy.saturating_sub(*b0) as f64 / dt as f64);
            }
        }
        // A GPU without engine stats at all (an idle one may have no
        // clients) is idle.
        Some((best.unwrap_or(0.0) * 100.0).clamp(0.0, 100.0) as f32)
    }

    /// Sums the fdinfo of every DRM client of the GPUs in `wanted`, each
    /// client once.
    fn read_all(wanted: &HashSet<&str>) -> HashMap<String, Totals> {
        let mut out: HashMap<String, Totals> = HashMap::new();
        for pdev in wanted {
            out.entry(pdev.to_string()).or_default();
        }
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let Ok(procs) = std::fs::read_dir("/proc") else {
            return out;
        };
        for p in procs.flatten() {
            let name = p.file_name();
            if !name.to_string_lossy().bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
            let Ok(fds) = std::fs::read_dir(p.path().join("fdinfo")) else {
                continue;
            };
            for fd in fds.flatten() {
                let Ok(text) = std::fs::read_to_string(fd.path()) else {
                    continue;
                };
                if !text.contains("drm-driver:") {
                    continue;
                }
                let Some((pdev, client, t)) = parse_fdinfo(&text) else {
                    continue;
                };
                if !wanted.contains(pdev.as_str()) || !seen.insert((pdev.clone(), client)) {
                    continue;
                }
                let sum = out.entry(pdev).or_default();
                for (k, v) in t.engine_ns {
                    *sum.engine_ns.entry(k).or_default() += v;
                }
                for (k, v) in t.capacity {
                    sum.capacity.insert(k, v);
                }
                for (k, (b, total)) in t.cycles {
                    let e = sum.cycles.entry(k).or_default();
                    e.0 += b;
                    // The GPU's clock: the same for every client.
                    e.1 = e.1.max(total);
                }
                sum.resident += t.resident;
            }
        }
        out
    }

    /// The PCI address, client id and counters of one DRM file.
    pub(super) fn parse_fdinfo(text: &str) -> Option<(String, String, Totals)> {
        let mut pdev = None;
        let mut client = None;
        let mut t = Totals::default();
        let mut total_cycles: HashMap<String, u64> = HashMap::new();
        for line in text.lines() {
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            let v = v.trim();
            let num = || v.split_whitespace().next()?.parse::<u64>().ok();
            if k == "drm-pdev" {
                pdev = Some(v.to_string());
            } else if k == "drm-client-id" {
                client = Some(v.to_string());
            } else if let Some(e) = k.strip_prefix("drm-engine-capacity-") {
                if let Some(n) = num() {
                    t.capacity.insert(e.to_string(), n);
                }
            } else if let Some(e) = k.strip_prefix("drm-engine-") {
                if let Some(n) = num() {
                    t.engine_ns.insert(e.to_string(), n);
                }
            } else if let Some(e) = k.strip_prefix("drm-total-cycles-") {
                if let Some(n) = num() {
                    total_cycles.insert(e.to_string(), n);
                }
            } else if let Some(e) = k.strip_prefix("drm-cycles-") {
                if let Some(n) = num() {
                    t.cycles.insert(e.to_string(), (n, 0));
                }
            } else if k.starts_with("drm-resident-")
                && let Some(n) = super::parse_size(v)
            {
                t.resident += n;
            }
        }
        for (e, (_, total)) in t.cycles.iter_mut() {
            *total = total_cycles.get(e).copied().unwrap_or(0);
        }
        Some((pdev?, client?, t))
    }
}

#[cfg(target_os = "macos")]
mod mac {
    //! `IOAccelerator` statistics from the I/O registry.

    use std::ffi::{CString, c_char, c_void};

    use pumasi_proto::{
        gpu::{GpuInfo, GpuVendor},
        monitor::GpuMetrics,
    };

    type CfRef = *const c_void;
    const UTF8: u32 = 0x0800_0100;
    /// `kCFNumberSInt64Type`.
    const SINT64: isize = 4;

    #[link(name = "IOKit", kind = "framework")]
    unsafe extern "C" {
        fn IOServiceMatching(name: *const c_char) -> *mut c_void;
        fn IOServiceGetMatchingServices(main_port: u32, matching: *mut c_void, it: *mut u32)
        -> i32;
        fn IOIteratorNext(it: u32) -> u32;
        fn IOObjectRelease(obj: u32) -> i32;
        fn IOObjectGetClass(obj: u32, class: *mut c_char) -> i32;
        fn IORegistryEntryCreateCFProperty(
            entry: u32,
            key: CfRef,
            alloc: CfRef,
            options: u32,
        ) -> CfRef;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFStringCreateWithCString(alloc: CfRef, s: *const c_char, encoding: u32) -> CfRef;
        fn CFRelease(cf: CfRef);
        fn CFGetTypeID(cf: CfRef) -> usize;
        fn CFDictionaryGetTypeID() -> usize;
        fn CFNumberGetTypeID() -> usize;
        fn CFDictionaryGetValue(dict: CfRef, key: CfRef) -> CfRef;
        fn CFNumberGetValue(num: CfRef, kind: isize, out: *mut c_void) -> bool;
    }

    /// A CoreFoundation object we own.
    struct Owned(CfRef);

    impl Drop for Owned {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: created or copied by us, released once.
                unsafe { CFRelease(self.0) };
            }
        }
    }

    fn cf_string(s: &str) -> Option<Owned> {
        let c = CString::new(s).ok()?;
        // SAFETY: a valid C string.
        let cf = unsafe { CFStringCreateWithCString(std::ptr::null(), c.as_ptr(), UTF8) };
        (!cf.is_null()).then_some(Owned(cf))
    }

    fn number(dict: CfRef, key: &str) -> Option<i64> {
        let k = cf_string(key)?;
        // SAFETY: `dict` is a CFDictionary; the value is borrowed.
        unsafe {
            let v = CFDictionaryGetValue(dict, k.0);
            if v.is_null() || CFGetTypeID(v) != CFNumberGetTypeID() {
                return None;
            }
            let mut n: i64 = 0;
            CFNumberGetValue(v, SINT64, &mut n as *mut i64 as *mut c_void).then_some(n)
        }
    }

    /// (vendor, utilization %, memory in use in bytes) of each accelerator.
    fn accelerators() -> Vec<(GpuVendor, f32, u64)> {
        let mut out = Vec::new();
        let Ok(name) = CString::new("IOAccelerator") else {
            return out;
        };
        let Some(key) = cf_string("PerformanceStatistics") else {
            return out;
        };
        // SAFETY: plain I/O Kit calls; every object we get is released.
        unsafe {
            let matching = IOServiceMatching(name.as_ptr());
            if matching.is_null() {
                return out;
            }
            let mut it = 0u32;
            // Consumes `matching`.
            if IOServiceGetMatchingServices(0, matching, &mut it) != 0 {
                return out;
            }
            loop {
                let entry = IOIteratorNext(it);
                if entry == 0 {
                    break;
                }
                let mut class = [0 as c_char; 128];
                IOObjectGetClass(entry, class.as_mut_ptr());
                let class = std::ffi::CStr::from_ptr(class.as_ptr())
                    .to_string_lossy()
                    .to_lowercase();
                let stats = Owned(IORegistryEntryCreateCFProperty(
                    entry,
                    key.0,
                    std::ptr::null(),
                    0,
                ));
                IOObjectRelease(entry);
                if stats.0.is_null() || CFGetTypeID(stats.0) != CFDictionaryGetTypeID() {
                    continue;
                }
                let vendor = if class.contains("agx") {
                    GpuVendor::Apple
                } else if class.contains("amd") || class.contains("ati") || class.contains("radeon")
                {
                    GpuVendor::Amd
                } else if class.contains("intel") {
                    GpuVendor::Intel
                } else if class.contains("nv") || class.contains("geforce") {
                    GpuVendor::Nvidia
                } else {
                    GpuVendor::Other
                };
                let util = number(stats.0, "Device Utilization %")
                    .or_else(|| number(stats.0, "GPU Activity(%)"))
                    .unwrap_or(0);
                let mem = number(stats.0, "vramUsedBytes")
                    .or_else(|| number(stats.0, "In use system memory"))
                    .unwrap_or(0);
                out.push((vendor, util.clamp(0, 100) as f32, mem.max(0) as u64));
            }
            IOObjectRelease(it);
        }
        out
    }

    /// Matches accelerators to the listed GPUs by vendor, in order.
    pub fn sample(gpus: &[&GpuInfo]) -> Vec<GpuMetrics> {
        let mut acc = accelerators();
        let mut out = Vec::new();
        for g in gpus {
            let Some(i) = acc.iter().position(|(v, _, _)| *v == g.vendor) else {
                continue;
            };
            let (_, util, mem) = acc.remove(i);
            out.push(GpuMetrics {
                index: g.index,
                util_percent: util,
                mem_used_mib: mem >> 20,
                mem_total_mib: g.vram_mib,
                temp_c: None,
                power_w: None,
            });
        }
        out
    }
}

#[cfg(windows)]
pub mod windows {
    //! The GPU performance counters (PDH), and DXGI for the adapters.

    use std::collections::HashMap;

    use pumasi_proto::{
        gpu::{GpuInfo, GpuVendor},
        monitor::GpuMetrics,
    };
    use windows_sys::Win32::System::Performance::{
        PDH_FMT_COUNTERVALUE_ITEM_W, PDH_FMT_DOUBLE, PDH_MORE_DATA, PdhAddEnglishCounterW,
        PdhCloseQuery, PdhCollectQueryData, PdhGetFormattedCounterArrayW, PdhOpenQueryW,
    };

    /// A display adapter DXGI lists.
    #[derive(Debug, Clone)]
    pub struct Adapter {
        pub name: String,
        pub vendor: GpuVendor,
        pub dedicated_mib: u64,
        pub shared_mib: u64,
        /// `luid_0xHIGH_0xLOW`, as the performance counters name it.
        pub luid: String,
    }

    /// The hardware display adapters (not the software renderer).
    pub fn adapters() -> Vec<Adapter> {
        use ::windows::Win32::Graphics::Dxgi::{
            CreateDXGIFactory1, DXGI_ADAPTER_FLAG_SOFTWARE, IDXGIFactory1,
        };
        let mut out = Vec::new();
        // SAFETY: DXGI COM calls through the `windows` crate's wrappers.
        let Ok(factory) = (unsafe { CreateDXGIFactory1::<IDXGIFactory1>() }) else {
            return out;
        };
        for i in 0.. {
            let Ok(adapter) = (unsafe { factory.EnumAdapters1(i) }) else {
                break;
            };
            let Ok(d) = (unsafe { adapter.GetDesc1() }) else {
                continue;
            };
            if d.Flags & DXGI_ADAPTER_FLAG_SOFTWARE.0 as u32 != 0 || d.VendorId == 0x1414 {
                continue;
            }
            let len = d.Description.iter().position(|c| *c == 0).unwrap_or(128);
            out.push(Adapter {
                name: String::from_utf16_lossy(&d.Description[..len])
                    .trim()
                    .to_string(),
                vendor: match d.VendorId {
                    0x10de => GpuVendor::Nvidia,
                    0x1002 | 0x1022 => GpuVendor::Amd,
                    0x8086 => GpuVendor::Intel,
                    _ => GpuVendor::Other,
                },
                dedicated_mib: (d.DedicatedVideoMemory as u64) >> 20,
                shared_mib: (d.SharedSystemMemory as u64) >> 20,
                luid: format!(
                    "luid_0x{:08x}_0x{:08x}",
                    d.AdapterLuid.HighPart as u32, d.AdapterLuid.LowPart
                ),
            });
        }
        out
    }

    /// The GPUs to list on Windows besides those NVML found: every other
    /// hardware adapter (Intel, AMD; NVIDIA too when NVML found none).
    pub fn list(have_nvidia: bool) -> Vec<GpuInfo> {
        adapters()
            .into_iter()
            .filter(|a| !(have_nvidia && a.vendor == GpuVendor::Nvidia))
            .map(|a| GpuInfo {
                index: 0,
                vendor: a.vendor,
                name: a.name,
                uuid: String::new(),
                pci: String::new(),
                vram_mib: if a.dedicated_mib >= 256 {
                    a.dedicated_mib
                } else {
                    a.shared_mib
                },
                driver: String::new(),
                compute: String::new(),
                display: false,
                fp32_gflops: 0,
                measured: false,
                mem_bw_gb_s: 0,
                hash_rate: 0,
                docker: false,
                vfio: false,
                render_node: String::new(),
            })
            .collect()
    }

    /// An open PDH query with the two GPU counters.
    type Handle = *mut std::ffi::c_void;

    pub struct WinGpu {
        query: Handle,
        engine: Handle,
        dedicated: Handle,
        shared: Handle,
        /// GPU index to (LUID, uses dedicated memory).
        luids: HashMap<u32, (String, bool)>,
    }

    // The query handle is used from one thread at a time.
    unsafe impl Send for WinGpu {}

    impl Drop for WinGpu {
        fn drop(&mut self) {
            // SAFETY: a query we opened, closed once.
            unsafe { PdhCloseQuery(self.query) };
        }
    }

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    impl WinGpu {
        pub fn open(gpus: &[GpuInfo]) -> Option<Self> {
            let adapters = adapters();
            let mut luids = HashMap::new();
            let mut used = Vec::new();
            for g in gpus {
                if let Some((i, a)) = adapters
                    .iter()
                    .enumerate()
                    .find(|(i, a)| !used.contains(i) && a.vendor == g.vendor && a.name == g.name)
                {
                    used.push(i);
                    luids.insert(g.index, (a.luid.clone(), a.dedicated_mib >= 256));
                }
            }
            if luids.is_empty() {
                return None;
            }
            // SAFETY: PDH calls with valid out pointers and NUL-ended strings.
            unsafe {
                let mut query: Handle = std::ptr::null_mut();
                if PdhOpenQueryW(std::ptr::null(), 0, &mut query) != 0 {
                    return None;
                }
                let add = |path: &str| -> Option<Handle> {
                    let mut c: Handle = std::ptr::null_mut();
                    let w = wide(path);
                    (PdhAddEnglishCounterW(query, w.as_ptr(), 0, &mut c) == 0).then_some(c)
                };
                let (Some(engine), Some(dedicated), Some(shared)) = (
                    add(r"\GPU Engine(*)\Utilization Percentage"),
                    add(r"\GPU Adapter Memory(*)\Dedicated Usage"),
                    add(r"\GPU Adapter Memory(*)\Shared Usage"),
                ) else {
                    PdhCloseQuery(query);
                    return None;
                };
                PdhCollectQueryData(query);
                Some(Self {
                    query,
                    engine,
                    dedicated,
                    shared,
                    luids,
                })
            }
        }

        /// Instance name and value of each instance of a counter.
        fn values(&self, counter: Handle) -> Vec<(String, f64)> {
            let mut out = Vec::new();
            // SAFETY: the buffer is sized as PDH asks; items point into it.
            unsafe {
                let mut size = 0u32;
                let mut count = 0u32;
                let rc = PdhGetFormattedCounterArrayW(
                    counter,
                    PDH_FMT_DOUBLE,
                    &mut size,
                    &mut count,
                    std::ptr::null_mut(),
                );
                if rc != PDH_MORE_DATA {
                    return out;
                }
                let mut buf = vec![0u8; size as usize + 16];
                let items = buf.as_mut_ptr() as *mut PDH_FMT_COUNTERVALUE_ITEM_W;
                if PdhGetFormattedCounterArrayW(
                    counter,
                    PDH_FMT_DOUBLE,
                    &mut size,
                    &mut count,
                    items,
                ) != 0
                {
                    return out;
                }
                for i in 0..count as usize {
                    let it = &*items.add(i);
                    if it.FmtValue.CStatus != 0 || it.szName.is_null() {
                        continue;
                    }
                    let mut len = 0;
                    while *it.szName.add(len) != 0 {
                        len += 1;
                    }
                    let name = String::from_utf16_lossy(std::slice::from_raw_parts(it.szName, len));
                    out.push((name, it.FmtValue.Anonymous.doubleValue));
                }
            }
            out
        }

        pub fn sample(&mut self, gpus: &[&GpuInfo]) -> Vec<GpuMetrics> {
            // SAFETY: our open query.
            if unsafe { PdhCollectQueryData(self.query) } != 0 {
                return Vec::new();
            }
            let engines = engine_use(&self.values(self.engine));
            let dedicated = self.values(self.dedicated);
            let shared = self.values(self.shared);
            let mem = |list: &[(String, f64)], luid: &str| -> u64 {
                list.iter()
                    .filter(|(n, _)| n.to_ascii_lowercase().starts_with(luid))
                    .map(|(_, v)| *v as u64)
                    .sum()
            };
            gpus.iter()
                .filter_map(|g| {
                    let (luid, dedicated_mem) = self.luids.get(&g.index)?;
                    Some(GpuMetrics {
                        index: g.index,
                        util_percent: engines.get(luid.as_str()).copied().unwrap_or(0.0),
                        mem_used_mib: if *dedicated_mem {
                            mem(&dedicated, luid)
                        } else {
                            mem(&shared, luid)
                        } >> 20,
                        mem_total_mib: g.vram_mib,
                        temp_c: None,
                        power_w: None,
                    })
                })
                .collect()
        }
    }

    /// Utilization of each adapter (by `luid_...`) from the `GPU Engine`
    /// instances (`pid_1234_luid_0x00000000_0x0000d1b2_phys_0_eng_0_engtype_3D`):
    /// each engine's use summed over processes, the busiest engine.
    pub fn engine_use(values: &[(String, f64)]) -> HashMap<String, f32> {
        let mut by_engine: HashMap<(String, String), f64> = HashMap::new();
        for (name, v) in values {
            let n = name.to_ascii_lowercase();
            let Some(at) = n.find("luid_") else {
                continue;
            };
            let rest = &n[at..];
            // luid_0xHIGH_0xLOW
            let luid: String = rest.splitn(4, '_').take(3).collect::<Vec<_>>().join("_");
            let engine = rest
                .find("_eng_")
                .map(|e| rest[e..].split("_engtype").next().unwrap_or("").to_string())
                .unwrap_or_default();
            *by_engine.entry((luid, engine)).or_default() += v;
        }
        let mut out: HashMap<String, f32> = HashMap::new();
        for ((luid, _), v) in by_engine {
            let e = out.entry(luid).or_default();
            *e = e.max(v.clamp(0.0, 100.0) as f32);
        }
        out
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn engines_add_up_per_adapter() {
            let v = vec![
                (
                    "pid_10_luid_0x00000000_0x0000D1B2_phys_0_eng_0_engtype_3D".into(),
                    20.0,
                ),
                (
                    "pid_11_luid_0x00000000_0x0000D1B2_phys_0_eng_0_engtype_3D".into(),
                    15.0,
                ),
                (
                    "pid_11_luid_0x00000000_0x0000D1B2_phys_0_eng_3_engtype_VideoDecode".into(),
                    30.0,
                ),
                (
                    "pid_12_luid_0x00000000_0x0000E000_phys_0_eng_0_engtype_3D".into(),
                    5.0,
                ),
            ];
            let u = engine_use(&v);
            assert_eq!(u["luid_0x00000000_0x0000d1b2"], 35.0);
            assert_eq!(u["luid_0x00000000_0x0000e000"], 5.0);
        }

        #[test]
        fn lists_adapters_without_failing() {
            for a in adapters() {
                assert!(a.luid.starts_with("luid_0x"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sizes() {
        assert_eq!(super::parse_size("12 KiB"), Some(12 << 10));
        assert_eq!(super::parse_size("3 MiB"), Some(3 << 20));
        assert_eq!(super::parse_size("100"), Some(100));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn drm_fdinfo_of_i915_and_xe() {
        use super::drm::{Totals, parse_fdinfo, utilization};
        let i915 = "pos:\t0\nflags:\t02100002\ndrm-driver:\ti915\ndrm-client-id:\t7\ndrm-pdev:\t0000:00:02.0\ndrm-total-system0:\t4096 KiB\ndrm-resident-system0:\t2048 KiB\ndrm-engine-render:\t1000000000 ns\ndrm-engine-copy:\t0 ns\ndrm-engine-video:\t500000000 ns\ndrm-engine-capacity-video:\t2\n";
        let (pdev, client, t) = parse_fdinfo(i915).unwrap();
        assert_eq!((pdev.as_str(), client.as_str()), ("0000:00:02.0", "7"));
        assert_eq!(t.resident, 2048 << 10);
        assert_eq!(t.engine_ns["render"], 1_000_000_000);
        assert_eq!(t.capacity["video"], 2);
        let mut later = t.clone();
        later.engine_ns.insert("render".into(), 1_250_000_000);
        // 0.25 s busy of 1 s.
        assert_eq!(utilization(&t, &later, 1_000_000_000), Some(25.0));
        let xe = "drm-driver:\txe\ndrm-client-id:\t3\ndrm-pdev:\t0000:00:02.0\ndrm-cycles-rcs:\t100\ndrm-total-cycles-rcs:\t1000\n";
        let (_, _, a) = parse_fdinfo(xe).unwrap();
        let mut b = a.clone();
        b.cycles.insert("rcs".into(), (600, 2000));
        assert_eq!(utilization(&a, &b, 1), Some(50.0));
        assert_eq!(
            utilization(&Totals::default(), &Totals::default(), 1),
            Some(0.0)
        );
        assert!(parse_fdinfo("drm-driver: i915\n").is_none(), "no pdev");
    }
}
