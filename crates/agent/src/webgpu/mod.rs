//! The WebGPU executor (phase 14, experimental): wasm tasks that asked for
//! a GPU run WGSL compute shaders on one adapter through wgpu, which talks
//! to Vulkan, Metal, DX12 or OpenGL, whichever the machine has. It is the
//! only way to use GPUs without CUDA (Intel and AMD integrated GPUs, the
//! GPUs of old Intel Macs, Apple Silicon).
//!
//! The module imports these functions from `pumasi_webgpu`. Pointers and
//! lengths are into the module's exported `memory`; every call returns a
//! negative number on failure, and `error` copies out the reason (WGSL
//! compile errors included):
//!
//! | import | does |
//! | --- | --- |
//! | `adapter(ptr, cap) -> len` | the adapter as JSON (name, backend, limits, features); -1 without a GPU |
//! | `shader(src, len, entry, entry_len) -> handle` | compiles a WGSL compute entry point |
//! | `buffer(size: i64) -> handle` | a zero-filled storage buffer (size a multiple of 4) |
//! | `write(buf, offset: i64, src, len) -> 0` | copies memory into a buffer |
//! | `read(buf, offset: i64, dst, len) -> 0` | waits for earlier dispatches, copies a buffer out |
//! | `dispatch(shader, bufs, n, x, y, z) -> 0` | runs `x*y*z` workgroups with `n` buffers (`u32` handles at `bufs`) at `@group(0) @binding(0..n)` |
//! | `wait() -> 0` | waits for every dispatch so far |
//! | `release(handle) -> 0` | frees a shader or buffer |
//! | `error(ptr, cap) -> len` | the last failure's message |
//!
//! The calls map one to one onto WebGPU (`createShaderModule` and
//! `createComputePipeline` with an automatic layout, `createBuffer`,
//! `queue.writeBuffer`, a compute pass, `mapAsync`), so modules can move
//! to the component model's `wasi:webgpu` (wasi-gfx) once Wasmtime runs
//! it. The job's memory limit applies to its GPU buffers too, and so do
//! its timeout and cancellation: the device lives in a worker process
//! ([`worker`], 1.10) that is killed when they hit, which stops even a
//! shader that never ends.
//!
//! This module finds and measures the adapters (FP32, and the GPU
//! challenge's hash rate with [`hash`], 1.10); [`host`] is the API.

pub mod draw;
pub mod hash;
pub mod host;
pub mod worker;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use pumasi_proto::{
    gpu::{GpuInfo, GpuVendor},
    webgpu::{
        AdapterExtra, WEBGPU_BENCH_VERSION, WebGpuAdapter, WebGpuBackend, WebGpuDeviceType,
        WebGpuExtra, WebGpuProfile,
    },
};
use tracing::{debug, info, warn};

pub use host::{GpuState, WebGpuTask, add_to_linker};
pub use worker::Worker;

/// Target length of one benchmark dispatch.
const BENCH_TARGET: Duration = Duration::from_millis(150);

/// The machine's WebGPU adapters, opened once by the agent.
pub struct WebGpu {
    adapters: Vec<(WebGpuAdapter, wgpu::Adapter)>,
    /// Per adapter (by index): what it has beyond the profile (1.10).
    extra: Vec<AdapterExtra>,
    /// Memory bandwidth in GB/s, by adapter index (0: not measured).
    mem_bw: HashMap<u32, u32>,
    /// GPU challenge kernels, by adapter index, kept open once made.
    hashers: Mutex<HashMap<u32, Arc<hash::Hasher>>>,
    /// Where tasks' GPU work runs.
    worker: Worker,
    _instance: wgpu::Instance,
}

impl std::fmt::Debug for WebGpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebGpu")
            .field("adapters", &self.adapters.len())
            .finish()
    }
}

/// Which adapters to offer.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Offer software rasterizers (lavapipe, llvmpipe, WARP) too: slow,
    /// for tests and machines without a GPU driver.
    pub software: bool,
    /// Measure each adapter's FP32 throughput, memory bandwidth and GPU
    /// challenge hash rate.
    pub bench: bool,
    /// Where tasks' GPU work runs (1.10): `pumasi` uses
    /// [`Worker::this_binary`], so the task's timeout can stop it.
    pub worker: Worker,
}

pub(crate) fn backend_of(b: wgpu::Backend) -> Option<WebGpuBackend> {
    Some(match b {
        wgpu::Backend::Vulkan => WebGpuBackend::Vulkan,
        wgpu::Backend::Metal => WebGpuBackend::Metal,
        wgpu::Backend::Dx12 => WebGpuBackend::Dx12,
        wgpu::Backend::Gl => WebGpuBackend::Gl,
        _ => return None,
    })
}

fn device_type_of(t: wgpu::DeviceType) -> WebGpuDeviceType {
    match t {
        wgpu::DeviceType::DiscreteGpu => WebGpuDeviceType::Discrete,
        wgpu::DeviceType::IntegratedGpu => WebGpuDeviceType::Integrated,
        wgpu::DeviceType::VirtualGpu => WebGpuDeviceType::Virtual,
        wgpu::DeviceType::Cpu => WebGpuDeviceType::Cpu,
        wgpu::DeviceType::Other => WebGpuDeviceType::Other,
    }
}

/// Native APIs first: one physical GPU shows up once per backend.
fn backend_rank(b: WebGpuBackend) -> u8 {
    match b {
        WebGpuBackend::Metal | WebGpuBackend::Dx12 => 0,
        WebGpuBackend::Vulkan => 1,
        WebGpuBackend::Gl => 2,
    }
}

/// The backends to look at: OpenGL only on Linux and BSDs, as a fallback
/// for GPUs without Vulkan (on Windows and macOS it adds nothing but
/// duplicates). `WGPU_BACKEND` overrides it.
pub(crate) fn backends() -> wgpu::Backends {
    let default = if cfg!(any(windows, target_os = "macos")) {
        wgpu::Backends::PRIMARY
    } else {
        wgpu::Backends::PRIMARY | wgpu::Backends::GL
    };
    wgpu::Backends::from_env().unwrap_or(default)
}

fn normalize_pci(s: &str) -> String {
    let s = s.trim().to_ascii_lowercase();
    // `01:00.0` and `0000:01:00.0` are the same device.
    if s.matches(':').count() == 1 {
        format!("0000:{s}")
    } else {
        s
    }
}

/// The device profile's GPU this adapter is: same PCI address, or else the
/// only GPU of its vendor whose name matches.
fn link_gpu(a: &WebGpuAdapter, gpus: &[GpuInfo]) -> Option<u32> {
    if a.software() {
        return None;
    }
    // A GPU in MIG mode draws nothing and its slices are not adapters.
    let gpus: Vec<GpuInfo> = gpus.iter().filter(|g| !g.mig()).cloned().collect();
    let gpus = gpus.as_slice();
    if !a.pci.is_empty() {
        let pci = normalize_pci(&a.pci);
        if let Some(g) = gpus
            .iter()
            .find(|g| !g.pci.is_empty() && normalize_pci(&g.pci) == pci)
        {
            return Some(g.index);
        }
    }
    let name = a.name.to_ascii_lowercase();
    let same: Vec<&GpuInfo> = gpus
        .iter()
        .filter(|g| g.vendor == a.vendor && g.vendor != GpuVendor::Other)
        .filter(|g| {
            let g = g.name.to_ascii_lowercase();
            !g.is_empty() && (name.contains(&g) || g.contains(&name))
        })
        .collect();
    match same.as_slice() {
        [g] => Some(g.index),
        _ => None,
    }
}

impl WebGpu {
    /// Opens the machine's adapters that can run compute shaders. Never
    /// fails: what could not be used goes to `notes`. `gpus` is the
    /// device profile, to tell which listed GPU each adapter is.
    pub fn detect(opts: Options, gpus: &[GpuInfo], notes: &mut Vec<String>) -> Self {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: backends(),
            ..wgpu::InstanceDescriptor::new_without_display_handle_from_env()
        });
        let found = pollster::block_on(instance.enumerate_adapters(backends()));
        let mut seen: Vec<(WebGpuAdapter, wgpu::Adapter, u32, u32)> = Vec::new();
        for adapter in found {
            let info = adapter.get_info();
            let Some(backend) = backend_of(info.backend) else {
                continue;
            };
            let device_type = device_type_of(info.device_type);
            if !adapter
                .get_downlevel_capabilities()
                .flags
                .contains(wgpu::DownlevelFlags::COMPUTE_SHADERS)
            {
                notes.push(format!(
                    "WebGPU: {} ({backend}) cannot run compute shaders",
                    info.name
                ));
                continue;
            }
            if device_type == WebGpuDeviceType::Cpu && !opts.software {
                debug!(adapter = %info.name, "software WebGPU adapter not offered");
                continue;
            }
            let limits = adapter.limits();
            let mut driver = info.driver.trim().to_string();
            if !info.driver_info.trim().is_empty() {
                if !driver.is_empty() {
                    driver.push(' ');
                }
                driver.push_str(info.driver_info.trim());
            }
            let a = WebGpuAdapter {
                index: 0,
                name: info.name.trim().to_string(),
                vendor: GpuVendor::from_pci(info.vendor as u16),
                backend,
                device_type,
                driver,
                gpu: None,
                pci: info.device_pci_bus_id.trim().to_string(),
                max_buffer_mib: limits.max_buffer_size >> 20,
                max_binding_mib: limits.max_storage_buffer_binding_size >> 20,
                fp32_gflops: 0,
            };
            seen.push((a, adapter, info.vendor, info.device));
        }
        // One entry per physical GPU, on its best backend; OpenGL only for
        // GPUs nothing else reaches, and one software rasterizer at most.
        seen.sort_by_key(|(a, ..)| (backend_rank(a.backend), a.software()));
        let has_native_hw = seen
            .iter()
            .any(|(a, ..)| a.backend != WebGpuBackend::Gl && !a.software());
        let mut kept: Vec<(WebGpuAdapter, wgpu::Adapter)> = Vec::new();
        let mut keys: Vec<(u32, u32, String)> = Vec::new();
        for (a, adapter, vendor, device) in seen {
            if a.backend == WebGpuBackend::Gl && !a.software() && has_native_hw {
                continue;
            }
            if a.software() && kept.iter().any(|(k, _)| k.software()) {
                continue;
            }
            let key = if device != 0 {
                (vendor, device, String::new())
            } else {
                (vendor, 0, a.name.to_ascii_lowercase())
            };
            if keys.contains(&key) {
                continue;
            }
            keys.push(key);
            kept.push((a, adapter));
        }
        // Hardware first, so a node's first adapter is its best.
        kept.sort_by_key(|(a, _)| (a.software(), backend_rank(a.backend)));
        let mut extra = Vec::new();
        let mut mem_bw = HashMap::new();
        let mut hashers = HashMap::new();
        for (i, (a, adapter)) in kept.iter_mut().enumerate() {
            a.index = i as u32;
            a.gpu = link_gpu(a, gpus);
            let mut hash_rate = 0;
            if opts.bench {
                match bench(adapter) {
                    Ok(gflops) => a.fp32_gflops = gflops,
                    Err(e) => {
                        warn!(adapter = %a.name, "WebGPU benchmark failed: {e:#}");
                        notes.push(format!("WebGPU {}: benchmark failed: {e:#}", a.name));
                    }
                }
                match bench_mem(adapter) {
                    Ok(bw) => {
                        mem_bw.insert(a.index, bw);
                    }
                    Err(e) => debug!(adapter = %a.name, "WebGPU bandwidth: {e:#}"),
                }
                match hash::Hasher::new(adapter).and_then(|h| Ok((h.hash_rate()?, h))) {
                    Ok((rate, h)) => {
                        hash_rate = rate;
                        hashers.insert(a.index, Arc::new(h));
                    }
                    Err(e) => {
                        warn!(adapter = %a.name, "WebGPU challenge kernel failed: {e:#}");
                        notes.push(format!("WebGPU {}: GPU challenge failed: {e:#}", a.name));
                    }
                }
            }
            extra.push(AdapterExtra {
                index: a.index,
                hash_rate,
                features: worker::feature_names(adapter.features() & worker::wanted_features()),
            });
            info!(
                adapter = a.index,
                name = %a.describe(),
                gpu = ?a.gpu,
                hash_rate,
                features = ?extra.last().map(|e| &e.features),
                "WebGPU adapter"
            );
        }
        // Two adapters must not claim one GPU (the cluster refuses that).
        let mut linked = std::collections::HashSet::new();
        for (a, _) in kept.iter_mut() {
            if let Some(g) = a.gpu
                && !linked.insert(g)
            {
                a.gpu = None;
            }
        }
        Self {
            adapters: kept,
            extra,
            mem_bw,
            hashers: Mutex::new(hashers),
            worker: opts.worker,
            _instance: instance,
        }
    }

    /// What the adapters have beyond the profile (1.10), to be signed.
    pub fn extra(&self, node: [u8; 32]) -> WebGpuExtra {
        WebGpuExtra {
            node,
            measured_unix: now_unix(),
            adapters: self.extra.clone(),
        }
    }

    /// What the agent measured of the adapter that is GPU `gpu` of the
    /// device profile: FP32 GFLOPS, memory bandwidth in GB/s and the GPU
    /// challenge hash rate (1.10: how GPUs without CUDA are measured).
    pub fn measured_gpu(&self, gpu: u32) -> Option<(u32, u32, u64)> {
        let (a, _) = self.adapters.iter().find(|(a, _)| a.gpu == Some(gpu))?;
        let hash = self.extra.iter().find(|e| e.index == a.index)?.hash_rate;
        Some((
            a.fp32_gflops,
            self.mem_bw.get(&a.index).copied().unwrap_or(0),
            hash,
        ))
    }

    /// Answers a GPU challenge on the adapter in `slot` (a GPU index for
    /// adapters that are listed GPUs). Empty when it cannot.
    pub fn solve(&self, slot: u32, c: &pumasi_proto::gpu::GpuChallenge) -> Vec<u64> {
        let Some(index) = self.by_slot(slot) else {
            return Vec::new();
        };
        let hasher = {
            let mut hashers = self.hashers.lock().unwrap();
            match hashers.get(&index) {
                Some(h) => h.clone(),
                None => {
                    let Some((_, adapter)) = self.get(index) else {
                        return Vec::new();
                    };
                    match hash::Hasher::new(adapter) {
                        Ok(h) => hashers.entry(index).or_insert(Arc::new(h)).clone(),
                        Err(e) => {
                            warn!("GPU check over WebGPU: {e:#}");
                            return Vec::new();
                        }
                    }
                }
            }
        };
        let rate = self
            .extra
            .iter()
            .find(|e| e.index == index)
            .map_or(0, |e| e.hash_rate);
        match hasher.solve(c, rate) {
            Ok(xs) => xs,
            Err(e) => {
                warn!("GPU check over WebGPU failed: {e:#}");
                Vec::new()
            }
        }
    }

    pub fn adapters(&self) -> Vec<WebGpuAdapter> {
        self.adapters.iter().map(|(a, _)| a.clone()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }

    /// The adapter in this slot (as the cluster names it in assignments).
    pub fn by_slot(&self, slot: u32) -> Option<u32> {
        self.adapters
            .iter()
            .find(|(a, _)| a.slot() == slot)
            .map(|(a, _)| a.index)
    }

    fn get(&self, index: u32) -> Option<&(WebGpuAdapter, wgpu::Adapter)> {
        self.adapters.iter().find(|(a, _)| a.index == index)
    }

    /// The signed-to-be profile.
    pub fn profile(&self, node: [u8; 32], notes: Vec<String>) -> WebGpuProfile {
        profile(node, self.adapters(), notes)
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A profile for these adapters (empty when the node offers none).
pub fn profile(node: [u8; 32], adapters: Vec<WebGpuAdapter>, notes: Vec<String>) -> WebGpuProfile {
    WebGpuProfile {
        bench_version: WEBGPU_BENCH_VERSION,
        node,
        measured_unix: now_unix(),
        adapters,
        notes,
    }
}

use worker::open_device;

/// Device memory bandwidth in GB/s: a buffer copied on the GPU, counting
/// what is read and what is written.
pub fn bench_mem(adapter: &wgpu::Adapter) -> Result<u32> {
    let (device, queue) = open_device(adapter)?;
    let size = (adapter.limits().max_buffer_size / 4).clamp(4 << 20, 256 << 20) & !3;
    let usage = wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST;
    let a = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("bw-a"),
        size,
        usage,
        mapped_at_creation: false,
    });
    let b = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("bw-b"),
        size,
        usage,
        mapped_at_creation: false,
    });
    let copy = |times: u32| -> Result<Duration> {
        let mut enc = device.create_command_encoder(&Default::default());
        for i in 0..times {
            let (from, to) = if i % 2 == 0 { (&a, &b) } else { (&b, &a) };
            enc.copy_buffer_to_buffer(from, 0, to, 0, size);
        }
        let start = Instant::now();
        let index = queue.submit([enc.finish()]);
        device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(index),
                timeout: Some(Duration::from_secs(20)),
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(start.elapsed())
    };
    copy(1)?;
    let mut times = 2;
    loop {
        let took = copy(times)?;
        if took >= BENCH_TARGET || times >= 256 {
            let bytes = 2.0 * size as f64 * times as f64;
            return Ok((bytes / took.as_secs_f64().max(1e-6) / 1e9).round() as u32);
        }
        times *= 2;
    }
}

/// Every chain does one FMA (2 FLOPs) per step.
const BENCH_CHAINS: u32 = 8;
const BENCH_STEPS: u32 = 256;
const BENCH_WORKGROUP: u32 = 64;
const BENCH_WGSL: &str = r#"
@group(0) @binding(0) var<storage, read_write> out: array<f32>;
@group(0) @binding(1) var<storage, read> k: array<f32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    let s = f32(id.x) * 1e-9;
    var a = vec4<f32>(s, s + 0.1, s + 0.2, s + 0.3);
    var b = vec4<f32>(s + 0.4, s + 0.5, s + 0.6, s + 0.7);
    let m = vec4<f32>(k[0]);
    let c = vec4<f32>(k[1]);
    for (var i = 0u; i < 256u; i = i + 1u) {
        a = fma(a, m, c);
        b = fma(b, m, c);
    }
    let r = a + b;
    out[id.x] = r.x + r.y + r.z + r.w;
}
"#;

/// FP32 GFLOPS of a stream of independent FMAs: the same kind of figure
/// as the CUDA benchmark, so billing compares like with like.
pub fn bench(adapter: &wgpu::Adapter) -> Result<u32> {
    let (device, queue) = open_device(adapter)?;
    let errors: Arc<Mutex<Option<String>>> = Default::default();
    {
        let errors = errors.clone();
        device.on_uncaptured_error(Arc::new(move |e: wgpu::Error| {
            errors.lock().unwrap().get_or_insert(e.to_string());
        }));
    }
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("bench"),
        source: wgpu::ShaderSource::Wgsl(BENCH_WGSL.into()),
    });
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("bench"),
        layout: None,
        module: &module,
        entry_point: Some("main"),
        compilation_options: Default::default(),
        cache: None,
    });
    let max_groups = device.limits().max_compute_workgroups_per_dimension.max(1);
    let max_bytes = device
        .limits()
        .max_storage_buffer_binding_size
        .min(device.limits().max_buffer_size);
    let max_groups =
        max_groups.min((max_bytes / 4 / BENCH_WORKGROUP as u64).clamp(1, u32::MAX as u64) as u32);
    let out = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("bench-out"),
        size: max_groups as u64 * BENCH_WORKGROUP as u64 * 4,
        usage: wgpu::BufferUsages::STORAGE,
        mapped_at_creation: false,
    });
    let k = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("bench-k"),
        size: 16,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    queue.write_buffer(&k, 0, bytemuck_f32(&[0.999_9, 0.000_1, 0.0, 0.0]));
    let group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("bench"),
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: out.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: k.as_entire_binding(),
            },
        ],
    });
    let run = |groups: u32| -> Result<Duration> {
        let mut enc = device.create_command_encoder(&Default::default());
        {
            let mut pass = enc.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &group, &[]);
            pass.dispatch_workgroups(groups, 1, 1);
        }
        let start = Instant::now();
        let index = queue.submit([enc.finish()]);
        device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(index),
                timeout: Some(Duration::from_secs(20)),
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let error = errors.lock().unwrap().take();
        if let Some(e) = error {
            bail!("{e}");
        }
        Ok(start.elapsed())
    };
    // Warm up (shader compilation), then grow the dispatch until it takes
    // long enough to time.
    run(1)?;
    let mut groups = 16u32.min(max_groups);
    let mut best = 0f64;
    let started = Instant::now();
    loop {
        let took = run(groups)?;
        let flops =
            groups as f64 * BENCH_WORKGROUP as f64 * BENCH_STEPS as f64 * BENCH_CHAINS as f64 * 2.0;
        best = best.max(flops / took.as_secs_f64().max(1e-6) / 1e9);
        if took >= BENCH_TARGET
            || groups >= max_groups
            || started.elapsed() > Duration::from_secs(5)
        {
            break;
        }
        let grow = (BENCH_TARGET.as_secs_f64() / took.as_secs_f64().max(1e-4)).clamp(2.0, 64.0);
        groups = ((groups as f64 * grow) as u64).min(max_groups as u64) as u32;
    }
    Ok(best.round().clamp(1.0, u32::MAX as f64) as u32)
}

fn bytemuck_f32(v: &[f32]) -> &[u8] {
    // SAFETY: f32 has no padding and any byte pattern is a valid u8.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The first adapter, software ones included; `None` (and a note) when
    /// the machine has none, unless `PUMASI_TEST_WEBGPU=require`.
    pub(crate) fn any_adapter() -> Option<wgpu::Adapter> {
        let w = WebGpu::detect(
            Options {
                software: true,
                ..Default::default()
            },
            &[],
            &mut Vec::new(),
        );
        let first = w.adapters.first().map(|(_, a)| a.clone());
        if first.is_none() {
            assert!(
                std::env::var("PUMASI_TEST_WEBGPU").as_deref() != Ok("require"),
                "no WebGPU adapter, and PUMASI_TEST_WEBGPU=require"
            );
            eprintln!("no WebGPU adapter here; skipped");
        }
        first
    }

    #[test]
    fn adapters_measure_bandwidth() {
        let Some(adapter) = any_adapter() else {
            return;
        };
        assert!(bench_mem(&adapter).unwrap() > 0);
    }
}
