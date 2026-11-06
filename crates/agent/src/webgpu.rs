//! The WebGPU host API (phase 14, experimental): wasm tasks that asked for
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
//! | `adapter(ptr, cap) -> len` | the adapter as JSON (name, backend, limits); -1 without a GPU |
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
//! its timeout and cancellation: waiting on the GPU stops when they hit.

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    gpu::{GpuInfo, GpuVendor},
    webgpu::{
        WEBGPU_BENCH_VERSION, WEBGPU_MODULE, WebGpuAdapter, WebGpuBackend, WebGpuDeviceType,
        WebGpuProfile,
    },
};
use tracing::{debug, info, warn};
use wasmtime::{Caller, Extern, Linker, Trap};

/// Most shaders and buffers a task may hold at once.
const MAX_OBJECTS: usize = 4096;
/// How often a wait on the GPU looks at the deadline and cancellation.
const WAIT_STEP: Duration = Duration::from_millis(50);
/// Target length of one benchmark dispatch.
const BENCH_TARGET: Duration = Duration::from_millis(150);

/// The machine's WebGPU adapters, opened once by the agent.
pub struct WebGpu {
    adapters: Vec<(WebGpuAdapter, wgpu::Adapter)>,
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
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// Offer software rasterizers (lavapipe, llvmpipe, WARP) too: slow,
    /// for tests and machines without a GPU driver.
    pub software: bool,
    /// Measure each adapter's FP32 throughput.
    pub bench: bool,
}

fn backend_of(b: wgpu::Backend) -> Option<WebGpuBackend> {
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
fn backends() -> wgpu::Backends {
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
        for (i, (a, adapter)) in kept.iter_mut().enumerate() {
            a.index = i as u32;
            a.gpu = link_gpu(a, gpus);
            if opts.bench {
                match bench(adapter) {
                    Ok(gflops) => a.fp32_gflops = gflops,
                    Err(e) => {
                        warn!(adapter = %a.name, "WebGPU benchmark failed: {e:#}");
                        notes.push(format!("WebGPU {}: benchmark failed: {e:#}", a.name));
                    }
                }
            }
            info!(
                adapter = a.index,
                name = %a.describe(),
                gpu = ?a.gpu,
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
            _instance: instance,
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

/// A profile for these adapters (empty when the node offers none).
pub fn profile(node: [u8; 32], adapters: Vec<WebGpuAdapter>, notes: Vec<String>) -> WebGpuProfile {
    WebGpuProfile {
        bench_version: WEBGPU_BENCH_VERSION,
        node,
        measured_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        adapters,
        notes,
    }
}

fn open_device(adapter: &wgpu::Adapter) -> Result<(wgpu::Device, wgpu::Queue)> {
    let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
        label: Some("pumasi"),
        required_limits: adapter.limits(),
        ..Default::default()
    }))
    .context("open the GPU")?;
    Ok((device, queue))
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
        if let Some(e) = errors.lock().unwrap().take() {
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

// ---------------------------------------------------------------------------
// The host API
// ---------------------------------------------------------------------------

/// The adapter a task runs on.
#[derive(Debug, Clone)]
pub struct WebGpuTask {
    pub gpu: Arc<WebGpu>,
    pub adapter: u32,
}

impl WebGpuTask {
    /// The adapter's description.
    pub fn info(&self) -> Option<WebGpuAdapter> {
        self.gpu.get(self.adapter).map(|(a, _)| a.clone())
    }
}

/// What the host API keeps per task: the adapter it may use and, once the
/// module made its first call, the device and its objects.
pub struct GpuState {
    task: Option<WebGpuTask>,
    session: Option<Session>,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    limit_bytes: u64,
    error: String,
    /// Time spent waiting for the GPU.
    pub busy: Duration,
    /// Dispatches run.
    pub dispatches: u64,
}

impl Default for GpuState {
    fn default() -> Self {
        Self::new(None, Instant::now(), Default::default(), 0)
    }
}

struct Session {
    device: wgpu::Device,
    queue: wgpu::Queue,
    objects: Vec<Option<Object>>,
    bytes: u64,
    /// Errors wgpu raised outside an error scope, and device loss.
    fault: Arc<Mutex<Option<String>>>,
    info: String,
}

enum Object {
    Shader(wgpu::ComputePipeline),
    Buffer(wgpu::Buffer, u64),
}

/// Why a call stopped: a failure the module sees (-1 and a message), or
/// the end of the task (timeout, cancellation).
enum Stop {
    Fail(String),
    Abort(wasmtime::Error),
}

impl From<String> for Stop {
    fn from(s: String) -> Self {
        Stop::Fail(s)
    }
}

impl From<&str> for Stop {
    fn from(s: &str) -> Self {
        Stop::Fail(s.to_string())
    }
}

type Call<T> = std::result::Result<T, Stop>;

impl GpuState {
    pub fn new(
        task: Option<WebGpuTask>,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        limit_bytes: u64,
    ) -> Self {
        Self {
            task,
            session: None,
            deadline,
            cancel,
            limit_bytes,
            error: String::new(),
            busy: Duration::ZERO,
            dispatches: 0,
        }
    }

    /// What the task did on the GPU, for its stderr; `None` when it did
    /// not use it.
    pub fn summary(&self) -> Option<String> {
        self.session.as_ref()?;
        let name = self
            .task
            .as_ref()
            .and_then(WebGpuTask::info)
            .map(|a| a.describe())
            .unwrap_or_default();
        Some(format!(
            "WebGPU {name}: {} dispatches, {} ms waiting for the GPU",
            self.dispatches,
            self.busy.as_millis()
        ))
    }

    /// The device, opened on first use.
    fn session(&mut self) -> Call<&mut Session> {
        if self.session.is_none() {
            let task = self
                .task
                .as_ref()
                .ok_or("this task has no GPU (ask for one with gpus = 1)")?;
            let (info, adapter) = task.gpu.get(task.adapter).ok_or("the GPU is gone")?;
            let (device, queue) = open_device(adapter).map_err(|e| format!("{e:#}"))?;
            let fault: Arc<Mutex<Option<String>>> = Default::default();
            {
                let fault = fault.clone();
                device.on_uncaptured_error(Arc::new(move |e: wgpu::Error| {
                    fault.lock().unwrap().get_or_insert(e.to_string());
                }));
            }
            {
                let fault = fault.clone();
                device.set_device_lost_callback(move |_, msg| {
                    fault
                        .lock()
                        .unwrap()
                        .get_or_insert(format!("the GPU was lost: {msg}"));
                });
            }
            let l = device.limits();
            let info = serde_json::json!({
                "name": info.name,
                "vendor": info.vendor.to_string(),
                "backend": info.backend.to_string(),
                "device_type": info.device_type.to_string(),
                "driver": info.driver,
                "fp32_gflops": info.fp32_gflops,
                "max_buffer_size": l.max_buffer_size,
                "max_storage_buffer_binding_size": l.max_storage_buffer_binding_size,
                "max_storage_buffers_per_shader_stage": l.max_storage_buffers_per_shader_stage,
                "max_compute_workgroup_size_x": l.max_compute_workgroup_size_x,
                "max_compute_workgroup_size_y": l.max_compute_workgroup_size_y,
                "max_compute_workgroup_size_z": l.max_compute_workgroup_size_z,
                "max_compute_invocations_per_workgroup": l.max_compute_invocations_per_workgroup,
                "max_compute_workgroups_per_dimension": l.max_compute_workgroups_per_dimension,
                "max_compute_workgroup_storage_size": l.max_compute_workgroup_storage_size,
                "memory_limit": self.limit_bytes,
            })
            .to_string();
            self.session = Some(Session {
                device,
                queue,
                objects: Vec::new(),
                bytes: 0,
                fault,
                info,
            });
        }
        let s = self.session.as_mut().expect("opened");
        if let Some(f) = s.fault.lock().unwrap().clone() {
            return Err(Stop::Fail(f));
        }
        Ok(s)
    }

    /// Waits for the GPU to finish what was submitted, up to the task's
    /// deadline.
    fn wait_idle(&mut self) -> Call<()> {
        let (deadline, cancel) = (self.deadline, self.cancel.clone());
        let start = Instant::now();
        let s = self.session()?;
        let r = wait_device(&s.device, deadline, &cancel);
        let fault = s.fault.lock().unwrap().clone();
        self.busy += start.elapsed();
        r?;
        match fault {
            Some(f) => Err(Stop::Fail(f)),
            None => Ok(()),
        }
    }
}

fn wait_device(device: &wgpu::Device, deadline: Instant, cancel: &AtomicBool) -> Call<()> {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(Stop::Abort(wasmtime::Error::msg(
                crate::executor::CANCELLED,
            )));
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Stop::Abort(Trap::Interrupt.into()));
        }
        match device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: Some(left.min(WAIT_STEP)),
        }) {
            Ok(_) => return Ok(()),
            Err(wgpu::PollError::Timeout) => continue,
            Err(e) => return Err(Stop::Fail(format!("GPU: {e}"))),
        }
    }
}

/// Runs `f` with an error scope, turning what wgpu caught into a failure.
fn scoped<R>(device: &wgpu::Device, f: impl FnOnce() -> R) -> Call<R> {
    let validation = device.push_error_scope(wgpu::ErrorFilter::Validation);
    let memory = device.push_error_scope(wgpu::ErrorFilter::OutOfMemory);
    let r = f();
    let oom = pollster::block_on(memory.pop());
    let invalid = pollster::block_on(validation.pop());
    if let Some(e) = invalid.or(oom) {
        return Err(Stop::Fail(e.to_string()));
    }
    Ok(r)
}

fn to_len(v: i32) -> Call<usize> {
    usize::try_from(v).map_err(|_| Stop::from("negative length"))
}

fn range(data: &[u8], ptr: i32, len: i32) -> Call<std::ops::Range<usize>> {
    let start = to_len(ptr)?;
    let end = start
        .checked_add(to_len(len)?)
        .filter(|e| *e <= data.len())
        .ok_or("pointer outside the module's memory")?;
    Ok(start..end)
}

fn aligned(v: u64, what: &str) -> Call<u64> {
    if !v.is_multiple_of(wgpu::COPY_BUFFER_ALIGNMENT) {
        return Err(Stop::Fail(format!("{what} must be a multiple of 4")));
    }
    Ok(v)
}

impl Session {
    fn add(&mut self, o: Object) -> Call<i32> {
        if let Some(i) = self.objects.iter().position(Option::is_none) {
            self.objects[i] = Some(o);
            return Ok(i as i32 + 1);
        }
        if self.objects.len() >= MAX_OBJECTS {
            return Err(Stop::Fail(format!(
                "too many GPU objects (at most {MAX_OBJECTS}); release some"
            )));
        }
        self.objects.push(Some(o));
        Ok(self.objects.len() as i32)
    }

    fn object(&self, handle: i32) -> Call<&Object> {
        usize::try_from(handle)
            .ok()
            .and_then(|h| h.checked_sub(1))
            .and_then(|i| self.objects.get(i))
            .and_then(Option::as_ref)
            .ok_or_else(|| Stop::Fail(format!("no GPU object {handle}")))
    }

    fn buffer(&self, handle: i32) -> Call<(&wgpu::Buffer, u64)> {
        match self.object(handle)? {
            Object::Buffer(b, size) => Ok((b, *size)),
            Object::Shader(_) => Err(Stop::Fail(format!("{handle} is a shader, not a buffer"))),
        }
    }
}

/// Calls `f` with the module's memory and the GPU state, and turns its
/// result into what the module sees: `Ok(n)` as is, failures as -1 with
/// the message kept for `error`, the end of the task as a trap.
fn with_state<T: 'static>(
    caller: &mut Caller<'_, T>,
    get: fn(&mut T) -> &mut GpuState,
    f: impl FnOnce(&mut [u8], &mut GpuState) -> Call<i32>,
) -> wasmtime::Result<i32> {
    let Some(Extern::Memory(memory)) = caller.get_export("memory") else {
        let st = get(caller.data_mut());
        st.error = "the module exports no memory".into();
        return Ok(-1);
    };
    let (data, t) = memory.data_and_store_mut(caller);
    let st = get(t);
    match std::panic::catch_unwind(AssertUnwindSafe(|| f(data, st))) {
        Ok(Ok(n)) => Ok(n),
        Ok(Err(Stop::Fail(msg))) => {
            st.error = msg;
            Ok(-1)
        }
        Ok(Err(Stop::Abort(e))) => Err(e),
        Err(_) => {
            // The driver or wgpu panicked: the device cannot be trusted.
            st.session = None;
            st.error = "the GPU driver failed".into();
            Ok(-1)
        }
    }
}

/// Copies `n` bytes of a buffer out, once the GPU is done with it.
fn read_buffer(
    s: &mut Session,
    buf: i32,
    offset: u64,
    n: u64,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Call<Vec<u8>> {
    let (b, size) = s.buffer(buf)?;
    if offset.saturating_add(n) > size {
        return Err("the read goes past the end of the buffer".into());
    }
    let staging = scoped(&s.device, || {
        let staging = s.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("read"),
            size: n,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let mut enc = s.device.create_command_encoder(&Default::default());
        enc.copy_buffer_to_buffer(b, offset, &staging, 0, n);
        s.queue.submit([enc.finish()]);
        staging
    })?;
    type Mapped = Arc<Mutex<Option<std::result::Result<(), wgpu::BufferAsyncError>>>>;
    let mapped: Mapped = Default::default();
    {
        let mapped = mapped.clone();
        staging.map_async(wgpu::MapMode::Read, .., move |r| {
            *mapped.lock().unwrap() = Some(r);
        });
    }
    loop {
        wait_device(&s.device, deadline, cancel)?;
        if mapped.lock().unwrap().is_some() {
            break;
        }
        if let Some(f) = s.fault.lock().unwrap().clone() {
            return Err(Stop::Fail(f));
        }
    }
    match mapped.lock().unwrap().take() {
        Some(Ok(())) => {}
        Some(Err(e)) => return Err(Stop::Fail(format!("read the buffer: {e}"))),
        None => return Err("read the buffer: no answer".into()),
    }
    let out = staging
        .get_mapped_range(..)
        .map_err(|e| Stop::Fail(format!("read the buffer: {e}")))?
        .to_vec();
    staging.unmap();
    Ok(out)
}

/// Adds the `pumasi_webgpu` imports. Every module can import them; they
/// fail politely in tasks without a GPU.
pub fn add_to_linker<T: 'static>(
    linker: &mut Linker<T>,
    get: fn(&mut T) -> &mut GpuState,
) -> Result<()> {
    let m = WEBGPU_MODULE;
    linker.func_wrap(
        m,
        "adapter",
        move |mut c: Caller<'_, T>, ptr: i32, cap: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                if st.task.is_none() {
                    return Err("this task has no GPU (ask for one with gpus = 1)".into());
                }
                let info = st.session()?.info.clone();
                let n = info.len().min(to_len(cap)?);
                let r = range(data, ptr, n as i32)?;
                data[r].copy_from_slice(&info.as_bytes()[..n]);
                Ok(info.len() as i32)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "error",
        move |mut c: Caller<'_, T>, ptr: i32, cap: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let msg = st.error.as_bytes();
                let n = msg.len().min(to_len(cap)?);
                let r = range(data, ptr, n as i32)?;
                data[r].copy_from_slice(&msg[..n]);
                Ok(msg.len() as i32)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "shader",
        move |mut c: Caller<'_, T>,
              src: i32,
              src_len: i32,
              entry: i32,
              entry_len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let src = std::str::from_utf8(&data[range(data, src, src_len)?])
                    .map_err(|_| "the shader is not UTF-8")?
                    .to_string();
                let entry = std::str::from_utf8(&data[range(data, entry, entry_len)?])
                    .map_err(|_| "the entry point is not UTF-8")?
                    .to_string();
                let s = st.session()?;
                let pipeline = scoped(&s.device, || {
                    let module = s.device.create_shader_module(wgpu::ShaderModuleDescriptor {
                        label: Some("task"),
                        source: wgpu::ShaderSource::Wgsl(src.into()),
                    });
                    s.device
                        .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                            label: Some("task"),
                            layout: None,
                            module: &module,
                            entry_point: Some(&entry),
                            compilation_options: Default::default(),
                            cache: None,
                        })
                })?;
                s.add(Object::Shader(pipeline))
            })
        },
    )?;
    linker.func_wrap(
        m,
        "buffer",
        move |mut c: Caller<'_, T>, size: i64| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                let size = u64::try_from(size).map_err(|_| "negative size")?;
                aligned(size, "a buffer's size")?;
                if size == 0 {
                    return Err("a buffer needs a size".into());
                }
                let limit = st.limit_bytes;
                let s = st.session()?;
                if s.bytes.saturating_add(size) > limit {
                    return Err(Stop::Fail(format!(
                        "GPU buffers would take {} MiB, over the job's memory limit of {} MiB",
                        (s.bytes + size) >> 20,
                        limit >> 20
                    )));
                }
                let max = s.device.limits().max_buffer_size;
                if size > max {
                    return Err(Stop::Fail(format!(
                        "this GPU's largest buffer is {} MiB",
                        max >> 20
                    )));
                }
                let buf = scoped(&s.device, || {
                    s.device.create_buffer(&wgpu::BufferDescriptor {
                        label: Some("task"),
                        size,
                        usage: wgpu::BufferUsages::STORAGE
                            | wgpu::BufferUsages::UNIFORM
                            | wgpu::BufferUsages::COPY_SRC
                            | wgpu::BufferUsages::COPY_DST,
                        mapped_at_creation: false,
                    })
                })?;
                s.bytes += size;
                s.add(Object::Buffer(buf, size))
            })
        },
    )?;
    linker.func_wrap(
        m,
        "write",
        move |mut c: Caller<'_, T>,
              buf: i32,
              offset: i64,
              src: i32,
              len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let offset = aligned(
                    u64::try_from(offset).map_err(|_| "negative offset")?,
                    "the offset",
                )?;
                let r = range(data, src, len)?;
                aligned(r.len() as u64, "the length")?;
                let s = st.session()?;
                let (b, size) = s.buffer(buf)?;
                if offset.saturating_add(r.len() as u64) > size {
                    return Err("the write goes past the end of the buffer".into());
                }
                scoped(&s.device, || s.queue.write_buffer(b, offset, &data[r]))?;
                Ok(0)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "read",
        move |mut c: Caller<'_, T>,
              buf: i32,
              offset: i64,
              dst: i32,
              len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let offset = aligned(
                    u64::try_from(offset).map_err(|_| "negative offset")?,
                    "the offset",
                )?;
                let r = range(data, dst, len)?;
                let n = aligned(r.len() as u64, "the length")?;
                if n == 0 {
                    return Ok(0);
                }
                let (deadline, cancel) = (st.deadline, st.cancel.clone());
                let start = Instant::now();
                let result = read_buffer(st.session()?, buf, offset, n, deadline, &cancel);
                st.busy += start.elapsed();
                data[r].copy_from_slice(&result?);
                Ok(0)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "dispatch",
        move |mut c: Caller<'_, T>,
              shader: i32,
              bufs: i32,
              n: i32,
              x: i32,
              y: i32,
              z: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let count = to_len(n)?;
                if count > 64 {
                    return Err("at most 64 buffers per dispatch".into());
                }
                let r = range(data, bufs, (count * 4) as i32)?;
                let handles: Vec<i32> = data[r]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| i32::from_le_bytes(*c))
                    .collect();
                let dim =
                    |v: i32| u32::try_from(v).map_err(|_| Stop::from("negative workgroup count"));
                let (x, y, z) = (dim(x)?, dim(y)?, dim(z)?);
                let s = st.session()?;
                let Object::Shader(pipeline) = s.object(shader)? else {
                    return Err(Stop::Fail(format!("{shader} is a buffer, not a shader")));
                };
                let mut buffers = Vec::with_capacity(count);
                for h in &handles {
                    buffers.push(s.buffer(*h)?.0);
                }
                scoped(&s.device, || {
                    let group = (!buffers.is_empty()).then(|| {
                        let entries: Vec<wgpu::BindGroupEntry> = buffers
                            .iter()
                            .enumerate()
                            .map(|(i, b)| wgpu::BindGroupEntry {
                                binding: i as u32,
                                resource: b.as_entire_binding(),
                            })
                            .collect();
                        s.device.create_bind_group(&wgpu::BindGroupDescriptor {
                            label: Some("task"),
                            layout: &pipeline.get_bind_group_layout(0),
                            entries: &entries,
                        })
                    });
                    let mut enc = s.device.create_command_encoder(&Default::default());
                    {
                        let mut pass = enc.begin_compute_pass(&Default::default());
                        pass.set_pipeline(pipeline);
                        if let Some(g) = &group {
                            pass.set_bind_group(0, g, &[]);
                        }
                        pass.dispatch_workgroups(x, y, z);
                    }
                    s.queue.submit([enc.finish()]);
                })?;
                st.dispatches += 1;
                Ok(0)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "wait",
        move |mut c: Caller<'_, T>| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                st.wait_idle()?;
                Ok(0)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "release",
        move |mut c: Caller<'_, T>, handle: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                let s = st.session()?;
                s.object(handle)?;
                let i = handle as usize - 1;
                if let Some(Object::Buffer(b, size)) = s.objects[i].take() {
                    b.destroy();
                    s.bytes -= size;
                }
                Ok(0)
            })
        },
    )?;
    Ok(())
}
