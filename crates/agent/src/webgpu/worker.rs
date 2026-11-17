//! The WebGPU worker (1.10): a wasm task's GPU work runs in a process of
//! its own, so the task's timeout can stop a shader that never ends.
//!
//! A WGSL shader cannot be interrupted once it runs, and dropping a wgpu
//! device waits for the GPU to go idle, which a runaway shader never
//! lets happen (on a software rasterizer it spins CPU threads inside the
//! agent forever). So the host API ([`super::host`]) does not hold the
//! device: it starts this binary again as a worker (`pumasi
//! __webgpu-worker`) at the task's first GPU call and sends it one
//! request per call over its stdin, reading one answer per request from
//! its stdout. The agent waits for every answer with the task's deadline
//! and its cancellation in view, as it waits for any GPU work. When
//! either hits, it kills the worker: the operating system tears the
//! process down, the GPU driver ends its context and the shader with it,
//! and the agent and its other tasks never notice. The worker also never
//! lets more than [`MAX_IN_FLIGHT`] submissions queue up, so work is
//! handed to the GPU in bounded pieces and a stuck dispatch shows as an
//! unanswered call.
//!
//! [`Worker::Thread`] runs the same loop on a thread of the agent instead,
//! for embedding and tests that have no worker binary; it cannot stop a
//! runaway shader.

use std::{
    collections::VecDeque,
    io::{Read, Write},
    panic::AssertUnwindSafe,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use pumasi_proto::webgpu::{WebGpuBackend, WebGpuFeature};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::{backend_of, backends};

/// The first argument that makes the `pumasi` binary a worker.
pub const WORKER_ARG: &str = "__webgpu-worker";
/// Set in a worker's environment ([`test_entry`] looks for it).
const WORKER_ENV: &str = "PUMASI_WEBGPU_WORKER";
/// What a worker prints first, so whatever its binary printed before
/// (a test harness's banner) is skipped.
const HELLO: &[u8] = b"pumasi-webgpu-worker/1\n";
/// Most shaders and buffers a task may hold at once.
pub const MAX_OBJECTS: usize = 4096;
/// Submissions the worker lets the GPU queue before it waits for the
/// oldest.
pub const MAX_IN_FLIGHT: usize = 16;
/// How often a wait for an answer looks at the deadline and cancellation.
const WAIT_STEP: Duration = Duration::from_millis(50);
/// How long a worker whose task ended gets to exit before it is killed.
const EXIT_GRACE: Duration = Duration::from_secs(2);

/// Where a task's GPU work runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Worker {
    /// On a thread of this process: no binary needed, but a shader that
    /// never ends cannot be stopped.
    #[default]
    Thread,
    /// In a process of its own: `exe` with `args` (and [`WORKER_ENV`] set),
    /// which must call [`serve_stdio`] (`pumasi __webgpu-worker` does).
    Process { exe: PathBuf, args: Vec<String> },
}

impl Worker {
    /// This binary as `<exe> __webgpu-worker` (the `pumasi` CLI).
    pub fn this_binary() -> Self {
        match std::env::current_exe() {
            Ok(exe) => Worker::Process {
                exe,
                args: vec![WORKER_ARG.into()],
            },
            Err(e) => {
                warn!("cannot find this binary ({e}); WebGPU work runs in the agent");
                Worker::Thread
            }
        }
    }

    /// The running test binary, running only the test `test` (which must
    /// call [`test_entry`]).
    pub fn test_binary(test: &str) -> Self {
        Worker::Process {
            exe: std::env::current_exe().expect("test binary"),
            args: vec![
                "--exact".into(),
                test.into(),
                "--nocapture".into(),
                "--test-threads=1".into(),
            ],
        }
    }
}

/// Which adapter a worker opens: what tells it apart among the machine's
/// adapters, as the agent found it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterKey {
    pub backend: WebGpuBackend,
    pub vendor: u32,
    pub device: u32,
    pub name: String,
}

impl AdapterKey {
    pub fn of(adapter: &wgpu::Adapter) -> Option<Self> {
        let info = adapter.get_info();
        Some(Self {
            backend: backend_of(info.backend)?,
            vendor: info.vendor,
            device: info.device,
            name: info.name.trim().to_string(),
        })
    }

    /// Finds the adapter on this machine.
    pub fn find(&self) -> Option<wgpu::Adapter> {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: backends(),
            ..wgpu::InstanceDescriptor::new_without_display_handle_from_env()
        });
        pollster::block_on(instance.enumerate_adapters(backends()))
            .into_iter()
            .find(|a| Self::of(a).as_ref() == Some(self))
    }
}

/// One host API call.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Req {
    /// Opens the device. `info` is the adapter as JSON; the answer adds
    /// the device's limits and features to it.
    Open {
        key: AdapterKey,
        limit_bytes: u64,
        info: String,
        /// The adapter's number among the task's (1.16): its objects'
        /// handles carry it in their upper bits.
        tag: u32,
    },
    Shader {
        src: String,
        entry: String,
    },
    Buffer {
        size: u64,
    },
    Write {
        buf: i32,
        offset: u64,
        data: Vec<u8>,
    },
    Read {
        buf: i32,
        offset: u64,
        len: u64,
    },
    Dispatch {
        shader: i32,
        bufs: Vec<i32>,
        x: u32,
        y: u32,
        z: u32,
    },
    Wait,
    Release {
        handle: i32,
    },
    // Drawing (1.16): textures, samplers, render shaders, bind groups and
    // render passes. Descriptors are JSON with WebGPU's own names
    // ([`super::draw`]).
    Texture {
        json: String,
    },
    Sampler {
        json: String,
    },
    RenderShader {
        src: String,
        json: String,
    },
    /// `region`: mip level, x, y, z, width, height, depth.
    WriteTexture {
        tex: i32,
        region: [u32; 7],
        data: Vec<u8>,
    },
    ReadTexture {
        tex: i32,
        region: [u32; 7],
    },
    BindGroup {
        shader: i32,
        group: u32,
        entries: Vec<(u32, i32)>,
    },
    DispatchGroups {
        shader: i32,
        groups: Vec<i32>,
        x: u32,
        y: u32,
        z: u32,
    },
    Render {
        json: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Resp {
    Value(i32),
    Data(Vec<u8>),
    Info(String),
    /// The call failed; the device is still good.
    Fail(String),
    /// The device is gone (lost, or the driver failed): every later call
    /// fails with this.
    Dead(String),
}

fn write_frame<T: Serialize>(w: &mut impl Write, v: &T) -> std::io::Result<()> {
    let bytes = postcard::to_stdvec(v).map_err(std::io::Error::other)?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| std::io::Error::other("a GPU call of 4 GiB or more"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&bytes)?;
    w.flush()
}

/// `Ok(None)` at a clean end of the stream.
fn read_frame<T: for<'de> Deserialize<'de>>(r: &mut impl Read) -> std::io::Result<Option<T>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut buf)?;
    postcard::from_bytes(&buf)
        .map(Some)
        .map_err(std::io::Error::other)
}

// ---------------------------------------------------------------------------
// The worker's side
// ---------------------------------------------------------------------------

/// Runs a worker on this process's stdin and stdout until the agent closes
/// stdin. Returns the exit code.
pub fn serve_stdio() -> i32 {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if out.write_all(HELLO).and_then(|()| out.flush()).is_err() {
        return 1;
    }
    serve(&mut stdin.lock(), &mut out, AdapterKey::find);
    0
}

/// For a test binary used as the worker ([`Worker::test_binary`]): serves
/// and exits when this process was started as a worker, else returns.
pub fn test_entry() {
    if std::env::var_os(WORKER_ENV).is_some() {
        std::process::exit(serve_stdio());
    }
}

/// Answers requests until the stream ends.
pub(crate) fn serve(
    r: &mut impl Read,
    w: &mut impl Write,
    find: impl Fn(&AdapterKey) -> Option<wgpu::Adapter>,
) {
    let mut session: Option<Session> = None;
    let mut dead: Option<String> = None;
    loop {
        let req: Req = match read_frame(r) {
            Ok(Some(req)) => req,
            Ok(None) => return,
            Err(e) => {
                debug!("WebGPU worker: {e}");
                return;
            }
        };
        let resp = if let Some(d) = &dead {
            Resp::Dead(d.clone())
        } else {
            match std::panic::catch_unwind(AssertUnwindSafe(|| handle(&mut session, req, &find))) {
                Ok(r) => r,
                Err(_) => Resp::Dead("the GPU driver failed".into()),
            }
        };
        if let Resp::Dead(d) = &resp {
            // The device cannot be trusted any more.
            dead = Some(d.clone());
            session = None;
        }
        if write_frame(w, &resp).is_err() {
            return;
        }
    }
}

pub(super) struct Session {
    pub(super) adapter: wgpu::Adapter,
    pub(super) device: wgpu::Device,
    pub(super) queue: wgpu::Queue,
    objects: Vec<Option<Object>>,
    pub(super) bytes: u64,
    pub(super) limit_bytes: u64,
    /// Errors wgpu raised outside an error scope, and device loss.
    fault: Arc<Mutex<Option<String>>>,
    in_flight: VecDeque<wgpu::SubmissionIndex>,
    /// The adapter's number among the task's, in every handle's upper
    /// bits (1.16).
    tag: u32,
}

/// Handles are `tag << HANDLE_BITS | n`, with `n` from 1 to
/// [`MAX_OBJECTS`], so a task with several GPUs (1.16) can tell which one
/// an object lives on; a task with one GPU sees the numbers it always did.
pub const HANDLE_BITS: u32 = 16;

pub(super) enum Object {
    Shader(wgpu::ComputePipeline),
    Buffer(wgpu::Buffer, u64),
    /// The texture, its default view and the bytes it takes (1.16).
    Texture(wgpu::Texture, wgpu::TextureView, u64),
    Sampler(wgpu::Sampler),
    Render(wgpu::RenderPipeline),
    Group(wgpu::BindGroup),
}

impl Object {
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Object::Shader(_) => "a compute shader",
            Object::Buffer(..) => "a buffer",
            Object::Texture(..) => "a texture",
            Object::Sampler(_) => "a sampler",
            Object::Render(_) => "a render shader",
            Object::Group(_) => "a bind group",
        }
    }
}

/// The optional features a task's device gets when its adapter has them.
pub(crate) fn wanted_features() -> wgpu::Features {
    wgpu::Features::SHADER_F16 | wgpu::Features::SUBGROUP
}

/// The proto names of `features`.
pub(crate) fn feature_names(features: wgpu::Features) -> Vec<WebGpuFeature> {
    let mut out = Vec::new();
    if features.contains(wgpu::Features::SHADER_F16) {
        out.push(WebGpuFeature::ShaderF16);
    }
    if features.contains(wgpu::Features::SUBGROUP) {
        out.push(WebGpuFeature::Subgroups);
    }
    out
}

/// Opens a device with the adapter's own limits and the optional features
/// it has.
pub(crate) fn open_device(adapter: &wgpu::Adapter) -> Result<(wgpu::Device, wgpu::Queue)> {
    let features = adapter.features() & wanted_features();
    let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
        label: Some("pumasi"),
        required_features: features,
        required_limits: adapter.limits(),
        ..Default::default()
    }))
    .context("open the GPU")?;
    Ok((device, queue))
}

/// Runs `f` with an error scope, turning what wgpu caught into a failure.
pub(super) fn scoped<R>(
    device: &wgpu::Device,
    f: impl FnOnce() -> R,
) -> std::result::Result<R, String> {
    let validation = device.push_error_scope(wgpu::ErrorFilter::Validation);
    let memory = device.push_error_scope(wgpu::ErrorFilter::OutOfMemory);
    let r = f();
    let oom = pollster::block_on(memory.pop());
    let invalid = pollster::block_on(validation.pop());
    match invalid.or(oom) {
        Some(e) => Err(e.to_string()),
        None => Ok(r),
    }
}

impl Session {
    fn fault(&self) -> Option<String> {
        self.fault.lock().unwrap().clone()
    }

    pub(super) fn add(&mut self, o: Object) -> std::result::Result<i32, String> {
        let i = match self.objects.iter().position(Option::is_none) {
            Some(i) => {
                self.objects[i] = Some(o);
                i
            }
            None => {
                if self.objects.len() >= MAX_OBJECTS {
                    return Err(format!(
                        "too many GPU objects (at most {MAX_OBJECTS}); release some"
                    ));
                }
                self.objects.push(Some(o));
                self.objects.len() - 1
            }
        };
        Ok(((self.tag << HANDLE_BITS) | (i as u32 + 1)) as i32)
    }

    /// The slot of `handle` in `objects`.
    fn index(&self, handle: i32) -> std::result::Result<usize, String> {
        let h = u32::try_from(handle).map_err(|_| format!("no GPU object {handle}"))?;
        if h >> HANDLE_BITS != self.tag {
            return Err(format!(
                "GPU object {handle} belongs to another of the task's GPUs"
            ));
        }
        ((h & ((1 << HANDLE_BITS) - 1)) as usize)
            .checked_sub(1)
            .filter(|i| self.objects.get(*i).is_some_and(Option::is_some))
            .ok_or_else(|| format!("no GPU object {handle}"))
    }

    pub(super) fn object(&self, handle: i32) -> std::result::Result<&Object, String> {
        let i = self.index(handle)?;
        Ok(self.objects[i].as_ref().expect("checked"))
    }

    pub(super) fn buffer(&self, handle: i32) -> std::result::Result<(&wgpu::Buffer, u64), String> {
        match self.object(handle)? {
            Object::Buffer(b, size) => Ok((b, *size)),
            o => Err(format!("{handle} is {}, not a buffer", o.kind())),
        }
    }

    /// Waits for the oldest submissions while more than
    /// [`MAX_IN_FLIGHT`] are queued.
    pub(super) fn submitted(
        &mut self,
        index: wgpu::SubmissionIndex,
    ) -> std::result::Result<(), String> {
        self.in_flight.push_back(index);
        while self.in_flight.len() > MAX_IN_FLIGHT {
            let oldest = self.in_flight.pop_front().expect("not empty");
            self.wait_until(Some(oldest), || true)?;
        }
        Ok(())
    }

    /// After a wait for everything: nothing is queued any more.
    pub(super) fn drained(&mut self) {
        self.in_flight.clear();
    }

    /// Waits for the GPU until `done` or a fault. Never gives up on its
    /// own: the agent kills the worker when the task's time is up.
    pub(super) fn wait_until(
        &self,
        submission: Option<wgpu::SubmissionIndex>,
        done: impl Fn() -> bool,
    ) -> std::result::Result<(), String> {
        loop {
            if let Some(f) = self.fault() {
                return Err(f);
            }
            match self.device.poll(wgpu::PollType::Wait {
                submission_index: submission.clone(),
                timeout: Some(Duration::from_secs(1)),
            }) {
                Ok(_) if done() => return Ok(()),
                Ok(_) | Err(wgpu::PollError::Timeout) => continue,
                Err(e) => return Err(format!("GPU: {e}")),
            }
        }
    }
}

fn handle(
    session: &mut Option<Session>,
    req: Req,
    find: &impl Fn(&AdapterKey) -> Option<wgpu::Adapter>,
) -> Resp {
    let r = match req {
        Req::Open {
            key,
            limit_bytes,
            info,
            tag,
        } => return open(session, &key, limit_bytes, &info, tag, find),
        req => match session.as_mut() {
            Some(s) => match s.fault() {
                Some(f) => return Resp::Dead(f),
                None => call(s, req),
            },
            None => Err("the GPU is not open".to_string()),
        },
    };
    match r {
        Ok(resp) => resp,
        Err(msg) => match session.as_ref().and_then(Session::fault) {
            // A fault while the call ran means the device is gone.
            Some(f) if f.starts_with("the GPU was lost") => Resp::Dead(f),
            _ => Resp::Fail(msg),
        },
    }
}

fn open(
    session: &mut Option<Session>,
    key: &AdapterKey,
    limit_bytes: u64,
    info: &str,
    tag: u32,
    find: &impl Fn(&AdapterKey) -> Option<wgpu::Adapter>,
) -> Resp {
    let Some(adapter) = find(key) else {
        return Resp::Dead(format!("the GPU {} is gone", key.name));
    };
    let (device, queue) = match open_device(&adapter) {
        Ok(d) => d,
        Err(e) => return Resp::Dead(format!("{e:#}")),
    };
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
    let mut json: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(info).unwrap_or_default();
    let extra = serde_json::json!({
        "max_buffer_size": l.max_buffer_size,
        "max_storage_buffer_binding_size": l.max_storage_buffer_binding_size,
        "max_storage_buffers_per_shader_stage": l.max_storage_buffers_per_shader_stage,
        "max_compute_workgroup_size_x": l.max_compute_workgroup_size_x,
        "max_compute_workgroup_size_y": l.max_compute_workgroup_size_y,
        "max_compute_workgroup_size_z": l.max_compute_workgroup_size_z,
        "max_compute_invocations_per_workgroup": l.max_compute_invocations_per_workgroup,
        "max_compute_workgroups_per_dimension": l.max_compute_workgroups_per_dimension,
        "max_compute_workgroup_storage_size": l.max_compute_workgroup_storage_size,
        "max_texture_dimension_2d": l.max_texture_dimension_2d,
        "max_texture_dimension_3d": l.max_texture_dimension_3d,
        "max_bind_groups": l.max_bind_groups,
        "max_color_attachments": l.max_color_attachments,
        "max_vertex_buffers": l.max_vertex_buffers,
        "memory_limit": limit_bytes,
        "features": feature_names(device.features())
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>(),
    });
    if let serde_json::Value::Object(m) = extra {
        json.extend(m);
    }
    *session = Some(Session {
        adapter,
        device,
        queue,
        objects: Vec::new(),
        bytes: 0,
        limit_bytes,
        fault,
        in_flight: VecDeque::new(),
        tag,
    });
    Resp::Info(serde_json::Value::Object(json).to_string())
}

fn aligned(v: u64, what: &str) -> std::result::Result<u64, String> {
    if !v.is_multiple_of(wgpu::COPY_BUFFER_ALIGNMENT) {
        return Err(format!("{what} must be a multiple of 4"));
    }
    Ok(v)
}

fn call(s: &mut Session, req: Req) -> std::result::Result<Resp, String> {
    match req {
        Req::Open { .. } => unreachable!("handled before"),
        Req::Shader { src, entry } => {
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
            Ok(Resp::Value(s.add(Object::Shader(pipeline))?))
        }
        Req::Buffer { size } => {
            aligned(size, "a buffer's size")?;
            if size == 0 {
                return Err("a buffer needs a size".into());
            }
            if s.bytes.saturating_add(size) > s.limit_bytes {
                return Err(format!(
                    "GPU buffers would take {} MiB, over the job's memory limit of {} MiB",
                    (s.bytes + size) >> 20,
                    s.limit_bytes >> 20
                ));
            }
            let max = s.device.limits().max_buffer_size;
            if size > max {
                return Err(format!("this GPU's largest buffer is {} MiB", max >> 20));
            }
            let buf = scoped(&s.device, || {
                s.device.create_buffer(&wgpu::BufferDescriptor {
                    label: Some("task"),
                    size,
                    // Vertex and index buffers for drawing since 1.16.
                    usage: wgpu::BufferUsages::STORAGE
                        | wgpu::BufferUsages::UNIFORM
                        | wgpu::BufferUsages::VERTEX
                        | wgpu::BufferUsages::INDEX
                        | wgpu::BufferUsages::COPY_SRC
                        | wgpu::BufferUsages::COPY_DST,
                    mapped_at_creation: false,
                })
            })?;
            s.bytes += size;
            Ok(Resp::Value(s.add(Object::Buffer(buf, size))?))
        }
        Req::Write { buf, offset, data } => {
            aligned(offset, "the offset")?;
            aligned(data.len() as u64, "the length")?;
            let (b, size) = s.buffer(buf)?;
            if offset.saturating_add(data.len() as u64) > size {
                return Err("the write goes past the end of the buffer".into());
            }
            scoped(&s.device, || s.queue.write_buffer(b, offset, &data))?;
            Ok(Resp::Value(0))
        }
        Req::Read { buf, offset, len } => {
            aligned(offset, "the offset")?;
            aligned(len, "the length")?;
            if len == 0 {
                return Ok(Resp::Data(Vec::new()));
            }
            Ok(Resp::Data(read_buffer(s, buf, offset, len)?))
        }
        Req::Dispatch {
            shader,
            bufs,
            x,
            y,
            z,
        } => {
            if bufs.len() > 64 {
                return Err("at most 64 buffers per dispatch".into());
            }
            let pipeline = match s.object(shader)? {
                Object::Shader(p) => p,
                o => return Err(format!("{shader} is {}, not a compute shader", o.kind())),
            };
            let mut buffers = Vec::with_capacity(bufs.len());
            for h in &bufs {
                buffers.push(s.buffer(*h)?.0);
            }
            let index = scoped(&s.device, || {
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
                s.queue.submit([enc.finish()])
            })?;
            // Bounded: the GPU never has more than this queued from us.
            s.submitted(index)?;
            Ok(Resp::Value(0))
        }
        Req::Wait => {
            s.wait_until(None, || true)?;
            s.in_flight.clear();
            Ok(Resp::Value(0))
        }
        Req::Release { handle } => {
            let i = s.index(handle)?;
            match s.objects[i].take() {
                Some(Object::Buffer(b, size)) => {
                    b.destroy();
                    s.bytes -= size;
                }
                Some(Object::Texture(t, _, size)) => {
                    t.destroy();
                    s.bytes -= size;
                }
                _ => {}
            }
            Ok(Resp::Value(0))
        }
        Req::Texture { json } => super::draw::texture(s, &json),
        Req::Sampler { json } => super::draw::sampler(s, &json),
        Req::RenderShader { src, json } => super::draw::render_shader(s, &src, &json),
        Req::WriteTexture { tex, region, data } => {
            super::draw::write_texture(s, tex, region, &data)
        }
        Req::ReadTexture { tex, region } => super::draw::read_texture(s, tex, region),
        Req::BindGroup {
            shader,
            group,
            entries,
        } => super::draw::bind_group(s, shader, group, &entries),
        Req::DispatchGroups {
            shader,
            groups,
            x,
            y,
            z,
        } => super::draw::dispatch_groups(s, shader, &groups, [x, y, z]),
        Req::Render { json } => super::draw::render(s, &json),
    }
}

/// Copies `n` bytes of a buffer out, once the GPU is done with it.
fn read_buffer(
    s: &mut Session,
    buf: i32,
    offset: u64,
    n: u64,
) -> std::result::Result<Vec<u8>, String> {
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
    map_read(s, &staging)
}

/// Waits for the GPU to finish what was submitted and copies `staging`
/// (a `MAP_READ` buffer) out.
pub(super) fn map_read(
    s: &mut Session,
    staging: &wgpu::Buffer,
) -> std::result::Result<Vec<u8>, String> {
    type Mapped = Arc<Mutex<Option<std::result::Result<(), wgpu::BufferAsyncError>>>>;
    let mapped: Mapped = Default::default();
    {
        let mapped = mapped.clone();
        staging.map_async(wgpu::MapMode::Read, .., move |r| {
            *mapped.lock().unwrap() = Some(r);
        });
    }
    s.wait_until(None, || mapped.lock().unwrap().is_some())?;
    s.drained();
    let answer = mapped.lock().unwrap().take();
    match answer {
        Some(Ok(())) => {}
        Some(Err(e)) => return Err(format!("read the buffer: {e}")),
        None => return Err("read the buffer: no answer".into()),
    }
    let out = staging
        .get_mapped_range(..)
        .map_err(|e| format!("read the buffer: {e}"))?
        .to_vec();
    staging.unmap();
    Ok(out)
}

// ---------------------------------------------------------------------------
// The agent's side
// ---------------------------------------------------------------------------

/// Why a call ended without an answer.
#[derive(Debug)]
pub(crate) enum Gone {
    /// The task's deadline passed (the worker was killed).
    Timeout,
    /// The task was cancelled (the worker was killed).
    Cancelled,
    /// The worker stopped or could not start.
    Failed(String),
}

/// A running worker and the way to talk to it.
pub(crate) struct Client {
    to: Option<Box<dyn Write + Send>>,
    from: mpsc::Receiver<Option<Resp>>,
    child: Option<Child>,
    /// The end of the worker's stderr, for when it dies.
    stderr: Arc<Mutex<Vec<u8>>>,
}

impl Client {
    /// Starts a worker. `local` is the adapter itself, for
    /// [`Worker::Thread`].
    pub fn start(worker: &Worker, local: wgpu::Adapter) -> Result<Self> {
        let (tx, rx) = mpsc::channel();
        let stderr: Arc<Mutex<Vec<u8>>> = Default::default();
        match worker {
            Worker::Thread => {
                let (req_r, req_w) = std::io::pipe()?;
                let (resp_r, resp_w) = std::io::pipe()?;
                std::thread::Builder::new()
                    .name("pumasi-webgpu".into())
                    .spawn(move || {
                        let (mut r, mut w) = (req_r, resp_w);
                        serve(&mut r, &mut w, move |_| Some(local.clone()));
                    })?;
                spawn_reader(resp_r, tx, false);
                Ok(Self {
                    to: Some(Box::new(req_w)),
                    from: rx,
                    child: None,
                    stderr,
                })
            }
            Worker::Process { exe, args } => {
                let mut cmd = Command::new(exe);
                cmd.args(args)
                    .env(WORKER_ENV, "1")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
                    cmd.creation_flags(CREATE_NO_WINDOW);
                }
                let mut child = cmd
                    .spawn()
                    .with_context(|| format!("start the WebGPU worker {}", exe.display()))?;
                let out = child.stdout.take().context("worker stdout")?;
                let err = child.stderr.take().context("worker stderr")?;
                let to = child.stdin.take().context("worker stdin")?;
                spawn_reader(out, tx, true);
                {
                    let stderr = stderr.clone();
                    std::thread::Builder::new()
                        .name("pumasi-webgpu-err".into())
                        .spawn(move || keep_tail(err, &stderr))?;
                }
                Ok(Self {
                    to: Some(Box::new(to)),
                    from: rx,
                    child: Some(child),
                    stderr,
                })
            }
        }
    }

    /// Sends one request and waits for its answer until `deadline` or
    /// `cancel`, killing the worker when either comes first.
    pub fn call(
        &mut self,
        req: &Req,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> std::result::Result<Resp, Gone> {
        let Some(to) = self.to.as_mut() else {
            return Err(Gone::Failed("the GPU worker has stopped".into()));
        };
        if let Err(e) = write_frame(to, req) {
            return Err(self.failed(&format!("send to the GPU worker: {e}")));
        }
        loop {
            if cancel.load(Ordering::Relaxed) {
                self.kill();
                return Err(Gone::Cancelled);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                self.kill();
                return Err(Gone::Timeout);
            }
            match self.from.recv_timeout(left.min(WAIT_STEP)) {
                Ok(Some(resp)) => return Ok(resp),
                Ok(None) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(self.failed("the GPU worker stopped"));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
            }
        }
    }

    fn failed(&mut self, what: &str) -> Gone {
        self.kill();
        // Give the stderr reader a moment to catch the worker's last words.
        std::thread::sleep(Duration::from_millis(50));
        let tail = String::from_utf8_lossy(&self.stderr.lock().unwrap()).to_string();
        let tail = tail.trim();
        if tail.is_empty() {
            Gone::Failed(what.to_string())
        } else {
            Gone::Failed(format!("{what}: {tail}"))
        }
    }

    /// Ends the worker now; the GPU driver drops its device.
    pub fn kill(&mut self) {
        self.to = None;
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            // Reaping can wait on the driver; not on the task's time.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Closing stdin ends a worker that is waiting for a request; one
        // still busy on the GPU is killed shortly after.
        self.to = None;
        if let Some(mut child) = self.child.take() {
            std::thread::spawn(move || {
                let until = Instant::now() + EXIT_GRACE;
                while Instant::now() < until {
                    if let Ok(Some(_)) = child.try_wait() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                let _ = child.kill();
                let _ = child.wait();
            });
        }
    }
}

/// Reads answers into `tx` (`None` when the stream ends). A worker
/// process's stdout starts with whatever its binary printed before
/// [`HELLO`].
fn spawn_reader(r: impl Read + Send + 'static, tx: mpsc::Sender<Option<Resp>>, hello: bool) {
    let spawned = std::thread::Builder::new()
        .name("pumasi-webgpu-out".into())
        .spawn(move || {
            let mut r = std::io::BufReader::new(r);
            if hello && skip_to_hello(&mut r).is_err() {
                let _ = tx.send(None);
                return;
            }
            loop {
                match read_frame::<Resp>(&mut r) {
                    Ok(Some(resp)) => {
                        if tx.send(Some(resp)).is_err() {
                            return;
                        }
                    }
                    _ => {
                        let _ = tx.send(None);
                        return;
                    }
                }
            }
        });
    if let Err(e) = spawned {
        warn!("start the WebGPU reader: {e}");
    }
}

/// Reads up to and including [`HELLO`].
fn skip_to_hello(r: &mut impl std::io::BufRead) -> Result<()> {
    let mut line = Vec::new();
    for _ in 0..1000 {
        line.clear();
        if r.read_until(b'\n', &mut line)? == 0 {
            bail!("the worker ended before it started");
        }
        if line.ends_with(HELLO) {
            return Ok(());
        }
    }
    bail!("the worker did not say hello")
}

/// Keeps the last 4 KiB of `r`.
fn keep_tail(mut r: impl Read, tail: &Mutex<Vec<u8>>) {
    let mut buf = [0u8; 1024];
    while let Ok(n) = r.read(&mut buf) {
        if n == 0 {
            return;
        }
        let mut t = tail.lock().unwrap();
        t.extend_from_slice(&buf[..n]);
        let len = t.len();
        if len > 4096 {
            t.drain(..len - 4096);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_and_hello_is_found() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &Resp::Value(7)).unwrap();
        write_frame(&mut buf, &Resp::Data(vec![1, 2, 3])).unwrap();
        let mut r = &buf[..];
        assert!(matches!(
            read_frame::<Resp>(&mut r).unwrap(),
            Some(Resp::Value(7))
        ));
        assert!(
            matches!(read_frame::<Resp>(&mut r).unwrap(), Some(Resp::Data(d)) if d == [1, 2, 3])
        );
        assert!(read_frame::<Resp>(&mut r).unwrap().is_none());
        let mut noisy = b"\nrunning 1 test\n".to_vec();
        noisy.extend_from_slice(HELLO);
        noisy.extend_from_slice(&buf);
        let mut r = std::io::BufReader::new(&noisy[..]);
        skip_to_hello(&mut r).unwrap();
        assert!(matches!(
            read_frame::<Resp>(&mut r).unwrap(),
            Some(Resp::Value(7))
        ));
        assert!(skip_to_hello(&mut std::io::BufReader::new(&b"nothing\n"[..])).is_err());
    }
}
