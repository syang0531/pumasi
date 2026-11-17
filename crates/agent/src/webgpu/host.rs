//! The `pumasi_webgpu` imports a wasm module calls. Each one checks its
//! arguments against the module's memory and turns into one request to
//! the task's GPU worker ([`super::worker`]), which holds the device. The
//! task's deadline and cancellation bound every wait for an answer: when
//! either hits, the worker is killed and the call ends the task.

use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};

use anyhow::Result;
use pumasi_proto::webgpu::{WEBGPU_MODULE, WebGpuAdapter};
use wasmtime::{Caller, Extern, Linker, Trap};

use super::{
    WebGpu,
    worker::{AdapterKey, Client, Gone, HANDLE_BITS, Req, Resp},
};

/// The adapters a task runs on: one for `gpus = 1`, more since 1.16.
#[derive(Debug, Clone)]
pub struct WebGpuTask {
    pub gpu: Arc<WebGpu>,
    /// Adapter indices, in the order the module numbers them (`select`).
    pub adapters: Vec<u32>,
}

impl WebGpuTask {
    /// A task with one adapter.
    pub fn one(gpu: Arc<WebGpu>, adapter: u32) -> Self {
        Self {
            gpu,
            adapters: vec![adapter],
        }
    }

    /// The description of the task's `n`th adapter.
    pub fn info(&self, n: usize) -> Option<WebGpuAdapter> {
        self.gpu.get(*self.adapters.get(n)?).map(|(a, _)| a.clone())
    }
}

/// One of the task's adapters, once the module used it: the worker that
/// holds its device.
#[derive(Default)]
struct Device {
    client: Option<Client>,
    /// The adapter as JSON, once the device is open.
    info: String,
    /// Why the device cannot be used any more.
    dead: Option<String>,
}

/// What the host API keeps per task: the adapters it may use and, once the
/// module used one, the worker holding its device.
pub struct GpuState {
    task: Option<WebGpuTask>,
    devices: Vec<Device>,
    /// The adapter new objects go to (`select`).
    current: usize,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    limit_bytes: u64,
    error: String,
    /// Time spent waiting for the GPU.
    pub busy: Duration,
    /// Dispatches run.
    pub dispatches: u64,
    /// Draws run (1.16).
    pub draws: u64,
    /// The worker was killed at the deadline or on cancellation.
    pub killed: bool,
}

impl Default for GpuState {
    fn default() -> Self {
        Self::new(None, Instant::now(), Default::default(), 0)
    }
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

const NO_GPU: &str = "this task has no GPU (ask for one with gpus = 1)";
const ODD: &str = "the GPU worker answered oddly";

impl GpuState {
    pub fn new(
        task: Option<WebGpuTask>,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        limit_bytes: u64,
    ) -> Self {
        let n = task.as_ref().map_or(0, |t| t.adapters.len());
        Self {
            task,
            devices: (0..n).map(|_| Device::default()).collect(),
            current: 0,
            deadline,
            cancel,
            limit_bytes,
            error: String::new(),
            busy: Duration::ZERO,
            dispatches: 0,
            draws: 0,
            killed: false,
        }
    }

    /// What the task did on the GPU, for its stderr; `None` when it did
    /// not use it.
    pub fn summary(&self) -> Option<String> {
        let used: Vec<usize> = (0..self.devices.len())
            .filter(|i| !self.devices[*i].info.is_empty())
            .collect();
        // A worker killed while it was still opening the device counts as
        // used too.
        if used.is_empty() && !self.killed {
            return None;
        }
        let names: Vec<String> = match &self.task {
            Some(t) => used
                .iter()
                .chain(used.is_empty().then_some(&0))
                .filter_map(|i| t.info(*i))
                .map(|a| a.describe())
                .collect(),
            None => Vec::new(),
        };
        let mut s = format!(
            "WebGPU {}: {} dispatches",
            names.join(" and "),
            self.dispatches
        );
        if self.draws > 0 {
            s.push_str(&format!(", {} draws", self.draws));
        }
        s.push_str(&format!(
            ", {} ms waiting for the GPU",
            self.busy.as_millis()
        ));
        if self.killed {
            s.push_str("; the GPU work was stopped at the task's end");
        }
        Some(s)
    }

    /// The adapter that holds `handle`.
    fn device_of(&self, handle: i32) -> Call<usize> {
        let d = (u32::try_from(handle).map_err(|_| format!("no GPU object {handle}"))?
            >> HANDLE_BITS) as usize;
        if self.task.is_none() {
            return Err(NO_GPU.into());
        }
        if d >= self.devices.len() {
            return Err(Stop::Fail(format!("no GPU object {handle}")));
        }
        Ok(d)
    }

    /// Sends a request to the worker of adapter `d` (started and its
    /// device opened on first use) and returns its answer.
    fn send_to(&mut self, d: usize, req: Req) -> Call<Resp> {
        let task = self.task.as_ref().ok_or(NO_GPU)?;
        if d >= self.devices.len() {
            return Err("the task has no such GPU".into());
        }
        if let Some(e) = &self.devices[d].dead {
            return Err(Stop::Fail(e.clone()));
        }
        if self.devices[d].client.is_none() {
            let index = task.adapters[d];
            let (a, adapter) = task.gpu.get(index).ok_or("the GPU is gone")?;
            let key = AdapterKey::of(adapter).ok_or("the GPU is gone")?;
            let info = serde_json::json!({
                "name": a.name,
                "vendor": a.vendor.to_string(),
                "backend": a.backend.to_string(),
                "device_type": a.device_type.to_string(),
                "driver": a.driver,
                "fp32_gflops": a.fp32_gflops,
                "index": d,
                "count": task.adapters.len(),
            })
            .to_string();
            // The job's memory limit is shared out evenly among its GPUs.
            let limit_bytes = self.limit_bytes / task.adapters.len().max(1) as u64;
            let client = Client::start(&task.gpu.worker, adapter.clone());
            let mut client = match client {
                Ok(c) => c,
                Err(e) => return Err(Stop::Fail(self.die(d, format!("{e:#}")))),
            };
            let open = Req::Open {
                key,
                limit_bytes,
                info,
                tag: d as u32,
            };
            let resp = client.call(&open, self.deadline, &self.cancel);
            self.devices[d].client = Some(client);
            match self.answer(d, resp)? {
                Resp::Info(info) => self.devices[d].info = info,
                _ => return Err(Stop::Fail(self.die(d, ODD.into()))),
            }
        }
        let client = self.devices[d].client.as_mut().expect("started");
        let resp = client.call(&req, self.deadline, &self.cancel);
        self.answer(d, resp)
    }

    /// [`Self::send_to`] the current adapter.
    fn send(&mut self, req: Req) -> Call<Resp> {
        self.send_to(self.current, req)
    }

    /// Turns a worker's answer into what the call returns.
    fn answer(&mut self, d: usize, resp: std::result::Result<Resp, Gone>) -> Call<Resp> {
        match resp {
            Ok(Resp::Fail(msg)) => Err(Stop::Fail(msg)),
            Ok(Resp::Dead(msg)) => Err(Stop::Fail(self.die(d, msg))),
            Ok(r) => Ok(r),
            Err(Gone::Timeout) => {
                self.stop_all();
                Err(Stop::Abort(Trap::Interrupt.into()))
            }
            Err(Gone::Cancelled) => {
                self.stop_all();
                Err(Stop::Abort(wasmtime::Error::msg(
                    crate::executor::CANCELLED,
                )))
            }
            Err(Gone::Failed(msg)) => Err(Stop::Fail(self.die(d, msg))),
        }
    }

    /// The task ends: every worker goes.
    fn stop_all(&mut self) {
        self.killed = true;
        for dev in &mut self.devices {
            if let Some(mut c) = dev.client.take() {
                c.kill();
            }
        }
    }

    /// Adapter `d`'s device is gone for good: later calls on it fail with
    /// `msg`.
    fn die(&mut self, d: usize, msg: String) -> String {
        if let Some(dev) = self.devices.get_mut(d) {
            dev.client = None;
            dev.dead = Some(msg.clone());
        }
        msg
    }

    fn value_of(r: Resp) -> Call<i32> {
        match r {
            Resp::Value(v) => Ok(v),
            _ => Err(ODD.into()),
        }
    }

    /// A call that waits on the GPU, counted as time spent waiting.
    fn timed_to(&mut self, d: usize, req: Req) -> Call<Resp> {
        let start = Instant::now();
        let r = self.send_to(d, req);
        self.busy += start.elapsed();
        r
    }
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

fn offset(v: i64) -> Call<u64> {
    u64::try_from(v).map_err(|_| Stop::from("negative offset"))
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
            let msg = "the GPU host failed".to_string();
            for d in 0..st.devices.len() {
                st.die(d, msg.clone());
            }
            st.error = msg;
            Ok(-1)
        }
    }
}

/// Reads a UTF-8 string out of the module's memory.
fn text(data: &[u8], ptr: i32, len: i32, what: &str) -> Call<String> {
    std::str::from_utf8(&data[range(data, ptr, len)?])
        .map(str::to_string)
        .map_err(|_| Stop::Fail(format!("{what} is not UTF-8")))
}

/// Reads `n` little-endian `i32`s.
fn ints(data: &[u8], ptr: i32, n: usize) -> Call<Vec<i32>> {
    let r = range(data, ptr, (n * 4) as i32)?;
    Ok(data[r]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| i32::from_le_bytes(*c))
        .collect())
}

fn dim(v: i32) -> Call<u32> {
    u32::try_from(v).map_err(|_| Stop::from("negative size or count"))
}

/// A texture region: mip level, x, y, z, width, height, depth.
fn region(r: [i32; 7]) -> Call<[u32; 7]> {
    let mut out = [0u32; 7];
    for (o, v) in out.iter_mut().zip(r) {
        *o = dim(v)?;
    }
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
                    return Err(NO_GPU.into());
                }
                let cur = st.current;
                if st.devices[cur].info.is_empty() {
                    // Opens the device (the first call does).
                    st.send(Req::Wait)?;
                }
                let info = st.devices[cur].info.clone();
                let n = info.len().min(to_len(cap)?);
                let r = range(data, ptr, n as i32)?;
                data[r].copy_from_slice(&info.as_bytes()[..n]);
                Ok(info.len() as i32)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "adapters",
        move |mut c: Caller<'_, T>| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| Ok(st.devices.len() as i32))
        },
    )?;
    linker.func_wrap(
        m,
        "select",
        move |mut c: Caller<'_, T>, index: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                if st.task.is_none() {
                    return Err(NO_GPU.into());
                }
                let i = usize::try_from(index)
                    .ok()
                    .filter(|i| *i < st.devices.len())
                    .ok_or_else(|| {
                        format!("the task has {} GPUs, numbered from 0", st.devices.len())
                    })?;
                st.current = i;
                Ok(0)
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
                let src = text(data, src, src_len, "the shader")?;
                let entry = text(data, entry, entry_len, "the entry point")?;
                GpuState::value_of(st.send(Req::Shader { src, entry })?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "buffer",
        move |mut c: Caller<'_, T>, size: i64| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                let size = u64::try_from(size).map_err(|_| "negative size")?;
                GpuState::value_of(st.send(Req::Buffer { size })?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "write",
        move |mut c: Caller<'_, T>,
              buf: i32,
              off: i64,
              src: i32,
              len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let offset = offset(off)?;
                let r = range(data, src, len)?;
                let d = st.device_of(buf)?;
                GpuState::value_of(st.send_to(
                    d,
                    Req::Write {
                        buf,
                        offset,
                        data: data[r].to_vec(),
                    },
                )?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "read",
        move |mut c: Caller<'_, T>,
              buf: i32,
              off: i64,
              dst: i32,
              len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let offset = offset(off)?;
                let r = range(data, dst, len)?;
                let len = r.len() as u64;
                let d = st.device_of(buf)?;
                match st.timed_to(d, Req::Read { buf, offset, len })? {
                    Resp::Data(v) if v.len() == r.len() => {
                        data[r].copy_from_slice(&v);
                        Ok(0)
                    }
                    _ => Err(ODD.into()),
                }
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
                let bufs = ints(data, bufs, count)?;
                let (x, y, z) = (dim(x)?, dim(y)?, dim(z)?);
                let d = st.device_of(shader)?;
                // A dispatch can wait for older ones (the worker bounds
                // what is queued), so it counts as waiting too.
                let v = GpuState::value_of(st.timed_to(
                    d,
                    Req::Dispatch {
                        shader,
                        bufs,
                        x,
                        y,
                        z,
                    },
                )?)?;
                st.dispatches += 1;
                Ok(v)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "wait",
        move |mut c: Caller<'_, T>| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                if st.task.is_none() {
                    return Err(NO_GPU.into());
                }
                let open: Vec<usize> = (0..st.devices.len())
                    .filter(|d| st.devices[*d].client.is_some())
                    .collect();
                if open.is_empty() {
                    // Opens the current device, as it always did.
                    return GpuState::value_of(st.timed_to(st.current, Req::Wait)?);
                }
                for d in open {
                    GpuState::value_of(st.timed_to(d, Req::Wait)?)?;
                }
                Ok(0)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "release",
        move |mut c: Caller<'_, T>, handle: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                let d = st.device_of(handle)?;
                GpuState::value_of(st.send_to(d, Req::Release { handle })?)
            })
        },
    )?;
    // Drawing (1.16).
    linker.func_wrap(
        m,
        "texture",
        move |mut c: Caller<'_, T>, ptr: i32, len: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let json = text(data, ptr, len, "the texture descriptor")?;
                GpuState::value_of(st.send(Req::Texture { json })?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "sampler",
        move |mut c: Caller<'_, T>, ptr: i32, len: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let json = text(data, ptr, len, "the sampler descriptor")?;
                GpuState::value_of(st.send(Req::Sampler { json })?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "render_shader",
        move |mut c: Caller<'_, T>,
              src: i32,
              src_len: i32,
              json: i32,
              json_len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let src = text(data, src, src_len, "the shader")?;
                let json = text(data, json, json_len, "the render shader descriptor")?;
                GpuState::value_of(st.send(Req::RenderShader { src, json })?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "write_texture",
        move |mut c: Caller<'_, T>,
              tex: i32,
              mip: i32,
              x: i32,
              y: i32,
              z: i32,
              w: i32,
              h: i32,
              depth: i32,
              src: i32,
              len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let region = region([mip, x, y, z, w, h, depth])?;
                let r = range(data, src, len)?;
                let d = st.device_of(tex)?;
                GpuState::value_of(st.send_to(
                    d,
                    Req::WriteTexture {
                        tex,
                        region,
                        data: data[r].to_vec(),
                    },
                )?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "read_texture",
        move |mut c: Caller<'_, T>,
              tex: i32,
              mip: i32,
              x: i32,
              y: i32,
              z: i32,
              w: i32,
              h: i32,
              depth: i32,
              dst: i32,
              len: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let region = region([mip, x, y, z, w, h, depth])?;
                let r = range(data, dst, len)?;
                let d = st.device_of(tex)?;
                match st.timed_to(d, Req::ReadTexture { tex, region })? {
                    Resp::Data(v) if v.len() == r.len() => {
                        data[r].copy_from_slice(&v);
                        Ok(0)
                    }
                    Resp::Data(v) => Err(Stop::Fail(format!(
                        "the region takes {} bytes, not {}",
                        v.len(),
                        r.len()
                    ))),
                    _ => Err(ODD.into()),
                }
            })
        },
    )?;
    linker.func_wrap(
        m,
        "bind_group",
        move |mut c: Caller<'_, T>,
              shader: i32,
              group: i32,
              entries: i32,
              n: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let count = to_len(n)?;
                if count > 64 {
                    return Err("at most 64 entries per bind group".into());
                }
                let raw = ints(data, entries, count * 2)?;
                let entries: Vec<(u32, i32)> = raw
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|[b, h]| Ok((dim(*b)?, *h)))
                    .collect::<Call<_>>()?;
                let group = dim(group)?;
                let d = st.device_of(shader)?;
                GpuState::value_of(st.send_to(
                    d,
                    Req::BindGroup {
                        shader,
                        group,
                        entries,
                    },
                )?)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "dispatch_groups",
        move |mut c: Caller<'_, T>,
              shader: i32,
              groups: i32,
              n: i32,
              x: i32,
              y: i32,
              z: i32|
              -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let count = to_len(n)?;
                if count > 8 {
                    return Err("at most 8 bind groups".into());
                }
                let groups = ints(data, groups, count)?;
                let (x, y, z) = (dim(x)?, dim(y)?, dim(z)?);
                let d = st.device_of(shader)?;
                let v = GpuState::value_of(st.timed_to(
                    d,
                    Req::DispatchGroups {
                        shader,
                        groups,
                        x,
                        y,
                        z,
                    },
                )?)?;
                st.dispatches += 1;
                Ok(v)
            })
        },
    )?;
    linker.func_wrap(
        m,
        "render",
        move |mut c: Caller<'_, T>, ptr: i32, len: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |data, st| {
                let json = text(data, ptr, len, "the render pass")?;
                // The pass runs on the GPU its render shader lives on.
                let pipeline = serde_json::from_str::<serde_json::Value>(&json)
                    .ok()
                    .and_then(|v| v.get("pipeline")?.as_i64())
                    .and_then(|v| i32::try_from(v).ok())
                    .ok_or("the render pass names no pipeline")?;
                let d = st.device_of(pipeline)?;
                let draws = GpuState::value_of(st.timed_to(d, Req::Render { json })?)?;
                st.draws += draws as u64;
                Ok(0)
            })
        },
    )?;
    Ok(())
}
