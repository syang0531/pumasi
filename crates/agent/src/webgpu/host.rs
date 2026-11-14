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
    worker::{AdapterKey, Client, Gone, Req, Resp},
};

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
/// module made its first call, the worker holding its device.
pub struct GpuState {
    task: Option<WebGpuTask>,
    client: Option<Client>,
    /// The adapter as JSON, once the device is open.
    info: String,
    /// Why the device cannot be used any more.
    dead: Option<String>,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    limit_bytes: u64,
    error: String,
    /// Time spent waiting for the GPU.
    pub busy: Duration,
    /// Dispatches run.
    pub dispatches: u64,
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

impl GpuState {
    pub fn new(
        task: Option<WebGpuTask>,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        limit_bytes: u64,
    ) -> Self {
        Self {
            task,
            client: None,
            info: String::new(),
            dead: None,
            deadline,
            cancel,
            limit_bytes,
            error: String::new(),
            busy: Duration::ZERO,
            dispatches: 0,
            killed: false,
        }
    }

    /// What the task did on the GPU, for its stderr; `None` when it did
    /// not use it.
    pub fn summary(&self) -> Option<String> {
        // A worker killed while it was still opening the device counts as
        // used too.
        if self.info.is_empty() && !self.killed {
            return None;
        }
        let name = self
            .task
            .as_ref()
            .and_then(WebGpuTask::info)
            .map(|a| a.describe())
            .unwrap_or_default();
        let mut s = format!(
            "WebGPU {name}: {} dispatches, {} ms waiting for the GPU",
            self.dispatches,
            self.busy.as_millis()
        );
        if self.killed {
            s.push_str("; the GPU work was stopped at the task's end");
        }
        Some(s)
    }

    /// Sends a request to the task's worker (started and its device opened
    /// on first use) and returns its answer.
    fn send(&mut self, req: Req) -> Call<Resp> {
        if let Some(d) = &self.dead {
            return Err(Stop::Fail(d.clone()));
        }
        if self.client.is_none() {
            let task = self.task.as_ref().ok_or(NO_GPU)?;
            let (a, adapter) = task.gpu.get(task.adapter).ok_or("the GPU is gone")?;
            let key = AdapterKey::of(adapter).ok_or("the GPU is gone")?;
            let info = serde_json::json!({
                "name": a.name,
                "vendor": a.vendor.to_string(),
                "backend": a.backend.to_string(),
                "device_type": a.device_type.to_string(),
                "driver": a.driver,
                "fp32_gflops": a.fp32_gflops,
            })
            .to_string();
            let mut client = Client::start(&task.gpu.worker, adapter.clone())
                .map_err(|e| self.die(format!("{e:#}")))?;
            let open = Req::Open {
                key,
                limit_bytes: self.limit_bytes,
                info,
            };
            let resp = client.call(&open, self.deadline, &self.cancel);
            self.client = Some(client);
            match self.answer(resp)? {
                Resp::Info(info) => self.info = info,
                _ => return Err(Stop::Fail(self.die("the GPU worker answered oddly".into()))),
            }
        }
        let client = self.client.as_mut().expect("started");
        let resp = client.call(&req, self.deadline, &self.cancel);
        self.answer(resp)
    }

    /// Turns a worker's answer into what the call returns.
    fn answer(&mut self, resp: std::result::Result<Resp, Gone>) -> Call<Resp> {
        match resp {
            Ok(Resp::Fail(msg)) => Err(Stop::Fail(msg)),
            Ok(Resp::Dead(msg)) => Err(Stop::Fail(self.die(msg))),
            Ok(r) => Ok(r),
            Err(Gone::Timeout) => {
                self.killed = true;
                self.client = None;
                Err(Stop::Abort(Trap::Interrupt.into()))
            }
            Err(Gone::Cancelled) => {
                self.killed = true;
                self.client = None;
                Err(Stop::Abort(wasmtime::Error::msg(
                    crate::executor::CANCELLED,
                )))
            }
            Err(Gone::Failed(msg)) => Err(Stop::Fail(self.die(msg))),
        }
    }

    /// The device is gone for good: later calls fail with `msg`.
    fn die(&mut self, msg: String) -> String {
        self.client = None;
        self.dead = Some(msg.clone());
        msg
    }

    fn value(&mut self, req: Req) -> Call<i32> {
        match self.send(req)? {
            Resp::Value(v) => Ok(v),
            _ => Err("the GPU worker answered oddly".into()),
        }
    }

    /// A call that waits on the GPU, counted as time spent waiting.
    fn timed(&mut self, req: Req) -> Call<Resp> {
        let start = Instant::now();
        let r = self.send(req);
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
            st.error = st.die("the GPU host failed".into());
            Ok(-1)
        }
    }
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
                if st.info.is_empty() {
                    // Opens the device (the first call does).
                    st.send(Req::Wait)?;
                }
                let info = st.info.clone();
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
                st.value(Req::Shader { src, entry })
            })
        },
    )?;
    linker.func_wrap(
        m,
        "buffer",
        move |mut c: Caller<'_, T>, size: i64| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| {
                let size = u64::try_from(size).map_err(|_| "negative size")?;
                st.value(Req::Buffer { size })
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
                st.value(Req::Write {
                    buf,
                    offset,
                    data: data[r].to_vec(),
                })
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
                match st.timed(Req::Read { buf, offset, len })? {
                    Resp::Data(d) if d.len() == r.len() => {
                        data[r].copy_from_slice(&d);
                        Ok(0)
                    }
                    _ => Err("the GPU worker answered oddly".into()),
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
                let r = range(data, bufs, (count * 4) as i32)?;
                let bufs: Vec<i32> = data[r]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| i32::from_le_bytes(*c))
                    .collect();
                let dim =
                    |v: i32| u32::try_from(v).map_err(|_| Stop::from("negative workgroup count"));
                let (x, y, z) = (dim(x)?, dim(y)?, dim(z)?);
                // A dispatch can wait for older ones (the worker bounds
                // what is queued), so it counts as waiting too.
                match st.timed(Req::Dispatch {
                    shader,
                    bufs,
                    x,
                    y,
                    z,
                })? {
                    Resp::Value(v) => {
                        st.dispatches += 1;
                        Ok(v)
                    }
                    _ => Err("the GPU worker answered oddly".into()),
                }
            })
        },
    )?;
    linker.func_wrap(
        m,
        "wait",
        move |mut c: Caller<'_, T>| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| match st.timed(Req::Wait)? {
                Resp::Value(v) => Ok(v),
                _ => Err("the GPU worker answered oddly".into()),
            })
        },
    )?;
    linker.func_wrap(
        m,
        "release",
        move |mut c: Caller<'_, T>, handle: i32| -> wasmtime::Result<i32> {
            with_state(&mut c, get, |_, st| st.value(Req::Release { handle }))
        },
    )?;
    Ok(())
}
