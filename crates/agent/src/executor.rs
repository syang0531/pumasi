//! The WASM executor: runs one task inside a fresh Wasmtime store.
//!
//! Every task gets:
//! - no filesystem (no preopened directories) and no network,
//! - only the args, env and stdin from the job spec,
//! - a memory cap (store limiter), a fuel cap (instruction metering) and a
//!   wall-clock cap (epoch interruption),
//! - NaN canonicalization, so float results are bit-identical across CPUs.
//!   That makes redundant execution usable for result verification later.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use pumasi_proto::{
    ModuleHash,
    job::{Limits, MAX_OUTPUT_BYTES, TaskInput},
    msg::Outcome,
};
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap};
use wasmtime_wasi::{
    I32Exit, WasiCtxBuilder,
    p1::{self, WasiP1Ctx},
    p2::pipe::{MemoryInputPipe, MemoryOutputPipe},
};

/// Granularity of the wall-clock limit.
const EPOCH_TICK: Duration = Duration::from_millis(10);
/// Compiled modules kept in memory.
const MODULE_CACHE_CAP: usize = 256;

/// What a finished run produced.
#[derive(Debug, Clone)]
pub struct RunResult {
    pub outcome: Outcome,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub fuel_used: u64,
    pub wall_ms: u64,
}

struct StoreState {
    wasi: WasiP1Ctx,
    limits: StoreLimits,
}

/// Shared, thread-safe executor. Cheap to clone.
#[derive(Clone)]
pub struct WasmExecutor {
    engine: Engine,
    linker: Arc<Linker<StoreState>>,
    modules: Arc<Mutex<HashMap<ModuleHash, Module>>>,
    _ticker: Arc<Ticker>,
}

/// Stops the epoch thread when the last executor clone is dropped.
struct Ticker(Arc<AtomicBool>);

impl Drop for Ticker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl WasmExecutor {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        config.cranelift_nan_canonicalization(true);
        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        p1::add_to_linker_sync(&mut linker, |s: &mut StoreState| &mut s.wasi)?;

        let stop = Arc::new(AtomicBool::new(false));
        // The epoch ticker drives every store's wall-clock deadline.
        {
            let engine = engine.clone();
            let stop = stop.clone();
            std::thread::Builder::new()
                .name("pumasi-epoch".into())
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(EPOCH_TICK);
                        engine.increment_epoch();
                    }
                })?;
        }

        Ok(Self {
            engine,
            linker: Arc::new(linker),
            modules: Default::default(),
            _ticker: Arc::new(Ticker(stop)),
        })
    }

    /// True if the module is already compiled and cached.
    pub fn has_module(&self, hash: &ModuleHash) -> bool {
        self.modules.lock().unwrap().contains_key(hash)
    }

    /// Compiles and caches a module. Verifies the hash first.
    pub fn load_module(&self, hash: ModuleHash, bytes: &[u8]) -> Result<()> {
        anyhow::ensure!(
            pumasi_proto::module_hash(bytes) == hash,
            "module hash mismatch"
        );
        if self.has_module(&hash) {
            return Ok(());
        }
        let module =
            Module::new(&self.engine, bytes).map_err(|e| anyhow::anyhow!("invalid module: {e}"))?;
        let mut cache = self.modules.lock().unwrap();
        if cache.len() >= MODULE_CACHE_CAP {
            // Simple bound; eviction order does not matter much here.
            if let Some(k) = cache.keys().next().copied() {
                cache.remove(&k);
            }
        }
        cache.insert(hash, module);
        Ok(())
    }

    /// Runs a cached module to completion. Blocking: call from a blocking
    /// thread. `cancel` can be set from another thread to abort.
    pub fn run(
        &self,
        hash: &ModuleHash,
        program: &str,
        input: &TaskInput,
        limits: &Limits,
        cancel: Arc<AtomicBool>,
    ) -> RunResult {
        let start = Instant::now();
        let module = match self.modules.lock().unwrap().get(hash).cloned() {
            Some(m) => m,
            None => return RunResult::error("module not loaded", start),
        };

        let stdout = MemoryOutputPipe::new(MAX_OUTPUT_BYTES);
        let stderr = MemoryOutputPipe::new(MAX_OUTPUT_BYTES);
        let mut args = vec![program.to_string()];
        args.extend(input.args.iter().cloned());
        let env: Vec<(String, String)> = input
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let wasi = WasiCtxBuilder::new()
            .args(&args)
            .envs(&env)
            .stdin(MemoryInputPipe::new(input.stdin.clone()))
            .stdout(stdout.clone())
            .stderr(stderr.clone())
            .build_p1();

        let memory = usize::try_from(limits.memory_bytes).unwrap_or(usize::MAX);
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(memory)
            .instances(4)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &self.engine,
            StoreState {
                wasi,
                limits: store_limits,
            },
        );
        store.limiter(|s| &mut s.limits);

        let fuel_budget = limits.fuel.unwrap_or(u64::MAX);
        if let Err(e) = store.set_fuel(fuel_budget) {
            return RunResult::error(&format!("set fuel: {e}"), start);
        }
        // Every epoch tick, check for cancellation and the wall-clock deadline.
        let deadline = Instant::now() + Duration::from_millis(limits.timeout_ms);
        store.epoch_deadline_callback(move |_| {
            if cancel.load(Ordering::Relaxed) {
                return Err(wasmtime::Error::msg(CANCELLED));
            }
            if Instant::now() >= deadline {
                return Err(Trap::Interrupt.into());
            }
            Ok(wasmtime::UpdateDeadline::Continue(1))
        });
        store.set_epoch_deadline(1);

        let result = (|| -> wasmtime::Result<()> {
            let instance = self.linker.instantiate(&mut store, &module)?;
            let start = instance.get_typed_func::<(), ()>(&mut store, "_start")?;
            start.call(&mut store, ())
        })();

        let fuel_used = fuel_budget.saturating_sub(store.get_fuel().unwrap_or(0));
        drop(store);

        let outcome = match result {
            Ok(()) => Outcome::Exited(0),
            Err(e) => classify(e),
        };
        RunResult {
            outcome,
            stdout: stdout.contents().to_vec(),
            stderr: stderr.contents().to_vec(),
            fuel_used,
            wall_ms: start.elapsed().as_millis() as u64,
        }
    }
}

const CANCELLED: &str = "pumasi: task cancelled";

fn classify(e: wasmtime::Error) -> Outcome {
    if let Some(exit) = e.downcast_ref::<I32Exit>() {
        return Outcome::Exited(exit.0);
    }
    if let Some(trap) = e.downcast_ref::<Trap>() {
        return match trap {
            Trap::OutOfFuel => Outcome::OutOfFuel,
            Trap::Interrupt => Outcome::Timeout,
            other => Outcome::Trap(other.to_string()),
        };
    }
    let msg = format!("{e:#}");
    if msg.contains(CANCELLED) {
        return Outcome::Cancelled;
    }
    if msg.contains("_start")
        || msg.contains("unknown import")
        || msg.contains("incompatible import")
    {
        return Outcome::Error(msg);
    }
    Outcome::Trap(msg)
}

impl RunResult {
    fn error(msg: &str, start: Instant) -> Self {
        Self {
            outcome: Outcome::Error(msg.to_string()),
            stdout: Vec::new(),
            stderr: Vec::new(),
            fuel_used: 0,
            wall_ms: start.elapsed().as_millis() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec_wat(wat: &str, input: TaskInput, limits: Limits) -> RunResult {
        let bytes = wat::parse_str(wat).unwrap();
        let hash = pumasi_proto::module_hash(&bytes);
        let ex = WasmExecutor::new().unwrap();
        ex.load_module(hash, &bytes).unwrap();
        ex.run(&hash, "test", &input, &limits, Default::default())
    }

    /// Writes "hi\n" to stdout via fd_write, then exits with the code in
    /// the first argument count (argc - 1).
    const HELLO: &str = r#"
    (module
      (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
      (import "wasi_snapshot_preview1" "args_sizes_get" (func $args (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (data (i32.const 16) "hi\n")
      (func (export "_start")
        (i32.store (i32.const 0) (i32.const 16))
        (i32.store (i32.const 4) (i32.const 3))
        (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8)))
        (drop (call $args (i32.const 100) (i32.const 104)))
        (call $exit (i32.sub (i32.load (i32.const 100)) (i32.const 1)))))
    "#;

    const SPIN: &str = r#"(module (func (export "_start") (loop br 0)))"#;

    #[test]
    fn captures_stdout_and_exit_code() {
        let r = exec_wat(HELLO, TaskInput::default(), Limits::default());
        assert_eq!(r.outcome, Outcome::Exited(0));
        assert_eq!(r.stdout, b"hi\n");
        assert!(r.fuel_used > 0);

        let input = TaskInput {
            args: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        let r = exec_wat(HELLO, input, Limits::default());
        assert_eq!(r.outcome, Outcome::Exited(2));
    }

    #[test]
    fn fuel_limit() {
        let limits = Limits {
            fuel: Some(100_000),
            ..Default::default()
        };
        let r = exec_wat(SPIN, TaskInput::default(), limits);
        assert_eq!(r.outcome, Outcome::OutOfFuel);
        assert_eq!(r.fuel_used, 100_000);
    }

    #[test]
    fn timeout() {
        let limits = Limits {
            timeout_ms: 200,
            ..Default::default()
        };
        let r = exec_wat(SPIN, TaskInput::default(), limits);
        assert_eq!(r.outcome, Outcome::Timeout);
        assert!(
            r.wall_ms >= 150 && r.wall_ms < 5000,
            "wall_ms = {}",
            r.wall_ms
        );
    }

    #[test]
    fn memory_limit() {
        // Asks for 100 pages (6.4 MiB) up front with a 1 MiB cap.
        let wat = r#"(module (memory 100) (func (export "_start")))"#;
        let limits = Limits {
            memory_bytes: 1024 * 1024,
            ..Default::default()
        };
        let r = exec_wat(wat, TaskInput::default(), limits);
        assert!(matches!(r.outcome, Outcome::Trap(_)), "{:?}", r.outcome);
    }

    #[test]
    fn cancel() {
        let bytes = wat::parse_str(SPIN).unwrap();
        let hash = pumasi_proto::module_hash(&bytes);
        let ex = WasmExecutor::new().unwrap();
        ex.load_module(hash, &bytes).unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let c2 = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            c2.store(true, Ordering::Relaxed);
        });
        let r = ex.run(
            &hash,
            "t",
            &TaskInput::default(),
            &Limits::default(),
            cancel,
        );
        assert_eq!(r.outcome, Outcome::Cancelled);
    }

    #[test]
    fn no_filesystem_access() {
        // path_open on fd 3 (the first preopen) must fail: there is none.
        let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_prestat_get" (func $prestat (param i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
          (memory (export "memory") 1)
          (func (export "_start")
            (call $exit (call $prestat (i32.const 3) (i32.const 0)))))
        "#;
        let r = exec_wat(wat, TaskInput::default(), Limits::default());
        // errno 8 = EBADF: no preopened directory exists.
        assert_eq!(r.outcome, Outcome::Exited(8));
    }

    #[test]
    fn rejects_bad_hash() {
        let ex = WasmExecutor::new().unwrap();
        assert!(ex.load_module([0; 32], b"\0asm\x01\0\0\0").is_err());
    }
}
