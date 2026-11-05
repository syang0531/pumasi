//! The WASM executor: runs one task inside a fresh Wasmtime store.
//!
//! Every task gets:
//! - no network, and no filesystem except two directories: `/input`
//!   (read-only, the job's files) and `/output` (collected afterwards),
//! - only the args, env and stdin from the job spec,
//! - a memory cap (store limiter), a fuel cap (instruction metering) and a
//!   wall-clock cap (epoch interruption),
//! - NaN canonicalization, so float results are bit-identical across CPUs.
//! - optionally a deterministic mode (virtual clock, seeded random numbers)
//!   so any honest node produces the same result and a re-run on another
//!   node can check it.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use pumasi_proto::{
    ModuleHash,
    job::{
        FileEntry, INPUT_DIR, Limits, MAX_OUTPUT_BYTES, MAX_OUTPUT_FILES_BYTES, OUTPUT_DIR,
        TaskInput, read_files, write_files,
    },
    msg::Outcome,
    store::OutputMode,
};

use crate::tempdir::TempDir;
use rand010::SeedableRng;
use wasmtime::{
    Config, Engine, Linker, Module, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder, Trap,
};
use wasmtime_wasi::{
    FsPerms, HostMonotonicClock, HostWallClock, I32Exit, WasiCtxBuilder,
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
    pub files: Vec<FileEntry>,
    pub fuel_used: u64,
    pub wall_ms: u64,
    /// Highest linear memory reached (WASM only).
    pub peak_memory_bytes: Option<u64>,
    /// The output files, left on disk when the task's output may go to the
    /// file store ([`TaskIo::output`] other than inline); `files` is then
    /// empty.
    pub out_dir: Option<Arc<TempDir>>,
}

/// What a task gets beyond its inline files (protocol 8).
#[derive(Debug, Clone, Default)]
pub struct TaskIo {
    /// Files from the store, already placed in a directory, for `/input`
    /// (the job's inline files are added to it).
    pub staged: Option<PathBuf>,
    /// Where `/output` goes.
    pub output: OutputMode,
    /// Where to keep output files on disk (the file store's disk).
    pub scratch: Option<PathBuf>,
    /// A private image taken from the file store.
    pub image: Option<LocalImage>,
}

impl TaskIo {
    /// Whether output files stay on disk instead of going in the report.
    pub fn keeps_output(&self) -> bool {
        self.output != OutputMode::Inline
    }

    /// A directory for output files kept on disk.
    pub fn out_dir(&self, tag: &str) -> Result<TempDir> {
        match &self.scratch {
            Some(base) => TempDir::new_in(base, tag),
            None => TempDir::new(tag),
        }
    }
}

/// An image exported from the file store: `image.json`, `config.json` and
/// the layers (`layer-000`, ...) in `dir`.
#[derive(Debug, Clone)]
pub struct LocalImage {
    pub digest: String,
    pub dir: PathBuf,
}

struct StoreState {
    wasi: WasiP1Ctx,
    limits: PeakLimiter,
}

/// Enforces the store limits and remembers the largest memory granted.
struct PeakLimiter {
    inner: StoreLimits,
    peak: usize,
}

impl ResourceLimiter for PeakLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let ok = self.inner.memory_growing(current, desired, maximum)?;
        if ok {
            self.peak = self.peak.max(desired);
        }
        Ok(ok)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.inner.memory_grow_failed(error)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.inner.table_growing(current, desired, maximum)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.inner.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.inner.instances()
    }

    fn tables(&self) -> usize {
        self.inner.tables()
    }

    fn memories(&self) -> usize {
        self.inner.memories()
    }
}

/// Start of the virtual wall clock in deterministic mode (2000-01-01 UTC).
const VIRTUAL_EPOCH: Duration = Duration::from_secs(946_684_800);
/// How far the virtual clocks move on every read.
const VIRTUAL_TICK: Duration = Duration::from_millis(1);

/// A clock that advances by a fixed step each time it is read, so a task
/// sees the same times on every machine and a "wait until N seconds have
/// passed" loop still ends.
#[derive(Clone, Default)]
struct VirtualClock(Arc<std::sync::atomic::AtomicU64>);

impl VirtualClock {
    fn tick(&self) -> Duration {
        let n = self.0.fetch_add(1, Ordering::Relaxed) + 1;
        VIRTUAL_TICK * n as u32
    }
}

impl HostWallClock for VirtualClock {
    fn resolution(&self) -> Duration {
        VIRTUAL_TICK
    }
    fn now(&self) -> Duration {
        VIRTUAL_EPOCH + self.tick()
    }
}

impl HostMonotonicClock for VirtualClock {
    fn resolution(&self) -> u64 {
        VIRTUAL_TICK.as_nanos() as u64
    }
    fn now(&self) -> u64 {
        self.tick().as_nanos() as u64
    }
}

/// Derives an independent 32-byte seed for one purpose from a task seed.
fn derive_seed(seed: &[u8; 32], purpose: &str) -> [u8; 32] {
    blake3::keyed_hash(seed, purpose.as_bytes()).into()
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
    /// thread. `seed` turns on deterministic mode. `cancel` can be set from
    /// another thread to abort.
    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &self,
        hash: &ModuleHash,
        program: &str,
        input: &TaskInput,
        shared_files: &[FileEntry],
        limits: &Limits,
        seed: Option<[u8; 32]>,
        cancel: Arc<AtomicBool>,
    ) -> RunResult {
        self.run_io(
            hash,
            program,
            input,
            shared_files,
            limits,
            seed,
            cancel,
            &TaskIo::default(),
        )
    }

    /// [`Self::run`] with mounted files and outputs that may go to the file
    /// store.
    #[allow(clippy::too_many_arguments)]
    pub fn run_io(
        &self,
        hash: &ModuleHash,
        program: &str,
        input: &TaskInput,
        shared_files: &[FileEntry],
        limits: &Limits,
        seed: Option<[u8; 32]>,
        cancel: Arc<AtomicBool>,
        io: &TaskIo,
    ) -> RunResult {
        let start = Instant::now();
        let module = match self.modules.lock().unwrap().get(hash).cloned() {
            Some(m) => m,
            None => return RunResult::error("module not loaded", start),
        };

        // Scratch space: /input holds the job's files, /output is collected.
        let scratch = match TempDir::new("wasm") {
            Ok(d) => d,
            Err(e) => return RunResult::error(&format!("{e:#}"), start),
        };
        let in_dir = match &io.staged {
            Some(d) => d.clone(),
            None => scratch.path().join("input"),
        };
        let kept_out = if io.keeps_output() {
            match io.out_dir("out") {
                Ok(d) => Some(Arc::new(d)),
                Err(e) => return RunResult::error(&format!("{e:#}"), start),
            }
        } else {
            None
        };
        let out_dir = match &kept_out {
            Some(d) => d.path().to_path_buf(),
            None => scratch.path().join("output"),
        };
        let prepared = std::fs::create_dir_all(&in_dir)
            .and_then(|_| std::fs::create_dir_all(&out_dir))
            .map_err(anyhow::Error::from)
            .and_then(|_| write_files(&in_dir, shared_files))
            .and_then(|_| write_files(&in_dir, &input.files));
        if let Err(e) = prepared {
            return RunResult::error(&format!("prepare input files: {e:#}"), start);
        }

        let stdout = MemoryOutputPipe::new(MAX_OUTPUT_BYTES);
        let stderr = MemoryOutputPipe::new(MAX_OUTPUT_BYTES);
        let mut args = vec![program.to_string()];
        args.extend(input.args.iter().cloned());
        let env: Vec<(String, String)> = input
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut builder = WasiCtxBuilder::new();
        builder
            .args(&args)
            .envs(&env)
            .stdin(MemoryInputPipe::new(input.stdin.clone()))
            .stdout(stdout.clone())
            .stderr(stderr.clone());
        if let Some(seed) = &seed {
            let clock = VirtualClock::default();
            builder
                .wall_clock(clock.clone())
                .monotonic_clock(clock)
                .secure_random(rand010::rngs::StdRng::from_seed(derive_seed(
                    seed, "secure",
                )))
                .insecure_random(rand010::rngs::StdRng::from_seed(derive_seed(
                    seed, "insecure",
                )))
                .insecure_random_seed(u128::from_le_bytes(
                    derive_seed(seed, "insecure-seed")[..16]
                        .try_into()
                        .expect("16 bytes"),
                ));
        }
        let preopened = builder
            .preopened_dir(&in_dir, INPUT_DIR, FsPerms::ReadOnly)
            .and_then(|b| b.preopened_dir(&out_dir, OUTPUT_DIR, FsPerms::ReadWrite))
            .map(|_| ());
        if let Err(e) = preopened {
            return RunResult::error(&format!("preopen directories: {e}"), start);
        }
        let wasi = builder.build_p1();

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
                limits: PeakLimiter {
                    inner: store_limits,
                    peak: 0,
                },
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
        let peak = store.data().limits.peak as u64;
        drop(store);

        let outcome = match result {
            Ok(()) => Outcome::Exited(0),
            Err(e) => classify(e),
        };
        let mut stderr = stderr.contents().to_vec();
        let files = match kept_out {
            Some(_) => Vec::new(),
            None => collect_output(&out_dir, &mut stderr),
        };
        RunResult {
            outcome,
            stdout: stdout.contents().to_vec(),
            stderr,
            files,
            fuel_used,
            wall_ms: start.elapsed().as_millis() as u64,
            peak_memory_bytes: Some(peak),
            out_dir: kept_out,
        }
    }
}

const CANCELLED: &str = "pumasi: task cancelled";

/// Reads `/output` back, noting on stderr if the size cap cut it short.
pub(crate) fn collect_output(out_dir: &std::path::Path, stderr: &mut Vec<u8>) -> Vec<FileEntry> {
    match read_files(out_dir, MAX_OUTPUT_FILES_BYTES) {
        Ok((files, truncated)) => {
            if truncated {
                stderr.extend_from_slice(
                    format!(
                        "\n[pumasi] output files exceeded {} MiB; some were dropped\n",
                        MAX_OUTPUT_FILES_BYTES / 1024 / 1024
                    )
                    .as_bytes(),
                );
            }
            files
        }
        Err(e) => {
            stderr.extend_from_slice(
                format!("\n[pumasi] could not read output files: {e:#}\n").as_bytes(),
            );
            Vec::new()
        }
    }
}

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
            files: Vec::new(),
            fuel_used: 0,
            wall_ms: start.elapsed().as_millis() as u64,
            peak_memory_bytes: None,
            out_dir: None,
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
        ex.run(
            &hash,
            "test",
            &input,
            &[],
            &limits,
            None,
            Default::default(),
        )
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
            &[],
            &Limits::default(),
            None,
            cancel,
        );
        assert_eq!(r.outcome, Outcome::Cancelled);
    }

    /// Reads /input/in.txt and copies it to /output/out.txt. Checks that
    /// no other directory exists, that `..` cannot escape /input, and that
    /// /input is read-only. Exits with a distinct code per failed check.
    const FILES_WAT: &str = r#"
    (module
      (import "wasi_snapshot_preview1" "fd_prestat_get" (func $prestat (param i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "path_open" (func $open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "fd_read" (func $read (param i32 i32 i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
      (memory (export "memory") 1)
      (data (i32.const 100) "in.txt")
      (data (i32.const 120) "out.txt")
      (data (i32.const 140) "../x")
      (data (i32.const 160) "new.txt")
      (func (export "_start")
        (if (i32.ne (call $prestat (i32.const 5) (i32.const 0)) (i32.const 8)) (then (call $exit (i32.const 10))))
        (if (i32.eqz (call $open (i32.const 3) (i32.const 0) (i32.const 140) (i32.const 4) (i32.const 0) (i64.const 536870911) (i64.const 536870911) (i32.const 0) (i32.const 200)))
          (then (call $exit (i32.const 11))))
        (if (i32.eqz (call $open (i32.const 3) (i32.const 0) (i32.const 160) (i32.const 7) (i32.const 1) (i64.const 536870911) (i64.const 536870911) (i32.const 0) (i32.const 200)))
          (then (call $exit (i32.const 12))))
        (if (call $open (i32.const 3) (i32.const 0) (i32.const 100) (i32.const 6) (i32.const 0) (i64.const 2) (i64.const 0) (i32.const 0) (i32.const 200))
          (then (call $exit (i32.const 13))))
        (i32.store (i32.const 300) (i32.const 1000))
        (i32.store (i32.const 304) (i32.const 100))
        (drop (call $read (i32.load (i32.const 200)) (i32.const 300) (i32.const 1) (i32.const 308)))
        (if (call $open (i32.const 4) (i32.const 0) (i32.const 120) (i32.const 7) (i32.const 1) (i64.const 536870911) (i64.const 536870911) (i32.const 0) (i32.const 200))
          (then (call $exit (i32.const 14))))
        (i32.store (i32.const 400) (i32.const 1000))
        (i32.store (i32.const 404) (i32.load (i32.const 308)))
        (drop (call $write (i32.load (i32.const 200)) (i32.const 400) (i32.const 1) (i32.const 408)))
        (call $exit (i32.const 0))))
    "#;

    #[test]
    fn input_and_output_files() {
        let bytes = wat::parse_str(FILES_WAT).unwrap();
        let hash = pumasi_proto::module_hash(&bytes);
        let ex = WasmExecutor::new().unwrap();
        ex.load_module(hash, &bytes).unwrap();
        let shared = vec![FileEntry {
            path: "in.txt".into(),
            data: b"payload".to_vec(),
        }];
        let r = ex.run(
            &hash,
            "t",
            &TaskInput::default(),
            &shared,
            &Limits::default(),
            None,
            Default::default(),
        );
        assert_eq!(
            r.outcome,
            Outcome::Exited(0),
            "stderr: {}",
            String::from_utf8_lossy(&r.stderr)
        );
        assert_eq!(
            r.files,
            vec![FileEntry {
                path: "out.txt".into(),
                data: b"payload".to_vec()
            }]
        );

        // Without the input file the open fails with a clear exit code.
        let r = ex.run(
            &hash,
            "t",
            &TaskInput::default(),
            &[],
            &Limits::default(),
            None,
            Default::default(),
        );
        assert_eq!(r.outcome, Outcome::Exited(13));
    }

    /// Writes 16 random bytes and the realtime and monotonic clocks
    /// (8 bytes each) to stdout.
    const ENTROPY_WAT: &str = r#"
    (module
      (import "wasi_snapshot_preview1" "random_get" (func $random (param i32 i32) (result i32)))
      (import "wasi_snapshot_preview1" "clock_time_get" (func $now (param i32 i64 i32) (result i32)))
      (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
      (memory (export "memory") 2)
      (func (export "_start")
        (drop (call $random (i32.const 104) (i32.const 16)))
        (drop (call $now (i32.const 0) (i64.const 1) (i32.const 120)))
        (drop (call $now (i32.const 1) (i64.const 1) (i32.const 128)))
        (i32.store (i32.const 0) (i32.const 104))
        (i32.store (i32.const 4) (i32.const 32))
        (drop (call $write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8)))))
    "#;

    #[test]
    fn deterministic_mode_and_peak_memory() {
        let bytes = wat::parse_str(ENTROPY_WAT).unwrap();
        let hash = pumasi_proto::module_hash(&bytes);
        let ex = WasmExecutor::new().unwrap();
        ex.load_module(hash, &bytes).unwrap();
        let run = |seed: Option<[u8; 32]>| {
            ex.run(
                &hash,
                "t",
                &TaskInput::default(),
                &[],
                &Limits::default(),
                seed,
                Default::default(),
            )
        };
        let a = run(Some([1; 32]));
        let b = run(Some([1; 32]));
        let c = run(Some([2; 32]));
        assert_eq!(a.outcome, Outcome::Exited(0));
        assert_eq!(a.stdout.len(), 32);
        assert_eq!(a.stdout, b.stdout, "same seed, same output");
        assert_eq!(a.fuel_used, b.fuel_used);
        assert_ne!(
            a.stdout[..16],
            c.stdout[..16],
            "seed changes the random bytes"
        );
        let realtime = u64::from_le_bytes(a.stdout[16..24].try_into().unwrap());
        assert!(
            (946_684_800_000_000_000..946_684_900_000_000_000).contains(&realtime),
            "virtual clock starts in 2000: {realtime}"
        );
        let x = run(None);
        let y = run(None);
        assert_ne!(x.stdout[..16], y.stdout[..16], "real randomness by default");
        assert_eq!(a.peak_memory_bytes, Some(2 * 65536));
    }

    #[test]
    fn rejects_bad_hash() {
        let ex = WasmExecutor::new().unwrap();
        assert!(ex.load_module([0; 32], b"\0asm\x01\0\0\0").is_err());
    }
}
