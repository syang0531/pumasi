//! Benchmarks: what this machine can do, measured when the agent starts
//! (if its saved profile is stale) and again every day while it is idle.
//!
//! - **CPU**: the kernel of [`pumasi_proto::perf`] in Wasmtime, one thread
//!   and then every offered CPU at once, as a score where the reference
//!   vCPU is 1000.
//! - **Memory**: copy bandwidth over a buffer much larger than the caches.
//! - **Disk** (the drive under the agent's work directory), fio-style:
//!   sequential write and read in 1 MiB blocks, 4 KiB random reads one at a
//!   time (latency) and eight at a time (IOPS), all with the page cache
//!   bypassed (O_DIRECT on Linux, F_NOCACHE on macOS, unbuffered I/O on
//!   Windows). The latency and speed say whether it is a spinning disk, a
//!   SATA SSD or NVMe.
//! - **SMART**: health, wear and errors from `smartctl -j` when it is
//!   installed (and allowed to read the drive).
//!
//! The profile is signed with the node key and sent to the control plane,
//! which bills CPU time by the score and can check it with a challenge.

use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use anyhow::Result;
use iroh::SecretKey;
use pumasi_proto::perf::{
    BENCH_VERSION, CpuPerf, DiskClass, DiskPerf, KERNEL_WAT, MemoryPerf, PerfProfile, SignedPerf,
    SmartInfo, classify_disk, score_for,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use wasmtime::{Config, Engine, Instance, Module, Store, TypedFunc};

const PROFILE_FILE: &str = "perf.json";
const BENCH_FILE: &str = ".pumasi-bench.tmp";
const MIB: u64 = 1 << 20;
const BLOCK: usize = 1 << 20;
const PAGE: usize = 4096;

/// How hard to benchmark, and where.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Directory whose drive is measured (the agent's work directory).
    pub dir: PathBuf,
    /// Length of each CPU run.
    pub cpu: Duration,
    /// Memory copy buffer size (halved when the machine has little).
    pub memory_bytes: u64,
    /// Size of the disk test file, and the time each disk phase may take.
    pub disk_bytes: u64,
    pub disk: Duration,
    /// Ask `smartctl` about the drive.
    pub smart: bool,
    /// Measure again after this long.
    pub interval: Duration,
    /// Test hook: report CPU scores this many times higher than measured,
    /// like a provider inflating its benchmark. 1 is honest.
    #[doc(hidden)]
    pub inflate: u32,
}

impl BenchConfig {
    /// About ten seconds on a typical machine.
    pub fn standard(dir: PathBuf) -> Self {
        Self {
            dir,
            cpu: Duration::from_millis(1000),
            memory_bytes: 256 * MIB,
            disk_bytes: 256 * MIB,
            disk: Duration::from_millis(1500),
            smart: true,
            interval: Duration::from_secs(24 * 3600),
            inflate: 1,
        }
    }

    /// Well under a second, for tests.
    pub fn quick(dir: PathBuf) -> Self {
        Self {
            dir,
            cpu: Duration::from_millis(150),
            memory_bytes: 16 * MIB,
            disk_bytes: 8 * MIB,
            disk: Duration::from_millis(150),
            smart: false,
            interval: Duration::from_secs(24 * 3600),
            inflate: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// CPU
// ---------------------------------------------------------------------------

/// The benchmark kernel compiled for Wasmtime, with fuel metering on as for
/// real tasks.
#[derive(Clone)]
pub struct CpuKernel {
    engine: Engine,
    module: Module,
}

struct KernelRun {
    store: Store<()>,
    func: TypedFunc<(i64, i64), i64>,
}

impl CpuKernel {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config)?;
        let module = Module::new(&engine, KERNEL_WAT)
            .map_err(|e| anyhow::anyhow!("compile the benchmark kernel: {e:#}"))?;
        Ok(Self { engine, module })
    }

    fn instance(&self) -> Result<KernelRun> {
        let mut store = Store::new(&self.engine, ());
        store.set_fuel(u64::MAX)?;
        let instance = Instance::new(&mut store, &self.module, &[])?;
        let func = instance.get_typed_func::<(i64, i64), i64>(&mut store, "bench")?;
        Ok(KernelRun { store, func })
    }

    /// Runs the kernel once: its result and the fuel it burned.
    pub fn run(&self, seed: u64, iters: u64) -> Result<(u64, u64)> {
        let mut k = self.instance()?;
        let r = k.func.call(&mut k.store, (seed as i64, iters as i64))?;
        let fuel = u64::MAX - k.store.get_fuel()?;
        Ok((r as u64, fuel))
    }

    /// Kernel iterations per second on one thread over about `window`.
    fn rate(&self, window: Duration) -> Result<(u64, u128)> {
        let mut k = self.instance()?;
        let mut chunk: u64 = 200_000;
        let mut iters = 0u64;
        let start = Instant::now();
        let mut seed = 1u64;
        while start.elapsed() < window {
            let t = Instant::now();
            k.func.call(&mut k.store, (seed as i64, chunk as i64))?;
            iters += chunk;
            seed += 1;
            // Chunks of about 20 ms keep the overhead negligible.
            if t.elapsed() < Duration::from_millis(10) {
                chunk *= 2;
            }
        }
        Ok((iters, start.elapsed().as_nanos()))
    }
}

fn cpu(kernel: &CpuKernel, threads: u32, window: Duration) -> Result<CpuPerf> {
    // Warm up (the first call compiles trampolines, the CPU leaves idle
    // clocks), then measure one thread alone.
    kernel.rate(window / 5)?;
    let (iters, nanos) = kernel.rate(window)?;
    let single = score_for(iters, nanos);
    // Every offered CPU at once.
    let threads = threads.max(1);
    let barrier = Arc::new(Barrier::new(threads as usize));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let k = kernel.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                k.rate(window)
            })
        })
        .collect();
    let mut multi = 0u64;
    for h in handles {
        let (iters, nanos) = h
            .join()
            .map_err(|_| anyhow::anyhow!("benchmark thread panicked"))??;
        multi += score_for(iters, nanos) as u64;
    }
    let mut sys = sysinfo::System::new_with_specifics(
        sysinfo::RefreshKind::nothing().with_cpu(sysinfo::CpuRefreshKind::nothing()),
    );
    sys.refresh_cpu_list(sysinfo::CpuRefreshKind::nothing());
    let model = sys
        .cpus()
        .first()
        .map(|c| c.brand().trim().to_string())
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| std::env::consts::ARCH.to_string());
    Ok(CpuPerf {
        model,
        threads,
        single,
        multi: multi.min(u32::MAX as u64) as u32,
    })
}

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

fn memory(want: u64, window: Duration) -> MemoryPerf {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let total = sys.total_memory();
    // At most an eighth of the machine's memory, split in two buffers.
    let size = (want.min(total / 8).max(4 * MIB) / 2) as usize;
    let src = vec![1u8; size];
    let mut dst = vec![0u8; size];
    dst.copy_from_slice(&src);
    let start = Instant::now();
    let mut copies = 0u64;
    while start.elapsed() < window || copies < 2 {
        dst.copy_from_slice(std::hint::black_box(&src));
        std::hint::black_box(&mut dst);
        copies += 1;
    }
    let secs = start.elapsed().as_secs_f64();
    let mb = (2 * size as u64 * copies) as f64 / 1e6;
    MemoryPerf {
        total_bytes: total,
        copy_mb_s: (mb / secs).min(u32::MAX as f64) as u32,
    }
}

// ---------------------------------------------------------------------------
// Disk
// ---------------------------------------------------------------------------

/// A page-aligned buffer, as unbuffered I/O requires.
struct Aligned {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

// SAFETY: plain owned memory.
unsafe impl Send for Aligned {}

impl Aligned {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, PAGE).expect("valid layout");
        // SAFETY: non-zero size.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Self { ptr, layout }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: owned allocation of this size.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }

    fn as_ref(&self) -> &[u8] {
        // SAFETY: owned allocation of this size.
        unsafe { std::slice::from_raw_parts(self.ptr, self.layout.size()) }
    }
}

impl Drop for Aligned {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

/// Opens the test file so reads and writes bypass the page cache.
fn open_direct(path: &Path) -> io::Result<std::fs::File> {
    let mut o = std::fs::OpenOptions::new();
    o.read(true).write(true).create(true).truncate(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.custom_flags(libc::O_DIRECT);
        o.open(path)
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        let f = o.open(path)?;
        // SAFETY: valid descriptor; F_NOCACHE takes an int.
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(f)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
        const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
        o.custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH);
        o.open(path)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (o, path);
        Err(io::Error::new(io::ErrorKind::Unsupported, "no direct I/O"))
    }
}

fn open_buffered(path: &Path) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}

fn read_at(f: &std::fs::File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_at(f, buf, offset)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::FileExt::seek_read(f, buf, offset)
    }
}

fn write_at(f: &std::fs::File, buf: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_at(f, buf, offset)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::FileExt::seek_write(f, buf, offset)
    }
}

/// Tells the OS to forget the file's cached pages (buffered fallback).
fn drop_cache(f: &std::fs::File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: valid descriptor; advisory only.
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
}

/// xorshift64*: offsets for random reads.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// The raw disk measurements.
#[derive(Debug)]
struct DiskNumbers {
    direct: bool,
    seq_write_mb_s: u32,
    seq_read_mb_s: u32,
    p50_us: u32,
    p99_us: u32,
    iops: u32,
}

fn measure_disk(dir: &Path, bytes: u64, window: Duration) -> Result<DiskNumbers> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(BENCH_FILE);
    struct Remove<'a>(&'a Path);
    impl Drop for Remove<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0);
        }
    }
    let _remove = Remove(&path);
    let bytes = (bytes / BLOCK as u64).max(4) * BLOCK as u64;
    let mut block = Aligned::new(BLOCK);
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for chunk in block.as_mut().chunks_mut(8) {
        chunk.copy_from_slice(&rng.next().to_le_bytes()[..chunk.len()]);
    }

    // Unbuffered if the filesystem allows it (tmpfs and some overlays do
    // not), else buffered with the cache dropped after writing.
    let (file, mut direct) = match open_direct(&path) {
        Ok(f) => match write_at(&f, block.as_ref(), 0) {
            Ok(n) if n == BLOCK => (f, true),
            _ => (open_buffered(&path)?, false),
        },
        Err(e) => {
            debug!("no direct I/O in {}: {e}", dir.display());
            (open_buffered(&path)?, false)
        }
    };

    // Sequential write, synced.
    let start = Instant::now();
    let mut written = 0u64;
    while written < bytes && (written < 4 * BLOCK as u64 || start.elapsed() < window) {
        let n = write_at(&file, block.as_ref(), written)?;
        anyhow::ensure!(n == BLOCK, "short write");
        written += BLOCK as u64;
    }
    file.sync_all()?;
    let secs = start.elapsed().as_secs_f64();
    let seq_write_mb_s = (written as f64 / 1e6 / secs) as u32;
    if !direct {
        drop_cache(&file);
    }

    // Sequential read.
    let start = Instant::now();
    let mut read = 0u64;
    while read < written && (read < 4 * BLOCK as u64 || start.elapsed() < window) {
        let n = read_at(&file, block.as_mut(), read)?;
        if n == 0 {
            break;
        }
        read += n as u64;
    }
    let secs = start.elapsed().as_secs_f64();
    let seq_read_mb_s = (read as f64 / 1e6 / secs) as u32;

    // Random 4 KiB reads, one at a time.
    let pages = written / PAGE as u64;
    let mut page = Aligned::new(PAGE);
    let mut lat: Vec<u32> = Vec::new();
    let start = Instant::now();
    while lat.len() < 20 || (start.elapsed() < window / 2 && lat.len() < 20_000) {
        let off = rng.next() % pages * PAGE as u64;
        let t = Instant::now();
        read_at(&file, page.as_mut(), off)?;
        lat.push(t.elapsed().as_micros().min(u32::MAX as u128) as u32);
    }
    lat.sort_unstable();
    let p50_us = lat[lat.len() / 2];
    let p99_us = lat[(lat.len() * 99 / 100).min(lat.len() - 1)];

    // Eight at a time.
    let file = Arc::new(file);
    let barrier = Arc::new(Barrier::new(8));
    let budget = window / 2;
    let handles: Vec<_> = (0..8u64)
        .map(|i| {
            let file = file.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || -> io::Result<(u64, f64)> {
                let mut page = Aligned::new(PAGE);
                let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ (i + 1));
                barrier.wait();
                let start = Instant::now();
                let mut n = 0u64;
                while n < 8 || (start.elapsed() < budget && n < 50_000) {
                    let off = rng.next() % pages * PAGE as u64;
                    read_at(&file, page.as_mut(), off)?;
                    n += 1;
                }
                Ok((n, start.elapsed().as_secs_f64()))
            })
        })
        .collect();
    let mut iops = 0.0;
    for h in handles {
        let (n, secs) = h
            .join()
            .map_err(|_| anyhow::anyhow!("disk benchmark thread panicked"))??;
        iops += n as f64 / secs.max(1e-9);
    }
    // A cached "disk" answers in well under a microsecond; nothing real does.
    if p50_us == 0 && seq_read_mb_s > 20_000 {
        direct = false;
    }
    Ok(DiskNumbers {
        direct,
        seq_write_mb_s,
        seq_read_mb_s,
        p50_us,
        p99_us,
        iops: iops.min(u32::MAX as f64) as u32,
    })
}

/// What the OS knows about the drive under `dir`: its kind, name and size.
struct DriveInfo {
    os_class: Option<DiskClass>,
    device: Option<String>,
    total: u64,
    free: u64,
}

fn drive_info(dir: &Path) -> DriveInfo {
    let dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let best = disks
        .list()
        .iter()
        .filter(|d| dir.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len());
    let Some(d) = best else {
        return DriveInfo {
            os_class: None,
            device: None,
            total: 0,
            free: 0,
        };
    };
    let name = d.name().to_string_lossy().to_string();
    let os_class = match d.kind() {
        sysinfo::DiskKind::HDD => Some(DiskClass::Hdd),
        sysinfo::DiskKind::SSD if name.contains("nvme") => Some(DiskClass::Nvme),
        sysinfo::DiskKind::SSD => Some(DiskClass::Ssd),
        _ => None,
    };
    DriveInfo {
        os_class,
        device: whole_device(&name),
        total: d.total_space(),
        free: d.available_space(),
    }
}

/// The whole-drive device for a partition, as `smartctl` wants it:
/// `/dev/nvme0n1p2` -> `/dev/nvme0n1`, `/dev/sda1` -> `/dev/sda`,
/// `/dev/disk3s1s1` -> `/dev/disk3`.
fn whole_device(name: &str) -> Option<String> {
    let name = name.trim();
    if !name.starts_with("/dev/") {
        return None;
    }
    let base = &name[5..];
    if let Some(rest) = base.strip_prefix("disk") {
        let n: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        return (!n.is_empty()).then(|| format!("/dev/disk{n}"));
    }
    if base.starts_with("nvme") || base.starts_with("mmcblk") {
        return Some(match base.rfind('p') {
            Some(i)
                if i > 4
                    && base[i + 1..].chars().all(|c| c.is_ascii_digit())
                    && i + 1 < base.len() =>
            {
                format!("/dev/{}", &base[..i])
            }
            _ => name.to_string(),
        });
    }
    if base.starts_with("sd") || base.starts_with("hd") || base.starts_with("vd") {
        return Some(format!(
            "/dev/{}",
            base.trim_end_matches(|c: char| c.is_ascii_digit())
        ));
    }
    None
}

/// Reads health and wear from `smartctl -j -a` output.
pub fn parse_smartctl(json: &str) -> Option<SmartInfo> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let model = v["model_name"]
        .as_str()
        .or(v["device"]["name"].as_str())?
        .to_string();
    let passed = v["smart_status"]["passed"].as_bool();
    let nvme = &v["nvme_smart_health_information_log"];
    let attrs: Vec<&serde_json::Value> = v["ata_smart_attributes"]["table"]
        .as_array()
        .map(|t| t.iter().collect())
        .unwrap_or_default();
    let attr = |id: u64| attrs.iter().find(|a| a["id"].as_u64() == Some(id));
    let percent_used = nvme["percentage_used"]
        .as_u64()
        .map(|p| p.min(255) as u8)
        .or_else(|| {
            // Wear leveling count, SSD life left, media wearout indicator,
            // percent lifetime remaining: normalized value is life left.
            [177, 231, 233, 202]
                .iter()
                .find_map(|&id| attr(id)?["value"].as_u64())
                .map(|left| 100u64.saturating_sub(left.min(100)) as u8)
        });
    let power_on_hours = v["power_on_time"]["hours"]
        .as_u64()
        .map(|h| h.min(u32::MAX as u64) as u32);
    let media_errors = nvme["media_errors"].as_u64().or_else(|| {
        let raw = |id| attr(id).and_then(|a| a["raw"]["value"].as_u64());
        match (raw(5), raw(197)) {
            (None, None) => None,
            (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
        }
    });
    let temperature_c = v["temperature"]["current"].as_i64().map(|t| t as i16);
    Some(SmartInfo {
        model,
        passed,
        percent_used,
        power_on_hours,
        media_errors,
        temperature_c,
    })
}

fn smart(device: Option<&str>) -> std::result::Result<SmartInfo, String> {
    let mut candidates: Vec<String> = device.map(String::from).into_iter().collect();
    if cfg!(windows) && candidates.is_empty() {
        candidates.push("/dev/sda".into());
    }
    if candidates.is_empty() {
        return Err("SMART: could not tell which drive the work directory is on".into());
    }
    let mut last = String::new();
    for dev in candidates {
        let out = match std::process::Command::new("smartctl")
            .args(["-j", "-a", &dev])
            .stdin(std::process::Stdio::null())
            .output()
        {
            Ok(o) => o,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err("SMART: smartctl is not installed (smartmontools)".into());
            }
            Err(e) => return Err(format!("SMART: smartctl: {e}")),
        };
        // smartctl's exit status is a bit mask of findings; read the JSON.
        if let Some(info) = parse_smartctl(&String::from_utf8_lossy(&out.stdout)) {
            return Ok(info);
        }
        last = format!(
            "SMART: smartctl could not read {dev} (exit {}; it may need root)",
            out.status.code().unwrap_or(-1)
        );
    }
    Err(last)
}

// ---------------------------------------------------------------------------
// The whole profile
// ---------------------------------------------------------------------------

/// Measures this machine. Blocking; takes about `cpu * 2.4 + disk * 4` plus
/// the memory test.
pub fn measure(
    config: &BenchConfig,
    kernel: &CpuKernel,
    node: &SecretKey,
    threads: u32,
) -> Result<PerfProfile> {
    let mut notes = Vec::new();
    let mut cpu = cpu(kernel, threads, config.cpu)?;
    if config.inflate > 1 {
        cpu.single = cpu.single.saturating_mul(config.inflate);
        cpu.multi = cpu.multi.saturating_mul(config.inflate);
    }
    let memory = memory(
        config.memory_bytes,
        config.cpu.min(Duration::from_millis(400)),
    );
    let drive = drive_info(&config.dir);
    let disk = if drive.free != 0 && drive.free < config.disk_bytes * 4 {
        notes.push(format!(
            "disk: skipped, only {} MiB free in {}",
            drive.free / MIB,
            config.dir.display()
        ));
        None
    } else {
        match measure_disk(&config.dir, config.disk_bytes, config.disk) {
            Ok(n) => {
                if !n.direct {
                    notes.push(
                        "disk: the filesystem does not allow direct I/O, so reads came from the page cache"
                            .into(),
                    );
                }
                let smart = if config.smart {
                    match smart(drive.device.as_deref()) {
                        Ok(s) => Some(s),
                        Err(e) => {
                            notes.push(e);
                            None
                        }
                    }
                } else {
                    None
                };
                Some(DiskPerf {
                    class: classify_disk(n.direct, n.seq_read_mb_s, n.p50_us),
                    os_class: drive.os_class,
                    direct: n.direct,
                    seq_read_mb_s: n.seq_read_mb_s,
                    seq_write_mb_s: n.seq_write_mb_s,
                    rand_read_p50_us: n.p50_us,
                    rand_read_p99_us: n.p99_us,
                    rand_read_iops: n.iops,
                    total_bytes: drive.total,
                    free_bytes: drive.free,
                    smart,
                })
            }
            Err(e) => {
                notes.push(format!("disk: {e:#}"));
                None
            }
        }
    };
    Ok(PerfProfile {
        bench_version: BENCH_VERSION,
        node: *node.public().as_bytes(),
        measured_unix: now_unix(),
        agent_version: pumasi_proto::version().to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        cpu,
        memory,
        disk,
        notes,
    })
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The saved profile, kept next to the node key.
#[derive(Serialize, Deserialize)]
struct Saved {
    profile: PerfProfile,
}

/// Loads the saved profile if it still describes this machine: the same
/// benchmarks, key, CPU and memory, measured within `interval`.
pub fn load_fresh(
    data_dir: &Path,
    node: &SecretKey,
    threads: u32,
    interval: Duration,
) -> Option<PerfProfile> {
    let text = std::fs::read_to_string(data_dir.join(PROFILE_FILE)).ok()?;
    let saved: Saved = serde_json::from_str(&text).ok()?;
    let p = saved.profile;
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let fresh = p.bench_version == BENCH_VERSION
        && p.node == *node.public().as_bytes()
        && p.cpu.threads == threads.max(1)
        && p.memory.total_bytes == sys.total_memory()
        && now_unix().saturating_sub(p.measured_unix) < interval.as_secs();
    fresh.then_some(p)
}

pub fn save(data_dir: &Path, profile: &PerfProfile) {
    let res = serde_json::to_vec_pretty(&Saved {
        profile: profile.clone(),
    })
    .map_err(anyhow::Error::from)
    .and_then(|b| Ok(std::fs::write(data_dir.join(PROFILE_FILE), b)?));
    if let Err(e) = res {
        warn!("could not save the benchmark profile: {e:#}");
    }
}

/// The saved profile if fresh, else a new measurement (saved). Blocking.
pub fn profile(
    config: &BenchConfig,
    data_dir: &Path,
    kernel: &CpuKernel,
    node: &SecretKey,
    threads: u32,
    force: bool,
) -> Result<SignedPerf> {
    let p = match load_fresh(data_dir, node, threads, config.interval).filter(|_| !force) {
        Some(p) => p,
        None => {
            info!("benchmarking this machine");
            let p = measure(config, kernel, node, threads)?;
            log_profile(&p);
            save(data_dir, &p);
            p
        }
    };
    Ok(SignedPerf::sign(p, node))
}

fn log_profile(p: &PerfProfile) {
    let disk = p
        .disk
        .as_ref()
        .map(|d| {
            format!(
                "{} {} MB/s read, {} us random read",
                d.class, d.seq_read_mb_s, d.rand_read_p50_us
            )
        })
        .unwrap_or_else(|| "not measured".into());
    info!(
        cpu_single = p.cpu.single,
        cpu_multi = p.cpu.multi,
        threads = p.cpu.threads,
        memory_mb_s = p.memory.copy_mb_s,
        %disk,
        "benchmark done"
    );
    for n in &p.notes {
        info!("benchmark note: {n}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::perf::{FUEL_PER_ITER, kernel_native};

    #[test]
    fn wasm_kernel_matches_native_and_burns_the_documented_fuel() {
        let k = CpuKernel::new().unwrap();
        for (seed, iters) in [(1u64, 0u64), (7, 1), (42, 10_000), (u64::MAX, 3)] {
            let (r, _) = k.run(seed, iters).unwrap();
            assert_eq!(r, kernel_native(seed, iters), "seed {seed} iters {iters}");
        }
        let (_, f1) = k.run(5, 100_000).unwrap();
        let (_, f2) = k.run(5, 200_000).unwrap();
        assert_eq!((f2 - f1) / 100_000, FUEL_PER_ITER);
    }

    #[test]
    fn quick_profile_measures_cpu_memory_and_disk() {
        let dir = crate::tempdir::TempDir::new("bench").unwrap();
        let key = SecretKey::generate();
        let k = CpuKernel::new().unwrap();
        let config = BenchConfig::quick(dir.path().to_path_buf());
        let p = measure(&config, &k, &key, 2).unwrap();
        assert!(p.cpu.single > 0 && p.cpu.multi > 0, "{:?}", p.cpu);
        assert_eq!(p.cpu.threads, 2);
        assert!(p.memory.copy_mb_s > 0);
        let d = p.disk.as_ref().expect("disk measured");
        assert!(d.seq_read_mb_s > 0 && d.seq_write_mb_s > 0 && d.rand_read_iops > 0);
        assert!(!dir.path().join(BENCH_FILE).exists(), "test file removed");
        eprintln!("{p:#?}");

        // Saved and reloaded while fresh; stale after the interval.
        save(dir.path(), &p);
        assert_eq!(
            load_fresh(dir.path(), &key, 2, Duration::from_secs(3600)),
            Some(p.clone())
        );
        assert_eq!(
            load_fresh(dir.path(), &key, 4, Duration::from_secs(3600)),
            None
        );
        assert_eq!(
            load_fresh(
                dir.path(),
                &SecretKey::generate(),
                2,
                Duration::from_secs(3600)
            ),
            None
        );
        // An inflated benchmark reports the multiple.
        let inflated = BenchConfig {
            inflate: 10,
            ..config
        };
        let q = measure(&inflated, &k, &key, 1).unwrap();
        assert!(
            q.cpu.single > p.cpu.single * 3,
            "{} vs {}",
            q.cpu.single,
            p.cpu.single
        );
    }

    #[test]
    fn whole_devices() {
        assert_eq!(
            whole_device("/dev/nvme0n1p2").as_deref(),
            Some("/dev/nvme0n1")
        );
        assert_eq!(
            whole_device("/dev/nvme0n1").as_deref(),
            Some("/dev/nvme0n1")
        );
        assert_eq!(whole_device("/dev/sda1").as_deref(), Some("/dev/sda"));
        assert_eq!(
            whole_device("/dev/mmcblk0p1").as_deref(),
            Some("/dev/mmcblk0")
        );
        assert_eq!(
            whole_device("/dev/disk3s1s1").as_deref(),
            Some("/dev/disk3")
        );
        assert_eq!(whole_device("overlay"), None);
        assert_eq!(whole_device("/dev/mapper/vg-root"), None);
    }

    #[test]
    fn smartctl_json() {
        let nvme = r#"{"device":{"name":"/dev/nvme0"},"model_name":"Samsung SSD 970 EVO 1TB",
            "smart_status":{"passed":true},
            "nvme_smart_health_information_log":{"percentage_used":3,"media_errors":0},
            "power_on_time":{"hours":8123},"temperature":{"current":41}}"#;
        let s = parse_smartctl(nvme).unwrap();
        assert_eq!(s.model, "Samsung SSD 970 EVO 1TB");
        assert_eq!(
            (
                s.passed,
                s.percent_used,
                s.power_on_hours,
                s.media_errors,
                s.temperature_c
            ),
            (Some(true), Some(3), Some(8123), Some(0), Some(41))
        );
        let ata = r#"{"model_name":"WDC WD40EFRX","smart_status":{"passed":false},
            "ata_smart_attributes":{"table":[
              {"id":5,"value":100,"raw":{"value":8}},
              {"id":197,"value":200,"raw":{"value":2}}]},
            "power_on_time":{"hours":40000}}"#;
        let s = parse_smartctl(ata).unwrap();
        assert_eq!(
            (s.passed, s.percent_used, s.media_errors),
            (Some(false), None, Some(10))
        );
        let ssd = r#"{"model_name":"Samsung SSD 860","ata_smart_attributes":{"table":[
              {"id":177,"value":92,"raw":{"value":120}}]}}"#;
        assert_eq!(parse_smartctl(ssd).unwrap().percent_used, Some(8));
        assert!(parse_smartctl("not json").is_none());
    }
}
