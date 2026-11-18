//! What a run used (1.21): CPU time, peak memory, disk and network bytes,
//! measured on the node and sent after the report
//! ([`pumasi_proto::msg::ReportTail::usage`]).
//!
//! - Docker on Linux: the container's cgroup (v2, or v1) and network namespace,
//!   read every half second while it runs. Elsewhere (Docker Desktop, the
//!   Pumasi WSL distro), or when the agent cannot see the cgroup:
//!   `docker stats` every few seconds, so short peaks can be missed.
//! - MicroVMs: the VMM process (CPU time, its resident memory at most,
//!   disk bytes) and the gateway's byte counts.
//! - WASM: the engine's linear memory and the CPU time of the thread that
//!   ran it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pumasi_proto::attempt::RunUse;
use tokio::process::Command;

/// The CPU time of the calling thread, in milliseconds.
pub fn thread_cpu_ms() -> Option<u64> {
    // macOS: the kernel's own count. `CLOCK_THREAD_CPUTIME_ID` there comes
    // back in raw Mach ticks under Rosetta 2 (an x86-64 agent on Apple
    // Silicon), 24 times too small.
    #[cfg(target_os = "macos")]
    {
        // SAFETY: the calling thread's port (no reference is added, so
        // none is released) and a `thread_basic_info` with its size in
        // `integer_t`s for the kernel to fill.
        unsafe {
            let port = libc::pthread_mach_thread_np(libc::pthread_self());
            let mut info: libc::thread_basic_info = std::mem::zeroed();
            let mut count = libc::THREAD_BASIC_INFO_COUNT;
            let r = libc::thread_info(
                port,
                libc::THREAD_BASIC_INFO as libc::thread_flavor_t,
                (&mut info as *mut libc::thread_basic_info).cast(),
                &mut count,
            );
            if r != libc::KERN_SUCCESS {
                return None;
            }
            let ms = |t: libc::time_value_t| t.seconds as u64 * 1000 + t.microseconds as u64 / 1000;
            Some(ms(info.user_time) + ms(info.system_time))
        }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid timespec for the call to fill.
        let r = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        (r == 0).then(|| ts.tv_sec as u64 * 1000 + ts.tv_nsec as u64 / 1_000_000)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::FILETIME;
        use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};
        let z = || FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut c, mut e, mut k, mut u) = (z(), z(), z(), z());
        // SAFETY: the pseudo-handle of the current thread and four valid
        // FILETIMEs to fill.
        let ok = unsafe { GetThreadTimes(GetCurrentThread(), &mut c, &mut e, &mut k, &mut u) };
        let t = |f: FILETIME| ((f.dwHighDateTime as u64) << 32) | f.dwLowDateTime as u64;
        // 100 ns units.
        (ok != 0).then(|| (t(k) + t(u)) / 10_000)
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

/// Samples what a run uses in the background until [`Sampler::finish`].
pub struct Sampler {
    shared: Arc<Mutex<RunUse>>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Sampler {
    /// What the samples saw.
    pub async fn finish(self) -> RunUse {
        self.stop.store(true, Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(3), self.task).await;
        *self.shared.lock().unwrap()
    }

    fn spawn<F>(f: F) -> Self
    where
        F: FnOnce(Arc<Mutex<RunUse>>, Arc<AtomicBool>) -> tokio::task::JoinHandle<()>,
    {
        let shared = Arc::new(Mutex::new(RunUse::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let task = f(shared.clone(), stop.clone());
        Self { shared, stop, task }
    }

    /// A container started by the Docker CLI `bin`.
    pub fn docker(bin: String, name: String) -> Self {
        Self::spawn(|shared, stop| tokio::spawn(docker_loop(bin, name, shared, stop)))
    }

    /// A process (a microVM's VMM).
    pub fn process(pid: u32) -> Self {
        Self::spawn(|shared, stop| tokio::spawn(process_loop(pid, shared, stop)))
    }

    /// Lets the energy meter read the CPU time while the run goes on
    /// (1.22).
    pub fn feed(&self, f: &crate::energy::CpuFeed) {
        f.attach(self.shared.clone());
    }
}

/// Sleeps `d` in short steps, returning early once `stop` is set.
async fn nap(d: Duration, stop: &AtomicBool) {
    let step = Duration::from_millis(100);
    let mut left = d;
    while !left.is_zero() && !stop.load(Ordering::Relaxed) {
        let s = left.min(step);
        tokio::time::sleep(s).await;
        left -= s;
    }
}

async fn docker_loop(bin: String, name: String, shared: Arc<Mutex<RunUse>>, stop: Arc<AtomicBool>) {
    // The container's main process, once it runs.
    let mut cgroup = None;
    for _ in 0..20 {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if let Some(pid) = container_pid(&bin, &name).await {
            cgroup = Cgroup::of(pid);
            break;
        }
        nap(Duration::from_millis(250), &stop).await;
    }
    match cgroup {
        Some(cg) => loop {
            if let Some(u) = cg.read() {
                merge(&mut shared.lock().unwrap(), u);
            }
            if stop.load(Ordering::Relaxed) {
                break;
            }
            nap(Duration::from_millis(500), &stop).await;
        },
        None => {
            // `docker stats` takes a second or two to answer.
            let mut cpu_ms = 0f64;
            let mut last = std::time::Instant::now();
            while !stop.load(Ordering::Relaxed) {
                if let Some(s) = docker_stats(&bin, &name).await {
                    let now = std::time::Instant::now();
                    cpu_ms += s.cpu_percent / 100.0 * now.duration_since(last).as_millis() as f64;
                    last = now;
                    let mut u = shared.lock().unwrap();
                    u.cpu_ms = Some(cpu_ms as u64);
                    u.peak_memory_bytes = Some(u.peak_memory_bytes.unwrap_or(0).max(s.memory));
                    u.net_rx_bytes = Some(s.net_rx);
                    u.net_tx_bytes = Some(s.net_tx);
                    u.disk_read_bytes = Some(s.disk_read);
                    u.disk_write_bytes = Some(s.disk_write);
                }
                nap(Duration::from_secs(2), &stop).await;
            }
        }
    }
}

/// Keeps the highest of each figure: they only grow while a run lasts
/// (memory is its peak).
fn merge(into: &mut RunUse, u: RunUse) {
    fn max(a: &mut Option<u64>, b: Option<u64>) {
        if let Some(b) = b {
            *a = Some(a.unwrap_or(0).max(b));
        }
    }
    max(&mut into.cpu_ms, u.cpu_ms);
    max(&mut into.peak_memory_bytes, u.peak_memory_bytes);
    max(&mut into.disk_read_bytes, u.disk_read_bytes);
    max(&mut into.disk_write_bytes, u.disk_write_bytes);
    max(&mut into.net_rx_bytes, u.net_rx_bytes);
    max(&mut into.net_tx_bytes, u.net_tx_bytes);
}

async fn container_pid(bin: &str, name: &str) -> Option<u32> {
    let out = tokio::time::timeout(
        Duration::from_secs(10),
        crate::wsl_docker::docker_command(bin)
            .args(["inspect", "--format", "{{.State.Pid}}", name])
            .stdin(std::process::Stdio::null())
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    let pid: u32 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    (pid > 0).then_some(pid)
}

/// A container's cgroup directories and its main process, on this host:
/// one for cgroup v2, one per controller for v1.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct Cgroup {
    v2: Option<std::path::PathBuf>,
    v1_memory: Option<std::path::PathBuf>,
    v1_cpu: Option<std::path::PathBuf>,
    v1_blkio: Option<std::path::PathBuf>,
    pid: u32,
}

impl Cgroup {
    #[cfg(target_os = "linux")]
    fn of(pid: u32) -> Option<Self> {
        let text = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
        let root = std::path::Path::new("/sys/fs/cgroup");
        let mut cg = Cgroup {
            v2: None,
            v1_memory: None,
            v1_cpu: None,
            v1_blkio: None,
            pid,
        };
        for line in text.lines() {
            // `ID:CONTROLLERS:PATH`; v2 is `0::PATH`.
            let mut parts = line.splitn(3, ':');
            let (Some(_), Some(ctl), Some(rel)) = (parts.next(), parts.next(), parts.next()) else {
                continue;
            };
            let rel = rel.trim_start_matches('/');
            if ctl.is_empty() {
                let dir = root.join(rel);
                if dir.join("cpu.stat").exists() {
                    cg.v2 = Some(dir);
                }
                continue;
            }
            let ctls: Vec<&str> = ctl.split(',').collect();
            let dir = root.join(ctl).join(rel);
            if ctls.contains(&"memory") {
                cg.v1_memory = Some(dir.clone());
            }
            if ctls.contains(&"cpuacct") {
                cg.v1_cpu = Some(dir.clone());
            }
            if ctls.contains(&"blkio") {
                cg.v1_blkio = Some(dir);
            }
        }
        let v1 = cg
            .v1_memory
            .as_ref()
            .is_some_and(|d| d.join("memory.max_usage_in_bytes").exists());
        (cg.v2.is_some() || v1).then_some(cg)
    }

    #[cfg(not(target_os = "linux"))]
    fn of(_pid: u32) -> Option<Self> {
        None
    }

    fn read(&self) -> Option<RunUse> {
        let mut u = match &self.v2 {
            Some(dir) => {
                let read = |f: &str| std::fs::read_to_string(dir.join(f)).ok();
                let cpu = read("cpu.stat")?;
                let mut u = RunUse {
                    cpu_ms: field(&cpu, "usage_usec").map(|us| us / 1000),
                    ..Default::default()
                };
                u.peak_memory_bytes = read("memory.peak")
                    .or_else(|| read("memory.current"))
                    .and_then(|s| s.trim().parse().ok());
                if let Some(io) = read("io.stat") {
                    let (r, w) = io_bytes(&io);
                    u.disk_read_bytes = Some(r);
                    u.disk_write_bytes = Some(w);
                }
                u
            }
            None => {
                let read = |d: &Option<std::path::PathBuf>, f: &str| {
                    d.as_ref()
                        .and_then(|d| std::fs::read_to_string(d.join(f)).ok())
                };
                let mut u = RunUse {
                    peak_memory_bytes: read(&self.v1_memory, "memory.max_usage_in_bytes")
                        .and_then(|s| s.trim().parse().ok()),
                    ..Default::default()
                };
                u.peak_memory_bytes?;
                // Nanoseconds.
                u.cpu_ms = read(&self.v1_cpu, "cpuacct.usage")
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(|ns| ns / 1_000_000);
                if let Some(io) = read(&self.v1_blkio, "blkio.throttle.io_service_bytes") {
                    let (r, w) = blkio_bytes(&io);
                    u.disk_read_bytes = Some(r);
                    u.disk_write_bytes = Some(w);
                }
                u
            }
        };
        if let Ok(dev) = std::fs::read_to_string(format!("/proc/{}/net/dev", self.pid)) {
            let (rx, tx) = net_bytes(&dev);
            u.net_rx_bytes = Some(rx);
            u.net_tx_bytes = Some(tx);
        }
        Some(u)
    }
}

/// Bytes read and written in cgroup v1's `blkio.throttle.io_service_bytes`
/// (`MAJ:MIN Read N` lines; the `Total` lines are left out).
fn blkio_bytes(text: &str) -> (u64, u64) {
    let (mut r, mut w) = (0u64, 0u64);
    for line in text.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if let [_, op, n] = f.as_slice() {
            let n: u64 = n.parse().unwrap_or(0);
            match *op {
                "Read" => r += n,
                "Write" => w += n,
                _ => {}
            }
        }
    }
    (r, w)
}

/// `usage_usec 1234` in a flat-keyed cgroup file.
fn field(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|l| {
        let (k, v) = l.split_once(' ')?;
        (k == key).then(|| v.trim().parse().ok())?
    })
}

/// Bytes read and written over every device in `io.stat`.
fn io_bytes(text: &str) -> (u64, u64) {
    let (mut r, mut w) = (0u64, 0u64);
    for line in text.lines() {
        for kv in line.split_whitespace().skip(1) {
            match kv.split_once('=') {
                Some(("rbytes", v)) => r += v.parse::<u64>().unwrap_or(0),
                Some(("wbytes", v)) => w += v.parse::<u64>().unwrap_or(0),
                _ => {}
            }
        }
    }
    (r, w)
}

/// Bytes received and sent over every interface but loopback in
/// `/proc/PID/net/dev`.
fn net_bytes(text: &str) -> (u64, u64) {
    let (mut rx, mut tx) = (0u64, 0u64);
    for line in text.lines().skip(2) {
        let Some((iface, rest)) = line.split_once(':') else {
            continue;
        };
        if iface.trim() == "lo" {
            continue;
        }
        let v: Vec<u64> = rest
            .split_whitespace()
            .map(|n| n.parse().unwrap_or(0))
            .collect();
        if v.len() >= 9 {
            rx += v[0];
            tx += v[8];
        }
    }
    (rx, tx)
}

/// One `docker stats --no-stream` line.
#[derive(Debug, Default, PartialEq)]
struct Stats {
    cpu_percent: f64,
    memory: u64,
    net_rx: u64,
    net_tx: u64,
    disk_read: u64,
    disk_write: u64,
}

async fn docker_stats(bin: &str, name: &str) -> Option<Stats> {
    let mut cmd: Command = crate::wsl_docker::docker_command(bin);
    let out = tokio::time::timeout(
        Duration::from_secs(10),
        cmd.args([
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}|{{.BlockIO}}",
            name,
        ])
        .stdin(std::process::Stdio::null())
        .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_stats(String::from_utf8_lossy(&out.stdout).lines().next()?)
}

/// `0.15%|12.5MiB / 1GiB|1.2kB / 648B|0B / 4.1MB`.
fn parse_stats(line: &str) -> Option<Stats> {
    let parts: Vec<&str> = line.trim().split('|').collect();
    if parts.len() != 4 {
        return None;
    }
    let pair = |s: &str| -> Option<(u64, u64)> {
        let (a, b) = s.split_once('/')?;
        Some((human_bytes(a)?, human_bytes(b)?))
    };
    let (net_rx, net_tx) = pair(parts[2])?;
    let (disk_read, disk_write) = pair(parts[3])?;
    Some(Stats {
        cpu_percent: parts[0].trim().trim_end_matches('%').parse().ok()?,
        memory: human_bytes(parts[1].split('/').next()?)?,
        net_rx,
        net_tx,
        disk_read,
        disk_write,
    })
}

/// `12.5MiB`, `1.2kB`, `648B`, `0B` (Docker's units, binary and decimal).
fn human_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.trim().parse().ok()?;
    let mult: f64 = match unit.trim() {
        "" | "B" => 1.0,
        "kB" | "KB" => 1e3,
        "MB" => 1e6,
        "GB" => 1e9,
        "TB" => 1e12,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        "TiB" => 1024f64.powi(4),
        _ => return None,
    };
    Some((n * mult).round() as u64)
}

async fn process_loop(pid: u32, shared: Arc<Mutex<RunUse>>, stop: Arc<AtomicBool>) {
    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
    let pid = Pid::from_u32(pid);
    let mut sys = System::new();
    let kind = ProcessRefreshKind::nothing()
        .with_cpu()
        .with_memory()
        .with_disk_usage();
    loop {
        sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, kind);
        if let Some(p) = sys.process(pid) {
            let d = p.disk_usage();
            let mut u = shared.lock().unwrap();
            merge(
                &mut u,
                RunUse {
                    cpu_ms: Some(p.accumulated_cpu_time()),
                    peak_memory_bytes: Some(p.memory()),
                    disk_read_bytes: Some(d.total_read_bytes),
                    disk_write_bytes: Some(d.total_written_bytes),
                    ..Default::default()
                },
            );
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        nap(Duration::from_millis(500), &stop).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_cgroup_v1_blkio() {
        let text = "8:0 Read 4096\n8:0 Write 8192\n8:0 Sync 0\n8:0 Total 12288\n8:16 Read 1\nTotal 12289\n";
        assert_eq!(blkio_bytes(text), (4097, 8192));
    }

    #[test]
    fn reads_cgroup_and_proc_files() {
        assert_eq!(
            field("usage_usec 1500000\nuser_usec 1000000\n", "usage_usec"),
            Some(1_500_000)
        );
        assert_eq!(
            io_bytes("8:0 rbytes=4096 wbytes=8192 rios=1 wios=2\n259:0 rbytes=1 wbytes=2 rios=0\n"),
            (4097, 8194)
        );
        let dev = "Inter-|   Receive                                                |  Transmit\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    lo:     100       1    0    0    0     0          0         0      100       1    0    0    0     0       0          0\n  eth0:    2048      10    0    0    0     0          0         0     1024       5    0    0    0     0       0          0\n";
        assert_eq!(net_bytes(dev), (2048, 1024));
    }

    #[test]
    fn parses_docker_stats() {
        let s = parse_stats("12.50%|12.5MiB / 1GiB|1.2kB / 648B|0B / 4.1MB").unwrap();
        assert_eq!(s.cpu_percent, 12.5);
        assert_eq!(s.memory, 13_107_200);
        assert_eq!((s.net_rx, s.net_tx), (1200, 648));
        assert_eq!((s.disk_read, s.disk_write), (0, 4_100_000));
        assert!(parse_stats("garbage").is_none());
    }

    #[test]
    fn the_thread_clock_moves() {
        let a = thread_cpu_ms().unwrap();
        let mut x = 0u64;
        let t = std::time::Instant::now();
        while t.elapsed() < Duration::from_millis(60) {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        std::hint::black_box(x);
        let b = thread_cpu_ms().unwrap();
        assert!(b >= a + 20, "{a} -> {b}");
    }
}
