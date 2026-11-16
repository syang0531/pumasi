//! Warm microVMs (1.14): a few guests booted ahead of time, each waiting
//! for one task of an image it already has mounted.
//!
//! When a task that can run warm starts, the executor takes a warm guest of
//! the same image and size if there is one, writes the task's input onto
//! that guest's control disk (see `pumasi_guest::ControlLayout`), and boots
//! another one for the next task. A warm guest runs one task and powers
//! off, like any other; what it saves is the VMM start, the kernel boot and
//! the mounts, which on an old Intel Mac are most of a short task.
//!
//! The pool is bounded by a number of guests and by memory, stays empty on
//! battery power, and lets guests go after some idle minutes. A guest whose
//! host went away (an agent restart) powers itself off after the same idle
//! limit. Should a backend not pass the control disk's new contents to the
//! guest (the guest never says it got the task), the pool turns itself off
//! and tasks boot as before.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tracing::{debug, info, warn};

use super::vmm::Vmm;
use crate::tempdir::TempDir;

/// How many warm guests at most, and how much memory they may take.
#[derive(Debug, Clone)]
pub struct WarmConfig {
    /// Guests kept booted at most (0 turns the pool off).
    pub max: usize,
    /// Memory all warm guests together may be given, in MiB (0: a quarter
    /// of the machine's memory).
    pub max_mem_mib: u64,
    /// How long a warm guest waits for a task before it is let go.
    pub idle: Duration,
}

impl Default for WarmConfig {
    fn default() -> Self {
        Self {
            max: DEFAULT_WARM_VMS,
            max_mem_mib: 0,
            idle: DEFAULT_IDLE,
        }
    }
}

/// Warm guests by default.
pub const DEFAULT_WARM_VMS: usize = 2;
/// How long a warm guest stays without a task.
pub const DEFAULT_IDLE: Duration = Duration::from_secs(10 * 60);
/// How long a guest has to boot and say it is ready.
pub const READY_TIMEOUT: Duration = Duration::from_secs(90);
/// How long a ready guest has to say it got the task.
pub const TAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// What a warm guest was booted for: a task fits it when it runs the same
/// image with the same CPUs and memory.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub digest: String,
    pub vcpus: u32,
    pub mem_mib: u64,
}

/// A booted guest waiting for its task, and its disks.
pub struct WarmVm {
    pub key: PoolKey,
    pub vmm: Vmm,
    pub running: super::RunningVm,
    /// Holds the guest's disks; removed with it.
    pub dir: TempDir,
    pub control: PathBuf,
    pub output: PathBuf,
    pub spawned: Instant,
}

impl WarmVm {
    fn alive(&mut self) -> bool {
        matches!(self.running.child.try_wait(), Ok(None))
    }
}

#[derive(Default)]
struct State {
    vms: Vec<WarmVm>,
    /// Guests being booted.
    booting: Vec<PoolKey>,
    /// Why the pool turned itself off, if it did.
    broken: Option<String>,
    on_battery: bool,
}

/// The warm guests of one executor.
pub struct WarmPool {
    pub config: WarmConfig,
    /// Memory warm guests may take together, in MiB.
    mem_budget: u64,
    state: Mutex<State>,
}

impl WarmPool {
    pub fn new(config: WarmConfig, machine_mem_mib: u64) -> Arc<Self> {
        let mem_budget = if config.max_mem_mib > 0 {
            config.max_mem_mib
        } else {
            machine_mem_mib / 4
        };
        Arc::new(Self {
            config,
            mem_budget,
            state: Mutex::default(),
        })
    }

    /// Whether warm guests are used at all here.
    pub fn enabled(&self) -> bool {
        self.config.max > 0 && self.mem_budget > 0 && self.state.lock().unwrap().broken.is_none()
    }

    /// A live warm guest for `key`, the oldest first.
    pub fn take(&self, key: &PoolKey) -> Option<WarmVm> {
        let mut st = self.state.lock().unwrap();
        loop {
            let i = st.vms.iter().position(|v| v.key == *key)?;
            let mut vm = st.vms.remove(i);
            if vm.alive() {
                return Some(vm);
            }
            debug!(digest = %key.digest, "a warm microVM had stopped; dropping it");
        }
    }

    /// Whether another guest for `key` may boot now; if so it counts as
    /// booting until [`Self::booted`] or [`Self::boot_failed`]. Makes room
    /// by letting go of the oldest guest of another image that waited a
    /// while.
    pub fn may_boot(&self, key: &PoolKey) -> bool {
        let mut st = self.state.lock().unwrap();
        if st.broken.is_some() || st.on_battery || self.config.max == 0 {
            return false;
        }
        // One spare per image and size at a time: a task takes it and its
        // replacement boots while the task runs.
        let spare = st.vms.iter().filter(|v| v.key == *key).count()
            + st.booting.iter().filter(|k| *k == key).count();
        if spare >= 1 {
            return false;
        }
        loop {
            let count = st.vms.len() + st.booting.len();
            let mem: u64 = st
                .vms
                .iter()
                .map(|v| v.key.mem_mib)
                .chain(st.booting.iter().map(|k| k.mem_mib))
                .sum();
            if count < self.config.max && mem + key.mem_mib <= self.mem_budget {
                st.booting.push(key.clone());
                return true;
            }
            let evict = st
                .vms
                .iter()
                .enumerate()
                .filter(|(_, v)| v.key != *key && v.spawned.elapsed() > Duration::from_secs(30))
                .min_by_key(|(_, v)| v.spawned)
                .map(|(i, _)| i);
            match evict {
                Some(i) => {
                    let v = st.vms.remove(i);
                    debug!(digest = %v.key.digest, "letting a warm microVM go to make room");
                }
                None => return false,
            }
        }
    }

    pub fn booted(&self, vm: WarmVm) {
        let mut st = self.state.lock().unwrap();
        if let Some(i) = st.booting.iter().position(|k| *k == vm.key) {
            st.booting.remove(i);
        }
        if st.broken.is_none() && !st.on_battery {
            st.vms.push(vm);
        }
    }

    pub fn boot_failed(&self, key: &PoolKey) {
        let mut st = self.state.lock().unwrap();
        if let Some(i) = st.booting.iter().position(|k| k == key) {
            st.booting.remove(i);
        }
    }

    /// Turns the pool off for good (this agent run): warm guests do not
    /// work here.
    pub fn break_with(&self, why: String) {
        let mut st = self.state.lock().unwrap();
        if st.broken.is_none() {
            warn!(
                "warm microVMs are off on this machine: {why}. Tasks boot their own VM as before"
            );
            st.broken = Some(why);
        }
        st.vms.clear();
    }

    /// Lets go of guests that waited too long or stopped, and of all of
    /// them on battery power.
    pub fn tidy(&self, on_battery: bool) {
        let mut st = self.state.lock().unwrap();
        if on_battery && !st.on_battery && !st.vms.is_empty() {
            info!(
                count = st.vms.len(),
                "on battery power: stopping the warm microVMs"
            );
        }
        st.on_battery = on_battery;
        if on_battery {
            st.vms.clear();
            return;
        }
        let idle = self.config.idle;
        st.vms
            .retain_mut(|v| v.alive() && v.spawned.elapsed() < idle);
    }

    /// How many guests are warm now, and how many are booting.
    pub fn counts(&self) -> (usize, usize) {
        let st = self.state.lock().unwrap();
        (st.vms.len(), st.booting.len())
    }
}

/// Keeps the pool tidy until it is dropped.
pub fn spawn_tidier(pool: &Arc<WarmPool>) {
    let weak = Arc::downgrade(pool);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(20)).await;
            let Some(pool) = weak.upgrade() else { return };
            if pool.config.max == 0 {
                return;
            }
            let on_battery = tokio::task::spawn_blocking(|| {
                use crate::availability::Probe;
                crate::availability::SystemProbe.on_battery() == Some(true)
            })
            .await
            .unwrap_or(false);
            pool.tidy(on_battery);
        }
    });
}

/// Whether the console at `path` has a line with `needle` (whole line,
/// after the guest's prefix), reading from `*seen` on.
pub fn console_has(path: &Path, needle: &str, seen: &mut u64) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    let from = seen.saturating_sub(256);
    if f.seek(SeekFrom::Start(from)).is_err() {
        return false;
    }
    let mut new = Vec::new();
    let _ = f.read_to_end(&mut new);
    *seen = from + new.len() as u64;
    String::from_utf8_lossy(&new)
        .lines()
        .any(|l| l.trim_end_matches('\r').trim_end() == needle)
}

/// The file in a warm guest's directory that names its VMM process.
pub const PID_FILE: &str = "vmm.pid";

/// Stops the warm guests an earlier run of this agent left (it restarted
/// without stopping them): each one's directory under `run_dir` names its
/// VMM, which is killed when its command line still points into that
/// directory (so a reused process id is left alone).
pub fn kill_stale(run_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(run_dir) else {
        return;
    };
    let mut stale = Vec::new();
    for e in entries.flatten() {
        let dir = e.path();
        let Ok(pid) = std::fs::read_to_string(dir.join(PID_FILE)) else {
            continue;
        };
        if let Ok(pid) = pid.trim().parse::<u32>() {
            stale.push((sysinfo::Pid::from_u32(pid), dir));
        }
    }
    if stale.is_empty() {
        return;
    }
    let mut sys = sysinfo::System::new();
    let pids: Vec<sysinfo::Pid> = stale.iter().map(|(p, _)| *p).collect();
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&pids),
        true,
        sysinfo::ProcessRefreshKind::nothing().with_cmd(sysinfo::UpdateKind::Always),
    );
    for (pid, dir) in stale {
        let Some(p) = sys.process(pid) else { continue };
        let needle = dir.to_string_lossy().into_owned();
        if p.cmd()
            .iter()
            .any(|a| a.to_string_lossy().contains(needle.as_str()))
        {
            info!(
                pid = pid.as_u32(),
                "stopping a warm microVM left by an earlier run"
            );
            p.kill();
        }
    }
}

/// The machine's memory in MiB.
pub fn machine_mem_mib() -> u64 {
    let mut s = sysinfo::System::new();
    s.refresh_memory();
    s.total_memory() >> 20
}

/// Every key in `pool`'s warm guests (tests).
#[cfg(test)]
fn keys(pool: &WarmPool) -> std::collections::HashMap<PoolKey, usize> {
    let st = pool.state.lock().unwrap();
    let mut m = std::collections::HashMap::new();
    for v in &st.vms {
        *m.entry(v.key.clone()).or_default() += 1;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(d: &str, mem: u64) -> PoolKey {
        PoolKey {
            digest: d.into(),
            vcpus: 1,
            mem_mib: mem,
        }
    }

    #[test]
    fn budget_counts_booting_guests_and_one_spare_per_image() {
        let pool = WarmPool::new(
            WarmConfig {
                max: 2,
                max_mem_mib: 1024,
                idle: DEFAULT_IDLE,
            },
            16 << 10,
        );
        assert!(pool.enabled());
        assert!(pool.may_boot(&key("a", 512)));
        // One spare per image and size.
        assert!(!pool.may_boot(&key("a", 512)));
        assert!(pool.may_boot(&key("b", 512)));
        assert_eq!(pool.counts(), (0, 2));
        // Two booting: no third, and nothing old enough to evict.
        assert!(!pool.may_boot(&key("c", 256)));
        pool.boot_failed(&key("b", 512));
        // Memory: 512 booting + 768 is over 1024.
        assert!(!pool.may_boot(&key("c", 768)));
        assert!(pool.may_boot(&key("c", 512)));
        assert!(keys(&pool).is_empty());
    }

    #[test]
    fn battery_and_breakage_turn_it_off() {
        let pool = WarmPool::new(WarmConfig::default(), 8 << 10);
        pool.tidy(true);
        assert!(!pool.may_boot(&key("a", 256)));
        pool.tidy(false);
        assert!(pool.may_boot(&key("a", 256)));
        pool.break_with("test".into());
        assert!(!pool.enabled());
        assert!(!pool.may_boot(&key("b", 256)));
        let off = WarmPool::new(
            WarmConfig {
                max: 0,
                ..Default::default()
            },
            8 << 10,
        );
        assert!(!off.enabled());
    }

    #[test]
    fn console_lines_are_found_once_written() {
        let dir = TempDir::new("pool-console").unwrap();
        let path = dir.path().join("console.log");
        let mut seen = 0;
        assert!(!console_has(
            &path,
            pumasi_guest::POOL_READY_LINE,
            &mut seen
        ));
        std::fs::write(&path, "[    0.1] Linux\r\npumasi-guest: readyish\n").unwrap();
        assert!(!console_has(
            &path,
            pumasi_guest::POOL_READY_LINE,
            &mut seen
        ));
        std::fs::write(
            &path,
            "[    0.1] Linux\r\npumasi-guest: readyish\npumasi-guest: ready\r\n",
        )
        .unwrap();
        assert!(console_has(&path, pumasi_guest::POOL_READY_LINE, &mut seen));
    }
}
