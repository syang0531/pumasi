//! What each run drew (1.22): the agent integrates the power its sensors
//! read over every run, and the run's share goes in the report's tail
//! ([`pumasi_proto::msg::ReportTail::energy`]).
//!
//! Every metrics sample (5 s) gives the CPU package's power and each GPU's.
//! A sample's power stands for the interval since the previous one: RAPL,
//! the SMC, IOReport and hwmon's energy counters already give the mean over
//! that interval, and NVML's reading is close to it. The CPU's energy over
//! an interval is shared among the runs by the CPU time each used in it;
//! when the machine was busier than its runs (other programs), the runs get
//! only their part. A run whose CPU time cannot be read while it runs
//! (WASM) counts as one busy core. A GPU's energy goes to the runs holding
//! that GPU, in equal parts. Intervals longer than [`MAX_GAP`] (the agent
//! was disconnected, the machine slept) are skipped.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pumasi_proto::{attempt::RunUse, energy::RunEnergy, msg::TaskRef};

/// The longest interval between two samples that is still integrated.
pub const MAX_GAP: Duration = Duration::from_secs(30);

/// Where a running run's CPU time can be read: the usage sampler of its
/// container or microVM attaches its figures here ([`crate::usage`]).
#[derive(Debug, Clone, Default)]
pub struct CpuFeed(Arc<Mutex<Option<Arc<Mutex<RunUse>>>>>);

impl CpuFeed {
    pub fn attach(&self, shared: Arc<Mutex<RunUse>>) {
        *self.0.lock().unwrap() = Some(shared);
    }

    /// The run's CPU time so far, when a sampler feeds it.
    pub fn cpu_ms(&self) -> Option<u64> {
        let inner = self.0.lock().unwrap().clone()?;
        let u = inner.lock().unwrap();
        u.cpu_ms
    }
}

/// One power reading of the machine.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PowerSample {
    /// The CPU package(s), when the machine reads it.
    pub cpu_w: Option<f64>,
    /// Each GPU's power, by its index.
    pub gpu_w: Vec<(u32, f64)>,
    /// How many cores the whole machine kept busy (its CPU use times its
    /// logical CPUs), when known.
    pub busy_cores: Option<f64>,
}

struct Run {
    gpus: Vec<u32>,
    feed: Option<CpuFeed>,
    /// CPU time at the last sample (fed runs).
    last_cpu_ms: Option<u64>,
    cpu_j: f64,
    gpu_j: f64,
    cpu_seen: bool,
    gpu_seen: bool,
    /// Its share of the CPU's power and its GPUs' power in the last
    /// interval, for the part after the last sample when it ends.
    last_cpu_share_w: Option<f64>,
    last_gpu_w: Option<f64>,
}

#[derive(Default)]
struct Inner {
    runs: HashMap<TaskRef, Run>,
    last: Option<(Instant, PowerSample)>,
}

/// Integrates power over runs. Cheap to clone; shared by the metrics loop
/// and the runs.
#[derive(Clone, Default)]
pub struct Meter(Arc<Mutex<Inner>>);

impl Meter {
    /// A run started, holding these GPUs (their indexes). `feed`: where
    /// its CPU time can be read while it runs; without one it counts as
    /// one busy core.
    pub fn start(&self, task: TaskRef, gpus: Vec<u32>, feed: Option<CpuFeed>) {
        let last_cpu_ms = feed.as_ref().and_then(|f| f.cpu_ms());
        self.0.lock().unwrap().runs.insert(
            task,
            Run {
                gpus,
                feed,
                last_cpu_ms,
                cpu_j: 0.0,
                gpu_j: 0.0,
                cpu_seen: false,
                gpu_seen: false,
                last_cpu_share_w: None,
                last_gpu_w: None,
            },
        );
    }

    /// A new reading taken at `at`: the interval since the previous one is
    /// shared among the runs.
    pub fn sample(&self, at: Instant, s: PowerSample) {
        let mut inner = self.0.lock().unwrap();
        let prev = inner.last.replace((at, s.clone()));
        let Some((then, _)) = prev else {
            for r in inner.runs.values_mut() {
                r.last_cpu_ms = r.feed.as_ref().and_then(|f| f.cpu_ms()).or(r.last_cpu_ms);
            }
            return;
        };
        let dt = at.saturating_duration_since(then);
        let secs = dt.as_secs_f64();
        // CPU time each run used in the interval, in core-seconds.
        let mut used: Vec<(TaskRef, f64)> = Vec::new();
        for (t, r) in inner.runs.iter_mut() {
            let now = r.feed.as_ref().and_then(|f| f.cpu_ms());
            let cores = match (r.feed.is_some(), now, r.last_cpu_ms) {
                (true, Some(n), Some(l)) => n.saturating_sub(l) as f64 / 1000.0,
                // Fed, but nothing read yet: it has not started using CPU
                // that the sampler saw.
                (true, Some(_), None) | (true, None, _) => 0.0,
                (false, _, _) => secs,
            };
            if now.is_some() {
                r.last_cpu_ms = now;
            }
            used.push((*t, cores));
        }
        if dt > MAX_GAP || secs <= 0.0 {
            return;
        }
        let runs_cores: f64 = used.iter().map(|(_, c)| c).sum();
        let machine_cores = s.busy_cores.map(|b| b * secs).unwrap_or(0.0);
        let denom = runs_cores.max(machine_cores);
        // Runs per GPU.
        let mut holders: HashMap<u32, u32> = HashMap::new();
        for r in inner.runs.values() {
            for g in &r.gpus {
                *holders.entry(*g).or_default() += 1;
            }
        }
        for (t, cores) in used {
            let r = inner.runs.get_mut(&t).expect("listed above");
            if let Some(w) = s.cpu_w {
                let share = if denom > 0.0 { cores / denom } else { 0.0 };
                r.cpu_j += w * secs * share;
                r.cpu_seen = true;
                r.last_cpu_share_w = Some(w * share);
            } else {
                r.last_cpu_share_w = None;
            }
            let mut gw = None;
            for g in &r.gpus {
                if let Some((_, w)) = s.gpu_w.iter().find(|(i, _)| i == g) {
                    let part = w / holders.get(g).copied().unwrap_or(1).max(1) as f64;
                    *gw.get_or_insert(0.0) += part;
                }
            }
            if let Some(w) = gw {
                r.gpu_j += w * secs;
                r.gpu_seen = true;
            }
            r.last_gpu_w = gw;
        }
    }

    /// A run ended: what it drew, with the time since the last sample at
    /// the last sample's power. `None` when nothing was measured.
    pub fn finish(&self, task: TaskRef) -> Option<RunEnergy> {
        self.finish_at(task, Instant::now())
    }

    fn finish_at(&self, task: TaskRef, at: Instant) -> Option<RunEnergy> {
        let mut inner = self.0.lock().unwrap();
        let mut r = inner.runs.remove(&task)?;
        let since = inner
            .last
            .as_ref()
            .map(|(t, _)| at.saturating_duration_since(*t))
            .filter(|d| *d <= MAX_GAP);
        if let Some(d) = since {
            let secs = d.as_secs_f64();
            match r.last_cpu_share_w {
                Some(w) => {
                    r.cpu_j += w * secs;
                    r.cpu_seen = true;
                }
                // A run shorter than a sample: one core's part of the last
                // reading.
                None => {
                    if let Some((_, s)) = &inner.last
                        && let Some(w) = s.cpu_w
                    {
                        let busy = s.busy_cores.unwrap_or(1.0).max(1.0);
                        r.cpu_j += w * secs / busy;
                        r.cpu_seen = true;
                    }
                }
            }
            if let Some(w) = r.last_gpu_w {
                r.gpu_j += w * secs;
                r.gpu_seen = true;
            } else if let Some((_, s)) = &inner.last {
                let gw: f64 = r
                    .gpus
                    .iter()
                    .filter_map(|g| s.gpu_w.iter().find(|(i, _)| i == g).map(|(_, w)| *w))
                    .sum();
                if r.gpus.iter().any(|g| s.gpu_w.iter().any(|(i, _)| i == g)) {
                    r.gpu_j += gw * secs;
                    r.gpu_seen = true;
                }
            }
        }
        let mwh = |j: f64| (j / 3.6).round().max(0.0) as u64;
        let e = RunEnergy {
            cpu_mwh: r.cpu_seen.then(|| mwh(r.cpu_j)),
            gpu_mwh: r.gpu_seen.then(|| mwh(r.gpu_j)),
        };
        (!e.is_empty()).then_some(e)
    }

    /// Forgets a run that ended without a report.
    pub fn forget(&self, task: TaskRef) {
        self.0.lock().unwrap().runs.remove(&task);
    }
}

/// Watt-hours of a recorded trace of (seconds, watts) readings, each
/// reading standing for the interval before it, as [`Meter`] integrates.
pub fn trace_wh(trace: &[(f64, f64)]) -> f64 {
    trace
        .windows(2)
        .map(|w| {
            let dt = w[1].0 - w[0].0;
            if dt <= 0.0 || dt > MAX_GAP.as_secs_f64() {
                0.0
            } else {
                w[1].1 * dt
            }
        })
        .sum::<f64>()
        / 3600.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::job::JobId;

    fn task(i: u32) -> TaskRef {
        TaskRef {
            job: JobId(1),
            index: i,
            attempt: 1,
            check: false,
        }
    }

    fn feed(cpu_ms: u64) -> (CpuFeed, Arc<Mutex<RunUse>>) {
        let f = CpuFeed::default();
        let shared = Arc::new(Mutex::new(RunUse {
            cpu_ms: Some(cpu_ms),
            ..Default::default()
        }));
        f.attach(shared.clone());
        (f, shared)
    }

    /// A recorded RAPL trace (5 s apart) of a machine running one task:
    /// idle, a ramp, a plateau, and back.
    const TRACE: &[f64] = &[
        11.8, 12.1, 35.4, 64.9, 65.2, 64.7, 65.0, 66.1, 65.8, 64.9, 65.3, 40.2, 13.0, 12.2,
    ];

    #[test]
    fn a_recorded_trace_integrates_to_its_watt_hours() {
        let m = Meter::default();
        let t0 = Instant::now();
        // The task is the only thing busy and uses one full core per
        // second of the trace (its feed moves by 5000 ms per sample).
        let (f, shared) = feed(0);
        m.start(task(0), vec![], Some(f));
        let mut points = Vec::new();
        for (i, w) in TRACE.iter().enumerate() {
            let secs = i as f64 * 5.0;
            shared.lock().unwrap().cpu_ms = Some(i as u64 * 5000);
            m.sample(
                t0 + Duration::from_secs_f64(secs),
                PowerSample {
                    cpu_w: Some(*w),
                    gpu_w: vec![],
                    busy_cores: Some(1.0),
                },
            );
            points.push((secs, *w));
        }
        let expected: f64 = TRACE[1..].iter().map(|w| w * 5.0).sum::<f64>() / 3600.0;
        assert!((trace_wh(&points) - expected).abs() < 1e-9);
        let end = t0 + Duration::from_secs_f64((TRACE.len() - 1) as f64 * 5.0);
        let e = m.finish_at(task(0), end).unwrap();
        let got = e.cpu_mwh.unwrap() as f64 / 1000.0;
        assert!(
            (got - expected).abs() < 0.001,
            "{got} Wh, expected {expected} Wh"
        );
        assert_eq!(e.gpu_mwh, None);
        // 3174 J: 0.88 Wh in all.
        assert!((expected - 0.88167).abs() < 0.0001, "{expected}");
    }

    #[test]
    fn the_cpu_is_shared_by_cpu_time_and_gpus_by_holders() {
        let m = Meter::default();
        let t0 = Instant::now();
        let (fa, a) = feed(0);
        let (fb, b) = feed(0);
        m.start(task(0), vec![0], Some(fa));
        m.start(task(1), vec![0, 1], Some(fb));
        m.sample(t0, PowerSample::default());
        // In 10 s, a used 3 core-seconds and b 1; the machine was busy for
        // 8 (others used 4). The CPU drew 100 W, GPU 0 200 W, GPU 1 50 W.
        a.lock().unwrap().cpu_ms = Some(3000);
        b.lock().unwrap().cpu_ms = Some(1000);
        let at = t0 + Duration::from_secs(10);
        m.sample(
            at,
            PowerSample {
                cpu_w: Some(100.0),
                gpu_w: vec![(0, 200.0), (1, 50.0)],
                busy_cores: Some(0.8),
            },
        );
        let ea = m.finish_at(task(0), at).unwrap();
        let eb = m.finish_at(task(1), at).unwrap();
        // 1000 J of CPU: a 3/8, b 1/8.
        assert_eq!(ea.cpu_mwh, Some((375.0f64 / 3.6).round() as u64));
        assert_eq!(eb.cpu_mwh, Some((125.0f64 / 3.6).round() as u64));
        // GPU 0's 2000 J split in two; b also has GPU 1's 500 J.
        assert_eq!(ea.gpu_mwh, Some((1000.0f64 / 3.6).round() as u64));
        assert_eq!(eb.gpu_mwh, Some((1500.0f64 / 3.6).round() as u64));
    }

    #[test]
    fn gaps_are_skipped_and_unfed_runs_count_one_core() {
        let m = Meter::default();
        let t0 = Instant::now();
        m.start(task(0), vec![], None);
        let s = |w| PowerSample {
            cpu_w: Some(w),
            gpu_w: vec![],
            busy_cores: Some(2.0),
        };
        m.sample(t0, s(40.0));
        // 5 s with the machine two cores busy: half of 40 W.
        m.sample(t0 + Duration::from_secs(5), s(40.0));
        // The machine slept for a minute: not counted.
        m.sample(t0 + Duration::from_secs(65), s(40.0));
        let e = m.finish_at(task(0), t0 + Duration::from_secs(65)).unwrap();
        assert_eq!(e.cpu_mwh, Some((100.0f64 / 3.6).round() as u64));
        // Nothing to read, nothing reported.
        m.start(task(1), vec![], None);
        let none = Meter::default();
        none.start(task(1), vec![], None);
        assert_eq!(none.finish(task(1)), None);
    }

    #[test]
    fn a_run_shorter_than_a_sample_gets_the_last_reading() {
        let m = Meter::default();
        let t0 = Instant::now();
        m.sample(
            t0,
            PowerSample {
                cpu_w: Some(36.0),
                gpu_w: vec![(0, 72.0)],
                busy_cores: Some(1.0),
            },
        );
        m.start(task(0), vec![0], None);
        let e = m.finish_at(task(0), t0 + Duration::from_secs(2)).unwrap();
        // 2 s of 36 W and of 72 W.
        assert_eq!(e.cpu_mwh, Some(20));
        assert_eq!(e.gpu_mwh, Some(40));
    }
}
