//! Retries, reruns, suspended jobs, periodic checkpoints and what runs use
//! (1.21).
//!
//! A job file asks for retries (`retries`, `retry_on`, `retry_delay`) and
//! periodic checkpoints (`checkpoint_every`); they travel in
//! [`crate::job::JobExt::run`] ([`JobRun`]). The control plane keeps every
//! attempt of a task and why it ended ([`AttemptView`]), with what the run
//! used as its node measured it ([`RunUse`], sent after the report in
//! [`crate::msg::ReportTail::usage`]).

use std::fmt;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::economy::MilliCu;
use crate::job::{Limits, format_duration_ms, parse_duration_ms};
use crate::msg::Outcome;
use crate::store::format_size;

/// The most retries a task may have.
pub const MAX_RETRIES: u32 = 100;
/// The first retry's delay when a job gives none.
pub const DEFAULT_RETRY_DELAY_MS: u64 = 10_000;
/// The delay doubles after every retry, up to this.
pub const MAX_RETRY_DELAY_MS: u64 = 3_600_000;
/// The shortest and longest `checkpoint_every`.
pub const MIN_CHECKPOINT_EVERY_MS: u64 = 10_000;
pub const MAX_CHECKPOINT_EVERY_MS: u64 = 86_400_000;
/// The file the agent puts in a task's `/checkpoint` to ask it for a
/// checkpoint while it runs: the task saves its state there and deletes
/// the file, and the agent stores what `/checkpoint` then holds.
pub const CHECKPOINT_REQUEST_FILE: &str = ".pumasi-checkpoint";
/// The environment variable that tells a task of a job with retries how
/// many times it was retried (0 on the first run).
pub const RETRY_ENV: &str = "PUMASI_RETRY";
/// How many attempts of a task the control plane remembers.
pub const MAX_ATTEMPT_RECORDS: usize = 50;

/// When a failed run is tried again (`retry_on`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryOn {
    /// It exited with any code but 0, or crashed (a WASM trap).
    Exit,
    /// It exited with this code (`exit:75`).
    ExitCode(i32),
    /// It ran out of memory.
    Oom,
    /// It ran past its timeout.
    Timeout,
    /// The node could not run it (pulling the image failed, ...).
    Error,
}

impl RetryOn {
    /// What `retry_on` means when a job leaves it out.
    pub const DEFAULT: [RetryOn; 4] = [
        RetryOn::Exit,
        RetryOn::Oom,
        RetryOn::Timeout,
        RetryOn::Error,
    ];
}

impl fmt::Display for RetryOn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryOn::Exit => f.write_str("exit"),
            RetryOn::ExitCode(c) => write!(f, "exit:{c}"),
            RetryOn::Oom => f.write_str("oom"),
            RetryOn::Timeout => f.write_str("timeout"),
            RetryOn::Error => f.write_str("error"),
        }
    }
}

impl std::str::FromStr for RetryOn {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim().to_ascii_lowercase();
        Ok(match s.as_str() {
            "exit" => RetryOn::Exit,
            "oom" => RetryOn::Oom,
            "timeout" => RetryOn::Timeout,
            "error" => RetryOn::Error,
            _ => match s.strip_prefix("exit:") {
                Some(code) => RetryOn::ExitCode(
                    code.parse()
                        .with_context(|| format!("bad exit code in retry_on {s:?}"))?,
                ),
                None => bail!("unknown retry_on {s:?} (exit, exit:CODE, oom, timeout or error)"),
            },
        })
    }
}

/// How a run ended, for retries and the attempt list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndKind {
    Succeeded,
    /// Exited with this code (not 0).
    Exit(i32),
    Oom,
    Timeout,
    OutOfFuel,
    /// A WASM trap other than memory.
    Crashed,
    /// The node could not run it.
    Error,
    Cancelled,
    /// Its node went away, or its report was refused.
    Lost,
    /// Stopped to make room, to move, or because its job was suspended:
    /// it does not count against the job's attempts.
    Moved,
}

impl EndKind {
    pub fn of(o: &Outcome) -> EndKind {
        match o {
            Outcome::Exited(0) => EndKind::Succeeded,
            Outcome::Exited(c) => EndKind::Exit(*c),
            Outcome::OutOfFuel => EndKind::OutOfFuel,
            Outcome::Timeout => EndKind::Timeout,
            Outcome::Trap(m) if is_oom(m) => EndKind::Oom,
            Outcome::Trap(_) => EndKind::Crashed,
            Outcome::Error(_) => EndKind::Error,
            Outcome::Cancelled => EndKind::Cancelled,
        }
    }
}

impl fmt::Display for EndKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EndKind::Succeeded => f.write_str("ok"),
            EndKind::Exit(c) => write!(f, "exit {c}"),
            EndKind::Oom => f.write_str("out of memory"),
            EndKind::Timeout => f.write_str("timeout"),
            EndKind::OutOfFuel => f.write_str("out of fuel"),
            EndKind::Crashed => f.write_str("crashed"),
            EndKind::Error => f.write_str("error"),
            EndKind::Cancelled => f.write_str("cancelled"),
            EndKind::Lost => f.write_str("lost"),
            EndKind::Moved => f.write_str("moved"),
        }
    }
}

/// Whether a trap message means the run ran out of memory: Docker's
/// OOM kill and the WASM engine's memory limit.
fn is_oom(m: &str) -> bool {
    let m = m.to_ascii_lowercase();
    m.contains("out of memory") || m.contains("memory limit")
}

/// `retries = 3`, `retry_on = ["exit:75", "oom"]`, `retry_delay = "30s"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub retries: u32,
    /// Empty means [`RetryOn::DEFAULT`].
    pub on: Vec<RetryOn>,
    /// Before the first retry; it doubles after each.
    pub delay_ms: u64,
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_RETRIES).contains(&self.retries),
            "retries must be between 1 and {MAX_RETRIES}"
        );
        ensure!(
            self.delay_ms <= MAX_RETRY_DELAY_MS,
            "retry_delay must be at most {}",
            format_duration_ms(MAX_RETRY_DELAY_MS)
        );
        ensure!(self.on.len() <= 32, "retry_on lists at most 32 reasons");
        Ok(())
    }

    /// Whether a run that ended so is tried again.
    pub fn retries_on(&self, end: EndKind) -> bool {
        let on: &[RetryOn] = if self.on.is_empty() {
            &RetryOn::DEFAULT
        } else {
            &self.on
        };
        on.iter().any(|r| match (r, end) {
            (RetryOn::Exit, EndKind::Exit(_) | EndKind::Crashed) => true,
            (RetryOn::ExitCode(c), EndKind::Exit(e)) => *c == e,
            (RetryOn::Oom, EndKind::Oom) => true,
            (RetryOn::Timeout, EndKind::Timeout) => true,
            (RetryOn::Error, EndKind::Error) => true,
            _ => false,
        })
    }

    /// How long to wait before retry number `n` (from 1).
    pub fn delay_before(&self, n: u32) -> u64 {
        let doublings = n.saturating_sub(1).min(32);
        self.delay_ms
            .saturating_mul(1u64 << doublings)
            .min(MAX_RETRY_DELAY_MS.max(self.delay_ms))
    }

    /// `2 retries on exit, oom (delay 10s)`.
    pub fn describe(&self) -> String {
        let on: Vec<String> = if self.on.is_empty() {
            RetryOn::DEFAULT.iter().map(|r| r.to_string()).collect()
        } else {
            self.on.iter().map(|r| r.to_string()).collect()
        };
        format!(
            "{} retr{} on {} (delay {}, doubling)",
            self.retries,
            if self.retries == 1 { "y" } else { "ies" },
            on.join(", "),
            format_duration_ms(self.delay_ms)
        )
    }
}

/// What a job asks of its runs beyond its limits (1.21).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRun {
    pub retry: Option<RetryPolicy>,
    /// Ask the task for a checkpoint this often while it runs.
    pub checkpoint_every_ms: Option<u64>,
}

impl JobRun {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// `checkpoints` is whether the job keeps checkpoints at all; `gang`
    /// whether it starts its tasks together.
    pub fn validate(&self, checkpoints: bool, gang: bool) -> Result<()> {
        if let Some(r) = &self.retry {
            r.validate()?;
            ensure!(
                !gang,
                "a gang cannot have retries yet: its tasks start again together already when one is lost"
            );
        }
        if let Some(ms) = self.checkpoint_every_ms {
            ensure!(
                (MIN_CHECKPOINT_EVERY_MS..=MAX_CHECKPOINT_EVERY_MS).contains(&ms),
                "checkpoint_every must be between {} and {}",
                format_duration_ms(MIN_CHECKPOINT_EVERY_MS),
                format_duration_ms(MAX_CHECKPOINT_EVERY_MS)
            );
            ensure!(checkpoints, "checkpoint_every needs checkpoints");
        }
        Ok(())
    }

    /// Reads the job file's fields.
    pub fn from_file(
        retries: Option<u32>,
        retry_on: &[String],
        retry_delay: Option<&str>,
        checkpoint_every: Option<&str>,
    ) -> Result<JobRun> {
        let retry = match retries {
            None | Some(0) => {
                ensure!(
                    retry_on.is_empty() && retry_delay.is_none(),
                    "retry_on and retry_delay need retries"
                );
                None
            }
            Some(n) => {
                let mut on = retry_on
                    .iter()
                    .map(|s| s.parse::<RetryOn>())
                    .collect::<Result<Vec<_>>>()?;
                on.dedup();
                Some(RetryPolicy {
                    retries: n,
                    on,
                    delay_ms: retry_delay
                        .map(parse_duration_ms)
                        .transpose()
                        .context("retry_delay")?
                        .unwrap_or(DEFAULT_RETRY_DELAY_MS),
                })
            }
        };
        let checkpoint_every_ms = checkpoint_every
            .map(parse_duration_ms)
            .transpose()
            .context("checkpoint_every")?;
        Ok(JobRun {
            retry,
            checkpoint_every_ms,
        })
    }

    /// `3 retries on exit (delay 10s, doubling), checkpoint every 15m`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(r) = &self.retry {
            parts.push(r.describe());
        }
        if let Some(ms) = self.checkpoint_every_ms {
            parts.push(format!("checkpoint every {}", format_duration_ms(ms)));
        }
        parts.join(", ")
    }
}

/// What one run used, as its node measured it: Linux cgroups for Docker,
/// the VMM process for microVMs, the engine for WASM. `None` where the
/// node could not tell.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunUse {
    /// CPU time, user and system.
    pub cpu_ms: Option<u64>,
    pub peak_memory_bytes: Option<u64>,
    pub disk_read_bytes: Option<u64>,
    pub disk_write_bytes: Option<u64>,
    pub net_rx_bytes: Option<u64>,
    pub net_tx_bytes: Option<u64>,
}

impl RunUse {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// `cpu 12.4s  memory 310 MiB  disk 1.2 MiB read 40 MiB written  net 3 KiB in 1 KiB out`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(ms) = self.cpu_ms {
            parts.push(format!("cpu {}", format_cpu(ms)));
        }
        if let Some(b) = self.peak_memory_bytes {
            parts.push(format!("memory {}", format_size(b)));
        }
        match (self.disk_read_bytes, self.disk_write_bytes) {
            (None, None) => {}
            (r, w) => parts.push(format!(
                "disk {} read {} written",
                format_size(r.unwrap_or(0)),
                format_size(w.unwrap_or(0))
            )),
        }
        match (self.net_rx_bytes, self.net_tx_bytes) {
            (None, None) => {}
            (r, t) => parts.push(format!(
                "net {} in {} out",
                format_size(r.unwrap_or(0)),
                format_size(t.unwrap_or(0))
            )),
        }
        parts.join("  ")
    }
}

/// `850ms`, `12.4s`, `3m 05s`.
pub fn format_cpu(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{}m {:02}s", ms / 60_000, (ms / 1000) % 60)
    }
}

/// What a task asked for and did not use, when it is far more: under a
/// quarter of its memory (of at least 256 MiB), or of its CPUs.
pub fn overasked(limits: &Limits, wall_ms: u64, u: &RunUse) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(peak) = u.peak_memory_bytes
        && limits.memory_bytes >= 256 << 20
        && peak.saturating_mul(4) < limits.memory_bytes
    {
        out.push(format!(
            "asked for {} of memory and used at most {}",
            format_size(limits.memory_bytes),
            format_size(peak)
        ));
    }
    if let (Some(cpu), Some(asked)) = (u.cpu_ms, limits.cpu_millis)
        && asked >= 1000
        && wall_ms >= 10_000
    {
        // CPUs used on average, in thousandths.
        let used = cpu.saturating_mul(1000) / wall_ms.max(1);
        if used.saturating_mul(4) < asked as u64 {
            out.push(format!(
                "asked for {} CPUs and used {:.2} on average",
                asked as f64 / 1000.0,
                used as f64 / 1000.0
            ));
        }
    }
    out
}

/// One attempt of a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptView {
    pub attempt: u32,
    pub node: String,
    /// The node's name, when it has one.
    pub node_name: Option<String>,
    pub started_unix: u64,
    /// `None` while it runs.
    pub ended_unix: Option<u64>,
    pub end: Option<EndKind>,
    /// More about how it ended (the outcome, why it was lost or moved).
    pub why: String,
    pub wall_ms: u64,
    pub usage: Option<RunUse>,
    /// It ended in a retry.
    pub retried: bool,
    /// The first attempt after `pumasi job rerun`.
    pub rerun: bool,
}

/// A task's attempts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunsView {
    pub index: u32,
    pub attempts: Vec<AttemptView>,
    /// Retries used since the task was submitted or last rerun.
    pub retries: u32,
    /// When it goes back to the queue, while it waits to be retried.
    pub retry_unix: Option<u64>,
    /// What its current or last run asked for and did not use.
    pub overasked: Vec<String>,
}

/// A job's suspension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suspension {
    pub by: String,
    pub since_unix: u64,
}

/// A job's retries, suspension and every task's attempts
/// ([`crate::msg::AdminOp::JobRuns`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRunsView {
    pub run: JobRun,
    pub suspended: Option<Suspension>,
    /// Only tasks with an attempt.
    pub tasks: Vec<TaskRunsView>,
    /// Attempts left out of `tasks` because they are old.
    pub dropped: u32,
    /// What the job's runs used in all.
    pub total: RunUse,
    pub cost: MilliCu,
    /// What each attempt drew, where its node measured it: (task, attempt,
    /// energy) (1.22). Last, so a 1.21 client reads the rest.
    #[serde(default, deserialize_with = "crate::wire::trailing")]
    pub energy: Vec<(u32, u32, crate::energy::RunEnergy)>,
    /// The job's runs together.
    #[serde(default, deserialize_with = "crate::wire::trailing")]
    pub total_energy: crate::energy::RunEnergy,
}

impl JobRunsView {
    /// What one attempt drew.
    pub fn energy_of(&self, task: u32, attempt: u32) -> Option<&crate::energy::RunEnergy> {
        self.energy
            .iter()
            .find(|(t, a, _)| *t == task && *a == attempt)
            .map(|(_, _, e)| e)
    }
}

/// Which tasks `pumasi job rerun` runs again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RerunWhich {
    /// Every task that failed or was cancelled.
    Failed,
    /// These tasks, whatever they ended as.
    Tasks(Vec<u32>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_on_parses_and_matches() {
        let p = RetryPolicy {
            retries: 2,
            on: vec!["exit:75".parse().unwrap(), "oom".parse().unwrap()],
            delay_ms: 1000,
        };
        assert!(p.retries_on(EndKind::Exit(75)));
        assert!(!p.retries_on(EndKind::Exit(1)));
        assert!(p.retries_on(EndKind::of(&Outcome::Trap(
            "out of memory (limit 64 MiB)".into()
        ))));
        assert!(!p.retries_on(EndKind::Timeout));
        assert!(!p.retries_on(EndKind::Cancelled));
        let all = RetryPolicy { on: vec![], ..p };
        for e in [
            EndKind::Exit(1),
            EndKind::Crashed,
            EndKind::Oom,
            EndKind::Timeout,
            EndKind::Error,
        ] {
            assert!(all.retries_on(e), "{e}");
        }
        for e in [
            EndKind::Succeeded,
            EndKind::OutOfFuel,
            EndKind::Cancelled,
            EndKind::Moved,
        ] {
            assert!(!all.retries_on(e), "{e}");
        }
        assert!("exit:x".parse::<RetryOn>().is_err());
        assert!("sometimes".parse::<RetryOn>().is_err());
        assert_eq!("EXIT:3".parse::<RetryOn>().unwrap().to_string(), "exit:3");
    }

    #[test]
    fn delays_double_up_to_the_cap() {
        let p = RetryPolicy {
            retries: 50,
            on: vec![],
            delay_ms: 10_000,
        };
        assert_eq!(p.delay_before(1), 10_000);
        assert_eq!(p.delay_before(2), 20_000);
        assert_eq!(p.delay_before(3), 40_000);
        assert_eq!(p.delay_before(40), MAX_RETRY_DELAY_MS);
        let none = RetryPolicy { delay_ms: 0, ..p };
        assert_eq!(none.delay_before(5), 0);
    }

    #[test]
    fn job_run_from_file() {
        let r = JobRun::from_file(Some(3), &["timeout".into()], Some("5s"), Some("15m")).unwrap();
        assert_eq!(r.retry.as_ref().unwrap().delay_ms, 5000);
        assert_eq!(r.checkpoint_every_ms, Some(900_000));
        assert!(r.validate(true, false).is_ok());
        assert!(r.validate(false, false).is_err());
        assert!(r.validate(true, true).is_err());
        assert!(JobRun::from_file(None, &["exit".into()], None, None).is_err());
        let short = JobRun::from_file(None, &[], None, Some("1s")).unwrap();
        assert!(short.validate(true, false).is_err());
        assert!(
            JobRun::from_file(Some(0), &[], None, None)
                .unwrap()
                .is_empty()
        );
        assert!(
            JobRun::from_file(Some(MAX_RETRIES + 1), &[], None, None)
                .unwrap()
                .validate(false, false)
                .is_err()
        );
    }

    #[test]
    fn overasked_tasks_are_pointed_out() {
        let limits = Limits {
            memory_bytes: 4 << 30,
            fuel: None,
            timeout_ms: 60_000,
            cpu_millis: Some(4000),
        };
        let u = RunUse {
            cpu_ms: Some(6_000),
            peak_memory_bytes: Some(300 << 20),
            ..Default::default()
        };
        let hints = overasked(&limits, 20_000, &u);
        assert_eq!(hints.len(), 2, "{hints:?}");
        assert!(hints[0].contains("4.0 GiB"), "{}", hints[0]);
        assert!(hints[1].contains("0.30"), "{}", hints[1]);
        let busy = RunUse {
            cpu_ms: Some(70_000),
            peak_memory_bytes: Some(3 << 30),
            ..Default::default()
        };
        assert!(overasked(&limits, 20_000, &busy).is_empty());
    }
}
