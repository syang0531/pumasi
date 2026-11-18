//! Retries, reruns, suspended jobs, periodic checkpoints and what runs use
//! (1.21). Extends [`State`].
//!
//! Every placement of a task is an attempt ([`AttemptRecord`]), kept with
//! how it ended and what its node measured, in its own table. A failed run
//! of a job with retries goes back to the queue after a delay that doubles
//! each time, preferring another node; a retry does not count against
//! `max_attempts` (which is about lost nodes). A rerun puts finished tasks
//! back in the queue under the same job id. A suspended job's queued tasks
//! wait and its running ones are moved off their nodes the way preemption
//! moves them (gently, with a checkpoint, when the job keeps them).

use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::attempt::{
    AttemptView, EndKind, JobRun, JobRunsView, MAX_ATTEMPT_RECORDS, RerunWhich, RunUse, Suspension,
    TaskRunsView,
};
use pumasi_proto::energy::RunEnergy;
use pumasi_proto::job::JobId;
use pumasi_proto::monitor::EventLevel;
use pumasi_proto::msg::{Outcome, TaskRef, TaskState};
use pumasi_proto::store::StoredOutput;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{State, TaskTimes, now_unix};

/// A job's retries, periodic checkpoints and suspension, in its own table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRunRecord {
    pub run: JobRun,
    pub suspended: Option<Suspension>,
}

impl JobRunRecord {
    pub fn is_empty(&self) -> bool {
        self.run.is_empty() && self.suspended.is_none()
    }
}

/// One placement of a task. The layout is stored: add fields only in a
/// new record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub attempt: u32,
    pub node: String,
    pub started_unix: u64,
    pub ended_unix: Option<u64>,
    pub end: Option<EndKind>,
    pub why: String,
    pub wall_ms: u64,
    pub usage: Option<RunUse>,
    pub retried: bool,
    pub rerun: bool,
    /// The task's moves (placements that do not count) when it started:
    /// more when it ends means it was moved, not lost.
    pub moves_at_start: u32,
}

/// A task's attempts and retries. The layout is stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunsRecord {
    /// The newest [`MAX_ATTEMPT_RECORDS`].
    pub attempts: Vec<AttemptRecord>,
    /// Older attempts no longer kept.
    pub dropped: u32,
    /// Retries since the task was submitted or last rerun.
    pub retries: u32,
    /// Placements that do not count against `max_attempts` besides moves:
    /// retries, and every attempt before the last rerun.
    pub spent: u32,
    /// When a retry goes back to the queue (unix milliseconds).
    pub retry_at_ms: Option<u64>,
    /// The next attempt is the first after a rerun.
    pub rerun_next: bool,
    /// What attempts drew, by attempt number (1.22), for the attempts
    /// still kept. Last, so a 1.21 replica still reads the record, and a
    /// 1.21 record reads with none.
    #[serde(default, deserialize_with = "pumasi_proto::wire::trailing")]
    pub energy: Vec<(u32, RunEnergy)>,
}

/// Runtime state of retries and runs.
#[derive(Default)]
pub(crate) struct RunState {
    pub tasks: HashMap<(JobId, u32), TaskRunsRecord>,
    /// Retries waiting out their delay: (due, job, task).
    pub delayed: BTreeSet<(u64, JobId, u32)>,
    /// What runs used, from their reports' tails, until the report counts.
    pub report_use: HashMap<TaskRef, RunUse>,
    /// What runs drew, likewise (1.22).
    pub report_energy: HashMap<TaskRef, RunEnergy>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Sums what two runs used, where either measured it.
fn add_use(total: &mut RunUse, u: &RunUse) {
    fn add(a: &mut Option<u64>, b: Option<u64>) {
        if let Some(b) = b {
            *a = Some(a.unwrap_or(0).saturating_add(b));
        }
    }
    add(&mut total.cpu_ms, u.cpu_ms);
    add(&mut total.disk_read_bytes, u.disk_read_bytes);
    add(&mut total.disk_write_bytes, u.disk_write_bytes);
    add(&mut total.net_rx_bytes, u.net_rx_bytes);
    add(&mut total.net_tx_bytes, u.net_tx_bytes);
    if let Some(p) = u.peak_memory_bytes {
        total.peak_memory_bytes = Some(total.peak_memory_bytes.unwrap_or(0).max(p));
    }
}

impl State {
    /// Reads attempts and job settings, after the jobs were loaded.
    pub(crate) fn load_runs(&mut self) -> Result<()> {
        for (id, r) in self.store.job_runs()? {
            if let Some(j) = self.jobs.get_mut(&JobId(id)) {
                j.meta.run = r;
            }
        }
        for ((job, index), r) in self.store.task_runs()? {
            let id = JobId(job);
            let pending = self
                .jobs
                .get(&id)
                .and_then(|j| j.tasks.get(index as usize))
                .is_some_and(|t| t.state == TaskState::Pending);
            if pending && let Some(at) = r.retry_at_ms {
                self.runs.delayed.insert((at, id, index));
            }
            self.runs.tasks.insert((id, index), r);
        }
        Ok(())
    }

    fn persist_runs(&self, job: JobId, index: u32) {
        let r = self.runs.tasks.get(&(job, index));
        if let Err(e) = self.store.put_task_runs(job.0, &[(index, r)]) {
            warn!("persist attempts: {e:#}");
        }
    }

    fn persist_job_run(&self, job: JobId) {
        if let Some(j) = self.jobs.get(&job)
            && let Err(e) = self.store.put_job_run(job.0, &j.meta.run)
        {
            warn!("persist job settings: {e:#}");
        }
    }

    /// Placements of a task that do not count against `max_attempts`
    /// besides moves: retries, and everything before its last rerun.
    pub(crate) fn spent_attempts(&self, job: JobId, index: u32) -> u32 {
        self.runs.tasks.get(&(job, index)).map_or(0, |r| r.spent)
    }

    /// `PUMASI_RETRY` for a run of the task: the retries so far, for jobs
    /// with retries.
    pub(crate) fn retry_env(&self, job: JobId, index: u32) -> Option<String> {
        self.jobs.get(&job)?.meta.run.run.retry.as_ref()?;
        Some(
            self.runs
                .tasks
                .get(&(job, index))
                .map_or(0, |r| r.retries)
                .to_string(),
        )
    }

    /// The period of a run's checkpoints, for checkpointing jobs that ask.
    pub(crate) fn checkpoint_every(&self, job: JobId) -> Option<u64> {
        let j = self.jobs.get(&job)?;
        j.meta.place.checkpoint.as_ref()?;
        j.meta.run.run.checkpoint_every_ms
    }

    /// Whether a queued run of the task waits anyway: its job is
    /// suspended, or its retry is not due.
    pub(crate) fn run_held_back(&self, job: JobId, index: u32) -> bool {
        if self
            .jobs
            .get(&job)
            .is_some_and(|j| j.meta.run.suspended.is_some())
        {
            return true;
        }
        self.runs
            .tasks
            .get(&(job, index))
            .and_then(|r| r.retry_at_ms)
            .is_some_and(|at| at > now_ms())
    }

    /// Nodes a retry of the task would rather not go to: where its last
    /// attempts failed.
    pub(crate) fn avoid_nodes(&self, job: JobId, index: u32) -> Vec<String> {
        let Some(r) = self.runs.tasks.get(&(job, index)) else {
            return Vec::new();
        };
        if r.retries == 0 {
            return Vec::new();
        }
        r.attempts
            .iter()
            .rev()
            .filter(|a| a.retried)
            .take(r.retries as usize)
            .map(|a| a.node.clone())
            .collect()
    }

    /// A run of the task was placed on `node`.
    pub(crate) fn attempt_started(&mut self, r: TaskRef, node: &str) {
        if r.check {
            return;
        }
        let moves = self.free_attempts(r.job, r.index, false);
        let rec = self.runs.tasks.entry((r.job, r.index)).or_default();
        let rerun = std::mem::take(&mut rec.rerun_next);
        rec.retry_at_ms = None;
        rec.attempts.push(AttemptRecord {
            attempt: r.attempt,
            node: node.to_string(),
            started_unix: now_unix(),
            ended_unix: None,
            end: None,
            why: String::new(),
            wall_ms: 0,
            usage: None,
            retried: false,
            rerun,
            moves_at_start: moves,
        });
        if rec.attempts.len() > MAX_ATTEMPT_RECORDS {
            let n = rec.attempts.len() - MAX_ATTEMPT_RECORDS;
            rec.attempts.drain(..n);
            rec.dropped += n as u32;
            let first = rec.attempts.first().map_or(0, |a| a.attempt);
            rec.energy.retain(|(a, _)| *a >= first);
        }
        self.persist_runs(r.job, r.index);
    }

    /// A run ended: how, and what it used.
    fn attempt_ended(&mut self, r: TaskRef, end: EndKind, why: String, wall_ms: u64) {
        if r.check {
            return;
        }
        let usage = self.runs.report_use.remove(&r);
        let energy = self.runs.report_energy.remove(&r);
        let Some(rec) = self.runs.tasks.get_mut(&(r.job, r.index)) else {
            return;
        };
        let Some(a) = rec
            .attempts
            .iter_mut()
            .rev()
            .find(|a| a.attempt == r.attempt && a.ended_unix.is_none())
        else {
            return;
        };
        a.ended_unix = Some(now_unix());
        a.end = Some(end);
        a.why = why;
        a.wall_ms = wall_ms;
        a.usage = usage.filter(|u| !u.is_empty());
        if let Some(e) = energy.filter(|e| !e.is_empty()) {
            rec.energy.retain(|(n, _)| *n != r.attempt);
            rec.energy.push((r.attempt, e));
        }
        self.persist_runs(r.job, r.index);
    }

    /// A run reported: records its end. A failure the job retries goes
    /// back to the queue (after its delay) instead of ending the task;
    /// answers whether it did.
    pub(crate) fn on_run_ended(
        &mut self,
        node_id: &str,
        r: TaskRef,
        outcome: &Outcome,
        wall_ms: u64,
    ) -> bool {
        let end = EndKind::of(outcome);
        self.attempt_ended(r, end, outcome.to_string(), wall_ms);
        let Some(job) = self.jobs.get(&r.job) else {
            return false;
        };
        if job.meta.cancelled {
            return false;
        }
        let Some(policy) = job.meta.run.run.retry.clone() else {
            return false;
        };
        let used = self
            .runs
            .tasks
            .get(&(r.job, r.index))
            .map_or(0, |t| t.retries);
        if used >= policy.retries || !policy.retries_on(end) {
            return false;
        }
        let n = used + 1;
        let delay = policy.delay_before(n);
        let rec = self.runs.tasks.entry((r.job, r.index)).or_default();
        rec.retries = n;
        rec.spent += 1;
        if let Some(a) = rec
            .attempts
            .iter_mut()
            .rev()
            .find(|a| a.attempt == r.attempt)
        {
            a.retried = true;
        }
        let at = now_ms() + delay;
        if delay > 0 {
            rec.retry_at_ms = Some(at);
            self.runs.delayed.insert((at, r.job, r.index));
        }
        self.persist_runs(r.job, r.index);
        info!(task = %r, node = %node_id, retry = n, of = policy.retries, delay_ms = delay, "{end}; retrying");
        let name = self.node_name(node_id);
        self.event(
            EventLevel::Info,
            "task.retry",
            Some(node_id),
            Some(r.job),
            pumasi_proto::text::Text::new(
                "task.retry",
                [
                    r.job.to_string(),
                    r.index.to_string(),
                    name,
                    end.to_string(),
                    n.to_string(),
                    policy.retries.to_string(),
                    pumasi_proto::job::format_duration_ms(delay),
                ],
            ),
        );
        if delay == 0 {
            self.push_run(r.job, r.index, false, true);
        }
        true
    }

    /// A run went back to the queue (its node went away, or it was moved).
    pub(crate) fn on_run_requeued(&mut self, r: TaskRef, reason: &str) {
        if r.check {
            return;
        }
        let moves = self.free_attempts(r.job, r.index, false);
        let moved = self
            .runs
            .tasks
            .get(&(r.job, r.index))
            .and_then(|t| t.attempts.iter().rev().find(|a| a.attempt == r.attempt))
            .is_some_and(|a| moves > a.moves_at_start);
        let end = if moved { EndKind::Moved } else { EndKind::Lost };
        self.attempt_ended(r, end, reason.to_string(), 0);
    }

    /// The node's measure of what a run used, kept until its report counts.
    pub fn note_use(&mut self, node_id: &str, generation: u64, task: TaskRef, u: RunUse) {
        let running = self
            .nodes
            .get(node_id)
            .is_some_and(|n| n.generation == generation && n.running.contains_key(&task));
        if running {
            self.runs.report_use.insert(task, u);
        }
    }

    /// What the node measured a run drew (1.22), kept until its report
    /// counts.
    pub fn note_energy(&mut self, node_id: &str, generation: u64, task: TaskRef, e: RunEnergy) {
        let running = self
            .nodes
            .get(node_id)
            .is_some_and(|n| n.generation == generation && n.running.contains_key(&task));
        if running {
            self.runs.report_energy.insert(task, e);
        }
    }

    /// Retries whose delay is over go back to the queue.
    pub(crate) fn tick_retries(&mut self) {
        let now = now_ms();
        let mut due = Vec::new();
        while let Some(&(at, job, index)) = self.runs.delayed.first() {
            if at > now {
                break;
            }
            self.runs.delayed.pop_first();
            due.push((at, job, index));
        }
        for (at, job, index) in due {
            let pending = self
                .jobs
                .get(&job)
                .and_then(|j| j.tasks.get(index as usize))
                .is_some_and(|t| t.state == TaskState::Pending);
            let Some(rec) = self.runs.tasks.get_mut(&(job, index)) else {
                continue;
            };
            if rec.retry_at_ms != Some(at) {
                continue;
            }
            rec.retry_at_ms = None;
            self.persist_runs(job, index);
            if pending {
                self.push_run(job, index, false, true);
            }
        }
    }

    /// A checkpoint a running task saved when asked (1.21): kept like the
    /// one a stopping run leaves, and the run goes on.
    pub fn on_checkpointed(
        &mut self,
        node_id: &str,
        generation: u64,
        task: TaskRef,
        checkpoint: StoredOutput,
    ) {
        let running = self
            .nodes
            .get(node_id)
            .is_some_and(|n| n.generation == generation && n.running.contains_key(&task));
        let current = self
            .jobs
            .get(&task.job)
            .and_then(|j| {
                j.meta.place.checkpoint.as_ref()?;
                j.tasks.get(task.index as usize)
            })
            .is_some_and(|t| {
                t.attempts == task.attempt && matches!(t.state, TaskState::Running { .. })
            });
        if !running || !current || task.check {
            return;
        }
        info!(task = %task, node = %node_id, files = checkpoint.files, bytes = checkpoint.size, "periodic checkpoint saved");
        self.save_checkpoint(task, node_id, checkpoint, false);
    }

    // -- suspend, resume, rerun ----------------------------------------------

    fn job_for_action(&self, id: JobId) -> Result<&crate::state::Job> {
        let job = self.jobs.get(&id).context("no such job")?;
        ensure!(
            job.meta.origin.submitted_by != crate::verify::HONEYPOT_BY,
            "no such job"
        );
        Ok(job)
    }

    /// Holds a job: queued tasks wait, running ones are moved off their
    /// nodes (saving a checkpoint first where the job keeps them).
    pub fn suspend_job(&mut self, id: JobId, by: &str) -> Result<()> {
        let job = self.job_for_action(id)?;
        ensure!(
            !Self::summary(job).state.is_terminal(),
            "job {id} has ended; nothing to suspend"
        );
        if job.meta.run.suspended.is_some() {
            return Ok(());
        }
        if let Some(j) = self.jobs.get_mut(&id) {
            j.meta.run.suspended = Some(Suspension {
                by: by.to_string(),
                since_unix: now_unix(),
            });
        }
        self.persist_job_run(id);
        self.queue.retain(|q| q.job != id || q.check);
        let running: Vec<(String, TaskRef)> = self
            .nodes
            .iter()
            .flat_map(|(n, rt)| {
                rt.running
                    .keys()
                    .filter(|t| t.job == id && !t.check && !rt.stopping.contains_key(t))
                    .map(|t| (n.clone(), *t))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (node, t) in &running {
            self.evict(node, *t, "its job was suspended");
        }
        info!(job = %id, %by, moved = running.len(), "job suspended");
        self.event(
            EventLevel::Info,
            "job.suspended",
            None,
            Some(id),
            pumasi_proto::text::Text::new(
                "job.suspended",
                [id.to_string(), by.to_string(), running.len().to_string()],
            ),
        );
        self.schedule();
        Ok(())
    }

    pub fn resume_job(&mut self, id: JobId, by: &str) -> Result<()> {
        let job = self.job_for_action(id)?;
        if job.meta.run.suspended.is_none() {
            return Ok(());
        }
        if let Some(j) = self.jobs.get_mut(&id) {
            j.meta.run.suspended = None;
        }
        self.persist_job_run(id);
        // Its queue timeout starts again.
        self.mgmt.ready_since.remove(&id);
        if !self.mgmt.waiting.contains(&id) {
            self.queue_job(id);
        }
        info!(job = %id, %by, "job resumed");
        self.event(
            EventLevel::Info,
            "job.resumed",
            None,
            Some(id),
            pumasi_proto::text::Text::new("job.resumed", [id.to_string(), by.to_string()]),
        );
        self.schedule();
        Ok(())
    }

    /// Runs tasks of a job again under the same id: each gets a new
    /// attempt with the same inputs, without its last output or
    /// checkpoint. Answers which.
    pub fn rerun_job(&mut self, id: JobId, which: &RerunWhich, by: &str) -> Result<Vec<u32>> {
        let job = self.job_for_action(id)?;
        ensure!(
            !self.mgmt.waiting.contains(&id),
            "job {id} still waits for the jobs it runs after; nothing of it ran yet"
        );
        let finished = |s: &TaskState| {
            matches!(
                s,
                TaskState::Succeeded | TaskState::Failed(_) | TaskState::Cancelled
            )
        };
        let picked: Vec<u32> = match which {
            RerunWhich::Failed => job
                .tasks
                .iter()
                .enumerate()
                .filter(|(_, t)| matches!(t.state, TaskState::Failed(_) | TaskState::Cancelled))
                .map(|(i, _)| i as u32)
                .collect(),
            RerunWhich::Tasks(list) => {
                let mut v = list.clone();
                v.sort_unstable();
                v.dedup();
                for &i in &v {
                    let Some(t) = job.tasks.get(i as usize) else {
                        bail!("job {id} has no task {i} (it has {})", job.tasks.len());
                    };
                    ensure!(
                        finished(&t.state),
                        "task {i} has not finished; only finished tasks run again"
                    );
                }
                v
            }
        };
        ensure!(
            !picked.is_empty(),
            "job {id} has no failed or cancelled tasks to run again"
        );
        let had_store = job.meta.ext.finished.is_some();
        let cancelled = job.meta.cancelled;
        for &i in &picked {
            self.set_stored(id, i, None);
            if self.sched.checkpoints.remove(&(id, i)).is_some() {
                if let Err(e) = self.store.put_checkpoint(id.0, i, None) {
                    warn!("persist checkpoint: {e:#}");
                }
                self.store_cmd(crate::datasets::StoreCmd::Untag(crate::files::ckpt_tag(
                    id.0, i,
                )));
            }
            let moves = self.free_attempts(id, i, false);
            let Some(task) = self
                .jobs
                .get_mut(&id)
                .and_then(|j| j.tasks.get_mut(i as usize))
            else {
                continue;
            };
            task.state = TaskState::Pending;
            task.node = None;
            task.fuel_used = 0;
            task.wall_ms = 0;
            task.stdout.clear();
            task.stderr.clear();
            task.output_files.clear();
            task.check = None;
            task.times = TaskTimes::default();
            task.ran = None;
            let spent = task.attempts.saturating_sub(moves);
            let rec = self.runs.tasks.entry((id, i)).or_default();
            rec.retries = 0;
            rec.spent = spent;
            rec.retry_at_ms = None;
            rec.rerun_next = true;
            self.persist_runs(id, i);
        }
        if let Some(job) = self.jobs.get_mut(&id) {
            job.meta.cancelled = false;
            let refs: Vec<(u32, &crate::state::TaskRecord)> = picked
                .iter()
                .map(|&i| (i, &job.tasks[i as usize]))
                .collect();
            if let Err(e) = self.store.put_tasks(id.0, &refs) {
                warn!("persist tasks: {e:#}");
            }
            if had_store {
                // Its store work ended: what it mounts is tagged again, and
                // a private image is pulled again.
                job.meta.ext.finished = None;
            }
            if had_store || cancelled {
                let meta = job.meta.clone();
                if let Err(e) = self.store.put_job_meta(&meta) {
                    warn!("persist job: {e:#}");
                }
            }
        }
        if had_store {
            self.after_submit(id);
            let keeps = self.jobs.get(&id).is_some_and(|j| {
                j.meta.ext.output_dataset.is_some()
                    || j.tasks.iter().any(|t| t.ext.stored.is_some())
            });
            if keeps {
                self.files.open_jobs.insert(id);
            }
        }
        self.monitor.active_jobs.insert(id);
        self.mgmt.ready_since.remove(&id);
        self.queue_job(id);
        info!(job = %id, %by, tasks = picked.len(), "job rerun");
        self.event(
            EventLevel::Info,
            "job.rerun",
            None,
            Some(id),
            pumasi_proto::text::Text::new(
                "job.rerun",
                [id.to_string(), by.to_string(), picked.len().to_string()],
            ),
        );
        self.schedule();
        Ok(picked)
    }

    /// Why a job's queued tasks wait, when it is suspended.
    pub(crate) fn suspended_reason(&self, id: JobId) -> Option<String> {
        let s = self.jobs.get(&id)?.meta.run.suspended.as_ref()?;
        Some(format!(
            "suspended by {} at {} (`pumasi job resume {id}`)",
            s.by,
            pumasi_proto::audit::rfc3339(s.since_unix)
        ))
    }

    pub fn job_runs_view(&self, id: JobId) -> Option<JobRunsView> {
        let job = self.jobs.get(&id)?;
        let mut v = JobRunsView {
            run: job.meta.run.run.clone(),
            suspended: job.meta.run.suspended.clone(),
            cost: job.tasks.iter().map(|t| t.cost).sum(),
            ..Default::default()
        };
        for (i, t) in job.tasks.iter().enumerate() {
            let Some(r) = self.runs.tasks.get(&(id, i as u32)) else {
                continue;
            };
            v.dropped += r.dropped;
            for a in &r.attempts {
                if let Some(u) = &a.usage {
                    add_use(&mut v.total, u);
                }
            }
            for (attempt, e) in &r.energy {
                v.total_energy.add(e);
                v.energy.push((i as u32, *attempt, *e));
            }
            let overasked = r
                .attempts
                .iter()
                .rev()
                .find_map(|a| a.usage.map(|u| (u, a.wall_ms)))
                .map(|(u, wall)| pumasi_proto::attempt::overasked(&job.meta.limits, wall, &u))
                .unwrap_or_default();
            v.tasks.push(TaskRunsView {
                index: i as u32,
                attempts: r
                    .attempts
                    .iter()
                    .map(|a| AttemptView {
                        attempt: a.attempt,
                        node_name: self.members.get(&a.node).map(|_| self.node_name(&a.node)),
                        node: a.node.clone(),
                        started_unix: a.started_unix,
                        ended_unix: a.ended_unix,
                        end: a.end,
                        why: a.why.clone(),
                        wall_ms: a.wall_ms,
                        usage: a.usage,
                        retried: a.retried,
                        rerun: a.rerun,
                    })
                    .collect(),
                retries: r.retries,
                retry_unix: r
                    .retry_at_ms
                    .filter(|_| t.state == TaskState::Pending)
                    .map(|ms| ms.div_ceil(1000)),
                overasked,
            });
        }
        Some(v)
    }
}
