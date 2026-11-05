//! Result checks: a sample of tasks run by providers is re-run on another
//! node (a trusted owner machine when one is free) and the results are
//! compared.
//!
//! A run is accepted when a trusted run agrees with it, or when a majority
//! of runs agree. Runs that disagree with the accepted one lose their held
//! earning (the consumer gets it back) and earn their node a strike. Two
//! untrusted runs that disagree call for a third; if three runs still have
//! no majority, the task fails and every untrusted earning is refunded.

use pumasi_proto::msg::{Outcome, TaskRef, TaskReport, TaskState, Verification};
use tracing::{info, warn};

use crate::state::{RunOutput, RunRecord, State, final_state, reproducible};

/// Two runs agree when they produced the same result. Fuel must match
/// too, unless the nodes run different agent versions (which may meter
/// slightly differently).
fn agree(a: &RunRecord, b: &RunRecord) -> bool {
    a.digest == b.digest && (a.agent_version != b.agent_version || a.fuel_used == b.fuel_used)
}

/// The run whose result is accepted, if the runs settle it.
fn winner(runs: &[RunRecord]) -> Option<usize> {
    if let Some(t) = runs.iter().position(|r| r.trusted) {
        return Some(t);
    }
    (0..runs.len()).find(|&i| {
        let votes = runs.iter().filter(|r| agree(&runs[i], r)).count();
        votes * 2 > runs.len() && votes >= 2
    })
}

/// Most runs a check uses (the first run plus two check runs).
const MAX_RUNS: usize = 3;

impl State {
    pub(crate) fn on_check_report(&mut self, node_id: &str, report: TaskReport, digest: [u8; 32]) {
        let r = report.task;
        let (trusted, account, version, _, _) = self.node_record(node_id);
        let max_attempts = self
            .jobs
            .get(&r.job)
            .map(|j| j.meta.max_attempts)
            .unwrap_or(1);
        let Some(task) = self
            .jobs
            .get_mut(&r.job)
            .and_then(|j| j.tasks.get_mut(r.index as usize))
        else {
            return;
        };
        let Some(check) = task.check.as_mut() else {
            return;
        };
        if check.status != Verification::Pending || task.state != TaskState::Verifying {
            // The check was dropped meanwhile (job cancelled).
            return;
        }
        check.running_on = None;
        if !reproducible(&report.outcome) {
            // Nothing comparable came back (timeout, agent error): try
            // another node, or give up on checking.
            if check.attempts >= max_attempts {
                warn!(task = %r, "check runs did not finish; keeping the first result");
                check.status =
                    Verification::Skipped(format!("check run ended with {}", report.outcome));
                task.state = final_state(&check.runs[0].outcome);
            } else {
                self.push_run(r.job, r.index, true, true);
            }
            self.persist_task(r);
            return;
        }
        check.runs.push(RunRecord {
            task: r,
            node: node_id.to_string(),
            account,
            trusted,
            agent_version: version,
            outcome: report.outcome,
            digest,
            fuel_used: report.fuel_used,
            output: Some(RunOutput {
                stdout: report.stdout,
                stderr: report.stderr,
                files: report.files,
                fuel_used: report.fuel_used,
                wall_ms: report.wall_ms,
            }),
        });
        self.resolve_check(r);
    }

    /// Settles a check if its runs allow it, or asks for another run.
    fn resolve_check(&mut self, r: TaskRef) {
        let Some(task) = self
            .jobs
            .get_mut(&r.job)
            .and_then(|j| j.tasks.get_mut(r.index as usize))
        else {
            return;
        };
        let Some(check) = task.check.as_mut() else {
            return;
        };
        let runs = check.runs.clone();
        let mut passed = Vec::new();
        let mut rejected: Vec<RunRecord> = Vec::new();
        // A check run whose result won (its stored output becomes the task's).
        let mut won_attempt = None;
        match winner(&runs) {
            Some(w) => {
                let win = &runs[w];
                for run in &runs {
                    if agree(win, run) {
                        passed.push(run.node.clone());
                    } else {
                        rejected.push(run.clone());
                    }
                }
                // The first run's output is in the task; replace it when a
                // check run's result won instead.
                if !agree(win, &runs[0])
                    && let Some(out) = &win.output
                {
                    task.stdout = out.stdout.clone();
                    task.stderr = out.stderr.clone();
                    task.output_files = out.files.clone();
                    task.fuel_used = out.fuel_used;
                    task.wall_ms = out.wall_ms;
                    task.node = Some(win.node.clone());
                    won_attempt = Some(win.task.attempt);
                }
                check.status = if rejected.is_empty() {
                    Verification::Passed
                } else {
                    Verification::Mismatch {
                        bad: rejected.iter().map(|x| x.node.clone()).collect(),
                    }
                };
                task.state = final_state(&win.outcome);
                info!(task = %r, status = ?check.status, "check settled");
            }
            None if runs.len() < MAX_RUNS => {
                info!(task = %r, "runs disagree; asking another node");
                self.push_run(r.job, r.index, true, true);
                self.persist_task(r);
                return;
            }
            None => {
                check.status = Verification::Inconclusive;
                task.state = TaskState::Failed(Outcome::Error(
                    "runs on different nodes disagreed and no majority settled it".into(),
                ));
                rejected = runs.iter().filter(|x| !x.trusted).cloned().collect();
                warn!(task = %r, "check inconclusive");
            }
        }
        let inconclusive = matches!(check.status, Verification::Inconclusive);
        self.settle_check_stored(r.job, r.index, won_attempt);
        self.persist_task(r);
        for node in passed {
            self.credit_pass(&node);
        }
        for run in rejected {
            self.forfeit(run.task, "result rejected by a check");
            if !inconclusive {
                self.strike(
                    &run.node,
                    &format!("result of {} rejected by a check", run.task),
                );
            }
        }
    }

    /// Puts a check run back in the queue after its node vanished.
    pub(crate) fn requeue_check(&mut self, r: TaskRef, reason: &str) {
        let Some(job) = self.jobs.get_mut(&r.job) else {
            return;
        };
        let max = job.meta.max_attempts;
        let cancelled = job.meta.cancelled;
        let free = self
            .mgmt
            .preempted
            .get(&(r.job, r.index, true))
            .copied()
            .unwrap_or(0);
        let Some(task) = job.tasks.get_mut(r.index as usize) else {
            return;
        };
        let Some(check) = task.check.as_mut() else {
            return;
        };
        if check.attempts != r.attempt || check.running_on.is_none() {
            return;
        }
        check.running_on = None;
        if cancelled || task.state != TaskState::Verifying {
            return;
        }
        if check.attempts.saturating_sub(free) >= max {
            check.status = Verification::Skipped(format!("{reason}; no node finished a check run"));
            task.state = final_state(&check.runs[0].outcome);
        } else {
            self.push_run(r.job, r.index, true, true);
        }
        self.persist_task(r);
    }

    pub(crate) fn persist_task(&self, r: TaskRef) {
        if let Some(task) = self
            .jobs
            .get(&r.job)
            .and_then(|j| j.tasks.get(r.index as usize))
            && let Err(e) = self.store.put_tasks(r.job.0, &[(r.index, task)])
        {
            warn!("persist task: {e:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::job::JobId;

    fn run(node: &str, trusted: bool, digest: u8, fuel: u64) -> RunRecord {
        RunRecord {
            task: TaskRef {
                job: JobId(1),
                index: 0,
                attempt: 1,
                check: false,
            },
            node: node.into(),
            account: node.into(),
            trusted,
            agent_version: "1".into(),
            outcome: Outcome::Exited(0),
            digest: [digest; 32],
            fuel_used: fuel,
            output: None,
        }
    }

    #[test]
    fn trusted_run_settles() {
        let runs = [run("p", false, 1, 10), run("o", true, 2, 10)];
        assert_eq!(winner(&runs), Some(1));
        assert!(!agree(&runs[0], &runs[1]));
    }

    #[test]
    fn majority_settles_and_ties_do_not() {
        let tie = [run("a", false, 1, 10), run("b", false, 2, 10)];
        assert_eq!(winner(&tie), None);
        let agree2 = [run("a", false, 1, 10), run("b", false, 1, 10)];
        assert_eq!(winner(&agree2), Some(0));
        let three = [
            run("a", false, 1, 10),
            run("b", false, 2, 10),
            run("c", false, 2, 10),
        ];
        assert_eq!(winner(&three), Some(1));
        let split = [
            run("a", false, 1, 10),
            run("b", false, 2, 10),
            run("c", false, 3, 10),
        ];
        assert_eq!(winner(&split), None);
    }

    #[test]
    fn fuel_counts_only_between_equal_versions() {
        let a = run("a", false, 1, 10);
        let mut b = run("b", false, 1, 11);
        assert!(!agree(&a, &b), "same version, fuel differs");
        b.agent_version = "2".into();
        assert!(agree(&a, &b));
    }
}
