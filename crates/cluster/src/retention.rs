//! Forgetting old finished jobs (phase 17). Every job used to stay in
//! memory and in the database for good, with its tasks' stdout, stderr and
//! small output files, so a cluster that runs for months grew without
//! bound. Finished jobs submitted longer ago than the retention (default
//! 30 days, `pumasi cluster start --keep-jobs`) are deleted, with the
//! modules and file bundles no other job uses. Receipts, holds, the ledger
//! and the audit log are kept; output datasets in the file store follow
//! their own tags and quotas.

use std::{collections::HashSet, time::Duration};

use pumasi_proto::{ModuleHash, audit::rfc3339, job::JobId};
use tracing::{info, warn};

use crate::{audit::Actor, state::State};

/// How long finished jobs are kept unless `--keep-jobs` says otherwise.
pub const DEFAULT_KEEP_JOBS: Duration = Duration::from_secs(30 * 86_400);
/// At most this many jobs go in one sweep, which keeps the transaction
/// (and its Raft entry) small; the next sweep takes the rest.
const MAX_PER_SWEEP: usize = 2_000;

impl State {
    /// Deletes finished jobs submitted before `now_unix - keep`, except
    /// those a waiting job depends on and those with store work or
    /// earnings still open. `keep` of zero keeps everything. Returns how
    /// many were deleted.
    pub fn expire_jobs(&mut self, keep: Duration, now_unix: u64) -> usize {
        if keep.is_zero() {
            return 0;
        }
        let cutoff = now_unix.saturating_sub(keep.as_secs());
        let terminal = |j: &crate::state::Job| Self::summary(j).state.is_terminal();
        let needed: HashSet<JobId> = self
            .jobs
            .values()
            .filter(|j| !terminal(j))
            .flat_map(|j| j.meta.sched.after.iter().copied())
            .collect();
        let held: HashSet<JobId> = self.holds.keys().map(|t| t.job).collect();
        let mut old: Vec<(u64, JobId)> = self
            .jobs
            .values()
            .filter(|j| j.meta.submitted_unix < cutoff && terminal(j))
            .map(|j| (j.meta.seq, j.meta.id))
            .filter(|(_, id)| {
                !needed.contains(id)
                    && !held.contains(id)
                    && !self.files.open_jobs.contains(id)
                    && !self.files.finishing.contains(id)
            })
            .collect();
        if old.is_empty() {
            return 0;
        }
        // Oldest first, so a backlog shrinks in order.
        old.sort_unstable();
        old.truncate(MAX_PER_SWEEP);
        let gone: HashSet<JobId> = old.iter().map(|(_, id)| *id).collect();

        let mut rows = Vec::with_capacity(gone.len());
        let mut blobs: HashSet<ModuleHash> = HashSet::new();
        for id in &gone {
            let job = &self.jobs[id];
            rows.push((id.0, job.tasks.len() as u32));
            blobs.extend(job.meta.module_hash);
            blobs.extend(job.meta.files_hash);
        }
        for job in self.jobs.values().filter(|j| !gone.contains(&j.meta.id)) {
            for h in [job.meta.module_hash, job.meta.files_hash]
                .into_iter()
                .flatten()
            {
                blobs.remove(&h);
            }
        }
        let blobs: Vec<ModuleHash> = blobs.into_iter().collect();
        if let Err(e) = self.store.delete_jobs(&rows, &blobs) {
            warn!("could not delete old jobs: {e:#}");
            return 0;
        }
        for id in &gone {
            self.jobs.remove(id);
            self.mgmt.waiting.remove(id);
            self.mgmt.ready_since.remove(id);
            self.monitor.active_jobs.remove(id);
        }
        self.mgmt
            .preempted
            .retain(|(job, _, _), _| !gone.contains(job));
        for h in &blobs {
            self.modules.remove(h);
        }
        let n = gone.len();
        info!(
            "deleted {n} finished jobs submitted before {}",
            rfc3339(cutoff)
        );
        self.audit(
            &Actor::system(),
            "jobs.expire",
            format!("{n} jobs"),
            format!(
                "finished jobs submitted before {} ({} blobs freed)",
                rfc3339(cutoff),
                blobs.len()
            ),
            true,
        );
        n
    }
}
