//! Remote API clients (phase 15): what a program elsewhere needs to hand
//! work to the cluster safely and know when it cannot. Account job
//! policies, who submitted each job and the image digests it ran,
//! executors that come and go (Docker Desktop stopped), queue timeouts,
//! and which nodes can take an account's work right now. Extends
//! [`State`].

use std::{collections::HashMap, time::Instant};

use anyhow::Result;
use pumasi_proto::{
    economy::{validate_account, vcpus},
    gpu::GpuRequest,
    job::{Executor, GpuCapability, JobId, JobSubmission, Tier, format_duration_ms},
    manage::{AccountPolicy, LabelSet},
    monitor::EventLevel,
    msg::{
        Capacity, ExecutorReport, JobCheck, NodeCapacity, OWNER_ACCOUNT, Outcome, TaskDetail,
        TaskRef, TaskState, TaskTiming,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{Need, QueueItem, State};

/// Image digests kept per job: tasks of one job normally run one image,
/// so more means a tag moved while it ran.
const MAX_DIGESTS: usize = 16;

/// Who submitted a job and the rest of what phase 15 keeps about it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobOrigin {
    /// The access ticket's name, `cluster admin`, or `schedule NAME`.
    pub submitted_by: String,
    pub queue_timeout_ms: Option<u64>,
    pub gpu_capabilities: Vec<GpuCapability>,
    /// The images its tasks ran, by digest, as the nodes reported them.
    pub image_digests: Vec<String>,
}

impl JobOrigin {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

impl State {
    // -- policies -----------------------------------------------------------

    pub fn account_policy(&self, account: &str) -> AccountPolicy {
        self.mgmt.policies.get(account).cloned().unwrap_or_default()
    }

    pub fn set_account_policy(&mut self, account: &str, p: AccountPolicy) -> Result<()> {
        validate_account(account)?;
        p.validate()?;
        if p.is_empty() {
            self.store.put_account_policy(account, None)?;
            self.mgmt.policies.remove(account);
        } else {
            self.store.put_account_policy(account, Some(&p))?;
            self.mgmt.policies.insert(account.to_string(), p);
        }
        info!(%account, policy = %self.account_policy(account).describe(), "job policy set");
        Ok(())
    }

    /// Holds a submitter's job to its account's policy (and adds the
    /// policy's selector).
    pub(crate) fn admit_policy(&self, account: &str, sub: &mut JobSubmission) -> Result<()> {
        match self.mgmt.policies.get(account) {
            Some(p) => p.admit(sub),
            None => Ok(()),
        }
    }

    // -- what nodes report --------------------------------------------------

    /// A node's executors changed (Docker Desktop stopped or started), or
    /// whether its Docker GPUs can draw (protocol 13).
    pub fn on_executors(&mut self, id: &str, generation: u64, mut report: ExecutorReport) {
        let Some(m) = self.members.get(id) else {
            return;
        };
        let Some(n) = self.nodes.get_mut(id) else {
            return;
        };
        if n.generation != generation {
            return;
        }
        if !report.executors.contains(&Executor::Wasm) {
            report.executors.insert(0, Executor::Wasm);
        }
        report.unavailable.truncate(8);
        for (_, why) in report.unavailable.iter_mut() {
            why.truncate(300);
        }
        let executors = m.executors(&report.executors);
        let before: Vec<Executor> = n.executors.clone();
        let changed = before != executors
            || n.unavailable != report.unavailable
            || n.gpu_graphics != report.gpu_graphics;
        if !changed {
            return;
        }
        let gained: Vec<Executor> = executors
            .iter()
            .filter(|e| !before.contains(e))
            .copied()
            .collect();
        let lost: Vec<(Executor, String)> = before
            .iter()
            .filter(|e| !executors.contains(e))
            .map(|e| {
                let why = report
                    .unavailable
                    .iter()
                    .find(|(u, _)| u == e)
                    .map(|(_, w)| w.clone())
                    .unwrap_or_else(|| "no longer offered".into());
                (*e, why)
            })
            .collect();
        n.info.executors = report.executors;
        n.executors = executors;
        n.unavailable = report.unavailable;
        n.gpu_graphics = report.gpu_graphics;
        let name = self.node_name(id);
        for (e, why) in lost {
            info!(node = %id, executor = %e, %why, "executor unavailable");
            self.event(
                EventLevel::Warning,
                "node.executor_unavailable",
                Some(id),
                None,
                format!("node {name}: {e} unavailable: {why}"),
            );
        }
        for e in gained {
            info!(node = %id, executor = %e, "executor available");
            self.event(
                EventLevel::Info,
                "node.executor_available",
                Some(id),
                None,
                format!("node {name}: {e} available"),
            );
        }
        self.schedule();
    }

    /// The image a running task uses, by digest (protocol 13).
    pub fn on_image(
        &mut self,
        id: &str,
        generation: u64,
        task: TaskRef,
        image: String,
        digest: String,
    ) {
        let Some(n) = self.nodes.get(id) else {
            return;
        };
        if n.generation != generation || !n.running.contains_key(&task) {
            return;
        }
        let digest: String = digest.trim().chars().take(300).collect();
        if digest.is_empty() {
            return;
        }
        let Some(job) = self.jobs.get_mut(&task.job) else {
            return;
        };
        let origin = &mut job.meta.origin;
        if origin.image_digests.contains(&digest) || origin.image_digests.len() >= MAX_DIGESTS {
            return;
        }
        origin.image_digests.push(digest.clone());
        if let Err(e) = self.store.put_job_origin(task.job.0, origin) {
            warn!("persist job origin: {e:#}");
        }
        let name = self.node_name(id);
        let image: String = image.chars().take(300).collect();
        self.event(
            EventLevel::Info,
            "task.image",
            Some(id),
            Some(task.job),
            format!(
                "task {}/{} on {name} runs {image} ({digest})",
                task.job, task.index
            ),
        );
    }

    // -- queue timeouts -----------------------------------------------------

    /// Fails the tasks of jobs with a queue timeout that no node took in
    /// time, with the reason.
    pub(crate) fn tick_queue_timeouts(&mut self) {
        let now = Instant::now();
        let active: Vec<(JobId, u64)> = self
            .monitor
            .active_jobs
            .iter()
            .filter_map(|id| {
                let j = self.jobs.get(id)?;
                let t = j.meta.origin.queue_timeout_ms?;
                (!j.meta.cancelled).then_some((*id, t))
            })
            .collect();
        self.mgmt
            .ready_since
            .retain(|id, _| active.iter().any(|(a, _)| a == id));
        for (id, timeout_ms) in active {
            if self.mgmt.waiting.contains(&id) {
                // Its dependencies have not finished: the clock starts
                // when it could run.
                self.mgmt.ready_since.remove(&id);
                continue;
            }
            let since = *self.mgmt.ready_since.entry(id).or_insert(now);
            if now.duration_since(since).as_millis() < timeout_ms as u128 {
                continue;
            }
            let Some(job) = self.jobs.get(&id) else {
                continue;
            };
            let late: Vec<u32> = job
                .tasks
                .iter()
                .enumerate()
                .filter(|(_, t)| t.state == TaskState::Pending && t.attempts == 0)
                .map(|(i, _)| i as u32)
                .collect();
            if late.is_empty() {
                continue;
            }
            let why = self.why_unplaced(id, late[0]);
            let msg = format!(
                "no node took it within the queue timeout of {}: {why}",
                format_duration_ms(timeout_ms)
            );
            warn!(job = %id, tasks = late.len(), "{msg}");
            let Some(job) = self.jobs.get_mut(&id) else {
                continue;
            };
            for &i in &late {
                job.tasks[i as usize].state = TaskState::Failed(Outcome::Error(msg.clone()));
            }
            let refs: Vec<(u32, &crate::state::TaskRecord)> =
                late.iter().map(|&i| (i, &job.tasks[i as usize])).collect();
            if let Err(e) = self.store.put_tasks(id.0, &refs) {
                warn!("persist tasks: {e:#}");
            }
            self.queue
                .retain(|q| !(q.job == id && late.contains(&q.index)));
            self.event(
                EventLevel::Warning,
                "job.queue_timeout",
                None,
                Some(id),
                format!(
                    "job {id}: {} task{} {msg}",
                    late.len(),
                    if late.len() == 1 { "" } else { "s" }
                ),
            );
        }
    }

    /// Why no node takes a task: every node with its reason.
    pub(crate) fn why_unplaced(&mut self, job: JobId, index: u32) -> String {
        let priority = self.priority_of(job);
        let item = QueueItem {
            job,
            index,
            check: false,
            priority,
            since: Instant::now(),
        };
        let Some(need) = self.need_for(&item) else {
            return "the task is not waiting".into();
        };
        if self.members.is_empty() {
            return "the cluster has no nodes".into();
        }
        let usage = self.account_usage();
        if !self.within_quota(&need, &usage) {
            return format!(
                "account {} is at its quota (`pumasi quota {}`)",
                need.consumer, need.consumer
            );
        }
        let labels = self.online_labels();
        let mut parts: Vec<String> = self
            .members
            .keys()
            .map(|id| {
                let name = self.node_name(id);
                let why = match self.nodes.get(id) {
                    None => "offline".to_string(),
                    Some(n) => match self.refusal(id, n, &need, &labels, 0, &[]) {
                        Some(r) => r.to_string(),
                        None => "busy with other work".to_string(),
                    },
                };
                format!("{name} ({why})")
            })
            .collect();
        parts.sort();
        parts.join(", ")
    }

    // -- checking a job before it is submitted (1.4) -------------------------

    /// What [`Self::submit_by`] checks, without creating the job, and
    /// which nodes would take one of its tasks right now.
    pub fn check_job(
        &self,
        mut sub: JobSubmission,
        upload_bytes: u64,
        submitter: bool,
    ) -> Result<JobCheck> {
        sub.validate()?;
        let account = sub.account.take().unwrap_or_else(|| OWNER_ACCOUNT.into());
        validate_account(&account)?;
        let cap = self.priority_cap(&account, submitter);
        anyhow::ensure!(
            sub.priority <= cap,
            "priority {} is above the {cap} account {account} may use (`pumasi quota {account} --max-priority`)",
            sub.priority
        );
        let wait = self.check_deps(&sub.after)?;
        let (ext, _) = self.prepare_store(&sub, &account)?;
        // Input files too large to go with the job are stored first.
        if upload_bytes > 0 {
            self.check_quota(&account, upload_bytes)?;
        }
        let floor = self.policy.ledger.floor();
        let can_pay = self
            .accounts
            .get(&account)
            .is_some_and(|a| a.balance > floor)
            && !self.over_daily(&account);
        let confidential = sub.tier == Tier::Confidential;
        let need = Need {
            item: QueueItem {
                job: JobId(0),
                index: 0,
                check: false,
                priority: sub.priority,
                since: Instant::now(),
            },
            executor: sub.executor,
            memory: sub.limits.memory_bytes,
            cpus: match sub.executor {
                Executor::Wasm => 1,
                _ => vcpus(sub.limits.cpu_millis),
            },
            store_job: ext.needs_store || upload_bytes > 0,
            gpu: sub.gpu.clone(),
            vm_network: sub.executor.runs_images()
                && sub.docker.as_ref().is_some_and(|d| d.network),
            // A private image is pulled into the store after the job is
            // submitted, for the architectures of the nodes that fit.
            images: None,
            consumer: account.clone(),
            can_pay,
            exclude_nodes: Vec::new(),
            exclude_accounts: Vec::new(),
            selector: sub.selector.clone(),
            tolerations: sub.tolerations.clone(),
            gpu_caps: !sub.gpu_capabilities.is_empty(),
            gpu_graphics: sub.needs_gpu_graphics(),
            trusted_only: sub.tier == Tier::Trusted,
            // Sealed once the job exists; the nodes must have a TEE.
            confidential: None,
        };
        let labels = self.online_labels();
        let mut ready_nodes = Vec::new();
        let mut reasons = Vec::new();
        let mut ids: Vec<&String> = self.members.keys().collect();
        ids.sort_by_key(|id| (self.node_name(id), (*id).clone()));
        for id in ids {
            let name = self.node_name(id);
            let why = match self.nodes.get(id) {
                None => Some("offline".to_string()),
                Some(n) => match self.refusal(id, n, &need, &labels, 0, &[]) {
                    Some(r) => Some(r.to_string()),
                    None if confidential
                        && (n.perf.protocol < pumasi_proto::CONFIDENTIAL_PROTOCOL
                            || self.conf.usable_tees(id).is_empty()) =>
                    {
                        Some("does not start confidential VMs (SEV-SNP or TDX)".to_string())
                    }
                    None => None,
                },
            };
            match why {
                None => ready_nodes.push(name),
                Some(why) => reasons.push(format!("{name} ({why})")),
            }
        }
        let waiting = if wait {
            Some(match sub.after.as_slice() {
                [one] => format!("it waits for job {one}"),
                many => format!("it waits for {} jobs", many.len()),
            })
        } else if !self.within_quota(&need, &self.account_usage()) {
            ready_nodes.clear();
            Some(format!(
                "account {account} is at its quota (`pumasi quota {account}`)"
            ))
        } else if !ready_nodes.is_empty() {
            None
        } else if reasons.is_empty() {
            Some("the cluster has no nodes".into())
        } else {
            Some(format!("no node takes it now: {}", reasons.join(", ")))
        };
        if wait {
            ready_nodes.clear();
        }
        Ok(JobCheck {
            account,
            tasks: sub.tasks.len() as u32,
            ready_nodes,
            waiting,
        })
    }

    // -- task details (1.4) ---------------------------------------------------

    /// What a node measured of a run, kept until the run's report is
    /// applied (the report follows at once).
    pub fn note_timing(&mut self, node_id: &str, generation: u64, task: TaskRef, t: TaskTiming) {
        let running = self
            .nodes
            .get(node_id)
            .is_some_and(|n| n.generation == generation && n.running.contains_key(&task));
        if running {
            self.mgmt.report_timing.insert(task, t);
        }
    }

    /// Where each task of a job ran, by name, and how long its parts took.
    pub fn task_details(&self, id: JobId) -> Option<Vec<TaskDetail>> {
        let job = self.jobs.get(&id)?;
        Some(
            job.tasks
                .iter()
                .enumerate()
                .map(|(i, t)| TaskDetail {
                    index: i as u32,
                    node_name: t.node.as_deref().map(|n| self.node_name(n)),
                    queued_ms: t.times.queued_ms,
                    fetch_ms: t.times.fetch_ms,
                    pull_ms: t.times.pull_ms,
                    // Agents from 1.4 on leave the pull out of `wall_ms`;
                    // an older one's `wall_ms` holds it, unmeasured.
                    run_ms: t.wall_ms,
                    upload_ms: t.times.upload_ms,
                })
                .collect(),
        )
    }

    fn online_labels(&self) -> HashMap<String, LabelSet> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.online)
            .map(|(id, _)| (id.clone(), self.node_labels(id)))
            .collect()
    }

    // -- capacity -----------------------------------------------------------

    /// Which nodes would take a new task of `account` now, by executor,
    /// and why the others would not: a plain task (one CPU, no GPU) within
    /// the account's policy.
    pub fn capacity(&self, account: &str) -> Capacity {
        let policy = self.account_policy(account);
        // An image job (1.5) runs where docker or microvm does, so it has
        // no entry of its own: a CLI older than 1.5 reads this answer too.
        let executors: Vec<Executor> = if policy.executors.is_empty() {
            vec![Executor::Wasm, Executor::Docker, Executor::Microvm]
        } else {
            policy
                .executors
                .iter()
                .copied()
                .filter(|e| *e != Executor::Image)
                .collect()
        };
        let floor = self.policy.ledger.floor();
        let can_pay = self
            .accounts
            .get(account)
            .is_some_and(|a| a.balance > floor)
            && !self.over_daily(account);
        let need_for = |executor: Executor, gpu: Option<GpuRequest>, graphics: bool| Need {
            item: QueueItem {
                job: JobId(0),
                index: 0,
                check: false,
                priority: 0,
                since: Instant::now(),
            },
            executor,
            memory: 64 * 1024,
            cpus: 1,
            store_job: false,
            gpu,
            vm_network: false,
            images: None,
            consumer: account.to_string(),
            can_pay,
            exclude_nodes: Vec::new(),
            exclude_accounts: Vec::new(),
            selector: policy.selector.clone(),
            tolerations: policy.tolerations.clone().unwrap_or_default(),
            gpu_caps: graphics,
            gpu_graphics: graphics,
            trusted_only: false,
            confidential: None,
        };
        let usage = self.account_usage();
        let at_quota = !self.within_quota(&need_for(Executor::Wasm, None, false), &usage);
        let labels = self.online_labels();
        let one_gpu = GpuRequest {
            count: 1,
            vendor: None,
            min_vram_mib: 0,
        };
        let mut cap = Capacity {
            account: account.to_string(),
            ..Default::default()
        };
        let mut ready: HashMap<Executor, u32> = HashMap::new();
        let mut ids: Vec<&String> = self.members.keys().collect();
        ids.sort_by_key(|id| (self.node_name(id), (*id).clone()));
        for id in ids {
            let name = self.node_name(id);
            let Some(n) = self.nodes.get(id).filter(|n| n.online) else {
                cap.nodes.push(NodeCapacity {
                    node: name,
                    executors: Vec::new(),
                    free_slots: 0,
                    free_gpus: 0,
                    reason: Some("offline".into()),
                    unavailable: Vec::new(),
                });
                continue;
            };
            let mut ok = Vec::new();
            let mut reasons: Vec<String> = Vec::new();
            for &e in &executors {
                let need = need_for(e, None, false);
                match self.refusal(id, n, &need, &labels, 0, &[]) {
                    None => ok.push(e),
                    Some(r) => {
                        let r = r.to_string();
                        if !reasons.contains(&r) {
                            reasons.push(r);
                        }
                    }
                }
            }
            let busy = n.gpu.busy_set();
            let free_gpus = self
                .devices
                .get(id)
                .map(|d| {
                    d.gpus()
                        .iter()
                        .filter(|g| g.usable() && !busy.contains(&g.index))
                        .count() as u32
                })
                .unwrap_or(0);
            if at_quota && !ok.is_empty() {
                ok.clear();
                reasons = vec![format!("account {account} is at its quota")];
            }
            for e in &ok {
                *ready.entry(*e).or_default() += 1;
            }
            // A policy that gives its tasks no GPU has no GPU node ready.
            let gpu_allowed = policy.max_gpus_per_task != Some(0);
            let image_gpu = |graphics: bool| {
                [Executor::Docker, Executor::Microvm]
                    .into_iter()
                    .filter(|e| {
                        gpu_allowed && ok.contains(e) && (!graphics || *e == Executor::Docker)
                    })
                    .any(|e| {
                        self.refusal(
                            id,
                            n,
                            &need_for(e, Some(one_gpu.clone()), graphics),
                            &labels,
                            0,
                            &[],
                        )
                        .is_none()
                    })
            };
            if image_gpu(false) {
                cap.ready_gpu += 1;
            }
            if image_gpu(true) {
                cap.ready_gpu_graphics += 1;
            }
            cap.nodes.push(NodeCapacity {
                node: name,
                reason: ok.is_empty().then(|| reasons.join("; ")),
                executors: ok,
                free_slots: n.info.slots.saturating_sub(n.running.len() as u32),
                free_gpus,
                unavailable: n.unavailable.clone(),
            });
        }
        cap.ready = executors
            .iter()
            .map(|e| (*e, ready.get(e).copied().unwrap_or(0)))
            .collect();
        cap
    }
}
