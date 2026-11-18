//! Result checks for work that re-running cannot check (1.20).
//!
//! - **Honeypots.** After a provider finishes a task, the control plane
//!   may slip it a task whose answer it already knows, at the rate the
//!   policy sets for the provider's trust level. It is an ordinary job to
//!   the agent: a random job id, the name, limits and executor of the
//!   task it just ran, the account it would expect. Built-in ones are made
//!   up per executor (a seeded WASI module, or a `sh -c` loop in the
//!   policy's image); an account's own are copies of its finished tasks
//!   (`pumasi honeypot add`), used once a trusted node has run them again
//!   with the same answer. A wrong answer forfeits the run's earning,
//!   strikes the node (so its results are re-run every time and its
//!   honeypot rate goes up) and keeps every earning of its account held
//!   for the policy's hold time. Honeypot jobs are left out of job lists
//!   and events.
//! - **Disk and memory challenges.** Like the CPU's, for untrusted nodes
//!   whose agents know them: the node writes seeded data, reads it back in
//!   an order the cluster picks, then reads 4 KiB blocks each chosen by
//!   the last one's data; or runs the memory kernel. The cluster times
//!   each answer, checks it against its own computation, and keeps the
//!   best timing of each claim. The `disk` label of a provider follows the
//!   timing, not the claim.
//! - **Re-timing.** Every `retime_secs` the cluster times providers' CPU,
//!   disk and memory again, and a timing well below the best one raises
//!   the benchmark alert.
//! - **Scores.** Uptime (over about a week, announced sleep not counted),
//!   check and honeypot results, and finished against lost runs make a
//!   score per node that placement prefers among equal choices.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{
    ModuleHash,
    gpu::GpuRequest,
    job::{DockerSpec, Executor, GpuCapability, JobId, JobSubmission, Limits, TaskInput, Tier},
    monitor::EventLevel,
    msg::{OWNER_ACCOUNT, Outcome, TaskRef, TaskState, ToAgent},
    perf::DiskClass,
    receipt::output_digest,
    verify::{
        BLOCKS_PER_CHUNK, DISK_CHUNK, DiskChallenge, DiskCheck, DiskStep, HoneypotResult,
        HoneypotRunView, HoneypotView, MemoryChallenge, MemoryCheck, NEW_UNTIL, ProviderScore,
        SequentialAnswer, TrustLevel, VerifyPolicy, VerifyPolicyPatch, VerifyView, chunk_order,
        class_rank, disk_folds, disk_reads, last_folds, memory_kernel, memory_passes,
        memory_traffic, prepare_answer, random_walk, timed_class,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::state::{State, now_unix, reproducible};

const POLICY_META: &str = "verify_policy";
const HONEYPOTS_META: &str = "honeypots";
const RUNS_META: &str = "honeypot_runs";
const NODES_META: &str = "verify_nodes";
const HELD_META: &str = "honeypot_held";
/// `submitted_by` of honeypot jobs: how they are known after a restart.
pub(crate) const HONEYPOT_BY: &str = "honeypot";
/// Honeypot runs kept for `pumasi honeypot`.
const RECENT: usize = 50;
/// Honeypots made from one job at most.
const MAX_PER_ADD: usize = 16;
/// Honeypots kept at most.
const MAX_HONEYPOTS: usize = 256;
/// A honeypot its node has not taken in this long is dropped.
const HONEYPOT_QUEUE_MS: u64 = 30 * 60 * 1000;
/// A confirmation run waits this long for a trusted node.
const CONFIRM_QUEUE_MS: u64 = 24 * 3600 * 1000;
/// Uptime is counted over about this half-life.
const UPTIME_HALF_LIFE_SECS: f64 = 7.0 * 86400.0;
/// Counters are written down at least this often.
const FLUSH_EVERY: Duration = Duration::from_secs(300);
/// Target length of a timed disk read step and a memory challenge.
const DISK_MS: u64 = 500;
const MEMORY_MS: u64 = 400;
/// A challenge step not answered in this long counts as answered then.
const CHALLENGE_TIMEOUT: Duration = Duration::from_secs(120);
/// Wait at least this long between two challenges to the same node.
const RETRY_GAP: Duration = Duration::from_secs(5);
/// A claim more than this multiple of the timing is inflated.
const INFLATED: u64 = 2;
/// Timings of one claim that must all look inflated before a strike.
const STRIKE_AFTER: u32 = 2;
/// The smallest disk challenge (the node's free space allowing).
const MIN_DISK_BYTES: u64 = 16 * DISK_CHUNK as u64;
/// The most a disk challenge writes ([`crate::ClusterConfig::challenge_bytes`]).
pub const DISK_CHALLENGE_BYTES: u64 = 256 << 20;
/// The most memory a memory challenge touches.
pub const MEMORY_CHALLENGE_BYTES: u64 = 128 << 20;

/// A honeypot copied from an account's finished task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HoneypotRecord {
    pub id: u32,
    pub account: String,
    pub name: String,
    pub source: JobId,
    pub task: u32,
    pub executor: Executor,
    pub module_hash: Option<ModuleHash>,
    pub docker: Option<DockerSpec>,
    pub files_hash: Option<ModuleHash>,
    pub limits: Limits,
    pub deterministic: bool,
    pub gpu: Option<GpuRequest>,
    pub gpu_capabilities: Vec<GpuCapability>,
    pub input: TaskInput,
    pub answer: [u8; 32],
    pub confirmed: bool,
    pub problem: Option<String>,
    pub sent: u32,
    pub passed: u32,
    pub failed: u32,
    pub added_unix: u64,
}

/// A honeypot job sent out and not settled yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HoneypotRun {
    /// `None` for a built-in one.
    pub honeypot: Option<u32>,
    /// The provider it is for; `None` for a confirmation run on a trusted
    /// node.
    pub node: Option<String>,
    pub answer: [u8; 32],
    pub executor: Executor,
    pub sent_unix: u64,
}

/// What the cluster knows about a node beyond its member record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct NodeVerify {
    pub honeypots_passed: u32,
    pub honeypots_failed: u32,
    pub runs_done: u64,
    pub runs_lost: u64,
    /// Decayed seconds online and seconds counted.
    pub up_secs: f64,
    pub seen_secs: f64,
    pub disk: Option<DiskCheck>,
    pub memory: Option<MemoryCheck>,
    /// Why the disk could not be timed, as the agent said.
    pub disk_error: Option<String>,
    /// When the cluster last timed it on its own (re-timing).
    pub timed_unix: u64,
}

/// What a disk challenge claims, from the profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiskClaim {
    pub class: DiskClass,
    pub seq_read_mb_s: u32,
    pub p50_us: u32,
}

/// A challenge sent and not answered yet.
#[derive(Debug, Clone)]
pub(crate) enum Pending {
    Disk {
        generation: u64,
        challenge: DiskChallenge,
        sent: Instant,
        claim: DiskClaim,
        bytes: u64,
        reads: u32,
        retime: bool,
        /// Answers and their times (minus the round trip) so far.
        answers: Vec<([u8; 32], Duration, u64)>,
    },
    Memory {
        generation: u64,
        challenge: MemoryChallenge,
        sent: Instant,
        claimed_mb_s: u32,
        retime: bool,
    },
}

/// Per node, while it is connected.
#[derive(Debug, Default)]
pub(crate) struct NodeChallenges {
    pub disk_wanted: bool,
    pub memory_wanted: bool,
    pub retime: bool,
    pub pending: Option<Pending>,
    pub last: Option<Instant>,
}

/// A finished disk challenge, checked off the state lock.
#[derive(Debug, Clone)]
pub struct DiskDone {
    pub node: String,
    pub seed: u64,
    pub bytes: u64,
    pub reads: u32,
    pub claim_class: DiskClass,
    pub claim_seq: u32,
    pub claim_p50: u32,
    pub retime: bool,
    /// Prepare, Sequential and Random: (nonce, answer, time).
    pub steps: Vec<(u64, [u8; 32], Duration)>,
}

impl DiskDone {
    /// Whether every answer was right (slow: it makes the data again).
    pub fn check(&self) -> bool {
        if self.steps.len() != 3 {
            return false;
        }
        let folds = disk_folds(self.seed, self.bytes);
        let (n0, a0, _) = self.steps[0];
        if prepare_answer(n0, last_folds(&folds)) != a0 {
            return false;
        }
        let (n1, a1, _) = self.steps[1];
        let chunks = (self.bytes / DISK_CHUNK as u64) as u32;
        let mut seq = SequentialAnswer::new(n1);
        for c in chunk_order(self.seed, n1, chunks) {
            let c = c as usize;
            seq.chunk(&folds[c * BLOCKS_PER_CHUNK..(c + 1) * BLOCKS_PER_CHUNK]);
        }
        if seq.finish() != a1 {
            return false;
        }
        let (n2, a2, _) = self.steps[2];
        random_walk(n2, self.reads, folds.len() as u64, |i| {
            Ok::<_, ()>(folds[i as usize])
        })
        .is_ok_and(|a| a == a2)
    }
}

/// A finished memory challenge.
#[derive(Debug, Clone)]
pub struct MemoryDone {
    pub node: String,
    pub challenge: MemoryChallenge,
    pub answer: [u8; 32],
    pub took: Duration,
    pub claimed_mb_s: u32,
    pub retime: bool,
}

impl MemoryDone {
    pub fn check(&self) -> bool {
        let c = self.challenge;
        let r = memory_kernel(c.seed, c.bytes, c.passes);
        self.answer[..8] == r.to_le_bytes()
    }
}

/// What to do with a challenge answer.
pub enum Answered {
    /// Nothing (stale, or the next step was sent).
    Nothing,
    Disk(Box<DiskDone>),
    Memory(Box<MemoryDone>),
}

pub(crate) struct VerifyState {
    pub policy: VerifyPolicy,
    pub honeypots: BTreeMap<u32, HoneypotRecord>,
    pub runs: HashMap<JobId, HoneypotRun>,
    pub recent: VecDeque<HoneypotRunView>,
    pub nodes: HashMap<String, NodeVerify>,
    pub held: BTreeMap<String, u64>,
    pub challenges: HashMap<String, NodeChallenges>,
    /// Honeypot jobs by the node they are kept for.
    pub pinned: HashMap<JobId, String>,
    /// Size of disk and memory challenges (smaller in tests).
    pub disk_bytes: u64,
    pub memory_bytes: u64,
    pub last_tick: Option<Instant>,
    pub last_flush: Option<Instant>,
    pub dirty: bool,
}

impl Default for VerifyState {
    fn default() -> Self {
        Self {
            policy: VerifyPolicy::default(),
            honeypots: BTreeMap::new(),
            runs: HashMap::new(),
            recent: VecDeque::new(),
            nodes: HashMap::new(),
            held: BTreeMap::new(),
            challenges: HashMap::new(),
            pinned: HashMap::new(),
            disk_bytes: DISK_CHALLENGE_BYTES,
            memory_bytes: MEMORY_CHALLENGE_BYTES,
            last_tick: None,
            last_flush: None,
            dirty: false,
        }
    }
}

impl State {
    // -- loading and saving ---------------------------------------------------

    pub(crate) fn load_verify(&mut self) -> Result<()> {
        let v = &mut self.verify;
        v.policy = self.store.meta_record(POLICY_META)?.unwrap_or_default();
        v.honeypots = self
            .store
            .meta_record::<Vec<HoneypotRecord>>(HONEYPOTS_META)?
            .unwrap_or_default()
            .into_iter()
            .map(|h| (h.id, h))
            .collect();
        v.runs = self
            .store
            .meta_record::<Vec<(JobId, HoneypotRun)>>(RUNS_META)?
            .unwrap_or_default()
            .into_iter()
            .collect();
        v.pinned = v
            .runs
            .iter()
            .filter_map(|(j, r)| Some((*j, r.node.clone()?)))
            .collect();
        v.nodes = self
            .store
            .meta_record::<Vec<(String, NodeVerify)>>(NODES_META)?
            .unwrap_or_default()
            .into_iter()
            .collect();
        v.held = self
            .store
            .meta_record::<Vec<(String, u64)>>(HELD_META)?
            .unwrap_or_default()
            .into_iter()
            .collect();
        Ok(())
    }

    fn save_honeypots(&self) {
        let rows: Vec<&HoneypotRecord> = self.verify.honeypots.values().collect();
        if let Err(e) = self.store.put_meta_record(HONEYPOTS_META, &rows) {
            warn!("persist honeypots: {e:#}");
        }
    }

    fn save_runs(&self) {
        let rows: Vec<(&JobId, &HoneypotRun)> = self.verify.runs.iter().collect();
        if let Err(e) = self.store.put_meta_record(RUNS_META, &rows) {
            warn!("persist honeypot runs: {e:#}");
        }
    }

    fn save_held(&self) {
        let rows: Vec<(&String, &u64)> = self.verify.held.iter().collect();
        if let Err(e) = self.store.put_meta_record(HELD_META, &rows) {
            warn!("persist held accounts: {e:#}");
        }
    }

    pub(crate) fn save_verify_nodes(&mut self) {
        let mut rows: Vec<(&String, &NodeVerify)> = self
            .verify
            .nodes
            .iter()
            .filter(|(id, _)| self.members.contains_key(*id))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(b.0));
        if let Err(e) = self.store.put_meta_record(NODES_META, &rows) {
            warn!("persist node checks: {e:#}");
        }
        self.verify.dirty = false;
        self.verify.last_flush = Some(Instant::now());
    }

    fn node_verify(&mut self, id: &str) -> &mut NodeVerify {
        self.verify.nodes.entry(id.to_string()).or_default()
    }

    // -- the policy -------------------------------------------------------------

    pub fn verify_policy(&self) -> VerifyPolicy {
        self.verify.policy.clone()
    }

    pub fn set_verify_policy(&mut self, patch: &VerifyPolicyPatch) -> Result<()> {
        let mut p = self.verify.policy.clone();
        patch.apply(&mut p);
        p.validate()?;
        self.store.put_meta_record(POLICY_META, &p)?;
        info!(?p, "honeypot policy set");
        self.verify.policy = p;
        Ok(())
    }

    /// Releases an account's earnings held after a failed honeypot.
    pub fn release_held(&mut self, account: &str) -> Result<()> {
        ensure!(
            self.verify.held.remove(account).is_some(),
            "account {account} has no earnings held by a honeypot"
        );
        self.save_held();
        info!(%account, "held earnings released");
        Ok(())
    }

    /// Whether an account's earnings are held by a failed honeypot.
    pub(crate) fn held_by_honeypot(&self, account: &str, now: u64) -> bool {
        self.verify.held.get(account).is_some_and(|&t| t > now)
    }

    // -- trust and scores ---------------------------------------------------------

    /// A node's trust level, which sets its honeypot rate.
    pub(crate) fn trust_level(&self, id: &str) -> TrustLevel {
        let Some(m) = self.members.get(id) else {
            return TrustLevel::Owner;
        };
        if m.trusted() {
            TrustLevel::Owner
        } else if self.conf.trusted_nodes.contains(id) {
            TrustLevel::Trusted
        } else if m.strikes > 0 {
            TrustLevel::Struck
        } else if m.checks_passed + self.verify.nodes.get(id).map_or(0, |v| v.honeypots_passed)
            < NEW_UNTIL
        {
            TrustLevel::New
        } else {
            TrustLevel::Provider
        }
    }

    /// A node's score and what went into it.
    pub fn provider_score(&self, id: &str) -> Option<ProviderScore> {
        let m = self.members.get(id)?;
        let v = self.verify.nodes.get(id).cloned().unwrap_or_default();
        let level = self.trust_level(id);
        let uptime = if v.seen_secs < 1.0 {
            1000
        } else {
            ((v.up_secs / v.seen_secs) * 1000.0)
                .round()
                .clamp(0.0, 1000.0) as u32
        };
        let suspended = !m.trusted() && m.strikes >= self.policy.max_strikes;
        let (score, checks, completion) = pumasi_proto::verify::score(
            uptime,
            level,
            m.checks_passed + v.honeypots_passed,
            m.strikes,
            v.runs_done,
            v.runs_lost,
            suspended,
        );
        Some(ProviderScore {
            node: id.to_string(),
            name: self.node_name(id),
            account: m.account.clone(),
            score,
            level,
            uptime_permille: uptime,
            checks_permille: checks,
            completion_permille: completion,
            checks_passed: m.checks_passed,
            strikes: m.strikes,
            honeypots_passed: v.honeypots_passed,
            honeypots_failed: v.honeypots_failed,
            runs_done: v.runs_done,
            runs_lost: v.runs_lost,
            disk: v.disk.clone(),
            memory: v.memory.clone(),
            disk_label: self.disk_label(id),
            held_until: self.verify.held.get(&m.account).copied(),
        })
    }

    /// The score placement compares, in steps of 100 so that close scores
    /// still spread work by free slots.
    pub(crate) fn placement_score(&self, id: &str) -> u32 {
        self.provider_score(id).map_or(0, |s| s.score / 100)
    }

    /// The node's `disk` label: the profile's class on owner machines; on
    /// providers the slower of the claim and the cluster's timing, and
    /// none until it was timed.
    pub(crate) fn disk_label(&self, id: &str) -> Option<DiskClass> {
        let m = self.members.get(id)?;
        let d = m.perf.as_ref()?.profile.disk.as_ref()?;
        class_rank(d.class)?;
        if m.trusted() {
            return Some(d.class);
        }
        let c = self.verify.nodes.get(id)?.disk.as_ref()?;
        if !(c.correct && claim_of(d) == claim_from_check(c)) {
            return None;
        }
        let timed = class_rank(c.class)?;
        Some(if timed < class_rank(d.class)? {
            c.class
        } else {
            d.class
        })
    }

    pub fn verify_view(&self) -> VerifyView {
        let mut scores: Vec<ProviderScore> = self
            .members
            .keys()
            .filter_map(|id| self.provider_score(id))
            .collect();
        scores.sort_by(|a, b| a.name.cmp(&b.name).then(a.node.cmp(&b.node)));
        let now = now_unix();
        let mut runs: Vec<HoneypotRunView> = self
            .verify
            .runs
            .iter()
            .filter_map(|(job, r)| {
                let node = r.node.clone()?;
                Some(HoneypotRunView {
                    job: *job,
                    node_name: self.node_name(&node),
                    node,
                    honeypot: r.honeypot,
                    executor: r.executor,
                    sent_unix: r.sent_unix,
                    result: None,
                })
            })
            .collect();
        runs.sort_by_key(|r| std::cmp::Reverse(r.sent_unix));
        runs.extend(self.verify.recent.iter().cloned());
        runs.truncate(RECENT);
        VerifyView {
            policy: self.verify.policy.clone(),
            honeypots: self
                .verify
                .honeypots
                .values()
                .map(|h| HoneypotView {
                    id: h.id,
                    account: h.account.clone(),
                    name: h.name.clone(),
                    executor: h.executor,
                    source: h.source,
                    task: h.task,
                    confirmed: h.confirmed,
                    problem: h.problem.clone(),
                    sent: h.sent,
                    passed: h.passed,
                    failed: h.failed,
                    added_unix: h.added_unix,
                })
                .collect(),
            runs,
            scores,
            held: self
                .verify
                .held
                .iter()
                .filter(|(_, t)| **t > now)
                .map(|(a, t)| (a.clone(), *t))
                .collect(),
        }
    }

    // -- counters -----------------------------------------------------------------

    /// A run that came back (any outcome but an agent error).
    pub(crate) fn note_done(&mut self, node: &str) {
        self.node_verify(node).runs_done += 1;
        self.verify.dirty = true;
    }

    /// Runs a node lost: it vanished with them, sent a forged receipt, or
    /// its agent failed them.
    pub(crate) fn note_lost(&mut self, node: &str, n: u64) {
        if n == 0 || !self.members.contains_key(node) {
            return;
        }
        self.node_verify(node).runs_lost += n;
        self.verify.dirty = true;
    }

    pub(crate) fn forget_verify(&mut self, node: &str) {
        self.verify.nodes.remove(node);
        self.verify.challenges.remove(node);
        self.save_verify_nodes();
    }

    /// Counts uptime, re-times providers that are due, settles honeypots
    /// that ended without a report and writes the counters down now and
    /// then.
    pub(crate) fn tick_verify(&mut self) {
        let now = Instant::now();
        let dt = self
            .verify
            .last_tick
            .map(|t| now.duration_since(t).as_secs_f64())
            .unwrap_or(0.0);
        self.verify.last_tick = Some(now);
        if dt > 0.0 {
            let keep = 0.5f64.powf(dt / UPTIME_HALF_LIFE_SECS);
            let ids: Vec<String> = self.members.keys().cloned().collect();
            for id in ids {
                if self.asleep(&id) {
                    continue;
                }
                let online = self.nodes.get(&id).is_some_and(|n| n.online);
                let v = self.node_verify(&id);
                v.up_secs = v.up_secs * keep + if online { dt } else { 0.0 };
                v.seen_secs = v.seen_secs * keep + dt;
            }
            self.verify.dirty = true;
        }
        self.retime_due();
        self.settle_lost_honeypots();
        self.expire_verify_challenges();
        if self.verify.dirty
            && self
                .verify
                .last_flush
                .is_none_or(|t| t.elapsed() >= FLUSH_EVERY)
        {
            self.save_verify_nodes();
        }
    }

    // -- honeypots ------------------------------------------------------------------

    /// Whether a job is a honeypot (kept out of lists and events).
    pub(crate) fn is_honeypot_job(&self, id: JobId) -> bool {
        self.jobs
            .get(&id)
            .is_some_and(|j| j.meta.origin.submitted_by == HONEYPOT_BY)
    }

    /// Module and file bundles the honeypots need kept.
    pub(crate) fn honeypot_blobs(&self) -> Vec<ModuleHash> {
        self.verify
            .honeypots
            .values()
            .flat_map(|h| [h.module_hash, h.files_hash])
            .flatten()
            .collect()
    }

    /// Makes honeypots of a finished job's succeeded tasks (or one of
    /// them), and asks a trusted node to confirm each.
    pub fn add_honeypots(&mut self, job: JobId, task: Option<u32>) -> Result<Vec<u32>> {
        let j = self.jobs.get(&job).context("no such job")?;
        ensure!(!self.is_honeypot_job(job), "that job is a honeypot itself");
        let m = &j.meta;
        ensure!(
            m.tier != Tier::Confidential,
            "confidential jobs cannot be honeypots (their results are sealed)"
        );
        ensure!(
            !m.ext.needs_store && !m.ext.private_image,
            "jobs that use the file store cannot be honeypots yet"
        );
        ensure!(
            m.place.checkpoint.is_none() && !m.place.gang,
            "checkpointing jobs and gangs cannot be honeypots"
        );
        ensure!(
            m.net.is_empty(),
            "jobs with a network allow list or cap cannot be honeypots"
        );
        let taken = |i: u32| {
            self.verify
                .honeypots
                .values()
                .any(|h| h.source == job && h.task == i)
        };
        let indexes: Vec<u32> = match task {
            Some(t) => {
                ensure!(!taken(t), "task {t} is a honeypot already");
                let rec = j.tasks.get(t as usize).context("no such task")?;
                ensure!(
                    rec.state == TaskState::Succeeded,
                    "task {t} did not succeed"
                );
                vec![t]
            }
            None => (0..j.tasks.len() as u32)
                .filter(|&i| j.tasks[i as usize].state == TaskState::Succeeded && !taken(i))
                .take(MAX_PER_ADD)
                .collect(),
        };
        ensure!(
            !indexes.is_empty(),
            "the job has no succeeded task that is not a honeypot already"
        );
        ensure!(
            self.verify.honeypots.len() + indexes.len() <= MAX_HONEYPOTS,
            "{MAX_HONEYPOTS} honeypots at most; remove some first"
        );
        if let Some(h) = m.module_hash {
            ensure!(
                self.modules.contains_key(&h),
                "the job's module is gone from the control plane"
            );
        }
        let executor = match m.executor {
            Executor::Image => j
                .tasks
                .get(indexes[0] as usize)
                .and_then(|t| t.ran)
                .unwrap_or(Executor::Image),
            e => e,
        };
        // All or none.
        for &i in &indexes {
            let t = &j.tasks[i as usize];
            ensure!(
                t.ext.mounts.is_empty() && t.ext.stored.is_none(),
                "tasks that use the file store cannot be honeypots yet"
            );
        }
        let first = self.verify.honeypots.keys().max().map_or(1, |k| k + 1);
        let mut made = Vec::new();
        for (next, i) in (first..).zip(indexes) {
            let t = &j.tasks[i as usize];
            let answer = output_digest(&Outcome::Exited(0), &t.stdout, &t.output_files, None);
            let rec = HoneypotRecord {
                id: next,
                account: m.account.clone(),
                name: m.name.clone(),
                source: job,
                task: i,
                executor,
                module_hash: m.module_hash,
                docker: m.docker.clone(),
                files_hash: m.files_hash,
                limits: m.limits,
                deterministic: m.deterministic,
                gpu: m.gpu.clone(),
                gpu_capabilities: m.origin.gpu_capabilities.clone(),
                input: t.input.clone(),
                answer,
                confirmed: false,
                problem: None,
                sent: 0,
                passed: 0,
                failed: 0,
                added_unix: now_unix(),
            };
            made.push(rec.id);
            self.verify.honeypots.insert(rec.id, rec);
        }
        self.save_honeypots();
        for id in &made {
            if let Err(e) = self.confirm_honeypot(*id) {
                warn!(honeypot = id, "could not ask for a confirmation run: {e:#}");
            }
        }
        info!(job = %job, honeypots = made.len(), "honeypots added");
        Ok(made)
    }

    pub fn remove_honeypot(&mut self, id: u32) -> Result<()> {
        ensure!(
            self.verify.honeypots.remove(&id).is_some(),
            "no honeypot {id}"
        );
        self.save_honeypots();
        Ok(())
    }

    /// The submission of a honeypot (copied) for `account`.
    fn honeypot_submission(&self, h: &HoneypotRecord) -> Result<JobSubmission> {
        let module = match h.module_hash {
            Some(hash) => self
                .modules
                .get(&hash)
                .context("the honeypot's module is gone")?
                .to_vec(),
            None => Vec::new(),
        };
        let files = match h.files_hash {
            Some(hash) => {
                let b = self
                    .modules
                    .get(&hash)
                    .context("the honeypot's input files are gone")?;
                postcard::from_bytes(b)?
            }
            None => Vec::new(),
        };
        let mut sub = blank_submission(h.name.clone(), h.executor, h.limits);
        sub.module = module;
        sub.docker = h.docker.clone();
        sub.files = files;
        sub.tasks = vec![h.input.clone()];
        sub.account = Some(h.account.clone());
        sub.deterministic = h.deterministic;
        sub.gpu = h.gpu.clone();
        sub.gpu_capabilities = h.gpu_capabilities.clone();
        Ok(sub)
    }

    /// Runs a new honeypot on a trusted node to see that its answer holds.
    fn confirm_honeypot(&mut self, id: u32) -> Result<()> {
        let h = self.verify.honeypots.get(&id).context("no such honeypot")?;
        let mut sub = self.honeypot_submission(h)?;
        sub.tier = Tier::Trusted;
        sub.queue_timeout_ms = Some(CONFIRM_QUEUE_MS);
        let run = HoneypotRun {
            honeypot: Some(id),
            node: None,
            answer: h.answer,
            executor: h.executor,
            sent_unix: now_unix(),
        };
        self.submit_honeypot(sub, run, None, Vec::new())?;
        Ok(())
    }

    fn submit_honeypot(
        &mut self,
        sub: JobSubmission,
        run: HoneypotRun,
        node: Option<&str>,
        tolerations: Vec<pumasi_proto::manage::Toleration>,
    ) -> Result<JobId> {
        let mut sub = sub;
        sub.tolerations = tolerations;
        let id = self.submit_by(sub, false, None, HONEYPOT_BY.into())?;
        if let Some(n) = node {
            self.verify.pinned.insert(id, n.to_string());
        }
        self.verify.runs.insert(id, run);
        self.save_runs();
        Ok(id)
    }

    /// After a provider finished a task of job `after`: maybe a honeypot
    /// for it, made to look like that task.
    pub(crate) fn maybe_honeypot(&mut self, node: &str, after: TaskRef) {
        let level = self.trust_level(node);
        let rate = self.verify.policy.permille(level);
        if rate == 0 || self.is_honeypot_job(after.job) {
            return;
        }
        if self
            .verify
            .runs
            .values()
            .any(|r| r.node.as_deref() == Some(node))
        {
            return;
        }
        if rand::random_range(0..1000u32) >= rate {
            return;
        }
        let Some(j) = self.jobs.get(&after.job) else {
            return;
        };
        let executor = match j.meta.executor {
            Executor::Image => j
                .tasks
                .get(after.index as usize)
                .and_then(|t| t.ran)
                .unwrap_or(Executor::Docker),
            e => e,
        };
        let account = j.meta.account.clone();
        let name = j.meta.name.clone();
        let mut limits = j.meta.limits;
        limits.fuel = None;
        limits.timeout_ms = limits.timeout_ms.max(120_000);
        let deterministic = j.meta.deterministic;
        let tolerations = j.meta.sched.tolerations.clone();
        // The account's own honeypots of this executor first.
        let own: Vec<u32> = self
            .verify
            .honeypots
            .values()
            .filter(|h| {
                h.confirmed && h.problem.is_none() && h.account == account && h.executor == executor
            })
            .map(|h| h.id)
            .collect();
        let made = if !own.is_empty() {
            let id = own[rand::random_range(0..own.len())];
            let h = self.verify.honeypots[&id].clone();
            self.honeypot_submission(&h).map(|sub| {
                (
                    sub,
                    HoneypotRun {
                        honeypot: Some(id),
                        node: Some(node.to_string()),
                        answer: h.answer,
                        executor,
                        sent_unix: now_unix(),
                    },
                )
            })
        } else {
            builtin_honeypot(&self.verify.policy, executor, name, limits, deterministic).map(
                |(sub, answer)| {
                    (
                        sub,
                        HoneypotRun {
                            honeypot: None,
                            node: Some(node.to_string()),
                            answer,
                            executor,
                            sent_unix: now_unix(),
                        },
                    )
                },
            )
        };
        let (mut sub, run) = match made {
            Ok(m) => m,
            Err(e) => {
                debug!(node = %node, "no honeypot: {e:#}");
                return;
            }
        };
        sub.queue_timeout_ms = Some(HONEYPOT_QUEUE_MS);
        let honeypot = run.honeypot;
        match self.submit_honeypot(sub, run, Some(node), tolerations) {
            Ok(id) => {
                if let Some(h) = honeypot.and_then(|h| self.verify.honeypots.get_mut(&h)) {
                    h.sent += 1;
                    self.save_honeypots();
                }
                debug!(node = %node, job = %id, "honeypot sent");
            }
            Err(e) => debug!(node = %node, "no honeypot: {e:#}"),
        }
    }

    /// The node a honeypot job is kept for.
    pub(crate) fn honeypot_node(&self, job: JobId) -> Option<&str> {
        self.verify.pinned.get(&job).map(|s| s.as_str())
    }

    /// A honeypot's report: compared with the known answer.
    pub(crate) fn on_honeypot_report(
        &mut self,
        node: &str,
        r: TaskRef,
        outcome: &Outcome,
        digest: [u8; 32],
    ) {
        let Some(run) = self.verify.runs.remove(&r.job) else {
            return;
        };
        self.verify.pinned.remove(&r.job);
        self.save_runs();
        let result = if !reproducible(outcome) {
            HoneypotResult::Inconclusive(outcome.to_string())
        } else if digest == run.answer {
            HoneypotResult::Passed
        } else {
            HoneypotResult::Failed
        };
        // A confirmation run on a trusted node.
        let Some(target) = run.node.clone() else {
            if let Some(h) = run.honeypot.and_then(|h| self.verify.honeypots.get_mut(&h)) {
                match &result {
                    HoneypotResult::Passed => {
                        h.confirmed = true;
                        info!(honeypot = h.id, "honeypot confirmed");
                    }
                    HoneypotResult::Failed => {
                        h.problem = Some(format!(
                            "its result changed when {} ran it again: it is not deterministic",
                            self.members
                                .get(node)
                                .map(|m| m.info.name.clone())
                                .unwrap_or_else(|| node.to_string())
                        ));
                        warn!(honeypot = h.id, "honeypot's answer changed; not used");
                    }
                    HoneypotResult::Inconclusive(why) => {
                        h.problem = Some(format!("the confirmation run ended with {why}"));
                    }
                }
                self.save_honeypots();
            }
            return;
        };
        if target != node {
            return;
        }
        let account = self
            .members
            .get(node)
            .map(|m| m.account.clone())
            .unwrap_or_default();
        match &result {
            HoneypotResult::Passed => {
                self.node_verify(node).honeypots_passed += 1;
                if let Some(h) = run.honeypot.and_then(|h| self.verify.honeypots.get_mut(&h)) {
                    h.passed += 1;
                }
            }
            HoneypotResult::Failed => {
                self.node_verify(node).honeypots_failed += 1;
                if let Some(h) = run.honeypot.and_then(|h| self.verify.honeypots.get_mut(&h)) {
                    h.failed += 1;
                }
                self.forfeit(r, "wrong answer to a honeypot task");
                self.strike(node, "wrong answer to a honeypot task");
                let until = now_unix() + self.verify.policy.hold_secs;
                let held = self.verify.held.entry(account.clone()).or_default();
                *held = (*held).max(until);
                self.save_held();
                let name = self.node_name(node);
                let text = pumasi_proto::text::Text::new(
                    "honeypot.failed",
                    [
                        name.clone(),
                        account.clone(),
                        pumasi_proto::monitor::human_duration(self.verify.policy.hold_secs),
                    ],
                );
                let msg = text.en();
                warn!(node = %node, "{msg}");
                self.event(
                    EventLevel::Critical,
                    "honeypot.failed",
                    Some(node),
                    None,
                    text,
                );
                self.alert_once("honeypot", &name, EventLevel::Critical, &msg);
            }
            HoneypotResult::Inconclusive(_) => {}
        }
        if run.honeypot.is_some() {
            self.save_honeypots();
        }
        self.save_verify_nodes();
        self.verify.recent.push_front(HoneypotRunView {
            job: r.job,
            node: node.to_string(),
            node_name: self.node_name(node),
            honeypot: run.honeypot,
            executor: run.executor,
            sent_unix: run.sent_unix,
            result: Some(result),
        });
        self.verify.recent.truncate(RECENT);
    }

    /// Honeypot jobs that ended without a report (the node left, the queue
    /// timed out): nobody is blamed.
    fn settle_lost_honeypots(&mut self) {
        let ended: Vec<(JobId, String)> = self
            .verify
            .runs
            .keys()
            .filter_map(|id| match self.jobs.get(id) {
                None => Some((*id, "gone".to_string())),
                Some(j) => match j.tasks.first().map(|t| &t.state) {
                    Some(TaskState::Failed(o)) => Some((*id, o.to_string())),
                    Some(TaskState::Cancelled) => Some((*id, "cancelled".into())),
                    Some(TaskState::Succeeded) => Some((*id, "no report".into())),
                    _ => None,
                },
            })
            .collect();
        if ended.is_empty() {
            return;
        }
        for (id, why) in ended {
            let Some(run) = self.verify.runs.remove(&id) else {
                continue;
            };
            self.verify.pinned.remove(&id);
            match run.node {
                Some(node) => {
                    self.verify.recent.push_front(HoneypotRunView {
                        job: id,
                        node_name: self.node_name(&node),
                        node,
                        honeypot: run.honeypot,
                        executor: run.executor,
                        sent_unix: run.sent_unix,
                        result: Some(HoneypotResult::Inconclusive(why)),
                    });
                }
                None => {
                    if let Some(h) = run.honeypot.and_then(|h| self.verify.honeypots.get_mut(&h)) {
                        h.problem = Some(format!("no trusted node confirmed it: {why}"));
                    }
                    self.save_honeypots();
                }
            }
        }
        self.verify.recent.truncate(RECENT);
        self.save_runs();
    }

    // -- challenges -----------------------------------------------------------------

    /// After a new profile from an untrusted node: time its disk and memory
    /// if the claim is new.
    pub(crate) fn want_verify_challenges(&mut self, id: &str) {
        if self.agent_minor(id) < pumasi_proto::VERIFY_MINOR {
            return;
        }
        let Some(m) = self.members.get(id) else {
            return;
        };
        if m.trusted() {
            return;
        }
        let Some(p) = m.perf.as_ref().map(|p| &p.profile) else {
            return;
        };
        let v = self.verify.nodes.get(id);
        let disk = p.disk.as_ref().is_some_and(|d| {
            d.direct
                && class_rank(d.class).is_some()
                && v.and_then(|v| v.disk.as_ref())
                    .is_none_or(|c| claim_from_check(c) != claim_of(d))
        });
        let memory = p.memory.copy_mb_s > 0
            && v.and_then(|v| v.memory.as_ref())
                .is_none_or(|c| c.claimed_mb_s != p.memory.copy_mb_s);
        let c = self.verify.challenges.entry(id.to_string()).or_default();
        c.disk_wanted |= disk;
        c.memory_wanted |= memory;
    }

    /// Whether a node waits for (or is answering) a disk or memory
    /// challenge, and so gets no tasks.
    pub(crate) fn verify_busy(&self, id: &str) -> bool {
        self.verify
            .challenges
            .get(id)
            .is_some_and(|c| c.pending.is_some() || c.disk_wanted || c.memory_wanted)
    }

    /// Providers due to be timed again.
    fn retime_due(&mut self) {
        let every = self.verify.policy.retime_secs;
        if every == 0 {
            return;
        }
        let now = now_unix();
        let due: Vec<String> = self
            .nodes
            .iter()
            .filter(|(id, n)| {
                n.online
                    && !n.trusted
                    && n.perf.protocol >= 7
                    && self.members.get(*id).is_some_and(|m| m.perf.is_some())
            })
            .map(|(id, _)| id.clone())
            .filter(|id| {
                let v = self.verify.nodes.get(id);
                let last = v.map_or(0, |v| v.timed_unix);
                last == 0 || now.saturating_sub(last) >= every
            })
            .collect();
        for id in due {
            let first = self.verify.nodes.get(&id).is_none_or(|v| v.timed_unix == 0);
            self.node_verify(&id).timed_unix = now;
            self.verify.dirty = true;
            if first {
                // Timed when it joined or sent its profile; the clock
                // starts now.
                continue;
            }
            info!(node = %id, "timing the node's CPU, disk and memory again");
            if let Some(n) = self.nodes.get_mut(&id) {
                n.perf.wanted = true;
                n.perf.retime = true;
            }
            if self.agent_minor(&id) >= pumasi_proto::VERIFY_MINOR {
                let disk = self
                    .members
                    .get(&id)
                    .and_then(|m| m.perf.as_ref())
                    .and_then(|p| p.profile.disk.as_ref())
                    .is_some_and(|d| d.direct && class_rank(d.class).is_some());
                let c = self.verify.challenges.entry(id.clone()).or_default();
                c.disk_wanted |= disk;
                c.memory_wanted = true;
                c.retime = true;
            }
        }
    }

    /// Sends disk and memory challenges to idle nodes that should be timed
    /// (after the CPU challenge, which goes first).
    pub(crate) fn drive_verify_challenges(&mut self) {
        let now = Instant::now();
        let ids: Vec<String> = self
            .verify
            .challenges
            .iter()
            .filter(|(_, c)| c.pending.is_none() && (c.disk_wanted || c.memory_wanted))
            .filter(|(_, c)| c.last.is_none_or(|t| t.elapsed() >= RETRY_GAP))
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let Some(n) = self.nodes.get(&id) else {
                self.verify.challenges.remove(&id);
                continue;
            };
            if !n.online
                || n.perf.wanted
                || n.perf.challenge.is_some()
                || !n.running.is_empty()
                || self.upgrading.contains_key(&id)
                || n.tx.is_none()
            {
                continue;
            }
            let generation = n.generation;
            let tx = n.tx.clone().expect("checked");
            let rtt_free_mem = n.info.memory_bytes;
            let Some(profile) = self
                .members
                .get(&id)
                .and_then(|m| m.perf.as_ref())
                .map(|p| p.profile.clone())
            else {
                self.verify.challenges.remove(&id);
                continue;
            };
            let disk_bytes = self.verify.disk_bytes;
            let memory_bytes = self.verify.memory_bytes;
            let c = self.verify.challenges.get_mut(&id).expect("listed");
            let retime = c.retime;
            if c.disk_wanted {
                c.disk_wanted = false;
                let Some(d) = profile.disk.as_ref().filter(|d| d.direct) else {
                    continue;
                };
                // A quarter of its free space at most.
                let room = (d.free_bytes / 4) / DISK_CHUNK as u64 * DISK_CHUNK as u64;
                let bytes = disk_bytes.min(room);
                if bytes < MIN_DISK_BYTES.min(disk_bytes) {
                    debug!(node = %id, "too little free disk for a disk challenge");
                    continue;
                }
                let challenge = DiskChallenge {
                    nonce: rand::random(),
                    seed: rand::random(),
                    step: DiskStep::Prepare { bytes },
                };
                if tx.send(ToAgent::DiskChallenge(challenge)).is_err() {
                    continue;
                }
                info!(node = %id, bytes, class = %d.class, "timing the node's disk");
                c.last = Some(now);
                c.pending = Some(Pending::Disk {
                    generation,
                    challenge,
                    sent: now,
                    claim: claim_of(d),
                    bytes,
                    reads: disk_reads(d.rand_read_p50_us, DISK_MS),
                    retime,
                    answers: Vec::new(),
                });
            } else if c.memory_wanted {
                c.memory_wanted = false;
                let claimed = profile.memory.copy_mb_s;
                if claimed == 0 {
                    continue;
                }
                let bytes = memory_bytes.min(rtt_free_mem / 8).max(1 << 20);
                let challenge = MemoryChallenge {
                    nonce: rand::random(),
                    seed: rand::random(),
                    bytes,
                    passes: memory_passes(claimed, bytes, MEMORY_MS),
                };
                if tx.send(ToAgent::MemoryChallenge(challenge)).is_err() {
                    continue;
                }
                info!(node = %id, claimed, passes = challenge.passes, "timing the node's memory");
                c.last = Some(now);
                c.pending = Some(Pending::Memory {
                    generation,
                    challenge,
                    sent: now,
                    claimed_mb_s: claimed,
                    retime,
                });
            }
            if !c.disk_wanted && !c.memory_wanted && c.pending.is_none() {
                c.retime = false;
            }
        }
    }

    /// Takes a node's answer to a challenge step: sends the next step, or
    /// hands back a finished challenge for the caller to check (off the
    /// state lock).
    pub fn on_challenge_result(
        &mut self,
        id: &str,
        generation: u64,
        nonce: u64,
        result: Result<[u8; 32], String>,
    ) -> Answered {
        let rtt = Duration::from_micros(
            self.nodes.get(id).and_then(|n| n.perf.rtt_us).unwrap_or(0) as u64,
        );
        let tx = self.nodes.get(id).and_then(|n| n.tx.clone());
        let Some(c) = self.verify.challenges.get_mut(id) else {
            return Answered::Nothing;
        };
        let matches = match &c.pending {
            Some(Pending::Disk {
                generation: g,
                challenge,
                ..
            }) => *g == generation && challenge.nonce == nonce,
            Some(Pending::Memory {
                generation: g,
                challenge,
                ..
            }) => *g == generation && challenge.nonce == nonce,
            None => false,
        };
        if !matches {
            return Answered::Nothing;
        }
        let pending = c.pending.take().expect("matched");
        let answer = match result {
            Ok(a) => a,
            Err(why) => {
                warn!(node = %id, "could not answer a challenge: {why}");
                if matches!(pending, Pending::Disk { .. }) {
                    self.node_verify(id).disk_error = Some(why);
                    self.save_verify_nodes();
                }
                self.schedule();
                return Answered::Nothing;
            }
        };
        match pending {
            Pending::Memory {
                challenge,
                sent,
                claimed_mb_s,
                retime,
                ..
            } => Answered::Memory(Box::new(MemoryDone {
                node: id.to_string(),
                challenge,
                answer,
                took: sent.elapsed().saturating_sub(rtt),
                claimed_mb_s,
                retime,
            })),
            Pending::Disk {
                generation,
                challenge,
                sent,
                claim,
                bytes,
                reads,
                retime,
                mut answers,
            } => {
                answers.push((answer, sent.elapsed().saturating_sub(rtt), challenge.nonce));
                let next = match challenge.step {
                    DiskStep::Prepare { .. } => Some(DiskStep::Sequential),
                    DiskStep::Sequential => Some(DiskStep::Random { reads }),
                    DiskStep::Random { .. } => None,
                };
                match next {
                    Some(step) => {
                        let next = DiskChallenge {
                            nonce: rand::random(),
                            seed: challenge.seed,
                            step,
                        };
                        if let Some(tx) = tx
                            && tx.send(ToAgent::DiskChallenge(next)).is_ok()
                        {
                            c.pending = Some(Pending::Disk {
                                generation,
                                challenge: next,
                                sent: Instant::now(),
                                claim,
                                bytes,
                                reads,
                                retime,
                                answers,
                            });
                        }
                        Answered::Nothing
                    }
                    None => Answered::Disk(Box::new(DiskDone {
                        node: id.to_string(),
                        seed: challenge.seed,
                        bytes,
                        reads,
                        claim_class: claim.class,
                        claim_seq: claim.seq_read_mb_s,
                        claim_p50: claim.p50_us,
                        retime,
                        steps: answers.into_iter().map(|(a, t, n)| (n, a, t)).collect(),
                    })),
                }
            }
        }
    }

    /// Records the cluster's timing of a node's disk.
    pub fn apply_disk_check(&mut self, d: &DiskDone, correct: bool) {
        let id = d.node.as_str();
        let mb_s = |bytes: u64, t: Duration| -> u32 {
            (bytes as f64 / 1e6 / t.as_secs_f64().max(1e-6)).min(u32::MAX as f64) as u32
        };
        let time = |i: usize| d.steps.get(i).map_or(CHALLENGE_TIMEOUT, |s| s.2);
        let write = mb_s(d.bytes, time(0));
        let read = mb_s(d.bytes, time(1));
        let read_us = (time(2).as_micros() / d.reads.max(1) as u128).min(u32::MAX as u128) as u32;
        let claim = DiskClaim {
            class: d.claim_class,
            seq_read_mb_s: d.claim_seq,
            p50_us: d.claim_p50,
        };
        let old = self
            .verify
            .nodes
            .get(id)
            .and_then(|v| v.disk.clone())
            .filter(|c| claim_from_check(c) == claim);
        let (best_read, best_us, best_write, attempts) = match &old {
            Some(c) if c.correct && correct => (
                c.seq_read_mb_s.max(read),
                c.read_us.min(read_us),
                c.seq_write_mb_s.max(write),
                c.attempts + 1,
            ),
            _ => (read, read_us, write, 1),
        };
        let check = DiskCheck {
            claimed_class: claim.class,
            claimed_seq_read_mb_s: claim.seq_read_mb_s,
            claimed_p50_us: claim.p50_us,
            seq_write_mb_s: best_write,
            seq_read_mb_s: best_read,
            read_us: best_us,
            class: timed_class(best_read, best_us),
            correct,
            attempts,
            unix: now_unix(),
        };
        if d.retime
            && let Some(o) = &old
        {
            self.note_timed_drop(
                id,
                &[
                    ("disk read MB/s", o.seq_read_mb_s, read),
                    // Latency: lower is better, so compare reads per second.
                    (
                        "disk reads/s",
                        1_000_000 / o.read_us.max(1),
                        1_000_000 / read_us.max(1),
                    ),
                ],
            );
        }
        let v = self.node_verify(id);
        v.disk = Some(check.clone());
        v.disk_error = None;
        self.save_verify_nodes();
        let inflated = claim.seq_read_mb_s as u64 > best_read as u64 * INFLATED + 50
            || (best_us as u64) > claim.p50_us as u64 * INFLATED + 100;
        if !correct {
            self.strike(id, "wrong answer to the disk check");
        } else if inflated && attempts < STRIKE_AFTER {
            info!(node = %id, claimed = claim.seq_read_mb_s, timed = read, "disk benchmark looks inflated; timing it again");
            self.verify
                .challenges
                .entry(id.to_string())
                .or_default()
                .disk_wanted = true;
        } else if inflated {
            let why = format!(
                "disk benchmark inflated: the profile claims {} ({} MB/s, {} us a read), the cluster timed {} ({best_read} MB/s, {best_us} us)",
                claim.class, claim.seq_read_mb_s, claim.p50_us, check.class
            );
            self.inflated_event(id, &why);
            self.strike(id, &why);
        } else {
            info!(node = %id, class = %check.class, read = best_read, read_us = best_us, "disk confirmed");
        }
        self.schedule();
    }

    /// Records the cluster's timing of a node's memory bandwidth.
    pub fn apply_memory_check(&mut self, d: &MemoryDone, correct: bool) {
        let id = d.node.as_str();
        let c = d.challenge;
        let timed =
            (memory_traffic(c.bytes, c.passes) as f64 / 1e6 / d.took.as_secs_f64().max(1e-6))
                .min(u32::MAX as f64) as u32;
        let old = self
            .verify
            .nodes
            .get(id)
            .and_then(|v| v.memory.clone())
            .filter(|m| m.claimed_mb_s == d.claimed_mb_s);
        let (best, attempts) = match &old {
            Some(m) if m.correct && correct => (m.measured_mb_s.max(timed), m.attempts + 1),
            _ => (timed, 1),
        };
        if d.retime
            && let Some(o) = &old
        {
            self.note_timed_drop(id, &[("memory MB/s", o.measured_mb_s, timed)]);
        }
        let v = self.node_verify(id);
        v.memory = Some(MemoryCheck {
            claimed_mb_s: d.claimed_mb_s,
            measured_mb_s: best,
            correct,
            attempts,
            unix: now_unix(),
        });
        self.save_verify_nodes();
        let inflated = d.claimed_mb_s as u64 > best as u64 * INFLATED;
        if !correct {
            self.strike(id, "wrong answer to the memory check");
        } else if inflated && attempts < STRIKE_AFTER {
            info!(node = %id, claimed = d.claimed_mb_s, timed, "memory benchmark looks inflated; timing it again");
            self.verify
                .challenges
                .entry(id.to_string())
                .or_default()
                .memory_wanted = true;
        } else if inflated {
            let why = format!(
                "memory benchmark inflated: the profile claims {} MB/s, the cluster timed {best}",
                d.claimed_mb_s
            );
            self.inflated_event(id, &why);
            self.strike(id, &why);
        } else {
            info!(node = %id, claimed = d.claimed_mb_s, measured = best, "memory confirmed");
        }
        self.schedule();
    }

    fn inflated_event(&mut self, id: &str, why: &str) {
        let name = self.node_name(id);
        self.event(
            EventLevel::Warning,
            "perf.inflated",
            Some(id),
            None,
            pumasi_proto::text::Text::new("perf.inflated", [name, why.to_string()]),
        );
    }

    /// A re-timing well below the best timing raises the benchmark alert.
    pub(crate) fn note_timed_drop(&mut self, id: &str, figures: &[(&str, u32, u32)]) {
        let Some(limit) = self.monitor.policy.perf_drop_percent else {
            return;
        };
        let parts: Vec<String> = figures
            .iter()
            .filter(|(_, old, new)| {
                *old > 0 && new < old && (old - new) as u64 * 100 / *old as u64 >= limit as u64
            })
            .map(|(what, old, new)| {
                format!(
                    "timed {what} {old} -> {new} (-{}%)",
                    (old - new) as u64 * 100 / *old as u64
                )
            })
            .collect();
        if parts.is_empty() {
            return;
        }
        let name = self.node_name(id);
        let text = pumasi_proto::text::Text::new("perf.drop", [name.clone(), parts.join(", ")]);
        let msg = text.en();
        self.event(EventLevel::Warning, "perf.drop", Some(id), None, text);
        self.alert_once("perf_drop", &name, EventLevel::Warning, &msg);
    }

    /// Challenge steps not answered in time count as answered now.
    fn expire_verify_challenges(&mut self) {
        let late: Vec<String> = self
            .verify
            .challenges
            .iter()
            .filter_map(|(id, c)| match &c.pending {
                Some(Pending::Disk { sent, .. } | Pending::Memory { sent, .. })
                    if sent.elapsed() > CHALLENGE_TIMEOUT =>
                {
                    Some(id.clone())
                }
                _ => None,
            })
            .collect();
        for id in late {
            // Not struck: a node busy with its owner's work, or one that
            // lost its connection, is not a liar. It is timed again when
            // its profile changes or re-timing comes round.
            warn!(node = %id, "no answer to a disk or memory check in time");
            if let Some(c) = self.verify.challenges.get_mut(&id) {
                c.pending = None;
            }
            let v = self.node_verify(&id);
            v.disk_error = Some("did not answer a challenge in time".into());
            self.save_verify_nodes();
        }
    }

    /// Drops a node's challenges when it goes offline.
    pub(crate) fn verify_offline(&mut self, id: &str) {
        if let Some(c) = self.verify.challenges.get_mut(id) {
            c.pending = None;
        }
    }
}

/// What a profile's disk claims.
fn claim_of(d: &pumasi_proto::perf::DiskPerf) -> DiskClaim {
    DiskClaim {
        class: d.class,
        seq_read_mb_s: d.seq_read_mb_s,
        p50_us: d.rand_read_p50_us,
    }
}

fn claim_from_check(c: &DiskCheck) -> DiskClaim {
    DiskClaim {
        class: c.claimed_class,
        seq_read_mb_s: c.claimed_seq_read_mb_s,
        p50_us: c.claimed_p50_us,
    }
}

/// A submission with nothing in it but a name, an executor and limits.
fn blank_submission(name: String, executor: Executor, limits: Limits) -> JobSubmission {
    JobSubmission {
        name,
        executor,
        module: Vec::new(),
        docker: None,
        files: Vec::new(),
        tasks: Vec::new(),
        limits,
        tier: Tier::Open,
        max_attempts: 1,
        max_price_per_hour: None,
        account: None,
        deterministic: false,
        mounts: Vec::new(),
        task_mounts: Vec::new(),
        output_dataset: None,
        gpu: None,
        priority: 0,
        selector: Default::default(),
        tolerations: Vec::new(),
        after: Vec::new(),
        queue_timeout_ms: None,
        gpu_capabilities: Vec::new(),
        placement: Default::default(),
        gpu_ext: Default::default(),
        net: Default::default(),
    }
}

/// A built-in honeypot for `executor`, with the name and limits of the
/// task it follows, and its answer.
fn builtin_honeypot(
    policy: &VerifyPolicy,
    executor: Executor,
    name: String,
    limits: Limits,
    deterministic: bool,
) -> Result<(JobSubmission, [u8; 32])> {
    let seed: u64 = rand::random();
    let mut sub = blank_submission(name, executor, limits);
    sub.account = Some(OWNER_ACCOUNT.into());
    sub.tasks = vec![TaskInput::default()];
    let stdout = match executor {
        Executor::Wasm => {
            let b = pumasi_proto::verify::builtin_wasm(seed);
            sub.module = wat::parse_str(&b.wat).context("built-in honeypot module")?;
            sub.deterministic = deterministic;
            // Room for its one page.
            sub.limits.memory_bytes = sub.limits.memory_bytes.max(1 << 20);
            b.stdout
        }
        Executor::Docker | Executor::Microvm => {
            if policy.image.is_empty() {
                bail!("no image for built-in honeypots");
            }
            let (command, stdout) = pumasi_proto::verify::builtin_shell(seed);
            sub.docker = Some(DockerSpec {
                image: policy.image.clone(),
                command,
                network: false,
                gpus: None,
            });
            sub.limits.memory_bytes = sub.limits.memory_bytes.max(32 << 20);
            stdout
        }
        Executor::Image => bail!("an image task runs as Docker or a microVM"),
    };
    let answer = output_digest(&Outcome::Exited(0), &stdout, &[], None);
    Ok((sub, answer))
}
