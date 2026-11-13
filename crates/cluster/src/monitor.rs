//! Monitoring (phase 11): node metrics kept as downsampled time series,
//! cluster events, live task output, alert rules and the webhooks alerts
//! go to, and the Prometheus text the `/metrics` endpoint serves. Extends
//! [`State`].

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::Write as _,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use pumasi_proto::{
    MONITOR_MINOR,
    job::{Executor, JobId, MAX_OUTPUT_BYTES},
    monitor::{
        AlertPolicy, AlertTuning, AlertTuningView, AlertView, AlertsView, ClusterMetrics,
        EventLevel, EventView, LOG_CHUNK_BYTES, LogChunk, MetricPoint, MetricsView, NodeMetrics,
        NodeMetricsView, Silence, SilenceSpec, WebhookFormat, WebhookSpec, WebhookView,
        escape_label, human_duration, redact_url,
    },
    msg::{JobState, TaskRef, TaskState},
    store::format_size,
};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::state::{State, now_unix};

/// Events kept in memory for `pumasi events` (all of them stay in the
/// database for the retention period).
const EVENT_RING: usize = 10_000;
/// Events kept in the database at most.
const MAX_STORED_EVENTS: usize = 200_000;
/// How often events and finished metric steps are written out.
const FLUSH_EVERY: Duration = Duration::from_secs(15);
/// How often old events and metrics are deleted.
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
/// Time series steps and how long each is kept (capped by the retention):
/// 10 s for an hour, 1 min for a day, 10 min for the rest. Every tier is
/// written out (the 10 s one since 1.6, so a restart keeps the last hour).
const TIERS: [(u64, u64); 3] = [(10, 3600), (60, 86_400), (600, u64::MAX)];
/// A sample older than this does not count as the node's current state.
const FRESH_SECS: u64 = 60;
/// Webhook notices kept for `recent_notices` (tests, debugging).
const RECENT_NOTICES: usize = 100;
/// Meta keys of the 1.6 alert settings.
const ALERT_TUNING_META: &str = "alert_tuning";
const SILENCES_META: &str = "alert_silences";
/// Silences kept at most.
const MAX_SILENCES: usize = 256;

/// One node's metrics: the latest sample and the time series.
pub(crate) struct NodeSeries {
    latest: Option<(u64, NodeMetrics)>,
    rates: (u64, u64),
    tiers: Vec<Tier>,
}

struct Tier {
    step: u64,
    keep: usize,
    points: VecDeque<MetricPoint>,
    acc: Option<Acc>,
}

/// Sums over one step of a tier.
#[derive(Clone, Default)]
struct Acc {
    start: u64,
    n: u32,
    cpu: f64,
    load: f64,
    mem_used: f64,
    mem_total: u64,
    disk_used: u64,
    disk_total: u64,
    rx: f64,
    tx: f64,
    temp: Option<f32>,
    gpu: f64,
    gpu_n: u32,
    gpu_mem: f64,
    running: f64,
}

impl Acc {
    fn add(&mut self, p: &MetricPoint) {
        self.n += 1;
        self.cpu += p.cpu_percent as f64;
        self.load += p.load1 as f64;
        self.mem_used += p.memory_used as f64;
        self.mem_total = p.memory_total;
        self.disk_used = p.disk_used;
        self.disk_total = p.disk_total;
        self.rx += p.net_rx_bps as f64;
        self.tx += p.net_tx_bps as f64;
        if let Some(t) = p.temp_c {
            self.temp = Some(self.temp.map_or(t, |a| a.max(t)));
        }
        if let Some(g) = p.gpu_percent {
            self.gpu += g as f64;
            self.gpu_n += 1;
        }
        self.gpu_mem += p.gpu_mem_used_mib as f64;
        self.running += p.running as f64;
    }

    fn point(&self) -> MetricPoint {
        let n = self.n.max(1) as f64;
        MetricPoint {
            unix: self.start,
            cpu_percent: (self.cpu / n) as f32,
            load1: (self.load / n) as f32,
            memory_used: (self.mem_used / n) as u64,
            memory_total: self.mem_total,
            disk_used: self.disk_used,
            disk_total: self.disk_total,
            net_rx_bps: (self.rx / n) as u64,
            net_tx_bps: (self.tx / n) as u64,
            temp_c: self.temp,
            gpu_percent: (self.gpu_n > 0).then(|| (self.gpu / self.gpu_n as f64) as f32),
            gpu_mem_used_mib: (self.gpu_mem / n) as u64,
            running: (self.running / n) as f32,
        }
    }
}

impl NodeSeries {
    fn new(retention: u64) -> Self {
        Self {
            latest: None,
            rates: (0, 0),
            tiers: TIERS
                .iter()
                .map(|&(step, window)| Tier {
                    step,
                    keep: (window.min(retention) / step).max(1) as usize,
                    points: VecDeque::new(),
                    acc: None,
                })
                .collect(),
        }
    }

    /// Adds a sample taken at `now`; returns the steps it finished, to
    /// write out.
    fn add(&mut self, now: u64, m: NodeMetrics) -> Vec<(u8, MetricPoint)> {
        if let Some((then, prev)) = &self.latest
            && now > *then
        {
            let dt = now - then;
            let rate = |a: u64, b: u64| a.checked_sub(b).map_or(0, |d| d / dt);
            self.rates = (rate(m.net_rx, prev.net_rx), rate(m.net_tx, prev.net_tx));
        }
        let gpu_percent = (!m.gpus.is_empty())
            .then(|| m.gpus.iter().map(|g| g.util_percent).sum::<f32>() / m.gpus.len() as f32);
        let instant = MetricPoint {
            unix: now,
            cpu_percent: m.cpu_percent,
            load1: m.load1,
            memory_used: m.memory_used,
            memory_total: m.memory_total,
            disk_used: m.disk_used,
            disk_total: m.disk_total,
            net_rx_bps: self.rates.0,
            net_tx_bps: self.rates.1,
            temp_c: m.hottest(),
            gpu_percent,
            gpu_mem_used_mib: m.gpus.iter().map(|g| g.mem_used_mib).sum(),
            running: m.running as f32,
        };
        let mut done = Vec::new();
        for (i, tier) in self.tiers.iter_mut().enumerate() {
            let start = now / tier.step * tier.step;
            if tier.acc.as_ref().is_some_and(|a| a.start != start) {
                let p = tier.acc.take().expect("checked").point();
                done.push((i as u8, p.clone()));
                // A step left open at a restart was stored part done.
                if tier.points.back().is_some_and(|b| b.unix == p.unix) {
                    tier.points.pop_back();
                }
                tier.points.push_back(p);
                while tier.points.len() > tier.keep {
                    tier.points.pop_front();
                }
            }
            tier.acc
                .get_or_insert_with(|| Acc {
                    start,
                    ..Default::default()
                })
                .add(&instant);
        }
        self.latest = Some((now, m));
        done
    }

    /// Points of the tier that fits `since_secs`, from `now - since_secs`,
    /// the unfinished step last.
    fn series(&self, now: u64, since_secs: u64) -> (u32, Vec<MetricPoint>) {
        let i = match since_secs {
            s if s <= TIERS[0].1 => 0,
            s if s <= TIERS[1].1 => 1,
            _ => 2,
        };
        let tier = &self.tiers[i];
        let from = now.saturating_sub(since_secs);
        let open = tier.acc.as_ref().map(Acc::point);
        let mut out: Vec<MetricPoint> = tier
            .points
            .iter()
            .filter(|p| p.unix >= from && open.as_ref().is_none_or(|o| o.unix != p.unix))
            .cloned()
            .collect();
        out.extend(open);
        (tier.step as u32, out)
    }

    fn fresh(&self, now: u64) -> Option<&NodeMetrics> {
        self.latest
            .as_ref()
            .filter(|(t, _)| now.saturating_sub(*t) <= FRESH_SECS)
            .map(|(_, m)| m)
    }
}

/// A running task's output so far.
pub(crate) struct LiveLog {
    attempt: u32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// A webhook and how its deliveries went.
pub(crate) struct WebhookState {
    spec: WebhookSpec,
    sent: u64,
    failed: u64,
    last_unix: Option<u64>,
    last_error: Option<String>,
}

/// An alert going to the webhooks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Notice {
    pub cluster: String,
    /// `firing` or `resolved`.
    pub status: String,
    pub rule: String,
    pub subject: String,
    pub level: String,
    pub message: String,
    pub unix: u64,
    pub since_unix: u64,
    /// A firing alert's notice sent again (1.6: the repeat setting).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub repeat: bool,
}

impl Notice {
    /// The text chat webhooks show.
    pub fn text(&self) -> String {
        if self.repeat {
            format!(
                "🔁 [{}] still firing after {}: {} ({})",
                self.cluster,
                human_duration(self.unix.saturating_sub(self.since_unix)),
                self.message,
                self.rule
            )
        } else if self.status == "resolved" {
            format!(
                "✅ [{}] resolved: {} ({})",
                self.cluster, self.message, self.rule
            )
        } else {
            let icon = if self.level == "critical" {
                "🚨"
            } else {
                "⚠️"
            };
            format!("{icon} [{}] {} ({})", self.cluster, self.message, self.rule)
        }
    }

    /// The request body in a webhook's format.
    pub fn body(&self, format: WebhookFormat) -> String {
        match format {
            WebhookFormat::Slack => serde_json::json!({ "text": self.text() }).to_string(),
            WebhookFormat::Discord => serde_json::json!({ "content": self.text() }).to_string(),
            WebhookFormat::Json => serde_json::to_string(self).expect("notice encodes"),
        }
    }
}

/// One POST for the webhook worker.
pub struct Delivery {
    pub webhook: String,
    pub url: String,
    pub body: String,
    /// `pumasi webhook test` waits for the answer.
    pub reply: Option<oneshot::Sender<Result<(), String>>>,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub runs_succeeded: u64,
    pub runs_failed: u64,
    pub preemptions: u64,
    pub check_mismatches: u64,
    pub jobs_finished: BTreeMap<&'static str, u64>,
    pub events: BTreeMap<EventLevel, u64>,
}

/// Monitoring state.
pub(crate) struct Monitor {
    pub retention_secs: u64,
    pub started_unix: u64,
    pub series: HashMap<String, NodeSeries>,
    pending_points: Vec<(String, u8, MetricPoint)>,
    pub live: HashMap<(JobId, u32), LiveLog>,
    events: VecDeque<EventView>,
    pending_events: Vec<EventView>,
    next_event_seq: u64,
    /// Jobs not finished yet, watched to report when they finish.
    pub active_jobs: HashSet<JobId>,
    /// Finished task runs in the failure-rate window: (unix, succeeded).
    runs: VecDeque<(u64, bool)>,
    pub counters: Counters,
    /// Queued runs held back by quotas in the last scheduling pass.
    pub quota_blocked: u32,
    pub policy: AlertPolicy,
    /// Repeat and resolved notices, per-node thresholds (1.6).
    pub tuning: AlertTuning,
    /// Silences, ended ones removed every second.
    silences: Vec<Silence>,
    /// Stateful alerts firing now, by (rule, subject).
    firing: BTreeMap<(String, String), AlertView>,
    /// When a firing alert's last notice went out; missing while a
    /// silence has kept it quiet from the start.
    notified: HashMap<(String, String), u64>,
    webhooks: BTreeMap<String, WebhookState>,
    deliver: Option<mpsc::UnboundedSender<Delivery>>,
    recent: VecDeque<Notice>,
    /// The control plane's own data disk: (used, total).
    pub cp_disk: Option<(u64, u64)>,
    last_flush: Instant,
    last_prune: Instant,
    pub last_jobs_check: Instant,
}

impl Default for Monitor {
    fn default() -> Self {
        Self {
            retention_secs: 7 * 86_400,
            started_unix: now_unix(),
            series: HashMap::new(),
            pending_points: Vec::new(),
            live: HashMap::new(),
            events: VecDeque::new(),
            pending_events: Vec::new(),
            next_event_seq: 1,
            active_jobs: HashSet::new(),
            runs: VecDeque::new(),
            counters: Counters::default(),
            quota_blocked: 0,
            policy: AlertPolicy::default(),
            tuning: AlertTuning::default(),
            silences: Vec::new(),
            firing: BTreeMap::new(),
            notified: HashMap::new(),
            webhooks: BTreeMap::new(),
            deliver: None,
            recent: VecDeque::new(),
            cp_disk: None,
            last_flush: Instant::now(),
            last_prune: Instant::now(),
            last_jobs_check: Instant::now(),
        }
    }
}

fn tier_since(now: u64, retention: u64) -> Vec<u64> {
    TIERS
        .iter()
        .map(|&(_, window)| now.saturating_sub(window.min(retention)))
        .collect()
}

impl State {
    /// Loads stored events, metrics, the alert policy and webhooks, and
    /// starts watching unfinished jobs.
    pub(crate) fn load_monitor(&mut self, retention: Duration) -> Result<()> {
        let m = &mut self.monitor;
        m.retention_secs = retention.as_secs().max(3600);
        m.started_unix = now_unix();
        if let Some(p) = self.store.alert_policy()? {
            m.policy = p;
        }
        if let Some(t) = self.store.meta_record(ALERT_TUNING_META)? {
            m.tuning = t;
        }
        if let Some(s) = self.store.meta_record::<Vec<Silence>>(SILENCES_META)? {
            m.silences = s;
        }
        for w in self.store.webhooks()? {
            m.webhooks.insert(
                w.name.clone(),
                WebhookState {
                    spec: w,
                    sent: 0,
                    failed: 0,
                    last_unix: None,
                    last_error: None,
                },
            );
        }
        let events = self.store.load_events(EVENT_RING)?;
        m.next_event_seq = events.last().map_or(1, |e| e.seq + 1);
        m.events = events.into();
        let now = now_unix();
        let mut points = self.store.load_points(&tier_since(now, m.retention_secs))?;
        points.sort_by_key(|(_, _, p)| p.unix);
        for (node, tier, p) in points {
            let retention = m.retention_secs;
            let s = m
                .series
                .entry(node)
                .or_insert_with(|| NodeSeries::new(retention));
            if let Some(t) = s.tiers.get_mut(tier as usize) {
                t.points.push_back(p);
                while t.points.len() > t.keep {
                    t.points.pop_front();
                }
            }
        }
        let active: Vec<JobId> = self
            .jobs
            .values()
            .filter(|j| !Self::summary(j).state.is_terminal())
            .map(|j| j.meta.id)
            .collect();
        self.monitor.active_jobs.extend(active);
        Ok(())
    }

    /// How long metrics and events are kept (at least an hour).
    pub fn set_metrics_retention(&mut self, d: Duration) {
        let r = d.as_secs().max(3600);
        self.monitor.retention_secs = r;
        for s in self.monitor.series.values_mut() {
            for (t, &(step, window)) in s.tiers.iter_mut().zip(TIERS.iter()) {
                t.keep = (window.min(r) / step).max(1) as usize;
                while t.points.len() > t.keep {
                    t.points.pop_front();
                }
            }
        }
    }

    /// The control plane's data disk: (used, total) bytes.
    pub fn monitor_cp_disk(&mut self, disk: Option<(u64, u64)>) {
        self.monitor.cp_disk = disk;
    }

    /// Hands deliveries to the server's webhook worker.
    pub fn attach_webhooks(&mut self, tx: mpsc::UnboundedSender<Delivery>) {
        self.monitor.deliver = Some(tx);
    }

    // -- events -------------------------------------------------------------

    /// Records an event. `node` is a node id.
    pub(crate) fn event(
        &mut self,
        level: EventLevel,
        kind: &str,
        node: Option<&str>,
        job: Option<JobId>,
        message: impl Into<String>,
    ) {
        let node_name = node.map(|id| self.node_name(id));
        let account = job
            .and_then(|j| self.jobs.get(&j))
            .map(|j| j.meta.account.clone());
        let m = &mut self.monitor;
        let e = EventView {
            seq: m.next_event_seq,
            unix: now_unix(),
            level,
            kind: kind.to_string(),
            node: node_name,
            job,
            account,
            message: message.into(),
        };
        m.next_event_seq += 1;
        *m.counters.events.entry(level).or_default() += 1;
        m.events.push_back(e.clone());
        while m.events.len() > EVENT_RING {
            m.events.pop_front();
        }
        m.pending_events.push(e);
    }

    /// A node's name, or the start of its id.
    pub(crate) fn node_name(&self, id: &str) -> String {
        self.members
            .get(id)
            .map(|m| m.info.name.clone())
            .or_else(|| self.nodes.get(id).map(|n| n.info.name.clone()))
            .unwrap_or_else(|| id.chars().take(10).collect())
    }

    /// Events after `after` and not before `since_unix`, oldest first; with
    /// `after` 0, the newest `limit` of them.
    pub fn events(&self, after: u64, since_unix: u64, limit: u32) -> Vec<EventView> {
        let limit = limit.clamp(1, EVENT_RING as u32) as usize;
        let it = self
            .monitor
            .events
            .iter()
            .filter(|e| e.seq > after && e.unix >= since_unix);
        if after == 0 {
            let all: Vec<&EventView> = it.collect();
            all[all.len().saturating_sub(limit)..]
                .iter()
                .map(|e| (*e).clone())
                .collect()
        } else {
            it.take(limit).cloned().collect()
        }
    }

    // -- metrics ------------------------------------------------------------

    pub fn on_metrics(&mut self, id: &str, generation: u64, m: NodeMetrics) {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation)
        {
            return;
        }
        let retention = self.monitor.retention_secs;
        let done = self
            .monitor
            .series
            .entry(id.to_string())
            .or_insert_with(|| NodeSeries::new(retention))
            .add(now_unix(), m);
        self.monitor
            .pending_points
            .extend(done.into_iter().map(|(t, p)| (id.to_string(), t, p)));
    }

    fn cluster_metrics(&self) -> ClusterMetrics {
        let mut queued: BTreeMap<i32, u32> = BTreeMap::new();
        for q in &self.queue {
            *queued.entry(q.priority).or_default() += 1;
        }
        let c = &self.monitor.counters;
        ClusterMetrics {
            queued: queued.into_iter().rev().collect(),
            running: self.nodes.values().map(|n| n.running.len() as u32).sum(),
            waiting_jobs: self.mgmt.waiting.len() as u32,
            quota_blocked: self.monitor.quota_blocked,
            runs_succeeded: c.runs_succeeded,
            runs_failed: c.runs_failed,
            preemptions: c.preemptions,
            check_mismatches: c.check_mismatches,
            alerts_firing: self.monitor.firing.len() as u32,
        }
    }

    /// Latest metrics of every node (or one), with their time series over
    /// `since_secs` when it is above 0.
    pub fn metrics_view(&self, node: Option<&str>, since_secs: u64) -> Result<MetricsView> {
        let only = node.map(|n| self.resolve_node(n)).transpose()?;
        let now = now_unix();
        let since = since_secs.min(self.monitor.retention_secs);
        let mut nodes: Vec<NodeMetricsView> = self
            .members
            .values()
            .filter(|m| only.as_ref().is_none_or(|o| *o == m.id))
            .map(|m| {
                let rt = self.nodes.get(&m.id);
                let s = self.monitor.series.get(&m.id);
                let (step_secs, series) = match s {
                    Some(s) if since > 0 => s.series(now, since),
                    _ => (0, Vec::new()),
                };
                NodeMetricsView {
                    id: m.id.clone(),
                    name: m.info.name.clone(),
                    online: rt.is_some_and(|n| n.online),
                    slots: rt.map_or(m.info.slots, |n| n.info.slots),
                    running: rt.map_or(0, |n| n.running.len() as u32),
                    latest: s.and_then(|s| s.latest.as_ref().map(|(_, m)| m.clone())),
                    latest_unix: s
                        .and_then(|s| s.latest.as_ref().map(|(t, _)| *t))
                        .unwrap_or(0),
                    net_rx_bps: s.map_or(0, |s| s.rates.0),
                    net_tx_bps: s.map_or(0, |s| s.rates.1),
                    step_secs,
                    series,
                }
            })
            .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        Ok(MetricsView {
            nodes,
            cluster: self.cluster_metrics(),
        })
    }

    // -- live output ----------------------------------------------------------

    /// Output a running task printed (protocol 10).
    pub fn on_logs(
        &mut self,
        id: &str,
        generation: u64,
        task: TaskRef,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    ) {
        if task.check
            || !self
                .nodes
                .get(id)
                .is_some_and(|n| n.generation == generation && n.running.contains_key(&task))
        {
            return;
        }
        let live = self
            .monitor
            .live
            .entry((task.job, task.index))
            .or_insert_with(|| LiveLog {
                attempt: task.attempt,
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        if live.attempt != task.attempt {
            *live = LiveLog {
                attempt: task.attempt,
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
        for (buf, new) in [(&mut live.stdout, stdout), (&mut live.stderr, stderr)] {
            let room = MAX_OUTPUT_BYTES.saturating_sub(buf.len());
            buf.extend_from_slice(&new[..new.len().min(room)]);
        }
    }

    /// A task's output from the given offsets of run `attempt`.
    pub fn logs(
        &self,
        job: JobId,
        index: u32,
        attempt: u32,
        stdout_from: u64,
        stderr_from: u64,
    ) -> Result<LogChunk> {
        let j = self.jobs.get(&job).context("no such job")?;
        let t = j.tasks.get(index as usize).with_context(|| {
            format!(
                "job {job} has {} tasks (0 to {})",
                j.tasks.len(),
                j.tasks.len().saturating_sub(1)
            )
        })?;
        let empty: &[u8] = &[];
        let (done, out, err): (bool, &[u8], &[u8]) = match &t.state {
            TaskState::Running { .. } => match self.monitor.live.get(&(job, index)) {
                Some(l) if l.attempt == t.attempts => (false, &l.stdout, &l.stderr),
                _ => (false, empty, empty),
            },
            TaskState::Pending => (false, empty, empty),
            _ => (true, &t.stdout, &t.stderr),
        };
        let restart = attempt != t.attempts;
        let slice = |all: &[u8], from: u64| -> (u64, Vec<u8>) {
            let from = if restart {
                0
            } else {
                (from as usize).min(all.len())
            };
            let end = all.len().min(from + LOG_CHUNK_BYTES);
            (from as u64, all[from..end].to_vec())
        };
        let (stdout_from, stdout) = slice(out, stdout_from);
        let (stderr_from, stderr) = slice(err, stderr_from);
        // More may follow even after the end when a chunk was cut short.
        let complete = stdout_from as usize + stdout.len() >= out.len()
            && stderr_from as usize + stderr.len() >= err.len();
        Ok(LogChunk {
            attempt: t.attempts,
            stdout_from,
            stdout,
            stderr_from,
            stderr,
            state: t.state.clone(),
            done: done && complete,
            // A microVM streams its output from 1.6 agents on.
            live: match &t.state {
                _ if j.meta.executor != Executor::Microvm && t.ran != Some(Executor::Microvm) => {
                    true
                }
                TaskState::Running { node } => self.node_minor(node) >= MONITOR_MINOR,
                _ => false,
            },
        })
    }

    // -- hooks from the rest of the state machine ---------------------------

    /// A task's own run finished on a node.
    pub(crate) fn note_run(
        &mut self,
        node: &str,
        task: TaskRef,
        outcome: &pumasi_proto::msg::Outcome,
    ) {
        self.monitor.live.remove(&(task.job, task.index));
        let ok = outcome.is_success();
        let m = &mut self.monitor;
        if ok {
            m.counters.runs_succeeded += 1;
        } else {
            m.counters.runs_failed += 1;
        }
        m.runs.push_back((now_unix(), ok));
        if m.runs.len() > 100_000 {
            m.runs.pop_front();
        }
        if !ok && !matches!(outcome, pumasi_proto::msg::Outcome::Cancelled) {
            self.event(
                EventLevel::Warning,
                "task.failed",
                Some(node),
                Some(task.job),
                format!("task {}/{} failed: {outcome}", task.job, task.index),
            );
        }
    }

    /// A check run disagreed with an accepted result (or no majority
    /// settled it).
    pub(crate) fn note_mismatch(&mut self, task: TaskRef, bad: &[String], inconclusive: bool) {
        self.monitor.counters.check_mismatches += 1;
        let names: Vec<String> = bad.iter().map(|id| self.node_name(id)).collect();
        let msg = if inconclusive {
            format!(
                "check of task {}/{} inconclusive: runs on different nodes disagreed",
                task.job, task.index
            )
        } else {
            format!(
                "check of task {}/{} rejected the result of {}",
                task.job,
                task.index,
                names.join(", ")
            )
        };
        let subject = names.first().cloned().unwrap_or_else(|| "cluster".into());
        self.event(
            EventLevel::Warning,
            "check.mismatch",
            None,
            Some(task.job),
            msg.clone(),
        );
        if self.monitor.policy.check_mismatch {
            self.alert_once("check_mismatch", &subject, EventLevel::Warning, &msg);
        }
    }

    /// A node sent a new benchmark: alert when it is much slower than the
    /// last one.
    pub(crate) fn note_benchmark(&mut self, node: &str, old: Option<(u32, u32)>, new: (u32, u32)) {
        let Some((old_cpu, old_mem)) = old else {
            return;
        };
        let Some(limit) = self.monitor.policy.perf_drop_percent else {
            return;
        };
        let drop = |a: u32, b: u32| -> u32 {
            if a == 0 || b >= a {
                0
            } else {
                ((a - b) as u64 * 100 / a as u64) as u32
            }
        };
        let (cpu, mem) = (drop(old_cpu, new.0), drop(old_mem, new.1));
        let mut parts = Vec::new();
        if cpu >= limit as u32 {
            parts.push(format!("CPU {old_cpu} -> {} (-{cpu}%)", new.0));
        }
        if mem >= limit as u32 {
            parts.push(format!("memory {old_mem} -> {} MB/s (-{mem}%)", new.1));
        }
        if parts.is_empty() {
            return;
        }
        let name = self.node_name(node);
        let msg = format!("benchmark of {name} dropped: {}", parts.join(", "));
        self.event(
            EventLevel::Warning,
            "perf.drop",
            Some(node),
            None,
            msg.clone(),
        );
        self.alert_once("perf_drop", &name, EventLevel::Warning, &msg);
    }

    // -- alerts -------------------------------------------------------------

    pub fn alerts_view(&self) -> AlertsView {
        AlertsView {
            policy: self.monitor.policy.clone(),
            firing: self.monitor.firing.values().cloned().collect(),
            webhooks: self
                .monitor
                .webhooks
                .values()
                .map(|w| WebhookView {
                    name: w.spec.name.clone(),
                    target: redact_url(&w.spec.url),
                    format: w.spec.format,
                    sent: w.sent,
                    failed: w.failed,
                    last_unix: w.last_unix,
                    last_error: w.last_error.clone(),
                })
                .collect(),
        }
    }

    pub fn set_alert_policy(&mut self, p: AlertPolicy) -> Result<()> {
        p.validate()?;
        self.store.put_alert_policy(&p)?;
        info!(?p, "alert policy set");
        self.monitor.policy = p;
        self.evaluate_alerts(now_unix());
        Ok(())
    }

    pub fn set_webhook(&mut self, w: WebhookSpec) -> Result<()> {
        w.validate()?;
        self.store.put_webhook(&w.name, Some(&w))?;
        info!(name = %w.name, target = %redact_url(&w.url), format = %w.format, "webhook set");
        let prev = self.monitor.webhooks.remove(&w.name);
        self.monitor.webhooks.insert(
            w.name.clone(),
            WebhookState {
                spec: w,
                sent: prev.as_ref().map_or(0, |p| p.sent),
                failed: prev.as_ref().map_or(0, |p| p.failed),
                last_unix: prev.as_ref().and_then(|p| p.last_unix),
                last_error: prev.and_then(|p| p.last_error),
            },
        );
        Ok(())
    }

    pub fn remove_webhook(&mut self, name: &str) -> Result<()> {
        ensure!(
            self.monitor.webhooks.contains_key(name),
            "no webhook named {name:?}"
        );
        self.store.put_webhook(name, None)?;
        self.monitor.webhooks.remove(name);
        Ok(())
    }

    pub(crate) fn webhook_specs(&self) -> Vec<WebhookSpec> {
        self.monitor
            .webhooks
            .values()
            .map(|w| w.spec.clone())
            .collect()
    }

    /// A test notice for one webhook, to send now.
    pub fn test_delivery(&self, name: &str) -> Result<(String, String)> {
        let w = self
            .monitor
            .webhooks
            .get(name)
            .with_context(|| format!("no webhook named {name:?}"))?;
        let now = now_unix();
        let n = Notice {
            cluster: self.cluster_name.clone(),
            status: "firing".into(),
            rule: "test".into(),
            subject: "cluster".into(),
            level: EventLevel::Info.to_string(),
            message: format!("test alert from pumasi cluster {}", self.cluster_name),
            unix: now,
            since_unix: now,
            repeat: false,
        };
        Ok((w.spec.url.clone(), n.body(w.spec.format)))
    }

    /// How a delivery went (from the webhook worker).
    pub fn on_delivery(&mut self, name: &str, result: Result<(), String>) {
        let Some(w) = self.monitor.webhooks.get_mut(name) else {
            return;
        };
        w.last_unix = Some(now_unix());
        match result {
            Ok(()) => {
                w.sent += 1;
                w.last_error = None;
            }
            Err(e) => {
                warn!(webhook = %name, "webhook delivery failed: {e}");
                w.failed += 1;
                w.last_error = Some(e);
            }
        }
    }

    /// Notices sent to webhooks lately, oldest first.
    pub fn recent_notices(&self) -> Vec<Notice> {
        self.monitor.recent.iter().cloned().collect()
    }

    fn notify(&mut self, n: Notice) {
        for w in self.monitor.webhooks.values() {
            if let Some(tx) = &self.monitor.deliver {
                let _ = tx.send(Delivery {
                    webhook: w.spec.name.clone(),
                    url: w.spec.url.clone(),
                    body: n.body(w.spec.format),
                    reply: None,
                });
            }
        }
        self.monitor.recent.push_back(n);
        while self.monitor.recent.len() > RECENT_NOTICES {
            self.monitor.recent.pop_front();
        }
    }

    fn notice(&self, status: &str, a: &AlertView) -> Notice {
        Notice {
            cluster: self.cluster_name.clone(),
            status: status.into(),
            rule: a.rule.clone(),
            subject: a.subject.clone(),
            level: a.level.to_string(),
            message: a.message.clone(),
            unix: now_unix(),
            since_unix: a.since_unix,
            repeat: false,
        }
    }

    /// An alert about something that happened once (no "resolved").
    fn alert_once(&mut self, rule: &str, subject: &str, level: EventLevel, message: &str) {
        if self.silenced(rule, subject, now_unix()) {
            return;
        }
        let a = AlertView {
            rule: rule.into(),
            subject: subject.into(),
            level,
            message: message.into(),
            since_unix: now_unix(),
        };
        let n = self.notice("firing", &a);
        self.notify(n);
    }

    /// What should be firing now, by (rule, subject).
    pub(crate) fn wanted_alerts(
        &mut self,
        now: u64,
    ) -> BTreeMap<(String, String), (EventLevel, String)> {
        let p = self.monitor.policy.clone();
        let mut want = BTreeMap::new();
        let mut put = |rule: &str, subject: &str, level, msg: String| {
            want.insert((rule.to_string(), subject.to_string()), (level, msg));
        };
        for m in self.members.values() {
            let name = m.info.name.clone();
            let rt = self.nodes.get(&m.id).filter(|n| n.online);
            // Thresholds for this node (1.6), else the cluster's.
            let o = (!self.monitor.tuning.overrides.is_empty())
                .then(|| {
                    self.monitor
                        .tuning
                        .override_for(&self.node_labels(&m.id))
                        .cloned()
                })
                .flatten();
            let node_offline_secs = o
                .as_ref()
                .and_then(|o| o.node_offline_secs)
                .unwrap_or(p.node_offline_secs);
            let disk_percent = o
                .as_ref()
                .and_then(|o| o.disk_percent)
                .unwrap_or(p.disk_percent);
            let temp_c = o.as_ref().and_then(|o| o.temp_c).unwrap_or(p.temp_c);
            if let Some(after) = node_offline_secs
                && rt.is_none()
                && !self.asleep(&m.id)
                && !self.node_config(&m.id).is_some_and(|c| c.cordoned)
            {
                let seen = self
                    .nodes
                    .get(&m.id)
                    .map_or(0, |n| n.last_seen_unix)
                    .max(self.monitor.started_unix);
                let gone = now.saturating_sub(seen);
                if gone >= after {
                    put(
                        "node_offline",
                        &name,
                        EventLevel::Critical,
                        format!(
                            "node {name} offline for {}",
                            human_duration(gone.max(after))
                        ),
                    );
                }
            }
            let latest = rt.and_then(|_| self.monitor.series.get(&m.id)?.fresh(now));
            if let (Some(limit), Some(l)) = (disk_percent, latest)
                && let Some(pct) = l.disk_percent()
                && pct >= limit as f32
            {
                put(
                    "disk",
                    &name,
                    EventLevel::Warning,
                    format!(
                        "disk of {name} {pct:.0}% full ({} of {})",
                        format_size(l.disk_used),
                        format_size(l.disk_total)
                    ),
                );
            }
            if let (Some(limit), Some(l)) = (temp_c, latest)
                && let Some(t) = l.hottest()
                && t >= limit as f32
            {
                put(
                    "temperature",
                    &name,
                    EventLevel::Warning,
                    format!("{name} is at {t:.0} °C"),
                );
            }
            if p.smart
                && let Some(s) = m
                    .perf
                    .as_ref()
                    .and_then(|p| p.profile.disk.as_ref())
                    .and_then(|d| d.smart.as_ref())
            {
                if s.passed == Some(false) {
                    put(
                        "smart",
                        &name,
                        EventLevel::Critical,
                        format!("drive {} of {name} fails its SMART health check", s.model),
                    );
                } else if s.percent_used.is_some_and(|u| u >= 90) {
                    put(
                        "smart",
                        &name,
                        EventLevel::Warning,
                        format!(
                            "drive {} of {name} has used {}% of its endurance",
                            s.model,
                            s.percent_used.unwrap_or(0)
                        ),
                    );
                } else if s.media_errors.is_some_and(|e| e > 0) {
                    put(
                        "smart",
                        &name,
                        EventLevel::Warning,
                        format!(
                            "drive {} of {name} reports {} media errors",
                            s.model,
                            s.media_errors.unwrap_or(0)
                        ),
                    );
                }
            }
        }
        if let (Some(limit), Some((used, total))) = (p.disk_percent, self.monitor.cp_disk)
            && total > 0
        {
            let pct = used as f32 * 100.0 / total as f32;
            if pct >= limit as f32 {
                put(
                    "disk",
                    "control-plane",
                    EventLevel::Warning,
                    format!(
                        "control plane disk (file store) {pct:.0}% full ({} of {})",
                        format_size(used),
                        format_size(total)
                    ),
                );
            }
        }
        if let Some(f) = p.failure_rate {
            let from = now.saturating_sub(f.window_secs);
            while self.monitor.runs.front().is_some_and(|(t, _)| *t < from) {
                self.monitor.runs.pop_front();
            }
            let n = self.monitor.runs.len() as u64;
            let failed = self.monitor.runs.iter().filter(|(_, ok)| !ok).count() as u64;
            if n >= f.min_runs.max(1) as u64 && failed * 100 >= f.percent as u64 * n {
                put(
                    "failure_rate",
                    "cluster",
                    EventLevel::Warning,
                    format!(
                        "{failed} of {n} runs failed in the last {} ({}%)",
                        human_duration(f.window_secs),
                        failed * 100 / n
                    ),
                );
            }
        }
        want
    }

    /// Fires alerts that should fire and resolves those that should not;
    /// sends notices unless a silence covers them, again after the repeat
    /// time, and when a silence ends on an alert still firing.
    pub(crate) fn evaluate_alerts(&mut self, now: u64) {
        let want = self.wanted_alerts(now);
        let gone: Vec<(String, String)> = self
            .monitor
            .firing
            .keys()
            .filter(|k| !want.contains_key(*k))
            .cloned()
            .collect();
        for k in gone {
            let a = self.monitor.firing.remove(&k).expect("listed");
            let told = self.monitor.notified.remove(&k).is_some();
            info!(rule = %a.rule, subject = %a.subject, "alert resolved");
            self.event(
                EventLevel::Info,
                "alert.resolved",
                None,
                None,
                format!("resolved: {}", a.message),
            );
            if told && self.monitor.tuning.resolved {
                let n = self.notice("resolved", &a);
                self.notify(n);
            }
        }
        let repeat = self.monitor.tuning.repeat_secs;
        for ((rule, subject), (level, message)) in want {
            let key = (rule.clone(), subject.clone());
            let quiet = self.silenced(&rule, &subject, now);
            if let Some(a) = self.monitor.firing.get_mut(&key) {
                // The message follows the measurement (`offline for 5m`).
                a.message = message;
                if quiet {
                    continue;
                }
                let again = match self.monitor.notified.get(&key) {
                    // A silence kept it quiet until now.
                    None => Some(false),
                    Some(last) => repeat
                        .filter(|r| now.saturating_sub(*last) >= *r)
                        .map(|_| true),
                };
                if let Some(is_repeat) = again {
                    let a = a.clone();
                    let mut n = self.notice("firing", &a);
                    n.repeat = is_repeat;
                    self.notify(n);
                    self.monitor.notified.insert(key, now);
                }
                continue;
            }
            let a = AlertView {
                rule,
                subject,
                level,
                message,
                since_unix: now,
            };
            warn!(rule = %a.rule, subject = %a.subject, silenced = quiet, "alert firing: {}", a.message);
            let text = if quiet {
                format!("{} (silenced)", a.message)
            } else {
                a.message.clone()
            };
            self.event(level, "alert.firing", None, None, text);
            if !quiet {
                let n = self.notice("firing", &a);
                self.notify(n);
                self.monitor.notified.insert(key.clone(), now);
            }
            self.monitor.firing.insert(key, a);
        }
    }

    /// Whether a silence keeps this alert quiet now.
    fn silenced(&self, rule: &str, subject: &str, now: u64) -> bool {
        let mut labels = None;
        self.monitor.silences.iter().any(|s| {
            s.until_unix > now
                && s.rule.as_deref().is_none_or(|r| r == rule)
                && match &s.nodes {
                    None => true,
                    Some(sel) => {
                        let l = labels.get_or_insert_with(|| {
                            self.members
                                .values()
                                .find(|m| m.info.name == subject)
                                .map(|m| self.node_labels(&m.id))
                        });
                        l.as_ref().is_some_and(|l| sel.matches(l))
                    }
                }
        })
    }

    /// Repeat and resolved notices, per-node thresholds and silences.
    pub fn alert_tuning_view(&self) -> AlertTuningView {
        let now = now_unix();
        AlertTuningView {
            tuning: self.monitor.tuning.clone(),
            silences: self
                .monitor
                .silences
                .iter()
                .filter(|s| s.until_unix > now)
                .cloned()
                .collect(),
            silenced: self
                .monitor
                .firing
                .keys()
                .filter(|(r, s)| self.silenced(r, s, now))
                .cloned()
                .collect(),
        }
    }

    pub fn set_alert_tuning(&mut self, t: AlertTuning) -> Result<()> {
        t.validate()?;
        self.store.put_meta_record(ALERT_TUNING_META, &t)?;
        info!(?t, "alert tuning set");
        self.monitor.tuning = t;
        self.evaluate_alerts(now_unix());
        Ok(())
    }

    /// Adds a silence; answers with its id.
    pub fn add_silence(&mut self, spec: SilenceSpec, by: &str) -> Result<String> {
        spec.validate()?;
        let now = now_unix();
        self.monitor.silences.retain(|s| s.until_unix > now);
        ensure!(
            self.monitor.silences.len() < MAX_SILENCES,
            "too many silences ({MAX_SILENCES}); end some with `pumasi alert unsilence`"
        );
        let id = loop {
            let id = format!("{:08x}", rand::random::<u32>());
            if !self.monitor.silences.iter().any(|s| s.id == id) {
                break id;
            }
        };
        let s = Silence {
            id: id.clone(),
            rule: spec.rule,
            nodes: spec.nodes,
            until_unix: now + spec.secs,
            comment: spec.comment,
            by: by.to_string(),
            created_unix: now,
        };
        let mut all = self.monitor.silences.clone();
        all.push(s.clone());
        self.store.put_meta_record(SILENCES_META, &all)?;
        info!(id = %s.id, what = %s.describe(), until = s.until_unix, "alerts silenced");
        self.event(
            EventLevel::Info,
            "alert.silenced",
            None,
            None,
            format!(
                "{} silenced for {} by {by}{}",
                s.describe(),
                human_duration(spec.secs),
                if s.comment.is_empty() {
                    String::new()
                } else {
                    format!(": {}", s.comment)
                }
            ),
        );
        self.monitor.silences = all;
        Ok(id)
    }

    /// Ends a silence (id or unique prefix).
    pub fn remove_silence(&mut self, id: &str) -> Result<()> {
        let id = id.trim();
        ensure!(!id.is_empty(), "which silence?");
        let found: Vec<usize> = self
            .monitor
            .silences
            .iter()
            .enumerate()
            .filter(|(_, s)| s.id.starts_with(id))
            .map(|(i, _)| i)
            .collect();
        let i = match found.as_slice() {
            [i] => *i,
            [] => anyhow::bail!("no silence {id:?}"),
            _ => anyhow::bail!("{id:?} names more than one silence"),
        };
        let mut all = self.monitor.silences.clone();
        let s = all.remove(i);
        self.store.put_meta_record(SILENCES_META, &all)?;
        self.monitor.silences = all;
        self.event(
            EventLevel::Info,
            "alert.unsilenced",
            None,
            None,
            format!("silence of {} ended", s.describe()),
        );
        self.evaluate_alerts(now_unix());
        Ok(())
    }

    /// Drops silences that ended.
    fn prune_silences(&mut self, now: u64) {
        if self.monitor.silences.iter().all(|s| s.until_unix > now) {
            return;
        }
        let mut all = self.monitor.silences.clone();
        all.retain(|s| s.until_unix > now);
        if let Err(e) = self.store.put_meta_record(SILENCES_META, &all) {
            warn!("persist silences: {e:#}");
            return;
        }
        self.monitor.silences = all;
    }

    // -- housekeeping ---------------------------------------------------------

    /// Called from [`State::tick`]: alerts, finished jobs, writing out.
    pub(crate) fn tick_monitor(&mut self) {
        let now = now_unix();
        self.evaluate_alerts(now);
        if self.monitor.last_jobs_check.elapsed() >= Duration::from_secs(1) {
            self.monitor.last_jobs_check = Instant::now();
            self.prune_silences(now);
            self.check_jobs();
            // Output of runs that ended without a report (node lost,
            // preempted) is no longer live.
            let live: Vec<(JobId, u32)> = self.monitor.live.keys().copied().collect();
            for k in live {
                let running = self
                    .jobs
                    .get(&k.0)
                    .and_then(|j| j.tasks.get(k.1 as usize))
                    .is_some_and(|t| {
                        matches!(t.state, TaskState::Running { .. })
                            && self
                                .monitor
                                .live
                                .get(&k)
                                .is_some_and(|l| l.attempt == t.attempts)
                    });
                if !running {
                    self.monitor.live.remove(&k);
                }
            }
        }
        if self.monitor.last_flush.elapsed() >= FLUSH_EVERY
            || self.monitor.pending_events.len() >= 500
        {
            self.flush_monitor();
        }
        if self.monitor.last_prune.elapsed() >= PRUNE_EVERY {
            self.monitor.last_prune = Instant::now();
            let retention = self.monitor.retention_secs;
            let nodes: HashSet<String> = self.members.keys().cloned().collect();
            self.monitor.series.retain(|id, _| nodes.contains(id));
            match self.store.prune_monitor(
                now.saturating_sub(retention),
                MAX_STORED_EVENTS,
                &tier_since(now, retention),
                &nodes,
            ) {
                Ok((e, p)) if e + p > 0 => {
                    info!(events = e, points = p, "pruned old monitoring data")
                }
                Ok(_) => {}
                Err(e) => warn!("prune monitoring data: {e:#}"),
            }
        }
    }

    /// Reports jobs that finished since the last look.
    fn check_jobs(&mut self) {
        let mut finished = Vec::new();
        for id in &self.monitor.active_jobs {
            match self.jobs.get(id) {
                Some(j) => {
                    let s = Self::summary(j);
                    if s.state.is_terminal() {
                        finished.push((*id, Some(s)));
                    }
                }
                None => finished.push((*id, None)),
            }
        }
        for (id, s) in finished {
            self.monitor.active_jobs.remove(&id);
            let Some(s) = s else {
                continue;
            };
            let (level, kind, label) = match s.state {
                JobState::Succeeded => (EventLevel::Info, "job.succeeded", "succeeded"),
                JobState::Cancelled => (EventLevel::Info, "job.cancelled", "cancelled"),
                _ => (EventLevel::Warning, "job.failed", "failed"),
            };
            *self
                .monitor
                .counters
                .jobs_finished
                .entry(label)
                .or_default() += 1;
            self.event(
                level,
                kind,
                None,
                Some(id),
                format!(
                    "job {id} ({}) {label}: {} of {} tasks succeeded",
                    s.name, s.succeeded, s.tasks
                ),
            );
        }
    }

    /// Queues the steps not finished yet, to write at shutdown: a restart
    /// keeps the last minutes too. The finished step replaces it later.
    pub fn keep_open_steps(&mut self) {
        let m = &mut self.monitor;
        for (node, s) in &m.series {
            for (i, t) in s.tiers.iter().enumerate() {
                if let Some(a) = &t.acc {
                    m.pending_points.push((node.clone(), i as u8, a.point()));
                }
            }
        }
    }

    /// Writes pending events and finished metric steps to the database.
    pub fn flush_monitor(&mut self) {
        self.monitor.last_flush = Instant::now();
        let events = std::mem::take(&mut self.monitor.pending_events);
        if let Err(e) = self.store.put_events(&events) {
            warn!("persist events: {e:#}");
        }
        let points = std::mem::take(&mut self.monitor.pending_points);
        if let Err(e) = self.store.put_points(&points) {
            warn!("persist metrics: {e:#}");
        }
        self.save_links(false);
    }

    // -- Prometheus -----------------------------------------------------------

    /// Everything `/metrics` serves, in the Prometheus text format.
    pub fn prometheus(&self) -> String {
        let now = now_unix();
        let mut w = Prom::default();
        w.family("pumasi_build_info", "gauge", "The control plane's version.");
        w.sample(
            "pumasi_build_info",
            &[
                ("version", pumasi_proto::version()),
                ("cluster", &self.cluster_name),
            ],
            1.0,
        );

        let mut members: Vec<_> = self.members.values().collect();
        members.sort_by(|a, b| a.info.name.cmp(&b.info.name).then(a.id.cmp(&b.id)));
        let node_labels = |m: &crate::state::Member| -> Vec<(&'static str, String)> {
            vec![("node", m.info.name.clone()), ("node_id", m.id.clone())]
        };
        type Get<'a> = Box<dyn Fn(&crate::state::Member) -> Option<f64> + 'a>;
        let per_node: Vec<(&str, &str, &str, Get)> = vec![
            (
                "pumasi_node_up",
                "gauge",
                "1 when the node is connected.",
                Box::new(|m| Some(self.nodes.get(&m.id).is_some_and(|n| n.online) as u8 as f64)),
            ),
            (
                "pumasi_node_slots",
                "gauge",
                "Tasks the node runs at once.",
                Box::new(|m| {
                    Some(self.nodes.get(&m.id).map_or(m.info.slots, |n| n.info.slots) as f64)
                }),
            ),
            (
                "pumasi_node_running_tasks",
                "gauge",
                "Runs on the node now.",
                Box::new(|m| Some(self.nodes.get(&m.id).map_or(0, |n| n.running.len()) as f64)),
            ),
            (
                "pumasi_node_cpu_score",
                "gauge",
                "CPU score runs are billed at (1000 = the reference vCPU).",
                Box::new(|m| Some(crate::perf::cpu_score(m).0 as f64)),
            ),
            (
                "pumasi_node_last_seen_timestamp_seconds",
                "gauge",
                "When the node was last heard from.",
                Box::new(|m| {
                    self.nodes
                        .get(&m.id)
                        .map(|n| n.last_seen_unix as f64)
                        .filter(|t| *t > 0.0)
                }),
            ),
            (
                "pumasi_node_rtt_seconds",
                "gauge",
                "Round trip to the control plane.",
                Box::new(|m| {
                    self.nodes
                        .get(&m.id)
                        .filter(|n| n.online)
                        .and_then(|n| n.perf.rtt_us)
                        .map(|us| us as f64 / 1e6)
                }),
            ),
        ];
        for (name, kind, help, get) in &per_node {
            w.family(name, kind, help);
            for m in &members {
                if let Some(v) = get(m) {
                    w.sample_owned(name, &node_labels(m), v);
                }
            }
        }

        // Machine metrics, from each node's latest sample while it is fresh.
        type GetM = fn(&NodeMetrics, &NodeSeries) -> Option<f64>;
        let machine: [(&str, &str, &str, GetM); 10] = [
            (
                "pumasi_node_cpu_usage_ratio",
                "gauge",
                "Busy share of the node's CPUs (0 to 1).",
                |m, _| Some(m.cpu_percent as f64 / 100.0),
            ),
            (
                "pumasi_node_load1",
                "gauge",
                "One-minute load average.",
                |m, _| Some(m.load1 as f64),
            ),
            (
                "pumasi_node_memory_used_bytes",
                "gauge",
                "Memory in use.",
                |m, _| Some(m.memory_used as f64),
            ),
            (
                "pumasi_node_memory_total_bytes",
                "gauge",
                "Memory installed.",
                |m, _| Some(m.memory_total as f64),
            ),
            (
                "pumasi_node_disk_used_bytes",
                "gauge",
                "Used space on the agent's disk.",
                |m, _| (m.disk_total > 0).then_some(m.disk_used as f64),
            ),
            (
                "pumasi_node_disk_total_bytes",
                "gauge",
                "Size of the agent's disk.",
                |m, _| (m.disk_total > 0).then_some(m.disk_total as f64),
            ),
            (
                "pumasi_node_network_receive_bytes_total",
                "counter",
                "Bytes received on all interfaces but loopback.",
                |m, _| Some(m.net_rx as f64),
            ),
            (
                "pumasi_node_network_transmit_bytes_total",
                "counter",
                "Bytes sent on all interfaces but loopback.",
                |m, _| Some(m.net_tx as f64),
            ),
            (
                "pumasi_node_temperature_celsius",
                "gauge",
                "The hottest CPU or board sensor.",
                |m, _| m.temp_c.map(|t| t as f64),
            ),
            (
                "pumasi_node_metrics_age_seconds",
                "gauge",
                "Seconds since the node's last sample.",
                |_, _| None,
            ),
        ];
        for (name, kind, help, get) in machine {
            w.family(name, kind, help);
            for m in &members {
                let Some(s) = self.monitor.series.get(&m.id) else {
                    continue;
                };
                let online = self.nodes.get(&m.id).is_some_and(|n| n.online);
                if name == "pumasi_node_metrics_age_seconds" {
                    if let Some((t, _)) = &s.latest {
                        w.sample_owned(name, &node_labels(m), now.saturating_sub(*t) as f64);
                    }
                    continue;
                }
                if !online {
                    continue;
                }
                if let Some(v) = s.fresh(now).and_then(|l| get(l, s)) {
                    w.sample_owned(name, &node_labels(m), v);
                }
            }
        }
        type GetG = fn(&pumasi_proto::monitor::GpuMetrics) -> Option<f64>;
        let gpu: [(&str, &str, GetG); 5] = [
            (
                "pumasi_gpu_utilization_ratio",
                "Busy share of the GPU (0 to 1).",
                |g| Some(g.util_percent as f64 / 100.0),
            ),
            ("pumasi_gpu_memory_used_bytes", "GPU memory in use.", |g| {
                Some((g.mem_used_mib << 20) as f64)
            }),
            (
                "pumasi_gpu_memory_total_bytes",
                "GPU memory installed.",
                |g| (g.mem_total_mib > 0).then_some((g.mem_total_mib << 20) as f64),
            ),
            ("pumasi_gpu_temperature_celsius", "GPU temperature.", |g| {
                g.temp_c.map(|t| t as f64)
            }),
            ("pumasi_gpu_power_watts", "GPU power draw.", |g| {
                g.power_w.map(|p| p as f64)
            }),
        ];
        for (name, help, get) in gpu {
            w.family(name, "gauge", help);
            for m in &members {
                if !self.nodes.get(&m.id).is_some_and(|n| n.online) {
                    continue;
                }
                let Some(l) = self.monitor.series.get(&m.id).and_then(|s| s.fresh(now)) else {
                    continue;
                };
                for g in &l.gpus {
                    if let Some(v) = get(g) {
                        let mut labels = node_labels(m);
                        labels.push(("gpu", g.index.to_string()));
                        w.sample_owned(name, &labels, v);
                    }
                }
            }
        }

        // The queue and runs.
        let c = self.cluster_metrics();
        w.family(
            "pumasi_queued_runs",
            "gauge",
            "Runs waiting for a node, by job priority.",
        );
        for (p, n) in &c.queued {
            w.sample(
                "pumasi_queued_runs",
                &[("priority", &p.to_string())],
                *n as f64,
            );
        }
        w.family("pumasi_running_runs", "gauge", "Runs on nodes now.");
        w.sample("pumasi_running_runs", &[], c.running as f64);
        w.family(
            "pumasi_waiting_jobs",
            "gauge",
            "Jobs waiting for other jobs to finish.",
        );
        w.sample("pumasi_waiting_jobs", &[], c.waiting_jobs as f64);
        w.family(
            "pumasi_quota_blocked_runs",
            "gauge",
            "Queued runs held back by their account's quota.",
        );
        w.sample("pumasi_quota_blocked_runs", &[], c.quota_blocked as f64);
        let mut active: BTreeMap<&str, u64> = [("queued", 0), ("running", 0)].into();
        for id in &self.monitor.active_jobs {
            if let Some(j) = self.jobs.get(id) {
                match Self::summary(j).state {
                    JobState::Queued => *active.entry("queued").or_default() += 1,
                    JobState::Running => *active.entry("running").or_default() += 1,
                    _ => {}
                }
            }
        }
        w.family("pumasi_jobs", "gauge", "Unfinished jobs by state.");
        for (s, n) in &active {
            w.sample("pumasi_jobs", &[("state", s)], *n as f64);
        }
        w.family(
            "pumasi_jobs_finished_total",
            "counter",
            "Jobs that finished since the control plane started.",
        );
        for s in ["succeeded", "failed", "cancelled"] {
            let n = self
                .monitor
                .counters
                .jobs_finished
                .get(s)
                .copied()
                .unwrap_or(0);
            w.sample("pumasi_jobs_finished_total", &[("state", s)], n as f64);
        }
        w.family(
            "pumasi_runs_total",
            "counter",
            "Task runs that finished since the control plane started.",
        );
        w.sample(
            "pumasi_runs_total",
            &[("outcome", "succeeded")],
            c.runs_succeeded as f64,
        );
        w.sample(
            "pumasi_runs_total",
            &[("outcome", "failed")],
            c.runs_failed as f64,
        );
        w.family(
            "pumasi_preemptions_total",
            "counter",
            "Runs stopped to make room.",
        );
        w.sample("pumasi_preemptions_total", &[], c.preemptions as f64);
        w.family(
            "pumasi_check_mismatches_total",
            "counter",
            "Check runs that disagreed with a result.",
        );
        w.sample(
            "pumasi_check_mismatches_total",
            &[],
            c.check_mismatches as f64,
        );
        w.family("pumasi_events_total", "counter", "Cluster events by level.");
        for level in [EventLevel::Info, EventLevel::Warning, EventLevel::Critical] {
            let n = self
                .monitor
                .counters
                .events
                .get(&level)
                .copied()
                .unwrap_or(0);
            w.sample(
                "pumasi_events_total",
                &[("level", &level.to_string())],
                n as f64,
            );
        }
        w.family(
            "pumasi_alerts_firing",
            "gauge",
            "Alerts firing now, by rule.",
        );
        let mut by_rule: BTreeMap<&str, u64> = [
            "node_offline",
            "failure_rate",
            "disk",
            "temperature",
            "smart",
        ]
        .iter()
        .map(|r| (*r, 0))
        .collect();
        for (rule, _) in self.monitor.firing.keys() {
            *by_rule.entry(rule.as_str()).or_default() += 1;
        }
        for (rule, n) in &by_rule {
            w.sample("pumasi_alerts_firing", &[("rule", rule)], *n as f64);
        }

        // Accounts and the store.
        let mut accounts: Vec<_> = self.accounts.values().collect();
        accounts.sort_by(|a, b| a.name.cmp(&b.name));
        w.family(
            "pumasi_account_balance_cu",
            "gauge",
            "Spendable CU of each account.",
        );
        for a in &accounts {
            w.sample(
                "pumasi_account_balance_cu",
                &[("account", &a.name)],
                a.balance as f64 / 1000.0,
            );
        }
        w.family(
            "pumasi_account_held_cu",
            "gauge",
            "Earnings waiting out the hold.",
        );
        for a in &accounts {
            w.sample(
                "pumasi_account_held_cu",
                &[("account", &a.name)],
                a.held as f64 / 1000.0,
            );
        }
        w.family(
            "pumasi_store_bytes",
            "gauge",
            "File store bytes of each account.",
        );
        for u in self.store_usage() {
            w.sample(
                "pumasi_store_bytes",
                &[("account", &u.account)],
                u.bytes as f64,
            );
        }
        if let Some((used, total)) = self.monitor.cp_disk {
            w.family(
                "pumasi_control_plane_disk_used_bytes",
                "gauge",
                "Used space on the control plane's data disk.",
            );
            w.sample("pumasi_control_plane_disk_used_bytes", &[], used as f64);
            w.family(
                "pumasi_control_plane_disk_total_bytes",
                "gauge",
                "Size of the control plane's data disk.",
            );
            w.sample("pumasi_control_plane_disk_total_bytes", &[], total as f64);
        }
        w.out
    }
}

/// Writes the exposition format, leaving out families with no samples.
#[derive(Default)]
struct Prom {
    out: String,
    header: Option<String>,
}

impl Prom {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        self.header = Some(format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], v: f64) {
        if let Some(h) = self.header.take() {
            self.out.push_str(&h);
        }
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, val)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{k}=\"{}\"", escape_label(val));
            }
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {}", fmt_value(v));
    }

    fn sample_owned(&mut self, name: &str, labels: &[(&str, String)], v: f64) {
        let l: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        self.sample(name, &l, v);
    }
}

fn fmt_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        }
    } else if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::monitor::GpuMetrics;

    fn sample(cpu: f32, rx: u64) -> NodeMetrics {
        NodeMetrics {
            cpu_percent: cpu,
            memory_used: 1 << 30,
            memory_total: 4 << 30,
            disk_used: 10,
            disk_total: 100,
            net_rx: rx,
            gpus: vec![GpuMetrics {
                util_percent: 50.0,
                mem_used_mib: 100,
                temp_c: Some(70.0),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn series_downsample_and_rates() {
        let mut s = NodeSeries::new(7 * 86_400);
        let t0 = 1_000_000_020; // a multiple of 60
        let mut done = Vec::new();
        for i in 0..30u64 {
            done.extend(s.add(t0 + i * 5, sample(i as f32, i * 5000)));
        }
        // 30 samples 5 s apart: 150 s, two finished minutes.
        assert_eq!(s.rates.0, 1000, "5000 bytes in 5 s");
        let minutes: Vec<&MetricPoint> = done
            .iter()
            .filter(|(t, _)| *t == 1)
            .map(|(_, p)| p)
            .collect();
        assert_eq!(minutes.len(), 2);
        assert_eq!(minutes[0].unix % 60, 0);
        // The first minute averages samples 0..=11 (or fewer, by alignment).
        assert!(minutes[0].cpu_percent < minutes[1].cpu_percent);
        assert_eq!(minutes[0].gpu_percent, Some(50.0));
        assert_eq!(minutes[0].temp_c, Some(70.0));
        let (step, points) = s.series(t0 + 150, 3600);
        assert_eq!(step, 10);
        assert_eq!(
            points.len(),
            14 + 1,
            "14 finished steps and the current one"
        );
        let (step, _) = s.series(t0 + 150, 86_400);
        assert_eq!(step, 60);
        let (step, _) = s.series(t0 + 150, 7 * 86_400);
        assert_eq!(step, 600);
        // A counter that went backwards (a reboot) is not a huge rate.
        s.add(t0 + 155, sample(1.0, 10));
        assert_eq!(s.rates.0, 0);
    }

    #[test]
    fn tiers_keep_no_more_than_retention() {
        let s = NodeSeries::new(3600);
        assert_eq!(s.tiers[0].keep, 360);
        assert_eq!(s.tiers[1].keep, 60);
        assert_eq!(s.tiers[2].keep, 6);
    }

    #[test]
    fn notices_in_each_format() {
        let n = Notice {
            cluster: "homelab".into(),
            status: "firing".into(),
            rule: "node_offline".into(),
            subject: "desktop".into(),
            level: "critical".into(),
            message: "node desktop offline for 2m".into(),
            unix: 10,
            since_unix: 5,
            repeat: false,
        };
        let slack: serde_json::Value = serde_json::from_str(&n.body(WebhookFormat::Slack)).unwrap();
        assert!(
            slack["text"]
                .as_str()
                .unwrap()
                .contains("[homelab] node desktop offline")
        );
        let discord: serde_json::Value =
            serde_json::from_str(&n.body(WebhookFormat::Discord)).unwrap();
        assert!(discord["content"].as_str().unwrap().contains("🚨"));
        let json: serde_json::Value = serde_json::from_str(&n.body(WebhookFormat::Json)).unwrap();
        assert_eq!(json["rule"], "node_offline");
        assert_eq!(json["status"], "firing");
        assert_eq!(json["subject"], "desktop");
    }

    #[test]
    fn values_print_as_prometheus_numbers() {
        assert_eq!(fmt_value(3.0), "3");
        assert_eq!(fmt_value(0.25), "0.25");
        assert_eq!(fmt_value(f64::NAN), "NaN");
        assert_eq!(fmt_value(f64::INFINITY), "+Inf");
    }
}
