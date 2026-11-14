//! Scheduling beyond priorities and selectors (1.9): gangs, spreading and
//! affinity between jobs, quotas in CPUs and memory (also per node label),
//! checkpoints, and schedules in a time zone of their own.
//!
//! None of this changes a record that already crosses the wire or sits on
//! disk. A [`Placement`] rides in [`JobSubmission::placement`], which
//! postcard skips: the CLI sends it beside the submission
//! ([`crate::msg::AdminOp::SubmitPlaced`]) only when there is one, so a
//! 1.8 control plane still takes every job that does not use it, and the
//! control plane keeps it in a table of its own.

use std::{collections::BTreeMap, fmt, str::FromStr};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    job::{Executor, JobSubmission, Tier, format_duration_ms, parse_duration_ms},
    manage::{LabelSet, Selector, validate_label_key},
    store::{format_size, parse_size},
};

/// The stop signal's grace period when a job asks for checkpoints without
/// one.
pub const DEFAULT_GRACE_MS: u64 = 30_000;
/// The longest grace period a job may ask for.
pub const MAX_GRACE_MS: u64 = 10 * 60_000;
/// Where a checkpointing task keeps what it wants back (an environment
/// variable of the same name says it too).
pub const CHECKPOINT_DIR: &str = "/checkpoint";
pub const CHECKPOINT_ENV: &str = "PUMASI_CHECKPOINT";
/// Jobs a job may run next to or away from.
pub const MAX_AFFINITY: usize = 16;

/// What nodes are grouped by when tasks spread or keep close: each node on
/// its own, or every node with the same value of a label (`zone`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Domain {
    #[default]
    Node,
    Label(String),
}

impl Domain {
    /// The group a node is in, or `None` when it lacks the label.
    pub fn of(&self, node: &str, labels: &LabelSet) -> Option<String> {
        match self {
            Domain::Node => Some(node.to_string()),
            Domain::Label(k) => labels.get(k).filter(|v| !v.is_empty()).map(|v| {
                let mut v = v.clone();
                v.sort();
                v.join("|")
            }),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if let Domain::Label(k) = self {
            validate_label_key(k)?;
        }
        Ok(())
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Domain::Node => f.write_str("node"),
            Domain::Label(k) => f.write_str(k),
        }
    }
}

impl FromStr for Domain {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let d = match s {
            "node" | "" => Domain::Node,
            k => Domain::Label(k.to_string()),
        };
        d.validate()?;
        Ok(d)
    }
}

/// Spreading a job's tasks over nodes or label values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spread {
    pub by: Domain,
    /// At most this many of the job's runs at once in one group; more wait.
    /// Without it the scheduler only prefers the group with the fewest.
    pub max: Option<u32>,
}

/// Checkpoints: a task that has to move gets a stop signal and a grace
/// period, and what it wrote to [`CHECKPOINT_DIR`] comes back when it runs
/// again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// From the stop signal (SIGTERM) to the kill.
    pub grace_ms: u64,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            grace_ms: DEFAULT_GRACE_MS,
        }
    }
}

/// How a job's tasks are placed beyond priority and selector (1.9).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// Every task of the job starts together or none does; when one has to
    /// stop, the others stop too and they start again together.
    pub gang: bool,
    pub spread: Option<Spread>,
    /// Only where tasks of each of these jobs (names or ids, the same
    /// account's) run now, grouped by `by`.
    pub near: Vec<String>,
    /// Never where tasks of any of these jobs run now.
    pub away: Vec<String>,
    /// What `near` and `away` group nodes by.
    pub by: Domain,
    pub checkpoint: Option<Checkpoint>,
}

impl Placement {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Whether it says anything about where tasks go (not only about
    /// starting together or checkpoints).
    pub fn constrains_nodes(&self) -> bool {
        self.spread.is_some() || !self.near.is_empty() || !self.away.is_empty()
    }

    pub fn validate(&self, sub: &JobSubmission) -> Result<()> {
        if let Some(s) = &self.spread {
            s.by.validate()?;
            if let Some(m) = s.max {
                ensure!(m >= 1, "spread.max must be at least 1");
            }
        }
        self.by.validate()?;
        ensure!(
            self.near.len() + self.away.len() <= MAX_AFFINITY,
            "a job names at most {MAX_AFFINITY} jobs in `near` and `away`"
        );
        for j in self.near.iter().chain(&self.away) {
            ensure!(
                !j.trim().is_empty() && j.len() <= 128,
                "`near` and `away` name jobs by name or id: {j:?}"
            );
        }
        if let Some(n) = self.near.iter().find(|n| self.away.contains(n)) {
            bail!("job {n:?} is both in `near` and in `away`");
        }
        if self.gang {
            ensure!(
                sub.tasks.len() <= 1024,
                "a gang has at most 1024 tasks (this job has {})",
                sub.tasks.len()
            );
        }
        if let Some(c) = &self.checkpoint {
            ensure!(
                (1000..=MAX_GRACE_MS).contains(&c.grace_ms),
                "checkpoint.grace must be between 1s and {}",
                format_duration_ms(MAX_GRACE_MS)
            );
            ensure!(
                matches!(
                    sub.executor,
                    Executor::Wasm | Executor::Docker | Executor::Image
                ),
                "checkpoints are for wasm and docker jobs (not microVMs yet)"
            );
            ensure!(
                sub.tier != Tier::Confidential,
                "confidential jobs cannot keep checkpoints: they would leave the VM in the clear"
            );
            ensure!(
                !sub.deterministic,
                "a deterministic job cannot keep checkpoints (a resumed run computes differently); set deterministic = false"
            );
        }
        Ok(())
    }

    /// `gang, spread by zone (max 2), near db, away batch, checkpoints (grace 30s)`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if self.gang {
            parts.push("gang".to_string());
        }
        if let Some(s) = &self.spread {
            let mut p = format!("spread by {}", s.by);
            if let Some(m) = s.max {
                p.push_str(&format!(" (max {m})"));
            }
            parts.push(p);
        }
        let by = match &self.by {
            Domain::Node => String::new(),
            d => format!(" (by {d})"),
        };
        if !self.near.is_empty() {
            parts.push(format!("near {}{by}", self.near.join(", ")));
        }
        if !self.away.is_empty() {
            parts.push(format!("away from {}{by}", self.away.join(", ")));
        }
        if let Some(c) = &self.checkpoint {
            parts.push(format!(
                "checkpoints (grace {})",
                format_duration_ms(c.grace_ms)
            ));
        }
        if parts.is_empty() {
            "none".into()
        } else {
            parts.join(", ")
        }
    }
}

// ---------------------------------------------------------------------------
// job.toml
// ---------------------------------------------------------------------------

/// `spread = "zone"` or `spread = { by = "zone", max = 2 }`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum SpreadFile {
    By(String),
    Table(SpreadTable),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpreadTable {
    #[serde(default)]
    pub by: Option<String>,
    pub max: Option<u32>,
}

/// `[affinity]` in job.toml.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffinityFile {
    #[serde(default)]
    pub near: Vec<String>,
    #[serde(default)]
    pub away: Vec<String>,
    /// `node` (default) or a label key.
    pub by: Option<String>,
}

/// `checkpoint = true` or `checkpoint = { grace = "1m" }`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum CheckpointFile {
    On(bool),
    Table(CheckpointTable),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTable {
    #[serde(default)]
    pub grace: Option<String>,
}

/// The 1.9 fields of job.toml.
pub fn placement_of(
    gang: bool,
    spread: Option<&SpreadFile>,
    affinity: Option<&AffinityFile>,
    checkpoint: Option<&CheckpointFile>,
) -> Result<Placement> {
    let spread = match spread {
        None => None,
        Some(SpreadFile::By(by)) => Some(Spread {
            by: by.parse()?,
            max: None,
        }),
        Some(SpreadFile::Table(SpreadTable { by, max })) => Some(Spread {
            by: by.as_deref().unwrap_or("node").parse()?,
            max: *max,
        }),
    };
    let checkpoint = match checkpoint {
        None | Some(CheckpointFile::On(false)) => None,
        Some(CheckpointFile::On(true)) => Some(Checkpoint::default()),
        Some(CheckpointFile::Table(CheckpointTable { grace })) => Some(Checkpoint {
            grace_ms: grace
                .as_deref()
                .map(parse_duration_ms)
                .transpose()
                .context("checkpoint.grace")?
                .unwrap_or(DEFAULT_GRACE_MS),
        }),
    };
    let a = affinity.cloned().unwrap_or_default();
    Ok(Placement {
        gang,
        spread,
        near: a.near.iter().map(|s| s.trim().to_string()).collect(),
        away: a.away.iter().map(|s| s.trim().to_string()).collect(),
        by: a.by.as_deref().unwrap_or("node").parse()?,
        checkpoint,
    })
}

// ---------------------------------------------------------------------------
// Quotas in CPUs and memory
// ---------------------------------------------------------------------------

/// What an account's runs may hold at once in CPUs and memory, across the
/// cluster and on the nodes a selector picks (1.9). It sits beside
/// [`crate::manage::AccountQuota`] (slots, GPUs, CU per day), which keeps
/// its layout. A run counts the CPUs and memory its job asks for: a wasm
/// task one CPU, an image task `resources.cpus` rounded up (1 without).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceQuota {
    pub cpus: Option<u32>,
    pub memory_bytes: Option<u64>,
    /// Limits on the nodes each selector matches (`zone=office`).
    pub labels: Vec<LabelQuota>,
}

/// Limits on what an account holds on some nodes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelQuota {
    pub selector: Selector,
    pub slots: Option<u32>,
    pub cpus: Option<u32>,
    pub memory_bytes: Option<u64>,
    pub gpus: Option<u32>,
}

/// What runs hold: runs (slots), CPUs, memory and GPUs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceUse {
    pub runs: u32,
    pub cpus: u32,
    pub memory_bytes: u64,
    pub gpus: u32,
}

impl ResourceUse {
    pub fn add(&mut self, o: &ResourceUse) {
        self.runs += o.runs;
        self.cpus += o.cpus;
        self.memory_bytes += o.memory_bytes;
        self.gpus += o.gpus;
    }
}

/// Which limit `used` plus `more` would go over, if any.
fn over(
    slots: Option<u32>,
    cpus: Option<u32>,
    memory: Option<u64>,
    gpus: Option<u32>,
    used: &ResourceUse,
    more: &ResourceUse,
) -> Option<String> {
    if let Some(m) = slots
        && used.runs + more.runs > m
    {
        return Some(format!("{m} runs"));
    }
    if let Some(m) = cpus
        && used.cpus + more.cpus > m
    {
        return Some(format!("{m} CPUs"));
    }
    if let Some(m) = memory
        && used.memory_bytes + more.memory_bytes > m
    {
        return Some(format!("{} of memory", format_size(m)));
    }
    if let Some(m) = gpus
        && more.gpus > 0
        && used.gpus + more.gpus > m
    {
        return Some(format!("{m} GPUs"));
    }
    None
}

impl ResourceQuota {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.labels.len() <= 32, "at most 32 label quotas");
        for l in &self.labels {
            l.selector.validate()?;
            ensure!(
                !l.selector.is_empty(),
                "a label quota needs a selector (`zone=office`)"
            );
            ensure!(
                l.slots.is_some()
                    || l.cpus.is_some()
                    || l.memory_bytes.is_some()
                    || l.gpus.is_some(),
                "the label quota on {} limits nothing",
                l.selector
            );
        }
        Ok(())
    }

    /// The account-wide limit `used` plus `more` would go over.
    pub fn over(&self, used: &ResourceUse, more: &ResourceUse) -> Option<String> {
        over(None, self.cpus, self.memory_bytes, None, used, more)
    }

    /// `16 CPUs, 64.0 GiB; zone=office: 4 runs, 8 CPUs`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(c) = self.cpus {
            parts.push(format!("{c} CPUs"));
        }
        if let Some(m) = self.memory_bytes {
            parts.push(format!("{} of memory", format_size(m)));
        }
        let mut out = parts.join(", ");
        for l in &self.labels {
            if !out.is_empty() {
                out.push_str("; ");
            }
            out.push_str(&l.to_string());
        }
        if out.is_empty() { "none".into() } else { out }
    }
}

impl LabelQuota {
    /// The limit `used` (on the matching nodes) plus `more` would go over.
    pub fn over(&self, used: &ResourceUse, more: &ResourceUse) -> Option<String> {
        over(
            self.slots,
            self.cpus,
            self.memory_bytes,
            self.gpus,
            used,
            more,
        )
        .map(|what| format!("{what} on {}", self.selector))
    }
}

/// `zone=office slots=4 cpus=8 memory=16G gpus=1`: the selector, then the
/// limits, separated by spaces (`--label-quota` and the dashboard).
impl fmt::Display for LabelQuota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.selector)?;
        if let Some(s) = self.slots {
            write!(f, " slots={s}")?;
        }
        if let Some(c) = self.cpus {
            write!(f, " cpus={c}")?;
        }
        if let Some(m) = self.memory_bytes {
            write!(f, " memory={}", crate::store::format_size_short(m))?;
        }
        if let Some(g) = self.gpus {
            write!(f, " gpus={g}")?;
        }
        Ok(())
    }
}

impl FromStr for LabelQuota {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut words = s.split_whitespace();
        let selector: Selector = words
            .next()
            .context("a label quota is `SELECTOR slots=N cpus=N memory=SIZE gpus=N`")?
            .parse()?;
        let mut q = LabelQuota {
            selector,
            ..Default::default()
        };
        for w in words {
            let (k, v) = w
                .split_once('=')
                .with_context(|| format!("{w:?} is not `limit=value`"))?;
            let num = || -> Result<u32> {
                v.parse()
                    .with_context(|| format!("{k} must be a whole number, not {v:?}"))
            };
            match k {
                "slots" | "runs" => q.slots = Some(num()?),
                "cpus" => q.cpus = Some(num()?),
                "gpus" => q.gpus = Some(num()?),
                "memory" => q.memory_bytes = Some(parse_size(v)?),
                other => bail!("unknown limit {other:?} (slots, cpus, memory or gpus)"),
            }
        }
        ResourceQuota {
            labels: vec![q.clone()],
            ..Default::default()
        }
        .validate()?;
        Ok(q)
    }
}

/// An account's CPU and memory quota and what it holds now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub account: String,
    pub quota: ResourceQuota,
    pub used: ResourceUse,
    /// What it holds on the nodes of each of `quota.labels`, in order.
    pub labels_used: Vec<ResourceUse>,
}

/// `[[accounts.NAME.label_quotas]]` in cluster.toml.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelQuotaFile {
    pub selector: String,
    pub slots: Option<u32>,
    pub cpus: Option<u32>,
    pub memory: Option<String>,
    pub gpus: Option<u32>,
}

impl LabelQuotaFile {
    pub fn quota(&self) -> Result<LabelQuota> {
        Ok(LabelQuota {
            selector: self.selector.parse()?,
            slots: self.slots,
            cpus: self.cpus,
            memory_bytes: self.memory.as_deref().map(parse_size).transpose()?,
            gpus: self.gpus,
        })
    }
}

// ---------------------------------------------------------------------------
// Schedules and views
// ---------------------------------------------------------------------------

/// What a schedule has beyond its [`crate::manage::ScheduleSpec`] (1.9).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleExt {
    /// An IANA time zone (`Asia/Seoul`) the cron expression is read in;
    /// without one, the control plane's.
    pub tz: Option<String>,
    /// The scheduled job's placement.
    pub placement: Placement,
}

impl ScheduleExt {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Checks an IANA time zone name.
pub fn validate_tz(tz: &str) -> Result<()> {
    tz.parse::<chrono_tz::Tz>()
        .map_err(|_| anyhow::anyhow!("unknown time zone {tz:?} (an IANA name like Asia/Seoul)"))?;
    Ok(())
}

/// What `apply` carries beyond an [`crate::manage::ApplySpec`] (1.9).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyExt {
    pub resources: Vec<(String, ResourceQuota)>,
    pub schedules: Vec<(String, ScheduleExt)>,
}

impl ApplyExt {
    /// Nothing an older control plane would miss: no CPU or memory quota
    /// (a listed account without one has none) and no schedule time zone
    /// or placement.
    pub fn is_empty(&self) -> bool {
        self.resources.iter().all(|(_, q)| q.is_empty())
            && self.schedules.iter().all(|(_, s)| s.is_empty())
    }
}

/// A task's latest checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointView {
    pub index: u32,
    pub size: u64,
    pub files: u32,
    pub saved_unix: u64,
    /// The node that wrote it (its name).
    pub node: String,
    /// Runs that started from a checkpoint.
    pub resumed: u32,
}

/// A job's placement and how it goes (`pumasi job`, the dashboard).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementView {
    pub placement: Placement,
    /// Runs being stopped now so they can move (they keep their slot until
    /// they have saved their checkpoint).
    pub stopping: Vec<u32>,
    pub checkpoints: Vec<CheckpointView>,
    /// Why its queued tasks wait, when placement is why.
    pub waiting: Option<String>,
}

/// One limit per line, for `pumasi quota`.
pub fn usage_lines(v: &ResourceView) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(c) = v.quota.cpus {
        out.push(format!("cpus {}/{c}", v.used.cpus));
    }
    if let Some(m) = v.quota.memory_bytes {
        out.push(format!(
            "memory {}/{}",
            format_size(v.used.memory_bytes),
            format_size(m)
        ));
    }
    for (l, u) in v.quota.labels.iter().zip(&v.labels_used) {
        let mut parts = Vec::new();
        if let Some(s) = l.slots {
            parts.push(format!("runs {}/{s}", u.runs));
        }
        if let Some(c) = l.cpus {
            parts.push(format!("cpus {}/{c}", u.cpus));
        }
        if let Some(m) = l.memory_bytes {
            parts.push(format!(
                "memory {}/{}",
                format_size(u.memory_bytes),
                format_size(m)
            ));
        }
        if let Some(g) = l.gpus {
            parts.push(format!("gpus {}/{g}", u.gpus));
        }
        out.push(format!("on {}: {}", l.selector, parts.join(", ")));
    }
    out
}

/// Account name to quota, for comparing `apply` files.
pub fn resources_by_account(list: &[(String, ResourceQuota)]) -> BTreeMap<&str, &ResourceQuota> {
    list.iter().map(|(a, q)| (a.as_str(), q)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_quotas_read_and_print_the_same() {
        let q: LabelQuota = "zone=office slots=4 cpus=8 memory=16G gpus=1"
            .parse()
            .unwrap();
        assert_eq!(q.slots, Some(4));
        assert_eq!(q.cpus, Some(8));
        assert_eq!(q.memory_bytes, Some(16 << 30));
        assert_eq!(q.gpus, Some(1));
        assert_eq!(q.to_string().parse::<LabelQuota>().unwrap(), q);
        assert!("zone=office".parse::<LabelQuota>().is_err());
        assert!("zone=office disks=2".parse::<LabelQuota>().is_err());
        let used = ResourceUse {
            runs: 3,
            cpus: 6,
            memory_bytes: 0,
            gpus: 0,
        };
        let one = ResourceUse {
            runs: 1,
            cpus: 2,
            memory_bytes: 1 << 30,
            gpus: 0,
        };
        assert_eq!(q.over(&used, &one), None);
        let two = ResourceUse { runs: 2, ..one };
        assert_eq!(q.over(&used, &two).unwrap(), "4 runs on zone=office");
    }

    #[test]
    fn domains_group_nodes() {
        let mut l = LabelSet::new();
        l.insert("zone".into(), vec!["b".into(), "a".into()]);
        assert_eq!(Domain::Node.of("n1", &l).unwrap(), "n1");
        assert_eq!(Domain::Label("zone".into()).of("n1", &l).unwrap(), "a|b");
        assert_eq!(Domain::Label("rack".into()).of("n1", &l), None);
        assert_eq!("node".parse::<Domain>().unwrap(), Domain::Node);
        assert!("Bad Key".parse::<Domain>().is_err());
    }

    #[test]
    fn time_zones_are_checked() {
        validate_tz("Asia/Seoul").unwrap();
        validate_tz("UTC").unwrap();
        assert!(validate_tz("Mars/Olympus").is_err());
    }
}
