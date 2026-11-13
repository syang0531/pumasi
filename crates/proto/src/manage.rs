//! Cluster management (phase 10): node labels and taints, job selectors
//! and tolerations, priorities, account quotas, access tickets with roles,
//! schedules (cron) and the declarative `cluster.toml` that `pumasi apply`
//! reads.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail, ensure};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use serde::{Deserialize, Serialize};

use crate::{
    economy::{MilliCu, format_cu, parse_cu, validate_account},
    job::{Executor, JobFile, JobSubmission},
    monitor::{AlertPolicy, WebhookFormat, WebhookSpec},
    store::{format_size, parse_size},
};

/// Lowest and highest job priority. Jobs default to 0.
pub const MIN_PRIORITY: i32 = -1000;
pub const MAX_PRIORITY: i32 = 1000;

/// Labels the cluster derives from what a node reports; they cannot be set
/// by hand. `executor` and `gpu` may have several values.
pub const BUILTIN_LABELS: &[&str] = &[
    "name",
    "os",
    "arch",
    "role",
    "account",
    "executor",
    "gpu",
    "gpus",
    "disk",
    "gpu.graphics",
];

/// A node's labels: each key has one or more values.
pub type LabelSet = BTreeMap<String, Vec<String>>;

/// Label keys: 1 to 63 characters of `a-z`, `0-9`, `.`, `_`, `/`, `-`,
/// starting with a letter or digit.
pub fn validate_label_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= 63,
        "label key must be 1 to 63 characters: {key:?}"
    );
    let mut chars = key.chars();
    let first = chars.next().unwrap_or(' ');
    ensure!(
        first.is_ascii_lowercase() || first.is_ascii_digit(),
        "label key must start with a-z or 0-9: {key:?}"
    );
    ensure!(
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._/-".contains(c)),
        "label key may use only a-z, 0-9, '.', '_', '/' and '-': {key:?}"
    );
    Ok(())
}

/// Label values: 1 to 63 characters of `A-Z`, `a-z`, `0-9`, `.`, `_`, `-`.
pub fn validate_label_value(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= 63,
        "label value must be 1 to 63 characters: {value:?}"
    );
    ensure!(
        value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)),
        "label value may use only letters, digits, '.', '_' and '-': {value:?}"
    );
    Ok(())
}

/// A label an admin may set: a valid key that is not built in.
pub fn validate_user_label(key: &str, value: &str) -> Result<()> {
    validate_label_key(key)?;
    validate_label_value(value)?;
    ensure!(
        !BUILTIN_LABELS.contains(&key),
        "`{key}` is a built-in label (the cluster sets it from what the node reports)"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Taints and tolerations
// ---------------------------------------------------------------------------

/// Keeps jobs off a node unless they tolerate it: `gpu-only`,
/// `owner=alice`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Taint {
    pub key: String,
    pub value: Option<String>,
}

/// Lets a job onto nodes with a matching taint: `key` tolerates the key
/// with any value, `key=value` only that value, `*` every taint.
pub type Toleration = Taint;

impl Taint {
    pub fn validate(&self) -> Result<()> {
        if self.key == "*" && self.value.is_none() {
            return Ok(());
        }
        validate_label_key(&self.key)?;
        if let Some(v) = &self.value {
            validate_label_value(v)?;
        }
        Ok(())
    }

    /// Whether this toleration lets a job onto a node with `taint`.
    pub fn tolerates(&self, taint: &Taint) -> bool {
        (self.key == "*" && self.value.is_none())
            || (self.key == taint.key && (self.value.is_none() || self.value == taint.value))
    }
}

impl fmt::Display for Taint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(v) => write!(f, "{}={v}", self.key),
            None => f.write_str(&self.key),
        }
    }
}

impl FromStr for Taint {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let t = match s.split_once('=') {
            Some((k, v)) => Taint {
                key: k.trim().to_string(),
                value: Some(v.trim().to_string()),
            },
            None => Taint {
                key: s.to_string(),
                value: None,
            },
        };
        t.validate()?;
        Ok(t)
    }
}

/// Whether `tolerations` cover every taint of a node.
pub fn tolerated(taints: &[Taint], tolerations: &[Toleration]) -> bool {
    taints
        .iter()
        .all(|t| tolerations.iter().any(|tol| tol.tolerates(t)))
}

// ---------------------------------------------------------------------------
// Selectors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectorOp {
    /// The node has the key with one of these values.
    In(Vec<String>),
    /// The node lacks the key, or has none of these values.
    NotIn(Vec<String>),
    Exists,
    NotExists,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectorTerm {
    pub key: String,
    pub op: SelectorOp,
}

/// Which nodes a job may run on: every term must match. Written
/// `os=linux,disk=nvme|ssd,gpu!=amd,zone,!spot` on the command line and as
/// a `[selector]` table in job.toml.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selector(pub Vec<SelectorTerm>);

impl Selector {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn matches(&self, labels: &LabelSet) -> bool {
        self.0.iter().all(|t| {
            let have = labels.get(&t.key);
            match &t.op {
                SelectorOp::In(vals) => have.is_some_and(|h| h.iter().any(|v| vals.contains(v))),
                SelectorOp::NotIn(vals) => have.is_none_or(|h| !h.iter().any(|v| vals.contains(v))),
                SelectorOp::Exists => have.is_some(),
                SelectorOp::NotExists => have.is_none(),
            }
        })
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.0.len() <= 32, "a selector has at most 32 terms");
        for t in &self.0 {
            validate_label_key(&t.key)?;
            if let SelectorOp::In(v) | SelectorOp::NotIn(v) = &t.op {
                ensure!(!v.is_empty(), "selector term `{}` has no values", t.key);
                for v in v {
                    validate_label_value(v)?;
                }
            }
        }
        Ok(())
    }

    /// The `[selector]` table of job.toml.
    pub fn from_table(table: &BTreeMap<String, SelectorValue>) -> Result<Self> {
        let mut terms = Vec::new();
        for (key, value) in table {
            let op = match value {
                SelectorValue::Bool(true) => SelectorOp::Exists,
                SelectorValue::Bool(false) => SelectorOp::NotExists,
                SelectorValue::One(s) => match s.strip_prefix('!') {
                    Some(rest) => SelectorOp::NotIn(split_values(rest)),
                    None => SelectorOp::In(split_values(s)),
                },
                SelectorValue::Many(v) => {
                    SelectorOp::In(v.iter().map(|s| s.trim().into()).collect())
                }
            };
            terms.push(SelectorTerm {
                key: key.trim().to_string(),
                op,
            });
        }
        let s = Selector(terms);
        s.validate()?;
        Ok(s)
    }
}

fn split_values(s: &str) -> Vec<String> {
    s.split('|')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect()
}

/// A value in job.toml's `[selector]`: `"linux"`, `"!amd"`,
/// `"nvme|ssd"`, `["nvme", "ssd"]`, `true` (has the label) or `false`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum SelectorValue {
    Bool(bool),
    One(String),
    Many(Vec<String>),
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .0
            .iter()
            .map(|t| match &t.op {
                SelectorOp::In(v) => format!("{}={}", t.key, v.join("|")),
                SelectorOp::NotIn(v) => format!("{}!={}", t.key, v.join("|")),
                SelectorOp::Exists => t.key.clone(),
                SelectorOp::NotExists => format!("!{}", t.key),
            })
            .collect();
        f.write_str(&parts.join(","))
    }
}

impl FromStr for Selector {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut terms = Vec::new();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let term = if let Some((k, v)) = part.split_once("!=") {
                SelectorTerm {
                    key: k.trim().into(),
                    op: SelectorOp::NotIn(split_values(v)),
                }
            } else if let Some((k, v)) = part.split_once('=') {
                SelectorTerm {
                    key: k.trim().into(),
                    op: SelectorOp::In(split_values(v)),
                }
            } else if let Some(k) = part.strip_prefix('!') {
                SelectorTerm {
                    key: k.trim().into(),
                    op: SelectorOp::NotExists,
                }
            } else {
                SelectorTerm {
                    key: part.into(),
                    op: SelectorOp::Exists,
                }
            };
            terms.push(term);
        }
        let sel = Selector(terms);
        sel.validate()?;
        Ok(sel)
    }
}

impl Selector {
    /// Nodes as people name them (1.7): a node name, names separated by
    /// commas (`imac,gaming-pc`), or a label selector (`os=windows`), as
    /// `--canary`, `alert silence --on` and the dashboard take them.
    pub fn nodes(text: &str) -> Result<Selector> {
        let s = text.trim();
        ensure!(!s.is_empty(), "which nodes?");
        let sel: Selector = if s.contains(['=', '!']) {
            s.parse()?
        } else {
            let names: Vec<&str> = s.split(',').map(str::trim).filter(|n| !n.is_empty()).collect();
            format!("name={}", names.join("|")).parse()?
        };
        sel.validate()?;
        Ok(sel)
    }
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// What an admin set on a node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub labels: BTreeMap<String, String>,
    pub taints: Vec<Taint>,
    /// Takes no new tasks (`pumasi cordon`, `pumasi drain`).
    pub cordoned: bool,
}

impl NodeConfig {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.labels.len() <= 64, "a node has at most 64 labels");
        ensure!(self.taints.len() <= 32, "a node has at most 32 taints");
        for (k, v) in &self.labels {
            validate_user_label(k, v)?;
        }
        for t in &self.taints {
            t.validate()?;
            ensure!(t.key != "*", "`*` is for tolerations, not taints");
        }
        Ok(())
    }
}

/// A change to one node's configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEdit {
    pub set_labels: BTreeMap<String, String>,
    pub remove_labels: Vec<String>,
    pub add_taints: Vec<Taint>,
    /// Taint keys to remove (every value).
    pub remove_taints: Vec<String>,
    pub cordon: Option<bool>,
}

impl NodeEdit {
    pub fn apply(&self, c: &NodeConfig) -> Result<NodeConfig> {
        let mut c = c.clone();
        for k in &self.remove_labels {
            c.labels.remove(k);
        }
        for (k, v) in &self.set_labels {
            c.labels.insert(k.clone(), v.clone());
        }
        c.taints.retain(|t| !self.remove_taints.contains(&t.key));
        for t in &self.add_taints {
            // One value per key: a new value replaces the old one.
            c.taints.retain(|o| o.key != t.key);
            c.taints.push(t.clone());
        }
        c.taints.sort();
        if let Some(cordon) = self.cordon {
            c.cordoned = cordon;
        }
        c.validate()?;
        Ok(c)
    }
}

// ---------------------------------------------------------------------------
// Quotas
// ---------------------------------------------------------------------------

/// Limits on what one account's jobs use. `None` is no limit. Work over a
/// limit waits in the queue.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuota {
    /// Runs at once (task slots across the cluster).
    pub slots: Option<u32>,
    /// GPUs in use at once.
    pub gpus: Option<u32>,
    /// Milli-CU the account may spend in any 24 hours on other accounts'
    /// machines (and on storage); past it, only its own machines run its
    /// work.
    pub cu_per_day: Option<MilliCu>,
    /// Highest priority its jobs may ask for. Without it, jobs of a
    /// submitter ticket stay at 0 or below; the admin may use any.
    pub max_priority: Option<i32>,
    /// File store bytes (the same quota as `pumasi store quota`).
    pub storage_bytes: Option<u64>,
}

impl AccountQuota {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(p) = self.max_priority {
            ensure!(
                (MIN_PRIORITY..=MAX_PRIORITY).contains(&p),
                "max_priority must be between {MIN_PRIORITY} and {MAX_PRIORITY}"
            );
        }
        if let Some(c) = self.cu_per_day {
            ensure!(c >= 0, "cu_per_day must not be negative");
        }
        Ok(())
    }

    /// `slots 4, gpus 1, 100 CU/day, priority <= 10, storage 50.0 GiB`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(s) = self.slots {
            parts.push(format!("slots {s}"));
        }
        if let Some(g) = self.gpus {
            parts.push(format!("gpus {g}"));
        }
        if let Some(c) = self.cu_per_day {
            parts.push(format!("{} CU/day", format_cu(c)));
        }
        if let Some(p) = self.max_priority {
            parts.push(format!("priority <= {p}"));
        }
        if let Some(b) = self.storage_bytes {
            parts.push(format!("storage {}", format_size(b)));
        }
        if parts.is_empty() {
            "none".into()
        } else {
            parts.join(", ")
        }
    }
}

/// An account's quota and what it uses now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaView {
    pub account: String,
    pub quota: AccountQuota,
    /// Runs of its jobs right now.
    pub running: u32,
    pub gpus: u32,
    /// Milli-CU spent in the last 24 hours.
    pub spent_day: MilliCu,
    pub storage_bytes: u64,
    /// Tasks of its jobs waiting in the queue.
    pub queued: u32,
    // Phase 15 from here.
    /// What its jobs may be, for its submitter tickets.
    pub policy: AccountPolicy,
}

// ---------------------------------------------------------------------------
// Job policies (phase 15)
// ---------------------------------------------------------------------------

/// What jobs of one account may be, for submitter tickets of that account
/// (`pumasi quota NAME --images ... --executors docker --network deny`).
/// Admin tickets are not held to it. Every field left empty is no limit,
/// so the default policy allows everything.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountPolicy {
    /// Images the account's image jobs may use, as patterns (`*` any
    /// characters, `?` one). Matched against the image as written and as
    /// its full name (`python:3.12` is also `docker.io/library/python:3.12`).
    /// A pattern with `@sha256:` pins one image digest. Empty: any image.
    pub images: Vec<String>,
    /// Images must be named by digest (`name@sha256:...`), so a tag moved
    /// on the registry cannot change what runs.
    pub require_digest: bool,
    /// Executors its jobs may use. Empty: any.
    pub executors: Vec<Executor>,
    /// Refuse jobs that ask for the network (`network = true`).
    pub deny_network: bool,
    /// Added to every job's selector: its jobs run only on nodes that also
    /// match this.
    pub selector: Selector,
    /// Tolerations its jobs may carry (each one of the job's must be
    /// covered by one of these). `None`: any; `Some(empty)`: none, so its
    /// jobs never run on tainted nodes.
    pub tolerations: Option<Vec<Toleration>>,
    /// Longest task timeout its jobs may ask for, in milliseconds.
    pub max_timeout_ms: Option<u64>,
    /// Most GPUs one task may ask for (`gpus = "all"` is refused when set).
    pub max_gpus_per_task: Option<u32>,
}

impl AccountPolicy {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.images.len() <= 64, "at most 64 image patterns");
        for i in &self.images {
            ensure!(
                !i.trim().is_empty() && !i.contains(char::is_whitespace) && i.len() <= 512,
                "invalid image pattern {i:?}"
            );
        }
        self.selector.validate()?;
        if let Some(t) = &self.tolerations {
            ensure!(t.len() <= 32, "at most 32 tolerations");
            for t in t {
                t.validate()?;
            }
        }
        if let Some(t) = self.max_timeout_ms {
            ensure!(t > 0, "max_timeout must be positive");
        }
        if let Some(g) = self.max_gpus_per_task {
            ensure!(
                g <= crate::gpu::MAX_GPUS_PER_TASK,
                "max_gpus_per_task is at most {}",
                crate::gpu::MAX_GPUS_PER_TASK
            );
        }
        Ok(())
    }

    /// Whether `image` is one this policy allows.
    pub fn allows_image(&self, image: &str) -> bool {
        if self.require_digest && !image.contains("@sha256:") {
            return false;
        }
        if self.images.is_empty() {
            return true;
        }
        let full = full_image_name(image);
        self.images.iter().any(|p| {
            glob_match(p, image) || glob_match(p, &full) || glob_match(&full_image_name(p), &full)
        })
    }

    /// Checks a job against the policy and adds its selector. The error
    /// says what the policy refuses.
    pub fn admit(&self, sub: &mut JobSubmission) -> Result<()> {
        if !self.executors.is_empty() {
            // An image job runs as Docker or as a microVM: it is admitted
            // for those of the two the policy allows.
            let ways: Vec<Executor> = sub
                .executor
                .runs_as()
                .iter()
                .copied()
                .filter(|e| self.executors.contains(e))
                .collect();
            ensure!(
                self.executors.contains(&sub.executor) || !ways.is_empty(),
                "this account's policy allows only {} jobs, not {}",
                self.executors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                sub.executor
            );
            if !self.executors.contains(&sub.executor)
                && let [only] = ways.as_slice()
            {
                sub.executor = *only;
            }
        }
        if let Some(d) = &sub.docker {
            if !self.allows_image(&d.image) {
                if self.require_digest && !d.image.contains("@sha256:") {
                    bail!(
                        "this account's policy needs images named by digest (name@sha256:...), not {}",
                        d.image
                    );
                }
                bail!(
                    "this account's policy does not allow the image {} (allowed: {})",
                    d.image,
                    self.images.join(", ")
                );
            }
            ensure!(
                !(self.deny_network && d.network),
                "this account's policy does not allow network access (network = true)"
            );
        }
        if let Some(allowed) = &self.tolerations {
            for t in &sub.tolerations {
                ensure!(
                    allowed.iter().any(|a| a.tolerates(t)),
                    "this account's policy does not allow the toleration {t}"
                );
            }
        }
        if let Some(max) = self.max_timeout_ms {
            ensure!(
                sub.limits.timeout_ms <= max,
                "the timeout {} is above the {} this account's policy allows",
                crate::job::format_duration_ms(sub.limits.timeout_ms),
                crate::job::format_duration_ms(max)
            );
        }
        if let (Some(max), Some(g)) = (self.max_gpus_per_task, &sub.gpu) {
            ensure!(
                g.count != 0,
                "this account's policy allows at most {max} GPUs per task, so `gpus = \"all\"` is refused"
            );
            ensure!(
                g.count <= max,
                "{} GPUs per task is above the {max} this account's policy allows",
                g.count
            );
        }
        if !self.selector.is_empty() {
            for t in &self.selector.0 {
                if !sub.selector.0.contains(t) {
                    sub.selector.0.push(t.clone());
                }
            }
            sub.selector.validate()?;
        }
        Ok(())
    }

    /// `images ghcr.io/x/*, executors docker, no network, selector gpu=nvidia`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if !self.images.is_empty() {
            parts.push(format!("images {}", self.images.join(" ")));
        }
        if self.require_digest {
            parts.push("digests only".into());
        }
        if !self.executors.is_empty() {
            parts.push(format!(
                "executors {}",
                self.executors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if self.deny_network {
            parts.push("no network".into());
        }
        if !self.selector.is_empty() {
            parts.push(format!("selector {}", self.selector));
        }
        match &self.tolerations {
            Some(t) if t.is_empty() => parts.push("no tolerations".into()),
            Some(t) => parts.push(format!(
                "tolerations {}",
                t.iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )),
            None => {}
        }
        if let Some(t) = self.max_timeout_ms {
            parts.push(format!("timeout <= {}", crate::job::format_duration_ms(t)));
        }
        if let Some(g) = self.max_gpus_per_task {
            parts.push(format!("gpus/task <= {g}"));
        }
        if parts.is_empty() {
            "none".into()
        } else {
            parts.join(", ")
        }
    }
}

/// A change to one list of a policy (1.4): a whole new list, or entries
/// to add and to remove. Adding what is there and removing what is not
/// change nothing, so the same change can be sent twice.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPatch {
    pub set: Option<Vec<String>>,
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

impl ListPatch {
    pub fn is_empty(&self) -> bool {
        self.set.is_none() && self.add.is_empty() && self.remove.is_empty()
    }

    fn check(&self, what: &str) -> Result<()> {
        ensure!(
            self.set.is_none() || (self.add.is_empty() && self.remove.is_empty()),
            "{what}: give the whole list, or entries to add and remove, not both"
        );
        Ok(())
    }

    /// Applies the change to `list`. `open` says an empty list means no
    /// limit, so removing the last entry is refused: it would allow
    /// everything where the caller asked for less.
    fn apply<T: PartialEq + Clone>(
        &self,
        list: &mut Vec<T>,
        what: &str,
        open: bool,
        parse: impl Fn(&str) -> Result<T>,
    ) -> Result<()> {
        self.check(what)?;
        let parse_all = |v: &[String]| {
            v.iter()
                .map(|s| parse(s.trim()))
                .collect::<Result<Vec<T>>>()
        };
        if let Some(v) = &self.set {
            *list = Vec::new();
            for e in parse_all(v)? {
                if !list.contains(&e) {
                    list.push(e);
                }
            }
            return Ok(());
        }
        for e in parse_all(&self.add)? {
            if !list.contains(&e) {
                list.push(e);
            }
        }
        let remove = parse_all(&self.remove)?;
        let before = list.len();
        list.retain(|e| !remove.contains(e));
        ensure!(
            !(open && before > 0 && list.is_empty()),
            "{what}: removing the last entry would allow any; set the whole list to an empty one if that is meant"
        );
        Ok(())
    }
}

/// `wasm`, `docker`, `microvm` or `image`.
pub fn parse_executor(s: &str) -> Result<Executor> {
    match s.trim() {
        "wasm" => Ok(Executor::Wasm),
        "docker" => Ok(Executor::Docker),
        "microvm" => Ok(Executor::Microvm),
        "image" => Ok(Executor::Image),
        other => bail!("unknown executor {other:?} (wasm, docker, microvm or image)"),
    }
}

/// A change to an account's job policy that leaves what it does not name
/// as it is (1.4): `PATCH /api/v1/accounts/NAME/policy`, `pumasi quota
/// NAME --add-images ...` and the dashboard's Accounts page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyPatch {
    pub images: ListPatch,
    pub require_digest: Option<bool>,
    pub executors: ListPatch,
    pub deny_network: Option<bool>,
    pub selector: Option<Selector>,
    /// Entries as written on the command line (`key`, `key=value`, `*`).
    pub tolerations: ListPatch,
    /// Lifts the limit on tolerations (the policy's `None`).
    pub any_toleration: bool,
    pub max_timeout_ms: Option<Option<u64>>,
    pub max_gpus_per_task: Option<Option<u32>>,
}

impl PolicyPatch {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Changes `p` and checks the result.
    pub fn apply(&self, p: &mut AccountPolicy) -> Result<()> {
        self.images.apply(&mut p.images, "images", true, |s| {
            ensure!(!s.is_empty(), "an image pattern is empty");
            Ok(s.to_string())
        })?;
        self.executors
            .apply(&mut p.executors, "executors", true, parse_executor)?;
        p.executors.sort_by_key(|e| e.to_string());
        if let Some(v) = self.require_digest {
            p.require_digest = v;
        }
        if let Some(v) = self.deny_network {
            p.deny_network = v;
        }
        if let Some(s) = &self.selector {
            p.selector = s.clone();
        }
        ensure!(
            !(self.any_toleration && !self.tolerations.is_empty()),
            "tolerations: lift the limit or change the list, not both"
        );
        if self.any_toleration {
            p.tolerations = None;
        } else if !self.tolerations.is_empty() {
            ensure!(
                p.tolerations.is_some() || self.tolerations.remove.is_empty(),
                "tolerations: the account may carry any toleration, so there is nothing to remove; set the whole list first"
            );
            let mut list = p.tolerations.take().unwrap_or_default();
            self.tolerations
                .apply(&mut list, "tolerations", false, |s| s.parse::<Toleration>())?;
            p.tolerations = Some(list);
        }
        if let Some(v) = self.max_timeout_ms {
            p.max_timeout_ms = v;
        }
        if let Some(v) = self.max_gpus_per_task {
            p.max_gpus_per_task = v;
        }
        p.validate()
    }
}

/// An image reference with Docker's defaults filled in:
/// `python:3.12` is `docker.io/library/python:3.12`, `me/app` is
/// `docker.io/me/app:latest`.
pub fn full_image_name(image: &str) -> String {
    let (name, digest) = match image.split_once('@') {
        Some((n, d)) => (n, Some(d)),
        None => (image, None),
    };
    let (repo, tag) = match name.rsplit_once(':') {
        // A colon after the last slash is a tag; before it, a port.
        Some((r, t)) if !t.contains('/') => (r, Some(t)),
        _ => (name, None),
    };
    let first = repo.split('/').next().unwrap_or("");
    let has_registry =
        repo.contains('/') && (first.contains('.') || first.contains(':') || first == "localhost");
    let repo = if has_registry {
        repo.to_string()
    } else if repo.contains('/') {
        format!("docker.io/{repo}")
    } else {
        format!("docker.io/library/{repo}")
    };
    let mut out = repo;
    match (tag, digest) {
        (Some(t), _) => {
            out.push(':');
            out.push_str(t);
        }
        (None, None) => out.push_str(":latest"),
        (None, Some(_)) => {}
    }
    if let Some(d) = digest {
        out.push('@');
        out.push_str(d);
    }
    out
}

/// Matches `text` against a pattern where `*` is any run of characters
/// and `?` one character.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

// ---------------------------------------------------------------------------
// Access tickets
// ---------------------------------------------------------------------------

/// What an access ticket may do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessRole {
    /// Everything, like the cluster's own admin ticket.
    Admin,
    /// Submits and manages jobs, schedules and datasets of one account,
    /// and sees only those; cannot touch nodes, prices or other accounts.
    Submitter { account: String },
    /// Reads the cluster's state; changes nothing.
    Viewer,
}

impl AccessRole {
    pub fn validate(&self) -> Result<()> {
        if let AccessRole::Submitter { account } = self {
            validate_account(account)?;
        }
        Ok(())
    }
}

impl fmt::Display for AccessRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessRole::Admin => f.write_str("admin"),
            AccessRole::Submitter { account } => write!(f, "submitter ({account})"),
            AccessRole::Viewer => f.write_str("viewer"),
        }
    }
}

/// An access ticket as listed (never its secret).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessView {
    /// Short id derived from the secret, to revoke it by.
    pub id: String,
    pub name: String,
    pub role: AccessRole,
    pub created_unix: u64,
    pub expires_unix: Option<u64>,
}

/// The id of an access secret: the first bytes of its hash, in hex.
pub fn access_id(secret: &crate::Secret) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/access-id/v1\0");
    h.update(secret);
    crate::short_hex(h.finalize().as_bytes())
}

/// Ticket names: 1 to 64 printable characters.
pub fn validate_access_name(name: &str) -> Result<()> {
    ensure!(
        !name.trim().is_empty() && name.len() <= 64,
        "ticket name must be 1 to 64 characters"
    );
    ensure!(
        !name.chars().any(char::is_control),
        "ticket name must not contain control characters"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------

/// A job submitted again and again on a cron schedule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleSpec {
    pub name: String,
    /// Five-field cron (`minute hour day month weekday`) or `@daily`
    /// and friends, in the control plane's local time.
    pub cron: String,
    /// What each run submits (its account pays).
    pub job: JobSubmission,
    /// Start a run even when the previous one has not finished.
    pub overlap: bool,
}

impl ScheduleSpec {
    pub fn validate(&self) -> Result<()> {
        validate_schedule_name(&self.name)?;
        self.cron.parse::<Cron>()?;
        self.job.validate()?;
        ensure!(
            self.job.after.is_empty(),
            "a scheduled job cannot wait for other jobs (`after`)"
        );
        Ok(())
    }
}

/// Schedule names follow dataset names: 1 to 64 characters of `a-z`,
/// `0-9`, `.`, `_`, `-`.
pub fn validate_schedule_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 64,
        "schedule name must be 1 to 64 characters"
    );
    ensure!(
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c)),
        "schedule name may use only a-z, 0-9, '.', '_' and '-': {name:?}"
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleView {
    pub name: String,
    pub cron: String,
    pub account: String,
    /// The job's name and task count.
    pub job_name: String,
    pub tasks: u32,
    pub priority: i32,
    pub overlap: bool,
    pub next_unix: Option<u64>,
    pub last_unix: Option<u64>,
    pub last_job: Option<crate::job::JobId>,
    /// Why the last run was not submitted, if it was not.
    pub last_error: Option<String>,
    pub runs: u64,
}

/// A parsed cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    minutes: u64,
    hours: u32,
    days: u32,
    months: u16,
    weekdays: u8,
    any_day: bool,
    any_weekday: bool,
}

const MONTHS: &[&str] = &[
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const WEEKDAYS: &[&str] = &["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

fn cron_value(s: &str, min: u32, names: &[&str]) -> Result<u32> {
    let lower = s.to_ascii_lowercase();
    if let Some(i) = names.iter().position(|n| *n == lower) {
        return Ok(i as u32 + min);
    }
    s.parse::<u32>()
        .with_context(|| format!("invalid cron value {s:?}"))
}

fn cron_field(s: &str, min: u32, max: u32, names: &[&str]) -> Result<u64> {
    // Weekdays take 0-7, where 7 is Sunday again.
    let weekday = names == WEEKDAYS;
    let top = if weekday { 7 } else { max };
    let mut bits = 0u64;
    for part in s.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, st)) => {
                let st: u32 = st
                    .parse()
                    .with_context(|| format!("invalid cron step in {part:?}"))?;
                ensure!(st >= 1, "cron step must be at least 1 in {part:?}");
                (r, Some(st))
            }
            None => (part, None),
        };
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (cron_value(a, min, names)?, cron_value(b, min, names)?)
        } else {
            let v = cron_value(range, min, names)?;
            (v, if step.is_some() { top } else { v })
        };
        ensure!(
            lo >= min && hi <= top && lo <= hi,
            "cron field {s:?} is outside {min}-{max}"
        );
        let mut v = lo;
        while v <= hi {
            bits |= 1 << v;
            v += step.unwrap_or(1);
        }
    }
    if weekday && bits & (1 << 7) != 0 {
        bits = (bits & !(1 << 7)) | 1;
    }
    ensure!(bits != 0, "cron field {s:?} matches nothing");
    Ok(bits)
}

impl FromStr for Cron {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let expanded = match s {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            other if other.starts_with('@') => bail!("unknown cron macro {other:?}"),
            other => other,
        };
        let f: Vec<&str> = expanded.split_whitespace().collect();
        ensure!(
            f.len() == 5,
            "cron needs five fields (minute hour day month weekday), got {:?}",
            s
        );
        Ok(Cron {
            minutes: cron_field(f[0], 0, 59, &[])?,
            hours: cron_field(f[1], 0, 23, &[])? as u32,
            days: cron_field(f[2], 1, 31, &[])? as u32,
            months: cron_field(f[3], 1, 12, MONTHS)? as u16,
            weekdays: cron_field(f[4], 0, 6, WEEKDAYS)? as u8,
            any_day: f[2].starts_with('*'),
            any_weekday: f[4].starts_with('*'),
        })
    }
}

impl Cron {
    fn day_matches(&self, d: NaiveDate) -> bool {
        let dom = self.days & (1 << d.day()) != 0;
        let dow = self.weekdays & (1 << d.weekday().num_days_from_sunday()) != 0;
        match (self.any_day, self.any_weekday) {
            (true, true) => true,
            (true, false) => dow,
            (false, true) => dom,
            // Classic cron: either field may match when both are given.
            (false, false) => dom || dow,
        }
    }

    /// The first time after `after` (a unix time) that matches, in `tz`.
    pub fn next_after<Tz: TimeZone>(&self, after: i64, tz: &Tz) -> Option<i64> {
        let start = tz.timestamp_opt(after, 0).single()?.naive_local();
        let mut t: NaiveDateTime =
            start.date().and_hms_opt(start.hour(), start.minute(), 0)? + ChronoDuration::minutes(1);
        // Far more than a leap-day-on-a-weekday schedule needs.
        for _ in 0..200_000 {
            if self.months & (1 << t.month()) == 0 {
                let (y, m) = if t.month() == 12 {
                    (t.year() + 1, 1)
                } else {
                    (t.year(), t.month() + 1)
                };
                t = NaiveDate::from_ymd_opt(y, m, 1)?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.day_matches(t.date()) {
                t = t.date().succ_opt()?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if self.hours & (1 << t.hour()) == 0 {
                t = t.date().and_hms_opt(t.hour(), 0, 0)? + ChronoDuration::hours(1);
                continue;
            }
            if self.minutes & (1 << t.minute()) == 0 {
                t += ChronoDuration::minutes(1);
                continue;
            }
            match tz.from_local_datetime(&t).earliest() {
                Some(dt) if dt.timestamp() > after => return Some(dt.timestamp()),
                // In a daylight saving gap, or folded back before `after`.
                _ => t += ChronoDuration::minutes(1),
            }
        }
        None
    }

    /// [`Self::next_after`] in the machine's local time.
    pub fn next_local(&self, after: i64) -> Option<i64> {
        self.next_after(after, &chrono::Local)
    }
}

// ---------------------------------------------------------------------------
// cluster.toml and `pumasi apply`
// ---------------------------------------------------------------------------

/// Desired settings of one node, by name or id prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub node: String,
    pub labels: BTreeMap<String, String>,
    pub taints: Vec<Taint>,
    /// Left as it is when not given.
    pub cordoned: Option<bool>,
}

/// What `pumasi apply` sends: the file, resolved (job files read).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ApplySpec {
    pub nodes: Vec<NodeSpec>,
    pub accounts: Vec<(String, AccountQuota)>,
    pub schedules: Vec<ScheduleSpec>,
    /// Remove schedules (and webhooks) the file does not list.
    pub prune: bool,
    /// Report what would change without changing it.
    pub dry_run: bool,
    // Phase 11 from here.
    /// The whole alert policy, when the file has an `[alerts]` table.
    pub alerts: Option<AlertPolicy>,
    pub webhooks: Vec<WebhookSpec>,
    // Phase 15 from here.
    /// The job policy of every account in `accounts`.
    pub policies: Vec<(String, AccountPolicy)>,
}

impl ApplySpec {
    pub fn validate(&self) -> Result<()> {
        let mut seen = BTreeSet::new();
        for n in &self.nodes {
            ensure!(seen.insert(&n.node), "node {} is listed twice", n.node);
            NodeConfig {
                labels: n.labels.clone(),
                taints: n.taints.clone(),
                cordoned: false,
            }
            .validate()
            .with_context(|| format!("node {}", n.node))?;
        }
        let mut seen = BTreeSet::new();
        for (a, q) in &self.accounts {
            validate_account(a)?;
            ensure!(seen.insert(a), "account {a} is listed twice");
            q.validate().with_context(|| format!("account {a}"))?;
        }
        for (a, p) in &self.policies {
            p.validate().with_context(|| format!("account {a}"))?;
        }
        let mut seen = BTreeSet::new();
        for s in &self.schedules {
            ensure!(seen.insert(&s.name), "schedule {} is listed twice", s.name);
            s.validate()
                .with_context(|| format!("schedule {}", s.name))?;
        }
        if let Some(a) = &self.alerts {
            a.validate().context("alerts")?;
        }
        let mut seen = BTreeSet::new();
        for w in &self.webhooks {
            ensure!(seen.insert(&w.name), "webhook {} is listed twice", w.name);
            w.validate()
                .with_context(|| format!("webhook {}", w.name))?;
        }
        Ok(())
    }
}

/// The `cluster.toml` that `pumasi apply -f` reads:
///
/// ```toml
/// [nodes.gpu-box]                 # node name (or id prefix)
/// labels = { zone = "home", tier = "fast" }
/// taints = ["gpu-only"]
/// cordoned = false
///
/// [accounts.alice]
/// slots = 8
/// gpus = 1
/// cu_per_day = 100
/// max_priority = 10
/// storage = "50G"
///
/// [schedules.nightly]
/// cron = "0 3 * * *"
/// job = "nightly/job.toml"
/// account = "alice"
///
/// [alerts]                        # the whole policy; left-out rules
/// node_offline = "2m"             # take their defaults, "off" disables
/// disk = "85%"
///
/// [webhooks.ops]
/// url_env = "PUMASI_SLACK_URL"    # or url = "https://hooks.slack.com/..."
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterFile {
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeFile>,
    #[serde(default)]
    pub accounts: BTreeMap<String, AccountFile>,
    #[serde(default)]
    pub schedules: BTreeMap<String, ScheduleFile>,
    pub alerts: Option<AlertsFile>,
    #[serde(default)]
    pub webhooks: BTreeMap<String, WebhookFile>,
}

/// `[alerts]`: each rule a duration, percentage or number as text or
/// number, `"off"` or `false` to turn it off; left out, its default.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsFile {
    pub node_offline: Option<Setting>,
    pub failure_rate: Option<Setting>,
    pub failure_window: Option<Setting>,
    pub failure_min_runs: Option<u32>,
    pub disk: Option<Setting>,
    pub temperature: Option<Setting>,
    pub perf_drop: Option<Setting>,
    pub smart: Option<Setting>,
    pub check_mismatch: Option<Setting>,
}

/// A value in `[alerts]`: `false`, `90`, `"90%"`, `"2m"`, `"off"`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Setting {
    Bool(bool),
    Int(i64),
    Text(String),
}

impl Setting {
    fn text(&self) -> String {
        match self {
            Setting::Bool(false) => "off".into(),
            Setting::Bool(true) => "on".into(),
            Setting::Int(i) => i.to_string(),
            Setting::Text(t) => t.clone(),
        }
    }
}

impl AlertsFile {
    pub fn policy(&self) -> Result<AlertPolicy> {
        use crate::monitor::{FailureRate, parse_opt_percent, parse_opt_secs, parse_switch};
        let d = AlertPolicy::default();
        let secs = |s: &Option<Setting>, def: Option<u64>, what: &str| -> Result<Option<u64>> {
            match s {
                None => Ok(def),
                // A bare number of seconds.
                Some(Setting::Int(n)) if *n > 0 => Ok(Some(*n as u64)),
                Some(Setting::Bool(true)) => Ok(def),
                Some(v) => parse_opt_secs(&v.text()).with_context(|| what.to_string()),
            }
        };
        let percent = |s: &Option<Setting>, def: Option<u8>, what: &str| -> Result<Option<u8>> {
            match s {
                None | Some(Setting::Bool(true)) => Ok(def),
                Some(v) => parse_opt_percent(&v.text()).with_context(|| what.to_string()),
            }
        };
        let switch = |s: &Option<Setting>, def: bool, what: &str| -> Result<bool> {
            match s {
                None => Ok(def),
                Some(v) => parse_switch(&v.text()).with_context(|| what.to_string()),
            }
        };
        let base = d.failure_rate.unwrap_or_default();
        let failure_rate = percent(
            &self.failure_rate,
            Some(base.percent),
            "alerts.failure_rate",
        )?
        .map(|p| -> Result<FailureRate> {
            Ok(FailureRate {
                percent: p,
                window_secs: secs(
                    &self.failure_window,
                    Some(base.window_secs),
                    "alerts.failure_window",
                )?
                .unwrap_or(base.window_secs),
                min_runs: self.failure_min_runs.unwrap_or(base.min_runs),
            })
        })
        .transpose()?;
        let temp_c = match &self.temperature {
            None | Some(Setting::Bool(true)) => d.temp_c,
            Some(v) => {
                let t = v.text();
                if crate::monitor::is_off(&t) {
                    None
                } else {
                    Some(
                        t.trim()
                            .trim_end_matches('C')
                            .trim_end_matches('°')
                            .trim()
                            .parse::<u16>()
                            .context("alerts.temperature: degrees Celsius")?,
                    )
                }
            }
        };
        let p = AlertPolicy {
            node_offline_secs: secs(
                &self.node_offline,
                d.node_offline_secs,
                "alerts.node_offline",
            )?,
            failure_rate,
            disk_percent: percent(&self.disk, d.disk_percent, "alerts.disk")?,
            temp_c,
            perf_drop_percent: percent(&self.perf_drop, d.perf_drop_percent, "alerts.perf_drop")?,
            smart: switch(&self.smart, d.smart, "alerts.smart")?,
            check_mismatch: switch(
                &self.check_mismatch,
                d.check_mismatch,
                "alerts.check_mismatch",
            )?,
        };
        p.validate()?;
        Ok(p)
    }
}

/// `[webhooks.NAME]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookFile {
    pub url: Option<String>,
    /// An environment variable holding the URL, read where `apply` runs,
    /// so the file need not hold the secret.
    pub url_env: Option<String>,
    /// `slack`, `discord` or `json` (default: guessed from the URL).
    pub format: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFile {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub taints: Vec<String>,
    pub cordoned: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountFile {
    pub slots: Option<u32>,
    pub gpus: Option<u32>,
    pub cu_per_day: Option<NumberOrText>,
    pub max_priority: Option<i32>,
    pub storage: Option<String>,
    // Phase 15: the account's job policy.
    #[serde(default)]
    pub images: Vec<String>,
    #[serde(default)]
    pub require_digest: bool,
    #[serde(default)]
    pub executors: Vec<Executor>,
    /// `"deny"` refuses jobs with `network = true`; `"allow"` (default).
    pub network: Option<String>,
    /// Added to every job's selector: `"gpu=nvidia,os=windows"`.
    pub selector: Option<String>,
    /// Tolerations its jobs may carry; `[]` for none.
    pub tolerations: Option<Vec<String>>,
    pub max_timeout: Option<String>,
    pub max_gpus_per_task: Option<u32>,
}

/// `allow` or `deny` (`--network`, `network =` in cluster.toml): whether
/// a policy refuses jobs that ask for the network.
pub fn parse_network_rule(s: &str) -> Result<bool> {
    match s.trim() {
        "deny" => Ok(true),
        "allow" => Ok(false),
        other => bail!("network is `allow` or `deny`, not {other:?}"),
    }
}

impl AccountFile {
    /// The `[accounts.NAME]` policy fields.
    pub fn policy(&self) -> Result<AccountPolicy> {
        let mut executors = self.executors.clone();
        executors.sort_by_key(|e| e.to_string());
        executors.dedup();
        let p = AccountPolicy {
            images: self.images.clone(),
            require_digest: self.require_digest,
            executors,
            deny_network: self
                .network
                .as_deref()
                .map(parse_network_rule)
                .transpose()?
                .unwrap_or(false),
            selector: match &self.selector {
                Some(s) => s.parse()?,
                None => Selector::default(),
            },
            tolerations: self
                .tolerations
                .as_ref()
                .map(|v| v.iter().map(|t| t.parse::<Toleration>()).collect())
                .transpose()?,
            max_timeout_ms: self
                .max_timeout
                .as_deref()
                .map(crate::job::parse_duration_ms)
                .transpose()
                .context("max_timeout")?,
            max_gpus_per_task: self.max_gpus_per_task,
        };
        p.validate()?;
        Ok(p)
    }
}

/// `100`, `12.5` or `"12.5"`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum NumberOrText {
    Int(i64),
    Float(f64),
    Text(String),
}

impl NumberOrText {
    fn text(&self) -> String {
        match self {
            NumberOrText::Int(i) => i.to_string(),
            NumberOrText::Float(f) => f.to_string(),
            NumberOrText::Text(t) => t.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleFile {
    pub cron: String,
    /// A job.toml, relative to cluster.toml.
    pub job: PathBuf,
    pub account: Option<String>,
    #[serde(default)]
    pub overlap: bool,
}

impl ClusterFile {
    pub fn parse(text: &str) -> Result<Self> {
        toml::from_str(text).context("invalid cluster file")
    }

    /// Reads the file and the job files it names. Scheduled jobs carry
    /// their input files with them, so those must fit in `inline_limit`
    /// bytes (larger inputs belong in the file store: `store:NAME`).
    pub fn load(path: &Path, inline_limit: u64) -> Result<ApplySpec> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let file = Self::parse(&text)?;
        let base = path.parent().unwrap_or(Path::new("."));
        file.resolve(base, inline_limit)
    }

    pub fn resolve(self, base: &Path, inline_limit: u64) -> Result<ApplySpec> {
        let mut spec = ApplySpec::default();
        for (node, n) in self.nodes {
            let mut taints = n
                .taints
                .iter()
                .map(|t| t.parse::<Taint>())
                .collect::<Result<Vec<_>>>()
                .with_context(|| format!("node {node}"))?;
            taints.sort();
            spec.nodes.push(NodeSpec {
                node,
                labels: n.labels,
                taints,
                cordoned: n.cordoned,
            });
        }
        for (account, a) in self.accounts {
            let policy = a
                .policy()
                .with_context(|| format!("account {account}: policy"))?;
            let q = AccountQuota {
                slots: a.slots,
                gpus: a.gpus,
                cu_per_day: a
                    .cu_per_day
                    .map(|c| parse_cu(&c.text()))
                    .transpose()
                    .with_context(|| format!("account {account}: cu_per_day"))?,
                max_priority: a.max_priority,
                storage_bytes: a
                    .storage
                    .as_deref()
                    .map(parse_size)
                    .transpose()
                    .with_context(|| format!("account {account}: storage"))?,
            };
            spec.policies.push((account.clone(), policy));
            spec.accounts.push((account, q));
        }
        for (name, s) in self.schedules {
            let job_path = base.join(&s.job);
            let (mut job, upload) = JobFile::load_split(&job_path, inline_limit)
                .with_context(|| format!("schedule {name}: {}", job_path.display()))?;
            ensure!(
                upload.is_empty(),
                "schedule {name}: the job's input files are too large to keep with a schedule; put them in the file store (`pumasi store put`) and mount them with `store:NAME`"
            );
            if let Some(a) = &s.account {
                validate_account(a)?;
            }
            job.account = s.account;
            spec.schedules.push(ScheduleSpec {
                name,
                cron: s.cron,
                job,
                overlap: s.overlap,
            });
        }
        if let Some(a) = &self.alerts {
            spec.alerts = Some(a.policy()?);
        }
        for (name, w) in self.webhooks {
            let url = match (w.url, w.url_env) {
                (Some(u), None) => u,
                (None, Some(var)) => std::env::var(&var).with_context(|| {
                    format!("webhook {name}: environment variable {var} is not set")
                })?,
                _ => bail!("webhook {name}: give url or url_env (one of them)"),
            };
            let format = match &w.format {
                Some(f) => f.parse().with_context(|| format!("webhook {name}"))?,
                None => WebhookFormat::detect(&url),
            };
            spec.webhooks.push(WebhookSpec { name, url, format });
        }
        spec.validate()?;
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, Utc};

    #[test]
    fn policy_patch_changes_only_what_it_names() {
        let strs = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let mut p = AccountPolicy {
            images: strs(&["python:3.12-slim", "rust:1-slim"]),
            executors: vec![Executor::Docker],
            deny_network: true,
            max_timeout_ms: Some(60_000),
            ..Default::default()
        };
        let before = p.clone();

        // Nothing named: nothing changes.
        PolicyPatch::default().apply(&mut p).unwrap();
        assert_eq!(p, before);

        // Add and remove entries; the rest stays.
        let patch = PolicyPatch {
            images: ListPatch {
                add: strs(&["node:22-slim", "rust:1-slim"]),
                remove: strs(&["python:3.12-slim", "not-there"]),
                ..Default::default()
            },
            executors: ListPatch {
                add: strs(&["microvm"]),
                ..Default::default()
            },
            tolerations: ListPatch {
                add: strs(&["spot", "owner=alice"]),
                ..Default::default()
            },
            ..Default::default()
        };
        patch.apply(&mut p).unwrap();
        assert_eq!(p.images, strs(&["rust:1-slim", "node:22-slim"]));
        assert_eq!(p.executors, vec![Executor::Docker, Executor::Microvm]);
        assert_eq!(
            p.tolerations,
            Some(vec![
                "spot".parse().unwrap(),
                "owner=alice".parse().unwrap()
            ])
        );
        assert!(p.deny_network);
        assert_eq!(p.max_timeout_ms, Some(60_000));
        // The same change again changes nothing.
        let once = p.clone();
        patch.apply(&mut p).unwrap();
        assert_eq!(p, once);

        // Removing the last image would allow every image: refused, and
        // the policy is not touched by a caller that checks the result.
        let mut q = p.clone();
        let err = PolicyPatch {
            images: ListPatch {
                remove: strs(&["rust:1-slim", "node:22-slim"]),
                ..Default::default()
            },
            ..Default::default()
        }
        .apply(&mut q)
        .unwrap_err();
        assert!(err.to_string().contains("would allow any"), "{err}");
        // A whole new list may be empty.
        PolicyPatch {
            images: ListPatch {
                set: Some(Vec::new()),
                ..Default::default()
            },
            ..Default::default()
        }
        .apply(&mut p)
        .unwrap();
        assert!(p.images.is_empty());

        // Tolerations: removing down to none is fine (none is allowed
        // then), lifting the limit is its own switch.
        PolicyPatch {
            tolerations: ListPatch {
                remove: strs(&["spot", "owner=alice"]),
                ..Default::default()
            },
            ..Default::default()
        }
        .apply(&mut p)
        .unwrap();
        assert_eq!(p.tolerations, Some(Vec::new()));
        PolicyPatch {
            any_toleration: true,
            ..Default::default()
        }
        .apply(&mut p)
        .unwrap();
        assert_eq!(p.tolerations, None);
        assert!(
            PolicyPatch {
                tolerations: ListPatch {
                    remove: strs(&["spot"]),
                    ..Default::default()
                },
                ..Default::default()
            }
            .apply(&mut p)
            .is_err()
        );

        // Limits: set, and lifted with an explicit nothing.
        PolicyPatch {
            max_timeout_ms: Some(None),
            max_gpus_per_task: Some(Some(2)),
            deny_network: Some(false),
            selector: Some("gpu=nvidia".parse().unwrap()),
            ..Default::default()
        }
        .apply(&mut p)
        .unwrap();
        assert_eq!(p.max_timeout_ms, None);
        assert_eq!(p.max_gpus_per_task, Some(2));
        assert!(!p.deny_network);
        assert_eq!(p.selector.to_string(), "gpu=nvidia");

        // A list and edits together, and bad entries, are refused.
        assert!(
            PolicyPatch {
                images: ListPatch {
                    set: Some(strs(&["a"])),
                    add: strs(&["b"]),
                    ..Default::default()
                },
                ..Default::default()
            }
            .apply(&mut p)
            .is_err()
        );
        assert!(
            PolicyPatch {
                executors: ListPatch {
                    add: strs(&["podman"]),
                    ..Default::default()
                },
                ..Default::default()
            }
            .apply(&mut p)
            .is_err()
        );
    }

    #[test]
    fn image_names_and_patterns() {
        assert_eq!(
            full_image_name("python:3.12"),
            "docker.io/library/python:3.12"
        );
        assert_eq!(full_image_name("me/app"), "docker.io/me/app:latest");
        assert_eq!(
            full_image_name("localhost:5000/x@sha256:ab"),
            "localhost:5000/x@sha256:ab"
        );
        assert_eq!(full_image_name("ghcr.io/a/b:1"), "ghcr.io/a/b:1");
        assert!(glob_match("ghcr.io/lab/*", "ghcr.io/lab/blender:4.2"));
        assert!(glob_match("a?c", "abc") && !glob_match("a?c", "ac"));
        assert!(glob_match("*", "") && !glob_match("x*", "y"));
        let p = AccountPolicy {
            images: vec!["ghcr.io/lab/*".into(), "python:3.12-slim".into()],
            ..Default::default()
        };
        assert!(p.allows_image("ghcr.io/lab/render:1"));
        assert!(p.allows_image("python:3.12-slim"));
        assert!(p.allows_image("docker.io/library/python:3.12-slim"));
        assert!(!p.allows_image("python:3.13"));
        assert!(!p.allows_image("ghcr.io/other/render:1"));
        let pinned = AccountPolicy {
            require_digest: true,
            ..Default::default()
        };
        assert!(!pinned.allows_image("python:3.12"));
        assert!(pinned.allows_image("python@sha256:0123"));
    }

    #[test]
    fn policies_refuse_and_add_selectors() {
        let dir = std::env::temp_dir();
        let job = |text: &str| {
            crate::job::JobFile::parse(text)
                .unwrap()
                .resolve(&dir)
                .unwrap()
        };
        let p = AccountPolicy {
            images: vec!["ghcr.io/lab/*".into()],
            executors: vec![Executor::Docker],
            deny_network: true,
            selector: "gpu=nvidia".parse().unwrap(),
            tolerations: Some(vec![]),
            max_timeout_ms: Some(600_000),
            max_gpus_per_task: Some(1),
            ..Default::default()
        };
        p.validate().unwrap();
        let mut ok = job("image = \"ghcr.io/lab/a:1\"\ncommand = [\"true\"]");
        p.admit(&mut ok).unwrap();
        assert_eq!(ok.selector.to_string(), "gpu=nvidia");
        for (text, why) in [
            ("image = \"ghcr.io/x/a\"", "does not allow the image"),
            ("image = \"ghcr.io/lab/a\"\nnetwork = true", "network"),
            ("image = \"ghcr.io/lab/a\"\ngpus = 2", "above the 1"),
            (
                "image = \"ghcr.io/lab/a\"\ngpus = \"all\"",
                "gpus = \"all\"",
            ),
            (
                "image = \"ghcr.io/lab/a\"\n[resources]\ntimeout = \"1h\"",
                "above the 10m",
            ),
            (
                "image = \"ghcr.io/lab/a\"\ntolerations = [\"spot\"]",
                "toleration",
            ),
        ] {
            let mut sub = job(text);
            let e = p.admit(&mut sub).unwrap_err();
            assert!(format!("{e:#}").contains(why), "{text}: {e:#}");
        }
        assert!(AccountPolicy::default().is_empty());
        assert!(
            AccountPolicy {
                images: vec!["bad name".into()],
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    fn labels(pairs: &[(&str, &[&str])]) -> LabelSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn selectors_match_labels() {
        let node = labels(&[
            ("os", &["linux"]),
            ("disk", &["nvme"]),
            ("gpu", &["nvidia", "amd"]),
            ("zone", &["home"]),
        ]);
        let ok = |s: &str| s.parse::<Selector>().unwrap().matches(&node);
        assert!(ok(""));
        assert!(ok("os=linux"));
        assert!(ok("os=linux,disk=nvme|ssd"));
        assert!(!ok("os=macos"));
        assert!(ok("gpu=amd"), "any of several values");
        assert!(!ok("gpu!=amd"));
        assert!(ok("arch!=arm64"), "a missing label is not that value");
        assert!(ok("zone"));
        assert!(!ok("!zone"));
        assert!(ok("!spot"));
        assert!(!ok("spot"));
        let s: Selector = "os=linux,disk=nvme|ssd,gpu!=amd,zone,!spot"
            .parse()
            .unwrap();
        assert_eq!(s.to_string(), "os=linux,disk=nvme|ssd,gpu!=amd,zone,!spot");
        assert!("Bad=key".parse::<Selector>().is_err());
        assert!("os=".parse::<Selector>().is_err());

        let table: BTreeMap<String, SelectorValue> = toml::from_str(
            r#"
            os = "linux"
            disk = ["nvme", "ssd"]
            gpu = "!intel"
            zone = true
            spot = false
            "#,
        )
        .unwrap();
        let s = Selector::from_table(&table).unwrap();
        assert!(s.matches(&node));
        assert!(!s.matches(&labels(&[("os", &["linux"]), ("disk", &["hdd"])])));
    }

    #[test]
    fn taints_need_tolerations() {
        let taints: Vec<Taint> = vec!["gpu-only".parse().unwrap(), "owner=alice".parse().unwrap()];
        let tol = |list: &[&str]| -> Vec<Toleration> {
            list.iter().map(|s| s.parse().unwrap()).collect()
        };
        assert!(tolerated(&[], &[]));
        assert!(!tolerated(&taints, &[]));
        assert!(!tolerated(&taints, &tol(&["gpu-only"])));
        assert!(tolerated(&taints, &tol(&["gpu-only", "owner"])));
        assert!(tolerated(&taints, &tol(&["gpu-only", "owner=alice"])));
        assert!(!tolerated(&taints, &tol(&["gpu-only", "owner=bob"])));
        assert!(tolerated(&taints, &tol(&["*"])));
        assert_eq!(taints[1].to_string(), "owner=alice");
    }

    #[test]
    fn node_edits() {
        let c = NodeConfig::default();
        let edit = NodeEdit {
            set_labels: [("zone".to_string(), "home".to_string())].into(),
            add_taints: vec!["gpu-only".parse().unwrap()],
            cordon: Some(true),
            ..Default::default()
        };
        let c = edit.apply(&c).unwrap();
        assert_eq!(c.labels["zone"], "home");
        assert!(c.cordoned);
        let c = NodeEdit {
            remove_labels: vec!["zone".into()],
            remove_taints: vec!["gpu-only".into()],
            cordon: Some(false),
            ..Default::default()
        }
        .apply(&c)
        .unwrap();
        assert!(c.is_empty());
        let bad = NodeEdit {
            set_labels: [("os".to_string(), "linux".to_string())].into(),
            ..Default::default()
        };
        assert!(bad.apply(&c).is_err(), "built-in labels are not settable");
    }

    fn at(s: &str) -> i64 {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M")
            .unwrap()
            .and_utc()
            .timestamp()
    }

    fn next(cron: &str, from: &str) -> String {
        let c: Cron = cron.parse().unwrap();
        let t = c.next_after(at(from), &Utc).unwrap();
        Utc.timestamp_opt(t, 0)
            .unwrap()
            .format("%Y-%m-%d %H:%M")
            .to_string()
    }

    #[test]
    fn cron_next_times() {
        assert_eq!(next("0 3 * * *", "2026-11-05 08:57"), "2026-11-06 03:00");
        assert_eq!(next("0 3 * * *", "2026-11-05 02:59"), "2026-11-05 03:00");
        assert_eq!(next("0 3 * * *", "2026-11-05 03:00"), "2026-11-06 03:00");
        assert_eq!(next("*/15 * * * *", "2026-11-05 08:57"), "2026-11-05 09:00");
        assert_eq!(next("@hourly", "2026-11-05 08:57"), "2026-11-05 09:00");
        // Thursday 2026-11-05: next Monday.
        assert_eq!(next("30 9 * * mon", "2026-11-05 08:57"), "2026-11-09 09:30");
        assert_eq!(next("0 0 * * 7", "2026-11-05 08:57"), "2026-11-08 00:00");
        assert_eq!(next("0 0 1 * *", "2026-11-05 08:57"), "2026-12-01 00:00");
        assert_eq!(next("0 0 29 2 *", "2026-11-05 08:57"), "2028-02-29 00:00");
        assert_eq!(
            next("0 9-17/4 * * 1-5", "2026-11-06 18:00"),
            "2026-11-09 09:00"
        );
        // Day of month or weekday when both are given.
        assert_eq!(next("0 0 13 * fri", "2026-11-05 08:57"), "2026-11-06 00:00");
        assert_eq!(next("0 0 1 jan *", "2026-11-05 08:57"), "2027-01-01 00:00");
        for bad in [
            "",
            "* * * *",
            "60 * * * *",
            "* 24 * * *",
            "@often",
            "*/0 * * * *",
        ] {
            assert!(bad.parse::<Cron>().is_err(), "{bad:?}");
        }
        // In a fixed time zone (Seoul): 03:00 there is 18:00 UTC the day before.
        let kst = FixedOffset::east_opt(9 * 3600).unwrap();
        let c: Cron = "0 3 * * *".parse().unwrap();
        let t = c.next_after(at("2026-11-05 08:57"), &kst).unwrap();
        assert_eq!(t, at("2026-11-05 18:00"));
    }

    #[test]
    fn example_cluster_file_loads() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/cluster.toml");
        let spec = ClusterFile::load(&path, 16 << 20).unwrap();
        spec.validate().unwrap();
        assert_eq!(spec.nodes.len(), 2);
        assert_eq!(spec.accounts.len(), 2);
        assert_eq!(spec.schedules[0].job.tasks.len(), 4);
        assert_eq!(spec.alerts, Some(AlertPolicy::default()));
    }

    #[test]
    fn cluster_file_resolves() {
        let dir =
            std::env::temp_dir().join(format!("pumasi-cluster-file-{}", rand::random::<u64>()));
        std::fs::create_dir_all(dir.join("nightly")).unwrap();
        std::fs::write(dir.join("nightly/m.wasm"), b"\0asm\x01\0\0\0").unwrap();
        std::fs::write(
            dir.join("nightly/job.toml"),
            "module = \"m.wasm\"\nshards = 2\npriority = 5\n[selector]\nos = \"linux\"\n",
        )
        .unwrap();
        let text = r#"
            [nodes.gpu-box]
            labels = { zone = "home" }
            taints = ["gpu-only", "a=b"]
            cordoned = true

            [accounts.alice]
            slots = 4
            cu_per_day = 12.5
            storage = "1G"

            [schedules.nightly]
            cron = "0 3 * * *"
            job = "nightly/job.toml"
            account = "alice"
        "#;
        std::fs::write(dir.join("cluster.toml"), text).unwrap();
        let spec = ClusterFile::load(&dir.join("cluster.toml"), 1 << 20).unwrap();
        assert_eq!(spec.nodes[0].node, "gpu-box");
        assert_eq!(spec.nodes[0].taints[0].to_string(), "a=b", "sorted");
        assert_eq!(spec.nodes[0].cordoned, Some(true));
        let (a, q) = &spec.accounts[0];
        assert_eq!(a, "alice");
        assert_eq!(q.slots, Some(4));
        assert_eq!(q.cu_per_day, Some(12_500));
        assert_eq!(q.storage_bytes, Some(1 << 30));
        let s = &spec.schedules[0];
        assert_eq!(s.job.account.as_deref(), Some("alice"));
        assert_eq!(s.job.tasks.len(), 2);
        assert_eq!(s.job.priority, 5);
        assert!(!s.job.selector.is_empty());
        assert!(ClusterFile::parse("[nodes.x]\nlables = {}\n").is_err());
    }
}
