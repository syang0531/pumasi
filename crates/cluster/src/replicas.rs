//! Copies of dataset versions on several machines (1.8), and the control
//! plane's side of the S3 gateway (keys, signatures, object records).
//!
//! The leader keeps every dataset version in its own store, as before.
//! On top of that each version is wanted on `factor` copies (the
//! dataset's own number, else the cluster's default, 2), and every control
//! plane replica counts as one: with three control planes the default is
//! met without any node. The rest are placed on owner nodes labelled
//! [`STORAGE_LABEL`], each within its quota (the label's value), spread
//! over the nodes with the most room. A node keeps its replicas in its own
//! store, tagged so the cache's eviction leaves them alone, and serves
//! them like its cache.
//!
//! The planner ([`State::tick_replicas`]) runs from the tick: it places
//! what is missing, drops replicas of removed versions or nodes and extras
//! (a node that came back after its copy was replaced elsewhere), and
//! sends each 1.8 node the full list it should hold
//! ([`ToAgent::Hold`]) whenever that list changes. A node's answer
//! ([`FromAgent::Held`]) says which ones it has complete. A replica on a
//! node that has been offline for [`RepState::repair_after`] no longer
//! counts, so the planner places another one; removing the node drops it
//! at once.
//!
//! Replicas are also how the leader gets data back that it lost: the
//! repair worker in `server.rs` checks the leader's store for every
//! version and fetches what is missing from the nodes that hold it, so a
//! control plane started with an empty `blobs` directory (a lost disk, a
//! move to another PC) refills itself. Scrubs read every collection back
//! against its BLAKE3 hashes, on the leader and (when asked by
//! [`ToAgent::Scrub`]) on nodes; a node drops a damaged replica and
//! fetches it again.
//!
//! [`FromAgent::Held`]: pumasi_proto::msg::FromAgent::Held
//! [`ToAgent::Scrub`]: pumasi_proto::msg::ToAgent::Scrub

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use iroh::EndpointAddr;
use pumasi_proto::{
    REPLICA_MINOR, Secret,
    manage::AccessRole,
    monitor::EventLevel,
    msg::ToAgent,
    replica::{
        DEFAULT_REPLICAS, DatasetReplicas, MAX_S3_META_BYTES, ReplicaState, ReplicaView,
        ReplicationView, S3KeyView, S3Meta, STORAGE_LABEL, ScrubReport, StorageNodeView,
        storage_quota, validate_factor,
    },
    s3, secrets_eq,
    store::{BlobHash, hash_hex},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    manage::Caller,
    state::{State, now_unix},
};

/// Default time a storage node may stay offline before its replicas are
/// placed elsewhere.
pub const DEFAULT_REPAIR_AFTER: Duration = Duration::from_secs(6 * 3600);
/// Default time between scrubs of a store.
pub const DEFAULT_SCRUB_EVERY: Duration = Duration::from_secs(7 * 86_400);
/// The planner runs at most this often.
const PLAN_EVERY: Duration = Duration::from_millis(500);
/// Most other nodes named as places to fetch replicas from.
const MAX_SOURCES: usize = 8;
/// What a node keeps free of its disk when its label sets no quota.
const FREE_SHARE: u64 = 10;
/// What a node with a quota keeps free of its disk at least.
const MIN_FREE: u64 = 1 << 30;
/// Meta keys.
const DEFAULT_FACTOR_META: &str = "replication_default";
const SCRUBS_META: &str = "replica_scrubs";
/// Most S3 keys per ticket.
const MAX_S3_KEYS: usize = 32;
/// How long an S3 signature's credential lasts.
const S3_TOKEN_TTL: Duration = Duration::from_secs(300);

/// One node's copy of a dataset version, as stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRecord {
    pub size: u64,
    pub state: ReplicaState,
    pub since_unix: u64,
}

/// An S3 access key, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3KeyRecord {
    pub id: String,
    /// The secret access key the client signs with.
    pub secret: String,
    /// The secret of the ticket it acts as.
    pub parent: Secret,
    pub label: String,
    pub created_unix: u64,
    pub last_used_unix: Option<u64>,
}

/// The replicas part of [`State`].
pub(crate) struct RepState {
    pub default_factor: u8,
    pub factors: BTreeMap<String, u8>,
    pub records: BTreeMap<(BlobHash, String), ReplicaRecord>,
    /// The list each connected node was last sent.
    sent: HashMap<String, Vec<BlobHash>>,
    /// Why a node's last download of a replica stopped.
    errors: HashMap<(BlobHash, String), String>,
    /// Each node's last scrub: when, and collections found damaged.
    scrubs: BTreeMap<String, (u64, u32)>,
    /// Scrubs asked for and not answered yet.
    scrubbing: HashMap<String, Instant>,
    /// The leader's own last scrub.
    pub leader_scrub: Option<(u64, u32)>,
    /// Versions the leader lacks (from the repair worker).
    pub leader_missing: HashSet<BlobHash>,
    /// Control plane replicas: each one is a copy.
    pub control_planes: u8,
    last_plan: Option<Instant>,
    pub repair_after: Duration,
    pub scrub_every: Duration,
    pub s3_keys: BTreeMap<String, S3KeyRecord>,
    /// Credentials handed out for checked S3 signatures: key id and expiry.
    s3_tokens: HashMap<Secret, (String, Instant)>,
}

impl Default for RepState {
    fn default() -> Self {
        Self {
            default_factor: DEFAULT_REPLICAS,
            factors: BTreeMap::new(),
            records: BTreeMap::new(),
            sent: HashMap::new(),
            errors: HashMap::new(),
            scrubs: BTreeMap::new(),
            scrubbing: HashMap::new(),
            leader_scrub: None,
            leader_missing: HashSet::new(),
            control_planes: 1,
            last_plan: None,
            repair_after: DEFAULT_REPAIR_AFTER,
            scrub_every: DEFAULT_SCRUB_EVERY,
            s3_keys: BTreeMap::new(),
            s3_tokens: HashMap::new(),
        }
    }
}

/// A node as the planner sees it.
struct Candidate {
    id: String,
    online: bool,
    /// May get new replicas now.
    usable: bool,
    /// Its replicas count (labelled, a member, not offline too long).
    live: bool,
    quota: Option<u64>,
    room: u64,
}

impl State {
    pub(crate) fn load_replicas(&mut self) -> Result<()> {
        if let Some(b) = self.store.get_meta(DEFAULT_FACTOR_META)?
            && let Some(f) = b.first()
        {
            self.reps.default_factor = *f;
        }
        self.reps.factors = self.store.ds_replication()?.into_iter().collect();
        self.reps.records = self
            .store
            .replicas()?
            .into_iter()
            .map(|(root, node, r)| ((root, node), r))
            .collect();
        if let Some(b) = self.store.get_meta(SCRUBS_META)? {
            let v: Vec<(String, u64, u32)> = postcard::from_bytes(&b).unwrap_or_default();
            self.reps.scrubs = v.into_iter().map(|(n, t, b)| (n, (t, b))).collect();
            // The leader's own scrub is kept under the empty name.
            self.reps.leader_scrub = self.reps.scrubs.remove("");
        }
        self.reps.s3_keys = self
            .store
            .s3_keys()?
            .into_iter()
            .map(|k| (k.id.clone(), k))
            .collect();
        Ok(())
    }

    /// Settings of this run of the control plane.
    pub fn set_replica_timing(&mut self, repair_after: Duration, scrub_every: Duration) {
        self.reps.repair_after = repair_after;
        self.reps.scrub_every = scrub_every;
    }

    pub(crate) fn set_control_planes(&mut self, n: u8) {
        self.reps.control_planes = n.max(1);
    }

    /// The copies a dataset wants.
    fn factor_of(&self, name: &str) -> u8 {
        self.reps
            .factors
            .get(name)
            .copied()
            .unwrap_or(self.reps.default_factor)
    }

    /// Every dataset version's collection, with the copies it wants (the
    /// most any version with that collection wants) and its size.
    fn wanted_roots(&self) -> BTreeMap<BlobHash, (u8, u64)> {
        let mut out: BTreeMap<BlobHash, (u8, u64)> = BTreeMap::new();
        for d in self.files.datasets.values() {
            let f = self.factor_of(&d.name);
            let e = out.entry(d.root).or_insert((f, d.size));
            e.0 = e.0.max(f);
        }
        out
    }

    /// How long a node has been offline, or 0.
    fn offline_secs(&self, id: &str, now: u64) -> u64 {
        match self.nodes.get(id) {
            Some(n) if n.online => 0,
            Some(n) => now.saturating_sub(n.last_seen_unix),
            None => now.saturating_sub(self.monitor.started_unix),
        }
    }

    /// The storage label's quota of a node: `None` when it has no (valid)
    /// label, `Some(None)` for "up to 90% of the disk".
    fn storage_label(&self, id: &str) -> Option<Option<u64>> {
        let v = self.node_config(id)?.labels.get(STORAGE_LABEL)?;
        storage_quota(v).ok()
    }

    fn used_on(&self, id: &str) -> (u64, u32) {
        self.reps
            .records
            .iter()
            .filter(|((_, n), _)| n == id)
            .fold((0, 0), |(b, c), (_, r)| (b + r.size, c + 1))
    }

    /// Why a labelled node gets no new replicas, if it does not.
    fn not_used(&self, id: &str) -> Option<String> {
        let Some(m) = self.members.get(id) else {
            return Some("not a member".into());
        };
        if !m.trusted() {
            return Some("replicas go only to the owner's machines".into());
        }
        let Some(n) = self.nodes.get(id).filter(|n| n.online && n.tx.is_some()) else {
            return Some("offline".into());
        };
        if n.perf.protocol < pumasi_proto::STORE_PROTOCOL || self.node_minor(id) < REPLICA_MINOR {
            return Some("its agent is older than 1.8".into());
        }
        None
    }

    fn candidates(&self, now: u64) -> Vec<Candidate> {
        let repair = self.reps.repair_after.as_secs();
        let mut out = Vec::new();
        for id in self.members.keys() {
            let Some(quota) = self.storage_label(id) else {
                continue;
            };
            let online = self.nodes.get(id).is_some_and(|n| n.online);
            let usable = self.not_used(id).is_none();
            let live = self.members.get(id).is_some_and(|m| m.trusted())
                && (online || self.offline_secs(id, now) < repair);
            let (used, _) = self.used_on(id);
            // A quota is the owner's word, as long as the disk has room
            // (keeping a little free); without one, a tenth of the disk
            // stays free.
            let keep_free = |total: u64| match quota {
                Some(_) => MIN_FREE.min(total / FREE_SHARE),
                None => total / FREE_SHARE,
            };
            let disk_room = self.latest_disk(id).map(|(total, used_disk)| {
                (total.saturating_sub(used_disk)).saturating_sub(keep_free(total))
            });
            let room = match (quota, disk_room) {
                (Some(q), Some(d)) => q.saturating_sub(used).min(d),
                (Some(q), None) => q.saturating_sub(used),
                (None, Some(d)) => d,
                (None, None) => u64::MAX / 2,
            };
            out.push(Candidate {
                id: id.clone(),
                online,
                usable,
                live,
                quota,
                room,
            });
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Places, drops and announces replicas. From the tick.
    pub(crate) fn tick_replicas(&mut self) {
        if self
            .reps
            .last_plan
            .is_some_and(|t| t.elapsed() < PLAN_EVERY)
        {
            return;
        }
        self.reps.last_plan = Some(Instant::now());
        self.expire_s3_tokens();
        let now = now_unix();
        let wanted = self.wanted_roots();
        let mut cands = self.candidates(now);
        let by_id: HashMap<String, usize> = cands
            .iter()
            .enumerate()
            .map(|(i, c)| (c.id.clone(), i))
            .collect();
        let mut put: Vec<(BlobHash, String, ReplicaRecord)> = Vec::new();
        let mut gone: Vec<(BlobHash, String)> = Vec::new();
        // Replicas of collections no version uses, and of nodes that are no
        // longer storage nodes.
        for (root, node) in self.reps.records.keys() {
            if !wanted.contains_key(root) || !by_id.contains_key(node) {
                gone.push((*root, node.clone()));
            }
        }
        for k in &gone {
            self.reps.records.remove(k);
        }
        let cps = self.reps.control_planes;
        for (root, (factor, size)) in &wanted {
            let need = factor.saturating_sub(cps) as usize;
            let mut live: Vec<(String, ReplicaRecord, bool)> = self
                .reps
                .records
                .range((*root, String::new())..)
                .take_while(|((r, _), _)| r == root)
                .filter_map(|((_, n), rec)| {
                    let c = &cands[by_id[n]];
                    c.live.then(|| (n.clone(), *rec, c.online))
                })
                .collect();
            if live.len() < need {
                let have: HashSet<&str> = self
                    .reps
                    .records
                    .range((*root, String::new())..)
                    .take_while(|((r, _), _)| r == root)
                    .map(|((_, n), _)| n.as_str())
                    .collect();
                let mut options: Vec<usize> = cands
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.usable && c.room >= *size && !have.contains(c.id.as_str()))
                    .map(|(i, _)| i)
                    .collect();
                // The most room first.
                options.sort_by_key(|i| std::cmp::Reverse(cands[*i].room));
                for i in options.into_iter().take(need - live.len()) {
                    let rec = ReplicaRecord {
                        size: *size,
                        state: ReplicaState::Copying,
                        since_unix: now,
                    };
                    cands[i].room -= size;
                    let node = cands[i].id.clone();
                    info!(dataset = %self.label_of(root), node = %self.node_name(&node), "placing a replica");
                    self.reps.records.insert((*root, node.clone()), rec);
                    put.push((*root, node.clone(), rec));
                    live.push((node, rec, true));
                }
            } else if live.len() > need {
                // Keep held copies on online nodes first, oldest first.
                live.sort_by_key(|(n, r, online)| {
                    (
                        r.state != ReplicaState::Held,
                        !*online,
                        r.since_unix,
                        n.clone(),
                    )
                });
                let kept_copying = live
                    .iter()
                    .take(need)
                    .any(|(_, r, _)| r.state != ReplicaState::Held);
                for (node, rec, _) in live.into_iter().skip(need) {
                    if kept_copying && rec.state == ReplicaState::Held {
                        continue;
                    }
                    self.reps.records.remove(&(*root, node.clone()));
                    gone.push((*root, node));
                }
            }
        }
        if let Err(e) = self.store.put_replicas(&put, &gone) {
            warn!("persist replicas: {e:#}");
        }
        for k in &gone {
            self.reps.errors.remove(k);
        }
        self.announce_replicas();
        self.ask_scrubs(now);
    }

    /// `name@version` of a collection, for logs.
    fn label_of(&self, root: &BlobHash) -> String {
        self.files
            .datasets
            .values()
            .find(|d| d.root == *root)
            .map(|d| format!("{}@{}", d.name, d.version))
            .unwrap_or_else(|| hash_hex(root)[..12].to_string())
    }

    /// Sends each connected 1.8 node its list when it changed.
    fn announce_replicas(&mut self) {
        let mut lists: BTreeMap<&str, Vec<BlobHash>> = BTreeMap::new();
        for (root, node) in self.reps.records.keys() {
            lists.entry(node.as_str()).or_default().push(*root);
        }
        let mut sends: Vec<(String, Vec<BlobHash>, Vec<EndpointAddr>)> = Vec::new();
        for (id, n) in &self.nodes {
            if !n.online || n.tx.is_none() || self.node_minor(id) < REPLICA_MINOR {
                continue;
            }
            let list = lists.get(id.as_str()).cloned().unwrap_or_default();
            if self.reps.sent.get(id) == Some(&list) {
                continue;
            }
            // Other nodes that have any of them.
            let roots: HashSet<&BlobHash> = list.iter().collect();
            let mut sources: Vec<EndpointAddr> = Vec::new();
            let mut seen = HashSet::new();
            for ((root, other), rec) in &self.reps.records {
                if other == id
                    || rec.state != ReplicaState::Held
                    || !roots.contains(root)
                    || !seen.insert(other.clone())
                {
                    continue;
                }
                if let Some(addr) = self.files.caches.get(other).and_then(|c| c.addr.clone()) {
                    sources.push(addr);
                }
                if sources.len() >= MAX_SOURCES {
                    break;
                }
            }
            sends.push((id.clone(), list, sources));
        }
        for (id, roots, sources) in sends {
            let sent = self
                .nodes
                .get(&id)
                .and_then(|n| n.tx.as_ref())
                .is_some_and(|tx| {
                    tx.send(ToAgent::Hold {
                        roots: roots.clone(),
                        sources,
                    })
                    .is_ok()
                });
            if sent {
                self.reps.sent.insert(id, roots);
            }
        }
    }

    /// A node's session ended: it gets its list again when it is back.
    pub(crate) fn forget_replicas_sent(&mut self, id: &str) {
        self.reps.sent.remove(id);
        self.reps.scrubbing.remove(id);
    }

    /// Asks storage nodes whose last scrub is older than `scrub_every` for
    /// another, one node at a time.
    fn ask_scrubs(&mut self, now: u64) {
        self.reps
            .scrubbing
            .retain(|_, t| t.elapsed() < Duration::from_secs(6 * 3600));
        if !self.reps.scrubbing.is_empty() {
            return;
        }
        let every = self.reps.scrub_every.as_secs();
        let due = self
            .reps
            .sent
            .iter()
            .filter(|(_, list)| !list.is_empty())
            .map(|(id, _)| id.clone())
            .find(|id| {
                let last = self.reps.scrubs.get(id).map_or_else(
                    // Never scrubbed: count from when its first replica
                    // was placed.
                    || {
                        self.reps
                            .records
                            .iter()
                            .filter(|((_, n), _)| n == id)
                            .map(|(_, r)| r.since_unix)
                            .min()
                            .unwrap_or(now)
                    },
                    |(t, _)| *t,
                );
                now.saturating_sub(last) >= every
            });
        if let Some(id) = due {
            self.scrub_node(&id);
        }
    }

    fn scrub_node(&mut self, id: &str) -> bool {
        let sent = self
            .nodes
            .get(id)
            .filter(|_| self.node_minor(id) >= REPLICA_MINOR)
            .and_then(|n| n.tx.as_ref())
            .is_some_and(|tx| tx.send(ToAgent::Scrub).is_ok());
        if sent {
            self.reps.scrubbing.insert(id.to_string(), Instant::now());
        }
        sent
    }

    /// `pumasi store scrub [NODE]`: one node, or every storage node (the
    /// leader's own store is the caller's to check).
    pub fn scrub_now(&mut self, node: Option<&str>) -> Result<u32> {
        let ids: Vec<String> = match node {
            Some(n) => vec![self.resolve_node(n)?],
            None => self.reps.sent.keys().cloned().collect(),
        };
        let mut asked = 0;
        for id in ids {
            if self.scrub_node(&id) {
                asked += 1;
            } else if node.is_some() {
                bail!("node {} is offline or older than 1.8", self.node_name(&id));
            }
        }
        Ok(asked)
    }

    /// A node said which replicas it has complete.
    pub(crate) fn on_held(
        &mut self,
        node: &str,
        generation: u64,
        roots: Vec<BlobHash>,
        failed: Vec<(BlobHash, String)>,
        scrub: Option<ScrubReport>,
    ) {
        if self.nodes.get(node).map(|n| n.generation) != Some(generation) {
            return;
        }
        let now = now_unix();
        let held: HashSet<BlobHash> = roots.into_iter().collect();
        let mut put = Vec::new();
        for ((root, n), rec) in self.reps.records.iter_mut() {
            if n != node {
                continue;
            }
            let state = if held.contains(root) {
                ReplicaState::Held
            } else {
                ReplicaState::Copying
            };
            if rec.state != state {
                rec.state = state;
                rec.since_unix = now;
                put.push((*root, n.clone(), *rec));
            }
        }
        for (root, _, _) in &put {
            self.reps.errors.remove(&(*root, node.to_string()));
        }
        for (root, why) in failed {
            self.reps.errors.insert((root, node.to_string()), why);
        }
        if let Err(e) = self.store.put_replicas(&put, &[]) {
            warn!("persist replicas: {e:#}");
        }
        if let Some(s) = scrub {
            self.reps.scrubbing.remove(node);
            self.reps
                .scrubs
                .insert(node.to_string(), (now, s.bad.len() as u32));
            self.save_scrubs();
            let name = self.node_name(node);
            if s.bad.is_empty() {
                self.event(
                    EventLevel::Info,
                    "store.scrub",
                    Some(node),
                    None,
                    format!(
                        "{name} checked {} replicas ({}): no damage",
                        s.checked,
                        pumasi_proto::store::format_size(s.bytes)
                    ),
                );
            } else {
                let what: Vec<String> = s.bad.iter().map(|r| self.label_of(r)).collect();
                self.event(
                    EventLevel::Warning,
                    "store.damaged",
                    Some(node),
                    None,
                    format!(
                        "{name} found damaged data in {}; fetching it again",
                        what.join(", ")
                    ),
                );
            }
        }
    }

    fn save_scrubs(&self) {
        let leader = String::new();
        let v: Vec<(&String, u64, u32)> = self
            .reps
            .scrubs
            .iter()
            .chain(self.reps.leader_scrub.as_ref().map(|s| (&leader, s)))
            .map(|(n, (t, b))| (n, *t, *b))
            .collect();
        if let Ok(b) = postcard::to_stdvec(&v)
            && let Err(e) = self.store.put_meta(SCRUBS_META, &b)
        {
            warn!("persist scrubs: {e:#}");
        }
    }

    /// The leader scrubbed its own store.
    pub(crate) fn on_leader_scrub(&mut self, report: ScrubReport) {
        self.reps.leader_scrub = Some((now_unix(), report.bad.len() as u32));
        self.save_scrubs();
        if report.bad.is_empty() {
            self.event(
                EventLevel::Info,
                "store.scrub",
                None,
                None,
                format!(
                    "the control plane checked {} collections ({}): no damage",
                    report.checked,
                    pumasi_proto::store::format_size(report.bytes)
                ),
            );
        } else {
            let what: Vec<String> = report.bad.iter().map(|r| self.label_of(r)).collect();
            self.event(
                EventLevel::Critical,
                "store.damaged",
                None,
                None,
                format!(
                    "the control plane's store has damaged data in {}; node replicas keep good copies",
                    what.join(", ")
                ),
            );
        }
    }

    /// Every dataset version's collection with the nodes that hold it, for
    /// the leader's repair worker.
    pub(crate) fn repair_sources(&self) -> Vec<(BlobHash, Vec<EndpointAddr>)> {
        self.wanted_roots()
            .into_keys()
            .map(|root| {
                let addrs = self
                    .reps
                    .records
                    .range((root, String::new())..)
                    .take_while(|((r, _), _)| *r == root)
                    .filter(|(_, rec)| rec.state == ReplicaState::Held)
                    .filter_map(|((_, n), _)| self.files.caches.get(n)?.addr.clone())
                    .collect();
                (root, addrs)
            })
            .collect()
    }

    /// Copies of a collection now (control planes and counted node
    /// replicas), and the most it can have now (what it wants, or fewer
    /// when there are not enough storage nodes): a write is acknowledged
    /// once the first reaches the second.
    pub(crate) fn copies_of(&self, root: &BlobHash, control_planes: u8) -> (u8, u8) {
        let now = now_unix();
        let (factor, size) = self.wanted_roots().get(root).copied().unwrap_or((1, 0));
        let cands = self.candidates(now);
        let mut held = 0u8;
        let mut possible = 0u8;
        for c in &cands {
            let rec = self.reps.records.get(&(*root, c.id.clone()));
            match rec {
                Some(r) if c.live => {
                    possible = possible.saturating_add(1);
                    if r.state == ReplicaState::Held {
                        held = held.saturating_add(1);
                    }
                }
                None if c.usable && c.room >= size => possible = possible.saturating_add(1),
                _ => {}
            }
        }
        let need = factor.saturating_sub(self.reps.control_planes);
        let have = control_planes.saturating_add(held);
        let can = control_planes
            .saturating_add(need.min(possible))
            .min(factor);
        (have, can.max(1))
    }

    pub fn replication_view(&self, name: Option<&str>) -> Result<ReplicationView> {
        let now = now_unix();
        let cands = self.candidates(now);
        let live: HashMap<&str, &Candidate> = cands.iter().map(|c| (c.id.as_str(), c)).collect();
        let mut datasets = Vec::new();
        let versions: Vec<&crate::state::DatasetRecord> = match name {
            Some(n) => {
                let v: Vec<_> = self
                    .files
                    .datasets
                    .range((n.to_string(), 0)..=(n.to_string(), u32::MAX))
                    .map(|(_, d)| d)
                    .collect();
                ensure!(!v.is_empty(), "no dataset {n}");
                v
            }
            None => self.files.datasets.values().collect(),
        };
        let cps = self.reps.control_planes;
        for d in versions {
            let factor = self.factor_of(&d.name);
            let mut nodes = Vec::new();
            let mut held = 0u8;
            for ((_, n), rec) in self
                .reps
                .records
                .range((d.root, String::new())..)
                .take_while(|((r, _), _)| *r == d.root)
            {
                let c = live.get(n.as_str());
                let counted = rec.state == ReplicaState::Held && c.is_some_and(|c| c.live);
                if counted {
                    held += 1;
                }
                nodes.push(ReplicaView {
                    node: n.clone(),
                    name: self.node_name(n),
                    state: rec.state,
                    online: c.is_some_and(|c| c.online),
                    counted,
                    since_unix: rec.since_unix,
                    error: self.reps.errors.get(&(d.root, n.clone())).cloned(),
                });
            }
            let leader_has = !self.reps.leader_missing.contains(&d.root);
            let cp_copies = if leader_has { cps } else { cps - 1 };
            datasets.push(DatasetReplicas {
                name: d.name.clone(),
                version: d.version,
                account: d.account.clone(),
                root: d.root,
                size: d.size,
                factor,
                own_factor: self.reps.factors.contains_key(&d.name),
                copies: cp_copies.saturating_add(held),
                leader_has,
                nodes,
            });
        }
        let nodes = cands
            .iter()
            .map(|c| {
                let (used, replicas) = self.used_on(&c.id);
                let disk = self.latest_disk(&c.id);
                let scrub = self.reps.scrubs.get(&c.id);
                StorageNodeView {
                    node: c.id.clone(),
                    name: self.node_name(&c.id),
                    online: c.online,
                    not_used: self.not_used(&c.id),
                    quota: c.quota,
                    used,
                    replicas,
                    disk_total: disk.map(|d| d.0),
                    disk_free: disk.map(|(t, u)| t.saturating_sub(u)),
                    scrubbed_unix: scrub.map(|s| s.0),
                    bad: scrub.map_or(0, |s| s.1),
                }
            })
            .collect();
        Ok(ReplicationView {
            default_factor: self.reps.default_factor,
            control_planes: cps,
            under: datasets.iter().filter(|d| d.copies < d.factor).count() as u32,
            leader_missing: datasets.iter().filter(|d| !d.leader_has).count() as u32,
            datasets,
            nodes,
            scrubbed_unix: self.reps.leader_scrub.map(|s| s.0),
            bad: self.reps.leader_scrub.map_or(0, |s| s.1),
        })
    }

    /// Sets a dataset's copies (or with `None` gives it the default
    /// again), or without a name the default.
    pub fn set_replication(&mut self, name: Option<&str>, factor: Option<u8>) -> Result<()> {
        if let Some(f) = factor {
            validate_factor(f)?;
        }
        match name {
            Some(n) => {
                self.dataset(n, None)?;
                self.store.put_ds_replication(n, factor)?;
                match factor {
                    Some(f) => self.reps.factors.insert(n.to_string(), f),
                    None => self.reps.factors.remove(n),
                };
            }
            None => {
                let f = factor.context("the default needs a number of copies")?;
                self.store.put_meta(DEFAULT_FACTOR_META, &[f])?;
                self.reps.default_factor = f;
            }
        }
        self.reps.last_plan = None;
        Ok(())
    }

    /// A dataset's own number of copies goes with its last version.
    pub(crate) fn forget_replication(&mut self, name: &str) {
        if self.reps.factors.remove(name).is_some()
            && let Err(e) = self.store.put_ds_replication(name, None)
        {
            warn!("persist replication: {e:#}");
        }
        if let Err(e) = self.store.remove_s3_meta(name) {
            warn!("forget S3 records: {e:#}");
        }
    }

    // -- S3 keys ------------------------------------------------------------

    fn s3_key_view(&self, r: &S3KeyRecord) -> S3KeyView {
        let who: Option<Caller> = self.access_caller(&r.parent);
        S3KeyView {
            id: r.id.clone(),
            name: who
                .as_ref()
                .map_or("cluster admin".to_string(), |c| c.name().to_string()),
            role: who.map_or(AccessRole::Admin, |c| c.role()),
            label: r.label.clone(),
            created_unix: r.created_unix,
            last_used_unix: r.last_used_unix,
        }
    }

    /// A new S3 key for the ticket with secret `parent`.
    pub(crate) fn create_s3_key(
        &mut self,
        parent: Secret,
        label: &str,
    ) -> Result<(String, S3KeyView)> {
        let label: String = label.trim().chars().take(100).collect();
        let mine = self
            .reps
            .s3_keys
            .values()
            .filter(|k| secrets_eq(&k.parent, &parent))
            .count();
        ensure!(
            mine < MAX_S3_KEYS,
            "this ticket has {MAX_S3_KEYS} S3 keys already; revoke one first"
        );
        let id_bytes: [u8; 10] = rand::random();
        let id = format!(
            "{}{}",
            s3::KEY_PREFIX,
            data_encoding::BASE32_NOPAD.encode(&id_bytes)
        );
        let secret_bytes: [u8; 30] = rand::random();
        let secret = data_encoding::BASE64URL_NOPAD.encode(&secret_bytes);
        let r = S3KeyRecord {
            id: id.clone(),
            secret: secret.clone(),
            parent,
            label,
            created_unix: now_unix(),
            last_used_unix: None,
        };
        self.store.put_s3_key(&id, Some(&r))?;
        let view = self.s3_key_view(&r);
        self.reps.s3_keys.insert(id, r);
        Ok((secret, view))
    }

    /// S3 keys, newest first: all of them (`of: None`), or those of the
    /// ticket with secret `of`.
    pub(crate) fn list_s3_keys(&self, of: Option<&Secret>) -> Vec<S3KeyView> {
        let mut v: Vec<&S3KeyRecord> = self
            .reps
            .s3_keys
            .values()
            .filter(|k| of.is_none_or(|p| secrets_eq(p, &k.parent)))
            .collect();
        v.sort_by(|a, b| b.created_unix.cmp(&a.created_unix).then(a.id.cmp(&b.id)));
        v.into_iter().map(|k| self.s3_key_view(k)).collect()
    }

    pub(crate) fn revoke_s3_key(&mut self, id: &str, of: Option<&Secret>) -> Result<S3KeyView> {
        let id = id.trim();
        let r = self
            .reps
            .s3_keys
            .get(id)
            .filter(|k| of.is_none_or(|p| secrets_eq(p, &k.parent)))
            .with_context(|| format!("no such S3 key: {id}"))?
            .clone();
        self.store.put_s3_key(id, None)?;
        self.reps.s3_keys.remove(id);
        self.reps.s3_tokens.retain(|_, (k, _)| k != id);
        Ok(self.s3_key_view(&r))
    }

    /// Checks an S3 signature; on success hands out a credential that acts
    /// as the key's ticket for a few minutes, the day's signing key, and
    /// the ticket's caller. `ticket_ok` says whether a ticket secret is
    /// still valid.
    pub(crate) fn s3_auth(
        &mut self,
        key: &str,
        date: &str,
        region: &str,
        string_to_sign: &str,
        signature: &str,
        expires: Option<u32>,
    ) -> Result<(Secret, [u8; 32], Secret)> {
        let r = self
            .reps
            .s3_keys
            .get(key)
            .context("InvalidAccessKeyId")?
            .clone();
        s3::check_string_to_sign(string_to_sign, date, region, now_unix(), expires)
            .map_err(|e| anyhow::anyhow!(e))?;
        let signing = s3::signing_key(&r.secret, date, region);
        ensure!(
            s3::same(&s3::sign(&signing, string_to_sign), signature),
            "SignatureDoesNotMatch"
        );
        let token = pumasi_proto::new_secret();
        self.reps
            .s3_tokens
            .insert(token, (key.to_string(), Instant::now() + S3_TOKEN_TTL));
        let now = now_unix();
        if r.last_used_unix.is_none_or(|t| now >= t + 300) {
            let mut r2 = r.clone();
            r2.last_used_unix = Some(now);
            if let Err(e) = self.store.put_s3_key(key, Some(&r2)) {
                warn!("persist S3 key: {e:#}");
            }
            self.reps.s3_keys.insert(key.to_string(), r2);
        }
        Ok((token, signing, r.parent))
    }

    /// The ticket secret an S3 credential stands for.
    pub(crate) fn find_s3_token(&self, secret: &Secret) -> Option<Secret> {
        let (key, expires) = self.reps.s3_tokens.get(secret)?;
        if Instant::now() >= *expires {
            return None;
        }
        self.reps.s3_keys.get(key).map(|k| k.parent)
    }

    fn expire_s3_tokens(&mut self) {
        let now = Instant::now();
        self.reps.s3_tokens.retain(|_, (_, t)| *t > now);
    }

    /// S3 keys whose ticket is gone (revoked, expired, a rotated admin
    /// secret) are deleted.
    pub(crate) fn tick_s3_keys(&mut self, admin: &Secret) {
        let dead: Vec<String> = self
            .reps
            .s3_keys
            .values()
            .filter(|k| !secrets_eq(&k.parent, admin) && self.access_caller(&k.parent).is_none())
            .map(|k| k.id.clone())
            .collect();
        for id in dead {
            info!(key = %id, "S3 key ended with its ticket");
            let _ = self.revoke_s3_key(&id, None);
        }
    }

    // -- S3 objects -----------------------------------------------------------

    /// Records an S3 write: a new version of `name` unless the latest is
    /// no longer `base`, and the object records it changes.
    pub(crate) fn s3_commit(
        &mut self,
        stored: pumasi_proto::store::StoredOutput,
        name: &str,
        base: Option<u32>,
        account: Option<String>,
        meta: Vec<(String, Option<S3Meta>)>,
    ) -> Result<pumasi_proto::store::DatasetView> {
        let latest = self.dataset(name, None).ok().map(|d| d.version);
        ensure!(
            latest == base,
            "Conflict: dataset {name} changed while this write was under way"
        );
        for (path, m) in &meta {
            pumasi_proto::job::validate_rel_path(path)?;
            if let Some(m) = m {
                let bytes: usize = m.meta.iter().map(|(k, v)| k.len() + v.len()).sum();
                ensure!(
                    bytes <= MAX_S3_META_BYTES,
                    "user metadata over {MAX_S3_META_BYTES} bytes"
                );
            }
        }
        let account = match (account, latest) {
            (Some(a), _) => Some(a),
            (None, Some(_)) => self.dataset(name, None).ok().map(|d| d.account.clone()),
            (None, None) => None,
        };
        let view = self
            .commit_upload(stored, Some(name.to_string()), account)?
            .context("a named commit makes a version")?;
        self.store.put_s3_meta(name, &meta)?;
        // S3 buckets have no versions: a write replaces the version the
        // gateway's previous write made (each version counts against the
        // account's quota). Versions made otherwise stay.
        if let Some(b) = base
            && b != view.version
        {
            if self.store.s3_version(name)? == Some(b) {
                let _ = self.remove_datasets(name, Some(b));
            }
            self.store.put_s3_version(name, view.version)?;
        } else if base.is_none() {
            self.store.put_s3_version(name, view.version)?;
        }
        Ok(view)
    }

    pub(crate) fn s3_meta_of(&self, name: &str) -> Vec<(String, S3Meta)> {
        self.store.s3_meta(name).unwrap_or_default()
    }
}

/// Nodes holding replicas of these collections, as fetch sources for a
/// task's mounts (after the nodes that cache them).
pub(crate) fn replica_peers(
    reps: &RepState,
    caches: &HashMap<String, crate::datasets::NodeCache>,
    node: &str,
    roots: &[BlobHash],
) -> Vec<EndpointAddr> {
    let mut out: Vec<EndpointAddr> = Vec::new();
    for root in roots {
        for ((_, n), rec) in reps
            .records
            .range((*root, String::new())..)
            .take_while(|((r, _), _)| r == root)
        {
            if n == node || rec.state != ReplicaState::Held {
                continue;
            }
            if let Some(a) = caches.get(n).and_then(|c| c.addr.clone())
                && !out.iter().any(|o| o.id == a.id)
            {
                out.push(a);
            }
        }
    }
    out
}

/// Collections named in a list, as a set (for agents' reports).
pub fn root_set(roots: &[BlobHash]) -> BTreeSet<BlobHash> {
    roots.iter().copied().collect()
}
