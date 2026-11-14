//! The file store's records and rules on the control plane (phase 8):
//! datasets and their versions, job mounts, stored task outputs, the
//! output dataset made when a job ends, quotas, storage and egress
//! billing, cluster secrets and private images.
//!
//! The data itself lives in the iroh-blobs store ([`crate::files`]). The
//! state machine never touches it: it sends [`StoreCmd`]s (set or delete a
//! tag, pull an image, combine outputs) to a worker that does the I/O and
//! reports back. Tags are what garbage collection keeps, so every record
//! here that refers to data has a tag, and [`State::desired_tags`] lists
//! them all for the worker to reconcile at startup.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use iroh::EndpointAddr;
use pumasi_proto::{
    STORE_PROTOCOL,
    economy::validate_account,
    job::{JobId, JobSubmission},
    msg::{JobState, LedgerKind, OWNER_ACCOUNT, Outcome, SecretView, TaskState},
    perf::DiskClass,
    registry::{Credentials, ImageRef, normalize_registry},
    store::{
        BlobHash, DatasetView, ImageBlob, Mount, MountRequest, MountSource, StoragePolicy,
        StoreItem, StoreTarget, StoreUsageView, StoredOutput, egress_charge, storage_charge,
        validate_dataset_name,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{
    files::{self, Peer},
    state::{State, now_unix},
};

/// Committed uploads a job has not used are forgotten after this long.
const UPLOAD_TTL_SECS: u64 = 24 * 3600;
/// Most nodes an assignment names as places to fetch from.
const MAX_PEERS: usize = 4;
const STORAGE_POLICY_META: &str = "storage_policy";
const STORAGE_BILLED_META: &str = "storage_billed";

/// One version of a dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetRecord {
    pub name: String,
    pub version: u32,
    pub account: String,
    pub root: BlobHash,
    pub size: u64,
    pub files: u32,
    pub created_unix: u64,
    pub job: Option<JobId>,
}

impl DatasetRecord {
    pub fn view(&self) -> DatasetView {
        DatasetView {
            name: self.name.clone(),
            version: self.version,
            account: self.account.clone(),
            root: self.root,
            size: self.size,
            files: self.files,
            created_unix: self.created_unix,
            job: self.job,
        }
    }
}

/// A private image being pulled into the store for one architecture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageState {
    Pulling,
    Ready(ImageBlob),
    Failed(String),
}

/// What a job needs from the file store (kept in its own table).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobExt {
    /// Runs only on nodes that speak the file store protocol.
    pub needs_store: bool,
    /// Mounted in every task.
    pub mounts: Vec<Mount>,
    /// Outputs always go to the store and become a version of this
    /// dataset when the job ends.
    pub output_dataset: Option<String>,
    /// The image's registry has credentials on the control plane, so the
    /// control plane pulls it into the store for the nodes.
    pub private_image: bool,
    /// Private image by OCI architecture (`amd64`, `arm64`).
    pub images: BTreeMap<String, ImageState>,
    /// Set once the end of the job was handled: the output dataset version
    /// made (`name@3`), or empty.
    pub finished: Option<String>,
}

impl JobExt {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Every collection the job's tasks may mount.
    fn roots(&self) -> impl Iterator<Item = &BlobHash> {
        self.mounts.iter().map(|m| &m.root)
    }
}

/// What a task mounts and what it stored (kept in its own table).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskExt {
    pub mounts: Vec<Mount>,
    /// The accepted run's output files, when they went to the store.
    pub stored: Option<StoredOutput>,
    /// Stored outputs of check runs, by attempt, until the check settles.
    pub check_stored: Vec<(u32, StoredOutput)>,
}

impl TaskExt {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// A collection the CLI uploaded for a job's inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRecord {
    pub account: String,
    pub size: u64,
    pub files: u32,
    pub unix: u64,
}

/// Work for the file store worker, done in order.
#[derive(Debug, Clone)]
pub enum StoreCmd {
    Tag(String, BlobHash),
    Untag(String),
    UntagPrefix(String),
    /// Make the store's tags exactly these (at startup).
    Reconcile(Vec<(String, BlobHash)>),
    /// Pull a private image for one architecture into the store, then
    /// call [`State::on_image_ready`].
    PrepareImage {
        job: JobId,
        arch: String,
        reference: String,
        /// A confidential job's key: the image is sealed with it (phase
        /// 16) and kept only sealed.
        seal: Option<[u8; 32]>,
    },
    /// Build and seal a confidential task's input, then call
    /// [`State::on_input_sealed`].
    SealInput(Box<crate::confidential::SealInputJob>),
    /// Combine the tasks' stored outputs into one collection, then call
    /// [`State::on_job_output`].
    FinishJob {
        job: JobId,
        parts: Vec<(String, BlobHash)>,
    },
}

/// What a node's store cache holds and where to reach it.
#[derive(Debug, Clone, Default)]
pub struct NodeCache {
    pub addr: Option<EndpointAddr>,
    pub roots: HashSet<BlobHash>,
}

/// The file store part of [`State`].
pub(crate) struct FileState {
    pub datasets: BTreeMap<(String, u32), DatasetRecord>,
    pub quotas: HashMap<String, u64>,
    pub secrets: BTreeMap<String, (u64, String)>,
    pub uploads: HashMap<BlobHash, UploadRecord>,
    pub policy: StoragePolicy,
    /// The control plane's disk, which picks the storage rate.
    pub disk_class: DiskClass,
    pub cmds: Option<mpsc::UnboundedSender<StoreCmd>>,
    pub caches: HashMap<String, NodeCache>,
    /// Jobs whose end still has store work (an output dataset, tags).
    pub open_jobs: HashSet<JobId>,
    /// Jobs whose outputs are being combined.
    pub finishing: HashSet<JobId>,
    /// Storage is charged up to this time.
    pub billed_unix: u64,
    pub bill_every: Duration,
    pub last_bill_check: Option<Instant>,
}

impl Default for FileState {
    fn default() -> Self {
        Self {
            datasets: BTreeMap::new(),
            quotas: HashMap::new(),
            secrets: BTreeMap::new(),
            uploads: HashMap::new(),
            policy: StoragePolicy::default(),
            disk_class: DiskClass::Unknown,
            cmds: None,
            caches: HashMap::new(),
            open_jobs: HashSet::new(),
            finishing: HashSet::new(),
            billed_unix: 0,
            bill_every: Duration::from_secs(3600),
            last_bill_check: None,
        }
    }
}

fn send(cmds: &Option<mpsc::UnboundedSender<StoreCmd>>, cmd: StoreCmd) {
    if let Some(tx) = cmds {
        let _ = tx.send(cmd);
    }
}

/// The OCI name of a node's architecture.
pub fn oci_arch_of(arch: &str) -> String {
    match arch {
        "x86_64" | "x86-64" | "amd64" => "amd64".into(),
        "aarch64" | "arm64" => "arm64".into(),
        other => other.to_string(),
    }
}

/// Nodes (other than `node`) that have any of these collections cached,
/// best first: those with more of them, then the closest (`rtt`, the
/// measured round trip to a node, 1.6), then those never measured. Only
/// connected nodes have a cache entry.
pub(crate) fn peers_for(
    caches: &HashMap<String, NodeCache>,
    node: &str,
    roots: &[BlobHash],
    rtt: impl Fn(&str) -> Option<u32>,
) -> Vec<EndpointAddr> {
    if roots.is_empty() {
        return vec![];
    }
    let mut found: Vec<(usize, u32, &EndpointAddr)> = caches
        .iter()
        .filter(|(id, _)| id.as_str() != node)
        .filter_map(|(id, c)| {
            let have = roots.iter().filter(|r| c.roots.contains(*r)).count();
            c.addr
                .as_ref()
                .filter(|_| have > 0)
                .map(|a| (have, rtt(id).unwrap_or(u32::MAX), a))
        })
        .collect();
    found.sort_by_key(|(have, rtt, _)| (std::cmp::Reverse(*have), *rtt));
    found
        .into_iter()
        .take(MAX_PEERS)
        .map(|(_, _, a)| a.clone())
        .collect()
}

/// The dataset a job's outputs go to when it names none.
pub fn job_dataset_name(job: JobId) -> String {
    format!("job-{job}")
}

fn validate_secret_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 255,
        "secret name must be 1 to 255 characters"
    );
    ensure!(
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || "./:_-".contains(c)),
        "secret name may use only letters, digits and . / : _ -"
    );
    Ok(())
}

impl State {
    /// Loads the file store records at startup.
    pub(crate) fn load_files(
        &mut self,
        datasets: Vec<DatasetRecord>,
        quotas: Vec<(String, u64)>,
        secrets: Vec<(String, u64, String)>,
        uploads: Vec<(BlobHash, UploadRecord)>,
    ) -> Result<()> {
        let f = &mut self.files;
        f.datasets = datasets
            .into_iter()
            .map(|d| ((d.name.clone(), d.version), d))
            .collect();
        f.quotas = quotas.into_iter().collect();
        f.secrets = secrets.into_iter().map(|(n, t, v)| (n, (t, v))).collect();
        f.uploads = uploads.into_iter().collect();
        if let Some(b) = self.store.get_meta(STORAGE_POLICY_META)? {
            f.policy = postcard::from_bytes(&b).context("bad storage policy")?;
        }
        f.billed_unix = match self.store.get_meta(STORAGE_BILLED_META)? {
            Some(b) => u64::from_le_bytes(b.try_into().unwrap_or([0; 8])),
            None => now_unix(),
        };
        for job in self.jobs.values_mut() {
            // Pulls in flight died with the restart; they start again.
            job.meta
                .ext
                .images
                .retain(|_, s| !matches!(s, ImageState::Pulling));
            let stored = job.tasks.iter().any(|t| t.ext.stored.is_some())
                || self
                    .sched
                    .checkpoints
                    .keys()
                    .any(|(j, _)| *j == job.meta.id);
            if job.meta.ext.finished.is_none() && (job.meta.ext.needs_store || stored) {
                self.files.open_jobs.insert(job.meta.id);
            }
        }
        Ok(())
    }

    /// Hands the state the file store worker's queue, and the settings of
    /// this run of the control plane.
    pub fn attach_store(
        &mut self,
        cmds: mpsc::UnboundedSender<StoreCmd>,
        disk_class: DiskClass,
        bill_every: Duration,
    ) {
        let _ = cmds.send(StoreCmd::Reconcile(self.desired_tags()));
        self.files.cmds = Some(cmds);
        self.files.disk_class = disk_class;
        self.files.bill_every = bill_every;
    }

    pub(crate) fn store_cmd(&self, cmd: StoreCmd) {
        send(&self.files.cmds, cmd);
    }

    /// Every tag the store should have.
    pub fn desired_tags(&self) -> Vec<(String, BlobHash)> {
        let mut out = Vec::new();
        for d in self.files.datasets.values() {
            out.push((files::ds_tag(&d.name, d.version), d.root));
        }
        for root in self.files.uploads.keys() {
            out.push((files::up_tag(root), *root));
        }
        for job in self.jobs.values() {
            let id = job.meta.id;
            let done = matches!(&job.meta.ext.finished, Some(f) if !f.is_empty());
            if done {
                continue;
            }
            for (i, t) in job.tasks.iter().enumerate() {
                if let Some(s) = &t.ext.stored {
                    out.push((files::out_tag(id.0, i as u32), s.root));
                }
                for (attempt, s) in &t.ext.check_stored {
                    out.push((files::chk_tag(id.0, i as u32, *attempt), s.root));
                }
            }
            if job.meta.ext.finished.is_some() {
                continue;
            }
            for ((j, i), r) in &self.sched.checkpoints {
                if *j == id {
                    out.push((files::ckpt_tag(id.0, *i), r.root));
                }
            }
            let roots: HashSet<BlobHash> = job
                .meta
                .ext
                .roots()
                .chain(
                    job.tasks
                        .iter()
                        .flat_map(|t| t.ext.mounts.iter().map(|m| &m.root)),
                )
                .copied()
                .collect();
            for root in roots {
                out.push((files::in_tag(id.0, &root), root));
            }
            for (arch, s) in &job.meta.ext.images {
                if let ImageState::Ready(img) = s {
                    out.push((
                        files::img_tag(&format!("{id}/{arch}"), &img.digest),
                        img.root,
                    ));
                }
            }
        }
        out
    }

    // -- submission -------------------------------------------------------

    fn resolve_mount(&self, account: &str, req: &MountRequest) -> Result<Mount> {
        let (root, size, label) = match &req.source {
            MountSource::Dataset { name, version } => {
                let d = self.dataset(name, *version)?;
                (d.root, d.size, format!("{}@{}", d.name, d.version))
            }
            MountSource::Upload { root } => {
                let u =
                    self.files.uploads.get(root).context(
                        "the uploaded input files are not in the store (upload them again)",
                    )?;
                ensure!(
                    u.account == account,
                    "the uploaded input files belong to account {}",
                    u.account
                );
                (*root, u.size, "upload".to_string())
            }
        };
        Ok(Mount {
            root,
            select: req.select.clone(),
            at: req.at.clone(),
            label,
            size,
        })
    }

    /// The store part of a new job: mounts resolved to collections, and
    /// whether its image comes through the store.
    pub(crate) fn prepare_store(
        &self,
        sub: &JobSubmission,
        account: &str,
    ) -> Result<(JobExt, Vec<TaskExt>)> {
        let mut ext = JobExt {
            mounts: sub
                .mounts
                .iter()
                .map(|m| self.resolve_mount(account, m))
                .collect::<Result<_>>()?,
            output_dataset: sub.output_dataset.clone(),
            ..Default::default()
        };
        let mut tasks: Vec<TaskExt> = vec![TaskExt::default(); sub.tasks.len()];
        for (t, reqs) in tasks.iter_mut().zip(&sub.task_mounts) {
            t.mounts = reqs
                .iter()
                .map(|m| self.resolve_mount(account, m))
                .collect::<Result<_>>()?;
        }
        if sub.executor.runs_images()
            && let Some(d) = &sub.docker
            && let Ok(image) = ImageRef::parse(&d.image)
        {
            // A confidential job's image is always pulled here: the nodes
            // get it only sealed.
            ext.private_image = sub.tier == pumasi_proto::job::Tier::Confidential
                || self.registry_credentials().contains_key(&image.registry);
        }
        if let Some(name) = &ext.output_dataset {
            validate_dataset_name(name)?;
            self.check_quota(account, 0)?;
        }
        ext.needs_store = !ext.mounts.is_empty()
            || ext.output_dataset.is_some()
            || ext.private_image
            || tasks.iter().any(|t| !t.mounts.is_empty());
        Ok((ext, tasks))
    }

    /// Tags what a new job mounts so it stays until the job ends.
    pub(crate) fn after_submit(&mut self, id: JobId) {
        let Some(job) = self.jobs.get(&id) else {
            return;
        };
        if !job.meta.ext.needs_store {
            return;
        }
        let roots: HashSet<BlobHash> = job
            .meta
            .ext
            .roots()
            .chain(
                job.tasks
                    .iter()
                    .flat_map(|t| t.ext.mounts.iter().map(|m| &m.root)),
            )
            .copied()
            .collect();
        for root in &roots {
            self.store_cmd(StoreCmd::Tag(files::in_tag(id.0, root), *root));
        }
        // An upload belongs to the job that uses it from now on.
        for root in &roots {
            if self.files.uploads.remove(root).is_some() {
                if let Err(e) = self.store.put_upload(root, None) {
                    warn!("persist upload: {e:#}");
                }
                self.store_cmd(StoreCmd::Untag(files::up_tag(root)));
            }
        }
        self.files.open_jobs.insert(id);
    }

    // -- scheduling -------------------------------------------------------

    /// Asks for a job's private image on every architecture among the
    /// nodes that could run it. Returns the architectures it is ready on.
    pub(crate) fn want_images(&mut self, id: JobId) -> Vec<(String, ImageBlob)> {
        let seal = self
            .jobs
            .get(&id)
            .is_some_and(|j| j.meta.tier == pumasi_proto::job::Tier::Confidential)
            .then(|| self.job_key(id));
        let Some(job) = self.jobs.get_mut(&id) else {
            return vec![];
        };
        let executor = job.meta.executor;
        let Some(reference) = job.meta.docker.as_ref().map(|d| d.image.clone()) else {
            return vec![];
        };
        // Confidential VMs run on amd64 hosts only (SEV-SNP, TDX).
        let archs: HashSet<String> = if seal.is_some() {
            HashSet::from(["amd64".to_string()])
        } else {
            self.nodes
                .values()
                .filter(|n| {
                    n.online
                        && n.perf.protocol >= STORE_PROTOCOL
                        && executor.runs_as().iter().any(|e| n.executors.contains(e))
                })
                .map(|n| oci_arch_of(&n.info.arch))
                .collect()
        };
        for arch in archs {
            if !job.meta.ext.images.contains_key(&arch) {
                job.meta
                    .ext
                    .images
                    .insert(arch.clone(), ImageState::Pulling);
                info!(job = %id, %arch, image = %reference, "pulling a private image into the store");
                if let Some(tx) = &self.files.cmds {
                    let _ = tx.send(StoreCmd::PrepareImage {
                        job: id,
                        arch,
                        reference: reference.clone(),
                        seal,
                    });
                }
            }
        }
        job.meta
            .ext
            .images
            .iter()
            .filter_map(|(a, s)| match s {
                ImageState::Ready(img) => Some((a.clone(), img.clone())),
                _ => None,
            })
            .collect()
    }

    /// A node told what its store cache holds.
    pub fn on_store_state(
        &mut self,
        node: &str,
        generation: u64,
        addr: Option<EndpointAddr>,
        full: bool,
        added: Vec<BlobHash>,
        removed: Vec<BlobHash>,
    ) {
        if self.nodes.get(node).map(|n| n.generation) != Some(generation) {
            return;
        }
        let c = self.files.caches.entry(node.to_string()).or_default();
        if addr.is_some() {
            c.addr = addr;
        }
        if full {
            c.roots.clear();
        }
        for r in removed {
            c.roots.remove(&r);
        }
        c.roots.extend(added);
    }

    /// Who may use the store's blobs ALPN: members, and members with a run
    /// in flight (they push outputs).
    pub fn store_peer(&self, id: &iroh::EndpointId) -> Peer {
        let id = id.to_string();
        let member = self.members.contains_key(&id);
        let pusher = member && self.nodes.get(&id).is_some_and(|n| !n.running.is_empty());
        Peer { member, pusher }
    }

    // -- reports ------------------------------------------------------------

    /// Records a task's stored output (accepted run) and tags it.
    pub(crate) fn set_stored(&mut self, job: JobId, index: u32, stored: Option<StoredOutput>) {
        let Some(task) = self
            .jobs
            .get_mut(&job)
            .and_then(|j| j.tasks.get_mut(index as usize))
        else {
            return;
        };
        let before = task.ext.stored;
        task.ext.stored = stored;
        let tag = files::out_tag(job.0, index);
        match stored {
            Some(s) => {
                self.store_cmd(StoreCmd::Tag(tag, s.root));
                self.files.open_jobs.insert(job);
            }
            None if before.is_some() => self.store_cmd(StoreCmd::Untag(tag)),
            None => {}
        }
    }

    /// Keeps a check run's stored output until its check settles.
    pub(crate) fn add_check_stored(
        &mut self,
        job: JobId,
        index: u32,
        attempt: u32,
        s: StoredOutput,
    ) {
        if let Some(task) = self
            .jobs
            .get_mut(&job)
            .and_then(|j| j.tasks.get_mut(index as usize))
        {
            task.ext.check_stored.push((attempt, s));
            self.store_cmd(StoreCmd::Tag(files::chk_tag(job.0, index, attempt), s.root));
        }
    }

    /// A check settled: the winner's stored output (if a check run won)
    /// becomes the task's, and the check runs' outputs are let go.
    pub(crate) fn settle_check_stored(
        &mut self,
        job: JobId,
        index: u32,
        winner_attempt: Option<u32>,
    ) {
        let Some(task) = self
            .jobs
            .get_mut(&job)
            .and_then(|j| j.tasks.get_mut(index as usize))
        else {
            return;
        };
        let won = winner_attempt.map(|a| {
            task.ext
                .check_stored
                .iter()
                .find(|(x, _)| *x == a)
                .map(|(_, s)| *s)
        });
        let had_checks = !task.ext.check_stored.is_empty();
        task.ext.check_stored.clear();
        if let Some(stored) = won {
            self.set_stored(job, index, stored);
        }
        if had_checks {
            self.store_cmd(StoreCmd::UntagPrefix(format!(
                "chk/{:016x}/{index}/",
                job.0
            )));
        }
    }

    // -- the end of a job ---------------------------------------------------

    fn job_done(&self, id: JobId) -> bool {
        self.jobs.get(&id).is_some_and(|j| {
            matches!(
                Self::summary(j).state,
                JobState::Succeeded | JobState::Failed | JobState::Cancelled
            )
        })
    }

    /// Makes the output dataset of jobs that ended and lets go of what they
    /// mounted.
    fn finish_jobs(&mut self) {
        let done: Vec<JobId> = self
            .files
            .open_jobs
            .iter()
            .copied()
            .filter(|id| !self.files.finishing.contains(id) && self.job_done(*id))
            .collect();
        for id in done {
            let Some(job) = self.jobs.get(&id) else {
                self.files.open_jobs.remove(&id);
                continue;
            };
            let parts: Vec<(String, BlobHash)> = job
                .tasks
                .iter()
                .enumerate()
                .filter_map(|(i, t)| t.ext.stored.map(|s| (format!("task-{i}"), s.root)))
                .collect();
            if parts.is_empty() {
                self.end_job_store(id, String::new());
            } else {
                self.files.finishing.insert(id);
                self.store_cmd(StoreCmd::FinishJob { job: id, parts });
            }
        }
    }

    fn end_job_store(&mut self, id: JobId, finished: String) {
        self.files.open_jobs.remove(&id);
        self.files.finishing.remove(&id);
        self.store_cmd(StoreCmd::UntagPrefix(files::in_prefix(id.0)));
        self.store_cmd(StoreCmd::UntagPrefix(format!("img/{id}/")));
        // Checkpoint records stay (for the job's page); their data goes.
        self.store_cmd(StoreCmd::UntagPrefix(files::ckpt_prefix(id.0)));
        self.conf_job_ended(id);
        if let Some(job) = self.jobs.get_mut(&id) {
            job.meta.ext.finished = Some(finished);
            job.meta.ext.images.clear();
            let meta = job.meta.clone();
            if let Err(e) = self.store.put_job_meta(&meta) {
                warn!("persist job: {e:#}");
            }
        }
    }

    /// The worker combined a finished job's stored outputs.
    pub fn on_job_output(&mut self, id: JobId, combined: Result<StoredOutput>) {
        let Some(job) = self.jobs.get(&id) else {
            self.files.finishing.remove(&id);
            return;
        };
        let combined = match combined {
            Ok(c) => c,
            Err(e) => {
                warn!(job = %id, "could not make the job's output dataset: {e:#}");
                // The task outputs stay, each under its own tag.
                self.end_job_store(id, String::new());
                return;
            }
        };
        let name = job
            .meta
            .ext
            .output_dataset
            .clone()
            .unwrap_or_else(|| job_dataset_name(id));
        let account = job.meta.account.clone();
        let version = self.next_version(&name);
        let d = DatasetRecord {
            name: name.clone(),
            version,
            account,
            root: combined.root,
            size: combined.size,
            files: combined.files,
            created_unix: now_unix(),
            job: Some(id),
        };
        if let Err(e) = self.store.put_dataset(&d) {
            warn!("persist dataset: {e:#}");
            self.end_job_store(id, String::new());
            return;
        }
        info!(job = %id, dataset = %format!("{name}@{version}"), files = d.files, bytes = d.size, "job outputs stored");
        self.store_cmd(StoreCmd::Tag(files::ds_tag(&name, version), d.root));
        self.store_cmd(StoreCmd::Untag(format!("job/{id}")));
        self.store_cmd(StoreCmd::UntagPrefix(files::out_prefix(id.0)));
        self.files.datasets.insert((name.clone(), version), d);
        self.end_job_store(id, format!("{name}@{version}"));
    }

    /// The worker pulled (or failed to pull) a private image.
    pub fn on_image_ready(&mut self, id: JobId, arch: &str, result: Result<ImageBlob>) {
        let Some(job) = self.jobs.get_mut(&id) else {
            return;
        };
        match result {
            Ok(img) => {
                info!(job = %id, %arch, digest = %img.digest, "private image ready");
                send(
                    &self.files.cmds,
                    StoreCmd::Tag(
                        files::img_tag(&format!("{id}/{arch}"), &img.digest),
                        img.root,
                    ),
                );
                job.meta
                    .ext
                    .images
                    .insert(arch.to_string(), ImageState::Ready(img));
            }
            Err(e) => {
                let msg = format!("{e:#}");
                warn!(job = %id, %arch, "could not pull the private image: {msg}");
                job.meta
                    .ext
                    .images
                    .insert(arch.to_string(), ImageState::Failed(msg.clone()));
                let hopeless = job
                    .meta
                    .ext
                    .images
                    .values()
                    .all(|s| matches!(s, ImageState::Failed(_)));
                if hopeless {
                    let mut changed = Vec::new();
                    for (i, t) in job.tasks.iter_mut().enumerate() {
                        if t.state == TaskState::Pending {
                            t.state = TaskState::Failed(Outcome::Error(format!(
                                "pull the private image: {msg}"
                            )));
                            changed.push(i as u32);
                        }
                    }
                    let refs: Vec<(u32, &crate::state::TaskRecord)> = changed
                        .iter()
                        .map(|&i| (i, &job.tasks[i as usize]))
                        .collect();
                    if let Err(e) = self.store.put_tasks(id.0, &refs) {
                        warn!("persist tasks: {e:#}");
                    }
                    self.queue.retain(|q| q.job != id);
                }
            }
        }
        let meta = job.meta.clone();
        if let Err(e) = self.store.put_job_meta(&meta) {
            warn!("persist job: {e:#}");
        }
        self.schedule();
    }

    // -- datasets -----------------------------------------------------------

    fn next_version(&self, name: &str) -> u32 {
        self.files
            .datasets
            .range((name.to_string(), 0)..=(name.to_string(), u32::MAX))
            .next_back()
            .map(|((_, v), _)| v + 1)
            .unwrap_or(1)
    }

    /// A dataset version (the latest when `version` is `None`).
    pub fn dataset(&self, name: &str, version: Option<u32>) -> Result<&DatasetRecord> {
        let found = match version {
            Some(v) => self.files.datasets.get(&(name.to_string(), v)),
            None => self
                .files
                .datasets
                .range((name.to_string(), 0)..=(name.to_string(), u32::MAX))
                .next_back()
                .map(|(_, d)| d),
        };
        found.with_context(|| match version {
            Some(v) => format!("no dataset {name}@{v}"),
            None => format!("no dataset {name}"),
        })
    }

    /// Bytes stored for each account, and its dataset versions.
    pub(crate) fn usage_by_account(&self) -> BTreeMap<String, (u64, u32)> {
        let mut out: BTreeMap<String, (u64, u32)> = BTreeMap::new();
        for d in self.files.datasets.values() {
            let e = out.entry(d.account.clone()).or_default();
            e.0 += d.size;
            e.1 += 1;
        }
        for u in self.files.uploads.values() {
            out.entry(u.account.clone()).or_default().0 += u.size;
        }
        for job in self.jobs.values() {
            if matches!(&job.meta.ext.finished, Some(f) if !f.is_empty()) {
                continue;
            }
            let bytes: u64 = job
                .tasks
                .iter()
                .filter_map(|t| t.ext.stored.map(|s| s.size))
                .sum();
            if bytes > 0 {
                out.entry(job.meta.account.clone()).or_default().0 += bytes;
            }
        }
        out
    }

    fn quota_of(&self, account: &str) -> Option<u64> {
        self.files
            .quotas
            .get(account)
            .copied()
            .or(self.files.policy.default_quota)
    }

    pub(crate) fn check_quota(&self, account: &str, adding: u64) -> Result<()> {
        if let Some(q) = self.quota_of(account) {
            let used = self
                .usage_by_account()
                .get(account)
                .map(|u| u.0)
                .unwrap_or(0);
            ensure!(
                used.saturating_add(adding) <= q && (adding > 0 || used < q),
                "account {account} would store {} of its {} quota",
                pumasi_proto::store::format_size(used.saturating_add(adding)),
                pumasi_proto::store::format_size(q)
            );
        }
        Ok(())
    }

    /// Records a complete uploaded collection: a new dataset version, or a
    /// job input the next submission mounts.
    pub fn commit_upload(
        &mut self,
        stored: StoredOutput,
        name: Option<String>,
        account: Option<String>,
    ) -> Result<Option<DatasetView>> {
        let account = account.unwrap_or_else(|| OWNER_ACCOUNT.into());
        validate_account(&account)?;
        self.ensure_account(&account)?;
        let root = stored.root;
        match name {
            Some(name) => {
                validate_dataset_name(&name)?;
                // The same data as the latest version is not a new version.
                if let Ok(d) = self.dataset(&name, None)
                    && d.root == root
                {
                    return Ok(Some(d.view()));
                }
                self.check_quota(&account, stored.size)?;
                let version = self.next_version(&name);
                let d = DatasetRecord {
                    name: name.clone(),
                    version,
                    account,
                    root,
                    size: stored.size,
                    files: stored.files,
                    created_unix: now_unix(),
                    job: None,
                };
                self.store.put_dataset(&d)?;
                self.store_cmd(StoreCmd::Tag(files::ds_tag(&name, version), root));
                info!(dataset = %format!("{name}@{version}"), files = d.files, bytes = d.size, "dataset stored");
                let view = d.view();
                self.files.datasets.insert((name, version), d);
                Ok(Some(view))
            }
            None => {
                if !self.files.uploads.contains_key(&root) {
                    self.check_quota(&account, stored.size)?;
                }
                let u = UploadRecord {
                    account,
                    size: stored.size,
                    files: stored.files,
                    unix: now_unix(),
                };
                self.store.put_upload(&root, Some(&u))?;
                self.store_cmd(StoreCmd::Tag(files::up_tag(&root), root));
                self.files.uploads.insert(root, u);
                Ok(None)
            }
        }
    }

    pub fn list_datasets(&self, name: Option<&str>) -> Vec<DatasetView> {
        match name {
            Some(n) => self
                .files
                .datasets
                .range((n.to_string(), 0)..=(n.to_string(), u32::MAX))
                .map(|(_, d)| d.view())
                .collect(),
            None => {
                let mut latest: BTreeMap<&str, &DatasetRecord> = BTreeMap::new();
                for d in self.files.datasets.values() {
                    latest.insert(&d.name, d);
                }
                latest.values().map(|d| d.view()).collect()
            }
        }
    }

    /// Deletes one version of a dataset, or every version.
    pub fn remove_datasets(
        &mut self,
        name: &str,
        version: Option<u32>,
    ) -> Result<Vec<DatasetView>> {
        let keys: Vec<(String, u32)> = match version {
            Some(v) => {
                self.dataset(name, Some(v))?;
                vec![(name.to_string(), v)]
            }
            None => self
                .files
                .datasets
                .range((name.to_string(), 0)..=(name.to_string(), u32::MAX))
                .map(|(k, _)| k.clone())
                .collect(),
        };
        ensure!(!keys.is_empty(), "no dataset {name}");
        self.store.remove_datasets(&keys)?;
        let mut out = Vec::new();
        for k in keys {
            self.store_cmd(StoreCmd::Untag(files::ds_tag(&k.0, k.1)));
            if let Some(d) = self.files.datasets.remove(&k) {
                info!(dataset = %format!("{}@{}", d.name, d.version), "dataset removed");
                out.push(d.view());
            }
        }
        if self.dataset(name, None).is_err() {
            self.forget_replication(name);
        }
        Ok(out)
    }

    /// What `pumasi store get` downloads, and the account paying for it.
    pub fn open_target(&self, target: &StoreTarget) -> Result<(String, Vec<StoreItem>)> {
        match target {
            StoreTarget::Dataset { name, version } => {
                let d = self.dataset(name, *version)?;
                Ok((
                    d.account.clone(),
                    vec![StoreItem {
                        dir: String::new(),
                        root: d.root,
                        size: d.size,
                        files: d.files,
                    }],
                ))
            }
            StoreTarget::Job { id } => {
                let job = self.jobs.get(id).context("no such job")?;
                let account = job.meta.account.clone();
                if let Some(f) = job.meta.ext.finished.as_deref().filter(|f| !f.is_empty())
                    && let Some((name, v)) = f.rsplit_once('@')
                    && let Ok(d) = self.dataset(name, v.parse().ok())
                {
                    return Ok((
                        account,
                        vec![StoreItem {
                            dir: String::new(),
                            root: d.root,
                            size: d.size,
                            files: d.files,
                        }],
                    ));
                }
                let items = job
                    .tasks
                    .iter()
                    .enumerate()
                    .filter_map(|(i, t)| {
                        t.ext.stored.map(|s| StoreItem {
                            dir: format!("task-{i}"),
                            root: s.root,
                            size: s.size,
                            files: s.files,
                        })
                    })
                    .collect();
                Ok((account, items))
            }
        }
    }

    pub fn store_usage(&self) -> Vec<StoreUsageView> {
        let usage = self.usage_by_account();
        let mut names: Vec<&String> = usage.keys().chain(self.files.quotas.keys()).collect();
        names.sort();
        names.dedup();
        names
            .into_iter()
            .map(|a| {
                let (bytes, datasets) = usage.get(a).copied().unwrap_or((0, 0));
                StoreUsageView {
                    account: a.clone(),
                    bytes,
                    quota: self.quota_of(a),
                    datasets,
                }
            })
            .collect()
    }

    pub fn set_quota(&mut self, account: &str, bytes: Option<u64>) -> Result<()> {
        validate_account(account)?;
        self.store.set_quota(account, bytes)?;
        match bytes {
            Some(b) => self.files.quotas.insert(account.to_string(), b),
            None => self.files.quotas.remove(account),
        };
        Ok(())
    }

    pub fn storage_policy(&self) -> (StoragePolicy, DiskClass) {
        (self.files.policy, self.files.disk_class)
    }

    pub fn set_storage_policy(&mut self, p: StoragePolicy) -> Result<()> {
        p.validate()?;
        self.store
            .put_meta(STORAGE_POLICY_META, &postcard::to_stdvec(&p)?)?;
        self.files.policy = p;
        Ok(())
    }

    // -- secrets --------------------------------------------------------------

    pub fn set_secret(&mut self, name: &str, value: &str) -> Result<()> {
        validate_secret_name(name)?;
        ensure!(!value.is_empty(), "empty secret");
        ensure!(value.len() <= 64 * 1024, "secret too long");
        if let Some(host) = name.strip_prefix("registry/") {
            ensure!(!host.is_empty(), "name the registry: registry/<host>");
            Credentials::parse(value)?;
        }
        let now = now_unix();
        self.store.put_secret(name, now, Some(value))?;
        self.files
            .secrets
            .insert(name.to_string(), (now, value.to_string()));
        info!(secret = %name, "secret set");
        Ok(())
    }

    pub fn remove_secret(&mut self, name: &str) -> Result<()> {
        if self.files.secrets.remove(name).is_none() {
            bail!("no secret {name}");
        }
        self.store.put_secret(name, 0, None)?;
        Ok(())
    }

    pub fn secrets(&self) -> Vec<SecretView> {
        self.files
            .secrets
            .iter()
            .map(|(n, (t, _))| SecretView {
                name: n.clone(),
                set_unix: *t,
            })
            .collect()
    }

    /// Registry logins from `registry/<host>` secrets, by normalized host.
    pub fn registry_credentials(&self) -> HashMap<String, Credentials> {
        self.files
            .secrets
            .iter()
            .filter_map(|(n, (_, v))| {
                let host = n.strip_prefix("registry/")?;
                Some((normalize_registry(host), Credentials::parse(v).ok()?))
            })
            .collect()
    }

    // -- billing ----------------------------------------------------------------

    /// The CLI downloaded `bytes` paid for by `account`.
    pub fn charge_egress(&mut self, account: &str, bytes: u64) {
        let exact = egress_charge(bytes, self.policy.rates.egress_gib);
        let memo = format!("{} downloaded", pumasi_proto::store::format_size(bytes));
        self.transfer(account, OWNER_ACCOUNT, LedgerKind::Egress, exact, memo);
    }

    /// Charges every account for what it stored since the last time.
    fn bill_storage(&mut self) {
        let now_i = Instant::now();
        if self
            .files
            .last_bill_check
            .is_some_and(|t| now_i.duration_since(t) < Duration::from_millis(200))
        {
            return;
        }
        self.files.last_bill_check = Some(now_i);
        let every = self.files.bill_every.as_secs().max(1);
        let now = now_unix();
        if now < self.files.billed_unix + every {
            return;
        }
        let secs = now - self.files.billed_unix;
        let rate = self
            .files
            .policy
            .rate(&self.policy.rates, self.files.disk_class);
        for (account, (bytes, _)) in self.usage_by_account() {
            let exact = storage_charge(bytes, secs, rate);
            if exact > 0 && account != OWNER_ACCOUNT {
                let memo = format!("{} for {secs}s", pumasi_proto::store::format_size(bytes));
                self.transfer(&account, OWNER_ACCOUNT, LedgerKind::Storage, exact, memo);
            }
        }
        self.files.billed_unix = now;
        if let Err(e) = self.store.put_meta(STORAGE_BILLED_META, &now.to_le_bytes()) {
            warn!("persist storage billing time: {e:#}");
        }
    }

    /// File store housekeeping, from the tick.
    pub(crate) fn tick_files(&mut self) {
        self.finish_jobs();
        let now = now_unix();
        let expired: Vec<BlobHash> = self
            .files
            .uploads
            .iter()
            .filter(|(_, u)| now > u.unix + UPLOAD_TTL_SECS)
            .map(|(r, _)| *r)
            .collect();
        for root in expired {
            self.files.uploads.remove(&root);
            if let Err(e) = self.store.put_upload(&root, None) {
                warn!("persist upload: {e:#}");
            }
            self.store_cmd(StoreCmd::Untag(files::up_tag(&root)));
        }
        self.bill_storage();
    }
}
