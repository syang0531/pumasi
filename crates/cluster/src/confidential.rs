//! The confidential tier on the control plane (phase 16): the key broker.
//!
//! A confidential job's image and each task's input are sealed here, with
//! keys derived from the cluster key, and go to the node only sealed. The
//! node boots them in a SEV-SNP or TDX microVM whose guest attests: the
//! control plane checks the evidence ([`crate::attest`]), and only for a
//! genuine TEE, running a launch measurement an admin trusts, for this
//! very run (its nonce and the guest's fresh key are in the report), does
//! it release the keys, sealed to the guest's key. The guest seals its
//! result to the run; the control plane opens it before it counts.
//!
//! Leases (one per placed run) live in memory: after a restart or a
//! failover, runs in flight are done again.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_guest::{
    RunSpec,
    seal::{self, Kind, SealReader, SealWriter, TaskKeys},
};
use pumasi_proto::{
    blobs,
    confidential::{
        ConfidentialTask, ConfidentialView, Evidence, MIN_MEASUREMENT_PREFIX, MeasurementView,
        SEALED_IMAGE_FILE, SEALED_INPUT_FILE, SEALED_OUTPUT_FILE, SealedBlob, TeeKind,
        TeeNodeView, TeeReport,
    },
    job::{DockerSpec, FileEntry, JobId, MAX_OUTPUT_FILES_BYTES, TaskInput, Tier},
    monitor::EventLevel,
    msg::{Outcome, TaskReport, TaskRef},
    store::{IMAGE_MANIFEST, ImageBlob, ImageManifest, Mount, StoredOutput, image_layer_name},
    vmdisk,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    attest::{Checked, Verifier},
    files::FileStore,
    state::{State, now_unix},
};

/// The one extra file in a sealed image's collection: the image manifest
/// (with its config: entrypoint, environment), sealed with the image key.
pub const SEALED_CONFIG_FILE: &str = "config.sealed";
/// Inputs sealed at once, at most.
const MAX_SEALING: usize = 4;
/// What a confidential run may leave in `/output` when it keeps its
/// outputs (an output dataset): it lives in the VM's memory first.
pub const MAX_CONFIDENTIAL_OUTPUT_BYTES: u64 = 1 << 30;
/// Memory a confidential VM needs besides the task, the image and the
/// input: the guest kernel's share of the tmpfs and the page cache.
const CONF_OVERHEAD_MIB: u64 = 256;

/// A task's sealed input, ready to place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedInput {
    pub image: SealedBlob,
    pub input: SealedBlob,
    pub output_bytes: u64,
    pub extra_mib: u64,
}

/// A placed confidential run: who may attest for it, once.
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    pub node: String,
    pub nonce: [u8; 32],
    pub tee: TeeKind,
    /// The guest attested (whatever the answer): no second try.
    pub used: bool,
    /// Its output goes to the store (an output dataset).
    pub keep: bool,
}

/// A launch measurement seen in a checked attestation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementRecord {
    pub tee: TeeKind,
    pub trusted: bool,
    pub note: String,
    pub node: String,
    pub agent_version: String,
    pub vcpus: u32,
    pub first_seen_unix: u64,
    pub last_seen_unix: u64,
    pub seen: u64,
    pub detail: String,
}

/// The confidential part of [`State`].
#[derive(Default)]
pub(crate) struct ConfState {
    /// The control plane accepts the test stand-in for a TEE.
    pub test_tee: bool,
    pub verifier: Option<Arc<Verifier>>,
    /// Sealed inputs, by (job, task). Kept in memory: sealed again after a
    /// restart.
    pub inputs: HashMap<(JobId, u32), SealedInput>,
    pub sealing: HashSet<(JobId, u32)>,
    pub leases: HashMap<TaskRef, Lease>,
    /// What each node reported it can start (protocol 14).
    pub tees: HashMap<String, TeeReport>,
    /// Launch measurements by id (persisted).
    pub measurements: BTreeMap<String, MeasurementRecord>,
    /// Providers' nodes an admin trusts with `tier = "trusted"` jobs
    /// (persisted).
    pub trusted_nodes: BTreeSet<String>,
}

/// What [`State::conf_need`] found for a queued confidential run.
#[derive(Debug, Clone)]
pub(crate) enum ConfNeed {
    /// Its image or input is still being sealed (or waits its turn).
    Sealing(&'static str),
    Ready(SealedInput),
}

/// What the store worker needs to seal one task's input.
#[derive(Debug, Clone)]
pub struct SealInputJob {
    pub job: JobId,
    pub index: u32,
    pub job_key: [u8; 32],
    pub image: BlobHash,
    pub spec: DockerSpec,
    pub input: TaskInput,
    pub shared: Vec<FileEntry>,
    pub mounts: Vec<Mount>,
    pub keep: bool,
}

use pumasi_proto::store::BlobHash;

/// What the control plane needs to open a run's sealed output.
#[derive(Clone)]
pub struct OpenInfo {
    pub task_key: [u8; 32],
    pub nonce: [u8; 32],
    pub keep: bool,
    pub memory_bytes: u64,
}

impl ConfState {
    /// The TEEs a node can be given work in, best first.
    pub fn usable_tees(&self, node: &str) -> Vec<TeeKind> {
        let Some(r) = self.tees.get(node) else {
            return vec![];
        };
        let mut v: Vec<TeeKind> = r
            .tees
            .iter()
            .copied()
            .filter(|t| *t != TeeKind::Test || self.test_tee)
            .collect();
        v.sort_by_key(|t| *t == TeeKind::Test);
        v.dedup();
        v
    }
}

/// The key a confidential job's image and inputs are sealed with, derived
/// from the cluster key (the same on every control plane replica).
pub fn job_key(cluster_key: &iroh::SecretKey, job: JobId) -> [u8; 32] {
    let mut ikm = cluster_key.to_bytes().to_vec();
    ikm.extend_from_slice(&job.0.to_le_bytes());
    blake3::derive_key("pumasi 2026 confidential job key v1", &ikm)
}

fn short(id: &str) -> &str {
    &id[..id.len().min(16)]
}

// ---------------------------------------------------------------------------
// Sealing (the store worker, off the state lock)
// ---------------------------------------------------------------------------

/// Seals a pulled image: its layers as one sealed tar and its manifest as
/// a sealed file, tagged `tag` in place of the plain collection (which
/// the store then drops).
pub async fn seal_image(
    files: &FileStore,
    plain: &ImageBlob,
    job_key: &[u8; 32],
    tag: &str,
) -> Result<ImageBlob> {
    let entries = files.entries(&plain.root).await?;
    let work = files.scratch()?;
    let res: Result<ImageBlob> = async {
        blobs::export(files.api(), &entries, &work, false, |n| Some(n.to_string())).await?;
        let manifest_path = work.join(IMAGE_MANIFEST);
        let manifest: ImageManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path)?).context("image manifest")?;
        let layers: Vec<PathBuf> = (0..manifest.layers.len())
            .map(|i| work.join(image_layer_name(i)))
            .collect();
        let key = seal::image_key(job_key);
        let (image, config) = (work.join(SEALED_IMAGE_FILE), work.join(SEALED_CONFIG_FILE));
        {
            let (image, config) = (image.clone(), config.clone());
            tokio::task::spawn_blocking(move || -> Result<()> {
                let out = std::fs::File::create(&image)?;
                let w = SealWriter::new(out, &key, Kind::Layers, b"")?;
                vmdisk::write_layers_tar(w, &layers)?.finish()?;
                let mut src = std::fs::File::open(&manifest_path)?;
                seal::seal_all(
                    &mut src,
                    std::fs::File::create(&config)?,
                    &key,
                    Kind::Input,
                    b"config",
                )?;
                Ok(())
            })
            .await??;
        }
        let stored = files
            .import(
                &[
                    (SEALED_CONFIG_FILE.to_string(), config),
                    (SEALED_IMAGE_FILE.to_string(), image),
                ],
                tag,
            )
            .await?;
        Ok(ImageBlob {
            root: stored.root,
            digest: plain.digest.clone(),
            reference: plain.reference.clone(),
        })
    }
    .await;
    let _ = std::fs::remove_dir_all(&work);
    res
}

/// The one file `name` of a collection, exported to `dir`.
async fn export_one(files: &FileStore, root: &BlobHash, name: &str, dir: &Path) -> Result<u64> {
    let entries = files.entries(root).await?;
    let e = entries
        .iter()
        .find(|e| e.name == name)
        .with_context(|| format!("no {name} in the collection"))?;
    blobs::export(files.api(), std::slice::from_ref(e), dir, false, |n| {
        Some(n.to_string())
    })
    .await
}

/// Builds and seals one task's input: the run spec (from the image's
/// sealed config and the job), stdin, the job's and task's files and its
/// mounts' files, tagged `tag`.
pub async fn seal_input(files: &FileStore, j: SealInputJob, tag: &str) -> Result<SealedInput> {
    let work = files.scratch()?;
    let res: Result<SealedInput> = async {
        let image_size = export_one(files, &j.image, SEALED_IMAGE_FILE, &work.join("img")).await?;
        export_one(files, &j.image, SEALED_CONFIG_FILE, &work).await?;
        let image_key = seal::image_key(&j.job_key);
        let manifest: ImageManifest = {
            let f = std::fs::File::open(work.join(SEALED_CONFIG_FILE))?;
            let mut r = SealReader::new(f, &image_key, Kind::Input, b"config")
                .context("open the sealed image config")?;
            let mut b = Vec::new();
            r.read_to_end(&mut b)?;
            serde_json::from_slice(&b).context("image manifest")?
        };
        let _ = std::fs::remove_dir_all(work.join("img"));
        // The mounts' files, placed as an agent would.
        let staged_dir = work.join("staged");
        std::fs::create_dir_all(&staged_dir)?;
        for m in &j.mounts {
            let entries = files.entries(&m.root).await?;
            let picked = entries.iter().filter(|e| m.selects(&e.name)).count();
            if let Some(sel) = &m.select
                && picked == 0
            {
                bail!("{} has no file or directory {sel}", m.label);
            }
            blobs::export(files.api(), &entries, &staged_dir, false, |name| {
                m.selects(name).then(|| m.place(name))
            })
            .await
            .with_context(|| format!("place {} in /input", m.label))?;
        }
        let mut spec: RunSpec = vmdisk::run_spec(&manifest.config, &j.spec, &j.input);
        if j.keep {
            spec.output_limit = MAX_CONFIDENTIAL_OUTPUT_BYTES;
            spec.output_files = 1_000_000;
        }
        let output_bytes = seal::sealed_len(spec.output_disk_bytes()).div_ceil(1 << 20) << 20;
        let input_path = work.join(SEALED_INPUT_FILE);
        let task_key = seal::task_key(&j.job_key, j.index);
        let input_size = {
            let (input_path, spec) = (input_path.clone(), spec.clone());
            let (stdin, shared, own) = (j.input.stdin.clone(), j.shared, j.input.files.clone());
            tokio::task::spawn_blocking(move || -> Result<u64> {
                let staged = pumasi_proto::job::dir_files(&staged_dir)?;
                let out = std::fs::File::create(&input_path)?;
                let w = SealWriter::new(out, &task_key, Kind::Input, b"")?;
                let w =
                    vmdisk::write_input_tar(w, &spec, &stdin, &shared, &own, &staged)?;
                Ok(w.finish()?.1)
            })
            .await??
        };
        let stored = files
            .import(&[(SEALED_INPUT_FILE.to_string(), input_path)], tag)
            .await?;
        // The image unpacks to a few times its compressed size, and the
        // input and outputs sit in memory beside it.
        let output_mem = if j.keep {
            MAX_CONFIDENTIAL_OUTPUT_BYTES
        } else {
            MAX_OUTPUT_FILES_BYTES as u64
        };
        let extra = 3 * image_size + 2 * input_size + output_mem;
        Ok(SealedInput {
            image: SealedBlob {
                root: j.image,
                size: image_size,
            },
            input: SealedBlob {
                root: stored.root,
                size: input_size,
            },
            output_bytes,
            extra_mib: extra.div_ceil(1 << 20) + CONF_OVERHEAD_MIB,
        })
    }
    .await;
    let _ = std::fs::remove_dir_all(&work);
    res
}

/// A confidential run's report as the guest made it: the sealed output
/// opened, its outcome, output and files in place of what the node said.
pub struct Opened {
    pub report: TaskReport,
    pub stored: Option<StoredOutput>,
}

/// Opens a confidential run's sealed output (`report.files` or the
/// stored collection). A run with no sealed output failed before its keys
/// came (nothing confidential happened), so it can only be an error. A
/// sealed output that does not open is the node's doing: `Err`.
pub async fn open_output(
    files: &FileStore,
    info: &OpenInfo,
    report: &TaskReport,
    stored: Option<&StoredOutput>,
    tag: &str,
) -> Result<Opened> {
    let work = files.scratch()?;
    let res: Result<Opened> = async {
        let sealed_path = work.join(SEALED_OUTPUT_FILE);
        let have = if let Some(f) = report.files.iter().find(|f| f.path == SEALED_OUTPUT_FILE) {
            std::fs::write(&sealed_path, &f.data)?;
            true
        } else if let Some(s) = stored {
            let entries = files.entries(&s.root).await?;
            match entries.iter().find(|e| e.name == SEALED_OUTPUT_FILE) {
                Some(e) => {
                    blobs::export(files.api(), std::slice::from_ref(e), &work, false, |n| {
                        Some(n.to_string())
                    })
                    .await?;
                    true
                }
                None => false,
            }
        } else {
            false
        };
        let mut out = report.clone();
        if !have {
            out.outcome = match &report.outcome {
                Outcome::Error(e) => Outcome::Error(e.clone()),
                Outcome::Cancelled => Outcome::Cancelled,
                Outcome::Timeout => Outcome::Timeout,
                _ => Outcome::Error("the node sent no sealed result".into()),
            };
            out.stdout.clear();
            out.files.clear();
            return Ok(Opened {
                report: out,
                stored: None,
            });
        }
        let kept = work.join("out");
        std::fs::create_dir_all(&kept)?;
        let guest = {
            let (info, kept, keep) = (info.clone(), kept.clone(), info.keep);
            tokio::task::spawn_blocking(move || -> Result<vmdisk::GuestOutput> {
                let f = std::fs::File::open(&sealed_path)?;
                let r = SealReader::new(f, &info.task_key, Kind::Output, &info.nonce)
                    .context("the sealed output is not this run's")?;
                let mut r = r;
                let g = vmdisk::parse_output(&mut r, keep.then_some(kept.as_path()))
                    .context("the sealed output does not open")?;
                // Read to the end: the last chunk's tag proves nothing was
                // cut off.
                std::io::copy(&mut r, &mut std::io::sink())
                    .context("the sealed output does not open")?;
                Ok(g)
            })
            .await??
        };
        let mut stderr = guest.stderr;
        vmdisk::note_dropped(&mut stderr, &guest.result);
        out.outcome = vmdisk::outcome_of(&guest.result.outcome, info.memory_bytes);
        out.stdout = guest.stdout;
        out.stderr = stderr;
        out.files = guest.files;
        let stored = if info.keep {
            let list: Vec<(String, PathBuf)> = pumasi_proto::job::dir_files(&kept)?
                .into_iter()
                .map(|(rel, p, _)| (rel, p))
                .collect();
            Some(files.import(&list, tag).await?)
        } else {
            None
        };
        Ok(Opened { report: out, stored })
    }
    .await;
    let _ = std::fs::remove_dir_all(&work);
    res
}

// ---------------------------------------------------------------------------
// The state machine's side
// ---------------------------------------------------------------------------

impl State {
    pub(crate) fn job_key(&self, job: JobId) -> [u8; 32] {
        job_key(&self.key, job)
    }

    /// Accept the test stand-in for a TEE (`--insecure-test-tee`), and
    /// check evidence with `verifier`.
    pub fn attach_verifier(&mut self, verifier: Arc<Verifier>) {
        self.conf.test_tee = verifier.test_tee();
        self.conf.verifier = Some(verifier);
    }

    pub(crate) fn load_conf(
        &mut self,
        measurements: Vec<(String, MeasurementRecord)>,
        trusted_nodes: Vec<String>,
    ) {
        self.conf.measurements = measurements.into_iter().collect();
        self.conf.trusted_nodes = trusted_nodes.into_iter().collect();
    }

    /// Runs `tier = "trusted"` jobs: one of the owner's machines, or a
    /// provider's an admin trusts.
    pub(crate) fn trusted_host(&self, node: &str) -> bool {
        self.members.get(node).is_some_and(|m| m.trusted())
            || self.conf.trusted_nodes.contains(node)
    }

    /// Whether a queued confidential run can be placed: its image sealed
    /// (asked for here), then its input (sealed a few at a time).
    pub(crate) fn conf_need(&mut self, job: JobId, index: u32) -> ConfNeed {
        if let Some(s) = self.conf.inputs.get(&(job, index)) {
            return ConfNeed::Ready(s.clone());
        }
        let image = self.want_images(job).into_iter().find(|(a, _)| a == "amd64");
        let Some((_, image)) = image else {
            return ConfNeed::Sealing("sealing the image");
        };
        if self.conf.sealing.contains(&(job, index)) {
            return ConfNeed::Sealing("sealing the input");
        }
        if self.conf.sealing.len() >= MAX_SEALING {
            return ConfNeed::Sealing("waiting to seal the input");
        }
        let Some(j) = self.jobs.get(&job) else {
            return ConfNeed::Sealing("gone");
        };
        let Some(task) = j.tasks.get(index as usize) else {
            return ConfNeed::Sealing("gone");
        };
        let shared = j
            .meta
            .files_hash
            .and_then(|h| self.modules.get(&h))
            .and_then(|b| postcard::from_bytes::<Vec<FileEntry>>(b).ok())
            .unwrap_or_default();
        let req = SealInputJob {
            job,
            index,
            job_key: self.job_key(job),
            image: image.root,
            spec: j.meta.docker.clone().unwrap_or_default(),
            input: task.input.clone(),
            shared,
            mounts: j
                .meta
                .ext
                .mounts
                .iter()
                .chain(task.ext.mounts.iter())
                .cloned()
                .collect(),
            keep: j.meta.ext.output_dataset.is_some(),
        };
        self.conf.sealing.insert((job, index));
        self.store_cmd(crate::datasets::StoreCmd::SealInput(Box::new(req)));
        ConfNeed::Sealing("sealing the input")
    }

    /// The worker sealed (or failed to seal) a task's input.
    pub fn on_input_sealed(&mut self, job: JobId, index: u32, r: Result<SealedInput>) {
        self.conf.sealing.remove(&(job, index));
        match r {
            Ok(s) => {
                if self.jobs.contains_key(&job) {
                    self.conf.inputs.insert((job, index), s);
                }
            }
            Err(e) => {
                let msg = format!("seal the input: {e:#}");
                warn!(job = %job, index, "{msg}");
                self.fail_pending(job, index, &msg);
            }
        }
        self.schedule();
    }

    /// Fails a task that is still waiting to run.
    fn fail_pending(&mut self, job: JobId, index: u32, msg: &str) {
        let Some(task) = self
            .jobs
            .get_mut(&job)
            .and_then(|j| j.tasks.get_mut(index as usize))
        else {
            return;
        };
        if task.state != pumasi_proto::msg::TaskState::Pending {
            return;
        }
        task.state = pumasi_proto::msg::TaskState::Failed(Outcome::Error(msg.to_string()));
        if let Err(e) = self.store.put_tasks(job.0, &[(index, &*task)]) {
            warn!("persist task: {e:#}");
        }
        self.queue.retain(|q| !(q.job == job && q.index == index));
    }

    /// Forgets the sealed inputs of a job that ended.
    pub(crate) fn conf_job_ended(&mut self, job: JobId) {
        self.conf.inputs.retain(|(j, _), _| *j != job);
    }

    /// The confidential part of an assignment, and a lease for its run.
    pub(crate) fn conf_assign(
        &mut self,
        r: TaskRef,
        node: &str,
        sealed: &SealedInput,
        keep: bool,
    ) -> Option<ConfidentialTask> {
        let tee = *self.conf.usable_tees(node).first()?;
        let mut nonce = [0u8; 32];
        seal::random(&mut nonce);
        self.conf.leases.insert(
            r,
            Lease {
                node: node.to_string(),
                nonce,
                tee,
                used: false,
                keep,
            },
        );
        Some(ConfidentialTask {
            tee,
            nonce,
            image: sealed.image,
            input: sealed.input,
            output_bytes: sealed.output_bytes,
            extra_mib: sealed.extra_mib,
        })
    }

    /// Drops the leases of runs no node is running any more.
    pub(crate) fn sweep_leases(&mut self) {
        let nodes = &self.nodes;
        self.conf.leases.retain(|r, l| {
            nodes
                .get(&l.node)
                .is_some_and(|n| n.running.contains_key(r))
        });
    }

    /// A node told which TEEs it starts confidential VMs in.
    pub fn on_tee(&mut self, node: &str, generation: u64, report: TeeReport) {
        if self.nodes.get(node).map(|n| n.generation) != Some(generation) {
            return;
        }
        if !report.tees.is_empty() {
            info!(node = %node, tees = ?report.tees, "confidential VMs available");
        }
        self.conf.tees.insert(node.to_string(), report);
        self.schedule();
    }

    /// The first half of a key request: the lease must be this node's,
    /// for a run it is running, not used before, and for the TEE it was
    /// placed for. Uses the lease up. Returns the report data the evidence
    /// must carry and the verifier.
    pub fn begin_attest(
        &mut self,
        node: &str,
        generation: u64,
        task: TaskRef,
        ev: &Evidence,
    ) -> std::result::Result<([u8; 64], Arc<Verifier>), String> {
        let n = self
            .nodes
            .get(node)
            .filter(|n| n.generation == generation)
            .ok_or("stale session")?;
        if !n.running.contains_key(&task) {
            return Err("the run is not running on this node".into());
        }
        let verifier = self.conf.verifier.clone().ok_or("no verifier")?;
        let lease = self
            .conf
            .leases
            .get_mut(&task)
            .ok_or("no key lease for this run (the control plane restarted?)")?;
        if lease.node != node {
            return Err("the run was placed on another node".into());
        }
        if lease.used {
            return Err("this run already asked for its keys".into());
        }
        lease.used = true;
        if ev.tee != lease.tee {
            return Err(format!(
                "evidence from {} for a run placed in {}",
                ev.tee, lease.tee
            ));
        }
        Ok((seal::report_data(&lease.nonce, &ev.key), verifier))
    }

    /// The second half: records the measurement and releases the keys,
    /// sealed to the guest's key, when the evidence checked out and an
    /// admin trusts the measurement.
    pub fn finish_attest(
        &mut self,
        node: &str,
        task: TaskRef,
        ev: &Evidence,
        checked: Result<Checked>,
    ) -> std::result::Result<Vec<u8>, String> {
        let r = self.finish_attest_inner(node, task, ev, checked);
        match &r {
            Ok(_) => self.event(
                EventLevel::Info,
                "confidential.keys",
                Some(node),
                Some(task.job),
                format!("keys released to task {}/{}", task.job, task.index),
            ),
            Err(why) => {
                warn!(node = %node, task = %task, "keys refused: {why}");
                self.event(
                    EventLevel::Warning,
                    "confidential.refused",
                    Some(node),
                    Some(task.job),
                    format!("keys refused for task {}/{}: {why}", task.job, task.index),
                );
            }
        }
        r
    }

    fn finish_attest_inner(
        &mut self,
        node: &str,
        task: TaskRef,
        ev: &Evidence,
        checked: Result<Checked>,
    ) -> std::result::Result<Vec<u8>, String> {
        let checked = checked.map_err(|e| format!("the attestation does not check out: {e:#}"))?;
        let lease = self
            .conf
            .leases
            .get(&task)
            .filter(|l| l.node == node)
            .cloned()
            .ok_or("the run ended")?;
        let (agent_version, vcpus, name) = self
            .nodes
            .get(node)
            .map(|n| (n.info.agent_version.clone(), n.info.cpus, n.info.name.clone()))
            .unwrap_or_default();
        let vcpus = self
            .jobs
            .get(&task.job)
            .map(|j| pumasi_proto::economy::vcpus(j.meta.limits.cpu_millis))
            .unwrap_or(vcpus);
        let now = now_unix();
        let id = checked.measurement.clone();
        let rec = self
            .conf
            .measurements
            .entry(id.clone())
            .or_insert_with(|| MeasurementRecord {
                tee: checked.tee,
                trusted: false,
                note: String::new(),
                node: name.clone(),
                agent_version: agent_version.clone(),
                vcpus,
                first_seen_unix: now,
                last_seen_unix: now,
                seen: 0,
                detail: String::new(),
            });
        let new = rec.seen == 0;
        rec.seen += 1;
        rec.last_seen_unix = now;
        rec.node = name;
        rec.agent_version = agent_version;
        rec.vcpus = vcpus;
        rec.detail = format!("{} ({})", checked.detail, checked.chip);
        let trusted = rec.trusted && rec.tee == checked.tee;
        let rec = rec.clone();
        if let Err(e) = self.store.put_measurement(&id, Some(&rec)) {
            warn!("persist measurement: {e:#}");
        }
        if !trusted {
            if new {
                self.event(
                    EventLevel::Warning,
                    "confidential.measurement",
                    Some(node),
                    Some(task.job),
                    format!(
                        "new {} launch measurement {}: trust it with `pumasi confidential trust {}` if this agent's guest is the one you expect",
                        checked.tee,
                        short(&id),
                        short(&id)
                    ),
                );
            }
            return Err(format!(
                "launch measurement {} is not trusted (`pumasi confidential trust {}`)",
                short(&id),
                short(&id)
            ));
        }
        let job_key = self.job_key(task.job);
        let keys = TaskKeys {
            image: seal::image_key(&job_key),
            task: seal::task_key(&job_key, task.index),
        };
        seal::wrap_keys(&ev.key, &lease.nonce, &keys).map_err(|e| format!("wrap the keys: {e}"))
    }

    /// What opening a confidential run's output needs, when the run is a
    /// confidential job's and its lease is still here.
    pub fn open_info(&self, task: &TaskRef) -> Option<Option<OpenInfo>> {
        let job = self.jobs.get(&task.job)?;
        if job.meta.tier != Tier::Confidential {
            return None;
        }
        Some(self.conf.leases.get(task).map(|l| OpenInfo {
            task_key: seal::task_key(&self.job_key(task.job), task.index),
            nonce: l.nonce,
            keep: l.keep,
            memory_bytes: job.meta.limits.memory_bytes,
        }))
    }

    pub fn confidential_view(&self) -> ConfidentialView {
        let mut nodes: Vec<TeeNodeView> = self
            .members
            .values()
            .map(|m| {
                let r = self.conf.tees.get(&m.id);
                TeeNodeView {
                    id: m.id.clone(),
                    name: m.info.name.clone(),
                    online: self.nodes.get(&m.id).is_some_and(|n| n.online),
                    tees: self.conf.usable_tees(&m.id),
                    unavailable: r.and_then(|r| r.unavailable.clone()),
                    trusted_host: self.trusted_host(&m.id),
                    trusted_by_admin: self.conf.trusted_nodes.contains(&m.id),
                }
            })
            .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        let mut measurements: Vec<MeasurementView> = self
            .conf
            .measurements
            .iter()
            .map(|(id, m)| MeasurementView {
                id: id.clone(),
                tee: m.tee,
                trusted: m.trusted,
                note: m.note.clone(),
                node: m.node.clone(),
                agent_version: m.agent_version.clone(),
                vcpus: m.vcpus,
                first_seen_unix: m.first_seen_unix,
                last_seen_unix: m.last_seen_unix,
                seen: m.seen,
                detail: m.detail.clone(),
            })
            .collect();
        measurements.sort_by_key(|m| std::cmp::Reverse(m.last_seen_unix));
        ConfidentialView {
            test_tee: self.conf.test_tee,
            nodes,
            measurements,
        }
    }

    /// Trusts (or stops trusting) a measurement, by id or unique prefix.
    pub fn trust_measurement(&mut self, id: &str, trusted: bool, note: &str) -> Result<String> {
        let id = id.trim().to_ascii_lowercase();
        ensure!(
            id.len() >= MIN_MEASUREMENT_PREFIX,
            "give at least {MIN_MEASUREMENT_PREFIX} characters of the measurement"
        );
        let found: Vec<&String> = self
            .conf
            .measurements
            .keys()
            .filter(|k| k.starts_with(&id))
            .collect();
        let full = match found.as_slice() {
            [one] => (*one).clone(),
            [] => bail!(
                "no measurement {id} was seen; it shows up in `pumasi confidential` after a confidential task attested with it"
            ),
            _ => bail!("{id} matches several measurements; give more of it"),
        };
        let rec = self.conf.measurements.get_mut(&full).expect("found");
        rec.trusted = trusted;
        if !note.is_empty() || !trusted {
            rec.note = note.to_string();
        }
        let rec = rec.clone();
        self.store.put_measurement(&full, Some(&rec))?;
        self.event(
            EventLevel::Info,
            "confidential.trust",
            None,
            None,
            format!(
                "{} launch measurement {} {}",
                rec.tee,
                short(&full),
                if trusted { "trusted" } else { "no longer trusted" }
            ),
        );
        Ok(full)
    }

    /// Lets a provider's node run `tier = "trusted"` jobs, or stops it.
    pub fn trust_node(&mut self, node: &str, trusted: bool) -> Result<()> {
        let id = self.resolve_node(node)?;
        let changed = if trusted {
            self.conf.trusted_nodes.insert(id.clone())
        } else {
            self.conf.trusted_nodes.remove(&id)
        };
        if changed {
            self.store.put_trusted_node(&id, trusted)?;
            let name = self.node_name(&id);
            self.event(
                EventLevel::Info,
                "node.trust",
                Some(&id),
                None,
                format!(
                    "{name} {} trusted jobs",
                    if trusted { "now runs" } else { "no longer runs" }
                ),
            );
            self.schedule();
        }
        Ok(())
    }
}
