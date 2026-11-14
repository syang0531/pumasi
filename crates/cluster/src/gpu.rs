//! GPUs on the control plane: nodes' signed device profiles, the FP32
//! GFLOPS each GPU is billed at, and the GPU challenge that checks a
//! provider's claim.
//!
//! A provider could claim a faster GPU than it has, or a GPU it does not
//! have, to earn more CU per hour. As with CPUs (`perf.rs`):
//!
//! 1. Until the cluster has timed a provider GPU, it is billed at most
//!    [`REFERENCE_GPU_GFLOPS`].
//! 2. A challenge: when an untrusted node reports a GPU (and, at random,
//!    after GPU runs, at the result-check rate), the cluster waits until
//!    the GPU is idle and sends a proof of work for it
//!    ([`pumasi_proto::gpu`]). Finding the solutions takes a known number
//!    of hashes on average, so the time the answer takes gives the GPU's
//!    hash rate, which the cluster compares with the rate the node
//!    measured itself. Checking the answer costs 64 hashes. The GPU is
//!    then billed at no more than 1.25 times what the cluster timed; a
//!    wrong answer is a strike, and so is a claim over twice the timed
//!    rate twice in a row.
//!
//! Results of GPU jobs are not deterministic, so they are not re-run on
//! other nodes; these random challenges, whose answers can be checked, are
//! what keeps provider GPUs honest.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use pumasi_proto::{
    gpu::{
        GPU_CHALLENGE_SOLUTIONS, GPU_SHARE_WHOLE, GpuChallenge, GpuExt, GpuInfo, GpuUse,
        REFERENCE_GPU_GFLOPS, SignedDevices, check_answer, expected_hashes, target_for,
    },
    job::Executor,
    msg::{GpuView, TaskRef, ToAgent, WebGpuView},
    receipt::GpuBilling,
    webgpu::{
        SignedWebGpu, SignedWebGpuExtra, UNCHECKED_WEBGPU_GFLOPS, WEBGPU_SLOT_BASE, WebGpuAdapter,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{Member, State, now_unix};

/// Target length of a GPU challenge at the claimed rate.
const CHALLENGE_MS: u64 = 500;
/// A challenge not answered in this long counts as answered at this time.
pub(crate) const CHALLENGE_TIMEOUT: Duration = Duration::from_secs(60);
const SLACK_PERMILLE: u64 = 1250;
const INFLATED: u64 = 2;
const STRIKE_AFTER: u32 = 2;
const RETRY_GAP: Duration = Duration::from_secs(5);
/// Random re-checks after GPU runs: at most one per GPU in this time.
const SAMPLE_GAP: Duration = Duration::from_secs(600);

/// A node's latest device profile and the cluster's checks of its GPUs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecord {
    pub signed: SignedDevices,
    pub checks: Vec<GpuCheck>,
}

impl DeviceRecord {
    pub fn gpus(&self) -> &[GpuInfo] {
        &self.signed.profile.gpus
    }

    fn check(&self, g: &GpuInfo) -> Option<&GpuCheck> {
        self.checks
            .iter()
            .find(|c| c.index == g.index && c.claimed_hash_rate == g.hash_rate)
    }
}

/// A node's latest WebGPU extras (1.10) and the cluster's checks of its
/// adapters that are not listed GPUs, by slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebGpuExtraRecord {
    pub signed: SignedWebGpuExtra,
    pub checks: Vec<GpuCheck>,
}

impl WebGpuExtraRecord {
    /// The adapter's hash rate over WebGPU as the node measured it.
    pub fn hash_rate(&self, a: &WebGpuAdapter) -> u64 {
        self.signed.extra.get(a.index).map_or(0, |x| x.hash_rate)
    }

    /// The check of the adapter's current claim.
    pub fn check(&self, a: &WebGpuAdapter) -> Option<&GpuCheck> {
        let claimed = self.hash_rate(a);
        self.checks
            .iter()
            .find(|c| c.index == a.slot() && c.claimed_hash_rate == claimed)
    }
}

/// The cluster's own timing of one GPU's challenge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuCheck {
    pub index: u32,
    /// The profile's hash rate at the time.
    pub claimed_hash_rate: u64,
    /// The best rate the cluster timed for this claim.
    pub measured_hash_rate: u64,
    /// The answer was right (or did not come in time).
    pub correct: bool,
    pub attempts: u32,
    pub unix: u64,
}

/// A GPU challenge sent and not answered yet.
#[derive(Debug, Clone)]
pub(crate) struct PendingGpu {
    pub challenge: GpuChallenge,
    pub sent: Instant,
    pub claimed: u64,
}

/// Per-connection GPU state.
#[derive(Debug, Default)]
pub(crate) struct GpuRuntime {
    /// GPUs (by index) the cluster wants to time.
    pub wanted: Vec<u32>,
    pub pending: Option<PendingGpu>,
    pub last: Option<Instant>,
    /// When each GPU was last picked for a random re-check.
    pub sampled: Vec<(u32, Instant)>,
    /// GPUs in use, by run.
    pub busy: std::collections::HashMap<TaskRef, Vec<u32>>,
    /// Runs that hold part of their GPU (1.10), in hundredths; the others
    /// in `busy` hold their GPUs whole.
    pub shares: std::collections::HashMap<TaskRef, u32>,
}

impl GpuRuntime {
    /// Every GPU in use, whole or in part.
    pub fn busy_set(&self) -> HashSet<u32> {
        self.busy.values().flatten().copied().collect()
    }

    /// What the node's GPUs hold.
    pub fn usage(&self) -> GpuUse {
        gpu_use(self, &[], &[])
    }

    /// The GPUs a run holds, each with its part in hundredths
    /// ([`GPU_SHARE_WHOLE`] for a whole GPU).
    pub fn held(&self, t: &TaskRef) -> Vec<(u32, u32)> {
        let share = self.shares.get(t).copied().unwrap_or(GPU_SHARE_WHOLE);
        self.busy
            .get(t)
            .map(|g| g.iter().map(|i| (*i, share)).collect())
            .unwrap_or_default()
    }

    /// A run takes `gpus` (part `share` of its one GPU when not 0).
    pub fn hold(&mut self, t: TaskRef, gpus: Vec<u32>, share: u32) {
        if gpus.is_empty() {
            return;
        }
        if share > 0 && share < GPU_SHARE_WHOLE {
            self.shares.insert(t, share);
        }
        self.busy.insert(t, gpus);
    }

    /// A run gives its GPUs back: which they were, and its part (0 when
    /// it held them whole).
    pub fn release(&mut self, t: &TaskRef) -> (Vec<u32>, u32) {
        let share = self.shares.remove(t).unwrap_or(0);
        (self.busy.remove(t).unwrap_or_default(), share)
    }

    /// GPU tasks wait while a GPU of the node is being timed.
    pub fn checking(&self) -> bool {
        !self.wanted.is_empty() || self.pending.is_some()
    }
}

/// What a node's GPUs hold with some runs' GPUs given back (`freed`) and
/// planned runs' taken (`taken`), each `(index, hundredths)`.
pub(crate) fn gpu_use(n: &GpuRuntime, freed: &[(u32, u32)], taken: &[(u32, u32)]) -> GpuUse {
    // Per GPU: runs holding it whole, and the hundredths held in parts.
    let mut held: std::collections::HashMap<u32, (i64, i64)> = Default::default();
    let mut add = |i: u32, share: u32, sign: i64| {
        let e = held.entry(i).or_default();
        if share >= GPU_SHARE_WHOLE {
            e.0 += sign;
        } else {
            e.1 += sign * share as i64;
        }
    };
    for t in n.busy.keys() {
        for (i, share) in n.held(t) {
            add(i, share, 1);
        }
    }
    for &(i, share) in freed {
        add(i, share, -1);
    }
    for &(i, share) in taken {
        add(i, share, 1);
    }
    let mut u = GpuUse::default();
    for (i, (whole, part)) in held {
        if whole > 0 {
            u.whole.insert(i);
        }
        if part > 0 {
            u.shared.insert(i, part.min(GPU_SHARE_WHOLE as i64) as u32);
        }
    }
    u
}

/// The FP32 GFLOPS a GPU's runs are billed at, and why it is not simply
/// the profile's.
pub fn billed_gflops(m: &Member, rec: &DeviceRecord, g: &GpuInfo) -> (u32, Option<String>) {
    if m.trusted() {
        return (g.fp32_gflops, None);
    }
    let capped = |why: &str| {
        if g.fp32_gflops > REFERENCE_GPU_GFLOPS {
            (
                REFERENCE_GPU_GFLOPS,
                Some(format!(
                    "{why}: billed at {} TFLOPS",
                    pumasi_proto::gpu::format_tflops(REFERENCE_GPU_GFLOPS)
                )),
            )
        } else {
            (g.fp32_gflops, None)
        }
    };
    if g.hash_rate == 0 {
        return capped("cannot be checked (no GPU challenge on this GPU)");
    }
    match rec.check(g) {
        Some(c) if !c.correct => (
            0,
            Some("gave a wrong answer to the cluster's GPU check".into()),
        ),
        Some(c) => timed_cap(g.fp32_gflops, g.hash_rate, c),
        None => capped("not confirmed yet"),
    }
}

/// FP32 GFLOPS billed at no more than [`SLACK_PERMILLE`] of the rate the
/// cluster timed for the claimed hash rate.
fn timed_cap(gflops: u32, claimed: u64, c: &GpuCheck) -> (u32, Option<String>) {
    let allowed = (c.measured_hash_rate as u128 * SLACK_PERMILLE as u128 / 1000).max(1);
    if claimed as u128 > allowed {
        (
            (gflops as u128 * allowed / claimed as u128) as u32,
            Some(format!(
                "capped: the cluster timed {} hashes/s, the profile claims {claimed}",
                c.measured_hash_rate
            )),
        )
    } else {
        (gflops, None)
    }
}

/// The FP32 GFLOPS a WebGPU adapter's runs are billed at. An untrusted
/// node's adapter is billed at most [`UNCHECKED_WEBGPU_GFLOPS`] until the
/// cluster has timed it: as the listed GPU it is, or (1.10) with a GPU
/// challenge over WebGPU itself. The checked figure then caps it.
pub fn billed_webgpu_gflops(
    m: &Member,
    rec: Option<&DeviceRecord>,
    extra: Option<&WebGpuExtraRecord>,
    a: &WebGpuAdapter,
) -> (u32, Option<String>) {
    if m.trusted() {
        return (a.fp32_gflops, None);
    }
    if a.gpu.is_none()
        && let Some((x, c)) = extra.and_then(|x| x.check(a).map(|c| (x, c)))
    {
        if !c.correct {
            return (
                0,
                Some("gave a wrong answer to the cluster's GPU check".into()),
            );
        }
        return timed_cap(a.fp32_gflops, x.hash_rate(a), c);
    }
    let linked = a
        .gpu
        .and_then(|i| rec.and_then(|r| r.gpus().iter().find(|g| g.index == i).map(|g| (r, g))));
    if let Some((r, g)) = linked
        && r.check(g).is_some_and(|c| c.correct)
    {
        let (cap, note) = billed_gflops(m, r, g);
        return if a.fp32_gflops > cap {
            (cap, note)
        } else {
            (a.fp32_gflops, None)
        };
    }
    if linked.is_some_and(|(r, g)| r.check(g).is_some_and(|c| !c.correct)) {
        return (
            0,
            Some("gave a wrong answer to the cluster's GPU check".into()),
        );
    }
    if a.fp32_gflops > UNCHECKED_WEBGPU_GFLOPS {
        (
            UNCHECKED_WEBGPU_GFLOPS,
            Some(format!(
                "cannot be checked over WebGPU: billed at {} TFLOPS",
                pumasi_proto::gpu::format_tflops(UNCHECKED_WEBGPU_GFLOPS)
            )),
        )
    } else {
        (a.fp32_gflops, None)
    }
}

/// The GPU maps [`pick_gpus`] reads, borrowed from the state.
pub(crate) struct GpuMaps<'a> {
    pub devices: &'a std::collections::HashMap<String, DeviceRecord>,
    pub webgpu: &'a std::collections::HashMap<String, SignedWebGpu>,
    pub extra: &'a std::collections::HashMap<String, WebGpuExtraRecord>,
    pub config: &'a std::collections::HashMap<String, pumasi_proto::manage::NodeConfig>,
}

/// The node label that keeps runs from sharing a node's GPUs (1.10):
/// `gpu.share=off`.
pub const SHARE_LABEL: &str = "gpu.share";

/// [`State::pick_gpus`] on the maps alone, for callers that hold other
/// parts of the state.
pub(crate) fn pick_gpus(
    maps: &GpuMaps,
    id: &str,
    req: &pumasi_proto::gpu::GpuRequest,
    ext: &GpuExt,
    executor: Executor,
    used: &GpuUse,
) -> Option<Vec<u32>> {
    match executor {
        Executor::Wasm => maps.webgpu.get(id).and_then(|w| {
            req.pick_webgpu_with(
                &w.profile.adapters,
                maps.extra.get(id).map(|e| &e.signed.extra),
                &ext.features,
                &used.busy(),
            )
        }),
        Executor::Docker | Executor::Microvm | Executor::Image => {
            let sharing = !maps
                .config
                .get(id)
                .and_then(|c| c.labels.get(SHARE_LABEL))
                .is_some_and(|v| v == "off");
            maps.devices.get(id).and_then(|d| {
                req.pick_shared(d.gpus(), used, executor, ext.share, |g| {
                    sharing && g.shareable()
                })
            })
        }
    }
}

impl State {
    /// Stores a node's signed WebGPU adapters (phase 14).
    pub fn on_webgpu(&mut self, id: &str, generation: u64, signed: SignedWebGpu) -> Result<()> {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation)
        {
            return Ok(());
        }
        signed.verify()?;
        let key: iroh::PublicKey = id.parse()?;
        ensure!(
            &signed.profile.node == key.as_bytes(),
            "WebGPU profile is for another node"
        );
        if !self.members.contains_key(id) {
            return Ok(());
        }
        for a in &signed.profile.adapters {
            info!(
                node = %id,
                adapter = a.index,
                name = %a.describe(),
                gpu = ?a.gpu,
                "WebGPU adapter"
            );
        }
        if self.webgpu.get(id) != Some(&signed) {
            self.store.put_webgpu(id, &signed)?;
            self.webgpu.insert(id.to_string(), signed);
        }
        self.want_adapter_checks(id);
        self.schedule();
        Ok(())
    }

    /// Stores a node's signed WebGPU extras (1.10): the hash rate of each
    /// adapter over WebGPU and the optional features it has. An untrusted
    /// node's adapters that are not listed GPUs are then timed with GPU
    /// challenges over WebGPU.
    pub fn on_webgpu_extra(
        &mut self,
        id: &str,
        generation: u64,
        signed: SignedWebGpuExtra,
    ) -> Result<()> {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation)
        {
            return Ok(());
        }
        signed.verify()?;
        let key: iroh::PublicKey = id.parse()?;
        ensure!(
            &signed.extra.node == key.as_bytes(),
            "WebGPU extras are for another node"
        );
        if !self.members.contains_key(id) {
            return Ok(());
        }
        let old = self.webgpu_extra.get(id);
        if old.map(|r| &r.signed) != Some(&signed) {
            // Checks of claims that did not change still hold.
            let checks: Vec<GpuCheck> = old
                .map(|r| {
                    r.checks
                        .iter()
                        .filter(|c| {
                            signed.extra.adapters.iter().any(|x| {
                                WEBGPU_SLOT_BASE + x.index == c.index
                                    && x.hash_rate == c.claimed_hash_rate
                            })
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            for x in &signed.extra.adapters {
                info!(
                    node = %id,
                    adapter = x.index,
                    hash_rate = x.hash_rate,
                    features = ?x.features,
                    "WebGPU adapter extras"
                );
            }
            let rec = WebGpuExtraRecord { signed, checks };
            self.store.put_webgpu_extra(id, &rec)?;
            self.webgpu_extra.insert(id.to_string(), rec);
        }
        self.want_adapter_checks(id);
        self.schedule();
        Ok(())
    }

    /// The hash rates of a node's adapters that are not listed GPUs, by
    /// slot, where the node measured one (1.10).
    fn adapter_claims(&self, id: &str) -> Vec<(u32, u64)> {
        let (Some(w), Some(x)) = (self.webgpu.get(id), self.webgpu_extra.get(id)) else {
            return Vec::new();
        };
        w.profile
            .adapters
            .iter()
            .filter(|a| a.gpu.is_none())
            .map(|a| (a.slot(), x.hash_rate(a)))
            .filter(|(_, r)| *r > 0)
            .collect()
    }

    /// The hash rate a node claims for a GPU, or an adapter by slot.
    fn claimed_hash_rate(&self, id: &str, index: u32) -> u64 {
        if index >= WEBGPU_SLOT_BASE {
            return self
                .adapter_claims(id)
                .into_iter()
                .find(|(s, _)| *s == index)
                .map_or(0, |(_, r)| r);
        }
        self.node_gpus(id)
            .iter()
            .find(|g| g.index == index)
            .map_or(0, |g| g.hash_rate)
    }

    /// An untrusted node's adapters with a claim the cluster has not timed
    /// are wanted for a GPU challenge over WebGPU.
    fn want_adapter_checks(&mut self, id: &str) {
        if self.members.get(id).is_none_or(|m| m.trusted()) {
            return;
        }
        let unchecked: Vec<u32> = match (self.webgpu.get(id), self.webgpu_extra.get(id)) {
            (Some(w), Some(x)) => w
                .profile
                .adapters
                .iter()
                .filter(|a| a.gpu.is_none() && x.hash_rate(a) > 0 && x.check(a).is_none())
                .map(|a| a.slot())
                .collect(),
            _ => Vec::new(),
        };
        let Some(n) = self.nodes.get_mut(id) else {
            return;
        };
        let pending = n.gpu.pending.as_ref().map(|p| p.challenge.device);
        n.gpu.wanted.retain(|i| *i < WEBGPU_SLOT_BASE);
        n.gpu
            .wanted
            .extend(unchecked.into_iter().filter(|s| Some(*s) != pending));
    }

    /// GPUs for one run on a node: listed GPUs for image jobs (or part of
    /// one, 1.10), a WebGPU adapter (by slot) for wasm jobs.
    pub(crate) fn pick_gpus(
        &self,
        id: &str,
        req: &pumasi_proto::gpu::GpuRequest,
        ext: &GpuExt,
        executor: Executor,
        used: &GpuUse,
    ) -> Option<Vec<u32>> {
        pick_gpus(&self.gpu_maps(), id, req, ext, executor, used)
    }

    pub(crate) fn gpu_maps(&self) -> GpuMaps<'_> {
        GpuMaps {
            devices: &self.devices,
            webgpu: &self.webgpu,
            extra: &self.webgpu_extra,
            config: &self.mgmt.node_config,
        }
    }

    /// The WebGPU views of a member for the status.
    pub(crate) fn webgpu_views(&self, m: &Member) -> Vec<WebGpuView> {
        let Some(w) = self.webgpu.get(&m.id) else {
            return Vec::new();
        };
        let busy: Vec<(TaskRef, u32)> = self
            .nodes
            .get(&m.id)
            .filter(|n| n.online)
            .map(|n| {
                n.gpu
                    .busy
                    .iter()
                    .flat_map(|(t, gs)| gs.iter().map(move |g| (*t, *g)))
                    .collect()
            })
            .unwrap_or_default();
        let rec = self.devices.get(&m.id);
        let extra = self.webgpu_extra.get(&m.id);
        w.profile
            .adapters
            .iter()
            .map(|a| {
                let (billed, note) = billed_webgpu_gflops(m, rec, extra, a);
                WebGpuView {
                    adapter: a.clone(),
                    billed_gflops: billed,
                    note,
                    busy: busy.iter().find(|(_, s)| *s == a.slot()).map(|(t, _)| *t),
                }
            })
            .collect()
    }

    /// Stores a node's signed device profile. New GPUs of an untrusted
    /// node are timed by challenges.
    pub fn on_devices(&mut self, id: &str, generation: u64, signed: SignedDevices) -> Result<()> {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation)
        {
            return Ok(());
        }
        signed.verify()?;
        let key: iroh::PublicKey = id.parse()?;
        ensure!(
            &signed.profile.node == key.as_bytes(),
            "device profile is for another node"
        );
        let Some(m) = self.members.get(id) else {
            return Ok(());
        };
        let trusted = m.trusted();
        let p = &signed.profile;
        let indices: HashSet<u32> = p.gpus.iter().map(|g| g.index).collect();
        ensure!(
            indices.len() == p.gpus.len(),
            "device profile lists a GPU twice"
        );
        for g in &p.gpus {
            info!(
                node = %id,
                gpu = g.index,
                name = %g.name,
                vram_mib = g.vram_mib,
                gflops = g.fp32_gflops,
                docker = g.docker,
                vfio = g.vfio,
                "GPU"
            );
        }
        let old = self.devices.get(id);
        // Checks of claims that did not change still hold.
        let checks: Vec<GpuCheck> = old
            .map(|r| {
                r.checks
                    .iter()
                    .filter(|c| {
                        p.gpus
                            .iter()
                            .any(|g| g.index == c.index && g.hash_rate == c.claimed_hash_rate)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let rec = DeviceRecord { signed, checks };
        let wanted: Vec<u32> = if trusted {
            Vec::new()
        } else {
            rec.gpus()
                .iter()
                .filter(|g| g.usable() && g.hash_rate > 0 && rec.check(g).is_none())
                .map(|g| g.index)
                .collect()
        };
        if old != Some(&rec) {
            self.store.put_devices(id, &rec)?;
            self.devices.insert(id.to_string(), rec);
        }
        if let Some(n) = self.nodes.get_mut(id) {
            // Adapters wanted for a check over WebGPU stay wanted.
            n.gpu.wanted.retain(|i| *i >= WEBGPU_SLOT_BASE);
            n.gpu.wanted.extend(wanted);
        }
        self.schedule();
        Ok(())
    }

    /// The device profile of an online node that speaks protocol 9.
    pub(crate) fn node_gpus(&self, id: &str) -> &[GpuInfo] {
        self.devices.get(id).map(|r| r.gpus()).unwrap_or_default()
    }

    /// The GPU views of a member for the status.
    pub(crate) fn gpu_views(&self, m: &Member) -> Vec<GpuView> {
        let Some(rec) = self.devices.get(&m.id) else {
            return Vec::new();
        };
        let busy: Vec<(TaskRef, u32)> = self
            .nodes
            .get(&m.id)
            .filter(|n| n.online)
            .map(|n| {
                n.gpu
                    .busy
                    .iter()
                    .flat_map(|(t, gs)| gs.iter().map(move |g| (*t, *g)))
                    .collect()
            })
            .unwrap_or_default();
        let shares = self
            .nodes
            .get(&m.id)
            .filter(|n| n.online)
            .map(|n| n.gpu.usage().shared)
            .unwrap_or_default();
        rec.gpus()
            .iter()
            .map(|g| {
                let (billed, mut note) = billed_gflops(m, rec, g);
                let runs = busy.iter().filter(|(_, i)| *i == g.index).count();
                if let Some(part) = shares.get(&g.index) {
                    let shared = format!(
                        "shared by {runs} run{} ({} GPU in use)",
                        if runs == 1 { "" } else { "s" },
                        pumasi_proto::gpu::format_share(*part)
                    );
                    note = Some(match note {
                        Some(n) => format!("{n}; {shared}"),
                        None => shared,
                    });
                }
                GpuView {
                    info: g.clone(),
                    billed_gflops: billed,
                    note,
                    busy: busy.iter().find(|(_, i)| *i == g.index).map(|(t, _)| *t),
                }
            })
            .collect()
    }

    /// What a run's GPUs cost per hour and what goes on its receipt. A run
    /// that held `share` hundredths of its GPU (1.10) pays that part of
    /// its FP32 rate and memory.
    pub(crate) fn gpu_billing(
        &self,
        node_id: &str,
        executor: Executor,
        gpus: &[u32],
        share: u32,
    ) -> Option<GpuBilling> {
        let mut b = self.whole_gpu_billing(node_id, executor, gpus)?;
        if share > 0 && share < GPU_SHARE_WHOLE {
            b.gflops = b.gflops * share as u64 / GPU_SHARE_WHOLE as u64;
            b.vram_mib = b.vram_mib * share as u64 / GPU_SHARE_WHOLE as u64;
            for n in b.names.iter_mut() {
                n.push_str(&format!(
                    " ({} GPU)",
                    pumasi_proto::gpu::format_share(share)
                ));
            }
        }
        Some(b)
    }

    fn whole_gpu_billing(
        &self,
        node_id: &str,
        executor: Executor,
        gpus: &[u32],
    ) -> Option<GpuBilling> {
        if gpus.is_empty() {
            return None;
        }
        let m = self.members.get(node_id)?;
        let mut b = GpuBilling {
            names: Vec::new(),
            gflops: 0,
            vram_mib: 0,
            exact_ucu: 0,
        };
        if executor == Executor::Wasm {
            let w = self.webgpu.get(node_id)?;
            for slot in gpus {
                let Some(a) = w.profile.adapters.iter().find(|a| a.slot() == *slot) else {
                    continue;
                };
                b.names.push(format!("{} (WebGPU, {})", a.name, a.backend));
                b.gflops += billed_webgpu_gflops(
                    m,
                    self.devices.get(node_id),
                    self.webgpu_extra.get(node_id),
                    a,
                )
                .0 as u64;
            }
            return Some(b);
        }
        let rec = self.devices.get(node_id)?;
        for i in gpus {
            let Some(g) = rec.gpus().iter().find(|g| g.index == *i) else {
                continue;
            };
            b.names.push(g.name.clone());
            b.gflops += billed_gflops(m, rec, g).0 as u64;
            b.vram_mib += g.vram_mib;
        }
        Some(b)
    }

    /// After a GPU run on an untrusted node: at the result-check rate,
    /// time its GPUs again once they are idle.
    pub(crate) fn sample_gpus(&mut self, node_id: &str, gpus: &[u32]) {
        if gpus.is_empty() {
            return;
        }
        let (trusted, _, _, passes, strikes) = self.node_record(node_id);
        if trusted {
            return;
        }
        let rate = self.policy.sample_permille(passes, strikes);
        if rand::random_range(0..1000) >= rate {
            return;
        }
        let checkable: Vec<u32> = gpus
            .iter()
            .copied()
            .filter(|i| self.claimed_hash_rate(node_id, *i) > 0)
            .collect();
        let Some(n) = self.nodes.get_mut(node_id) else {
            return;
        };
        n.gpu.sampled.retain(|(_, t)| t.elapsed() < SAMPLE_GAP);
        for i in checkable {
            if n.gpu.sampled.iter().any(|(g, _)| *g == i) || n.gpu.wanted.contains(&i) {
                continue;
            }
            info!(node = %node_id, gpu = i, "GPU picked for a check");
            n.gpu.sampled.push((i, Instant::now()));
            n.gpu.wanted.push(i);
        }
    }

    /// Sends GPU challenges to nodes whose wanted GPUs are idle.
    pub(crate) fn drive_gpu_challenges(&mut self) {
        let now = Instant::now();
        // What each node waiting for a check claims for its wanted GPUs
        // and adapters. Adapters are timed over WebGPU only by agents
        // that know how (minor 6).
        let claims: std::collections::HashMap<String, Vec<(u32, u64)>> = self
            .nodes
            .iter()
            .filter(|(_, n)| !n.gpu.wanted.is_empty())
            .map(|(id, n)| {
                let slots = self.agent_minor(id) >= pumasi_proto::GPU_MINOR;
                let c = n
                    .gpu
                    .wanted
                    .iter()
                    .filter(|i| slots || **i < WEBGPU_SLOT_BASE)
                    .map(|i| (*i, self.claimed_hash_rate(id, *i)))
                    .filter(|(_, r)| *r > 0)
                    .collect();
                (id.clone(), c)
            })
            .collect();
        for (id, n) in self.nodes.iter_mut() {
            if !n.online
                || n.gpu.wanted.is_empty()
                || n.gpu.pending.is_some()
                || n.perf.protocol < pumasi_proto::DEVICES_PROTOCOL
                || self.upgrading.contains_key(id)
                || n.gpu.last.is_some_and(|t| t.elapsed() < RETRY_GAP)
            {
                continue;
            }
            let busy = n.gpu.busy_set();
            let claims = claims.get(id).map(Vec::as_slice).unwrap_or_default();
            // Forget GPUs and adapters the profiles no longer have.
            n.gpu.wanted.retain(|i| claims.iter().any(|(c, _)| c == i));
            let Some(pos) = n.gpu.wanted.iter().position(|i| !busy.contains(i)) else {
                continue;
            };
            let index = n.gpu.wanted[pos];
            let Some(&(_, claimed)) = claims.iter().find(|(c, _)| *c == index) else {
                continue;
            };
            let Some(tx) = &n.tx else { continue };
            let hashes = (claimed as u128 * CHALLENGE_MS as u128 / 1000)
                .clamp(GPU_CHALLENGE_SOLUTIONS as u128 * 1000, u64::MAX as u128)
                as u64;
            let challenge = GpuChallenge {
                nonce: rand::random(),
                device: index,
                seed: rand::random(),
                target: target_for(hashes, GPU_CHALLENGE_SOLUTIONS),
                solutions: GPU_CHALLENGE_SOLUTIONS,
            };
            if tx.send(ToAgent::GpuChallenge(challenge)).is_err() {
                continue;
            }
            info!(node = %id, gpu = index, claimed, "timing the node's GPU");
            n.gpu.wanted.remove(pos);
            n.gpu.last = Some(now);
            n.gpu.pending = Some(PendingGpu {
                challenge,
                sent: now,
                claimed,
            });
        }
    }

    /// A node answered its GPU challenge.
    pub fn on_gpu_result(&mut self, id: &str, generation: u64, nonce: u64, solutions: Vec<u64>) {
        let Some(n) = self.nodes.get_mut(id) else {
            return;
        };
        if n.generation != generation
            || n.gpu.pending.as_ref().map(|p| p.challenge.nonce) != Some(nonce)
        {
            return;
        }
        let p = n.gpu.pending.take().expect("checked");
        let rtt = Duration::from_micros(n.perf.rtt_us.unwrap_or(0) as u64);
        let took = p.sent.elapsed().saturating_sub(rtt);
        if solutions.is_empty() {
            // The GPU could not run it this time: no verdict, and the GPU
            // stays unconfirmed (billed at the reference at most).
            warn!(node = %id, gpu = p.challenge.device, "the node could not run the GPU check");
            self.schedule();
            return;
        }
        let correct = check_answer(&p.challenge, &solutions);
        self.apply_gpu_check(id, &p, took, correct);
    }

    fn apply_gpu_check(&mut self, id: &str, p: &PendingGpu, took: Duration, correct: bool) {
        let c = &p.challenge;
        let hashes = expected_hashes(c.target, c.solutions) as u128;
        let timed = (hashes * 1_000_000_000 / took.as_nanos().max(1_000_000)) as u64;
        let record = |checks: &mut Vec<GpuCheck>| {
            let prev = checks
                .iter()
                .position(|k| k.index == c.device && k.claimed_hash_rate == p.claimed);
            let (measured, attempts) = match prev.map(|i| &checks[i]) {
                Some(k) if k.correct && correct => {
                    (k.measured_hash_rate.max(timed), k.attempts + 1)
                }
                _ => (timed, 1),
            };
            checks.retain(|k| k.index != c.device);
            checks.push(GpuCheck {
                index: c.device,
                claimed_hash_rate: p.claimed,
                measured_hash_rate: measured,
                correct,
                attempts,
                unix: now_unix(),
            });
            (measured, attempts)
        };
        let (measured, attempts) = if c.device >= WEBGPU_SLOT_BASE {
            let Some(x) = self.webgpu_extra.get_mut(id) else {
                return;
            };
            let r = record(&mut x.checks);
            let x = x.clone();
            if let Err(e) = self.store.put_webgpu_extra(id, &x) {
                warn!("persist WebGPU extras: {e:#}");
            }
            r
        } else {
            let Some(rec) = self.devices.get_mut(id) else {
                return;
            };
            let r = record(&mut rec.checks);
            let rec = rec.clone();
            if let Err(e) = self.store.put_devices(id, &rec) {
                warn!("persist devices: {e:#}");
            }
            r
        };
        if !correct {
            self.strike(id, "wrong answer to the GPU check");
        } else if p.claimed as u128 > measured as u128 * INFLATED as u128 {
            if attempts < STRIKE_AFTER {
                info!(node = %id, gpu = c.device, claimed = p.claimed, timed, "GPU looks slower than claimed; timing it again");
                if let Some(n) = self.nodes.get_mut(id) {
                    n.gpu.wanted.push(c.device);
                }
            } else {
                self.strike(
                    id,
                    &format!(
                        "GPU {} inflated: the profile claims {} hashes/s, the cluster timed {measured}",
                        c.device, p.claimed
                    ),
                );
            }
        } else {
            info!(node = %id, gpu = c.device, claimed = p.claimed, measured, "GPU confirmed");
            self.credit_pass(id);
        }
        self.schedule();
    }

    /// GPU challenges that were not answered in time count as answered
    /// now (slow, but not wrong).
    pub(crate) fn expire_gpu_challenges(&mut self) {
        let late: Vec<(String, PendingGpu)> = self
            .nodes
            .iter_mut()
            .filter_map(|(id, n)| {
                let late = n
                    .gpu
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.sent.elapsed() > CHALLENGE_TIMEOUT);
                late.then(|| (id.clone(), n.gpu.pending.take().expect("checked")))
            })
            .collect();
        for (id, p) in late {
            warn!(node = %id, gpu = p.challenge.device, "no answer to the GPU check in time");
            self.apply_gpu_check(&id, &p, CHALLENGE_TIMEOUT, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use pumasi_proto::{
        gpu::{DeviceProfile, GPU_BENCH_VERSION, GpuVendor},
        msg::{NodeInfo, Role},
    };

    fn gpu(gflops: u32, hash_rate: u64) -> GpuInfo {
        GpuInfo {
            index: 0,
            vendor: GpuVendor::Nvidia,
            name: "rtx".into(),
            uuid: "GPU-1".into(),
            pci: String::new(),
            vram_mib: 12 << 10,
            driver: "570".into(),
            compute: "8.6".into(),
            display: false,
            fp32_gflops: gflops,
            measured: true,
            mem_bw_gb_s: 360,
            hash_rate,
            docker: true,
            vfio: false,
            render_node: String::new(),
        }
    }

    fn record(g: GpuInfo, checks: Vec<GpuCheck>) -> DeviceRecord {
        let key = SecretKey::generate();
        DeviceRecord {
            signed: SignedDevices::sign(
                DeviceProfile {
                    bench_version: GPU_BENCH_VERSION,
                    node: *key.public().as_bytes(),
                    measured_unix: 1,
                    gpus: vec![g],
                    microvm_network: false,
                    notes: vec![],
                },
                &key,
            ),
            checks,
        }
    }

    fn member(role: Role) -> Member {
        Member {
            id: "n".into(),
            info: NodeInfo {
                name: "n".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                cpus: 8,
                memory_bytes: 16 << 30,
                slots: 4,
                agent_version: "t".into(),
                executors: vec![],
            },
            joined_unix: 0,
            role,
            account: "bob".into(),
            allow_docker: true,
            checks_passed: 0,
            strikes: 0,
            perf: None,
            perf_check: None,
        }
    }

    fn check(claimed: u64, measured: u64, correct: bool) -> GpuCheck {
        GpuCheck {
            index: 0,
            claimed_hash_rate: claimed,
            measured_hash_rate: measured,
            correct,
            attempts: 1,
            unix: 0,
        }
    }

    #[test]
    fn provider_gpus_are_billed_at_what_the_cluster_confirmed() {
        let g = gpu(30_000, 1_000_000);
        // The owner's GPUs: as measured.
        let owner = member(Role::Owner);
        assert_eq!(
            billed_gflops(&owner, &record(g.clone(), vec![]), &g).0,
            30_000
        );
        let p = member(Role::Provider);
        // Not timed yet: the reference at most.
        let (b, note) = billed_gflops(&p, &record(g.clone(), vec![]), &g);
        assert_eq!(b, REFERENCE_GPU_GFLOPS);
        assert!(note.unwrap().contains("not confirmed"));
        // Small GPUs are not capped.
        let small = gpu(5_000, 1_000_000);
        assert_eq!(
            billed_gflops(&p, &record(small.clone(), vec![]), &small).0,
            5_000
        );
        // Confirmed within the slack.
        let ok = record(g.clone(), vec![check(1_000_000, 900_000, true)]);
        assert_eq!(billed_gflops(&p, &ok, &g), (30_000, None));
        // Timed at half the claim: billed at 1.25 times what was timed.
        let slow = record(g.clone(), vec![check(1_000_000, 500_000, true)]);
        assert_eq!(billed_gflops(&p, &slow, &g).0, 18_750);
        // A wrong answer: nothing.
        let wrong = record(g.clone(), vec![check(1_000_000, 0, false)]);
        assert_eq!(billed_gflops(&p, &wrong, &g).0, 0);
        // A check of an older claim does not count.
        let old = record(g.clone(), vec![check(2_000_000, 2_000_000, true)]);
        assert_eq!(billed_gflops(&p, &old, &g).0, REFERENCE_GPU_GFLOPS);
        // A GPU that cannot run the challenge stays at the reference.
        let amd = gpu(40_000, 0);
        assert_eq!(
            billed_gflops(&p, &record(amd.clone(), vec![]), &amd).0,
            REFERENCE_GPU_GFLOPS
        );
    }
}
