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
        GPU_CHALLENGE_SOLUTIONS, GpuChallenge, GpuInfo, REFERENCE_GPU_GFLOPS, SignedDevices,
        check_answer, expected_hashes, target_for,
    },
    msg::{GpuView, TaskRef, ToAgent},
    receipt::GpuBilling,
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
}

impl GpuRuntime {
    pub fn busy_set(&self) -> HashSet<u32> {
        self.busy.values().flatten().copied().collect()
    }

    /// GPU tasks wait while a GPU of the node is being timed.
    pub fn checking(&self) -> bool {
        !self.wanted.is_empty() || self.pending.is_some()
    }
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
        Some(c) => {
            let allowed = (c.measured_hash_rate as u128 * SLACK_PERMILLE as u128 / 1000).max(1);
            if g.hash_rate as u128 > allowed {
                let gflops = (g.fp32_gflops as u128 * allowed / g.hash_rate as u128) as u32;
                (
                    gflops,
                    Some(format!(
                        "capped: the cluster timed {} hashes/s, the profile claims {}",
                        c.measured_hash_rate, g.hash_rate
                    )),
                )
            } else {
                (g.fp32_gflops, None)
            }
        }
        None => capped("not confirmed yet"),
    }
}

impl State {
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
            n.gpu.wanted = wanted;
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
        rec.gpus()
            .iter()
            .map(|g| {
                let (billed, note) = billed_gflops(m, rec, g);
                GpuView {
                    info: g.clone(),
                    billed_gflops: billed,
                    note,
                    busy: busy.iter().find(|(_, i)| *i == g.index).map(|(t, _)| *t),
                }
            })
            .collect()
    }

    /// What a run's GPUs cost per hour and what goes on its receipt.
    pub(crate) fn gpu_billing(&self, node_id: &str, gpus: &[u32]) -> Option<GpuBilling> {
        if gpus.is_empty() {
            return None;
        }
        let m = self.members.get(node_id)?;
        let rec = self.devices.get(node_id)?;
        let mut b = GpuBilling {
            names: Vec::new(),
            gflops: 0,
            vram_mib: 0,
            exact_ucu: 0,
        };
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
            .filter(|i| {
                self.node_gpus(node_id)
                    .iter()
                    .any(|g| g.index == *i && g.hash_rate > 0)
            })
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
            let gpus = self
                .devices
                .get(id)
                .map(|r| r.gpus().to_vec())
                .unwrap_or_default();
            // Forget GPUs the profile no longer has.
            n.gpu
                .wanted
                .retain(|i| gpus.iter().any(|g| g.index == *i && g.hash_rate > 0));
            let Some(pos) = n.gpu.wanted.iter().position(|i| !busy.contains(i)) else {
                continue;
            };
            let index = n.gpu.wanted[pos];
            let Some(g) = gpus.iter().find(|g| g.index == index) else {
                continue;
            };
            let Some(tx) = &n.tx else { continue };
            let hashes = (g.hash_rate as u128 * CHALLENGE_MS as u128 / 1000)
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
            info!(node = %id, gpu = index, claimed = g.hash_rate, "timing the node's GPU");
            n.gpu.wanted.remove(pos);
            n.gpu.last = Some(now);
            n.gpu.pending = Some(PendingGpu {
                challenge,
                sent: now,
                claimed: g.hash_rate,
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
        let Some(rec) = self.devices.get_mut(id) else {
            return;
        };
        let prev = rec
            .checks
            .iter()
            .position(|k| k.index == c.device && k.claimed_hash_rate == p.claimed);
        let (measured, attempts) = match prev.map(|i| &rec.checks[i]) {
            Some(k) if k.correct && correct => (k.measured_hash_rate.max(timed), k.attempts + 1),
            _ => (timed, 1),
        };
        rec.checks.retain(|k| k.index != c.device);
        rec.checks.push(GpuCheck {
            index: c.device,
            claimed_hash_rate: p.claimed,
            measured_hash_rate: measured,
            correct,
            attempts,
            unix: now_unix(),
        });
        let rec = rec.clone();
        if let Err(e) = self.store.put_devices(id, &rec) {
            warn!("persist devices: {e:#}");
        }
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
