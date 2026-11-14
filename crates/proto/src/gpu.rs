//! GPUs and other devices a node offers: what the agent found and
//! measured (signed with its node key), what a job asks for, and the GPU
//! challenge the control plane uses to check a provider's GPU.
//!
//! Devices travel in their own message ([`crate::msg::FromAgent::Devices`],
//! protocol 9) rather than in the hello or the performance profile, so
//! older agents and stored records keep their layout.
//!
//! The challenge is a proof of work: hash `(seed, x)` for `x = 0, 1, 2,
//! ...` with [`gpu_hash`] and find [`GPU_CHALLENGE_SOLUTIONS`] values of
//! `x` whose hash is below a target. Finding them takes a predictable
//! number of hashes on average, checking them takes one hash each, and the
//! hash uses only 32-bit integer operations, which every GPU runs at full
//! speed. The agent measures its own hash rate the same way when it
//! benchmarks, so the control plane compares like with like.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    str::FromStr,
};

use anyhow::{Context, Result, bail, ensure};
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::job::Executor;

const DEVICES_DOMAIN: &[u8] = b"pumasi/devices/node/v1\0";

/// Bumped when the GPU benchmark changes so old numbers are not compared
/// with new ones.
pub const GPU_BENCH_VERSION: u32 = 1;

/// FP32 GFLOPS an unconfirmed provider GPU is billed at, at most: about an
/// NVIDIA T4 or RTX 3060.
pub const REFERENCE_GPU_GFLOPS: u32 = 10_000;

/// Solutions a GPU challenge asks for. More solutions make the time it
/// takes less random (its spread is about 1/sqrt(n)).
pub const GPU_CHALLENGE_SOLUTIONS: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum GpuVendor {
    Nvidia,
    Amd,
    Intel,
    Apple,
    Other,
}

impl fmt::Display for GpuVendor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            GpuVendor::Nvidia => "nvidia",
            GpuVendor::Amd => "amd",
            GpuVendor::Intel => "intel",
            GpuVendor::Apple => "apple",
            GpuVendor::Other => "other",
        })
    }
}

impl FromStr for GpuVendor {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "nvidia" => GpuVendor::Nvidia,
            "amd" => GpuVendor::Amd,
            "intel" => GpuVendor::Intel,
            "apple" => GpuVendor::Apple,
            _ => bail!("unknown GPU vendor {s:?} (expected nvidia, amd, intel or apple)"),
        })
    }
}

impl GpuVendor {
    /// The PCI vendor id.
    pub fn from_pci(id: u16) -> Self {
        match id {
            0x10de => GpuVendor::Nvidia,
            0x1002 => GpuVendor::Amd,
            0x8086 => GpuVendor::Intel,
            0x106b => GpuVendor::Apple,
            _ => GpuVendor::Other,
        }
    }
}

/// One GPU as the node sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuInfo {
    /// The node's own number for it; assignments name GPUs by it.
    pub index: u32,
    pub vendor: GpuVendor,
    pub name: String,
    /// NVIDIA's `GPU-...` UUID, else empty.
    pub uuid: String,
    /// PCI address (`0000:01:00.0`), when known.
    pub pci: String,
    pub vram_mib: u64,
    /// Driver version (NVIDIA), or the kernel driver bound to it
    /// (`amdgpu`, `vfio-pci`).
    pub driver: String,
    /// CUDA compute capability (`8.6`) or AMD GFX target (`gfx1030`).
    pub compute: String,
    /// Drives a display: never passed through to a microVM.
    pub display: bool,
    /// FP32 throughput in GFLOPS: measured when `measured`, otherwise
    /// estimated from the chip's units and clock (0: unknown).
    pub fp32_gflops: u32,
    pub measured: bool,
    /// Device memory bandwidth in GB/s (0: not measured).
    pub mem_bw_gb_s: u32,
    /// Challenge hashes per second the node measured (0: cannot run the
    /// challenge, e.g. no CUDA).
    pub hash_rate: u64,
    /// Docker jobs can use it (NVIDIA Container Toolkit, or AMD's
    /// `/dev/kfd` and render node).
    pub docker: bool,
    /// MicroVM jobs can use it: bound to `vfio-pci`, its IOMMU group is
    /// clean, and the GPU guest kernel is installed.
    pub vfio: bool,
    /// AMD only: the render node (`/dev/dri/renderD128`) for Docker.
    pub render_node: String,
}

impl GpuInfo {
    /// Whether some executor on this node can run jobs on it.
    pub fn usable(&self) -> bool {
        self.docker || self.vfio
    }

    pub fn usable_by(&self, executor: Executor) -> bool {
        match executor {
            Executor::Docker => self.docker,
            Executor::Microvm => self.vfio,
            Executor::Image => self.docker || self.vfio,
            Executor::Wasm => false,
        }
    }

    /// `NVIDIA GeForce RTX 3060 (12 GiB, 12.7 TFLOPS)`.
    pub fn describe(&self) -> String {
        // Integrated GPUs (Apple silicon) share the system memory.
        let mem = if self.vram_mib == 0 {
            "shared memory".to_string()
        } else {
            format_mib(self.vram_mib)
        };
        let mut s = format!("{} ({mem}", self.name);
        if self.fp32_gflops > 0 {
            s.push_str(&format!(
                ", {} TFLOPS{}",
                format_tflops(self.fp32_gflops),
                if self.measured { "" } else { " est." }
            ));
        }
        s.push(')');
        s
    }
}

/// `12.7` from 12_700 GFLOPS.
pub fn format_tflops(gflops: u32) -> String {
    let tenths = (gflops as u64 + 50) / 100;
    format!("{}.{}", tenths / 10, tenths % 10)
}

fn format_mib(mib: u64) -> String {
    if mib >= 1024 {
        let tenths = (mib * 10 + 512) / 1024;
        if tenths.is_multiple_of(10) {
            format!("{} GiB", tenths / 10)
        } else {
            format!("{}.{} GiB", tenths / 10, tenths % 10)
        }
    } else {
        format!("{mib} MiB")
    }
}

/// The devices a node offers, signed by it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceProfile {
    pub bench_version: u32,
    /// The node's public key.
    pub node: [u8; 32],
    pub measured_unix: u64,
    pub gpus: Vec<GpuInfo>,
    /// MicroVMs here can get a network device (jobs with `network = true`).
    pub microvm_network: bool,
    /// What could not be detected or measured, and why.
    pub notes: Vec<String>,
}

impl DeviceProfile {
    pub fn usable_gpus(&self) -> impl Iterator<Item = &GpuInfo> {
        self.gpus.iter().filter(|g| g.usable())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedDevices {
    pub profile: DeviceProfile,
    pub sig: Signature,
}

fn devices_message(p: &DeviceProfile) -> Vec<u8> {
    let mut m = DEVICES_DOMAIN.to_vec();
    m.extend(postcard::to_stdvec(p).expect("profile encodes"));
    m
}

impl SignedDevices {
    pub fn sign(profile: DeviceProfile, key: &SecretKey) -> Self {
        let sig = key.sign(&devices_message(&profile));
        Self { profile, sig }
    }

    pub fn verify(&self) -> Result<()> {
        let key = PublicKey::from_bytes(&self.profile.node).context("bad node key")?;
        key.verify(&devices_message(&self.profile), &self.sig)
            .map_err(|_| anyhow::anyhow!("device profile signature does not match"))
    }
}

/// The GPUs a job's tasks need, each task its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuRequest {
    /// GPUs per task; 0 means every matching GPU of the node (at least
    /// one), like `docker run --gpus all`.
    pub count: u32,
    pub vendor: Option<GpuVendor>,
    pub min_vram_mib: u64,
}

/// Most GPUs one task may ask for.
pub const MAX_GPUS_PER_TASK: u32 = 16;

/// A whole GPU in [`GpuExt::share`]'s hundredths.
pub const GPU_SHARE_WHOLE: u32 = 100;

/// What a job asks of its GPUs beyond its [`GpuRequest`] (1.10). It rides
/// beside the submission ([`crate::job::JobExt`]), never inside it, so the
/// submission keeps its layout.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuExt {
    /// Part of one GPU per task, in hundredths (1 to 99): `gpus = 0.5` is
    /// 50. Tasks with parts share a GPU by time-slicing (no memory
    /// partitioning). 0: whole GPUs, as [`GpuRequest::count`] says.
    pub share: u32,
    /// WebGPU features the adapter must have (wasm jobs).
    pub features: Vec<crate::webgpu::WebGpuFeature>,
}

impl GpuExt {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.share < GPU_SHARE_WHOLE,
            "a GPU part must be less than a whole GPU"
        );
        ensure!(self.features.len() <= 8, "too many WebGPU features");
        Ok(())
    }
}

/// `0.5` from 50 hundredths.
pub fn format_share(share: u32) -> String {
    if share.is_multiple_of(10) {
        format!("0.{}", share / 10)
    } else {
        format!("0.{share:02}")
    }
}

/// What a node's GPUs hold now (1.10): GPUs taken whole, and the parts of
/// shared GPUs in hundredths.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GpuUse {
    pub whole: HashSet<u32>,
    pub shared: HashMap<u32, u32>,
}

impl GpuUse {
    /// Every GPU in use, whole or in part: what a whole-GPU run must avoid.
    pub fn busy(&self) -> HashSet<u32> {
        let mut b = self.whole.clone();
        b.extend(self.shared.iter().filter(|(_, s)| **s > 0).map(|(g, _)| *g));
        b
    }

    /// Nothing in use but these whole GPUs (callers that know no parts).
    pub fn of_busy(busy: &HashSet<u32>) -> Self {
        Self {
            whole: busy.clone(),
            shared: HashMap::new(),
        }
    }
}
impl GpuRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.count <= MAX_GPUS_PER_TASK,
            "a task can use at most {MAX_GPUS_PER_TASK} GPUs"
        );
        Ok(())
    }

    pub fn matches(&self, g: &GpuInfo, executor: Executor) -> bool {
        g.usable_by(executor)
            && self.vendor.is_none_or(|v| v == g.vendor)
            && g.vram_mib >= self.min_vram_mib
    }

    /// Picks GPUs for one task among `gpus`, leaving out `busy` ones: the
    /// smallest that fit, so large GPUs stay free for jobs that need them.
    pub fn pick(
        &self,
        gpus: &[GpuInfo],
        busy: &HashSet<u32>,
        executor: Executor,
    ) -> Option<Vec<u32>> {
        let matching: Vec<&GpuInfo> = gpus.iter().filter(|g| self.matches(g, executor)).collect();
        if self.count == 0 {
            if matching.is_empty() || matching.iter().any(|g| busy.contains(&g.index)) {
                return None;
            }
            return Some(matching.iter().map(|g| g.index).collect());
        }
        let mut free: Vec<&GpuInfo> = matching
            .into_iter()
            .filter(|g| !busy.contains(&g.index))
            .collect();
        if free.len() < self.count as usize {
            return None;
        }
        free.sort_by_key(|g| (g.vram_mib, g.fp32_gflops, g.index));
        let mut picked: Vec<u32> = free[..self.count as usize]
            .iter()
            .map(|g| g.index)
            .collect();
        picked.sort_unstable();
        Some(picked)
    }

    /// Whether a node with these GPUs could ever run the task (all of them
    /// free).
    pub fn possible(&self, gpus: &[GpuInfo], executor: Executor) -> bool {
        self.pick(gpus, &HashSet::new(), executor).is_some()
    }

    /// Picks GPUs for one task when it may take `share` hundredths of one
    /// (1.10): a GPU no run holds whole, that `shareable` allows and that
    /// has room left, the fullest that fits so whole GPUs stay free. With
    /// `share` 0 this is [`Self::pick`] around every GPU in use.
    pub fn pick_shared(
        &self,
        gpus: &[GpuInfo],
        used: &GpuUse,
        executor: Executor,
        share: u32,
        shareable: impl Fn(&GpuInfo) -> bool,
    ) -> Option<Vec<u32>> {
        if share == 0 || share >= GPU_SHARE_WHOLE {
            return self.pick(gpus, &used.busy(), executor);
        }
        gpus.iter()
            .filter(|g| self.matches(g, executor) && shareable(g))
            .filter(|g| !used.whole.contains(&g.index))
            .filter_map(|g| {
                let taken = used.shared.get(&g.index).copied().unwrap_or(0);
                (taken + share <= GPU_SHARE_WHOLE).then_some((taken, g))
            })
            .max_by_key(|(taken, g)| {
                (
                    *taken,
                    std::cmp::Reverse((g.vram_mib, g.fp32_gflops, g.index)),
                )
            })
            .map(|(_, g)| vec![g.index])
    }
}

impl GpuInfo {
    /// Whether runs can share it by time-slicing (1.10): Docker reaches
    /// it. A GPU passed through to a microVM belongs to that VM alone.
    pub fn shareable(&self) -> bool {
        self.docker
    }

    /// An NVIDIA MIG slice (its UUID starts with `MIG-`), listed as a GPU
    /// of its own (1.10).
    pub fn mig(&self) -> bool {
        self.uuid.starts_with("MIG-")
    }
}

impl fmt::Display for GpuRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.count == 0 {
            f.write_str("all")?;
        } else {
            write!(f, "{}", self.count)?;
        }
        if let Some(v) = self.vendor {
            write!(f, " {v}")?;
        }
        f.write_str(if self.count == 1 { " GPU" } else { " GPUs" })?;
        if self.min_vram_mib > 0 {
            write!(f, " with {} or more", format_mib(self.min_vram_mib))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The GPU challenge
// ---------------------------------------------------------------------------

/// A GPU challenge: find `solutions` values of `x` with
/// `gpu_hash(seed, x) < target` on GPU `device`, as fast as possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuChallenge {
    pub nonce: u64,
    pub device: u32,
    pub seed: u64,
    pub target: u64,
    pub solutions: u32,
}

#[inline]
fn mix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x7feb_352d);
    h ^= h >> 15;
    h = h.wrapping_mul(0x846c_a68b);
    h ^= h >> 16;
    h
}

/// The challenge hash: 32-bit operations only (multiply, xor, shift,
/// rotate), so the agent's GPU kernel computes exactly this.
#[inline]
pub fn gpu_hash(seed: u64, x: u64) -> u64 {
    let mut a = (seed as u32) ^ (x as u32);
    let mut b = ((seed >> 32) as u32) ^ ((x >> 32) as u32);
    for r in 0..4u32 {
        a = mix(a ^ b.rotate_left(13) ^ r.wrapping_mul(0x9e37_79b9));
        b = mix(b ^ a.rotate_left(7) ^ 0x85eb_ca6b);
    }
    ((a as u64) << 32) | b as u64
}

/// The target that makes finding `solutions` solutions take `hashes`
/// hashes on average.
pub fn target_for(hashes: u64, solutions: u32) -> u64 {
    let per_solution = (hashes / solutions.max(1) as u64).max(1);
    u64::MAX / per_solution
}

/// Hashes it takes on average to find `solutions` solutions below
/// `target`.
pub fn expected_hashes(target: u64, solutions: u32) -> u64 {
    let per_solution = u64::MAX / target.max(1);
    per_solution.saturating_mul(solutions as u64)
}

/// Checks an answer: `solutions` distinct values, each hashing below the
/// target.
pub fn check_answer(c: &GpuChallenge, xs: &[u64]) -> bool {
    let distinct: HashSet<u64> = xs.iter().copied().collect();
    xs.len() == c.solutions as usize
        && distinct.len() == xs.len()
        && xs.iter().all(|&x| gpu_hash(c.seed, x) < c.target)
}

/// Solves a challenge on the CPU (tests, and machines measuring the
/// reference). Scans `x` from 0.
pub fn solve_cpu(seed: u64, target: u64, solutions: u32) -> Vec<u64> {
    let mut out = Vec::with_capacity(solutions as usize);
    let mut x = 0u64;
    while out.len() < solutions as usize {
        if gpu_hash(seed, x) < target {
            out.push(x);
        }
        x += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(index: u32, vendor: GpuVendor, vram_gib: u64, docker: bool) -> GpuInfo {
        GpuInfo {
            index,
            vendor,
            name: format!("gpu{index}"),
            uuid: String::new(),
            pci: String::new(),
            vram_mib: vram_gib << 10,
            driver: String::new(),
            compute: String::new(),
            display: false,
            fp32_gflops: 10_000,
            measured: true,
            mem_bw_gb_s: 0,
            hash_rate: 0,
            docker,
            vfio: false,
            render_node: String::new(),
        }
    }

    #[test]
    fn requests_pick_free_matching_gpus() {
        let gpus = vec![
            gpu(0, GpuVendor::Nvidia, 24, true),
            gpu(1, GpuVendor::Nvidia, 8, true),
            gpu(2, GpuVendor::Amd, 16, true),
            gpu(3, GpuVendor::Nvidia, 12, false),
        ];
        let none = HashSet::new();
        let two = GpuRequest {
            count: 2,
            vendor: Some(GpuVendor::Nvidia),
            min_vram_mib: 0,
        };
        assert_eq!(two.pick(&gpus, &none, Executor::Docker), Some(vec![0, 1]));
        assert_eq!(two.pick(&gpus, &HashSet::from([1]), Executor::Docker), None);
        assert!(two.possible(&gpus, Executor::Docker));
        // No microVM passthrough on this node.
        assert!(!two.possible(&gpus, Executor::Microvm));
        let one = GpuRequest {
            count: 1,
            vendor: None,
            min_vram_mib: 10 << 10,
        };
        // The smallest that fits: the 16 GiB AMD card, then the 24 GiB one.
        assert_eq!(one.pick(&gpus, &none, Executor::Docker), Some(vec![2]));
        assert_eq!(
            one.pick(&gpus, &HashSet::from([2]), Executor::Docker),
            Some(vec![0])
        );
        let all = GpuRequest {
            count: 0,
            vendor: Some(GpuVendor::Nvidia),
            min_vram_mib: 0,
        };
        assert_eq!(all.pick(&gpus, &none, Executor::Docker), Some(vec![0, 1]));
        assert_eq!(all.pick(&gpus, &HashSet::from([0]), Executor::Docker), None);
        assert_eq!(all.pick(&[], &none, Executor::Docker), None);
        assert_eq!(all.to_string(), "all nvidia GPUs");
        assert_eq!(one.to_string(), "1 GPU with 10 GiB or more");
        assert!(
            GpuRequest {
                count: 99,
                vendor: None,
                min_vram_mib: 0
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn parts_of_gpus_share_and_whole_runs_keep_out() {
        let gpus = vec![
            gpu(0, GpuVendor::Nvidia, 24, true),
            gpu(1, GpuVendor::Nvidia, 10, true),
            gpu(2, GpuVendor::Nvidia, 10, false),
        ];
        let one = GpuRequest {
            count: 1,
            vendor: None,
            min_vram_mib: 0,
        };
        let all = |_: &GpuInfo| true;
        let mut used = GpuUse::default();
        // The smallest Docker GPU first, then the same one again while it
        // has room.
        assert_eq!(
            one.pick_shared(&gpus, &used, Executor::Docker, 50, all),
            Some(vec![1])
        );
        used.shared.insert(1, 50);
        assert_eq!(
            one.pick_shared(&gpus, &used, Executor::Docker, 50, all),
            Some(vec![1])
        );
        // Too little left on it: the next GPU.
        assert_eq!(
            one.pick_shared(&gpus, &used, Executor::Docker, 60, all),
            Some(vec![0])
        );
        // A whole run avoids every GPU with parts on it.
        assert_eq!(
            one.pick_shared(&gpus, &used, Executor::Docker, 0, all),
            Some(vec![0])
        );
        used.whole.insert(0);
        assert_eq!(
            one.pick_shared(&gpus, &used, Executor::Docker, 60, all),
            None
        );
        assert_eq!(
            one.pick_shared(&gpus, &used, Executor::Docker, 0, all),
            None
        );
        // A node that does not share its GPUs.
        assert_eq!(
            one.pick_shared(&gpus, &GpuUse::default(), Executor::Docker, 50, |_| false),
            None
        );
        assert_eq!(used.busy(), HashSet::from([0, 1]));
        assert_eq!(format_share(50), "0.5");
        assert_eq!(format_share(25), "0.25");
        assert_eq!(format_share(5), "0.05");
        assert!(
            GpuExt {
                share: 100,
                features: vec![]
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn challenge_answers_check_out() {
        assert_eq!(gpu_hash(1, 2), gpu_hash(1, 2));
        assert_ne!(gpu_hash(1, 2), gpu_hash(1, 3));
        assert_ne!(gpu_hash(1, 2), gpu_hash(2, 2));
        // Known values, so the GPU kernels can be checked against them.
        assert_eq!(gpu_hash(0, 0), 0x986e_5fb3_5fa9_ac7c);
        assert_eq!(
            gpu_hash(0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210),
            0xb7a6_82af_43e2_5065
        );
        let target = target_for(64 * 1000, 64);
        assert_eq!(expected_hashes(target, 64), 64 * 1000);
        let c = GpuChallenge {
            nonce: 1,
            device: 0,
            seed: 42,
            target,
            solutions: 64,
        };
        let xs = solve_cpu(c.seed, c.target, c.solutions);
        assert!(check_answer(&c, &xs));
        // About 64,000 hashes on average.
        assert!(*xs.last().unwrap() < 200_000, "{}", xs.last().unwrap());
        let mut dup = xs.clone();
        dup[1] = dup[0];
        assert!(!check_answer(&c, &dup));
        assert!(!check_answer(&c, &xs[..63]));
        let mut wrong = xs.clone();
        wrong[5] += 1;
        assert!(!check_answer(&c, &wrong) || gpu_hash(c.seed, wrong[5]) < c.target);
    }

    #[test]
    fn signed_devices_resist_edits() {
        let key = SecretKey::generate();
        let p = DeviceProfile {
            bench_version: GPU_BENCH_VERSION,
            node: *key.public().as_bytes(),
            measured_unix: 1,
            gpus: vec![gpu(0, GpuVendor::Nvidia, 12, true)],
            microvm_network: true,
            notes: vec![],
        };
        let s = SignedDevices::sign(p, &key);
        s.verify().unwrap();
        let mut forged = s.clone();
        forged.profile.gpus[0].fp32_gflops *= 2;
        assert!(forged.verify().is_err());
        assert_eq!(s.profile.gpus[0].describe(), "gpu0 (12 GiB, 10.0 TFLOPS)");
        assert_eq!(format_tflops(12_740), "12.7");
        assert_eq!("NVIDIA".parse::<GpuVendor>().unwrap(), GpuVendor::Nvidia);
        assert_eq!(GpuVendor::from_pci(0x1002), GpuVendor::Amd);
    }
}
