//! Node performance profiles: what a machine measured about itself (CPU,
//! memory bandwidth, the disk it works on, that disk's SMART health),
//! signed with its node key, and the CPU benchmark kernel both the agent
//! and the control plane can run.
//!
//! The CPU score is measured in the same WASM sandbox tasks run in: a fixed
//! integer kernel ([`KERNEL_WAT`]) runs under Wasmtime with fuel metering,
//! and the score is its speed in iterations per second relative to the
//! reference vCPU ([`REFERENCE_ITERS_PER_SEC`] = score 1000). Because the
//! kernel is seeded and its result cannot be guessed, the control plane can
//! send a node a fresh seed, time the answer itself and recompute it with
//! [`kernel_native`] to see whether the node's claimed score holds.

use anyhow::{Context, Result};
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

const PERF_DOMAIN: &[u8] = b"pumasi/perf/node/v1\0";

/// Score of the reference vCPU. One CU is one reference vCPU for an hour.
pub const REFERENCE_SCORE: u32 = 1000;

/// Kernel iterations per second on the reference vCPU: one hardware thread
/// of a 2.5 GHz Skylake-class server core (an AWS m5 vCPU), running the
/// kernel in Wasmtime with fuel metering on.
pub const REFERENCE_ITERS_PER_SEC: u64 = 150_000_000;

/// Fuel the kernel's loop burns per iteration (checked by the agent's
/// tests against Wasmtime's metering).
pub const FUEL_PER_ITER: u64 = 46;

/// Fuel per second of the reference vCPU. WASM runs are billed for the
/// reference time their fuel stands for, so the same task costs the same
/// on any machine.
pub const REFERENCE_FUEL_PER_SEC: u64 = REFERENCE_ITERS_PER_SEC * FUEL_PER_ITER;

/// Bounds for the CPU score used in billing.
pub const MIN_SCORE: u32 = 100;
pub const MAX_SCORE: u32 = 5000;

/// Bumped when the benchmarks change in a way that makes old profiles
/// incomparable; agents re-measure then.
pub const BENCH_VERSION: u32 = 1;

/// The CPU benchmark kernel. `bench(seed, iters)` fills a 4 KiB table from
/// the seed, then runs `iters` rounds of dependent 64-bit multiply, rotate,
/// xor and table reads and writes, and returns a digest of its state. It
/// uses integers only, so every machine gets the same answer.
pub const KERNEL_WAT: &str = r#"
(module
  (memory 1)
  (func (export "bench") (param $seed i64) (param $iters i64) (result i64)
    (local $x i64) (local $y i64) (local $t i64) (local $i i64) (local $a i32)
    (local.set $x (local.get $seed))
    (block $filled
      (loop $fill
        (br_if $filled (i32.ge_u (local.get $a) (i32.const 4096)))
        (local.set $x
          (i64.add (i64.mul (local.get $x) (i64.const 0x5851F42D4C957F2D))
                   (i64.const 0x14057B7EF767814F)))
        (i64.store (local.get $a)
          (i64.xor (local.get $x) (i64.shr_u (local.get $x) (i64.const 29))))
        (local.set $a (i32.add (local.get $a) (i32.const 8)))
        (br $fill)))
    (local.set $y (i64.xor (local.get $seed) (i64.const 0x2545F4914F6CDD1D)))
    (block $done
      (loop $round
        (br_if $done (i64.ge_u (local.get $i) (local.get $iters)))
        (local.set $x
          (i64.add (i64.mul (local.get $x) (i64.const 0x9E3779B97F4A7C15))
                   (local.get $i)))
        (local.set $y
          (i64.add (i64.rotl (i64.xor (local.get $y) (local.get $x)) (i64.const 23))
                   (i64.shr_u (local.get $x) (i64.const 17))))
        (local.set $a
          (i32.shl (i32.and (i32.wrap_i64 (local.get $y)) (i32.const 511))
                   (i32.const 3)))
        (local.set $t (i64.load (local.get $a)))
        (i64.store (local.get $a)
          (i64.xor (i64.add (local.get $t) (local.get $x)) (local.get $y)))
        (local.set $x (i64.xor (local.get $x) (local.get $t)))
        (local.set $i (i64.add (local.get $i) (i64.const 1)))
        (br $round)))
    (i64.xor (local.get $x) (local.get $y))))
"#;

/// The kernel in plain Rust: the same result as [`KERNEL_WAT`] for the
/// same arguments.
pub fn kernel_native(seed: u64, iters: u64) -> u64 {
    let mut table = [0u64; 512];
    let mut x = seed;
    for slot in table.iter_mut() {
        x = x
            .wrapping_mul(0x5851_F42D_4C95_7F2D)
            .wrapping_add(0x1405_7B7E_F767_814F);
        *slot = x ^ (x >> 29);
    }
    let mut y = seed ^ 0x2545_F491_4F6C_DD1D;
    for i in 0..iters {
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i);
        y = (y ^ x).rotate_left(23).wrapping_add(x >> 17);
        let a = (y as u32 & 511) as usize;
        let t = table[a];
        table[a] = t.wrapping_add(x) ^ y;
        x ^= t;
    }
    x ^ y
}

/// Score for running `iters` kernel iterations in `nanos` nanoseconds.
pub fn score_for(iters: u64, nanos: u128) -> u32 {
    let per_sec = iters as u128 * 1_000_000_000 / nanos.max(1);
    (per_sec * REFERENCE_SCORE as u128 / REFERENCE_ITERS_PER_SEC as u128).min(u32::MAX as u128)
        as u32
}

/// Iterations a node with `score` runs in `millis` milliseconds.
pub fn iters_for(score: u32, millis: u64) -> u64 {
    REFERENCE_ITERS_PER_SEC * score as u64 / REFERENCE_SCORE as u64 * millis / 1000
}

/// A challenge from the control plane: run the kernel with this seed and
/// iteration count and send back the result, as fast as possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchChallenge {
    pub nonce: u64,
    pub seed: u64,
    pub iters: u64,
}

/// What kind of drive a node's work directory is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskClass {
    /// Spinning disk: random reads take milliseconds.
    Hdd,
    /// SATA or SAS SSD (or a network block device of that speed).
    Ssd,
    Nvme,
    /// Could not tell (no direct I/O, so reads came from the page cache).
    Unknown,
}

impl std::fmt::Display for DiskClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DiskClass::Hdd => "hdd",
            DiskClass::Ssd => "ssd",
            DiskClass::Nvme => "nvme",
            DiskClass::Unknown => "unknown",
        })
    }
}

/// Classifies a drive from what it measured. Seeks make a spinning disk's
/// random 4 KiB reads take milliseconds; flash answers in well under one.
/// NVMe reads sequentially faster than SATA allows (about 550 MB/s).
pub fn classify_disk(direct: bool, seq_read_mb_s: u32, rand_read_p50_us: u32) -> DiskClass {
    if !direct {
        DiskClass::Unknown
    } else if rand_read_p50_us >= 1500 {
        DiskClass::Hdd
    } else if seq_read_mb_s >= 700 {
        DiskClass::Nvme
    } else {
        DiskClass::Ssd
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuPerf {
    pub model: String,
    /// Hardware threads the multi-thread run used (the CPUs offered).
    pub threads: u32,
    /// Score of one thread with the machine otherwise idle.
    pub single: u32,
    /// Combined score of all `threads` running at once.
    pub multi: u32,
}

impl CpuPerf {
    /// What one vCPU delivers when every offered CPU is busy: the score
    /// billing uses (hyperthreads and shared caches make it lower than the
    /// single-thread score).
    pub fn per_vcpu(&self) -> u32 {
        (self.multi / self.threads.max(1)).min(self.single.max(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPerf {
    pub total_bytes: u64,
    /// Copy bandwidth, counting bytes read and written (like STREAM copy).
    pub copy_mb_s: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartInfo {
    pub model: String,
    /// The drive's own overall health verdict.
    pub passed: Option<bool>,
    /// Share of rated endurance used (NVMe percentage used, or 100 minus
    /// an SSD's wear-leveling or life-left attribute).
    pub percent_used: Option<u8>,
    pub power_on_hours: Option<u32>,
    /// NVMe media errors, or reallocated plus pending sectors.
    pub media_errors: Option<u64>,
    pub temperature_c: Option<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskPerf {
    /// Classified from the measurements, see [`classify_disk`].
    pub class: DiskClass,
    /// What the operating system says the drive is, when it says.
    pub os_class: Option<DiskClass>,
    /// Reads bypassed the page cache (O_DIRECT, F_NOCACHE or unbuffered
    /// I/O). Without it the read numbers are the cache's.
    pub direct: bool,
    pub seq_read_mb_s: u32,
    pub seq_write_mb_s: u32,
    /// 4 KiB random reads, one at a time: median and 99th percentile
    /// latency.
    pub rand_read_p50_us: u32,
    pub rand_read_p99_us: u32,
    /// 4 KiB random reads per second with 8 in flight.
    pub rand_read_iops: u32,
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub smart: Option<SmartInfo>,
}

/// Everything a node measured about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfProfile {
    pub bench_version: u32,
    /// The node's public key.
    pub node: [u8; 32],
    pub measured_unix: u64,
    pub agent_version: String,
    pub os: String,
    pub arch: String,
    pub cpu: CpuPerf,
    pub memory: MemoryPerf,
    /// The drive under the agent's work directory.
    pub disk: Option<DiskPerf>,
    /// What could not be measured, and why.
    pub notes: Vec<String>,
}

/// A profile signed by the node it describes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedPerf {
    pub profile: PerfProfile,
    pub sig: Signature,
}

fn perf_message(p: &PerfProfile) -> Vec<u8> {
    let mut m = PERF_DOMAIN.to_vec();
    m.extend(postcard::to_stdvec(p).expect("profile encodes"));
    m
}

impl SignedPerf {
    pub fn sign(profile: PerfProfile, key: &SecretKey) -> Self {
        let sig = key.sign(&perf_message(&profile));
        Self { profile, sig }
    }

    /// Checks the signature against the node key inside the profile.
    pub fn verify(&self) -> Result<()> {
        let key = PublicKey::from_bytes(&self.profile.node).context("bad node key")?;
        key.verify(&perf_message(&self.profile), &self.sig)
            .map_err(|_| anyhow::anyhow!("profile signature does not match"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(node: &SecretKey) -> PerfProfile {
        PerfProfile {
            bench_version: BENCH_VERSION,
            node: *node.public().as_bytes(),
            measured_unix: 1,
            agent_version: "t".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            cpu: CpuPerf {
                model: "cpu".into(),
                threads: 4,
                single: 1200,
                multi: 3600,
            },
            memory: MemoryPerf {
                total_bytes: 8 << 30,
                copy_mb_s: 10_000,
            },
            disk: None,
            notes: vec![],
        }
    }

    #[test]
    fn signed_profiles_verify_and_resist_edits() {
        let key = SecretKey::generate();
        let s = SignedPerf::sign(profile(&key), &key);
        s.verify().unwrap();
        let mut forged = s.clone();
        forged.profile.cpu.single *= 2;
        assert!(forged.verify().is_err());
        assert_eq!(s.profile.cpu.per_vcpu(), 900);
    }

    #[test]
    fn disk_classes() {
        // A 7200 rpm disk: 160 MB/s, 8 ms random reads.
        assert_eq!(classify_disk(true, 160, 8000), DiskClass::Hdd);
        // A SATA SSD: 520 MB/s, 90 us.
        assert_eq!(classify_disk(true, 520, 90), DiskClass::Ssd);
        // An NVMe drive: 2.8 GB/s, 40 us.
        assert_eq!(classify_disk(true, 2800, 40), DiskClass::Nvme);
        // Page cache: no verdict.
        assert_eq!(classify_disk(false, 9000, 1), DiskClass::Unknown);
    }

    #[test]
    fn kernel_is_seeded_and_scores_scale() {
        assert_eq!(kernel_native(1, 1000), kernel_native(1, 1000));
        assert_ne!(kernel_native(1, 1000), kernel_native(2, 1000));
        assert_ne!(kernel_native(1, 1000), kernel_native(1, 1001));
        assert_eq!(score_for(REFERENCE_ITERS_PER_SEC, 1_000_000_000), 1000);
        assert_eq!(score_for(REFERENCE_ITERS_PER_SEC, 2_000_000_000), 500);
        assert_eq!(iters_for(2000, 500), REFERENCE_ITERS_PER_SEC);
    }
}
