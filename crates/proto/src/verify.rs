//! Result checks for work that re-running cannot check (1.20): honeypot
//! tasks with known answers, disk and memory challenges next to the CPU
//! one, and a score per provider.
//!
//! - **Honeypots.** The control plane slips tasks whose answer it already
//!   knows into providers' queues: built-in ones it makes up per executor
//!   ([`builtin_wasm`], [`builtin_shell`]) and copies of an account's own
//!   finished jobs (`pumasi honeypot add`). A wrong answer is treated like
//!   a failed check, and the provider's earnings stay held.
//! - **Disk and memory challenges.** Like the CPU challenge of
//!   [`crate::perf`], the control plane sends a seed, times the answer and
//!   recomputes it. The agent measures its own profile with the same code
//!   ([`disk_chunk`], [`block_fold`], [`memory_pass`]), so an honest
//!   claim and the cluster's timing agree.
//! - **Scores.** Uptime, check and honeypot results and completed runs
//!   make one number per node ([`score`]) that placement prefers.

use serde::{Deserialize, Serialize};

use crate::{
    job::{Executor, JobId},
    perf::DiskClass,
};

/// Disk challenges read in blocks of this size (4 KiB, as random reads
/// in the benchmark).
pub const DISK_BLOCK: usize = 4096;
/// And write and read sequentially in chunks of this size.
pub const DISK_CHUNK: usize = 1 << 20;
/// Blocks per chunk.
pub const BLOCKS_PER_CHUNK: usize = DISK_CHUNK / DISK_BLOCK;

// ---------------------------------------------------------------------------
// Policy and views
// ---------------------------------------------------------------------------

/// How a node's results are trusted, which sets how often it gets a
/// honeypot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustLevel {
    /// One of the owner's machines: never gets a honeypot.
    Owner,
    /// A provider's machine an admin trusts (`pumasi trust`).
    Trusted,
    /// A provider that has passed fewer than [`NEW_UNTIL`] checks and
    /// honeypots.
    New,
    Provider,
    /// A provider with a strike (a failed check or honeypot, a forged
    /// receipt, a wrong benchmark answer).
    Struck,
}

/// Passed checks and honeypots before a provider is no longer new.
pub const NEW_UNTIL: u32 = 10;

impl std::fmt::Display for TrustLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TrustLevel::Owner => "owner",
            TrustLevel::Trusted => "trusted",
            TrustLevel::New => "new",
            TrustLevel::Provider => "provider",
            TrustLevel::Struck => "struck",
        })
    }
}

/// The cluster's honeypot and challenge settings (`pumasi honeypot set`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyPolicy {
    /// Honeypots are sent at all.
    pub honeypots: bool,
    /// Chance, in thousandths, that a provider gets a honeypot after each
    /// task it finishes, by trust level.
    pub new_permille: u32,
    pub provider_permille: u32,
    pub trusted_permille: u32,
    pub struck_permille: u32,
    /// The image of built-in Docker and microVM honeypots: any image with
    /// a POSIX `sh`. An image the cluster's real jobs use hides them best.
    /// Empty: no built-in image honeypots.
    pub image: String,
    /// A provider that fails a honeypot has none of its earnings released
    /// for this long.
    pub hold_secs: u64,
    /// The cluster times providers' CPU, disk and memory again this often
    /// (0: only when their profile changes).
    pub retime_secs: u64,
}

impl Default for VerifyPolicy {
    fn default() -> Self {
        Self {
            honeypots: true,
            new_permille: 100,
            provider_permille: 20,
            trusted_permille: 0,
            struck_permille: 250,
            image: "busybox:1.36".into(),
            hold_secs: 24 * 3600,
            retime_secs: 24 * 3600,
        }
    }
}

impl VerifyPolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, v) in [
            ("new", self.new_permille),
            ("provider", self.provider_permille),
            ("trusted", self.trusted_permille),
            ("struck", self.struck_permille),
        ] {
            anyhow::ensure!(
                v <= 1000,
                "the {name} honeypot rate is above 1000 per mille"
            );
        }
        anyhow::ensure!(
            !self.image.starts_with('-') && !self.image.contains(char::is_whitespace),
            "invalid image name"
        );
        anyhow::ensure!(
            self.retime_secs == 0 || self.retime_secs >= 600,
            "time providers again every 10 minutes at most"
        );
        Ok(())
    }

    /// The chance, in thousandths, that a node of this level gets a
    /// honeypot after a task.
    pub fn permille(&self, level: TrustLevel) -> u32 {
        if !self.honeypots {
            return 0;
        }
        match level {
            TrustLevel::Owner => 0,
            TrustLevel::Trusted => self.trusted_permille,
            TrustLevel::New => self.new_permille,
            TrustLevel::Provider => self.provider_permille,
            TrustLevel::Struck => self.struck_permille,
        }
    }
}

/// Changes to the policy; `None` leaves a field alone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyPolicyPatch {
    pub honeypots: Option<bool>,
    pub new_permille: Option<u32>,
    pub provider_permille: Option<u32>,
    pub trusted_permille: Option<u32>,
    pub struck_permille: Option<u32>,
    pub image: Option<String>,
    pub hold_secs: Option<u64>,
    pub retime_secs: Option<u64>,
}

impl VerifyPolicyPatch {
    pub fn apply(&self, p: &mut VerifyPolicy) {
        if let Some(v) = self.honeypots {
            p.honeypots = v;
        }
        if let Some(v) = self.new_permille {
            p.new_permille = v;
        }
        if let Some(v) = self.provider_permille {
            p.provider_permille = v;
        }
        if let Some(v) = self.trusted_permille {
            p.trusted_permille = v;
        }
        if let Some(v) = self.struck_permille {
            p.struck_permille = v;
        }
        if let Some(v) = &self.image {
            p.image = v.trim().to_string();
        }
        if let Some(v) = self.hold_secs {
            p.hold_secs = v;
        }
        if let Some(v) = self.retime_secs {
            p.retime_secs = v;
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// A honeypot copied from one of an account's finished tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoneypotView {
    pub id: u32,
    pub account: String,
    /// The job's name, which its copies carry too.
    pub name: String,
    pub executor: Executor,
    /// The job and task it was copied from.
    pub source: JobId,
    pub task: u32,
    /// A trusted node ran it again and got the same answer, so it does not
    /// change from run to run: only confirmed honeypots are sent.
    pub confirmed: bool,
    /// Why it is not used (its answer changed on a trusted node).
    pub problem: Option<String>,
    pub sent: u32,
    pub passed: u32,
    pub failed: u32,
    pub added_unix: u64,
}

/// How a honeypot run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HoneypotResult {
    Passed,
    /// The answer was wrong.
    Failed,
    /// Nothing comparable came back (timeout, agent error, the node left):
    /// no one is blamed.
    Inconclusive(String),
}

impl std::fmt::Display for HoneypotResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HoneypotResult::Passed => f.write_str("passed"),
            HoneypotResult::Failed => f.write_str("FAILED"),
            HoneypotResult::Inconclusive(why) => write!(f, "inconclusive: {why}"),
        }
    }
}

/// One honeypot sent to a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoneypotRunView {
    pub job: JobId,
    pub node: String,
    pub node_name: String,
    /// `None` for a built-in one.
    pub honeypot: Option<u32>,
    pub executor: Executor,
    pub sent_unix: u64,
    /// `None` while it runs.
    pub result: Option<HoneypotResult>,
}

/// The cluster's timing of a node's disk, beside what its profile claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCheck {
    pub claimed_class: DiskClass,
    pub claimed_seq_read_mb_s: u32,
    pub claimed_p50_us: u32,
    /// The best the cluster timed for this claim.
    pub seq_write_mb_s: u32,
    pub seq_read_mb_s: u32,
    /// Mean time of a 4 KiB random read, one at a time.
    pub read_us: u32,
    /// The class the timing says, which the `disk` label follows.
    pub class: DiskClass,
    /// Every answer was right (or late).
    pub correct: bool,
    pub attempts: u32,
    pub unix: u64,
}

/// The cluster's timing of a node's memory bandwidth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCheck {
    pub claimed_mb_s: u32,
    /// The best the cluster timed for this claim.
    pub measured_mb_s: u32,
    pub correct: bool,
    pub attempts: u32,
    pub unix: u64,
}

/// A node's score and what went into it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderScore {
    pub node: String,
    pub name: String,
    pub account: String,
    /// 0 to 1000; placement prefers higher.
    pub score: u32,
    pub level: TrustLevel,
    /// Share of the time it was online (sleep it announced not counted),
    /// over about the last week, in thousandths.
    pub uptime_permille: u32,
    /// From passed and failed checks and honeypots.
    pub checks_permille: u32,
    /// From runs it finished and runs it lost (it vanished, sent a
    /// forged receipt or its agent failed).
    pub completion_permille: u32,
    pub checks_passed: u32,
    pub strikes: u32,
    pub honeypots_passed: u32,
    pub honeypots_failed: u32,
    pub runs_done: u64,
    pub runs_lost: u64,
    pub disk: Option<DiskCheck>,
    pub memory: Option<MemoryCheck>,
    /// The `disk` label: the profile's class on owner machines, and on
    /// providers the class the cluster timed (none until it has).
    pub disk_label: Option<DiskClass>,
    /// Its account's earnings are held until then after a failed
    /// honeypot.
    pub held_until: Option<u64>,
}

/// Everything `pumasi honeypot` and `pumasi score` show.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyView {
    pub policy: VerifyPolicy,
    pub honeypots: Vec<HoneypotView>,
    /// The latest honeypot runs, newest first.
    pub runs: Vec<HoneypotRunView>,
    pub scores: Vec<ProviderScore>,
    /// Accounts whose earnings are held after a failed honeypot, and
    /// until when.
    pub held: Vec<(String, u64)>,
}

/// A node's score from what went into it. Owner and trusted machines get
/// full marks for checks; a suspended node scores 0.
pub fn score(
    uptime_permille: u32,
    level: TrustLevel,
    passed: u32,
    strikes: u32,
    done: u64,
    lost: u64,
    suspended: bool,
) -> (u32, u32, u32) {
    let checks = match level {
        TrustLevel::Owner | TrustLevel::Trusted => 1000,
        // A new provider starts at half and earns the rest; a strike
        // weighs four passes.
        _ => {
            let good = passed as u64 + 1;
            (good * 1000 / (good + 1 + 4 * strikes as u64)) as u32
        }
    };
    let completion = ((done + 1) * 1000 / (done + lost + 1)) as u32;
    let total = if suspended {
        0
    } else {
        (uptime_permille.min(1000) + 2 * checks + completion) / 4
    };
    (total, checks, completion)
}

// ---------------------------------------------------------------------------
// Challenges
// ---------------------------------------------------------------------------

/// One step of a disk challenge. The cluster times each step's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskStep {
    /// Write `bytes` of [`disk_chunk`] data with the page cache bypassed,
    /// synced, into the work directory.
    Prepare { bytes: u64 },
    /// Read the whole file, a chunk at a time, in [`chunk_order`].
    Sequential,
    /// [`random_walk`] over the file: `reads` 4 KiB reads, each at an
    /// offset the last one's data decides. Then remove the file.
    Random { reads: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskChallenge {
    pub nonce: u64,
    /// The data's seed (the same for every step of one challenge).
    pub seed: u64,
    pub step: DiskStep,
}

/// The most a disk challenge may ask an agent to write; agents refuse
/// more.
pub const MAX_DISK_CHALLENGE_BYTES: u64 = 4 << 30;
/// The most random reads one disk challenge step may ask for.
pub const MAX_DISK_CHALLENGE_READS: u32 = 1 << 20;
/// The most memory a memory challenge may ask an agent to touch.
pub const MAX_MEMORY_CHALLENGE_BYTES: u64 = 1 << 30;
/// The most passes a memory challenge may ask for.
pub const MAX_MEMORY_CHALLENGE_PASSES: u32 = 4096;

/// Run [`memory_kernel`] and send back its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryChallenge {
    pub nonce: u64,
    pub seed: u64,
    /// Both buffers together.
    pub bytes: u64,
    pub passes: u32,
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Fills `out` (one chunk) with the challenge data of chunk `index`.
pub fn disk_chunk(seed: u64, index: u64, out: &mut [u8]) {
    let key = blake3::derive_key("pumasi disk challenge v1", &seed.to_le_bytes());
    let mut h = blake3::Hasher::new_keyed(&key);
    h.update(&index.to_le_bytes());
    h.finalize_xof().fill(out);
}

/// A cheap digest of one 4 KiB block that needs every byte of it.
pub fn block_fold(block: &[u8]) -> u64 {
    let mut a = 0xCBF2_9CE4_8422_2325u64;
    for w in block.as_chunks::<8>().0 {
        a = (a ^ u64::from_le_bytes(*w)).wrapping_mul(0x0000_0100_0000_01B3);
    }
    a
}

/// The folds of every block of one chunk.
pub fn chunk_folds(chunk: &[u8]) -> Vec<u64> {
    chunk
        .as_chunks::<DISK_BLOCK>()
        .0
        .iter()
        .map(|b| block_fold(b))
        .collect()
}

/// Every block's fold of a challenge file of `bytes`: what the control
/// plane recomputes to check the answers.
pub fn disk_folds(seed: u64, bytes: u64) -> Vec<u64> {
    let chunks = bytes / DISK_CHUNK as u64;
    let mut buf = vec![0u8; DISK_CHUNK];
    let mut out = Vec::with_capacity(chunks as usize * BLOCKS_PER_CHUNK);
    for c in 0..chunks {
        disk_chunk(seed, c, &mut buf);
        out.extend(chunk_folds(&buf));
    }
    out
}

/// The answer to [`DiskStep::Prepare`]: proof the data was made, from the
/// fold of each chunk's last block.
pub fn prepare_answer(nonce: u64, last_folds: impl IntoIterator<Item = u64>) -> [u8; 32] {
    let key = blake3::derive_key("pumasi disk prepare v1", &nonce.to_le_bytes());
    let mut h = blake3::Hasher::new_keyed(&key);
    for f in last_folds {
        h.update(&f.to_le_bytes());
    }
    *h.finalize().as_bytes()
}

/// The fold of each chunk's last block, from every block's fold.
pub fn last_folds(folds: &[u64]) -> impl Iterator<Item = u64> + '_ {
    folds.chunks(BLOCKS_PER_CHUNK).map(|c| c[c.len() - 1])
}

/// The order [`DiskStep::Sequential`] reads the chunks in.
pub fn chunk_order(seed: u64, nonce: u64, chunks: u32) -> Vec<u32> {
    let mut v: Vec<u32> = (0..chunks).collect();
    let mut x = seed ^ nonce.rotate_left(32);
    for i in (1..v.len()).rev() {
        x = splitmix(x);
        v.swap(i, (x % (i as u64 + 1)) as usize);
    }
    v
}

/// The answer to [`DiskStep::Sequential`], from each chunk's block folds
/// in reading order.
pub struct SequentialAnswer(blake3::Hasher);

impl SequentialAnswer {
    pub fn new(nonce: u64) -> Self {
        let key = blake3::derive_key("pumasi disk sequential v1", &nonce.to_le_bytes());
        Self(blake3::Hasher::new_keyed(&key))
    }

    pub fn chunk(&mut self, folds: &[u64]) {
        for f in folds {
            self.0.update(&f.to_le_bytes());
        }
    }

    pub fn finish(self) -> [u8; 32] {
        *self.0.finalize().as_bytes()
    }
}

/// The answer to [`DiskStep::Random`]: `reads` blocks, each picked by the
/// last one's fold, so they cannot be read ahead or in parallel.
/// `fold_at` reads block `i` and folds it.
pub fn random_walk<E>(
    nonce: u64,
    reads: u32,
    blocks: u64,
    mut fold_at: impl FnMut(u64) -> Result<u64, E>,
) -> Result<[u8; 32], E> {
    let key = blake3::derive_key("pumasi disk random v1", &nonce.to_le_bytes());
    let mut h = blake3::Hasher::new_keyed(&key);
    let blocks = blocks.max(1);
    let mut i = splitmix(nonce) % blocks;
    for j in 0..reads as u64 {
        let f = fold_at(i)?;
        h.update(&f.to_le_bytes());
        i = splitmix(nonce ^ f ^ j) % blocks;
    }
    Ok(*h.finalize().as_bytes())
}

/// Random reads for a challenge of about `millis` at `read_us` a read.
pub fn disk_reads(read_us: u32, millis: u64) -> u32 {
    (millis * 1000 / read_us.max(20) as u64).clamp(32, 8192) as u32
}

/// One pass of the memory benchmark and challenge: every word of `dst`
/// from the same word of `src` and another `offset` words on. Bound by
/// memory bandwidth (three streams), not arithmetic.
pub fn memory_pass(src: &[u64], dst: &mut [u64], key: u64, offset: usize) {
    let n = src.len();
    let r = ((key >> 58) | 1) as u32;
    let offset = offset % n.max(1);
    let (head, tail) = dst.split_at_mut(n - offset);
    for ((d, s), o) in head.iter_mut().zip(&src[..n - offset]).zip(&src[offset..]) {
        *d = (s ^ key).rotate_left(r).wrapping_add(*o);
    }
    for ((d, s), o) in tail.iter_mut().zip(&src[n - offset..]).zip(&src[..offset]) {
        *d = (s ^ key).rotate_left(r).wrapping_add(*o);
    }
}

/// The two buffers of the memory kernel, filled for `seed`.
pub fn memory_buffers(seed: u64, bytes: u64) -> (Vec<u64>, Vec<u64>) {
    let n = (bytes / 16).max(1024) as usize;
    let a: Vec<u64> = (0..n as u64)
        .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ seed)
        .collect();
    (a, vec![0u64; n])
}

/// The key and offset of pass `p`.
pub fn memory_step(seed: u64, p: u32, n: usize) -> (u64, usize) {
    let k = splitmix(seed ^ (p as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93));
    (k, (splitmix(k) % n.max(1) as u64) as usize)
}

/// A digest of the buffer a run ended with.
pub fn memory_digest(buf: &[u64]) -> u64 {
    buf.iter().enumerate().fold(0u64, |acc, (i, w)| {
        acc.wrapping_add(w.wrapping_mul(2 * i as u64 + 1))
    })
}

/// The memory challenge: `passes` of [`memory_pass`] over two buffers of
/// `bytes` together, then a digest of the last one.
pub fn memory_kernel(seed: u64, bytes: u64, passes: u32) -> u64 {
    let (mut a, mut b) = memory_buffers(seed, bytes);
    let n = a.len();
    for p in 0..passes {
        let (k, off) = memory_step(seed, p, n);
        if p % 2 == 0 {
            memory_pass(&a, &mut b, k, off);
        } else {
            memory_pass(&b, &mut a, k, off);
        }
    }
    memory_digest(if passes % 2 == 1 { &b } else { &a })
}

/// Bytes a [`memory_kernel`] run moves, counted like the benchmark's
/// figure: the fill, three streams a pass, and the digest's read, of one
/// buffer each.
pub fn memory_traffic(bytes: u64, passes: u32) -> u64 {
    let one = (bytes / 16).max(1024) * 8;
    one * (2 + 3 * passes as u64)
}

/// Passes for a challenge of about `millis` at `mb_s`.
pub fn memory_passes(mb_s: u32, bytes: u64, millis: u64) -> u32 {
    let one = (bytes / 16).max(1024) * 8;
    let total = mb_s as u64 * 1_000_000 * millis / 1000;
    (total / (3 * one).max(1)).clamp(1, 256) as u32
}

/// The class a disk times as, by the rules of [`crate::perf::classify_disk`]
/// (a random read's mean time standing for the median).
pub fn timed_class(seq_read_mb_s: u32, read_us: u32) -> DiskClass {
    crate::perf::classify_disk(true, seq_read_mb_s, read_us)
}

/// Orders classes from slow to fast; `Unknown` is none.
pub fn class_rank(c: DiskClass) -> Option<u8> {
    match c {
        DiskClass::Hdd => Some(0),
        DiskClass::Ssd => Some(1),
        DiskClass::Nvme => Some(2),
        DiskClass::Unknown => None,
    }
}

// ---------------------------------------------------------------------------
// Built-in honeypots
// ---------------------------------------------------------------------------

/// A built-in WASM honeypot: a WASI command (as WAT) and what it prints.
/// Its constants, loop count and data differ every time, so no two have
/// the same module hash or size.
#[derive(Debug, Clone)]
pub struct BuiltinWasm {
    pub wat: String,
    pub stdout: Vec<u8>,
}

/// A WASI command made from `seed` that folds a random data segment and
/// runs a seeded integer loop, then prints the result in hex.
pub fn builtin_wasm(seed: u64) -> BuiltinWasm {
    let mut s = seed;
    let mut next = || {
        s = splitmix(s);
        s
    };
    let a = next();
    let b = next();
    let c = next();
    let m = next() | 1;
    let r = next() % 63 + 1;
    let sh = next() % 63 + 1;
    let iters = 500_000 + next() % 7_500_000;
    let len = (256 + next() % 16_128) as usize / 8 * 8;
    let data: Vec<u8> = (0..len / 8).flat_map(|_| next().to_le_bytes()).collect();

    // The same computation natively.
    let mut x = a;
    let mut y = b;
    for w in data.as_chunks::<8>().0 {
        x = (x ^ u64::from_le_bytes(*w)).wrapping_mul(m);
    }
    for _ in 0..iters {
        x = x.wrapping_mul(m).wrapping_add(c);
        y = (y ^ x).rotate_left(r as u32).wrapping_add(x >> sh);
    }
    let stdout = format!("{:016x}\n", x ^ y).into_bytes();

    let escaped: String = data.iter().map(|b| format!("\\{b:02x}")).collect();
    let end = 1024 + len;
    let wat = format!(
        r#"(module
  (import "wasi_snapshot_preview1" "fd_write" (func $w (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "{escaped}")
  (func (export "_start")
    (local $x i64) (local $y i64) (local $i i64) (local $p i32) (local $d i32)
    (local.set $x (i64.const {a:#x}))
    (local.set $y (i64.const {b:#x}))
    (local.set $p (i32.const 1024))
    (block $fd (loop $fl
      (br_if $fd (i32.ge_u (local.get $p) (i32.const {end})))
      (local.set $x (i64.mul (i64.xor (local.get $x) (i64.load (local.get $p))) (i64.const {m:#x})))
      (local.set $p (i32.add (local.get $p) (i32.const 8)))
      (br $fl)))
    (block $done (loop $round
      (br_if $done (i64.ge_u (local.get $i) (i64.const {iters})))
      (local.set $x (i64.add (i64.mul (local.get $x) (i64.const {m:#x})) (i64.const {c:#x})))
      (local.set $y (i64.add (i64.rotl (i64.xor (local.get $y) (local.get $x)) (i64.const {r}))
                             (i64.shr_u (local.get $x) (i64.const {sh}))))
      (local.set $i (i64.add (local.get $i) (i64.const 1)))
      (br $round)))
    (local.set $x (i64.xor (local.get $x) (local.get $y)))
    (block $hd (loop $hl
      (br_if $hd (i32.ge_u (local.get $d) (i32.const 16)))
      (local.set $p (i32.wrap_i64 (i64.and
        (i64.shr_u (local.get $x)
          (i64.extend_i32_u (i32.sub (i32.const 60) (i32.shl (local.get $d) (i32.const 2)))))
        (i64.const 15))))
      (i32.store8 (i32.add (i32.const 256) (local.get $d))
        (i32.add (local.get $p)
          (select (i32.const 48) (i32.const 87) (i32.lt_u (local.get $p) (i32.const 10)))))
      (local.set $d (i32.add (local.get $d) (i32.const 1)))
      (br $hl)))
    (i32.store8 (i32.const 272) (i32.const 10))
    (i32.store (i32.const 0) (i32.const 256))
    (i32.store (i32.const 4) (i32.const 17))
    (drop (call $w (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8)))))
"#
    );
    BuiltinWasm { wat, stdout }
}

/// A built-in image honeypot: a `sh -c` command for any image with a
/// POSIX shell, and what it prints.
pub fn builtin_shell(seed: u64) -> (Vec<String>, Vec<u8>) {
    const P: u64 = 2_147_483_647;
    let mut s = seed;
    let mut next = || {
        s = splitmix(s);
        s
    };
    let mut x = next() % P;
    let m = 2 + next() % 65_000;
    let c = next() % P;
    let n = 3_000 + next() % 27_000;
    let script = format!(
        "x={x}; i=0; while [ $i -lt {n} ]; do x=$(( (x * {m} + {c}) % {P} )); i=$((i + 1)); done; echo $x"
    );
    for _ in 0..n {
        x = (x * m + c) % P;
    }
    (
        vec!["sh".into(), "-c".into(), script],
        format!("{x}\n").into_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_reward_history_and_punish_strikes() {
        // A new provider with nothing yet: half marks for checks.
        let (s, checks, done) = score(1000, TrustLevel::New, 0, 0, 0, 0, false);
        assert_eq!((checks, done), (500, 1000));
        assert_eq!(s, (1000 + 1000 + 1000) / 4);
        // Twenty passes, all runs done, always online.
        let (good, _, _) = score(1000, TrustLevel::Provider, 20, 0, 50, 0, false);
        // One strike among them.
        let (struck, c, _) = score(1000, TrustLevel::Struck, 20, 1, 50, 0, false);
        assert!(good > struck && c < 1000, "{good} {struck}");
        // Owner machines: full marks for checks.
        assert_eq!(score(500, TrustLevel::Owner, 0, 0, 9, 1, false).1, 1000);
        // Lost runs and downtime count; suspension is zero.
        let (flaky, _, comp) = score(400, TrustLevel::Provider, 20, 0, 5, 5, false);
        assert!(flaky < good && comp < 600);
        assert_eq!(score(1000, TrustLevel::Provider, 20, 0, 50, 0, true).0, 0);
    }

    #[test]
    fn policy_rates_by_level() {
        let mut p = VerifyPolicy::default();
        assert_eq!(p.permille(TrustLevel::Owner), 0);
        assert!(p.permille(TrustLevel::Struck) > p.permille(TrustLevel::New));
        assert!(p.permille(TrustLevel::New) > p.permille(TrustLevel::Provider));
        VerifyPolicyPatch {
            honeypots: Some(false),
            ..Default::default()
        }
        .apply(&mut p);
        assert_eq!(p.permille(TrustLevel::Struck), 0);
        p.new_permille = 1001;
        assert!(p.validate().is_err());
    }

    #[test]
    fn disk_answers_agree_from_data_and_folds() {
        let (seed, nonce) = (7, 99);
        let bytes = 4 * DISK_CHUNK as u64;
        let folds = disk_folds(seed, bytes);
        assert_eq!(folds.len(), 4 * BLOCKS_PER_CHUNK);
        // The agent's way: chunks in order, folded as read.
        let mut chunk = vec![0u8; DISK_CHUNK];
        let mut seq = SequentialAnswer::new(nonce);
        for c in chunk_order(seed, nonce, 4) {
            disk_chunk(seed, c as u64, &mut chunk);
            seq.chunk(&chunk_folds(&chunk));
        }
        let mut want = SequentialAnswer::new(nonce);
        for c in chunk_order(seed, nonce, 4) {
            let c = c as usize;
            want.chunk(&folds[c * BLOCKS_PER_CHUNK..(c + 1) * BLOCKS_PER_CHUNK]);
        }
        assert_eq!(seq.finish(), want.finish());
        // A random walk over the data and over the folds.
        let read = |i: u64| -> Result<u64, ()> {
            let mut c = vec![0u8; DISK_CHUNK];
            disk_chunk(seed, i / BLOCKS_PER_CHUNK as u64, &mut c);
            let o = (i as usize % BLOCKS_PER_CHUNK) * DISK_BLOCK;
            Ok(block_fold(&c[o..o + DISK_BLOCK]))
        };
        let a = random_walk(nonce, 50, folds.len() as u64, read).unwrap();
        let b = random_walk(nonce, 50, folds.len() as u64, |i| {
            Ok::<_, ()>(folds[i as usize])
        })
        .unwrap();
        assert_eq!(a, b);
        assert_ne!(
            a,
            random_walk(nonce + 1, 50, folds.len() as u64, |i| {
                Ok::<_, ()>(folds[i as usize])
            })
            .unwrap()
        );
        assert_ne!(
            chunk_order(seed, nonce, 64),
            chunk_order(seed, nonce + 1, 64)
        );
    }

    #[test]
    fn memory_kernel_is_seeded() {
        let a = memory_kernel(1, 1 << 16, 3);
        assert_eq!(a, memory_kernel(1, 1 << 16, 3));
        assert_ne!(a, memory_kernel(2, 1 << 16, 3));
        assert_ne!(a, memory_kernel(1, 1 << 16, 4));
        assert!(memory_passes(10_000, 64 << 20, 400) >= 1);
        assert_eq!(memory_traffic(1 << 20, 1), (1 << 16) * 8 * 5);
    }

    #[test]
    fn builtins_differ_every_time() {
        let a = builtin_wasm(1);
        let b = builtin_wasm(2);
        assert_ne!(a.wat, b.wat);
        assert_eq!(a.stdout.len(), 17);
        let (cmd, out) = builtin_shell(3);
        assert_eq!(cmd[0], "sh");
        assert!(out.ends_with(b"\n"));
        assert_ne!(builtin_shell(4).1, out);
    }
}
