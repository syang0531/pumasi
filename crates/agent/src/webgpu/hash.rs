//! The GPU challenge over WebGPU (1.10): the same proof of work as the
//! CUDA kernel ([`pumasi_proto::gpu::gpu_hash`]: 32-bit multiplies, xors,
//! shifts and rotates, which WGSL has), so the cluster can time GPUs that
//! have no CUDA (AMD, Intel, Apple, any WebGPU adapter) and bill them at
//! what it timed instead of a cap.
//!
//! Each invocation hashes [`PER_THREAD`] consecutive values of `x` and
//! appends those whose hash is below the target to a buffer with an atomic
//! counter. Dispatches are sized to take about [`STEP`], well under the
//! drivers' watchdogs (Windows resets a GPU stuck for 2 seconds).

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::gpu::{GpuChallenge, gpu_hash};

use super::worker::open_device;

const WORKGROUP: u32 = 64;
/// Values of `x` each invocation hashes.
pub const PER_THREAD: u32 = 256;
/// Target length of one dispatch.
const STEP: Duration = Duration::from_millis(100);
/// How long the hash rate is measured for, at least.
const MEASURE: Duration = Duration::from_millis(300);
/// Solutions one dispatch can return.
const CAP: u32 = 1024;
/// Most workgroups in one dispatch: `x` offsets stay within 32 bits.
const MAX_GROUPS: u32 = (1 << 24) / WORKGROUP;

pub const HASH_WGSL: &str = r#"
struct Params {
    seed_lo: u32, seed_hi: u32,
    target_lo: u32, target_hi: u32,
    base_lo: u32, base_hi: u32,
    groups_x: u32, cap: u32,
}

@group(0) @binding(0) var<uniform> p: Params;
@group(0) @binding(1) var<storage, read_write> count: atomic<u32>;
@group(0) @binding(2) var<storage, read_write> found: array<u32>;

fn mix(v: u32) -> u32 {
    var h = v;
    h = h ^ (h >> 16u);
    h = h * 0x7feb352du;
    h = h ^ (h >> 15u);
    h = h * 0x846ca68bu;
    h = h ^ (h >> 16u);
    return h;
}

fn rotl(v: u32, n: u32) -> u32 {
    return (v << n) | (v >> (32u - n));
}

@compute @workgroup_size(64)
fn main(@builtin(workgroup_id) wg: vec3<u32>, @builtin(local_invocation_index) li: u32) {
    let group = wg.x + wg.y * p.groups_x;
    let offset = (group * 64u + li) * 256u;
    let lo0 = p.base_lo + offset;
    let hi0 = p.base_hi + select(0u, 1u, lo0 < offset);
    for (var i = 0u; i < 256u; i = i + 1u) {
        let xl = lo0 + i;
        let xh = hi0 + select(0u, 1u, xl < lo0);
        var a = p.seed_lo ^ xl;
        var b = p.seed_hi ^ xh;
        for (var r = 0u; r < 4u; r = r + 1u) {
            a = mix(a ^ rotl(b, 13u) ^ (r * 0x9e3779b9u));
            b = mix(b ^ rotl(a, 7u) ^ 0x85ebca6bu);
        }
        if (a < p.target_hi || (a == p.target_hi && b < p.target_lo)) {
            let k = atomicAdd(&count, 1u);
            if (k < p.cap) {
                found[2u * k] = xl;
                found[2u * k + 1u] = xh;
            }
        }
    }
}
"#;

/// The challenge kernel, compiled on one adapter's device.
pub struct Hasher {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    params: wgpu::Buffer,
    count: wgpu::Buffer,
    found: wgpu::Buffer,
    staging: wgpu::Buffer,
    group: wgpu::BindGroup,
    max_x: u32,
}

impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hasher")
    }
}

impl Hasher {
    pub fn new(adapter: &wgpu::Adapter) -> Result<Self> {
        let (device, queue) = open_device(adapter)?;
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("challenge"),
            source: wgpu::ShaderSource::Wgsl(HASH_WGSL.into()),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("challenge"),
            layout: None,
            module: &module,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        let buffer = |label, size, usage| {
            device.create_buffer(&wgpu::BufferDescriptor {
                label: Some(label),
                size,
                usage,
                mapped_at_creation: false,
            })
        };
        use wgpu::BufferUsages as U;
        let params = buffer("params", 32, U::UNIFORM | U::COPY_DST);
        let count = buffer("count", 4, U::STORAGE | U::COPY_SRC | U::COPY_DST);
        let found_bytes = CAP as u64 * 8;
        let found = buffer("found", found_bytes, U::STORAGE | U::COPY_SRC);
        let staging = buffer("staging", 4 + found_bytes, U::MAP_READ | U::COPY_DST);
        let group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("challenge"),
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: params.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: count.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: found.as_entire_binding(),
                },
            ],
        });
        if let Some(e) = pollster::block_on(scope.pop()) {
            bail!("the challenge kernel: {e}");
        }
        let max_x = device.limits().max_compute_workgroups_per_dimension.max(1);
        Ok(Self {
            device,
            queue,
            pipeline,
            params,
            count,
            found,
            staging,
            group,
            max_x,
        })
    }

    /// Hashes `groups * 64 * 256` values of `x` from `base`. Returns how
    /// many it hashed, the values below `target` it found (at most
    /// [`CAP`]) and how long the GPU took.
    fn run(
        &self,
        seed: u64,
        target: u64,
        base: u64,
        groups: u32,
    ) -> Result<(u64, Vec<u64>, Duration)> {
        let groups = groups.clamp(1, MAX_GROUPS);
        let gx = groups.min(self.max_x);
        let gy = groups.div_ceil(gx);
        let words = [
            seed as u32,
            (seed >> 32) as u32,
            target as u32,
            (target >> 32) as u32,
            base as u32,
            (base >> 32) as u32,
            gx,
            CAP,
        ];
        let bytes: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
        self.queue.write_buffer(&self.params, 0, &bytes);
        self.queue.write_buffer(&self.count, 0, &[0; 4]);
        let mut enc = self.device.create_command_encoder(&Default::default());
        {
            let mut pass = enc.begin_compute_pass(&Default::default());
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.group, &[]);
            pass.dispatch_workgroups(gx, gy, 1);
        }
        enc.copy_buffer_to_buffer(&self.count, 0, &self.staging, 0, 4);
        enc.copy_buffer_to_buffer(&self.found, 0, &self.staging, 4, CAP as u64 * 8);
        let start = Instant::now();
        let index = self.queue.submit([enc.finish()]);
        let slice = self.staging.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |r| {
            let _ = tx.send(r);
        });
        self.device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(index),
                timeout: Some(Duration::from_secs(20)),
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let took = start.elapsed();
        rx.recv_timeout(Duration::from_secs(5))
            .context("the GPU did not hand the answer back")?
            .context("read the answer")?;
        let data = slice
            .get_mapped_range()
            .map_err(|e| anyhow::anyhow!("read the answer: {e}"))?
            .to_vec();
        self.staging.unmap();
        let word = |i: usize| u32::from_le_bytes(data[i * 4..i * 4 + 4].try_into().unwrap());
        let n = word(0).min(CAP) as usize;
        let xs = (0..n)
            .map(|k| word(1 + 2 * k) as u64 | (word(2 + 2 * k) as u64) << 32)
            .collect();
        let hashed = gx as u64 * gy as u64 * WORKGROUP as u64 * PER_THREAD as u64;
        Ok((hashed, xs, took))
    }

    /// Workgroups that take about [`STEP`] at `rate` hashes per second.
    fn groups_for(rate: u64) -> u32 {
        let per_group = WORKGROUP as u64 * PER_THREAD as u64;
        ((rate as u128 * STEP.as_millis() / 1000 / per_group as u128) as u64)
            .clamp(1, MAX_GROUPS as u64) as u32
    }

    /// Hashes per second, the way a challenge is solved (nothing is below
    /// a target of 0, so nothing is written).
    pub fn hash_rate(&self) -> Result<u64> {
        self.run(1, 0, 0, 1)?; // compiles the pipeline
        let mut groups = 16u32;
        let mut hashed = 0u64;
        let mut busy = Duration::ZERO;
        let mut best = 0f64;
        let started = Instant::now();
        loop {
            let seed = (groups as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let (n, _, took) = self.run(seed, 0, 0, groups)?;
            hashed += n;
            busy += took;
            best = best.max(n as f64 / took.as_secs_f64().max(1e-6));
            if (busy >= MEASURE && took >= STEP / 2)
                || groups >= MAX_GROUPS
                || started.elapsed() > Duration::from_secs(5)
            {
                break;
            }
            if took < STEP {
                let grow = (STEP.as_secs_f64() / took.as_secs_f64().max(1e-4)).clamp(1.0, 64.0);
                groups = ((groups as f64 * grow) as u64).clamp(1, MAX_GROUPS as u64) as u32;
            }
        }
        ensure!(hashed > 0, "nothing hashed");
        Ok(best.round() as u64)
    }

    /// Finds the challenge's solutions, checking each against the CPU's
    /// hash so a GPU that computes it wrong never sends a wrong answer.
    /// `rate` (the measured hash rate, or 0) sizes the dispatches.
    pub fn solve(&self, c: &GpuChallenge, rate: u64) -> Result<Vec<u64>> {
        let mut groups = if rate > 0 { Self::groups_for(rate) } else { 64 };
        let mut base = 0u64;
        let mut found: Vec<u64> = Vec::new();
        let started = Instant::now();
        while found.len() < c.solutions as usize {
            ensure!(
                started.elapsed() < Duration::from_secs(55),
                "the challenge took too long"
            );
            let (n, xs, took) = self.run(c.seed, c.target, base, groups)?;
            for x in xs {
                ensure!(
                    gpu_hash(c.seed, x) < c.target,
                    "this GPU computes the challenge hash wrong"
                );
                found.push(x);
            }
            base = base.checked_add(n).context("ran out of values")?;
            if rate == 0 && took < STEP {
                groups = groups.saturating_mul(2).min(MAX_GROUPS);
            }
        }
        found.sort_unstable();
        found.dedup();
        found.truncate(c.solutions as usize);
        ensure!(
            found.len() == c.solutions as usize,
            "found too few distinct values"
        );
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::gpu::{check_answer, solve_cpu, target_for};

    #[test]
    fn the_wgsl_kernel_agrees_with_the_cpu() {
        let Some(adapter) = super::super::tests::any_adapter() else {
            return;
        };
        let h = Hasher::new(&adapter).unwrap();
        let rate = h.hash_rate().unwrap();
        assert!(rate > 0);
        let c = GpuChallenge {
            nonce: 1,
            device: 0,
            seed: 0x0123_4567_89ab_cdef,
            target: target_for(64 * 2000, 64),
            solutions: 64,
        };
        let xs = h.solve(&c, 0).unwrap();
        assert!(check_answer(&c, &xs), "{xs:?}");
        // The first solutions are the CPU's first solutions: nothing was
        // skipped (the kernel scans from 0 like the CPU).
        let cpu = solve_cpu(c.seed, c.target, 8);
        assert_eq!(&xs[..8], &cpu[..]);
        // Values across 2^32 carry into the high word correctly: one
        // workgroup's 16384 values around it give exactly the CPU's.
        let target = target_for(100 * 64, 64);
        let base = (1u64 << 32) - 8192;
        let (n, mut got, _) = h.run(c.seed, target, base, 1).unwrap();
        got.sort_unstable();
        let want: Vec<u64> = (base..base + n)
            .filter(|&x| gpu_hash(c.seed, x) < target)
            .collect();
        assert!(want.iter().any(|&x| x >> 32 == 1) && want.iter().any(|&x| x >> 32 == 0));
        assert_eq!(got, want);
    }
}
