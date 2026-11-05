//! The GPU benchmark and the GPU challenge on NVIDIA GPUs, through the
//! CUDA driver API loaded at run time (`libcuda.so.1`, `nvcuda.dll`).
//! Nothing needs to be installed beyond the NVIDIA driver: the kernels are
//! PTX text that the driver compiles for whatever GPU it has.
//!
//! - FP32: a kernel of independent fused multiply-add chains, timed.
//! - Memory bandwidth: device-to-device copies, timed.
//! - Hash rate: the challenge kernel ([`pumasi_proto::gpu::gpu_hash`])
//!   with a target nothing reaches, timed. The same kernel answers the
//!   control plane's challenges.
//!
//! The hash kernel is generated from a small list of 32-bit operations
//! ([`hash_ops`]); the tests run that list on the CPU against
//! `gpu_hash`, so the PTX computes exactly what the control plane checks.

use std::{
    ffi::{CString, c_char, c_int, c_uint, c_void},
    fmt::Write as _,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use libloading::Library;
use pumasi_proto::gpu::GpuChallenge;

type CuResult = c_int;
type CuDevice = c_int;
type CuContext = *mut c_void;
type CuModule = *mut c_void;
type CuFunction = *mut c_void;
type CuPtr = u64;

const ATTR_MULTIPROCESSOR_COUNT: c_int = 16;

/// The driver API entry points the benchmark uses.
struct Api {
    _lib: Library,
    init: unsafe extern "C" fn(c_uint) -> CuResult,
    device_count: unsafe extern "C" fn(*mut c_int) -> CuResult,
    device_get: unsafe extern "C" fn(*mut CuDevice, c_int) -> CuResult,
    attribute: unsafe extern "C" fn(*mut c_int, c_int, CuDevice) -> CuResult,
    pci_bus_id: unsafe extern "C" fn(*mut c_char, c_int, CuDevice) -> CuResult,
    ctx_create: unsafe extern "C" fn(*mut CuContext, c_uint, CuDevice) -> CuResult,
    ctx_destroy: unsafe extern "C" fn(CuContext) -> CuResult,
    synchronize: unsafe extern "C" fn() -> CuResult,
    module_load: unsafe extern "C" fn(*mut CuModule, *const c_void) -> CuResult,
    module_unload: unsafe extern "C" fn(CuModule) -> CuResult,
    function: unsafe extern "C" fn(*mut CuFunction, CuModule, *const c_char) -> CuResult,
    alloc: unsafe extern "C" fn(*mut CuPtr, usize) -> CuResult,
    free: unsafe extern "C" fn(CuPtr) -> CuResult,
    memset32: unsafe extern "C" fn(CuPtr, c_uint, usize) -> CuResult,
    dtoh: unsafe extern "C" fn(*mut c_void, CuPtr, usize) -> CuResult,
    dtod: unsafe extern "C" fn(CuPtr, CuPtr, usize) -> CuResult,
    #[allow(clippy::type_complexity)]
    launch: unsafe extern "C" fn(
        CuFunction,
        c_uint,
        c_uint,
        c_uint,
        c_uint,
        c_uint,
        c_uint,
        c_uint,
        *mut c_void,
        *mut *mut c_void,
        *mut *mut c_void,
    ) -> CuResult,
}

fn check(r: CuResult, what: &str) -> Result<()> {
    if r != 0 {
        bail!("{what} failed (CUDA error {r})");
    }
    Ok(())
}

impl Api {
    fn load() -> Result<Self> {
        let names: &[&str] = if cfg!(windows) {
            &["nvcuda.dll"]
        } else {
            &["libcuda.so.1", "libcuda.so"]
        };
        let lib = names
            .iter()
            .find_map(|n| unsafe { Library::new(n) }.ok())
            .context("the CUDA driver library is not installed")?;
        unsafe {
            macro_rules! sym {
                ($name:literal) => {
                    *lib.get(concat!($name, "\0").as_bytes())
                        .context(concat!("CUDA driver has no ", $name))?
                };
            }
            let api = Api {
                init: sym!("cuInit"),
                device_count: sym!("cuDeviceGetCount"),
                device_get: sym!("cuDeviceGet"),
                attribute: sym!("cuDeviceGetAttribute"),
                pci_bus_id: sym!("cuDeviceGetPCIBusId"),
                ctx_create: sym!("cuCtxCreate_v2"),
                ctx_destroy: sym!("cuCtxDestroy_v2"),
                synchronize: sym!("cuCtxSynchronize"),
                module_load: sym!("cuModuleLoadData"),
                module_unload: sym!("cuModuleUnload"),
                function: sym!("cuModuleGetFunction"),
                alloc: sym!("cuMemAlloc_v2"),
                free: sym!("cuMemFree_v2"),
                memset32: sym!("cuMemsetD32_v2"),
                dtoh: sym!("cuMemcpyDtoH_v2"),
                dtod: sym!("cuMemcpyDtoD_v2"),
                launch: sym!("cuLaunchKernel"),
                _lib: lib,
            };
            check((api.init)(0), "cuInit")?;
            Ok(api)
        }
    }
}

/// A CUDA context on one GPU, with the benchmark kernels loaded.
pub struct Gpu {
    api: Api,
    ctx: CuContext,
    module: CuModule,
    fma: CuFunction,
    hash: CuFunction,
    sms: u32,
}

impl Drop for Gpu {
    fn drop(&mut self) {
        unsafe {
            (self.api.module_unload)(self.module);
            (self.api.ctx_destroy)(self.ctx);
        }
    }
}

/// Device memory freed on drop.
struct Buf<'a> {
    api: &'a Api,
    ptr: CuPtr,
}

impl<'a> Buf<'a> {
    fn new(api: &'a Api, bytes: usize) -> Result<Self> {
        let mut ptr = 0;
        check(unsafe { (api.alloc)(&mut ptr, bytes) }, "cuMemAlloc")?;
        Ok(Self { api, ptr })
    }
}

impl Drop for Buf<'_> {
    fn drop(&mut self) {
        unsafe { (self.api.free)(self.ptr) };
    }
}

/// What the benchmark measured on one GPU.
#[derive(Debug, Clone, Copy, Default)]
pub struct Measured {
    pub fp32_gflops: u32,
    pub mem_bw_gb_s: u32,
    pub hash_rate: u64,
}

const BLOCK: u32 = 256;
/// Hashes per thread per launch of the hash kernel.
const PER_THREAD: u32 = 64;
/// Solutions the hash kernel can store per launch.
const CAP: u32 = 1024;

impl Gpu {
    /// Opens the GPU at PCI address `pci` (`0000:01:00.0`).
    pub fn open(pci: &str) -> Result<Self> {
        let api = Api::load()?;
        unsafe {
            let mut n = 0;
            check((api.device_count)(&mut n), "cuDeviceGetCount")?;
            let mut found = None;
            for i in 0..n {
                let mut d = 0;
                check((api.device_get)(&mut d, i), "cuDeviceGet")?;
                let mut buf = [0 as c_char; 32];
                if (api.pci_bus_id)(buf.as_mut_ptr(), buf.len() as c_int, d) == 0 {
                    let id: String = buf
                        .iter()
                        .take_while(|&&c| c != 0)
                        .map(|&c| c as u8 as char)
                        .collect();
                    if super::nvml::normalize_pci(&id) == pci {
                        found = Some(d);
                        break;
                    }
                }
            }
            let dev = found.with_context(|| format!("CUDA does not see the GPU at {pci}"))?;
            let mut sms = 0;
            check(
                (api.attribute)(&mut sms, ATTR_MULTIPROCESSOR_COUNT, dev),
                "cuDeviceGetAttribute",
            )?;
            let mut ctx = std::ptr::null_mut();
            check((api.ctx_create)(&mut ctx, 0, dev), "cuCtxCreate")?;
            let ptx = CString::new(module_ptx()).expect("no NUL in PTX");
            let mut module = std::ptr::null_mut();
            let r = (api.module_load)(&mut module, ptx.as_ptr().cast());
            if r != 0 {
                (api.ctx_destroy)(ctx);
                bail!("the CUDA driver could not compile the benchmark (CUDA error {r})");
            }
            let mut fma = std::ptr::null_mut();
            let mut hash = std::ptr::null_mut();
            check(
                (api.function)(&mut fma, module, c"pumasi_fma".as_ptr()),
                "cuModuleGetFunction",
            )?;
            check(
                (api.function)(&mut hash, module, c"pumasi_hash".as_ptr()),
                "cuModuleGetFunction",
            )?;
            Ok(Self {
                api,
                ctx,
                module,
                fma,
                hash,
                sms: sms.max(1) as u32,
            })
        }
    }

    fn launch(&self, f: CuFunction, blocks: u32, params: &mut [*mut c_void]) -> Result<()> {
        unsafe {
            check(
                (self.api.launch)(
                    f,
                    blocks,
                    1,
                    1,
                    BLOCK,
                    1,
                    1,
                    0,
                    std::ptr::null_mut(),
                    params.as_mut_ptr(),
                    std::ptr::null_mut(),
                ),
                "cuLaunchKernel",
            )?;
            check((self.api.synchronize)(), "cuCtxSynchronize")
        }
    }

    /// FP32 GFLOPS: the best of a few runs of about `target` each.
    pub fn fp32_gflops(&self, target: Duration) -> Result<u32> {
        let blocks = self.sms * 8;
        let threads = (blocks * BLOCK) as usize;
        let out = Buf::new(&self.api, threads * 4)?;
        let mut iters: u32 = 256;
        let mut best = 0f64;
        for _ in 0..12 {
            let mut ptr = out.ptr;
            let mut n = iters;
            let mut add: f32 = 1e-3;
            let mut params: [*mut c_void; 3] = [
                (&raw mut ptr).cast(),
                (&raw mut n).cast(),
                (&raw mut add).cast(),
            ];
            let start = Instant::now();
            self.launch(self.fma, blocks, &mut params)?;
            let took = start.elapsed();
            // 32 fused multiply-adds per iteration, two FLOPs each.
            let flops = threads as f64 * iters as f64 * 64.0;
            best = best.max(flops / took.as_secs_f64() / 1e9);
            if took < target / 2 {
                iters = iters.saturating_mul(2);
            } else if took >= target {
                break;
            }
        }
        Ok(best.min(u32::MAX as f64) as u32)
    }

    /// Device memory bandwidth in GB/s (bytes read plus written).
    pub fn mem_bw(&self, vram_mib: u64) -> Result<u32> {
        let bytes = ((vram_mib << 20) / 8).clamp(16 << 20, 512 << 20) as usize;
        let a = Buf::new(&self.api, bytes)?;
        let b = Buf::new(&self.api, bytes)?;
        unsafe {
            check((self.api.memset32)(a.ptr, 1, bytes / 4), "cuMemsetD32")?;
            check((self.api.dtod)(b.ptr, a.ptr, bytes), "cuMemcpyDtoD")?;
            check((self.api.synchronize)(), "cuCtxSynchronize")?;
            let rounds = 8;
            let start = Instant::now();
            for _ in 0..rounds {
                check((self.api.dtod)(b.ptr, a.ptr, bytes), "cuMemcpyDtoD")?;
            }
            check((self.api.synchronize)(), "cuCtxSynchronize")?;
            let secs = start.elapsed().as_secs_f64().max(1e-6);
            Ok((2.0 * bytes as f64 * rounds as f64 / secs / 1e9) as u32)
        }
    }

    /// Threads per launch of the hash kernel: a power of two, so launches
    /// tile the 32-bit range of `x`'s low half.
    fn hash_blocks(&self) -> u32 {
        (self.sms * 8).next_power_of_two().clamp(64, 1 << 14)
    }

    /// Runs the hash kernel over `[base, base + per launch)`: the
    /// solutions below `target` it found.
    fn hash_launch(
        &self,
        seed: u64,
        base: u64,
        target: u64,
        count: &Buf,
        out: &Buf,
    ) -> Result<Vec<u64>> {
        unsafe {
            check((self.api.memset32)(count.ptr, 0, 1), "cuMemsetD32")?;
        }
        let mut seed_lo = seed as u32;
        let mut seed_hi = (seed >> 32) as u32;
        let mut base_lo = base as u32;
        let mut base_hi = (base >> 32) as u32;
        let mut target_hi = (target >> 32) as u32;
        let mut target_lo = target as u32;
        let mut per_thread = PER_THREAD;
        let mut count_ptr = count.ptr;
        let mut out_ptr = out.ptr;
        let mut cap = CAP;
        let mut params: [*mut c_void; 10] = [
            (&raw mut seed_lo).cast(),
            (&raw mut seed_hi).cast(),
            (&raw mut base_lo).cast(),
            (&raw mut base_hi).cast(),
            (&raw mut target_hi).cast(),
            (&raw mut target_lo).cast(),
            (&raw mut per_thread).cast(),
            (&raw mut count_ptr).cast(),
            (&raw mut out_ptr).cast(),
            (&raw mut cap).cast(),
        ];
        self.launch(self.hash, self.hash_blocks(), &mut params)?;
        let mut n: u32 = 0;
        unsafe {
            check(
                (self.api.dtoh)((&raw mut n).cast(), count.ptr, 4),
                "cuMemcpyDtoH",
            )?;
        }
        let n = n.min(CAP) as usize;
        let mut words = vec![0u32; n * 2];
        if n > 0 {
            unsafe {
                check(
                    (self.api.dtoh)(words.as_mut_ptr().cast(), out.ptr, n * 8),
                    "cuMemcpyDtoH",
                )?;
            }
        }
        Ok(words
            .chunks(2)
            .map(|w| ((w[1] as u64) << 32) | w[0] as u64)
            .collect())
    }

    fn per_launch(&self) -> u64 {
        self.hash_blocks() as u64 * BLOCK as u64 * PER_THREAD as u64
    }

    /// Challenge hashes per second.
    pub fn hash_rate(&self, target: Duration) -> Result<u64> {
        let count = Buf::new(&self.api, 4)?;
        let out = Buf::new(&self.api, CAP as usize * 8)?;
        // Warm up (the first launch compiles and loads).
        self.hash_launch(1, 0, 0, &count, &out)?;
        let start = Instant::now();
        let mut hashes = 0u64;
        let mut base = 0u64;
        while start.elapsed() < target {
            self.hash_launch(rand_seed(), base, 0, &count, &out)?;
            hashes += self.per_launch();
            base = base.wrapping_add(self.per_launch());
        }
        Ok((hashes as f64 / start.elapsed().as_secs_f64()) as u64)
    }

    /// Answers a challenge: the first solutions found scanning `x` up.
    pub fn solve(&self, c: &GpuChallenge) -> Result<Vec<u64>> {
        ensure!(
            c.solutions as usize <= CAP as usize,
            "too many solutions asked"
        );
        let count = Buf::new(&self.api, 4)?;
        let out = Buf::new(&self.api, CAP as usize * 8)?;
        let mut found: Vec<u64> = Vec::new();
        let mut base = 0u64;
        let deadline = Instant::now() + Duration::from_secs(120);
        while found.len() < c.solutions as usize {
            ensure!(Instant::now() < deadline, "the GPU check took too long");
            found.extend(self.hash_launch(c.seed, base, c.target, &count, &out)?);
            base += self.per_launch();
        }
        found.sort_unstable();
        found.dedup();
        found.truncate(c.solutions as usize);
        Ok(found)
    }

    /// Measures FP32, memory bandwidth and the hash rate.
    pub fn measure(&self, vram_mib: u64) -> Result<Measured> {
        Ok(Measured {
            fp32_gflops: self.fp32_gflops(Duration::from_millis(400))?,
            mem_bw_gb_s: self.mem_bw(vram_mib).unwrap_or(0),
            hash_rate: self.hash_rate(Duration::from_millis(500))?,
        })
    }
}

fn rand_seed() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    t.as_nanos() as u64 ^ 0x9e37_79b9_7f4a_7c15
}

// ---------------------------------------------------------------------------
// PTX
// ---------------------------------------------------------------------------

const FMA_CHAINS: usize = 8;
const FMA_UNROLL: usize = 4;

fn fma_ptx(p: &mut String) {
    let _ = writeln!(
        p,
        ".visible .entry pumasi_fma(.param .u64 out, .param .u32 iters, .param .f32 add)
{{
  .reg .pred %p<2>;
  .reg .f32 %f<{n}>;
  .reg .b32 %r<8>;
  .reg .b64 %rd<4>;
  ld.param.u64 %rd1, [out];
  ld.param.u32 %r1, [iters];
  ld.param.f32 %f0, [add];
  mov.u32 %r2, %ctaid.x;
  mov.u32 %r3, %ntid.x;
  mov.u32 %r4, %tid.x;
  mad.lo.u32 %r5, %r2, %r3, %r4;
  cvt.rn.f32.u32 %f1, %r5;
  mul.f32 %f1, %f1, 0f3A83126F;",
        n = FMA_CHAINS + 2
    );
    for i in 2..=FMA_CHAINS {
        let _ = writeln!(p, "  add.f32 %f{i}, %f{}, %f0;", i - 1);
    }
    let m = FMA_CHAINS + 1;
    let _ = writeln!(
        p,
        "  mov.f32 %f{m}, 0f3F7FFFF0;\n  mov.u32 %r6, 0;\nFMA_LOOP:"
    );
    for _ in 0..FMA_UNROLL {
        for i in 1..=FMA_CHAINS {
            let _ = writeln!(p, "  fma.rn.f32 %f{i}, %f{i}, %f{m}, %f0;");
        }
    }
    let _ = writeln!(
        p,
        "  add.u32 %r6, %r6, 1;\n  setp.lt.u32 %p1, %r6, %r1;\n  @%p1 bra FMA_LOOP;"
    );
    for i in 2..=FMA_CHAINS {
        let _ = writeln!(p, "  add.f32 %f1, %f1, %f{i};");
    }
    let _ = writeln!(
        p,
        "  cvta.to.global.u64 %rd2, %rd1;
  mul.wide.u32 %rd3, %r5, 4;
  add.u64 %rd2, %rd2, %rd3;
  st.global.f32 [%rd2], %f1;
  ret;
}}"
    );
}

/// One 32-bit operation of the hash, on named registers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    /// dst = a ^ b
    Xor(&'static str, &'static str, &'static str),
    /// dst = a ^ constant
    XorK(&'static str, &'static str, u32),
    /// dst = a >> k
    Shr(&'static str, &'static str, u32),
    /// dst = a << k
    Shl(&'static str, &'static str, u32),
    /// dst = a | b
    Or(&'static str, &'static str, &'static str),
    /// dst = a * constant (low 32 bits)
    MulK(&'static str, &'static str, u32),
}

fn mix_ops(ops: &mut Vec<Op>, h: &'static str) {
    ops.extend([
        Op::Shr("t", h, 16),
        Op::Xor(h, h, "t"),
        Op::MulK(h, h, 0x7feb_352d),
        Op::Shr("t", h, 15),
        Op::Xor(h, h, "t"),
        Op::MulK(h, h, 0x846c_a68b),
        Op::Shr("t", h, 16),
        Op::Xor(h, h, "t"),
    ]);
}

/// The hash's rounds, from `a` and `b` (seed xor x, low and high halves)
/// to the result's high (`a`) and low (`b`) halves. Uses `t` and `u` as
/// temporaries.
pub(crate) fn hash_ops() -> Vec<Op> {
    let mut ops = Vec::new();
    for r in 0..4u32 {
        // a = mix(a ^ rotl(b, 13) ^ r * golden)
        ops.extend([
            Op::Shl("u", "b", 13),
            Op::Shr("t", "b", 19),
            Op::Or("u", "u", "t"),
            Op::Xor("a", "a", "u"),
            Op::XorK("a", "a", r.wrapping_mul(0x9e37_79b9)),
        ]);
        mix_ops(&mut ops, "a");
        // b = mix(b ^ rotl(a, 7) ^ constant)
        ops.extend([
            Op::Shl("u", "a", 7),
            Op::Shr("t", "a", 25),
            Op::Or("u", "u", "t"),
            Op::Xor("b", "b", "u"),
            Op::XorK("b", "b", 0x85eb_ca6b),
        ]);
        mix_ops(&mut ops, "b");
    }
    ops
}

fn reg(name: &str) -> &'static str {
    match name {
        "a" => "%ha",
        "b" => "%hb",
        "t" => "%ht",
        _ => "%hu",
    }
}

fn hash_ptx(p: &mut String) {
    let _ = writeln!(
        p,
        ".visible .entry pumasi_hash(
  .param .u32 seed_lo, .param .u32 seed_hi,
  .param .u32 base_lo, .param .u32 base_hi,
  .param .u32 target_hi, .param .u32 target_lo,
  .param .u32 per_thread,
  .param .u64 count_ptr, .param .u64 out_ptr, .param .u32 cap)
{{
  .reg .pred %p<6>;
  .reg .b32 %ha, %hb, %ht, %hu;
  .reg .b32 %s<4>;
  .reg .b32 %r<16>;
  .reg .b64 %rd<8>;
  ld.param.u32 %s0, [seed_lo];
  ld.param.u32 %s1, [seed_hi];
  ld.param.u32 %r1, [base_lo];
  ld.param.u32 %r2, [base_hi];
  ld.param.u32 %s2, [target_hi];
  ld.param.u32 %s3, [target_lo];
  ld.param.u32 %r3, [per_thread];
  ld.param.u64 %rd1, [count_ptr];
  ld.param.u64 %rd2, [out_ptr];
  ld.param.u32 %r4, [cap];
  cvta.to.global.u64 %rd1, %rd1;
  cvta.to.global.u64 %rd2, %rd2;
  mov.u32 %r5, %ctaid.x;
  mov.u32 %r6, %ntid.x;
  mov.u32 %r7, %tid.x;
  mad.lo.u32 %r8, %r5, %r6, %r7;
  mov.u32 %r9, %nctaid.x;
  mul.lo.u32 %r10, %r9, %r6;
  add.u32 %r11, %r1, %r8;
  mov.u32 %r12, 0;
HASH_LOOP:
  xor.b32 %ha, %s0, %r11;
  xor.b32 %hb, %s1, %r2;"
    );
    for op in hash_ops() {
        let line = match op {
            Op::Xor(d, a, b) => format!("xor.b32 {}, {}, {};", reg(d), reg(a), reg(b)),
            Op::XorK(d, a, k) => format!("xor.b32 {}, {}, {k};", reg(d), reg(a)),
            Op::Shr(d, a, k) => format!("shr.u32 {}, {}, {k};", reg(d), reg(a)),
            Op::Shl(d, a, k) => format!("shl.b32 {}, {}, {k};", reg(d), reg(a)),
            Op::Or(d, a, b) => format!("or.b32 {}, {}, {};", reg(d), reg(a), reg(b)),
            Op::MulK(d, a, k) => format!("mul.lo.u32 {}, {}, {k};", reg(d), reg(a)),
        };
        let _ = writeln!(p, "  {line}");
    }
    // hit = a < target_hi || (a == target_hi && b < target_lo)
    let _ = writeln!(
        p,
        "  setp.lt.u32 %p1, %ha, %s2;
  setp.eq.u32 %p2, %ha, %s2;
  setp.lt.u32 %p3, %hb, %s3;
  and.pred %p2, %p2, %p3;
  or.pred %p1, %p1, %p2;
  @!%p1 bra HASH_NEXT;
  atom.global.add.u32 %r13, [%rd1], 1;
  setp.ge.u32 %p4, %r13, %r4;
  @%p4 bra HASH_NEXT;
  mul.wide.u32 %rd3, %r13, 8;
  add.u64 %rd4, %rd2, %rd3;
  st.global.u32 [%rd4], %r11;
  st.global.u32 [%rd4+4], %r2;
HASH_NEXT:
  add.u32 %r11, %r11, %r10;
  add.u32 %r12, %r12, 1;
  setp.lt.u32 %p5, %r12, %r3;
  @%p5 bra HASH_LOOP;
  ret;
}}"
    );
}

/// Both kernels as one PTX module. Target `sm_50` (Maxwell, 2014) and
/// newer: the driver compiles it for the GPU it has.
pub(crate) fn module_ptx() -> String {
    let mut p = String::from(".version 6.0\n.target sm_50\n.address_size 64\n\n");
    fma_ptx(&mut p);
    p.push('\n');
    hash_ptx(&mut p);
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::gpu::gpu_hash;

    /// Runs the hash's operations the way the GPU does.
    fn run_ops(seed: u64, x: u64) -> u64 {
        let mut regs = std::collections::HashMap::from([
            ("a", (seed as u32) ^ (x as u32)),
            ("b", ((seed >> 32) as u32) ^ ((x >> 32) as u32)),
            ("t", 0u32),
            ("u", 0u32),
        ]);
        for op in hash_ops() {
            let (d, v) = match op {
                Op::Xor(d, a, b) => (d, regs[a] ^ regs[b]),
                Op::XorK(d, a, k) => (d, regs[a] ^ k),
                Op::Shr(d, a, k) => (d, regs[a] >> k),
                Op::Shl(d, a, k) => (d, regs[a] << k),
                Op::Or(d, a, b) => (d, regs[a] | regs[b]),
                Op::MulK(d, a, k) => (d, regs[a].wrapping_mul(k)),
            };
            regs.insert(d, v);
        }
        ((regs["a"] as u64) << 32) | regs["b"] as u64
    }

    #[test]
    fn the_kernel_computes_the_challenge_hash() {
        for (seed, x) in [
            (0u64, 0u64),
            (42, 7),
            (0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210),
            (u64::MAX, u64::MAX),
        ] {
            assert_eq!(run_ops(seed, x), gpu_hash(seed, x), "{seed:x} {x:x}");
        }
    }

    #[test]
    fn ptx_is_well_formed_text() {
        let p = module_ptx();
        assert!(p.starts_with(".version"));
        assert_eq!(p.matches('{').count(), p.matches('}').count());
        for name in ["pumasi_fma", "pumasi_hash", "HASH_LOOP:", "FMA_LOOP:"] {
            assert!(p.contains(name), "{name}");
        }
        // 32 multiply-adds per FMA iteration, as the FLOP count assumes.
        assert_eq!(p.matches("fma.rn.f32").count(), FMA_CHAINS * FMA_UNROLL);
        assert_eq!(FMA_CHAINS * FMA_UNROLL * 2, 64);
        // Every register the hash names is declared.
        for line in p.lines().filter(|l| l.contains("%h")) {
            for r in line.split(|c: char| !c.is_alphanumeric() && c != '%') {
                if r.starts_with("%h") {
                    assert!(["%ha", "%hb", "%ht", "%hu"].contains(&r), "{line}");
                }
            }
        }
    }
}
