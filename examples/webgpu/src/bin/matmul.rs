//! Multiplies two N x N matrices on the task's GPU with a tiled WGSL
//! shader, checks the product against the same multiplication on the CPU
//! (inside the wasm sandbox), and reports both speeds:
//!
//!   matmul [N]        (default 512)
//!
//! The inputs come from a fixed seed, so every machine multiplies the same
//! matrices; the CPU result is bit-identical everywhere (Wasmtime runs
//! IEEE floats the same on every CPU), and each GPU must match it within
//! the tolerance. Exits 1 when it does not. Writes `/output/matmul.json`.

use std::time::Instant;

use webgpu_examples::{
    Buffer, Rng, Shader, adapter, dispatch, json_field, max_relative_error, wait,
};

const TOLERANCE: f64 = 1e-3;

const WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> a: array<f32>;
@group(0) @binding(1) var<storage, read> b: array<f32>;
@group(0) @binding(2) var<storage, read_write> c: array<f32>;
@group(0) @binding(3) var<storage, read> dims: array<u32>;

const T: u32 = 16u;
var<workgroup> ta: array<f32, 256>;
var<workgroup> tb: array<f32, 256>;

@compute @workgroup_size(16, 16)
fn main(@builtin(global_invocation_id) gid: vec3<u32>,
        @builtin(local_invocation_id) lid: vec3<u32>) {
    let n = dims[0];
    let row = gid.y;
    let col = gid.x;
    var acc = 0.0;
    let tiles = (n + T - 1u) / T;
    for (var t = 0u; t < tiles; t = t + 1u) {
        let ac = t * T + lid.x;
        let br = t * T + lid.y;
        ta[lid.y * T + lid.x] = select(0.0, a[row * n + ac], row < n && ac < n);
        tb[lid.y * T + lid.x] = select(0.0, b[br * n + col], br < n && col < n);
        workgroupBarrier();
        for (var k = 0u; k < T; k = k + 1u) {
            acc = acc + ta[lid.y * T + k] * tb[k * T + lid.x];
        }
        workgroupBarrier();
    }
    if (row < n && col < n) {
        c[row * n + col] = acc;
    }
}
"#;

fn cpu_matmul(a: &[f32], b: &[f32], n: usize) -> Vec<f32> {
    let mut c = vec![0f32; n * n];
    for i in 0..n {
        let row = &mut c[i * n..(i + 1) * n];
        for k in 0..n {
            let aik = a[i * n + k];
            let bk = &b[k * n..(k + 1) * n];
            for (cj, bkj) in row.iter_mut().zip(bk) {
                *cj += aik * bkj;
            }
        }
    }
    c
}

fn checksum(c: &[f32]) -> f64 {
    c.iter().map(|v| *v as f64).sum()
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("N must be a number"))
        .unwrap_or(512);
    assert!((16..=8192).contains(&n), "N must be between 16 and 8192");
    let Some(info) = adapter() else {
        eprintln!("this task has no GPU: run it with `gpus = 1` (or `pumasi exec --gpu`)");
        std::process::exit(2);
    };
    let name = json_field(&info, "name").unwrap_or_default();
    let backend = json_field(&info, "backend").unwrap_or_default();
    println!("adapter: {name} ({backend})");

    let mut rng = Rng(42);
    let a: Vec<f32> = (0..n * n).map(|_| rng.next_f32()).collect();
    let b: Vec<f32> = (0..n * n).map(|_| rng.next_f32()).collect();
    let flops = 2.0 * (n as f64).powi(3);

    let run = || -> webgpu_examples::Result<(Vec<f32>, f64, f64)> {
        let total = Instant::now();
        let shader = Shader::new(WGSL, "main")?;
        let ga = Buffer::from_f32(&a)?;
        let gb = Buffer::from_f32(&b)?;
        let gc = Buffer::new(n * n * 4)?;
        let dims = Buffer::from_u32(&[n as u32])?;
        let groups = (n as u32).div_ceil(16);
        // Once to compile and warm up, then timed.
        dispatch(&shader, &[&ga, &gb, &gc, &dims], [groups, groups, 1])?;
        wait()?;
        let kernel = Instant::now();
        dispatch(&shader, &[&ga, &gb, &gc, &dims], [groups, groups, 1])?;
        wait()?;
        let kernel_ms = kernel.elapsed().as_secs_f64() * 1e3;
        let c = gc.read_f32()?;
        Ok((c, kernel_ms, total.elapsed().as_secs_f64() * 1e3))
    };
    let (gpu, kernel_ms, total_ms) = match run() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("GPU: {e}");
            std::process::exit(3);
        }
    };

    let start = Instant::now();
    let cpu = cpu_matmul(&a, &b, n);
    let cpu_ms = start.elapsed().as_secs_f64() * 1e3;

    let err = max_relative_error(&gpu, &cpu);
    let ok = err <= TOLERANCE;
    let gflops = |ms: f64| flops / (ms.max(1e-3) * 1e-3) / 1e9;
    println!(
        "matmul {n}x{n}: gpu {kernel_ms:.1} ms ({:.1} GFLOPS; {total_ms:.1} ms with uploads and compile), cpu {cpu_ms:.1} ms ({:.2} GFLOPS), speedup {:.1}x",
        gflops(kernel_ms),
        gflops(cpu_ms),
        cpu_ms / kernel_ms.max(1e-3)
    );
    println!(
        "checksum gpu {:.6e} cpu {:.6e}; max relative error {err:.2e} (tolerance {TOLERANCE:.0e}): {}",
        checksum(&gpu),
        checksum(&cpu),
        if ok { "ok" } else { "MISMATCH" }
    );
    let report = format!(
        "{{\"n\":{n},\"adapter\":{name:?},\"backend\":{backend:?},\"gpu_ms\":{kernel_ms:.3},\"gpu_total_ms\":{total_ms:.3},\"cpu_ms\":{cpu_ms:.3},\"gpu_gflops\":{:.3},\"cpu_gflops\":{:.3},\"max_relative_error\":{err:e},\"checksum_cpu\":{:e},\"checksum_gpu\":{:e},\"ok\":{ok}}}\n",
        gflops(kernel_ms),
        gflops(cpu_ms),
        checksum(&cpu),
        checksum(&gpu)
    );
    if std::path::Path::new("/output").is_dir() {
        let _ = std::fs::write("/output/matmul.json", report);
    }
    if !ok {
        std::process::exit(1);
    }
}
