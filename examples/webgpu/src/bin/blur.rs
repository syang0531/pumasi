//! Gaussian blur of an image on the task's GPU (two WGSL passes), checked
//! against the same blur on the CPU, with both speeds reported:
//!
//!   blur [RADIUS]     (default 8)
//!
//! Reads the first binary PPM (`P6`) in `/input`, or draws a 1024 x 1024
//! test pattern when there is none, and writes `/output/blurred.ppm`.
//! Exits 1 when the GPU's image differs from the CPU's beyond the
//! tolerance.

use std::time::Instant;

use webgpu_examples::{Buffer, Shader, adapter, dispatch, json_field, max_relative_error, wait};

const TOLERANCE: f64 = 1e-3;

const WGSL: &str = r#"
@group(0) @binding(0) var<storage, read> src: array<f32>;
@group(0) @binding(1) var<storage, read_write> dst: array<f32>;
@group(0) @binding(2) var<storage, read> p: array<u32>;
@group(0) @binding(3) var<storage, read> w: array<f32>;

@compute @workgroup_size(16, 16)
fn main(@builtin(global_invocation_id) g: vec3<u32>) {
    let width = p[0];
    let height = p[1];
    let r = i32(p[2]);
    let horizontal = p[3] == 1u;
    if (g.x >= width || g.y >= height) {
        return;
    }
    let plane = width * height;
    for (var ch = 0u; ch < 3u; ch = ch + 1u) {
        var acc = 0.0;
        for (var k = -r; k <= r; k = k + 1) {
            var x = i32(g.x);
            var y = i32(g.y);
            if (horizontal) {
                x = clamp(x + k, 0, i32(width) - 1);
            } else {
                y = clamp(y + k, 0, i32(height) - 1);
            }
            acc = acc + w[u32(k + r)] * src[ch * plane + u32(y) * width + u32(x)];
        }
        dst[ch * plane + g.y * width + g.x] = acc;
    }
}
"#;

struct Image {
    width: usize,
    height: usize,
    /// Planar R, G, B, each `width * height` floats in 0..=255.
    planes: Vec<f32>,
}

fn parse_ppm(data: &[u8]) -> Option<Image> {
    let mut fields = Vec::new();
    let mut i = 0;
    while fields.len() < 4 && i < data.len() {
        while i < data.len() && data[i].is_ascii_whitespace() {
            i += 1;
        }
        if data.get(i) == Some(&b'#') {
            while i < data.len() && data[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        while i < data.len() && !data[i].is_ascii_whitespace() {
            i += 1;
        }
        fields.push(std::str::from_utf8(&data[start..i]).ok()?.to_string());
    }
    if fields.first()? != "P6" || fields.get(3)? != "255" {
        return None;
    }
    let (width, height): (usize, usize) = (fields[1].parse().ok()?, fields[2].parse().ok()?);
    let pixels = data.get(i + 1..i + 1 + width * height * 3)?;
    let mut planes = vec![0f32; width * height * 3];
    for (p, rgb) in pixels.chunks_exact(3).enumerate() {
        for ch in 0..3 {
            planes[ch * width * height + p] = rgb[ch] as f32;
        }
    }
    Some(Image {
        width,
        height,
        planes,
    })
}

fn test_pattern() -> Image {
    let (width, height) = (1024, 1024);
    let mut planes = vec![0f32; width * height * 3];
    for y in 0..height {
        for x in 0..width {
            let p = y * width + x;
            let check = ((x / 64 + y / 64) % 2) as f32 * 255.0;
            planes[p] = check;
            planes[width * height + p] = (x * 255 / width) as f32;
            planes[2 * width * height + p] = (y * 255 / height) as f32;
        }
    }
    Image {
        width,
        height,
        planes,
    }
}

fn weights(radius: usize) -> Vec<f32> {
    let sigma = (radius as f32 / 2.0).max(0.5);
    let w: Vec<f32> = (0..=2 * radius)
        .map(|i| {
            let d = i as f32 - radius as f32;
            (-d * d / (2.0 * sigma * sigma)).exp()
        })
        .collect();
    let sum: f32 = w.iter().sum();
    w.iter().map(|v| v / sum).collect()
}

fn cpu_pass(src: &[f32], w: &[f32], width: usize, height: usize, horizontal: bool) -> Vec<f32> {
    let r = (w.len() / 2) as isize;
    let plane = width * height;
    let mut dst = vec![0f32; src.len()];
    for ch in 0..3 {
        for y in 0..height {
            for x in 0..width {
                let mut acc = 0f32;
                for k in -r..=r {
                    let (sx, sy) = if horizontal {
                        ((x as isize + k).clamp(0, width as isize - 1) as usize, y)
                    } else {
                        (x, (y as isize + k).clamp(0, height as isize - 1) as usize)
                    };
                    acc += w[(k + r) as usize] * src[ch * plane + sy * width + sx];
                }
                dst[ch * plane + y * width + x] = acc;
            }
        }
    }
    dst
}

fn write_ppm(path: &str, img: &Image, planes: &[f32]) {
    let n = img.width * img.height;
    let mut out = format!("P6\n{} {}\n255\n", img.width, img.height).into_bytes();
    for p in 0..n {
        for ch in 0..3 {
            out.push(planes[ch * n + p].round().clamp(0.0, 255.0) as u8);
        }
    }
    let _ = std::fs::write(path, out);
}

fn main() {
    let radius: usize = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("RADIUS must be a number"))
        .unwrap_or(8);
    assert!(
        (1..=64).contains(&radius),
        "RADIUS must be between 1 and 64"
    );
    let Some(info) = adapter() else {
        eprintln!("this task has no GPU: run it with `gpus = 1` (or `pumasi exec --gpu`)");
        std::process::exit(2);
    };
    println!(
        "adapter: {} ({})",
        json_field(&info, "name").unwrap_or_default(),
        json_field(&info, "backend").unwrap_or_default()
    );
    let img = std::fs::read_dir("/input")
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "ppm"))
        .find_map(|e| std::fs::read(e.path()).ok().and_then(|d| parse_ppm(&d)))
        .unwrap_or_else(test_pattern);
    let (width, height) = (img.width, img.height);
    let w = weights(radius);

    let run = || -> webgpu_examples::Result<(Vec<f32>, f64)> {
        let shader = Shader::new(WGSL, "main")?;
        let src = Buffer::from_f32(&img.planes)?;
        let tmp = Buffer::new(img.planes.len() * 4)?;
        let dst = Buffer::new(img.planes.len() * 4)?;
        let horizontal = Buffer::from_u32(&[width as u32, height as u32, radius as u32, 1])?;
        let vertical = Buffer::from_u32(&[width as u32, height as u32, radius as u32, 0])?;
        let weights = Buffer::from_f32(&w)?;
        let groups = [(width as u32).div_ceil(16), (height as u32).div_ceil(16), 1];
        let mut best = f64::MAX;
        // The first round compiles and warms up; the best of three counts.
        for _ in 0..3 {
            let start = Instant::now();
            dispatch(&shader, &[&src, &tmp, &horizontal, &weights], groups)?;
            dispatch(&shader, &[&tmp, &dst, &vertical, &weights], groups)?;
            wait()?;
            best = best.min(start.elapsed().as_secs_f64() * 1e3);
        }
        Ok((dst.read_f32()?, best))
    };
    let (gpu, gpu_ms) = match run() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("GPU: {e}");
            std::process::exit(3);
        }
    };
    let start = Instant::now();
    let cpu = cpu_pass(
        &cpu_pass(&img.planes, &w, width, height, true),
        &w,
        width,
        height,
        false,
    );
    let cpu_ms = start.elapsed().as_secs_f64() * 1e3;
    let err = max_relative_error(&gpu, &cpu);
    let ok = err <= TOLERANCE;
    println!(
        "blur {width}x{height} radius {radius}: gpu {gpu_ms:.1} ms, cpu {cpu_ms:.1} ms, speedup {:.1}x",
        cpu_ms / gpu_ms.max(1e-3)
    );
    println!(
        "max relative error {err:.2e} (tolerance {TOLERANCE:.0e}): {}",
        if ok { "ok" } else { "MISMATCH" }
    );
    if std::path::Path::new("/output").is_dir() {
        write_ppm("/output/blurred.ppm", &img, &gpu);
    }
    if !ok {
        std::process::exit(1);
    }
}
