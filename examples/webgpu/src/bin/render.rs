//! Draws a picture on the task's GPU (1.16) and checks it against the same
//! picture drawn on the CPU, with both speeds reported:
//!
//!   render [SIZE]     (default 512: a SIZE x SIZE picture)
//!
//! Three steps, each a different part of WebGPU's drawing:
//!
//! 1. a compute shader writes a 64 x 64 checkerboard into a storage
//!    texture (a uniform buffer in group 0, the texture in group 1), which
//!    is read back and must match the CPU's exactly;
//! 2. a render pass draws one triangle over the whole picture that samples
//!    the checkerboard through a linear sampler (two bind groups);
//! 3. a second pass, loaded over the first, draws 24 overlapping
//!    triangles from a vertex buffer with colors blended by their alpha.
//!
//! The CPU follows WebGPU's rules (pixel centers, edge functions, bilinear
//! filtering, unorm rounding after each blend). GPUs filter and snap
//! vertices with a few bits less precision than floats, so a pixel may be
//! off by a step or two, and a pixel right on an edge may land on the
//! other side: the picture passes when no channel is off by more than
//! 4/255 except on at most 0.1% of the pixels. Writes `/output/render.ppm`.
//! Exits 1 on a mismatch.

use std::time::Instant;

use webgpu_examples::{
    BindGroup, Buffer, RenderShader, Resource, Rng, Sampler, Shader, Texture, adapter,
    dispatch_groups, json_field, render,
};

/// Texels of the checkerboard on a side.
const TEX: u32 = 64;
const TRIANGLES: usize = 24;
/// Largest difference a pixel's channel may have, in 255ths.
const STEPS: u8 = 4;
/// Share of the pixels that may differ by more (edges).
const EDGE_SHARE: f64 = 0.001;

const CHECKER_WGSL: &str = r#"
struct Params { cell: u32, a: u32, b: u32, c: u32 }
@group(0) @binding(0) var<uniform> params: Params;
@group(1) @binding(0) var dst: texture_storage_2d<rgba8unorm, write>;

@compute @workgroup_size(8, 8)
fn main(@builtin(global_invocation_id) g: vec3<u32>) {
    if (g.x >= 64u || g.y >= 64u) {
        return;
    }
    let odd = ((g.x / params.cell) + (g.y / params.cell)) % 2u == 1u;
    let c = select(64u, 224u, odd);
    textureStore(dst, g.xy, vec4<f32>(f32(g.x * 4u), f32(g.y * 4u), f32(c), 255.0) / 255.0);
}
"#;

const BACKGROUND_WGSL: &str = r#"
struct View { size: vec4<f32> }
@group(0) @binding(0) var<uniform> view: View;
@group(1) @binding(0) var pattern: texture_2d<f32>;
@group(1) @binding(1) var smp: sampler;

@vertex
fn vs(@builtin(vertex_index) i: u32) -> @builtin(position) vec4<f32> {
    let p = array(vec2(-1.0, -1.0), vec2(3.0, -1.0), vec2(-1.0, 3.0));
    return vec4(p[i], 0.0, 1.0);
}

@fragment
fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
    return textureSample(pattern, smp, pos.xy / view.size.xy);
}
"#;

const TRIANGLES_WGSL: &str = r#"
struct Out {
    @builtin(position) pos: vec4<f32>,
    @location(0) color: vec4<f32>,
}

@vertex
fn vs(@location(0) p: vec2<f32>, @location(1) color: vec4<f32>) -> Out {
    return Out(vec4(p, 0.0, 1.0), color);
}

@fragment
fn fs(v: Out) -> @location(0) vec4<f32> {
    return v.color;
}
"#;

/// A vertex: position in clip space, then RGBA.
#[derive(Clone, Copy)]
struct Vertex {
    p: [f32; 2],
    c: [f32; 4],
}

fn checker() -> Vec<u8> {
    let mut t = Vec::with_capacity((TEX * TEX * 4) as usize);
    for y in 0..TEX {
        for x in 0..TEX {
            let odd = (x / 8 + y / 8) % 2 == 1;
            t.extend_from_slice(&[
                (x * 4) as u8,
                (y * 4) as u8,
                if odd { 224 } else { 64 },
                255,
            ]);
        }
    }
    t
}

fn triangles() -> Vec<Vertex> {
    let mut rng = Rng(0x7072_6f6d_6973_6531);
    let mut v = Vec::with_capacity(TRIANGLES * 3);
    for _ in 0..TRIANGLES {
        let alpha = 0.35 + 0.5 * rng.next_f32();
        // Each triangle's corners are near one another, so they overlap
        // without covering everything.
        let (cx, cy) = (rng.next_f32() * 1.6 - 0.8, rng.next_f32() * 1.6 - 0.8);
        for _ in 0..3 {
            v.push(Vertex {
                p: [
                    (cx + rng.next_f32() * 1.0 - 0.5).clamp(-1.0, 1.0),
                    (cy + rng.next_f32() * 1.0 - 0.5).clamp(-1.0, 1.0),
                ],
                c: [rng.next_f32(), rng.next_f32(), rng.next_f32(), alpha],
            });
        }
    }
    v
}

/// Float to unorm8 as GPUs store it: round to nearest.
fn unorm(v: f32) -> u8 {
    (v.clamp(0.0, 1.0) * 255.0 + 0.5).floor() as u8
}

fn cpu_render(size: u32, tex: &[u8], tris: &[Vertex]) -> Vec<u8> {
    let n = size as usize;
    let mut px = vec![0u8; n * n * 4];
    let texel = |x: i64, y: i64, ch: usize| -> f32 {
        let x = x.clamp(0, TEX as i64 - 1) as usize;
        let y = y.clamp(0, TEX as i64 - 1) as usize;
        tex[(y * TEX as usize + x) * 4 + ch] as f32 / 255.0
    };
    // 2: bilinear sampling with clamp-to-edge at each pixel's center.
    for y in 0..n {
        for x in 0..n {
            let u = (x as f32 + 0.5) / size as f32 * TEX as f32 - 0.5;
            let v = (y as f32 + 0.5) / size as f32 * TEX as f32 - 0.5;
            let (x0, y0) = (u.floor(), v.floor());
            let (fx, fy) = (u - x0, v - y0);
            let (x0, y0) = (x0 as i64, y0 as i64);
            for ch in 0..4 {
                let top = texel(x0, y0, ch) * (1.0 - fx) + texel(x0 + 1, y0, ch) * fx;
                let bottom = texel(x0, y0 + 1, ch) * (1.0 - fx) + texel(x0 + 1, y0 + 1, ch) * fx;
                px[(y * n + x) * 4 + ch] = unorm(top * (1.0 - fy) + bottom * fy);
            }
        }
    }
    // 3: triangles in order, blended over what is there.
    let screen = |v: &Vertex| {
        (
            (v.p[0] + 1.0) * 0.5 * size as f32,
            (1.0 - v.p[1]) * 0.5 * size as f32,
        )
    };
    let edge = |a: (f32, f32), b: (f32, f32), p: (f32, f32)| {
        (b.0 - a.0) * (p.1 - a.1) - (b.1 - a.1) * (p.0 - a.0)
    };
    for t in tris.chunks_exact(3) {
        let (a, b, c) = (screen(&t[0]), screen(&t[1]), screen(&t[2]));
        let area = edge(a, b, c);
        if area == 0.0 {
            continue;
        }
        let x0 = a.0.min(b.0).min(c.0).floor().max(0.0) as usize;
        let x1 = (a.0.max(b.0).max(c.0).ceil() as usize).min(n);
        let y0 = a.1.min(b.1).min(c.1).floor().max(0.0) as usize;
        let y1 = (a.1.max(b.1).max(c.1).ceil() as usize).min(n);
        for y in y0..y1 {
            for x in x0..x1 {
                let p = (x as f32 + 0.5, y as f32 + 0.5);
                let (w0, w1, w2) = (
                    edge(b, c, p) / area,
                    edge(c, a, p) / area,
                    edge(a, b, p) / area,
                );
                if w0 < 0.0 || w1 < 0.0 || w2 < 0.0 {
                    continue;
                }
                let color: Vec<f32> = (0..4)
                    .map(|ch| w0 * t[0].c[ch] + w1 * t[1].c[ch] + w2 * t[2].c[ch])
                    .collect();
                let alpha = color[3];
                let at = (y * n + x) * 4;
                for ch in 0..3 {
                    let dst = px[at + ch] as f32 / 255.0;
                    px[at + ch] = unorm(color[ch] * alpha + dst * (1.0 - alpha));
                }
                let dst = px[at + 3] as f32 / 255.0;
                px[at + 3] = unorm(alpha + dst * (1.0 - alpha));
            }
        }
    }
    px
}

fn gpu_render(size: u32, tris: &[Vertex]) -> webgpu_examples::Result<(Vec<u8>, Vec<u8>, f64)> {
    // 1: the checkerboard, from a compute shader into a storage texture.
    let pattern = Texture::new(&format!(
        r#"{{"size":[{TEX},{TEX}],"format":"rgba8unorm"}}"#
    ))?;
    let fill = Shader::new(CHECKER_WGSL, "main")?;
    let params = Buffer::from_u32(&[8, 0, 0, 0])?;
    let g0 = BindGroup::compute(&fill, 0, &[(0, Resource::Buffer(&params))])?;
    let g1 = BindGroup::compute(&fill, 1, &[(0, Resource::Texture(&pattern))])?;
    dispatch_groups(&fill, &[&g0, &g1], [TEX / 8, TEX / 8, 1])?;
    let texels = pattern.read(0, [0, 0, 0], [TEX, TEX, 1], 4)?;

    let target = Texture::new(&format!(
        r#"{{"size":[{size},{size}],"format":"rgba8unorm"}}"#
    ))?;
    let sampler = Sampler::new(r#"{"magFilter":"linear","minFilter":"linear"}"#)?;
    let view = Buffer::from_f32(&[size as f32, size as f32, 0.0, 0.0])?;
    let background = RenderShader::new(
        BACKGROUND_WGSL,
        r#"{"vertex":{"entryPoint":"vs"},
            "fragment":{"entryPoint":"fs","targets":[{"format":"rgba8unorm"}]}}"#,
    )?;
    let b0 = BindGroup::render(&background, 0, &[(0, Resource::Buffer(&view))])?;
    let b1 = BindGroup::render(
        &background,
        1,
        &[
            (0, Resource::Texture(&pattern)),
            (1, Resource::Sampler(&sampler)),
        ],
    )?;
    let shapes = RenderShader::new(
        TRIANGLES_WGSL,
        r#"{"vertex":{"entryPoint":"vs","buffers":[{"arrayStride":24,"attributes":[
                {"format":"float32x2","offset":0,"shaderLocation":0},
                {"format":"float32x4","offset":8,"shaderLocation":1}]}]},
            "fragment":{"entryPoint":"fs","targets":[{"format":"rgba8unorm","blend":{
                "color":{"srcFactor":"src-alpha","dstFactor":"one-minus-src-alpha"},
                "alpha":{"srcFactor":"one","dstFactor":"one-minus-src-alpha"}}}]}}"#,
    )?;
    let flat: Vec<f32> = tris
        .iter()
        .flat_map(|v| [v.p[0], v.p[1], v.c[0], v.c[1], v.c[2], v.c[3]])
        .collect();
    let vertices = Buffer::from_f32(&flat)?;

    let first = format!(
        r#"{{"pipeline":{},"colorAttachments":[{{"view":{},"clearValue":[0,0,0,1]}}],
            "bindGroups":[{},{}],"draws":[{{"vertexCount":3}}]}}"#,
        background.handle(),
        target.handle(),
        b0.handle(),
        b1.handle()
    );
    let second = format!(
        r#"{{"pipeline":{},"colorAttachments":[{{"view":{},"loadOp":"load"}}],
            "vertexBuffers":[{}],"draws":[{{"vertexCount":{}}}]}}"#,
        shapes.handle(),
        target.handle(),
        vertices.handle(),
        tris.len()
    );
    let mut best = f64::MAX;
    let mut pixels = Vec::new();
    // The first round compiles and warms up; the best of three counts.
    for _ in 0..3 {
        let start = Instant::now();
        render(&first)?;
        render(&second)?;
        pixels = target.read(0, [0, 0, 0], [size, size, 1], 4)?;
        best = best.min(start.elapsed().as_secs_f64() * 1e3);
    }
    Ok((texels, pixels, best))
}

fn write_ppm(path: &str, size: u32, rgba: &[u8]) {
    let mut out = format!("P6\n{size} {size}\n255\n").into_bytes();
    for p in rgba.chunks_exact(4) {
        out.extend_from_slice(&p[..3]);
    }
    let _ = std::fs::write(path, out);
}

fn main() {
    let size: u32 = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("SIZE must be a number"))
        .unwrap_or(512);
    assert!(
        (16..=4096).contains(&size),
        "SIZE must be between 16 and 4096"
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
    let tris = triangles();
    let (texels, gpu, gpu_ms) = match gpu_render(size, &tris) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("GPU: {e}");
            std::process::exit(3);
        }
    };
    let pattern = checker();
    let start = Instant::now();
    let cpu = cpu_render(size, &pattern, &tris);
    let cpu_ms = start.elapsed().as_secs_f64() * 1e3;

    let pattern_ok = texels == pattern;
    let mut over = 0usize;
    let mut largest = 0u8;
    for (g, c) in gpu.chunks_exact(4).zip(cpu.chunks_exact(4)) {
        let d = g
            .iter()
            .zip(c)
            .map(|(a, b)| a.abs_diff(*b))
            .max()
            .unwrap_or(0);
        largest = largest.max(d);
        if d > STEPS {
            over += 1;
        }
    }
    let limit = ((size as f64 * size as f64) * EDGE_SHARE).ceil() as usize;
    let ok = pattern_ok && over <= limit;
    println!(
        "render {size}x{size} ({TRIANGLES} triangles over a sampled texture): gpu {gpu_ms:.1} ms, cpu {cpu_ms:.1} ms, speedup {:.1}x",
        cpu_ms / gpu_ms.max(1e-3)
    );
    println!(
        "storage texture {}; largest difference {largest}/255, {over} pixels over {STEPS}/255 (at most {limit}): {}",
        if pattern_ok { "exact" } else { "DIFFERS" },
        if ok { "ok" } else { "MISMATCH" }
    );
    if std::path::Path::new("/output").is_dir() {
        write_ppm("/output/render.ppm", size, &gpu);
        write_ppm("/output/render-cpu.ppm", size, &cpu);
    }
    if !ok {
        std::process::exit(1);
    }
}
