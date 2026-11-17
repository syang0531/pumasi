//! Drawing over WebGPU (1.16): textures, samplers, render shaders, bind
//! groups beyond group 0, and render passes, on the task's worker
//! ([`super::worker`]).
//!
//! Descriptors travel as JSON with WebGPU's own names: the keys of the
//! JavaScript API's dictionaries (`GPUTextureDescriptor`,
//! `GPUSamplerDescriptor`, `GPURenderPipelineDescriptor` without `module`
//! and `layout`, `GPURenderPassDescriptor` plus what is set on the pass)
//! and its enum strings (`"rgba8unorm"`, `"triangle-list"`,
//! `"clamp-to-edge"`), so a module ports to and from browser WebGPU and
//! `wasi:webgpu` without renaming anything. Where WebGPU takes an object,
//! these take its handle; a texture binds and renders through its default
//! view. Layouts are automatic, so a bind group is made for one shader's
//! group and fits only that shader, as `getBindGroupLayout` gives.

use serde::Deserialize;

use super::worker::{Object, Resp, Session, map_read, scoped};

type R = std::result::Result<Resp, String>;

fn parse<'a, T: Deserialize<'a>>(json: &'a str, what: &str) -> std::result::Result<T, String> {
    serde_json::from_str(json).map_err(|e| format!("the {what} descriptor: {e}"))
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
enum Dimension {
    #[serde(rename = "1d")]
    D1,
    #[default]
    #[serde(rename = "2d")]
    D2,
    #[serde(rename = "3d")]
    D3,
}

/// `GPUTextureDescriptor`. `size` is `[width, height, depthOrArrayLayers]`
/// (the last two default to 1); `usage` is WebGPU's flags (`COPY_SRC` 1,
/// `COPY_DST` 2, `TEXTURE_BINDING` 4, `STORAGE_BINDING` 8,
/// `RENDER_ATTACHMENT` 16) and defaults to every one the format allows.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TextureDesc {
    size: Vec<u32>,
    format: wgpu::TextureFormat,
    #[serde(default)]
    usage: Option<u32>,
    #[serde(default = "one")]
    mip_level_count: u32,
    #[serde(default = "one")]
    sample_count: u32,
    #[serde(default)]
    dimension: Dimension,
}

/// `GPUSamplerDescriptor`, with WebGPU's defaults.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
struct SamplerDesc {
    address_mode_u: wgpu::AddressMode,
    address_mode_v: wgpu::AddressMode,
    address_mode_w: wgpu::AddressMode,
    mag_filter: wgpu::FilterMode,
    min_filter: wgpu::FilterMode,
    mipmap_filter: wgpu::MipmapFilterMode,
    lod_min_clamp: Option<f32>,
    lod_max_clamp: Option<f32>,
    compare: Option<wgpu::CompareFunction>,
    max_anisotropy: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VertexBuffer {
    array_stride: u64,
    #[serde(default)]
    step_mode: wgpu::VertexStepMode,
    attributes: Vec<wgpu::VertexAttribute>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VertexStage {
    entry_point: String,
    #[serde(default)]
    buffers: Vec<VertexBuffer>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlendComponent {
    #[serde(default = "blend_one")]
    src_factor: wgpu::BlendFactor,
    #[serde(default = "blend_zero")]
    dst_factor: wgpu::BlendFactor,
    #[serde(default)]
    operation: wgpu::BlendOperation,
}

fn blend_one() -> wgpu::BlendFactor {
    wgpu::BlendFactor::One
}

fn blend_zero() -> wgpu::BlendFactor {
    wgpu::BlendFactor::Zero
}

impl From<&BlendComponent> for wgpu::BlendComponent {
    fn from(c: &BlendComponent) -> Self {
        wgpu::BlendComponent {
            src_factor: c.src_factor,
            dst_factor: c.dst_factor,
            operation: c.operation,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Blend {
    color: BlendComponent,
    alpha: BlendComponent,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Target {
    format: wgpu::TextureFormat,
    #[serde(default)]
    blend: Option<Blend>,
    #[serde(default)]
    write_mask: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FragmentStage {
    entry_point: String,
    targets: Vec<Target>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Cull {
    #[default]
    None,
    Front,
    Back,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
struct Primitive {
    topology: wgpu::PrimitiveTopology,
    strip_index_format: Option<wgpu::IndexFormat>,
    front_face: wgpu::FrontFace,
    cull_mode: Cull,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DepthStencil {
    format: wgpu::TextureFormat,
    #[serde(default)]
    depth_write_enabled: bool,
    #[serde(default)]
    depth_compare: Option<wgpu::CompareFunction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Multisample {
    #[serde(default = "one")]
    count: u32,
    #[serde(default)]
    alpha_to_coverage_enabled: bool,
}

/// `GPURenderPipelineDescriptor` without `module` (the WGSL source comes
/// with it) and `layout` (always automatic).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenderShaderDesc {
    vertex: VertexStage,
    #[serde(default)]
    fragment: Option<FragmentStage>,
    #[serde(default)]
    primitive: Primitive,
    #[serde(default)]
    depth_stencil: Option<DepthStencil>,
    #[serde(default)]
    multisample: Option<Multisample>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Load {
    #[default]
    Clear,
    Load,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Store {
    #[default]
    Store,
    Discard,
}

impl From<Store> for wgpu::StoreOp {
    fn from(s: Store) -> Self {
        match s {
            Store::Store => wgpu::StoreOp::Store,
            Store::Discard => wgpu::StoreOp::Discard,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColorAttachment {
    view: i32,
    #[serde(default)]
    resolve_target: Option<i32>,
    #[serde(default)]
    clear_value: Option<[f64; 4]>,
    #[serde(default)]
    load_op: Load,
    #[serde(default)]
    store_op: Store,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DepthAttachment {
    view: i32,
    #[serde(default = "depth_one")]
    depth_clear_value: f32,
    #[serde(default)]
    depth_load_op: Load,
    #[serde(default)]
    depth_store_op: Store,
}

fn depth_one() -> f32 {
    1.0
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexBuffer {
    buffer: i32,
    format: wgpu::IndexFormat,
    #[serde(default)]
    offset: u64,
}

/// One `draw` (with `vertexCount`) or `drawIndexed` (with `indexCount`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Draw {
    #[serde(default)]
    vertex_count: Option<u32>,
    #[serde(default)]
    index_count: Option<u32>,
    #[serde(default = "one")]
    instance_count: u32,
    #[serde(default)]
    first_vertex: u32,
    #[serde(default)]
    first_index: u32,
    #[serde(default)]
    base_vertex: i32,
    #[serde(default)]
    first_instance: u32,
}

/// `GPURenderPassDescriptor` plus what is set on the pass: one render
/// shader, its bind groups (index = group), vertex buffers (index = slot),
/// an index buffer, viewport and scissor, and the draws.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenderPass {
    pipeline: i32,
    #[serde(default)]
    color_attachments: Vec<ColorAttachment>,
    #[serde(default)]
    depth_stencil_attachment: Option<DepthAttachment>,
    #[serde(default)]
    bind_groups: Vec<i32>,
    #[serde(default)]
    vertex_buffers: Vec<i32>,
    #[serde(default)]
    index_buffer: Option<IndexBuffer>,
    /// `[x, y, width, height, minDepth, maxDepth]`.
    #[serde(default)]
    viewport: Option<[f32; 6]>,
    /// `[x, y, width, height]`.
    #[serde(default)]
    scissor_rect: Option<[u32; 4]>,
    draws: Vec<Draw>,
}

/// The texture of `handle`, its default view and its size in bytes.
fn texture_of(
    s: &Session,
    handle: i32,
) -> std::result::Result<(&wgpu::Texture, &wgpu::TextureView), String> {
    match s.object(handle)? {
        Object::Texture(t, v, _) => Ok((t, v)),
        o => Err(format!("{handle} is {}, not a texture", o.kind())),
    }
}

/// Bytes a texture takes, every mip level and sample counted.
fn texture_bytes(
    format: wgpu::TextureFormat,
    size: wgpu::Extent3d,
    dim: wgpu::TextureDimension,
    mips: u32,
    samples: u32,
) -> u64 {
    let texel = format
        .block_copy_size(None)
        .or_else(|| format.block_copy_size(Some(wgpu::TextureAspect::DepthOnly)))
        .unwrap_or(4)
        .max(4) as u64;
    let (bw, bh) = format.block_dimensions();
    (0..mips)
        .map(|l| {
            let m = size.mip_level_size(l, dim);
            let blocks = (m.width.div_ceil(bw) as u64) * (m.height.div_ceil(bh) as u64);
            blocks * m.depth_or_array_layers as u64
        })
        .sum::<u64>()
        * texel
        * samples.max(1) as u64
}

pub(super) fn texture(s: &mut Session, json: &str) -> R {
    let d: TextureDesc = parse(json, "texture")?;
    if d.size.is_empty() || d.size.len() > 3 || d.size.contains(&0) {
        return Err("a texture's size is [width, height, depth], none of them 0".into());
    }
    let size = wgpu::Extent3d {
        width: d.size[0],
        height: d.size.get(1).copied().unwrap_or(1),
        depth_or_array_layers: d.size.get(2).copied().unwrap_or(1),
    };
    let dimension = match d.dimension {
        Dimension::D1 => wgpu::TextureDimension::D1,
        Dimension::D2 => wgpu::TextureDimension::D2,
        Dimension::D3 => wgpu::TextureDimension::D3,
    };
    let allowed = s
        .adapter
        .get_texture_format_features(d.format)
        .allowed_usages;
    let usage = match d.usage {
        Some(u) => wgpu::TextureUsages::from_bits(u)
            .ok_or_else(|| format!("unknown texture usage flags {u:#x}"))?,
        None => {
            let mut u = allowed
                & (wgpu::TextureUsages::COPY_SRC
                    | wgpu::TextureUsages::COPY_DST
                    | wgpu::TextureUsages::TEXTURE_BINDING
                    | wgpu::TextureUsages::STORAGE_BINDING
                    | wgpu::TextureUsages::RENDER_ATTACHMENT);
            // Storage needs one sample and is rarely what a multisampled
            // or mipmapped texture is for.
            if d.sample_count > 1 || d.mip_level_count > 1 {
                u -= wgpu::TextureUsages::STORAGE_BINDING;
            }
            if d.sample_count > 1 {
                u &= wgpu::TextureUsages::RENDER_ATTACHMENT | wgpu::TextureUsages::TEXTURE_BINDING;
            }
            u
        }
    };
    let bytes = texture_bytes(d.format, size, dimension, d.mip_level_count, d.sample_count);
    if s.bytes.saturating_add(bytes) > s.limit_bytes {
        return Err(format!(
            "GPU buffers and textures would take {} MiB, over the job's memory limit of {} MiB",
            (s.bytes + bytes) >> 20,
            s.limit_bytes >> 20
        ));
    }
    let (tex, view) = scoped(&s.device, || {
        let t = s.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("task"),
            size,
            mip_level_count: d.mip_level_count,
            sample_count: d.sample_count,
            dimension,
            format: d.format,
            usage,
            view_formats: &[],
        });
        let v = t.create_view(&Default::default());
        (t, v)
    })?;
    s.bytes += bytes;
    Ok(Resp::Value(s.add(Object::Texture(tex, view, bytes))?))
}

pub(super) fn sampler(s: &mut Session, json: &str) -> R {
    let d: SamplerDesc = parse(json, "sampler")?;
    let sampler = scoped(&s.device, || {
        s.device.create_sampler(&wgpu::SamplerDescriptor {
            label: Some("task"),
            address_mode_u: d.address_mode_u,
            address_mode_v: d.address_mode_v,
            address_mode_w: d.address_mode_w,
            mag_filter: d.mag_filter,
            min_filter: d.min_filter,
            mipmap_filter: d.mipmap_filter,
            lod_min_clamp: d.lod_min_clamp.unwrap_or(0.0),
            lod_max_clamp: d.lod_max_clamp.unwrap_or(32.0),
            compare: d.compare,
            anisotropy_clamp: d.max_anisotropy.unwrap_or(1),
            border_color: None,
        })
    })?;
    Ok(Resp::Value(s.add(Object::Sampler(sampler))?))
}

pub(super) fn render_shader(s: &mut Session, src: &str, json: &str) -> R {
    let d: RenderShaderDesc = parse(json, "render shader")?;
    let attributes: Vec<&[wgpu::VertexAttribute]> = d
        .vertex
        .buffers
        .iter()
        .map(|b| b.attributes.as_slice())
        .collect();
    let buffers: Vec<Option<wgpu::VertexBufferLayout>> = d
        .vertex
        .buffers
        .iter()
        .zip(&attributes)
        .map(|(b, a)| {
            Some(wgpu::VertexBufferLayout {
                array_stride: b.array_stride,
                step_mode: b.step_mode,
                attributes: a,
            })
        })
        .collect();
    let targets: Vec<Option<wgpu::ColorTargetState>> = d
        .fragment
        .as_ref()
        .map(|f| {
            f.targets
                .iter()
                .map(|t| {
                    Some(wgpu::ColorTargetState {
                        format: t.format,
                        blend: t.blend.as_ref().map(|b| wgpu::BlendState {
                            color: (&b.color).into(),
                            alpha: (&b.alpha).into(),
                        }),
                        write_mask: t
                            .write_mask
                            .map(wgpu::ColorWrites::from_bits_truncate)
                            .unwrap_or(wgpu::ColorWrites::ALL),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let primitive = wgpu::PrimitiveState {
        topology: d.primitive.topology,
        strip_index_format: d.primitive.strip_index_format,
        front_face: d.primitive.front_face,
        cull_mode: match d.primitive.cull_mode {
            Cull::None => None,
            Cull::Front => Some(wgpu::Face::Front),
            Cull::Back => Some(wgpu::Face::Back),
        },
        ..Default::default()
    };
    let depth_stencil = d.depth_stencil.as_ref().map(|ds| wgpu::DepthStencilState {
        format: ds.format,
        depth_write_enabled: Some(ds.depth_write_enabled),
        depth_compare: Some(ds.depth_compare.unwrap_or(wgpu::CompareFunction::Always)),
        stencil: Default::default(),
        bias: Default::default(),
    });
    let multisample = d
        .multisample
        .as_ref()
        .map(|m| wgpu::MultisampleState {
            count: m.count,
            mask: !0,
            alpha_to_coverage_enabled: m.alpha_to_coverage_enabled,
        })
        .unwrap_or_default();
    let pipeline = scoped(&s.device, || {
        let module = s.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("task"),
            source: wgpu::ShaderSource::Wgsl(src.into()),
        });
        s.device
            .create_render_pipeline(&wgpu::RenderPipelineDescriptor {
                label: Some("task"),
                layout: None,
                vertex: wgpu::VertexState {
                    module: &module,
                    entry_point: Some(&d.vertex.entry_point),
                    compilation_options: Default::default(),
                    buffers: &buffers,
                },
                primitive,
                depth_stencil,
                multisample,
                fragment: d.fragment.as_ref().map(|f| wgpu::FragmentState {
                    module: &module,
                    entry_point: Some(&f.entry_point),
                    compilation_options: Default::default(),
                    targets: &targets,
                }),
                multiview_mask: None,
                cache: None,
            })
    })?;
    Ok(Resp::Value(s.add(Object::Render(pipeline))?))
}

/// What a texture copy covers, and its tightly packed byte layout.
struct Copy {
    mip: u32,
    origin: wgpu::Origin3d,
    extent: wgpu::Extent3d,
    aspect: wgpu::TextureAspect,
    /// Bytes per row, tightly packed.
    row: u32,
    rows: u32,
}

impl Copy {
    fn len(&self) -> u64 {
        self.row as u64 * self.rows as u64 * self.extent.depth_or_array_layers as u64
    }
}

fn copy_of(tex: &wgpu::Texture, region: [u32; 7]) -> std::result::Result<Copy, String> {
    let [mip, x, y, z, w, h, d] = region;
    if w == 0 || h == 0 || d == 0 {
        return Err("a texture copy needs a width, height and depth".into());
    }
    let format = tex.format();
    if format.block_dimensions() != (1, 1) {
        return Err(format!(
            "{format:?} textures cannot be copied by the host API"
        ));
    }
    let (aspect, texel) = match format.block_copy_size(None) {
        Some(n) => (wgpu::TextureAspect::All, n),
        None => match format.block_copy_size(Some(wgpu::TextureAspect::DepthOnly)) {
            Some(n) => (wgpu::TextureAspect::DepthOnly, n),
            None => return Err(format!("{format:?} textures cannot be copied")),
        },
    };
    let row = w.checked_mul(texel).ok_or("the copy's rows are too long")?;
    Ok(Copy {
        mip,
        origin: wgpu::Origin3d { x, y, z },
        extent: wgpu::Extent3d {
            width: w,
            height: h,
            depth_or_array_layers: d,
        },
        aspect,
        row,
        rows: h,
    })
}

pub(super) fn write_texture(s: &mut Session, tex: i32, region: [u32; 7], data: &[u8]) -> R {
    let (t, _) = texture_of(s, tex)?;
    let c = copy_of(t, region)?;
    if data.len() as u64 != c.len() {
        return Err(format!(
            "the region takes {} bytes (tightly packed rows of {} bytes), not {}",
            c.len(),
            c.row,
            data.len()
        ));
    }
    scoped(&s.device, || {
        s.queue.write_texture(
            wgpu::TexelCopyTextureInfo {
                texture: t,
                mip_level: c.mip,
                origin: c.origin,
                aspect: c.aspect,
            },
            data,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(c.row),
                rows_per_image: Some(c.rows),
            },
            c.extent,
        )
    })?;
    Ok(Resp::Value(0))
}

pub(super) fn read_texture(s: &mut Session, tex: i32, region: [u32; 7]) -> R {
    let (t, _) = texture_of(s, tex)?;
    let c = copy_of(t, region)?;
    // Copies to a buffer take rows of a multiple of 256 bytes; the module
    // gets them tightly packed.
    let padded = c.row.next_multiple_of(wgpu::COPY_BYTES_PER_ROW_ALIGNMENT);
    let size = padded as u64 * c.rows as u64 * c.extent.depth_or_array_layers as u64;
    let staging = scoped(&s.device, || {
        let staging = s.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("read"),
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let mut enc = s.device.create_command_encoder(&Default::default());
        enc.copy_texture_to_buffer(
            wgpu::TexelCopyTextureInfo {
                texture: t,
                mip_level: c.mip,
                origin: c.origin,
                aspect: c.aspect,
            },
            wgpu::TexelCopyBufferInfo {
                buffer: &staging,
                layout: wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded),
                    rows_per_image: Some(c.rows),
                },
            },
            c.extent,
        );
        s.queue.submit([enc.finish()]);
        staging
    })?;
    let raw = map_read(s, &staging)?;
    if padded == c.row {
        return Ok(Resp::Data(raw));
    }
    let mut out = Vec::with_capacity(c.len() as usize);
    for line in raw.chunks_exact(padded as usize) {
        out.extend_from_slice(&line[..c.row as usize]);
    }
    Ok(Resp::Data(out))
}

pub(super) fn bind_group(s: &mut Session, shader: i32, group: u32, entries: &[(u32, i32)]) -> R {
    let layout = match s.object(shader)? {
        Object::Shader(p) => scoped(&s.device, || p.get_bind_group_layout(group))?,
        Object::Render(p) => scoped(&s.device, || p.get_bind_group_layout(group))?,
        o => return Err(format!("{shader} is {}, not a shader", o.kind())),
    };
    let mut list = Vec::with_capacity(entries.len());
    for (binding, h) in entries {
        let resource = match s.object(*h)? {
            Object::Buffer(b, _) => b.as_entire_binding(),
            Object::Texture(_, v, _) => wgpu::BindingResource::TextureView(v),
            Object::Sampler(x) => wgpu::BindingResource::Sampler(x),
            o => return Err(format!("{h} is {}, which does not bind", o.kind())),
        };
        list.push(wgpu::BindGroupEntry {
            binding: *binding,
            resource,
        });
    }
    let g = scoped(&s.device, || {
        s.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("task"),
            layout: &layout,
            entries: &list,
        })
    })?;
    Ok(Resp::Value(s.add(Object::Group(g))?))
}

/// Bind groups by index (`0`: nothing at that index).
fn groups_of<'a>(
    s: &'a Session,
    handles: &[i32],
) -> std::result::Result<Vec<Option<&'a wgpu::BindGroup>>, String> {
    handles
        .iter()
        .map(|h| match *h {
            0 => Ok(None),
            h => match s.object(h)? {
                Object::Group(g) => Ok(Some(g)),
                o => Err(format!("{h} is {}, not a bind group", o.kind())),
            },
        })
        .collect()
}

pub(super) fn dispatch_groups(s: &mut Session, shader: i32, groups: &[i32], n: [u32; 3]) -> R {
    let pipeline = match s.object(shader)? {
        Object::Shader(p) => p,
        o => return Err(format!("{shader} is {}, not a compute shader", o.kind())),
    };
    let bound = groups_of(s, groups)?;
    let index = scoped(&s.device, || {
        let mut enc = s.device.create_command_encoder(&Default::default());
        {
            let mut pass = enc.begin_compute_pass(&Default::default());
            pass.set_pipeline(pipeline);
            for (i, g) in bound.iter().enumerate() {
                if let Some(g) = g {
                    pass.set_bind_group(i as u32, *g, &[]);
                }
            }
            pass.dispatch_workgroups(n[0], n[1], n[2]);
        }
        s.queue.submit([enc.finish()])
    })?;
    s.submitted(index)?;
    Ok(Resp::Value(0))
}

pub(super) fn render(s: &mut Session, json: &str) -> R {
    let p: RenderPass = parse(json, "render pass")?;
    if p.draws.is_empty() {
        return Err("a render pass needs at least one draw".into());
    }
    let pipeline = match s.object(p.pipeline)? {
        Object::Render(r) => r,
        o => {
            return Err(format!(
                "{} is {}, not a render shader",
                p.pipeline,
                o.kind()
            ));
        }
    };
    let mut colors = Vec::with_capacity(p.color_attachments.len());
    for c in &p.color_attachments {
        let (_, view) = texture_of(s, c.view)?;
        let resolve = match c.resolve_target {
            Some(h) => Some(texture_of(s, h)?.1),
            None => None,
        };
        let v = c.clear_value.unwrap_or([0.0; 4]);
        colors.push(Some(wgpu::RenderPassColorAttachment {
            view,
            depth_slice: None,
            resolve_target: resolve,
            ops: wgpu::Operations {
                load: match c.load_op {
                    Load::Clear => wgpu::LoadOp::Clear(wgpu::Color {
                        r: v[0],
                        g: v[1],
                        b: v[2],
                        a: v[3],
                    }),
                    Load::Load => wgpu::LoadOp::Load,
                },
                store: c.store_op.into(),
            },
        }));
    }
    let depth = match &p.depth_stencil_attachment {
        Some(d) => Some(wgpu::RenderPassDepthStencilAttachment {
            view: texture_of(s, d.view)?.1,
            depth_ops: Some(wgpu::Operations {
                load: match d.depth_load_op {
                    Load::Clear => wgpu::LoadOp::Clear(d.depth_clear_value),
                    Load::Load => wgpu::LoadOp::Load,
                },
                store: d.depth_store_op.into(),
            }),
            stencil_ops: None,
        }),
        None => None,
    };
    let bound = groups_of(s, &p.bind_groups)?;
    let mut vertex = Vec::with_capacity(p.vertex_buffers.len());
    for h in &p.vertex_buffers {
        vertex.push(s.buffer(*h)?.0);
    }
    let index_buffer = match &p.index_buffer {
        Some(i) => Some((s.buffer(i.buffer)?.0, i.format, i.offset)),
        None => None,
    };
    for d in &p.draws {
        match (d.vertex_count, d.index_count) {
            (Some(_), None) => {}
            (None, Some(_)) if index_buffer.is_some() => {}
            (None, Some(_)) => return Err("an indexed draw needs an indexBuffer".into()),
            _ => return Err("a draw has either vertexCount or indexCount".into()),
        }
    }
    let index = scoped(&s.device, || {
        let mut enc = s.device.create_command_encoder(&Default::default());
        {
            let mut pass = enc.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("task"),
                color_attachments: &colors,
                depth_stencil_attachment: depth,
                timestamp_writes: None,
                occlusion_query_set: None,
                multiview_mask: None,
            });
            pass.set_pipeline(pipeline);
            for (i, g) in bound.iter().enumerate() {
                if let Some(g) = g {
                    pass.set_bind_group(i as u32, *g, &[]);
                }
            }
            for (slot, b) in vertex.iter().enumerate() {
                pass.set_vertex_buffer(slot as u32, b.slice(..));
            }
            if let Some((b, format, offset)) = index_buffer {
                pass.set_index_buffer(b.slice(offset..), format);
            }
            if let Some([x, y, w, h, min, max]) = p.viewport {
                pass.set_viewport(x, y, w, h, min, max);
            }
            if let Some([x, y, w, h]) = p.scissor_rect {
                pass.set_scissor_rect(x, y, w, h);
            }
            for d in &p.draws {
                let instances = d.first_instance..d.first_instance + d.instance_count;
                match (d.vertex_count, d.index_count) {
                    (Some(n), _) => pass.draw(d.first_vertex..d.first_vertex + n, instances),
                    (_, Some(n)) => pass.draw_indexed(
                        d.first_index..d.first_index + n,
                        d.base_vertex,
                        instances,
                    ),
                    _ => {}
                }
            }
        }
        s.queue.submit([enc.finish()])
    })?;
    s.submitted(index)?;
    Ok(Resp::Value(p.draws.len() as i32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors_use_webgpu_names() {
        let t: TextureDesc =
            serde_json::from_str(r#"{"size":[64,32],"format":"rgba8unorm-srgb","usage":20}"#)
                .unwrap();
        assert_eq!(t.format, wgpu::TextureFormat::Rgba8UnormSrgb);
        assert_eq!(t.size, [64, 32]);
        let s: SamplerDesc = serde_json::from_str(
            r#"{"magFilter":"linear","addressModeU":"repeat","compare":"less-equal"}"#,
        )
        .unwrap();
        assert_eq!(s.mag_filter, wgpu::FilterMode::Linear);
        assert_eq!(s.address_mode_u, wgpu::AddressMode::Repeat);
        assert_eq!(s.address_mode_v, wgpu::AddressMode::ClampToEdge);
        let r: RenderShaderDesc = serde_json::from_str(
            r#"{"vertex":{"entryPoint":"vs","buffers":[{"arrayStride":24,"attributes":[
                  {"format":"float32x2","offset":0,"shaderLocation":0},
                  {"format":"float32x4","offset":8,"shaderLocation":1}]}]},
                "fragment":{"entryPoint":"fs","targets":[{"format":"rgba8unorm","blend":{
                  "color":{"srcFactor":"src-alpha","dstFactor":"one-minus-src-alpha"},
                  "alpha":{"srcFactor":"one","dstFactor":"one-minus-src-alpha"}}}]},
                "primitive":{"topology":"triangle-strip","cullMode":"back"}}"#,
        )
        .unwrap();
        assert_eq!(r.vertex.buffers[0].attributes[1].shader_location, 1);
        assert_eq!(r.primitive.topology, wgpu::PrimitiveTopology::TriangleStrip);
        let p: RenderPass = serde_json::from_str(
            r#"{"pipeline":3,"colorAttachments":[{"view":4,"clearValue":[0,0,0,1]}],
                "bindGroups":[5,0,6],"draws":[{"vertexCount":3},{"indexCount":6,"instanceCount":2}]}"#,
        )
        .unwrap();
        assert_eq!(p.bind_groups, [5, 0, 6]);
        assert_eq!(p.draws[1].instance_count, 2);
        // Unknown keys are mistakes, not silently ignored.
        assert!(
            serde_json::from_str::<TextureDesc>(r#"{"size":[1],"format":"r8unorm","sizee":1}"#)
                .is_err()
        );
        assert_eq!(
            texture_bytes(
                wgpu::TextureFormat::Rgba8Unorm,
                wgpu::Extent3d {
                    width: 4,
                    height: 4,
                    depth_or_array_layers: 1
                },
                wgpu::TextureDimension::D2,
                3,
                1
            ),
            (16 + 4 + 1) * 4
        );
    }

    use crate::webgpu::worker::{AdapterKey, Client, Req, Worker};
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    /// The first adapter, software ones included, served on a thread.
    fn client() -> Option<Client> {
        let mut notes = Vec::new();
        let gpu = crate::webgpu::WebGpu::detect(
            crate::webgpu::Options {
                software: true,
                ..Default::default()
            },
            &[],
            &mut notes,
        );
        let Some((_, adapter)) = gpu.adapters.first() else {
            assert!(
                std::env::var("PUMASI_TEST_WEBGPU").as_deref() != Ok("require"),
                "no WebGPU adapter: {notes:?}"
            );
            eprintln!("skipped: no WebGPU adapter here");
            return None;
        };
        let mut c = Client::start(&Worker::Thread, adapter.clone()).unwrap();
        let open = Req::Open {
            key: AdapterKey::of(adapter).unwrap(),
            limit_bytes: 64 << 20,
            info: "{}".into(),
            tag: 0,
        };
        assert!(matches!(call(&mut c, open), Resp::Info(_)));
        Some(c)
    }

    fn call(c: &mut Client, req: Req) -> Resp {
        c.call(
            &req,
            Instant::now() + Duration::from_secs(60),
            &AtomicBool::new(false),
        )
        .unwrap()
    }

    fn value(c: &mut Client, req: Req) -> i32 {
        match call(c, req) {
            Resp::Value(v) => v,
            other => panic!("{other:?}"),
        }
    }

    const QUAD_WGSL: &str = r#"
struct Tint { k: vec4<f32> }
@group(0) @binding(0) var<uniform> tint: Tint;
@group(1) @binding(0) var src: texture_2d<f32>;
@group(1) @binding(1) var smp: sampler;

@vertex
fn vs(@builtin(vertex_index) i: u32) -> @builtin(position) vec4<f32> {
    let p = array(vec2(-1.0, -1.0), vec2(3.0, -1.0), vec2(-1.0, 3.0));
    return vec4(p[i], 0.0, 1.0);
}

@fragment
fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {
    return textureSample(src, smp, pos.xy / 16.0) * tint.k;
}
"#;

    const SOLID_WGSL: &str = r#"
@vertex
fn vs(@location(0) p: vec2<f32>) -> @builtin(position) vec4<f32> {
    return vec4(p, 0.0, 1.0);
}

@fragment
fn fs() -> @location(0) vec4<f32> {
    return vec4(1.0, 1.0, 1.0, 1.0);
}
"#;

    const FILL_WGSL: &str = r#"
struct Color { c: vec4<f32> }
@group(0) @binding(0) var<uniform> color: Color;
@group(1) @binding(0) var dst: texture_storage_2d<rgba8unorm, write>;

@compute @workgroup_size(8, 8)
fn main(@builtin(global_invocation_id) g: vec3<u32>) {
    textureStore(dst, g.xy, color.c);
}
"#;

    fn f32s(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    /// Draws through the worker: a texture sampled with two bind groups,
    /// then an indexed quad from vertex buffers over it, and a compute
    /// shader that writes a storage texture. Pixels are checked exactly
    /// (every value is 0, 0.5 or 1 of 255 steps, away from edges).
    #[test]
    fn textures_samplers_and_render_passes() {
        let Some(mut c) = client() else { return };
        let target = value(
            &mut c,
            Req::Texture {
                json: r#"{"size":[16,16],"format":"rgba8unorm"}"#.into(),
            },
        );
        let src = value(
            &mut c,
            Req::Texture {
                json: r#"{"size":[2,2],"format":"rgba8unorm","usage":6}"#.into(),
            },
        );
        // Red, green / blue, gray.
        let texels: Vec<u8> = vec![
            255, 0, 0, 255, 0, 255, 0, 255, 0, 0, 255, 255, 128, 128, 128, 255,
        ];
        value(
            &mut c,
            Req::WriteTexture {
                tex: src,
                region: [0, 0, 0, 0, 2, 2, 1],
                data: texels,
            },
        );
        let smp = value(
            &mut c,
            Req::Sampler {
                json: r#"{"magFilter":"nearest"}"#.into(),
            },
        );
        let tint = value(&mut c, Req::Buffer { size: 16 });
        value(
            &mut c,
            Req::Write {
                buf: tint,
                offset: 0,
                data: f32s(&[1.0, 1.0, 1.0, 1.0]),
            },
        );
        let quad = value(
            &mut c,
            Req::RenderShader {
                src: QUAD_WGSL.into(),
                json: r#"{"vertex":{"entryPoint":"vs"},
                          "fragment":{"entryPoint":"fs","targets":[{"format":"rgba8unorm"}]}}"#
                    .into(),
            },
        );
        let g0 = value(
            &mut c,
            Req::BindGroup {
                shader: quad,
                group: 0,
                entries: vec![(0, tint)],
            },
        );
        let g1 = value(
            &mut c,
            Req::BindGroup {
                shader: quad,
                group: 1,
                entries: vec![(0, src), (1, smp)],
            },
        );
        let pass = format!(
            r#"{{"pipeline":{quad},"colorAttachments":[{{"view":{target},"clearValue":[0,0,0,1]}}],
                "bindGroups":[{g0},{g1}],"draws":[{{"vertexCount":3}}]}}"#
        );
        assert!(matches!(
            call(&mut c, Req::Render { json: pass }),
            Resp::Value(1)
        ));

        // A white quad over the top-left quarter, indexed, loaded over it.
        let solid = value(
            &mut c,
            Req::RenderShader {
                src: SOLID_WGSL.into(),
                json: r#"{"vertex":{"entryPoint":"vs","buffers":[{"arrayStride":8,
                            "attributes":[{"format":"float32x2","offset":0,"shaderLocation":0}]}]},
                          "fragment":{"entryPoint":"fs","targets":[{"format":"rgba8unorm"}]}}"#
                    .into(),
            },
        );
        let verts = value(&mut c, Req::Buffer { size: 32 });
        value(
            &mut c,
            Req::Write {
                buf: verts,
                offset: 0,
                data: f32s(&[-1.0, 1.0, 0.0, 1.0, 0.0, 0.0, -1.0, 0.0]),
            },
        );
        let index = value(&mut c, Req::Buffer { size: 12 });
        let idx: Vec<u8> = [0u16, 1, 2, 0, 2, 3]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        value(
            &mut c,
            Req::Write {
                buf: index,
                offset: 0,
                data: idx,
            },
        );
        let pass = format!(
            r#"{{"pipeline":{solid},"colorAttachments":[{{"view":{target},"loadOp":"load"}}],
                "vertexBuffers":[{verts}],"indexBuffer":{{"buffer":{index},"format":"uint16"}},
                "draws":[{{"indexCount":6}}]}}"#
        );
        assert!(matches!(
            call(&mut c, Req::Render { json: pass }),
            Resp::Value(1)
        ));

        let Resp::Data(px) = call(
            &mut c,
            Req::ReadTexture {
                tex: target,
                region: [0, 0, 0, 0, 16, 16, 1],
            },
        ) else {
            panic!("no pixels")
        };
        assert_eq!(px.len(), 16 * 16 * 4);
        let at = |x: usize, y: usize| &px[(y * 16 + x) * 4..(y * 16 + x) * 4 + 4];
        assert_eq!(at(3, 3), [255, 255, 255, 255], "the quad");
        assert_eq!(at(12, 3), [0, 255, 0, 255], "green");
        assert_eq!(at(3, 12), [0, 0, 255, 255], "blue");
        assert_eq!(at(12, 12), [128, 128, 128, 255], "gray");

        // A compute shader writes a storage texture through group 1.
        let fill = value(
            &mut c,
            Req::Shader {
                src: FILL_WGSL.into(),
                entry: "main".into(),
            },
        );
        let color = value(&mut c, Req::Buffer { size: 16 });
        value(
            &mut c,
            Req::Write {
                buf: color,
                offset: 0,
                data: f32s(&[0.0, 1.0, 1.0, 1.0]),
            },
        );
        let out = value(
            &mut c,
            Req::Texture {
                json: r#"{"size":[64,8],"format":"rgba8unorm"}"#.into(),
            },
        );
        let a = value(
            &mut c,
            Req::BindGroup {
                shader: fill,
                group: 0,
                entries: vec![(0, color)],
            },
        );
        let b = value(
            &mut c,
            Req::BindGroup {
                shader: fill,
                group: 1,
                entries: vec![(0, out)],
            },
        );
        value(
            &mut c,
            Req::DispatchGroups {
                shader: fill,
                groups: vec![a, b],
                x: 8,
                y: 1,
                z: 1,
            },
        );
        // A 64-texel row is 256 bytes; a 3-texel region is not, and comes
        // back tightly packed anyway.
        let Resp::Data(px) = call(
            &mut c,
            Req::ReadTexture {
                tex: out,
                region: [0, 61, 2, 0, 3, 2, 1],
            },
        ) else {
            panic!("no pixels")
        };
        assert_eq!(px, [0, 255, 255, 255].repeat(6));

        // Mistakes come back as failures, and the device stays good.
        for (req, says) in [
            (
                Req::Texture {
                    json: r#"{"size":[4],"format":"rgba9unorm"}"#.into(),
                },
                "texture descriptor",
            ),
            (
                Req::WriteTexture {
                    tex: src,
                    region: [0, 0, 0, 0, 2, 2, 1],
                    data: vec![0; 15],
                },
                "16",
            ),
            (
                Req::Render {
                    json: format!(r#"{{"pipeline":{src},"draws":[]}}"#),
                },
                "at least one draw",
            ),
            (
                Req::BindGroup {
                    shader: quad,
                    group: 1,
                    entries: vec![(0, src)],
                },
                "",
            ),
            (
                Req::Release {
                    handle: (1 << 16) | 1,
                },
                "another of the task's GPUs",
            ),
        ] {
            match call(&mut c, req) {
                Resp::Fail(m) => assert!(m.contains(says), "{m}"),
                other => panic!("{other:?}"),
            }
        }
        assert!(matches!(call(&mut c, Req::Wait), Resp::Value(0)));
        // Textures count against the memory limit (64 MiB here).
        match call(
            &mut c,
            Req::Texture {
                json: r#"{"size":[4096,4096],"format":"rgba32float"}"#.into(),
            },
        ) {
            Resp::Fail(m) => assert!(m.contains("memory limit"), "{m}"),
            other => panic!("{other:?}"),
        }
    }
}
