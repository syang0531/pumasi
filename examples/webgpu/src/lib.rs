//! Bindings for Pumasi's WebGPU host API (`pumasi_webgpu`), for wasm
//! modules built for `wasm32-wasip1`. A wasm job with `gpus = 1` gets one
//! GPU and runs WGSL compute shaders on it, and since 1.16 draws with
//! textures, samplers and render passes ([`Texture`], [`RenderShader`],
//! [`render`]); `gpus = 2` or more gives it several ([`adapters`],
//! [`select`]). See `docs/architecture.md` ("WebGPU executor", "Phase
//! 32") for the calls.
//!
//! ```no_run
//! use webgpu_examples::{Buffer, Shader, dispatch, wait};
//! let shader = Shader::new(SRC, "main")?;
//! let data = Buffer::from_f32(&[1.0, 2.0, 3.0, 4.0])?;
//! dispatch(&shader, &[&data], [1, 1, 1])?;
//! wait()?;
//! let out = data.read_f32()?;
//! ```

mod sys {
    #[link(wasm_import_module = "pumasi_webgpu")]
    unsafe extern "C" {
        pub fn adapter(ptr: *mut u8, cap: i32) -> i32;
        pub fn shader(src: *const u8, len: i32, entry: *const u8, entry_len: i32) -> i32;
        pub fn buffer(size: i64) -> i32;
        pub fn write(buf: i32, offset: i64, src: *const u8, len: i32) -> i32;
        pub fn read(buf: i32, offset: i64, dst: *mut u8, len: i32) -> i32;
        pub fn dispatch(shader: i32, bufs: *const i32, n: i32, x: i32, y: i32, z: i32) -> i32;
        pub fn wait() -> i32;
        pub fn release(handle: i32) -> i32;
        pub fn error(ptr: *mut u8, cap: i32) -> i32;
        // 1.16
        pub fn adapters() -> i32;
        pub fn select(index: i32) -> i32;
        pub fn texture(json: *const u8, len: i32) -> i32;
        pub fn sampler(json: *const u8, len: i32) -> i32;
        pub fn render_shader(src: *const u8, len: i32, json: *const u8, json_len: i32) -> i32;
        pub fn write_texture(
            tex: i32,
            mip: i32,
            x: i32,
            y: i32,
            z: i32,
            w: i32,
            h: i32,
            d: i32,
            src: *const u8,
            len: i32,
        ) -> i32;
        pub fn read_texture(
            tex: i32,
            mip: i32,
            x: i32,
            y: i32,
            z: i32,
            w: i32,
            h: i32,
            d: i32,
            dst: *mut u8,
            len: i32,
        ) -> i32;
        pub fn bind_group(shader: i32, group: i32, entries: *const i32, n: i32) -> i32;
        pub fn dispatch_groups(
            shader: i32,
            groups: *const i32,
            n: i32,
            x: i32,
            y: i32,
            z: i32,
        ) -> i32;
        pub fn render(json: *const u8, len: i32) -> i32;
    }
}

/// What the host said went wrong.
#[derive(Debug, Clone)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

fn last_error() -> Error {
    let mut buf = vec![0u8; 4096];
    let n = unsafe { sys::error(buf.as_mut_ptr(), buf.len() as i32) };
    buf.truncate(n.clamp(0, buf.len() as i32) as usize);
    Error(String::from_utf8_lossy(&buf).into_owned())
}

fn check(r: i32) -> Result<i32> {
    if r < 0 { Err(last_error()) } else { Ok(r) }
}

/// The task's GPU as JSON (name, backend, limits), or `None` when the
/// task has none (the job did not ask for `gpus = 1`).
pub fn adapter() -> Option<String> {
    let mut buf = vec![0u8; 4096];
    let n = unsafe { sys::adapter(buf.as_mut_ptr(), buf.len() as i32) };
    if n < 0 {
        return None;
    }
    if n as usize > buf.len() {
        buf = vec![0u8; n as usize];
        unsafe { sys::adapter(buf.as_mut_ptr(), buf.len() as i32) };
    }
    buf.truncate(n as usize);
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// A string field of the adapter's JSON (no JSON parser needed for that).
pub fn json_field(json: &str, key: &str) -> Option<String> {
    let at = json.find(&format!("\"{key}\":"))? + key.len() + 3;
    let rest = json[at..].trim_start();
    if let Some(s) = rest.strip_prefix('"') {
        return s.find('"').map(|end| s[..end].to_string());
    }
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    Some(rest[..end].trim().to_string())
}

/// A compiled WGSL compute entry point.
pub struct Shader(i32);

impl Shader {
    pub fn new(wgsl: &str, entry: &str) -> Result<Self> {
        let h = unsafe {
            sys::shader(
                wgsl.as_ptr(),
                wgsl.len() as i32,
                entry.as_ptr(),
                entry.len() as i32,
            )
        };
        check(h).map(Shader)
    }
}

impl Drop for Shader {
    fn drop(&mut self) {
        unsafe { sys::release(self.0) };
    }
}

/// A storage buffer on the GPU.
pub struct Buffer {
    handle: i32,
    size: usize,
}

impl Buffer {
    /// A zero-filled buffer; `size` is rounded up to a multiple of 4.
    pub fn new(size: usize) -> Result<Self> {
        let size = size.div_ceil(4) * 4;
        let handle = check(unsafe { sys::buffer(size as i64) })?;
        Ok(Self { handle, size })
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let b = Self::new(data.len())?;
        b.write(0, data)?;
        Ok(b)
    }

    pub fn from_f32(data: &[f32]) -> Result<Self> {
        Self::from_bytes(&f32_bytes(data))
    }

    pub fn from_u32(data: &[u32]) -> Result<Self> {
        Self::from_bytes(
            &data
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<u8>>(),
        )
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Copies `data` (a multiple of 4 bytes) in at `offset`.
    pub fn write(&self, offset: usize, data: &[u8]) -> Result<()> {
        check(unsafe { sys::write(self.handle, offset as i64, data.as_ptr(), data.len() as i32) })
            .map(drop)
    }

    /// Waits for earlier dispatches and copies the buffer out.
    pub fn read(&self) -> Result<Vec<u8>> {
        let mut out = vec![0u8; self.size];
        check(unsafe { sys::read(self.handle, 0, out.as_mut_ptr(), out.len() as i32) })?;
        Ok(out)
    }

    pub fn read_f32(&self) -> Result<Vec<f32>> {
        Ok(self
            .read()?
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        unsafe { sys::release(self.handle) };
    }
}

pub fn f32_bytes(data: &[f32]) -> Vec<u8> {
    data.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Runs `groups` workgroups of `shader` with `buffers` at `@group(0)
/// @binding(0..)`. Returns at once; [`wait`] or a read waits for it.
pub fn dispatch(shader: &Shader, buffers: &[&Buffer], groups: [u32; 3]) -> Result<()> {
    let handles: Vec<i32> = buffers.iter().map(|b| b.handle).collect();
    check(unsafe {
        sys::dispatch(
            shader.0,
            handles.as_ptr(),
            handles.len() as i32,
            groups[0] as i32,
            groups[1] as i32,
            groups[2] as i32,
        )
    })
    .map(drop)
}

/// Waits until every dispatch so far has finished.
pub fn wait() -> Result<()> {
    check(unsafe { sys::wait() }).map(drop)
}

/// How many GPUs the task has (1.16): `gpus = 2` gives two. 0 without
/// any.
pub fn adapters() -> usize {
    unsafe { sys::adapters() }.max(0) as usize
}

/// Makes the task's GPU number `index` (from 0) the one new shaders,
/// buffers and textures go to, and the one [`adapter`] describes (1.16).
/// Every object stays on the GPU it was made on.
pub fn select(index: usize) -> Result<()> {
    check(unsafe { sys::select(index as i32) }).map(drop)
}

/// A texture (1.16), made from a `GPUTextureDescriptor` as JSON:
/// `{"size":[w,h],"format":"rgba8unorm"}`; `usage` defaults to every use
/// the format allows.
pub struct Texture(i32);

impl Texture {
    pub fn new(descriptor: &str) -> Result<Self> {
        check(unsafe { sys::texture(descriptor.as_ptr(), descriptor.len() as i32) }).map(Texture)
    }

    /// The handle, for JSON descriptors (`"view": handle`).
    pub fn handle(&self) -> i32 {
        self.0
    }

    /// Copies tightly packed texels into the region `[x, y, z]` and
    /// `[width, height, depth]` of mip level `mip`.
    pub fn write(&self, mip: u32, origin: [u32; 3], size: [u32; 3], data: &[u8]) -> Result<()> {
        check(unsafe {
            sys::write_texture(
                self.0,
                mip as i32,
                origin[0] as i32,
                origin[1] as i32,
                origin[2] as i32,
                size[0] as i32,
                size[1] as i32,
                size[2] as i32,
                data.as_ptr(),
                data.len() as i32,
            )
        })
        .map(drop)
    }

    /// Waits for earlier work and copies a region out, tightly packed
    /// (`bytes_per_texel` is 4 for `rgba8unorm`).
    pub fn read(
        &self,
        mip: u32,
        origin: [u32; 3],
        size: [u32; 3],
        bytes_per_texel: usize,
    ) -> Result<Vec<u8>> {
        let mut out =
            vec![0u8; size[0] as usize * size[1] as usize * size[2] as usize * bytes_per_texel];
        check(unsafe {
            sys::read_texture(
                self.0,
                mip as i32,
                origin[0] as i32,
                origin[1] as i32,
                origin[2] as i32,
                size[0] as i32,
                size[1] as i32,
                size[2] as i32,
                out.as_mut_ptr(),
                out.len() as i32,
            )
        })?;
        Ok(out)
    }
}

impl Drop for Texture {
    fn drop(&mut self) {
        unsafe { sys::release(self.0) };
    }
}

/// A sampler (1.16) from a `GPUSamplerDescriptor` as JSON:
/// `{"magFilter":"linear","minFilter":"linear"}`.
pub struct Sampler(i32);

impl Sampler {
    pub fn new(descriptor: &str) -> Result<Self> {
        check(unsafe { sys::sampler(descriptor.as_ptr(), descriptor.len() as i32) }).map(Sampler)
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        unsafe { sys::release(self.0) };
    }
}

/// A render pipeline (1.16): WGSL with vertex and fragment entry points,
/// and a `GPURenderPipelineDescriptor` as JSON without `module` and
/// `layout`: `{"vertex":{"entryPoint":"vs"},"fragment":{"entryPoint":"fs",
/// "targets":[{"format":"rgba8unorm"}]}}`.
pub struct RenderShader(i32);

impl RenderShader {
    pub fn new(wgsl: &str, descriptor: &str) -> Result<Self> {
        check(unsafe {
            sys::render_shader(
                wgsl.as_ptr(),
                wgsl.len() as i32,
                descriptor.as_ptr(),
                descriptor.len() as i32,
            )
        })
        .map(RenderShader)
    }

    pub fn handle(&self) -> i32 {
        self.0
    }
}

impl Drop for RenderShader {
    fn drop(&mut self) {
        unsafe { sys::release(self.0) };
    }
}

/// What goes in a bind group.
pub enum Resource<'a> {
    Buffer(&'a Buffer),
    Texture(&'a Texture),
    Sampler(&'a Sampler),
}

impl Resource<'_> {
    fn handle(&self) -> i32 {
        match self {
            Resource::Buffer(b) => b.handle,
            Resource::Texture(t) => t.0,
            Resource::Sampler(s) => s.0,
        }
    }
}

/// A bind group (1.16) for group `group` of a compute or render shader,
/// with `(binding, resource)` entries. It fits only that shader (layouts
/// are automatic).
pub struct BindGroup(i32);

impl BindGroup {
    fn make(shader: i32, group: u32, entries: &[(u32, Resource)]) -> Result<Self> {
        let flat: Vec<i32> = entries
            .iter()
            .flat_map(|(b, r)| [*b as i32, r.handle()])
            .collect();
        check(unsafe { sys::bind_group(shader, group as i32, flat.as_ptr(), entries.len() as i32) })
            .map(BindGroup)
    }

    pub fn compute(shader: &Shader, group: u32, entries: &[(u32, Resource)]) -> Result<Self> {
        Self::make(shader.0, group, entries)
    }

    pub fn render(shader: &RenderShader, group: u32, entries: &[(u32, Resource)]) -> Result<Self> {
        Self::make(shader.0, group, entries)
    }

    pub fn handle(&self) -> i32 {
        self.0
    }
}

impl Drop for BindGroup {
    fn drop(&mut self) {
        unsafe { sys::release(self.0) };
    }
}

impl Buffer {
    /// The handle, for JSON descriptors (`"vertexBuffers": [handle]`).
    pub fn handle(&self) -> i32 {
        self.handle
    }
}

/// Runs `groups` workgroups of `shader` with bind groups at `@group(0..)`
/// (1.16).
pub fn dispatch_groups(shader: &Shader, bind: &[&BindGroup], groups: [u32; 3]) -> Result<()> {
    let handles: Vec<i32> = bind.iter().map(|b| b.0).collect();
    check(unsafe {
        sys::dispatch_groups(
            shader.0,
            handles.as_ptr(),
            handles.len() as i32,
            groups[0] as i32,
            groups[1] as i32,
            groups[2] as i32,
        )
    })
    .map(drop)
}

/// Runs one render pass (1.16), described as JSON: a
/// `GPURenderPassDescriptor` plus `pipeline`, `bindGroups`,
/// `vertexBuffers`, `indexBuffer`, `viewport`, `scissorRect` and `draws`,
/// with handles where WebGPU takes objects.
pub fn render(pass: &str) -> Result<()> {
    check(unsafe { sys::render(pass.as_ptr(), pass.len() as i32) }).map(drop)
}

/// SplitMix64, so every run builds the same inputs.
pub struct Rng(pub u64);

impl Rng {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// In [0, 1).
    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
}

/// Largest error relative to the reference's magnitude.
pub fn max_relative_error(gpu: &[f32], cpu: &[f32]) -> f64 {
    let scale = cpu
        .iter()
        .fold(0f64, |m, v| m.max(v.abs() as f64))
        .max(1e-30);
    gpu.iter()
        .zip(cpu)
        .map(|(g, c)| {
            if g.is_finite() {
                (*g as f64 - *c as f64).abs() / scale
            } else {
                f64::INFINITY
            }
        })
        .fold(0f64, f64::max)
}
