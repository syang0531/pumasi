//! Bindings for Pumasi's WebGPU host API (`pumasi_webgpu`), for wasm
//! modules built for `wasm32-wasip1`. A wasm job with `gpus = 1` gets one
//! GPU and runs WGSL compute shaders on it; see `docs/architecture.md`
//! ("WebGPU executor") for the calls.
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
