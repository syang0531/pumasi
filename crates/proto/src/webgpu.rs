//! WebGPU adapters a node offers to wasm jobs (phase 14, experimental).
//!
//! A wasm job that asks for a GPU (`gpus = 1`) gets one WebGPU adapter: the
//! agent opens it with wgpu (Vulkan, Metal, DX12 or OpenGL, whatever the
//! machine has) and the module drives it through the `pumasi_webgpu`
//! imports ([`WEBGPU_IMPORTS`]), running WGSL compute shaders. This is the
//! only way to use GPUs without CUDA: Intel and AMD integrated GPUs, the
//! GPUs of old Intel Macs, Apple Silicon.
//!
//! Adapters travel in their own signed message
//! ([`crate::msg::FromAgent::WebGpu`], protocol 12) so the device profile
//! of phase 9 keeps its layout. An adapter that is one of the node's
//! listed GPUs ([`WebGpuAdapter::gpu`]) shares its slot with it, so a
//! Docker job and a WebGPU job never use the same GPU at once.

use std::{collections::HashSet, fmt};

use anyhow::{Context, Result, ensure};
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::gpu::{GpuRequest, GpuVendor, format_tflops};

const WEBGPU_DOMAIN: &[u8] = b"pumasi/webgpu/node/v1\0";

/// Bumped when the WebGPU benchmark changes.
pub const WEBGPU_BENCH_VERSION: u32 = 1;

/// The import module of the host API.
pub const WEBGPU_MODULE: &str = "pumasi_webgpu";

/// The functions of [`WEBGPU_MODULE`], for documentation and tests. Every
/// call returns a negative number on failure and `error` gives the reason.
pub const WEBGPU_IMPORTS: &[&str] = &[
    "adapter",
    "shader",
    "buffer",
    "write",
    "read",
    "dispatch",
    "wait",
    "release",
    "error",
    // 1.16: several adapters, and drawing.
    "adapters",
    "select",
    "texture",
    "sampler",
    "render_shader",
    "write_texture",
    "read_texture",
    "bind_group",
    "dispatch_groups",
    "render",
];

/// Most WebGPU adapters one wasm task may ask for (`gpus = 8`, 1.16).
pub const MAX_WEBGPU_PER_TASK: u32 = 8;

/// Slots of adapters that are not one of the node's listed GPUs start
/// here, so they never collide with GPU indices in the busy set.
pub const WEBGPU_SLOT_BASE: u32 = 1 << 16;

/// FP32 GFLOPS an untrusted node's WebGPU adapter is billed at, at most,
/// until the cluster has timed it with a GPU challenge (1.10: over WebGPU
/// itself, or as the CUDA GPU it is): about a recent laptop's integrated
/// GPU.
pub const UNCHECKED_WEBGPU_GFLOPS: u32 = 1_000;

const WEBGPU_EXTRA_DOMAIN: &[u8] = b"pumasi/webgpu-extra/node/v1\0";

/// Optional WebGPU features a device is opened with when its adapter has
/// them (1.10); a wasm job can require them (`gpu.features`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WebGpuFeature {
    /// `enable f16;` in WGSL: 16-bit floats.
    ShaderF16,
    /// `enable subgroups;` in WGSL: subgroup operations.
    Subgroups,
}

impl WebGpuFeature {
    pub const ALL: [WebGpuFeature; 2] = [WebGpuFeature::ShaderF16, WebGpuFeature::Subgroups];
}

impl fmt::Display for WebGpuFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The names WebGPU itself uses.
        f.write_str(match self {
            WebGpuFeature::ShaderF16 => "shader-f16",
            WebGpuFeature::Subgroups => "subgroups",
        })
    }
}

impl std::str::FromStr for WebGpuFeature {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim().to_ascii_lowercase();
        Self::ALL
            .into_iter()
            .find(|f| f.to_string() == s)
            .ok_or_else(|| {
                anyhow::anyhow!("unknown WebGPU feature {s:?} (shader-f16 or subgroups)")
            })
    }
}

/// What one adapter has beyond [`WebGpuAdapter`] (1.10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterExtra {
    /// [`WebGpuAdapter::index`].
    pub index: u32,
    /// GPU challenge hashes per second over WebGPU, as the node measured
    /// them (0: not measured). The cluster times the adapter against it.
    pub hash_rate: u64,
    /// Optional features its devices are opened with.
    pub features: Vec<WebGpuFeature>,
}

/// What a node's WebGPU adapters have beyond its [`WebGpuProfile`],
/// signed by it (1.10). A message of its own, so the phase 14 profile and
/// what is stored of it keep their layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebGpuExtra {
    /// The node's public key.
    pub node: [u8; 32],
    pub measured_unix: u64,
    pub adapters: Vec<AdapterExtra>,
}

impl WebGpuExtra {
    pub fn get(&self, index: u32) -> Option<&AdapterExtra> {
        self.adapters.iter().find(|a| a.index == index)
    }

    /// Whether adapter `index` has every one of `features`.
    pub fn has(&self, index: u32, features: &[WebGpuFeature]) -> bool {
        features.is_empty()
            || self
                .get(index)
                .is_some_and(|a| features.iter().all(|f| a.features.contains(f)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedWebGpuExtra {
    pub extra: WebGpuExtra,
    pub sig: Signature,
}

fn extra_message(e: &WebGpuExtra) -> Vec<u8> {
    let mut m = WEBGPU_EXTRA_DOMAIN.to_vec();
    m.extend(postcard::to_stdvec(e).expect("extra encodes"));
    m
}

impl SignedWebGpuExtra {
    pub fn sign(extra: WebGpuExtra, key: &SecretKey) -> Self {
        let sig = key.sign(&extra_message(&extra));
        Self { extra, sig }
    }

    pub fn verify(&self) -> Result<()> {
        let key = PublicKey::from_bytes(&self.extra.node).context("bad node key")?;
        key.verify(&extra_message(&self.extra), &self.sig)
            .map_err(|_| anyhow::anyhow!("WebGPU extra signature does not match"))?;
        let indices: HashSet<u32> = self.extra.adapters.iter().map(|a| a.index).collect();
        ensure!(
            indices.len() == self.extra.adapters.len(),
            "WebGPU extra lists an adapter twice"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WebGpuBackend {
    Vulkan,
    Metal,
    Dx12,
    Gl,
}

impl fmt::Display for WebGpuBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WebGpuBackend::Vulkan => "vulkan",
            WebGpuBackend::Metal => "metal",
            WebGpuBackend::Dx12 => "dx12",
            WebGpuBackend::Gl => "gl",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WebGpuDeviceType {
    Discrete,
    Integrated,
    Virtual,
    /// A software rasterizer (lavapipe, llvmpipe, WARP, SwiftShader).
    Cpu,
    Other,
}

impl fmt::Display for WebGpuDeviceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WebGpuDeviceType::Discrete => "discrete",
            WebGpuDeviceType::Integrated => "integrated",
            WebGpuDeviceType::Virtual => "virtual",
            WebGpuDeviceType::Cpu => "software",
            WebGpuDeviceType::Other => "other",
        })
    }
}

/// One WebGPU adapter as the node sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebGpuAdapter {
    /// The node's own number for it.
    pub index: u32,
    pub name: String,
    pub vendor: GpuVendor,
    pub backend: WebGpuBackend,
    pub device_type: WebGpuDeviceType,
    /// Driver name and version, as the backend reports them.
    pub driver: String,
    /// The index of the same GPU in the node's device profile, when it is
    /// one of them.
    pub gpu: Option<u32>,
    /// PCI address, when the backend reports it.
    pub pci: String,
    /// Largest buffer, and largest storage buffer binding, in MiB.
    pub max_buffer_mib: u64,
    pub max_binding_mib: u64,
    /// FP32 throughput the agent measured with a WGSL shader (0: not
    /// measured).
    pub fp32_gflops: u32,
}

impl WebGpuAdapter {
    /// The adapter's slot among the node's GPUs: its GPU's index, or a
    /// number of its own above [`WEBGPU_SLOT_BASE`].
    pub fn slot(&self) -> u32 {
        self.gpu.unwrap_or(WEBGPU_SLOT_BASE + self.index)
    }

    /// A software rasterizer running on the CPU.
    pub fn software(&self) -> bool {
        self.device_type == WebGpuDeviceType::Cpu
    }

    /// `Intel(R) Iris(TM) Plus Graphics (metal, integrated, 420 GFLOPS)`.
    pub fn describe(&self) -> String {
        let mut s = format!("{} ({}, {}", self.name, self.backend, self.device_type);
        if self.fp32_gflops >= 1000 {
            s.push_str(&format!(", {} TFLOPS", format_tflops(self.fp32_gflops)));
        } else if self.fp32_gflops > 0 {
            s.push_str(&format!(", {} GFLOPS", self.fp32_gflops));
        }
        s.push(')');
        s
    }
}

/// The WebGPU adapters a node offers, signed by it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebGpuProfile {
    pub bench_version: u32,
    /// The node's public key.
    pub node: [u8; 32],
    pub measured_unix: u64,
    pub adapters: Vec<WebGpuAdapter>,
    /// What could not be found or measured, and why.
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedWebGpu {
    pub profile: WebGpuProfile,
    pub sig: Signature,
}

fn webgpu_message(p: &WebGpuProfile) -> Vec<u8> {
    let mut m = WEBGPU_DOMAIN.to_vec();
    m.extend(postcard::to_stdvec(p).expect("profile encodes"));
    m
}

impl SignedWebGpu {
    pub fn sign(profile: WebGpuProfile, key: &SecretKey) -> Self {
        let sig = key.sign(&webgpu_message(&profile));
        Self { profile, sig }
    }

    pub fn verify(&self) -> Result<()> {
        let key = PublicKey::from_bytes(&self.profile.node).context("bad node key")?;
        key.verify(&webgpu_message(&self.profile), &self.sig)
            .map_err(|_| anyhow::anyhow!("WebGPU profile signature does not match"))?;
        let indices: HashSet<u32> = self.profile.adapters.iter().map(|a| a.index).collect();
        ensure!(
            indices.len() == self.profile.adapters.len(),
            "WebGPU profile lists an adapter twice"
        );
        let slots: HashSet<u32> = self.profile.adapters.iter().map(|a| a.slot()).collect();
        ensure!(
            slots.len() == self.profile.adapters.len(),
            "WebGPU profile maps two adapters to one GPU"
        );
        Ok(())
    }
}

impl GpuRequest {
    /// Checks a GPU request of a wasm job: one WebGPU adapter per task, or
    /// up to [`MAX_WEBGPU_PER_TASK`] since 1.16.
    pub fn validate_webgpu(&self) -> Result<()> {
        ensure!(
            (1..=MAX_WEBGPU_PER_TASK).contains(&self.count),
            "a wasm task uses 1 to {MAX_WEBGPU_PER_TASK} GPUs (gpus = 1), through WebGPU"
        );
        ensure!(
            self.min_vram_mib == 0,
            "WebGPU does not report GPU memory; drop gpu.min_vram_gb for wasm jobs"
        );
        Ok(())
    }

    /// Whether an adapter fits the request.
    pub fn matches_webgpu(&self, a: &WebGpuAdapter) -> bool {
        self.vendor.is_none_or(|v| v == a.vendor)
    }

    /// Picks `count` WebGPU adapters for one wasm task, leaving out busy
    /// slots: hardware before software, then the fastest. Returns their
    /// slots, in the order the module numbers them.
    pub fn pick_webgpu(&self, adapters: &[WebGpuAdapter], busy: &HashSet<u32>) -> Option<Vec<u32>> {
        self.pick_webgpu_with(adapters, None, &[], busy)
    }

    /// [`Self::pick_webgpu`] among the adapters that have `features`
    /// (1.10), as the node's [`WebGpuExtra`] says.
    pub fn pick_webgpu_with(
        &self,
        adapters: &[WebGpuAdapter],
        extra: Option<&WebGpuExtra>,
        features: &[WebGpuFeature],
        busy: &HashSet<u32>,
    ) -> Option<Vec<u32>> {
        let mut fit: Vec<&WebGpuAdapter> = adapters
            .iter()
            .filter(|a| self.matches_webgpu(a) && !busy.contains(&a.slot()))
            .filter(|a| features.is_empty() || extra.is_some_and(|e| e.has(a.index, features)))
            .collect();
        let want = self.count.max(1) as usize;
        if fit.len() < want {
            return None;
        }
        fit.sort_by_key(|a| {
            std::cmp::Reverse((!a.software(), a.fp32_gflops, std::cmp::Reverse(a.index)))
        });
        Some(fit[..want].iter().map(|a| a.slot()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn adapter(index: u32, gpu: Option<u32>, kind: WebGpuDeviceType) -> WebGpuAdapter {
        WebGpuAdapter {
            index,
            name: format!("adapter{index}"),
            vendor: GpuVendor::Intel,
            backend: WebGpuBackend::Vulkan,
            device_type: kind,
            driver: String::new(),
            gpu,
            pci: String::new(),
            max_buffer_mib: 256,
            max_binding_mib: 128,
            fp32_gflops: 100 * (index + 1),
        }
    }

    #[test]
    fn picks_hardware_first_and_skips_busy_slots() {
        let adapters = vec![
            adapter(0, None, WebGpuDeviceType::Cpu),
            adapter(1, Some(0), WebGpuDeviceType::Integrated),
        ];
        let req = GpuRequest {
            count: 1,
            vendor: None,
            min_vram_mib: 0,
        };
        assert_eq!(req.pick_webgpu(&adapters, &HashSet::new()), Some(vec![0]));
        assert_eq!(
            req.pick_webgpu(&adapters, &HashSet::from([0])),
            Some(vec![WEBGPU_SLOT_BASE])
        );
        assert_eq!(
            req.pick_webgpu(&adapters, &HashSet::from([0, WEBGPU_SLOT_BASE])),
            None
        );
        let amd = GpuRequest {
            vendor: Some(GpuVendor::Amd),
            ..req.clone()
        };
        assert_eq!(amd.pick_webgpu(&adapters, &HashSet::new()), None);
        assert!(req.validate_webgpu().is_ok());
        // Several adapters (1.16): the fastest first, all of them free.
        let two = GpuRequest {
            count: 2,
            ..req.clone()
        };
        assert!(two.validate_webgpu().is_ok());
        assert_eq!(
            two.pick_webgpu(&adapters, &HashSet::new()),
            Some(vec![0, WEBGPU_SLOT_BASE])
        );
        assert_eq!(two.pick_webgpu(&adapters, &HashSet::from([0])), None);
        for count in [0, MAX_WEBGPU_PER_TASK + 1] {
            assert!(
                GpuRequest {
                    count,
                    ..req.clone()
                }
                .validate_webgpu()
                .is_err()
            );
        }
        assert!(
            GpuRequest {
                min_vram_mib: 1024,
                ..req
            }
            .validate_webgpu()
            .is_err()
        );
    }

    #[test]
    fn profiles_are_signed() {
        let key = SecretKey::generate();
        let profile = WebGpuProfile {
            bench_version: WEBGPU_BENCH_VERSION,
            node: *key.public().as_bytes(),
            measured_unix: 1,
            adapters: vec![adapter(0, None, WebGpuDeviceType::Discrete)],
            notes: Vec::new(),
        };
        let signed = SignedWebGpu::sign(profile.clone(), &key);
        signed.verify().unwrap();
        let mut forged = signed.clone();
        forged.profile.adapters[0].fp32_gflops = 99_999;
        assert!(forged.verify().is_err());
        let twice = SignedWebGpu::sign(
            WebGpuProfile {
                adapters: vec![
                    adapter(0, Some(1), WebGpuDeviceType::Discrete),
                    adapter(1, Some(1), WebGpuDeviceType::Discrete),
                ],
                ..profile
            },
            &key,
        );
        assert!(twice.verify().is_err(), "two adapters on one GPU");
    }

    #[test]
    fn features_pick_and_sign() {
        let key = SecretKey::generate();
        let extra = WebGpuExtra {
            node: *key.public().as_bytes(),
            measured_unix: 1,
            adapters: vec![
                AdapterExtra {
                    index: 0,
                    hash_rate: 5,
                    features: vec![WebGpuFeature::ShaderF16],
                },
                AdapterExtra {
                    index: 1,
                    hash_rate: 9,
                    features: vec![],
                },
            ],
        };
        let signed = SignedWebGpuExtra::sign(extra.clone(), &key);
        signed.verify().unwrap();
        let mut forged = signed.clone();
        forged.extra.adapters[1].hash_rate = 99;
        assert!(forged.verify().is_err());
        let adapters = vec![
            adapter(0, None, WebGpuDeviceType::Integrated),
            adapter(1, None, WebGpuDeviceType::Discrete),
        ];
        let req = GpuRequest {
            count: 1,
            vendor: None,
            min_vram_mib: 0,
        };
        let none = HashSet::new();
        // The faster adapter without a feature requirement...
        assert_eq!(
            req.pick_webgpu_with(&adapters, Some(&extra), &[], &none),
            Some(vec![WEBGPU_SLOT_BASE + 1])
        );
        // ...the one with f16 when the job needs it, and none for both.
        let f16 = [WebGpuFeature::ShaderF16];
        assert_eq!(
            req.pick_webgpu_with(&adapters, Some(&extra), &f16, &none),
            Some(vec![WEBGPU_SLOT_BASE])
        );
        assert_eq!(req.pick_webgpu_with(&adapters, None, &f16, &none), None);
        assert_eq!(
            req.pick_webgpu_with(&adapters, Some(&extra), &WebGpuFeature::ALL, &none),
            None
        );
        assert_eq!(
            "Shader-F16".parse::<WebGpuFeature>().unwrap(),
            WebGpuFeature::ShaderF16
        );
        assert!("f64".parse::<WebGpuFeature>().is_err());
    }
}
