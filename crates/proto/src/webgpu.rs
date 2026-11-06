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
    "adapter", "shader", "buffer", "write", "read", "dispatch", "wait", "release", "error",
];

/// Slots of adapters that are not one of the node's listed GPUs start
/// here, so they never collide with GPU indices in the busy set.
pub const WEBGPU_SLOT_BASE: u32 = 1 << 16;

/// FP32 GFLOPS an untrusted node's WebGPU adapter is billed at, at most,
/// when the cluster cannot time it (no GPU challenge runs on WebGPU yet):
/// about a recent laptop's integrated GPU.
pub const UNCHECKED_WEBGPU_GFLOPS: u32 = 1_000;

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
    /// Checks a GPU request of a wasm job: one WebGPU adapter per task.
    pub fn validate_webgpu(&self) -> Result<()> {
        ensure!(
            self.count == 1,
            "a wasm task uses one GPU (gpus = 1), through WebGPU"
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

    /// Picks a WebGPU adapter for one wasm task, leaving out busy slots:
    /// hardware before software, then the fastest. Returns its slot.
    pub fn pick_webgpu(&self, adapters: &[WebGpuAdapter], busy: &HashSet<u32>) -> Option<Vec<u32>> {
        adapters
            .iter()
            .filter(|a| self.matches_webgpu(a) && !busy.contains(&a.slot()))
            .max_by_key(|a| (!a.software(), a.fp32_gflops, std::cmp::Reverse(a.index)))
            .map(|a| vec![a.slot()])
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
        assert!(
            GpuRequest {
                count: 2,
                ..req.clone()
            }
            .validate_webgpu()
            .is_err()
        );
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
}
