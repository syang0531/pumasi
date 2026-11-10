//! What Docker GPU tasks can do on this machine (phase 15): CUDA,
//! OptiX, and drawing (EGL, OpenGL, Vulkan) inside a container. `pumasi
//! gpu check --docker-run` runs [`probe`] and saves the answer next to the
//! agent's state; the agent reads it and reports whether its Docker GPUs
//! can draw (the node label `gpu.graphics=true`), so only such nodes get
//! jobs that ask for `capabilities = ["graphics"]`.
//!
//! The probe starts a container with every NVIDIA driver capability and
//! looks at what the NVIDIA Container Toolkit put in it: `nvidia-smi`,
//! the NVIDIA EGL, GLX and Vulkan drivers on Linux, or `/dev/dxg` and the
//! D3D12 libraries on WSL 2 (Docker Desktop), where drawing goes through
//! Mesa's D3D12 driver in the image. With an image that has `eglinfo` or
//! `vulkaninfo` (the `examples/blender-render` image does), it also asks
//! them which renderer they get.
//!
//! Docker Desktop gives `--gpus` containers `/dev/dxg` and the CUDA
//! libraries but not `/usr/lib/wsl/lib`, where the Windows driver's D3D12
//! libraries are, so nothing can draw on the GPU there. On Windows hosts
//! the probe and graphics tasks get that folder read-only from Docker
//! Desktop's VM ([`wsl_libs_args`]): the one host folder Pumasi puts in a
//! container.

use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// The image probed when none is given: only `sh` and `ls` are needed.
pub const DEFAULT_PROBE_IMAGE: &str = "ubuntu:24.04";
/// Where `pumasi gpu check --docker-run` saves its answer, in the agent's
/// data directory.
pub const CHECK_FILE: &str = "docker-gpu.json";

/// The folder in Docker Desktop's VM with the Windows GPU driver's Linux
/// libraries (`libd3d12.so`, `libdxcore.so`, `libcuda.so`, ...).
pub const WSL_LIBS: &str = "/usr/lib/wsl";

/// Whether graphics tasks and the probe get [`WSL_LIBS`] when nobody says:
/// on Windows hosts, where Docker means Docker Desktop on WSL 2.
pub fn wsl_libs_auto() -> bool {
    cfg!(windows)
}

/// The `docker create`/`run` options that put [`WSL_LIBS`] in a container,
/// read-only. `--volume` (not `--mount`) so a VM without the folder gives
/// an empty one instead of failing the task: drawing then falls back to
/// the CPU, as it did before.
pub fn wsl_libs_args() -> [String; 2] {
    ["--volume".into(), format!("{WSL_LIBS}:{WSL_LIBS}:ro")]
}

/// Prints `key=value` lines about the container's GPU stack.
const SCRIPT: &str = r#"
smi=""
for b in nvidia-smi /usr/lib/wsl/lib/nvidia-smi /usr/bin/nvidia-smi; do
  if command -v "$b" >/dev/null 2>&1 || [ -x "$b" ]; then smi="$b"; break; fi
done
if [ -n "$smi" ]; then
  echo "cuda=$("$smi" -L 2>&1 | head -n 8 | tr '\n' ';')"
else
  echo "cuda="
fi
has() { for d in /usr/lib/x86_64-linux-gnu /usr/lib/aarch64-linux-gnu /usr/lib64 /usr/lib /usr/lib/wsl/lib /usr/lib/wsl/drivers; do ls "$d"/$1 >/dev/null 2>&1 && { echo yes; return; }; done; echo no; }
echo "egl_nvidia=$(has 'libEGL_nvidia.so*')"
echo "glx_nvidia=$(has 'libGLX_nvidia.so*')"
echo "optix=$(has 'libnvoptix.so*')"
vk=no
for f in /etc/vulkan/icd.d/nvidia_icd.json /usr/share/vulkan/icd.d/nvidia_icd.json /etc/vulkan/icd.d/nvidia_icd.x86_64.json; do
  [ -e "$f" ] && vk=yes
done
echo "vulkan_nvidia=$vk"
[ -e /dev/dxg ] && echo "wsl_dxg=yes" || echo "wsl_dxg=no"
echo "wsl_d3d12=$(has 'libd3d12.so*')"
echo "mesa_d3d12=$(has 'dri/d3d12_dri.so')"
# What a WSL 2 task does to draw (examples/blender-render/render.sh): Mesa's
# d3d12 driver, loading the Windows driver's libd3d12 from /usr/lib/wsl/lib.
if [ -e /dev/dxg ] && [ -e /usr/lib/wsl/lib/libd3d12.so ]; then
  export LD_LIBRARY_PATH="/usr/lib/wsl/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export GALLIUM_DRIVER=d3d12
fi
if command -v eglinfo >/dev/null 2>&1; then
  echo "egl_renderer=$(eglinfo -B 2>/dev/null | grep -i 'renderer' | head -n 1 | sed 's/.*: *//')"
fi
if command -v vulkaninfo >/dev/null 2>&1; then
  echo "vulkan_device=$(vulkaninfo --summary 2>/dev/null | grep -i 'deviceName' | head -n 1 | sed 's/.*= *//')"
fi
"#;

/// What the probe found.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerGpuCheck {
    /// The image probed.
    pub image: String,
    /// The probe container had [`WSL_LIBS`] from the host.
    #[serde(default)]
    pub wsl_libs: bool,
    pub checked_unix: u64,
    /// The GPUs `nvidia-smi -L` lists inside the container (CUDA works).
    pub cuda: Vec<String>,
    /// The OptiX library is there (Blender Cycles' OptiX device).
    pub optix: bool,
    /// The NVIDIA EGL/GLX/Vulkan drivers are there (Linux hosts).
    pub nvidia_graphics: bool,
    /// WSL 2 GPU paravirtualization (`/dev/dxg` and the D3D12 library):
    /// drawing works through Mesa's D3D12 driver when the image has it.
    pub wsl: bool,
    /// The image has Mesa's D3D12 driver.
    pub mesa_d3d12: bool,
    /// What `eglinfo` and `vulkaninfo` in the image reported, if it has
    /// them.
    pub egl_renderer: Option<String>,
    pub vulkan_device: Option<String>,
    /// Docker GPU tasks here can draw.
    pub graphics: bool,
    /// Why (or why not), for people.
    pub notes: Vec<String>,
}

/// A renderer or device name of a software rasterizer (the CPU).
fn is_software(r: &str) -> bool {
    let r = r.to_ascii_lowercase();
    r.contains("llvmpipe") || r.contains("softpipe") || r.contains("lavapipe")
}

/// A renderer or device name of a GPU.
fn is_hardware(r: &str) -> bool {
    let r = r.to_ascii_lowercase();
    !r.is_empty()
        && (r.contains("nvidia")
            || r.contains("d3d12")
            || r.contains("geforce")
            || r.contains("rtx"))
}

impl DockerGpuCheck {
    /// Where Vulkan draws in the container: `Some(true)` on the GPU,
    /// `Some(false)` on the CPU (lavapipe/llvmpipe), `None` when unknown.
    /// It can differ from EGL/OpenGL: on WSL 2, Mesa draws GL through D3D12
    /// but Vulkan on the CPU.
    pub fn vulkan_on_gpu(&self) -> Option<bool> {
        match self.vulkan_device.as_deref() {
            Some(d) if is_software(d) => Some(false),
            Some(d) if is_hardware(d) => Some(true),
            Some(_) => None,
            None => self.nvidia_graphics.then_some(true),
        }
    }

    /// Reads the `key=value` lines the script printed, in a container that
    /// had [`WSL_LIBS`] if `wsl_libs`.
    pub fn parse(image: &str, wsl_libs: bool, out: &str) -> Self {
        let mut c = DockerGpuCheck {
            image: image.to_string(),
            wsl_libs,
            checked_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            ..Default::default()
        };
        let mut egl_nvidia = false;
        let mut vulkan_nvidia = false;
        let mut glx_nvidia = false;
        let mut dxg = false;
        let mut d3d12 = false;
        for line in out.lines() {
            let Some((k, v)) = line.trim().split_once('=') else {
                continue;
            };
            let v = v.trim();
            let yes = v == "yes";
            match k {
                "cuda" => {
                    c.cuda = v
                        .split(';')
                        .map(str::trim)
                        .filter(|l| l.starts_with("GPU "))
                        .map(String::from)
                        .collect()
                }
                "egl_nvidia" => egl_nvidia = yes,
                "glx_nvidia" => glx_nvidia = yes,
                "vulkan_nvidia" => vulkan_nvidia = yes,
                "optix" => c.optix = yes,
                "wsl_dxg" => dxg = yes,
                "wsl_d3d12" => d3d12 = yes,
                "mesa_d3d12" => c.mesa_d3d12 = yes,
                "egl_renderer" if !v.is_empty() => c.egl_renderer = Some(v.to_string()),
                "vulkan_device" if !v.is_empty() => c.vulkan_device = Some(v.to_string()),
                _ => {}
            }
        }
        c.nvidia_graphics = egl_nvidia && (vulkan_nvidia || glx_nvidia);
        c.wsl = dxg && d3d12;
        // Docker Desktop's OptiX library does not start in a container
        // (Blender: "OptiX initialization failed with error code 7805"),
        // with or without /usr/lib/wsl.
        let wsl_optix = dxg && !c.nvidia_graphics && c.optix;
        if wsl_optix {
            c.optix = false;
        }
        let software = |r: &Option<String>| r.as_deref().is_some_and(is_software);
        let hardware = |r: &Option<String>| r.as_deref().is_some_and(is_hardware);
        if c.cuda.is_empty() {
            c.notes.push(
                "no GPU inside the container: Docker cannot pass NVIDIA GPUs (install the NVIDIA Container Toolkit; on Windows, Docker Desktop with the WSL 2 backend and a current NVIDIA driver)".into(),
            );
            return c;
        }
        if hardware(&c.egl_renderer) || hardware(&c.vulkan_device) {
            c.graphics = true;
            c.notes.push(format!(
                "drawing works on the GPU ({})",
                c.egl_renderer
                    .clone()
                    .or_else(|| c.vulkan_device.clone())
                    .unwrap_or_default()
            ));
        } else if software(&c.egl_renderer) || software(&c.vulkan_device) {
            c.notes.push(format!(
                "drawing falls back to the CPU ({}): the image's GL/Vulkan does not reach the GPU",
                c.egl_renderer
                    .clone()
                    .or_else(|| c.vulkan_device.clone())
                    .unwrap_or_default()
            ));
        } else if c.nvidia_graphics {
            c.graphics = true;
            c.notes.push(
                "the NVIDIA EGL and Vulkan drivers are in the container (graphics capability)"
                    .into(),
            );
        } else if c.wsl {
            c.graphics = true;
            c.notes.push(
                "WSL 2: drawing goes through Mesa's D3D12 driver, so images need Mesa 22 or newer (as in examples/blender-render); confirm with `pumasi gpu check --docker-run --image <your image>`".into(),
            );
        } else {
            c.notes.push(
                "no NVIDIA EGL/Vulkan driver in the container: the toolkit did not mount the graphics libraries".into(),
            );
        }
        if c.graphics && c.vulkan_on_gpu() == Some(false) {
            c.notes.push(format!(
                "Vulkan draws on the CPU here ({}): jobs that draw with Vulkan (such as Blender's Vulkan backend) will not use the GPU; EGL/OpenGL does",
                c.vulkan_device.clone().unwrap_or_default()
            ));
        }
        if dxg && !d3d12 {
            c.notes.push(format!(
                "WSL 2 without the D3D12 libraries in the container: Docker Desktop does not put {WSL_LIBS}/lib in GPU containers{}",
                if c.wsl_libs {
                    ", and its VM has no such folder to lend either"
                } else {
                    " (with --wsl-libs yes here and --docker-wsl-libs yes on the agent, graphics tasks get it read-only from Docker Desktop's VM; that is the default on Windows)"
                }
            ));
        }
        if wsl_optix {
            c.notes.push(
                "OptiX does not start in Docker Desktop containers: Blender Cycles uses CUDA"
                    .into(),
            );
        } else if !c.optix {
            c.notes
                .push("no OptiX library: Blender Cycles can use CUDA, not OptiX".into());
        }
        c
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join(CHECK_FILE), serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }

    /// The last saved check, if any.
    pub fn load(dir: &Path) -> Option<Self> {
        let text = std::fs::read_to_string(dir.join(CHECK_FILE)).ok()?;
        serde_json::from_str(&text).ok()
    }
}

/// The `docker run` arguments of the probe.
fn probe_args(image: &str, wsl_libs: bool) -> Vec<String> {
    let mut a: Vec<String> = [
        "run",
        "--rm",
        "--pull",
        "missing",
        "--network",
        "none",
        "--gpus",
        "all",
        "--env",
        "NVIDIA_DRIVER_CAPABILITIES=all",
    ]
    .map(String::from)
    .into();
    if wsl_libs {
        a.extend(wsl_libs_args());
    }
    a.extend(["--entrypoint", "sh", image, "-c", SCRIPT].map(String::from));
    a
}

/// Runs the probe in `image` with every NVIDIA driver capability, and
/// with [`WSL_LIBS`] when `wsl_libs` (as graphics tasks get it).
pub async fn probe(docker: &str, image: &str, wsl_libs: bool) -> Result<DockerGpuCheck> {
    let out = tokio::time::timeout(
        Duration::from_secs(15 * 60),
        tokio::process::Command::new(docker)
            .args(probe_args(image, wsl_libs))
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .context("the probe container did not finish within 15 minutes")?
    .with_context(|| format!("could not run `{docker}`"))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() && !stdout.contains("cuda=") {
        bail!(
            "`{docker} run --gpus all {image}` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(DockerGpuCheck::parse(image, wsl_libs, &stdout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_host_with_graphics_libraries() {
        let c = DockerGpuCheck::parse(
            "ubuntu:24.04",
            false,
            "cuda=GPU 0: NVIDIA GeForce RTX 3080 (UUID: GPU-1);\negl_nvidia=yes\nglx_nvidia=yes\noptix=yes\nvulkan_nvidia=yes\nwsl_dxg=no\nwsl_d3d12=no\nmesa_d3d12=no\n",
        );
        assert_eq!(c.cuda, vec!["GPU 0: NVIDIA GeForce RTX 3080 (UUID: GPU-1)"]);
        assert!(c.graphics && c.nvidia_graphics && c.optix && !c.wsl);
    }

    #[test]
    fn wsl_and_renderers() {
        let wsl = "cuda=GPU 0: NVIDIA GeForce RTX 3080 (UUID: GPU-1);\negl_nvidia=no\nglx_nvidia=no\noptix=yes\nvulkan_nvidia=no\nwsl_dxg=yes\nwsl_d3d12=yes\nmesa_d3d12=no\n";
        let c = DockerGpuCheck::parse("ubuntu:24.04", false, wsl);
        assert!(c.wsl && c.graphics && !c.nvidia_graphics);
        // An image whose GL ends up on llvmpipe cannot draw on the GPU.
        let c = DockerGpuCheck::parse(
            "blender",
            true,
            &format!("{wsl}egl_renderer=llvmpipe (LLVM 17.0.6, 256 bits)\n"),
        );
        assert!(!c.graphics);
        let c = DockerGpuCheck::parse(
            "blender",
            true,
            &format!("{wsl}mesa_d3d12=yes\negl_renderer=D3D12 (NVIDIA GeForce RTX 3080)\n"),
        );
        assert!(c.graphics && c.mesa_d3d12);
        // Docker Desktop's OptiX library is there but does not start.
        assert!(!c.optix && c.notes.iter().any(|n| n.contains("OptiX does not start")));
    }

    #[test]
    fn docker_desktop_without_the_wsl_libraries() {
        // What Docker Desktop 4.80 gives a `--gpus` container: /dev/dxg and
        // CUDA, but no /usr/lib/wsl/lib.
        let out = "cuda=GPU 0: NVIDIA GeForce RTX 3080 (UUID: GPU-1);\negl_nvidia=no\nglx_nvidia=no\noptix=no\nvulkan_nvidia=no\nwsl_dxg=yes\nwsl_d3d12=no\nmesa_d3d12=yes\negl_renderer=llvmpipe (LLVM 20.1.2, 256 bits)\n";
        let c = DockerGpuCheck::parse("blender", false, out);
        assert!(!c.graphics && !c.wsl);
        assert!(
            c.notes.iter().any(|n| n.contains("--docker-wsl-libs yes")),
            "{:?}",
            c.notes
        );
        let c = DockerGpuCheck::parse("blender", true, out);
        assert!(
            c.notes.iter().any(|n| n.contains("no such folder")),
            "{:?}",
            c.notes
        );
    }

    #[test]
    fn the_probe_borrows_the_wsl_libraries_only_when_asked() {
        let a = probe_args("blender", false).join(" ");
        assert!(!a.contains("/usr/lib/wsl:"));
        let a = probe_args("blender", true).join(" ");
        assert!(
            a.contains("--volume /usr/lib/wsl:/usr/lib/wsl:ro --entrypoint sh blender -c"),
            "{a}"
        );
    }

    #[test]
    fn wsl_vulkan_on_the_cpu_is_told_apart_from_gl() {
        // Docker Desktop with /usr/lib/wsl lent: GL through D3D12, Vulkan on
        // lavapipe/llvmpipe (Mesa has no Vulkan driver onto D3D12 here).
        let out = "cuda=GPU 0: NVIDIA GeForce RTX 3080 (UUID: GPU-1);\negl_nvidia=no\nglx_nvidia=no\noptix=no\nvulkan_nvidia=no\nwsl_dxg=yes\nwsl_d3d12=yes\nmesa_d3d12=yes\negl_renderer=D3D12 (NVIDIA GeForce RTX 3080)\nvulkan_device=llvmpipe (LLVM 20.1.2, 256 bits)\n";
        let c = DockerGpuCheck::parse("blender", true, out);
        assert!(c.graphics);
        assert_eq!(c.vulkan_on_gpu(), Some(false));
        assert!(
            c.notes
                .iter()
                .any(|n| n.contains("Vulkan draws on the CPU")),
            "{:?}",
            c.notes
        );
        let nvidia = "cuda=GPU 0: NVIDIA L4 (UUID: GPU-1);\negl_nvidia=yes\nvulkan_nvidia=yes\n";
        let c = DockerGpuCheck::parse("blender", false, nvidia);
        assert_eq!(c.vulkan_on_gpu(), Some(true));
        assert!(
            !c.notes
                .iter()
                .any(|n| n.contains("Vulkan draws on the CPU"))
        );
    }

    #[test]
    fn no_gpu_in_the_container() {
        let c = DockerGpuCheck::parse("ubuntu:24.04", false, "cuda=\negl_nvidia=no\n");
        assert!(c.cuda.is_empty() && !c.graphics);
        assert!(c.notes[0].contains("no GPU inside the container"));
    }
}
