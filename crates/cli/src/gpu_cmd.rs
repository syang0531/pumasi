//! `pumasi gpu`: what GPUs this machine has and what the agent could do
//! with them.

use std::time::Instant;

use anyhow::Result;
use clap::Subcommand;
use pumasi_agent::gpu::{self, GpuEnv, vfio};
use pumasi_proto::gpu::{GpuChallenge, GpuInfo, GpuVendor, check_answer, target_for};

#[derive(Subcommand)]
pub enum GpuCmd {
    /// List this machine's GPUs, benchmarked the way the agent does.
    List {
        /// Skip the benchmark (faster; FP32 is then an estimate).
        #[arg(long)]
        no_bench: bool,
        /// Print the list as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Check each GPU: can Docker jobs use it, can microVMs get it
    /// (VFIO passthrough, with the commands to set it up), and does it
    /// answer the cluster's GPU check correctly.
    Check {
        /// Only this GPU: its index from `pumasi gpu list` or its PCI
        /// address.
        gpu: Option<String>,
        /// Docker binary to ask about NVIDIA Container Toolkit support.
        #[arg(long, default_value = "docker")]
        docker: String,
        /// Also start a container with the GPUs to see what Docker GPU
        /// tasks can do here: CUDA, OptiX, and drawing (EGL, OpenGL,
        /// Vulkan). Saves the answer for the agent, which then offers
        /// graphics to jobs that ask for it (label `gpu.graphics`).
        #[arg(long = "docker-run")]
        docker_run: bool,
        /// The image to probe with --docker-run (it needs `sh`). One with
        /// `eglinfo` or `vulkaninfo`, such as the examples/blender-render
        /// image, also shows which renderer drawing gets.
        #[arg(long, requires = "docker_run", default_value = pumasi_agent::gpu::docker_check::DEFAULT_PROBE_IMAGE)]
        image: String,
        /// Whether the probe gets the WSL 2 driver libraries
        /// (`/usr/lib/wsl`, read-only) the way graphics tasks do: `auto`
        /// does on Windows. Match the agent's `--docker-wsl-libs`.
        #[arg(long, requires = "docker_run", value_enum, default_value = "auto")]
        wsl_libs: crate::GraphicsArg,
    },
    /// List the WebGPU adapters (Vulkan, Metal, DX12, OpenGL) that wasm
    /// jobs with `gpus = 1` could use here, benchmarked the way the agent
    /// does (`pumasi agent --allow-webgpu` offers them).
    Webgpu {
        /// Include software rasterizers (lavapipe, llvmpipe, WARP).
        #[arg(long)]
        software: bool,
        /// Skip the benchmark.
        #[arg(long)]
        no_bench: bool,
        /// Print the list as JSON.
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(home: &std::path::Path, cmd: GpuCmd) -> Result<i32> {
    match cmd {
        GpuCmd::List { no_bench, json } => {
            let mut notes = Vec::new();
            let env = GpuEnv {
                bench: !no_bench,
                ..GpuEnv::default()
            };
            let gpus = tokio::task::spawn_blocking(move || {
                let g = gpu::detect(&env, &mut notes);
                (g, notes)
            })
            .await?;
            let (gpus, notes) = gpus;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "gpus": gpus,
                        "notes": notes,
                    }))?
                );
                return Ok(0);
            }
            if gpus.is_empty() {
                println!("no GPUs found");
            }
            for g in &gpus {
                print_gpu(g);
            }
            for n in &notes {
                println!("note: {n}");
            }
            Ok(0)
        }
        GpuCmd::Check {
            gpu: only,
            docker,
            docker_run,
            image,
            wsl_libs,
        } => {
            let code = check(only, &docker).await?;
            if docker_run {
                let wsl_libs = wsl_libs
                    .forced()
                    .unwrap_or_else(pumasi_agent::gpu::docker_check::wsl_libs_auto);
                return docker_probe(home, &docker, &image, wsl_libs).await;
            }
            Ok(code)
        }
        GpuCmd::Webgpu {
            software,
            no_bench,
            json,
        } => webgpu(software, !no_bench, json).await,
    }
}

async fn webgpu(software: bool, bench: bool, json: bool) -> Result<i32> {
    let (adapters, notes) = tokio::task::spawn_blocking(move || {
        let mut notes = Vec::new();
        let gpus = gpu::detect(&GpuEnv::default(), &mut notes);
        let w = pumasi_agent::webgpu::WebGpu::detect(
            pumasi_agent::webgpu::Options { software, bench },
            &gpus,
            &mut notes,
        );
        (w.adapters(), notes)
    })
    .await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "adapters": adapters,
                "notes": notes,
            }))?
        );
        return Ok(0);
    }
    if adapters.is_empty() {
        println!(
            "no WebGPU adapter can run compute shaders here{}",
            if software {
                ""
            } else {
                " (--software includes software rasterizers)"
            }
        );
    }
    for a in &adapters {
        println!("WebGPU {} {}", a.index, a.describe());
        let mut facts = vec![format!("vendor {}", a.vendor)];
        if let Some(g) = a.gpu {
            facts.push(format!("GPU {g} of `pumasi gpu list`"));
        }
        if !a.pci.is_empty() {
            facts.push(format!("pci {}", a.pci));
        }
        if !a.driver.is_empty() {
            facts.push(format!("driver {}", a.driver));
        }
        facts.push(format!(
            "buffers up to {} MiB (bindings {} MiB)",
            a.max_buffer_mib, a.max_binding_mib
        ));
        println!("  {}", facts.join(", "));
    }
    for n in &notes {
        println!("note: {n}");
    }
    Ok(0)
}

fn print_gpu(g: &GpuInfo) {
    println!("GPU {} {}", g.index, g.describe());
    let mut facts = vec![format!("vendor {}", g.vendor)];
    if !g.pci.is_empty() {
        facts.push(format!("pci {}", g.pci));
    }
    if !g.driver.is_empty() {
        facts.push(format!("driver {}", g.driver));
    }
    if !g.compute.is_empty() {
        facts.push(format!("compute {}", g.compute));
    }
    if g.mem_bw_gb_s > 0 {
        facts.push(format!("{} GB/s", g.mem_bw_gb_s));
    }
    if g.hash_rate > 0 {
        facts.push(format!("{:.2} Ghash/s", g.hash_rate as f64 / 1e9));
    }
    if g.display {
        facts.push("drives a display".into());
    }
    println!("  {}", facts.join(", "));
}

async fn check(only: Option<String>, docker: &str) -> Result<i32> {
    let has_docker = pumasi_agent::microvm::vmm::find_program(docker, None, &[]).is_some();
    let docker_nvidia = has_docker && gpu::docker_nvidia(docker).await;
    let env = GpuEnv {
        docker_nvidia,
        docker_amd: has_docker && cfg!(target_os = "linux"),
        vfio: cfg!(target_os = "linux"),
        bench: true,
    };
    let (gpus, notes) = tokio::task::spawn_blocking(move || {
        let mut notes = Vec::new();
        let g = gpu::detect(&env, &mut notes);
        (g, notes)
    })
    .await?;
    let selected: Vec<&GpuInfo> = gpus
        .iter()
        .filter(|g| match &only {
            None => true,
            Some(s) => s.parse::<u32>().ok() == Some(g.index) || g.pci == s.to_ascii_lowercase(),
        })
        .collect();
    if selected.is_empty() {
        println!(
            "no GPUs found{}",
            if only.is_some() { " that match" } else { "" }
        );
        for n in &notes {
            println!("note: {n}");
        }
        return Ok(1);
    }
    let mut usable = 0;
    for g in selected {
        print_gpu(g);
        // Docker.
        match g.vendor {
            GpuVendor::Nvidia if g.docker => println!("  docker: ok (NVIDIA Container Toolkit)"),
            GpuVendor::Nvidia if !has_docker => println!("  docker: no `{docker}` found"),
            GpuVendor::Nvidia if g.driver == "vfio-pci" => {
                println!("  docker: no, the GPU is bound to vfio-pci (kept for microVMs)")
            }
            GpuVendor::Nvidia => println!(
                "  docker: no, install the NVIDIA Container Toolkit (nvidia-ctk runtime configure --runtime=docker)"
            ),
            GpuVendor::Amd if g.docker => {
                println!("  docker: ok (/dev/kfd and {})", g.render_node)
            }
            GpuVendor::Amd => {
                println!("  docker: no, needs the amdgpu driver with /dev/kfd and a render node")
            }
            _ => println!("  docker: no, only NVIDIA and AMD GPUs can be used by jobs"),
        }
        // MicroVM passthrough.
        if cfg!(target_os = "linux") && !g.pci.is_empty() {
            match vfio::ready(&g.pci) {
                Ok(()) if g.display => {
                    println!("  microvm: no, it drives a display")
                }
                Ok(()) => println!(
                    "  microvm: ready for passthrough (IOMMU group {})",
                    vfio::iommu_group(&g.pci).unwrap_or_default()
                ),
                Err(why) => {
                    println!("  microvm: not ready: {why}");
                    if !g.display && matches!(g.vendor, GpuVendor::Nvidia) {
                        println!("    to dedicate it to microVMs (it leaves the host driver):");
                        for c in vfio::bind_commands(&g.pci) {
                            println!("      {c}");
                        }
                    }
                }
            }
        } else {
            println!("  microvm: passthrough needs Linux with an IOMMU");
        }
        // The cluster's GPU check, run locally.
        if g.vendor == GpuVendor::Nvidia && g.driver != "vfio-pci" && g.hash_rate > 0 {
            let challenge = GpuChallenge {
                nonce: 0,
                device: g.index,
                seed: rand_seed(),
                // About half a second of work at the measured rate.
                target: target_for(g.hash_rate / 2, pumasi_proto::gpu::GPU_CHALLENGE_SOLUTIONS),
                solutions: pumasi_proto::gpu::GPU_CHALLENGE_SOLUTIONS,
            };
            let pci = g.pci.clone();
            let c = challenge;
            let start = Instant::now();
            let xs = tokio::task::spawn_blocking(move || gpu::solve(&pci, &c)).await?;
            let took = start.elapsed();
            if check_answer(&challenge, &xs) {
                println!("  gpu check: ok ({} ms)", took.as_millis());
            } else {
                println!("  gpu check: FAILED (wrong or missing answer)");
            }
        } else if g.vendor == GpuVendor::Nvidia && g.driver != "vfio-pci" {
            println!(
                "  gpu check: cannot run (no CUDA driver); the cluster bills it as unconfirmed"
            );
        }
        if g.usable() {
            usable += 1;
        }
    }
    for n in &notes {
        println!("note: {n}");
    }
    println!();
    println!(
        "{usable} GPU(s) usable by jobs here (the agent offers them unless started with --no-gpus)"
    );
    Ok(0)
}

/// `pumasi gpu check --docker-run`: what Docker GPU tasks can do here.
async fn docker_probe(
    home: &std::path::Path,
    docker: &str,
    image: &str,
    wsl_libs: bool,
) -> Result<i32> {
    use pumasi_agent::gpu::docker_check;
    println!();
    if wsl_libs {
        println!(
            "docker GPU tasks ({image}, graphics tasks with {} read-only):",
            docker_check::WSL_LIBS
        );
    } else {
        println!("docker GPU tasks ({image}):");
    }
    let c = match docker_check::probe(docker, image, wsl_libs).await {
        Ok(c) => c,
        Err(e) => {
            println!("  failed: {e:#}");
            return Ok(1);
        }
    };
    let yes_no = |b: bool| if b { "yes" } else { "no" };
    if c.cuda.is_empty() {
        println!("  CUDA: no GPU in the container");
    } else {
        println!("  CUDA: {}", c.cuda.join("; "));
    }
    println!("  OptiX: {}", yes_no(c.optix));
    let how = if c.nvidia_graphics {
        "NVIDIA EGL driver"
    } else if c.wsl {
        "WSL 2, through Mesa's D3D12 driver"
    } else {
        "no driver for drawing"
    };
    println!("  EGL/OpenGL: {} ({how})", yes_no(c.graphics));
    if let Some(r) = &c.egl_renderer {
        println!("  EGL renderer: {r}");
    }
    // Vulkan apart: on WSL 2 it draws on the CPU while GL uses the GPU.
    let device = c.vulkan_device.as_deref();
    match (c.vulkan_on_gpu(), device) {
        (Some(true), Some(d)) => println!("  Vulkan: yes ({d})"),
        (Some(true), None) => println!("  Vulkan: yes (NVIDIA Vulkan driver)"),
        (Some(false), Some(d)) => println!("  Vulkan: CPU only ({d})"),
        (_, Some(d)) => println!("  Vulkan: {d}"),
        (_, None) => println!("  Vulkan: not checked (no vulkaninfo in the image)"),
    }
    for n in &c.notes {
        println!("  note: {n}");
    }
    let dir = home.join("agent");
    c.save(&dir)?;
    println!();
    if c.graphics {
        println!(
            "saved: the agent offers graphics to Docker GPU jobs (label gpu.graphics=true) from its next check, within 30 s ({})",
            dir.join(docker_check::CHECK_FILE).display()
        );
    } else {
        println!(
            "saved: jobs that ask for graphics do not come here ({}); `pumasi agent start --gpu-graphics yes` overrides it",
            dir.join(docker_check::CHECK_FILE).display()
        );
    }
    Ok(if c.cuda.is_empty() { 1 } else { 0 })
}

fn rand_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.finish()
}
