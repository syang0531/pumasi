//! Virtual machine monitors the microVM executor can drive, and the pieces
//! every boot needs (initramfs, kernel command line).

use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use serde::Serialize;

/// Which VMM boots the VMs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Firecracker (Linux with KVM). The default where it is installed.
    Firecracker,
    /// QEMU's `microvm` machine (x86_64) or `virt` machine (aarch64), with
    /// KVM or HVF when available and software emulation otherwise.
    Qemu,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Backend::Firecracker => "firecracker",
            Backend::Qemu => "qemu",
        })
    }
}

impl FromStr for Backend {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "firecracker" => Ok(Backend::Firecracker),
            "qemu" => Ok(Backend::Qemu),
            _ => bail!("unknown VM backend {s:?} (expected firecracker or qemu)"),
        }
    }
}

/// One virtio block device.
#[derive(Debug, Clone)]
pub struct Disk {
    pub path: PathBuf,
    pub read_only: bool,
}

/// Everything needed to boot one VM.
#[derive(Debug, Clone)]
pub struct VmSpec {
    pub kernel: PathBuf,
    pub initrd: PathBuf,
    /// `build` or `run`, passed to the guest as `pumasi.mode`.
    pub mode: &'static str,
    pub disks: Vec<Disk>,
    pub vcpus: u32,
    pub mem_mib: u64,
}

#[derive(Debug, Clone)]
pub struct Vmm {
    pub backend: Backend,
    pub bin: PathBuf,
    /// QEMU only: the host has KVM (or HVF) for it.
    pub accelerated: bool,
}

impl Vmm {
    /// The VMM command for `spec`. Writes any config file into `dir`.
    pub fn command(&self, spec: &VmSpec, dir: &Path) -> Result<tokio::process::Command> {
        match self.backend {
            Backend::Firecracker => self.firecracker(spec, dir),
            Backend::Qemu => Ok(self.qemu(spec)),
        }
    }

    fn firecracker(&self, spec: &VmSpec, dir: &Path) -> Result<tokio::process::Command> {
        #[derive(Serialize)]
        struct BootSource {
            kernel_image_path: PathBuf,
            initrd_path: PathBuf,
            boot_args: String,
        }
        #[derive(Serialize)]
        struct Drive {
            drive_id: String,
            path_on_host: PathBuf,
            is_root_device: bool,
            is_read_only: bool,
        }
        #[derive(Serialize)]
        struct Machine {
            vcpu_count: u32,
            mem_size_mib: u64,
            smt: bool,
        }
        #[derive(Serialize)]
        struct Config {
            #[serde(rename = "boot-source")]
            boot_source: BootSource,
            drives: Vec<Drive>,
            #[serde(rename = "machine-config")]
            machine: Machine,
        }
        let mut args = vec!["console=ttyS0", "reboot=k", "panic=1"];
        if cfg!(target_arch = "x86_64") {
            args.extend(["pci=off", "i8042.noaux", "i8042.nomux", "i8042.dumbkbd"]);
        }
        let config = Config {
            boot_source: BootSource {
                kernel_image_path: spec.kernel.clone(),
                initrd_path: spec.initrd.clone(),
                boot_args: cmdline(&args, spec.mode),
            },
            drives: spec
                .disks
                .iter()
                .enumerate()
                .map(|(i, d)| Drive {
                    drive_id: format!("d{i}"),
                    path_on_host: d.path.clone(),
                    is_root_device: false,
                    is_read_only: d.read_only,
                })
                .collect(),
            machine: Machine {
                vcpu_count: spec.vcpus,
                mem_size_mib: spec.mem_mib,
                smt: false,
            },
        };
        let path = dir.join("firecracker.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&config)?)
            .with_context(|| format!("write {}", path.display()))?;
        let mut cmd = tokio::process::Command::new(&self.bin);
        cmd.arg("--no-api")
            .arg("--config-file")
            .arg(&path)
            .args(["--level", "Warning"]);
        Ok(cmd)
    }

    fn qemu(&self, spec: &VmSpec) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.bin);
        let x86 = cfg!(target_arch = "x86_64");
        if x86 && self.accelerated && cfg!(target_os = "linux") {
            // KVM's in-kernel 8259 is never programmed by this kernel (no
            // legacy PIC in its ACPI tables), so the PIT's first tick
            // arrives as vector 0 and panics the boot as a divide error.
            // The local APIC timer and kvm-clock are all it needs.
            cmd.args(["-M", "microvm,pic=off,pit=off"]);
        } else if x86 {
            cmd.args(["-M", "microvm"]);
        } else {
            cmd.args(["-M", "virt"]);
        }
        let accel = if cfg!(target_os = "macos") {
            "hvf"
        } else {
            "kvm"
        };
        if self.accelerated {
            cmd.args(["-accel", accel]);
        }
        cmd.args(["-accel", "tcg", "-cpu", "max"])
            .args(["-m", &spec.mem_mib.to_string()])
            .args(["-smp", &spec.vcpus.to_string()])
            .args(["-nodefaults", "-no-user-config", "-no-reboot"])
            .args(["-display", "none", "-serial", "stdio"])
            .arg("-kernel")
            .arg(&spec.kernel)
            .arg("-initrd")
            .arg(&spec.initrd);
        let console = if x86 {
            "console=ttyS0"
        } else {
            "console=ttyAMA0"
        };
        let mut args = vec![console, "reboot=t", "panic=1"];
        if x86 && !self.accelerated {
            // Without KVM there is no kvm-clock, and calibrating the TSC
            // against the emulated timers sometimes hangs the boot.
            args.push("tsc_early_khz=2000000");
        }
        cmd.arg("-append").arg(cmdline(&args, spec.mode));
        for (i, d) in spec.disks.iter().enumerate() {
            // QEMU wants commas in file names doubled.
            let file = d.path.display().to_string().replace(',', ",,");
            let ro = if d.read_only { "on" } else { "off" };
            cmd.arg("-drive")
                .arg(format!(
                    "id=d{i},file={file},format=raw,if=none,readonly={ro}"
                ))
                .arg("-device")
                .arg(format!("virtio-blk-device,drive=d{i}"));
        }
        cmd
    }
}

fn cmdline(base: &[&str], mode: &str) -> String {
    let mut parts: Vec<String> = base.iter().map(|s| s.to_string()).collect();
    parts.extend(
        ["quiet", "loglevel=3", "random.trust_cpu=on", "rdinit=/init"]
            .iter()
            .map(|s| s.to_string()),
    );
    parts.push(format!("{}={mode}", pumasi_guest::MODE_PARAM));
    // Debugging aid, e.g. PUMASI_VM_KERNEL_ARGS="loglevel=7".
    if let Ok(extra) = std::env::var("PUMASI_VM_KERNEL_ARGS") {
        parts.extend(extra.split_whitespace().map(String::from));
    }
    parts.join(" ")
}

/// Looks for a program in `dir`, then on `PATH`, then in `extra` dirs.
pub fn find_program(name: &str, dir: Option<&Path>, extra: &[&str]) -> Option<PathBuf> {
    let exe = |p: PathBuf| p.is_file().then_some(p);
    if let Some(found) = dir.and_then(|d| exe(d.join(name))) {
        return Some(found);
    }
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain(extra.iter().map(PathBuf::from))
        .find_map(|d| exe(d.join(name)))
}

/// QEMU's system emulator for this machine's architecture.
pub fn qemu_name() -> String {
    format!("qemu-system-{}", std::env::consts::ARCH)
}

/// True when this process may open `/dev/kvm`.
pub fn kvm_usable() -> bool {
    cfg!(target_os = "linux")
        && std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
            .is_ok()
}

/// A newc cpio archive holding the guest init as `/init`, the directories
/// it mounts onto, and `/dev/console` (so the kernel gives init a console).
pub fn initramfs(init: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut ino = 1;
    let mut add = |name: &str, mode: u32, rdev: (u32, u32), data: &[u8]| {
        let header = format!(
            "070701{ino:08x}{mode:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}",
            0,
            0,
            1,
            0,
            data.len(),
            0,
            0,
            rdev.0,
            rdev.1,
            name.len() + 1,
            0
        );
        ino += 1;
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(name.as_bytes());
        out.push(0);
        while out.len() % 4 != 0 {
            out.push(0);
        }
        out.extend_from_slice(data);
        while out.len() % 4 != 0 {
            out.push(0);
        }
    };
    for dir in ["dev", "proc", "sys", "newroot", "lower", "scratch"] {
        add(dir, 0o040755, (0, 0), &[]);
    }
    add("dev/console", 0o020600, (5, 1), &[]);
    add("init", 0o100755, (0, 0), init);
    add("TRAILER!!!", 0, (0, 0), &[]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initramfs_layout() {
        let cpio = initramfs(b"\x7fELF-fake");
        assert!(cpio.starts_with(b"070701"));
        assert_eq!(cpio.len() % 4, 0);
        let text = String::from_utf8_lossy(&cpio);
        for name in ["dev/console", "init", "newroot", "TRAILER!!!"] {
            assert!(text.contains(name), "{name} missing");
        }
    }

    #[test]
    fn qemu_command_line() {
        let vmm = Vmm {
            backend: Backend::Qemu,
            bin: "qemu".into(),
            accelerated: false,
        };
        let spec = VmSpec {
            kernel: "/k".into(),
            initrd: "/i".into(),
            mode: "run",
            disks: vec![Disk {
                path: "/a,b.img".into(),
                read_only: true,
            }],
            vcpus: 2,
            mem_mib: 300,
        };
        let cmd = vmm.command(&spec, Path::new("/tmp")).unwrap();
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let joined = args.join(" ");
        assert!(joined.contains("-no-reboot"));
        assert!(joined.contains("file=/a,,b.img,format=raw,if=none,readonly=on"));
        assert!(joined.contains("pumasi.mode=run"));
        assert!(joined.contains("-smp 2"));
        assert!(!joined.contains("-accel kvm"));
    }
}
