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
    /// Apple's Virtualization.framework (macOS 11+), run in the signed
    /// helper. The default on macOS.
    Vz,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Backend::Firecracker => "firecracker",
            Backend::Qemu => "qemu",
            Backend::Vz => "vz",
        })
    }
}

impl FromStr for Backend {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "firecracker" => Ok(Backend::Firecracker),
            "qemu" => Ok(Backend::Qemu),
            "vz" => Ok(Backend::Vz),
            _ => bail!("unknown VM backend {s:?} (expected firecracker, qemu or vz)"),
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
    /// QEMU only: the host has KVM, HVF or WHPX for it.
    pub accelerated: bool,
    /// The host's TSC rate, passed to accelerated guests that have no
    /// paravirtual clock (WHPX), so they need not calibrate it.
    pub tsc_khz: Option<u64>,
}

/// QEMU's hardware accelerator on this OS.
pub fn qemu_accel() -> &'static str {
    if cfg!(target_os = "macos") {
        "hvf"
    } else if cfg!(windows) {
        // The in-kernel (Hyper-V) local APIC trips up Linux guests on some
        // Windows builds; QEMU's own interrupt controllers always work.
        "whpx,kernel-irqchip=off"
    } else {
        "kvm"
    }
}

/// Where QEMU writes the guest console when it cannot use its stdout.
pub const SERIAL_LOG: &str = "serial.log";

impl Vmm {
    /// The VMM command for `spec`. Writes any config file into `dir`.
    pub fn command(&self, spec: &VmSpec, dir: &Path) -> Result<tokio::process::Command> {
        match self.backend {
            Backend::Firecracker => self.firecracker(spec, dir),
            Backend::Qemu => Ok(self.qemu(spec, dir)),
            Backend::Vz => self.vz(spec, dir),
        }
    }

    /// The signed helper with a JSON config: `pumasi-vz __vz-run vz.json`.
    fn vz(&self, spec: &VmSpec, dir: &Path) -> Result<tokio::process::Command> {
        let config = super::vz::VzConfig {
            kernel: spec.kernel.clone(),
            initrd: spec.initrd.clone(),
            // The console is virtio (hvc0); a guest reboot stops the VM.
            cmdline: cmdline(&["console=hvc0", "panic=1"], spec.mode),
            disks: spec
                .disks
                .iter()
                .map(|d| (d.path.clone(), d.read_only))
                .collect(),
            cpus: spec.vcpus,
            mem_mib: spec.mem_mib,
        };
        let path = dir.join("vz.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&config)?)
            .with_context(|| format!("write {}", path.display()))?;
        let mut cmd = tokio::process::Command::new(&self.bin);
        cmd.arg(super::vz::RUN_ARG).arg(&path);
        Ok(cmd)
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

    fn qemu(&self, spec: &VmSpec, dir: &Path) -> tokio::process::Command {
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
        // Escape hatches for trying other settings on a new machine, e.g.
        // PUMASI_QEMU_ACCEL=tcg or PUMASI_QEMU_ACCEL=whpx.
        let accel = std::env::var("PUMASI_QEMU_ACCEL").ok();
        let whpx = cfg!(windows)
            && match &accel {
                Some(a) => a.starts_with("whpx"),
                None => self.accelerated,
            };
        match &accel {
            Some(a) => {
                cmd.args(["-accel", a]);
            }
            None => {
                if self.accelerated {
                    cmd.args(["-accel", qemu_accel()]);
                }
                cmd.args(["-accel", "tcg"]);
            }
        }
        // WHPX takes CPUID leaf 1 from QEMU's CPU model, and `max` there is
        // TCG's feature list; Nehalem is the x86-64-v2 baseline every
        // machine with WHPX has.
        let cpu = std::env::var("PUMASI_QEMU_CPU")
            .unwrap_or_else(|_| if whpx { "Nehalem" } else { "max" }.into());
        cmd.args(["-cpu", &cpu])
            .args(["-m", &spec.mem_mib.to_string()])
            .args(["-smp", &spec.vcpus.to_string()])
            .args(["-nodefaults", "-no-user-config", "-no-reboot"])
            .args(["-display", "none"]);
        if cfg!(windows) {
            // QEMU's stdio console polls the console's input handle, which
            // a service or a redirected NUL stdin does not have.
            cmd.arg("-serial")
                .arg(format!("file:{}", dir.join(SERIAL_LOG).display()));
        } else {
            cmd.args(["-serial", "stdio"]);
        }
        cmd.arg("-kernel")
            .arg(&spec.kernel)
            .arg("-initrd")
            .arg(&spec.initrd);
        let console = if x86 {
            "console=ttyS0"
        } else {
            "console=ttyAMA0"
        };
        // The guest ends the VM by rebooting, which `-no-reboot` turns into
        // QEMU exiting. A triple fault (`reboot=t`) does that everywhere but
        // under WHPX, which reports it as an unrecoverable exception and
        // leaves QEMU hanging; the microvm machine's ACPI reset register
        // (`reboot=a`) is a clean reset request there.
        let reboot = if whpx { "reboot=a" } else { "reboot=t" };
        let mut args = vec![console.to_string(), reboot.into(), "panic=1".into()];
        if x86 && (!self.accelerated || whpx) {
            // Without KVM there is no kvm-clock, and calibrating the TSC
            // against the emulated timers sometimes hangs the boot. Under
            // WHPX the guest reads the host's TSC, whose rate we measured.
            let khz = self.tsc_khz.filter(|_| whpx).unwrap_or(2_000_000);
            args.push(format!("tsc_early_khz={khz}"));
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
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
        // More QEMU arguments, for trying settings on a new machine.
        if let Ok(extra) = std::env::var("PUMASI_QEMU_ARGS") {
            cmd.args(extra.split_whitespace());
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
    let name = format!("{name}{}", std::env::consts::EXE_SUFFIX);
    let name = name.as_str();
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
            tsc_khz: None,
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

    #[test]
    fn vz_command_writes_its_config() {
        let dir = crate::tempdir::TempDir::new("vz").unwrap();
        let vmm = Vmm {
            backend: Backend::Vz,
            bin: "/vm/pumasi-vz".into(),
            accelerated: true,
            tsc_khz: None,
        };
        let spec = VmSpec {
            kernel: "/k".into(),
            initrd: "/i".into(),
            mode: "run",
            disks: vec![Disk {
                path: "/a.img".into(),
                read_only: true,
            }],
            vcpus: 2,
            mem_mib: 300,
        };
        let cmd = vmm.command(&spec, dir.path()).unwrap();
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args[0], "__vz-run");
        let config: super::super::vz::VzConfig =
            serde_json::from_slice(&std::fs::read(&args[1]).unwrap()).unwrap();
        assert!(config.cmdline.contains("console=hvc0"));
        assert!(config.cmdline.contains("pumasi.mode=run"));
        assert_eq!(config.disks, vec![("/a.img".into(), true)]);
        assert_eq!("vz".parse::<Backend>().unwrap(), Backend::Vz);
    }
}
