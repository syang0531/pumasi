//! Confidential VMs on this host (phase 16): which TEEs work here, and
//! the firmware and kernel a confidential guest boots.
//!
//! A SEV-SNP or TDX guest boots with QEMU and KVM from OVMF built for it,
//! which measures the kernel, initramfs (with the guest init) and command
//! line before it boots them. The kernel needs the SEV-SNP or TDX guest
//! drivers and `CONFIG_TSM_REPORTS` (Linux 6.7+). They live in the VM
//! directory's `tee/` subdirectory:
//!
//! - `tee/vmlinuz`: the guest kernel (bzImage),
//! - `tee/OVMF-snp.fd`: OVMF's `AmdSevX64` build, for SEV-SNP,
//! - `tee/OVMF-tdx.fd`: OVMF's TDX build (`IntelTdxX64` or `OvmfX64` with
//!   TDX), for TDX.

use std::path::{Path, PathBuf};

use pumasi_proto::confidential::{TeeKind, TeeReport};

pub const TEE_DIR: &str = "tee";
pub const TEE_KERNEL_FILE: &str = "vmlinuz";
pub const SNP_FIRMWARE_FILE: &str = "OVMF-snp.fd";
pub const TDX_FIRMWARE_FILE: &str = "OVMF-tdx.fd";

/// What the node's owner asked for.
#[derive(Debug, Clone, Default)]
pub struct TeeOptions {
    /// Offer SEV-SNP and TDX VMs where the host has them
    /// (`--allow-confidential`).
    pub enabled: bool,
    /// Offer the test stand-in (`--insecure-test-tee`): an ordinary VM
    /// whose guest attests with no hardware. A control plane accepts it
    /// only when started with the same flag. Tests only.
    pub test: bool,
}

/// What a confidential guest of one kind boots.
#[derive(Debug, Clone)]
pub struct TeeBoot {
    pub firmware: PathBuf,
    pub kernel: PathBuf,
}

/// The TEEs this host starts confidential VMs in.
#[derive(Debug, Clone, Default)]
pub struct TeeAssets {
    pub snp: Option<TeeBoot>,
    pub tdx: Option<TeeBoot>,
    pub test: bool,
    /// Why none, when the owner asked.
    pub unavailable: Option<String>,
}

impl TeeAssets {
    /// Looks at the host: KVM's SEV-SNP or TDX support, QEMU, and the
    /// firmware and kernel in `dir/tee`.
    pub fn detect(opts: &TeeOptions, dir: &Path, qemu: bool, kvm: bool) -> Self {
        let mut a = TeeAssets {
            test: opts.test,
            ..Default::default()
        };
        if !opts.enabled {
            return a;
        }
        let tee = dir.join(TEE_DIR);
        let boot = |firmware: &str| -> Result<TeeBoot, String> {
            let kernel = tee.join(TEE_KERNEL_FILE);
            let firmware = tee.join(firmware);
            for p in [&kernel, &firmware] {
                if !p.is_file() {
                    return Err(format!("no {}", p.display()));
                }
            }
            Ok(TeeBoot { firmware, kernel })
        };
        let mut why = Vec::new();
        if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            why.push("confidential VMs need Linux on x86-64".to_string());
        } else if !kvm {
            why.push("no usable /dev/kvm".to_string());
        } else if !qemu {
            why.push("QEMU is not installed".to_string());
        } else {
            if kvm_param("kvm_amd", "sev_snp") {
                match boot(SNP_FIRMWARE_FILE) {
                    Ok(b) => a.snp = Some(b),
                    Err(e) => why.push(format!("SEV-SNP: {e}")),
                }
            }
            if kvm_param("kvm_intel", "tdx") {
                match boot(TDX_FIRMWARE_FILE) {
                    Ok(b) => a.tdx = Some(b),
                    Err(e) => why.push(format!("TDX: {e}")),
                }
            }
            if a.snp.is_none() && a.tdx.is_none() && why.is_empty() {
                why.push(
                    "KVM has neither SEV-SNP (kvm_amd sev_snp) nor TDX (kvm_intel tdx) on; check the BIOS and the host kernel"
                        .into(),
                );
            }
        }
        if a.snp.is_none() && a.tdx.is_none() {
            a.unavailable = Some(why.join("; "));
        }
        a
    }

    pub fn report(&self) -> TeeReport {
        let mut tees = Vec::new();
        if self.snp.is_some() {
            tees.push(TeeKind::SevSnp);
        }
        if self.tdx.is_some() {
            tees.push(TeeKind::Tdx);
        }
        if self.test {
            tees.push(TeeKind::Test);
        }
        TeeReport {
            tees,
            unavailable: self.unavailable.clone(),
        }
    }
}

/// Whether a KVM module parameter is on (`Y` or `1`).
fn kvm_param(module: &str, param: &str) -> bool {
    std::fs::read_to_string(format!("/sys/module/{module}/parameters/{param}"))
        .is_ok_and(|v| matches!(v.trim(), "Y" | "y" | "1"))
}

/// The memory encryption bit's position (CPUID 0x8000001F, EBX[5:0]).
pub fn cbitpos() -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        let max = std::arch::x86_64::__cpuid(0x8000_0000).eax;
        if max >= 0x8000_001f {
            let r = std::arch::x86_64::__cpuid(0x8000_001f);
            let bit = r.ebx & 0x3f;
            if bit != 0 {
                return bit;
            }
        }
    }
    51
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_unless_asked() {
        let dir = Path::new("/nonexistent");
        let a = TeeAssets::detect(&TeeOptions::default(), dir, true, true);
        assert!(a.report().tees.is_empty());
        assert!(a.unavailable.is_none());
        let a = TeeAssets::detect(
            &TeeOptions {
                enabled: false,
                test: true,
            },
            dir,
            false,
            false,
        );
        assert_eq!(a.report().tees, vec![TeeKind::Test]);
        let a = TeeAssets::detect(
            &TeeOptions {
                enabled: true,
                test: false,
            },
            dir,
            true,
            false,
        );
        assert!(a.report().tees.is_empty());
        assert!(a.unavailable.is_some());
    }
}
