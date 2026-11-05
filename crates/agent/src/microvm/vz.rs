//! The macOS backend: Apple's Virtualization.framework (macOS 11+, Intel
//! and Apple Silicon), which needs nothing installed.
//!
//! Two constraints shape it:
//!
//! - A process may use the framework only if its code signature carries
//!   the `com.apple.security.virtualization` entitlement. Ad-hoc signing is
//!   enough, so the agent keeps a copy of its own binary next to the VM
//!   files (`pumasi-vz`), signed with `/usr/bin/codesign`, and runs each VM
//!   in it like any other VMM process: `pumasi-vz __vz-run <config>`. The
//!   agent's own binary stays untouched (its hash is what upgrades
//!   compare), and killing the helper at a deadline ends the VM.
//! - macOS 10.15 has no Virtualization.framework, and the release binary
//!   must still start there, so the framework is loaded with `dlopen` and
//!   used through the Objective-C runtime instead of being linked.
//!
//! The VM gets the kernel and initramfs through `VZLinuxBootLoader`, one
//! virtio-blk device per disk, an entropy device, and the guest console
//! (`hvc0`) on the helper's stdout. Its devices are virtio-pci, so the
//! guest kernel differs from Firecracker's: Firecracker's 6.1 CI build with
//! PCI on Apple Silicon, and the same configuration as a bzImage on Intel,
//! which is what `VZLinuxBootLoader` boots there (see `setup`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// The hidden `pumasi` subcommand that runs one VM in the signed helper.
pub const RUN_ARG: &str = "__vz-run";
/// The signed copy of the agent binary in the VM directory.
pub const HELPER_FILE: &str = "pumasi-vz";
/// Names the binary the helper was copied from.
const HELPER_MARKER: &str = "pumasi-vz.source";
const ENTITLEMENTS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>com.apple.security.virtualization</key>
	<true/>
</dict>
</plist>
"#;

/// One VM, as the agent hands it to the helper (JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VzConfig {
    pub kernel: PathBuf,
    pub initrd: PathBuf,
    pub cmdline: String,
    /// Disk images and whether each is read-only.
    pub disks: Vec<(PathBuf, bool)>,
    pub cpus: u32,
    pub mem_mib: u64,
}

/// Makes sure `<dir>/pumasi-vz` is a signed copy of this binary, and
/// returns its path.
pub fn ensure_helper(dir: &Path) -> Result<PathBuf> {
    let exe = std::env::current_exe().context("find this binary")?;
    let bytes = std::fs::read(&exe).with_context(|| format!("read {}", exe.display()))?;
    let id = blake3::hash(&bytes).to_hex().to_string();
    let helper = dir.join(HELPER_FILE);
    let marker = dir.join(HELPER_MARKER);
    if helper.is_file() && std::fs::read_to_string(&marker).is_ok_and(|m| m.trim() == id) {
        return Ok(helper);
    }
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!("{HELPER_FILE}.tmp{}", std::process::id()));
    std::fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
    set_executable(&tmp)?;
    let plist = dir.join("pumasi-vz.entitlements");
    std::fs::write(&plist, ENTITLEMENTS)?;
    let out = std::process::Command::new("/usr/bin/codesign")
        .args(["--force", "--sign", "-", "--entitlements"])
        .arg(&plist)
        .arg(&tmp)
        .output()
        .context("run /usr/bin/codesign")?;
    if !out.status.success() {
        let _ = std::fs::remove_file(&tmp);
        bail!(
            "codesign could not sign the VM helper: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    std::fs::rename(&tmp, &helper)?;
    std::fs::write(&marker, &id)?;
    Ok(helper)
}

#[cfg(unix)]
fn set_executable(p: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_: &Path) -> Result<()> {
    Ok(())
}

/// Whether Virtualization.framework exists here (macOS 11+), whether or
/// not the hardware can use it.
pub fn framework_present() -> bool {
    #[cfg(target_os = "macos")]
    {
        imp::load().is_ok()
    }
    #[cfg(not(target_os = "macos"))]
    false
}

/// Whether this Mac can run Virtualization.framework VMs, or why not.
pub fn status() -> std::result::Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        imp::status()
    }
    #[cfg(not(target_os = "macos"))]
    Err("Virtualization.framework exists only on macOS".into())
}

/// Runs the VM in `config_path` until it stops, on this (the main)
/// thread. Returns the process exit code. Only the signed helper calls it.
pub fn run_main(config_path: &Path) -> i32 {
    let res = std::fs::read(config_path)
        .with_context(|| format!("read {}", config_path.display()))
        .and_then(|b| serde_json::from_slice::<VzConfig>(&b).context("parse the VM config"))
        .and_then(|c| run(&c));
    match res {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("pumasi-vz: {e:#}");
            1
        }
    }
}

fn run(config: &VzConfig) -> Result<()> {
    ensure!(!config.disks.is_empty(), "no disks");
    #[cfg(target_os = "macos")]
    {
        imp::run(config)
    }
    #[cfg(not(target_os = "macos"))]
    bail!("Virtualization.framework exists only on macOS")
}

#[cfg(target_os = "macos")]
mod imp {
    use std::{
        cell::RefCell,
        ffi::{CStr, CString, c_void},
        path::Path,
        ptr,
        rc::Rc,
        sync::OnceLock,
        time::{Duration, Instant},
    };

    use anyhow::{Context, Result, bail};
    use block2::RcBlock;
    use objc2::{
        msg_send,
        rc::{Allocated, Retained},
        runtime::{AnyClass, AnyObject, Bool},
    };

    use super::VzConfig;

    const FRAMEWORK: &CStr = c"/System/Library/Frameworks/Virtualization.framework/Virtualization";

    // VZVirtualMachineState
    const STATE_STOPPED: isize = 0;
    const STATE_ERROR: isize = 3;

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        static kCFRunLoopDefaultMode: *const c_void;
        fn CFRunLoopRunInMode(mode: *const c_void, seconds: f64, after_source: u8) -> i32;
    }

    /// Loads the framework once. `Err` on macOS 10.15, which lacks it.
    pub fn load() -> std::result::Result<(), String> {
        static LOADED: OnceLock<std::result::Result<(), String>> = OnceLock::new();
        LOADED
            .get_or_init(|| {
                // SAFETY: loading a system framework by absolute path.
                let h = unsafe { libc::dlopen(FRAMEWORK.as_ptr(), libc::RTLD_NOW) };
                if h.is_null() {
                    Err("Virtualization.framework needs macOS 11 (Big Sur) or later; on macOS 10.15 this Mac runs WASM jobs only".into())
                } else {
                    Ok(())
                }
            })
            .clone()
    }

    fn class(name: &CStr) -> Result<&'static AnyClass> {
        AnyClass::get(name).with_context(|| format!("no class {}", name.to_string_lossy()))
    }

    pub fn status() -> std::result::Result<(), String> {
        load()?;
        let cls = class(c"VZVirtualMachineConfiguration").map_err(|e| e.to_string())?;
        // SAFETY: a class property available since macOS 11.
        let ok: Bool = unsafe { msg_send![cls, isSupported] };
        if ok.as_bool() {
            Ok(())
        } else {
            Err("this Mac does not support virtualization (no VT-x with EPT, or it is a VM itself without nested virtualization)".into())
        }
    }

    fn nsstring(s: &str) -> Result<Retained<AnyObject>> {
        let c = CString::new(s).context("string with a NUL byte")?;
        // SAFETY: a valid C string; the result is retained.
        let s: Option<Retained<AnyObject>> =
            unsafe { msg_send![class(c"NSString")?, stringWithUTF8String: c.as_ptr()] };
        s.context("NSString")
    }

    fn file_url(p: &Path) -> Result<Retained<AnyObject>> {
        let s = nsstring(&p.display().to_string())?;
        // SAFETY: fileURLWithPath: takes a non-null NSString.
        let u: Option<Retained<AnyObject>> =
            unsafe { msg_send![class(c"NSURL")?, fileURLWithPath: &*s] };
        u.context("NSURL")
    }

    fn array(items: &[Retained<AnyObject>]) -> Result<Retained<AnyObject>> {
        let ptrs: Vec<*const AnyObject> = items.iter().map(|o| &**o as *const AnyObject).collect();
        // SAFETY: `count` valid object pointers.
        let a: Option<Retained<AnyObject>> = unsafe {
            msg_send![class(c"NSArray")?, arrayWithObjects: ptrs.as_ptr(), count: ptrs.len()]
        };
        a.context("NSArray")
    }

    fn alloc(name: &CStr) -> Result<Allocated<AnyObject>> {
        // SAFETY: `alloc` on an NSObject subclass, initialized by the caller.
        Ok(unsafe { msg_send![class(name)?, alloc] })
    }

    fn new(name: &CStr) -> Result<Retained<AnyObject>> {
        // SAFETY: `new` on an NSObject subclass.
        let o: Option<Retained<AnyObject>> = unsafe { msg_send![class(name)?, new] };
        o.with_context(|| format!("new {}", name.to_string_lossy()))
    }

    fn describe(err: *mut AnyObject) -> String {
        if err.is_null() {
            return "unknown error".into();
        }
        // SAFETY: an NSError; localizedDescription is a non-null NSString
        // whose UTF8String lives as long as the autorelease pool.
        unsafe {
            let d: *mut AnyObject = msg_send![err, localizedDescription];
            if d.is_null() {
                return "unknown error".into();
            }
            let c: *const std::ffi::c_char = msg_send![d, UTF8String];
            if c.is_null() {
                return "unknown error".into();
            }
            CStr::from_ptr(c).to_string_lossy().into_owned()
        }
    }

    pub fn run(c: &VzConfig) -> Result<()> {
        load().map_err(anyhow::Error::msg)?;
        status().map_err(anyhow::Error::msg)?;
        let config_cls = class(c"VZVirtualMachineConfiguration")?;

        // Boot loader: kernel, initramfs, command line.
        let kernel = file_url(&c.kernel)?;
        let initrd = file_url(&c.initrd)?;
        let cmdline = nsstring(&c.cmdline)?;
        // SAFETY (this whole block): documented Virtualization.framework
        // selectors of macOS 11, with arguments of the declared types.
        let boot: Retained<AnyObject> = unsafe {
            let a: Option<Retained<AnyObject>> = msg_send![
                alloc(c"VZLinuxBootLoader")?,
                initWithKernelURL: &*kernel
            ];
            a.context("VZLinuxBootLoader")?
        };
        unsafe {
            let _: () = msg_send![&*boot, setInitialRamdiskURL: &*initrd];
            let _: () = msg_send![&*boot, setCommandLine: &*cmdline];
        }

        // Disks.
        let mut disks = Vec::new();
        for (path, read_only) in &c.disks {
            let url = file_url(path)?;
            let mut err: *mut AnyObject = ptr::null_mut();
            let attachment: Option<Retained<AnyObject>> = unsafe {
                msg_send![
                    alloc(c"VZDiskImageStorageDeviceAttachment")?,
                    initWithURL: &*url,
                    readOnly: Bool::new(*read_only),
                    error: &mut err
                ]
            };
            let attachment = attachment
                .with_context(|| format!("attach {}: {}", path.display(), describe(err)))?;
            let dev: Option<Retained<AnyObject>> = unsafe {
                msg_send![
                    alloc(c"VZVirtioBlockDeviceConfiguration")?,
                    initWithAttachment: &*attachment
                ]
            };
            disks.push(dev.context("VZVirtioBlockDeviceConfiguration")?);
        }

        // Console on our stdout (the agent's console.log).
        let stdout: Option<Retained<AnyObject>> =
            unsafe { msg_send![class(c"NSFileHandle")?, fileHandleWithStandardOutput] };
        let stdout = stdout.context("stdout handle")?;
        let none: Option<&AnyObject> = None;
        let attachment: Option<Retained<AnyObject>> = unsafe {
            msg_send![
                alloc(c"VZFileHandleSerialPortAttachment")?,
                initWithFileHandleForReading: none,
                fileHandleForWriting: &*stdout
            ]
        };
        let attachment = attachment.context("VZFileHandleSerialPortAttachment")?;
        let console = new(c"VZVirtioConsoleDeviceSerialPortConfiguration")?;
        unsafe {
            let _: () = msg_send![&*console, setAttachment: &*attachment];
        }
        let entropy = new(c"VZVirtioEntropyDeviceConfiguration")?;

        // CPUs and memory, within what the framework allows.
        let (min_cpus, max_cpus, min_mem, max_mem): (usize, usize, u64, u64) = unsafe {
            (
                msg_send![config_cls, minimumAllowedCPUCount],
                msg_send![config_cls, maximumAllowedCPUCount],
                msg_send![config_cls, minimumAllowedMemorySize],
                msg_send![config_cls, maximumAllowedMemorySize],
            )
        };
        let cpus = (c.cpus as usize).clamp(min_cpus.max(1), max_cpus.max(1));
        let mem = (c.mem_mib << 20).clamp(min_mem, max_mem.max(min_mem));
        let mem = mem.div_ceil(1 << 20) << 20;

        let config = new(c"VZVirtualMachineConfiguration")?;
        unsafe {
            let _: () = msg_send![&*config, setBootLoader: &*boot];
            let _: () = msg_send![&*config, setCPUCount: cpus];
            let _: () = msg_send![&*config, setMemorySize: mem];
            let _: () = msg_send![&*config, setStorageDevices: &*array(&disks)?];
            let _: () = msg_send![&*config, setSerialPorts: &*array(&[console])?];
            let _: () = msg_send![&*config, setEntropyDevices: &*array(&[entropy])?];
            let mut err: *mut AnyObject = ptr::null_mut();
            let ok: Bool = msg_send![&*config, validateWithError: &mut err];
            if !ok.as_bool() {
                bail!("invalid VM configuration: {}", describe(err));
            }
        }

        // The VM lives on the main queue, which the run loop below drains.
        let vm: Retained<AnyObject> = unsafe {
            let v: Option<Retained<AnyObject>> = msg_send![
                alloc(c"VZVirtualMachine")?,
                initWithConfiguration: &*config
            ];
            v.context("VZVirtualMachine")?
        };
        let started: Rc<RefCell<Option<std::result::Result<(), String>>>> = Rc::default();
        let done = started.clone();
        let handler = RcBlock::new(move |err: *mut AnyObject| {
            *done.borrow_mut() = Some(if err.is_null() {
                Ok(())
            } else {
                Err(describe(err))
            });
        });
        unsafe {
            let _: () = msg_send![&*vm, startWithCompletionHandler: &*handler];
        }
        let begun = Instant::now();
        loop {
            // SAFETY: on the main thread; the default mode constant.
            unsafe { CFRunLoopRunInMode(kCFRunLoopDefaultMode, 0.2, 0) };
            match &*started.borrow() {
                None if begun.elapsed() > Duration::from_secs(60) => {
                    bail!("the VM did not start within 60 s")
                }
                None => continue,
                Some(Err(e)) => bail!("the VM did not start: {e}"),
                Some(Ok(())) => {}
            }
            let state: isize = unsafe { msg_send![&*vm, state] };
            match state {
                // The guest powered off or rebooted (which stops a VM too).
                STATE_STOPPED => return Ok(()),
                STATE_ERROR => bail!("the VM stopped with an error"),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_roundtrips_as_json() {
        let c = VzConfig {
            kernel: "/k".into(),
            initrd: "/i".into(),
            cmdline: "console=hvc0".into(),
            disks: vec![("/a.img".into(), true), ("/b.img".into(), false)],
            cpus: 2,
            mem_mib: 352,
        };
        let back: VzConfig = serde_json::from_slice(&serde_json::to_vec(&c).unwrap()).unwrap();
        assert_eq!(back.disks, c.disks);
        assert_eq!(back.mem_mib, 352);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn unavailable_elsewhere() {
        assert!(status().is_err());
    }
}
