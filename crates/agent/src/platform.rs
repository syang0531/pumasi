//! The few OS-specific pieces the executors need: sparse files, Windows Job
//! Objects for the processes they start, and the Windows Hypervisor
//! Platform check. On other systems these are no-ops.

use std::fs::File;

/// Marks `f` sparse, so extending it does not allocate the zeros. Unix
/// files are sparse already; NTFS files only after `FSCTL_SET_SPARSE`.
pub fn set_sparse(f: &File) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::{IO::DeviceIoControl, Ioctl::FSCTL_SET_SPARSE};
        let mut returned = 0u32;
        // SAFETY: a valid file handle, no buffers, synchronous call.
        let ok = unsafe {
            DeviceIoControl(
                f.as_raw_handle(),
                FSCTL_SET_SPARSE,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    #[cfg(not(windows))]
    let _ = f;
    Ok(())
}

/// A Windows Job Object holding processes the agent started (a VMM, a
/// Docker CLI). Closing it kills them: when the task ends, and when the
/// agent itself dies, since the OS closes the handle then. Elsewhere it
/// does nothing (`kill_on_drop` covers Unix).
pub struct ProcessJob {
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
}

// SAFETY: a job handle may be used from any thread.
unsafe impl Send for ProcessJob {}
unsafe impl Sync for ProcessJob {}

impl ProcessJob {
    /// A job that kills its processes when it is closed.
    pub fn new() -> std::io::Result<Self> {
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::{
                CreateJobObjectW, JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JobObjectExtendedLimitInformation, SetInformationJobObject,
            };
            // SAFETY: plain Win32 calls; the handle is owned by `job` from
            // here on and closed in `drop`.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return Err(std::io::Error::last_os_error());
            }
            let job = Self { handle };
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            info.BasicLimitInformation.LimitFlags =
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
            let ok = unsafe {
                SetInformationJobObject(
                    job.handle,
                    JobObjectExtendedLimitInformation,
                    &info as *const _ as *const std::ffi::c_void,
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            if ok == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(job)
        }
        #[cfg(not(windows))]
        Ok(Self {})
    }

    /// Puts `child` into the job. The child has started already; the VMMs
    /// and CLIs this is used for do not start processes of their own before
    /// this runs.
    pub fn assign(&self, child: &tokio::process::Child) -> std::io::Result<()> {
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::AssignProcessToJobObject;
            let Some(process) = child.raw_handle() else {
                return Ok(()); // already reaped
            };
            // SAFETY: both handles are valid for the duration of the call.
            if unsafe { AssignProcessToJobObject(self.handle, process) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        #[cfg(not(windows))]
        let _ = child;
        Ok(())
    }
}

impl Drop for ProcessJob {
    fn drop(&mut self) {
        #[cfg(windows)]
        // SAFETY: the handle is ours and closed exactly once.
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.handle);
        }
    }
}

/// Whether the Windows Hypervisor Platform is enabled and a hypervisor is
/// running, which QEMU's `whpx` accelerator needs. `Err` says why not.
/// Always `Err` on other systems.
pub fn whpx_status() -> Result<(), String> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::LibraryLoader::{
            GetProcAddress, LOAD_LIBRARY_SEARCH_SYSTEM32, LoadLibraryExW,
        };
        const FEATURE_OFF: &str = "the Windows Hypervisor Platform feature is off; enable it \
            (as administrator: `Enable-WindowsOptionalFeature -Online -FeatureName \
            HypervisorPlatform`, or Windows Features > Windows Hypervisor Platform) and reboot";
        let name: Vec<u16> = "WinHvPlatform.dll\0".encode_utf16().collect();
        // The DLL exists only while the feature is on, so load it at run
        // time (linking it would stop the agent from starting at all).
        // SAFETY: loading a system DLL by name from System32 only.
        let module = unsafe {
            LoadLibraryExW(
                name.as_ptr(),
                std::ptr::null_mut(),
                LOAD_LIBRARY_SEARCH_SYSTEM32,
            )
        };
        if module.is_null() {
            return Err(FEATURE_OFF.into());
        }
        let Some(proc) = (unsafe { GetProcAddress(module, c"WHvGetCapability".as_ptr().cast()) })
        else {
            return Err(FEATURE_OFF.into());
        };
        type GetCapability =
            unsafe extern "system" fn(u32, *mut std::ffi::c_void, u32, *mut u32) -> i32;
        // SAFETY: WHvGetCapability has this signature (WinHvPlatform.h).
        let get: GetCapability = unsafe { std::mem::transmute(proc) };
        let mut present: i32 = 0;
        let mut written = 0u32;
        // WHvCapabilityCodeHypervisorPresent = 0, answered as a BOOL.
        let hr = unsafe {
            get(
                0,
                &mut present as *mut i32 as *mut std::ffi::c_void,
                4,
                &mut written,
            )
        };
        if hr < 0 {
            return Err(format!("WHvGetCapability failed (HRESULT {hr:#010x})"));
        }
        if present == 0 {
            return Err(
                "no hypervisor is running: turn on virtualization (VT-x/AMD-V) in the \
                firmware settings, and check that `bcdedit` does not set \
                hypervisorlaunchtype off"
                    .into(),
            );
        }
        Ok(())
    }
    #[cfg(not(windows))]
    Err("the Windows Hypervisor Platform exists only on Windows".into())
}

/// This CPU's time stamp counter rate in kHz, measured against the system
/// clock over `window`. A hardware-accelerated guest reads the same TSC,
/// so passing this as `tsc_early_khz` spares its kernel calibrating it
/// against emulated timers (which can stall a boot).
pub fn measure_tsc_khz(window: std::time::Duration) -> Option<u64> {
    #[cfg(target_arch = "x86_64")]
    {
        use std::time::Instant;
        // SAFETY: RDTSC is available on every x86_64 CPU.
        let (t0, c0) = (Instant::now(), unsafe { core::arch::x86_64::_rdtsc() });
        std::thread::sleep(window);
        let (c1, t1) = (unsafe { core::arch::x86_64::_rdtsc() }, Instant::now());
        let nanos = t1.duration_since(t0).as_nanos() as u64;
        let khz = (c1.wrapping_sub(c0) as u128 * 1_000_000 / nanos.max(1) as u128) as u64;
        // Anything outside 100 MHz to 10 GHz is a broken measurement.
        (100_000..=10_000_000).contains(&khz).then_some(khz)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = window;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_file_and_job() {
        let dir = crate::tempdir::TempDir::new("platform").unwrap();
        let f = File::create(dir.path().join("s")).unwrap();
        set_sparse(&f).unwrap();
        f.set_len(1 << 30).unwrap();
        ProcessJob::new().unwrap();
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn tsc_rate_is_plausible() {
        let khz = measure_tsc_khz(std::time::Duration::from_millis(20));
        assert!(khz.is_some(), "{khz:?}");
    }

    /// The job kills its process when it is dropped.
    #[cfg(windows)]
    #[tokio::test]
    async fn dropping_the_job_kills_the_process() {
        let mut child = tokio::process::Command::new("ping")
            .args(["-n", "30", "127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let job = ProcessJob::new().unwrap();
        job.assign(&child).unwrap();
        drop(job);
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .expect("process still running after its job closed")
            .unwrap();
        assert!(!status.success());
    }
}
