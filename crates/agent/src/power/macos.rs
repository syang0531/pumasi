//! macOS: an IOKit power assertion and the system power notifications
//! (`IORegisterForSystemPower`), on a thread with its own run loop.

use std::{
    ffi::{CString, c_char, c_void},
    sync::OnceLock,
};

use super::{PowerBackend, PowerEvent, PowerHandler};

pub struct IoKit;

type CfString = *const c_void;

const UTF8: u32 = 0x0800_0100;
const ASSERTION_LEVEL_ON: u32 = 255;
const CAN_SYSTEM_SLEEP: u32 = 0xe000_0270;
const SYSTEM_WILL_SLEEP: u32 = 0xe000_0280;
const SYSTEM_WILL_NOT_SLEEP: u32 = 0xe000_0290;
const SYSTEM_HAS_POWERED_ON: u32 = 0xe000_0300;

type PowerCallback = extern "C" fn(*mut c_void, u32, u32, *mut c_void);

#[link(name = "IOKit", kind = "framework")]
unsafe extern "C" {
    fn IOPMAssertionCreateWithName(
        assertion_type: CfString,
        level: u32,
        name: CfString,
        id: *mut u32,
    ) -> i32;
    fn IOPMAssertionRelease(id: u32) -> i32;
    fn IORegisterForSystemPower(
        refcon: *mut c_void,
        port: *mut *mut c_void,
        callback: PowerCallback,
        notifier: *mut u32,
    ) -> u32;
    fn IONotificationPortGetRunLoopSource(port: *mut c_void) -> *mut c_void;
    fn IOAllowPowerChange(kernel_port: u32, notification_id: isize) -> i32;
}

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    fn CFStringCreateWithCString(alloc: *const c_void, s: *const c_char, encoding: u32)
    -> CfString;
    fn CFRelease(cf: *const c_void);
    fn CFRunLoopGetCurrent() -> *mut c_void;
    fn CFRunLoopAddSource(run_loop: *mut c_void, source: *mut c_void, mode: CfString);
    fn CFRunLoopRun();
    static kCFRunLoopCommonModes: CfString;
}

fn cf_string(s: &str) -> Result<CfString, String> {
    let c = CString::new(s).map_err(|e| e.to_string())?;
    // SAFETY: a valid C string; the result is ours to release.
    let cf = unsafe { CFStringCreateWithCString(std::ptr::null(), c.as_ptr(), UTF8) };
    if cf.is_null() {
        return Err("CFStringCreateWithCString failed".into());
    }
    Ok(cf)
}

/// A power assertion, released when dropped.
struct Assertion(u32);

impl Drop for Assertion {
    fn drop(&mut self) {
        // SAFETY: an id IOPMAssertionCreateWithName gave us, released once.
        unsafe {
            IOPMAssertionRelease(self.0);
        }
    }
}

/// The root power domain's port and the handler, for the callback.
static WATCH: OnceLock<(u32, PowerHandler)> = OnceLock::new();

extern "C" fn on_power(_refcon: *mut c_void, _service: u32, message: u32, argument: *mut c_void) {
    let Some((root, handler)) = WATCH.get() else {
        return;
    };
    match message {
        // Idle sleep asks first: never object (the assertion is what
        // keeps the machine awake while tasks run).
        CAN_SYSTEM_SLEEP => {
            // SAFETY: acknowledges the notification we were just given.
            unsafe {
                IOAllowPowerChange(*root, argument as isize);
            }
        }
        SYSTEM_WILL_SLEEP => {
            handler(PowerEvent::Sleep);
            // SAFETY: as above; the system sleeps once every client did.
            unsafe {
                IOAllowPowerChange(*root, argument as isize);
            }
        }
        SYSTEM_WILL_NOT_SLEEP | SYSTEM_HAS_POWERED_ON => handler(PowerEvent::Wake),
        _ => {}
    }
}

impl PowerBackend for IoKit {
    fn stay_awake(&self) -> Result<Box<dyn Send>, String> {
        let kind = cf_string("PreventUserIdleSystemSleep")?;
        let name = cf_string("Pumasi is running cluster tasks")?;
        let mut id = 0u32;
        // SAFETY: two valid CFStrings and a place for the id.
        let rc = unsafe { IOPMAssertionCreateWithName(kind, ASSERTION_LEVEL_ON, name, &mut id) };
        // SAFETY: created above, released once.
        unsafe {
            CFRelease(kind);
            CFRelease(name);
        }
        if rc != 0 {
            return Err(format!("IOPMAssertionCreateWithName failed ({rc:#x})"));
        }
        Ok(Box::new(Assertion(id)))
    }

    fn watch(&self, on_event: PowerHandler) -> Result<(), String> {
        if WATCH.get().is_some() {
            return Err("power notifications are already being followed".into());
        }
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        std::thread::Builder::new()
            .name("pumasi-power".into())
            .spawn(move || {
                let mut port: *mut c_void = std::ptr::null_mut();
                let mut notifier = 0u32;
                // SAFETY: out-pointers for the port and the notifier; the
                // callback is a plain function that lives for ever.
                let root = unsafe {
                    IORegisterForSystemPower(
                        std::ptr::null_mut(),
                        &mut port,
                        on_power,
                        &mut notifier,
                    )
                };
                if root == 0 || port.is_null() {
                    let _ = ready_tx.send(Err("IORegisterForSystemPower failed".into()));
                    return;
                }
                if WATCH.set((root, on_event)).is_err() {
                    let _ =
                        ready_tx.send(Err("power notifications are already being followed".into()));
                    return;
                }
                // SAFETY: the port is valid; this thread's run loop
                // delivers the notifications for as long as it runs.
                unsafe {
                    CFRunLoopAddSource(
                        CFRunLoopGetCurrent(),
                        IONotificationPortGetRunLoopSource(port),
                        kCFRunLoopCommonModes,
                    );
                }
                let _ = ready_tx.send(Ok(()));
                // SAFETY: runs this thread's run loop until the process
                // ends.
                unsafe { CFRunLoopRun() };
            })
            .map_err(|e| format!("thread: {e}"))?;
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| "the power notification thread did not start".to_string())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The assertion shows in `pmset -g assertions` while held.
    #[test]
    fn the_assertion_is_listed_while_held() {
        let listed = || {
            let out = std::process::Command::new("pmset")
                .args(["-g", "assertions"])
                .output()
                .expect("pmset runs");
            String::from_utf8_lossy(&out.stdout).contains("Pumasi is running cluster tasks")
        };
        let hold = IoKit.stay_awake().expect("assertion");
        assert!(listed(), "the assertion is not in pmset -g assertions");
        drop(hold);
        assert!(!listed(), "the assertion was not released");
    }
}
