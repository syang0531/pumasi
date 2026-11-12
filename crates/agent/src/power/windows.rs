//! Windows: a power request while tasks run, and the suspend and resume
//! notifications, which reach a service and a console process alike.

use std::ffi::c_void;

use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE},
    System::{
        Power::{
            DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS, PowerClearRequest, PowerCreateRequest,
            PowerRegisterSuspendResumeNotification, PowerRequestSystemRequired, PowerSetRequest,
        },
        Threading::{POWER_REQUEST_CONTEXT_SIMPLE_STRING, REASON_CONTEXT, REASON_CONTEXT_0},
    },
};

use super::{PowerBackend, PowerEvent, PowerHandler};

pub struct WinPower;

const POWER_REQUEST_CONTEXT_VERSION: u32 = 0;
const DEVICE_NOTIFY_CALLBACK: u32 = 2;
const PBT_APMSUSPEND: u32 = 4;
const PBT_APMRESUMESUSPEND: u32 = 7;
const PBT_APMRESUMEAUTOMATIC: u32 = 18;

/// A power request, cleared and closed when dropped.
struct Request(HANDLE);

// SAFETY: a power request handle may be used from any thread.
unsafe impl Send for Request {}

impl Drop for Request {
    fn drop(&mut self) {
        // SAFETY: the handle is ours and closed exactly once.
        unsafe {
            PowerClearRequest(self.0, PowerRequestSystemRequired);
            CloseHandle(self.0);
        }
    }
}

/// Called by the system on a thread of its own; `context` is the handler
/// we leaked in `watch`.
unsafe extern "system" fn on_power(
    context: *const c_void,
    kind: u32,
    _setting: *const c_void,
) -> u32 {
    // SAFETY: `context` is the `PowerHandler` leaked in `watch`, which
    // lives for the rest of the process.
    let handler = unsafe { &*(context as *const PowerHandler) };
    match kind {
        PBT_APMSUSPEND => handler(PowerEvent::Sleep),
        PBT_APMRESUMEAUTOMATIC | PBT_APMRESUMESUSPEND => handler(PowerEvent::Wake),
        _ => {}
    }
    0
}

impl PowerBackend for WinPower {
    fn stay_awake(&self) -> Result<Box<dyn Send>, String> {
        let mut reason: Vec<u16> = "Pumasi is running cluster tasks\0".encode_utf16().collect();
        let context = REASON_CONTEXT {
            Version: POWER_REQUEST_CONTEXT_VERSION,
            Flags: POWER_REQUEST_CONTEXT_SIMPLE_STRING,
            Reason: REASON_CONTEXT_0 {
                SimpleReasonString: reason.as_mut_ptr(),
            },
        };
        // SAFETY: a valid context whose string outlives the call (the
        // system copies it).
        let handle = unsafe { PowerCreateRequest(&context) };
        if handle.is_null() || handle as isize == -1 {
            return Err(format!(
                "PowerCreateRequest failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let request = Request(handle);
        // SAFETY: the handle was just created.
        if unsafe { PowerSetRequest(request.0, PowerRequestSystemRequired) } == 0 {
            return Err(format!(
                "PowerSetRequest failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(Box::new(request))
    }

    fn watch(&self, on_event: PowerHandler) -> Result<(), String> {
        // The registration lasts as long as the process: the handler and
        // the handle are never freed.
        let context = Box::into_raw(Box::new(on_event)) as *mut c_void;
        let params = DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS {
            Callback: Some(on_power),
            Context: context,
        };
        let mut registration: *mut c_void = std::ptr::null_mut();
        // SAFETY: valid parameters; the system copies them.
        let rc = unsafe {
            PowerRegisterSuspendResumeNotification(
                DEVICE_NOTIFY_CALLBACK,
                &params as *const _ as HANDLE,
                &mut registration,
            )
        };
        if rc != 0 {
            return Err(format!(
                "PowerRegisterSuspendResumeNotification failed (error {rc})"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_power_request_can_be_taken_and_released() {
        let hold = WinPower.stay_awake().expect("power request");
        drop(hold);
    }
}
