//! Admin calls arrive from anyone who knows the control plane's endpoint
//! id (sign-in ops need no secret), so decoding must never panic.
#![no_main]
use libfuzzer_sys::fuzz_target;
use pumasi_proto::msg::{AdminRequest, AdminResponse};

fuzz_target!(|data: &[u8]| {
    if let Ok(req) = postcard::from_bytes::<AdminRequest>(data) {
        let _ = req.op.is_sign_in();
        let bytes = postcard::to_stdvec(&req).expect("re-encode");
        postcard::from_bytes::<AdminRequest>(&bytes).expect("decode what we encoded");
    }
    let _ = postcard::from_bytes::<AdminResponse>(data);
});
