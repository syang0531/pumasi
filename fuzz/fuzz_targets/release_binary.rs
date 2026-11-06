//! Rollouts read build info and the target out of uploaded binaries.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = pumasi_proto::release::read_build_info(data);
    let _ = pumasi_proto::release::detect_target(data);
});
