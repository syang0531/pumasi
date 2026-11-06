//! A microVM guest's output disk is written by untrusted code.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = pumasi_proto::vmdisk::parse_output(data, None);
});
