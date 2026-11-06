//! Receipts are passed to people outside the cluster and verified there.
#![no_main]
use libfuzzer_sys::fuzz_target;
use pumasi_proto::receipt::Receipt;

fuzz_target!(|data: &[u8]| {
    if let Ok(r) = Receipt::decode(data) {
        let _ = r.verify();
        let _ = pumasi_proto::receipt::ReceiptRow::from(&r);
    }
});
