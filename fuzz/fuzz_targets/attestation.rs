//! SEV-SNP evidence comes from the node being attested, which is exactly
//! who it must not trust.
#![no_main]
use libfuzzer_sys::fuzz_target;
use pumasi_cluster::attest::{check_snp, vcek_from_aux};

fuzz_target!(|data: &[u8]| {
    let _ = vcek_from_aux(data);
    let _ = pumasi_guest::seal::AttestRequest::decode(data);
    let Some((&split, rest)) = data.split_first() else {
        return;
    };
    let split = usize::from(split).min(rest.len());
    let (vcek, report) = rest.split_at(split);
    let _ = check_snp(report, vcek, &[0; 64]);
});
