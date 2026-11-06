# Recorded attestation evidence

Real SEV-SNP and TDX evidence for the tests in `crates/cluster/src/attest.rs`,
which check that genuine evidence passes and that forgeries made from it
(changed bytes, another run's report data, the wrong certificate) get
nothing.

| File | What | From |
| --- | --- | --- |
| `report_milan.hex` | A SEV-SNP attestation report from an AMD Milan host | the `sev` crate 8.0.0 test data (Apache-2.0) |
| `vcek_milan.der` | That host's VCEK | the `sev` crate 8.0.0 test data (Apache-2.0) |
| `vcek_turin.der` | A Turin VCEK (another generation's chain) | the `sev` crate 8.0.0 test data (Apache-2.0) |
| `tdx_quote`, `tdx_quote_collateral.json` | A TDX quote and Intel's collateral for its platform | `dcap-qvl` 0.7.1 samples (MIT) |
| `tdx_quote_outdated`, `tdx_quote_outdated_collateral.json` | A TDX quote from a platform whose TCB is out of date | `dcap-qvl` 0.7.1 samples (MIT) |
