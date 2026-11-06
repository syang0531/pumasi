//! Checks the evidence of a confidential task's guest (phase 16) before
//! the control plane releases its keys.
//!
//! - **SEV-SNP.** The attestation report must be signed by the chip's VCEK,
//!   the VCEK by AMD's ASK and ARK for its generation (Milan, Genoa or
//!   Turin; the ARK and ASK are built in, never taken from the node), the
//!   firmware versions in the VCEK must be the ones the report says, and
//!   the guest policy must forbid debugging and migration agents. The VCEK
//!   comes from the guest's extended report when the host loaded the
//!   certificates, else from AMD's key distribution service.
//! - **TDX.** The quote is checked with Intel's DCAP collateral (PCK chain
//!   to Intel's root, CRLs, TCB info and QE identity, fetched from Intel's
//!   PCS and cached) by `dcap-qvl`; the platform's TCB status must not be
//!   out of date or revoked, and the TD must not be debuggable.
//! - **Test.** The stand-in for tests, accepted only by a control plane
//!   started with `--insecure-test-tee`.
//!
//! In every case the report data must be the one the control plane
//! expects for the run (its nonce and the guest's key), so a report cannot
//! be replayed for another run or another key. Whether the launch
//! measurement is trusted is the state's decision, not this module's.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::confidential::{Evidence, TeeKind};
use sha2::{Digest, Sha384};

/// What a check found: which TEE, the launch measurement (its id), and
/// details for the admin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    pub tee: TeeKind,
    /// See [`pumasi_proto::confidential::MeasurementView::id`].
    pub measurement: String,
    pub detail: String,
    /// SEV-SNP: the start of the chip id. TDX: the platform's FMSPC.
    pub chip: String,
}

fn hex(b: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(b)
}

// ---------------------------------------------------------------------------
// SEV-SNP
// ---------------------------------------------------------------------------

/// AMD processor generations with SEV-SNP, by KDS product name.
const SNP_GENERATIONS: &[&str] = &["Milan", "Genoa", "Turin"];

fn snp_ca(generation: &str) -> Result<sev::certs::snp::ca::Chain> {
    use sev::certs::snp::builtin::{genoa, milan, turin};
    let (ark, ask) = match generation {
        "Milan" => (milan::ark()?, milan::ask()?),
        "Genoa" => (genoa::ark()?, genoa::ask()?),
        "Turin" => (turin::ark()?, turin::ask()?),
        other => bail!("unknown SEV-SNP generation {other}"),
    };
    Ok(sev::certs::snp::ca::Chain { ark, ask })
}

/// The firmware versions and chip id a VCEK certifies.
#[derive(Debug, Default, PartialEq, Eq)]
struct VcekClaims {
    bl: Option<u8>,
    tee: Option<u8>,
    snp: Option<u8>,
    ucode: Option<u8>,
    fmc: Option<u8>,
    hwid: Vec<u8>,
}

/// A small DER INTEGER (an SPL) or OCTET STRING (the hardware id), as AMD
/// puts them in VCEK extensions.
fn der_value(raw: &[u8]) -> Option<(u8, &[u8])> {
    let (&tag, rest) = raw.split_first()?;
    let (&len, rest) = rest.split_first()?;
    let len = len as usize;
    (len < 0x80 && rest.len() == len).then_some((tag, rest))
}

fn vcek_claims(der: &[u8]) -> Result<VcekClaims> {
    use x509_cert::der::{Decode, oid::ObjectIdentifier};
    let cert = x509_cert::Certificate::from_der(der).context("parse the VCEK")?;
    let spl = |n: &str| ObjectIdentifier::new_unwrap(n);
    let (bl, tee, snp, ucode, fmc, hwid) = (
        spl("1.3.6.1.4.1.3704.1.3.1"),
        spl("1.3.6.1.4.1.3704.1.3.2"),
        spl("1.3.6.1.4.1.3704.1.3.3"),
        spl("1.3.6.1.4.1.3704.1.3.8"),
        spl("1.3.6.1.4.1.3704.1.3.9"),
        spl("1.3.6.1.4.1.3704.1.4"),
    );
    let mut c = VcekClaims::default();
    for ext in cert.tbs_certificate.extensions.iter().flatten() {
        let raw = ext.extn_value.as_bytes();
        let int = || -> Option<u8> {
            let (tag, v) = der_value(raw)?;
            (tag == 0x02).then_some(())?;
            // A leading zero keeps values of 128 and up positive.
            match v {
                [x] => Some(*x),
                [0, x] => Some(*x),
                _ => None,
            }
        };
        if ext.extn_id == bl {
            c.bl = int();
        } else if ext.extn_id == tee {
            c.tee = int();
        } else if ext.extn_id == snp {
            c.snp = int();
        } else if ext.extn_id == ucode {
            c.ucode = int();
        } else if ext.extn_id == fmc {
            c.fmc = int();
        } else if ext.extn_id == hwid {
            c.hwid = match der_value(raw) {
                Some((0x04, v)) => v.to_vec(),
                _ => raw.to_vec(),
            };
        }
    }
    Ok(c)
}

/// The VCEK in an extended report's certificate table, if there is one.
/// Entries: a GUID, then the offset and length (from the table's start)
/// of the certificate, ending with an all-zero GUID.
pub fn vcek_from_aux(aux: &[u8]) -> Option<Vec<u8>> {
    // 63da758d-e664-4564-adc5-f4b93be8accd
    const VCEK_GUID: [u8; 16] = [
        0x63, 0xda, 0x75, 0x8d, 0xe6, 0x64, 0x45, 0x64, 0xad, 0xc5, 0xf4, 0xb9, 0x3b, 0xe8, 0xac,
        0xcd,
    ];
    for entry in aux.as_chunks::<24>().0.iter().take(64) {
        let guid = &entry[..16];
        if guid == [0u8; 16] {
            break;
        }
        if guid == VCEK_GUID {
            let at = u32::from_le_bytes(entry[16..20].try_into().ok()?) as usize;
            let len = u32::from_le_bytes(entry[20..24].try_into().ok()?) as usize;
            return aux.get(at..at.checked_add(len)?).map(<[u8]>::to_vec);
        }
    }
    None
}

/// The KDS product name for a report: from its CPUID fields (report
/// version 3 and later), else every generation in turn.
fn snp_products(report: &sev::firmware::guest::AttestationReport) -> Vec<&'static str> {
    match (report.cpuid_fam_id, report.cpuid_mod_id) {
        (Some(0x19), Some(m)) if m < 0x10 => vec!["Milan"],
        (Some(0x19), Some(_)) => vec!["Genoa"],
        (Some(0x1a), Some(_)) => vec!["Turin"],
        _ => vec!["Milan", "Genoa"],
    }
}

/// Checks a SEV-SNP attestation report against its VCEK.
pub fn check_snp(report: &[u8], vcek: &[u8], expected: &[u8; 64]) -> Result<Checked> {
    use sev::certs::snp::{Certificate, Chain, Verifiable};
    let parsed =
        <sev::firmware::guest::AttestationReport as sev::parser::ByteParser<()>>::from_bytes(
            report,
        )
        .context("parse the SEV-SNP report")?;
    ensure!(
        parsed.sig_algo == 1,
        "the report is not signed with ECDSA P-384"
    );
    let vek = Certificate::from_der(vcek).context("parse the VCEK")?;
    // The VCEK must chain to AMD's built-in root for its generation, and
    // sign the report.
    let generation = SNP_GENERATIONS
        .iter()
        .find(|g| {
            snp_ca(g).is_ok_and(|ca| {
                let chain = Chain {
                    ca,
                    vek: vek.clone(),
                };
                chain.verify().is_ok()
            })
        })
        .context("the VCEK is not signed by AMD (no Milan, Genoa or Turin root signs it)")?;
    let chain = Chain {
        ca: snp_ca(generation)?,
        vek,
    };
    (&chain, &parsed)
        .verify()
        .map_err(|e| anyhow::anyhow!("the report's signature does not check out: {e}"))?;
    // The VCEK is for these firmware versions on this chip; the report
    // must claim the same, or an old (vulnerable) firmware's key could
    // sign for a newer one.
    let claims = vcek_claims(vcek)?;
    let t = &parsed.reported_tcb;
    let same = claims.bl == Some(t.bootloader)
        && claims.tee == Some(t.tee)
        && claims.snp == Some(t.snp)
        && claims.ucode == Some(t.microcode)
        && (t.fmc.is_none() || claims.fmc == t.fmc);
    ensure!(
        same,
        "the VCEK's firmware versions ({claims:?}) are not the report's ({t:?})"
    );
    ensure!(
        !claims.hwid.is_empty() && parsed.chip_id.starts_with(&claims.hwid),
        "the VCEK is for another chip"
    );
    ensure!(
        !parsed.policy.debug_allowed(),
        "the guest policy allows debugging, so the host can read its memory"
    );
    ensure!(
        !parsed.policy.migrate_ma_allowed(),
        "the guest policy allows a migration agent"
    );
    ensure!(
        parsed.report_data == *expected,
        "the report data is not this run's (a replayed report, or another key)"
    );
    Ok(Checked {
        tee: TeeKind::SevSnp,
        measurement: hex(&parsed.measurement),
        detail: format!(
            "{generation}, TCB bootloader {} tee {} snp {} microcode {}{}, policy {:#x}, vmpl {}, report v{}",
            t.bootloader,
            t.tee,
            t.snp,
            t.microcode,
            t.fmc.map(|f| format!(" fmc {f}")).unwrap_or_default(),
            parsed.policy.0,
            parsed.vmpl,
            parsed.version
        ),
        chip: hex(&parsed.chip_id[..8]),
    })
}

// ---------------------------------------------------------------------------
// TDX
// ---------------------------------------------------------------------------

/// TCB statuses a TDX platform may have and still get keys: up to date,
/// or with mitigations that are configuration or software choices.
const TDX_ACCEPTED: &[&str] = &[
    "UpToDate",
    "SWHardeningNeeded",
    "ConfigurationNeeded",
    "ConfigurationAndSWHardeningNeeded",
];

/// The measurement id of a TD: SHA-384 of MRTD (the firmware), RTMR1 (the
/// kernel) and RTMR2 (its command line and initramfs). RTMR0 holds the
/// firmware's configuration, which changes with the VM's memory size.
pub fn tdx_measurement(mr_td: &[u8; 48], rtmr1: &[u8; 48], rtmr2: &[u8; 48]) -> String {
    let mut h = Sha384::new();
    h.update(mr_td);
    h.update(rtmr1);
    h.update(rtmr2);
    hex(&h.finalize())
}

/// Checks a TDX quote with its collateral at time `now` (Unix seconds).
pub fn check_tdx(
    quote: &[u8],
    collateral: &dcap_qvl::QuoteCollateralV3,
    now: u64,
    expected: &[u8; 64],
) -> Result<Checked> {
    let verified = dcap_qvl::verify::verify(quote, collateral, now)
        .map_err(|e| anyhow::anyhow!("the TDX quote does not check out: {e:#}"))?;
    ensure!(
        TDX_ACCEPTED.contains(&verified.status.as_str()),
        "the platform's TCB is {} (advisories: {})",
        verified.status,
        verified.advisory_ids.join(", ")
    );
    let td = verified
        .report
        .as_td10()
        .context("the quote is not from a TD")?;
    ensure!(
        td.td_attributes[0] & 1 == 0,
        "the TD is debuggable, so the host can read its memory"
    );
    ensure!(
        td.report_data == *expected,
        "the report data is not this run's (a replayed quote, or another key)"
    );
    let fmspc = {
        dcap_qvl::quote::Quote::parse(quote)
            .ok()
            .and_then(|q| dcap_qvl::intel::quote_fmspc(&q).ok())
            .map(|f| hex(f.as_ref()))
            .unwrap_or_default()
    };
    Ok(Checked {
        tee: TeeKind::Tdx,
        measurement: tdx_measurement(&td.mr_td, &td.rt_mr1, &td.rt_mr2),
        detail: format!(
            "TCB {}{}, MRTD {}, RTMR0 {}, RTMR1 {}, RTMR2 {}, RTMR3 {}",
            verified.status,
            if verified.advisory_ids.is_empty() {
                String::new()
            } else {
                format!(" ({})", verified.advisory_ids.join(", "))
            },
            hex(&td.mr_td),
            hex(&td.rt_mr0),
            hex(&td.rt_mr1),
            hex(&td.rt_mr2),
            hex(&td.rt_mr3)
        ),
        chip: fmspc,
    })
}

// ---------------------------------------------------------------------------
// The test stand-in
// ---------------------------------------------------------------------------

/// Checks test evidence: no hardware vouches for it, so only a control
/// plane started with `--insecure-test-tee` takes it. Its measurement is
/// what the guest says it runs (SHA-384 of its init).
pub fn check_test(ev: &Evidence, expected: &[u8; 64], allowed: bool) -> Result<Checked> {
    ensure!(
        allowed,
        "test evidence (no TEE) is refused: the control plane was not started with --insecure-test-tee"
    );
    ensure!(ev.report.len() == 48, "test evidence has no measurement");
    ensure!(
        ev.report_data.as_slice() == expected.as_slice(),
        "the report data is not this run's"
    );
    Ok(Checked {
        tee: TeeKind::Test,
        measurement: hex(&ev.report),
        detail: "test stand-in: no hardware vouches for this".into(),
        chip: "test".into(),
    })
}

// ---------------------------------------------------------------------------
// The verifier: collateral from AMD and Intel, cached
// ---------------------------------------------------------------------------

const AMD_KDS: &str = "https://kdsintf.amd.com";
const INTEL_PCS: &str = "https://api.trustedservices.intel.com";
const INTEL_ROOT_CRL: &str = "https://certificates.trustedservices.intel.com/IntelSGXRootCA.der";
/// How long fetched TDX collateral is used before it is fetched again.
const COLLATERAL_TTL: Duration = Duration::from_secs(6 * 3600);

/// Checks evidence, fetching what the checks need from AMD and Intel.
pub struct Verifier {
    http: reqwest::Client,
    test_tee: bool,
    /// VCEKs by KDS URL (chip and firmware versions).
    vceks: Mutex<HashMap<String, Vec<u8>>>,
    /// TDX collateral by FMSPC and CA.
    collateral: Mutex<HashMap<String, (Instant, dcap_qvl::QuoteCollateralV3)>>,
}

impl Verifier {
    pub fn new(test_tee: bool) -> Result<Self> {
        Ok(Self {
            http: pumasi_proto::registry::http_client()?,
            test_tee,
            vceks: Mutex::default(),
            collateral: Mutex::default(),
        })
    }

    pub fn test_tee(&self) -> bool {
        self.test_tee
    }

    /// Checks `ev` for a run whose report data must be `expected`.
    pub async fn check(&self, ev: &Evidence, expected: &[u8; 64]) -> Result<Checked> {
        match ev.tee {
            TeeKind::Test => check_test(ev, expected, self.test_tee),
            TeeKind::SevSnp => {
                let vcek = match vcek_from_aux(&ev.aux) {
                    Some(v) => v,
                    None => self.fetch_vcek(&ev.report).await?,
                };
                check_snp(&ev.report, &vcek, expected)
            }
            TeeKind::Tdx => {
                let collateral = self.tdx_collateral(&ev.report).await?;
                check_tdx(&ev.report, &collateral, crate::state::now_unix(), expected)
            }
        }
    }

    async fn get(&self, url: &str) -> Result<reqwest::Response> {
        let resp = self
            .http
            .get(url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .with_context(|| format!("fetch {url}"))?;
        ensure!(
            resp.status().is_success(),
            "fetch {url}: HTTP {}",
            resp.status()
        );
        Ok(resp)
    }

    /// The VCEK for a report's chip and firmware versions, from AMD's key
    /// distribution service.
    async fn fetch_vcek(&self, report: &[u8]) -> Result<Vec<u8>> {
        let parsed =
            <sev::firmware::guest::AttestationReport as sev::parser::ByteParser<()>>::from_bytes(
                report,
            )
            .context("parse the SEV-SNP report")?;
        let t = &parsed.reported_tcb;
        let mut last = None;
        for product in snp_products(&parsed) {
            let (hwid, fmc) = if product == "Turin" {
                (
                    hex(&parsed.chip_id[..8]),
                    format!("&fmcSPL={}", t.fmc.unwrap_or(0)),
                )
            } else {
                (hex(&parsed.chip_id), String::new())
            };
            let url = format!(
                "{AMD_KDS}/vcek/v1/{product}/{hwid}?blSPL={}&teeSPL={}&snpSPL={}&ucodeSPL={}{fmc}",
                t.bootloader, t.tee, t.snp, t.microcode
            );
            if let Some(v) = self.vceks.lock().unwrap().get(&url) {
                return Ok(v.clone());
            }
            match self.get(&url).await {
                Ok(resp) => {
                    let der = resp.bytes().await?.to_vec();
                    self.vceks.lock().unwrap().insert(url, der.clone());
                    return Ok(der);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last
            .unwrap_or_else(|| anyhow::anyhow!("no product"))
            .context("get the VCEK from AMD (the host loaded no certificates)"))
    }

    /// Intel's collateral for a quote's platform, cached for a while.
    async fn tdx_collateral(&self, quote: &[u8]) -> Result<dcap_qvl::QuoteCollateralV3> {
        let parsed = dcap_qvl::quote::Quote::parse(quote).context("parse the TDX quote")?;
        let fmspc = hex(&dcap_qvl::intel::quote_fmspc(&parsed).context("the quote's FMSPC")?);
        let ca = match dcap_qvl::intel::quote_ca(&parsed).context("the quote's PCK CA")? {
            dcap_qvl::config::PckCa::Platform => "platform",
            _ => "processor",
        };
        let key = format!("{fmspc}/{ca}");
        if let Some((at, c)) = self.collateral.lock().unwrap().get(&key)
            && at.elapsed() < COLLATERAL_TTL
        {
            return Ok(c.clone());
        }
        let header = |resp: &reqwest::Response, names: &[&str]| -> Result<String> {
            names
                .iter()
                .find_map(|n| resp.headers().get(*n))
                .and_then(|v| v.to_str().ok())
                .map(percent_decode)
                .with_context(|| format!("no {} header", names[0]))
        };
        let resp = self
            .get(&format!(
                "{INTEL_PCS}/sgx/certification/v4/pckcrl?ca={ca}&encoding=der"
            ))
            .await?;
        let pck_crl_issuer_chain = header(&resp, &["SGX-PCK-CRL-Issuer-Chain"])?;
        let pck_crl = resp.bytes().await?.to_vec();
        let resp = self
            .get(&format!(
                "{INTEL_PCS}/tdx/certification/v4/tcb?fmspc={fmspc}"
            ))
            .await?;
        let tcb_info_issuer_chain = header(
            &resp,
            &["TCB-Info-Issuer-Chain", "SGX-TCB-Info-Issuer-Chain"],
        )?;
        let tcb: serde_json::Value =
            serde_json::from_slice(&resp.bytes().await?).context("TCB info")?;
        let resp = self
            .get(&format!("{INTEL_PCS}/tdx/certification/v4/qe/identity"))
            .await?;
        let qe_identity_issuer_chain = header(&resp, &["SGX-Enclave-Identity-Issuer-Chain"])?;
        let qe: serde_json::Value =
            serde_json::from_slice(&resp.bytes().await?).context("QE identity")?;
        let root_ca_crl = self.get(INTEL_ROOT_CRL).await?.bytes().await?.to_vec();
        let signature = |v: &serde_json::Value| -> Result<Vec<u8>> {
            let s = v["signature"].as_str().context("no signature")?;
            data_encoding::HEXLOWER_PERMISSIVE
                .decode(s.as_bytes())
                .context("bad signature")
        };
        let collateral = dcap_qvl::QuoteCollateralV3 {
            pck_crl_issuer_chain,
            root_ca_crl,
            pck_crl,
            tcb_info_issuer_chain,
            tcb_info: tcb["tcbInfo"].to_string(),
            tcb_info_signature: signature(&tcb)?,
            qe_identity_issuer_chain,
            qe_identity: qe["enclaveIdentity"].to_string(),
            qe_identity_signature: signature(&qe)?,
            pck_certificate_chain: None,
        };
        self.collateral
            .lock()
            .unwrap()
            .insert(key, (Instant::now(), collateral.clone()));
        Ok(collateral)
    }
}

/// Decodes `%XX` escapes (Intel sends PEM chains URL-encoded in headers).
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (digit(b[i + 1]), digit(b[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MILAN_REPORT_HEX: &[u8] = include_bytes!("../tests/data/attest/report_milan.hex");
    const MILAN_VCEK: &[u8] = include_bytes!("../tests/data/attest/vcek_milan.der");
    const TURIN_VCEK: &[u8] = include_bytes!("../tests/data/attest/vcek_turin.der");
    const TDX_QUOTE: &[u8] = include_bytes!("../tests/data/attest/tdx_quote");
    const TDX_COLLATERAL: &[u8] = include_bytes!("../tests/data/attest/tdx_quote_collateral.json");
    const TDX_OUTDATED: &[u8] = include_bytes!("../tests/data/attest/tdx_quote_outdated");
    const TDX_OUTDATED_COLLATERAL: &[u8] =
        include_bytes!("../tests/data/attest/tdx_quote_outdated_collateral.json");

    fn milan_report() -> Vec<u8> {
        let text = String::from_utf8(MILAN_REPORT_HEX.to_vec()).unwrap();
        data_encoding::HEXUPPER_PERMISSIVE
            .decode(text.trim().as_bytes())
            .unwrap()
    }

    fn report_data_of(report: &[u8]) -> [u8; 64] {
        report[0x50..0x90].try_into().unwrap()
    }

    #[test]
    fn a_recorded_snp_report_checks_out_with_its_vcek() {
        let report = milan_report();
        let rd = report_data_of(&report);
        let c = check_snp(&report, MILAN_VCEK, &rd).unwrap();
        assert_eq!(c.tee, TeeKind::SevSnp);
        assert_eq!(c.measurement, hex(&report[0x90..0xc0]));
        assert!(c.detail.starts_with("Milan"), "{}", c.detail);
    }

    #[test]
    fn forged_or_replayed_snp_reports_get_nothing() {
        let report = milan_report();
        let rd = report_data_of(&report);
        // Another run's report data (a replay, or a key the guest does not
        // hold).
        let mut other = rd;
        other[0] ^= 1;
        let e = check_snp(&report, MILAN_VCEK, &other).unwrap_err();
        assert!(format!("{e:#}").contains("report data"), "{e:#}");
        // A changed measurement, policy or report data byte breaks the
        // signature.
        for at in [0x08, 0x50, 0x90, 0x1a0] {
            let mut bad = report.clone();
            bad[at] ^= 0x80;
            let rd = report_data_of(&bad);
            assert!(check_snp(&bad, MILAN_VCEK, &rd).is_err(), "byte {at:#x}");
        }
        // A VCEK of another chip (and generation).
        assert!(check_snp(&report, TURIN_VCEK, &rd).is_err());
        // Not a certificate at all, or not a report.
        assert!(check_snp(&report, b"not a certificate", &rd).is_err());
        assert!(check_snp(&report[..100], MILAN_VCEK, &rd).is_err());
    }

    #[test]
    fn a_vcek_signed_by_its_own_key_is_not_amd() {
        // Swap the VCEK's issuer signature bits: the chain to AMD's root
        // breaks even though the certificate still parses.
        let mut vcek = MILAN_VCEK.to_vec();
        let n = vcek.len();
        vcek[n - 10] ^= 0xff;
        let report = milan_report();
        let e = check_snp(&report, &vcek, &report_data_of(&report)).unwrap_err();
        assert!(format!("{e:#}").contains("not signed by AMD"), "{e:#}");
    }

    #[test]
    fn the_vcek_is_found_in_an_extended_report() {
        // ARK, then VCEK, then the end.
        let mut table = Vec::new();
        let guid_ark = [
            0xc0, 0xb4, 0x06, 0xa4, 0xa8, 0x03, 0x49, 0x52, 0x97, 0x43, 0x3f, 0xb6, 0x01, 0x4c,
            0xd0, 0xae,
        ];
        let guid_vcek = [
            0x63, 0xda, 0x75, 0x8d, 0xe6, 0x64, 0x45, 0x64, 0xad, 0xc5, 0xf4, 0xb9, 0x3b, 0xe8,
            0xac, 0xcd,
        ];
        let start = 3 * 24;
        for (guid, at, len) in [
            (guid_ark, start as u32, 4u32),
            (guid_vcek, start as u32 + 4, MILAN_VCEK.len() as u32),
        ] {
            table.extend_from_slice(&guid);
            table.extend_from_slice(&at.to_le_bytes());
            table.extend_from_slice(&len.to_le_bytes());
        }
        table.extend_from_slice(&[0u8; 24]);
        table.extend_from_slice(b"ark!");
        table.extend_from_slice(MILAN_VCEK);
        assert_eq!(vcek_from_aux(&table).as_deref(), Some(MILAN_VCEK));
        assert_eq!(vcek_from_aux(&table[..24 * 3]), None);
        assert_eq!(vcek_from_aux(&[]), None);
    }

    fn collateral(json: &[u8]) -> dcap_qvl::QuoteCollateralV3 {
        serde_json::from_slice(json).unwrap()
    }

    /// A time inside the collateral's validity (its TCB info and QE
    /// identity issue dates and next updates).
    fn valid_time(c: &dcap_qvl::QuoteCollateralV3) -> u64 {
        let window = |j: &str| {
            let v: serde_json::Value = serde_json::from_str(j).unwrap();
            let t = |k: &str| chrono_like(v[k].as_str().unwrap());
            (t("issueDate"), t("nextUpdate"))
        };
        let (a, b) = window(&c.tcb_info);
        let (c2, d) = window(&c.qe_identity);
        let from = a.max(c2);
        let to = b.min(d);
        assert!(from < to);
        from + (to - from) / 2
    }

    /// Seconds since 1970 of an RFC 3339 UTC time (`2025-01-01T00:00:00Z`).
    fn chrono_like(s: &str) -> u64 {
        let s = s.trim_end_matches('Z');
        let (date, time) = s.split_once('T').unwrap();
        let d: Vec<i64> = date.split('-').map(|x| x.parse().unwrap()).collect();
        let t: Vec<i64> = time
            .split(':')
            .map(|x| x.split('.').next().unwrap().parse().unwrap())
            .collect();
        // Days from civil (Howard Hinnant's algorithm).
        let (y, m, day) = (d[0] - (d[1] <= 2) as i64, d[1], d[2]);
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era * 146097 + doe - 719468;
        (days * 86400 + t[0] * 3600 + t[1] * 60 + t[2]) as u64
    }

    fn quote_report_data(quote: &[u8]) -> [u8; 64] {
        let parsed = dcap_qvl::quote::Quote::parse(quote).unwrap();
        parsed.report.as_td10().unwrap().report_data
    }

    #[test]
    fn a_recorded_tdx_quote_checks_out_with_its_collateral() {
        let c = collateral(TDX_COLLATERAL);
        let now = valid_time(&c);
        let rd = quote_report_data(TDX_QUOTE);
        let checked = check_tdx(TDX_QUOTE, &c, now, &rd).unwrap();
        assert_eq!(checked.tee, TeeKind::Tdx);
        assert_eq!(checked.measurement.len(), 96);
        assert!(
            checked.detail.starts_with("TCB UpToDate"),
            "{}",
            checked.detail
        );
    }

    #[test]
    fn forged_replayed_or_outdated_tdx_quotes_get_nothing() {
        let c = collateral(TDX_COLLATERAL);
        let now = valid_time(&c);
        let rd = quote_report_data(TDX_QUOTE);
        let mut other = rd;
        other[63] ^= 1;
        let e = check_tdx(TDX_QUOTE, &c, now, &other).unwrap_err();
        assert!(format!("{e:#}").contains("report data"), "{e:#}");
        // A changed byte in the TD report (MRTD, then the report data).
        for at in [48 + 136, 48 + 520] {
            let mut bad = TDX_QUOTE.to_vec();
            bad[at] ^= 1;
            assert!(check_tdx(&bad, &c, now, &rd).is_err(), "byte {at}");
        }
        // Collateral past its next update.
        assert!(check_tdx(TDX_QUOTE, &c, now + 400 * 86400, &rd).is_err());
        // A platform whose TCB is out of date (below every level Intel
        // lists, or listed as out of date).
        let old = collateral(TDX_OUTDATED_COLLATERAL);
        let rd = quote_report_data(TDX_OUTDATED);
        let e = check_tdx(TDX_OUTDATED, &old, valid_time(&old), &rd).unwrap_err();
        let e = format!("{e:#}");
        assert!(e.contains("OutOfDate") || e.contains("TCB level"), "{e}");
    }

    #[test]
    fn test_evidence_needs_the_insecure_flag() {
        let ev = Evidence {
            tee: TeeKind::Test,
            report: vec![1; 48],
            aux: vec![],
            key: [2; 32],
            report_data: vec![3; 64],
        };
        assert!(check_test(&ev, &[3; 64], false).is_err());
        assert!(check_test(&ev, &[4; 64], true).is_err());
        let c = check_test(&ev, &[3; 64], true).unwrap();
        assert_eq!(c.measurement, hex(&[1; 48]));
    }

    #[test]
    fn percent_decoding() {
        assert_eq!(
            percent_decode("-----BEGIN%20CERT%0A-----"),
            "-----BEGIN CERT\n-----"
        );
        assert_eq!(percent_decode("100%"), "100%");
    }
}
