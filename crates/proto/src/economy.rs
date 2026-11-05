//! Compute units (CU), the rate table and the settlement policy.
//!
//! One CU is one reference vCPU (CPU score 1000, see [`crate::perf`]) used
//! for one hour. Every other resource has a rate in CU, which the cluster
//! owner can change. The engine knows only CU; what a CU is worth in money
//! is not its business.
//!
//! Ledger amounts are integers in milli-CU, so sums never drift. A single
//! run is priced in micro-CU first, and what does not make a whole milli-CU
//! is carried to the next run between the same two accounts, so many short
//! runs add up exactly.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    job::Executor,
    perf::{MAX_SCORE, MIN_SCORE, REFERENCE_FUEL_PER_SEC, REFERENCE_SCORE},
};

/// A ledger amount in milli-CU (1 CU = 1,000). Signed so balances can go
/// below zero (an overdraft, or a member's debt in mutual mode).
pub type MilliCu = i64;
/// A rate or an exact charge in micro-CU (1 CU = 1,000,000).
pub type MicroCu = i64;

pub const MILLI_PER_CU: i64 = 1_000;
pub const MICRO_PER_CU: i64 = 1_000_000;
pub const MICRO_PER_MILLI: i64 = 1_000;

fn format_fixed(v: i64, places: u32) -> String {
    let scale = 10u64.pow(places);
    let sign = if v < 0 { "-" } else { "" };
    let abs = v.unsigned_abs();
    let (whole, frac) = (abs / scale, abs % scale);
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    let frac = format!("{frac:0width$}", width = places as usize);
    format!("{sign}{whole}.{}", frac.trim_end_matches('0'))
}

fn parse_fixed(s: &str, places: u32) -> Result<i64> {
    let s = s.trim();
    ensure!(!s.is_empty(), "empty amount");
    ensure!(!s.starts_with('-'), "amount must not be negative");
    let s = s.strip_suffix("CU").unwrap_or(s).trim();
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    ensure!(
        whole.chars().all(|c| c.is_ascii_digit()) && frac.chars().all(|c| c.is_ascii_digit()),
        "invalid amount {s:?}"
    );
    ensure!(
        frac.len() <= places as usize,
        "amounts have at most {places} decimal places: {s:?}"
    );
    ensure!(
        !whole.is_empty() || !frac.is_empty(),
        "invalid amount {s:?}"
    );
    let whole: i64 = if whole.is_empty() {
        0
    } else {
        whole.parse().context("amount too large")?
    };
    let frac: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<width$}", width = places as usize).parse()?
    };
    whole
        .checked_mul(10i64.pow(places))
        .and_then(|w| w.checked_add(frac))
        .context("amount too large")
}

/// Formats milli-CU as CU, trimming trailing zeros: `1500` -> `1.5`.
pub fn format_cu(m: MilliCu) -> String {
    format_fixed(m, 3)
}

/// Parses a non-negative amount of CU with at most three decimal places
/// (`12.5`, `0.001`, `3 CU`) into milli-CU.
pub fn parse_cu(s: &str) -> Result<MilliCu> {
    parse_fixed(s, 3)
}

/// Formats a micro-CU rate as CU: `250000` -> `0.25`.
pub fn format_rate(u: MicroCu) -> String {
    format_fixed(u, 6)
}

/// Parses a rate in CU with at most six decimal places into micro-CU.
pub fn parse_rate(s: &str) -> Result<MicroCu> {
    parse_fixed(s, 6)
}

/// Parses a sampling rate given as a fraction (`0.1`) or a percentage
/// (`10%`) into parts per thousand.
pub fn parse_rate_permille(s: &str) -> Result<u32> {
    let s = s.trim();
    let value: f64 = if let Some(p) = s.strip_suffix('%') {
        p.trim().parse::<f64>().context("invalid percentage")? / 100.0
    } else {
        s.parse().context("invalid rate")?
    };
    if !(0.0..=1.0).contains(&value) {
        bail!("rate must be between 0 and 1 (or 0% and 100%)");
    }
    Ok((value * 1000.0).round() as u32)
}

/// Account names: 1 to 32 characters of `a-z`, `0-9`, `-`, `_`.
pub fn validate_account(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 32,
        "account name must be 1 to 32 characters"
    );
    ensure!(
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_'),
        "account name may use only a-z, 0-9, '-' and '_': {name:?}"
    );
    Ok(())
}

/// What one run of one task used, as far as pricing is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metered {
    pub executor: Executor,
    pub fuel_used: u64,
    pub wall_ms: u64,
    /// Reserved memory (the job's limit).
    pub memory_bytes: u64,
    /// CPU limit in thousandths of a core; `None` counts as one core.
    pub cpu_millis: Option<u32>,
    /// CPU score of the node's vCPUs (1000 = the reference vCPU).
    pub cpu_score: u32,
}

/// The CU rate table: what each resource costs in micro-CU. CPU time is
/// not in it: one reference vCPU-hour is one CU by definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CuRates {
    /// Per GiB-hour of reserved memory.
    pub memory_gib_hour: MicroCu,
    /// Per GiB-month of stored data (the cluster file store, phase 8).
    pub storage_gib_month: MicroCu,
    /// Per GiB sent out of the cluster (phase 8 onwards).
    pub egress_gib: MicroCu,
}

impl Default for CuRates {
    /// One vCPU with 4 GiB for an hour is 2 CU, with 2 GiB 1.5 CU.
    fn default() -> Self {
        Self {
            memory_gib_hour: 250_000,
            storage_gib_month: 500_000,
            egress_gib: 100_000,
        }
    }
}

const NS_PER_HOUR: u128 = 3_600_000_000_000;
const GIB: u128 = 1 << 30;

/// Whole vCPUs a microVM gets for a CPU limit in thousandths of a core:
/// rounded up, one when unset.
pub fn vcpus(cpu_millis: Option<u32>) -> u32 {
    cpu_millis.map_or(1, |m| m.div_ceil(1000).max(1))
}

/// Clamps a node's CPU score to what billing accepts.
pub fn billing_score(score: u32) -> u32 {
    score.clamp(MIN_SCORE, MAX_SCORE)
}

impl CuRates {
    /// The reference time a run stands for, in nanoseconds: its wall time
    /// scaled by the node's CPU score (a node twice as fast as the
    /// reference finishes in half the time and is billed the same), or for
    /// WASM, the time its fuel takes on the reference vCPU.
    pub fn reference_ns(m: &Metered) -> u128 {
        match m.executor {
            Executor::Wasm => m.fuel_used as u128 * 1_000_000_000 / REFERENCE_FUEL_PER_SEC as u128,
            Executor::Docker | Executor::Microvm => {
                m.wall_ms as u128 * 1_000_000 * billing_score(m.cpu_score) as u128
                    / REFERENCE_SCORE as u128
            }
        }
    }

    /// The exact price of one run in micro-CU (rounded down).
    pub fn charge(&self, m: &Metered) -> MicroCu {
        let rate = |p: MicroCu| p.max(0) as u128;
        let t = Self::reference_ns(m);
        // vCPU-thousandths: a WASM task uses one core.
        let cpu = match m.executor {
            Executor::Wasm => 1000,
            Executor::Docker => m.cpu_millis.unwrap_or(1000) as u128,
            Executor::Microvm => vcpus(m.cpu_millis) as u128 * 1000,
        };
        let compute = cpu * t * MICRO_PER_CU as u128 / (1000 * NS_PER_HOUR);
        let memory = (m.memory_bytes as u128 * t / GIB).saturating_mul(rate(self.memory_gib_hour))
            / NS_PER_HOUR;
        (compute + memory).min(i64::MAX as u128) as MicroCu
    }

    /// CU per hour of `vcpus` reference vCPUs with `memory_gib` reserved,
    /// in micro-CU (for showing examples).
    pub fn per_hour(&self, vcpus: u32, memory_gib: u32) -> MicroCu {
        vcpus as i64 * MICRO_PER_CU + memory_gib as i64 * self.memory_gib_hour
    }
}

/// How accounts get CU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LedgerMode {
    /// The owner grants CU (`pumasi cu add`); a job's account needs a
    /// positive balance to use other people's machines.
    #[default]
    Prepaid,
    /// Money-free pooling (품앗이): nobody grants CU. Lending a machine
    /// earns CU, using others' machines spends them, and a balance may go
    /// down to minus `credit_limit` (milli-CU). The balances always add up
    /// to zero (plus whatever was granted before switching).
    Mutual { credit_limit: MilliCu },
}

impl LedgerMode {
    /// The balance an account must stay above to use other accounts'
    /// machines.
    pub fn floor(&self) -> MilliCu {
        match self {
            LedgerMode::Prepaid => 0,
            LedgerMode::Mutual { credit_limit } => -credit_limit,
        }
    }
}

/// Rate table plus the rules for holding and checking provider earnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub rates: CuRates,
    /// How long a provider's earnings stay held after a receipt before they
    /// become spendable. This is the window in which a failed check can
    /// still claw them back.
    pub hold_secs: u64,
    /// Base share (per thousand) of provider-run WASM tasks that are re-run
    /// on another node to check the result. New providers and providers
    /// with a failed check are sampled more often.
    pub verify_permille: u32,
    /// Failed checks after which a provider node gets no more tasks.
    pub max_strikes: u32,
    pub ledger: LedgerMode,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            rates: CuRates::default(),
            hold_secs: 3600,
            verify_permille: 100,
            max_strikes: 3,
            ledger: LedgerMode::Prepaid,
        }
    }
}

/// Checks that pass before a provider node counts as established; until
/// then it is sampled at least [`NEW_NODE_PERMILLE`].
pub const WARMUP_PASSES: u32 = 10;
pub const NEW_NODE_PERMILLE: u32 = 500;

impl Policy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.rates.memory_gib_hour >= 0
                && self.rates.storage_gib_month >= 0
                && self.rates.egress_gib >= 0,
            "rates must not be negative"
        );
        ensure!(
            self.verify_permille <= 1000,
            "verify rate must be at most 1"
        );
        ensure!(self.max_strikes >= 1, "max strikes must be at least 1");
        if let LedgerMode::Mutual { credit_limit } = self.ledger {
            ensure!(credit_limit >= 0, "credit limit must not be negative");
        }
        Ok(())
    }

    /// Sampling rate (per thousand) for a provider node with the given
    /// record. A rate of zero turns checking off entirely.
    pub fn sample_permille(&self, passes: u32, strikes: u32) -> u32 {
        if self.verify_permille == 0 {
            0
        } else if strikes > 0 {
            1000
        } else if passes < WARMUP_PASSES {
            self.verify_permille.max(NEW_NODE_PERMILLE)
        } else {
            self.verify_permille
        }
    }
}

/// Exact conversion of an amount in the phase 2 credits (micro-credits) to
/// milli-CU, as a numerator over `cpu_hour` (the old price of one
/// core-hour, in micro-credits): one core-hour's price becomes one CU.
pub fn credits_to_milli_cu_num(micro_credits: i64, cpu_hour: i64) -> (i128, i128) {
    (
        micro_credits as i128 * MILLI_PER_CU as i128,
        cpu_hour.max(1) as i128,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perf::REFERENCE_FUEL_PER_SEC;

    #[test]
    fn cu_amounts_roundtrip() {
        assert_eq!(parse_cu("12.5").unwrap(), 12_500);
        assert_eq!(parse_cu("0.001").unwrap(), 1);
        assert_eq!(parse_cu(".5").unwrap(), 500);
        assert_eq!(parse_cu("3 CU").unwrap(), 3_000);
        for bad in ["", "-1", "1.0001", "abc", "1.2.3", "."] {
            assert!(parse_cu(bad).is_err(), "{bad:?}");
        }
        assert_eq!(format_cu(12_500), "12.5");
        assert_eq!(format_cu(1), "0.001");
        assert_eq!(format_cu(-1_000), "-1");
        assert_eq!(format_cu(0), "0");
        assert_eq!(parse_rate("0.25").unwrap(), 250_000);
        assert_eq!(format_rate(250_000), "0.25");
        assert_eq!(format_rate(1), "0.000001");
    }

    #[test]
    fn rates() {
        assert_eq!(parse_rate_permille("0.1").unwrap(), 100);
        assert_eq!(parse_rate_permille("25%").unwrap(), 250);
        assert_eq!(parse_rate_permille("1").unwrap(), 1000);
        assert!(parse_rate_permille("1.5").is_err());
        let p = Policy::default();
        assert_eq!(p.sample_permille(0, 0), 500);
        assert_eq!(p.sample_permille(50, 0), 100);
        assert_eq!(p.sample_permille(50, 1), 1000);
        let off = Policy {
            verify_permille: 0,
            ..p
        };
        assert_eq!(off.sample_permille(0, 2), 0);
    }

    fn docker(wall_ms: u64, memory_gib: u64, cpu_millis: Option<u32>, score: u32) -> Metered {
        Metered {
            executor: Executor::Docker,
            fuel_used: 0,
            wall_ms,
            memory_bytes: memory_gib << 30,
            cpu_millis,
            cpu_score: score,
        }
    }

    #[test]
    fn one_vcpu_hour_is_one_cu_and_memory_has_its_own_rate() {
        let r = CuRates::default();
        // The examples from the roadmap: 1 vCPU + 2 GiB = 1.5 CU per hour,
        // 1 vCPU + 4 GiB = 2 CU.
        assert_eq!(r.charge(&docker(3_600_000, 0, None, 1000)), 1_000_000);
        assert_eq!(r.charge(&docker(3_600_000, 2, None, 1000)), 1_500_000);
        assert_eq!(r.charge(&docker(3_600_000, 4, None, 1000)), 2_000_000);
        assert_eq!(r.per_hour(1, 2), 1_500_000);
        // Half an hour on 1.5 cores with 2 GiB.
        assert_eq!(
            r.charge(&docker(1_800_000, 2, Some(1500), 1000)),
            750_000 + 250_000
        );
        // A microVM is billed for whole vCPUs.
        let vm = Metered {
            executor: Executor::Microvm,
            ..docker(1_800_000, 2, Some(1500), 1000)
        };
        assert_eq!(r.charge(&vm), 1_000_000 + 250_000);
        assert_eq!(vcpus(None), 1);
        assert_eq!(vcpus(Some(250)), 1);
        assert_eq!(vcpus(Some(2001)), 3);
    }

    #[test]
    fn the_same_work_costs_the_same_on_a_slow_or_fast_machine() {
        let r = CuRates::default();
        // The same job: an hour on the reference vCPU, two hours on a
        // machine half as fast, 40 minutes on one 1.5 times as fast.
        let reference = r.charge(&docker(3_600_000, 4, None, 1000));
        let slow = r.charge(&docker(7_200_000, 4, None, 500));
        let fast = r.charge(&docker(2_400_000, 4, None, 1500));
        assert_eq!(reference, 2_000_000);
        assert_eq!(slow, reference);
        assert_eq!(fast, reference);
        // A measured job never matches exactly; 10% slower than its score
        // says stays within 10%.
        let noisy = r.charge(&docker(7_920_000, 4, None, 500));
        assert!((noisy - reference).abs() * 10 <= reference);
        // Scores outside the billing range are clamped.
        assert_eq!(
            r.charge(&docker(3_600_000, 0, None, 1)),
            r.charge(&docker(3_600_000, 0, None, MIN_SCORE))
        );
        // WASM is billed by fuel, wherever it ran.
        let wasm = Metered {
            executor: Executor::Wasm,
            fuel_used: REFERENCE_FUEL_PER_SEC * 3600,
            wall_ms: 1,
            memory_bytes: 0,
            cpu_millis: None,
            cpu_score: 3000,
        };
        assert_eq!(r.charge(&wasm), 1_000_000);
        assert_eq!(
            r.charge(&Metered {
                wall_ms: 99_999_999,
                cpu_score: 200,
                ..wasm
            }),
            1_000_000
        );
    }

    #[test]
    fn mutual_mode_lets_balances_go_negative_to_the_limit() {
        assert_eq!(LedgerMode::Prepaid.floor(), 0);
        assert_eq!(
            LedgerMode::Mutual {
                credit_limit: 50_000
            }
            .floor(),
            -50_000
        );
        let bad = Policy {
            ledger: LedgerMode::Mutual { credit_limit: -1 },
            ..Policy::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn credits_convert_at_the_old_cpu_price() {
        // With the old default (0.05 credits per core-hour), 1 credit is
        // 20 CU.
        let (n, d) = credits_to_milli_cu_num(1_000_000, 50_000);
        assert_eq!(n / d, 20_000);
    }
}
