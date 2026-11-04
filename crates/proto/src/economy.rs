//! Credits, the fixed price list and the settlement policy.
//!
//! Phase 2 keeps accounts in credits, an internal unit with no payment rail
//! behind it. Amounts are integers in micro-credits so sums never drift.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::job::Executor;

/// An amount in micro-credits (1 credit = 1,000,000). Signed so balances
/// can show an overdraft.
pub type Micros = i64;

pub const MICROS_PER_CREDIT: i64 = 1_000_000;

/// Formats micro-credits as a decimal number of credits, trimming trailing
/// zeros: `12500000` -> `12.5`, `42` -> `0.000042`.
pub fn format_credits(m: Micros) -> String {
    let sign = if m < 0 { "-" } else { "" };
    let abs = m.unsigned_abs();
    let whole = abs / MICROS_PER_CREDIT as u64;
    let frac = abs % MICROS_PER_CREDIT as u64;
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    let frac = format!("{frac:06}");
    format!("{sign}{whole}.{}", frac.trim_end_matches('0'))
}

/// Parses a non-negative decimal number of credits with at most six
/// decimal places.
pub fn parse_credits(s: &str) -> Result<Micros> {
    let s = s.trim();
    ensure!(!s.is_empty(), "empty amount");
    ensure!(!s.starts_with('-'), "amount must not be negative");
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    ensure!(
        whole.chars().all(|c| c.is_ascii_digit()) && frac.chars().all(|c| c.is_ascii_digit()),
        "invalid amount {s:?}"
    );
    ensure!(
        frac.len() <= 6,
        "amounts have at most 6 decimal places: {s:?}"
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
        format!("{frac:0<6}").parse()?
    };
    whole
        .checked_mul(MICROS_PER_CREDIT)
        .and_then(|w| w.checked_add(frac))
        .context("amount too large")
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
}

/// The fixed price list. All prices in micro-credits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceList {
    /// Per billion fuel units of a WASM task. Fuel counts instructions, so
    /// a slow machine earns the same as a fast one for the same work, and a
    /// replica can check the number.
    pub wasm_per_gfuel: Micros,
    /// Per core-hour of a Docker task's wall time (its `cpus` limit, one
    /// core when unset).
    pub docker_cpu_hour: Micros,
    /// Per GiB-hour of reserved memory, for both executors.
    pub memory_gib_hour: Micros,
}

impl Default for PriceList {
    fn default() -> Self {
        Self {
            wasm_per_gfuel: 10_000,
            docker_cpu_hour: 50_000,
            memory_gib_hour: 5_000,
        }
    }
}

const MS_PER_HOUR: u128 = 3_600_000;
const GIB: u128 = 1 << 30;

impl PriceList {
    /// The price of one run, rounded down to whole micro-credits.
    pub fn charge(&self, m: &Metered) -> Micros {
        let price = |p: Micros| p.max(0) as u128;
        let wall = m.wall_ms as u128;
        let compute = match m.executor {
            Executor::Wasm => m.fuel_used as u128 * price(self.wasm_per_gfuel) / 1_000_000_000,
            Executor::Docker => {
                let cpu = m.cpu_millis.unwrap_or(1000) as u128;
                cpu * wall * price(self.docker_cpu_hour) / (1000 * MS_PER_HOUR)
            }
        };
        let memory =
            m.memory_bytes as u128 * wall * price(self.memory_gib_hour) / (GIB * MS_PER_HOUR);
        (compute + memory).min(i64::MAX as u128) as Micros
    }
}

/// Price list plus the rules for holding and checking provider earnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub prices: PriceList,
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
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            prices: PriceList::default(),
            hold_secs: 3600,
            verify_permille: 100,
            max_strikes: 3,
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
            self.prices.wasm_per_gfuel >= 0
                && self.prices.docker_cpu_hour >= 0
                && self.prices.memory_gib_hour >= 0,
            "prices must not be negative"
        );
        ensure!(
            self.verify_permille <= 1000,
            "verify rate must be at most 1"
        );
        ensure!(self.max_strikes >= 1, "max strikes must be at least 1");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credits_roundtrip() {
        assert_eq!(parse_credits("12.5").unwrap(), 12_500_000);
        assert_eq!(parse_credits("0.000042").unwrap(), 42);
        assert_eq!(parse_credits(".5").unwrap(), 500_000);
        assert_eq!(parse_credits("3").unwrap(), 3_000_000);
        for bad in ["", "-1", "1.0000001", "abc", "1.2.3", "."] {
            assert!(parse_credits(bad).is_err(), "{bad:?}");
        }
        assert_eq!(format_credits(12_500_000), "12.5");
        assert_eq!(format_credits(42), "0.000042");
        assert_eq!(format_credits(-1_000_000), "-1");
        assert_eq!(format_credits(0), "0");
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

    #[test]
    fn pricing() {
        let prices = PriceList {
            wasm_per_gfuel: 1_000_000,
            docker_cpu_hour: 2_000_000,
            memory_gib_hour: 1_000_000,
        };
        // 3 billion fuel, no memory time.
        let wasm = Metered {
            executor: Executor::Wasm,
            fuel_used: 3_000_000_000,
            wall_ms: 0,
            memory_bytes: 1 << 30,
            cpu_millis: None,
        };
        assert_eq!(prices.charge(&wasm), 3_000_000);
        // Half an hour on 1.5 cores with 2 GiB reserved.
        let docker = Metered {
            executor: Executor::Docker,
            fuel_used: 0,
            wall_ms: 1_800_000,
            memory_bytes: 2 << 30,
            cpu_millis: Some(1500),
        };
        assert_eq!(prices.charge(&docker), 1_500_000 + 1_000_000);
    }
}
