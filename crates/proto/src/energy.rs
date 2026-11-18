//! Energy and electricity (1.22): what each run drew, what a node's
//! electricity costs against the CU it earns, the temperature at which a
//! node stops taking tasks, and the estimate `pumasi bench --estimate`
//! prints before a machine joins.
//!
//! Power is what the machine's sensors read: the CPU package (RAPL on
//! Linux, the SMC or IOReport on Macs, a hardware monitor on Windows) and
//! the GPUs. It is not what the wall socket gives (no PSU losses, disks,
//! fans or screen), so figures are a floor, and they are shown, never
//! billed.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::economy::{CuRates, MICRO_PER_CU, MicroCu, billing_score};
use crate::perf::REFERENCE_SCORE;

/// What one run drew on its node, in milliwatt-hours. The agent
/// integrates the CPU package's power over the run, shared among the
/// node's runs by their CPU time (other programs on the machine keep
/// their share), and the power of the run's GPUs, shared equally among
/// the runs on the same GPU. `None`: the node cannot read that power.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEnergy {
    pub cpu_mwh: Option<u64>,
    pub gpu_mwh: Option<u64>,
}

impl RunEnergy {
    pub fn is_empty(&self) -> bool {
        self.cpu_mwh.is_none() && self.gpu_mwh.is_none()
    }

    /// CPU and GPU together, where either was measured.
    pub fn total_mwh(&self) -> Option<u64> {
        match (self.cpu_mwh, self.gpu_mwh) {
            (None, None) => None,
            (a, b) => Some(a.unwrap_or(0).saturating_add(b.unwrap_or(0))),
        }
    }

    /// Adds another run's energy, where either measured it.
    pub fn add(&mut self, o: &RunEnergy) {
        fn add(a: &mut Option<u64>, b: Option<u64>) {
            if let Some(b) = b {
                *a = Some(a.unwrap_or(0).saturating_add(b));
            }
        }
        add(&mut self.cpu_mwh, o.cpu_mwh);
        add(&mut self.gpu_mwh, o.gpu_mwh);
    }

    /// `1.2 Wh (CPU 1 Wh, GPU 0.2 Wh)`, or empty.
    pub fn describe(&self) -> String {
        let Some(total) = self.total_mwh() else {
            return String::new();
        };
        match (self.cpu_mwh, self.gpu_mwh) {
            (Some(c), Some(g)) => format!(
                "{} (CPU {}, GPU {})",
                format_wh(total),
                format_wh(c),
                format_wh(g)
            ),
            (Some(_), None) => format!("{} (CPU)", format_wh(total)),
            _ => format!("{} (GPU)", format_wh(total)),
        }
    }
}

/// Milliwatt-hours as `0.012 Wh`, `3.4 Wh`, `120 Wh` or `2.35 kWh`.
pub fn format_wh(mwh: u64) -> String {
    let wh = mwh as f64 / 1000.0;
    if wh >= 1000.0 {
        format!("{:.2} kWh", wh / 1000.0)
    } else if wh >= 100.0 {
        format!("{wh:.0} Wh")
    } else if wh >= 10.0 {
        format!("{wh:.1} Wh")
    } else if wh >= 0.1 {
        format!("{wh:.2} Wh")
    } else {
        format!("{wh:.3} Wh")
    }
}

/// Milli-won as `1,234 won` (whole won) or `0.42 won` under ten.
pub fn format_won(mwon: u64) -> String {
    let won = mwon as f64 / 1000.0;
    if won < 10.0 {
        return format!("{won:.2} won");
    }
    let whole = won.round() as u64;
    let digits = whole.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    format!("{out} won")
}

/// Hours of the day when electricity is cheaper, with their price: whole
/// hours of the control plane's local time, `23-07` meaning 23:00 to
/// 07:00. KEPCO's time-of-use plans change price on the hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheapHours {
    /// First cheap hour, 0 to 23.
    pub from: u8,
    /// The hour cheap electricity ends, 0 to 23 (not cheap itself).
    pub to: u8,
    /// Won per kWh in those hours.
    pub price: u32,
}

impl CheapHours {
    /// Whether the hour starting at `h` (0 to 23) is cheap.
    pub fn covers(&self, h: u8) -> bool {
        if self.from <= self.to {
            (self.from..self.to).contains(&h)
        } else {
            h >= self.from || h < self.to
        }
    }

    /// `23-07` or `23:00-07:00`.
    pub fn parse_range(s: &str) -> Result<(u8, u8)> {
        let (a, b) = s
            .trim()
            .split_once('-')
            .with_context(|| format!("cheap hours look like 23-07 or 23:00-07:00, not {s:?}"))?;
        let hour = |t: &str| -> Result<u8> {
            let t = t.trim();
            let h = match t.split_once(':') {
                Some((h, m)) => {
                    ensure!(
                        m.trim() == "00",
                        "cheap hours start and end on the hour: {s:?}"
                    );
                    h
                }
                None => t,
            };
            let h: u8 = h
                .trim()
                .parse()
                .with_context(|| format!("invalid hour in {s:?}"))?;
            ensure!(h < 24, "hours are 0 to 23: {s:?}");
            Ok(h)
        };
        let (from, to) = (hour(a)?, hour(b)?);
        ensure!(from != to, "cheap hours need an end other than their start");
        Ok((from, to))
    }

    /// `23:00-07:00 at 80 won/kWh`.
    pub fn describe(&self) -> String {
        format!(
            "{:02}:00-{:02}:00 at {} won/kWh",
            self.from, self.to, self.price
        )
    }
}

/// The most a kWh may cost (won), against typing errors.
pub const MAX_PRICE: u32 = 100_000;
/// Temperatures `--max-temperature` takes, in °C.
pub const MAX_TEMP_RANGE: std::ops::RangeInclusive<u16> = 40..=120;
/// How long a node stays over (or, once paused, under) its limit before
/// it pauses (or takes tasks again).
pub const HOT_HOLD_SECS: u64 = 60;

/// A node's electricity and temperature settings, kept by the control
/// plane (`pumasi node set`, `[nodes.NAME]` in `cluster.toml`). The layout
/// is stored and on the wire: add fields only at the end, with
/// [`crate::wire::trailing`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEnergy {
    /// Won per kWh.
    pub price: Option<u32>,
    /// Cheaper hours, when the plan has them.
    pub cheap: Option<CheapHours>,
    /// Take no new tasks while a CPU or GPU sensor stays over this many
    /// °C for a minute ([`HOT_HOLD_SECS`]); with the agent's `--on-pause
    /// requeue` its running tasks move too.
    pub max_temp_c: Option<u16>,
}

impl NodeEnergy {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(p) = self.price {
            ensure!(p <= MAX_PRICE, "a kWh costs at most {MAX_PRICE} won here");
        }
        if let Some(c) = &self.cheap {
            ensure!(
                self.price.is_some(),
                "cheap hours need the normal price too (--electricity-price)"
            );
            ensure!(
                c.from < 24 && c.to < 24 && c.from != c.to,
                "invalid cheap hours"
            );
            ensure!(
                c.price <= MAX_PRICE,
                "a kWh costs at most {MAX_PRICE} won here"
            );
        }
        if let Some(t) = self.max_temp_c {
            ensure!(
                MAX_TEMP_RANGE.contains(&t),
                "--max-temperature takes {} to {} °C",
                MAX_TEMP_RANGE.start(),
                MAX_TEMP_RANGE.end()
            );
        }
        Ok(())
    }

    /// Won per kWh in the hour starting at `h`.
    pub fn price_at(&self, h: u8) -> Option<u32> {
        match &self.cheap {
            Some(c) if c.covers(h) => Some(c.price),
            _ => self.price,
        }
    }

    /// What `mwh_by_hour` (24 values, one per local hour) cost, in
    /// milli-won; `None` without a price.
    pub fn cost_mwon(&self, mwh_by_hour: &[u64]) -> Option<u64> {
        self.price?;
        Some(
            mwh_by_hour
                .iter()
                .enumerate()
                .map(|(h, mwh)| {
                    mwh.saturating_mul(self.price_at(h as u8).unwrap_or(0) as u64) / 1000
                })
                .sum(),
        )
    }

    /// `120 won/kWh, 23:00-07:00 at 80 won/kWh, pause over 85 °C`, or
    /// `none`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(p) = self.price {
            parts.push(format!("{p} won/kWh"));
        }
        if let Some(c) = &self.cheap {
            parts.push(c.describe());
        }
        if let Some(t) = self.max_temp_c {
            parts.push(format!("pause over {t} °C"));
        }
        if parts.is_empty() {
            "none".into()
        } else {
            parts.join(", ")
        }
    }
}

/// A change to [`NodeEnergy`]: `None` keeps a setting, `Some(None)`
/// removes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeEnergyPatch {
    pub price: Option<Option<u32>>,
    pub cheap: Option<Option<(u8, u8, u32)>>,
    pub max_temp_c: Option<Option<u16>>,
}

impl NodeEnergyPatch {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn apply(&self, e: &mut NodeEnergy) -> Result<()> {
        if let Some(p) = self.price {
            e.price = p;
            if p.is_none() {
                e.cheap = None;
            }
        }
        if let Some(c) = self.cheap {
            e.cheap = c.map(|(from, to, price)| CheapHours { from, to, price });
        }
        if let Some(t) = self.max_temp_c {
            e.max_temp_c = t;
        }
        e.validate()
    }
}

/// A price in won per kWh (`120`, `120.5` rounds), or `off`.
pub fn parse_price(s: &str) -> Result<Option<u32>> {
    if crate::monitor::is_off(s) {
        return Ok(None);
    }
    let t = s
        .trim()
        .trim_end_matches("won/kWh")
        .trim_end_matches("원")
        .trim();
    let v: f64 = t
        .parse()
        .with_context(|| format!("a price is won per kWh, like 120: {s:?}"))?;
    ensure!(
        v.is_finite() && v >= 0.0 && v <= MAX_PRICE as f64,
        "a price is 0 to {MAX_PRICE} won per kWh: {s:?}"
    );
    Ok(Some(v.round() as u32))
}

/// A temperature in °C (`85`, `85C`, `85°C`), or `off`.
pub fn parse_temp(s: &str) -> Result<Option<u16>> {
    if crate::monitor::is_off(s) {
        return Ok(None);
    }
    let t = s
        .trim()
        .trim_end_matches(['C', 'c'])
        .trim_end_matches('°')
        .trim();
    let v: u16 = t
        .parse()
        .with_context(|| format!("a temperature is °C, like 85: {s:?}"))?;
    ensure!(
        MAX_TEMP_RANGE.contains(&v),
        "--max-temperature takes {} to {} °C",
        MAX_TEMP_RANGE.start(),
        MAX_TEMP_RANGE.end()
    );
    Ok(Some(v))
}

/// `23-07@80`: cheap hours and their price, or `off`.
pub fn parse_cheap(s: &str) -> Result<Option<(u8, u8, u32)>> {
    if crate::monitor::is_off(s) {
        return Ok(None);
    }
    let Some((range, price)) = s.split_once('@') else {
        bail!("cheap hours look like 23-07@80 (hours, then won per kWh): {s:?}");
    };
    let (from, to) = CheapHours::parse_range(range)?;
    let price = parse_price(price)?.context("cheap hours need a price")?;
    Ok(Some((from, to, price)))
}

/// One node's settings and temperature state, in [`EnergyView`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeEnergyView {
    pub id: String,
    pub name: String,
    pub account: String,
    pub settings: NodeEnergy,
    /// Paused for its temperature since then.
    pub hot_since_unix: Option<u64>,
    /// The hottest sensor in its latest sample.
    pub temp_c: Option<f32>,
    /// CPU package and GPU power in its latest sample, when it reads any.
    pub watts: Option<f32>,
}

/// One node's day: energy, what it cost, and the CU its runs were worth.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EnergyDay {
    /// The control plane's local date, `2026-11-18`.
    pub day: String,
    pub node: String,
    pub name: String,
    /// The account the node earns for.
    pub account: String,
    /// CPU package and GPUs, measured.
    pub mwh: u64,
    /// How long it was measured (power read and the node online).
    pub measured_secs: u32,
    /// At the node's price (now); `None` without one.
    pub cost_mwon: Option<u64>,
    /// What its runs were worth at the cluster's prices, billed or not
    /// (runs for the node's own account are free but still worth this).
    pub worth_ucu: MicroCu,
    /// CU it earned: what other accounts' runs moved to its account
    /// (milli-CU, held or not).
    pub earned_mcu: i64,
    pub runs: u32,
}

impl EnergyDay {
    /// Won per CU of worth, when both are known and non-zero.
    pub fn won_per_cu(&self) -> Option<f64> {
        let cost = self.cost_mwon? as f64 / 1000.0;
        (self.worth_ucu > 0).then(|| cost / (self.worth_ucu as f64 / MICRO_PER_CU as f64))
    }
}

/// `pumasi energy`: each node's days, newest first, and each node's
/// settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EnergyView {
    pub days: Vec<EnergyDay>,
    pub nodes: Vec<NodeEnergyView>,
    /// The control plane's UTC offset, which days and cheap hours follow
    /// (`+09:00`).
    pub utc_offset: String,
}

impl EnergyView {
    /// Days summed per account and date, newest first: (day, account,
    /// totals as an [`EnergyDay`] with empty node fields).
    pub fn by_account(&self) -> Vec<EnergyDay> {
        let mut out: Vec<EnergyDay> = Vec::new();
        for d in &self.days {
            match out
                .iter_mut()
                .find(|o| o.day == d.day && o.account == d.account)
            {
                Some(o) => {
                    o.mwh += d.mwh;
                    o.measured_secs = o.measured_secs.max(d.measured_secs);
                    o.cost_mwon = match (o.cost_mwon, d.cost_mwon) {
                        (None, None) => None,
                        (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
                    };
                    o.worth_ucu += d.worth_ucu;
                    o.earned_mcu += d.earned_mcu;
                    o.runs += d.runs;
                }
                None => out.push(EnergyDay {
                    node: String::new(),
                    name: String::new(),
                    ..d.clone()
                }),
            }
        }
        out.sort_by(|a, b| b.day.cmp(&a.day).then(a.account.cmp(&b.account)));
        out
    }
}

/// What a machine would earn and cost at full load
/// (`pumasi bench --estimate`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Estimate {
    pub vcpus: u32,
    /// Its CPU score per vCPU (1000 = the reference vCPU).
    pub score_per_vcpu: u32,
    /// Memory tasks could reserve: all but 2 GiB for the system.
    pub memory_gib: f64,
    pub cpu_ucu: MicroCu,
    pub memory_ucu: MicroCu,
    pub gpu_ucu: MicroCu,
    /// Power at full load, when measured or given.
    pub watts: Option<f64>,
    /// Won per kWh it was priced at.
    pub price: Option<u32>,
}

impl Estimate {
    /// CU per hour with every vCPU busy, the memory reserved and the GPUs
    /// held.
    pub fn new(
        rates: &CuRates,
        vcpus: u32,
        score_per_vcpu: u32,
        memory_bytes: u64,
        gpus: &[(u64, u64)],
    ) -> Self {
        let memory_gib = ((memory_bytes as f64 / (1u64 << 30) as f64) - 2.0).max(0.0);
        let cpu_ucu = vcpus as i64 * billing_score(score_per_vcpu) as i64 * MICRO_PER_CU
            / REFERENCE_SCORE as i64;
        let memory_ucu = (memory_gib * rates.memory_gib_hour.max(0) as f64) as i64;
        let gpu_ucu = gpus
            .iter()
            .map(|(gflops, vram)| rates.gpu_per_hour(*gflops, *vram))
            .sum();
        Self {
            vcpus,
            score_per_vcpu,
            memory_gib,
            cpu_ucu,
            memory_ucu,
            gpu_ucu,
            watts: None,
            price: None,
        }
    }

    pub fn cu_per_hour(&self) -> MicroCu {
        self.cpu_ucu + self.memory_ucu + self.gpu_ucu
    }

    /// Won per hour at full load.
    pub fn won_per_hour(&self) -> Option<f64> {
        Some(self.watts? / 1000.0 * self.price? as f64)
    }

    /// Won of electricity per CU earned.
    pub fn won_per_cu(&self) -> Option<f64> {
        let cu = self.cu_per_hour() as f64 / MICRO_PER_CU as f64;
        (cu > 0.0).then(|| self.won_per_hour().map(|w| w / cu))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cheap_hours_wrap_midnight() {
        let c = CheapHours {
            from: 23,
            to: 7,
            price: 80,
        };
        assert!(c.covers(23) && c.covers(0) && c.covers(6));
        assert!(!c.covers(7) && !c.covers(12) && !c.covers(22));
        let day = CheapHours {
            from: 10,
            to: 12,
            price: 1,
        };
        assert!(day.covers(10) && day.covers(11) && !day.covers(12));
        assert_eq!(CheapHours::parse_range("23:00-07:00").unwrap(), (23, 7));
        assert_eq!(CheapHours::parse_range("23-7").unwrap(), (23, 7));
        assert!(CheapHours::parse_range("23:30-07:00").is_err());
        assert!(CheapHours::parse_range("5-5").is_err());
        assert_eq!(parse_cheap("23-07@80").unwrap(), Some((23, 7, 80)));
        assert_eq!(parse_cheap("off").unwrap(), None);
        assert!(parse_cheap("23-07").is_err());
    }

    #[test]
    fn cost_follows_the_hours() {
        let e = NodeEnergy {
            price: Some(120),
            cheap: Some(CheapHours {
                from: 23,
                to: 7,
                price: 80,
            }),
            max_temp_c: None,
        };
        e.validate().unwrap();
        let mut hours = vec![0u64; 24];
        // 1 kWh at 03:00 (cheap) and 1 kWh at 12:00.
        hours[3] = 1_000_000;
        hours[12] = 1_000_000;
        assert_eq!(e.cost_mwon(&hours), Some(200_000));
        assert_eq!(NodeEnergy::default().cost_mwon(&hours), None);
        assert_eq!(format_won(200_000), "200 won");
        assert_eq!(format_won(1_234_567_000), "1,234,567 won");
        assert_eq!(format_won(420), "0.42 won");
        assert_eq!(e.describe(), "120 won/kWh, 23:00-07:00 at 80 won/kWh");
    }

    #[test]
    fn patches_and_limits() {
        let mut e = NodeEnergy::default();
        NodeEnergyPatch {
            price: Some(Some(120)),
            cheap: Some(Some((23, 7, 80))),
            max_temp_c: Some(Some(85)),
        }
        .apply(&mut e)
        .unwrap();
        assert_eq!(e.price_at(2), Some(80));
        assert_eq!(e.price_at(9), Some(120));
        // Removing the price removes the cheap hours with it.
        NodeEnergyPatch {
            price: Some(None),
            ..Default::default()
        }
        .apply(&mut e)
        .unwrap();
        assert_eq!((e.price, e.cheap, e.max_temp_c), (None, None, Some(85)));
        assert!(
            NodeEnergyPatch {
                cheap: Some(Some((1, 2, 3))),
                ..Default::default()
            }
            .apply(&mut NodeEnergy::default())
            .is_err()
        );
        assert_eq!(parse_temp("85°C").unwrap(), Some(85));
        assert!(parse_temp("200").is_err());
        assert_eq!(parse_price("120.4").unwrap(), Some(120));
        assert_eq!(parse_price("off").unwrap(), None);
        assert!(parse_price("-1").is_err());
    }

    #[test]
    fn energy_adds_and_prints() {
        let mut t = RunEnergy::default();
        assert!(t.is_empty() && t.describe().is_empty());
        t.add(&RunEnergy {
            cpu_mwh: Some(1500),
            gpu_mwh: None,
        });
        t.add(&RunEnergy {
            cpu_mwh: Some(500),
            gpu_mwh: Some(250),
        });
        assert_eq!(t.total_mwh(), Some(2250));
        assert_eq!(t.describe(), "2.25 Wh (CPU 2.00 Wh, GPU 0.25 Wh)");
        assert_eq!(format_wh(12), "0.012 Wh");
        assert_eq!(format_wh(123_456), "123 Wh");
        assert_eq!(format_wh(2_345_000), "2.35 kWh");
    }

    #[test]
    fn estimate_at_full_load() {
        let rates = CuRates::default();
        // 8 vCPUs at the reference score, 18 GiB (16 for tasks), one
        // RTX 3080-like GPU (29.8 TFLOPS, 10 GiB).
        let mut e = Estimate::new(&rates, 8, 1000, 18 << 30, &[(29_800, 10_240)]);
        assert_eq!(e.cpu_ucu, 8 * MICRO_PER_CU);
        assert_eq!(e.memory_ucu, 16 * 250_000);
        assert_eq!(e.gpu_ucu, 2_980_000 + 3_000_000);
        assert_eq!(e.cu_per_hour(), 17_980_000);
        assert_eq!(e.won_per_hour(), None);
        e.watts = Some(400.0);
        e.price = Some(120);
        assert_eq!(e.won_per_hour(), Some(48.0));
        let per_cu = e.won_per_cu().unwrap();
        assert!((per_cu - 48.0 / 17.98).abs() < 1e-9);
    }

    #[test]
    fn days_sum_per_account() {
        let day = |node: &str, account: &str, mwh, cost| EnergyDay {
            day: "2026-11-18".into(),
            node: node.into(),
            name: node.into(),
            account: account.into(),
            mwh,
            measured_secs: 100,
            cost_mwon: cost,
            worth_ucu: 1_000_000,
            earned_mcu: 10,
            runs: 1,
        };
        let v = EnergyView {
            days: vec![
                day("a", "stan", 1000, Some(120)),
                day("b", "stan", 500, None),
                day("c", "kim", 10, Some(1)),
            ],
            ..Default::default()
        };
        let per = v.by_account();
        assert_eq!(per.len(), 2);
        let stan = per.iter().find(|d| d.account == "stan").unwrap();
        assert_eq!((stan.mwh, stan.cost_mwon, stan.runs), (1500, Some(120), 2));
        assert_eq!(stan.worth_ucu, 2_000_000);
        assert!((stan.won_per_cu().unwrap() - 0.06).abs() < 1e-9);
    }
}
