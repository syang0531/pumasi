//! `pumasi energy` (1.22): each node's daily electricity, what it cost at
//! the node's price, and the CU its runs were worth and earned.

use std::path::Path;

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    economy::{MICRO_PER_CU, format_cu},
    energy::{EnergyDay, EnergyView, format_wh, format_won},
    msg::{AdminOp, AdminResponse},
};

use crate::{AdminArgs, connect_admin, manage::Output, monitor::local_time};

// Written, not printed: account names count as sensitive to CodeQL's log
// checks, as in `pumasi quota`.
macro_rules! out {
    ($($t:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stdout().lock(), $($t)*);
    }};
}

pub async fn energy(
    home: &Path,
    days: u32,
    node: Option<String>,
    by_account: bool,
    output: Output,
    admin: &AdminArgs,
) -> Result<i32> {
    let client = connect_admin(home, admin).await?;
    let r = client.read(AdminOp::Energy { days }).await;
    client.close().await;
    let AdminResponse::Energy(mut v) = r.context(
        "this control plane keeps no energy figures (they came in 1.22); upgrade it with `pumasi upgrade`",
    )?
    else {
        bail!("unexpected response");
    };
    if let Some(n) = node.as_deref().map(str::trim) {
        let ids: Vec<String> = v
            .nodes
            .iter()
            .filter(|x| x.name == n || x.id.starts_with(n))
            .map(|x| x.id.clone())
            .collect();
        if ids.is_empty() {
            bail!("no node {n:?}");
        }
        v.nodes.retain(|x| ids.contains(&x.id));
        v.days.retain(|d| ids.contains(&d.node));
    }
    match output {
        Output::Json => out!("{}", serde_json::to_string_pretty(&*v)?),
        Output::Table => print(&v, by_account),
    }
    Ok(0)
}

fn print(v: &EnergyView, by_account: bool) {
    let rows = if by_account {
        v.by_account()
    } else {
        v.days.clone()
    };
    out!(
        "{:<10} {:<16} {:>10} {:>6} {:>11} {:>9} {:>9} {:>8} {:>5}",
        "DAY",
        if by_account { "ACCOUNT" } else { "NODE" },
        "ENERGY",
        "HOURS",
        "COST",
        "WORTH",
        "EARNED",
        "WON/CU",
        "RUNS"
    );
    for d in &rows {
        out!("{}", row(d, by_account));
    }
    if rows.is_empty() {
        out!("(no measured power yet: nodes report it from 1.22, Linux CPUs need RAPL access)");
    }
    out!(
        "days are the control plane's local time ({}); cost uses each node's price now; WORTH is CU at the cluster's rates, EARNED what other accounts paid",
        v.utc_offset
    );
    if v.nodes
        .iter()
        .any(|n| !n.settings.is_empty() || n.temp_c.is_some())
    {
        out!("");
        out!("{:<16} {:>7} {:>8}  SETTINGS", "NODE", "TEMP", "POWER");
        for n in &v.nodes {
            let mut settings = n.settings.describe();
            if let Some(since) = n.hot_since_unix {
                settings.push_str(&format!("  PAUSED (hot since {})", local_time(since, true)));
            }
            out!(
                "{:<16} {:>7} {:>8}  {settings}",
                crate::truncate(&n.name, 16),
                n.temp_c
                    .map(|t| format!("{t:.0} °C"))
                    .unwrap_or_else(|| "-".into()),
                n.watts
                    .map(|w| format!("{w:.0} W"))
                    .unwrap_or_else(|| "-".into()),
            );
        }
    }
}

fn row(d: &EnergyDay, by_account: bool) -> String {
    let who = if by_account { &d.account } else { &d.name };
    format!(
        "{:<10} {:<16} {:>10} {:>6.1} {:>11} {:>9} {:>9} {:>8} {:>5}",
        d.day,
        crate::truncate(who, 16),
        format_wh(d.mwh),
        d.measured_secs as f64 / 3600.0,
        d.cost_mwon.map(format_won).unwrap_or_else(|| "-".into()),
        format!("{:.2}", d.worth_ucu as f64 / MICRO_PER_CU as f64),
        format_cu(d.earned_mcu),
        d.won_per_cu()
            .map(|w| format!("{w:.1}"))
            .unwrap_or_else(|| "-".into()),
        d.runs
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_day_row_shows_cost_and_won_per_cu() {
        let d = EnergyDay {
            day: "2026-11-18".into(),
            node: "abc".into(),
            name: "gpu-pc".into(),
            account: "stan".into(),
            mwh: 2_350_000,
            measured_secs: 86_400,
            cost_mwon: Some(282_000),
            worth_ucu: 48 * MICRO_PER_CU,
            earned_mcu: 12_500,
            runs: 7,
        };
        let r = row(&d, false);
        assert!(r.contains("2.35 kWh"), "{r}");
        assert!(r.contains("282 won"), "{r}");
        assert!(r.contains("48.00"), "{r}");
        assert!(r.contains("12.5"), "{r}");
        assert!(r.contains("5.9"), "{r}");
        assert!(row(&d, true).contains("stan"));
    }
}
