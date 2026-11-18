//! Electricity and heat (1.22): each node's energy by local hour and day,
//! what it cost at the node's price against the CU its runs were worth,
//! and the pause of a node that stays over its temperature limit.
//!
//! Every metrics sample (5 s) is integrated: the node's CPU package power
//! (from its latest [`NodeMetricsExtra`], 1.15 agents and later) and its
//! GPUs' power, over the time since its previous sample (skipped over a
//! minute, when it was away). The energy goes in the hour of the control
//! plane's local time it fell in, so cheap hours and a price set later
//! still price it by hour. Receipts add what each run was worth at the
//! cluster's prices (also runs for the node's own account, which move no
//! CU) and what it earned. Days are kept for [`KEEP_DAYS`].
//!
//! [`NodeMetricsExtra`]: pumasi_proto::monitor::NodeMetricsExtra

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Result, ensure};
use chrono::{Datelike, Local, NaiveDate, TimeZone, Timelike};
use pumasi_proto::{
    energy::{EnergyDay, EnergyView, HOT_HOLD_SECS, NodeEnergy, NodeEnergyView},
    monitor::{EventLevel, NodeMetrics},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{State, now_unix};

/// How long days of energy are kept.
pub const KEEP_DAYS: i32 = 400;
/// The longest gap between two samples that is integrated.
const MAX_GAP_SECS: u64 = 60;

/// One node's day. The layout is stored: add fields only at the end, with
/// [`pumasi_proto::wire::trailing`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayRecord {
    /// Milliwatt-hours in each local hour (24).
    pub hours_mwh: Vec<u64>,
    pub measured_secs: u32,
    /// What its runs were worth at the cluster's prices.
    pub worth_ucu: i64,
    /// Milli-CU its account earned from them.
    pub earned_mcu: i64,
    pub runs: u32,
}

impl DayRecord {
    fn add_mwh(&mut self, hour: u8, mwh: u64) {
        if self.hours_mwh.len() < 24 {
            self.hours_mwh.resize(24, 0);
        }
        self.hours_mwh[hour as usize % 24] += mwh;
    }

    pub fn mwh(&self) -> u64 {
        self.hours_mwh.iter().sum()
    }
}

/// A node over its limit: since when it is paused, and why.
#[derive(Debug, Clone)]
pub(crate) struct Hot {
    pub since: u64,
    pub reason: String,
}

#[derive(Default)]
pub(crate) struct EnergyState {
    /// By node id.
    pub settings: HashMap<String, NodeEnergy>,
    pub days: BTreeMap<(String, i32), DayRecord>,
    dirty: BTreeSet<(String, i32)>,
    /// When each node's last sample was integrated, and the milliwatt-hours
    /// not yet whole.
    last: HashMap<String, (u64, f64)>,
    over_since: HashMap<String, u64>,
    under_since: HashMap<String, u64>,
    pub hot: HashMap<String, Hot>,
    pruned_day: i32,
}

/// Local day number (days since 1970-01-01 in the control plane's time
/// zone) and hour of a unix time.
pub fn local_day_hour(unix: u64) -> (i32, u8) {
    let t = Local
        .timestamp_opt(unix as i64, 0)
        .single()
        .unwrap_or_else(Local::now);
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
    let day = (t.date_naive() - epoch).num_days() as i32;
    (day, t.hour() as u8)
}

/// `2026-11-18` for a local day number.
pub fn day_string(day: i32) -> String {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
    let d = epoch + chrono::Duration::days(day as i64);
    format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day())
}

/// The control plane's UTC offset now, `+09:00`.
pub fn utc_offset() -> String {
    Local::now().format("%:z").to_string()
}

impl State {
    pub(crate) fn load_energy(&mut self) -> Result<()> {
        for (node, e) in self.store.node_energy()? {
            self.energy.settings.insert(node, e);
        }
        let (today, _) = local_day_hour(now_unix());
        for (k, r) in self.store.energy_days(today - KEEP_DAYS)? {
            self.energy.days.insert(k, r);
        }
        Ok(())
    }

    /// A node's sample (from [`State::on_metrics`]): its energy since the
    /// last one, and its temperature against its limit.
    pub(crate) fn energy_sample(&mut self, id: &str, now: u64, m: &NodeMetrics) {
        let cpu_w = self
            .monitor
            .series
            .get(id)
            .and_then(|s| s.extra.as_ref())
            .and_then(|x| x.cpu_power_w);
        let gpu_w: Vec<f32> = m.gpus.iter().filter_map(|g| g.power_w).collect();
        let watts = match (cpu_w, gpu_w.is_empty()) {
            (None, true) => None,
            (c, _) => Some(c.unwrap_or(0.0) as f64 + gpu_w.iter().map(|w| *w as f64).sum::<f64>()),
        };
        let e = &mut self.energy;
        let prev = e.last.get(id).copied();
        let mut frac = prev.map_or(0.0, |(_, f)| f);
        if let (Some(w), Some((then, _))) = (watts, prev)
            && now > then
            && now - then <= MAX_GAP_SECS
            && w.is_finite()
            && w >= 0.0
        {
            let secs = now - then;
            let mwh = w * secs as f64 / 3.6 + frac;
            let whole = mwh.floor();
            frac = mwh - whole;
            let (day, hour) = local_day_hour(now);
            let rec = e.days.entry((id.to_string(), day)).or_default();
            rec.add_mwh(hour, whole as u64);
            rec.measured_secs += secs as u32;
            e.dirty.insert((id.to_string(), day));
        }
        e.last.insert(id.to_string(), (now, frac));
        self.hot_sample(id, now, m.hottest());
    }

    /// Pauses a node that stayed over its limit for [`HOT_HOLD_SECS`], and
    /// lets it take tasks again once it stayed under for as long.
    pub(crate) fn hot_sample(&mut self, id: &str, now: u64, temp: Option<f32>) {
        let limit = self.energy.settings.get(id).and_then(|s| s.max_temp_c);
        let Some(limit) = limit else {
            if self.energy.hot.contains_key(id) {
                self.cool_down(id, "its temperature limit was removed");
            }
            self.energy.over_since.remove(id);
            self.energy.under_since.remove(id);
            return;
        };
        let Some(t) = temp else {
            return;
        };
        let over = t > limit as f32;
        if self.energy.hot.contains_key(id) {
            if over {
                self.energy.under_since.remove(id);
                return;
            }
            let since = *self.energy.under_since.entry(id.to_string()).or_insert(now);
            if now.saturating_sub(since) >= HOT_HOLD_SECS {
                self.cool_down(id, &format!("under {limit} °C for a minute"));
            }
            return;
        }
        if !over {
            self.energy.over_since.remove(id);
            return;
        }
        let since = *self.energy.over_since.entry(id.to_string()).or_insert(now);
        if now.saturating_sub(since) < HOT_HOLD_SECS {
            return;
        }
        let reason = format!("over {limit} °C for a minute ({t:.0} °C)");
        info!(node = %id, %reason, "node too hot; pausing it");
        self.energy.over_since.remove(id);
        self.energy.hot.insert(
            id.to_string(),
            Hot {
                since: now,
                reason: reason.clone(),
            },
        );
        let name = self.node_name(id);
        self.event(
            EventLevel::Warning,
            "node.paused",
            Some(id),
            None,
            pumasi_proto::text::Text::new("node.paused", [name, reason.clone()]),
        );
        // `--on-pause requeue`: its runs move, as for its other rules.
        if self.requeues_on_pause(id) {
            self.move_runs(id, &format!("node paused ({reason})"), true);
        }
        self.schedule();
    }

    fn cool_down(&mut self, id: &str, why: &str) {
        self.energy.hot.remove(id);
        self.energy.under_since.remove(id);
        info!(node = %id, why, "node cool again");
        // Still paused by its own rules: say nothing yet.
        if self.nodes.get(id).is_some_and(|n| n.paused.is_some()) {
            return;
        }
        let name = self.node_name(id);
        self.event(
            EventLevel::Info,
            "node.resumed",
            Some(id),
            None,
            pumasi_proto::text::Text::new("node.resumed", [&name]),
        );
        self.schedule();
    }

    /// Why a node takes no tasks for its temperature.
    pub(crate) fn hot_reason(&self, id: &str) -> Option<&str> {
        self.energy.hot.get(id).map(|h| h.reason.as_str())
    }

    /// A run on `node` was recorded: what it was worth and earned.
    pub(crate) fn energy_note_run(&mut self, node: &str, worth_ucu: i64, earned_mcu: i64) {
        let (day, _) = local_day_hour(now_unix());
        let key = (node.to_string(), day);
        let rec = self.energy.days.entry(key.clone()).or_default();
        rec.worth_ucu = rec.worth_ucu.saturating_add(worth_ucu.max(0));
        rec.earned_mcu = rec.earned_mcu.saturating_add(earned_mcu.max(0));
        rec.runs += 1;
        self.energy.dirty.insert(key);
    }

    /// Writes the days that changed (with the monitor's flush), and drops
    /// days older than [`KEEP_DAYS`] once a day.
    pub(crate) fn flush_energy(&mut self) {
        let (today, _) = local_day_hour(now_unix());
        let dirty = std::mem::take(&mut self.energy.dirty);
        let rows: Vec<((String, i32), DayRecord)> = dirty
            .into_iter()
            .filter_map(|k| self.energy.days.get(&k).map(|r| (k, r.clone())))
            .collect();
        let prune = (self.energy.pruned_day != today).then_some(today - KEEP_DAYS);
        if rows.is_empty() && prune.is_none() {
            return;
        }
        if let Some(before) = prune {
            self.energy.days.retain(|(_, d), _| *d >= before);
            self.energy.pruned_day = today;
        }
        if let Err(e) = self.store.put_energy_days(&rows, prune) {
            warn!("persist energy: {e:#}");
        }
    }

    /// Changes a node's electricity and temperature settings.
    pub fn set_node_energy(&mut self, node: &str, e: NodeEnergy) -> Result<String> {
        e.validate()?;
        let id = self.resolve_node(node)?;
        ensure!(self.members.contains_key(&id), "no node {node:?}");
        let stored = (!e.is_empty()).then_some(&e);
        self.store.put_node_energy(&id, stored)?;
        if e.is_empty() {
            self.energy.settings.remove(&id);
        } else {
            self.energy.settings.insert(id.clone(), e);
        }
        // A limit removed or raised takes effect with the next sample;
        // one removed lets the node go at once.
        if self
            .energy
            .settings
            .get(&id)
            .and_then(|s| s.max_temp_c)
            .is_none()
            && self.energy.hot.contains_key(&id)
        {
            self.cool_down(&id, "its temperature limit was removed");
        }
        Ok(id)
    }

    /// Nodes' settings and their last `days` local days (today included).
    pub fn energy_view(&self, days: u32) -> EnergyView {
        let (today, _) = local_day_hour(now_unix());
        let first = today - days.clamp(1, KEEP_DAYS as u32) as i32 + 1;
        let account_of = |id: &str| -> String { self.node_record(id).1 };
        let mut out = EnergyView {
            utc_offset: utc_offset(),
            ..Default::default()
        };
        for ((node, day), r) in self.energy.days.range((String::new(), i32::MIN)..) {
            if *day < first {
                continue;
            }
            let settings = self.energy.settings.get(node).cloned().unwrap_or_default();
            let mut hours = r.hours_mwh.clone();
            hours.resize(24, 0);
            out.days.push(EnergyDay {
                day: day_string(*day),
                node: node.clone(),
                name: self.node_name(node),
                account: account_of(node),
                mwh: r.mwh(),
                measured_secs: r.measured_secs,
                cost_mwon: settings.cost_mwon(&hours),
                worth_ucu: r.worth_ucu,
                earned_mcu: r.earned_mcu,
                runs: r.runs,
            });
        }
        out.days
            .sort_by(|a, b| b.day.cmp(&a.day).then(a.name.cmp(&b.name)));
        let mut ids: Vec<&String> = self.members.keys().collect();
        ids.sort_by_key(|id| self.node_name(id));
        let now = now_unix();
        for id in ids {
            let latest = self.monitor.series.get(id.as_str());
            let fresh = latest.and_then(|s| s.latest_fresh(now));
            let cpu_w = latest
                .and_then(|s| s.extra.as_ref())
                .and_then(|x| x.cpu_power_w);
            let watts = fresh.and_then(|m| {
                let g: Vec<f32> = m.gpus.iter().filter_map(|g| g.power_w).collect();
                match (cpu_w, g.is_empty()) {
                    (None, true) => None,
                    (c, _) => Some(c.unwrap_or(0.0) + g.iter().sum::<f32>()),
                }
            });
            out.nodes.push(NodeEnergyView {
                id: id.clone(),
                name: self.node_name(id),
                account: account_of(id),
                settings: self.energy.settings.get(id).cloned().unwrap_or_default(),
                hot_since_unix: self.energy.hot.get(id).map(|h| h.since),
                temp_c: fresh.and_then(|m| m.hottest()),
                watts,
            });
        }
        out
    }

    /// A node went offline: its temperature state starts over.
    pub(crate) fn energy_offline(&mut self, id: &str) {
        self.energy.last.remove(id);
        self.energy.over_since.remove(id);
        self.energy.under_since.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_and_hours_are_local() {
        let now = Local::now();
        let (day, hour) = local_day_hour(now.timestamp() as u64);
        assert_eq!(hour as u32, now.hour());
        assert_eq!(day_string(day), now.format("%Y-%m-%d").to_string());
        assert_eq!(day_string(0), "1970-01-01");
        let mut r = DayRecord::default();
        r.add_mwh(23, 5);
        r.add_mwh(0, 7);
        assert_eq!((r.hours_mwh.len(), r.mwh()), (24, 12));
    }
}
