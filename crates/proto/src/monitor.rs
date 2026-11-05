//! Monitoring (phase 11): the metrics nodes report, the time series the
//! control plane keeps, cluster events, live task logs, alert rules and
//! the webhooks alerts go to.

use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    job::{JobId, parse_duration_ms},
    msg::TaskState,
};

/// How often an agent samples its machine.
pub const METRICS_EVERY_SECS: u64 = 5;

/// What a node measured about itself at one moment (protocol 10).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Busy share of all CPUs, 0 to 100.
    pub cpu_percent: f32,
    /// One-minute load average (0 where the OS has none, like Windows).
    pub load1: f32,
    pub memory_used: u64,
    pub memory_total: u64,
    /// The disk under the agent's data directory.
    pub disk_used: u64,
    pub disk_total: u64,
    /// Bytes received and sent on every interface but loopback, since the
    /// machine started (counters).
    pub net_rx: u64,
    pub net_tx: u64,
    /// The hottest temperature sensor, in °C, where the OS shows one.
    pub temp_c: Option<f32>,
    pub gpus: Vec<GpuMetrics>,
    /// Tasks running when sampled.
    pub running: u32,
}

/// One GPU at one moment.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GpuMetrics {
    /// [`crate::gpu::GpuInfo::index`].
    pub index: u32,
    pub util_percent: f32,
    pub mem_used_mib: u64,
    pub mem_total_mib: u64,
    pub temp_c: Option<f32>,
    pub power_w: Option<f32>,
}

impl NodeMetrics {
    /// The hottest sensor, GPUs included.
    pub fn hottest(&self) -> Option<f32> {
        self.gpus
            .iter()
            .filter_map(|g| g.temp_c)
            .chain(self.temp_c)
            .fold(None, |a: Option<f32>, t| Some(a.map_or(t, |a| a.max(t))))
    }

    pub fn disk_percent(&self) -> Option<f32> {
        (self.disk_total > 0).then(|| self.disk_used as f32 * 100.0 / self.disk_total as f32)
    }
}

/// A node's metrics over one step of a time series: averages over the
/// step (temperatures: the highest), rates in bytes per second.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint {
    /// Start of the step.
    pub unix: u64,
    pub cpu_percent: f32,
    pub load1: f32,
    pub memory_used: u64,
    pub memory_total: u64,
    pub disk_used: u64,
    pub disk_total: u64,
    pub net_rx_bps: u64,
    pub net_tx_bps: u64,
    pub temp_c: Option<f32>,
    /// Mean use of the node's GPUs, when it has any.
    pub gpu_percent: Option<f32>,
    /// VRAM in use on all its GPUs.
    pub gpu_mem_used_mib: u64,
    pub running: f32,
}

/// What `pumasi metrics` and `pumasi top` show.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsView {
    pub nodes: Vec<NodeMetricsView>,
    pub cluster: ClusterMetrics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeMetricsView {
    pub id: String,
    pub name: String,
    pub online: bool,
    pub slots: u32,
    pub running: u32,
    /// The latest sample and when it came; none from agents older than
    /// protocol 10.
    pub latest: Option<NodeMetrics>,
    pub latest_unix: u64,
    /// Network rates between the last two samples.
    pub net_rx_bps: u64,
    pub net_tx_bps: u64,
    /// The time series asked for, oldest first, `step_secs` apart.
    pub step_secs: u32,
    pub series: Vec<MetricPoint>,
}

/// The cluster's queue and run counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMetrics {
    /// Runs waiting for a node, by job priority (highest first).
    pub queued: Vec<(i32, u32)>,
    pub running: u32,
    /// Jobs waiting for other jobs (`after`).
    pub waiting_jobs: u32,
    /// Queued runs held back by their account's quota in the last
    /// scheduling pass.
    pub quota_blocked: u32,
    /// Since the control plane started.
    pub runs_succeeded: u64,
    pub runs_failed: u64,
    pub preemptions: u64,
    pub check_mismatches: u64,
    pub alerts_firing: u32,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EventLevel {
    #[default]
    Info,
    Warning,
    Critical,
}

impl fmt::Display for EventLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EventLevel::Info => "info",
            EventLevel::Warning => "warning",
            EventLevel::Critical => "critical",
        })
    }
}

/// Something that happened in the cluster: a node came or went, a job
/// finished, a check disagreed, an alert fired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventView {
    pub seq: u64,
    pub unix: u64,
    pub level: EventLevel,
    /// `node.offline`, `job.failed`, `alert.firing`, ...
    pub kind: String,
    /// The node's name.
    pub node: Option<String>,
    pub job: Option<JobId>,
    /// The job's account.
    pub account: Option<String>,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Live logs
// ---------------------------------------------------------------------------

/// Output of one task from the offsets asked for: what its current run
/// printed so far, or all of it once the task is done.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogChunk {
    /// The run the output belongs to. When it is not the attempt asked
    /// for, the offsets start again from 0.
    pub attempt: u32,
    pub stdout_from: u64,
    pub stdout: Vec<u8>,
    pub stderr_from: u64,
    pub stderr: Vec<u8>,
    pub state: TaskState,
    /// The task finished: nothing more will come.
    pub done: bool,
    /// The output is streamed while the task runs (WASM and Docker; a
    /// microVM's output comes when it finishes).
    pub live: bool,
}

/// Largest output one [`LogChunk`] carries per stream.
pub const LOG_CHUNK_BYTES: usize = 1 << 20;

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

/// When the cluster raises alerts. `None` (or `false`) turns a rule off.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertPolicy {
    /// A node (not cordoned) offline for this long.
    pub node_offline_secs: Option<u64>,
    /// Too many runs failing across the cluster.
    pub failure_rate: Option<FailureRate>,
    /// A node's disk, or the control plane's, fuller than this percentage.
    pub disk_percent: Option<u8>,
    /// A CPU or GPU sensor hotter than this, in °C.
    pub temp_c: Option<u16>,
    /// A node's new benchmark this many percent slower than its last one.
    pub perf_drop_percent: Option<u8>,
    /// A drive's SMART health failing, worn out (90% used) or with media
    /// errors.
    pub smart: bool,
    /// A check run disagreeing with a provider's result.
    pub check_mismatch: bool,
}

impl Default for AlertPolicy {
    fn default() -> Self {
        Self {
            node_offline_secs: Some(60),
            failure_rate: Some(FailureRate::default()),
            disk_percent: Some(90),
            temp_c: Some(90),
            perf_drop_percent: Some(25),
            smart: true,
            check_mismatch: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureRate {
    pub percent: u8,
    pub window_secs: u64,
    /// Fewer runs than this in the window never fire.
    pub min_runs: u32,
}

impl Default for FailureRate {
    fn default() -> Self {
        Self {
            percent: 50,
            window_secs: 900,
            min_runs: 10,
        }
    }
}

impl AlertPolicy {
    pub fn validate(&self) -> Result<()> {
        if let Some(f) = &self.failure_rate {
            ensure!(
                (1..=100).contains(&f.percent),
                "failure rate must be 1% to 100%"
            );
            ensure!(f.window_secs >= 60, "failure window must be at least 1m");
        }
        if let Some(p) = self.disk_percent {
            ensure!((1..=100).contains(&p), "disk must be 1% to 100%");
        }
        if let Some(p) = self.perf_drop_percent {
            ensure!((1..=99).contains(&p), "perf drop must be 1% to 99%");
        }
        Ok(())
    }

    /// One line per rule, for `pumasi alert ls`.
    pub fn describe(&self) -> Vec<(&'static str, String)> {
        let off = || "off".to_string();
        vec![
            (
                "node_offline",
                self.node_offline_secs
                    .map(|s| format!("a node offline for {}", human_duration(s)))
                    .unwrap_or_else(off),
            ),
            (
                "failure_rate",
                self.failure_rate
                    .map(|f| {
                        format!(
                            "{}% of runs failing over {} (at least {} runs)",
                            f.percent,
                            human_duration(f.window_secs),
                            f.min_runs
                        )
                    })
                    .unwrap_or_else(off),
            ),
            (
                "disk",
                self.disk_percent
                    .map(|p| format!("a disk over {p}% full"))
                    .unwrap_or_else(off),
            ),
            (
                "temperature",
                self.temp_c
                    .map(|t| format!("a sensor over {t} °C"))
                    .unwrap_or_else(off),
            ),
            (
                "perf_drop",
                self.perf_drop_percent
                    .map(|p| format!("a benchmark {p}% slower than the last"))
                    .unwrap_or_else(off),
            ),
            (
                "smart",
                if self.smart {
                    "a drive failing SMART, worn out or with media errors".into()
                } else {
                    off()
                },
            ),
            (
                "check_mismatch",
                if self.check_mismatch {
                    "a check run disagreeing with a result".into()
                } else {
                    off()
                },
            ),
        ]
    }
}

/// `90s`, `2m`, `1h`, `1d`.
pub fn human_duration(secs: u64) -> String {
    match secs {
        s if s >= 86_400 && s % 86_400 == 0 => format!("{}d", s / 86_400),
        s if s >= 3600 && s % 3600 == 0 => format!("{}h", s / 3600),
        s if s >= 60 && s % 60 == 0 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

/// `off`, `none`, `false` or `0` turn a rule off.
pub fn is_off(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "off" | "none" | "false" | "no" | "0"
    )
}

/// A duration (`2m`), or `off`.
pub fn parse_opt_secs(s: &str) -> Result<Option<u64>> {
    if is_off(s) {
        return Ok(None);
    }
    Ok(Some(parse_duration_ms(s)? / 1000))
}

/// A percentage (`90%` or `90`), or `off`.
pub fn parse_opt_percent(s: &str) -> Result<Option<u8>> {
    if is_off(s) {
        return Ok(None);
    }
    let n: u8 = s
        .trim()
        .trim_end_matches('%')
        .trim()
        .parse()
        .with_context(|| format!("invalid percentage {s:?}"))?;
    ensure!((1..=100).contains(&n), "percentage must be 1 to 100: {s:?}");
    Ok(Some(n))
}

/// `on`/`off` and the like.
pub fn parse_switch(s: &str) -> Result<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => bail!("expected on or off, not {s:?}"),
    }
}

/// An alert that is firing now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertView {
    /// `node_offline`, `disk`, ...
    pub rule: String,
    /// What it is about: a node's name, or `cluster`.
    pub subject: String,
    pub level: EventLevel,
    pub message: String,
    pub since_unix: u64,
}

/// What a webhook's body looks like.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookFormat {
    /// `{"text": ...}`: Slack incoming webhooks, Mattermost, Rocket.Chat,
    /// and Discord's `/slack` endpoints.
    Slack,
    /// `{"content": ...}`: Discord webhooks.
    Discord,
    /// The whole alert as JSON, for your own receiver.
    #[default]
    Json,
}

impl WebhookFormat {
    /// The format a webhook URL most likely wants.
    pub fn detect(url: &str) -> Self {
        let host = url_host(url).to_ascii_lowercase();
        let path = url.to_ascii_lowercase();
        if host.ends_with("discord.com") || host.ends_with("discordapp.com") {
            if path.trim_end_matches('/').ends_with("/slack") {
                WebhookFormat::Slack
            } else {
                WebhookFormat::Discord
            }
        } else if host.ends_with("slack.com") || path.contains("/hooks/") {
            WebhookFormat::Slack
        } else {
            WebhookFormat::Json
        }
    }
}

impl fmt::Display for WebhookFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WebhookFormat::Slack => "slack",
            WebhookFormat::Discord => "discord",
            WebhookFormat::Json => "json",
        })
    }
}

impl FromStr for WebhookFormat {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "slack" | "mattermost" => WebhookFormat::Slack,
            "discord" => WebhookFormat::Discord,
            "json" => WebhookFormat::Json,
            other => bail!("unknown webhook format {other:?} (slack, discord or json)"),
        })
    }
}

/// Where alerts go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSpec {
    pub name: String,
    pub url: String,
    pub format: WebhookFormat,
}

impl WebhookSpec {
    pub fn validate(&self) -> Result<()> {
        crate::manage::validate_schedule_name(&self.name)
            .map_err(|e| anyhow::anyhow!("{e:#}").context("webhook name"))?;
        let lower = self.url.to_ascii_lowercase();
        ensure!(
            lower.starts_with("https://") || lower.starts_with("http://"),
            "webhook URL must start with https:// or http://"
        );
        ensure!(!url_host(&self.url).is_empty(), "webhook URL has no host");
        ensure!(self.url.len() <= 2048, "webhook URL is too long");
        Ok(())
    }
}

/// The host of a URL (no scheme, credentials, port or path).
pub fn url_host(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    if let Some(v6) = host.strip_prefix('[') {
        return v6.split(']').next().unwrap_or("");
    }
    host.split(':').next().unwrap_or("")
}

/// A URL with everything after the host hidden: webhook URLs carry their
/// secret in the path.
pub fn redact_url(url: &str) -> String {
    let scheme = url.split_once("://").map_or("https", |(s, _)| s);
    format!("{scheme}://{}/…", url_host(url))
}

/// A webhook and how its deliveries went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookView {
    pub name: String,
    /// The URL with its path hidden.
    pub target: String,
    pub format: WebhookFormat,
    pub sent: u64,
    pub failed: u64,
    pub last_unix: Option<u64>,
    pub last_error: Option<String>,
}

/// Rules, what fires now, and where alerts go.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlertsView {
    pub policy: AlertPolicy,
    pub firing: Vec<AlertView>,
    pub webhooks: Vec<WebhookView>,
}

// ---------------------------------------------------------------------------
// Prometheus text format
// ---------------------------------------------------------------------------

/// Checks text in the Prometheus exposition format (version 0.0.4) the
/// way a scraper reads it: `# HELP` and `# TYPE` lines before their
/// samples, valid metric and label names, quoted and escaped label values,
/// numeric values, no family split in two and no sample twice. Returns the
/// number of samples.
pub fn lint_exposition(text: &str) -> Result<usize> {
    use std::collections::{HashMap, HashSet};
    let valid_name = |n: &str, colon: bool| {
        let mut c = n.chars();
        c.next()
            .is_some_and(|f| f.is_ascii_alphabetic() || f == '_' || (colon && f == ':'))
            && c.all(|x| x.is_ascii_alphanumeric() || x == '_' || (colon && x == ':'))
    };
    let mut types: HashMap<String, String> = HashMap::new();
    let mut done: HashSet<String> = HashSet::new();
    let mut current: Option<String> = None;
    let mut seen: HashSet<String> = HashSet::new();
    let mut samples = 0;
    ensure!(
        text.is_empty() || text.ends_with('\n'),
        "the text must end with a newline"
    );
    for (no, line) in text.lines().enumerate() {
        let no = no + 1;
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix('#') {
            let mut parts = rest.trim_start().splitn(3, ' ');
            let word = parts.next().unwrap_or("");
            if word != "HELP" && word != "TYPE" {
                continue;
            }
            let name = parts.next().unwrap_or("");
            ensure!(
                valid_name(name, true),
                "line {no}: bad metric name {name:?}"
            );
            if current.as_deref() != Some(name) {
                ensure!(
                    !done.contains(name),
                    "line {no}: family {name} split in two"
                );
                if let Some(c) = current.take() {
                    done.insert(c);
                }
                current = Some(name.to_string());
            }
            if word == "TYPE" {
                let t = parts.next().unwrap_or("").trim();
                ensure!(
                    matches!(t, "counter" | "gauge" | "histogram" | "summary" | "untyped"),
                    "line {no}: unknown type {t:?}"
                );
                ensure!(
                    types.insert(name.to_string(), t.to_string()).is_none(),
                    "line {no}: second TYPE for {name}"
                );
            }
            continue;
        }
        // name{labels} value [timestamp]
        let (name, rest) = match line.find(['{', ' ']) {
            Some(i) => line.split_at(i),
            None => bail!("line {no}: no value"),
        };
        ensure!(
            valid_name(name, true),
            "line {no}: bad metric name {name:?}"
        );
        let family = ["_total", "_bucket", "_sum", "_count", "_created"]
            .iter()
            .find_map(|s| name.strip_suffix(s).filter(|f| types.contains_key(*f)))
            .unwrap_or(name);
        if current.as_deref() != Some(family) {
            ensure!(
                !done.contains(family),
                "line {no}: family {family} split in two"
            );
            if let Some(c) = current.take() {
                done.insert(c);
            }
            current = Some(family.to_string());
        }
        let (labels, value_part) = if let Some(body) = rest.strip_prefix('{') {
            let mut labels = Vec::new();
            let mut chars = body.char_indices().peekable();
            let mut end = None;
            loop {
                while chars.peek().is_some_and(|(_, c)| *c == ' ' || *c == ',') {
                    chars.next();
                }
                match chars.peek() {
                    Some((i, '}')) => {
                        end = Some(*i);
                        break;
                    }
                    None => break,
                    _ => {}
                }
                let mut key = String::new();
                while let Some((_, c)) = chars.peek() {
                    if *c == '=' {
                        break;
                    }
                    key.push(*c);
                    chars.next();
                }
                ensure!(
                    valid_name(&key, false) && !key.starts_with("__"),
                    "line {no}: bad label name {key:?}"
                );
                ensure!(
                    chars.next().map(|(_, c)| c) == Some('='),
                    "line {no}: expected ="
                );
                ensure!(
                    chars.next().map(|(_, c)| c) == Some('"'),
                    "line {no}: label value must be quoted"
                );
                let mut value = String::new();
                loop {
                    match chars.next() {
                        Some((_, '\\')) => match chars.next() {
                            Some((_, 'n')) => value.push('\n'),
                            Some((_, '\\')) => value.push('\\'),
                            Some((_, '"')) => value.push('"'),
                            other => bail!("line {no}: bad escape {other:?}"),
                        },
                        Some((_, '"')) => break,
                        Some((_, c)) => value.push(c),
                        None => bail!("line {no}: unterminated label value"),
                    }
                }
                ensure!(
                    !labels.iter().any(|(k, _): &(String, String)| *k == key),
                    "line {no}: label {key} twice"
                );
                labels.push((key, value));
            }
            let end = end.with_context(|| format!("line {no}: unterminated labels"))?;
            (labels, &body[end + 1..])
        } else {
            (Vec::new(), rest)
        };
        let mut fields = value_part.split_whitespace();
        let value = fields
            .next()
            .with_context(|| format!("line {no}: no value"))?;
        ensure!(
            matches!(value, "NaN" | "+Inf" | "-Inf") || value.parse::<f64>().is_ok(),
            "line {no}: bad value {value:?}"
        );
        if let Some(ts) = fields.next() {
            ensure!(ts.parse::<i64>().is_ok(), "line {no}: bad timestamp {ts:?}");
        }
        ensure!(fields.next().is_none(), "line {no}: trailing text");
        let mut sorted = labels.clone();
        sorted.sort();
        let key = format!("{name}{sorted:?}");
        ensure!(seen.insert(key), "line {no}: duplicate sample {name}");
        samples += 1;
    }
    Ok(samples)
}

/// Escapes a label value for the exposition format.
pub fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_formats_from_urls() {
        use WebhookFormat::*;
        assert_eq!(
            WebhookFormat::detect("https://hooks.slack.com/services/T0/B0/xyz"),
            Slack
        );
        assert_eq!(
            WebhookFormat::detect("https://discord.com/api/webhooks/1/abc"),
            Discord
        );
        assert_eq!(
            WebhookFormat::detect("https://discord.com/api/webhooks/1/abc/slack"),
            Slack
        );
        assert_eq!(
            WebhookFormat::detect("https://chat.example.org/hooks/abc"),
            Slack,
            "Mattermost"
        );
        assert_eq!(WebhookFormat::detect("http://127.0.0.1:9000/alert"), Json);
        assert_eq!(
            redact_url("https://user:pw@hooks.slack.com:443/services/secret"),
            "https://hooks.slack.com/…"
        );
        assert_eq!(url_host("http://[::1]:8080/x"), "::1");
    }

    #[test]
    fn rule_values() {
        assert_eq!(parse_opt_secs("2m").unwrap(), Some(120));
        assert_eq!(parse_opt_secs("off").unwrap(), None);
        assert_eq!(parse_opt_percent("90%").unwrap(), Some(90));
        assert_eq!(parse_opt_percent("off").unwrap(), None);
        assert!(parse_opt_percent("150%").is_err());
        assert!(parse_switch("on").unwrap());
        assert!(AlertPolicy::default().validate().is_ok());
        assert_eq!(human_duration(120), "2m");
        assert_eq!(human_duration(90), "90s");
    }

    #[test]
    fn hottest_includes_gpus() {
        let m = NodeMetrics {
            temp_c: Some(60.0),
            gpus: vec![GpuMetrics {
                temp_c: Some(81.0),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(m.hottest(), Some(81.0));
        assert_eq!(NodeMetrics::default().hottest(), None);
    }

    #[test]
    fn lints_the_exposition_format() {
        let good = "# HELP a_total Things.\n# TYPE a_total counter\na_total{x=\"1\",y=\"a\\\"b\"} 3\na_total{x=\"2\"} 4 1700000000000\n# TYPE b gauge\nb NaN\n";
        assert_eq!(lint_exposition(good).unwrap(), 3);
        let bad = [
            "a{x=1} 1\n",
            "a{x=\"1\"} one\n",
            "1a 1\n",
            "a 1\nb 1\na 2\n",
            "a{x=\"1\"} 1\na{x=\"1\"} 2\n",
            "a 1",
            "# TYPE a countr\na 1\n",
            "a{__x=\"1\"} 1\n",
        ];
        for b in bad {
            assert!(lint_exposition(b).is_err(), "{b:?} passed");
        }
    }
}
