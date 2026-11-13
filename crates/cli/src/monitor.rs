//! Monitoring commands (phase 11): `top`, `metrics`, `logs`, `events`,
//! `alert` and `webhook`.

use std::{
    io::{IsTerminal, Write},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Subcommand};
use pumasi_cluster::AdminClient;
use pumasi_proto::{
    job::{JobId, parse_duration_ms},
    manage::Selector,
    monitor::{
        AlertOverride, AlertPolicy, AlertTuning, AlertTuningView, AlertsView, EventLevel,
        EventView, FailureRate, LogChunk, MetricsView, NetView, NodeMetricsView, SilenceSpec,
        WebhookFormat, WebhookSpec, human_bps, human_duration, human_rtt, parse_opt_percent,
        parse_opt_secs, parse_switch,
    },
    msg::{AdminOp, AdminResponse, TaskState},
    store::format_size,
};

use super::{AdminArgs, connect_admin, manage::Output, truncate};

#[derive(Subcommand)]
pub enum AlertCmd {
    /// Show the alert rules, what fires now and where alerts go.
    Ls {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Change alert rules; the ones not given stay as they are. `off`
    /// turns a rule off. With `--on`, change the offline, disk and
    /// temperature thresholds of some nodes only.
    Set {
        #[command(flatten)]
        rules: Box<AlertRules>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Keep alerts quiet for a while: no webhook notices for them (they
    /// still show as firing). Without a rule, every rule; without `--on`,
    /// every node and the cluster.
    Silence {
        /// node_offline, failure_rate, disk, temperature, perf_drop, smart
        /// or check_mismatch.
        rule: Option<String>,
        /// A node name, or a label selector such as `gpu=nvidia`.
        #[arg(long, value_name = "NODE|SELECTOR")]
        on: Option<String>,
        /// How long, e.g. 2h or 3d.
        #[arg(long = "for", value_name = "DURATION", default_value = "2h")]
        duration: String,
        /// Why, for whoever looks later.
        #[arg(long, default_value = "")]
        comment: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// End a silence early (its id from `pumasi alert ls`).
    Unsilence {
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Args)]
pub struct AlertRules {
    /// A node (not cordoned) offline this long, e.g. 2m.
    #[arg(long, value_name = "DURATION|off")]
    node_offline: Option<String>,
    /// This share of runs failing, e.g. 50%.
    #[arg(long, value_name = "PERCENT|off")]
    failure_rate: Option<String>,
    /// Over how long the failure rate is measured, e.g. 15m.
    #[arg(long, value_name = "DURATION")]
    failure_window: Option<String>,
    /// Fewer runs than this in the window never alert.
    #[arg(long)]
    failure_min_runs: Option<u32>,
    /// A node's disk, or the control plane's, this full, e.g. 90%.
    #[arg(long, value_name = "PERCENT|off")]
    disk: Option<String>,
    /// A CPU or GPU sensor this hot, in °C.
    #[arg(long, value_name = "CELSIUS|off")]
    temperature: Option<String>,
    /// A new benchmark this much slower than the node's last one.
    #[arg(long, value_name = "PERCENT|off")]
    perf_drop: Option<String>,
    /// A drive failing SMART, worn out or with media errors.
    #[arg(long, value_name = "on|off")]
    smart: Option<String>,
    /// A check run disagreeing with a provider's result.
    #[arg(long, value_name = "on|off")]
    check_mismatch: Option<String>,
    /// Start from the default rules instead of the current ones.
    #[arg(long)]
    defaults: bool,
    /// Send a firing alert's notice again this often while it fires,
    /// e.g. 4h.
    #[arg(long, value_name = "DURATION|off")]
    repeat: Option<String>,
    /// Send a notice when an alert stops firing.
    #[arg(long, value_name = "on|off")]
    resolved: Option<String>,
    /// Set --node-offline, --disk and --temperature for these nodes only:
    /// a node name, or a label selector such as `os=windows`. `inherit`
    /// as a value goes back to the cluster's rule.
    #[arg(long, value_name = "NODE|SELECTOR")]
    on: Option<String>,
}

#[derive(Subcommand)]
pub enum WebhookCmd {
    /// Send alerts to a URL: a Slack or Mattermost incoming webhook, a
    /// Discord webhook, or your own receiver (JSON).
    Add {
        name: String,
        url: String,
        /// `slack`, `discord` or `json` (default: guessed from the URL).
        #[arg(long)]
        format: Option<WebhookFormat>,
        /// Do not send a test alert after adding it.
        #[arg(long)]
        no_test: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// List webhooks (their URLs' paths hidden) and how deliveries went.
    Ls {
        #[command(flatten)]
        admin: AdminArgs,
    },
    Rm {
        name: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Send a test alert now.
    Test {
        name: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

async fn call(home: &Path, admin: &AdminArgs, op: AdminOp) -> Result<AdminResponse> {
    let client = connect_admin(home, admin).await?;
    let r = client.call(op).await;
    client.close().await;
    r
}

async fn metrics(client: &AdminClient, node: Option<String>, since: u64) -> Result<MetricsView> {
    match client
        .call(AdminOp::Metrics {
            node,
            since_secs: since,
        })
        .await?
    {
        AdminResponse::Metrics(m) => Ok(*m),
        _ => bail!("unexpected response"),
    }
}

pub(crate) fn local_time(unix: u64, with_date: bool) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_opt(unix as i64, 0).single() {
        Some(t) if with_date => t.format("%Y-%m-%d %H:%M:%S").to_string(),
        Some(t) => t.format("%H:%M:%S").to_string(),
        None => unix.to_string(),
    }
}

fn rate(bps: u64) -> String {
    if bps == 0 {
        "0".into()
    } else {
        format!("{}/s", format_size(bps))
    }
}

fn percent(used: u64, total: u64) -> String {
    used.saturating_mul(100)
        .checked_div(total)
        .map_or_else(|| "-".into(), |p| format!("{p}%"))
}

// ---------------------------------------------------------------------------
// top and metrics
// ---------------------------------------------------------------------------

/// `pumasi top`: nodes and the queue, refreshed until Ctrl-C.
pub async fn top(home: &Path, admin: &AdminArgs, interval: &str, once: bool) -> Result<i32> {
    let every = Duration::from_millis(parse_duration_ms(interval)?.max(500));
    let client = connect_admin(home, admin).await?;
    let tty = std::io::stdout().is_terminal();
    loop {
        let m = metrics(&client, None, 0).await?;
        let text = render_top(&m);
        let mut out = std::io::stdout().lock();
        if tty && !once {
            // Home, clear the screen.
            write!(out, "\x1b[H\x1b[2J")?;
        }
        write!(out, "{text}")?;
        out.flush()?;
        drop(out);
        if once {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    client.close().await;
    Ok(0)
}

pub fn render_top(m: &MetricsView) -> String {
    let mut s = String::new();
    let c = &m.cluster;
    let queued: u32 = c.queued.iter().map(|(_, n)| n).sum();
    let by_priority = if c.queued.len() > 1 || c.queued.iter().any(|(p, _)| *p != 0) {
        format!(
            " ({})",
            c.queued
                .iter()
                .map(|(p, n)| format!("p{p}: {n}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };
    s.push_str(&format!(
        "{}  runs: {} running, {queued} queued{by_priority}, {} jobs waiting, {} held by quotas   alerts: {}\n",
        local_time(now_unix(), true),
        c.running,
        c.waiting_jobs,
        c.quota_blocked,
        c.alerts_firing
    ));
    s.push_str(&format!(
        "since the control plane started: {} runs succeeded, {} failed, {} preempted, {} check mismatches\n\n",
        c.runs_succeeded, c.runs_failed, c.preemptions, c.check_mismatches
    ));
    s.push_str(&format!(
        "{:<16} {:<8} {:>5} {:>5} {:>5} {:>19} {:>5} {:>23} {:>6} {:>14}\n",
        "NODE", "STATE", "TASKS", "CPU", "LOAD", "MEMORY", "DISK", "NET IN / OUT", "TEMP", "GPU"
    ));
    for n in &m.nodes {
        s.push_str(&node_row(n));
        s.push('\n');
    }
    if m.nodes.is_empty() {
        s.push_str("(no nodes yet: `pumasi token` makes a join ticket)\n");
    }
    s
}

fn node_row(n: &NodeMetricsView) -> String {
    let state = if n.online { "online" } else { "offline" };
    let tasks = format!("{}/{}", n.running, n.slots);
    let Some(l) = n.latest.as_ref().filter(|_| n.online) else {
        return format!(
            "{:<16} {:<8} {:>5} {:>5}",
            truncate(&n.name, 16),
            state,
            tasks,
            if n.online {
                "(older agent: no metrics)"
            } else {
                ""
            }
        );
    };
    let mem = format!(
        "{}/{}",
        format_size(l.memory_used),
        format_size(l.memory_total)
    );
    // The hottest sensor, GPUs included, as the dashboard shows it (a
    // Windows machine often reports only its GPU's temperature).
    let temp = l
        .hottest()
        .map(|t| format!("{t:.0}°C"))
        .unwrap_or_else(|| "-".into());
    let gpu = if l.gpus.is_empty() {
        "-".to_string()
    } else {
        let util = l.gpus.iter().map(|g| g.util_percent).sum::<f32>() / l.gpus.len() as f32;
        let mem: u64 = l.gpus.iter().map(|g| g.mem_used_mib).sum();
        format!("{util:.0}% {}", format_size(mem << 20))
    };
    let load = if cfg!(windows) && l.load1 == 0.0 {
        "-".to_string()
    } else {
        format!("{:.1}", l.load1)
    };
    format!(
        "{:<16} {:<8} {:>5} {:>4.0}% {:>5} {:>19} {:>5} {:>23} {:>6} {:>14}",
        truncate(&n.name, 16),
        state,
        tasks,
        l.cpu_percent,
        load,
        mem,
        percent(l.disk_used, l.disk_total),
        format!("{} / {}", rate(n.net_rx_bps), rate(n.net_tx_bps)),
        temp,
        gpu
    )
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `pumasi metrics [NODE]`: the latest of every node, or one node's time
/// series.
pub async fn show_metrics(
    home: &Path,
    admin: &AdminArgs,
    node: Option<String>,
    since: &str,
    output: Output,
) -> Result<i32> {
    let since_secs = parse_duration_ms(since)? / 1000;
    let client = connect_admin(home, admin).await?;
    let ask = if node.is_some() { since_secs } else { 0 };
    let m = metrics(&client, node.clone(), ask).await?;
    client.close().await;
    if let Output::Json = output {
        println!("{}", serde_json::to_string_pretty(&m)?);
        return Ok(0);
    }
    let Some(_) = node else {
        print!("{}", render_top(&m));
        return Ok(0);
    };
    let Some(n) = m.nodes.first() else {
        bail!("no such node");
    };
    println!(
        "{} ({}), every {} over the last {}",
        n.name,
        if n.online { "online" } else { "offline" },
        human_duration(n.step_secs.max(1) as u64),
        human_duration(since_secs)
    );
    if n.series.is_empty() {
        println!("no metrics yet (agents send them from protocol 10, every 5 seconds)");
        return Ok(0);
    }
    println!(
        "{:<19} {:>5} {:>5} {:>16} {:>5} {:>10} {:>10} {:>6} {:>5} {:>6}",
        "TIME", "CPU", "LOAD", "MEMORY", "DISK", "NET IN", "NET OUT", "TEMP", "GPU", "TASKS"
    );
    for p in &n.series {
        println!(
            "{:<19} {:>4.0}% {:>5.1} {:>16} {:>5} {:>10} {:>10} {:>6} {:>5} {:>6.1}",
            local_time(p.unix, true),
            p.cpu_percent,
            p.load1,
            format!(
                "{}/{}",
                format_size(p.memory_used),
                format_size(p.memory_total)
            ),
            percent(p.disk_used, p.disk_total),
            rate(p.net_rx_bps),
            rate(p.net_tx_bps),
            p.temp_c
                .map(|t| format!("{t:.0}°C"))
                .unwrap_or_else(|| "-".into()),
            p.gpu_percent
                .map(|g| format!("{g:.0}%"))
                .unwrap_or_else(|| "-".into()),
            p.running
        );
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// logs and events
// ---------------------------------------------------------------------------

/// `pumasi logs JOB`: a task's output; with `follow`, as it comes.
pub async fn logs(
    home: &Path,
    admin: &AdminArgs,
    job: JobId,
    index: u32,
    follow: bool,
) -> Result<i32> {
    let client = connect_admin(home, admin).await?;
    let (mut attempt, mut out_at, mut err_at) = (0u32, 0u64, 0u64);
    let mut first = true;
    let mut said_not_live = false;
    let mut said_waiting = false;
    loop {
        let chunk: LogChunk = match client
            .call(AdminOp::Logs {
                job,
                index,
                attempt,
                stdout_from: out_at,
                stderr_from: err_at,
            })
            .await?
        {
            AdminResponse::Logs(c) => *c,
            _ => bail!("unexpected response"),
        };
        if chunk.attempt != attempt {
            if !first && (out_at > 0 || err_at > 0) {
                eprintln!("[pumasi] the task started over (run {})", chunk.attempt);
            }
            attempt = chunk.attempt;
        }
        first = false;
        {
            let mut o = std::io::stdout().lock();
            o.write_all(&chunk.stdout)?;
            o.flush()?;
            let mut e = std::io::stderr().lock();
            e.write_all(&chunk.stderr)?;
            e.flush()?;
        }
        out_at = chunk.stdout_from + chunk.stdout.len() as u64;
        err_at = chunk.stderr_from + chunk.stderr.len() as u64;
        let more = !chunk.stdout.is_empty() || !chunk.stderr.is_empty();
        if chunk.done {
            if follow {
                eprintln!("[pumasi] task {job}/{index}: {}", state_text(&chunk.state));
            }
            break;
        }
        if !follow && !more {
            break;
        }
        if follow {
            if matches!(chunk.state, TaskState::Pending) && !said_waiting {
                eprintln!("[pumasi] waiting for the task to start");
                said_waiting = true;
            }
            if matches!(chunk.state, TaskState::Running { .. }) && !chunk.live && !said_not_live {
                eprintln!(
                    "[pumasi] the node's agent is older than 1.6: a microVM's output comes when the task finishes"
                );
                said_not_live = true;
            }
        }
        if !more {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                _ = tokio::signal::ctrl_c() => break,
            }
        }
    }
    client.close().await;
    Ok(0)
}

fn state_text(s: &TaskState) -> String {
    match s {
        TaskState::Pending => "pending".into(),
        TaskState::Running { node } => format!("running on {node}"),
        TaskState::Succeeded => "succeeded".into(),
        TaskState::Failed(o) => format!("failed ({o})"),
        TaskState::Cancelled => "cancelled".into(),
        TaskState::Verifying => "finished, a check run is pending".into(),
    }
}

fn event_line(e: &EventView) -> String {
    let level = match e.level {
        EventLevel::Info => "info",
        EventLevel::Warning => "WARN",
        EventLevel::Critical => "CRIT",
    };
    format!(
        "{}  {level:<4}  {:<15} {}",
        local_time(e.unix, true),
        e.kind,
        e.message
    )
}

/// `pumasi events`: recent cluster events; with `follow`, new ones as
/// they happen.
pub async fn events(
    home: &Path,
    admin: &AdminArgs,
    since: Option<&str>,
    limit: u32,
    follow: bool,
    output: Output,
) -> Result<i32> {
    let since_unix = match since {
        Some(s) => now_unix().saturating_sub(parse_duration_ms(s)? / 1000),
        None => 0,
    };
    let client = connect_admin(home, admin).await?;
    let print = |list: &[EventView]| -> Result<()> {
        for e in list {
            match output {
                Output::Json => println!("{}", serde_json::to_string(e)?),
                Output::Table => println!("{}", event_line(e)),
            }
        }
        std::io::stdout().flush()?;
        Ok(())
    };
    let fetch = |after: u64, since_unix: u64, limit: u32| {
        let client = &client;
        async move {
            match client
                .call(AdminOp::Events {
                    after,
                    since_unix,
                    limit,
                })
                .await?
            {
                AdminResponse::Events(v) => Ok::<_, anyhow::Error>(v),
                _ => bail!("unexpected response"),
            }
        }
    };
    let first = fetch(0, since_unix, limit).await?;
    print(&first)?;
    let mut last = first.last().map_or(0, |e| e.seq);
    if follow {
        if last == 0 {
            // Nothing yet: start after whatever the newest event is.
            last = fetch(0, 0, 1).await?.last().map_or(0, |e| e.seq);
        }
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                _ = tokio::signal::ctrl_c() => break,
            }
            let new = fetch(last, 0, 1000).await?;
            if let Some(e) = new.last() {
                last = e.seq;
            }
            print(&new)?;
        }
    }
    client.close().await;
    Ok(0)
}

// ---------------------------------------------------------------------------
// alerts and webhooks
// ---------------------------------------------------------------------------

fn print_alerts(v: &AlertsView, t: Option<&AlertTuningView>) {
    println!("rules:");
    for (rule, text) in v.policy.describe() {
        println!("  {rule:<15} {text}");
    }
    if let Some(t) = t {
        println!(
            "  {:<15} {}",
            "repeat",
            t.tuning
                .repeat_secs
                .map_or("off (one notice per alert)".into(), |r| format!(
                    "every {} while firing",
                    human_duration(r)
                ))
        );
        println!(
            "  {:<15} {}",
            "resolved",
            if t.tuning.resolved {
                "a notice when an alert stops firing"
            } else {
                "off"
            }
        );
        for o in &t.tuning.overrides {
            println!("  on {:<12} {}", o.nodes.to_string(), o.describe(false));
        }
    }
    println!();
    if v.firing.is_empty() {
        println!("firing: nothing");
    } else {
        println!("firing:");
        for a in &v.firing {
            let quiet = t.is_some_and(|t| {
                t.silenced
                    .iter()
                    .any(|(r, s)| *r == a.rule && *s == a.subject)
            });
            println!(
                "  {:<15} {:<8} since {}  {}{}",
                a.rule,
                a.level,
                local_time(a.since_unix, true),
                a.message,
                if quiet { " (silenced)" } else { "" }
            );
        }
    }
    if let Some(t) = t
        && !t.silences.is_empty()
    {
        println!();
        println!("silences:");
        for s in &t.silences {
            println!(
                "  {}  {:<30} until {}  by {}{}",
                s.id,
                s.describe(),
                local_time(s.until_unix, true),
                s.by,
                if s.comment.is_empty() {
                    String::new()
                } else {
                    format!(": {}", s.comment)
                }
            );
        }
    }
    println!();
    if v.webhooks.is_empty() {
        println!("webhooks: none (add one with `pumasi webhook add NAME URL`)");
    } else {
        print_webhooks(v);
    }
}

fn print_webhooks(v: &AlertsView) {
    println!(
        "{:<16} {:<8} {:<36} {:>5} {:>6}  LAST",
        "WEBHOOK", "FORMAT", "TARGET", "SENT", "FAILED"
    );
    for w in &v.webhooks {
        let last = match (&w.last_unix, &w.last_error) {
            (None, _) => "-".to_string(),
            (Some(t), None) => format!("ok at {}", local_time(*t, true)),
            (Some(t), Some(e)) => format!("failed at {}: {e}", local_time(*t, true)),
        };
        println!(
            "{:<16} {:<8} {:<36} {:>5} {:>6}  {last}",
            truncate(&w.name, 16),
            w.format.to_string(),
            truncate(&w.target, 36),
            w.sent,
            w.failed
        );
    }
}

async fn alerts(client: &AdminClient) -> Result<AlertsView> {
    match client.call(AdminOp::Alerts).await? {
        AdminResponse::Alerts(a) => Ok(*a),
        _ => bail!("unexpected response"),
    }
}

/// The 1.6 settings (a control plane older than 1.6 does not answer).
async fn tuning(client: &AdminClient) -> Result<AlertTuningView> {
    match client
        .call(AdminOp::AlertTuning)
        .await
        .context("repeat notices, per-node thresholds and silences need a 1.6 control plane")?
    {
        AdminResponse::AlertTuning(t) => Ok(*t),
        _ => bail!("unexpected response"),
    }
}

/// A node name or a label selector: a bare word that is no selector
/// syntax names a node.
fn node_target(s: &str) -> Result<Selector> {
    let s = s.trim();
    let sel = if s.contains(['=', ',', '!']) {
        s.parse::<Selector>()?
    } else {
        format!("name={s}").parse::<Selector>()?
    };
    sel.validate()?;
    Ok(sel)
}

async fn set_tuning(client: &AdminClient, t: AlertTuning) -> Result<AlertTuningView> {
    match client.call(AdminOp::SetAlertTuning(Box::new(t))).await? {
        AdminResponse::AlertTuning(t) => Ok(*t),
        _ => bail!("unexpected response"),
    }
}

/// `95`, `95C`, `95°C` or `off`.
fn parse_celsius(v: &str) -> Result<Option<u16>> {
    if pumasi_proto::monitor::is_off(v) {
        return Ok(None);
    }
    Ok(Some(
        v.trim()
            .trim_end_matches('C')
            .trim_end_matches('°')
            .trim()
            .parse()
            .with_context(|| format!("invalid temperature {v:?}"))?,
    ))
}

pub async fn alert(home: &Path, cmd: AlertCmd) -> Result<i32> {
    match cmd {
        AlertCmd::Ls { admin } => {
            let client = connect_admin(home, &admin).await?;
            let v = alerts(&client).await?;
            let t = tuning(&client).await.ok();
            client.close().await;
            print_alerts(&v, t.as_ref());
        }
        AlertCmd::Silence {
            rule,
            on,
            duration,
            comment,
            admin,
        } => {
            let spec = SilenceSpec {
                rule,
                nodes: on.as_deref().map(node_target).transpose()?,
                secs: parse_duration_ms(&duration)? / 1000,
                comment,
            };
            spec.validate()?;
            let client = connect_admin(home, &admin).await?;
            let t = match client
                .call(AdminOp::Silence(Box::new(spec.clone())))
                .await?
            {
                AdminResponse::AlertTuning(t) => *t,
                _ => bail!("unexpected response"),
            };
            let v = alerts(&client).await?;
            client.close().await;
            if let Some(s) = t.silences.iter().max_by_key(|s| s.created_unix) {
                println!(
                    "silenced {} until {} (id {})",
                    s.describe(),
                    local_time(s.until_unix, true),
                    s.id
                );
                println!();
            }
            print_alerts(&v, Some(&t));
        }
        AlertCmd::Unsilence { id, admin } => {
            let client = connect_admin(home, &admin).await?;
            client.call(AdminOp::Unsilence { id: id.clone() }).await?;
            client.close().await;
            println!("silence {id} ended");
        }
        AlertCmd::Set { rules, admin } => {
            let AlertRules {
                node_offline,
                failure_rate,
                failure_window,
                failure_min_runs,
                disk,
                temperature,
                perf_drop,
                smart,
                check_mismatch,
                defaults,
                repeat,
                resolved,
                on,
            } = *rules;
            let client = connect_admin(home, &admin).await?;
            if let Some(target) = on {
                ensure!(
                    failure_rate.is_none()
                        && failure_window.is_none()
                        && failure_min_runs.is_none()
                        && perf_drop.is_none()
                        && smart.is_none()
                        && check_mismatch.is_none()
                        && !defaults
                        && repeat.is_none()
                        && resolved.is_none(),
                    "with --on, only --node-offline, --disk and --temperature can be set"
                );
                let nodes = node_target(&target)?;
                let mut t = tuning(&client).await?;
                let inherit =
                    |v: &Option<String>| v.as_deref().is_some_and(|v| v.trim() == "inherit");
                let i = match t.tuning.overrides.iter().position(|o| o.nodes == nodes) {
                    Some(i) => i,
                    None => {
                        t.tuning.overrides.push(AlertOverride {
                            nodes,
                            node_offline_secs: None,
                            disk_percent: None,
                            temp_c: None,
                        });
                        t.tuning.overrides.len() - 1
                    }
                };
                let o = &mut t.tuning.overrides[i];
                if let Some(v) = &node_offline {
                    o.node_offline_secs = (!inherit(&node_offline))
                        .then(|| parse_opt_secs(v))
                        .transpose()?;
                }
                if let Some(v) = &disk {
                    o.disk_percent = (!inherit(&disk))
                        .then(|| parse_opt_percent(v))
                        .transpose()?;
                }
                if let Some(v) = &temperature {
                    o.temp_c = (!inherit(&temperature))
                        .then(|| parse_celsius(v))
                        .transpose()?;
                }
                t.tuning.overrides.retain(|o| !o.is_empty());
                let t = set_tuning(&client, t.tuning).await?;
                let v = alerts(&client).await?;
                client.close().await;
                print_alerts(&v, Some(&t));
                return Ok(0);
            }
            if repeat.is_some() || resolved.is_some() {
                let mut t = tuning(&client).await?;
                if let Some(v) = repeat {
                    t.tuning.repeat_secs = parse_opt_secs(&v)?;
                }
                if let Some(v) = resolved {
                    t.tuning.resolved = parse_switch(&v)?;
                }
                set_tuning(&client, t.tuning).await?;
            }
            let rules_given = node_offline.is_some()
                || failure_rate.is_some()
                || failure_window.is_some()
                || failure_min_runs.is_some()
                || disk.is_some()
                || temperature.is_some()
                || perf_drop.is_some()
                || smart.is_some()
                || check_mismatch.is_some()
                || defaults;
            let mut p = if defaults {
                AlertPolicy::default()
            } else {
                alerts(&client).await?.policy
            };
            if let Some(v) = node_offline {
                p.node_offline_secs = parse_opt_secs(&v)?;
            }
            if let Some(v) = failure_rate {
                p.failure_rate = parse_opt_percent(&v)?.map(|percent| FailureRate {
                    percent,
                    ..p.failure_rate.unwrap_or_default()
                });
            }
            if failure_window.is_some() || failure_min_runs.is_some() {
                let Some(f) = p.failure_rate.as_mut() else {
                    bail!("the failure rate rule is off; turn it on with --failure-rate");
                };
                if let Some(w) = failure_window {
                    f.window_secs = parse_duration_ms(&w)? / 1000;
                }
                if let Some(n) = failure_min_runs {
                    f.min_runs = n;
                }
            }
            if let Some(v) = disk {
                p.disk_percent = parse_opt_percent(&v)?;
            }
            if let Some(v) = temperature {
                p.temp_c = parse_celsius(&v)?;
            }
            if let Some(v) = perf_drop {
                p.perf_drop_percent = parse_opt_percent(&v)?;
            }
            if let Some(v) = smart {
                p.smart = parse_switch(&v)?;
            }
            if let Some(v) = check_mismatch {
                p.check_mismatch = parse_switch(&v)?;
            }
            let v = if rules_given {
                match client.call(AdminOp::SetAlertPolicy(Box::new(p))).await? {
                    AdminResponse::Alerts(a) => *a,
                    _ => bail!("unexpected response"),
                }
            } else {
                alerts(&client).await?
            };
            let t = tuning(&client).await.ok();
            client.close().await;
            print_alerts(&v, t.as_ref());
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// net
// ---------------------------------------------------------------------------

/// `pumasi net`: round trips between nodes as a matrix (row: from, column:
/// to), then each pair's bandwidth.
pub async fn net(home: &Path, admin: &AdminArgs, output: Output) -> Result<i32> {
    let v = match call(home, admin, AdminOp::Net)
        .await
        .context("measuring the network between nodes needs a 1.6 control plane")?
    {
        AdminResponse::Net(v) => *v,
        _ => bail!("unexpected response"),
    };
    if let Output::Json = output {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(0);
    }
    print!("{}", render_net(&v));
    Ok(0)
}

pub fn render_net(v: &NetView) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    if v.links.is_empty() {
        let _ = writeln!(
            out,
            "nothing measured yet: agents of 1.6 or later probe each other {}",
            if v.every_secs == 0 {
                "never: the control plane was started with --net-probe-every off".to_string()
            } else {
                format!(
                    "one pair every {} (a matrix of {} nodes fills in about {})",
                    human_duration(v.every_secs),
                    v.nodes.len(),
                    human_duration(
                        v.every_secs
                            * (v.nodes.len() * v.nodes.len().saturating_sub(1)).max(1) as u64
                    )
                )
            }
        );
        return out;
    }
    let nodes: Vec<&String> = v
        .nodes
        .iter()
        .filter(|n| v.links.iter().any(|l| l.from == **n || l.to == **n))
        .collect();
    let w = nodes
        .iter()
        .map(|n| n.chars().count())
        .max()
        .unwrap_or(4)
        .clamp(4, 16);
    let _ = writeln!(out, "round trip (row: from, column: to)");
    let _ = write!(out, "{:<w$} ", "");
    for n in &nodes {
        let _ = write!(out, " {:>10}", truncate(n, 10));
    }
    let _ = writeln!(out);
    for a in &nodes {
        let _ = write!(out, "{:<w$} ", truncate(a, w));
        for b in &nodes {
            let cell = if a == b {
                "-".to_string()
            } else {
                match v.link(a, b) {
                    Some(l) if l.error.is_some() => "failed".into(),
                    Some(l) => l.rtt_us.map(human_rtt).unwrap_or_else(|| "?".into()),
                    None => "".into(),
                }
            };
            let _ = write!(out, " {cell:>10}");
        }
        let _ = writeln!(out);
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "{:<16} {:<16} {:>10} {:>12} {:<7} {:<16} ERROR",
        "FROM", "TO", "RTT", "BANDWIDTH", "PATH", "MEASURED"
    );
    for l in &v.links {
        let _ = writeln!(
            out,
            "{:<16} {:<16} {:>10} {:>12} {:<7} {:<16} {}",
            truncate(&l.from, 16),
            truncate(&l.to, 16),
            l.rtt_us.map(human_rtt).unwrap_or_else(|| "-".into()),
            l.bps.map(human_bps).unwrap_or_else(|| "-".into()),
            match l.direct {
                Some(true) => "direct",
                Some(false) => "relay",
                None => "-",
            },
            local_time(l.unix, true),
            l.error.as_deref().unwrap_or("")
        );
    }
    out
}

pub async fn webhook(home: &Path, cmd: WebhookCmd) -> Result<i32> {
    match cmd {
        WebhookCmd::Add {
            name,
            url,
            format,
            no_test,
            admin,
        } => {
            let format = format.unwrap_or_else(|| WebhookFormat::detect(&url));
            let spec = WebhookSpec {
                name: name.clone(),
                url,
                format,
            };
            spec.validate()?;
            let client = connect_admin(home, &admin).await?;
            client.call(AdminOp::SetWebhook(Box::new(spec))).await?;
            println!("webhook {name} added ({format})");
            if !no_test {
                match client
                    .call(AdminOp::TestWebhook { name: name.clone() })
                    .await
                {
                    Ok(_) => println!("test alert delivered"),
                    Err(e) => {
                        client.close().await;
                        bail!(
                            "{e:#}\n(the webhook is saved; fix the URL with `pumasi webhook add {name} URL` or remove it)"
                        );
                    }
                }
            }
            client.close().await;
        }
        WebhookCmd::Ls { admin } => {
            let client = connect_admin(home, &admin).await?;
            let v = alerts(&client).await?;
            client.close().await;
            if v.webhooks.is_empty() {
                println!("no webhooks");
            } else {
                print_webhooks(&v);
            }
        }
        WebhookCmd::Rm { name, admin } => {
            call(home, &admin, AdminOp::RemoveWebhook { name: name.clone() }).await?;
            println!("webhook {name} removed");
        }
        WebhookCmd::Test { name, admin } => {
            call(home, &admin, AdminOp::TestWebhook { name: name.clone() }).await?;
            println!("test alert delivered to {name}");
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::monitor::{ClusterMetrics, NodeMetrics};

    #[test]
    fn top_renders_nodes_with_and_without_metrics() {
        let m = MetricsView {
            nodes: vec![
                NodeMetricsView {
                    id: "a".into(),
                    name: "desktop".into(),
                    online: true,
                    slots: 8,
                    running: 3,
                    latest: Some(NodeMetrics {
                        cpu_percent: 45.0,
                        load1: 2.0,
                        memory_used: 6 << 30,
                        memory_total: 16 << 30,
                        disk_used: 55,
                        disk_total: 100,
                        temp_c: Some(62.0),
                        ..Default::default()
                    }),
                    net_rx_bps: 1 << 20,
                    ..Default::default()
                },
                NodeMetricsView {
                    id: "b".into(),
                    name: "laptop".into(),
                    ..Default::default()
                },
            ],
            cluster: ClusterMetrics {
                queued: vec![(10, 5), (0, 25)],
                running: 3,
                ..Default::default()
            },
        };
        let text = render_top(&m);
        assert!(text.contains("30 queued (p10: 5, p0: 25)"), "{text}");
        assert!(text.contains("desktop"));
        assert!(text.contains("45%"));
        assert!(text.contains("55%"));
        assert!(text.contains("62°C"));
        assert!(text.contains("laptop           offline"), "{text}");
    }
}
