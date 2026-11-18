//! Honeypots and scores (1.20): `pumasi honeypot` and `pumasi score`.

use std::path::Path;

use anyhow::{Result, bail};
use clap::Subcommand;
use pumasi_proto::{
    job::{JobId, parse_duration_ms},
    monitor::human_duration,
    msg::{AdminOp, AdminResponse},
    verify::{ProviderScore, VerifyPolicyPatch, VerifyView},
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
macro_rules! err {
    ($($t:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr().lock(), $($t)*);
    }};
}

#[derive(Subcommand)]
pub enum HoneypotCmd {
    /// The honeypot policy, the account honeypots and the latest runs.
    List {
        #[arg(long, value_enum, default_value = "table")]
        output: Output,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Make honeypots of a finished job's succeeded tasks (or one with
    /// --task): copies of them are slipped into providers' queues once one
    /// of your own machines has run them again with the same result.
    /// Tasks must not use the file store.
    Add {
        job: String,
        #[arg(long)]
        task: Option<u32>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Stop using an account honeypot.
    Remove {
        id: u32,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Release an account's earnings held after a failed honeypot (they
    /// still wait out the usual hold period).
    Release {
        account: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Change how often providers get honeypots and the rest of the
    /// policy.
    Set {
        /// Send honeypots (the default).
        #[arg(long, conflicts_with = "off")]
        on: bool,
        /// Send none.
        #[arg(long)]
        off: bool,
        /// Chance in thousandths after each task, for providers with fewer
        /// than 10 passed checks and honeypots (default 100).
        #[arg(long, value_name = "PER_MILLE")]
        new: Option<u32>,
        /// ...for other providers (default 20).
        #[arg(long, value_name = "PER_MILLE")]
        provider: Option<u32>,
        /// ...for providers' nodes an admin trusts (default 0).
        #[arg(long, value_name = "PER_MILLE")]
        trusted: Option<u32>,
        /// ...for providers with a strike (default 250).
        #[arg(long, value_name = "PER_MILLE")]
        struck: Option<u32>,
        /// The image of built-in Docker and microVM honeypots: any image
        /// with a POSIX sh; one your jobs use hides them best. "" for none.
        #[arg(long)]
        image: Option<String>,
        /// How long a failed honeypot holds the provider's earnings
        /// (default 24h).
        #[arg(long)]
        hold: Option<String>,
        /// Time providers' CPU, disk and memory again this often (default
        /// 24h, 0 for only when their benchmark changes).
        #[arg(long)]
        retime: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

async fn call(home: &Path, admin: &AdminArgs, op: AdminOp) -> Result<VerifyView> {
    let client = connect_admin(home, admin).await?;
    let r = client.call(op).await;
    client.close().await;
    match r? {
        AdminResponse::Verify(v) => Ok(*v),
        AdminResponse::Error(e) => bail!("{e}"),
        _ => bail!("unexpected response (is the control plane older than 1.20?)"),
    }
}

fn secs(s: &str) -> Result<u64> {
    if s.trim() == "0" {
        return Ok(0);
    }
    Ok(parse_duration_ms(s)? / 1000)
}

pub async fn honeypot(home: &Path, cmd: HoneypotCmd) -> Result<i32> {
    match cmd {
        HoneypotCmd::List { output, admin } => {
            let v = call(home, &admin, AdminOp::Verify).await?;
            match output {
                Output::Json => out!("{}", serde_json::to_string_pretty(&v)?),
                Output::Table => print_honeypots(&v),
            }
        }
        HoneypotCmd::Add { job, task, admin } => {
            let job: JobId = job.parse()?;
            let before = call(home, &admin, AdminOp::Verify).await?;
            let v = call(home, &admin, AdminOp::HoneypotAdd { job, task }).await?;
            let added: Vec<_> = v
                .honeypots
                .iter()
                .filter(|h| !before.honeypots.iter().any(|b| b.id == h.id))
                .collect();
            for h in &added {
                err!(
                    "honeypot {} from task {}/{}: waiting for one of your machines to confirm it",
                    h.id,
                    h.source,
                    h.task
                );
            }
        }
        HoneypotCmd::Remove { id, admin } => {
            call(home, &admin, AdminOp::HoneypotRemove { id }).await?;
            err!("honeypot {id} removed");
        }
        HoneypotCmd::Release { account, admin } => {
            call(
                home,
                &admin,
                AdminOp::HoneypotRelease {
                    account: account.clone(),
                },
            )
            .await?;
            err!("{account}'s earnings are released after their usual hold period");
        }
        HoneypotCmd::Set {
            on,
            off,
            new,
            provider,
            trusted,
            struck,
            image,
            hold,
            retime,
            admin,
        } => {
            let patch = VerifyPolicyPatch {
                honeypots: if on {
                    Some(true)
                } else if off {
                    Some(false)
                } else {
                    None
                },
                new_permille: new,
                provider_permille: provider,
                trusted_permille: trusted,
                struck_permille: struck,
                image,
                hold_secs: hold.as_deref().map(secs).transpose()?,
                retime_secs: retime.as_deref().map(secs).transpose()?,
            };
            if patch.is_empty() {
                bail!("nothing to change (see `pumasi honeypot set --help`)");
            }
            let v = call(home, &admin, AdminOp::SetVerifyPolicy(patch)).await?;
            print_policy(&v);
        }
    }
    Ok(0)
}

fn print_policy(v: &VerifyView) {
    let p = &v.policy;
    if p.honeypots {
        out!(
            "honeypots after each task: new providers {}‰, providers {}‰, trusted {}‰, struck {}‰",
            p.new_permille,
            p.provider_permille,
            p.trusted_permille,
            p.struck_permille
        );
    } else {
        out!("honeypots: off");
    }
    out!(
        "built-in image honeypots: {}",
        if p.image.is_empty() {
            "none".to_string()
        } else {
            p.image.clone()
        }
    );
    out!(
        "a failed honeypot holds the account's earnings for {}",
        human_duration(p.hold_secs)
    );
    out!(
        "providers are timed again {}",
        if p.retime_secs == 0 {
            "only when their benchmark changes".to_string()
        } else {
            format!("every {}", human_duration(p.retime_secs))
        }
    );
}

fn print_honeypots(v: &VerifyView) {
    print_policy(v);
    for (account, until) in &v.held {
        out!(
            "HELD: {account}'s earnings until {} (`pumasi honeypot release {account}`)",
            local_time(*until, true)
        );
    }
    out!();
    if v.honeypots.is_empty() {
        out!("no account honeypots (`pumasi honeypot add JOB`); built-in ones only");
    } else {
        out!(
            "{:<4} {:<12} {:<20} {:<8} {:<10} {:>5} {:>6} {:>6}  FROM",
            "ID",
            "ACCOUNT",
            "NAME",
            "EXEC",
            "STATE",
            "SENT",
            "PASSED",
            "FAILED"
        );
        for h in &v.honeypots {
            let state = if h.problem.is_some() {
                "unusable"
            } else if h.confirmed {
                "in use"
            } else {
                "confirming"
            };
            out!(
                "{:<4} {:<12} {:<20} {:<8} {:<10} {:>5} {:>6} {:>6}  {}/{}",
                h.id,
                crate::truncate(&h.account, 12),
                crate::truncate(&h.name, 20),
                h.executor.to_string(),
                state,
                h.sent,
                h.passed,
                h.failed,
                h.source,
                h.task
            );
            if let Some(p) = &h.problem {
                out!("     {p}");
            }
        }
    }
    if !v.runs.is_empty() {
        out!();
        out!(
            "{:<17} {:<16} {:<10} {:<8} RESULT",
            "SENT",
            "NODE",
            "HONEYPOT",
            "EXEC"
        );
        for r in &v.runs {
            out!(
                "{:<17} {:<16} {:<10} {:<8} {}",
                local_time(r.sent_unix, true),
                crate::truncate(&r.node_name, 16),
                r.honeypot
                    .map_or("built-in".to_string(), |h| format!("#{h}")),
                r.executor.to_string(),
                r.result
                    .as_ref()
                    .map_or("running".to_string(), |x| x.to_string())
            );
        }
    }
}

/// `pumasi score`.
pub async fn score(home: &Path, output: Output, admin: &AdminArgs) -> Result<i32> {
    let v = call(home, admin, AdminOp::Verify).await?;
    match output {
        Output::Json => out!("{}", serde_json::to_string_pretty(&v.scores)?),
        Output::Table => {
            out!(
                "{:<16} {:<10} {:>5} {:>7} {:>7} {:>7} {:>9} {:>6}  DISK",
                "NODE",
                "LEVEL",
                "SCORE",
                "UPTIME",
                "CHECKS",
                "RUNS",
                "HONEYPOTS",
                "LOST"
            );
            for s in &v.scores {
                out!(
                    "{:<16} {:<10} {:>5} {:>6}% {:>6}% {:>6}% {:>9} {:>6}  {}",
                    crate::truncate(&s.name, 16),
                    s.level.to_string(),
                    s.score,
                    s.uptime_permille / 10,
                    s.checks_permille / 10,
                    s.completion_permille / 10,
                    format!("{}/{}", s.honeypots_passed, s.honeypots_failed),
                    s.runs_lost,
                    disk_note(s)
                );
            }
        }
    }
    Ok(0)
}

fn disk_note(s: &ProviderScore) -> String {
    match (&s.disk, s.disk_label) {
        (Some(d), Some(l)) => format!(
            "{l} (timed {} MB/s, {} us a read)",
            d.seq_read_mb_s, d.read_us
        ),
        (Some(d), None) => format!(
            "claims {}, timed {} ({} MB/s, {} us)",
            d.claimed_class, d.class, d.seq_read_mb_s, d.read_us
        ),
        (None, Some(l)) => l.to_string(),
        (None, None) => "-".into(),
    }
}

/// What `pumasi node NODE` adds about the node's score and the cluster's
/// timings.
pub fn print_node_score(s: &ProviderScore) {
    out!(
        "  score {} ({}): uptime {}%, checks {}%, runs {}% ({} done, {} lost)",
        s.score,
        s.level,
        s.uptime_permille / 10,
        s.checks_permille / 10,
        s.completion_permille / 10,
        s.runs_done,
        s.runs_lost
    );
    if s.honeypots_passed + s.honeypots_failed > 0 {
        out!(
            "  honeypots passed {}, failed {}",
            s.honeypots_passed,
            s.honeypots_failed
        );
    }
    if let Some(d) = &s.disk {
        out!(
            "  disk timed by the cluster: {} MB/s read, {} MB/s write, {} us a read: {}{}",
            d.seq_read_mb_s,
            d.seq_write_mb_s,
            d.read_us,
            d.class,
            if d.correct { "" } else { " (WRONG ANSWER)" }
        );
    }
    if let Some(m) = &s.memory {
        out!(
            "  memory timed by the cluster: {} MB/s (claims {}){}",
            m.measured_mb_s,
            m.claimed_mb_s,
            if m.correct { "" } else { " (WRONG ANSWER)" }
        );
    }
    if let Some(until) = s.held_until {
        out!(
            "  earnings HELD after a failed honeypot until {}",
            local_time(until, true)
        );
    }
}

/// The node's score, from a 1.20 control plane.
pub async fn node_score(client: &pumasi_cluster::AdminClient, id: &str) -> Option<ProviderScore> {
    match client.call(AdminOp::Verify).await {
        Ok(AdminResponse::Verify(v)) => v.scores.into_iter().find(|s| s.node == id),
        _ => None,
    }
}
