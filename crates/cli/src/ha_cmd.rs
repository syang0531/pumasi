//! `pumasi ha` (control plane replicas), `pumasi backup` and `pumasi
//! cluster restore` (phase 13).

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use pumasi_proto::{
    ha::HaView,
    msg::{AdminOp, AdminResponse},
};

use crate::{AdminArgs, connect_admin, manage::Output as OutputArg, parse_duration_ms, short};

#[derive(Subcommand)]
pub enum HaCmd {
    /// Show the control plane replicas: who leads, who votes, how far
    /// each has the log.
    Status {
        #[arg(short = 'o', long, value_enum, default_value = "table")]
        output: OutputArg,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Make a ticket for one more control plane replica (run `pumasi
    /// cluster replica <TICKET>` on that machine). Turns replication on
    /// the first time. Use 3 or 5 replicas: a majority must be up.
    Invite {
        /// How long the ticket stays valid.
        #[arg(long, default_value = "1h")]
        ttl: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Take a replica out of the control plane (endpoint id or prefix).
    Remove {
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

pub async fn run(home: &Path, cmd: HaCmd) -> Result<i32> {
    match cmd {
        HaCmd::Status { output, admin } => {
            let client = connect_admin(home, &admin).await?;
            let resp = client.call(AdminOp::HaStatus).await?;
            client.close().await;
            let AdminResponse::Ha(view) = resp else {
                bail!("unexpected response")
            };
            match output {
                OutputArg::Json => println!("{}", serde_json::to_string_pretty(&*view)?),
                OutputArg::Table => print_status(&view),
            }
            Ok(0)
        }
        HaCmd::Invite { ttl, admin } => {
            let ttl_secs = parse_duration_ms(&ttl)? / 1000;
            let client = connect_admin(home, &admin).await?;
            let resp = client.call(AdminOp::HaInvite { ttl_secs }).await?;
            client.close().await;
            let AdminResponse::JoinToken { ticket, .. } = resp else {
                bail!("unexpected response")
            };
            eprintln!("on the new control plane machine (ticket valid {ttl}, once):");
            eprintln!("  pumasi cluster replica {ticket}");
            eprintln!();
            eprintln!(
                "afterwards, hand out new join and admin tickets (`pumasi token`, `pumasi access create`): they list every replica"
            );
            println!("{ticket}");
            Ok(0)
        }
        HaCmd::Remove { id, admin } => {
            let client = connect_admin(home, &admin).await?;
            let resp = client.call(AdminOp::HaRemove { id }).await?;
            client.close().await;
            let AdminResponse::Ha(view) = resp else {
                bail!("unexpected response")
            };
            print_status(&view);
            Ok(0)
        }
    }
}

fn print_status(v: &HaView) {
    if !v.enabled {
        println!("one control plane, no replicas (add one with `pumasi ha invite`)");
        return;
    }
    let voters = v.replicas.iter().filter(|r| r.voter).count();
    let up = v.voters_up(5_000);
    println!(
        "term {}  log {}  applied {}  snapshot {}",
        v.term,
        opt(v.committed),
        opt(v.applied),
        opt(v.snapshot)
    );
    println!(
        "{up} of {voters} voting replicas answering, {} needed",
        v.quorum()
    );
    if voters < 3 {
        println!("note: with fewer than 3 voting replicas, losing one stops the cluster");
    }
    println!();
    println!("{:<12} {:<9} {:>9} {:>10}", "REPLICA", "ROLE", "LOG", "SEEN");
    for r in &v.replicas {
        let role = if r.leader {
            "leader"
        } else if r.voter {
            "follower"
        } else {
            "learner"
        };
        let seen = match r.last_seen_ms {
            Some(0) if r.leader => "-".to_string(),
            Some(ms) => format!("{:.1}s ago", ms as f64 / 1000.0),
            None => "never".to_string(),
        };
        println!(
            "{:<12} {:<9} {:>9} {:>10}",
            short(&r.id),
            role,
            opt(r.matched),
            seen
        );
    }
}

fn opt(v: Option<u64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "-".into())
}

/// `pumasi backup FILE`: a consistent copy of the control plane's state.
pub async fn backup(home: &Path, admin: &AdminArgs, file: &PathBuf) -> Result<i32> {
    let client = connect_admin(home, admin).await?;
    let mut out: Vec<u8> = Vec::new();
    let mut expect: Option<([u8; 32], u64)> = None;
    loop {
        let resp = client
            .call(AdminOp::Backup {
                offset: out.len() as u64,
            })
            .await?;
        let AdminResponse::Backup {
            total,
            digest,
            bytes,
        } = resp
        else {
            bail!("unexpected response")
        };
        match expect {
            None => expect = Some((digest, total)),
            Some((d, _)) if d != digest => {
                // Someone else started a backup; start over.
                out.clear();
                expect = None;
                continue;
            }
            _ => {}
        }
        if bytes.is_empty() && (out.len() as u64) < total {
            bail!("the control plane sent an empty chunk");
        }
        out.extend(bytes);
        if out.len() as u64 >= total {
            break;
        }
    }
    client.close().await;
    let (digest, _) = expect.context("no backup")?;
    anyhow::ensure!(
        blake3::hash(&out).as_bytes() == &digest,
        "the backup did not arrive intact; run it again"
    );
    let tmp = file.with_extension("partial");
    {
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(&out)?;
        f.sync_all()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, file)?;
    eprintln!(
        "wrote {} ({}); it holds the cluster key and admin secret, keep it safe",
        file.display(),
        pumasi_proto::store::format_size(out.len() as u64)
    );
    eprintln!("the file store's data is not in it: back up the cluster directory's blobs/ too");
    Ok(0)
}

/// `pumasi cluster restore FILE`: a single control plane from a backup.
pub fn restore(home: &Path, file: &Path) -> Result<i32> {
    let bytes = std::fs::read(file).with_context(|| format!("read {}", file.display()))?;
    let dir = home.join("cluster");
    let r = pumasi_cluster::backup::restore(&bytes, &dir)?;
    eprintln!(
        "restored cluster \"{}\" ({}) into {} ({} rows, backed up at unix {})",
        r.cluster_name,
        short(&r.cluster_id),
        dir.display(),
        r.rows,
        r.created_unix
    );
    eprintln!(
        "start it with `pumasi cluster start`; nodes reconnect on their own when it is reachable where the old one was"
    );
    Ok(0)
}
