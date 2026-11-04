//! `pumasi upgrade`: roll new agent binaries out to the cluster.

use std::{collections::HashSet, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use pumasi_cluster::AdminClient;
use pumasi_proto::{
    PROTOCOL_VERSION,
    msg::{AdminOp, AdminResponse, RELEASE_CHUNK, RolloutView, UpgradeState},
    release::{ReleaseFile, detect_target, hex, read_build_info, sha256},
};

use crate::{AdminArgs, connect_admin, short, truncate, utc};

#[derive(Subcommand)]
pub enum UpgradeCmd {
    /// Roll out new agent binaries, one per platform (for example the
    /// release assets pumasi-linux-x86_64 and pumasi-windows-x86_64.exe).
    /// The control plane signs each with the cluster key. Nodes that
    /// accept upgrades (your machines by default, provider machines only
    /// with --auto-upgrade) install the one for their platform once idle,
    /// and roll back if it does not work.
    Rollout {
        /// Binaries to roll out; the platform is read from each file.
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Allow binaries that speak another protocol version than this
        /// CLI (their agents may be unable to reconnect, and roll back).
        #[arg(long)]
        force: bool,
        /// Wait until every node has upgraded or cannot.
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long, e.g. 10m.
        #[arg(long, default_value = "15m")]
        timeout: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show the current rollout and where each node stands.
    Status {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Stop offering the current rollout. Nodes keep the binary they run.
    Cancel {
        #[command(flatten)]
        admin: AdminArgs,
    },
}

pub async fn run(home: &std::path::Path, cmd: UpgradeCmd) -> Result<i32> {
    match cmd {
        UpgradeCmd::Rollout {
            files,
            force,
            wait,
            timeout,
            admin,
        } => {
            let mut prepared = Vec::new();
            let mut targets = HashSet::new();
            for path in &files {
                let bytes =
                    std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
                let target = detect_target(&bytes)
                    .with_context(|| format!("{} is not a pumasi binary", path.display()))?;
                let Some(info) = read_build_info(&bytes) else {
                    bail!(
                        "{} has no pumasi build marker: not a pumasi binary, or one older than \
                         the first release with cluster upgrades",
                        path.display()
                    );
                };
                if info.protocol != PROTOCOL_VERSION && !force {
                    bail!(
                        "{} speaks protocol {}, this CLI {PROTOCOL_VERSION}; pass --force to roll it out anyway",
                        path.display(),
                        info.protocol
                    );
                }
                if !targets.insert(target.clone()) {
                    bail!("two binaries for {target}");
                }
                let file = ReleaseFile {
                    target,
                    version: info.version,
                    protocol: info.protocol,
                    sha256: sha256(&bytes),
                    size: bytes.len() as u64,
                };
                prepared.push((path.clone(), file, bytes));
            }
            let client = connect_admin(home, &admin).await?;
            for (path, file, bytes) in &prepared {
                eprint!(
                    "uploading {} ({}, {}, {:.1} MiB)",
                    path.display(),
                    file.target,
                    file.version,
                    file.size as f64 / (1 << 20) as f64
                );
                upload(&client, file, bytes).await?;
                eprintln!(" done");
            }
            let files = prepared.into_iter().map(|(_, f, _)| f).collect();
            let AdminResponse::Rollout(view) =
                client.call(AdminOp::Rollout { files, force }).await?
            else {
                bail!("unexpected response");
            };
            print_rollout(&view);
            if !wait {
                client.close().await;
                return Ok(0);
            }
            let timeout = Duration::from_millis(pumasi_proto::job::parse_duration_ms(&timeout)?);
            let code = wait_rollout(&client, timeout).await;
            client.close().await;
            code
        }
        UpgradeCmd::Status { admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Rollout(view) = client.call(AdminOp::RolloutStatus).await? else {
                bail!("unexpected response");
            };
            client.close().await;
            print_rollout(&view);
            Ok(0)
        }
        UpgradeCmd::Cancel { admin } => {
            let client = connect_admin(home, &admin).await?;
            client.call(AdminOp::CancelRollout).await?;
            client.close().await;
            println!("rollout cancelled; nodes keep the binary they run");
            Ok(0)
        }
    }
}

async fn upload(client: &AdminClient, file: &ReleaseFile, bytes: &[u8]) -> Result<()> {
    let mut offset = 0usize;
    loop {
        let end = (offset + RELEASE_CHUNK as usize).min(bytes.len());
        let resp = client
            .call(AdminOp::UploadRelease {
                sha256: file.sha256,
                size: file.size,
                offset: offset as u64,
                bytes: bytes[offset..end].to_vec(),
            })
            .await?;
        let AdminResponse::Uploaded { have } = resp else {
            bail!("unexpected response");
        };
        // The control plane may already have it.
        if have >= file.size {
            return Ok(());
        }
        offset = have as usize;
        eprint!(".");
    }
}

fn settled(state: &UpgradeState) -> bool {
    !matches!(
        state,
        UpgradeState::Waiting | UpgradeState::Draining | UpgradeState::Upgrading
    )
}

async fn wait_rollout(client: &AdminClient, timeout: Duration) -> Result<i32> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = String::new();
    loop {
        let AdminResponse::Rollout(view) = client.call(AdminOp::RolloutStatus).await? else {
            bail!("unexpected response");
        };
        let pending = view.nodes.iter().filter(|n| !settled(&n.state)).count();
        let line = format!("{pending} node(s) still upgrading");
        if line != last {
            eprintln!("{line}");
            last = line;
        }
        if pending == 0 {
            println!();
            print_rollout(&view);
            let failed = view
                .nodes
                .iter()
                .any(|n| matches!(n.state, UpgradeState::Failed(_)));
            return Ok(if failed { 1 } else { 0 });
        }
        if tokio::time::Instant::now() > deadline {
            print_rollout(&view);
            bail!("timed out waiting for the rollout");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn print_rollout(view: &RolloutView) {
    if view.releases.is_empty() {
        println!("no rollout in progress");
    } else {
        println!("rollout started {} UTC", utc(view.started_unix));
        println!();
        println!("{:<16} {:<20} SHA256", "PLATFORM", "VERSION");
        for r in &view.releases {
            let f = r.file();
            println!(
                "{:<16} {:<20} {}",
                f.target.to_string(),
                truncate(&f.version, 20),
                hex(&f.sha256)
            );
        }
    }
    println!();
    println!(
        "{:<12} {:<16} {:<16} {:<20} {:<14} STATE",
        "NODE", "NAME", "PLATFORM", "VERSION", "BINARY"
    );
    for n in &view.nodes {
        println!(
            "{:<12} {:<16} {:<16} {:<20} {:<14} {}",
            short(&n.id),
            truncate(&n.name, 16),
            n.target.to_string(),
            truncate(&n.version, 20),
            n.exe_sha256
                .map(|h| hex(&h)[..12].to_string())
                .unwrap_or_else(|| "-".into()),
            n.state
        );
    }
    if view.nodes.is_empty() {
        println!("(no nodes)");
    }
}
