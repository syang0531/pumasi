//! `pumasi upgrade`: roll new agent binaries out to the cluster.

use std::{collections::HashSet, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use pumasi_cluster::AdminClient;
use pumasi_proto::{
    PROTOCOL_VERSION,
    msg::{AdminOp, AdminResponse, RELEASE_CHUNK, RolloutView, UpgradeState},
    release::{
        FetchState, LatestReleaseView, ReleaseFile, detect_target, hex, is_older, read_build_info,
        sha256,
    },
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
    /// Upgrade the cluster to the newest published release (or
    /// --version). The control plane downloads the binary of every
    /// platform its nodes run from GitHub, checks each against the
    /// release's SHA256SUMS, its platform and its build marker, and rolls
    /// them out like `pumasi upgrade rollout`. The control plane and the
    /// dashboard on a machine whose agent upgraded restart into the new
    /// binary once the rollout settles.
    Latest {
        /// A release tag such as v1.2.4 (default: the newest release).
        #[arg(long)]
        version: Option<String>,
        /// Only show the newest release and the nodes' versions.
        #[arg(long, conflicts_with_all = ["version", "force", "wait"])]
        check: bool,
        /// Allow binaries that speak another protocol version than the
        /// control plane.
        #[arg(long)]
        force: bool,
        /// Wait until every node has upgraded or cannot.
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long, e.g. 10m (the download
        /// counts too).
        #[arg(long, default_value = "30m")]
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
        UpgradeCmd::Latest {
            version,
            check,
            force,
            wait,
            timeout,
            admin,
        } => {
            let timeout = Duration::from_millis(pumasi_proto::job::parse_duration_ms(&timeout)?);
            let client = connect_admin(home, &admin).await?;
            let code = latest(&client, version, check, force, wait, timeout).await;
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

async fn latest_view(client: &AdminClient, check: bool) -> Result<LatestReleaseView> {
    match client.call(AdminOp::LatestRelease { check }).await? {
        AdminResponse::LatestRelease(v) => Ok(*v),
        _ => bail!("unexpected response"),
    }
}

async fn latest(
    client: &AdminClient,
    version: Option<String>,
    check: bool,
    force: bool,
    wait: bool,
    timeout: Duration,
) -> Result<i32> {
    if check {
        let v = latest_view(client, true).await?;
        match (&v.latest, &v.check_error) {
            (Some(tag), None) => println!("newest release: {tag} ({})", v.source),
            (Some(tag), Some(e)) => {
                println!("newest release: {tag} (seen earlier; checking now failed: {e})")
            }
            (None, Some(e)) => bail!("could not look up the newest release: {e}"),
            (None, None) => println!("newest release: unknown"),
        }
        println!("control plane: {}", v.control_plane);
        let AdminResponse::Rollout(view) = client.call(AdminOp::RolloutStatus).await? else {
            bail!("unexpected response");
        };
        println!();
        println!("{:<12} {:<16} {:<16} VERSION", "NODE", "NAME", "PLATFORM");
        for n in &view.nodes {
            let behind = v.latest.as_deref().is_some_and(|l| is_older(&n.version, l));
            println!(
                "{:<12} {:<16} {:<16} {}{}",
                short(&n.id),
                truncate(&n.name, 16),
                n.target.to_string(),
                n.version,
                if behind { "  (upgrade available)" } else { "" }
            );
        }
        if v.latest
            .as_deref()
            .is_some_and(|l| is_older(&v.control_plane, l))
        {
            println!();
            println!("run `pumasi upgrade latest` to upgrade");
        }
        return Ok(0);
    }
    let deadline = tokio::time::Instant::now() + timeout;
    let started = client
        .call(AdminOp::UpgradeToLatest { version, force })
        .await?;
    let AdminResponse::LatestRelease(_) = started else {
        bail!("unexpected response");
    };
    let mut last = String::new();
    let done = loop {
        let v = latest_view(client, false).await?;
        match v.fetch {
            FetchState::Running { version, step, .. } => {
                let line = format!("{version}: {step}");
                if line != last {
                    eprintln!("{line}");
                    last = line;
                }
            }
            FetchState::Done {
                version, summary, ..
            } => break (version, summary),
            FetchState::Failed {
                version, reason, ..
            } => bail!(
                "upgrade to {} failed: {reason}",
                version.as_deref().unwrap_or("the newest release")
            ),
            FetchState::Idle => bail!("the control plane forgot the download (did it restart?)"),
        }
        if tokio::time::Instant::now() > deadline {
            bail!("timed out waiting for the download");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    eprintln!("{}: {}", done.0, done.1);
    let AdminResponse::Rollout(view) = client.call(AdminOp::RolloutStatus).await? else {
        bail!("unexpected response");
    };
    print_rollout(&view);
    if !wait {
        return Ok(0);
    }
    let left = deadline.saturating_duration_since(tokio::time::Instant::now());
    wait_rollout(client, left).await
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
