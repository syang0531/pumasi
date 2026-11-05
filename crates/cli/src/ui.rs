//! `pumasi ui`: the web dashboard (phase 12), served from this machine.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Args;
use pumasi_dashboard::UiConfig;
use pumasi_proto::store::parse_size;

use crate::{AdminArgs, load_ticket, net_for};

/// Port tried first when `--listen` is not given.
const DEFAULT_PORT: u16 = 7400;

#[derive(Args)]
pub struct UiArgs {
    /// Address to serve on. Default 127.0.0.1:7400, or another free port
    /// if that one is taken. On an address other than loopback every
    /// browser signs in with its own ticket, over plain HTTP: put a TLS
    /// reverse proxy in front.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// No sign-in link: every browser signs in with its own ticket (an
    /// access ticket from `pumasi access create`).
    #[arg(long)]
    login_only: bool,
    /// Do not open a browser.
    #[arg(long)]
    no_open: bool,
    /// Also write the URL to this file (for scripts; it holds the sign-in
    /// token).
    #[arg(long)]
    url_file: Option<PathBuf>,
    /// Largest upload from the browser (job inputs, datasets).
    #[arg(long, default_value = "4GiB")]
    max_upload: String,
    #[command(flatten)]
    admin: AdminArgs,
}

pub async fn run(home: &Path, args: UiArgs) -> Result<i32> {
    let ticket = load_ticket(home, &args.admin)?;
    let net = net_for(&ticket.cluster, None);
    let upload_limit = parse_size(&args.max_upload)?;
    let listener = match args.listen {
        Some(addr) => tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("listen on {addr}"))?,
        None => {
            let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
            match tokio::net::TcpListener::bind((ip, DEFAULT_PORT)).await {
                Ok(l) => l,
                Err(_) => tokio::net::TcpListener::bind((ip, 0)).await?,
            }
        }
    };
    let addr = listener.local_addr()?;
    if !addr.ip().is_loopback() {
        eprintln!(
            "warning: serving on {addr} over plain HTTP; tickets typed into the sign-in page cross the network unencrypted. Put a TLS reverse proxy in front."
        );
    }
    let ui = pumasi_dashboard::start_on(
        listener,
        UiConfig {
            ticket,
            net,
            listen: addr,
            login_only: args.login_only,
            upload_limit,
        },
    )
    .await?;
    let url = ui.url();
    if let Some(path) = &args.url_file {
        pumasi_proto::net::write_private(path, url.as_bytes())?;
    }
    eprintln!("dashboard: {url}");
    eprintln!("(Ctrl-C stops it)");
    if !args.no_open {
        open_browser(&url);
    }
    tokio::select! {
        r = ui.wait() => r?,
        _ = crate::ctrl_c() => {}
    }
    Ok(0)
}

/// Best effort: a machine without a desktop just shows the URL.
fn open_browser(url: &str) {
    let mut cmd = if cfg!(target_os = "macos") {
        let mut c = std::process::Command::new("open");
        c.arg(url);
        c
    } else if cfg!(windows) {
        let mut c = std::process::Command::new("rundll32");
        c.args(["url.dll,FileProtocolHandler", url]);
        c
    } else {
        let mut c = std::process::Command::new("xdg-open");
        c.arg(url);
        c
    };
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let _ = cmd.spawn();
}
