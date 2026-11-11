//! `pumasi ui`: the web dashboard (phase 12), served from this machine.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Args;
use pumasi_cluster::AdminClient;
use pumasi_dashboard::UiConfig;
use pumasi_proto::store::parse_size;

use crate::{AdminArgs, load_ticket_with_source, net_for_all, ticket_hint};

/// Port tried first when `--listen` is not given.
pub const DEFAULT_PORT: u16 = 7400;

/// Where a dashboard service writes its sign-in link, in the home.
pub const URL_FILE: &str = "ui.url";

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
    /// Serve only the JSON API (`/api/v1`) for programs elsewhere: bearer
    /// tickets only, no pages and no sign-in link. Put it behind a
    /// Cloudflare Tunnel, a TLS reverse proxy, or `--tls-cert`.
    #[arg(long)]
    api_only: bool,
    /// Serve HTTPS with this certificate chain (PEM).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// The certificate's private key (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// A header with the client's address, trusted only on connections
    /// from this machine (a tunnel or reverse proxy here), for limiting
    /// failed sign-ins per client: `CF-Connecting-IP` behind Cloudflare
    /// Tunnel, `X-Forwarded-For` behind most proxies (its last address
    /// is used: the one the proxy here added).
    #[arg(long)]
    real_ip_header: Option<String>,
    /// Allow `--api-only` on an address other than loopback without
    /// `--tls-cert` (tickets then cross the network unencrypted; only for
    /// a TLS proxy on another machine of a private network).
    #[arg(long)]
    insecure_http: bool,
    /// The URL browsers use to reach this dashboard (behind a tunnel or
    /// proxy, its public address, e.g. https://pumasi.example.com). SSO
    /// sends browsers back to <URL>/sso/callback; register that at the
    /// identity provider.
    #[arg(long)]
    public_url: Option<String>,
    /// Keep running the old code when a newer pumasi is installed over
    /// this binary (by default the dashboard restarts into it; signed-in
    /// browsers then sign in again).
    #[arg(long)]
    no_follow_binary: bool,
    #[command(flatten)]
    admin: AdminArgs,
}

pub async fn run(home: &Path, args: UiArgs) -> Result<i32> {
    run_until(home, args, crate::ctrl_c(), false).await
}

/// Serves until `shutdown` resolves.
pub async fn run_until(
    home: &Path,
    args: UiArgs,
    shutdown: impl std::future::Future<Output = ()>,
    in_service: bool,
) -> Result<i32> {
    let follow = if args.no_follow_binary {
        None
    } else {
        crate::follow::Follow::new()
    };
    let (ticket, source) = load_ticket_with_source(home, &args.admin)?;
    let net = net_for_all(&ticket.candidates(), None);
    let probe = (ticket.clone(), ticket_hint(home, &ticket, &source));
    let upload_limit = parse_size(&args.max_upload)?;
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(pumasi_dashboard::tls_config(cert, key)?),
        _ => None,
    };
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
    if !addr.ip().is_loopback() && tls.is_none() && args.api_only && !args.insecure_http {
        anyhow::bail!(
            "refusing to serve the API on {addr} over plain HTTP: bearer tickets would cross the network unencrypted. Give --tls-cert and --tls-key, listen on 127.0.0.1 behind a tunnel or TLS proxy, or pass --insecure-http"
        );
    }
    if !addr.ip().is_loopback() && tls.is_none() {
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
            api_only: args.api_only,
            tls,
            real_ip_header: args.real_ip_header.clone(),
            public_url: args.public_url.clone(),
        },
    )
    .await?;
    let url = ui.url();
    if let Some(path) = &args.url_file {
        pumasi_proto::net::write_private(path, url.as_bytes())?;
    }
    if args.api_only {
        eprintln!("API: {}api/v1 (Authorization: Bearer <ticket>)", url);
        eprintln!("described at {url}api/openapi.json");
    } else {
        eprintln!("dashboard: {url}");
    }
    eprintln!("(Ctrl-C stops it)");
    // Check the control plane once now, so a wrong or stale ticket shows up
    // here and not only as an error page in the browser.
    tokio::spawn(async move {
        let (ticket, hint) = probe;
        match AdminClient::connect(&ticket, net).await {
            Ok(client) => client.close().await,
            Err(e) => eprintln!("warning: cannot reach the control plane: {e:#}; {hint}"),
        }
    });
    let restarted = std::env::var_os(crate::follow::RESTARTED_ENV).is_some();
    if !args.no_open && !args.api_only && !restarted {
        open_browser(&url);
    }
    // A newer binary installed over this one: restart into it (signed-in
    // browsers sign in again).
    let stop = crate::follow::until_stop(shutdown, follow, || true);
    tokio::select! {
        r = ui.wait() => r?,
        stop = stop => {
            if let crate::follow::Stop::Upgrade { version, exe } = stop {
                eprintln!("restarting the dashboard into pumasi {version}");
                return crate::follow::restart(in_service, &exe);
            }
        }
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
