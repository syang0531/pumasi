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
    /// Serve HTTPS with a certificate for this name from Let's Encrypt (or
    /// `--acme-directory`), ordered and renewed by the dashboard itself
    /// (repeatable; the first is the main name). The name must point at
    /// this machine, and the CA must reach port 443 (TLS-ALPN-01: listen
    /// on :443 or forward it here) or port 80 (`--acme-challenge http-01`).
    #[arg(long, value_name = "DOMAIN", conflicts_with = "tls_cert")]
    acme: Vec<String>,
    /// A contact address for the CA (expiry notices).
    #[arg(long, requires = "acme")]
    acme_email: Option<String>,
    /// The ACME directory URL. Default: Let's Encrypt;
    /// `letsencrypt-staging` for its staging server.
    #[arg(long, requires = "acme")]
    acme_directory: Option<String>,
    /// Also trust this CA (PEM) for the ACME server itself (a private CA
    /// such as step-ca, or a test server).
    #[arg(long, requires = "acme")]
    acme_ca: Option<PathBuf>,
    /// How to prove the name: `tls-alpn-01` (on the dashboard's HTTPS
    /// port) or `http-01` (on a plain HTTP listener that also sends
    /// browsers to HTTPS).
    #[arg(long, default_value = "tls-alpn-01", requires = "acme")]
    acme_challenge: pumasi_dashboard::acme::Challenge,
    /// Where the `http-01` listener binds.
    #[arg(long, default_value = "0.0.0.0:80", requires = "acme")]
    acme_http_listen: SocketAddr,
    /// A header with the client's address, trusted only on connections
    /// from this machine (a tunnel or reverse proxy here) and from
    /// `--trusted-proxy` addresses, for limiting failed sign-ins per
    /// client: `CF-Connecting-IP` behind Cloudflare Tunnel,
    /// `X-Forwarded-For` behind most proxies (its last address is used:
    /// the one the proxy added).
    #[arg(long)]
    real_ip_header: Option<String>,
    /// A reverse proxy on another machine whose `--real-ip-header` is
    /// trusted too: its address as this machine sees it, or a network
    /// such as 192.168.1.0/24 (repeatable). Anything that can connect
    /// from such an address picks the address it is counted under, so
    /// let only the proxy reach this port.
    #[arg(long, value_name = "ADDR[/PREFIX]", requires = "real_ip_header")]
    trusted_proxy: Vec<pumasi_dashboard::TrustedProxy>,
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
    /// Keep running the old code when another pumasi is installed over
    /// this binary (by default the dashboard restarts into it; signed-in
    /// browsers then sign in again).
    #[arg(long)]
    no_follow_binary: bool,
    #[command(flatten)]
    admin: AdminArgs,
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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
        // Back down too, when the control plane on this binary rolls an
        // upgrade back (1.13).
        crate::follow::Follow::new().map(|f| f.also_older())
    };
    let own_exe = follow.as_ref().map(|f| f.exe().to_path_buf());
    let (ticket, source) = load_ticket_with_source(home, &args.admin)?;
    let net = net_for_all(&ticket.candidates(), None);
    let probe = (ticket.clone(), ticket_hint(home, &ticket, &source));
    let ticket_for_install = ticket.clone();
    let upload_limit = parse_size(&args.max_upload)?;
    let acme = if args.acme.is_empty() {
        None
    } else {
        let directory = match args.acme_directory.as_deref() {
            None | Some("letsencrypt") => pumasi_dashboard::acme::LETS_ENCRYPT.to_string(),
            Some("letsencrypt-staging") => pumasi_dashboard::acme::STAGING.to_string(),
            Some(url) => url.to_string(),
        };
        Some(pumasi_dashboard::acme::Acme::new(
            pumasi_dashboard::acme::AcmeConfig {
                domains: args.acme.clone(),
                email: args.acme_email.clone(),
                directory,
                ca: args.acme_ca.clone(),
                cache: pumasi_dashboard::acme::default_cache(home),
                challenge: args.acme_challenge,
                http_listen: args.acme_http_listen,
            },
        )?)
    };
    let tls = match (&args.tls_cert, &args.tls_key, &acme) {
        (Some(cert), Some(key), _) => Some(pumasi_dashboard::tls_config(cert, key)?),
        (_, _, Some(acme)) => Some(acme.server_config()),
        _ => None,
    };
    // Restarted into a new binary: on Windows the old process lets go of
    // its port only as it exits, just after starting this one (1.13).
    let restarted = std::env::var_os(crate::follow::RESTARTED_ENV).is_some();
    let listener = match args.listen {
        Some(addr) => bind_retrying(addr, restarted)
            .await
            .with_context(|| format!("listen on {addr}"))?,
        None => {
            let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
            match bind_retrying(SocketAddr::new(ip, DEFAULT_PORT), restarted).await {
                Ok(l) => l,
                Err(_) => tokio::net::TcpListener::bind((ip, 0)).await?,
            }
        }
    };
    let addr = listener.local_addr()?;
    if !addr.ip().is_loopback() && tls.is_none() && args.api_only && !args.insecure_http {
        anyhow::bail!(
            "refusing to serve the API on {addr} over plain HTTP: bearer tickets would cross the network unencrypted. Give --tls-cert and --tls-key or --acme, listen on 127.0.0.1 behind a tunnel or TLS proxy, or pass --insecure-http"
        );
    }
    if !addr.ip().is_loopback() && tls.is_none() {
        eprintln!(
            "warning: serving on {addr} over plain HTTP; tickets typed into the sign-in page cross the network unencrypted. Put a TLS reverse proxy in front."
        );
    }
    if !addr.ip().is_loopback() && args.real_ip_header.is_some() && args.trusted_proxy.is_empty() {
        eprintln!(
            "warning: --real-ip-header is trusted only on connections from this machine. Behind a reverse proxy on another machine, every client counts as the proxy's address (one client's failed sign-ins block them all) until you name it: --trusted-proxy ADDR"
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
            trusted_proxies: args.trusted_proxy.clone(),
            public_url: args.public_url.clone().or_else(|| {
                acme.as_ref().map(|a| match addr.port() {
                    443 => format!("https://{}", a.domain()),
                    port => format!("https://{}:{port}", a.domain()),
                })
            }),
        },
    )
    .await?;
    // Order once the port is open: the CA's TLS-ALPN-01 check comes in
    // through it.
    let _acme = acme.as_ref().map(|a| {
        let a = a.clone();
        tokio::spawn(async move {
            if let Err(e) = a.run().await {
                eprintln!("error: ACME: {e:#}");
            }
        })
    });
    let _acme = _acme.map(AbortOnDrop);
    let url = match &acme {
        // The certificate names the domain, not the address.
        Some(a) if args.public_url.is_none() => {
            let path = ui
                .url()
                .splitn(4, '/')
                .nth(3)
                .unwrap_or_default()
                .to_string();
            match addr.port() {
                443 => format!("https://{}/{path}", a.domain()),
                port => format!("https://{}:{port}/{path}", a.domain()),
            }
        }
        _ => ui.url(),
    };
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
    if !args.no_open && !args.api_only && !restarted {
        open_browser(&url);
    }
    // A newer binary installed over this one: restart into it (signed-in
    // browsers sign in again).
    let stop = crate::follow::until_stop(shutdown, follow, |_| async { Ok(()) });
    let install = async {
        match &own_exe {
            Some(exe) => install_rollout_binary(home, &ticket_for_install, net, exe).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        r = ui.wait() => r?,
        never = install => match never {},
        stop = stop => {
            if let crate::follow::Stop::Upgrade { version, exe } = stop {
                eprintln!("restarting the dashboard into pumasi {version}");
                return crate::follow::restart(in_service, &exe);
            }
        }
    }
    Ok(0)
}

/// Binds `addr`; when `retry`, keeps trying for 10 seconds while the
/// address is in use.
async fn bind_retrying(addr: SocketAddr, retry: bool) -> std::io::Result<tokio::net::TcpListener> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Err(e)
                if retry
                    && e.kind() == std::io::ErrorKind::AddrInUse
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            r => return r,
        }
    }
}

/// A dashboard on a machine with neither a control plane nor an agent
/// (whose upgrades replace the shared binary) installs the current
/// rollout's binary for its platform itself (1.13): signed by the cluster
/// key in its ticket, newer than its own, fetched from the control plane
/// and checked against the signed size and SHA-256. Following the binary
/// then restarts it. Each binary is tried once.
async fn install_rollout_binary(
    home: &Path,
    ticket: &pumasi_proto::ticket::AdminTicket,
    net: pumasi_proto::net::NetMode,
    exe: &Path,
) -> std::convert::Infallible {
    use pumasi_proto::{
        msg::{AdminOp, AdminResponse},
        release::{Target, is_older, read_build_info, sha256_file},
    };
    let every = std::env::var("PUMASI_TEST_OWN_UPGRADE_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map_or(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs,
        );
    let cluster_key = *ticket.cluster.id.as_bytes();
    let mut tried = std::collections::HashSet::new();
    let mut last_err: Option<String> = None;
    loop {
        tokio::time::sleep(every).await;
        if home.join("cluster").join("state.redb").exists()
            || home.join("agent").join("secret.key").exists()
        {
            continue;
        }
        let res: Result<()> = async {
            let client = AdminClient::connect(ticket, net).await?;
            let res = async {
                let AdminResponse::Rollout(view) = client.call(AdminOp::RolloutStatus).await?
                else {
                    anyhow::bail!("unexpected answer to RolloutStatus");
                };
                let Some(release) = view
                    .releases
                    .iter()
                    .find(|r| r.file().target == Target::host())
                else {
                    return Ok(());
                };
                let file = release.file().clone();
                if !is_older(pumasi_proto::version(), &file.version) || !tried.insert(file.sha256) {
                    return Ok(());
                }
                release
                    .verify(&cluster_key)
                    .context("the release is not signed by this cluster")?;
                let path = exe.to_path_buf();
                let on_disk = tokio::task::spawn_blocking(move || {
                    std::fs::read(&path).ok().and_then(|b| read_build_info(&b))
                })
                .await
                .ok()
                .flatten();
                if on_disk.is_some_and(|b| !is_older(&b.version, &file.version)) {
                    return Ok(());
                }
                eprintln!(
                    "installing pumasi {} over {} (the cluster's rollout)",
                    file.version,
                    exe.display()
                );
                let dir = home.join("ui-upgrade");
                std::fs::create_dir_all(&dir)?;
                let tmp = dir.join("download");
                let mut out = std::fs::File::create(&tmp)?;
                let mut offset = 0u64;
                while offset < file.size {
                    let AdminResponse::ReleaseChunk(bytes) = client
                        .call(AdminOp::ReleaseChunk {
                            sha256: file.sha256,
                            offset,
                        })
                        .await?
                    else {
                        anyhow::bail!("unexpected answer to ReleaseChunk");
                    };
                    anyhow::ensure!(!bytes.is_empty(), "the download stopped at {offset} bytes");
                    std::io::Write::write_all(&mut out, &bytes)?;
                    offset += bytes.len() as u64;
                }
                drop(out);
                let check = tmp.clone();
                let sha = tokio::task::spawn_blocking(move || sha256_file(&check)).await??;
                anyhow::ensure!(
                    offset == file.size && sha == file.sha256,
                    "the downloaded binary does not match the signed release"
                );
                let res = pumasi_agent::upgrade::replace_binary(
                    &tmp,
                    file.sha256,
                    &file.version,
                    exe,
                    true,
                )
                .await;
                let _ = std::fs::remove_dir_all(&dir);
                res?;
                eprintln!("installed pumasi {}", file.version);
                Ok(())
            }
            .await;
            client.close().await;
            res
        }
        .await;
        // Said once while it stays the same (the control plane may be down
        // for a while).
        let err = res.err().map(|e| format!("{e:#}"));
        if let Some(e) = &err
            && last_err.as_ref() != Some(e)
        {
            eprintln!("could not install the cluster's rollout binary: {e}");
        }
        last_err = err;
    }
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
