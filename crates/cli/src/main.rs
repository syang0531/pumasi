//! `pumasi`: one binary for the control plane, the node agent and the
//! admin CLI.

use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use pumasi_agent::{Agent, AgentConfig, WasmExecutor, default_node_name};
use pumasi_cluster::{ADMIN_TICKET_FILE, AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    job::{JobFile, JobId, Limits, TaskInput, parse_duration_ms},
    msg::{AdminOp, AdminResponse, ClusterStatus, JobView, Outcome, TaskState},
    net::NetMode,
    ticket::{AdminTicket, JoinTicket},
};

#[derive(Parser)]
#[command(
    name = "pumasi",
    version,
    about = "Pool your machines into one cluster and run WebAssembly batch jobs on it."
)]
struct Cli {
    /// State directory (default: $PUMASI_HOME or ~/.pumasi).
    #[arg(long, global = true, env = "PUMASI_HOME")]
    home: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the cluster control plane.
    #[command(subcommand)]
    Cluster(ClusterCmd),
    /// Run a node agent that contributes this machine to a cluster.
    #[command(subcommand)]
    Agent(AgentCmd),
    /// Create a ticket that lets another machine join the cluster.
    Token {
        /// How long the ticket stays valid, e.g. 30m, 24h.
        #[arg(long, default_value = "24h")]
        ttl: String,
        /// Number of nodes that may join with it (default: unlimited).
        #[arg(long)]
        uses: Option<u32>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Submit a job described by a job.toml file.
    Run {
        job_file: PathBuf,
        /// Return right after submitting instead of waiting for the result.
        #[arg(long)]
        detach: bool,
        /// Print each task's stdout when the job finishes.
        #[arg(long, short)]
        output: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show nodes and recent jobs.
    Status {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show one job and its tasks.
    Job {
        id: String,
        /// Print each task's stdout and stderr.
        #[arg(long, short)]
        output: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Cancel a job.
    Cancel {
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Remove a node from the cluster (it must be re-invited to rejoin).
    RemoveNode {
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Save an admin ticket so this machine can manage a remote cluster.
    Login { ticket: String },
    /// Run a module locally in the same sandbox the agent uses.
    Exec {
        module: PathBuf,
        /// Arguments passed to the module.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
        #[arg(long)]
        fuel: Option<u64>,
        #[arg(long, default_value = "300s")]
        timeout: String,
        #[arg(long, default_value_t = 256)]
        memory_mb: u64,
    },
}

#[derive(Subcommand)]
enum ClusterCmd {
    /// Start the control plane (creates the cluster on first run).
    Start {
        /// Cluster name, used when the cluster is first created.
        #[arg(long, default_value = "my-cluster")]
        name: String,
        #[command(flatten)]
        net: NetArgs,
        /// Do not print a join ticket at startup.
        #[arg(long)]
        no_token: bool,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    /// Join a cluster with a ticket and start serving it.
    Join {
        ticket: String,
        #[command(flatten)]
        opts: AgentOpts,
    },
    /// Reconnect to the cluster this machine joined before.
    Start {
        #[command(flatten)]
        opts: AgentOpts,
    },
}

#[derive(Args, Clone)]
struct AgentOpts {
    /// Node name shown in `pumasi status` (default: hostname).
    #[arg(long)]
    name: Option<String>,
    /// Tasks to run at once (default: number of CPUs).
    #[arg(long)]
    slots: Option<u32>,
    #[command(flatten)]
    net: NetArgs,
}

#[derive(Args, Clone, Default)]
struct NetArgs {
    /// `public` (relays, works across NAT) or `local` (direct IP only).
    #[arg(long)]
    net: Option<NetMode>,
    /// Local UDP address to bind, e.g. 0.0.0.0:7480.
    #[arg(long)]
    bind: Option<SocketAddr>,
}

#[derive(Args, Clone)]
struct AdminArgs {
    /// Admin ticket (default: saved by `pumasi login` or by a local cluster).
    #[arg(long, env = "PUMASI_ADMIN", hide_env_values = true)]
    admin: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let long_running = matches!(cli.cmd, Cmd::Cluster(_) | Cmd::Agent(_));
    let default_filter = if long_running {
        "pumasi=info,warn"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("PUMASI_LOG")
                .unwrap_or_else(|_| default_filter.into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    match run(cli).await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(1);
        }
    }
}

fn home_dir(cli_home: &Option<PathBuf>) -> Result<PathBuf> {
    if let Some(h) = cli_home {
        return Ok(h.clone());
    }
    let base = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot find home directory; set PUMASI_HOME")?;
    Ok(PathBuf::from(base).join(".pumasi"))
}

/// Picks local mode for tickets that carry only IP addresses.
fn net_for(addr: &iroh::EndpointAddr, explicit: Option<NetMode>) -> NetMode {
    explicit.unwrap_or_else(|| {
        if addr.relay_urls().next().is_some() {
            NetMode::Public
        } else {
            NetMode::Local
        }
    })
}

async fn run(cli: Cli) -> Result<i32> {
    let home = home_dir(&cli.home)?;
    match cli.cmd {
        Cmd::Cluster(ClusterCmd::Start {
            name,
            net,
            no_token,
        }) => cluster_start(&home, name, net, no_token).await,
        Cmd::Agent(cmd) => agent(&home, cmd).await,
        Cmd::Token { ttl, uses, admin } => {
            let ttl_secs = parse_duration_ms(&ttl)? / 1000;
            let client = connect_admin(&home, &admin).await?;
            let resp = client
                .call(AdminOp::CreateJoinToken { ttl_secs, uses })
                .await?;
            client.close().await;
            let AdminResponse::JoinToken { ticket, .. } = resp else {
                bail!("unexpected response")
            };
            println!("{ticket}");
            Ok(0)
        }
        Cmd::Run {
            job_file,
            detach,
            output,
            admin,
        } => {
            let sub = JobFile::load(&job_file)?;
            let tasks = sub.tasks.len();
            let client = connect_admin(&home, &admin).await?;
            let AdminResponse::Submitted { id } =
                client.call(AdminOp::Submit(Box::new(sub))).await?
            else {
                bail!("unexpected response");
            };
            eprintln!("submitted job {id} ({tasks} tasks)");
            if detach {
                println!("{id}");
                client.close().await;
                return Ok(0);
            }
            let view = wait_for_job(&client, id).await?;
            client.close().await;
            print_job(&view, output);
            Ok(
                if view.summary.state == pumasi_proto::msg::JobState::Succeeded {
                    0
                } else {
                    2
                },
            )
        }
        Cmd::Status { admin } => {
            let client = connect_admin(&home, &admin).await?;
            let AdminResponse::Status(status) = client.call(AdminOp::Status).await? else {
                bail!("unexpected response");
            };
            client.close().await;
            print_status(&status);
            Ok(0)
        }
        Cmd::Job { id, output, admin } => {
            let id: JobId = id.parse()?;
            let client = connect_admin(&home, &admin).await?;
            let AdminResponse::Job(view) = client
                .call(AdminOp::Job {
                    id,
                    with_output: output,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            print_job(&view, output);
            Ok(0)
        }
        Cmd::Cancel { id, admin } => {
            let id: JobId = id.parse()?;
            let client = connect_admin(&home, &admin).await?;
            client.call(AdminOp::Cancel { id }).await?;
            client.close().await;
            eprintln!("cancelled job {id}");
            Ok(0)
        }
        Cmd::RemoveNode { id, admin } => {
            let client = connect_admin(&home, &admin).await?;
            client.call(AdminOp::RemoveNode { id: id.clone() }).await?;
            client.close().await;
            eprintln!("removed node {id}");
            Ok(0)
        }
        Cmd::Login { ticket } => {
            let t: AdminTicket = ticket.parse()?;
            let path = home.join(ADMIN_TICKET_FILE);
            pumasi_proto::net::write_private(&path, t.to_string().as_bytes())?;
            eprintln!("saved admin ticket to {}", path.display());
            Ok(0)
        }
        Cmd::Exec {
            module,
            args,
            fuel,
            timeout,
            memory_mb,
        } => exec_local(&module, args, fuel, &timeout, memory_mb).await,
    }
}

async fn cluster_start(home: &Path, name: String, net: NetArgs, no_token: bool) -> Result<i32> {
    let mut config = ClusterConfig::new(home.join("cluster"));
    config.name = name;
    config.net = net.net.unwrap_or_default();
    // A fixed port lets agents reconnect after a restart in local mode.
    config.bind = Some(net.bind.unwrap_or_else(|| "0.0.0.0:7480".parse().unwrap()));
    let cluster = Cluster::start(config.clone()).await?;
    let status = cluster.status();
    eprintln!();
    eprintln!(
        "cluster \"{}\" is running ({} known nodes)",
        status.cluster_name,
        status.nodes.len()
    );
    eprintln!(
        "admin ticket saved to {}",
        config.data_dir.join(ADMIN_TICKET_FILE).display()
    );
    if !no_token {
        let ticket = cluster.create_join_ticket(Duration::from_secs(24 * 3600), None)?;
        eprintln!();
        eprintln!("add a machine (ticket valid 24h):");
        eprintln!("  pumasi agent join {ticket}");
    }
    eprintln!();
    tokio::signal::ctrl_c().await?;
    eprintln!("shutting down");
    cluster.shutdown().await?;
    Ok(0)
}

async fn agent(home: &Path, cmd: AgentCmd) -> Result<i32> {
    let (ticket, opts) = match cmd {
        AgentCmd::Join { ticket, opts } => (Some(ticket.parse::<JoinTicket>()?), opts),
        AgentCmd::Start { opts } => (None, opts),
    };
    let data_dir = home.join("agent");
    let net = match &ticket {
        Some(t) => net_for(&t.cluster, opts.net.net),
        None => {
            let saved = std::fs::read_to_string(data_dir.join("membership.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<pumasi_agent::Membership>(&s).ok());
            match saved {
                Some(m) => net_for(&m.cluster, opts.net.net),
                None => opts.net.net.unwrap_or_default(),
            }
        }
    };
    let config = AgentConfig {
        data_dir,
        name: opts.name.unwrap_or_else(default_node_name),
        slots: opts.slots,
        net,
        bind: opts.net.bind,
    };
    let agent = Agent::new(config).await?;
    eprintln!(
        "agent {} ({}, {} slots) connecting...",
        agent.info().name,
        agent.id().fmt_short(),
        agent.info().slots
    );
    let serve = async move {
        match ticket {
            Some(t) => agent.join(t).await,
            None => agent.resume().await,
        }
    };
    tokio::select! {
        r = serve => r.map(|_| 0),
        _ = tokio::signal::ctrl_c() => {
            eprintln!("shutting down");
            Ok(0)
        }
    }
}

async fn connect_admin(home: &Path, args: &AdminArgs) -> Result<AdminClient> {
    let text = match &args.admin {
        Some(t) => t.clone(),
        None => {
            let candidates = [
                home.join(ADMIN_TICKET_FILE),
                home.join("cluster").join(ADMIN_TICKET_FILE),
            ];
            let found = candidates.iter().find(|p| p.exists()).with_context(|| {
                "no admin ticket found: start a cluster here, run `pumasi login <TICKET>`, or pass --admin"
            })?;
            std::fs::read_to_string(found)?
        }
    };
    let ticket: AdminTicket = text.parse()?;
    let net = net_for(&ticket.cluster, None);
    AdminClient::connect(&ticket, net).await
}

async fn wait_for_job(client: &AdminClient, id: JobId) -> Result<JobView> {
    let mut last = String::new();
    loop {
        let AdminResponse::Job(view) = client
            .call(AdminOp::Job {
                id,
                with_output: false,
            })
            .await?
        else {
            bail!("unexpected response");
        };
        let s = &view.summary;
        let line = format!(
            "{}: {}/{} done, {} running, {} failed",
            s.state,
            s.succeeded + s.failed,
            s.tasks,
            s.running,
            s.failed
        );
        if line != last {
            eprintln!("{line}");
            last = line;
        }
        if s.state.is_terminal() {
            let AdminResponse::Job(full) = client
                .call(AdminOp::Job {
                    id,
                    with_output: true,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            return Ok(*full);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn print_status(s: &ClusterStatus) {
    println!("cluster {} ({})", s.cluster_name, short(&s.cluster_id));
    println!();
    println!(
        "{:<12} {:<16} {:<8} {:<14} {:>5} {:>8} {:>10}",
        "NODE", "NAME", "STATE", "PLATFORM", "SLOTS", "RUNNING", "COMPLETED"
    );
    for n in &s.nodes {
        println!(
            "{:<12} {:<16} {:<8} {:<14} {:>5} {:>8} {:>10}",
            short(&n.id),
            truncate(&n.info.name, 16),
            if n.online { "online" } else { "offline" },
            format!("{}/{}", n.info.os, n.info.arch),
            n.info.slots,
            n.running,
            n.completed
        );
    }
    if s.nodes.is_empty() {
        println!("(no nodes yet: create a ticket with `pumasi token`)");
    }
    println!();
    println!(
        "{:<17} {:<20} {:<10} {:>7} {:>7} {:>7}",
        "JOB", "NAME", "STATE", "TASKS", "DONE", "FAILED"
    );
    for j in s.jobs.iter().take(20) {
        println!(
            "{:<17} {:<20} {:<10} {:>7} {:>7} {:>7}",
            j.id.to_string(),
            truncate(&j.name, 20),
            j.state.to_string(),
            j.tasks,
            j.succeeded,
            j.failed
        );
    }
    if s.jobs.is_empty() {
        println!("(no jobs yet: submit one with `pumasi run job.toml`)");
    }
}

fn print_job(v: &JobView, output: bool) {
    let s = &v.summary;
    println!("job {} \"{}\": {}", s.id, s.name, s.state);
    println!(
        "  tasks {}  succeeded {}  failed {}  fuel {}",
        s.tasks, s.succeeded, s.failed, v.total_fuel
    );
    let mut stdout = std::io::stdout().lock();
    for t in &v.tasks {
        let state = match &t.state {
            TaskState::Pending => "pending".to_string(),
            TaskState::Running { .. } => "running".to_string(),
            TaskState::Succeeded => "ok".to_string(),
            TaskState::Failed(o) => format!("failed ({o})"),
            TaskState::Cancelled => "cancelled".to_string(),
        };
        let show_line = output || !matches!(t.state, TaskState::Succeeded) || v.tasks.len() <= 20;
        if show_line {
            let _ = writeln!(
                stdout,
                "  [{}] {} node={} attempts={} {}ms",
                t.index,
                state,
                t.node.as_deref().map(short).unwrap_or("-"),
                t.attempts,
                t.wall_ms
            );
        }
        if output {
            if let Some(out) = &t.stdout
                && !out.is_empty()
            {
                let _ = stdout.write_all(out);
                if !out.ends_with(b"\n") {
                    let _ = stdout.write_all(b"\n");
                }
            }
            if let Some(err) = &t.stderr
                && !err.is_empty()
            {
                eprint!("{}", String::from_utf8_lossy(err));
            }
        }
    }
}

async fn exec_local(
    module: &Path,
    args: Vec<String>,
    fuel: Option<u64>,
    timeout: &str,
    memory_mb: u64,
) -> Result<i32> {
    let bytes = std::fs::read(module).with_context(|| format!("read {}", module.display()))?;
    let hash = pumasi_proto::module_hash(&bytes);
    let limits = Limits {
        memory_bytes: memory_mb * 1024 * 1024,
        fuel,
        timeout_ms: parse_duration_ms(timeout)?,
    };
    let program = module
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut stdin = Vec::new();
    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        std::io::Read::read_to_end(&mut std::io::stdin(), &mut stdin)?;
    }
    let input = TaskInput {
        args,
        stdin,
        ..Default::default()
    };
    let result = tokio::task::spawn_blocking(move || -> Result<_> {
        let ex = WasmExecutor::new()?;
        ex.load_module(hash, &bytes)?;
        Ok(ex.run(
            &hash,
            &program,
            &input,
            &limits,
            Arc::new(AtomicBool::new(false)),
        ))
    })
    .await??;
    std::io::stdout().write_all(&result.stdout)?;
    std::io::stderr().write_all(&result.stderr)?;
    eprintln!(
        "[pumasi] {}  fuel={}  {}ms",
        result.outcome, result.fuel_used, result.wall_ms
    );
    Ok(match result.outcome {
        Outcome::Exited(c) => c,
        _ => 125,
    })
}

fn short(id: &str) -> &str {
    &id[..id.len().min(10)]
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n - 1).collect::<String>() + "…"
    }
}
