//! `pumasi`: one binary for the control plane, the node agent and the
//! admin CLI.

mod service;
mod upgrade;

use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use pumasi_agent::{
    Agent, AgentConfig, AgentExit, MicrovmConfig, MicrovmExecutor, UpgradeConfig, WasmExecutor,
    default_node_name,
    microvm::{Assets, Backend},
};
use pumasi_cluster::{ADMIN_TICKET_FILE, AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{Policy, format_credits, parse_credits, parse_rate_permille, validate_account},
    job::{
        Executor, JobFile, JobId, Limits, MAX_INPUT_FILES_BYTES, TaskInput, parse_duration_ms,
        read_files, write_files,
    },
    msg::{
        AccountView, AdminOp, AdminResponse, ClusterStatus, Invite, JobView, LedgerEntry,
        LedgerKind, Outcome, Role, TaskState, Verification,
    },
    net::NetMode,
    receipt::Receipt,
    ticket::{AdminTicket, JoinTicket},
};

#[derive(Parser)]
#[command(
    name = "pumasi",
    version = pumasi_proto::version(),
    about = "Pool your machines into one cluster and run batch jobs (WebAssembly or containers) on it."
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
    /// Invite someone else's machine to join as a provider. It runs WASM
    /// tasks and, if it opts in with --allow-microvm, image jobs in microVMs
    /// (Docker only with --allow-docker), earns credits for ACCOUNT, and has
    /// a sample of its WASM results checked.
    Invite {
        /// Account the machine earns for, usually its owner's name
        /// (a-z, 0-9, '-', '_').
        account: String,
        /// How long the invite stays valid, e.g. 24h, 7d.
        #[arg(long, default_value = "7d")]
        ttl: String,
        /// Number of machines that may join with it.
        #[arg(long, default_value_t = 1)]
        uses: u32,
        /// Also send Docker jobs to this machine (it must opt in with
        /// --allow-docker too). Containers give the provider full view of
        /// the job; use only for people you trust.
        #[arg(long)]
        allow_docker: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Prepaid credit: balances, deposits, payouts and the ledger.
    #[command(subcommand)]
    Credit(CreditCmd),
    /// Show the price list and settlement policy, or change it.
    Prices {
        #[command(subcommand)]
        cmd: Option<PricesCmd>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show a job's signed usage receipts, or export them to a file.
    Receipts {
        id: String,
        /// Write the receipts as JSON to this file.
        #[arg(long)]
        export: Option<PathBuf>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Check the signatures of exported receipts (works offline).
    VerifyReceipts {
        file: PathBuf,
        /// Also require every receipt to come from this control plane.
        #[arg(long)]
        cluster: Option<String>,
    },
    /// Submit a job described by a job.toml file.
    Run {
        job_file: PathBuf,
        /// Account that pays for runs on other people's machines
        /// (default: owner).
        #[arg(long)]
        account: Option<String>,
        /// Return right after submitting instead of waiting for the result.
        #[arg(long)]
        detach: bool,
        /// Print each task's stdout when the job finishes.
        #[arg(long, short)]
        output: bool,
        /// Where to save files tasks wrote to /output
        /// (default: ./pumasi-out/<job id>).
        #[arg(long)]
        out: Option<PathBuf>,
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
        /// Download files tasks wrote to /output into this directory.
        #[arg(long)]
        out: Option<PathBuf>,
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
    /// Run the agent as a service that starts at boot (systemd, launchd,
    /// Windows service).
    #[command(subcommand)]
    Service(service::ServiceCmd),
    /// Roll new agent binaries out to the cluster's nodes.
    #[command(subcommand)]
    Upgrade(upgrade::UpgradeCmd),
    /// MicroVM support: install the guest kernel, check the setup, or run
    /// an image locally in a microVM.
    #[command(subcommand)]
    Vm(VmCmd),
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
        /// Directory whose contents appear read-only at /input.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Where to save what the module writes to /output.
        #[arg(long, default_value = "pumasi-out/exec")]
        out: PathBuf,
        /// Deterministic mode, as cluster jobs use by default: virtual clock
        /// and random numbers seeded from this text.
        #[arg(long)]
        seed: Option<String>,
    },
}

#[derive(Subcommand)]
enum VmCmd {
    /// Download the guest kernel (and Firecracker on Linux, QEMU on Windows)
    /// and install the guest init, so `pumasi agent ... --allow-microvm`
    /// works. On Windows, run it as administrator.
    Setup {
        /// Guest init to install, a static Linux build of `pumasi-guest`
        /// (default: keep the installed one, or use the one next to this
        /// binary).
        #[arg(long)]
        guest: Option<PathBuf>,
        /// Download again even if the files exist.
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        vm: VmArgs,
    },
    /// Show what is installed and which VM monitor would be used.
    Status {
        #[command(flatten)]
        vm: VmArgs,
    },
    /// Run an image in a microVM on this machine, the way an agent would.
    Run {
        image: String,
        /// Command and arguments (default: the image's own command).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
        /// Environment variables, KEY=VALUE.
        #[arg(long = "env", short = 'e')]
        env: Vec<String>,
        #[arg(long, default_value = "300s")]
        timeout: String,
        #[arg(long, default_value_t = 256)]
        memory_mb: u64,
        /// CPU cores (rounded up to whole vCPUs).
        #[arg(long)]
        cpus: Option<f64>,
        /// Directory whose contents appear at /input.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Where to save what the task writes to /output.
        #[arg(long, default_value = "pumasi-out/vm")]
        out: PathBuf,
        #[command(flatten)]
        vm: VmArgs,
    },
}

#[derive(Args, Clone, Default)]
struct VmArgs {
    /// Directory with the guest kernel and init, and the image cache
    /// (default: <home>/vm).
    #[arg(long, env = "PUMASI_VM_DIR")]
    vm_dir: Option<PathBuf>,
    /// VM monitor: firecracker or qemu (default: Firecracker when installed
    /// and KVM is usable, else QEMU).
    #[arg(long)]
    vm_backend: Option<Backend>,
}

impl VmArgs {
    fn config(&self, home: &Path) -> MicrovmConfig {
        MicrovmConfig {
            dir: self.vm_dir.clone().unwrap_or_else(|| home.join("vm")),
            backend: self.vm_backend,
        }
    }
}

#[derive(Subcommand)]
enum CreditCmd {
    /// Show every account's balance.
    List {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Add prepaid credit to an account (bookkeeping only; no money moves).
    Add {
        account: String,
        /// Credits, e.g. 50 or 12.5.
        amount: String,
        #[arg(long, default_value = "")]
        memo: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Record a payout from an account's available balance.
    Payout {
        account: String,
        amount: String,
        #[arg(long, default_value = "")]
        memo: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show ledger entries, newest first.
    Ledger {
        /// Only this account.
        #[arg(long)]
        account: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: u32,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Subcommand)]
enum PricesCmd {
    /// Change prices or policy; options left out keep their value.
    Set {
        /// Credits per billion fuel units of a WASM task.
        #[arg(long)]
        wasm_per_gfuel: Option<String>,
        /// Credits per core-hour of a Docker task.
        #[arg(long)]
        docker_cpu_hour: Option<String>,
        /// Credits per vCPU-hour of a microVM task.
        #[arg(long)]
        microvm_cpu_hour: Option<String>,
        /// Credits per GiB-hour of reserved memory.
        #[arg(long)]
        memory_gib_hour: Option<String>,
        /// How long provider earnings stay held, e.g. 1h, 1d.
        #[arg(long)]
        hold: Option<String>,
        /// Share of provider-run WASM tasks to check, e.g. 0.1 or 10%.
        #[arg(long)]
        verify_rate: Option<String>,
        /// Failed checks after which a provider machine gets no tasks.
        #[arg(long)]
        max_strikes: Option<u32>,
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
    /// Also run docker jobs on this machine. Only for machines you own:
    /// containers share this machine's kernel.
    #[arg(long)]
    allow_docker: bool,
    /// Docker-compatible CLI to use for docker jobs.
    #[arg(long, default_value = "docker")]
    docker_bin: String,
    /// Also run image jobs in microVMs. Each task gets its own VM and
    /// kernel, so this is safe on machines you share with a cluster.
    /// Needs `pumasi vm setup` first.
    #[arg(long)]
    allow_microvm: bool,
    #[command(flatten)]
    vm: VmArgs,
    /// Accept agent upgrades the cluster rolls out (`pumasi upgrade
    /// rollout`). Your own machines accept them by default; a provider
    /// machine needs this flag.
    #[arg(long, conflicts_with = "no_auto_upgrade")]
    auto_upgrade: bool,
    /// Never let the cluster replace this agent's binary.
    #[arg(long)]
    no_auto_upgrade: bool,
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

fn main() {
    pumasi_proto::build_info();
    let cli = Cli::parse();
    let long_running = matches!(
        cli.cmd,
        Cmd::Cluster(_) | Cmd::Agent(_) | Cmd::Service(service::ServiceCmd::Run { .. })
    );
    let default_filter = if long_running {
        "pumasi=info,warn"
    } else {
        "warn"
    };
    let filter = tracing_subscriber::EnvFilter::try_from_env("PUMASI_LOG")
        .unwrap_or_else(|_| default_filter.into());

    // Under the Windows service manager there is no console: log to a file
    // and hand the main thread to the service dispatcher.
    #[cfg(windows)]
    if let Cmd::Service(service::ServiceCmd::Run { agent_args }) = &cli.cmd {
        let home = match home_dir(&cli.home) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("error: {e:#}");
                std::process::exit(1);
            }
        };
        let logs = home.join("logs");
        let _ = std::fs::create_dir_all(&logs);
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(logs.join("agent.log"))
        {
            Ok(file) => tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(file))
                .init(),
            Err(_) => tracing_subscriber::fmt().with_env_filter(filter).init(),
        }
        std::process::exit(service::windows::run(home, agent_args.clone()));
    }

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let runtime = tokio::runtime::Runtime::new().expect("start the async runtime");
    let code = runtime.block_on(run(cli));
    match code {
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
        Cmd::Agent(cmd) => agent(&home, cmd, ctrl_c(), false).await,
        Cmd::Service(cmd) => service_cmd(&home, cmd),
        Cmd::Upgrade(cmd) => upgrade::run(&home, cmd).await,
        Cmd::Token { ttl, uses, admin } => {
            let ttl_secs = parse_duration_ms(&ttl)? / 1000;
            let client = connect_admin(&home, &admin).await?;
            let resp = client
                .call(AdminOp::CreateJoinToken {
                    ttl_secs,
                    uses,
                    invite: None,
                })
                .await?;
            client.close().await;
            let AdminResponse::JoinToken { ticket, .. } = resp else {
                bail!("unexpected response")
            };
            println!("{ticket}");
            Ok(0)
        }
        Cmd::Invite {
            account,
            ttl,
            uses,
            allow_docker,
            admin,
        } => {
            validate_account(&account)?;
            let ttl_secs = parse_duration_ms(&ttl)? / 1000;
            let client = connect_admin(&home, &admin).await?;
            let resp = client
                .call(AdminOp::CreateJoinToken {
                    ttl_secs,
                    uses: Some(uses),
                    invite: Some(Invite {
                        account: account.clone(),
                        allow_docker,
                    }),
                })
                .await?;
            client.close().await;
            let AdminResponse::JoinToken { ticket, .. } = resp else {
                bail!("unexpected response")
            };
            eprintln!(
                "invite for {account} ({} machine{}, valid {ttl}, {}):",
                uses,
                if uses == 1 { "" } else { "s" },
                if allow_docker {
                    "wasm, microvm and docker"
                } else {
                    "wasm and microvm"
                }
            );
            eprintln!("send this command to the provider; it runs on their machine:");
            eprintln!();
            println!("pumasi agent join {ticket}");
            Ok(0)
        }
        Cmd::Credit(cmd) => credit(&home, cmd).await,
        Cmd::Prices { cmd, admin } => prices(&home, cmd, &admin).await,
        Cmd::Receipts { id, export, admin } => {
            let id: JobId = id.parse()?;
            let client = connect_admin(&home, &admin).await?;
            let AdminResponse::Receipts(receipts) = client.call(AdminOp::Receipts { id }).await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            match export {
                Some(path) => {
                    let rows: Vec<ReceiptRow> = receipts.iter().map(ReceiptRow::from).collect();
                    std::fs::write(&path, serde_json::to_vec_pretty(&rows)?)
                        .with_context(|| format!("write {}", path.display()))?;
                    eprintln!("wrote {} receipts to {}", rows.len(), path.display());
                }
                None => print_receipts(&receipts),
            }
            Ok(0)
        }
        Cmd::VerifyReceipts { file, cluster } => verify_receipts(&file, cluster.as_deref()),
        Cmd::Run {
            job_file,
            account,
            detach,
            output,
            out,
            admin,
        } => {
            let mut sub = JobFile::load(&job_file)?;
            if let Some(a) = &account {
                validate_account(a)?;
            }
            sub.account = account;
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
            print_job(&view, output);
            let dir = out.unwrap_or_else(|| PathBuf::from("pumasi-out").join(id.to_string()));
            download_outputs(&client, &view, &dir, false).await?;
            client.close().await;
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
        Cmd::Job {
            id,
            output,
            out,
            admin,
        } => {
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
            print_job(&view, output);
            if let Some(dir) = out {
                download_outputs(&client, &view, &dir, true).await?;
            }
            client.close().await;
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
        Cmd::Vm(cmd) => vm(&home, cmd).await,
        Cmd::Exec {
            module,
            args,
            fuel,
            timeout,
            memory_mb,
            input,
            out,
            seed,
        } => {
            exec_local(
                &module,
                args,
                fuel,
                &timeout,
                memory_mb,
                input.as_deref(),
                &out,
                seed.map(|s| pumasi_proto::module_hash(s.as_bytes())),
            )
            .await
        }
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

async fn ctrl_c() {
    let _ = tokio::signal::ctrl_c().await;
}

/// A ticket `pumasi service install --ticket` left for the first start.
const PENDING_TICKET_FILE: &str = "join.ticket";

/// Runs the agent until `shutdown` resolves. After an upgrade or a
/// rollback it restarts the process: in place on Unix; on Windows a new
/// process, or under the service manager (`in_service`) it returns
/// [`service::RESTART_EXIT_CODE`] so the service is restarted.
async fn agent(
    home: &Path,
    cmd: AgentCmd,
    shutdown: impl std::future::Future<Output = ()>,
    in_service: bool,
) -> Result<i32> {
    let data_dir = home.join("agent");
    let membership = data_dir.join("membership.json");
    let pending_ticket = data_dir.join(PENDING_TICKET_FILE);
    let (ticket, opts) = match cmd {
        AgentCmd::Join { ticket, opts } => (Some(ticket.parse::<JoinTicket>()?), opts),
        AgentCmd::Start { opts } if !membership.exists() && pending_ticket.exists() => {
            let text = std::fs::read_to_string(&pending_ticket)?;
            let ticket = text
                .parse::<JoinTicket>()
                .with_context(|| format!("bad ticket in {}", pending_ticket.display()))?;
            (Some(ticket), opts)
        }
        AgentCmd::Start { opts } => {
            if membership.exists() {
                let _ = std::fs::remove_file(&pending_ticket);
            }
            (None, opts)
        }
    };
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
        docker: opts.allow_docker.then(|| opts.docker_bin.clone()),
        microvm: opts.allow_microvm.then(|| opts.vm.config(home)),
        upgrade: own_exe().map(|exe| UpgradeConfig {
            exe,
            auto: if opts.auto_upgrade {
                Some(true)
            } else if opts.no_auto_upgrade {
                Some(false)
            } else {
                None
            },
            smoke_test: true,
        }),
        corrupt_results: false,
    };
    let exe = config.upgrade.as_ref().map(|u| u.exe.clone());
    let agent = Agent::new(config).await?;
    eprintln!(
        "agent {} ({}, {} slots, {}) connecting...",
        agent.info().name,
        agent.id().fmt_short(),
        agent.info().slots,
        executor_list(&agent.info().executors)
    );
    let serve = async move {
        match ticket {
            Some(t) => agent.join(t).await,
            None => agent.resume().await,
        }
    };
    let exit = tokio::select! {
        r = serve => r?,
        _ = shutdown => {
            eprintln!("shutting down");
            return Ok(0);
        }
    };
    match &exit {
        AgentExit::Upgraded { version } => eprintln!("upgraded to {version}; restarting"),
        AgentExit::RolledBack { reason } => {
            eprintln!("rolled back to the previous binary ({reason}); restarting")
        }
    }
    if in_service {
        return Ok(service::RESTART_EXIT_CODE);
    }
    let exe = exe.context("restart: unknown binary path")?;
    restart_self(&exe)
}

/// The running binary, if the agent can replace it.
fn own_exe() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    Some(exe.canonicalize().unwrap_or(exe))
}

/// Runs the binary at `exe` again with this process's arguments.
fn restart_self(exe: &Path) -> Result<i32> {
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let err = std::process::Command::new(exe).args(&args).exec();
        Err(anyhow::Error::from(err).context(format!("restart {}", exe.display())))
    }
    #[cfg(not(unix))]
    {
        std::process::Command::new(exe)
            .args(&args)
            .spawn()
            .with_context(|| format!("restart {}", exe.display()))?;
        Ok(0)
    }
}

/// What the Windows service runs: `agent start` with the given options.
#[cfg_attr(not(windows), allow(dead_code))]
async fn run_service_agent(
    home: PathBuf,
    agent_args: Vec<String>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<i32> {
    let cmd = parse_agent_args(&agent_args)?;
    agent(&home, cmd, shutdown, true).await
}

/// Parses options meant for `pumasi agent start`.
fn parse_agent_args(agent_args: &[String]) -> Result<AgentCmd> {
    let argv = ["pumasi", "agent", "start"]
        .into_iter()
        .map(String::from)
        .chain(agent_args.iter().cloned());
    match Cli::try_parse_from(argv) {
        Ok(Cli {
            cmd: Cmd::Agent(cmd),
            ..
        }) => Ok(cmd),
        Ok(_) => bail!("not agent options"),
        Err(e) => bail!("invalid agent options {agent_args:?}: {e}"),
    }
}

fn service_cmd(home: &Path, cmd: service::ServiceCmd) -> Result<i32> {
    use service::ServiceCmd;
    match cmd {
        ServiceCmd::Install {
            ticket,
            scope,
            dry_run,
            agent_args,
        } => {
            parse_agent_args(&agent_args)?;
            let home = service::absolute(home)?;
            let data_dir = home.join("agent");
            if let Some(t) = &ticket {
                t.parse::<JoinTicket>()?;
            }
            if !dry_run {
                match &ticket {
                    Some(t) => pumasi_proto::net::write_private(
                        &data_dir.join(PENDING_TICKET_FILE),
                        t.trim().as_bytes(),
                    )?,
                    None if !data_dir.join("membership.json").exists() => bail!(
                        "this machine has not joined a cluster yet (no {}); pass --ticket",
                        data_dir.join("membership.json").display()
                    ),
                    None => {}
                }
            }
            let exe = own_exe().context("cannot find this binary's path")?;
            service::install(
                &service::Spec {
                    exe,
                    home,
                    agent_args,
                },
                scope,
                dry_run,
            )
        }
        ServiceCmd::Uninstall { scope } => service::uninstall(scope),
        ServiceCmd::Status { scope } => service::status(scope),
        ServiceCmd::Run { agent_args } => {
            let _ = agent_args;
            bail!(
                "`service run` is what the Windows service manager runs; use `pumasi agent start`"
            )
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
        "{:<12} {:<16} {:<9} {:<18} {:<14} {:<12} {:<12} {:>5} {:>8} {:>10} {:>8}",
        "NODE",
        "NAME",
        "STATE",
        "ROLE",
        "PLATFORM",
        "VERSION",
        "EXECUTORS",
        "SLOTS",
        "RUNNING",
        "COMPLETED",
        "CHECKS"
    );
    for n in &s.nodes {
        let role = match n.role {
            Role::Owner => "owner".to_string(),
            Role::Provider => format!("provider:{}", n.account),
        };
        let state = if n.suspended {
            "suspended"
        } else if n.online {
            "online"
        } else {
            "offline"
        };
        let checks = if n.role == Role::Owner {
            "-".to_string()
        } else {
            format!("{}/{}", n.checks_passed, n.strikes)
        };
        println!(
            "{:<12} {:<16} {:<9} {:<18} {:<14} {:<12} {:<12} {:>5} {:>8} {:>10} {:>8}",
            short(&n.id),
            truncate(&n.info.name, 16),
            state,
            truncate(&role, 18),
            format!("{}/{}", n.info.os, n.info.arch),
            truncate(&n.info.agent_version, 12),
            executor_list(&n.executors),
            n.info.slots,
            n.running,
            n.completed,
            checks
        );
    }
    if s.nodes.iter().any(|n| n.role == Role::Provider) {
        println!(
            "(CHECKS: passed/failed result checks; providers run WASM and microVM jobs, Docker only if invited with --allow-docker)"
        );
    }
    if s.nodes.is_empty() {
        println!("(no nodes yet: create a ticket with `pumasi token`)");
    }
    println!();
    println!(
        "{:<17} {:<20} {:<10} {:>7} {:>7} {:>7} {:<10} {:>10}",
        "JOB", "NAME", "STATE", "TASKS", "DONE", "FAILED", "ACCOUNT", "COST"
    );
    for j in s.jobs.iter().take(20) {
        println!(
            "{:<17} {:<20} {:<10} {:>7} {:>7} {:>7} {:<10} {:>10}",
            j.id.to_string(),
            truncate(&j.name, 20),
            j.state.to_string(),
            j.tasks,
            j.succeeded,
            j.failed,
            truncate(&j.account, 10),
            format_credits(j.cost)
        );
    }
    if s.jobs.is_empty() {
        println!("(no jobs yet: submit one with `pumasi run job.toml`)");
    }
}

fn print_job(v: &JobView, output: bool) {
    let s = &v.summary;
    println!("job {} \"{}\": {}", s.id, s.name, s.state);
    match &v.image {
        Some(image) => println!("  executor {} ({image})", v.executor),
        None => println!("  executor {}", v.executor),
    }
    println!(
        "  account {}  cost {} credits{}",
        s.account,
        format_credits(s.cost),
        if v.deterministic {
            "  deterministic"
        } else {
            ""
        }
    );
    let files: usize = v.tasks.iter().map(|t| t.output_files.len()).sum();
    print!(
        "  tasks {}  succeeded {}  failed {}",
        s.tasks, s.succeeded, s.failed
    );
    if v.executor == Executor::Wasm {
        print!("  fuel {}", v.total_fuel);
    }
    if files > 0 {
        print!("  output files {files}");
    }
    println!();
    let mut stdout = std::io::stdout().lock();
    for t in &v.tasks {
        let state = match &t.state {
            TaskState::Pending => "pending".to_string(),
            TaskState::Running { .. } => "running".to_string(),
            TaskState::Succeeded => "ok".to_string(),
            TaskState::Failed(o) => format!("failed ({o})"),
            TaskState::Cancelled => "cancelled".to_string(),
            TaskState::Verifying => "checking".to_string(),
        };
        let check = match &t.verification {
            Verification::NotChecked => String::new(),
            Verification::Pending => " check=pending".into(),
            Verification::Passed => " check=passed".into(),
            Verification::Mismatch { bad } => format!(
                " check=MISMATCH (rejected {})",
                bad.iter().map(|b| short(b)).collect::<Vec<_>>().join(",")
            ),
            Verification::Inconclusive => " check=inconclusive".into(),
            Verification::Skipped(why) => format!(" check=skipped ({why})"),
        };
        let cost = if t.cost > 0 {
            format!(" cost={}", format_credits(t.cost))
        } else {
            String::new()
        };
        let show_line = output || !matches!(t.state, TaskState::Succeeded) || v.tasks.len() <= 20;
        if show_line {
            let _ = writeln!(
                stdout,
                "  [{}] {} node={} attempts={} {}ms{}{}",
                t.index,
                state,
                t.node.as_deref().map(short).unwrap_or("-"),
                t.attempts,
                t.wall_ms,
                cost,
                check
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

fn executor_list(executors: &[Executor]) -> String {
    executors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Fetches every task's `/output` files into `dir/task-<index>/`.
/// Returns the number of files written.
async fn download_outputs(
    client: &AdminClient,
    view: &JobView,
    dir: &Path,
    always_report: bool,
) -> Result<usize> {
    let mut count = 0;
    for t in view.tasks.iter().filter(|t| !t.output_files.is_empty()) {
        let resp = client
            .call(AdminOp::TaskFiles {
                id: view.summary.id,
                index: t.index,
            })
            .await?;
        let AdminResponse::Files(files) = resp else {
            bail!("unexpected response");
        };
        let target = dir.join(format!("task-{}", t.index));
        std::fs::create_dir_all(&target).with_context(|| format!("create {}", target.display()))?;
        write_files(&target, &files)?;
        count += files.len();
    }
    if count > 0 || always_report {
        eprintln!("saved {count} output files to {}", dir.display());
    }
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
async fn exec_local(
    module: &Path,
    args: Vec<String>,
    fuel: Option<u64>,
    timeout: &str,
    memory_mb: u64,
    input_dir: Option<&Path>,
    out_dir: &Path,
    seed: Option<[u8; 32]>,
) -> Result<i32> {
    let bytes = std::fs::read(module).with_context(|| format!("read {}", module.display()))?;
    let hash = pumasi_proto::module_hash(&bytes);
    let limits = Limits {
        memory_bytes: memory_mb * 1024 * 1024,
        fuel,
        timeout_ms: parse_duration_ms(timeout)?,
        cpu_millis: None,
    };
    let shared = match input_dir {
        Some(dir) => {
            let (files, truncated) = read_files(dir, MAX_INPUT_FILES_BYTES)?;
            if truncated {
                bail!("{} is larger than the input limit", dir.display());
            }
            files
        }
        None => Vec::new(),
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
            &shared,
            &limits,
            seed,
            Arc::new(AtomicBool::new(false)),
        ))
    })
    .await??;
    std::io::stdout().write_all(&result.stdout)?;
    std::io::stderr().write_all(&result.stderr)?;
    if !result.files.is_empty() {
        std::fs::create_dir_all(out_dir)?;
        write_files(out_dir, &result.files)?;
        eprintln!(
            "[pumasi] saved {} output files to {}",
            result.files.len(),
            out_dir.display()
        );
    }
    eprintln!(
        "[pumasi] {}  fuel={}  {}ms",
        result.outcome, result.fuel_used, result.wall_ms
    );
    Ok(match result.outcome {
        Outcome::Exited(c) => c,
        _ => 125,
    })
}

async fn vm(home: &Path, cmd: VmCmd) -> Result<i32> {
    match cmd {
        VmCmd::Setup { guest, force, vm } => {
            let config = vm.config(home);
            let report =
                pumasi_agent::microvm::setup::setup(&config.dir, guest.as_deref(), force).await?;
            for f in &report.installed {
                eprintln!("installed {f}");
            }
            for f in &report.kept {
                eprintln!("kept      {f}");
            }
            vm_status(&config)
        }
        VmCmd::Status { vm } => vm_status(&vm.config(home)),
        VmCmd::Run {
            image,
            command,
            env,
            timeout,
            memory_mb,
            cpus,
            input,
            out,
            vm,
        } => {
            let exec = MicrovmExecutor::detect(&vm.config(home), "local").await?;
            let mut task_env = std::collections::BTreeMap::new();
            for kv in env {
                let (k, v) = kv
                    .split_once('=')
                    .with_context(|| format!("--env wants KEY=VALUE, got {kv:?}"))?;
                task_env.insert(k.to_string(), v.to_string());
            }
            let files = match &input {
                Some(dir) => {
                    let (files, truncated) = read_files(dir, MAX_INPUT_FILES_BYTES)?;
                    if truncated {
                        bail!("{} is larger than the input limit", dir.display());
                    }
                    files
                }
                None => Vec::new(),
            };
            let mut stdin = Vec::new();
            if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
                std::io::Read::read_to_end(&mut std::io::stdin(), &mut stdin)?;
            }
            let spec = pumasi_proto::job::DockerSpec {
                image,
                command,
                network: false,
                gpus: None,
            };
            let limits = Limits {
                memory_bytes: memory_mb << 20,
                fuel: None,
                timeout_ms: parse_duration_ms(&timeout)?,
                cpu_millis: cpus.map(|c| (c * 1000.0).round() as u32),
            };
            let task = pumasi_proto::msg::TaskRef {
                job: JobId(0),
                index: 0,
                attempt: 1,
                check: false,
            };
            let input = TaskInput {
                args: vec![],
                env: task_env,
                stdin,
                files: vec![],
            };
            let cancel = Arc::new(AtomicBool::new(false));
            let flag = cancel.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            });
            let r = exec.run(task, &spec, &input, &files, &limits, cancel).await;
            std::io::stdout().write_all(&r.stdout)?;
            std::io::stderr().write_all(&r.stderr)?;
            if !r.files.is_empty() {
                std::fs::create_dir_all(&out)?;
                write_files(&out, &r.files)?;
                eprintln!(
                    "[pumasi] saved {} output files to {}",
                    r.files.len(),
                    out.display()
                );
            }
            eprintln!(
                "[pumasi] {}  {}ms ({})",
                r.outcome,
                r.wall_ms,
                exec.backend()
            );
            Ok(match r.outcome {
                Outcome::Exited(c) => c,
                _ => 125,
            })
        }
    }
}

fn vm_status(config: &MicrovmConfig) -> Result<i32> {
    let a = Assets::find(&config.dir);
    let show = |name: &str, p: &Option<PathBuf>, hint: &str| match p {
        Some(p) => println!("  {name:<12} {}", p.display()),
        None => println!("  {name:<12} missing ({hint})"),
    };
    println!("microVM setup in {}", config.dir.display());
    show("kernel", &a.kernel, "run `pumasi vm setup`");
    match &a.guest {
        Some(g) => println!("  {:<12} {g}", "guest init"),
        None => println!(
            "  {:<12} missing (build pumasi-guest for linux-musl, then `pumasi vm setup --guest <path>`)",
            "guest init"
        ),
    }
    if cfg!(target_os = "linux") {
        show("firecracker", &a.firecracker, "run `pumasi vm setup`");
    }
    let qemu_hint = if cfg!(windows) {
        "run `pumasi vm setup` as administrator"
    } else {
        "optional; install QEMU"
    };
    show("qemu", &a.qemu, qemu_hint);
    if cfg!(target_os = "linux") {
        println!(
            "  {:<12} {}",
            "kvm",
            if a.kvm {
                "usable"
            } else {
                "not usable (enable virtualization; add this user to the `kvm` group)"
            }
        );
    }
    if let Some(whpx) = &a.whpx {
        match whpx {
            Ok(()) if cfg!(target_arch = "x86_64") => println!("  {:<12} usable", "whpx"),
            Ok(()) => println!("  {:<12} on, but QEMU uses it only on x86_64", "whpx"),
            Err(why) => println!("  {:<12} not usable: {why}", "whpx"),
        }
    }
    let ready = a.kernel.is_some() && a.guest.is_some();
    match a.pick(config.backend) {
        Ok(vmm) if ready => {
            println!(
                "ready: microVMs boot with {}{}",
                vmm.backend,
                if vmm.accelerated {
                    ""
                } else {
                    " (software emulation: slow)"
                }
            );
            Ok(0)
        }
        Ok(_) => {
            println!("not ready: see the missing items above");
            Ok(1)
        }
        Err(e) => {
            println!("not ready: {e:#}");
            Ok(1)
        }
    }
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

// ---------------------------------------------------------------------------
// Credit, prices and receipts
// ---------------------------------------------------------------------------

async fn credit(home: &Path, cmd: CreditCmd) -> Result<i32> {
    match cmd {
        CreditCmd::List { admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Accounts(accounts) = client.call(AdminOp::Accounts).await? else {
                bail!("unexpected response");
            };
            client.close().await;
            print_accounts(&accounts);
        }
        CreditCmd::Add {
            account,
            amount,
            memo,
            admin,
        } => {
            validate_account(&account)?;
            let amount = parse_credits(&amount)?;
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Accounts(a) = client
                .call(AdminOp::Deposit {
                    account,
                    amount,
                    memo,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            print_accounts(&a);
        }
        CreditCmd::Payout {
            account,
            amount,
            memo,
            admin,
        } => {
            let amount = parse_credits(&amount)?;
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Accounts(a) = client
                .call(AdminOp::Payout {
                    account,
                    amount,
                    memo,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            print_accounts(&a);
        }
        CreditCmd::Ledger {
            account,
            limit,
            admin,
        } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Ledger(entries) =
                client.call(AdminOp::Ledger { account, limit }).await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            print_ledger(&entries);
        }
    }
    Ok(0)
}

fn print_accounts(accounts: &[AccountView]) {
    println!(
        "{:<16} {:>14} {:>14} {:>14} {:>14}",
        "ACCOUNT", "AVAILABLE", "HELD", "SPENT", "EARNED"
    );
    for a in accounts {
        println!(
            "{:<16} {:>14} {:>14} {:>14} {:>14}",
            truncate(&a.name, 16),
            format_credits(a.balance),
            format_credits(a.held),
            format_credits(a.spent),
            format_credits(a.earned)
        );
    }
}

fn print_ledger(entries: &[LedgerEntry]) {
    println!(
        "{:>6} {:<20} {:<12} {:<9} {:>14}  DETAIL",
        "SEQ", "TIME (UTC)", "ACCOUNT", "KIND", "AMOUNT"
    );
    for e in entries {
        let amount = match e.kind {
            // These move held money, not the spendable balance.
            LedgerKind::Hold | LedgerKind::Forfeit => format!("({})", format_credits(e.amount)),
            _ => format_credits(e.amount),
        };
        let mut detail = e.task.map(|t| t.to_string()).unwrap_or_default();
        if !e.memo.is_empty() {
            if !detail.is_empty() {
                detail.push_str("  ");
            }
            detail.push_str(&e.memo);
        }
        println!(
            "{:>6} {:<20} {:<12} {:<9} {:>14}  {}",
            e.seq,
            utc(e.unix),
            truncate(&e.account, 12),
            e.kind.to_string(),
            amount,
            detail
        );
    }
    if entries.is_empty() {
        println!("(no entries)");
    } else {
        println!("(amounts in parentheses move held earnings, not the available balance)");
    }
}

async fn prices(home: &Path, cmd: Option<PricesCmd>, admin: &AdminArgs) -> Result<i32> {
    let client = connect_admin(home, admin).await?;
    let AdminResponse::Policy(mut p) = client.call(AdminOp::GetPolicy).await? else {
        bail!("unexpected response");
    };
    if let Some(PricesCmd::Set {
        wasm_per_gfuel,
        docker_cpu_hour,
        microvm_cpu_hour,
        memory_gib_hour,
        hold,
        verify_rate,
        max_strikes,
    }) = cmd
    {
        if let Some(v) = wasm_per_gfuel {
            p.prices.wasm_per_gfuel = parse_credits(&v)?;
        }
        if let Some(v) = docker_cpu_hour {
            p.prices.docker_cpu_hour = parse_credits(&v)?;
        }
        if let Some(v) = microvm_cpu_hour {
            p.prices.microvm_cpu_hour = parse_credits(&v)?;
        }
        if let Some(v) = memory_gib_hour {
            p.prices.memory_gib_hour = parse_credits(&v)?;
        }
        if let Some(v) = hold {
            p.hold_secs = parse_duration_ms(&v)? / 1000;
        }
        if let Some(v) = verify_rate {
            p.verify_permille = parse_rate_permille(&v)?;
        }
        if let Some(v) = max_strikes {
            p.max_strikes = v;
        }
        let AdminResponse::Policy(saved) = client.call(AdminOp::SetPolicy(p)).await? else {
            bail!("unexpected response");
        };
        p = saved;
        eprintln!("saved");
    }
    client.close().await;
    print_policy(&p);
    Ok(0)
}

fn print_policy(p: &Policy) {
    println!("price list (credits)");
    println!(
        "  wasm-per-gfuel   {:>12}  per billion fuel units",
        format_credits(p.prices.wasm_per_gfuel)
    );
    println!(
        "  docker-cpu-hour  {:>12}  per core-hour of wall time",
        format_credits(p.prices.docker_cpu_hour)
    );
    println!(
        "  microvm-cpu-hour {:>12}  per vCPU-hour of wall time",
        format_credits(p.prices.microvm_cpu_hour)
    );
    println!(
        "  memory-gib-hour  {:>12}  per GiB-hour of reserved memory",
        format_credits(p.prices.memory_gib_hour)
    );
    println!("settlement");
    println!(
        "  hold             {:>12}  before provider earnings become available",
        human_secs(p.hold_secs)
    );
    println!(
        "  verify-rate      {:>11}%  of provider-run WASM tasks re-run elsewhere (new or flagged providers: more)",
        p.verify_permille as f64 / 10.0
    );
    println!(
        "  max-strikes      {:>12}  failed checks before a provider machine is suspended",
        p.max_strikes
    );
    println!("runs on machines of the job's own account are free.");
}

fn human_secs(s: u64) -> String {
    if s > 0 && s.is_multiple_of(86_400) {
        format!("{}d", s / 86_400)
    } else if s > 0 && s.is_multiple_of(3600) {
        format!("{}h", s / 3600)
    } else if s > 0 && s.is_multiple_of(60) {
        format!("{}m", s / 60)
    } else {
        format!("{s}s")
    }
}

/// One exported receipt: readable fields plus the exact signed encoding.
#[derive(serde::Serialize, serde::Deserialize)]
struct ReceiptRow {
    id: String,
    job: String,
    task: u32,
    attempt: u32,
    check: bool,
    node: String,
    cluster: String,
    executor: String,
    outcome: String,
    fuel_used: u64,
    wall_ms: u64,
    billed_wall_ms: u64,
    memory_limit_bytes: u64,
    peak_memory_bytes: Option<u64>,
    consumer: String,
    provider: String,
    amount: String,
    recorded_unix: u64,
    receipt: String,
}

impl From<&Receipt> for ReceiptRow {
    fn from(r: &Receipt) -> Self {
        let u = &r.usage;
        let b = &r.billing;
        Self {
            id: data_hex(&r.id()),
            job: u.task.job.to_string(),
            task: u.task.index,
            attempt: u.task.attempt,
            check: u.task.check,
            node: r.node_id(),
            cluster: r.cluster_id(),
            executor: u.executor.to_string(),
            outcome: u.outcome.to_string(),
            fuel_used: u.fuel_used,
            wall_ms: u.wall_ms,
            billed_wall_ms: b.billed_wall_ms,
            memory_limit_bytes: u.memory_limit_bytes,
            peak_memory_bytes: u.peak_memory_bytes,
            consumer: b.consumer.clone(),
            provider: b.provider.clone(),
            amount: format_credits(b.amount),
            recorded_unix: b.recorded_unix,
            receipt: r.to_blob(),
        }
    }
}

fn data_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn print_receipts(receipts: &[Receipt]) {
    println!(
        "{:<13} {:<10} {:<12} {:<12} {:>14} {:>9} {:>10} {:<10} {:>12}",
        "RECEIPT", "RUN", "NODE", "OUTCOME", "FUEL", "WALL ms", "PEAK MiB", "PROVIDER", "AMOUNT"
    );
    for r in receipts {
        let u = &r.usage;
        let run = format!(
            "{}#{}{}",
            u.task.index,
            u.task.attempt,
            if u.task.check { "c" } else { "" }
        );
        println!(
            "{:<13} {:<10} {:<12} {:<12} {:>14} {:>9} {:>10} {:<10} {:>12}",
            r.short_id(),
            run,
            short(&r.node_id()),
            truncate(&u.outcome.to_string(), 12),
            u.fuel_used,
            r.billing.billed_wall_ms,
            u.peak_memory_bytes
                .map(|b| format!("{:.1}", b as f64 / (1 << 20) as f64))
                .unwrap_or_else(|| "-".into()),
            truncate(&r.billing.provider, 10),
            format_credits(r.billing.amount)
        );
    }
    if receipts.is_empty() {
        println!("(no receipts yet)");
    } else {
        println!("(RUN: task#attempt, c = check run; export with --export to verify offline)");
    }
}

fn verify_receipts(file: &Path, cluster: Option<&str>) -> Result<i32> {
    let text = std::fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
    let rows: Vec<ReceiptRow> = serde_json::from_str(&text).context("not a receipts export")?;
    let mut bad = 0;
    for row in &rows {
        let verdict = (|| -> Result<()> {
            let r = Receipt::from_blob(&row.receipt)?;
            r.verify()?;
            // The readable fields must say what the signed encoding says.
            let expect = ReceiptRow::from(&r);
            anyhow::ensure!(
                serde_json::to_value(&expect)? == serde_json::to_value(row)?,
                "readable fields were edited"
            );
            if let Some(c) = cluster {
                anyhow::ensure!(
                    r.cluster_id().starts_with(c.trim()),
                    "signed by another control plane ({})",
                    short(&r.cluster_id())
                );
            }
            Ok(())
        })();
        match verdict {
            Ok(()) => println!(
                "ok   {} {}/{}#{}",
                &row.id[..12.min(row.id.len())],
                row.job,
                row.task,
                row.attempt
            ),
            Err(e) => {
                bad += 1;
                println!(
                    "BAD  {} {}/{}#{}: {e:#}",
                    &row.id[..12.min(row.id.len())],
                    row.job,
                    row.task,
                    row.attempt
                );
            }
        }
    }
    println!(
        "{} receipts, {} valid, {} invalid",
        rows.len(),
        rows.len() - bad,
        bad
    );
    Ok(if bad == 0 { 0 } else { 1 })
}

/// `YYYY-MM-DD HH:MM:SS` in UTC, without a date library.
fn utc(unix: u64) -> String {
    let days = (unix / 86_400) as i64;
    let secs = unix % 86_400;
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_dates() {
        assert_eq!(utc(0), "1970-01-01 00:00:00");
        assert_eq!(utc(946_684_800), "2000-01-01 00:00:00");
        assert_eq!(utc(1_793_793_000), "2026-11-04 11:50:00");
    }
}
