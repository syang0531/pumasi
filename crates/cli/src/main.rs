//! `pumasi`: one binary for the control plane, the node agent and the
//! admin CLI.

mod gpu_cmd;
mod ha_cmd;
mod manage;
mod monitor;
mod service;
mod ui;
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
    Agent, AgentConfig, AgentExit, BenchConfig, MicrovmConfig, MicrovmExecutor, UpgradeConfig,
    WasmExecutor, default_node_name,
    microvm::{Assets, Backend},
};
use pumasi_cluster::{ADMIN_TICKET_FILE, AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{
        LedgerMode, Policy, format_cu, format_rate, parse_cu, parse_rate, parse_rate_permille,
        validate_account,
    },
    job::{
        Executor, JobFile, JobId, Limits, MAX_INPUT_FILES_BYTES, TaskInput, parse_duration_ms,
        read_files, write_files,
    },
    msg::{
        AccountView, AdminOp, AdminResponse, ClusterStatus, Invite, JobView, LedgerEntry,
        LedgerKind, NodeView, Outcome, Role, TaskState, Verification,
    },
    net::NetMode,
    perf::{DiskClass, PerfProfile},
    receipt::{Receipt, ReceiptRow},
    store::{DatasetView, StoragePolicy, StoreRef, StoreTarget, format_size, parse_size},
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
    /// (Docker only with --allow-docker), earns CU for ACCOUNT, and has
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
    /// Compute units (CU): balances, grants, payouts and the ledger.
    #[command(subcommand, alias = "credit")]
    Cu(CuCmd),
    /// The cluster file store: datasets (named, versioned collections of
    /// files) that jobs mount with `store:NAME` in job.toml, and job
    /// outputs.
    #[command(subcommand)]
    Store(StoreCmd),
    /// Cluster secrets kept on the control plane, e.g. private registry
    /// logins (`registry/<host>`).
    #[command(subcommand)]
    Secret(SecretCmd),
    /// Show the CU rates and settlement policy, or change them.
    Prices {
        #[command(subcommand)]
        cmd: Option<PricesCmd>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Benchmark this machine the way the agent does (CPU, memory, the disk
    /// under DIR, SMART) and print the profile.
    Bench {
        /// Directory whose drive to measure (default: the agent's).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// A short run (about a second) instead of the full one.
        #[arg(long)]
        quick: bool,
        /// Print the profile as JSON.
        #[arg(long)]
        json: bool,
        /// Measure only this many CPUs (default: all).
        #[arg(long)]
        cpus: Option<u32>,
    },
    /// This machine's GPUs: list them, or check what jobs could do with
    /// them (Docker, microVM passthrough, the cluster's GPU check).
    #[command(subcommand)]
    Gpu(gpu_cmd::GpuCmd),
    /// Show one node: its benchmark profile, billed CPU score, latency and
    /// checks. With --rebenchmark, ask it to measure itself again.
    Node {
        id: String,
        #[arg(long)]
        rebenchmark: bool,
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
        /// Priority (-1000 to 1000; overrides job.toml). Higher runs first
        /// and may stop lower-priority tasks to get room.
        #[arg(long, allow_hyphen_values = true)]
        priority: Option<i32>,
        /// Start only after this job succeeded (repeat for several).
        #[arg(long)]
        after: Vec<String>,
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
    /// Save an admin ticket (or an access ticket from `pumasi access
    /// create`) so this machine can manage a remote cluster.
    Login { ticket: String },
    /// Stop giving a node new tasks (NODE: name or id prefix). Running
    /// tasks finish.
    Cordon {
        node: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Give a cordoned or drained node tasks again.
    Uncordon {
        node: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Cordon a node and move its running tasks to other nodes (they do
    /// not count against the jobs' attempts), e.g. before maintenance.
    Drain {
        node: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Set (`key=value`) or remove (`key-`) labels on a node, for job
    /// selectors (`[selector]` in job.toml).
    Label {
        node: String,
        #[arg(required = true)]
        labels: Vec<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Add (`key` or `key=value`) or remove (`key-`) taints on a node: only
    /// jobs that tolerate them (`tolerations` in job.toml) run there.
    Taint {
        node: String,
        #[arg(required = true)]
        taints: Vec<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show every account's quota and usage, or change one account's.
    /// Give a value, or `none` to lift that limit.
    Quota {
        account: Option<String>,
        /// Tasks running at once.
        #[arg(long)]
        slots: Option<String>,
        /// GPUs in use at once.
        #[arg(long)]
        gpus: Option<String>,
        /// CU it may spend in any 24 hours on other accounts' machines and
        /// storage.
        #[arg(long)]
        cu_per_day: Option<String>,
        /// Highest job priority (submitter tickets stay at 0 without it).
        #[arg(long, allow_hyphen_values = true)]
        max_priority: Option<String>,
        /// File store space, e.g. 50G.
        #[arg(long)]
        storage: Option<String>,
        /// Start from no limits instead of the current quota (and job
        /// policy).
        #[arg(long)]
        clear: bool,
        #[command(flatten)]
        policy: manage::PolicyArgs,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Access tickets with a role: admin, submitter (one account) or
    /// viewer.
    #[command(subcommand)]
    Access(manage::AccessCmd),
    /// Show which ticket this machine uses and what it may do.
    Whoami {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Jobs submitted on a cron schedule.
    #[command(subcommand)]
    Schedule(manage::ScheduleCmd),
    /// Make the cluster match a cluster.toml: node labels, taints and
    /// cordons, account quotas, schedules, alert rules and webhooks. Applying it twice changes
    /// nothing the second time.
    Apply {
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
        /// Show what would change without changing it.
        #[arg(long)]
        dry_run: bool,
        /// Remove schedules the file does not list.
        #[arg(long)]
        prune: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// List nodes, jobs, accounts, schedules or access tickets.
    Get {
        #[arg(value_enum)]
        kind: manage::GetKind,
        /// Only nodes whose labels match, e.g. `os=linux,disk=nvme|ssd`.
        #[arg(short = 'l', long)]
        selector: Option<String>,
        #[arg(short = 'o', long, value_enum, default_value = "table")]
        output: manage::Output,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show one node, job, account or schedule in detail.
    Describe {
        #[arg(value_enum)]
        kind: manage::DescribeKind,
        name: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Nodes' CPU, memory, disk, network, temperature and GPUs, and the
    /// queue, refreshed until Ctrl-C.
    Top {
        /// How often to refresh.
        #[arg(long, default_value = "2s")]
        interval: String,
        /// Print once and exit.
        #[arg(long)]
        once: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Node metrics: the latest of every node, or one node's history.
    Metrics {
        /// Node name or id prefix.
        node: Option<String>,
        /// How far back, e.g. 1h, 24h, 7d (10 s steps up to 1h, 1 min up
        /// to 24h, 10 min beyond).
        #[arg(long, default_value = "1h")]
        since: String,
        #[arg(short = 'o', long, value_enum, default_value = "table")]
        output: manage::Output,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// A task's output (stdout and stderr), also while it runs.
    Logs {
        job: String,
        /// Task index.
        #[arg(short = 't', long, default_value_t = 0)]
        task: u32,
        /// Keep printing until the task finishes.
        #[arg(short = 'f', long)]
        follow: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Cluster events: nodes coming and going, jobs finishing, failed
    /// checks, alerts.
    Events {
        /// Keep printing new events until Ctrl-C.
        #[arg(short = 'f', long)]
        follow: bool,
        /// Only events this recent, e.g. 1h.
        #[arg(long)]
        since: Option<String>,
        /// How many recent events to show.
        #[arg(short = 'n', long, default_value_t = 50)]
        limit: u32,
        #[arg(short = 'o', long, value_enum, default_value = "table")]
        output: manage::Output,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Open the web dashboard: a web server on this machine that talks to
    /// the cluster with your ticket, so the control plane opens no port.
    /// Nodes, jobs (submit, live output, downloads), storage, CU, invites,
    /// prices and events, plus a JSON API under /api/v1.
    Ui(ui::UiArgs),
    /// Control plane replicas (high availability): status, invite, remove.
    #[command(subcommand)]
    Ha(ha_cmd::HaCmd),
    /// Copy the control plane's state (jobs, ledger, accounts, receipts,
    /// datasets, settings) and the cluster key into FILE, for `pumasi
    /// cluster restore`.
    Backup {
        file: PathBuf,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Alert rules: node offline, failure rate, disk, temperature,
    /// benchmark drops, SMART, check mismatches.
    #[command(subcommand)]
    Alert(monitor::AlertCmd),
    /// Where alerts go: Slack, Discord or any URL that takes JSON.
    #[command(subcommand)]
    Webhook(monitor::WebhookCmd),
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
        /// Give the module this machine's best WebGPU adapter (the
        /// `pumasi_webgpu` imports), as a wasm job with `gpus = 1` gets.
        #[arg(long, conflicts_with = "seed")]
        gpu: bool,
        /// With --gpu, accept a software rasterizer when there is no GPU.
        #[arg(long, requires = "gpu")]
        webgpu_software: bool,
    },
}

#[derive(Subcommand)]
enum StoreCmd {
    /// Upload files and directories as a new version of dataset NAME.
    /// Run it again after an interruption: only what is missing is sent.
    Put {
        name: String,
        /// Files and directories (directories keep their paths inside).
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Account that stores the dataset and pays for it (default: owner).
        #[arg(long)]
        account: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Download a dataset (NAME, NAME@VERSION) or a job's stored outputs
    /// (--job ID). Run it again after an interruption to resume.
    Get {
        /// NAME or NAME@VERSION.
        dataset: Option<String>,
        /// Download this job's outputs instead (`task-<index>/...`).
        #[arg(long, conflicts_with = "dataset")]
        job: Option<String>,
        /// Destination directory (default: ./NAME or ./pumasi-out/<job>).
        #[arg(long, short)]
        out: Option<PathBuf>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// List datasets (the latest version of each, or every version of NAME).
    Ls {
        name: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Delete a dataset version (NAME@VERSION) or every version (NAME).
    /// The disk space comes back once no job uses the files.
    Rm {
        dataset: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// What each account stores, its quota and the control plane's disk use.
    Usage {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Set an account's storage quota (e.g. 100G), or `none` to remove it.
    Quota {
        account: String,
        size: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Subcommand)]
enum SecretCmd {
    /// Store a secret. `registry/<host>` (e.g. registry/ghcr.io) holds
    /// `user:password` or a token for pulling private images; nodes never
    /// see it. Without VALUE it is read from stdin.
    Set {
        name: String,
        value: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// List secret names.
    Ls {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Delete a secret.
    Rm {
        name: String,
        #[command(flatten)]
        admin: AdminArgs,
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
        /// Also install a GPU guest bundle (a directory with `vmlinux` and
        /// `drivers.ext4`, built by scripts/kernel/build-gpu.sh), so GPUs
        /// bound to vfio-pci can be passed through to microVMs. Linux with
        /// KVM and QEMU only; experimental.
        #[arg(long, value_name = "DIR")]
        gpu_bundle: Option<PathBuf>,
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
        /// Give the VM a network card that reaches public addresses only
        /// (needs QEMU).
        #[arg(long)]
        network: bool,
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
    /// VM monitor: firecracker, qemu or vz (default: Firecracker when
    /// installed and KVM is usable, vz on macOS 11+, else QEMU).
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
enum CuCmd {
    /// Show every account's balance.
    List {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Grant CU to an account (prepaid mode; bookkeeping, no money moves).
    Add {
        account: String,
        /// CU, e.g. 50 or 12.5.
        amount: String,
        #[arg(long, default_value = "")]
        memo: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Record CU leaving an account's available balance (prepaid mode).
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
    /// Change rates or policy; options left out keep their value. CPU time
    /// has no rate: one reference vCPU-hour is one CU.
    Set {
        /// CU per GiB-hour of reserved memory, e.g. 0.25.
        #[arg(long)]
        memory_gib_hour: Option<String>,
        /// CU per GiB-month of stored data.
        #[arg(long)]
        storage_gib_month: Option<String>,
        /// CU per GiB sent out of the cluster.
        #[arg(long)]
        egress_gib: Option<String>,
        /// CU per hour of GPU time per FP32 TFLOPS, e.g. 0.1.
        #[arg(long)]
        gpu_tflops_hour: Option<String>,
        /// CU per hour per GiB of GPU memory, e.g. 0.3.
        #[arg(long)]
        gpu_vram_gib_hour: Option<String>,
        /// CU per GiB-month stored when the control plane's disk is a
        /// hard disk (`storage-gib-month` is the SSD rate).
        #[arg(long)]
        storage_hdd_gib_month: Option<String>,
        /// CU per GiB-month stored on an NVMe control plane disk.
        #[arg(long)]
        storage_nvme_gib_month: Option<String>,
        /// Storage quota of accounts without their own (e.g. 50G), or
        /// `none`.
        #[arg(long)]
        default_quota: Option<String>,
        /// `prepaid` (the owner grants CU) or `mutual` (품앗이: lending
        /// earns CU, using spends them, nobody grants any).
        #[arg(long)]
        ledger: Option<LedgerArg>,
        /// Mutual mode: how far below zero an account may go, in CU.
        #[arg(long)]
        credit_limit: Option<String>,
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
        /// The kind of disk the file store is on, which picks the storage
        /// rate (`pumasi prices`).
        #[arg(long, value_enum)]
        storage_class: Option<StorageClassArg>,
        #[command(flatten)]
        metrics: MetricsArgs,
    },
    /// Join an existing cluster's control plane as a replica, with a
    /// ticket from `pumasi ha invite`, and run it (restart it later with
    /// `pumasi cluster start`).
    Replica {
        ticket: String,
        #[command(flatten)]
        net: NetArgs,
        /// The kind of disk the file store is on.
        #[arg(long, value_enum)]
        storage_class: Option<StorageClassArg>,
        #[command(flatten)]
        metrics: MetricsArgs,
    },
    /// Make a control plane from a `pumasi backup` file (into an empty
    /// cluster directory).
    Restore { file: PathBuf },
}

#[derive(Args, Clone, Default)]
struct MetricsArgs {
    /// Serve Prometheus metrics on this address, e.g. 127.0.0.1:9464
    /// (`/metrics`). Off unless given.
    #[arg(long, value_name = "ADDR")]
    metrics_addr: Option<SocketAddr>,
    /// Require `Authorization: Bearer TOKEN` on /metrics.
    #[arg(long, env = "PUMASI_METRICS_TOKEN", hide_env_values = true)]
    metrics_token: Option<String>,
    /// How long node metrics and events are kept, e.g. 7d.
    #[arg(long, default_value = "7d")]
    metrics_retention: String,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum StorageClassArg {
    Hdd,
    Ssd,
    Nvme,
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

#[derive(Clone, Copy, clap::ValueEnum)]
enum GraphicsArg {
    Auto,
    Yes,
    No,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum LedgerArg {
    Prepaid,
    Mutual,
}

#[derive(Args, Clone)]
struct AgentOpts {
    /// Node name shown in `pumasi status` (default: hostname).
    #[arg(long)]
    name: Option<String>,
    /// Tasks to run at once (default: the CPUs offered, fewer with little
    /// memory: one per GiB beyond the first two).
    #[arg(long)]
    slots: Option<u32>,
    /// Offer only this share of the CPUs (1-100). Image jobs that need
    /// more CPUs go to other machines.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=100))]
    max_cpu_percent: Option<u32>,
    /// Take new tasks only while on AC power (laptops). Running tasks
    /// finish when the power cord comes out.
    #[arg(long)]
    only_on_ac: bool,
    /// Take new tasks only after the keyboard and mouse have been idle
    /// this many minutes.
    #[arg(long, value_name = "MINUTES")]
    idle_minutes: Option<u64>,
    /// Take new tasks only at these local times, e.g. `22:00-07:00` or
    /// `00:00-08:00,12:00-13:00`.
    #[arg(long, value_name = "HH:MM-HH:MM[,...]", value_parser = parse_hours)]
    hours: Option<Hours>,
    /// Also run docker jobs on this machine. Only for machines you own:
    /// containers share this machine's kernel.
    #[arg(long)]
    allow_docker: bool,
    /// Docker-compatible CLI to use for docker jobs.
    #[arg(long, default_value = "docker")]
    docker_bin: String,
    /// Whether Docker GPU tasks here can draw (OpenGL, EGL, Vulkan), so
    /// jobs with `capabilities = ["graphics"]` come here: `auto` goes by
    /// the last `pumasi gpu check --docker-run`.
    #[arg(long, value_enum, default_value = "auto")]
    gpu_graphics: GraphicsArg,
    /// Also run image jobs in microVMs. Each task gets its own VM and
    /// kernel, so this is safe on machines you share with a cluster.
    /// Needs `pumasi vm setup` first.
    #[arg(long)]
    allow_microvm: bool,
    /// Also offer this machine's GPUs to wasm jobs that ask for one, through
    /// WebGPU (Vulkan, Metal, DX12 or OpenGL): experimental. The only way
    /// to use GPUs without CUDA, such as integrated GPUs and old Macs'.
    #[arg(long, env = "PUMASI_ALLOW_WEBGPU")]
    allow_webgpu: bool,
    /// With --allow-webgpu, offer a software rasterizer (lavapipe,
    /// llvmpipe, WARP) too when the machine has one. Slow; for testing.
    #[arg(long, env = "PUMASI_WEBGPU_SOFTWARE")]
    webgpu_software: bool,
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
    /// Do not benchmark this machine. Its runs are then billed at the
    /// reference CPU score (1000).
    #[arg(long)]
    no_benchmark: bool,
    /// Do not offer this machine's GPUs to the cluster (they are still
    /// listed in the device profile, but no job gets them).
    #[arg(long)]
    no_gpus: bool,
    /// How often to benchmark again (while idle), e.g. 24h or 7d.
    #[arg(long, default_value = "24h")]
    bench_interval: String,
    /// Disk space for datasets and private images fetched from the
    /// cluster's file store, e.g. 50G (default 20G). The least recently
    /// used go first.
    #[arg(long, value_name = "SIZE")]
    store_cache: Option<String>,
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

/// `--hours`: one value holding several ranges.
#[derive(Debug, Clone)]
struct Hours(Vec<pumasi_agent::availability::HourRange>);

fn parse_hours(s: &str) -> Result<Hours, String> {
    pumasi_agent::availability::parse_hours(s)
        .map(Hours)
        .map_err(|e| format!("{e:#}"))
}

fn main() {
    pumasi_proto::build_info();
    // The signed Virtualization.framework helper (a copy of this binary)
    // runs one VM on its main thread; see `pumasi_agent::microvm::vz`.
    let mut args = std::env::args_os().skip(1);
    if args
        .next()
        .is_some_and(|a| a == pumasi_agent::microvm::vz::RUN_ARG)
    {
        let config = PathBuf::from(args.next().unwrap_or_default());
        std::process::exit(pumasi_agent::microvm::vz::run_main(&config));
    }
    let cli = Cli::parse();
    let long_running = matches!(
        cli.cmd,
        Cmd::Cluster(_) | Cmd::Agent(_) | Cmd::Service(service::ServiceCmd::Run { .. })
    );
    let default_filter = if long_running {
        "pumasi=info,openraft=error,warn"
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

    // The command future is large: keep it on the heap, not the stack
    // (the main thread has only 1 MiB on Windows; see build.rs).
    let runtime = tokio::runtime::Runtime::new().expect("start the async runtime");
    let code = runtime.block_on(Box::pin(run(cli)));
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
/// [`net_for`] over every address of a cluster: public if any has a relay.
fn net_for_all(addrs: &[iroh::EndpointAddr], explicit: Option<NetMode>) -> NetMode {
    explicit.unwrap_or_else(|| {
        if addrs.iter().any(|a| a.relay_urls().next().is_some()) {
            NetMode::Public
        } else {
            NetMode::Local
        }
    })
}

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
            storage_class,
            metrics,
        }) => cluster_start(&home, name, net, no_token, storage_class, metrics, None).await,
        Cmd::Cluster(ClusterCmd::Replica {
            ticket,
            net,
            storage_class,
            metrics,
        }) => {
            let ticket: pumasi_proto::ticket::ReplicaTicket = ticket.parse()?;
            cluster_start(
                &home,
                String::new(),
                net,
                true,
                storage_class,
                metrics,
                Some(ticket),
            )
            .await
        }
        Cmd::Cluster(ClusterCmd::Restore { file }) => ha_cmd::restore(&home, &file),
        Cmd::Ha(cmd) => ha_cmd::run(&home, cmd).await,
        Cmd::Backup { file, admin } => ha_cmd::backup(&home, &admin, &file).await,
        Cmd::Top {
            interval,
            once,
            admin,
        } => monitor::top(&home, &admin, &interval, once).await,
        Cmd::Metrics {
            node,
            since,
            output,
            admin,
        } => monitor::show_metrics(&home, &admin, node, &since, output).await,
        Cmd::Logs {
            job,
            task,
            follow,
            admin,
        } => monitor::logs(&home, &admin, job.parse()?, task, follow).await,
        Cmd::Events {
            follow,
            since,
            limit,
            output,
            admin,
        } => monitor::events(&home, &admin, since.as_deref(), limit, follow, output).await,
        Cmd::Ui(args) => ui::run(&home, args).await,
        Cmd::Alert(cmd) => monitor::alert(&home, cmd).await,
        Cmd::Webhook(cmd) => monitor::webhook(&home, cmd).await,
        Cmd::Store(cmd) => store(&home, cmd).await,
        Cmd::Secret(cmd) => secret(&home, cmd).await,
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
        Cmd::Cu(cmd) => cu(&home, cmd).await,
        Cmd::Prices { cmd, admin } => prices(&home, cmd, &admin).await,
        Cmd::Bench {
            dir,
            quick,
            json,
            cpus,
        } => bench(&home, dir, quick, json, cpus).await,
        Cmd::Gpu(cmd) => gpu_cmd::run(&home, cmd).await,
        Cmd::Node {
            id,
            rebenchmark,
            admin,
        } => {
            let client = connect_admin(&home, &admin).await?;
            let AdminResponse::Status(s) = client.call(AdminOp::Status).await? else {
                bail!("unexpected response");
            };
            let matches: Vec<&NodeView> = s
                .nodes
                .iter()
                .filter(|n| n.id.starts_with(id.trim()))
                .collect();
            let n = match matches.as_slice() {
                [one] => *one,
                [] => bail!("no such node"),
                _ => bail!("node id prefix {id:?} is ambiguous; give more characters"),
            };
            if rebenchmark {
                client.call(AdminOp::Benchmark { id: n.id.clone() }).await?;
                eprintln!(
                    "asked {} to benchmark again; check back in a minute",
                    n.info.name
                );
            }
            client.close().await;
            print_node(n);
            Ok(0)
        }
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
            priority,
            after,
            admin,
        } => {
            let (mut sub, upload) = JobFile::load_split(&job_file, INLINE_INPUT_BYTES)?;
            if let Some(a) = &account {
                validate_account(a)?;
            }
            sub.account = account;
            if let Some(p) = priority {
                sub.priority = p;
            }
            for a in &after {
                sub.after.push(a.parse()?);
            }
            sub.validate()?;
            let tasks = sub.tasks.len();
            let client = connect_admin(&home, &admin).await?;
            // Inputs too large to send with the job go through the file store.
            let id = client.submit(sub, &upload, |m| eprintln!("{m}")).await?;
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
            eprintln!(
                "saved the ticket to {} (`pumasi whoami` shows what it may do)",
                path.display()
            );
            Ok(0)
        }
        Cmd::Cordon { node, admin } => {
            let edit = pumasi_proto::manage::NodeEdit {
                cordon: Some(true),
                ..Default::default()
            };
            manage::edit_node(&home, &admin, &node, edit).await?;
            eprintln!("cordoned {node}: it takes no new tasks; running ones finish");
            Ok(0)
        }
        Cmd::Uncordon { node, admin } => {
            let edit = pumasi_proto::manage::NodeEdit {
                cordon: Some(false),
                ..Default::default()
            };
            manage::edit_node(&home, &admin, &node, edit).await?;
            eprintln!("uncordoned {node}");
            Ok(0)
        }
        Cmd::Drain { node, admin } => manage::drain(&home, &admin, &node).await,
        Cmd::Label {
            node,
            labels,
            admin,
        } => {
            let edit = manage::label_edit(&labels)?;
            manage::edit_node(&home, &admin, &node, edit).await
        }
        Cmd::Taint {
            node,
            taints,
            admin,
        } => {
            let edit = manage::taint_edit(&taints)?;
            manage::edit_node(&home, &admin, &node, edit).await
        }
        Cmd::Quota {
            account,
            slots,
            gpus,
            cu_per_day,
            max_priority,
            storage,
            clear,
            policy,
            admin,
        } => {
            manage::quota(
                &home,
                &admin,
                account,
                slots,
                gpus,
                cu_per_day,
                max_priority,
                storage,
                clear,
                policy,
            )
            .await
        }
        Cmd::Access(cmd) => manage::access(&home, cmd).await,
        Cmd::Whoami { admin } => manage::whoami(&home, &admin).await,
        Cmd::Schedule(cmd) => manage::schedule(&home, cmd).await,
        Cmd::Apply {
            file,
            dry_run,
            prune,
            admin,
        } => manage::apply(&home, &admin, &file, dry_run, prune).await,
        Cmd::Get {
            kind,
            selector,
            output,
            admin,
        } => manage::get(&home, &admin, kind, selector, output).await,
        Cmd::Describe { kind, name, admin } => manage::describe(&home, &admin, kind, &name).await,
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
            gpu,
            webgpu_software,
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
                gpu.then_some(webgpu_software),
            )
            .await
        }
    }
}

async fn cluster_start(
    home: &Path,
    name: String,
    net: NetArgs,
    no_token: bool,
    storage_class: Option<StorageClassArg>,
    metrics: MetricsArgs,
    replica: Option<pumasi_proto::ticket::ReplicaTicket>,
) -> Result<i32> {
    let mut config = ClusterConfig::new(home.join("cluster"));
    config.name = name;
    config.metrics_addr = metrics.metrics_addr;
    config.metrics_token = metrics.metrics_token.filter(|t| !t.is_empty());
    config.metrics_retention =
        Duration::from_millis(parse_duration_ms(&metrics.metrics_retention)?);
    config.disk_class = match storage_class {
        Some(StorageClassArg::Hdd) => DiskClass::Hdd,
        Some(StorageClassArg::Ssd) => DiskClass::Ssd,
        Some(StorageClassArg::Nvme) => DiskClass::Nvme,
        None => DiskClass::Unknown,
    };
    config.net = match (&replica, net.net) {
        (_, Some(n)) => n,
        (Some(t), None) => net_for_all(&t.candidates(), None),
        (None, None) => NetMode::default(),
    };
    // A fixed port lets agents reconnect after a restart in local mode.
    config.bind = Some(net.bind.unwrap_or_else(|| "0.0.0.0:7480".parse().unwrap()));
    let cluster = match &replica {
        Some(t) => Cluster::join_replica(config.clone(), t).await?,
        None => Cluster::start(config.clone()).await?,
    };
    eprintln!();
    if !cluster.is_leader() {
        eprintln!(
            "control plane replica {} is running and follows the leader",
            short(&cluster.endpoint_id().to_string())
        );
        eprintln!("`pumasi ha status` shows the replicas; this one takes over if the leader fails");
        eprintln!();
        tokio::signal::ctrl_c().await?;
        eprintln!("shutting down");
        cluster.shutdown().await?;
        return Ok(0);
    }
    let status = cluster.status();
    eprintln!(
        "cluster \"{}\" is running ({} known nodes)",
        status.cluster_name,
        status.nodes.len()
    );
    eprintln!(
        "admin ticket saved to {}",
        config.data_dir.join(ADMIN_TICKET_FILE).display()
    );
    if let Some(addr) = cluster.metrics_addr() {
        eprintln!("Prometheus metrics at http://{addr}/metrics");
    }
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
        Some(t) => net_for_all(&t.candidates(), opts.net.net),
        None => {
            let saved = std::fs::read_to_string(data_dir.join("membership.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<pumasi_agent::Membership>(&s).ok());
            match saved {
                Some(m) => net_for_all(
                    &pumasi_proto::ticket::candidates(&m.cluster, &m.replicas),
                    opts.net.net,
                ),
                None => opts.net.net.unwrap_or_default(),
            }
        }
    };
    // The drive measured is the one under the agent's data directory.
    let bench = if opts.no_benchmark {
        None
    } else {
        let mut b = BenchConfig::standard(data_dir.clone());
        b.interval = Duration::from_millis(parse_duration_ms(&opts.bench_interval)?);
        Some(b)
    };
    let config = AgentConfig {
        data_dir,
        name: opts.name.unwrap_or_else(default_node_name),
        slots: opts.slots,
        max_cpu_percent: opts.max_cpu_percent,
        availability: {
            let policy = pumasi_agent::availability::Policy {
                only_on_ac: opts.only_on_ac,
                idle_for: opts.idle_minutes.map(|m| Duration::from_secs(m * 60)),
                hours: opts.hours.clone().map(|h| h.0).unwrap_or_default(),
            };
            policy
                .is_active()
                .then(|| pumasi_agent::AvailabilityConfig::new(policy))
        },
        net,
        bind: opts.net.bind,
        docker: opts.allow_docker.then(|| opts.docker_bin.clone()),
        microvm: opts.allow_microvm.then(|| opts.vm.config(home)),
        bench,
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
        gpus: !opts.no_gpus,
        fake_gpus: None,
        webgpu: (opts.allow_webgpu && !opts.no_gpus).then_some(pumasi_agent::webgpu::Options {
            software: opts.webgpu_software,
            bench: !opts.no_benchmark,
        }),
        store_cache_bytes: opts.store_cache.as_deref().map(parse_size).transpose()?,
        gpu_graphics: match opts.gpu_graphics {
            GraphicsArg::Auto => None,
            GraphicsArg::Yes => Some(true),
            GraphicsArg::No => Some(false),
        },
    };
    let exe = config.upgrade.as_ref().map(|u| u.exe.clone());
    let agent = Agent::new(config).await?;
    eprintln!(
        "agent {} ({}, {} slots, {}) connecting...",
        agent.info().name,
        agent.id().fmt_short(),
        agent.info().slots,
        executor_list(&agent.executors_now().executors)
    );
    for (e, why) in agent.executors_now().unavailable {
        eprintln!("{e} jobs wait until it is usable: {why} (checked every 30 s)");
    }
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
    let ticket = load_ticket(home, args)?;
    let net = net_for_all(&ticket.candidates(), None);
    AdminClient::connect(&ticket, net).await
}

/// The admin or access ticket: `--admin`, else the one `pumasi login` or a
/// local cluster saved.
fn load_ticket(home: &Path, args: &AdminArgs) -> Result<AdminTicket> {
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
    text.parse()
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
        "{:<12} {:<16} {:<9} {:<18} {:<14} {:<12} {:<12} {:>5} {:>8} {:>10} {:>8} {:>6} {:<7} {:>8}",
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
        "CHECKS",
        "SCORE",
        "DISK",
        "RTT ms"
    );
    for n in &s.nodes {
        let role = match n.role {
            Role::Owner => "owner".to_string(),
            Role::Provider => format!("provider:{}", n.account),
        };
        let state = if n.suspended {
            "suspended"
        } else if n.online && n.config.cordoned {
            "cordoned"
        } else if n.online && n.paused.is_some() {
            "paused"
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
        let disk = n
            .perf
            .as_ref()
            .and_then(|p| p.disk.as_ref())
            .map(|d| d.class.to_string())
            .unwrap_or_else(|| "-".into());
        let rtt = n
            .rtt_us
            .map(|r| format!("{:.1}", r as f64 / 1000.0))
            .unwrap_or_else(|| "-".into());
        let score = format!(
            "{}{}",
            n.cpu_score,
            if n.perf_note.is_some() { "*" } else { "" }
        );
        println!(
            "{:<12} {:<16} {:<9} {:<18} {:<14} {:<12} {:<12} {:>5} {:>8} {:>10} {:>8} {:>6} {:<7} {:>8}",
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
            checks,
            score,
            disk,
            rtt
        );
    }
    if s.nodes.iter().any(|n| n.perf_note.is_some()) {
        println!("(SCORE: billed CPU score, 1000 = reference vCPU; * see `pumasi node <id>`)");
    }
    for n in s.nodes.iter().filter(|n| !n.gpus.is_empty()) {
        for g in &n.gpus {
            println!("{:<16} {}", truncate(&n.info.name, 16), gpu_line(g));
        }
    }
    for n in s.nodes.iter() {
        for w in &n.webgpu {
            println!("{:<16} {}", truncate(&n.info.name, 16), webgpu_line(w));
        }
    }
    for n in s.nodes.iter().filter(|n| n.online) {
        if let Some(reason) = &n.paused {
            println!("{} is paused: {reason}", truncate(&n.info.name, 16));
        }
        for (e, why) in &n.unavailable {
            println!("{}: {e} unavailable: {why}", truncate(&n.info.name, 16));
        }
    }
    if let Some(c) = &s.capacity {
        print_capacity(c);
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
            format_cu(j.cost)
        );
    }
    if s.jobs.is_empty() {
        println!("(no jobs yet: submit one with `pumasi run job.toml`)");
    }
}

/// Which nodes take the account's work now.
fn print_capacity(c: &pumasi_proto::msg::Capacity) {
    let ready: Vec<String> = c.ready.iter().map(|(e, n)| format!("{e} {n}")).collect();
    let mut line = format!(
        "ready for account {} now: {}, GPU {}",
        c.account,
        ready.join(", "),
        c.ready_gpu
    );
    if c.ready_gpu_graphics > 0 {
        line.push_str(&format!(" (graphics {})", c.ready_gpu_graphics));
    }
    println!();
    println!("{line}");
    for n in &c.nodes {
        if let Some(why) = &n.reason {
            println!("  {}: {why}", truncate(&n.node, 16));
        }
    }
}

fn print_job(v: &JobView, output: bool) {
    let s = &v.summary;
    println!("job {} \"{}\": {}", s.id, s.name, s.state);
    match &v.image {
        Some(image) => println!("  executor {} ({image})", v.executor),
        None => println!("  executor {}", v.executor),
    }
    if let Some(g) = &v.gpu {
        println!("  gpus: {g}");
    }
    if v.network {
        println!("  network: on");
    }
    if s.priority != 0 {
        println!("  priority {}", s.priority);
    }
    if !v.selector.is_empty() {
        println!("  selector {}", v.selector);
    }
    if !v.tolerations.is_empty() {
        let t: Vec<String> = v.tolerations.iter().map(|t| t.to_string()).collect();
        println!("  tolerates {}", t.join(","));
    }
    if !v.after.is_empty() {
        let a: Vec<String> = v.after.iter().map(|j| j.to_string()).collect();
        println!("  after {}", a.join(","));
    }
    if let Some(w) = &v.waiting {
        println!("  {w}");
    }
    if v.schedule.is_none() && !v.submitted_by.is_empty() {
        println!("  submitted by {}", v.submitted_by);
    }
    if let Some(name) = &v.schedule {
        println!("  submitted by schedule {name}");
    }
    for d in &v.image_digests {
        println!("  image digest {d}");
    }
    if let Some(t) = v.queue_timeout_ms {
        println!(
            "  queue timeout {}",
            pumasi_proto::job::format_duration_ms(t)
        );
    }
    if !v.gpu_capabilities.is_empty() {
        let c: Vec<String> = v.gpu_capabilities.iter().map(|c| c.to_string()).collect();
        println!("  gpu capabilities {}", c.join(","));
    }
    println!(
        "  account {}  cost {} CU{}",
        s.account,
        format_cu(s.cost),
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
            format!(" cost={}", format_cu(t.cost))
        } else {
            String::new()
        };
        let cost = if t.preempted > 0 {
            format!("{cost} moved={}", t.preempted)
        } else {
            cost
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

/// `GPU 0 NVIDIA GeForce RTX 3060 (12 GiB, 12.7 TFLOPS) docker [busy 3f2a…/0]`.
fn webgpu_line(w: &pumasi_proto::msg::WebGpuView) -> String {
    let a = &w.adapter;
    let mut s = format!("WebGPU {} {} wasm", a.index, a.describe());
    if let Some(g) = a.gpu {
        s.push_str(&format!(" (GPU {g})"));
    }
    if w.billed_gflops != a.fp32_gflops {
        s.push_str(&format!(
            " billed {} TFLOPS",
            pumasi_proto::gpu::format_tflops(w.billed_gflops)
        ));
    }
    if let Some(t) = &w.busy {
        s.push_str(&format!(" [running {t}]"));
    }
    s
}

fn gpu_line(g: &pumasi_proto::msg::GpuView) -> String {
    let i = &g.info;
    let mut uses = Vec::new();
    if i.docker {
        uses.push("docker");
    }
    if i.vfio {
        uses.push("microvm");
    }
    let mut s = format!(
        "GPU {} {} {}",
        i.index,
        i.describe(),
        if uses.is_empty() {
            "(not usable here)".to_string()
        } else {
            uses.join(",")
        }
    );
    if g.billed_gflops != i.fp32_gflops {
        s.push_str(&format!(
            " billed {} TFLOPS",
            pumasi_proto::gpu::format_tflops(g.billed_gflops)
        ));
    }
    if let Some(t) = &g.busy {
        s.push_str(&format!(" [running {t}]"));
    }
    s
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
    if view.tasks.iter().any(|t| t.stored.is_some()) {
        let (files, bytes) = client
            .store_get(
                StoreTarget::Job {
                    id: view.summary.id,
                },
                dir,
                |m| eprintln!("outputs: {m}"),
            )
            .await?;
        eprintln!(
            "saved {files} output files ({}) from the file store",
            format_size(bytes)
        );
        count += files as usize;
    }
    if count > 0 || always_report {
        eprintln!("saved {count} output files to {}", dir.display());
    }
    Ok(count)
}

/// Shared input files above this go through the file store.
const INLINE_INPUT_BYTES: u64 = 16 << 20;

async fn store(home: &Path, cmd: StoreCmd) -> Result<i32> {
    match cmd {
        StoreCmd::Put {
            name,
            paths,
            account,
            admin,
        } => {
            pumasi_proto::store::validate_dataset_name(&name)?;
            if let Some(a) = &account {
                validate_account(a)?;
            }
            let files: Vec<(String, PathBuf)> = put_files(&paths)?;
            anyhow::ensure!(!files.is_empty(), "no files to upload");
            let client = connect_admin(home, &admin).await?;
            let (stored, view) = client
                .store_put(&files, Some(name.clone()), account, |m| eprintln!("{m}"))
                .await?;
            client.close().await;
            match view {
                Some(d) => println!(
                    "{}@{}: {} files, {}",
                    d.name,
                    d.version,
                    d.files,
                    format_size(d.size)
                ),
                None => println!(
                    "{name}: {} files, {}",
                    stored.files,
                    format_size(stored.size)
                ),
            }
            Ok(0)
        }
        StoreCmd::Get {
            dataset,
            job,
            out,
            admin,
        } => {
            let (target, default_dir) = match (dataset, job) {
                (Some(d), None) => {
                    let r: StoreRef = d.parse()?;
                    anyhow::ensure!(r.path.is_none(), "give a dataset as NAME or NAME@VERSION");
                    let dir = PathBuf::from(&r.name);
                    (
                        StoreTarget::Dataset {
                            name: r.name,
                            version: r.version,
                        },
                        dir,
                    )
                }
                (None, Some(j)) => {
                    let id: JobId = j.parse()?;
                    (
                        StoreTarget::Job { id },
                        PathBuf::from("pumasi-out").join(id.to_string()),
                    )
                }
                _ => bail!("name a dataset or give --job ID"),
            };
            let dir = out.unwrap_or(default_dir);
            let client = connect_admin(home, &admin).await?;
            let (files, bytes) = client.store_get(target, &dir, |m| eprintln!("{m}")).await?;
            client.close().await;
            eprintln!(
                "saved {files} files ({}) to {}",
                format_size(bytes),
                dir.display()
            );
            Ok(0)
        }
        StoreCmd::Ls { name, admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Datasets(list) = client.call(AdminOp::StoreList { name }).await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            print_datasets(&list);
            Ok(0)
        }
        StoreCmd::Rm { dataset, admin } => {
            let r: StoreRef = dataset.parse()?;
            anyhow::ensure!(r.path.is_none(), "give a dataset as NAME or NAME@VERSION");
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Datasets(gone) = client
                .call(AdminOp::StoreRemove {
                    name: r.name,
                    version: r.version,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            for d in gone {
                eprintln!("removed {}@{}", d.name, d.version);
            }
            Ok(0)
        }
        StoreCmd::Usage { admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::StoreUsage { usage, disk_bytes } =
                client.call(AdminOp::StoreUsage).await?
            else {
                bail!("unexpected response");
            };
            let AdminResponse::StoragePolicy { policy, class } =
                client.call(AdminOp::GetStoragePolicy).await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            println!(
                "{:<16} {:>12} {:>12} {:>9}",
                "ACCOUNT", "STORED", "QUOTA", "DATASETS"
            );
            for u in &usage {
                println!(
                    "{:<16} {:>12} {:>12} {:>9}",
                    u.account,
                    format_size(u.bytes),
                    u.quota.map(format_size).unwrap_or_else(|| "-".into()),
                    u.datasets
                );
            }
            println!(
                "control plane disk: {} ({class}; files shared between versions count once)",
                format_size(disk_bytes)
            );
            if let Some(q) = policy.default_quota {
                println!("default quota: {}", format_size(q));
            }
            Ok(0)
        }
        StoreCmd::Quota {
            account,
            size,
            admin,
        } => {
            validate_account(&account)?;
            let bytes = match size.as_str() {
                "none" | "off" => None,
                s => Some(parse_size(s)?),
            };
            let client = connect_admin(home, &admin).await?;
            client
                .call(AdminOp::StoreQuota {
                    account: account.clone(),
                    bytes,
                })
                .await?;
            client.close().await;
            match bytes {
                Some(b) => eprintln!("{account} may store {}", format_size(b)),
                None => eprintln!("{account} has no quota of its own"),
            }
            Ok(0)
        }
    }
}

/// The files `pumasi store put` uploads: files by name, directories with
/// the paths inside them (`data/` gives `a.csv`, `sub/b.csv`, ...).
fn put_files(paths: &[PathBuf]) -> Result<Vec<(String, PathBuf)>> {
    let mut out = std::collections::BTreeMap::new();
    for p in paths {
        let meta = std::fs::metadata(p).with_context(|| format!("read {}", p.display()))?;
        if meta.is_dir() {
            for (rel, full, _) in pumasi_proto::job::dir_files(p)? {
                out.insert(rel, full);
            }
        } else {
            let name = p
                .file_name()
                .context("bad file name")?
                .to_string_lossy()
                .into_owned();
            out.insert(name, p.clone());
        }
    }
    Ok(out.into_iter().collect())
}

fn print_datasets(list: &[DatasetView]) {
    if list.is_empty() {
        println!("no datasets");
        return;
    }
    println!(
        "{:<24} {:>7} {:>8} {:>11}  {:<10} {:<20} JOB",
        "NAME", "VERSION", "FILES", "SIZE", "ACCOUNT", "CREATED"
    );
    for d in list {
        println!(
            "{:<24} {:>7} {:>8} {:>11}  {:<10} {:<20} {}",
            d.name,
            d.version,
            d.files,
            format_size(d.size),
            d.account,
            utc(d.created_unix),
            d.job.map(|j| j.to_string()).unwrap_or_default()
        );
    }
}

async fn secret(home: &Path, cmd: SecretCmd) -> Result<i32> {
    match cmd {
        SecretCmd::Set { name, value, admin } => {
            let value = match value {
                Some(v) => v,
                None => {
                    let mut v = String::new();
                    std::io::Read::read_to_string(&mut std::io::stdin(), &mut v)?;
                    v.trim_end_matches(['\r', '\n']).to_string()
                }
            };
            let client = connect_admin(home, &admin).await?;
            client
                .call(AdminOp::SecretSet {
                    name: name.clone(),
                    value,
                })
                .await?;
            client.close().await;
            eprintln!("saved secret {name}");
            Ok(0)
        }
        SecretCmd::Ls { admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Secrets(list) = client.call(AdminOp::SecretList).await? else {
                bail!("unexpected response");
            };
            client.close().await;
            if list.is_empty() {
                println!("no secrets");
            }
            for s in list {
                println!("{:<40} set {}", s.name, utc(s.set_unix));
            }
            Ok(0)
        }
        SecretCmd::Rm { name, admin } => {
            let client = connect_admin(home, &admin).await?;
            client
                .call(AdminOp::SecretRemove { name: name.clone() })
                .await?;
            client.close().await;
            eprintln!("removed secret {name}");
            Ok(0)
        }
    }
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
    gpu: Option<bool>,
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
    let webgpu = match gpu {
        Some(software) => {
            let (gpu, notes) = tokio::task::spawn_blocking(move || {
                let mut notes = Vec::new();
                let gpu = pumasi_agent::webgpu::WebGpu::detect(
                    pumasi_agent::webgpu::Options {
                        software,
                        bench: false,
                    },
                    &[],
                    &mut notes,
                );
                (gpu, notes)
            })
            .await?;
            for n in &notes {
                eprintln!("[pumasi] note: {n}");
            }
            let Some(a) = gpu.adapters().into_iter().next() else {
                bail!(
                    "no WebGPU adapter can run compute shaders here{}",
                    if software {
                        ""
                    } else {
                        " (--webgpu-software accepts a software rasterizer)"
                    }
                );
            };
            eprintln!("[pumasi] GPU: {}", a.describe());
            Some(pumasi_agent::webgpu::WebGpuTask {
                gpu: Arc::new(gpu),
                adapter: a.index,
            })
        }
        None => None,
    };
    let result = tokio::task::spawn_blocking(move || -> Result<_> {
        let ex = WasmExecutor::new()?;
        ex.load_module(hash, &bytes)?;
        let io = pumasi_agent::executor::TaskIo {
            webgpu,
            ..Default::default()
        };
        Ok(ex.run_io(
            &hash,
            &program,
            &input,
            &shared,
            &limits,
            seed,
            Arc::new(AtomicBool::new(false)),
            &io,
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
        VmCmd::Setup {
            guest,
            force,
            gpu_bundle,
            vm,
        } => {
            let config = vm.config(home);
            let report =
                pumasi_agent::microvm::setup::setup(&config.dir, guest.as_deref(), force).await?;
            for f in &report.installed {
                eprintln!("installed {f}");
            }
            if let Some(b) = &gpu_bundle {
                for f in pumasi_agent::microvm::setup::install_gpu_bundle(&config.dir, b)? {
                    eprintln!("installed {f}");
                }
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
            network,
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
                network,
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
    } else if cfg!(target_os = "macos") {
        "optional, untested on macOS; the vz backend needs none"
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
    if let Some(vz) = &a.vz {
        match vz {
            Ok(()) => println!("  {:<12} usable (Virtualization.framework)", "vz"),
            Err(why) => println!("  {:<12} not usable: {why}", "vz"),
        }
    }
    let ready = a.kernel.is_some() && a.guest.is_some();
    match a.pick(config.backend, &config.dir) {
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
            let gpu = config.dir.join(pumasi_agent::microvm::GPU_DIR);
            println!(
                "gpu passthrough: {}",
                if !cfg!(target_os = "linux") {
                    "Linux only"
                } else if !gpu.join(pumasi_agent::microvm::GPU_DRIVER_FILE).is_file() {
                    "not set up (`pumasi vm setup --gpu-bundle DIR`; experimental)"
                } else if !a.kvm || a.qemu.is_none() {
                    "needs QEMU with KVM"
                } else {
                    "ready for GPUs bound to vfio-pci (`pumasi gpu check`)"
                }
            );
            println!(
                "network: {}",
                if vmm.backend == Backend::Qemu
                    || (vmm.backend == Backend::Firecracker && a.qemu.is_some())
                {
                    "jobs with network = true get a filtered network card (QEMU)"
                } else {
                    "not available (jobs with network = true need QEMU)"
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
// Compute units, rates, benchmarks and receipts
// ---------------------------------------------------------------------------

async fn cu(home: &Path, cmd: CuCmd) -> Result<i32> {
    match cmd {
        CuCmd::List { admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Accounts(accounts) = client.call(AdminOp::Accounts).await? else {
                bail!("unexpected response");
            };
            let AdminResponse::Policy(p) = client.call(AdminOp::GetPolicy).await? else {
                bail!("unexpected response");
            };
            client.close().await;
            print_accounts(&accounts);
            if let LedgerMode::Mutual { credit_limit } = p.ledger {
                println!(
                    "(mutual mode: lending earns CU, using spends them; an account may go down to -{} CU)",
                    format_cu(credit_limit)
                );
            }
        }
        CuCmd::Add {
            account,
            amount,
            memo,
            admin,
        } => {
            validate_account(&account)?;
            let amount = parse_cu(&amount)?;
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
        CuCmd::Payout {
            account,
            amount,
            memo,
            admin,
        } => {
            let amount = parse_cu(&amount)?;
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
        CuCmd::Ledger {
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
        "ACCOUNT", "AVAILABLE CU", "HELD", "SPENT", "EARNED"
    );
    for a in accounts {
        println!(
            "{:<16} {:>14} {:>14} {:>14} {:>14}",
            truncate(&a.name, 16),
            format_cu(a.balance),
            format_cu(a.held),
            format_cu(a.spent),
            format_cu(a.earned)
        );
    }
}

fn print_ledger(entries: &[LedgerEntry]) {
    println!(
        "{:>6} {:<20} {:<12} {:<9} {:>14}  DETAIL",
        "SEQ", "TIME (UTC)", "ACCOUNT", "KIND", "CU"
    );
    for e in entries {
        let amount = match e.kind {
            // These move held CU, not the spendable balance.
            LedgerKind::Hold | LedgerKind::Forfeit => format!("({})", format_cu(e.amount)),
            _ => format_cu(e.amount),
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
    let AdminResponse::StoragePolicy {
        policy: mut storage,
        class,
    } = client.call(AdminOp::GetStoragePolicy).await?
    else {
        bail!("unexpected response");
    };
    if let Some(PricesCmd::Set {
        memory_gib_hour,
        storage_gib_month,
        egress_gib,
        gpu_tflops_hour,
        gpu_vram_gib_hour,
        storage_hdd_gib_month,
        storage_nvme_gib_month,
        default_quota,
        ledger,
        credit_limit,
        hold,
        verify_rate,
        max_strikes,
    }) = cmd
    {
        if let Some(v) = memory_gib_hour {
            p.rates.memory_gib_hour = parse_rate(&v)?;
        }
        if let Some(v) = storage_gib_month {
            p.rates.storage_gib_month = parse_rate(&v)?;
        }
        if let Some(v) = egress_gib {
            p.rates.egress_gib = parse_rate(&v)?;
        }
        if let Some(v) = gpu_tflops_hour {
            p.rates.gpu_tflops_hour = parse_rate(&v)?;
        }
        if let Some(v) = gpu_vram_gib_hour {
            p.rates.gpu_vram_gib_hour = parse_rate(&v)?;
        }
        let limit = credit_limit.as_deref().map(parse_cu).transpose()?;
        p.ledger = match (ledger, p.ledger) {
            (Some(LedgerArg::Prepaid), _) => {
                anyhow::ensure!(limit.is_none(), "--credit-limit is for mutual mode");
                LedgerMode::Prepaid
            }
            (Some(LedgerArg::Mutual), old) => LedgerMode::Mutual {
                credit_limit: limit.unwrap_or(match old {
                    LedgerMode::Mutual { credit_limit } => credit_limit,
                    LedgerMode::Prepaid => 20_000,
                }),
            },
            (None, LedgerMode::Mutual { credit_limit }) => LedgerMode::Mutual {
                credit_limit: limit.unwrap_or(credit_limit),
            },
            (None, LedgerMode::Prepaid) => {
                anyhow::ensure!(
                    limit.is_none(),
                    "--credit-limit is for mutual mode (add --ledger mutual)"
                );
                LedgerMode::Prepaid
            }
        };
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
        let before = storage;
        if let Some(v) = storage_hdd_gib_month {
            storage.hdd_gib_month = parse_rate(&v)?;
        }
        if let Some(v) = storage_nvme_gib_month {
            storage.nvme_gib_month = parse_rate(&v)?;
        }
        if let Some(v) = default_quota {
            storage.default_quota = match v.as_str() {
                "none" | "off" => None,
                s => Some(parse_size(s)?),
            };
        }
        if storage != before {
            let AdminResponse::StoragePolicy { policy, .. } =
                client.call(AdminOp::SetStoragePolicy(storage)).await?
            else {
                bail!("unexpected response");
            };
            storage = policy;
        }
        eprintln!("saved");
    }
    client.close().await;
    print_policy(&p);
    print_storage(&p, &storage, class);
    Ok(0)
}

fn print_storage(p: &Policy, s: &StoragePolicy, class: DiskClass) {
    println!("file store (by the control plane's disk, now {class})");
    println!(
        "  storage-hdd-gib-month {:>4}  per GiB-month on a hard disk",
        format_rate(s.hdd_gib_month)
    );
    println!(
        "  storage-gib-month {:>8}  per GiB-month on an SSD (or an unknown disk)",
        format_rate(p.rates.storage_gib_month)
    );
    println!(
        "  storage-nvme-gib-month {:>3}  per GiB-month on NVMe",
        format_rate(s.nvme_gib_month)
    );
    println!(
        "  default-quota {:>12}  per account without its own (`pumasi store quota`)",
        s.default_quota
            .map(format_size)
            .unwrap_or_else(|| "none".into())
    );
}

fn print_policy(p: &Policy) {
    let r = &p.rates;
    println!("compute units: 1 CU = one reference vCPU (CPU score 1000) for one hour");
    println!(
        "  cpu                       1  per vCPU-hour at score 1000 (runs are billed for wall time x score / 1000)"
    );
    println!(
        "  wasm                      1  per hour of reference time its fuel stands for ({} Gfuel)",
        pumasi_proto::perf::REFERENCE_FUEL_PER_SEC * 3600 / 1_000_000_000
    );
    println!(
        "  memory-gib-hour {:>10}  per GiB-hour of reserved memory",
        format_rate(r.memory_gib_hour)
    );
    println!(
        "  storage-gib-month {:>8}  per GiB-month stored (cluster file store)",
        format_rate(r.storage_gib_month)
    );
    println!(
        "  egress-gib {:>15}  per GiB sent out of the cluster",
        format_rate(r.egress_gib)
    );
    println!(
        "  e.g. 1 vCPU + 2 GiB for an hour = {} CU, 1 vCPU + 4 GiB = {} CU",
        format_rate(r.per_hour(1, 2)),
        format_rate(r.per_hour(1, 4))
    );
    println!(
        "  gpu-tflops-hour {:>10}  per hour per FP32 TFLOPS of the GPUs a run holds",
        format_rate(r.gpu_tflops_hour)
    );
    println!(
        "  gpu-vram-gib-hour {:>8}  per hour per GiB of their memory",
        format_rate(r.gpu_vram_gib_hour)
    );
    println!(
        "  e.g. a 12 GiB, 12.7 TFLOPS GPU for an hour = {} CU on top of CPU and memory",
        format_rate(r.gpu_per_hour(12_700, 12 * 1024))
    );
    println!("ledger");
    match p.ledger {
        LedgerMode::Prepaid => println!(
            "  prepaid: the owner grants CU (`pumasi cu add`); a job's account needs a positive balance"
        ),
        LedgerMode::Mutual { credit_limit } => println!(
            "  mutual (품앗이): lending earns CU, using spends them; accounts may go down to -{} CU",
            format_cu(credit_limit)
        ),
    }
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

async fn bench(
    home: &Path,
    dir: Option<PathBuf>,
    quick: bool,
    json: bool,
    cpus: Option<u32>,
) -> Result<i32> {
    let dir = dir.unwrap_or_else(|| home.join("agent"));
    let config = if quick {
        BenchConfig::quick(dir)
    } else {
        BenchConfig::standard(dir)
    };
    let cpus = cpus.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1)
    });
    if !json {
        eprintln!("benchmarking (about {} s)...", if quick { 2 } else { 12 });
    }
    let p = tokio::task::spawn_blocking(move || -> Result<PerfProfile> {
        let kernel = pumasi_agent::bench::CpuKernel::new()?;
        pumasi_agent::bench::measure(&config, &kernel, &iroh::SecretKey::generate(), cpus)
    })
    .await??;
    if json {
        println!("{}", serde_json::to_string_pretty(&p)?);
    } else {
        print_profile(&p);
    }
    Ok(0)
}

fn print_profile(p: &PerfProfile) {
    let c = &p.cpu;
    println!("cpu      {} ({} threads)", c.model, c.threads);
    println!(
        "         score {} single-thread, {} all threads, {} per vCPU (reference vCPU = 1000)",
        c.single,
        c.multi,
        c.per_vcpu()
    );
    println!(
        "memory   {:.1} GiB, copy {} MB/s",
        p.memory.total_bytes as f64 / (1u64 << 30) as f64,
        p.memory.copy_mb_s
    );
    match &p.disk {
        Some(d) => {
            let os = d
                .os_class
                .map(|c| format!(" (the OS says {c})"))
                .unwrap_or_default();
            println!("disk     {}{}", d.class, os);
            println!(
                "         sequential {} MB/s read, {} MB/s write; 4 KiB random read {} us median, {} us p99, {} IOPS (8 in flight)",
                d.seq_read_mb_s,
                d.seq_write_mb_s,
                d.rand_read_p50_us,
                d.rand_read_p99_us,
                d.rand_read_iops
            );
            if d.total_bytes > 0 {
                println!(
                    "         {:.0} GiB free of {:.0} GiB",
                    d.free_bytes as f64 / (1u64 << 30) as f64,
                    d.total_bytes as f64 / (1u64 << 30) as f64
                );
            }
            if let Some(s) = &d.smart {
                let mut parts = vec![s.model.clone()];
                if let Some(ok) = s.passed {
                    parts.push(if ok {
                        "health ok".into()
                    } else {
                        "HEALTH FAILING".into()
                    });
                }
                if let Some(u) = s.percent_used {
                    parts.push(format!("{u}% of endurance used"));
                }
                if let Some(h) = s.power_on_hours {
                    parts.push(format!("{h} h powered on"));
                }
                if let Some(e) = s.media_errors {
                    parts.push(format!("{e} media errors"));
                }
                if let Some(t) = s.temperature_c {
                    parts.push(format!("{t} C"));
                }
                println!("smart    {}", parts.join(", "));
            }
        }
        None => println!("disk     not measured"),
    }
    for n in &p.notes {
        println!("note     {n}");
    }
}

fn print_node(n: &NodeView) {
    println!("node {} \"{}\"", n.id, n.info.name);
    let state = if n.suspended {
        "suspended"
    } else if n.online && n.paused.is_some() {
        "paused"
    } else if n.online {
        "online"
    } else {
        "offline"
    };
    println!(
        "  {state}, {} {}/{}, agent {}, {} slots, {}",
        n.role,
        n.info.os,
        n.info.arch,
        n.info.agent_version,
        n.info.slots,
        executor_list(&n.executors)
    );
    if let Some(reason) = &n.paused {
        println!("  paused: {reason}");
    }
    if n.config.cordoned {
        println!("  cordoned: takes no new tasks (`pumasi uncordon` to undo)");
    }
    let labels: Vec<String> = n
        .labels
        .iter()
        .map(|(k, v)| format!("{k}={}", v.join("|")))
        .collect();
    println!("  labels {}", labels.join(","));
    if !n.config.taints.is_empty() {
        let taints: Vec<String> = n.config.taints.iter().map(|t| t.to_string()).collect();
        println!("  taints {}", taints.join(","));
    }
    if let Some(rtt) = n.rtt_us {
        println!(
            "  latency {:.1} ms round trip to the control plane{}",
            rtt as f64 / 1000.0,
            if n.relayed {
                " (through a relay)"
            } else {
                " (direct)"
            }
        );
    }
    println!("  billed cpu score {}", n.cpu_score);
    if let Some(note) = &n.perf_note {
        println!("    {note}");
    }
    if n.role == Role::Provider {
        println!("  checks passed {}, strikes {}", n.checks_passed, n.strikes);
    }
    if n.executors.contains(&Executor::Microvm) {
        println!(
            "  microVM network {}",
            if n.microvm_network {
                "available (jobs with network = true)"
            } else {
                "not available"
            }
        );
    }
    if n.gpus.is_empty() {
        println!("  no GPUs");
    }
    for g in &n.gpus {
        println!("  {}", gpu_line(g));
        if let Some(note) = &g.note {
            println!("    {note}");
        }
        let i = &g.info;
        let mut facts = Vec::new();
        if !i.pci.is_empty() {
            facts.push(format!("pci {}", i.pci));
        }
        if !i.driver.is_empty() {
            facts.push(format!("driver {}", i.driver));
        }
        if !i.compute.is_empty() {
            facts.push(format!("compute {}", i.compute));
        }
        if i.mem_bw_gb_s > 0 {
            facts.push(format!("{} GB/s", i.mem_bw_gb_s));
        }
        if i.hash_rate > 0 {
            facts.push(format!("{:.1} Ghash/s", i.hash_rate as f64 / 1e9));
        }
        if i.display {
            facts.push("drives a display".into());
        }
        if !facts.is_empty() {
            println!("    {}", facts.join(", "));
        }
    }
    for w in &n.webgpu {
        println!("  {}", webgpu_line(w));
        if let Some(note) = &w.note {
            println!("    {note}");
        }
        let a = &w.adapter;
        let mut facts = vec![format!("vendor {}", a.vendor)];
        if !a.driver.is_empty() {
            facts.push(format!("driver {}", a.driver));
        }
        facts.push(format!(
            "buffers up to {} MiB (bindings {} MiB)",
            a.max_buffer_mib, a.max_binding_mib
        ));
        println!("    {}", facts.join(", "));
    }
    match &n.perf {
        Some(p) => {
            println!("benchmark of {} UTC", utc(p.measured_unix));
            print_profile(p);
        }
        None => println!("no benchmark profile"),
    }
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

fn print_receipts(receipts: &[Receipt]) {
    println!(
        "{:<13} {:<10} {:<12} {:<12} {:>14} {:>9} {:>10} {:<10} {:>12} {:>10}",
        "RECEIPT",
        "RUN",
        "NODE",
        "OUTCOME",
        "FUEL",
        "WALL ms",
        "PEAK MiB",
        "PROVIDER",
        "CU",
        "GPU CU"
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
            "{:<13} {:<10} {:<12} {:<12} {:>14} {:>9} {:>10} {:<10} {:>12} {:>10}",
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
            format_cu(r.billing.amount),
            r.gpu
                .as_ref()
                .map(|g| format_rate(g.exact_ucu))
                .unwrap_or_else(|| "-".into())
        );
    }
    if receipts.is_empty() {
        println!("(no receipts yet)");
    } else {
        println!(
            "(RUN: task#attempt, c = check run; GPU CU: the GPU part of the exact price; export with --export to verify offline)"
        );
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
