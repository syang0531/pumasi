//! `pumasi`: one binary for the control plane, the node agent and the
//! admin CLI.

mod follow;
mod gpu_cmd;
mod ha_cmd;
mod manage;
mod monitor;
mod security;
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
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

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
    /// Create a ticket that lets another machine join the cluster; `token
    /// ls` lists the ones still usable, `token revoke` cancels one.
    #[command(args_conflicts_with_subcommands = true)]
    Token {
        #[command(subcommand)]
        cmd: Option<manage::TokenCmd>,
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
    /// Access keys for the S3-compatible gateway that `pumasi ui` serves
    /// at `/s3` (datasets are buckets).
    #[command(subcommand)]
    S3(S3Cmd),
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
    /// Show one node (NODE: name or id prefix): its benchmark profile,
    /// billed CPU score, latency and checks. With --rebenchmark, ask it to
    /// measure itself again. `pumasi node rename` changes its name.
    #[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
    Node {
        #[command(subcommand)]
        cmd: Option<NodeCmd>,
        #[arg(required = true, value_name = "NODE")]
        id: Option<String>,
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
    /// NODE: name or id prefix.
    RemoveNode {
        #[arg(value_name = "NODE")]
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Save an admin ticket (or an access ticket from `pumasi access
    /// create`) for the other commands. With --sso or --ldap, sign in with
    /// the organization's account instead (TICKET is then the sign-in
    /// ticket from `pumasi sso ticket`) and save the session's ticket.
    Login {
        ticket: String,
        /// Sign in with the cluster's OpenID Connect provider (opens a
        /// code to enter in a browser, on any device).
        #[arg(long, conflicts_with = "ldap")]
        sso: bool,
        /// Sign in with this LDAP user (the password is asked for, or
        /// read from PUMASI_PASSWORD).
        #[arg(long, value_name = "USER")]
        ldap: Option<String>,
    },
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
        policy: Box<manage::PolicyArgs>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Access tickets with a role: admin, submitter (one account) or
    /// viewer.
    #[command(subcommand)]
    Access(manage::AccessCmd),
    /// The audit log: who changed what, as a hash chain the cluster key
    /// signs. Admins only.
    #[command(subcommand)]
    Audit(security::AuditCmd),
    /// Single sign-on: an OpenID Connect provider or LDAP directory, and
    /// rules that map its groups to roles.
    #[command(subcommand)]
    Sso(security::SsoCmd),
    /// The cluster's own admin ticket.
    #[command(subcommand)]
    Admin(security::AdminCmd),
    /// Show which ticket this machine uses and what it may do.
    Whoami {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Jobs submitted on a cron schedule.
    #[command(subcommand)]
    Schedule(manage::ScheduleCmd),
    /// The confidential tier: TEE nodes and the launch measurements their
    /// guests attested with, which an admin trusts before any key is
    /// released.
    #[command(subcommand)]
    Confidential(manage::ConfCmd),
    /// Let a provider's node run `tier = "trusted"` jobs (your own
    /// machines always do).
    Trust {
        node: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Stop a provider's node from running `tier = "trusted"` jobs.
    Untrust {
        node: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
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
    /// The network between nodes: round trips and bandwidth the agents
    /// measured to each other (1.6 agents, one pair now and then).
    Net {
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
    /// Where the copies of each dataset version are: control planes and
    /// nodes labelled `storage` (all datasets, or every version of NAME),
    /// and the storage nodes with their quotas.
    Replicas {
        name: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// How many copies to keep of dataset NAME (control planes included),
    /// or `default` to use the cluster's; with --default, the cluster's
    /// number for datasets without their own (2 unless set).
    Copies {
        /// NAME COPIES, or COPIES with --default.
        #[arg(required = true, num_args = 1..=2)]
        args: Vec<String>,
        #[arg(long)]
        default: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Read every stored collection back and check it against its hashes
    /// now: the control plane's store and every storage node's replicas,
    /// or one node's. Results appear in `pumasi events` and
    /// `pumasi store replicas`.
    Scrub {
        node: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Subcommand)]
enum S3Cmd {
    /// Make an S3 access key that acts as this ticket (its role and
    /// account), for `aws s3`, rclone or boto3 against `pumasi ui`'s `/s3`.
    /// The secret is shown once.
    CreateKey {
        /// A note to tell keys apart (`laptop rclone`).
        #[arg(long, default_value = "")]
        label: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// List S3 keys (this ticket's, or every one with an admin ticket).
    Keys {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Revoke an S3 key.
    RevokeKey {
        id: String,
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
        /// (QEMU or vz; Firecracker needs QEMU next to it).
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
            confidential: Default::default(),
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
enum NodeCmd {
    /// Give a node a display name, shown in `pumasi status`, the dashboard
    /// and events, and usable wherever a command takes NODE. It replaces
    /// the name the node's agent reports (`--name`, by default the host
    /// name) until --reset.
    Rename {
        /// The node: its current name or an id prefix.
        node: String,
        /// The new name (up to 64 characters, unique in the cluster).
        #[arg(required_unless_present = "reset", conflicts_with = "reset")]
        name: Option<String>,
        /// Go back to the name the node's agent reports.
        #[arg(long)]
        reset: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Wake a sleeping node over the LAN: every machine of the cluster on
    /// its LAN sends its network card the magic packet. The node must
    /// have opted in (`--wake-on-lan`).
    Wake {
        /// The node: its name or an id prefix.
        node: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Show or change when a node takes tasks (1.7): its agent writes the
    /// change to its service's options file (agent.toml) and restarts with
    /// it once its running tasks end, taking no new ones meanwhile. Only
    /// for agents of 1.7 or later run as a service. With no change, shows
    /// the settings.
    Set {
        /// The node: its name or an id prefix.
        node: String,
        /// Tasks at once.
        #[arg(long, conflicts_with = "default_slots")]
        slots: Option<u32>,
        /// Go back to the default number of slots.
        #[arg(long)]
        default_slots: bool,
        /// Offer only this share of the CPUs (1-100).
        #[arg(long, conflicts_with = "all_cpus")]
        max_cpu_percent: Option<u32>,
        /// Offer all the CPUs.
        #[arg(long)]
        all_cpus: bool,
        /// Take tasks only on AC power.
        #[arg(long, conflicts_with = "any_power")]
        only_on_ac: bool,
        /// Take tasks on battery too.
        #[arg(long)]
        any_power: bool,
        /// Take tasks only after this many idle minutes.
        #[arg(long, value_name = "MINUTES", conflicts_with = "no_idle")]
        idle_minutes: Option<u64>,
        /// Take tasks while someone uses the machine too.
        #[arg(long)]
        no_idle: bool,
        /// Take tasks only at these local times, e.g. `22:00-07:00`.
        #[arg(long, value_name = "HH:MM-HH:MM[,...]", conflicts_with = "any_time")]
        hours: Option<String>,
        /// Take tasks at any time of day.
        #[arg(long)]
        any_time: bool,
        #[command(flatten)]
        admin: AdminArgs,
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
        #[command(flatten)]
        release: ReleaseArgs,
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
        #[command(flatten)]
        release: ReleaseArgs,
    },
    /// Make a control plane from a `pumasi backup` file (into an empty
    /// cluster directory).
    Restore { file: PathBuf },
}

#[derive(Args, Clone, Default)]
struct ReleaseArgs {
    /// Where `pumasi upgrade latest` finds published releases: a GitHub
    /// releases URL (`<url>/latest` must redirect to the newest tag).
    #[arg(long, value_name = "URL", default_value = pumasi_proto::release::DEFAULT_RELEASE_SOURCE)]
    release_source: String,
    /// Keep running the old code when a newer pumasi is installed over
    /// this binary (by default the control plane restarts into it once the
    /// rollout has settled).
    #[arg(long)]
    no_follow_binary: bool,
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
    /// How long finished jobs are kept (counted from submission), e.g.
    /// 90d; 0 keeps them forever. Older ones are deleted with their logs
    /// and small outputs; receipts and the audit log stay.
    #[arg(long, default_value = "30d")]
    keep_jobs: String,
    /// Wake a sleeping node (one started with --wake-on-lan) for work
    /// that has waited this long for a busy node, e.g. 10m; `never` wakes
    /// nodes only for work that no online node can run at all.
    #[arg(long, default_value = "10m", value_name = "DURATION|never")]
    wake_after: String,
    /// Ask one agent to measure the network to another this often (a few
    /// round trips and 1 MiB), for `pumasi net` and to pick the closest
    /// file store cache; `off` never asks.
    #[arg(long, default_value = "20s", value_name = "DURATION|off")]
    net_probe_every: String,
    /// How long a storage node may stay offline before its dataset
    /// replicas are copied to other storage nodes, e.g. 6h.
    #[arg(long, default_value = "6h", value_name = "DURATION")]
    replica_repair_after: String,
    /// How often every store is read back and checked against its hashes
    /// (the control plane's and each storage node's), e.g. 7d.
    #[arg(long, default_value = "7d", value_name = "DURATION")]
    scrub_every: String,
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
pub(crate) enum GraphicsArg {
    Auto,
    Yes,
    No,
}

impl GraphicsArg {
    /// `None` for `auto`.
    pub(crate) fn forced(self) -> Option<bool> {
        match self {
            GraphicsArg::Auto => None,
            GraphicsArg::Yes => Some(true),
            GraphicsArg::No => Some(false),
        }
    }
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
    /// What happens to the tasks this machine is running when it pauses
    /// (on battery, in use, outside its hours): `finish` lets them finish,
    /// `requeue` stops them and runs them on another machine.
    #[arg(long, value_name = "finish|requeue", default_value = "finish")]
    on_pause: pumasi_proto::power::OnPause,
    /// Let the machine sleep on its usual schedule even while it runs
    /// tasks. By default the agent keeps it awake (not its display) from
    /// the first running task until the last one ends.
    #[arg(long)]
    allow_sleep: bool,
    /// Let the cluster wake this machine over the LAN when work waits for
    /// it (and with `pumasi node wake`): another machine of the cluster on
    /// the same LAN sends its network card the magic packet. Wake-on-LAN
    /// must also be on in the firmware and the OS (see the README).
    #[arg(long)]
    wake_on_lan: bool,
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
    /// Whether Docker GPU tasks that ask for graphics get the WSL 2
    /// driver libraries (`/usr/lib/wsl`, read-only), which Docker Desktop
    /// leaves out of GPU containers: `auto` does on Windows. The only
    /// host folder Pumasi puts in a container.
    #[arg(long, value_enum, default_value = "auto")]
    docker_wsl_libs: GraphicsArg,
    /// Also run image jobs in microVMs. Each task gets its own VM and
    /// kernel, so this is safe on machines you share with a cluster.
    /// Needs `pumasi vm setup` first.
    #[arg(long)]
    allow_microvm: bool,
    /// With --allow-microvm, also run `tier = "confidential"` jobs in
    /// SEV-SNP or TDX microVMs, whose memory this machine cannot read.
    /// Needs a host with SEV-SNP or TDX on, and the confidential guest's
    /// firmware and kernel in the VM directory's `tee/` (see the docs).
    #[arg(long, requires = "allow_microvm")]
    allow_confidential: bool,
    /// With --allow-microvm, run confidential jobs in ordinary microVMs
    /// that pretend to attest: for tests only, nothing is protected. The
    /// control plane must be started with --insecure-test-tee too.
    #[arg(long, requires = "allow_microvm", hide = true)]
    insecure_test_tee: bool,
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
    /// Your own relay server (an iroh-relay URL such as
    /// https://relay.example.com) instead of n0's public ones; repeat for
    /// more. On the control plane, tickets carry it, so agents and clients
    /// use it without the option.
    #[arg(long = "relay", value_name = "URL")]
    relays: Vec<pumasi_proto::net::RelayUrl>,
}

impl NetArgs {
    /// `--relay`, else the cluster's own relays named in its addresses.
    fn relays_for(&self, cluster: &[iroh::EndpointAddr]) -> Vec<pumasi_proto::net::RelayUrl> {
        if self.relays.is_empty() {
            pumasi_proto::net::custom_relays(cluster)
        } else {
            self.relays.clone()
        }
    }
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
    // `pumasi idle-helper`: started by the Windows agent service in each
    // signed-in session, to learn when someone is using the machine.
    if std::env::args_os()
        .nth(1)
        .is_some_and(|a| a == pumasi_agent::availability::IDLE_HELPER)
    {
        let code = match pumasi_agent::availability::idle_helper_main() {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("error: {e:#}");
                1
            }
        };
        std::process::exit(code);
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
    if let Cmd::Service(service::ServiceCmd::Run { role, args }) = &cli.cmd {
        let home = match home_dir(&cli.home) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("error: {e:#}");
                std::process::exit(1);
            }
        };
        let log = role.log_file(&home);
        let _ = std::fs::create_dir_all(home.join("logs"));
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
        {
            Ok(file) => tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(file))
                .finish()
                .with(quiet_shutdown())
                .init(),
            Err(_) => tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_ansi(false)
                .finish()
                .with(quiet_shutdown())
                .init(),
        }
        std::process::exit(service::windows::run(*role, home, args.clone()));
    }

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(use_color())
        .with_writer(std::io::stderr)
        .finish()
        .with(quiet_shutdown())
        .init();

    // The command future is large: keep it on the heap, not the stack
    // (the main thread has only 1 MiB on Windows; see build.rs).
    let home = home_dir(&cli.home).ok();
    let runtime = tokio::runtime::Runtime::new().expect("start the async runtime");
    let code = runtime.block_on(Box::pin(run(cli)));
    match code {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e:#}");
            if let Some(hint) = home.and_then(|h| permission_hint(&e, &h)) {
                eprintln!("note: {hint}");
            }
            std::process::exit(1);
        }
    }
}

/// Colored logs only on a terminal, and not with `NO_COLOR` set: a log
/// redirected to a file gets no escape codes.
fn use_color() -> bool {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())
}

/// Drops iroh's `relay_recv_channel closed` error, which it logs while an
/// endpoint with a relay closes normally (the relay actor stops before
/// the socket does), so a command that worked prints no ERROR line. It is
/// the only error event of that module, and it means nothing else.
fn quiet_shutdown() -> tracing_subscriber::filter::FilterFn<impl Fn(&tracing::Metadata<'_>) -> bool>
{
    tracing_subscriber::filter::filter_fn(|m| {
        !(*m.level() == tracing::Level::ERROR && m.target() == "iroh::socket::transports::relay")
    })
}

fn home_dir(cli_home: &Option<PathBuf>) -> Result<PathBuf> {
    if let Some(h) = cli_home {
        return Ok(h.clone());
    }
    let base = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot find home directory; set PUMASI_HOME")?;
    let home = PathBuf::from(base).join(".pumasi");
    Ok(sudo_home(home))
}

/// Under `sudo`, macOS (and some Linux setups) keep the user's `HOME`, so
/// root would make `~/.pumasi` and own it, and the user's own commands
/// (`pumasi vm setup`, `pumasi login`) then fail on it. Use root's home
/// there instead, as Linux's sudo does, unless root already owns the
/// user's `~/.pumasi` (an agent service installed that way before keeps
/// its state).
#[cfg(unix)]
fn sudo_home(home: PathBuf) -> PathBuf {
    use std::os::unix::fs::MetadataExt;
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 || std::env::var_os("SUDO_USER").is_none() {
        return home;
    }
    // SAFETY: getpwuid returns static storage or null, read at once.
    let root = unsafe {
        let pw = libc::getpwuid(0);
        if pw.is_null() || (*pw).pw_dir.is_null() {
            return home;
        }
        PathBuf::from(
            std::ffi::CStr::from_ptr((*pw).pw_dir)
                .to_string_lossy()
                .into_owned(),
        )
    };
    let root_home = root.join(".pumasi");
    if home == root_home || std::fs::metadata(&home).is_ok_and(|m| m.uid() == 0) {
        return home;
    }
    root_home
}

#[cfg(not(unix))]
fn sudo_home(home: PathBuf) -> PathBuf {
    home
}

/// A hint for an error that comes from a home this user cannot write,
/// such as one an agent service installed with `sudo` made.
fn permission_hint(e: &anyhow::Error, home: &Path) -> Option<String> {
    let denied = e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
    });
    if !denied {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        let me = unsafe { libc::geteuid() };
        if me != 0 && std::fs::metadata(home).is_ok_and(|m| m.uid() != me) {
            return Some(format!(
                "{} belongs to another user (root, when an agent service was installed with sudo): run this command with sudo to work on that agent, or give your own --home",
                home.display()
            ));
        }
    }
    let _ = home;
    None
}

/// Picks local mode for tickets that carry only IP addresses: public if any
/// address of the cluster has a relay.
fn net_for_all(addrs: &[iroh::EndpointAddr], explicit: Option<NetMode>) -> NetMode {
    explicit.unwrap_or_else(|| {
        if addrs.iter().any(|a| a.relay_urls().next().is_some()) {
            NetMode::Public
        } else {
            NetMode::Local
        }
    })
}

async fn run(cli: Cli) -> Result<i32> {
    let home = home_dir(&cli.home)?;
    // An installed service (systemd, launchd): the role's command with
    // the options in its options file. The Windows service manager's
    // entry point is taken in `main`.
    #[cfg(not(windows))]
    if let Cmd::Service(service::ServiceCmd::Run { role, args }) = &cli.cmd {
        note_options_file(*role, &home, args);
        let options = service::run_options(*role, &home, args.clone())?;
        let cmd = parse_service_args(*role, &options)?;
        return Box::pin(run(Cli {
            home: cli.home,
            cmd,
        }))
        .await;
    }
    match cli.cmd {
        Cmd::Cluster(ClusterCmd::Start {
            name,
            net,
            no_token,
            storage_class,
            metrics,
            release,
        }) => {
            cluster_start(
                &home,
                name,
                net,
                no_token,
                storage_class,
                metrics,
                release,
                None,
                shutdown_signal(),
                false,
            )
            .await
        }
        Cmd::Cluster(ClusterCmd::Replica {
            ticket,
            net,
            storage_class,
            metrics,
            release,
        }) => {
            let ticket: pumasi_proto::ticket::ReplicaTicket = ticket.parse()?;
            cluster_start(
                &home,
                String::new(),
                net,
                true,
                storage_class,
                metrics,
                release,
                Some(ticket),
                shutdown_signal(),
                false,
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
        Cmd::Net { output, admin } => monitor::net(&home, &admin, output).await,
        Cmd::Ui(args) => ui::run(&home, args).await,
        Cmd::Alert(cmd) => monitor::alert(&home, cmd).await,
        Cmd::Webhook(cmd) => monitor::webhook(&home, cmd).await,
        Cmd::Store(cmd) => store(&home, cmd).await,
        Cmd::Secret(cmd) => secret(&home, cmd).await,
        Cmd::S3(cmd) => s3_cmd(&home, cmd).await,
        Cmd::Agent(cmd) => agent(&home, cmd, ctrl_c(), false).await,
        Cmd::Service(cmd) => service_cmd(&home, cmd),
        Cmd::Upgrade(cmd) => upgrade::run(&home, cmd).await,
        Cmd::Token { cmd: Some(cmd), .. } => manage::token(&home, cmd).await,
        Cmd::Token {
            cmd: None,
            ttl,
            uses,
            admin,
        } => {
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
            cmd:
                Some(NodeCmd::Rename {
                    node,
                    name,
                    reset,
                    admin,
                }),
            ..
        } => {
            let client = connect_admin(&home, &admin).await?;
            let name = if reset { None } else { name };
            client
                .call(AdminOp::RenameNode {
                    node: node.clone(),
                    name: name.clone(),
                })
                .await?;
            client.close().await;
            match name {
                Some(n) => eprintln!("renamed {node} to {n}"),
                None => eprintln!("{node} goes by the name its agent reports again"),
            }
            Ok(0)
        }
        Cmd::Node {
            cmd: Some(NodeCmd::Wake { node, admin }),
            ..
        } => {
            let client = connect_admin(&home, &admin).await?;
            let res = client.call(AdminOp::WakeNode { node }).await;
            client.close().await;
            let AdminResponse::Applied(lines) = res? else {
                bail!("unexpected response");
            };
            for l in lines {
                println!("{l}");
            }
            eprintln!(
                "it takes a machine up to a minute to wake and reconnect; `pumasi status` shows it"
            );
            Ok(0)
        }
        Cmd::Node {
            cmd:
                Some(NodeCmd::Set {
                    node,
                    slots,
                    default_slots,
                    max_cpu_percent,
                    all_cpus,
                    only_on_ac,
                    any_power,
                    idle_minutes,
                    no_idle,
                    hours,
                    any_time,
                    admin,
                }),
            ..
        } => {
            let client = connect_admin(&home, &admin).await?;
            let res = async {
                let AdminResponse::NodeSettings(view) = client
                    .call(AdminOp::NodeSettings { node: node.clone() })
                    .await?
                else {
                    bail!("unexpected response");
                };
                let Some(report) = view.report.clone() else {
                    bail!(
                        "{} has not reported its settings: it is offline, or its agent is older than 1.7",
                        view.name
                    );
                };
                let mut s = report.settings.clone();
                let before = s.clone();
                if let Some(v) = slots {
                    s.slots = Some(v);
                }
                if default_slots {
                    s.slots = None;
                }
                if let Some(v) = max_cpu_percent {
                    s.max_cpu_percent = Some(v);
                }
                if all_cpus {
                    s.max_cpu_percent = None;
                }
                if only_on_ac {
                    s.only_on_ac = true;
                }
                if any_power {
                    s.only_on_ac = false;
                }
                if let Some(v) = idle_minutes {
                    s.idle_minutes = Some(v);
                }
                if no_idle {
                    s.idle_minutes = None;
                }
                if let Some(v) = hours {
                    s.hours = v;
                }
                if any_time {
                    s.hours.clear();
                }
                if s == before {
                    println!("{}: {}", view.name, report.settings.describe());
                    match &report.file {
                        Some(f) => println!("kept in {f} on that machine"),
                        None => println!("(started by hand: it cannot change them from here)"),
                    }
                    if report.restarting {
                        println!("restarts with them once its running tasks end");
                    }
                    return Ok(());
                }
                let AdminResponse::NodeSettings(after) = client
                    .call(AdminOp::SetNodeSettings {
                        node,
                        settings: Box::new(s.clone()),
                    })
                    .await?
                else {
                    bail!("unexpected response");
                };
                println!("{}: {}", after.name, s.describe());
                if after.report.as_ref().is_some_and(|r| r.restarting) {
                    println!("written; the agent restarts with them once its running tasks end");
                }
                Ok(())
            }
            .await;
            client.close().await;
            res.map(|_| 0)
        }
        Cmd::Node {
            cmd: None,
            id,
            rebenchmark,
            admin,
        } => {
            let id = id.unwrap_or_default();
            let client = connect_admin(&home, &admin).await?;
            let AdminResponse::Status(s) = client.call(AdminOp::Status).await? else {
                bail!("unexpected response");
            };
            let by_name: Vec<&NodeView> = s
                .nodes
                .iter()
                .filter(|n| n.info.name == id.trim())
                .collect();
            let matches: Vec<&NodeView> = if by_name.len() == 1 {
                by_name
            } else {
                s.nodes
                    .iter()
                    .filter(|n| n.id.starts_with(id.trim()))
                    .collect()
            };
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
            let power = node_power(&client).await;
            client.close().await;
            print_node(n, power.iter().find(|p| p.id == n.id));
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
            let ran = task_executors(&client, &view).await;
            print_job(&view, &task_details(&client, id).await, &ran, output);
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
            let power = node_power(&client).await;
            client.close().await;
            print_status(&status, &power);
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
            let ran = task_executors(&client, &view).await;
            print_job(&view, &task_details(&client, id).await, &ran, output);
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
        Cmd::Login { ticket, sso, ldap } if sso || ldap.is_some() => {
            security::login(&home, &ticket, ldap).await
        }
        Cmd::Login { ticket, .. } => {
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
                *policy,
            )
            .await
        }
        Cmd::Access(cmd) => manage::access(&home, cmd).await,
        Cmd::Audit(cmd) => security::audit(&home, cmd).await,
        Cmd::Sso(cmd) => security::sso(&home, cmd).await,
        Cmd::Admin(cmd) => security::admin(&home, cmd).await,
        Cmd::Whoami { admin } => manage::whoami(&home, &admin).await,
        Cmd::Schedule(cmd) => manage::schedule(&home, cmd).await,
        Cmd::Confidential(cmd) => manage::confidential(&home, cmd).await,
        Cmd::Trust { node, admin } => manage::trust_node(&home, &admin, &node, true).await,
        Cmd::Untrust { node, admin } => manage::trust_node(&home, &admin, &node, false).await,
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

#[allow(clippy::too_many_arguments)]
async fn cluster_start(
    home: &Path,
    name: String,
    net: NetArgs,
    no_token: bool,
    storage_class: Option<StorageClassArg>,
    metrics: MetricsArgs,
    release: ReleaseArgs,
    replica: Option<pumasi_proto::ticket::ReplicaTicket>,
    shutdown: impl std::future::Future<Output = ()>,
    in_service: bool,
) -> Result<i32> {
    let mut config = ClusterConfig::new(home.join("cluster"));
    config.name = name;
    config.release_source = release.release_source.clone();
    let follow = if release.no_follow_binary {
        None
    } else {
        follow::Follow::new()
    };
    config.metrics_addr = metrics.metrics_addr;
    config.metrics_token = metrics.metrics_token.filter(|t| !t.is_empty());
    config.metrics_retention =
        Duration::from_millis(parse_duration_ms(&metrics.metrics_retention)?);
    config.keep_jobs = Duration::from_millis(parse_duration_ms(&metrics.keep_jobs)?);
    config.wake_after = match metrics.wake_after.trim() {
        "never" => None,
        d => Some(Duration::from_millis(
            parse_duration_ms(d).context("--wake-after")?,
        )),
    };
    config.probe_every = if pumasi_proto::monitor::is_off(&metrics.net_probe_every) {
        None
    } else {
        Some(Duration::from_millis(
            parse_duration_ms(&metrics.net_probe_every).context("--net-probe-every")?,
        ))
    };
    config.replica_repair_after = Duration::from_millis(
        parse_duration_ms(&metrics.replica_repair_after).context("--replica-repair-after")?,
    );
    config.scrub_every =
        Duration::from_millis(parse_duration_ms(&metrics.scrub_every).context("--scrub-every")?)
            .max(Duration::from_secs(60));
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
    config.relays = match &replica {
        Some(t) => net.relays_for(&t.candidates()),
        None => net.relays.clone(),
    };
    if config.net == NetMode::Local && !config.relays.is_empty() {
        bail!("--relay needs public network mode; drop --net local");
    }
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
        return stop_cluster(home, cluster, shutdown, follow, in_service).await;
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
        eprintln!(
            "add a machine (ticket {}, valid 24h for any number of machines; `pumasi token revoke {}` cancels it):",
            pumasi_proto::msg::join_token_id(&ticket.secret),
            pumasi_proto::msg::join_token_id(&ticket.secret),
        );
        eprintln!("  pumasi agent join {ticket}");
    }
    eprintln!();
    stop_cluster(home, cluster, shutdown, follow, in_service).await
}

/// Runs the control plane until `shutdown`, or until a newer binary was
/// installed over this one and the rollout settled: then restarts into it.
/// With no member agent on this machine to install that binary, the
/// control plane installs the rollout's binary for its platform itself.
async fn stop_cluster(
    home: &Path,
    cluster: Cluster,
    shutdown: impl std::future::Future<Output = ()>,
    follow: Option<follow::Follow>,
    in_service: bool,
) -> Result<i32> {
    let own_exe = follow.as_ref().map(|f| f.exe().to_path_buf());
    if let Some(exe) = &own_exe {
        pumasi_agent::upgrade::remove_old_binaries(exe);
    }
    let upgrade_own = async {
        match own_exe {
            Some(exe) => upgrade_own_binary(&cluster, home, exe).await,
            None => std::future::pending().await,
        }
    };
    let stop = tokio::select! {
        stop = follow::until_stop(shutdown, follow, || cluster.rollout_settled()) => stop,
        never = upgrade_own => match never {},
    };
    match stop {
        follow::Stop::Shutdown => {
            eprintln!("shutting down");
            cluster.shutdown().await?;
            Ok(0)
        }
        follow::Stop::Upgrade { version, exe } => {
            eprintln!("restarting the control plane into pumasi {version}");
            cluster.shutdown().await?;
            follow::restart(in_service, &exe)
        }
    }
}

/// Installs the current rollout's binary for this platform over the
/// control plane's own when no agent on this machine is a member of the
/// cluster to do it ([`Cluster::own_upgrade`]). Following the binary then
/// restarts the control plane (once the rollout settles) and the
/// dashboard into it. Each binary is tried once per start.
async fn upgrade_own_binary(
    cluster: &Cluster,
    home: &Path,
    exe: PathBuf,
) -> std::convert::Infallible {
    let mut tried = std::collections::HashSet::new();
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let local_node = local_agent_id(home);
        let Some(up) = cluster.own_upgrade(local_node.as_deref()) else {
            continue;
        };
        if !tried.insert(up.file.sha256) {
            continue;
        }
        // Installed already (by hand, or by an agent that has since left)?
        let path = exe.clone();
        let on_disk = tokio::task::spawn_blocking(move || {
            std::fs::read(&path)
                .ok()
                .and_then(|b| pumasi_proto::release::read_build_info(&b))
        })
        .await
        .ok()
        .flatten();
        if on_disk.is_some_and(|b| !pumasi_proto::release::is_older(&b.version, &up.file.version)) {
            continue;
        }
        let version = &up.file.version;
        eprintln!(
            "no agent on this machine installs pumasi {version}; installing it over {}",
            exe.display()
        );
        match pumasi_agent::upgrade::replace_binary(&up.path, up.file.sha256, version, &exe, true)
            .await
        {
            Ok(()) => eprintln!("installed pumasi {version}"),
            Err(e) => eprintln!(
                "could not install pumasi {version} over {}: {e:#} (run the install script on this machine instead)",
                exe.display()
            ),
        }
    }
}

/// The node id of the agent that keeps its state in this home, if one
/// ever started here.
fn local_agent_id(home: &Path) -> Option<String> {
    let text = std::fs::read_to_string(home.join("agent").join("secret.key")).ok()?;
    let bytes: [u8; 32] = data_encoding::HEXLOWER
        .decode(text.trim().as_bytes())
        .ok()?
        .try_into()
        .ok()?;
    Some(iroh::SecretKey::from_bytes(&bytes).public().to_string())
}

async fn ctrl_c() {
    shutdown_signal().await
}

/// Ctrl-C, or on Unix also SIGTERM (what `systemctl stop` and launchd
/// send), so a service stops cleanly.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            return;
        }
    }
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
    // The cluster's addresses: from the ticket, or from the last join.
    let cluster_addrs = match &ticket {
        Some(t) => t.candidates(),
        None => std::fs::read_to_string(data_dir.join("membership.json"))
            .ok()
            .and_then(|s| serde_json::from_str::<pumasi_agent::Membership>(&s).ok())
            .map(|m| pumasi_proto::ticket::candidates(&m.cluster, &m.replicas))
            .unwrap_or_default(),
    };
    let net = if cluster_addrs.is_empty() {
        opts.net.net.unwrap_or_default()
    } else {
        net_for_all(&cluster_addrs, opts.net.net)
    };
    let relays = opts.net.relays_for(&cluster_addrs);
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
        relays,
        bind: opts.net.bind,
        docker: opts.allow_docker.then(|| opts.docker_bin.clone()),
        microvm: opts.allow_microvm.then(|| {
            let mut c = opts.vm.config(home);
            c.confidential = pumasi_agent::microvm::tee::TeeOptions {
                enabled: opts.allow_confidential,
                test: opts.insecure_test_tee,
            };
            c
        }),
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
        gpu_graphics: opts.gpu_graphics.forced(),
        docker_wsl_libs: opts.docker_wsl_libs.forced(),
        power: pumasi_agent::PowerOptions::system(
            opts.allow_sleep,
            opts.wake_on_lan,
            opts.on_pause,
        ),
    };
    let exe = config.upgrade.as_ref().map(|u| u.exe.clone());
    let mut agent = Agent::new(config).await?;
    if let Some(file) = AGENT_OPTIONS_FILE.get() {
        let home = home.to_path_buf();
        agent = agent.with_settings_hook(pumasi_agent::SettingsHook {
            file: file.clone(),
            write: std::sync::Arc::new(move |s| write_agent_settings(&home, s)),
        });
    }
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
        AgentExit::Reconfigured => eprintln!("restarting with the new settings"),
    }
    if in_service {
        return Ok(service::RESTART_EXIT_CODE);
    }
    let exe = exe.context("restart: unknown binary path")?;
    restart_self(&exe)
}

/// The running binary, if the agent can replace it.
pub(crate) fn own_exe() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let exe = exe.canonicalize().unwrap_or(exe);
    Some(if cfg!(windows) { plain_path(exe) } else { exe })
}

/// `C:\x` for Windows' verbatim `\\?\C:\x` (and `\\server\share` for
/// `\\?\UNC\server\share`), which `canonicalize` returns there and which
/// netsh firewall rules, among others, do not accept.
fn plain_path(p: PathBuf) -> PathBuf {
    let s = p.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = s.strip_prefix(r"\\?\")
        && rest.as_bytes().get(1) == Some(&b':')
    {
        PathBuf::from(rest)
    } else {
        p
    }
}

/// Runs the binary at `exe` again with this process's arguments.
pub(crate) fn restart_self(exe: &Path) -> Result<i32> {
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

/// What the Windows service runs: `agent start`, `cluster start` or `ui`
/// with the options in the role's options file (or, for a service
/// installed before 1.4, the given ones), until `shutdown` resolves.
#[cfg_attr(not(windows), allow(dead_code))]
async fn run_service(
    role: service::Role,
    home: PathBuf,
    args: Vec<String>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<i32> {
    note_options_file(role, &home, &args);
    let args = service::run_options(role, &home, args)?;
    match parse_service_args(role, &args)? {
        Cmd::Agent(cmd) => agent(&home, cmd, shutdown, true).await,
        Cmd::Cluster(ClusterCmd::Start {
            name,
            net,
            no_token,
            storage_class,
            metrics,
            release,
        }) => {
            cluster_start(
                &home,
                name,
                net,
                no_token,
                storage_class,
                metrics,
                release,
                None,
                shutdown,
                true,
            )
            .await
        }
        Cmd::Ui(args) => ui::run_until(&home, args, shutdown, true).await,
        _ => bail!("not {} options", role.as_str()),
    }
}

/// The options file of an agent run as a service in the 1.4 form (its
/// options in `agent.toml`, none in the definition): the dashboard may
/// change its settings there (1.7).
static AGENT_OPTIONS_FILE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

fn note_options_file(role: service::Role, home: &Path, inline: &[String]) {
    if role == service::Role::Agent && inline.is_empty() {
        let _ = AGENT_OPTIONS_FILE.set(role.options_file(home));
    }
}

/// Writes settings from the dashboard into the agent service's options
/// file, as `pumasi service set agent` would (without the restart, which
/// the agent does itself once idle).
fn write_agent_settings(home: &Path, settings: &pumasi_proto::ui::NodeSettings) -> Result<()> {
    let role = service::Role::Agent;
    let current = service::read_options(role, home)?.unwrap_or_default();
    let merged = service::merge_options(&service_command(role), &current, &settings.changes())?;
    parse_service_args(role, &merged)?;
    if merged != current {
        service::write_options(role, home, &merged)?;
    }
    Ok(())
}

/// Parses options meant for the command a service runs (`pumasi agent
/// start`, `pumasi cluster start` or `pumasi ui`).
fn parse_service_args(role: service::Role, args: &[String]) -> Result<Cmd> {
    let cmd: &[&str] = match role {
        service::Role::Agent => &["agent", "start"],
        service::Role::Cluster => &["cluster", "start"],
        service::Role::Ui => &["ui"],
    };
    let argv = std::iter::once("pumasi")
        .chain(cmd.iter().copied())
        .map(String::from)
        .chain(args.iter().cloned());
    if has_flag(args, "--home") {
        bail!("give --home before `service install`, not after `--`");
    }
    match Cli::try_parse_from(argv) {
        Ok(Cli { cmd, .. }) => Ok(cmd),
        Err(e) => bail!("invalid {} options {args:?}: {e}", role.as_str()),
    }
}

/// Parses options meant for `pumasi agent start`.
fn parse_agent_args(agent_args: &[String]) -> Result<AgentCmd> {
    match parse_service_args(service::Role::Agent, agent_args)? {
        Cmd::Agent(cmd) => Ok(cmd),
        _ => bail!("not agent options"),
    }
}

/// Whether `args` holds the option `--flag` (alone or as `--flag=...`).
fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter()
        .take_while(|a| *a != "--")
        .any(|a| a == flag || a.starts_with(&format!("{flag}=")))
}

/// The options a control plane or dashboard service needs on top of the
/// user's: no join ticket printed at every start into the log, no
/// browser, a fixed address, and the sign-in link in a file.
fn service_defaults(role: service::Role, home: &Path, mut args: Vec<String>) -> Vec<String> {
    match role {
        service::Role::Agent => {}
        service::Role::Cluster => {
            if !has_flag(&args, "--no-token") {
                args.push("--no-token".into());
            }
        }
        service::Role::Ui => {
            if !has_flag(&args, "--no-open") {
                args.push("--no-open".into());
            }
            if !has_flag(&args, "--listen") {
                args.push("--listen".into());
                args.push(format!("127.0.0.1:{}", ui::DEFAULT_PORT));
            }
            if !has_flag(&args, "--api-only") && !has_flag(&args, "--url-file") {
                args.push("--url-file".into());
                args.push(home.join(ui::URL_FILE).to_string_lossy().into_owned());
            }
        }
    }
    args
}

/// The command a service of `role` runs (`pumasi agent start`...), to
/// tell which of its options take a value.
fn service_command(role: service::Role) -> clap::Command {
    let mut root = <Cli as clap::CommandFactory>::command();
    root.build();
    let mut cmd = &root;
    for name in role.command() {
        cmd = cmd
            .find_subcommand(name)
            .expect("the commands services run exist");
    }
    cmd.clone()
}

/// Writes the service's options where the service reads them, then
/// installs and starts it. `inline_options` keeps them in the service
/// definition instead, as releases before 1.4 did.
fn install_service(
    mut spec: service::Spec,
    options: Vec<String>,
    inline_options: bool,
    scope: service::ScopeArgs,
    dry_run: bool,
) -> Result<i32> {
    let file = spec.role.options_file(&spec.home);
    if inline_options {
        if !dry_run {
            let _ = std::fs::remove_file(&file);
        }
        spec.inline = Some(options);
    } else if dry_run {
        eprintln!(
            "options ({}): {}",
            file.display(),
            service::show_options(&options)
        );
    } else {
        service::write_options(spec.role, &spec.home, &options)?;
        println!(
            "options: {} ({}; change them with `pumasi service set`)",
            service::show_options(&options),
            file.display()
        );
    }
    service::install(&spec, scope, dry_run)
}

/// `pumasi service set`: the installed service's options with `changes`
/// applied, written to its options file, and the service restarted. A
/// service installed before 1.4 is installed again in the form that reads
/// the file.
fn service_set(
    role: service::Role,
    scope: service::ScopeArgs,
    dry_run: bool,
    changes: &[String],
) -> Result<i32> {
    let Some(installed) = service::installed(role, scope)? else {
        bail!(
            "the {0} service is not installed here (`pumasi service install {0}`)",
            role.as_str()
        );
    };
    let def = installed.definition(role)?;
    let current = match &def.inline {
        Some(options) => options.clone(),
        None => service::read_options(role, &def.home)?.unwrap_or_default(),
    };
    let merged = service::merge_options(&service_command(role), &current, changes)?;
    let merged = service_defaults(role, &def.home, merged);
    parse_service_args(role, &merged)?;
    let file = role.options_file(&def.home);
    let moves = def.inline.is_some();
    if dry_run {
        println!("options:  {}", service::show_options(&merged));
        println!("          (nothing changed: --dry-run)");
        return Ok(0);
    }
    if merged == current && !moves {
        println!("options:  {}", service::show_options(&merged));
        println!("          (nothing to change)");
        return Ok(0);
    }
    service::can_control(role, installed.scope)?;
    service::write_options(role, &def.home, &merged)?;
    if moves {
        service::install(
            &service::Spec {
                role,
                exe: installed.exe.clone(),
                home: def.home.clone(),
                inline: None,
                user: installed.user.clone(),
            },
            installed.scope,
            false,
        )?;
        println!(
            "moved the options from the service definition to {}",
            file.display()
        );
    } else {
        service::restart(role, installed.scope)?;
        println!("restarted the {} service", role.as_str());
    }
    println!("options:  {}", service::show_options(&merged));
    println!("          ({})", file.display());
    Ok(0)
}

/// `pumasi service restart`: one service, or every Pumasi service
/// installed here.
fn service_restart(role: Option<service::Role>, scope: service::ScopeArgs) -> Result<i32> {
    use service::Role;
    // The control plane first: the dashboard and the agent find it again
    // by themselves.
    let roles = match role {
        Some(r) => vec![r],
        None => vec![Role::Cluster, Role::Ui, Role::Agent],
    };
    let mut found = Vec::new();
    for r in roles {
        match service::installed(r, scope)? {
            Some(installed) => found.push((r, installed.scope)),
            None if role.is_some() => bail!(
                "the {0} service is not installed here (`pumasi service install {0}`)",
                r.as_str()
            ),
            None => {}
        }
    }
    if found.is_empty() {
        println!("no Pumasi service is installed");
        return Ok(3);
    }
    // Before any is stopped.
    for (r, scope) in &found {
        service::can_control(*r, *scope)?;
    }
    for (r, scope) in found {
        service::restart(r, scope)?;
        println!("restarted the {} service", r.as_str());
    }
    Ok(0)
}

fn service_cmd(home: &Path, cmd: service::ServiceCmd) -> Result<i32> {
    use service::{Role, ServiceCmd};
    match cmd {
        ServiceCmd::Restart { role, scope } => service_restart(role, scope),
        ServiceCmd::Set {
            role,
            scope,
            dry_run,
            changes,
        } => service_set(role, scope, dry_run, &changes),
        ServiceCmd::Install {
            role: role @ (Role::Cluster | Role::Ui),
            ticket,
            scope,
            dry_run,
            inline_options,
            args,
        } => {
            if ticket.is_some() {
                bail!("--ticket is for the agent; the control plane and the dashboard take none");
            }
            let home = service::absolute(home)?;
            let args = service_defaults(role, &home, args);
            parse_service_args(role, &args)?;
            let user = if service::is_system(scope) {
                service::home_owner(&home)
            } else {
                None
            };
            if !dry_run {
                eprintln!(
                    "home: {}{}",
                    home.display(),
                    match &user {
                        Some(u) => format!(" (the service runs as {u}, its owner)"),
                        None => String::new(),
                    }
                );
                if std::env::var_os("SUDO_USER").is_some() && user.is_none() && !cfg!(windows) {
                    eprintln!(
                        "note: root owns this home; for your own cluster use `sudo pumasi --home ~/.pumasi service install {}`",
                        role.as_str()
                    );
                }
                if role == Role::Ui
                    && !has_flag(&args, "--admin")
                    && let Err(e) = load_ticket_with_source(&home, &AdminArgs { admin: None })
                {
                    eprintln!(
                        "note: no admin ticket in this home yet ({e:#}); the dashboard starts once the control plane here has made one, or after `pumasi login`"
                    );
                }
                if role == Role::Cluster {
                    eprintln!(
                        "note: stop a `pumasi cluster start` running in a terminal first: both use the same data and port"
                    );
                }
            }
            let exe = own_exe().context("cannot find this binary's path")?;
            let code = install_service(
                service::Spec {
                    role,
                    exe,
                    home: home.clone(),
                    inline: None,
                    user,
                },
                args.clone(),
                inline_options,
                scope,
                dry_run,
            )?;
            if !dry_run && role == Role::Ui && !has_flag(&args, "--api-only") {
                println!(
                    "the dashboard's sign-in link is written to {} when it starts",
                    home.join(ui::URL_FILE).display()
                );
            }
            Ok(code)
        }
        ServiceCmd::Install {
            role: Role::Agent,
            ticket,
            scope,
            dry_run,
            inline_options,
            args: agent_args,
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
            install_service(
                service::Spec {
                    role: Role::Agent,
                    exe,
                    home,
                    inline: None,
                    user: None,
                },
                agent_args,
                inline_options,
                scope,
                dry_run,
            )
        }
        ServiceCmd::Uninstall { role, scope } => service::uninstall(role, scope),
        ServiceCmd::Status { role, scope } => service::status(role, scope),
        ServiceCmd::Run { role, .. } => {
            bail!(
                "`service run` is what the service manager runs; use `pumasi {}`",
                match role {
                    Role::Agent => "agent start",
                    Role::Cluster => "cluster start",
                    Role::Ui => "ui",
                }
            )
        }
    }
}

async fn connect_admin(home: &Path, args: &AdminArgs) -> Result<AdminClient> {
    let (ticket, source) = load_ticket_with_source(home, args)?;
    let net = net_for_all(&ticket.candidates(), None);
    AdminClient::connect(&ticket, net)
        .await
        .map_err(|e| e.context(ticket_hint(home, &ticket, &source)))
}

/// Where the ticket in use came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TicketSource {
    /// `--admin` or `PUMASI_ADMIN`.
    Given,
    /// A file: the one `pumasi login` saved, or a local cluster's.
    File(PathBuf),
}

impl std::fmt::Display for TicketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Given => f.write_str("--admin / PUMASI_ADMIN"),
            Self::File(p) => write!(f, "{}", p.display()),
        }
    }
}

/// The admin or access ticket: `--admin`, else the one `pumasi login` or a
/// local cluster saved.
fn load_ticket(home: &Path, args: &AdminArgs) -> Result<AdminTicket> {
    Ok(load_ticket_with_source(home, args)?.0)
}

/// [`load_ticket`], and where it came from.
fn load_ticket_with_source(home: &Path, args: &AdminArgs) -> Result<(AdminTicket, TicketSource)> {
    let (text, source) = match &args.admin {
        Some(t) => (t.clone(), TicketSource::Given),
        None => {
            let candidates = [
                home.join(ADMIN_TICKET_FILE),
                home.join("cluster").join(ADMIN_TICKET_FILE),
            ];
            let found = candidates.iter().find(|p| p.exists()).with_context(|| {
                "no admin ticket found: start a cluster here, run `pumasi login <TICKET>`, or pass --admin"
            })?;
            (
                std::fs::read_to_string(found)?,
                TicketSource::File(found.clone()),
            )
        }
    };
    let ticket = text
        .parse()
        .with_context(|| format!("the ticket from {source} is not valid"))?;
    Ok((ticket, source))
}

/// Says which ticket a failed connection used, and points at the local
/// cluster's own ticket when that names another cluster (a `pumasi login`
/// left over from an earlier or another cluster wins over it).
fn ticket_hint(home: &Path, ticket: &AdminTicket, source: &TicketSource) -> String {
    let mut hint = format!(
        "using the ticket from {source} (cluster {})",
        ticket.cluster.id.fmt_short()
    );
    let local = home.join("cluster").join(ADMIN_TICKET_FILE);
    if *source != TicketSource::File(local.clone())
        && let Ok(text) = std::fs::read_to_string(&local)
        && let Ok(own) = text.parse::<AdminTicket>()
        && own.cluster.id != ticket.cluster.id
    {
        hint.push_str(&format!(
            "; the cluster started on this machine is {} and its ticket is {}: `pumasi login` with it to use that one",
            own.cluster.id.fmt_short(),
            local.display()
        ));
    }
    hint
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

fn print_status(s: &ClusterStatus, power: &[pumasi_proto::power::NodePowerView]) {
    let asleep = |id: &str| {
        power
            .iter()
            .any(|p| p.id == id && p.asleep_since_unix.is_some())
    };
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
        } else if asleep(&n.id) {
            "asleep"
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
    for p in power {
        let Some(n) = s.nodes.iter().find(|n| n.id == p.id && !n.online) else {
            continue;
        };
        if let Some(since) = p.asleep_since_unix {
            let wake = match (&p.report, p.can_wake) {
                (Some(r), true) if r.wake_on_lan => {
                    format!(
                        "; `pumasi node wake {}` wakes it, and so does work that waits for it",
                        n.info.name
                    )
                }
                (Some(r), false) if r.wake_on_lan => {
                    "; no machine of the cluster is online on its LAN to wake it".to_string()
                }
                _ => String::new(),
            };
            println!(
                "{} is asleep since {}{wake}",
                truncate(&n.info.name, 16),
                monitor::local_time(since, true)
            );
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

/// Jobs with more tasks list only the ones that did not succeed, unless
/// asked for their output.
const MAX_TASK_LINES: usize = 100;

/// Where each task of a job ran, by name, and how long the parts of its
/// run took (1.4). Empty when the control plane is older and does not
/// answer: the job is then printed without them.
async fn task_details(client: &AdminClient, id: JobId) -> Vec<pumasi_proto::msg::TaskDetail> {
    match client.call(AdminOp::TaskDetails { id }).await {
        Ok(AdminResponse::TaskDetails(d)) => d,
        _ => Vec::new(),
    }
}

/// ` queued=120ms pull=55000ms`: the parts of a run beside its billed
/// time, when there were any.
fn task_times(d: &pumasi_proto::msg::TaskDetail) -> String {
    [
        ("queued", d.queued_ms),
        ("fetch", d.fetch_ms),
        ("pull", d.pull_ms),
        ("upload", d.upload_ms),
    ]
    .into_iter()
    .filter(|(_, ms)| *ms > 0)
    .map(|(what, ms)| format!(" {what}={ms}ms"))
    .collect()
}

/// Every node's sleep and wake state (1.5). Empty when the control plane
/// is older and does not answer.
async fn node_power(client: &AdminClient) -> Vec<pumasi_proto::power::NodePowerView> {
    match client.call(AdminOp::NodePower).await {
        Ok(AdminResponse::NodePower(p)) => p,
        _ => Vec::new(),
    }
}

/// Whether each task of an `executor = "image"` job runs as Docker or in
/// a microVM. Empty for other jobs.
async fn task_executors(client: &AdminClient, v: &JobView) -> Vec<Option<Executor>> {
    if v.executor != Executor::Image {
        return Vec::new();
    }
    match client
        .call(AdminOp::TaskExecutors { id: v.summary.id })
        .await
    {
        Ok(AdminResponse::TaskExecutors(e)) => e,
        _ => Vec::new(),
    }
}

fn print_job(
    v: &JobView,
    details: &[pumasi_proto::msg::TaskDetail],
    ran: &[Option<Executor>],
    output: bool,
) {
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
        let show_line =
            output || !matches!(t.state, TaskState::Succeeded) || v.tasks.len() <= MAX_TASK_LINES;
        if show_line {
            let detail = details.iter().find(|d| d.index == t.index);
            // The node's name when the control plane gives it, else the
            // start of its id.
            let node = detail
                .and_then(|d| d.node_name.as_deref())
                .or(t.node.as_deref().map(short))
                .unwrap_or("-");
            // An image job's task: what it ran (or runs) as.
            let node = match ran.get(t.index as usize).copied().flatten() {
                Some(e) => format!("{node} as={e}"),
                None => node.to_string(),
            };
            let _ = writeln!(
                stdout,
                "  [{}] {} node={} attempts={} {}ms{}{}{}",
                t.index,
                state,
                node,
                t.attempts,
                t.wall_ms,
                detail.map(task_times).unwrap_or_default(),
                cost,
                check
            );
        }
        // A failed task shows why without another command: the end of
        // its stderr.
        if !output
            && matches!(t.state, TaskState::Failed(_))
            && let Some(err) = &t.stderr
        {
            let text = String::from_utf8_lossy(err);
            let (tail, cut) = tail_lines(&text, 5);
            if cut {
                let _ = writeln!(
                    stdout,
                    "      ... (`pumasi logs {} --task {}` has all of it)",
                    s.id, t.index
                );
            }
            for line in tail {
                let _ = writeln!(stdout, "      {line}");
            }
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
    if !output && v.tasks.len() > MAX_TASK_LINES {
        let hidden = v
            .tasks
            .iter()
            .filter(|t| matches!(t.state, TaskState::Succeeded))
            .count();
        if hidden > 0 {
            let _ = writeln!(
                stdout,
                "  ({hidden} succeeded tasks not listed; `pumasi job {} --output` lists every task)",
                s.id
            );
        }
    }
}

/// The last `n` non-empty lines of `text`, and whether there were more.
fn tail_lines(text: &str, n: usize) -> (Vec<&str>, bool) {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty())
        .collect();
    let cut = lines.len() > n;
    (lines[lines.len().saturating_sub(n)..].to_vec(), cut)
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
            "(no job can use it yet; `pumasi node` on this node says how)".to_string()
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
            if let Some(d) = &view {
                print_copies(&client, d).await;
            }
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
        StoreCmd::Replicas { name, admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Replication(r) = client.call(AdminOp::Replication { name }).await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            print_replication(&r);
            Ok(0)
        }
        StoreCmd::Copies {
            args,
            default,
            admin,
        } => {
            let parse = |s: &str| -> Result<Option<u8>> {
                if s == "default" {
                    return Ok(None);
                }
                let n: u8 = s
                    .parse()
                    .with_context(|| format!("not a number of copies: {s}"))?;
                pumasi_proto::replica::validate_factor(n)?;
                Ok(Some(n))
            };
            let (name, factor) = match (default, args.as_slice()) {
                (true, [n]) => (None, parse(n)?),
                (false, [name, n]) => (Some(name.clone()), parse(n)?),
                (true, _) => bail!("give one number with --default"),
                (false, _) => bail!("give a dataset and a number of copies (or `default`)"),
            };
            anyhow::ensure!(
                name.is_some() || factor.is_some(),
                "the default needs a number"
            );
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::Replication(r) = client
                .call(AdminOp::SetReplication {
                    name: name.clone(),
                    factor,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            match (name, factor) {
                (Some(n), Some(f)) => eprintln!("{n} keeps {f} copies"),
                (Some(n), None) => eprintln!("{n} keeps the default {} copies", r.default_factor),
                (None, _) => eprintln!("datasets keep {} copies unless set", r.default_factor),
            }
            Ok(0)
        }
        StoreCmd::Scrub { node, admin } => {
            let client = connect_admin(home, &admin).await?;
            client.call(AdminOp::Scrub { node: node.clone() }).await?;
            client.close().await;
            match node {
                Some(n) => eprintln!("{n} is checking its replicas; see `pumasi events`"),
                None => eprintln!(
                    "the control plane and the storage nodes are checking their stores; see `pumasi events`"
                ),
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

/// Says on how many copies a new version is (1.8 control planes).
async fn print_copies(client: &AdminClient, d: &pumasi_proto::store::DatasetView) {
    let Ok(AdminResponse::Replication(r)) = client
        .call(AdminOp::Replication {
            name: Some(d.name.clone()),
        })
        .await
    else {
        return;
    };
    let Some(v) = r.datasets.iter().find(|v| v.version == d.version) else {
        return;
    };
    let mut on = vec![if r.control_planes > 1 {
        format!("{} control planes", r.control_planes)
    } else {
        "the control plane".to_string()
    }];
    on.extend(v.nodes.iter().filter(|n| n.counted).map(|n| n.name.clone()));
    eprintln!("on {} of {} copies: {}", v.copies, v.factor, on.join(", "));
    if v.copies < v.factor {
        let usable = r.nodes.iter().filter(|n| n.not_used.is_none()).count();
        if usable == 0 {
            eprintln!(
                "warning: no node keeps replicas; label owner nodes with \
                 `pumasi node label NODE storage=SIZE` (e.g. storage=200G)"
            );
        } else {
            eprintln!("warning: fewer copies than wanted; see `pumasi store replicas`");
        }
    }
}

fn print_replication(r: &pumasi_proto::replica::ReplicationView) {
    use pumasi_proto::replica::ReplicaState;
    println!(
        "default copies: {}; control planes: {}",
        r.default_factor, r.control_planes
    );
    println!("{:<28} {:>10} {:>7}  NODES", "DATASET", "SIZE", "COPIES");
    for d in &r.datasets {
        let nodes: Vec<String> = d
            .nodes
            .iter()
            .map(|n| {
                let state = match (n.state, n.counted) {
                    (ReplicaState::Held, true) => "",
                    (ReplicaState::Held, false) => " (offline, not counted)",
                    (ReplicaState::Copying, _) => " (copying)",
                };
                format!("{}{state}", n.name)
            })
            .collect();
        let mut copies = format!("{}/{}", d.copies, d.factor);
        if d.own_factor {
            copies.push('*');
        }
        println!(
            "{:<28} {:>10} {:>7}  {}{}",
            format!("{}@{}", d.name, d.version),
            format_size(d.size),
            copies,
            if nodes.is_empty() {
                "-".into()
            } else {
                nodes.join(", ")
            },
            if d.leader_has {
                ""
            } else {
                "  [control plane fetching it back]"
            }
        );
    }
    if r.datasets.iter().any(|d| d.own_factor) {
        println!("* the dataset's own number of copies");
    }
    if !r.nodes.is_empty() {
        println!();
        println!(
            "{:<20} {:>10} {:>10} {:>9}  {:<12} SCRUBBED",
            "STORAGE NODE", "USED", "QUOTA", "REPLICAS", "STATE"
        );
        for n in &r.nodes {
            let state = match &n.not_used {
                None => "in use".to_string(),
                Some(why) => why.clone(),
            };
            let scrub = match n.scrubbed_unix {
                Some(t) => format!(
                    "{}{}",
                    crate::monitor::local_time(t, true),
                    if n.bad > 0 {
                        format!(" ({} damaged)", n.bad)
                    } else {
                        String::new()
                    }
                ),
                None => "never".into(),
            };
            println!(
                "{:<20} {:>10} {:>10} {:>9}  {:<12} {scrub}",
                n.name,
                format_size(n.used),
                n.quota
                    .map(format_size)
                    .unwrap_or_else(|| "90% disk".into()),
                n.replicas,
                state
            );
        }
    }
    if r.under > 0 {
        println!("{} dataset versions have fewer copies than wanted", r.under);
    }
    if r.bad > 0 {
        println!(
            "the control plane's last scrub found {} damaged collections",
            r.bad
        );
    }
}

async fn s3_cmd(home: &Path, cmd: S3Cmd) -> Result<i32> {
    match cmd {
        S3Cmd::CreateKey { label, admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::S3Key { secret, view } =
                client.call(AdminOp::S3KeyCreate { label }).await?
            else {
                bail!("unexpected response");
            };
            client.close().await;
            println!("access key id:     {}", view.id);
            println!("secret access key: {secret}");
            eprintln!(
                "acts as {} ({}); the secret is not shown again. Point S3 clients at \
                 http://127.0.0.1:7400/s3 (pumasi ui), region us-east-1, path-style addressing.",
                view.name, view.role
            );
            Ok(0)
        }
        S3Cmd::Keys { admin } => {
            let client = connect_admin(home, &admin).await?;
            let AdminResponse::S3Keys(keys) = client.call(AdminOp::S3Keys).await? else {
                bail!("unexpected response");
            };
            client.close().await;
            println!(
                "{:<26} {:<20} {:<12} {:<20} LABEL",
                "ID", "TICKET", "ROLE", "LAST USED"
            );
            for k in keys {
                println!(
                    "{:<26} {:<20} {:<12} {:<20} {}",
                    k.id,
                    k.name,
                    k.role.to_string(),
                    k.last_used_unix
                        .map(|t| crate::monitor::local_time(t, true))
                        .unwrap_or_else(|| "never".into()),
                    k.label
                );
            }
            Ok(0)
        }
        S3Cmd::RevokeKey { id, admin } => {
            let client = connect_admin(home, &admin).await?;
            client.call(AdminOp::S3KeyRevoke { id: id.clone() }).await?;
            client.close().await;
            eprintln!("revoked {id}");
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
                if vmm.backend == Backend::Vz {
                    "jobs with network = true get a filtered network card (vz)"
                } else if vmm.backend == Backend::Qemu
                    || (vmm.backend == Backend::Firecracker && a.qemu.is_some())
                {
                    "jobs with network = true get a filtered network card (QEMU)"
                } else {
                    "not available (Firecracker needs QEMU next to it for jobs with network = true)"
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

fn print_node(n: &NodeView, power: Option<&pumasi_proto::power::NodePowerView>) {
    println!("node {} \"{}\"", n.id, n.info.name);
    let state = if n.suspended {
        "suspended"
    } else if n.online && n.paused.is_some() {
        "paused"
    } else if n.online {
        "online"
    } else if power.is_some_and(|p| p.asleep_since_unix.is_some()) {
        "asleep"
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
    if let Some(p) = power {
        if let (Some(since), false) = (p.asleep_since_unix, n.online) {
            println!("  asleep since {}", monitor::local_time(since, true));
        }
        if let Some(r) = &p.report {
            let mut sleep = vec![
                if r.stays_awake {
                    "stays awake while it runs tasks"
                } else {
                    "may sleep while it runs tasks"
                }
                .to_string(),
            ];
            if r.sleep_notice {
                sleep.push("says so before it sleeps".into());
            }
            if r.on_pause == pumasi_proto::power::OnPause::Requeue {
                sleep.push("moves its tasks when it pauses".into());
            }
            println!("  sleep: {}", sleep.join(", "));
            for note in &r.notes {
                println!("    {note}");
            }
            let cards: Vec<String> = r
                .lan
                .iter()
                .map(|i| {
                    format!(
                        "{} {} ({}{})",
                        i.name,
                        pumasi_proto::power::format_mac(&i.mac),
                        i.cidr(),
                        if i.wireless { ", Wi-Fi" } else { "" }
                    )
                })
                .collect();
            println!(
                "  wake-on-lan: {}{}",
                if !r.wake_on_lan {
                    "off (agent option --wake-on-lan)"
                } else if p.can_wake {
                    "on"
                } else {
                    "on, but no machine of the cluster is online on its LAN"
                },
                if cards.is_empty() {
                    String::new()
                } else {
                    format!("; {}", cards.join(", "))
                }
            );
            if let (Some(at), Some(by)) = (p.last_wake_unix, &p.last_wake_by) {
                println!(
                    "  last magic packet: {} by {by}",
                    monitor::local_time(at, true)
                );
            }
        }
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
    for (en, _) in n.gpu_hints() {
        println!("  GPU: {en}");
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
    fn service_set_changes_the_real_commands_options() {
        use service::Role;
        let strs = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<String>>();
        // Issue #42: `--allow-webgpu` on an agent that has other options.
        let agent = service_command(Role::Agent);
        let merged = service::merge_options(
            &agent,
            &strs(&["--allow-docker", "--net", "local", "--name", "win-1"]),
            &strs(&["--allow-webgpu", "--no-allow-docker", "--slots", "4"]),
        )
        .unwrap();
        assert_eq!(
            merged,
            strs(&[
                "--net",
                "local",
                "--name",
                "win-1",
                "--allow-webgpu",
                "--slots",
                "4"
            ])
        );
        let Cmd::Agent(AgentCmd::Start { opts }) =
            parse_service_args(Role::Agent, &merged).unwrap()
        else {
            panic!()
        };
        assert!(opts.allow_webgpu && !opts.allow_docker);
        assert_eq!(opts.slots, Some(4));
        // Flags that come in pairs, and ones with only a `--no-` form.
        let merged = service::merge_options(
            &agent,
            &strs(&["--auto-upgrade", "--no-gpus", "--no-benchmark"]),
            &strs(&["--no-auto-upgrade", "--gpus", "--no-no-benchmark"]),
        )
        .unwrap();
        assert_eq!(merged, strs(&["--no-auto-upgrade"]));
        // What the result must still be: valid options of the command.
        let broken = service::merge_options(
            &agent,
            &strs(&["--allow-microvm", "--allow-confidential"]),
            &strs(&["--no-allow-microvm"]),
        )
        .unwrap();
        assert!(parse_service_args(Role::Agent, &broken).is_err());
        assert!(service::merge_options(&agent, &[], &strs(&["--home", "/x"])).is_err());

        // The control plane and the dashboard keep what a service needs.
        let home = Path::new("/var/lib/pumasi");
        let ui = service_command(Role::Ui);
        let current = service_defaults(Role::Ui, home, strs(&["--api-only"]));
        let merged = service::merge_options(
            &ui,
            &current,
            &strs(&["--listen", "0.0.0.0:7443", "--no-no-open"]),
        )
        .unwrap();
        let merged = service_defaults(Role::Ui, home, merged);
        assert!(has_flag(&merged, "--no-open") && has_flag(&merged, "--api-only"));
        assert_eq!(merged.iter().filter(|a| *a == "--listen").count(), 1);
        assert!(merged.contains(&"0.0.0.0:7443".to_string()));
        parse_service_args(Role::Ui, &merged).unwrap();
        let cluster = service_command(Role::Cluster);
        let merged = service::merge_options(
            &cluster,
            &strs(&["--no-token", "--name", "home"]),
            &strs(&["--relay", "https://relay.example.com"]),
        )
        .unwrap();
        parse_service_args(Role::Cluster, &merged).unwrap();
        assert!(has_flag(&merged, "--no-token"));
    }

    #[test]
    fn plain_windows_paths() {
        let plain = |s: &str| plain_path(PathBuf::from(s)).to_string_lossy().into_owned();
        assert_eq!(
            plain(r"\\?\C:\Program Files\Pumasi\pumasi.exe"),
            r"C:\Program Files\Pumasi\pumasi.exe"
        );
        assert_eq!(
            plain(r"\\?\UNC\host\share\pumasi.exe"),
            r"\\host\share\pumasi.exe"
        );
        assert_eq!(plain(r"C:\pumasi.exe"), r"C:\pumasi.exe");
        assert_eq!(
            plain(r"\\?\Volume{x}\pumasi.exe"),
            r"\\?\Volume{x}\pumasi.exe"
        );
        assert_eq!(plain("/usr/local/bin/pumasi"), "/usr/local/bin/pumasi");
    }

    #[test]
    fn utc_dates() {
        assert_eq!(utc(0), "1970-01-01 00:00:00");
        assert_eq!(utc(946_684_800), "2000-01-01 00:00:00");
        assert_eq!(utc(1_793_793_000), "2026-11-04 11:50:00");
    }
}
