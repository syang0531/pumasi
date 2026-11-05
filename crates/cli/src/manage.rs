//! Cluster management commands (phase 10): cordon, drain, labels, taints,
//! quotas, access tickets, schedules, `apply`, `get` and `describe`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Subcommand, ValueEnum};
use pumasi_cluster::AdminClient;
use pumasi_proto::{
    economy::{format_cu, parse_cu, validate_account},
    job::{JobFile, JobId},
    manage::{
        AccessRole, AccessView, AccountQuota, ClusterFile, NodeEdit, QuotaView, ScheduleSpec,
        ScheduleView, Selector, Taint,
    },
    msg::{AccountView, AdminOp, AdminResponse, ClusterStatus, NodeView},
    store::{format_size, parse_size},
};

use super::{
    AdminArgs, INLINE_INPUT_BYTES, connect_admin, print_job, print_node, short, truncate, utc,
};

#[derive(Subcommand)]
pub enum AccessCmd {
    /// Make a ticket for someone else: `admin` (everything), `submitter`
    /// (jobs, schedules and datasets of one account, and sees only those)
    /// or `viewer` (reads, changes nothing). They save it with
    /// `pumasi login`.
    Create {
        /// Who or what it is for, e.g. "alice laptop".
        name: String,
        #[arg(long, value_enum)]
        role: RoleArg,
        /// The account a submitter ticket works for.
        #[arg(long)]
        account: Option<String>,
        /// How long it stays valid, e.g. 30d (default: until revoked).
        #[arg(long)]
        ttl: Option<String>,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// List access tickets (never their secrets).
    Ls {
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Revoke a ticket by id (or a unique prefix) or by name.
    Revoke {
        id: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub enum RoleArg {
    Admin,
    Submitter,
    Viewer,
}

#[derive(Subcommand)]
pub enum ScheduleCmd {
    /// Submit JOB_FILE on a cron schedule, in the control plane's local
    /// time. Input files travel with the schedule (up to 16 MiB; larger
    /// ones belong in the file store).
    Add {
        name: String,
        job_file: PathBuf,
        /// `minute hour day month weekday`, e.g. "0 3 * * *", or @hourly,
        /// @daily, @weekly, @monthly.
        #[arg(long)]
        cron: String,
        /// Account that pays (default: owner).
        #[arg(long)]
        account: Option<String>,
        /// Start a run even if the previous one is still going.
        #[arg(long)]
        overlap: bool,
        #[command(flatten)]
        admin: AdminArgs,
    },
    Ls {
        #[command(flatten)]
        admin: AdminArgs,
    },
    Rm {
        name: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
    /// Submit a schedule's job now.
    Run {
        name: String,
        #[command(flatten)]
        admin: AdminArgs,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub enum GetKind {
    #[value(alias = "node")]
    Nodes,
    #[value(alias = "job")]
    Jobs,
    #[value(alias = "account")]
    Accounts,
    #[value(alias = "schedule")]
    Schedules,
    Access,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum DescribeKind {
    Node,
    Job,
    Account,
    Schedule,
}

#[derive(Clone, Copy, Default, ValueEnum)]
pub enum Output {
    #[default]
    Table,
    Json,
}

async fn call(home: &Path, admin: &AdminArgs, op: AdminOp) -> Result<AdminResponse> {
    let client = connect_admin(home, admin).await?;
    let r = client.call(op).await;
    client.close().await;
    r
}

async fn status(client: &AdminClient) -> Result<ClusterStatus> {
    match client.call(AdminOp::Status).await? {
        AdminResponse::Status(s) => Ok(s),
        _ => bail!("unexpected response"),
    }
}

async fn quotas(client: &AdminClient) -> Result<Vec<QuotaView>> {
    match client.call(AdminOp::Quotas).await? {
        AdminResponse::Quotas(q) => Ok(q),
        _ => bail!("unexpected response"),
    }
}

async fn schedules(client: &AdminClient) -> Result<Vec<ScheduleView>> {
    match client.call(AdminOp::Schedules).await? {
        AdminResponse::Schedules(s) => Ok(s),
        _ => bail!("unexpected response"),
    }
}

/// `pumasi cordon`, `uncordon`, `label`, `taint`.
pub async fn edit_node(home: &Path, admin: &AdminArgs, node: &str, edit: NodeEdit) -> Result<i32> {
    call(
        home,
        admin,
        AdminOp::EditNode {
            node: node.to_string(),
            edit: Box::new(edit),
        },
    )
    .await?;
    Ok(0)
}

/// `key=value` sets a label, `key-` removes it.
pub fn label_edit(args: &[String]) -> Result<NodeEdit> {
    let mut edit = NodeEdit::default();
    for a in args {
        if let Some(k) = a.strip_suffix('-') {
            edit.remove_labels.push(k.to_string());
        } else if let Some((k, v)) = a.split_once('=') {
            pumasi_proto::manage::validate_user_label(k, v)?;
            edit.set_labels.insert(k.to_string(), v.to_string());
        } else {
            bail!("labels are `key=value` (set) or `key-` (remove), not {a:?}");
        }
    }
    Ok(edit)
}

/// `key` or `key=value` adds a taint, `key-` removes it.
pub fn taint_edit(args: &[String]) -> Result<NodeEdit> {
    let mut edit = NodeEdit::default();
    for a in args {
        match a.strip_suffix('-') {
            Some(k) => edit.remove_taints.push(k.to_string()),
            None => edit.add_taints.push(a.parse::<Taint>()?),
        }
    }
    Ok(edit)
}

pub async fn drain(home: &Path, admin: &AdminArgs, node: &str) -> Result<i32> {
    let AdminResponse::Drained { moved } = call(
        home,
        admin,
        AdminOp::Drain {
            node: node.to_string(),
        },
    )
    .await?
    else {
        bail!("unexpected response");
    };
    eprintln!(
        "drained {node}: {moved} running task{} moved to other nodes; it takes no new tasks until `pumasi uncordon {node}`",
        if moved == 1 { "" } else { "s" }
    );
    Ok(0)
}

/// A limit given on the command line: a value, or `none` to lift it.
fn limit<T>(v: &Option<String>, parse: impl Fn(&str) -> Result<T>) -> Result<Option<Option<T>>> {
    match v.as_deref().map(str::trim) {
        None => Ok(None),
        Some("none") => Ok(Some(None)),
        Some(s) => Ok(Some(Some(parse(s)?))),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn quota(
    home: &Path,
    admin: &AdminArgs,
    account: Option<String>,
    slots: Option<String>,
    gpus: Option<String>,
    cu_per_day: Option<String>,
    max_priority: Option<String>,
    storage: Option<String>,
    clear: bool,
) -> Result<i32> {
    let Some(account) = account else {
        let client = connect_admin(home, admin).await?;
        let list = quotas(&client).await;
        client.close().await;
        print_quotas(&list?);
        return Ok(0);
    };
    validate_account(&account)?;
    let num = |s: &str| s.parse::<u32>().context("expected a number or `none`");
    let slots = limit(&slots, num)?;
    let gpus = limit(&gpus, num)?;
    let cu_per_day = limit(&cu_per_day, parse_cu)?;
    let max_priority = limit(&max_priority, |s| {
        s.parse::<i32>().context("expected a number or `none`")
    })?;
    let storage = limit(&storage, parse_size)?;
    let client = connect_admin(home, admin).await?;
    let set = async {
        let mut q = if clear {
            AccountQuota::default()
        } else {
            quotas(&client)
                .await?
                .into_iter()
                .find(|v| v.account == account)
                .map(|v| v.quota)
                .unwrap_or_default()
        };
        if let Some(v) = slots {
            q.slots = v;
        }
        if let Some(v) = gpus {
            q.gpus = v;
        }
        if let Some(v) = cu_per_day {
            q.cu_per_day = v;
        }
        if let Some(v) = max_priority {
            q.max_priority = v;
        }
        if let Some(v) = storage {
            q.storage_bytes = v;
        }
        client
            .call(AdminOp::SetQuota {
                account: account.clone(),
                quota: q.clone(),
            })
            .await?;
        anyhow::Ok(q)
    };
    let q = set.await;
    client.close().await;
    let q = q?;
    eprintln!("quota of {account}: {}", q.describe());
    Ok(0)
}

fn print_quotas(list: &[QuotaView]) {
    println!(
        "{:<16} {:>9} {:>7} {:>14} {:>8} {:>18} {:>7}",
        "ACCOUNT", "RUNNING", "GPUS", "CU/24H", "MAX PRI", "STORAGE", "QUEUED"
    );
    let of = |used: String, max: Option<String>| match max {
        Some(m) => format!("{used}/{m}"),
        None => used,
    };
    for v in list {
        let q = &v.quota;
        println!(
            "{:<16} {:>9} {:>7} {:>14} {:>8} {:>18} {:>7}",
            truncate(&v.account, 16),
            of(v.running.to_string(), q.slots.map(|s| s.to_string())),
            of(v.gpus.to_string(), q.gpus.map(|s| s.to_string())),
            of(format_cu(v.spent_day), q.cu_per_day.map(format_cu)),
            q.max_priority
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
            of(
                format_size(v.storage_bytes),
                q.storage_bytes.map(format_size)
            ),
            v.queued
        );
    }
    if list.is_empty() {
        println!("(no accounts)");
    }
    println!("(used/limit; no limit when only one number is shown)");
}

pub async fn access(home: &Path, cmd: AccessCmd) -> Result<i32> {
    match cmd {
        AccessCmd::Create {
            name,
            role,
            account,
            ttl,
            admin,
        } => {
            let role = match (role, account) {
                (RoleArg::Submitter, Some(account)) => {
                    validate_account(&account)?;
                    AccessRole::Submitter { account }
                }
                (RoleArg::Submitter, None) => bail!("a submitter ticket needs --account"),
                (_, Some(_)) => bail!("--account is only for submitter tickets"),
                (RoleArg::Admin, None) => AccessRole::Admin,
                (RoleArg::Viewer, None) => AccessRole::Viewer,
            };
            let ttl_secs = ttl
                .as_deref()
                .map(pumasi_proto::job::parse_duration_ms)
                .transpose()?
                .map(|ms| ms / 1000);
            let AdminResponse::AccessTicket { ticket, view } = call(
                home,
                &admin,
                AdminOp::CreateAccess {
                    name,
                    role,
                    ttl_secs,
                },
            )
            .await?
            else {
                bail!("unexpected response");
            };
            eprintln!(
                "{} ticket \"{}\" (id {}){}; it is shown only now. On their machine:",
                view.role,
                view.name,
                view.id,
                match view.expires_unix {
                    Some(t) => format!(", valid until {} UTC", utc(t)),
                    None => String::new(),
                }
            );
            eprintln!();
            println!("pumasi login {ticket}");
            Ok(0)
        }
        AccessCmd::Ls { admin } => {
            let AdminResponse::AccessList(list) = call(home, &admin, AdminOp::ListAccess).await?
            else {
                bail!("unexpected response");
            };
            print_access(&list);
            Ok(0)
        }
        AccessCmd::Revoke { id, admin } => {
            let AdminResponse::AccessList(gone) =
                call(home, &admin, AdminOp::RevokeAccess { id }).await?
            else {
                bail!("unexpected response");
            };
            for v in gone {
                eprintln!("revoked {} ticket \"{}\" ({})", v.role, v.name, v.id);
            }
            Ok(0)
        }
    }
}

fn print_access(list: &[AccessView]) {
    println!(
        "{:<14} {:<24} {:<22} {:<20} {:<20}",
        "ID", "NAME", "ROLE", "CREATED (UTC)", "EXPIRES (UTC)"
    );
    for v in list {
        println!(
            "{:<14} {:<24} {:<22} {:<20} {:<20}",
            v.id,
            truncate(&v.name, 24),
            truncate(&v.role.to_string(), 22),
            utc(v.created_unix),
            v.expires_unix.map(utc).unwrap_or_else(|| "never".into())
        );
    }
    if list.is_empty() {
        println!("(no access tickets: `pumasi access create NAME --role viewer`)");
    }
}

pub async fn whoami(home: &Path, admin: &AdminArgs) -> Result<i32> {
    let AdminResponse::WhoAmI { name, role } = call(home, admin, AdminOp::WhoAmI).await? else {
        bail!("unexpected response");
    };
    println!("{name}: {role}");
    Ok(0)
}

pub async fn schedule(home: &Path, cmd: ScheduleCmd) -> Result<i32> {
    match cmd {
        ScheduleCmd::Add {
            name,
            job_file,
            cron,
            account,
            overlap,
            admin,
        } => {
            let (mut job, upload) = JobFile::load_split(&job_file, INLINE_INPUT_BYTES)?;
            if !upload.is_empty() {
                bail!(
                    "the job's input files are too large to keep with a schedule; put them in the file store (`pumasi store put`) and mount them with `store:NAME`"
                );
            }
            if let Some(a) = &account {
                validate_account(a)?;
            }
            job.account = account;
            let spec = ScheduleSpec {
                name: name.clone(),
                cron,
                job,
                overlap,
            };
            spec.validate()?;
            let client = connect_admin(home, &admin).await?;
            let list = match client.call(AdminOp::SetSchedule(Box::new(spec))).await {
                Ok(_) => schedules(&client).await,
                Err(e) => Err(e),
            };
            client.close().await;
            let list = list?;
            if let Some(s) = list.iter().find(|s| s.name == name) {
                eprintln!(
                    "schedule {name} set; next run {}",
                    s.next_unix
                        .map(|t| format!("{} UTC", utc(t)))
                        .unwrap_or_else(|| "never".into())
                );
            }
            Ok(0)
        }
        ScheduleCmd::Ls { admin } => {
            let client = connect_admin(home, &admin).await?;
            let list = schedules(&client).await;
            client.close().await;
            print_schedules(&list?);
            Ok(0)
        }
        ScheduleCmd::Rm { name, admin } => {
            call(home, &admin, AdminOp::RemoveSchedule { name: name.clone() }).await?;
            eprintln!("removed schedule {name}");
            Ok(0)
        }
        ScheduleCmd::Run { name, admin } => {
            let AdminResponse::Submitted { id } =
                call(home, &admin, AdminOp::RunSchedule { name }).await?
            else {
                bail!("unexpected response");
            };
            println!("{id}");
            Ok(0)
        }
    }
}

fn print_schedules(list: &[ScheduleView]) {
    println!(
        "{:<20} {:<16} {:<12} {:<18} {:>5} {:<20} {:<17}",
        "NAME", "CRON", "ACCOUNT", "JOB", "RUNS", "NEXT (UTC)", "LAST JOB"
    );
    for s in list {
        println!(
            "{:<20} {:<16} {:<12} {:<18} {:>5} {:<20} {:<17}",
            truncate(&s.name, 20),
            truncate(&s.cron, 16),
            truncate(&s.account, 12),
            truncate(&format!("{} ({})", s.job_name, s.tasks), 18),
            s.runs,
            s.next_unix.map(utc).unwrap_or_else(|| "-".into()),
            s.last_job
                .map(|j| j.to_string())
                .unwrap_or_else(|| "-".into())
        );
        if let Some(e) = &s.last_error {
            println!("  last run not submitted: {e}");
        }
    }
    if list.is_empty() {
        println!("(no schedules: `pumasi schedule add NAME job.toml --cron \"0 3 * * *\"`)");
    }
}

pub async fn apply(
    home: &Path,
    admin: &AdminArgs,
    file: &Path,
    dry_run: bool,
    prune: bool,
) -> Result<i32> {
    let mut spec = ClusterFile::load(file, INLINE_INPUT_BYTES)?;
    spec.dry_run = dry_run;
    spec.prune = prune;
    let AdminResponse::Applied(changes) = call(home, admin, AdminOp::Apply(Box::new(spec))).await?
    else {
        bail!("unexpected response");
    };
    if changes.is_empty() {
        eprintln!("{}: nothing to change", file.display());
    }
    for c in &changes {
        println!("{}{c}", if dry_run { "would change " } else { "" });
    }
    Ok(0)
}

fn json<T: serde::Serialize>(v: &T) -> Result<i32> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(0)
}

fn node_state(n: &NodeView) -> &'static str {
    if n.suspended {
        "suspended"
    } else if !n.online {
        "offline"
    } else if n.config.cordoned {
        "cordoned"
    } else if n.paused.is_some() {
        "paused"
    } else {
        "online"
    }
}

fn find_node<'a>(nodes: &'a [NodeView], name: &str) -> Result<&'a NodeView> {
    let by_name: Vec<&NodeView> = nodes.iter().filter(|n| n.info.name == name).collect();
    if let [one] = by_name.as_slice() {
        return Ok(one);
    }
    let by_id: Vec<&NodeView> = nodes.iter().filter(|n| n.id.starts_with(name)).collect();
    match by_id.as_slice() {
        [one] => Ok(one),
        [] if by_name.is_empty() => bail!("no node {name:?}"),
        _ => bail!("{name:?} matches more than one node; use more of the id"),
    }
}

pub async fn get(
    home: &Path,
    admin: &AdminArgs,
    kind: GetKind,
    selector: Option<String>,
    output: Output,
) -> Result<i32> {
    let selector: Option<Selector> = selector.as_deref().map(str::parse).transpose()?;
    let client = connect_admin(home, admin).await?;
    let result = get_inner(&client, kind, selector, output).await;
    client.close().await;
    result
}

async fn get_inner(
    client: &AdminClient,
    kind: GetKind,
    selector: Option<Selector>,
    output: Output,
) -> Result<i32> {
    match kind {
        GetKind::Nodes => {
            let mut nodes = status(client).await?.nodes;
            if let Some(sel) = &selector {
                nodes.retain(|n| sel.matches(&n.labels));
            }
            if let Output::Json = output {
                return json(&nodes);
            }
            println!(
                "{:<12} {:<16} {:<10} {:>5} {:>7}  {:<40} TAINTS",
                "NODE", "NAME", "STATE", "SLOTS", "RUNNING", "LABELS"
            );
            for n in &nodes {
                let labels: Vec<String> = n
                    .config
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                let taints: Vec<String> = n.config.taints.iter().map(|t| t.to_string()).collect();
                println!(
                    "{:<12} {:<16} {:<10} {:>5} {:>7}  {:<40} {}",
                    short(&n.id),
                    truncate(&n.info.name, 16),
                    node_state(n),
                    n.info.slots,
                    n.running,
                    truncate(&labels.join(","), 40),
                    taints.join(",")
                );
            }
            if nodes.is_empty() {
                println!("(no nodes)");
            }
            println!(
                "(LABELS: set by `pumasi label`; `pumasi describe node NAME` shows built-in ones too)"
            );
        }
        GetKind::Jobs => {
            if selector.is_some() {
                bail!("-l selects nodes; it does not apply to jobs");
            }
            let jobs = status(client).await?.jobs;
            if let Output::Json = output {
                return json(&jobs);
            }
            println!(
                "{:<17} {:<20} {:<10} {:>5} {:>7} {:>7} {:>7} {:<12} {:>10}",
                "JOB", "NAME", "STATE", "PRI", "TASKS", "DONE", "FAILED", "ACCOUNT", "COST"
            );
            for j in &jobs {
                println!(
                    "{:<17} {:<20} {:<10} {:>5} {:>7} {:>7} {:>7} {:<12} {:>10}",
                    j.id.to_string(),
                    truncate(&j.name, 20),
                    j.state.to_string(),
                    j.priority,
                    j.tasks,
                    j.succeeded,
                    j.failed,
                    truncate(&j.account, 12),
                    format_cu(j.cost)
                );
            }
            if jobs.is_empty() {
                println!("(no jobs)");
            }
        }
        GetKind::Accounts => {
            let list = quotas(client).await?;
            if let Output::Json = output {
                return json(&list);
            }
            print_quotas(&list);
        }
        GetKind::Schedules => {
            let list = schedules(client).await?;
            if let Output::Json = output {
                return json(&list);
            }
            print_schedules(&list);
        }
        GetKind::Access => {
            let AdminResponse::AccessList(list) = client.call(AdminOp::ListAccess).await? else {
                bail!("unexpected response");
            };
            if let Output::Json = output {
                return json(&list);
            }
            print_access(&list);
        }
    }
    Ok(0)
}

pub async fn describe(
    home: &Path,
    admin: &AdminArgs,
    kind: DescribeKind,
    name: &str,
) -> Result<i32> {
    let client = connect_admin(home, admin).await?;
    let result = describe_inner(&client, kind, name).await;
    client.close().await;
    result
}

async fn describe_inner(client: &AdminClient, kind: DescribeKind, name: &str) -> Result<i32> {
    match kind {
        DescribeKind::Node => {
            let s = status(client).await?;
            let n = find_node(&s.nodes, name)?;
            print_node(n);
        }
        DescribeKind::Job => {
            let id: JobId = name.parse()?;
            let AdminResponse::Job(v) = client
                .call(AdminOp::Job {
                    id,
                    with_output: false,
                })
                .await?
            else {
                bail!("unexpected response");
            };
            print_job(&v, false);
        }
        DescribeKind::Account => {
            let list = quotas(client).await?;
            let q = list
                .iter()
                .find(|q| q.account == name)
                .with_context(|| format!("no account {name}"))?;
            let AdminResponse::Accounts(accounts) = client.call(AdminOp::Accounts).await? else {
                bail!("unexpected response");
            };
            let a: Option<&AccountView> = accounts.iter().find(|a| a.name == name);
            println!("account {name}");
            if let Some(a) = a {
                println!(
                    "  balance {} CU, held {} CU, spent {} CU, earned {} CU",
                    format_cu(a.balance),
                    format_cu(a.held),
                    format_cu(a.spent),
                    format_cu(a.earned)
                );
            }
            println!("  quota: {}", q.quota.describe());
            println!(
                "  now: {} running, {} GPUs, {} queued, {} CU in the last 24 h, {} stored",
                q.running,
                q.gpus,
                q.queued,
                format_cu(q.spent_day),
                format_size(q.storage_bytes)
            );
            let s = status(client).await?;
            let jobs: Vec<_> = s
                .jobs
                .iter()
                .filter(|j| j.account == name)
                .take(10)
                .collect();
            if !jobs.is_empty() {
                println!("  recent jobs:");
                for j in jobs {
                    println!(
                        "    {} {:<20} {:<10} priority {}",
                        j.id,
                        truncate(&j.name, 20),
                        j.state.to_string(),
                        j.priority
                    );
                }
            }
        }
        DescribeKind::Schedule => {
            let list = schedules(client).await?;
            let s = list
                .iter()
                .find(|s| s.name == name)
                .with_context(|| format!("no schedule {name}"))?;
            println!("schedule {} \"{}\"", s.name, s.cron);
            println!(
                "  job {} ({} tasks), account {}, priority {}{}",
                s.job_name,
                s.tasks,
                s.account,
                s.priority,
                if s.overlap { ", runs may overlap" } else { "" }
            );
            println!(
                "  next run {}",
                s.next_unix
                    .map(|t| format!("{} UTC", utc(t)))
                    .unwrap_or_else(|| "never".into())
            );
            match (s.last_unix, s.last_job) {
                (Some(t), Some(j)) => println!(
                    "  last run {} UTC: job {j} ({} runs so far)",
                    utc(t),
                    s.runs
                ),
                (Some(t), None) => println!("  last tried {} UTC", utc(t)),
                _ => println!("  not run yet"),
            }
            if let Some(e) = &s.last_error {
                println!("  last run not submitted: {e}");
            }
        }
    }
    Ok(0)
}
