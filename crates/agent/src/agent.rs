//! Agent runtime: keeps a connection to the control plane, runs assigned
//! tasks on the executor, and reports results.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use pumasi_proto::{
    AGENT_ALPN, ModuleHash, PROTOCOL_VERSION, Secret,
    job::{Executor, FileEntry},
    msg::{
        AgentHello, FromAgent, HelloReply, NodeInfo, Role, TaskAssignment, TaskRef, TaskReport,
        ToAgent,
    },
    net::{NetMode, bind_endpoint, load_or_create_key},
    receipt::{SignedUsage, Usage, result_digest},
    ticket::JoinTicket,
    wire::{read_msg, write_msg},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{docker::DockerExecutor, executor::WasmExecutor};

const MEMBERSHIP_FILE: &str = "membership.json";
const KEY_FILE: &str = "secret.key";
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub data_dir: PathBuf,
    pub name: String,
    /// Concurrent tasks. Defaults to the number of CPUs.
    pub slots: Option<u32>,
    pub net: NetMode,
    pub bind: Option<SocketAddr>,
    /// Docker CLI to use for docker jobs (`docker` or `podman`). `None`
    /// means this node runs WASM tasks only.
    pub docker: Option<String>,
    /// Test hook: tamper with every successful result before reporting it,
    /// like a provider that skips the work. Never set this in real use.
    #[doc(hidden)]
    pub corrupt_results: bool,
}

/// What the agent remembers about the cluster it joined.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Membership {
    pub cluster: EndpointAddr,
    pub cluster_name: String,
    #[serde(default)]
    pub role: Role,
    #[serde(default)]
    pub account: String,
}

type FileCache = Arc<Mutex<HashMap<ModuleHash, Arc<Vec<FileEntry>>>>>;

pub struct Agent {
    endpoint: Endpoint,
    /// Signs usage receipts. The same key as the endpoint identity.
    key: SecretKey,
    executor: WasmExecutor,
    docker: Option<DockerExecutor>,
    /// Shared job files by content hash.
    files: FileCache,
    config: AgentConfig,
    info: NodeInfo,
}

/// Why a session ended.
enum SessionEnd {
    /// The control plane refused us; retrying will not help.
    Rejected(String),
    /// The connection dropped after a successful handshake.
    Lost(anyhow::Error),
    /// Could not connect or handshake.
    Failed(anyhow::Error),
}

impl Agent {
    pub async fn new(config: AgentConfig) -> Result<Self> {
        let key = load_or_create_key(&config.data_dir.join(KEY_FILE))?;
        let endpoint = bind_endpoint(key.clone(), config.net, vec![], config.bind).await?;
        let executor = WasmExecutor::new()?;
        let docker =
            match &config.docker {
                Some(bin) => {
                    let label = endpoint.id().fmt_short().to_string();
                    Some(DockerExecutor::detect(bin, &label).await.context(
                        "--allow-docker was given but Docker is not usable on this machine",
                    )?)
                }
                None => None,
            };
        let mut info = node_info(&config);
        if docker.is_some() {
            info.executors.push(Executor::Docker);
        }
        Ok(Self {
            endpoint,
            key,
            executor,
            docker,
            files: Default::default(),
            config,
            info,
        })
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn info(&self) -> &NodeInfo {
        &self.info
    }

    /// Loads the saved membership, if this agent has joined a cluster before.
    pub fn membership(&self) -> Result<Option<Membership>> {
        let path = self.config.data_dir.join(MEMBERSHIP_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)?;
        Ok(Some(
            serde_json::from_str(&text).context("corrupt membership file")?,
        ))
    }

    fn save_membership(&self, m: &Membership) -> Result<()> {
        let path = self.config.data_dir.join(MEMBERSHIP_FILE);
        std::fs::create_dir_all(&self.config.data_dir)?;
        std::fs::write(&path, serde_json::to_vec_pretty(m)?)?;
        Ok(())
    }

    /// Joins with a ticket, then serves the cluster until a fatal error.
    pub async fn join(self, ticket: JoinTicket) -> Result<()> {
        self.serve(ticket.cluster, Some(ticket.secret)).await
    }

    /// Reconnects to the cluster this agent joined before.
    pub async fn resume(self) -> Result<()> {
        let Some(m) = self.membership()? else {
            bail!("this agent has not joined a cluster yet; run `pumasi agent join <TICKET>`");
        };
        self.serve(m.cluster, None).await
    }

    /// Serves the cluster forever, reconnecting with backoff when the
    /// connection drops. Returns only if the control plane rejects us.
    pub async fn serve(self, cluster: EndpointAddr, mut join_secret: Option<Secret>) -> Result<()> {
        let mut backoff = Duration::from_millis(500);
        loop {
            match self.session(&cluster, &mut join_secret).await {
                SessionEnd::Rejected(reason) => bail!("cluster rejected this node: {reason}"),
                SessionEnd::Lost(e) => {
                    warn!("lost connection to cluster: {e:#}");
                    backoff = Duration::from_millis(500);
                }
                SessionEnd::Failed(e) => warn!("could not reach cluster: {e:#}"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    async fn session(
        &self,
        cluster: &EndpointAddr,
        join_secret: &mut Option<Secret>,
    ) -> SessionEnd {
        let conn = match self.endpoint.connect(cluster.clone(), AGENT_ALPN).await {
            Ok(c) => c,
            Err(e) => return SessionEnd::Failed(e.into()),
        };
        let (mut send, mut recv) = match conn.open_bi().await {
            Ok(s) => s,
            Err(e) => return SessionEnd::Failed(e.into()),
        };
        let hello = AgentHello {
            protocol_version: PROTOCOL_VERSION,
            join_secret: *join_secret,
            node: self.info.clone(),
        };
        if let Err(e) = write_msg(&mut send, &hello).await {
            return SessionEnd::Failed(e);
        }
        let (cluster_name, heartbeat_ms, role, account) = match read_msg::<_, HelloReply>(&mut recv)
            .await
        {
            Ok(Some(HelloReply::Welcome {
                cluster_name,
                heartbeat_ms,
                role,
                account,
            })) => (cluster_name, heartbeat_ms, role, account),
            Ok(Some(HelloReply::Rejected { reason })) => return SessionEnd::Rejected(reason),
            Ok(None) => return SessionEnd::Failed(anyhow::anyhow!("cluster closed the stream")),
            Err(e) => return SessionEnd::Failed(e),
        };
        info!(cluster = %cluster_name, node = %self.id().fmt_short(), slots = self.info.slots, %role, %account, "joined cluster");
        if role == Role::Provider && self.docker.is_some() {
            info!("docker jobs run here only if the cluster's invite allowed them");
        }
        if join_secret.take().is_some() {
            let m = Membership {
                cluster: cluster.clone(),
                cluster_name: cluster_name.clone(),
                role,
                account: account.clone(),
            };
            if let Err(e) = self.save_membership(&m) {
                warn!("could not save membership: {e:#}");
            }
        }

        let running: Arc<Mutex<HashMap<TaskRef, Arc<AtomicBool>>>> = Default::default();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<FromAgent>();

        // Writer: the only task that touches the send stream.
        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if let Err(e) = write_msg(&mut send, &msg).await {
                    debug!("write failed: {e:#}");
                    break;
                }
            }
        });

        // Heartbeats.
        let heartbeat = {
            let out_tx = out_tx.clone();
            let running = running.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(heartbeat_ms.max(100)));
                loop {
                    tick.tick().await;
                    let tasks: Vec<TaskRef> = running.lock().unwrap().keys().copied().collect();
                    if out_tx
                        .send(FromAgent::Heartbeat { running: tasks })
                        .is_err()
                    {
                        break;
                    }
                }
            })
        };

        // Reader: handle assignments and cancellations.
        let end = loop {
            match read_msg::<_, ToAgent>(&mut recv).await {
                Ok(Some(ToAgent::Assign(mut a))) => {
                    // Cache shared files and compile modules here, in message
                    // order, so later assignments that only carry hashes
                    // find them.
                    if let (Some(hash), Some(files)) = (a.files_hash, a.files.take()) {
                        self.files.lock().unwrap().insert(hash, Arc::new(files));
                    }
                    let load_error = match (a.module.take(), a.module_hash) {
                        (Some(bytes), Some(hash)) => {
                            let executor = self.executor.clone();
                            tokio::task::spawn_blocking(move || executor.load_module(hash, &bytes))
                                .await
                                .map_err(anyhow::Error::from)
                                .and_then(|r| r)
                                .err()
                        }
                        _ => None,
                    };
                    match load_error {
                        None => self.start_task(*a, *cluster.id.as_bytes(), &running, &out_tx),
                        Some(e) => {
                            let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                                a.task,
                                format!("{e:#}"),
                            ))));
                        }
                    }
                }
                Ok(Some(ToAgent::Cancel(task))) => {
                    if let Some(flag) = running.lock().unwrap().get(&task) {
                        flag.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Ok(None) => break anyhow::anyhow!("cluster closed the stream"),
                Err(e) => break e,
            }
        };

        // Stop everything from this session; the control plane reschedules.
        for flag in running.lock().unwrap().values() {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        heartbeat.abort();
        writer.abort();
        conn.close(0u32.into(), b"bye");
        SessionEnd::Lost(end)
    }

    fn start_task(
        &self,
        a: TaskAssignment,
        cluster: [u8; 32],
        running: &Arc<Mutex<HashMap<TaskRef, Arc<AtomicBool>>>>,
        out_tx: &mpsc::UnboundedSender<FromAgent>,
    ) {
        let cancel = Arc::new(AtomicBool::new(false));
        running.lock().unwrap().insert(a.task, cancel.clone());
        let executor = self.executor.clone();
        let docker = self.docker.clone();
        let shared: Arc<Vec<FileEntry>> = match a.files_hash {
            Some(h) => match self.files.lock().unwrap().get(&h) {
                Some(f) => f.clone(),
                None => {
                    running.lock().unwrap().remove(&a.task);
                    let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                        a.task,
                        "job input files not received".into(),
                    ))));
                    return;
                }
            },
            None => Arc::new(Vec::new()),
        };
        let running = running.clone();
        let out_tx = out_tx.clone();
        let key = self.key.clone();
        let corrupt = self.config.corrupt_results;
        debug!(task = %a.task, executor = %a.executor, "starting task");
        tokio::spawn(async move {
            let task = a.task;
            let result: Result<crate::executor::RunResult> = match a.executor {
                Executor::Wasm => {
                    let Some(hash) = a.module_hash else {
                        let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                            task,
                            "no module".into(),
                        ))));
                        return;
                    };
                    let a = a.clone();
                    tokio::task::spawn_blocking(move || {
                        executor.run(
                            &hash, &a.program, &a.input, &shared, &a.limits, a.seed, cancel,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("executor panicked: {e}"))
                }
                Executor::Docker => match (docker, a.docker.as_ref()) {
                    (Some(d), Some(spec)) => Ok(d
                        .run(task, spec, &a.input, &shared, &a.limits, cancel)
                        .await),
                    (None, _) => Err(anyhow::anyhow!("docker is not enabled on this node")),
                    (_, None) => Err(anyhow::anyhow!("docker task without an image")),
                },
            };
            running.lock().unwrap().remove(&task);
            let report = match result {
                Ok(mut r) => {
                    if corrupt && r.outcome.is_success() {
                        r.stdout = b"made up".to_vec();
                    }
                    // Nothing ran, so there is nothing to bill.
                    let usage =
                        (!matches!(r.outcome, pumasi_proto::msg::Outcome::Error(_))).then(|| {
                            let usage = Usage {
                                cluster,
                                node: *key.public().as_bytes(),
                                task,
                                executor: a.executor,
                                outcome: r.outcome.clone(),
                                fuel_used: r.fuel_used,
                                wall_ms: r.wall_ms,
                                memory_limit_bytes: a.limits.memory_bytes,
                                peak_memory_bytes: r.peak_memory_bytes,
                                cpu_millis: a.limits.cpu_millis,
                                result_digest: result_digest(&r.outcome, &r.stdout, &r.files),
                                finished_unix: now_unix(),
                            };
                            SignedUsage::sign(usage, &key)
                        });
                    TaskReport {
                        task,
                        outcome: r.outcome,
                        stdout: r.stdout,
                        stderr: r.stderr,
                        files: r.files,
                        fuel_used: r.fuel_used,
                        wall_ms: r.wall_ms,
                        usage,
                    }
                }
                Err(e) => error_report(task, format!("{e:#}")),
            };
            debug!(task = %task, outcome = %report.outcome, "task finished");
            let _ = out_tx.send(FromAgent::Done(Box::new(report)));
        });
    }
}

fn error_report(task: TaskRef, msg: String) -> TaskReport {
    TaskReport {
        task,
        outcome: pumasi_proto::msg::Outcome::Error(msg),
        stdout: vec![],
        stderr: vec![],
        files: vec![],
        fuel_used: 0,
        wall_ms: 0,
        usage: None,
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn node_info(config: &AgentConfig) -> NodeInfo {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    NodeInfo {
        name: config.name.clone(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        cpus,
        memory_bytes: sys.total_memory(),
        slots: config.slots.unwrap_or(cpus).max(1),
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
        executors: vec![Executor::Wasm],
    }
}

/// A reasonable default node name: the hostname, or "node".
pub fn default_node_name() -> String {
    sysinfo::System::host_name().unwrap_or_else(|| "node".to_string())
}
