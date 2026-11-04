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
use iroh::{Endpoint, EndpointAddr, EndpointId};
use pumasi_proto::{
    AGENT_ALPN, PROTOCOL_VERSION, Secret,
    msg::{
        AgentHello, FromAgent, HelloReply, NodeInfo, TaskAssignment, TaskRef, TaskReport, ToAgent,
    },
    net::{NetMode, bind_endpoint, load_or_create_key},
    ticket::JoinTicket,
    wire::{read_msg, write_msg},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::executor::WasmExecutor;

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
}

/// What the agent remembers about the cluster it joined.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Membership {
    pub cluster: EndpointAddr,
    pub cluster_name: String,
}

pub struct Agent {
    endpoint: Endpoint,
    executor: WasmExecutor,
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
        let endpoint = bind_endpoint(key, config.net, vec![], config.bind).await?;
        let executor = WasmExecutor::new()?;
        let info = node_info(&config);
        Ok(Self {
            endpoint,
            executor,
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
        let (cluster_name, heartbeat_ms) = match read_msg::<_, HelloReply>(&mut recv).await {
            Ok(Some(HelloReply::Welcome {
                cluster_name,
                heartbeat_ms,
            })) => (cluster_name, heartbeat_ms),
            Ok(Some(HelloReply::Rejected { reason })) => return SessionEnd::Rejected(reason),
            Ok(None) => return SessionEnd::Failed(anyhow::anyhow!("cluster closed the stream")),
            Err(e) => return SessionEnd::Failed(e),
        };
        info!(cluster = %cluster_name, node = %self.id().fmt_short(), slots = self.info.slots, "joined cluster");
        if join_secret.take().is_some() {
            let m = Membership {
                cluster: cluster.clone(),
                cluster_name: cluster_name.clone(),
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
                    // Compile here, in message order, so later assignments
                    // that reuse this module (and carry no bytes) find it.
                    let load_error = match a.module.take() {
                        Some(bytes) => {
                            let executor = self.executor.clone();
                            let hash = a.module_hash;
                            tokio::task::spawn_blocking(move || executor.load_module(hash, &bytes))
                                .await
                                .map_err(anyhow::Error::from)
                                .and_then(|r| r)
                                .err()
                        }
                        None => None,
                    };
                    match load_error {
                        None => self.start_task(*a, &running, &out_tx),
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
        running: &Arc<Mutex<HashMap<TaskRef, Arc<AtomicBool>>>>,
        out_tx: &mpsc::UnboundedSender<FromAgent>,
    ) {
        let cancel = Arc::new(AtomicBool::new(false));
        running.lock().unwrap().insert(a.task, cancel.clone());
        let executor = self.executor.clone();
        let running = running.clone();
        let out_tx = out_tx.clone();
        debug!(task = %a.task, "starting task");
        tokio::spawn(async move {
            let task = a.task;
            let result = tokio::task::spawn_blocking(move || {
                executor.run(&a.module_hash, &a.program, &a.input, &a.limits, cancel)
            })
            .await;
            running.lock().unwrap().remove(&task);
            let report = match result {
                Ok(r) => TaskReport {
                    task,
                    outcome: r.outcome,
                    stdout: r.stdout,
                    stderr: r.stderr,
                    fuel_used: r.fuel_used,
                    wall_ms: r.wall_ms,
                },
                Err(e) => error_report(task, format!("executor panicked: {e}")),
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
        fuel_used: 0,
        wall_ms: 0,
    }
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
    }
}

/// A reasonable default node name: the hostname, or "node".
pub fn default_node_name() -> String {
    sysinfo::System::host_name().unwrap_or_else(|| "node".to_string())
}
