//! Agent runtime: keeps a connection to the control plane, runs assigned
//! tasks on the executor, and reports results.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, protocol::Router};
use pumasi_proto::{
    AGENT_ALPN, BLOBS_ALPN, ModuleHash, PROTOCOL_VERSION, Secret,
    job::{Executor, FileEntry, MAX_OUTPUT_FILES_BYTES, dir_files, read_files},
    msg::{
        AgentHello, Availability, FromAgent, HelloReply, NodeInfo, Role, TaskAssignment, TaskRef,
        TaskReport, ToAgent,
    },
    net::{NetMode, bind_endpoint, dialable_addr, load_or_create_key},
    perf::SignedPerf,
    receipt::{SignedUsage, Usage, output_digest},
    store::{OutputMode, StoredOutput},
    ticket::JoinTicket,
    wire::{read_msg, write_msg},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc, watch};
use tracing::{debug, info, warn};

use crate::{
    availability::AvailabilityConfig,
    bench::{self, BenchConfig, CpuKernel},
    docker::DockerExecutor,
    executor::{LocalImage, RunResult, TaskIo, WasmExecutor},
    microvm::{MicrovmConfig, MicrovmExecutor},
    store::{DEFAULT_CACHE_BYTES, InUse, NodeStore},
    tempdir::TempDir,
    upgrade::{AgentExit, Startup, TRIAL_TIMEOUT, UpgradeConfig, Upgrader},
};

const MEMBERSHIP_FILE: &str = "membership.json";
const KEY_FILE: &str = "secret.key";
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub data_dir: PathBuf,
    pub name: String,
    /// Concurrent tasks. Defaults to the CPUs offered, and fewer on a
    /// machine with little memory (one per GiB beyond the first two).
    pub slots: Option<u32>,
    /// Offer only this share of the CPUs (1 to 100): the node reports that
    /// many CPUs, so image jobs needing more go elsewhere, and slots and
    /// Docker containers without a CPU limit follow it.
    pub max_cpu_percent: Option<u32>,
    /// When the node takes new tasks (power, idle, hours). `None`: always.
    pub availability: Option<AvailabilityConfig>,
    pub net: NetMode,
    pub bind: Option<SocketAddr>,
    /// Docker CLI to use for docker jobs (`docker` or `podman`). `None`
    /// means this node runs WASM tasks only.
    pub docker: Option<String>,
    /// Run image jobs in microVMs. `None` means no microvm jobs here.
    pub microvm: Option<MicrovmConfig>,
    /// Let the cluster replace this agent's binary. `None`: never.
    pub upgrade: Option<UpgradeConfig>,
    /// Benchmark this machine (at start when the saved profile is stale,
    /// then every `interval` while idle). `None`: never; the node is then
    /// billed at the reference score.
    pub bench: Option<BenchConfig>,
    /// Test hook: tamper with every successful result before reporting it,
    /// like a provider that skips the work. Never set this in real use.
    #[doc(hidden)]
    pub corrupt_results: bool,
    /// Size of the file store cache (datasets and private images this
    /// node fetched). `None`: [`DEFAULT_CACHE_BYTES`].
    pub store_cache_bytes: Option<u64>,
    /// Offer this machine's GPUs to GPU jobs.
    pub gpus: bool,
    /// Test hook: report these GPUs instead of detecting any. Docker gets
    /// no device options for them (their vendor is `Other`), only
    /// `PUMASI_GPUS`. Never set this in real use.
    #[doc(hidden)]
    pub fake_gpus: Option<Vec<pumasi_proto::gpu::GpuInfo>>,
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
    microvm: Option<MicrovmExecutor>,
    /// Shared job files by content hash.
    files: FileCache,
    config: AgentConfig,
    info: NodeInfo,
    upgrader: Option<Arc<Upgrader>>,
    /// What the availability policy says now, kept current by a
    /// background check.
    available: watch::Receiver<Availability>,
    /// The CPU benchmark kernel, also used to answer challenges.
    kernel: CpuKernel,
    /// The latest signed performance profile.
    perf: Arc<Mutex<Option<SignedPerf>>>,
    /// Wakes the benchmark loop to measure again now.
    rebench: Arc<Notify>,
    /// The file store cache, which other nodes may fetch from.
    node_store: NodeStore,
    /// GPUs and microVM networking, signed; sent after the welcome.
    devices: Option<pumasi_proto::gpu::SignedDevices>,
    _router: Router,
}

/// Why a session ended.
enum SessionEnd {
    /// The control plane refused us; retrying will not help.
    Rejected(String),
    /// The connection dropped after a successful handshake.
    Lost(anyhow::Error),
    /// Could not connect or handshake.
    Failed(anyhow::Error),
    /// Installed a new binary; the process must restart.
    Upgraded(String),
}

impl Agent {
    pub async fn new(config: AgentConfig) -> Result<Self> {
        let key = load_or_create_key(&config.data_dir.join(KEY_FILE))?;
        let endpoint = bind_endpoint(
            key.clone(),
            config.net,
            vec![BLOBS_ALPN.to_vec()],
            config.bind,
        )
        .await?;
        let node_store = NodeStore::open(
            &config.data_dir.join("store"),
            config.store_cache_bytes.unwrap_or(DEFAULT_CACHE_BYTES),
            endpoint.clone(),
        )
        .await?;
        let router = Router::builder(endpoint.clone())
            .accept(BLOBS_ALPN, node_store.protocol())
            .spawn();
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
        let microvm =
            match &config.microvm {
                Some(vm) => {
                    let label = endpoint.id().fmt_short().to_string();
                    Some(MicrovmExecutor::detect(vm, &label).await.context(
                        "--allow-microvm was given but microVMs cannot run on this machine",
                    )?)
                }
                None => None,
            };
        let upgrader = match &config.upgrade {
            Some(u) => match Upgrader::new(u.clone(), &config.data_dir) {
                Ok(up) => Some(Arc::new(up)),
                Err(e) => {
                    warn!("cluster upgrades are off for this agent: {e:#}");
                    None
                }
            },
            None => None,
        };
        let mut info = node_info(&config);
        let docker =
            docker.map(|d| d.with_cpu_cap(config.max_cpu_percent.is_some().then_some(info.cpus)));
        let available = match &config.availability {
            Some(a) if a.policy.is_active() => {
                let now = a.check().await;
                log_availability(&now);
                let (tx, rx) = watch::channel(now);
                let a = a.clone();
                tokio::spawn(async move {
                    while !tx.is_closed() {
                        tokio::time::sleep(a.interval).await;
                        let now = a.check().await;
                        if *tx.borrow() != now {
                            log_availability(&now);
                            if tx.send(now).is_err() {
                                break;
                            }
                        }
                    }
                });
                rx
            }
            _ => watch::channel(Availability::Available).1,
        };
        let kernel = CpuKernel::new()?;
        let perf = match &config.bench {
            Some(b) => {
                let (b, dir, k, key, cpus) = (
                    b.clone(),
                    config.data_dir.clone(),
                    kernel.clone(),
                    key.clone(),
                    info.cpus,
                );
                match tokio::task::spawn_blocking(move || {
                    bench::profile(&b, &dir, &k, &key, cpus, false)
                })
                .await?
                {
                    Ok(p) => Some(p),
                    Err(e) => {
                        warn!("benchmark failed: {e:#}");
                        None
                    }
                }
            }
            None => None,
        };
        let devices = {
            let env = crate::gpu::GpuEnv {
                docker_nvidia: match &docker {
                    Some(d) if config.gpus => crate::gpu::docker_nvidia(d.bin()).await,
                    _ => false,
                },
                docker_amd: config.gpus && docker.is_some() && cfg!(target_os = "linux"),
                vfio: config.gpus && microvm.as_ref().is_some_and(|m| m.gpu_passthrough()),
                bench: config.gpus && config.bench.is_some(),
            };
            let fake = config.fake_gpus.clone();
            let detect = config.gpus || fake.is_some();
            let (gpus, notes) = if let Some(f) = fake {
                (f, Vec::new())
            } else if detect {
                tokio::task::spawn_blocking(move || {
                    let mut notes = Vec::new();
                    let g = crate::gpu::detect(&env, &mut notes);
                    (g, notes)
                })
                .await?
            } else {
                (Vec::new(), Vec::new())
            };
            for g in &gpus {
                info!(
                    gpu = g.index,
                    name = %g.describe(),
                    docker = g.docker,
                    microvm = g.vfio,
                    "GPU found"
                );
            }
            let network = microvm.as_ref().is_some_and(|m| m.network_capable());
            let profile = crate::gpu::profile(*key.public().as_bytes(), gpus, network, notes);
            Some(pumasi_proto::gpu::SignedDevices::sign(profile, &key))
        };
        if docker.is_some() {
            info.executors.push(Executor::Docker);
        }
        if microvm.is_some() {
            info.executors.push(Executor::Microvm);
        }
        Ok(Self {
            endpoint,
            key,
            executor,
            docker,
            microvm,
            files: Default::default(),
            config,
            info,
            upgrader,
            available,
            kernel,
            perf: Arc::new(Mutex::new(perf)),
            rebench: Arc::new(Notify::new()),
            node_store,
            devices,
            _router: router,
        })
    }

    /// The signed device profile (GPUs, microVM networking).
    pub fn devices(&self) -> Option<&pumasi_proto::gpu::SignedDevices> {
        self.devices.as_ref()
    }

    /// The latest signed performance profile, if the machine was measured.
    pub fn perf(&self) -> Option<SignedPerf> {
        self.perf.lock().unwrap().clone()
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

    /// Joins with a ticket, then serves the cluster until a fatal error or
    /// an upgrade.
    pub async fn join(self, ticket: JoinTicket) -> Result<AgentExit> {
        self.serve(ticket.cluster, Some(ticket.secret)).await
    }

    /// Reconnects to the cluster this agent joined before.
    pub async fn resume(self) -> Result<AgentExit> {
        let Some(m) = self.membership()? else {
            bail!("this agent has not joined a cluster yet; run `pumasi agent join <TICKET>`");
        };
        self.serve(m.cluster, None).await
    }

    /// Serves the cluster, reconnecting with backoff when the connection
    /// drops. Returns an error if the control plane rejects us, and
    /// [`AgentExit`] when the process must restart to run a new (or the
    /// previous) binary.
    pub async fn serve(
        self,
        cluster: EndpointAddr,
        join_secret: Option<Secret>,
    ) -> Result<AgentExit> {
        let exit = self.serve_inner(cluster, join_secret).await;
        // Let go of the file store so a restarted agent can open it.
        self.node_store.shutdown().await;
        exit
    }

    async fn serve_inner(
        &self,
        cluster: EndpointAddr,
        mut join_secret: Option<Secret>,
    ) -> Result<AgentExit> {
        // On trial: running a freshly installed binary not yet welcomed.
        let mut trial: Option<Instant> = None;
        if let Some(up) = &self.upgrader {
            match up.startup()? {
                Startup::Normal => {}
                Startup::Trial => trial = Some(Instant::now()),
                Startup::RolledBack(reason) => return Ok(AgentExit::RolledBack { reason }),
            }
        }
        let rollback = |reason: String| -> Result<AgentExit> {
            let up = self
                .upgrader
                .as_ref()
                .expect("on trial only with an upgrader");
            up.rollback(&reason)?;
            Ok(AgentExit::RolledBack { reason })
        };
        let mut backoff = Duration::from_millis(500);
        loop {
            match self.session(&cluster, &mut join_secret).await {
                SessionEnd::Rejected(reason) => {
                    if trial.is_some() {
                        return rollback(format!("cluster rejected the new binary: {reason}"));
                    }
                    bail!("cluster rejected this node: {reason}")
                }
                SessionEnd::Upgraded(version) => return Ok(AgentExit::Upgraded { version }),
                SessionEnd::Lost(e) => {
                    // The handshake succeeded, so the trial (if any) is over.
                    trial = None;
                    warn!("lost connection to cluster: {e:#}");
                    backoff = Duration::from_millis(500);
                }
                SessionEnd::Failed(e) => {
                    warn!("could not reach cluster: {e:#}");
                    if let Some(since) = trial
                        && since.elapsed() > TRIAL_TIMEOUT
                    {
                        return rollback(format!(
                            "the new binary could not reach the cluster for {} s: {e:#}",
                            TRIAL_TIMEOUT.as_secs()
                        ));
                    }
                }
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
        let mut available = self.available.clone();
        let hello = AgentHello {
            protocol_version: PROTOCOL_VERSION,
            join_secret: *join_secret,
            node: self.info.clone(),
            upgrade: self.upgrader.as_ref().map(|u| u.hello()),
            availability: available.borrow_and_update().clone(),
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
        if let Some(up) = &self.upgrader {
            up.commit();
        }
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
        // Other nodes fetch cached collections from this address.
        let addr = dialable_addr(&self.endpoint, self.config.net).await;
        self.node_store.attach(out_tx.clone(), Some(addr));

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

        // The profile, then a new one whenever the machine is measured again:
        // when the profile goes stale or the cluster asks, once no task runs.
        if let Some(p) = self.perf() {
            let _ = out_tx.send(FromAgent::Perf(Box::new(p)));
        }
        if let Some(d) = &self.devices {
            let _ = out_tx.send(FromAgent::Devices(Box::new(d.clone())));
        }
        let benchmarks = self.config.bench.clone().map(|config| {
            let out_tx = out_tx.clone();
            let running = running.clone();
            let perf = self.perf.clone();
            let rebench = self.rebench.clone();
            let (dir, kernel, key, cpus) = (
                self.config.data_dir.clone(),
                self.kernel.clone(),
                self.key.clone(),
                self.info.cpus,
            );
            tokio::spawn(async move {
                loop {
                    let age = perf
                        .lock()
                        .unwrap()
                        .as_ref()
                        .map(|p| now_unix().saturating_sub(p.profile.measured_unix))
                        .unwrap_or(u64::MAX);
                    let wait = config.interval.saturating_sub(Duration::from_secs(age));
                    tokio::select! {
                        _ = tokio::time::sleep(wait.max(Duration::from_secs(60))) => {}
                        _ = rebench.notified() => {}
                    }
                    while !running.lock().unwrap().is_empty() {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    let (c, d, k, key) = (config.clone(), dir.clone(), kernel.clone(), key.clone());
                    let res = tokio::task::spawn_blocking(move || {
                        bench::profile(&c, &d, &k, &key, cpus, true)
                    })
                    .await;
                    match res {
                        Ok(Ok(p)) => {
                            *perf.lock().unwrap() = Some(p.clone());
                            if out_tx.send(FromAgent::Perf(Box::new(p))).is_err() {
                                break;
                            }
                        }
                        Ok(Err(e)) => warn!("benchmark failed: {e:#}"),
                        Err(e) => warn!("benchmark panicked: {e}"),
                    }
                }
            })
        });

        // Availability changes, as they happen (the hello carried the
        // state at connect time).
        let availability = {
            let out_tx = out_tx.clone();
            tokio::spawn(async move {
                while available.changed().await.is_ok() {
                    let now = available.borrow_and_update().clone();
                    if out_tx.send(FromAgent::Availability(now)).is_err() {
                        break;
                    }
                }
            })
        };

        // An upgrade in progress reports here when it is done.
        let (up_tx, mut up_rx) = mpsc::unbounded_channel::<([u8; 32], Result<String>)>();
        let mut upgrading = false;

        // Reader: handle assignments and cancellations.
        let end = loop {
            let msg = tokio::select! {
                m = read_msg::<_, ToAgent>(&mut recv) => m,
                Some((sha, res)) = up_rx.recv() => {
                    upgrading = false;
                    match res {
                        Ok(version) => {
                            heartbeat.abort();
                            availability.abort();
                            if let Some(b) = &benchmarks {
                                b.abort();
                            }
                            writer.abort();
                            conn.close(0u32.into(), b"upgrading");
                            return SessionEnd::Upgraded(version);
                        }
                        Err(e) => {
                            let reason = format!("{e:#}");
                            warn!("upgrade failed: {reason}");
                            if let Some(up) = &self.upgrader {
                                up.record_failure(&sha, &reason);
                            }
                            let _ = out_tx.send(FromAgent::UpgradeFailed { sha256: sha, reason });
                            continue;
                        }
                    }
                }
            };
            match msg {
                Ok(Some(ToAgent::Upgrade(release))) => {
                    let sha = release.file().sha256;
                    let refuse = match &self.upgrader {
                        None => Some("this agent cannot replace its own binary".to_string()),
                        Some(up) if !up.accepts(role) => Some(
                            "upgrades are off on this node (start the agent with --auto-upgrade to accept them)"
                                .to_string(),
                        ),
                        Some(up) => up
                            .failed_before(&sha)
                            .map(|r| format!("failed before on this node: {r}")),
                    };
                    if let Some(reason) = refuse {
                        let _ = out_tx.send(FromAgent::UpgradeFailed {
                            sha256: sha,
                            reason,
                        });
                        continue;
                    }
                    if upgrading {
                        continue;
                    }
                    upgrading = true;
                    let up = self.upgrader.clone().expect("checked above");
                    let conn = conn.clone();
                    let running = running.clone();
                    let up_tx = up_tx.clone();
                    let cluster_key = *cluster.id.as_bytes();
                    info!(version = %release.file().version, "cluster offered an agent upgrade");
                    tokio::spawn(async move {
                        let res = async {
                            let staged = up.fetch(&conn, *release, &cluster_key, role).await?;
                            // Tasks placed before the offer finish first.
                            while !running.lock().unwrap().is_empty() {
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                            up.install(staged).await
                        }
                        .await;
                        let _ = up_tx.send((sha, res));
                    });
                }
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
                        None => self.start_task(*a, cluster, &running, &out_tx),
                        Some(e) => {
                            let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                                a.task,
                                format!("{e:#}"),
                            ))));
                        }
                    }
                }
                Ok(Some(ToAgent::Benchmark(ch))) => {
                    // As fast as possible: the cluster times the answer.
                    let kernel = self.kernel.clone();
                    let out_tx = out_tx.clone();
                    tokio::spawn(async move {
                        let res =
                            tokio::task::spawn_blocking(move || kernel.run(ch.seed, ch.iters))
                                .await;
                        match res {
                            Ok(Ok((result, _))) => {
                                let _ = out_tx.send(FromAgent::BenchResult {
                                    nonce: ch.nonce,
                                    result,
                                });
                            }
                            Ok(Err(e)) => warn!("benchmark check failed: {e:#}"),
                            Err(e) => warn!("benchmark check panicked: {e}"),
                        }
                    });
                }
                Ok(Some(ToAgent::GpuChallenge(ch))) => {
                    let pci = self
                        .devices
                        .as_ref()
                        .and_then(|d| d.profile.gpus.iter().find(|g| g.index == ch.device))
                        .map(|g| g.pci.clone())
                        .unwrap_or_default();
                    let out_tx = out_tx.clone();
                    tokio::spawn(async move {
                        let solutions = if pci.is_empty() {
                            Vec::new()
                        } else {
                            tokio::task::spawn_blocking(move || crate::gpu::solve(&pci, &ch))
                                .await
                                .unwrap_or_default()
                        };
                        let _ = out_tx.send(FromAgent::GpuResult {
                            nonce: ch.nonce,
                            solutions,
                        });
                    });
                }
                Ok(Some(ToAgent::Rebenchmark)) => {
                    if self.config.bench.is_some() {
                        info!("the cluster asked for a new benchmark");
                        self.rebench.notify_one();
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
        availability.abort();
        if let Some(b) = &benchmarks {
            b.abort();
        }
        writer.abort();
        conn.close(0u32.into(), b"bye");
        SessionEnd::Lost(end)
    }

    fn start_task(
        &self,
        a: TaskAssignment,
        cluster_addr: &EndpointAddr,
        running: &Arc<Mutex<HashMap<TaskRef, Arc<AtomicBool>>>>,
        out_tx: &mpsc::UnboundedSender<FromAgent>,
    ) {
        let cancel = Arc::new(AtomicBool::new(false));
        running.lock().unwrap().insert(a.task, cancel.clone());
        let executor = self.executor.clone();
        let docker = self.docker.clone();
        let microvm = self.microvm.clone();
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
        let gpus: Vec<pumasi_proto::gpu::GpuInfo> = self
            .devices
            .as_ref()
            .map(|d| {
                d.profile
                    .gpus
                    .iter()
                    .filter(|g| a.gpus.contains(&g.index))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if gpus.len() != a.gpus.len() {
            running.lock().unwrap().remove(&a.task);
            let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                a.task,
                format!("this node has no GPU {:?}", a.gpus),
            ))));
            return;
        }
        let node_store = self.node_store.clone();
        let cluster_addr = cluster_addr.clone();
        let cluster = *cluster_addr.id.as_bytes();
        debug!(task = %a.task, executor = %a.executor, "starting task");
        tokio::spawn(async move {
            let task = a.task;
            // Mounted datasets and a private image come from the file store
            // first; the run's timeout starts after.
            let prepared =
                prepare_io(&node_store, &a, &cluster_addr, &docker, &microvm, &cancel).await;
            let (mut io, _held, _dirs) = match prepared {
                Ok(p) => p,
                Err(e) => {
                    running.lock().unwrap().remove(&task);
                    let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                        task,
                        format!("{e:#}"),
                    ))));
                    return;
                }
            };
            io.gpus = gpus;
            let result: Result<RunResult> = match a.executor {
                Executor::Wasm => {
                    let Some(hash) = a.module_hash else {
                        let _ = out_tx.send(FromAgent::Done(Box::new(error_report(
                            task,
                            "no module".into(),
                        ))));
                        return;
                    };
                    let a = a.clone();
                    let io = io.clone();
                    tokio::task::spawn_blocking(move || {
                        executor.run_io(
                            &hash, &a.program, &a.input, &shared, &a.limits, a.seed, cancel, &io,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("executor panicked: {e}"))
                }
                Executor::Docker => match (docker, a.docker.as_ref()) {
                    (Some(d), Some(spec)) => Ok(d
                        .run_io(task, spec, &a.input, &shared, &a.limits, cancel, &io)
                        .await),
                    (None, _) => Err(anyhow::anyhow!("docker is not enabled on this node")),
                    (_, None) => Err(anyhow::anyhow!("docker task without an image")),
                },
                Executor::Microvm => match (microvm, a.docker.as_ref()) {
                    (Some(vm), Some(spec)) => Ok(vm
                        .run_io(task, spec, &a.input, &shared, &a.limits, cancel, &io)
                        .await),
                    (None, _) => Err(anyhow::anyhow!("microVMs are not enabled on this node")),
                    (_, None) => Err(anyhow::anyhow!("microvm task without an image")),
                },
            };
            // Output files kept on disk go in the report when they fit, or
            // to the file store.
            let result = match result {
                Ok(r) => finish_output(r, a.output, &node_store, &cluster_addr).await,
                Err(e) => Err(e),
            };
            running.lock().unwrap().remove(&task);
            let mut stored: Option<StoredOutput> = None;
            let report = match result {
                Ok((mut r, s)) => {
                    stored = s;
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
                                result_digest: output_digest(
                                    &r.outcome,
                                    &r.stdout,
                                    &r.files,
                                    stored.as_ref(),
                                ),
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
            let _ = out_tx.send(FromAgent::Report {
                report: Box::new(report),
                stored,
            });
        });
    }
}

/// Fetches what a task mounts (and its private image) from the file store
/// and places it for the executor. Returns the executor's view, the
/// collections held in the cache while the task runs, and the scratch
/// directories to remove afterwards.
async fn prepare_io(
    store: &NodeStore,
    a: &TaskAssignment,
    cluster: &EndpointAddr,
    docker: &Option<DockerExecutor>,
    microvm: &Option<MicrovmExecutor>,
    cancel: &AtomicBool,
) -> Result<(TaskIo, Vec<InUse>, Vec<TempDir>)> {
    let mut held = Vec::new();
    let mut dirs = Vec::new();
    let scratch = store.scratch("io")?;
    let mut io = TaskIo {
        output: a.output,
        scratch: Some(scratch.path().to_path_buf()),
        ..Default::default()
    };
    dirs.push(scratch);
    if !a.mounts.is_empty() {
        let mut mounts = Vec::new();
        for m in &a.mounts {
            let used = store
                .ensure(m.root, &m.label, &a.peers, cluster, cancel)
                .await?;
            mounts.push((m.clone(), used));
        }
        let dir = store.scratch("input")?;
        let bytes = store.stage(&mounts, dir.path()).await?;
        debug!(task = %a.task, bytes, "placed mounted files");
        io.staged = Some(dir.path().to_path_buf());
        dirs.push(dir);
        held.extend(mounts.into_iter().map(|(_, u)| u));
    }
    if let Some(img) = &a.image {
        let loaded = match a.executor {
            Executor::Docker => match docker {
                Some(d) => d.has_local_image(&img.digest).await,
                None => false,
            },
            Executor::Microvm => microvm.as_ref().is_some_and(|vm| vm.has_image(&img.digest)),
            Executor::Wasm => true,
        };
        let dir = store.scratch("image")?;
        if !loaded {
            let label = format!("image {}", img.reference);
            let used = store
                .ensure(img.root, &label, &a.peers, cluster, cancel)
                .await?;
            store.export_all(&used, dir.path()).await?;
            held.push(used);
        }
        io.image = Some(LocalImage {
            digest: img.digest.clone(),
            dir: dir.path().to_path_buf(),
        });
        dirs.push(dir);
    }
    Ok((io, held, dirs))
}

/// Decides where a run's output files go: in the report when they fit
/// (and the job allows it), else to the file store.
async fn finish_output(
    mut r: RunResult,
    mode: OutputMode,
    store: &NodeStore,
    cluster: &EndpointAddr,
) -> Result<(RunResult, Option<StoredOutput>)> {
    let Some(dir) = r.out_dir.take() else {
        return Ok((r, None));
    };
    let files = dir_files(dir.path())?;
    if files.is_empty() {
        return Ok((r, None));
    }
    let total: u64 = files.iter().map(|(_, _, n)| n).sum();
    if mode == OutputMode::Auto && total <= MAX_OUTPUT_FILES_BYTES as u64 {
        let (files, _) = read_files(dir.path(), MAX_OUTPUT_FILES_BYTES)?;
        r.files = files;
        return Ok((r, None));
    }
    let stored = store.push_output(dir.path(), cluster).await?;
    info!(
        files = stored.files,
        bytes = stored.size,
        "output files uploaded to the file store"
    );
    Ok((r, Some(stored)))
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

fn log_availability(a: &Availability) {
    match a.paused_reason() {
        Some(reason) => info!(%reason, "paused: taking no new tasks"),
        None => info!("available: taking tasks"),
    }
}

/// CPUs a node offers: all of them, or `percent` of them (at least one).
pub fn offered_cpus(cores: u32, percent: Option<u32>) -> u32 {
    match percent {
        Some(p) => (cores * p.min(100) / 100).max(1),
        None => cores,
    }
}

/// Default slots: one per offered CPU, but no more than one per GiB of
/// memory beyond the first two (kept for the OS), so a 4 GiB laptop runs
/// two tasks at once whatever its core count.
pub fn default_slots(cpus: u32, memory_bytes: u64) -> u32 {
    const GIB: u64 = 1 << 30;
    let by_memory = (memory_bytes.saturating_sub(2 * GIB) / GIB).max(1);
    cpus.min(by_memory.min(u32::MAX as u64) as u32).max(1)
}

fn node_info(config: &AgentConfig) -> NodeInfo {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    let cpus = offered_cpus(cores, config.max_cpu_percent);
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let memory_bytes = sys.total_memory();
    NodeInfo {
        name: config.name.clone(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        cpus,
        memory_bytes,
        slots: config
            .slots
            .unwrap_or_else(|| default_slots(cpus, memory_bytes))
            .max(1),
        agent_version: pumasi_proto::version().to_string(),
        executors: vec![Executor::Wasm],
    }
}

/// A reasonable default node name: the hostname, or "node".
pub fn default_node_name() -> String {
    sysinfo::System::host_name().unwrap_or_else(|| "node".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_cap_and_low_memory_slots() {
        const GIB: u64 = 1 << 30;
        assert_eq!(offered_cpus(8, None), 8);
        assert_eq!(offered_cpus(8, Some(50)), 4);
        assert_eq!(offered_cpus(4, Some(10)), 1);
        assert_eq!(offered_cpus(4, Some(300)), 4);
        // A 2013 MacBook Air: 4 threads, 4 GiB.
        assert_eq!(default_slots(4, 4 * GIB), 2);
        assert_eq!(default_slots(4, 8 * GIB), 4);
        assert_eq!(default_slots(16, 16 * GIB), 14);
        assert_eq!(default_slots(8, 2 * GIB), 1);
        assert_eq!(default_slots(2, 64 * GIB), 2);
    }
}
