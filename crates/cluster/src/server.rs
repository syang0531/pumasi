//! Network front of the control plane: accepts agent and admin connections
//! over iroh and drives the [`State`] machine.
//!
//! A control plane with replicas (phase 13, see [`crate::ha`]) runs the
//! same services, but only on the replica that leads: [`Shared`] is what
//! a replica keeps for its whole life (endpoint, file store, Raft), and
//! [`Inner`] is one term of leadership (the state machine and the tasks
//! that drive it), started when the replica becomes the leader and
//! stopped when it is not anymore. Other replicas answer agents and admin
//! calls with where the leader is.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr, SecretKey,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use pumasi_proto::{
    ADMIN_ALPN, AGENT_ALPN, BLOBS_ALPN, HA_PROTOCOL, MIN_AGENT_PROTOCOL, PROTOCOL_VERSION,
    RAFT_ALPN, Secret, blobs,
    msg::{
        AdminOp, AdminRequest, AdminResponse, AgentHello, ClusterStatus, FetchReply, FetchRequest,
        FromAgent, HelloReply, Invite, RELEASE_CHUNK, Role, ToAgent,
    },
    net::{NetMode, bind_endpoint, dialable_addr, load_or_create_key, write_private},
    perf::DiskClass,
    perf::kernel_native,
    release::SignedRelease,
    secrets_eq,
    store::{BlobHash, MAX_STORE_OUTPUT_BYTES, StoreTarget, format_size},
    ticket::{AdminTicket, JoinTicket, ReplicaTicket},
    wire::{read_frame, read_msg, write_msg},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tracing::{debug, info, warn};

use crate::{
    access,
    datasets::StoreCmd,
    files::{FileStore, Peer},
    ha::{self, Ha},
    manage::Caller,
    rollout::{ReleaseStore, release_dir},
    state::State,
    store::{Sink, Store},
};

const KEY_FILE: &str = "secret.key";
const DB_FILE: &str = "state.redb";
pub const ADMIN_TICKET_FILE: &str = "admin.ticket";
const ADMIN_SECRET_META: &str = "admin_secret";
const NAME_META: &str = "cluster_name";
/// How long a backup made for `pumasi backup` is kept for its chunks.
const BACKUP_TTL: Duration = Duration::from_secs(600);
/// Largest chunk of a backup in one answer.
const BACKUP_CHUNK: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub data_dir: PathBuf,
    /// Used only when the cluster is created; kept afterwards.
    pub name: String,
    pub net: NetMode,
    pub bind: Option<SocketAddr>,
    pub heartbeat: Duration,
    /// A node silent for this long is considered gone.
    pub node_timeout: Duration,
    /// How often the file store deletes data nothing refers to.
    pub gc_interval: Duration,
    /// How often storage is charged.
    pub storage_bill_every: Duration,
    /// The control plane's disk, which picks the storage rate.
    pub disk_class: DiskClass,
    /// Serve Prometheus metrics over HTTP on this address (opt-in).
    pub metrics_addr: Option<SocketAddr>,
    /// Require `Authorization: Bearer <token>` on `/metrics`.
    pub metrics_token: Option<String>,
    /// How long node metrics and events are kept.
    pub metrics_retention: Duration,
    /// How often a follower copies the leader's file store.
    pub mirror_every: Duration,
}

impl ClusterConfig {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            name: "my-cluster".into(),
            net: NetMode::Public,
            bind: None,
            heartbeat: Duration::from_secs(2),
            node_timeout: Duration::from_secs(10),
            gc_interval: Duration::from_secs(600),
            storage_bill_every: Duration::from_secs(3600),
            disk_class: DiskClass::Unknown,
            metrics_addr: None,
            metrics_token: None,
            metrics_retention: Duration::from_secs(7 * 86_400),
            mirror_every: Duration::from_secs(30),
        }
    }
}

/// What a control plane replica keeps for its whole life.
pub(crate) struct Shared {
    pub(crate) endpoint: Endpoint,
    pub(crate) config: ClusterConfig,
    addr: Mutex<EndpointAddr>,
    pub(crate) releases: ReleaseStore,
    pub(crate) files: FileStore,
    /// The cluster key: signs releases and receipts.
    key: SecretKey,
    /// The state database, writing directly (the state machine's side).
    store: Store,
    /// Raft, when replication is on.
    ha: RwLock<Option<Arc<Ha>>>,
    /// The services of the current term, on the leader.
    active: RwLock<Option<Arc<Inner>>>,
    /// The last backup made, and when.
    backup: Mutex<Option<CachedBackup>>,
    /// Follows Raft: starts and stops the services (replicas only).
    lead: Mutex<Option<JoinHandle<()>>>,
}

impl Shared {
    fn addr(&self) -> EndpointAddr {
        self.addr.lock().unwrap().clone()
    }

    pub(crate) fn active(&self) -> Option<Arc<Inner>> {
        self.active.read().unwrap().clone()
    }

    fn ha(&self) -> Option<Arc<Ha>> {
        self.ha.read().unwrap().clone()
    }

    /// The cluster's key as an address (with this replica's addresses
    /// when it is the replica that created the cluster), and the
    /// replicas, for tickets.
    fn ticket_addrs(&self) -> (EndpointAddr, Vec<EndpointAddr>) {
        let me = self.addr();
        let cluster_id = self.key.public();
        let Some(ha) = self.ha() else {
            return (me, Vec::new());
        };
        let mut replicas = ha.replica_addrs();
        if let Some(r) = replicas.iter_mut().find(|r| r.id == me.id) {
            *r = me.clone();
        }
        let cluster = if me.id == cluster_id {
            me
        } else {
            replicas
                .iter()
                .find(|r| r.id == cluster_id)
                .cloned()
                .unwrap_or_else(|| EndpointAddr::new(cluster_id))
        };
        (cluster, replicas)
    }

    /// Where the leader is, when this replica is not it.
    fn leader_hint(&self) -> Option<EndpointAddr> {
        self.ha().and_then(|h| h.leader_addr())
    }

    /// Who may use the file store: members of the cluster, and the other
    /// control plane replicas (they copy everything).
    fn store_peer(&self, id: &iroh::EndpointId) -> Peer {
        if self.ha().is_some_and(|h| h.is_replica(id)) && *id != self.endpoint.id() {
            return Peer {
                member: true,
                pusher: false,
            };
        }
        match self.active() {
            Some(inner) => inner.state().store_peer(id),
            None => Peer::default(),
        }
    }

    fn write_admin_ticket(&self, secret: Secret) {
        let (cluster, replicas) = self.ticket_addrs();
        let t = AdminTicket {
            cluster,
            secret,
            replicas,
        };
        if let Err(e) = write_private(
            &self.config.data_dir.join(ADMIN_TICKET_FILE),
            t.to_string().as_bytes(),
        ) {
            warn!("could not write the admin ticket: {e:#}");
        }
    }
}

/// One term of leadership: the state machine and what drives it.
pub(crate) struct Inner {
    shared: Arc<Shared>,
    state: Mutex<State>,
    admin_secret: Secret,
    /// Set when this term ends; sessions close.
    stopped: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    metrics_addr: Option<SocketAddr>,
}

impl std::ops::Deref for Inner {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        &self.shared
    }
}

impl Inner {
    pub(crate) fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("state lock poisoned")
    }

    fn ticket_addrs(&self) -> (EndpointAddr, Vec<EndpointAddr>) {
        self.shared.ticket_addrs()
    }

    /// Ends this term: sessions close, tasks stop.
    fn stop(&self) {
        let _ = self.stopped.send(true);
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

/// Starts the services of a term on `store` (writing directly on a single
/// control plane, to Raft on a leader).
async fn start_services(shared: &Arc<Shared>, store: Store) -> Result<Arc<Inner>> {
    let config = &shared.config;
    let admin_secret: Secret = match store.get_meta(ADMIN_SECRET_META)? {
        Some(b) => b
            .try_into()
            .map_err(|_| anyhow::anyhow!("corrupt admin secret"))?,
        None => {
            let s = pumasi_proto::new_secret();
            store.put_meta(ADMIN_SECRET_META, &s)?;
            s
        }
    };
    let name = match store.get_meta(NAME_META)? {
        Some(b) => String::from_utf8(b).unwrap_or_else(|_| config.name.clone()),
        None => {
            store.put_meta(NAME_META, config.name.as_bytes())?;
            config.name.clone()
        }
    };
    let mut state = State::load(
        name.clone(),
        shared.key.public().to_string(),
        store,
        shared.key.clone(),
    )?;
    state.set_metrics_retention(config.metrics_retention);
    let (stopped, _) = watch::channel(false);
    let prometheus = match config.metrics_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind the metrics address {addr}"))?;
            let local = listener.local_addr()?;
            if !local.ip().is_loopback() && config.metrics_token.is_none() {
                warn!(
                    "metrics are served on {local} without a token; anyone who reaches it sees node names and usage"
                );
            }
            info!(addr = %local, "serving Prometheus metrics on /metrics");
            Some((listener, local))
        }
        None => None,
    };
    let inner = Arc::new(Inner {
        shared: shared.clone(),
        state: Mutex::new(state),
        admin_secret,
        stopped,
        tasks: Mutex::new(Vec::new()),
        metrics_addr: prometheus.as_ref().map(|(_, a)| *a),
    });

    let mut tasks = Vec::new();
    let (store_tx, store_rx) = mpsc::unbounded_channel();
    inner
        .state()
        .attach_store(store_tx, config.disk_class, config.storage_bill_every);
    tasks.push(tokio::spawn(store_worker(inner.clone(), store_rx)));
    let (hook_tx, hook_rx) = mpsc::unbounded_channel();
    inner.state().attach_webhooks(hook_tx);
    tasks.push(tokio::spawn(crate::prometheus::webhook_worker(
        inner.clone(),
        hook_rx,
    )));
    if let Some((listener, _)) = prometheus {
        tasks.push(tokio::spawn(crate::prometheus::serve(
            inner.clone(),
            listener,
            config.metrics_token.clone(),
        )));
    }
    tasks.push({
        let inner = inner.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            let mut ticks: u64 = 0;
            loop {
                tick.tick().await;
                // The control plane's own disk, every 30 seconds.
                if ticks.is_multiple_of(60) {
                    let dir = inner.config.data_dir.clone();
                    if let Ok(disk) =
                        tokio::task::spawn_blocking(move || crate::prometheus::disk_of(&dir)).await
                    {
                        inner.state().monitor_cp_disk(disk);
                    }
                }
                ticks += 1;
                inner.state().tick(inner.config.node_timeout);
                // Keep the advertised address fresh (relay may change).
                let fresh = inner.endpoint.addr();
                if !fresh.addrs.is_empty() {
                    *inner.shared.addr.lock().unwrap() = fresh;
                }
            }
        })
    });
    *inner.tasks.lock().unwrap() = tasks;
    // The local CLI picks this up to talk to the cluster.
    shared.write_admin_ticket(admin_secret);
    *shared.active.write().unwrap() = Some(inner.clone());
    Ok(inner)
}

/// A running control plane (or control plane replica).
pub struct Cluster {
    shared: Arc<Shared>,
    router: Router,
}

/// The endpoint key of the replica in `dir`: its own when it has one, else
/// the cluster key.
fn endpoint_key(dir: &std::path::Path, cluster_key: &SecretKey) -> Result<SecretKey> {
    let path = dir.join(ha::ENDPOINT_KEY_FILE);
    if path.exists() {
        load_or_create_key(&path)
    } else {
        Ok(cluster_key.clone())
    }
}

impl Cluster {
    pub async fn start(config: ClusterConfig) -> Result<Self> {
        Self::start_on(config, None).await
    }

    async fn bind(config: &ClusterConfig, key: SecretKey) -> Result<Endpoint> {
        bind_endpoint(
            key,
            config.net,
            vec![
                AGENT_ALPN.to_vec(),
                ADMIN_ALPN.to_vec(),
                BLOBS_ALPN.to_vec(),
                RAFT_ALPN.to_vec(),
            ],
            config.bind,
        )
        .await
    }

    /// Starts on an endpoint already bound (a replica that just joined
    /// keeps the address it joined with), or binds one.
    async fn start_on(config: ClusterConfig, endpoint: Option<Endpoint>) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)
            .with_context(|| format!("create {}", config.data_dir.display()))?;
        let key = load_or_create_key(&config.data_dir.join(KEY_FILE))?;
        let store = Store::open(&config.data_dir.join(DB_FILE))?;
        let replicated = Ha::enabled_in(&config.data_dir);

        let files = FileStore::open(&config.data_dir.join("blobs"), config.gc_interval).await?;
        let endpoint = match endpoint {
            Some(e) => e,
            None => Self::bind(&config, endpoint_key(&config.data_dir, &key)?).await?,
        };
        let addr = dialable_addr(&endpoint, config.net).await;
        let shared = Arc::new(Shared {
            endpoint: endpoint.clone(),
            config: config.clone(),
            addr: Mutex::new(addr),
            releases: ReleaseStore::new(release_dir(&config.data_dir)),
            files,
            key,
            store,
            ha: RwLock::new(None),
            active: RwLock::new(None),
            backup: Mutex::new(None),
            lead: Mutex::new(None),
        });

        let blobs_protocol = {
            let gate = shared.clone();
            let egress = shared.clone();
            shared.files.protocol(
                Arc::new(move |id| gate.store_peer(id)),
                Arc::new(move |account, bytes| {
                    if let Some(inner) = egress.active() {
                        inner.state().charge_egress(account, bytes);
                    }
                }),
            )
        };
        let router = Router::builder(endpoint.clone())
            .accept(AGENT_ALPN, AgentHandler(shared.clone()))
            .accept(ADMIN_ALPN, AdminHandler(shared.clone()))
            .accept(BLOBS_ALPN, blobs_protocol)
            .accept(RAFT_ALPN, RaftHandler(shared.clone()))
            .spawn();

        let cluster = Self {
            shared: shared.clone(),
            router,
        };
        if replicated {
            let ha = Ha::start(
                &config.data_dir,
                endpoint.clone(),
                shared.addr(),
                shared.store.db().clone(),
                false,
            )
            .await?;
            *shared.ha.write().unwrap() = Some(ha.clone());
            *shared.lead.lock().unwrap() = Some(tokio::spawn(lead(shared.clone(), ha)));
            info!(id = %endpoint.id(), cluster = %shared.key.public(), "control plane replica started");
        } else {
            let inner = start_services(&shared, shared.store.with_sink(Sink::Direct)).await?;
            info!(name = %inner.state().cluster_name, id = %endpoint.id(), "control plane started");
        }
        Ok(cluster)
    }

    /// Joins an existing cluster's control plane as a new replica with a
    /// replica ticket, then runs like [`Cluster::start`] (which restarts
    /// it later). `config.data_dir` must not hold a control plane yet.
    pub async fn join_replica(config: ClusterConfig, ticket: &ReplicaTicket) -> Result<Self> {
        let dir = &config.data_dir;
        anyhow::ensure!(
            !dir.join(DB_FILE).exists() && !dir.join(KEY_FILE).exists(),
            "{} already holds a control plane; use an empty directory",
            dir.display()
        );
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let own = load_or_create_key(&dir.join(ha::ENDPOINT_KEY_FILE))?;
        // The endpoint this replica keeps: the leader records its address.
        let endpoint = Self::bind(&config, own).await?;
        let addr = dialable_addr(&endpoint, config.net).await;
        let res = ha::request_join(&endpoint, addr, &ticket.candidates(), ticket.secret).await;
        let (cluster_key, leader, replicas) = match res {
            Ok(r) => r,
            Err(e) => {
                endpoint.close().await;
                let _ = std::fs::remove_file(dir.join(ha::ENDPOINT_KEY_FILE));
                return Err(e);
            }
        };
        let key = SecretKey::from_bytes(&cluster_key);
        anyhow::ensure!(
            key.public() == ticket.cluster.id,
            "the control plane sent a key that is not the cluster's"
        );
        pumasi_proto::net::save_key(&dir.join(KEY_FILE), &key)?;
        let mut peers: Vec<iroh::EndpointId> = replicas.iter().map(|r| r.id).collect();
        peers.push(leader.id);
        Ha::prepare_join(dir, &peers)?;
        info!(leader = %leader.id.fmt_short(), "joined the control plane; catching up");
        Self::start_on(config, Some(endpoint)).await
    }

    fn inner(&self) -> Arc<Inner> {
        self.shared
            .active()
            .expect("this control plane replica is not the leader")
    }

    /// Whether this replica runs the cluster now (always, without
    /// replicas).
    pub fn is_leader(&self) -> bool {
        self.shared.active().is_some()
    }

    /// The Raft state, when replication is on.
    pub fn ha_view(&self) -> Option<pumasi_proto::ha::HaView> {
        self.shared.ha().map(|h| h.view())
    }

    /// This replica's own endpoint id.
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.shared.endpoint.id()
    }

    pub fn name(&self) -> String {
        self.inner().state().cluster_name.clone()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.shared.addr()
    }

    pub fn admin_ticket(&self) -> AdminTicket {
        let (cluster, replicas) = self.shared.ticket_addrs();
        AdminTicket {
            cluster,
            secret: self.inner().admin_secret,
            replicas,
        }
    }

    pub fn create_join_ticket(&self, ttl: Duration, uses: Option<u32>) -> Result<JoinTicket> {
        self.create_invite(ttl, uses, None)
    }

    /// A join ticket; with `invite`, for someone else's machine joining as
    /// a provider.
    pub fn create_invite(
        &self,
        ttl: Duration,
        uses: Option<u32>,
        invite: Option<Invite>,
    ) -> Result<JoinTicket> {
        let token = self.inner().state().create_token(ttl, uses, invite)?;
        let (cluster, replicas) = self.shared.ticket_addrs();
        Ok(JoinTicket {
            cluster,
            secret: token.secret,
            replicas,
        })
    }

    pub fn status(&self) -> ClusterStatus {
        self.inner().state().status()
    }

    /// Where uploaded release binaries are kept.
    pub fn release_store(&self) -> ReleaseStore {
        self.shared.releases.clone()
    }

    /// Sends a release to a node as is. For tests of forged releases.
    #[doc(hidden)]
    pub fn send_release_for_tests(&self, node: &str, release: SignedRelease) -> Result<()> {
        self.inner().state().send_release_for_tests(node, release)
    }

    /// Bytes the file store takes on disk.
    pub fn store_disk_bytes(&self) -> u64 {
        self.shared.files.disk_bytes()
    }

    /// Where `/metrics` is served, when it is.
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.shared.active().and_then(|i| i.metrics_addr)
    }

    /// The Prometheus text `/metrics` serves.
    pub fn prometheus_text(&self) -> String {
        self.inner().state().prometheus()
    }

    /// Alerts sent to webhooks lately (for tests).
    pub fn recent_notices(&self) -> Vec<crate::monitor::Notice> {
        self.inner().state().recent_notices()
    }

    /// Stops at once without saving anything pending, like a crash or a
    /// power cut: writes not yet committed are lost. For tests.
    #[doc(hidden)]
    pub async fn crash(self) {
        let lead_task = self.shared.lead.lock().unwrap().take();
        if let Some(l) = lead_task {
            l.abort();
            let _ = l.await;
        }
        if let Some(inner) = self.shared.active.write().unwrap().take() {
            inner.stop();
        }
        let ha = self.shared.ha.write().unwrap().take();
        if let Some(ha) = ha {
            ha.shutdown().await;
        }
        self.shared.endpoint.close().await;
        let _ = self.router.shutdown().await;
        self.shared.files.store.shutdown().await.ok();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&self.shared) > 1 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        let lead_task = self.shared.lead.lock().unwrap().take();
        if let Some(l) = lead_task {
            l.abort();
            let _ = l.await;
        }
        let active = self.shared.active.write().unwrap().take();
        if let Some(inner) = active {
            inner.stop();
            let tasks: Vec<_> = inner.tasks.lock().unwrap().drain(..).collect();
            for t in tasks {
                let _ = t.await;
            }
            // Pending metrics and events (with replicas, only if this is
            // still the leader: Raft takes them while it runs).
            inner.state().flush_monitor();
            if let Some(ha) = self.shared.ha() {
                // Give the last writes a moment to commit.
                let _ = tokio::time::timeout(Duration::from_secs(2), ha.raft.ensure_linearizable())
                    .await;
            }
            drop(inner);
        }
        let ha = self.shared.ha.write().unwrap().take();
        if let Some(ha) = ha {
            ha.shutdown().await;
        }
        self.router.shutdown().await.context("router shutdown")?;
        self.shared.files.store.shutdown().await.ok();
        // Tasks of ended sessions let go of the state as their connections
        // close; the database stays locked until the last one has.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&self.shared) > 1 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

/// Turns replication on for a running single control plane: Raft starts
/// with this replica alone, the state machine's writes go through it from
/// now on, and a snapshot covers everything from before.
async fn enable_ha(inner: &Arc<Inner>) -> Result<Arc<Ha>> {
    let shared = &inner.shared;
    if let Some(ha) = shared.ha() {
        return Ok(ha);
    }
    let ha = Ha::start(
        &shared.config.data_dir,
        shared.endpoint.clone(),
        shared.addr(),
        shared.store.db().clone(),
        true,
    )
    .await?;
    // Wait until this replica leads its cluster of one.
    let mut m = ha.raft.metrics();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !ha.is_leader() {
            if m.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .context("timed out waiting for the first election")?;
    let (sink, rx) = crate::store::pipe();
    inner
        .tasks
        .lock()
        .unwrap()
        .push(tokio::spawn(ha.clone().replicate(rx)));
    {
        let mut st = inner.state();
        let store = st.store.with_sink(sink);
        st.store = store;
    }
    // Whatever was written before is in the database but in no log entry:
    // new replicas must start from a snapshot of it.
    ha.compact().await?;
    *shared.ha.write().unwrap() = Some(ha.clone());
    *shared.lead.lock().unwrap() = Some(tokio::spawn(lead(shared.clone(), ha.clone())));
    shared.write_admin_ticket(inner.admin_secret);
    Ok(ha)
}

/// Follows Raft on a replica: serves while this replica is the leader and
/// a majority answers it, stops serving otherwise; copies the file store
/// while following.
async fn lead(shared: Arc<Shared>, ha: Arc<Ha>) {
    let mut metrics = ha.raft.metrics();
    let mut last_quorum = Instant::now();
    let mut last_check: Option<Instant> = None;
    let mut last_mirror: Option<Instant> = None;
    let mut replicas: Vec<EndpointAddr> = Vec::new();
    let mut check = tokio::time::interval(Duration::from_millis(500));
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let leader = ha.is_leader();
        let active = shared.active();
        if leader && last_check.is_none_or(|t| t.elapsed() >= Duration::from_millis(450)) {
            // A leader cut off from the majority must stop serving before
            // the others elect a new one: confirm it with a heartbeat
            // round to a majority.
            last_check = Some(Instant::now());
            let ok = tokio::time::timeout(Duration::from_secs(1), ha.raft.get_read_log_id())
                .await
                .is_ok_and(|r| r.is_ok());
            if ok {
                last_quorum = Instant::now();
            }
        } else if !leader {
            last_check = None;
        }
        let serve = leader && last_quorum.elapsed() < ha::LEASE;
        match (serve, active) {
            (true, None) => {
                // Everything committed must be applied before the state
                // machine loads.
                match tokio::time::timeout(Duration::from_secs(10), ha.raft.ensure_linearizable())
                    .await
                {
                    Ok(Ok(_)) => {
                        // Replicating before the state loads: loading
                        // writes, and reads wait for those writes.
                        let (sink, rx) = crate::store::pipe();
                        let replicate = tokio::spawn(ha.clone().replicate(rx));
                        match start_services(&shared, shared.store.with_sink(sink)).await {
                            Ok(inner) => {
                                inner.tasks.lock().unwrap().push(replicate);
                                replicas.clear();
                                info!(
                                    term = ha.metrics().current_term,
                                    "this replica leads the control plane"
                                );
                            }
                            Err(e) => {
                                replicate.abort();
                                warn!("could not start the control plane: {e:#}")
                            }
                        }
                    }
                    Ok(Err(e)) => debug!("not ready to lead: {e}"),
                    Err(_) => debug!("not ready to lead: timed out"),
                }
            }
            (false, Some(inner)) => {
                *shared.active.write().unwrap() = None;
                inner.stop();
                warn!("this replica stopped leading the control plane");
            }
            (true, Some(inner)) => {
                let now = ha.replica_addrs();
                if now != replicas {
                    inner.state().send_replicas(&now);
                    shared.write_admin_ticket(inner.admin_secret);
                    replicas = now;
                }
            }
            (false, None) => {
                let due = last_mirror.is_none_or(|t| t.elapsed() >= shared.config.mirror_every);
                if due
                    && let Some(leader) = ha.leader_addr()
                    && leader.id != shared.endpoint.id()
                {
                    last_mirror = Some(Instant::now());
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        match ha::mirror::sync(&shared.endpoint, &shared.files, leader).await {
                            Ok(0) => {}
                            Ok(n) => info!(collections = n, "copied from the leader's file store"),
                            Err(e) => debug!("copying the leader's file store: {e:#}"),
                        }
                        // Keep a local admin ticket that works with any
                        // replica once the state has the admin secret.
                        if let Ok(Some(b)) = shared.store.get_meta(ADMIN_SECRET_META)
                            && let Ok(secret) = <Secret>::try_from(b)
                        {
                            shared.write_admin_ticket(secret);
                        }
                    });
                }
            }
        }
        tokio::select! {
            _ = metrics.changed() => {}
            _ = check.tick() => {}
        }
    }
}

/// Lists the file store's tags for followers.
#[derive(Clone)]
struct Tags(Arc<Shared>);

impl ha::AsyncTags for Tags {
    async fn list(&self) -> Result<Vec<(String, BlobHash)>> {
        self.0.files.tags().await
    }
}

#[derive(Clone)]
struct RaftHandler(Arc<Shared>);

impl std::fmt::Debug for RaftHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RaftHandler")
    }
}

impl ProtocolHandler for RaftHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let Some(ha) = self.0.ha() else {
            conn.close(1u32.into(), b"replication is off");
            return Ok(());
        };
        let key = self.0.key.to_bytes();
        if let Err(e) = ha.serve(conn, key, Tags(self.0.clone())).await {
            debug!("raft connection ended: {e:#}");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Agent protocol
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AgentHandler(Arc<Shared>);

impl std::fmt::Debug for AgentHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AgentHandler")
    }
}

impl ProtocolHandler for AgentHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        if let Err(e) = agent_session(self.0.clone(), conn).await {
            debug!("agent session ended: {e:#}");
        }
        Ok(())
    }
}

async fn agent_session(shared: Arc<Shared>, conn: Connection) -> Result<()> {
    let node_id = conn.remote_id().to_string();
    let (mut send, mut recv) = conn.accept_bi().await?;
    let Some(frame) = read_frame(&mut recv).await? else {
        return Ok(());
    };
    let hello = AgentHello::decode(&frame).context("decode hello")?;
    let Some(inner) = shared.active() else {
        // A replica that is not the leader: say where it is to agents that
        // understand, close on older ones (they retry).
        if hello.protocol_version >= HA_PROTOCOL {
            let (_, replicas) = shared.ticket_addrs();
            write_msg(
                &mut send,
                &HelloReply::NotLeader {
                    leader: shared.leader_hint(),
                    replicas,
                },
            )
            .await?;
            send.finish()?;
            let _ = tokio::time::timeout(Duration::from_secs(2), conn.closed()).await;
        } else {
            conn.close(2u32.into(), b"not the leader");
        }
        return Ok(());
    };
    let mut stopped = inner.stopped.subscribe();
    if *stopped.borrow() {
        return Ok(());
    }

    let verdict: Result<(Role, String)> = if hello.protocol_version > PROTOCOL_VERSION {
        Err(anyhow::anyhow!(
            "protocol version {} not supported (cluster speaks {PROTOCOL_VERSION}); upgrade the control plane first",
            hello.protocol_version
        ))
    } else if hello.protocol_version < MIN_AGENT_PROTOCOL {
        Err(anyhow::anyhow!(
            "protocol version {} not supported (cluster speaks {MIN_AGENT_PROTOCOL} to {PROTOCOL_VERSION}); upgrade the agent",
            hello.protocol_version
        ))
    } else {
        inner
            .state()
            .authorize(&node_id, &hello.node, hello.join_secret.as_ref())
    };
    let (role, account) = match verdict {
        Ok(v) => v,
        Err(e) => {
            warn!(node = %node_id, "rejected node: {e:#}");
            write_msg(
                &mut send,
                &HelloReply::Rejected {
                    reason: format!("{e:#}"),
                },
            )
            .await?;
            send.finish()?;
            // Give the reply a moment to flush before the connection drops.
            let _ = tokio::time::timeout(Duration::from_secs(2), conn.closed()).await;
            return Ok(());
        }
    };

    let cluster_name = inner.state().cluster_name.clone();
    write_msg(
        &mut send,
        &HelloReply::Welcome {
            cluster_name,
            heartbeat_ms: inner.config.heartbeat.as_millis() as u64,
            role,
            account: account.clone(),
        },
    )
    .await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<ToAgent>();
    if hello.protocol_version >= HA_PROTOCOL {
        let (_, replicas) = inner.ticket_addrs();
        if !replicas.is_empty() {
            let _ = tx.send(ToAgent::Replicas(replicas));
        }
    }
    let (kill_tx, mut kill_rx) = oneshot::channel();
    let generation = inner.state().on_connect(
        &node_id,
        hello.protocol_version,
        hello.node.clone(),
        hello.upgrade.clone(),
        hello.availability.clone(),
        tx,
        kill_tx,
    );
    // Release downloads come on their own streams of this connection.
    let fetcher = tokio::spawn(serve_fetches(inner.clone(), conn.clone(), node_id.clone()));
    info!(node = %node_id, name = %hello.node.name, slots = hello.node.slots, %role, %account, "node online");

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write_msg(&mut send, &msg).await {
                debug!("write to agent failed: {e:#}");
                break;
            }
        }
    });

    let result: Result<()> = loop {
        tokio::select! {
            msg = read_msg::<_, FromAgent>(&mut recv) => match msg {
                Ok(Some(FromAgent::Heartbeat { .. })) => {
                    let (rtt, relayed) = path_rtt(&conn);
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_rtt(&node_id, generation, rtt, relayed);
                }
                Ok(Some(FromAgent::Perf(p))) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    if let Err(e) = st.on_perf(&node_id, generation, *p) {
                        warn!(node = %node_id, "rejected performance profile: {e:#}");
                    }
                }
                Ok(Some(FromAgent::BenchResult { nonce, result })) => {
                    let taken = {
                        let mut st = inner.state();
                        st.on_heartbeat(&node_id, generation);
                        st.take_challenge(&node_id, generation, nonce)
                    };
                    if let Some((challenge, took)) = taken {
                        // Recompute the answer off the state lock: it takes
                        // about as long as the node's run.
                        let inner = inner.clone();
                        let node_id = node_id.clone();
                        tokio::spawn(async move {
                            let b = challenge.bench;
                            let expect = tokio::task::spawn_blocking(move || {
                                kernel_native(b.seed, b.iters)
                            })
                            .await;
                            if let Ok(expect) = expect {
                                inner.state().apply_challenge(
                                    &node_id,
                                    &challenge,
                                    took,
                                    expect == result,
                                );
                            }
                        });
                    }
                }
                Ok(Some(FromAgent::Done(report))) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_report(&node_id, generation, *report);
                }
                Ok(Some(FromAgent::Report { report, stored })) => {
                    // A stored output must be complete in the store and be
                    // what the report says before the result counts.
                    let verdict: Result<(), String> = match &stored {
                        None => Ok(()),
                        Some(s) if s.size > MAX_STORE_OUTPUT_BYTES => Err(format!(
                            "stored output of {} is over the {} limit",
                            format_size(s.size),
                            format_size(MAX_STORE_OUTPUT_BYTES)
                        )),
                        Some(s) => match inner.files.stored(&s.root).await {
                            Ok(got) if got == *s => Ok(()),
                            Ok(got) => Err(format!(
                                "the stored output has {} files of {} bytes, not {} of {}",
                                got.files, got.size, s.files, s.size
                            )),
                            Err(e) => Err(format!("stored output: {e:#}")),
                        },
                    };
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    match verdict {
                        Ok(()) => st.on_report_stored(&node_id, generation, *report, stored),
                        Err(why) => st.on_report_rejected(&node_id, generation, report.task, &why),
                    }
                }
                Ok(Some(FromAgent::StoreState { addr, full, added, removed })) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_store_state(&node_id, generation, addr, full, added, removed);
                }
                Ok(Some(FromAgent::UpgradeFailed { sha256, reason })) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_upgrade_failed(&node_id, generation, sha256, reason);
                }
                Ok(Some(FromAgent::Devices(d))) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    if let Err(e) = st.on_devices(&node_id, generation, *d) {
                        warn!(node = %node_id, "rejected device profile: {e:#}");
                    }
                }
                Ok(Some(FromAgent::GpuResult { nonce, solutions })) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_gpu_result(&node_id, generation, nonce, solutions);
                }
                Ok(Some(FromAgent::Availability(a))) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_availability(&node_id, generation, a);
                }
                Ok(Some(FromAgent::Metrics(m))) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_metrics(&node_id, generation, *m);
                }
                Ok(Some(FromAgent::Logs { task, stdout, stderr })) => {
                    let mut st = inner.state();
                    st.on_heartbeat(&node_id, generation);
                    st.on_logs(&node_id, generation, task, stdout, stderr);
                }
                Ok(None) => break Ok(()),
                Err(e) => break Err(e),
            },
            _ = &mut kill_rx => break Ok(()),
            _ = stopped.changed() => break Ok(()),
        }
    };

    inner.state().on_disconnect(&node_id, generation);
    info!(node = %node_id, "node offline");
    writer.abort();
    fetcher.abort();
    conn.close(0u32.into(), b"session ended");
    // Wait until the fetcher has let go of the state, so a shutdown that
    // waits for sessions really releases the database.
    let _ = fetcher.await;
    result
}

/// Round-trip time of the connection's selected path, and whether that
/// path goes through a relay.
fn path_rtt(conn: &Connection) -> (Option<Duration>, bool) {
    let paths = conn.paths();
    paths
        .iter()
        .find(|p| p.is_selected())
        .map(|p| (Some(p.rtt()), p.is_relay()))
        .unwrap_or((None, false))
}

/// Serves chunks of release binaries to an agent: one [`FetchRequest`] and
/// one [`FetchReply`] per stream.
async fn serve_fetches(inner: Arc<Inner>, conn: Connection, node_id: String) {
    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
        let inner = inner.clone();
        let node_id = node_id.clone();
        tokio::spawn(async move {
            let res: Result<()> = async {
                let Some(req) = read_msg::<_, FetchRequest>(&mut recv).await? else {
                    return Ok(());
                };
                let reply = if !inner.state().may_fetch(&node_id, &req.sha256) {
                    FetchReply::Error("not a binary of the current rollout".into())
                } else {
                    let store = inner.releases.clone();
                    let len = req.len.min(RELEASE_CHUNK);
                    match tokio::task::spawn_blocking(move || {
                        store.read(&req.sha256, req.offset, len)
                    })
                    .await?
                    {
                        Ok(bytes) => FetchReply::Data(bytes),
                        Err(e) => FetchReply::Error(format!("{e:#}")),
                    }
                };
                write_msg(&mut send, &reply).await?;
                send.finish()?;
                Ok(())
            }
            .await;
            if let Err(e) = res {
                debug!("release fetch failed: {e:#}");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Admin protocol
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AdminHandler(Arc<Shared>);

impl std::fmt::Debug for AdminHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdminHandler")
    }
}

impl ProtocolHandler for AdminHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(s) => s,
                Err(_) => return Ok(()),
            };
            let shared = self.0.clone();
            tokio::spawn(async move {
                let res: Result<()> = async {
                    let Some(req) = read_msg::<_, AdminRequest>(&mut recv).await? else {
                        return Ok(());
                    };
                    let Some(inner) = shared.active() else {
                        // Nothing done: the client asks the leader instead.
                        write_msg(
                            &mut send,
                            &AdminResponse::NotLeader {
                                leader: shared.leader_hint(),
                            },
                        )
                        .await?;
                        send.finish()?;
                        return Ok(());
                    };
                    let caller = if secrets_eq(&req.secret, &inner.admin_secret) {
                        Some(Caller::Admin {
                            name: "cluster admin".into(),
                        })
                    } else {
                        inner.state().access_caller(&req.secret)
                    };
                    let resp = match caller {
                        None => AdminResponse::Error(
                            "invalid admin credentials (a wrong, expired or revoked ticket)".into(),
                        ),
                        Some(caller) => {
                            let admitted = {
                                let st = inner.state();
                                access::admit(&st, &caller, req.op)
                            };
                            let resp = match admitted {
                                Err(e) => AdminResponse::Error(format!("{e:#}")),
                                Ok(op) => match handle_store_admin(&inner, op).await {
                                    Ok(r) => {
                                        r.unwrap_or_else(|e| AdminResponse::Error(format!("{e:#}")))
                                    }
                                    Err(op) => handle_admin(&inner, &caller, op),
                                },
                            };
                            access::scope(&caller, resp)
                        }
                    };
                    write_msg(&mut send, &resp).await?;
                    send.finish()?;
                    Ok(())
                }
                .await;
                if let Err(e) = res {
                    debug!("admin request failed: {e:#}");
                }
            });
        }
    }
}

/// Runs the file store worker: tags, image pulls and output datasets, in
/// the order the state machine asked for them.
async fn store_worker(inner: Arc<Inner>, mut rx: mpsc::UnboundedReceiver<StoreCmd>) {
    while let Some(cmd) = rx.recv().await {
        let files = &inner.files;
        let res = match cmd {
            StoreCmd::Tag(name, root) => files.tag(&name, blobs::to_hash(&root)).await,
            StoreCmd::Untag(name) => files.untag(&name).await,
            StoreCmd::UntagPrefix(prefix) => files.untag_prefix(&prefix).await,
            StoreCmd::Reconcile(tags) => files.reconcile(&tags).await,
            StoreCmd::PrepareImage {
                job,
                arch,
                reference,
            } => {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let creds = inner.state().registry_credentials();
                    let owner = format!("{job}/{arch}");
                    let r = inner
                        .files
                        .pull_image(&reference, &arch, &owner, creds)
                        .await;
                    inner.state().on_image_ready(job, &arch, r);
                });
                Ok(())
            }
            StoreCmd::FinishJob { job, parts } => {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let r = inner.files.combine(&parts, &format!("job/{job}")).await;
                    inner.state().on_job_output(job, r);
                });
                Ok(())
            }
        };
        if let Err(e) = res {
            warn!("file store: {e:#}");
        }
    }
}

fn endpoint_id(bytes: &[u8; 32]) -> Result<iroh::EndpointId> {
    iroh::EndpointId::from_bytes(bytes).context("bad endpoint id")
}

/// File store admin calls, which wait on the store. Other calls come back
/// as `Err` for [`handle_admin`].
async fn handle_store_admin(
    inner: &Arc<Inner>,
    op: AdminOp,
) -> Result<Result<AdminResponse>, AdminOp> {
    let files = &inner.files;
    Ok(match op {
        AdminOp::StoreUpload { uploader, root } => {
            async {
                let who = endpoint_id(&uploader)?;
                let hash = blobs::to_hash(&root);
                files.allow_upload(who, hash);
                let (missing, have) = blobs::missing(files.api(), hash).await?;
                Ok(AdminResponse::StoreMissing { missing, have })
            }
            .await
        }
        AdminOp::StoreCommit {
            root,
            name,
            account,
        } => {
            async {
                let stored = files.stored(&root).await?;
                let view = inner.state().commit_upload(stored, name, account)?;
                Ok(match view {
                    Some(v) => AdminResponse::Datasets(vec![v]),
                    None => AdminResponse::Ok,
                })
            }
            .await
        }
        AdminOp::StoreList { name } => Ok(AdminResponse::Datasets(
            inner.state().list_datasets(name.as_deref()),
        )),
        AdminOp::StoreRemove { name, version } => inner
            .state()
            .remove_datasets(&name, version)
            .map(AdminResponse::Datasets),
        AdminOp::StoreOpen { reader, target } => (|| {
            let who = endpoint_id(&reader)?;
            let (account, items) = inner.state().open_target(&target)?;
            if let StoreTarget::Job { .. } = target {
                anyhow::ensure!(!items.is_empty(), "the job has no outputs in the store");
            }
            let roots: Vec<_> = items.iter().map(|i| blobs::to_hash(&i.root)).collect();
            files.allow_read(who, &account, &roots);
            Ok(AdminResponse::StoreItems(items))
        })(),
        AdminOp::StoreUsage => {
            let usage = inner.state().store_usage();
            let f = files.clone();
            let disk_bytes = tokio::task::spawn_blocking(move || f.disk_bytes())
                .await
                .unwrap_or(0);
            Ok(AdminResponse::StoreUsage { usage, disk_bytes })
        }
        AdminOp::StoreQuota { account, bytes } => inner
            .state()
            .set_quota(&account, bytes)
            .map(|()| AdminResponse::Ok),
        AdminOp::SecretSet { name, value } => inner
            .state()
            .set_secret(&name, &value)
            .map(|()| AdminResponse::Ok),
        AdminOp::SecretList => Ok(AdminResponse::Secrets(inner.state().secrets())),
        AdminOp::SecretRemove { name } => inner
            .state()
            .remove_secret(&name)
            .map(|()| AdminResponse::Ok),
        AdminOp::GetStoragePolicy => {
            let (policy, class) = inner.state().storage_policy();
            Ok(AdminResponse::StoragePolicy { policy, class })
        }
        AdminOp::SetStoragePolicy(p) => {
            let mut st = inner.state();
            st.set_storage_policy(p).map(|()| {
                let (policy, class) = st.storage_policy();
                AdminResponse::StoragePolicy { policy, class }
            })
        }
        AdminOp::TestWebhook { name } => {
            async {
                let (url, body) = inner.state().test_delivery(&name)?;
                let (tx, rx) = oneshot::channel();
                let sent = crate::monitor::Delivery {
                    webhook: name.clone(),
                    url,
                    body,
                    reply: Some(tx),
                };
                crate::prometheus::deliver(inner.clone(), sent);
                match rx.await {
                    Ok(Ok(())) => Ok(AdminResponse::Ok),
                    Ok(Err(e)) => Err(anyhow::anyhow!("webhook {name}: {e}")),
                    Err(_) => Err(anyhow::anyhow!("webhook {name}: no answer")),
                }
            }
            .await
        }
        AdminOp::HaStatus => Ok(AdminResponse::Ha(Box::new(match inner.ha() {
            Some(ha) => ha.view(),
            None => pumasi_proto::ha::HaView {
                enabled: false,
                leader: inner.endpoint.id().to_string(),
                replicas: vec![pumasi_proto::ha::ReplicaView {
                    id: inner.endpoint.id().to_string(),
                    voter: true,
                    leader: true,
                    matched: None,
                    last_seen_ms: Some(0),
                }],
                ..Default::default()
            },
        }))),
        AdminOp::HaInvite { ttl_secs } => {
            async {
                let ttl = Duration::from_secs(ttl_secs.clamp(60, 7 * 86_400));
                let ha = enable_ha(inner).await?;
                let secret = ha.invite(ttl);
                let (cluster, replicas) = inner.ticket_addrs();
                let ticket = ReplicaTicket {
                    cluster,
                    secret,
                    replicas,
                };
                Ok(AdminResponse::JoinToken {
                    ticket: ticket.to_string(),
                    expires_unix: crate::state::now_unix() + ttl.as_secs(),
                })
            }
            .await
        }
        AdminOp::HaRemove { id } => {
            async {
                let ha = inner.ha().context("this control plane has no replicas")?;
                ha.remove(&id).await?;
                Ok(AdminResponse::Ha(Box::new(ha.view())))
            }
            .await
        }
        AdminOp::Backup { offset } => {
            async {
                let (bytes, digest) = backup_bytes(&inner.shared, offset == 0).await?;
                let start = (offset as usize).min(bytes.len());
                let end = (start + BACKUP_CHUNK).min(bytes.len());
                Ok(AdminResponse::Backup {
                    total: bytes.len() as u64,
                    digest,
                    bytes: bytes[start..end].to_vec(),
                })
            }
            .await
        }
        other => return Err(other),
    })
}

/// The current backup: made anew when `fresh` or when the last one is
/// old.
async fn backup_bytes(shared: &Arc<Shared>, fresh: bool) -> Result<(Arc<Vec<u8>>, [u8; 32])> {
    {
        let b = shared.backup.lock().unwrap();
        if let Some((made, bytes, digest)) = &*b
            && !fresh
            && made.elapsed() < BACKUP_TTL
        {
            return Ok((bytes.clone(), *digest));
        }
    }
    let db = shared.store.db().clone();
    let key = shared.key.to_bytes();
    let bytes = tokio::task::spawn_blocking(move || crate::backup::make(&db, key)).await??;
    let digest = *blake3::hash(&bytes).as_bytes();
    let bytes = Arc::new(bytes);
    *shared.backup.lock().unwrap() = Some((Instant::now(), bytes.clone(), digest));
    Ok((bytes, digest))
}

fn handle_admin(inner: &Inner, caller: &Caller, op: AdminOp) -> AdminResponse {
    let result: Result<AdminResponse> = (|| match op {
        AdminOp::CreateJoinToken {
            ttl_secs,
            uses,
            invite,
        } => {
            let token = inner
                .state()
                .create_token(Duration::from_secs(ttl_secs), uses, invite)?;
            let (cluster, replicas) = inner.ticket_addrs();
            let ticket = JoinTicket {
                cluster,
                secret: token.secret,
                replicas,
            };
            Ok(AdminResponse::JoinToken {
                ticket: ticket.to_string(),
                expires_unix: token.expires_unix,
            })
        }
        AdminOp::Submit(sub) => {
            let submitter = matches!(caller, Caller::Submitter { .. });
            let id = inner.state().submit_from(*sub, submitter, None)?;
            Ok(AdminResponse::Submitted { id })
        }
        AdminOp::Status => Ok(AdminResponse::Status(inner.state().status())),
        AdminOp::Job { id, with_output } => match inner.state().job_view(id, with_output) {
            Some(v) => Ok(AdminResponse::Job(Box::new(v))),
            None => Ok(AdminResponse::Error(format!("no job {id}"))),
        },
        AdminOp::TaskFiles { id, index } => {
            Ok(AdminResponse::Files(inner.state().task_files(id, index)?))
        }
        AdminOp::Cancel { id } => {
            inner.state().cancel(id)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::RemoveNode { id } => {
            inner.state().remove_node(&id)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::Receipts { id } => Ok(AdminResponse::Receipts(inner.state().receipts(id)?)),
        AdminOp::Accounts => Ok(AdminResponse::Accounts(inner.state().accounts())),
        AdminOp::Ledger { account, limit } => Ok(AdminResponse::Ledger(
            inner
                .state()
                .ledger(account.as_deref(), limit.clamp(1, 10_000) as usize)?,
        )),
        AdminOp::Deposit {
            account,
            amount,
            memo,
        } => {
            let a = inner.state().deposit(&account, amount, &memo)?;
            Ok(AdminResponse::Accounts(vec![a]))
        }
        AdminOp::Payout {
            account,
            amount,
            memo,
        } => {
            let a = inner.state().payout(&account, amount, &memo)?;
            Ok(AdminResponse::Accounts(vec![a]))
        }
        AdminOp::Benchmark { id } => {
            inner.state().request_benchmark(&id)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::GetPolicy => Ok(AdminResponse::Policy(inner.state().policy())),
        AdminOp::SetPolicy(p) => {
            let mut st = inner.state();
            st.set_policy(p)?;
            Ok(AdminResponse::Policy(st.policy()))
        }
        AdminOp::UploadRelease {
            sha256,
            size,
            offset,
            bytes,
        } => {
            anyhow::ensure!(bytes.len() <= RELEASE_CHUNK as usize, "chunk too large");
            let have = inner.releases.upload(&sha256, size, offset, &bytes)?;
            Ok(AdminResponse::Uploaded { have })
        }
        AdminOp::Rollout { files, force } => {
            let mut st = inner.state();
            let view = st.start_rollout(files, force, |f| inner.releases.has(&f.sha256, f.size))?;
            inner.releases.prune(&st.rollout_hashes());
            Ok(AdminResponse::Rollout(view))
        }
        AdminOp::RolloutStatus => Ok(AdminResponse::Rollout(inner.state().rollout_view())),
        AdminOp::CancelRollout => {
            inner.state().cancel_rollout()?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::EditNode { node, edit } => {
            inner.state().edit_node(&node, &edit)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::Drain { node } => Ok(AdminResponse::Drained {
            moved: inner.state().drain(&node)?,
        }),
        AdminOp::SetQuota { account, quota } => {
            inner.state().set_account_quota(&account, quota)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::Quotas => Ok(AdminResponse::Quotas(inner.state().quotas())),
        AdminOp::CreateAccess {
            name,
            role,
            ttl_secs,
        } => {
            let r = inner
                .state()
                .create_access(&name, role, ttl_secs.map(Duration::from_secs))?;
            let (cluster, replicas) = inner.ticket_addrs();
            let ticket = AdminTicket {
                cluster,
                secret: r.secret,
                replicas,
            };
            Ok(AdminResponse::AccessTicket {
                ticket: ticket.to_string(),
                view: r.view(),
            })
        }
        AdminOp::ListAccess => Ok(AdminResponse::AccessList(inner.state().list_access())),
        AdminOp::RevokeAccess { id } => {
            Ok(AdminResponse::AccessList(inner.state().revoke_access(&id)?))
        }
        AdminOp::WhoAmI => Ok(AdminResponse::WhoAmI {
            name: caller.name().to_string(),
            role: caller.role(),
        }),
        AdminOp::SetSchedule(spec) => {
            inner.state().set_schedule(*spec)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::Schedules => Ok(AdminResponse::Schedules(inner.state().schedules())),
        AdminOp::RemoveSchedule { name } => {
            inner.state().remove_schedule(&name)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::RunSchedule { name } => Ok(AdminResponse::Submitted {
            id: inner.state().run_schedule(&name, true)?,
        }),
        AdminOp::Apply(spec) => Ok(AdminResponse::Applied(inner.state().apply(*spec)?)),
        AdminOp::Metrics { node, since_secs } => Ok(AdminResponse::Metrics(Box::new(
            inner.state().metrics_view(node.as_deref(), since_secs)?,
        ))),
        AdminOp::Events {
            after,
            since_unix,
            limit,
        } => Ok(AdminResponse::Events(
            inner.state().events(after, since_unix, limit),
        )),
        AdminOp::Logs {
            job,
            index,
            attempt,
            stdout_from,
            stderr_from,
        } => Ok(AdminResponse::Logs(Box::new(inner.state().logs(
            job,
            index,
            attempt,
            stdout_from,
            stderr_from,
        )?))),
        AdminOp::Alerts => Ok(AdminResponse::Alerts(Box::new(inner.state().alerts_view()))),
        AdminOp::SetAlertPolicy(p) => {
            let mut st = inner.state();
            st.set_alert_policy(*p)?;
            Ok(AdminResponse::Alerts(Box::new(st.alerts_view())))
        }
        AdminOp::SetWebhook(w) => {
            inner.state().set_webhook(*w)?;
            Ok(AdminResponse::Ok)
        }
        AdminOp::RemoveWebhook { name } => {
            inner.state().remove_webhook(&name)?;
            Ok(AdminResponse::Ok)
        }
        other => anyhow::bail!("unexpected admin call {other:?}"),
    })();
    result.unwrap_or_else(|e| AdminResponse::Error(format!("{e:#}")))
}

// ---------------------------------------------------------------------------
// Admin client
// ---------------------------------------------------------------------------

/// Client for the admin protocol, used by the CLI and tests.
///
/// With control plane replicas it tries the ticket's addresses until one
/// answers and follows a replica that is not the leader to the leader. A
/// call is sent again only when it certainly did not run: the replica
/// said it is not the leader, or the call could not be sent at all.
pub struct AdminClient {
    pub(crate) endpoint: Endpoint,
    conn: Arc<Mutex<Connection>>,
    secret: Secret,
    /// The ticket's cluster address: its id is the cluster key.
    pub(crate) cluster: EndpointAddr,
    /// Every address worth trying, newest knowledge first.
    candidates: Arc<Mutex<Vec<EndpointAddr>>>,
}

/// Connects to the first of `candidates` that answers on the admin ALPN.
async fn connect_any(endpoint: &Endpoint, candidates: &[EndpointAddr]) -> Result<Connection> {
    let per = if candidates.len() > 1 {
        Duration::from_secs(8)
    } else {
        Duration::from_secs(20)
    };
    let mut last = anyhow::anyhow!("the ticket names no control plane");
    for addr in candidates {
        match tokio::time::timeout(per, endpoint.connect(addr.clone(), ADMIN_ALPN)).await {
            Ok(Ok(c)) => return Ok(c),
            Ok(Err(e)) => last = anyhow::Error::from(e).context("could not connect to the cluster"),
            Err(_) => last = anyhow::anyhow!("timed out connecting to the cluster"),
        }
    }
    Err(last)
}

/// A backup made, when, and its digest.
type CachedBackup = (Instant, Arc<Vec<u8>>, [u8; 32]);

/// How sending one call went.
enum Sent {
    Answered(AdminResponse),
    /// The call never left: safe to send elsewhere.
    Unsent(anyhow::Error),
    /// It may have run; the answer was lost.
    Lost(anyhow::Error),
}

async fn send_call(conn: &Connection, req: &AdminRequest) -> Sent {
    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(s) => s,
        Err(e) => return Sent::Unsent(e.into()),
    };
    if let Err(e) = write_msg(&mut send, req).await {
        return Sent::Unsent(e);
    }
    if let Err(e) = send.finish() {
        return Sent::Lost(e.into());
    }
    match read_msg::<_, AdminResponse>(&mut recv).await {
        Ok(Some(r)) => Sent::Answered(r),
        Ok(None) => Sent::Lost(anyhow::anyhow!(
            "cluster closed the stream without replying"
        )),
        Err(e) => Sent::Lost(e),
    }
}

impl AdminClient {
    pub async fn connect(ticket: &AdminTicket, net: NetMode) -> Result<Self> {
        let endpoint = bind_endpoint(iroh::SecretKey::generate(), net, vec![], None).await?;
        Self::connect_with(endpoint, ticket).await
    }

    pub async fn connect_with(endpoint: Endpoint, ticket: &AdminTicket) -> Result<Self> {
        let candidates = ticket.candidates();
        let conn = connect_any(&endpoint, &candidates).await?;
        Ok(Self {
            endpoint,
            conn: Arc::new(Mutex::new(conn)),
            secret: ticket.secret,
            cluster: ticket.cluster.clone(),
            candidates: Arc::new(Mutex::new(candidates)),
        })
    }

    /// The same connection, making calls with another ticket's secret (the
    /// dashboard's browser sessions). The control plane checks each call
    /// against that secret's role. Closing it closes the shared connection.
    pub fn with_secret(&self, secret: Secret) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            conn: self.conn.clone(),
            secret,
            cluster: self.cluster.clone(),
            candidates: self.candidates.clone(),
        }
    }

    fn conn(&self) -> Connection {
        self.conn.lock().unwrap().clone()
    }

    /// The connection is gone (the control plane restarted, the network
    /// dropped); calls fail until [`Self::reconnect`].
    pub fn is_closed(&self) -> bool {
        self.conn().close_reason().is_some()
    }

    /// A new connection from the same endpoint, with the same secret.
    pub async fn reconnect(&self) -> Result<Self> {
        let candidates = self.candidates.lock().unwrap().clone();
        let conn = connect_any(&self.endpoint, &candidates).await?;
        Ok(Self {
            endpoint: self.endpoint.clone(),
            conn: Arc::new(Mutex::new(conn)),
            secret: self.secret,
            cluster: self.cluster.clone(),
            candidates: self.candidates.clone(),
        })
    }

    /// The cluster's id (its key), whichever replica answers.
    pub fn cluster_id(&self) -> iroh::EndpointId {
        self.cluster.id
    }

    /// The address of the control plane replica this client talks to.
    pub(crate) fn current_addr(&self) -> EndpointAddr {
        let id = self.conn().remote_id();
        self.candidates
            .lock()
            .unwrap()
            .iter()
            .find(|a| a.id == id)
            .cloned()
            .unwrap_or_else(|| EndpointAddr::new(id))
    }

    /// Moves to another replica: `leader` when one was named, else the
    /// next address that answers.
    async fn switch(&self, leader: Option<EndpointAddr>) -> Result<()> {
        let current = self.conn().remote_id();
        let order: Vec<EndpointAddr> = {
            let mut c = self.candidates.lock().unwrap();
            if let Some(l) = &leader {
                c.retain(|a| a.id != l.id);
                c.insert(0, l.clone());
            }
            let mut order: Vec<EndpointAddr> =
                c.iter().filter(|a| a.id != current).cloned().collect();
            // The current one last: it may have become the leader.
            order.extend(c.iter().filter(|a| a.id == current).cloned());
            order
        };
        let conn = connect_any(&self.endpoint, &order).await?;
        let old = std::mem::replace(&mut *self.conn.lock().unwrap(), conn);
        if old.remote_id() != self.conn().remote_id() {
            old.close(0u32.into(), b"moved to the leader");
        }
        Ok(())
    }

    pub async fn call(&self, op: AdminOp) -> Result<AdminResponse> {
        let req = AdminRequest {
            secret: self.secret,
            op,
        };
        // Elections take a few seconds; keep trying a little longer.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut last: anyhow::Error;
        loop {
            match send_call(&self.conn(), &req).await {
                Sent::Answered(AdminResponse::Error(e)) => anyhow::bail!(e),
                Sent::Answered(AdminResponse::NotLeader { leader }) => {
                    last = anyhow::anyhow!("no control plane replica is the leader right now");
                    if tokio::time::Instant::now() > deadline {
                        return Err(last);
                    }
                    if leader.is_none() {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    if let Err(e) = self.switch(leader).await {
                        last = e;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
                Sent::Answered(resp) => return Ok(resp),
                Sent::Unsent(e) => {
                    last = e;
                    if tokio::time::Instant::now() > deadline
                        || self.candidates.lock().unwrap().len() < 2
                    {
                        return Err(last);
                    }
                    if let Err(e) = self.switch(None).await {
                        last = e;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
                Sent::Lost(e) => return Err(e),
            }
            let _ = &last;
        }
    }

    pub async fn close(self) {
        self.conn().close(0u32.into(), b"done");
        self.endpoint.close().await;
    }
}
