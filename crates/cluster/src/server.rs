//! Network front of the control plane: accepts agent and admin connections
//! over iroh and drives the [`State`] machine.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use pumasi_proto::{
    ADMIN_ALPN, AGENT_ALPN, BLOBS_ALPN, MIN_AGENT_PROTOCOL, PROTOCOL_VERSION, Secret, blobs,
    msg::{
        AdminOp, AdminRequest, AdminResponse, AgentHello, ClusterStatus, FetchReply, FetchRequest,
        FromAgent, HelloReply, Invite, RELEASE_CHUNK, Role, ToAgent,
    },
    net::{NetMode, bind_endpoint, dialable_addr, load_or_create_key, write_private},
    perf::DiskClass,
    perf::kernel_native,
    release::SignedRelease,
    secrets_eq,
    store::{MAX_STORE_OUTPUT_BYTES, StoreTarget, format_size},
    ticket::{AdminTicket, JoinTicket},
    wire::{read_frame, read_msg, write_msg},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, info, warn};

use crate::{
    access,
    datasets::StoreCmd,
    files::FileStore,
    manage::Caller,
    rollout::{ReleaseStore, release_dir},
    state::State,
    store::Store,
};

const KEY_FILE: &str = "secret.key";
const DB_FILE: &str = "state.redb";
pub const ADMIN_TICKET_FILE: &str = "admin.ticket";
const ADMIN_SECRET_META: &str = "admin_secret";
const NAME_META: &str = "cluster_name";

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
        }
    }
}

struct Inner {
    state: Mutex<State>,
    admin_secret: Secret,
    endpoint: Endpoint,
    config: ClusterConfig,
    addr: Mutex<EndpointAddr>,
    releases: ReleaseStore,
    files: FileStore,
}

impl Inner {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("state lock poisoned")
    }

    fn addr(&self) -> EndpointAddr {
        self.addr.lock().unwrap().clone()
    }
}

/// A running control plane.
pub struct Cluster {
    inner: Arc<Inner>,
    router: Router,
    ticker: JoinHandle<()>,
    worker: JoinHandle<()>,
}

impl Cluster {
    pub async fn start(config: ClusterConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)
            .with_context(|| format!("create {}", config.data_dir.display()))?;
        let key = load_or_create_key(&config.data_dir.join(KEY_FILE))?;
        let signing_key = key.clone();
        let store = Store::open(&config.data_dir.join(DB_FILE))?;

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

        let files = FileStore::open(&config.data_dir.join("blobs"), config.gc_interval).await?;
        let endpoint = bind_endpoint(
            key,
            config.net,
            vec![
                AGENT_ALPN.to_vec(),
                ADMIN_ALPN.to_vec(),
                BLOBS_ALPN.to_vec(),
            ],
            config.bind,
        )
        .await?;
        let state = State::load(name.clone(), endpoint.id().to_string(), store, signing_key)?;
        let addr = dialable_addr(&endpoint, config.net).await;

        let inner = Arc::new(Inner {
            state: Mutex::new(state),
            admin_secret,
            endpoint: endpoint.clone(),
            config: config.clone(),
            addr: Mutex::new(addr),
            releases: ReleaseStore::new(release_dir(&config.data_dir)),
            files,
        });

        let (store_tx, store_rx) = mpsc::unbounded_channel();
        inner
            .state()
            .attach_store(store_tx, config.disk_class, config.storage_bill_every);
        let worker = tokio::spawn(store_worker(inner.clone(), store_rx));
        let blobs_protocol = {
            let gate_inner = inner.clone();
            let egress_inner = inner.clone();
            inner.files.protocol(
                Arc::new(move |id| gate_inner.state().store_peer(id)),
                Arc::new(move |account, bytes| egress_inner.state().charge_egress(account, bytes)),
            )
        };

        let router = Router::builder(endpoint.clone())
            .accept(AGENT_ALPN, AgentHandler(inner.clone()))
            .accept(ADMIN_ALPN, AdminHandler(inner.clone()))
            .accept(BLOBS_ALPN, blobs_protocol)
            .spawn();

        let ticker = {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(500));
                loop {
                    tick.tick().await;
                    inner.state().tick(inner.config.node_timeout);
                    // Keep the advertised address fresh (relay may change).
                    let fresh = inner.endpoint.addr();
                    if !fresh.addrs.is_empty() {
                        *inner.addr.lock().unwrap() = fresh;
                    }
                }
            })
        };

        let cluster = Self {
            inner,
            router,
            ticker,
            worker,
        };
        // The local CLI picks this up to talk to the cluster.
        write_private(
            &config.data_dir.join(ADMIN_TICKET_FILE),
            cluster.admin_ticket().to_string().as_bytes(),
        )?;
        info!(name = %name, id = %endpoint.id(), "control plane started");
        Ok(cluster)
    }

    pub fn name(&self) -> String {
        self.inner.state().cluster_name.clone()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.inner.addr()
    }

    pub fn admin_ticket(&self) -> AdminTicket {
        AdminTicket {
            cluster: self.addr(),
            secret: self.inner.admin_secret,
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
        let token = self.inner.state().create_token(ttl, uses, invite)?;
        Ok(JoinTicket {
            cluster: self.addr(),
            secret: token.secret,
        })
    }

    pub fn status(&self) -> ClusterStatus {
        self.inner.state().status()
    }

    /// Where uploaded release binaries are kept.
    pub fn release_store(&self) -> ReleaseStore {
        self.inner.releases.clone()
    }

    /// Sends a release to a node as is. For tests of forged releases.
    #[doc(hidden)]
    pub fn send_release_for_tests(&self, node: &str, release: SignedRelease) -> Result<()> {
        self.inner.state().send_release_for_tests(node, release)
    }

    /// Bytes the file store takes on disk.
    pub fn store_disk_bytes(&self) -> u64 {
        self.inner.files.disk_bytes()
    }

    pub async fn shutdown(self) -> Result<()> {
        self.ticker.abort();
        self.worker.abort();
        // Aborted tasks drop their state handles when they next yield;
        // wait for that so the database is closed when this returns.
        let _ = self.ticker.await;
        let _ = self.worker.await;
        self.router.shutdown().await.context("router shutdown")?;
        self.inner.files.store.shutdown().await.ok();
        // Tasks of ended sessions let go of the state as their connections
        // close; the database stays locked until the last one has.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&self.inner) > 1 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Agent protocol
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AgentHandler(Arc<Inner>);

impl std::fmt::Debug for AgentHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AgentHandler")
    }
}

impl ProtocolHandler for AgentHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let inner = self.0.clone();
        if let Err(e) = agent_session(inner, conn).await {
            debug!("agent session ended: {e:#}");
        }
        Ok(())
    }
}

async fn agent_session(inner: Arc<Inner>, conn: Connection) -> Result<()> {
    let node_id = conn.remote_id().to_string();
    let (mut send, mut recv) = conn.accept_bi().await?;
    let Some(frame) = read_frame(&mut recv).await? else {
        return Ok(());
    };
    let hello = AgentHello::decode(&frame).context("decode hello")?;

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
                Ok(None) => break Ok(()),
                Err(e) => break Err(e),
            },
            _ = &mut kill_rx => break Ok(()),
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
struct AdminHandler(Arc<Inner>);

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
            let inner = self.0.clone();
            tokio::spawn(async move {
                let res: Result<()> = async {
                    let Some(req) = read_msg::<_, AdminRequest>(&mut recv).await? else {
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
        other => return Err(other),
    })
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
            let ticket = JoinTicket {
                cluster: inner.addr(),
                secret: token.secret,
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
            let ticket = AdminTicket {
                cluster: inner.addr(),
                secret: r.secret,
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
        other => anyhow::bail!("unexpected admin call {other:?}"),
    })();
    result.unwrap_or_else(|e| AdminResponse::Error(format!("{e:#}")))
}

// ---------------------------------------------------------------------------
// Admin client
// ---------------------------------------------------------------------------

/// Client for the admin protocol, used by the CLI and tests.
pub struct AdminClient {
    pub(crate) endpoint: Endpoint,
    conn: Connection,
    secret: Secret,
    pub(crate) cluster: EndpointAddr,
}

impl AdminClient {
    pub async fn connect(ticket: &AdminTicket, net: NetMode) -> Result<Self> {
        let endpoint = bind_endpoint(iroh::SecretKey::generate(), net, vec![], None).await?;
        Self::connect_with(endpoint, ticket).await
    }

    pub async fn connect_with(endpoint: Endpoint, ticket: &AdminTicket) -> Result<Self> {
        let conn = tokio::time::timeout(
            Duration::from_secs(20),
            endpoint.connect(ticket.cluster.clone(), ADMIN_ALPN),
        )
        .await
        .context("timed out connecting to the cluster")?
        .context("could not connect to the cluster")?;
        Ok(Self {
            endpoint,
            conn,
            secret: ticket.secret,
            cluster: ticket.cluster.clone(),
        })
    }

    pub async fn call(&self, op: AdminOp) -> Result<AdminResponse> {
        let (mut send, mut recv) = self.conn.open_bi().await?;
        write_msg(
            &mut send,
            &AdminRequest {
                secret: self.secret,
                op,
            },
        )
        .await?;
        send.finish()?;
        let resp = read_msg::<_, AdminResponse>(&mut recv)
            .await?
            .context("cluster closed the stream without replying")?;
        if let AdminResponse::Error(e) = resp {
            anyhow::bail!(e);
        }
        Ok(resp)
    }

    pub async fn close(self) {
        self.conn.close(0u32.into(), b"done");
        self.endpoint.close().await;
    }
}
