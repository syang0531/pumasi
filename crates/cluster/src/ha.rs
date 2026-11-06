//! Control plane high availability (phase 13): the control plane's state
//! replicated over Raft ([openraft]) to 3 or 5 replicas.
//!
//! What is replicated is the state database itself. Every write the
//! state machine makes is a [`Batch`](crate::store::Batch) of row puts
//! and deletes; the leader proposes it as a log entry and every replica
//! (the leader too) applies committed entries to its own `state.redb`, in
//! the same order, in one transaction per entry that also records the
//! applied log id. A replica's database therefore always holds exactly a
//! committed prefix of the history: a money move (`write_ledger`) is one
//! entry, so it is on every replica or on none.
//!
//! Only the leader runs the cluster: it loads the state machine from its
//! database once it has applied everything committed, serves agents, the
//! CLI and the dashboard, and runs the scheduler. Its memory runs ahead of
//! its database by one commit; writes it made but could not commit before
//! it died are lost, exactly as if a single control plane had restarted
//! (running tasks are placed again, receipts and charges that never
//! committed never happened). Followers answer agents and admin calls
//! with where the leader is. A leader that cannot reach a majority stops
//! serving within a few seconds, so a cut-off old leader does not keep
//! running the cluster beside the new one.
//!
//! Each replica has its own iroh endpoint key (`endpoint.key`); the
//! cluster key in `secret.key`, which signs releases and receipts and
//! which agents know the cluster by, is the same on every replica (the
//! replica that created the cluster uses it as its endpoint key too, so
//! existing agents keep reaching it).
//!
//! The file store's data is not in the log: followers copy the leader's
//! tagged collections in the background ([`mirror`]).

// openraft's own error type is large; the storage traits return it.
#![allow(clippy::result_large_err)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::Debug,
    io::Cursor,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::Connection};
use openraft::{
    AnyError, ChangeMembers, Config, Entry, EntryPayload, LogId, LogState, Raft, RaftLogReader,
    RaftMetrics, RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, ServerState, Snapshot,
    SnapshotMeta, SnapshotPolicy, StorageError, StorageIOError, StoredMembership, Vote,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use pumasi_proto::{
    RAFT_ALPN, Secret,
    ha::{HaView, ReplicaView},
    secrets_eq,
    store::BlobHash,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, info, warn};

use crate::store::{self, Op};

/// The Raft log, vote and committed index (separate from the state).
pub const RAFT_DB: &str = "raft.redb";
/// The latest snapshot of the state, for replicas that are far behind.
const SNAPSHOT_FILE: &str = "snapshot.bin";
/// This replica's own iroh key, when it is not the cluster key.
pub const ENDPOINT_KEY_FILE: &str = "endpoint.key";
/// Replicas a joining replica trusts until it has a membership.
const JOIN_PEERS_FILE: &str = "join.peers";

/// Largest Raft message (a batch of entries can carry job modules).
const MAX_RAFT_FRAME: usize = 512 * 1024 * 1024;
/// A leader that heard from no majority for this long stops serving.
pub const LEASE: Duration = Duration::from_secs(3);

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("log");
const LOG_META: TableDefinition<&str, &[u8]> = TableDefinition::new("log_meta");
/// In the state database: the applied log id and membership, written in
/// the same transaction as the rows they cover.
const SM_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_sm");

const VOTE: &str = "vote";
const COMMITTED: &str = "committed";
const PURGED: &str = "purged";
const APPLIED: &str = "applied";
const MEMBERSHIP: &str = "membership";

/// A replica as Raft knows it: its address.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replica {
    pub addr: Option<EndpointAddr>,
}

impl std::fmt::Display for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.addr {
            Some(a) => write!(f, "{}", a.id.fmt_short()),
            None => f.write_str("?"),
        }
    }
}

/// A log entry's payload: the row writes of one or more store calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request(pub Vec<Op>);

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} writes", self.0.len())
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = (),
        NodeId = u64,
        Node = Replica,
);

pub type NodeId = u64;
type C = TypeConfig;
type SErr = StorageError<NodeId>;

/// The Raft node id of an endpoint: its first 8 bytes.
pub fn node_id(id: &EndpointId) -> NodeId {
    u64::from_le_bytes(id.as_bytes()[..8].try_into().expect("8 bytes"))
}

fn io_read(e: impl std::fmt::Display) -> SErr {
    StorageIOError::read(AnyError::error(e.to_string())).into()
}

fn io_write(e: impl std::fmt::Display) -> SErr {
    StorageIOError::write(AnyError::error(e.to_string())).into()
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    postcard::to_stdvec(v).expect("raft state encodes")
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, SErr> {
    postcard::from_bytes(b).map_err(io_read)
}

pub fn raft_config() -> Result<Arc<Config>> {
    let c = Config {
        cluster_name: "pumasi".into(),
        heartbeat_interval: 250,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        install_snapshot_timeout: 30_000,
        max_payload_entries: 64,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(5000),
        max_in_snapshot_log_to_keep: 1000,
        purge_batch_size: 256,
        snapshot_max_chunk_size: 4 * 1024 * 1024,
        ..Default::default()
    };
    Ok(Arc::new(c.validate().context("raft config")?))
}

// ---------------------------------------------------------------------------
// Log storage
// ---------------------------------------------------------------------------

/// The Raft log in its own redb file.
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).with_context(|| format!("open {}", path.display()))?;
        let tx = db.begin_write()?;
        tx.open_table(LOG)?;
        tx.open_table(LOG_META)?;
        tx.commit()?;
        Ok(Self { db: Arc::new(db) })
    }

    fn meta<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>, SErr> {
        let tx = self.db.begin_read().map_err(io_read)?;
        let t = tx.open_table(LOG_META).map_err(io_read)?;
        match t.get(key).map_err(io_read)? {
            Some(v) => Ok(Some(dec(v.value())?)),
            None => Ok(None),
        }
    }

    fn put_meta<T: Serialize>(&self, key: &str, v: &T) -> Result<(), SErr> {
        let tx = self.db.begin_write().map_err(io_write)?;
        tx.open_table(LOG_META)
            .map_err(io_write)?
            .insert(key, enc(v).as_slice())
            .map_err(io_write)?;
        tx.commit().map_err(io_write)
    }
}

impl RaftLogReader<C> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, SErr> {
        let tx = self.db.begin_read().map_err(io_read)?;
        let t = tx.open_table(LOG).map_err(io_read)?;
        let mut out = Vec::new();
        for row in t.range(range).map_err(io_read)? {
            let (_, v) = row.map_err(io_read)?;
            out.push(dec(v.value())?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<C> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, SErr> {
        let purged: Option<LogId<NodeId>> = self.meta(PURGED)?;
        let last = {
            let tx = self.db.begin_read().map_err(io_read)?;
            let t = tx.open_table(LOG).map_err(io_read)?;
            match t.last().map_err(io_read)? {
                Some((_, v)) => Some(dec::<Entry<C>>(v.value())?.log_id),
                None => None,
            }
        };
        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: last.or(purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), SErr> {
        self.put_meta(VOTE, vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, SErr> {
        self.meta(VOTE)
    }

    async fn save_committed(&mut self, c: Option<LogId<NodeId>>) -> Result<(), SErr> {
        self.put_meta(COMMITTED, &c)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, SErr> {
        Ok(self.meta::<Option<LogId<NodeId>>>(COMMITTED)?.flatten())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), SErr>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let res = (|| {
            let tx = self.db.begin_write().map_err(io_write)?;
            {
                let mut t = tx.open_table(LOG).map_err(io_write)?;
                for e in entries {
                    t.insert(e.log_id.index, enc(&e).as_slice())
                        .map_err(io_write)?;
                }
            }
            tx.commit().map_err(io_write)
        })();
        callback.log_io_completed(
            res.as_ref()
                .map(|_| ())
                .map_err(|e| std::io::Error::other(e.to_string())),
        );
        res
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), SErr> {
        let tx = self.db.begin_write().map_err(io_write)?;
        {
            let mut t = tx.open_table(LOG).map_err(io_write)?;
            t.retain_in(log_id.index.., |_, _| false)
                .map_err(io_write)?;
        }
        tx.commit().map_err(io_write)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), SErr> {
        let tx = self.db.begin_write().map_err(io_write)?;
        {
            let mut t = tx.open_table(LOG).map_err(io_write)?;
            t.retain_in(..=log_id.index, |_, _| false)
                .map_err(io_write)?;
            tx.open_table(LOG_META)
                .map_err(io_write)?
                .insert(PURGED, enc(&Some(log_id)).as_slice())
                .map_err(io_write)?;
        }
        tx.commit().map_err(io_write)
    }
}

// ---------------------------------------------------------------------------
// State machine: the state database
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, Replica>,
    data: Vec<u8>,
}

/// Applies committed entries to `state.redb`.
#[derive(Clone)]
pub struct StateMachine {
    db: Arc<Database>,
    dir: PathBuf,
}

type Applied = (Option<LogId<NodeId>>, StoredMembership<NodeId, Replica>);

fn read_applied(db: &Database) -> Result<Applied, SErr> {
    let tx = db.begin_read().map_err(io_read)?;
    applied_in(&tx)
}

fn applied_in(tx: &redb::ReadTransaction) -> Result<Applied, SErr> {
    let t = match tx.open_table(SM_META) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Default::default()),
        Err(e) => return Err(io_read(e)),
    };
    let applied = match t.get(APPLIED).map_err(io_read)? {
        Some(v) => dec(v.value())?,
        None => None,
    };
    let membership = match t.get(MEMBERSHIP).map_err(io_read)? {
        Some(v) => dec(v.value())?,
        None => StoredMembership::default(),
    };
    Ok((applied, membership))
}

impl StateMachine {
    pub fn new(db: Arc<Database>, dir: &Path) -> Self {
        Self {
            db,
            dir: dir.to_path_buf(),
        }
    }

    fn save_snapshot(&self, s: &StoredSnapshot) -> Result<(), SErr> {
        let path = self.dir.join(SNAPSHOT_FILE);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, enc(s)).map_err(io_write)?;
        std::fs::rename(&tmp, &path).map_err(io_write)
    }

    fn build(&self) -> Result<Snapshot<C>, SErr> {
        let tx = self.db.begin_read().map_err(io_read)?;
        let (applied, membership) = applied_in(&tx)?;
        let dump = store::dump(&tx).map_err(io_read)?;
        drop(tx);
        let data = enc(&dump);
        let meta = SnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
            snapshot_id: format!(
                "{}-{}",
                applied.map(|l| l.index).unwrap_or(0),
                crate::state::now_unix()
            ),
        };
        let stored = StoredSnapshot { meta, data };
        self.save_snapshot(&stored)?;
        Ok(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        })
    }
}

impl RaftSnapshotBuilder<C> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, SErr> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.build())
            .await
            .map_err(io_read)?
    }
}

impl RaftStateMachine<C> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<Applied, SErr> {
        read_applied(&self.db)
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, SErr>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let tx = self.db.begin_write().map_err(io_write)?;
        let mut out = Vec::new();
        {
            let mut meta = tx.open_table(SM_META).map_err(io_write)?;
            let mut last = None;
            let mut membership = None;
            for e in entries {
                match e.payload {
                    EntryPayload::Blank => {}
                    EntryPayload::Normal(req) => store::apply_ops(&tx, &req.0).map_err(|err| {
                        SErr::from(StorageIOError::apply(
                            e.log_id,
                            AnyError::error(format!("{err:#}")),
                        ))
                    })?,
                    EntryPayload::Membership(m) => {
                        membership = Some(StoredMembership::new(Some(e.log_id), m));
                    }
                }
                last = Some(e.log_id);
                out.push(());
            }
            if let Some(m) = membership {
                meta.insert(MEMBERSHIP, enc(&m).as_slice())
                    .map_err(io_write)?;
            }
            if last.is_some() {
                meta.insert(APPLIED, enc(&last).as_slice())
                    .map_err(io_write)?;
            }
        }
        tx.commit().map_err(io_write)?;
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, SErr> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Replica>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), SErr> {
        let data = snapshot.into_inner();
        let dump: store::Dump = dec(&data)?;
        let this = self.clone();
        let m = meta.clone();
        tokio::task::spawn_blocking(move || -> Result<(), SErr> {
            let tx = this.db.begin_write().map_err(io_write)?;
            store::load_dump(&tx, &dump).map_err(io_write)?;
            {
                let mut t = tx.open_table(SM_META).map_err(io_write)?;
                t.insert(APPLIED, enc(&m.last_log_id).as_slice())
                    .map_err(io_write)?;
                t.insert(MEMBERSHIP, enc(&m.last_membership).as_slice())
                    .map_err(io_write)?;
            }
            tx.commit().map_err(io_write)?;
            this.save_snapshot(&StoredSnapshot {
                meta: m.clone(),
                data,
            })?;
            info!(
                index = m.last_log_id.map(|l| l.index).unwrap_or(0),
                "installed a snapshot of the control plane state"
            );
            Ok(())
        })
        .await
        .map_err(io_write)?
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, SErr> {
        let path = self.dir.join(SNAPSHOT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_read(e)),
        };
        let s: StoredSnapshot = dec(&bytes)?;
        Ok(Some(Snapshot {
            meta: s.meta,
            snapshot: Box::new(Cursor::new(s.data)),
        }))
    }
}

// ---------------------------------------------------------------------------
// Network: Raft over iroh
// ---------------------------------------------------------------------------

/// What one replica asks another on [`RAFT_ALPN`], one per stream.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftReq {
    Append(AppendEntriesRequest<C>),
    Vote(VoteRequest<NodeId>),
    Snapshot(InstallSnapshotRequest<C>),
    /// A new replica with a replica ticket's secret.
    Join {
        secret: Secret,
        addr: EndpointAddr,
    },
    /// The leader's file store tags, for followers' copies.
    Tags,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RaftResp {
    Append(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    Snapshot(Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>),
    Join(JoinReply),
    Tags(Vec<(String, BlobHash)>),
    /// Refused: not a replica, or not ready.
    Refused(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum JoinReply {
    /// Welcome: the cluster key, and the leader that will add the replica.
    Accepted {
        cluster_key: [u8; 32],
        leader: EndpointAddr,
        replicas: Vec<EndpointAddr>,
    },
    NotLeader {
        leader: Option<EndpointAddr>,
    },
    Rejected(String),
}

pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, msg: &T) -> Result<()> {
    let bytes = postcard::to_stdvec(msg).context("encode raft message")?;
    if bytes.len() > MAX_RAFT_FRAME {
        bail!("raft message too large: {} bytes", bytes.len());
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    r: &mut R,
) -> Result<T> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await.context("read raft frame")?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_RAFT_FRAME {
        bail!("raft frame too large: {len} bytes");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await.context("read raft frame")?;
    postcard::from_bytes(&buf).context("decode raft message")
}

/// One request and its answer on a new stream of `conn`.
pub async fn call(conn: &Connection, req: &RaftReq) -> Result<RaftResp> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, req).await?;
    send.finish()?;
    read_frame(&mut recv).await
}

/// When each replica last answered this one.
pub type Seen = Arc<Mutex<HashMap<NodeId, Instant>>>;

#[derive(Clone)]
pub struct Network {
    endpoint: Endpoint,
    seen: Seen,
}

pub struct Peer {
    endpoint: Endpoint,
    target: NodeId,
    addr: Option<EndpointAddr>,
    conn: Option<Connection>,
    seen: Seen,
}

impl RaftNetworkFactory<C> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, node: &Replica) -> Peer {
        Peer {
            endpoint: self.endpoint.clone(),
            target,
            addr: node.addr.clone(),
            conn: None,
            seen: self.seen.clone(),
        }
    }
}

enum CallError {
    Unreachable(anyhow::Error),
    Network(anyhow::Error),
}

impl CallError {
    fn rpc<E: std::error::Error>(self) -> RPCError<NodeId, Replica, E> {
        match self {
            CallError::Unreachable(e) => {
                RPCError::Unreachable(Unreachable::new(&AnyError::error(format!("{e:#}"))))
            }
            CallError::Network(e) => {
                RPCError::Network(NetworkError::new(&AnyError::error(format!("{e:#}"))))
            }
        }
    }
}

impl Peer {
    async fn rpc(&mut self, req: RaftReq, ttl: Duration) -> Result<RaftResp, CallError> {
        let Some(addr) = self.addr.clone() else {
            return Err(CallError::Unreachable(anyhow::anyhow!(
                "no address for replica {}",
                self.target
            )));
        };
        let conn = match self.conn.take() {
            Some(c) if c.close_reason().is_none() => c,
            _ => tokio::time::timeout(
                ttl.max(Duration::from_secs(2)),
                self.endpoint.connect(addr, RAFT_ALPN),
            )
            .await
            .map_err(|_| CallError::Unreachable(anyhow::anyhow!("timed out connecting")))?
            .map_err(|e| CallError::Unreachable(e.into()))?,
        };
        match tokio::time::timeout(ttl, call(&conn, &req)).await {
            Ok(Ok(resp)) => {
                self.conn = Some(conn);
                self.seen
                    .lock()
                    .unwrap()
                    .insert(self.target, Instant::now());
                Ok(resp)
            }
            Ok(Err(e)) => Err(CallError::Network(e)),
            Err(_) => {
                // Keep the connection: the answer was just slow.
                self.conn = Some(conn);
                Err(CallError::Network(anyhow::anyhow!("no answer in {ttl:?}")))
            }
        }
    }
}

fn unexpected<E: std::error::Error>(r: RaftResp) -> RPCError<NodeId, Replica, E> {
    let why = match r {
        RaftResp::Refused(why) => why,
        other => format!("unexpected answer {other:?}"),
    };
    RPCError::Network(NetworkError::new(&AnyError::error(why)))
}

impl RaftNetwork<C> for Peer {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Replica, RaftError<NodeId>>> {
        match self.rpc(RaftReq::Append(rpc), option.hard_ttl()).await {
            Ok(RaftResp::Append(r)) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            Ok(other) => Err(unexpected(other)),
            Err(e) => Err(e.rpc()),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Replica, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.rpc(RaftReq::Snapshot(rpc), option.hard_ttl()).await {
            Ok(RaftResp::Snapshot(r)) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            Ok(other) => Err(unexpected(other)),
            Err(e) => Err(e.rpc()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Replica, RaftError<NodeId>>> {
        match self.rpc(RaftReq::Vote(rpc), option.hard_ttl()).await {
            Ok(RaftResp::Vote(r)) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            Ok(other) => Err(unexpected(other)),
            Err(e) => Err(e.rpc()),
        }
    }
}

// ---------------------------------------------------------------------------
// The replica
// ---------------------------------------------------------------------------

/// This replica's Raft node and what goes with it.
pub struct Ha {
    pub raft: Raft<C>,
    pub id: NodeId,
    pub endpoint: Endpoint,
    dir: PathBuf,
    seen: Seen,
    /// Replica ticket secrets this leader handed out, and until when.
    invites: Mutex<HashMap<Secret, Instant>>,
    /// Who may send Raft messages before this replica has a membership.
    join_peers: Mutex<HashSet<EndpointId>>,
}

impl Ha {
    /// Starts the Raft node over `state_db`. With `bootstrap`, a cluster
    /// of this replica alone is initialized (replication turned on).
    pub async fn start(
        dir: &Path,
        endpoint: Endpoint,
        addr: EndpointAddr,
        state_db: Arc<Database>,
        bootstrap: bool,
    ) -> Result<Arc<Self>> {
        let id = node_id(&endpoint.id());
        let log = LogStore::open(&dir.join(RAFT_DB))?;
        let sm = StateMachine::new(state_db, dir);
        let seen: Seen = Default::default();
        let network = Network {
            endpoint: endpoint.clone(),
            seen: seen.clone(),
        };
        let raft = Raft::new(id, raft_config()?, network, log, sm)
            .await
            .map_err(|e| anyhow::anyhow!("start raft: {e}"))?;
        let join_peers = match std::fs::read_to_string(dir.join(JOIN_PEERS_FILE)) {
            Ok(text) => text
                .lines()
                .filter_map(|l| l.trim().parse::<EndpointId>().ok())
                .collect(),
            Err(_) => HashSet::new(),
        };
        let ha = Arc::new(Self {
            raft,
            id,
            endpoint: endpoint.clone(),
            dir: dir.to_path_buf(),
            seen,
            invites: Default::default(),
            join_peers: Mutex::new(join_peers),
        });
        if bootstrap && !ha.raft.is_initialized().await? {
            let mut members = BTreeMap::new();
            members.insert(id, Replica { addr: Some(addr) });
            ha.raft
                .initialize(members)
                .await
                .map_err(|e| anyhow::anyhow!("initialize raft: {e}"))?;
            info!("control plane replication turned on");
        }
        Ok(ha)
    }

    /// Whether this data directory has replication on.
    pub fn enabled_in(dir: &Path) -> bool {
        dir.join(RAFT_DB).exists()
    }

    /// Before a new replica first starts: who may talk Raft to it until it
    /// has a membership.
    pub fn prepare_join(dir: &Path, peers: &[EndpointId]) -> Result<()> {
        let text: String = peers.iter().map(|p| format!("{p}\n")).collect();
        std::fs::write(dir.join(JOIN_PEERS_FILE), text)?;
        // The log's existence marks replication as on.
        LogStore::open(&dir.join(RAFT_DB))?;
        Ok(())
    }

    pub fn metrics(&self) -> RaftMetrics<NodeId, Replica> {
        self.raft.metrics().borrow().clone()
    }

    pub fn is_leader(&self) -> bool {
        let m = self.raft.metrics();
        let m = m.borrow();
        m.state == ServerState::Leader && m.current_leader == Some(self.id)
    }

    /// Every replica in the membership, with its address.
    pub fn members(&self) -> Vec<(NodeId, Replica)> {
        let m = self.raft.metrics();
        let m = m.borrow();
        m.membership_config
            .membership()
            .nodes()
            .map(|(id, n)| (*id, n.clone()))
            .collect()
    }

    /// The replicas' addresses (this one's fresh).
    pub fn replica_addrs(&self) -> Vec<EndpointAddr> {
        let me = self.endpoint.addr();
        self.members()
            .into_iter()
            .filter_map(|(id, n)| {
                if id == self.id {
                    Some(me.clone())
                } else {
                    n.addr
                }
            })
            .collect()
    }

    pub fn leader_addr(&self) -> Option<EndpointAddr> {
        let leader = self.raft.metrics().borrow().current_leader?;
        if leader == self.id {
            return Some(self.endpoint.addr());
        }
        self.members()
            .into_iter()
            .find(|(id, _)| *id == leader)
            .and_then(|(_, n)| n.addr)
    }

    /// Whether `peer` is a replica (or, before this replica has a
    /// membership, one it was told to trust).
    pub fn is_replica(&self, peer: &EndpointId) -> bool {
        let id = node_id(peer);
        let members = self.members();
        if members
            .iter()
            .any(|(m, n)| *m == id && n.addr.as_ref().is_none_or(|a| a.id == *peer))
        {
            return true;
        }
        members.is_empty() && self.join_peers.lock().unwrap().contains(peer)
    }

    /// The Raft state for `pumasi ha status`.
    pub fn view(&self) -> HaView {
        let m = self.metrics();
        let membership = m.membership_config.membership();
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let seen = self.seen.lock().unwrap().clone();
        let mut replicas: Vec<ReplicaView> = membership
            .nodes()
            .map(|(id, n)| ReplicaView {
                id: n
                    .addr
                    .as_ref()
                    .map(|a| a.id.to_string())
                    .unwrap_or_else(|| id.to_string()),
                voter: voters.contains(id),
                leader: m.current_leader == Some(*id),
                matched: if *id == self.id {
                    m.last_log_index
                } else {
                    m.replication
                        .as_ref()
                        .and_then(|r| r.get(id).cloned().flatten())
                        .map(|l| l.index)
                },
                last_seen_ms: if *id == self.id {
                    Some(0)
                } else {
                    seen.get(id).map(|t| t.elapsed().as_millis() as u64)
                },
            })
            .collect();
        replicas.sort_by(|a, b| b.leader.cmp(&a.leader).then(a.id.cmp(&b.id)));
        HaView {
            enabled: true,
            leader: self.endpoint.id().to_string(),
            term: m.current_term,
            committed: m.last_log_index,
            applied: m.last_applied.map(|l| l.index),
            snapshot: m.snapshot.map(|l| l.index),
            replicas,
        }
    }

    /// A one-time secret for a replica ticket.
    pub fn invite(&self, ttl: Duration) -> Secret {
        let s = pumasi_proto::new_secret();
        let mut inv = self.invites.lock().unwrap();
        inv.retain(|_, t| *t > Instant::now());
        inv.insert(s, Instant::now() + ttl);
        s
    }

    fn take_invite(&self, secret: &Secret) -> bool {
        let mut inv = self.invites.lock().unwrap();
        inv.retain(|_, t| *t > Instant::now());
        let found = inv.keys().find(|k| secrets_eq(k, secret)).copied();
        match found {
            Some(k) => {
                inv.remove(&k);
                true
            }
            None => false,
        }
    }

    /// Takes a snapshot of everything applied and drops the log before it,
    /// so a new replica starts from the snapshot (needed right after
    /// replication is turned on: the state from before is in no entry).
    pub async fn compact(&self) -> Result<()> {
        let applied = self.metrics().last_applied.map(|l| l.index).unwrap_or(0);
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| anyhow::anyhow!("snapshot: {e}"))?;
        let mut m = self.raft.metrics();
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if m.borrow().snapshot.is_some_and(|s| s.index >= applied) {
                    break;
                }
                if m.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .context("timed out taking a snapshot")?;
        self.raft
            .trigger()
            .purge_log(applied)
            .await
            .map_err(|e| anyhow::anyhow!("purge: {e}"))?;
        Ok(())
    }

    /// Adds a replica that joined with a ticket: as a learner until it
    /// has caught up, then as a voter.
    async fn add_replica(self: Arc<Self>, peer: EndpointId, addr: EndpointAddr) {
        let id = node_id(&peer);
        let deadline = Instant::now() + Duration::from_secs(600);
        loop {
            match self
                .raft
                .add_learner(
                    id,
                    Replica {
                        addr: Some(addr.clone()),
                    },
                    true,
                )
                .await
            {
                Ok(_) => break,
                Err(e) if Instant::now() < deadline => {
                    debug!(replica = %peer.fmt_short(), "adding a learner: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    warn!(replica = %peer.fmt_short(), "could not add the replica: {e}");
                    return;
                }
            }
        }
        // Replicas joining at the same time each change the membership,
        // and Raft takes one change at a time: wait for the other one.
        loop {
            let voters = BTreeSet::from([id]);
            match self
                .raft
                .change_membership(ChangeMembers::AddVoterIds(voters), false)
                .await
            {
                Ok(_) => {
                    info!(replica = %peer.fmt_short(), "a control plane replica joined");
                    break;
                }
                Err(e) if Instant::now() < deadline => {
                    debug!(replica = %peer.fmt_short(), "making the replica a voter: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    warn!(replica = %peer.fmt_short(), "could not make the replica a voter: {e}");
                    break;
                }
            }
        }
    }

    /// Takes a replica (id or prefix of its endpoint id) out of the
    /// membership.
    pub async fn remove(&self, prefix: &str) -> Result<()> {
        let prefix = prefix.trim();
        anyhow::ensure!(!prefix.is_empty(), "name a replica");
        let found: Vec<(NodeId, String)> = self
            .members()
            .into_iter()
            .map(|(id, n)| (id, n.addr.map(|a| a.id.to_string()).unwrap_or_default()))
            .filter(|(_, s)| s.starts_with(prefix))
            .collect();
        let (id, name) = match found.as_slice() {
            [one] => one.clone(),
            [] => bail!("no replica {prefix}"),
            _ => bail!("{prefix} matches more than one replica"),
        };
        let voters: BTreeSet<NodeId> = self
            .metrics()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        anyhow::ensure!(
            !(voters.len() == 1 && voters.contains(&id)),
            "{name} is the only voting replica"
        );
        let mut ids = BTreeSet::new();
        ids.insert(id);
        if voters.contains(&id) {
            self.raft
                .change_membership(ChangeMembers::RemoveVoters(ids.clone()), false)
                .await
                .map_err(|e| anyhow::anyhow!("remove {name}: {e}"))?;
        }
        if self.members().iter().any(|(m, _)| *m == id) {
            self.raft
                .change_membership(ChangeMembers::RemoveNodes(ids), false)
                .await
                .map_err(|e| anyhow::anyhow!("remove {name}: {e}"))?;
        }
        info!(replica = %name, "removed a control plane replica");
        Ok(())
    }

    /// Serves another replica's connection on [`RAFT_ALPN`]. `tags` lists
    /// the file store's tags (on the leader).
    pub async fn serve(
        self: Arc<Self>,
        conn: Connection,
        cluster_key: [u8; 32],
        tags: impl AsyncTags,
    ) -> Result<()> {
        let peer = conn.remote_id();
        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(s) => s,
                Err(_) => return Ok(()),
            };
            let req: RaftReq = match read_frame(&mut recv).await {
                Ok(r) => r,
                Err(e) => {
                    debug!("raft request: {e:#}");
                    continue;
                }
            };
            let this = self.clone();
            let tags = tags.clone();
            tokio::spawn(async move {
                let resp = this.handle(peer, req, cluster_key, tags).await;
                if write_frame(&mut send, &resp).await.is_ok() {
                    let _ = send.finish();
                }
            });
        }
    }

    async fn handle(
        self: Arc<Self>,
        peer: EndpointId,
        req: RaftReq,
        cluster_key: [u8; 32],
        tags: impl AsyncTags,
    ) -> RaftResp {
        if let RaftReq::Join { secret, addr } = req {
            return RaftResp::Join(self.join(peer, secret, addr, cluster_key).await);
        }
        if !self.is_replica(&peer) {
            return RaftResp::Refused("not a replica of this cluster".into());
        }
        match req {
            RaftReq::Append(r) => RaftResp::Append(self.raft.append_entries(r).await),
            RaftReq::Vote(r) => RaftResp::Vote(self.raft.vote(r).await),
            RaftReq::Snapshot(r) => RaftResp::Snapshot(self.raft.install_snapshot(r).await),
            RaftReq::Tags => match tags.list().await {
                Ok(t) => RaftResp::Tags(t),
                Err(e) => RaftResp::Refused(format!("{e:#}")),
            },
            RaftReq::Join { .. } => unreachable!("handled above"),
        }
    }

    async fn join(
        self: Arc<Self>,
        peer: EndpointId,
        secret: Secret,
        addr: EndpointAddr,
        cluster_key: [u8; 32],
    ) -> JoinReply {
        if !self.is_leader() {
            return JoinReply::NotLeader {
                leader: self.leader_addr(),
            };
        }
        if addr.id != peer {
            return JoinReply::Rejected("the address is not the caller's".into());
        }
        if !self.take_invite(&secret) {
            return JoinReply::Rejected(
                "invalid replica ticket (wrong, used, expired, or made by another leader)".into(),
            );
        }
        info!(replica = %peer.fmt_short(), "a control plane replica is joining");
        let mut replicas = self.replica_addrs();
        replicas.push(addr.clone());
        tokio::spawn(self.clone().add_replica(peer, addr));
        JoinReply::Accepted {
            cluster_key,
            leader: self.endpoint.addr(),
            replicas,
        }
    }

    /// Hands the leader's store writes to Raft, in order, several store
    /// calls per entry. Ends when this replica stops being the leader.
    pub async fn replicate(self: Arc<Self>, pipe: store::Pipe) {
        const MAX_ENTRY: usize = 16 * 1024 * 1024;
        let store::Pipe { mut rx, in_flight } = pipe;
        while let Some((mut last, first)) = rx.recv().await {
            let mut ops = first;
            let mut size: usize = ops.iter().map(op_size).sum();
            while size < MAX_ENTRY {
                match rx.try_recv() {
                    Ok((n, more)) => {
                        size += more.iter().map(op_size).sum::<usize>();
                        ops.extend(more);
                        last = n;
                    }
                    Err(_) => break,
                }
            }
            // `client_write` returns once the entry is applied here.
            if let Err(e) = self.raft.client_write(Request(ops)).await {
                warn!("could not replicate a state change: {e}");
                in_flight.applied(u64::MAX);
                return;
            }
            in_flight.applied(last);
        }
        in_flight.applied(u64::MAX);
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

fn op_size(op: &Op) -> usize {
    match op {
        Op::Put { key, value, .. } => key.len() + value.len() + 8,
        Op::Del { key, .. } => key.len() + 8,
    }
}

/// Lists the file store's tags (name, root), for followers' copies.
pub trait AsyncTags: Clone + Send + Sync + 'static {
    fn list(&self) -> impl std::future::Future<Output = Result<Vec<(String, BlobHash)>>> + Send;
}

/// Asks the replicas in `candidates` to let this one join, following
/// redirects to the leader.
pub async fn request_join(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    candidates: &[EndpointAddr],
    secret: Secret,
) -> Result<([u8; 32], EndpointAddr, Vec<EndpointAddr>)> {
    let mut queue: Vec<EndpointAddr> = candidates.to_vec();
    let mut tried: HashSet<EndpointId> = HashSet::new();
    let mut last_err = anyhow::anyhow!("the ticket names no control plane");
    while let Some(target) = queue.pop() {
        if !tried.insert(target.id) {
            continue;
        }
        let conn = match tokio::time::timeout(
            Duration::from_secs(15),
            endpoint.connect(target.clone(), RAFT_ALPN),
        )
        .await
        {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                last_err = anyhow::anyhow!("{}: {e}", target.id.fmt_short());
                continue;
            }
            Err(_) => {
                last_err = anyhow::anyhow!("{}: timed out", target.id.fmt_short());
                continue;
            }
        };
        let resp = call(
            &conn,
            &RaftReq::Join {
                secret,
                addr: addr.clone(),
            },
        )
        .await;
        conn.close(0u32.into(), b"done");
        match resp {
            Ok(RaftResp::Join(JoinReply::Accepted {
                cluster_key,
                leader,
                replicas,
            })) => return Ok((cluster_key, leader, replicas)),
            Ok(RaftResp::Join(JoinReply::NotLeader { leader })) => match leader {
                Some(l) => {
                    tried.remove(&l.id);
                    queue.push(l);
                    last_err = anyhow::anyhow!("{} is not the leader", target.id.fmt_short());
                }
                None => {
                    last_err = anyhow::anyhow!(
                        "{} is not the leader and knows no leader (is a majority of replicas up?)",
                        target.id.fmt_short()
                    )
                }
            },
            Ok(RaftResp::Join(JoinReply::Rejected(why))) => bail!("{why}"),
            Ok(other) => last_err = anyhow::anyhow!("unexpected answer {other:?}"),
            Err(e) => last_err = e,
        }
    }
    Err(last_err.context("could not join the control plane"))
}

/// Followers copy the leader's file store: every tagged collection, then
/// the same tags, so a new leader has the data its state refers to.
pub mod mirror {
    use super::*;
    use crate::files::FileStore;
    use pumasi_proto::{BLOBS_ALPN, blobs};

    /// One pass: fetch what is missing from the leader at `leader`.
    pub async fn sync(
        endpoint: &Endpoint,
        files: &FileStore,
        leader: EndpointAddr,
    ) -> Result<usize> {
        let conn = tokio::time::timeout(
            Duration::from_secs(15),
            endpoint.connect(leader.clone(), RAFT_ALPN),
        )
        .await
        .context("timed out")??;
        let tags = match call(&conn, &RaftReq::Tags).await? {
            RaftResp::Tags(t) => t,
            RaftResp::Refused(why) => bail!("{why}"),
            other => bail!("unexpected answer {other:?}"),
        };
        conn.close(0u32.into(), b"done");
        let mut fetched = 0;
        let mut blobs_conn: Option<Connection> = None;
        let mut have: Vec<(String, BlobHash)> = Vec::new();
        for (name, root) in tags {
            let hash = blobs::to_hash(&root);
            let (_, complete) = blobs::missing(files.api(), hash).await?;
            if !complete {
                let c = match &blobs_conn {
                    Some(c) if c.close_reason().is_none() => c.clone(),
                    _ => {
                        let c = tokio::time::timeout(
                            Duration::from_secs(15),
                            endpoint.connect(leader.clone(), BLOBS_ALPN),
                        )
                        .await
                        .context("timed out")??;
                        blobs_conn = Some(c.clone());
                        c
                    }
                };
                match blobs::fetch(files.api(), c, hash).await {
                    Ok(_) => fetched += 1,
                    Err(e) => {
                        debug!(tag = %name, "copy from the leader: {e:#}");
                        continue;
                    }
                }
            }
            have.push((name, root));
        }
        if let Some(c) = blobs_conn {
            c.close(0u32.into(), b"done");
        }
        files.reconcile(&have).await?;
        Ok(fetched)
    }
}
