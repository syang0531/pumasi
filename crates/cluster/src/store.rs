//! Durable control-plane state in a single redb file.
//!
//! Records are postcard-encoded. Tasks are stored one per key so a task
//! completing rewrites only that task, not the whole job. Postcard is not
//! self-describing, so the record layout is versioned (`schema` in `meta`)
//! and older layouts are migrated when the database is opened.
//!
//! Every write is recorded as a [`Batch`] of row puts and deletes. A
//! single control plane applies it at once in one transaction; a leader
//! of replicated control planes (phase 13) hands it to Raft instead, and
//! every replica (the leader too) applies it once a majority has it, so
//! the database only ever holds committed writes. The leader's state
//! machine in memory runs ahead of its database by that commit.

use std::{path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    ModuleHash, Secret,
    economy::Policy,
    gpu::GpuRequest,
    msg::{AccountView, LedgerEntry, LedgerKind, TaskRef},
    receipt::Receipt,
};
use redb::{
    Database, Key, ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition, TableError, TableHandle, Value, WriteTransaction,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;
use tracing::info;

use crate::{
    manage::{AccessRecord, JobSched, ScheduleRecord},
    remote::JobOrigin,
    state::{DatasetRecord, Hold, JobExt, JobMeta, Member, TaskExt, TaskRecord, TokenRecord},
};
use pumasi_proto::audit::AuditEntry;
use pumasi_proto::manage::{AccountPolicy, AccountQuota, NodeConfig};
use pumasi_proto::monitor::{AlertPolicy, EventView, MetricPoint, WebhookSpec};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const MEMBERS: TableDefinition<&str, &[u8]> = TableDefinition::new("members");
const TOKENS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("tokens");
const JOBS: TableDefinition<u128, &[u8]> = TableDefinition::new("jobs");
/// Key: (job id, task index).
const TASKS: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("tasks");
const MODULES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("modules");
const ACCOUNTS: TableDefinition<&str, &[u8]> = TableDefinition::new("accounts");
/// Key: entry sequence number.
const LEDGER: TableDefinition<u64, &[u8]> = TableDefinition::new("ledger");
/// Key: the run (job, index, attempt, check).
type RunKey = (u128, u32, u32, bool);
const HOLDS: TableDefinition<RunKey, &[u8]> = TableDefinition::new("holds");
const RECEIPTS: TableDefinition<RunKey, &[u8]> = TableDefinition::new("receipts");
/// Micro-CU owed below one milli-CU, key: (consumer, provider).
const CARRY: TableDefinition<(&str, &str), i64> = TableDefinition::new("carry");
/// The phase 2 credit ledger and receipts, kept as they were when the
/// database moved to compute units (layout 4).
const CREDIT_LEDGER: TableDefinition<u64, &[u8]> = TableDefinition::new("credit_ledger");
const CREDIT_RECEIPTS: TableDefinition<RunKey, &[u8]> = TableDefinition::new("credit_receipts");
/// File store records (phase 8). New tables rather than new fields, so the
/// existing record layouts stay as they are.
/// Key: job id. What a job mounts and where its outputs go.
const JOB_EXT: TableDefinition<u128, &[u8]> = TableDefinition::new("job_ext");
/// Key: (job id, task index). A task's mounts and stored output.
const TASK_EXT: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("task_ext");
/// Key: (dataset name, version).
const DATASETS: TableDefinition<(&str, u32), &[u8]> = TableDefinition::new("datasets");
/// Key: account. Storage quota in bytes.
const QUOTAS: TableDefinition<&str, u64> = TableDefinition::new("quotas");
/// Key: secret name. Value: (set unix time, value).
const SECRETS: TableDefinition<&str, &[u8]> = TableDefinition::new("secrets");
/// Key: collection root. Committed job inputs not yet used by a job:
/// (account, size, files, committed unix).
const UPLOADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("uploads");
/// A node's latest signed device profile and the cluster's GPU checks
/// (phase 9), by node id.
const MEMBER_DEVICES: TableDefinition<&str, &[u8]> = TableDefinition::new("member_devices");
/// The GPUs a job's tasks need (phase 9), by job id.
const JOB_GPU: TableDefinition<u128, &[u8]> = TableDefinition::new("job_gpu");
/// Cluster management (phase 10), again in tables of their own. A job's
/// priority, selector, tolerations, dependencies and schedule, by job id.
const JOB_SCHED: TableDefinition<u128, &[u8]> = TableDefinition::new("job_sched");
/// Labels, taints and cordon an admin set on a node, by node id.
const NODE_CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("node_config");
/// Account quotas (all but storage, which stays in `quotas`).
const ACCOUNT_QUOTAS: TableDefinition<&str, &[u8]> = TableDefinition::new("account_quotas");
/// Runs stopped to make room (preemption, drain), by (job, task, check):
/// placements that do not count against the job's attempts.
const PREEMPTED: TableDefinition<(u128, u32, bool), u32> = TableDefinition::new("preempted");
/// Access tickets, by secret.
const ACCESS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("access");
/// Schedules, by name.
const SCHEDULES: TableDefinition<&str, &[u8]> = TableDefinition::new("schedules");
/// Monitoring (phase 11). Cluster events, by sequence number.
const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");
/// Downsampled node metrics, by (node id, tier, step start).
const METRICS: TableDefinition<(&str, u8, u64), &[u8]> = TableDefinition::new("metrics");
/// Webhooks, by name.
const WEBHOOKS: TableDefinition<&str, &[u8]> = TableDefinition::new("webhooks");
/// A node's latest signed WebGPU adapters (phase 14), by node id.
const MEMBER_WEBGPU: TableDefinition<&str, &[u8]> = TableDefinition::new("member_webgpu");
/// Remote API clients (phase 15). What each account's jobs may be, by
/// account.
const ACCOUNT_POLICIES: TableDefinition<&str, &[u8]> = TableDefinition::new("account_policies");
/// Who submitted a job, its queue timeout, GPU capabilities and the image
/// digests its tasks ran, by job id.
const JOB_ORIGIN: TableDefinition<u128, &[u8]> = TableDefinition::new("job_origin");
/// The confidential tier (phase 16). Launch measurements seen in checked
/// attestations and whether an admin trusts them, by measurement id.
const MEASUREMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("measurements");
/// Providers' nodes an admin trusts with `tier = "trusted"` jobs, by node
/// id (the value is empty).
const TRUSTED_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("trusted_nodes");
/// Self-hosting 1.0 (phase 17). The audit log, a hash chain, by sequence
/// number.
const AUDIT: TableDefinition<u64, &[u8]> = TableDefinition::new("audit");
/// 1.4. How long the parts of a task's run took, by (job id, task index).
const TASK_TIMES: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("task_times");
/// The executor an `executor = "image"` task's run was placed with (1.5).
const TASK_EXEC: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("task_exec");

const SCHEMA_META: &str = "schema";
const POLICY_META: &str = "policy";
/// The old price of a core-hour in micro-credits, which became one CU.
const CREDIT_RATE_META: &str = "credit_cpu_hour";
/// Set once the credit receipts were countersigned again in CU.
const CREDIT_RECEIPTS_DONE_META: &str = "credit_receipts_converted";
const ALERT_POLICY_META: &str = "alert_policy";
/// SSO settings (phase 17).
const SSO_META: &str = "sso";
/// Layout version written by this build.
const SCHEMA: u32 = 7;

fn run_key(t: &TaskRef) -> RunKey {
    (t.job.0, t.index, t.attempt, t.check)
}

/// Calls `$m!(TABLE, ...)` with every table of the state, in a fixed order
/// (a table's position is its number in [`Op`]; append new tables at the
/// end).
macro_rules! tables {
    ($m:ident) => {
        $m!(
            META,
            MEMBERS,
            TOKENS,
            JOBS,
            TASKS,
            MODULES,
            ACCOUNTS,
            LEDGER,
            HOLDS,
            RECEIPTS,
            CARRY,
            CREDIT_LEDGER,
            CREDIT_RECEIPTS,
            JOB_EXT,
            TASK_EXT,
            DATASETS,
            QUOTAS,
            SECRETS,
            UPLOADS,
            MEMBER_DEVICES,
            JOB_GPU,
            JOB_SCHED,
            NODE_CONFIG,
            ACCOUNT_QUOTAS,
            PREEMPTED,
            ACCESS,
            SCHEDULES,
            EVENTS,
            METRICS,
            WEBHOOKS,
            MEMBER_WEBGPU,
            ACCOUNT_POLICIES,
            JOB_ORIGIN,
            MEASUREMENTS,
            TRUSTED_NODES,
            AUDIT,
            TASK_TIMES,
            TASK_EXEC
        )
    };
}

macro_rules! table_names {
    ($($t:ident),*) => {
        [$($t.name()),*]
    };
}

/// The number of a table in [`Op`].
fn table_no(name: &str) -> u8 {
    let names = tables!(table_names);
    names
        .iter()
        .position(|n| *n == name)
        .expect("every table is listed in tables!") as u8
}

/// One row write, in redb's own encoding of the key and value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Put {
        table: u8,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Del {
        table: u8,
        key: Vec<u8>,
    },
}

/// The writes of one store call, applied all or nothing.
#[derive(Debug, Default)]
pub struct Batch(pub Vec<Op>);

impl Batch {
    fn put<'k, 'v, K: Key + 'static, V: Value + 'static>(
        &mut self,
        table: TableDefinition<K, V>,
        key: impl std::borrow::Borrow<K::SelfType<'k>>,
        value: impl std::borrow::Borrow<V::SelfType<'v>>,
    ) {
        self.0.push(Op::Put {
            table: table_no(table.name()),
            key: K::as_bytes(key.borrow()).as_ref().to_vec(),
            value: V::as_bytes(value.borrow()).as_ref().to_vec(),
        });
    }

    fn del<'k, K: Key + 'static, V: Value + 'static>(
        &mut self,
        table: TableDefinition<K, V>,
        key: impl std::borrow::Borrow<K::SelfType<'k>>,
    ) {
        self.0.push(Op::Del {
            table: table_no(table.name()),
            key: K::as_bytes(key.borrow()).as_ref().to_vec(),
        });
    }
}

fn apply_typed<K: Key + 'static, V: Value + 'static>(
    tx: &WriteTransaction,
    table: TableDefinition<K, V>,
    op: &Op,
) -> Result<()> {
    let mut t = tx.open_table(table)?;
    match op {
        Op::Put { key, value, .. } => {
            t.insert(K::from_bytes(key), V::from_bytes(value))?;
        }
        Op::Del { key, .. } => {
            t.remove(K::from_bytes(key))?;
        }
    }
    Ok(())
}

/// Applies row writes inside a write transaction.
pub fn apply_ops(tx: &WriteTransaction, ops: &[Op]) -> Result<()> {
    macro_rules! dispatch {
        ($($t:ident),*) => {
            for op in ops {
                let (Op::Put { table, .. } | Op::Del { table, .. }) = op;
                let names = tables!(table_names);
                let Some(name) = names.get(*table as usize) else {
                    bail!("a write to table {table}, which this build does not know");
                };
                $(
                    if *name == $t.name() {
                        apply_typed(tx, $t, op)?;
                        continue;
                    }
                )*
            }
        };
    }
    tables!(dispatch);
    Ok(())
}

/// Every row of the state: (table, key, value) in redb's encoding. A copy
/// of the database for snapshots and backups.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Dump {
    pub rows: Vec<(u8, Vec<u8>, Vec<u8>)>,
}

fn dump_typed<K: Key + 'static, V: Value + 'static>(
    tx: &ReadTransaction,
    table: TableDefinition<K, V>,
    out: &mut Dump,
) -> Result<()> {
    let t = match tx.open_table(table) {
        Ok(t) => t,
        // Tables of old layouts that this database never had.
        Err(TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let no = table_no(table.name());
    for row in t.iter()? {
        let (k, v) = row?;
        out.rows.push((
            no,
            K::as_bytes(&k.value()).as_ref().to_vec(),
            V::as_bytes(&v.value()).as_ref().to_vec(),
        ));
    }
    Ok(())
}

/// Copies every row as of a read transaction.
pub fn dump(tx: &ReadTransaction) -> Result<Dump> {
    let mut out = Dump::default();
    macro_rules! each {
        ($($t:ident),*) => {
            $( dump_typed(tx, $t, &mut out)?; )*
        };
    }
    tables!(each);
    Ok(out)
}

/// Replaces every table's rows with a dump's, inside a write transaction.
pub fn load_dump(tx: &WriteTransaction, d: &Dump) -> Result<()> {
    macro_rules! wipe {
        ($($t:ident),*) => {
            $(
                tx.delete_table($t)?;
                tx.open_table($t)?;
            )*
        };
    }
    tables!(wipe);
    let ops: Vec<Op> = d
        .rows
        .iter()
        .map(|(table, key, value)| Op::Put {
            table: *table,
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    apply_ops(tx, &ops)
}

/// Where a store's writes go.
#[derive(Clone)]
pub enum Sink {
    /// Straight into the database.
    Direct,
    /// To the Raft log (the leader of replicated control planes); the
    /// state machine applies them once committed.
    Replicate(mpsc::UnboundedSender<(u64, Vec<Op>)>, Arc<InFlight>),
}

/// The receiving end of [`Sink::Replicate`]: numbered writes, and where to
/// say which of them are applied.
pub struct Pipe {
    pub rx: mpsc::UnboundedReceiver<(u64, Vec<Op>)>,
    pub in_flight: Arc<InFlight>,
}

/// A sink that hands writes to Raft, and the end Raft reads them from.
pub fn pipe() -> (Sink, Pipe) {
    let (tx, rx) = mpsc::unbounded_channel();
    let in_flight = Arc::new(InFlight::default());
    (
        Sink::Replicate(tx, in_flight.clone()),
        Pipe { rx, in_flight },
    )
}

/// Writes sent to Raft and not applied yet. A read of the database waits
/// for the writes made before it, so the leader reads what it wrote (its
/// memory runs ahead of the database by the writes in flight).
#[derive(Default)]
pub struct InFlight {
    /// (last sent, last applied)
    counts: std::sync::Mutex<(u64, u64)>,
    applied: std::sync::Condvar,
}

impl InFlight {
    /// Every write up to `upto` is applied (or will never be: `u64::MAX`
    /// when replication stopped).
    pub fn applied(&self, upto: u64) {
        let mut c = self.counts.lock().unwrap();
        c.1 = c.1.max(upto);
        self.applied.notify_all();
    }

    /// Waits until the writes sent so far are applied, for at most
    /// `limit` (without a quorum they may never be).
    fn wait(&self, limit: std::time::Duration) {
        let c = self.counts.lock().unwrap();
        let target = c.0;
        let (_counts, r) = self
            .applied
            .wait_timeout_while(c, limit, |c| c.1 < target)
            .unwrap();
        if r.timed_out() {
            tracing::warn!("reading the state database before the last writes were applied");
        }
    }
}

/// How long a read waits for the writes before it.
const READ_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

pub struct Store {
    db: Arc<Database>,
    sink: Sink,
}

/// Everything loaded at startup.
#[derive(Default)]
pub struct Snapshot {
    pub job_ext: Vec<(u128, JobExt)>,
    pub task_ext: Vec<((u128, u32), TaskExt)>,
    pub task_times: Vec<((u128, u32), crate::state::TaskTimes)>,
    pub task_exec: Vec<((u128, u32), pumasi_proto::job::Executor)>,
    pub datasets: Vec<DatasetRecord>,
    pub quotas: Vec<(String, u64)>,
    pub secrets: Vec<(String, u64, String)>,
    pub uploads: Vec<([u8; 32], crate::state::UploadRecord)>,
    pub devices: Vec<(String, crate::gpu::DeviceRecord)>,
    pub webgpu: Vec<(String, pumasi_proto::webgpu::SignedWebGpu)>,
    pub job_gpu: Vec<(u128, GpuRequest)>,
    pub job_sched: Vec<(u128, JobSched)>,
    pub node_config: Vec<(String, NodeConfig)>,
    pub account_quotas: Vec<(String, AccountQuota)>,
    pub preempted: Vec<((u128, u32, bool), u32)>,
    pub access: Vec<AccessRecord>,
    pub schedules: Vec<ScheduleRecord>,
    pub account_policies: Vec<(String, AccountPolicy)>,
    pub job_origin: Vec<(u128, JobOrigin)>,
    pub measurements: Vec<(String, crate::confidential::MeasurementRecord)>,
    pub trusted_nodes: Vec<String>,
    pub members: Vec<Member>,
    pub tokens: Vec<TokenRecord>,
    pub jobs: Vec<(JobMeta, Vec<TaskRecord>)>,
    pub modules: Vec<(ModuleHash, Vec<u8>)>,
    pub accounts: Vec<AccountView>,
    pub holds: Vec<Hold>,
    pub carry: Vec<((String, String), i64)>,
    pub next_ledger_seq: u64,
    pub policy: Option<Policy>,
}

/// Ledger changes written in one transaction.
#[derive(Default)]
pub struct LedgerWrite<'a> {
    pub accounts: Vec<&'a AccountView>,
    pub entries: &'a [LedgerEntry],
    pub put_holds: &'a [Hold],
    pub del_holds: &'a [TaskRef],
    pub carries: &'a [(String, String, i64)],
    pub receipt: Option<&'a Receipt>,
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    postcard::to_stdvec(v).expect("state encodes")
}

fn dec<T: DeserializeOwned>(b: &[u8]) -> Result<T> {
    postcard::from_bytes(b).context("corrupt record in state database")
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let db = Database::create(path).with_context(|| format!("open {}", path.display()))?;
        Self::init(Arc::new(db), path)
    }

    /// The same database, with writes going to `sink`.
    pub fn with_sink(&self, sink: Sink) -> Self {
        Self {
            db: self.db.clone(),
            sink,
        }
    }

    /// The database, shared with the Raft state machine.
    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }

    /// Records writes: applied now, or replicated first.
    fn commit(&self, b: Batch) -> Result<()> {
        if b.0.is_empty() {
            return Ok(());
        }
        match &self.sink {
            Sink::Direct => {
                let tx = self.db.begin_write()?;
                apply_ops(&tx, &b.0)?;
                tx.commit()?;
                Ok(())
            }
            Sink::Replicate(tx, in_flight) => {
                // Numbered under the lock, so numbers follow channel order.
                let mut c = in_flight.counts.lock().unwrap();
                let n = c.0 + 1;
                tx.send((n, b.0)).map_err(|_| {
                    anyhow::anyhow!("this control plane replica is no longer the leader")
                })?;
                c.0 = n;
                Ok(())
            }
        }
    }

    /// A read transaction that sees every write this store made before.
    fn read(&self) -> Result<redb::ReadTransaction> {
        if let Sink::Replicate(_, in_flight) = &self.sink {
            // Callers hold the state lock, which other workers may be
            // waiting for: let the runtime move Raft's tasks elsewhere.
            let multi = tokio::runtime::Handle::try_current()
                .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
            if multi {
                tokio::task::block_in_place(|| in_flight.wait(READ_WAIT));
            } else {
                in_flight.wait(READ_WAIT);
            }
        }
        Ok(self.db.begin_read()?)
    }

    /// Prepares a database: creates missing tables and migrates older
    /// layouts.
    fn init(db: Arc<Database>, path: &Path) -> Result<Self> {
        // Create all tables so reads never fail on a fresh database.
        let tx = db.begin_write()?;
        let fresh = {
            let members = tx.open_table(MEMBERS)?;
            let jobs = tx.open_table(JOBS)?;
            let tokens = tx.open_table(TOKENS)?;
            members.is_empty()? && jobs.is_empty()? && tokens.is_empty()?
        };
        tx.open_table(TASKS)?;
        tx.open_table(MODULES)?;
        tx.open_table(ACCOUNTS)?;
        tx.open_table(LEDGER)?;
        tx.open_table(HOLDS)?;
        tx.open_table(RECEIPTS)?;
        tx.open_table(CARRY)?;
        tx.open_table(JOB_EXT)?;
        tx.open_table(TASK_EXT)?;
        tx.open_table(DATASETS)?;
        tx.open_table(QUOTAS)?;
        tx.open_table(SECRETS)?;
        tx.open_table(UPLOADS)?;
        tx.open_table(MEMBER_DEVICES)?;
        tx.open_table(JOB_GPU)?;
        tx.open_table(JOB_SCHED)?;
        tx.open_table(NODE_CONFIG)?;
        tx.open_table(ACCOUNT_QUOTAS)?;
        tx.open_table(PREEMPTED)?;
        tx.open_table(ACCESS)?;
        tx.open_table(SCHEDULES)?;
        tx.open_table(EVENTS)?;
        tx.open_table(METRICS)?;
        tx.open_table(WEBHOOKS)?;
        tx.open_table(MEMBER_WEBGPU)?;
        tx.open_table(ACCOUNT_POLICIES)?;
        tx.open_table(JOB_ORIGIN)?;
        tx.open_table(MEASUREMENTS)?;
        tx.open_table(TRUSTED_NODES)?;
        tx.open_table(AUDIT)?;
        tx.open_table(TASK_TIMES)?;
        tx.open_table(TASK_EXEC)?;
        let schema = {
            let mut meta = tx.open_table(META)?;
            let stored = meta.get(SCHEMA_META)?.map(|v| v.value().to_vec());
            match stored {
                Some(b) => {
                    u32::from_le_bytes(b.as_slice().try_into().context("corrupt schema version")?)
                }
                None if fresh => {
                    meta.insert(SCHEMA_META, SCHEMA.to_le_bytes().as_slice())?;
                    SCHEMA
                }
                // Phase 1 databases have no version.
                None => 1,
            }
        };
        tx.commit()?;
        if schema > SCHEMA {
            bail!(
                "{} was written by a newer pumasi (layout {schema}); upgrade this binary",
                path.display()
            );
        }
        let store = Self {
            db,
            sink: Sink::Direct,
        };
        if schema < 2 {
            store.migrate_v1()?;
        }
        if schema < 3 {
            store.migrate_v2()?;
        }
        if schema < 4 {
            store.migrate_v3()?;
        }
        if schema < 5 {
            store.migrate_v4()?;
        }
        if schema < 6 {
            // Layout 6 (phase 10) only adds tables; older binaries would
            // ignore selectors, priorities and access tickets, so the
            // version keeps them from opening the database.
            store.put_meta(SCHEMA_META, &6u32.to_le_bytes())?;
            info!("migrated state database to layout 6");
        }
        // Phase 11 only adds tables (events, metrics, webhooks) and a meta
        // key that older binaries ignore safely, so the layout stays 6.
        if schema < 7 {
            store.migrate_v6()?;
        }
        Ok(store)
    }

    /// Rewrites phase 1 records (no roles, accounts or checks) in the
    /// current layout. Existing machines become owner machines and existing
    /// jobs belong to the owner account.
    fn migrate_v1(&self) -> Result<()> {
        let tx = self.db.begin_write()?;
        let mut counts = (0, 0, 0, 0);
        {
            let mut t = tx.open_table(MEMBERS)?;
            let rows: Vec<(String, Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value().to_string(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let m: legacy::MemberV3 = dec::<legacy::Member>(&v)?.into();
                t.insert(k.as_str(), enc(&m).as_slice())?;
                counts.0 += 1;
            }
        }
        {
            let mut t = tx.open_table(TOKENS)?;
            let rows: Vec<(Vec<u8>, Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let tok: TokenRecord = dec::<legacy::TokenRecord>(&v)?.into();
                t.insert(k.as_slice(), enc(&tok).as_slice())?;
                counts.1 += 1;
            }
        }
        {
            let mut t = tx.open_table(JOBS)?;
            let rows: Vec<(u128, Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let meta: JobMeta = dec::<legacy::JobMeta>(&v)?.into();
                t.insert(k, enc(&meta).as_slice())?;
                counts.2 += 1;
            }
        }
        {
            let mut t = tx.open_table(TASKS)?;
            let rows: Vec<((u128, u32), Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let task: TaskRecord = dec::<legacy::TaskRecord>(&v)?.into();
                t.insert(k, enc(&task).as_slice())?;
                counts.3 += 1;
            }
        }
        tx.open_table(META)?
            .insert(SCHEMA_META, 2u32.to_le_bytes().as_slice())?;
        tx.commit()?;
        info!(
            members = counts.0,
            tokens = counts.1,
            jobs = counts.2,
            tasks = counts.3,
            "migrated state database to layout 2"
        );
        Ok(())
    }

    /// Layout 3 adds the microVM price to the stored policy; it starts at
    /// the Docker price.
    fn migrate_v2(&self) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut meta = tx.open_table(META)?;
            let old = meta.get(POLICY_META)?.map(|v| v.value().to_vec());
            if let Some(old) = old {
                let p: legacy::PolicyV3 = dec::<legacy::PolicyV2>(&old)?.into();
                meta.insert(POLICY_META, enc(&p).as_slice())?;
            }
            meta.insert(SCHEMA_META, 3u32.to_le_bytes().as_slice())?;
        }
        tx.commit()?;
        info!("migrated state database to layout 3");
        Ok(())
    }

    /// Layout 7 (phase 15) adds a queue timeout and GPU capabilities to
    /// the job a schedule submits; schedules get neither.
    fn migrate_v6(&self) -> Result<()> {
        let tx = self.db.begin_write()?;
        let mut n = 0;
        {
            let mut t = tx.open_table(SCHEDULES)?;
            let rows: Vec<(String, Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value().to_string(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let r: ScheduleRecord = dec::<legacy::ScheduleRecordV6>(&v)?.into();
                t.insert(k.as_str(), enc(&r).as_slice())?;
                n += 1;
            }
            let mut meta = tx.open_table(META)?;
            meta.insert(SCHEMA_META, 7u32.to_le_bytes().as_slice())?;
        }
        tx.commit()?;
        info!(schedules = n, "migrated state database to layout 7");
        Ok(())
    }

    /// Layout 5 adds GPU rates to the stored policy (at their defaults).
    /// GPUs of nodes and jobs live in tables of their own.
    fn migrate_v4(&self) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut meta = tx.open_table(META)?;
            let old = meta.get(POLICY_META)?.map(|v| v.value().to_vec());
            if let Some(old) = old {
                let p: Policy = dec::<legacy::PolicyV4>(&old)?.into();
                meta.insert(POLICY_META, enc(&p).as_slice())?;
            }
            meta.insert(SCHEMA_META, 5u32.to_le_bytes().as_slice())?;
        }
        tx.commit()?;
        info!("migrated state database to layout 5");
        Ok(())
    }

    /// Layout 4 moves from credits to compute units. The old price of one
    /// core-hour becomes one CU: balances, holds, earnings and task costs
    /// are converted at that rate (balances so that their total converts
    /// exactly, rounded down), each account's ledger starts again with a
    /// `convert` entry for its opening balance, and the credit ledger and
    /// receipts are kept in tables of their own (the receipts are
    /// countersigned again in CU when the control plane starts, see
    /// [`Store::credit_receipts`]). Members get room for a performance
    /// profile.
    fn migrate_v3(&self) -> Result<()> {
        let tx = self.db.begin_write()?;
        let cpu_hour;
        {
            let mut meta = tx.open_table(META)?;
            let old = meta.get(POLICY_META)?.map(|v| v.value().to_vec());
            cpu_hour = match old {
                Some(old) => {
                    let (p, rate) = dec::<legacy::PolicyV3>(&old)?.convert();
                    meta.insert(POLICY_META, enc(&p).as_slice())?;
                    rate
                }
                None => legacy::OLD_CPU_HOUR,
            };
            meta.insert(CREDIT_RATE_META, cpu_hour.to_le_bytes().as_slice())?;
        }
        {
            let mut t = tx.open_table(MEMBERS)?;
            let rows: Vec<(String, Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value().to_string(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let m: Member = dec::<legacy::MemberV3>(&v)?.into();
                t.insert(k.as_str(), enc(&m).as_slice())?;
            }
        }
        let mut holds: Vec<Hold> = Vec::new();
        for row in tx.open_table(HOLDS)?.iter()? {
            holds.push(dec(row?.1.value())?);
        }
        let mut accounts: Vec<AccountView> = Vec::new();
        for row in tx.open_table(ACCOUNTS)?.iter()? {
            accounts.push(dec(row?.1.value())?);
        }
        let before: Vec<AccountView> = accounts.clone();
        legacy::convert_balances(&mut accounts, &mut holds, cpu_hour);
        {
            let mut t = tx.open_table(HOLDS)?;
            for h in &holds {
                t.insert(run_key(&h.task), enc(h).as_slice())?;
            }
            let mut t = tx.open_table(ACCOUNTS)?;
            for a in &accounts {
                t.insert(a.name.as_str(), enc(a).as_slice())?;
            }
        }
        {
            let mut t = tx.open_table(TASKS)?;
            let rows: Vec<((u128, u32), Vec<u8>)> = t
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, v) in rows {
                let mut task: TaskRecord = dec(&v)?;
                if task.cost != 0 {
                    task.cost = legacy::to_milli_cu(task.cost, cpu_hour);
                    t.insert(k, enc(&task).as_slice())?;
                }
            }
        }
        // The credit ledger moves aside; each account starts over with its
        // converted balance.
        let mut next_seq = 1;
        {
            let mut old = tx.open_table(LEDGER)?;
            let rows: Vec<(u64, Vec<u8>)> = old
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            let mut archive = tx.open_table(CREDIT_LEDGER)?;
            for (k, v) in &rows {
                archive.insert(*k, v.as_slice())?;
                old.remove(*k)?;
                next_seq = next_seq.max(k + 1);
            }
            let now = crate::state::now_unix();
            for (a, b) in accounts.iter().zip(&before) {
                if a.balance == 0 && a.held == 0 {
                    continue;
                }
                let e = LedgerEntry {
                    seq: next_seq,
                    unix: now,
                    account: a.name.clone(),
                    kind: LedgerKind::Convert,
                    amount: a.balance,
                    task: None,
                    memo: format!(
                        "converted {} credits ({} held) to CU at {} CU per credit",
                        legacy::format_credits(b.balance),
                        legacy::format_credits(b.held),
                        pumasi_proto::economy::format_rate(1_000_000_000_000 / cpu_hour)
                    ),
                };
                next_seq += 1;
                old.insert(e.seq, enc(&e).as_slice())?;
            }
        }
        {
            let mut old = tx.open_table(RECEIPTS)?;
            let rows: Vec<(RunKey, Vec<u8>)> = old
                .iter()?
                .map(|r| r.map(|(k, v)| (k.value(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            let mut archive = tx.open_table(CREDIT_RECEIPTS)?;
            for (k, v) in &rows {
                archive.insert(*k, v.as_slice())?;
                old.remove(*k)?;
            }
        }
        tx.open_table(META)?
            .insert(SCHEMA_META, 4u32.to_le_bytes().as_slice())?;
        tx.commit()?;
        info!(
            accounts = accounts.len(),
            holds = holds.len(),
            cpu_hour,
            "migrated state database to layout 4 (compute units)"
        );
        Ok(())
    }

    /// Receipts from the credit ledger still to be countersigned in CU,
    /// with the old price of a core-hour. `None` when there is nothing to
    /// convert.
    pub(crate) fn credit_receipts(&self) -> Result<Option<(i64, Vec<legacy::ReceiptV1>)>> {
        let tx = self.read()?;
        let meta = tx.open_table(META)?;
        if meta.get(CREDIT_RECEIPTS_DONE_META)?.is_some() {
            return Ok(None);
        }
        let Some(rate) = meta.get(CREDIT_RATE_META)? else {
            return Ok(None);
        };
        let rate = i64::from_le_bytes(rate.value().try_into().context("corrupt credit rate")?);
        let Ok(t) = tx.open_table(CREDIT_RECEIPTS) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        for row in t.iter()? {
            out.push(dec(row?.1.value())?);
        }
        Ok(Some((rate, out)))
    }

    /// Stores the receipts countersigned again in CU.
    pub(crate) fn finish_credit_receipts(&self, receipts: &[Receipt]) -> Result<()> {
        let mut b = Batch::default();
        for r in receipts {
            b.put(RECEIPTS, run_key(&r.usage.task), enc(r).as_slice());
        }
        b.put(META, CREDIT_RECEIPTS_DONE_META, [1u8].as_slice());
        self.commit(b)
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let tx = self.read()?;
        let t = tx.open_table(META)?;
        Ok(t.get(key)?.map(|v| v.value().to_vec()))
    }

    pub fn put_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut b = Batch::default();
        b.put(META, key, value);
        self.commit(b)
    }

    pub fn delete_meta(&self, key: &str) -> Result<()> {
        let mut b = Batch::default();
        b.del(META, key);
        self.commit(b)
    }

    pub fn put_policy(&self, p: &Policy) -> Result<()> {
        self.put_meta(POLICY_META, &enc(p))
    }

    pub fn load(&self) -> Result<Snapshot> {
        let tx = self.read()?;
        let mut snap = Snapshot::default();
        for row in tx.open_table(MEMBERS)?.iter()? {
            let (_, v) = row?;
            snap.members.push(dec(v.value())?);
        }
        for row in tx.open_table(TOKENS)?.iter()? {
            let (_, v) = row?;
            snap.tokens.push(dec(v.value())?);
        }
        for row in tx.open_table(MODULES)?.iter()? {
            let (k, v) = row?;
            let hash: ModuleHash = k.value().try_into().context("bad module key")?;
            snap.modules.push((hash, v.value().to_vec()));
        }
        let tasks = tx.open_table(TASKS)?;
        for row in tx.open_table(JOBS)?.iter()? {
            let (k, v) = row?;
            let id = k.value();
            let meta: JobMeta = dec(v.value())?;
            let mut list = Vec::new();
            for t in tasks.range((id, 0)..=(id, u32::MAX))? {
                let (_, v) = t?;
                list.push(dec(v.value())?);
            }
            snap.jobs.push((meta, list));
        }
        for row in tx.open_table(ACCOUNTS)?.iter()? {
            let (_, v) = row?;
            snap.accounts.push(dec(v.value())?);
        }
        for row in tx.open_table(HOLDS)?.iter()? {
            let (_, v) = row?;
            snap.holds.push(dec(v.value())?);
        }
        for row in tx.open_table(CARRY)?.iter()? {
            let (k, v) = row?;
            let (c, p) = k.value();
            snap.carry.push(((c.to_string(), p.to_string()), v.value()));
        }
        for row in tx.open_table(JOB_EXT)?.iter()? {
            let (k, v) = row?;
            snap.job_ext.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(TASK_EXT)?.iter()? {
            let (k, v) = row?;
            snap.task_ext.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(TASK_TIMES)?.iter()? {
            let (k, v) = row?;
            snap.task_times.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(TASK_EXEC)?.iter()? {
            let (k, v) = row?;
            snap.task_exec.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(DATASETS)?.iter()? {
            let (_, v) = row?;
            snap.datasets.push(dec(v.value())?);
        }
        for row in tx.open_table(QUOTAS)?.iter()? {
            let (k, v) = row?;
            snap.quotas.push((k.value().to_string(), v.value()));
        }
        for row in tx.open_table(SECRETS)?.iter()? {
            let (k, v) = row?;
            let (set, value): (u64, String) = dec(v.value())?;
            snap.secrets.push((k.value().to_string(), set, value));
        }
        for row in tx.open_table(UPLOADS)?.iter()? {
            let (k, v) = row?;
            let root: [u8; 32] = k.value().try_into().context("bad upload key")?;
            snap.uploads.push((root, dec(v.value())?));
        }
        for row in tx.open_table(MEMBER_DEVICES)?.iter()? {
            let (k, v) = row?;
            snap.devices.push((k.value().to_string(), dec(v.value())?));
        }
        for row in tx.open_table(MEMBER_WEBGPU)?.iter()? {
            let (k, v) = row?;
            snap.webgpu.push((k.value().to_string(), dec(v.value())?));
        }
        for row in tx.open_table(JOB_GPU)?.iter()? {
            let (k, v) = row?;
            snap.job_gpu.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(JOB_SCHED)?.iter()? {
            let (k, v) = row?;
            snap.job_sched.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(NODE_CONFIG)?.iter()? {
            let (k, v) = row?;
            snap.node_config
                .push((k.value().to_string(), dec(v.value())?));
        }
        for row in tx.open_table(ACCOUNT_QUOTAS)?.iter()? {
            let (k, v) = row?;
            snap.account_quotas
                .push((k.value().to_string(), dec(v.value())?));
        }
        for row in tx.open_table(PREEMPTED)?.iter()? {
            let (k, v) = row?;
            snap.preempted.push((k.value(), v.value()));
        }
        for row in tx.open_table(ACCESS)?.iter()? {
            let (_, v) = row?;
            snap.access.push(dec(v.value())?);
        }
        for row in tx.open_table(SCHEDULES)?.iter()? {
            let (_, v) = row?;
            snap.schedules.push(dec(v.value())?);
        }
        for row in tx.open_table(ACCOUNT_POLICIES)?.iter()? {
            let (k, v) = row?;
            snap.account_policies
                .push((k.value().to_string(), dec(v.value())?));
        }
        for row in tx.open_table(JOB_ORIGIN)?.iter()? {
            let (k, v) = row?;
            snap.job_origin.push((k.value(), dec(v.value())?));
        }
        for row in tx.open_table(MEASUREMENTS)?.iter()? {
            let (k, v) = row?;
            snap.measurements
                .push((k.value().to_string(), dec(v.value())?));
        }
        for row in tx.open_table(TRUSTED_NODES)?.iter()? {
            let (k, _) = row?;
            snap.trusted_nodes.push(k.value().to_string());
        }
        snap.next_ledger_seq = tx
            .open_table(LEDGER)?
            .last()?
            .map(|(k, _)| k.value() + 1)
            .unwrap_or(1);
        snap.policy = match tx.open_table(META)?.get(POLICY_META)? {
            Some(v) => Some(dec(v.value())?),
            None => None,
        };
        Ok(snap)
    }

    pub fn put_member(&self, m: &Member) -> Result<()> {
        let mut b = Batch::default();
        b.put(MEMBERS, m.id.as_str(), enc(m).as_slice());
        self.commit(b)
    }

    pub fn remove_member(&self, id: &str) -> Result<()> {
        let mut b = Batch::default();
        b.del(MEMBERS, id);
        b.del(MEMBER_DEVICES, id);
        b.del(MEMBER_WEBGPU, id);
        b.del(NODE_CONFIG, id);
        b.del(TRUSTED_NODES, id);
        self.commit(b)
    }

    pub fn put_devices(&self, id: &str, d: &crate::gpu::DeviceRecord) -> Result<()> {
        let mut b = Batch::default();
        b.put(MEMBER_DEVICES, id, enc(d).as_slice());
        self.commit(b)
    }

    pub fn put_webgpu(&self, id: &str, w: &pumasi_proto::webgpu::SignedWebGpu) -> Result<()> {
        let mut b = Batch::default();
        b.put(MEMBER_WEBGPU, id, enc(w).as_slice());
        self.commit(b)
    }

    pub fn put_token(&self, t: &TokenRecord) -> Result<()> {
        let mut b = Batch::default();
        b.put(TOKENS, t.secret.as_slice(), enc(t).as_slice());
        self.commit(b)
    }

    pub fn remove_token(&self, secret: &Secret) -> Result<()> {
        let mut b = Batch::default();
        b.del(TOKENS, secret.as_slice());
        self.commit(b)
    }

    /// Writes a new job with all its tasks and any new content blobs
    /// (module, shared file bundle) in one transaction. A job's file store
    /// details go to their own tables; a used upload is forgotten.
    pub fn put_new_job(
        &self,
        meta: &JobMeta,
        tasks: &[TaskRecord],
        blobs: &[(ModuleHash, &[u8])],
        used_uploads: &[[u8; 32]],
    ) -> Result<()> {
        let mut b = Batch::default();
        for (hash, bytes) in blobs {
            b.put(MODULES, hash.as_slice(), *bytes);
        }
        b.put(JOBS, meta.id.0, enc(meta).as_slice());
        if !meta.ext.is_empty() {
            b.put(JOB_EXT, meta.id.0, enc(&meta.ext).as_slice());
        }
        if let Some(g) = &meta.gpu {
            b.put(JOB_GPU, meta.id.0, enc(g).as_slice());
        }
        if !meta.sched.is_empty() {
            b.put(JOB_SCHED, meta.id.0, enc(&meta.sched).as_slice());
        }
        if !meta.origin.is_empty() {
            b.put(JOB_ORIGIN, meta.id.0, enc(&meta.origin).as_slice());
        }
        for (i, task) in tasks.iter().enumerate() {
            b.put(TASKS, (meta.id.0, i as u32), enc(task).as_slice());
            if !task.ext.is_empty() {
                b.put(TASK_EXT, (meta.id.0, i as u32), enc(&task.ext).as_slice());
            }
        }
        for root in used_uploads {
            b.del(UPLOADS, root.as_slice());
        }
        self.commit(b)
    }

    /// Deletes old jobs (each with its task count) and the content blobs
    /// no remaining job uses, in one transaction. Receipts and holds stay:
    /// they are billing records.
    pub fn delete_jobs(&self, jobs: &[(u128, u32)], blobs: &[ModuleHash]) -> Result<()> {
        let mut b = Batch::default();
        for &(job, tasks) in jobs {
            b.del(JOBS, job);
            b.del(JOB_EXT, job);
            b.del(JOB_GPU, job);
            b.del(JOB_SCHED, job);
            b.del(JOB_ORIGIN, job);
            for i in 0..tasks {
                b.del(TASKS, (job, i));
                b.del(TASK_EXT, (job, i));
                b.del(TASK_TIMES, (job, i));
                b.del(TASK_EXEC, (job, i));
                b.del(PREEMPTED, (job, i, false));
                b.del(PREEMPTED, (job, i, true));
            }
        }
        for hash in blobs {
            b.del(MODULES, hash.as_slice());
        }
        self.commit(b)
    }

    pub fn put_job_meta(&self, meta: &JobMeta) -> Result<()> {
        let mut b = Batch::default();
        b.put(JOBS, meta.id.0, enc(meta).as_slice());
        if meta.ext.is_empty() {
            b.del(JOB_EXT, meta.id.0);
        } else {
            b.put(JOB_EXT, meta.id.0, enc(&meta.ext).as_slice());
        }
        self.commit(b)
    }

    pub fn put_tasks(&self, job: u128, tasks: &[(u32, &TaskRecord)]) -> Result<()> {
        let mut b = Batch::default();
        for (i, task) in tasks {
            b.put(TASKS, (job, *i), enc(*task).as_slice());
            if task.ext.is_empty() {
                b.del(TASK_EXT, (job, *i));
            } else {
                b.put(TASK_EXT, (job, *i), enc(&task.ext).as_slice());
            }
            if task.times.is_empty() {
                b.del(TASK_TIMES, (job, *i));
            } else {
                b.put(TASK_TIMES, (job, *i), enc(&task.times).as_slice());
            }
            match &task.ran {
                Some(e) => b.put(TASK_EXEC, (job, *i), enc(e).as_slice()),
                None => b.del(TASK_EXEC, (job, *i)),
            }
        }
        self.commit(b)
    }

    /// Writes (or with `None` deletes) one row of a table keyed by a
    /// string.
    fn put_str_row(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: Option<Vec<u8>>,
    ) -> Result<()> {
        let mut b = Batch::default();
        match value {
            Some(v) => b.put(table, key, v.as_slice()),
            None => b.del(table, key),
        }
        self.commit(b)
    }

    pub fn put_node_config(&self, id: &str, c: Option<&NodeConfig>) -> Result<()> {
        self.put_str_row(NODE_CONFIG, id, c.map(enc))
    }

    pub fn put_account_quota(&self, account: &str, q: Option<&AccountQuota>) -> Result<()> {
        self.put_str_row(ACCOUNT_QUOTAS, account, q.map(enc))
    }

    pub fn put_account_policy(&self, account: &str, p: Option<&AccountPolicy>) -> Result<()> {
        self.put_str_row(ACCOUNT_POLICIES, account, p.map(enc))
    }

    pub fn put_measurement(
        &self,
        id: &str,
        m: Option<&crate::confidential::MeasurementRecord>,
    ) -> Result<()> {
        self.put_str_row(MEASUREMENTS, id, m.map(enc))
    }

    pub fn put_trusted_node(&self, id: &str, trusted: bool) -> Result<()> {
        self.put_str_row(TRUSTED_NODES, id, trusted.then(Vec::new))
    }

    pub fn put_job_origin(&self, job: u128, o: &JobOrigin) -> Result<()> {
        let mut b = Batch::default();
        b.put(JOB_ORIGIN, job, enc(o).as_slice());
        self.commit(b)
    }

    pub fn put_schedule(&self, name: &str, r: Option<&ScheduleRecord>) -> Result<()> {
        self.put_str_row(SCHEDULES, name, r.map(enc))
    }

    pub fn put_preempted(&self, key: (u128, u32, bool), count: u32) -> Result<()> {
        let mut b = Batch::default();
        b.put(PREEMPTED, key, count);
        self.commit(b)
    }

    pub fn put_access(&self, r: &AccessRecord) -> Result<()> {
        let mut b = Batch::default();
        b.put(ACCESS, r.secret.as_slice(), enc(r).as_slice());
        self.commit(b)
    }

    pub fn remove_access(&self, secret: &Secret) -> Result<()> {
        let mut b = Batch::default();
        b.del(ACCESS, secret.as_slice());
        self.commit(b)
    }

    // -- monitoring (phase 11) ---------------------------------------------

    pub fn put_events(&self, events: &[EventView]) -> Result<()> {
        let mut b = Batch::default();
        for e in events {
            b.put(EVENTS, e.seq, enc(e).as_slice());
        }
        self.commit(b)
    }

    /// The newest `limit` events, oldest first.
    pub fn load_events(&self, limit: usize) -> Result<Vec<EventView>> {
        let tx = self.read()?;
        let mut out = Vec::new();
        for row in tx.open_table(EVENTS)?.iter()?.rev().take(limit) {
            let (_, v) = row?;
            out.push(dec(v.value())?);
        }
        out.reverse();
        Ok(out)
    }

    pub fn put_points(&self, points: &[(String, u8, MetricPoint)]) -> Result<()> {
        let mut b = Batch::default();
        for (node, tier, p) in points {
            b.put(METRICS, (node.as_str(), *tier, p.unix), enc(p).as_slice());
        }
        self.commit(b)
    }

    /// Every stored point not older than `since[tier]`.
    pub fn load_points(&self, since: &[u64]) -> Result<Vec<(String, u8, MetricPoint)>> {
        let tx = self.read()?;
        let mut out = Vec::new();
        for row in tx.open_table(METRICS)?.iter()? {
            let (k, v) = row?;
            let (node, tier, unix) = k.value();
            if since.get(tier as usize).is_some_and(|s| unix >= *s) {
                out.push((node.to_string(), tier, dec(v.value())?));
            }
        }
        Ok(out)
    }

    /// Deletes events before `events_before` (and beyond the newest
    /// `max_events`), points older than `points_before[tier]`, and every
    /// point of nodes not in `nodes`. With replication the rows are picked
    /// from what is applied so far, which only misses rows too new to go.
    pub fn prune_monitor(
        &self,
        events_before: u64,
        max_events: usize,
        points_before: &[u64],
        nodes: &std::collections::HashSet<String>,
    ) -> Result<(usize, usize)> {
        let mut b = Batch::default();
        let events;
        let points;
        {
            let tx = self.read()?;
            let t = tx.open_table(EVENTS)?;
            let total = t.len()? as usize;
            let mut doomed = Vec::new();
            for (i, row) in t.iter()?.enumerate() {
                let (k, v) = row?;
                let e: EventView = dec(v.value())?;
                if e.unix < events_before || total - i > max_events {
                    doomed.push(k.value());
                } else {
                    break;
                }
            }
            for k in &doomed {
                b.del(EVENTS, *k);
            }
            events = doomed.len();
            let t = tx.open_table(METRICS)?;
            let mut n = 0;
            for row in t.iter()? {
                let (k, _) = row?;
                let (node, tier, unix) = k.value();
                let old = points_before.get(tier as usize).is_none_or(|b| unix < *b);
                if old || !nodes.contains(node) {
                    b.del(METRICS, (node, tier, unix));
                    n += 1;
                }
            }
            points = n;
        }
        self.commit(b)?;
        Ok((events, points))
    }

    pub fn alert_policy(&self) -> Result<Option<AlertPolicy>> {
        self.get_meta(ALERT_POLICY_META)?
            .map(|b| dec(&b))
            .transpose()
    }

    pub fn put_alert_policy(&self, p: &AlertPolicy) -> Result<()> {
        self.put_meta(ALERT_POLICY_META, &enc(p))
    }

    /// A record kept under a meta key (1.6: alert tuning, silences, the
    /// network links), in postcard.
    pub fn meta_record<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_meta(key)?.map(|b| dec(&b)).transpose()
    }

    pub fn put_meta_record<T: Serialize>(&self, key: &str, v: &T) -> Result<()> {
        self.put_meta(key, &enc(v))
    }

    pub fn webhooks(&self) -> Result<Vec<WebhookSpec>> {
        let tx = self.read()?;
        let mut out = Vec::new();
        for row in tx.open_table(WEBHOOKS)?.iter()? {
            let (_, v) = row?;
            out.push(dec(v.value())?);
        }
        Ok(out)
    }

    pub fn put_webhook(&self, name: &str, w: Option<&WebhookSpec>) -> Result<()> {
        self.put_str_row(WEBHOOKS, name, w.map(enc))
    }

    /// What each account spent (charges, storage, egress) since `since`:
    /// (account, unix, milli-CU), oldest first.
    pub fn spent_since(&self, since: u64) -> Result<Vec<(String, u64, i64)>> {
        let tx = self.read()?;
        let mut out = Vec::new();
        for row in tx.open_table(LEDGER)?.iter()?.rev() {
            let (_, v) = row?;
            let e: LedgerEntry = dec(v.value())?;
            if e.unix < since {
                break;
            }
            if e.amount < 0
                && matches!(
                    e.kind,
                    LedgerKind::Charge | LedgerKind::Storage | LedgerKind::Egress
                )
            {
                out.push((e.account, e.unix, -e.amount));
            }
        }
        out.reverse();
        Ok(out)
    }

    pub fn put_dataset(&self, d: &DatasetRecord) -> Result<()> {
        let mut b = Batch::default();
        b.put(DATASETS, (d.name.as_str(), d.version), enc(d).as_slice());
        self.commit(b)
    }

    pub fn remove_datasets(&self, keys: &[(String, u32)]) -> Result<()> {
        let mut b = Batch::default();
        for (n, v) in keys {
            b.del(DATASETS, (n.as_str(), *v));
        }
        self.commit(b)
    }

    pub fn set_quota(&self, account: &str, bytes: Option<u64>) -> Result<()> {
        let mut b = Batch::default();
        match bytes {
            Some(n) => b.put(QUOTAS, account, n),
            None => b.del(QUOTAS, account),
        }
        self.commit(b)
    }

    pub fn put_secret(&self, name: &str, set_unix: u64, value: Option<&str>) -> Result<()> {
        let mut b = Batch::default();
        match value {
            Some(v) => b.put(SECRETS, name, enc(&(set_unix, v.to_string())).as_slice()),
            None => b.del(SECRETS, name),
        }
        self.commit(b)
    }

    pub fn put_upload(
        &self,
        root: &[u8; 32],
        u: Option<&crate::state::UploadRecord>,
    ) -> Result<()> {
        let mut b = Batch::default();
        match u {
            Some(u) => b.put(UPLOADS, root.as_slice(), enc(u).as_slice()),
            None => b.del(UPLOADS, root.as_slice()),
        }
        self.commit(b)
    }

    /// Applies balance changes, ledger entries, holds and a receipt
    /// atomically, so money is never half-moved (with replication, one
    /// log entry: every replica has all of it or none).
    pub fn write_ledger(&self, w: &LedgerWrite<'_>) -> Result<()> {
        let mut b = Batch::default();
        for a in &w.accounts {
            b.put(ACCOUNTS, a.name.as_str(), enc(*a).as_slice());
        }
        for e in w.entries {
            b.put(LEDGER, e.seq, enc(e).as_slice());
        }
        for h in w.put_holds {
            b.put(HOLDS, run_key(&h.task), enc(h).as_slice());
        }
        for k in w.del_holds {
            b.del(HOLDS, run_key(k));
        }
        for (c, p, v) in w.carries {
            b.put(CARRY, (c.as_str(), p.as_str()), *v);
        }
        if let Some(r) = w.receipt {
            b.put(RECEIPTS, run_key(&r.usage.task), enc(r).as_slice());
        }
        self.commit(b)
    }

    /// Ledger entries, newest first, optionally for one account.
    pub fn ledger(&self, account: Option<&str>, limit: usize) -> Result<Vec<LedgerEntry>> {
        let tx = self.read()?;
        let mut out = Vec::new();
        for row in tx.open_table(LEDGER)?.iter()?.rev() {
            if out.len() >= limit {
                break;
            }
            let (_, v) = row?;
            let e: LedgerEntry = dec(v.value())?;
            if account.is_none_or(|a| a == e.account) {
                out.push(e);
            }
        }
        Ok(out)
    }

    // -- audit log and SSO (phase 17) ----------------------------------------

    pub fn put_audit(&self, e: &AuditEntry) -> Result<()> {
        let mut b = Batch::default();
        b.put(AUDIT, e.seq, enc(e).as_slice());
        self.commit(b)
    }

    /// The newest audit entry.
    pub fn last_audit(&self) -> Result<Option<AuditEntry>> {
        let tx = self.read()?;
        let t = tx.open_table(AUDIT)?;
        t.last()?.map(|(_, v)| dec(v.value())).transpose()
    }

    /// Up to `limit` audit entries after `after`, oldest first; with
    /// `after` 0, the newest `limit`.
    pub fn audit(&self, after: u64, limit: usize) -> Result<Vec<AuditEntry>> {
        let tx = self.read()?;
        let t = tx.open_table(AUDIT)?;
        let mut out = Vec::new();
        if after == 0 {
            for row in t.iter()?.rev().take(limit) {
                out.push(dec(row?.1.value())?);
            }
            out.reverse();
        } else {
            for row in t.range(after + 1..)?.take(limit) {
                out.push(dec(row?.1.value())?);
            }
        }
        Ok(out)
    }

    /// Walks every audit entry in order, in one read transaction.
    pub fn each_audit(&self, mut f: impl FnMut(AuditEntry) -> Result<()>) -> Result<()> {
        let tx = self.read()?;
        let t = tx.open_table(AUDIT)?;
        for row in t.iter()? {
            f(dec(row?.1.value())?)?;
        }
        Ok(())
    }

    pub fn sso(&self) -> Result<Option<crate::sso::SsoConfig>> {
        // Phase 17 stored it without `device`; postcard has no field
        // names, so the old layout is read as such.
        self.get_meta(SSO_META)?
            .map(|b| {
                dec::<crate::sso::SsoConfig>(&b)
                    .or_else(|_| dec::<crate::sso::SsoConfigV1>(&b).map(Into::into))
            })
            .transpose()
    }

    pub fn put_sso(&self, c: &crate::sso::SsoConfig) -> Result<()> {
        self.put_meta(SSO_META, &enc(c))
    }

    /// Every receipt of one job, in task order.
    pub fn receipts(&self, job: u128) -> Result<Vec<Receipt>> {
        let tx = self.read()?;
        let t = tx.open_table(RECEIPTS)?;
        let mut out = Vec::new();
        for row in t.range((job, 0, 0, false)..=(job, u32::MAX, u32::MAX, true))? {
            let (_, v) = row?;
            out.push(Receipt::decode(v.value()).context("corrupt receipt in state database")?);
        }
        Ok(out)
    }
}

/// Older record layouts, kept only to migrate old databases.
pub(crate) mod legacy {
    use std::collections::HashMap;

    use pumasi_proto::{
        economy::{CuRates, LedgerMode, MICRO_PER_CU, Policy},
        msg::AccountView,
        receipt::Usage,
    };
    use serde::Serialize;

    use crate::state::Hold;

    /// Layout 2 policy: no microVM price.
    #[derive(serde::Deserialize)]
    pub struct PolicyV2 {
        prices: PricesV2,
        hold_secs: u64,
        verify_permille: u32,
        max_strikes: u32,
    }

    #[derive(serde::Deserialize)]
    struct PricesV2 {
        wasm_per_gfuel: i64,
        docker_cpu_hour: i64,
        memory_gib_hour: i64,
    }

    impl From<PolicyV2> for PolicyV3 {
        fn from(p: PolicyV2) -> Self {
            Self {
                prices: PricesV3 {
                    wasm_per_gfuel: p.prices.wasm_per_gfuel,
                    docker_cpu_hour: p.prices.docker_cpu_hour,
                    microvm_cpu_hour: p.prices.docker_cpu_hour,
                    memory_gib_hour: p.prices.memory_gib_hour,
                },
                hold_secs: p.hold_secs,
                verify_permille: p.verify_permille,
                max_strikes: p.max_strikes,
            }
        }
    }

    /// Layout 3 policy: a price list in micro-credits.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct PolicyV3 {
        pub prices: PricesV3,
        pub hold_secs: u64,
        pub verify_permille: u32,
        pub max_strikes: u32,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct PricesV3 {
        pub wasm_per_gfuel: i64,
        pub docker_cpu_hour: i64,
        pub microvm_cpu_hour: i64,
        pub memory_gib_hour: i64,
    }

    /// Layout 4 policy: CU rates without GPU rates.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct PolicyV4 {
        pub rates: RatesV4,
        pub hold_secs: u64,
        pub verify_permille: u32,
        pub max_strikes: u32,
        pub ledger: LedgerMode,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct RatesV4 {
        pub memory_gib_hour: i64,
        pub storage_gib_month: i64,
        pub egress_gib: i64,
    }

    impl From<PolicyV4> for Policy {
        fn from(p: PolicyV4) -> Self {
            Policy {
                rates: CuRates {
                    memory_gib_hour: p.rates.memory_gib_hour,
                    storage_gib_month: p.rates.storage_gib_month,
                    egress_gib: p.rates.egress_gib,
                    ..CuRates::default()
                },
                hold_secs: p.hold_secs,
                verify_permille: p.verify_permille,
                max_strikes: p.max_strikes,
                ledger: p.ledger,
            }
        }
    }

    /// The old default price of a core-hour (0.05 credits).
    pub const OLD_CPU_HOUR: i64 = 50_000;
    const OLD_DEFAULT: PricesV3 = PricesV3 {
        wasm_per_gfuel: 10_000,
        docker_cpu_hour: 50_000,
        microvm_cpu_hour: 50_000,
        memory_gib_hour: 5_000,
    };

    impl PolicyV3 {
        /// The CU policy, and the price of a core-hour that becomes one CU.
        /// The old default prices give the new default rates; custom ones
        /// keep their memory-to-CPU ratio.
        pub fn convert(self) -> (PolicyV4, i64) {
            let p = &self.prices;
            let cpu_hour = [p.docker_cpu_hour, p.microvm_cpu_hour]
                .into_iter()
                .find(|&v| v > 0)
                .unwrap_or(OLD_CPU_HOUR);
            let defaults = CuRates::default();
            let rates = RatesV4 {
                memory_gib_hour: if self.prices == OLD_DEFAULT {
                    defaults.memory_gib_hour
                } else {
                    (p.memory_gib_hour.max(0) as i128 * MICRO_PER_CU as i128 / cpu_hour as i128)
                        as i64
                },
                storage_gib_month: defaults.storage_gib_month,
                egress_gib: defaults.egress_gib,
            };
            (
                PolicyV4 {
                    rates,
                    hold_secs: self.hold_secs,
                    verify_permille: self.verify_permille,
                    max_strikes: self.max_strikes,
                    ledger: LedgerMode::Prepaid,
                },
                cpu_hour,
            )
        }
    }

    /// Micro-credits to milli-CU, rounded down.
    pub fn to_milli_cu(micro_credits: i64, cpu_hour: i64) -> i64 {
        let (n, d) = pumasi_proto::economy::credits_to_milli_cu_num(micro_credits, cpu_hour);
        n.div_euclid(d) as i64
    }

    pub fn format_credits(m: i64) -> String {
        let sign = if m < 0 { "-" } else { "" };
        let abs = m.unsigned_abs();
        let (whole, frac) = (abs / 1_000_000, abs % 1_000_000);
        if frac == 0 {
            return format!("{sign}{whole}");
        }
        let frac = format!("{frac:06}");
        format!("{sign}{whole}.{}", frac.trim_end_matches('0'))
    }

    /// Converts balances and holds from micro-credits to milli-CU. Holds
    /// and the spent and earned totals round down one by one; the
    /// available balances are set so that every account's available plus
    /// held is its old total converted, and the cluster-wide total of
    /// those converts exactly (rounded down once): the remainders go to
    /// the accounts that lost most to rounding.
    pub fn convert_balances(accounts: &mut [AccountView], holds: &mut [Hold], cpu_hour: i64) {
        let d = cpu_hour.max(1) as i128;
        let mut held: HashMap<String, i64> = HashMap::new();
        for h in holds.iter_mut() {
            h.amount = to_milli_cu(h.amount, cpu_hour);
            *held.entry(h.provider.clone()).or_default() += h.amount;
        }
        let nums: Vec<i128> = accounts
            .iter()
            .map(|a| (a.balance as i128 + a.held as i128) * 1000)
            .collect();
        let target = nums.iter().sum::<i128>().div_euclid(d);
        let mut totals: Vec<i128> = nums.iter().map(|n| n.div_euclid(d)).collect();
        let mut extra = target - totals.iter().sum::<i128>();
        let mut order: Vec<usize> = (0..accounts.len()).collect();
        order.sort_by(|&a, &b| {
            nums[b]
                .rem_euclid(d)
                .cmp(&nums[a].rem_euclid(d))
                .then(accounts[a].name.cmp(&accounts[b].name))
        });
        for i in order {
            if extra <= 0 {
                break;
            }
            totals[i] += 1;
            extra -= 1;
        }
        for (a, total) in accounts.iter_mut().zip(totals) {
            let h = held.get(&a.name).copied().unwrap_or(0);
            a.held = h;
            a.balance = total as i64 - h;
            a.spent = to_milli_cu(a.spent, cpu_hour);
            a.earned = to_milli_cu(a.earned, cpu_hour);
        }
    }

    /// Layout 3 receipt billing, in micro-credits.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct BillingV1 {
        pub consumer: String,
        pub provider: String,
        pub billed_wall_ms: u64,
        pub amount: i64,
        pub check: bool,
        pub recorded_unix: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct ReceiptV1 {
        pub usage: Usage,
        pub node_sig: iroh::Signature,
        pub billing: BillingV1,
        pub cluster_sig: iroh::Signature,
    }

    /// Layout 2 and 3 member: no performance profile.
    #[derive(Serialize, Deserialize)]
    pub struct MemberV3 {
        pub id: String,
        pub info: NodeInfo,
        pub joined_unix: u64,
        pub role: Role,
        pub account: String,
        pub allow_docker: bool,
        pub checks_passed: u32,
        pub strikes: u32,
    }

    impl From<MemberV3> for crate::state::Member {
        fn from(m: MemberV3) -> Self {
            Self {
                id: m.id,
                info: m.info,
                joined_unix: m.joined_unix,
                role: m.role,
                account: m.account,
                allow_docker: m.allow_docker,
                checks_passed: m.checks_passed,
                strikes: m.strikes,
                perf: None,
                perf_check: None,
            }
        }
    }

    use pumasi_proto::{
        ModuleHash, Secret,
        job::{DockerSpec, Executor, FileEntry, JobId, Limits, TaskInput, Tier},
        msg::{NodeInfo, OWNER_ACCOUNT, Role, TaskState},
    };
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Member {
        id: String,
        info: NodeInfo,
        joined_unix: u64,
    }

    impl From<Member> for MemberV3 {
        fn from(m: Member) -> Self {
            Self {
                id: m.id,
                info: m.info,
                joined_unix: m.joined_unix,
                role: Role::Owner,
                account: OWNER_ACCOUNT.into(),
                allow_docker: true,
                checks_passed: 0,
                strikes: 0,
            }
        }
    }

    #[derive(Deserialize)]
    pub struct TokenRecord {
        secret: Secret,
        expires_unix: u64,
        uses_left: Option<u32>,
    }

    impl From<TokenRecord> for crate::state::TokenRecord {
        fn from(t: TokenRecord) -> Self {
            Self {
                secret: t.secret,
                expires_unix: t.expires_unix,
                uses_left: t.uses_left,
                invite: None,
            }
        }
    }

    #[derive(Deserialize)]
    pub struct JobMeta {
        id: JobId,
        name: String,
        executor: Executor,
        module_hash: Option<ModuleHash>,
        docker: Option<DockerSpec>,
        files_hash: Option<ModuleHash>,
        limits: Limits,
        tier: Tier,
        max_attempts: u32,
        max_price_per_hour: Option<f64>,
        submitted_unix: u64,
        cancelled: bool,
        seq: u64,
    }

    impl From<JobMeta> for crate::state::JobMeta {
        fn from(m: JobMeta) -> Self {
            Self {
                id: m.id,
                name: m.name,
                executor: m.executor,
                module_hash: m.module_hash,
                docker: m.docker,
                files_hash: m.files_hash,
                limits: m.limits,
                tier: m.tier,
                max_attempts: m.max_attempts,
                max_price_per_hour: m.max_price_per_hour,
                submitted_unix: m.submitted_unix,
                cancelled: m.cancelled,
                seq: m.seq,
                account: OWNER_ACCOUNT.into(),
                deterministic: false,
                ext: Default::default(),
                gpu: None,
                sched: Default::default(),
                origin: Default::default(),
            }
        }
    }

    #[derive(Deserialize)]
    pub struct TaskRecord {
        input: TaskInput,
        state: TaskState,
        attempts: u32,
        node: Option<String>,
        fuel_used: u64,
        wall_ms: u64,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        output_files: Vec<FileEntry>,
    }

    impl From<TaskRecord> for crate::state::TaskRecord {
        fn from(t: TaskRecord) -> Self {
            Self {
                ran: None,
                input: t.input,
                state: t.state,
                attempts: t.attempts,
                node: t.node,
                fuel_used: t.fuel_used,
                wall_ms: t.wall_ms,
                stdout: t.stdout,
                stderr: t.stderr,
                output_files: t.output_files,
                check: None,
                cost: 0,
                ext: Default::default(),
                times: Default::default(),
            }
        }
    }

    /// Layout 6 schedules: the job submission had no queue timeout or GPU
    /// capabilities.
    #[derive(serde::Deserialize)]
    pub struct ScheduleRecordV6 {
        spec: ScheduleSpecV6,
        next_unix: Option<u64>,
        last_unix: Option<u64>,
        last_job: Option<pumasi_proto::job::JobId>,
        last_error: Option<String>,
        runs: u64,
    }

    #[derive(serde::Deserialize)]
    struct ScheduleSpecV6 {
        name: String,
        cron: String,
        job: JobSubmissionV6,
        overlap: bool,
    }

    #[derive(serde::Deserialize)]
    struct JobSubmissionV6 {
        name: String,
        executor: pumasi_proto::job::Executor,
        module: Vec<u8>,
        docker: Option<pumasi_proto::job::DockerSpec>,
        files: Vec<pumasi_proto::job::FileEntry>,
        tasks: Vec<pumasi_proto::job::TaskInput>,
        limits: pumasi_proto::job::Limits,
        tier: pumasi_proto::job::Tier,
        max_attempts: u32,
        max_price_per_hour: Option<f64>,
        account: Option<String>,
        deterministic: bool,
        mounts: Vec<pumasi_proto::store::MountRequest>,
        task_mounts: Vec<Vec<pumasi_proto::store::MountRequest>>,
        output_dataset: Option<String>,
        gpu: Option<pumasi_proto::gpu::GpuRequest>,
        priority: i32,
        selector: pumasi_proto::manage::Selector,
        tolerations: Vec<pumasi_proto::manage::Toleration>,
        after: Vec<pumasi_proto::job::JobId>,
    }

    impl From<ScheduleRecordV6> for crate::manage::ScheduleRecord {
        fn from(r: ScheduleRecordV6) -> Self {
            let j = r.spec.job;
            Self {
                spec: pumasi_proto::manage::ScheduleSpec {
                    name: r.spec.name,
                    cron: r.spec.cron,
                    job: pumasi_proto::job::JobSubmission {
                        name: j.name,
                        executor: j.executor,
                        module: j.module,
                        docker: j.docker,
                        files: j.files,
                        tasks: j.tasks,
                        limits: j.limits,
                        tier: j.tier,
                        max_attempts: j.max_attempts,
                        max_price_per_hour: j.max_price_per_hour,
                        account: j.account,
                        deterministic: j.deterministic,
                        mounts: j.mounts,
                        task_mounts: j.task_mounts,
                        output_dataset: j.output_dataset,
                        gpu: j.gpu,
                        priority: j.priority,
                        selector: j.selector,
                        tolerations: j.tolerations,
                        after: j.after,
                        queue_timeout_ms: None,
                        gpu_capabilities: Vec::new(),
                    },
                    overlap: r.spec.overlap,
                },
                next_unix: r.next_unix,
                last_unix: r.last_unix,
                last_job: r.last_job,
                last_error: r.last_error,
                runs: r.runs,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Hold;
    use pumasi_proto::{
        job::{Executor, JobId, Limits, TaskInput, Tier},
        msg::{NodeInfo, TaskState},
    };
    use serde::Serialize;

    /// Phase 1 layouts, written the way a phase 1 binary wrote them.
    #[derive(Serialize)]
    struct V1Member {
        id: String,
        info: NodeInfo,
        joined_unix: u64,
    }

    #[derive(Serialize)]
    struct V1Job {
        id: JobId,
        name: String,
        executor: Executor,
        module_hash: Option<ModuleHash>,
        docker: Option<pumasi_proto::job::DockerSpec>,
        files_hash: Option<ModuleHash>,
        limits: Limits,
        tier: Tier,
        max_attempts: u32,
        max_price_per_hour: Option<f64>,
        submitted_unix: u64,
        cancelled: bool,
        seq: u64,
    }

    #[derive(Serialize)]
    struct V1Task {
        input: TaskInput,
        state: TaskState,
        attempts: u32,
        node: Option<String>,
        fuel_used: u64,
        wall_ms: u64,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        output_files: Vec<pumasi_proto::job::FileEntry>,
    }

    #[derive(serde::Serialize)]
    struct V2Prices {
        wasm_per_gfuel: i64,
        docker_cpu_hour: i64,
        memory_gib_hour: i64,
    }

    #[derive(serde::Serialize)]
    struct V2Policy {
        prices: V2Prices,
        hold_secs: u64,
        verify_permille: u32,
        max_strikes: u32,
    }

    #[test]
    fn migrates_layout6_schedules() {
        use pumasi_proto::job::{Executor, JobSubmission, Limits, TaskInput, Tier};
        let path =
            std::env::temp_dir().join(format!("pumasi-store-{}.redb", rand::random::<u64>()));
        let r = crate::manage::ScheduleRecord {
            spec: pumasi_proto::manage::ScheduleSpec {
                name: "nightly".into(),
                cron: "@daily".into(),
                job: JobSubmission {
                    name: "report".into(),
                    executor: Executor::Wasm,
                    module: b"\0asm\x01\0\0\0".to_vec(),
                    docker: None,
                    files: vec![],
                    tasks: vec![TaskInput::default(); 2],
                    limits: Limits::default(),
                    tier: Tier::Open,
                    max_attempts: 2,
                    max_price_per_hour: None,
                    account: Some("lab".into()),
                    deterministic: true,
                    mounts: vec![],
                    task_mounts: vec![],
                    output_dataset: None,
                    gpu: None,
                    priority: 3,
                    selector: Default::default(),
                    tolerations: vec![],
                    after: vec![],
                    queue_timeout_ms: None,
                    gpu_capabilities: vec![],
                },
                overlap: true,
            },
            next_unix: Some(1_800_000_000),
            last_unix: None,
            last_job: None,
            last_error: Some("x".into()),
            runs: 4,
        };
        // A layout 6 record is the same without the job's last two fields
        // (an empty option and an empty list, a byte each).
        let job = enc(&r.spec.job);
        let record = |job: &[u8]| {
            let mut b = enc(&r.spec.name);
            b.extend(enc(&r.spec.cron));
            b.extend_from_slice(job);
            b.extend(enc(&r.spec.overlap));
            b.extend(enc(&r.next_unix));
            b.extend(enc(&r.last_unix));
            b.extend(enc(&r.last_job));
            b.extend(enc(&r.last_error));
            b.extend(enc(&r.runs));
            b
        };
        assert_eq!(record(&job), enc(&r));
        let old = record(&job[..job.len() - 2]);
        {
            let store = Store::open(&path).unwrap();
            let tx = store.db.begin_write().unwrap();
            {
                let mut t = tx.open_table(SCHEDULES).unwrap();
                t.insert("nightly", old.as_slice()).unwrap();
                let mut meta = tx.open_table(META).unwrap();
                meta.insert(SCHEMA_META, 6u32.to_le_bytes().as_slice())
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        let snap = Store::open(&path).unwrap().load().unwrap();
        assert_eq!(snap.schedules.len(), 1);
        assert_eq!(enc(&snap.schedules[0]), enc(&r));
        assert_eq!(snap.schedules[0].spec.job.priority, 3);
        // Reopening does not migrate again.
        let again = Store::open(&path).unwrap().load().unwrap();
        assert_eq!(enc(&again.schedules[0]), enc(&r));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn migrates_a_layout2_policy() {
        let path =
            std::env::temp_dir().join(format!("pumasi-store-{}.redb", rand::random::<u64>()));
        {
            let db = Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                let mut meta = tx.open_table(META).unwrap();
                meta.insert(SCHEMA_META, 2u32.to_le_bytes().as_slice())
                    .unwrap();
                let old = V2Policy {
                    prices: V2Prices {
                        wasm_per_gfuel: 1,
                        docker_cpu_hour: 7,
                        memory_gib_hour: 3,
                    },
                    hold_secs: 60,
                    verify_permille: 250,
                    max_strikes: 2,
                };
                meta.insert(POLICY_META, enc(&old).as_slice()).unwrap();
            }
            tx.open_table(MEMBERS).unwrap();
            tx.commit().unwrap();
        }
        let p = Store::open(&path).unwrap().load().unwrap().policy.unwrap();
        // Through layout 3 (microVM price = docker price) to CU: memory
        // keeps its ratio to a core-hour.
        assert_eq!(p.rates.memory_gib_hour, 3 * 1_000_000 / 7);
        assert_eq!(p.ledger, pumasi_proto::economy::LedgerMode::Prepaid);
        assert_eq!(
            (p.hold_secs, p.verify_permille, p.max_strikes),
            (60, 250, 2)
        );
        // Reopening does not migrate again.
        let again = Store::open(&path).unwrap().load().unwrap().policy.unwrap();
        assert_eq!(again, p);
        let _ = std::fs::remove_file(&path);
    }

    fn account(name: &str, balance: i64, held: i64, spent: i64, earned: i64) -> AccountView {
        AccountView {
            name: name.into(),
            balance,
            held,
            spent,
            earned,
        }
    }

    #[test]
    fn converts_balances_so_the_total_matches() {
        let task = TaskRef {
            job: JobId(1),
            index: 0,
            attempt: 1,
            check: false,
        };
        let mut accounts = vec![
            account("owner", 962_500, 0, 37_500, 0),
            account("bob", 25_000, 12_500, 0, 25_000),
            account("alice", 30, 0, 0, 0),
            account("dave", 30, 0, 0, 0),
            account("carol", -333, 0, 333, 0),
        ];
        let mut holds = vec![Hold {
            task,
            consumer: "owner".into(),
            provider: "bob".into(),
            amount: 12_500,
            release_after_unix: 9,
        }];
        let total_before: i64 = accounts.iter().map(|a| a.balance + a.held).sum();
        legacy::convert_balances(&mut accounts, &mut holds, legacy::OLD_CPU_HOUR);
        let total_after: i64 = accounts.iter().map(|a| a.balance + a.held).sum();
        // 1 credit = 20 CU at the old default price: micro-credits / 50.
        assert_eq!(total_after, (total_before * 1000).div_euclid(50_000));
        let get = |n: &str| accounts.iter().find(|a| a.name == n).unwrap().clone();
        assert_eq!(get("owner"), account("owner", 19_250, 0, 750, 0));
        assert_eq!(get("bob"), account("bob", 500, 250, 0, 500));
        assert_eq!(holds[0].amount, 250);
        assert_eq!(get("carol").balance, -7);
        // 0.6 + 0.6 + 0.34 milli-CU lost to rounding make one: it goes to
        // the largest remainder (alice and dave tie; by name).
        assert_eq!((get("alice").balance, get("dave").balance), (1, 0));
    }

    #[test]
    fn migrates_a_layout3_database_to_compute_units() {
        use crate::state::State;
        use iroh::SecretKey;
        use pumasi_proto::{
            msg::{Outcome, Role},
            receipt::{SignedUsage, Usage, result_digest},
        };
        let path =
            std::env::temp_dir().join(format!("pumasi-store-{}.redb", rand::random::<u64>()));
        let cluster = SecretKey::generate();
        let node = SecretKey::generate();
        let task = TaskRef {
            job: JobId(3),
            index: 0,
            attempt: 1,
            check: false,
        };
        {
            let db = Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                let mut meta = tx.open_table(META).unwrap();
                meta.insert(SCHEMA_META, 3u32.to_le_bytes().as_slice())
                    .unwrap();
                let old = legacy::PolicyV3 {
                    prices: legacy::PricesV3 {
                        wasm_per_gfuel: 10_000,
                        docker_cpu_hour: 50_000,
                        microvm_cpu_hour: 50_000,
                        memory_gib_hour: 5_000,
                    },
                    hold_secs: 60,
                    verify_permille: 100,
                    max_strikes: 3,
                };
                meta.insert(POLICY_META, enc(&old).as_slice()).unwrap();
                let m = legacy::MemberV3 {
                    id: node.public().to_string(),
                    info: NodeInfo {
                        name: "bob".into(),
                        os: "linux".into(),
                        arch: "x86_64".into(),
                        cpus: 2,
                        memory_bytes: 1 << 30,
                        slots: 1,
                        agent_version: "0.6".into(),
                        executors: vec![Executor::Wasm],
                    },
                    joined_unix: 1,
                    role: Role::Provider,
                    account: "bob".into(),
                    allow_docker: false,
                    checks_passed: 4,
                    strikes: 1,
                };
                tx.open_table(MEMBERS)
                    .unwrap()
                    .insert(m.id.as_str(), enc(&m).as_slice())
                    .unwrap();
                let mut accounts = tx.open_table(ACCOUNTS).unwrap();
                for a in [
                    account("owner", 962_500, 0, 37_500, 0),
                    account("bob", 25_000, 12_500, 0, 25_000),
                ] {
                    accounts
                        .insert(a.name.as_str(), enc(&a).as_slice())
                        .unwrap();
                }
                let hold = Hold {
                    task,
                    consumer: "owner".into(),
                    provider: "bob".into(),
                    amount: 12_500,
                    release_after_unix: 9,
                };
                tx.open_table(HOLDS)
                    .unwrap()
                    .insert(run_key(&task), enc(&hold).as_slice())
                    .unwrap();
                let entry = LedgerEntry {
                    seq: 7,
                    unix: 1,
                    account: "owner".into(),
                    kind: LedgerKind::Deposit,
                    amount: 1_000_000,
                    task: None,
                    memo: String::new(),
                };
                tx.open_table(LEDGER)
                    .unwrap()
                    .insert(7, enc(&entry).as_slice())
                    .unwrap();
                let usage = Usage {
                    cluster: *cluster.public().as_bytes(),
                    node: *node.public().as_bytes(),
                    task,
                    executor: Executor::Wasm,
                    outcome: Outcome::Exited(0),
                    fuel_used: 12_500,
                    wall_ms: 5,
                    memory_limit_bytes: 1 << 20,
                    peak_memory_bytes: None,
                    cpu_millis: None,
                    result_digest: result_digest(&Outcome::Exited(0), b"", &[]),
                    finished_unix: 1,
                };
                let signed = SignedUsage::sign(usage, &node);
                let old = legacy::ReceiptV1 {
                    usage: signed.usage,
                    node_sig: signed.node_sig,
                    billing: legacy::BillingV1 {
                        consumer: "owner".into(),
                        provider: "bob".into(),
                        billed_wall_ms: 5,
                        amount: 12_500,
                        check: false,
                        recorded_unix: 2,
                    },
                    cluster_sig: cluster.sign(b"old"),
                };
                tx.open_table(RECEIPTS)
                    .unwrap()
                    .insert(run_key(&task), enc(&old).as_slice())
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        let store = Store::open(&path).unwrap();
        let snap = store.load().unwrap();
        assert_eq!(snap.members[0].strikes, 1);
        assert!(snap.members[0].perf.is_none());
        let p = snap.policy.unwrap();
        assert_eq!(p.rates, pumasi_proto::economy::CuRates::default());
        assert_eq!(p.hold_secs, 60);
        let bob = snap.accounts.iter().find(|a| a.name == "bob").unwrap();
        assert_eq!((bob.balance, bob.held, bob.earned), (500, 250, 500));
        assert_eq!(snap.holds[0].amount, 250);
        let ledger = store.ledger(None, 10).unwrap();
        assert_eq!(ledger.len(), 2);
        assert!(
            ledger
                .iter()
                .all(|e| e.kind == LedgerKind::Convert && e.seq >= 8)
        );
        assert_eq!(
            ledger.iter().find(|e| e.account == "owner").unwrap().amount,
            19_250
        );
        // The receipt is countersigned again, in CU, when the control
        // plane starts; the node's signature still holds.
        assert!(store.receipts(3).unwrap().is_empty());
        let st = State::load(
            "t".into(),
            cluster.public().to_string(),
            store,
            cluster.clone(),
        )
        .unwrap();
        let rs = st.store.receipts(3).unwrap();
        assert_eq!(rs.len(), 1);
        rs[0].verify().unwrap();
        assert_eq!(
            (rs[0].billing.amount, rs[0].billing.exact_ucu),
            (250, 250_000)
        );
        drop(st);
        // Once only.
        let store = Store::open(&path).unwrap();
        assert!(store.credit_receipts().unwrap().is_none());
        assert_eq!(store.ledger(None, 10).unwrap().len(), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn migrates_a_phase1_database() {
        let path =
            std::env::temp_dir().join(format!("pumasi-store-{}.redb", rand::random::<u64>()));
        {
            let db = Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            let info = NodeInfo {
                name: "old".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                cpus: 4,
                memory_bytes: 1 << 30,
                slots: 4,
                agent_version: "0.1.0".into(),
                executors: vec![Executor::Wasm],
            };
            tx.open_table(MEMBERS)
                .unwrap()
                .insert(
                    "abc",
                    enc(&V1Member {
                        id: "abc".into(),
                        info,
                        joined_unix: 1,
                    })
                    .as_slice(),
                )
                .unwrap();
            let job = V1Job {
                id: JobId(5),
                name: "j".into(),
                executor: Executor::Wasm,
                module_hash: None,
                docker: None,
                files_hash: None,
                limits: Limits::default(),
                tier: Tier::Open,
                max_attempts: 3,
                max_price_per_hour: None,
                submitted_unix: 2,
                cancelled: false,
                seq: 0,
            };
            tx.open_table(JOBS)
                .unwrap()
                .insert(5u128, enc(&job).as_slice())
                .unwrap();
            let task = V1Task {
                input: TaskInput::default(),
                state: TaskState::Succeeded,
                attempts: 1,
                node: Some("abc".into()),
                fuel_used: 9,
                wall_ms: 1,
                stdout: b"out".to_vec(),
                stderr: vec![],
                output_files: vec![],
            };
            tx.open_table(TASKS)
                .unwrap()
                .insert((5u128, 0u32), enc(&task).as_slice())
                .unwrap();
            tx.commit().unwrap();
        }
        let store = Store::open(&path).unwrap();
        let snap = store.load().unwrap();
        assert_eq!(snap.members.len(), 1);
        assert_eq!(snap.members[0].account, "owner");
        assert_eq!(snap.jobs.len(), 1);
        assert_eq!(snap.jobs[0].0.account, "owner");
        assert_eq!(snap.jobs[0].1[0].stdout, b"out");
        drop(store);
        // Opening again does not migrate twice.
        let snap = Store::open(&path).unwrap().load().unwrap();
        assert_eq!(snap.jobs[0].1[0].fuel_used, 9);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn phase_17_sso_settings_read_with_the_device_flow_off() {
        #[derive(Serialize)]
        struct V17 {
            oidc: Option<pumasi_proto::sso::OidcSettings>,
            ldap: Option<pumasi_proto::sso::LdapSettings>,
            maps: Vec<pumasi_proto::sso::RoleMap>,
            session_hours: u32,
        }
        let path = std::env::temp_dir().join(format!("pumasi-sso-{}.db", rand::random::<u64>()));
        let store = Store::open(&path).unwrap();
        store
            .put_meta(
                SSO_META,
                &enc(&V17 {
                    oidc: None,
                    ldap: None,
                    maps: vec![],
                    session_hours: 7,
                }),
            )
            .unwrap();
        let c = store.sso().unwrap().unwrap();
        assert_eq!((c.session_hours, c.device), (7, false));
        let on = crate::sso::SsoConfig { device: true, ..c };
        store.put_sso(&on).unwrap();
        assert_eq!(store.sso().unwrap().unwrap(), on);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }
}
