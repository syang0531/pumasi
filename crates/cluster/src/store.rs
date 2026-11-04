//! Durable control-plane state in a single redb file.
//!
//! Records are postcard-encoded. Tasks are stored one per key so a task
//! completing rewrites only that task, not the whole job. Postcard is not
//! self-describing, so the record layout is versioned (`schema` in `meta`)
//! and older layouts are migrated when the database is opened.

use std::path::Path;

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    ModuleHash, Secret,
    economy::Policy,
    msg::{AccountView, LedgerEntry, TaskRef},
    receipt::Receipt,
};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use tracing::info;

use crate::state::{Hold, JobMeta, Member, TaskRecord, TokenRecord};

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

const SCHEMA_META: &str = "schema";
const POLICY_META: &str = "policy";
/// Layout version written by this build.
const SCHEMA: u32 = 3;

fn run_key(t: &TaskRef) -> RunKey {
    (t.job.0, t.index, t.attempt, t.check)
}

pub struct Store {
    db: Database,
}

/// Everything loaded at startup.
#[derive(Default)]
pub struct Snapshot {
    pub members: Vec<Member>,
    pub tokens: Vec<TokenRecord>,
    pub jobs: Vec<(JobMeta, Vec<TaskRecord>)>,
    pub modules: Vec<(ModuleHash, Vec<u8>)>,
    pub accounts: Vec<AccountView>,
    pub holds: Vec<Hold>,
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
        let store = Self { db };
        if schema < 2 {
            store.migrate_v1()?;
        }
        if schema < 3 {
            store.migrate_v2()?;
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
                let m: Member = dec::<legacy::Member>(&v)?.into();
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
                let p: Policy = dec::<legacy::PolicyV2>(&old)?.into();
                meta.insert(POLICY_META, enc(&p).as_slice())?;
            }
            meta.insert(SCHEMA_META, SCHEMA.to_le_bytes().as_slice())?;
        }
        tx.commit()?;
        info!("migrated state database to layout {SCHEMA}");
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(META)?;
        Ok(t.get(key)?.map(|v| v.value().to_vec()))
    }

    pub fn put_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(META)?.insert(key, value)?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_meta(&self, key: &str) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(META)?.remove(key)?;
        tx.commit()?;
        Ok(())
    }

    pub fn put_policy(&self, p: &Policy) -> Result<()> {
        self.put_meta(POLICY_META, &enc(p))
    }

    pub fn load(&self) -> Result<Snapshot> {
        let tx = self.db.begin_read()?;
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
        let tx = self.db.begin_write()?;
        tx.open_table(MEMBERS)?
            .insert(m.id.as_str(), enc(m).as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_member(&self, id: &str) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(MEMBERS)?.remove(id)?;
        tx.commit()?;
        Ok(())
    }

    pub fn put_token(&self, t: &TokenRecord) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(TOKENS)?
            .insert(t.secret.as_slice(), enc(t).as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_token(&self, secret: &Secret) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(TOKENS)?.remove(secret.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    /// Writes a new job with all its tasks and any new content blobs
    /// (module, shared file bundle) in one transaction.
    pub fn put_new_job(
        &self,
        meta: &JobMeta,
        tasks: &[TaskRecord],
        blobs: &[(ModuleHash, &[u8])],
    ) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(MODULES)?;
            for (hash, bytes) in blobs {
                t.insert(hash.as_slice(), *bytes)?;
            }
            drop(t);
            tx.open_table(JOBS)?
                .insert(meta.id.0, enc(meta).as_slice())?;
            let mut t = tx.open_table(TASKS)?;
            for (i, task) in tasks.iter().enumerate() {
                t.insert((meta.id.0, i as u32), enc(task).as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_job_meta(&self, meta: &JobMeta) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(JOBS)?
            .insert(meta.id.0, enc(meta).as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn put_tasks(&self, job: u128, tasks: &[(u32, &TaskRecord)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(TASKS)?;
            for (i, task) in tasks {
                t.insert((job, *i), enc(*task).as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Applies balance changes, ledger entries, holds and a receipt
    /// atomically, so money is never half-moved.
    pub fn write_ledger(&self, w: &LedgerWrite<'_>) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(ACCOUNTS)?;
            for a in &w.accounts {
                t.insert(a.name.as_str(), enc(*a).as_slice())?;
            }
            let mut t = tx.open_table(LEDGER)?;
            for e in w.entries {
                t.insert(e.seq, enc(e).as_slice())?;
            }
            let mut t = tx.open_table(HOLDS)?;
            for h in w.put_holds {
                t.insert(run_key(&h.task), enc(h).as_slice())?;
            }
            for k in w.del_holds {
                t.remove(run_key(k))?;
            }
            if let Some(r) = w.receipt {
                tx.open_table(RECEIPTS)?
                    .insert(run_key(&r.usage.task), enc(r).as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Ledger entries, newest first, optionally for one account.
    pub fn ledger(&self, account: Option<&str>, limit: usize) -> Result<Vec<LedgerEntry>> {
        let tx = self.db.begin_read()?;
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

    /// Every receipt of one job, in task order.
    pub fn receipts(&self, job: u128) -> Result<Vec<Receipt>> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(RECEIPTS)?;
        let mut out = Vec::new();
        for row in t.range((job, 0, 0, false)..=(job, u32::MAX, u32::MAX, true))? {
            let (_, v) = row?;
            out.push(dec(v.value())?);
        }
        Ok(out)
    }
}

/// Older record layouts, kept only to migrate old databases.
mod legacy {
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

    impl From<PolicyV2> for pumasi_proto::economy::Policy {
        fn from(p: PolicyV2) -> Self {
            Self {
                prices: pumasi_proto::economy::PriceList {
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

    impl From<Member> for crate::state::Member {
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
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(p.prices.docker_cpu_hour, 7);
        assert_eq!(p.prices.microvm_cpu_hour, 7, "starts at the docker price");
        assert_eq!(p.prices.memory_gib_hour, 3);
        assert_eq!(
            (p.hold_secs, p.verify_permille, p.max_strikes),
            (60, 250, 2)
        );
        // Reopening does not migrate again.
        let again = Store::open(&path).unwrap().load().unwrap().policy.unwrap();
        assert_eq!(again, p);
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
}
