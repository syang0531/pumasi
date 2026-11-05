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
    msg::{AccountView, LedgerEntry, LedgerKind, TaskRef},
    receipt::Receipt,
};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use tracing::info;

use crate::state::{
    DatasetRecord, Hold, JobExt, JobMeta, Member, TaskExt, TaskRecord, TokenRecord,
};

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

const SCHEMA_META: &str = "schema";
const POLICY_META: &str = "policy";
/// The old price of a core-hour in micro-credits, which became one CU.
const CREDIT_RATE_META: &str = "credit_cpu_hour";
/// Set once the credit receipts were countersigned again in CU.
const CREDIT_RECEIPTS_DONE_META: &str = "credit_receipts_converted";
/// Layout version written by this build.
const SCHEMA: u32 = 4;

fn run_key(t: &TaskRef) -> RunKey {
    (t.job.0, t.index, t.attempt, t.check)
}

pub struct Store {
    db: Database,
}

/// Everything loaded at startup.
#[derive(Default)]
pub struct Snapshot {
    pub job_ext: Vec<(u128, JobExt)>,
    pub task_ext: Vec<((u128, u32), TaskExt)>,
    pub datasets: Vec<DatasetRecord>,
    pub quotas: Vec<(String, u64)>,
    pub secrets: Vec<(String, u64, String)>,
    pub uploads: Vec<([u8; 32], crate::state::UploadRecord)>,
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
        if schema < 4 {
            store.migrate_v3()?;
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
        let tx = self.db.begin_read()?;
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
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(RECEIPTS)?;
            for r in receipts {
                t.insert(run_key(&r.usage.task), enc(r).as_slice())?;
            }
            tx.open_table(META)?
                .insert(CREDIT_RECEIPTS_DONE_META, [1u8].as_slice())?;
        }
        tx.commit()?;
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
    /// (module, shared file bundle) in one transaction. A job's file store
    /// details go to their own tables; a used upload is forgotten.
    pub fn put_new_job(
        &self,
        meta: &JobMeta,
        tasks: &[TaskRecord],
        blobs: &[(ModuleHash, &[u8])],
        used_uploads: &[[u8; 32]],
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
            if !meta.ext.is_empty() {
                tx.open_table(JOB_EXT)?
                    .insert(meta.id.0, enc(&meta.ext).as_slice())?;
            }
            let mut t = tx.open_table(TASKS)?;
            let mut ext = tx.open_table(TASK_EXT)?;
            for (i, task) in tasks.iter().enumerate() {
                t.insert((meta.id.0, i as u32), enc(task).as_slice())?;
                if !task.ext.is_empty() {
                    ext.insert((meta.id.0, i as u32), enc(&task.ext).as_slice())?;
                }
            }
            let mut up = tx.open_table(UPLOADS)?;
            for root in used_uploads {
                up.remove(root.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_job_meta(&self, meta: &JobMeta) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            tx.open_table(JOBS)?
                .insert(meta.id.0, enc(meta).as_slice())?;
            let mut ext = tx.open_table(JOB_EXT)?;
            if meta.ext.is_empty() {
                ext.remove(meta.id.0)?;
            } else {
                ext.insert(meta.id.0, enc(&meta.ext).as_slice())?;
            }
        }
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
            let mut ext = tx.open_table(TASK_EXT)?;
            for (i, task) in tasks {
                t.insert((job, *i), enc(*task).as_slice())?;
                if task.ext.is_empty() {
                    ext.remove((job, *i))?;
                } else {
                    ext.insert((job, *i), enc(&task.ext).as_slice())?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_dataset(&self, d: &DatasetRecord) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(DATASETS)?
            .insert((d.name.as_str(), d.version), enc(d).as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_datasets(&self, keys: &[(String, u32)]) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(DATASETS)?;
            for (n, v) in keys {
                t.remove((n.as_str(), *v))?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn set_quota(&self, account: &str, bytes: Option<u64>) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(QUOTAS)?;
            match bytes {
                Some(b) => {
                    t.insert(account, b)?;
                }
                None => {
                    t.remove(account)?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_secret(&self, name: &str, set_unix: u64, value: Option<&str>) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(SECRETS)?;
            match value {
                Some(v) => {
                    t.insert(name, enc(&(set_unix, v.to_string())).as_slice())?;
                }
                None => {
                    t.remove(name)?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_upload(
        &self,
        root: &[u8; 32],
        u: Option<&crate::state::UploadRecord>,
    ) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(UPLOADS)?;
            match u {
                Some(u) => {
                    t.insert(root.as_slice(), enc(u).as_slice())?;
                }
                None => {
                    t.remove(root.as_slice())?;
                }
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
            let mut t = tx.open_table(CARRY)?;
            for (c, p, v) in w.carries {
                t.insert((c.as_str(), p.as_str()), *v)?;
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
        pub fn convert(self) -> (Policy, i64) {
            let p = &self.prices;
            let cpu_hour = [p.docker_cpu_hour, p.microvm_cpu_hour]
                .into_iter()
                .find(|&v| v > 0)
                .unwrap_or(OLD_CPU_HOUR);
            let rates = if self.prices == OLD_DEFAULT {
                CuRates::default()
            } else {
                CuRates {
                    memory_gib_hour: (p.memory_gib_hour.max(0) as i128 * MICRO_PER_CU as i128
                        / cpu_hour as i128) as i64,
                    ..CuRates::default()
                }
            };
            (
                Policy {
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
                ext: Default::default(),
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
}
