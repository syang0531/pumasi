//! Durable control-plane state in a single redb file.
//!
//! Records are postcard-encoded. Tasks are stored one per key so a task
//! completing rewrites only that task, not the whole job.

use std::path::Path;

use anyhow::{Context, Result};
use pumasi_proto::{ModuleHash, Secret};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};

use crate::state::{JobMeta, Member, TaskRecord, TokenRecord};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const MEMBERS: TableDefinition<&str, &[u8]> = TableDefinition::new("members");
const TOKENS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("tokens");
const JOBS: TableDefinition<u128, &[u8]> = TableDefinition::new("jobs");
/// Key: (job id, task index).
const TASKS: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("tasks");
const MODULES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("modules");

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
        tx.open_table(META)?;
        tx.open_table(MEMBERS)?;
        tx.open_table(TOKENS)?;
        tx.open_table(JOBS)?;
        tx.open_table(TASKS)?;
        tx.open_table(MODULES)?;
        tx.commit()?;
        Ok(Self { db })
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

    /// Writes a new job with all its tasks and its module in one transaction.
    pub fn put_new_job(
        &self,
        meta: &JobMeta,
        tasks: &[TaskRecord],
        module: Option<&[u8]>,
    ) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            if let Some(bytes) = module {
                tx.open_table(MODULES)?
                    .insert(meta.module_hash.as_slice(), bytes)?;
            }
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
}
