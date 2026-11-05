//! `pumasi backup` and `pumasi cluster restore` (phase 13): a copy of the
//! control plane's state database and the cluster key in one file.
//!
//! The copy is taken in one read transaction, so it is consistent: every
//! ledger entry with its balances, every job with its tasks. With
//! replicas it is what the leader has applied, which is everything
//! committed. The file store's data (`<cluster dir>/blobs`) and uploaded
//! release binaries are not in it.
//!
//! A restore makes a single control plane from the file in an empty
//! directory; replicas are added again with `pumasi ha invite`. The file
//! holds the cluster key, the admin secret and the cluster's secrets
//! (registry logins): keep it like the admin ticket.

use std::path::Path;

use anyhow::{Context, Result, bail};
use redb::{Database, ReadableDatabase};
use serde::{Deserialize, Serialize};

use crate::store::{self, Dump, Store};

const MAGIC: &[u8; 16] = b"PUMASI-BACKUP\0\0\x01";

#[derive(Serialize, Deserialize)]
struct BackupFile {
    created_unix: u64,
    /// The pumasi version that made it.
    version: String,
    cluster_key: [u8; 32],
    dump: Dump,
}

/// What a restore wrote.
#[derive(Debug, Clone)]
pub struct Restored {
    pub cluster_name: String,
    pub cluster_id: String,
    pub created_unix: u64,
    pub rows: usize,
}

/// A backup of `db` with the cluster key.
pub fn make(db: &Database, cluster_key: [u8; 32]) -> Result<Vec<u8>> {
    let tx = db.begin_read()?;
    let dump = store::dump(&tx)?;
    drop(tx);
    let file = BackupFile {
        created_unix: crate::state::now_unix(),
        version: pumasi_proto::version().to_string(),
        cluster_key,
        dump,
    };
    let mut out = MAGIC.to_vec();
    out.extend(postcard::to_stdvec(&file).context("encode backup")?);
    Ok(out)
}

fn parse(bytes: &[u8]) -> Result<BackupFile> {
    let Some(body) = bytes.strip_prefix(MAGIC.as_slice()) else {
        bail!("not a pumasi backup");
    };
    postcard::from_bytes(body).context("the backup is corrupt")
}

/// Writes a single control plane into `data_dir` (which must not hold one)
/// from a backup.
pub fn restore(bytes: &[u8], data_dir: &Path) -> Result<Restored> {
    let file = parse(bytes)?;
    let db_path = data_dir.join("state.redb");
    let key_path = data_dir.join("secret.key");
    if db_path.exists() || key_path.exists() {
        bail!(
            "{} already holds a control plane; restore into an empty directory",
            data_dir.display()
        );
    }
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("create {}", data_dir.display()))?;
    let store = Store::open(&db_path)?;
    {
        let tx = store.db().begin_write()?;
        store::load_dump(&tx, &file.dump)?;
        tx.commit()?;
    }
    let name = store
        .get_meta("cluster_name")?
        .and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_default();
    drop(store);
    // Opening again migrates a backup of an older layout.
    drop(Store::open(&db_path)?);
    let key = iroh::SecretKey::from_bytes(&file.cluster_key);
    pumasi_proto::net::save_key(&key_path, &key)?;
    Ok(Restored {
        cluster_name: name,
        cluster_id: key.public().to_string(),
        created_unix: file.created_unix,
        rows: file.dump.rows.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_and_restore_keep_every_row() {
        let dir = std::env::temp_dir().join(format!("pumasi-backup-{}", uuid::Uuid::new_v4()));
        let store = Store::open(&dir.join("a/state.redb")).unwrap();
        store.put_meta("cluster_name", b"lab").unwrap();
        store.put_meta("admin_secret", &[7u8; 32]).unwrap();
        let bytes = make(store.db(), [3u8; 32]).unwrap();
        let rows = {
            let tx = store.db().begin_read().unwrap();
            store::dump(&tx).unwrap().rows
        };
        drop(store);
        let r = restore(&bytes, &dir.join("b")).unwrap();
        assert_eq!(r.cluster_name, "lab");
        assert_eq!(r.rows, rows.len());
        let again = Store::open(&dir.join("b/state.redb")).unwrap();
        let got = {
            let tx = again.db().begin_read().unwrap();
            store::dump(&tx).unwrap().rows
        };
        assert_eq!(got, rows);
        assert_eq!(
            pumasi_proto::net::load_or_create_key(&dir.join("b/secret.key"))
                .unwrap()
                .to_bytes(),
            [3u8; 32]
        );
        // Not over an existing control plane, and not from garbage.
        assert!(restore(&bytes, &dir.join("b")).is_err());
        assert!(restore(b"hello", &dir.join("c")).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
