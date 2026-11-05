//! Helpers around iroh-blobs shared by the control plane, the agent and
//! the CLI: opening a store, turning files into a collection and back,
//! and moving collections over a connection (resuming from what the
//! receiving side already has).

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use iroh::endpoint::Connection;
pub use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{Store, TempTag},
    format::collection::Collection,
    protocol::{ChunkRangesSeq, PushRequest},
    store::{GcConfig, ProtectCb, ProtectOutcome, fs::FsStore},
};
use iroh_blobs::{
    api::blobs::{AddPathOptions, BlobStatus, ExportMode, ExportOptions, ImportMode},
    hashseq::HashSeq,
    store::fs::options::Options,
};

use crate::{job::validate_rel_path, store::BlobHash};

/// Most entries a collection may have.
pub const MAX_COLLECTION_ENTRIES: usize = 1_000_000;

pub fn to_hash(h: &BlobHash) -> Hash {
    Hash::from_bytes(*h)
}

pub fn from_hash(h: Hash) -> BlobHash {
    *h.as_bytes()
}

/// Opens (or creates) a file-backed store in `dir`, with garbage
/// collection every `gc` when given: blobs no tag (or the protect
/// callback) refers to are deleted.
pub async fn open_store(dir: &Path, gc: Option<(Duration, Option<ProtectCb>)>) -> Result<FsStore> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let mut options = Options::new(dir);
    options.gc = gc.map(|(interval, add_protected)| GcConfig {
        interval,
        add_protected,
    });
    FsStore::load_with_opts(dir.join("blobs.db"), options)
        .await
        .map_err(|e| anyhow::anyhow!("open file store in {}: {e:#}", dir.display()))
}

/// One file of a collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub hash: Hash,
    pub size: u64,
}

/// Adds files to the store as a collection. `files` are (name in the
/// collection, path, size). With `reference`, large files are referenced
/// where they are instead of copied (they must not change afterwards).
/// Returns the collection's root (protected while the tag lives), its
/// total size and file count.
pub async fn import_files(
    store: &Store,
    files: &[(String, PathBuf)],
    reference: bool,
) -> Result<(TempTag, u64, u32)> {
    ensure!(
        files.len() <= MAX_COLLECTION_ENTRIES,
        "too many files ({}; the limit is {MAX_COLLECTION_ENTRIES})",
        files.len()
    );
    let batch = store
        .batch()
        .await
        .map_err(|e| anyhow::anyhow!("open a batch: {e}"))?;
    let mut tags = Vec::with_capacity(files.len());
    let mut collection = Collection::default();
    let mut total = 0u64;
    let mut seen = HashSet::new();
    for (name, path) in files {
        validate_rel_path(name)?;
        ensure!(seen.insert(name.clone()), "duplicate file {name}");
        let tag = batch
            .add_path_with_opts(AddPathOptions {
                path: absolute(path)?,
                format: BlobFormat::Raw,
                mode: if reference {
                    ImportMode::TryReference
                } else {
                    ImportMode::Copy
                },
            })
            .temp_tag()
            .await
            .map_err(|e| anyhow::anyhow!("add {}: {e}", path.display()))?;
        total += blob_size(store, tag.hash()).await?;
        collection.push(name.clone(), tag.hash());
        tags.push(tag);
    }
    let root = collection
        .store(store)
        .await
        .map_err(|e| anyhow::anyhow!("store collection: {e}"))?;
    drop(tags);
    drop(batch);
    Ok((root, total, files.len() as u32))
}

/// Paths given to the store must be absolute.
fn absolute(p: &Path) -> Result<PathBuf> {
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(p))
    }
}

/// The size of a complete blob.
pub async fn blob_size(store: &Store, hash: Hash) -> Result<u64> {
    match store
        .blobs()
        .status(hash)
        .await
        .map_err(|e| anyhow::anyhow!("blob status: {e}"))?
    {
        BlobStatus::Complete { size } => Ok(size),
        BlobStatus::Partial { .. } => bail!("blob {} is incomplete", hash.fmt_short()),
        BlobStatus::NotFound => bail!("blob {} is missing", hash.fmt_short()),
    }
}

/// The files of a complete collection, with checked names.
pub async fn entries(store: &Store, root: Hash) -> Result<Vec<Entry>> {
    let c = Collection::load(root, store)
        .await
        .map_err(|e| anyhow::anyhow!("read collection {}: {e}", root.fmt_short()))?;
    ensure!(
        c.len() <= MAX_COLLECTION_ENTRIES,
        "collection has too many entries"
    );
    let mut out = Vec::with_capacity(c.len());
    let mut seen = HashSet::new();
    for (name, hash) in c.iter() {
        validate_rel_path(name).with_context(|| format!("bad entry in collection {name:?}"))?;
        ensure!(seen.insert(name.clone()), "duplicate entry {name}");
        out.push(Entry {
            name: name.clone(),
            hash: *hash,
            size: blob_size(store, *hash).await?,
        });
    }
    Ok(out)
}

/// Whether the store has all of a collection, and the ranges it lacks.
pub async fn missing(store: &Store, root: Hash) -> Result<(ChunkRangesSeq, bool)> {
    let local = store
        .remote()
        .local(HashAndFormat::hash_seq(root))
        .await
        .map_err(|e| anyhow::anyhow!("local state of {}: {e}", root.fmt_short()))?;
    Ok((local.missing().ranges, local.is_complete()))
}

/// Downloads what the store lacks of a collection over `conn` (a
/// connection on the blobs ALPN). Partial data stays, so a later call
/// continues where this one stopped. Returns the bytes read.
pub async fn fetch(store: &Store, conn: Connection, root: Hash) -> Result<u64> {
    let stats = store
        .remote()
        .fetch(conn, HashAndFormat::hash_seq(root))
        .await
        .map_err(|e| anyhow::anyhow!("download {}: {e}", root.fmt_short()))?;
    Ok(stats.payload_bytes_read)
}

/// Sends `ranges` of a collection to the other side of `conn`, which must
/// accept the push.
pub async fn push(
    store: &Store,
    conn: Connection,
    root: Hash,
    ranges: ChunkRangesSeq,
) -> Result<()> {
    store
        .remote()
        .execute_push(conn.clone(), PushRequest::new(root, ranges.clone()))
        .await
        .map_err(|e| anyhow::anyhow!("upload {}: {e}", root.fmt_short()))?;
    // The push is done once the data is written to the stream, not once
    // the other side has stored it, and closing the connection now would
    // cut it short. The other side writes the blobs in order, so wait
    // until it has all of the last one sent.
    let members = hash_seq_members(store, root).await;
    let mut last = None;
    for (offset, _) in ranges.iter_non_empty_infinite() {
        match members.get(offset as usize) {
            Some(h) => last = Some(*h),
            None => break,
        }
    }
    if let Some(hash) = last {
        wait_stored(store, conn, hash).await?;
    }
    Ok(())
}

/// Waits until the other side of `conn` has all of blob `hash`, giving up
/// when it stops making progress.
async fn wait_stored(store: &Store, conn: Connection, hash: Hash) -> Result<()> {
    use n0_future::StreamExt;
    const STALL: Duration = Duration::from_secs(60);
    let updates = store
        .remote()
        .observe(conn, iroh_blobs::protocol::ObserveRequest::new(hash));
    let mut updates = std::pin::pin!(updates);
    let mut have = iroh_blobs::api::proto::Bitfield::empty();
    loop {
        match tokio::time::timeout(STALL, updates.next()).await {
            Ok(Some(Ok(update))) => {
                have.update(&update);
                if have.is_complete() {
                    return Ok(());
                }
            }
            Ok(Some(Err(e))) => bail!("upload {}: {e}", hash.fmt_short()),
            Ok(None) => bail!("upload {}: the other side stopped", hash.fmt_short()),
            Err(_) => bail!("upload {}: no progress for {STALL:?}", hash.fmt_short()),
        }
    }
}

/// Every hash a collection consists of (root, names, files), for keeping
/// it from garbage collection. Only what the store has.
pub async fn hash_seq_members(store: &Store, root: Hash) -> Vec<Hash> {
    let mut out = vec![root];
    if let Ok(bytes) = store.blobs().get_bytes(root).await
        && let Ok(seq) = HashSeq::try_from(bytes)
    {
        out.extend(seq.iter());
    }
    out
}

/// Writes the entries of a collection that `place` maps to a relative
/// path under `dest`. With `move_out`, large files are moved out of the
/// store instead of copied, when the store owns them (use only for a store
/// that is about to be deleted, or whose files stay where they were put).
pub async fn export(
    store: &Store,
    entries: &[Entry],
    dest: &Path,
    move_out: bool,
    place: impl Fn(&str) -> Option<String>,
) -> Result<u64> {
    let dest = absolute(dest)?;
    let mut total = 0;
    for e in entries {
        let Some(rel) = place(&e.name) else {
            continue;
        };
        validate_rel_path(&rel)?;
        let target = dest.join(&rel);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let _ = std::fs::remove_file(&target);
        store
            .blobs()
            .export_with_opts(ExportOptions {
                hash: e.hash,
                mode: if move_out {
                    ExportMode::TryReference
                } else {
                    ExportMode::Copy
                },
                target: target.clone(),
            })
            .finish()
            .await
            .map_err(|err| anyhow::anyhow!("write {}: {err}", target.display()))?;
        total += e.size;
    }
    Ok(total)
}

/// Makes the store's tags under `prefixes` exactly `desired` (name, root
/// collection): sets the missing ones and deletes the rest. Returns how
/// many were deleted.
pub async fn reconcile_tags(
    store: &Store,
    prefixes: &[&str],
    desired: &[(String, Hash)],
) -> Result<usize> {
    use n0_future::StreamExt;
    let want: std::collections::HashMap<&str, Hash> =
        desired.iter().map(|(n, h)| (n.as_str(), *h)).collect();
    let mut stale = Vec::new();
    let mut have = HashSet::new();
    for prefix in prefixes {
        let mut tags = store
            .tags()
            .list_prefix(prefix.as_bytes())
            .await
            .map_err(|e| anyhow::anyhow!("list tags: {e}"))?;
        while let Some(t) = tags.next().await {
            let t = t.map_err(|e| anyhow::anyhow!("list tags: {e}"))?;
            let name = String::from_utf8_lossy(t.name.as_ref()).into_owned();
            match want.get(name.as_str()) {
                Some(h) if *h == t.hash && t.format == BlobFormat::HashSeq => {
                    have.insert(name);
                }
                _ => stale.push(name),
            }
        }
    }
    for name in &stale {
        store
            .tags()
            .delete(name.as_str())
            .await
            .map_err(|e| anyhow::anyhow!("delete tag {name}: {e}"))?;
    }
    for (name, hash) in desired {
        if !have.contains(name) {
            store
                .tags()
                .set(name.as_str(), HashAndFormat::hash_seq(*hash))
                .await
                .map_err(|e| anyhow::anyhow!("set tag {name}: {e}"))?;
        }
    }
    Ok(stale.len())
}
