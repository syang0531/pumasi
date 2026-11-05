//! The node's side of the cluster file store: a cache of collections
//! (mounted datasets, private images) in an iroh-blobs store under
//! `<data>/store`, filled from other nodes or the control plane, and
//! served to other nodes of the cluster.
//!
//! - Downloads resume: partial data stays in the store, so a fetch that
//!   breaks off (a peer leaving, a lost connection) continues from where it
//!   stopped, from the next peer or the control plane. Everything is
//!   verified against its BLAKE3 hash while it streams.
//! - The cache keeps collections by `cache/<hash>` tags, least recently
//!   used first out once it is over its size; the store's garbage
//!   collector then deletes what no tag keeps.
//! - Output files too large for a report are imported as a collection and
//!   pushed to the control plane.
//! - Other nodes may fetch what is cached here (a collection's hash is the
//!   capability: only members are told hashes); nobody may push.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::BlobsProtocol;
use pumasi_proto::{
    BLOBS_ALPN,
    blobs::{self, ChunkRangesSeq, Entry, FsStore, Hash},
    msg::FromAgent,
    store::{BlobHash, Mount, StoredOutput, hash_hex},
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::tempdir::TempDir;

/// Default size of the cache.
pub const DEFAULT_CACHE_BYTES: u64 = 20 << 30;
const GC_INTERVAL: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Attempts at the control plane after the peers.
const CP_ATTEMPTS: u32 = 4;
const CACHE_TAG: &str = "cache/";

#[derive(Debug, Clone)]
struct Cached {
    size: u64,
    used: Instant,
    /// Tasks using it now.
    in_use: u32,
}

#[derive(Clone)]
pub struct NodeStore {
    store: FsStore,
    dir: PathBuf,
    cap: u64,
    endpoint: Endpoint,
    cache: Arc<Mutex<HashMap<BlobHash, Cached>>>,
    /// One download at a time per collection.
    fetching: Arc<Mutex<HashMap<BlobHash, Arc<tokio::sync::Mutex<()>>>>>,
    /// Where cache changes are reported (the current session).
    listener: Arc<Mutex<Option<mpsc::UnboundedSender<FromAgent>>>>,
}

impl NodeStore {
    pub async fn open(dir: &Path, cap: u64, endpoint: Endpoint) -> Result<Self> {
        // A previous agent in this process may still be letting go of it.
        let mut tries = 0;
        let store = loop {
            match blobs::open_store(dir, Some((GC_INTERVAL, None))).await {
                Ok(s) => break s,
                Err(e) if tries < 25 => {
                    tries += 1;
                    debug!("open the file store cache (retrying): {e:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => return Err(e),
            }
        };
        let _ = std::fs::remove_dir_all(dir.join("tmp"));
        let this = Self {
            store,
            dir: dir.to_path_buf(),
            cap,
            endpoint,
            cache: Default::default(),
            fetching: Default::default(),
            listener: Default::default(),
        };
        this.load().await;
        Ok(this)
    }

    /// Rebuilds the cache list from the store's tags.
    async fn load(&self) {
        use n0_future::StreamExt;
        let Ok(mut tags) = self.store.tags().list_prefix(CACHE_TAG).await else {
            return;
        };
        let mut found = Vec::new();
        while let Some(Ok(t)) = tags.next().await {
            found.push(t.hash);
        }
        for hash in found {
            if let Ok(entries) = blobs::entries(&self.store, hash).await {
                let size = entries.iter().map(|e| e.size).sum();
                self.cache.lock().unwrap().insert(
                    blobs::from_hash(hash),
                    Cached {
                        size,
                        used: Instant::now(),
                        in_use: 0,
                    },
                );
            }
        }
        let n = self.cache.lock().unwrap().len();
        if n > 0 {
            info!(collections = n, "file store cache loaded");
        }
    }

    /// The handler other nodes fetch from (reads only).
    pub fn protocol(&self) -> BlobsProtocol {
        BlobsProtocol::new(&self.store, None)
    }

    pub fn api(&self) -> &blobs::Store {
        &self.store
    }

    /// Reports the cache to a new session, then its changes.
    pub fn attach(&self, tx: mpsc::UnboundedSender<FromAgent>, addr: Option<EndpointAddr>) {
        let roots: Vec<BlobHash> = self.cache.lock().unwrap().keys().copied().collect();
        let _ = tx.send(FromAgent::StoreState {
            addr,
            full: true,
            added: roots,
            removed: vec![],
        });
        *self.listener.lock().unwrap() = Some(tx);
    }

    fn report(&self, added: Vec<BlobHash>, removed: Vec<BlobHash>) {
        if let Some(tx) = self.listener.lock().unwrap().as_ref() {
            let _ = tx.send(FromAgent::StoreState {
                addr: None,
                full: false,
                added,
                removed,
            });
        }
    }

    /// A scratch directory on the store's disk, removed on drop.
    pub fn scratch(&self, tag: &str) -> Result<TempDir> {
        TempDir::new_in(&self.dir.join("tmp"), tag)
    }

    /// Makes sure the store has all of `root`, fetching what it lacks from
    /// `peers` and then the control plane, and marks it in use until the
    /// returned guard drops.
    pub async fn ensure(
        &self,
        root: BlobHash,
        label: &str,
        peers: &[EndpointAddr],
        cluster: &EndpointAddr,
        cancel: &AtomicBool,
    ) -> Result<InUse> {
        let lock = self
            .fetching
            .lock()
            .unwrap()
            .entry(root)
            .or_default()
            .clone();
        let _one = lock.lock().await;
        let hash = blobs::to_hash(&root);
        let (_, mut complete) = blobs::missing(&self.store, hash).await?;
        if !complete {
            let start = Instant::now();
            let mut read = 0u64;
            let mut last_err: Option<anyhow::Error> = None;
            let sources = peers
                .iter()
                .filter(|p| p.id != self.endpoint.id())
                .map(|p| (p, false))
                .chain(std::iter::repeat_n((cluster, true), CP_ATTEMPTS as usize));
            let mut backoff = Duration::from_secs(1);
            for (addr, is_cp) in sources {
                if cancel.load(Ordering::Relaxed) {
                    bail!("cancelled while downloading {label}");
                }
                match self.fetch_from(addr, hash, cancel).await {
                    Ok(n) => read += n,
                    Err(e) => {
                        debug!(%label, from = %addr.id.fmt_short(), "download stopped: {e:#}");
                        last_err = Some(e);
                    }
                }
                (_, complete) = blobs::missing(&self.store, hash).await?;
                if complete {
                    break;
                }
                if is_cp {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
            if !complete {
                let e = last_err.unwrap_or_else(|| anyhow::anyhow!("no source had it"));
                return Err(e.context(format!("download {label}")));
            }
            info!(%label, bytes = read, secs = start.elapsed().as_secs(), "downloaded from the file store");
        }
        let entries = blobs::entries(&self.store, hash).await?;
        let size = entries.iter().map(|e| e.size).sum();
        {
            let mut c = self.cache.lock().unwrap();
            let e = c.entry(root).or_insert(Cached {
                size,
                used: Instant::now(),
                in_use: 0,
            });
            e.used = Instant::now();
            e.in_use += 1;
        }
        let tag = format!("{CACHE_TAG}{}", hash_hex(&root));
        self.store
            .tags()
            .set(tag.as_str(), blobs::HashAndFormat::hash_seq(hash))
            .await
            .map_err(|e| anyhow::anyhow!("tag {tag}: {e}"))?;
        self.report(vec![root], vec![]);
        self.evict().await;
        Ok(InUse {
            root,
            entries,
            cache: self.cache.clone(),
        })
    }

    async fn fetch_from(
        &self,
        addr: &EndpointAddr,
        hash: Hash,
        cancel: &AtomicBool,
    ) -> Result<u64> {
        let conn = tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.endpoint.connect(addr.clone(), BLOBS_ALPN),
        )
        .await
        .context("connect timed out")?
        .context("connect")?;
        let fetch = blobs::fetch(&self.store, conn.clone(), hash);
        tokio::pin!(fetch);
        loop {
            tokio::select! {
                r = &mut fetch => return r,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    if cancel.load(Ordering::Relaxed) {
                        conn.close(0u32.into(), b"cancelled");
                        bail!("cancelled");
                    }
                }
            }
        }
    }

    /// Drops least recently used collections nobody uses while the cache
    /// is over its size.
    async fn evict(&self) {
        let victims: Vec<BlobHash> = {
            let mut c = self.cache.lock().unwrap();
            let mut total: u64 = c.values().map(|e| e.size).sum();
            let mut order: Vec<(BlobHash, Instant, u64)> = c
                .iter()
                .filter(|(_, e)| e.in_use == 0)
                .map(|(r, e)| (*r, e.used, e.size))
                .collect();
            order.sort_by_key(|(_, used, _)| *used);
            let mut out = Vec::new();
            for (root, _, size) in order {
                if total <= self.cap {
                    break;
                }
                total -= size;
                c.remove(&root);
                out.push(root);
            }
            out
        };
        if victims.is_empty() {
            return;
        }
        for root in &victims {
            let tag = format!("{CACHE_TAG}{}", hash_hex(root));
            if let Err(e) = self.store.tags().delete(tag.as_str()).await {
                warn!("delete tag {tag}: {e}");
            }
        }
        info!(
            collections = victims.len(),
            "file store cache over its size; dropped the least recently used"
        );
        self.report(vec![], victims);
    }

    /// Writes a task's mounts under `dir` (its `/input`).
    pub async fn stage(&self, mounts: &[(Mount, InUse)], dir: &Path) -> Result<u64> {
        let mut total = 0;
        for (m, used) in mounts {
            let picked = used.entries.iter().filter(|e| m.selects(&e.name)).count();
            if let Some(sel) = &m.select
                && picked == 0
            {
                bail!("{} has no file or directory {sel}", m.label);
            }
            total += blobs::export(&self.store, &used.entries, dir, false, |name| {
                m.selects(name).then(|| m.place(name))
            })
            .await
            .with_context(|| format!("place {} in /input", m.label))?;
        }
        Ok(total)
    }

    /// Writes the files of a cached collection under `dir`.
    pub async fn export_all(&self, used: &InUse, dir: &Path) -> Result<u64> {
        blobs::export(&self.store, &used.entries, dir, false, |n| {
            Some(n.to_string())
        })
        .await
    }

    /// Imports the output files under `dir` as a collection and pushes it
    /// to the control plane.
    pub async fn push_output(&self, dir: &Path, cluster: &EndpointAddr) -> Result<StoredOutput> {
        let files: Vec<(String, PathBuf)> = pumasi_proto::job::dir_files(dir)?
            .into_iter()
            .map(|(rel, path, _)| (rel, path))
            .collect();
        let (tt, size, n) = blobs::import_files(&self.store, &files, false).await?;
        let root = tt.hash();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let res: Result<()> = async {
                let conn = tokio::time::timeout(
                    CONNECT_TIMEOUT,
                    self.endpoint.connect(cluster.clone(), BLOBS_ALPN),
                )
                .await
                .context("connect timed out")?
                .context("connect")?;
                blobs::push(&self.store, conn, root, ChunkRangesSeq::all()).await
            }
            .await;
            match res {
                Ok(()) => break,
                Err(e) if attempt < 3 => {
                    warn!("uploading output files failed (attempt {attempt}): {e:#}");
                    tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
                }
                Err(e) => return Err(e.context("upload output files")),
            }
        }
        drop(tt);
        Ok(StoredOutput {
            root: blobs::from_hash(root),
            size,
            files: n,
        })
    }

    pub async fn shutdown(&self) {
        let _ = self.store.shutdown().await;
    }
}

/// A cached collection a task is using; evictable again once dropped.
pub struct InUse {
    pub root: BlobHash,
    pub entries: Vec<Entry>,
    cache: Arc<Mutex<HashMap<BlobHash, Cached>>>,
}

impl Drop for InUse {
    fn drop(&mut self) {
        if let Some(e) = self.cache.lock().unwrap().get_mut(&self.root) {
            e.in_use = e.in_use.saturating_sub(1);
            e.used = Instant::now();
        }
    }
}
