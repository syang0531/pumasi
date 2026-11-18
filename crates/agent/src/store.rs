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
    store::{BlobHash, CachePolicy, Mount, StoredOutput, hash_hex},
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
/// Tags of the dataset versions this node keeps as replicas (1.8). The
/// cache's eviction never touches them.
const REPLICA_TAG: &str = "replica/";
/// A damaged replica waits this long for the garbage collector to delete
/// its blobs before it is fetched again.
const QUARANTINE: Duration = Duration::from_secs(3 * GC_INTERVAL.as_secs());
/// Failed replica downloads are tried again after this long.
const RETRY_EVERY: Duration = Duration::from_secs(30);

/// The replicas this node should keep, from the control plane.
#[derive(Default)]
struct Replicas {
    want: std::collections::BTreeSet<BlobHash>,
    sources: Vec<EndpointAddr>,
    cluster: Option<EndpointAddr>,
    failed: HashMap<BlobHash, String>,
    /// Damaged ones, until when they are left alone.
    quarantine: HashMap<BlobHash, Instant>,
}

#[derive(Debug, Clone)]
struct Cached {
    size: u64,
    used: Instant,
    /// Tasks using it now.
    in_use: u32,
    /// The account whose task fetched it last (1.18); empty when not known
    /// (it was cached before the agent restarted, or by an older control
    /// plane's task).
    account: String,
}

/// What the control plane says about the cache (1.18), and the agent's own
/// size for it.
#[derive(Debug, Clone, Default)]
struct Policy {
    /// `--store-cache`.
    own_cap: u64,
    cap: Option<u64>,
    per_account: Option<u64>,
    keep: std::collections::HashSet<BlobHash>,
}

impl Policy {
    fn cap(&self) -> u64 {
        self.cap.unwrap_or(self.own_cap)
    }
}

/// Which unused collections go, in order, so the cache fits: first what
/// takes an account over its share, then the least recently used, and
/// what queued tasks will mount only when nothing else is left.
fn eviction_order(cache: &HashMap<BlobHash, Cached>, policy: &Policy) -> Vec<BlobHash> {
    let mut out = Vec::new();
    let mut gone = std::collections::HashSet::new();
    let mut idle: Vec<(&BlobHash, &Cached)> = cache.iter().filter(|(_, e)| e.in_use == 0).collect();
    // Oldest first, and among the kept ones too.
    idle.sort_by_key(|(r, e)| (policy.keep.contains(*r), e.used));
    if let Some(share) = policy.per_account {
        let mut per: HashMap<&str, u64> = HashMap::new();
        for e in cache.values().filter(|e| !e.account.is_empty()) {
            *per.entry(e.account.as_str()).or_default() += e.size;
        }
        for (root, e) in &idle {
            let held = per.get_mut(e.account.as_str());
            if let Some(held) = held
                && *held > share
            {
                *held -= e.size;
                gone.insert(**root);
                out.push(**root);
            }
        }
    }
    let mut total: u64 = cache
        .iter()
        .filter(|(r, _)| !gone.contains(*r))
        .map(|(_, e)| e.size)
        .sum();
    let cap = policy.cap();
    for (root, e) in idle {
        if total <= cap {
            break;
        }
        if gone.insert(*root) {
            total -= e.size;
            out.push(*root);
        }
    }
    out
}

#[derive(Clone)]
pub struct NodeStore {
    store: FsStore,
    dir: PathBuf,
    policy: Arc<Mutex<Policy>>,
    endpoint: Endpoint,
    cache: Arc<Mutex<HashMap<BlobHash, Cached>>>,
    /// One download at a time per collection.
    fetching: Arc<Mutex<HashMap<BlobHash, Arc<tokio::sync::Mutex<()>>>>>,
    /// Where cache changes are reported (the current session).
    listener: Arc<Mutex<Option<mpsc::UnboundedSender<FromAgent>>>>,
    replicas: Arc<Mutex<Replicas>>,
    replica_wake: Arc<tokio::sync::Notify>,
    /// One replica list at a time, in the order they came.
    hold_lock: Arc<tokio::sync::Mutex<()>>,
    /// Shared by every handle: the replica loop stops once it holds the
    /// last one (the agent let go of the store, which can then close).
    alive: Arc<()>,
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
            policy: Arc::new(Mutex::new(Policy {
                own_cap: cap,
                ..Default::default()
            })),
            endpoint,
            cache: Default::default(),
            fetching: Default::default(),
            listener: Default::default(),
            replicas: Default::default(),
            replica_wake: Default::default(),
            hold_lock: Default::default(),
            alive: Default::default(),
        };
        this.load().await;
        tokio::spawn(this.clone().replica_loop());
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
                        account: String::new(),
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
    /// Where [`Self::scratch`] makes its directories.
    pub fn scratch_root(&self) -> std::path::PathBuf {
        self.dir.join("tmp")
    }

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
        // Keep what is here, and what the download writes, from the garbage
        // collector until the cache tag below does: it deletes every blob no
        // tag refers to when it starts, partial ones being written included,
        // and a task then failed reading a blob whose file was gone (1.13).
        let _keep = self
            .store
            .tags()
            .temp_tag(blobs::HashAndFormat::hash_seq(hash))
            .await
            .map_err(|e| anyhow::anyhow!("protect {label}: {e}"))?;
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
                account: String::new(),
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

    /// Counts a cached collection against the account whose task fetched
    /// it (1.18), and makes room if that takes the account over its
    /// share.
    pub fn charge(&self, root: BlobHash, account: &str) {
        if account.is_empty() {
            return;
        }
        let changed = {
            let mut c = self.cache.lock().unwrap();
            match c.get_mut(&root) {
                Some(e) if e.account != account => {
                    e.account = account.to_string();
                    true
                }
                _ => false,
            }
        };
        if changed && self.policy.lock().unwrap().per_account.is_some() {
            let this = self.clone();
            tokio::spawn(async move { this.evict().await });
        }
    }

    /// Takes the control plane's cache settings (1.18) and evicts what no
    /// longer fits.
    pub async fn set_policy(&self, p: CachePolicy) {
        let changed = {
            let mut policy = self.policy.lock().unwrap();
            let keep: std::collections::HashSet<BlobHash> = p.keep.into_iter().collect();
            let changed = policy.cap != p.cap || policy.per_account != p.per_account;
            policy.cap = p.cap;
            policy.per_account = p.per_account;
            policy.keep = keep;
            changed
        };
        if changed {
            let p = self.policy.lock().unwrap().clone();
            info!(
                cap = p.cap(),
                per_account = ?p.per_account,
                "file store cache settings from the cluster"
            );
        }
        self.evict().await;
    }

    /// Drops collections nobody uses while the cache is over its size or
    /// an account over its share ([`eviction_order`]).
    async fn evict(&self) {
        let victims: Vec<BlobHash> = {
            let policy = self.policy.lock().unwrap().clone();
            let mut c = self.cache.lock().unwrap();
            let out = eviction_order(&c, &policy);
            for root in &out {
                c.remove(root);
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
            "file store cache over its size or an account's share; dropped the least recently used"
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

    /// Writes one file of a cached collection under `dir`.
    pub async fn export_one(&self, used: &InUse, name: &str, dir: &Path) -> Result<u64> {
        anyhow::ensure!(
            used.entries.iter().any(|e| e.name == name),
            "{name} is not in the collection"
        );
        blobs::export(&self.store, &used.entries, dir, false, |n| {
            (n == name).then(|| n.to_string())
        })
        .await
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

    // -- replicas (1.8) -------------------------------------------------------

    /// The control plane's list of replicas to keep ([`pumasi_proto::msg::ToAgent::Hold`]):
    /// drops the ones no longer listed, tags the new ones and fetches them
    /// in the background, and says right away which it has.
    pub async fn hold(
        &self,
        _one: tokio::sync::OwnedMutexGuard<()>,
        roots: Vec<BlobHash>,
        sources: Vec<EndpointAddr>,
        cluster: EndpointAddr,
    ) {
        use n0_future::StreamExt;
        let want: std::collections::BTreeSet<BlobHash> = roots.into_iter().collect();
        {
            let mut r = self.replicas.lock().unwrap();
            r.want = want.clone();
            r.sources = sources;
            r.cluster = Some(cluster);
            r.failed.retain(|k, _| want.contains(k));
        }
        // Drop the tags of replicas no longer wanted.
        let mut stale = Vec::new();
        if let Ok(mut tags) = self.store.tags().list_prefix(REPLICA_TAG).await {
            while let Some(Ok(t)) = tags.next().await {
                let root = blobs::from_hash(t.hash);
                if !want.contains(&root) {
                    stale.push(String::from_utf8_lossy(t.name.as_ref()).into_owned());
                }
            }
        }
        for name in &stale {
            if let Err(e) = self.store.tags().delete(name.as_str()).await {
                warn!("delete tag {name}: {e}");
            }
        }
        if !stale.is_empty() {
            info!(
                replicas = stale.len(),
                "dropped replicas the cluster no longer keeps here"
            );
        }
        let quarantined: Vec<BlobHash> = {
            let r = self.replicas.lock().unwrap();
            r.quarantine.keys().copied().collect()
        };
        for root in &want {
            if quarantined.contains(root) {
                continue;
            }
            let tag = format!("{REPLICA_TAG}{}", hash_hex(root));
            if let Err(e) = self
                .store
                .tags()
                .set(
                    tag.as_str(),
                    blobs::HashAndFormat::hash_seq(blobs::to_hash(root)),
                )
                .await
            {
                warn!("tag {tag}: {e}");
            }
        }
        self.report_held(None).await;
        self.replica_wake.notify_one();
    }

    /// Taken before [`Self::hold`], so lists apply in the order they came.
    pub async fn hold_guard(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.hold_lock.clone().lock_owned().await
    }

    /// Says which wanted replicas are complete.
    async fn report_held(&self, scrub: Option<pumasi_proto::replica::ScrubReport>) {
        let (want, failed) = {
            let r = self.replicas.lock().unwrap();
            (
                // A damaged one is not held, whatever its bitfield says.
                r.want
                    .iter()
                    .filter(|k| !r.quarantine.contains_key(*k))
                    .copied()
                    .collect::<Vec<_>>(),
                r.failed
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        let mut held = Vec::new();
        for root in want {
            if matches!(
                blobs::missing(&self.store, blobs::to_hash(&root)).await,
                Ok((_, true))
            ) {
                held.push(root);
            }
        }
        if let Some(tx) = self.listener.lock().unwrap().as_ref() {
            let _ = tx.send(FromAgent::Held {
                roots: held,
                failed,
                scrub,
            });
        }
    }

    /// Fetches wanted replicas the store lacks, one at a time.
    async fn replica_loop(self) {
        loop {
            // A wake, or the retry period; each second, whether the agent
            // still uses this store.
            let deadline = Instant::now() + RETRY_EVERY;
            loop {
                if Arc::strong_count(&self.alive) <= 1 {
                    return;
                }
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    break;
                }
                let wait = left.min(Duration::from_secs(1));
                if tokio::time::timeout(wait, self.replica_wake.notified())
                    .await
                    .is_ok()
                {
                    break;
                }
            }
            let (want, sources, cluster) = {
                let mut r = self.replicas.lock().unwrap();
                let now = Instant::now();
                let back: Vec<BlobHash> = r
                    .quarantine
                    .iter()
                    .filter(|(_, t)| **t <= now)
                    .map(|(k, _)| *k)
                    .collect();
                for k in back {
                    r.quarantine.remove(&k);
                }
                let want: Vec<BlobHash> = r
                    .want
                    .iter()
                    .filter(|k| !r.quarantine.contains_key(*k))
                    .copied()
                    .collect();
                (want, r.sources.clone(), r.cluster.clone())
            };
            let Some(cluster) = cluster else {
                continue;
            };
            let mut changed = false;
            for root in want {
                let hash = blobs::to_hash(&root);
                if matches!(blobs::missing(&self.store, hash).await, Ok((_, true))) {
                    continue;
                }
                // Keep partial data from the garbage collector.
                let tag = format!("{REPLICA_TAG}{}", hash_hex(&root));
                let _ = self
                    .store
                    .tags()
                    .set(tag.as_str(), blobs::HashAndFormat::hash_seq(hash))
                    .await;
                let mut last: Option<anyhow::Error> = None;
                let never = AtomicBool::new(false);
                for addr in sources
                    .iter()
                    .filter(|a| a.id != self.endpoint.id())
                    .chain([&cluster])
                {
                    match self.fetch_from(addr, hash, &never).await {
                        Ok(_) => {}
                        Err(e) => last = Some(e),
                    }
                    if matches!(blobs::missing(&self.store, hash).await, Ok((_, true))) {
                        last = None;
                        break;
                    }
                }
                // Still wanted? The list may have changed meanwhile.
                let mut r = self.replicas.lock().unwrap();
                if !r.want.contains(&root) {
                    continue;
                }
                match last {
                    None => {
                        r.failed.remove(&root);
                        info!(replica = %hash.fmt_short(), "replica stored");
                    }
                    Some(e) => {
                        debug!(replica = %hash.fmt_short(), "replica download stopped: {e:#}");
                        r.failed.insert(root, format!("{e:#}"));
                    }
                }
                changed = true;
            }
            if changed {
                self.report_held(None).await;
            }
        }
    }

    /// Reads every complete replica back against its hashes; damaged ones
    /// are untagged so the garbage collector deletes them, and fetched
    /// again after a while. Reports the result.
    pub async fn scrub(&self) {
        let want: Vec<BlobHash> = self.replicas.lock().unwrap().want.iter().copied().collect();
        let mut report = pumasi_proto::replica::ScrubReport::default();
        for root in want {
            let hash = blobs::to_hash(&root);
            if !matches!(blobs::missing(&self.store, hash).await, Ok((_, true))) {
                continue;
            }
            report.checked += 1;
            match blobs::verify(&self.store, hash).await {
                Ok(b) => report.bytes += b,
                Err(e) => {
                    warn!("replica {} is damaged: {e:#}", hash.fmt_short());
                    report.bad.push(root);
                    for prefix in [REPLICA_TAG, CACHE_TAG] {
                        let tag = format!("{prefix}{}", hash_hex(&root));
                        let _ = self.store.tags().delete(tag.as_str()).await;
                    }
                    self.cache.lock().unwrap().remove(&root);
                    self.replicas
                        .lock()
                        .unwrap()
                        .quarantine
                        .insert(root, Instant::now() + QUARANTINE);
                }
            }
        }
        info!(
            checked = report.checked,
            damaged = report.bad.len(),
            "scrubbed the replicas"
        );
        self.report_held(Some(report)).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(size: u64, age_s: u64, account: &str, in_use: u32) -> Cached {
        Cached {
            size,
            used: Instant::now() - Duration::from_secs(age_s),
            in_use,
            account: account.into(),
        }
    }

    #[test]
    fn eviction_keeps_queued_work_and_shares() {
        let r = |n: u8| [n; 32];
        let mut cache = HashMap::new();
        cache.insert(r(1), entry(40, 50, "alice", 0));
        cache.insert(r(2), entry(40, 40, "alice", 0));
        cache.insert(r(3), entry(40, 30, "bob", 0));
        cache.insert(r(4), entry(40, 20, "bob", 1));
        let mut p = Policy {
            own_cap: 1000,
            ..Default::default()
        };
        assert!(eviction_order(&cache, &p).is_empty(), "it fits");
        // The cluster's size wins over the agent's own.
        p.cap = Some(100);
        assert_eq!(eviction_order(&cache, &p), vec![r(1), r(2)], "oldest first");
        // What queued tasks mount goes last.
        p.keep.insert(r(1));
        assert_eq!(eviction_order(&cache, &p), vec![r(2), r(3)]);
        // One account over its share loses its own first; one in use stays.
        let p = Policy {
            own_cap: 1000,
            per_account: Some(40),
            ..Default::default()
        };
        // alice had 80 and loses her oldest; bob's 80 includes one in
        // use, so only his idle one can go.
        assert_eq!(eviction_order(&cache, &p), vec![r(1), r(3)]);
    }
}
