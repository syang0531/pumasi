//! The control plane's side of the cluster file store: an iroh-blobs store
//! in `<cluster dir>/blobs`, who may read and write it over the blobs
//! ALPN, the tags that keep data alive, and collection work (combining
//! job outputs, importing private images).
//!
//! Access:
//!
//! - Members (agents) may read anything (a collection's hash works as a
//!   read capability: nodes only learn the hashes of what they are given)
//!   and push while they have a task running (their outputs).
//! - The CLI's endpoint may push one upload, or read the collections it
//!   asked for, for an hour after an admin call allowed it; reads are
//!   metered as egress.
//! - Nobody else gets past the connection.
//!
//! Tags (the references garbage collection keeps):
//!
//! | Tag | Keeps |
//! | --- | --- |
//! | `ds/<name>/<version>` | a dataset version |
//! | `out/<job>/<index>` | a task's stored output |
//! | `chk/<job>/<index>/<attempt>` | a check run's stored output |
//! | `in/<job>/<root>` | a collection an unfinished job mounts |
//! | `up/<root>` | a committed upload not used by a job yet |
//! | `img/<arch>/<digest>` | a private image pulled into the store |
//!
//! Pushed data has no tag until the report or commit that names it, so
//! the garbage collector also keeps every collection pushed or allowed in
//! the last two hours.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use iroh::EndpointId;
use iroh_blobs::{
    BlobsProtocol,
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ObserveMode, ProviderMessage,
        RequestMode, RequestUpdate,
    },
};
use pumasi_proto::{
    blobs::{self, Collection, Entry, FsStore, Hash, HashAndFormat, ProtectCb, ProtectOutcome},
    registry::{Credentials, Registry},
    store::{
        BlobHash, IMAGE_CONFIG, IMAGE_MANIFEST, ImageBlob, ImageManifest, StoredOutput, hash_hex,
        image_layer_name,
    },
};
use tracing::{debug, info, warn};

/// How long an admin call's permission to upload or download lasts.
pub const GRANT_TTL: Duration = Duration::from_secs(3600);
/// Pushed or allowed collections are kept from garbage collection this
/// long, until a tag refers to them.
const PROTECT_RECENT: Duration = Duration::from_secs(2 * 3600);

/// What a connecting endpoint is to the cluster.
#[derive(Debug, Clone, Copy, Default)]
pub struct Peer {
    pub member: bool,
    /// A member with a task running: may push its output.
    pub pusher: bool,
}

/// Looks up an endpoint among the members.
pub type Gate = Arc<dyn Fn(&EndpointId) -> Peer + Send + Sync>;
/// Called with (account, bytes) when a download by the CLI ends.
pub type Egress = Arc<dyn Fn(&str, u64) + Send + Sync>;

#[derive(Debug, Clone)]
struct Grant {
    expires: Instant,
    account: String,
    read: HashSet<Hash>,
    write: HashSet<Hash>,
}

#[derive(Clone)]
pub struct FileStore {
    pub(crate) store: FsStore,
    dir: PathBuf,
    grants: Arc<Mutex<HashMap<EndpointId, Grant>>>,
    /// Collections pushed or allowed recently, and when.
    recent: Arc<Mutex<HashMap<Hash, Instant>>>,
}

impl std::fmt::Debug for FileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileStore({})", self.dir.display())
    }
}

impl FileStore {
    pub async fn open(dir: &Path, gc_interval: Duration) -> Result<Self> {
        let recent: Arc<Mutex<HashMap<Hash, Instant>>> = Default::default();
        let cell: Arc<OnceLock<blobs::Store>> = Default::default();
        let protect: ProtectCb = {
            let (recent, cell) = (recent.clone(), cell.clone());
            Arc::new(move |live| {
                let roots: Vec<Hash> = {
                    let mut r = recent.lock().unwrap();
                    r.retain(|_, t| t.elapsed() < PROTECT_RECENT);
                    r.keys().copied().collect()
                };
                // The callback's future must be Sync, which the store's
                // calls are not: read the collections in a task.
                let cell = cell.clone();
                let members = tokio::spawn(async move {
                    let mut out = Vec::new();
                    if let Some(store) = cell.get() {
                        for root in roots {
                            out.extend(blobs::hash_seq_members(store, root).await);
                        }
                    }
                    out
                });
                Box::pin(async move {
                    if let Ok(m) = members.await {
                        live.extend(m);
                    }
                    ProtectOutcome::Continue
                })
            })
        };
        let store = blobs::open_store(dir, Some((gc_interval, Some(protect)))).await?;
        let _ = cell.set(store.clone().into());
        Ok(Self {
            store,
            dir: dir.to_path_buf(),
            grants: Default::default(),
            recent,
        })
    }

    pub fn api(&self) -> &blobs::Store {
        &self.store
    }

    /// Bytes the store takes on disk.
    pub fn disk_bytes(&self) -> u64 {
        fn walk(p: &Path) -> u64 {
            let Ok(rd) = std::fs::read_dir(p) else {
                return 0;
            };
            rd.flatten()
                .map(|e| match e.file_type() {
                    Ok(t) if t.is_dir() => walk(&e.path()),
                    Ok(_) => e.metadata().map(|m| m.len()).unwrap_or(0),
                    Err(_) => 0,
                })
                .sum()
        }
        walk(&self.dir)
    }

    fn protect(&self, root: Hash) {
        self.recent.lock().unwrap().insert(root, Instant::now());
    }

    /// Lets `who` push `root` for an hour.
    pub fn allow_upload(&self, who: EndpointId, root: Hash) {
        self.protect(root);
        let mut g = self.grants.lock().unwrap();
        let e = g.entry(who).or_insert_with(|| Grant {
            expires: Instant::now() + GRANT_TTL,
            account: String::new(),
            read: HashSet::new(),
            write: HashSet::new(),
        });
        e.expires = Instant::now() + GRANT_TTL;
        e.write.insert(root);
    }

    /// Lets `who` fetch these collections for an hour; the bytes count as
    /// egress of `account`.
    pub fn allow_read(&self, who: EndpointId, account: &str, roots: &[Hash]) {
        let mut g = self.grants.lock().unwrap();
        let e = g.entry(who).or_insert_with(|| Grant {
            expires: Instant::now() + GRANT_TTL,
            account: account.to_string(),
            read: HashSet::new(),
            write: HashSet::new(),
        });
        e.expires = Instant::now() + GRANT_TTL;
        e.account = account.to_string();
        e.read.extend(roots.iter().copied());
    }

    fn grant(&self, who: &EndpointId) -> Option<Grant> {
        let mut g = self.grants.lock().unwrap();
        g.retain(|_, v| v.expires > Instant::now());
        g.get(who).cloned()
    }

    /// The protocol handler for the blobs ALPN, with the access rules
    /// above.
    pub fn protocol(&self, gate: Gate, egress: Egress) -> BlobsProtocol {
        let mask = EventMask {
            connected: ConnectMode::Intercept,
            get: RequestMode::InterceptLog,
            get_many: RequestMode::Intercept,
            push: RequestMode::Intercept,
            observe: ObserveMode::Intercept,
            ..EventMask::DEFAULT
        };
        let (tx, mut rx) = EventSender::channel(64, mask);
        let this = self.clone();
        tokio::spawn(async move {
            let mut conns: HashMap<u64, EndpointId> = HashMap::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    ProviderMessage::ClientConnected(m) => {
                        let ok = match m.endpoint_id {
                            Some(id) if gate(&id).member || this.grant(&id).is_some() => {
                                conns.insert(m.connection_id, id);
                                Ok(())
                            }
                            other => {
                                debug!(peer = ?other, "refused a file store connection");
                                Err(AbortReason::Permission)
                            }
                        };
                        m.tx.send(ok).await.ok();
                    }
                    ProviderMessage::ConnectionClosed(m) => {
                        conns.remove(&m.connection_id);
                    }
                    ProviderMessage::GetRequestReceived(m) => {
                        let Some(id) = conns.get(&m.connection_id).copied() else {
                            m.tx.send(Err(AbortReason::Permission)).await.ok();
                            continue;
                        };
                        if gate(&id).member {
                            m.tx.send(Ok(())).await.ok();
                            // The request ends if nobody takes its updates.
                            let mut updates = m.rx;
                            tokio::spawn(
                                async move { while let Ok(Some(_)) = updates.recv().await {} },
                            );
                            continue;
                        }
                        match this.grant(&id) {
                            Some(g) if g.read.contains(&m.request.hash) => {
                                m.tx.send(Ok(())).await.ok();
                                let mut updates = m.rx;
                                let egress = egress.clone();
                                tokio::spawn(async move {
                                    let mut bytes = 0u64;
                                    while let Ok(Some(u)) = updates.recv().await {
                                        match u {
                                            RequestUpdate::Completed(c) => {
                                                bytes += c.stats.payload_bytes_sent
                                            }
                                            RequestUpdate::Aborted(a) => {
                                                bytes += a.stats.payload_bytes_sent
                                            }
                                            _ => {}
                                        }
                                    }
                                    if bytes > 0 {
                                        egress(&g.account, bytes);
                                    }
                                });
                            }
                            _ => {
                                m.tx.send(Err(AbortReason::Permission)).await.ok();
                            }
                        }
                    }
                    ProviderMessage::GetManyRequestReceived(m) => {
                        let ok = conns
                            .get(&m.connection_id)
                            .is_some_and(|id| gate(id).member);
                        m.tx.send(if ok {
                            Ok(())
                        } else {
                            Err(AbortReason::Permission)
                        })
                        .await
                        .ok();
                    }
                    ProviderMessage::ObserveRequestReceived(m) => {
                        let ok = conns.contains_key(&m.connection_id);
                        m.tx.send(if ok {
                            Ok(())
                        } else {
                            Err(AbortReason::Permission)
                        })
                        .await
                        .ok();
                    }
                    ProviderMessage::PushRequestReceived(m) => {
                        let hash = m.request.hash;
                        let ok = conns.get(&m.connection_id).is_some_and(|id| {
                            gate(id).pusher
                                || this.grant(id).is_some_and(|g| g.write.contains(&hash))
                        });
                        if ok {
                            this.protect(hash);
                        } else {
                            warn!(hash = %hash.fmt_short(), "refused a push to the file store");
                        }
                        m.tx.send(if ok {
                            Ok(())
                        } else {
                            Err(AbortReason::Permission)
                        })
                        .await
                        .ok();
                    }
                    ProviderMessage::Throttle(m) => {
                        m.tx.send(Ok(())).await.ok();
                    }
                    _ => {}
                }
            }
        });
        BlobsProtocol::new(&self.store, Some(tx))
    }

    pub async fn tag(&self, name: &str, root: Hash) -> Result<()> {
        // From now on the tag keeps it.
        self.recent.lock().unwrap().remove(&root);
        self.store
            .tags()
            .set(name, HashAndFormat::hash_seq(root))
            .await
            .map_err(|e| anyhow::anyhow!("tag {name}: {e}"))
    }

    pub async fn untag(&self, name: &str) -> Result<()> {
        self.store
            .tags()
            .delete(name)
            .await
            .map_err(|e| anyhow::anyhow!("delete tag {name}: {e}"))?;
        Ok(())
    }

    pub async fn untag_prefix(&self, prefix: &str) -> Result<()> {
        self.store
            .tags()
            .delete_prefix(prefix)
            .await
            .map_err(|e| anyhow::anyhow!("delete tags {prefix}*: {e}"))?;
        Ok(())
    }

    /// Checks that a collection is complete and reads its files.
    pub async fn entries(&self, root: &BlobHash) -> Result<Vec<Entry>> {
        let root = blobs::to_hash(root);
        let (_, complete) = blobs::missing(&self.store, root).await?;
        anyhow::ensure!(
            complete,
            "collection {} is not complete in the store",
            root.fmt_short()
        );
        blobs::entries(&self.store, root).await
    }

    /// The size and file count of a complete collection.
    pub async fn stored(&self, root: &BlobHash) -> Result<StoredOutput> {
        let entries = self.entries(root).await?;
        Ok(StoredOutput {
            root: *root,
            size: entries.iter().map(|e| e.size).sum(),
            files: entries.len() as u32,
        })
    }

    /// One collection made of several, each under its own directory
    /// (`task-0/...`), tagged `tag`. No file data is copied.
    pub async fn combine(&self, parts: &[(String, BlobHash)], tag: &str) -> Result<StoredOutput> {
        let mut c = Collection::default();
        let mut size = 0;
        for (dir, root) in parts {
            for e in self.entries(root).await? {
                c.push(format!("{dir}/{}", e.name), e.hash);
                size += e.size;
            }
        }
        let files = c.len() as u32;
        let tt = c
            .store(&self.store)
            .await
            .map_err(|e| anyhow::anyhow!("store collection: {e}"))?;
        self.tag(tag, tt.hash()).await?;
        Ok(StoredOutput {
            root: blobs::from_hash(tt.hash()),
            size,
            files,
        })
    }

    /// Imports files as a collection tagged `tag` (copied into the store).
    pub async fn import(&self, files: &[(String, PathBuf)], tag: &str) -> Result<StoredOutput> {
        let (tt, size, files) = blobs::import_files(&self.store, files, false).await?;
        self.tag(tag, tt.hash()).await?;
        Ok(StoredOutput {
            root: blobs::from_hash(tt.hash()),
            size,
            files,
        })
    }

    /// The root a tag points at, if the tag exists.
    pub async fn tagged(&self, name: &str) -> Result<Option<BlobHash>> {
        Ok(self
            .store
            .tags()
            .get(name)
            .await
            .map_err(|e| anyhow::anyhow!("read tag {name}: {e}"))?
            .map(|t| blobs::from_hash(t.hash)))
    }

    /// Every tag (name, root collection), for control plane replicas that
    /// copy the store.
    pub async fn tags(&self) -> Result<Vec<(String, BlobHash)>> {
        use n0_future::StreamExt;
        let mut out = Vec::new();
        for prefix in TAG_PREFIXES {
            let mut tags = self
                .store
                .tags()
                .list_prefix(prefix.as_bytes())
                .await
                .map_err(|e| anyhow::anyhow!("list tags: {e}"))?;
            while let Some(t) = tags.next().await {
                let t = t.map_err(|e| anyhow::anyhow!("list tags: {e}"))?;
                out.push((
                    String::from_utf8_lossy(t.name.as_ref()).into_owned(),
                    blobs::from_hash(t.hash),
                ));
            }
        }
        Ok(out)
    }

    /// Makes the store's tags exactly `desired` (at startup).
    pub async fn reconcile(&self, desired: &[(String, BlobHash)]) -> Result<()> {
        let desired: Vec<(String, Hash)> = desired
            .iter()
            .map(|(n, r)| (n.clone(), blobs::to_hash(r)))
            .collect();
        let removed = blobs::reconcile_tags(&self.store, TAG_PREFIXES, &desired).await?;
        if removed > 0 {
            info!(removed, "dropped file store tags nothing refers to");
        }
        Ok(())
    }

    /// Pulls `reference` for `arch` with the cluster's registry logins and
    /// keeps it as an image collection (see [`pumasi_proto::store::ImageBlob`])
    /// tagged `img/<owner>/<digest>`.
    pub async fn pull_image(
        &self,
        reference: &str,
        arch: &str,
        owner: &str,
        creds: HashMap<String, Credentials>,
    ) -> Result<ImageBlob> {
        let reg = Registry::with_credentials(creds)?.for_arch(arch);
        let resolved = reg.resolve(reference).await?;
        let work = self.scratch()?;
        let res: Result<ImageBlob> = async {
            let mut list: Vec<(String, PathBuf)> = Vec::new();
            let manifest = ImageManifest {
                reference: reference.to_string(),
                digest: resolved.digest.clone(),
                config_digest: resolved.config_digest.clone(),
                config: resolved.config.clone(),
                layers: resolved.layers.clone(),
            };
            let p = work.join(IMAGE_MANIFEST);
            std::fs::write(&p, serde_json::to_vec(&manifest)?)?;
            list.push((IMAGE_MANIFEST.to_string(), p));
            let p = work.join(IMAGE_CONFIG);
            std::fs::write(&p, &resolved.config_bytes)?;
            list.push((IMAGE_CONFIG.to_string(), p));
            let cancel = std::sync::atomic::AtomicBool::new(false);
            for (i, layer) in resolved.layers.iter().enumerate() {
                let name = image_layer_name(i);
                let p = work.join(&name);
                reg.download(&resolved.image, layer, &p, &cancel)
                    .await
                    .with_context(|| format!("pull {reference}"))?;
                list.push((name, p));
            }
            let stored = self
                .import(&list, &img_tag(owner, &resolved.digest))
                .await?;
            Ok(ImageBlob {
                root: stored.root,
                digest: resolved.digest.clone(),
                reference: reference.to_string(),
            })
        }
        .await;
        let _ = std::fs::remove_dir_all(&work);
        res
    }

    /// A scratch directory inside the store's directory (same disk).
    pub fn scratch(&self) -> Result<PathBuf> {
        let p = self
            .dir
            .join("tmp")
            .join(format!("{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(&p).with_context(|| format!("create {}", p.display()))?;
        Ok(p)
    }
}

/// Every tag the control plane manages.
const TAG_PREFIXES: &[&str] = &["ds/", "out/", "chk/", "in/", "up/", "img/", "job/", "ckpt/"];

pub fn ds_tag(name: &str, version: u32) -> String {
    format!("ds/{name}/{version}")
}

pub fn out_tag(job: u128, index: u32) -> String {
    format!("out/{job:016x}/{index}")
}

pub fn out_prefix(job: u128) -> String {
    format!("out/{job:016x}/")
}

pub fn chk_tag(job: u128, index: u32, attempt: u32) -> String {
    format!("chk/{job:016x}/{index}/{attempt}")
}

/// A task's latest checkpoint (1.9).
pub fn ckpt_tag(job: u128, index: u32) -> String {
    format!("ckpt/{job:016x}/{index}")
}

pub fn ckpt_prefix(job: u128) -> String {
    format!("ckpt/{job:016x}/")
}

pub fn in_prefix(job: u128) -> String {
    format!("in/{job:016x}/")
}

pub fn in_tag(job: u128, root: &BlobHash) -> String {
    format!("in/{job:016x}/{}", hash_hex(root))
}

pub fn up_tag(root: &BlobHash) -> String {
    format!("up/{}", hash_hex(root))
}

pub fn img_tag(arch: &str, digest: &str) -> String {
    format!("img/{arch}/{digest}")
}
