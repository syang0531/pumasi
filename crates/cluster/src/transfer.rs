//! Moving files in and out of the cluster file store from the CLI
//! (`pumasi store put` / `get`, and `pumasi run` with large inputs and
//! outputs). Both directions resume: an upload asks the control plane what
//! it still lacks, and a download keeps its partial data in
//! `<dest>/.pumasi-partial` until it is complete.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use pumasi_proto::{
    BLOBS_ALPN, blobs,
    job::{JobId, JobSubmission},
    msg::{AdminOp, AdminResponse},
    store::{
        DatasetView, MountRequest, MountSource, StoreItem, StoreTarget, StoredOutput, format_size,
    },
};

use crate::server::AdminClient;

/// Where an interrupted download keeps what it has, under the destination.
pub const PARTIAL_DIR: &str = ".pumasi-partial";
const ATTEMPTS: u32 = 5;

impl AdminClient {
    async fn blobs_conn(&self) -> Result<iroh::endpoint::Connection> {
        tokio::time::timeout(
            Duration::from_secs(20),
            self.endpoint.connect(self.current_addr(), BLOBS_ALPN),
        )
        .await
        .context("timed out connecting to the file store")?
        .context("could not connect to the file store")
    }

    /// Uploads files (path in the collection, file) as a collection and
    /// records it as a new version of dataset `name`, or, without a name,
    /// as job input for the next submission. Only what the control plane
    /// lacks is sent, so running it again after an interruption continues.
    pub async fn store_put(
        &self,
        files: &[(String, PathBuf)],
        name: Option<String>,
        account: Option<String>,
        progress: impl Fn(String),
    ) -> Result<(StoredOutput, Option<DatasetView>)> {
        let dir = std::env::temp_dir().join(format!("pumasi-put-{:016x}", rand::random::<u64>()));
        let store = blobs::open_store(&dir, None).await?;
        let res = async {
            progress(format!("hashing {} files", files.len()));
            let (tt, size, n) = blobs::import_files(&store, files, true).await?;
            let root = tt.hash();
            let stored = StoredOutput {
                root: blobs::from_hash(root),
                size,
                files: n,
            };
            let mut attempt = 0;
            loop {
                attempt += 1;
                let AdminResponse::StoreMissing { missing, have } = self
                    .call(AdminOp::StoreUpload {
                        uploader: *self.endpoint.id().as_bytes(),
                        root: stored.root,
                    })
                    .await?
                else {
                    bail!("unexpected response");
                };
                if have {
                    break;
                }
                if attempt == 1 {
                    progress(format!("uploading {} ({n} files)", format_size(size)));
                } else {
                    progress(format!("resuming the upload (attempt {attempt})"));
                }
                let pushed = async {
                    let conn = self.blobs_conn().await?;
                    blobs::push(&store, conn, root, missing).await
                }
                .await;
                if let Err(e) = pushed {
                    if attempt >= ATTEMPTS {
                        return Err(e);
                    }
                    progress(format!("upload interrupted: {e:#}"));
                    tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
                }
            }
            let view = match self
                .call(AdminOp::StoreCommit {
                    root: stored.root,
                    name,
                    account,
                })
                .await?
            {
                AdminResponse::Datasets(mut v) => v.pop(),
                _ => None,
            };
            drop(tt);
            Ok((stored, view))
        }
        .await;
        store.shutdown().await.ok();
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
        res
    }

    /// This client's endpoint id: the uploader or reader the control plane
    /// lets push or fetch.
    pub fn endpoint_bytes(&self) -> [u8; 32] {
        *self.endpoint.id().as_bytes()
    }

    /// Sends what the control plane lacks of a collection in `store`
    /// (which must have those parts), resuming after interruptions; the
    /// caller then commits it (1.8: the S3 gateway, whose collections
    /// mostly reuse files the control plane has).
    pub async fn push_collection(&self, store: &blobs::Store, root: blobs::Hash) -> Result<()> {
        let mut attempt = 0;
        for _ in 0..4 * ATTEMPTS {
            let AdminResponse::StoreMissing { missing, have } = self
                .call(AdminOp::StoreUpload {
                    uploader: self.endpoint_bytes(),
                    root: blobs::from_hash(root),
                })
                .await?
            else {
                bail!("unexpected response");
            };
            if have {
                return Ok(());
            }
            // Until the control plane has the collection's root it cannot
            // tell which of its files it has, and asks for all of them:
            // send the root alone first (this store may lack those files).
            let root_missing = missing
                .iter_infinite()
                .next()
                .is_some_and(|r| !r.is_empty());
            let ranges = if root_missing {
                blobs::ChunkRangesSeq::root()
            } else {
                missing
            };
            let pushed = async {
                let conn = self.blobs_conn().await?;
                blobs::push(store, conn, root, ranges).await
            }
            .await;
            if let Err(e) = pushed {
                attempt += 1;
                if attempt >= ATTEMPTS {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
        }
        bail!("the upload of {} makes no progress", root.fmt_short())
    }

    /// Fetches some files of a dataset version into `store` (counted from
    /// 0, each with the byte range wanted or all of it), with the
    /// version's root and names.
    pub async fn fetch_dataset_files(
        &self,
        store: &blobs::Store,
        name: &str,
        version: u32,
        files: &[(u64, Option<std::ops::Range<u64>>)],
    ) -> Result<()> {
        let AdminResponse::StoreItems(items) = self
            .call(AdminOp::StoreOpen {
                reader: self.endpoint_bytes(),
                target: StoreTarget::Dataset {
                    name: name.to_string(),
                    version: Some(version),
                },
            })
            .await?
        else {
            bail!("unexpected response");
        };
        let item = items.first().context("nothing to download")?;
        let root = blobs::to_hash(&item.root);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let got = async {
                let conn = self.blobs_conn().await?;
                let r = blobs::fetch_files(store, conn.clone(), root, files).await;
                conn.close(0u32.into(), b"done");
                r
            }
            .await;
            match got {
                Ok(_) => return Ok(()),
                Err(e) if attempt >= ATTEMPTS => return Err(e),
                Err(_) => tokio::time::sleep(Duration::from_secs(attempt as u64)).await,
            }
        }
    }

    /// Submits a job. `upload` (path in `/input`, file) holds the shared
    /// input files that were too large to send with it
    /// ([`pumasi_proto::job::JobFile::load_split`]): they go through the
    /// file store first and are mounted at `/input`.
    pub async fn submit(
        &self,
        mut sub: JobSubmission,
        upload: &[(String, PathBuf)],
        progress: impl Fn(String),
    ) -> Result<JobId> {
        if !upload.is_empty() {
            let (stored, _) = self
                .store_put(upload, None, sub.account.clone(), |m| {
                    progress(format!("input files: {m}"))
                })
                .await?;
            sub.mounts.insert(
                0,
                MountRequest {
                    source: MountSource::Upload { root: stored.root },
                    select: None,
                    at: String::new(),
                },
            );
        }
        match self.call(AdminOp::submit(sub)).await? {
            AdminResponse::Submitted { id } => Ok(id),
            _ => bail!("unexpected response"),
        }
    }

    /// Downloads a dataset version or a job's stored outputs into `dest`.
    /// Returns the files and bytes written.
    pub async fn store_get(
        &self,
        target: StoreTarget,
        dest: &Path,
        progress: impl Fn(String),
    ) -> Result<(u32, u64)> {
        let AdminResponse::StoreItems(items) = self
            .call(AdminOp::StoreOpen {
                reader: *self.endpoint.id().as_bytes(),
                target,
            })
            .await?
        else {
            bail!("unexpected response");
        };
        std::fs::create_dir_all(dest).with_context(|| format!("create {}", dest.display()))?;
        let partial = dest.join(PARTIAL_DIR);
        let store = blobs::open_store(&partial, None).await?;
        let res = self.fetch_items(&store, &items, dest, &progress).await;
        store.shutdown().await.ok();
        drop(store);
        if res.is_ok() {
            let _ = std::fs::remove_dir_all(&partial);
        }
        res
    }

    async fn fetch_items(
        &self,
        store: &blobs::FsStore,
        items: &[StoreItem],
        dest: &Path,
        progress: &impl Fn(String),
    ) -> Result<(u32, u64)> {
        let (mut files, mut bytes) = (0u32, 0u64);
        for item in items {
            let root = blobs::to_hash(&item.root);
            let mut attempt = 0;
            loop {
                let (_, complete) = blobs::missing(store, root).await?;
                if complete {
                    break;
                }
                attempt += 1;
                if attempt == 1 {
                    let into = if item.dir.is_empty() {
                        String::new()
                    } else {
                        format!(" into {}/", item.dir)
                    };
                    progress(format!(
                        "downloading {} ({} files){into}",
                        format_size(item.size),
                        item.files
                    ));
                } else {
                    progress(format!("resuming the download (attempt {attempt})"));
                }
                let got = async {
                    let conn = self.blobs_conn().await?;
                    blobs::fetch(store, conn, root).await
                }
                .await;
                if let Err(e) = got {
                    if attempt >= ATTEMPTS {
                        return Err(e);
                    }
                    progress(format!("download interrupted: {e:#}"));
                    tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
                }
            }
            let entries = blobs::entries(store, root).await?;
            let target = if item.dir.is_empty() {
                dest.to_path_buf()
            } else {
                dest.join(&item.dir)
            };
            bytes += blobs::export(store, &entries, &target, true, |n| Some(n.to_string())).await?;
            files += entries.len() as u32;
        }
        Ok((files, bytes))
    }
}
