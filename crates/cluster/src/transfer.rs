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
    msg::{AdminOp, AdminResponse},
    store::{DatasetView, StoreItem, StoreTarget, StoredOutput, format_size},
};

use crate::server::AdminClient;

/// Where an interrupted download keeps what it has, under the destination.
pub const PARTIAL_DIR: &str = ".pumasi-partial";
const ATTEMPTS: u32 = 5;

impl AdminClient {
    async fn blobs_conn(&self) -> Result<iroh::endpoint::Connection> {
        tokio::time::timeout(
            Duration::from_secs(20),
            self.endpoint.connect(self.cluster.clone(), BLOBS_ALPN),
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
