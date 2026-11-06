//! Files a browser sends (job inputs, datasets) land in a temporary
//! directory first, keeping their relative paths, so the CLI's own code
//! (`JobFile::load_split`, `store_put`) takes them from there.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{Context as _, Result, bail};
use axum::extract::Multipart;
use n0_future::Stream;
use pumasi_proto::job::validate_rel_path;
use tokio::io::AsyncWriteExt;

/// A directory removed when dropped.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Result<Self> {
        let p =
            std::env::temp_dir().join(format!("pumasi-ui-{tag}-{:016x}", rand::random::<u64>()));
        // Only for this user (uploads and output zips are other people's
        // data, and the temporary directory is shared on Unix), and never
        // a directory someone else made first.
        let mut b = std::fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
        b.create(&p)
            .with_context(|| format!("create {}", p.display()))?;
        Ok(Self(p))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct Upload {
    pub dir: TempDir,
    /// Text fields by name.
    pub fields: HashMap<String, String>,
    /// Files written, as paths relative to `dir`.
    pub files: Vec<String>,
}

impl Upload {
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .get(name)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
    }
}

/// Reads a multipart form: parts with a file name are written under the
/// directory (directory uploads send `folder/sub/file`), the rest are
/// text fields.
pub async fn receive(mut form: Multipart, tag: &str) -> Result<Upload> {
    let dir = TempDir::new(tag)?;
    let mut fields = HashMap::new();
    let mut files = Vec::new();
    while let Some(mut part) = form.next_field().await.context("read the upload")? {
        let name = part.name().unwrap_or("").to_string();
        match part.file_name().map(str::to_string) {
            Some(file) if !file.is_empty() => {
                let rel = file.replace('\\', "/");
                let rel = rel.trim_start_matches("./").to_string();
                validate_rel_path(&rel).with_context(|| format!("file name {file:?}"))?;
                let target = dir.path().join(&rel);
                if let Some(parent) = target.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let mut out = tokio::fs::File::create(&target)
                    .await
                    .with_context(|| format!("create {}", target.display()))?;
                while let Some(chunk) = part.chunk().await.context("read the upload")? {
                    out.write_all(&chunk).await?;
                }
                out.flush().await?;
                files.push(rel);
            }
            // A file input left empty sends a part without a name.
            Some(_) => while part.chunk().await.context("read the upload")?.is_some() {},
            None => {
                let text = part.text().await.context("read the form")?;
                if fields.insert(name.clone(), text).is_some() && name != "files" {
                    bail!("field {name} sent twice");
                }
            }
        }
    }
    Ok(Upload { dir, fields, files })
}

/// Every file under `dir`, as (relative path with `/`, full path), in a
/// stable order.
pub fn walk(dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).with_context(|| format!("read {}", d.display()))? {
            let e = e?;
            let p = e.path();
            if e.file_type()?.is_dir() {
                stack.push(p);
            } else {
                let rel = p
                    .strip_prefix(dir)
                    .unwrap_or(&p)
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push((rel, p));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// A file streamed to the browser that takes its temporary directory with
/// it when the response ends.
pub struct TempFileStream {
    // Dropped before `_dir`, so the file is closed before the directory
    // goes (Windows cannot remove open files).
    inner: tokio_util::io::ReaderStream<tokio::fs::File>,
    _dir: TempDir,
}

impl TempFileStream {
    pub async fn open(dir: TempDir, file: &Path) -> Result<Self> {
        let f = tokio::fs::File::open(file).await?;
        Ok(Self {
            inner: tokio_util::io::ReaderStream::new(f),
            _dir: dir,
        })
    }
}

impl Stream for TempFileStream {
    type Item = std::io::Result<axum::body::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn temporary_directories_are_private() {
        let d = super::TempDir::new("test").unwrap();
        assert!(d.path().is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(d.path()).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        let p = d.path().to_path_buf();
        drop(d);
        assert!(!p.exists());
    }
}
