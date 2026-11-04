//! A scratch directory removed on drop.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("pumasi-{tag}-{:016x}", rand_u64()));
        std::fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self(path))
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

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one((std::process::id(), std::time::SystemTime::now()))
}
