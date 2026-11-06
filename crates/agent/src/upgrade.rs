//! Self-upgrade: installs a release the cluster offers, and rolls back if
//! the new binary does not work.
//!
//! Steps for an offered [`SignedRelease`]:
//!
//! 1. Refuse it unless the node accepts upgrades, the signature is the
//!    cluster's (the key the node joined), the platform is this one, and
//!    this node did not fail with the same binary before.
//! 2. Download it over the cluster connection in chunks and check its size
//!    and SHA-256 against the signed release.
//! 3. Copy it next to the running binary and run `<new> --version`: it must
//!    start and print the release's version.
//! 4. Back the running binary up to `<data dir>/upgrade/previous`, rename
//!    the new one over it (on Windows the running one is renamed aside
//!    first) and write `pending.json`. The caller then restarts.
//!
//! The restarted process is on trial until the cluster welcomes it. If the
//! cluster rejects it, it cannot reach the cluster for
//! [`TRIAL_TIMEOUT`], or it is started more than [`MAX_TRIAL_BOOTS`] times
//! without getting there (crash loop under a service manager), it puts the
//! previous binary back, remembers the failed release, and restarts again.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use iroh::endpoint::Connection;
use pumasi_proto::{
    msg::{FetchReply, FetchRequest, RELEASE_CHUNK, Role, UpgradeHello},
    release::{SignedRelease, Target, hex, sha256_file},
    wire::{read_msg, write_msg},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// How long a freshly upgraded agent may go without being welcomed by the
/// cluster before it rolls back.
pub const TRIAL_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Starts of a freshly upgraded binary that never got welcomed before it
/// rolls back.
pub const MAX_TRIAL_BOOTS: u32 = 3;
const SMOKE_TIMEOUT: Duration = Duration::from_secs(30);
const PENDING_FILE: &str = "pending.json";
const FAILED_FILE: &str = "failed.json";

/// How this agent upgrades itself.
#[derive(Debug, Clone)]
pub struct UpgradeConfig {
    /// The binary to replace: the one running.
    pub exe: PathBuf,
    /// `Some(true)`: accept upgrades (`--auto-upgrade`, needed on provider
    /// machines). `Some(false)`: refuse them. `None`: accept them on the
    /// owner's machines only.
    pub auto: Option<bool>,
    /// Run `<new binary> --version` before swapping it in. Off only in
    /// tests, whose "binaries" are not executables.
    pub smoke_test: bool,
}

/// Why [`crate::Agent::serve`] returned. Either way the caller restarts
/// the process so the binary now at the agent's path runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentExit {
    /// A new binary was installed.
    Upgraded { version: String },
    /// The new binary did not work; the previous one was put back.
    RolledBack { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pending {
    from_sha256: String,
    to_sha256: String,
    version: String,
    boots: u32,
    started_unix: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Failed {
    releases: Vec<(String, String)>,
}

/// What the agent found about a previous upgrade when it started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
    Normal,
    /// Running a freshly installed binary that has not been welcomed yet.
    Trial,
    /// Rolled back already; restart to run the previous binary.
    RolledBack(String),
}

/// A downloaded, verified release waiting to be installed.
pub struct Staged {
    path: PathBuf,
    release: SignedRelease,
}

pub(crate) struct Upgrader {
    config: UpgradeConfig,
    dir: PathBuf,
    exe_sha256: [u8; 32],
}

impl Upgrader {
    pub fn new(config: UpgradeConfig, data_dir: &Path) -> Result<Self> {
        let exe_sha256 = sha256_file(&config.exe)
            .with_context(|| format!("hash the agent binary {}", config.exe.display()))?;
        let dir = data_dir.join("upgrade");
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        // Left behind by a Windows swap; deletable once that binary exited.
        let _ = std::fs::remove_file(old_path(&config.exe));
        Ok(Self {
            config,
            dir,
            exe_sha256,
        })
    }

    pub fn hello(&self) -> UpgradeHello {
        UpgradeHello {
            exe_sha256: self.exe_sha256,
            auto: self.config.auto,
        }
    }

    pub fn accepts(&self, role: Role) -> bool {
        self.hello().accepts(role)
    }

    fn pending(&self) -> Option<Pending> {
        let text = std::fs::read_to_string(self.dir.join(PENDING_FILE)).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn save_pending(&self, p: &Pending) -> Result<()> {
        write_atomic(&self.dir.join(PENDING_FILE), &serde_json::to_vec_pretty(p)?)
    }

    fn clear_pending(&self) {
        let _ = std::fs::remove_file(self.dir.join(PENDING_FILE));
    }

    /// The reason this node failed with a release before, if it did.
    pub fn failed_before(&self, sha256: &[u8; 32]) -> Option<String> {
        let failed = self.failed();
        let key = hex(sha256);
        failed
            .releases
            .into_iter()
            .find(|(s, _)| *s == key)
            .map(|(_, r)| r)
    }

    fn failed(&self) -> Failed {
        std::fs::read_to_string(self.dir.join(FAILED_FILE))
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default()
    }

    pub fn record_failure(&self, sha256: &[u8; 32], reason: &str) {
        let mut failed = self.failed();
        let key = hex(sha256);
        failed.releases.retain(|(s, _)| *s != key);
        failed.releases.push((key, reason.to_string()));
        // Keep the file small.
        let n = failed.releases.len();
        if n > 32 {
            failed.releases.drain(..n - 32);
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&failed)
            && let Err(e) = write_atomic(&self.dir.join(FAILED_FILE), &bytes)
        {
            warn!("could not record the failed upgrade: {e:#}");
        }
    }

    /// Checks for an unfinished upgrade. Counts this start if the agent is
    /// on trial, and rolls back after too many.
    pub fn startup(&self) -> Result<Startup> {
        let Some(mut p) = self.pending() else {
            return Ok(Startup::Normal);
        };
        if p.to_sha256 != hex(&self.exe_sha256) {
            // Already rolled back, or replaced by hand.
            self.clear_pending();
            return Ok(Startup::Normal);
        }
        p.boots += 1;
        if p.boots > MAX_TRIAL_BOOTS {
            let reason = format!(
                "version {} started {} times without reaching the cluster",
                p.version, MAX_TRIAL_BOOTS
            );
            self.rollback(&reason)?;
            return Ok(Startup::RolledBack(reason));
        }
        self.save_pending(&p)?;
        info!(version = %p.version, start = p.boots, "running a new agent binary on trial");
        Ok(Startup::Trial)
    }

    /// The cluster welcomed the new binary: the upgrade is done.
    pub fn commit(&self) {
        if let Some(p) = self.pending()
            && p.to_sha256 == hex(&self.exe_sha256)
        {
            info!(version = %p.version, "upgrade complete");
            self.clear_pending();
        }
    }

    /// Puts the previous binary back and remembers the failed release.
    pub fn rollback(&self, reason: &str) -> Result<()> {
        warn!("rolling back the agent binary: {reason}");
        let previous = self.dir.join(previous_name());
        ensure!(
            previous.exists(),
            "no previous binary at {}",
            previous.display()
        );
        let tmp = sibling_tmp(&self.config.exe, "rollback");
        std::fs::copy(&previous, &tmp)
            .with_context(|| format!("copy {} to {}", previous.display(), tmp.display()))?;
        swap_in(&tmp, &self.config.exe)?;
        self.record_failure(&self.exe_sha256, reason);
        self.clear_pending();
        Ok(())
    }

    /// Steps 1 and 2: checks the offer and downloads the binary.
    pub async fn fetch(
        &self,
        conn: &Connection,
        release: SignedRelease,
        cluster: &[u8; 32],
        role: Role,
    ) -> Result<Staged> {
        ensure!(
            self.accepts(role),
            "upgrades are off on this node (start the agent with --auto-upgrade to accept them)"
        );
        release.verify(cluster)?;
        let file = release.file().clone();
        ensure!(
            file.target == Target::host(),
            "release is for {}, this node is {}",
            file.target,
            Target::host()
        );
        // Make sure the swap can happen before downloading anything.
        let probe = sibling_tmp(&self.config.exe, "probe");
        std::fs::write(&probe, b"").with_context(|| {
            format!(
                "cannot write next to the agent binary {} (run the agent as a user that owns it)",
                self.config.exe.display()
            )
        })?;
        let _ = std::fs::remove_file(&probe);

        let path = self.dir.join(format!("{}.download", hex(&file.sha256)));
        let mut out =
            std::fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let mut hasher = pumasi_proto::release::Sha256Hasher::default();
        let mut offset = 0u64;
        while offset < file.size {
            let len = (file.size - offset).min(RELEASE_CHUNK as u64) as u32;
            let chunk = fetch_chunk(conn, file.sha256, offset, len).await?;
            ensure!(
                !chunk.is_empty() && chunk.len() as u64 <= (file.size - offset),
                "bad chunk at {offset}"
            );
            offset += chunk.len() as u64;
            // Hashing and writing megabytes off the async threads, which
            // also carry the heartbeats: a slow machine otherwise misses
            // enough of them to be dropped mid-upgrade.
            (out, hasher) = blocking(move || {
                std::io::Write::write_all(&mut out, &chunk)?;
                hasher.update(&chunk);
                Ok((out, hasher))
            })
            .await?;
        }
        blocking(move || Ok(out.sync_all()?)).await?;
        let got = hasher.finish();
        if got != file.sha256 {
            let _ = std::fs::remove_file(&path);
            bail!(
                "downloaded binary has SHA-256 {}, the signed release says {}",
                hex(&got),
                hex(&file.sha256)
            );
        }
        Ok(Staged { path, release })
    }

    /// Steps 3 and 4: tries the new binary and swaps it in.
    pub async fn install(&self, staged: Staged) -> Result<String> {
        let file = staged.release.file().clone();
        let new = sibling_tmp(&self.config.exe, "new");
        let res = async {
            let (from, to) = (staged.path.clone(), new.clone());
            let copied = blocking(move || {
                std::fs::copy(&from, &to)
                    .with_context(|| format!("copy the new binary to {}", to.display()))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&to, std::fs::Permissions::from_mode(0o755))?;
                }
                sha256_file(&to)
            })
            .await?;
            // The copy must still be the verified binary.
            ensure!(copied == file.sha256, "new binary changed while copying");
            if self.config.smoke_test {
                smoke_test(&new, &file.version).await?;
            }
            let previous = self.dir.join(previous_name());
            let exe = self.config.exe.clone();
            blocking(move || {
                std::fs::copy(&exe, &previous).with_context(|| {
                    format!("back up the current binary to {}", previous.display())
                })?;
                Ok(())
            })
            .await?;
            self.save_pending(&Pending {
                from_sha256: hex(&self.exe_sha256),
                to_sha256: hex(&file.sha256),
                version: file.version.clone(),
                boots: 0,
                started_unix: now_unix(),
            })?;
            if let Err(e) = swap_in(&new, &self.config.exe) {
                self.clear_pending();
                return Err(e);
            }
            Ok(())
        }
        .await;
        let _ = std::fs::remove_file(&staged.path);
        if res.is_err() {
            let _ = std::fs::remove_file(&new);
        }
        res?;
        info!(version = %file.version, "installed a new agent binary; restarting");
        Ok(file.version)
    }
}

/// Runs `f` on the blocking thread pool.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .context("blocking task failed")?
}

async fn fetch_chunk(
    conn: &Connection,
    sha256: [u8; 32],
    offset: u64,
    len: u32,
) -> Result<Vec<u8>> {
    let (mut send, mut recv) = conn.open_bi().await.context("open download stream")?;
    write_msg(
        &mut send,
        &FetchRequest {
            sha256,
            offset,
            len,
        },
    )
    .await?;
    send.finish()?;
    match read_msg::<_, FetchReply>(&mut recv).await? {
        Some(FetchReply::Data(bytes)) => Ok(bytes),
        Some(FetchReply::Error(e)) => bail!("cluster refused the download: {e}"),
        None => bail!("cluster closed the download stream"),
    }
}

/// Runs `<new> --version`; it must exit 0 and print `version`.
async fn smoke_test(new: &Path, version: &str) -> Result<()> {
    let out = tokio::time::timeout(
        SMOKE_TIMEOUT,
        tokio::process::Command::new(new)
            .arg("--version")
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("new binary did not answer --version in time")?
    .with_context(|| format!("could not run the new binary {}", new.display()))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    ensure!(
        out.status.success(),
        "new binary failed to start ({}): {}",
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
    ensure!(
        stdout.split_whitespace().any(|w| w == version),
        "new binary reports {:?}, the release says {version}",
        stdout.trim()
    );
    Ok(())
}

fn previous_name() -> &'static str {
    if cfg!(windows) {
        "previous.exe"
    } else {
        "previous"
    }
}

/// A temporary path in the binary's directory, so the final rename stays
/// on one filesystem.
fn sibling_tmp(exe: &Path, tag: &str) -> PathBuf {
    let name = exe
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pumasi".into());
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    exe.with_file_name(format!(".{name}.{tag}-{}{suffix}", std::process::id()))
}

fn old_path(exe: &Path) -> PathBuf {
    let mut s = exe.as_os_str().to_owned();
    s.push(".old");
    PathBuf::from(s)
}

/// Moves `new` over `exe`. Atomic on Unix. Windows cannot replace a
/// running executable but can rename it, so the running one moves to
/// `<exe>.old` first (deleted at the next start).
fn swap_in(new: &Path, exe: &Path) -> Result<()> {
    if cfg!(windows) {
        let old = old_path(exe);
        let _ = std::fs::remove_file(&old);
        std::fs::rename(exe, &old).with_context(|| format!("move {} aside", exe.display()))?;
        if let Err(e) = std::fs::rename(new, exe) {
            let _ = std::fs::rename(&old, exe);
            return Err(e).with_context(|| format!("put the new binary at {}", exe.display()));
        }
    } else {
        std::fs::rename(new, exe)
            .with_context(|| format!("put the new binary at {}", exe.display()))?;
    }
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::release::{Release, ReleaseFile, sha256};

    fn setup(tag: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("pumasi-up-{tag}-{}", rand_u64()));
        let bin = root.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let exe = bin.join("pumasi");
        std::fs::write(&exe, b"old binary").unwrap();
        (root.join("data"), exe)
    }

    fn rand_u64() -> u64 {
        use std::hash::{BuildHasher, RandomState};
        RandomState::new().hash_one(std::time::SystemTime::now())
    }

    fn upgrader(data: &Path, exe: &Path) -> Upgrader {
        Upgrader::new(
            UpgradeConfig {
                exe: exe.to_path_buf(),
                auto: None,
                smoke_test: false,
            },
            data,
        )
        .unwrap()
    }

    fn staged(data: &Path, bytes: &[u8]) -> Staged {
        let key = iroh::SecretKey::from_bytes(&[3; 32]);
        let path = data.join("upgrade").join("x.download");
        std::fs::write(&path, bytes).unwrap();
        Staged {
            path,
            release: SignedRelease::sign(
                Release {
                    cluster: *key.public().as_bytes(),
                    file: ReleaseFile {
                        target: Target::host(),
                        version: "9.9.9".into(),
                        protocol: pumasi_proto::PROTOCOL_VERSION,
                        sha256: sha256(bytes),
                        size: bytes.len() as u64,
                    },
                    created_unix: 0,
                },
                &key,
            ),
        }
    }

    #[tokio::test]
    async fn installs_commits_and_keeps_a_backup() {
        let (data, exe) = setup("ok");
        let up = upgrader(&data, &exe);
        assert_eq!(up.startup().unwrap(), Startup::Normal);
        let version = up.install(staged(&data, b"new binary")).await.unwrap();
        assert_eq!(version, "9.9.9");
        assert_eq!(std::fs::read(&exe).unwrap(), b"new binary");
        assert_eq!(
            std::fs::read(data.join("upgrade").join(previous_name())).unwrap(),
            b"old binary"
        );

        // The restarted process is on trial until welcomed.
        let up = upgrader(&data, &exe);
        assert_eq!(up.startup().unwrap(), Startup::Trial);
        up.commit();
        let up = upgrader(&data, &exe);
        assert_eq!(up.startup().unwrap(), Startup::Normal);
        // Nothing but the binary itself is left next to it.
        let names: Vec<_> = std::fs::read_dir(exe.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
    }

    #[tokio::test]
    async fn rolls_back_when_rejected_and_remembers_the_release() {
        let (data, exe) = setup("reject");
        let up = upgrader(&data, &exe);
        up.install(staged(&data, b"broken binary")).await.unwrap();

        let up = upgrader(&data, &exe);
        assert_eq!(up.startup().unwrap(), Startup::Trial);
        up.rollback("cluster rejected this node").unwrap();
        assert_eq!(std::fs::read(&exe).unwrap(), b"old binary");
        assert_eq!(
            up.failed_before(&sha256(b"broken binary")).as_deref(),
            Some("cluster rejected this node")
        );

        // Back on the old binary: no trial.
        let up = upgrader(&data, &exe);
        assert_eq!(up.startup().unwrap(), Startup::Normal);
    }

    #[tokio::test]
    async fn rolls_back_after_a_crash_loop() {
        let (data, exe) = setup("loop");
        upgrader(&data, &exe)
            .install(staged(&data, b"crashy binary"))
            .await
            .unwrap();
        for _ in 0..MAX_TRIAL_BOOTS {
            assert_eq!(upgrader(&data, &exe).startup().unwrap(), Startup::Trial);
        }
        assert!(matches!(
            upgrader(&data, &exe).startup().unwrap(),
            Startup::RolledBack(_)
        ));
        assert_eq!(std::fs::read(&exe).unwrap(), b"old binary");
        assert!(
            upgrader(&data, &exe)
                .failed_before(&sha256(b"crashy binary"))
                .is_some()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn smoke_test_rejects_a_binary_that_does_not_start() {
        use std::os::unix::fs::PermissionsExt;
        let (data, exe) = setup("smoke");
        let script = data.join("fake");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(&script, "#!/bin/sh\necho pumasi 1.0.0\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        smoke_test(&script, "1.0.0").await.unwrap();
        assert!(smoke_test(&script, "2.0.0").await.is_err());
        std::fs::write(&script, "#!/bin/sh\nexit 3\n").unwrap();
        assert!(smoke_test(&script, "1.0.0").await.is_err());
        let _ = exe;
    }
}
