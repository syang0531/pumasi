//! The control plane's side of its own upgrade (1.13): keeping the running
//! binary to roll back to, the trial after restarting into a new one, and
//! the rollback. The records are `pumasi_cluster::own`; when to restart is
//! `Cluster::may_restart`.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use pumasi_cluster::{
    Cluster,
    own::{HEALTHY_FOR, OwnTrial, Pending, Startup, trial_timeout},
};
use pumasi_proto::release::{hex, sha256_file};

/// The control plane's binary and the records of its upgrades.
pub struct Own {
    exe: PathBuf,
    /// The running binary's SHA-256, computed in the background at start.
    sha: Arc<tokio::sync::OnceCell<[u8; 32]>>,
    trial: OwnTrial,
    /// The upgrade this start is the trial of.
    on_trial: Option<Pending>,
    home: PathBuf,
}

/// What a control plane does after looking at its upgrade records.
pub enum Begin {
    Run(Own),
    /// Too many starts of a new binary: the previous one is back; restart
    /// into it.
    Restart,
}

/// The running image: on Linux the process's own, which stays readable
/// after the file at `exe` is replaced.
fn running_image(exe: &Path) -> PathBuf {
    if cfg!(target_os = "linux") {
        PathBuf::from("/proc/self/exe")
    } else {
        exe.to_path_buf()
    }
}

async fn hash(path: PathBuf) -> Result<[u8; 32]> {
    tokio::task::spawn_blocking(move || sha256_file(&path))
        .await
        .context("hash task")?
}

impl Own {
    async fn sha(&self) -> Result<[u8; 32]> {
        let exe = self.exe.clone();
        self.sha
            .get_or_try_init(|| hash(running_image(&exe)))
            .await
            .copied()
    }

    pub fn on_trial(&self) -> bool {
        self.on_trial.is_some()
    }

    /// Makes sure `previous` holds the running binary, so a new one can be
    /// rolled back. Returns whether it does.
    pub async fn keep_previous(&self) -> bool {
        match self.try_keep_previous().await {
            Ok(()) => true,
            Err(e) => {
                eprintln!("cannot keep the running control plane binary for a rollback: {e:#}");
                false
            }
        }
    }

    async fn try_keep_previous(&self) -> Result<()> {
        let own = self.sha().await?;
        let previous = self.trial.previous_path();
        if previous.exists() && hash(previous.clone()).await.ok() == Some(own) {
            return Ok(());
        }
        std::fs::create_dir_all(self.trial.dir())?;
        // The running binary: still at its path unless something replaced
        // it already, in which case the agent's own backup or (on Linux)
        // the running image has it.
        let mut old = self.exe.clone().into_os_string();
        old.push(".old");
        let mut candidates = vec![running_image(&self.exe), self.exe.clone(), old.into()];
        for name in ["previous", "previous.exe"] {
            candidates.push(self.home.join("agent").join("upgrade").join(name));
        }
        for c in candidates {
            if !c.exists() || hash(c.clone()).await.ok() != Some(own) {
                continue;
            }
            let tmp = previous.with_extension("tmp");
            std::fs::copy(&c, &tmp)
                .with_context(|| format!("copy {} to {}", c.display(), tmp.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
            }
            std::fs::rename(&tmp, &previous)?;
            return Ok(());
        }
        anyhow::bail!(
            "no copy of the running binary ({}) is left",
            &hex(&own)[..12]
        )
    }

    /// Before restarting into the binary now at `exe`: records the trial,
    /// if the running binary could be kept.
    pub async fn prepare_restart(&self, version: &str) {
        if !self.keep_previous().await {
            eprintln!("restarting into pumasi {version} without a rollback");
            return;
        }
        let res: Result<()> = async {
            let own = self.sha().await?;
            let new = hash(self.exe.clone()).await?;
            self.trial
                .begin(&own, pumasi_proto::version(), &new, version)
        }
        .await;
        if let Err(e) = res {
            eprintln!("could not record the upgrade's trial: {e:#}");
        }
    }

    /// Puts `previous` back over the binary and remembers why.
    pub async fn roll_back(&self, reason: &str) -> Result<()> {
        let pending = self
            .on_trial
            .clone()
            .or_else(|| self.trial.pending())
            .context("no upgrade on trial")?;
        roll_back(&self.trial, &self.exe, &pending, reason).await
    }

    /// While on trial: commits once this control plane has been healthy
    /// for a while, or resolves with why it failed (not healthy within the
    /// trial's time). Never resolves otherwise.
    pub async fn watch(&self, cluster: &Cluster) -> String {
        let Some(p) = &self.on_trial else {
            return std::future::pending().await;
        };
        let timeout = trial_timeout();
        let deadline = Instant::now() + timeout;
        let mut healthy_since: Option<Instant> = None;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if cluster.healthy() {
                let since = *healthy_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= HEALTHY_FOR {
                    self.trial.commit();
                    eprintln!("pumasi {} works; the upgrade is complete", p.version);
                    return std::future::pending().await;
                }
            } else {
                healthy_since = None;
            }
            if Instant::now() >= deadline {
                return format!(
                    "pumasi {} did not become healthy within {}s (as the leader, or as a follower caught up with one)",
                    p.version,
                    timeout.as_secs()
                );
            }
        }
    }
}

async fn roll_back(trial: &OwnTrial, exe: &Path, pending: &Pending, reason: &str) -> Result<()> {
    let previous = trial.previous_path();
    let sha = hash(previous.clone())
        .await
        .with_context(|| format!("no previous binary at {}", previous.display()))?;
    eprintln!(
        "rolling the control plane back to pumasi {}: {reason}",
        pending.from_version
    );
    pumasi_agent::upgrade::replace_binary(&previous, sha, &pending.from_version, exe, false)
        .await
        .context("put the previous binary back")?;
    trial.record_failure(pending, reason);
    Ok(())
}

/// At a control plane's start, before the cluster starts: counts a start
/// of a new binary on trial, and rolls back after too many.
pub async fn startup(home: &Path, data_dir: &Path, exe: &Path) -> Result<Begin> {
    let trial = OwnTrial::new(data_dir);
    let sha: Arc<tokio::sync::OnceCell<[u8; 32]>> = Default::default();
    let mut own = Own {
        exe: exe.to_path_buf(),
        sha: sha.clone(),
        trial: trial.clone(),
        on_trial: None,
        home: home.to_path_buf(),
    };
    if trial.pending().is_some() {
        let s = own.sha().await?;
        match trial.startup(&s)? {
            Startup::Normal => {}
            Startup::Trial(p) => {
                eprintln!(
                    "running pumasi {} on trial (start {} of {}); it rolls back to {} unless it becomes healthy",
                    p.version,
                    p.boots,
                    pumasi_cluster::own::MAX_TRIAL_BOOTS,
                    p.from_version
                );
                own.on_trial = Some(p);
            }
            Startup::RollBack { pending, reason } => {
                roll_back(&trial, exe, &pending, &reason).await?;
                return Ok(Begin::Restart);
            }
        }
    } else {
        // Hash in the background: needed only once there is something to
        // roll back to.
        let image = running_image(exe);
        tokio::spawn(async move {
            let _ = sha.get_or_try_init(|| hash(image)).await;
        });
    }
    // Test hook: a binary that dies right at start, after the count above.
    if trial
        .exit_at_start()
        .is_some_and(|v| v == pumasi_proto::version())
    {
        eprintln!(
            "exiting at start ({})",
            pumasi_cluster::own::EXIT_AT_START_FILE
        );
        std::process::exit(3);
    }
    Ok(Begin::Run(own))
}
