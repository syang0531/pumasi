//! The control plane's own upgrade (1.13): the trial a control plane runs
//! after restarting into a new binary, and what it remembers about it, in
//! `<cluster dir>/upgrade/`.
//!
//! Before a control plane restarts into a new binary it keeps the running
//! one as `previous` and writes `pending.json`. The new binary counts its
//! starts while on trial; it commits (deletes `pending.json`) once it has
//! been healthy for a while: serving as the leader, or following a leader
//! and caught up. A start that fails, a fourth start without a commit (a
//! crash loop under a service manager), or no health within
//! [`trial_timeout`] puts `previous` back (the CLI does the swap), records
//! the release in `failed.json` so it is not installed again, and restarts.
//! The replacing and swapping of binaries is the CLI's; this module only
//! keeps the records.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use pumasi_proto::release::hex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Starts of a new binary that never committed before it rolls back.
pub const MAX_TRIAL_BOOTS: u32 = 3;
/// How long a control plane must stay healthy before its upgrade counts.
pub const HEALTHY_FOR: Duration = Duration::from_secs(10);
const PENDING_FILE: &str = "pending.json";
const FAILED_FILE: &str = "failed.json";
/// Test hook: a control plane whose version is the text of this file exits
/// right after counting its start (a binary that dies at start).
pub const EXIT_AT_START_FILE: &str = "test-exit-at-start";

/// How long a control plane on trial may take to become healthy: 5 minutes,
/// or `PUMASI_CP_TRIAL_SECS`.
pub fn trial_timeout() -> Duration {
    std::env::var("PUMASI_CP_TRIAL_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(5 * 60))
}

/// An upgrade under trial.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub from_sha256: String,
    pub from_version: String,
    pub to_sha256: String,
    pub version: String,
    pub boots: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Failed {
    /// (SHA-256, version, why), oldest first.
    releases: Vec<(String, String, String)>,
}

/// What a control plane found about its own upgrade when it started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
    Normal,
    /// Running a new binary that has not committed yet.
    Trial(Pending),
    /// Too many starts without a commit: put `previous` back.
    RollBack {
        pending: Pending,
        reason: String,
    },
}

/// The records of a control plane's own upgrades.
#[derive(Debug, Clone)]
pub struct OwnTrial {
    dir: PathBuf,
}

impl OwnTrial {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            dir: data_dir.join("upgrade"),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where the binary to roll back to is kept.
    pub fn previous_path(&self) -> PathBuf {
        self.dir.join(if cfg!(windows) {
            "previous.exe"
        } else {
            "previous"
        })
    }

    pub fn pending(&self) -> Option<Pending> {
        let text = std::fs::read_to_string(self.dir.join(PENDING_FILE)).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn save_pending(&self, p: &Pending) -> Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("create {}", self.dir.display()))?;
        write_atomic(&self.dir.join(PENDING_FILE), &serde_json::to_vec_pretty(p)?)
    }

    fn clear_pending(&self) {
        let _ = std::fs::remove_file(self.dir.join(PENDING_FILE));
    }

    /// Records the restart into a new binary; `previous_path` must hold the
    /// running one already.
    pub fn begin(
        &self,
        from_sha256: &[u8; 32],
        from_version: &str,
        to_sha256: &[u8; 32],
        version: &str,
    ) -> Result<()> {
        self.save_pending(&Pending {
            from_sha256: hex(from_sha256),
            from_version: from_version.to_string(),
            to_sha256: hex(to_sha256),
            version: version.to_string(),
            boots: 0,
        })
    }

    /// Checks for an unfinished upgrade of the binary with SHA-256
    /// `own_sha256`, counting this start.
    pub fn startup(&self, own_sha256: &[u8; 32]) -> Result<Startup> {
        let Some(mut p) = self.pending() else {
            return Ok(Startup::Normal);
        };
        if p.to_sha256 != hex(own_sha256) {
            // Rolled back already, or replaced by hand.
            self.clear_pending();
            return Ok(Startup::Normal);
        }
        p.boots += 1;
        if p.boots > MAX_TRIAL_BOOTS {
            let reason = format!(
                "pumasi {} started {MAX_TRIAL_BOOTS} times without becoming healthy",
                p.version
            );
            return Ok(Startup::RollBack { pending: p, reason });
        }
        self.save_pending(&p)?;
        info!(version = %p.version, start = p.boots, "running a new control plane binary on trial");
        Ok(Startup::Trial(p))
    }

    /// The new binary stayed healthy: the upgrade is done.
    pub fn commit(&self) {
        if let Some(p) = self.pending() {
            info!(version = %p.version, "control plane upgrade complete");
            self.clear_pending();
        }
    }

    /// Remembers that the binary of `pending` failed (after `previous` was
    /// put back), so it is not installed again.
    pub fn record_failure(&self, pending: &Pending, reason: &str) {
        warn!(version = %pending.version, "control plane upgrade rolled back: {reason}");
        let mut failed = self.load_failed();
        failed.releases.retain(|(s, _, _)| *s != pending.to_sha256);
        failed.releases.push((
            pending.to_sha256.clone(),
            pending.version.clone(),
            reason.to_string(),
        ));
        let n = failed.releases.len();
        if n > 16 {
            failed.releases.drain(..n - 16);
        }
        match serde_json::to_vec_pretty(&failed) {
            Ok(bytes) => {
                if let Err(e) = std::fs::create_dir_all(&self.dir)
                    .map_err(anyhow::Error::from)
                    .and_then(|_| write_atomic(&self.dir.join(FAILED_FILE), &bytes))
                {
                    warn!("could not record the failed upgrade: {e:#}");
                }
            }
            Err(e) => warn!("could not record the failed upgrade: {e}"),
        }
        self.clear_pending();
    }

    fn load_failed(&self) -> Failed {
        std::fs::read_to_string(self.dir.join(FAILED_FILE))
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default()
    }

    /// Why the binary with this SHA-256 was rolled back, if it was.
    pub fn failed_with(&self, sha256: &[u8; 32]) -> Option<String> {
        let key = hex(sha256);
        self.load_failed()
            .releases
            .into_iter()
            .find(|(s, _, _)| *s == key)
            .map(|(_, _, why)| why)
    }

    /// The latest release this control plane rolled back from: (version,
    /// why).
    pub fn last_failure(&self) -> Option<(String, String)> {
        self.load_failed()
            .releases
            .pop()
            .map(|(_, version, why)| (version, why))
    }

    /// The version the test hook says should die at start, if any.
    pub fn exit_at_start(&self) -> Option<String> {
        std::fs::read_to_string(self.dir.join(EXIT_AT_START_FILE))
            .ok()
            .map(|s| s.trim().to_string())
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename to {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_binary_rolls_back_after_three_starts_without_a_commit() {
        let dir = std::env::temp_dir().join(format!("pumasi-own-{}", rand::random::<u64>()));
        let t = OwnTrial::new(&dir);
        let (old, new) = ([1u8; 32], [2u8; 32]);
        assert_eq!(t.startup(&new).unwrap(), Startup::Normal);
        t.begin(&old, "1.12.0", &new, "1.13.0").unwrap();
        // The old binary starting (rolled back by hand) clears the trial.
        assert_eq!(t.startup(&old).unwrap(), Startup::Normal);
        assert!(t.pending().is_none());

        t.begin(&old, "1.12.0", &new, "1.13.0").unwrap();
        for boot in 1..=MAX_TRIAL_BOOTS {
            match t.startup(&new).unwrap() {
                Startup::Trial(p) => assert_eq!(p.boots, boot),
                other => panic!("start {boot}: {other:?}"),
            }
        }
        let Startup::RollBack { pending, reason } = t.startup(&new).unwrap() else {
            panic!("a fourth start rolls back");
        };
        assert!(reason.contains("3 times"), "{reason}");
        t.record_failure(&pending, &reason);
        assert!(t.pending().is_none());
        assert_eq!(t.failed_with(&new), Some(reason.clone()));
        assert_eq!(t.failed_with(&old), None);
        assert_eq!(t.last_failure(), Some(("1.13.0".into(), reason)));

        // A commit ends a trial.
        t.begin(&old, "1.12.0", &[3; 32], "1.13.1").unwrap();
        assert!(matches!(t.startup(&[3; 32]).unwrap(), Startup::Trial(_)));
        t.commit();
        assert_eq!(t.startup(&[3; 32]).unwrap(), Startup::Normal);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
