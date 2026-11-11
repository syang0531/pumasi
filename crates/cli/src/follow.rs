//! The control plane and the dashboard follow their binary (1.3).
//!
//! On a machine that runs the control plane, the dashboard and an agent
//! from one `pumasi` binary, a rollout replaces that binary through the
//! agent, but the control plane and the dashboard keep running the old
//! code. They therefore watch the file they started from: once a newer
//! pumasi is installed there (by the agent's upgrade, or by hand with the
//! install script) and it answers `--version`, they stop and start again
//! from it, the same way the agent restarts after an upgrade. The control
//! plane waits until the rollout has settled first, so it does not cut the
//! other nodes off in the middle of theirs.

use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use pumasi_proto::release::{is_older, read_build_info};

/// How often the binary is looked at (a `stat`; it is read only when it
/// changed).
const CHECK_EVERY: Duration = Duration::from_secs(30);
const SMOKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Set in the environment of a process restarted into a new binary.
pub const RESTARTED_ENV: &str = "PUMASI_RESTARTED";

type Stamp = Option<(u64, Option<SystemTime>)>;

fn stamp(p: &Path) -> Stamp {
    std::fs::metadata(p)
        .ok()
        .map(|m| (m.len(), m.modified().ok()))
}

/// Watches the binary this process started from.
pub struct Follow {
    exe: PathBuf,
    seen: Stamp,
}

impl Follow {
    /// `None` when the binary's path is unknown.
    pub fn new() -> Option<Self> {
        let exe = crate::own_exe()?;
        let seen = stamp(&exe);
        Some(Self { exe, seen })
    }

    /// Resolves with the new version once a newer pumasi that starts is
    /// installed where this one was started from.
    pub async fn newer(&mut self) -> String {
        loop {
            tokio::time::sleep(CHECK_EVERY).await;
            if let Some(v) = self.check().await {
                return v;
            }
        }
    }

    async fn check(&mut self) -> Option<String> {
        let now = stamp(&self.exe);
        if now == self.seen || now.is_none() {
            return None;
        }
        self.seen = now;
        let exe = self.exe.clone();
        let info = tokio::task::spawn_blocking(move || {
            std::fs::read(&exe).ok().and_then(|b| read_build_info(&b))
        })
        .await
        .ok()??;
        if !is_older(pumasi_proto::version(), &info.version) {
            return None;
        }
        match smoke_test(&self.exe, &info.version).await {
            Ok(()) => Some(info.version),
            Err(e) => {
                eprintln!(
                    "a newer pumasi ({}) is at {}, but it does not start: {e}; staying on {}",
                    info.version,
                    self.exe.display(),
                    pumasi_proto::version()
                );
                None
            }
        }
    }
}

async fn smoke_test(exe: &Path, version: &str) -> Result<(), String> {
    let out = tokio::time::timeout(
        SMOKE_TIMEOUT,
        tokio::process::Command::new(exe)
            .arg("--version")
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "it did not answer --version in time".to_string())?
    .map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() || !stdout.split_whitespace().any(|w| w == version) {
        return Err(format!(
            "--version said {:?} ({})",
            stdout.trim(),
            out.status
        ));
    }
    Ok(())
}

/// Why a control plane or dashboard stopped.
pub enum Stop {
    Shutdown,
    /// A newer binary (`version`) was installed at `exe`: restart into it.
    Upgrade {
        version: String,
        exe: PathBuf,
    },
}

/// Waits for `shutdown`, or (with `follow`) for a newer binary and then
/// for `ready` to say it is a good moment to restart.
pub async fn until_stop(
    shutdown: impl std::future::Future<Output = ()>,
    follow: Option<Follow>,
    ready: impl Fn() -> bool,
) -> Stop {
    tokio::pin!(shutdown);
    let Some(mut follow) = follow else {
        shutdown.await;
        return Stop::Shutdown;
    };
    let version = tokio::select! {
        _ = &mut shutdown => return Stop::Shutdown,
        v = follow.newer() => v,
    };
    let mut told = false;
    loop {
        if ready() {
            return Stop::Upgrade {
                version,
                exe: follow.exe,
            };
        }
        if !told {
            eprintln!("pumasi {version} is installed; restarting into it once the rollout settles");
            told = true;
        }
        tokio::select! {
            _ = &mut shutdown => return Stop::Shutdown,
            _ = tokio::time::sleep(Duration::from_secs(10)) => {}
        }
    }
}

/// Starts this process again from the new binary at `exe` (the path it
/// started from: the running image may since have been renamed aside):
/// under a service manager by exiting with
/// [`crate::service::RESTART_EXIT_CODE`], else in place.
pub fn restart(in_service: bool, exe: &Path) -> anyhow::Result<i32> {
    if in_service {
        return Ok(crate::service::RESTART_EXIT_CODE);
    }
    // SAFETY: called on the way out, when no other thread reads the
    // environment any more; the restarted process inherits it.
    unsafe { std::env::set_var(RESTARTED_ENV, "1") };
    crate::restart_self(exe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn follows_a_newer_binary_only() {
        let dir = std::env::temp_dir().join(format!("pumasi-follow-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let exe = dir.join("pumasi");
        // Not a pumasi binary: ignored.
        std::fs::write(&exe, b"something").unwrap();
        let mut f = Follow {
            exe: exe.clone(),
            seen: None,
        };
        assert_eq!(f.check().await, None);
        // Unchanged since the last look: not even read.
        assert_eq!(f.check().await, None);
        // The same version as this one: nothing to do.
        let same = format!("xx{}yy", pumasi_proto::BUILD_INFO);
        std::fs::write(&exe, same).unwrap();
        assert_eq!(f.check().await, None);
        // A newer marker in a file that cannot run: refused.
        let newer = format!(
            "pumasi-build-info:v=999.0.0;p={};",
            pumasi_proto::PROTOCOL_VERSION
        );
        std::fs::write(&exe, format!("zz{newer}zz")).unwrap();
        assert_eq!(f.check().await, None);
        let _ = std::fs::remove_dir_all(dir);
    }
}
