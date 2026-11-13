//! What the dashboard of 1.7 (phase 23) asks the control plane for:
//! sign-in sessions that live in the cluster's state, and each node's
//! availability settings, changed from the browser.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::manage::AccessRole;

/// A session unused for this long ends.
pub const SESSION_IDLE_SECS: u64 = 12 * 3600;
/// Sessions the control plane keeps at most; the least recently used
/// goes first.
pub const MAX_SESSIONS: usize = 10_000;
/// The longest label a session keeps.
pub const MAX_SESSION_LABEL: usize = 200;

/// The id of a session: the first bytes of its secret's hash, in hex.
/// Shown and used to revoke it; the secret itself is never stored.
pub fn session_hash(secret: &crate::Secret) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/ui-session/v1\0");
    h.update(secret);
    *h.finalize().as_bytes()
}

pub fn session_id(hash: &[u8; 32]) -> String {
    data_encoding::HEXLOWER.encode(&hash[..8])
}

/// A browser signed in to `pumasi ui` (1.7). The session's own secret
/// stands in for the ticket the browser signed in with, with the same
/// name and role, until it ends, is revoked, or that ticket is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionView {
    pub id: String,
    /// The name of the ticket it signed in with (`cluster admin` for the
    /// cluster's own).
    pub name: String,
    pub role: AccessRole,
    /// The browser and address, as the dashboard saw them.
    pub label: String,
    pub created_unix: u64,
    pub last_used_unix: u64,
    /// The session making the call.
    pub current: bool,
}

/// When a node takes tasks: the options of `pumasi agent start` that the
/// dashboard edits (1.7). The agent writes them to its service's options
/// file (`agent.toml`) and restarts with them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSettings {
    /// Tasks at once (`--slots`); `None` for the default.
    pub slots: Option<u32>,
    /// Share of the CPUs offered, 1 to 100 (`--max-cpu-percent`).
    pub max_cpu_percent: Option<u32>,
    /// Only on AC power (`--only-on-ac`).
    pub only_on_ac: bool,
    /// Only after this many idle minutes (`--idle-minutes`).
    pub idle_minutes: Option<u64>,
    /// Local hours, `22:00-07:00,12:00-13:00` (`--hours`); empty for any
    /// time.
    pub hours: String,
}

impl NodeSettings {
    pub fn validate(&self) -> Result<()> {
        if let Some(s) = self.slots {
            ensure!((1..=1024).contains(&s), "slots must be 1 to 1024");
        }
        if let Some(p) = self.max_cpu_percent {
            ensure!((1..=100).contains(&p), "the CPU share must be 1 to 100 percent");
        }
        if let Some(m) = self.idle_minutes {
            ensure!(
                (1..=24 * 60).contains(&m),
                "idle minutes must be 1 to 1440"
            );
        }
        check_hours(&self.hours)?;
        Ok(())
    }

    /// The options that set these, for `pumasi service set agent --`:
    /// `--NAME VALUE` for what is set and `--no-NAME` for what is not.
    pub fn changes(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut opt = |name: &str, v: Option<String>| match v {
            Some(v) => {
                out.push(format!("--{name}"));
                out.push(v);
            }
            None => out.push(format!("--no-{name}")),
        };
        opt("slots", self.slots.map(|v| v.to_string()));
        opt("max-cpu-percent", self.max_cpu_percent.map(|v| v.to_string()));
        opt("idle-minutes", self.idle_minutes.map(|v| v.to_string()));
        let hours = self.hours.trim();
        opt("hours", (!hours.is_empty()).then(|| hours.to_string()));
        out.push(if self.only_on_ac {
            "--only-on-ac".into()
        } else {
            "--no-only-on-ac".into()
        });
        out
    }

    /// One line: `2 slots · 50% CPU · AC only · idle 10 min · 22:00-07:00`.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        parts.push(match self.slots {
            Some(s) => format!("{s} slots"),
            None => "default slots".into(),
        });
        if let Some(p) = self.max_cpu_percent {
            parts.push(format!("{p}% CPU"));
        }
        if self.only_on_ac {
            parts.push("AC only".into());
        }
        if let Some(m) = self.idle_minutes {
            parts.push(format!("idle {m} min"));
        }
        if !self.hours.trim().is_empty() {
            parts.push(self.hours.trim().to_string());
        }
        parts.join(" · ")
    }
}

/// `HH:MM-HH:MM[,...]`, as `--hours` takes it.
pub fn check_hours(s: &str) -> Result<()> {
    let time = |t: &str| -> Result<u32> {
        let (h, m) = t
            .trim()
            .split_once(':')
            .with_context(|| format!("{t:?} is not HH:MM"))?;
        let h: u32 = h.trim().parse().with_context(|| format!("hour in {t:?}"))?;
        let m: u32 = m.trim().parse().with_context(|| format!("minute in {t:?}"))?;
        ensure!(
            m < 60 && (h < 24 || (h == 24 && m == 0)),
            "{t:?} is not a time of day"
        );
        Ok(h * 60 + m)
    };
    if s.trim().is_empty() {
        return Ok(());
    }
    for range in s.split(',') {
        let (a, b) = range
            .split_once('-')
            .with_context(|| format!("{range:?} is not HH:MM-HH:MM"))?;
        let (a, b) = (time(a)?, time(b)?);
        ensure!(a != b, "{range:?} is empty");
        ensure!(a < 24 * 60, "{range:?} starts at the end of the day");
    }
    Ok(())
}

/// What an agent of 1.7 says about its settings: after it connects, and
/// in answer to a change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingsReport {
    pub settings: NodeSettings,
    /// The options file it keeps them in, when it can change them (it
    /// runs as a service of 1.4 or later); `None` when it was started by
    /// hand, with its options on the command line.
    pub file: Option<String>,
    /// A change is written and the agent restarts with it once its
    /// running tasks end; it takes no new tasks meanwhile.
    pub restarting: bool,
}

/// A node's settings as the control plane knows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSettingsView {
    pub id: String,
    pub name: String,
    pub online: bool,
    /// `None` for an agent older than 1.7 or one that has not connected
    /// since the control plane started.
    pub report: Option<SettingsReport>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_become_options() {
        let s = NodeSettings {
            slots: Some(2),
            max_cpu_percent: None,
            only_on_ac: true,
            idle_minutes: Some(10),
            hours: "22:00-07:00".into(),
        };
        s.validate().unwrap();
        assert_eq!(
            s.changes(),
            [
                "--slots",
                "2",
                "--no-max-cpu-percent",
                "--idle-minutes",
                "10",
                "--hours",
                "22:00-07:00",
                "--only-on-ac"
            ]
        );
        assert_eq!(s.describe(), "2 slots · AC only · idle 10 min · 22:00-07:00");
        let d = NodeSettings::default();
        assert!(d.changes().contains(&"--no-hours".to_string()));
        assert!(d.changes().contains(&"--no-only-on-ac".to_string()));
    }

    #[test]
    fn bad_settings_are_refused() {
        for bad in [
            NodeSettings {
                slots: Some(0),
                ..Default::default()
            },
            NodeSettings {
                max_cpu_percent: Some(101),
                ..Default::default()
            },
            NodeSettings {
                hours: "25:00-07:00".into(),
                ..Default::default()
            },
            NodeSettings {
                hours: "08:00-08:00".into(),
                ..Default::default()
            },
            NodeSettings {
                hours: "8am".into(),
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        check_hours("00:00-08:00, 12:00-13:00").unwrap();
    }

    #[test]
    fn session_ids_are_stable_and_short() {
        let h = session_hash(&[7; 32]);
        assert_eq!(h, session_hash(&[7; 32]));
        assert_ne!(h, session_hash(&[8; 32]));
        assert_eq!(session_id(&h).len(), 16);
    }
}
