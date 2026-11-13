//! Dashboard sessions kept in the control plane's state (1.7).
//!
//! Before 1.7 `pumasi ui` kept sessions in its own memory, so restarting
//! or upgrading it signed everyone out. Now a browser that signs in gets
//! a session from the control plane ([`AdminOp::StartSession`], made with
//! the ticket it signed in with): a new 32-byte secret that the control
//! plane takes in place of that ticket, with its name and role, on every
//! admin call. The dashboard puts the secret in the browser's cookie and
//! keeps nothing it cannot ask for again, so a new `pumasi ui` process
//! (or another one in front of the same cluster) knows the browser.
//!
//! The state holds the hash of the session's secret, never the secret,
//! with the ticket it stands for (whose secret the state holds anyway),
//! the browser's label, and when it was made and last used. A session
//! ends when it goes unused for [`SESSION_IDLE_SECS`], when it is revoked
//! or signs out, or when its ticket is revoked, expires or (for the
//! cluster's admin ticket) is rotated. Sessions are a table of the state,
//! so they reach control plane replicas through Raft like everything
//! else; the last-used time is written at most every few minutes.
//!
//! [`AdminOp::StartSession`]: pumasi_proto::msg::AdminOp::StartSession

use std::collections::HashMap;

use anyhow::{Result, bail, ensure};
use pumasi_proto::{
    Secret, secrets_eq,
    ui::{
        MAX_SESSION_LABEL, MAX_SESSIONS, SESSION_IDLE_SECS, SessionView, session_hash, session_id,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    manage::Caller,
    state::{State, now_unix},
};

/// How stale a session's stored last-used time may get.
const TOUCH_EVERY_SECS: u64 = 300;

/// One session, as stored (postcard, in `ui_sessions`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    /// [`session_hash`] of its secret.
    pub hash: [u8; 32],
    /// The secret of the ticket it signed in with.
    pub parent: Secret,
    pub label: String,
    pub created_unix: u64,
    pub last_used_unix: u64,
}

/// The sessions part of [`State`].
#[derive(Default)]
pub(crate) struct SessionState {
    map: HashMap<[u8; 32], SessionRecord>,
    /// The last-used time each session has on disk.
    stored_use: HashMap<[u8; 32], u64>,
}

/// A session found for a secret: its hash and the ticket it stands for.
pub(crate) struct Found {
    pub hash: [u8; 32],
    pub parent: Secret,
}

impl State {
    pub(crate) fn load_sessions(&mut self) -> Result<()> {
        let now = now_unix();
        let mut gone = Vec::new();
        for r in self.store.ui_sessions()? {
            if now.saturating_sub(r.last_used_unix) >= SESSION_IDLE_SECS {
                gone.push(r.hash);
                continue;
            }
            self.ui.stored_use.insert(r.hash, r.last_used_unix);
            self.ui.map.insert(r.hash, r);
        }
        self.store.put_ui_sessions(&[], &gone)?;
        Ok(())
    }

    /// The session `secret` opens, if any; counts as a use.
    pub(crate) fn find_session(&mut self, secret: &Secret) -> Option<Found> {
        if self.ui.map.is_empty() {
            return None;
        }
        let hash = session_hash(secret);
        let now = now_unix();
        let r = self.ui.map.get_mut(&hash)?;
        if now.saturating_sub(r.last_used_unix) >= SESSION_IDLE_SECS {
            self.end_sessions(&[hash], "idle");
            return None;
        }
        r.last_used_unix = now;
        Some(Found {
            hash,
            parent: r.parent,
        })
    }

    /// Starts a session for the ticket with secret `parent`.
    pub(crate) fn start_session(&mut self, parent: Secret, label: &str) -> Result<(Secret, SessionView)> {
        let label: String = label
            .chars()
            .filter(|c| !c.is_control())
            .take(MAX_SESSION_LABEL)
            .collect();
        // Anyone with a valid ticket can sign in again and again: keep the
        // table bounded by dropping the least recently used.
        if self.ui.map.len() >= MAX_SESSIONS
            && let Some(oldest) = self
                .ui
                .map
                .values()
                .min_by_key(|r| r.last_used_unix)
                .map(|r| r.hash)
        {
            self.end_sessions(&[oldest], "too many sessions");
        }
        let secret = pumasi_proto::new_secret();
        let now = now_unix();
        let r = SessionRecord {
            hash: session_hash(&secret),
            parent,
            label: label.trim().to_string(),
            created_unix: now,
            last_used_unix: now,
        };
        self.store.put_ui_sessions(&[&r], &[])?;
        self.ui.stored_use.insert(r.hash, now);
        let hash = r.hash;
        self.ui.map.insert(hash, r);
        let view = self.session_view(&self.ui.map[&hash], Some(&hash));
        Ok((secret, view))
    }

    fn session_view(&self, r: &SessionRecord, current: Option<&[u8; 32]>) -> SessionView {
        // The cluster's admin ticket is the only parent without an access
        // record.
        let who: Option<Caller> = self.access_caller(&r.parent);
        SessionView {
            id: session_id(&r.hash),
            name: who
                .as_ref()
                .map_or("cluster admin".to_string(), |c| c.name().to_string()),
            role: who.map_or(pumasi_proto::manage::AccessRole::Admin, |c| c.role()),
            label: r.label.clone(),
            created_unix: r.created_unix,
            last_used_unix: r.last_used_unix,
            current: current == Some(&r.hash),
        }
    }

    /// Sessions, newest use first: all of them (`of: None`, for admins),
    /// or those of the ticket with secret `of`.
    pub(crate) fn list_sessions(&self, of: Option<&Secret>, current: Option<&[u8; 32]>) -> Vec<SessionView> {
        let mut v: Vec<&SessionRecord> = self
            .ui
            .map
            .values()
            .filter(|r| of.is_none_or(|p| secrets_eq(p, &r.parent)))
            .collect();
        v.sort_by(|a, b| b.last_used_unix.cmp(&a.last_used_unix).then(a.hash.cmp(&b.hash)));
        v.into_iter()
            .map(|r| self.session_view(r, current))
            .collect()
    }

    /// Revokes the session whose id starts with `id` (one only), of the
    /// ticket `of` unless `of` is `None`. Returns its view.
    pub(crate) fn revoke_session(&mut self, id: &str, of: Option<&Secret>) -> Result<SessionView> {
        let id = id.trim().to_ascii_lowercase();
        ensure!(!id.is_empty(), "which session?");
        let found: Vec<[u8; 32]> = self
            .ui
            .map
            .values()
            .filter(|r| session_id(&r.hash).starts_with(&id))
            .filter(|r| of.is_none_or(|p| secrets_eq(p, &r.parent)))
            .map(|r| r.hash)
            .collect();
        match found.as_slice() {
            [] => bail!("no such session: {id}"),
            [h] => {
                let view = self.session_view(&self.ui.map[h], None);
                self.end_sessions(&[*h], "revoked");
                Ok(view)
            }
            _ => bail!("{id} is the start of {} session ids; give more of it", found.len()),
        }
    }

    pub(crate) fn end_sessions(&mut self, hashes: &[[u8; 32]], why: &str) {
        let mut gone = Vec::new();
        for h in hashes {
            if self.ui.map.remove(h).is_some() {
                info!(session = %session_id(h), "session ended: {why}");
                gone.push(*h);
            }
            self.ui.stored_use.remove(h);
        }
        if let Err(e) = self.store.put_ui_sessions(&[], &gone) {
            warn!("persist sessions: {e:#}");
        }
    }

    /// Writes last-used times that moved by more than a few minutes and
    /// ends idle sessions and those whose ticket is gone (`admin` is the
    /// cluster's admin secret now). Called every few seconds.
    pub(crate) fn tick_sessions(&mut self, admin: &Secret) {
        if self.ui.map.is_empty() {
            return;
        }
        let now = now_unix();
        let mut ended = Vec::new();
        let mut touched = Vec::new();
        for r in self.ui.map.values() {
            let idle = now.saturating_sub(r.last_used_unix) >= SESSION_IDLE_SECS;
            let parent_ok =
                secrets_eq(&r.parent, admin) || self.access_caller(&r.parent).is_some();
            if idle || !parent_ok {
                ended.push(r.hash);
            } else if r.last_used_unix
                >= self.ui.stored_use.get(&r.hash).copied().unwrap_or(0) + TOUCH_EVERY_SECS
            {
                touched.push(r.hash);
            }
        }
        if !ended.is_empty() {
            self.end_sessions(&ended, "idle, or its ticket is gone");
        }
        if !touched.is_empty() {
            let recs: Vec<&SessionRecord> = touched.iter().filter_map(|h| self.ui.map.get(h)).collect();
            match self.store.put_ui_sessions(&recs, &[]) {
                Ok(()) => {
                    for r in recs {
                        self.ui.stored_use.insert(r.hash, r.last_used_unix);
                    }
                }
                Err(e) => warn!("persist sessions: {e:#}"),
            }
        }
    }

    /// Writes every session's last use (at shutdown).
    pub(crate) fn save_sessions(&mut self) {
        let recs: Vec<&SessionRecord> = self
            .ui
            .map
            .values()
            .filter(|r| self.ui.stored_use.get(&r.hash) != Some(&r.last_used_unix))
            .collect();
        if let Err(e) = self.store.put_ui_sessions(&recs, &[]) {
            warn!("persist sessions: {e:#}");
        }
    }
}
