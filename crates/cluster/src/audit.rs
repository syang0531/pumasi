//! The audit log (phase 17): every call that changes the cluster, every
//! refused call, sign-ins and nodes joining, as a hash chain in the state
//! database (table `audit`). Entries are written through the store like
//! everything else, so control plane replicas hold the same chain and a
//! new leader continues it. See [`pumasi_proto::audit`] for the format.

use anyhow::{Result, ensure};
use pumasi_proto::{
    audit::{AuditEntry, AuditHead, GENESIS},
    job::Executor,
    msg::{AdminOp, OWNER_ACCOUNT},
    store::StoreTarget,
};
use tracing::warn;

use crate::{
    manage::Caller,
    state::{State, now_unix},
};

/// Longest detail kept in an entry.
const MAX_DETAIL: usize = 1024;

/// Where the chain stands.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuditTip {
    pub next_seq: u64,
    pub head: [u8; 32],
}

impl Default for AuditTip {
    fn default() -> Self {
        Self {
            next_seq: 1,
            head: GENESIS,
        }
    }
}

/// Who did something, for an entry.
#[derive(Debug, Clone)]
pub struct Actor {
    pub name: String,
    pub role: String,
    pub peer: String,
}

impl Actor {
    pub fn caller(c: &Caller, peer: &str) -> Self {
        Self {
            name: c.name().to_string(),
            role: c.role().to_string(),
            peer: peer.to_string(),
        }
    }

    pub fn system() -> Self {
        Self {
            name: "control plane".into(),
            role: "system".into(),
            peer: String::new(),
        }
    }

    pub fn anonymous(peer: &str) -> Self {
        Self {
            name: "anonymous".into(),
            role: "anonymous".into(),
            peer: peer.to_string(),
        }
    }
}

fn clip(mut s: String) -> String {
    if s.len() > MAX_DETAIL {
        let mut cut = MAX_DETAIL;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
        s.push('…');
    }
    s
}

impl State {
    /// Picks up the chain where the database leaves it.
    pub(crate) fn load_audit(&mut self) -> Result<()> {
        self.audit_tip = match self.store.last_audit()? {
            Some(e) => AuditTip {
                next_seq: e.seq + 1,
                head: e.hash,
            },
            None => AuditTip::default(),
        };
        Ok(())
    }

    /// Appends an entry. A failure to store it is logged, not returned:
    /// the call it records has already happened.
    pub(crate) fn audit(
        &mut self,
        who: &Actor,
        action: &str,
        target: impl Into<String>,
        detail: impl Into<String>,
        ok: bool,
    ) {
        let e = AuditEntry {
            seq: self.audit_tip.next_seq,
            unix: now_unix(),
            actor: who.name.clone(),
            role: who.role.clone(),
            peer: who.peer.clone(),
            action: action.to_string(),
            target: clip(target.into()),
            detail: clip(detail.into()),
            ok,
            prev: GENESIS,
            hash: GENESIS,
        }
        .seal(self.audit_tip.head);
        match self.store.put_audit(&e) {
            Ok(()) => {
                self.audit_tip = AuditTip {
                    next_seq: e.seq + 1,
                    head: e.hash,
                };
            }
            Err(err) => warn!("could not write audit entry {}: {err:#}", e.line()),
        }
    }

    pub fn audit_entries(&self, after: u64, limit: u32) -> Result<Vec<AuditEntry>> {
        self.store
            .audit(after, limit.clamp(1, 10_000) as usize)
    }

    /// Checks the whole chain from the first entry and signs its head.
    pub fn audit_verify(&self) -> Result<AuditHead> {
        let mut prev = GENESIS;
        let mut next = 1u64;
        self.store.each_audit(|e| {
            pumasi_proto::audit::verify_chain(prev, next, std::slice::from_ref(&e))?;
            prev = e.hash;
            next += 1;
            Ok(())
        })?;
        let seq = next - 1;
        ensure!(
            seq + 1 == self.audit_tip.next_seq && prev == self.audit_tip.head,
            "the audit log in the database ends at #{seq}, but this control plane wrote up to #{} (it was changed underneath)",
            self.audit_tip.next_seq - 1
        );
        let unix = now_unix();
        let signature = self
            .key
            .sign(&AuditHead::signed_bytes(seq, &prev, unix))
            .to_bytes()
            .to_vec();
        Ok(AuditHead {
            cluster: self.key.public().to_string(),
            entries: seq,
            seq,
            hash: prev,
            unix,
            signature,
        })
    }
}

/// The audit action, target and detail of an admin call, or `None` for
/// calls that only read (and are not about secrets or backups).
pub fn describe(op: &AdminOp) -> Option<(&'static str, String, String)> {
    use AdminOp::*;
    let none = String::new;
    Some(match op {
        CreateJoinToken {
            ttl_secs,
            uses,
            invite: Some(i),
        } => (
            "invite.create",
            i.account.clone(),
            format!(
                "ttl {ttl_secs}s, uses {}, docker {}",
                uses.map_or("unlimited".into(), |u| u.to_string()),
                i.allow_docker
            ),
        ),
        CreateJoinToken { ttl_secs, uses, .. } => (
            "token.create",
            none(),
            format!(
                "ttl {ttl_secs}s, uses {}",
                uses.map_or("unlimited".into(), |u| u.to_string())
            ),
        ),
        Submit(sub) => {
            let mut d = format!(
                "{} task(s), executor {}, tier {:?}",
                sub.tasks.len(),
                match sub.executor {
                    Executor::Wasm => "wasm",
                    Executor::Docker => "docker",
                    Executor::Microvm => "microvm",
                },
                sub.tier
            );
            if let Some(img) = &sub.docker {
                d.push_str(&format!(", image {}", img.image));
                if img.network {
                    d.push_str(", network");
                }
            }
            if let Some(g) = &sub.gpu {
                d.push_str(&format!(", {} GPU(s) per task", g.count));
            }
            (
                "job.submit",
                format!(
                    "{} (account {})",
                    sub.name,
                    sub.account.as_deref().unwrap_or(OWNER_ACCOUNT)
                ),
                d,
            )
        }
        Cancel { id } => ("job.cancel", id.to_string(), none()),
        RemoveNode { id } => ("node.remove", id.clone(), none()),
        Deposit {
            account,
            amount,
            memo,
        } => (
            "cu.deposit",
            account.clone(),
            format!("{} milli-CU: {memo}", amount),
        ),
        Payout {
            account,
            amount,
            memo,
        } => (
            "cu.payout",
            account.clone(),
            format!("{} milli-CU: {memo}", amount),
        ),
        Benchmark { id } => ("node.benchmark", id.clone(), none()),
        SetPolicy(_) => ("prices.set", none(), none()),
        // Chunks of a binary; the rollout that uses it is logged.
        UploadRelease { .. } => return None,
        Rollout { files, force } => (
            "rollout.start",
            none(),
            format!(
                "{} file(s){}: {}",
                files.len(),
                if *force { ", forced" } else { "" },
                files
                    .iter()
                    .map(|f| pumasi_proto::short_hex(&f.sha256))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
        CancelRollout => ("rollout.cancel", none(), none()),
        StoreCommit { name, account, .. } => (
            "store.commit",
            name.clone().unwrap_or_else(|| "(job input)".into()),
            format!("account {}", account.as_deref().unwrap_or(OWNER_ACCOUNT)),
        ),
        StoreRemove { name, version } => (
            "store.remove",
            match version {
                Some(v) => format!("{name}@{v}"),
                None => name.clone(),
            },
            none(),
        ),
        StoreOpen { target, .. } => (
            "store.download",
            match target {
                StoreTarget::Dataset { name, version } => match version {
                    Some(v) => format!("dataset {name}@{v}"),
                    None => format!("dataset {name}"),
                },
                StoreTarget::Job { id } => format!("job {id}"),
            },
            none(),
        ),
        StoreQuota { account, bytes } => (
            "store.quota",
            account.clone(),
            bytes.map_or("none".into(), |b| format!("{b} bytes")),
        ),
        SecretSet { name, .. } => ("secret.set", name.clone(), none()),
        SecretRemove { name } => ("secret.remove", name.clone(), none()),
        SecretList => ("secret.list", none(), none()),
        SetStoragePolicy(_) => ("storage-prices.set", none(), none()),
        EditNode { node, edit } => ("node.edit", node.clone(), format!("{edit:?}")),
        Drain { node } => ("node.drain", node.clone(), none()),
        SetQuota { account, quota } => ("quota.set", account.clone(), format!("{quota:?}")),
        CreateAccess {
            name,
            role,
            ttl_secs,
        } => (
            "access.create",
            name.clone(),
            format!(
                "role {role}, {}",
                ttl_secs.map_or("no expiry".into(), |t| format!("ttl {t}s"))
            ),
        ),
        RevokeAccess { id } => ("access.revoke", id.clone(), none()),
        SetSchedule(spec) => (
            "schedule.set",
            spec.name.clone(),
            format!("cron {:?}, job {}", spec.cron, spec.job.name),
        ),
        RemoveSchedule { name } => ("schedule.remove", name.clone(), none()),
        RunSchedule { name } => ("schedule.run", name.clone(), none()),
        Apply(spec) => ("apply", none(), format!("dry run {}", spec.dry_run)),
        SetAlertPolicy(_) => ("alerts.set", none(), none()),
        // A webhook URL is a secret of its own (Slack's are): host only.
        SetWebhook(w) => (
            "webhook.set",
            w.name.clone(),
            w.url
                .split("://")
                .nth(1)
                .and_then(|r| r.split(['/', '?']).next())
                .unwrap_or("")
                .rsplit('@')
                .next()
                .unwrap_or("")
                .to_string(),
        ),
        RemoveWebhook { name } => ("webhook.remove", name.clone(), none()),
        TestWebhook { name } => ("webhook.test", name.clone(), none()),
        HaInvite { ttl_secs } => ("ha.invite", none(), format!("ttl {ttl_secs}s")),
        HaRemove { id } => ("ha.remove", id.clone(), none()),
        // A backup holds every secret: log the first chunk.
        Backup { offset: 0 } => ("backup", none(), none()),
        Backup { .. } => return None,
        SetAccountPolicy { account, policy } => {
            ("account-policy.set", account.clone(), format!("{policy:?}"))
        }
        TrustMeasurement { id, trusted, note } => (
            if *trusted {
                "measurement.trust"
            } else {
                "measurement.untrust"
            },
            id.clone(),
            note.clone(),
        ),
        TrustNode { node, trusted } => (
            if *trusted { "node.trust" } else { "node.untrust" },
            node.clone(),
            none(),
        ),
        RotateAdminSecret => ("admin.rotate", none(), none()),
        SsoSet(u) => {
            use pumasi_proto::sso::SsoUpdate::*;
            match &**u {
                Oidc(Some(o)) => (
                    "sso.oidc",
                    o.issuer.clone(),
                    format!("client {}, groups claim {}", o.client_id, o.groups_claim),
                ),
                Oidc(None) => ("sso.oidc", none(), "off".into()),
                Ldap(Some(l)) => (
                    "sso.ldap",
                    l.url.clone(),
                    format!("user base {}", l.user_base),
                ),
                Ldap(None) => ("sso.ldap", none(), "off".into()),
                Map(m) => ("sso.map", m.subject.to_string(), m.role.to_string()),
                Unmap(s) => ("sso.unmap", s.to_string(), none()),
                SessionHours(h) => ("sso.session", none(), format!("{h} hours")),
            }
        }
        // Reads.
        Status
        | Job { .. }
        | TaskFiles { .. }
        | Receipts { .. }
        | Accounts
        | Ledger { .. }
        | GetPolicy
        | RolloutStatus
        | StoreUpload { .. }
        | StoreList { .. }
        | StoreUsage
        | GetStoragePolicy
        | Quotas
        | ListAccess
        | WhoAmI
        | Schedules
        | Metrics { .. }
        | Events { .. }
        | Logs { .. }
        | Alerts
        | HaStatus
        | Confidential
        | Audit { .. }
        | AuditVerify
        | SsoGet => return None,
        // Sign-ins log themselves, with who it turned out to be.
        SsoInfo
        | SsoStart { .. }
        | SsoFinish { .. }
        | SsoDeviceStart
        | SsoDevicePoll { .. }
        | LdapLogin { .. } => return None,
    })
}

/// The name of a refused call, for its entry (any call, reads too).
pub fn refused_action(op: &AdminOp) -> String {
    match describe(op) {
        Some((a, _, _)) => a.to_string(),
        None if matches!(op, AdminOp::Audit { .. } | AdminOp::AuditVerify) => "audit.read".into(),
        None if matches!(op, AdminOp::SsoGet) => "sso.read".into(),
        None => {
            let dbg = format!("{op:?}");
            let name: String = dbg
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            format!("call.{name}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_urls_are_not_logged() {
        let op = AdminOp::SetWebhook(Box::new(pumasi_proto::monitor::WebhookSpec {
            name: "ops".into(),
            url: "https://user:pw@hooks.slack.com/services/T000/B000/XXXX?x=1".into(),
            format: pumasi_proto::monitor::WebhookFormat::Slack,
        }));
        let (a, t, d) = describe(&op).unwrap();
        assert_eq!((a, t.as_str(), d.as_str()), ("webhook.set", "ops", "hooks.slack.com"));
    }

    #[test]
    fn reads_are_not_logged_but_refusals_are_named() {
        assert!(describe(&AdminOp::Status).is_none());
        assert_eq!(refused_action(&AdminOp::Status), "call.Status");
        assert_eq!(
            refused_action(&AdminOp::Cancel {
                id: pumasi_proto::job::JobId(1)
            }),
            "job.cancel"
        );
        let secret = AdminOp::SecretSet {
            name: "registry/x".into(),
            value: "hunter2".into(),
        };
        let (_, t, d) = describe(&secret).unwrap();
        assert!(!t.contains("hunter2") && !d.contains("hunter2"));
    }

    #[test]
    fn long_details_are_clipped_on_a_char_boundary() {
        let s = clip("가".repeat(1000));
        assert!(s.len() <= MAX_DETAIL + 3);
        assert!(s.ends_with('…'));
    }
}
