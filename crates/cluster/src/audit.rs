//! The audit log (phase 17): every call that changes the cluster, every
//! refused call, sign-ins and nodes joining, as a hash chain in the state
//! database (table `audit`). Entries are written through the store like
//! everything else, so control plane replicas hold the same chain and a
//! new leader continues it. See [`pumasi_proto::audit`] for the format.

use anyhow::{Result, ensure};
use pumasi_proto::{
    audit::{AuditCheckpoint, AuditEntry, AuditHead, GENESIS, rfc3339},
    job::Executor,
    msg::{AdminOp, OWNER_ACCOUNT},
    store::StoreTarget,
};
use tracing::warn;

use crate::{
    manage::Caller,
    state::{State, now_unix},
};

/// Ends a walk over the audit log early.
#[derive(Debug)]
struct StopWalk;

impl std::fmt::Display for StopWalk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stop")
    }
}

impl std::error::Error for StopWalk {}

/// Longest detail kept in an entry.
const MAX_DETAIL: usize = 1024;

/// Where the chain stands.
#[derive(Debug, Clone)]
pub(crate) struct AuditTip {
    pub next_seq: u64,
    pub head: [u8; 32],
    /// Where a pruned log starts (1.12).
    pub checkpoint: Option<AuditCheckpoint>,
}

impl Default for AuditTip {
    fn default() -> Self {
        Self {
            next_seq: 1,
            head: GENESIS,
            checkpoint: None,
        }
    }
}

impl AuditTip {
    /// The last pruned entry (0: none).
    pub fn floor(&self) -> u64 {
        self.checkpoint.as_ref().map_or(0, |c| c.seq)
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
        let checkpoint = self.store.audit_checkpoint()?;
        self.audit_tip = match self.store.last_audit()? {
            Some(e) if checkpoint.as_ref().is_none_or(|c| e.seq > c.seq) => AuditTip {
                next_seq: e.seq + 1,
                head: e.hash,
                checkpoint,
            },
            // Everything up to the checkpoint is gone (or about to be).
            _ => match checkpoint {
                Some(c) => AuditTip {
                    next_seq: c.seq + 1,
                    head: c.hash,
                    checkpoint: Some(c),
                },
                None => AuditTip::default(),
            },
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
                self.audit_tip.next_seq = e.seq + 1;
                self.audit_tip.head = e.hash;
            }
            Err(err) => warn!("could not write audit entry {}: {err:#}", e.line()),
        }
    }

    pub fn audit_entries(&self, after: u64, limit: u32) -> Result<Vec<AuditEntry>> {
        self.store.audit(
            after,
            limit.clamp(1, 10_000) as usize,
            self.audit_tip.floor(),
        )
    }

    /// Up to `limit` entries from `from` on.
    pub fn audit_from(&self, from: u64, limit: u32) -> Result<Vec<AuditEntry>> {
        // `after` counts from the entry before; `after: 0` would mean the
        // newest, so the floor (at least 0) keeps it a range.
        let floor = self.audit_tip.floor();
        let after = from.saturating_sub(1).max(floor);
        if after == 0 {
            let mut out = Vec::new();
            self.store
                .each_audit_limited(0, limit.clamp(1, 10_000) as usize, &mut out)?;
            return Ok(out);
        }
        self.store
            .audit(after, limit.clamp(1, 10_000) as usize, floor)
    }

    pub fn audit_checkpoint(&self) -> Option<AuditCheckpoint> {
        self.audit_tip.checkpoint.clone()
    }

    /// Checks the whole chain from the first entry (after a prune, from
    /// its checkpoint, whose signature is checked too) and signs its head.
    pub fn audit_verify(&self) -> Result<AuditHead> {
        let (mut prev, mut next) = match &self.audit_tip.checkpoint {
            Some(c) => {
                c.verify()?;
                ensure!(
                    c.cluster == self.key.public().to_string(),
                    "the audit checkpoint is signed by another key"
                );
                (c.hash, c.seq + 1)
            }
            None => (GENESIS, 1u64),
        };
        let start = next;
        self.store.each_audit(start - 1, |e| {
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
            entries: next - start,
            seq,
            hash: prev,
            unix,
            signature,
        })
    }

    /// Prunes the audit log up to and including entry `through` (1.12):
    /// checks the whole chain, signs a checkpoint at `through` (whose hash
    /// must be `expect`, when given), removes the entries and logs the
    /// prune as the next entry. Returns the checkpoint and how many
    /// entries went.
    pub fn audit_prune(
        &mut self,
        through: u64,
        expect: Option<[u8; 32]>,
        who: &Actor,
    ) -> Result<(AuditCheckpoint, u64)> {
        let floor = self.audit_tip.floor();
        let last = self.audit_tip.next_seq - 1;
        ensure!(through > floor, "entries up to #{floor} are already pruned");
        ensure!(
            through <= last,
            "the audit log ends at #{last}; there is no #{through}"
        );
        // Prove the chain is intact before throwing any of it away: a
        // checkpoint vouches for everything before it.
        self.audit_verify()
            .map_err(|e| anyhow::anyhow!("not pruning a log that does not verify: {e:#}"))?;
        let entry = self
            .store
            .audit_entry(through)?
            .ok_or_else(|| anyhow::anyhow!("audit entry #{through} is missing"))?;
        if let Some(h) = expect {
            ensure!(
                entry.hash == h,
                "audit entry #{through} does not have the hash given (the entries saved are not the ones in the log)"
            );
        }
        let unix = now_unix();
        let signature = self
            .key
            .sign(&AuditCheckpoint::signed_bytes(
                through,
                &entry.hash,
                entry.unix,
                floor,
                unix,
            ))
            .to_bytes()
            .to_vec();
        let cp = AuditCheckpoint {
            cluster: self.key.public().to_string(),
            seq: through,
            hash: entry.hash,
            entry_unix: entry.unix,
            previous_seq: floor,
            unix,
            signature,
        };
        let removed = self.store.prune_audit(&cp)?;
        self.audit_tip.checkpoint = Some(cp.clone());
        self.audit(
            who,
            "audit.prune",
            format!("#{} to #{through}", floor + 1),
            format!(
                "{removed} entries, up to {} UTC; checkpoint hash {}",
                rfc3339(entry.unix),
                data_encoding::HEXLOWER.encode(&entry.hash)
            ),
            true,
        );
        warn!(
            "audit log pruned: entries #{} to #{through} removed behind a signed checkpoint",
            floor + 1
        );
        Ok((cp, removed))
    }

    /// Prunes entries older than `keep` (`pumasi cluster start
    /// --keep-audit`), keeping the newest one at least. Zero keeps all.
    pub fn audit_prune_older(&mut self, keep: std::time::Duration, now: u64) -> Option<u64> {
        if keep.is_zero() {
            return None;
        }
        let cutoff = now.saturating_sub(keep.as_secs());
        let floor = self.audit_tip.floor();
        // The newest entry before the cutoff, never the head itself.
        let mut through = None;
        let last = self.audit_tip.next_seq.saturating_sub(1);
        let res = self.store.each_audit(floor, |e| {
            if e.unix < cutoff && e.seq < last {
                through = Some(e.seq);
                Ok(())
            } else {
                // Entries are in time order: stop at the first newer one.
                anyhow::bail!(StopWalk)
            }
        });
        if let Err(e) = res
            && !e.is::<StopWalk>()
        {
            warn!("could not read the audit log to prune it: {e:#}");
            return None;
        }
        let through = through?;
        match self.audit_prune(through, None, &Actor::system()) {
            Ok((_, n)) => Some(n),
            Err(e) => {
                warn!("could not prune the audit log: {e:#}");
                None
            }
        }
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
                    Executor::Image => "image",
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
        UpgradeToLatest { version, force } => (
            "rollout.latest",
            version.clone().unwrap_or_else(|| "latest".into()),
            if *force { "forced".into() } else { none() },
        ),
        CanaryRollout {
            files,
            force,
            canary,
        } => (
            "rollout.start",
            none(),
            format!(
                "{} file(s){}, canary {} (soak {}s, {} tasks{})",
                files.len(),
                if *force { ", forced" } else { "" },
                canary.nodes,
                canary.soak_secs,
                canary.min_tasks,
                if canary.hold { ", held" } else { "" }
            ),
        ),
        CanaryUpgradeToLatest {
            version,
            force,
            canary,
        } => (
            "rollout.latest",
            version.clone().unwrap_or_else(|| "latest".into()),
            format!(
                "{}canary {} (soak {}s, {} tasks{})",
                if *force { "forced, " } else { "" },
                canary.nodes,
                canary.soak_secs,
                canary.min_tasks,
                if canary.hold { ", held" } else { "" }
            ),
        ),
        PromoteCanary { nodes } => (
            "rollout.promote",
            if nodes.is_empty() {
                "every node".into()
            } else {
                nodes.join(",")
            },
            none(),
        ),
        SetNodeSettings { node, settings } => ("node.settings", node.clone(), settings.describe()),
        // Sessions log themselves in `server::session_call`.
        StartSession { .. } | RevokeSession { .. } | EndSession => return None,
        RenameNode { node, name } => (
            "node.rename",
            node.clone(),
            match name {
                Some(n) => format!("to {n:?}"),
                None => "back to the agent's name".into(),
            },
        ),
        SetReplication { name, factor } => (
            "store.replication",
            name.clone().unwrap_or_else(|| "(default)".into()),
            factor.map_or("the default".into(), |f| format!("{f} copies")),
        ),
        Scrub { node } => (
            "store.scrub",
            node.clone().unwrap_or_else(|| "control plane".into()),
            none(),
        ),
        S3Commit {
            name,
            account,
            meta,
            ..
        } => (
            "s3.write",
            name.clone(),
            format!(
                "account {}, {}",
                account.as_deref().unwrap_or(OWNER_ACCOUNT),
                meta.iter()
                    .take(8)
                    .map(|(p, m)| format!("{}{p}", if m.is_some() { "+" } else { "-" }))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        ),
        // S3 keys log themselves in `server::s3_key_call`.
        S3KeyCreate { .. } | S3KeyRevoke { .. } | S3Auth { .. } => return None,
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
        WakeNode { node } => ("node.wake", node.clone(), none()),
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
        // A whole ticket holds its secret: log its id only.
        RevokeJoinToken { id } => (
            "token.revoke",
            match id.trim().parse::<pumasi_proto::ticket::JoinTicket>() {
                Ok(t) => pumasi_proto::msg::join_token_id(&t.secret),
                Err(_) => id.trim().to_string(),
            },
            none(),
        ),
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
        SetAlertTuning(t) => (
            "alerts.tune",
            none(),
            format!(
                "repeat {:?}, resolved {}, {} overrides",
                t.repeat_secs,
                t.resolved,
                t.overrides.len()
            ),
        ),
        Silence(s) => (
            "alerts.silence",
            s.rule.clone().unwrap_or_default(),
            format!(
                "{} for {}s {}",
                s.nodes.as_ref().map(|n| n.to_string()).unwrap_or_default(),
                s.secs,
                s.comment
            ),
        ),
        Unsilence { id } => ("alerts.unsilence", id.clone(), none()),
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
            if *trusted {
                "node.trust"
            } else {
                "node.untrust"
            },
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
                Device(on) => ("sso.device", none(), if *on { "on" } else { "off" }.into()),
            }
        }
        // Reads.
        Status
        | Job { .. }
        | TaskFiles { .. }
        | Receipts { .. }
        | ReceiptsFull { .. }
        | TaskDetails { .. }
        | TaskExecutors { .. }
        | NodePower
        | AlertTuning
        | Net
        | Sessions
        | NodeSettings { .. }
        | Canary
        | Replication { .. }
        | StoreFiles { .. }
        | S3Keys
        | CheckJob { .. }
        | Accounts
        | Ledger { .. }
        | GetPolicy
        | RolloutStatus
        | LatestRelease { .. }
        | StoreUpload { .. }
        | StoreList { .. }
        | StoreUsage
        | GetStoragePolicy
        | Quotas
        | ListAccess
        | ListJoinTokens
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
        | SsoGet
        | Resources
        | Placement { .. }
        | AuditCheckpoint
        | AuditFrom { .. }
        | ControlPlanes
        | ReleaseChunk { .. }
        | MetricsWait { .. }
        | EventsKeyed { .. }
        | AlertTexts
        | JobWait { .. }
        | LogsWait { .. }
        | JobCaps
        | Verify
        | JobRuns { .. }
        | Energy { .. }
        | AlertsFull
        | ScimView => return None,
        // Unwrapped before anything looks at it.
        FollowerRead(inner) => return describe(inner),
        // Logged by the prune itself, with what went.
        AuditPrune { .. } => return None,
        // Counting requests is not a change to the cluster.
        Limit(_) => return None,
        // SCIM changes log themselves, one entry per person or group.
        Scim(_) => return None,
        ScimTokenCreate { label } => ("scim.token.create", label.clone(), none()),
        ScimTokenRevoke { id } => ("scim.token.revoke", id.clone(), none()),
        SetResources { account, quota } => ("resources.set", account.clone(), quota.describe()),
        SetJobCaps { account, caps } => ("job_caps.set", account.clone(), caps.describe()),
        SetNodeEnergy { node, settings } => ("node.energy", node.clone(), settings.describe()),
        SetVerifyPolicy(p) => ("honeypot.policy", none(), format!("{p:?}")),
        HoneypotAdd { job, task } => (
            "honeypot.add",
            job.to_string(),
            task.map_or("every succeeded task".into(), |t| format!("task {t}")),
        ),
        HoneypotRemove { id } => ("honeypot.remove", id.to_string(), none()),
        HoneypotRelease { account } => ("honeypot.release", account.clone(), none()),
        Rerun { id, which } => (
            "job.rerun",
            id.to_string(),
            match which {
                pumasi_proto::attempt::RerunWhich::Failed => "failed tasks".into(),
                pumasi_proto::attempt::RerunWhich::Tasks(t) => format!("tasks {t:?}"),
            },
        ),
        Suspend { id } => ("job.suspend", id.to_string(), none()),
        Resume { id } => ("job.resume", id.to_string(), none()),
        // Joined into the plain calls before they get here.
        SubmitPlaced { .. }
        | CheckJobPlaced { .. }
        | SetScheduleExt { .. }
        | ApplyExt { .. }
        | ApplyMore { .. }
        | SubmitExt { .. }
        | SubmitNet { .. }
        | CheckJobNet { .. }
        | CheckJobExt { .. }
        | SubmitRun { .. }
        | CheckJobRun { .. }
        | SetScheduleMore { .. }
        | ApplySched { .. } => {
            return describe(&op.clone().joined());
        }
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
        None if matches!(
            op,
            AdminOp::Audit { .. }
                | AdminOp::AuditVerify
                | AdminOp::AuditCheckpoint
                | AdminOp::AuditFrom { .. }
        ) =>
        {
            "audit.read".into()
        }
        None if matches!(op, AdminOp::AuditPrune { .. }) => "audit.prune".into(),
        None if matches!(op, AdminOp::SsoGet) => "sso.read".into(),
        None => format!("call.{}", variant_name(op)),
    }
}

/// The name of `op`'s variant (`StoreUpload`), from the start of its
/// `Debug` output. Formatting stops there: the whole output of a store or
/// release chunk would be several times its 32 MiB of data, on every call.
fn variant_name(op: &AdminOp) -> String {
    use std::fmt::Write;
    struct Name(String);
    impl Write for Name {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            for c in s.chars() {
                if !c.is_ascii_alphanumeric() || self.0.len() >= 64 {
                    return Err(std::fmt::Error);
                }
                self.0.push(c);
            }
            Ok(())
        }
    }
    let mut n = Name(String::new());
    let _ = write!(n, "{op:?}");
    n.0
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
        assert_eq!(
            (a, t.as_str(), d.as_str()),
            ("webhook.set", "ops", "hooks.slack.com")
        );
    }

    #[test]
    fn refusals_are_named_without_formatting_the_payload() {
        let op = AdminOp::UploadRelease {
            sha256: [0; 32],
            size: 64 << 20,
            offset: 0,
            bytes: vec![255; 64 << 20],
        };
        let t = std::time::Instant::now();
        assert_eq!(refused_action(&op), "call.UploadRelease");
        // The whole `Debug` output would be about 320 MB.
        assert!(t.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(variant_name(&AdminOp::Status), "Status");
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
