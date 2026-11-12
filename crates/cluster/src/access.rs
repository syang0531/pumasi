//! What each access ticket may do (phase 10). The cluster's admin ticket
//! and admin-role tickets may do everything; a viewer only reads; a
//! submitter works with one account's jobs, schedules and datasets and sees
//! only those.

use anyhow::{Context, Result, ensure};
use pumasi_proto::{
    job::{JobId, JobSubmission},
    msg::{AdminOp, AdminResponse},
    store::{MountSource, StoreTarget},
};

use crate::{manage::Caller, state::State};

/// A short name for an admin call, for messages.
fn op_name(op: &AdminOp) -> &'static str {
    use AdminOp::*;
    match op {
        CreateJoinToken {
            invite: Some(_), ..
        } => "invite machines",
        CreateJoinToken { .. } => "create join tickets",
        Submit(_) => "submit jobs",
        Status => "read the status",
        Job { .. } => "read jobs",
        TaskFiles { .. } => "read task outputs",
        Cancel { .. } => "cancel jobs",
        RemoveNode { .. } => "remove nodes",
        Receipts { .. } => "read receipts",
        Accounts => "read accounts",
        Ledger { .. } => "read the ledger",
        Deposit { .. } => "grant CU",
        Payout { .. } => "pay CU out",
        Benchmark { .. } => "benchmark nodes",
        GetPolicy => "read prices",
        SetPolicy(_) => "change prices",
        UploadRelease { .. } | Rollout { .. } | CancelRollout => "roll out upgrades",
        RolloutStatus => "read the rollout",
        StoreUpload { .. } | StoreCommit { .. } => "upload to the store",
        StoreList { .. } => "list datasets",
        StoreRemove { .. } => "remove datasets",
        StoreOpen { .. } => "download from the store",
        StoreUsage => "read store usage",
        StoreQuota { .. } => "set storage quotas",
        SecretSet { .. } | SecretRemove { .. } => "change secrets",
        SecretList => "list secrets",
        GetStoragePolicy => "read storage prices",
        SetStoragePolicy(_) => "change storage prices",
        EditNode { .. } => "change nodes",
        Drain { .. } => "drain nodes",
        SetQuota { .. } => "set quotas",
        Quotas => "read quotas",
        CreateAccess { .. } | RevokeAccess { .. } => "manage access tickets",
        ListAccess => "list access tickets",
        WhoAmI => "ask who it is",
        SetSchedule(_) | RemoveSchedule { .. } => "change schedules",
        Schedules => "list schedules",
        RunSchedule { .. } => "run schedules",
        Apply(_) => "apply cluster files",
        Metrics { .. } => "read metrics",
        Events { .. } => "read events",
        Logs { .. } => "read task output",
        Alerts => "read alerts",
        SetAlertPolicy(_) => "change alert rules",
        SetWebhook(_) | RemoveWebhook { .. } => "change webhooks",
        TestWebhook { .. } => "test webhooks",
        HaStatus => "read the control plane replicas",
        HaInvite { .. } | HaRemove { .. } => "change the control plane replicas",
        Backup { .. } => "back up the control plane",
        SetAccountPolicy { .. } => "set job policies",
        Confidential => "read the confidential tier",
        TrustMeasurement { .. } => "trust launch measurements",
        TrustNode { .. } => "trust nodes",
        Audit { .. } | AuditVerify => "read the audit log",
        RotateAdminSecret => "replace the admin ticket",
        SsoGet => "read the SSO settings",
        SsoSet(_) => "change the SSO settings",
        SsoInfo
        | SsoStart { .. }
        | SsoFinish { .. }
        | SsoDeviceStart
        | SsoDevicePoll { .. }
        | LdapLogin { .. } => "sign in",
        ListJoinTokens => "list join tickets",
        RevokeJoinToken { .. } => "revoke join tickets",
        RenameNode { .. } => "rename nodes",
        LatestRelease { check: false } => "read the release check",
        LatestRelease { check: true } => "check for releases",
        UpgradeToLatest { .. } => "roll out upgrades",
        TaskDetails { .. } | TaskExecutors { .. } => "read jobs",
        NodePower => "read the status",
        WakeNode { .. } => "wake nodes",
        CheckJob { .. } => "check jobs",
    }
}

fn denied(caller: &Caller, op: &AdminOp) -> anyhow::Error {
    anyhow::anyhow!(
        "a {} ticket cannot {}; that needs an admin ticket",
        match caller {
            Caller::Admin { .. } => "admin",
            Caller::Submitter { .. } => "submitter",
            Caller::Viewer { .. } => "viewer",
        },
        op_name(op)
    )
}

/// Checks a call against the caller's role. A submitter's calls are
/// pinned to its account (and refused for anything of another account).
pub fn admit(st: &State, caller: &Caller, op: AdminOp) -> Result<AdminOp> {
    use AdminOp::*;
    match caller {
        Caller::Admin { .. } => Ok(op),
        Caller::Viewer { .. } => match op {
            Status
            | Job { .. }
            | TaskDetails { .. }
            | TaskExecutors { .. }
            | NodePower
            | TaskFiles { .. }
            | Receipts { .. }
            | Accounts
            | Ledger { .. }
            | GetPolicy
            | GetStoragePolicy
            | RolloutStatus
            | LatestRelease { check: false }
            | StoreList { .. }
            | StoreUsage
            | Quotas
            | Schedules
            | WhoAmI
            | Metrics { .. }
            | Events { .. }
            | Logs { .. }
            | Alerts
            | Confidential
            | HaStatus => Ok(op),
            other => Err(denied(caller, &other)),
        },
        Caller::Submitter { account, .. } => submitter(st, caller, account, op),
    }
}

/// The account a submitter's request names must be its own.
fn pin(asked: &mut Option<String>, account: &str) -> Result<()> {
    if let Some(a) = asked.as_deref() {
        ensure!(
            a == account,
            "this ticket submits for account {account}, not {a}"
        );
    }
    *asked = Some(account.to_string());
    Ok(())
}

fn own_job(st: &State, account: &str, id: &JobId) -> Result<()> {
    let job = st.jobs.get(id).context("no such job")?;
    ensure!(
        job.meta.account == account,
        "job {id} belongs to another account"
    );
    Ok(())
}

fn own_dataset(st: &State, account: &str, name: &str, version: Option<u32>) -> Result<()> {
    let d = st.dataset(name, version)?;
    ensure!(
        d.account == account,
        "dataset {name} belongs to account {}",
        d.account
    );
    Ok(())
}

/// A submitter mounts only its own account's datasets.
fn own_mounts(st: &State, account: &str, sub: &JobSubmission) -> Result<()> {
    for m in sub.mounts.iter().chain(sub.task_mounts.iter().flatten()) {
        if let MountSource::Dataset { name, version } = &m.source {
            own_dataset(st, account, name, *version)?;
        }
    }
    Ok(())
}

fn submitter(st: &State, caller: &Caller, account: &str, op: AdminOp) -> Result<AdminOp> {
    use AdminOp::*;
    match &op {
        Cancel { id }
        | Receipts { id }
        | TaskFiles { id, .. }
        | Job { id, .. }
        | TaskDetails { id }
        | TaskExecutors { id }
        | Logs { job: id, .. } => own_job(st, account, id)?,
        RemoveSchedule { name } | RunSchedule { name } => {
            if let Some(other) = st.schedule_account(name) {
                ensure!(
                    other == account,
                    "schedule {name} belongs to account {other}"
                );
            }
        }
        _ => {}
    }
    Ok(match op {
        Submit(mut sub) => {
            pin(&mut sub.account, account)?;
            own_mounts(st, account, &sub)?;
            st.admit_policy(account, &mut sub)?;
            Submit(sub)
        }
        // Checked exactly as the submission would be.
        CheckJob {
            mut sub,
            upload_bytes,
        } => {
            pin(&mut sub.account, account)?;
            own_mounts(st, account, &sub)?;
            st.admit_policy(account, &mut sub)?;
            CheckJob { sub, upload_bytes }
        }
        op @ (Cancel { .. }
        | Receipts { .. }
        | TaskFiles { .. }
        | Job { .. }
        | TaskDetails { .. }
        | TaskExecutors { .. }
        | Logs { .. }) => op,
        op @ (Metrics { .. } | Events { .. } | Alerts) => op,
        op @ (Status
        | NodePower
        | Accounts
        | GetPolicy
        | GetStoragePolicy
        | StoreUsage
        | Quotas
        | Schedules
        | WhoAmI
        | Confidential
        | StoreList { .. }
        | StoreUpload { .. }) => op,
        Ledger {
            account: mut asked,
            limit,
        } => {
            pin(&mut asked, account)?;
            Ledger {
                account: asked,
                limit,
            }
        }
        StoreCommit {
            root,
            name,
            account: mut asked,
        } => {
            pin(&mut asked, account)?;
            if let Some(n) = &name
                && st.dataset(n, None).is_ok()
            {
                own_dataset(st, account, n, None)?;
            }
            StoreCommit {
                root,
                name,
                account: asked,
            }
        }
        StoreRemove { name, version } => {
            match version {
                Some(v) => own_dataset(st, account, &name, Some(v))?,
                None => {
                    for d in st.list_datasets(Some(&name)) {
                        ensure!(
                            d.account == account,
                            "dataset {name} has versions of account {}",
                            d.account
                        );
                    }
                }
            }
            StoreRemove { name, version }
        }
        StoreOpen { reader, target } => {
            match &target {
                StoreTarget::Dataset { name, version } => own_dataset(st, account, name, *version)?,
                StoreTarget::Job { id } => own_job(st, account, id)?,
            }
            StoreOpen { reader, target }
        }
        SetSchedule(mut spec) => {
            pin(&mut spec.job.account, account)?;
            own_mounts(st, account, &spec.job)?;
            st.admit_policy(account, &mut spec.job)?;
            if let Some(other) = st.schedule_account(&spec.name) {
                ensure!(
                    other == account,
                    "schedule {} belongs to account {other}",
                    spec.name
                );
            }
            let cap = st.priority_cap(account, true);
            ensure!(
                spec.job.priority <= cap,
                "priority {} is above the {cap} account {account} may use",
                spec.job.priority
            );
            SetSchedule(spec)
        }
        op @ (RemoveSchedule { .. } | RunSchedule { .. }) => op,
        other => return Err(denied(caller, &other)),
    })
}

/// Leaves out of a reply what a submitter may not see: other accounts'
/// jobs, balances, datasets, quotas and schedules.
pub fn scope(caller: &Caller, resp: AdminResponse) -> AdminResponse {
    let Caller::Submitter { account, .. } = caller else {
        return resp;
    };
    match resp {
        AdminResponse::Status(mut s) => {
            s.jobs.retain(|j| j.account == *account);
            AdminResponse::Status(s)
        }
        AdminResponse::Accounts(mut v) => {
            v.retain(|a| a.name == *account);
            AdminResponse::Accounts(v)
        }
        AdminResponse::Datasets(mut v) => {
            v.retain(|d| d.account == *account);
            AdminResponse::Datasets(v)
        }
        AdminResponse::StoreUsage { mut usage, .. } => {
            usage.retain(|u| u.account == *account);
            AdminResponse::StoreUsage {
                usage,
                disk_bytes: 0,
            }
        }
        AdminResponse::Quotas(mut v) => {
            v.retain(|q| q.account == *account);
            AdminResponse::Quotas(v)
        }
        AdminResponse::Schedules(mut v) => {
            v.retain(|s| s.account == *account);
            AdminResponse::Schedules(v)
        }
        // Events about nodes and the cluster, and its own jobs.
        AdminResponse::Events(mut v) => {
            v.retain(|e| e.account.as_ref().is_none_or(|a| a == account));
            AdminResponse::Events(v)
        }
        other => other,
    }
}
