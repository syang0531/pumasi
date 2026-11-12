//! Reading and changing an account's quota and job policy (1.4), for the
//! API (`/api/v1/accounts/{name}/policy`, `whoami`) and the Accounts page.
//!
//! The control plane has one call that replaces a policy and one that
//! replaces a quota. A change that names only some fields is made here:
//! read what the account has, apply the change, write it back.

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{
    Secret,
    economy::{MilliCu, validate_account},
    manage::{AccessRole, AccountPolicy, AccountQuota, ListPatch, PolicyPatch, QuotaView},
    msg::{AdminOp, AdminResponse},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::St;

/// A change to an account's quota: only the limits named, `Some(None)`
/// lifting one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuotaPatch {
    pub slots: Option<Option<u32>>,
    pub gpus: Option<Option<u32>>,
    pub cu_per_day: Option<Option<MilliCu>>,
    pub max_priority: Option<Option<i32>>,
    pub storage_bytes: Option<Option<u64>>,
}

impl QuotaPatch {
    fn apply(&self, q: &mut AccountQuota) {
        if let Some(v) = self.slots {
            q.slots = v;
        }
        if let Some(v) = self.gpus {
            q.gpus = v;
        }
        if let Some(v) = self.cu_per_day {
            q.cu_per_day = v;
        }
        if let Some(v) = self.max_priority {
            q.max_priority = v;
        }
        if let Some(v) = self.storage_bytes {
            q.storage_bytes = v;
        }
    }
}

/// An account nothing was set for yet.
pub fn blank(account: &str) -> QuotaView {
    QuotaView {
        account: account.to_string(),
        quota: AccountQuota::default(),
        running: 0,
        gpus: 0,
        spent_day: 0,
        storage_bytes: 0,
        queued: 0,
        policy: AccountPolicy::default(),
    }
}

/// Every account the ticket may see, with its quota, use and policy.
pub async fn list(st: &St, secret: Secret) -> Result<Vec<QuotaView>> {
    match st.backend.call(secret, AdminOp::Quotas).await? {
        AdminResponse::Quotas(v) => Ok(v),
        _ => bail!("unexpected response"),
    }
}

/// One account as `role` may see it. A submitter sees its own account
/// (blank until something is set for it) and no other; `None` when the
/// cluster has no such account.
pub async fn get(
    st: &St,
    secret: Secret,
    role: &AccessRole,
    account: &str,
) -> Result<Option<QuotaView>> {
    validate_account(account)?;
    if let AccessRole::Submitter { account: own } = role {
        ensure!(
            own == account,
            "this ticket cannot read account {account}'s policy (it is for account {own})"
        );
    }
    let found = list(st, secret)
        .await?
        .into_iter()
        .find(|q| q.account == account);
    Ok(match (found, role) {
        (Some(q), _) => Some(q),
        (None, AccessRole::Submitter { .. }) => Some(blank(account)),
        (None, _) => None,
    })
}

/// Changes what `policy` and `quota` name and leaves the rest, for an
/// account that may not have anything set yet. Returns the account after
/// the change. The control plane refuses tickets that are not admin.
pub async fn patch(
    st: &St,
    secret: Secret,
    role: &AccessRole,
    account: &str,
    policy: &PolicyPatch,
    quota: &QuotaPatch,
) -> Result<QuotaView> {
    // The control plane refuses the change itself; this also refuses a
    // body that changes nothing, so the answer does not depend on it.
    ensure!(
        matches!(role, AccessRole::Admin),
        "this ticket cannot change account policies: it takes an admin ticket"
    );
    let before = get(st, secret, role, account)
        .await?
        .unwrap_or_else(|| blank(account));
    let mut p = before.policy.clone();
    policy.apply(&mut p)?;
    let mut q = before.quota.clone();
    quota.apply(&mut q);
    q.validate()?;
    if p != before.policy {
        expect_ok(
            st.backend
                .call(
                    secret,
                    AdminOp::SetAccountPolicy {
                        account: account.to_string(),
                        policy: Box::new(p),
                    },
                )
                .await?,
        )?;
    }
    if q != before.quota {
        expect_ok(
            st.backend
                .call(
                    secret,
                    AdminOp::SetQuota {
                        account: account.to_string(),
                        quota: q,
                    },
                )
                .await?,
        )?;
    }
    Ok(get(st, secret, role, account)
        .await?
        .unwrap_or_else(|| blank(account)))
}

fn expect_ok(r: AdminResponse) -> Result<()> {
    match r {
        AdminResponse::Ok => Ok(()),
        _ => bail!("unexpected response"),
    }
}

/// A policy as the API shows it: the words of `pumasi quota` and
/// `[accounts.NAME]` in cluster.toml, not the wire form.
pub fn policy_json(p: &AccountPolicy) -> Value {
    json!({
        "images": p.images,
        "require_digest": p.require_digest,
        "executors": p.executors,
        "network": if p.deny_network { "deny" } else { "allow" },
        "selector": p.selector.to_string(),
        "tolerations": p.tolerations.as_ref().map(|t| {
            t.iter().map(|t| t.to_string()).collect::<Vec<_>>()
        }),
        "max_timeout_ms": p.max_timeout_ms,
        "max_gpus_per_task": p.max_gpus_per_task,
    })
}

pub fn quota_json(q: &AccountQuota) -> Value {
    json!({
        "slots": q.slots,
        "gpus": q.gpus,
        "cu_per_day": q.cu_per_day,
        "max_priority": q.max_priority,
        "storage_bytes": q.storage_bytes,
    })
}

pub fn usage_json(v: &QuotaView) -> Value {
    json!({
        "running": v.running,
        "gpus": v.gpus,
        "queued": v.queued,
        "spent_day": v.spent_day,
        "storage_bytes": v.storage_bytes,
    })
}

/// `GET /api/v1/accounts/{name}/policy`: the policy's fields, then the
/// account's name, quota and use.
pub fn view_json(v: &QuotaView) -> Value {
    let mut out = policy_json(&v.policy);
    let m = out.as_object_mut().expect("an object");
    m.insert("account".into(), json!(v.account));
    m.insert("quota".into(), quota_json(&v.quota));
    m.insert("usage".into(), usage_json(v));
    out
}

/// A list in a PATCH body: the whole list, or what to add and remove.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ListBody {
    Set(Vec<String>),
    Edit(EditBody),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditBody {
    #[serde(default)]
    add: Vec<String>,
    #[serde(default)]
    remove: Vec<String>,
}

impl From<ListBody> for ListPatch {
    fn from(b: ListBody) -> Self {
        match b {
            ListBody::Set(v) => ListPatch {
                set: Some(v),
                ..Default::default()
            },
            ListBody::Edit(e) => ListPatch {
                set: None,
                add: e.add,
                remove: e.remove,
            },
        }
    }
}

/// Tells a field that is `null` from one that is missing.
fn given<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaBody {
    #[serde(default, deserialize_with = "given")]
    slots: Option<Option<u32>>,
    #[serde(default, deserialize_with = "given")]
    gpus: Option<Option<u32>>,
    #[serde(default, deserialize_with = "given")]
    cu_per_day: Option<Option<MilliCu>>,
    #[serde(default, deserialize_with = "given")]
    max_priority: Option<Option<i32>>,
    #[serde(default, deserialize_with = "given")]
    storage_bytes: Option<Option<u64>>,
}

/// The body of `PATCH /api/v1/accounts/{name}/policy`. Every field may be
/// left out. What `GET` answers is accepted as it is (its `account` must
/// be the one in the path, its `usage` is ignored), so a client may read,
/// change and send back.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchBody {
    images: Option<ListBody>,
    require_digest: Option<bool>,
    executors: Option<ListBody>,
    network: Option<String>,
    selector: Option<String>,
    #[serde(default, deserialize_with = "given")]
    tolerations: Option<Option<ListBody>>,
    #[serde(default, deserialize_with = "given")]
    max_timeout_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "given")]
    max_gpus_per_task: Option<Option<u32>>,
    quota: Option<QuotaBody>,
    account: Option<String>,
    #[allow(dead_code)]
    usage: Option<Value>,
}

/// Reads a PATCH body for `account`.
pub fn parse_patch(account: &str, body: &[u8]) -> Result<(PolicyPatch, QuotaPatch)> {
    let b: PatchBody = serde_json::from_slice(body).context("the body is not a policy change")?;
    if let Some(a) = &b.account {
        ensure!(
            a == account,
            "the body is for account {a}, the path for {account}"
        );
    }
    let (tolerations, any_toleration) = match b.tolerations {
        None => (ListPatch::default(), false),
        Some(None) => (ListPatch::default(), true),
        Some(Some(l)) => (l.into(), false),
    };
    let policy = PolicyPatch {
        images: b.images.map(Into::into).unwrap_or_default(),
        require_digest: b.require_digest,
        executors: b.executors.map(Into::into).unwrap_or_default(),
        deny_network: b
            .network
            .as_deref()
            .map(pumasi_proto::manage::parse_network_rule)
            .transpose()?,
        selector: b
            .selector
            .as_deref()
            .map(|s| s.trim().parse())
            .transpose()
            .context("selector")?,
        tolerations,
        any_toleration,
        max_timeout_ms: b.max_timeout_ms,
        max_gpus_per_task: b.max_gpus_per_task,
    };
    let q = b.quota.unwrap_or_default();
    let quota = QuotaPatch {
        slots: q.slots,
        gpus: q.gpus,
        cu_per_day: q.cu_per_day,
        max_priority: q.max_priority,
        storage_bytes: q.storage_bytes,
    };
    Ok((policy, quota))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_bodies() {
        // Nothing named: nothing changes.
        let (p, q) = parse_patch("ci", b"{}").unwrap();
        assert!(p.is_empty() && q == QuotaPatch::default());

        // Lists as a whole or as edits; null lifts a limit; a missing
        // field is left alone.
        let (p, q) = parse_patch(
            "ci",
            br#"{"images": {"add": ["node:22-slim"], "remove": ["rust:1-slim"]},
                "executors": ["docker"],
                "network": "deny",
                "selector": "gpu=nvidia",
                "tolerations": null,
                "max_timeout_ms": null,
                "max_gpus_per_task": 2,
                "quota": {"slots": 4, "gpus": null}}"#,
        )
        .unwrap();
        assert_eq!(p.images.add, ["node:22-slim"]);
        assert_eq!(p.images.remove, ["rust:1-slim"]);
        assert_eq!(p.images.set, None);
        assert_eq!(p.executors.set, Some(vec!["docker".to_string()]));
        assert_eq!(p.deny_network, Some(true));
        assert_eq!(p.selector.as_ref().unwrap().to_string(), "gpu=nvidia");
        assert!(p.any_toleration);
        assert_eq!(p.max_timeout_ms, Some(None));
        assert_eq!(p.max_gpus_per_task, Some(Some(2)));
        assert_eq!(p.require_digest, None);
        assert_eq!(q.slots, Some(Some(4)));
        assert_eq!(q.gpus, Some(None));
        assert_eq!(q.cu_per_day, None);

        // What GET answers goes back in as it is.
        let view = QuotaView {
            policy: AccountPolicy {
                images: vec!["python:3.12-slim".into()],
                deny_network: true,
                tolerations: Some(vec!["spot".parse().unwrap()]),
                max_timeout_ms: Some(60_000),
                ..Default::default()
            },
            quota: AccountQuota {
                slots: Some(2),
                ..Default::default()
            },
            ..blank("ci")
        };
        let body = serde_json::to_vec(&view_json(&view)).unwrap();
        let (p, q) = parse_patch("ci", &body).unwrap();
        let mut policy = AccountPolicy::default();
        p.apply(&mut policy).unwrap();
        assert_eq!(policy, view.policy);
        let mut quota = AccountQuota::default();
        q.apply(&mut quota);
        assert_eq!(quota, view.quota);

        // Refused: unknown fields, another account's body, bad values.
        assert!(parse_patch("ci", br#"{"image": ["a"]}"#).is_err());
        assert!(parse_patch("ci", br#"{"images": {"adds": ["a"]}}"#).is_err());
        assert!(
            parse_patch(
                "ci",
                &serde_json::to_vec(&view_json(&blank("other"))).unwrap()
            )
            .is_err()
        );
        assert!(parse_patch("ci", br#"{"network": "maybe"}"#).is_err());
        assert!(parse_patch("ci", br#"{"selector": "=="}"#).is_err());
        assert!(parse_patch("ci", b"[]").is_err());
    }

    /// The OpenAPI document and the code agree on the fields: what the
    /// schemas name is what GET answers and what PATCH accepts.
    #[test]
    fn openapi_describes_the_policy() {
        let doc: Value = serde_json::from_str(crate::api::OPENAPI).unwrap();
        let schemas = &doc["components"]["schemas"];
        let keys = |v: &Value| -> Vec<String> {
            let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
            k.sort();
            k
        };

        // GET: the policy's fields, then account, quota and usage.
        let view = view_json(&blank("ci"));
        let mut documented = keys(&schemas["AccountPolicy"]["properties"]);
        assert_eq!(documented, keys(&policy_json(&AccountPolicy::default())));
        documented.extend(["account", "quota", "usage"].map(String::from));
        documented.sort();
        assert_eq!(documented, keys(&view));
        assert_eq!(
            keys(&schemas["AccountQuota"]["properties"]),
            keys(&view["quota"])
        );
        assert_eq!(
            keys(&schemas["AccountUsage"]["properties"]),
            keys(&view["usage"])
        );

        // PATCH: every documented field is accepted alone, in each form
        // the schema gives it, and nothing else is.
        let patch = &schemas["AccountPolicyPatch"];
        assert_eq!(patch["additionalProperties"], false);
        let fields = keys(&patch["properties"]);
        assert_eq!(fields, keys(&view));
        let list = [
            json!(["a"]),
            json!({"add": ["a"]}),
            json!({"remove": ["a"]}),
        ];
        let executors = [
            json!(["wasm"]),
            json!({"add": ["docker"], "remove": ["microvm"]}),
        ];
        for f in &fields {
            let samples: Vec<Value> = match f.as_str() {
                "images" => list.to_vec(),
                "executors" => executors.to_vec(),
                "tolerations" => list.iter().cloned().chain([Value::Null]).collect(),
                "require_digest" => vec![json!(true)],
                "network" => vec![json!("allow"), json!("deny")],
                "selector" => vec![json!("zone=lab"), json!("")],
                "max_timeout_ms" | "max_gpus_per_task" => vec![json!(3), Value::Null],
                "account" => vec![json!("ci")],
                "usage" => vec![view["usage"].clone()],
                "quota" => keys(&schemas["AccountQuota"]["properties"])
                    .iter()
                    .flat_map(|q| [json!({ q: 5 }), json!({ q: null })])
                    .collect(),
                other => panic!("no sample for {other}"),
            };
            for v in samples {
                let body = serde_json::to_vec(&json!({ f: v })).unwrap();
                parse_patch("ci", &body)
                    .unwrap_or_else(|e| panic!("{}: {e:#}", String::from_utf8_lossy(&body)));
            }
        }
        let edit = &schemas["ListChange"]["oneOf"][1];
        assert_eq!(edit["additionalProperties"], false);
        assert_eq!(keys(&edit["properties"]), ["add", "remove"]);

        // The route uses these schemas.
        let route = &doc["paths"]["/api/v1/accounts/{name}/policy"];
        assert_eq!(
            route["patch"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/AccountPolicyPatch"
        );
        for method in ["get", "patch"] {
            assert_eq!(
                route[method]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/AccountPolicyView"
            );
        }
    }
}
