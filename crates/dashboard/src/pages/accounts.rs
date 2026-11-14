//! Accounts (1.4): each account's quota, what it uses now and the job
//! policy its submitter tickets are held to. Admins change them here; a
//! viewer reads them, and a submitter sees its own account.

use axum::{
    Form,
    extract::{Path, Query},
    http::HeaderMap,
    response::{IntoResponse, Redirect, Response},
};
use maud::html;
use pumasi_proto::{
    economy::{format_cu, parse_cu, validate_account},
    job::{Executor, format_duration_ms, parse_duration_ms},
    manage::{ListPatch, PolicyPatch, QuotaView, parse_network_rule},
    store::{format_size, parse_size},
};
use serde::Deserialize;

use pumasi_proto::msg::{AdminOp, AdminResponse};
use pumasi_proto::sched::{ResourceQuota, ResourceView};

use super::{Ctx, Nav, encode, hx, table, tile};
use crate::accounts::{self, QuotaPatch};

/// The account's CPU and memory quota and use (1.9); `None` from an older
/// control plane.
async fn resources(ctx: &Ctx, name: &str) -> Option<ResourceView> {
    match ctx.call(AdminOp::Resources).await {
        Ok(AdminResponse::Resources(v)) => Some(
            v.into_iter()
                .find(|r| r.account == name)
                .unwrap_or_else(|| ResourceView {
                    account: name.to_string(),
                    quota: Default::default(),
                    used: Default::default(),
                    labels_used: Vec::new(),
                }),
        ),
        _ => None,
    }
}

/// `used / limit`, or only what is used when there is no limit.
fn of(used: String, limit: Option<String>) -> String {
    match limit {
        Some(m) => format!("{used} / {m}"),
        None => used,
    }
}

pub async fn list(ctx: Ctx) -> Response {
    let l = ctx.l;
    let list = match accounts::list(&ctx.st, ctx.me.secret).await {
        Ok(v) => v,
        Err(e) => return ctx.fail(e, false),
    };
    let rows = list
        .iter()
        .map(|v| {
            let q = &v.quota;
            html! {
                tr data-account=(v.account) {
                    td { a href={"/accounts/" (encode(&v.account))} { strong { (v.account) } } }
                    td { (of(v.running.to_string(), q.slots.map(|s| s.to_string()))) }
                    td { (of(v.gpus.to_string(), q.gpus.map(|s| s.to_string()))) }
                    td { (of(format_cu(v.spent_day), q.cu_per_day.map(format_cu))) }
                    td { @match q.max_priority { Some(p) => { (p) }, None => { span class="dim" { "—" } } } }
                    td { (of(format_size(v.storage_bytes), q.storage_bytes.map(format_size))) }
                    td { (v.queued) }
                    td class="policy" {
                        @if v.policy.is_empty() { span class="dim" { (l.t("none", "없음")) } }
                        @else { (v.policy.describe()) }
                    }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Accounts", "계정")) } }
        p class="dim" { (l.t(
            "Quotas hold what an account uses at once. The job policy holds what jobs sent with the account's submitter tickets may be; admin tickets are not held to it.",
            "쿼터는 계정이 한 번에 쓰는 양을 제한합니다. 작업 정책은 그 계정의 제출자 티켓으로 보낸 작업에 적용되며, 관리자 티켓에는 적용되지 않습니다."
        )) }
        @if ctx.me.is_admin() {
            form class="inline" method="get" action="/accounts/open" {
                input name="account" placeholder=(l.t("account", "계정")) required size="16" pattern="[a-z0-9_-]+";
                button type="submit" { (l.t("Open or create", "열기 또는 만들기")) }
            }
        }
    };
    let live = html! {
        section {
            (table(
                &[
                    l.t("Account", "계정"),
                    l.t("Running", "실행 중"),
                    "GPU",
                    l.t("CU / 24h", "CU / 24시간"),
                    l.t("Max priority", "최대 우선순위"),
                    l.t("Storage", "저장소"),
                    l.t("Queued", "대기"),
                    l.t("Job policy", "작업 정책"),
                ],
                rows,
                l.t("No accounts yet.", "아직 계정이 없습니다."),
            ))
            p class="dim" { (l.t("Used / limit; one number means no limit.", "사용량 / 한도. 숫자가 하나면 한도가 없습니다.")) }
        }
    };
    ctx.page(
        l.t("Accounts", "계정"),
        Nav::Accounts,
        top,
        Some(("10s", live)),
        html! {},
    )
}

#[derive(Deserialize)]
pub struct OpenQuery {
    account: String,
}

/// The form on the list: goes to the account's page.
pub async fn open(Query(q): Query<OpenQuery>) -> Response {
    Redirect::to(&format!("/accounts/{}", encode(q.account.trim()))).into_response()
}

pub async fn detail(ctx: Ctx, Path(name): Path<String>) -> Response {
    match render(&ctx, &name).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render(ctx: &Ctx, name: &str) -> anyhow::Result<Response> {
    let l = ctx.l;
    let admin = ctx.me.is_admin();
    let found = accounts::get(&ctx.st, ctx.me.secret, &ctx.me.role, name).await?;
    let v = match found {
        Some(v) => v,
        // An admin may set a quota and a policy before the account has
        // done anything.
        None if admin => accounts::blank(name),
        None => anyhow::bail!("no such account: {name}"),
    };
    let res = resources(ctx, name).await;
    let q = &v.quota;
    let p = &v.policy;
    let has = |e: Executor| p.executors.contains(&e);
    let (tol_mode, tol_text) = match &p.tolerations {
        None => ("any", String::new()),
        Some(t) if t.is_empty() => ("none", String::new()),
        Some(t) => (
            "list",
            t.iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(","),
        ),
    };
    let num = |v: Option<String>| v.unwrap_or_default();
    let top = html! {
        div class="page-head" {
            h1 { a href="/accounts" { (l.t("Accounts", "계정")) } " / " (v.account) }
        }
    };
    let live = html! {
        section class="tiles" {
            (tile(l.t("Running", "실행 중"), of(v.running.to_string(), q.slots.map(|s| s.to_string())), None))
            (tile("GPU", of(v.gpus.to_string(), q.gpus.map(|s| s.to_string())), None))
            (tile(l.t("Queued", "대기"), v.queued.to_string(), None))
            (tile(l.t("CU in 24h", "24시간 CU"), of(format_cu(v.spent_day), q.cu_per_day.map(format_cu)), None))
            (tile(l.t("Storage", "저장소"), of(format_size(v.storage_bytes), q.storage_bytes.map(format_size)), None))
            @if let Some(r) = &res {
                (tile("CPU", of(r.used.cpus.to_string(), r.quota.cpus.map(|c| c.to_string())), None))
                (tile(l.t("Memory", "메모리"), of(format_size(r.used.memory_bytes), r.quota.memory_bytes.map(format_size)), None))
            }
        }
        @if let Some(r) = res.as_ref().filter(|r| !r.quota.labels.is_empty()) {
            (table(&[l.t("Nodes", "노드"), l.t("Runs", "실행"), "CPU", l.t("Memory", "메모리"), "GPU"],
                r.quota.labels.iter().zip(&r.labels_used).map(|(lq, u)| html! {
                    tr {
                        td { code { (lq.selector.to_string()) } }
                        td { (of(u.runs.to_string(), lq.slots.map(|x| x.to_string()))) }
                        td { (of(u.cpus.to_string(), lq.cpus.map(|x| x.to_string()))) }
                        td { (of(format_size(u.memory_bytes), lq.memory_bytes.map(format_size))) }
                        td { (of(u.gpus.to_string(), lq.gpus.map(|x| x.to_string()))) }
                    }
                }).collect(), ""))
        }
    };
    let form = html! {
        form #account-form class="stack" hx-post={"/accounts/" (encode(&v.account))} hx-target="#flash" {
            fieldset disabled[!admin] {
                legend { (l.t("Job policy", "작업 정책")) }
                p class="dim" { (l.t(
                    "For jobs sent with this account's submitter tickets. Whatever is left empty is not limited.",
                    "이 계정의 제출자 티켓으로 보낸 작업에 적용됩니다. 비워 둔 항목은 제한하지 않습니다."
                )) }
                label for="images" { (l.t("Allowed images", "허용 이미지")) }
                p class="dim" { (l.t(
                    "One pattern per line (* matches any characters), or name@sha256:... for one digest. Empty: any image.",
                    "한 줄에 패턴 하나(*는 아무 문자열과 일치), 또는 name@sha256:... 형식으로 다이제스트 하나를 지정합니다. 비우면 모든 이미지를 허용합니다."
                )) }
                textarea #images name="images" rows="4" spellcheck="false" { (p.images.join("\n")) }
                div class="row" {
                    label { input type="checkbox" name="require_digest" value="true" checked[p.require_digest];
                        " " (l.t("Only images named by digest", "다이제스트로 지정한 이미지만")) }
                }
                div class="row" {
                    span { (l.t("Executors", "실행기")) ": " }
                    label { input type="checkbox" name="exec_wasm" value="true" checked[has(Executor::Wasm)]; " wasm" }
                    label { input type="checkbox" name="exec_docker" value="true" checked[has(Executor::Docker)]; " docker" }
                    label { input type="checkbox" name="exec_microvm" value="true" checked[has(Executor::Microvm)]; " microvm" }
                    span class="dim" { (l.t("(none checked: any)", "(모두 끄면 제한 없음)")) }
                }
                div class="row" {
                    div {
                        label for="network" { (l.t("Network", "네트워크")) }
                        select #network name="network" {
                            option value="allow" selected[!p.deny_network] { (l.t("allow", "허용")) }
                            option value="deny" selected[p.deny_network] { (l.t("deny", "거부")) }
                        }
                    }
                    div {
                        label for="max_timeout" { (l.t("Longest timeout", "최대 타임아웃")) }
                        input #max_timeout name="max_timeout" size="8" placeholder=(l.t("no limit", "제한 없음"))
                            value=(num(p.max_timeout_ms.map(format_duration_ms)));
                    }
                    div {
                        label for="max_gpus_per_task" { (l.t("GPUs per task", "태스크당 GPU")) }
                        input #max_gpus_per_task name="max_gpus_per_task" type="number" min="0" placeholder=(l.t("no limit", "제한 없음"))
                            value=(num(p.max_gpus_per_task.map(|g| g.to_string())));
                    }
                }
                div class="row" {
                    div {
                        label for="selector" { (l.t("Selector added to every job", "모든 작업에 더하는 셀렉터")) }
                        input #selector name="selector" size="28" placeholder="gpu=nvidia,os=linux" value=(p.selector.to_string());
                    }
                    div {
                        label for="tolerations_mode" { (l.t("Tolerations", "톨러레이션")) }
                        select #tolerations_mode name="tolerations_mode" {
                            option value="any" selected[tol_mode == "any"] { (l.t("any", "제한 없음")) }
                            option value="none" selected[tol_mode == "none"] { (l.t("none", "허용 안 함")) }
                            option value="list" selected[tol_mode == "list"] { (l.t("only these", "아래 목록만")) }
                        }
                        " "
                        input #tolerations name="tolerations" size="20" placeholder="spot,owner=alice" value=(tol_text);
                    }
                }
            }
            fieldset disabled[!admin] {
                legend { (l.t("Quota", "쿼터")) }
                p class="dim" { (l.t("Empty: no limit.", "비우면 한도가 없습니다.")) }
                div class="row" {
                    div {
                        label for="slots" { (l.t("Runs at once", "동시 실행")) }
                        input #slots name="slots" type="number" min="0" value=(num(q.slots.map(|s| s.to_string())));
                    }
                    div {
                        label for="gpus" { (l.t("GPUs at once", "동시 GPU")) }
                        input #gpus name="gpus" type="number" min="0" value=(num(q.gpus.map(|s| s.to_string())));
                    }
                    div {
                        label for="cu_per_day" { (l.t("CU per 24h", "24시간당 CU")) }
                        input #cu_per_day name="cu_per_day" size="8" value=(num(q.cu_per_day.map(format_cu)));
                    }
                    div {
                        label for="max_priority" { (l.t("Max priority", "최대 우선순위")) }
                        input #max_priority name="max_priority" type="number" min="-1000" max="1000"
                            value=(num(q.max_priority.map(|s| s.to_string())));
                    }
                    div {
                        label for="storage" { (l.t("Storage", "저장소")) }
                        input #storage name="storage" size="8" placeholder="10GB" value=(num(q.storage_bytes.map(format_size)));
                    }
                }
            }
            @if let Some(r) = &res {
                fieldset disabled[!admin] {
                    legend { (l.t("CPU and memory", "CPU와 메모리")) }
                    p class="dim" { (l.t(
                        "What the account's running tasks hold at once (a WASM task holds one CPU). Empty: no limit.",
                        "이 계정의 실행 중인 태스크가 한꺼번에 차지하는 양입니다(WASM 태스크는 CPU 1개). 비우면 한도가 없습니다."
                    )) }
                    div class="row" {
                        div {
                            label for="cpus" { "CPU" }
                            input #cpus name="cpus" type="number" min="0" value=(num(r.quota.cpus.map(|c| c.to_string())));
                        }
                        div {
                            label for="memory" { (l.t("Memory", "메모리")) }
                            input #memory name="memory" size="8" placeholder="64G" value=(num(r.quota.memory_bytes.map(format_size)));
                        }
                    }
                    label for="label_quotas" { (l.t("On the nodes a label picks", "라벨로 고른 노드에서")) }
                    p class="dim" { (l.t(
                        "One per line: a selector, then slots, cpus, memory and gpus, e.g. zone=office slots=4 cpus=8 memory=16G",
                        "한 줄에 하나씩: 셀렉터 다음에 slots, cpus, memory, gpus. 예: zone=office slots=4 cpus=8 memory=16G"
                    )) }
                    textarea #label_quotas name="label_quotas" rows="3" spellcheck="false" {
                        (r.quota.labels.iter().map(|q| q.to_string()).collect::<Vec<_>>().join("\n"))
                    }
                }
            }
            @if admin {
                div class="row" { button type="submit" { (l.t("Save", "저장")) } }
            } @else {
                p class="dim" { (l.t("Only an admin ticket changes these.", "관리자 티켓만 바꿀 수 있습니다.")) }
            }
        }
    };
    Ok(ctx.page(
        &format!("{} · {}", l.t("Accounts", "계정"), v.account),
        Nav::Accounts,
        top,
        Some(("10s", live)),
        form,
    ))
}

/// The form holds every field, so what is empty is set to "no limit".
#[derive(Deserialize, Default)]
pub struct AccountForm {
    images: Option<String>,
    require_digest: Option<String>,
    exec_wasm: Option<String>,
    exec_docker: Option<String>,
    exec_microvm: Option<String>,
    network: Option<String>,
    selector: Option<String>,
    tolerations_mode: Option<String>,
    tolerations: Option<String>,
    max_timeout: Option<String>,
    max_gpus_per_task: Option<String>,
    slots: Option<String>,
    gpus: Option<String>,
    cu_per_day: Option<String>,
    max_priority: Option<String>,
    storage: Option<String>,
    cpus: Option<String>,
    memory: Option<String>,
    label_quotas: Option<String>,
}

fn opt(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

/// Entries separated by commas, spaces or line ends.
fn entries(s: &Option<String>) -> Vec<String> {
    s.as_deref()
        .unwrap_or("")
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|e| !e.is_empty())
        .map(str::to_string)
        .collect()
}

impl AccountForm {
    fn patches(&self) -> anyhow::Result<(PolicyPatch, QuotaPatch)> {
        let whole = |v: Vec<String>| ListPatch {
            set: Some(v),
            ..Default::default()
        };
        let mut executors = Vec::new();
        for (on, name) in [
            (&self.exec_wasm, "wasm"),
            (&self.exec_docker, "docker"),
            (&self.exec_microvm, "microvm"),
        ] {
            if on.is_some() {
                executors.push(name.to_string());
            }
        }
        let (tolerations, any_toleration) = match opt(&self.tolerations_mode).unwrap_or("any") {
            "any" => (ListPatch::default(), true),
            "none" => (whole(Vec::new()), false),
            "list" => (whole(entries(&self.tolerations)), false),
            other => anyhow::bail!("tolerations: any, none or list, not {other:?}"),
        };
        let number = |s: &Option<String>, what: &str| -> anyhow::Result<Option<u32>> {
            opt(s)
                .map(|v| {
                    v.parse::<u32>()
                        .map_err(|_| anyhow::anyhow!("{what}: a whole number, or empty"))
                })
                .transpose()
        };
        let policy = PolicyPatch {
            images: whole(entries(&self.images)),
            require_digest: Some(self.require_digest.is_some()),
            executors: whole(executors),
            deny_network: Some(parse_network_rule(opt(&self.network).unwrap_or("allow"))?),
            selector: Some(opt(&self.selector).unwrap_or("").parse()?),
            tolerations,
            any_toleration,
            max_timeout_ms: Some(opt(&self.max_timeout).map(parse_duration_ms).transpose()?),
            max_gpus_per_task: Some(number(&self.max_gpus_per_task, "GPUs per task")?),
        };
        let quota = QuotaPatch {
            slots: Some(number(&self.slots, "runs at once")?),
            gpus: Some(number(&self.gpus, "GPUs at once")?),
            cu_per_day: Some(opt(&self.cu_per_day).map(parse_cu).transpose()?),
            max_priority: Some(
                opt(&self.max_priority)
                    .map(|v| {
                        v.parse::<i32>()
                            .map_err(|_| anyhow::anyhow!("max priority: a whole number, or empty"))
                    })
                    .transpose()?,
            ),
            storage_bytes: Some(opt(&self.storage).map(parse_size).transpose()?),
        };
        Ok((policy, quota))
    }

    /// The CPU and memory quota (1.9), when the form had those fields.
    fn resources(&self) -> anyhow::Result<Option<ResourceQuota>> {
        if self.cpus.is_none() && self.memory.is_none() && self.label_quotas.is_none() {
            return Ok(None);
        }
        let q = ResourceQuota {
            cpus: opt(&self.cpus)
                .map(|v| {
                    v.parse::<u32>()
                        .map_err(|_| anyhow::anyhow!("CPUs: a whole number, or empty"))
                })
                .transpose()?,
            memory_bytes: opt(&self.memory).map(parse_size).transpose()?,
            labels: self
                .label_quotas
                .as_deref()
                .unwrap_or("")
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(|l| l.parse())
                .collect::<anyhow::Result<_>>()?,
        };
        q.validate()?;
        Ok(Some(q))
    }
}

pub async fn save(
    ctx: Ctx,
    headers: HeaderMap,
    Path(name): Path<String>,
    Form(f): Form<AccountForm>,
) -> Response {
    let l = ctx.l;
    let res: anyhow::Result<QuotaView> = async {
        validate_account(&name)?;
        let (policy, quota) = f.patches()?;
        let res = f.resources()?;
        let v =
            accounts::patch(&ctx.st, ctx.me.secret, &ctx.me.role, &name, &policy, &quota).await?;
        if let Some(q) = res {
            ctx.call(AdminOp::SetResources {
                account: name.clone(),
                quota: Box::new(q),
            })
            .await?;
        }
        Ok(v)
    }
    .await;
    match res {
        Ok(v) => ctx.done(format!(
            "{} {}: {} · {}",
            l.t("Saved", "저장했습니다"),
            v.account,
            v.quota.describe(),
            v.policy.describe()
        )),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::manage::AccountPolicy;

    #[test]
    fn the_form_sets_every_field() {
        let s = |v: &str| Some(v.to_string());
        let f = AccountForm {
            images: s("python:3.12-slim\nrust:1-slim, node:22-slim\n"),
            require_digest: None,
            exec_docker: s("true"),
            network: s("deny"),
            selector: s("gpu=nvidia"),
            tolerations_mode: s("list"),
            tolerations: s("spot, owner=alice"),
            max_timeout: s("2h"),
            max_gpus_per_task: s("1"),
            slots: s("4"),
            cu_per_day: s("12.5"),
            storage: s("10GB"),
            ..Default::default()
        };
        let (p, q) = f.patches().unwrap();
        // A policy that had other values ends up with the form's.
        let mut policy = AccountPolicy {
            images: vec!["old".into()],
            require_digest: true,
            executors: vec![Executor::Wasm],
            ..Default::default()
        };
        p.apply(&mut policy).unwrap();
        assert_eq!(
            policy.images,
            ["python:3.12-slim", "rust:1-slim", "node:22-slim"]
        );
        assert!(!policy.require_digest);
        assert_eq!(policy.executors, [Executor::Docker]);
        assert!(policy.deny_network);
        assert_eq!(policy.selector.to_string(), "gpu=nvidia");
        assert_eq!(policy.tolerations.as_ref().unwrap().len(), 2);
        assert_eq!(policy.max_timeout_ms, Some(2 * 3600 * 1000));
        assert_eq!(policy.max_gpus_per_task, Some(1));
        assert_eq!(q.slots, Some(Some(4)));
        assert_eq!(q.gpus, Some(None));
        assert_eq!(q.cu_per_day, Some(Some(12_500)));
        assert_eq!(q.max_priority, Some(None));
        assert!(q.storage_bytes.unwrap().unwrap() > 0);

        // An empty form lifts everything.
        let (p, q) = AccountForm::default().patches().unwrap();
        p.apply(&mut policy).unwrap();
        assert!(policy.is_empty());
        assert_eq!(
            q,
            QuotaPatch {
                slots: Some(None),
                gpus: Some(None),
                cu_per_day: Some(None),
                max_priority: Some(None),
                storage_bytes: Some(None),
            }
        );

        assert!(
            AccountForm {
                max_gpus_per_task: s("many"),
                ..Default::default()
            }
            .patches()
            .is_err()
        );
    }
}
