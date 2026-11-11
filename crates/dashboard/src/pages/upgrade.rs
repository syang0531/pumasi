//! The upgrade panel on the Nodes page (1.3): the newest published
//! release, the nodes' versions, and a button that has the control plane
//! download that release and roll it out (`pumasi upgrade latest`).

use axum::{
    http::HeaderMap,
    response::{Html, IntoResponse, Response},
};
use maud::{Markup, html};
use pumasi_proto::{
    msg::{AdminOp, AdminResponse, RolloutView, UpgradeState},
    release::{FetchState, LatestReleaseView, is_older},
};

use super::{Ctx, hx};
use crate::fmt;

/// The placeholder the Nodes page puts where the panel loads.
pub fn placeholder(ctx: &Ctx) -> Markup {
    if !ctx.me.is_admin() {
        return html! {};
    }
    html! {
        section #upgrade hx-get="/upgrade" hx-trigger="load" hx-swap="outerHTML" {
            h2 { (ctx.t("Upgrade", "업그레이드")) }
            p class="dim" { (ctx.t("Checking for a new release…", "새 릴리스를 확인하는 중…")) }
        }
    }
}

pub async fn panel(ctx: Ctx) -> Response {
    match render(&ctx).await {
        Ok(m) => Html(m.into_string()).into_response(),
        Err(e) => Html(
            html! { section #upgrade { p class="flash bad" { (format!("{e:#}")) } } }.into_string(),
        )
        .into_response(),
    }
}

pub async fn start(ctx: Ctx, headers: HeaderMap) -> Response {
    let l = ctx.l;
    let res = ctx
        .call(AdminOp::UpgradeToLatest {
            version: None,
            force: false,
        })
        .await;
    match res {
        Ok(_) => ctx.done(l.t(
            "Downloading the release; the rollout starts once every binary is checked.",
            "릴리스를 받는 중입니다. 모든 바이너리를 검증하면 롤아웃을 시작합니다.",
        )),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

async fn render(ctx: &Ctx) -> anyhow::Result<Markup> {
    let v = match ctx.call(AdminOp::LatestRelease { check: true }).await? {
        AdminResponse::LatestRelease(v) => *v,
        _ => anyhow::bail!("unexpected response"),
    };
    let rollout = match ctx.call(AdminOp::RolloutStatus).await? {
        AdminResponse::Rollout(r) => r,
        _ => anyhow::bail!("unexpected response"),
    };
    Ok(markup(ctx, &v, &rollout))
}

fn busy(v: &LatestReleaseView, r: &RolloutView) -> bool {
    matches!(v.fetch, FetchState::Running { .. })
        || r.nodes.iter().any(|n| {
            matches!(
                n.state,
                UpgradeState::Waiting | UpgradeState::Draining | UpgradeState::Upgrading
            )
        })
}

fn markup(ctx: &Ctx, v: &LatestReleaseView, r: &RolloutView) -> Markup {
    let l = ctx.l;
    let behind: Vec<_> = match &v.latest {
        Some(tag) => r
            .nodes
            .iter()
            .filter(|n| is_older(&n.version, tag))
            .collect(),
        None => Vec::new(),
    };
    let cp_behind = v
        .latest
        .as_deref()
        .is_some_and(|t| is_older(&v.control_plane, t));
    let running = matches!(v.fetch, FetchState::Running { .. });
    // Reload while something moves, so the state below stays current.
    let trigger = if busy(v, r) { "every 3s" } else { "every 600s" };
    html! {
        section #upgrade hx-get="/upgrade" hx-trigger=(trigger) hx-swap="outerHTML" {
            h2 { (l.t("Upgrade", "업그레이드")) }
            dl class="facts" {
                dt { (l.t("Newest release", "최신 릴리스")) }
                dd {
                    @match &v.latest {
                        Some(tag) => { strong { (tag) } }
                        None => { "—" }
                    }
                    @if let Some(e) = &v.check_error {
                        " " span class="dim" title=(e) { (l.t("(could not check just now)", "(방금 확인하지 못함)")) }
                    }
                }
                dt { (l.t("Control plane", "컨트롤 플레인")) }
                dd { (v.control_plane) }
                dt { (l.t("Nodes behind", "업그레이드할 노드")) }
                dd {
                    @if v.latest.is_none() { "—" }
                    @else if behind.is_empty() { (l.t("none, all up to date", "없음, 모두 최신")) }
                    @else {
                        (behind.len()) ": "
                        (behind.iter().map(|n| format!("{} ({})", n.name, n.version)).collect::<Vec<_>>().join(", "))
                    }
                }
                @match &v.fetch {
                    FetchState::Idle => {}
                    FetchState::Running { version, step, .. } => {
                        dt { (l.t("Download", "다운로드")) } dd { (version) ": " (step) }
                    }
                    FetchState::Done { version, summary, finished_unix } => {
                        dt { (l.t("Last upgrade", "마지막 업그레이드")) } dd { (version) ": " (summary) " · " (fmt::ago(l, *finished_unix)) }
                    }
                    FetchState::Failed { version, reason, finished_unix } => {
                        dt { (l.t("Last upgrade", "마지막 업그레이드")) }
                        dd class="bad" { (version.as_deref().unwrap_or("")) " " (l.t("failed:", "실패:")) " " (reason) " · " (fmt::ago(l, *finished_unix)) }
                    }
                }
            }
            @if !r.releases.is_empty() {
                h3 { (l.t("Rollout", "롤아웃")) " · " (fmt::ago(l, r.started_unix)) }
                table {
                    thead { tr {
                        th { (l.t("Node", "노드")) } th { (l.t("Platform", "플랫폼")) }
                        th { (l.t("Version", "버전")) } th { (l.t("State", "상태")) }
                    } }
                    tbody {
                        @for n in &r.nodes {
                            tr {
                                td { (n.name) } td { (n.target.to_string()) }
                                td { (n.version) } td { (n.state.to_string()) }
                            }
                        }
                    }
                }
            }
            @if let Some(tag) = &v.latest {
                @if !running && (!behind.is_empty() || cp_behind) {
                    p {
                        button class="warn" hx-post="/upgrade" hx-target="#flash"
                            hx-confirm=(format!("{} {tag}?\n{}", l.t("Upgrade the cluster to", "클러스터를 다음 버전으로 업그레이드할까요:"),
                                l.t("The control plane downloads and checks the release, then nodes install it one at a time once idle (busy ones are drained first).",
                                    "컨트롤 플레인이 릴리스를 받아 검증한 뒤, 노드가 한가할 때 하나씩 설치합니다(바쁜 노드는 먼저 비웁니다).")))
                            { (l.t("Upgrade to", "업그레이드:")) " " (tag) }
                    }
                }
            }
            @if cp_behind {
                p class="dim" { (l.t(
                    "The control plane and dashboard restart into the new version by themselves once the rollout has settled (the agent on their machine installs it, or the control plane does when its machine has none).",
                    "컨트롤 플레인과 대시보드는 롤아웃이 끝나면 새 버전으로 스스로 재시작합니다(같은 PC의 에이전트가 설치하고, 에이전트가 없으면 컨트롤 플레인이 직접 설치).")) }
            }
        }
    }
}
