//! The upgrade panel on the Nodes page (1.3): the newest published
//! release, the nodes' versions, and a button that has the control plane
//! download that release and roll it out (`pumasi upgrade latest`).

use axum::{
    Form,
    extract::rejection::FormRejection,
    http::HeaderMap,
    response::{Html, IntoResponse, Response},
};
use maud::{Markup, html};
use pumasi_proto::{
    job::parse_duration_ms,
    manage::Selector,
    msg::{AdminOp, AdminResponse, RolloutView, UpgradeState},
    release::{
        CanaryPhase, CanarySpec, CanaryView, DEFAULT_CANARY_SOAK_SECS, FetchState,
        LatestReleaseView, is_older,
    },
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

/// The canary a start form asks for: `canary` once per node checked,
/// `soak` and `tasks`. `None` when no node is checked.
pub fn canary_from(form: &[(String, String)]) -> anyhow::Result<Option<CanarySpec>> {
    let get = |k: &str| {
        form.iter()
            .find(|(n, v)| n == k && !v.trim().is_empty())
            .map(|(_, v)| v.trim())
    };
    let nodes: Vec<&str> = form
        .iter()
        .filter(|(k, v)| k == "canary" && !v.trim().is_empty())
        .map(|(_, v)| v.trim())
        .collect();
    if nodes.is_empty() {
        return Ok(None);
    }
    let spec = CanarySpec {
        nodes: Selector::nodes(&nodes.join(","))?,
        soak_secs: match get("soak") {
            Some(s) => parse_duration_ms(s)? / 1000,
            None => DEFAULT_CANARY_SOAK_SECS,
        },
        min_tasks: match get("tasks") {
            Some(t) => t
                .parse()
                .map_err(|_| anyhow::anyhow!("tasks: a whole number"))?,
            None => 0,
        },
        hold: get("hold").is_some(),
    };
    spec.validate()?;
    Ok(Some(spec))
}

pub async fn start(
    ctx: Ctx,
    headers: HeaderMap,
    form: Result<Form<Vec<(String, String)>>, FormRejection>,
) -> Response {
    let l = ctx.l;
    let form = form.map(|Form(f)| f).unwrap_or_default();
    let canary = match canary_from(&form) {
        Ok(c) => c,
        Err(e) => return ctx.fail(e, hx(&headers)),
    };
    let op = match canary {
        Some(c) => AdminOp::CanaryUpgradeToLatest {
            version: None,
            force: false,
            canary: Box::new(c),
        },
        None => AdminOp::UpgradeToLatest {
            version: None,
            force: false,
        },
    };
    match ctx.call(op).await {
        Ok(_) => ctx.done(l.t(
            "Downloading the release; the rollout starts once every binary is checked.",
            "릴리스를 받는 중입니다. 모든 바이너리를 검증하면 롤아웃을 시작합니다.",
        )),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

/// Upgrades the nodes checked next (`node` once each), or every node
/// when none is.
pub async fn promote(
    ctx: Ctx,
    headers: HeaderMap,
    form: Result<Form<Vec<(String, String)>>, FormRejection>,
) -> Response {
    let nodes: Vec<String> = form
        .map(|Form(f)| f)
        .unwrap_or_default()
        .into_iter()
        .filter(|(k, v)| k == "node" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
        .collect();
    let msg = if nodes.is_empty() {
        ctx.t(
            "Promoted: every node gets the release now.",
            "승격했습니다. 이제 모든 노드가 릴리스를 받습니다.",
        )
        .to_string()
    } else {
        format!(
            "{} {}",
            ctx.t("Upgrading next:", "다음으로 업그레이드:"),
            nodes.join(", ")
        )
    };
    match ctx.call(AdminOp::PromoteCanary { nodes }).await {
        Ok(_) => upgrade_refresh(ctx.done(msg)),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn cancel(ctx: Ctx, headers: HeaderMap) -> Response {
    match ctx.call(AdminOp::CancelRollout).await {
        Ok(_) => {
            let msg = ctx
                .t(
                    "Rollout cancelled; nodes keep the version they have.",
                    "롤아웃을 취소했습니다. 노드는 지금 버전을 유지합니다.",
                )
                .to_string();
            upgrade_refresh(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

/// The panel reloads on its own trigger too.
fn upgrade_refresh(mut r: Response) -> Response {
    r.headers_mut().insert(
        "hx-trigger",
        axum::http::HeaderValue::from_static("refresh, upgrade-changed"),
    );
    r
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
    // A control plane older than 1.7 has no canaries.
    let canary = match ctx.call(AdminOp::Canary).await {
        Ok(AdminResponse::Canary(c)) => c.map(|c| *c),
        _ => None,
    };
    Ok(markup(ctx, &v, &rollout, canary.as_ref()))
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

fn markup(ctx: &Ctx, v: &LatestReleaseView, r: &RolloutView, c: Option<&CanaryView>) -> Markup {
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
    // A halted canary moves only when an admin acts: no reload that would
    // clear the nodes checked below.
    let halted = c.is_some_and(|c| matches!(c.phase, CanaryPhase::Halted { .. }));
    let trigger = if !halted && (busy(v, r) || c.is_some_and(|c| c.phase == CanaryPhase::Canary)) {
        "every 3s, upgrade-changed from:body"
    } else {
        "every 600s, upgrade-changed from:body"
    };
    html! {
        section #upgrade hx-get="/upgrade" hx-trigger=(trigger) hx-swap="outerHTML" hx-disinherit="*" {
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
            @if let Some(c) = c { (canary(ctx, c)) }
            @if let Some(tag) = &v.latest {
                @if !running && (!behind.is_empty() || cp_behind) {
                    form #upgrade-form class="stack" {
                        @if behind.len() > 1 {
                            fieldset class="canary-pick" {
                                legend { (l.t("Canary (optional): upgrade these first", "카나리(선택): 먼저 업그레이드할 노드")) }
                                @for n in &behind {
                                    label class="check" { input type="checkbox" name="canary" value=(n.name); " " (n.name) " " span class="dim" { (n.version) } }
                                }
                                div class="row" {
                                    label { (l.t("then wait", "그 뒤 대기")) " " input name="soak" value=(fmt::human_duration(DEFAULT_CANARY_SOAK_SECS)) size="5"; }
                                    label { (l.t("and tasks each", "노드당 작업 수")) " " input name="tasks" type="number" min="0" value="0" size="4"; }
                                    label class="check" { input type="checkbox" name="hold" value="on"; " " (l.t("wait for me to promote", "직접 승격할 때까지 대기")) }
                                }
                            }
                        }
                        button type="button" class="warn" hx-post="/upgrade" hx-target="#flash" hx-include="closest form"
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

fn canary(ctx: &Ctx, c: &CanaryView) -> Markup {
    let l = ctx.l;
    html! {
        div #canary class="canary" data-phase=(match &c.phase { CanaryPhase::Canary => "canary", CanaryPhase::Promoted { .. } => "promoted", CanaryPhase::Halted { .. } => "halted" }) {
            h3 { (l.t("Canary", "카나리")) " · " code { (c.spec.nodes.to_string()) } }
            p {
                @match &c.phase {
                    CanaryPhase::Canary => {
                        (l.t("Upgrading these first; the rest wait.", "이 노드를 먼저 업그레이드하고, 나머지는 기다립니다."))
                        @if let Some(w) = &c.waiting_for { " " span class="dim" { "(" (w) ")" } }
                    }
                    CanaryPhase::Promoted { unix, by } => {
                        (l.t("Promoted: every node gets the release.", "승격됨: 모든 노드가 릴리스를 받습니다.")) " " span class="dim" { (by) " · " (fmt::ago(l, *unix)) }
                    }
                    CanaryPhase::Halted { reason } => {
                        span class="bad" { (l.t("Halted:", "멈춤:")) " " (reason) }
                    }
                }
            }
            table {
                thead { tr { th { (l.t("Canary", "카나리")) } th { (l.t("Upgraded", "업그레이드")) } th { (l.t("Tasks ok / failed", "작업 성공 / 실패")) } } }
                tbody {
                    @for n in &c.canaries {
                        tr { td { (n.name) } td { @match n.upgraded_unix { Some(t) => (fmt::ago(l, t)), None => "—" } } td { (n.tasks_ok) " / " (n.tasks_failed) } }
                    }
                }
            }
            @if !matches!(c.phase, CanaryPhase::Promoted { .. }) {
                @if !c.held.is_empty() {
                    form #promote-form class="stack" hx-post="/upgrade/promote" hx-target="#flash" {
                        fieldset {
                            legend { (l.t("Waiting for the canary", "카나리를 기다리는 노드")) }
                            @for n in &c.held {
                                label class="check" { input type="checkbox" name="node" value=(n); " " (n) }
                            }
                        }
                        div class="row" {
                            button type="submit" { (l.t("Upgrade checked next", "선택한 노드 업그레이드")) }
                        }
                    }
                }
                div class="row" {
                    button class="warn" hx-post="/upgrade/promote" hx-target="#flash"
                        hx-confirm=(l.t("Promote the release to every node now?", "지금 모든 노드에 릴리스를 적용할까요?")) { (l.t("Promote all", "모두 승격")) }
                    " "
                    button class="danger" hx-post="/upgrade/cancel" hx-target="#flash"
                        hx-confirm=(l.t("Cancel the rollout? Upgraded nodes keep the new version.", "롤아웃을 취소할까요? 업그레이드한 노드는 새 버전을 유지합니다.")) { (l.t("Cancel rollout", "롤아웃 취소")) }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn form(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn checked_nodes_become_the_canary() {
        assert_eq!(canary_from(&form(&[("soak", "30m")])).unwrap(), None);
        let c = canary_from(&form(&[
            ("canary", "node-a"),
            ("canary", "node-b"),
            ("soak", "1h"),
            ("tasks", "3"),
            ("hold", "on"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(c.nodes, Selector::nodes("node-a,node-b").unwrap());
        assert_eq!((c.soak_secs, c.min_tasks, c.hold), (3600, 3, true));
        let c = canary_from(&form(&[("canary", "node-a")]))
            .unwrap()
            .unwrap();
        assert_eq!(c.soak_secs, DEFAULT_CANARY_SOAK_SECS);
        assert!(canary_from(&form(&[("canary", "node-a"), ("tasks", "many")])).is_err());
    }
}
