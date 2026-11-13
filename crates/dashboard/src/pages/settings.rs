//! Settings (1.7): the browsers signed in, with Revoke, and for admins a
//! `cluster.toml` editor that shows what `apply` would change before it
//! changes it.

use std::fmt::Write as _;

use axum::{Form, extract::Path, http::HeaderMap, response::Response};
use maud::{Markup, html};
use pumasi_proto::{
    manage::{ApplySpec, ClusterFile},
    monitor::AlertPolicy,
    msg::{AdminOp, AdminResponse, NodeView},
    ui::SessionView,
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, role_text, status, table};
use crate::fmt;

pub async fn page(ctx: Ctx) -> Response {
    let l = ctx.l;
    let sessions: Option<Vec<SessionView>> = match ctx.call(AdminOp::Sessions).await {
        Ok(AdminResponse::Sessions(s)) => Some(s),
        // A control plane older than 1.7 keeps no sessions.
        _ => None,
    };
    let rows = sessions
        .iter()
        .flatten()
        .map(|s| {
            html! {
                tr data-session=(s.id) {
                    td { code { (s.id) } }
                    td { (s.name) " · " (role_text(l, &s.role)) }
                    td { (s.label) }
                    td { (fmt::time(s.created_unix)) }
                    td { (fmt::ago(l, s.last_used_unix)) }
                    td {
                        @if s.current {
                            span class="badge" { (l.t("this browser", "이 브라우저")) } " "
                            button class="link" hx-post="/logout" { (l.t("Sign out", "로그아웃")) }
                        } @else {
                            button class="link danger" hx-post={"/settings/sessions/" (s.id) "/revoke"} hx-target="#flash"
                                hx-confirm=(l.t("Sign this browser out?", "이 브라우저를 로그아웃시킬까요?")) { (l.t("Revoke", "취소")) }
                        }
                    }
                }
            }
        })
        .collect();
    let admin = ctx.me.is_admin();
    let editor = if admin {
        match export(&ctx).await {
            Ok(text) => editor(&ctx, &text),
            Err(e) => html! { div class="flash bad" { (format!("{e:#}")) } },
        }
    } else {
        html! {}
    };
    let top = html! {
        div class="page-head" { h1 { (l.t("Settings", "설정")) } }
    };
    let live = html! {
        section {
            h2 { (l.t("Signed-in browsers", "로그인한 브라우저")) }
            @match &sessions {
                None => p class="empty" { (l.t(
                    "This control plane is older than 1.7: sign-ins live in this dashboard's memory and end when it restarts.",
                    "컨트롤 플레인이 1.7보다 오래되어 로그인이 이 대시보드의 메모리에만 있고, 다시 시작하면 끝납니다.")) },
                Some(_) => {
                    p class="dim" { (if admin {
                        l.t("Every browser signed in to this cluster. A session ends after 12 hours unused, when revoked, or when its ticket is.",
                            "이 클러스터에 로그인한 모든 브라우저입니다. 12시간 쓰지 않거나, 취소하거나, 그 티켓이 취소되면 끝납니다.")
                    } else {
                        l.t("The browsers signed in with your ticket. A session ends after 12 hours unused.",
                            "내 티켓으로 로그인한 브라우저입니다. 12시간 쓰지 않으면 끝납니다.")
                    }) }
                    (table(&[l.t("Session", "세션"), l.t("Who", "누구"), l.t("Browser", "브라우저"), l.t("Signed in", "로그인"), l.t("Last used", "마지막 사용"), ""],
                        rows, l.t("No sessions.", "세션이 없습니다.")))
                }
            }
        }
    };
    ctx.page(
        l.t("Settings", "설정"),
        Nav::Settings,
        top,
        Some(("30s", live)),
        editor,
    )
}

fn editor(ctx: &Ctx, text: &str) -> Markup {
    let l = ctx.l;
    html! {
        section #cluster-file {
            h2 { "cluster.toml" }
            p class="dim" { (l.t(
                "The cluster's node labels, taints and cordons and its alert rules, as `pumasi apply` reads them. Add [accounts.NAME] tables, [webhooks.NAME] or more nodes, press Preview to see what would change, then Apply. Schedules name job files: add them on the Schedules page or with `pumasi apply`.",
                "노드 라벨·테인트·격리와 경보 규칙을 `pumasi apply`가 읽는 형식으로 보여 줍니다. [accounts.이름], [webhooks.이름]이나 노드를 더하고 미리 보기로 바뀔 내용을 확인한 뒤 적용하세요. 예약 작업은 작업 파일을 가리키므로 예약 페이지나 `pumasi apply`로 추가합니다."
            )) }
            form #apply-form class="stack" hx-post="/settings/apply" hx-target="#apply-result" {
                textarea name="text" rows="18" spellcheck="false" { (text) }
                div class="row" {
                    button type="submit" name="mode" value="preview" { (l.t("Preview", "미리 보기")) }
                    button type="submit" name="mode" value="apply" class="primary"
                        hx-confirm=(l.t("Apply this file to the cluster?", "이 파일을 클러스터에 적용할까요?")) { (l.t("Apply", "적용")) }
                }
            }
            div #apply-result {}
        }
    }
}

/// The cluster as a `cluster.toml`: every node's labels, taints and
/// cordon, and the alert rules.
async fn export(ctx: &Ctx) -> anyhow::Result<String> {
    let s = status(ctx).await?;
    let policy = match ctx.call(AdminOp::Alerts).await? {
        AdminResponse::Alerts(a) => a.policy,
        _ => AlertPolicy::default(),
    };
    Ok(cluster_toml(&s.nodes, &policy))
}

fn q(s: &str) -> String {
    toml::Value::String(s.to_string()).to_string()
}

fn key(s: &str) -> String {
    if !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        s.to_string()
    } else {
        q(s)
    }
}

pub fn cluster_toml(nodes: &[NodeView], p: &AlertPolicy) -> String {
    let mut out = String::new();
    let mut nodes: Vec<&NodeView> = nodes.iter().collect();
    nodes.sort_by(|a, b| a.info.name.cmp(&b.info.name));
    for n in nodes {
        let c = &n.config;
        let _ = writeln!(out, "[nodes.{}]", key(&n.info.name));
        let labels: Vec<String> = c
            .labels
            .iter()
            .map(|(k, v)| format!("{} = {}", key(k), q(v)))
            .collect();
        let _ = writeln!(out, "labels = {{ {} }}", labels.join(", "));
        let taints: Vec<String> = c.taints.iter().map(|t| q(&t.to_string())).collect();
        let _ = writeln!(out, "taints = [{}]", taints.join(", "));
        let _ = writeln!(out, "cordoned = {}\n", c.cordoned);
    }
    let off = || "\"off\"".to_string();
    let secs = |s: Option<u64>| s.map_or_else(off, |s| q(&format!("{s}s")));
    let pct = |v: Option<u8>| v.map_or_else(off, |v| q(&format!("{v}%")));
    out.push_str("[alerts]\n");
    let _ = writeln!(out, "node_offline = {}", secs(p.node_offline_secs));
    match p.failure_rate {
        Some(f) => {
            let _ = writeln!(out, "failure_rate = {}", q(&format!("{}%", f.percent)));
            let _ = writeln!(
                out,
                "failure_window = {}",
                q(&format!("{}s", f.window_secs))
            );
            let _ = writeln!(out, "failure_min_runs = {}", f.min_runs);
        }
        None => {
            let _ = writeln!(out, "failure_rate = {}", off());
        }
    }
    let _ = writeln!(out, "disk = {}", pct(p.disk_percent));
    let _ = writeln!(
        out,
        "temperature = {}",
        p.temp_c.map_or_else(off, |t| t.to_string())
    );
    let _ = writeln!(out, "perf_drop = {}", pct(p.perf_drop_percent));
    let _ = writeln!(out, "smart = {}", p.smart);
    let _ = writeln!(out, "check_mismatch = {}", p.check_mismatch);
    out
}

/// A `cluster.toml` sent from a browser or the API, as `apply` takes it.
/// It may not name job files (schedules) or environment variables
/// (`url_env`): both would be read on the dashboard's machine. It never
/// prunes, since it need not list every schedule and webhook.
pub fn apply_spec(text: &str, dry_run: bool) -> anyhow::Result<ApplySpec> {
    let file = ClusterFile::parse(text)?;
    anyhow::ensure!(
        file.schedules.is_empty(),
        "schedules name job files, which the dashboard cannot read: add them on the Schedules page or with `pumasi apply`"
    );
    anyhow::ensure!(
        file.webhooks.values().all(|w| w.url_env.is_none()),
        "url_env reads this dashboard's environment: give the webhook's url"
    );
    // Nothing is read from this directory: schedules are refused.
    let mut spec = file.resolve(std::path::Path::new("."), 0)?;
    spec.dry_run = dry_run;
    spec.prune = false;
    Ok(spec)
}

#[derive(Deserialize)]
pub struct ApplyForm {
    text: String,
    mode: Option<String>,
}

pub async fn apply(ctx: Ctx, headers: HeaderMap, Form(f): Form<ApplyForm>) -> Response {
    let dry_run = f.mode.as_deref() != Some("apply");
    let res = async {
        let spec = apply_spec(&f.text, dry_run)?;
        match ctx.call(AdminOp::Apply(Box::new(spec))).await? {
            AdminResponse::Applied(c) => Ok(c),
            _ => anyhow::bail!("unexpected response"),
        }
    }
    .await;
    let l = ctx.l;
    match res {
        Ok(changes) => {
            let body = html! {
                div class={"flash " (if dry_run { "info" } else { "ok" })} role="status" data-applied=(if dry_run { "preview" } else { "applied" }) {
                    @if changes.is_empty() {
                        (l.t("Nothing to change.", "바꿀 것이 없습니다."))
                    } @else {
                        strong { (if dry_run { l.t("Apply would change:", "적용하면 바뀌는 것:") } else { l.t("Changed:", "바꾼 것:") }) }
                        ul class="diff" { @for c in &changes { li { code { (c) } } } }
                    }
                }
            };
            let mut r = axum::response::IntoResponse::into_response(axum::response::Html(
                body.into_string(),
            ));
            if !dry_run {
                r.headers_mut().insert(
                    "hx-trigger",
                    axum::http::HeaderValue::from_static("refresh"),
                );
            }
            r
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn revoke(ctx: Ctx, headers: HeaderMap, Path(id): Path<String>) -> Response {
    match ctx.call(AdminOp::RevokeSession { id: id.clone() }).await {
        Ok(_) => {
            ctx.st.sessions.forget_id(&id);
            let msg = format!("{} {id}", ctx.t("Signed out:", "로그아웃시켰습니다:"));
            ctx.done(msg)
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}
