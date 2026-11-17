//! Settings (1.7): the browsers signed in, with Revoke, and for admins a
//! `cluster.toml` editor that shows what `apply` would change before it
//! changes it.

use std::fmt::Write as _;

use axum::{Form, extract::Path, http::HeaderMap, response::Response};
use maud::{Markup, html};
use pumasi_proto::{
    manage::{ApplySpec, ClusterFile, ScheduleView, SelectorOp},
    monitor::{AlertPolicy, AlertTuning},
    msg::{AdminOp, AdminResponse, NodeView},
    ui::SessionView,
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, role_text, status, table};
use crate::fmt;
use pumasi_proto::monitor::human_duration;

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
        let file = match export(&ctx).await {
            Ok(text) => editor(&ctx, &text),
            Err(e) => html! { div class="flash bad" { (format!("{e:#}")) } },
        };
        html! { (secrets_section(&ctx).await) (file) }
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

/// The cluster's secrets (1.15): names and when each was set, a form to
/// add one, and per secret a new value (rotate) or Remove. Values go to
/// the control plane and are never shown again.
async fn secrets_section(ctx: &Ctx) -> Markup {
    let l = ctx.l;
    let list = match ctx.call(AdminOp::SecretList).await {
        Ok(AdminResponse::Secrets(s)) => Ok(s),
        Ok(_) => Ok(Vec::new()),
        Err(e) => Err(format!("{e:#}")),
    };
    html! {
        section #secrets hx-get="/settings/secrets" hx-trigger="refresh from:body" hx-swap="outerHTML" hx-disinherit="*" {
            h2 { (l.t("Secrets", "시크릿")) }
            p class="dim" { (l.t(
                "Values the control plane keeps without anyone seeing them again: registry/HOST logs jobs in to a private registry (user:password or a token), and the login never leaves the control plane. A value is sent once and never shown.",
                "컨트롤 플레인이 다시 볼 수 없게 보관하는 값입니다. registry/호스트는 작업이 비공개 레지스트리에 로그인할 때 쓰고(user:password 또는 토큰), 로그인 정보는 컨트롤 플레인 밖으로 나가지 않습니다. 값은 한 번 보내면 다시 보이지 않습니다."
            )) }
            @match &list {
                Err(e) => div class="flash bad" { (e) },
                Ok(list) => {
                    (table(&[l.t("Name", "이름"), l.t("Set", "설정 시각"), l.t("New value", "새 값"), ""],
                        list.iter().map(|s| html! {
                            tr data-secret=(s.name) {
                                td { code { (s.name) } }
                                td { (fmt::time(s.set_unix)) }
                                td {
                                    form class="inline" hx-post="/settings/secrets" hx-target="#flash" {
                                        input type="hidden" name="name" value=(s.name);
                                        input type="password" name="value" required autocomplete="new-password"
                                            aria-label=(l.t("New value", "새 값"));
                                        " "
                                        button type="submit" name="action" value="rotate" { (l.t("Rotate", "교체")) }
                                    }
                                }
                                td {
                                    form class="inline" hx-post="/settings/secrets" hx-target="#flash"
                                        hx-confirm=(l.t("Remove this secret? Images that need it can no longer be pulled.", "이 시크릿을 지울까요? 이것이 필요한 이미지는 더 받을 수 없습니다.")) {
                                        input type="hidden" name="name" value=(s.name);
                                        button type="submit" class="link danger" name="action" value="remove" { (l.t("Remove", "지우기")) }
                                    }
                                }
                            }
                        }).collect(),
                        l.t("No secrets.", "시크릿이 없습니다.")))
                }
            }
            form #secret-form class="row" hx-post="/settings/secrets" hx-target="#flash" {
                div { label for="secret-name" { (l.t("Name", "이름")) }
                    input #secret-name name="name" required placeholder="registry/ghcr.io"; }
                div { label for="secret-value" { (l.t("Value", "값")) }
                    input #secret-value type="password" name="value" required autocomplete="new-password"; }
                div { button type="submit" name="action" value="add" { (l.t("Add secret", "시크릿 추가")) } }
            }
        }
    }
}

pub async fn secrets_fragment(ctx: Ctx) -> Response {
    axum::response::IntoResponse::into_response(axum::response::Html(
        secrets_section(&ctx).await.into_string(),
    ))
}

#[derive(Deserialize)]
pub struct SecretForm {
    name: String,
    value: Option<String>,
    action: Option<String>,
}

pub async fn secret(ctx: Ctx, headers: HeaderMap, Form(f): Form<SecretForm>) -> Response {
    let name = f.name.trim().to_string();
    let action = f.action.unwrap_or_else(|| "add".into());
    let res = async {
        anyhow::ensure!(!name.is_empty(), "a secret needs a name");
        let existing = match ctx.call(AdminOp::SecretList).await? {
            AdminResponse::Secrets(s) => s.iter().any(|s| s.name == name),
            _ => false,
        };
        let done = match action.as_str() {
            "remove" => {
                ctx.call(AdminOp::SecretRemove { name: name.clone() })
                    .await?;
                ctx.t("Removed secret", "시크릿을 지웠습니다:")
            }
            "add" | "rotate" => {
                let value = f.value.unwrap_or_default();
                anyhow::ensure!(!value.is_empty(), "a secret needs a value");
                // Add never replaces by accident; Rotate needs one to replace.
                if action == "add" {
                    anyhow::ensure!(
                        !existing,
                        "secret {name} exists: rotate it to replace its value"
                    );
                } else {
                    anyhow::ensure!(existing, "no such secret: {name}");
                }
                ctx.call(AdminOp::SecretSet {
                    name: name.clone(),
                    value,
                })
                .await?;
                if action == "add" {
                    ctx.t("Added secret", "시크릿을 추가했습니다:")
                } else {
                    ctx.t("Rotated secret", "시크릿을 교체했습니다:")
                }
            }
            _ => anyhow::bail!("no such action: {action}"),
        };
        anyhow::Ok(done)
    }
    .await;
    match res {
        Ok(done) => ctx.done(format!("{done} {name}")),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

fn editor(ctx: &Ctx, text: &str) -> Markup {
    let l = ctx.l;
    html! {
        section #cluster-file {
            h2 { "cluster.toml" }
            p class="dim" { (l.t(
                "The cluster's node labels, taints and cordons, its alert rules with thresholds per node ([alerts.nodes.NAME]) or label ([alerts.labels.\"os=windows\"]), and its schedules, as `pumasi apply` reads them. Change a schedule's cron, time zone or overlap here (its job stays); add [silences.NAME] with an until, [accounts.NAME], [webhooks.NAME] or more nodes, press Preview to see what would change, then Apply. New schedules need a job: add them on the Schedules page.",
                "노드 라벨·테인트·격리, 노드별([alerts.nodes.이름])·라벨별([alerts.labels.\"os=windows\"]) 임계값을 포함한 경보 규칙, 예약 작업을 `pumasi apply`가 읽는 형식으로 보여 줍니다. 예약의 cron·시간대·겹침은 여기서 바꿀 수 있고(작업은 그대로), until을 단 [silences.이름], [accounts.이름], [webhooks.이름]이나 노드를 더하고 미리 보기로 바뀔 내용을 확인한 뒤 적용하세요. 새 예약에는 작업이 필요하니 예약 페이지에서 추가합니다."
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
/// cordon, the alert rules with their per-node thresholds (1.15), and the
/// schedules without their jobs (1.15), which applying keeps.
async fn export(ctx: &Ctx) -> anyhow::Result<String> {
    let s = status(ctx).await?;
    let policy = match ctx.call(AdminOp::Alerts).await? {
        AdminResponse::Alerts(a) => a.policy,
        _ => AlertPolicy::default(),
    };
    // A control plane older than 1.15 cannot apply these: leave them out.
    let new = matches!(
        ctx.call(AdminOp::AlertTexts).await,
        Ok(AdminResponse::AlertTexts(_))
    );
    let tuning = match ctx.call(AdminOp::AlertTuning).await {
        Ok(AdminResponse::AlertTuning(t)) if new => Some(t.tuning),
        _ => None,
    };
    let schedules = match ctx.call(AdminOp::Schedules).await {
        Ok(AdminResponse::Schedules(s)) if tuning.is_some() => s,
        _ => Vec::new(),
    };
    Ok(cluster_toml(&s.nodes, &policy, tuning.as_ref(), &schedules))
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

pub fn cluster_toml(
    nodes: &[NodeView],
    p: &AlertPolicy,
    tuning: Option<&AlertTuning>,
    schedules: &[ScheduleView],
) -> String {
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
        if labels.is_empty() {
            out.push_str("labels = {}\n");
        } else {
            let _ = writeln!(out, "labels = {{ {} }}", labels.join(", "));
        }
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
    if let Some(t) = tuning {
        let _ = writeln!(
            out,
            "repeat = {}",
            t.repeat_secs.map_or_else(off, |s| q(&human_duration(s)))
        );
        let _ = writeln!(out, "resolved = {}", t.resolved);
        for o in &t.overrides {
            // `name=imac` alone is [alerts.nodes.imac]; anything else a
            // selector.
            let name = match o.nodes.0.as_slice() {
                [t] if t.key == "name" => match &t.op {
                    SelectorOp::In(v) if v.len() == 1 => Some(v[0].clone()),
                    _ => None,
                },
                _ => None,
            };
            match name {
                Some(n) => {
                    let _ = writeln!(out, "\n[alerts.nodes.{}]", key(&n));
                }
                None => {
                    let _ = writeln!(out, "\n[alerts.labels.{}]", q(&o.nodes.to_string()));
                }
            }
            if let Some(v) = o.node_offline_secs {
                let _ = writeln!(
                    out,
                    "node_offline = {}",
                    v.map_or_else(off, |s| q(&human_duration(s)))
                );
            }
            if let Some(v) = o.disk_percent {
                let _ = writeln!(out, "disk = {}", pct(v));
            }
            if let Some(v) = o.temp_c {
                let _ = writeln!(
                    out,
                    "temperature = {}",
                    v.map_or_else(off, |t| t.to_string())
                );
            }
        }
    }
    let mut schedules: Vec<&ScheduleView> = schedules.iter().collect();
    schedules.sort_by(|a, b| a.name.cmp(&b.name));
    for s in schedules {
        // The time zone comes in the same field: `0 3 * * * (Asia/Seoul)`.
        let (cron, tz) = match s.cron.strip_suffix(')').and_then(|c| c.rsplit_once(" (")) {
            Some((c, tz)) => (c, Some(tz)),
            None => (s.cron.as_str(), None),
        };
        let _ = writeln!(out, "\n[schedules.{}]", key(&s.name));
        let _ = writeln!(out, "cron = {}", q(cron));
        if let Some(tz) = tz {
            let _ = writeln!(out, "tz = {}", q(tz));
        }
        let _ = writeln!(out, "overlap = {}", s.overlap);
    }
    out
}

/// A `cluster.toml` sent from a browser or the API, as `apply` takes it.
/// It may not name job files or environment variables (`url_env`): both
/// would be read on the dashboard's machine. A schedule without a job
/// keeps the one it has (1.15). It never prunes, since it need not list
/// every schedule, webhook and silence.
pub fn apply_spec(text: &str, dry_run: bool) -> anyhow::Result<ApplySpec> {
    let file = ClusterFile::parse(text)?;
    anyhow::ensure!(
        file.schedules.values().all(|s| s.job.is_none()),
        "a schedule's job names a job file, which the dashboard cannot read: leave `job` out to keep the schedule's job, or add the job on the Schedules page"
    );
    anyhow::ensure!(
        file.webhooks.values().all(|w| w.url_env.is_none()),
        "url_env reads this dashboard's environment: give the webhook's url"
    );
    // Nothing is read from this directory: job files are refused.
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
        match ctx.call(AdminOp::apply(spec)).await? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use pumasi_proto::monitor::FailureRate;

    #[test]
    fn the_exported_alerts_read_back_the_same() {
        for p in [
            AlertPolicy::default(),
            AlertPolicy {
                node_offline_secs: None,
                failure_rate: None,
                disk_percent: Some(80),
                temp_c: None,
                perf_drop_percent: None,
                smart: false,
                check_mismatch: false,
            },
            AlertPolicy {
                node_offline_secs: Some(150),
                failure_rate: Some(FailureRate {
                    percent: 30,
                    window_secs: 7200,
                    min_runs: 4,
                }),
                ..Default::default()
            },
        ] {
            let text = cluster_toml(&[], &p, None, &[]);
            let spec = apply_spec(&text, true).unwrap();
            assert_eq!(spec.alerts, Some(p), "{text}");
            assert!(spec.dry_run && !spec.prune);
        }
    }

    #[test]
    fn tuning_and_schedules_read_back_the_same() {
        use pumasi_proto::monitor::AlertOverride;
        let tuning = AlertTuning {
            repeat_secs: Some(3600),
            resolved: false,
            overrides: vec![
                AlertOverride {
                    nodes: "name=imac".parse().unwrap(),
                    node_offline_secs: None,
                    disk_percent: Some(Some(95)),
                    temp_c: Some(None),
                },
                AlertOverride {
                    nodes: "os=windows,gpu".parse().unwrap(),
                    node_offline_secs: Some(Some(600)),
                    disk_percent: None,
                    temp_c: Some(Some(90)),
                },
            ],
        };
        let sched = |name: &str, cron: &str, overlap| ScheduleView {
            name: name.into(),
            cron: cron.into(),
            account: "owner".into(),
            job_name: "j".into(),
            tasks: 1,
            priority: 0,
            overlap,
            next_unix: None,
            last_unix: None,
            last_job: None,
            last_error: None,
            runs: 0,
        };
        let schedules = [
            sched("nightly", "0 3 * * * (Asia/Seoul)", false),
            sched("hourly", "@hourly", true),
        ];
        let text = cluster_toml(&[], &AlertPolicy::default(), Some(&tuning), &schedules);
        let spec = apply_spec(&text, true).unwrap();
        assert_eq!(spec.more().alert_tuning, Some(tuning), "{text}");
        assert_eq!(spec.keep_jobs.len(), 2, "{text}");
        let nightly = spec.schedules.iter().find(|s| s.name == "nightly").unwrap();
        assert_eq!(nightly.cron, "0 3 * * *");
        assert_eq!(nightly.tz.as_deref(), Some("Asia/Seoul"));
        assert!(!nightly.overlap);
        let hourly = spec.schedules.iter().find(|s| s.name == "hourly").unwrap();
        assert_eq!(
            (hourly.cron.as_str(), hourly.tz.as_deref(), hourly.overlap),
            ("@hourly", None, true)
        );
    }

    #[test]
    fn files_that_would_read_the_dashboards_machine_are_refused() {
        let sched = "[schedules.nightly]\ncron = \"@daily\"\njob = \"/etc/passwd\"\n";
        let e = apply_spec(sched, true).unwrap_err();
        assert!(format!("{e:#}").contains("Schedules page"), "{e:#}");
        let env = "[webhooks.chat]\nurl_env = \"HOME\"\n";
        let e = apply_spec(env, false).unwrap_err();
        assert!(format!("{e:#}").contains("url_env"), "{e:#}");
        let ok = "[webhooks.chat]\nurl = \"https://hooks.slack.com/services/x\"\n\n[nodes.\"my pc\"]\nlabels = { room = \"study\" }\ntaints = [\"busy=true\"]\n";
        let spec = apply_spec(ok, false).unwrap();
        assert_eq!(spec.webhooks.len(), 1);
        assert_eq!(spec.nodes[0].node, "my pc");
        assert!(!spec.dry_run);
    }

    #[test]
    fn odd_names_are_quoted() {
        assert_eq!(key("imac"), "imac");
        assert_eq!(key("my pc"), "\"my pc\"");
        assert_eq!(key("a.b"), "\"a.b\"");
    }
}
