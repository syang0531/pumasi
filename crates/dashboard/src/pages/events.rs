//! Cluster events (newest first, live) and alerts: rules, what fires,
//! and the webhooks they go to.

use axum::{
    Form,
    extract::{Path, Query},
    http::HeaderMap,
    response::Response,
};
use maud::html;
use pumasi_proto::{
    job::parse_duration_ms,
    manage::Selector,
    monitor::{ALERT_RULES, AlertTuningView, AlertsView, EventView, SilenceSpec, human_duration},
    msg::{AdminOp, AdminResponse},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, table};
use crate::fmt;
use crate::i18n::Lang;

#[derive(Deserialize)]
pub struct EventsQuery {
    level: Option<String>,
}

pub async fn page(ctx: Ctx, Query(q): Query<EventsQuery>) -> Response {
    let l = ctx.l;
    let level = q.level.unwrap_or_default();
    let events: Vec<EventView> = match ctx
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 300,
        })
        .await
    {
        Ok(AdminResponse::Events(e)) => e,
        Ok(_) => Vec::new(),
        Err(e) => return ctx.fail(e, false),
    };
    let alerts = match ctx.call(AdminOp::Alerts).await {
        Ok(AdminResponse::Alerts(a)) => *a,
        _ => AlertsView::default(),
    };
    // Repeat notices, per-node thresholds and silences (1.6).
    let tuning: Option<AlertTuningView> = match ctx.call(AdminOp::AlertTuning).await {
        Ok(AdminResponse::AlertTuning(t)) => Some(*t),
        _ => None,
    };
    let silenced = |rule: &str, subject: &str| {
        tuning
            .as_ref()
            .is_some_and(|t| t.silenced.iter().any(|(r, s)| r == rule && s == subject))
    };
    let admin = ctx.me.is_admin();
    let rows = events
        .iter()
        .rev()
        .filter(|e| level.is_empty() || e.level.to_string() == level)
        .map(|e| {
            html! {
                tr class={"level-" (e.level)} {
                    td { (fmt::time(e.unix)) }
                    td { span class={"badge lvl-" (e.level)} { (e.level.to_string()) } }
                    td { code { (e.kind) } }
                    td { (e.node.clone().unwrap_or_default()) }
                    td { @if let Some(j) = e.job { a href={"/jobs/" (j)} { code { (j) } } } }
                    td { (e.message) }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Events and alerts", "이벤트와 경보")) } }
        nav class="tabs" {
            @for (v, label) in [("", l.t("All", "전체")), ("warning", l.t("Warnings", "경고")), ("critical", l.t("Critical", "심각"))] {
                a href={"/events?level=" (v)} class=[(v == level).then_some("active")] { (label) }
            }
        }
    };
    let live = html! {
        @if !alerts.firing.is_empty() {
            section {
                h2 { (l.t("Firing", "발생 중")) }
                ul class="alerts" {
                    @for a in &alerts.firing {
                        li class={"level-" (a.level)} {
                            strong { (a.rule) } " · " (a.subject) " — " (a.message) " " span class="dim" { (fmt::ago(l, a.since_unix)) }
                            @if silenced(&a.rule, &a.subject) { " " span class="badge" { (l.t("silenced", "묵음")) } }
                        }
                    }
                }
            }
        }
        (table(&[l.t("Time", "시각"), l.t("Level", "수준"), l.t("Kind", "종류"), l.t("Node", "노드"), l.t("Job", "작업"), l.t("Message", "내용")],
            rows, l.t("No events.", "이벤트가 없습니다.")))
    };
    let bottom = html! {
        div class="cols" {
            section {
                h2 { (l.t("Alert rules", "경보 규칙")) }
                dl class="facts" {
                    @for (rule, text) in alerts.policy.describe_in(l == Lang::Ko) { dt { code { (rule) } } dd { (text) } }
                    @if let Some(t) = &tuning {
                        dt { code { "repeat" } }
                        dd { @match t.tuning.repeat_secs {
                            Some(r) => { (l.t("every", "")) " " (human_duration(r)) " " (l.t("while firing", "마다 다시 알림")) }
                            None => { (l.t("off (one notice per alert)", "꺼짐 (경보마다 한 번)")) }
                        } }
                        dt { code { "resolved" } }
                        dd { @if t.tuning.resolved { (l.t("a notice when an alert stops firing", "경보가 그치면 알림")) } @else { (l.t("off", "꺼짐")) } }
                        @for o in &t.tuning.overrides {
                            dt { code { (o.nodes.to_string()) } }
                            dd { (o.describe(l == Lang::Ko)) }
                        }
                    }
                }
                p class="dim" { (l.t("Change them with `pumasi alert set` or `[alerts]` in cluster.toml; `pumasi alert set --on NODE` sets one node's thresholds.", "`pumasi alert set`이나 cluster.toml의 `[alerts]`로 바꿉니다. 노드 하나의 기준은 `pumasi alert set --on 노드`로 바꿉니다.")) }
            }
            @if let Some(t) = &tuning {
                section {
                    h2 { (l.t("Silences", "묵음")) }
                    @if t.silences.is_empty() { p class="empty" { (l.t("None: every alert sends its notices.", "없습니다. 모든 경보가 알림을 보냅니다.")) } }
                    ul class="plain" {
                        @for s in &t.silences {
                            li {
                                strong { (s.describe()) } " · " (l.t("until", "까지:")) " " (fmt::time(s.until_unix)) " · " (s.by)
                                @if !s.comment.is_empty() { br; span class="dim" { (s.comment) } }
                                @if admin { " " button hx-post={"/events/silences/" (s.id) "/end"} hx-target="#flash" { (l.t("End", "끝내기")) } }
                            }
                        }
                    }
                    @if admin {
                        form class="stack" hx-post="/events/silences" hx-target="#flash" {
                            label { (l.t("Rule", "규칙")) " "
                                select name="rule" {
                                    option value="" { (l.t("every rule", "모든 규칙")) }
                                    @for r in ALERT_RULES { option value=(r) { (r) } }
                                }
                            }
                            label { (l.t("Nodes (name or selector, empty: all)", "노드 (이름이나 선택자, 비우면 전체)")) " " input name="on" placeholder="imac, os=windows"; }
                            label { (l.t("For", "기간")) " " input name="for" value="2h" size="6"; }
                            label { (l.t("Comment", "메모")) " " input name="comment"; }
                            button { (l.t("Silence", "묵음 설정")) }
                        }
                    }
                }
            }
            section {
                h2 { (l.t("Webhooks", "웹훅")) }
                @if alerts.webhooks.is_empty() { p class="empty" { (l.t("None. Add one with `pumasi webhook add`.", "없습니다. `pumasi webhook add`로 추가하세요.")) } }
                ul class="plain" {
                    @for w in &alerts.webhooks {
                        li {
                            strong { (w.name) } " · " code { (w.target) } " · " (format!("{:?}", w.format).to_lowercase())
                            br; span class="dim" { (w.sent) " " (l.t("sent", "전송")) ", " (w.failed) " " (l.t("failed", "실패"))
                                @if let Some(e) = &w.last_error { " — " (e) } }
                            @if admin { " " button hx-post={"/events/webhooks/" (w.name) "/test"} hx-target="#flash" { (l.t("Send a test", "테스트 보내기")) } }
                        }
                    }
                }
            }
        }
    };
    ctx.page(
        l.t("Events", "이벤트"),
        Nav::Events,
        top,
        Some(("5s", live)),
        bottom,
    )
}

/// Sends a test alert to a webhook (1.6).
pub async fn test_webhook(ctx: Ctx, headers: HeaderMap, Path(name): Path<String>) -> Response {
    match ctx.call(AdminOp::TestWebhook { name: name.clone() }).await {
        Ok(_) => {
            let msg = format!(
                "{} {name}",
                ctx.l
                    .t("Test alert delivered to", "테스트 경보를 보냈습니다:")
            );
            ctx.done(msg)
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

#[derive(Deserialize, Default)]
pub struct SilenceForm {
    rule: Option<String>,
    on: Option<String>,
    #[serde(rename = "for")]
    duration: Option<String>,
    comment: Option<String>,
}

/// A node name, or a label selector (`os=windows`).
pub fn node_target(s: &str) -> anyhow::Result<Selector> {
    let s = s.trim();
    let sel: Selector = if s.contains(['=', ',', '!']) {
        s.parse()?
    } else {
        format!("name={s}").parse()?
    };
    sel.validate()?;
    Ok(sel)
}

/// Turns a silence form into a spec.
pub fn silence_spec(
    rule: Option<String>,
    on: Option<String>,
    duration: Option<String>,
    comment: Option<String>,
) -> anyhow::Result<SilenceSpec> {
    let spec = SilenceSpec {
        rule: rule.filter(|r| !r.trim().is_empty()),
        nodes: on
            .as_deref()
            .filter(|o| !o.trim().is_empty())
            .map(node_target)
            .transpose()?,
        secs: parse_duration_ms(duration.as_deref().unwrap_or("2h"))? / 1000,
        comment: comment.unwrap_or_default().trim().to_string(),
    };
    spec.validate()?;
    Ok(spec)
}

pub async fn silence(ctx: Ctx, headers: HeaderMap, Form(f): Form<SilenceForm>) -> Response {
    let spec = match silence_spec(f.rule, f.on, f.duration, f.comment) {
        Ok(s) => s,
        Err(e) => return ctx.fail(e, hx(&headers)),
    };
    let what = format!(
        "{} ({})",
        spec.rule.as_deref().unwrap_or("*"),
        human_duration(spec.secs)
    );
    match ctx.call(AdminOp::Silence(Box::new(spec))).await {
        Ok(_) => {
            let msg = format!("{} {what}", ctx.l.t("Silenced:", "묵음을 설정했습니다:"));
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn unsilence(ctx: Ctx, headers: HeaderMap, Path(id): Path<String>) -> Response {
    match ctx.call(AdminOp::Unsilence { id }).await {
        Ok(_) => {
            let msg = ctx.l.t("Silence ended.", "묵음을 끝냈습니다.").to_string();
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

/// The silences are outside the live region: show the page again.
fn reload(mut r: Response) -> Response {
    r.headers_mut()
        .insert("hx-refresh", axum::http::HeaderValue::from_static("true"));
    r
}
