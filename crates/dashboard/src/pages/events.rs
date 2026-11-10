//! Cluster events (newest first, live) and alerts: rules, what fires,
//! and the webhooks they go to.

use axum::{extract::Query, response::Response};
use maud::html;
use pumasi_proto::{
    monitor::{AlertsView, EventView},
    msg::{AdminOp, AdminResponse},
};
use serde::Deserialize;

use super::{Ctx, Nav, table};
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
                        li class={"level-" (a.level)} { strong { (a.rule) } " · " (a.subject) " — " (a.message) " " span class="dim" { (fmt::ago(l, a.since_unix)) } }
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
                }
                p class="dim" { (l.t("Change them with `pumasi alert set` or `[alerts]` in cluster.toml.", "`pumasi alert set`이나 cluster.toml의 `[alerts]`로 바꿉니다.")) }
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
