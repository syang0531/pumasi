//! The front page: how the cluster is doing right now.

use axum::response::Response;
use maud::html;
use pumasi_proto::{
    monitor::{AlertsView, EventView, MetricsView},
    msg::{AdminOp, AdminResponse, JobState},
};

use super::{Ctx, Nav, status, table, tile};
use crate::fmt;

pub async fn page(ctx: Ctx) -> Response {
    match render(&ctx).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render(ctx: &Ctx) -> anyhow::Result<Response> {
    let l = ctx.l;
    let s = status(ctx).await?;
    // Older control planes or narrower roles may refuse these: show what
    // there is.
    let metrics = match ctx
        .call(AdminOp::Metrics {
            node: None,
            since_secs: 0,
        })
        .await
    {
        Ok(AdminResponse::Metrics(m)) => *m,
        _ => MetricsView::default(),
    };
    let events: Vec<EventView> = match ctx
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 8,
        })
        .await
    {
        Ok(AdminResponse::Events(e)) => e,
        _ => Vec::new(),
    };
    let alerts = match ctx.call(AdminOp::Alerts).await {
        Ok(AdminResponse::Alerts(a)) => *a,
        _ => AlertsView::default(),
    };
    let online = s.nodes.iter().filter(|n| n.online).count();
    let slots: u32 = s
        .nodes
        .iter()
        .filter(|n| n.online)
        .map(|n| n.info.slots)
        .sum();
    let running: u32 = s.nodes.iter().map(|n| n.running).sum();
    let queued: u32 = metrics.cluster.queued.iter().map(|(_, n)| n).sum();
    let active = s.jobs.iter().filter(|j| !j.state.is_terminal()).count();
    let failed = s
        .jobs
        .iter()
        .filter(|j| j.state == JobState::Failed)
        .count();
    let gpus: usize = s
        .nodes
        .iter()
        .filter(|n| n.online)
        .map(|n| n.gpus.len())
        .sum();
    let busy_gpus: usize = s
        .nodes
        .iter()
        .flat_map(|n| &n.gpus)
        .filter(|g| g.busy.is_some())
        .count();

    let mut jobs = s.jobs.clone();
    jobs.sort_by_key(|j| std::cmp::Reverse(j.submitted_unix));
    let job_rows = jobs
        .iter()
        .take(8)
        .map(|j| {
            html! {
                tr {
                    td { a href={"/jobs/" (j.id)} { code { (j.id) } } }
                    td { (j.name) }
                    td { (fmt::job_state(l, j.state)) }
                    td { (j.succeeded + j.failed) "/" (j.tasks) }
                    td { (j.account) }
                    td { (fmt::ago(l, j.submitted_unix)) }
                }
            }
        })
        .collect();
    let event_rows = events
        .iter()
        .rev()
        .map(|e| {
            html! {
                tr class={"level-" (e.level)} {
                    td { (fmt::time(e.unix)) }
                    td { code { (e.kind) } }
                    td { (e.message) }
                }
            }
        })
        .collect();

    let live = html! {
        section class="tiles" {
            (tile(l.t("Nodes online", "온라인 노드"), format!("{online} / {}", s.nodes.len()), None))
            (tile(l.t("Tasks running", "실행 중인 작업"), format!("{running} / {slots}"), Some(l.t("of free slots", "슬롯 대비").into())))
            (tile(l.t("Queued runs", "대기 중인 실행"), queued.to_string(),
                (metrics.cluster.quota_blocked > 0).then(|| format!("{} {}", metrics.cluster.quota_blocked, l.t("held by quotas", "쿼터로 보류")))))
            (tile(l.t("Active jobs", "진행 중인 잡"), active.to_string(),
                (failed > 0).then(|| format!("{failed} {}", l.t("failed", "실패")))))
            @if gpus > 0 {
                (tile("GPU", format!("{busy_gpus} / {gpus}"), Some(l.t("in use", "사용 중").into())))
            }
            (tile(l.t("Alerts firing", "발생 중인 경보"), alerts.firing.len().to_string(), None))
        }
        @if !alerts.firing.is_empty() {
            section {
                h2 { (l.t("Alerts", "경보")) }
                ul class="alerts" {
                    @for a in &alerts.firing {
                        li class={"level-" (a.level)} {
                            strong { (a.rule) } " · " (a.subject) " — " (a.message)
                            " " span class="dim" { (fmt::ago(l, a.since_unix)) }
                        }
                    }
                }
            }
        }
        div class="cols" {
            section {
                h2 { a href="/jobs" { (l.t("Recent jobs", "최근 작업")) } }
                (table(&[l.t("Job", "작업"), l.t("Name", "이름"), l.t("State", "상태"), l.t("Tasks", "태스크"), l.t("Account", "계정"), l.t("Submitted", "제출")],
                    job_rows, l.t("No jobs yet.", "아직 작업이 없습니다.")))
            }
            section {
                h2 { a href="/events" { (l.t("Recent events", "최근 이벤트")) } }
                (table(&[l.t("Time", "시각"), l.t("Kind", "종류"), l.t("Message", "내용")],
                    event_rows, l.t("No events.", "이벤트가 없습니다.")))
            }
        }
    };
    let top = html! {
        div class="page-head" {
            h1 { (l.t("Overview", "개요")) }
            @if ctx.me.can_submit() {
                a class="button" href="/jobs/new" { (l.t("Submit a job", "작업 제출")) }
            }
        }
    };
    Ok(ctx.page(
        l.t("Overview", "개요"),
        Nav::Overview,
        top,
        Some(("5s", live)),
        html! {},
    ))
}
