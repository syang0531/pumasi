//! Cron schedules (1.7): the list, a form to add or replace one with a
//! job.toml and its files, Run now and Remove.

use axum::{
    extract::{Multipart, Path},
    http::HeaderMap,
    response::Response,
};
use maud::html;
use pumasi_proto::{
    manage::{ScheduleSpec, ScheduleView},
    msg::{AdminOp, AdminResponse},
};

use super::{Ctx, Nav, hx, table};
use crate::{fmt, uploads};

const EXAMPLE: &str = r#"name = "nightly"
image = "busybox:1.36"
command = ["sh", "-c", "date > /output/when.txt"]

[resources]
memory_mb = 64
timeout = "5m"
"#;

pub async fn page(ctx: Ctx) -> Response {
    let l = ctx.l;
    let list: Vec<ScheduleView> = match ctx.call(AdminOp::Schedules).await {
        Ok(AdminResponse::Schedules(s)) => s,
        Ok(_) => Vec::new(),
        Err(e) => return ctx.fail(e, false),
    };
    let can = ctx.me.can_submit();
    let mine = |s: &ScheduleView| ctx.me.account().is_none_or(|a| a == s.account);
    let rows = list
        .iter()
        .map(|s| {
            html! {
                tr data-schedule=(s.name) {
                    td { strong { (s.name) } }
                    td { code { (s.cron) } }
                    td { (s.job_name) " · " (s.tasks) " " (l.t("tasks", "태스크")) }
                    td { (s.account) }
                    td { @match s.next_unix { Some(t) => (fmt::time(t)), None => (l.t("never", "없음")) } }
                    td {
                        @match s.last_job { Some(j) => { a href={"/jobs/" (j)} { code { (j) } } " " }, None => {} }
                        @if let Some(e) = &s.last_error { span class="bad" { (e) } }
                    }
                    td { (s.runs) }
                    td class="actions" {
                        @if can && mine(s) {
                            button class="link" hx-post={"/schedules/" (s.name) "/run"} hx-target="#flash" { (l.t("Run now", "지금 실행")) }
                            " "
                            button class="link danger" hx-post={"/schedules/" (s.name) "/remove"} hx-target="#flash"
                                hx-confirm=(l.t("Remove this schedule? Jobs it already started stay.", "이 예약을 지울까요? 이미 시작한 작업은 남습니다.")) { (l.t("Remove", "지우기")) }
                        }
                    }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Schedules", "예약 작업")) } }
        p class="dim" { (l.t(
            "Jobs submitted on a cron schedule, in the control plane's local time; each run's account pays.",
            "cron 일정에 따라 제출되는 작업입니다. 시간은 컨트롤 플레인의 현지 시각이고, 실행마다 그 계정이 냅니다."
        )) }
    };
    let live = table(
        &[
            l.t("Name", "이름"),
            "cron",
            l.t("Job", "작업"),
            l.t("Account", "계정"),
            l.t("Next run", "다음 실행"),
            l.t("Last run", "지난 실행"),
            l.t("Runs", "실행 수"),
            "",
        ],
        rows,
        l.t("No schedules.", "예약 작업이 없습니다."),
    );
    let account = ctx.me.account().map(str::to_string);
    let bottom = html! {
        @if can {
            section {
                h2 { (l.t("Add or replace a schedule", "예약 추가·교체")) }
                form #schedule-form class="stack" hx-post="/schedules" hx-encoding="multipart/form-data" hx-target="#flash"
                    hx-disabled-elt="find button[type=submit]" {
                    div class="row" {
                        div { label for="s-name" { (l.t("Name", "이름")) } input #s-name name="name" required placeholder="nightly" pattern="[a-z0-9._-]{1,64}"; }
                        div { label for="s-cron" { "cron" } input #s-cron name="cron" required placeholder="0 3 * * *" value="@daily"; }
                    }
                    p class="dim" { (l.t("minute hour day month weekday, or @hourly, @daily, @weekly, @monthly.",
                        "분 시 일 월 요일, 또는 @hourly, @daily, @weekly, @monthly.")) }
                    label for="s-job" { "job.toml" }
                    textarea #s-job name="job" rows="10" spellcheck="false" { (EXAMPLE) }
                    div class="row" {
                        div { label for="s-files" { (l.t("Files (up to 16 MiB in all)", "파일 (모두 합쳐 16 MiB까지)")) } input #s-files type="file" name="files" multiple; }
                        div {
                            label for="s-account" { (l.t("Account", "계정")) }
                            @if let Some(a) = &account { input #s-account name="account" value=(a) readonly; }
                            @else { input #s-account name="account" placeholder=(l.t("default: the owner", "기본: 소유자")); }
                        }
                    }
                    label class="check" { input type="checkbox" name="overlap" value="on"; " " (l.t("Start a run even when the last one has not finished", "지난 실행이 안 끝나도 새로 시작")) }
                    div class="row" { button type="submit" { (l.t("Save schedule", "예약 저장")) } }
                }
            }
        }
    };
    ctx.page(
        l.t("Schedules", "예약 작업"),
        Nav::Schedules,
        top,
        Some(("10s", live)),
        bottom,
    )
}

pub async fn add(ctx: Ctx, headers: HeaderMap, form: Multipart) -> Response {
    let res = async {
        let up = uploads::receive(form, "schedule").await?;
        let name = up.field("name").unwrap_or_default().to_string();
        let cron = up.field("cron").unwrap_or_default().to_string();
        let overlap = up.field("overlap").is_some();
        let (job, upload) = super::jobs::load_upload(&up).await?;
        anyhow::ensure!(
            upload.is_empty(),
            "the job's input files are too large to keep with a schedule; put them in the file store and mount them with `store:NAME`"
        );
        let spec = ScheduleSpec {
            name: name.clone(),
            cron,
            job,
            overlap,
        };
        spec.validate()?;
        ctx.call(AdminOp::SetSchedule(Box::new(spec))).await?;
        anyhow::Ok(name)
    }
    .await;
    match res {
        Ok(name) => {
            let msg = format!(
                "{} {name}",
                ctx.t("Schedule saved:", "예약을 저장했습니다:")
            );
            ctx.done(msg)
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn action(
    ctx: Ctx,
    headers: HeaderMap,
    Path((name, action)): Path<(String, String)>,
) -> Response {
    let (op, done) = match action.as_str() {
        "run" => (
            AdminOp::RunSchedule { name: name.clone() },
            ctx.t("Submitted a run of", "실행을 제출했습니다:"),
        ),
        "remove" => (
            AdminOp::RemoveSchedule { name: name.clone() },
            ctx.t("Removed schedule", "예약을 지웠습니다:"),
        ),
        _ => return ctx.fail(anyhow::anyhow!("no such action: {action}"), hx(&headers)),
    };
    match ctx.call(op).await {
        Ok(_) => {
            let msg = format!("{done} {name}");
            ctx.done(msg)
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}
