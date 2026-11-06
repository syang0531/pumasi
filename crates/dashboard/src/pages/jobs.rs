//! Jobs: the list, submitting one (job.toml and its files from the
//! browser), a job's tasks, cancelling, live task output, and downloading
//! outputs and receipts.

use std::{convert::Infallible, time::Duration};

use anyhow::Context as _;
use axum::{
    body::Body,
    extract::{Multipart, Path, Query},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse, Redirect, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use maud::{Markup, html};
use pumasi_proto::{
    job::{JobFile, JobId},
    monitor::LogChunk,
    msg::{AdminOp, AdminResponse, JobState, JobSummary, JobView, TaskState, Verification},
    receipt::ReceiptRow,
    store::StoreTarget,
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, status, table};
use crate::{
    St,
    fmt::{self, format_size},
    i18n::Lang,
    uploads::{self, TempDir, TempFileStream},
    zip::ZipWriter,
};

/// Shared input files above this go through the file store (as with
/// `pumasi run`).
const INLINE_INPUT_BYTES: u64 = 16 << 20;

#[derive(Deserialize)]
pub struct ListQuery {
    state: Option<String>,
}

pub async fn list(ctx: Ctx, Query(q): Query<ListQuery>) -> Response {
    match render_list(&ctx, q.state.as_deref().unwrap_or("all")).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

fn state_filter(f: &str, j: &JobSummary) -> bool {
    match f {
        "active" => !j.state.is_terminal(),
        "succeeded" => j.state == JobState::Succeeded,
        "failed" => j.state == JobState::Failed,
        "cancelled" => j.state == JobState::Cancelled,
        _ => true,
    }
}

async fn render_list(ctx: &Ctx, filter: &str) -> anyhow::Result<Response> {
    let l = ctx.l;
    let s = status(ctx).await?;
    let mut jobs: Vec<&JobSummary> = s.jobs.iter().filter(|j| state_filter(filter, j)).collect();
    jobs.sort_by_key(|j| std::cmp::Reverse(j.submitted_unix));
    let rows = jobs
        .iter()
        .map(|j| {
            let done = j.succeeded + j.failed;
            html! {
                tr data-job=(j.id) {
                    td { a href={"/jobs/" (j.id)} { code { (j.id) } } }
                    td { (j.name) }
                    td { (fmt::job_state(l, j.state)) }
                    td { (fmt::meter(fmt::percent(done as u64, j.tasks as u64), format!("{done}/{}", j.tasks))) }
                    td { (j.running) }
                    td { (j.failed) }
                    td { (j.account) }
                    td { (j.priority) }
                    td { (fmt::cu(j.cost)) }
                    td { (fmt::time(j.submitted_unix)) }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" {
            h1 { (l.t("Jobs", "작업")) }
            @if ctx.me.can_submit() {
                a class="button" href="/jobs/new" { (l.t("Submit a job", "작업 제출")) }
            }
        }
        nav class="tabs" {
            @for (f, label) in [("all", l.t("All", "전체")), ("active", l.t("Active", "진행 중")), ("succeeded", l.t("Succeeded", "성공")),
                                ("failed", l.t("Failed", "실패")), ("cancelled", l.t("Cancelled", "취소"))] {
                a href={"/jobs?state=" (f)} class=[(f == filter).then_some("active")] { (label) }
            }
        }
    };
    let live = table(
        &[
            l.t("Job", "작업"),
            l.t("Name", "이름"),
            l.t("State", "상태"),
            l.t("Progress", "진행"),
            l.t("Running", "실행 중"),
            l.t("Failed", "실패"),
            l.t("Account", "계정"),
            l.t("Priority", "우선순위"),
            l.t("Cost", "비용"),
            l.t("Submitted", "제출"),
        ],
        rows,
        l.t("No jobs here.", "해당하는 작업이 없습니다."),
    );
    Ok(ctx.page(
        l.t("Jobs", "작업"),
        Nav::Jobs,
        top,
        Some(("3s", live)),
        html! {},
    ))
}

const EXAMPLE: &str = r#"# Same format as `pumasi run job.toml`. Files the job lists
# (module, stdin, files) come from the files you attach below.
name = "hello"
image = "busybox:1.36"
command = ["sh", "-c", "echo hello from task {shard} > /output/hello.txt; cat /output/hello.txt"]
shards = 2

[resources]
memory_mb = 64
timeout = "5m"
"#;

pub async fn new(ctx: Ctx) -> Response {
    let l = ctx.l;
    let account = ctx.me.account().map(str::to_string);
    let body = html! {
        div class="page-head" { h1 { (l.t("Submit a job", "작업 제출")) } }
        form #submit-form class="stack" hx-post="/jobs" hx-encoding="multipart/form-data" hx-target="#flash"
            hx-indicator="#submit-busy" hx-disabled-elt="find button[type=submit]" {
            label for="job" { "job.toml" }
            p class="dim" { (l.t(
                "Leave it empty to use a job.toml among the attached files.",
                "비워 두면 첨부한 파일 중 job.toml을 씁니다."
            )) }
            textarea #job name="job" rows="14" spellcheck="false" { (EXAMPLE) }
            div class="row" {
                div {
                    label for="files" { (l.t("Files", "파일")) }
                    input #files type="file" name="files" multiple;
                }
                div {
                    label for="folder" { (l.t("Or a folder", "또는 폴더")) }
                    input #folder type="file" name="files" webkitdirectory multiple;
                }
            }
            div class="row" {
                div {
                    label for="account" { (l.t("Account", "계정")) }
                    @if let Some(a) = &account {
                        input #account name="account" value=(a) readonly;
                    } @else {
                        input #account name="account" placeholder=(l.t("default: the owner", "기본: 소유자"));
                    }
                }
                div {
                    label for="priority" { (l.t("Priority", "우선순위")) }
                    input #priority name="priority" type="number" min="-1000" max="1000" placeholder="0";
                }
            }
            div class="row" {
                button type="submit" { (l.t("Submit", "제출")) }
                span #submit-busy class="busy" { (l.t("Uploading and submitting…", "업로드하고 제출하는 중…")) }
            }
        }
    };
    ctx.page(
        l.t("Submit a job", "작업 제출"),
        Nav::Jobs,
        body,
        None,
        html! {},
    )
}

pub async fn submit(ctx: Ctx, headers: HeaderMap, form: Multipart) -> Response {
    match do_submit(&ctx, form).await {
        Ok(id) => {
            let to = format!("/jobs/{id}");
            if hx(&headers) {
                ([("hx-redirect", to)], StatusCode::OK).into_response()
            } else {
                Redirect::to(&to).into_response()
            }
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

/// Writes the form's job.toml and files to a temporary directory and
/// submits from there, as `pumasi run` would, except that the job file may
/// only name files in that directory: it comes from whoever holds a
/// ticket, and this process runs on the admin's machine.
pub(crate) async fn submit_upload(
    st: &St,
    secret: pumasi_proto::Secret,
    up: uploads::Upload,
) -> anyhow::Result<JobId> {
    let job_path = up.dir.path().join("job.toml");
    if let Some(text) = up.field("job") {
        tokio::fs::write(&job_path, text).await?;
    } else if !job_path.exists() {
        anyhow::bail!("no job.toml: write one or attach it");
    }
    let (mut sub, upload) = JobFile::load_split_confined(&job_path, INLINE_INPUT_BYTES)?;
    if let Some(a) = up.field("account") {
        sub.account = Some(a.to_string());
    }
    if let Some(p) = up.field("priority") {
        sub.priority = p.parse().context("priority must be a whole number")?;
    }
    if let Some(after) = up.field("after") {
        for a in after.split([',', ' ']).filter(|s| !s.is_empty()) {
            sub.after.push(a.parse()?);
        }
    }
    sub.validate()?;
    let client = st.backend.client(secret).await?;
    let id = client
        .submit(sub, &upload, |m| tracing::debug!("submit: {m}"))
        .await?;
    drop(up);
    Ok(id)
}

async fn do_submit(ctx: &Ctx, form: Multipart) -> anyhow::Result<JobId> {
    let up = uploads::receive(form, "job").await?;
    submit_upload(&ctx.st, ctx.me.secret, up).await
}

pub(crate) async fn job_view(ctx: &Ctx, id: JobId) -> anyhow::Result<JobView> {
    match ctx
        .call(AdminOp::Job {
            id,
            with_output: false,
        })
        .await?
    {
        AdminResponse::Job(v) => Ok(*v),
        _ => anyhow::bail!("unexpected response"),
    }
}

fn parse_id(s: &str) -> anyhow::Result<JobId> {
    s.parse().map_err(|_| anyhow::anyhow!("no such job: {s}"))
}

pub async fn detail(ctx: Ctx, Path(id): Path<String>) -> Response {
    match render_detail(&ctx, &id).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

fn verification(l: Lang, v: &Verification) -> Markup {
    match v {
        Verification::NotChecked => html! { span class="dim" { "—" } },
        Verification::Pending => html! { span class="badge info" { (l.t("checking", "검증 중")) } },
        Verification::Passed => html! { span class="badge ok" { (l.t("passed", "통과")) } },
        Verification::Mismatch { bad } => {
            html! { span class="badge bad" title=(bad.join(", ")) { (l.t("mismatch", "불일치")) } }
        }
        Verification::Inconclusive => {
            html! { span class="badge warn" { (l.t("inconclusive", "판정 불가")) } }
        }
        Verification::Skipped(r) => {
            html! { span class="dim" title=(r) { (l.t("skipped", "건너뜀")) } }
        }
    }
}

/// Node ids to names, for tasks (which name their node by id).
async fn node_names(ctx: &Ctx) -> Vec<(String, String)> {
    match status(ctx).await {
        Ok(s) => s.nodes.into_iter().map(|n| (n.id, n.info.name)).collect(),
        Err(_) => Vec::new(),
    }
}

fn name_of(names: &[(String, String)], id: &str) -> String {
    names
        .iter()
        .find(|(i, _)| i == id)
        .map(|(_, n)| n.clone())
        .unwrap_or_else(|| fmt::short(id).to_string())
}

fn task_state(l: Lang, state: &TaskState, names: &[(String, String)]) -> Markup {
    match state {
        TaskState::Running { node } => fmt::task_state(
            l,
            &TaskState::Running {
                node: name_of(names, node),
            },
        ),
        s => fmt::task_state(l, s),
    }
}

fn has_outputs(v: &JobView) -> bool {
    v.tasks
        .iter()
        .any(|t| !t.output_files.is_empty() || t.stored.is_some())
}

async fn render_detail(ctx: &Ctx, id: &str) -> anyhow::Result<Response> {
    let l = ctx.l;
    let id = parse_id(id)?;
    let v = job_view(ctx, id).await?;
    let names = node_names(ctx).await;
    let s = &v.summary;
    let terminal = s.state.is_terminal();
    let top = html! {
        div class="page-head" {
            h1 { (s.name) " " code class="dim" { (s.id) } }
            div class="actions" {
                @if ctx.me.can_submit() && !terminal {
                    button class="danger" hx-post={"/jobs/" (s.id) "/cancel"} hx-target="#flash"
                        hx-confirm=(l.t("Cancel this job?", "이 작업을 취소할까요?")) { (l.t("Cancel", "취소")) }
                }
                a class="button secondary" href={"/jobs/" (s.id) "/receipts.json"} download { (l.t("Receipts", "영수증")) }
            }
        }
    };
    let rows = v
        .tasks
        .iter()
        .map(|t| {
            html! {
                tr data-task=(t.index) {
                    td { a href={"/jobs/" (s.id) "/tasks/" (t.index)} { "#" (t.index) } }
                    td { (task_state(l, &t.state, &names)) }
                    td { (t.attempts) @if t.preempted > 0 { " " span class="dim" { "(" (t.preempted) " " (l.t("moved", "이동")) ")" } } }
                    td { @if let Some(n) = &t.node { a href={"/nodes/" (n)} { (name_of(&names, n)) } } }
                    td { @if t.wall_ms > 0 { (fmt::ms(t.wall_ms)) } }
                    td { (fmt::cu(t.cost)) }
                    td { (verification(l, &t.verification)) }
                    td class="files" {
                        @for (path, size) in &t.output_files {
                            a href={"/jobs/" (s.id) "/tasks/" (t.index) "/files/" (super::encode(path))} download { (path) }
                            " " span class="dim" { (format_size(*size)) } br;
                        }
                        @if let Some(st) = &t.stored {
                            span class="dim" { (st.files) " " (l.t("files in the store", "개 파일(저장소)")) " · " (format_size(st.size)) }
                        }
                    }
                    td { a href={"/jobs/" (s.id) "/tasks/" (t.index)} { (l.t("output", "출력")) } }
                }
            }
        })
        .collect();
    let done = s.succeeded + s.failed;
    let live = html! {
        section class="tiles" {
            div class="tile" { div class="tile-label" { (l.t("State", "상태")) } div class="tile-value" #job-state { (fmt::job_state(l, s.state)) } }
            (super::tile(l.t("Tasks done", "완료한 태스크"), format!("{done} / {}", s.tasks), Some(format!("{} {}", s.running, l.t("running", "실행 중")))))
            (super::tile(l.t("Failed", "실패"), s.failed.to_string(), None))
            (super::tile(l.t("Cost", "비용"), fmt::cu(s.cost), None))
        }
        @if let Some(p) = &v.preparing { div class="flash info" { (p) } }
        @if let Some(w) = &v.waiting { div class="flash info" { (w) } }
        @if has_outputs(&v) {
            p { a class="button" #download-all href={"/jobs/" (s.id) "/outputs.zip"} download { (l.t("Download all outputs (.zip)", "출력 모두 받기 (.zip)")) } }
        }
        (table(&[l.t("Task", "태스크"), l.t("State", "상태"), l.t("Attempts", "시도"), l.t("Node", "노드"), l.t("Time", "시간"),
                 l.t("Cost", "비용"), l.t("Check", "검증"), l.t("Output files", "출력 파일"), ""],
            rows, l.t("No tasks.", "태스크가 없습니다.")))
    };
    let bottom = html! {
        section {
            h2 { (l.t("Details", "세부 정보")) }
            dl class="facts" {
                dt { (l.t("Executor", "실행기")) } dd { (v.executor.to_string()) @if let Some(i) = &v.image { " · " code { (i) } } }
                dt { (l.t("Account", "계정")) } dd { (s.account) }
                dt { (l.t("Priority", "우선순위")) } dd { (s.priority) }
                dt { (l.t("Submitted", "제출")) } dd { (fmt::time(s.submitted_unix)) }
                dt { (l.t("Limits", "제한")) } dd {
                    (format_size(v.limits.memory_bytes)) " · " (fmt::ms(v.limits.timeout_ms))
                    @if let Some(c) = v.limits.cpu_millis { " · " (format!("{:.2}", c as f64 / 1000.0)) " CPU" }
                }
                @if let Some(g) = &v.gpu { dt { "GPU" } dd { (if g.count == 0 { "all".to_string() } else { g.count.to_string() }) } }
                @if !v.mounts.is_empty() { dt { (l.t("Mounts", "마운트")) } dd { (v.mounts.join(", ")) } }
                @if let Some(d) = &v.output_dataset { dt { (l.t("Output dataset", "출력 데이터셋")) } dd { a href={"/storage/" (d.split('@').next().unwrap_or(d))} { (d) } } }
                @if !v.selector.0.is_empty() { dt { (l.t("Selector", "셀렉터")) } dd { code { (v.selector.to_string()) } } }
                @if !v.after.is_empty() { dt { (l.t("After", "선행 작업")) } dd {
                    @for a in &v.after { a href={"/jobs/" (a)} { code { (a) } } " " }
                } }
                @if let Some(sch) = &v.schedule { dt { (l.t("Schedule", "스케줄")) } dd { (sch) } }
                dt { (l.t("Deterministic", "결정적 실행")) } dd { (if v.deterministic { l.t("yes", "예") } else { l.t("no", "아니요") }) }
                @if v.network { dt { (l.t("Network", "네트워크")) } dd { (l.t("allowed", "허용")) } }
            }
        }
    };
    let every = if terminal { "30s" } else { "2s" };
    Ok(ctx.page(&s.name, Nav::Jobs, top, Some((every, live)), bottom))
}

pub async fn cancel(ctx: Ctx, headers: HeaderMap, Path(id): Path<String>) -> Response {
    let res = async {
        let id = parse_id(&id)?;
        ctx.call(AdminOp::Cancel { id }).await
    }
    .await;
    match res {
        Ok(_) => ctx.done(ctx.t("Cancelled.", "취소했습니다.")),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn receipts(ctx: Ctx, Path(id): Path<String>) -> Response {
    let res = async {
        let id = parse_id(&id)?;
        match ctx.call(AdminOp::Receipts { id }).await? {
            AdminResponse::Receipts(r) => {
                let rows: Vec<ReceiptRow> = r.iter().map(ReceiptRow::from).collect();
                Ok((id, serde_json::to_vec_pretty(&rows)?))
            }
            _ => anyhow::bail!("unexpected response"),
        }
    }
    .await;
    match res {
        Ok((id, json)) => (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                ),
                (
                    header::CONTENT_DISPOSITION,
                    attachment(&format!("receipts-{id}.json")),
                ),
            ],
            json,
        )
            .into_response(),
        Err(e) => ctx.fail(e, false),
    }
}

/// `Content-Disposition: attachment` with a UTF-8 file name.
pub fn attachment(name: &str) -> HeaderValue {
    let base = name.rsplit('/').next().unwrap_or(name);
    let ascii: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{ascii}\"; filename*=UTF-8''{}",
        super::encode(base).replace('/', "%2F")
    ))
    .unwrap_or_else(|_| HeaderValue::from_static("attachment"))
}

pub async fn file(ctx: Ctx, Path((id, index, path)): Path<(String, u32, String)>) -> Response {
    let res = async {
        let id = parse_id(&id)?;
        match ctx.call(AdminOp::TaskFiles { id, index }).await? {
            AdminResponse::Files(files) => files
                .into_iter()
                .find(|f| f.path == path)
                .ok_or_else(|| anyhow::anyhow!("no such file: {path}")),
            _ => anyhow::bail!("unexpected response"),
        }
    }
    .await;
    match res {
        Ok(f) => (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                ),
                (header::CONTENT_DISPOSITION, attachment(&f.path)),
            ],
            f.data,
        )
            .into_response(),
        Err(e) => ctx.fail(e, false),
    }
}

/// Every task's output files (inline and stored) as one zip, built in a
/// temporary directory and streamed.
pub async fn outputs_zip(ctx: Ctx, Path(id): Path<String>) -> Response {
    match build_outputs_zip(&ctx.st, ctx.me.secret, &id).await {
        Ok((dir, zip, name)) => zip_response(dir, &zip, &name).await,
        Err(e) => ctx.fail(e, false),
    }
}

pub(crate) async fn zip_response(dir: TempDir, zip: &std::path::Path, name: &str) -> Response {
    let len = std::fs::metadata(zip).map(|m| m.len()).unwrap_or(0);
    match TempFileStream::open(dir, zip).await {
        Ok(stream) => (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/zip"),
                ),
                (header::CONTENT_DISPOSITION, attachment(name)),
                (header::CONTENT_LENGTH, HeaderValue::from(len)),
            ],
            Body::from_stream(stream),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")).into_response(),
    }
}

pub(crate) async fn build_outputs_zip(
    st: &St,
    secret: pumasi_proto::Secret,
    id: &str,
) -> anyhow::Result<(TempDir, std::path::PathBuf, String)> {
    let id = parse_id(id)?;
    let client = st.backend.client(secret).await?;
    let AdminResponse::Job(v) = client
        .call(AdminOp::Job {
            id,
            with_output: false,
        })
        .await?
    else {
        anyhow::bail!("unexpected response");
    };
    anyhow::ensure!(has_outputs(&v), "this job has no output files (yet)");
    let dir = TempDir::new("out")?;
    let mut inline = Vec::new();
    for t in v.tasks.iter().filter(|t| !t.output_files.is_empty()) {
        if let AdminResponse::Files(files) = client
            .call(AdminOp::TaskFiles { id, index: t.index })
            .await?
        {
            for f in files {
                inline.push((format!("task-{}/{}", t.index, f.path), f.data));
            }
        }
    }
    let stored_dir = dir.path().join("stored");
    if v.tasks.iter().any(|t| t.stored.is_some()) {
        client
            .store_get(StoreTarget::Job { id }, &stored_dir, |m| {
                tracing::debug!("outputs of {id}: {m}")
            })
            .await?;
    }
    let zip_path = dir.path().join("outputs.zip");
    let zp = zip_path.clone();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let file = std::io::BufWriter::new(std::fs::File::create(&zp)?);
        let mut z = ZipWriter::new(file, fmt::now_unix());
        for (name, data) in &inline {
            z.add(name, &mut data.as_slice())?;
        }
        if stored_dir.exists() {
            for (rel, full) in uploads::walk(&stored_dir)? {
                if rel.starts_with(pumasi_cluster::transfer::PARTIAL_DIR) {
                    continue;
                }
                z.add(&rel, &mut std::fs::File::open(&full)?)?;
            }
        }
        z.finish()?;
        Ok(())
    })
    .await??;
    Ok((dir, zip_path, format!("pumasi-{id}-outputs.zip")))
}

#[derive(Deserialize)]
pub struct TaskQuery {
    attempt: Option<u32>,
}

pub async fn task(ctx: Ctx, Path((id, index)): Path<(String, u32)>) -> Response {
    match render_task(&ctx, &id, index).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render_task(ctx: &Ctx, id: &str, index: u32) -> anyhow::Result<Response> {
    let l = ctx.l;
    let id = parse_id(id)?;
    let v = job_view(ctx, id).await?;
    let t = v
        .tasks
        .iter()
        .find(|t| t.index == index)
        .ok_or_else(|| anyhow::anyhow!("no such task: {index}"))?
        .clone();
    let names = node_names(ctx).await;
    let title = format!("{} #{index}", v.summary.name);
    let top = html! {
        div class="page-head" {
            h1 { a href={"/jobs/" (id)} { (v.summary.name) } " · " (l.t("task", "태스크")) " #" (index) }
        }
    };
    let live = html! {
        section { dl class="facts" {
            dt { (l.t("State", "상태")) } dd { (task_state(l, &t.state, &names)) }
            dt { (l.t("Attempts", "시도")) } dd { (t.attempts) }
            @if let Some(n) = &t.node { dt { (l.t("Node", "노드")) } dd { a href={"/nodes/" (n)} { (name_of(&names, n)) } } }
            @if t.wall_ms > 0 { dt { (l.t("Time", "시간")) } dd { (fmt::ms(t.wall_ms)) } }
            dt { (l.t("Cost", "비용")) } dd { (fmt::cu(t.cost)) }
            dt { (l.t("Output files", "출력 파일")) } dd class="files" {
                @if t.output_files.is_empty() && t.stored.is_none() { span class="dim" { "—" } }
                @for (path, size) in &t.output_files {
                    a href={"/jobs/" (id) "/tasks/" (index) "/files/" (super::encode(path))} download { (path) }
                    " " span class="dim" { (format_size(*size)) } br;
                }
                @if let Some(st) = &t.stored {
                    a href={"/jobs/" (id) "/outputs.zip"} download { (st.files) " " (l.t("files in the store", "개 파일(저장소)")) } " · " (format_size(st.size))
                }
            }
        } }
    };
    let finished = matches!(
        t.state,
        TaskState::Succeeded | TaskState::Failed(_) | TaskState::Cancelled
    );
    let bottom = html! {
        section class="logs" data-log-stream={"/jobs/" (id) "/tasks/" (index) "/stream"} data-finished=(finished) {
            div class="log-head" {
                h2 { (l.t("Output", "출력")) }
                span class="dim" data-log-status { (l.t("connecting…", "연결 중…")) }
            }
            h3 { "stdout" }
            pre class="log" data-log="stdout" {}
            h3 { "stderr" }
            pre class="log err" data-log="stderr" {}
        }
    };
    let every = if finished { "60s" } else { "3s" };
    Ok(ctx.page(&title, Nav::Jobs, top, Some((every, live)), bottom))
}

/// Server-sent events with a task's output as it is printed: the
/// dashboard polls the control plane like `pumasi logs -f` (every 250 ms)
/// and forwards what is new.
pub async fn stream(
    ctx: Ctx,
    Path((id, index)): Path<(String, u32)>,
    Query(q): Query<TaskQuery>,
) -> Response {
    let Ok(job) = parse_id(&id) else {
        return (StatusCode::NOT_FOUND, "no such job").into_response();
    };
    let state = LogState {
        st: ctx.st.clone(),
        secret: ctx.me.secret,
        lang: ctx.l,
        job,
        index,
        attempt: q.attempt.unwrap_or(u32::MAX),
        out: 0,
        err: 0,
        out_carry: Vec::new(),
        err_carry: Vec::new(),
        first: true,
        finished: false,
    };
    let events = n0_future::stream::unfold(state, |mut s| async move {
        if s.finished {
            return None;
        }
        let ev = s.next_event().await;
        Some((Ok::<_, Infallible>(ev), s))
    });
    Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response()
}

struct LogState {
    st: St,
    secret: pumasi_proto::Secret,
    lang: Lang,
    job: JobId,
    index: u32,
    attempt: u32,
    out: u64,
    err: u64,
    out_carry: Vec<u8>,
    err_carry: Vec<u8>,
    first: bool,
    finished: bool,
}

impl LogState {
    async fn next_event(&mut self) -> Event {
        loop {
            if !self.first {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            self.first = false;
            let res = self
                .st
                .backend
                .call(
                    self.secret,
                    AdminOp::Logs {
                        job: self.job,
                        index: self.index,
                        attempt: if self.attempt == u32::MAX {
                            0
                        } else {
                            self.attempt
                        },
                        stdout_from: self.out,
                        stderr_from: self.err,
                    },
                )
                .await;
            let chunk: LogChunk = match res {
                Ok(AdminResponse::Logs(c)) => *c,
                Ok(_) => {
                    self.finished = true;
                    return Event::default().event("error").data("unexpected response");
                }
                Err(e) => {
                    self.finished = true;
                    return Event::default().event("error").data(format!("{e:#}"));
                }
            };
            let reset = chunk.attempt != self.attempt;
            if reset {
                self.attempt = chunk.attempt;
                self.out = 0;
                self.err = 0;
                self.out_carry.clear();
                self.err_carry.clear();
            }
            self.out = chunk.stdout_from + chunk.stdout.len() as u64;
            self.err = chunk.stderr_from + chunk.stderr.len() as u64;
            self.out_carry.extend_from_slice(&chunk.stdout);
            self.err_carry.extend_from_slice(&chunk.stderr);
            let stdout = take_utf8(&mut self.out_carry, chunk.done);
            let stderr = take_utf8(&mut self.err_carry, chunk.done);
            if chunk.done {
                self.finished = true;
            }
            if reset || chunk.done || !stdout.is_empty() || !stderr.is_empty() {
                let l = self.lang;
                let status = if chunk.done {
                    l.t("finished", "끝남")
                } else if chunk.live {
                    l.t("live", "실시간")
                } else {
                    l.t(
                        "output comes when the task ends (microVM)",
                        "태스크가 끝나면 출력이 옵니다(microVM)",
                    )
                };
                let data = serde_json::json!({
                    "attempt": chunk.attempt,
                    "reset": reset,
                    "stdout": stdout,
                    "stderr": stderr,
                    "done": chunk.done,
                    "status": status,
                });
                return Event::default().event("chunk").data(data.to_string());
            }
        }
    }
}

/// Takes the complete UTF-8 text from `buf`, keeping a character cut at
/// the end for the next chunk (all of it when `done`).
pub fn take_utf8(buf: &mut Vec<u8>, done: bool) -> String {
    let cut = if done { buf.len() } else { complete_len(buf) };
    let s = String::from_utf8_lossy(&buf[..cut]).into_owned();
    buf.drain(..cut);
    s
}

fn complete_len(b: &[u8]) -> usize {
    let n = b.len();
    for back in 1..=n.min(4) {
        let c = b[n - back];
        if c & 0xC0 == 0x80 {
            continue;
        }
        let need = if c >= 0xF0 {
            4
        } else if c >= 0xE0 {
            3
        } else if c >= 0xC0 {
            2
        } else {
            1
        };
        return if need > back { n - back } else { n };
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_split_across_chunks() {
        let text = "출력 ok".as_bytes();
        let mut buf = text[..2].to_vec();
        assert_eq!(take_utf8(&mut buf, false), "");
        buf.extend_from_slice(&text[2..4]);
        assert_eq!(take_utf8(&mut buf, false), "출");
        buf.extend_from_slice(&text[4..]);
        assert_eq!(take_utf8(&mut buf, false), "력 ok");
        assert!(buf.is_empty());
        let mut bad = vec![b'a', 0xE2];
        assert_eq!(take_utf8(&mut bad, true), "a\u{FFFD}");
    }

    #[test]
    fn file_names_in_downloads() {
        assert_eq!(
            attachment("task-0/결과 1.txt").to_str().unwrap(),
            "attachment; filename=\"___1.txt\"; filename*=UTF-8''%EA%B2%B0%EA%B3%BC%201.txt"
        );
    }
}
