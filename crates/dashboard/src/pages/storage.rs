//! The cluster file store: datasets, their versions, usage against
//! quotas, uploads from the browser and downloads as zip.

use anyhow::Context as _;
use axum::{
    Form,
    extract::{Multipart, Path, Query},
    http::HeaderMap,
    response::Response,
};
use maud::html;
use pumasi_proto::{
    msg::{AdminOp, AdminResponse},
    store::{DatasetView, StoreTarget, StoreUsageView},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, table};
use crate::{
    fmt::{self, format_size},
    uploads::{self, TempDir},
    zip::ZipWriter,
};

async fn datasets(ctx: &Ctx, name: Option<String>) -> anyhow::Result<Vec<DatasetView>> {
    match ctx.call(AdminOp::StoreList { name }).await? {
        AdminResponse::Datasets(d) => Ok(d),
        _ => anyhow::bail!("unexpected response"),
    }
}

pub async fn list(ctx: Ctx) -> Response {
    match render_list(&ctx).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render_list(ctx: &Ctx) -> anyhow::Result<Response> {
    let l = ctx.l;
    let mut list = datasets(ctx, None).await?;
    list.sort_by(|a, b| a.name.cmp(&b.name));
    let (usage, disk): (Vec<StoreUsageView>, u64) = match ctx.call(AdminOp::StoreUsage).await {
        Ok(AdminResponse::StoreUsage { usage, disk_bytes }) => (usage, disk_bytes),
        _ => (Vec::new(), 0),
    };
    let can_download = ctx.me.can_submit();
    let rows = list
        .iter()
        .map(|d| {
            html! {
                tr data-dataset=(d.name) {
                    td { a href={"/storage/" (d.name)} { strong { (d.name) } } }
                    td { "v" (d.version) }
                    td { (d.files) }
                    td { (format_size(d.size)) }
                    td { (d.account) }
                    td { (fmt::time(d.created_unix)) }
                    td { @if let Some(j) = d.job { a href={"/jobs/" (j)} { code { (j) } } } }
                    td { @if can_download {
                        a href={"/storage/" (d.name) "/download.zip?version=" (d.version)} download { (l.t("Download", "받기")) }
                    } }
                }
            }
        })
        .collect();
    let usage_rows = usage
        .iter()
        .map(|u| {
            html! {
                tr {
                    td { (u.account) }
                    td { (u.datasets) }
                    td { @if let Some(q) = u.quota {
                        (fmt::meter(fmt::percent(u.bytes, q), format!("{} / {}", format_size(u.bytes), format_size(q))))
                    } @else { (format_size(u.bytes)) } }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Storage", "저장소")) } }
    };
    let live = html! {
        section {
            h2 { (l.t("Datasets", "데이터셋")) }
            (table(&[l.t("Name", "이름"), l.t("Latest", "최신"), l.t("Files", "파일"), l.t("Size", "크기"), l.t("Account", "계정"),
                     l.t("Created", "생성"), l.t("From job", "출처 작업"), ""],
                rows, l.t("No datasets yet.", "아직 데이터셋이 없습니다.")))
        }
        section {
            h2 { (l.t("Usage", "사용량")) }
            @if disk > 0 { p class="dim" { (l.t("On the control plane's disk:", "컨트롤 플레인 디스크 사용:")) " " (format_size(disk)) } }
            (table(&[l.t("Account", "계정"), l.t("Datasets", "데이터셋"), l.t("Stored", "저장량")], usage_rows, "—"))
        }
    };
    let bottom = html! {
        @if ctx.me.can_submit() {
            section {
                h2 { (l.t("Upload a dataset", "데이터셋 올리기")) }
                p class="dim" { (l.t("Each upload makes a new version. Jobs mount it with `store:NAME` in `files`.",
                                     "올릴 때마다 새 버전이 생깁니다. 작업은 `files`에 `store:이름`으로 마운트합니다.")) }
                form class="stack" hx-post="/storage" hx-encoding="multipart/form-data" hx-target="#flash"
                    hx-indicator="#upload-busy" hx-disabled-elt="find button[type=submit]" {
                    div class="row" {
                        div { label for="ds-name" { (l.t("Name", "이름")) } input #ds-name name="name" required pattern="[a-z0-9][a-z0-9._-]*"; }
                        @if ctx.me.account().is_none() {
                            div { label for="ds-account" { (l.t("Account", "계정")) } input #ds-account name="account" placeholder=(l.t("default: the owner", "기본: 소유자")); }
                        }
                    }
                    div class="row" {
                        div { label for="ds-files" { (l.t("Files", "파일")) } input #ds-files type="file" name="files" multiple; }
                        div { label for="ds-folder" { (l.t("Or a folder", "또는 폴더")) } input #ds-folder type="file" name="files" webkitdirectory multiple; }
                    }
                    div class="row" {
                        button type="submit" { (l.t("Upload", "올리기")) }
                        span #upload-busy class="busy" { (l.t("Uploading…", "올리는 중…")) }
                    }
                }
            }
        }
    };
    Ok(ctx.page(
        l.t("Storage", "저장소"),
        Nav::Storage,
        top,
        Some(("10s", live)),
        bottom,
    ))
}

pub async fn versions(ctx: Ctx, Path(name): Path<String>) -> Response {
    let l = ctx.l;
    let list = match datasets(&ctx, Some(name.clone())).await {
        Ok(v) => v,
        Err(e) => return ctx.fail(e, false),
    };
    let can = ctx.me.can_submit();
    let rows = list
        .iter()
        .rev()
        .map(|d| {
            html! {
                tr {
                    td { "v" (d.version) }
                    td { (d.files) }
                    td { (format_size(d.size)) }
                    td { (d.account) }
                    td { (fmt::time(d.created_unix)) }
                    td { @if let Some(j) = d.job { a href={"/jobs/" (j)} { code { (j) } } } }
                    td {
                        @if can {
                            a href={"/storage/" (d.name) "/download.zip?version=" (d.version)} download { (l.t("Download", "받기")) }
                            " "
                            button class="link danger" hx-post={"/storage/" (d.name) "/remove"} hx-vals=(serde_json::json!({"version": d.version}).to_string())
                                hx-target="#flash" hx-confirm=(l.t("Delete this version?", "이 버전을 지울까요?")) { (l.t("Delete", "삭제")) }
                        }
                    }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { a href="/storage" { (l.t("Storage", "저장소")) } " · " (name) } }
    };
    let live = table(
        &[
            l.t("Version", "버전"),
            l.t("Files", "파일"),
            l.t("Size", "크기"),
            l.t("Account", "계정"),
            l.t("Created", "생성"),
            l.t("From job", "출처 작업"),
            "",
        ],
        rows,
        l.t("No versions.", "버전이 없습니다."),
    );
    ctx.page(&name, Nav::Storage, top, Some(("10s", live)), html! {})
}

pub async fn upload(ctx: Ctx, headers: HeaderMap, form: Multipart) -> Response {
    let res = async {
        let up = uploads::receive(form, "dataset").await?;
        let name = up
            .field("name")
            .context("a dataset name is needed")?
            .to_string();
        let account = up.field("account").map(str::to_string);
        anyhow::ensure!(!up.files.is_empty(), "choose files or a folder to upload");
        let files: Vec<(String, std::path::PathBuf)> = up
            .files
            .iter()
            .map(|r| (r.clone(), up.dir.path().join(r)))
            .collect();
        let client = ctx.st.backend.client(ctx.me.secret).await?;
        let (_, view) = client
            .store_put(&files, Some(name.clone()), account, |m| {
                tracing::debug!("dataset {name}: {m}")
            })
            .await?;
        Ok::<_, anyhow::Error>(match view {
            Some(v) => format!(
                "{}@{} ({} files, {})",
                v.name,
                v.version,
                v.files,
                format_size(v.size)
            ),
            None => name,
        })
    }
    .await;
    match res {
        Ok(what) => ctx.done(format!("{} {what}", ctx.t("Uploaded", "올렸습니다:"))),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

#[derive(Deserialize)]
pub struct VersionForm {
    version: Option<u32>,
}

pub async fn remove(
    ctx: Ctx,
    headers: HeaderMap,
    Path(name): Path<String>,
    Form(f): Form<VersionForm>,
) -> Response {
    match ctx
        .call(AdminOp::StoreRemove {
            name: name.clone(),
            version: f.version,
        })
        .await
    {
        Ok(_) => ctx.done(ctx.t(
            "Deleted. The space comes back once nothing else uses the files.",
            "지웠습니다. 다른 곳에서 쓰지 않는 파일의 공간이 회수됩니다.",
        )),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn download(
    ctx: Ctx,
    Path(name): Path<String>,
    Query(f): Query<VersionForm>,
) -> Response {
    let res = async {
        let client = ctx.st.backend.client(ctx.me.secret).await?;
        let dir = TempDir::new("ds")?;
        let files = dir.path().join("files");
        client
            .store_get(
                StoreTarget::Dataset {
                    name: name.clone(),
                    version: f.version,
                },
                &files,
                |m| tracing::debug!("dataset {name}: {m}"),
            )
            .await?;
        let zip = dir.path().join("dataset.zip");
        let zp = zip.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut z = ZipWriter::new(
                std::io::BufWriter::new(std::fs::File::create(&zp)?),
                fmt::now_unix(),
            );
            for (rel, full) in uploads::walk(&files)? {
                if rel.starts_with(pumasi_cluster::transfer::PARTIAL_DIR) {
                    continue;
                }
                z.add(&rel, &mut std::fs::File::open(&full)?)?;
            }
            z.finish()?;
            Ok(())
        })
        .await??;
        let label = match f.version {
            Some(v) => format!("{name}-v{v}.zip"),
            None => format!("{name}.zip"),
        };
        Ok::<_, anyhow::Error>((dir, zip, label))
    }
    .await;
    match res {
        Ok((dir, zip, label)) => super::jobs::zip_response(dir, &zip, &label).await,
        Err(e) => ctx.fail(e, false),
    }
}
