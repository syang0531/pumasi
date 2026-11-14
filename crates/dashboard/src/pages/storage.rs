//! The cluster file store: datasets, their versions, usage against
//! quotas, uploads from the browser and downloads as zip; since 1.8 also
//! the copies of each version (on control planes and storage nodes),
//! the storage nodes' quotas and scrubs, and S3 access keys.

use anyhow::Context as _;
use axum::{
    Form,
    extract::{Multipart, Path, Query},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use maud::{Markup, html};
use pumasi_proto::{
    msg::{AdminOp, AdminResponse},
    replica::{DatasetReplicas, ReplicaState, ReplicationView},
    store::{DatasetView, StoreTarget, StoreUsageView},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, table, tile};
use crate::{
    fmt::{self, format_size},
    uploads::{self, TempDir},
    zip::ZipWriter,
};

async fn replication(ctx: &Ctx, name: Option<String>) -> Option<ReplicationView> {
    match ctx.call(AdminOp::Replication { name }).await {
        Ok(AdminResponse::Replication(v)) => Some(*v),
        _ => None,
    }
}

/// "2 / 2" copies, marked when there are fewer than wanted or the leader
/// lacks the data.
fn copies_badge(l: crate::i18n::Lang, r: &DatasetReplicas) -> Markup {
    let class = if r.copies >= r.factor { "ok" } else { "warn" };
    html! {
        span class={"badge " (class)} data-copies=(r.copies) data-factor=(r.factor)
            title=(l.t("copies there are / copies wanted (control planes included)", "있는 사본 / 원하는 사본 (컨트롤 플레인 포함)")) {
            (r.copies) " / " (r.factor)
        }
        @if !r.leader_has {
            " " span class="badge bad" title=(l.t("the leader lacks this version and is fetching it back from storage nodes", "리더에 이 버전이 없어 저장 노드에서 다시 받고 있습니다")) {
                (l.t("refilling", "복구 중"))
            }
        }
    }
}

fn replication_section(ctx: &Ctx, r: &ReplicationView) -> Markup {
    let l = ctx.l;
    let node_rows = r
        .nodes
        .iter()
        .map(|n| {
            let room = match (n.quota, n.disk_total) {
                (Some(q), _) => format_size(q),
                (None, Some(t)) => format!("{} ({})", l.t("90% of disk", "디스크의 90%"), format_size(t * 9 / 10)),
                (None, None) => l.t("90% of disk", "디스크의 90%").to_string(),
            };
            html! {
                tr data-storage-node=(n.name) {
                    td { a href={"/nodes/" (n.node)} { (n.name) } }
                    td {
                        @if let Some(why) = &n.not_used {
                            span class="badge muted" title=(why) { (l.t("not used", "사용 안 함")) } " " span class="dim" { (why) }
                        } @else if n.online {
                            span class="badge ok" { (l.t("in use", "사용 중")) }
                        } @else {
                            span class="badge bad" { (l.t("offline", "오프라인")) }
                        }
                    }
                    td { (room) }
                    td { @if let Some(q) = n.quota {
                        (fmt::meter(fmt::percent(n.used, q), format!("{} / {}", format_size(n.used), format_size(q))))
                    } @else { (format_size(n.used)) } }
                    td { (n.replicas) }
                    td { @if let Some(f) = n.disk_free { (format_size(f)) } @else { "—" } }
                    td {
                        @if let Some(t) = n.scrubbed_unix { (fmt::ago(l, t)) } @else { (l.t("never", "없음")) }
                        @if n.bad > 0 { " " span class="badge bad" { (n.bad) " " (l.t("damaged", "손상")) } }
                    }
                }
            }
        })
        .collect();
    html! {
        section #replication {
            h2 { (l.t("Copies", "사본")) }
            p class="dim" {
                (l.t("Each dataset version is kept on", "데이터셋 버전마다 기본"))
                " " strong { (r.default_factor) } " "
                (l.t("copies unless it sets its own number; each control plane replica counts as one", "개의 사본을 둡니다 (데이터셋이 따로 정하지 않으면). 컨트롤 플레인 복제본 하나가 사본 하나입니다"))
                " (" (r.control_planes) "). "
                (l.t("Other copies go on nodes labelled", "나머지 사본은 다음 라벨이 붙은 노드에 둡니다:"))
                " " code { "storage=SIZE" } "."
            }
            div class="tiles" {
                (tile(l.t("Under-replicated", "사본 부족"), r.under.to_string(), Some(l.t("versions on fewer copies than wanted", "원하는 수보다 사본이 적은 버전").into())))
                (tile(l.t("Refilling the leader", "리더 복구 중"), r.leader_missing.to_string(), None))
                (tile(l.t("Leader's last scrub", "리더 마지막 검사"),
                    r.scrubbed_unix.map(|t| fmt::ago(l, t)).unwrap_or_else(|| l.t("never", "없음").into()),
                    (r.bad > 0).then(|| format!("{} {}", r.bad, l.t("damaged collections", "개 컬렉션 손상")))))
            }
            h3 { (l.t("Storage nodes", "저장 노드")) }
            (table(&[l.t("Node", "노드"), l.t("State", "상태"), l.t("Quota", "할당량"), l.t("Used", "사용"),
                     l.t("Replicas", "복제본"), l.t("Disk free", "디스크 여유"), l.t("Last scrub", "마지막 검사")],
                node_rows, l.t("No storage nodes. Label a node with storage=200G (or storage=yes) to keep copies there.",
                               "저장 노드가 없습니다. 노드에 storage=200G (또는 storage=yes) 라벨을 붙이면 사본을 둡니다.")))
        }
    }
}

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
    let rep = replication(ctx, None).await;
    let copies = |d: &DatasetView| {
        rep.as_ref().and_then(|r| {
            r.datasets
                .iter()
                .find(|x| x.name == d.name && x.version == d.version)
        })
    };
    let can_download = ctx.me.can_submit();
    let rows = list
        .iter()
        .map(|d| {
            html! {
                tr data-dataset=(d.name) {
                    td { a href={"/storage/" (d.name)} { strong { (d.name) } } }
                    td { "v" (d.version) }
                    td { @if let Some(r) = copies(d) { (copies_badge(l, r)) } @else { "—" } }
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
            (table(&[l.t("Name", "이름"), l.t("Latest", "최신"), l.t("Copies", "사본"), l.t("Files", "파일"), l.t("Size", "크기"),
                     l.t("Account", "계정"), l.t("Created", "생성"), l.t("From job", "출처 작업"), ""],
                rows, l.t("No datasets yet.", "아직 데이터셋이 없습니다.")))
        }
        @if let Some(r) = &rep { (replication_section(ctx, r)) }
        section {
            h2 { (l.t("Usage", "사용량")) }
            @if disk > 0 { p class="dim" { (l.t("On the control plane's disk:", "컨트롤 플레인 디스크 사용:")) " " (format_size(disk)) } }
            (table(&[l.t("Account", "계정"), l.t("Datasets", "데이터셋"), l.t("Stored", "저장량")], usage_rows, "—"))
        }
    };
    let keys = match ctx.call(AdminOp::S3Keys).await {
        Ok(AdminResponse::S3Keys(k)) => k,
        _ => Vec::new(),
    };
    let key_rows = keys
        .iter()
        .map(|k| {
            html! {
                tr data-s3-key=(k.id) {
                    td { code { (k.id) } }
                    td { (k.label) }
                    td { (k.name) " (" (super::role_text(l, &k.role)) ")" }
                    td { (fmt::time(k.created_unix)) }
                    td { @if let Some(t) = k.last_used_unix { (fmt::ago(l, t)) } @else { (l.t("never", "없음")) } }
                    td { button class="link danger" hx-post={"/storage/s3-keys/" (k.id) "/revoke"} hx-target="#flash"
                        hx-confirm=(l.t("Revoke this key? Clients using it stop working.", "이 키를 폐기할까요? 이 키를 쓰는 클라이언트가 멈춥니다.")) { (l.t("Revoke", "폐기")) } }
                }
            }
        })
        .collect();
    let endpoint = format!("{}/s3", ctx.st.public_url);
    let bottom = html! {
        @if ctx.me.is_admin() {
            section {
                h2 { (l.t("Copies and scrubs", "사본과 검사")) }
                form class="row" hx-post="/storage/replication" hx-target="#flash" {
                    div { label for="rep-default" { (l.t("Copies of each version (default)", "버전마다 사본 수 (기본)")) }
                        input #rep-default type="number" name="factor" min="1" max="9" required
                            value=(rep.as_ref().map_or(2, |r| r.default_factor)); }
                    button type="submit" { (l.t("Save", "저장")) }
                }
                p {
                    button hx-post="/storage/scrub" hx-target="#flash" { (l.t("Scrub now", "지금 검사")) }
                    " " span class="dim" { (l.t("Reads every copy back and checks it against its BLAKE3 hashes (also weekly on its own).",
                                                  "모든 사본을 다시 읽어 BLAKE3 해시와 대조합니다 (매주 자동으로도 합니다).")) }
                }
            }
        }
        section #s3 {
            h2 { (l.t("S3 access keys", "S3 액세스 키")) }
            p class="dim" {
                (l.t("S3 clients (aws, rclone, boto3) reach the datasets at", "S3 클라이언트(aws, rclone, boto3)는 다음 주소로 데이터셋을 씁니다:"))
                " " code { (endpoint) } " "
                (l.t("with path-style addressing and any region: buckets are datasets, objects their files. A key acts as the ticket it was made with.",
                     "(경로 방식 주소, 리전은 아무거나). 버킷은 데이터셋, 객체는 그 파일입니다. 키는 만든 티켓의 권한으로 동작합니다."))
            }
            (table(&[l.t("Key id", "키 ID"), l.t("Label", "메모"), l.t("Acts as", "권한"), l.t("Created", "생성"), l.t("Last used", "마지막 사용"), ""],
                key_rows, l.t("No S3 keys yet.", "아직 S3 키가 없습니다.")))
            form class="row" hx-post="/storage/s3-keys" hx-target="#flash" {
                div { label for="s3-label" { (l.t("Label", "메모")) } input #s3-label name="label" placeholder="laptop rclone"; }
                button type="submit" { (l.t("Create a key", "키 만들기")) }
            }
        }
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
    let rep = replication(&ctx, Some(name.clone())).await;
    let copies = |v: u32| {
        rep.as_ref()
            .and_then(|r| r.datasets.iter().find(|x| x.name == name && x.version == v))
    };
    let rows = list
        .iter()
        .rev()
        .map(|d| {
            html! {
                tr data-version=(d.version) {
                    td { "v" (d.version) }
                    td {
                        @if let Some(r) = copies(d.version) {
                            (copies_badge(l, r))
                            @for n in &r.nodes {
                                " " span class={"badge " (match (n.state, n.counted) { (ReplicaState::Held, true) => "ok", (ReplicaState::Held, false) => "muted", _ => "warn" })}
                                    title=(n.error.clone().unwrap_or_default()) data-replica=(n.name) {
                                    (n.name) " · " (match n.state { ReplicaState::Held => l.t("held", "보관"), ReplicaState::Copying => l.t("copying", "복사 중") })
                                }
                            }
                        } @else { "—" }
                    }
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
            l.t("Copies", "사본"),
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
    let own = rep
        .as_ref()
        .and_then(|r| r.datasets.iter().find(|x| x.name == name))
        .map(|r| (r.own_factor, r.factor));
    let bottom = html! {
        @if can && let Some((own, factor)) = own {
            section {
                h2 { (l.t("Copies", "사본")) }
                form class="row" hx-post="/storage/replication" hx-target="#flash" {
                    input type="hidden" name="name" value=(name);
                    div { label for="rep-factor" { (l.t("Copies of each version", "버전마다 사본 수")) }
                        input #rep-factor type="number" name="factor" min="1" max="9" value=(factor); }
                    button type="submit" { (l.t("Save", "저장")) }
                    @if own {
                        button type="submit" name="factor" value="" class="link" { (l.t("Use the cluster default", "클러스터 기본값 쓰기")) }
                    }
                }
            }
        }
    };
    ctx.page(&name, Nav::Storage, top, Some(("10s", live)), bottom)
}

#[derive(Deserialize)]
pub struct ReplicationForm {
    name: Option<String>,
    factor: Option<String>,
}

pub async fn set_replication(
    ctx: Ctx,
    headers: HeaderMap,
    Form(f): Form<ReplicationForm>,
) -> Response {
    let res = async {
        let factor = match f.factor.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(v) => Some(
                v.parse::<u8>()
                    .context("the number of copies must be 1 to 9")?,
            ),
        };
        let name = f.name.filter(|n| !n.trim().is_empty());
        anyhow::ensure!(
            name.is_some() || factor.is_some(),
            "the cluster default needs a number of copies"
        );
        ctx.call(AdminOp::SetReplication { name, factor }).await
    }
    .await;
    match res {
        Ok(_) => ctx.done(ctx.t(
            "Saved. Copies are added or dropped within a minute.",
            "저장했습니다. 1분 안에 사본이 늘거나 줄어듭니다.",
        )),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn scrub(ctx: Ctx, headers: HeaderMap) -> Response {
    match ctx.call(AdminOp::Scrub { node: None }).await {
        Ok(_) => ctx.done(ctx.t(
            "Scrubbing the control plane and every storage node; damage shows up here and in Events.",
            "컨트롤 플레인과 모든 저장 노드를 검사합니다. 손상이 있으면 여기와 이벤트에 나타납니다.",
        )),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

#[derive(Deserialize)]
pub struct KeyForm {
    #[serde(default)]
    label: String,
}

pub async fn create_key(ctx: Ctx, headers: HeaderMap, Form(f): Form<KeyForm>) -> Response {
    let l = ctx.l;
    match ctx
        .call(AdminOp::S3KeyCreate {
            label: f.label.trim().to_string(),
        })
        .await
    {
        Ok(AdminResponse::S3Key { secret, view }) => (
            [("hx-trigger", "refresh")],
            axum::response::Html(
                html! {
                    div class="flash ok" role="status" data-s3-secret {
                        p { (l.t("Key made. Copy the secret now: it is not shown again.", "키를 만들었습니다. 비밀 키는 다시 보여주지 않으니 지금 복사하세요.")) }
                        p { (l.t("Access key id", "액세스 키 ID")) ": " code #s3-new-id { (view.id) } }
                        p { (l.t("Secret access key", "비밀 액세스 키")) ": " code #s3-new-secret { (secret) } }
                    }
                }
                .into_string(),
            ),
        )
            .into_response(),
        Ok(_) => ctx.fail(anyhow::anyhow!("unexpected response"), hx(&headers)),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn revoke_key(ctx: Ctx, headers: HeaderMap, Path(id): Path<String>) -> Response {
    match ctx.call(AdminOp::S3KeyRevoke { id }).await {
        Ok(_) => ctx.done(ctx.t("Revoked.", "폐기했습니다.")),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
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
