//! HTML pages. Each page renders on the server; a region marked `live`
//! reloads itself every few seconds (htmx), and buttons post with htmx
//! and show their result in the flash area at the top.

pub mod access;
pub mod cu;
pub mod events;
pub mod jobs;
pub mod login;
pub mod nodes;
pub mod overview;
pub mod prices;
pub mod storage;

use axum::{
    Router,
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, request::Parts},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use maud::{DOCTYPE, Markup, html};
use pumasi_proto::{
    manage::AccessRole,
    msg::{AdminOp, AdminResponse},
};

use crate::{
    St,
    auth::{CSRF_HEADER, Identity, Page},
    i18n::Lang,
};

pub fn routes(uploads: axum::extract::DefaultBodyLimit) -> Router<St> {
    Router::new()
        .route("/", get(overview::page))
        .route("/nodes", get(nodes::list))
        .route("/nodes/{node}", get(nodes::detail))
        .route("/nodes/{node}/{action}", post(nodes::action))
        .route("/jobs", get(jobs::list).post(jobs::submit).layer(uploads))
        .route("/jobs/new", get(jobs::new))
        .route("/jobs/{id}", get(jobs::detail))
        .route("/jobs/{id}/cancel", post(jobs::cancel))
        .route("/jobs/{id}/outputs.zip", get(jobs::outputs_zip))
        .route("/jobs/{id}/receipts.json", get(jobs::receipts))
        .route("/jobs/{id}/tasks/{index}", get(jobs::task))
        .route("/jobs/{id}/tasks/{index}/stream", get(jobs::stream))
        .route("/jobs/{id}/tasks/{index}/files/{*path}", get(jobs::file))
        .route(
            "/storage",
            get(storage::list).post(storage::upload).layer(uploads),
        )
        .route("/storage/{name}", get(storage::versions))
        .route("/storage/{name}/remove", post(storage::remove))
        .route("/storage/{name}/download.zip", get(storage::download))
        .route("/cu", get(cu::page))
        .route("/cu/{action}", post(cu::action))
        .route("/access", get(access::page))
        .route("/access/{action}", post(access::action))
        .route("/prices", get(prices::page))
        .route("/events", get(events::page))
}

/// What every page needs: who is asking, in which language, and where.
pub struct Ctx {
    pub l: Lang,
    pub me: Identity,
    pub st: St,
    /// The request path with its query, for live regions to reload.
    pub uri: String,
    /// An htmx request for the `live` region only.
    pub live_only: bool,
}

impl FromRequestParts<St> for Ctx {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, st: &St) -> Result<Self, Response> {
        let Page(me) = Page::from_request_parts(parts, st).await?;
        Ok(Ctx {
            l: Lang::from_headers(&parts.headers),
            me,
            st: st.clone(),
            uri: parts
                .uri
                .path_and_query()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| "/".into()),
            live_only: is_live(&parts.headers),
        })
    }
}

fn is_live(h: &HeaderMap) -> bool {
    h.contains_key("hx-request") && h.get("hx-target").is_some_and(|v| v == "live")
}

impl Ctx {
    pub async fn call(&self, op: AdminOp) -> anyhow::Result<AdminResponse> {
        self.st.backend.call(self.me.secret, op).await
    }

    pub fn t(&self, en: &'static str, ko: &'static str) -> &'static str {
        self.l.t(en, ko)
    }

    pub fn cluster_name(&self) -> String {
        self.st.cluster_name.lock().unwrap().clone()
    }

    pub fn set_cluster_name(&self, name: &str) {
        let mut n = self.st.cluster_name.lock().unwrap();
        if *n != name {
            *n = name.to_string();
        }
    }

    /// A page: `top` stays put, `live` reloads every `every` (and after a
    /// button worked), `bottom` stays put.
    pub fn page(
        &self,
        title: &str,
        nav: Nav,
        top: Markup,
        live: Option<(&str, Markup)>,
        bottom: Markup,
    ) -> Response {
        let live = live.map(|(every, body)| {
            html! {
                div #live hx-get=(self.uri) hx-trigger={"every " (every) ", refresh from:body"}
                    hx-target="this" hx-swap="outerHTML" hx-disinherit="*" { (body) }
            }
        });
        if self.live_only {
            return Html(live.unwrap_or_default().into_string()).into_response();
        }
        Html(
            self.layout(
                title,
                nav,
                html! { (top) @if let Some(l) = live { (l) } (bottom) },
            )
            .into_string(),
        )
        .into_response()
    }

    fn layout(&self, title: &str, nav: Nav, body: Markup) -> Markup {
        let l = self.l;
        let cluster = self.cluster_name();
        let v = pumasi_proto::version();
        let csrf = format!(r#"{{"{CSRF_HEADER}": "{}"}}"#, self.me.csrf);
        let other = match l {
            Lang::En => ("ko", "한국어"),
            Lang::Ko => ("en", "English"),
        };
        html! {
            (DOCTYPE)
            html lang=(l.code()) {
                head {
                    meta charset="utf-8";
                    meta name="viewport" content="width=device-width, initial-scale=1";
                    meta name="htmx-config" content=r#"{"includeIndicatorStyles":false,"allowEval":false,"allowScriptTags":false}"#;
                    title { (title) " · Pumasi" }
                    link rel="stylesheet" href={"/static/app.css?v=" (v)};
                    script src={"/static/htmx.min.js?v=" (v)} defer {}
                    script src={"/static/app.js?v=" (v)} defer {}
                }
                body hx-headers=(csrf) {
                    header class="top" {
                        a class="brand" href="/" { "Pumasi" span class="cluster" { (cluster) } }
                        nav {
                            @for (n, href, en, ko) in NAV {
                                @if n.visible(&self.me) {
                                    a href=(href) class=[(nav == *n).then_some("active")] { (l.t(en, ko)) }
                                }
                            }
                        }
                        div class="me" {
                            span class="role" title=(role_text(l, &self.me.role)) {
                                (self.me.name) " · " (role_text(l, &self.me.role))
                            }
                            a href={"/lang/" (other.0) "?next=" (encode(&self.uri))} { (other.1) }
                            button class="link" hx-post="/logout" { (l.t("Sign out", "로그아웃")) }
                        }
                    }
                    main {
                        div #flash aria-live="polite" {}
                        (body)
                    }
                }
            }
        }
    }

    /// The result of a button: a message in the flash area, and the live
    /// region reloads.
    pub fn done(&self, msg: impl Into<String>) -> Response {
        let msg = msg.into();
        (
            [("hx-trigger", "refresh")],
            Html(html! { div class="flash ok" role="status" { (msg) } }.into_string()),
        )
            .into_response()
    }

    /// An error: in the flash area for htmx requests, else as a page.
    pub fn fail(&self, e: anyhow::Error, hx: bool) -> Response {
        let msg = format!("{e:#}");
        if hx {
            return (
                [("hx-retarget", "#flash"), ("hx-reswap", "innerHTML")],
                Html(html! { div class="flash bad" role="alert" { (msg) } }.into_string()),
            )
                .into_response();
        }
        let status = status_for(&msg);
        let mut r = self.page(
            self.t("Error", "오류"),
            Nav::None,
            html! { h1 { (self.t("Something went wrong", "문제가 생겼습니다")) } div class="flash bad" { (msg) } },
            None,
            html! {},
        );
        *r.status_mut() = status;
        r
    }
}

pub fn status_for(msg: &str) -> StatusCode {
    if msg.contains("cannot reach the control plane") || msg.contains("connection") {
        StatusCode::BAD_GATEWAY
    } else if crate::auth::is_bad_credentials(msg) {
        StatusCode::UNAUTHORIZED
    } else if msg.contains("ticket cannot")
        || msg.contains("account's policy")
        || msg.contains("belongs to")
    {
        StatusCode::FORBIDDEN
    } else if msg.starts_with("no such") {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    }
}

pub fn role_text(l: Lang, r: &AccessRole) -> String {
    match r {
        AccessRole::Admin => l.t("admin", "관리자").into(),
        AccessRole::Submitter { account } => format!("{} ({account})", l.t("submitter", "제출자")),
        AccessRole::Viewer => l.t("viewer", "열람자").into(),
    }
}

/// Percent-encodes a value for a query string.
pub fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nav {
    None,
    Overview,
    Nodes,
    Jobs,
    Storage,
    Cu,
    Access,
    Prices,
    Events,
}

impl Nav {
    fn visible(self, me: &Identity) -> bool {
        match self {
            Nav::Access => me.is_admin(),
            _ => true,
        }
    }
}

const NAV: &[(Nav, &str, &str, &str)] = &[
    (Nav::Overview, "/", "Overview", "개요"),
    (Nav::Nodes, "/nodes", "Nodes", "노드"),
    (Nav::Jobs, "/jobs", "Jobs", "작업"),
    (Nav::Storage, "/storage", "Storage", "저장소"),
    (Nav::Cu, "/cu", "CU", "CU"),
    (Nav::Access, "/access", "Invites", "초대"),
    (Nav::Prices, "/prices", "Prices", "요금"),
    (Nav::Events, "/events", "Events", "이벤트"),
];

/// A table with a header row; `empty` shows when there are no rows.
pub fn table(head: &[&str], rows: Vec<Markup>, empty: &str) -> Markup {
    html! {
        @if rows.is_empty() {
            p class="empty" { (empty) }
        } @else {
            div class="table-wrap" {
                table {
                    thead { tr { @for h in head { th { (h) } } } }
                    tbody { @for r in rows { (r) } }
                }
            }
        }
    }
}

/// A summary number.
pub fn tile(label: &str, value: impl Into<String>, note: Option<String>) -> Markup {
    let value = value.into();
    html! {
        div class="tile" {
            div class="tile-label" { (label) }
            div class="tile-value" { (value) }
            @if let Some(n) = note { div class="tile-note" { (n) } }
        }
    }
}

/// Status and nodes, also keeping the cluster's name for the header.
pub async fn status(ctx: &Ctx) -> anyhow::Result<pumasi_proto::msg::ClusterStatus> {
    match ctx.call(AdminOp::Status).await? {
        AdminResponse::Status(s) => {
            ctx.set_cluster_name(&s.cluster_name);
            Ok(s)
        }
        _ => anyhow::bail!("unexpected response"),
    }
}

/// Whether the request came from htmx (a button or form).
pub fn hx(headers: &HeaderMap) -> bool {
    headers.contains_key("hx-request")
}
