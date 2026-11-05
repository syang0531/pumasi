//! Signing in and out, and the language switch.

use axum::{
    Form,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
};
use maud::{DOCTYPE, Markup, html};
use serde::Deserialize;

use crate::{
    St,
    auth::{self, SESSION_COOKIE},
    i18n::{self, Lang},
};

#[derive(Deserialize)]
pub struct LoginQuery {
    token: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    ticket: String,
}

fn signed_in(id: &str) -> Response {
    (
        [(header::SET_COOKIE, auth::session_cookie(id))],
        Redirect::to("/"),
    )
        .into_response()
}

/// The sign-in page, or with the token `pumasi ui` printed, a session with
/// its own ticket.
pub async fn page(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<LoginQuery>,
) -> Response {
    let l = Lang::from_headers(&headers);
    if let Some(given) = q.token {
        let ok = st.token.as_deref().is_some_and(|t| auth::same(t, &given));
        if !ok {
            return form(
                l,
                Some(l.t(
                    "That link is not valid (any more). Sign in with a ticket.",
                    "유효하지 않은 링크입니다. 티켓으로 로그인하세요.",
                )),
                StatusCode::UNAUTHORIZED,
            );
        }
        return match auth::identify(&st, st.backend.own_secret()).await {
            Ok(ident) => signed_in(&st.sessions.create(ident)),
            Err(e) => form(l, Some(&format!("{e:#}")), StatusCode::BAD_GATEWAY),
        };
    }
    form(l, None, StatusCode::OK)
}

pub async fn submit(
    State(st): State<St>,
    headers: HeaderMap,
    Form(f): Form<LoginForm>,
) -> Response {
    let l = Lang::from_headers(&headers);
    // Sign-in forms only from this origin.
    if let (Some(origin), Some(host)) = (
        headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()),
        headers.get(header::HOST).and_then(|v| v.to_str().ok()),
    ) && origin.split_once("://").map(|x| x.1) != Some(host)
    {
        return form(
            l,
            Some("cross-origin sign-in refused"),
            StatusCode::FORBIDDEN,
        );
    }
    let res = async {
        let t = auth::parse_ticket(&st, &f.ticket)?;
        auth::identify(&st, t.secret).await
    }
    .await;
    match res {
        Ok(ident) => signed_in(&st.sessions.create(ident)),
        Err(e) => form(l, Some(&format!("{e:#}")), StatusCode::UNAUTHORIZED),
    }
}

pub async fn logout(State(st): State<St>, headers: HeaderMap) -> Response {
    // Logging out needs no CSRF token: the worst a forged one does is
    // sign someone out.
    if let Some(id) = auth::cookie(&headers, SESSION_COOKIE) {
        st.sessions.remove(&id);
    }
    (
        [
            (header::SET_COOKIE, auth::clear_session_cookie()),
            (
                header::HeaderName::from_static("hx-redirect"),
                "/login".into(),
            ),
        ],
        Redirect::to("/login"),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct LangQuery {
    next: Option<String>,
}

pub async fn set_lang(Path(lang): Path<String>, Query(q): Query<LangQuery>) -> Response {
    let Some(l) = Lang::parse(&lang) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // Only paths on this server.
    let next = q
        .next
        .filter(|n| n.starts_with('/') && !n.starts_with("//") && !n.contains('\\'))
        .unwrap_or_else(|| "/".into());
    (
        [(
            header::SET_COOKIE,
            format!(
                "{}={}; Path=/; SameSite=Lax; Max-Age=31536000",
                i18n::COOKIE,
                l.code()
            ),
        )],
        Redirect::to(&next),
    )
        .into_response()
}

fn form(l: Lang, error: Option<&str>, status: StatusCode) -> Response {
    let v = pumasi_proto::version();
    let body: Markup = html! {
        (DOCTYPE)
        html lang=(l.code()) {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (l.t("Sign in", "로그인")) " · Pumasi" }
                link rel="stylesheet" href={"/static/app.css?v=" (v)};
            }
            body class="login" {
                main class="login-box" {
                    h1 { "Pumasi" }
                    p { (l.t(
                        "Sign in with the cluster's admin ticket or an access ticket (`pumasi access create`). What you can do depends on its role.",
                        "클러스터의 관리자 티켓이나 액세스 티켓(`pumasi access create`)으로 로그인하세요. 할 수 있는 일은 티켓의 역할에 따라 다릅니다."
                    )) }
                    @if let Some(e) = error { div class="flash bad" role="alert" { (e) } }
                    form method="post" action="/login" {
                        label for="ticket" { (l.t("Ticket", "티켓")) }
                        textarea #ticket name="ticket" rows="4" required placeholder="pumasiadmin..." autocomplete="off" spellcheck="false" {}
                        button type="submit" { (l.t("Sign in", "로그인")) }
                    }
                    p class="dim" {
                        a href="/lang/ko?next=/login" { "한국어" } " · " a href="/lang/en?next=/login" { "English" }
                    }
                }
            }
        }
    };
    (status, Html(body.into_string())).into_response()
}
