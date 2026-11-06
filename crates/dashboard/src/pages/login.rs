//! Signing in and out, and the language switch.

use axum::{
    Form,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
};
use maud::{DOCTYPE, Markup, html};
use serde::Deserialize;

use pumasi_proto::{
    msg::{AdminOp, AdminResponse},
    sso::{Hidden, SsoInfo},
};

use crate::{
    St,
    auth::{self, SESSION_COOKIE},
    i18n::{self, Lang},
    limit::ClientIp,
};

#[derive(Deserialize)]
pub struct LoginQuery {
    token: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    ticket: String,
}

#[derive(Deserialize)]
pub struct LdapForm {
    user: String,
    password: String,
}

#[derive(Deserialize)]
pub struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// What sign-in methods the cluster offers (nothing, if it cannot say).
async fn sso_info(st: &St) -> SsoInfo {
    match st.backend.call([0; 32], AdminOp::SsoInfo).await {
        Ok(AdminResponse::SsoInfo(i)) => i,
        _ => SsoInfo::default(),
    }
}

async fn render(st: &St, l: Lang, error: Option<&str>, status: StatusCode) -> Response {
    let info = sso_info(st).await;
    form(l, &info, error, status)
}

/// Starts a session for an access ticket the control plane just made.
async fn session_from(st: &St, ticket: &str) -> anyhow::Result<String> {
    let t = auth::parse_ticket(st, ticket)?;
    let ident = auth::identify(st, t.secret).await?;
    Ok(st.sessions.create(ident))
}

/// Sends the browser to the OpenID Connect provider.
pub async fn sso_start(
    State(st): State<St>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
) -> Response {
    let l = Lang::from_headers(&headers);
    if let Some(ip) = ip
        && let Err(wait) = st.sign_ins.take(&ip)
    {
        return crate::limit::slow_down(
            wait,
            "too many sign-ins from this address; try again later",
        );
    }
    let redirect_uri = format!("{}/sso/callback", st.public_url);
    match st
        .backend
        .call([0; 32], AdminOp::SsoStart { redirect_uri })
        .await
    {
        Ok(AdminResponse::SsoRedirect { url, state }) => (
            [(
                header::SET_COOKIE,
                auth::sso_cookie(&state, st.public_url.starts_with("https://")),
            )],
            Redirect::to(&url),
        )
            .into_response(),
        Ok(_) => render(&st, l, Some("unexpected response"), StatusCode::BAD_GATEWAY).await,
        Err(e) => render(&st, l, Some(&format!("{e:#}")), StatusCode::BAD_GATEWAY).await,
    }
}

/// Where the provider sends the browser back.
pub async fn sso_callback(
    State(st): State<St>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let l = Lang::from_headers(&headers);
    if let Err(r) = st.limiter.check(ip) {
        return r;
    }
    if let Some(e) = q.error {
        let msg = match q.error_description {
            Some(d) => format!("{e}: {d}"),
            None => e,
        };
        return render(&st, l, Some(&msg), StatusCode::UNAUTHORIZED).await;
    }
    let (Some(code), Some(state)) = (q.code, q.state) else {
        return render(
            &st,
            l,
            Some("the provider sent no code"),
            StatusCode::BAD_REQUEST,
        )
        .await;
    };
    // The state must be the one this browser was sent off with: otherwise
    // someone could sign this browser in as themselves.
    let mine = auth::cookie(&headers, auth::SSO_COOKIE).unwrap_or_default();
    if mine.is_empty() || !auth::same(&mine, &state) {
        st.limiter.failed(ip);
        return render(
            &st,
            l,
            Some(l.t(
                "This sign-in did not start in this browser (or took too long). Try again.",
                "이 브라우저에서 시작한 로그인이 아니거나 시간이 초과되었습니다. 다시 시도하세요.",
            )),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let res = async {
        match st
            .backend
            .call([0; 32], AdminOp::SsoFinish { state, code })
            .await?
        {
            AdminResponse::AccessTicket { ticket, .. } => session_from(&st, &ticket).await,
            _ => anyhow::bail!("unexpected response"),
        }
    }
    .await;
    match res {
        Ok(id) => {
            // A page rather than a redirect: the session cookie is
            // SameSite=Strict, and a redirect still counts as part of the
            // provider's cross-site navigation, so it would not be sent.
            let body = html! {
                (DOCTYPE)
                html { head {
                    meta charset="utf-8";
                    meta http-equiv="refresh" content="0;url=/";
                    title { "Pumasi" }
                } body { p { a href="/" { (l.t("Signed in. Continue", "로그인했습니다. 계속")) } } } }
            };
            let mut r = Html(body.into_string()).into_response();
            let h = r.headers_mut();
            h.append(
                header::SET_COOKIE,
                auth::session_cookie(&id, st.public_url.starts_with("https://"))
                    .parse()
                    .expect("cookie"),
            );
            h.append(
                header::SET_COOKIE,
                auth::clear_sso_cookie().parse().expect("cookie"),
            );
            r
        }
        Err(e) => {
            st.limiter.failed(ip);
            render(&st, l, Some(&format!("{e:#}")), StatusCode::UNAUTHORIZED).await
        }
    }
}

/// Signs in with an LDAP user name and password.
pub async fn ldap(
    State(st): State<St>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    Form(f): Form<LdapForm>,
) -> Response {
    let l = Lang::from_headers(&headers);
    if let Err(r) = st.limiter.check(ip) {
        return r;
    }
    if !same_origin(&headers) {
        return render(
            &st,
            l,
            Some("cross-origin sign-in refused"),
            StatusCode::FORBIDDEN,
        )
        .await;
    }
    let res = async {
        match st
            .backend
            .call(
                [0; 32],
                AdminOp::LdapLogin {
                    user: f.user,
                    password: Hidden(f.password),
                },
            )
            .await?
        {
            AdminResponse::AccessTicket { ticket, .. } => session_from(&st, &ticket).await,
            _ => anyhow::bail!("unexpected response"),
        }
    }
    .await;
    match res {
        Ok(id) => signed_in(&st, &id),
        Err(e) => {
            st.limiter.failed(ip);
            render(&st, l, Some(&format!("{e:#}")), StatusCode::UNAUTHORIZED).await
        }
    }
}

/// A form posted from a page of this server (or by a client that sends no
/// Origin).
fn same_origin(headers: &HeaderMap) -> bool {
    match (
        headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()),
        headers.get(header::HOST).and_then(|v| v.to_str().ok()),
    ) {
        (Some(origin), Some(host)) => origin.split_once("://").map(|x| x.1) == Some(host),
        _ => true,
    }
}

fn signed_in(st: &St, id: &str) -> Response {
    (
        [(
            header::SET_COOKIE,
            auth::session_cookie(id, st.public_url.starts_with("https://")),
        )],
        Redirect::to("/"),
    )
        .into_response()
}

/// The sign-in page, or with the token `pumasi ui` printed, a session with
/// its own ticket.
pub async fn page(
    State(st): State<St>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    Query(q): Query<LoginQuery>,
) -> Response {
    let l = Lang::from_headers(&headers);
    if let Some(given) = q.token {
        if let Err(r) = st.limiter.check(ip) {
            return r;
        }
        let ok = st.token.as_deref().is_some_and(|t| auth::same(t, &given));
        if !ok {
            st.limiter.failed(ip);
            return render(
                &st,
                l,
                Some(l.t(
                    "That link is not valid (any more). Sign in with a ticket.",
                    "유효하지 않은 링크입니다. 티켓으로 로그인하세요.",
                )),
                StatusCode::UNAUTHORIZED,
            )
            .await;
        }
        return match auth::identify(&st, st.backend.own_secret()).await {
            Ok(ident) => signed_in(&st, &st.sessions.create(ident)),
            Err(e) => render(&st, l, Some(&format!("{e:#}")), StatusCode::BAD_GATEWAY).await,
        };
    }
    render(&st, l, None, StatusCode::OK).await
}

pub async fn submit(
    State(st): State<St>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    Form(f): Form<LoginForm>,
) -> Response {
    let l = Lang::from_headers(&headers);
    if let Err(r) = st.limiter.check(ip) {
        return r;
    }
    // Sign-in forms only from this origin.
    if !same_origin(&headers) {
        return render(
            &st,
            l,
            Some("cross-origin sign-in refused"),
            StatusCode::FORBIDDEN,
        )
        .await;
    }
    let res = async {
        let t = auth::parse_ticket(&st, &f.ticket)?;
        auth::identify(&st, t.secret).await
    }
    .await;
    match res {
        Ok(ident) => signed_in(&st, &st.sessions.create(ident)),
        Err(e) => {
            let msg = format!("{e:#}");
            let status = crate::pages::status_for(&msg);
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::BAD_REQUEST {
                st.limiter.failed(ip);
            }
            let status = if status == StatusCode::BAD_GATEWAY {
                status
            } else {
                StatusCode::UNAUTHORIZED
            };
            render(&st, l, Some(&msg), status).await
        }
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
    let next = q
        .next
        .filter(|n| local_path(n))
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

/// A path on this server, safe to redirect to. Browsers drop tabs and
/// newlines from URLs and read `\` as `/`, so `/\t/evil.example` would
/// become `//evil.example`, another site: only visible ASCII is allowed.
fn local_path(n: &str) -> bool {
    n.starts_with('/')
        && !n.starts_with("//")
        && !n.contains('\\')
        && n.bytes().all(|b| b.is_ascii_graphic())
}

fn form(l: Lang, sso: &SsoInfo, error: Option<&str>, status: StatusCode) -> Response {
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
                    @if let Some(name) = &sso.oidc {
                        p { a class="button" href="/sso/start" { (l.t("Sign in with ", "로그인: ")) (name) } }
                    }
                    @if sso.ldap {
                        form method="post" action="/login/ldap" class="stack" {
                            label for="user" { (l.t("Directory user", "디렉터리 사용자")) }
                            input #user name="user" type="text" required autocomplete="username" spellcheck="false";
                            label for="password" { (l.t("Password", "비밀번호")) }
                            input #password name="password" type="password" required autocomplete="current-password";
                            button type="submit" { (l.t("Sign in", "로그인")) }
                        }
                    }
                    @if sso.oidc.is_some() || sso.ldap {
                        p class="dim" { (l.t("Or with a ticket:", "또는 티켓으로:")) }
                    }
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

#[cfg(test)]
mod tests {
    #[test]
    fn language_switch_redirects_only_here() {
        for ok in ["/", "/jobs?x=1", "/login"] {
            assert!(super::local_path(ok), "{ok}");
        }
        for bad in [
            "//evil.example",
            "/\\evil.example",
            "/\t/evil.example",
            "/\n/evil.example",
            "/ /evil.example",
            "https://evil.example",
            "evil",
        ] {
            assert!(!super::local_path(bad), "{bad:?}");
        }
    }
}
