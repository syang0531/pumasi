//! Who is using the dashboard. There are no dashboard accounts: a browser
//! signs in with a Pumasi ticket (the cluster's admin ticket or an access
//! ticket from `pumasi access create`), and every call it makes carries
//! that ticket's secret, so the control plane's role checks (admin,
//! submitter, viewer) and reply filtering apply unchanged.
//!
//! - `pumasi ui` prints a URL with a random token; opening it signs the
//!   browser in with the ticket `pumasi ui` runs with (unless
//!   `--login-only`).
//! - The sign-in page takes any ticket of the same cluster.
//! - The JSON API also takes `Authorization: Bearer <ticket>` (and, with
//!   `--api-only`, nothing else).
//! - Failed sign-ins and wrong bearer tickets count against the client's
//!   address ([`crate::limit`]).
//!
//! Sessions live in memory (a restart signs everyone out) behind an
//! HttpOnly, SameSite=Strict cookie, and requests that change something
//! must repeat the session's CSRF token in the `X-Pumasi-CSRF` header.

// Rejections are finished responses.
#![allow(clippy::result_large_err)]

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, Method, StatusCode, header, request::Parts},
    response::{IntoResponse, Redirect, Response},
};
use pumasi_proto::{Secret, manage::AccessRole, ticket::AdminTicket};

use crate::St;

pub const SESSION_COOKIE: &str = "pumasi_session";
pub const CSRF_HEADER: &str = "x-pumasi-csrf";
/// A session unused for this long is dropped.
const IDLE: Duration = Duration::from_secs(12 * 3600);

/// The ticket behind a request.
#[derive(Debug, Clone)]
pub struct Identity {
    pub secret: Secret,
    /// The access ticket's name (`admin` for the cluster's own ticket).
    pub name: String,
    pub role: AccessRole,
    /// Empty for bearer tokens, which need no CSRF protection.
    pub csrf: String,
}

impl Identity {
    pub fn is_admin(&self) -> bool {
        matches!(self.role, AccessRole::Admin)
    }

    /// May submit and cancel jobs and manage datasets (its own account's,
    /// for a submitter).
    pub fn can_submit(&self) -> bool {
        !matches!(self.role, AccessRole::Viewer)
    }

    pub fn account(&self) -> Option<&str> {
        match &self.role {
            AccessRole::Submitter { account } => Some(account),
            _ => None,
        }
    }
}

#[derive(Default)]
pub struct Sessions {
    map: Mutex<HashMap<String, (Identity, Instant)>>,
}

impl Sessions {
    /// Starts a session; returns its id for the cookie.
    pub fn create(&self, mut ident: Identity) -> String {
        let id = random_token();
        ident.csrf = random_token();
        let mut map = self.map.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, (_, used)| now.duration_since(*used) < IDLE);
        map.insert(id.clone(), (ident, now));
        id
    }

    pub fn get(&self, id: &str) -> Option<Identity> {
        let mut map = self.map.lock().unwrap();
        let now = Instant::now();
        match map.get_mut(id) {
            Some((ident, used)) if now.duration_since(*used) < IDLE => {
                *used = now;
                Some(ident.clone())
            }
            Some(_) => {
                map.remove(id);
                None
            }
            None => None,
        }
    }

    pub fn remove(&self, id: &str) {
        self.map.lock().unwrap().remove(id);
    }
}

/// 160 random bits as lowercase base32.
pub fn random_token() -> String {
    let bytes: [u8; 20] = rand::random();
    data_encoding::BASE32_NOPAD
        .encode(&bytes)
        .to_ascii_lowercase()
}

/// Compares secrets without leaking where they differ.
pub fn same(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

pub fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|kv| {
            let (k, v) = kv.trim().split_once('=')?;
            (k == name).then(|| v.to_string())
        })
}

/// Holds a browser sign-in's state between the provider and the
/// callback. `Lax`, because the provider sends the browser back with a
/// cross-site navigation, which carries no `Strict` cookie.
pub const SSO_COOKIE: &str = "pumasi_sso";

pub fn sso_cookie(state: &str, secure: bool) -> String {
    format!(
        "{SSO_COOKIE}={state}; Path=/sso; HttpOnly; SameSite=Lax; Max-Age=600{}",
        if secure { "; Secure" } else { "" }
    )
}

pub fn clear_sso_cookie() -> String {
    format!("{SSO_COOKIE}=; Path=/sso; HttpOnly; SameSite=Lax; Max-Age=0")
}

pub fn session_cookie(id: &str) -> String {
    format!("{SESSION_COOKIE}={id}; Path=/; HttpOnly; SameSite=Strict")
}

pub fn clear_session_cookie() -> String {
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0")
}

/// Parses a ticket and checks it belongs to this dashboard's cluster.
pub fn parse_ticket(st: &St, text: &str) -> anyhow::Result<AdminTicket> {
    let t: AdminTicket = text
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("not a Pumasi admin or access ticket"))?;
    anyhow::ensure!(
        t.cluster.id == st.backend.cluster_id(),
        "this ticket is for another cluster"
    );
    Ok(t)
}

/// The control plane's answer for a wrong, expired or revoked ticket.
pub fn is_bad_credentials(msg: &str) -> bool {
    msg.contains("invalid admin credentials")
}

/// Who a bearer ticket is, counting failures against the client.
async fn bearer(st: &St, parts: &Parts, text: &str) -> Result<Identity, Response> {
    let ip = st.limiter.client_ip(parts);
    st.limiter.check(ip)?;
    let t = match parse_ticket(st, text) {
        Ok(t) => t,
        Err(e) => {
            st.limiter.failed(ip);
            return Err(crate::api::error(
                StatusCode::UNAUTHORIZED,
                &format!("{e:#}"),
            ));
        }
    };
    if let Some(ident) = st.limiter.identity(&t.secret) {
        return Ok(ident);
    }
    match identify(st, t.secret).await {
        Ok(ident) => {
            st.limiter.remember(&ident);
            Ok(ident)
        }
        Err(e) => {
            let msg = format!("{e:#}");
            if is_bad_credentials(&msg) {
                st.limiter.failed(ip);
                Err(crate::api::error(StatusCode::UNAUTHORIZED, &msg))
            } else {
                Err(crate::api::error(crate::pages::status_for(&msg), &msg))
            }
        }
    }
}

/// Asks the control plane who `secret` is; fails for revoked or unknown
/// tickets.
pub async fn identify(st: &St, secret: Secret) -> anyhow::Result<Identity> {
    match st
        .backend
        .call(secret, pumasi_proto::msg::AdminOp::WhoAmI)
        .await?
    {
        pumasi_proto::msg::AdminResponse::WhoAmI { name, role } => Ok(Identity {
            secret,
            name,
            role,
            csrf: String::new(),
        }),
        _ => anyhow::bail!("unexpected response"),
    }
}

fn session_of(st: &St, parts: &Parts) -> Option<Identity> {
    let id = cookie(&parts.headers, SESSION_COOKIE)?;
    st.sessions.get(&id)
}

fn csrf_ok(parts: &Parts, ident: &Identity) -> bool {
    if matches!(parts.method, Method::GET | Method::HEAD) {
        return true;
    }
    parts
        .headers
        .get(CSRF_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| same(v, &ident.csrf))
}

/// Slows down a ticket that makes requests faster than
/// [`crate::limit::TICKET_RATE`] a second (phase 17).
fn throttle(st: &St, ident: &Identity) -> Result<(), Response> {
    st.tickets.take(&ident.secret).map_err(|wait| {
        crate::limit::slow_down(wait, "too many requests with this ticket; slow down")
    })
}

/// A signed-in browser, for pages. Others are sent to the sign-in page.
pub struct Page(pub Identity);

impl FromRequestParts<St> for Page {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, st: &St) -> Result<Self, Response> {
        let Some(ident) = session_of(st, parts) else {
            if parts.headers.contains_key("hx-request") {
                return Err(([("hx-redirect", "/login")], StatusCode::UNAUTHORIZED).into_response());
            }
            return Err(Redirect::to("/login").into_response());
        };
        if !csrf_ok(parts, &ident) {
            return Err((StatusCode::FORBIDDEN, "missing or wrong CSRF token").into_response());
        }
        throttle(st, &ident)?;
        Ok(Page(ident))
    }
}

/// A signed-in browser or a bearer ticket, for the JSON API.
pub struct Api(pub Identity);

impl FromRequestParts<St> for Api {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, st: &St) -> Result<Self, Response> {
        let bearer = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if let Some(text) = bearer {
            // The control plane still checks the secret and its role on
            // every call.
            let ident = self::bearer(st, parts, text).await?;
            throttle(st, &ident)?;
            return Ok(Api(ident));
        }
        if st.api_only {
            return Err(crate::api::error(
                StatusCode::UNAUTHORIZED,
                "send Authorization: Bearer <ticket>",
            ));
        }
        match session_of(st, parts) {
            Some(ident) if csrf_ok(parts, &ident) => {
                throttle(st, &ident)?;
                Ok(Api(ident))
            }
            Some(_) => Err(crate::api::error(
                StatusCode::FORBIDDEN,
                "missing or wrong CSRF token (X-Pumasi-CSRF)",
            )),
            None => Err(crate::api::error(
                StatusCode::UNAUTHORIZED,
                "sign in, or send Authorization: Bearer <ticket>",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn cookies_and_tokens() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_static("x=1; pumasi_session=abc; y=2"),
        );
        assert_eq!(cookie(&h, SESSION_COOKIE).as_deref(), Some("abc"));
        assert_eq!(cookie(&h, "z"), None);
        let a = random_token();
        assert_eq!(a.len(), 32);
        assert_ne!(a, random_token());
        assert!(same(&a, &a.clone()));
        assert!(!same(&a, &random_token()));
        assert!(!same("a", "ab"));
    }

    #[test]
    fn sessions_expire_and_keep_csrf() {
        let s = Sessions::default();
        let id = s.create(Identity {
            secret: [1; 32],
            name: "admin".into(),
            role: AccessRole::Admin,
            csrf: String::new(),
        });
        let got = s.get(&id).unwrap();
        assert_eq!(got.csrf.len(), 32);
        assert!(s.get("nope").is_none());
        s.remove(&id);
        assert!(s.get(&id).is_none());
    }
}
