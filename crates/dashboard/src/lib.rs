//! Web dashboard for a Pumasi cluster (phase 12), served by `pumasi ui`.
//!
//! `pumasi ui` runs a small HTTP server on the admin's own machine
//! (127.0.0.1 by default) and talks to the cluster over the same iroh
//! admin connection the CLI uses, so the control plane opens no port and
//! browsers never need to reach it. Pages are HTML rendered on the server
//! (maud templates compiled into the binary) with htmx for live updates
//! and forms; there is no JavaScript build. The same server offers a JSON
//! API under `/api/v1` (described at `/api/openapi.json`).
//!
//! Phase 15 lets programs elsewhere use that API: `--api-only` serves
//! just the API (bearer tickets, no pages, no cookies), `--tls-cert` and
//! `--tls-key` serve HTTPS, and failed sign-ins are rate limited per
//! client address (see [`limit`]). A Cloudflare Tunnel or a reverse proxy
//! in front works too.

mod accounts;
mod api;
mod auth;
mod backend;
mod charts;
mod fmt;
mod i18n;
mod limit;
mod pages;
mod tls;
mod uploads;
mod zip;

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use pumasi_proto::{net::NetMode, ticket::AdminTicket};
use tokio::net::TcpListener;

use crate::{
    auth::Sessions,
    backend::Backend,
    limit::{AuthLimiter, ClientAddr},
};

pub use limit::TrustedProxy;
pub use tls::server_config as tls_config;

pub struct UiConfig {
    /// The ticket the dashboard connects with (and, unless `login_only`,
    /// acts with for browsers that open the printed URL).
    pub ticket: AdminTicket,
    pub net: NetMode,
    pub listen: SocketAddr,
    /// Every browser signs in with its own ticket; the printed URL carries
    /// no token. Always on when `listen` is not a loopback address.
    pub login_only: bool,
    /// Largest upload (job inputs, datasets) in bytes.
    pub upload_limit: u64,
    /// Only the JSON API (`/api/v1`, `/api/openapi.json`, `/healthz`),
    /// for programs: bearer tickets only, no pages, no sessions, and no
    /// Host check (a tunnel or proxy sends its own host name).
    pub api_only: bool,
    /// Serve HTTPS with this certificate (from [`tls_config`]).
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// A header that carries the client's address when the connection
    /// comes from loopback (a tunnel or reverse proxy on this machine)
    /// or from one of `trusted_proxies`, such as `CF-Connecting-IP`;
    /// used to rate limit failed sign-ins.
    pub real_ip_header: Option<String>,
    /// Reverse proxies on other machines whose `real_ip_header` is
    /// believed too.
    pub trusted_proxies: Vec<TrustedProxy>,
    /// The address browsers reach the dashboard at (behind a tunnel or
    /// proxy, its public URL), for the SSO redirect URI
    /// `<public_url>/sso/callback`. Default: the listening address.
    pub public_url: Option<String>,
}

pub(crate) struct AppState {
    backend: Backend,
    sessions: Sessions,
    /// Opens a session with the dashboard's own ticket (`/login?token=`);
    /// `None` with `login_only`.
    token: Option<String>,
    /// Host headers accepted when listening on loopback (DNS rebinding).
    hosts: Option<Vec<String>>,
    cluster_name: Mutex<String>,
    upload_limit: u64,
    api_only: bool,
    limiter: AuthLimiter,
    /// Requests per ticket (phase 17).
    tickets: limit::Buckets<pumasi_proto::Secret>,
    /// Sign-ins started per client address.
    sign_ins: limit::Buckets<std::net::IpAddr>,
    /// Open `?wait=` long polls per ticket.
    waits: limit::Waits,
    /// Where the provider sends browsers back to.
    public_url: String,
}

pub(crate) type St = Arc<AppState>;

/// A running dashboard.
pub struct Ui {
    addr: SocketAddr,
    token: Option<String>,
    https: bool,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Ui {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The URL to open: with the sign-in token unless `login_only`.
    pub fn url(&self) -> String {
        let host = if self.addr.ip().is_unspecified() {
            "127.0.0.1".to_string()
        } else if self.addr.is_ipv6() {
            format!("[{}]", self.addr.ip())
        } else {
            self.addr.ip().to_string()
        };
        let scheme = if self.https { "https" } else { "http" };
        match &self.token {
            Some(t) => format!("{scheme}://{host}:{}/login?token={t}", self.addr.port()),
            None => format!("{scheme}://{host}:{}/", self.addr.port()),
        }
    }

    /// Serves until the task ends (it does not on its own).
    pub async fn wait(self) -> Result<()> {
        self.task.await??;
        Ok(())
    }

    pub fn stop(&self) {
        self.task.abort();
    }
}

/// Binds the listener and starts serving.
pub async fn start(config: UiConfig) -> Result<Ui> {
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("listen on {}", config.listen))?;
    start_on(listener, config).await
}

pub async fn start_on(listener: TcpListener, config: UiConfig) -> Result<Ui> {
    let addr = listener.local_addr()?;
    let loopback = addr.ip().is_loopback();
    let token = (!config.login_only && !config.api_only && loopback).then(auth::random_token);
    let hosts = (loopback && !config.api_only).then(|| {
        let p = addr.port();
        let mut hosts = vec![
            format!("127.0.0.1:{p}"),
            format!("localhost:{p}"),
            format!("[::1]:{p}"),
        ];
        // Behind a tunnel or proxy on this machine, browsers send the
        // public name: the one `--public-url` gives is accepted too.
        if let Some(u) = &config.public_url {
            hosts.extend(public_hosts(u));
        }
        hosts
    });
    let scheme = if config.tls.is_some() {
        "https"
    } else {
        "http"
    };
    let public_url = match &config.public_url {
        Some(u) => u.trim_end_matches('/').to_string(),
        None if addr.ip().is_unspecified() || addr.ip().is_loopback() => {
            format!("{scheme}://127.0.0.1:{}", addr.port())
        }
        None if addr.is_ipv6() => format!("{scheme}://[{}]:{}", addr.ip(), addr.port()),
        None => format!("{scheme}://{addr}"),
    };
    let state = Arc::new(AppState {
        backend: Backend::new(config.ticket, config.net),
        sessions: Sessions::default(),
        token: token.clone(),
        hosts,
        cluster_name: Mutex::new(String::new()),
        upload_limit: config.upload_limit,
        api_only: config.api_only,
        limiter: AuthLimiter::new(config.real_ip_header, config.trusted_proxies),
        tickets: limit::Buckets::new(limit::TICKET_BURST, limit::TICKET_RATE),
        sign_ins: limit::Buckets::new(limit::SIGN_IN_BURST, limit::SIGN_IN_PER_MIN / 60.0),
        waits: limit::Waits::default(),
        public_url,
    });
    let app = router(state).into_make_service_with_connect_info::<ClientAddr>();
    let https = config.tls.is_some();
    let task = match config.tls {
        Some(tls) => {
            let listener = tls::TlsListener::new(listener, tls)?;
            tokio::spawn(async move { axum::serve(listener, app).await })
        }
        None => tokio::spawn(async move { axum::serve(listener, app).await }),
    };
    Ok(Ui {
        addr,
        token,
        https,
        task,
    })
}

/// The Host headers browsers send for `public_url` (`https://name[:port]/...`):
/// its authority, and without the port when that is the scheme's default.
fn public_hosts(public_url: &str) -> Vec<String> {
    let (scheme, rest) = public_url
        .split_once("://")
        .unwrap_or(("https", public_url));
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority
        .rsplit('@')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if authority.is_empty() {
        return Vec::new();
    }
    let default = match scheme.to_ascii_lowercase().as_str() {
        "http" => ":80",
        _ => ":443",
    };
    let mut out = vec![authority.clone()];
    if let Some(bare) = authority.strip_suffix(default) {
        out.push(bare.to_string());
    }
    out
}

/// Largest request body except uploads and `/api/v1/op` (phase 17):
/// forms and small JSON. Without it, anyone could make the dashboard
/// buffer a body as large as the upload limit before signing in.
pub const BODY_LIMIT: usize = 1 << 20;
/// Largest `/api/v1/op` body (an admin call as JSON).
pub const OP_BODY_LIMIT: usize = 32 << 20;

fn router(state: St) -> Router {
    let uploads = DefaultBodyLimit::max(state.upload_limit.min(usize::MAX as u64) as usize);
    let api = Router::new()
        .nest("/api/v1", api::routes(uploads))
        .route("/api/openapi.json", get(api::openapi))
        .route("/healthz", get(|| async { "ok" }));
    let app = if state.api_only {
        api
    } else {
        api.merge(pages::routes(uploads))
            .route("/login", get(pages::login::page).post(pages::login::submit))
            .route("/login/ldap", post(pages::login::ldap))
            .route("/sso/start", get(pages::login::sso_start))
            .route("/sso/callback", get(pages::login::sso_callback))
            .route("/logout", post(pages::login::logout))
            .route("/lang/{lang}", get(pages::login::set_lang))
            .route("/static/{file}", get(asset))
    };
    app.layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state)
}

/// Refuses foreign Host headers on loopback and sets the security headers
/// on every response.
async fn guard(State(st): State<St>, req: Request, next: Next) -> Response {
    if let Some(hosts) = &st.hosts {
        let host = req
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if !hosts.iter().any(|h| h.eq_ignore_ascii_case(host)) {
            return (StatusCode::MISDIRECTED_REQUEST, "unexpected Host header").into_response();
        }
    }
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    let set = |h: &mut axum::http::HeaderMap, k: &'static str, v: &'static str| {
        h.entry(k).or_insert(HeaderValue::from_static(v));
    };
    set(
        h,
        "content-security-policy",
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; \
         connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
    );
    set(h, "x-frame-options", "DENY");
    set(h, "x-content-type-options", "nosniff");
    // The sign-in URL carries a token: never send it to other sites.
    // (`no-referrer` would also blank the Origin of our own form posts.)
    set(h, "referrer-policy", "same-origin");
    set(h, "cache-control", "no-store");
    resp
}

/// Aborts a background task when dropped.
pub(crate) struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

const HTMX: &[u8] = include_bytes!("../assets/htmx.min.js");
const APP_JS: &[u8] = include_bytes!("../assets/app.js");
const APP_CSS: &[u8] = include_bytes!("../assets/app.css");

async fn asset(axum::extract::Path(file): axum::extract::Path<String>) -> Response {
    let (body, mime) = match file.as_str() {
        "htmx.min.js" => (HTMX, "text/javascript; charset=utf-8"),
        "app.js" => (APP_JS, "text/javascript; charset=utf-8"),
        "app.css" => (APP_CSS, "text/css; charset=utf-8"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    (
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    /// The end-to-end tests' module is its WAT source compiled.
    /// `PUMASI_BLESS=1 cargo test -p pumasi-dashboard` rewrites it.
    #[test]
    fn e2e_fixture_matches_its_source() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("e2e/fixtures");
        let wasm = wat::parse_file(dir.join("ticker.wat")).unwrap();
        if std::env::var_os("PUMASI_BLESS").is_some() {
            std::fs::write(dir.join("ticker.wasm"), &wasm).unwrap();
        }
        assert_eq!(std::fs::read(dir.join("ticker.wasm")).unwrap(), wasm);
    }

    #[test]
    fn public_url_hosts() {
        use super::public_hosts;
        assert_eq!(
            public_hosts("https://Cluster.Example.org/"),
            ["cluster.example.org"]
        );
        assert_eq!(
            public_hosts("https://cluster.example.org:443"),
            ["cluster.example.org:443", "cluster.example.org"]
        );
        assert_eq!(
            public_hosts("https://cluster.example.org:8443/x?y"),
            ["cluster.example.org:8443"]
        );
        assert_eq!(public_hosts("http://box.lan:80"), ["box.lan:80", "box.lan"]);
        assert_eq!(public_hosts("https://[2001:db8::1]"), ["[2001:db8::1]"]);
        assert!(public_hosts("https://").is_empty());
    }
}
