//! Single sign-on (phase 17): OpenID Connect (browser and device flows)
//! and LDAP, with rules that map groups to roles. A successful sign-in
//! becomes an ordinary access ticket named `sso:<who>` or `ldap:<user>`
//! that expires after the session length; the role checks of phase 10
//! apply to it unchanged.
//!
//! What the control plane checks:
//!
//! - OIDC: the authorization code is exchanged by the control plane itself
//!   (with the client secret and a PKCE verifier only it knows), and the ID
//!   token's signature (asymmetric algorithms only, keys from the
//!   provider's JWKS), issuer, audience, `azp`, expiry and nonce are
//!   checked. Email rules match only addresses the provider marked
//!   verified.
//! - LDAP: the user is looked up with the service account, then bound as
//!   themselves with the password; empty passwords are refused (an LDAP
//!   "unauthenticated bind" would succeed), filter values are escaped,
//!   and failed passwords are limited per user name.
//!
//! Sign-ins under way live in memory on the leader; a failover in the
//! middle of one means signing in again.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use pumasi_proto::{
    manage::AccessRole,
    sso::{
        DEFAULT_SESSION_HOURS, DeviceStart, Identity, LdapSettings, MAX_SESSION_HOURS,
        OidcSettings, RoleMap, SsoInfo, SsoUpdate, SsoView, role_for,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::{
    audit::Actor,
    manage::AccessRecord,
    state::{State, now_unix},
};

/// How long a browser has to come back from the provider.
const PENDING_TTL: Duration = Duration::from_secs(600);
/// Sign-ins under way at most (the oldest are dropped).
const MAX_PENDING: usize = 1000;
/// Discovery documents and keys are fetched again after this.
const CACHE_TTL: Duration = Duration::from_secs(3600);
/// Keys are fetched again for an unknown `kid` at most this often.
const JWKS_REFRESH: Duration = Duration::from_secs(60);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// Failed LDAP passwords per user name before a pause.
const LDAP_MAX_FAILURES: u32 = 5;
const LDAP_WINDOW: Duration = Duration::from_secs(300);
const LDAP_BLOCK: Duration = Duration::from_secs(300);
/// Live sessions kept per person (the oldest are revoked).
const MAX_SESSIONS_PER_NAME: usize = 10;
/// Clock skew allowed for ID tokens.
const LEEWAY_SECS: u64 = 60;

/// The stored SSO settings (meta key `sso`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsoConfig {
    pub oidc: Option<OidcSettings>,
    pub ldap: Option<LdapSettings>,
    pub maps: Vec<RoleMap>,
    /// 0 means [`DEFAULT_SESSION_HOURS`].
    pub session_hours: u32,
}

impl SsoConfig {
    fn hours(&self) -> u32 {
        if self.session_hours == 0 {
            DEFAULT_SESSION_HOURS
        } else {
            self.session_hours
        }
    }
}

#[derive(Default)]
pub(crate) struct SsoState {
    pub config: SsoConfig,
}

/// An access ticket name that SSO hands out.
pub fn is_sso_name(name: &str) -> bool {
    name.starts_with("sso:") || name.starts_with("ldap:")
}

impl State {
    pub(crate) fn load_sso(&mut self) -> Result<()> {
        self.sso.config = self.store.sso()?.unwrap_or_default();
        Ok(())
    }

    pub fn sso_config(&self) -> SsoConfig {
        self.sso.config.clone()
    }

    pub fn sso_view(&self) -> SsoView {
        let c = &self.sso.config;
        let now = now_unix();
        SsoView {
            oidc: c.oidc.as_ref().map(OidcSettings::view),
            ldap: c.ldap.as_ref().map(LdapSettings::view),
            maps: c.maps.clone(),
            session_hours: c.hours(),
            sessions: self
                .mgmt
                .access
                .iter()
                .filter(|r| is_sso_name(&r.name) && r.expires_unix.is_none_or(|e| e > now))
                .count() as u32,
        }
    }

    pub fn sso_update(&mut self, u: SsoUpdate) -> Result<SsoView> {
        let mut c = self.sso.config.clone();
        match u {
            SsoUpdate::Oidc(o) => {
                if let Some(o) = &o {
                    o.validate()?;
                }
                c.oidc = o.map(|b| *b);
            }
            SsoUpdate::Ldap(l) => {
                if let Some(l) = &l {
                    l.validate()?;
                }
                c.ldap = l.map(|b| *b);
            }
            SsoUpdate::Map(m) => {
                m.role.validate()?;
                if let AccessRole::Submitter { account } = &m.role {
                    self.ensure_account(account)?;
                }
                match c.maps.iter_mut().find(|x| x.subject == m.subject) {
                    Some(x) => x.role = m.role,
                    None => {
                        ensure!(c.maps.len() < 1000, "too many sign-in rules");
                        c.maps.push(m)
                    }
                }
            }
            SsoUpdate::Unmap(s) => {
                let before = c.maps.len();
                c.maps.retain(|m| m.subject != s);
                ensure!(c.maps.len() < before, "no rule for {s}");
            }
            SsoUpdate::SessionHours(h) => {
                ensure!(
                    (1..=MAX_SESSION_HOURS).contains(&h),
                    "a session lasts 1 to {MAX_SESSION_HOURS} hours"
                );
                c.session_hours = h;
            }
        }
        self.store.put_sso(&c)?;
        self.sso.config = c;
        Ok(self.sso_view())
    }

    /// What sign-in offers. `device` assumes the provider has the device
    /// flow (most do); starting one says so when it does not.
    pub fn sso_info(&self) -> SsoInfo {
        let c = &self.sso.config;
        SsoInfo {
            oidc: c.oidc.as_ref().map(|o| o.name.clone()),
            device: c.oidc.is_some(),
            ldap: c.ldap.is_some(),
        }
    }

    /// Turns a checked identity into a session ticket, or refuses it when
    /// no rule gives it a role. Logs either way.
    pub(crate) fn sso_session(
        &mut self,
        who: &Identity,
        method: &str,
        peer: &str,
    ) -> Result<AccessRecord> {
        let action = format!("{method}.login");
        let Some(role) = role_for(&self.sso.config.maps, who) else {
            let actor = Actor {
                name: who.name.clone(),
                role: "anonymous".into(),
                peer: peer.into(),
            };
            let groups = who.groups.join(", ");
            self.audit(
                &actor,
                &action,
                &who.name,
                format!("no sign-in rule matches (groups: {groups})"),
                false,
            );
            bail!(
                "{} signed in, but no rule gives them a role in this cluster (ask an admin: pumasi sso map)",
                who.name
            );
        };
        let now = now_unix();
        // Forget expired tickets, and keep each person's newest sessions.
        let expired: Vec<_> = self
            .mgmt
            .access
            .iter()
            .filter(|r| r.expires_unix.is_some_and(|e| e <= now))
            .map(|r| r.secret)
            .collect();
        let mut theirs: Vec<(u64, [u8; 32])> = self
            .mgmt
            .access
            .iter()
            .filter(|r| r.name == who.name && r.expires_unix.is_none_or(|e| e > now))
            .map(|r| (r.created_unix, r.secret))
            .collect();
        theirs.sort();
        let surplus = theirs.len().saturating_sub(MAX_SESSIONS_PER_NAME - 1);
        for secret in expired
            .into_iter()
            .chain(theirs.into_iter().take(surplus).map(|(_, s)| s))
        {
            self.store.remove_access(&secret)?;
            self.mgmt.access.retain(|r| r.secret != secret);
        }
        let hours = self.sso.config.hours();
        let r = self.create_access(
            &who.name,
            role.clone(),
            Some(Duration::from_secs(u64::from(hours) * 3600)),
        )?;
        let actor = Actor {
            name: who.name.clone(),
            role: role.to_string(),
            peer: peer.into(),
        };
        self.audit(
            &actor,
            &action,
            &who.name,
            format!("role {role}, {hours} h, groups: {}", who.groups.join(", ")),
            true,
        );
        info!(name = %who.name, %role, "signed in with {method}");
        Ok(r)
    }

    /// Logs a sign-in the provider or directory refused, for a person it
    /// knows (so the log cannot be flooded with made-up names).
    pub(crate) fn sso_refused(&mut self, name: &str, method: &str, peer: &str, why: &str) {
        let actor = Actor {
            name: name.into(),
            role: "anonymous".into(),
            peer: peer.into(),
        };
        self.audit(&actor, &format!("{method}.login"), name, why, false);
    }
}

// ---------------------------------------------------------------------------
// Talking to providers and directories
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct Discovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    #[serde(default)]
    device_authorization_endpoint: Option<String>,
}

struct PendingLogin {
    nonce: String,
    verifier: String,
    redirect_uri: String,
    issuer: String,
    at: Instant,
}

struct PendingDevice {
    device_code: String,
    issuer: String,
    interval: Duration,
    next_poll: Instant,
    expires: Instant,
}

#[derive(Default)]
struct Failures {
    window_start: Option<Instant>,
    count: u32,
    blocked_until: Option<Instant>,
}

/// The HTTP and LDAP side of SSO, and sign-ins under way. One per leader
/// term.
pub struct Runtime {
    http: Mutex<Option<reqwest::Client>>,
    discovery: tokio::sync::Mutex<Option<(String, Discovery, Instant)>>,
    jwks: tokio::sync::Mutex<Option<(String, jsonwebtoken::jwk::JwkSet, Instant)>>,
    pending: Mutex<HashMap<String, PendingLogin>>,
    devices: Mutex<HashMap<String, PendingDevice>>,
    ldap_failures: Mutex<HashMap<String, Failures>>,
    ldap_slots: tokio::sync::Semaphore,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            http: Mutex::new(None),
            discovery: Default::default(),
            jwks: Default::default(),
            pending: Default::default(),
            devices: Default::default(),
            ldap_failures: Default::default(),
            ldap_slots: tokio::sync::Semaphore::new(4),
        }
    }
}

/// 256 random bits, base64url.
fn random_token() -> String {
    let b: [u8; 32] = rand::random();
    data_encoding::BASE64URL_NOPAD.encode(&b)
}

fn pkce_challenge(verifier: &str) -> String {
    use sha2::Digest;
    data_encoding::BASE64URL_NOPAD.encode(&sha2::Sha256::digest(verifier.as_bytes()))
}

/// `application/x-www-form-urlencoded`.
fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", pct(k), pct(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A redirect URI must come back to a dashboard's callback over HTTPS (or
/// to this machine).
fn check_redirect(uri: &str) -> Result<()> {
    pumasi_proto::sso::check_url(uri, &["https"], &["http"]).context("redirect URI")?;
    let path = uri
        .split_once("://")
        .and_then(|(_, r)| r.find('/').map(|i| &r[i..]))
        .unwrap_or("");
    ensure!(
        path.split(['?', '#']).next() == Some("/sso/callback"),
        "the redirect URI must end in /sso/callback"
    );
    Ok(())
}

impl Runtime {
    fn http(&self) -> Result<reqwest::Client> {
        let mut h = self.http.lock().unwrap();
        if let Some(c) = &*h {
            return Ok(c.clone());
        }
        let c = pumasi_proto::registry::http_client()?;
        *h = Some(c.clone());
        Ok(c)
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let resp = self
            .http()?
            .get(url)
            .header("accept", "application/json")
            .timeout(HTTP_TIMEOUT)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.bytes().await?;
        ensure!(status.is_success(), "GET {url}: HTTP {status}");
        ensure!(body.len() <= 1 << 20, "GET {url}: answer too large");
        serde_json::from_slice(&body).with_context(|| format!("GET {url}: not the JSON expected"))
    }

    /// POSTs a form; answers with the status and the JSON body.
    async fn post_form(&self, url: &str, body: String) -> Result<(u16, Value)> {
        let resp = self
            .http()?
            .post(url)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("accept", "application/json")
            .body(body)
            .timeout(HTTP_TIMEOUT)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        ensure!(body.len() <= 1 << 20, "POST {url}: answer too large");
        let v: Value = serde_json::from_slice(&body)
            .with_context(|| format!("POST {url}: HTTP {status}, not JSON"))?;
        Ok((status, v))
    }

    async fn discover(&self, o: &OidcSettings) -> Result<Discovery> {
        let mut cache = self.discovery.lock().await;
        if let Some((issuer, d, at)) = &*cache
            && *issuer == o.issuer
            && at.elapsed() < CACHE_TTL
        {
            return Ok(d.clone());
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            o.issuer.trim_end_matches('/')
        );
        let d: Discovery = self.get_json(&url).await?;
        ensure!(
            d.issuer == o.issuer,
            "the provider says its issuer is {:?}, not {:?}",
            d.issuer,
            o.issuer
        );
        for u in [&d.authorization_endpoint, &d.token_endpoint, &d.jwks_uri]
            .into_iter()
            .chain(d.device_authorization_endpoint.as_ref())
        {
            pumasi_proto::sso::check_url(u, &["https"], &["http"])?;
        }
        *cache = Some((o.issuer.clone(), d.clone(), Instant::now()));
        Ok(d)
    }

    async fn key_for(&self, jwks_uri: &str, kid: Option<&str>) -> Result<jsonwebtoken::DecodingKey> {
        let mut cache = self.jwks.lock().await;
        let find = |set: &jsonwebtoken::jwk::JwkSet| -> Option<jsonwebtoken::jwk::Jwk> {
            match kid {
                Some(k) => set.find(k).cloned(),
                // Without a kid, only a set of one key is unambiguous.
                None if set.keys.len() == 1 => set.keys.first().cloned(),
                None => None,
            }
        };
        let fresh = |at: &Instant, max: Duration| at.elapsed() < max;
        if let Some((uri, set, at)) = &*cache
            && uri == jwks_uri
            && fresh(at, CACHE_TTL)
            && let Some(k) = find(set)
        {
            return Ok(jsonwebtoken::DecodingKey::from_jwk(&k)?);
        }
        // Unknown key: fetch again, unless that just happened.
        if let Some((uri, _, at)) = &*cache
            && uri == jwks_uri
            && fresh(at, JWKS_REFRESH)
        {
            bail!("the ID token's key {kid:?} is not among the provider's keys");
        }
        let set: jsonwebtoken::jwk::JwkSet = self.get_json(jwks_uri).await?;
        let k = find(&set);
        *cache = Some((jwks_uri.to_string(), set, Instant::now()));
        let k = k.with_context(|| format!("the ID token's key {kid:?} is not among the provider's keys"))?;
        Ok(jsonwebtoken::DecodingKey::from_jwk(&k)?)
    }

    /// Checks an ID token and says who it names.
    async fn verify_id_token(
        &self,
        o: &OidcSettings,
        d: &Discovery,
        token: &str,
        nonce: Option<&str>,
    ) -> Result<Identity> {
        use jsonwebtoken::Algorithm::*;
        let header = jsonwebtoken::decode_header(token).context("the ID token is malformed")?;
        // Asymmetric algorithms only: never `none` or a MAC keyed with
        // something public.
        ensure!(
            matches!(
                header.alg,
                RS256 | RS384 | RS512 | PS256 | PS384 | PS512 | ES256 | ES384 | EdDSA
            ),
            "the ID token uses {:?}, which is not accepted",
            header.alg
        );
        let key = self.key_for(&d.jwks_uri, header.kid.as_deref()).await?;
        let mut v = jsonwebtoken::Validation::new(header.alg);
        v.set_issuer(&[&o.issuer]);
        v.set_audience(&[&o.client_id]);
        v.set_required_spec_claims(&["exp", "iat", "iss", "aud", "sub"]);
        v.leeway = LEEWAY_SECS;
        let claims = jsonwebtoken::decode::<Value>(token, &key, &v)
            .context("the ID token does not check out")?
            .claims;
        identity_from_claims(o, &claims, nonce)
    }

    /// The provider's URL for a browser sign-in, and its state.
    pub async fn start(&self, o: &OidcSettings, redirect_uri: &str) -> Result<(String, String)> {
        check_redirect(redirect_uri)?;
        let d = self.discover(o).await?;
        let state = random_token();
        let nonce = random_token();
        let verifier = random_token();
        let mut scopes = vec!["openid".to_string()];
        scopes.extend(o.scopes.iter().filter(|s| *s != "openid").cloned());
        let query = form(&[
            ("response_type", "code"),
            ("client_id", &o.client_id),
            ("redirect_uri", redirect_uri),
            ("scope", &scopes.join(" ")),
            ("state", &state),
            ("nonce", &nonce),
            ("code_challenge", &pkce_challenge(&verifier)),
            ("code_challenge_method", "S256"),
        ]);
        let sep = if d.authorization_endpoint.contains('?') {
            '&'
        } else {
            '?'
        };
        let url = format!("{}{sep}{query}", d.authorization_endpoint);
        let mut p = self.pending.lock().unwrap();
        p.retain(|_, l| l.at.elapsed() < PENDING_TTL);
        if p.len() >= MAX_PENDING
            && let Some(oldest) = p.iter().min_by_key(|(_, l)| l.at).map(|(k, _)| k.clone())
        {
            p.remove(&oldest);
        }
        p.insert(
            state.clone(),
            PendingLogin {
                nonce,
                verifier,
                redirect_uri: redirect_uri.to_string(),
                issuer: o.issuer.clone(),
                at: Instant::now(),
            },
        );
        Ok((url, state))
    }

    /// Exchanges the code a browser brought back.
    pub async fn finish(&self, o: &OidcSettings, state: &str, code: &str) -> Result<Identity> {
        let login = self
            .pending
            .lock()
            .unwrap()
            .remove(state)
            .filter(|l| l.at.elapsed() < PENDING_TTL)
            .context("this sign-in is unknown or too old; start again")?;
        ensure!(
            login.issuer == o.issuer,
            "the provider changed during the sign-in; start again"
        );
        ensure!(!code.is_empty() && code.len() <= 4096, "bad authorization code");
        let d = self.discover(o).await?;
        let (status, body) = self
            .post_form(
                &d.token_endpoint,
                form(&[
                    ("grant_type", "authorization_code"),
                    ("code", code),
                    ("redirect_uri", &login.redirect_uri),
                    ("client_id", &o.client_id),
                    ("client_secret", o.client_secret.expose()),
                    ("code_verifier", &login.verifier),
                ]),
            )
            .await?;
        let token = id_token(status, &body)?;
        self.verify_id_token(o, &d, &token, Some(&login.nonce)).await
    }

    pub async fn device_start(&self, o: &OidcSettings) -> Result<DeviceStart> {
        let d = self.discover(o).await?;
        let endpoint = d
            .device_authorization_endpoint
            .clone()
            .context("the provider offers no device sign-in; sign in through the dashboard")?;
        let mut scopes = vec!["openid".to_string()];
        scopes.extend(o.scopes.iter().filter(|s| *s != "openid").cloned());
        let (status, body) = self
            .post_form(
                &endpoint,
                form(&[
                    ("client_id", &o.client_id),
                    ("client_secret", o.client_secret.expose()),
                    ("scope", &scopes.join(" ")),
                ]),
            )
            .await?;
        if status != 200 {
            bail!("the provider refused the device sign-in: {}", oauth_error(&body));
        }
        let s = |k: &str| body.get(k).and_then(Value::as_str).map(str::to_string);
        let device_code = s("device_code").context("no device_code in the answer")?;
        let user_code = s("user_code").context("no user_code in the answer")?;
        // Google calls it verification_url.
        let verification_uri = s("verification_uri")
            .or_else(|| s("verification_url"))
            .context("no verification_uri in the answer")?;
        let interval = body.get("interval").and_then(Value::as_u64).unwrap_or(5).clamp(1, 60);
        let expires = body
            .get("expires_in")
            .and_then(Value::as_u64)
            .unwrap_or(600)
            .clamp(30, 1800);
        let handle = random_token();
        let mut devs = self.devices.lock().unwrap();
        let now = Instant::now();
        devs.retain(|_, d| d.expires > now);
        ensure!(devs.len() < MAX_PENDING, "too many device sign-ins under way");
        devs.insert(
            handle.clone(),
            PendingDevice {
                device_code,
                issuer: o.issuer.clone(),
                interval: Duration::from_secs(interval),
                next_poll: now,
                expires: now + Duration::from_secs(expires),
            },
        );
        Ok(DeviceStart {
            handle,
            user_code,
            verification_uri,
            verification_uri_complete: s("verification_uri_complete"),
            interval_secs: interval,
            expires_secs: expires,
        })
    }

    /// `Ok(None)` (with the interval to wait) while the user has not
    /// finished.
    pub async fn device_poll(
        &self,
        o: &OidcSettings,
        handle: &str,
    ) -> Result<std::result::Result<Identity, u64>> {
        let (device_code, interval) = {
            let mut devs = self.devices.lock().unwrap();
            let now = Instant::now();
            let d = devs
                .get_mut(handle)
                .filter(|d| d.expires > now && d.issuer == o.issuer)
                .context("this device sign-in is unknown or expired; start again")?;
            if now < d.next_poll {
                return Ok(Err(d.interval.as_secs()));
            }
            d.next_poll = now + d.interval;
            (d.device_code.clone(), d.interval)
        };
        let disc = self.discover(o).await?;
        let (status, body) = self
            .post_form(
                &disc.token_endpoint,
                form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", &device_code),
                    ("client_id", &o.client_id),
                    ("client_secret", o.client_secret.expose()),
                ]),
            )
            .await?;
        if status != 200 {
            match body.get("error").and_then(Value::as_str) {
                Some("authorization_pending") => return Ok(Err(interval.as_secs())),
                Some("slow_down") => {
                    let slower = interval + Duration::from_secs(5);
                    if let Some(d) = self.devices.lock().unwrap().get_mut(handle) {
                        d.interval = slower;
                        d.next_poll = Instant::now() + slower;
                    }
                    return Ok(Err(slower.as_secs()));
                }
                _ => {
                    self.devices.lock().unwrap().remove(handle);
                    bail!("the device sign-in failed: {}", oauth_error(&body));
                }
            }
        }
        self.devices.lock().unwrap().remove(handle);
        let token = id_token(status, &body)?;
        self.verify_id_token(o, &disc, &token, None).await.map(Ok)
    }

    /// Whether `user` may try a password now.
    fn ldap_allowed(&self, user: &str) -> Result<()> {
        let map = self.ldap_failures.lock().unwrap();
        if let Some(until) = map.get(user).and_then(|f| f.blocked_until)
            && until > Instant::now()
        {
            bail!(
                "too many wrong passwords for {user}; try again in {} s",
                (until - Instant::now()).as_secs().max(1)
            );
        }
        Ok(())
    }

    fn ldap_failed(&self, user: &str) {
        let now = Instant::now();
        let mut map = self.ldap_failures.lock().unwrap();
        if map.len() > 10_000 {
            map.retain(|_, f| {
                f.blocked_until.is_some_and(|u| u > now)
                    || f.window_start.is_some_and(|s| now - s < LDAP_WINDOW)
            });
        }
        let f = map.entry(user.to_string()).or_default();
        match f.window_start {
            Some(s) if now - s < LDAP_WINDOW => f.count += 1,
            _ => {
                f.window_start = Some(now);
                f.count = 1;
            }
        }
        if f.count >= LDAP_MAX_FAILURES {
            f.blocked_until = Some(now + LDAP_BLOCK);
            f.count = 0;
            f.window_start = None;
            warn!(user, "too many wrong LDAP passwords; paused");
        }
    }

    /// Checks a user name and password against the directory. `Err` with
    /// `Some(name)` when the user exists but the password was wrong (that
    /// is logged; unknown users are not).
    pub async fn ldap_login(
        &self,
        l: &LdapSettings,
        user: &str,
        password: &str,
    ) -> std::result::Result<Identity, (anyhow::Error, Option<String>)> {
        let user = user.trim();
        let plain = |e: anyhow::Error| (e, None);
        if user.is_empty() || user.len() > 256 || user.chars().any(char::is_control) {
            return Err(plain(anyhow!("bad user name")));
        }
        // An empty password is an LDAP "unauthenticated bind", which many
        // servers accept as a success.
        if password.is_empty() || password.len() > 1024 {
            return Err(plain(anyhow!("wrong user name or password")));
        }
        self.ldap_allowed(user).map_err(plain)?;
        let _slot = self
            .ldap_slots
            .acquire()
            .await
            .map_err(|_| plain(anyhow!("shutting down")))?;
        let found = ldap_lookup(l, user).await.map_err(plain)?;
        let Some((dn, mut groups, mail)) = found else {
            self.ldap_failed(user);
            return Err(plain(anyhow!("wrong user name or password")));
        };
        match ldap_bind(l, &dn, password).await {
            Ok(true) => {}
            Ok(false) => {
                self.ldap_failed(user);
                return Err((
                    anyhow!("wrong user name or password"),
                    Some(format!("ldap:{user}")),
                ));
            }
            Err(e) => return Err(plain(e)),
        }
        if !l.group_base.is_empty() {
            groups.extend(ldap_groups(l, &dn, user).await.map_err(plain)?);
        }
        Ok(Identity {
            name: clip_name(&format!("ldap:{user}")),
            // Directory addresses are what the organization wrote down.
            verified_email: mail.map(|m| m.to_ascii_lowercase()),
            groups,
        })
    }
}

fn clip_name(s: &str) -> String {
    s.chars().take(64).collect()
}

fn oauth_error(body: &Value) -> String {
    let e = body.get("error").and_then(Value::as_str).unwrap_or("error");
    match body.get("error_description").and_then(Value::as_str) {
        Some(d) => format!("{e}: {}", d.chars().take(300).collect::<String>()),
        None => e.to_string(),
    }
}

fn id_token(status: u16, body: &Value) -> Result<String> {
    if status != 200 {
        bail!("the provider refused the code: {}", oauth_error(body));
    }
    body.get("id_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("the provider sent no ID token (is the openid scope allowed?)")
}

/// Who verified claims name.
fn identity_from_claims(o: &OidcSettings, c: &Value, nonce: Option<&str>) -> Result<Identity> {
    if let Some(n) = nonce {
        let got = c.get("nonce").and_then(Value::as_str).unwrap_or("");
        ensure!(
            pumasi_proto::secrets_eq_str(got, n),
            "the ID token's nonce does not match this sign-in"
        );
    }
    // With several audiences, the token must be for us.
    if let Some(Value::Array(auds)) = c.get("aud")
        && auds.len() > 1
    {
        ensure!(
            c.get("azp").and_then(Value::as_str) == Some(o.client_id.as_str()),
            "the ID token was issued to another client"
        );
    }
    if let Some(azp) = c.get("azp").and_then(Value::as_str) {
        ensure!(azp == o.client_id, "the ID token was issued to another client");
    }
    let sub = c
        .get("sub")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .context("the ID token has no subject")?;
    let verified = match c.get("email_verified") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    };
    let email = c
        .get("email")
        .and_then(Value::as_str)
        .filter(|e| verified && e.contains('@'))
        .map(str::to_ascii_lowercase);
    let groups = match c.get(&o.groups_claim) {
        Some(Value::Array(v)) => v
            .iter()
            .filter_map(Value::as_str)
            .take(1000)
            .map(str::to_string)
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    };
    let name = clip_name(&format!("sso:{}", email.as_deref().unwrap_or(sub)));
    Ok(Identity {
        name,
        verified_email: email,
        groups,
    })
}

// -- LDAP ---------------------------------------------------------------------

async fn ldap_connect(l: &LdapSettings) -> Result<ldap3::Ldap> {
    let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(pumasi_proto::registry::root_certs())
    .with_no_client_auth();
    let settings = ldap3::LdapConnSettings::new()
        .set_conn_timeout(Duration::from_secs(10))
        .set_starttls(l.starttls)
        .set_config(std::sync::Arc::new(tls));
    let (conn, mut ldap) = ldap3::LdapConnAsync::with_settings(settings, &l.url)
        .await
        .with_context(|| format!("connect to {}", l.url))?;
    ldap3::drive!(conn);
    ldap.with_timeout(Duration::from_secs(10));
    Ok(ldap)
}

fn fill(template: &str, user: &str, dn: &str) -> String {
    template
        .replace("{user}", &ldap3::ldap_escape(user))
        .replace("{dn}", &ldap3::ldap_escape(dn))
}

/// The user's DN, groups and mail, with the service account.
async fn ldap_lookup(
    l: &LdapSettings,
    user: &str,
) -> Result<Option<(String, Vec<String>, Option<String>)>> {
    let mut ldap = ldap_connect(l).await?;
    if !l.bind_dn.is_empty() {
        ldap.simple_bind(&l.bind_dn, l.bind_password.expose())
            .await?
            .success()
            .context("the service account's bind failed (check --bind-dn and its password)")?;
    }
    let group_attr = if l.group_attr.is_empty() {
        "memberOf"
    } else {
        &l.group_attr
    };
    let (entries, _) = ldap
        .search(
            &l.user_base,
            ldap3::Scope::Subtree,
            &fill(&l.user_filter, user, ""),
            vec![group_attr, "mail"],
        )
        .await?
        .success()
        .context("the user search failed")?;
    let _ = ldap.unbind().await;
    match entries.len() {
        0 => Ok(None),
        1 => {
            let e = ldap3::SearchEntry::construct(entries.into_iter().next().unwrap());
            let groups = e
                .attrs
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(group_attr))
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let mail = e
                .attrs
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("mail"))
                .and_then(|(_, v)| v.first().cloned());
            Ok(Some((e.dn, groups, mail)))
        }
        n => bail!("{n} directory entries match {user:?}; the user filter must find one"),
    }
}

/// Whether the password binds as `dn`.
async fn ldap_bind(l: &LdapSettings, dn: &str, password: &str) -> Result<bool> {
    let mut ldap = ldap_connect(l).await?;
    let r = ldap.simple_bind(dn, password).await?;
    let _ = ldap.unbind().await;
    // 49: invalidCredentials.
    match r.rc {
        0 => Ok(true),
        49 => Ok(false),
        rc => bail!("the directory refused the bind ({rc}: {})", r.text),
    }
}

/// Groups found under `group_base` (for directories without `memberOf`).
async fn ldap_groups(l: &LdapSettings, dn: &str, user: &str) -> Result<Vec<String>> {
    let mut ldap = ldap_connect(l).await?;
    if !l.bind_dn.is_empty() {
        ldap.simple_bind(&l.bind_dn, l.bind_password.expose())
            .await?
            .success()?;
    }
    let (entries, _) = ldap
        .search(
            &l.group_base,
            ldap3::Scope::Subtree,
            &fill(&l.group_filter, user, dn),
            vec!["cn"],
        )
        .await?
        .success()
        .context("the group search failed")?;
    let _ = ldap.unbind().await;
    Ok(entries
        .into_iter()
        .take(1000)
        .map(|e| ldap3::SearchEntry::construct(e).dn)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn settings() -> OidcSettings {
        OidcSettings {
            name: "Test".into(),
            issuer: "https://idp.example.com".into(),
            client_id: "pumasi".into(),
            client_secret: pumasi_proto::sso::Hidden("s".into()),
            scopes: vec!["email".into()],
            groups_claim: "groups".into(),
        }
    }

    #[test]
    fn claims_to_identity() {
        let o = settings();
        let c = json!({"sub": "123", "email": "Alice@Example.com", "email_verified": true,
                       "groups": ["eng", "ops"], "nonce": "n1", "aud": "pumasi"});
        let who = identity_from_claims(&o, &c, Some("n1")).unwrap();
        assert_eq!(who.name, "sso:alice@example.com");
        assert_eq!(who.verified_email.as_deref(), Some("alice@example.com"));
        assert_eq!(who.groups, ["eng", "ops"]);
        assert!(identity_from_claims(&o, &c, Some("n2")).is_err());
        assert!(identity_from_claims(&o, &c, Some("")).is_err());

        // Unverified email: named by subject, no email rules.
        let c = json!({"sub": "123", "email": "boss@example.com", "aud": "pumasi"});
        let who = identity_from_claims(&o, &c, None).unwrap();
        assert_eq!(who.name, "sso:123");
        assert_eq!(who.verified_email, None);

        // Several audiences need azp = us.
        let c = json!({"sub": "1", "aud": ["pumasi", "other"]});
        assert!(identity_from_claims(&o, &c, None).is_err());
        let c = json!({"sub": "1", "aud": ["pumasi", "other"], "azp": "pumasi"});
        identity_from_claims(&o, &c, None).unwrap();
        let c = json!({"sub": "1", "aud": "pumasi", "azp": "other"});
        assert!(identity_from_claims(&o, &c, None).is_err());
        assert!(identity_from_claims(&o, &json!({"aud": "pumasi"}), None).is_err());
    }

    #[test]
    fn forms_and_redirects() {
        assert_eq!(form(&[("a b", "c&d=é")]), "a%20b=c%26d%3D%C3%A9");
        assert_eq!(pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"), "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
        check_redirect("https://pumasi.example.com/sso/callback").unwrap();
        check_redirect("http://127.0.0.1:7400/sso/callback").unwrap();
        assert!(check_redirect("http://pumasi.example.com/sso/callback").is_err());
        assert!(check_redirect("https://evil.example.com/steal").is_err());
        assert!(check_redirect("https://evil.example.com/sso/callback/../x").is_err());
    }

    #[test]
    fn filters_are_escaped() {
        assert_eq!(fill("(uid={user})", "a*)(uid=*", ""), "(uid=a\\2a\\29\\28uid=\\2a)");
        assert_eq!(fill("(member={dn})", "x", "cn=a(b)"), "(member=cn=a\\28b\\29)");
    }
}
