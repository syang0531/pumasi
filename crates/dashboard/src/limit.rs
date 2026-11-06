//! Slows down ticket guessing (phase 15). A dashboard reachable from the
//! internet (through a tunnel or with `--tls-cert`) sees every failed
//! sign-in and every wrong bearer ticket: after [`MAX_FAILURES`] of them
//! from one client address within [`WINDOW`], that address gets `429 Too
//! Many Requests` for [`BLOCK`]. Tickets carry 256 random bits, so this
//! is not what keeps them safe; it keeps a guesser from loading the
//! control plane and the logs.
//!
//! The client address is the TCP peer, or, when the peer is loopback and
//! `--real-ip-header` names a header (such as Cloudflare's
//! `CF-Connecting-IP`), the address in that header: a tunnel connects
//! from 127.0.0.1 for everyone.

// Rejections are finished responses.
#![allow(clippy::result_large_err)]

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{
    extract::connect_info::{ConnectInfo, Connected},
    http::{HeaderMap, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    serve::IncomingStream,
};
use pumasi_proto::Secret;

use crate::auth::Identity;

/// Failures from one address that start a block.
pub const MAX_FAILURES: u32 = 10;
/// The window failures are counted in.
pub const WINDOW: Duration = Duration::from_secs(60);
/// How long a blocked address waits.
pub const BLOCK: Duration = Duration::from_secs(5 * 60);
/// Addresses remembered at most (the oldest are dropped).
const MAX_TRACKED: usize = 10_000;
/// How long a bearer ticket's identity is remembered. The control plane
/// checks the secret on every call anyway; this only saves the `WhoAmI`
/// round trip that names the caller.
const IDENTITY_TTL: Duration = Duration::from_secs(60);

/// The peer address of a connection, plain or TLS.
#[derive(Debug, Clone, Copy)]
pub struct ClientAddr(pub SocketAddr);

impl Connected<IncomingStream<'_, tokio::net::TcpListener>> for ClientAddr {
    fn connect_info(stream: IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        ClientAddr(*stream.remote_addr())
    }
}

impl Connected<IncomingStream<'_, crate::tls::TlsListener>> for ClientAddr {
    fn connect_info(stream: IncomingStream<'_, crate::tls::TlsListener>) -> Self {
        ClientAddr(*stream.remote_addr())
    }
}

#[derive(Default)]
struct Entry {
    window_start: Option<Instant>,
    failures: u32,
    blocked_until: Option<Instant>,
}

pub struct AuthLimiter {
    real_ip_header: Option<String>,
    map: Mutex<HashMap<IpAddr, Entry>>,
    identities: Mutex<HashMap<Secret, (Identity, Instant)>>,
}

impl AuthLimiter {
    pub fn new(real_ip_header: Option<String>) -> Self {
        Self {
            real_ip_header: real_ip_header.map(|h| h.to_ascii_lowercase()),
            map: Mutex::default(),
            identities: Mutex::default(),
        }
    }

    /// The client's address for counting failures.
    pub fn client_ip(&self, parts: &Parts) -> Option<IpAddr> {
        let peer = parts
            .extensions
            .get::<ConnectInfo<ClientAddr>>()
            .map(|c| c.0.0.ip())?;
        Some(self.client_ip_of(peer, &parts.headers))
    }

    pub fn client_ip_of(&self, peer: IpAddr, headers: &HeaderMap) -> IpAddr {
        if peer.is_loopback()
            && let Some(name) = &self.real_ip_header
            && let Some(ip) = headers
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split(',').next())
                .and_then(|v| v.trim().parse::<IpAddr>().ok())
        {
            return ip;
        }
        peer
    }

    /// `Err` with a 429 response while `ip` is blocked.
    pub fn check(&self, ip: Option<IpAddr>) -> Result<(), Response> {
        let Some(ip) = ip else { return Ok(()) };
        let now = Instant::now();
        let map = self.map.lock().unwrap();
        match map.get(&ip).and_then(|e| e.blocked_until) {
            Some(until) if until > now => Err(too_many(until - now)),
            _ => Ok(()),
        }
    }

    /// Counts a failed sign-in or bearer ticket from `ip`.
    pub fn failed(&self, ip: Option<IpAddr>) {
        let Some(ip) = ip else { return };
        let now = Instant::now();
        let mut map = self.map.lock().unwrap();
        if map.len() >= MAX_TRACKED && !map.contains_key(&ip) {
            map.retain(|_, e| {
                e.blocked_until.is_some_and(|u| u > now)
                    || e.window_start
                        .is_some_and(|s| now.duration_since(s) < WINDOW)
            });
            if map.len() >= MAX_TRACKED {
                // Still full of live entries: forget an arbitrary one.
                if let Some(k) = map.keys().next().copied() {
                    map.remove(&k);
                }
            }
        }
        let e = map.entry(ip).or_default();
        if e.blocked_until.is_some_and(|u| u > now) {
            return;
        }
        match e.window_start {
            Some(s) if now.duration_since(s) < WINDOW => e.failures += 1,
            _ => {
                e.window_start = Some(now);
                e.failures = 1;
            }
        }
        if e.failures >= MAX_FAILURES {
            e.blocked_until = Some(now + BLOCK);
            e.failures = 0;
            e.window_start = None;
            tracing::warn!(%ip, "too many failed sign-ins; blocked for {}s", BLOCK.as_secs());
        }
    }

    pub fn identity(&self, secret: &Secret) -> Option<Identity> {
        let now = Instant::now();
        let map = self.identities.lock().unwrap();
        map.get(secret)
            .filter(|(_, at)| now.duration_since(*at) < IDENTITY_TTL)
            .map(|(i, _)| i.clone())
    }

    pub fn remember(&self, ident: &Identity) {
        let now = Instant::now();
        let mut map = self.identities.lock().unwrap();
        if map.len() >= 1024 {
            map.retain(|_, (_, at)| now.duration_since(*at) < IDENTITY_TTL);
        }
        if map.len() < 4096 {
            map.insert(ident.secret, (ident.clone(), now));
        }
    }
}

/// The client's address, for handlers that count failures themselves.
pub struct ClientIp(pub Option<IpAddr>);

impl axum::extract::FromRequestParts<crate::St> for ClientIp {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        st: &crate::St,
    ) -> Result<Self, Self::Rejection> {
        Ok(ClientIp(st.limiter.client_ip(parts)))
    }
}

fn too_many(left: Duration) -> Response {
    let mut r = crate::api::error(
        StatusCode::TOO_MANY_REQUESTS,
        "too many failed sign-ins from this address; try again later",
    );
    if let Ok(v) = axum::http::HeaderValue::from_str(&left.as_secs().max(1).to_string()) {
        r.headers_mut().insert(header::RETRY_AFTER, v);
    }
    r.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_after_too_many_failures() {
        let l = AuthLimiter::new(None);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        let other: IpAddr = "203.0.113.8".parse().unwrap();
        for _ in 0..MAX_FAILURES - 1 {
            l.failed(Some(ip));
        }
        assert!(l.check(Some(ip)).is_ok());
        l.failed(Some(ip));
        let r = l.check(Some(ip)).unwrap_err();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(r.headers().contains_key(header::RETRY_AFTER));
        assert!(l.check(Some(other)).is_ok());
        assert!(l.check(None).is_ok());
    }

    #[test]
    fn real_ip_header_only_from_loopback() {
        let l = AuthLimiter::new(Some("CF-Connecting-IP".into()));
        let mut h = HeaderMap::new();
        h.insert("cf-connecting-ip", "198.51.100.1".parse().unwrap());
        let lo: IpAddr = "127.0.0.1".parse().unwrap();
        let far: IpAddr = "192.0.2.9".parse().unwrap();
        assert_eq!(
            l.client_ip_of(lo, &h),
            "198.51.100.1".parse::<IpAddr>().unwrap()
        );
        // A client that reaches us directly cannot pick its own address.
        assert_eq!(l.client_ip_of(far, &h), far);
        let plain = AuthLimiter::new(None);
        assert_eq!(plain.client_ip_of(lo, &h), lo);
    }
}
