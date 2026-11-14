//! Slows down ticket guessing (phase 15). A dashboard reachable from the
//! internet (through a tunnel or with `--tls-cert`) sees every failed
//! sign-in and every wrong bearer ticket: after [`MAX_FAILURES`] of them
//! from one client address within [`WINDOW`], that address gets `429 Too
//! Many Requests` for [`BLOCK`]. Tickets carry 256 random bits, so this
//! is not what keeps them safe; it keeps a guesser from loading the
//! control plane and the logs.
//!
//! The client address is the TCP peer, or, when `--real-ip-header` names
//! a header (such as Cloudflare's `CF-Connecting-IP`) and the peer is
//! loopback or one of the `--trusted-proxy` addresses, the address in
//! that header: a tunnel connects from 127.0.0.1 for everyone, and a
//! reverse proxy on another machine from its own address.

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

/// A reverse proxy on another machine whose `--real-ip-header` is
/// believed (`--trusted-proxy`): one address, or a network such as
/// `192.168.1.0/24`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrustedProxy {
    net: IpAddr,
    prefix: u8,
}

impl TrustedProxy {
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.net, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let shift = 32 - u32::from(self.prefix);
                u32::from(net) >> shift == u32::from(ip) >> shift
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let shift = 128 - u32::from(self.prefix);
                u128::from(net) >> shift == u128::from(ip) >> shift
            }
            _ => false,
        }
    }
}

impl std::str::FromStr for TrustedProxy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let (addr, prefix) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let net = addr
            .trim()
            .parse::<IpAddr>()
            .map_err(|_| format!("{s:?} is not an IP address or a network (192.168.1.0/24)"))?
            .to_canonical();
        let max = if net.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            None => max,
            Some(p) => p
                .trim()
                .parse::<u8>()
                .ok()
                .filter(|p| *p <= max)
                .ok_or_else(|| format!("{s:?}: the prefix length is 1 to {max}"))?,
        };
        // Everyone a proxy: any client could then pick the address it is
        // counted under.
        if prefix == 0 {
            return Err(format!(
                "{s:?} would trust the header from every address; name the proxy's address or network"
            ));
        }
        Ok(Self { net, prefix })
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
    trusted_proxies: Vec<TrustedProxy>,
    map: Mutex<HashMap<IpAddr, Entry>>,
    identities: Mutex<HashMap<Secret, (Identity, Instant)>>,
    /// Addresses the control plane says are blocked: failures every
    /// dashboard of the cluster counted (1.12).
    shared_blocked: Mutex<HashMap<IpAddr, Instant>>,
    /// Where failures go to be counted by the control plane.
    reporter: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<IpAddr>>,
}

impl AuthLimiter {
    pub fn new(real_ip_header: Option<String>, trusted_proxies: Vec<TrustedProxy>) -> Self {
        Self {
            real_ip_header: real_ip_header.map(|h| h.to_ascii_lowercase()),
            trusted_proxies,
            map: Mutex::default(),
            identities: Mutex::default(),
            shared_blocked: Mutex::default(),
            reporter: std::sync::OnceLock::new(),
        }
    }

    /// Sends failures to the control plane from now on.
    pub fn report_to(&self, tx: tokio::sync::mpsc::UnboundedSender<IpAddr>) {
        let _ = self.reporter.set(tx);
    }

    /// Replaces the copy of the control plane's blocked addresses.
    pub fn set_shared_blocked(&self, blocked: &[(String, u64)]) {
        let now = Instant::now();
        let map: HashMap<IpAddr, Instant> = blocked
            .iter()
            .filter_map(|(ip, ms)| {
                let ip: IpAddr = ip.parse().ok()?;
                Some((ip.to_canonical(), now + Duration::from_millis(*ms)))
            })
            .collect();
        *self.shared_blocked.lock().unwrap() = map;
    }

    /// The client's address for counting failures.
    pub fn client_ip(&self, parts: &Parts) -> Option<IpAddr> {
        let peer = parts
            .extensions
            .get::<ConnectInfo<ClientAddr>>()
            .map(|c| c.0.0.ip())?;
        Some(self.client_ip_of(peer, &parts.headers))
    }

    fn is_proxy(&self, ip: IpAddr) -> bool {
        self.trusted_proxies.iter().any(|p| p.contains(ip))
    }

    pub fn client_ip_of(&self, peer: IpAddr, headers: &HeaderMap) -> IpAddr {
        // On a dual-stack listener an IPv4 peer is `::ffff:a.b.c.d`.
        let peer = peer.to_canonical();
        let Some(name) = &self.real_ip_header else {
            return peer;
        };
        // A client that reaches us directly cannot pick its own address.
        if !peer.is_loopback() && !self.is_proxy(peer) {
            return peer;
        }
        // From the end: the last address is the one the proxy in front
        // of us added. Earlier ones in `X-Forwarded-For` are whatever the
        // client sent, so taking the first would let anyone pick the
        // address they are counted under. Behind a chain of proxies, the
        // addresses of the trusted ones are skipped: what a trusted proxy
        // wrote before its own entry is its word too. (A proxy may also
        // add a header line of its own after the client's: the last line
        // is the nearest.)
        let mut found = None;
        let lines = headers.get_all(name.as_str()).iter().rev();
        'lines: for line in lines {
            let Ok(line) = line.to_str() else { break };
            for part in line.rsplit(',') {
                let Ok(ip) = part.trim().parse::<IpAddr>() else {
                    break 'lines;
                };
                let ip = ip.to_canonical();
                found = Some(ip);
                if !self.is_proxy(ip) {
                    break 'lines;
                }
            }
        }
        found.unwrap_or(peer)
    }

    /// `Err` with a 429 response while `ip` is blocked.
    pub fn check(&self, ip: Option<IpAddr>) -> Result<(), Response> {
        let Some(ip) = ip else { return Ok(()) };
        let now = Instant::now();
        let local = self
            .map
            .lock()
            .unwrap()
            .get(&ip)
            .and_then(|e| e.blocked_until);
        let shared = self.shared_blocked.lock().unwrap().get(&ip).copied();
        match local.into_iter().chain(shared).max() {
            Some(until) if until > now => Err(too_many(until - now)),
            _ => Ok(()),
        }
    }

    /// Counts a failed sign-in or bearer ticket from `ip`.
    pub fn failed(&self, ip: Option<IpAddr>) {
        let Some(ip) = ip else { return };
        if let Some(tx) = self.reporter.get() {
            let _ = tx.send(ip.to_canonical());
        }
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

/// Requests one ticket may make in a burst and per second after it
/// (phase 17). A dashboard page with live regions makes well under one a
/// second; a script that loops faster than this is slowed down rather
/// than allowed to load the control plane. Since 1.12 the control plane
/// keeps the buckets for every dashboard ([`SharedLimits`]).
pub use pumasi_proto::limit::{SIGN_IN_BURST, SIGN_IN_PER_MIN, TICKET_BURST, TICKET_RATE};
/// `?wait=` long polls one ticket may hold open at once.
pub const MAX_WAITS: u32 = 8;

/// The control plane's side of the limits (1.12): every `pumasi ui` of a
/// cluster takes tokens from the same buckets there. A dashboard whose
/// ticket is not an admin's, or whose control plane is older, keeps
/// counting on its own (the local buckets), as before.
pub struct SharedLimits {
    /// Shared counting is off until this time (a refusal turns it off for
    /// good, a failed call for a minute).
    off_until: Mutex<Option<Instant>>,
    /// Tokens taken from the control plane and not spent yet, per ticket.
    leases: Mutex<HashMap<String, u32>>,
}

/// What a shared take came to.
pub enum Shared {
    Allowed,
    Wait(Duration),
    /// Not shared now: count locally.
    Local,
}

impl Default for SharedLimits {
    fn default() -> Self {
        Self {
            off_until: Mutex::new(None),
            leases: Mutex::default(),
        }
    }
}

impl SharedLimits {
    pub fn on(&self) -> bool {
        self.off_until
            .lock()
            .unwrap()
            .is_none_or(|u| Instant::now() >= u)
    }

    /// Stops sharing for a while (`None`: for good).
    pub fn pause(&self, why: &str, for_: Option<Duration>) {
        let mut off = self.off_until.lock().unwrap();
        let was_on = off.is_none_or(|u| Instant::now() >= u);
        let until = match for_ {
            Some(d) => Instant::now() + d,
            // Far enough to be never for a running dashboard.
            None => Instant::now() + Duration::from_secs(100 * 365 * 86_400),
        };
        *off = Some(until);
        if was_on {
            tracing::warn!("request limits are counted by this dashboard alone: {why}");
        }
    }

    /// Spends a leased token of `key`, if there is one.
    pub fn spend_lease(&self, key: &str) -> bool {
        let mut l = self.leases.lock().unwrap();
        match l.get_mut(key) {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }

    pub fn add_lease(&self, key: String, n: u32) {
        let mut l = self.leases.lock().unwrap();
        if l.len() >= MAX_TRACKED && !l.contains_key(&key) {
            l.retain(|_, n| *n > 0);
            if l.len() >= MAX_TRACKED {
                l.clear();
            }
        }
        *l.entry(key).or_default() += n;
    }
}

pub use pumasi_proto::limit::Buckets;

/// `429` for a ticket that is going too fast.
pub fn slow_down(wait: Duration, msg: &str) -> Response {
    let mut r = crate::api::error(StatusCode::TOO_MANY_REQUESTS, msg);
    if let Ok(v) = axum::http::HeaderValue::from_str(&wait.as_secs().max(1).to_string()) {
        r.headers_mut().insert(header::RETRY_AFTER, v);
    }
    r
}

/// Counts open long polls per ticket; the guard gives the slot back.
#[derive(Default)]
pub struct Waits {
    map: Mutex<HashMap<Secret, u32>>,
}

pub struct WaitSlot<'a> {
    waits: &'a Waits,
    secret: Secret,
}

impl Waits {
    pub fn enter(&self, secret: Secret) -> Option<WaitSlot<'_>> {
        let mut map = self.map.lock().unwrap();
        let n = map.entry(secret).or_default();
        if *n >= MAX_WAITS {
            return None;
        }
        *n += 1;
        Some(WaitSlot {
            waits: self,
            secret,
        })
    }
}

impl Drop for WaitSlot<'_> {
    fn drop(&mut self) {
        let mut map = self.waits.map.lock().unwrap();
        if let Some(n) = map.get_mut(&self.secret) {
            *n -= 1;
            if *n == 0 {
                map.remove(&self.secret);
            }
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
        let l = AuthLimiter::new(None, Vec::new());
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
    fn buckets_refill() {
        // At given times: with the clock, a slow machine refills the
        // bucket between two takes.
        let b = Buckets::new(3.0, 1000.0);
        let k = 1u8;
        let t = Instant::now();
        for _ in 0..3 {
            b.take_at(&k, t).unwrap();
        }
        let wait = b.take_at(&k, t).unwrap_err();
        assert_eq!(wait, Duration::from_millis(1));
        let half = t + Duration::from_micros(500);
        assert_eq!(b.take_at(&k, half).unwrap_err(), Duration::from_micros(500));
        b.take_at(&2u8, half).unwrap();
        let later = t + Duration::from_millis(5);
        // Five tokens' worth of time fills it to the burst and no further.
        for _ in 0..3 {
            b.take_at(&k, later).unwrap();
        }
        b.take_at(&k, later).unwrap_err();
        let slow = Buckets::new(1.0, 0.5);
        slow.take(&k).unwrap();
        assert!(slow.take(&k).unwrap_err() > Duration::from_millis(1500));
    }

    #[test]
    fn wait_slots() {
        let w = Waits::default();
        let s = [7u8; 32];
        let held: Vec<_> = (0..MAX_WAITS).map(|_| w.enter(s).unwrap()).collect();
        assert!(w.enter(s).is_none());
        assert!(w.enter([8; 32]).is_some());
        drop(held);
        assert!(w.enter(s).is_some());
        assert!(w.map.lock().unwrap().is_empty());
    }

    #[test]
    fn real_ip_header_only_from_loopback() {
        let l = AuthLimiter::new(Some("CF-Connecting-IP".into()), Vec::new());
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
        let plain = AuthLimiter::new(None, Vec::new());
        assert_eq!(plain.client_ip_of(lo, &h), lo);
        // Behind a proxy that appends to X-Forwarded-For, only the last
        // address is the proxy's word; the client wrote the rest.
        let xff = AuthLimiter::new(Some("X-Forwarded-For".into()), Vec::new());
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "10.9.9.9, 203.0.113.7".parse().unwrap());
        assert_eq!(
            xff.client_ip_of(lo, &h),
            "203.0.113.7".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn trusted_proxy_parses() {
        let one: TrustedProxy = "192.168.1.20".parse().unwrap();
        assert!(one.contains("192.168.1.20".parse().unwrap()));
        assert!(!one.contains("192.168.1.21".parse().unwrap()));
        // As a dual-stack listener reports an IPv4 peer.
        assert!(one.contains("::ffff:192.168.1.20".parse().unwrap()));
        let net: TrustedProxy = "10.1.0.0/16".parse().unwrap();
        assert!(net.contains("10.1.255.3".parse().unwrap()));
        assert!(!net.contains("10.2.0.1".parse().unwrap()));
        assert!(!net.contains("::1".parse().unwrap()));
        // Host bits in a network are ignored.
        let odd: TrustedProxy = "192.168.1.20/24".parse().unwrap();
        assert!(odd.contains("192.168.1.7".parse().unwrap()));
        let v6: TrustedProxy = "fd00:1::/32".parse().unwrap();
        assert!(v6.contains("fd00:1:2::9".parse().unwrap()));
        assert!(!v6.contains("fd00:2::9".parse().unwrap()));
        assert!(!v6.contains("10.0.0.1".parse().unwrap()));
        for bad in [
            "",
            "proxy.example.com",
            "192.168.1.0/33",
            "fd00::/129",
            "192.168.1.0/x",
            "0.0.0.0/0",
            "::/0",
        ] {
            assert!(bad.parse::<TrustedProxy>().is_err(), "{bad}");
        }
    }

    #[test]
    fn real_ip_header_from_a_trusted_proxy() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let l = AuthLimiter::new(
            Some("X-Forwarded-For".into()),
            vec![
                "192.168.1.20".parse().unwrap(),
                "10.8.0.0/24".parse().unwrap(),
            ],
        );
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "203.0.113.7".parse().unwrap());
        // The proxy on another machine speaks for its clients...
        assert_eq!(l.client_ip_of(ip("192.168.1.20"), &h), ip("203.0.113.7"));
        assert_eq!(
            l.client_ip_of(ip("::ffff:192.168.1.20"), &h),
            ip("203.0.113.7")
        );
        assert_eq!(l.client_ip_of(ip("10.8.0.3"), &h), ip("203.0.113.7"));
        assert_eq!(l.client_ip_of(ip("127.0.0.1"), &h), ip("203.0.113.7"));
        // ...its neighbour does not.
        assert_eq!(l.client_ip_of(ip("192.168.1.21"), &h), ip("192.168.1.21"));
        // Without the header, or with one that is no address, the peer.
        assert_eq!(
            l.client_ip_of(ip("192.168.1.20"), &HeaderMap::new()),
            ip("192.168.1.20")
        );
        let mut junk = HeaderMap::new();
        junk.insert("x-forwarded-for", "unknown".parse().unwrap());
        assert_eq!(
            l.client_ip_of(ip("192.168.1.20"), &junk),
            ip("192.168.1.20")
        );

        // Two proxies in a row (a tunnel here behind the proxy elsewhere,
        // or the other way round): the trusted ones are skipped, and what
        // the client wrote before them is still not believed.
        let mut chain = HeaderMap::new();
        chain.insert(
            "x-forwarded-for",
            "198.51.100.99, 203.0.113.7, 192.168.1.20".parse().unwrap(),
        );
        assert_eq!(l.client_ip_of(ip("10.8.0.3"), &chain), ip("203.0.113.7"));
        assert_eq!(l.client_ip_of(ip("127.0.0.1"), &chain), ip("203.0.113.7"));
        // A request the proxy itself made counts as the proxy.
        let mut own = HeaderMap::new();
        own.insert("x-forwarded-for", "192.168.1.20".parse().unwrap());
        assert_eq!(l.client_ip_of(ip("192.168.1.20"), &own), ip("192.168.1.20"));

        // A proxy that adds a header line of its own after the client's:
        // the last line counts.
        let mut lines = HeaderMap::new();
        lines.append("x-forwarded-for", "198.51.100.99".parse().unwrap());
        lines.append("x-forwarded-for", "203.0.113.7".parse().unwrap());
        assert_eq!(
            l.client_ip_of(ip("192.168.1.20"), &lines),
            ip("203.0.113.7")
        );

        // Without --real-ip-header a proxy's address is just an address.
        let plain = AuthLimiter::new(None, vec!["192.168.1.20".parse().unwrap()]);
        assert_eq!(
            plain.client_ip_of(ip("192.168.1.20"), &h),
            ip("192.168.1.20")
        );
    }
}
