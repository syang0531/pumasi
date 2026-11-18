//! Request limits shared by every `pumasi ui` process (1.12). A dashboard
//! used to count failed sign-ins and each ticket's requests in its own
//! memory, so two dashboards in front of one cluster (or one restarted)
//! let a guesser or a runaway script go twice as fast. The control plane
//! now keeps the counts: a dashboard asks it for a few tokens at a time
//! ([`LimitOp::Take`]) and spends them locally, reports each failure
//! ([`LimitOp::Failed`]), and keeps a copy of the blocked addresses
//! ([`LimitOp::Blocked`]) that it refreshes every few seconds.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

/// Failures from one address that start a block...
pub const MAX_FAILURES: u32 = 10;
/// ...within this many seconds...
pub const WINDOW_SECS: u64 = 60;
/// ...and how long the block lasts.
pub const BLOCK_SECS: u64 = 300;

/// Requests one ticket may make in a burst, and per second after it.
pub const TICKET_BURST: f64 = 120.0;
pub const TICKET_RATE: f64 = 20.0;
/// Sign-ins one client address may start in a burst, and per minute.
pub const SIGN_IN_BURST: f64 = 10.0;
pub const SIGN_IN_PER_MIN: f64 = 10.0;

/// Tokens a dashboard takes from a ticket's bucket at a time: it spends
/// them before asking again, so one in eight requests waits for the
/// control plane. Unspent tokens are at most this many per dashboard.
pub const TICKET_LEASE: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Bucket {
    /// A ticket's requests; the key is the BLAKE3 hash of its secret, in
    /// hex.
    Ticket,
    /// Sign-ins started from a client address.
    SignIn,
}

impl Bucket {
    /// Its burst and its rate per second.
    pub fn params(self) -> (f64, f64) {
        match self {
            Bucket::Ticket => (TICKET_BURST, TICKET_RATE),
            Bucket::SignIn => (SIGN_IN_BURST, SIGN_IN_PER_MIN / 60.0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LimitOp {
    /// Takes up to `want` tokens (at most 64) from a bucket.
    Take {
        bucket: Bucket,
        key: String,
        want: u32,
    },
    /// A failed sign-in or wrong ticket from a client address. Answers
    /// with the blocked addresses, this one included once it is blocked.
    Failed { ip: String },
    /// The client addresses blocked now.
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LimitReply {
    /// Tokens given (possibly fewer than asked), and with none, how long
    /// until there is one.
    Taken { got: u32, wait_ms: u64 },
    /// Blocked addresses and the milliseconds left for each.
    Blocked(Vec<(String, u64)>),
}

/// A ticket's bucket key: no dashboard sends the secret itself.
pub fn ticket_key(secret: &crate::Secret) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"pumasi/limit-ticket/v1\0");
    h.update(secret);
    data_encoding::HEXLOWER.encode(&h.finalize().as_bytes()[..16])
}

/// Keys a [`Buckets`] or [`Failures`] remembers at most (the oldest go).
const MAX_TRACKED: usize = 10_000;

/// Token buckets, one per key (a ticket, a client address, a peer).
pub struct Buckets<K> {
    burst: f64,
    per_sec: f64,
    map: Mutex<HashMap<K, (f64, Instant)>>,
}

impl<K: std::hash::Hash + Eq + Clone> Buckets<K> {
    pub fn new(burst: f64, per_sec: f64) -> Self {
        Self {
            burst,
            per_sec,
            map: Mutex::default(),
        }
    }

    pub fn for_bucket(b: Bucket) -> Self {
        let (burst, rate) = b.params();
        Self::new(burst, rate)
    }

    /// Takes one token, or says how long until there is one.
    pub fn take(&self, key: &K) -> Result<(), Duration> {
        self.take_at(key, Instant::now())
    }

    pub fn take_at(&self, key: &K, now: Instant) -> Result<(), Duration> {
        match self.take_up_to_at(key, 1, now) {
            (1, _) => Ok(()),
            (_, wait) => Err(wait),
        }
    }

    /// Takes up to `want` whole tokens; with none, how long until one.
    pub fn take_up_to(&self, key: &K, want: u32) -> (u32, Duration) {
        self.take_up_to_at(key, want, Instant::now())
    }

    pub fn take_up_to_at(&self, key: &K, want: u32, now: Instant) -> (u32, Duration) {
        let mut map = self.map.lock().unwrap();
        if map.len() >= MAX_TRACKED && !map.contains_key(key) {
            // Full buckets are the same as none.
            let (burst, rate) = (self.burst, self.per_sec);
            map.retain(|_, (t, at)| *t + now.duration_since(*at).as_secs_f64() * rate < burst);
            if map.len() >= MAX_TRACKED
                && let Some(k) = map.keys().next().cloned()
            {
                map.remove(&k);
            }
        }
        let (tokens, at) = map.entry(key.clone()).or_insert((self.burst, now));
        *tokens = (*tokens + now.duration_since(*at).as_secs_f64() * self.per_sec).min(self.burst);
        *at = now;
        let got = (tokens.floor() as u32).min(want);
        *tokens -= f64::from(got);
        if got > 0 || want == 0 {
            (got, Duration::ZERO)
        } else {
            (0, Duration::from_secs_f64((1.0 - *tokens).max(0.0) / self.per_sec))
        }
    }
}

/// A copy of what a [`Buckets`] or [`Failures`] holds, as ages rather than
/// instants, so it can be written down and taken up by another process
/// (1.19: the leader replicates its limits, so a failover keeps them).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BucketCopy {
    pub key: String,
    pub tokens: f64,
    /// How long before the copy was made the bucket was last filled up.
    pub age_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureCopy {
    pub key: String,
    /// Failures in the current window and how old the window is.
    pub failures: u32,
    pub window_age_ms: Option<u64>,
    /// How long the block still lasts.
    pub blocked_ms: Option<u64>,
}

/// Entries a copy keeps at most per kind (the emptiest buckets first).
pub const MAX_COPIED: usize = 2_000;

fn ms(d: Duration) -> u64 {
    d.as_millis().min(u128::from(u64::MAX)) as u64
}

impl<K: std::hash::Hash + Eq + Clone> Buckets<K> {
    /// The buckets that are not full (a full bucket is the same as none),
    /// emptiest first, at most [`MAX_COPIED`]; `name` turns a key into
    /// text.
    pub fn copy(&self, now: Instant, name: impl Fn(&K) -> String) -> Vec<BucketCopy> {
        let map = self.map.lock().unwrap();
        let mut out: Vec<BucketCopy> = map
            .iter()
            .filter_map(|(k, (t, at))| {
                let age = now.saturating_duration_since(*at);
                let level = (*t + age.as_secs_f64() * self.per_sec).min(self.burst);
                (level < self.burst).then(|| BucketCopy {
                    key: name(k),
                    tokens: *t,
                    age_ms: ms(age),
                })
            })
            .collect();
        out.sort_by(|a, b| a.tokens.total_cmp(&b.tokens));
        out.truncate(MAX_COPIED);
        out
    }

    /// Takes up a copy made `elapsed` ago: each bucket becomes the copy's
    /// level, or stays lower when it already is.
    pub fn restore(
        &self,
        copy: &[BucketCopy],
        elapsed: Duration,
        now: Instant,
        key: impl Fn(&str) -> Option<K>,
    ) {
        let mut map = self.map.lock().unwrap();
        for c in copy {
            let Some(k) = key(&c.key) else { continue };
            if map.len() >= MAX_TRACKED && !map.contains_key(&k) {
                break;
            }
            let at = now
                .checked_sub(Duration::from_millis(c.age_ms) + elapsed)
                .unwrap_or(now);
            let tokens = c.tokens.clamp(0.0, self.burst);
            let level = |t: f64, at: Instant| {
                t + now.saturating_duration_since(at).as_secs_f64() * self.per_sec
            };
            match map.get(&k) {
                Some(&(t, a)) if level(t, a) <= level(tokens, at) => {}
                _ => {
                    map.insert(k, (tokens, at));
                }
            }
        }
    }
}

#[derive(Default)]
struct FailEntry {
    window_start: Option<Instant>,
    failures: u32,
    blocked_until: Option<Instant>,
}

/// Failed sign-ins per key: [`MAX_FAILURES`] within [`WINDOW_SECS`]
/// block the key for [`BLOCK_SECS`].
pub struct Failures<K: std::hash::Hash + Eq> {
    map: Mutex<HashMap<K, FailEntry>>,
}

impl<K: std::hash::Hash + Eq> Default for Failures<K> {
    fn default() -> Self {
        Self {
            map: Mutex::default(),
        }
    }
}

impl<K: std::hash::Hash + Eq + Clone> Failures<K> {
    /// How long `key` stays blocked, if it is.
    pub fn blocked(&self, key: &K) -> Option<Duration> {
        let now = Instant::now();
        let map = self.map.lock().unwrap();
        map.get(key)
            .and_then(|e| e.blocked_until)
            .filter(|u| *u > now)
            .map(|u| u - now)
    }

    /// Every blocked key and how long it stays blocked.
    pub fn all_blocked(&self) -> Vec<(K, Duration)> {
        let now = Instant::now();
        let map = self.map.lock().unwrap();
        map.iter()
            .filter_map(|(k, e)| {
                e.blocked_until
                    .filter(|u| *u > now)
                    .map(|u| (k.clone(), u - now))
            })
            .collect()
    }

    /// Blocks `key` until `until` (a copy of a block made elsewhere).
    pub fn block_until(&self, key: K, until: Instant) {
        let mut map = self.map.lock().unwrap();
        let e = map.entry(key).or_default();
        if e.blocked_until.is_none_or(|u| u < until) {
            e.blocked_until = Some(until);
        }
    }

    /// Keys with failures in an open window or a block, at most
    /// [`MAX_COPIED`] (blocks first).
    pub fn copy(&self, now: Instant, name: impl Fn(&K) -> String) -> Vec<FailureCopy> {
        let window = Duration::from_secs(WINDOW_SECS);
        let map = self.map.lock().unwrap();
        let mut out: Vec<FailureCopy> = map
            .iter()
            .filter_map(|(k, e)| {
                let blocked = e.blocked_until.filter(|u| *u > now).map(|u| ms(u - now));
                let window = e
                    .window_start
                    .map(|s| now.saturating_duration_since(s))
                    .filter(|age| *age < window && e.failures > 0);
                (blocked.is_some() || window.is_some()).then(|| FailureCopy {
                    key: name(k),
                    failures: if window.is_some() { e.failures } else { 0 },
                    window_age_ms: window.map(ms),
                    blocked_ms: blocked,
                })
            })
            .collect();
        out.sort_by_key(|c| (c.blocked_ms.is_none(), std::cmp::Reverse(c.failures)));
        out.truncate(MAX_COPIED);
        out
    }

    /// Takes up a copy made `elapsed` ago, keeping whatever is worse of the
    /// copy and what is here already.
    pub fn restore(
        &self,
        copy: &[FailureCopy],
        elapsed: Duration,
        now: Instant,
        key: impl Fn(&str) -> Option<K>,
    ) {
        let mut map = self.map.lock().unwrap();
        for c in copy {
            let Some(k) = key(&c.key) else { continue };
            if map.len() >= MAX_TRACKED && !map.contains_key(&k) {
                break;
            }
            let e = map.entry(k).or_default();
            if let Some(left) = c.blocked_ms.map(Duration::from_millis)
                && let Some(left) = left.checked_sub(elapsed)
            {
                let until = now + left;
                if e.blocked_until.is_none_or(|u| u < until) {
                    e.blocked_until = Some(until);
                }
            }
            if let Some(age) = c.window_age_ms.map(Duration::from_millis)
                && c.failures > e.failures
                && let Some(start) = now.checked_sub(age + elapsed)
            {
                e.window_start = Some(start);
                e.failures = c.failures;
            }
        }
    }

    /// Counts a failure; true when it started a block.
    pub fn failed(&self, key: K) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(WINDOW_SECS);
        let mut map = self.map.lock().unwrap();
        if map.len() >= MAX_TRACKED && !map.contains_key(&key) {
            map.retain(|_, e| {
                e.blocked_until.is_some_and(|u| u > now)
                    || e.window_start
                        .is_some_and(|s| now.duration_since(s) < window)
            });
            if map.len() >= MAX_TRACKED
                && let Some(k) = map.keys().next().cloned()
            {
                map.remove(&k);
            }
        }
        let e = map.entry(key).or_default();
        if e.blocked_until.is_some_and(|u| u > now) {
            return false;
        }
        match e.window_start {
            Some(s) if now.duration_since(s) < window => e.failures += 1,
            _ => {
                e.window_start = Some(now);
                e.failures = 1;
            }
        }
        if e.failures >= MAX_FAILURES {
            e.blocked_until = Some(now + Duration::from_secs(BLOCK_SECS));
            e.failures = 0;
            e.window_start = None;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_up_to_gives_what_there_is() {
        let b: Buckets<u8> = Buckets::new(10.0, 1.0);
        let t = Instant::now();
        assert_eq!(b.take_up_to_at(&1, 8, t).0, 8);
        assert_eq!(b.take_up_to_at(&1, 8, t).0, 2);
        let (got, wait) = b.take_up_to_at(&1, 8, t);
        assert_eq!(got, 0);
        assert!(wait > Duration::from_millis(900), "{wait:?}");
        // A second later there is one again.
        assert_eq!(b.take_up_to_at(&1, 8, t + Duration::from_secs(1)).0, 1);
        // Other keys have their own.
        assert_eq!(b.take_up_to_at(&2, 3, t).0, 3);
    }

    #[test]
    fn failures_block_and_copies_of_blocks_hold() {
        let f: Failures<&str> = Failures::default();
        for i in 1..MAX_FAILURES {
            assert!(!f.failed("a"), "blocked after {i}");
        }
        assert!(f.failed("a"));
        assert!(f.blocked(&"a").unwrap() > Duration::from_secs(BLOCK_SECS - 5));
        assert!(f.blocked(&"b").is_none());
        f.block_until("b", Instant::now() + Duration::from_secs(30));
        assert_eq!(f.all_blocked().len(), 2);
    }

    #[test]
    fn copies_carry_levels_failures_and_blocks() {
        let now = Instant::now();
        let b: Buckets<String> = Buckets::new(10.0, 1.0);
        assert_eq!(b.take_up_to_at(&"a".to_string(), 8, now).0, 8);
        b.take_up_to_at(&"full".to_string(), 0, now);
        let copy = b.copy(now, |k| k.clone());
        assert_eq!(copy.len(), 1, "full buckets are left out: {copy:?}");
        let other: Buckets<String> = Buckets::new(10.0, 1.0);
        other.restore(&copy, Duration::ZERO, now, |k| Some(k.to_string()));
        assert_eq!(other.take_up_to_at(&"a".to_string(), 8, now).0, 2);

        let f: Failures<String> = Failures::default();
        for _ in 0..4 {
            f.failed("x".into());
        }
        for _ in 0..MAX_FAILURES {
            f.failed("y".into());
        }
        let copy = f.copy(Instant::now(), |k| k.clone());
        assert_eq!(copy.len(), 2);
        let g: Failures<String> = Failures::default();
        g.restore(&copy, Duration::from_secs(1), Instant::now(), |k| {
            Some(k.to_string())
        });
        assert!(g.blocked(&"y".to_string()).is_some());
        // Four failures came along: six more block.
        for _ in 0..MAX_FAILURES - 5 {
            assert!(!g.failed("x".into()));
        }
        assert!(g.failed("x".into()));
    }

    #[test]
    fn ticket_keys_hide_the_secret() {
        let k = ticket_key(&[7; 32]);
        assert_eq!(k.len(), 32);
        assert_ne!(k, ticket_key(&[8; 32]));
    }
}
