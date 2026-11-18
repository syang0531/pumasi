//! Request limits on the control plane (1.12).
//!
//! - Sign-in calls need no ticket, so anyone who can reach the control
//!   plane could make it ask the identity provider or the directory as
//!   fast as they like. Each peer (iroh endpoint) gets a small budget,
//!   and every peer together a larger one, so changing keys does not help
//!   much either. A dashboard signs people in on their behalf with its
//!   own admin ticket and is not limited here: it limits each browser's
//!   address itself, with the counts kept below.
//! - The counts every `pumasi ui` process shares ([`LimitOp`]): token
//!   buckets per ticket and per address starting a sign-in, and failed
//!   sign-ins per address. They live in memory on the leader, which
//!   writes a copy into the replicated state every couple of seconds when
//!   they changed (1.19), so a new leader, or the same one restarted,
//!   takes them up: one write per interval, not one per request.

use std::{
    net::IpAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use pumasi_proto::{
    limit::{Bucket, BucketCopy, Buckets, FailureCopy, Failures, LimitOp, LimitReply},
    msg::AdminOp,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// The meta key of the limits' copy in the state (1.19).
pub(crate) const LIMITS_META: &str = "limits";

/// What [`Limits`] hold, written into the state ([`LIMITS_META`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct LimitsCopy {
    /// When the copy was made (UNIX milliseconds).
    pub at_unix_ms: u64,
    pub peers: Vec<BucketCopy>,
    pub polls: Vec<BucketCopy>,
    pub all: Vec<BucketCopy>,
    pub tickets: Vec<BucketCopy>,
    pub sign_ins: Vec<BucketCopy>,
    pub failures: Vec<FailureCopy>,
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Sign-in calls one peer may make in a burst, and per minute after it
/// (`ClusterConfig::sign_in_per_min`).
pub const PEER_PER_MIN: f64 = 10.0;
/// A device sign-in polls every 5 seconds or more: its own budget.
pub const POLL_BURST: f64 = 5.0;
pub const POLL_PER_MIN: f64 = 20.0;
/// Every peer without a ticket together (at least 6 and 12 times one
/// peer's budget).
pub const ALL_BURST: f64 = 60.0;
pub const ALL_PER_MIN: f64 = 120.0;
/// Most tokens one [`LimitOp::Take`] gets.
const MAX_TAKE: u32 = 64;

pub(crate) struct Limits {
    peers: Buckets<String>,
    polls: Buckets<String>,
    all: Buckets<()>,
    tickets: Buckets<String>,
    sign_ins: Buckets<String>,
    failures: Failures<IpAddr>,
    /// Something changed since the last [`Limits::copy`].
    dirty: AtomicBool,
}

impl Default for Limits {
    fn default() -> Self {
        Self::new(PEER_PER_MIN)
    }
}

impl Limits {
    /// Limits with `per_min` sign-in calls per peer (in a burst, and per
    /// minute after it).
    pub fn new(per_min: f64) -> Self {
        let per_min = per_min.max(1.0);
        Self {
            peers: Buckets::new(per_min, per_min / 60.0),
            polls: Buckets::new(POLL_BURST, POLL_PER_MIN / 60.0),
            all: Buckets::new(
                ALL_BURST.max(per_min * 6.0),
                ALL_PER_MIN.max(per_min * 12.0) / 60.0,
            ),
            tickets: Buckets::for_bucket(Bucket::Ticket),
            sign_ins: Buckets::for_bucket(Bucket::SignIn),
            failures: Failures::default(),
            dirty: AtomicBool::new(false),
        }
    }

    /// A copy to write into the state, when something changed since the
    /// last one.
    pub fn copy_if_changed(&self) -> Option<LimitsCopy> {
        self.dirty
            .swap(false, Ordering::Relaxed)
            .then(|| self.copy())
    }

    pub fn copy(&self) -> LimitsCopy {
        let now = Instant::now();
        let s = |k: &String| k.clone();
        LimitsCopy {
            at_unix_ms: unix_ms(),
            peers: self.peers.copy(now, s),
            polls: self.polls.copy(now, s),
            all: self.all.copy(now, |_| String::new()),
            tickets: self.tickets.copy(now, s),
            sign_ins: self.sign_ins.copy(now, s),
            failures: self.failures.copy(now, |ip| ip.to_string()),
        }
    }

    /// Takes up a copy the leader before wrote: what was spent and who is
    /// blocked stays spent and blocked.
    pub fn restore(&self, c: &LimitsCopy) {
        let now = Instant::now();
        let elapsed = Duration::from_millis(unix_ms().saturating_sub(c.at_unix_ms));
        let s = |k: &str| Some(k.to_string());
        self.peers.restore(&c.peers, elapsed, now, s);
        self.polls.restore(&c.polls, elapsed, now, s);
        self.all.restore(&c.all, elapsed, now, |_| Some(()));
        self.tickets.restore(&c.tickets, elapsed, now, s);
        self.sign_ins.restore(&c.sign_ins, elapsed, now, s);
        self.failures
            .restore(&c.failures, elapsed, now, |k| k.parse::<IpAddr>().ok());
    }

    fn touched(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Whether peer `peer` may make sign-in call `op` without a ticket
    /// now; `Err` says how long to wait.
    pub fn sign_in(&self, peer: &str, op: &AdminOp) -> Result<(), Duration> {
        let bucket = match op {
            // Reads the settings from memory: nothing to protect.
            AdminOp::SsoInfo => return Ok(()),
            AdminOp::SsoDevicePoll { .. } => &self.polls,
            _ => &self.peers,
        };
        self.touched();
        bucket.take(&peer.to_string())?;
        self.all.take(&())
    }

    /// A call that carried a wrong credential of its own (a SCIM token):
    /// it costs the peer like a sign-in.
    pub fn wrong_credential(&self, peer: &str) -> Result<(), Duration> {
        self.touched();
        self.peers.take(&peer.to_string())?;
        self.all.take(&())
    }

    pub fn call(&self, op: LimitOp) -> anyhow::Result<LimitReply> {
        if !matches!(op, LimitOp::Blocked) {
            self.touched();
        }
        Ok(match op {
            LimitOp::Take { bucket, key, want } => {
                anyhow::ensure!(key.len() <= 128, "limit key too long");
                let want = want.clamp(1, MAX_TAKE);
                let b = match bucket {
                    Bucket::Ticket => &self.tickets,
                    Bucket::SignIn => &self.sign_ins,
                };
                let (got, wait) = b.take_up_to(&key, want);
                LimitReply::Taken {
                    got,
                    wait_ms: wait.as_millis().min(u128::from(u64::MAX)) as u64,
                }
            }
            LimitOp::Failed { ip } => {
                let ip: IpAddr = ip
                    .parse()
                    .map_err(|_| anyhow::anyhow!("{ip:?} is not an IP address"))?;
                if self.failures.failed(ip.to_canonical()) {
                    warn!(
                        %ip,
                        "too many failed sign-ins through the dashboards; blocked for {}s",
                        pumasi_proto::limit::BLOCK_SECS
                    );
                }
                self.blocked()
            }
            LimitOp::Blocked => self.blocked(),
        })
    }

    fn blocked(&self) -> LimitReply {
        LimitReply::Blocked(
            self.failures
                .all_blocked()
                .into_iter()
                .map(|(ip, left)| (ip.to_string(), left.as_millis() as u64))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_have_a_budget_and_all_of_them_another() {
        let l = Limits::default();
        let op = AdminOp::SsoDeviceStart;
        for _ in 0..PEER_PER_MIN as u32 {
            l.sign_in("a", &op).unwrap();
        }
        assert!(l.sign_in("a", &op).is_err());
        // The settings are free.
        l.sign_in("a", &AdminOp::SsoInfo).unwrap();
        // Polls have their own budget.
        l.sign_in("a", &AdminOp::SsoDevicePoll { handle: "h".into() })
            .unwrap();
        // New keys run into the total.
        let mut refused = false;
        for i in 0..200 {
            if l.sign_in(&format!("peer{i}"), &op).is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused, "no limit on all peers together");
    }

    #[test]
    fn a_copy_keeps_blocks_and_spent_tokens() {
        let l = Limits::default();
        assert!(l.copy_if_changed().is_none());
        for _ in 0..pumasi_proto::limit::MAX_FAILURES {
            l.call(LimitOp::Failed {
                ip: "198.51.100.7".into(),
            })
            .unwrap();
        }
        for _ in 0..3 {
            l.call(LimitOp::Failed {
                ip: "198.51.100.8".into(),
            })
            .unwrap();
        }
        for _ in 0..PEER_PER_MIN as u32 {
            l.sign_in("p", &AdminOp::SsoDeviceStart).unwrap();
        }
        let c = l.copy_if_changed().expect("changed");
        assert!(l.copy_if_changed().is_none());
        let bytes = postcard::to_stdvec(&c).unwrap();
        let back: LimitsCopy = postcard::from_bytes(&bytes).unwrap();
        let next = Limits::default();
        next.restore(&back);
        let LimitReply::Blocked(b) = next.call(LimitOp::Blocked).unwrap() else {
            panic!()
        };
        assert_eq!(b.len(), 1, "{b:?}");
        assert!(next.sign_in("p", &AdminOp::SsoDeviceStart).is_err());
        // The open window came along: seven more block the other address.
        for _ in 0..pumasi_proto::limit::MAX_FAILURES - 4 {
            next.call(LimitOp::Failed {
                ip: "198.51.100.8".into(),
            })
            .unwrap();
        }
        let LimitReply::Blocked(b) = next
            .call(LimitOp::Failed {
                ip: "198.51.100.8".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(b.len(), 2, "{b:?}");
    }

    #[test]
    fn shared_counts() {
        let l = Limits::default();
        let take = |want| match l
            .call(LimitOp::Take {
                bucket: Bucket::SignIn,
                key: "203.0.113.9".into(),
                want,
            })
            .unwrap()
        {
            LimitReply::Taken { got, wait_ms } => (got, wait_ms),
            other => panic!("{other:?}"),
        };
        assert_eq!(take(64).0, pumasi_proto::limit::SIGN_IN_BURST as u32);
        let (got, wait) = take(1);
        assert_eq!(got, 0);
        assert!(wait > 1000, "{wait}");
        for _ in 0..pumasi_proto::limit::MAX_FAILURES - 1 {
            let r = l
                .call(LimitOp::Failed {
                    ip: "::ffff:198.51.100.4".into(),
                })
                .unwrap();
            assert_eq!(r, LimitReply::Blocked(vec![]));
        }
        let LimitReply::Blocked(b) = l
            .call(LimitOp::Failed {
                ip: "198.51.100.4".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].0, "198.51.100.4");
        assert!(l.call(LimitOp::Failed { ip: "nope".into() }).is_err());
    }
}
