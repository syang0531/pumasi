//! Node cache settings (1.18): how big each node's file store cache may
//! get, how much of it one account's collections may take, and which
//! collections queued tasks will mount, so eviction leaves those for last.
//!
//! The sizes are node labels, set like any other (`pumasi label`,
//! `cluster.toml`, the dashboard): `cache=50G` replaces the agent's own
//! `--store-cache` and `cache.account=10G` caps each account's share. The
//! leader sends each connected agent of minor [`NET_MINOR`] its
//! [`CachePolicy`] when it changes ([`ToAgent::Cache`]); the agent evicts
//! (`agent/src/store.rs`).

use std::{
    collections::{BTreeSet, HashSet},
    time::{Duration, Instant},
};

use pumasi_proto::{
    NET_MINOR,
    msg::ToAgent,
    store::{BlobHash, CACHE_ACCOUNT_LABEL, CACHE_LABEL, CachePolicy, MAX_CACHE_KEEP, parse_size},
};

use crate::{datasets::ImageState, state::State};

/// How often the leader looks at what the queue mounts.
const CACHE_EVERY: Duration = Duration::from_secs(5);

/// A size label's value, when it is one (`50G`, `512M`).
fn size_label(v: Option<&String>) -> Option<u64> {
    v.and_then(|v| parse_size(v).ok()).filter(|n| *n > 0)
}

impl State {
    /// What queued runs will mount: their datasets and uploads, their
    /// private images, at most [`MAX_CACHE_KEEP`].
    fn queued_roots(&self) -> Vec<BlobHash> {
        let mut out = BTreeSet::new();
        let mut jobs = HashSet::new();
        'items: for item in &self.queue {
            let Some(job) = self.jobs.get(&item.job) else {
                continue;
            };
            if jobs.insert(item.job) {
                let image_roots = job.meta.ext.images.values().filter_map(|s| match s {
                    ImageState::Ready(i) => Some(i.root),
                    _ => None,
                });
                for root in job
                    .meta
                    .ext
                    .mounts
                    .iter()
                    .map(|m| m.root)
                    .chain(image_roots)
                {
                    out.insert(root);
                    if out.len() >= MAX_CACHE_KEEP {
                        break 'items;
                    }
                }
            }
            if let Some(t) = job.tasks.get(item.index as usize) {
                for m in &t.ext.mounts {
                    out.insert(m.root);
                    if out.len() >= MAX_CACHE_KEEP {
                        break 'items;
                    }
                }
            }
        }
        out.into_iter().collect()
    }

    /// A node's cache settings from its labels, with what the queue
    /// mounts.
    pub(crate) fn cache_policy(&self, id: &str, keep: &[BlobHash]) -> CachePolicy {
        let labels = self.node_config(id).map(|c| &c.labels);
        CachePolicy {
            cap: size_label(labels.and_then(|l| l.get(CACHE_LABEL))),
            per_account: size_label(labels.and_then(|l| l.get(CACHE_ACCOUNT_LABEL))),
            keep: keep.to_vec(),
        }
    }

    /// Sends each connected 1.18 agent its cache settings when they
    /// changed, every [`CACHE_EVERY`] at most.
    pub(crate) fn tick_caches(&mut self) {
        if self
            .mgmt
            .cache_tick
            .is_some_and(|t| t.elapsed() < CACHE_EVERY)
        {
            return;
        }
        self.mgmt.cache_tick = Some(Instant::now());
        let keep = self.queued_roots();
        let mut sends = Vec::new();
        for (id, n) in &self.nodes {
            if !n.online || n.tx.is_none() || self.node_minor(id) < NET_MINOR {
                continue;
            }
            let p = self.cache_policy(id, &keep);
            if self.mgmt.cache_sent.get(id) != Some(&p) {
                sends.push((id.clone(), p));
            }
        }
        for (id, p) in sends {
            let sent = self
                .nodes
                .get(&id)
                .and_then(|n| n.tx.as_ref())
                .is_some_and(|tx| tx.send(ToAgent::Cache(Box::new(p.clone()))).is_ok());
            if sent {
                self.mgmt.cache_sent.insert(id, p);
            }
        }
    }

    /// A node's session ended: it gets its settings again when it is back.
    pub(crate) fn forget_cache_sent(&mut self, id: &str) {
        self.mgmt.cache_sent.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_labels() {
        assert_eq!(size_label(Some(&"50G".to_string())), Some(50 << 30));
        assert_eq!(size_label(Some(&"yes".to_string())), None);
        assert_eq!(size_label(Some(&"0".to_string())), None);
        assert_eq!(size_label(None), None);
    }
}
