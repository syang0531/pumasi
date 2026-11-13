//! The network between nodes (1.6): the control plane asks one agent at a
//! time to probe another over iroh ([`ToAgent::Probe`]: a few round trips
//! and a short transfer), keeps the last result of each pair, and uses it
//! to point a node at the closest cache in the file store. Extends
//! [`State`].

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::Result;
use pumasi_proto::{
    MONITOR_MINOR,
    monitor::{LinkView, NetView, PROBE_BYTES, ProbeResult},
    msg::ToAgent,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::state::{State, now_unix};

/// Meta key of the links, written now and then.
const LINKS_META: &str = "net_links";
/// A probe without an answer after this long is given up.
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often changed links are written out.
const SAVE_EVERY: Duration = Duration::from_secs(60);
/// How often the control plane asks for a probe by default: with five
/// nodes, each of the 20 directions is measured about every 7 minutes,
/// and the transfers average under 60 KB/s across the cluster.
pub const DEFAULT_PROBE_EVERY: Duration = Duration::from_secs(20);

/// The last probe from one node to another.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub rtt_us: Option<u32>,
    pub bps: Option<u64>,
    pub direct: Option<bool>,
    pub unix: u64,
    pub error: Option<String>,
}

/// The network part of [`State`].
pub(crate) struct NetState {
    /// How often to ask for a probe (`None`: never).
    pub every: Option<Duration>,
    /// By (from, to) node id.
    pub links: HashMap<(String, String), Link>,
    /// The probe asked for and not answered yet: (from, to, when).
    pending: Option<(String, String, Instant)>,
    last_sent: Option<Instant>,
    dirty: bool,
    last_save: Instant,
}

impl NetState {
    /// The round trip between two nodes, either way.
    pub(crate) fn rtt_between(&self, a: &str, b: &str) -> Option<u32> {
        let get = |x: &str, y: &str| {
            self.links
                .get(&(x.to_string(), y.to_string()))
                .filter(|l| l.error.is_none())
                .and_then(|l| l.rtt_us)
        };
        get(a, b).or_else(|| get(b, a))
    }
}

impl Default for NetState {
    fn default() -> Self {
        Self {
            every: Some(DEFAULT_PROBE_EVERY),
            links: HashMap::new(),
            pending: None,
            last_sent: None,
            dirty: false,
            last_save: Instant::now(),
        }
    }
}

impl State {
    /// Loads the links measured before a restart.
    pub(crate) fn load_net(&mut self) -> Result<()> {
        if let Some(links) = self
            .store
            .meta_record::<Vec<(String, String, Link)>>(LINKS_META)?
        {
            self.net.links = links
                .into_iter()
                .filter(|(a, b, _)| self.members.contains_key(a) && self.members.contains_key(b))
                .map(|(a, b, l)| ((a, b), l))
                .collect();
        }
        Ok(())
    }

    /// How often the control plane asks for a probe (`None`: never).
    pub fn set_probe_every(&mut self, every: Option<Duration>) {
        self.net.every = every.filter(|d| !d.is_zero());
    }

    /// The minor of a node's current session.
    pub(crate) fn node_minor(&self, id: &str) -> u32 {
        self.power.nodes.get(id).map_or(0, |r| r.minor)
    }

    /// Asks for the next probe when it is time: the pair measured longest
    /// ago (never measured first), between online 1.6 agents whose file
    /// store address is known.
    pub(crate) fn tick_net(&mut self) {
        let Some(every) = self.net.every else {
            return;
        };
        if let Some((_, _, at)) = &self.net.pending {
            if at.elapsed() < PROBE_TIMEOUT {
                return;
            }
            let (from, to, _) = self.net.pending.take().expect("checked");
            debug!(%from, %to, "network probe got no answer");
            self.record_link(
                &from,
                &to,
                Link {
                    unix: now_unix(),
                    error: Some("no answer".into()),
                    ..Default::default()
                },
            );
        }
        if self.net.last_sent.is_some_and(|t| t.elapsed() < every) {
            self.save_links(false);
            return;
        }
        let ready: Vec<&String> = self
            .nodes
            .iter()
            .filter(|(id, n)| {
                n.online
                    && n.tx.is_some()
                    && self.node_minor(id) >= MONITOR_MINOR
                    && !self.asleep(id)
            })
            .map(|(id, _)| id)
            .collect();
        let mut best: Option<(u64, &String, &String)> = None;
        for from in &ready {
            for to in &ready {
                if from == to || !self.files.caches.get(*to).is_some_and(|c| c.addr.is_some()) {
                    continue;
                }
                let age = self
                    .net
                    .links
                    .get(&((*from).clone(), (*to).clone()))
                    .map_or(0, |l| l.unix);
                if best.is_none_or(|(b, _, _)| age < b) {
                    best = Some((age, from, to));
                }
            }
        }
        let Some((_, from, to)) = best else {
            return;
        };
        let (from, to) = (from.clone(), to.clone());
        let addr = self.files.caches[&to].addr.clone().expect("checked");
        let sent = self
            .nodes
            .get(&from)
            .and_then(|n| n.tx.as_ref())
            .is_some_and(|tx| {
                tx.send(ToAgent::Probe {
                    node: to.clone(),
                    addr,
                    bytes: PROBE_BYTES,
                })
                .is_ok()
            });
        self.net.last_sent = Some(Instant::now());
        if sent {
            self.net.pending = Some((from, to, Instant::now()));
        }
        self.save_links(false);
    }

    /// What an agent measured.
    pub fn on_probe(&mut self, id: &str, generation: u64, r: ProbeResult) {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation)
        {
            return;
        }
        if self
            .net
            .pending
            .as_ref()
            .is_some_and(|(f, t, _)| f == id && *t == r.node)
        {
            self.net.pending = None;
        }
        if !self.members.contains_key(&r.node) {
            return;
        }
        if let Some(e) = &r.error {
            debug!(from = %id, to = %r.node, "network probe failed: {e}");
        }
        let link = Link {
            rtt_us: r.rtt_us,
            bps: r.bps(),
            direct: r.direct,
            unix: now_unix(),
            error: r.error.map(|e| e.chars().take(200).collect()),
        };
        self.record_link(id, &r.node.clone(), link);
    }

    fn record_link(&mut self, from: &str, to: &str, link: Link) {
        self.net
            .links
            .insert((from.to_string(), to.to_string()), link);
        self.net.dirty = true;
    }

    /// Writes changed links out now and then (or now, with `force`).
    pub(crate) fn save_links(&mut self, force: bool) {
        if !self.net.dirty || (!force && self.net.last_save.elapsed() < SAVE_EVERY) {
            return;
        }
        self.net.last_save = Instant::now();
        self.net.dirty = false;
        self.net
            .links
            .retain(|(a, b), _| self.members.contains_key(a) && self.members.contains_key(b));
        let all: Vec<(&String, &String, &Link)> =
            self.net.links.iter().map(|((a, b), l)| (a, b, l)).collect();
        if let Err(e) = self.store.put_meta_record(LINKS_META, &all) {
            warn!("persist network links: {e:#}");
        }
    }

    /// `pumasi net`.
    pub fn net_view(&self) -> NetView {
        let mut nodes: Vec<String> = self.members.values().map(|m| m.info.name.clone()).collect();
        nodes.sort();
        nodes.dedup();
        let mut links: Vec<LinkView> = self
            .net
            .links
            .iter()
            .filter(|((a, b), _)| self.members.contains_key(a) && self.members.contains_key(b))
            .map(|((a, b), l)| LinkView {
                from: self.node_name(a),
                to: self.node_name(b),
                rtt_us: l.rtt_us,
                bps: l.bps,
                direct: l.direct,
                unix: l.unix,
                error: l.error.clone(),
            })
            .collect();
        links.sort_by(|a, b| a.from.cmp(&b.from).then(a.to.cmp(&b.to)));
        NetView {
            nodes,
            links,
            every_secs: self.net.every.map_or(0, |d| d.as_secs().max(1)),
        }
    }
}
