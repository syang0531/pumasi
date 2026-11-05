//! Node performance: signed profiles from agents, the CPU score runs are
//! billed at, and challenges that check a provider's claimed score.
//!
//! A provider could claim a faster CPU than it has, to earn more CU per
//! hour. Three things stop that:
//!
//! 1. Until the cluster has timed a node itself, it bills the node at most
//!    the reference score (1000).
//! 2. A challenge: when an untrusted node sends a new profile (and when its
//!    WASM runs look slower than it claims), the cluster waits until the
//!    node is idle, sends it a fresh seed for the benchmark kernel and times
//!    the answer itself. The answer is recomputed natively, so the node
//!    cannot skip the work, and nothing runs faster than the hardware. The
//!    node is then billed at no more than 1.25 times the best the cluster
//!    timed. A wrong answer is a strike, and so is a claim over twice the
//!    timed score twice in a row (the first time it is timed again, in case
//!    the machine was just busy).
//! 3. Faking the wall time of runs does not help either: CPU time is billed
//!    as wall time times score, so reporting runs as shorter to look
//!    consistent with a high score earns exactly as much as being honest.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use pumasi_proto::{
    economy::billing_score,
    msg::ToAgent,
    perf::{
        BenchChallenge, FUEL_PER_ITER, MAX_SCORE, MIN_SCORE, REFERENCE_SCORE, SignedPerf,
        iters_for, score_for,
    },
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::state::{Member, State, now_unix};

/// Target length of a challenge at the claimed speed.
const CHALLENGE_MS: u64 = 500;
/// A challenge not answered in this long counts as answered at this time.
pub(crate) const CHALLENGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Slack between the profile and the cluster's timing (the timing includes
/// network and scheduling delays).
const SLACK_PERMILLE: u64 = 1250;
/// A profile claiming more than this multiple of the timed score is a lie.
const INFLATED: u64 = 2;
/// WASM runs at least this long tell something about the node's speed.
const MIN_RUN_MS: u64 = 300;
/// Runs remembered for the speed cross-check.
const RATE_SAMPLES: usize = 8;
/// At most one challenge from the cross-check per node in this time.
const RECHECK_AFTER: Duration = Duration::from_secs(3600);
/// Timings of the same claim that must all look inflated before the node
/// is struck: a machine busy with its owner's work for a moment is not a
/// liar, so it is timed again and the best timing counts.
const STRIKE_AFTER: u32 = 2;
/// Wait at least this long between two challenges to the same node.
const RETRY_GAP: Duration = Duration::from_secs(5);

/// The cluster's own timing of a node's CPU benchmark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfCheck {
    /// The profile's single-thread score at the time.
    pub claimed_single: u32,
    /// The best single-thread score the cluster timed for this claim.
    pub measured_single: u32,
    /// The node's answer was right (or it did not answer in time).
    pub correct: bool,
    /// Timings of this claim so far.
    pub attempts: u32,
    pub unix: u64,
}

/// A challenge sent and not answered yet.
#[derive(Debug, Clone)]
pub(crate) struct Challenge {
    pub bench: BenchChallenge,
    pub sent: Instant,
    pub claimed_single: u32,
}

/// Per-connection performance state.
#[derive(Debug, Default)]
pub(crate) struct PerfRuntime {
    /// Agent protocol of the connection: challenges need 7.
    pub protocol: u32,
    pub rtt_us: Option<u32>,
    pub relayed: bool,
    /// The node should be timed: it gets no new tasks until it is idle and
    /// answered.
    pub wanted: bool,
    pub challenge: Option<Challenge>,
    pub last_challenge: Option<Instant>,
    /// Single-thread scores implied by recent WASM runs.
    pub implied: VecDeque<u32>,
}

/// The CPU score a member's runs are billed at, and why it is not simply
/// the profile's.
pub fn cpu_score(m: &Member) -> (u32, Option<String>) {
    let Some(perf) = &m.perf else {
        return (
            REFERENCE_SCORE,
            Some("no benchmark yet (older agent, or benchmarks off): billed at 1000".into()),
        );
    };
    let cpu = &perf.profile.cpu;
    let per_vcpu = cpu.per_vcpu();
    if m.trusted() {
        return (billing_score(per_vcpu), None);
    }
    match &m.perf_check {
        Some(c) if c.claimed_single == cpu.single => {
            if !c.correct {
                return (
                    MIN_SCORE,
                    Some("gave a wrong answer to the cluster's benchmark check".into()),
                );
            }
            let allowed = (c.measured_single as u64 * SLACK_PERMILLE / 1000).max(1);
            if cpu.single as u64 > allowed {
                let capped = (per_vcpu as u64 * allowed / cpu.single.max(1) as u64) as u32;
                (
                    billing_score(capped),
                    Some(format!(
                        "capped: the cluster timed {} single-thread, the profile claims {}",
                        c.measured_single, cpu.single
                    )),
                )
            } else {
                (billing_score(per_vcpu), None)
            }
        }
        _ if per_vcpu > REFERENCE_SCORE => (
            REFERENCE_SCORE,
            Some("not confirmed yet: billed at 1000 until the cluster times it".into()),
        ),
        _ => (billing_score(per_vcpu), None),
    }
}

impl State {
    /// The score a node's runs are billed at.
    pub(crate) fn billing_score_of(&self, node_id: &str) -> u32 {
        self.members
            .get(node_id)
            .map(|m| cpu_score(m).0)
            .unwrap_or(REFERENCE_SCORE)
    }

    /// Stores a node's signed profile. A new profile from an untrusted node
    /// is timed by a challenge.
    pub fn on_perf(&mut self, id: &str, generation: u64, signed: SignedPerf) -> Result<()> {
        if !self
            .nodes
            .get(id)
            .is_some_and(|n| n.generation == generation)
        {
            return Ok(());
        }
        signed.verify()?;
        let key: iroh::PublicKey = id.parse()?;
        ensure!(
            &signed.profile.node == key.as_bytes(),
            "profile is for another node"
        );
        let Some(m) = self.members.get_mut(id) else {
            return Ok(());
        };
        let p = &signed.profile;
        info!(
            node = %id,
            single = p.cpu.single,
            multi = p.cpu.multi,
            threads = p.cpu.threads,
            disk = %p.disk.as_ref().map(|d| d.class.to_string()).unwrap_or_else(|| "-".into()),
            "performance profile"
        );
        let checked = m
            .perf_check
            .as_ref()
            .is_some_and(|c| c.claimed_single == p.cpu.single);
        let trusted = m.trusted();
        let scores = |p: &pumasi_proto::perf::PerfProfile| (p.cpu.per_vcpu(), p.memory.copy_mb_s);
        let new_scores = scores(p);
        let old_scores = m
            .perf
            .as_ref()
            .filter(|old| old.profile.measured_unix != p.measured_unix)
            .map(|old| scores(&old.profile));
        let changed = m.perf.as_ref() != Some(&signed);
        if changed {
            m.perf = Some(signed);
            let m = m.clone();
            self.store.put_member(&m)?;
            self.note_benchmark(id, old_scores, new_scores);
        }
        if !trusted
            && !checked
            && let Some(n) = self.nodes.get_mut(id)
        {
            n.perf.wanted = true;
        }
        self.schedule();
        Ok(())
    }

    /// The control plane's round-trip time to a node.
    pub fn on_rtt(&mut self, id: &str, generation: u64, rtt: Option<Duration>, relayed: bool) {
        if let Some(n) = self.nodes.get_mut(id)
            && n.generation == generation
        {
            n.perf.rtt_us = rtt.map(|d| d.as_micros().min(u32::MAX as u128) as u32);
            n.perf.relayed = relayed;
        }
    }

    /// Asks a node to measure itself again.
    pub fn request_benchmark(&mut self, id: &str) -> Result<()> {
        let id = self.resolve_member(id)?;
        let n = self
            .nodes
            .get(&id)
            .filter(|n| n.online)
            .ok_or_else(|| anyhow::anyhow!("node is offline"))?;
        ensure!(
            n.perf.protocol >= 7,
            "the node's agent is too old to benchmark; upgrade it"
        );
        if let Some(tx) = &n.tx {
            let _ = tx.send(ToAgent::Rebenchmark);
        }
        Ok(())
    }

    /// Sends challenges to idle nodes that should be timed.
    pub(crate) fn drive_challenges(&mut self) {
        let now = Instant::now();
        for (id, n) in self.nodes.iter_mut() {
            if !n.online
                || !n.perf.wanted
                || n.perf.challenge.is_some()
                || n.perf.protocol < 7
                || !n.running.is_empty()
                || self.upgrading.contains_key(id)
                || n.perf
                    .last_challenge
                    .is_some_and(|t| t.elapsed() < RETRY_GAP)
            {
                continue;
            }
            let Some(claimed) = self
                .members
                .get(id)
                .and_then(|m| m.perf.as_ref())
                .map(|p| p.profile.cpu.single)
            else {
                n.perf.wanted = false;
                continue;
            };
            let Some(tx) = &n.tx else { continue };
            let bench = BenchChallenge {
                nonce: rand::random(),
                seed: rand::random(),
                iters: iters_for(claimed.clamp(MIN_SCORE, MAX_SCORE), CHALLENGE_MS),
            };
            if tx.send(ToAgent::Benchmark(bench)).is_err() {
                continue;
            }
            info!(node = %id, claimed, iters = bench.iters, "timing the node's CPU benchmark");
            n.perf.wanted = false;
            n.perf.last_challenge = Some(now);
            n.perf.challenge = Some(Challenge {
                bench,
                sent: now,
                claimed_single: claimed,
            });
        }
    }

    /// Takes the challenge a node answered: what was asked and how long the
    /// answer took, minus the round trip. The caller checks the answer.
    pub(crate) fn take_challenge(
        &mut self,
        id: &str,
        generation: u64,
        nonce: u64,
    ) -> Option<(Challenge, Duration)> {
        let n = self.nodes.get_mut(id)?;
        if n.generation != generation
            || n.perf.challenge.as_ref().map(|c| c.bench.nonce) != Some(nonce)
        {
            return None;
        }
        let c = n.perf.challenge.take()?;
        let rtt = Duration::from_micros(n.perf.rtt_us.unwrap_or(0) as u64);
        let took = c.sent.elapsed().saturating_sub(rtt);
        Some((c, took))
    }

    /// Records the cluster's timing of a node's benchmark.
    pub(crate) fn apply_challenge(
        &mut self,
        id: &str,
        c: &Challenge,
        took: Duration,
        correct: bool,
    ) {
        let timed = score_for(c.bench.iters, took.as_nanos().max(1_000_000));
        let Some(m) = self.members.get_mut(id) else {
            return;
        };
        let (measured, attempts) = match &m.perf_check {
            Some(p) if p.claimed_single == c.claimed_single && p.correct && correct => {
                (p.measured_single.max(timed), p.attempts + 1)
            }
            _ => (timed, 1),
        };
        m.perf_check = Some(PerfCheck {
            claimed_single: c.claimed_single,
            measured_single: measured,
            correct,
            attempts,
            unix: now_unix(),
        });
        let m = m.clone();
        if let Err(e) = self.store.put_member(&m) {
            warn!("persist member: {e:#}");
        }
        if !correct {
            self.strike(id, "wrong answer to the benchmark check");
        } else if c.claimed_single as u64 > measured as u64 * INFLATED && attempts < STRIKE_AFTER {
            info!(
                node = %id,
                claimed = c.claimed_single,
                timed,
                "benchmark looks inflated; timing it again"
            );
            if let Some(n) = self.nodes.get_mut(id) {
                n.perf.wanted = true;
            }
        } else if c.claimed_single as u64 > measured as u64 * INFLATED {
            self.strike(
                id,
                &format!(
                    "benchmark inflated: the profile claims {} single-thread, the cluster timed {measured}",
                    c.claimed_single
                ),
            );
        } else {
            info!(node = %id, claimed = c.claimed_single, measured, "benchmark confirmed");
        }
        self.schedule();
    }

    /// Challenges that were not answered in time count as answered now.
    pub(crate) fn expire_challenges(&mut self) {
        let late: Vec<(String, Challenge)> = self
            .nodes
            .iter_mut()
            .filter_map(|(id, n)| {
                let late = n
                    .perf
                    .challenge
                    .as_ref()
                    .is_some_and(|c| c.sent.elapsed() > CHALLENGE_TIMEOUT);
                late.then(|| (id.clone(), n.perf.challenge.take().expect("checked")))
            })
            .collect();
        for (id, c) in late {
            warn!(node = %id, "no answer to the benchmark check in time");
            self.apply_challenge(&id, &c, CHALLENGE_TIMEOUT, true);
        }
    }

    /// Cross-checks a WASM run's speed against the node's claimed score: a
    /// node whose runs keep going much slower than its profile says is
    /// timed again.
    pub(crate) fn note_run_speed(&mut self, id: &str, fuel_used: u64, wall_ms: u64) {
        if wall_ms < MIN_RUN_MS || fuel_used == 0 {
            return;
        }
        let Some(claimed) = self
            .members
            .get(id)
            .filter(|m| !m.trusted())
            .and_then(|m| m.perf.as_ref())
            .map(|p| p.profile.cpu.single)
        else {
            return;
        };
        let Some(n) = self.nodes.get_mut(id) else {
            return;
        };
        let implied = score_for(fuel_used / FUEL_PER_ITER, wall_ms as u128 * 1_000_000);
        n.perf.implied.push_back(implied);
        while n.perf.implied.len() > RATE_SAMPLES {
            n.perf.implied.pop_front();
        }
        if n.perf.implied.len() < RATE_SAMPLES / 2 {
            return;
        }
        let mut sorted: Vec<u32> = n.perf.implied.iter().copied().collect();
        sorted.sort_unstable();
        let median = sorted[sorted.len() / 2];
        let recent = n
            .perf
            .last_challenge
            .is_some_and(|t| t.elapsed() < RECHECK_AFTER);
        if claimed as u64 > median as u64 * INFLATED && !recent && !n.perf.wanted {
            info!(node = %id, claimed, median, "runs are slower than the profile says; timing it again");
            n.perf.wanted = true;
            n.perf.implied.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use pumasi_proto::{
        msg::{NodeInfo, Role},
        perf::{BENCH_VERSION, CpuPerf, MemoryPerf, PerfProfile},
    };

    fn member(role: Role, single: u32, multi: u32, check: Option<PerfCheck>) -> Member {
        let key = SecretKey::generate();
        let profile = PerfProfile {
            bench_version: BENCH_VERSION,
            node: *key.public().as_bytes(),
            measured_unix: 1,
            agent_version: "t".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            cpu: CpuPerf {
                model: "c".into(),
                threads: 4,
                single,
                multi,
            },
            memory: MemoryPerf {
                total_bytes: 1,
                copy_mb_s: 1,
            },
            disk: None,
            notes: vec![],
        };
        Member {
            id: key.public().to_string(),
            info: NodeInfo {
                name: "n".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                cpus: 4,
                memory_bytes: 1 << 30,
                slots: 1,
                agent_version: "t".into(),
                executors: vec![],
            },
            joined_unix: 0,
            role,
            account: "bob".into(),
            allow_docker: false,
            checks_passed: 0,
            strikes: 0,
            perf: Some(SignedPerf::sign(profile, &key)),
            perf_check: check,
        }
    }

    fn check(claimed: u32, measured: u32, correct: bool) -> Option<PerfCheck> {
        Some(PerfCheck {
            claimed_single: claimed,
            measured_single: measured,
            correct,
            attempts: 1,
            unix: 0,
        })
    }

    #[test]
    fn billed_scores() {
        // Owner machines are billed at their profile's per-vCPU score.
        assert_eq!(cpu_score(&member(Role::Owner, 1500, 4800, None)).0, 1200);
        // Providers: at most 1000 until timed.
        let (s, note) = cpu_score(&member(Role::Provider, 1500, 4800, None));
        assert_eq!(s, 1000);
        assert!(note.unwrap().contains("not confirmed"));
        // A slow provider is billed at its own score right away.
        assert_eq!(cpu_score(&member(Role::Provider, 700, 2400, None)).0, 600);
        // Confirmed within the slack.
        let (s, note) = cpu_score(&member(Role::Provider, 1500, 4800, check(1500, 1300, true)));
        assert_eq!((s, note), (1200, None));
        // Claimed 3000, timed 1000: capped to 1250 single, per-vCPU scaled.
        let (s, note) = cpu_score(&member(Role::Provider, 3000, 9600, check(3000, 1000, true)));
        assert_eq!(s, 2400 * 1250 / 3000);
        assert!(note.unwrap().contains("capped"));
        // A check of an older profile does not count.
        assert_eq!(
            cpu_score(&member(Role::Provider, 3000, 9600, check(2000, 2000, true))).0,
            1000
        );
        // A wrong answer: the minimum.
        assert_eq!(
            cpu_score(&member(
                Role::Provider,
                1000,
                4000,
                check(1000, 1000, false)
            ))
            .0,
            MIN_SCORE
        );
        // No profile at all: the reference.
        let mut m = member(Role::Provider, 1, 1, None);
        m.perf = None;
        assert_eq!(cpu_score(&m).0, REFERENCE_SCORE);
    }
}
