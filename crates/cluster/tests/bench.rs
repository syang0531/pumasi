//! The benchmark checks (phase 7) end to end: a real control plane times
//! the CPUs of real agents over iroh on localhost.
//!
//! In a test binary of its own, because the timing needs the CPU: cargo
//! runs test binaries one after another, while the tests in `e2e.rs` run
//! several at once, and with those competing an honest node was timed at
//! under half its own benchmark twice in a row and struck.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    msg::{AdminOp, Invite},
    net::NetMode,
    ticket::JoinTicket,
};
use tokio::task::JoinHandle;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-bench-{tag}-{}", rand_u64()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::SystemTime::now())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pumasi=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
}

async fn start_cluster(dir: PathBuf) -> Result<Cluster> {
    let mut config = ClusterConfig::new(dir);
    config.name = "test-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some("127.0.0.1:0".parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(2);
    Cluster::start(config).await
}

struct RunningAgent {
    endpoint: iroh::Endpoint,
    handle: JoinHandle<Result<AgentExit>>,
}

impl RunningAgent {
    /// Simulates a crash: stop serving and drop the network immediately.
    async fn kill(self) {
        self.handle.abort();
        self.endpoint.close().await;
    }
}

fn agent_config(name: &str, slots: u32, dir: PathBuf) -> AgentConfig {
    AgentConfig {
        data_dir: dir,
        name: name.into(),
        slots: Some(slots),
        net: NetMode::Local,
        relays: Vec::new(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: None,
        microvm: None,
        upgrade: None,
        corrupt_results: false,
        store_cache_bytes: None,
        gpus: false,
        fake_gpus: None,
        webgpu: None,
        gpu_graphics: None,
        docker_wsl_libs: None,
        power: Default::default(),
        max_cpu_percent: None,
        availability: None,
        bench: None,
    }
}

fn spawn_join(agent: Agent, ticket: JoinTicket) -> RunningAgent {
    let endpoint = agent.endpoint().clone();
    let handle = tokio::spawn(agent.join(ticket));
    RunningAgent { endpoint, handle }
}

async fn admin(cluster: &Cluster) -> AdminClient {
    AdminClient::connect(&cluster.admin_ticket(), NetMode::Local)
        .await
        .unwrap()
}

async fn wait_online(cluster: &Cluster, n: usize) {
    for _ in 0..200 {
        if cluster.status().nodes.iter().filter(|n| n.online).count() >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nodes did not come online: {:#?}", cluster.status().nodes);
}

fn invite(cluster: &Cluster, account: &str) -> JoinTicket {
    cluster
        .create_invite(
            Duration::from_secs(60),
            Some(1),
            Some(Invite {
                account: account.into(),
                allow_docker: false,
            }),
        )
        .unwrap()
}

async fn wait_for(what: &str, cond: impl Fn() -> bool) {
    // A minute: a second timing of an inflated benchmark runs eight times
    // longer than claimed.
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn benched(name: &str, dir: PathBuf, inflate: u32) -> Agent {
    let mut config = agent_config(name, 1, dir.clone());
    let mut bench = pumasi_agent::BenchConfig::quick(dir);
    bench.inflate = inflate;
    config.bench = Some(bench);
    Agent::new(config).await.unwrap()
}

fn node_named(cluster: &Cluster, name: &str) -> pumasi_proto::msg::NodeView {
    cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.info.name == name)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn benchmarks_are_signed_and_an_inflated_one_is_caught() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let own = benched("own", tempdir("o"), 1).await;
    let honest = benched("honest", tempdir("h"), 1).await;
    // Claims a CPU eight times faster than it is.
    let liar = benched("liar", tempdir("l"), 8).await;
    let claimed = liar.perf().unwrap().profile.cpu.single;
    let o = spawn_join(own, ticket);
    let h = spawn_join(honest, invite(&cluster, "alice"));
    let l = spawn_join(liar, invite(&cluster, "mallory"));
    wait_online(&cluster, 3).await;

    // Every node sends its signed profile; the cluster times the
    // providers' CPU benchmark itself.
    wait_for("profiles and checks", || {
        let s = cluster.status();
        s.nodes.iter().all(|n| n.perf.is_some())
            && node_named(&cluster, "liar").strikes > 0
            // Timed, or slow enough not to need it. Under load the
            // cluster's timing can differ from the agent's a little, which
            // caps the billed score but is no strike.
            && !node_named(&cluster, "honest")
                .perf_note
                .is_some_and(|n| n.contains("not confirmed"))
    })
    .await;
    let own = node_named(&cluster, "own");
    let p = own.perf.as_ref().unwrap();
    assert!(p.cpu.single > 0 && p.memory.copy_mb_s > 0);
    let disk = p.disk.as_ref().expect("disk measured");
    assert!(
        disk.seq_read_mb_s > 0 && disk.rand_read_iops > 0,
        "{disk:?}"
    );
    // Owner machines are billed at their own score, unchecked.
    assert_eq!(
        own.cpu_score,
        pumasi_proto::economy::billing_score(p.cpu.per_vcpu())
    );
    assert!(own.rtt_us.is_some(), "the cluster measures latency");

    let honest = node_named(&cluster, "honest");
    assert_eq!(honest.strikes, 0, "{:?}", honest.perf_note);
    let liar = node_named(&cluster, "liar");
    assert_eq!(liar.perf.as_ref().unwrap().cpu.single, claimed);
    assert!(
        liar.perf_note.as_deref().unwrap_or("").contains("capped"),
        "{:?}",
        liar.perf_note
    );
    // Billed at about what the cluster timed (at most 1.25x its best
    // timing, which a busy runner can put at twice the real speed), not
    // the eightfold claim.
    assert!(
        (liar.cpu_score as u64) * 2 < liar.perf.as_ref().unwrap().cpu.per_vcpu() as u64,
        "billed {} for a claim of {}",
        liar.cpu_score,
        liar.perf.as_ref().unwrap().cpu.per_vcpu()
    );

    // The owner can ask a node to measure again.
    let before = node_named(&cluster, "honest").perf.unwrap();
    client
        .call(AdminOp::Benchmark {
            id: node_named(&cluster, "honest").id,
        })
        .await
        .unwrap();
    wait_for("a new profile", || {
        node_named(&cluster, "honest").perf.as_ref() != Some(&before)
    })
    .await;

    client.close().await;
    o.kill().await;
    h.kill().await;
    l.kill().await;
    cluster.shutdown().await.unwrap();
}
