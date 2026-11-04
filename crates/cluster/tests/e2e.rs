//! End-to-end tests: a real control plane and real agents talking over iroh
//! on localhost.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    job::{Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{AdminOp, AdminResponse, JobState, JobView, TaskState},
    net::NetMode,
    ticket::JoinTicket,
};
use tokio::task::JoinHandle;

/// Prints argv[1] to stdout. If argv[2] exists, first busy-waits for
/// 1.5 seconds of wall-clock time (to simulate a long task).
const ECHO_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "clock_time_get" (func $now (param i32 i64 i32) (result i32)))
  (memory (export "memory") 1)
  (func $time (result i64)
    (drop (call $now (i32.const 1) (i64.const 1000) (i32.const 32)))
    (i64.load (i32.const 32)))
  (func (export "_start")
    (local $p i32) (local $len i32) (local $start i64)
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $args_get (i32.const 64) (i32.const 1024)))
    (if (i32.ge_u (i32.load (i32.const 0)) (i32.const 3))
      (then
        (local.set $start (call $time))
        (block $done (loop $spin
          (br_if $done (i64.ge_u (i64.sub (call $time) (local.get $start)) (i64.const 1500000000)))
          (br $spin)))))
    (local.set $p (i32.load (i32.const 68)))
    (block $done (loop $l
      (br_if $done (i32.eqz (i32.load8_u (i32.add (local.get $p) (local.get $len)))))
      (local.set $len (i32.add (local.get $len) (i32.const 1)))
      (br $l)))
    (i32.store (i32.const 8) (local.get $p))
    (i32.store (i32.const 12) (local.get $len))
    (drop (call $fd_write (i32.const 1) (i32.const 8) (i32.const 1) (i32.const 16)))))
"#;

fn tempdir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("pumasi-e2e-{tag}-{}", rand_u64()));
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
    handle: JoinHandle<Result<()>>,
}

impl RunningAgent {
    /// Simulates a crash: stop serving and drop the network immediately.
    async fn kill(self) {
        self.handle.abort();
        self.endpoint.close().await;
    }
}

async fn agent(name: &str, slots: u32, dir: PathBuf) -> Agent {
    Agent::new(AgentConfig {
        data_dir: dir,
        name: name.into(),
        slots: Some(slots),
        net: NetMode::Local,
        bind: Some("127.0.0.1:0".parse().unwrap()),
    })
    .await
    .unwrap()
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

fn submission(args: Vec<Vec<&str>>) -> JobSubmission {
    JobSubmission {
        name: "echo".into(),
        executor: Executor::Wasm,
        module: wat::parse_str(ECHO_WAT).unwrap(),
        tasks: args
            .into_iter()
            .map(|a| TaskInput {
                args: a.into_iter().map(String::from).collect(),
                ..Default::default()
            })
            .collect(),
        limits: Limits {
            timeout_ms: 30_000,
            ..Default::default()
        },
        tier: Tier::Open,
        max_attempts: 3,
        max_price_per_hour: None,
    }
}

async fn submit(client: &AdminClient, sub: JobSubmission) -> JobId {
    match client.call(AdminOp::Submit(Box::new(sub))).await.unwrap() {
        AdminResponse::Submitted { id } => id,
        other => panic!("unexpected {other:?}"),
    }
}

async fn wait_job(client: &AdminClient, id: JobId, secs: u64) -> JobView {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let AdminResponse::Job(view) = client
            .call(AdminOp::Job {
                id,
                with_output: true,
            })
            .await
            .unwrap()
        else {
            panic!("expected job view");
        };
        if view.summary.state.is_terminal() {
            return *view;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job did not finish: {view:#?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_a_sharded_job_across_two_nodes() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_join(agent("a", 2, tempdir("a")).await, ticket.clone());
    let b = spawn_join(agent("b", 2, tempdir("b")).await, ticket);
    wait_online(&cluster, 2).await;

    let client = admin(&cluster).await;
    let shards: Vec<String> = (0..20).map(|i| format!("shard-{i}")).collect();
    let id = submit(
        &client,
        submission(shards.iter().map(|s| vec![s.as_str()]).collect()),
    )
    .await;
    let view = wait_job(&client, id, 30).await;

    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(t.stdout.as_deref(), Some(format!("shard-{i}").as_bytes()));
        assert!(t.fuel_used > 0);
    }
    let nodes: std::collections::HashSet<_> =
        view.tasks.iter().filter_map(|t| t.node.clone()).collect();
    assert_eq!(nodes.len(), 2, "work should be spread over both nodes");
    assert!(view.total_fuel > 0);

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reschedules_tasks_when_a_node_dies() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_join(agent("a", 2, tempdir("a")).await, ticket.clone());
    wait_online(&cluster, 1).await;

    let client = admin(&cluster).await;
    // Two slow tasks: both land on `a`, the only node.
    let id = submit(
        &client,
        submission(vec![vec!["x", "slow"], vec!["y", "slow"]]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let b = spawn_join(agent("b", 2, tempdir("b")).await, ticket);
    wait_online(&cluster, 2).await;
    a.kill().await;

    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let b_id = cluster
        .status()
        .nodes
        .iter()
        .find(|n| n.info.name == "b")
        .unwrap()
        .id
        .clone();
    for t in &view.tasks {
        assert_eq!(t.attempts, 2, "each task ran once on a, then on b");
        assert_eq!(t.node.as_ref(), Some(&b_id));
    }

    client.close().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_bad_ticket_and_reconnects_known_node() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();

    // A forged ticket is rejected and `join` returns an error.
    let forged = JoinTicket {
        cluster: cluster.addr(),
        secret: [7; 32],
    };
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        agent("evil", 1, tempdir("e")).await.join(forged),
    )
    .await
    .expect("join should fail fast");
    assert!(res.is_err());

    // A single-use ticket works once.
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), Some(1))
        .unwrap();
    let dir = tempdir("a");
    let a = spawn_join(agent("a", 1, dir.clone()).await, ticket.clone());
    wait_online(&cluster, 1).await;
    let reuse = tokio::time::timeout(
        Duration::from_secs(10),
        agent("other", 1, tempdir("o")).await.join(ticket),
    )
    .await
    .expect("join should fail fast");
    assert!(
        reuse.is_err(),
        "single-use ticket must not admit a second node"
    );

    // The member restarts and rejoins without any ticket.
    a.kill().await;
    for _ in 0..100 {
        if cluster.status().nodes.iter().all(|n| !n.online) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let again = agent("a", 1, dir).await;
    assert!(again.membership().unwrap().is_some());
    let endpoint = again.endpoint().clone();
    let h = tokio::spawn(again.resume());
    wait_online(&cluster, 1).await;
    assert_eq!(cluster.status().nodes.len(), 1);

    h.abort();
    endpoint.close().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_and_cancelled_jobs() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_join(agent("a", 1, tempdir("a")).await, ticket);
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;

    // Out of fuel is a task failure, not something to retry.
    let mut sub = submission(vec![vec!["x", "slow"]]);
    sub.limits.fuel = Some(10_000);
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 20).await;
    assert_eq!(view.summary.state, JobState::Failed);
    assert_eq!(view.tasks[0].attempts, 1);
    assert!(matches!(
        view.tasks[0].state,
        TaskState::Failed(pumasi_proto::msg::Outcome::OutOfFuel)
    ));

    // Cancel: one task running (1 slot), two waiting.
    let id = submit(
        &client,
        submission(vec![
            vec!["a", "slow"],
            vec!["b", "slow"],
            vec!["c", "slow"],
        ]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    client.call(AdminOp::Cancel { id }).await.unwrap();
    let view = wait_job(&client, id, 20).await;
    assert_eq!(view.summary.state, JobState::Cancelled, "{view:#?}");

    // Bad credentials are refused.
    let mut bad = cluster.admin_ticket();
    bad.secret = [1; 32];
    let intruder = AdminClient::connect(&bad, NetMode::Local).await.unwrap();
    assert!(intruder.call(AdminOp::Status).await.is_err());

    intruder.close().await;
    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_survives_a_control_plane_restart() {
    init_tracing();
    let dir = tempdir("c");
    let cluster = start_cluster(dir.clone()).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let agent_dir = tempdir("a");
    let a = spawn_join(agent("a", 2, agent_dir.clone()).await, ticket);
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;
    let done = submit(&client, submission(vec![vec!["before"]])).await;
    wait_job(&client, done, 20).await;
    client.close().await;
    a.kill().await;
    let name = cluster.name();
    cluster.shutdown().await.unwrap();

    // Restart on the same data dir: same identity, members and jobs.
    let cluster = start_cluster(dir).await.unwrap();
    assert_eq!(cluster.name(), name);
    let status = cluster.status();
    assert_eq!(status.nodes.len(), 1);
    assert_eq!(status.jobs.len(), 1);
    assert_eq!(status.jobs[0].state, JobState::Succeeded);

    // A job submitted while no node is online waits, then runs once the
    // known agent comes back (with a new port, so use the fresh address).
    let client = admin(&cluster).await;
    let queued = submit(&client, submission(vec![vec!["after"]])).await;
    let again = agent("a", 2, agent_dir).await;
    let endpoint = again.endpoint().clone();
    let h = tokio::spawn(again.serve(cluster.addr(), None));
    let view = wait_job(&client, queued, 20).await;
    assert_eq!(view.summary.state, JobState::Succeeded);
    assert_eq!(view.tasks[0].stdout.as_deref(), Some(&b"after"[..]));

    client.close().await;
    h.abort();
    endpoint.close().await;
    cluster.shutdown().await.unwrap();
}
