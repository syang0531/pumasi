//! End-to-end tests: a real control plane and real agents talking over iroh
//! on localhost.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{Policy, PriceList},
    job::{DockerSpec, Executor, FileEntry, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{
        AccountView, AdminOp, AdminResponse, Invite, JobState, JobView, LedgerKind, Role,
        TaskState, Verification,
    },
    net::NetMode,
    receipt::Receipt,
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
        docker: None,
        corrupt_results: false,
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
        docker: None,
        files: vec![],
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
        account: None,
        // These tests time tasks with the real clock.
        deterministic: false,
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

    // Removing it by the short id `pumasi status` shows revokes it: the
    // agent is cut off and its reconnect is rejected.
    let short: String = cluster.status().nodes[0].id.chars().take(10).collect();
    let client = admin(&cluster).await;
    assert!(
        client
            .call(AdminOp::RemoveNode { id: "zzzz".into() })
            .await
            .is_err()
    );
    client
        .call(AdminOp::RemoveNode { id: short })
        .await
        .unwrap();
    assert!(cluster.status().nodes.is_empty());
    let res = tokio::time::timeout(Duration::from_secs(15), h)
        .await
        .expect("removed agent should stop")
        .unwrap();
    assert!(res.is_err(), "agent must be rejected after removal");

    client.close().await;
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

// ---------------------------------------------------------------------------
// Phase 2: invited providers, receipts, credit, checks
// ---------------------------------------------------------------------------

async fn provider(name: &str, slots: u32, dir: PathBuf, corrupt: bool) -> Agent {
    Agent::new(AgentConfig {
        data_dir: dir,
        name: name.into(),
        slots: Some(slots),
        net: NetMode::Local,
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: None,
        corrupt_results: corrupt,
    })
    .await
    .unwrap()
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

/// Prices that make the echo task cost a visible amount: one credit per
/// million fuel units.
fn test_policy(hold_secs: u64, verify_permille: u32, max_strikes: u32) -> Policy {
    Policy {
        prices: PriceList {
            wasm_per_gfuel: 1_000_000_000,
            docker_cpu_hour: 0,
            memory_gib_hour: 0,
        },
        hold_secs,
        verify_permille,
        max_strikes,
    }
}

async fn set_policy(client: &AdminClient, p: Policy) {
    client.call(AdminOp::SetPolicy(p)).await.unwrap();
}

async fn accounts(client: &AdminClient) -> Vec<AccountView> {
    match client.call(AdminOp::Accounts).await.unwrap() {
        AdminResponse::Accounts(a) => a,
        other => panic!("unexpected {other:?}"),
    }
}

async fn account(client: &AdminClient, name: &str) -> AccountView {
    accounts(client)
        .await
        .into_iter()
        .find(|a| a.name == name)
        .unwrap_or_else(|| panic!("no account {name}"))
}

async fn deposit(client: &AdminClient, name: &str, credits: i64) {
    client
        .call(AdminOp::Deposit {
            account: name.into(),
            amount: credits * 1_000_000,
            memo: "test".into(),
        })
        .await
        .unwrap();
}

async fn receipts(client: &AdminClient, id: JobId) -> Vec<Receipt> {
    match client.call(AdminOp::Receipts { id }).await.unwrap() {
        AdminResponse::Receipts(r) => r,
        other => panic!("unexpected {other:?}"),
    }
}

fn deterministic_echo(n: usize) -> JobSubmission {
    let shards: Vec<String> = (0..n).map(|i| format!("shard-{i}")).collect();
    let mut sub = submission(shards.iter().map(|s| vec![s.as_str()]).collect());
    sub.deterministic = true;
    sub
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invited_provider_is_paid_from_prepaid_credit() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    // No checks here (there is no second node to run them); a short hold.
    set_policy(&client, test_policy(1, 0, 3)).await;

    let p = spawn_join(
        provider("bobs-pc", 2, tempdir("p"), false).await,
        invite(&cluster, "bob"),
    );
    wait_online(&cluster, 1).await;
    let node = cluster.status().nodes[0].clone();
    assert_eq!(node.role, Role::Provider);
    assert_eq!(node.account, "bob");
    assert_eq!(node.executors, vec![Executor::Wasm]);

    // Without credit the owner's job does not go to bob's machine.
    let id = submit(&client, deterministic_echo(4)).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let AdminResponse::Job(view) = client
        .call(AdminOp::Job {
            id,
            with_output: false,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(view.summary.state, JobState::Queued, "{view:#?}");

    // Prepaid credit unblocks it.
    deposit(&client, "owner", 1000).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(t.stdout.as_deref(), Some(format!("shard-{i}").as_bytes()));
        assert!(t.cost > 0);
    }
    let cost = view.summary.cost;
    assert!(cost > 0);
    assert_eq!(cost, view.tasks.iter().map(|t| t.cost).sum::<i64>());

    // One countersigned receipt per run, each verifiable on its own.
    let rs = receipts(&client, id).await;
    assert_eq!(rs.len(), 4);
    for r in &rs {
        r.verify().unwrap();
        assert_eq!(r.billing.consumer, "owner");
        assert_eq!(r.billing.provider, "bob");
        assert_eq!(r.node_id(), node.id);
        assert_eq!(r.cluster_id(), cluster.status().cluster_id);
        assert!(r.usage.fuel_used > 0);
        assert!(r.usage.peak_memory_bytes.unwrap() >= 65536);
        assert!(r.billing.billed_wall_ms <= r.usage.wall_ms);
        // One credit per million fuel.
        assert_eq!(r.billing.amount, r.usage.fuel_used as i64);
    }
    assert_eq!(rs.iter().map(|r| r.billing.amount).sum::<i64>(), cost);

    let owner = account(&client, "owner").await;
    assert_eq!(owner.balance, 1000 * 1_000_000 - cost);
    assert_eq!(owner.spent, cost);

    // After the hold period bob's earnings become available.
    let mut bob = account(&client, "bob").await;
    for _ in 0..100 {
        if bob.held == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        bob = account(&client, "bob").await;
    }
    assert_eq!((bob.balance, bob.held, bob.earned), (cost, 0, cost));

    // Payouts come out of the available balance only.
    assert!(
        client
            .call(AdminOp::Payout {
                account: "bob".into(),
                amount: cost + 1,
                memo: String::new(),
            })
            .await
            .is_err()
    );
    client
        .call(AdminOp::Payout {
            account: "bob".into(),
            amount: cost,
            memo: "test".into(),
        })
        .await
        .unwrap();
    assert_eq!(account(&client, "bob").await.balance, 0);

    let AdminResponse::Ledger(entries) = client
        .call(AdminOp::Ledger {
            account: Some("bob".into()),
            limit: 100,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    let kinds: Vec<LedgerKind> = entries.iter().map(|e| e.kind).collect();
    assert_eq!(kinds.iter().filter(|k| **k == LedgerKind::Hold).count(), 4);
    assert_eq!(
        kinds.iter().filter(|k| **k == LedgerKind::Release).count(),
        4
    );
    assert_eq!(kinds[0], LedgerKind::Payout, "newest first");

    // The owner account cannot be handed out in an invite.
    assert!(
        client
            .call(AdminOp::CreateJoinToken {
                ttl_secs: 60,
                uses: Some(1),
                invite: Some(Invite {
                    account: "owner".into(),
                    allow_docker: false,
                }),
            })
            .await
            .is_err()
    );

    client.close().await;
    p.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checks_catch_a_dishonest_provider() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    // Check every provider result; long hold so nothing is released.
    set_policy(&client, test_policy(3600, 1000, 2)).await;
    deposit(&client, "owner", 1000).await;

    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let o = spawn_join(agent("own", 1, tempdir("o")).await, ticket);
    let h = spawn_join(
        provider("honest", 1, tempdir("h"), false).await,
        invite(&cluster, "alice"),
    );
    let m = spawn_join(
        provider("cheat", 1, tempdir("m"), true).await,
        invite(&cluster, "mallory"),
    );
    wait_online(&cluster, 3).await;
    let id_of = |name: &str| {
        cluster
            .status()
            .nodes
            .iter()
            .find(|n| n.info.name == name)
            .unwrap()
            .id
            .clone()
    };
    let (honest_id, cheat_id) = (id_of("honest"), id_of("cheat"));

    let id = submit(&client, deterministic_echo(6)).await;
    let view = wait_job(&client, id, 60).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let mut caught = 0;
    for (i, t) in view.tasks.iter().enumerate() {
        // Every result is correct: the cheat's were replaced by the
        // trusted check run's.
        assert_eq!(
            t.stdout.as_deref(),
            Some(format!("shard-{i}").as_bytes()),
            "{view:#?}"
        );
        if let Verification::Mismatch { bad } = &t.verification {
            assert_eq!(bad, &vec![cheat_id.clone()]);
            caught += 1;
        }
    }
    assert!(caught >= 1, "the cheat should have been caught: {view:#?}");

    let status = cluster.status();
    let cheat = status.nodes.iter().find(|n| n.id == cheat_id).unwrap();
    let honest = status.nodes.iter().find(|n| n.id == honest_id).unwrap();
    assert_eq!(cheat.strikes, caught);
    assert_eq!(honest.strikes, 0);
    let honest_ran = view
        .tasks
        .iter()
        .filter(|t| t.verification == Verification::Passed)
        .count() as u32;
    // It may also have run check runs for other tasks.
    assert!(honest.checks_passed >= honest_ran);

    // Mallory's held earnings were forfeited and refunded; alice keeps hers
    // on hold.
    let mallory = account(&client, "mallory").await;
    assert_eq!((mallory.held, mallory.earned, mallory.balance), (0, 0, 0));
    let alice = account(&client, "alice").await;
    let owner = account(&client, "owner").await;
    assert_eq!(
        owner.spent, alice.held,
        "owner pays only for accepted provider runs"
    );
    assert_eq!(owner.balance, 1000 * 1_000_000 - owner.spent);
    let AdminResponse::Ledger(entries) = client
        .call(AdminOp::Ledger {
            account: None,
            limit: 1000,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    let forfeits = entries
        .iter()
        .filter(|e| e.kind == LedgerKind::Forfeit)
        .count() as u32;
    let refunds = entries
        .iter()
        .filter(|e| e.kind == LedgerKind::Refund)
        .count() as u32;
    assert_eq!((forfeits, refunds), (caught, caught));

    // The cheat's receipts are still valid signatures: it signed what it
    // claimed. The check, not the signature, is what caught it.
    for r in receipts(&client, id).await {
        r.verify().unwrap();
    }

    // Enough strikes suspend the machine: new work avoids it.
    if caught >= 2 {
        assert!(cheat.suspended);
        let id = submit(&client, deterministic_echo(4)).await;
        let view = wait_job(&client, id, 60).await;
        assert_eq!(view.summary.state, JobState::Succeeded);
        assert!(
            view.tasks
                .iter()
                .all(|t| t.node.as_deref() != Some(&cheat_id))
        );
    }

    client.close().await;
    o.kill().await;
    h.kill().await;
    m.kill().await;
    cluster.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Docker executor (skipped when no Docker daemon is available)
// ---------------------------------------------------------------------------

/// True when a Docker daemon that runs Linux containers is reachable
/// (Windows CI runners have Docker in Windows-container mode).
fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["version", "--format", "{{.Server.Os}}"])
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "linux")
        .unwrap_or(false)
}

fn docker_job(command: &[&str], tasks: Vec<TaskInput>) -> JobSubmission {
    JobSubmission {
        name: "docker-test".into(),
        executor: Executor::Docker,
        module: vec![],
        docker: Some(DockerSpec {
            image: "busybox:1.36".into(),
            command: command.iter().map(|s| s.to_string()).collect(),
            network: false,
            gpus: None,
        }),
        files: vec![FileEntry {
            path: "shared.txt".into(),
            data: b"shared+".to_vec(),
        }],
        tasks,
        limits: Limits {
            timeout_ms: 60_000,
            memory_bytes: 64 << 20,
            ..Default::default()
        },
        tier: Tier::Open,
        max_attempts: 3,
        max_price_per_hour: None,
        account: None,
        deterministic: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docker_tasks_get_files_and_run_only_on_docker_nodes() {
    if !docker_available() {
        eprintln!("skipping: docker is not available");
        return;
    }
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let wasm_only = spawn_join(agent("wasm-only", 4, tempdir("w")).await, ticket.clone());
    let docker_agent = Agent::new(AgentConfig {
        data_dir: tempdir("d"),
        name: "docker".into(),
        slots: Some(4),
        net: NetMode::Local,
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: Some("docker".into()),
        corrupt_results: false,
    })
    .await
    .unwrap();
    let docker_id = docker_agent.id().to_string();
    let d = spawn_join(docker_agent, ticket);
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;

    // Shared + per-task input files, an output file, stdout, and no network.
    let script = "cat shared.txt mine.txt > /output/out.txt && mkdir -p /output/sub && echo $0 > /output/sub/arg.txt && ! wget -q -T 2 -O /dev/null http://1.1.1.1 && echo isolated";
    let tasks = (0..3)
        .map(|i| TaskInput {
            args: vec![format!("arg{i}")],
            files: vec![FileEntry {
                path: "mine.txt".into(),
                data: format!("task{i}").into_bytes(),
            }],
            ..Default::default()
        })
        .collect();
    let id = submit(&client, docker_job(&["sh", "-c", script], tasks)).await;
    let view = wait_job(&client, id, 300).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for (i, t) in view.tasks.iter().enumerate() {
        assert_eq!(
            t.node.as_deref(),
            Some(docker_id.as_str()),
            "docker task on a wasm-only node"
        );
        assert_eq!(t.stdout.as_deref(), Some(&b"isolated\n"[..]));
        let AdminResponse::Files(files) = client
            .call(AdminOp::TaskFiles {
                id,
                index: i as u32,
            })
            .await
            .unwrap()
        else {
            panic!("expected files");
        };
        assert_eq!(
            files,
            vec![
                FileEntry {
                    path: "out.txt".into(),
                    data: format!("shared+task{i}").into_bytes()
                },
                FileEntry {
                    path: "sub/arg.txt".into(),
                    data: format!("arg{i}\n").into_bytes()
                },
            ]
        );
    }

    // Timeout kills the container; a non-zero exit is reported as such.
    let mut slow = docker_job(&["sleep", "30"], vec![TaskInput::default()]);
    slow.limits.timeout_ms = 1500;
    let id = submit(&client, slow).await;
    let view = wait_job(&client, id, 60).await;
    assert!(
        matches!(
            view.tasks[0].state,
            TaskState::Failed(pumasi_proto::msg::Outcome::Timeout)
        ),
        "{view:#?}"
    );

    let id = submit(
        &client,
        docker_job(&["sh", "-c", "exit 3"], vec![TaskInput::default()]),
    )
    .await;
    let view = wait_job(&client, id, 60).await;
    assert!(
        matches!(
            view.tasks[0].state,
            TaskState::Failed(pumasi_proto::msg::Outcome::Exited(3))
        ),
        "{view:#?}"
    );

    // No leftover containers.
    let out = std::process::Command::new("docker")
        .args(["ps", "-aq", "--filter", "label=pumasi=1"])
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        "containers left behind"
    );

    client.close().await;
    wasm_only.kill().await;
    d.kill().await;
    cluster.shutdown().await.unwrap();
}
