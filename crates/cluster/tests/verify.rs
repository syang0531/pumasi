//! Honeypots and scores (phase 36, 1.20) end to end: a real control plane
//! slips known-answer tasks into real agents' queues over iroh on
//! localhost. The disk challenge is tested in `bench.rs`, which times
//! machines in a test binary of its own.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{CuRates, Policy},
    job::{Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{AccountView, AdminOp, AdminResponse, Invite, JobState, JobView},
    net::NetMode,
    ticket::JoinTicket,
    verify::{HoneypotResult, VerifyPolicyPatch, VerifyView},
};
use tokio::task::JoinHandle;

/// Prints argv[1] to stdout.
const ECHO_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "_start")
    (local $p i32) (local $len i32)
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $args_get (i32.const 64) (i32.const 1024)))
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
    let p = std::env::temp_dir().join(format!("pumasi-verify-{tag}-{}", rand_u64()));
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
    Cluster::start(config).await
}

struct RunningAgent {
    endpoint: iroh::Endpoint,
    handle: JoinHandle<Result<AgentExit>>,
}

impl RunningAgent {
    async fn kill(self) {
        self.handle.abort();
        self.endpoint.close().await;
    }
}

async fn agent(name: &str, dir: PathBuf, corrupt: bool) -> Agent {
    Agent::new(AgentConfig {
        data_dir: dir,
        name: name.into(),
        slots: Some(1),
        net: NetMode::Local,
        relays: Vec::new(),
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: None,
        microvm: None,
        upgrade: None,
        corrupt_results: corrupt,
        store_cache_bytes: None,
        gpus: false,
        fake_gpus: None,
        webgpu: None,
        gpu_graphics: None,
        docker_wsl_libs: None,
        gpu_mps: false,
        power: Default::default(),
        max_cpu_percent: None,
        availability: None,
        bench: None,
    })
    .await
    .unwrap()
}

fn spawn_join(agent: Agent, ticket: JoinTicket) -> RunningAgent {
    let endpoint = agent.endpoint().clone();
    let handle = tokio::spawn(agent.join(ticket));
    RunningAgent { endpoint, handle }
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

async fn admin(cluster: &Cluster) -> AdminClient {
    AdminClient::connect(&cluster.admin_ticket(), NetMode::Local)
        .await
        .unwrap()
}

async fn wait_online(cluster: &Cluster, n: usize) {
    wait_for("nodes online", || {
        cluster.status().nodes.iter().filter(|n| n.online).count() >= n
    })
    .await;
}

async fn wait_for(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn node_id(cluster: &Cluster, name: &str) -> String {
    cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.info.name == name)
        .unwrap()
        .id
}

fn echo(n: usize) -> JobSubmission {
    JobSubmission {
        placement: Default::default(),
        gpu_ext: Default::default(),
        net: Default::default(),
        name: "render-frames".into(),
        executor: Executor::Wasm,
        module: wat::parse_str(ECHO_WAT).unwrap(),
        docker: None,
        files: vec![],
        tasks: (0..n)
            .map(|i| TaskInput {
                args: vec![format!("frame-{i}")],
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
        deterministic: true,
        mounts: vec![],
        task_mounts: vec![],
        output_dataset: None,
        gpu: None,
        priority: 0,
        selector: Default::default(),
        tolerations: vec![],
        after: vec![],
        queue_timeout_ms: None,
        gpu_capabilities: vec![],
    }
}

async fn run(client: &AdminClient, sub: JobSubmission) -> (JobId, JobView) {
    let id = match client.call(AdminOp::Submit(Box::new(sub))).await.unwrap() {
        AdminResponse::Submitted { id } => id,
        other => panic!("unexpected {other:?}"),
    };
    for _ in 0..600 {
        let AdminResponse::Job(view) = client
            .call(AdminOp::Job {
                id,
                with_output: true,
            })
            .await
            .unwrap()
        else {
            panic!("expected a job view");
        };
        if view.summary.state.is_terminal() {
            return (id, *view);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("job {id} did not finish");
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
        .unwrap_or_else(|| AccountView {
            name: name.into(),
            balance: 0,
            held: 0,
            spent: 0,
            earned: 0,
        })
}

async fn verify(client: &AdminClient, op: AdminOp) -> VerifyView {
    match client.call(op).await.unwrap() {
        AdminResponse::Verify(v) => *v,
        other => panic!("unexpected {other:?}"),
    }
}

/// Rates that make the tiny echo task cost a visible amount.
fn policy(hold_secs: u64) -> Policy {
    Policy {
        rates: CuRates {
            memory_gib_hour: 100_000_000_000_000_000,
            ..CuRates::default()
        },
        hold_secs,
        // No result checks: only honeypots catch the cheat here.
        verify_permille: 0,
        // Strikes lower its score but never suspend it, so placement has
        // two nodes to choose from.
        max_strikes: 100,
        ..Policy::default()
    }
}

fn honeypots(permille: u32) -> VerifyPolicyPatch {
    VerifyPolicyPatch {
        honeypots: Some(permille > 0),
        new_permille: Some(permille),
        provider_permille: Some(permille),
        struck_permille: Some(permille),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_fails_honeypots_is_held_and_placed_last() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    client.call(AdminOp::SetPolicy(policy(1))).await.unwrap();
    client
        .call(AdminOp::Deposit {
            account: "owner".into(),
            amount: 1_000_000_000_000,
            memo: "test".into(),
        })
        .await
        .unwrap();
    // A honeypot after every task.
    verify(&client, AdminOp::SetVerifyPolicy(honeypots(1000))).await;

    let h = spawn_join(
        agent("honest", tempdir("h"), false).await,
        invite(&cluster, "alice"),
    );
    let m = spawn_join(
        agent("cheat", tempdir("m"), true).await,
        invite(&cluster, "mallory"),
    );
    wait_online(&cluster, 2).await;
    let (honest, cheat) = (node_id(&cluster, "honest"), node_id(&cluster, "cheat"));

    let (_, view) = run(&client, echo(6)).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    // Both ran the job's tasks, each followed by a built-in honeypot.
    wait_for("both nodes' honeypots", || {
        let v = cluster.verify_view();
        let ended = |node: &str, r: HoneypotResult| {
            v.runs
                .iter()
                .any(|x| x.node == node && x.result.as_ref() == Some(&r))
        };
        ended(&honest, HoneypotResult::Passed) && ended(&cheat, HoneypotResult::Failed)
    })
    .await;

    let v = cluster.verify_view();
    // Honeypots are hidden from job lists: only the one job shows.
    let AdminResponse::Status(s) = client.call(AdminOp::Status).await.unwrap() else {
        panic!()
    };
    assert_eq!(s.jobs.len(), 1, "{:#?}", s.jobs);
    assert!(v.runs.iter().all(|r| r.honeypot.is_none()), "built-in only");
    assert!(v.runs.iter().all(|r| r.executor == Executor::Wasm));
    // Mallory's earnings are held; the cheat is struck and scores lower.
    let until = v
        .held
        .iter()
        .find(|(a, _)| a == "mallory")
        .map(|(_, t)| *t)
        .expect("mallory's earnings held");
    assert!(
        until > pumasi_cluster::state::now_unix() + 3600,
        "held for a day"
    );
    assert!(!v.held.iter().any(|(a, _)| a == "alice"));
    let score = |id: &str| v.scores.iter().find(|s| s.node == id).unwrap().clone();
    let (hs, cs) = (score(&honest), score(&cheat));
    assert!(
        hs.honeypots_passed >= 1 && hs.honeypots_failed == 0,
        "{hs:?}"
    );
    assert!(cs.honeypots_failed >= 1 && cs.strikes >= 1, "{cs:?}");
    assert!(hs.score > cs.score, "{hs:?} {cs:?}");
    assert!(cs.held_until.is_some() && hs.held_until.is_none());
    assert!(
        cluster.prometheus_text().contains("pumasi_node_score{"),
        "the score is exported"
    );

    // Both now earn from a second job: alice's earnings are released after
    // the one-second hold, mallory's are not.
    verify(&client, AdminOp::SetVerifyPolicy(honeypots(0))).await;
    let (_, view) = run(&client, echo(4)).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let cheat_ran = view
        .tasks
        .iter()
        .any(|t| t.node.as_deref() == Some(cheat.as_str()));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let alice = account(&client, "alice").await;
    assert!(
        alice.earned > 0 && alice.held == 0,
        "earned {} held {}",
        alice.earned,
        alice.held
    );
    let mallory = account(&client, "mallory").await;
    if cheat_ran {
        assert!(mallory.held > 0, "nothing held");
    }
    // The owner lets them go: they wait out the usual hold.
    let v = verify(
        &client,
        AdminOp::HoneypotRelease {
            account: "mallory".into(),
        },
    )
    .await;
    assert!(v.held.is_empty());
    let mut released = false;
    for _ in 0..50 {
        if account(&client, "mallory").await.held == 0 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(released, "still held");

    // With both idle, single tasks go to the node with the higher score.
    for _ in 0..3 {
        let (_, view) = run(&client, echo(1)).await;
        assert_eq!(
            view.tasks[0].node.as_deref(),
            Some(honest.as_str()),
            "{view:#?}"
        );
    }

    client.close().await;
    h.kill().await;
    m.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_account_honeypot_is_confirmed_then_sent_to_providers() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let client = admin(&cluster).await;
    client.call(AdminOp::SetPolicy(policy(1))).await.unwrap();
    client
        .call(AdminOp::Deposit {
            account: "owner".into(),
            amount: 1_000_000_000_000,
            memo: "test".into(),
        })
        .await
        .unwrap();
    verify(&client, AdminOp::SetVerifyPolicy(honeypots(1000))).await;
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let o = spawn_join(agent("own", tempdir("o"), false).await, ticket);
    wait_online(&cluster, 1).await;

    // A job on the owner's own machine becomes a honeypot.
    let (job, view) = run(&client, echo(2)).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    let v = verify(&client, AdminOp::HoneypotAdd { job, task: Some(1) }).await;
    assert_eq!(v.honeypots.len(), 1, "{v:#?}");
    let id = v.honeypots[0].id;
    assert_eq!((v.honeypots[0].source, v.honeypots[0].task), (job, 1));
    // The same task twice is refused.
    assert!(
        client
            .call(AdminOp::HoneypotAdd { job, task: Some(1) })
            .await
            .is_err()
    );
    // A trusted machine runs it again before it is used.
    wait_for("the honeypot confirmed", || {
        cluster.verify_view().honeypots.iter().any(|h| h.confirmed)
    })
    .await;
    o.kill().await;

    // A provider running the owner's tasks then gets it.
    let p = spawn_join(
        agent("provider", tempdir("p"), false).await,
        invite(&cluster, "alice"),
    );
    wait_online(&cluster, 1).await;
    let provider = node_id(&cluster, "provider");
    let (_, view) = run(&client, echo(1)).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    wait_for("the account honeypot passed", || {
        cluster.verify_view().runs.iter().any(|r| {
            r.node == provider && r.honeypot == Some(id) && r.result == Some(HoneypotResult::Passed)
        })
    })
    .await;
    let v = cluster.verify_view();
    let h = &v.honeypots[0];
    assert!(h.sent >= 1 && h.passed >= 1 && h.failed == 0, "{h:?}");
    // The copy looks like the job it came from.
    assert_eq!(h.name, "render-frames");

    let v = verify(&client, AdminOp::HoneypotRemove { id }).await;
    assert!(v.honeypots.is_empty());

    client.close().await;
    p.kill().await;
    cluster.shutdown().await.unwrap();
}
