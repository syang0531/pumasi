//! End-to-end tests of cluster management (phase 10) over real iroh
//! connections: selectors, drain, preemption, quotas, access tickets and
//! `apply`.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    job::{Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    manage::{AccessRole, AccountQuota, ApplySpec, NodeEdit, NodeSpec},
    msg::{AdminOp, AdminResponse, JobState, JobView, NodeView, TaskState},
    net::NetMode,
    ticket::{AdminTicket, JoinTicket},
};
use tokio::task::JoinHandle;

/// Prints argv[1]. With an argv[2], first busy-waits 3 seconds of
/// wall-clock time (a long task to drain or preempt).
const SLOW_ECHO_WAT: &str = r#"
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
          (br_if $done (i64.ge_u (i64.sub (call $time) (local.get $start)) (i64.const 3000000000)))
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
    let p = std::env::temp_dir().join(format!("pumasi-manage-{tag}-{}", rand_u64()));
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
    async fn kill(self) {
        self.handle.abort();
        self.endpoint.close().await;
    }
}

async fn spawn_agent(name: &str, slots: u32, ticket: JoinTicket) -> RunningAgent {
    let config = AgentConfig {
        data_dir: tempdir(name),
        name: name.into(),
        slots: Some(slots),
        net: NetMode::Local,
        bind: Some("127.0.0.1:0".parse().unwrap()),
        docker: None,
        microvm: None,
        upgrade: None,
        corrupt_results: false,
        store_cache_bytes: None,
        gpus: false,
        fake_gpus: None,
        max_cpu_percent: None,
        availability: None,
        bench: None,
    };
    let agent = Agent::new(config).await.unwrap();
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

/// One task per entry: `["x"]` echoes x at once, `["x", "slow"]` after 3 s.
fn submission(args: Vec<Vec<&str>>) -> JobSubmission {
    JobSubmission {
        name: "echo".into(),
        executor: Executor::Wasm,
        module: wat::parse_str(SLOW_ECHO_WAT).unwrap(),
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
            timeout_ms: 60_000,
            ..Default::default()
        },
        tier: Tier::Open,
        max_attempts: 1,
        max_price_per_hour: None,
        account: None,
        deterministic: false,
        mounts: vec![],
        task_mounts: vec![],
        output_dataset: None,
        gpu: None,
        priority: 0,
        selector: Default::default(),
        tolerations: vec![],
        after: vec![],
    }
}

async fn submit(client: &AdminClient, sub: JobSubmission) -> JobId {
    match client.call(AdminOp::Submit(Box::new(sub))).await.unwrap() {
        AdminResponse::Submitted { id } => id,
        other => panic!("unexpected {other:?}"),
    }
}

async fn job(client: &AdminClient, id: JobId) -> JobView {
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
    *view
}

async fn wait_job(client: &AdminClient, id: JobId, secs: u64) -> JobView {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let view = job(client, id).await;
        if view.summary.state.is_terminal() {
            return view;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job did not finish: {view:#?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits until `n` tasks of a job run.
async fn wait_running(client: &AdminClient, id: JobId, n: usize) -> JobView {
    for _ in 0..200 {
        let view = job(client, id).await;
        if view
            .tasks
            .iter()
            .filter(|t| matches!(t.state, TaskState::Running { .. }))
            .count()
            >= n
        {
            return view;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("tasks did not start: {:#?}", job(client, id).await);
}

fn node(cluster: &Cluster, name: &str) -> NodeView {
    cluster
        .status()
        .nodes
        .into_iter()
        .find(|n| n.info.name == name)
        .unwrap()
}

async fn edit(client: &AdminClient, node: &str, edit: NodeEdit) {
    client
        .call(AdminOp::EditNode {
            node: node.into(),
            edit: Box::new(edit),
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selectors_drain_and_preemption() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", 2, ticket.clone()).await;
    let b = spawn_agent("b", 2, ticket).await;
    wait_online(&cluster, 2).await;
    let client = admin(&cluster).await;
    let (a_id, b_id) = (node(&cluster, "a").id, node(&cluster, "b").id);

    // Labels and a selector: only b runs the job.
    edit(
        &client,
        "b",
        NodeEdit {
            set_labels: [("zone".to_string(), "lab".to_string())].into(),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(node(&cluster, "b").labels["zone"], vec!["lab".to_string()]);
    let mut sub = submission((0..4).map(|_| vec!["x"]).collect());
    sub.selector = "zone=lab,os!=plan9".parse().unwrap();
    let id = submit(&client, sub).await;
    let view = wait_job(&client, id, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert!(
        view.tasks.iter().all(|t| t.node.as_deref() == Some(&b_id)),
        "selector: {view:#?}"
    );

    // Drain: runs on a move to b, and a gets nothing new.
    edit(
        &client,
        "b",
        NodeEdit {
            cordon: Some(true),
            ..Default::default()
        },
    )
    .await;
    let id = submit(
        &client,
        submission(vec![vec!["d0", "slow"], vec!["d1", "slow"]]),
    )
    .await;
    let view = wait_running(&client, id, 2).await;
    assert!(view.tasks.iter().all(|t| t.node.as_deref() == Some(&a_id)));
    edit(
        &client,
        "b",
        NodeEdit {
            cordon: Some(false),
            ..Default::default()
        },
    )
    .await;
    let AdminResponse::Drained { moved } = client
        .call(AdminOp::Drain { node: "a".into() })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(moved, 2);
    assert!(node(&cluster, "a").config.cordoned);
    let view = wait_job(&client, id, 30).await;
    // max_attempts is 1, yet the moved runs did not count.
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    for t in &view.tasks {
        assert_eq!(t.node.as_deref(), Some(b_id.as_str()), "{view:#?}");
        assert_eq!(t.preempted, 1);
        assert_eq!(t.attempts, 2);
    }
    let quick = submit(&client, submission(vec![vec!["q"]; 3])).await;
    let view = wait_job(&client, quick, 30).await;
    assert!(view.tasks.iter().all(|t| t.node.as_deref() == Some(&b_id)));

    // Preemption on b alone (a stays cordoned): two slow low-priority runs
    // fill it; a high-priority job takes a slot at once and finishes
    // first; the stopped run comes back and finishes too.
    let low = submit(
        &client,
        submission(vec![vec!["l0", "slow"], vec!["l1", "slow"]]),
    )
    .await;
    wait_running(&client, low, 2).await;
    let mut sub = submission(vec![vec!["high"]]);
    sub.priority = 100;
    let high = submit(&client, sub).await;
    let view = wait_job(&client, high, 10).await;
    assert_eq!(view.summary.state, JobState::Succeeded);
    assert_eq!(view.tasks[0].stdout.as_deref(), Some(&b"high"[..]));
    let low_now = job(&client, low).await;
    assert!(
        !low_now.summary.state.is_terminal(),
        "the high-priority job ran before the low one finished"
    );
    let view = wait_job(&client, low, 30).await;
    assert_eq!(view.summary.state, JobState::Succeeded, "{view:#?}");
    assert_eq!(view.tasks.iter().map(|t| t.preempted).sum::<u32>(), 1);

    client.close().await;
    a.kill().await;
    b.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quotas_access_tickets_and_apply() {
    init_tracing();
    let cluster = start_cluster(tempdir("c")).await.unwrap();
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", 4, ticket).await;
    wait_online(&cluster, 1).await;
    let client = admin(&cluster).await;

    // A submitter ticket for alice.
    let AdminResponse::AccessTicket { ticket, view } = client
        .call(AdminOp::CreateAccess {
            name: "alice laptop".into(),
            role: AccessRole::Submitter {
                account: "alice".into(),
            },
            ttl_secs: Some(3600),
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(view.expires_unix.is_some());
    let ticket: AdminTicket = ticket.parse().unwrap();
    let alice = AdminClient::connect(&ticket, NetMode::Local).await.unwrap();
    let AdminResponse::WhoAmI { role, .. } = alice.call(AdminOp::WhoAmI).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        role,
        AccessRole::Submitter {
            account: "alice".into()
        }
    );
    // It cannot remove nodes or change quotas.
    let node_id = node(&cluster, "a").id;
    let err = alice
        .call(AdminOp::RemoveNode {
            id: node_id.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("cannot remove nodes"),
        "{err:#}"
    );
    assert!(
        alice
            .call(AdminOp::SetQuota {
                account: "alice".into(),
                quota: AccountQuota::default(),
            })
            .await
            .is_err()
    );
    assert!(cluster.status().nodes.iter().any(|n| n.id == node_id));

    // alice's runs on the owner's machine are paid; with a quota of one
    // slot, her four tasks run one at a time.
    client
        .call(AdminOp::Deposit {
            account: "alice".into(),
            amount: 1_000_000,
            memo: "test".into(),
        })
        .await
        .unwrap();
    client
        .call(AdminOp::SetQuota {
            account: "alice".into(),
            quota: AccountQuota {
                slots: Some(1),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let owners = submit(&client, submission(vec![vec!["o"]])).await;
    let id = submit(&alice, submission(vec![vec!["t", "slow"]; 2])).await;
    let mut max_running = 0;
    loop {
        let v = job(&alice, id).await;
        let running = v
            .tasks
            .iter()
            .filter(|t| matches!(t.state, TaskState::Running { .. }))
            .count();
        max_running = max_running.max(running);
        if v.summary.state.is_terminal() {
            assert_eq!(v.summary.state, JobState::Succeeded, "{v:#?}");
            assert_eq!(v.summary.account, "alice");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(max_running, 1, "the quota allows one run at a time");
    // She sees only her jobs.
    let AdminResponse::Status(s) = alice.call(AdminOp::Status).await.unwrap() else {
        panic!()
    };
    assert!(s.jobs.iter().all(|j| j.account == "alice"));
    assert!(alice.call(AdminOp::Cancel { id: owners }).await.is_err());
    // Revoked: the ticket stops working.
    client
        .call(AdminOp::RevokeAccess {
            id: view.id.clone(),
        })
        .await
        .unwrap();
    assert!(alice.call(AdminOp::Status).await.is_err());
    alice.close().await;

    // apply: the first time changes things, the second time nothing.
    let spec = ApplySpec {
        nodes: vec![NodeSpec {
            node: "a".into(),
            labels: [("zone".to_string(), "home".to_string())].into(),
            taints: vec!["batch".parse().unwrap()],
            cordoned: Some(false),
        }],
        accounts: vec![(
            "alice".into(),
            AccountQuota {
                slots: Some(2),
                gpus: Some(0),
                ..Default::default()
            },
        )],
        schedules: vec![],
        prune: false,
        dry_run: false,
        ..Default::default()
    };
    let AdminResponse::Applied(first) = client
        .call(AdminOp::Apply(Box::new(spec.clone())))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(first.len(), 2, "{first:?}");
    let AdminResponse::Applied(second) = client.call(AdminOp::Apply(Box::new(spec))).await.unwrap()
    else {
        panic!()
    };
    assert!(second.is_empty(), "{second:?}");
    let n = node(&cluster, "a");
    assert_eq!(n.config.taints[0].to_string(), "batch");
    // The taint keeps jobs that do not tolerate it off.
    let id = submit(&client, submission(vec![vec!["t"]])).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(job(&client, id).await.summary.state, JobState::Queued);
    let mut sub = submission(vec![vec!["t"]]);
    sub.tolerations = vec!["batch".parse().unwrap()];
    let id = submit(&client, sub).await;
    assert_eq!(
        wait_job(&client, id, 30).await.summary.state,
        JobState::Succeeded
    );

    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_wait_for_their_dependencies_across_a_restart() {
    init_tracing();
    let dir = tempdir("c");
    let cluster = start_cluster(dir.clone()).await.unwrap();
    let client = admin(&cluster).await;
    // No nodes yet: the first job waits in the queue, the second for it.
    let first = submit(&client, submission(vec![vec!["first"]])).await;
    let mut sub = submission(vec![vec!["second"]]);
    sub.after = vec![first];
    let second = submit(&client, sub).await;
    let v = job(&client, second).await;
    assert_eq!(
        v.waiting.as_deref(),
        Some(&*format!("waiting for job {first}"))
    );
    client.close().await;
    cluster.shutdown().await.unwrap();

    // After a restart the second job still waits, then runs after the
    // first succeeds.
    let cluster = start_cluster(dir).await.unwrap();
    let client = admin(&cluster).await;
    assert!(job(&client, second).await.waiting.is_some());
    let ticket = cluster
        .create_join_ticket(Duration::from_secs(60), None)
        .unwrap();
    let a = spawn_agent("a", 2, ticket).await;
    let v1 = wait_job(&client, first, 30).await;
    let v2 = wait_job(&client, second, 30).await;
    assert_eq!(v1.summary.state, JobState::Succeeded);
    assert_eq!(v2.summary.state, JobState::Succeeded);
    assert_eq!(v2.tasks[0].stdout.as_deref(), Some(&b"second"[..]));
    assert!(v2.waiting.is_none());
    client.close().await;
    a.kill().await;
    cluster.shutdown().await.unwrap();
}
