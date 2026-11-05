//! Control plane high availability (phase 13): three replicas over Raft,
//! the leader killed mid-job, and backups.

use std::{collections::HashSet, path::PathBuf, time::Duration};

use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{CuRates, Policy},
    ha::HaView,
    job::{Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    msg::{AccountView, AdminOp, AdminResponse, Invite, JobState, JobView},
    net::NetMode,
    receipt::Receipt,
    ticket::{JoinTicket, ReplicaTicket},
};
use tokio::task::JoinHandle;

/// Prints argv[1]; with an argv[2], first spins for 1.5 s of wall time.
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
    let p = std::env::temp_dir().join(format!("pumasi-ha-{tag}-{}", uuid_like()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn uuid_like() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::SystemTime::now())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pumasi=info,openraft=error,warn".into()),
        )
        .with_test_writer()
        .try_init();
}

fn config(dir: PathBuf, bind: &str) -> ClusterConfig {
    let mut config = ClusterConfig::new(dir);
    config.name = "ha-cluster".into();
    config.net = NetMode::Local;
    config.bind = Some(bind.parse().unwrap());
    config.heartbeat = Duration::from_millis(200);
    config.node_timeout = Duration::from_secs(3);
    config.mirror_every = Duration::from_secs(1);
    config
}

/// A free local port, so a replica comes back where it was.
fn free_port() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    // iroh binds UDP; the same number is free there too in practice.
    let u = std::net::UdpSocket::bind(("127.0.0.1", port));
    match u {
        Ok(_) => format!("127.0.0.1:{port}"),
        Err(_) => free_port(),
    }
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
        max_attempts: 5,
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

/// What the owner starts with: enough for every job in the test.
const DEPOSIT: i64 = 1_000_000_000_000;

/// Rates that make the tiny echo task cost a visible amount.
fn policy() -> Policy {
    Policy {
        rates: CuRates {
            memory_gib_hour: 100_000_000_000_000_000,
            ..CuRates::default()
        },
        hold_secs: 1,
        verify_permille: 0,
        max_strikes: 3,
        ..Policy::default()
    }
}

async fn call(client: &AdminClient, op: AdminOp) -> AdminResponse {
    client.call(op).await.unwrap()
}

async fn ha_view(client: &AdminClient) -> HaView {
    match call(client, AdminOp::HaStatus).await {
        AdminResponse::Ha(v) => *v,
        other => panic!("unexpected {other:?}"),
    }
}

async fn accounts(client: &AdminClient) -> Vec<AccountView> {
    match call(client, AdminOp::Accounts).await {
        AdminResponse::Accounts(a) => a,
        other => panic!("unexpected {other:?}"),
    }
}

async fn receipts(client: &AdminClient, id: JobId) -> Vec<Receipt> {
    match call(client, AdminOp::Receipts { id }).await {
        AdminResponse::Receipts(r) => r,
        other => panic!("unexpected {other:?}"),
    }
}

async fn submit(client: &AdminClient, sub: JobSubmission) -> JobId {
    match call(client, AdminOp::Submit(Box::new(sub))).await {
        AdminResponse::Submitted { id } => id,
        other => panic!("unexpected {other:?}"),
    }
}

async fn job(client: &AdminClient, id: JobId) -> anyhow::Result<JobView> {
    match client
        .call(AdminOp::Job {
            id,
            with_output: true,
        })
        .await?
    {
        AdminResponse::Job(v) => Ok(*v),
        other => panic!("unexpected {other:?}"),
    }
}

async fn wait_job(client: &AdminClient, id: JobId, secs: u64) -> JobView {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        // Calls may fail for a moment while a new leader is elected.
        if let Ok(view) = job(client, id).await
            && view.summary.state.is_terminal()
        {
            return view;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("job did not finish: {:#?}", job(client, id).await);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn invite_replica(client: &AdminClient) -> ReplicaTicket {
    match call(client, AdminOp::HaInvite { ttl_secs: 600 }).await {
        AdminResponse::JoinToken { ticket, .. } => ticket.parse().unwrap(),
        other => panic!("unexpected {other:?}"),
    }
}

/// Waits until `voters` replicas vote and all have the leader's log.
async fn wait_replicated(client: &AdminClient, voters: usize) -> HaView {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(AdminResponse::Ha(v)) = client.call(AdminOp::HaStatus).await {
            let n = v.replicas.iter().filter(|r| r.voter).count();
            let caught_up = v
                .replicas
                .iter()
                .all(|r| r.matched.is_some() && r.matched >= v.applied);
            if n == voters && caught_up {
                return *v;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("replicas did not catch up: {v:#?}");
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct RunningAgent {
    endpoint: iroh::Endpoint,
    handle: JoinHandle<anyhow::Result<AgentExit>>,
}

async fn provider(name: &str, slots: u32, ticket: JoinTicket) -> RunningAgent {
    let agent = Agent::new(AgentConfig {
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
    })
    .await
    .unwrap();
    let endpoint = agent.endpoint().clone();
    let handle = tokio::spawn(agent.join(ticket));
    RunningAgent { endpoint, handle }
}

/// Money only moves between accounts: what was granted is all there is.
fn total(accounts: &[AccountView]) -> i64 {
    accounts.iter().map(|a| a.balance + a.held).sum()
}

/// One receipt per task, no task paid twice, and the receipts add up to
/// what the job cost.
fn paid_once(view: &JobView, rs: &[Receipt]) {
    assert_eq!(rs.len(), view.tasks.len(), "{rs:#?}");
    let tasks: HashSet<u32> = rs.iter().map(|r| r.usage.task.index).collect();
    assert_eq!(tasks.len(), view.tasks.len(), "a task was paid twice");
    for r in rs {
        r.verify().unwrap();
    }
    assert_eq!(
        rs.iter().map(|r| r.billing.amount).sum::<i64>(),
        view.summary.cost
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_replicas_survive_losing_the_leader() {
    init_tracing();
    let (dir_a, dir_b, dir_c) = (tempdir("a"), tempdir("b"), tempdir("c"));
    let bind_a = free_port();
    let a = Cluster::start(config(dir_a.clone(), &bind_a))
        .await
        .unwrap();
    let cluster_id = a.endpoint_id();
    let client = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    // State from before replication: it reaches the replicas in a snapshot.
    call(&client, AdminOp::SetPolicy(policy())).await;
    call(
        &client,
        AdminOp::Deposit {
            account: "owner".into(),
            amount: DEPOSIT,
            memo: "test".into(),
        },
    )
    .await;
    assert!(!ha_view(&client).await.enabled);

    // Two more replicas.
    let b = Cluster::join_replica(
        config(dir_b.clone(), "127.0.0.1:0"),
        &invite_replica(&client).await,
    )
    .await
    .unwrap();
    let c = Cluster::join_replica(
        config(dir_c.clone(), "127.0.0.1:0"),
        &invite_replica(&client).await,
    )
    .await
    .unwrap();
    let v = wait_replicated(&client, 3).await;
    assert!(v.enabled);
    assert!(a.is_leader() && !b.is_leader() && !c.is_leader());
    // A used replica ticket does not work twice.
    let used = invite_replica(&client).await;
    let d_cluster = Cluster::join_replica(config(tempdir("d"), "127.0.0.1:0"), &used)
        .await
        .unwrap();
    assert!(
        Cluster::join_replica(config(tempdir("e"), "127.0.0.1:0"), &used)
            .await
            .is_err()
    );
    let v = wait_replicated(&client, 4).await;
    d_cluster.shutdown().await.unwrap();
    let d = v
        .replicas
        .iter()
        .find(|r| {
            ![a.endpoint_id(), b.endpoint_id(), c.endpoint_id()]
                .iter()
                .any(|id| id.to_string() == r.id)
        })
        .unwrap()
        .id
        .clone();
    let v = match call(&client, AdminOp::HaRemove { id: d }).await {
        AdminResponse::Ha(v) => *v,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(v.replicas.len(), 3, "{v:#?}");

    // New tickets list every replica; an agent and a client using them
    // follow the leader.
    let ticket = match call(
        &client,
        AdminOp::CreateJoinToken {
            ttl_secs: 600,
            uses: Some(1),
            invite: Some(Invite {
                account: "bob".into(),
                allow_docker: false,
            }),
        },
    )
    .await
    {
        AdminResponse::JoinToken { ticket, .. } => ticket.parse::<JoinTicket>().unwrap(),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(ticket.replicas.len(), 3);
    assert_eq!(ticket.cluster.id, cluster_id);
    let bob = provider("bob", 2, ticket).await;
    let admin_ticket = a.admin_ticket();
    assert_eq!(admin_ticket.replicas.len(), 3);
    let client = AdminClient::connect(&admin_ticket, NetMode::Local)
        .await
        .unwrap();

    // A job before the failure.
    let first = submit(&client, submission(vec![vec!["one"], vec!["two"]])).await;
    let first = wait_job(&client, first, 60).await;
    assert_eq!(first.summary.state, JobState::Succeeded, "{first:#?}");
    paid_once(&first, &receipts(&client, first.summary.id).await);
    assert_eq!(total(&accounts(&client).await), DEPOSIT);

    // A slow job, and the leader dies in the middle of it.
    let slow = submit(
        &client,
        submission((0..6).map(|_| vec!["slow", "spin"]).collect()),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    a.crash().await;

    // A new leader takes over; the job finishes there (its interrupted
    // tasks run again), and nothing is paid twice.
    let slow = wait_job(&client, slow, 120).await;
    assert_eq!(slow.summary.state, JobState::Succeeded, "{slow:#?}");
    for t in &slow.tasks {
        assert_eq!(t.stdout.as_deref(), Some(b"slow".as_slice()));
    }
    assert!(b.is_leader() || c.is_leader());
    paid_once(&slow, &receipts(&client, slow.summary.id).await);
    paid_once(&first, &receipts(&client, first.summary.id).await);
    let accs = accounts(&client).await;
    assert_eq!(total(&accs), DEPOSIT, "{accs:#?}");
    let owner = accs.iter().find(|a| a.name == "owner").unwrap();
    assert_eq!(owner.spent, first.summary.cost + slow.summary.cost);
    let v = ha_view(&client).await;
    assert_eq!(v.voters_up(5_000), 2, "{v:#?}");

    // The old leader comes back as a follower and catches up.
    let a = Cluster::start(config(dir_a.clone(), &bind_a))
        .await
        .unwrap();
    let v = wait_replicated(&client, 3).await;
    assert_eq!(v.voters_up(5_000), 3, "{v:#?}");
    assert!(!a.is_leader());
    let mut caught_up = false;
    for _ in 0..100 {
        if a.ha_view().unwrap().applied >= v.applied {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(caught_up, "{:#?}", a.ha_view());

    // And the cluster keeps working on the new leader.
    let third = submit(&client, submission(vec![vec!["three"]])).await;
    let third = wait_job(&client, third, 60).await;
    assert_eq!(third.summary.state, JobState::Succeeded, "{third:#?}");
    assert_eq!(total(&accounts(&client).await), DEPOSIT);

    client.close().await;
    bob.handle.abort();
    bob.endpoint.close().await;
    for cl in [a, b, c] {
        cl.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_restores_the_cluster() {
    init_tracing();
    let dir = tempdir("orig");
    let cluster = Cluster::start(config(dir.clone(), "127.0.0.1:0"))
        .await
        .unwrap();
    let client = AdminClient::connect(&cluster.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    call(
        &client,
        AdminOp::Deposit {
            account: "alice".into(),
            amount: 42_000,
            memo: "grant".into(),
        },
    )
    .await;
    let id = submit(&client, submission(vec![vec!["kept"]])).await;
    let before = accounts(&client).await;

    let mut bytes = Vec::new();
    let mut digest = None;
    loop {
        let AdminResponse::Backup {
            total,
            digest: d,
            bytes: chunk,
        } = call(
            &client,
            AdminOp::Backup {
                offset: bytes.len() as u64,
            },
        )
        .await
        else {
            panic!()
        };
        digest.get_or_insert(d);
        assert_eq!(digest, Some(d));
        bytes.extend(chunk);
        if bytes.len() as u64 >= total {
            break;
        }
    }
    assert_eq!(blake3::hash(&bytes).as_bytes(), &digest.unwrap());
    client.close().await;
    cluster.shutdown().await.unwrap();

    let restored = tempdir("restored");
    let r = pumasi_cluster::backup::restore(&bytes, &restored).unwrap();
    assert_eq!(r.cluster_name, "ha-cluster");
    let again = Cluster::start(config(restored, "127.0.0.1:0"))
        .await
        .unwrap();
    let client = AdminClient::connect(&again.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    assert_eq!(accounts(&client).await, before);
    assert!(job(&client, id).await.is_ok());
    // The same cluster: the same key.
    assert_eq!(again.status().cluster_id, r.cluster_id);
    client.close().await;
    again.shutdown().await.unwrap();
}
