//! Control plane high availability (phase 13): three replicas over Raft,
//! the leader killed mid-job, and backups.

use std::{collections::HashSet, path::PathBuf, time::Duration};

use pumasi_agent::{Agent, AgentConfig, AgentExit};
use pumasi_cluster::{AdminClient, Cluster, ClusterConfig};
use pumasi_proto::{
    economy::{CuRates, Policy},
    ha::HaView,
    job::{Executor, JobId, JobSubmission, Limits, TaskInput, Tier},
    manage::AccessRole,
    msg::{AccountView, AdminOp, AdminResponse, Invite, JobState, JobView},
    net::NetMode,
    receipt::Receipt,
    ticket::{AdminTicket, JoinTicket, ReplicaTicket},
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
                // The agent's task starts and ends too: a failover test
                // that hangs then shows where the tasks stopped.
                .unwrap_or_else(|_| "pumasi=info,pumasi_agent=debug,openraft=error,warn".into()),
        )
        .with_test_writer()
        .try_init();
}

fn no_honeypots() -> pumasi_proto::verify::VerifyPolicyPatch {
    pumasi_proto::verify::VerifyPolicyPatch {
        honeypots: Some(false),
        ..Default::default()
    }
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
        placement: Default::default(),
        gpu_ext: Default::default(),
        net: Default::default(),
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
        queue_timeout_ms: None,
        gpu_capabilities: vec![],
        run: Default::default(),
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
        gpu_mps: false,
        power: Default::default(),
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
    // No honeypots: they would bill the owner for runs the test counts.
    call(&client, AdminOp::SetVerifyPolicy(no_honeypots())).await;
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

    // The old leader comes back as a follower and catches up. Windows
    // can take a moment to release the crashed endpoint's UDP port.
    let mut tries = 0;
    let a = loop {
        match Cluster::start(config(dir_a.clone(), &bind_a)).await {
            Ok(a) => break a,
            Err(e) if tries < 20 => {
                tries += 1;
                eprintln!("restarting the old leader: {e:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("restarting the old leader: {e:#}"),
        }
    };
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

/// A start that fails after opening the file store lets go of it, so the
/// next start in the same process works (1.13: a failed restart of a
/// replica left the store's database locked, and every retry failed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_start_releases_what_it_opened() {
    init_tracing();
    let dir = tempdir("failed-start");
    let cluster = Cluster::start(config(dir.clone(), "127.0.0.1:0"))
        .await
        .unwrap();
    cluster.shutdown().await.unwrap();
    // A Raft log that cannot be opened: replication looks on, and the
    // start fails after the file store and the endpoint are open.
    std::fs::write(dir.join("raft.redb"), b"not a database").unwrap();
    for _ in 0..2 {
        let err = Cluster::start(config(dir.clone(), "127.0.0.1:0"))
            .await
            .err()
            .expect("a broken log fails the start");
        let msg = format!("{err:#}");
        assert!(msg.contains("raft.redb"), "{msg}");
        assert!(!msg.contains("already open"), "{msg}");
    }
    std::fs::remove_file(dir.join("raft.redb")).unwrap();
    let cluster = Cluster::start(config(dir.clone(), "127.0.0.1:0"))
        .await
        .expect("starts once the log is gone");
    cluster.shutdown().await.unwrap();
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

/// A secret from an access ticket the cluster made.
async fn access_secret(client: &AdminClient, name: &str, role: AccessRole) -> pumasi_proto::Secret {
    match call(
        client,
        AdminOp::CreateAccess {
            name: name.into(),
            role,
            ttl_secs: None,
        },
    )
    .await
    {
        AdminResponse::AccessTicket { ticket, .. } => ticket.parse::<AdminTicket>().unwrap().secret,
        other => panic!("unexpected {other:?}"),
    }
}

/// A client whose main connection is to one replica only.
async fn client_of(replica: &Cluster, secret: pumasi_proto::Secret) -> AdminClient {
    let t = AdminTicket {
        cluster: replica.addr(),
        secret,
        replicas: Vec::new(),
    };
    AdminClient::connect(&t, NetMode::Local).await.unwrap()
}

/// 1.12: followers answer the status, events and latest metrics from
/// what the leader hands them; tickets revoked a moment ago are sent to
/// the leader. 1.19: jobs, task details, metrics history, and
/// submitters' own views too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_answer_reads() {
    init_tracing();
    let (dir_a, dir_b) = (tempdir("ra"), tempdir("rb"));
    let a = Cluster::start(config(dir_a.clone(), &free_port()))
        .await
        .unwrap();
    let admin = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    let b = Cluster::join_replica(
        config(dir_b.clone(), "127.0.0.1:0"),
        &invite_replica(&admin).await,
    )
    .await
    .unwrap();
    wait_replicated(&admin, 2).await;
    assert!(a.is_leader() && !b.is_leader());
    let viewer = access_secret(&admin, "watcher", AccessRole::Viewer).await;
    let submitter = access_secret(
        &admin,
        "robot",
        AccessRole::Submitter {
            account: "robots".into(),
        },
    )
    .await;

    // The follower answers once the leader handed it a cache.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    return_to_follower_retry(&b, viewer, deadline).await;
    let on_b = client_of(&b, viewer).await;
    for op in [
        AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 20,
        },
        AdminOp::Metrics {
            node: None,
            since_secs: 0,
        },
    ] {
        let r = on_b
            .call(AdminOp::FollowerRead(Box::new(op)))
            .await
            .unwrap();
        assert!(
            matches!(r, AdminResponse::Events(_) | AdminResponse::Metrics(_)),
            "{r:?}"
        );
        assert_eq!(
            on_b.connected_to(),
            b.endpoint_id(),
            "the follower sent a read on"
        );
    }
    // A plain call still goes to the leader.
    call(&on_b, AdminOp::Status).await;
    assert_eq!(on_b.connected_to(), a.endpoint_id());

    // 1.19: jobs, task details and metrics history too. A job no
    // provider runs stays pending, so the leader hands it over.
    call(&admin, AdminOp::SetPolicy(policy())).await;
    call(&admin, AdminOp::SetVerifyPolicy(no_honeypots())).await;
    call(
        &admin,
        AdminOp::Deposit {
            account: "owner".into(),
            amount: DEPOSIT,
            memo: "test".into(),
        },
    )
    .await;
    let id = submit(&admin, submission(vec![vec!["waits"]])).await;
    // (A client the follower sent on stays with the leader: a fresh one
    // each try.)
    let on_b2 = eventually(20, "the follower answering for the job", || async {
        let c = client_of(&b, viewer).await;
        let r = c
            .call(AdminOp::FollowerRead(Box::new(AdminOp::Job {
                id,
                with_output: false,
            })))
            .await;
        match r {
            Ok(AdminResponse::Job(v)) if c.connected_to() == b.endpoint_id() => {
                assert_eq!(v.summary.id, id);
                Ok(c)
            }
            other => {
                let e = format!("{other:?}");
                c.close().await;
                Err(e)
            }
        }
    })
    .await;
    let r = on_b2
        .call(AdminOp::FollowerRead(Box::new(AdminOp::TaskDetails { id })))
        .await
        .unwrap();
    assert!(matches!(r, AdminResponse::TaskDetails(_)), "{r:?}");
    assert_eq!(on_b2.connected_to(), b.endpoint_id());
    // Metrics history from the follower's own copy.
    let r = on_b2
        .call(AdminOp::FollowerRead(Box::new(AdminOp::Metrics {
            node: None,
            since_secs: 3600,
        })))
        .await
        .unwrap();
    assert!(matches!(r, AdminResponse::Metrics(_)), "{r:?}");
    assert_eq!(on_b2.connected_to(), b.endpoint_id());
    on_b2.close().await;

    // A submitter's status: the follower answers once it has the
    // account's capacity, and only with the submitter's own jobs.
    let sub_b = eventually(20, "the follower answering a submitter", || async {
        let c = client_of(&b, submitter).await;
        match c
            .call(AdminOp::FollowerRead(Box::new(AdminOp::Status)))
            .await
        {
            Ok(AdminResponse::Status(s)) if c.connected_to() == b.endpoint_id() => Ok((c, s)),
            other => {
                let e = format!("{other:?}");
                c.close().await;
                Err(e)
            }
        }
    })
    .await;
    let (sub_b, s) = sub_b;
    assert!(s.capacity.is_some(), "{s:?}");
    assert!(
        s.jobs.iter().all(|j| j.id != id),
        "the admin's job leaked: {s:?}"
    );
    // Someone else's job: not from the follower's cache.
    let r = sub_b
        .call(AdminOp::FollowerRead(Box::new(AdminOp::Job {
            id,
            with_output: false,
        })))
        .await;
    assert!(!matches!(r, Ok(AdminResponse::Job(_))), "{r:?}");

    // A revoked ticket is refused by the follower as soon as the
    // revocation reached its copy of the state.
    call(
        &admin,
        AdminOp::RevokeAccess {
            id: "watcher".into(),
        },
    )
    .await;
    // The leader answers before the write went through Raft (the store
    // queues it), so neither a fixed wait nor the leader's applied index
    // read right after says when the follower has it: wait for the refusal.
    let gone = client_of(&b, viewer).await;
    let e = eventually(30, "the follower refusing a revoked ticket", || async {
        match gone
            .call(AdminOp::FollowerRead(Box::new(AdminOp::Status)))
            .await
        {
            Ok(_) => Err("still answered".to_string()),
            Err(e) => Ok(format!("{e:#}")),
        }
    })
    .await;
    assert!(e.contains("invalid admin credentials"), "{e}");

    // `read` uses a follower when the ticket names replicas.
    let full = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    assert!(matches!(
        full.read(AdminOp::Status).await.unwrap(),
        AdminResponse::Status(_)
    ));

    for c in [on_b, sub_b, gone, full, admin] {
        c.close().await;
    }
    b.shutdown().await.unwrap();
    a.shutdown().await.unwrap();
}

/// Waits until a fresh client on the follower gets a follower answer.
async fn return_to_follower_retry(
    b: &Cluster,
    secret: pumasi_proto::Secret,
    deadline: tokio::time::Instant,
) {
    loop {
        let c = client_of(b, secret).await;
        let r = c
            .call(AdminOp::FollowerRead(Box::new(AdminOp::Status)))
            .await;
        let here = c.connected_to() == b.endpoint_id();
        c.close().await;
        if r.is_ok() && here {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower never answered"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// A binary that says it is pumasi `version` (the start of this test's
/// own executable and a build-info line).
fn binary_claiming(version: &str) -> Vec<u8> {
    use std::io::Read;
    let mut bytes = vec![0u8; 4096];
    let mut f = std::fs::File::open(std::env::current_exe().unwrap()).unwrap();
    f.read_exact(&mut bytes).unwrap();
    bytes.extend_from_slice(
        format!(
            "\0pumasi-build-info:v={version};p={};\0",
            pumasi_proto::PROTOCOL_VERSION
        )
        .as_bytes(),
    );
    bytes
}

/// A version newer than this build's.
fn newer_version() -> String {
    let mine = pumasi_proto::version();
    let core_end = mine.find(['-', '+']).unwrap_or(mine.len());
    let core: String = mine[..core_end]
        .chars()
        .map(|c| if c.is_ascii_digit() { '9' } else { c })
        .collect();
    format!("{core}{}", &mine[core_end..])
}

/// Starts a replica again from its directory (Windows can take a moment to
/// let go of the old endpoint's port).
async fn restart(dir: &std::path::Path, bind: &str) -> Cluster {
    let mut tries = 0;
    loop {
        match Cluster::start(config(dir.to_path_buf(), bind)).await {
            Ok(c) => return c,
            Err(e) if tries < 20 => {
                tries += 1;
                eprintln!("restarting a replica: {e:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("restarting a replica: {e:#}"),
        }
    }
}

/// Retries `f` for up to `secs` until it is `Ok`; panics with the last
/// error.
async fn eventually<T, F, Fut>(secs: u64, what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) if tokio::time::Instant::now() > deadline => panic!("{what}: {e}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// The control plane's own upgrade with replicas (1.13): followers one at
/// a time with the leader's leave, never while that would cost the quorum,
/// and the leader last, stepping down; a job running across it finishes
/// and is paid once.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replicas_upgrade_one_at_a_time() {
    use pumasi_proto::release::{ControlPlaneUpgrade, ReleaseFile, Target, sha256};
    init_tracing();
    let (dir_a, dir_b, dir_c) = (tempdir("up-a"), tempdir("up-b"), tempdir("up-c"));
    let (bind_a, bind_b, bind_c) = (free_port(), free_port(), free_port());
    let a = Cluster::start(config(dir_a.clone(), &bind_a))
        .await
        .unwrap();
    let client = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    call(&client, AdminOp::SetPolicy(policy())).await;
    // No honeypots: they would bill the owner for runs the test counts.
    call(&client, AdminOp::SetVerifyPolicy(no_honeypots())).await;
    call(
        &client,
        AdminOp::Deposit {
            account: "owner".into(),
            amount: DEPOSIT,
            memo: "test".into(),
        },
    )
    .await;
    let b = Cluster::join_replica(
        config(dir_b.clone(), &bind_b),
        &invite_replica(&client).await,
    )
    .await
    .unwrap();
    let c = Cluster::join_replica(
        config(dir_c.clone(), &bind_c),
        &invite_replica(&client).await,
    )
    .await
    .unwrap();
    wait_replicated(&client, 3).await;
    assert!(a.is_leader());
    let client = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
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
    let bob = provider("bob", 2, ticket).await;
    let warm = submit(&client, submission(vec![vec!["warm"]])).await;
    assert_eq!(
        wait_job(&client, warm, 60).await.summary.state,
        JobState::Succeeded
    );

    // A release newer than every replica.
    let version = newer_version();
    let bytes = binary_claiming(&version);
    let file = ReleaseFile {
        target: Target::host(),
        version: version.clone(),
        protocol: pumasi_proto::PROTOCOL_VERSION,
        sha256: sha256(&bytes),
        size: bytes.len() as u64,
    };
    match call(
        &client,
        AdminOp::UploadRelease {
            sha256: file.sha256,
            size: file.size,
            offset: 0,
            bytes: bytes.clone(),
        },
    )
    .await
    {
        AdminResponse::Uploaded { have } => assert_eq!(have, file.size),
        other => panic!("unexpected {other:?}"),
    }
    match call(
        &client,
        AdminOp::Rollout {
            files: vec![file.clone()],
            force: false,
        },
    )
    .await
    {
        AdminResponse::Rollout(_) => {}
        other => panic!("unexpected {other:?}"),
    }

    // Every replica, followers too, finds the binary to install.
    for (name, cl) in [("a", &a), ("b", &b), ("c", &c)] {
        let up = eventually(60, &format!("{name} has the binary"), || async {
            cl.own_upgrade(None)
                .ok_or_else(|| "no upgrade yet".to_string())
        })
        .await;
        assert_eq!(up.file.sha256, file.sha256);
        assert_eq!(std::fs::read(&up.path).unwrap(), bytes);
    }

    // The leader goes last.
    let why = eventually(30, "the leader waits for the followers", || async {
        match a.may_restart(&version).await {
            Err(why) if why.contains("followers go first") => Ok(why),
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    assert!(
        why.contains(&b.endpoint_id().fmt_short().to_string())
            || why.contains(&c.endpoint_id().fmt_short().to_string()),
        "{why}"
    );

    // A follower may not restart while that would leave no majority.
    c.shutdown().await.unwrap();
    let why = eventually(30, "no turn without a majority", || async {
        match b.may_restart(&version).await {
            Err(why) if why.contains("majority") => Ok(why),
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    assert!(why.contains("2 of 3"), "{why}");
    let c = restart(&dir_c, &bind_c).await;
    wait_replicated(&client, 3).await;

    // One follower at a time.
    eventually(30, "b gets its turn", || b.may_restart(&version)).await;
    let why = c.may_restart(&version).await.unwrap_err();
    assert!(why.contains("is restarting"), "{why}");
    let view = a.control_planes();
    assert_eq!(view.len(), 3, "{view:#?}");
    let b_view = view
        .iter()
        .find(|v| v.id == b.endpoint_id().to_string())
        .unwrap();
    assert_eq!(b_view.state, ControlPlaneUpgrade::Upgrading, "{view:#?}");

    // B restarts into the new version; then it is C's turn.
    b.shutdown().await.unwrap();
    let b = restart(&dir_b, &bind_b).await;
    b.report_version_for_tests(&version);
    eventually(60, "c gets its turn once b runs the new version", || {
        c.may_restart(&version)
    })
    .await;
    let why = a.may_restart(&version).await.unwrap_err();
    assert!(
        why.contains("restarting") || why.contains("still runs"),
        "{why}"
    );
    c.shutdown().await.unwrap();
    let c = restart(&dir_c, &bind_c).await;
    c.report_version_for_tests(&version);

    // Then the leader, while a job runs: it steps down, one of the others
    // takes over, and the job finishes there.
    eventually(60, "the leader's turn", || a.may_restart(&version)).await;
    let slow = submit(
        &client,
        submission((0..4).map(|_| vec!["slow", "spin"]).collect()),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    a.shutdown().await.unwrap();
    let a = restart(&dir_a, &bind_a).await;
    a.report_version_for_tests(&version);
    let slow = wait_job(&client, slow, 120).await;
    assert_eq!(slow.summary.state, JobState::Succeeded, "{slow:#?}");
    paid_once(&slow, &receipts(&client, slow.summary.id).await);
    assert_eq!(total(&accounts(&client).await), DEPOSIT);
    // (A replica back within the election timeout, with the whole log, may
    // win the election again; on the new version either way.)
    let leader = eventually(30, "a leader", || async {
        [&a, &b, &c]
            .into_iter()
            .find(|r| r.is_leader())
            .ok_or_else(|| "no leader".to_string())
    })
    .await;

    // Every replica is current in the new leader's view.
    eventually(30, "every replica is current", || async {
        let view = leader.control_planes();
        if view.len() == 3 && view.iter().all(|v| v.state == ControlPlaneUpgrade::Current) {
            Ok(())
        } else {
            Err(format!("{view:#?}"))
        }
    })
    .await;
    match call(&client, AdminOp::ControlPlanes).await {
        AdminResponse::ControlPlanes(v) => assert_eq!(v.len(), 3, "{v:#?}"),
        other => panic!("unexpected {other:?}"),
    }

    client.close().await;
    bob.handle.abort();
    bob.endpoint.close().await;
    for cl in [a, b, c] {
        cl.shutdown().await.unwrap();
    }
}

/// Sends failed sign-ins for `ip`; answers whether it is blocked.
async fn fail_sign_in(client: &AdminClient, ip: &str) -> anyhow::Result<bool> {
    use pumasi_proto::limit::{LimitOp, LimitReply};
    match client
        .call(AdminOp::Limit(Box::new(LimitOp::Failed { ip: ip.into() })))
        .await?
    {
        AdminResponse::Limit(r) => match *r {
            LimitReply::Blocked(list) => Ok(list.iter().any(|(a, _)| a == ip)),
            other => panic!("unexpected {other:?}"),
        },
        other => panic!("unexpected {other:?}"),
    }
}

/// 1.19 gate: the failed sign-ins counted by a leader that is then killed
/// still count on the new one; and a replica restarted on another address
/// rejoins without an invite, by telling the leader its new address.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn limits_survive_a_new_leader_and_replicas_move() {
    use pumasi_proto::limit::MAX_FAILURES;
    init_tracing();
    let (dir_a, dir_b, dir_c) = (tempdir("lim-a"), tempdir("lim-b"), tempdir("lim-c"));
    let a = Cluster::start(config(dir_a.clone(), &free_port()))
        .await
        .unwrap();
    let first = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    let b = Cluster::join_replica(
        config(dir_b.clone(), &free_port()),
        &invite_replica(&first).await,
    )
    .await
    .unwrap();
    let c = Cluster::join_replica(
        config(dir_c.clone(), &free_port()),
        &invite_replica(&first).await,
    )
    .await
    .unwrap();
    wait_replicated(&first, 3).await;
    first.close().await;
    let client = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();

    // A burst of failed sign-ins, short of a block.
    let ip = "203.0.113.9";
    let before = MAX_FAILURES - 4;
    for _ in 0..before {
        assert!(!fail_sign_in(&client, ip).await.unwrap());
    }
    // The leader writes its counts down every 2 s; then it dies.
    tokio::time::sleep(Duration::from_millis(3000)).await;
    a.crash().await;

    // The new leader carries on counting where the old one stopped.
    let mut blocked = false;
    let mut sent = 0;
    eventually(60, "a new leader", || async {
        if b.is_leader() || c.is_leader() {
            Ok(())
        } else {
            Err("no leader yet".to_string())
        }
    })
    .await;
    while sent < MAX_FAILURES - before {
        match fail_sign_in(&client, ip).await {
            Ok(b) => {
                sent += 1;
                blocked = b;
            }
            // The client finding the new leader.
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    assert!(
        blocked,
        "{before} failures before the failover and {sent} after did not block"
    );

    // A follower restarted on another port rejoins with no invite: the
    // leader learns the new address and replicates to it again.
    let (leader, follower, dir_f) = if b.is_leader() {
        (b, c, dir_c)
    } else {
        (c, b, dir_b)
    };
    let id = follower.endpoint_id();
    follower.crash().await;
    let new_bind = free_port();
    let moved = restart(&dir_f, &new_bind).await;
    assert_eq!(moved.endpoint_id(), id);
    let port: u16 = new_bind.rsplit(':').next().unwrap().parse().unwrap();
    eventually(60, "the membership naming the new address", || async {
        let addrs = leader.membership();
        match addrs.iter().find(|m| m.id == id) {
            Some(m) if m.ip_addrs().any(|s| s.port() == port) => Ok(()),
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    // New writes reach it.
    call(
        &client,
        AdminOp::Deposit {
            account: "owner".into(),
            amount: 1,
            memo: "after the move".into(),
        },
    )
    .await;
    let want = leader.ha_view().unwrap().applied;
    eventually(60, "the moved replica catching up", || async {
        let got = moved.ha_view().unwrap().applied;
        if got >= want {
            Ok(())
        } else {
            Err(format!("{got:?} < {want:?}"))
        }
    })
    .await;

    client.close().await;
    for cl in [moved, leader] {
        cl.shutdown().await.unwrap();
    }
}

/// 1.19 gate: `?wait=` (JobWait) and `logs -f` (LogsWait) are answered as
/// soon as the job ends, not at the next poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushed_waits_answer_at_once() {
    init_tracing();
    let a = Cluster::start(config(tempdir("wait"), "127.0.0.1:0"))
        .await
        .unwrap();
    let client = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    call(&client, AdminOp::SetPolicy(policy())).await;
    // No honeypots: they would bill the owner for runs the test counts.
    call(&client, AdminOp::SetVerifyPolicy(no_honeypots())).await;
    call(
        &client,
        AdminOp::Deposit {
            account: "owner".into(),
            amount: DEPOSIT,
            memo: "test".into(),
        },
    )
    .await;
    // No provider: the job waits until it is cancelled.
    let id = submit(&client, submission(vec![vec!["never"]])).await;

    // A wait that runs out answers with the job as it is.
    let t = std::time::Instant::now();
    match call(&client, AdminOp::JobWait { id, wait_ms: 300 }).await {
        AdminResponse::Job(v) => assert!(!v.summary.state.is_terminal()),
        other => panic!("unexpected {other:?}"),
    }
    assert!(
        t.elapsed() >= Duration::from_millis(250),
        "{:?}",
        t.elapsed()
    );

    let waiter = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    let logs = AdminClient::connect(&a.admin_ticket(), NetMode::Local)
        .await
        .unwrap();
    let job_wait = tokio::spawn(async move {
        let r = waiter
            .call(AdminOp::JobWait {
                id,
                wait_ms: 30_000,
            })
            .await
            .unwrap();
        (r, std::time::Instant::now(), waiter)
    });
    let logs_wait = tokio::spawn(async move {
        let r = logs
            .call(AdminOp::LogsWait {
                job: id,
                index: 0,
                attempt: 0,
                stdout_from: 0,
                stderr_from: 0,
                wait_ms: 30_000,
            })
            .await
            .unwrap();
        (r, std::time::Instant::now(), logs)
    });
    // Both are waiting on the control plane by now.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!job_wait.is_finished() && !logs_wait.is_finished());
    call(&client, AdminOp::Cancel { id }).await;
    let ended = std::time::Instant::now();

    let (r, at, waiter) = job_wait.await.unwrap();
    match r {
        AdminResponse::Job(v) => assert_eq!(v.summary.state, JobState::Cancelled, "{v:#?}"),
        other => panic!("unexpected {other:?}"),
    }
    let late = at.saturating_duration_since(ended);
    assert!(
        late < Duration::from_millis(100),
        "JobWait answered {late:?} after the end"
    );
    let (r, at, logs) = logs_wait.await.unwrap();
    assert!(matches!(r, AdminResponse::Logs(_)), "{r:?}");
    let late = at.saturating_duration_since(ended);
    assert!(
        late < Duration::from_millis(100),
        "LogsWait answered {late:?} after the end"
    );

    // And the client helper uses the pushed wait.
    let t = std::time::Instant::now();
    let r = client.wait_job(id, Duration::from_secs(10)).await.unwrap();
    assert!(matches!(r, AdminResponse::Job(_)));
    assert!(t.elapsed() < Duration::from_secs(1));

    for c in [client, waiter, logs] {
        c.close().await;
    }
    a.shutdown().await.unwrap();
}
